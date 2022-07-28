/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateDownloader;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.rescaling.RocksDBRescalingCompactFiltersManager;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyMetaData;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;
import static org.apache.flink.util.Preconditions.checkArgument;

public class RocksDBRestoreWithRescalingByFileIngestion<K>
        extends AbstractRocksDBRestoreOperation<K> {
    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBRestoreWithRescalingByFileIngestion.class);
    public static final String SECONDARY_DB_DIR_FORMAT = "secondary_db_%d_to_%d";

    private final String operatorIdentifier;
    private final long lastCompletedCheckpointId;
    private final SortedMap<Long, Set<StateHandleID>> restoredSstFiles;
    private UUID backendUID;
    private final long writeBatchSize;

    private List<SecondaryDBMeta> secondaryDbInstances;
    private final boolean parallelizedL0Compaction;

    public RocksDBRestoreWithRescalingByFileIngestion(
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            int numberOfTransferringThreads,
            CloseableRegistry cancelStreamRegistry,
            ClassLoader userCodeClassLoader,
            Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            StateSerializerProvider<K> keySerializerProvider,
            File instanceBasePath,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            RocksDBRescalingCompactFiltersManager rescalingCompactFiltersManager,
            boolean parallelizedL0Compaction) {

        super(
                keyGroupRange,
                keyGroupPrefixBytes,
                numberOfTransferringThreads,
                cancelStreamRegistry,
                userCodeClassLoader,
                kvStateInformation,
                keySerializerProvider,
                instanceBasePath,
                instanceRocksDBPath,
                dbOptions,
                columnFamilyOptionsFactory,
                nativeMetricOptions,
                metricGroup,
                restoreStateHandles,
                ttlCompactFiltersManager,
                rescalingCompactFiltersManager);
        this.operatorIdentifier = operatorIdentifier;
        this.restoredSstFiles = new TreeMap<>();
        this.lastCompletedCheckpointId = -1L;
        this.backendUID = UUID.randomUUID();
        checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
        this.writeBatchSize = writeBatchSize;
        this.parallelizedL0Compaction = parallelizedL0Compaction;
    }

    @Override
    public RocksDBRestoreResult restore() throws Exception {
        long startTs;
        if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
            return null;
        }

        final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

        boolean isRescaling =
                (restoreStateHandles.size() > 1
                        || !Objects.equals(theFirstStateHandle.getKeyGroupRange(), keyGroupRange));

        checkArgument(
                isRescaling,
                "RocksDBRestoreWithRescalingByFileIngestion must be used for restore-with-rescaling");

        secondaryDbInstances = new ArrayList<>(restoreStateHandles.size());
        ExecutorService parallelL0CompactionExecutor = null;

        try {
            startTs = System.currentTimeMillis();
            prepareSecondaryDbInstances();

            startTs = System.currentTimeMillis();
            if (parallelizedL0Compaction) {
                parallelL0CompactionExecutor = Executors.newFixedThreadPool(secondaryDbInstances.size());
                List<Future<Boolean>> compactionJobs = new ArrayList<>();

                long parallelizeFileIngestionStart = System.currentTimeMillis();
                for (SecondaryDBMeta secondaryDb : secondaryDbInstances) {
                    compactionJobs.add(parallelL0CompactionExecutor.submit(new L0CompactionJob(secondaryDb)));
                }

                for (Future<Boolean> compactionJob : compactionJobs) {
                    compactionJob.get();
                }

            } else {
                for (SecondaryDBMeta secondaryDb : secondaryDbInstances) {
                    secondaryDb.compactionL0();
                }
            }

        } catch (Exception e) {
            throw new Exception("Restore with rescaling by file ingestion error " + e);
        } finally {
            for (SecondaryDBMeta db : secondaryDbInstances) {
                db.close();
            }

            if (parallelL0CompactionExecutor != null) {
                parallelL0CompactionExecutor.shutdown();
                parallelL0CompactionExecutor.awaitTermination(0, TimeUnit.MILLISECONDS);
            }
        }

        if (!instanceRocksDBPath.mkdirs()) {
            String errMsg =
                    "Could not create RocksDB data directory: "
                            + instanceBasePath.getAbsolutePath();
            LOG.error(errMsg);
            throw new IOException(errMsg);
        }

        SecondaryDBMeta firstSecondaryDB = secondaryDbInstances.iterator().next();
        firstSecondaryDB.hasIngested = true;

        restoreFromLocalState(transferSecondaryMetaToLocalStateHandle(firstSecondaryDB));

        ingestSecondaryDbInstances();
        cleanSecondaryDbPath();

        return new RocksDBRestoreResult(
                this.db,
                defaultColumnFamilyHandle,
                nativeMetricMonitor,
                lastCompletedCheckpointId,
                backendUID,
                restoredSstFiles);
    }

    private void prepareSecondaryDbInstances() throws Exception {
        for (KeyedStateHandle stateHandle : restoreStateHandles) {
            secondaryDbInstances.add(restoreDBInstanceFromStateHandle(stateHandle));
        }
        Collections.sort(secondaryDbInstances);
    }

    private void ingestSecondaryDbInstances() throws RocksDBException {
        for (Map.Entry<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> entry :
                kvStateInformation.entrySet()) {
            String stateName = entry.getKey();
            RocksDBKeyedStateBackend.RocksDbKvStateInfo kvStateInfo = entry.getValue();
            ColumnFamilyHandle columnFamilyHandle = kvStateInfo.columnFamilyHandle;
            IngestionContext context = new IngestionContext(stateName);

            for (SecondaryDBMeta secondaryDb : secondaryDbInstances) {
                if (secondaryDb.hasIngested) {
                    continue;
                }
                List<List<FlinkSstFileMetaData>> files = secondaryDb.getStateFiles(stateName);
                context.addFiles(files);
            }

            Preconditions.checkArgument(context.ingestFiles.get(0).isEmpty(), "no files in L0!");
            for (int level = context.ingestFiles.size() - 1; level >= 1; level--) {
                if (context.ingestFiles.get(level).isEmpty()) {
                    continue;
                }

                IngestExternalFileOptions ingestExternalFileOptions =
                        new IngestExternalFileOptions();
                int ingestFileNum = context.ingestFiles.get(level).size();
                List<String> externalFiles = new ArrayList<>(ingestFileNum);
                long[] smallestSeqnos = new long[ingestFileNum];
                long[] largestSeqnos = new long[ingestFileNum];

                int idx = 0;
                for (FlinkSstFileMetaData fileToIngest : context.ingestFiles.get(level)) {
                    externalFiles.add(fileToIngest.path() + "/" + fileToIngest.fileName());
                    smallestSeqnos[idx] = fileToIngest.smallestSeqno();
                    largestSeqnos[idx++] = fileToIngest.largestSeqno();
                }

                ingestExternalFileOptions.setTargetLevel(level - 1);
                ingestExternalFileOptions.setSmallestSeqnos(smallestSeqnos);
                ingestExternalFileOptions.setLargestSeqnos(largestSeqnos);
                ingestExternalFileOptions.setIsPlainFile(true);
                ingestExternalFileOptions.setMoveFiles(true);

                db.ingestExternalFile(columnFamilyHandle, externalFiles, ingestExternalFileOptions);
            }
        }
    }

    public void cleanSecondaryDbPath() throws IOException {
        for (SecondaryDBMeta secondaryDb : secondaryDbInstances) {
            FileUtils.deleteDirectory(secondaryDb.dbPath.toFile());
        }
        secondaryDbInstances.clear();
    }

    private void restoreInstanceDirectoryFromPath(Path source, String instanceRocksDBPath) throws IOException {
        final Path instanceRocksDBDirectory = Paths.get(instanceRocksDBPath);
        final Path[] files = FileUtils.listDirectory(source);

        for (Path file : files) {
            final String fileName = file.getFileName().toString();
            final Path targetFile = instanceRocksDBDirectory.resolve(fileName);
            if (fileName.endsWith(SST_FILE_SUFFIX)) {
                Files.createLink(targetFile, file);
            } else {
                Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private void restoreFromLocalState(IncrementalLocalKeyedStateHandle localKeyedStateHandle)
            throws Exception {
        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(localKeyedStateHandle.getMetaDataState());
        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();
        columnFamilyDescriptors =
                createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, true);
        columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);

        Path restoreSourcePath = localKeyedStateHandle.getDirectoryStateHandle().getDirectory();

        if (!instanceRocksDBPath.exists() && !instanceRocksDBPath.mkdirs()) {
            String errMsg =
                    "Could not create RocksDB data directory: "
                            + instanceBasePath.getAbsolutePath();
            LOG.error(errMsg);
            throw new IOException(errMsg);
        }

        restoreInstanceDirectoryFromPath(restoreSourcePath, dbPath);

        openDB();

        registerColumnFamilyHandles(stateMetaInfoSnapshots);

    }

    private void registerColumnFamilyHandles(List<StateMetaInfoSnapshot> metaInfoSnapshots) {
        for (int i = 0; i < metaInfoSnapshots.size(); ++i) {
            getOrRegisterStateColumnFamilyHandle(
                    columnFamilyHandles.get(i), metaInfoSnapshots.get(i));
        }
    }

    private IncrementalLocalKeyedStateHandle transferSecondaryMetaToLocalStateHandle(SecondaryDBMeta secondaryDBMeta) {
       KeyedStateHandle originalStateHandle = secondaryDBMeta.stateHandle;
       if (originalStateHandle instanceof IncrementalLocalKeyedStateHandle) {
           return (IncrementalLocalKeyedStateHandle) originalStateHandle;
       }

       IncrementalRemoteKeyedStateHandle remoteStateHandle = (IncrementalRemoteKeyedStateHandle) originalStateHandle;

       return new IncrementalLocalKeyedStateHandle(
                remoteStateHandle.getBackendIdentifier(),
                remoteStateHandle.getCheckpointId(),
                new DirectoryStateHandle(secondaryDBMeta.dbPath),
                remoteStateHandle.getKeyGroupRange(),
                remoteStateHandle.getMetaStateHandle(),
                remoteStateHandle.getSharedState().keySet());
    }

    private SecondaryDBMeta restoreDBInstanceFromStateHandle(
            KeyedStateHandle restoreStateHandle) throws Exception {

        KeyGroupRange keyGroupRangeForSecondaryDbInstance = restoreStateHandle.getKeyGroupRange();
        Path secondaryDbPath;
        KeyedBackendSerializationProxy<K> serializationProxy;
        if (restoreStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
            IncrementalRemoteKeyedStateHandle remoteStateHandle = (IncrementalRemoteKeyedStateHandle) restoreStateHandle;
            String secondaryDbDir =
                    String.format(
                            SECONDARY_DB_DIR_FORMAT,
                            keyGroupRangeForSecondaryDbInstance.getStartKeyGroup(),
                            keyGroupRangeForSecondaryDbInstance.getEndKeyGroup());
            secondaryDbPath =
                    new File(instanceBasePath, secondaryDbDir).getAbsoluteFile().toPath();

            try (RocksDBStateDownloader rocksDBStateDownloader =
                         new RocksDBStateDownloader(numberOfTransferringThreads)) {
                rocksDBStateDownloader.transferAllStateDataToDirectory(
                        remoteStateHandle, secondaryDbPath, cancelStreamRegistry);
            }
            serializationProxy = readMetaData(remoteStateHandle.getMetaStateHandle());
        } else if (restoreStateHandle instanceof IncrementalLocalKeyedStateHandle) {
            IncrementalLocalKeyedStateHandle localStateHandle = (IncrementalLocalKeyedStateHandle) restoreStateHandle;
            secondaryDbPath = localStateHandle.getDirectoryStateHandle().getDirectory();
            serializationProxy = readMetaData(localStateHandle.getMetaDataState());
        } else {
            throw new Exception("Restore state handle type for secondary db is illegal");
        }

        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

        List<ColumnFamilyHandle> columnFamilyHandles =
                new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

        ColumnFamilyOptions columnFamilyOptions =
                RocksDBOperationUtils.createColumnFamilyOptions(
                        x -> PredefinedOptions.COMPACTION_DISABLED.createColumnOptions(), "default");

        RocksDB restoreDb =
                RocksDBOperationUtils.openDB(
                        secondaryDbPath.toString(),
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        columnFamilyOptions,
                        dbOptions);

        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            ColumnFamilyMetaData cfmd = restoreDb.getColumnFamilyMetaData(columnFamilyHandle);
            for (int level = 0; level < cfmd.levels().size(); level++) {
                List<SstFileMetaData> levelFiles = cfmd.levels().get(level).files();
                for (SstFileMetaData file : levelFiles) {
                    if (file.beingCompacted()) {
                        throw new RocksDBException("file " + new FlinkSstFileMetaData(file) + " is begin compaction!");
                    }
                }
            }
        }

        return new SecondaryDBMeta(
                secondaryDbPath,
                keyGroupRangeForSecondaryDbInstance,
                restoreStateHandle,
                restoreDb,
                columnFamilyHandles,
                columnFamilyDescriptors,
                stateMetaInfoSnapshots);
    }

    private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots, boolean registerTtlCompactFilter) {

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>(stateMetaInfoSnapshots.size());

        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
            RegisteredStateMetaInfoBase metaInfoBase =
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
            ColumnFamilyDescriptor columnFamilyDescriptor =
                    RocksDBOperationUtils.createColumnFamilyDescriptor(
                            metaInfoBase,
                            columnFamilyOptionsFactory,
                            registerTtlCompactFilter ? ttlCompactFiltersManager : null);
            columnFamilyDescriptors.add(columnFamilyDescriptor);
        }
        return columnFamilyDescriptors;
    }

    private class SecondaryDBMeta implements Comparable<SecondaryDBMeta> {
        private final Path dbPath;
        private final KeyGroupRange keyGroupRange;
        private final KeyedStateHandle stateHandle;
        private boolean hasIngested = false;

        @Nonnull private final RocksDB db;

        @Nonnull private final ColumnFamilyHandle defaultColumnFamilyHandle;

        @Nonnull private final List<ColumnFamilyHandle> columnFamilyHandles;

        @Nonnull private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

        @Nonnull private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        private Map<String, List<List<FlinkSstFileMetaData>>> stateNameToFiles;

        public SecondaryDBMeta(
                Path dbPath,
                KeyGroupRange keyGroupRange,
                KeyedStateHandle stateHandle,
                @Nonnull RocksDB db,
                @Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
                @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

            this.dbPath = dbPath;
            this.keyGroupRange = keyGroupRange;
            this.stateHandle = stateHandle;
            this.db = db;
            this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
            this.columnFamilyHandles = columnFamilyHandles;
            this.columnFamilyDescriptors = columnFamilyDescriptors;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.stateNameToFiles = new HashMap<>(columnFamilyHandles.size());
        }

        @Override
        public int compareTo(SecondaryDBMeta o) {
            return keyGroupRange.getStartKeyGroup() - o.keyGroupRange.getStartKeyGroup();
        }

        @Override
        public String toString() {
            return "SecondaryDBMeta{"
                    + "dbPath="
                    + dbPath
                    + ", keyGroupRange="
                    + keyGroupRange
                    + '}';
        }

        public void compactionL0() throws RocksDBException {
            for (int i = 0; i < columnFamilyHandles.size(); i++) {
                ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
                StateMetaInfoSnapshot metaInfo = stateMetaInfoSnapshots.get(i);
                String stateName = metaInfo.getName();

                ColumnFamilyMetaData cfmd = db.getColumnFamilyMetaData(columnFamilyHandle);
                List<FlinkSstFileMetaData> l0Files = new ArrayList<>();

                if (cfmd.levels().size() == 0) {
                    return;
                } else {
                    for (SstFileMetaData file : cfmd.levels().get(0).files()) {
                        l0Files.add(new FlinkSstFileMetaData(file));
                    }
                }

                if (!l0Files.isEmpty()) {
                    CompactionOptions compactionOptions = new CompactionOptions();
                    compactionOptions.setOutputFileSizeLimit(64 * 1024 * 1024L);
                    List<String> l0FileNames =
                            l0Files.stream()
                                    .map(FlinkSstFileMetaData::fileName)
                                    .collect(Collectors.toList());
                    CompactionJobInfo compactionJobInfo = new CompactionJobInfo();
                    long compactionL0StartTs = System.currentTimeMillis();
                    db.compactFiles(
                            compactionOptions,
                            columnFamilyHandle,
                            l0FileNames,
                            1,
                            0,
                            compactionJobInfo);
                }

                cfmd = db.getColumnFamilyMetaData(columnFamilyHandle);
                List<List<FlinkSstFileMetaData>> files = new ArrayList<>();
                stateNameToFiles.put(stateName, files);
                for (int level = 0; level < cfmd.levels().size(); level++) {
                    List<SstFileMetaData> levelFiles = cfmd.levels().get(level).files();
                    List<FlinkSstFileMetaData> flinkLevelFiles = new ArrayList<>();
                    for (SstFileMetaData levelFile : levelFiles) {
                        flinkLevelFiles.add(new FlinkSstFileMetaData(levelFile));
                    }
                    files.add(flinkLevelFiles);
                }
            }
        }

        List<List<FlinkSstFileMetaData>> getStateFiles(String stateName) {
            return stateNameToFiles.get(stateName);
        }

        public void close() {
            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(columnFamilyDescriptors.size() + 1);
            columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
            RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeAllQuietly(columnFamilyHandles);
            IOUtils.closeQuietly(db);
            IOUtils.closeAllQuietly(columnFamilyOptions);
        }
    }

    private class IngestionContext {
        private final String stateName;
        private final List<List<FlinkSstFileMetaData>> ingestFiles;

        public IngestionContext(String stateName) {
            this.stateName = stateName;
            ingestFiles = new ArrayList<>();
            for (int i = 0; i < 7; i++) {
                ingestFiles.add(new ArrayList<>());
            }
        }

        public void addFiles(List<List<FlinkSstFileMetaData>> files) {
            Preconditions.checkArgument(files.get(0).isEmpty(), "L0 should not have files!");
            for (int level = 2; level < files.size(); level++) {
                ingestFiles.get(level).addAll(files.get(level));
            }
            Collections.reverse(files.get(1));
            ingestFiles.get(1).addAll(files.get(1));
        }
    }

    private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle)
            throws Exception {

        InputStream inputStream = null;

        try {
            inputStream = metaStateHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(inputStream);
            DataInputView in = new DataInputViewStreamWrapper(inputStream);
            return readMetaData(in);
        } finally {
            if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
                inputStream.close();
            }
        }
    }

    private class L0CompactionJob implements Callable<Boolean> {
        private final SecondaryDBMeta secondaryDbInstance;

        public L0CompactionJob(SecondaryDBMeta secondaryDbInstance) {
            this.secondaryDbInstance = secondaryDbInstance;
        }

        @Override
        public Boolean call() throws Exception {
            secondaryDbInstance.compactionL0();
            return true;
        }
    }
}
