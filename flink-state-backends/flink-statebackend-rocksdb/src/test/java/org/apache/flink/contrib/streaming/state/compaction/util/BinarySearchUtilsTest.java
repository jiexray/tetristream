/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.compaction.util;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;

public class BinarySearchUtilsTest {
    @Test
    public void testDefaultComparator() {
        InternalKey targetKey1 =
                new InternalKey(String.format("key-%08d", 109).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey2 =
                new InternalKey(String.format("key-%08d", 111).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey3 =
                new InternalKey(String.format("key-%08d", 111).getBytes(StandardCharsets.UTF_8), 1);
        Comparator<InternalKey> cmp = DefaultComparator.getInstance();
        Assert.assertEquals(cmp.compare(targetKey1, targetKey2), -1);
        Assert.assertEquals(cmp.compare(targetKey2, targetKey1), 1);
        Assert.assertEquals(cmp.compare(targetKey2, targetKey2), 0);
        Assert.assertEquals(cmp.compare(targetKey2, targetKey3), 1);
    }

    @Test
    public void testLowerBoundWithinInterval() {
        List<FlinkSstFileMetaData> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(
                    new FlinkSstFileMetaData(
                            String.format("file-%d", i),
                            String.format("path-%d", i),
                            i,
                            0,
                            0,
                            String.format("key-%08d", 3 * i).getBytes(StandardCharsets.UTF_8),
                            String.format("key-%08d", 3 * i + 2).getBytes(StandardCharsets.UTF_8),
                            10,
                            false,
                            10,
                            0));
        }

        Comparator<InternalKey> defaultCmp = DefaultComparator.getInstance();

        InternalKey targetKey1 =
                new InternalKey(String.format("key-%08d", 10).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey2 =
                new InternalKey(
                        String.format("key-%08d", 1000).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey3 =
                new InternalKey(String.format("key-%08d", 0).getBytes(StandardCharsets.UTF_8), 0);

        BiFunction<FlinkSstFileMetaData, InternalKey, Boolean> cmp =
                (file, key) -> {
                    InternalKey fileKey = new InternalKey(file.largestKey(), file.largestSeqno());
                    return defaultCmp.compare(fileKey, key) < 0;
                };

        int startIndex = BinarySearchUtils.lowerBound(files, 0, files.size(), targetKey1, cmp);
        Assert.assertEquals(startIndex, 3);
        startIndex = BinarySearchUtils.lowerBound(files, 0, files.size(), targetKey2, cmp);
        Assert.assertEquals(startIndex, 10);
        startIndex = BinarySearchUtils.lowerBound(files, 0, files.size(), targetKey3, cmp);
        Assert.assertEquals(startIndex, 0);
    }

    @Test
    public void testLowerBoundWithinInterval2() {
        List<FlinkSstFileMetaData> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(
                    new FlinkSstFileMetaData(
                            String.format("file-%d", i),
                            String.format("path-%d", i),
                            i,
                            0,
                            0,
                            String.format("key-%08d", 1).getBytes(StandardCharsets.UTF_8),
                            String.format("key-%08d", 1).getBytes(StandardCharsets.UTF_8),
                            10,
                            false,
                            10,
                            0));
        }

        Comparator<InternalKey> defaultCmp = DefaultComparator.getInstance();

        InternalKey targetKey1 =
                new InternalKey(String.format("key-%08d", 1).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey2 =
                new InternalKey(String.format("key-%08d", 2).getBytes(StandardCharsets.UTF_8), 0);

        BiFunction<FlinkSstFileMetaData, InternalKey, Boolean> cmp =
                (file, key) -> {
                    InternalKey fileKey = new InternalKey(file.smallestKey(), file.smallestSeqno());
                    return defaultCmp.compare(fileKey, key) < 0;
                };

        int startIndex = BinarySearchUtils.lowerBound(files, 0, files.size(), targetKey1, cmp);
        Assert.assertEquals(startIndex, 0);
        startIndex = BinarySearchUtils.lowerBound(files, 0, files.size(), targetKey2, cmp);
        Assert.assertEquals(startIndex, 10);
    }

    @Test
    public void testUpperBoundWithinInterval1() {
        List<FlinkSstFileMetaData> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(
                    new FlinkSstFileMetaData(
                            String.format("file-%d", i),
                            String.format("path-%d", i),
                            i,
                            0,
                            0,
                            String.format("key-%08d", 3 * i).getBytes(StandardCharsets.UTF_8),
                            String.format("key-%08d", 3 * i + 2).getBytes(StandardCharsets.UTF_8),
                            10,
                            false,
                            10,
                            0));
        }

        Comparator<InternalKey> defaultCmp = DefaultComparator.getInstance();

        InternalKey targetKey1 =
                new InternalKey(String.format("key-%08d", 10).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey2 =
                new InternalKey(
                        String.format("key-%08d", 1000).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey3 =
                new InternalKey(String.format("key-%08d", 0).getBytes(StandardCharsets.UTF_8), 0);

        BiFunction<FlinkSstFileMetaData, InternalKey, Boolean> cmp =
                (file, key) -> {
                    InternalKey fileKey = new InternalKey(file.largestKey(), file.largestSeqno());
                    return defaultCmp.compare(key, fileKey) < 0;
                };

        int endIndex = BinarySearchUtils.upperBound(files, 0, files.size(), targetKey1, cmp);
        Assert.assertEquals(endIndex, 3);
        endIndex = BinarySearchUtils.upperBound(files, 0, files.size(), targetKey2, cmp);
        Assert.assertEquals(endIndex, 10);
        endIndex = BinarySearchUtils.upperBound(files, 0, files.size(), targetKey3, cmp);
        Assert.assertEquals(endIndex, 0);
    }

    @Test
    public void testUpperBoundWithinInterval2() {
        List<FlinkSstFileMetaData> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(
                    new FlinkSstFileMetaData(
                            String.format("file-%d", i),
                            String.format("path-%d", i),
                            i,
                            0,
                            0,
                            String.format("key-%08d", 1).getBytes(StandardCharsets.UTF_8),
                            String.format("key-%08d", 1).getBytes(StandardCharsets.UTF_8),
                            10,
                            false,
                            10,
                            0));
        }

        Comparator<InternalKey> defaultCmp = DefaultComparator.getInstance();

        InternalKey targetKey1 =
                new InternalKey(String.format("key-%08d", 1).getBytes(StandardCharsets.UTF_8), 0);
        InternalKey targetKey2 =
                new InternalKey(String.format("key-%08d", 0).getBytes(StandardCharsets.UTF_8), 0);

        BiFunction<FlinkSstFileMetaData, InternalKey, Boolean> cmp =
                (file, key) -> {
                    InternalKey fileKey = new InternalKey(file.smallestKey(), file.smallestSeqno());
                    return defaultCmp.compare(key, fileKey) < 0;
                };

        int startIndex = BinarySearchUtils.upperBound(files, 0, files.size(), targetKey1, cmp);
        Assert.assertEquals(startIndex, 10);
        startIndex = BinarySearchUtils.upperBound(files, 0, files.size(), targetKey2, cmp);
        Assert.assertEquals(startIndex, 0);
    }
}
