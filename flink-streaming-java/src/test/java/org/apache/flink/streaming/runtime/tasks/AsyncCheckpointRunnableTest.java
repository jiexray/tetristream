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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/** Tests for {@link AsyncCheckpointRunnable}. */
public class AsyncCheckpointRunnableTest {

    @Test
    public void testDeclineWithAsyncCheckpointExceptionWhenRunning() {
        testAsyncCheckpointException(true);
    }

    @Test
    public void testDeclineWithAsyncCheckpointExceptionWhenNotRunning() {
        testAsyncCheckpointException(false);
    }

    private void testAsyncCheckpointException(boolean isTaskRunning) {
        final Map<OperatorID, OperatorSnapshotFutures> snapshotsInProgress = new HashMap<>();
        snapshotsInProgress.put(
                new OperatorID(),
                new OperatorSnapshotFutures(
                        ExceptionallyDoneFuture.of(
                                new RuntimeException("Async Checkpoint Exception")),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty())));

        final TestEnvironment environment = new TestEnvironment();
        final AsyncCheckpointRunnable runnable =
                createAsyncRunnable(snapshotsInProgress, environment, isTaskRunning);
        runnable.run();

        if (isTaskRunning) {
            Assert.assertTrue(environment.getCause() instanceof CheckpointException);
            Assert.assertSame(
                    ((CheckpointException) environment.getCause()).getCheckpointFailureReason(),
                    CheckpointFailureReason.CHECKPOINT_ASYNC_EXCEPTION);
        } else {
            Assert.assertNull(environment.getCause());
        }
    }

    @Test
    public void testDeclineAsyncCheckpoint() {
        CheckpointFailureReason originalReason =
                CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM;

        final Map<OperatorID, OperatorSnapshotFutures> snapshotsInProgress = new HashMap<>();
        snapshotsInProgress.put(
                new OperatorID(),
                new OperatorSnapshotFutures(
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        DoneFuture.of(SnapshotResult.empty()),
                        ExceptionallyDoneFuture.of(new CheckpointException(originalReason)),
                        DoneFuture.of(SnapshotResult.empty())));

        final TestEnvironment environment = new TestEnvironment();
        final AsyncCheckpointRunnable runnable =
                createAsyncRunnable(snapshotsInProgress, environment, true);
        runnable.run();

        Assert.assertSame(
                ((CheckpointException) environment.getCause()).getCheckpointFailureReason(),
                originalReason);
    }

    private AsyncCheckpointRunnable createAsyncRunnable(
            Map<OperatorID, OperatorSnapshotFutures> snapshotsInProgress,
            TestEnvironment environment,
            boolean isTaskRunning) {
        return new AsyncCheckpointRunnable(
                snapshotsInProgress,
                new CheckpointMetaData(1, 1L),
                new CheckpointMetrics(),
                1L,
                "Task Name",
                r -> {},
                r -> {},
                environment,
                (msg, ex) -> {},
                () -> isTaskRunning);
    }

    private static class TestEnvironment extends StreamMockEnvironment {

        Throwable cause = null;

        TestEnvironment() {
            this(
                    new Configuration(),
                    new Configuration(),
                    new ExecutionConfig(),
                    1L,
                    new MockInputSplitProvider(),
                    1,
                    new TestTaskStateManager());
        }

        TestEnvironment(
                Configuration jobConfig,
                Configuration taskConfig,
                ExecutionConfig executionConfig,
                long memorySize,
                MockInputSplitProvider inputSplitProvider,
                int bufferSize,
                TaskStateManager taskStateManager) {
            super(
                    jobConfig,
                    taskConfig,
                    executionConfig,
                    memorySize,
                    inputSplitProvider,
                    bufferSize,
                    taskStateManager);
        }

        @Override
        public void declineCheckpoint(long checkpointId, Throwable cause) {
            this.cause = cause;
        }

        Throwable getCause() {
            return cause;
        }
    }
}
