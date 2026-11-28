/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.SerializableSupplier;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link FlinkGlobalAggregatedCommitterOperator}. */
class FlinkGlobalAggregatedCommitterOperatorTest {

    @Test
    void testLifecycleWithCheckpointRestoreAndCommit() throws Exception {
        // First lifecycle: no restore, only snapshot
        SinkAggregatedCommitter<String, String> delegate1 = mock(SinkAggregatedCommitter.class);
        when(delegate1.combine(any()))
                .thenAnswer(
                        invocation -> {
                            List<String> strings = invocation.getArgument(0);
                            return String.join("|", strings);
                        });

        FlinkGlobalAggregatedCommitterOperator<String, String> operator1 =
                createOperator(() -> delegate1);
        OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                harness1 = createHarness(operator1);
        OperatorSubtaskState snapshot0;
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                ignored = harness1) {
            harness1.initializeEmptyState();
            harness1.open();

            // Process checkpoint 0: summary from subtask 0, lineage from subtask 0
            harness1.processElement(new CommittableSummary<>(0, 2, 0L, 1, 1, 0), 0);
            harness1.processElement(
                    new CommittableWithLineage<>(new CheckpointCommitWrapper<>("oldA", 0L), 0L, 0),
                    0);

            // No commits, no restore, no close yet
            verify(delegate1, never()).commit(any());
            verify(delegate1, never()).restoreCommit(any());
            verify(delegate1, never()).close();

            // Snapshot 0
            snapshot0 = harness1.snapshot(0L, 0L);
            assertNotNull(snapshot0);

            // Close first harness and operator
        }
        // The operator close should invoke delegate.close exactly once
        verify(delegate1, times(1)).close();

        // Second lifecycle: restore from snapshot0, then checkpoint 1 commit
        SinkAggregatedCommitter<String, String> delegate2 = mock(SinkAggregatedCommitter.class);
        when(delegate2.combine(any()))
                .thenAnswer(
                        invocation -> {
                            List<String> strings = invocation.getArgument(0);
                            return String.join("|", strings);
                        });

        FlinkGlobalAggregatedCommitterOperator<String, String> operator2 =
                createOperator(() -> delegate2);
        OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                harness2 = createHarness(operator2);

        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                ignored = harness2) {
            // Initialize state with snapshot0 (restored checkpoint id = 0)
            harness2.initializeState(snapshot0);
            harness2.open();

            // Process remaining elements from checkpoint 0 (summary from subtask 1, lineage from
            // subtask 1)
            harness2.processElement(new CommittableSummary<>(1, 2, 0L, 1, 1, 0), 0);
            harness2.processElement(
                    new CommittableWithLineage<>(new CheckpointCommitWrapper<>("oldB", 0L), 0L, 1),
                    0);

            // Verify restore called with combined old elements "oldA|oldB"
            verify(delegate2).restoreCommit(Collections.singletonList("oldA|oldB"));
            verify(delegate2, never()).commit(any());

            // Process checkpoint 1 elements
            harness2.processElement(new CommittableSummary<>(0, 2, 1L, 1, 1, 0), 0);
            harness2.processElement(
                    new CommittableWithLineage<>(new CheckpointCommitWrapper<>("newA", 1L), 1L, 0),
                    0);
            harness2.processElement(new CommittableSummary<>(1, 2, 1L, 1, 1, 0), 0);
            harness2.processElement(
                    new CommittableWithLineage<>(new CheckpointCommitWrapper<>("newB", 1L), 1L, 1),
                    0);

            // Verify commit called with combined new elements "newA|newB"
            verify(delegate2).commit(Collections.singletonList("newA|newB"));

            // Verify restore called only once and only with old elements
            verify(delegate2, times(1)).restoreCommit(any());
            verify(delegate2, never()).restoreCommit(Collections.singletonList("newA|newB"));

            // Verify init and close on delegate2
            verify(delegate2, times(1)).init();
        }
        verify(delegate2, times(1)).close();
    }

    private FlinkGlobalAggregatedCommitterOperator<String, String> createOperator(
            SerializableSupplier<SinkAggregatedCommitter<String, String>> delegateSupplier) {
        return new FlinkGlobalAggregatedCommitterOperator<>(
                delegateSupplier,
                () -> new CheckpointCommitWrapperSerializer<>(new DefaultSerializer<>()));
    }

    private OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
            createHarness(FlinkGlobalAggregatedCommitterOperator<String, String> operator)
                    throws Exception {
        OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup();
        return harness;
    }
}
