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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlinkSimpleAggregatedCommitterTest {
    @Test
    void testLegacyRestoredCommittableUsesRecoveryCommit() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        when(delegate.combine(Collections.singletonList("prepared"))).thenReturn("batch");
        Committer.CommitRequest<CommitWrapper<String>> request =
                mock(Committer.CommitRequest.class);
        when(request.getCommittable()).thenReturn(new CommitWrapper<>("prepared"));

        FlinkSimpleAggregatedCommitter<String, String> committer =
                new FlinkSimpleAggregatedCommitter<>(delegate, OptionalLong.of(8));
        committer.commit(Collections.singletonList(request));

        verify(delegate).restoreCommit(Collections.singletonList("batch"));
        verify(delegate, never()).commit(anyList());
        verify(request).signalAlreadyCommitted();
        committer.close();
    }

    @Test
    void testRestoredAndFreshRequestsAreSeparatedBeforeCombine() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        when(delegate.combine(Collections.singletonList("old"))).thenReturn("old-batch");
        when(delegate.combine(Collections.singletonList("new"))).thenReturn("new-batch");
        Committer.CommitRequest<CommitWrapper<String>> old = request("old", 8);
        Committer.CommitRequest<CommitWrapper<String>> fresh = request("new", 9);
        FlinkSimpleAggregatedCommitter<String, String> committer =
                new FlinkSimpleAggregatedCommitter<>(delegate, OptionalLong.of(8));
        committer.commit(Arrays.asList(fresh, old));
        InOrder order = inOrder(delegate);
        order.verify(delegate).restoreCommit(Collections.singletonList("old-batch"));
        order.verify(delegate).commit(Collections.singletonList("new-batch"));
        verify(old).signalAlreadyCommitted();
        verify(fresh).signalAlreadyCommitted();
        committer.close();
    }

    @Test
    void testNewCommitFailureIsNeverReconciledOrAcknowledged() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        when(delegate.combine(Collections.singletonList("new"))).thenReturn("batch");
        doThrow(new IOException("unknown XID")).when(delegate).commit(anyList());
        Committer.CommitRequest<CommitWrapper<String>> fresh = request("new", 9);
        FlinkSimpleAggregatedCommitter<String, String> committer =
                new FlinkSimpleAggregatedCommitter<>(delegate, OptionalLong.of(8));
        Assertions.assertThrows(
                IOException.class, () -> committer.commit(Collections.singletonList(fresh)));
        verify(delegate, never()).restoreCommit(anyList());
        verify(fresh, never()).signalAlreadyCommitted();
        verify(fresh).signalFailedWithKnownReason(any());
        committer.close();
    }

    @Test
    void testOutstandingRecoveryCommitFailsWithoutAcknowledging() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        when(delegate.combine(Collections.singletonList("old"))).thenReturn("batch");
        when(delegate.restoreCommit(anyList())).thenReturn(Collections.singletonList("pending"));
        Committer.CommitRequest<CommitWrapper<String>> old = request("old", 8);
        FlinkSimpleAggregatedCommitter<String, String> committer =
                new FlinkSimpleAggregatedCommitter<>(delegate, OptionalLong.of(8));
        Assertions.assertThrows(
                IOException.class, () -> committer.commit(Collections.singletonList(old)));
        verify(old, never()).signalAlreadyCommitted();
        committer.close();
    }

    @Test
    void testFreshWriterAfterRestoreStartsAboveRestoredBoundary() throws Exception {
        SinkWriter delegate = mock(SinkWriter.class);
        WriterInitContext init = mock(WriterInitContext.class);
        when(init.getRestoredCheckpointId()).thenReturn(OptionalLong.of(100));
        SinkWriter.Context context = mock(SinkWriter.Context.class, RETURNS_DEEP_STUBS);
        when(delegate.prepareCommit(101)).thenReturn(Optional.of("prepared"));
        FlinkSinkWriter<String, String> writer = new FlinkSinkWriter<>(delegate, init, context);
        CommitWrapper<String> wrapper = writer.prepareCommit().iterator().next();
        Assertions.assertEquals(101, ((CheckpointCommitWrapper<String>) wrapper).getCheckpointId());
        verify(delegate).prepareCommit(101);
        writer.close();
    }

    private Committer.CommitRequest<CommitWrapper<String>> request(String value, long checkpoint) {
        Committer.CommitRequest<CommitWrapper<String>> request =
                mock(Committer.CommitRequest.class);
        when(request.getCommittable()).thenReturn(new CheckpointCommitWrapper<>(value, checkpoint));
        return request;
    }

    @Test
    void testAggregatedSinkCreatesNoOpLocalCommitter() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        SeaTunnelSink sink = mock(SeaTunnelSink.class);
        when(sink.createAggregatedCommitter()).thenReturn(Optional.of(delegate));
        when(sink.createCommitter()).thenReturn(Optional.empty());
        CommitterInitContext context = mock(CommitterInitContext.class);
        when(context.getRestoredCheckpointId()).thenReturn(OptionalLong.empty());
        Committer<CommitWrapper<String>> committer =
                new FlinkSink<String, String, String>(sink, Collections.emptyList(), 1)
                        .createCommitter(context);

        // Verify no-op behavior by signaling already committed on a dummy request
        Committer.CommitRequest<CommitWrapper<String>> request =
                mock(Committer.CommitRequest.class);
        when(request.getCommittable()).thenReturn(new CommitWrapper<>("dummy"));
        committer.commit(Collections.singletonList(request));
        verify(request).signalAlreadyCommitted();
        verify(delegate, never()).commit(anyList());
        verify(delegate, never()).init();
        committer.close();
    }

    @Test
    void testAggregatedSinkUsesOneGlobalCommitterWithParallelWriters() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        SeaTunnelSink sink = mock(SeaTunnelSink.class);
        when(sink.createAggregatedCommitter()).thenReturn(Optional.of(delegate));
        when(sink.createCommitter()).thenReturn(Optional.empty());
        when(sink.getCommitInfoSerializer())
                .thenReturn(
                        Optional.of(
                                new org.apache.seatunnel.api.serialization.DefaultSerializer<>()));
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env =
                org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                        .getExecutionEnvironment();
        env.setParallelism(2);
        env.fromElements(new org.apache.seatunnel.api.table.type.SeaTunnelRow(new Object[] {1}))
                .sinkTo(new FlinkSink<String, String, String>(sink, Collections.emptyList(), 2))
                .setParallelism(2);
        java.util.Collection<org.apache.flink.streaming.api.graph.StreamNode> nodes =
                env.getStreamGraph().getStreamNodes();
        org.apache.flink.streaming.api.graph.StreamNode global =
                nodes.stream()
                        .filter(node -> node.getOperatorName().contains("Global Committer"))
                        .findFirst()
                        .orElseThrow(
                                () -> new AssertionError("Missing global aggregated committer"));
        Assertions.assertEquals(1, global.getParallelism());
        Assertions.assertTrue(
                nodes.stream()
                        .anyMatch(
                                node ->
                                        node.getOperatorName().contains("Writer")
                                                && node.getParallelism() == 2));
    }

    @Test
    void testNewJobUsesStrictCommit() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        when(delegate.combine(Collections.singletonList("prepared"))).thenReturn("batch");
        Committer.CommitRequest<CommitWrapper<String>> request =
                mock(Committer.CommitRequest.class);
        when(request.getCommittable()).thenReturn(new CommitWrapper<>("prepared"));
        FlinkSimpleAggregatedCommitter<String, String> committer =
                new FlinkSimpleAggregatedCommitter<>(delegate);

        committer.commit(Collections.singletonList(request));

        verify(delegate).commit(Collections.singletonList("batch"));
        verify(delegate, never()).restoreCommit(anyList());
        committer.close();
    }
}
