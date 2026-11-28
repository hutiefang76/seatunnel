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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.GlobalCommitterOperator;
import org.apache.flink.util.function.SerializableSupplier;

/**
 * A {@link GlobalCommitterOperator} that wraps SeaTunnel's {@link SinkAggregatedCommitter}.
 *
 * <p>This operator ensures that the underlying {@link SinkAggregatedCommitter} is created exactly
 * once per operator instance and is properly closed when the operator closes. It also restores the
 * checkpoint ID before state initialization to support recovery.
 *
 * @param <C> The type of commit information.
 * @param <G> The type of global commit information.
 */
final class FlinkGlobalAggregatedCommitterOperator<C, G>
        extends GlobalCommitterOperator<CommitWrapper<C>, G> {

    private static final long serialVersionUID = 1L;

    private final AggregatedCommitterFactory<C, G> factory;

    /**
     * Constructs a new operator.
     *
     * @param aggregatedCommitterSupplier supplier for the {@link SinkAggregatedCommitter}
     * @param committableSerializerSupplier supplier for the serializer of {@link CommitWrapper}
     */
    FlinkGlobalAggregatedCommitterOperator(
            SerializableSupplier<SinkAggregatedCommitter<C, G>> aggregatedCommitterSupplier,
            SerializableSupplier<SimpleVersionedSerializer<CommitWrapper<C>>>
                    committableSerializerSupplier) {
        this(
                new AggregatedCommitterFactory<>(aggregatedCommitterSupplier),
                committableSerializerSupplier);
    }

    private FlinkGlobalAggregatedCommitterOperator(
            AggregatedCommitterFactory<C, G> factory,
            SerializableSupplier<SimpleVersionedSerializer<CommitWrapper<C>>> serializer) {
        super(factory, serializer, true);
        this.factory = factory;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        // Set the restored checkpoint ID on the actual committer before calling super.
        factory.setRestoredCheckpointId(context.getRestoredCheckpointId());
        super.initializeState(context);
    }

    @Override
    public void close() throws Exception {
        Exception firstException = null;
        Committer<CommitWrapper<C>> committer = factory.committer;
        if (committer != null) {
            try {
                committer.close();
            } catch (Exception e) {
                firstException = e;
            }
        }
        try {
            super.close();
        } catch (Exception e) {
            if (firstException == null) {
                firstException = e;
            } else {
                firstException.addSuppressed(e);
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * A factory that creates and holds a single instance of {@link FlinkSimpleAggregatedCommitter}.
     *
     * <p>This factory is serializable, but after deserialization it will create a new instance when
     * requested. However, it is guaranteed that {@link #get()} is called only once by the Flink
     * runtime ({@link GlobalCommitterOperator#setup}).
     *
     * @param <C> commit type
     * @param <G> global commit type
     */
    private static class AggregatedCommitterFactory<C, G>
            implements SerializableSupplier<Committer<CommitWrapper<C>>> {

        private static final long serialVersionUID = 1L;

        private final SerializableSupplier<SinkAggregatedCommitter<C, G>> supplier;
        private transient FlinkSimpleAggregatedCommitter<C, G> committer;

        AggregatedCommitterFactory(SerializableSupplier<SinkAggregatedCommitter<C, G>> supplier) {
            this.supplier = supplier;
        }

        @Override
        public Committer<CommitWrapper<C>> get() {
            if (committer == null) {
                committer = new FlinkSimpleAggregatedCommitter<>(supplier.get());
            }
            return committer;
        }

        void setRestoredCheckpointId(java.util.OptionalLong restoredCheckpointId) {
            if (committer != null) {
                committer.setRestoredCheckpointId(restoredCheckpointId);
            }
        }
    }
}
