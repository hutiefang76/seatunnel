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

package org.apache.seatunnel.e2e.connector.hive;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.commit.HiveSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreCatalog;
import org.apache.seatunnel.translation.flink.sink.FlinkSink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration test that executes actual Flink jobs with the real Hive sink committer on local
 * filesystem. The fixture writes UTF-8 files through parallel writers and commits them through the
 * production Flink adapter and Hive committer. HiveMetaStoreCatalog is mocked; Hive writer formats
 * and a running Metastore are outside this fixture.
 */
public class HiveFlinkOverwriteIT {

    /** Shared latch for deterministic publish order across writers in a test case. */
    private static volatile CountDownLatch publishLatch;

    /**
     * Test sink that writes UTF-8 staged files and uses the real HiveSinkAggregatedCommitter for
     * commit. Implements SeaTunnelSink so it can be adapted to Flink via FlinkSink.
     */
    private static class TestableSink
            implements SeaTunnelSink<
                    SeaTunnelRow, String, FileCommitInfo, FileAggregatedCommitInfo> {

        private final String root;
        private final boolean partitioned;

        TestableSink(Path root, boolean partitioned) {
            this.root = root.toString();
            this.partitioned = partitioned;
        }

        @Override
        public SinkWriter<SeaTunnelRow, FileCommitInfo, String> createWriter(
                SinkWriter.Context context) throws IOException {
            return new TestableWriter(context, Paths.get(root), partitioned);
        }

        @Override
        public Optional<Serializer<FileCommitInfo>> getCommitInfoSerializer() {
            return Optional.of(new DefaultSerializer<>());
        }

        @Override
        public Optional<Serializer<FileAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
            return Optional.of(new DefaultSerializer<>());
        }

        @Override
        public Optional<
                        org.apache.seatunnel.api.sink.SinkAggregatedCommitter<
                                FileCommitInfo, FileAggregatedCommitInfo>>
                createAggregatedCommitter() throws IOException {
            DeterministicPublishCommitter committer =
                    new DeterministicPublishCommitter(
                            createHiveConfig(), "db", "tbl", createLocalHadoopConf());
            try {
                Field field = HiveSinkAggregatedCommitter.class.getDeclaredField("hiveMetaStore");
                field.setAccessible(true);
                field.set(committer, Mockito.mock(HiveMetaStoreCatalog.class));
            } catch (ReflectiveOperationException error) {
                throw new IOException(error);
            }
            return Optional.of(committer);
        }

        @Override
        public String getPluginName() {
            return "TestableHiveSink";
        }
    }

    /** Writer that buffers rows and writes them to a staged file on prepareCommit. */
    private static class TestableWriter
            implements SinkWriter<SeaTunnelRow, FileCommitInfo, String> {

        private final SinkWriter.Context context;
        private final Path root;
        private final boolean partitioned;
        private final List<Integer> buffer = new ArrayList<>();
        private final Path transactionDir;

        TestableWriter(SinkWriter.Context context, Path root, boolean partitioned)
                throws IOException {
            this.context = context;
            this.root = root;
            this.partitioned = partitioned;
            this.transactionDir =
                    root.resolve(
                            "staging/"
                                    + UUID.randomUUID()
                                    + "/T_job_"
                                    + context.getIndexOfSubtask()
                                    + "_1");
            Files.createDirectories(transactionDir);
        }

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            Integer value = (Integer) element.getField(0);
            buffer.add(value);
        }

        @Override
        public Optional<FileCommitInfo> prepareCommit() throws IOException {
            return prepareCommit(1L);
        }

        @Override
        public Optional<FileCommitInfo> prepareCommit(long checkpointId) throws IOException {
            if (buffer.isEmpty()) {
                return Optional.empty();
            }

            Path targetBase = root.resolve(partitioned ? "warehouse/pt=1" : "warehouse");
            Path source = transactionDir.resolve("writer" + context.getIndexOfSubtask() + ".txt");
            Files.write(
                    source,
                    buffer.stream().map(String::valueOf).collect(Collectors.toList()),
                    StandardCharsets.UTF_8);

            LinkedHashMap<String, String> needMoveFiles = new LinkedHashMap<>();
            needMoveFiles.put(
                    source.toString(),
                    targetBase.resolve("writer" + context.getIndexOfSubtask() + ".txt").toString());

            LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>();
            if (partitioned) {
                partitionDirAndValuesMap.put("pt=1", Collections.singletonList("1"));
            }

            buffer.clear();
            return Optional.of(
                    new FileCommitInfo(
                            needMoveFiles, partitionDirAndValuesMap, transactionDir.toString()));
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {
            // Staged files are moved by the committer; nothing to close here.
        }
    }

    /**
     * HiveSinkAggregatedCommitter with deterministic publish order for the two writers. It ensures
     * writer 0 publishes first unless both commits arrive simultaneously.
     */
    private static class DeterministicPublishCommitter extends HiveSinkAggregatedCommitter {

        DeterministicPublishCommitter(
                ReadonlyConfig cfg, String dbName, String tableName, HadoopConf hadoopConf) {
            super(cfg, dbName, tableName, hadoopConf);
        }

        @Override
        public List<FileAggregatedCommitInfo> commit(
                List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
            if (publishLatch != null) {
                boolean containsWriter0 =
                        aggregatedCommitInfos.stream()
                                .anyMatch(
                                        info ->
                                                info.getTransactionMap().keySet().stream()
                                                        .anyMatch(dir -> dir.contains("T_job_0_")));
                boolean containsWriter1 =
                        aggregatedCommitInfos.stream()
                                .anyMatch(
                                        info ->
                                                info.getTransactionMap().keySet().stream()
                                                        .anyMatch(dir -> dir.contains("T_job_1_")));

                if (containsWriter0 && containsWriter1) {
                    // Both present: publish immediately (the commit itself is deterministic).
                    return super.commit(aggregatedCommitInfos);
                } else if (containsWriter1) {
                    // Only writer1: wait until writer0 has published.
                    try {
                        if (!publishLatch.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("Timeout waiting for writer0 publication");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted waiting for writer0", e);
                    }
                }
                // Writer0 alone or after latch: delegate normally.
                List<FileAggregatedCommitInfo> result = super.commit(aggregatedCommitInfos);
                if (containsWriter0 && !containsWriter1) {
                    publishLatch.countDown();
                }
                return result;
            }
            return super.commit(aggregatedCommitInfos);
        }
    }

    @Test
    void testFlinkJobWithOverwriteAndRealCommitter(@TempDir Path tempDir) throws Exception {
        for (boolean partitioned : new boolean[] {false, true}) {
            for (int parallelism : new int[] {1, 2}) {
                runSingleJob(
                        tempDir.resolve("p" + parallelism + "_" + partitioned),
                        parallelism,
                        partitioned);
            }
        }
    }

    private void runSingleJob(Path root, int parallelism, boolean partitioned) throws Exception {
        // Setup directories and old file to ensure overwrite deletes it.
        Path warehouse = root.resolve(partitioned ? "warehouse/pt=1" : "warehouse");
        Files.createDirectories(warehouse);
        Path oldFile = warehouse.resolve("old.txt");
        Files.writeString(oldFile, "old", StandardCharsets.UTF_8);

        publishLatch = new CountDownLatch(1);

        TestableSink sink = new TestableSink(root, partitioned);

        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("taskmanager.numberOfTaskSlots", "2");
        flinkConfig.setString("rest.bind-address", "127.0.0.1");
        flinkConfig.setInteger("rest.port", 0);
        flinkConfig.setString("jobmanager.bind-host", "127.0.0.1");
        flinkConfig.setString("taskmanager.bind-host", "127.0.0.1");
        flinkConfig.setString("jobmanager.rpc.address", "127.0.0.1");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(parallelism, flinkConfig);
        env.setParallelism(parallelism);
        env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);

        env.fromElements(new SeaTunnelRow(new Object[] {0}), new SeaTunnelRow(new Object[] {1}))
                .rebalance()
                .sinkTo(new FlinkSink<>(sink, Collections.emptyList(), parallelism))
                .setParallelism(parallelism);

        env.execute("HiveFlinkOverwriteIT");

        // Verify old file is gone and correct content is present.
        Assertions.assertFalse(Files.exists(oldFile), "Old file should be overwritten");
        List<Integer> values = new ArrayList<>();
        try (Stream<Path> files = Files.list(warehouse)) {
            files.filter(Files::isRegularFile)
                    .forEach(
                            file -> {
                                try {
                                    for (String line :
                                            Files.readAllLines(file, StandardCharsets.UTF_8)) {
                                        if (!line.trim().isEmpty()) {
                                            values.add(Integer.parseInt(line.trim()));
                                        }
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
        }
        Collections.sort(values);
        Assertions.assertEquals(List.of(0, 1), values, "Should contain exactly rows 0 and 1");

        // Ensure each writer output count matches parallelism if applicable.
        if (parallelism == 2) {
            Assertions.assertEquals(2, values.size(), "Two rows expected");
        }
    }

    private static ReadonlyConfig createHiveConfig() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put(HiveSinkOptions.METASTORE_URI.key(), "thrift://dummy:9083");
        map.put(HiveSinkOptions.HADOOP_CONF_PATH.key(), "/tmp");
        map.put(HiveSinkOptions.HIVE_SITE_PATH.key(), "/tmp/hive-site.xml");
        map.put(HiveSinkOptions.OVERWRITE.key(), true);
        return ReadonlyConfig.fromMap(map);
    }

    private static HadoopConf createLocalHadoopConf() {
        HadoopConf conf = new HadoopConf("file:///");
        LinkedHashMap<String, String> options = new LinkedHashMap<>();
        options.put("fs.file.impl.disable.cache", "true");
        conf.setExtraOptions(options);
        return conf;
    }
}
