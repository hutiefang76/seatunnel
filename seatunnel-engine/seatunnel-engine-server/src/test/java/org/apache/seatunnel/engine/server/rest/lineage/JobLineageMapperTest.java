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

package org.apache.seatunnel.engine.server.rest.lineage;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.VertexInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class JobLineageMapperTest {

    private final JobLineageMapper mapper =
            new JobLineageMapper(new JobLineageMapper.Limits(32, 64, 16, 128));

    @Test
    void mapsAndSortsTopologyDeterministically() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                3L,
                new VertexInfo(
                        3L,
                        PluginType.SINK,
                        "sink",
                        Arrays.asList(
                                TablePath.of("warehouse", "orders"),
                                TablePath.of("warehouse", "orders"))));
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "source",
                        Arrays.asList(
                                TablePath.of("sales", "orders"),
                                TablePath.of("sales", "customers"))));
        vertices.put(
                2L,
                new VertexInfo(
                        2L, PluginType.TRANSFORM, "transform", Collections.emptyList()));

        Map<Integer, List<Edge>> pipelineEdges = new LinkedHashMap<>();
        pipelineEdges.put(2, Collections.singletonList(new Edge(2L, 3L)));
        pipelineEdges.put(
                1,
                Arrays.asList(
                        new Edge(1L, 2L),
                        new Edge(1L, 2L)));

        JobLineageResponse response = mapper.map(dag(vertices, pipelineEdges));

        Assertions.assertEquals("42", response.getJobId());
        Assertions.assertEquals("EXECUTION", response.getGraphKind());
        Assertions.assertEquals("JOB", response.getIdScope());
        Assertions.assertEquals(Arrays.asList("1", "2", "3"),
                Arrays.asList(
                        response.getNodes().get(0).getId(),
                        response.getNodes().get(1).getId(),
                        response.getNodes().get(2).getId()));
        Assertions.assertEquals(
                Arrays.asList("sales.customers", "sales.orders"),
                response.getNodes().get(0).getTablePaths());
        Assertions.assertEquals(
                JobLineageResponse.DatasetMetadata.NOT_APPLICABLE,
                response.getNodes().get(1).getDatasetMetadata());
        Assertions.assertEquals(2, response.getEdges().size());
        Assertions.assertEquals(1, response.getEdges().get(0).getPipelineId());
        Assertions.assertEquals("1", response.getEdges().get(0).getSourceNodeId());
        Assertions.assertEquals("2", response.getEdges().get(0).getTargetNodeId());
    }

    @Test
    void reportsUnavailableDatasetMetadataWithoutDroppingTopology() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "source",
                        Arrays.asList(null, TablePath.DEFAULT)));
        vertices.put(
                2L,
                new VertexInfo(
                        2L,
                        PluginType.SINK,
                        "sink",
                        Collections.singletonList(TablePath.of("warehouse", "events"))));

        JobLineageResponse response =
                mapper.map(
                        dag(
                                vertices,
                                Collections.singletonMap(
                                        0, Collections.singletonList(new Edge(1L, 2L)))));

        Assertions.assertEquals(
                JobLineageResponse.DatasetMetadata.UNAVAILABLE,
                response.getNodes().get(0).getDatasetMetadata());
        Assertions.assertTrue(response.getNodes().get(0).getTablePaths().isEmpty());
        Assertions.assertEquals(1, response.getWarnings().size());
        Assertions.assertEquals(
                JobLineageResponse.WarningCode.DATASET_METADATA_UNAVAILABLE,
                response.getWarnings().get(0).getCode());
        Assertions.assertEquals("1", response.getWarnings().get(0).getNodeId());
    }

    @Test
    void rejectsMismatchedVertexIdentity() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        2L,
                        PluginType.SOURCE,
                        "source",
                        Collections.singletonList(TablePath.of("db", "table"))));

        JobLineageMapper.LineageMappingException error =
                Assertions.assertThrows(
                        JobLineageMapper.LineageMappingException.class,
                        () -> mapper.map(dag(vertices, Collections.emptyMap())));

        Assertions.assertEquals(
                JobLineageMapper.ErrorCode.INVALID_TOPOLOGY, error.getErrorCode());
    }

    @Test
    void rejectsDanglingEdge() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "source",
                        Collections.singletonList(TablePath.of("db", "table"))));

        JobLineageMapper.LineageMappingException error =
                Assertions.assertThrows(
                        JobLineageMapper.LineageMappingException.class,
                        () ->
                                mapper.map(
                                        dag(
                                                vertices,
                                                Collections.singletonMap(
                                                        0,
                                                        Collections.singletonList(
                                                                new Edge(1L, 99L))))));

        Assertions.assertEquals(
                JobLineageMapper.ErrorCode.INVALID_TOPOLOGY, error.getErrorCode());
    }

    @Test
    void rejectsCycleAcrossPipelines() {
        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "source",
                        Collections.singletonList(TablePath.of("db", "source"))));
        vertices.put(
                2L,
                new VertexInfo(
                        2L,
                        PluginType.SINK,
                        "sink",
                        Collections.singletonList(TablePath.of("db", "sink"))));

        Map<Integer, List<Edge>> pipelineEdges = new HashMap<>();
        pipelineEdges.put(0, Collections.singletonList(new Edge(1L, 2L)));
        pipelineEdges.put(1, Collections.singletonList(new Edge(2L, 1L)));

        JobLineageMapper.LineageMappingException error =
                Assertions.assertThrows(
                        JobLineageMapper.LineageMappingException.class,
                        () -> mapper.map(dag(vertices, pipelineEdges)));

        Assertions.assertEquals(
                JobLineageMapper.ErrorCode.INVALID_TOPOLOGY, error.getErrorCode());
    }

    @Test
    void enforcesUtf8StringLimit() {
        JobLineageMapper shortStringMapper =
                new JobLineageMapper(new JobLineageMapper.Limits(4, 4, 4, 5));

        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "中文",
                        Collections.singletonList(TablePath.of("db", "t"))));

        JobLineageMapper.LineageMappingException error =
                Assertions.assertThrows(
                        JobLineageMapper.LineageMappingException.class,
                        () -> shortStringMapper.map(dag(vertices, Collections.emptyMap())));

        Assertions.assertEquals(
                JobLineageMapper.ErrorCode.LINEAGE_GRAPH_TOO_LARGE, error.getErrorCode());
    }

    @Test
    void enforcesNodeAndEdgeBounds() {
        JobLineageMapper oneNodeMapper =
                new JobLineageMapper(new JobLineageMapper.Limits(1, 1, 1, 128));

        Map<Long, VertexInfo> vertices = new HashMap<>();
        vertices.put(
                1L,
                new VertexInfo(
                        1L,
                        PluginType.SOURCE,
                        "source",
                        Collections.singletonList(TablePath.of("db", "source"))));
        vertices.put(
                2L,
                new VertexInfo(
                        2L,
                        PluginType.SINK,
                        "sink",
                        Collections.singletonList(TablePath.of("db", "sink"))));

        JobLineageMapper.LineageMappingException error =
                Assertions.assertThrows(
                        JobLineageMapper.LineageMappingException.class,
                        () -> oneNodeMapper.map(dag(vertices, Collections.emptyMap())));

        Assertions.assertEquals(
                JobLineageMapper.ErrorCode.LINEAGE_GRAPH_TOO_LARGE, error.getErrorCode());
    }

    @Test
    void rejectsUnavailableSnapshot() {
        JobLineageMapper.LineageMappingException error =
                Assertions.assertThrows(
                        JobLineageMapper.LineageMappingException.class,
                        () -> mapper.map(null));

        Assertions.assertEquals(
                JobLineageMapper.ErrorCode.LINEAGE_UNAVAILABLE, error.getErrorCode());
    }

    private static JobDAGInfo dag(
            Map<Long, VertexInfo> vertices, Map<Integer, List<Edge>> edges) {
        return new JobDAGInfo(
                42L,
                Collections.singletonMap("secret", "must-not-be-projected"),
                edges,
                vertices,
                null,
                Collections.emptySet());
    }
}
