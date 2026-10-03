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
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.VertexInfo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Pure bounded projection from {@link JobDAGInfo} to the public job-lineage shape. */
public final class JobLineageMapper {

    public enum ErrorCode {
        LINEAGE_UNAVAILABLE,
        INVALID_TOPOLOGY,
        LINEAGE_GRAPH_TOO_LARGE
    }

    public static final class LineageMappingException extends RuntimeException {
        private final ErrorCode errorCode;

        private LineageMappingException(ErrorCode errorCode, String message) {
            super(message);
            this.errorCode = errorCode;
        }

        public ErrorCode getErrorCode() {
            return errorCode;
        }
    }

    /** Structural limits are injected so the REST layer can own final policy values. */
    public static final class Limits {
        private final int maxNodes;
        private final int maxEdges;
        private final int maxTablePathsPerNode;
        private final int maxStringBytes;

        public Limits(
                int maxNodes, int maxEdges, int maxTablePathsPerNode, int maxStringBytes) {
            if (maxNodes <= 0
                    || maxEdges <= 0
                    || maxTablePathsPerNode <= 0
                    || maxStringBytes <= 0) {
                throw new IllegalArgumentException("Lineage limits must be positive");
            }
            this.maxNodes = maxNodes;
            this.maxEdges = maxEdges;
            this.maxTablePathsPerNode = maxTablePathsPerNode;
            this.maxStringBytes = maxStringBytes;
        }
    }

    private static final Comparator<JobLineageResponse.Node> NODE_ORDER =
            Comparator.comparingLong(node -> Long.parseLong(node.getId()));

    private static final Comparator<JobLineageResponse.Edge> EDGE_ORDER =
            Comparator.comparingInt(JobLineageResponse.Edge::getPipelineId)
                    .thenComparingLong(edge -> Long.parseLong(edge.getSourceNodeId()))
                    .thenComparingLong(edge -> Long.parseLong(edge.getTargetNodeId()));

    private static final Comparator<JobLineageResponse.Warning> WARNING_ORDER =
            Comparator.comparing((JobLineageResponse.Warning warning) -> warning.getCode().name())
                    .thenComparingLong(warning -> Long.parseLong(warning.getNodeId()));

    private final Limits limits;

    public JobLineageMapper(Limits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    public JobLineageResponse map(JobDAGInfo dagInfo) {
        if (dagInfo == null
                || dagInfo.getJobId() == null
                || dagInfo.getVertexInfoMap() == null
                || dagInfo.getVertexInfoMap().isEmpty()) {
            throw error(ErrorCode.LINEAGE_UNAVAILABLE, "A usable DAG snapshot is not available");
        }

        if (dagInfo.getVertexInfoMap().size() > limits.maxNodes) {
            throw tooLarge("Node limit exceeded");
        }

        List<JobLineageResponse.Warning> warnings = new ArrayList<>();
        List<JobLineageResponse.Node> nodes =
                mapNodes(dagInfo.getVertexInfoMap(), warnings);
        List<JobLineageResponse.Edge> edges =
                mapEdges(dagInfo.getPipelineEdges(), dagInfo.getVertexInfoMap().keySet());

        validateAcyclic(dagInfo.getVertexInfoMap().keySet(), edges);

        Collections.sort(nodes, NODE_ORDER);
        Collections.sort(edges, EDGE_ORDER);
        Collections.sort(warnings, WARNING_ORDER);

        return new JobLineageResponse(
                1,
                dagInfo.getJobId().toString(),
                "EXECUTION",
                "JOB",
                nodes,
                edges,
                warnings);
    }

    private List<JobLineageResponse.Node> mapNodes(
            Map<Long, VertexInfo> vertexInfoMap, List<JobLineageResponse.Warning> warnings) {
        List<JobLineageResponse.Node> nodes = new ArrayList<>(vertexInfoMap.size());

        for (Map.Entry<Long, VertexInfo> entry : vertexInfoMap.entrySet()) {
            Long key = entry.getKey();
            VertexInfo vertex = entry.getValue();
            if (key == null || vertex == null || key.longValue() != vertex.getVertexId()) {
                throw error(
                        ErrorCode.INVALID_TOPOLOGY,
                        "Vertex map key does not match the vertex identity");
            }

            JobLineageResponse.NodeKind kind = mapKind(vertex.getType());
            String nodeId = Long.toString(vertex.getVertexId());
            String name = vertex.getConnectorType() == null ? "" : vertex.getConnectorType();
            checkStringBound(name, "Node name exceeds the string limit");

            if (kind == JobLineageResponse.NodeKind.TRANSFORM) {
                nodes.add(
                        new JobLineageResponse.Node(
                                nodeId,
                                kind,
                                name,
                                Collections.emptyList(),
                                JobLineageResponse.DatasetMetadata.NOT_APPLICABLE));
                continue;
            }

            TreeSet<String> paths = new TreeSet<>();
            boolean omitted = false;
            if (vertex.getTablePaths() == null || vertex.getTablePaths().isEmpty()) {
                omitted = true;
            } else {
                for (TablePath tablePath : vertex.getTablePaths()) {
                    if (tablePath == null || TablePath.DEFAULT.equals(tablePath)) {
                        omitted = true;
                        continue;
                    }
                    String path = tablePath.toString();
                    checkStringBound(path, "Table path exceeds the string limit");
                    paths.add(path);
                    if (paths.size() > limits.maxTablePathsPerNode) {
                        throw tooLarge("Table-path limit exceeded");
                    }
                }
            }

            JobLineageResponse.DatasetMetadata metadata =
                    paths.isEmpty()
                            ? JobLineageResponse.DatasetMetadata.UNAVAILABLE
                            : JobLineageResponse.DatasetMetadata.REPORTED;
            if (omitted || paths.isEmpty()) {
                warnings.add(
                        new JobLineageResponse.Warning(
                                JobLineageResponse.WarningCode.DATASET_METADATA_UNAVAILABLE,
                                nodeId));
            }

            nodes.add(
                    new JobLineageResponse.Node(
                            nodeId, kind, name, new ArrayList<>(paths), metadata));
        }
        return nodes;
    }

    private List<JobLineageResponse.Edge> mapEdges(
            Map<Integer, List<org.apache.seatunnel.engine.core.job.Edge>> pipelineEdges,
            Set<Long> vertexIds) {
        if (pipelineEdges == null) {
            throw error(ErrorCode.INVALID_TOPOLOGY, "Pipeline edges are missing");
        }

        List<JobLineageResponse.Edge> edges = new ArrayList<>();
        Set<PipelineEdgeKey> seen = new HashSet<>();

        for (Map.Entry<Integer, List<org.apache.seatunnel.engine.core.job.Edge>> entry :
                pipelineEdges.entrySet()) {
            Integer pipelineId = entry.getKey();
            List<org.apache.seatunnel.engine.core.job.Edge> pipeline = entry.getValue();
            if (pipelineId == null || pipeline == null) {
                throw error(ErrorCode.INVALID_TOPOLOGY, "Pipeline edge group is invalid");
            }

            for (org.apache.seatunnel.engine.core.job.Edge edge : pipeline) {
                if (edge == null
                        || edge.getInputVertexId() == null
                        || edge.getTargetVertexId() == null) {
                    throw error(ErrorCode.INVALID_TOPOLOGY, "Edge endpoint is missing");
                }

                long source = edge.getInputVertexId();
                long target = edge.getTargetVertexId();
                if (!vertexIds.contains(source) || !vertexIds.contains(target)) {
                    throw error(ErrorCode.INVALID_TOPOLOGY, "Edge references an unknown vertex");
                }
                if (source == target) {
                    throw error(ErrorCode.INVALID_TOPOLOGY, "Self-loop is not valid lineage");
                }

                PipelineEdgeKey key = new PipelineEdgeKey(pipelineId, source, target);
                if (seen.add(key)) {
                    edges.add(
                            new JobLineageResponse.Edge(
                                    pipelineId,
                                    Long.toString(source),
                                    Long.toString(target)));
                    if (edges.size() > limits.maxEdges) {
                        throw tooLarge("Edge limit exceeded");
                    }
                }
            }
        }

        return edges;
    }

    private void validateAcyclic(
            Set<Long> vertexIds, List<JobLineageResponse.Edge> lineageEdges) {
        Map<Long, Integer> indegree = new HashMap<>();
        Map<Long, List<Long>> adjacency = new HashMap<>();
        for (Long vertexId : vertexIds) {
            indegree.put(vertexId, 0);
            adjacency.put(vertexId, new ArrayList<>());
        }

        Set<NodePair> topologyEdges = new HashSet<>();
        for (JobLineageResponse.Edge edge : lineageEdges) {
            long source = Long.parseLong(edge.getSourceNodeId());
            long target = Long.parseLong(edge.getTargetNodeId());
            NodePair pair = new NodePair(source, target);
            if (topologyEdges.add(pair)) {
                adjacency.get(source).add(target);
                indegree.put(target, indegree.get(target) + 1);
            }
        }

        Deque<Long> ready = new ArrayDeque<>();
        for (Map.Entry<Long, Integer> entry : indegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.add(entry.getKey());
            }
        }

        int visited = 0;
        while (!ready.isEmpty()) {
            long source = ready.removeFirst();
            visited++;
            for (long target : adjacency.get(source)) {
                int next = indegree.get(target) - 1;
                indegree.put(target, next);
                if (next == 0) {
                    ready.addLast(target);
                }
            }
        }

        if (visited != vertexIds.size()) {
            throw error(ErrorCode.INVALID_TOPOLOGY, "Lineage graph contains a cycle");
        }
    }

    private JobLineageResponse.NodeKind mapKind(PluginType type) {
        if (type == null) {
            throw error(ErrorCode.INVALID_TOPOLOGY, "Vertex kind is missing");
        }
        switch (type) {
            case SOURCE:
                return JobLineageResponse.NodeKind.SOURCE;
            case TRANSFORM:
                return JobLineageResponse.NodeKind.TRANSFORM;
            case SINK:
                return JobLineageResponse.NodeKind.SINK;
            default:
                throw error(ErrorCode.INVALID_TOPOLOGY, "Unsupported vertex kind");
        }
    }

    private void checkStringBound(String value, String message) {
        if (value.getBytes(StandardCharsets.UTF_8).length > limits.maxStringBytes) {
            throw tooLarge(message);
        }
    }

    private LineageMappingException tooLarge(String message) {
        return error(ErrorCode.LINEAGE_GRAPH_TOO_LARGE, message);
    }

    private LineageMappingException error(ErrorCode code, String message) {
        return new LineageMappingException(code, message);
    }

    private static final class PipelineEdgeKey {
        private final int pipelineId;
        private final long source;
        private final long target;

        private PipelineEdgeKey(int pipelineId, long source, long target) {
            this.pipelineId = pipelineId;
            this.source = source;
            this.target = target;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PipelineEdgeKey)) {
                return false;
            }
            PipelineEdgeKey that = (PipelineEdgeKey) other;
            return pipelineId == that.pipelineId && source == that.source && target == that.target;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pipelineId, source, target);
        }
    }

    private static final class NodePair {
        private final long source;
        private final long target;

        private NodePair(long source, long target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof NodePair)) {
                return false;
            }
            NodePair that = (NodePair) other;
            return source == that.source && target == that.target;
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, target);
        }
    }
}
