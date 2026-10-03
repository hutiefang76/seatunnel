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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable job-scoped execution-topology response used by the lineage mapper. */
public final class JobLineageResponse {

    public enum NodeKind {
        SOURCE,
        TRANSFORM,
        SINK
    }

    public enum DatasetMetadata {
        REPORTED,
        UNAVAILABLE,
        NOT_APPLICABLE
    }

    public enum WarningCode {
        DATASET_METADATA_UNAVAILABLE
    }

    public static final class Node {
        private final String id;
        private final NodeKind kind;
        private final String name;
        private final List<String> tablePaths;
        private final DatasetMetadata datasetMetadata;

        public Node(
                String id,
                NodeKind kind,
                String name,
                List<String> tablePaths,
                DatasetMetadata datasetMetadata) {
            this.id = id;
            this.kind = kind;
            this.name = name;
            this.tablePaths =
                    Collections.unmodifiableList(new ArrayList<>(tablePaths));
            this.datasetMetadata = datasetMetadata;
        }

        public String getId() {
            return id;
        }

        public NodeKind getKind() {
            return kind;
        }

        public String getName() {
            return name;
        }

        public List<String> getTablePaths() {
            return tablePaths;
        }

        public DatasetMetadata getDatasetMetadata() {
            return datasetMetadata;
        }
    }

    public static final class Edge {
        private final int pipelineId;
        private final String sourceNodeId;
        private final String targetNodeId;

        public Edge(int pipelineId, String sourceNodeId, String targetNodeId) {
            this.pipelineId = pipelineId;
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
        }

        public int getPipelineId() {
            return pipelineId;
        }

        public String getSourceNodeId() {
            return sourceNodeId;
        }

        public String getTargetNodeId() {
            return targetNodeId;
        }
    }

    public static final class Warning {
        private final WarningCode code;
        private final String nodeId;

        public Warning(WarningCode code, String nodeId) {
            this.code = code;
            this.nodeId = nodeId;
        }

        public WarningCode getCode() {
            return code;
        }

        public String getNodeId() {
            return nodeId;
        }
    }

    private final int schemaVersion;
    private final String jobId;
    private final String graphKind;
    private final String idScope;
    private final List<Node> nodes;
    private final List<Edge> edges;
    private final List<Warning> warnings;

    public JobLineageResponse(
            int schemaVersion,
            String jobId,
            String graphKind,
            String idScope,
            List<Node> nodes,
            List<Edge> edges,
            List<Warning> warnings) {
        this.schemaVersion = schemaVersion;
        this.jobId = jobId;
        this.graphKind = graphKind;
        this.idScope = idScope;
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
        this.edges = Collections.unmodifiableList(new ArrayList<>(edges));
        this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public String getJobId() {
        return jobId;
    }

    public String getGraphKind() {
        return graphKind;
    }

    public String getIdScope() {
        return idScope;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public List<Warning> getWarnings() {
        return warnings;
    }
}
