/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Freezes generated table queries before schema discovery; never rewrites user SQL or init files.
 */
final class DuckLakeSourceSnapshot {
    private DuckLakeSourceSnapshot() {}

    static Map<TablePath, JdbcSourceTable> loadTables(JdbcSourceConfig config)
            throws SQLException, ClassNotFoundException {
        if (!config.isDuckLakeSnapshotAuto()) {
            return JdbcCatalogUtils.getTables(
                    config.getJdbcConnectionConfig(),
                    config.getTableConfigList(),
                    config.getMultiTableFailurePolicy());
        }
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(),
                        config.getJdbcConnectionConfig().getDialect(),
                        config.getCompatibleMode());
        if (!(dialect instanceof DuckDBDialect)) {
            throw new IllegalArgumentException(
                    "ducklake_snapshot_auto requires the DuckDB dialect");
        }
        // Validate all selections before connecting, so an invalid later table cannot be skipped.
        for (JdbcSourceTableConfig table : config.getTableConfigList()) {
            if (table.getTablePath() == null
                    || table.getTablePath().isEmpty()
                    || (table.getQuery() != null && !table.getQuery().isEmpty())
                    || Boolean.TRUE.equals(table.getUseRegex())) {
                throw new IllegalArgumentException(
                        "ducklake_snapshot_auto requires literal catalog.schema.table paths without query or use_regex");
            }
        }
        Map<String, Long> versions = new HashMap<>();
        List<JdbcSourceTableConfig> pinned = new ArrayList<>();
        JdbcConnectionProvider provider =
                dialect.getJdbcConnectionProvider(config.getJdbcConnectionConfig());
        try {
            Connection connection = provider.getOrEstablishConnection();
            for (JdbcSourceTableConfig table : config.getTableConfigList()) {
                TablePath path = dialect.parse(table.getTablePath());
                String catalog = path.getDatabaseName();
                if (catalog == null || catalog.isEmpty()) {
                    throw new IllegalArgumentException(
                            "ducklake_snapshot_auto requires an explicit DuckLake catalog for "
                                    + table.getTablePath());
                }
                String catalogKey = catalog.toLowerCase(Locale.ROOT);
                Long version = versions.get(catalogKey);
                if (version == null) {
                    try (PreparedStatement statement =
                            connection.prepareStatement(
                                    "SELECT MAX(snapshot_id) FROM ducklake_snapshots(?)")) {
                        statement.setString(1, catalog);
                        try (ResultSet result = statement.executeQuery()) {
                            if (!result.next())
                                throw new SQLException(
                                        "No DuckLake snapshot found for catalog " + catalog);
                            version = result.getLong(1);
                            if (result.wasNull())
                                throw new SQLException(
                                        "No DuckLake snapshot found for catalog " + catalog);
                        }
                    }
                    versions.put(catalogKey, version);
                }
                String query =
                        "SELECT * FROM "
                                + dialect.tableIdentifier(path)
                                + " AT (VERSION => "
                                + version
                                + ")";
                pinned.add(
                        JdbcSourceTableConfig.builder()
                                .tablePath(table.getTablePath())
                                .query(query)
                                .partitionColumn(table.getPartitionColumn())
                                .partitionNumber(table.getPartitionNumber())
                                .partitionStart(table.getPartitionStart())
                                .partitionEnd(table.getPartitionEnd())
                                .useSelectCount(table.getUseSelectCount())
                                .skipAnalyze(table.getSkipAnalyze())
                                .useRegex(false)
                                .build());
            }
        } finally {
            provider.closeConnection();
        }
        Map<TablePath, JdbcSourceTable> tables = new LinkedHashMap<>();
        for (JdbcSourceTableConfig table : pinned) {
            TablePath path = dialect.parse(table.getTablePath());
            // Query-only discovery avoids merging current catalog metadata into the pinned schema.
            JdbcSourceTableConfig queryOnly =
                    JdbcSourceTableConfig.builder().query(table.getQuery()).build();
            Map<TablePath, JdbcSourceTable> discovered =
                    JdbcCatalogUtils.getTables(
                            config.getJdbcConnectionConfig(),
                            Collections.singletonList(queryOnly),
                            config.getMultiTableFailurePolicy());
            if (discovered.isEmpty()) continue;
            CatalogTable queryTable = discovered.values().iterator().next().getCatalogTable();
            CatalogTable pinnedTable =
                    CatalogTable.of(
                            TableIdentifier.of(
                                    queryTable.getCatalogName(),
                                    path.getDatabaseName(),
                                    path.getSchemaName(),
                                    path.getTableName()),
                            queryTable);
            tables.put(
                    path,
                    JdbcSourceTable.builder()
                            .tablePath(path)
                            .query(table.getQuery())
                            .partitionColumn(table.getPartitionColumn())
                            .partitionNumber(table.getPartitionNumber())
                            .partitionStart(table.getPartitionStart())
                            .partitionEnd(table.getPartitionEnd())
                            .useSelectCount(table.getUseSelectCount())
                            .skipAnalyze(table.getSkipAnalyze())
                            .catalogTable(pinnedTable)
                            .build());
        }
        return tables;
    }
}
