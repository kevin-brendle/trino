/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.snowflake;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.TableInfo;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.snowflake.SnowflakeCatalog;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class TrinoSnowflakeCatalog
        extends AbstractTrinoCatalog
{
    private static final int PER_QUERY_CACHE_SIZE = 1000;
    private static final int NAMESPACE_SCHEMA_LEVEL = 2;

    private final SnowflakeCatalog icebergSnowflakeCatalog;
    private final Cache<SchemaTableName, TableMetadata> tableMetadataCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(PER_QUERY_CACHE_SIZE)
            .build();
    private final String snowflakeDatabase;

    public TrinoSnowflakeCatalog(
            SnowflakeCatalog icebergSnowflakeCatalog,
            CatalogName catalogName,
            TypeManager typeManager,
            TrinoFileSystemFactory trinoFileSystemFactory,
            IcebergTableOperationsProvider tableOperationsProvider,
            String snowflakeDatabase)
    {
        super(catalogName, typeManager, tableOperationsProvider, trinoFileSystemFactory, false);
        this.icebergSnowflakeCatalog = requireNonNull(icebergSnowflakeCatalog, "icebergSnowflakeCatalog is null");
        this.snowflakeDatabase = requireNonNull(snowflakeDatabase, "snowflakeDatabase is null");
    }

    public SnowflakeCatalog getIcebergSnowflakeCatalog()
    {
        return icebergSnowflakeCatalog;
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        return icebergSnowflakeCatalog.namespaceExists(Namespace.of(snowflakeDatabase, namespace));
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        try {
            return icebergSnowflakeCatalog.listNamespaces(Namespace.of(snowflakeDatabase))
                    .stream()
                    .map(namespace -> namespace.level(NAMESPACE_SCHEMA_LEVEL - 1))
                    .toList();
        }
        catch (NoSuchNamespaceException e) {
            throw new RuntimeException("Snowflake database %s is not found".formatted(snowflakeDatabase));
        }
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropNamespace is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        return ImmutableMap.of(); // Returning an empty map as icebergSnowflakeCatalog.loadNamespaceMetadata() returns an empty map.
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        throw new TrinoException(NOT_SUPPORTED, "createNamespace is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace)
    {
        return namespace
                .map(Stream::of)
                .orElseGet(() -> listNamespaces(session).stream())
                .flatMap(schema -> {
                    List<TableIdentifier> tableIdentifiers;
                    try {
                        tableIdentifiers = icebergSnowflakeCatalog.listTables(Namespace.of(snowflakeDatabase, schema));
                    }
                    catch (NoSuchNamespaceException ignored) {
                        tableIdentifiers = List.of();
                    }
                    // views and materialized views are currently not supported, so everything is a table
                    return tableIdentifiers
                            .stream()
                            .map(table -> new TableInfo(new SchemaTableName(schema, table.name()), TableInfo.ExtendedRelationType.TABLE));
                })
                .toList();
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        throw new TrinoException(NOT_SUPPORTED, "newCreateTableTransaction is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public Transaction newCreateOrReplaceTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        throw new TrinoException(NOT_SUPPORTED, "newCreateOrReplaceTableTransaction is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "registerTable is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "unregisterTable is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropTable is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropCorruptedTable is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameTable is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableMetadata metadata;
        try {
            metadata = uncheckedCacheGet(
                    tableMetadataCache,
                    schemaTableName,
                    () -> {
                        BaseTable baseTable;
                        try {
                            baseTable = (BaseTable) icebergSnowflakeCatalog.loadTable(TableIdentifier.of(snowflakeDatabase, schemaTableName.getSchemaName(), schemaTableName.getTableName()));
                        }
                        catch (NoSuchTableException e) {
                            throw new TableNotFoundException(schemaTableName);
                        }
                        // Creating a new base table is necessary to adhere to Trino's expectations for quoted table names
                        return new BaseTable(baseTable.operations(), quotedTableName(schemaTableName)).operations().current();
                    });
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw e;
        }

        return getIcebergTableWithMetadata(this, tableOperationsProvider, session, schemaTableName, metadata);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateTableComment is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewComment is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewColumnComment is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "defaultTableLocation is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> materializedViewProperties, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateMaterializedViewColumnComment is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "getMaterializedViewStorageTable is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateColumnComment is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        return Optional.empty();
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "doGetMaterializedView is not supported for Iceberg Snowflake catalogs");
    }

    @Override
    protected void invalidateTableCache(SchemaTableName schemaTableName)
    {
        tableMetadataCache.invalidate(schemaTableName);
    }
}
