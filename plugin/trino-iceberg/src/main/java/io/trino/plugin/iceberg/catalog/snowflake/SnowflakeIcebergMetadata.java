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

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class SnowflakeIcebergMetadata
        extends IcebergMetadata
{
    public SnowflakeIcebergMetadata(TypeManager typeManager, CatalogHandle trinoCatalogHandle, JsonCodec<CommitTaskData> commitTaskCodec, TrinoCatalog catalog, IcebergFileSystemFactory fileSystemFactory, TableStatisticsWriter tableStatisticsWriter)
    {
        super(typeManager, trinoCatalogHandle, commitTaskCodec, catalog, fileSystemFactory, tableStatisticsWriter);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support creating tables");
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support setting table comments");
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support setting view comments");
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support setting view column comments");
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support setting materialized view column comments");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support creating tables");
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support inserts");
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support procedures");
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support dropping tables");
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support renaming tables");
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support setting table properties");
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support adding columns");
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support dropping columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support renaming columns");
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support statistics collection");
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support statistics collection");
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support merge");
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support merge");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support views");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support views");
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support views");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support views");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support views");
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support deletes");
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            List<String> sourceTableFunctions)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support materialized views");
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Iceberg Snowflake catalog does not support setting column comments");
    }
}
