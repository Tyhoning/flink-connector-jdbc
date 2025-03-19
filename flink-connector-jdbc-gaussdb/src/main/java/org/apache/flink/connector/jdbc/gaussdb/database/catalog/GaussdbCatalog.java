/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.core.database.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.getBriefAuthProperties;

/**
 * A catalog implementation for GaussDB This class provides functionality to interact with GaussDB
 * databases, schemas, and tables, while filtering out built-in databases and schemas that should
 * not be exposed to users.
 */
@Internal
public class GaussdbCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GaussdbCatalog.class);

    /** The default database name for GaussDB. */
    public static final String DEFAULT_DATABASE = "gaussdb";

    // ------ GaussDB default objects that shouldn't be exposed to users ------

    /** A set of built-in databases in GaussDB that should not be exposed to users. */
    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("template0");
                    add("template1");
                }
            };

    /** A set of built-in schemas in GaussDB that should not be exposed to users. */
    private static final Set<String> builtinSchemas =
            new HashSet<String>() {
                {
                    add("pg_toast");
                    add("pg_temp_1");
                    add("pg_toast_temp_1");
                    add("pg_catalog");
                    add("information_schema");
                }
            };

    /** The type mapper for converting GaussDB types to Flink types. */
    protected final JdbcCatalogTypeMapper dialectTypeMapper;

    /**
     * Constructs a new {@link GaussdbCatalog} instance for testing purposes.
     *
     * @param userClassLoader The class loader for user-defined classes.
     * @param catalogName The name of the catalog.
     * @param defaultDatabase The default database name.
     * @param username The username for database authentication.
     * @param pwd The password for database authentication.
     * @param baseUrl The base URL for the database connection.
     */
    @VisibleForTesting
    public GaussdbCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                getBriefAuthProperties(username, pwd));
    }

    /**
     * Constructs a new {@link GaussdbCatalog} instance.
     *
     * @param userClassLoader The class loader for user-defined classes.
     * @param catalogName The name of the catalog.
     * @param defaultDatabase The default database name.
     * @param baseUrl The base URL for the database connection.
     * @param connectProperties The connection properties for the database.
     */
    public GaussdbCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            Properties connectProperties) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                new GaussdbTypeMapper(),
                connectProperties);
    }

    /**
     * Constructs a new {@link GaussdbCatalog} instance (deprecated).
     *
     * @param userClassLoader The class loader for user-defined classes.
     * @param catalogName The name of the catalog.
     * @param defaultDatabase The default database name.
     * @param username The username for database authentication.
     * @param pwd The password for database authentication.
     * @param baseUrl The base URL for the database connection.
     * @param dialectTypeMapper The type mapper for converting GaussDB types to Flink types.
     * @deprecated Use {@link #GaussdbCatalog(ClassLoader, String, String, String, Properties)}
     *     instead.
     */
    @Deprecated
    protected GaussdbCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl,
            JdbcCatalogTypeMapper dialectTypeMapper) {
        this(
                userClassLoader,
                catalogName,
                defaultDatabase,
                baseUrl,
                dialectTypeMapper,
                getBriefAuthProperties(username, pwd));
    }

    /**
     * Constructs a new {@link GaussdbCatalog} instance.
     *
     * @param userClassLoader The class loader for user-defined classes.
     * @param catalogName The name of the catalog.
     * @param defaultDatabase The default database name.
     * @param baseUrl The base URL for the database connection.
     * @param dialectTypeMapper The type mapper for converting GaussDB types to Flink types.
     * @param connectProperties The connection properties for the database.
     */
    protected GaussdbCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String baseUrl,
            JdbcCatalogTypeMapper dialectTypeMapper,
            Properties connectProperties) {
        super(userClassLoader, catalogName, defaultDatabase, baseUrl, connectProperties);
        this.dialectTypeMapper = dialectTypeMapper;
    }

    // ------ databases ------

    /**
     * Lists all databases in the GaussDB instance, excluding built-in databases.
     *
     * @return A list of database names.
     * @throws CatalogException If an error occurs while listing databases.
     */
    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT datname FROM pg_database;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    // ------ schemas ------

    /**
     * Returns the set of built-in schemas in GaussDB that should not be exposed to users.
     *
     * @return A set of built-in schema names.
     */
    protected Set<String> getBuiltinSchemas() {
        return builtinSchemas;
    }

    // ------ tables ------

    /**
     * Retrieves a list of tables in the specified schemas, excluding built-in tables.
     *
     * @param conn The database connection.
     * @param schemas The list of schemas to query.
     * @return A list of table names in the format "schema.table".
     * @throws SQLException If an error occurs while querying the database.
     */
    protected List<String> getPureTables(Connection conn, List<String> schemas)
            throws SQLException {
        List<String> tables = Lists.newArrayList();

        // position 1 is database name, position 2 is schema name, position 3 is table name
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT * FROM information_schema.tables "
                                + "WHERE table_type = 'BASE TABLE' "
                                + "AND table_schema = ? "
                                + "ORDER BY table_type, table_name;")) {
            for (String schema : schemas) {
                // Column index 1 is database name, 2 is schema name, 3 is table name
                extractColumnValuesByStatement(ps, 3, null, schema).stream()
                        .map(pureTable -> schema + "." + pureTable)
                        .forEach(tables::add);
            }
            return tables;
        }
    }

    /**
     * Lists all tables in the specified database, excluding built-in tables.
     *
     * @param databaseName The name of the database.
     * @return A list of table names in the format "schema.table".
     * @throws DatabaseNotExistException If the specified database does not exist.
     * @throws CatalogException If an error occurs while listing tables.
     */
    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {

        Preconditions.checkState(
                StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        final String url = getDatabaseUrl(databaseName);
        try (Connection conn = DriverManager.getConnection(url, connectionProperties)) {
            // get all schemas
            List<String> schemas;
            try (PreparedStatement ps =
                    conn.prepareStatement("SELECT schema_name FROM information_schema.schemata;")) {
                schemas =
                        extractColumnValuesByStatement(
                                ps, 1, pgSchema -> !getBuiltinSchemas().contains(pgSchema));
            }

            // get all tables
            return getPureTables(conn, schemas);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list tables for database %s", databaseName), e);
        }
    }

    /**
     * Converts a JDBC type to a Flink {@link DataType}.
     *
     * @param tablePath The path of the table.
     * @param metadata The result set metadata.
     * @param colIndex The column index.
     * @return The corresponding Flink {@link DataType}.
     * @throws SQLException If an error occurs while accessing the metadata.
     */
    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    /**
     * Checks if a table exists in the specified database.
     *
     * @param tablePath The path of the table.
     * @return {@code true} if the table exists, {@code false} otherwise.
     * @throws CatalogException If an error occurs while checking the table existence.
     */
    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
        return tables.contains(getSchemaTableName(tablePath));
    }

    /**
     * Retrieves the table name from the given {@link ObjectPath}.
     *
     * @param tablePath The path of the table.
     * @return The table name.
     */
    @Override
    protected String getTableName(ObjectPath tablePath) {
        return GaussdbTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgTableName();
    }

    /**
     * Retrieves the schema name from the given {@link ObjectPath}.
     *
     * @param tablePath The path of the table.
     * @return The schema name.
     */
    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return GaussdbTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgSchemaName();
    }

    /**
     * Retrieves the full schema and table name from the given {@link ObjectPath}.
     *
     * @param tablePath The path of the table.
     * @return The full schema and table name in the format "schema.table".
     */
    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return GaussdbTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath();
    }
}
