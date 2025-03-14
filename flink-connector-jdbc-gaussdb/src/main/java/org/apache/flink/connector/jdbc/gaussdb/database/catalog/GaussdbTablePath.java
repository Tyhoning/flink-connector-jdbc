package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A utility class for representing and manipulating table paths in GaussDB. */
@Internal
public class GaussdbTablePath {

    /** The default schema name for PostgreSQL-compatible databases like GaussDB. */
    private static final String DEFAULT_POSTGRES_SCHEMA_NAME = "public";

    /** The schema name in GaussDB. */
    private final String pgSchemaName;

    /** The table name in GaussDB. */
    private final String pgTableName;

    /**
     * Constructs a new {@link GaussdbTablePath} instance.
     *
     * @param pgSchemaName The schema name in GaussDB. Must not be null or empty.
     * @param pgTableName The table name in GaussDB. Must not be null or empty.
     * @throws IllegalArgumentException If the schema name or table name is null or empty.
     */
    public GaussdbTablePath(String pgSchemaName, String pgTableName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(pgSchemaName),
                "Schema name is not valid. Null or empty is not allowed");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(pgTableName),
                "Table name is not valid. Null or empty is not allowed");

        this.pgSchemaName = pgSchemaName;
        this.pgTableName = pgTableName;
    }

    /**
     * Creates a {@link GaussdbTablePath} instance from a Flink table name. The Flink table name can
     * be in the format "schema.table" or just "table". If no schema is specified, the default
     * schema name is used.
     *
     * @param flinkTableName The Flink table name.
     * @return A {@link GaussdbTablePath} instance representing the schema and table.
     * @throws IllegalArgumentException If the Flink table name is invalid or cannot be parsed.
     */
    public static GaussdbTablePath fromFlinkTableName(String flinkTableName) {
        if (flinkTableName.contains(".")) {
            String[] path = flinkTableName.split("\\.");

            checkArgument(
                    path != null && path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            flinkTableName, path.length));

            return new GaussdbTablePath(path[0], path[1]);
        } else {
            return new GaussdbTablePath(getDefaultSchemaName(), flinkTableName);
        }
    }

    /**
     * Converts a schema and table name into a Flink table name.
     *
     * @param schema The schema name.
     * @param table The table name.
     * @return The Flink table name in the format "schema.table".
     */
    public static String toFlinkTableName(String schema, String table) {
        return new GaussdbTablePath(schema, table).getFullPath();
    }

    /**
     * Returns the full table path in the format "schema.table".
     *
     * @return The full table path.
     */
    public String getFullPath() {
        return String.format("%s.%s", pgSchemaName, pgTableName);
    }

    /**
     * Returns the table name in GaussDB.
     *
     * @return The table name.
     */
    public String getPgTableName() {
        return pgTableName;
    }

    /**
     * Returns the schema name in GaussDB.
     *
     * @return The schema name.
     */
    public String getPgSchemaName() {
        return pgSchemaName;
    }

    /**
     * Returns the default schema name for PostgreSQL-compatible databases like GaussDB.
     *
     * @return The default schema name.
     */
    protected static String getDefaultSchemaName() {
        return DEFAULT_POSTGRES_SCHEMA_NAME;
    }

    /**
     * Returns a string representation of the table path in the format "schema.table".
     *
     * @return The string representation of the table path.
     */
    @Override
    public String toString() {
        return getFullPath();
    }

    /**
     * Compares this {@link GaussdbTablePath} with another object for equality. Two {@link
     * GaussdbTablePath} instances are equal if their schema and table names are equal.
     *
     * @param o The object to compare with.
     * @return {@code true} if the objects are equal, {@code false} otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GaussdbTablePath that = (GaussdbTablePath) o;
        return Objects.equals(pgSchemaName, that.pgSchemaName)
                && Objects.equals(pgTableName, that.pgTableName);
    }

    /**
     * Returns the hash code for this {@link GaussdbTablePath}.
     *
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(pgSchemaName, pgTableName);
    }
}
