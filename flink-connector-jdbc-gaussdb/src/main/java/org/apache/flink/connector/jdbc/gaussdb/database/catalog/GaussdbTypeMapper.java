package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalogTypeMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/** A type mapper implementation for GaussDB that converts PostgreSQL-compatible types. */
@Internal
public class GaussdbTypeMapper implements JdbcCatalogTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(GaussdbTypeMapper.class);

    // GaussDB JDBC driver maps several aliases to real types. We use the real types rather than
    // aliases:
    // serial2 <=> int2
    // smallserial <=> int2
    // serial4 <=> serial
    // serial8 <=> bigserial
    // smallint <=> int2
    // integer <=> int4
    // int <=> int4
    // bigint <=> int8
    // float <=> float8
    // boolean <=> bool
    // decimal <=> numeric

    /** The GaussDB type for smallserial. */
    private static final String PG_SMALLSERIAL = "smallserial";

    /** The GaussDB type for serial. */
    protected static final String PG_SERIAL = "serial";

    /** The GaussDB type for bigserial. */
    protected static final String PG_BIGSERIAL = "bigserial";

    /** The GaussDB type for bytea (binary data). */
    private static final String PG_BYTEA = "bytea";

    /** The GaussDB type for bytea array. */
    private static final String PG_BYTEA_ARRAY = "_bytea";

    /** The GaussDB type for smallint (int2). */
    private static final String PG_SMALLINT = "int2";

    /** The GaussDB type for smallint array. */
    private static final String PG_SMALLINT_ARRAY = "_int2";

    /** The GaussDB type for integer (int4). */
    private static final String PG_INTEGER = "int4";

    /** The GaussDB type for integer array. */
    private static final String PG_INTEGER_ARRAY = "_int4";

    /** The GaussDB type for bigint (int8). */
    private static final String PG_BIGINT = "int8";

    /** The GaussDB type for bigint array. */
    private static final String PG_BIGINT_ARRAY = "_int8";

    /** The GaussDB type for real (float4). */
    private static final String PG_REAL = "float4";

    /** The GaussDB type for real array. */
    private static final String PG_REAL_ARRAY = "_float4";

    /** The GaussDB type for double precision (float8). */
    private static final String PG_DOUBLE_PRECISION = "float8";

    /** The GaussDB type for double precision array. */
    private static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";

    /** The GaussDB type for numeric. */
    private static final String PG_NUMERIC = "numeric";

    /** The GaussDB type for numeric array. */
    private static final String PG_NUMERIC_ARRAY = "_numeric";

    /** The GaussDB type for boolean. */
    private static final String PG_BOOLEAN = "bool";

    /** The GaussDB type for boolean array. */
    private static final String PG_BOOLEAN_ARRAY = "_bool";

    /** The GaussDB type for timestamp. */
    private static final String PG_TIMESTAMP = "timestamp";

    /** The GaussDB type for timestamp array. */
    private static final String PG_TIMESTAMP_ARRAY = "_timestamp";

    /** The GaussDB type for timestamp with time zone. */
    private static final String PG_TIMESTAMPTZ = "timestamptz";

    /** The GaussDB type for timestamp with time zone array. */
    private static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";

    /** The GaussDB type for date. */
    private static final String PG_DATE = "date";

    /** The GaussDB type for date array. */
    private static final String PG_DATE_ARRAY = "_date";

    /** The GaussDB type for time. */
    private static final String PG_TIME = "time";

    /** The GaussDB type for time array. */
    private static final String PG_TIME_ARRAY = "_time";

    /** The GaussDB type for text. */
    private static final String PG_TEXT = "text";

    /** The GaussDB type for text array. */
    private static final String PG_TEXT_ARRAY = "_text";

    /** The GaussDB type for char (bpchar). */
    private static final String PG_CHAR = "bpchar";

    /** The GaussDB type for char array. */
    private static final String PG_CHAR_ARRAY = "_bpchar";

    /** The GaussDB type for character. */
    private static final String PG_CHARACTER = "character";

    /** The GaussDB type for character array. */
    private static final String PG_CHARACTER_ARRAY = "_character";

    /** The GaussDB type for character varying (varchar). */
    private static final String PG_CHARACTER_VARYING = "varchar";

    /** The GaussDB type for character varying array. */
    private static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";

    /**
     * Maps a GaussDB column type (from {@link ResultSetMetaData}) to a Flink {@link DataType}.
     *
     * @param tablePath The path of the table (schema and table name).
     * @param metadata The metadata of the result set.
     * @param colIndex The index of the column in the result set.
     * @return The corresponding Flink {@link DataType}.
     * @throws SQLException If an error occurs while accessing the metadata.
     * @throws UnsupportedOperationException If the GaussDB type is not supported.
     */
    @Override
    public DataType mapping(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String pgType = metadata.getColumnTypeName(colIndex);

        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        DataType dataType = getMapping(pgType, precision, scale);
        if (dataType == null) {
            throw new UnsupportedOperationException(
                    String.format("Doesn't support %s type '%s' yet", getDBType(), pgType));
        }
        return dataType;
    }

    /**
     * Maps a GaussDB type to a Flink {@link DataType} based on the type name, precision, and scale.
     *
     * @param pgType The GaussDB type name.
     * @param precision The precision of the type (if applicable).
     * @param scale The scale of the type (if applicable).
     * @return The corresponding Flink {@link DataType}, or {@code null} if the type is not
     *     supported.
     */
    protected DataType getMapping(String pgType, int precision, int scale) {
        switch (pgType) {
            case PG_BOOLEAN:
                return DataTypes.BOOLEAN();
            case PG_BOOLEAN_ARRAY:
                return DataTypes.ARRAY(DataTypes.BOOLEAN());
            case PG_BYTEA:
                return DataTypes.BYTES();
            case PG_BYTEA_ARRAY:
                return DataTypes.ARRAY(DataTypes.BYTES());
            case PG_SMALLINT:
            case PG_SMALLSERIAL:
                return DataTypes.SMALLINT();
            case PG_SMALLINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.SMALLINT());
            case PG_INTEGER:
            case PG_SERIAL:
                return DataTypes.INT();
            case PG_INTEGER_ARRAY:
                return DataTypes.ARRAY(DataTypes.INT());
            case PG_BIGINT:
            case PG_BIGSERIAL:
                return DataTypes.BIGINT();
            case PG_BIGINT_ARRAY:
                return DataTypes.ARRAY(DataTypes.BIGINT());
            case PG_REAL:
                return DataTypes.FLOAT();
            case PG_REAL_ARRAY:
                return DataTypes.ARRAY(DataTypes.FLOAT());
            case PG_DOUBLE_PRECISION:
                return DataTypes.DOUBLE();
            case PG_DOUBLE_PRECISION_ARRAY:
                return DataTypes.ARRAY(DataTypes.DOUBLE());
            case PG_NUMERIC:
                // Handle numeric types with explicit precision and scale.
                if (precision > 0) {
                    return DataTypes.DECIMAL(precision, scale);
                }
                // Default to maximum precision and scale if not specified.
                return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18);
            case PG_NUMERIC_ARRAY:
                // Handle numeric array types with explicit precision and scale.
                if (precision > 0) {
                    return DataTypes.ARRAY(DataTypes.DECIMAL(precision, scale));
                }
                // Default to maximum precision and scale if not specified.
                return DataTypes.ARRAY(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18));
            case PG_CHAR:
            case PG_CHARACTER:
                return DataTypes.CHAR(precision);
            case PG_CHAR_ARRAY:
            case PG_CHARACTER_ARRAY:
                return DataTypes.ARRAY(DataTypes.CHAR(precision));
            case PG_CHARACTER_VARYING:
                return DataTypes.VARCHAR(precision);
            case PG_CHARACTER_VARYING_ARRAY:
                return DataTypes.ARRAY(DataTypes.VARCHAR(precision));
            case PG_TEXT:
                return DataTypes.STRING();
            case PG_TEXT_ARRAY:
                return DataTypes.ARRAY(DataTypes.STRING());
            case PG_TIMESTAMP:
                return DataTypes.TIMESTAMP(scale);
            case PG_TIMESTAMP_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP(scale));
            case PG_TIMESTAMPTZ:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale);
            case PG_TIMESTAMPTZ_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(scale));
            case PG_TIME:
                return DataTypes.TIME(scale);
            case PG_TIME_ARRAY:
                return DataTypes.ARRAY(DataTypes.TIME(scale));
            case PG_DATE:
                return DataTypes.DATE();
            case PG_DATE_ARRAY:
                return DataTypes.ARRAY(DataTypes.DATE());
            default:
                return null;
        }
    }

    /**
     * Returns the database type name (e.g., "Gaussdb").
     *
     * @return The database type name.
     */
    protected String getDBType() {
        return "Gaussdb";
    }
}
