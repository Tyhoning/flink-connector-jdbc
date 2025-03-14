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

package org.apache.flink.connector.jdbc.gaussdb.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A dialect implementation for GaussDB, This class provides specific implementations for GaussDB's
 * SQL syntax, data types, and other database-specific behaviors.
 */
@Internal
public class GaussdbDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to GaussSQL docs:
    // https://www.GaussSQl.org/docs/12/datatype-datetime.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to GaussSQl docs:
    // https://www.GaussSQl.org/docs/12/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
    private static final int MAX_DECIMAL_PRECISION = 1000;
    private static final int MIN_DECIMAL_PRECISION = 1;

    /**
     * {@inheritDoc}
     *
     * <p>Returns a {@link GaussdbDialectConverter} instance for the specified {@link RowType}.
     *
     * @param rowType the type of row for which the converter is needed.
     * @return a {@link GaussdbDialectConverter} instance.
     */
    @Override
    public GaussdbDialectConverter getRowConverter(RowType rowType) {
        return new GaussdbDialectConverter(rowType);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the default driver name for GaussDB, which is "org.GaussSQl.Driver".
     *
     * @return an {@link Optional} containing the default driver name as a {@link String}.
     */
    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.huawei.gaussdb.jdbc.Driver");
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the name of the dialect, which is "GaussSQL".
     *
     * @return the name of the dialect as a {@link String}.
     */
    @Override
    public String dialectName() {
        return "GaussSQL";
    }

    /**
     * {@inheritDoc}
     *
     * <p>Generates a LIMIT clause for the specified limit value.
     *
     * @param limit the limit value.
     * @return the LIMIT clause as a {@link String}.
     */
    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Generates an upsert (INSERT ... ON CONFLICT ... DO UPDATE) statement for GaussDB. This
     * method constructs a SQL statement that inserts a row into the specified table and updates it
     * if a conflict occurs on the unique key fields.
     *
     * @param tableName the name of the table.
     * @param fieldNames the names of the fields to insert.
     * @param uniqueKeyFields the names of the unique key fields.
     * @return an {@link Optional} containing the upsert statement as a {@link String}, or an empty
     *     {@link Optional} if no upsert statement is needed.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        final Set<String> uniqueKeyFieldsSet = new HashSet<>(Arrays.asList(uniqueKeyFields));
        String updateClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !uniqueKeyFieldsSet.contains(f))
                        .map(f -> quoteIdentifier(f) + "=values(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        String conflictAction =
                updateClause.isEmpty() ? " DO NOTHING" : String.format(" %s", updateClause);
        return Optional.of(
                getInsertIntoStatement(tableName, fieldNames)
                        + " ON DUPLICATE KEY update "
                        + conflictAction);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Quotes an identifier (e.g., table name or column name) for use in SQL statements. This
     * implementation returns the identifier as-is without any quoting.
     *
     * @param identifier the identifier to quote.
     * @return the quoted identifier as a {@link String}.
     */
    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the range of valid precision values for the DECIMAL type in GaussDB. The precision
     * range is defined as [1, 1000].
     *
     * @return an {@link Optional} containing the precision range as a {@link Range}.
     */
    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the range of valid precision values for the TIMESTAMP type in GaussDB. The
     * precision range is defined as [1, 6].
     *
     * @return an {@link Optional} containing the precision range as a {@link Range}.
     */
    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the set of supported logical types for GaussDB. The supported types include CHAR,
     * VARCHAR, BOOLEAN, VARBINARY, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
     * DATE, TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE,
     * and ARRAY.
     *
     * @return a {@link Set} of supported {@link LogicalTypeRoot} values.
     */
    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.ARRAY);
    }
}
