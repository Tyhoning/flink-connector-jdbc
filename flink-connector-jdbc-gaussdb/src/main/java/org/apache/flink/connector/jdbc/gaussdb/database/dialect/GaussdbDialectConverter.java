package org.apache.flink.connector.jdbc.gaussdb.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.lang.reflect.Array;

/**
 * A dialect converter implementation for GaussDB, This class provides specific implementations for
 * converting between GaussDB's data types and internal data structures.
 */
@Internal
public class GaussdbDialectConverter extends AbstractDialectConverter {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new {@link GaussdbDialectConverter} instance for the specified {@link RowType}.
     *
     * @param rowType the type of row for which this converter is intended.
     */
    protected GaussdbDialectConverter(RowType rowType) {
        super(rowType);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Creates an internal converter for the specified {@link LogicalType}. If the type is an
     * array, it creates a specialized converter for GaussDB arrays; otherwise, it delegates to the
     * superclass implementation.
     *
     * @param type the logical type for which the converter is needed.
     * @return a {@link JdbcDeserializationConverter} instance.
     */
    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();

        if (root == LogicalTypeRoot.ARRAY) {
            ArrayType arrayType = (ArrayType) type;
            return createGaussdbArrayConverter(arrayType);
        } else {
            return createPrimitiveConverter(type);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Creates a nullable external converter for the specified {@link LogicalType}. If the type
     * is an array, it throws an exception since writing ARRAY types is not yet supported.
     * Otherwise, it delegates to the superclass implementation.
     *
     * @param type the logical type for which the converter is needed.
     * @return a {@link JdbcSerializationConverter} instance.
     */
    @Override
    protected JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        if (root == LogicalTypeRoot.ARRAY) {
            // note: Writing ARRAY type is not yet supported by GaussdbQL dialect now.
            return (val, index, statement) -> {
                throw new IllegalStateException(
                        String.format(
                                "Writing ARRAY type is not yet supported in JDBC:%s.",
                                converterName()));
            };
        } else {
            return super.createNullableExternalConverter(type);
        }
    }

    /**
     * Creates a specialized deserialization converter for GaussDB arrays. This method handles the
     * conversion of PostgreSQL arrays into internal array data structures.
     *
     * @param arrayType the array type for which the converter is needed.
     * @return a {@link JdbcDeserializationConverter} instance for arrays.
     */
    private JdbcDeserializationConverter createGaussdbArrayConverter(ArrayType arrayType) {
        // Since PGJDBC 42.2.15 (https://github.com/pgjdbc/pgjdbc/pull/1194) bytea[] is wrapped in
        // primitive byte arrays
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        final JdbcDeserializationConverter elementConverter =
                createNullableInternalConverter(arrayType.getElementType());
        return val -> {
            java.sql.Array pgArray = (java.sql.Array) val;
            Object[] in = (Object[]) pgArray.getArray();
            final Object[] array = (Object[]) Array.newInstance(elementClass, in.length);
            for (int i = 0; i < in.length; i++) {
                array[i] = elementConverter.deserialize(in[i]);
            }
            return new GenericArrayData(array);
        };
    }

    /**
     * Creates a primitive converter for the specified {@link LogicalType}. This method is provided
     * to allow future extensions for GaussDB-specific primitive types.
     *
     * @param type the logical type for which the converter is needed.
     * @return a {@link JdbcDeserializationConverter} instance.
     */
    private JdbcDeserializationConverter createPrimitiveConverter(LogicalType type) {
        return super.createInternalConverter(type);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the name of the converter, which is "GaussdbSQL".
     *
     * @return the name of the converter as a {@link String}.
     */
    @Override
    public String converterName() {
        return "GaussdbSQL";
    }
}
