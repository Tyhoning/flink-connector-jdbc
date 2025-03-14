package org.apache.flink.connector.jdbc.gaussdb.database.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.RowType;

/**
 * This class allows for customization of converter-specific behavior, such as the converter name.
 */
@Internal
public abstract class CompatibleGaussdbDialectConverter extends GaussdbDialectConverter {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new {@link CompatibleGaussdbDialectConverter} instance for the specified {@link
     * RowType}.
     *
     * @param rowType the type of row for which this converter is intended.
     */
    protected CompatibleGaussdbDialectConverter(RowType rowType) {
        super(rowType);
    }

    /**
     * Returns the name of the compatible converter. This method should be implemented by subclasses
     * to provide the specific converter name that this compatibility layer supports.
     *
     * @return the name of the compatible converter as a {@link String}.
     */
    protected abstract String compatibleConverterName();

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns the compatible converter name provided by {@link
     * #compatibleConverterName()}.
     *
     * @return the name of the converter as a {@link String}.
     */
    @Override
    public String converterName() {
        return compatibleConverterName();
    }
}
