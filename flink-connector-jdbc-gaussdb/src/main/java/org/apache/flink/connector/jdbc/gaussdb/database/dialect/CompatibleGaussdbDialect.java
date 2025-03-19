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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.RowType;

import java.util.Optional;

/**
 * An abstract class that extends to provide compatibility with different versions or configurations
 * of the GaussDB database.
 */
@PublicEvolving
public abstract class CompatibleGaussdbDialect extends GaussdbDialect {

    private static final long serialVersionUID = 1L;

    /**
     * Returns the name of the compatible dialect. This method should be implemented by subclasses
     * to provide the specific dialect name that this compatibility layer supports.
     *
     * @return the name of the compatible dialect as a {@link String}.
     */
    protected abstract String compatibleDialectName();

    /**
     * Provides a row converter that is compatible with the specific GaussDB dialect. This method
     * should be implemented by subclasses to return an appropriate {@link
     * CompatibleGaussdbDialectConverter} for the given {@link RowType}.
     *
     * @param rowType the type of row for which the converter is needed.
     * @return a {@link CompatibleGaussdbDialectConverter} instance.
     */
    protected abstract CompatibleGaussdbDialectConverter compatibleRowConverter(RowType rowType);

    /**
     * Returns the optional driver name that is compatible with this dialect. This method should be
     * implemented by subclasses to provide the specific driver name if needed.
     *
     * @return an {@link Optional} containing the driver name as a {@link String}, or an empty
     *     {@link Optional} if no specific driver is required.
     */
    protected abstract Optional<String> compatibleDriverName();

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns the compatible dialect name provided by {@link
     * #compatibleDialectName()}.
     *
     * @return the name of the dialect as a {@link String}.
     */
    @Override
    public String dialectName() {
        return compatibleDialectName();
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns the compatible row converter provided by {@link
     * #compatibleRowConverter(RowType)}.
     *
     * @param rowType the type of row for which the converter is needed.
     * @return a {@link CompatibleGaussdbDialectConverter} instance.
     */
    @Override
    public CompatibleGaussdbDialectConverter getRowConverter(RowType rowType) {
        return compatibleRowConverter(rowType);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns the compatible driver name provided by {@link
     * #compatibleDriverName()}.
     *
     * @return an {@link Optional} containing the driver name as a {@link String}, or an empty
     *     {@link Optional} if no specific driver is required.
     */
    @Override
    public Optional<String> defaultDriverName() {
        return compatibleDriverName();
    }
}
