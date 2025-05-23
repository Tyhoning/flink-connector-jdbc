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

package org.apache.flink.connector.jdbc.gaussdb.database;

import org.apache.flink.connector.jdbc.core.database.JdbcFactoryLoader;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.factory.JdbcCatalogFactoryOptions;
import org.apache.flink.connector.jdbc.gaussdb.GaussdbTestBase;
import org.apache.flink.connector.jdbc.gaussdb.database.catalog.GaussdbCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link JdbcCatalogFactory}.
 *
 * <p>Notes: The source code is based on PostgresFactoryTest.
 */
class GaussdbFactoryTest implements GaussdbTestBase {

    protected static String baseUrl;
    protected static JdbcCatalog catalog;

    protected static final String TEST_CATALOG_NAME = "mygs";

    @BeforeEach
    void setup() {
        String jdbcUrl = getMetadata().getJdbcUrl();
        baseUrl = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));

        catalog =
                JdbcFactoryLoader.loadCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        TEST_CATALOG_NAME,
                        GaussdbCatalog.DEFAULT_DATABASE,
                        getMetadata().getUsername(),
                        getMetadata().getPassword(),
                        baseUrl,
                        null);
    }

    @Test
    void test() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), JdbcCatalogFactoryOptions.IDENTIFIER);
        options.put(
                JdbcCatalogFactoryOptions.DEFAULT_DATABASE.key(), GaussdbCatalog.DEFAULT_DATABASE);
        options.put(JdbcCatalogFactoryOptions.USERNAME.key(), getMetadata().getUsername());
        options.put(JdbcCatalogFactoryOptions.PASSWORD.key(), getMetadata().getPassword());
        options.put(JdbcCatalogFactoryOptions.BASE_URL.key(), baseUrl);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        TEST_CATALOG_NAME,
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());

        checkEquals(catalog, (JdbcCatalog) actualCatalog);

        assertThat((JdbcCatalog) actualCatalog).isInstanceOf(GaussdbCatalog.class);
    }

    private static void checkEquals(JdbcCatalog c1, JdbcCatalog c2) {
        assertThat(c2).isEqualTo(c1);
    }
}
