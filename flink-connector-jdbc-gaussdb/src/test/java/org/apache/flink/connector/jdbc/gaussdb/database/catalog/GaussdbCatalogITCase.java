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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.connector.jdbc.gaussdb.database.catalog.GaussdbCatalog.DEFAULT_DATABASE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for {@link GaussdbCatalog}.
 *
 * <p>Notes: The source code is based on PostgresCatalogITCase.
 */
class GaussdbCatalogITCase extends GaussdbCatalogTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    void setup() {
        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        // use PG catalog
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @Test
    void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select id from %s", TABLE1))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testWithoutSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE1))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testWithSchema() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from `%s`",
                                                GaussdbTablePath.fromFlinkTableName(TABLE1)))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s.%s.`%s`",
                                                TEST_CATALOG_NAME,
                                                DEFAULT_DATABASE,
                                                GaussdbTablePath.fromFlinkTableName(TABLE1)))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testInsert() throws Exception {
        tEnv.executeSql(String.format("insert into %s select * from `%s`", TABLE4, TABLE1)).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE4))
                                .execute()
                                .collect());
        assertThat(results).hasToString("[+I[1]]");
    }

    @Test
    void testGroupByInsert() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "insert into `%s` "
                                        + "select `int`, cast('41' as bytes), `short`, max(`long`), max(`real`), "
                                        + "max(`double_precision`), max(`numeric`), max(`decimal`), max(`boolean`), "
                                        + "max(`text`), 'B', 'C', max(`character_varying`), max(`timestamp`), "
                                        + "max(`date`), max(`time`), max(`default_numeric`) "
                                        + "from `%s` group by `int`, `short`",
                                TABLE_PRIMITIVE_TYPE2, TABLE_PRIMITIVE_TYPE))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from `%s`", TABLE_PRIMITIVE_TYPE2))
                                .execute()
                                .collect());
        assertThat(results)
                .hasToString(
                        "[+I[1, [52, 49], 3, 4, 5.5, 6.6, 7.70000, 8.8, true, a, B, C  , d, 2016-06-22T19:10:25, 2015-01-01T00:00, 00:51:03, 500.000000000000000000]]");
    }

    @Test
    void testPrimitiveTypes() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE_PRIMITIVE_TYPE))
                                .execute()
                                .collect());

        assertThat(results)
                .hasToString(
                        "[+I[1, [50], 3, 4, 5.5, 6.6, 7.70000, 8.8, true, a, b, c  , d, 2016-06-22T19:10:25, 2015-01-01T00:00, 00:51:03, 500.000000000000000000]]");
    }

    @Test
    void testArrayTypes() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE_ARRAY_TYPE))
                                .execute()
                                .collect());

        assertThat(results)
                .hasToString(
                        "[+I["
                                + "[1, 2, 3], "
                                + "[[50], [51], [52]], "
                                + "[3, 4, 5], "
                                + "[4, 5, 6], "
                                + "[5.5, 6.6, 7.7], "
                                + "[6.6, 7.7, 8.8], "
                                + "[7.70000, 8.80000, 9.90000], "
                                + "[8.800000000000000000, 9.900000000000000000, 10.100000000000000000], "
                                + "[9.90, 10.10, 11.11], "
                                + "[true, false, true], "
                                + "[a, b, c], "
                                + "[b, c, d], "
                                + "[b  , c  , d  ], "
                                + "[b, c, d], "
                                + "[2016-06-22T19:10:25, 2019-06-22T19:10:25], "
                                + "[2015-01-01T00:00, 2020-01-01T00:00], "
                                + "[00:51:03, 00:59:03], "
                                + "null, "
                                + "null]]");
    }

    @Test
    void testSerialTypes() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE_SERIAL_TYPE))
                                .execute()
                                .collect());

        assertThat(results)
                .hasToString(
                        "[+I["
                                + "32767, "
                                + "2147483647, "
                                + "32767, "
                                + "2147483647, "
                                + "9223372036854775807, "
                                + "9223372036854775807]]");
    }
}
