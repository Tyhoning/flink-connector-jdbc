/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.gaussdb.testutils;

import org.apache.flink.connector.jdbc.testutils.DatabaseMetadata;

import com.huawei.gaussdb.jdbc.util.PSQLException;
import com.huawei.gaussdb.jdbc.xa.PGXADataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.XADataSource;

/**
 * Gaussdb Metadata.
 *
 * <p>Notes: The source code is based on PostgresMetadata.
 */
public class GaussdbMetadata implements DatabaseMetadata {

    private final String username;
    private final String password;
    private final String url;
    private final String driver;
    private final String version;
    private final boolean xaEnabled;

    public GaussdbMetadata(GaussDBContainer<?> container) {
        this(container, false);
    }

    public GaussdbMetadata(JdbcDatabaseContainer<?> container, boolean hasXaEnabled) {
        this.username = container.getUsername();
        this.password = container.getPassword();
        this.url = container.getJdbcUrl();
        this.driver = container.getDriverClassName();
        this.version = container.getDockerImageName();
        this.xaEnabled = hasXaEnabled;
    }

    @Override
    public String getJdbcUrl() {
        return this.url;
    }

    @Override
    public String getJdbcUrlWithCredentials() {
        return String.format("%s&user=%s&password=%s", getJdbcUrl(), getUsername(), getPassword());
    }

    @Override
    public String getUsername() {
        return this.username;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public XADataSource buildXaDataSource() {
        if (!xaEnabled) {
            throw new UnsupportedOperationException();
        }

        PGXADataSource xaDataSource = new PGXADataSource();
        try {
            xaDataSource.setUrl(getJdbcUrl());
        } catch (PSQLException e) {
            throw new RuntimeException(e);
        }
        xaDataSource.setUser(getUsername());
        xaDataSource.setPassword(getPassword());
        return xaDataSource;
    }

    @Override
    public String getDriverClass() {
        return this.driver;
    }

    @Override
    public String getVersion() {
        return version;
    }
}
