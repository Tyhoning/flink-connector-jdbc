package org.apache.flink.connector.jdbc.gaussdb.database;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.gaussdb.database.catalog.GaussdbCatalog;
import org.apache.flink.connector.jdbc.gaussdb.database.dialect.GaussdbDialect;

/** A factory implementation for creating GaussDB-specific JDBC components. */
@Internal
public class GaussdbFactory implements JdbcFactory {

    /**
     * {@inheritDoc}
     *
     * <p>Checks if the provided JDBC URL is supported by this factory. This implementation returns
     * {@code true} if the URL starts with "jdbc:gaussdb:".
     *
     * @param url the JDBC URL to check.
     * @return {@code true} if the URL is supported, otherwise {@code false}.
     */
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:gaussdb:");
    }

    /**
     * {@inheritDoc}
     *
     * <p>Creates a new instance of {@link GaussdbDialect}, which provides GaussDB-specific SQL
     * dialect support.
     *
     * @return a new {@link JdbcDialect} instance for GaussDB.
     */
    @Override
    public JdbcDialect createDialect() {
        return new GaussdbDialect();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Creates a new instance of {@link GaussdbCatalog}, which provides GaussDB-specific catalog
     * support.
     *
     * @param classLoader the class loader to use for loading resources.
     * @param catalogName the name of the catalog.
     * @param defaultDatabase the default database to connect to.
     * @param username the username for the database connection.
     * @param pwd the password for the database connection.
     * @param baseUrl the base URL for the database connection.
     * @return a new {@link JdbcCatalog} instance for GaussDB.
     */
    @Override
    public JdbcCatalog createCatalog(
            ClassLoader classLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        return new GaussdbCatalog(
                classLoader, catalogName, defaultDatabase, username, pwd, baseUrl);
    }
}
