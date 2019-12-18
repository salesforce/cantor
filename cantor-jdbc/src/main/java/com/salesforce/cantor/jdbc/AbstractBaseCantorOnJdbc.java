/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.salesforce.cantor.jdbc.JdbcUtils.addParameters;
import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

abstract class AbstractBaseCantorOnJdbc {

    // internal database used to keep track of namespace lookup tables
    private static final String cantorInternalDatabaseName = "cantor";
    // all queries have to finish within 30 seconds
    private static final int maxQueryTimeoutSeconds = 30;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final DataSource dataSource;

    // used by underlying implementation to create whatever internal tables are needed as part of namespace creation
    protected abstract void createInternalTables(Connection connection, String namespace) throws IOException;

    protected AbstractBaseCantorOnJdbc(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected String getCantorInternalDatabaseName() {
        return cantorInternalDatabaseName;
    }

    protected abstract String getNamespaceLookupTableName();

    protected void createNamespace(final String namespace) throws IOException {
        init();

        final String databaseName = getDatabaseNameForNamespace(namespace);
        logger.info("creating namespace: '{}' database name: '{}'", namespace, databaseName);

        Connection connection = null;
        try {
            // open a transaction
            connection = openTransaction(getConnection());

            // create cantor internal database if not exists
            executeUpdate(connection, getCreateInternalDatabaseSql());

            // create namespace lookup table
            executeUpdate(connection, getCreateNamespaceLookupTableSql());

            // create database for namespace if not exists
            executeUpdate(connection, getCreateDatabaseSql(databaseName));

            final String sql = String.format("INSERT INTO %s.%s SET %s = ?, %s = ? ON DUPLICATE KEY UPDATE %s = ?",
                    quote(cantorInternalDatabaseName), quote(getNamespaceLookupTableName()),
                    quote(getNamespaceColumnName()), quote(getDatabaseColumnName()),
                    quote(getNamespaceColumnName())
            );
            // add namespace to list of namespaces
            executeUpdate(connection, sql, namespace, databaseName, namespace);

            // create objects table for namespace
            createInternalTables(connection, namespace);
        } finally {
            closeConnection(connection);
        }
    }

    private void init() throws IOException {
        if (!this.isInitialized.getAndSet(true)) {
            doCreateInternalDatabase();
        }
    }

    private void doCreateInternalDatabase() throws IOException {
        Connection connection = null;
        try {
            // open a transaction
            connection = openTransaction(getConnection());

            // create cantor internal database if not exists
            executeUpdate(connection, getCreateInternalDatabaseSql());
            executeUpdate(connection, getCreateNamespaceLookupTableSql());
        } finally {
            closeConnection(connection);
        }
    }

    private String getCreateNamespaceLookupTableSql() {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s ( " +
                        " %s VARCHAR(512) NOT NULL, " +
                        " %s VARCHAR(128) NOT NULL, " +
                        " PRIMARY KEY (%s, %s) ) ",
                quote(cantorInternalDatabaseName), quote(getNamespaceLookupTableName()),
                quote(getNamespaceColumnName()),
                quote(getDatabaseColumnName()),
                quote(getNamespaceColumnName()),
                quote(getDatabaseColumnName())
        );
    }

    protected Collection<String> getNamespaces() throws IOException {
        init();

        final String sql = String.format("SELECT %s FROM %s.%s",
                quote(getNamespaceColumnName()),
                quote(cantorInternalDatabaseName),
                quote(getNamespaceLookupTableName())
        );
        final List<String> results = new ArrayList<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setQueryTimeout(maxQueryTimeoutSeconds);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        results.add(resultSet.getString(1));
                    }
                }
            }
            return results;
        } catch (SQLException e) {
            logger.warn("exception on .namepsaces()", e);
            throw new IOException(e);
        }
    }

    protected void dropNamespace(final String namespace) throws IOException {
        init();

        final String databaseName = getDatabaseNameForNamespace(namespace);
        logger.info("dropping namespace '{}' database name: '{}' if exists", namespace, databaseName);
        Connection connection = null;
        try {
            // open a transaction
            connection = openTransaction(getConnection());

            // drop the database if exists
            final String dropDatabaseSql = getDropDatabaseSql(databaseName);
            executeUpdate(dropDatabaseSql);

            final String sql = String.format("DELETE FROM %s.%s WHERE %s = ?",
                    quote(cantorInternalDatabaseName), quote(getNamespaceLookupTableName()),
                    quote(getDatabaseColumnName())
            );
            // remove namespace/database from lookup table
            executeUpdate(connection, sql, databaseName);
        } finally {
            closeConnection(connection);
        }
    }

    protected String getDatabaseNameForNamespace(final String namespace) {
        final String cleanName = namespace.replaceAll("[^A-Za-z0-9_\\-]", "").toLowerCase();
        return String.format("cantor-%s-%s",
                cleanName.substring(0, Math.min(32, cleanName.length())), Math.abs(namespace.hashCode()));
    }

    protected String getCreateInternalDatabaseSql() {
        return String.format("CREATE DATABASE IF NOT EXISTS %s", quote(cantorInternalDatabaseName));
    }

    protected String getCreateDatabaseSql(final String database) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s", quote(database));
    }

    protected String getDropDatabaseSql(final String database) {
        return String.format("DROP DATABASE IF EXISTS %s", quote(database));
    }

    protected String getDropTableSql(final String namespace, final String tableName) {
        return String.format("DROP TABLE %s", getTableFullName(namespace, tableName));
    }

    protected String getTableFullName(final String namespace, final String tableName) {
        return String.format("%s.%s", quote(getDatabaseNameForNamespace(namespace)), quote(tableName));
    }

    protected DataSource getDataSource() {
        return this.dataSource;
    }

    protected Connection getConnection() throws IOException {
        try {
            return getDataSource().getConnection();
        } catch (SQLException e) {
            logger.warn("failed to get connection", e);
            throw new IOException(e);
        }
    }

    protected void closeConnection(final Connection connection) throws IOException {
        if (connection == null) {
            return;
        }
        try {
            try {
                // commit if connection is not auto-commit
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } catch (SQLException e) {
                logger.warn("exception executing commit", e);
                try {
                    connection.rollback();
                    throw new IOException("operation rolled back");
                } catch (SQLException rollbackexception) {
                    logger.warn("exception executing rollback", rollbackexception);
                    throw new IOException(e);
                }
            }
        } finally {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.warn("exception closing connection", e);
                throw new IOException(e);
            }
        }
    }

    // open a new transaction
    protected Connection openTransaction(final Connection connection) throws IOException {
        try {
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            logger.warn("exception opening transaction", e);
            throw new IOException(e);
        }
    }

    protected int executeUpdate(final String sql, final Object... parameters) throws IOException {
        final Connection connection = getConnection();
        try {
            return executeUpdate(connection, sql, parameters);
        } finally {
            closeConnection(connection);
        }
    }

    protected int executeUpdate(final Connection connection, final String sql, final Object... parameters) throws IOException {
        try {
            do {
                try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    preparedStatement.setQueryTimeout(maxQueryTimeoutSeconds);
                    addParameters(preparedStatement, parameters);
                    return preparedStatement.executeUpdate();
                } catch (SQLTransactionRollbackException e) {
                    // retry
                }
            } while (true);
        } catch (SQLException e) {
            logger.debug("exception caught on executing update sql: '{}'", sql);
            throw new IOException(e);
        }
    }

    protected int[] executeBatchUpdate(final Connection connection,
                                       final String sql,
                                       final Collection<Object[]> batchParameters) throws IOException {
        try {
            do {
                try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    preparedStatement.setQueryTimeout(maxQueryTimeoutSeconds);
                    for (final Object[] parameters : batchParameters) {
                        addParameters(preparedStatement, parameters);
                        preparedStatement.addBatch();
                    }
                    return preparedStatement.executeBatch();
                } catch (SQLTransactionRollbackException e) {
                    // retry
                }
            } while (true);
        } catch (SQLException e) {
            logger.debug("exception caught on executing update sql: '{}'", sql);
            throw new IOException(e);
        }
    }

    protected String getNamespaceColumnName() {
        return "NAMESPACE";
    }

    protected String getDatabaseColumnName() {
        return "DATABASE";
    }

}

