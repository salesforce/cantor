/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.jdbc;

import com.salesforce.cantor.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static com.salesforce.cantor.common.ObjectsPreconditions.*;
import static com.salesforce.cantor.jdbc.JdbcUtils.getPlaceholders;
import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public abstract class AbstractBaseObjectsOnJdbc
        extends AbstractBaseCantorOnJdbc
        implements Objects {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected AbstractBaseObjectsOnJdbc(final DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        return getNamespaces();
    }

    @Override
    public void create(final String namespace) throws IOException {
        checkCreate(namespace);
        createNamespace(namespace);
    }

    @Override
    public void drop(final String namespace) throws IOException {
        checkDrop(namespace);
        dropNamespace(namespace);
    }

    @Override
    public Collection<String> keys(final String namespace, final int start, final int count) throws IOException {
        checkKeys(namespace, start, count);
        return doKeys(namespace, start, count);
    }

    @Override
    public void store(final String namespace, final String key, final byte[] value) throws IOException {
        checkStore(namespace, key, value);
        doStore(namespace, key, value);
    }

    @Override
    public void store(final String namespace, final Map<String, byte[]> batch) throws IOException {
        checkStore(namespace, batch);
        doStore(namespace, batch);
    }

    @Override
    public byte[] get(final String namespace, final String key) throws IOException {
        checkGet(namespace, key);
        return doGet(namespace, key);
    }

    @Override
    public Map<String, byte[]> get(final String namespace, final Collection<String> keys) throws IOException {
        checkGet(namespace, keys);
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }
        return doGet(namespace, keys);
    }

    @Override
    public int size(final String namespace) throws IOException {
        checkSize(namespace);
        return doSize(namespace);
    }

    @Override
    public boolean delete(final String namespace, final String key) throws IOException {
        checkDelete(namespace, key);
        return doDelete(namespace, key);
    }

    @Override
    public void delete(final String namespace, final Collection<String> objects) throws IOException {
        checkDelete(namespace, objects);
        if (objects.isEmpty()) {
            return;
        }
        doDelete(namespace, objects);
    }

    @Override
    protected void createInternalTables(Connection connection, String namespace) throws IOException {
        createObjectsTable(connection, namespace);
    }

    @Override
    protected void doValidations() throws IOException {
        logger.info("looking for mismatch between database and objects lookup tables");
        // TODO
    }

    private Collection<String> doKeys(final String namespace, final int start, final int count) throws IOException {
        final String sql = String.format("SELECT %s FROM %s %s",
                quote(getKeyColumnName()),
                getTableFullName(namespace, getObjectsTableName()),
                getLimitString(start, count)
        );
        final List<String> results = new ArrayList<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final String key = resultSet.getString(1);
                        results.add(key);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("exception on objects.keys()", e);
            throw new IOException(e);
        }
        return results;
    }

    private byte[] doGet(final String namespace, final String key) throws IOException {
        final String sql = String.format("SELECT %s FROM %s WHERE %s = ?",
                quote(getValueColumnName()),
                getTableFullName(namespace, getObjectsTableName()),
                quote(getKeyColumnName())
        );
        try (final Connection connection = getConnection()){
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, key);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getBytes(1);
                    }
                    return null;
                }
            }
        } catch (SQLException e) {
            logger.warn("exception on objects.get()", e);
            throw new IOException(e);
        }
    }

    private Map<String, byte[]> doGet(final String namespace, final Collection<String> keys) throws IOException {
        final String sql = String.format("SELECT %s, %s FROM %s WHERE %s IN (%s)",
                quote(getKeyColumnName()),
                quote(getValueColumnName()),
                getTableFullName(namespace, getObjectsTableName()),
                quote(getKeyColumnName()),
                getPlaceholders(keys.size())
        );
        final Map<String, byte[]> results = new LinkedHashMap<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                int placeholderIndex = 1;
                for (String key : keys) {
                    preparedStatement.setString(placeholderIndex, key);
                    placeholderIndex += 1;
                }
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final String key = resultSet.getString(1);
                        final byte[] val = resultSet.getBytes(2);
                        results.put(key, val);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("exception on objects.get()", e);
            throw new IOException(e);
        }
        return results;
    }

    private void doStore(final String namespace, final String key, final byte[] bytes) throws IOException {
        final String sql = String.format("INSERT INTO %s SET %s = ?, %s = ? ON DUPLICATE KEY UPDATE %s = ?",
                getTableFullName(namespace, getObjectsTableName()),
                quote(getKeyColumnName()),
                quote(getValueColumnName()),
                quote(getValueColumnName())
        );
        executeUpdate(sql, key, bytes, bytes);
    }

    private void doStore(final String namespace, final Map<String, byte[]> objects) throws IOException {
        final String sql = String.format("INSERT INTO %s SET %s = ?, %s = ? ON DUPLICATE KEY UPDATE %s = ?",
                getTableFullName(namespace, getObjectsTableName()),
                quote(getKeyColumnName()),
                quote(getValueColumnName()),
                quote(getValueColumnName())
        );
        Connection connection = null;
        try {
            final List<Object[]> parameters = new ArrayList<>();
            for (final Map.Entry<String, byte[]> entry : objects.entrySet()) {
                parameters.add(new Object[]{entry.getKey(), entry.getValue(), entry.getValue()});
            }

            // open a transaction to store all objects atomically
            connection = openTransaction(getConnection());

            // store objects
            executeBatchUpdate(connection, sql, parameters);
        } finally {
            closeConnection(connection);
        }
    }

    private boolean doDelete(final String namespace, final String key) throws IOException {
        final String sql = String.format("DELETE FROM %s WHERE %s = ?",
                getTableFullName(namespace, getObjectsTableName()),
                quote(getKeyColumnName())
        );
        return executeUpdate(sql, key) == 1;
    }

    private void doDelete(final String namespace, final Collection<String> keys) throws IOException {
        final String sql = String.format("DELETE FROM %s WHERE %s IN (%s)",
                getTableFullName(namespace, getObjectsTableName()),
                quote(getKeyColumnName()),
                getPlaceholders(keys.size())
        );
        executeUpdate(sql, keys.toArray());
    }

    private int doSize(final String namespace) throws IOException {
        final String sql = String.format("SELECT COUNT(*) FROM %s",
                getTableFullName(namespace, getObjectsTableName())
        );
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("exception on objects.size()", e);
            throw new IOException(e);
        }
        // this should never happen
        throw new IllegalStateException();
    }

    private void createObjectsTable(final Connection connection, final String namespace) throws IOException {
        logger.info("creating objects table for namespace '{}' if not exists", namespace);
        final String createObjectTableSql = getCreateObjectsTableSql(namespace);
        executeUpdate(connection, createObjectTableSql);
    }

    protected abstract String getCreateObjectsTableSql(final String namespace);

    protected String getKeyColumnName() {
        return "KEY";
    }

    protected String getValueColumnName() {
        return "VALUE";
    }

    protected String getObjectsTableName() {
        return "CANTOR-OBJECTS";
    }

    @Override
    protected String getNamespaceLookupTableName() {
        return "OBJECTS-NAMESPACES";
    }

    private String getLimitString(final int start, final int count) {
        if (start == 0 && count == -1) {
            return " ";
        }
        return " LIMIT " + start + "," + count;
    }
}

