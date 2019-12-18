/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.jdbc;

import com.salesforce.cantor.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.common.CommonPreconditions.checkCreate;
import static com.salesforce.cantor.common.CommonPreconditions.checkDrop;
import static com.salesforce.cantor.common.MapsPreconditions.checkDelete;
import static com.salesforce.cantor.common.MapsPreconditions.checkGet;
import static com.salesforce.cantor.common.MapsPreconditions.checkStore;
import static com.salesforce.cantor.jdbc.JdbcUtils.addParameters;
import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public abstract class AbstractBaseMapsOnJdbc extends AbstractBaseCantorOnJdbc implements Maps {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected AbstractBaseMapsOnJdbc(final DataSource dataSource) {
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
    public void store(final String namespace, final Map<String, String> map) throws IOException {
        checkStore(namespace, map);
        doStore(namespace, map);
    }

    @Override
    public Collection<Map<String, String>> get(final String namespace, final Map<String, String> query) throws IOException {
        checkGet(namespace, query);
        return doGet(namespace, query);
    }

    @Override
    public int delete(final String namespace, final Map<String, String> query) throws IOException {
        checkDelete(namespace, query);
        return doDelete(namespace, query);
    }

    @Override
    protected String getNamespaceLookupTableName() {
        return "MAPS-NAMESPACES";
    }

    @Override
    protected void createInternalTables(final Connection connection, final String namespace) throws IOException {
        logger.info("creating map lookup table for namespace: {}", namespace);
        final String chunkLookupTableSql = getCreateLookupTableSql(namespace);
        executeUpdate(connection, chunkLookupTableSql);
    }

    protected abstract String getCreateLookupTableSql(String namespace);

    protected abstract String getCreateMapTableSql(String chunkTableName,
                                                   String namespace,
                                                   Map<String, String> map
    );

    private void createMapTable(final Connection connection,
                                final String namespace,
                                final Map<String, String> map) throws IOException {

        final String mapTableName = getMapTableName(map.keySet());
        logger.info("creating map table {}.{}", namespace, mapTableName);

        // create map table
        final String sql = getCreateMapTableSql(mapTableName, namespace, map);
        executeUpdate(connection, sql);

        // add map table description to lookup table
        addMapToLookupTable(connection, namespace, mapTableName, map);
    }

    private void addMapToLookupTable(final Connection connection,
                                     final String namespace,
                                     final String mapTableName,
                                     final Map<String, String> map) throws IOException {
        logger.info("adding map table '{}' to lookup table", mapTableName);
        final String sql = String.format("INSERT INTO %s SET %s = ?, %s = ?, %s = ? " +
                        "ON DUPLICATE KEY UPDATE %s = ?",
                getTableFullName(namespace, getChunksLookupTableName()),
                quote(getTableNameColumnName()),
                quote(getKeyColumnName()),
                quote(getColumnColumnName()),
                quote(getTableNameColumnName())
        );
        // add a row for each map key
        for (final String metadataKey : map.keySet()) {
            executeUpdate(connection, sql,
                    mapTableName,
                    metadataKey,
                    getMapKeyColumnName(metadataKey),
                    mapTableName
            );
        }
    }

    // returns map of map table column names to the key names
    // e.g., "M_HOST" -> "Host"
    private Map<String, String> getColumnNameToKeyNameMap(final String namespace,
                                                          final String chunkTableName) throws IOException {
        final String sql = String.format("SELECT %s, %s FROM %s WHERE %s IS NOT NULL AND %s = ? ",
                quote(getColumnColumnName()),
                quote(getKeyColumnName()),
                getTableFullName(namespace, getChunksLookupTableName()),
                quote(getKeyColumnName()),
                quote(getTableNameColumnName())
        );
        final Map<String, String> results = new HashMap<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, chunkTableName);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        results.put(resultSet.getString(1), resultSet.getString(2));
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("caught exception executing query sql '{}': {}", sql, e.getMessage());
            throw new IOException(e);
        }
        return results;
    }

    private void doStore(final String namespace, final Map<String, String> map) throws IOException {
        final String insertSql = getMapTableInsertSql(namespace, map);
        final Object[] parameters = getMapTableInsertParameters(map);

        // open a transaction and try to insert all or rollback; partial success is not allowed
        Connection connection = null;
        try {
            connection = openTransaction(getConnection());
            try {
                executeUpdate(connection, insertSql, parameters);
            } catch (IOException e) {
                createMapTable(connection, namespace, map);
                executeUpdate(connection, insertSql, parameters);
            }
        } finally {
            closeConnection(connection);
        }
    }

    private Object[] getMapTableInsertParameters(final Map<String, String> map) {
        final Object[] results = new Object[map.size()];
        final List<String> sortedKeys = getKeysOrdered(map);
        int index = 0;
        for (final String key : sortedKeys) {
            results[index++] = map.get(key);
        }
        return results;
    }

    private String getMapTableInsertSql(final String namespace, final Map<String, String> map) {
        final String mapTableName = getMapTableName(map.keySet());
        final List<String> sortedKeys = getKeysOrdered(map);
        final List<String> sortedKeysSqls = new ArrayList<>();
        for (final String key : sortedKeys) {
            sortedKeysSqls.add(String.format("%s = ?", quote(getMapKeyColumnName(key))));
        }
        return String.format("INSERT INTO %s SET %s",
                getTableFullName(namespace, mapTableName), String.join(",", sortedKeysSqls));
    }

    private Collection<Map<String, String>> doGet(final String namespace, final Map<String, String> query) throws IOException {

        final List<String> mapTables = getMapTableNames(namespace, query.keySet());

        // max of 10 concurrent calls; we don't want to blow up connections
        final ExecutorService executorService = Executors.newFixedThreadPool(10);
        final Collection<Map<String, String>> results = new CopyOnWriteArrayList<>();
        for (final String chunkTableName : mapTables) {
            executorService.submit(() ->
                    results.addAll(doGetOnMapTable(namespace, chunkTableName, query))
            );
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException("maps get operation timed out", e);
        }
        return results;
    }

    private Collection<Map<String, String>> doGetOnMapTable(final String namespace,
                                                            final String chunkTableName,
                                                            final Map<String, String> query) throws IOException {
        final String sqlFormat = "SELECT * FROM %s WHERE 1 %s";
        final Map<String, String> keyHashToName = getColumnNameToKeyNameMap(namespace, chunkTableName);
        final List<Object> parameters = new ArrayList<>();

        // construct the sql query and parameters for metadata and dimensions

        final String sql = String.format(sqlFormat, getTableFullName(namespace, chunkTableName), getMapQuerySql(query, parameters));

        final List<Map<String, String>> results = new ArrayList<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                addParameters(preparedStatement, parameters.toArray());
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final Map<String, String> map = new HashMap<>();
                        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        // for each column, get the name of the column,
                        for (int c = 1; c <= resultSet.getMetaData().getColumnCount(); ++c) {
                            final String columnName = resultSetMetaData.getColumnName(c);
                            map.put(keyHashToName.get(columnName), resultSet.getString(c));
                        }
                        results.add(map);
                    }
                }
            }
            return results;
        } catch (SQLException e) {
            logger.warn("caught exception executing query sql '{}': {}; ignoring.", sql, e.getMessage());
            throw new IOException(e);
        }
    }

    private int doDelete(final String namespace,
                         final Map<String, String> metadataQuery) throws IOException {

        final List<String> chunkTables = getMapTableNames(
                namespace,
                metadataQuery.keySet()
        );

        int results = 0;
        for (final String chunkTableName : chunkTables) {
            results += doDeleteOnChunkTable(namespace,
                    chunkTableName,
                    metadataQuery
            );
        }
        return results;
    }

    private int doDeleteOnChunkTable(final String namespace,
                                     final String chunkTableName,
                                     final Map<String, String> metadataQuery) throws IOException {
        final String sqlFormat = "DELETE FROM %s WHERE 1 %s ";
        final List<Object> parameters = new ArrayList<>();

        // construct the sql query and parameters for metadata and dimensions

        final String sql = String.format(sqlFormat, getTableFullName(namespace, chunkTableName), getMapQuerySql(metadataQuery, parameters));
        return executeUpdate(sql, parameters.toArray());
    }

    // the metadata query object can contain these patterns:
    // '' (just a string): equals - 'user-id' => 'user-1'
    // '=': equals - 'user-id' => '=user-1'
    // '!=': not equals - 'user-id' => '!=user-1'
    // '~': regex like - 'user-id' => '~user-.*'
    // '!~': not regex like - 'user-id' => '!~user-.*'
    private String getMapQuerySql(final Map<String, String> metadataQuery, final List<Object> parameters) {
        if (metadataQuery.isEmpty()) {
            return " AND 1 ";
        }
        final StringBuilder sql = new StringBuilder();
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            final String column = quote(getMapKeyColumnName(entry.getKey()));
            final String query = entry.getValue();
            if (query.startsWith("~")) {
                sql.append(" AND ").append(getRegexQuery(column));
                parameters.add(query.substring(1));
            } else if (query.startsWith("!~")) {
                sql.append(" AND ").append(getNotRegexQuery(column));
                parameters.add(query.substring(2));
            } else if (query.startsWith("=")) {
                sql.append(" AND ").append(column).append(" = ? ");
                parameters.add(query.substring(1));
            } else if (query.startsWith("!=")) {
                sql.append(" AND ").append(column).append(" != ? ");
                parameters.add(query.substring(2));
            } else {
                sql.append(" AND ").append(column).append(" = ? ");
                parameters.add(query);
            }
        }
        return sql.toString();
    }

    abstract protected String getRegexQuery(String column);

    abstract protected String getNotRegexQuery(String column);

    // find the list of all map tables containing the given keys
    private List<String> getMapTableNames(final String namespace, final Collection<String> keys) throws IOException {
        int index = 0;
        final Object[] parameters = new Object[keys.size()];
        // build the where clause by OR'ing all column checks together
        final StringBuilder clauseBuilder = new StringBuilder();
        final StringJoiner whereJoiner = new StringJoiner(" OR ");
        for (final String key : keys) {
            clauseBuilder.setLength(0);
            whereJoiner.add(clauseBuilder.append(quote(getColumnColumnName())).append(" = ? ").toString());
            parameters[index++] = getMapKeyColumnName(key);
        }
        final List<String> tables = new ArrayList<>();
        final String sql = String.format("SELECT DISTINCT %s FROM %s WHERE %s",
                quote(getTableNameColumnName()),
                getTableFullName(namespace, getChunksLookupTableName()),
                whereJoiner.toString()
        );
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                logger.debug("executing sql query [[{}]] with parameters (({}))", sql, parameters);
                addParameters(preparedStatement, parameters);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        tables.add(resultSet.getString(1));
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("caught exception executing query sql '{}': {}", sql, e.getMessage());
            throw new IOException(e);
        }
        return tables;
    }

    protected List<String> getOrdered(final Collection<String> collection) {
        final List<String> keys = new ArrayList<>(collection);
        Collections.sort(keys);
        return keys;
    }

    protected List<String> getKeysOrdered(final Map<String, ?> map) {
        final List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        return keys;
    }

    // chunk table name is hash(namespace)_window(timestamp)_hash(metadata keys)_hash(dimension keys)
    private String getMapTableName(final Collection<String> keys) {
        return String.format("CANTOR-MAP-%s", getKeysHash(keys));
    }

    // unique identifier generated to distinguish between tables based on map keys
    private String getKeysHash(final Collection<String> keys) {
        final String orderedKeys = String.join(",", getOrdered(keys));
        return String.valueOf(Math.abs(orderedKeys.hashCode()));
    }

    protected String getChunksLookupTableName() {
        return "CANTOR-MAPS-LOOKUP";
    }

    protected String getTableNameColumnName() {
        return "TABLE_NAME";
    }

    protected String getKeyColumnName() {
        return "KEY";
    }

    protected String getColumnColumnName() {
        return "COLUMN";
    }

    protected String getMapKeyColumnName(final String metadataKey) {
        final String cleanKey = metadataKey.replaceAll("[^A-Za-z0-9_\\-]", "").toUpperCase();
        return cleanKey.substring(0, Math.min(32, cleanKey.length()))
                + "_"
                + Math.abs(metadataKey.hashCode());
    }
}