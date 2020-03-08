/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.jdbc;

import com.salesforce.cantor.Events;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.salesforce.cantor.common.CommonUtils.nullToEmpty;
import static com.salesforce.cantor.common.EventsPreconditions.*;
import static com.salesforce.cantor.jdbc.JdbcUtils.addParameters;
import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public abstract class AbstractBaseEventsOnJdbc extends AbstractBaseCantorOnJdbc implements Events {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected AbstractBaseEventsOnJdbc(final DataSource dataSource) {
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
    public void store(final String namespace, final Collection<Event> batch) throws IOException {
        checkStore(namespace, batch);
        doStore(namespace, batch);
    }

    @Override
    public List<Event> get(final String namespace,
                           final long startTimestampMillis,
                           final long endTimestampMillis,
                           final Map<String, String> metadataQuery,
                           final Map<String, String> dimensionsQuery,
                           final boolean includePayloads,
                           final boolean ascending,
                           final int limit) throws IOException {
        checkGet(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return doGet(namespace,
                startTimestampMillis,
                endTimestampMillis,
                nullToEmpty(metadataQuery),
                nullToEmpty(dimensionsQuery),
                includePayloads,
                ascending,
                limit
        );
    }

    @Override
    public int delete(final String namespace,
                      final long startTimestampMillis,
                      final long endTimestampMillis,
                      final Map<String, String> metadataQuery,
                      final Map<String, String> dimensionsQuery) throws IOException {
        checkDelete(namespace, startTimestampMillis, endTimestampMillis, metadataQuery, dimensionsQuery);
        return doDelete(namespace,
                startTimestampMillis,
                endTimestampMillis,
                nullToEmpty(metadataQuery),
                nullToEmpty(dimensionsQuery)
        );
    }

    @Override
    public Map<Long, Double> aggregate(final String namespace,
                                       final String dimension,
                                       final long startTimestampMillis,
                                       final long endTimestampMillis,
                                       final Map<String, String> metadataQuery,
                                       final Map<String, String> dimensionsQuery,
                                       final int aggregateIntervalMillis,
                                       final AggregationFunction aggregationFunction) throws IOException {
        checkAggregate(namespace,
                dimension,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery,
                dimensionsQuery,
                aggregateIntervalMillis,
                aggregationFunction
        );
        return doAggregate(namespace,
                dimension,
                startTimestampMillis,
                endTimestampMillis,
                nullToEmpty(metadataQuery),
                nullToEmpty(dimensionsQuery),
                aggregateIntervalMillis,
                aggregationFunction
        );
    }

    @Override
    public Set<String> metadata(final String namespace,
                                final String metadataKey,
                                final long startTimestampMillis,
                                final long endTimestampMillis,
                                final Map<String, String> metadataQuery,
                                final Map<String, String> dimensionsQuery) throws IOException {
        checkMetadata(namespace,
                metadataKey,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery,
                dimensionsQuery
        );
        return doMetadata(namespace,
                metadataKey,
                startTimestampMillis,
                endTimestampMillis,
                nullToEmpty(metadataQuery),
                nullToEmpty(dimensionsQuery)
        );
    }

    @Override
    public void expire(final String namespace, final long endTimestampMillis) throws IOException {
        checkExpire(namespace, endTimestampMillis);
        doExpire(namespace, endTimestampMillis);
    }

    @Override
    protected String getNamespaceLookupTableName() {
        return "EVENTS-NAMESPACES";
    }

    @Override
    protected void createInternalTables(final Connection connection, final String namespace) throws IOException {
        logger.info("creating chunk lookup table for namespace: {}", namespace);
        final String chunkLookupTableSql = getCreateChunkLookupTableSql(namespace);
        executeUpdate(connection, chunkLookupTableSql);
    }

    @Override
    protected void doValidations() throws IOException {
        logger.info("looking for mismatch between database and lookup tables");
        Connection connection = null;
        try {
            connection = getConnection();
            // for each namespace make sure tables in database and lookup table match
            for (final String namespace : getNamespaces()) {
                logger.info("verifying namespace '{}'", namespace);
                // get the list of all tables in the database
                final List<String> tablesInDatabase = getTablesInDatabase(connection, namespace);
                // get the list of tables in the lookup table
                final List<String> tablesInlookupTable = getChunkTableNames(
                        namespace, 0, Long.MAX_VALUE, Collections.emptyList(), Collections.emptyList()
                );
                // make sure all chunk tables exist in the database
                for (final String chunkTable : tablesInlookupTable) {
                    if (tablesInDatabase.contains(chunkTable)) {
                        // chunk table exists
                        continue;
                    }
                    logger.warn("chunk table '{}' in namespace '{}' does not exist in database; removing it from lookup table",
                            chunkTable, namespace
                    );
                    // could not find the chunk table in the database; remove it from the lookup table
                    removeChunkFromLookupTable(connection, namespace, chunkTable);
                }
                // make sure all tables in the database that start with chunk table name prefix exist in the chunk table
                for (final String databaseTable : tablesInDatabase) {
                    if (getChunksLookupTableName().equalsIgnoreCase(databaseTable)
                            || !databaseTable.startsWith(getChunkTableNamePrefix())
                            || tablesInlookupTable.contains(databaseTable)
                    ) {
                        // ignore chunk lookup and tables that do not start with the chunk table name prefix
                        continue;
                    }
                    // could not find the table in the lookup table; remove it from the database
                    logger.warn("table '{}' in namespace '{}' exists in database but not in lookup table", databaseTable, namespace);
                    dropTable(connection, namespace, databaseTable);
                }
            }
        } finally {
            closeConnection(connection);
        }
    }

    protected abstract String getCreateChunkLookupTableSql(String namespace);

    protected abstract String getCreateChunkTableSql(String chunkTableName,
                                                     String namespace,
                                                     Map<String, String> metadata,
                                                     Map<String, Double> dimensions
    );

    private void createChunkTable(final Connection connection,
                                  final String namespace,
                                  final Event sampleEvent) throws IOException {

        final String chunkTableName = getChunkTableName(
                sampleEvent.getTimestampMillis(),
                sampleEvent.getMetadata().keySet(),
                sampleEvent.getDimensions().keySet()
        );
        logger.info("creating chunk table {}.{}", namespace, chunkTableName);

        // create chunk table
        final String sql = getCreateChunkTableSql(chunkTableName, namespace, sampleEvent.getMetadata(), sampleEvent.getDimensions());

        // create the chunk table
        executeUpdate(connection, sql);

        // add chunk table description to lookup table
        addChunkToLookupTable(connection, namespace, chunkTableName, sampleEvent);
    }

    private void removeChunkFromLookupTable(final Connection connection,
                                            final String namespace,
                                            final String chunkTableName) throws IOException {
        logger.info("removing chunk '{}' from lookup table for namespace '{}'", chunkTableName, namespace);
        // construct the sql to delete chunk metadata from lookup table
        final String deleteChunkLookupSql = String.format("DELETE FROM %s WHERE %s = ?",
                getTableFullName(namespace, getChunksLookupTableName()),
                quote(getTableNameColumnName())
        );
        executeUpdate(connection, deleteChunkLookupSql, chunkTableName);
    }

    private void addChunkToLookupTable(final Connection connection,
                                       final String namespace,
                                       final String chunkTableName,
                                       final Event event) throws IOException {
        logger.info("adding chunk '{}' to lookup table for namespace '{}'", chunkTableName, namespace);
        executeUpdate(connection,
                String.format("INSERT INTO %s SET %s = ?, %s = ?, %s = ? ON DUPLICATE KEY UPDATE %s = ?",
                        getTableFullName(namespace, getChunksLookupTableName()),
                        quote(getTableNameColumnName()),
                        quote(getColumnColumnName()),
                        quote(getStartTimestampMillisColumnName()),
                        quote(getTableNameColumnName())
                ),
                chunkTableName,
                "",  // nothing for first column name
                getWindowForTimestamp(event.getTimestampMillis()),
                chunkTableName
        );
        final String sql = String.format("INSERT INTO %s SET %s = ?, %s = ?, %s = ?, %s = ? " +
                        "ON DUPLICATE KEY UPDATE %s = ?",
                getTableFullName(namespace, getChunksLookupTableName()),
                quote(getTableNameColumnName()),
                quote(getKeyColumnName()),
                quote(getColumnColumnName()),
                quote(getStartTimestampMillisColumnName()),
                quote(getTableNameColumnName())
        );
        // add a row for each metadata column
        for (final String metadataKey : event.getMetadata().keySet()) {
            executeUpdate(connection, sql,
                    chunkTableName,
                    metadataKey,
                    getMetadataKeyColumnName(metadataKey),
                    getWindowForTimestamp(event.getTimestampMillis()),
                    chunkTableName
            );
        }
        // add a row for each dimension column
        for (final String dimensionKey : event.getDimensions().keySet()) {
            executeUpdate(connection, sql,
                    chunkTableName,
                    dimensionKey,
                    getDimensionKeyColumnName(dimensionKey),
                    getWindowForTimestamp(event.getTimestampMillis()),
                    chunkTableName
            );
        }
    }

    private List<String> getTablesInDatabase(final Connection connection, final String namespace) throws IOException {
        final List<String> tables = new ArrayList<>();
        try {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            final ResultSet resultSet = databaseMetaData.getTables(getDatabaseNameForNamespace(namespace), null, "%", null);
            while (resultSet.next()) {
                tables.add(resultSet.getString(3)); // third column is the table name
            }
        } catch (SQLException e) {
            logger.warn("failed to fetch list of tables in database");
            throw new IOException(e);
        }
        return tables;
    }

    // returns map of metadata and dimension column names to the key names
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

    private void doExpire(final String namespace, final long endTimestampMillis) throws IOException {
        // the timestamp of the last event to keep must be larger than the window size
        if (endTimestampMillis < getWindowSizeMillis()) {
            logger.info("expiring end timestamp is smaller than the window size; ignoring.");
            return;
        }

        // find the last window before the end timestamp
        final long windowFloorEndTimestamp = getWindowForTimestamp(endTimestampMillis - getWindowSizeMillis() + 1);
        final List<String> chunkTables = getChunkTableNames(
                namespace,
                0,
                windowFloorEndTimestamp,
                Collections.emptyList(),
                Collections.emptyList()
        );
        for (final String chunkTable : chunkTables) {
            doExpireChunkTable(namespace, chunkTable);
        }
    }

    private void doExpireChunkTable(final String namespace, final String chunkTable) throws IOException {
        logger.info("expiring chunk table {} from namespace {}", chunkTable, namespace);

        // drop the chunk table and delete lookup entries in a transaction
        Connection connection = null;
        try {
            // open a transaction
            connection = openTransaction(getConnection());

            // delete chunk table from lookup table
            removeChunkFromLookupTable(connection, namespace, chunkTable);

            // remove chunk table name from lookup table
            dropTable(connection, namespace, chunkTable);
        } finally {
            closeConnection(connection);
        }
    }

    private void dropTable(final Connection connection, final String namespace, final String chunkTable) throws IOException {
        executeUpdate(connection, getDropTableSql(namespace, chunkTable));
    }

    private void doStore(final String namespace, final Collection<Event> batch) throws IOException {
        // convert to a map of chunk table name to batches of parameters to be passed to jdbc batch calls
        final Map<String, Collection<Object[]>> chunkTableToParameters = toChunkTableBatchParameters(batch);

        // convert to a map of chunk table name to insert sqls
        final Map<String, String> chunkTableToInsertSqls = toChunkTableInsertSqls(namespace, batch);

        // convert to map of chunk table name to create table sqls; used if write fails
        final Map<String, Event> chunkTableToCreateParameters = toChunkTableCreateParameters(batch);

        // open a transaction and try to insert all or rollback; partial success is not allowed
        Connection connection = null;
        try {
            connection = openTransaction(getConnection());
            for (final Map.Entry<String, Collection<Object[]>> entry : chunkTableToParameters.entrySet()) {
                final String insertSql = chunkTableToInsertSqls.get(entry.getKey());
                // execute batch insert for each chunk table; create chunk table if write fails and retry again.
                try {
                    executeBatchUpdate(connection, insertSql, entry.getValue());
                } catch (IOException e) {
                    // try to create a the chunk table and retry insert
                    final Event sampleEvent = chunkTableToCreateParameters.get(entry.getKey());
                    createChunkTable(connection, namespace, sampleEvent);

                    executeBatchUpdate(connection, insertSql, entry.getValue());
                }
            }
        } finally {
            closeConnection(connection);
        }
    }

    private Map<String, Event> toChunkTableCreateParameters(final Collection<Event> batch) {
        final Map<String, Event> chunkTableCreateParameters = new HashMap<>();
        for (final Event event : batch) {
            final long timestampMillis = event.getTimestampMillis();
            final Map<String, String> metadata = event.getMetadata();
            final Map<String, Double> dimensions = event.getDimensions();

            final String chunkTableName = getChunkTableName(timestampMillis, metadata.keySet(), dimensions.keySet());
            if (chunkTableCreateParameters.containsKey(chunkTableName)) {
                continue;
            }
            chunkTableCreateParameters.put(chunkTableName, event);
        }
        return chunkTableCreateParameters;
    }

    private Map<String, String> toChunkTableInsertSqls(final String namespace, final Collection<Event> batch) {
        final Map<String, String> chunkTableInsertSqls = new HashMap<>();
        for (final Event event : batch) {
            final long timestampMillis = event.getTimestampMillis();
            final Map<String, String> metadata = event.getMetadata();
            final Map<String, Double> dimensions = event.getDimensions();

            final String chunkTableName = getChunkTableName(timestampMillis, metadata.keySet(), dimensions.keySet());
            if (chunkTableInsertSqls.containsKey(chunkTableName)) {
                continue;
            }
            chunkTableInsertSqls.put(chunkTableName, getChunkTableInsertSql(namespace, timestampMillis, metadata, dimensions));
        }
        return chunkTableInsertSqls;
    }

    private String getChunkTableInsertSql(final String namespace,
                                          final long timestampMillis,
                                          final Map<String, String> metadata,
                                          final Map<String, Double> dimensions) {
        final String chunkTableName = getChunkTableName(timestampMillis, metadata.keySet(), dimensions.keySet());
        final String insertSql = String.format("INSERT INTO %s SET %s = ? ",
                getTableFullName(namespace, chunkTableName),
                quote(getEventTimestampColumnName())
        );
        final List<String> sortedMetadataKeys = getKeysOrdered(metadata);
        final List<String> sortedDimensionKeys = getKeysOrdered(dimensions);

        final StringBuilder builder = new StringBuilder(insertSql);
        for (final String metadataKey : sortedMetadataKeys) {
            builder.append(",").append(quote(getMetadataKeyColumnName(metadataKey))).append(" = ?");
        }
        for (final String dimensionKey : sortedDimensionKeys) {
            builder.append(",").append(quote(getDimensionKeyColumnName(dimensionKey))).append(" = ?");
        }
        builder.append(", ").append(quote(getPayloadColumnName())).append(" = ?");
        return builder.toString();
    }

    private Map<String, Collection<Object[]>> toChunkTableBatchParameters(final Collection<Event> batch) {
        final Map<String, Collection<Object[]>> sqlPerBatch = new HashMap<>();
        for (final Event event : batch) {
            final long timestampMillis = event.getTimestampMillis();
            final Map<String, String> metadata = event.getMetadata();
            final Map<String, Double> dimensions = event.getDimensions();
            final byte[] payload = event.getPayload();

            final String chunkTableName = getChunkTableName(timestampMillis, metadata.keySet(), dimensions.keySet());
            final List<String> sortedMetadataKeys = getKeysOrdered(metadata);
            final List<String> sortedDimensionKeys = getKeysOrdered(dimensions);

            final Object[] parameters = new Object[1 /* timestamp */ + metadata.size() + dimensions.size() + 1 /* payload */];
            int index = 0;
            parameters[index++] = timestampMillis;
            for (final String metadataKey : sortedMetadataKeys) {
                parameters[index++] = metadata.get(metadataKey);
            }
            for (final String dimensionKey : sortedDimensionKeys) {
                parameters[index++] = dimensions.get(dimensionKey);
            }
            parameters[index] = payload != null ? payload : new byte[0];
            if (!sqlPerBatch.containsKey(chunkTableName)) {
                sqlPerBatch.put(chunkTableName, new ArrayList<>());
            }
            sqlPerBatch.get(chunkTableName).add(parameters);
        }
        return sqlPerBatch;
    }

    private List<Event> doGet(final String namespace,
                              final long startTimestampMillis,
                              final long endTimestampMillis,
                              final Map<String, String> metadataQuery,
                              final Map<String, String> dimensionsQuery,
                              final boolean includePayloads,
                              final boolean ascending,
                              final int limit) throws IOException {

        final List<String> chunkTables = getChunkTableNames(
                namespace,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery.keySet(),
                dimensionsQuery.keySet()
        );
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final List<Event> results = new CopyOnWriteArrayList<>();
        for (final String chunkTableName : chunkTables) {
            executorService.submit(() ->
                    results.addAll(doGetOnChunkTable(namespace,
                            chunkTableName,
                            startTimestampMillis,
                            endTimestampMillis,
                            metadataQuery,
                            dimensionsQuery,
                            includePayloads,
                            ascending,
                            limit)
                    )
            );
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException("events get operation timed out", e);
        }

        // events are fetched from multiple tables, sort before returning
        sortEventsByTimestamp(results, ascending);
        if (limit > 0) {
            return results.subList(0, Math.min(limit, results.size()));
        }
        return results;
    }

    private List<Event> doGetOnChunkTable(final String namespace,
                                          final String chunkTableName,
                                          final long startTimestampMillis,
                                          final long endTimestampMillis,
                                          final Map<String, String> metadataQuery,
                                          final Map<String, String> dimensionsQuery,
                                          final boolean includePayloads,
                                          final boolean ascending,
                                          final int limit) throws IOException {
        Thread.currentThread().setName(String.format("get-chunk-%s.%s", namespace, chunkTableName));
        final String sqlFormat = "SELECT %s %s %s %s %s FROM %s WHERE %s BETWEEN ? AND ? ";
        final Map<String, String> keyHashToName = getColumnNameToKeyNameMap(namespace, chunkTableName);
        final StringBuilder sqlBuilder = new StringBuilder(String.format(sqlFormat,
                quote(getEventTimestampColumnName()),
                includePayloads ? "," : "",
                includePayloads ? quote(getPayloadColumnName()) : "",
                !keyHashToName.isEmpty() ? "," : "",
                keyHashToName.keySet().stream().map(JdbcUtils::quote).collect(Collectors.joining(",")),
                getTableFullName(namespace, chunkTableName),
                quote(getEventTimestampColumnName())
        ));

        final List<Object> parameters = new ArrayList<>();
        parameters.add(startTimestampMillis);
        parameters.add(endTimestampMillis);

        // construct the sql query and parameters for metadata and dimensions
        sqlBuilder.append(getMetadataQuerySql(metadataQuery, parameters));
        sqlBuilder.append(getDimensionQuerySql(dimensionsQuery, parameters));

        sqlBuilder.append(" ORDER BY ").append(getEventTimestampColumnName()).append(ascending ? " ASC " : " DESC ");
        if (limit > 0) {
            sqlBuilder.append(" LIMIT ? ");
            parameters.add(limit);
        }

        final String sql = sqlBuilder.toString();
        final List<Event> results = new ArrayList<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                addParameters(preparedStatement, parameters.toArray());
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final Map<String, String> metadata = new HashMap<>();
                        final Map<String, Double> dimensions = new HashMap<>();
                        final long timestampMillis = resultSet.getLong(1);
                        final byte[] payload = includePayloads ? resultSet.getBytes(2) : null;
                        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        // for each column, get the name of the column,
                        // if starts with m_ it's a metadata column, if starts with d_ it's a dimension column
                        for (int c = 2 + (includePayloads ? 1 : 0); c <= resultSet.getMetaData().getColumnCount(); ++c) {
                            final String columnName = resultSetMetaData.getColumnName(c);
                            if (columnName.equalsIgnoreCase(getPayloadColumnName())) {
                                continue;
                            }
                            if (columnName.startsWith(getMetadataKeyColumnNamePrefix())) {
                                // it's a metadata column
                                metadata.put(keyHashToName.get(columnName), resultSet.getString(c));
                            } else if (columnName.startsWith(getDimensionKeyColumnNamePrefix())) {
                                // it's a dimension column
                                dimensions.put(keyHashToName.get(columnName), resultSet.getDouble(c));
                            } else {
                                // this should never happen
                                throw new IllegalStateException("could not detect column '" + columnName + "'");
                            }
                        }
                        results.add(new Event(timestampMillis, metadata, dimensions, payload));
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
                         final long startTimestampMillis,
                         final long endTimestampMillis,
                         final Map<String, String> metadataQuery,
                         final Map<String, String> dimensionsQuery) throws IOException {

        final List<String> chunkTables = getChunkTableNames(
                namespace,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery.keySet(),
                dimensionsQuery.keySet()
        );

        int results = 0;
        for (final String chunkTableName : chunkTables) {
            results += doDeleteOnChunkTable(namespace,
                    chunkTableName,
                    startTimestampMillis,
                    endTimestampMillis,
                    metadataQuery,
                    dimensionsQuery
            );
        }
        return results;
    }

    private int doDeleteOnChunkTable(final String namespace,
                                          final String chunkTableName,
                                          final long startTimestampMillis,
                                          final long endTimestampMillis,
                                          final Map<String, String> metadataQuery,
                                          final Map<String, String> dimensionsQuery) throws IOException {
        final String sqlFormat = "DELETE FROM %s WHERE %s BETWEEN ? AND ?";
        final StringBuilder sqlBuilder = new StringBuilder(String.format(sqlFormat,
                getTableFullName(namespace, chunkTableName),
                quote(getEventTimestampColumnName())
        ));
        final List<Object> parameters = new ArrayList<>();
        parameters.add(startTimestampMillis);
        parameters.add(endTimestampMillis);

        // construct the sql query and parameters for metadata and dimensions
        sqlBuilder.append(getMetadataQuerySql(metadataQuery, parameters));
        sqlBuilder.append(getDimensionQuerySql(dimensionsQuery, parameters));

        final String sql = sqlBuilder.toString();
        return executeUpdate(sql, parameters.toArray());
    }

    private Map<Long, Double> doAggregate(final String namespace,
                                          final String dimension,
                                          final long startTimestampMillis,
                                          final long endTimestampMillis,
                                          final Map<String, String> metadataQuery,
                                          final Map<String, String> dimensionsQuery,
                                          final int aggregateIntervalMillis,
                                          final AggregationFunction aggregationFunction) throws IOException {

        final String sqlFormat = "SELECT %s, %s FROM %s WHERE %s BETWEEN ? AND ? ";
        final Set<String> dimensions = new HashSet<>(dimensionsQuery.keySet());
        // make sure the dimension exists
        dimensions.add(dimension);
        final List<String> chunkTables = getChunkTableNames(
                namespace,
                startTimestampMillis,
                endTimestampMillis,
                metadataQuery.keySet(),
                dimensions
        );
        // nothing found
        if (chunkTables.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<Long, Double> results = new LinkedHashMap<>();
        final List<Object> parameters = new ArrayList<>();
        final List<String> sqls = new ArrayList<>();
        for (final String chunkTableName : chunkTables) {
            final StringBuilder sqlBuilder = new StringBuilder(String.format(sqlFormat,
                    quote(getEventTimestampColumnName()),
                    quote(getDimensionKeyColumnName(dimension)),
                    getTableFullName(namespace, chunkTableName),
                    quote(getEventTimestampColumnName())
            ));
            parameters.add(startTimestampMillis);
            parameters.add(endTimestampMillis);

            // construct the sql query and parameters for metadata and dimensions
            sqlBuilder.append(getMetadataQuerySql(metadataQuery, parameters));
            sqlBuilder.append(getDimensionQuerySql(dimensionsQuery, parameters));

            sqls.add(sqlBuilder.toString());
        }

        final String selectSql = String.join(" UNION ALL ", sqls);
        final String sql = String.format("SELECT (%s - (%s %% %d)) AS TIMESTAMP_FLOOR, " +
                "%s(%s) AS AGGREGATE_VALUE " +
                "FROM (%s) AS `T` " +
                "GROUP BY TIMESTAMP_FLOOR ORDER BY TIMESTAMP_FLOOR ",
                quote(getEventTimestampColumnName()),
                quote(getEventTimestampColumnName()),
                aggregateIntervalMillis,
                aggregationFunction.toString(),
                quote(getDimensionKeyColumnName(dimension)),
                selectSql
        );

        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                addParameters(preparedStatement, parameters.toArray());
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final long timestampMillis = resultSet.getLong(1);
                        final double value = resultSet.getDouble(2);
                        if (!results.containsKey(timestampMillis)) {
                            results.put(timestampMillis, value);
                        } else {
                            // TODO: THIS IS A BUG! aggregation has to be done across all chunk tables
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn("caught exception executing query sql '{}': {}", sql, e.getMessage());
            throw new IOException(e);
        }
        return results;
    }

    private Set<String> doMetadata(final String namespace,
                                   final String metadataKey,
                                   final long startTimestampMillis,
                                   final long endTimestampMillis,
                                   final Map<String, String> metadataQuery,
                                   final Map<String, String> dimensionsQuery) throws IOException {

        final String sqlFormat = "SELECT DISTINCT %s AS METADATA_VALUE FROM %s WHERE %s BETWEEN ? AND ?";
        final Set<String> metadataKeys = new HashSet<>(metadataQuery.keySet());
        // make sure the metadata exists
        metadataKeys.add(metadataKey);
        final List<String> chunkTables = getChunkTableNames(
                namespace,
                startTimestampMillis,
                endTimestampMillis,
                metadataKeys,
                dimensionsQuery.keySet()
        );

        final ExecutorService executorService = Executors.newCachedThreadPool();
        final Set<String> results = new ConcurrentSkipListSet<>();
        for (final String chunkTableName : chunkTables) {
            final StringBuilder sqlBuilder = new StringBuilder(String.format(sqlFormat,
                    quote(getMetadataKeyColumnName(metadataKey)),
                    getTableFullName(namespace, chunkTableName),
                    quote(getEventTimestampColumnName())
            ));
            final List<Object> parameters = new ArrayList<>();
            parameters.add(startTimestampMillis);
            parameters.add(endTimestampMillis);

            // construct the sql query and parameters for metadata and dimensions
            sqlBuilder.append(getMetadataQuerySql(metadataQuery, parameters));
            sqlBuilder.append(getDimensionQuerySql(dimensionsQuery, parameters));

            final String sql = sqlBuilder.toString();

            executorService.submit(() -> {
                try (final Connection connection = getConnection()) {
                    try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        addParameters(preparedStatement, parameters.toArray());
                        try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                            while (resultSet.next()) {
                                results.add(resultSet.getString(1));
                            }
                        }
                    }
                } catch (SQLException | IOException e) {
                    logger.warn("caught exception executing query sql '{}': {}", sql, e.getMessage());
                }
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(30, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException("events get operation timed out", e);
        }
        return results;
    }

    // the metadata query object can contain these patterns:
    // '' (just a string): equals - 'user-id' => 'user-1'
    // '=': equals - 'user-id' => '=user-1'
    // '!=': not equals - 'user-id' => '!=user-1'
    // '~': regex like - 'user-id' => '~user-.*'
    // '!~': not regex like - 'user-id' => '!~user-.*'
    private String getMetadataQuerySql(final Map<String, String> metadataQuery, final List<Object> parameters) {
        if (metadataQuery.isEmpty()) {
            return " AND 1 ";
        }
        final StringBuilder sql = new StringBuilder();
        for (final Map.Entry<String, String> entry : metadataQuery.entrySet()) {
            final String column = quote(getMetadataKeyColumnName(entry.getKey()));
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

    // the dimension query object can contain these patterns:
    // '' (just a number): equals - 'cpu' => '90'
    // '=': equals - 'cpu' => '=90'
    // '!=': not equals - 'cpu' => '!=90'
    // '..': between - 'cpu' => '90..100'
    // '>': greater than - 'cpu' => '>90'
    // '>=': greater than or equals - 'cpu' => '>=90'
    // '<': less than - 'cpu' => '<90'
    // '<=': less than or equals - 'cpu' => '<=90'
    private String getDimensionQuerySql(final Map<String, String> dimensionsQuery, final List<Object> parameters) {
        if (dimensionsQuery.isEmpty()) {
            return " AND 1 ";
        }
        final StringBuilder sql = new StringBuilder();
        for (final Map.Entry<String, String> entry : dimensionsQuery.entrySet()) {
            final String column = quote(getDimensionKeyColumnName(entry.getKey()));
            final String query = entry.getValue();
            if (query.contains("..")) {
                sql.append(" AND ? BETWEEN ? AND ? ");
                parameters.add(Double.valueOf(query.substring(0, query.indexOf(".."))));
                parameters.add(Double.valueOf(query.substring(query.indexOf("..") + 2)));
            } else if (query.startsWith(">=")) {
                sql.append(" AND ").append(column).append(" >= ? ");
                parameters.add(Double.valueOf(query.substring(2)));
            } else if (query.startsWith("<=")) {
                sql.append(" AND ").append(column).append(" <= ? ");
                parameters.add(Double.valueOf(query.substring(2)));
            } else if (query.startsWith(">")) {
                sql.append(" AND ").append(column).append(" > ? ");
                parameters.add(Double.valueOf(query.substring(1)));
            } else if (query.startsWith("<")) {
                sql.append(" AND ").append(column).append(" < ? ");
                parameters.add(Double.valueOf(query.substring(1)));
            } else if (query.startsWith("!=")) {
                sql.append(" AND ").append(column).append(" != ? ");
                parameters.add(Double.valueOf(query.substring(2)));
            } else if (query.startsWith("=")) {
                sql.append(" AND ").append(column).append(" = ? ");
                parameters.add(Double.valueOf(query.substring(1)));
            } else {
                sql.append(" AND ").append(column).append(" = ? ");
                parameters.add(Double.valueOf(query));
            }
        }
        return sql.toString();
    }

    private void sortEventsByTimestamp(final List<Event> events, final boolean ascending) {
        events.sort((event1, event2) -> {
            if (event1.getTimestampMillis() < event2.getTimestampMillis()) {
                return ascending ? -1 : 1;
            } else if (event1.getTimestampMillis() > event2.getTimestampMillis()) {
                return ascending ? 1 : -1;
            }
            return 0;
        });
    }

    // find the list of all chunk tables containing events for
    // the given namespace, start and end, metadata and dimension keys
    private List<String> getChunkTableNames(final String namespace,
                                            final long startTimestampMillis,
                                            final long endTimestampMillis,
                                            final Collection<String> metadataKeys,
                                            final Collection<String> dimensionKeys) throws IOException {
        final StringBuilder sqlFormatBuilder = new StringBuilder();
        final Object[] parameters = new Object[2 + metadataKeys.size() + dimensionKeys.size()];
        sqlFormatBuilder.append("SELECT DISTINCT ").append(quote(getTableNameColumnName()))
                .append(" FROM ").append(getTableFullName(namespace, getChunksLookupTableName()))
                .append(" WHERE (")
                .append(quote(getStartTimestampMillisColumnName()))
                .append(" BETWEEN ? AND ?)")
        ;
        int index = 0;
        final long startWindow = getWindowForTimestamp(startTimestampMillis) - getWindowSizeMillis();
        parameters[index++] = startWindow >= 0 ? startWindow : 0;
        // make sure if max long is passed there's no overflow
        final long endTimestampWindow = Long.MAX_VALUE - getWindowForTimestamp(endTimestampMillis) > getWindowSizeMillis()
                ? getWindowForTimestamp(endTimestampMillis) + getWindowSizeMillis()
                : Long.MAX_VALUE;
        parameters[index++] = endTimestampWindow;

        // need to find the table names that have ALL of the metadata/dimension key columns using nested selects
        final String tableNameInSeparator = " AND " + quote(getTableNameColumnName()) + " IN (";
        final StringJoiner tableNameSelectJoiner = new StringJoiner(tableNameInSeparator);
        final StringBuilder innerSelectBuilder = new StringBuilder();
        for (final String metadataKey : metadataKeys) {
            innerSelectBuilder.setLength(0);
            innerSelectBuilder.append("SELECT ").append(quote(getTableNameColumnName()))
                    .append(" FROM ").append(getTableFullName(namespace, getChunksLookupTableName()))
                    .append(" WHERE ").append(quote(getColumnColumnName())).append(" = ?");
            tableNameSelectJoiner.add(innerSelectBuilder.toString());
            parameters[index++] = getMetadataKeyColumnName(metadataKey);
        }
        for (final String dimensionKey : dimensionKeys) {
            innerSelectBuilder.setLength(0);
            innerSelectBuilder.append("SELECT ").append(quote(getTableNameColumnName()))
                    .append(" FROM ").append(getTableFullName(namespace, getChunksLookupTableName()))
                    .append(" WHERE ").append(quote(getColumnColumnName())).append(" = ?");
            tableNameSelectJoiner.add(innerSelectBuilder.toString());
            parameters[index++] = getDimensionKeyColumnName(dimensionKey);
        }

        if (!metadataKeys.isEmpty() || !dimensionKeys.isEmpty()) {
            sqlFormatBuilder.append(" AND (")
                    .append(quote(getTableNameColumnName()))
                    .append(" IN (")
                    .append(tableNameSelectJoiner.toString());
            // close all open parentheses from metadata/dimension inner selects
            for (int i = 0; i < metadataKeys.size() + dimensionKeys.size(); i++) {
                sqlFormatBuilder.append(")");
            }
            // close single open paren from IN (
            sqlFormatBuilder.append(")");
        }

        final List<String> tables = new ArrayList<>();
        final String sql = sqlFormatBuilder.toString();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                addParameters(preparedStatement, parameters);
                logger.debug("executing chunk table sql query [[{}]] with parameters (({}))", sql, parameters);
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

    private long getWindowForTimestamp(final long timestampMillis) {
        return (timestampMillis / getWindowSizeMillis()) * getWindowSizeMillis();
    }

    private long getWindowSizeMillis() {
        return TimeUnit.DAYS.toMillis(1);  // one per day
    }

    protected List<String> getOrderedKeys(final Map<String, ?> map) {
        return getOrdered(map.keySet());
    }

    private List<String> getOrdered(final Collection<String> collection) {
        final List<String> keys = new ArrayList<>(collection);
        Collections.sort(keys);
        return keys;
    }

    private List<String> getKeysOrdered(final Map<String, ?> map) {
        final List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        return keys;
    }

    // chunk table name is hash(namespace)_window(timestamp)_hash(metadata keys)_hash(dimension keys)
    private String getChunkTableName(final long timestampMillis,
                                     final Collection<String> metadataKeys,
                                     final Collection<String> dimensionKeys) {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format("%s%s_%s",
                getChunkTableNamePrefix(),
                dateFormat.format(new Date(getWindowForTimestamp(timestampMillis))),
                getKeysHash(metadataKeys, dimensionKeys)
        );
    }

    // unique identifier generated to distinguish between tables based on metadata and dimension keys
    // the idnetifier is generated by hashing the csv string of the ordered list of metadata and dimensions keys
    private String getKeysHash(final Collection<String> metadataKeys,
                               final Collection<String> dimensionKeys) {
        final String orderedMetadataKeysCsv = String.join(",", getOrdered(metadataKeys));
        final String orderedDimensionKeysCsv = String.join(",", getOrdered(dimensionKeys));
        return Math.abs(orderedMetadataKeysCsv.hashCode()) + "_" + Math.abs(orderedDimensionKeysCsv.hashCode());
    }

    protected String getEventTimestampColumnName() {
        return "TIMESTAMP_MILLIS";
    }

    protected String getChunksLookupTableName() {
        return "CANTOR-EVENTS-CHUNKS-LOOKUP";
    }

    protected String getChunkTableNamePrefix() {
        return "CANTOR-EVENTS-CHUNK-";
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

    protected String getStartTimestampMillisColumnName() {
        return "START_TIMESTAMP_MILLIS";
    }

    protected String getPayloadColumnName() {
        return "PAYLOAD";
    }

    protected String getDimensionKeyColumnNamePrefix() {
        return "D_";
    }

    protected String getMetadataKeyColumnNamePrefix() {
        return "M_";
    }

    protected String getDimensionKeyColumnName(final String dimensionKey) {
        final String cleanKey = dimensionKey.replaceAll("[^A-Za-z0-9_\\-]", "").toUpperCase();
        return getDimensionKeyColumnNamePrefix()
                + cleanKey.substring(0, Math.min(32, cleanKey.length()))
                + "_"
                + Math.abs(dimensionKey.hashCode());
    }

    protected String getMetadataKeyColumnName(final String metadataKey) {
        final String cleanKey = metadataKey.replaceAll("[^A-Za-z0-9_\\-]", "").toUpperCase();
        return getMetadataKeyColumnNamePrefix()
                + cleanKey.substring(0, Math.min(32, cleanKey.length()))
                + "_"
                + Math.abs(metadataKey.hashCode());
    }
}