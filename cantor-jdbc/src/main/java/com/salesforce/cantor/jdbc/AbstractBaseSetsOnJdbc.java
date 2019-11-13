/*
 * Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.jdbc;

import com.salesforce.cantor.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.salesforce.cantor.common.SetsPreconditions.*;
import static com.salesforce.cantor.jdbc.JdbcUtils.addParameters;
import static com.salesforce.cantor.jdbc.JdbcUtils.getPlaceholders;
import static com.salesforce.cantor.jdbc.JdbcUtils.quote;

public abstract class AbstractBaseSetsOnJdbc extends AbstractBaseCantorOnJdbc implements Sets {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected AbstractBaseSetsOnJdbc(final DataSource dataSource) throws IOException {
        super(dataSource);
        doCreateInternalDatabase();
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
    public int size(final String namespace, final String set) throws IOException {
        checkSize(namespace, set);
        return doSize(namespace, set);
    }

    @Override
    public Collection<String> entries(final String namespace,
                                      final String set,
                                      final long min,
                                      final long max,
                                      final int start,
                                      final int count,
                                      final boolean ascending) throws IOException {
        checkEntries(namespace, set, min, max, start, count, ascending);
        return doEntries(namespace, set, min, max, start, count, getOrderByString(ascending));
    }

    @Override
    public Collection<String> sets(final String namespace) throws IOException {
        checkSets(namespace);
        return doSets(namespace);
    }

    @Override
    public Map<String, Long> get(final String namespace,
                                 final String set,
                                 final long min,
                                 final long max,
                                 final int start,
                                 final int count,
                                 final boolean ascending) throws IOException {
        checkGet(namespace, set, min, max, start, count, ascending);
        return doGet(namespace, set, min, max, start, count, getOrderByString(ascending));
    }

    @Override
    public Map<String, Long> union(final String namespace,
                                   final Collection<String> sets,
                                   final long min,
                                   final long max,
                                   final int start,
                                   final int count,
                                   final boolean ascending) throws IOException {
        checkUnion(namespace, sets, min, max, start, count, ascending);
        return doUnion(namespace, sets, min, max, start, count, getOrderByString(ascending));
    }

    @Override
    public Map<String, Long> intersect(final String namespace,
                                       final Collection<String> sets,
                                       final long min,
                                       final long max,
                                       final int start,
                                       final int count,
                                       final boolean ascending) throws IOException {
        checkIntersect(namespace, sets, min, max, start, count, ascending);
        return doIntersect(namespace, sets, min, max, start, count, getOrderByString(ascending));
    }

    @Override
    public Map<String, Long> pop(final String namespace,
                                 final String key,
                                 final long min,
                                 final long max,
                                 final int start,
                                 final int count,
                                 final boolean ascending) throws IOException {
        checkPop(namespace, key, min, max, start, count, ascending);
        return doPop(namespace, key, min, max, start, count, getOrderByString(ascending));
    }

    @Override
    public void delete(final String namespace, final String key, final long min, final long max) throws IOException {
        checkDelete(namespace, key, min, max);
        doDelete(namespace, key, min, max);
    }

    @Override
    public boolean delete(final String namespace, final String key, final String entry) throws IOException {
        checkDelete(namespace, key, entry);
        return doDelete(namespace, key, entry);
    }

    @Override
    public void delete(final String namespace, final String key, final Collection<String> entries) throws IOException {
        checkDelete(namespace, key, entries);
        doDelete(namespace, key, entries);
    }

    @Override
    public void add(final String namespace, final String key, final String entry, final long weight) throws IOException {
        checkAdd(namespace, key, entry, weight);
        doAdd(namespace, key, entry, weight);
    }

    @Override
    public void add(final String namespace, final String key, final Map<String, Long> entries) throws IOException {
        checkAdd(namespace, key, entries);
        doAdd(namespace, key, entries);
    }

    @Override
    public Long weight(final String namespace, final String key, final String entry) throws IOException {
        checkWeight(namespace, key, entry);
        return doWeight(namespace, key, entry);
    }

    @Override
    public void inc(final String namespace, final String key, final String entry, final long count) throws IOException {
        checkInc(namespace, key, entry, count);
        doInc(namespace, key, entry, count);
    }

    @Override
    protected String getNamespaceLookupTableName() {
        return "SETS-NAMESPACES";
    }

    @Override
    protected void createInternalTables(final Connection connection, final String namespace) throws IOException {
        logger.info("creating sets table for namespace '{}' if not exists", namespace);
        executeUpdate(connection, getCreateSetsTableSql(namespace));
    }

    private int doSize(final String namespace, final String set) throws IOException {
        final String sql = String.format("SELECT COUNT(*) FROM %s WHERE %s = ? ",
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName())
        );
        try (final Connection connection = getConnection()){
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, set);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getInt(1);
                    }
                    return 0;
                }
            }
        } catch (final SQLException e) {
            logger.warn("exception on sets.size()", e);
            throw new IOException(e);
        }
    }

    private Map<String, Long> doGet(final String namespace,
                                    final String set,
                                    final long min,
                                    final long max,
                                    final int start,
                                    final int count,
                                    final String orderby) throws IOException {
        final String sql = String.format("SELECT %s, %s FROM %s WHERE %s.%s = ? AND %s.%s BETWEEN ? AND ? %s %s",
                quote(getEntryColumnName()),
                quote(getWeightColumnName()),
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getSetsTableName()),
                quote(getWeightColumnName()),
                orderby,
                getLimitString(start, count)
        );
        final Map<String, Long> items = new LinkedHashMap<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, set);
                preparedStatement.setLong(2, min);
                preparedStatement.setLong(3, max);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final String key = resultSet.getString(1);
                        final Long weight = resultSet.getLong(2);
                        if (key != null && !key.isEmpty()) {
                            items.put(key, weight);
                        }
                    }
                }
            }
            return items;
        } catch (final SQLException e) {
            logger.warn("exception on sets.get()", e);
            throw new IOException(e);
        }
    }

    private Map<String, Long> doUnion(final String namespace,
                                      final Collection<String> sets,
                                      final long min,
                                      final long max,
                                      final int start,
                                      final int count,
                                      final String orderby) throws IOException {
        final String partialSql = String.format("SELECT %s, %s FROM %s WHERE %s = ? AND %s BETWEEN ? AND ?",
                quote(getEntryColumnName()),
                quote(getWeightColumnName()),
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getWeightColumnName())
        );
        final String sql = String.format("%s %s %s",
                String.join(" UNION ", Collections.nCopies(sets.size(), partialSql)),
                orderby,
                getLimitString(start, count)
        );
        final Map<String, Long> items = new LinkedHashMap<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                int index = 1;
                for (final String set : sets) {
                    preparedStatement.setString(index++, set);
                    preparedStatement.setLong(index++, min);
                    preparedStatement.setLong(index++, max);
                }
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final String key = resultSet.getString(1);
                        final Long weight = resultSet.getLong(2);
                        if (key != null && !key.isEmpty()) {
                            items.put(key, weight);
                        }
                    }
                }
                return items;
            }
        } catch (final SQLException e) {
            logger.warn("exception on sets.union()", e);
            throw new IOException(e);
        }
    }

    private Map<String, Long> doIntersect(final String namespace,
                                          final Collection<String> sets,
                                          final long min,
                                          final long max,
                                          final int start,
                                          final int count,
                                          final String orderby) throws IOException {
        int index = 0;
        final Object[] parameters = new Object[sets.size() + 2];
        parameters[index++] = min;
        parameters[index++] = max;
        // set keys are OR'd together to get all matching set entries
        final StringJoiner setsJoiner = new StringJoiner(" OR ");
        final String clause = getSetKeyColumnName() + " = ?";
        for (final String set : sets) {
            parameters[index++] = set;
            setsJoiner.add(clause);
        }
        final String sql = String.format("SELECT %s, %s FROM %s WHERE %s BETWEEN ? AND ? AND %s %s %s",
                quote(getEntryColumnName()),
                quote(getWeightColumnName()),
                getTableFullName(namespace, getSetsTableName()),
                quote(getWeightColumnName()),
                setsJoiner.toString(),
                orderby,
                getLimitString(start, count)
        );
        final Map<String, Long> items = new LinkedHashMap<>();
        try (final Connection connection = getConnection()) {
            logger.debug("executing sql query: {}", sql);
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                addParameters(preparedStatement, parameters);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final String key = resultSet.getString(1);
                        final Long weight = resultSet.getLong(2);
                        if (key != null && !key.isEmpty()) {
                            items.put(key, weight);
                        }
                    }
                }
                return items;
            }
        } catch (final SQLException e) {
            logger.warn("exception on sets.intersect()", e);
            throw new IOException(e);
        }
    }

    private Map<String, Long> doPop(final String namespace,
                                    final String set,
                                    final long min,
                                    final long max,
                                    final int start,
                                    final int count,
                                    final String orderby) throws IOException {
        final String selectSql = String.format("SELECT %s, %s FROM %s WHERE %s.%s = ? AND %s.%s BETWEEN ? AND ? %s %s FOR UPDATE",
                quote(getEntryColumnName()),
                quote(getWeightColumnName()),
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getSetsTableName()),
                quote(getWeightColumnName()),
                orderby,
                getLimitString(start, count)
        );
        Connection connection = null;
        try {
            // open transaction
            connection = openTransaction(getConnection());

            do {
                final Map<String, Long> items = new LinkedHashMap<>();
                try (final PreparedStatement selectPreparedStatement = connection.prepareStatement(selectSql)) {
                    selectPreparedStatement.setString(1, set);
                    selectPreparedStatement.setLong(2, min);
                    selectPreparedStatement.setLong(3, max);
                    // fetch entries
                    try (final ResultSet resultSet = selectPreparedStatement.executeQuery()) {
                        while (resultSet.next()) {
                            final String key = resultSet.getString(1);
                            final Long weight = resultSet.getLong(2);
                            if (key != null && !key.isEmpty()) {
                                items.put(key, weight);
                            }
                        }
                    }
                    if (items.isEmpty()) {
                        return Collections.emptyMap();
                    }

                    // delete fetched entries
                    final String deleteSql = String.format("DELETE FROM %s WHERE %s.%s = ? AND %s.%s IN (%s) ",
                            getTableFullName(namespace, getSetsTableName()),
                            quote(getSetsTableName()),
                            quote(getSetKeyColumnName()),
                            quote(getSetsTableName()),
                            quote(getEntryColumnName()),
                            getPlaceholders(items.size())
                    );
                    try (final PreparedStatement deletePreparedStatement = connection.prepareStatement(deleteSql)) {
                        int index = 1;
                        deletePreparedStatement.setString(index++, set);
                        for (final String entry : items.keySet()) {
                            deletePreparedStatement.setString(index++, entry);
                        }

                        final int deletedRows = deletePreparedStatement.executeUpdate();
                        if (deletedRows != items.size()) {
                            throw new SQLTransactionRollbackException("retry");
                        }
                        return items;
                    }
                } catch (SQLTransactionRollbackException e) {
                    // retry
                }
            } while (true);
        } catch (final SQLException e) {
            logger.warn("exception on sets.pop()", e);
            throw new IOException(e);
        } finally {
            closeConnection(connection);
        }
    }

    private List<String> doEntries(final String namespace,
                                   final String set,
                                   long min,
                                   long max,
                                   final int start,
                                   final int count,
                                   final String orderby) throws IOException {
        final String sql = String.format("SELECT %s FROM %s WHERE %s.%s = ? AND %s.%s BETWEEN ? AND ? %s %s",
                quote(getEntryColumnName()),
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getSetsTableName()),
                quote(getWeightColumnName()),
                orderby,
                getLimitString(start, count)
        );
        final List<String> entries = new LinkedList<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, set);
                preparedStatement.setLong(2, min);
                preparedStatement.setLong(3, max);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final String key = resultSet.getString(1);
                        if (key != null && !key.isEmpty()) {
                            entries.add(key);
                        }
                    }
                }
                return entries;
            }
        } catch (final SQLException e) {
            logger.warn("exception on sets.entries()", e);
            throw new IOException(e);
        }
    }

    private List<String> doSets(final String namespace) throws IOException {
        final String sql = String.format("SELECT DISTINCT %s FROM %s",
                quote(getSetKeyColumnName()),
                getTableFullName(namespace, getSetsTableName())
        );
        final List<String> sets = new LinkedList<>();
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        final String set = resultSet.getString(1);
                        if (set != null && !set.isEmpty()) {
                            sets.add(set);
                        }
                    }
                }
                return sets;
            }
        } catch (final SQLException e) {
            logger.warn("exception on sets.sets()", e);
            throw new IOException(e);
        }
    }

    private void doDelete(final String namespace, final String key, final long min, final long max) throws IOException {
        final String sql = String.format("DELETE FROM %s WHERE %s = ? AND %s BETWEEN ? AND ? ",
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getWeightColumnName())
        );
        executeUpdate(sql, key, min, max);
    }

    private boolean doDelete(final String namespace, final String set, final String entry) throws IOException {
        final String sql = String.format("DELETE FROM %s WHERE %s = ? AND %s = ? ",
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getEntryColumnName())
        );
        return executeUpdate(sql, set, entry) == 1;
    }

    private void doDelete(final String namespace, final String set, final Collection<String> entries) throws IOException {
        final String sql = String.format("DELETE FROM %s WHERE %s = ? AND %s IN (%s) ",
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getEntryColumnName()),
                getPlaceholders(entries.size())
        );
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, set);
                int placeholderIndex = 2;
                for (final String entry : entries) {
                    preparedStatement.setString(placeholderIndex, entry);
                    placeholderIndex += 1;
                }
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            logger.warn("exception on sets.delete()", e);
            throw new IOException(e);
        }
    }

    private Long doWeight(final String namespace, final String set, final String entry) throws IOException {
        final String sql = String.format("SELECT %s FROM %s WHERE %s = ? AND %s = ? ",
                quote(getWeightColumnName()),
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getEntryColumnName())
        );
        try (final Connection connection = getConnection()){
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, set);
                preparedStatement.setString(2, entry);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getLong(1);
                    }
                    return null;
                }
            }
        } catch (final SQLException e) {
            logger.warn("exception on sets.weight()", e);
            throw new IOException(e);
        }
    }

    private void doInc(final String namespace, final String set, final String entry, final long count) throws IOException {
        final String sql = String.format("UPDATE %s SET %s = %s + ? WHERE %s = ? AND %s = ? ",
                getTableFullName(namespace, getSetsTableName()),
                quote(getWeightColumnName()),
                quote(getWeightColumnName()),
                quote(getSetKeyColumnName()),
                quote(getEntryColumnName())
        );
        executeUpdate(sql, count, set, entry);
    }

    private void doAdd(final String namespace, final String set, final String entry, final long weight) throws IOException {
        final String sql = String.format("INSERT INTO %s SET %s = ?, %s = ?, %s = ? ON DUPLICATE KEY UPDATE %s= ? ",
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getEntryColumnName()),
                quote(getWeightColumnName()),
                quote(getWeightColumnName())
        );
        executeUpdate(sql, set, entry, weight, weight);
    }

    private void doAdd(final String namespace, final String set, final Map<String, Long> entries) throws IOException {
        final String sql = String.format("INSERT INTO %s SET %s = ?, %s = ?, %s = ? ON DUPLICATE KEY UPDATE %s= ? ",
                getTableFullName(namespace, getSetsTableName()),
                quote(getSetKeyColumnName()),
                quote(getEntryColumnName()),
                quote(getWeightColumnName()),
                quote(getWeightColumnName())
        );
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                for (final Map.Entry<String, Long> entry : entries.entrySet()) {
                    preparedStatement.clearParameters();
                    preparedStatement.setString(1, set);
                    preparedStatement.setString(2, entry.getKey());
                    preparedStatement.setLong(3, entry.getValue());
                    preparedStatement.setLong(4, entry.getValue());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        } catch (SQLException e) {
            logger.warn("exception on sets.add()", e);
            throw new IOException(e);
        }
    }

    protected abstract String getCreateSetsTableSql(final String namespace);

    private String getOrderByString(final boolean ascending) {
        final String order = ascending ? " ASC " : " DESC ";
        return " ORDER BY " + getWeightColumnName() + " " + order + " ";
    }

    private String getLimitString(final int start, final int count) {
        if (start == 0 && count == -1) {
            return " ";
        }
        return " LIMIT " + start + "," + count;
    }

    protected String getSetsTableName() {
        return "CANTOR-SETS";
    }

    protected String getEntryColumnName() {
        return "ENTRY";
    }

    protected String getSetKeyColumnName() {
        return "SET-KEY";
    }

    protected String getWeightColumnName() {
        return "WEIGHT";
    }
}

