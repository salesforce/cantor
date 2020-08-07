package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.jdbc.AbstractBaseEventsOnJdbc;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import com.sun.org.apache.xpath.internal.operations.Mult;
import org.apache.commons.collections4.map.MultiKeyMap;

public class EventsOnPhoenix extends AbstractBaseEventsOnJdbc implements Events {

    //TODO: create tables and sequences in one batch
    /** @inheritdoc */
    public EventsOnPhoenix(final DataSource dataSource) throws IOException {
        super(dataSource);
        String createMainTableSql = "create table if not exists cantor_events (timestampMillis BIGINT not null, " +
                "id BIGINT not null, namespace VARCHAR, payload VARBINARY CONSTRAINT pk PRIMARY KEY (timestampMillis, id))";
        String createMetadataTableSql = "create table if not exists cantor_events_m (timestampMillis BIGINT, " +
                "id BIGINT, m_key VARCHAR, m_value VARCHAR, iid BIGINT PRIMARY KEY)";
        String createDimensionsTableSql = "create table if not exists cantor_events_d (timestampMillis BIGINT, " +
                "id BIGINT, d_key VARCHAR, d_value DOUBLE, iid BIGINT PRIMARY KEY)";
        String createNamespaceTableSql = "create table if not exists cantor_events_namespace (namespace VARCHAR CONSTRAINT pk PRIMARY KEY (namespace))";
        String createIDSeqSql = "create sequence if not exists cantor_events_id";
        String createMIDSeqSql = "create sequence if not exists cantor_events_m_id";
        String createDIDSeqSql = "create sequence if not exists cantor_events_d_id";
        executeUpdate(createMainTableSql);
        executeUpdate(createMetadataTableSql);
        executeUpdate(createDimensionsTableSql);
        executeUpdate(createNamespaceTableSql);
        executeUpdate(createIDSeqSql);
        executeUpdate(createMIDSeqSql);
        executeUpdate(createDIDSeqSql);
    }

    public EventsOnPhoenix(final String path) throws IOException {
        this(PhoenixDataSourceProvider.getDatasource(new PhoenixDataSourceProperties().setPath(path)));
    }

    //TODO: batchUpdate multiple statements w/ parameters
    /** @inheritdoc */
    @Override
    public void store(String namespace, Collection<Event> batch) throws IOException {
        String upsertMainSql = String.format("upsert into %s values (?, next value for %s, ?, ?)",
                getMainTableName(), getMainIdSequence());
        String upsertMetadataSql = String.format("upsert into %s values (?, current value for %s, ?, ?, next value for %s)",
                getMetadataTableName(), getMainIdSequence(), getMetadataIdSequence());
        String upsertDimensionsSql = String.format("upsert into %s values (?, current value for %s, ?, ?, next value for %s)",
                getDimensionsTableName(), getMainIdSequence(), getDimensionsIdSequence());
        List<Object[]> metadataParameters;
        List<Object[]> dimensionsParameters;
        Collection<Event> storedEvents = new ArrayList<>();

        // iterate through events and insert into table one by one
        for (Event e : batch) {
            try (final Connection connection = getConnection()) {
                if (namespace == null) {
                    throw new IllegalArgumentException("Namespace should not be null");
                } else if (e.getTimestampMillis() < 0) {
                    throw new IllegalArgumentException("Invalid timestamp: " + e.getTimestampMillis());
                } if (e.getMetadata().size() > 100) {
                    throw new IllegalArgumentException(String.format("Metadata size is %s, larger than 100",
                            e.getMetadata().size()));
                } else if (e.getDimensions().size() > 400) {
                    throw new IllegalArgumentException(String.format("Dimensions size is %s, larger than 400",
                            e.getDimensions().size()));
                } else if (!namespaces().contains(namespace)) {
                    throw new IOException("Namespace does not exist");
                }

                // insert event into main table
                executeUpdate(connection, upsertMainSql, e.getTimestampMillis(), namespace, (e.getPayload() == null) ?
                        new byte[0] : e.getPayload());

                // insert one event's metadata into metadata table
                metadataParameters = new ArrayList<>();
                for (Map.Entry<String, String> m : e.getMetadata().entrySet()) {
                    metadataParameters.add(new Object[]{e.getTimestampMillis(), m.getKey(), m.getValue()});
                }
                executeBatchUpdate(connection, upsertMetadataSql, metadataParameters);

                // insert one event's dimensions into dimensions table
                dimensionsParameters = new ArrayList<>();
                for (Map.Entry<String, Double> d : e.getDimensions().entrySet()) {
                    dimensionsParameters.add(new Object[]{e.getTimestampMillis(), d.getKey(), d.getValue()});
                }
                executeBatchUpdate(connection, upsertDimensionsSql, dimensionsParameters);
                storedEvents.add(e);
            } catch (RuntimeException exception) {
                // when one of the events does not get stored properly, from giving illegal arguments or otherwise
                // other events from the batch already added must be removed
                rollback(namespace, storedEvents);
                throw exception;
            } catch (SQLException exception) {
                rollback(namespace, storedEvents);
                throw new IOException(exception);
            }
        }
    }

    /**
     * Since executeUpdate can only take either multiple statements or multiple parameters but not both, we are storing
     * events one by one using executeUpdate. Since events are stored sequentially, we need to manually rollback in
     * times of failure. Here we delete previously stored events from the batch when one or more events could not get
     * stored, due to database connection issues, illegal arguments, etc.
     * @param namespace to be previously stored to and roll-backed from
     * @param batch collection of events already stored from the same batch before failure
     * @throws IOException
     */
    private void rollback(String namespace, Collection<Event> batch) throws IOException {
        StringBuilder selectQuery;
        List<Object> parameterList;
        Object[] parameters;
        for (Event e: batch) {
            selectQuery = new StringBuilder(String.format("select e.%s, e.%s from %s as e ", getTimestampColumnName(),
                    getIdColumnName(), getMainTableName()));
            parameterList = new ArrayList<>();
            parameters = buildQueryAndParamOnSubqueries(selectQuery, parameterList, e.getTimestampMillis(),
                    e.getTimestampMillis() + 1, namespace, e.getMetadata(), getQuery(e.getDimensions()));
            try (final Connection connection = getConnection()) {
                // first find the timestamp and id of all entries that are to be deleted
                try (final PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString())) {
                    addParameters(preparedStatement, parameters);
                    try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {
                        StringBuilder deleteMainSql = new StringBuilder(String.format("delete from %s where %s = ? and %s = ?",
                                getMainTableName(), getTimestampColumnName(), getIdColumnName()));
                        StringBuilder deleteMetadataSql = new StringBuilder(String.format("delete from %s where %s = ? and %s = ?",
                                getMetadataTableName(), getTimestampColumnName(), getIdColumnName()));
                        StringBuilder deleteDimensionsSql = new StringBuilder(String.format("delete from %s where %s = ? and %s = ?",
                                getDimensionsTableName(), getTimestampColumnName(), getIdColumnName()));

                        parameters = null;
                        while (mainResultSet.next()) { // store the timestamp and id of all to-be-deleted entries in mainResultSet
                            parameters = new Object[]{e.getTimestampMillis(),
                                    mainResultSet.getLong(String.format("e.%s", getIdColumnName()))};
                        }

                        if (parameters != null) {
                            executeUpdate(deleteMetadataSql.toString(), parameters);
                            executeUpdate(deleteDimensionsSql.toString(), parameters);
                            executeUpdate(deleteMainSql.toString(), parameters);
                        }
                    }
                }
            } catch (SQLException exception) {
                throw new IOException(exception);
            }
        }
    }

    /**
     * Take a dimensions map and turn it into a dimensions query map that match events containing the "exact" values
     * Using quotes here because dimensions query does not support exact match, so using <= instead of ==
     * but hopefully with metadata query and timestamp/namespace requirement it is enough to get only the events needed
     * @param dimensions map of dimensions key/value pairs
     * @return map of dimensions query for select queries
     */
    private Map<String, String> getQuery(Map<String, Double> dimensions) {
        Map<String, String> query = new HashMap<>();
        for (Map.Entry<String, Double> d : dimensions.entrySet()) {
            query.put(d.getKey(), "<=" + d.getValue());
        }
        return query;
    }

    /** @inheritdoc */
    @Override
    public List<Event> get(String namespace, long startTimestampMillis, long endTimestampMillis,
                           Map<String, String> metadataQuery, Map<String, String> dimensionsQuery,
                           boolean includePayloads, boolean ascending, int limit) throws IOException {
        List<Event> events = new ArrayList<>();
        // the query to select from main table, timestamp and id (and payload if applicable) that fulfills the restriction
        // on timestamp, id, namespace, metadata and dimensions
        StringBuilder query = new StringBuilder();
        // build the select statement(in query variable) and the array of corresponding parameters
        Object[] parameters = buildSelectQuery(query, new ArrayList<>(), namespace, startTimestampMillis,
                endTimestampMillis, metadataQuery, dimensionsQuery, includePayloads, ascending, limit);

        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
                addParameters(preparedStatement, parameters);
                try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {
                    // select from metadata and dimensions table entries associated with eligible timestamp + id
                    // returned by selection from main table
                    StringBuilder metadataSql = new StringBuilder(String.format("select %s, %s, %s, %s from %s where (%s, %s) in (",
                            getTimestampColumnName(), getIdColumnName(), getMetadataKeyColumnName(),
                            getMetadataValueColumnName(), getMetadataTableName(), getTimestampColumnName(), getIdColumnName()));
                    StringBuilder dimensionsSql = new StringBuilder(String.format("select %s, %s, %s, %s from %s where (%s, %s) in (",
                            getTimestampColumnName(), getIdColumnName(), getDimensionsKeyColumnName(),
                            getDimensionsValueColumnName(), getDimensionsTableName(), getTimestampColumnName(), getIdColumnName()));
                    StringJoiner timeIdPairString = new StringJoiner(", ");
                    List<Object[]> mainResultMap = new ArrayList<>();

                    boolean isEmpty = true;
                    while (mainResultSet.next()) { // building metadataSql and dimensionsSql with eligible timestamp + id entries
                        isEmpty = false;
                        long timestamp = mainResultSet.getLong(String.format("e.%s", getTimestampColumnName()));
                        long id = mainResultSet.getLong(String.format("e.%s", getIdColumnName()));
                        timeIdPairString.add("(" + timestamp + ", " + id + ")");
                        mainResultMap.add(new Object[]{timestamp, id, (includePayloads) ?
                                mainResultSet.getBytes(String.format("e.%s", getPayloadColumnName())) : null});
                    }

                    // if nothing is selected from main table, do not need to further look into metadata/dimensions table
                    if (isEmpty) {
                        return events;
                    }

                    metadataSql.append(timeIdPairString.toString()).append(")");
                    dimensionsSql.append(timeIdPairString.toString()).append(")");

                    // use multikey map here because of composite key are used to index data from tables(timestamp and id)
                    // select valid metadata associated with valid timestamp and id and store them in metadataResultMap
                    MultiKeyMap<Long, Map<String, String>> metadataResultMap = populateMetadataResultMap(connection,
                            metadataSql, new MultiKeyMap<>());
                    // select valid dimensions associated with valid timestamp and id and store them in dimensionsResultMap
                    MultiKeyMap<Long, Map<String, Double>> dimensionsResultMap = populateDimensionsResultMap(connection,
                            dimensionsSql, new MultiKeyMap<>());

                    for (Object[] entry : mainResultMap) {
                        Event e = new Event((long)entry[0], metadataResultMap.get(entry[0], entry[1]),
                                dimensionsResultMap.get(entry[0], entry[1]), (includePayloads) ? (byte[])entry[2] : null);
                        events.add(e);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return events;
    }

    /**
     * Get all the metadata for events selected, indexed by timestamp and id.
     */
    private MultiKeyMap<Long, Map<String, String>> populateMetadataResultMap(Connection connection,
                                                                             StringBuilder metadataSql,
                                                                             MultiKeyMap<Long, Map<String,
                                                                                     String>> metadataResultMap)
            throws SQLException {
        try(final PreparedStatement preparedStatementM = connection.prepareStatement(metadataSql.toString())) {
            try (final ResultSet metadataResultSet = preparedStatementM.executeQuery()) {
                while (metadataResultSet.next()) {
                    long timestamp = metadataResultSet.getLong(getTimestampColumnName());
                    long id = metadataResultSet.getLong(getIdColumnName());
                    if (metadataResultMap.containsKey(timestamp, id)) {
                        metadataResultMap.get(timestamp, id).put(metadataResultSet.getString(getMetadataKeyColumnName()),
                                metadataResultSet.getString(getMetadataValueColumnName()));
                    } else {
                        metadataResultMap.put(timestamp, id, new HashMap<String, String>() {{
                            put(metadataResultSet.getString(getMetadataKeyColumnName()),
                                    metadataResultSet.getString(getMetadataValueColumnName())); }});
                    }
                }
            }
        }
        return metadataResultMap;
    }

    /**
     * Get all the dimensions for events selected, indexed by timestamp and id.
     */
    private MultiKeyMap<Long, Map<String, Double>> populateDimensionsResultMap(Connection connection,
                                                                               StringBuilder dimensionsSql,
                                                                               MultiKeyMap<Long, Map<String, Double>>
                                                                               dimensionsResultMap) throws SQLException {
        try(final PreparedStatement preparedStatementD = connection.prepareStatement(dimensionsSql.toString())) {
            try (final ResultSet dimensionsResultSet = preparedStatementD.executeQuery()) {
                while (dimensionsResultSet.next()) {
                    long timestamp = dimensionsResultSet.getLong(getTimestampColumnName());
                    long id = dimensionsResultSet.getLong(getIdColumnName());
                    if (dimensionsResultMap.containsKey(timestamp, id)) {
                        dimensionsResultMap.get(timestamp, id).put(dimensionsResultSet.getString(getDimensionsKeyColumnName()),
                                dimensionsResultSet.getDouble(getDimensionsValueColumnName()));
                    } else {
                        dimensionsResultMap.put(timestamp, id, new HashMap<String, Double>() {{
                            put(dimensionsResultSet.getString(getDimensionsKeyColumnName()),
                                    dimensionsResultSet.getDouble(getDimensionsValueColumnName())); }});
                    }
                }
            }
        }
        return dimensionsResultMap;
    }

    /**
     * Build the full SQL query for selecting required events and return corresponding parameters that go with the query
     */
    private Object[] buildSelectQuery(StringBuilder query, List<Object> parameterList, String namespace,
                                      long startTimestampMillis, long endTimestampMillis,
                                      Map<String, String> metadataQuery, Map<String, String> dimensionsQuery,
                                      boolean includePayloads, boolean ascending, int limit) {
        if (includePayloads) {
            query.append(String.format("select e.%s, e.%s, e.%s from %s as e ", getTimestampColumnName(),
                    getIdColumnName(), getPayloadColumnName(), getMainTableName()));
        } else {
            query.append(String.format("select e.%s, e.%s from %s as e ", getTimestampColumnName(), getIdColumnName(),
                    getMainTableName()));
        }

        // build up the query and parameterList from metadataQuery, dimensionsQuery, timestamps and namespace
        Object[] parameters = buildQueryAndParamOnSubqueries(query, parameterList, startTimestampMillis, endTimestampMillis,
                namespace, metadataQuery, dimensionsQuery);

        if (ascending) {
            query.append(String.format("order by e.%s asc", getTimestampColumnName()));
        } else {
            query.append(String.format("order by e.%s desc", getTimestampColumnName()));
        }
        if (limit > 0) {
            query.append(" limit ").append(limit);
        }

        return parameters;
    }

    /**
     * Build query and populate parameterList based on timestamps, namespace and queries for metadata and/or dimensions if applicable.
     * Returned is an array converted from ParameterList.
     */
    private Object[] buildQueryAndParamOnSubqueries(StringBuilder query, List<Object> parameterList,
                                                    long startTimestampMillis, long endTimestampMillis, String namespace,
                                                    Map<String, String> metadataQuery, Map<String, String> dimensionsQuery) {
        if ((metadataQuery == null || metadataQuery.isEmpty()) && (dimensionsQuery == null || dimensionsQuery.isEmpty())) {
            parameterList.addAll(Arrays.asList(startTimestampMillis, endTimestampMillis, namespace));
            query.append(String.format("where %s between ? and ? and %s = ? ", getTimestampColumnName(),
                    getNamespaceColumnName()));
        } else {
            if (metadataQuery == null || metadataQuery.isEmpty()) {
                addDimQueriesToQueryAndParam(query, parameterList, dimensionsQuery);
                query.append(String.format("where d.%s is null and d.%s is null ", getTimestampColumnName(),
                        getIdColumnName()));
            } else if (dimensionsQuery == null || dimensionsQuery.isEmpty()) {
                addMetaQueriesToQueryAndParam(query, parameterList, metadataQuery);
                query.append(String.format("where m.%s is null and m.%s is null ", getTimestampColumnName(),
                        getIdColumnName()));
            } else {
                addDimQueriesToQueryAndParam(query, parameterList, dimensionsQuery);
                addMetaQueriesToQueryAndParam(query, parameterList, metadataQuery);
                query.append(String.format("where d.%s is null and d.%s is null and m.%s is null and m.%s is null ",
                        getTimestampColumnName(), getIdColumnName(), getTimestampColumnName(), getIdColumnName()));
            }

            // add conditions in select query to ensure all keys appeared in metadataQuery and dimensionsQuery
            // are presented in all event entries returned
            addConditionsForKeyExisting(query, parameterList, metadataQuery, true);
            addConditionsForKeyExisting(query, parameterList, dimensionsQuery, false);

            query.append(String.format("and e.%s between ? and ? and %s = ? ", getTimestampColumnName(),
                    getNamespaceColumnName()));
            parameterList.add(startTimestampMillis);
            parameterList.add(endTimestampMillis);
            parameterList.add(namespace);
        }
        return parameterList.toArray(new Object[parameterList.size()]);
    }

    /**
     * Build query and populate parameterList based on metadataQuery argument in the main call(get, delete, etc).
     */
    private void addMetaQueriesToQueryAndParam(StringBuilder query, List<Object> parameterList, Map<String, String> queryMap) {
        StringJoiner subqueries = new StringJoiner(" ");
        query.append(String.format("left join (select %s, %s from %s where (case ", getTimestampColumnName(),
                getIdColumnName(), getMetadataTableName()));

        for (Map.Entry<String, String> q : queryMap.entrySet()) {
            parameterList.add(q.getKey());
            if (q.getValue().startsWith("~")) {
                subqueries.add(String.format("when %s = ? then (LENGTH(REGEXP_SUBSTR(%s, ?)) is null or LENGTH(REGEXP_SUBSTR(%s, ?)) != LENGTH(%s))",
                        getMetadataKeyColumnName(), getMetadataValueColumnName(), getMetadataValueColumnName(),
                        getMetadataValueColumnName()));
                if (!q.getValue().contains(".*")) {
                    parameterList.add(q.getValue().substring(1).replace("*", ".*"));
                    parameterList.add(q.getValue().substring(1).replace("*", ".*"));
                } else {
                    parameterList.add(q.getValue().substring(1));
                    parameterList.add(q.getValue().substring(1));
                }
            } else if (q.getValue().startsWith("!~")) {
                subqueries.add(String.format("when %s = ? then LENGTH(REGEXP_SUBSTR(%s, ?)) = LENGTH(%s)",
                        getMetadataKeyColumnName(), getMetadataValueColumnName(), getMetadataValueColumnName()));
                if (!q.getValue().contains(".*")) {
                    parameterList.add(q.getValue().substring(2).replace("*", ".*"));
                } else {
                    parameterList.add(q.getValue().substring(2));
                }
            } else if (q.getValue().startsWith("!")) { // exact not
                subqueries.add(String.format("when %s = ? then %s = ?", getMetadataKeyColumnName(),
                        getMetadataValueColumnName()));
                parameterList.add(q.getValue().substring(1));
            } else { // exact match
                subqueries.add(String.format("when %s = ? then %s != ?", getMetadataKeyColumnName(),
                        getMetadataValueColumnName()));
                if (q.getValue().startsWith("=")) {
                    parameterList.add(q.getValue().substring(1));
                } else {
                    parameterList.add(q.getValue());
                }
            }
        }
        query.append(subqueries.toString()).append(String.format(" end)) as m on e.%s = m.%s and e.%s = m.%s ",
                getTimestampColumnName(), getTimestampColumnName(), getIdColumnName(), getIdColumnName()));
    }

    /**
     * Build query and populate parameterList based on dimensionsQuery argument in the main call(get, delete, etc).
     */
    private void addDimQueriesToQueryAndParam(StringBuilder query, List<Object> parameterList,
                                              Map<String, String> queryMap) {
        StringJoiner subqueries = new StringJoiner(" ");
        query.append(String.format("left join (select %s, %s from %s where (case ", getTimestampColumnName(),
                getIdColumnName(), getDimensionsTableName()));

        for (Map.Entry<String, String> q : queryMap.entrySet()) {
            parameterList.add(q.getKey());
            if (q.getValue().startsWith("<=")) {
                subqueries.add(String.format("when %s = ? then %s > ?", getDimensionsKeyColumnName(),
                        getDimensionsValueColumnName()));
                parameterList.add(Double.parseDouble(q.getValue().substring(2)));
            } else if (q.getValue().startsWith(">=")) {
                subqueries.add(String.format("when %s = ? then %s < ?", getDimensionsKeyColumnName(),
                        getDimensionsValueColumnName()));
                parameterList.add(Double.parseDouble(q.getValue().substring(2)));
            } else if (q.getValue().startsWith("<")) {
                subqueries.add(String.format("when %s = ? then %s >= ?", getDimensionsKeyColumnName(),
                        getDimensionsValueColumnName()));
                parameterList.add(Double.parseDouble(q.getValue().substring(1)));
            } else if (q.getValue().startsWith(">")) {
                subqueries.add(String.format("when %s = ? then %s <= ?", getDimensionsKeyColumnName(),
                        getDimensionsValueColumnName()));
                parameterList.add(Double.parseDouble(q.getValue().substring(1)));
            } else { // between and
                subqueries.add(String.format("when %s = ? then %s not between ? and ?", getDimensionsKeyColumnName(),
                        getDimensionsValueColumnName()));
                parameterList.add(Double.parseDouble(q.getValue().split("\\.\\.")[0]));
                parameterList.add(Double.parseDouble(q.getValue().split("\\.\\.")[1]));
            }
        }
        query.append(subqueries.toString()).append(String.format(" end)) as d on e.%s = d.%s and e.%s = d.%s ",
                getTimestampColumnName(), getTimestampColumnName(), getIdColumnName(), getIdColumnName()));
    }

    /**
     * This part of the query is necessary to make sure all keys in metadata and dimension queries actually exist
     * in metadata/dimensions of events selected.
     */
    private void addConditionsForKeyExisting(StringBuilder query, List<Object> parameterList,
                                             Map<String, String> queryMap, boolean isMetadataQeury) {
        if (queryMap != null) {
            for (String key : queryMap.keySet()) {
                if (isMetadataQeury){
                    query.append(String.format("and exists (select 1 from %s as m where %s = ? and m.%s = e.%s and m.%s = e.%s) ",
                            getMetadataTableName(), getMetadataKeyColumnName(), getTimestampColumnName(),
                            getTimestampColumnName(), getIdColumnName(), getIdColumnName()));
                } else {
                    query.append(String.format("and exists (select 1 from %s as d where %s = ? and d.%s = e.%s and d.%s = e.%s) ",
                            getDimensionsTableName(), getDimensionsKeyColumnName(), getTimestampColumnName(),
                            getTimestampColumnName(), getIdColumnName(), getIdColumnName()));
                }
                parameterList.add(key);
            }
        }
    }

    /** @inheritdoc */
    @Override
    public int delete(String namespace, long startTimestampMillis, long endTimestampMillis,
                      Map<String, String> metadataQuery, Map<String, String> dimensionsQuery) throws IOException {
        StringBuilder selectQuery = new StringBuilder(String.format("select e.%s, e.%s from %s as e ",
                getTimestampColumnName(), getIdColumnName(), getMainTableName()));
        List<Object> parameterList = new ArrayList<>();
        Object[] parameters = buildQueryAndParamOnSubqueries(selectQuery, parameterList, startTimestampMillis,
                endTimestampMillis, namespace, metadataQuery, dimensionsQuery);
        int deletedRows;

        try (final Connection connection = getConnection()) {
            // first find the timestamp and id of all entries that are to be deleted
            try (final PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString())) {
                addParameters(preparedStatement, parameters);
                try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {
                    StringBuilder deleteMainSql = new StringBuilder(String.format("delete from %s where (%s, %s) in (",
                            getMainTableName(), getTimestampColumnName(), getIdColumnName()));
                    StringBuilder deleteMetadataSql = new StringBuilder(String.format("delete from %s where (%s, %s) in (",
                            getMetadataTableName(), getTimestampColumnName(), getIdColumnName()));
                    StringBuilder deleteDimensionsSql = new StringBuilder(String.format("delete from %s where (%s, %s) in (",
                            getDimensionsTableName(), getTimestampColumnName(), getIdColumnName()));
                    StringJoiner timeIdPairString = new StringJoiner(", ");

                    boolean isEmpty = true;
                    // store the timestamp and id of all to-be-deleted entries in mainResultSet
                    while (mainResultSet.next()) {
                        isEmpty = false;
                        long timestamp = mainResultSet.getLong(String.format("e.%s", getTimestampColumnName()));
                        long id = mainResultSet.getLong(String.format("e.%s", getIdColumnName()));
                        timeIdPairString.add("(" + timestamp + ", " + id + ")");
                    }

                    if (isEmpty) {
                        return 0;
                    }

                    deleteMainSql.append(timeIdPairString.toString()).append(")");
                    deleteMetadataSql.append(timeIdPairString.toString()).append(")");
                    deleteDimensionsSql.append(timeIdPairString.toString()).append(")");

                    executeUpdate(deleteMetadataSql.toString());
                    executeUpdate(deleteDimensionsSql.toString());
                    deletedRows = executeUpdate(deleteMainSql.toString());
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return deletedRows;
    }

    /** @inheritdoc */
    @Override
    public Set<String> metadata(String namespace, String metadataKey, long startTimestampMillis,
                                long endTimestampMillis, Map<String, String> metadataQuery,
                                Map<String, String> dimensionsQuery) throws IOException {
        Set<String> results = new HashSet<>();
        StringBuilder query = new StringBuilder(String.format("select distinct %s from %s where %s = ? and (%s, %s) in (",
                getMetadataValueColumnName(), getMetadataTableName(), getMetadataKeyColumnName(),
                getTimestampColumnName(), getIdColumnName()));
        List<Object> parameterList = new ArrayList<>();
        Object[] parameters;
        parameterList.add(metadataKey);
        query.append(String.format("select e.%s, e.%s from %s as e ", getTimestampColumnName(), getIdColumnName(),
                getMainTableName()));
        parameters = buildQueryAndParamOnSubqueries(query, parameterList, startTimestampMillis, endTimestampMillis,
                namespace, metadataQuery, dimensionsQuery);
        query.append(")");

        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
                addParameters(preparedStatement, parameters);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        results.add(resultSet.getString(getMetadataValueColumnName()));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return results;
    }

    /** @inheritdoc */
    @Override
    public Collection<String> namespaces() throws IOException {
        Collection<String> result = new HashSet<>();
        String query = String.format("select * from %s", getNamespaceTableName());
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        result.add(resultSet.getString(getNamespaceColumnName()));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return result;
    }

    /** @inheritdoc */
    @Override
    public void create(String namespace) throws IOException {
        String query = String.format("upsert into %s values (%s)", getNamespaceTableName(), quote(namespace));
        executeUpdate(query);
    }

    /** @inheritdoc */
    @Override
    public void drop(String namespace) throws IOException {
        Connection connection = null;
        try {
            String query = String.format("delete from %s where %s = %s", getNamespaceTableName(),
                    getNamespaceColumnName(), quote(namespace));
            String deleteFromMetadataSql = String.format("delete from %s where (%s, %s) in (select %s, %s from %s where %s = %s)",
                    getMetadataTableName(), getTimestampColumnName(), getIdColumnName(), getTimestampColumnName(),
                    getIdColumnName(), getMainTableName(), getNamespaceColumnName(), quote(namespace));
            String deleteFromDimensionsSql = String.format("delete from %s where (%s, %s) in (select %s, %s from %s where %s = %s)",
                    getDimensionsTableName(), getTimestampColumnName(), getIdColumnName(), getTimestampColumnName(),
                    getIdColumnName(), getMainTableName(), getNamespaceColumnName(), quote(namespace));
            String deleteFromMainSql = String.format("delete from %s where %s = %s", getMainTableName(),
                    getNamespaceColumnName(), quote(namespace));
            connection = openTransaction(getConnection());
            executeUpdate(connection, query);
            executeUpdate(connection, deleteFromMetadataSql);
            executeUpdate(connection, deleteFromDimensionsSql);
            executeUpdate(connection, deleteFromMainSql);
        } finally {
            closeConnection(connection);
        }
    }

    private String quote(String in) {
        return String.format("'%s'", in);
    }

    @Override
    protected String getPayloadColumnName() {
        return "PAYLOAD";
    }

    @Override
    protected String getNamespaceColumnName() {
        return "NAMESPACE";
    }

    private String getTimestampColumnName() {
        return "TIMESTAMPMILLIS";
    }

    private String getIdColumnName() {
        return "ID";
    }

    private String getMetadataKeyColumnName() {
        return "M_KEY";
    }

    private String getMetadataValueColumnName() {
        return "M_VALUE";
    }

    private String getDimensionsKeyColumnName() {
        return "D_KEY";
    }

    private String getDimensionsValueColumnName() {
        return "D_VALUE";
    }

    private String getMainTableName() {
        return "CANTOR_EVENTS";
    }

    private String getMetadataTableName() {
        return "CANTOR_EVENTS_M";
    }

    private String getDimensionsTableName() {
        return "CANTOR_EVENTS_D";
    }

    private String getNamespaceTableName() {
        return "CANTOR_EVENTS_NAMESPACE";
    }

    private String getMainIdSequence() {
        return "CANTOR_EVENTS_ID";
    }

    private String getMetadataIdSequence() {
        return "CANTOR_EVENTS_M_ID";
    }

    private String getDimensionsIdSequence() {
        return "CANTOR_EVENTS_D_ID";
    }

    public static void addParameters(final PreparedStatement preparedStatement, final Object... parameters)
            throws SQLException {
        if (parameters == null) {
            return;
        }
        int index = 0;
        for (final Object param : parameters) {
            index++;
            if (param instanceof Integer) {
                preparedStatement.setInt(index, (Integer) param);
            } else if (param instanceof Long) {
                preparedStatement.setLong(index, (Long) param);
            } else if (param instanceof Boolean) {
                preparedStatement.setBoolean(index, (Boolean) param);
            } else if (param instanceof String) {
                preparedStatement.setString(index, (String) param);
            } else if (param instanceof Double) {
                preparedStatement.setDouble(index, (Double) param);
            } else if (param instanceof Float) {
                preparedStatement.setFloat(index, (Float) param);
            } else if (param instanceof byte[]) {
                preparedStatement.setBytes(index, (byte[]) param);
            } else {
                throw new IllegalStateException("invalid parameter type: " + param);
            }
        }
    }

    /** @inheritdoc */
    @Override
    public Map<Long, Double> aggregate(String namespace, String dimension, long startTimestampMillis,
                                       long endTimestampMillis, Map<String, String> metadataQuery,
                                       Map<String, String> dimensionsQuery, int aggregateIntervalMillis,
                                       AggregationFunction aggregationFunction) throws IOException {
        return Collections.emptyMap();
    }

    /** @inheritdoc */
    @Override
    public void expire(String namespace, long endTimestampMillis) throws IOException {
    }

    @Override
    protected String getCreateChunkLookupTableSql(String namespace) {
        return null;
    }

    @Override
    protected String getCreateChunkTableSql(String chunkTableName, String namespace, Map<String, String> metadata,
                                            Map<String, Double> dimensions) {
        return null;
    }

    @Override
    protected String getRegexQuery(String column) {
        return null;
    }

    @Override
    protected String getNotRegexQuery(String column) {
        return null;
    }
}
