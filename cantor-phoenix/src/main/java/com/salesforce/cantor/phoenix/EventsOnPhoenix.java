package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.jdbc.AbstractBaseEventsOnJdbc;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.apache.commons.collections4.map.MultiKeyMap;

public class EventsOnPhoenix extends AbstractBaseEventsOnJdbc implements Events {

    //TODO: in the future create tables and sequences in one batch
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
    //TODO: if one event failed, are other events rolled back?
    //TODO: if inserting into metadata/dimensions failed, how to roll back inserting into previous table(s)?
    @Override
    public void store(String namespace, Collection<Event> batch) throws IOException {
        String upsertMainSql = "upsert into cantor_events values (?, next value for cantor_events_id, ?, ?)";
        String upsertMetadataSql = "upsert into cantor_events_m values (?, current value for cantor_events_id, ?, ?, next value for cantor_events_m_id)";
        String upsertDimensionsSql = "upsert into cantor_events_d values (?, current value for cantor_events_id, ?, ?, next value for cantor_events_d_id)";
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
                    throw new IllegalArgumentException(String.format("Metadata size is %s, larger than 100", e.getMetadata().size()));
                } else if (e.getDimensions().size() > 400) {
                    throw new IllegalArgumentException(String.format("Dimensions size is %s, larger than 400", e.getDimensions().size()));
                } else if (!namespaces().contains(namespace)) {
                    throw new IOException("Namespace does not exist");
                }

                // insert event into main table
                executeUpdate(connection, upsertMainSql, e.getTimestampMillis(), namespace, (e.getPayload() == null) ? new byte[0] : e.getPayload());

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
                exception.printStackTrace();
            }
        }
    }

    private void rollback(String namespace, Collection<Event> batch) throws IOException {
        StringBuilder selectQuery;
        List<Object> parameterList;
        Object[] parameters;
        for (Event e: batch) {
            selectQuery = new StringBuilder("select e.timestampMillis, e.id from cantor_events as e ");
            parameterList = new ArrayList<>();
            parameters = buildQueryAndParamOnSubqueries(selectQuery, parameterList, e.getTimestampMillis(), e.getTimestampMillis() + 1, namespace, e.getMetadata(), getQuery(e.getDimensions()));
            try (final Connection connection = getConnection()) {
                // first find the timestamp and id of all entries that are to be deleted
                try (final PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString())) {
                    addParameters(preparedStatement, parameters);
                    try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {
                        StringBuilder deleteMainSql = new StringBuilder("delete from cantor_events where timestampMillis = ? and id = ?");
                        StringBuilder deleteMetadataSql = new StringBuilder("delete from cantor_events_m where timestampMillis = ? and id = ?");
                        StringBuilder deleteDimensionsSql = new StringBuilder("delete from cantor_events_d where timestampMillis = ? and id = ?");

                        boolean isEmpty = true;
                        while (mainResultSet.next()) { // store the timestamp and id of all to-be-deleted entries in mainResultSet
                            isEmpty = false;
                            parameters = new Object[]{e.getTimestampMillis(), mainResultSet.getLong("e.id")};
                        }

                        if (!isEmpty) {
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

    private Map<String, String> getQuery(Map<String, Double> dimensions) {
        Map<String, String> query = new HashMap<>();
        for (Map.Entry<String, Double> d : dimensions.entrySet()) {
            query.put(d.getKey(), "<=" + d.getValue()); // dimensionsQuery does not support exact match, so using <= instead
        }
        return query;
    }

    @Override
    public List<Event> get(String namespace, long startTimestampMillis, long endTimestampMillis,
                           Map<String, String> metadataQuery, Map<String, String> dimensionsQuery,
                           boolean includePayloads, boolean ascending, int limit) throws IOException {
        List<Event> events = new ArrayList<>();
        // the query to select from main table, timestamp and id (and payload if applicable) that fulfills the restriction
        // on timestamp, id, namespace, metadata and dimensions
        StringBuilder query = new StringBuilder();
        // use List instead of array because the number of parameters is variable
        List<Object> parameterList = new ArrayList<>();
        Object[] parameters;

        if (includePayloads) {
            query.append("select /*+ USE_SORT_MERGE_JOIN*/ e.timestampMillis, e.id, e.payload from cantor_events as e ");
        } else {
            query.append("select /*+ USE_SORT_MERGE_JOIN*/ e.timestampMillis, e.id from cantor_events as e ");
        }

        // build up the query and parameterList from metadataQuery, dimensionsQuery, timestamps and namespace
        parameters = buildQueryAndParamOnSubqueries(query, parameterList, startTimestampMillis, endTimestampMillis, namespace, metadataQuery, dimensionsQuery);

        if (ascending) {
            query.append("order by e.timestampMillis asc");
        } else {
            query.append("order by e.timestampMillis desc");
        }
        if (limit > 0) {
            query.append(" limit ").append(limit);
        }

        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
                addParameters(preparedStatement, parameters);
                try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {
                    // select from metadata and dimensions table entries associated with eligible timestamp + id
                    // returned by selection from main table
                    StringBuilder metadataSql = new StringBuilder("select timestampMillis, id, m_key, m_value from cantor_events_m where (timestampMillis, id) in (");
                    StringBuilder dimensionsSql = new StringBuilder("select timestampMillis, id, d_key, d_value from cantor_events_d where (timestampMillis, id) in (");
                    StringJoiner timeIdPairString = new StringJoiner(", ");
                    List<Object[]> mainResultMap = new ArrayList<>();

                    boolean isEmpty = true;
                    while (mainResultSet.next()) { // building metadataSql and dimensionsSql with eligible timestamp + id entries
                        isEmpty = false;
                        long timestamp = mainResultSet.getLong("e.timestampMillis");
                        long id = mainResultSet.getLong("e.id");
                        timeIdPairString.add("(" + timestamp + ", " + id + ")");
                        mainResultMap.add(new Object[]{timestamp, id, (includePayloads) ? mainResultSet.getBytes("e.payload") : null});
                    }
                    // if nothing is selected from main table, do not need to further look into metadata/dimensions table
                    if (isEmpty) {
                        return events;
                    }

                    metadataSql.append(timeIdPairString.toString()).append(")");
                    dimensionsSql.append(timeIdPairString.toString()).append(")");
                    // use multikey map here because of composite key are used to index data from tables(timestamp and id)
                    MultiKeyMap<Long, Map<String, String>> metadataResultMap = new MultiKeyMap();
                    MultiKeyMap<Long, Map<String, Double>> dimensionsResultMap = new MultiKeyMap();

                    // select valid metadata associated with valid timestamp and id and store them in metadataResultMap
                    try(final PreparedStatement preparedStatementM = connection.prepareStatement(metadataSql.toString())) {
                        try (final ResultSet metadataResultSet = preparedStatementM.executeQuery()) {
                            while (metadataResultSet.next()) {
                                long timestamp = metadataResultSet.getLong("timestampMillis");
                                long id = metadataResultSet.getLong("id");
                                if (metadataResultMap.containsKey(timestamp, id)) {
                                    metadataResultMap.get(timestamp, id).put(metadataResultSet.getString("m_key"),
                                            metadataResultSet.getString("m_value"));
                                } else {
                                    metadataResultMap.put(timestamp, id, new HashMap<String, String>() {{
                                        put(metadataResultSet.getString("m_key"), metadataResultSet.getString("m_value")); }});
                                }
                            }
                        }
                    }

                    // select valid dimensions associated with valid timestamp and id and store them in dimensionsResultMap
                    try(final PreparedStatement preparedStatementD = connection.prepareStatement(dimensionsSql.toString())) {
                        try (final ResultSet dimensionsResultSet = preparedStatementD.executeQuery()) {
                            while (dimensionsResultSet.next()) {
                                long timestamp = dimensionsResultSet.getLong("timestampMillis");
                                long id = dimensionsResultSet.getLong("id");
                                if (dimensionsResultMap.containsKey(timestamp, id)) {
                                    dimensionsResultMap.get(timestamp, id).put(dimensionsResultSet.getString("d_key"),
                                            dimensionsResultSet.getDouble("d_value"));
                                } else {
                                    dimensionsResultMap.put(timestamp, id, new HashMap<String, Double>() {{
                                        put(dimensionsResultSet.getString("d_key"), dimensionsResultSet.getDouble("d_value")); }});
                                }
                            }
                        }
                    }

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
     * Build query and populate parameterList based on timestamps, namespace and queries for metadata and/or dimensions if applicable.
     * Returned is an array converted from ParameterList.
     */
    private Object[] buildQueryAndParamOnSubqueries(StringBuilder query, List<Object> parameterList, long startTimestampMillis, long endTimestampMillis, String namespace, Map<String, String> metadataQuery, Map<String, String> dimensionsQuery) {
        if ((metadataQuery == null || metadataQuery.isEmpty()) && (dimensionsQuery == null || dimensionsQuery.isEmpty())) {
            parameterList.addAll(Arrays.asList(startTimestampMillis, endTimestampMillis, namespace));
            query.append("where timestampMillis between ? and ? and namespace = ? ");
        } else {
            if (metadataQuery == null || metadataQuery.isEmpty()) {
                addDimQueriesToQueryAndParam(query, parameterList, dimensionsQuery);
                query.append("where d.timestampMillis is null and d.id is null ");
            } else if (dimensionsQuery == null || dimensionsQuery.isEmpty()) {
                addMetaQueriesToQueryAndParam(query, parameterList, metadataQuery);
                query.append("where m.timestampMillis is null and m.id is null ");
            } else {
                addDimQueriesToQueryAndParam(query, parameterList, dimensionsQuery);
                addMetaQueriesToQueryAndParam(query, parameterList, metadataQuery);
                query.append("where d.timestampMillis is null and d.id is null and m.timestampMillis is null and m.id is null ");
            }

            // add conditions in select query to ensure all keys appeared in metadataQuery and dimensionsQuery
            // are presented in all event entries returned
            addConditionsForKeyExisting(query, parameterList, metadataQuery, true);
            addConditionsForKeyExisting(query, parameterList, dimensionsQuery, false);

            query.append("and e.timestampMillis between ? and ? and namespace = ? ");
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
        query.append("left join (select timestampMillis, id from cantor_events_m where (case ");

        for (Map.Entry<String, String> q : queryMap.entrySet()) {
            parameterList.add(q.getKey());
            if (q.getValue().startsWith("~")) {
                subqueries.add("when m_key = ? then (LENGTH(REGEXP_SUBSTR(m_value, ?)) is null or LENGTH(REGEXP_SUBSTR(m_value, ?)) != LENGTH(m_value))");
                if (!q.getValue().contains(".*")) {
                    parameterList.add(q.getValue().substring(1).replace("*", ".*"));
                    parameterList.add(q.getValue().substring(1).replace("*", ".*"));
                } else {
                    parameterList.add(q.getValue().substring(1));
                    parameterList.add(q.getValue().substring(1));
                }
            } else if (q.getValue().startsWith("!~")) {
                subqueries.add("when m_key = ? then LENGTH(REGEXP_SUBSTR(m_value, ?)) = LENGTH(m_value)");
                if (!q.getValue().contains(".*")) {
                    parameterList.add(q.getValue().substring(2).replace("*", ".*"));
                } else {
                    parameterList.add(q.getValue().substring(2));
                }
            } else if (q.getValue().startsWith("!")) { // exact not
                subqueries.add("when m_key = ? then m_value = ?");
                parameterList.add(q.getValue().substring(1));
            } else { // exact match
                subqueries.add("when m_key = ? then m_value != ?");
                if (q.getValue().startsWith("=")) {
                    parameterList.add(q.getValue().substring(1));
                } else {
                    parameterList.add(q.getValue());
                }
            }
        }
        query.append(subqueries.toString()).append(" end)) as m on e.timestampMillis = m.timestampMillis and e.id = m.id ");
    }

    /**
     * Build query and populate parameterList based on dimensionsQuery argument in the main call(get, delete, etc).
     */
    private void addDimQueriesToQueryAndParam(StringBuilder query, List<Object> parameterList, Map<String, String> queryMap) {
        StringJoiner subqueries = new StringJoiner(" ");
        query.append("left join (select timestampMillis, id from cantor_events_d where (case ");

        for (Map.Entry<String, String> q : queryMap.entrySet()) {
            parameterList.add(q.getKey());
            if (q.getValue().startsWith("<=")) {
                subqueries.add("when d_key = ? then d_value > ?");
                parameterList.add(Double.parseDouble(q.getValue().substring(2)));
            } else if (q.getValue().startsWith(">=")) {
                subqueries.add("when d_key = ? then d_value < ?");
                parameterList.add(Double.parseDouble(q.getValue().substring(2)));
            } else if (q.getValue().startsWith("<")) {
                subqueries.add("when d_key = ? then d_value >= ?");
                parameterList.add(Double.parseDouble(q.getValue().substring(1)));
            } else if (q.getValue().startsWith(">")) {
                subqueries.add("when d_key = ? then d_value <= ?");
                parameterList.add(Double.parseDouble(q.getValue().substring(1)));
            } else { // between and
                subqueries.add("when d_key = ? then d_value not between ? and ?");
                parameterList.add(Double.parseDouble(q.getValue().split("\\.\\.")[0]));
                parameterList.add(Double.parseDouble(q.getValue().split("\\.\\.")[1]));
            }
        }
        query.append(subqueries.toString()).append(" end)) as d on e.timestampMillis = d.timestampMillis and e.id = d.id ");
    }

    private void addConditionsForKeyExisting(StringBuilder query, List<Object> parameterList, Map<String, String> queryMap, boolean isMetadataQeury) {
        if (queryMap != null) {
            for (String key : queryMap.keySet()) {
                if (isMetadataQeury){
                    query.append("and exists (select 1 from cantor_events_m as m where m_key = ? and m.timestampMillis = e.timestampMillis and m.id = e.id) ");
                } else {
                    query.append("and exists (select 1 from cantor_events_d as d where d_key = ? and d.timestampMillis = e.timestampMillis and d.id = e.id) ");
                }
                parameterList.add(key);
            }
        }
    }

    @Override
    public int delete(String namespace, long startTimestampMillis, long endTimestampMillis,
                      Map<String, String> metadataQuery, Map<String, String> dimensionsQuery) throws IOException {
        StringBuilder selectQuery = new StringBuilder("select e.timestampMillis, e.id from cantor_events as e ");
        List<Object> parameterList = new ArrayList<>();
        Object[] parameters = buildQueryAndParamOnSubqueries(selectQuery, parameterList, startTimestampMillis, endTimestampMillis, namespace, metadataQuery, dimensionsQuery);
        int deletedRows;

        try (final Connection connection = getConnection()) {
            // first find the timestamp and id of all entries that are to be deleted
            try (final PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString())) {
                addParameters(preparedStatement, parameters);
                try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {
                    StringBuilder deleteMainSql = new StringBuilder("delete from cantor_events where (timestampMillis, id) in (");
                    StringBuilder deleteMetadataSql = new StringBuilder("delete from cantor_events_m where (timestampMillis, id) in (");
                    StringBuilder deleteDimensionsSql = new StringBuilder("delete from cantor_events_d where (timestampMillis, id) in (");
                    StringJoiner timeIdPairString = new StringJoiner(", ");
                    List<Object[]> mainResultMap = new ArrayList<>();

                    boolean isEmpty = true;
                    while (mainResultSet.next()) { // store the timestamp and id of all to-be-deleted entries in mainResultSet
                        isEmpty = false;
                        long timestamp = mainResultSet.getLong("e.timestampMillis");
                        long id = mainResultSet.getLong("e.id");
                        timeIdPairString.add("(" + timestamp + ", " + id + ")");
                        mainResultMap.add(new Object[]{timestamp, id});
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

    @Override
    public Set<String> metadata(String namespace, String metadataKey, long startTimestampMillis,
                                long endTimestampMillis, Map<String, String> metadataQuery,
                                Map<String, String> dimensionsQuery) throws IOException {
        Set<String> results = new HashSet<>();
        StringBuilder query = new StringBuilder("select distinct m_value from cantor_events_m where m_key = ? and (timestampMillis, id) in (");
        List<Object> parameterList = new ArrayList<>();
        Object[] parameters;
        parameterList.add(metadataKey);
        query.append("select e.timestampMillis, e.id from cantor_events as e ");
        parameters = buildQueryAndParamOnSubqueries(query, parameterList, startTimestampMillis, endTimestampMillis, namespace, metadataQuery, dimensionsQuery);
        query.append(")");

        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
                addParameters(preparedStatement, parameters);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        results.add(resultSet.getString("m_value"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return results;
    }

    @Override
    public Collection<String> namespaces() throws IOException {
        Collection<String> result = new HashSet<>();
        String query = "select * from cantor_events_namespace";
        try (final Connection connection = getConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(query.toString())) {
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        result.add(resultSet.getString("namespace"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return result;
    }

    @Override
    public void create(String namespace) throws IOException {
        String query = String.format("upsert into cantor_events_namespace values ('%s')", namespace);
        executeUpdate(query);
    }

    @Override
    public void drop(String namespace) throws IOException {
        Connection connection = null;
        try {
            String query = String.format("delete from cantor_events_namespace where namespace = '%s'", namespace);
            String deleteFromMetadataSql = String.format("delete from cantor_events_m where (timestampMillis, id) in (select timestampMillis, id from cantor_events where namespace = '%s')", namespace);
            String deleteFromDimensionsSql = String.format("delete from cantor_events_d where (timestampMillis, id) in (select timestampMillis, id from cantor_events where namespace = '%s')", namespace);
            String deleteFromMainSql = String.format("delete from cantor_events where namespace = '%s'", namespace);
            connection = openTransaction(getConnection());
            executeUpdate(connection, query);
            executeUpdate(connection, deleteFromMetadataSql);
            executeUpdate(connection, deleteFromDimensionsSql);
            executeUpdate(connection, deleteFromMainSql);
        } finally {
            closeConnection(connection);
        }
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

    @Override
    public Map<Long, Double> aggregate(String namespace, String dimension, long startTimestampMillis,
                                       long endTimestampMillis, Map<String, String> metadataQuery,
                                       Map<String, String> dimensionsQuery, int aggregateIntervalMillis,
                                       AggregationFunction aggregationFunction) throws IOException {
        return Collections.emptyMap();
    }

    @Override
    public void expire(String namespace, long endTimestampMillis) throws IOException {
    }

    @Override
    protected String getCreateChunkLookupTableSql(String namespace) {
        return null;
    }

    @Override
    protected String getCreateChunkTableSql(String chunkTableName, String namespace, Map<String, String> metadata, Map<String, Double> dimensions) {
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
