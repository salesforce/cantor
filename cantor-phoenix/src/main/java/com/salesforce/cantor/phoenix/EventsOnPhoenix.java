package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Events;
import com.salesforce.cantor.jdbc.AbstractBaseEventsOnJdbc;
import static com.salesforce.cantor.jdbc.JdbcUtils.addParameters;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.apache.commons.collections4.map.MultiKeyMap;

public class EventsOnPhoenix extends AbstractBaseEventsOnJdbc implements Events {

    public EventsOnPhoenix(final DataSource dataSource) throws IOException {
        super(dataSource);
        String createMainTableSql = "create table if not exists cantor_events (timestampMillis BIGINT not null, " +
                "id BIGINT not null, namespace VARCHAR, payload VARBINARY CONSTRAINT pk PRIMARY KEY (timestampMillis, id))";
        String createMetadataTableSql = "create table if not exists cantor_events_m (timestampMillis BIGINT, " +
                "id BIGINT, m_key VARCHAR, m_value VARCHAR, iid BIGINT PRIMARY KEY)";
        String createDimensionsTableSql = "create table if not exists cantor_events_d (timestampMillis BIGINT, " +
                "id BIGINT, d_key VARCHAR, d_value DOUBLE, iid BIGINT PRIMARY KEY)";
        String createIDSeqSql = "create sequence if not exists cantor_events_id";
        String createMIDSeqSql = "create sequence if not exists cantor_events_m_id";
        String createDIDSeqSql = "create sequence if not exists cantor_events_d_id";
        executeUpdate(createMainTableSql);
        executeUpdate(createMetadataTableSql);
        executeUpdate(createDimensionsTableSql);
        executeUpdate(createIDSeqSql);
        executeUpdate(createMIDSeqSql);
        executeUpdate(createDIDSeqSql); //TODO: batch create methods?
    }

    @Override
    public void store(String namespace, Collection<Event> batch) throws IOException {
        String upsertMainSql = "upsert into cantor_events values (?, next value for cantor_events_id, ?, ?)";
        String upsertMetadataSql = "upsert into cantor_events_m values (?, current value for cantor_events_id, ?, ?, next value for cantor_events_m_id)";
        String upsertDimensionsSql = "upsert into cantor_events_d values (?, current value for cantor_events_id, ?, ?, next value for cantor_events_d_id)";
        Connection connection = null;
        List<Object[]> metadataParameters;
        List<Object[]> dimensionsParameters;
        //TODO: how to batchUpdate multiple statements w/ parameters
        //TODO: if one event failed, are other events rolled back?
        //TODO: if inserting into metadata/dimensions failed, how to roll back inserting into previous table(s)?
        for (Event e : batch) {
            try {
                if (namespace == null) {
                    throw new IllegalArgumentException("Namespace should not be null");
                } else if (e.getTimestampMillis() < 0) {
                    throw new IllegalArgumentException("Invalid timestamp: " + e.getTimestampMillis());
                } else if (e.getTimestampMillis() == 0) { //TODO: why are < 0 and == 0 different cases?
                    throw new IOException("Invalid timestamp: timestamp is zero");
                } if (e.getMetadata().size() > 100) {
                    throw new IllegalArgumentException(String.format("Metadata size is %s, larger than 100", e.getMetadata().size()));
                } else if (e.getDimensions().size() > 400) {
                    throw new IllegalArgumentException(String.format("Dimensions size is %s, larger than 400", e.getDimensions().size()));
                }

                connection = openTransaction(getConnection());
                executeUpdate(connection, upsertMainSql, e.getTimestampMillis(), namespace, (e.getPayload() == null) ? new byte[0] : e.getPayload());

                metadataParameters = new ArrayList<>();
                for (Map.Entry<String, String> m : e.getMetadata().entrySet()) {
                    metadataParameters.add(new Object[]{e.getTimestampMillis(), m.getKey(), m.getValue()});
                }
                executeBatchUpdate(connection, upsertMetadataSql, metadataParameters);

                dimensionsParameters = new ArrayList<>();
                for (Map.Entry<String, Double> d : e.getDimensions().entrySet()) {
                    dimensionsParameters.add(new Object[]{e.getTimestampMillis(), d.getKey(), d.getValue()});
                }
                executeBatchUpdate(connection, upsertDimensionsSql, dimensionsParameters);
            } finally {
                closeConnection(connection);
            }
        }
    }

    @Override
    public List<Event> get(String namespace, long startTimestampMillis, long endTimestampMillis,
                           Map<String, String> metadataQuery, Map<String, String> dimensionsQuery,
                           boolean includePayloads, boolean ascending, int limit) throws IOException {
        List<Event> events = new ArrayList<>();
        StringBuilder query = new StringBuilder();
        List<Object> parameterList = new ArrayList<>();
        Object[] parameters;

        if (includePayloads) { //TODO: not include namespace?
            query.append("select e.timestampMillis, e.id, e.payload from cantor_events as e ");
        } else {
            query.append("select e.timestampMillis, e.id from cantor_events as e ");
        }

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
                addParameters(preparedStatement, parameters); //TODO: revert it back from "public"
                try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {

                    StringBuilder metadataSql = new StringBuilder("select timestampMillis, id, m_key, m_value from cantor_events_m where (timestampMillis, id) in (");
                    StringBuilder dimensionsSql = new StringBuilder("select timestampMillis, id, d_key, d_value from cantor_events_d where (timestampMillis, id) in (");
                    StringJoiner timeIdPairString = new StringJoiner(", ");
                    List<Object[]> mainResultMap = new ArrayList<>();

                    boolean isEmpty = true;
                    while (mainResultSet.next()) {
                        isEmpty = false;
                        long timestamp = mainResultSet.getLong("e.timestampMillis");
                        long id = mainResultSet.getLong("e.id");
                        timeIdPairString.add("(" + timestamp + ", " + id + ")");
                        mainResultMap.add(new Object[]{timestamp, id, (includePayloads) ? mainResultSet.getBytes("payload") : null});
                    }

                    if (isEmpty) {
                        return events;
                    }

                    metadataSql.append(timeIdPairString.toString()).append(")");
                    dimensionsSql.append(timeIdPairString.toString()).append(")");
                    MultiKeyMap<Long, Map<String, String>> metadataResultMap = new MultiKeyMap();
                    MultiKeyMap<Long, Map<String, Double>> dimensionsResultMap = new MultiKeyMap();

                    try(final PreparedStatement preparedStatement1 = connection.prepareStatement(metadataSql.toString())) {
                        try (final ResultSet metadataResultSet = preparedStatement1.executeQuery()) {
                            while (metadataResultSet.next()) {
                                long timestamp = metadataResultSet.getLong("timestampMillis");
                                long id = metadataResultSet.getLong("id");
                                if (metadataResultMap.containsKey(timestamp, id)) {
                                    metadataResultMap.get(timestamp, id).put(metadataResultSet.getString("m_key"), metadataResultSet.getString("m_value"));
                                } else {
                                    metadataResultMap.put(timestamp, id, new HashMap<String, String>() {{
                                        put(metadataResultSet.getString("m_key"), metadataResultSet.getString("m_value")); }});
                                }
                            }
                        }
                    }

                    try(final PreparedStatement preparedStatement2 = connection.prepareStatement(dimensionsSql.toString())) {
                        try (final ResultSet dimensionsResultSet = preparedStatement2.executeQuery()) {
                            while (dimensionsResultSet.next()) {
                                long timestamp = dimensionsResultSet.getLong("timestampMillis");
                                long id = dimensionsResultSet.getLong("id");
                                if (dimensionsResultMap.containsKey(timestamp, id)) {
                                    dimensionsResultMap.get(timestamp, id).put(dimensionsResultSet.getString("d_key"), dimensionsResultSet.getDouble("d_value"));
                                } else {
                                    dimensionsResultMap.put(timestamp, id, new HashMap<String, Double>() {{
                                        put(dimensionsResultSet.getString("d_key"), dimensionsResultSet.getDouble("d_value")); }});
                                }
                            }
                        }
                    }

                    for (Object[] entry : mainResultMap) {
                        Event e = new Event((long)entry[0], metadataResultMap.get(entry[0], entry[1]), dimensionsResultMap.get(entry[0], entry[1]), (includePayloads) ? (byte[])entry[2] : null);
                        events.add(e);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return events;
    }

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

            addConditionsForKeyExisting(query, parameterList, metadataQuery, true);
            addConditionsForKeyExisting(query, parameterList, dimensionsQuery, false);

            query.append("and e.timestampMillis between ? and ? and namespace = ? ");
            parameterList.add(startTimestampMillis);
            parameterList.add(endTimestampMillis);
            parameterList.add(namespace);
            ;
        }
        return parameterList.toArray(new Object[parameterList.size()]);
    }

    private void addMetaQueriesToQueryAndParam(StringBuilder query, List<Object> parameterList, Map<String, String> queryMap) {
        StringJoiner subqueries = new StringJoiner(" ");
        query.append("left join (select timestampMillis, id from cantor_events_m where (case ");

        for (Map.Entry<String, String> q : queryMap.entrySet()) {
            parameterList.add(q.getKey());
            if (q.getValue().startsWith("~")) {
                subqueries.add("when m_key = ? then not REGEXP_SUBSTR(m_value, ?) = m_value");
                if (!q.getValue().contains(".*")) {
                    parameterList.add(q.getValue().substring(1).replace("*", ".*"));
                } else {
                    parameterList.add(q.getValue().substring(1));
                }
            } else if (q.getValue().startsWith("!~")) {
                subqueries.add("when m_key = ? then REGEXP_SUBSTR(m_value, ?) = m_value");
                if (!q.getValue().contains(".*")) {
                    parameterList.add(q.getValue().substring(2).replace("*", ".*"));
                } else {
                    parameterList.add(q.getValue().substring(2));
                }
            } else if (q.getValue().startsWith("!")) { // exact not
                subqueries.add("when m_key = ? then REGEXP_SUBSTR(m_value, ?) = m_value");
                parameterList.add(q.getValue().substring(1));
            } else { // exact match
                subqueries.add("when m_key = ? then not REGEXP_SUBSTR(m_value, ?) = m_value");
                parameterList.add(q.getValue());
            }
        }
        query.append(subqueries.toString()).append(" end)) as m on e.timestampMillis = m.timestampMillis and e.id = m.id ");
    }

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
            try (final PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString())) {
                addParameters(preparedStatement, parameters); //TODO: revert it back from "public"
                try (final ResultSet mainResultSet = preparedStatement.executeQuery()) {
                    StringBuilder deleteMainSql = new StringBuilder("delete from cantor_events where (timestampMillis, id) in (");
                    StringBuilder deleteMetadataSql = new StringBuilder("delete from cantor_events_m where (timestampMillis, id) in (");
                    StringBuilder deleteDimensionsSql = new StringBuilder("delete from cantor_events_d where (timestampMillis, id) in (");
                    StringJoiner timeIdPairString = new StringJoiner(", ");
                    List<Object[]> mainResultMap = new ArrayList<>();

                    boolean isEmpty = true;
                    while (mainResultSet.next()) {
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
    public Map<Long, Double> aggregate(String namespace, String dimension, long startTimestampMillis,
                                       long endTimestampMillis, Map<String, String> metadataQuery,
                                       Map<String, String> dimensionsQuery, int aggregateIntervalMillis,
                                       AggregationFunction aggregationFunction) throws IOException {
        return null;
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
                addParameters(preparedStatement, parameters); //TODO: revert it back from "public"
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

    @Override
    public Collection<String> namespaces() throws IOException {
        return null;
    }

    @Override
    public void create(String namespace) throws IOException {
    }

    @Override
    public void drop(String namespace) throws IOException {
    }
}
