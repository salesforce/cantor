package com.salesforce.cantor.phoenix;

import com.salesforce.cantor.Events;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class EventsOnPhoenix implements Events {
    private final String url;
    private static long ID = 0;

    public EventsOnPhoenix(final String url) {
        this.url = url;
    }

    @Override
    public void store(String namespace, Collection<Event> batch) throws IOException {
        Statement stmt = null;
        try {
            Connection con = DriverManager.getConnection(url);
            stmt = con.createStatement();
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS PROFILER_TABLE (id UNSIGNED_LONG PRIMARY KEY, timestampMillis UNSIGNED_LONG, namespace VARCHAR, dimensions VARCHAR, metadata VARCHAR, payload VARBINARY)");
            for (Event e : batch) {
                StringBuilder upsertQuery = new StringBuilder();
                upsertQuery.append("UPSERT INTO PROFILER_TABLE VALUES (")
                        .append(ID++).append(", ")
                        .append(e.getTimestampMillis()).append(", ")
                        .append("'").append(namespace).append("', ")
                        .append("'").append(serializeDimensions(e.getDimensions())).append("', ")
                        .append("'").append(serializeMetadata(e.getMetadata())).append("', ")
                        .append("CAST('").append(new String(Base64.getEncoder().encode(e.getPayload()))).append("' AS VARBINARY))");
                stmt.executeUpdate(upsertQuery.toString());
            }
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String serializeDimensions(Map<String, Double> dimensions) {
        if (dimensions.isEmpty()) {
            return "";
        }
        StringBuilder header = new StringBuilder();
        StringBuilder value = new StringBuilder();
        for (Map.Entry<String, Double> e: dimensions.entrySet()) {
            header.append(e.getKey()).append(";;");
            value.append(e.getValue()).append(";;");
        }
        return header.append("\\n").append(value.toString()).toString();
    }

    private String serializeMetadata(Map<String, String> metadata) {
        if (metadata.isEmpty()) {
            return "";
        }
        StringBuilder header = new StringBuilder();
        StringBuilder value = new StringBuilder();
        for (Map.Entry<String, String> e: metadata.entrySet()) {
            header.append(e.getKey()).append(";;");
            value.append("\"").append(e.getValue()).append("\"").append(";;");
        }
        return header.append("\\n").append(value.toString()).toString();
    }

    @Override
    public List<Event> get(String namespace, long startTimestampMillis, long endTimestampMillis, Map<String, String> metadataQuery, Map<String, String> dimensionsQuery, boolean includePayloads, boolean ascending, int limit) throws IOException {
        List<Event> events = new ArrayList<>();
        ResultSet rset = null;
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM PROFILER_TABLE WHERE timestampMillis >= ").append(startTimestampMillis)
             .append(" AND timestampMillis < ").append(endTimestampMillis)
             .append(" AND namespace = '").append(namespace).append("'"); //TODO: queries, order, payload, limit
        if (ascending) {
            query.append(" ORDER BY timestampMillis ASC");
        } else {
            query.append(" ORDER BY timestampMillis DESC");
        }
        if (limit > 0) {
            query.append(" LIMIT ").append(limit);
        }
        try {
            Connection con = DriverManager.getConnection(url);
            PreparedStatement statement = con.prepareStatement(query.toString());
            rset = statement.executeQuery();
            while (rset.next()) {
                Event e = new Event(rset.getLong("timestampMillis"),
                        deserializeMetadata(rset.getString("metadata")),
                        deserializeDimensions(rset.getString("dimensions")),
                        Base64.getDecoder().decode(rset.getBytes("payload")));
                events.add(e);
            }
            statement.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return events;
    }

    private Map<String, String> deserializeMetadata(String metadataString) {
        Map<String, String> metadata = new HashMap<>();
        String[] lines = metadataString.split("\\n");
        if (lines.length == 2) {
            String[] header = lines[0].split(";;");
            String[] values = lines[1].split(";;");
            for (int i = 0; i < header.length; i ++) { // last header/value is empty
                metadata.put(header[i], values[i].substring(1, values[i].length() - 1));
            }
        }
        return metadata;
    }

    private Map<String, Double> deserializeDimensions(String dimensionsString) {
        Map<String, Double> dimensions = new HashMap<>();
        String[] lines = dimensionsString.split("\\n");
        if (lines.length == 2) {
            String[] header = lines[0].split(";;");
            String[] values = lines[1].split(";;");
            for (int i = 0; i < header.length; i ++) { // last header/value is empty
                dimensions.put(header[i], Double.parseDouble(values[i]));
            }
        }
        return dimensions;
    }

    @Override
    public int delete(String namespace, long startTimestampMillis, long endTimestampMillis, Map<String, String> metadataQuery, Map<String, String> dimensionsQuery) throws IOException {
        return 0;
    }

    @Override
    public Map<Long, Double> aggregate(String namespace, String dimension, long startTimestampMillis, long endTimestampMillis, Map<String, String> metadataQuery, Map<String, String> dimensionsQuery, int aggregateIntervalMillis, AggregationFunction aggregationFunction) throws IOException {
        return null;
    }

    @Override
    public Set<String> metadata(String namespace, String metadataKey, long startTimestampMillis, long endTimestampMillis, Map<String, String> metadataQuery, Map<String, String> dimensionsQuery) throws IOException {
        return null;
    }

    @Override
    public void expire(String namespace, long endTimestampMillis) throws IOException {

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
