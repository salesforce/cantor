package com.salesforce.cantor.phoenix;

import org.apache.hadoop.hbase.HConstants;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class PhoenixDataSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixDataSourceProvider.class);

    public static synchronized DataSource getDatasource(final PhoenixDataSourceProperties builder) {
        return doGetDataSource();
    }

    private static DataSource doGetDataSource() {
        final String jdbcUrl = "jdbc:phoenix:localhost";
        try {
            // force loading Phoenix driver
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        final HikariDataSource connectionPoolDataSource = new HikariDataSource();

        connectionPoolDataSource.setJdbcUrl(jdbcUrl);

        connectionPoolDataSource.setConnectionTimeout(30 * 1000);  // 30 seconds connection timeout
        connectionPoolDataSource.addDataSourceProperty("useUnicode", true);
        connectionPoolDataSource.addDataSourceProperty("useSSL", false);
        connectionPoolDataSource.addDataSourceProperty("allowPublicKeyRetrieval", true);
        connectionPoolDataSource.addDataSourceProperty("serverTimezone", "UTC");
        connectionPoolDataSource.addDataSourceProperty("useJDBCCompliantTimezoneShift", true);
        connectionPoolDataSource.addDataSourceProperty("useLegacyDatetimeCode", false);
        connectionPoolDataSource.addDataSourceProperty("cachePrepStmts", true);
        connectionPoolDataSource.addDataSourceProperty("prepStmtCacheSize", 512);
        connectionPoolDataSource.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
        connectionPoolDataSource.addDataSourceProperty("useServerPrepStmts", true);
        connectionPoolDataSource.addDataSourceProperty("useLocalSessionState", true);
        connectionPoolDataSource.addDataSourceProperty("rewriteBatchedStatements", true);
        connectionPoolDataSource.addDataSourceProperty("cacheResultSetMetadata", true);
        connectionPoolDataSource.addDataSourceProperty("metadataCacheSize", 512);
        connectionPoolDataSource.addDataSourceProperty("cacheServerConfiguration", true);
        connectionPoolDataSource.addDataSourceProperty("elideSetAutoCommits", true);
        connectionPoolDataSource.addDataSourceProperty("continueBatchOnError", false);
        connectionPoolDataSource.addDataSourceProperty("maintainTimeStats", false);
        connectionPoolDataSource.addDataSourceProperty("maxRows", 100_000);  // max of 100,000 rows to be returned
//        TODO allow users to override this by a flag
//        connectionPoolDataSource.addDataSourceProperty("logger", "com.mysql.cj.log.Slf4JLogger");
//        connectionPoolDataSource.addDataSourceProperty("profileSQL", true);

        connectionPoolDataSource.setMaximumPoolSize(64);

        logger.info("jdbc url for datasource is: {} data source properties are: {}",
                jdbcUrl, connectionPoolDataSource.getDataSourceProperties()
        );
        return connectionPoolDataSource;
    }
}
