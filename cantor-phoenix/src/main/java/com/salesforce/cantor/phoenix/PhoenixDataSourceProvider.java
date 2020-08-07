package com.salesforce.cantor.phoenix;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class PhoenixDataSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixDataSourceProvider.class);

    public static synchronized DataSource getDatasource(final PhoenixDataSourceProperties builder) {
        return doGetDataSource(builder);
    }

    private static DataSource doGetDataSource(final PhoenixDataSourceProperties builder) {
        final String jdbcUrl = "jdbc:phoenix:" + builder.getHostname() + ":" + builder.getPath();;
        try {
            // force loading Phoenix drivers
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        final HikariDataSource connectionPoolDataSource = new HikariDataSource();

        connectionPoolDataSource.setJdbcUrl(jdbcUrl);

        logger.info("jdbc url for datasource is: {} data source properties are: {}",
                jdbcUrl, connectionPoolDataSource.getDataSourceProperties()
        );
        return connectionPoolDataSource;
    }
}
