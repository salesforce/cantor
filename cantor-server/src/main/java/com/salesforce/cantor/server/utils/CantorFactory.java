/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.server.utils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Sets;
import com.salesforce.cantor.h2.CantorOnH2;
import com.salesforce.cantor.h2.H2DataSourceProperties;
import com.salesforce.cantor.h2.H2DataSourceProvider;
import com.salesforce.cantor.misc.async.AsyncCantor;
import com.salesforce.cantor.misc.loggable.LoggableCantor;
import com.salesforce.cantor.misc.rw.ReadWriteCantor;
import com.salesforce.cantor.misc.sharded.ShardedCantor;
import com.salesforce.cantor.mysql.CantorOnMysql;
import com.salesforce.cantor.mysql.MysqlDataSourceProperties;
import com.salesforce.cantor.mysql.MysqlDataSourceProvider;
import com.salesforce.cantor.s3.CantorOnS3;
import com.salesforce.cantor.server.CantorEnvironment;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.salesforce.cantor.server.Constants.CANTOR_H2_COMPRESSED;
import static com.salesforce.cantor.server.Constants.CANTOR_H2_IN_MEMORY;
import static com.salesforce.cantor.server.Constants.CANTOR_H2_PASSWORD;
import static com.salesforce.cantor.server.Constants.CANTOR_H2_PATH;
import static com.salesforce.cantor.server.Constants.CANTOR_H2_USERNAME;
import static com.salesforce.cantor.server.Constants.CANTOR_MYSQL_HOSTNAME;
import static com.salesforce.cantor.server.Constants.CANTOR_MYSQL_PASSWORD;
import static com.salesforce.cantor.server.Constants.CANTOR_MYSQL_PORT;
import static com.salesforce.cantor.server.Constants.CANTOR_MYSQL_USERNAME;
import static com.salesforce.cantor.server.Constants.CANTOR_S3_BUCKET_NAME;
import static com.salesforce.cantor.server.Constants.CANTOR_S3_BUCKET_REGION;
import static com.salesforce.cantor.server.Constants.CANTOR_S3_ENDPOINT_OVERRIDE;
import static com.salesforce.cantor.server.Constants.CANTOR_S3_PROXY_HOST;
import static com.salesforce.cantor.server.Constants.CANTOR_S3_PROXY_PORT;
import static com.salesforce.cantor.server.Constants.CANTOR_S3_SETS_TYPE;

public class CantorFactory {
    private static final Logger logger = LoggerFactory.getLogger(CantorFactory.class);
    // this variable expects the format <hostname1>:<port1>,<hostname2>:<port2>,...
    private static final String ENV_MYSQL_SHARDS = "MYSQL_SHARDS";

    private final CantorEnvironment cantorEnvironment;

    public CantorFactory(final CantorEnvironment cantorEnvironment) {
        this.cantorEnvironment = cantorEnvironment;
    }

    public Cantor getCantor() throws IOException {
        final String storageType = this.cantorEnvironment.getStorageType();
        if (storageType.equalsIgnoreCase("s3")) {
            final Config config = this.cantorEnvironment.getConfig(storageType);
            if (!config.hasPath(CANTOR_S3_SETS_TYPE)) {
                throw new IllegalArgumentException("Missing configuration setting for 's3." + CANTOR_S3_SETS_TYPE + "'");
            }

            logger.info("creating s3 cantor instance with sets on {}...", config.getString(CANTOR_S3_SETS_TYPE));
            final String bucketName = config.getString(CANTOR_S3_BUCKET_NAME);
            if (Strings.isNullOrEmpty(bucketName)) {
                throw new IllegalArgumentException("Bucket name invalid. Please set 's3." + CANTOR_S3_BUCKET_NAME + "'");
            }

            // no support for s3 on sets therefore another cantor type must be used
            final Sets sets = getCantorByType(config.getString(CANTOR_S3_SETS_TYPE)).sets();
            return new LoggableCantor(new CantorOnS3(createAwsClient(config), bucketName) {
                @Override
                public Sets sets() {
                    return sets;
                }
            });
        }
        return getCantorByType(storageType);
    }

    private Cantor getCantorByType(final String storageType) throws IOException {
        if (storageType.equalsIgnoreCase("mysql")) {
            final String mysqlShards = this.cantorEnvironment.getEnvironmentVariable(ENV_MYSQL_SHARDS);

            final List<MysqlDataSourceProperties> dataSources;
            if (mysqlShards != null) {
                dataSources = getMysqlFromEnv(mysqlShards);
            } else {
                dataSources = getMysqlDataSourceProperties(this.cantorEnvironment.getConfigAsList(storageType));
            }

            if (dataSources.size() == 1) {
                logger.info("creating single instance mysql cantor...");
                final Cantor readCantor = new CantorOnMysql(MysqlDataSourceProvider.getDatasource(dataSources.get(0)));
                final Cantor writeCantor = new CantorOnMysql(MysqlDataSourceProvider.getDatasource(dataSources.get(0)));
                return new LoggableCantor(new AsyncCantor(new ReadWriteCantor(writeCantor, readCantor), newExecutorService()));
            }

            final Cantor[] cantors = new Cantor[dataSources.size()];
            for (int index = 0; index < cantors.length; index++) {
                final Cantor readCantor = new CantorOnMysql(MysqlDataSourceProvider.getDatasource(dataSources.get(index)));
                final Cantor writeCantor = new CantorOnMysql(MysqlDataSourceProvider.getDatasource(dataSources.get(index)));
                cantors[index] = new AsyncCantor(new ReadWriteCantor(writeCantor, readCantor), newExecutorService());
            }
            logger.info("creating shared mysql cantor with {} instances: {}", cantors.length, dataSources);
            return new LoggableCantor(new ShardedCantor(cantors));
        } else if (storageType.equalsIgnoreCase("h2")) {
            final List<H2DataSourceProperties> dataSources = getH2DataSourceProperties(
                    this.cantorEnvironment.getConfigAsList(storageType)
            );

            if (dataSources.size() == 1) {
                logger.info("creating single instance h2 cantor...");
                return new LoggableCantor(new CantorOnH2(H2DataSourceProvider.getDatasource(dataSources.get(0))));
            }

            final Cantor[] cantors = new Cantor[dataSources.size()];
            for (int index = 0; index < cantors.length; index++) {
                cantors[index] = new CantorOnH2(H2DataSourceProvider.getDatasource(dataSources.get(index)));
            }
            logger.info("creating shared h2 cantor with {} instances: {}", cantors.length, dataSources);
            return new LoggableCantor(new ShardedCantor(cantors));
        } else {
            throw new IllegalArgumentException("invalid storage type");
        }
    }

    private List<MysqlDataSourceProperties> getMysqlFromEnv(final String mysqlShardsString) {
        final List<MysqlDataSourceProperties> propertiesList = new ArrayList<>();

        final String[] mysqlShards = mysqlShardsString.split(",");
        for (final String mysqlShard : mysqlShards) {
            final String[] hostPort = mysqlShard.split(":");
            if (hostPort.length != 2) {
                throw new IllegalArgumentException(ENV_MYSQL_SHARDS + " is in an invalid format. Expected: <hostname1>:<port1>,<hostname2>:<port2>,... Found: " + mysqlShardsString);
            }

            final MysqlDataSourceProperties properties = new MysqlDataSourceProperties();
            properties.setHostname(hostPort[0]);
            properties.setPort(Integer.parseInt(hostPort[1]));
            propertiesList.add(properties);
        }
        return propertiesList;
    }

    private List<MysqlDataSourceProperties> getMysqlDataSourceProperties(final List<? extends Config> configObjects) {
        final List<MysqlDataSourceProperties> propertiesList = new ArrayList<>();
        for (final Config config : configObjects) {
            final MysqlDataSourceProperties properties = new MysqlDataSourceProperties();
            properties.setHostname(config.getString(CANTOR_MYSQL_HOSTNAME));
            properties.setPort(config.getInt(CANTOR_MYSQL_PORT));
            properties.setUsername(config.getString(CANTOR_MYSQL_USERNAME));
            properties.setPassword(config.getString(CANTOR_MYSQL_PASSWORD));
            propertiesList.add(properties);
        }
        return propertiesList;
    }

    private List<H2DataSourceProperties> getH2DataSourceProperties(final List<? extends Config> configObjects) {
        final List<H2DataSourceProperties> propertiesList = new ArrayList<>();
        for (final Config config : configObjects) {
            final H2DataSourceProperties properties = new H2DataSourceProperties();
            properties.setPath(config.getString(CANTOR_H2_PATH));
            properties.setInMemory(config.getBoolean(CANTOR_H2_IN_MEMORY));
            properties.setCompressed(config.getBoolean(CANTOR_H2_COMPRESSED));
            properties.setUsername(config.getString(CANTOR_H2_USERNAME));
            properties.setPassword(config.getString(CANTOR_H2_PASSWORD));
            propertiesList.add(properties);
        }
        return propertiesList;
    }

    private AmazonS3 createAwsClient(final Config config) {
        final String region = config.getString(CANTOR_S3_BUCKET_REGION);
        final AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard();

        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.withProtocol(Protocol.HTTPS)
            .withMaxConnections(256)
            .withConnectionMaxIdleMillis(TimeUnit.MINUTES.toMillis(5)) // keep connections alive for 5 minutes
            .withConnectionTimeout(3_000) // timeout on connect after 3 seconds
            .withRequestTimeout(10_000) // timeout out the request after 10 seconds
            .withMaxErrorRetry(3); // on errors, retry max of 3 times

        final boolean endpointOverride = config.getBoolean(CANTOR_S3_ENDPOINT_OVERRIDE);
        if (endpointOverride) {
            amazonS3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(String.format("%s-fips", region), region));
        } else {
            amazonS3ClientBuilder.withRegion(region);
        }

        final String proxyHost = config.getString(CANTOR_S3_PROXY_HOST);
        final int proxyPort = config.getInt(CANTOR_S3_PROXY_PORT);
        if (!Strings.isNullOrEmpty(proxyHost)) {
            clientConfiguration.setProxyHost(proxyHost);
            clientConfiguration.setProxyPort(proxyPort);
        }

        return amazonS3ClientBuilder.withClientConfiguration(clientConfiguration).build();
    }

    private ExecutorService newExecutorService() {
        return Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("cantor-worker-%d").build()
        );
    }
}
