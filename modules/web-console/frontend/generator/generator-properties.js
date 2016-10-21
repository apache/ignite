/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Properties generation entry point.
const $generatorProperties = {};

$generatorProperties.jdbcUrlTemplate = function(dialect) {
    switch (dialect) {
        case 'Oracle':
            return 'jdbc:oracle:thin:@[host]:[port]:[database]';
        case 'DB2':
            return 'jdbc:db2://[host]:[port]/[database]';
        case 'SQLServer':
            return 'jdbc:sqlserver://[host]:[port][;databaseName=database]';
        case 'MySQL':
            return 'jdbc:mysql://[host]:[port]/[database]';
        case 'PostgreSQL':
            return 'jdbc:postgresql://[host]:[port]/[database]';
        case 'H2':
            return 'jdbc:h2:tcp://[host]/[database]';
        default:
    }

    return 'jdbc:your_database';
};

$generatorProperties.createBuilder = function() {
    const res = $generatorCommon.builder();

    res.line('# ' + $generatorCommon.mainComment('list of properties'));

    return res;
};

/**
 * Generate properties file with properties stubs for stores data sources.
 *
 * @param res Resulting output with generated properties.
 * @param datasources Already added datasources.
 * @param storeFactory Current datasource factory.
 * @param dialect Current dialect.
 * @returns {string} Generated content.
 */
$generatorProperties.dataSourceProperties = function(res, datasources, storeFactory, dialect) {
    const beanId = storeFactory.dataSourceBean;

    const dsClsName = $generatorCommon.dataSourceClassName(dialect);

    const varType = res.importClass(dsClsName);

    const beanClassName = $generatorCommon.toJavaName(varType, storeFactory.dataSourceBean);

    if (!_.includes(datasources, beanClassName)) {
        datasources.push(beanClassName);

        res.needEmptyLine = true;

        switch (dialect) {
            case 'DB2':
                res.line(beanId + '.jdbc.server_name=YOUR_DATABASE_SERVER_NAME');
                res.line(beanId + '.jdbc.port_number=YOUR_JDBC_PORT_NUMBER');
                res.line(beanId + '.jdbc.driver_type=YOUR_JDBC_DRIVER_TYPE');
                res.line(beanId + '.jdbc.database_name=YOUR_DATABASE_NAME');

                break;

            default:
                res.line(beanId + '.jdbc.url=' + $generatorProperties.jdbcUrlTemplate(dialect));
        }

        res.line(beanId + '.jdbc.username=YOUR_USER_NAME');
        res.line(beanId + '.jdbc.password=YOUR_PASSWORD');
        res.line('');
    }
};

/**
 * Generate properties file with properties stubs for stores data sources.
 *
 * @param cluster Configuration to process.
 * @param res Resulting output with generated properties.
 * @returns {string} Generated content.
 */
$generatorProperties.dataSourcesProperties = function(cluster, res) {
    const datasources = [];

    if (cluster.caches && cluster.caches.length > 0) {
        _.forEach(cluster.caches, function(cache) {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
                const storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

                const dialect = storeFactory.connectVia ? (storeFactory.connectVia === 'DataSource' ? storeFactory.dialect : null) : storeFactory.dialect;

                const connectViaUrl = cache.cacheStoreFactory.kind === 'CacheJdbcBlobStoreFactory' && storeFactory.connectVia === 'URL';

                if (!res && (dialect || connectViaUrl))
                    res = $generatorProperties.createBuilder();

                if (dialect)
                    $generatorProperties.dataSourceProperties(res, datasources, storeFactory, dialect);

                if (connectViaUrl)
                    res.line('ds.' + storeFactory.user + '.password=YOUR_PASSWORD');
            }
        });
    }

    if (cluster.discovery.kind === 'Jdbc') {
        const ds = cluster.discovery.Jdbc;

        if (ds.dataSourceBean && ds.dialect) {
            if (!res)
                res = $generatorProperties.createBuilder();

            $generatorProperties.dataSourceProperties(res, datasources, ds, ds.dialect);
        }
    }

    return res;
};

/**
 * Generate properties file with properties stubs for cluster SSL configuration.
 *
 * @param cluster Cluster to get SSL configuration.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object
 */
$generatorProperties.sslProperties = function(cluster, res) {
    if (cluster.sslEnabled && cluster.sslContextFactory) {
        if (!res)
            res = $generatorProperties.createBuilder();

        res.needEmptyLine = true;

        if (_.isEmpty(cluster.sslContextFactory.keyStoreFilePath))
            res.line('ssl.key.storage.password=YOUR_SSL_KEY_STORAGE_PASSWORD');

        if (_.isEmpty(cluster.sslContextFactory.trustStoreFilePath))
            res.line('ssl.trust.storage.password=YOUR_SSL_TRUST_STORAGE_PASSWORD');
    }

    return res;
};

/**
 * Generate properties file with all possible properties.
 *
 * @param cluster Cluster to get configurations.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object
 */
$generatorProperties.generateProperties = function(cluster, res) {
    res = $generatorProperties.dataSourcesProperties(cluster, res);

    res = $generatorProperties.sslProperties(cluster, res);

    return res;
};

export default $generatorProperties;
