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

// For server side we should load required libraries.
if (typeof window === 'undefined') {
    _ = require('lodash');

    $generatorCommon = require('./generator-common');
}

// Properties generation entry point.
$generatorProperties = {};

/**
 * Generate properties file with properties stubs for stores data sources.
 *
 * @param cluster Configuration to process.
 * @returns {string} Generated content.
 */
$generatorProperties.dataSourcesProperties = function (cluster, res) {
    var datasources = [];

    if (cluster.caches && cluster.caches.length > 0) {
        _.forEach(cluster.caches, function (cache) {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
                var storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

                if (storeFactory.dialect) {
                    var beanId = storeFactory.dataSourceBean;

                    if (!_.contains(datasources, beanId)) {
                        datasources.push(beanId);

                        if (!res) {
                            res = $generatorCommon.builder();

                            res.line('# ' + $generatorCommon.mainComment());
                        }

                        res.needEmptyLine = true;

                        switch (storeFactory.dialect) {
                            case 'DB2':
                                res.line(beanId + '.jdbc.server_name=YOUR_JDBC_SERVER_NAME');
                                res.line(beanId + '.jdbc.port_number=YOUR_JDBC_PORT_NUMBER');
                                res.line(beanId + '.jdbc.database_name=YOUR_JDBC_DATABASE_TYPE');
                                res.line(beanId + '.jdbc.driver_type=YOUR_JDBC_DRIVER_TYPE');
                                break;

                            default:
                                res.line(beanId + '.jdbc.url=YOUR_JDBC_URL');
                        }

                        res.line(beanId + '.jdbc.username=YOUR_USER_NAME');
                        res.line(beanId + '.jdbc.password=YOUR_PASSWORD');
                        res.line();
                    }
                }
            }
        });
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
$generatorProperties.sslProperties = function (cluster, res) {
    if (cluster.sslEnabled && cluster.sslContextFactory) {
        if (!res) {
            res = $generatorCommon.builder();

            res.line('# ' + $generatorCommon.mainComment());
        }

        res.needEmptyLine = true;

        if ($commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.keyStoreFilePath))
            res.line('ssl.key.storage.password=YOUR_SSL_KEY_STORAGE_PASSWORD');

        if ($commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.trustStoreFilePath))
            res.line('ssl.trust.storage.password=YOUR_SSL_TRUST_STORAGE_PASSWORD');
    }

    return res;
}

// For server side we should export properties generation entry point.
if (typeof window === 'undefined') {
    module.exports = $generatorProperties;
}
