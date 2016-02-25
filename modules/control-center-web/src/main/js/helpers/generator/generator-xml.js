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

// XML generation entry point.
$generatorXml = {};

// Do XML escape.
$generatorXml.escape = function (s) {
    if (typeof(s) !== 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
};

// Add XML element.
$generatorXml.element = function (res, tag, attr1, val1, attr2, val2) {
    var elem = '<' + tag;

    if (attr1)
        elem += ' ' + attr1 + '="' + val1 + '"';

    if (attr2)
        elem += ' ' + attr2 + '="' + val2 + '"';

    elem += '/>';

    res.emptyLineIfNeeded();
    res.line(elem);
};

// Add property.
$generatorXml.property = function (res, obj, propName, setterName, dflt) {
    if ($commonUtils.isDefined(obj)) {
        var val = obj[propName];

        if ($commonUtils.isDefinedAndNotEmpty(val)) {
            var hasDflt = $commonUtils.isDefined(dflt);

            // Add to result if no default provided or value not equals to default.
            if (!hasDflt || (hasDflt && val !== dflt)) {
                $generatorXml.element(res, 'property', 'name', setterName ? setterName : propName, 'value', $generatorXml.escape(val));

                return true;
            }
        }
    }

    return false;
};

// Add property for class name.
$generatorXml.classNameProperty = function (res, obj, propName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val))
        $generatorXml.element(res, 'property', 'name', propName, 'value', $dataStructures.fullClassName(val));
};

// Add list property.
$generatorXml.listProperty = function (res, obj, propName, listType, rowFactory) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        if (!listType)
            listType = 'list';

        if (!rowFactory)
            rowFactory = function (val) {
                return '<value>' + $generatorXml.escape(val) + '</value>';
            };

        res.startBlock('<property name="' + propName + '">');
        res.startBlock('<' + listType + '>');

        _.forEach(val, function(v) {
            res.line(rowFactory(v));
        });

        res.endBlock('</' + listType + '>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Add array property
$generatorXml.arrayProperty = function (res, obj, propName, descr, rowFactory) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        if (!rowFactory)
            rowFactory = function (val) {
                return '<bean class="' + val + '"/>';
            };

        res.startBlock('<property name="' + propName + '">');
        res.startBlock('<list>');

        _.forEach(val, function (v) {
            res.append(rowFactory(v));
        });

        res.endBlock('</list>');
        res.endBlock('</property>');
    }
};

/**
 * Add bean property with internal content.
 *
 * @param res Optional configuration presentation builder object.
 * @param bean Bean object for code generation.
 * @param beanPropName Name of property to set generated bean as value.
 * @param desc Bean metadata object.
 * @param createBeanAlthoughNoProps Always generate bean even it has no properties defined.
 */
$generatorXml.beanProperty = function (res, bean, beanPropName, desc, createBeanAlthoughNoProps) {
    var props = desc.fields;

    if (bean && $commonUtils.hasProperty(bean, props)) {
        if (!createBeanAlthoughNoProps)
            res.startSafeBlock();

        res.emptyLineIfNeeded();
        res.startBlock('<property name="' + beanPropName + '">');

        if (createBeanAlthoughNoProps)
            res.startSafeBlock();

        res.startBlock('<bean class="' + desc.className + '">');

        var hasData = false;

        _.forIn(props, function(descr, propName) {
            if (props.hasOwnProperty(propName)) {
                if (descr) {
                    switch (descr.type) {
                        case 'list':
                            $generatorXml.listProperty(res, bean, propName, descr.setterName);

                            break;

                        case 'array':
                            $generatorXml.arrayProperty(res, bean, propName, descr);

                            break;

                        case 'propertiesAsList':
                            var val = bean[propName];

                            if (val && val.length > 0) {
                                res.startBlock('<property name="' + propName + '">');
                                res.startBlock('<props>');

                                _.forEach(val, function(nameAndValue) {
                                    var eqIndex = nameAndValue.indexOf('=');
                                    if (eqIndex >= 0) {
                                        res.line('<prop key="' + $generatorXml.escape(nameAndValue.substring(0, eqIndex)) + '">' +
                                            $generatorXml.escape(nameAndValue.substr(eqIndex + 1)) + '</prop>');
                                    }
                                });

                                res.endBlock('</props>');
                                res.endBlock('</property>');

                                hasData = true;
                            }

                            break;

                        case 'bean':
                            if ($commonUtils.isDefinedAndNotEmpty(bean[propName])) {
                                res.startBlock('<property name="' + propName + '">');
                                res.line('<bean class="' + bean[propName] + '"/>');
                                res.endBlock('</property>');

                                hasData = true;
                            }

                            break;

                        default:
                            if ($generatorXml.property(res, bean, propName, descr.setterName, descr.dflt))
                                hasData = true;
                    }
                }
                else
                    if ($generatorXml.property(res, bean, propName))
                        hasData = true;
            }
        });

        res.endBlock('</bean>');

        if (createBeanAlthoughNoProps && !hasData) {
            res.rollbackSafeBlock();

            res.line('<bean class="' + desc.className + '"/>');
        }

        res.endBlock('</property>');

        if (!createBeanAlthoughNoProps && !hasData)
            res.rollbackSafeBlock();
    }
    else if (createBeanAlthoughNoProps) {
        res.emptyLineIfNeeded();
        res.startBlock('<property name="' + beanPropName + '">');
        res.line('<bean class="' + desc.className + '"/>');
        res.endBlock('</property>');
    }
};

/**
 * Add bean property without internal content.
 *
 * @param res Optional configuration presentation builder object.
 * @param obj Object to take bean class name.
 * @param propName Property name.
 */
$generatorXml.simpleBeanProperty = function (res, obj, propName) {
    if ($commonUtils.isDefined(obj)) {
        var val = obj[propName];

        if ($commonUtils.isDefinedAndNotEmpty(val)) {
            res.startBlock('<property name="' + propName + '">');
            res.line('<bean class="' + val + '"/>');
            res.endBlock('</property>');
        }
    }

    return false;
};

// Generate eviction policy.
$generatorXml.evictionPolicy = function (res, evtPlc, propName) {
    if (evtPlc && evtPlc.kind) {
        $generatorXml.beanProperty(res, evtPlc[evtPlc.kind.toUpperCase()], propName,
            $generatorCommon.EVICTION_POLICIES[evtPlc.kind], true);
    }
};

// Generate discovery.
$generatorXml.clusterGeneral = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'name', 'gridName');
    $generatorXml.property(res, cluster, 'localHost');

    if (cluster.discovery) {
        res.startBlock('<property name="discoverySpi">');
        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">');
        res.startBlock('<property name="ipFinder">');

        var d = cluster.discovery;

        switch (d.kind) {
            case 'Multicast':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder">');

                if (d.Multicast) {
                    $generatorXml.property(res, d.Multicast, 'multicastGroup');
                    $generatorXml.property(res, d.Multicast, 'multicastPort');
                    $generatorXml.property(res, d.Multicast, 'responseWaitTime');
                    $generatorXml.property(res, d.Multicast, 'addressRequestAttempts');
                    $generatorXml.property(res, d.Multicast, 'localAddress');
                    $generatorXml.listProperty(res, d.Multicast, 'addresses');
                }

                res.endBlock('</bean>');

                break;

            case 'Vm':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder">');

                if (d.Vm) {
                    $generatorXml.listProperty(res, d.Vm, 'addresses');
                }

                res.endBlock('</bean>');

                break;

            case 'S3':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder">');

                if (d.S3) {
                    if (d.S3.bucketName)
                        res.line('<property name="bucketName" value="' + $generatorXml.escape(d.S3.bucketName) + '"/>');
                }

                res.endBlock('</bean>');

                break;

            case 'Cloud':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder">');

                if (d.Cloud) {
                    $generatorXml.property(res, d.Cloud, 'credential');
                    $generatorXml.property(res, d.Cloud, 'credentialPath');
                    $generatorXml.property(res, d.Cloud, 'identity');
                    $generatorXml.property(res, d.Cloud, 'provider');
                    $generatorXml.listProperty(res, d.Cloud, 'regions');
                    $generatorXml.listProperty(res, d.Cloud, 'zones');
                }

                res.endBlock('</bean>');

                break;

            case 'GoogleStorage':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder">');

                if (d.GoogleStorage) {
                    $generatorXml.property(res, d.GoogleStorage, 'projectName');
                    $generatorXml.property(res, d.GoogleStorage, 'bucketName');
                    $generatorXml.property(res, d.GoogleStorage, 'serviceAccountP12FilePath');
                    $generatorXml.property(res, d.GoogleStorage, 'serviceAccountId');
                }

                res.endBlock('</bean>');

                break;

            case 'Jdbc':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder">');

                if (d.Jdbc) {
                    res.line('<property name="initSchema" value="' + ($commonUtils.isDefined(d.Jdbc.initSchema) && d.Jdbc.initSchema) + '"/>');
                }

                res.endBlock('</bean>');

                break;

            case 'SharedFs':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder">');

                if (d.SharedFs) {
                    $generatorXml.property(res, d.SharedFs, 'path');
                }

                res.endBlock('</bean>');

                break;

            default:
                res.line('Unknown discovery kind: ' + d.kind);
        }

        res.endBlock('</property>');

        $generatorXml.clusterDiscovery(d, res);

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate atomics group.
$generatorXml.clusterAtomics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var atomics = cluster.atomicConfiguration;

    if ($commonUtils.hasAtLeastOneProperty(atomics, ['cacheMode', 'atomicSequenceReserveSize', 'backups'])) {
        res.startSafeBlock();

        res.emptyLineIfNeeded();

        res.startBlock('<property name="atomicConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.AtomicConfiguration">');

        var cacheMode = atomics.cacheMode ? atomics.cacheMode : 'PARTITIONED';

        var hasData = cacheMode !== 'PARTITIONED';

        $generatorXml.property(res, atomics, 'cacheMode', null, 'PARTITIONED');

        hasData = $generatorXml.property(res, atomics, 'atomicSequenceReserveSize', null, 1000) || hasData;

        if (cacheMode === 'PARTITIONED')
            hasData = $generatorXml.property(res, atomics, 'backups', null, 0) || hasData;

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;

        if (!hasData)
            res.rollbackSafeBlock();
    }

    return res;
};

// Generate binary group.
$generatorXml.clusterBinary = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var binary = cluster.binaryConfiguration;

    if ($generatorCommon.binaryIsDefined(binary)) {
        res.startBlock('<property name="binaryConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.BinaryConfiguration">');

        $generatorXml.simpleBeanProperty(res, binary, 'idMapper');
        $generatorXml.simpleBeanProperty(res, binary, 'serializer');
        $generatorXml.simpleBeanProperty(res, binary, 'nameMapper');

        if ($commonUtils.isDefinedAndNotEmpty(binary.typeConfigurations)) {
            res.startBlock('<property name="typeConfigurations">');
            res.startBlock('<list>');

            _.forEach(binary.typeConfigurations, function (type) {
                res.startBlock('<bean class="org.apache.ignite.binary.BinaryTypeConfiguration">');

                $generatorXml.property(res, type, 'typeName');
                $generatorXml.simpleBeanProperty(res, type, 'idMapper');
                $generatorXml.simpleBeanProperty(res, type, 'nameMapper');
                $generatorXml.simpleBeanProperty(res, type, 'serializer');
                $generatorXml.property(res, type, 'enum', undefined, false);

                res.endBlock('</bean>');
            });

            res.endBlock('</list>');
            res.endBlock('</property>');
        }

        $generatorXml.property(res, binary, 'compactFooter', undefined, true);

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate communication group.
$generatorXml.clusterCommunication = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.beanProperty(res, cluster.communication, 'communicationSpi', $generatorCommon.COMMUNICATION_CONFIGURATION);

    $generatorXml.property(res, cluster, 'networkTimeout', undefined, 5000);
    $generatorXml.property(res, cluster, 'networkSendRetryDelay', undefined, 1000);
    $generatorXml.property(res, cluster, 'networkSendRetryCount', undefined, 3);
    $generatorXml.property(res, cluster, 'segmentCheckFrequency');
    $generatorXml.property(res, cluster, 'waitForSegmentOnStart', null, false);
    $generatorXml.property(res, cluster, 'discoveryStartupDelay', undefined, 60000);

    res.needEmptyLine = true;

    return res;
};

/**
 * XML generator for cluster's REST access configuration.
 *
 * @param cluster Cluster to get REST configuration.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object
 */
$generatorXml.clusterConnector = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($commonUtils.isDefined(cluster.connector) && cluster.connector.enabled) {
        var cfg = _.cloneDeep($generatorCommon.CONNECTOR_CONFIGURATION);

        if (cluster.connector.sslEnabled) {
            cfg.fields.sslClientAuth = {dflt: false};
            cfg.fields.sslFactory = {type: 'bean'};
        }

        $generatorXml.beanProperty(res, cluster.connector, 'connectorConfiguration', cfg, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate deployment group.
$generatorXml.clusterDeployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorXml.property(res, cluster, 'deploymentMode', null, 'SHARED'))
        res.needEmptyLine = true;

    var p2pEnabled = cluster.peerClassLoadingEnabled;

    if ($commonUtils.isDefined(p2pEnabled)) {
        $generatorXml.property(res, cluster, 'peerClassLoadingEnabled', null, false);

        if (p2pEnabled) {
            $generatorXml.property(res, cluster, 'peerClassLoadingMissedResourcesCacheSize', null, 100);
            $generatorXml.property(res, cluster, 'peerClassLoadingThreadPoolSize', null, 2);
            $generatorXml.listProperty(res, cluster, 'peerClassLoadingLocalClassPathExclude');
        }

        res.needEmptyLine = true;
    }

    return res;
};

// Generate discovery group.
$generatorXml.clusterDiscovery = function (disco, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, disco, 'localAddress');
    $generatorXml.property(res, disco, 'localPort', null, 47500);
    $generatorXml.property(res, disco, 'localPortRange', null, 100);
    if ($commonUtils.isDefinedAndNotEmpty(disco.addressResolver))
        $generatorXml.beanProperty(res, disco, 'addressResolver', {className: disco.addressResolver}, true);
    $generatorXml.property(res, disco, 'socketTimeout', null, 5000);
    $generatorXml.property(res, disco, 'ackTimeout', null, 5000);
    $generatorXml.property(res, disco, 'maxAckTimeout', null, 600000);
    $generatorXml.property(res, disco, 'networkTimeout', null, 5000);
    $generatorXml.property(res, disco, 'joinTimeout', null, 0);
    $generatorXml.property(res, disco, 'threadPriority', null, 10);
    $generatorXml.property(res, disco, 'heartbeatFrequency', null, 2000);
    $generatorXml.property(res, disco, 'maxMissedHeartbeats', null, 1);
    $generatorXml.property(res, disco, 'maxMissedClientHeartbeats', null, 5);
    $generatorXml.property(res, disco, 'topHistorySize', null, 1000);
    if ($commonUtils.isDefinedAndNotEmpty(disco.listener))
        $generatorXml.beanProperty(res, disco, 'listener', {className: disco.listener}, true);
    if ($commonUtils.isDefinedAndNotEmpty(disco.dataExchange))
        $generatorXml.beanProperty(res, disco, 'dataExchange', {className: disco.dataExchange}, true);
    if ($commonUtils.isDefinedAndNotEmpty(disco.metricsProvider))
        $generatorXml.beanProperty(res, disco, 'metricsProvider', {className: disco.metricsProvider}, true);
    $generatorXml.property(res, disco, 'reconnectCount', null, 10);
    $generatorXml.property(res, disco, 'statisticsPrintFrequency', null, 0);
    $generatorXml.property(res, disco, 'ipFinderCleanFrequency', null, 60000);
    if ($commonUtils.isDefinedAndNotEmpty(disco.authenticator))
        $generatorXml.beanProperty(res, disco, 'authenticator', {className: disco.authenticator}, true);
    $generatorXml.property(res, disco, 'forceServerMode', null, false);
    $generatorXml.property(res, disco, 'clientReconnectDisabled', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate events group.
$generatorXml.clusterEvents = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.includeEventTypes && cluster.includeEventTypes.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="includeEventTypes">');

        if (cluster.includeEventTypes.length === 1)
            res.line('<util:constant static-field="org.apache.ignite.events.EventType.' + cluster.includeEventTypes[0] + '"/>');
        else {
            res.startBlock('<list>');

            var evtGrps = angular.element(document.getElementById('app')).injector().get('igniteEventGroups');

            _.forEach(cluster.includeEventTypes, function(eventGroup, ix) {
                if (ix > 0)
                    res.line();

                res.line('<!-- EventType.' + eventGroup + ' -->');

                var evtGrp = _.find(evtGrps, {value: eventGroup});

                if (evtGrp)
                    _.forEach(evtGrp.events, function(event) {
                        res.line('<util:constant static-field="' + evtGrp.class + '.' + event + '"/>');
                    });
            });

            res.endBlock('</list>');
        }

        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate marshaller group.
$generatorXml.clusterMarshaller = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind) {
        $generatorXml.beanProperty(res, marshaller[marshaller.kind], 'marshaller', $generatorCommon.MARSHALLERS[marshaller.kind], true);

        res.needEmptyLine = true;
    }

    $generatorXml.property(res, cluster, 'marshalLocalJobs', null, false);
    $generatorXml.property(res, cluster, 'marshallerCacheKeepAliveTime', null, 10000);
    $generatorXml.property(res, cluster, 'marshallerCacheThreadPoolSize', 'marshallerCachePoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorXml.clusterMetrics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'metricsExpireTime');
    $generatorXml.property(res, cluster, 'metricsHistorySize', null, 10000);
    $generatorXml.property(res, cluster, 'metricsLogFrequency', null, 60000);
    $generatorXml.property(res, cluster, 'metricsUpdateFrequency', null, 2000);

    res.needEmptyLine = true;

    return res;
};

// Generate swap group.
$generatorXml.clusterSwap = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind === 'FileSwapSpaceSpi') {
        $generatorXml.beanProperty(res, cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi',
            $generatorCommon.SWAP_SPACE_SPI, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate time group.
$generatorXml.clusterTime = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'clockSyncSamples', null, 8);
    $generatorXml.property(res, cluster, 'clockSyncFrequency', null, 120000);
    $generatorXml.property(res, cluster, 'timeServerPortBase', null, 31100);
    $generatorXml.property(res, cluster, 'timeServerPortRange', null, 100);

    res.needEmptyLine = true;

    return res;
};

// Generate thread pools group.
$generatorXml.clusterPools = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'publicThreadPoolSize');
    $generatorXml.property(res, cluster, 'systemThreadPoolSize');
    $generatorXml.property(res, cluster, 'managementThreadPoolSize');
    $generatorXml.property(res, cluster, 'igfsThreadPoolSize');
    $generatorXml.property(res, cluster, 'rebalanceThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate transactions group.
$generatorXml.clusterTransactions = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.beanProperty(res, cluster.transactionConfiguration, 'transactionConfiguration', $generatorCommon.TRANSACTION_CONFIGURATION, false);

    res.needEmptyLine = true;

    return res;
};

/**
 * XML generator for cluster's SSL configuration.
 *
 * @param cluster Cluster to get SSL configuration.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object
 */
$generatorXml.clusterSsl = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.sslEnabled && $commonUtils.isDefined(cluster.sslContextFactory)) {
        cluster.sslContextFactory.keyStorePassword =
            $commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.keyStoreFilePath) ? '${ssl.key.storage.password}' : undefined;

        cluster.sslContextFactory.trustStorePassword =
            $commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.trustStoreFilePath) ? '${ssl.trust.storage.password}' : undefined;

        var propsDesc = $commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.trustManagers) ?
            $generatorCommon.SSL_CONFIGURATION_TRUST_MANAGER_FACTORY :
            $generatorCommon.SSL_CONFIGURATION_TRUST_FILE_FACTORY;

        $generatorXml.beanProperty(res, cluster.sslContextFactory, 'sslContextFactory', propsDesc, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache general group.
$generatorXml.cacheGeneral = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'name');

    $generatorXml.property(res, cache, 'cacheMode');
    $generatorXml.property(res, cache, 'atomicityMode');

    if (cache.cacheMode === 'PARTITIONED')
        $generatorXml.property(res, cache, 'backups');

    $generatorXml.property(res, cache, 'readFromBackup');
    $generatorXml.property(res, cache, 'copyOnRead');
    $generatorXml.property(res, cache, 'invalidate');

    res.needEmptyLine = true;

    return res;
};

// Generate cache memory group.
$generatorXml.cacheMemory = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'memoryMode', null, 'ONHEAP_TIERED');
    $generatorXml.property(res, cache, 'offHeapMaxMemory', null, -1);

    res.needEmptyLine = true;

    $generatorXml.evictionPolicy(res, cache.evictionPolicy, 'evictionPolicy');

    res.needEmptyLine = true;

    $generatorXml.property(res, cache, 'swapEnabled', null, false);
    $generatorXml.property(res, cache, 'startSize', null, 1500000);

    res.needEmptyLine = true;

    return res;
};

// Generate cache query & indexing group.
$generatorXml.cacheQuery = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'sqlSchema');
    $generatorXml.property(res, cache, 'sqlOnheapRowCacheSize', null, 10240);
    $generatorXml.property(res, cache, 'longQueryWarningTimeout', null, 3000);
    $generatorXml.property(res, cache, 'snapshotableIndex', null, false);

    var indexedTypes = _.filter(cache.domains, function (domain) {
        return domain.queryMetadata === 'Annotations'
    });

    if (indexedTypes.length > 0) {
        res.startBlock('<property name="indexedTypes">');
        res.startBlock('<list>');

        _.forEach(indexedTypes, function(domain) {
            res.line('<value>' + $dataStructures.fullClassName(domain.keyType) + '</value>');
            res.line('<value>' + $dataStructures.fullClassName(domain.valueType) + '</value>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    $generatorXml.listProperty(res, cache, 'sqlFunctionClasses');

    $generatorXml.property(res, cache, 'sqlEscapeAll', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate cache store group.
$generatorXml.cacheStore = function(cache, domains, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var factoryKind = cache.cacheStoreFactory.kind;

        var storeFactory = cache.cacheStoreFactory[factoryKind];

        if (storeFactory) {
            if (factoryKind === 'CacheJdbcPojoStoreFactory') {
                res.startBlock('<property name="cacheStoreFactory">');
                res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory">');

                $generatorXml.property(res, storeFactory, 'dataSourceBean');

                res.startBlock('<property name="dialect">');
                res.line('<bean class="' + $generatorCommon.jdbcDialectClassName(storeFactory.dialect) + '"/>');
                res.endBlock('</property>');

                var domainConfigs = _.filter(domains, function (domain) {
                    return $commonUtils.isDefinedAndNotEmpty(domain.databaseTable);
                });

                if ($commonUtils.isDefinedAndNotEmpty(domainConfigs)) {
                    res.startBlock('<property name="types">');
                    res.startBlock('<list>');

                    _.forEach(domainConfigs, function (domain) {
                        res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.JdbcType">');

                        $generatorXml.property(res, cache, 'name', 'cacheName');

                        $generatorXml.classNameProperty(res, domain, 'keyType');
                        $generatorXml.property(res, domain, 'valueType');

                        $generatorXml.domainStore(domain, res);

                        res.endBlock('</bean>');
                    });

                    res.endBlock('</list>');
                    res.endBlock('</property>');
                }

                res.endBlock('</bean>');
                res.endBlock("</property>");
            }
            else if (factoryKind === 'CacheJdbcBlobStoreFactory') {
                res.startBlock('<property name="cacheStoreFactory">');
                res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory">');

                if (storeFactory.connectVia === 'DataSource')
                    $generatorXml.property(res, storeFactory, 'dataSourceBean');
                else {
                    $generatorXml.property(res, storeFactory, 'connectionUrl');
                    $generatorXml.property(res, storeFactory, 'user');
                    res.line('<property name="password" value="${ds.' + storeFactory.user + '.password}"/>');
                }

                $generatorXml.property(res, storeFactory, 'initSchema');
                $generatorXml.property(res, storeFactory, 'createTableQuery');
                $generatorXml.property(res, storeFactory, 'loadQuery');
                $generatorXml.property(res, storeFactory, 'insertQuery');
                $generatorXml.property(res, storeFactory, 'updateQuery');
                $generatorXml.property(res, storeFactory, 'deleteQuery');

                res.endBlock('</bean>');
                res.endBlock("</property>");
            }
            else
                $generatorXml.beanProperty(res, storeFactory, 'cacheStoreFactory', $generatorCommon.STORE_FACTORIES[factoryKind], true);

            if (storeFactory.dataSourceBean && (storeFactory.connectVia ? (storeFactory.connectVia === 'DataSource' ? storeFactory.dialect : undefined) : storeFactory.dialect)) {
                if (_.findIndex(res.datasources, function (ds) {
                        return ds.dataSourceBean === storeFactory.dataSourceBean;
                    }) < 0) {
                    res.datasources.push({
                        dataSourceBean: storeFactory.dataSourceBean,
                        className: $generatorCommon.DATA_SOURCES[storeFactory.dialect],
                        dialect: storeFactory.dialect
                    });
                }
            }

            res.needEmptyLine = true;
        }
    }

    $generatorXml.property(res, cache, 'storeKeepBinary', null, false);
    $generatorXml.property(res, cache, 'loadPreviousValue', null, false);
    $generatorXml.property(res, cache, 'readThrough', null, false);
    $generatorXml.property(res, cache, 'writeThrough', null, false);

    res.needEmptyLine = true;

    if (cache.writeBehindEnabled) {
        $generatorXml.property(res, cache, 'writeBehindEnabled', null, false);
        $generatorXml.property(res, cache, 'writeBehindBatchSize', null, 512);
        $generatorXml.property(res, cache, 'writeBehindFlushSize', null, 10240);
        $generatorXml.property(res, cache, 'writeBehindFlushFrequency', null, 5000);
        $generatorXml.property(res, cache, 'writeBehindFlushThreadCount', null, 1);
    }

    res.needEmptyLine = true;

    return res;
};

// Generate cache concurrency group.
$generatorXml.cacheConcurrency = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'maxConcurrentAsyncOperations', null, 500);
    $generatorXml.property(res, cache, 'defaultLockTimeout', null, 0);
    $generatorXml.property(res, cache, 'atomicWriteOrderMode');
    $generatorXml.property(res, cache, 'writeSynchronizationMode', null, "PRIMARY_SYNC");

    res.needEmptyLine = true;

    return res;
};

// Generate cache rebalance group.
$generatorXml.cacheRebalance = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode !== 'LOCAL') {
        $generatorXml.property(res, cache, 'rebalanceMode', null, 'ASYNC');
        $generatorXml.property(res, cache, 'rebalanceThreadPoolSize', null, 1);
        $generatorXml.property(res, cache, 'rebalanceBatchSize', null, 524288);
        $generatorXml.property(res, cache, 'rebalanceBatchesPrefetchCount', null, 2);
        $generatorXml.property(res, cache, 'rebalanceOrder', null, 0);
        $generatorXml.property(res, cache, 'rebalanceDelay', null, 0);
        $generatorXml.property(res, cache, 'rebalanceTimeout', null, 10000);
        $generatorXml.property(res, cache, 'rebalanceThrottle', null, 0);

        res.needEmptyLine = true;
    }

    if (cache.igfsAffinnityGroupSize) {
        res.startBlock('<property name="affinityMapper">');
        res.startBlock('<bean class="org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper">');
        res.line('<constructor-arg value="' + cache.igfsAffinnityGroupSize + '"/>');
        res.endBlock('</bean>');
        res.endBlock('</property>');
    }

    return res;
};

// Generate cache server near cache group.
$generatorXml.cacheServerNearCache = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode === 'PARTITIONED' && cache.nearCacheEnabled) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="nearConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.NearCacheConfiguration">');

        if (cache.nearConfiguration) {
            if (cache.nearConfiguration.nearStartSize)
                $generatorXml.property(res, cache.nearConfiguration, 'nearStartSize', null, 375000);

            $generatorXml.evictionPolicy(res, cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');
        }

        res.endBlock('</bean>');
        res.endBlock('</property>');
    }

    res.needEmptyLine = true;

    return res;
};

// Generate cache statistics group.
$generatorXml.cacheStatistics = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'statisticsEnabled', null, false);
    $generatorXml.property(res, cache, 'managementEnabled', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate domain model query fields.
$generatorXml.domainModelQueryFields = function (res, domain) {
    var fields = domain.fields;

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="fields">');
        res.startBlock('<map>');

        _.forEach(fields, function (field) {
            $generatorXml.element(res, 'entry', 'key', field.name, 'value', $dataStructures.fullClassName(field.className));
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model query fields.
$generatorXml.domainModelQueryAliases = function (res, domain) {
    var aliases = domain.aliases;

    if (aliases && aliases.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="aliases">');
        res.startBlock('<map>');

        _.forEach(aliases, function (alias) {
            $generatorXml.element(res, 'entry', 'key', alias.field, 'value', alias.alias);
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model indexes.
$generatorXml.domainModelQueryIndexes = function (res, domain) {
    var indexes = domain.indexes;

    if (indexes && indexes.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="indexes">');
        res.startBlock('<list>');

        _.forEach(indexes, function (index) {
            res.startBlock('<bean class="org.apache.ignite.cache.QueryIndex">');

            $generatorXml.property(res, index, 'name');
            $generatorXml.property(res, index, 'indexType');

            var fields = index.fields;

            if (fields && fields.length > 0) {
                res.startBlock('<property name="fields">');
                res.startBlock('<map>');

                _.forEach(fields, function (field) {
                    $generatorXml.element(res, 'entry', 'key', field.name, 'value', field.direction);
                });

                res.endBlock('</map>');
                res.endBlock('</property>');
            }

            res.endBlock('</bean>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model db fields.
$generatorXml.domainModelDatabaseFields = function (res, domain, fieldProp) {
    var fields = domain[fieldProp];

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="' + fieldProp + '">');

        res.startBlock('<list>');

        _.forEach(fields, function (field) {
            res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField">');

            $generatorXml.property(res, field, 'databaseFieldName');

            res.startBlock('<property name="databaseFieldType">');
            res.line('<util:constant static-field="java.sql.Types.' + field.databaseFieldType + '"/>');
            res.endBlock('</property>');

            $generatorXml.property(res, field, 'javaFieldName');

            $generatorXml.classNameProperty(res, field, 'javaFieldType');

            res.endBlock('</bean>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model general group.
$generatorXml.domainModelGeneral = function(domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    switch ($generatorCommon.domainQueryMetadata(domain)) {
        case 'Annotations':
            if ($commonUtils.isDefinedAndNotEmpty(domain.keyType) || $commonUtils.isDefinedAndNotEmpty(domain.valueType)) {
                res.startBlock('<property name="indexedTypes">');
                res.startBlock('<list>');

                if ($commonUtils.isDefinedAndNotEmpty(domain.keyType))
                    res.line('<value>' + $dataStructures.fullClassName(domain.keyType) + '</value>');
                else
                    res.line('<value>???</value>');

                if ($commonUtils.isDefinedAndNotEmpty(domain.valueType))
                    res.line('<value>' + $dataStructures.fullClassName(domain.valueType) + '</value>');
                else
                    res.line('<value>>???</value>');

                res.endBlock('</list>');
                res.endBlock('</property>');
            }

            break;

        case 'Configuration':
            $generatorXml.classNameProperty(res, domain, 'keyType');
            $generatorXml.property(res, domain, 'valueType');

            break;
    }

    res.needEmptyLine = true;

    return res;
};

// Generate domain model for query group.
$generatorXml.domainModelQuery = function(domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.domainQueryMetadata(domain) === 'Configuration') {
        $generatorXml.domainModelQueryFields(res, domain);
        $generatorXml.domainModelQueryAliases(res, domain);
        $generatorXml.domainModelQueryIndexes(res, domain);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate domain model for store group.
$generatorXml.domainStore = function(domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, domain, 'databaseSchema');
    $generatorXml.property(res, domain, 'databaseTable');

    res.needEmptyLine = true;

    $generatorXml.domainModelDatabaseFields(res, domain, 'keyFields');
    $generatorXml.domainModelDatabaseFields(res, domain, 'valueFields');

    res.needEmptyLine = true;

    return res;
};

$generatorXml.cacheQueryMetadata = function(domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.cache.QueryEntity">');

    $generatorXml.classNameProperty(res, domain, 'keyType');
    $generatorXml.property(res, domain, 'valueType');

    $generatorXml.domainModelQuery(domain, res);

    res.endBlock('</bean>');

    res.needEmptyLine = true;

    return res;
};

// Generate domain models configs.
$generatorXml.cacheDomains = function(domains, res) {
    if (!res)
        res = $generatorCommon.builder();

    var domainConfigs = _.filter(domains, function (domain) {
        return $generatorCommon.domainQueryMetadata(domain) === 'Configuration' &&
            $commonUtils.isDefinedAndNotEmpty(domain.fields);
    });

    if ($commonUtils.isDefinedAndNotEmpty(domainConfigs)) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="queryEntities">');
        res.startBlock('<list>');

        _.forEach(domainConfigs, function (domain) {
            $generatorXml.cacheQueryMetadata(domain, res);
        });

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    return res;
};

// Generate cache configs.
$generatorXml.cache = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.configuration.CacheConfiguration">');

    $generatorXml.cacheConfiguration(cache, res);

    res.endBlock('</bean>');

    return res;
};

// Generate cache configs.
$generatorXml.cacheConfiguration = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.cacheGeneral(cache, res);
    $generatorXml.cacheMemory(cache, res);
    $generatorXml.cacheQuery(cache, res);
    $generatorXml.cacheStore(cache, cache.domains, res);
    $generatorXml.cacheConcurrency(cache, res);
    $generatorXml.cacheRebalance(cache, res);
    $generatorXml.cacheServerNearCache(cache, res);
    $generatorXml.cacheStatistics(cache, res);
    $generatorXml.cacheDomains(cache.domains, res);

    return res;
};

// Generate caches configs.
$generatorXml.clusterCaches = function(caches, igfss, isSrvCfg, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($commonUtils.isDefinedAndNotEmpty(caches) || (isSrvCfg && $commonUtils.isDefinedAndNotEmpty(igfss))) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="cacheConfiguration">');
        res.startBlock('<list>');

        _.forEach(caches, function(cache) {
            $generatorXml.cache(cache, res);

            res.needEmptyLine = true;
        });

        if (isSrvCfg)
            _.forEach(igfss, function(igfs) {
                $generatorXml.cache($generatorCommon.igfsDataCache(igfs), res);

                res.needEmptyLine = true;

                $generatorXml.cache($generatorCommon.igfsMetaCache(igfs), res);

                res.needEmptyLine = true;
            });

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFSs configs.
$generatorXml.igfss = function(igfss, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($commonUtils.isDefinedAndNotEmpty(igfss)) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="fileSystemConfiguration">');
        res.startBlock('<list>');

        _.forEach(igfss, function(igfs) {
            res.startBlock('<bean class="org.apache.ignite.configuration.FileSystemConfiguration">');

            $generatorXml.igfsGeneral(igfs, res);
            $generatorXml.igfsIPC(igfs, res);
            $generatorXml.igfsFragmentizer(igfs, res);
            $generatorXml.igfsDualMode(igfs, res);
            $generatorXml.igfsSecondFS(igfs, res);
            $generatorXml.igfsMisc(igfs, res);

            res.endBlock('</bean>');

            res.needEmptyLine = true;
        });

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS IPC configuration.
$generatorXml.igfsIPC = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.ipcEndpointEnabled) {
        $generatorXml.beanProperty(res, igfs.ipcEndpointConfiguration, 'ipcEndpointConfiguration', $generatorCommon.IGFS_IPC_CONFIGURATION, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS fragmentizer configuration.
$generatorXml.igfsFragmentizer = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.fragmentizerEnabled) {
        $generatorXml.property(res, igfs, 'fragmentizerConcurrentFiles', undefined, 0);
        $generatorXml.property(res, igfs, 'fragmentizerThrottlingBlockLength', undefined, 16777216);
        $generatorXml.property(res, igfs, 'fragmentizerThrottlingDelay', undefined, 200);

        res.needEmptyLine = true;
    }
    else
        $generatorXml.property(res, igfs, 'fragmentizerEnabled');

    return res;
};

// Generate IGFS dual mode configuration.
$generatorXml.igfsDualMode = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, igfs, 'dualModeMaxPendingPutsSize', undefined, 0);

    if ($commonUtils.isDefinedAndNotEmpty(igfs.dualModePutExecutorService)) {
        res.startBlock('<property name="dualModePutExecutorService">');
        res.line('<bean class="' + igfs.dualModePutExecutorService + '"/>');
        res.endBlock('</property>');
    }

    $generatorXml.property(res, igfs, 'dualModePutExecutorServiceShutdown', undefined, false);

    res.needEmptyLine = true;

    return res;
};

$generatorXml.igfsSecondFS = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.secondaryFileSystemEnabled) {
        var secondFs = igfs.secondaryFileSystem || {};

        res.startBlock('<property name="secondaryFileSystem">');

        res.startBlock('<bean class="org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem">');

        var nameDefined = $commonUtils.isDefinedAndNotEmpty(secondFs.userName);
        var cfgDefined = $commonUtils.isDefinedAndNotEmpty(secondFs.cfgPath);

        if ($commonUtils.isDefinedAndNotEmpty(secondFs.uri))
            res.line('<constructor-arg index="0" value="' + secondFs.uri + '"/>');
        else {
            res.startBlock('<constructor-arg index="0">');
            res.line('<null/>');
            res.endBlock('</constructor-arg>');
        }

        if (cfgDefined || nameDefined) {
            if (cfgDefined)
                res.line('<constructor-arg index="1" value="' + secondFs.cfgPath + '"/>');
            else {
                res.startBlock('<constructor-arg index="1">');
                res.line('<null/>');
                res.endBlock('</constructor-arg>');
            }
        }

        if ($commonUtils.isDefinedAndNotEmpty(secondFs.userName))
            res.line('<constructor-arg index="2" value="' + secondFs.userName + '"/>');

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS general configuration.
$generatorXml.igfsGeneral = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($commonUtils.isDefinedAndNotEmpty(igfs.name)) {
        igfs.dataCacheName = $generatorCommon.igfsDataCache(igfs).name;
        igfs.metaCacheName = $generatorCommon.igfsMetaCache(igfs).name;

        $generatorXml.property(res, igfs, 'name');
        $generatorXml.property(res, igfs, 'dataCacheName');
        $generatorXml.property(res, igfs, 'metaCacheName');
        $generatorXml.property(res, igfs, 'defaultMode', undefined, "DUAL_ASYNC");

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS misc configuration.
$generatorXml.igfsMisc = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, igfs, 'blockSize', undefined, 65536);
    $generatorXml.property(res, igfs, 'streamBufferSize', undefined, 65536);
    $generatorXml.property(res, igfs, 'maxSpaceSize', undefined, 0);
    $generatorXml.property(res, igfs, 'maximumTaskRangeLength', undefined, 0);
    $generatorXml.property(res, igfs, 'managementPort', undefined, 11400);

    res.needEmptyLine = true;

    if (igfs.pathModes && igfs.pathModes.length > 0) {
        res.startBlock('<property name="pathModes">');
        res.startBlock('<map>');

        _.forEach(igfs.pathModes, function(pair) {
            res.line('<entry key="' + pair.path + '" value="' + pair.mode + '"/>');
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    $generatorXml.property(res, igfs, 'perNodeBatchSize', undefined, 100);
    $generatorXml.property(res, igfs, 'perNodeParallelBatchCount', undefined, 8);
    $generatorXml.property(res, igfs, 'prefetchBlocks', undefined, 0);
    $generatorXml.property(res, igfs, 'sequentialReadsBeforePrefetch', undefined, 0);
    $generatorXml.property(res, igfs, 'trashPurgeTimeout', undefined, 1000);

    return res;
};

// Generate DataSource beans.
$generatorXml.generateDataSources = function (datasources, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (datasources.length > 0) {
        res.line('<!-- Data source beans will be initialized from external properties file. -->');

        _.forEach(datasources, function (item) {
            var beanId = item.dataSourceBean;

            res.startBlock('<bean id="' + beanId + '" class="' + item.className + '">');

            switch (item.dialect) {
                case 'Generic':
                    res.line('<property name="jdbcUrl" value="${' + beanId + '.jdbc.url}"/>');

                    break;

                case 'DB2':
                    res.line('<property name="serverName" value="${' + beanId + '.jdbc.server_name}"/>');
                    res.line('<property name="portNumber" value="${' + beanId + '.jdbc.port_number}"/>');
                    res.line('<property name="databaseName" value="${' + beanId + '.jdbc.database_name}"/>');
                    res.line('<property name="driverType" value="${' + beanId + '.jdbc.driver_type}"/>');

                    break;

                case 'PostgreSQL':
                    res.line('<property name="url" value="${' + beanId + '.jdbc.url}"/>');

                    break;

                default:
                    res.line('<property name="URL" value="${' + beanId + '.jdbc.url}"/>');
            }

            res.line('<property name="user" value="${' + beanId + '.jdbc.username}"/>');
            res.line('<property name="password" value="${' + beanId + '.jdbc.password}"/>');

            res.endBlock('</bean>');

            res.needEmptyLine = true;

            res.emptyLineIfNeeded();
        });

        res.needEmptyLine = true;

        res.emptyLineIfNeeded();
    }

    return res;
};

$generatorXml.clusterConfiguration = function (cluster, clientNearCfg, res) {
    var isSrvCfg = !$commonUtils.isDefined(clientNearCfg);

    if (!isSrvCfg) {
        res.line('<property name="clientMode" value="true"/>');

        res.needEmptyLine = true;
    }

    $generatorXml.clusterGeneral(cluster, res);

    $generatorXml.clusterAtomics(cluster, res);

    $generatorXml.clusterBinary(cluster, res);

    $generatorXml.clusterCommunication(cluster, res);

    $generatorXml.clusterConnector(cluster, res);

    $generatorXml.clusterDeployment(cluster, res);

    $generatorXml.clusterEvents(cluster, res);

    $generatorXml.clusterMarshaller(cluster, res);

    $generatorXml.clusterMetrics(cluster, res);

    $generatorXml.clusterSwap(cluster, res);

    $generatorXml.clusterTime(cluster, res);

    $generatorXml.clusterPools(cluster, res);

    $generatorXml.clusterTransactions(cluster, res);

    $generatorXml.clusterCaches(cluster.caches, cluster.igfss, isSrvCfg, res);

    $generatorXml.clusterSsl(cluster, res);

    if (isSrvCfg)
        $generatorXml.igfss(cluster.igfss, res);

    return res;
};

$generatorXml.cluster = function (cluster, clientNearCfg) {
    if (cluster) {
        var res = $generatorCommon.builder(1);

        if (clientNearCfg) {
            res.startBlock('<bean id="nearCacheBean" class="org.apache.ignite.configuration.NearCacheConfiguration">');

            if (clientNearCfg.nearStartSize)
                $generatorXml.property(res, clientNearCfg, 'nearStartSize');

            if (clientNearCfg.nearEvictionPolicy && clientNearCfg.nearEvictionPolicy.kind)
                $generatorXml.evictionPolicy(res, clientNearCfg.nearEvictionPolicy, 'nearEvictionPolicy');

            res.endBlock('</bean>');

            res.needEmptyLine = true;

            res.emptyLineIfNeeded();
        }

        // Generate Ignite Configuration.
        res.startBlock('<bean class="org.apache.ignite.configuration.IgniteConfiguration">');

        $generatorXml.clusterConfiguration(cluster, clientNearCfg, res);

        res.endBlock('</bean>');

        // Build final XML:
        // 1. Add header.
        var xml = '<?xml version="1.0" encoding="UTF-8"?>\n\n';

        xml += '<!-- ' + $generatorCommon.mainComment() + ' -->\n\n';
        xml += '<beans xmlns="http://www.springframework.org/schema/beans"\n';
        xml += '       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n';
        xml += '       xmlns:util="http://www.springframework.org/schema/util"\n';
        xml += '       xsi:schemaLocation="http://www.springframework.org/schema/beans\n';
        xml += '                           http://www.springframework.org/schema/beans/spring-beans.xsd\n';
        xml += '                           http://www.springframework.org/schema/util\n';
        xml += '                           http://www.springframework.org/schema/util/spring-util.xsd">\n';

        // 2. Add external property file
        if ($generatorCommon.secretPropertiesNeeded(cluster)) {
            xml += '    <!-- Load external properties file. -->\n';
            xml += '    <bean id="placeholderConfig" class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">\n';
            xml += '        <property name="location" value="classpath:secret.properties"/>\n';
            xml += '    </bean>\n\n';
        }

        // 3. Add data sources.
        xml += $generatorXml.generateDataSources(res.datasources, $generatorCommon.builder(1)).asString();

        // 3. Add main content.
        xml += res.asString();

        // 4. Add footer.
        xml += '\n</beans>';

        return xml;
    }

    return '';
};
