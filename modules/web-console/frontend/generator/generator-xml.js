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
const $generatorXml = {};

// Do XML escape.
$generatorXml.escape = function(s) {
    if (typeof (s) !== 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
};

// Add constructor argument
$generatorXml.constructorArg = function(res, ix, obj, propName, dflt, opt) {
    const v = (obj ? obj[propName] : null) || dflt;

    if ($generatorCommon.isDefinedAndNotEmpty(v))
        res.line('<constructor-arg ' + (ix >= 0 ? 'index="' + ix + '" ' : '') + 'value="' + v + '"/>');
    else if (!opt) {
        res.startBlock('<constructor-arg ' + (ix >= 0 ? 'index="' + ix + '"' : '') + '>');
        res.line('<null/>');
        res.endBlock('</constructor-arg>');
    }
};

// Add XML element.
$generatorXml.element = function(res, tag, attr1, val1, attr2, val2) {
    let elem = '<' + tag;

    if (attr1)
        elem += ' ' + attr1 + '="' + val1 + '"';

    if (attr2)
        elem += ' ' + attr2 + '="' + val2 + '"';

    elem += '/>';

    res.emptyLineIfNeeded();
    res.line(elem);
};

// Add property.
$generatorXml.property = function(res, obj, propName, setterName, dflt) {
    if (!_.isNil(obj)) {
        const val = obj[propName];

        if ($generatorCommon.isDefinedAndNotEmpty(val)) {
            const missDflt = _.isNil(dflt);

            // Add to result if no default provided or value not equals to default.
            if (missDflt || (!missDflt && val !== dflt)) {
                $generatorXml.element(res, 'property', 'name', setterName ? setterName : propName, 'value', $generatorXml.escape(val));

                return true;
            }
        }
    }

    return false;
};

// Add property for class name.
$generatorXml.classNameProperty = function(res, obj, propName) {
    const val = obj[propName];

    if (!_.isNil(val))
        $generatorXml.element(res, 'property', 'name', propName, 'value', $generatorCommon.JavaTypes.fullClassName(val));
};

// Add list property.
$generatorXml.listProperty = function(res, obj, propName, listType, rowFactory) {
    const val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        if (!listType)
            listType = 'list';

        if (!rowFactory)
            rowFactory = (v) => '<value>' + $generatorXml.escape(v) + '</value>';

        res.startBlock('<property name="' + propName + '">');
        res.startBlock('<' + listType + '>');

        _.forEach(val, (v) => res.line(rowFactory(v)));

        res.endBlock('</' + listType + '>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Add array property
$generatorXml.arrayProperty = function(res, obj, propName, descr, rowFactory) {
    const val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        if (!rowFactory)
            rowFactory = (v) => '<bean class="' + v + '"/>';

        res.startBlock('<property name="' + propName + '">');
        res.startBlock('<list>');

        _.forEach(val, (v) => res.append(rowFactory(v)));

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
$generatorXml.beanProperty = function(res, bean, beanPropName, desc, createBeanAlthoughNoProps) {
    const props = desc.fields;

    if (bean && $generatorCommon.hasProperty(bean, props)) {
        if (!createBeanAlthoughNoProps)
            res.startSafeBlock();

        res.emptyLineIfNeeded();
        res.startBlock('<property name="' + beanPropName + '">');

        if (createBeanAlthoughNoProps)
            res.startSafeBlock();

        res.startBlock('<bean class="' + desc.className + '">');

        let hasData = false;

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
                            const val = bean[propName];

                            if (val && val.length > 0) {
                                res.startBlock('<property name="' + propName + '">');
                                res.startBlock('<props>');

                                _.forEach(val, function(nameAndValue) {
                                    const eqIndex = nameAndValue.indexOf('=');
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
                            if ($generatorCommon.isDefinedAndNotEmpty(bean[propName])) {
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
$generatorXml.simpleBeanProperty = function(res, obj, propName) {
    if (!_.isNil(obj)) {
        const val = obj[propName];

        if ($generatorCommon.isDefinedAndNotEmpty(val)) {
            res.startBlock('<property name="' + propName + '">');
            res.line('<bean class="' + val + '"/>');
            res.endBlock('</property>');
        }
    }

    return false;
};

// Generate eviction policy.
$generatorXml.evictionPolicy = function(res, evtPlc, propName) {
    if (evtPlc && evtPlc.kind) {
        $generatorXml.beanProperty(res, evtPlc[evtPlc.kind.toUpperCase()], propName,
            $generatorCommon.EVICTION_POLICIES[evtPlc.kind], true);
    }
};

// Generate discovery.
$generatorXml.clusterGeneral = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'name', 'gridName');
    $generatorXml.property(res, cluster, 'localHost');

    if (cluster.discovery) {
        res.startBlock('<property name="discoverySpi">');
        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">');
        res.startBlock('<property name="ipFinder">');

        const d = cluster.discovery;

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

                if (d.Vm)
                    $generatorXml.listProperty(res, d.Vm, 'addresses');

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
                    const datasource = d.Jdbc;

                    res.line('<property name="initSchema" value="' + (!_.isNil(datasource.initSchema) && datasource.initSchema) + '"/>');

                    if (datasource.dataSourceBean && datasource.dialect) {
                        res.line('<property name="dataSource" ref="' + datasource.dataSourceBean + '"/>');

                        if (_.findIndex(res.datasources, (ds) => ds.dataSourceBean === datasource.dataSourceBean) < 0) {
                            res.datasources.push({
                                dataSourceBean: datasource.dataSourceBean,
                                dialect: datasource.dialect
                            });
                        }
                    }
                }

                res.endBlock('</bean>');

                break;

            case 'SharedFs':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder">');

                if (d.SharedFs)
                    $generatorXml.property(res, d.SharedFs, 'path');

                res.endBlock('</bean>');

                break;

            case 'ZooKeeper':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder">');

                if (d.ZooKeeper) {
                    if ($generatorCommon.isDefinedAndNotEmpty(d.ZooKeeper.curator)) {
                        res.startBlock('<property name="curator">');
                        res.line('<bean class="' + d.ZooKeeper.curator + '"/>');
                        res.endBlock('</property>');
                    }

                    $generatorXml.property(res, d.ZooKeeper, 'zkConnectionString');

                    if (d.ZooKeeper.retryPolicy && d.ZooKeeper.retryPolicy.kind) {
                        const kind = d.ZooKeeper.retryPolicy.kind;
                        const retryPolicy = d.ZooKeeper.retryPolicy[kind];
                        const customClassDefined = retryPolicy && $generatorCommon.isDefinedAndNotEmpty(retryPolicy.className);

                        if (kind !== 'Custom' || customClassDefined)
                            res.startBlock('<property name="retryPolicy">');

                        switch (kind) {
                            case 'ExponentialBackoff':
                                res.startBlock('<bean class="org.apache.curator.retry.ExponentialBackoffRetry">');
                                $generatorXml.constructorArg(res, 0, retryPolicy, 'baseSleepTimeMs', 1000);
                                $generatorXml.constructorArg(res, 1, retryPolicy, 'maxRetries', 10);
                                $generatorXml.constructorArg(res, 2, retryPolicy, 'maxSleepMs', null, true);
                                res.endBlock('</bean>');

                                break;

                            case 'BoundedExponentialBackoff':
                                res.startBlock('<bean class="org.apache.curator.retry.BoundedExponentialBackoffRetry">');
                                $generatorXml.constructorArg(res, 0, retryPolicy, 'baseSleepTimeMs', 1000);
                                $generatorXml.constructorArg(res, 1, retryPolicy, 'maxSleepTimeMs', 2147483647);
                                $generatorXml.constructorArg(res, 2, retryPolicy, 'maxRetries', 10);
                                res.endBlock('</bean>');

                                break;

                            case 'UntilElapsed':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryUntilElapsed">');
                                $generatorXml.constructorArg(res, 0, retryPolicy, 'maxElapsedTimeMs', 60000);
                                $generatorXml.constructorArg(res, 1, retryPolicy, 'sleepMsBetweenRetries', 1000);
                                res.endBlock('</bean>');

                                break;

                            case 'NTimes':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryNTimes">');
                                $generatorXml.constructorArg(res, 0, retryPolicy, 'n', 10);
                                $generatorXml.constructorArg(res, 1, retryPolicy, 'sleepMsBetweenRetries', 1000);
                                res.endBlock('</bean>');

                                break;

                            case 'OneTime':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryOneTime">');
                                $generatorXml.constructorArg(res, 0, retryPolicy, 'sleepMsBetweenRetry', 1000);
                                res.endBlock('</bean>');

                                break;

                            case 'Forever':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryForever">');
                                $generatorXml.constructorArg(res, 0, retryPolicy, 'retryIntervalMs', 1000);
                                res.endBlock('</bean>');

                                break;

                            case 'Custom':
                                if (customClassDefined)
                                    res.line('<bean class="' + retryPolicy.className + '"/>');

                                break;

                            default:
                        }

                        if (kind !== 'Custom' || customClassDefined)
                            res.endBlock('</property>');
                    }

                    $generatorXml.property(res, d.ZooKeeper, 'basePath', null, '/services');
                    $generatorXml.property(res, d.ZooKeeper, 'serviceName', null, 'ignite');
                    $generatorXml.property(res, d.ZooKeeper, 'allowDuplicateRegistrations', null, false);
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
$generatorXml.clusterAtomics = function(atomics, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.hasAtLeastOneProperty(atomics, ['cacheMode', 'atomicSequenceReserveSize', 'backups'])) {
        res.startSafeBlock();

        res.emptyLineIfNeeded();

        res.startBlock('<property name="atomicConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.AtomicConfiguration">');

        const cacheMode = atomics.cacheMode ? atomics.cacheMode : 'PARTITIONED';

        let hasData = cacheMode !== 'PARTITIONED';

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
$generatorXml.clusterBinary = function(binary, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.binaryIsDefined(binary)) {
        res.startBlock('<property name="binaryConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.BinaryConfiguration">');

        $generatorXml.simpleBeanProperty(res, binary, 'idMapper');
        $generatorXml.simpleBeanProperty(res, binary, 'nameMapper');
        $generatorXml.simpleBeanProperty(res, binary, 'serializer');

        if ($generatorCommon.isDefinedAndNotEmpty(binary.typeConfigurations)) {
            res.startBlock('<property name="typeConfigurations">');
            res.startBlock('<list>');

            _.forEach(binary.typeConfigurations, function(type) {
                res.startBlock('<bean class="org.apache.ignite.binary.BinaryTypeConfiguration">');

                $generatorXml.property(res, type, 'typeName');
                $generatorXml.simpleBeanProperty(res, type, 'idMapper');
                $generatorXml.simpleBeanProperty(res, type, 'nameMapper');
                $generatorXml.simpleBeanProperty(res, type, 'serializer');
                $generatorXml.property(res, type, 'enum', null, false);

                res.endBlock('</bean>');
            });

            res.endBlock('</list>');
            res.endBlock('</property>');
        }

        $generatorXml.property(res, binary, 'compactFooter', null, true);

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache key configurations.
$generatorXml.clusterCacheKeyConfiguration = function(keyCfgs, res) {
    if (!res)
        res = $generatorCommon.builder();

    keyCfgs = _.filter(keyCfgs, (cfg) => cfg.typeName && cfg.affinityKeyFieldName);

    if (_.isEmpty(keyCfgs))
        return res;

    res.startBlock('<property name="cacheKeyConfiguration">');
    res.startBlock('<array>');

    _.forEach(keyCfgs, (cfg) => {
        res.startBlock('<bean class="org.apache.ignite.cache.CacheKeyConfiguration">');

        $generatorXml.constructorArg(res, -1, cfg, 'typeName');
        $generatorXml.constructorArg(res, -1, cfg, 'affinityKeyFieldName');

        res.endBlock('</bean>');
    });

    res.endBlock('</array>');
    res.endBlock('</property>');

    return res;
};

// Generate collision group.
$generatorXml.clusterCollision = function(collision, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (collision && collision.kind && collision.kind !== 'Noop') {
        const spi = collision[collision.kind];

        if (collision.kind !== 'Custom' || (spi && $generatorCommon.isDefinedAndNotEmpty(spi.class))) {
            res.startBlock('<property name="collisionSpi">');

            switch (collision.kind) {
                case 'JobStealing':
                    res.startBlock('<bean class="org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi">');
                    $generatorXml.property(res, spi, 'activeJobsThreshold', null, 95);
                    $generatorXml.property(res, spi, 'waitJobsThreshold', null, 0);
                    $generatorXml.property(res, spi, 'messageExpireTime', null, 1000);
                    $generatorXml.property(res, spi, 'maximumStealingAttempts', null, 5);
                    $generatorXml.property(res, spi, 'stealingEnabled', null, true);

                    if ($generatorCommon.isDefinedAndNotEmpty(spi.externalCollisionListener)) {
                        res.needEmptyLine = true;

                        res.startBlock('<property name="externalCollisionListener">');
                        res.line('<bean class="' + spi.externalCollisionListener + ' "/>');
                        res.endBlock('</property>');
                    }

                    if ($generatorCommon.isDefinedAndNotEmpty(spi.stealingAttributes)) {
                        res.needEmptyLine = true;

                        res.startBlock('<property name="stealingAttributes">');
                        res.startBlock('<map>');

                        _.forEach(spi.stealingAttributes, function(attr) {
                            $generatorXml.element(res, 'entry', 'key', attr.name, 'value', attr.value);
                        });

                        res.endBlock('</map>');
                        res.endBlock('</property>');
                    }

                    res.endBlock('</bean>');

                    break;

                case 'FifoQueue':
                    res.startBlock('<bean class="org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi">');
                    $generatorXml.property(res, spi, 'parallelJobsNumber');
                    $generatorXml.property(res, spi, 'waitingJobsNumber');
                    res.endBlock('</bean>');

                    break;

                case 'PriorityQueue':
                    res.startBlock('<bean class="org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi">');
                    $generatorXml.property(res, spi, 'parallelJobsNumber');
                    $generatorXml.property(res, spi, 'waitingJobsNumber');
                    $generatorXml.property(res, spi, 'priorityAttributeKey', null, 'grid.task.priority');
                    $generatorXml.property(res, spi, 'jobPriorityAttributeKey', null, 'grid.job.priority');
                    $generatorXml.property(res, spi, 'defaultPriority', null, 0);
                    $generatorXml.property(res, spi, 'starvationIncrement', null, 1);
                    $generatorXml.property(res, spi, 'starvationPreventionEnabled', null, true);
                    res.endBlock('</bean>');

                    break;

                case 'Custom':
                    res.line('<bean class="' + spi.class + '"/>');

                    break;

                default:
            }

            res.endBlock('</property>');
        }
    }

    return res;
};

// Generate communication group.
$generatorXml.clusterCommunication = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.beanProperty(res, cluster.communication, 'communicationSpi', $generatorCommon.COMMUNICATION_CONFIGURATION);

    $generatorXml.property(res, cluster, 'networkTimeout', null, 5000);
    $generatorXml.property(res, cluster, 'networkSendRetryDelay', null, 1000);
    $generatorXml.property(res, cluster, 'networkSendRetryCount', null, 3);
    $generatorXml.property(res, cluster, 'segmentCheckFrequency');
    $generatorXml.property(res, cluster, 'waitForSegmentOnStart', null, false);
    $generatorXml.property(res, cluster, 'discoveryStartupDelay', null, 60000);

    res.needEmptyLine = true;

    return res;
};

/**
 * XML generator for cluster's REST access configuration.
 *
 * @param connector Cluster REST connector configuration.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object
 */
$generatorXml.clusterConnector = function(connector, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!_.isNil(connector) && connector.enabled) {
        const cfg = _.cloneDeep($generatorCommon.CONNECTOR_CONFIGURATION);

        if (connector.sslEnabled) {
            cfg.fields.sslClientAuth = {dflt: false};
            cfg.fields.sslFactory = {type: 'bean'};
        }

        $generatorXml.beanProperty(res, connector, 'connectorConfiguration', cfg, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate deployment group.
$generatorXml.clusterDeployment = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorXml.property(res, cluster, 'deploymentMode', null, 'SHARED'))
        res.needEmptyLine = true;

    const p2pEnabled = cluster.peerClassLoadingEnabled;

    if (!_.isNil(p2pEnabled)) {
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
$generatorXml.clusterDiscovery = function(disco, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (disco) {
        $generatorXml.property(res, disco, 'localAddress');
        $generatorXml.property(res, disco, 'localPort', null, 47500);
        $generatorXml.property(res, disco, 'localPortRange', null, 100);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.addressResolver))
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
        if ($generatorCommon.isDefinedAndNotEmpty(disco.listener))
            $generatorXml.beanProperty(res, disco, 'listener', {className: disco.listener}, true);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.dataExchange))
            $generatorXml.beanProperty(res, disco, 'dataExchange', {className: disco.dataExchange}, true);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.metricsProvider))
            $generatorXml.beanProperty(res, disco, 'metricsProvider', {className: disco.metricsProvider}, true);
        $generatorXml.property(res, disco, 'reconnectCount', null, 10);
        $generatorXml.property(res, disco, 'statisticsPrintFrequency', null, 0);
        $generatorXml.property(res, disco, 'ipFinderCleanFrequency', null, 60000);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.authenticator))
            $generatorXml.beanProperty(res, disco, 'authenticator', {className: disco.authenticator}, true);
        $generatorXml.property(res, disco, 'forceServerMode', null, false);
        $generatorXml.property(res, disco, 'clientReconnectDisabled', null, false);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate events group.
$generatorXml.clusterEvents = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.includeEventTypes && cluster.includeEventTypes.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="includeEventTypes">');

        const evtGrps = angular.element(document.getElementById('app')).injector().get('igniteEventGroups');

        if (cluster.includeEventTypes.length === 1) {
            const evtGrp = _.find(evtGrps, {value: cluster.includeEventTypes[0]});

            if (evtGrp)
                res.line('<util:constant static-field="' + evtGrp.class + '.' + evtGrp.value + '"/>');
        }
        else {
            res.startBlock('<list>');

            _.forEach(cluster.includeEventTypes, (item, ix) => {
                ix > 0 && res.line();

                const evtGrp = _.find(evtGrps, {value: item});

                if (evtGrp) {
                    res.line('<!-- EventType.' + item + ' -->');

                    _.forEach(evtGrp.events, (event) => res.line('<util:constant static-field="' + evtGrp.class + '.' + event + '"/>'));
                }
            });

            res.endBlock('</list>');
        }

        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate failover group.
$generatorXml.clusterFailover = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.isDefinedAndNotEmpty(cluster.failoverSpi) && _.findIndex(cluster.failoverSpi, function(spi) {
        return $generatorCommon.isDefinedAndNotEmpty(spi.kind) && (spi.kind !== 'Custom' || $generatorCommon.isDefinedAndNotEmpty(_.get(spi, spi.kind + '.class')));
    }) >= 0) {
        res.startBlock('<property name="failoverSpi">');
        res.startBlock('<list>');

        _.forEach(cluster.failoverSpi, function(spi) {
            if (spi.kind && (spi.kind !== 'Custom' || $generatorCommon.isDefinedAndNotEmpty(_.get(spi, spi.kind + '.class')))) {
                const maxAttempts = _.get(spi, spi.kind + '.maximumFailoverAttempts');

                if ((spi.kind === 'JobStealing' || spi.kind === 'Always') && $generatorCommon.isDefinedAndNotEmpty(maxAttempts) && maxAttempts !== 5) {
                    res.startBlock('<bean class="' + $generatorCommon.failoverSpiClass(spi) + '">');

                    $generatorXml.property(res, spi[spi.kind], 'maximumFailoverAttempts', null, 5);

                    res.endBlock('</bean>');
                }
                else
                    res.line('<bean class="' + $generatorCommon.failoverSpiClass(spi) + '"/>');

                res.needEmptyLine = true;
            }
        });

        res.needEmptyLine = true;

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    return res;
};

// Generate marshaller group.
$generatorXml.clusterLogger = function(logger, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.loggerConfigured(logger)) {
        res.startBlock('<property name="gridLogger">');

        const log = logger[logger.kind];

        switch (logger.kind) {
            case 'Log4j2':
                res.startBlock('<bean class="org.apache.ignite.logger.log4j2.Log4J2Logger">');
                res.line('<constructor-arg value="' + $generatorXml.escape(log.path) + '"/>');
                $generatorXml.property(res, log, 'level');
                res.endBlock('</bean>');

                break;

            case 'Null':
                res.line('<bean class="org.apache.ignite.logger.NullLogger"/>');

                break;

            case 'Java':
                res.line('<bean class="org.apache.ignite.logger.java.JavaLogger"/>');

                break;

            case 'JCL':
                res.line('<bean class="org.apache.ignite.logger.jcl.JclLogger"/>');

                break;

            case 'SLF4J':
                res.line('<bean class="org.apache.ignite.logger.slf4j.Slf4jLogger"/>');

                break;

            case 'Log4j':
                if (log.mode === 'Default' && !$generatorCommon.isDefinedAndNotEmpty(log.level))
                    res.line('<bean class="org.apache.ignite.logger.log4j.Log4JLogger"/>');
                else {
                    res.startBlock('<bean class="org.apache.ignite.logger.log4j.Log4JLogger">');

                    if (log.mode === 'Path')
                        res.line('<constructor-arg value="' + $generatorXml.escape(log.path) + '"/>');

                    $generatorXml.property(res, log, 'level');
                    res.endBlock('</bean>');
                }

                break;

            case 'Custom':
                res.line('<bean class="' + log.class + '"/>');

                break;

            default:
        }

        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate marshaller group.
$generatorXml.clusterMarshaller = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    const marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind)
        $generatorXml.beanProperty(res, marshaller[marshaller.kind], 'marshaller', $generatorCommon.MARSHALLERS[marshaller.kind], true);

    res.softEmptyLine();

    $generatorXml.property(res, cluster, 'marshalLocalJobs', null, false);
    $generatorXml.property(res, cluster, 'marshallerCacheKeepAliveTime', null, 10000);
    $generatorXml.property(res, cluster, 'marshallerCacheThreadPoolSize', 'marshallerCachePoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorXml.clusterMetrics = function(cluster, res) {
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
$generatorXml.clusterSwap = function(cluster, res) {
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
$generatorXml.clusterTime = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cluster, 'clockSyncSamples', null, 8);
    $generatorXml.property(res, cluster, 'clockSyncFrequency', null, 120000);
    $generatorXml.property(res, cluster, 'timeServerPortBase', null, 31100);
    $generatorXml.property(res, cluster, 'timeServerPortRange', null, 100);

    res.needEmptyLine = true;

    return res;
};

// Generate OBC configuration group.
$generatorXml.clusterODBC = function(odbc, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (odbc && odbc.odbcEnabled)
        $generatorXml.beanProperty(res, odbc, 'odbcConfiguration', $generatorCommon.ODBC_CONFIGURATION, true);

    res.needEmptyLine = true;

    return res;
};

// Generate thread pools group.
$generatorXml.clusterPools = function(cluster, res) {
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
$generatorXml.clusterTransactions = function(transactionConfiguration, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.beanProperty(res, transactionConfiguration, 'transactionConfiguration', $generatorCommon.TRANSACTION_CONFIGURATION, false);

    res.needEmptyLine = true;

    return res;
};

// Generate user attributes group.
$generatorXml.clusterUserAttributes = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.isDefinedAndNotEmpty(cluster.attributes)) {
        res.startBlock('<property name="userAttributes">');
        res.startBlock('<map>');

        _.forEach(cluster.attributes, function(attr) {
            $generatorXml.element(res, 'entry', 'key', attr.name, 'value', attr.value);
        });

        res.endBlock('</map>');
        res.endBlock('</property>');
    }

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

    if (cluster.sslEnabled && !_.isNil(cluster.sslContextFactory)) {
        let sslFactory;

        if (_.isEmpty(cluster.sslContextFactory.keyStoreFilePath) && _.isEmpty(cluster.sslContextFactory.trustStoreFilePath))
            sslFactory = cluster.sslContextFactory;
        else {
            sslFactory = _.clone(cluster.sslContextFactory);

            sslFactory.keyStorePassword = _.isEmpty(cluster.sslContextFactory.keyStoreFilePath) ? null : '${ssl.key.storage.password}';
            sslFactory.trustStorePassword = _.isEmpty(cluster.sslContextFactory.trustStoreFilePath) ? null : '${ssl.trust.storage.password}';
        }

        const propsDesc = $generatorCommon.isDefinedAndNotEmpty(cluster.sslContextFactory.trustManagers) ?
            $generatorCommon.SSL_CONFIGURATION_TRUST_MANAGER_FACTORY :
            $generatorCommon.SSL_CONFIGURATION_TRUST_FILE_FACTORY;

        $generatorXml.beanProperty(res, sslFactory, 'sslContextFactory', propsDesc, true);

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

    if (cache.cacheMode === 'PARTITIONED' && $generatorXml.property(res, cache, 'backups'))
        $generatorXml.property(res, cache, 'readFromBackup');

    $generatorXml.property(res, cache, 'copyOnRead');

    if (cache.cacheMode === 'PARTITIONED' && cache.atomicityMode === 'TRANSACTIONAL')
        $generatorXml.property(res, cache, 'invalidate');

    res.needEmptyLine = true;

    return res;
};

// Generate cache memory group.
$generatorXml.cacheMemory = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'memoryMode', null, 'ONHEAP_TIERED');

    if (cache.memoryMode !== 'OFFHEAP_VALUES')
        $generatorXml.property(res, cache, 'offHeapMaxMemory', null, -1);

    res.softEmptyLine();

    $generatorXml.evictionPolicy(res, cache.evictionPolicy, 'evictionPolicy');

    res.softEmptyLine();

    $generatorXml.property(res, cache, 'startSize', null, 1500000);
    $generatorXml.property(res, cache, 'swapEnabled', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate cache query & indexing group.
$generatorXml.cacheQuery = function(cache, domains, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, cache, 'sqlSchema');
    $generatorXml.property(res, cache, 'sqlOnheapRowCacheSize', null, 10240);
    $generatorXml.property(res, cache, 'longQueryWarningTimeout', null, 3000);

    const indexedTypes = _.filter(domains, (domain) => domain.queryMetadata === 'Annotations');

    if (indexedTypes.length > 0) {
        res.softEmptyLine();

        res.startBlock('<property name="indexedTypes">');
        res.startBlock('<list>');

        _.forEach(indexedTypes, function(domain) {
            res.line('<value>' + $generatorCommon.JavaTypes.fullClassName(domain.keyType) + '</value>');
            res.line('<value>' + $generatorCommon.JavaTypes.fullClassName(domain.valueType) + '</value>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    res.softEmptyLine();

    $generatorXml.listProperty(res, cache, 'sqlFunctionClasses');

    res.softEmptyLine();

    $generatorXml.property(res, cache, 'snapshotableIndex', null, false);
    $generatorXml.property(res, cache, 'sqlEscapeAll', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate cache store group.
$generatorXml.cacheStore = function(cache, domains, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        const factoryKind = cache.cacheStoreFactory.kind;

        const storeFactory = cache.cacheStoreFactory[factoryKind];

        if (storeFactory) {
            if (factoryKind === 'CacheJdbcPojoStoreFactory') {
                res.startBlock('<property name="cacheStoreFactory">');
                res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory">');

                $generatorXml.property(res, storeFactory, 'dataSourceBean');

                res.startBlock('<property name="dialect">');
                res.line('<bean class="' + $generatorCommon.jdbcDialectClassName(storeFactory.dialect) + '"/>');
                res.endBlock('</property>');

                const domainConfigs = _.filter(domains, function(domain) {
                    return $generatorCommon.isDefinedAndNotEmpty(domain.databaseTable);
                });

                if ($generatorCommon.isDefinedAndNotEmpty(domainConfigs)) {
                    res.startBlock('<property name="types">');
                    res.startBlock('<list>');

                    _.forEach(domainConfigs, function(domain) {
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
                res.endBlock('</property>');
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
                res.endBlock('</property>');
            }
            else
                $generatorXml.beanProperty(res, storeFactory, 'cacheStoreFactory', $generatorCommon.STORE_FACTORIES[factoryKind], true);

            if (storeFactory.dataSourceBean && (storeFactory.connectVia ? (storeFactory.connectVia === 'DataSource' ? storeFactory.dialect : null) : storeFactory.dialect)) {
                if (_.findIndex(res.datasources, (ds) => ds.dataSourceBean === storeFactory.dataSourceBean) < 0) {
                    res.datasources.push({
                        dataSourceBean: storeFactory.dataSourceBean,
                        dialect: storeFactory.dialect
                    });
                }
            }
        }
    }

    res.softEmptyLine();

    $generatorXml.property(res, cache, 'storeKeepBinary', null, false);
    $generatorXml.property(res, cache, 'loadPreviousValue', null, false);
    $generatorXml.property(res, cache, 'readThrough', null, false);
    $generatorXml.property(res, cache, 'writeThrough', null, false);

    res.softEmptyLine();

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

// Generate cache node filter group.
$generatorXml.cacheNodeFilter = function(cache, igfss, res) {
    if (!res)
        res = $generatorCommon.builder();

    const kind = _.get(cache, 'nodeFilter.kind');

    if (_.isNil(kind) || _.isNil(cache.nodeFilter[kind]))
        return res;

    switch (kind) {
        case 'IGFS':
            const foundIgfs = _.find(igfss, (igfs) => igfs._id === cache.nodeFilter.IGFS.igfs);

            if (foundIgfs) {
                res.startBlock('<property name="nodeFilter">');
                res.startBlock('<bean class="org.apache.ignite.internal.processors.igfs.IgfsNodePredicate">');
                res.line('<constructor-arg value="' + foundIgfs.name + '"/>');
                res.endBlock('</bean>');
                res.endBlock('</property>');
            }

            break;

        case 'OnNodes':
            const nodes = cache.nodeFilter.OnNodes.nodeIds;

            if ($generatorCommon.isDefinedAndNotEmpty(nodes)) {
                res.startBlock('<property name="nodeFilter">');
                res.startBlock('<bean class="org.apache.ignite.internal.util.lang.GridNodePredicate">');
                res.startBlock('<constructor-arg>');
                res.startBlock('<list>');

                _.forEach(nodes, (nodeId) => {
                    res.startBlock('<bean class="java.util.UUID" factory-method="fromString">');
                    res.line('<constructor-arg value="' + nodeId + '"/>');
                    res.endBlock('</bean>');
                });

                res.endBlock('</list>');
                res.endBlock('</constructor-arg>');
                res.endBlock('</bean>');
                res.endBlock('</property>');
            }

            break;

        case 'Custom':
            res.startBlock('<property name="nodeFilter">');
            res.line('<bean class="' + cache.nodeFilter.Custom.className + '"/>');
            res.endBlock('</property>');

            break;

        default: break;
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
    $generatorXml.property(res, cache, 'writeSynchronizationMode', null, 'PRIMARY_SYNC');

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
    }

    res.softEmptyLine();

    if (cache.igfsAffinnityGroupSize) {
        res.startBlock('<property name="affinityMapper">');
        res.startBlock('<bean class="org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper">');
        $generatorXml.constructorArg(res, -1, cache, 'igfsAffinnityGroupSize');
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
$generatorXml.domainModelQueryFields = function(res, domain) {
    const fields = domain.fields;

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="fields">');
        res.startBlock('<map>');

        _.forEach(fields, function(field) {
            $generatorXml.element(res, 'entry', 'key', field.name, 'value', $generatorCommon.JavaTypes.fullClassName(field.className));
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model query fields.
$generatorXml.domainModelQueryAliases = function(res, domain) {
    const aliases = domain.aliases;

    if (aliases && aliases.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="aliases">');
        res.startBlock('<map>');

        _.forEach(aliases, function(alias) {
            $generatorXml.element(res, 'entry', 'key', alias.field, 'value', alias.alias);
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model indexes.
$generatorXml.domainModelQueryIndexes = function(res, domain) {
    const indexes = domain.indexes;

    if (indexes && indexes.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="indexes">');
        res.startBlock('<list>');

        _.forEach(indexes, function(index) {
            res.startBlock('<bean class="org.apache.ignite.cache.QueryIndex">');

            $generatorXml.property(res, index, 'name');
            $generatorXml.property(res, index, 'indexType');

            const fields = index.fields;

            if (fields && fields.length > 0) {
                res.startBlock('<property name="fields">');
                res.startBlock('<map>');

                _.forEach(fields, function(field) {
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
$generatorXml.domainModelDatabaseFields = function(res, domain, fieldProp) {
    const fields = domain[fieldProp];

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="' + fieldProp + '">');

        res.startBlock('<list>');

        _.forEach(fields, function(field) {
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
            if ($generatorCommon.isDefinedAndNotEmpty(domain.keyType) || $generatorCommon.isDefinedAndNotEmpty(domain.valueType)) {
                res.startBlock('<property name="indexedTypes">');
                res.startBlock('<list>');

                if ($generatorCommon.isDefinedAndNotEmpty(domain.keyType))
                    res.line('<value>' + $generatorCommon.JavaTypes.fullClassName(domain.keyType) + '</value>');
                else
                    res.line('<value>???</value>');

                if ($generatorCommon.isDefinedAndNotEmpty(domain.valueType))
                    res.line('<value>' + $generatorCommon.JavaTypes.fullClassName(domain.valueType) + '</value>');
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

        default:
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

    res.softEmptyLine();

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

    const domainConfigs = _.filter(domains, function(domain) {
        return $generatorCommon.domainQueryMetadata(domain) === 'Configuration' &&
            $generatorCommon.isDefinedAndNotEmpty(domain.fields);
    });

    if ($generatorCommon.isDefinedAndNotEmpty(domainConfigs)) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="queryEntities">');
        res.startBlock('<list>');

        _.forEach(domainConfigs, function(domain) {
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
    $generatorXml.cacheQuery(cache, cache.domains, res);
    $generatorXml.cacheStore(cache, cache.domains, res);

    const igfs = _.get(cache, 'nodeFilter.IGFS.instance');

    $generatorXml.cacheNodeFilter(cache, igfs ? [igfs] : [], res);
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

    if ($generatorCommon.isDefinedAndNotEmpty(caches) || (isSrvCfg && $generatorCommon.isDefinedAndNotEmpty(igfss))) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="cacheConfiguration">');
        res.startBlock('<list>');

        _.forEach(caches, function(cache) {
            $generatorXml.cache(cache, res);

            res.needEmptyLine = true;
        });

        if (isSrvCfg) {
            _.forEach(igfss, (igfs) => {
                $generatorXml.cache($generatorCommon.igfsDataCache(igfs), res);

                res.needEmptyLine = true;

                $generatorXml.cache($generatorCommon.igfsMetaCache(igfs), res);

                res.needEmptyLine = true;
            });
        }

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

    if ($generatorCommon.isDefinedAndNotEmpty(igfss)) {
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
        $generatorXml.property(res, igfs, 'fragmentizerConcurrentFiles', null, 0);
        $generatorXml.property(res, igfs, 'fragmentizerThrottlingBlockLength', null, 16777216);
        $generatorXml.property(res, igfs, 'fragmentizerThrottlingDelay', null, 200);

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

    $generatorXml.property(res, igfs, 'dualModeMaxPendingPutsSize', null, 0);

    if ($generatorCommon.isDefinedAndNotEmpty(igfs.dualModePutExecutorService)) {
        res.startBlock('<property name="dualModePutExecutorService">');
        res.line('<bean class="' + igfs.dualModePutExecutorService + '"/>');
        res.endBlock('</property>');
    }

    $generatorXml.property(res, igfs, 'dualModePutExecutorServiceShutdown', null, false);

    res.needEmptyLine = true;

    return res;
};

$generatorXml.igfsSecondFS = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.secondaryFileSystemEnabled) {
        const secondFs = igfs.secondaryFileSystem || {};

        res.startBlock('<property name="secondaryFileSystem">');

        res.startBlock('<bean class="org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem">');

        const nameDefined = $generatorCommon.isDefinedAndNotEmpty(secondFs.userName);
        const cfgDefined = $generatorCommon.isDefinedAndNotEmpty(secondFs.cfgPath);

        $generatorXml.constructorArg(res, 0, secondFs, 'uri');

        if (cfgDefined || nameDefined)
            $generatorXml.constructorArg(res, 1, secondFs, 'cfgPath');

        $generatorXml.constructorArg(res, 2, secondFs, 'userName', null, true);

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

    if ($generatorCommon.isDefinedAndNotEmpty(igfs.name)) {
        igfs.dataCacheName = $generatorCommon.igfsDataCache(igfs).name;
        igfs.metaCacheName = $generatorCommon.igfsMetaCache(igfs).name;

        $generatorXml.property(res, igfs, 'name');
        $generatorXml.property(res, igfs, 'dataCacheName');
        $generatorXml.property(res, igfs, 'metaCacheName');
        $generatorXml.property(res, igfs, 'defaultMode', null, 'DUAL_ASYNC');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS misc configuration.
$generatorXml.igfsMisc = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorXml.property(res, igfs, 'blockSize', null, 65536);
    $generatorXml.property(res, igfs, 'streamBufferSize', null, 65536);
    $generatorXml.property(res, igfs, 'maxSpaceSize', null, 0);
    $generatorXml.property(res, igfs, 'maximumTaskRangeLength', null, 0);
    $generatorXml.property(res, igfs, 'managementPort', null, 11400);
    $generatorXml.property(res, igfs, 'perNodeBatchSize', null, 100);
    $generatorXml.property(res, igfs, 'perNodeParallelBatchCount', null, 8);
    $generatorXml.property(res, igfs, 'prefetchBlocks', null, 0);
    $generatorXml.property(res, igfs, 'sequentialReadsBeforePrefetch', null, 0);
    $generatorXml.property(res, igfs, 'trashPurgeTimeout', null, 1000);
    $generatorXml.property(res, igfs, 'colocateMetadata', null, true);
    $generatorXml.property(res, igfs, 'relaxedConsistency', null, true);

    res.softEmptyLine();

    if (igfs.pathModes && igfs.pathModes.length > 0) {
        res.startBlock('<property name="pathModes">');
        res.startBlock('<map>');

        _.forEach(igfs.pathModes, function(pair) {
            res.line('<entry key="' + pair.path + '" value="' + pair.mode + '"/>');
        });

        res.endBlock('</map>');
        res.endBlock('</property>');
    }

    return res;
};

// Generate DataSource beans.
$generatorXml.generateDataSources = function(datasources, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (datasources.length > 0) {
        res.line('<!-- Data source beans will be initialized from external properties file. -->');

        _.forEach(datasources, (datasource) => $generatorXml.generateDataSource(datasource, res));

        res.needEmptyLine = true;

        res.emptyLineIfNeeded();
    }

    return res;
};

$generatorXml.generateDataSource = function(datasource, res) {
    const beanId = datasource.dataSourceBean;

    res.startBlock('<bean id="' + beanId + '" class="' + $generatorCommon.DATA_SOURCES[datasource.dialect] + '">');

    switch (datasource.dialect) {
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
};

$generatorXml.clusterConfiguration = function(cluster, clientNearCfg, res) {
    const isSrvCfg = _.isNil(clientNearCfg);

    if (!isSrvCfg) {
        res.line('<property name="clientMode" value="true"/>');

        res.needEmptyLine = true;
    }

    $generatorXml.clusterGeneral(cluster, res);

    $generatorXml.clusterAtomics(cluster.atomicConfiguration, res);

    $generatorXml.clusterBinary(cluster.binaryConfiguration, res);

    $generatorXml.clusterCacheKeyConfiguration(cluster.cacheKeyConfiguration, res);

    $generatorXml.clusterCollision(cluster.collision, res);

    $generatorXml.clusterCommunication(cluster, res);

    $generatorXml.clusterConnector(cluster.connector, res);

    $generatorXml.clusterDeployment(cluster, res);

    $generatorXml.clusterEvents(cluster, res);

    $generatorXml.clusterFailover(cluster, res);

    $generatorXml.clusterLogger(cluster.logger, res);

    $generatorXml.clusterODBC(cluster.odbc, res);

    $generatorXml.clusterMarshaller(cluster, res);

    $generatorXml.clusterMetrics(cluster, res);

    $generatorXml.clusterSwap(cluster, res);

    $generatorXml.clusterTime(cluster, res);

    $generatorXml.clusterPools(cluster, res);

    $generatorXml.clusterTransactions(cluster.transactionConfiguration, res);

    $generatorXml.clusterCaches(cluster.caches, cluster.igfss, isSrvCfg, res);

    $generatorXml.clusterSsl(cluster, res);

    if (isSrvCfg)
        $generatorXml.igfss(cluster.igfss, res);

    $generatorXml.clusterUserAttributes(cluster, res);

    return res;
};

$generatorXml.cluster = function(cluster, clientNearCfg) {
    if (cluster) {
        const res = $generatorCommon.builder(1);

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
        let xml = '<?xml version="1.0" encoding="UTF-8"?>\n\n';

        xml += '<!-- ' + $generatorCommon.mainComment('configuration') + ' -->\n\n';
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

export default $generatorXml;
