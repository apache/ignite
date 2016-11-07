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
const $generatorSpring = {};

// Do XML escape.
$generatorSpring.escape = function(s) {
    if (typeof (s) !== 'string')
        return s;

    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
};

// Add constructor argument
$generatorSpring.constructorArg = function(res, ix, obj, propName, dflt, opt) {
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
$generatorSpring.element = function(res, tag, attr1, val1, attr2, val2) {
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
$generatorSpring.property = function(res, obj, propName, setterName, dflt) {
    if (!_.isNil(obj)) {
        const val = obj[propName];

        if ($generatorCommon.isDefinedAndNotEmpty(val)) {
            const missDflt = _.isNil(dflt);

            // Add to result if no default provided or value not equals to default.
            if (missDflt || (!missDflt && val !== dflt)) {
                $generatorSpring.element(res, 'property', 'name', setterName ? setterName : propName, 'value', $generatorSpring.escape(val));

                return true;
            }
        }
    }

    return false;
};

// Add property for class name.
$generatorSpring.classNameProperty = function(res, obj, propName) {
    const val = obj[propName];

    if (!_.isNil(val))
        $generatorSpring.element(res, 'property', 'name', propName, 'value', $generatorCommon.JavaTypes.fullClassName(val));
};

// Add list property.
$generatorSpring.listProperty = function(res, obj, propName, listType, rowFactory) {
    const val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        if (!listType)
            listType = 'list';

        if (!rowFactory)
            rowFactory = (v) => '<value>' + $generatorSpring.escape(v) + '</value>';

        res.startBlock('<property name="' + propName + '">');
        res.startBlock('<' + listType + '>');

        _.forEach(val, (v) => res.line(rowFactory(v)));

        res.endBlock('</' + listType + '>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Add array property
$generatorSpring.arrayProperty = function(res, obj, propName, descr, rowFactory) {
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
$generatorSpring.beanProperty = function(res, bean, beanPropName, desc, createBeanAlthoughNoProps) {
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
                            $generatorSpring.listProperty(res, bean, propName, descr.setterName);

                            break;

                        case 'array':
                            $generatorSpring.arrayProperty(res, bean, propName, descr);

                            break;

                        case 'propertiesAsList':
                            const val = bean[propName];

                            if (val && val.length > 0) {
                                res.startBlock('<property name="' + propName + '">');
                                res.startBlock('<props>');

                                _.forEach(val, function(nameAndValue) {
                                    const eqIndex = nameAndValue.indexOf('=');
                                    if (eqIndex >= 0) {
                                        res.line('<prop key="' + $generatorSpring.escape(nameAndValue.substring(0, eqIndex)) + '">' +
                                            $generatorSpring.escape(nameAndValue.substr(eqIndex + 1)) + '</prop>');
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
                            if ($generatorSpring.property(res, bean, propName, descr.setterName, descr.dflt))
                                hasData = true;
                    }
                }
                else
                    if ($generatorSpring.property(res, bean, propName))
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
$generatorSpring.simpleBeanProperty = function(res, obj, propName) {
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
$generatorSpring.evictionPolicy = function(res, evtPlc, propName) {
    if (evtPlc && evtPlc.kind) {
        $generatorSpring.beanProperty(res, evtPlc[evtPlc.kind.toUpperCase()], propName,
            $generatorCommon.EVICTION_POLICIES[evtPlc.kind], true);
    }
};

// Generate discovery.
$generatorSpring.clusterGeneral = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cluster, 'name', 'gridName');
    $generatorSpring.property(res, cluster, 'localHost');

    if (cluster.discovery) {
        res.startBlock('<property name="discoverySpi">');
        res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">');
        res.startBlock('<property name="ipFinder">');

        const d = cluster.discovery;

        switch (d.kind) {
            case 'Multicast':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder">');

                if (d.Multicast) {
                    $generatorSpring.property(res, d.Multicast, 'multicastGroup');
                    $generatorSpring.property(res, d.Multicast, 'multicastPort');
                    $generatorSpring.property(res, d.Multicast, 'responseWaitTime');
                    $generatorSpring.property(res, d.Multicast, 'addressRequestAttempts');
                    $generatorSpring.property(res, d.Multicast, 'localAddress');
                    $generatorSpring.listProperty(res, d.Multicast, 'addresses');
                }

                res.endBlock('</bean>');

                break;

            case 'Vm':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder">');

                if (d.Vm)
                    $generatorSpring.listProperty(res, d.Vm, 'addresses');

                res.endBlock('</bean>');

                break;

            case 'S3':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder">');

                if (d.S3) {
                    if (d.S3.bucketName)
                        res.line('<property name="bucketName" value="' + $generatorSpring.escape(d.S3.bucketName) + '"/>');
                }

                res.endBlock('</bean>');

                break;

            case 'Cloud':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder">');

                if (d.Cloud) {
                    $generatorSpring.property(res, d.Cloud, 'credential');
                    $generatorSpring.property(res, d.Cloud, 'credentialPath');
                    $generatorSpring.property(res, d.Cloud, 'identity');
                    $generatorSpring.property(res, d.Cloud, 'provider');
                    $generatorSpring.listProperty(res, d.Cloud, 'regions');
                    $generatorSpring.listProperty(res, d.Cloud, 'zones');
                }

                res.endBlock('</bean>');

                break;

            case 'GoogleStorage':
                res.startBlock('<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder">');

                if (d.GoogleStorage) {
                    $generatorSpring.property(res, d.GoogleStorage, 'projectName');
                    $generatorSpring.property(res, d.GoogleStorage, 'bucketName');
                    $generatorSpring.property(res, d.GoogleStorage, 'serviceAccountP12FilePath');
                    $generatorSpring.property(res, d.GoogleStorage, 'serviceAccountId');
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

                        if (!_.find(res.datasources, { dataSourceBean: datasource.dataSourceBean })) {
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
                    $generatorSpring.property(res, d.SharedFs, 'path');

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

                    $generatorSpring.property(res, d.ZooKeeper, 'zkConnectionString');

                    if (d.ZooKeeper.retryPolicy && d.ZooKeeper.retryPolicy.kind) {
                        const kind = d.ZooKeeper.retryPolicy.kind;
                        const retryPolicy = d.ZooKeeper.retryPolicy[kind];
                        const customClassDefined = retryPolicy && $generatorCommon.isDefinedAndNotEmpty(retryPolicy.className);

                        if (kind !== 'Custom' || customClassDefined)
                            res.startBlock('<property name="retryPolicy">');

                        switch (kind) {
                            case 'ExponentialBackoff':
                                res.startBlock('<bean class="org.apache.curator.retry.ExponentialBackoffRetry">');
                                $generatorSpring.constructorArg(res, 0, retryPolicy, 'baseSleepTimeMs', 1000);
                                $generatorSpring.constructorArg(res, 1, retryPolicy, 'maxRetries', 10);
                                $generatorSpring.constructorArg(res, 2, retryPolicy, 'maxSleepMs', null, true);
                                res.endBlock('</bean>');

                                break;

                            case 'BoundedExponentialBackoff':
                                res.startBlock('<bean class="org.apache.curator.retry.BoundedExponentialBackoffRetry">');
                                $generatorSpring.constructorArg(res, 0, retryPolicy, 'baseSleepTimeMs', 1000);
                                $generatorSpring.constructorArg(res, 1, retryPolicy, 'maxSleepTimeMs', 2147483647);
                                $generatorSpring.constructorArg(res, 2, retryPolicy, 'maxRetries', 10);
                                res.endBlock('</bean>');

                                break;

                            case 'UntilElapsed':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryUntilElapsed">');
                                $generatorSpring.constructorArg(res, 0, retryPolicy, 'maxElapsedTimeMs', 60000);
                                $generatorSpring.constructorArg(res, 1, retryPolicy, 'sleepMsBetweenRetries', 1000);
                                res.endBlock('</bean>');

                                break;

                            case 'NTimes':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryNTimes">');
                                $generatorSpring.constructorArg(res, 0, retryPolicy, 'n', 10);
                                $generatorSpring.constructorArg(res, 1, retryPolicy, 'sleepMsBetweenRetries', 1000);
                                res.endBlock('</bean>');

                                break;

                            case 'OneTime':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryOneTime">');
                                $generatorSpring.constructorArg(res, 0, retryPolicy, 'sleepMsBetweenRetry', 1000);
                                res.endBlock('</bean>');

                                break;

                            case 'Forever':
                                res.startBlock('<bean class="org.apache.curator.retry.RetryForever">');
                                $generatorSpring.constructorArg(res, 0, retryPolicy, 'retryIntervalMs', 1000);
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

                    $generatorSpring.property(res, d.ZooKeeper, 'basePath', null, '/services');
                    $generatorSpring.property(res, d.ZooKeeper, 'serviceName', null, 'ignite');
                    $generatorSpring.property(res, d.ZooKeeper, 'allowDuplicateRegistrations', null, false);
                }

                res.endBlock('</bean>');

                break;

            default:
                res.line('Unknown discovery kind: ' + d.kind);
        }

        res.endBlock('</property>');

        $generatorSpring.clusterDiscovery(d, res);

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate atomics group.
$generatorSpring.clusterAtomics = function(atomics, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.hasAtLeastOneProperty(atomics, ['cacheMode', 'atomicSequenceReserveSize', 'backups'])) {
        res.startSafeBlock();

        res.emptyLineIfNeeded();

        res.startBlock('<property name="atomicConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.AtomicConfiguration">');

        const cacheMode = atomics.cacheMode ? atomics.cacheMode : 'PARTITIONED';

        let hasData = cacheMode !== 'PARTITIONED';

        $generatorSpring.property(res, atomics, 'cacheMode', null, 'PARTITIONED');

        hasData = $generatorSpring.property(res, atomics, 'atomicSequenceReserveSize', null, 1000) || hasData;

        if (cacheMode === 'PARTITIONED')
            hasData = $generatorSpring.property(res, atomics, 'backups', null, 0) || hasData;

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;

        if (!hasData)
            res.rollbackSafeBlock();
    }

    return res;
};

// Generate binary group.
$generatorSpring.clusterBinary = function(binary, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.binaryIsDefined(binary)) {
        res.startBlock('<property name="binaryConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.BinaryConfiguration">');

        $generatorSpring.simpleBeanProperty(res, binary, 'idMapper');
        $generatorSpring.simpleBeanProperty(res, binary, 'nameMapper');
        $generatorSpring.simpleBeanProperty(res, binary, 'serializer');

        if ($generatorCommon.isDefinedAndNotEmpty(binary.typeConfigurations)) {
            res.startBlock('<property name="typeConfigurations">');
            res.startBlock('<list>');

            _.forEach(binary.typeConfigurations, function(type) {
                res.startBlock('<bean class="org.apache.ignite.binary.BinaryTypeConfiguration">');

                $generatorSpring.property(res, type, 'typeName');
                $generatorSpring.simpleBeanProperty(res, type, 'idMapper');
                $generatorSpring.simpleBeanProperty(res, type, 'nameMapper');
                $generatorSpring.simpleBeanProperty(res, type, 'serializer');
                $generatorSpring.property(res, type, 'enum', null, false);

                res.endBlock('</bean>');
            });

            res.endBlock('</list>');
            res.endBlock('</property>');
        }

        $generatorSpring.property(res, binary, 'compactFooter', null, true);

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache key configurations.
$generatorSpring.clusterCacheKeyConfiguration = function(keyCfgs, res) {
    if (!res)
        res = $generatorCommon.builder();

    keyCfgs = _.filter(keyCfgs, (cfg) => cfg.typeName && cfg.affinityKeyFieldName);

    if (_.isEmpty(keyCfgs))
        return res;

    res.startBlock('<property name="cacheKeyConfiguration">');
    res.startBlock('<array>');

    _.forEach(keyCfgs, (cfg) => {
        res.startBlock('<bean class="org.apache.ignite.cache.CacheKeyConfiguration">');

        $generatorSpring.constructorArg(res, -1, cfg, 'typeName');
        $generatorSpring.constructorArg(res, -1, cfg, 'affinityKeyFieldName');

        res.endBlock('</bean>');
    });

    res.endBlock('</array>');
    res.endBlock('</property>');

    return res;
};

// Generate collision group.
$generatorSpring.clusterCollision = function(collision, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (collision && collision.kind && collision.kind !== 'Noop') {
        const spi = collision[collision.kind];

        if (collision.kind !== 'Custom' || (spi && $generatorCommon.isDefinedAndNotEmpty(spi.class))) {
            res.startBlock('<property name="collisionSpi">');

            switch (collision.kind) {
                case 'JobStealing':
                    res.startBlock('<bean class="org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi">');
                    $generatorSpring.property(res, spi, 'activeJobsThreshold', null, 95);
                    $generatorSpring.property(res, spi, 'waitJobsThreshold', null, 0);
                    $generatorSpring.property(res, spi, 'messageExpireTime', null, 1000);
                    $generatorSpring.property(res, spi, 'maximumStealingAttempts', null, 5);
                    $generatorSpring.property(res, spi, 'stealingEnabled', null, true);

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
                            $generatorSpring.element(res, 'entry', 'key', attr.name, 'value', attr.value);
                        });

                        res.endBlock('</map>');
                        res.endBlock('</property>');
                    }

                    res.endBlock('</bean>');

                    break;

                case 'FifoQueue':
                    res.startBlock('<bean class="org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi">');
                    $generatorSpring.property(res, spi, 'parallelJobsNumber');
                    $generatorSpring.property(res, spi, 'waitingJobsNumber');
                    res.endBlock('</bean>');

                    break;

                case 'PriorityQueue':
                    res.startBlock('<bean class="org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi">');
                    $generatorSpring.property(res, spi, 'parallelJobsNumber');
                    $generatorSpring.property(res, spi, 'waitingJobsNumber');
                    $generatorSpring.property(res, spi, 'priorityAttributeKey', null, 'grid.task.priority');
                    $generatorSpring.property(res, spi, 'jobPriorityAttributeKey', null, 'grid.job.priority');
                    $generatorSpring.property(res, spi, 'defaultPriority', null, 0);
                    $generatorSpring.property(res, spi, 'starvationIncrement', null, 1);
                    $generatorSpring.property(res, spi, 'starvationPreventionEnabled', null, true);
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
$generatorSpring.clusterCommunication = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.beanProperty(res, cluster.communication, 'communicationSpi', $generatorCommon.COMMUNICATION_CONFIGURATION);

    $generatorSpring.property(res, cluster, 'networkTimeout', null, 5000);
    $generatorSpring.property(res, cluster, 'networkSendRetryDelay', null, 1000);
    $generatorSpring.property(res, cluster, 'networkSendRetryCount', null, 3);
    $generatorSpring.property(res, cluster, 'segmentCheckFrequency');
    $generatorSpring.property(res, cluster, 'waitForSegmentOnStart', null, false);
    $generatorSpring.property(res, cluster, 'discoveryStartupDelay', null, 60000);

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
$generatorSpring.clusterConnector = function(connector, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!_.isNil(connector) && connector.enabled) {
        const cfg = _.cloneDeep($generatorCommon.CONNECTOR_CONFIGURATION);

        if (connector.sslEnabled) {
            cfg.fields.sslClientAuth = {dflt: false};
            cfg.fields.sslFactory = {type: 'bean'};
        }

        $generatorSpring.beanProperty(res, connector, 'connectorConfiguration', cfg, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate deployment group.
$generatorSpring.clusterDeployment = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorSpring.property(res, cluster, 'deploymentMode', null, 'SHARED'))
        res.needEmptyLine = true;

    const p2pEnabled = cluster.peerClassLoadingEnabled;

    if (!_.isNil(p2pEnabled)) {
        $generatorSpring.property(res, cluster, 'peerClassLoadingEnabled', null, false);

        if (p2pEnabled) {
            $generatorSpring.property(res, cluster, 'peerClassLoadingMissedResourcesCacheSize', null, 100);
            $generatorSpring.property(res, cluster, 'peerClassLoadingThreadPoolSize', null, 2);
            $generatorSpring.listProperty(res, cluster, 'peerClassLoadingLocalClassPathExclude');
        }

        res.needEmptyLine = true;
    }

    return res;
};

// Generate discovery group.
$generatorSpring.clusterDiscovery = function(disco, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (disco) {
        $generatorSpring.property(res, disco, 'localAddress');
        $generatorSpring.property(res, disco, 'localPort', null, 47500);
        $generatorSpring.property(res, disco, 'localPortRange', null, 100);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.addressResolver))
            $generatorSpring.beanProperty(res, disco, 'addressResolver', {className: disco.addressResolver}, true);
        $generatorSpring.property(res, disco, 'socketTimeout', null, 5000);
        $generatorSpring.property(res, disco, 'ackTimeout', null, 5000);
        $generatorSpring.property(res, disco, 'maxAckTimeout', null, 600000);
        $generatorSpring.property(res, disco, 'networkTimeout', null, 5000);
        $generatorSpring.property(res, disco, 'joinTimeout', null, 0);
        $generatorSpring.property(res, disco, 'threadPriority', null, 10);
        $generatorSpring.property(res, disco, 'heartbeatFrequency', null, 2000);
        $generatorSpring.property(res, disco, 'maxMissedHeartbeats', null, 1);
        $generatorSpring.property(res, disco, 'maxMissedClientHeartbeats', null, 5);
        $generatorSpring.property(res, disco, 'topHistorySize', null, 1000);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.listener))
            $generatorSpring.beanProperty(res, disco, 'listener', {className: disco.listener}, true);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.dataExchange))
            $generatorSpring.beanProperty(res, disco, 'dataExchange', {className: disco.dataExchange}, true);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.metricsProvider))
            $generatorSpring.beanProperty(res, disco, 'metricsProvider', {className: disco.metricsProvider}, true);
        $generatorSpring.property(res, disco, 'reconnectCount', null, 10);
        $generatorSpring.property(res, disco, 'statisticsPrintFrequency', null, 0);
        $generatorSpring.property(res, disco, 'ipFinderCleanFrequency', null, 60000);
        if ($generatorCommon.isDefinedAndNotEmpty(disco.authenticator))
            $generatorSpring.beanProperty(res, disco, 'authenticator', {className: disco.authenticator}, true);
        $generatorSpring.property(res, disco, 'forceServerMode', null, false);
        $generatorSpring.property(res, disco, 'clientReconnectDisabled', null, false);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate events group.
$generatorSpring.clusterEvents = function(cluster, res) {
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
$generatorSpring.clusterFailover = function(cluster, res) {
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

                    $generatorSpring.property(res, spi[spi.kind], 'maximumFailoverAttempts', null, 5);

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
$generatorSpring.clusterLogger = function(logger, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.loggerConfigured(logger)) {
        res.startBlock('<property name="gridLogger">');

        const log = logger[logger.kind];

        switch (logger.kind) {
            case 'Log4j2':
                res.startBlock('<bean class="org.apache.ignite.logger.log4j2.Log4J2Logger">');
                res.line('<constructor-arg value="' + $generatorSpring.escape(log.path) + '"/>');
                $generatorSpring.property(res, log, 'level');
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
                        res.line('<constructor-arg value="' + $generatorSpring.escape(log.path) + '"/>');

                    $generatorSpring.property(res, log, 'level');
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
$generatorSpring.clusterMarshaller = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    const marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind)
        $generatorSpring.beanProperty(res, marshaller[marshaller.kind], 'marshaller', $generatorCommon.MARSHALLERS[marshaller.kind], true);

    res.softEmptyLine();

    $generatorSpring.property(res, cluster, 'marshalLocalJobs', null, false);
    $generatorSpring.property(res, cluster, 'marshallerCacheKeepAliveTime', null, 10000);
    $generatorSpring.property(res, cluster, 'marshallerCacheThreadPoolSize', 'marshallerCachePoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorSpring.clusterMetrics = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cluster, 'metricsExpireTime');
    $generatorSpring.property(res, cluster, 'metricsHistorySize', null, 10000);
    $generatorSpring.property(res, cluster, 'metricsLogFrequency', null, 60000);
    $generatorSpring.property(res, cluster, 'metricsUpdateFrequency', null, 2000);

    res.needEmptyLine = true;

    return res;
};

// Generate swap group.
$generatorSpring.clusterSwap = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind === 'FileSwapSpaceSpi') {
        $generatorSpring.beanProperty(res, cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi',
            $generatorCommon.SWAP_SPACE_SPI, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate time group.
$generatorSpring.clusterTime = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cluster, 'clockSyncSamples', null, 8);
    $generatorSpring.property(res, cluster, 'clockSyncFrequency', null, 120000);
    $generatorSpring.property(res, cluster, 'timeServerPortBase', null, 31100);
    $generatorSpring.property(res, cluster, 'timeServerPortRange', null, 100);

    res.needEmptyLine = true;

    return res;
};

// Generate OBC configuration group.
$generatorSpring.clusterODBC = function(odbc, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (odbc && odbc.odbcEnabled)
        $generatorSpring.beanProperty(res, odbc, 'odbcConfiguration', $generatorCommon.ODBC_CONFIGURATION, true);

    res.needEmptyLine = true;

    return res;
};

// Generate thread pools group.
$generatorSpring.clusterPools = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cluster, 'publicThreadPoolSize');
    $generatorSpring.property(res, cluster, 'systemThreadPoolSize');
    $generatorSpring.property(res, cluster, 'managementThreadPoolSize');
    $generatorSpring.property(res, cluster, 'igfsThreadPoolSize');
    $generatorSpring.property(res, cluster, 'rebalanceThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate transactions group.
$generatorSpring.clusterTransactions = function(transactionConfiguration, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.beanProperty(res, transactionConfiguration, 'transactionConfiguration', $generatorCommon.TRANSACTION_CONFIGURATION, false);

    res.needEmptyLine = true;

    return res;
};

// Generate user attributes group.
$generatorSpring.clusterUserAttributes = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.isDefinedAndNotEmpty(cluster.attributes)) {
        res.startBlock('<property name="userAttributes">');
        res.startBlock('<map>');

        _.forEach(cluster.attributes, function(attr) {
            $generatorSpring.element(res, 'entry', 'key', attr.name, 'value', attr.value);
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
$generatorSpring.clusterSsl = function(cluster, res) {
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

        $generatorSpring.beanProperty(res, sslFactory, 'sslContextFactory', propsDesc, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache general group.
$generatorSpring.cacheGeneral = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cache, 'name');

    $generatorSpring.property(res, cache, 'cacheMode');
    $generatorSpring.property(res, cache, 'atomicityMode');

    if (cache.cacheMode === 'PARTITIONED' && $generatorSpring.property(res, cache, 'backups'))
        $generatorSpring.property(res, cache, 'readFromBackup');

    $generatorSpring.property(res, cache, 'copyOnRead');

    if (cache.cacheMode === 'PARTITIONED' && cache.atomicityMode === 'TRANSACTIONAL')
        $generatorSpring.property(res, cache, 'invalidate');

    res.needEmptyLine = true;

    return res;
};

// Generate cache memory group.
$generatorSpring.cacheMemory = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cache, 'memoryMode', null, 'ONHEAP_TIERED');

    if (cache.memoryMode !== 'OFFHEAP_VALUES')
        $generatorSpring.property(res, cache, 'offHeapMaxMemory', null, -1);

    res.softEmptyLine();

    $generatorSpring.evictionPolicy(res, cache.evictionPolicy, 'evictionPolicy');

    res.softEmptyLine();

    $generatorSpring.property(res, cache, 'startSize', null, 1500000);
    $generatorSpring.property(res, cache, 'swapEnabled', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate cache query & indexing group.
$generatorSpring.cacheQuery = function(cache, domains, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cache, 'sqlSchema');
    $generatorSpring.property(res, cache, 'sqlOnheapRowCacheSize', null, 10240);
    $generatorSpring.property(res, cache, 'longQueryWarningTimeout', null, 3000);

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

    $generatorSpring.listProperty(res, cache, 'sqlFunctionClasses');

    res.softEmptyLine();

    $generatorSpring.property(res, cache, 'snapshotableIndex', null, false);
    $generatorSpring.property(res, cache, 'sqlEscapeAll', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate cache store group.
$generatorSpring.cacheStore = function(cache, domains, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        const factoryKind = cache.cacheStoreFactory.kind;

        const storeFactory = cache.cacheStoreFactory[factoryKind];

        if (storeFactory) {
            if (factoryKind === 'CacheJdbcPojoStoreFactory') {
                res.startBlock('<property name="cacheStoreFactory">');
                res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory">');

                $generatorSpring.property(res, storeFactory, 'dataSourceBean');

                res.startBlock('<property name="dialect">');
                res.line('<bean class="' + $generatorCommon.jdbcDialectClassName(storeFactory.dialect) + '"/>');
                res.endBlock('</property>');

                if (storeFactory.sqlEscapeAll)
                    $generatorSpring.property(res, storeFactory, 'sqlEscapeAll');

                const domainConfigs = _.filter(domains, function(domain) {
                    return $generatorCommon.isDefinedAndNotEmpty(domain.databaseTable);
                });

                if ($generatorCommon.isDefinedAndNotEmpty(domainConfigs)) {
                    res.startBlock('<property name="types">');
                    res.startBlock('<list>');

                    _.forEach(domainConfigs, function(domain) {
                        res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.JdbcType">');

                        $generatorSpring.property(res, cache, 'name', 'cacheName');

                        $generatorSpring.classNameProperty(res, domain, 'keyType');
                        $generatorSpring.property(res, domain, 'valueType');

                        $generatorSpring.domainStore(domain, res);

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
                    $generatorSpring.property(res, storeFactory, 'dataSourceBean');
                else {
                    $generatorSpring.property(res, storeFactory, 'connectionUrl');
                    $generatorSpring.property(res, storeFactory, 'user');
                    res.line('<property name="password" value="${ds.' + storeFactory.user + '.password}"/>');
                }

                $generatorSpring.property(res, storeFactory, 'initSchema');
                $generatorSpring.property(res, storeFactory, 'createTableQuery');
                $generatorSpring.property(res, storeFactory, 'loadQuery');
                $generatorSpring.property(res, storeFactory, 'insertQuery');
                $generatorSpring.property(res, storeFactory, 'updateQuery');
                $generatorSpring.property(res, storeFactory, 'deleteQuery');

                res.endBlock('</bean>');
                res.endBlock('</property>');
            }
            else
                $generatorSpring.beanProperty(res, storeFactory, 'cacheStoreFactory', $generatorCommon.STORE_FACTORIES[factoryKind], true);

            if (storeFactory.dataSourceBean && (storeFactory.connectVia ? (storeFactory.connectVia === 'DataSource' ? storeFactory.dialect : null) : storeFactory.dialect)) {
                if (!_.find(res.datasources, { dataSourceBean: storeFactory.dataSourceBean})) {
                    res.datasources.push({
                        dataSourceBean: storeFactory.dataSourceBean,
                        dialect: storeFactory.dialect
                    });
                }
            }
        }
    }

    res.softEmptyLine();

    $generatorSpring.property(res, cache, 'storeKeepBinary', null, false);
    $generatorSpring.property(res, cache, 'loadPreviousValue', null, false);
    $generatorSpring.property(res, cache, 'readThrough', null, false);
    $generatorSpring.property(res, cache, 'writeThrough', null, false);

    res.softEmptyLine();

    if (cache.writeBehindEnabled) {
        $generatorSpring.property(res, cache, 'writeBehindEnabled', null, false);
        $generatorSpring.property(res, cache, 'writeBehindBatchSize', null, 512);
        $generatorSpring.property(res, cache, 'writeBehindFlushSize', null, 10240);
        $generatorSpring.property(res, cache, 'writeBehindFlushFrequency', null, 5000);
        $generatorSpring.property(res, cache, 'writeBehindFlushThreadCount', null, 1);
    }

    res.needEmptyLine = true;

    return res;
};

// Generate cache node filter group.
$generatorSpring.cacheNodeFilter = function(cache, igfss, res) {
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
$generatorSpring.cacheConcurrency = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cache, 'maxConcurrentAsyncOperations', null, 500);
    $generatorSpring.property(res, cache, 'defaultLockTimeout', null, 0);
    $generatorSpring.property(res, cache, 'atomicWriteOrderMode');
    $generatorSpring.property(res, cache, 'writeSynchronizationMode', null, 'PRIMARY_SYNC');

    res.needEmptyLine = true;

    return res;
};

// Generate cache rebalance group.
$generatorSpring.cacheRebalance = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode !== 'LOCAL') {
        $generatorSpring.property(res, cache, 'rebalanceMode', null, 'ASYNC');
        $generatorSpring.property(res, cache, 'rebalanceThreadPoolSize', null, 1);
        $generatorSpring.property(res, cache, 'rebalanceBatchSize', null, 524288);
        $generatorSpring.property(res, cache, 'rebalanceBatchesPrefetchCount', null, 2);
        $generatorSpring.property(res, cache, 'rebalanceOrder', null, 0);
        $generatorSpring.property(res, cache, 'rebalanceDelay', null, 0);
        $generatorSpring.property(res, cache, 'rebalanceTimeout', null, 10000);
        $generatorSpring.property(res, cache, 'rebalanceThrottle', null, 0);
    }

    res.softEmptyLine();

    if (cache.igfsAffinnityGroupSize) {
        res.startBlock('<property name="affinityMapper">');
        res.startBlock('<bean class="org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper">');
        $generatorSpring.constructorArg(res, -1, cache, 'igfsAffinnityGroupSize');
        res.endBlock('</bean>');
        res.endBlock('</property>');
    }

    return res;
};

// Generate cache server near cache group.
$generatorSpring.cacheServerNearCache = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode === 'PARTITIONED' && cache.nearCacheEnabled) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="nearConfiguration">');
        res.startBlock('<bean class="org.apache.ignite.configuration.NearCacheConfiguration">');

        if (cache.nearConfiguration) {
            if (cache.nearConfiguration.nearStartSize)
                $generatorSpring.property(res, cache.nearConfiguration, 'nearStartSize', null, 375000);

            $generatorSpring.evictionPolicy(res, cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');
        }

        res.endBlock('</bean>');
        res.endBlock('</property>');
    }

    res.needEmptyLine = true;

    return res;
};

// Generate cache statistics group.
$generatorSpring.cacheStatistics = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, cache, 'statisticsEnabled', null, false);
    $generatorSpring.property(res, cache, 'managementEnabled', null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate domain model query fields.
$generatorSpring.domainModelQueryFields = function(res, domain) {
    const fields = domain.fields;

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="fields">');
        res.startBlock('<map>');

        _.forEach(fields, function(field) {
            $generatorSpring.element(res, 'entry', 'key', field.name, 'value', $generatorCommon.JavaTypes.fullClassName(field.className));
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model query fields.
$generatorSpring.domainModelQueryAliases = function(res, domain) {
    const aliases = domain.aliases;

    if (aliases && aliases.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="aliases">');
        res.startBlock('<map>');

        _.forEach(aliases, function(alias) {
            $generatorSpring.element(res, 'entry', 'key', alias.field, 'value', alias.alias);
        });

        res.endBlock('</map>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model indexes.
$generatorSpring.domainModelQueryIndexes = function(res, domain) {
    const indexes = domain.indexes;

    if (indexes && indexes.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="indexes">');
        res.startBlock('<list>');

        _.forEach(indexes, function(index) {
            res.startBlock('<bean class="org.apache.ignite.cache.QueryIndex">');

            $generatorSpring.property(res, index, 'name');
            $generatorSpring.property(res, index, 'indexType');

            const fields = index.fields;

            if (fields && fields.length > 0) {
                res.startBlock('<property name="fields">');
                res.startBlock('<map>');

                _.forEach(fields, function(field) {
                    $generatorSpring.element(res, 'entry', 'key', field.name, 'value', field.direction);
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
$generatorSpring.domainModelDatabaseFields = function(res, domain, fieldProp) {
    const fields = domain[fieldProp];

    if (fields && fields.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="' + fieldProp + '">');

        res.startBlock('<list>');

        _.forEach(fields, function(field) {
            res.startBlock('<bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField">');

            $generatorSpring.property(res, field, 'databaseFieldName');

            res.startBlock('<property name="databaseFieldType">');
            res.line('<util:constant static-field="java.sql.Types.' + field.databaseFieldType + '"/>');
            res.endBlock('</property>');

            $generatorSpring.property(res, field, 'javaFieldName');

            $generatorSpring.classNameProperty(res, field, 'javaFieldType');

            res.endBlock('</bean>');
        });

        res.endBlock('</list>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }
};

// Generate domain model general group.
$generatorSpring.domainModelGeneral = function(domain, res) {
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
            $generatorSpring.classNameProperty(res, domain, 'keyType');
            $generatorSpring.property(res, domain, 'valueType');

            break;

        default:
    }

    res.needEmptyLine = true;

    return res;
};

// Generate domain model for query group.
$generatorSpring.domainModelQuery = function(domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.domainQueryMetadata(domain) === 'Configuration') {
        $generatorSpring.domainModelQueryFields(res, domain);
        $generatorSpring.domainModelQueryAliases(res, domain);
        $generatorSpring.domainModelQueryIndexes(res, domain);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate domain model for store group.
$generatorSpring.domainStore = function(domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, domain, 'databaseSchema');
    $generatorSpring.property(res, domain, 'databaseTable');

    res.softEmptyLine();

    $generatorSpring.domainModelDatabaseFields(res, domain, 'keyFields');
    $generatorSpring.domainModelDatabaseFields(res, domain, 'valueFields');

    res.needEmptyLine = true;

    return res;
};

$generatorSpring.cacheQueryMetadata = function(domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.cache.QueryEntity">');

    $generatorSpring.classNameProperty(res, domain, 'keyType');
    $generatorSpring.property(res, domain, 'valueType');

    $generatorSpring.domainModelQuery(domain, res);

    res.endBlock('</bean>');

    res.needEmptyLine = true;

    return res;
};

// Generate domain models configs.
$generatorSpring.cacheDomains = function(domains, res) {
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
            $generatorSpring.cacheQueryMetadata(domain, res);
        });

        res.endBlock('</list>');
        res.endBlock('</property>');
    }

    return res;
};

// Generate cache configs.
$generatorSpring.cache = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<bean class="org.apache.ignite.configuration.CacheConfiguration">');

    $generatorSpring.cacheConfiguration(cache, res);

    res.endBlock('</bean>');

    return res;
};

// Generate cache configs.
$generatorSpring.cacheConfiguration = function(cache, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.cacheGeneral(cache, res);
    $generatorSpring.cacheMemory(cache, res);
    $generatorSpring.cacheQuery(cache, cache.domains, res);
    $generatorSpring.cacheStore(cache, cache.domains, res);

    const igfs = _.get(cache, 'nodeFilter.IGFS.instance');

    $generatorSpring.cacheNodeFilter(cache, igfs ? [igfs] : [], res);
    $generatorSpring.cacheConcurrency(cache, res);
    $generatorSpring.cacheRebalance(cache, res);
    $generatorSpring.cacheServerNearCache(cache, res);
    $generatorSpring.cacheStatistics(cache, res);
    $generatorSpring.cacheDomains(cache.domains, res);

    return res;
};

// Generate caches configs.
$generatorSpring.clusterCaches = function(caches, igfss, isSrvCfg, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.isDefinedAndNotEmpty(caches) || (isSrvCfg && $generatorCommon.isDefinedAndNotEmpty(igfss))) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="cacheConfiguration">');
        res.startBlock('<list>');

        _.forEach(caches, function(cache) {
            $generatorSpring.cache(cache, res);

            res.needEmptyLine = true;
        });

        if (isSrvCfg) {
            _.forEach(igfss, (igfs) => {
                $generatorSpring.cache($generatorCommon.igfsDataCache(igfs), res);

                res.needEmptyLine = true;

                $generatorSpring.cache($generatorCommon.igfsMetaCache(igfs), res);

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
$generatorSpring.igfss = function(igfss, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.isDefinedAndNotEmpty(igfss)) {
        res.emptyLineIfNeeded();

        res.startBlock('<property name="fileSystemConfiguration">');
        res.startBlock('<list>');

        _.forEach(igfss, function(igfs) {
            res.startBlock('<bean class="org.apache.ignite.configuration.FileSystemConfiguration">');

            $generatorSpring.igfsGeneral(igfs, res);
            $generatorSpring.igfsIPC(igfs, res);
            $generatorSpring.igfsFragmentizer(igfs, res);
            $generatorSpring.igfsDualMode(igfs, res);
            $generatorSpring.igfsSecondFS(igfs, res);
            $generatorSpring.igfsMisc(igfs, res);

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
$generatorSpring.igfsIPC = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.ipcEndpointEnabled) {
        $generatorSpring.beanProperty(res, igfs.ipcEndpointConfiguration, 'ipcEndpointConfiguration', $generatorCommon.IGFS_IPC_CONFIGURATION, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS fragmentizer configuration.
$generatorSpring.igfsFragmentizer = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.fragmentizerEnabled) {
        $generatorSpring.property(res, igfs, 'fragmentizerConcurrentFiles', null, 0);
        $generatorSpring.property(res, igfs, 'fragmentizerThrottlingBlockLength', null, 16777216);
        $generatorSpring.property(res, igfs, 'fragmentizerThrottlingDelay', null, 200);

        res.needEmptyLine = true;
    }
    else
        $generatorSpring.property(res, igfs, 'fragmentizerEnabled');

    return res;
};

// Generate IGFS dual mode configuration.
$generatorSpring.igfsDualMode = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, igfs, 'dualModeMaxPendingPutsSize', null, 0);

    if ($generatorCommon.isDefinedAndNotEmpty(igfs.dualModePutExecutorService)) {
        res.startBlock('<property name="dualModePutExecutorService">');
        res.line('<bean class="' + igfs.dualModePutExecutorService + '"/>');
        res.endBlock('</property>');
    }

    $generatorSpring.property(res, igfs, 'dualModePutExecutorServiceShutdown', null, false);

    res.needEmptyLine = true;

    return res;
};

$generatorSpring.igfsSecondFS = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.secondaryFileSystemEnabled) {
        const secondFs = igfs.secondaryFileSystem || {};

        res.startBlock('<property name="secondaryFileSystem">');

        res.startBlock('<bean class="org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem">');

        const nameDefined = $generatorCommon.isDefinedAndNotEmpty(secondFs.userName);
        const cfgDefined = $generatorCommon.isDefinedAndNotEmpty(secondFs.cfgPath);

        $generatorSpring.constructorArg(res, 0, secondFs, 'uri');

        if (cfgDefined || nameDefined)
            $generatorSpring.constructorArg(res, 1, secondFs, 'cfgPath');

        $generatorSpring.constructorArg(res, 2, secondFs, 'userName', null, true);

        res.endBlock('</bean>');
        res.endBlock('</property>');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS general configuration.
$generatorSpring.igfsGeneral = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.isDefinedAndNotEmpty(igfs.name)) {
        igfs.dataCacheName = $generatorCommon.igfsDataCache(igfs).name;
        igfs.metaCacheName = $generatorCommon.igfsMetaCache(igfs).name;

        $generatorSpring.property(res, igfs, 'name');
        $generatorSpring.property(res, igfs, 'dataCacheName');
        $generatorSpring.property(res, igfs, 'metaCacheName');
        $generatorSpring.property(res, igfs, 'defaultMode', null, 'DUAL_ASYNC');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate IGFS misc configuration.
$generatorSpring.igfsMisc = function(igfs, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorSpring.property(res, igfs, 'blockSize', null, 65536);
    $generatorSpring.property(res, igfs, 'streamBufferSize', null, 65536);
    $generatorSpring.property(res, igfs, 'maxSpaceSize', null, 0);
    $generatorSpring.property(res, igfs, 'maximumTaskRangeLength', null, 0);
    $generatorSpring.property(res, igfs, 'managementPort', null, 11400);
    $generatorSpring.property(res, igfs, 'perNodeBatchSize', null, 100);
    $generatorSpring.property(res, igfs, 'perNodeParallelBatchCount', null, 8);
    $generatorSpring.property(res, igfs, 'prefetchBlocks', null, 0);
    $generatorSpring.property(res, igfs, 'sequentialReadsBeforePrefetch', null, 0);
    $generatorSpring.property(res, igfs, 'trashPurgeTimeout', null, 1000);
    $generatorSpring.property(res, igfs, 'colocateMetadata', null, true);
    $generatorSpring.property(res, igfs, 'relaxedConsistency', null, true);

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
$generatorSpring.generateDataSources = function(datasources, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (datasources.length > 0) {
        res.line('<!-- Data source beans will be initialized from external properties file. -->');

        _.forEach(datasources, (datasource) => $generatorSpring.generateDataSource(datasource, res));

        res.needEmptyLine = true;

        res.emptyLineIfNeeded();
    }

    return res;
};

$generatorSpring.generateDataSource = function(datasource, res) {
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

$generatorSpring.clusterConfiguration = function(cluster, clientNearCfg, res) {
    const isSrvCfg = _.isNil(clientNearCfg);

    if (!isSrvCfg) {
        res.line('<property name="clientMode" value="true"/>');

        res.needEmptyLine = true;
    }

    $generatorSpring.clusterGeneral(cluster, res);

    $generatorSpring.clusterAtomics(cluster.atomicConfiguration, res);

    $generatorSpring.clusterBinary(cluster.binaryConfiguration, res);

    $generatorSpring.clusterCacheKeyConfiguration(cluster.cacheKeyConfiguration, res);

    $generatorSpring.clusterCollision(cluster.collision, res);

    $generatorSpring.clusterCommunication(cluster, res);

    $generatorSpring.clusterConnector(cluster.connector, res);

    $generatorSpring.clusterDeployment(cluster, res);

    $generatorSpring.clusterEvents(cluster, res);

    $generatorSpring.clusterFailover(cluster, res);

    $generatorSpring.clusterLogger(cluster.logger, res);

    $generatorSpring.clusterODBC(cluster.odbc, res);

    $generatorSpring.clusterMarshaller(cluster, res);

    $generatorSpring.clusterMetrics(cluster, res);

    $generatorSpring.clusterSwap(cluster, res);

    $generatorSpring.clusterTime(cluster, res);

    $generatorSpring.clusterPools(cluster, res);

    $generatorSpring.clusterTransactions(cluster.transactionConfiguration, res);

    $generatorSpring.clusterCaches(cluster.caches, cluster.igfss, isSrvCfg, res);

    $generatorSpring.clusterSsl(cluster, res);

    if (isSrvCfg)
        $generatorSpring.igfss(cluster.igfss, res);

    $generatorSpring.clusterUserAttributes(cluster, res);

    return res;
};

$generatorSpring.cluster = function(cluster, clientNearCfg) {
    if (cluster) {
        const res = $generatorCommon.builder(1);

        if (clientNearCfg) {
            res.startBlock('<bean id="nearCacheBean" class="org.apache.ignite.configuration.NearCacheConfiguration">');

            if (clientNearCfg.nearStartSize)
                $generatorSpring.property(res, clientNearCfg, 'nearStartSize');

            if (clientNearCfg.nearEvictionPolicy && clientNearCfg.nearEvictionPolicy.kind)
                $generatorSpring.evictionPolicy(res, clientNearCfg.nearEvictionPolicy, 'nearEvictionPolicy');

            res.endBlock('</bean>');

            res.needEmptyLine = true;

            res.emptyLineIfNeeded();
        }

        // Generate Ignite Configuration.
        res.startBlock('<bean class="org.apache.ignite.configuration.IgniteConfiguration">');

        $generatorSpring.clusterConfiguration(cluster, clientNearCfg, res);

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
        xml += $generatorSpring.generateDataSources(res.datasources, $generatorCommon.builder(1)).asString();

        // 3. Add main content.
        xml += res.asString();

        // 4. Add footer.
        xml += '\n</beans>';

        return xml;
    }

    return '';
};

export default $generatorSpring;
