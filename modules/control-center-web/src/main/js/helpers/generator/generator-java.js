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

// Java generation entry point.
$generatorJava = {};

/**
 * Translate some value to valid java code.
 *
 * @param val Value to convert.
 * @param type Value type.
 * @returns {*} String with value that will be valid for java.
 */
$generatorJava.toJavaCode = function (val, type) {
    if (val == null)
        return 'null';

    if (type == 'raw')
        return val;

    if (type == 'class')
        return val + '.class';

    if (type == 'float')
        return val + 'f';

    if (type == 'path')
        return '"' + val.replace(/\\/g, '\\\\') + '"';

    if (type)
        return type + '.' + val;

    if (typeof(val) == 'string')
        return '"' + val.replace('"', '\\"') + '"';

    if (typeof(val) == 'number' || typeof(val) == 'boolean')
        return '' + val;

    throw "Unknown type: " + typeof(val) + ' (' + val + ')';
};

/**
 * @param propName Property name.
 * @param setterName Optional concrete setter name.
 * @returns Property setter with name by java conventions.
 */
$generatorJava.setterName = function (propName, setterName) {
    return setterName ? setterName : $commonUtils.toJavaName('set', propName);
};

/**
 * Add variable declaration.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param varFullType Variable full class name to be added to imports.
 * @param varFullActualType Variable actual full class name to be added to imports.
 * @param varFullGenericType1 Optional full class name of first generic.
 * @param varFullGenericType2 Optional full class name of second generic.
 */
$generatorJava.declareVariable = function (res, varName, varFullType, varFullActualType, varFullGenericType1, varFullGenericType2) {
    res.emptyLineIfNeeded();

    var varType = res.importClass(varFullType);

    var varNew = !res.vars[varName];

    if (varNew)
        res.vars[varName] = true;

    if (varFullActualType && varFullGenericType1) {
        var varActualType = res.importClass(varFullActualType);
        var varGenericType1 = res.importClass(varFullGenericType1);

        if (varFullGenericType2)
            var varGenericType2 = res.importClass(varFullGenericType2);

        res.line((varNew ? (varType + '<' + varGenericType1 + (varGenericType2 ? ', ' + varGenericType2 : '') + '> ') : '') + varName + ' = new ' + varActualType + '<>();');
    }
    else
        res.line((varNew ? (varType + ' ') : '') + varName + ' = new ' + varType + '();');

    res.needEmptyLine = true;
};

/**
 * Add custom variable declaration.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param varFullType Variable full class name to be added to imports.
 * @param varExpr Custom variable creation expression.
 */
$generatorJava.declareVariableCustom = function (res, varName, varFullType, varExpr) {
    var varType = res.importClass(varFullType);

    var varNew = !res.vars[varName];

    if (varNew)
        res.vars[varName] = true;

    res.line((varNew ? (varType + ' ') : '') + varName + ' = ' + varExpr + ';');

    res.needEmptyLine = true;
};

/**
 * Clear list of declared variables.
 *
 * @param res
 */
$generatorJava.resetVariables = function (res) {
    res.vars = {};
};

/**
 * Add property via setter / property name.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 * @param dataType Optional info about property data type.
 * @param setterName Optional special setter name.
 * @param dflt Optional default value.
 */
$generatorJava.property = function (res, varName, obj, propName, dataType, setterName, dflt) {
    var val = obj[propName];

    if ($commonUtils.isDefinedAndNotEmpty(val)) {
        var hasDflt = $commonUtils.isDefined(dflt);

        // Add to result if no default provided or value not equals to default.
        if (!hasDflt || (hasDflt && val != dflt)) {
            res.emptyLineIfNeeded();

            res.line(varName + '.' + $generatorJava.setterName(propName, setterName)
                + '(' + $generatorJava.toJavaCode(val, dataType ? res.importClass(dataType) : null) + ');');

            return true;
        }
    }

    return false;
};

/**
 * Add list property.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 * @param dataType Optional data type.
 * @param setterName Optional setter name.
 */
$generatorJava.listProperty = function (res, varName, obj, propName, dataType, setterName) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        res.importClass('java.util.Arrays');

        res.line(varName + '.' + $generatorJava.setterName(propName, setterName) +
            '(Arrays.asList(' +
                _.map(val, function (v) {
                    return $generatorJava.toJavaCode(v, dataType)
                }).join(', ') +
            '));');

        res.needEmptyLine = true;
    }
};

/**
 * Add array property.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 * @param setterName Optional setter name.
 */
$generatorJava.arrayProperty = function (res, varName, obj, propName, setterName) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        res.line(varName + '.' + $generatorJava.setterName(propName, setterName) + '({ ' +
            _.map(val, function (v) {
                return 'new ' + res.importClass(v) + '()'
            }).join(', ') +
        ' });');

        res.needEmptyLine = true;
    }
};

/**
 * Add multi-param property (setter with several arguments).
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 * @param dataType Optional data type.
 * @param setterName Optional setter name.
 */
$generatorJava.multiparamProperty = function (res, varName, obj, propName, dataType, setterName) {
    var val = obj[propName];

    if (val && val.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock(varName + '.' + $generatorJava.setterName(propName, setterName) + '(');

        _.forEach(val, function(v, ix) {
            res.append($generatorJava.toJavaCode(v, dataType) + (ix < val.length - 1 ? ', ' : ''));
        });

        res.endBlock(');');
    }
};

/**
 * Add complex bean.
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param bean
 * @param beanPropName
 * @param beanVarName
 * @param beanClass
 * @param props
 * @param createBeanAlthoughNoProps If 'true' then create empty bean.
 */
$generatorJava.beanProperty = function (res, varName, bean, beanPropName, beanVarName, beanClass, props, createBeanAlthoughNoProps) {
    if (bean && $commonUtils.hasProperty(bean, props)) {
        res.emptyLineIfNeeded();

        $generatorJava.declareVariable(res, beanVarName, beanClass);

        _.forIn(props, function(descr, propName) {
            if (props.hasOwnProperty(propName)) {
                if (descr) {
                    switch (descr.type) {
                        case 'list':
                            $generatorJava.listProperty(res, beanVarName, bean, propName, descr.elementsType, descr.setterName);
                            break;

                        case 'array':
                            $generatorJava.arrayProperty(res, beanVarName, bean, propName, descr.setterName);
                            break;

                        case 'enum':
                            $generatorJava.property(res, beanVarName, bean, propName, descr.enumClass, descr.setterName);
                            break;

                        case 'float':
                            $generatorJava.property(res, beanVarName, bean, propName, 'float', descr.setterName);
                            break;

                        case 'path':
                            $generatorJava.property(res, beanVarName, bean, propName, 'path', descr.setterName);
                            break;

                        case 'raw':
                            $generatorJava.property(res, beanVarName, bean, propName, 'raw', descr.setterName);
                            break;

                        case 'propertiesAsList':
                            var val = bean[propName];

                            if (val && val.length > 0) {
                                $generatorJava.declareVariable(res, descr.propVarName, 'java.util.Properties');

                                _.forEach(val, function(nameAndValue) {
                                    var eqIndex = nameAndValue.indexOf('=');

                                    if (eqIndex >= 0) {
                                        res.line(descr.propVarName + '.setProperty(' +
                                            '"' + nameAndValue.substring(0, eqIndex) + '", ' +
                                            '"' + nameAndValue.substr(eqIndex + 1) + '");');
                                    }
                                });

                                res.needEmptyLine = true;

                                res.line(beanVarName + '.' + $generatorJava.setterName(propName) + '(' + descr.propVarName + ');');
                            }
                            break;

                        case 'bean':
                            if ($commonUtils.isDefinedAndNotEmpty(bean[propName]))
                                res.line(beanVarName + '.' + $generatorJava.setterName(propName) + '(new ' + res.importClass(bean[propName]) + '());');

                            break;

                        default:
                            $generatorJava.property(res, beanVarName, bean, propName, null, descr.setterName, descr.dflt);
                    }
                }
                else {
                    $generatorJava.property(res, beanVarName, bean, propName);
                }
            }
        });

        res.needEmptyLine = true;

        res.line(varName + '.' + $generatorJava.setterName(beanPropName) + '(' + beanVarName + ');');

        res.needEmptyLine = true;
    }
    else if (createBeanAlthoughNoProps) {
        res.emptyLineIfNeeded();
        res.line(varName + '.' + $generatorJava.setterName(beanPropName) + '(new ' + res.importClass(beanClass) + '());');

        res.needEmptyLine = true;
    }
};

/**
 * Add eviction policy.
 *
 * @param res Resulting output with generated code.
 * @param varName Current using variable name.
 * @param evtPlc Data to add.
 * @param propName Name in source data.
 */
$generatorJava.evictionPolicy = function (res, varName, evtPlc, propName) {
    if (evtPlc && evtPlc.kind) {
        var evictionPolicyDesc = $generatorCommon.EVICTION_POLICIES[evtPlc.kind];

        var obj = evtPlc[evtPlc.kind.toUpperCase()];

        $generatorJava.beanProperty(res, varName, obj, propName, propName,
            evictionPolicyDesc.className, evictionPolicyDesc.fields, true);
    }
};

// Generate cluster general group.
$generatorJava.clusterGeneral = function (cluster, clientNearCfg, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.declareVariable(res, 'cfg', 'org.apache.ignite.configuration.IgniteConfiguration');

    $generatorJava.property(res, 'cfg', cluster, 'name', null, 'setGridName');
    res.needEmptyLine = true;

    if (clientNearCfg) {
        res.line('cfg.setClientMode(true);');

        res.needEmptyLine = true;
    }

    if (cluster.discovery) {
        var d = cluster.discovery;

        $generatorJava.declareVariable(res, 'discovery', 'org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi');

        switch (d.kind) {
            case 'Multicast':
                $generatorJava.beanProperty(res, 'discovery', d.Multicast, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder',
                    {
                        multicastGroup: null,
                        multicastPort: null,
                        responseWaitTime: null,
                        addressRequestAttempts: null,
                        localAddress: null,
                        addresses: {type: 'list'}
                    }, true);

                break;

            case 'Vm':
                $generatorJava.beanProperty(res, 'discovery', d.Vm, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder',
                    {addresses: {type: 'list'}}, true);

                break;

            case 'S3':
                $generatorJava.beanProperty(res, 'discovery', d.S3, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinder', {bucketName: null}, true);

                break;

            case 'Cloud':
                $generatorJava.beanProperty(res, 'discovery', d.Cloud, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.cloud.TcpDiscoveryCloudIpFinder',
                    {
                        credential: null,
                        credentialPath: null,
                        identity: null,
                        provider: null,
                        regions: {type: 'list'},
                        zones: {type: 'list'}
                    }, true);

                break;

            case 'GoogleStorage':
                $generatorJava.beanProperty(res, 'discovery', d.GoogleStorage, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinder',
                    {
                        projectName: null,
                        bucketName: null,
                        serviceAccountP12FilePath: null,
                        serviceAccountId: null
                    }, true);

                break;

            case 'Jdbc':
                $generatorJava.beanProperty(res, 'discovery', d.Jdbc, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder', {initSchema: null}, true);

                break;

            case 'SharedFs':
                $generatorJava.beanProperty(res, 'discovery', d.SharedFs, 'ipFinder', 'ipFinder',
                    'org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder', {path: null}, true);

                break;

            default:
                res.line('Unknown discovery kind: ' + d.kind);
        }

        res.needEmptyLine = false;

        $generatorJava.clusterDiscovery(d, res);

        res.emptyLineIfNeeded();

        res.line('cfg.setDiscoverySpi(discovery);');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate atomics group.
$generatorJava.clusterAtomics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var atomics = cluster.atomicConfiguration;

    if ($commonUtils.hasAtLeastOneProperty(atomics, ['cacheMode', 'atomicSequenceReserveSize', 'backups'])) {
        res.startSafeBlock();

        $generatorJava.declareVariable(res, 'atomicCfg', 'org.apache.ignite.configuration.AtomicConfiguration');

        $generatorJava.property(res, 'atomicCfg', atomics, 'cacheMode');

        var cacheMode = atomics.cacheMode ? atomics.cacheMode : 'PARTITIONED';

        var hasData = cacheMode != 'PARTITIONED';

        hasData = $generatorJava.property(res, 'atomicCfg', atomics, 'atomicSequenceReserveSize') || hasData;

        if (cacheMode == 'PARTITIONED')
            hasData = $generatorJava.property(res, 'atomicCfg', atomics, 'backups') || hasData;

        res.needEmptyLine = true;

        res.line('cfg.setAtomicConfiguration(atomicCfg);');

        res.needEmptyLine = true;

        if (!hasData)
            res.rollbackSafeBlock();
    }

    return res;
};

// Generate communication group.
$generatorJava.clusterCommunication = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var cfg = $generatorCommon.COMMUNICATION_CONFIGURATION;

    $generatorJava.beanProperty(res, 'cfg', cluster.communication, 'communicationSpi', 'commSpi', cfg.className, cfg.fields);

    res.needEmptyLine = false;

    $generatorJava.property(res, 'cfg', cluster, 'networkTimeout', null, null, 5000);
    $generatorJava.property(res, 'cfg', cluster, 'networkSendRetryDelay', null, null, 1000);
    $generatorJava.property(res, 'cfg', cluster, 'networkSendRetryCount', null, null, 3);
    $generatorJava.property(res, 'cfg', cluster, 'segmentCheckFrequency');
    $generatorJava.property(res, 'cfg', cluster, 'waitForSegmentOnStart', null, null, false);
    $generatorJava.property(res, 'cfg', cluster, 'discoveryStartupDelay', null, null, 600000);

    res.needEmptyLine = true;

    return res;
};

// Generate REST access group.
$generatorJava.clusterConnector = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($commonUtils.isDefined(cluster.connector) && cluster.connector.enabled) {
        var cfg = _.cloneDeep($generatorCommon.CONNECTOR_CONFIGURATION);

        if (cluster.connector.sslEnabled) {
            cfg.fields.sslClientAuth = {dflt: false};
            cfg.fields.sslFactory = {type: 'bean'};
        }

        $generatorJava.beanProperty(res, 'cfg', cluster.connector, 'connectorConfiguration', 'clientCfg',
            cfg.className, cfg.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate deployment group.
$generatorJava.clusterDeployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'deploymentMode', null, null, 'SHARED');

    res.needEmptyLine = true;

    var p2pEnabled = cluster.peerClassLoadingEnabled;

    if ($commonUtils.isDefined(p2pEnabled)) {
        $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingEnabled', null, null, false);

        if (p2pEnabled) {
            $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingMissedResourcesCacheSize');
            $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingThreadPoolSize');
            $generatorJava.multiparamProperty(res, 'cfg', cluster, 'peerClassLoadingLocalClassPathExclude');
        }

        res.needEmptyLine = true;
    }

    return res;
};

// Generate discovery group.
$generatorJava.clusterDiscovery = function (disco, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'discovery', disco, 'localAddress');
    $generatorJava.property(res, 'discovery', disco, 'localPort', null, null, 47500);
    $generatorJava.property(res, 'discovery', disco, 'localPortRange', null, null, 100);

    if ($commonUtils.isDefinedAndNotEmpty(disco.addressResolver)) {
        $generatorJava.beanProperty(res, 'discovery', disco, 'addressResolver', 'addressResolver', disco.addressResolver, {}, true);
        res.needEmptyLine = false;
    }

    $generatorJava.property(res, 'discovery', disco, 'socketTimeout', null, null, 5000);
    $generatorJava.property(res, 'discovery', disco, 'ackTimeout', null, null, 5000);
    $generatorJava.property(res, 'discovery', disco, 'maxAckTimeout', null, null, 600000);
    $generatorJava.property(res, 'discovery', disco, 'networkTimeout', null, null, 5000);
    $generatorJava.property(res, 'discovery', disco, 'joinTimeout', null, null, 0);
    $generatorJava.property(res, 'discovery', disco, 'threadPriority', null, null, 10);
    $generatorJava.property(res, 'discovery', disco, 'heartbeatFrequency', null, null, 2000);
    $generatorJava.property(res, 'discovery', disco, 'maxMissedHeartbeats', null, null, 1);
    $generatorJava.property(res, 'discovery', disco, 'maxMissedClientHeartbeats', null, null, 5);
    $generatorJava.property(res, 'discovery', disco, 'topHistorySize', null, null, 100);

    if ($commonUtils.isDefinedAndNotEmpty(disco.listener)) {
        $generatorJava.beanProperty(res, 'discovery', disco, 'listener', 'listener', disco.listener, {}, true);
        res.needEmptyLine = false;
    }

    if ($commonUtils.isDefinedAndNotEmpty(disco.dataExchange)) {
        $generatorJava.beanProperty(res, 'discovery', disco, 'dataExchange', 'dataExchange', disco.dataExchange, {}, true);
        res.needEmptyLine = false;
    }

    if ($commonUtils.isDefinedAndNotEmpty(disco.metricsProvider)) {
        $generatorJava.beanProperty(res, 'discovery', disco, 'metricsProvider', 'metricsProvider', disco.metricsProvider, {}, true);
        res.needEmptyLine = false;
    }

    $generatorJava.property(res, 'discovery', disco, 'reconnectCount', null, null, 10);
    $generatorJava.property(res, 'discovery', disco, 'statisticsPrintFrequency', null, null, 0);
    $generatorJava.property(res, 'discovery', disco, 'ipFinderCleanFrequency', null, null, 60000);

    if ($commonUtils.isDefinedAndNotEmpty(disco.authenticator)) {
        $generatorJava.beanProperty(res, 'discovery', disco, 'authenticator', 'authenticator', disco.authenticator, {}, true);
        res.needEmptyLine = false;
    }

    $generatorJava.property(res, 'discovery', disco, 'forceServerMode', null, null, false);
    $generatorJava.property(res, 'discovery', disco, 'clientReconnectDisabled', null, null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate events group.
$generatorJava.clusterEvents = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.includeEventTypes && cluster.includeEventTypes.length > 0) {
        res.emptyLineIfNeeded();

        if (cluster.includeEventTypes.length == 1) {
            res.importClass('org.apache.ignite.events.EventType');

            res.line('cfg.setIncludeEventTypes(EventType.' + cluster.includeEventTypes[0] + ');');
        }
        else {
            res.append('int[] events = new int[EventType.' + cluster.includeEventTypes[0] + '.length');

            _.forEach(cluster.includeEventTypes, function(e, ix) {
                if (ix > 0) {
                    res.needEmptyLine = true;

                    res.append('    + EventType.' + e + '.length');
                }
            });

            res.line('];');

            res.needEmptyLine = true;

            res.line('int k = 0;');

            _.forEach(cluster.includeEventTypes, function(e) {
                res.needEmptyLine = true;

                res.line('System.arraycopy(EventType.' + e + ', 0, events, k, EventType.' + e + '.length);');
                res.line('k += EventType.' + e + '.length;');
            });

            res.needEmptyLine = true;

            res.line('cfg.setIncludeEventTypes(events);');
        }

        res.needEmptyLine = true;
    }

    res.needEmptyLine = true;

    return res;
};

// Generate marshaller group.
$generatorJava.clusterMarshaller = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    var marshaller = cluster.marshaller;

    if (marshaller && marshaller.kind) {
        var marshallerDesc = $generatorCommon.MARSHALLERS[marshaller.kind];

        $generatorJava.beanProperty(res, 'cfg', marshaller[marshaller.kind], 'marshaller', 'marshaller',
            marshallerDesc.className, marshallerDesc.fields, true);

        $generatorJava.beanProperty(res, 'marshaller', marshaller[marshaller.kind], marshallerDesc.className, marshallerDesc.fields, true);
    }

    $generatorJava.property(res, 'cfg', cluster, 'marshalLocalJobs', null, null, false);
    $generatorJava.property(res, 'cfg', cluster, 'marshallerCacheKeepAliveTime');
    $generatorJava.property(res, 'cfg', cluster, 'marshallerCacheThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorJava.clusterMetrics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'metricsExpireTime');
    $generatorJava.property(res, 'cfg', cluster, 'metricsHistorySize');
    $generatorJava.property(res, 'cfg', cluster, 'metricsLogFrequency');
    $generatorJava.property(res, 'cfg', cluster, 'metricsUpdateFrequency');

    res.needEmptyLine = true;

    return res;
};

// Generate swap group.
$generatorJava.clusterSwap = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind == 'FileSwapSpaceSpi') {
        $generatorJava.beanProperty(res, 'cfg', cluster.swapSpaceSpi.FileSwapSpaceSpi, 'swapSpaceSpi', 'swapSpi',
            $generatorCommon.SWAP_SPACE_SPI.className, $generatorCommon.SWAP_SPACE_SPI.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate time group.
$generatorJava.clusterTime = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'clockSyncSamples');
    $generatorJava.property(res, 'cfg', cluster, 'clockSyncFrequency');
    $generatorJava.property(res, 'cfg', cluster, 'timeServerPortBase');
    $generatorJava.property(res, 'cfg', cluster, 'timeServerPortRange');

    res.needEmptyLine = true;

    return res;
};

// Generate thread pools group.
$generatorJava.clusterPools = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'publicThreadPoolSize');
    $generatorJava.property(res, 'cfg', cluster, 'systemThreadPoolSize');
    $generatorJava.property(res, 'cfg', cluster, 'managementThreadPoolSize');
    $generatorJava.property(res, 'cfg', cluster, 'igfsThreadPoolSize');
    $generatorJava.property(res, 'cfg', cluster, 'rebalanceThreadPoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate transactions group.
$generatorJava.clusterTransactions = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.beanProperty(res, 'cfg', cluster.transactionConfiguration, 'transactionConfiguration',
        'transactionConfiguration', $generatorCommon.TRANSACTION_CONFIGURATION.className,
        $generatorCommon.TRANSACTION_CONFIGURATION.fields);

    return res;
};

// Generate cache general group.
$generatorJava.cacheGeneral = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'name');

    res.importClass('org.apache.ignite.cache.CacheAtomicityMode');
    res.importClass('org.apache.ignite.cache.CacheMode');

    $generatorJava.property(res, varName, cache, 'cacheMode', 'CacheMode');
    $generatorJava.property(res, varName, cache, 'atomicityMode', 'CacheAtomicityMode');

    if (cache.cacheMode == 'PARTITIONED')
        $generatorJava.property(res, varName, cache, 'backups');

    $generatorJava.property(res, varName, cache, 'readFromBackup');
    $generatorJava.property(res, varName, cache, 'copyOnRead');
    $generatorJava.property(res, varName, cache, 'invalidate');

    res.needEmptyLine = true;

    return res;
};

// Generate cache memory group.
$generatorJava.cacheMemory = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'memoryMode', 'CacheMemoryMode');
    $generatorJava.property(res, varName, cache, 'offHeapMaxMemory');

    res.needEmptyLine = true;

    $generatorJava.evictionPolicy(res, varName, cache.evictionPolicy, 'evictionPolicy');

    $generatorJava.property(res, varName, cache, 'swapEnabled');
    $generatorJava.property(res, varName, cache, 'startSize');

    res.needEmptyLine = true;

    return res;
};

// Generate cache query & indexing group.
$generatorJava.cacheQuery = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'sqlOnheapRowCacheSize');
    $generatorJava.property(res, varName, cache, 'longQueryWarningTimeout');

    if (cache.indexedTypes && cache.indexedTypes.length > 0) {
        res.emptyLineIfNeeded();

        res.startBlock(varName + '.setIndexedTypes(');

        var len = cache.indexedTypes.length - 1;

        _.forEach(cache.indexedTypes, function(pair, ix) {
            res.line($generatorJava.toJavaCode(res.importClass(pair.keyClass), 'class') + ', ' +
                $generatorJava.toJavaCode(res.importClass(pair.valueClass), 'class') + (ix < len ? ',' : ''));
        });

        res.endBlock(');');

        res.needEmptyLine = true;
    }

    $generatorJava.multiparamProperty(res, varName, cache, 'sqlFunctionClasses', 'class');

    $generatorJava.property(res, varName, cache, 'sqlEscapeAll');

    res.needEmptyLine = true;

    return res;
};

/**
 * Generate cache store group.
 *
 * @param cache Cache descriptor.
 * @param metadatas Metadata descriptors.
 * @param cacheVarName Cache variable name.
 * @param res Resulting output with generated code.
 * @returns {*} Java code for cache store configuration.
 */
$generatorJava.cacheStore = function (cache, metadatas, cacheVarName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var factoryKind = cache.cacheStoreFactory.kind;

        var storeFactory = cache.cacheStoreFactory[factoryKind];

        if (storeFactory) {
            var storeFactoryDesc = $generatorCommon.STORE_FACTORIES[factoryKind];

            var varName = 'storeFactory' + storeFactoryDesc.suffix;

            var dataSourceFound = false;

            if (storeFactory.dialect) {
                var dataSourceBean = storeFactory.dataSourceBean;

                dataSourceFound = true;

                if (!_.contains(res.datasources, dataSourceBean)) {
                    res.datasources.push(dataSourceBean);

                    var dsClsName = $generatorCommon.dataSourceClassName(storeFactory.dialect);

                    res.needEmptyLine = true;

                    $generatorJava.declareVariable(res, 'dataSource', dsClsName);

                    switch (storeFactory.dialect) {
                        case 'Generic':
                            res.line('dataSource.setJdbcUrl(props.getProperty("' + dataSourceBean + '.jdbc.url"));');

                            break;

                        case 'DB2':
                            res.line('dataSource.setServerName(props.getProperty("' + dataSourceBean + '.jdbc.server_name"));');
                            res.line('dataSource.setPortNumber(Integer.valueOf(props.getProperty("' + dataSourceBean + '.jdbc.port_number")));');
                            res.line('dataSource.setDatabaseName(props.getProperty("' + dataSourceBean + '.jdbc.database_name"));');
                            res.line('dataSource.setDriverType(Integer.valueOf(props.getProperty("' + dataSourceBean + '.jdbc.driver_type")));');

                            break;

                        default:
                            res.line('dataSource.setURL(props.getProperty("' + dataSourceBean + '.jdbc.url"));');
                    }

                    res.line('dataSource.setUser(props.getProperty("' + dataSourceBean + '.jdbc.username"));');
                    res.line('dataSource.setPassword(props.getProperty("' + dataSourceBean + '.jdbc.password"));');

                    res.needEmptyLine = true;
                }
            }

            if (factoryKind == 'CacheJdbcPojoStoreFactory') {
                // Generate POJO store factory.
                $generatorJava.declareVariable(res, varName, 'org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory');

                if (dataSourceFound)
                    res.line(varName + '.setDataSource(dataSource);');

                res.line(varName + '.setDialect(new ' +
                    res.importClass($generatorCommon.jdbcDialectClassName(storeFactory.dialect)) + '());');

                res.needEmptyLine = true;

                if (metadatas && metadatas.length > 0) {
                    $generatorJava.declareVariable(res, 'jdbcTypes', 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.store.jdbc.JdbcType');

                    res.needEmptyLine = true;

                    _.forEach(metadatas, function (meta) {
                        $generatorJava.declareVariable(res, 'jdbcType', 'org.apache.ignite.cache.store.jdbc.JdbcType');

                        res.needEmptyLine = true;

                        $generatorJava.metadataStore(meta, true, res);

                        res.needEmptyLine = true;

                        res.line('jdbcTypes.add(jdbcType);');

                        res.needEmptyLine = true;
                    });

                    res.line(varName + '.setTypes(jdbcTypes.toArray(new JdbcType[jdbcTypes.size()]));');

                    res.needEmptyLine = true;
                }

                res.line(cacheVarName + '.setCacheStoreFactory(' + varName + ');');
            }
            else {
                $generatorJava.beanProperty(res, cacheVarName, storeFactory, 'cacheStoreFactory', varName,
                    storeFactoryDesc.className, storeFactoryDesc.fields, true);

                if (dataSourceFound)
                    res.line(varName + '.setDataSource(dataSource);');
            }

            res.needEmptyLine = true;
        }

        res.needEmptyLine = true;
    }

    $generatorJava.property(res, cacheVarName, cache, 'loadPreviousValue');
    $generatorJava.property(res, cacheVarName, cache, 'readThrough');
    $generatorJava.property(res, cacheVarName, cache, 'writeThrough');

    res.needEmptyLine = true;

    $generatorJava.property(res, cacheVarName, cache, 'writeBehindEnabled');
    $generatorJava.property(res, cacheVarName, cache, 'writeBehindBatchSize');
    $generatorJava.property(res, cacheVarName, cache, 'writeBehindFlushSize');
    $generatorJava.property(res, cacheVarName, cache, 'writeBehindFlushFrequency');
    $generatorJava.property(res, cacheVarName, cache, 'writeBehindFlushThreadCount');

    res.needEmptyLine = true;

    return res;
};

// Generate cache concurrency group.
$generatorJava.cacheConcurrency = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'maxConcurrentAsyncOperations');
    $generatorJava.property(res, varName, cache, 'defaultLockTimeout');
    $generatorJava.property(res, varName, cache, 'atomicWriteOrderMode', 'org.apache.ignite.cache.CacheAtomicWriteOrderMode');
    $generatorJava.property(res, varName, cache, 'writeSynchronizationMode', 'org.apache.ignite.cache.CacheWriteSynchronizationMode');

    res.needEmptyLine = true;

    return res;
};

// Generate cache rebalance group.
$generatorJava.cacheRebalance = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode != 'LOCAL') {
        $generatorJava.property(res, varName, cache, 'rebalanceMode', 'CacheRebalanceMode');
        $generatorJava.property(res, varName, cache, 'rebalanceThreadPoolSize');
        $generatorJava.property(res, varName, cache, 'rebalanceBatchSize');
        $generatorJava.property(res, varName, cache, 'rebalanceOrder');
        $generatorJava.property(res, varName, cache, 'rebalanceDelay');
        $generatorJava.property(res, varName, cache, 'rebalanceTimeout');
        $generatorJava.property(res, varName, cache, 'rebalanceThrottle');

        res.needEmptyLine = true;
    }

    if (cache.igfsAffinnityGroupSize) {
        res.line(varName + '.setAffinityMapper(new ' + res.importClass('org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper') + '(' + cache.igfsAffinnityGroupSize + '));');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache server near cache group.
$generatorJava.cacheServerNearCache = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheMode == 'PARTITIONED' && cache.nearCacheEnabled) {
        res.needEmptyLine = true;

        res.importClass('org.apache.ignite.configuration.NearCacheConfiguration');

        $generatorJava.beanProperty(res, varName, cache.nearConfiguration, 'nearConfiguration', 'nearConfiguration',
            'NearCacheConfiguration', {nearStartSize: null}, true);

        if (cache.nearConfiguration && cache.nearConfiguration.nearEvictionPolicy && cache.nearConfiguration.nearEvictionPolicy.kind) {
            $generatorJava.evictionPolicy(res, 'nearConfiguration', cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');
        }

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache statistics group.
$generatorJava.cacheStatistics = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'statisticsEnabled');
    $generatorJava.property(res, varName, cache, 'managementEnabled');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata query fields.
$generatorJava.metadataQueryFields = function (res, meta) {
    var fields = meta.fields;

    if (fields && fields.length > 0) {
        $generatorJava.declareVariable(res, 'fields', 'java.util.LinkedHashMap', 'java.util.LinkedHashMap', 'java.lang.String', 'java.lang.String');

        _.forEach(fields, function (field) {
            res.line('fields.put("' + field.name + '", "' + $dataStructures.fullClassName(field.className) + '");');
        });

        res.needEmptyLine = true;

        res.line('queryMeta.setFields(fields);');

        res.needEmptyLine = true;
    }
};

// Generate metadata query aliases.
$generatorJava.metadataQueryAliases = function (res, meta) {
    var aliases = meta.aliases;

    if (aliases && aliases.length > 0) {
        $generatorJava.declareVariable(res, 'aliases', 'java.util.Map', 'java.util.HashMap', 'java.lang.String', 'java.lang.String');

        _.forEach(aliases, function (alias) {
            res.line('aliases.put("' + alias.field + '", "' + alias.alias + '");');
        });

        res.needEmptyLine = true;

        res.line('queryMeta.setAliases(aliases);');

        res.needEmptyLine = true;
    }
};

// Generate metadata indexes.
$generatorJava.metadataQueryIndexes = function (res, meta) {
    var indexes = meta.indexes;

    if (indexes && indexes.length > 0) {
        res.needEmptyLine = true;

        $generatorJava.declareVariable(res, 'indexes', 'java.util.List', 'java.util.ArrayList', 'org.apache.ignite.cache.QueryIndex');

        _.forEach(indexes, function (index) {
            res.needEmptyLine = true;

            $generatorJava.declareVariable(res, 'index', 'org.apache.ignite.cache.QueryIndex');

            $generatorJava.property(res, 'index', index, 'name');
            $generatorJava.property(res, 'index', index, 'indexType', 'org.apache.ignite.cache.QueryIndexType');

            var fields = index.fields;

            if (fields && fields.length > 0) {
                $generatorJava.declareVariable(res, 'indFlds', 'java.util.LinkedHashMap', 'java.util.LinkedHashMap', 'String', 'Boolean');

                _.forEach(fields, function(field) {
                    res.line('indFlds.put("' + field.name + '", ' + field.direction + ');');
                });

                res.needEmptyLine = true;

                res.line('index.setFields(indFlds);');

                res.needEmptyLine = true;
            }

            res.line('indexes.add(index);');
        });

        res.needEmptyLine = true;

        res.line('queryMeta.setIndexes(indexes);');

        res.needEmptyLine = true;
    }
};

// Generate metadata db fields.
$generatorJava.metadataDatabaseFields = function (res, meta, fieldProperty) {
    var dbFields = meta[fieldProperty];

    if (dbFields && dbFields.length > 0) {
        res.needEmptyLine = true;

        res.importClass('java.sql.Types');

        res.startBlock('jdbcType.' + $commonUtils.toJavaName('set', fieldProperty) + '(');

        var lastIx = dbFields.length - 1;

        _.forEach(dbFields, function (field, ix) {
            res.importClass('org.apache.ignite.cache.store.jdbc.JdbcTypeField');

            res.line('new JdbcTypeField(' +
                'Types.' + field.databaseFieldType + ', ' + '"' + field.databaseFieldName + '", ' +
                res.importClass(field.javaFieldType) + '.class, ' + '"' + field.javaFieldName + '"'+ ')' + (ix < lastIx ? ',' : ''));
        });

        res.endBlock(');');

        res.needEmptyLine = true;
    }
};

// Generate metadata general group.
$generatorJava.metadataGeneral = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'typeMeta', meta, 'keyType');
    $generatorJava.property(res, 'typeMeta', meta, 'valueType');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for query group.
$generatorJava.metadataQuery = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.metadataQueryFields(res, meta);

    $generatorJava.metadataQueryAliases(res, meta);

    $generatorJava.metadataQueryIndexes(res, meta);

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for store group.
$generatorJava.metadataStore = function (meta, withTypes, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'jdbcType', meta, 'databaseSchema');
    $generatorJava.property(res, 'jdbcType', meta, 'databaseTable');

    if (withTypes) {
        $generatorJava.property(res, 'jdbcType', meta, 'keyType');
        $generatorJava.property(res, 'jdbcType', meta, 'valueType');
    }

    $generatorJava.metadataDatabaseFields(res, meta, 'keyFields');

    $generatorJava.metadataDatabaseFields(res, meta, 'valueFields');

    res.needEmptyLine = true;

    return res;
};

// Generate cache type metadata config.
$generatorJava.cacheQueryMetadata = function(meta, res) {
    $generatorJava.declareVariable(res, 'queryMeta', 'org.apache.ignite.cache.QueryEntity');

    $generatorJava.property(res, 'queryMeta', meta, 'keyType');
    $generatorJava.property(res, 'queryMeta', meta, 'valueType');

    res.needEmptyLine = true;

    $generatorJava.metadataQuery(meta, res);

    res.emptyLineIfNeeded();
    res.line('queryEntities.add(queryMeta);');

    res.needEmptyLine = true;
};

// Generate cache type metadata configs.
$generatorJava.cacheMetadatas = function (metadatas, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    // Generate cache type metadata configs.
    if (metadatas && metadatas.length > 0) {
        $generatorJava.declareVariable(res, 'queryEntities', 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.QueryEntity');

        _.forEach(metadatas, function (meta) {
            $generatorJava.cacheQueryMetadata(meta, res);
        });

        res.line(varName + '.setQueryEntities(queryEntities);');

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cache configs.
$generatorJava.cache = function(cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.cacheGeneral(cache, varName, res);

    $generatorJava.cacheMemory(cache, varName, res);

    $generatorJava.cacheQuery(cache, varName, res);

    $generatorJava.cacheStore(cache, cache.metadatas, varName, res);

    $generatorJava.cacheConcurrency(cache, varName, res);

    $generatorJava.cacheRebalance(cache, varName, res);

    $generatorJava.cacheServerNearCache(cache, varName, res);

    $generatorJava.cacheStatistics(cache, varName, res);

    $generatorJava.cacheMetadatas(cache.metadatas, varName, res);
};

// Generate cluster caches.
$generatorJava.clusterCaches = function (caches, igfss, res) {
    function clusterCache(res, cache, names) {
        res.emptyLineIfNeeded();

        var cacheName = $commonUtils.toJavaName('cache', cache.name);

        $generatorJava.declareVariable(res, cacheName, 'org.apache.ignite.configuration.CacheConfiguration');

        $generatorJava.cache(cache, cacheName, res);

        names.push(cacheName);

        res.needEmptyLine = true;
    }

    if (!res)
        res = $generatorCommon.builder();

    var names = [];

    if (caches && caches.length > 0) {
        res.emptyLineIfNeeded();

        _.forEach(caches, function (cache) {
            clusterCache(res, cache, names);
        });

        res.needEmptyLine = true;
    }

    if (igfss && igfss.length > 0) {
        res.emptyLineIfNeeded();

        _.forEach(igfss, function (igfs) {
            clusterCache(res, $generatorCommon.igfsDataCache(igfs), names);
            clusterCache(res, $generatorCommon.igfsMetaCache(igfs), names);
        });

        res.needEmptyLine = true;
    }

    if (names.length > 0) {
        res.line('cfg.setCacheConfiguration(' + names.join(', ') + ');');

        res.needEmptyLine = true;
    }

    return res;
};

/**
 * Generate java class code.
 *
 * @param meta Metadata object.
 * @param key If 'true' then key class should be generated.
 * @param pkg Package name.
 * @param useConstructor If 'true' then empty and full constructors should be generated.
 * @param includeKeyFields If 'true' then include key fields into value POJO.
 * @param res Resulting output with generated code.
 */
$generatorJava.javaClassCode = function (meta, key, pkg, useConstructor, includeKeyFields, res) {
    if (!res)
        res = $generatorCommon.builder();

    var type = (key ? meta.keyType : meta.valueType);
    type = type.substring(type.lastIndexOf('.') + 1);

    // Class comment.
    res.line('/**');
    res.line(' * ' + type + ' definition.');
    res.line(' *');
    res.line(' * ' + $generatorCommon.mainComment());
    res.line(' */');

    res.startBlock('public class ' + type + ' implements ' + res.importClass('java.io.Serializable') + ' {');

    res.line('/** */');
    res.line('private static final long serialVersionUID = 0L;');
    res.needEmptyLine = true;

    var allFields = (key || includeKeyFields) ? meta.keyFields.slice() : [];

    if (!key)
        _.forEach(meta.valueFields, function (valFld) {
            if (_.findIndex(allFields, function(fld) {
                return fld.javaFieldName == valFld.javaFieldName;
            }) < 0)
                allFields.push(valFld);
        });

    // Generate allFields declaration.
    _.forEach(allFields, function (field) {
        var fldName = field.javaFieldName;

        res.line('/** Value for ' + fldName + '. */');

        res.line('private ' + res.importClass(field.javaFieldType) + ' ' + fldName + ';');

        res.needEmptyLine = true;
    });

    // Generate constructors.
    if (useConstructor) {
        res.line('/**');
        res.line(' * Empty constructor.');
        res.line(' */');
        res.startBlock('public ' + type + '() {');
        res.line('// No-op.');
        res.endBlock('}');

        res.needEmptyLine = true;

        res.line('/**');
        res.line(' * Full constructor.');
        res.line(' */');
        res.startBlock('public ' + type + '(');

        _.forEach(allFields, function(field) {
            res.line(res.importClass(field.javaFieldType) + ' ' + field.javaFieldName + (fldIx < allFields.length - 1 ? ',' : ''))
        });

        res.endBlock(') {');

        res.startBlock();

        _.forEach(allFields, function (field) {
            res.line('this.' + field.javaFieldName +' = ' + field.javaFieldName + ';');
        });

        res.endBlock('}');

        res.needEmptyLine = true;
    }

    // Generate getters and setters methods.
    _.forEach(allFields, function (field) {
        var fldName = field.javaFieldName;

        var fldType = res.importClass(field.javaFieldType);

        res.line('/**');
        res.line(' * Gets ' + fldName + '.');
        res.line(' *');
        res.line(' * @return Value for ' + fldName + '.');
        res.line(' */');
        res.startBlock('public ' + fldType + ' ' + $commonUtils.toJavaName('get', fldName) + '() {');
        res.line('return ' + fldName + ';');
        res.endBlock('}');

        res.needEmptyLine = true;

        res.line('/**');
        res.line(' * Sets ' + fldName + '.');
        res.line(' *');
        res.line(' * @param ' + fldName + ' New value for ' + fldName + '.');
        res.line(' */');
        res.startBlock('public void ' + $commonUtils.toJavaName('set', fldName) + '(' + fldType + ' ' + fldName + ') {');
        res.line('this.' + fldName + ' = ' + fldName + ';');
        res.endBlock('}');

        res.needEmptyLine = true;
    });

    // Generate equals() method.
    res.line('/** {@inheritDoc} */');
    res.startBlock('@Override public boolean equals(Object o) {');
    res.startBlock('if (this == o)');
    res.line('return true;');
    res.endBlock();
    res.append('');

    res.startBlock('if (!(o instanceof ' + type + '))');
    res.line('return false;');
    res.endBlock();

    res.needEmptyLine = true;

    res.line(type + ' that = (' + type + ')o;');

    _.forEach(allFields, function (field) {
        res.needEmptyLine = true;

        var javaName = field.javaFieldName;

        res.startBlock('if (' + javaName + ' != null ? !' + javaName + '.equals(that.' + javaName + ') : that.' + javaName + ' != null)');

        res.line('return false;');
        res.endBlock()
    });

    res.needEmptyLine = true;

    res.line('return true;');
    res.endBlock('}');

    res.needEmptyLine = true;

    // Generate hashCode() method.
    res.line('/** {@inheritDoc} */');
    res.startBlock('@Override public int hashCode() {');

    var first = true;

    _.forEach(allFields, function (field) {
        var javaName = field.javaFieldName;

        if (!first)
            res.needEmptyLine = true;

        res.line(first ? 'int res = ' + javaName + ' != null ? ' + javaName + '.hashCode() : 0;'
            : 'res = 31 * res + (' + javaName + ' != null ? ' + javaName + '.hashCode() : 0);');

        first = false;
    });

    res.needEmptyLine = true;
    res.line('return res;');
    res.endBlock('}');
    res.needEmptyLine = true;

    // Generate toString() method.
    res.line('/** {@inheritDoc} */');
    res.startBlock('@Override public String toString() {');

    if (allFields.length > 0) {
        field = allFields[0];

        res.startBlock('return \"' + type + ' [' + field.javaFieldName + '=\" + ' + field.javaFieldName + ' +', type);

        for (fldIx = 1; fldIx < allFields.length; fldIx ++) {
            field = allFields[fldIx];

            var javaName = field.javaFieldName;

            res.line('\", ' + javaName + '=\" + ' + field.javaFieldName + ' +');
        }
    }

    res.line('\']\';');
    res.endBlock();
    res.endBlock('}');

    res.endBlock('}');

    return 'package ' + pkg + ';' + '\n\n' + res.generateImports() + '\n\n' + res.asString();
};

/**
 * Generate source code for type by its metadata.
 *
 * @param caches List of caches to generate POJOs for.
 * @param useConstructor If 'true' then generate constructors.
 * @param includeKeyFields If 'true' then include key fields into value POJO.
 */
$generatorJava.pojos = function (caches, useConstructor, includeKeyFields) {
    var metadatas = [];

    _.forEach(caches, function(cache) {
        _.forEach(cache.metadatas, function(meta) {
            // Skip already generated classes.
            if (!_.find(metadatas, {valueType: meta.valueType}) &&
                // Skip metadata without value fields.
                $commonUtils.isDefined(meta.valueFields) && meta.valueFields.length > 0) {
                var metadata = {};

                // Key class generation only if key is not build in java class.
                if ($commonUtils.isDefined(meta.keyFields) && meta.keyFields.length > 0) {
                    metadata.keyType = meta.keyType;
                    metadata.keyClass = $generatorJava.javaClassCode(meta, true,
                        meta.keyType.substring(0, meta.keyType.lastIndexOf('.')), useConstructor, includeKeyFields);
                }

                metadata.valueType = meta.valueType;
                metadata.valueClass = $generatorJava.javaClassCode(meta, false,
                    meta.valueType.substring(0, meta.valueType.lastIndexOf('.')), useConstructor, includeKeyFields);

                metadatas.push(metadata);
            }
        });
    });

    return metadatas;
};

/**
 * @param type Full type name.
 * @return Field java type name.
 */
$generatorJava.javaTypeName = function(type) {
    var ix = $generatorJava.javaBuildInClasses.indexOf(type);

    var resType = ix >= 0 ? $generatorJava.javaBuildInFullNameClasses[ix] : type;

    return resType.indexOf("java.lang.") >= 0 ? resType.substring(10) : resType;
};

/**
 * Java code generator for cluster's SSL configuration.
 *
 * @param cluster Cluster to get SSL configuration.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object
 */
$generatorJava.clusterSsl = function(cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.sslEnabled && $commonUtils.isDefined(cluster.sslContextFactory)) {

        cluster.sslContextFactory.keyStorePassword = $commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.keyStoreFilePath) ?
            'props.getProperty("ssl.key.storage.password").toCharArray()' : undefined;

        cluster.sslContextFactory.trustStorePassword = $commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.trustStoreFilePath) ?
            'props.getProperty("ssl.trust.storage.password").toCharArray()' : undefined;

        var propsDesc = $commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.trustManagers) ?
            $generatorCommon.SSL_CONFIGURATION_TRUST_MANAGER_FACTORY.fields :
            $generatorCommon.SSL_CONFIGURATION_TRUST_FILE_FACTORY.fields;

        $generatorJava.beanProperty(res, 'cfg', cluster.sslContextFactory, 'sslContextFactory', 'sslContextFactory',
            'org.apache.ignite.ssl.SslContextFactory', propsDesc, true);

        res.needEmptyLine = true;
    }

    return res;
};

/**
 * Java code generator for cluster's IGFS configurations.
 *
 * @param igfss List of configured IGFS.
 * @param varName Name of IGFS configuration variable.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object.
 */
$generatorJava.igfss = function(igfss, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($commonUtils.isDefinedAndNotEmpty(igfss)) {
        res.emptyLineIfNeeded();

        var arrayName = 'fileSystems';
        var igfsInst = 'igfs';

        res.line(res.importClass('org.apache.ignite.configuration.FileSystemConfiguration') + '[] ' + arrayName + ' = new FileSystemConfiguration[' + igfss.length + '];');

        _.forEach(igfss, function(igfs, ix) {
            $generatorJava.declareVariable(res, igfsInst, 'org.apache.ignite.configuration.FileSystemConfiguration');

            $generatorJava.igfsGeneral(igfs, igfsInst, res);
            $generatorJava.igfsIPC(igfs, igfsInst, res);
            $generatorJava.igfsFragmentizer(igfs, igfsInst, res);
            $generatorJava.igfsDualMode(igfs, igfsInst, res);
            $generatorJava.igfsSecondFS(igfs, igfsInst, res);
            $generatorJava.igfsMisc(igfs, igfsInst, res);

            res.line(arrayName + '[' + ix + '] = ' + igfsInst + ';');

            res.needEmptyLine = true;
        });

        res.line(varName + '.' + 'setFileSystemConfiguration(' + arrayName + ');');

        res.needEmptyLine = true;
    }

    return res;
};

/**
 * Java code generator for IGFS IPC configuration.
 *
 * @param igfs Configured IGFS.
 * @param varName Name of IGFS configuration variable.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object.
 */
$generatorJava.igfsIPC = function(igfs, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.ipcEndpointEnabled) {
        var desc = $generatorCommon.IGFS_IPC_CONFIGURATION;

        $generatorJava.beanProperty(res, varName, igfs.ipcEndpointConfiguration, 'ipcEndpointConfiguration', 'ipcEndpointCfg',
            desc.className, desc.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

/**
 * Java code generator for IGFS fragmentizer configuration.
 *
 * @param igfs Configured IGFS.
 * @param varName Name of IGFS configuration variable.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object.
 */
$generatorJava.igfsFragmentizer = function(igfs, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.fragmentizerEnabled) {
        $generatorJava.property(res, varName, igfs, 'fragmentizerConcurrentFiles', null, null, 0);
        $generatorJava.property(res, varName, igfs, 'fragmentizerThrottlingBlockLength', null, null, 16777216);
        $generatorJava.property(res, varName, igfs, 'fragmentizerThrottlingDelay', null, null, 200);

        res.needEmptyLine = true;
    }
    else
        $generatorJava.property(res, varName, igfs, 'fragmentizerEnabled');

    return res;
};

/**
 * Java code generator for IGFS dual mode configuration.
 *
 * @param igfs Configured IGFS.
 * @param varName Name of IGFS configuration variable.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object.
 */
$generatorJava.igfsDualMode = function(igfs, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, igfs, 'dualModeMaxPendingPutsSize', null, null, 0);

    if ($commonUtils.isDefinedAndNotEmpty(igfs.dualModePutExecutorService))
        res.line(varName + '.' + $generatorJava.setterName('dualModePutExecutorService') + '(new ' + res.importClass(igfs.dualModePutExecutorService) + '());');

    $generatorJava.property(res, varName, igfs, 'dualModePutExecutorServiceShutdown', null, null, false);

    res.needEmptyLine = true;

    return res;
};

$generatorJava.igfsSecondFS = function(igfs, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (igfs.secondaryFileSystemEnabled) {
        var secondFs = igfs.secondaryFileSystem || {};

        var uriDefined = $commonUtils.isDefinedAndNotEmpty(secondFs.uri);
        var nameDefined = $commonUtils.isDefinedAndNotEmpty(secondFs.userName);
        var cfgDefined = $commonUtils.isDefinedAndNotEmpty(secondFs.cfgPath);

        res.line(varName + '.setSecondaryFileSystem(new ' +
            res.importClass('org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem') + '(' +
                (uriDefined ? '"' + secondFs.uri + '"' : 'null') +
                (cfgDefined || nameDefined ? (cfgDefined ? ', "' + secondFs.cfgPath + '"' : ', null') : '') +
                (nameDefined ? ', "' + secondFs.userName + '"' : '') +
            '));');

        res.needEmptyLine = true;
    }

    return res;
};

/**
 * Java code generator for IGFS general configuration.
 *
 * @param igfs Configured IGFS.
 * @param varName Name of IGFS configuration variable.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object.
 */
$generatorJava.igfsGeneral = function(igfs, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($commonUtils.isDefinedAndNotEmpty(igfs.name)) {
        igfs.dataCacheName = $generatorCommon.igfsDataCache(igfs).name;
        igfs.metaCacheName = $generatorCommon.igfsMetaCache(igfs).name;

        $generatorJava.property(res, varName, igfs, 'name');
        $generatorJava.property(res, varName, igfs, 'dataCacheName');
        $generatorJava.property(res, varName, igfs, 'metaCacheName');

        res.needEmptyLine = true;
    }

    return res;
};

/**
 * Java code generator for IGFS misc configuration.
 *
 * @param igfs Configured IGFS.
 * @param varName Name of IGFS configuration variable.
 * @param res Optional configuration presentation builder object.
 * @returns Configuration presentation builder object.
 */
$generatorJava.igfsMisc = function(igfs, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, igfs, 'blockSize', null, null, 65536);
    $generatorJava.property(res, varName, igfs, 'streamBufferSize', null, null, 65536);
    $generatorJava.property(res, varName, igfs, 'defaultMode', 'org.apache.ignite.igfs.IgfsMode', undefined, "DUAL_ASYNC");
    $generatorJava.property(res, varName, igfs, 'maxSpaceSize', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'maximumTaskRangeLength', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'managementPort', null, null, 11400);

    if (igfs.pathModes && igfs.pathModes.length > 0) {
        res.needEmptyLine = true;

        $generatorJava.declareVariable(res, 'pathModes', 'java.util.Map', 'java.util.HashMap', 'String', 'org.apache.ignite.igfs.IgfsMode');

        _.forEach(igfs.pathModes, function (pair) {
            res.line('pathModes.put("' + pair.path + '", IgfsMode.' + pair.mode +');');
        });

        res.needEmptyLine = true;

        res.line(varName + '.setPathModes(pathModes);');
    }

    $generatorJava.property(res, varName, igfs, 'perNodeBatchSize', null, null, 100);
    $generatorJava.property(res, varName, igfs, 'perNodeParallelBatchCount', null, null, 8);
    $generatorJava.property(res, varName, igfs, 'prefetchBlocks', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'sequentialReadsBeforePrefetch', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'trashPurgeTimeout', null, null, 1000);

    res.needEmptyLine = true;

    return res;
};

/**
 * Function to generate java code for cluster configuration.
 *
 * @param cluster Cluster to process.
 * @param javaClass Class name for generate factory class otherwise generate code snippet.
 * @param clientNearCfg Near cache configuration for client node.
 * @param clientMode If `true` generates code for client mode or server mode otherwise.
 */
$generatorJava.cluster = function (cluster, javaClass, clientNearCfg, clientMode) {
    var res = $generatorCommon.builder();

    if (cluster) {
        if (javaClass) {
            res.line('/**');
            res.line(' * ' + $generatorCommon.mainComment());
            res.line(' */');
            res.startBlock('public class ' + javaClass + ' {');
            res.line('/**');
            res.line(' * Configure grid.');
            res.line(' *');
            res.line(' * @return Ignite configuration.');
            res.line(' * @throws Exception If failed to construct Ignite configuration instance.');
            res.line(' */');
            res.startBlock('public static IgniteConfiguration createConfiguration() throws Exception {');
        }

        var haveDS = _.findIndex(cluster.caches, function(cache) {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
                var factory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

                if (factory && factory.dialect)
                    return true;
            }

            return false;
        }) >= 0;

        if (haveDS || cluster.sslEnabled) {
            $generatorJava.declareVariableCustom(res, 'res', 'java.net.URL', 'IgniteConfiguration.class.getResource("/secret.properties")');

            $generatorJava.declareVariableCustom(res, 'propsFile', 'java.io.File', 'new File(res.toURI())');

            $generatorJava.declareVariable(res, 'props', 'java.util.Properties');

            res.needEmptyLine = true;

            res.startBlock('try (' + res.importClass('java.io.InputStream') + ' in = new ' + res.importClass('java.io.FileInputStream') + '(propsFile)) {');
            res.line('props.load(in);');
            res.endBlock('}');

            res.needEmptyLine = true;
        }

        $generatorJava.clusterGeneral(cluster, clientNearCfg, res);

        $generatorJava.clusterAtomics(cluster, res);

        $generatorJava.clusterCommunication(cluster, res);

        $generatorJava.clusterConnector(cluster, res);

        $generatorJava.clusterDeployment(cluster, res);

        $generatorJava.clusterEvents(cluster, res);

        $generatorJava.clusterMarshaller(cluster, res);

        $generatorJava.clusterMetrics(cluster, res);

        $generatorJava.clusterSwap(cluster, res);

        $generatorJava.clusterTime(cluster, res);

        $generatorJava.clusterPools(cluster, res);

        $generatorJava.clusterTransactions(cluster, res);

        $generatorJava.clusterCaches(cluster.caches, cluster.igfss, res);

        $generatorJava.clusterSsl(cluster, res);

        $generatorJava.igfss(cluster.igfss, 'cfg', res);

        if (javaClass) {
            if (clientMode)
                res.importClass('org.apache.ignite.Ignite');

            res.importClass('org.apache.ignite.Ignition');

            res.line('return cfg;');
            res.endBlock('}');

            res.needEmptyLine = true;
        }

        if (clientMode && clientNearCfg) {
            if (javaClass) {
                res.line('/**');
                res.line(' * Configure client near cache configuration.');
                res.line(' *');
                res.line(' * @return Near cache configuration.');
                res.line(' * @throws Exception If failed to construct near cache configuration instance.');
                res.line(' */');
                res.startBlock('public static NearCacheConfiguration createNearCacheConfiguration() throws Exception {');

                $generatorJava.resetVariables(res);
            }

            $generatorJava.declareVariable(res, 'clientNearCfg', 'org.apache.ignite.configuration.NearCacheConfiguration');

            if (clientNearCfg.nearStartSize) {
                $generatorJava.property(res, 'clientNearCfg', clientNearCfg, 'nearStartSize');

                res.needEmptyLine = true;
            }

            if (clientNearCfg.nearEvictionPolicy && clientNearCfg.nearEvictionPolicy.kind)
                $generatorJava.evictionPolicy(res, 'clientNearCfg', clientNearCfg.nearEvictionPolicy, 'nearEvictionPolicy');

            if (javaClass) {
                res.line('return clientNearCfg;');
                res.endBlock('}');
            }

            res.needEmptyLine = true;
        }

        if (javaClass) {
            res.line('/**');
            res.line(' * Sample usage of ' + javaClass + '.');
            res.line(' *');
            res.line(' * @param args Command line arguments, none required.');
            res.line(' * @throws Exception If sample execution failed.');
            res.line(' */');

            res.startBlock('public static void main(String[] args) throws Exception {');

            if (clientMode) {
                res.startBlock('try (Ignite ignite = Ignition.start(' + javaClass + '.createConfiguration())) {');

                if ($commonUtils.isDefinedAndNotEmpty(cluster.caches)) {
                    res.line('// Example of near cache creation on client node.');
                    res.line('ignite.getOrCreateNearCache("' + cluster.caches[0].name + '", ' + javaClass + '.createNearCacheConfiguration());');

                    res.needEmptyLine = true;
                }

                res.line('System.out.println("Write some code here...");');

                res.endBlock('}');
            }
            else
                res.line('Ignition.start(' + javaClass + '.createConfiguration());');

            res.endBlock('}');

            res.endBlock('}');

            return res.generateImports() + '\n\n' + res.asString();
        }
    }

    return res.asString();
};

/**
 * Function to generate java code for cluster configuration.
 * @param cluster Cluster to process.
 */
$generatorJava.nodeStartup = function (cluster) {
    var res = $generatorCommon.builder();

    res.importClass('org.apache.ignite.IgniteException');
    res.importClass('org.apache.ignite.Ignition');

    res.line('/**');
    res.line(' * ' + $generatorCommon.mainComment());
    res.line(' */');
    res.startBlock('public class NodeStartup {');

    res.line('/**');
    res.line(' * Start up an server node.');
    res.line(' *');
    res.line(' * @param args Command line arguments, none required.');
    res.line(' * @throws IgniteException If failed.');
    res.line(' */');

    res.startBlock('public static void main(String[] args) throws IgniteException {');
    res.line('Ignition.start("config/' + cluster.name + '-server.xml");');
    res.endBlock('}');

    res.endBlock('}');

    return res.generateImports() + '\n\n' + res.asString();
};
