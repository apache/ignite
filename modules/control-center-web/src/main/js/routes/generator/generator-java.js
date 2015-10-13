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

    $commonUtils = require('../../helpers/common-utils');
    $dataStructures = require('../../helpers/data-structures');
    $generatorCommon = require('./generator-common');
}

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
 * @param res Result holder.
 * @param varName Variable name to check.
 * @returns {boolean} 'true' if new variable required.
 */
$generatorJava.needNewVariable = function (res, varName) {
    var needNew = !res[varName];

    if (needNew)
        res[varName] = true;

    return needNew;
};

/**
 * Add variable declaration.
 *
 * @param res Resulting output with generated code.
 * @param varNew If 'true' then declare new variable otherwise reuse previously declared variable.
 * @param varName Variable name.
 * @param varFullType Variable full class name to be added to imports.
 * @param varFullActualType Variable actual full class name to be added to imports.
 * @param varFullGenericType1 Optional full class name of first generic.
 * @param varFullGenericType2 Optional full class name of second generic.
 */
$generatorJava.declareVariable = function (res, varNew, varName, varFullType, varFullActualType, varFullGenericType1, varFullGenericType2) {
    res.emptyLineIfNeeded();

    var varType = res.importClass(varFullType);

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
                + '(' + $generatorJava.toJavaCode(val, dataType) + ');');

            return true;
        }
    }

    return false;
};

/**
 * Add property via setter assuming that it is a 'Class'.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 */
$generatorJava.classNameProperty = function (res, varName, obj, propName) {
    var val = obj[propName];

    if ($commonUtils.isDefined(val)) {
        res.emptyLineIfNeeded();

        res.line(varName + '.' + $generatorJava.setterName(propName) + '(' + res.importClass(val) + '.class);');
    }
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
 * @param dataType Optional data type.
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

        res.append(varName + '.' + $generatorJava.setterName(propName, setterName) + '(');

        for (var i = 0; i < val.length; i++) {
            if (i > 0)
                res.append(', ');

            res.append($generatorJava.toJavaCode(val[i], dataType));
        }

        res.line(');');
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

        $generatorJava.declareVariable(res, true, beanVarName, beanClass);

        for (var propName in props) {
            if (props.hasOwnProperty(propName)) {
                var descr = props[propName];

                if (descr) {
                    switch (descr.type) {
                        case 'list':
                            $generatorJava.listProperty(res, beanVarName, bean, propName, descr.elementsType, descr.setterName);
                            break;

                        case 'array':
                            $generatorJava.arrayProperty(res, beanVarName, bean, propName, descr.setterName);
                            break;

                        case 'enum':
                            $generatorJava.property(res, beanVarName, bean, propName, res.importClass(descr.enumClass), descr.setterName);
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
                                res.line('Properties ' + descr.propVarName + ' = new Properties();');

                                for (var i = 0; i < val.length; i++) {
                                    var nameAndValue = val[i];

                                    var eqIndex = nameAndValue.indexOf('=');
                                    if (eqIndex >= 0) {
                                        res.line(descr.propVarName + '.setProperty('
                                            + nameAndValue.substring(0, eqIndex) + ', '
                                            + nameAndValue.substr(eqIndex + 1) + ');');
                                    }

                                }

                                res.line(beanVarName + '.' + $generatorJava.setterName(propName) + '(' + descr.propVarName + ');');
                            }
                            break;

                        case 'jdbcDialect':
                            if (bean[propName]) {
                                var jdbcDialectClsName = res.importClass($generatorCommon.jdbcDialectClassName(bean[propName]));

                                res.line(beanVarName + '.' + $generatorJava.setterName(propName) + '(new ' + jdbcDialectClsName + '());');
                            }

                            break;

                        default:
                            $generatorJava.property(res, beanVarName, bean, propName, null, descr.setterName);
                    }
                }
                else {
                    $generatorJava.property(res, beanVarName, bean, propName);
                }
            }
        }

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

    $generatorJava.declareVariable(res, true, 'cfg', 'org.apache.ignite.configuration.IgniteConfiguration');

    $generatorJava.property(res, 'cfg', cluster, 'name', null, 'setGridName');
    res.needEmptyLine = true;

    if (clientNearCfg) {
        res.line('cfg.setClientMode(true);');

        res.needEmptyLine = true;
    }

    if (cluster.discovery) {
        var d = cluster.discovery;

        $generatorJava.declareVariable(res, true, 'discovery', 'org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi');

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

        $generatorJava.declareVariable(res, true, 'atomicCfg', 'org.apache.ignite.configuration.AtomicConfiguration');

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

    $generatorJava.property(res, 'cfg', cluster, 'networkTimeout');
    $generatorJava.property(res, 'cfg', cluster, 'networkSendRetryDelay');
    $generatorJava.property(res, 'cfg', cluster, 'networkSendRetryCount');
    $generatorJava.property(res, 'cfg', cluster, 'segmentCheckFrequency');
    $generatorJava.property(res, 'cfg', cluster, 'waitForSegmentOnStart', null, null, false);
    $generatorJava.property(res, 'cfg', cluster, 'discoveryStartupDelay');

    res.needEmptyLine = true;

    return res;
};

// Generate deployment group.
$generatorJava.clusterDeployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'deploymentMode', null, null, 'SHARED');

    res.needEmptyLine = true;

    return res;
};

// Generate discovery group.
$generatorJava.clusterDiscovery = function (disco, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'discovery', disco, 'localAddress');
    $generatorJava.property(res, 'discovery', disco, 'localPort', undefined, undefined, 47500);
    $generatorJava.property(res, 'discovery', disco, 'localPortRange', undefined, undefined, 100);

    if ($commonUtils.isDefinedAndNotEmpty(disco.addressResolver)) {
        $generatorJava.beanProperty(res, 'discovery', disco, 'addressResolver', 'addressResolver', disco.addressResolver, {}, true);
        res.needEmptyLine = false;
    }

    $generatorJava.property(res, 'discovery', disco, 'socketTimeout');
    $generatorJava.property(res, 'discovery', disco, 'ackTimeout');
    $generatorJava.property(res, 'discovery', disco, 'maxAckTimeout', undefined, undefined, 600000);
    $generatorJava.property(res, 'discovery', disco, 'networkTimeout', undefined, undefined, 5000);
    $generatorJava.property(res, 'discovery', disco, 'joinTimeout', undefined, undefined, 0);
    $generatorJava.property(res, 'discovery', disco, 'threadPriority', undefined, undefined, 10);
    $generatorJava.property(res, 'discovery', disco, 'heartbeatFrequency', undefined, undefined, 2000);
    $generatorJava.property(res, 'discovery', disco, 'maxMissedHeartbeats', undefined, undefined, 1);
    $generatorJava.property(res, 'discovery', disco, 'maxMissedClientHeartbeats', undefined, undefined, 5);
    $generatorJava.property(res, 'discovery', disco, 'topHistorySize', undefined, undefined, 100);

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

    $generatorJava.property(res, 'discovery', disco, 'reconnectCount', undefined, undefined, 10);
    $generatorJava.property(res, 'discovery', disco, 'statisticsPrintFrequency', undefined, undefined, 0);
    $generatorJava.property(res, 'discovery', disco, 'ipFinderCleanFrequency', undefined, undefined, 60000);

    if ($commonUtils.isDefinedAndNotEmpty(disco.authenticator)) {
        $generatorJava.beanProperty(res, 'discovery', disco, 'authenticator', 'authenticator', disco.authenticator, {}, true);
        res.needEmptyLine = false;
    }

    $generatorJava.property(res, 'discovery', disco, 'forceServerMode', undefined, undefined, false);
    $generatorJava.property(res, 'discovery', disco, 'clientReconnectDisabled', undefined, undefined, false);

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

            for (i = 1; i < cluster.includeEventTypes.length; i++) {
                res.needEmptyLine = true;

                res.append('    + EventType.' + cluster.includeEventTypes[i] + '.length');
            }

            res.line('];');

            res.needEmptyLine = true;

            res.line('int k = 0;');

            for (i = 0; i < cluster.includeEventTypes.length; i++) {
                res.needEmptyLine = true;

                var e = cluster.includeEventTypes[i];

                res.line('System.arraycopy(EventType.' + e + ', 0, events, k, EventType.' + e + '.length);');
                res.line('k += EventType.' + e + '.length;');
            }

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

// Generate PeerClassLoading group.
$generatorJava.clusterP2p = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

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

        res.append(varName + '.setIndexedTypes(');

        for (var i = 0; i < cache.indexedTypes.length; i++) {
            if (i > 0)
                res.append(', ');

            var pair = cache.indexedTypes[i];

            res.append($generatorJava.toJavaCode(res.importClass(pair.keyClass), 'class')).append(', ').append($generatorJava.toJavaCode(res.importClass(pair.valueClass), 'class'))
        }

        res.line(');');
    }

    $generatorJava.multiparamProperty(res, varName, cache, 'sqlFunctionClasses', 'class');

    $generatorJava.property(res, varName, cache, 'sqlEscapeAll');

    res.needEmptyLine = true;

    return res;
};

// Generate cache store group.
$generatorJava.cacheStore = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

        if (storeFactory) {
            var storeFactoryDesc = $generatorCommon.STORE_FACTORIES[cache.cacheStoreFactory.kind];

            var sfVarName = $commonUtils.toJavaName('storeFactory', cache.name);
            var dsVarName = 'none';

            if (storeFactory.dialect) {
                var dataSourceBean = storeFactory.dataSourceBean;

                dsVarName = $commonUtils.toJavaName('dataSource', dataSourceBean);

                if (!_.contains(res.datasources, dataSourceBean)) {
                    res.datasources.push(dataSourceBean);

                    var dsClsName = $generatorCommon.dataSourceClassName(storeFactory.dialect);

                    res.needEmptyLine = true;

                    $generatorJava.declareVariable(res, true, dsVarName, dsClsName);

                    switch (storeFactory.dialect) {
                        case 'DB2':
                            res.line(dsVarName + '.setServerName(_SERVER_NAME_);');
                            res.line(dsVarName + '.setPortNumber(_PORT_NUMBER_);');
                            res.line(dsVarName + '.setDatabaseName(_DATABASE_NAME_);');
                            res.line(dsVarName + '.setDriverType(_DRIVER_TYPE_);');
                            break;

                        default:
                            res.line(dsVarName + '.setURL(_URL_);');
                    }

                    res.line(dsVarName + '.setUsername(_User_Name_);');
                    res.line(dsVarName + '.setPassword(_Password_);');
                }
            }

            $generatorJava.beanProperty(res, varName, storeFactory, 'cacheStoreFactory', sfVarName,
                storeFactoryDesc.className, storeFactoryDesc.fields, true);

            if (dsVarName != 'none')
                res.line(sfVarName + '.setDataSource(' + dsVarName + ');');

            res.needEmptyLine = true;
        }
    }

    $generatorJava.property(res, varName, cache, 'loadPreviousValue');
    $generatorJava.property(res, varName, cache, 'readThrough');
    $generatorJava.property(res, varName, cache, 'writeThrough');

    res.needEmptyLine = true;

    $generatorJava.property(res, varName, cache, 'writeBehindEnabled');
    $generatorJava.property(res, varName, cache, 'writeBehindBatchSize');
    $generatorJava.property(res, varName, cache, 'writeBehindFlushSize');
    $generatorJava.property(res, varName, cache, 'writeBehindFlushFrequency');
    $generatorJava.property(res, varName, cache, 'writeBehindFlushThreadCount');

    res.needEmptyLine = true;

    return res;
};

// Generate cache concurrency group.
$generatorJava.cacheConcurrency = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, varName, cache, 'maxConcurrentAsyncOperations');
    $generatorJava.property(res, varName, cache, 'defaultLockTimeout');
    $generatorJava.property(res, varName, cache, 'atomicWriteOrderMode');

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
$generatorJava.metadataQueryFields = function (res, meta, fieldProperty) {
    var fields = meta[fieldProperty];

    if (fields && fields.length > 0) {
        $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, fieldProperty), fieldProperty, 'java.util.Map', 'java.util.LinkedHashMap', 'java.lang.String', 'java.lang.Class<?>');

        _.forEach(fields, function (field) {
            res.line(fieldProperty + '.put("' + field.name.toUpperCase() + '", ' + res.importClass(field.className) + '.class);');
        });

        res.needEmptyLine = true;

        res.line('typeMeta.' + $commonUtils.toJavaName('set', fieldProperty) + '(' + fieldProperty + ');');

        res.needEmptyLine = true;
    }
};

// Generate metadata groups.
$generatorJava.metadataGroups = function (res, meta) {
    var groups = meta.groups;

    if (groups && groups.length > 0) {
        _.forEach(groups, function (group) {
            var fields = group.fields;

            if (fields && fields.length > 0) {
                res.importClass('java.util.Map');
                res.importClass('java.util.LinkedHashMap');
                res.importClass('org.apache.ignite.lang.IgniteBiTuple');

                var varNew = !res.groups;

                res.needEmptyLine = true;

                res.line((varNew ? 'Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> ' : '') +
                    "groups = new LinkedHashMap<>();");

                res.needEmptyLine = true;

                if (varNew)
                    res.groups = true;

                varNew = !res.groupItems;

                res.line((varNew ? 'LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> ' : '') +
                    'groupItems = new LinkedHashMap<>();');

                res.needEmptyLine = true;

                if (varNew)
                    res.groupItems = true;

                _.forEach(fields, function (field) {
                    res.line('groupItems.put("' + field.name + '", ' +
                        'new IgniteBiTuple<Class<?>, Boolean>(' + res.importClass(field.className) + '.class, ' + field.direction + '));');
                });

                res.needEmptyLine = true;

                res.line('groups.put("' + group.name + '", groupItems);');
            }
        });

        res.needEmptyLine = true;

        res.line('typeMeta.setGroups(groups);');

        res.needEmptyLine = true;
    }
};

// Generate metadata db fields.
$generatorJava.metadataDatabaseFields = function (res, meta, fieldProperty) {
    var dbFields = meta[fieldProperty];

    if (dbFields && dbFields.length > 0) {
        res.needEmptyLine = true;

        $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, fieldProperty), fieldProperty, 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.CacheTypeFieldMetadata');

        _.forEach(dbFields, function (field) {
            res.line(fieldProperty + '.add(new CacheTypeFieldMetadata(' +
                '"' + field.databaseName + '", ' +
                'java.sql.Types.' + field.databaseType + ', ' +
                '"' + field.javaName + '", ' +
                field.javaType + '.class'
                + '));');
        });

        res.line('typeMeta.' + $commonUtils.toJavaName('set', fieldProperty) + '(' + fieldProperty + ');');

        res.needEmptyLine = true;
    }
};

// Generate metadata general group.
$generatorJava.metadataGeneral = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.classNameProperty(res, 'typeMeta', meta, 'keyType');
    $generatorJava.classNameProperty(res, 'typeMeta', meta, 'valueType');

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for query group.
$generatorJava.metadataQuery = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.metadataQueryFields(res, meta, 'queryFields');
    $generatorJava.metadataQueryFields(res, meta, 'ascendingFields');
    $generatorJava.metadataQueryFields(res, meta, 'descendingFields');

    $generatorJava.listProperty(res, 'typeMeta', meta, 'textFields');

    $generatorJava.metadataGroups(res, meta);

    res.needEmptyLine = true;

    return res;
};

// Generate metadata for store group.
$generatorJava.metadataStore = function (meta, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'typeMeta', meta, 'databaseSchema');
    $generatorJava.property(res, 'typeMeta', meta, 'databaseTable');

    if (!$dataStructures.isJavaBuildInClass(meta.keyType))
        $generatorJava.metadataDatabaseFields(res, meta, 'keyFields');

    $generatorJava.metadataDatabaseFields(res, meta, 'valueFields');

    res.needEmptyLine = true;

    return res;
};

// Generate cache type metadata config.
$generatorJava.cacheMetadata = function(meta, res) {
    $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, 'typeMeta'), 'typeMeta', 'org.apache.ignite.cache.CacheTypeMetadata');

    $generatorJava.metadataGeneral(meta, res);
    $generatorJava.metadataQuery(meta, res);
    $generatorJava.metadataStore(meta, res);

    res.emptyLineIfNeeded();
    res.line('types.add(typeMeta);');

    res.needEmptyLine = true;
};

// Generate cache type metadata configs.
$generatorJava.cacheMetadatas = function (metadatas, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    // Generate cache type metadata configs.
    if (metadatas && metadatas.length > 0) {
        $generatorJava.declareVariable(res, $generatorJava.needNewVariable(res, 'types'), 'types', 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.CacheTypeMetadata');

        _.forEach(metadatas, function (meta) {
            $generatorJava.cacheMetadata(meta, res);
        });

        res.line(varName + '.setTypeMetadata(types);');

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

    $generatorJava.cacheStore(cache, varName, res);

    $generatorJava.cacheConcurrency(cache, varName, res);

    $generatorJava.cacheRebalance(cache, varName, res);

    $generatorJava.cacheServerNearCache(cache, varName, res);

    $generatorJava.cacheStatistics(cache, varName, res);

    $generatorJava.cacheMetadatas(cache.metadatas, varName, res);
};

// Generate cluster caches.
$generatorJava.clusterCaches = function (caches, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (caches && caches.length > 0) {
        res.emptyLineIfNeeded();

        var names = [];

        _.forEach(caches, function (cache) {
            res.emptyLineIfNeeded();

            var cacheName = $commonUtils.toJavaName('cache', cache.name);

            $generatorJava.declareVariable(res, true, cacheName, 'org.apache.ignite.configuration.CacheConfiguration');

            $generatorJava.cache(cache, cacheName, res);

            names.push(cacheName);

            res.needEmptyLine = true;
        });

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

    var fields = (key || includeKeyFields) ? meta.keyFields.slice() : [];

    if (!key)
        fields.push.apply(fields, meta.valueFields);

    for (var fldIx = fields.length - 1; fldIx >= 0; fldIx --) {
        var field = fields[fldIx];

        var ix = _.findIndex(fields, function(fld) {
            return fld.javaName == field.javaName;
        });

        if (ix >= 0 && ix < fldIx)
            fields.splice(fldIx, 1);
    }

    // Generate fields declaration.
    _.forEach(fields, function (field) {
        var fldName = field.javaName;

        res.line('/** Value for ' + fldName + '. */');

        res.line('private ' + res.importClass(field.javaType) + ' ' + fldName + ';');

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

        for (fldIx = 0; fldIx < fields.length; fldIx ++) {
            field = fields[fldIx];

            res.line(res.importClass(field.javaType) + ' ' + field.javaName + (fldIx < fields.length - 1 ? ',' : ''))
        }

        res.endBlock(') {');

        res.startBlock();

        _.forEach(fields, function (field) {
            res.line('this.' + field.javaName +' = ' + field.javaName + ';');
        });

        res.endBlock('}');

        res.needEmptyLine = true;
    }

    // Generate getters and setters methods.
    _.forEach(fields, function (field) {
        var fldName = field.javaName;

        var fldType = res.importClass(field.javaType);

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

    _.forEach(fields, function (field) {
        res.needEmptyLine = true;

        var javaName = field.javaName;

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

    _.forEach(fields, function (field) {
        var javaName = field.javaName;

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

    if (fields.length > 0) {
        field = fields[0];

        res.startBlock('return \"' + type + ' [' + field.javaName + '=\" + ' + field.javaName + ' +', type);

        for (fldIx = 1; fldIx < fields.length; fldIx ++) {
            field = fields[fldIx];

            var javaName = field.javaName;

            res.line('\", ' + javaName + '=\" + ' + field.javaName + ' +');
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
 * @param caches TODO.
 */
$generatorJava.pojos = function (caches, useConstructor, includeKeyFields) {
    var metadataNames = [];

    $generatorJava.metadatas = [];

    for (var cacheIx = 0; cacheIx < caches.length; cacheIx ++) {
        var cache = caches[cacheIx];

        for (var metaIx = 0; metaIx < cache.metadatas.length; metaIx ++) {
            var meta = cache.metadatas[metaIx];

            // Skip already generated classes.
            if (metadataNames.indexOf(meta.valueType) < 0) {
                // Skip metadata without value fields.
                if ($commonUtils.isDefined(meta.valueFields) && meta.valueFields.length > 0) {
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

                    $generatorJava.metadatas.push(metadata);
                }

                metadataNames.push(meta.valueType);
            }
        }
    }
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
        cluster.sslContextFactory.keyStorePassword =
            ($commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.keyStoreFilePath)) ? '_Key_Storage_Password_' : undefined;

        cluster.sslContextFactory.trustStorePassword = ($commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.trustStoreFilePath)) ?
            '_Trust_Storage_Password_' : undefined;

        var propsDesc = $commonUtils.isDefinedAndNotEmpty(cluster.sslContextFactory.trustManagers) ?
            $generatorCommon.SSL_CONFIGURATION_TRUST_MANAGER_FACTORY.fields :
            $generatorCommon.SSL_CONFIGURATION_TRUST_FILE_FACTORY.fields;

        $generatorJava.beanProperty(res, 'cfg', cluster.sslContextFactory, 'sslContextFactory', 'sslContextFactory',
            'org.apache.ignite.ssl.SslContextFactory', propsDesc, false);
    }

    return res;
};

/**
 * Function to generate java code for cluster configuration.
 *
 * @param cluster Cluster to process.
 * @param javaClass If 'true' then generate factory class otherwise generate code snippet.
 * @param clientNearCfg Near cache configuration for client node.
 */
$generatorJava.cluster = function (cluster, javaClass, clientNearCfg) {
    var res = $generatorCommon.builder();

    if (cluster) {
        if (javaClass) {
            res.line('/**');
            res.line(' * ' + $generatorCommon.mainComment());
            res.line(' */');
            res.startBlock('public class IgniteConfigurationFactory {');
            res.line('/**');
            res.line(' * Configure grid.');
            res.line(' */');
            res.startBlock('public IgniteConfiguration createConfiguration() {');
        }

        $generatorJava.clusterGeneral(cluster, clientNearCfg, res);

        $generatorJava.clusterAtomics(cluster, res);

        $generatorJava.clusterCommunication(cluster, res);

        $generatorJava.clusterDeployment(cluster, res);

        $generatorJava.clusterEvents(cluster, res);

        $generatorJava.clusterMarshaller(cluster, res);

        $generatorJava.clusterMetrics(cluster, res);

        $generatorJava.clusterP2p(cluster, res);

        $generatorJava.clusterSwap(cluster, res);

        $generatorJava.clusterTime(cluster, res);

        $generatorJava.clusterPools(cluster, res);

        $generatorJava.clusterTransactions(cluster, res);

        $generatorJava.clusterCaches(cluster.caches, res);

        $generatorJava.clusterSsl(cluster, res);

        if (javaClass) {
            res.line('return cfg;');
            res.endBlock('}');
            res.endBlock('}');

            return res.generateImports() + '\n\n' + res.asString();
        }
    }

    return res.asString();
};

// For server side we should export Java code generation entry point.
if (typeof window === 'undefined') {
    module.exports = $generatorJava;
}
