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
const $generatorJava = {};

/**
 * Translate some value to valid java code.
 *
 * @param val Value to convert.
 * @param type Value type.
 * @returns {*} String with value that will be valid for java.
 */
$generatorJava.toJavaCode = function (val, type) {
    if (val === null)
        return 'null';

    if (type === 'raw')
        return val;

    if (type === 'class')
        return val + '.class';

    if (type === 'float')
        return val + 'f';

    if (type === 'path')
        return '"' + val.replace(/\\/g, '\\\\') + '"';

    if (type)
        return type + '.' + val;

    if (typeof(val) === 'string')
        return '"' + val.replace('"', '\\"') + '"';

    if (typeof(val) === 'number' || typeof(val) === 'boolean')
        return '' + val;

    return 'Unknown type: ' + typeof(val) + ' (' + val + ')';
};

/**
 * @param propName Property name.
 * @param setterName Optional concrete setter name.
 * @returns Property setter with name by java conventions.
 */
$generatorJava.setterName = function (propName, setterName) {
    return setterName ? setterName : $generatorCommon.toJavaName('set', propName);
};

// Add constructor argument
$generatorJava.constructorArg = function (obj, propName, dflt, notFirst, opt) {
    var v = (obj ? obj[propName] : null) || dflt;

    if ($generatorCommon.isDefinedAndNotEmpty(v))
        return (notFirst ? ', ' : '') + $generatorJava.toJavaCode(v);
    else if (!opt)
        return notFirst ? ', null' : 'null';
    else
        return '';
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
 * @param subClass If 'true' then variable will be declared as anonymous subclass.
 */
$generatorJava.declareVariable = function (res, varName, varFullType, varFullActualType, varFullGenericType1, varFullGenericType2, subClass) {
    res.emptyLineIfNeeded();

    var varType = res.importClass(varFullType);

    var varNew = !res.vars[varName];

    if (varNew)
        res.vars[varName] = true;

    if (varFullActualType && varFullGenericType1) {
        var varActualType = res.importClass(varFullActualType);
        var varGenericType1 = res.importClass(varFullGenericType1);
        var varGenericType2 = null;

        if (varFullGenericType2)
            varGenericType2 = res.importClass(varFullGenericType2);

        res.line((varNew ? (varType + '<' + varGenericType1 + (varGenericType2 ? ', ' + varGenericType2 : '') + '> ') : '') +
            varName + ' = new ' + varActualType + '<>();');
    }
    else
        res.line((varNew ? (varType + ' ') : '') + varName + ' = new ' + varType + '()' + (subClass ? ' {' : ';'));

    if (!subClass)
        res.needEmptyLine = true;

    return varName;
};

/**
 * Add local variable declaration.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param varFullType Variable full class name to be added to imports.
 */
$generatorJava.declareVariableLocal = function (res, varName, varFullType) {
    var varType = res.importClass(varFullType);

    res.line(varType + ' ' + varName + ' = new ' + varType + '();');

    res.needEmptyLine = true;
};

/**
 * Add custom variable declaration.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param varFullType Variable full class name to be added to imports.
 * @param varExpr Custom variable creation expression.
 * @param modifier Additional variable modifier.
 */
$generatorJava.declareVariableCustom = function (res, varName, varFullType, varExpr, modifier) {
    var varType = res.importClass(varFullType);

    var varNew = !res.vars[varName];

    if (varNew)
        res.vars[varName] = true;

    res.line((varNew ? ((modifier ? modifier + ' ' : '') + varType + ' ') : '') + varName + ' = ' + varExpr + ';');

    res.needEmptyLine = true;
};

/**
 * Add array variable declaration.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param varFullType Variable full class name to be added to imports.
 * @param length Array length.
 */
$generatorJava.declareVariableArray = function (res, varName, varFullType, length) {
    var varType = res.importClass(varFullType);

    var varNew = !res.vars[varName];

    if (varNew)
        res.vars[varName] = true;

    res.line((varNew ? (varType + '[] ') : '') + varName + ' = new ' + varType + '[' + length + '];');

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

    if ($generatorCommon.isDefinedAndNotEmpty(val)) {
        var missDflt = _.isNil(dflt);

        // Add to result if no default provided or value not equals to default.
        if (missDflt || (!missDflt && val !== dflt)) {
            res.line(varName + '.' + $generatorJava.setterName(propName, setterName) +
                '(' + $generatorJava.toJavaCode(val, dataType) + ');');

            return true;
        }
    }

    return false;
};

/**
 * Add enum property via setter / property name.
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param obj Source object with data.
 * @param propName Property name to take from source object.
 * @param dataType Name of enum class
 * @param setterName Optional special setter name.
 * @param dflt Optional default value.
 */
$generatorJava.enumProperty = function (res, varName, obj, propName, dataType, setterName, dflt) {
    var val = obj[propName];

    if ($generatorCommon.isDefinedAndNotEmpty(val)) {
        var missDflt = _.isNil(dflt);

        // Add to result if no default provided or value not equals to default.
        if (missDflt || (!missDflt && val !== dflt)) {
            res.line(varName + '.' + $generatorJava.setterName(propName, setterName) +
                '(' + $generatorJava.toJavaCode(val, dataType ? res.importClass(dataType) : null) + ');');

            return true;
        }
    }

    return false;
};

// Add property for class name.
$generatorJava.classNameProperty = function (res, varName, obj, propName) {
    var val = obj[propName];

    if (!_.isNil(val)) {
        res.line(varName + '.' + $generatorJava.setterName(propName) +
            '("' + $generatorCommon.JavaTypes.fullClassName(val) + '");');
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
                    return $generatorJava.toJavaCode(v, dataType);
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
                return 'new ' + res.importClass(v) + '()';
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
 *
 * @param res Resulting output with generated code.
 * @param varName Variable name.
 * @param bean
 * @param beanPropName Bean property name.
 * @param beanVarName
 * @param beanClass Bean class.
 * @param props
 * @param createBeanAlthoughNoProps If 'true' then create empty bean.
 */
$generatorJava.beanProperty = function (res, varName, bean, beanPropName, beanVarName, beanClass, props, createBeanAlthoughNoProps) {
    if (bean && $generatorCommon.hasProperty(bean, props)) {
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
                            $generatorJava.enumProperty(res, beanVarName, bean, propName, descr.enumClass, descr.setterName, descr.dflt);
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
                            if ($generatorCommon.isDefinedAndNotEmpty(bean[propName]))
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

    $generatorJava.property(res, 'cfg', cluster, 'localHost');
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

            case 'ZooKeeper':
                var finderVar = 'ipFinder';

                $generatorJava.declareVariable(res, 'ipFinder', 'org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder');

                if (d.ZooKeeper) {
                    if ($generatorCommon.isDefinedAndNotEmpty(d.ZooKeeper.curator))
                        res.line(finderVar + '.setCurator(new ' + res.importClass(d.ZooKeeper.curator) + '());');

                    $generatorJava.property(res, finderVar, d.ZooKeeper, 'zkConnectionString');

                    if (d.ZooKeeper.retryPolicy && d.ZooKeeper.retryPolicy.kind) {
                        var kind = d.ZooKeeper.retryPolicy.kind;
                        var retryPolicy = d.ZooKeeper.retryPolicy[kind];

                        switch (kind) {
                            case 'ExponentialBackoff':
                                res.line(finderVar + '.setRetryPolicy(new ' + res.importClass('org.apache.curator.retry.ExponentialBackoffRetry') + '(' +
                                    $generatorJava.constructorArg(retryPolicy, 'baseSleepTimeMs', 1000) +
                                    $generatorJava.constructorArg(retryPolicy, 'maxRetries', 10, true) +
                                    $generatorJava.constructorArg(retryPolicy, 'maxSleepMs', null, true, true) + '));');

                                break;

                            case 'BoundedExponentialBackoff':
                                res.line(finderVar + '.setRetryPolicy(new ' + res.importClass('org.apache.curator.retry.BoundedExponentialBackoffRetry') + '(' +
                                    $generatorJava.constructorArg(retryPolicy, 'baseSleepTimeMs', 1000) +
                                    $generatorJava.constructorArg(retryPolicy, 'maxSleepTimeMs', 2147483647, true) +
                                    $generatorJava.constructorArg(retryPolicy, 'maxRetries', 10, true) + '));');

                                break;

                            case 'UntilElapsed':
                                res.line(finderVar + '.setRetryPolicy(new ' + res.importClass('org.apache.curator.retry.RetryUntilElapsed') + '(' +
                                    $generatorJava.constructorArg(retryPolicy, 'maxElapsedTimeMs', 60000) +
                                    $generatorJava.constructorArg(retryPolicy, 'sleepMsBetweenRetries', 1000, true) + '));');

                                break;

                            case 'NTimes':
                                res.line(finderVar + '.setRetryPolicy(new ' + res.importClass('org.apache.curator.retry.RetryNTimes') + '(' +
                                    $generatorJava.constructorArg(retryPolicy, 'n', 10) +
                                    $generatorJava.constructorArg(retryPolicy, 'sleepMsBetweenRetries', 1000, true) + '));');

                                break;

                            case 'OneTime':
                                res.line(finderVar + '.setRetryPolicy(new ' + res.importClass('org.apache.curator.retry.RetryOneTime') + '(' +
                                    $generatorJava.constructorArg(retryPolicy, 'sleepMsBetweenRetry', 1000) + '));');

                                break;

                            case 'Forever':
                                res.line(finderVar + '.setRetryPolicy(new ' + res.importClass('org.apache.curator.retry.RetryForever') + '(' +
                                    $generatorJava.constructorArg(retryPolicy, 'retryIntervalMs', 1000) + '));');

                                break;

                            case 'Custom':
                                if (retryPolicy && $generatorCommon.isDefinedAndNotEmpty(retryPolicy.className))
                                    res.line(finderVar + '.setRetryPolicy(new ' + res.importClass(retryPolicy.className) + '());');

                                break;
                        }
                    }

                    $generatorJava.property(res, finderVar, d.ZooKeeper, 'basePath', null, null, '/services');
                    $generatorJava.property(res, finderVar, d.ZooKeeper, 'serviceName', null, null, 'ignite');
                    $generatorJava.property(res, finderVar, d.ZooKeeper, 'allowDuplicateRegistrations', null, null, false);
                }

                res.line('discovery.setIpFinder(ipFinder);');

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
$generatorJava.clusterAtomics = function (atomics, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.hasAtLeastOneProperty(atomics, ['cacheMode', 'atomicSequenceReserveSize', 'backups'])) {
        res.startSafeBlock();

        $generatorJava.declareVariable(res, 'atomicCfg', 'org.apache.ignite.configuration.AtomicConfiguration');

        $generatorJava.enumProperty(res, 'atomicCfg', atomics, 'cacheMode', 'org.apache.ignite.cache.CacheMode', null, 'PARTITIONED');

        var cacheMode = atomics.cacheMode ? atomics.cacheMode : 'PARTITIONED';

        var hasData = cacheMode !== 'PARTITIONED';

        hasData = $generatorJava.property(res, 'atomicCfg', atomics, 'atomicSequenceReserveSize', null, null, 1000) || hasData;

        if (cacheMode === 'PARTITIONED')
            hasData = $generatorJava.property(res, 'atomicCfg', atomics, 'backups', null, null, 0) || hasData;

        res.needEmptyLine = true;

        res.line('cfg.setAtomicConfiguration(atomicCfg);');

        res.needEmptyLine = true;

        if (!hasData)
            res.rollbackSafeBlock();
    }

    return res;
};

// Generate binary group.
$generatorJava.clusterBinary = function (binary, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.binaryIsDefined(binary)) {
        var varName = 'binary';

        $generatorJava.declareVariable(res, varName, 'org.apache.ignite.configuration.BinaryConfiguration');

        if ($generatorCommon.isDefinedAndNotEmpty(binary.idMapper))
            res.line(varName + '.setIdMapper(new ' + res.importClass(binary.idMapper) + '());');

        if ($generatorCommon.isDefinedAndNotEmpty(binary.nameMapper))
            res.line(varName + '.setNameMapper(new ' + res.importClass(binary.nameMapper) + '());');

        if ($generatorCommon.isDefinedAndNotEmpty(binary.serializer))
            res.line(varName + '.setSerializer(new ' + res.importClass(binary.serializer) + '());');

        res.needEmptyLine = $generatorCommon.isDefinedAndNotEmpty(binary.idMapper) || $generatorCommon.isDefinedAndNotEmpty(binary.serializer);

        if ($generatorCommon.isDefinedAndNotEmpty(binary.typeConfigurations)) {
            var arrVar = 'types';

            $generatorJava.declareVariable(res, arrVar, 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.binary.BinaryTypeConfiguration');

            _.forEach(binary.typeConfigurations, function (type) {
                if ($generatorCommon.isDefinedAndNotEmpty(type.typeName)) {
                    // TODO IGNITE-2269 Replace using of separated methods for binary type configurations to extended constructors.
                    res.line(arrVar + '.add(' + $generatorJava.binaryTypeFunctionName(type.typeName) + '());');
                }
            });

            res.needEmptyLine = true;

            res.line(varName + '.setTypeConfigurations(' + arrVar + ');');

            res.needEmptyLine = true;
        }

        $generatorJava.property(res, varName, binary, 'compactFooter', undefined, undefined, true);

        res.needEmptyLine = true;

        res.line('cfg.setBinaryConfiguration(' + varName + ');');

        res.needEmptyLine = true;
    }

    return res;
};

// TODO IGNITE-2269 Remove specified methods after implamentation of extended constructors.
// Construct binary type configuration factory method name.
$generatorJava.binaryTypeFunctionName = function (typeName) {
    var dotIdx = typeName.lastIndexOf('.');

    var shortName = dotIdx > 0 ? typeName.substr(dotIdx + 1) : typeName;

    return $generatorCommon.toJavaName('binaryType', shortName);
};

// TODO IGNITE-2269 Remove specified methods after implamentation of extended constructors.
// Generate factory method for specified BinaryTypeConfiguration.
$generatorJava.binaryTypeConfiguration = function (type, res) {
    var typeName = type.typeName;

    res.line('/**');
    res.line(' * Create binary type configuration for ' + typeName + '.');
    res.line(' *');
    res.line(' * @return Configured binary type.');
    res.line(' */');
    res.startBlock('private static BinaryTypeConfiguration ' + $generatorJava.binaryTypeFunctionName(typeName) + '() {');

    $generatorJava.resetVariables(res);

    var typeVar = 'typeCfg';

    $generatorJava.declareVariable(res, typeVar, 'org.apache.ignite.binary.BinaryTypeConfiguration');

    $generatorJava.property(res, typeVar, type, 'typeName');

    if ($generatorCommon.isDefinedAndNotEmpty(type.idMapper))
        res.line(typeVar + '.setIdMapper(new ' + res.importClass(type.idMapper) + '());');

    if ($generatorCommon.isDefinedAndNotEmpty(type.nameMapper))
        res.line(typeVar + '.setNameMapper(new ' + res.importClass(type.nameMapper) + '());');

    if ($generatorCommon.isDefinedAndNotEmpty(type.serializer))
        res.line(typeVar + '.setSerializer(new ' + res.importClass(type.serializer) + '());');

    $generatorJava.property(res, typeVar, type, 'enum', null, null, false);

    res.needEmptyLine = true;

    res.line('return ' + typeVar + ';');
    res.endBlock('}');

    res.needEmptyLine = true;
};

// TODO IGNITE-2269 Remove specified methods after implamentation of extended constructors.
// Generates binary type configuration factory methods.
$generatorJava.binaryTypeConfigurations = function (binary, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!_.isNil(binary)) {
        _.forEach(binary.typeConfigurations, function (type) {
            $generatorJava.binaryTypeConfiguration(type, res);
        });
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
    $generatorJava.property(res, 'cfg', cluster, 'discoveryStartupDelay', null, null, 60000);

    res.needEmptyLine = true;

    return res;
};

// Generate REST access group.
$generatorJava.clusterConnector = function (connector, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!_.isNil(connector) && connector.enabled) {
        var cfg = _.cloneDeep($generatorCommon.CONNECTOR_CONFIGURATION);

        if (connector.sslEnabled) {
            cfg.fields.sslClientAuth = {dflt: false};
            cfg.fields.sslFactory = {type: 'bean'};
        }

        $generatorJava.beanProperty(res, 'cfg', connector, 'connectorConfiguration', 'clientCfg',
            cfg.className, cfg.fields, true);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate deployment group.
$generatorJava.clusterDeployment = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.enumProperty(res, 'cfg', cluster, 'deploymentMode', 'org.apache.ignite.configuration.DeploymentMode', null, 'SHARED');

    res.softEmptyLine();

    var p2pEnabled = cluster.peerClassLoadingEnabled;

    if (!_.isNil(p2pEnabled)) {
        $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingEnabled', null, null, false);

        if (p2pEnabled) {
            $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingMissedResourcesCacheSize', null, null, 100);
            $generatorJava.property(res, 'cfg', cluster, 'peerClassLoadingThreadPoolSize', null, null, 2);
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

    if (disco) {
        $generatorJava.property(res, 'discovery', disco, 'localAddress');
        $generatorJava.property(res, 'discovery', disco, 'localPort', null, null, 47500);
        $generatorJava.property(res, 'discovery', disco, 'localPortRange', null, null, 100);

        if ($generatorCommon.isDefinedAndNotEmpty(disco.addressResolver)) {
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
        $generatorJava.property(res, 'discovery', disco, 'topHistorySize', null, null, 1000);

        if ($generatorCommon.isDefinedAndNotEmpty(disco.listener)) {
            $generatorJava.beanProperty(res, 'discovery', disco, 'listener', 'listener', disco.listener, {}, true);
            res.needEmptyLine = false;
        }

        if ($generatorCommon.isDefinedAndNotEmpty(disco.dataExchange)) {
            $generatorJava.beanProperty(res, 'discovery', disco, 'dataExchange', 'dataExchange', disco.dataExchange, {}, true);
            res.needEmptyLine = false;
        }

        if ($generatorCommon.isDefinedAndNotEmpty(disco.metricsProvider)) {
            $generatorJava.beanProperty(res, 'discovery', disco, 'metricsProvider', 'metricsProvider', disco.metricsProvider, {}, true);
            res.needEmptyLine = false;
        }

        $generatorJava.property(res, 'discovery', disco, 'reconnectCount', null, null, 10);
        $generatorJava.property(res, 'discovery', disco, 'statisticsPrintFrequency', null, null, 0);
        $generatorJava.property(res, 'discovery', disco, 'ipFinderCleanFrequency', null, null, 60000);

        if ($generatorCommon.isDefinedAndNotEmpty(disco.authenticator)) {
            $generatorJava.beanProperty(res, 'discovery', disco, 'authenticator', 'authenticator', disco.authenticator, {}, true);
            res.needEmptyLine = false;
        }

        $generatorJava.property(res, 'discovery', disco, 'forceServerMode', null, null, false);
        $generatorJava.property(res, 'discovery', disco, 'clientReconnectDisabled', null, null, false);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate events group.
$generatorJava.clusterEvents = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.includeEventTypes && cluster.includeEventTypes.length > 0) {
        res.emptyLineIfNeeded();

        var evtGrps = angular.element(document.getElementById('app')).injector().get('igniteEventGroups');

        var evtGrpDscr = _.find(evtGrps, {value: cluster.includeEventTypes[0]});

        var evt = res.importStatic(evtGrpDscr.class + '.' + evtGrpDscr.value);

        if (cluster.includeEventTypes.length === 1)
            res.line('cfg.setIncludeEventTypes(' + evt + ');');
        else {
            _.forEach(cluster.includeEventTypes, function(value, ix) {
                var evtGrpDscr = _.find(evtGrps, {value: value});

                var evt = res.importStatic(evtGrpDscr.class + '.' + evtGrpDscr.value);

                if (ix === 0)
                    res.line('int[] events = new int[' + evt + '.length');
                else
                    res.line('    + ' + evt + '.length');
            });

            res.line('];');

            res.needEmptyLine = true;

            res.line('int k = 0;');

            _.forEach(cluster.includeEventTypes, function(value, idx) {
                res.needEmptyLine = true;

                var evtGrpDscr = _.find(evtGrps, {value: value});

                evt = res.importStatic(evtGrpDscr.class + '.' + value);

                res.line('System.arraycopy(' + evt + ', 0, events, k, ' + evt + '.length);');

                if (idx < cluster.includeEventTypes.length - 1)
                    res.line('k += ' + evt + '.length;');
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
    $generatorJava.property(res, 'cfg', cluster, 'marshallerCacheKeepAliveTime', null, null, 10000);
    $generatorJava.property(res, 'cfg', cluster, 'marshallerCacheThreadPoolSize', null, 'setMarshallerCachePoolSize');

    res.needEmptyLine = true;

    return res;
};

// Generate metrics group.
$generatorJava.clusterMetrics = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'cfg', cluster, 'metricsExpireTime');
    $generatorJava.property(res, 'cfg', cluster, 'metricsHistorySize', null, null, 10000);
    $generatorJava.property(res, 'cfg', cluster, 'metricsLogFrequency', null, null, 60000);
    $generatorJava.property(res, 'cfg', cluster, 'metricsUpdateFrequency', null, null, 2000);

    res.needEmptyLine = true;

    return res;
};

// Generate swap group.
$generatorJava.clusterSwap = function (cluster, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind === 'FileSwapSpaceSpi') {
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

    $generatorJava.property(res, 'cfg', cluster, 'clockSyncSamples', null, null, 8);
    $generatorJava.property(res, 'cfg', cluster, 'clockSyncFrequency', null, null, 120000);
    $generatorJava.property(res, 'cfg', cluster, 'timeServerPortBase', null, null, 31100);
    $generatorJava.property(res, 'cfg', cluster, 'timeServerPortRange', null, null, 100);

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
$generatorJava.clusterTransactions = function (transactionConfiguration, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.beanProperty(res, 'cfg', transactionConfiguration, 'transactionConfiguration',
        'transactionConfiguration', $generatorCommon.TRANSACTION_CONFIGURATION.className,
        $generatorCommon.TRANSACTION_CONFIGURATION.fields, false);

    return res;
};

// Generate cache general group.
$generatorJava.cacheGeneral = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!varName)
        varName = $generatorJava.nextVariableName('cache', cache);

    $generatorJava.property(res, varName, cache, 'name');

    $generatorJava.enumProperty(res, varName, cache, 'cacheMode', 'org.apache.ignite.cache.CacheMode');
    $generatorJava.enumProperty(res, varName, cache, 'atomicityMode', 'org.apache.ignite.cache.CacheAtomicityMode');

    if (cache.cacheMode === 'PARTITIONED')
        $generatorJava.property(res, varName, cache, 'backups');

    $generatorJava.property(res, varName, cache, 'readFromBackup');
    $generatorJava.property(res, varName, cache, 'copyOnRead');

    if (cache.cacheMode === 'PARTITIONED' && cache.atomicityMode === 'TRANSACTIONAL')
        $generatorJava.property(res, varName, cache, 'invalidate');

    res.needEmptyLine = true;

    return res;
};

// Generate cache memory group.
$generatorJava.cacheMemory = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!varName)
        varName = $generatorJava.nextVariableName('cache', cache);

    $generatorJava.enumProperty(res, varName, cache, 'memoryMode', 'org.apache.ignite.cache.CacheMemoryMode', null, 'ONHEAP_TIERED');
    $generatorJava.property(res, varName, cache, 'offHeapMaxMemory', null, null, -1);

    res.softEmptyLine();

    $generatorJava.evictionPolicy(res, varName, cache.evictionPolicy, 'evictionPolicy');

    $generatorJava.property(res, varName, cache, 'startSize', null, null, 1500000);
    $generatorJava.property(res, varName, cache, 'swapEnabled', null, null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate cache query & indexing group.
$generatorJava.cacheQuery = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!varName)
        varName = $generatorJava.nextVariableName('cache', cache);

    $generatorJava.property(res, varName, cache, 'sqlSchema');
    $generatorJava.property(res, varName, cache, 'sqlOnheapRowCacheSize', null, null, 10240);
    $generatorJava.property(res, varName, cache, 'longQueryWarningTimeout', null, null, 3000);

    res.softEmptyLine();

    $generatorJava.multiparamProperty(res, varName, cache, 'sqlFunctionClasses', 'class');

    res.softEmptyLine();

    $generatorJava.property(res, varName, cache, 'snapshotableIndex', null, null, false);
    $generatorJava.property(res, varName, cache, 'sqlEscapeAll', null, null, false);

    res.needEmptyLine = true;

    return res;
};

/**
 * Generate cache store datasource.
 *
 * @param storeFactory Factory to generate data source for.
 * @param res Resulting output with generated code.
 */
$generatorJava.cacheStoreDataSource = function (storeFactory, res) {
    var dialect = storeFactory.connectVia ? (storeFactory.connectVia === 'DataSource' ? storeFactory.dialect : null) : storeFactory.dialect;

    if (dialect) {
        var varName = 'dataSource';

        var dataSourceBean = storeFactory.dataSourceBean;

        var varType = res.importClass($generatorCommon.dataSourceClassName(dialect));

        res.line('public static final ' + varType + ' INSTANCE_' + dataSourceBean + ' = create' + dataSourceBean + '();');

        res.needEmptyLine = true;

        res.startBlock('private static ' + varType + ' create' + dataSourceBean + '() {');
        if (dialect === 'Oracle')
            res.startBlock('try {');

        $generatorJava.resetVariables(res);

        $generatorJava.declareVariable(res, varName, varType);

        switch (dialect) {
            case 'Generic':
                res.line(varName + '.setJdbcUrl(props.getProperty("' + dataSourceBean + '.jdbc.url"));');

                break;

            case 'DB2':
                res.line(varName + '.setServerName(props.getProperty("' + dataSourceBean + '.jdbc.server_name"));');
                res.line(varName + '.setPortNumber(Integer.valueOf(props.getProperty("' + dataSourceBean + '.jdbc.port_number")));');
                res.line(varName + '.setDatabaseName(props.getProperty("' + dataSourceBean + '.jdbc.database_name"));');
                res.line(varName + '.setDriverType(Integer.valueOf(props.getProperty("' + dataSourceBean + '.jdbc.driver_type")));');

                break;

            case 'PostgreSQL':
                res.line(varName + '.setUrl(props.getProperty("' + dataSourceBean + '.jdbc.url"));');

                break;

            default:
                res.line(varName + '.setURL(props.getProperty("' + dataSourceBean + '.jdbc.url"));');
        }

        res.line(varName + '.setUser(props.getProperty("' + dataSourceBean + '.jdbc.username"));');
        res.line(varName + '.setPassword(props.getProperty("' + dataSourceBean + '.jdbc.password"));');

        res.needEmptyLine = true;

        res.line('return dataSource;');

        if (dialect === 'Oracle') {
            res.endBlock('}');
            res.startBlock('catch (' + res.importClass('java.sql.SQLException') + ' ex) {');
            res.line('throw new Error(ex);');
            res.endBlock('}');
        }

        res.endBlock('}');

        res.needEmptyLine = true;

        return dataSourceBean;
    }

    return null;
};

$generatorJava.clusterDataSources = function (caches, res) {
    if (!res)
        res = $generatorCommon.builder();

    var datasources = [];

    var storeFound = false;

    _.forEach(caches, function (cache) {
        var factoryKind = cache.cacheStoreFactory.kind;

        var storeFactory = cache.cacheStoreFactory[factoryKind];

        if (storeFactory) {
            var beanClassName = $generatorJava.dataSourceClassName(res, storeFactory);

            if (beanClassName && !_.includes(datasources, beanClassName)) {
                datasources.push(beanClassName);

                if (factoryKind === 'CacheJdbcPojoStoreFactory' || factoryKind === 'CacheJdbcBlobStoreFactory') {
                    if (!storeFound) {
                        res.line('/** Helper class for datasource creation. */');
                        res.startBlock('public static class DataSources {');

                        storeFound = true;
                    }

                    $generatorJava.cacheStoreDataSource(storeFactory, res);
                }
            }
        }
    });

    if (storeFound) {
        res.endBlock('}');
    }

    return res;
};

/**
 * Generate cache store group.
 *
 * @param cache Cache descriptor.
 * @param domains Domain model descriptors.
 * @param cacheVarName Cache variable name.
 * @param res Resulting output with generated code.
 * @returns {*} Java code for cache store configuration.
 */
$generatorJava.cacheStore = function (cache, domains, cacheVarName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!cacheVarName)
        cacheVarName = $generatorJava.nextVariableName('cache', cache);

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
        var factoryKind = cache.cacheStoreFactory.kind;

        var storeFactory = cache.cacheStoreFactory[factoryKind];

        if (storeFactory) {
            var storeFactoryDesc = $generatorCommon.STORE_FACTORIES[factoryKind];

            var varName = 'storeFactory' + storeFactoryDesc.suffix;

            if (factoryKind === 'CacheJdbcPojoStoreFactory') {
                // Generate POJO store factory.
                $generatorJava.declareVariable(res, varName, 'org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory', null, null, null, true);
                res.deep++;

                res.line('/** {@inheritDoc} */');
                res.startBlock('@Override public ' + res.importClass('org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore') + ' create() {');

                res.line('setDataSource(DataSources.INSTANCE_' + storeFactory.dataSourceBean + ');');

                res.needEmptyLine = true;

                res.line('return super.create();');
                res.endBlock('}');
                res.endBlock('};');

                res.needEmptyLine = true;

                res.line(varName + '.setDialect(new ' +
                    res.importClass($generatorCommon.jdbcDialectClassName(storeFactory.dialect)) + '());');

                res.needEmptyLine = true;

                var domainConfigs = _.filter(domains, function (domain) {
                    return $generatorCommon.domainQueryMetadata(domain) === 'Configuration' &&
                        $generatorCommon.isDefinedAndNotEmpty(domain.databaseTable);
                });

                if ($generatorCommon.isDefinedAndNotEmpty(domainConfigs)) {
                    $generatorJava.declareVariable(res, 'jdbcTypes', 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.store.jdbc.JdbcType');

                    res.needEmptyLine = true;

                    _.forEach(domainConfigs, function (domain) {
                        if ($generatorCommon.isDefinedAndNotEmpty(domain.databaseTable))
                            res.line('jdbcTypes.add(jdbcType' + $generatorJava.extractType(domain.valueType) + '(' + cacheVarName + '.getName()));');
                    });

                    res.needEmptyLine = true;

                    res.line(varName + '.setTypes(jdbcTypes.toArray(new JdbcType[jdbcTypes.size()]));');

                    res.needEmptyLine = true;
                }

                res.line(cacheVarName + '.setCacheStoreFactory(' + varName + ');');
            }
            else if (factoryKind === 'CacheJdbcBlobStoreFactory') {
                // Generate POJO store factory.
                $generatorJava.declareVariable(res, varName, 'org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory', null, null, null, storeFactory.connectVia === 'DataSource');

                if (storeFactory.connectVia === 'DataSource') {
                    res.deep++;

                    res.line('/** {@inheritDoc} */');
                    res.startBlock('@Override public ' + res.importClass('org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore') + ' create() {');

                    res.line('setDataSource(DataSources.INSTANCE_' + storeFactory.dataSourceBean + ');');

                    res.needEmptyLine = true;

                    res.line('return super.create();');
                    res.endBlock('}');
                    res.endBlock('};');

                    res.needEmptyLine = true;

                    $generatorJava.property(res, varName, storeFactory, 'initSchema');
                    $generatorJava.property(res, varName, storeFactory, 'createTableQuery');
                    $generatorJava.property(res, varName, storeFactory, 'loadQuery');
                    $generatorJava.property(res, varName, storeFactory, 'insertQuery');
                    $generatorJava.property(res, varName, storeFactory, 'updateQuery');
                    $generatorJava.property(res, varName, storeFactory, 'deleteQuery');
                }
                else {
                    $generatorJava.property(res, varName, storeFactory, 'connectionUrl');

                    if (storeFactory.user) {
                        $generatorJava.property(res, varName, storeFactory, 'user');
                        res.line(varName + '.setPassword(props.getProperty("ds.' + storeFactory.user + '.password"));');
                    }
                }

                res.needEmptyLine = true;

                res.line(cacheVarName + '.setCacheStoreFactory(' + varName + ');');
            }
            else
                $generatorJava.beanProperty(res, cacheVarName, storeFactory, 'cacheStoreFactory', varName,
                    storeFactoryDesc.className, storeFactoryDesc.fields, true);

            res.needEmptyLine = true;
        }
    }

    res.softEmptyLine();

    $generatorJava.property(res, cacheVarName, cache, 'storeKeepBinary', null, null, false);
    $generatorJava.property(res, cacheVarName, cache, 'loadPreviousValue', null, null, false);
    $generatorJava.property(res, cacheVarName, cache, 'readThrough', null, null, false);
    $generatorJava.property(res, cacheVarName, cache, 'writeThrough', null, null, false);

    res.softEmptyLine();

    if (cache.writeBehindEnabled) {
        $generatorJava.property(res, cacheVarName, cache, 'writeBehindEnabled', null, null, false);
        $generatorJava.property(res, cacheVarName, cache, 'writeBehindBatchSize', null, null, 512);
        $generatorJava.property(res, cacheVarName, cache, 'writeBehindFlushSize', null, null, 10240);
        $generatorJava.property(res, cacheVarName, cache, 'writeBehindFlushFrequency', null, null, 5000);
        $generatorJava.property(res, cacheVarName, cache, 'writeBehindFlushThreadCount', null, null, 1);
    }

    res.needEmptyLine = true;

    return res;
};

// Generate cache concurrency group.
$generatorJava.cacheConcurrency = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!varName)
        varName = $generatorJava.nextVariableName('cache', cache);

    $generatorJava.property(res, varName, cache, 'maxConcurrentAsyncOperations', null, null, 500);
    $generatorJava.property(res, varName, cache, 'defaultLockTimeout', null, null, 0);
    $generatorJava.enumProperty(res, varName, cache, 'atomicWriteOrderMode', 'org.apache.ignite.cache.CacheAtomicWriteOrderMode');
    $generatorJava.enumProperty(res, varName, cache, 'writeSynchronizationMode', 'org.apache.ignite.cache.CacheWriteSynchronizationMode', null, null, "PRIMARY_SYNC");

    res.needEmptyLine = true;

    return res;
};

// Generate cache rebalance group.
$generatorJava.cacheRebalance = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!varName)
        varName = $generatorJava.nextVariableName('cache', cache);

    if (cache.cacheMode !== 'LOCAL') {
        $generatorJava.enumProperty(res, varName, cache, 'rebalanceMode', 'org.apache.ignite.cache.CacheRebalanceMode', null, 'ASYNC');
        $generatorJava.property(res, varName, cache, 'rebalanceThreadPoolSize', null, null, 1);
        $generatorJava.property(res, varName, cache, 'rebalanceBatchSize', null, null, 524288);
        $generatorJava.property(res, varName, cache, 'rebalanceBatchesPrefetchCount', null, null, 2);
        $generatorJava.property(res, varName, cache, 'rebalanceOrder', null, null, 0);
        $generatorJava.property(res, varName, cache, 'rebalanceDelay', null, null, 0);
        $generatorJava.property(res, varName, cache, 'rebalanceTimeout', null, null, 10000);
        $generatorJava.property(res, varName, cache, 'rebalanceThrottle', null, null, 0);
    }

    res.softEmptyLine();

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

    if (!varName)
        varName = $generatorJava.nextVariableName('cache', cache);

    if (cache.cacheMode === 'PARTITIONED' && cache.nearCacheEnabled) {
        res.needEmptyLine = true;

        if (cache.nearConfiguration) {
            $generatorJava.declareVariable(res, 'nearCfg', 'org.apache.ignite.configuration.NearCacheConfiguration');

            res.needEmptyLine = true;

            if (cache.nearConfiguration.nearStartSize) {
                $generatorJava.property(res, 'nearCfg', cache.nearConfiguration, 'nearStartSize', null, null, 375000);

                res.needEmptyLine = true;
            }

            if (cache.nearConfiguration.nearEvictionPolicy && cache.nearConfiguration.nearEvictionPolicy.kind) {
                $generatorJava.evictionPolicy(res, 'nearCfg', cache.nearConfiguration.nearEvictionPolicy, 'nearEvictionPolicy');

                res.needEmptyLine = true;
            }

            res.line(varName + '.setNearConfiguration(nearCfg);');

            res.needEmptyLine = true;
        }
    }

    return res;
};

// Generate cache statistics group.
$generatorJava.cacheStatistics = function (cache, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!varName)
        varName = $generatorJava.nextVariableName('cache', cache);

    $generatorJava.property(res, varName, cache, 'statisticsEnabled', null, null, false);
    $generatorJava.property(res, varName, cache, 'managementEnabled', null, null, false);

    res.needEmptyLine = true;

    return res;
};

// Generate domain model query fields.
$generatorJava.domainModelQueryFields = function (res, domain) {
    var fields = domain.fields;

    if (fields && fields.length > 0) {
        $generatorJava.declareVariable(res, 'fields', 'java.util.LinkedHashMap', 'java.util.LinkedHashMap', 'java.lang.String', 'java.lang.String');

        _.forEach(fields, function (field) {
            res.line('fields.put("' + field.name + '", "' + $generatorCommon.JavaTypes.fullClassName(field.className) + '");');
        });

        res.needEmptyLine = true;

        res.line('qryMeta.setFields(fields);');

        res.needEmptyLine = true;
    }
};

// Generate domain model query aliases.
$generatorJava.domainModelQueryAliases = function (res, domain) {
    var aliases = domain.aliases;

    if (aliases && aliases.length > 0) {
        $generatorJava.declareVariable(res, 'aliases', 'java.util.Map', 'java.util.HashMap', 'java.lang.String', 'java.lang.String');

        _.forEach(aliases, function (alias) {
            res.line('aliases.put("' + alias.field + '", "' + alias.alias + '");');
        });

        res.needEmptyLine = true;

        res.line('qryMeta.setAliases(aliases);');

        res.needEmptyLine = true;
    }
};

// Generate domain model indexes.
$generatorJava.domainModelQueryIndexes = function (res, domain) {
    var indexes = domain.indexes;

    if (indexes && indexes.length > 0) {
        res.needEmptyLine = true;

        $generatorJava.declareVariable(res, 'indexes', 'java.util.List', 'java.util.ArrayList', 'org.apache.ignite.cache.QueryIndex');

        _.forEach(indexes, function (index) {
            var fields = index.fields;

            // One row generation for 1 field index.
            if (fields && fields.length === 1) {
                var field = index.fields[0];

                res.line('indexes.add(new ' + res.importClass('org.apache.ignite.cache.QueryIndex') +
                    '("' + field.name + '", ' +
                    res.importClass('org.apache.ignite.cache.QueryIndexType') + '.' + index.indexType + ', ' +
                    field.direction + ', "' + index.name + '"));');
            }
            else {
                res.needEmptyLine = true;

                $generatorJava.declareVariable(res, 'index', 'org.apache.ignite.cache.QueryIndex');

                $generatorJava.property(res, 'index', index, 'name');
                $generatorJava.enumProperty(res, 'index', index, 'indexType', 'org.apache.ignite.cache.QueryIndexType');

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
            }
        });

        res.needEmptyLine = true;

        res.line('qryMeta.setIndexes(indexes);');

        res.needEmptyLine = true;
    }
};

// Generate domain model db fields.
$generatorJava.domainModelDatabaseFields = function (res, domain, fieldProperty) {
    var dbFields = domain[fieldProperty];

    if (dbFields && dbFields.length > 0) {
        res.needEmptyLine = true;

        res.importClass('java.sql.Types');

        res.startBlock('jdbcType.' + $generatorCommon.toJavaName('set', fieldProperty) + '(');

        var lastIx = dbFields.length - 1;

        res.importClass('org.apache.ignite.cache.store.jdbc.JdbcTypeField');

        _.forEach(dbFields, function (field, ix) {
            res.line('new JdbcTypeField(' +
                'Types.' + field.databaseFieldType + ', ' + '"' + field.databaseFieldName + '", ' +
                res.importClass(field.javaFieldType) + '.class, ' + '"' + field.javaFieldName + '"'+ ')' + (ix < lastIx ? ',' : ''));
        });

        res.endBlock(');');

        res.needEmptyLine = true;
    }
};

// Generate domain model general group.
$generatorJava.domainModelGeneral = function (domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    switch ($generatorCommon.domainQueryMetadata(domain)) {
        case 'Annotations':
            if ($generatorCommon.isDefinedAndNotEmpty(domain.keyType) || $generatorCommon.isDefinedAndNotEmpty(domain.valueType)) {
                var types = [];

                if ($generatorCommon.isDefinedAndNotEmpty(domain.keyType))
                    types.push($generatorJava.toJavaCode(res.importClass(domain.keyType), 'class'));
                else
                    types.push('???');

                if ($generatorCommon.isDefinedAndNotEmpty(domain.valueType))
                    types.push($generatorJava.toJavaCode(res.importClass(domain.valueType), 'class'));
                else
                    types.push('???');

                if ($generatorCommon.isDefinedAndNotEmpty(types)) {
                    res.startBlock('cache.setIndexedTypes(');

                    res.line(types.join(', '));

                    res.endBlock(');');
                }
            }

            break;

        case 'Configuration':
            $generatorJava.classNameProperty(res, 'jdbcTypes', domain, 'keyType');
            $generatorJava.property(res, 'jdbcTypes', domain, 'valueType');

            if ($generatorCommon.isDefinedAndNotEmpty(domain.fields)) {
                res.needEmptyLine = true;

                $generatorJava.classNameProperty(res, 'qryMeta', domain, 'keyType');
                $generatorJava.property(res, 'qryMeta', domain, 'valueType');
            }

            break;
    }

    res.needEmptyLine = true;

    return res;
};

// Generate domain model for query group.
$generatorJava.domainModelQuery = function (domain, res) {
    if (!res)
        res = $generatorCommon.builder();

    if ($generatorCommon.domainQueryMetadata(domain) === 'Configuration') {
        $generatorJava.domainModelQueryFields(res, domain);
        $generatorJava.domainModelQueryAliases(res, domain);
        $generatorJava.domainModelQueryIndexes(res, domain);

        res.needEmptyLine = true;
    }

    return res;
};

// Generate domain model for store group.
$generatorJava.domainStore = function (domain, withTypes, res) {
    if (!res)
        res = $generatorCommon.builder();

    $generatorJava.property(res, 'jdbcType', domain, 'databaseSchema');
    $generatorJava.property(res, 'jdbcType', domain, 'databaseTable');

    if (withTypes) {
        $generatorJava.classNameProperty(res, 'jdbcType', domain, 'keyType');
        $generatorJava.property(res, 'jdbcType', domain, 'valueType');
    }

    $generatorJava.domainModelDatabaseFields(res, domain, 'keyFields');
    $generatorJava.domainModelDatabaseFields(res, domain, 'valueFields');

    res.needEmptyLine = true;

    return res;
};

// Generate domain model configs.
$generatorJava.cacheDomains = function (domains, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    var domainConfigs = _.filter(domains, function (domain) {
        return $generatorCommon.domainQueryMetadata(domain) === 'Configuration' &&
            $generatorCommon.isDefinedAndNotEmpty(domain.fields);
    });

    // Generate domain model configs.
    if ($generatorCommon.isDefinedAndNotEmpty(domainConfigs)) {
        $generatorJava.declareVariable(res, 'queryEntities', 'java.util.Collection', 'java.util.ArrayList', 'org.apache.ignite.cache.QueryEntity');

        _.forEach(domainConfigs, function (domain) {
            if ($generatorCommon.isDefinedAndNotEmpty(domain.fields))
                res.line('queryEntities.add(queryEntity' + $generatorJava.extractType(domain.valueType) + '());');
        });

        res.needEmptyLine = true;

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
    $generatorJava.cacheStore(cache, cache.domains, varName, res);
    $generatorJava.cacheConcurrency(cache, varName, res);
    $generatorJava.cacheRebalance(cache, varName, res);
    $generatorJava.cacheServerNearCache(cache, varName, res);
    $generatorJava.cacheStatistics(cache, varName, res);
    $generatorJava.cacheDomains(cache.domains, varName, res);
};

// Generation of cache domain model in separate methods.
$generatorJava.clusterDomains = function (caches, res) {
    var domains = [];

    var typeVarName = 'jdbcType';
    var metaVarName = 'qryMeta';

    _.forEach(caches, function (cache) {
        _.forEach(cache.domains, function (domain) {
            if (_.isNil(_.find(domains, function (m) {
                    return m === domain.valueType;
                }))) {
                $generatorJava.resetVariables(res);

                var type = $generatorJava.extractType(domain.valueType);

                if ($generatorCommon.isDefinedAndNotEmpty(domain.databaseTable)) {
                    res.line('/**');
                    res.line(' * Create JDBC type for ' + type + '.');
                    res.line(' *');
                    res.line(' * @param cacheName Cache name.');
                    res.line(' * @return Configured JDBC type.');
                    res.line(' */');
                    res.startBlock('private static JdbcType jdbcType' + type + '(String cacheName) {');

                    $generatorJava.declareVariable(res, typeVarName, 'org.apache.ignite.cache.store.jdbc.JdbcType');

                    res.needEmptyLine = true;

                    res.line(typeVarName + '.setCacheName(cacheName);');

                    $generatorJava.domainStore(domain, true, res);

                    res.needEmptyLine = true;

                    res.line('return ' + typeVarName + ';');
                    res.endBlock('}');

                    res.needEmptyLine = true;
                }

                if ($generatorCommon.domainQueryMetadata(domain) === 'Configuration' &&
                    $generatorCommon.isDefinedAndNotEmpty(domain.fields)) {
                    res.line('/**');
                    res.line(' * Create SQL Query descriptor for ' + type + '.');
                    res.line(' *');
                    res.line(' * @return Configured query entity.');
                    res.line(' */');
                    res.startBlock('private static QueryEntity queryEntity' + type + '() {');

                    $generatorJava.declareVariable(res, metaVarName, 'org.apache.ignite.cache.QueryEntity');

                    $generatorJava.classNameProperty(res, metaVarName, domain, 'keyType');
                    $generatorJava.property(res, metaVarName, domain, 'valueType');

                    res.needEmptyLine = true;

                    $generatorJava.domainModelQuery(domain, res);

                    res.emptyLineIfNeeded();
                    res.line('return ' + metaVarName + ';');

                    res.needEmptyLine = true;

                    res.endBlock('}');
                }

                domains.push(domain.valueType);
            }
        });
    });
};

/**
 * @param prefix Variable prefix.
 * @param obj Object to process.
 * @param names Known names to generate next unique name.
 */
$generatorJava.nextVariableName = function (prefix, obj, names) {
    var nextName = $generatorCommon.toJavaName(prefix, obj.name);

    var checkNextName = function (name) {
        return name === nextName + (ix === 0 ? '' : '_' + ix);
    };

    var ix = 0;

    while (_.find(names, checkNextName)) {
        ix ++;
    }

    if (ix > 0)
        nextName = nextName + '_' + ix;

    return nextName;
};

// Generate cluster caches.
$generatorJava.clusterCaches = function (caches, igfss, isSrvCfg, res) {
    function clusterCache(res, cache, names) {
        res.emptyLineIfNeeded();

        var cacheName = $generatorJava.nextVariableName('cache', cache, names);

        $generatorJava.resetVariables(res);

        var hasDatasource = $generatorCommon.cacheHasDatasource(cache);

        res.line('/**');
        res.line(' * Create configuration for cache "' + cache.name + '".');
        res.line(' *');
        res.line(' * @return Configured cache.');

        if (hasDatasource)
            res.line(' * @throws Exception if failed to create cache configuration.');

        res.line(' */');
        res.startBlock('public static CacheConfiguration ' + cacheName + '()' + (hasDatasource ? ' throws Exception' : '') + ' {');

        $generatorJava.declareVariable(res, cacheName, 'org.apache.ignite.configuration.CacheConfiguration');

        $generatorJava.cache(cache, cacheName, res);

        res.line('return ' + cacheName + ';');
        res.endBlock('}');

        names.push(cacheName);

        res.needEmptyLine = true;
    }

    if (!res)
        res = $generatorCommon.builder();

    var names = [];

    if ($generatorCommon.isDefinedAndNotEmpty(caches)) {
        res.emptyLineIfNeeded();

        _.forEach(caches, function (cache) {
            clusterCache(res, cache, names);
        });

        res.needEmptyLine = true;
    }

    if (isSrvCfg && $generatorCommon.isDefinedAndNotEmpty(igfss)) {
        res.emptyLineIfNeeded();

        _.forEach(igfss, function (igfs) {
            clusterCache(res, $generatorCommon.igfsDataCache(igfs), names);
            clusterCache(res, $generatorCommon.igfsMetaCache(igfs), names);
        });

        res.needEmptyLine = true;
    }

    return res;
};

// Generate cluster caches.
$generatorJava.clusterCacheUse = function (caches, igfss, res) {
    function clusterCacheInvoke(cache, names) {
        names.push($generatorJava.nextVariableName('cache', cache, names) + '()');
    }

    if (!res)
        res = $generatorCommon.builder();

    var names = [];

    _.forEach(caches, function (cache) {
        clusterCacheInvoke(cache, names);
    });

    var igfsNames = [];

    _.forEach(igfss, function (igfs) {
        clusterCacheInvoke($generatorCommon.igfsDataCache(igfs), igfsNames);
        clusterCacheInvoke($generatorCommon.igfsMetaCache(igfs), igfsNames);
    });

    if (names.length > 0 || igfsNames.length > 0) {
        _.forEach(igfsNames, function (igfsName) {
            names.push(igfsName);
        });

        res.line('cfg.setCacheConfiguration(' + names.join(', ') + ');');

        res.needEmptyLine = true;
    }

    return res;
};

// Get class name from fully specified class path.
$generatorJava.extractType = function (fullType) {
    return fullType.substring(fullType.lastIndexOf('.') + 1);
};

/**
 * Generate java class code.
 *
 * @param domain Domain model object.
 * @param key If 'true' then key class should be generated.
 * @param pkg Package name.
 * @param useConstructor If 'true' then empty and full constructors should be generated.
 * @param includeKeyFields If 'true' then include key fields into value POJO.
 * @param res Resulting output with generated code.
 */
$generatorJava.javaClassCode = function (domain, key, pkg, useConstructor, includeKeyFields, res) {
    if (!res)
        res = $generatorCommon.builder();

    var type = $generatorJava.extractType(key ? domain.keyType : domain.valueType);

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

    var allFields = (key || includeKeyFields) ? domain.keyFields.slice() : [];

    if (!key)
        _.forEach(domain.valueFields, function (valFld) {
            if (_.findIndex(allFields, function(fld) {
                return fld.javaFieldName === valFld.javaFieldName;
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

        _.forEach(allFields, function(field, idx) {
            res.line(res.importClass(field.javaFieldType) + ' ' + field.javaFieldName + (idx < allFields.length - 1 ? ',' : ''));
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
        res.startBlock('public ' + fldType + ' ' + $generatorCommon.toJavaName('get', fldName) + '() {');
        res.line('return ' + fldName + ';');
        res.endBlock('}');

        res.needEmptyLine = true;

        res.line('/**');
        res.line(' * Sets ' + fldName + '.');
        res.line(' *');
        res.line(' * @param ' + fldName + ' New value for ' + fldName + '.');
        res.line(' */');
        res.startBlock('public void ' + $generatorCommon.toJavaName('set', fldName) + '(' + fldType + ' ' + fldName + ') {');
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
        var javaType = field.javaFieldType;

        if ($generatorCommon.JavaTypes.isJavaPrimitive(javaType)) {
            if ('float' === javaType)
                res.startBlock('if (Float.compare(' + javaName + ', that.' + javaName + ') != 0)');
            else if ('double' === javaType)
                res.startBlock('if (Double.compare(' + javaName + ', that.' + javaName + ') != 0)');
            else
                res.startBlock('if (' + javaName + ' != that.' + javaName + ')');
        }
        else
            res.startBlock('if (' + javaName + ' != null ? !' + javaName + '.equals(that.' + javaName + ') : that.' + javaName + ' != null)');

        res.line('return false;');
        res.endBlock();
    });

    res.needEmptyLine = true;

    res.line('return true;');
    res.endBlock('}');

    res.needEmptyLine = true;

    // Generate hashCode() method.
    res.line('/** {@inheritDoc} */');
    res.startBlock('@Override public int hashCode() {');

    var first = true;
    var tempVar = false;

    _.forEach(allFields, function (field) {
        var javaName = field.javaFieldName;
        var javaType = field.javaFieldType;

        if (!first)
            res.needEmptyLine = true;

        if ($generatorCommon.JavaTypes.isJavaPrimitive(javaType)) {
            if ('boolean' === javaType)
                res.line(first ? 'int res = ' + javaName + ' ? 1 : 0;' : 'res = 31 * res + (' + javaName + ' ? 1 : 0);');
            else if ('byte' === javaType || 'short' === javaType)
                res.line(first ? 'int res = (int)' + javaName + ';' : 'res = 31 * res + (int)' + javaName + ';');
            else if ('int' === javaType)
                res.line(first ? 'int res = ' + javaName + ';' : 'res = 31 * res + ' + javaName + ';');
            else if ('long' === javaType)
                res.line(first
                    ? 'int res = (int)(' + javaName + ' ^ (' + javaName + ' >>> 32));'
                    : 'res = 31 * res + (int)(' + javaName + ' ^ (' + javaName + ' >>> 32));');
            else if ('float' === javaType)
                res.line(first
                    ? 'int res = ' + javaName + ' != +0.0f ? Float.floatToIntBits(' + javaName + ') : 0;'
                    : 'res = 31 * res + (' + javaName + ' != +0.0f ? Float.floatToIntBits(' + javaName + ') : 0);');
            else if ('double' === javaType) {
                res.line((tempVar ? 'ig_hash_temp' : 'long ig_hash_temp') +
                        ' = Double.doubleToLongBits(' + javaName + ');');

                res.needEmptyLine = true;

                res.line(first
                        ? 'int res = (int)(ig_hash_temp ^ (ig_hash_temp >>> 32));'
                        : 'res = 31 * res + (int)(ig_hash_temp ^ (ig_hash_temp >>> 32));');

                    tempVar = true;
            }
        }
        else
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

    res.startBlock('return \"' + type + ' [" + ');

    _.forEach(allFields, function (field, idx) {
        res.line('\"' + field.javaFieldName + '=\" + ' + field.javaFieldName + (idx < allFields.length - 1 ? ' + ", " + ' : ' +'));
    });

    res.endBlock('"]";');
    res.endBlock('}');

    res.endBlock('}');

    return 'package ' + pkg + ';' + '\n\n' + res.generateImports() + '\n\n' + res.generateStaticImports() + '\n\n' + res.asString();
};

/**
 * Generate source code for type by its domain models.
 *
 * @param caches List of caches to generate POJOs for.
 * @param useConstructor If 'true' then generate constructors.
 * @param includeKeyFields If 'true' then include key fields into value POJO.
 */
$generatorJava.pojos = function (caches, useConstructor, includeKeyFields) {
    var pojos = [];

    _.forEach(caches, function(cache) {
        _.forEach(cache.domains, function(domain) {
            // Skip already generated classes.
            if (!_.find(pojos, {valueType: domain.valueType}) &&
                // Skip domain models without value fields.
                $generatorCommon.isDefinedAndNotEmpty(domain.valueFields)) {
                var pojo = {};

                // Key class generation only if key is not build in java class.
                if (!_.isNil(domain.keyFields) && domain.keyFields.length > 0) {
                    pojo.keyType = domain.keyType;
                    pojo.keyClass = $generatorJava.javaClassCode(domain, true,
                        domain.keyType.substring(0, domain.keyType.lastIndexOf('.')), useConstructor, includeKeyFields);
                }

                pojo.valueType = domain.valueType;
                pojo.valueClass = $generatorJava.javaClassCode(domain, false,
                    domain.valueType.substring(0, domain.valueType.lastIndexOf('.')), useConstructor, includeKeyFields);

                pojos.push(pojo);
            }
        });
    });

    return pojos;
};

/**
 * @param type Full type name.
 * @returns Field java type name.
 */
$generatorJava.javaTypeName = function(type) {
    var ix = $generatorJava.javaBuiltInClasses.indexOf(type);

    var resType = ix >= 0 ? $generatorJava.javaBuiltInFullNameClasses[ix] : type;

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

    if (cluster.sslEnabled && !_.isNil(cluster.sslContextFactory)) {

        cluster.sslContextFactory.keyStorePassword = $generatorCommon.isDefinedAndNotEmpty(cluster.sslContextFactory.keyStoreFilePath) ?
            'props.getProperty("ssl.key.storage.password").toCharArray()' : null;

        cluster.sslContextFactory.trustStorePassword = $generatorCommon.isDefinedAndNotEmpty(cluster.sslContextFactory.trustStoreFilePath) ?
            'props.getProperty("ssl.trust.storage.password").toCharArray()' : null;

        var propsDesc = $generatorCommon.isDefinedAndNotEmpty(cluster.sslContextFactory.trustManagers) ?
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

    if ($generatorCommon.isDefinedAndNotEmpty(igfss)) {
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

    if (!varName)
        varName = $generatorJava.nextVariableName('igfs', igfs);

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

    if (!varName)
        varName = $generatorJava.nextVariableName('igfs', igfs);

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

    if (!varName)
        varName = $generatorJava.nextVariableName('igfs', igfs);

    $generatorJava.property(res, varName, igfs, 'dualModeMaxPendingPutsSize', null, null, 0);

    if ($generatorCommon.isDefinedAndNotEmpty(igfs.dualModePutExecutorService))
        res.line(varName + '.' + $generatorJava.setterName('dualModePutExecutorService') + '(new ' + res.importClass(igfs.dualModePutExecutorService) + '());');

    $generatorJava.property(res, varName, igfs, 'dualModePutExecutorServiceShutdown', null, null, false);

    res.needEmptyLine = true;

    return res;
};

$generatorJava.igfsSecondFS = function(igfs, varName, res) {
    if (!res)
        res = $generatorCommon.builder();

    if (!varName)
        varName = $generatorJava.nextVariableName('igfs', igfs);

    if (igfs.secondaryFileSystemEnabled) {
        var secondFs = igfs.secondaryFileSystem || {};

        var nameDefined = $generatorCommon.isDefinedAndNotEmpty(secondFs.userName);
        var cfgDefined = $generatorCommon.isDefinedAndNotEmpty(secondFs.cfgPath);

        res.line(varName + '.setSecondaryFileSystem(new ' +
            res.importClass('org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem') + '(' +
                $generatorJava.constructorArg(secondFs, 'uri', null) +
                (cfgDefined || nameDefined ? $generatorJava.constructorArg(secondFs, 'cfgPath', null, true) : '') +
                $generatorJava.constructorArg(secondFs, 'userName', null, true, true) +
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

    if (!varName)
        varName = $generatorJava.nextVariableName('igfs', igfs);

    if ($generatorCommon.isDefinedAndNotEmpty(igfs.name)) {
        igfs.dataCacheName = $generatorCommon.igfsDataCache(igfs).name;
        igfs.metaCacheName = $generatorCommon.igfsMetaCache(igfs).name;

        $generatorJava.property(res, varName, igfs, 'name');
        $generatorJava.property(res, varName, igfs, 'dataCacheName');
        $generatorJava.property(res, varName, igfs, 'metaCacheName');
        $generatorJava.enumProperty(res, varName, igfs, 'defaultMode', 'org.apache.ignite.igfs.IgfsMode', null, "DUAL_ASYNC");

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

    if (!varName)
        varName = $generatorJava.nextVariableName('igfs', igfs);

    $generatorJava.property(res, varName, igfs, 'blockSize', null, null, 65536);
    $generatorJava.property(res, varName, igfs, 'streamBufferSize', null, null, 65536);
    $generatorJava.property(res, varName, igfs, 'maxSpaceSize', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'maximumTaskRangeLength', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'managementPort', null, null, 11400);
    $generatorJava.property(res, varName, igfs, 'perNodeBatchSize', null, null, 100);
    $generatorJava.property(res, varName, igfs, 'perNodeParallelBatchCount', null, null, 8);
    $generatorJava.property(res, varName, igfs, 'prefetchBlocks', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'sequentialReadsBeforePrefetch', null, null, 0);
    $generatorJava.property(res, varName, igfs, 'trashPurgeTimeout', null, null, 1000);
    $generatorJava.property(res, varName, igfs, 'colocateMetadata', null, null, true);
    $generatorJava.property(res, varName, igfs, 'relaxedConsistency', null, null, true);

    if (igfs.pathModes && igfs.pathModes.length > 0) {
        res.needEmptyLine = true;

        $generatorJava.declareVariable(res, 'pathModes', 'java.util.Map', 'java.util.HashMap', 'String', 'org.apache.ignite.igfs.IgfsMode');

        _.forEach(igfs.pathModes, function (pair) {
            res.line('pathModes.put("' + pair.path + '", IgfsMode.' + pair.mode +');');
        });

        res.needEmptyLine = true;

        res.line(varName + '.setPathModes(pathModes);');
    }

    res.needEmptyLine = true;

    return res;
};

$generatorJava.clusterConfiguration = function (cluster, clientNearCfg, res) {
    $generatorJava.clusterGeneral(cluster, clientNearCfg, res);

    $generatorJava.clusterAtomics(cluster.atomicConfiguration, res);

    $generatorJava.clusterBinary(cluster.binaryConfiguration, res);

    $generatorJava.clusterCommunication(cluster, res);

    $generatorJava.clusterConnector(cluster.connector, res);

    $generatorJava.clusterDeployment(cluster, res);

    $generatorJava.clusterEvents(cluster, res);

    $generatorJava.clusterMarshaller(cluster, res);

    $generatorJava.clusterMetrics(cluster, res);

    $generatorJava.clusterSwap(cluster, res);

    $generatorJava.clusterTime(cluster, res);

    $generatorJava.clusterPools(cluster, res);

    $generatorJava.clusterTransactions(cluster.transactionConfiguration, res);

    var isSrvCfg = _.isNil(clientNearCfg);

    if (isSrvCfg)
        $generatorJava.clusterCacheUse(cluster.caches, cluster.igfss, res);

    $generatorJava.clusterSsl(cluster, res);

    if (isSrvCfg)
        $generatorJava.igfss(cluster.igfss, 'cfg', res);

    return res;
};

// Generate loading of secret properties file.
$generatorJava.tryLoadSecretProperties = function (cluster, res) {
    if ($generatorCommon.secretPropertiesNeeded(cluster)) {
        res.importClass('org.apache.ignite.configuration.IgniteConfiguration');

        $generatorJava.declareVariableCustom(res, 'props', 'java.util.Properties', 'new Properties()', 'private static final');

        res.startBlock('static {');
        res.startBlock('try (' + res.importClass('java.io.InputStream') + ' in = IgniteConfiguration.class.getClassLoader().getResourceAsStream("secret.properties")) {');
        res.line('props.load(in);');
        res.endBlock('}');
        res.startBlock('catch (Exception ignored) {');
        res.line('// No-op.');
        res.endBlock('}');
        res.endBlock('}');

        res.needEmptyLine = true;
    }
};

/**
 * Function to generate java code for cluster configuration.
 *
 * @param cluster Cluster to process.
 * @param pkg Package name.
 * @param javaClass Class name for generate factory class otherwise generate code snippet.
 * @param clientNearCfg Optional near cache configuration for client node.
 */
$generatorJava.cluster = function (cluster, pkg, javaClass, clientNearCfg) {
    var res = $generatorCommon.builder();

    var isSrvCfg = _.isNil(clientNearCfg);

    if (cluster) {
        var resCfg = $generatorJava.clusterConfiguration(cluster, clientNearCfg, $generatorCommon.builder());

        res.mergeProps(resCfg);

        res.line('/**');
        res.line(' * ' + $generatorCommon.mainComment());
        res.line(' */');
        res.startBlock('public class ' + javaClass + ' {');

        $generatorJava.tryLoadSecretProperties(cluster, res);

        $generatorJava.clusterDataSources(cluster.caches, res);

        res.line('/**');
        res.line(' * Configure grid.');
        res.line(' *');
        res.line(' * @return Ignite configuration.');
        res.line(' * @throws Exception If failed to construct Ignite configuration instance.');
        res.line(' */');
        res.startBlock('public static IgniteConfiguration createConfiguration() throws Exception {');

        res.mergeLines(resCfg);

        res.needEmptyLine = true;

        res.line('return cfg;');
        res.endBlock('}');

        res.needEmptyLine = true;

        $generatorJava.clusterDomains(cluster.caches, res);

        $generatorJava.clusterCaches(cluster.caches, cluster.igfss, isSrvCfg, res);

        // TODO IGNITE-2269 Remove specified methods after implamentation of extended constructors.
        $generatorJava.binaryTypeConfigurations(cluster.binaryConfiguration, res);

        res.needEmptyLine = true;

        if (clientNearCfg) {
            res.line('/**');
            res.line(' * Configure client near cache configuration.');
            res.line(' *');
            res.line(' * @return Near cache configuration.');
            res.line(' * @throws Exception If failed to construct near cache configuration instance.');
            res.line(' */');
            res.startBlock('public static NearCacheConfiguration createNearCacheConfiguration() throws Exception {');

            $generatorJava.resetVariables(res);

            $generatorJava.declareVariable(res, 'clientNearCfg', 'org.apache.ignite.configuration.NearCacheConfiguration');

            if (clientNearCfg.nearStartSize) {
                $generatorJava.property(res, 'clientNearCfg', clientNearCfg, 'nearStartSize');

                res.needEmptyLine = true;
            }

            if (clientNearCfg.nearEvictionPolicy && clientNearCfg.nearEvictionPolicy.kind)
                $generatorJava.evictionPolicy(res, 'clientNearCfg', clientNearCfg.nearEvictionPolicy, 'nearEvictionPolicy');

            res.line('return clientNearCfg;');
            res.endBlock('}');

            res.needEmptyLine = true;
        }

        res.endBlock('}');

        return 'package ' + pkg + ';\n\n' + res.generateImports() + '\n\n' + res.generateStaticImports()  + '\n\n' + res.asString();
    }

    return res.asString();
};

/** Generate data source class name for specified store factory.
 *
 * @param res Optional configuration presentation builder object.
 * @param storeFactory Store factory for data source class name generation.
 * @returns {*} Data source class name.
 */
$generatorJava.dataSourceClassName = function (res, storeFactory) {
    var dialect = storeFactory.connectVia ? (storeFactory.connectVia === 'DataSource' ? storeFactory.dialect : null) : storeFactory.dialect;

    if (dialect) {
        var dataSourceBean = storeFactory.dataSourceBean;

        var dsClsName = $generatorCommon.dataSourceClassName(dialect);

        var varType = res.importClass(dsClsName);

        return $generatorCommon.toJavaName(varType, dataSourceBean);
    }

    return null;
};

// Descriptors for generation of demo data.
const PREDEFINED_QUERIES = [
    {
        schema: 'CARS',
        type: 'PARKING',
        create: 'CREATE TABLE IF NOT EXISTS CARS.PARKING (\n' +
            'ID       INTEGER     NOT NULL PRIMARY KEY,\n' +
            'NAME     VARCHAR(50) NOT NULL,\n' +
            'CAPACITY INTEGER NOT NULL)',
        clearQuery: 'DELETE FROM CARS.PARKING',
        insertCntConsts: [{name: 'DEMO_MAX_PARKING_CNT', val: 5, comment: 'How many parkings to generate.'}],
        insertPattern: ['INSERT INTO CARS.PARKING(ID, NAME, CAPACITY) VALUES(?, ?, ?)'],
        fillInsertParameters: function (res) {
            res.line('stmt.setInt(1, id);');
            res.line('stmt.setString(2, "Parking #" + (id + 1));');
            res.line('stmt.setInt(3, 10 + rnd.nextInt(20));');
        },
        selectQuery: [
            "SELECT * FROM PARKING WHERE CAPACITY >= 20"
        ]
    },
    {
        schema: 'CARS',
        type: 'CAR',
        create: 'CREATE TABLE IF NOT EXISTS CARS.CAR (\n' +
            'ID         INTEGER NOT NULL PRIMARY KEY,\n' +
            'PARKING_ID INTEGER NOT NULL,\n' +
            'NAME       VARCHAR(50) NOT NULL);',
        clearQuery: 'DELETE FROM CARS.CAR',
        rndRequired: true,
        insertCntConsts: [
            {name: 'DEMO_MAX_CAR_CNT', val: 10, comment: 'How many cars to generate.'},
            {name: 'DEMO_MAX_PARKING_CNT', val: 5, comment: 'How many parkings to generate.'}
        ],
        insertPattern: ['INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(?, ?, ?)'],
        fillInsertParameters: function (res) {
            res.line('stmt.setInt(1, id);');
            res.line('stmt.setInt(2, rnd.nextInt(DEMO_MAX_PARKING_CNT));');
            res.line('stmt.setString(3, "Car #" + (id + 1));');
        },
        selectQuery: [
            "SELECT * FROM CAR WHERE PARKINGID = 2"
        ]
    },
    {
        type: 'COUNTRY',
        create: 'CREATE TABLE IF NOT EXISTS COUNTRY (\n' +
            'ID         INTEGER NOT NULL PRIMARY KEY,\n' +
            'NAME       VARCHAR(50),\n' +
            'POPULATION INTEGER NOT NULL);',
        clearQuery: 'DELETE FROM COUNTRY',
        insertCntConsts: [{name: 'DEMO_MAX_COUNTRY_CNT', val: 5, comment: 'How many countries to generate.'}],
        insertPattern: ['INSERT INTO COUNTRY(ID, NAME, POPULATION) VALUES(?, ?, ?)'],
        fillInsertParameters: function (res) {
            res.line('stmt.setInt(1, id);');
            res.line('stmt.setString(2, "Country #" + (id + 1));');
            res.line('stmt.setInt(3, 10000000 + rnd.nextInt(100000000));');
        },
        selectQuery: [
            "SELECT * FROM COUNTRY WHERE POPULATION BETWEEN 15000000 AND 25000000"
        ]
    },
    {
        type: 'DEPARTMENT',
        create: 'CREATE TABLE IF NOT EXISTS DEPARTMENT (\n' +
            'ID         INTEGER NOT NULL PRIMARY KEY,\n' +
            'COUNTRY_ID INTEGER NOT NULL,\n' +
            'NAME       VARCHAR(50) NOT NULL);',
        clearQuery: 'DELETE FROM DEPARTMENT',
        rndRequired: true,
        insertCntConsts: [
            {name: 'DEMO_MAX_DEPARTMENT_CNT', val: 5, comment: 'How many departments to generate.'},
            {name: 'DEMO_MAX_COUNTRY_CNT', val: 5, comment: 'How many countries to generate.'}
        ],
        insertPattern: ['INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(?, ?, ?)'],
        fillInsertParameters: function (res) {
            res.line('stmt.setInt(1, id);');
            res.line('stmt.setInt(2, rnd.nextInt(DEMO_MAX_COUNTRY_CNT));');
            res.line('stmt.setString(3, "Department #" + (id + 1));');
        },
        selectQuery: [
            "SELECT * FROM DEPARTMENT"
        ]
    },
    {
        type: 'EMPLOYEE',
        create: 'CREATE TABLE IF NOT EXISTS EMPLOYEE (\n' +
            'ID            INTEGER NOT NULL PRIMARY KEY,\n' +
            'DEPARTMENT_ID INTEGER NOT NULL,\n' +
            'MANAGER_ID    INTEGER,\n' +
            'FIRST_NAME    VARCHAR(50) NOT NULL,\n' +
            'LAST_NAME     VARCHAR(50) NOT NULL,\n' +
            'EMAIL         VARCHAR(50) NOT NULL,\n' +
            'PHONE_NUMBER  VARCHAR(50),\n' +
            'HIRE_DATE     DATE        NOT NULL,\n' +
            'JOB           VARCHAR(50) NOT NULL,\n' +
            'SALARY        DOUBLE);',
        clearQuery: 'DELETE FROM EMPLOYEE',
        rndRequired: true,
        insertCntConsts: [
            {name: 'DEMO_MAX_EMPLOYEE_CNT', val: 10, comment: 'How many employees to generate.'},
            {name: 'DEMO_MAX_DEPARTMENT_CNT', val: 5, comment: 'How many departments to generate.'}
        ],
        specialGeneration: function (res, conVar) {
            //$generatorJava.declareVariableCustom(res, 'stmt', 'java.sql.PreparedStatement', conVar +
            //    '.prepareStatement("INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")');
            //
            //res.startBlock('for (int id = 0; id < DEMO_MAX_DEPARTMENT_CNT; id ++) {');
            //res.line('stmt.setInt(1, id);');
            //res.line('stmt.setInt(2, id);');
            //res.line('stmt.setString(3, "First name manager #" + (id + 1));');
            //res.line('stmt.setString(4, "Last name manager#" + (id + 1));');
            //res.line('stmt.setString(5, "Email manager#" + (id + 1));');
            //res.line('stmt.setString(6, "Phone number manager#" + (id + 1));');
            //res.line('stmt.setString(7, "2014-01-01");');
            //res.line('stmt.setString(8, "Job manager #" + (id + 1));');
            //res.line('stmt.setDouble(9, 1000.0 + rnd.nextInt(500));');
            //
            //res.needEmptyLine = true;
            //
            //res.line('stmt.executeUpdate();');
            //
            //res.endBlock('}');
            //
            //res.needEmptyLine = true;

            $generatorJava.declareVariableCustom(res, 'stmt', 'java.sql.PreparedStatement', conVar +
                '.prepareStatement("INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")');

            res.startBlock('for (int id = 0; id < DEMO_MAX_EMPLOYEE_CNT; id ++) {');

            res.needEmptyLine = true;

            res.line('int depId = rnd.nextInt(DEMO_MAX_DEPARTMENT_CNT);');

            res.line('stmt.setInt(1, DEMO_MAX_DEPARTMENT_CNT + id);');
            res.line('stmt.setInt(2, depId);');
            res.line('stmt.setInt(3, depId);');
            res.line('stmt.setString(4, "First name manager #" + (id + 1));');
            res.line('stmt.setString(5, "Last name manager#" + (id + 1));');
            res.line('stmt.setString(6, "Email manager#" + (id + 1));');
            res.line('stmt.setString(7, "Phone number manager#" + (id + 1));');
            res.line('stmt.setString(8, "2014-01-01");');
            res.line('stmt.setString(9, "Job manager #" + (id + 1));');
            res.line('stmt.setDouble(10, 600.0 + rnd.nextInt(300));');

            res.needEmptyLine = true;

            res.line('stmt.executeUpdate();');

            res.endBlock('}');

            res.needEmptyLine = true;
        },
        selectQuery: [
            "SELECT * FROM EMPLOYEE WHERE SALARY > 700"
        ]
    }
];

// Generate creation and execution of prepared statement.
function _prepareStatement(res, conVar, query, select) {
    if (query) {
        var lines = query.split('\n');

        _.forEach(lines, function (line, ix) {
            if (ix == 0)
                if (lines.length == 1)
                    res.line(conVar + '.prepareStatement("' + line + '").execute' + (select ? 'Query' : 'Update') + '();');
                else
                    res.startBlock(conVar + '.prepareStatement("' + line + '" +');
            else
                res.line('"' + line + '"' + (ix === lines.length - 1 ? ').execute' + (select ? 'Query' : 'Update') + '();' : ' +'));
        });

        if (lines.length > 0)
            res.needEmptyLine = true;

        if (lines.length > 1)
            res.endBlock();
    }
}

// Generate creation and execution of cache query.
function _multilineQuery(res, query, prefix, postfix) {
    if (query) {
        var lines = query.split('\n');

        _.forEach(lines, function (line, ix) {
            if (ix == 0)
                if (lines.length == 1)
                    res.line(prefix + '"' + line + '"' + postfix);
                else
                    res.startBlock(prefix + '"' + line + '" +');
            else
                res.line('"' + line + '"' + (ix === lines.length - 1 ? postfix : ' +'));
        });

        if (lines.length > 1)
            res.endBlock();
    }
}

/**
 * Checks if cluster has demo types.
 *
 * @param cluster Cluster to check.
 * @param demo Is demo enabled.
 * @returns {boolean} True if cluster has caches with demo types.
 */
$generatorJava.isDemoConfigured = function(cluster, demo) {
    return demo &&
        _.find(cluster.caches, (cache) => _.find(cache.domains, (domain) => _.find(PREDEFINED_QUERIES,
            (desc) => domain.valueType.toUpperCase().endsWith(desc.type))));
};

$generatorJava.generateDemo = function (cluster, res, factoryCls) {
    const cachesWithDataSource = _.filter(cluster.caches, (cache) => {
        if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
            const storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

            return storeFactory.connectVia === 'DataSource' && storeFactory.dialect ||
                !storeFactory.connectVia && storeFactory.dialect;
        }

        return false;
    });

    // Prepare array of cache and his demo domain model list. Every domain is contained only in first cache.
    var demoTypes = _.filter(_.map(cachesWithDataSource, (cache, idx) => {
        return {
            cache: cache,
            domains: _.filter(cache.domains, (domain) =>
                $generatorCommon.isDefinedAndNotEmpty(domain.valueFields) &&
                    !_.find(cachesWithDataSource, (checkCache, checkIx) => checkIx < idx && _.find(checkCache.domains, domain))
            )
        }
    }), (cache) => $generatorCommon.isDefinedAndNotEmpty(cache.domains));

    if ($generatorCommon.isDefinedAndNotEmpty(demoTypes)) {
        var typeByDs = {};

        // Group domain modes by data source
        _.forEach(demoTypes, function (type) {
            var ds = type.cache.cacheStoreFactory[type.cache.cacheStoreFactory.kind].dataSourceBean;

            if (!typeByDs[ds])
                typeByDs[ds] = [type];
            else
                typeByDs[ds].push(type);
        });

        var rndDefined = false;

        var generatedConsts = [];

        _.forEach(typeByDs, function (types) {
            _.forEach(types, function (type) {
                _.forEach(type.domains, function (domain) {
                    var desc = _.find(PREDEFINED_QUERIES, function (desc) {
                        return domain.valueType.toUpperCase().endsWith(desc.type);
                    });

                    if (desc) {
                        if (!rndDefined && desc.rndRequired) {
                            res.line('/** Random generator for demo data. */');
                            $generatorJava.declareVariableCustom(res, 'rnd', 'java.util.Random', 'new Random()', 'private static final');

                            rndDefined = true;
                        }

                        _.forEach(desc.insertCntConsts, function (cnt) {
                            if (!_.includes(generatedConsts, cnt.name)) {
                                res.line('/** ' + cnt.comment + ' */');
                                res.line('private static final int ' + cnt.name + ' = ' + cnt.val + ';');
                                res.needEmptyLine = true;

                                generatedConsts.push(cnt.name);
                            }
                        });
                    }
                });
            });
        });

        res.needEmptyLine = true;

        // Generation of fill database method
        res.line('/** Fill data for Demo. */');
        res.startBlock('private static void prepareDemoData() throws ' + res.importClass('java.sql.SQLException') + ' {');

        _.forEach(typeByDs, function (types, ds) {
            var conVar = ds + 'Con';

            res.startBlock('try (' + res.importClass('java.sql.Connection') + ' ' + conVar + ' = ' + res.importClass(factoryCls) + '.DataSources.INSTANCE_' + ds + '.getConnection()) {');

            _.forEach(types, function (type) {
                _.forEach(type.domains, function (domain) {
                    var desc = _.find(PREDEFINED_QUERIES, function (desc) {
                        return domain.valueType.toUpperCase().endsWith(desc.type);
                    });

                    if (desc) {
                        res.line('// Generate ' + desc.type + '.');

                        if (desc.schema)
                            _prepareStatement(res, conVar, 'CREATE SCHEMA IF NOT EXISTS ' + desc.schema);

                        _prepareStatement(res, conVar, desc.create);

                        _prepareStatement(res, conVar, desc.clearQuery);

                        res.needEmptyLine = true;

                        if (!desc.specialGeneration) {
                            $generatorJava.declareVariableCustom(res, 'stmt', 'java.sql.PreparedStatement', conVar + '.prepareStatement("' + desc.insertPattern + '")');

                            res.startBlock('for (int id = 0; id < ' + desc.insertCntConsts[0].name + '; id ++) {');

                            desc.fillInsertParameters(res);

                            res.needEmptyLine = true;

                            res.line('stmt.executeUpdate();');

                            res.endBlock('}');

                            res.needEmptyLine = true;
                        }
                        else
                            desc.specialGeneration(res, conVar);

                        res.line(conVar + '.commit();');

                        res.needEmptyLine = true;
                    }
                });
            });

            res.endBlock('}');

            res.needEmptyLine = true;
        });

        res.endBlock('}');

        res.needEmptyLine = true;

        res.line('/** Print result table to console */');
        res.startBlock('private static void printResult(' + res.importClass('java.util.List') + '<' + res.importClass('javax.cache.Cache') + '.Entry<Object, Object>> rows) {');
        res.startBlock('for (Cache.Entry<Object, Object> row: rows) {');
        res.line('System.out.println(row);');
        res.endBlock('}');
        res.endBlock('}');
        res.needEmptyLine = true;

        // Generation of execute queries method.
        res.line('/** Run demo. */');
        res.startBlock('private static void runDemo(Ignite ignite) throws SQLException {');

        var getType = function (fullType) {
            return fullType.substr(fullType.lastIndexOf('.') + 1);
        };

        var cacheLoaded = [];
        var rowVariableDeclared = false;

        _.forEach(typeByDs, function (types, ds) {
            var conVar = ds + 'Con';

            res.startBlock('try (Connection ' + conVar + ' = ' + factoryCls + '.DataSources.INSTANCE_' + ds + '.getConnection()) {');

            _.forEach(types, function (type) {
                _.forEach(type.domains, function (domain) {
                    var desc = _.find(PREDEFINED_QUERIES, function (desc) {
                        return domain.valueType.toUpperCase().endsWith(desc.type);
                    });

                    if (desc) {
                        _.forEach(desc.selectQuery, function (query) {
                            var cacheName = type.cache.name;

                            if (!_.includes(cacheLoaded, cacheName)) {
                                res.line('ignite.cache("' + cacheName + '").loadCache(null);');

                                cacheLoaded.push(cacheName);
                            }

                            _multilineQuery(res, query, (rowVariableDeclared ? 'rows' : 'List<Cache.Entry<Object, Object>> rows') +
                                ' = ignite.cache("' + cacheName + '").query(new ' + res.importClass('org.apache.ignite.cache.query.SqlQuery') +
                                '<>("' + getType(domain.valueType) + '", ', ')).getAll();');
                            res.line('printResult(rows);');
                            rowVariableDeclared = true;

                            res.needEmptyLine = true;
                        })
                    }
                });
            });

            res.endBlock('}');

            res.needEmptyLine = true;
        });

        res.endBlock('}');

        res.needEmptyLine = true;
    }

    return demoTypes;
};

/**
 * Function to generate java class for node startup with cluster configuration.
 *
 * @param cluster Cluster to process.
 * @param pkg Class package name.
 * @param cls Class name.
 * @param cfg Config.
 * @param factoryCls Optional fully qualified class name of configuration factory.
 * @param clientNearCfg Optional near cache configuration for client node.
 */
$generatorJava.nodeStartup = function (cluster, pkg, cls, cfg, factoryCls, clientNearCfg) {
    var demo = cls === 'DemoStartup';

    var res = $generatorCommon.builder();

    res.line('/**');
    res.line(' * ' + $generatorCommon.mainComment());

    if (demo) {
        res.line(' *');
        res.line(' * To start demo configure data sources in secret.properties file.');
        res.line(' * For H2 database it should be like following:');
        res.line(' * dsH2.jdbc.url=jdbc:h2:tcp://localhost/mem:DemoDB;DB_CLOSE_DELAY=-1');
        res.line(' * dsH2.jdbc.username=sa');
        res.line(' * dsH2.jdbc.password=');
    }

    res.line(' */');
    res.startBlock('public class ' + cls + ' {');

    if (demo)
        var demoTypes = $generatorJava.generateDemo(cluster, res, factoryCls);

    res.line('/**');
    res.line(' * Start up node with specified configuration.');
    res.line(' *');
    res.line(' * @param args Command line arguments, none required.');
    res.line(' * @throws Exception If failed.');
    res.line(' */');

    res.startBlock('public static void main(String[] args) throws Exception {');

    if (demo) {
        res.startBlock('try {');
        res.line('// Start H2 database server.');
        res.line(res.importClass('org.h2.tools.Server') + '.createTcpServer("-tcpDaemon").start();');
        res.endBlock('}');
        res.startBlock('catch (' + res.importClass('java.sql.SQLException') + ' ignore) {');
        res.line('// No-op.');
        res.endBlock('}');

        res.needEmptyLine = true;
    }

    if (factoryCls)
        res.importClass(factoryCls);

    if (clientNearCfg || $generatorCommon.isDefinedAndNotEmpty(demoTypes))
        res.line(res.importClass('org.apache.ignite.Ignite') + ' ignite = ' +
            res.importClass('org.apache.ignite.Ignition') + '.start(' + cfg + ');');
    else
        res.line(res.importClass('org.apache.ignite.Ignition') + '.start(' + cfg + ');');

    if (clientNearCfg) {
        res.needEmptyLine = true;

        if ($generatorCommon.isDefinedAndNotEmpty(cluster.caches)) {
            res.line('// Demo of near cache creation on client node.');

            var names = [];

            _.forEach(cluster.caches, function (cache) {
                $generatorJava.nextVariableName('cache', cache, names);

                res.line('ignite.getOrCreateCache(' + res.importClass(factoryCls) + '.' +
                    $generatorJava.nextVariableName('cache', cache, names[names.length - 1]) + '(), ' +
                    res.importClass(factoryCls) + '.createNearCacheConfiguration());');
            });

            res.needEmptyLine = true;
        }
    }

    if (demo) {
        res.needEmptyLine = true;

        res.line('prepareDemoData();');

        res.needEmptyLine = true;

        res.line('runDemo(ignite);');
    }

    res.endBlock('}');

    res.endBlock('}');

    return 'package ' + pkg + ';\n\n' + res.generateImports() + '\n\n' + res.generateStaticImports() + '\n\n' + res.asString();
};

export default $generatorJava;
