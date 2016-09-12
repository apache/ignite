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

// TODO: Refactor this service for legacy tables with more than one input field.
export default ['IgniteLegacyUtils', ['IgniteErrorPopover', (ErrorPopover) => {
    function isDefined(v) {
        return !_.isNil(v);
    }

    function isEmptyString(s) {
        if (isDefined(s))
            return s.trim().length === 0;

        return true;
    }

    const javaBuiltInClasses = [
        'BigDecimal',
        'Boolean',
        'Byte',
        'Date',
        'Double',
        'Float',
        'Integer',
        'Long',
        'Object',
        'Short',
        'String',
        'Time',
        'Timestamp',
        'UUID'
    ];

    const javaBuiltInTypes = [
        'BigDecimal',
        'boolean',
        'Boolean',
        'byte',
        'Byte',
        'Date',
        'double',
        'Double',
        'float',
        'Float',
        'int',
        'Integer',
        'long',
        'Long',
        'Object',
        'short',
        'Short',
        'String',
        'Time',
        'Timestamp',
        'UUID'
    ];

    const javaBuiltInFullNameClasses = [
        'java.math.BigDecimal',
        'java.lang.Boolean',
        'java.lang.Byte',
        'java.sql.Date',
        'java.lang.Double',
        'java.lang.Float',
        'java.lang.Integer',
        'java.lang.Long',
        'java.lang.Object',
        'java.lang.Short',
        'java.lang.String',
        'java.sql.Time',
        'java.sql.Timestamp',
        'java.util.UUID'
    ];

    /**
     * @param clsName Class name to check.
     * @returns {Boolean} 'true' if given class name is a java build-in type.
     */
    function isJavaBuiltInClass(clsName) {
        if (isEmptyString(clsName))
            return false;

        return _.includes(javaBuiltInClasses, clsName) || _.includes(javaBuiltInFullNameClasses, clsName);
    }

    const SUPPORTED_JDBC_TYPES = [
        'BIGINT',
        'BIT',
        'BOOLEAN',
        'BLOB',
        'CHAR',
        'CLOB',
        'DATE',
        'DECIMAL',
        'DOUBLE',
        'FLOAT',
        'INTEGER',
        'LONGNVARCHAR',
        'LONGVARCHAR',
        'NCHAR',
        'NUMERIC',
        'NVARCHAR',
        'REAL',
        'SMALLINT',
        'TIME',
        'TIMESTAMP',
        'TINYINT',
        'VARCHAR'
    ];

    const ALL_JDBC_TYPES = [
        {dbName: 'BIT', dbType: -7, javaType: 'Boolean', primitiveType: 'boolean'},
        {dbName: 'TINYINT', dbType: -6, javaType: 'Byte', primitiveType: 'byte'},
        {dbName: 'SMALLINT', dbType: 5, javaType: 'Short', primitiveType: 'short'},
        {dbName: 'INTEGER', dbType: 4, javaType: 'Integer', primitiveType: 'int'},
        {dbName: 'BIGINT', dbType: -5, javaType: 'Long', primitiveType: 'long'},
        {dbName: 'FLOAT', dbType: 6, javaType: 'Float', primitiveType: 'float'},
        {dbName: 'REAL', dbType: 7, javaType: 'Double', primitiveType: 'double'},
        {dbName: 'DOUBLE', dbType: 8, javaType: 'Double', primitiveType: 'double'},
        {dbName: 'NUMERIC', dbType: 2, javaType: 'BigDecimal'},
        {dbName: 'DECIMAL', dbType: 3, javaType: 'BigDecimal'},
        {dbName: 'CHAR', dbType: 1, javaType: 'String'},
        {dbName: 'VARCHAR', dbType: 12, javaType: 'String'},
        {dbName: 'LONGVARCHAR', dbType: -1, javaType: 'String'},
        {dbName: 'DATE', dbType: 91, javaType: 'Date'},
        {dbName: 'TIME', dbType: 92, javaType: 'Time'},
        {dbName: 'TIMESTAMP', dbType: 93, javaType: 'Timestamp'},
        {dbName: 'BINARY', dbType: -2, javaType: 'Object'},
        {dbName: 'VARBINARY', dbType: -3, javaType: 'Object'},
        {dbName: 'LONGVARBINARY', dbType: -4, javaType: 'Object'},
        {dbName: 'NULL', dbType: 0, javaType: 'Object'},
        {dbName: 'OTHER', dbType: 1111, javaType: 'Object'},
        {dbName: 'JAVA_OBJECT', dbType: 2000, javaType: 'Object'},
        {dbName: 'DISTINCT', dbType: 2001, javaType: 'Object'},
        {dbName: 'STRUCT', dbType: 2002, javaType: 'Object'},
        {dbName: 'ARRAY', dbType: 2003, javaType: 'Object'},
        {dbName: 'BLOB', dbType: 2004, javaType: 'Object'},
        {dbName: 'CLOB', dbType: 2005, javaType: 'String'},
        {dbName: 'REF', dbType: 2006, javaType: 'Object'},
        {dbName: 'DATALINK', dbType: 70, javaType: 'Object'},
        {dbName: 'BOOLEAN', dbType: 16, javaType: 'Boolean', primitiveType: 'boolean'},
        {dbName: 'ROWID', dbType: -8, javaType: 'Object'},
        {dbName: 'NCHAR', dbType: -15, javaType: 'String'},
        {dbName: 'NVARCHAR', dbType: -9, javaType: 'String'},
        {dbName: 'LONGNVARCHAR', dbType: -16, javaType: 'String'},
        {dbName: 'NCLOB', dbType: 2011, javaType: 'String'},
        {dbName: 'SQLXML', dbType: 2009, javaType: 'Object'}
    ];

    /*eslint-disable */
    const JAVA_KEYWORDS = [
        'abstract',
        'assert',
        'boolean',
        'break',
        'byte',
        'case',
        'catch',
        'char',
        'class',
        'const',
        'continue',
        'default',
        'do',
        'double',
        'else',
        'enum',
        'extends',
        'false',
        'final',
        'finally',
        'float',
        'for',
        'goto',
        'if',
        'implements',
        'import',
        'instanceof',
        'int',
        'interface',
        'long',
        'native',
        'new',
        'null',
        'package',
        'private',
        'protected',
        'public',
        'return',
        'short',
        'static',
        'strictfp',
        'super',
        'switch',
        'synchronized',
        'this',
        'throw',
        'throws',
        'transient',
        'true',
        'try',
        'void',
        'volatile',
        'while'
    ];
    /*eslint-enable */

    const VALID_JAVA_IDENTIFIER = new RegExp('^[a-zA-Z_$][a-zA-Z\\d_$]*$');

    function isValidJavaIdentifier(msg, ident, elemId, panels, panelId) {
        if (isEmptyString(ident))
            return ErrorPopover.show(elemId, msg + ' is invalid!', panels, panelId);

        if (_.includes(JAVA_KEYWORDS, ident))
            return ErrorPopover.show(elemId, msg + ' could not contains reserved java keyword: "' + ident + '"!', panels, panelId);

        if (!VALID_JAVA_IDENTIFIER.test(ident))
            return ErrorPopover.show(elemId, msg + ' contains invalid identifier: "' + ident + '"!', panels, panelId);

        return true;
    }

    function getModel(obj, field) {
        let path = field.path;

        if (!isDefined(path) || !isDefined(obj))
            return obj;

        path = path.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
        path = path.replace(/^\./, '');           // strip a leading dot

        const segs = path.split('.');
        let root = obj;

        while (segs.length > 0) {
            const pathStep = segs.shift();

            if (typeof root[pathStep] === 'undefined')
                root[pathStep] = {};

            root = root[pathStep];
        }

        return root;
    }

    /**
     * Extract datasource from cache or cluster.
     *
     * @param object Cache or cluster to extract datasource.
     * @returns {*} Datasource object or null if not set.
     */
    function extractDataSource(object) {
        // Extract from cluster object
        if (_.get(object, 'discovery.kind') === 'Jdbc') {
            const datasource = object.discovery.Jdbc;

            if (datasource.dataSourceBean && datasource.dialect)
                return datasource;
        } // Extract from cache object
        else if (_.get(object, 'cacheStoreFactory.kind')) {
            const storeFactory = object.cacheStoreFactory[object.cacheStoreFactory.kind];

            if (storeFactory.dialect || (storeFactory.connectVia === 'DataSource'))
                return storeFactory;
        }

        return null;
    }

    const cacheStoreJdbcDialects = [
        {value: 'Generic', label: 'Generic JDBC'},
        {value: 'Oracle', label: 'Oracle'},
        {value: 'DB2', label: 'IBM DB2'},
        {value: 'SQLServer', label: 'Microsoft SQL Server'},
        {value: 'MySQL', label: 'MySQL'},
        {value: 'PostgreSQL', label: 'PostgreSQL'},
        {value: 'H2', label: 'H2 database'}
    ];

    function domainForStoreConfigured(domain) {
        const isEmpty = !isDefined(domain) || (isEmptyString(domain.databaseSchema) &&
            isEmptyString(domain.databaseTable) &&
            _.isEmpty(domain.keyFields) &&
            _.isEmpty(domain.valueFields));

        return !isEmpty;
    }

    const DS_CHECK_SUCCESS = {checked: true};

    /**
     * Compare datasources of caches or clusters.
     *
     * @param firstObj First cache or cluster.
     * @param secondObj Second cache or cluster.
     * @returns {*} Check result object.
     */
    function compareDataSources(firstObj, secondObj) {
        const firstDs = extractDataSource(firstObj);
        const secondDs = extractDataSource(secondObj);

        if (firstDs && secondDs) {
            const firstDB = firstDs.dialect;
            const secondDB = secondDs.dialect;

            if (firstDs.dataSourceBean === secondDs.dataSourceBean && firstDB !== secondDB)
                return {checked: false, firstObj, firstDB, secondObj, secondDB};
        }

        return DS_CHECK_SUCCESS;
    }

    function compareSQLSchemaNames(firstCache, secondCache) {
        const firstName = firstCache.sqlSchema;
        const secondName = secondCache.sqlSchema;

        if (firstName && secondName && (firstName === secondName))
            return {checked: false, firstCache, secondCache};

        return DS_CHECK_SUCCESS;
    }

    function toJavaName(prefix, name) {
        const javaName = name ? name.replace(/[^A-Za-z_0-9]+/g, '_') : 'dflt';

        return prefix + javaName.charAt(0).toLocaleUpperCase() + javaName.slice(1);
    }

    return {
        getModel,
        mkOptions(options) {
            return _.map(options, (option) => {
                return {value: option, label: isDefined(option) ? option : 'Not set'};
            });
        },
        isDefined,
        hasProperty(obj, props) {
            for (const propName in props) {
                if (props.hasOwnProperty(propName)) {
                    if (obj[propName])
                        return true;
                }
            }

            return false;
        },
        isEmptyString,
        SUPPORTED_JDBC_TYPES,
        findJdbcType(jdbcType) {
            const res = _.find(ALL_JDBC_TYPES, (item) => item.dbType === jdbcType);

            return res ? res : {dbName: 'Unknown', javaType: 'Unknown'};
        },
        javaBuiltInClasses,
        javaBuiltInTypes,
        isJavaBuiltInClass,
        isValidJavaIdentifier,
        isValidJavaClass(msg, ident, allowBuiltInClass, elemId, packageOnly, panels, panelId) {
            if (isEmptyString(ident))
                return ErrorPopover.show(elemId, msg + ' could not be empty!', panels, panelId);

            const parts = ident.split('.');

            const len = parts.length;

            if (!allowBuiltInClass && isJavaBuiltInClass(ident))
                return ErrorPopover.show(elemId, msg + ' should not be the Java build-in class!', panels, panelId);

            if (len < 2 && !isJavaBuiltInClass(ident) && !packageOnly)
                return ErrorPopover.show(elemId, msg + ' does not have package specified!', panels, panelId);

            for (let i = 0; i < parts.length; i++) {
                const part = parts[i];

                if (!isValidJavaIdentifier(msg, part, elemId, panels, panelId))
                    return false;
            }

            return true;
        },
        domainForQueryConfigured(domain) {
            const isEmpty = !isDefined(domain) || (_.isEmpty(domain.fields) &&
                _.isEmpty(domain.aliases) &&
                _.isEmpty(domain.indexes));

            return !isEmpty;
        },
        domainForStoreConfigured,
        download(type, name, data) {
            const file = document.createElement('a');

            file.setAttribute('href', 'data:' + type + ';charset=utf-8,' + data);
            file.setAttribute('download', name);
            file.setAttribute('target', '_self');

            file.style.display = 'none';

            document.body.appendChild(file);

            file.click();

            document.body.removeChild(file);
        },
        getQueryVariable(name) {
            const attrs = window.location.search.substring(1).split('&');
            const attr = _.find(attrs, (a) => a === name || (a.indexOf('=') >= 0 && a.substr(0, a.indexOf('=')) === name));

            if (!isDefined(attr))
                return null;

            if (attr === name)
                return true;

            return attr.substr(attr.indexOf('=') + 1);
        },
        cacheStoreJdbcDialects,
        cacheStoreJdbcDialectsLabel(dialect) {
            const found = _.find(cacheStoreJdbcDialects, (dialectVal) => dialectVal.value === dialect);

            return found ? found.label : null;
        },
        checkDataSources(cluster, caches, checkCacheExt) {
            let res = DS_CHECK_SUCCESS;

            _.find(caches, (curCache, curIx) => {
                res = compareDataSources(curCache, cluster);

                if (!res.checked)
                    return true;

                if (isDefined(checkCacheExt)) {
                    if (checkCacheExt._id !== curCache._id) {
                        res = compareDataSources(checkCacheExt, curCache);

                        return !res.checked;
                    }

                    return false;
                }

                return _.find(caches, (checkCache, checkIx) => {
                    if (checkIx < curIx) {
                        res = compareDataSources(checkCache, curCache);

                        return !res.checked;
                    }

                    return false;
                });
            });

            return res;
        },
        checkCacheSQLSchemas(caches, checkCacheExt) {
            let res = DS_CHECK_SUCCESS;

            _.find(caches, (curCache, curIx) => {
                if (isDefined(checkCacheExt)) {
                    if (checkCacheExt._id !== curCache._id) {
                        res = compareSQLSchemaNames(checkCacheExt, curCache);

                        return !res.checked;
                    }

                    return false;
                }

                return _.find(caches, (checkCache, checkIx) => {
                    if (checkIx < curIx) {
                        res = compareSQLSchemaNames(checkCache, curCache);

                        return !res.checked;
                    }

                    return false;
                });
            });

            return res;
        },
        autoCacheStoreConfiguration(cache, domains) {
            const cacheStoreFactory = isDefined(cache.cacheStoreFactory) &&
                isDefined(cache.cacheStoreFactory.kind);

            if (!cacheStoreFactory && _.findIndex(domains, domainForStoreConfigured) >= 0) {
                const dflt = !cache.readThrough && !cache.writeThrough;

                return {
                    cacheStoreFactory: {
                        kind: 'CacheJdbcPojoStoreFactory',
                        CacheJdbcPojoStoreFactory: {
                            dataSourceBean: toJavaName('ds', cache.name),
                            dialect: 'Generic'
                        },
                        CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}
                    },
                    readThrough: dflt || cache.readThrough,
                    writeThrough: dflt || cache.writeThrough
                };
            }
        },
        autoClusterSwapSpiConfiguration(cluster, caches) {
            const swapConfigured = cluster.swapSpaceSpi && cluster.swapSpaceSpi.kind;

            if (!swapConfigured && _.find(caches, (cache) => cache.swapEnabled))
                return {swapSpaceSpi: {kind: 'FileSwapSpaceSpi'}};

            return null;
        },
        randomString(len) {
            const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            const possibleLen = possible.length;

            let res = '';

            for (let i = 0; i < len; i++)
                res += possible.charAt(Math.floor(Math.random() * possibleLen));

            return res;
        },
        checkFieldValidators(ui) {
            const form = ui.inputForm;
            const errors = form.$error;
            const errKeys = Object.keys(errors);

            if (errKeys && errKeys.length > 0) {
                const firstErrorKey = errKeys[0];

                const firstError = errors[firstErrorKey][0];
                const actualError = firstError.$error[firstErrorKey][0];

                const errNameFull = actualError.$name;
                const errNameShort = errNameFull.endsWith('TextInput') ? errNameFull.substring(0, errNameFull.length - 9) : errNameFull;

                const extractErrorMessage = (errName) => {
                    try {
                        return errors[firstErrorKey][0].$errorMessages[errName][firstErrorKey];
                    }
                    catch (ignored) {
                        try {
                            return form[firstError.$name].$errorMessages[errName][firstErrorKey];
                        }
                        catch (ignited) {
                            return false;
                        }
                    }
                };

                const msg = extractErrorMessage(errNameFull) || extractErrorMessage(errNameShort) || 'Invalid value!';

                return ErrorPopover.show(errNameFull, msg, ui, firstError.$name);
            }

            return true;
        }
    };
}]];
