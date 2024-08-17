

import saver from 'file-saver';
import _ from 'lodash';


// TODO: Refactor this service for legacy tables with more than one input field.
/**
 * @param {import('./ErrorPopover.service').default} ErrorPopover
 */
export default function service(ErrorPopover) {
    function isDefined(v) {
        return !_.isNil(v);
    }

    function isEmptyString(s) {
        if (isDefined(s))
            return s.trim().length === 0;

        return true;
    }

    /**
     * Extract datasource from cache or cluster.
     *
     * @param object Cache or cluster to extract datasource.
     * @returns {*} Datasource object or null if not set.
     */
    function extractDataSource(object) {
        let datasource = null;

        // Extract from cluster object
        if (_.get(object, 'discovery.kind') === 'Jdbc') {
            datasource = object.discovery.Jdbc;

            if (datasource.dataSourceBean && datasource.dialect)
                return datasource;
        } // Extract from JDBC checkpoint configuration.
        else if (_.get(object, 'kind') === 'JDBC') {
            datasource = object.JDBC;

            if (datasource.dataSourceBean && datasource.dialect)
                return datasource;
        } // Extract from cache object
        else if (_.get(object, 'cacheStoreFactory.kind')) {
            datasource = object.cacheStoreFactory[object.cacheStoreFactory.kind];

            if (datasource.dialect || (datasource.connectVia === 'DataSource'))
                return datasource;
        }

        return null;
    }

    function domainForStoreConfigured(domain) {
        const isEmpty = !isDefined(domain) || (isEmptyString(domain.databaseSchema) &&
            isEmptyString(domain.databaseTable) &&
            _.isEmpty(domain.keyFields) &&
            _.isEmpty(domain.valueFields));

        return !isEmpty;
    }
    
    return {

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
        
        domainForQueryConfigured(domain) {
            const isEmpty = !isDefined(domain) || (_.isEmpty(domain.fields) &&
                _.isEmpty(domain.aliases) &&
                _.isEmpty(domain.indexes));

            return !isEmpty;
        },
        domainForStoreConfigured,
        download(type = 'application/octet-stream', name = 'file.txt', data = '') {
            const file = new Blob([data], { type: `${type};charset=utf-8`});

            saver.saveAs(file, name, false);
        },
                
        autoCacheStoreConfiguration(cache, domains, jndiName, dialect) {
            const cacheStoreFactory = isDefined(cache.cacheStoreFactory) &&
                isDefined(cache.cacheStoreFactory.kind);

            if (!cacheStoreFactory && _.findIndex(domains, domainForStoreConfigured) >= 0) {
                const dflt = !cache.readThrough && !cache.writeThrough;

                return {
                    cacheStoreFactory: {
                        kind: 'CacheJdbcPojoStoreFactory',
                        CacheJdbcPojoStoreFactory: {
                            dataSourceBean: jndiName,
                            dialect: dialect
                        },
                        CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}
                    },
                    readThrough: dflt || cache.readThrough,
                    writeThrough: dflt || cache.writeThrough
                };
            }

            return {};
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

                const err = firstError.$error[firstErrorKey];

                const actualError = _.isArray(err) ? err[0] : firstError;

                const errNameFull = actualError.$name;
                const errNameShort = errNameFull.endsWith('TextInput') ? errNameFull.substring(0, errNameFull.length - 9) : errNameFull;

                const extractErrorMessage = (errName) => {
                    try {
                        return errors[firstErrorKey][0].$errorMessages[errName][firstErrorKey];
                    }
                    catch (ignored1) {
                        try {
                            return form[firstError.$name].$errorMessages[errName][firstErrorKey];
                        }
                        catch (ignored2) {
                            try {
                                return form.$errorMessages[errName][firstErrorKey];
                            }
                            catch (ignored3) {
                                return false;
                            }
                        }
                    }
                };

                const msg = extractErrorMessage(errNameFull) || extractErrorMessage(errNameShort) || 'Invalid value!';

                return ErrorPopover.show(errNameFull, msg, ui, firstError.$name);
            }

            return true;
        }
    };
}

service.$inject = ['IgniteErrorPopover'];
