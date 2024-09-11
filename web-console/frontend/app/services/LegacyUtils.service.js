

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
    
    function domainForQueryConfigured(domain) {
        const isEmpty = !isDefined(domain) || (_.isEmpty(domain.fields) &&
            _.isEmpty(domain.aliases) &&
            _.isEmpty(domain.indexes));

        return !isEmpty;
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
        
        domainForQueryConfigured,

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
        }        
    };
}

service.$inject = ['IgniteErrorPopover'];
