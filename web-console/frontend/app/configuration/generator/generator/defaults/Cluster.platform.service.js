

const enumValueMapper = (val) => _.capitalize(val);

const DFLT_CLUSTER = {
    atomics: {
        cacheMode: {
            clsName: 'Apache.Ignite.Core.Cache.Configuration.CacheMode',
            mapper: enumValueMapper
        }
    },
    transactionConfiguration: {
        defaultTxConcurrency: {
            clsName: 'Apache.Ignite.Core.Transactions.TransactionConcurrency',
            mapper: enumValueMapper
        },
        defaultTxIsolation: {
            clsName: 'Apache.Ignite.Core.Transactions.TransactionIsolation',
            mapper: enumValueMapper
        }
    }
};

export default class IgniteClusterPlatformDefaults {
    constructor() {
        Object.assign(this, DFLT_CLUSTER);
    }
}
