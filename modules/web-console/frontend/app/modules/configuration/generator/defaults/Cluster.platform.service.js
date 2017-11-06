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
