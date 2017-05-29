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

import { BehaviorSubject } from 'rxjs/BehaviorSubject';

const maskNull = (val) => _.isNil(val) ? 'null' : val;

const State = {
    INIT: 'INIT',
    AGENT_DISCONNECTED: 'AGENT_DISCONNECTED',
    CLUSTER_DISCONNECTED: 'CLUSTER_DISCONNECTED',
    CONNECTED: 'CONNECTED'
};

export default class IgniteAgentManager {
    static $inject = ['$rootScope', '$q', 'igniteSocketFactory', 'AgentModal'];

    constructor($root, $q, socketFactory, AgentModal) {
        this.$root = $root;
        this.$q = $q;
        this.socketFactory = socketFactory;

        /**
         * @type {AgentModal}
         */
        this.AgentModal = AgentModal;

        this.promises = new Set();

        $root.$on('$stateChangeSuccess', () => this.stopWatch());

        this.ignite2x = false;

        $root.$watch(() => _.get(this, 'cluster.clusterVersion'), (ver) => {
            if (_.isEmpty(ver))
                return;

            this.ignite2x = ver.startsWith('2.');
        }, true);

        /**
         * Connection to backend.
         * @type {Socket}
         */
        this.socket = null;

        this.connectionState = new BehaviorSubject(State.INIT);

        /**
         * Has agent with enabled demo mode.
         * @type {boolean}
         */
        this.hasDemo = false;

        this.clusters = [];
    }

    connect() {
        const self = this;

        if (_.nonNil(self.socket))
            return;

        self.socket = self.socketFactory();

        const onDisconnect = () => {
            self.connected = false;
        };

        self.socket.on('connect_error', onDisconnect);
        self.socket.on('disconnect', onDisconnect);

        self.connected = null;

        try {
            self.cluster = JSON.parse(localStorage.cluster);

            localStorage.removeItem('cluster');
        }
        catch (ignore) {
            // No-op.
        }

        self.socket.on('agents:stat', ({count, hasDemo, clusters}) => {
            self.hasDemo = hasDemo;

            const removed = _.differenceBy(self.clusters, clusters, 'id');

            if (_.nonEmpty(removed)) {
                _.pullAll(self.clusters, removed);

                if (self.cluster && _.find(removed, {id: self.cluster.id}))
                    self.cluster.disconnect = true;
            }

            const added = _.differenceBy(clusters, self.clusters, 'id');

            if (_.nonEmpty(added)) {
                self.clusters.push(...added);

                if (_.isNil(self.cluster))
                    self.cluster = _.head(added);

                if (self.cluster && _.find(added, {id: self.cluster.id}))
                    self.cluster.disconnect = false;
            }

            if (count === 0)
                self.connectionState.next(State.AGENT_DISCONNECTED);
            else if (self.$root.IgniteDemoMode || _.get(self.cluster, 'disconnect') === false)
                self.connectionState.next(State.CONNECTED);
            else
                self.connectionState.next(State.CLUSTER_DISCONNECTED);
        });
    }

    saveToStorage(cluster = this.cluster) {
        try {
            localStorage.cluster = JSON.stringify(cluster);
        } catch (ignore) {
            // No-op.
        }
    }

    /**
     * @param states
     * @returns {Promise}
     */
    awaitConnectionState(...states) {
        const defer = this.$q.defer();

        this.promises.add(defer);

        const subscription = this.connectionState.subscribe({
            next: (state) => {
                if (_.includes(states, state))
                    defer.resolve();
            }
        });

        return defer.promise
            .finally(() => {
                subscription.unsubscribe();

                this.promises.delete(defer);
            });
    }

    awaitCluster() {
        return this.awaitConnectionState(State.CONNECTED);
    }

    awaitAgent() {
        return this.awaitConnectionState(State.CONNECTED, State.CLUSTER_DISCONNECTED);
    }

    /**
     * @param {String} backText
     * @param {String} [backState]
     * @returns {Promise}
     */
    startAgentWatch(backText, backState) {
        const self = this;

        self.backText = backText;
        self.backState = backState;

        if (_.nonEmpty(self.clusters) && _.get(self.cluster, 'disconnect') === true) {
            self.cluster = _.head(self.clusters);

            self.connectionState.next(State.CONNECTED);
        }

        self.modalSubscription = this.connectionState.subscribe({
            next: (state) => {
                switch (state) {
                    case State.CONNECTED:
                    case State.CLUSTER_DISCONNECTED:
                        this.AgentModal.hide();

                        break;

                    case State.AGENT_DISCONNECTED:
                        this.AgentModal.agentDisconnected(self.backText, self.backState);

                        break;

                    default:
                    // Connection to backend is not established yet.
                }
            }
        });

        return self.awaitAgent();
    }

    /**
     * @param {String} backText
     * @param {String} [backState]
     * @returns {Promise}
     */
    startClusterWatch(backText, backState) {
        const self = this;

        self.backText = backText;
        self.backState = backState;

        if (_.nonEmpty(self.clusters) && _.get(self.cluster, 'disconnect') === true) {
            self.cluster = _.head(self.clusters);

            self.connectionState.next(State.CONNECTED);
        }

        self.modalSubscription = this.connectionState.subscribe({
            next: (state) => {
                switch (state) {
                    case State.CONNECTED:
                        this.AgentModal.hide();

                        break;

                    case State.AGENT_DISCONNECTED:
                        this.AgentModal.agentDisconnected(self.backText, self.backState);

                        break;

                    case State.CLUSTER_DISCONNECTED:
                        self.AgentModal.clusterDisconnected(self.backText, self.backState);

                        break;

                    default:
                    // Connection to backend is not established yet.
                }
            }
        });

        return self.awaitCluster();
    }

    stopWatch() {
        this.modalSubscription && this.modalSubscription.unsubscribe();

        this.AgentModal.hide();

        this.promises.forEach((promise) => promise.reject('Agent watch stopped.'));
    }

    /**
     *
     * @param {String} event
     * @param {Object} [args]
     * @returns {Promise}
     * @private
     */
    _emit(event, ...args) {
        if (!this.socket)
            return this.$q.reject('Failed to connect to server');

        const latch = this.$q.defer();

        const onDisconnect = () => {
            this.socket.removeListener('disconnect', onDisconnect);

            latch.reject('Connection to server was closed');
        };

        this.socket.on('disconnect', onDisconnect);

        args.push((err, res) => {
            this.socket.removeListener('disconnect', onDisconnect);

            if (err)
                latch.reject(err);

            latch.resolve(res);
        });

        this.socket.emit(event, ...args);

        return latch.promise;
    }

    drivers() {
        return this._emit('schemaImport:drivers');
    }

    /**
     * @param {Object} jdbcDriverJar
     * @param {Object} jdbcDriverClass
     * @param {Object} jdbcUrl
     * @param {Object} user
     * @param {Object} password
     * @returns {Promise}
     */
    schemas({jdbcDriverJar, jdbcDriverClass, jdbcUrl, user, password}) {
        const info = {user, password};

        return this._emit('schemaImport:schemas', {jdbcDriverJar, jdbcDriverClass, jdbcUrl, info});
    }

    /**
     * @param {Object} jdbcDriverJar
     * @param {Object} jdbcDriverClass
     * @param {Object} jdbcUrl
     * @param {Object} user
     * @param {Object} password
     * @param {Object} schemas
     * @param {Object} tablesOnly
     * @returns {Promise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
     */
    tables({jdbcDriverJar, jdbcDriverClass, jdbcUrl, user, password, schemas, tablesOnly}) {
        const info = {user, password};

        return this._emit('schemaImport:metadata', {jdbcDriverJar, jdbcDriverClass, jdbcUrl, info, schemas, tablesOnly});
    }

    /**
     *
     * @param {String} event
     * @param {Object} [args]
     * @returns {Promise}
     * @private
     */
    _rest(event, ...args) {
        return this._emit(event, _.get(this, 'cluster.id'), ...args);
    }

    /**
     * @param {Boolean} [attr]
     * @param {Boolean} [mtr]
     * @returns {Promise}
     */
    topology(attr = false, mtr = false) {
        return this._rest('node:rest', {cmd: 'top', attr, mtr});
    }

    /**
     * @param {String} [cacheName] Cache name.
     * @returns {Promise}
     */
    metadata(cacheName) {
        return this._rest('node:rest', {cmd: 'metadata', cacheName: maskNull(cacheName)})
            .then((caches) => {
                let types = [];

                const _compact = (className) => {
                    return className.replace('java.lang.', '').replace('java.util.', '').replace('java.sql.', '');
                };

                const _typeMapper = (meta, typeName) => {
                    const maskedName = _.isEmpty(meta.cacheName) ? '<default>' : meta.cacheName;

                    let fields = meta.fields[typeName];

                    let columns = [];

                    for (const fieldName in fields) {
                        if (fields.hasOwnProperty(fieldName)) {
                            const fieldClass = _compact(fields[fieldName]);

                            columns.push({
                                type: 'field',
                                name: fieldName,
                                clazz: fieldClass,
                                system: fieldName === '_KEY' || fieldName === '_VAL',
                                cacheName: meta.cacheName,
                                typeName,
                                maskedName
                            });
                        }
                    }

                    const indexes = [];

                    for (const index of meta.indexes[typeName]) {
                        fields = [];

                        for (const field of index.fields) {
                            fields.push({
                                type: 'index-field',
                                name: field,
                                order: index.descendings.indexOf(field) < 0,
                                unique: index.unique,
                                cacheName: meta.cacheName,
                                typeName,
                                maskedName
                            });
                        }

                        if (fields.length > 0) {
                            indexes.push({
                                type: 'index',
                                name: index.name,
                                children: fields,
                                cacheName: meta.cacheName,
                                typeName,
                                maskedName
                            });
                        }
                    }

                    columns = _.sortBy(columns, 'name');

                    if (!_.isEmpty(indexes)) {
                        columns = columns.concat({
                            type: 'indexes',
                            name: 'Indexes',
                            cacheName: meta.cacheName,
                            typeName,
                            maskedName,
                            children: indexes
                        });
                    }

                    return {
                        type: 'type',
                        cacheName: meta.cacheName || '',
                        typeName,
                        maskedName,
                        children: columns
                    };
                };

                for (const meta of caches) {
                    const cacheTypes = meta.types.map(_typeMapper.bind(null, meta));

                    if (!_.isEmpty(cacheTypes))
                        types = types.concat(cacheTypes);
                }

                return types;
            });
    }

    /**
     * @param {String} taskId
     * @param {Array.<String>|String} nids
     * @param {Array.<Object>} args
     */
    visorTask(taskId, nids, ...args) {
        args = _.map(args, (arg) => maskNull(arg));

        nids = _.isArray(nids) ? nids.join(';') : maskNull(nids);

        return this._rest('node:visor', taskId, nids, ...args);
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} enforceJoinOrder Flag whether enforce join order is enabled.
     * @param {Boolean} replicatedOnly Flag whether query contains only replicated tables.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {int} pageSz
     * @returns {Promise}
     */
    querySql(nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSz) {
        if (this.ignite2x) {
            return this.visorTask('querySqlX2', nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSz)
                .then(({error, result}) => {
                    if (_.isEmpty(error))
                        return result;

                    return Promise.reject(error);
                });
        }

        cacheName = _.isEmpty(cacheName) ? null : cacheName;

        let queryPromise;

        if (enforceJoinOrder)
            queryPromise = this.visorTask('querySqlV3', nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, local, pageSz);
        else if (nonCollocatedJoins)
            queryPromise = this.visorTask('querySqlV2', nid, cacheName, query, nonCollocatedJoins, local, pageSz);
        else
            queryPromise = this.visorTask('querySql', nid, cacheName, query, local, pageSz);

        return queryPromise
            .then(({key, value}) => {
                if (_.isEmpty(key))
                    return value;

                return Promise.reject(key);
            });
    }

    /**
     * @param {String} nid Node id.
     * @param {int} queryId
     * @param {int} pageSize
     * @returns {Promise}
     */
    queryNextPage(nid, queryId, pageSize) {
        return this.visorTask('queryFetch', nid, queryId, pageSize);
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} enforceJoinOrder Flag whether enforce join order is enabled.
     * @param {Boolean} replicatedOnly Flag whether query contains only replicated tables.
     * @param {Boolean} local Flag whether to execute query locally.
     * @returns {Promise}
     */
    querySqlGetAll(nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local) {
        // Page size for query.
        const pageSz = 1024;

        const fetchResult = (acc) => {
            if (!acc.hasMore)
                return acc;

            return this.queryNextPage(acc.responseNodeId, acc.queryId, pageSz)
                .then((res) => {
                    acc.rows = acc.rows.concat(res.rows);

                    acc.hasMore = res.hasMore;

                    return fetchResult(acc);
                });
        };

        return this.querySql(nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSz)
            .then(fetchResult);
    }

    /**
     * @param {String} nid Node id.
     * @param {int} [queryId]
     * @returns {Promise}
     */
    queryClose(nid, queryId) {
        if (this.ignite2x) {
            return this.visorTask('queryClose', nid, 'java.util.Map', 'java.util.UUID', 'java.util.Collection',
                nid + '=' + queryId);
        }

        return this.visorTask('queryClose', nid, queryId);
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} filter Filter text.
     * @param {Boolean} regEx Flag whether filter by regexp.
     * @param {Boolean} caseSensitive Case sensitive filtration.
     * @param {Boolean} near Scan near cache.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {int} pageSize Page size.
     * @returns {Promise}
     */
    queryScan(nid, cacheName, filter, regEx, caseSensitive, near, local, pageSize) {
        if (this.ignite2x) {
            return this.visorTask('queryScanX2', nid, cacheName, filter, regEx, caseSensitive, near, local, pageSize)
                .then(({error, result}) => {
                    if (_.isEmpty(error))
                        return result;

                    return Promise.reject(error);
                });
        }

        /** Prefix for node local key for SCAN near queries. */
        const SCAN_CACHE_WITH_FILTER = 'VISOR_SCAN_CACHE_WITH_FILTER';

        /** Prefix for node local key for SCAN near queries. */
        const SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE = 'VISOR_SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE';

        const prefix = caseSensitive ? SCAN_CACHE_WITH_FILTER_CASE_SENSITIVE : SCAN_CACHE_WITH_FILTER;
        const query = `${prefix}${filter}`;

        return this.querySql(nid, cacheName, query, false, false, false, local, pageSize);
    }

    /**
     /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} filter Filter text.
     * @param {Boolean} regEx Flag whether filter by regexp.
     * @param {Boolean} caseSensitive Case sensitive filtration.
     * @param {Boolean} near Scan near cache.
     * @param {Boolean} local Flag whether to execute query locally.
     * @returns {Promise}
     */
    queryScanGetAll(nid, cacheName, filter, regEx, caseSensitive, near, local) {
        // Page size for query.
        const pageSz = 1024;

        const fetchResult = (acc) => {
            if (!acc.hasMore)
                return acc;

            return this.queryNextPage(acc.responseNodeId, acc.queryId, pageSz)
                .then((res) => {
                    acc.rows = acc.rows.concat(res.rows);

                    acc.hasMore = res.hasMore;

                    return fetchResult(acc);
                });
        };

        return this.queryScan(nid, cacheName, filter, regEx, caseSensitive, near, local, pageSz)
            .then(fetchResult);
    }
}
