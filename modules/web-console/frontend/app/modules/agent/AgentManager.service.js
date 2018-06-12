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

import _ from 'lodash';
import {nonEmpty, nonNil} from 'app/utils/lodashMixins';

import { BehaviorSubject } from 'rxjs/BehaviorSubject';

import Worker from './decompress.worker';
import SimpleWorkerPool from '../../utils/SimpleWorkerPool';
import maskNull from 'app/core/utils/maskNull';

const State = {
    DISCONNECTED: 'DISCONNECTED',
    AGENT_DISCONNECTED: 'AGENT_DISCONNECTED',
    CLUSTER_DISCONNECTED: 'CLUSTER_DISCONNECTED',
    CONNECTED: 'CONNECTED'
};

const IGNITE_2_0 = '2.0.0';
const LAZY_QUERY_SINCE = [['2.1.4-p1', '2.2.0'], '2.2.1'];
const COLLOCATED_QUERY_SINCE = [['2.3.5', '2.4.0'], ['2.4.6', '2.5.0'], '2.5.2'];

class ConnectionState {
    constructor(cluster) {
        this.agents = [];
        this.cluster = cluster;
        this.clusters = [];
        this.state = State.DISCONNECTED;
    }

    updateCluster(cluster) {
        this.cluster = cluster;
        this.cluster.connected = !!_.find(this.clusters, {id: this.cluster.id});

        return cluster;
    }

    update(demo, count, clusters) {
        _.forEach(clusters, (cluster) => {
            cluster.name = cluster.id;
        });

        this.clusters = clusters;

        if (_.isEmpty(this.clusters))
            this.cluster = null;

        if (_.isNil(this.cluster))
            this.cluster = _.head(clusters);

        if (nonNil(this.cluster))
            this.cluster.connected = !!_.find(clusters, {id: this.cluster.id});

        if (count === 0)
            this.state = State.AGENT_DISCONNECTED;
        else if (demo || _.get(this.cluster, 'connected'))
            this.state = State.CONNECTED;
        else
            this.state = State.CLUSTER_DISCONNECTED;
    }

    useConnectedCluster() {
        if (nonEmpty(this.clusters) && !this.cluster.connected) {
            this.cluster = _.head(this.clusters);

            this.cluster.connected = true;

            this.state = State.CONNECTED;
        }
    }

    disconnect() {
        this.agents = [];

        if (this.cluster)
            this.cluster.disconnect = true;

        this.clusters = [];
        this.state = State.DISCONNECTED;
    }
}

export default class IgniteAgentManager {
    static $inject = ['$rootScope', '$q', '$transitions', 'igniteSocketFactory', 'AgentModal', 'UserNotifications', 'IgniteVersion' ];

    constructor($root, $q, $transitions, socketFactory, AgentModal, UserNotifications, Version) {
        Object.assign(this, {$root, $q, $transitions, socketFactory, AgentModal, UserNotifications, Version});

        this.promises = new Set();

        this.pool = new SimpleWorkerPool('decompressor', Worker, 4);

        this.socket = null; // Connection to backend.

        let cluster;

        try {
            cluster = JSON.parse(localStorage.cluster);

            localStorage.removeItem('cluster');
        }
        catch (ignore) {
            // No-op.
        }

        this.connectionSbj = new BehaviorSubject(new ConnectionState(cluster));

        let prevCluster;

        this.currentCluster$ = this.connectionSbj
            .distinctUntilChanged(({ cluster }) => prevCluster === cluster)
            .do(({ cluster }) => prevCluster = cluster);

        this.clusterVersion = '2.4.0';

        if (!this.isDemoMode()) {
            this.connectionSbj.subscribe({
                next: ({cluster}) => {
                    const version = _.get(cluster, 'clusterVersion');

                    if (_.isEmpty(version))
                        return;

                    this.clusterVersion = version;
                }
            });
        }
    }

    isDemoMode() {
        return this.$root.IgniteDemoMode;
    }

    available(...sinceVersion) {
        return this.Version.since(this.clusterVersion, ...sinceVersion);
    }

    connect() {
        const self = this;

        if (nonNil(self.socket))
            return;

        self.socket = self.socketFactory();

        const onDisconnect = () => {
            const conn = self.connectionSbj.getValue();

            conn.disconnect();

            self.connectionSbj.next(conn);
        };

        self.socket.on('connect_error', onDisconnect);

        self.socket.on('disconnect', onDisconnect);

        self.socket.on('agents:stat', ({clusters, count}) => {
            const conn = self.connectionSbj.getValue();

            conn.update(self.isDemoMode(), count, clusters);

            self.connectionSbj.next(conn);
        });

        self.socket.on('cluster:changed', (cluster) => this.updateCluster(cluster));

        self.socket.on('user:notifications', (notification) => this.UserNotifications.notification = notification);
    }

    saveToStorage(cluster = this.connectionSbj.getValue().cluster) {
        try {
            localStorage.cluster = JSON.stringify(cluster);
        } catch (ignore) {
            // No-op.
        }
    }

    updateCluster(newCluster) {
        const state = this.connectionSbj.getValue();

        const oldCluster = _.find(state.clusters, (cluster) => cluster.id === newCluster.id);

        if (!_.isNil(oldCluster)) {
            oldCluster.nids = newCluster.nids;
            oldCluster.addresses = newCluster.addresses;
            oldCluster.clusterVersion = newCluster.clusterVersion;
            oldCluster.active = newCluster.active;

            this.connectionSbj.next(state);
        }
    }

    switchCluster(cluster) {
        const state = this.connectionSbj.getValue();

        state.updateCluster(cluster);

        this.connectionSbj.next(state);

        this.saveToStorage(cluster);
    }

    /**
     * @param states
     * @returns {Promise}
     */
    awaitConnectionState(...states) {
        const defer = this.$q.defer();

        this.promises.add(defer);

        const subscription = this.connectionSbj.subscribe({
            next: ({state}) => {
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

        const conn = self.connectionSbj.getValue();

        conn.useConnectedCluster();

        self.connectionSbj.next(conn);

        this.modalSubscription && this.modalSubscription.unsubscribe();

        self.modalSubscription = this.connectionSbj.subscribe({
            next: ({state}) => {
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

        const conn = self.connectionSbj.getValue();

        conn.useConnectedCluster();

        self.connectionSbj.next(conn);

        this.modalSubscription && this.modalSubscription.unsubscribe();

        self.modalSubscription = this.connectionSbj.subscribe({
            next: ({state}) => {
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

        self.$transitions.onExit({}, () => self.stopWatch());

        return self.awaitCluster();
    }

    stopWatch() {
        this.modalSubscription && this.modalSubscription.unsubscribe();

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
                return latch.reject(err);

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
        return this._emit(event, _.get(this.connectionSbj.getValue(), 'cluster.id'), ...args)
            .then((data) => {
                if (data.zipped)
                    return this.pool.postMessage(data.data);

                return data;
            });
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
     * @returns {Promise}
     */
    metadata() {
        return this._rest('node:rest', {cmd: 'metadata'})
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
     * @param {Boolean} lazy query flag.
     * @param {Boolean} collocated Collocated query.
     * @returns {Promise}
     */
    querySql(nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSz, lazy, collocated) {
        if (this.available(IGNITE_2_0)) {
            let args = [cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSz];

            if (this.available(...COLLOCATED_QUERY_SINCE))
                args = [...args, lazy, collocated];
            else if (this.available(...LAZY_QUERY_SINCE))
                args = [...args, lazy];

            return this.visorTask('querySqlX2', nid, ...args).then(({error, result}) => {
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
        if (this.available(IGNITE_2_0))
            return this.visorTask('queryFetchX2', nid, queryId, pageSize);

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
     * @param {Boolean} lazy query flag.
     * @param {Boolean} collocated Collocated query.
     * @returns {Promise}
     */
    querySqlGetAll(nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, lazy, collocated) {
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

        return this.querySql(nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSz, lazy, collocated)
            .then(fetchResult);
    }

    /**
     * @param {String} nid Node id.
     * @param {int} [queryId]
     * @returns {Promise}
     */
    queryClose(nid, queryId) {
        if (this.available(IGNITE_2_0)) {
            return this.visorTask('queryCloseX2', nid, 'java.util.Map', 'java.util.UUID', 'java.util.Collection',
                nid + '=' + queryId);
        }

        return this.visorTask('queryClose', nid, nid, queryId);
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
        if (this.available(IGNITE_2_0)) {
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

    /**
     * Change cluster active state.
     *
     * @returns {Promise}
     */
    toggleClusterState() {
        const state = this.connectionSbj.getValue();
        const active = !state.cluster.active;

        return this.visorTask('toggleClusterState', null, active)
            .then(() => state.updateCluster(Object.assign(state.cluster, { active })));
    }
}
