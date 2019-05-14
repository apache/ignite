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
import 'rxjs/add/operator/first';

import AgentModal from './AgentModal.service';
// @ts-ignore
import Worker from './decompress.worker';
import SimpleWorkerPool from '../../utils/SimpleWorkerPool';
import maskNull from 'app/core/utils/maskNull';

import {ClusterSecretsManager} from './types/ClusterSecretsManager';
import ClusterLoginService from './components/cluster-login/service';

const State = {
    DISCONNECTED: 'DISCONNECTED',
    AGENT_DISCONNECTED: 'AGENT_DISCONNECTED',
    CLUSTER_DISCONNECTED: 'CLUSTER_DISCONNECTED',
    CONNECTED: 'CONNECTED'
};

const IGNITE_2_0 = '2.0.0';
const LAZY_QUERY_SINCE = [['2.1.4-p1', '2.2.0'], '2.2.1'];
const COLLOCATED_QUERY_SINCE = [['2.3.5', '2.4.0'], ['2.4.6', '2.5.0'], '2.5.2'];

// Error codes from o.a.i.internal.processors.restGridRestResponse.java

const SuccessStatus = {
    /** Command succeeded. */
    STATUS_SUCCESS: 0,
    /** Command failed. */
    STATUS_FAILED: 1,
    /** Authentication failure. */
    AUTH_FAILED: 2,
    /** Security check failed. */
    SECURITY_CHECK_FAILED: 3
};

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

        if (this.cluster)
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

export default class AgentManager {
    static $inject = ['$rootScope', '$q', '$transitions', 'igniteSocketFactory', AgentModal.name, 'UserNotifications', 'IgniteVersion', ClusterLoginService.name];

    /** @type {ng.IScope} */
    $root;

    /** @type {ng.IQService} */
    $q;

    /** @type {AgentModal} */
    agentModal;

    /** @type {ClusterLoginService} */
    ClusterLoginSrv;

    /** @type {String} */
    clusterVersion = '2.4.0';

    connectionSbj = new BehaviorSubject(new ConnectionState(AgentManager.restoreActiveCluster()));

    /** @type {ClusterSecretsManager} */
    clustersSecrets = new ClusterSecretsManager();

    pool = new SimpleWorkerPool('decompressor', Worker, 4);

    /** @type {Set<ng.IDifferend>} */
    promises = new Set();

    socket = null;

    static restoreActiveCluster() {
        try {
            return JSON.parse(localStorage.cluster);
        }
        catch (ignore) {
            return null;
        }
        finally {
            localStorage.removeItem('cluster');
        }
    }

    constructor($root, $q, $transitions, socketFactory, agentModal, UserNotifications, Version, ClusterLoginSrv) {
        Object.assign(this, {$root, $q, $transitions, socketFactory, agentModal, UserNotifications, Version, ClusterLoginSrv});

        let prevCluster;

        this.currentCluster$ = this.connectionSbj
            .distinctUntilChanged(({ cluster }) => prevCluster === cluster)
            .do(({ cluster }) => prevCluster = cluster);

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
        if (nonNil(this.socket))
            return;

        this.socket = this.socketFactory();

        const onDisconnect = () => {
            const conn = this.connectionSbj.getValue();

            conn.disconnect();

            this.connectionSbj.next(conn);
        };

        this.socket.on('connect_error', onDisconnect);

        this.socket.on('disconnect', onDisconnect);

        this.socket.on('agents:stat', ({clusters, count}) => {
            const conn = this.connectionSbj.getValue();

            conn.update(this.isDemoMode(), count, clusters);

            this.connectionSbj.next(conn);
        });

        this.socket.on('cluster:changed', (cluster) => this.updateCluster(cluster));

        this.socket.on('user:notifications', (notification) => this.UserNotifications.notification = notification);
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
     * @returns {ng.IPromise}
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
     * @returns {ng.IPromise}
     */
    startAgentWatch(backText, backState) {
        this.backText = backText;
        this.backState = backState;

        const conn = this.connectionSbj.getValue();

        conn.useConnectedCluster();

        this.connectionSbj.next(conn);

        this.modalSubscription && this.modalSubscription.unsubscribe();

        this.modalSubscription = this.connectionSbj.subscribe({
            next: ({state}) => {
                switch (state) {
                    case State.CONNECTED:
                    case State.CLUSTER_DISCONNECTED:
                        this.agentModal.hide();

                        break;

                    case State.AGENT_DISCONNECTED:
                        this.agentModal.agentDisconnected(this.backText, this.backState);

                        break;

                    default:
                        // Connection to backend is not established yet.
                }
            }
        });

        return this.awaitAgent();
    }

    /**
     * @param {String} backText
     * @param {String} [backState]
     * @returns {ng.IPromise}
     */
    startClusterWatch(backText, backState) {
        this.backText = backText;
        this.backState = backState;

        const conn = this.connectionSbj.getValue();

        conn.useConnectedCluster();

        this.connectionSbj.next(conn);

        this.modalSubscription && this.modalSubscription.unsubscribe();

        this.modalSubscription = this.connectionSbj.subscribe({
            next: ({state}) => {
                switch (state) {
                    case State.CONNECTED:
                        this.agentModal.hide();

                        break;

                    case State.AGENT_DISCONNECTED:
                        this.agentModal.agentDisconnected(this.backText, this.backState);

                        break;

                    case State.CLUSTER_DISCONNECTED:
                        this.agentModal.clusterDisconnected(this.backText, this.backState);

                        break;

                    default:
                    // Connection to backend is not established yet.
                }
            }
        });

        this.$transitions.onExit({}, () => this.stopWatch());

        return this.awaitCluster();
    }

    stopWatch() {
        this.modalSubscription && this.modalSubscription.unsubscribe();

        this.promises.forEach((promise) => promise.reject('Agent watch stopped.'));
    }

    /**
     *
     * @param {String} event
     * @param {Object} [payload]
     * @returns {ng.IPromise}
     * @private
     */
    _sendToAgent(event, payload = {}) {
        if (!this.socket)
            return this.$q.reject('Failed to connect to server');

        const latch = this.$q.defer();

        const onDisconnect = () => {
            this.socket.removeListener('disconnect', onDisconnect);

            latch.reject('Connection to server was closed');
        };

        this.socket.on('disconnect', onDisconnect);

        this.socket.emit(event, payload, (err, res) => {
            this.socket.removeListener('disconnect', onDisconnect);

            if (err)
                return latch.reject(err);

            latch.resolve(res);
        });

        return latch.promise;
    }

    drivers() {
        return this._sendToAgent('schemaImport:drivers');
    }

    /**
     * @param {{jdbcDriverJar: String, jdbcDriverClass: String, jdbcUrl: String, user: String, password: String}}
     * @returns {ng.IPromise}
     */
    schemas({jdbcDriverJar, jdbcDriverClass, jdbcUrl, user, password}) {
        const info = {user, password};

        return this._sendToAgent('schemaImport:schemas', {jdbcDriverJar, jdbcDriverClass, jdbcUrl, info});
    }

    /**
     * @param {{jdbcDriverJar: String, jdbcDriverClass: String, jdbcUrl: String, user: String, password: String, schemas: String, tablesOnly: String}}
     * @returns {ng.IPromise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
     */
    tables({jdbcDriverJar, jdbcDriverClass, jdbcUrl, user, password, schemas, tablesOnly}) {
        const info = {user, password};

        return this._sendToAgent('schemaImport:metadata', {jdbcDriverJar, jdbcDriverClass, jdbcUrl, info, schemas, tablesOnly});
    }

    /**
     * @param {Object} cluster
     * @param {Object} credentials
     * @param {String} event
     * @param {Object} params
     * @returns {ng.IPromise}
     * @private
     */
    _executeOnActiveCluster(cluster, credentials, event, params) {
        return this._sendToAgent(event, {clusterId: cluster.id, params, credentials})
            .then((res) => {
                const {status = SuccessStatus.STATUS_SUCCESS} = res;

                switch (status) {
                    case SuccessStatus.STATUS_SUCCESS:
                        if (cluster.secured)
                            this.clustersSecrets.get(cluster.id).sessionToken = res.sessionToken;

                        if (res.zipped)
                            return this.pool.postMessage(res.data);

                        return res;

                    case SuccessStatus.STATUS_FAILED:
                        if (res.error.startsWith('Failed to handle request - unknown session token (maybe expired session)')) {
                            this.clustersSecrets.get(cluster.id).resetSessionToken();

                            return this._executeOnCluster(event, params);
                        }

                        throw new Error(res.error);

                    case SuccessStatus.AUTH_FAILED:
                        this.clustersSecrets.get(cluster.id).resetCredentials();

                        throw new Error('Cluster authentication failed. Incorrect user and/or password.');

                    case SuccessStatus.SECURITY_CHECK_FAILED:
                        throw new Error('Access denied. You are not authorized to access this functionality.');

                    default:
                        throw new Error('Illegal status in node response');
                }
            });
    }

    /**
     * @param {String} event
     * @param {Object} params
     * @returns {Promise}
     * @private
     */
    _executeOnCluster(event, params) {
        if (this.isDemoMode())
            return Promise.resolve(this._executeOnActiveCluster({}, {}, event, params));

        return this.connectionSbj.first().toPromise()
            .then(({cluster}) => {
                if (_.isNil(cluster))
                    throw new Error('Failed to execute request on cluster.');

                if (cluster.secured) {
                    return Promise.resolve(this.clustersSecrets.get(cluster.id))
                        .then((secrets) => {
                            if (secrets.hasCredentials())
                                return secrets;

                            return this.ClusterLoginSrv.askCredentials(secrets)
                                .then((secrets) => {
                                    this.clustersSecrets.put(cluster.id, secrets);

                                    return secrets;
                                });
                        })
                        .then((secrets) => ({cluster, credentials: secrets.getCredentials()}));
                }

                return {cluster, credentials: {}};
            })
            .then(({cluster, credentials}) => this._executeOnActiveCluster(cluster, credentials, event, params));
    }

    /**
     * @param {Boolean} [attr]
     * @param {Boolean} [mtr]
     * @returns {Promise}
     */
    topology(attr = false, mtr = false) {
        return this._executeOnCluster('node:rest', {cmd: 'top', attr, mtr});
    }

    /**
     * @returns {Promise}
     */
    metadata() {
        return this._executeOnCluster('node:rest', {cmd: 'metadata'})
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

                    if (nonEmpty(indexes)) {
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

        return this._executeOnCluster('node:visor', {taskId, nids, args});
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} enforceJoinOrder Flag whether enforce join order is enabled.
     * @param {Boolean} replicatedOnly Flag whether query contains only replicated tables.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {Number} pageSz
     * @param {Boolean} [lazy] query flag.
     * @param {Boolean} [collocated] Collocated query.
     * @returns {Promise}
     */
    querySql(nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSz, lazy = false, collocated = false) {
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
     * @param {Number} queryId
     * @param {Number} pageSize
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
     * @param {Number} [queryId]
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
     * @param {Number} pageSize Page size.
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
        const { cluster } = this.connectionSbj.getValue();
        const active = !cluster.active;

        return this.visorTask('toggleClusterState', null, active)
            .then(() => this.updateCluster({ ...cluster, active }));
    }

    hasCredentials(clusterId) {
        return this.clustersSecrets.get(clusterId).hasCredentials();
    }
}
