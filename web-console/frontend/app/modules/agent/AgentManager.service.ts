

import _ from 'lodash';
import {nonEmpty, nonNil} from 'app/utils/lodashMixins';

import Sockette from 'sockette';

import {BehaviorSubject, Subject, Observable} from 'rxjs';
import {distinctUntilChanged, filter, first, map, pluck, shareReplay, tap} from 'rxjs/operators';

import uuidv4 from 'uuid/v4';

import AgentModal from './AgentModal.service';
// @ts-ignore
import Worker from './decompress.worker';
import SimpleWorkerPool from '../../utils/SimpleWorkerPool';


import {ClusterSecretsManager} from './types/ClusterSecretsManager';
import ClusterLoginService from './components/cluster-login/service';

import * as AgentTypes from 'app/types/Agent';
import {TransitionService} from '@uirouter/angularjs';
import VersionService from 'app/services/Version.service';
import UserNotifications from 'app/components/user-notifications/service';
import {DemoService} from 'app/modules/demo/Demo.module';

const __dbg = false;

enum State {
    INIT = 'INIT',
    AGENT_DISCONNECTED = 'AGENT_DISCONNECTED',
    CLUSTER_DISCONNECTED = 'CLUSTER_DISCONNECTED',
    CONNECTED = 'CONNECTED'
}

const IGNITE_2_0 = '2.0.0';

const COLLECT_BY_CACHE_GROUPS_SINCE = '2.7.0';
const AI_SUPPORT_SINCE = '2.16.99';

const EVENT_REST = 'node:rest';
const EVENT_VISOR = 'node:visor';

/**
 * Query execution result.
 * @typedef {{responseNodeId: String, queryId: String, columns: String[], rows: {Object[][]}, hasMore: Boolean, duration: Number}} VisorQueryResult
 */

/**
 * Query ping result.
 * @typedef {{}} VisorQueryPingResult
 */

/** Reserved cache names */
const RESERVED_CACHE_NAMES = [
    'ignite-sys-cache',
    'MetaStorage',
    'TxLog'
];

/** Error codes from o.a.i.internal.processors.restGridRestResponse.java */
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

/**
 * Save in local storage ID of specified cluster.
 *
 * @param {AgentTypes.ClusterStats} cluster Cluster to save it's ID in local storage.
 */
const _saveToStorage = (cluster) => {
    try {
        if (cluster)
            localStorage.clusterId = cluster.id;
    }
    catch (ignored) {
        // No-op.
    }
};

/**
 * Get ID of last active cluster from local storage.
 *
 * @return {string} ID of last active cluster.
 */
const getLastActiveClusterId = () => {
    try {
        return localStorage.clusterId;
    }
    catch (ignored) {
        localStorage.removeItem('clusterId');

        return null;
    }
};

const maskNull = (val) => {
    return _.isNil(val) ? 'null' : val;
}

class ConnectionState {
    cluster: AgentTypes.ClusterStats | null;
    clusters: AgentTypes.ClusterStats[];
    state: State;
    hasDemo: boolean;

    constructor() {
        this.cluster = null;
        this.clusters = [];
        this.state = State.INIT;
    }

    updateCluster(cluster: AgentTypes.ClusterStats) {
        this.cluster = cluster;

        return cluster;
    }

    update(demo: boolean, hasAgent: boolean, clusters: AgentTypes.ClusterStats[], hasDemo: boolean) {
        this.clusters = clusters;
        this.hasDemo = hasDemo;

        if (_.isEmpty(this.clusters))
            this.cluster = null;
        else if (_.isNil(this.cluster)) {
            const restoredCluster = _.find(clusters, {id: getLastActiveClusterId()});

            this.cluster = restoredCluster || _.head(clusters);

            _saveToStorage(this.cluster);
        }
        else {
            const updatedCluster = _.find(clusters, {id: this.cluster.id});

            if (updatedCluster)
                _.merge(this.cluster, updatedCluster);
            else {
                this.cluster = _.head(clusters);

                _saveToStorage(this.cluster);
            }
        }

        if (!hasAgent)
            this.state = State.AGENT_DISCONNECTED;
        else if (demo || this.cluster)
            this.state = State.CONNECTED;
        else
            this.state = State.CLUSTER_DISCONNECTED;
    }

    disconnect() {
        this.clusters = [];
        this.state = State.AGENT_DISCONNECTED;
    }
}

export default class AgentManager {
    static $inject = ['Demo', '$q', '$transitions', '$location', 'AgentModal', 'UserNotifications', 'IgniteVersion', 'ClusterLoginService'];

    currentCluster$: Observable<ConnectionState>;
    clusterIsActive$: Observable<boolean>;
    clusterIsAvailable$: Observable<boolean>;

    clusterVersion: string;

    features: AgentTypes.IgniteFeatures[];

    connectionSbj = new BehaviorSubject(new ConnectionState());

    clustersSecrets = new ClusterSecretsManager();

    pool = new SimpleWorkerPool('decompressor', Worker, 4);

    promises = new Set<angular.IDeferred<unknown>>();

    /** Websocket */
    ws = null;

    wsSubject = new Subject<AgentTypes.WebSocketResponse>();

    switchClusterListeners = new Set<() => Promise<any>>();

    addClusterSwitchListener(func) {
        this.switchClusterListeners.add(func);
    }

    removeClusterSwitchListener(func) {
        this.switchClusterListeners.delete(func);
    }

    constructor(
        private Demo: DemoService,
        private $q: ng.IQService,
        private $transitions: TransitionService,
        private $location: ng.ILocationService,
        private agentModal: AgentModal,
        private UserNotifications: UserNotifications,
        private Version: VersionService,
        private ClusterLoginSrv: ClusterLoginService
    ) {
        let prevCluster;

        this.currentCluster$ = this.connectionSbj.pipe(
            distinctUntilChanged(({ cluster }) => prevCluster === cluster),
            shareReplay(1),
            tap(({ cluster }) => prevCluster = cluster)
        );

        this.clusterIsActive$ = this.connectionSbj.pipe(
            map(({ cluster }) => cluster),
            filter((cluster) => Boolean(cluster)),
            pluck('active'),
            shareReplay(1)
        );

        this.clusterIsAvailable$ = this.connectionSbj.pipe(
            pluck('cluster'),
            map((cluster) => !!cluster),
            shareReplay(1)
        );

        this.connectionSbj.subscribe({
            next: ({cluster}) => {
                const version = this.getClusterVersion(cluster);

                if (_.isEmpty(version))
                    return;

                this.clusterVersion = version;
            }
        });

        this.currentCluster$.pipe(
            map(({ cluster }) => cluster),
            filter((cluster) => Boolean(cluster)),
            tap((cluster) => this.features = this.allFeatures(cluster))
        ).subscribe();
    }

    isDemoMode() {
        return !!this.Demo.enabled;
    }

    getClusterVersion(cluster) {
        return _.get(cluster, 'clusterVersion');
    }

    available(...sinceVersion) {        
        return this.Version.since(this.clusterVersion, ...sinceVersion);
    }

    connect() {
        if (nonNil(this.ws))
            return;

        const protocol = this.$location.protocol();
        const host = this.$location.host();
        const port = this.$location.port();

        const uri = `${protocol === 'https' ? 'wss' : 'ws'}://${host}:${port}/browsers?demoMode=${this.isDemoMode()}`;

        // Open websocket connection to backend.
        this.ws = new Sockette(uri, {
            onopen: (evt) => {
                if (__dbg)
                    console.log('[WS] Connected to server: ', evt);
            },
            onmessage: (msg) => {
                if (__dbg)
                    console.log('[WS] Received: ', msg);

                this.processWebSocketEvent(msg.data);
            },
            onreconnect: (evt) => {
                if (__dbg)
                    console.log('[WS] Reconnecting...', evt);
            },
            onclose: (evt) => {
                if (__dbg)
                    console.log('[WS] Disconnected from server: ', evt);

                const conn = this.connectionSbj.getValue();

                conn.disconnect();

                this.connectionSbj.next(conn);

                this.wsSubject.next({
                    requestId: 'any',
                    eventType: 'disconnected',
                    payload: 'none'
                });
            },
            onerror: (evt) => {
                if (__dbg)
                    console.log('[WS] Error on sending message to server: ', evt);
            }
        });
    }

    async processWebSocketEvent(data) {
        const evt = await this.pool.postMessage(data);

        const {requestId, eventType, payload} = evt;

        switch (eventType) {
            case 'agent:status':
                const {clusters, hasAgent, hasDemo} = payload;

                const conn = this.connectionSbj.getValue();

                conn.update(this.isDemoMode(), hasAgent, clusters, hasDemo);

                this.connectionSbj.next(conn);

                break;

            case 'admin:announcement':
                this.UserNotifications.announcement = payload;

                break;

            default:
                this.wsSubject.next({
                    requestId,
                    eventType,
                    payload
                });
        }
    }

    _sendWebSocketEvent(requestId, eventType, payload) {
        this.ws.json({
            requestId,
            eventType,
            payload
        });
    }

    /**
     * Save in local storage ID of specified cluster.
     * If cluster is not specified current cluster ID will be saved.
     *
     * @param {AgentTypes.ClusterStats} cluster Cluster to save it's ID in local storage.
     */
    saveToStorage(cluster = this.connectionSbj.getValue().cluster) {
        _saveToStorage(cluster);
    }

    updateCluster(newCluster) {
        const conn = this.connectionSbj.getValue();

        const oldCluster = _.find(conn.clusters, (cluster) => cluster.id === newCluster.id);

        if (oldCluster) {
            oldCluster.nids = newCluster.nids;
            oldCluster.addresses = newCluster.addresses;
            oldCluster.clusterVersion = this.getClusterVersion(newCluster);
            oldCluster.active = newCluster.active;

            if (conn.cluster && conn.cluster.id === newCluster.id)
                conn.cluster.active = newCluster.active;

            this.connectionSbj.next(conn);
        }
    }

    switchCluster(cluster) {
        return Promise.all(_.map([...this.switchClusterListeners], (lnr) => lnr()))
            .then(() => {
                const state = this.connectionSbj.getValue();

                state.updateCluster(cluster);

                this.connectionSbj.next(state);

                _saveToStorage(cluster);

                return Promise.resolve();
            });
    }

    /**
     * @param states
     * @returns {ng.IPromise}
     */
    awaitConnectionState(...states) {
        const defer: angular.IDeferred<unknown> = this.$q.defer();

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
     * Send message.
     *
     * @param {String} eventType
     * @param {Object} data
     * @returns {ng.IPromise}
     * @private
     */
    _sendToAgent(eventType, data = {}) {
        if (!this.ws)
            return this.$q.reject('Failed to connect to server');

        const latch = this.$q.defer();

        // Generate unique request ID in order to process response.
        const requestId = uuidv4();

        if (__dbg)
            console.log(`Sending request: ${eventType}, ${requestId}`);

        const reply$ = this.wsSubject.pipe(
            filter((evt) => evt.requestId === requestId || evt.eventType === 'disconnected'),
            first(),
            tap((evt) => {
                if (__dbg)
                    console.log('Received response: ', evt);

                if (evt.eventType === 'error')
                    latch.reject(evt.payload);
                else if (evt.eventType === 'disconnected')
                    latch.reject({message: 'Connection to web server was lost'});
                else
                    latch.resolve(evt.payload);
            })
        ).subscribe(() => {});

        try {
            this._sendWebSocketEvent(requestId, eventType, data);
        } catch (ignored) {
            reply$.unsubscribe();

            latch.reject({message: 'Failed to send request to web server'});
        }

        return latch.promise;
    }

    drivers() {
        return this._sendToAgent('schemaImport:drivers');
    }

    /**
     * @param {{jdbcDriverJar: String, jdbcDriverClass: String, jdbcUrl: String, userName: String, password: String, importSamples: Boolean}}
     * @returns {ng.IPromise}
     */
    schemas(dataSourceInfo) {
        return this._sendToAgent('schemaImport:schemas', dataSourceInfo);
    }

    /**
     * @param {{jdbcDriverJar: String, jdbcDriverClass: String, jdbcUrl: String, user: String, password: String, schemas: String, tablesOnly: Boolean}}
     * @returns {ng.IPromise} Promise on list of tables (see org.apache.ignite.schema.parser.DbTable java class)
     */
    tables(dataSourceInfo) {
        return this._sendToAgent('schemaImport:metadata', dataSourceInfo);
    }

    /** add@byron */
    startCluster(cluster) {
        return this._sendToAgent('agent:startCluster',cluster);
    }
    
    stopCluster(cluster) {
        return this._sendToAgent('agent:stopCluster',cluster);
    }
    
    callClusterService(cluster,serviceName,payload) {
        return this._sendToAgent('agent:callClusterService',{id:cluster.id,name:cluster.name,serviceName:serviceName,args:payload});
    }

    callCacheService(cluster,serviceName,payload) {
        return this._sendToAgent('agent:callClusterService',{id:cluster.id,name:cluster.name,serviceName:serviceName,serviceType:'CacheAgentService',args:payload});
    }
    
    callClusterCommand(cluster,cmdName,payload) {
        return this._sendToAgent('agent:callClusterCommand',{id:cluster.id,name:cluster.name,cmdName:cmdName,args:payload});
    }
    
    /** end@byron */
    
    /**
     * @param {Object} cluster
     * @param {Object} credentials
     * @param {String} event
     * @param {Object} params
     * @returns {ng.IPromise}
     * @private
     */
    _restOnActiveCluster(cluster, credentials, event, params) {
        return this._sendToAgent(event, {clusterId: cluster.id, params: _.merge({}, credentials, params)})
            .then((res:any) => {
                const {status = SuccessStatus.STATUS_SUCCESS} = res;

                switch (status) {
                    case SuccessStatus.STATUS_SUCCESS:
                        if (cluster.secured)
                            this.clustersSecrets.get(cluster.id).sessionToken = res.sessionToken;

                        return res.data;

                    case SuccessStatus.STATUS_FAILED:
                        if (res.error.startsWith('Failed to handle request - unknown session token (maybe expired session)')) {
                            this.clustersSecrets.get(cluster.id).resetSessionToken();

                            return this._restOnCluster(event, params);
                        }

                        throw new Error(res.error);

                    case SuccessStatus.AUTH_FAILED:
                        this.clustersSecrets.get(cluster.id).resetCredentials();

                        return this._restOnCluster(event, params, new Error('Incorrect user and/or password.'));

                    case SuccessStatus.SECURITY_CHECK_FAILED:
                        throw new Error('Access denied. You are not authorized to access this functionality.');

                    default:
                        throw new Error('Illegal status in node response:' + res.error);
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
        const now = Date.now()
        return this._restOnCluster(event, params)
            .then((res) => {
                if (typeof res === 'boolean') {
                    return res
                }
                // add@byron
                if('result' in res){
                    return res.result
                }
                // adapte query result
                if('items' in res && !('rows' in res)){
                    res['rows'] = res['items']
                }
                if('fieldsMetadata' in res && !('columns' in res)){
                    res['columns'] = res['fieldsMetadata']
                }
                if('last' in res && !('hasMore' in res)){
                    res['hasMore'] = !res['last']
                }
                if(!('duration' in res)){
                    const end = Date.now()
                    res['duration'] = end-now
                }
                return res
            });
    }

    /**
     * @param {String} event
     * @param {Object} params
     * @param {Error} repeatReason Error with reason of execution repeat.
     * @returns {Promise}
     * @private
     */
    _restOnCluster(event, params, repeatReason=null) {
        return this.connectionSbj.pipe(first()).toPromise()
            .then(({cluster}) => {
                if (_.isNil(cluster))
                    throw new Error('Failed to execute request on cluster.');

                if (cluster.secured) {
                    return Promise.resolve(this.clustersSecrets.get(cluster.id))
                        .then((secrets) => {
                            if (secrets.hasCredentials())
                                return secrets;

                            return this.ClusterLoginSrv.askCredentials(secrets, repeatReason)
                                .then((secrets) => {
                                    this.clustersSecrets.put(cluster.id, secrets);

                                    return secrets;
                                });
                        })
                        .then((secrets) => ({cluster, credentials: secrets.getCredentials()}));
                }

                return {cluster, credentials: {}};
            })
            .then(({cluster, credentials}) => this._restOnActiveCluster(cluster, credentials, event, params));
    }

    /**
     * @param {boolean} [attr] Collect node attributes.
     * @param {boolean} [mtr] Collect node metrics.
     * @param {boolean} [caches] Collect node caches descriptors.
     * @returns {Promise}
     */
    topology(attr = false, mtr = false, caches = false) {
        return this._restOnCluster(EVENT_REST, {cmd: 'top', attr, mtr, caches});
    }

    collectCacheNames(nid: string) {
        if (this.available(COLLECT_BY_CACHE_GROUPS_SINCE))
            return this.visorTask<AgentTypes.CacheNamesCollectorTaskResponse>('cacheNamesCollectorTask', nid,[]);

        return Promise.resolve({cacheGroupsNotAvailable: true});
    }

    text2sql(text:string,nid: string) {
        if (this.available(AI_SUPPORT_SINCE))
            return this.visorTask<AgentTypes.CacheNodesTaskResponse>('text2sql', nid,{text});

        return Promise.resolve({cacheGroupsNotAvailable: true});
    }

    text2gremlin(text:string,nid: string) {
        if (this.available(AI_SUPPORT_SINCE))
            return this.visorTask<AgentTypes.CacheNodesTaskResponse>('text2gremlin', nid,{text});

        return Promise.resolve({cacheGroupsNotAvailable: true});
    }

    publicCacheNames() {
        return this.collectCacheNames(null)
            .then((data) => {
                if (nonEmpty(data.caches)){
                    let caches = _.difference(_.keys(data.caches), RESERVED_CACHE_NAMES);
                    return _.filter(caches,(cache:string)=>{ 
                        return !cache.startsWith('INDEXES.') && !cache.startsWith('igfs-internal-')
                    });
                }

                return this.topology(false, false, true)
                    .then((nodes) => {
                        let caches = _.map(_.uniqBy(_.flatMap(nodes, 'caches'), 'name'), 'name');
                        return _.filter(caches,(cache)=>{ 
                            return !cache.startsWith('INDEXES.') && !cache.startsWith('_igfs-internal-')
                        });
                    });
            });
    }

    publicCaches() {
        return this.collectCacheNames(null)
            .then((data) => {
                let caches = _.difference(_.keys(data.caches), RESERVED_CACHE_NAMES);
                let cacheNames= _.filter(caches,(cache:string)=>{ 
                    return !cache.startsWith('INDEXES.') && !cache.startsWith('igfs-internal-')
                });
                let cachesInfo = _.map(cacheNames, (cacheName) => {
                    const schema = data.sqlSchemas && data.sqlSchemas[cacheName] || cacheName;
                    let comment = data.cachesComment && data.cachesComment[cacheName] || '';
                    if (comment) {
                        comment = ' (' + comment.slice(0, 24) + ')';
                    }
                    return {
                        value: cacheName,
                        label: cacheName + comment,
                        key: schema+'.'+ cacheName,
                        schema: schema       
                    }                
                });
                return cachesInfo;
            });
    }

    cacheNodes(cacheName: string) {
        return this.visorTask<AgentTypes.CacheNodesTaskResponse>('cacheNodesTaskX2', null, [cacheName]);
    }

    /**
     * @returns {Promise}
     */
    metadata(schema: string) {
        return this._restOnCluster(EVENT_REST, {cmd: 'metadata', cacheName: schema})
            .then((caches) => {
                let types = [];

                const _compact = (fieldName) => {
                    return fieldName.replace('java.lang.', '').replace('java.util.', '').replace('java.sql.', '');
                };

                const _typeMapper = (meta, typeName) => {
                    const maskedName = meta.cacheName;

                    let fields = meta.fields[typeName];

                    let columns = [];

                    for (const fieldName in fields) {
                        if (fields.hasOwnProperty(fieldName)) {                            
                            const filedInfo = fields[fieldName].split(' //')
                            const fieldClass = _compact(filedInfo[0]);
                            let description = null;
                            if(filedInfo.length>1){
                                description = filedInfo[1];
                            }
                            columns.push({
                                type: 'field',
                                name: fieldName,
                                clazz: fieldClass,
                                description: description,
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

                    let description = null;
                    if(meta.comments && meta.comments[typeName]){
                        description = meta.comments[typeName];
                    }

                    return {
                        type: 'type',
                        cacheName: meta.cacheName || '',
                        typeName,
                        maskedName,
                        description,
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
     * @param {Object|Array} args
     */
    visorTask<T>(taskId, nids, args): Promise<T> {        

        nids = _.isArray(nids) ? nids.join(';') : maskNull(nids);

        return this._executeOnCluster(EVENT_VISOR, {taskId, nids, args});
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} enforceJoinOrder Flag whether enforce join order is enabled.
     * @param {Boolean} replicatedOnly Flag whether query contains only replicated tables.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {Number} pageSize
     * @param {Boolean} [lazy] query flag.
     * @param {Boolean} [collocated] Collocated query.
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    querySql({nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSize, lazy = false, collocated = false}) {
        if (this.available(IGNITE_2_0)) {
            let args = {cacheName, qry:query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSize, lazy, collocated};

            
            return this.visorTask<AgentTypes.QuerySqlX2Response>('querySqlX2', nid, args).then((data) => {                
                if (!('error' in data) || !(data.error)){
                    if('result' in data){
                        return data.result
                    }
                    return data;
                }                   

                return Promise.reject(data.error);
            });
        }
        
    }

    /**
     * @param nid Node id.
     * @param queryId Query ID.
     * @param pageSize Page size in rows.
     * @returns Query execution result.
     */
    queryFetchFistsPage(nid: string, queryId: string, pageSize: number) {
        return this.visorTask<AgentTypes.QueryFetchFirstPageResult>('queryFetchFirstPage', nid, {qryId:queryId, pageSize}).then((data) => {
            if (!(data.error)){
                if(!(data.result)){
                    return data
                }
                return data.result;
            }                   

            return Promise.reject(data.error);
        });
    }

    /**
     * @param {String} nid Node id.
     * @param {String} queryId Query ID.
     * @returns {Promise.<VisorQueryPingResult>} Query execution result.
     */
    queryPing(nid, queryId) {   

        return Promise.resolve({queryPingSupported: false});
    }

    /**
     * @param {String} nid Node id.
     * @param {Number} queryId
     * @param {Number} pageSize
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    queryNextPage(nid, queryId, pageSize) {        

        return this.visorTask('queryFetch', nid, {qryId:queryId, pageSize});
    }

    /**
     * @param {String} nid Node id.
     * @param {Number} [queryId]
     * @returns {Promise<Void>}
     */
    queryClose(nid, queryId) {        

        return this.visorTask('queryClose', nid, {qryId:queryId});
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
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    queryScan(query) {
        const nid = query['nid']
        if (this.available(IGNITE_2_0)) {
            return this.visorTask('queryScanX2', nid, query)
                .then((data:any) => {
                    if (!(data.error)){
                        if(!(data.result)){
                            return data
                        }
                        return data.result;
                    }                   
    
                    return Promise.reject(data.error);
                    
                });
        }
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} enforceJoinOrder Flag whether enforce join order is enabled.
     * @param {Boolean} replicatedOnly Flag whether query contains only replicated tables.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {Number} pageSize
     * @param {Boolean} [lazy] query flag.
     * @param {Boolean} [collocated] Collocated query.
     * @returns {Promise.<VisorQueryResult>} Query execution result.
     */
    queryGremlin({nid, cacheName, query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSize, lazy = false, collocated = false}) {
        if (this.available(IGNITE_2_0)) {
            let args = {cacheName, qry:query, nonCollocatedJoins, enforceJoinOrder, replicatedOnly, local, pageSize, lazy, collocated};
            
            return this.visorTask<AgentTypes.QuerySqlX2Response>('queryGremlin', nid, args).then((data) => {                
                if (!('error' in data) || !(data.error)){
                    if('result' in data){
                        return data.result
                    }
                    return data;
                }                   

                return Promise.reject(data.error);
            });
        }
        
    }

    /**
     * Change cluster active state.
     *
     * @returns {Promise}
     */
    toggleClusterState() {
        const { cluster } = this.connectionSbj.getValue();
        const active = !cluster.active;
        if(active)
            return this.visorTask('toggleClusterState', null, {state:'ACTIVE'})
                .then(() => this.updateCluster({ ...cluster, active }));
        else
            return this.visorTask('toggleClusterState', null, {state:'INACTIVE'})
                .then(() => this.updateCluster({ ...cluster, active }));
    }

    hasCredentials(clusterId) {
        return this.clustersSecrets.get(clusterId).hasCredentials();
    }

    /**
     * Collect features supported by the cluster.
     *
     * @param cluster Cluster.
     * @return all supported features.
     */
    allFeatures(cluster: AgentTypes.ClusterStats): AgentTypes.IgniteFeatures[] {
        return _.reduce(AgentTypes.IgniteFeatures, (acc, featureId) => {
            if (_.isNumber(featureId) && this.featureSupported(cluster, featureId))
                acc.push(featureId);

            return acc;
        }, []);
    }

    /**
     * Check ignite feature is supported by cluster.
     *
     * @param cluster Cluster.
     * @param feature Feature to check enabled.
     * @return 'True' if feature is enabled or 'false' otherwise.
     */
    featureSupported(cluster: AgentTypes.ClusterStats, feature: AgentTypes.IgniteFeatures): boolean {
        if (_.isNil(cluster.supportedFeatures))
            return false;

        const bytes = this._base64ToArrayBuffer(cluster.supportedFeatures);

        const byteIdx = feature >>> 3;

        if (byteIdx >= bytes.length)
            return false;

        const bitIdx = feature & 0x7;

        return (bytes[byteIdx] & (1 << bitIdx)) !== 0;
    }

    /**
     * Decode base64 string to byte array.
     *
     * @param base64 Base64 string.
     * @return Result byte array.
     * @private
     */
    _base64ToArrayBuffer(base64: string): Uint8Array {
        const binary_string =  window.atob(base64);
        const len = binary_string.length;
        const bytes = new Uint8Array( len );

        for (let i = 0; i < len; i++)
            bytes[i] = binary_string.charCodeAt(i);

        return bytes;
    }
}
