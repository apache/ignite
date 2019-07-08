/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const Util = require('util');
const Errors = require('../Errors');
const IgniteClient = require('../IgniteClient');
const ClientSocket = require('./ClientSocket');
const AffinityAwarenessUtils = require('./AffinityAwarenessUtils');
const BinaryUtils = require('./BinaryUtils');
const BinaryObject = require('../BinaryObject');
const ArgumentChecker = require('./ArgumentChecker');
const Logger = require('./Logger');

class Router {

    constructor(onStateChanged) {
        this._state = IgniteClient.STATE.DISCONNECTED;
        this._onStateChanged = onStateChanged;

        this._affinityAwarenessAllowed = false;
        // ClientSocket instance with no node UUID
        this._legacyConnection = null;
        // Array of endpoints which we are not connected to. Mostly used when Affinity Awareness is on
        this._inactiveEndpoints = [];

        /** Affinity Awareness only fields */
        // This flag indicates if we have at least two alive connections
        this._affinityAwarenessActive = false;
        // Contains the background task (promise) or null
        this._backgroundConnectTask = null;
        // {Node UUID -> ClientSocket instance}
        this._connections = {};
        // {cacheId -> CacheAffinityMap}
        this._distributionMap = new Map();
        this._affinityTopologyVer = null;
    }

    async connect(communicator, config) {
        if (this._state !== IgniteClient.STATE.DISCONNECTED) {
            throw new Errors.IllegalStateError();
        }

        // Wait for background task to stop before we move forward
        await this._waitBackgroundConnect();

        this._communicator = communicator;
        this._config = config;
        this._affinityAwarenessAllowed = config._affinityAwareness;
        this._inactiveEndpoints = [...config._endpoints];

        await this._connect();
    }

    disconnect() {
        if (this._state !== IgniteClient.STATE.DISCONNECTED) {
            this._changeState(IgniteClient.STATE.DISCONNECTED);

            for (const socket of this._getAllConnections()) {
                    socket.disconnect();
            }

            this._cleanUp();
        }
    }

    async send(opCode, payloadWriter, payloadReader = null, affinityHint = null) {
        if (this._state !== IgniteClient.STATE.CONNECTED) {
            throw new Errors.IllegalStateError();
        }

        if (this._affinityAwarenessActive && affinityHint) {
            await this._affinitySend(opCode, payloadWriter, payloadReader, affinityHint);
        }
        else {
            // If _affinityAwarenessActive flag is not set, we have exactly one connection
            // but it can be either a legacy one or a modern one (with node UUID)
            // If affinityHint has not been passed, we want to always use one socket (as long as it is alive)
            // because some requests (e.g., SQL cursor-related) require to be sent to the same cluster node
            await this._getAllConnections()[0].sendRequest(opCode, payloadWriter, payloadReader);
        }
    }

    async _connect() {
        const errors = new Array();
        const endpoints = this._inactiveEndpoints;
        const config = this._config;
        const communicator = this._communicator;
        const onSocketDisconnect = this._onSocketDisconnect.bind(this);
        const onAffinityTopologyChange = this._onAffinityTopologyChange.bind(this);
        const endpointsNum = endpoints.length;
        const random = this._getRandomInt(endpointsNum);

        this._changeState(IgniteClient.STATE.CONNECTING);

        for (let i = 0; i < endpoints.length; i++) {
            const index = (i + random) % endpointsNum;
            const endpoint = endpoints[index];

            try {
                const socket = new ClientSocket(
                    endpoint, config, communicator,
                    onSocketDisconnect,
                    onAffinityTopologyChange);

                await socket.connect();
                Logger.logDebug(Util.format('Connected to %s', endpoint));
                this._changeState(IgniteClient.STATE.CONNECTED);
                this._addConnection(socket);

                this._runBackgroundConnect();

                return;
            }
            catch (err) {
                Logger.logDebug(Util.format('Could not connect to %s. Error: "%s"', endpoint, err.message));
                errors.push(Util.format('[%s] %s', endpoint, err.message));
            }
        }

        const error = errors.join('; ');
        this._changeState(IgniteClient.STATE.DISCONNECTED, error);
        throw new Errors.IgniteClientError(error);
    }

    // Can be called when there are no alive connections left
    async _reconnect() {
        await this._waitBackgroundConnect();
        await this._connect();
    }

    _runBackgroundConnect() {
        if (this._affinityAwarenessAllowed && !this._backgroundConnectTask) {
            // Only one task can be active
            this._backgroundConnectTask = this._backgroundConnect();
            this._backgroundConnectTask.then(() => this._backgroundConnectTask = null);
        }
    }

    async _waitBackgroundConnect() {
        if (this._backgroundConnectTask) {
            await this._backgroundConnectTask;
        }
    }

    async _backgroundConnect() {
        // Local copy of _inactiveEndpoints to make sure the array is not being changed during the 'for' cycle
        const endpoints = [...this._inactiveEndpoints];
        const config = this._config;
        const communicator = this._communicator;
        const onSocketDisconnect = this._onSocketDisconnect.bind(this);
        const onAffinityTopologyChange = this._onAffinityTopologyChange.bind(this);

        for (const endpoint of endpoints) {
            try {
                const socket = new ClientSocket(
                    endpoint, config, communicator,
                    onSocketDisconnect,
                    onAffinityTopologyChange);

                await socket.connect();
                Logger.logDebug(Util.format('Connected (in background) to %s', endpoint));

                // While we were waiting for socket to connect, someone could call disconnect()
                if (this._state !== IgniteClient.STATE.CONNECTED) {
                    // If became not connected, stop this task
                    socket.disconnect();
                    return;
                }

                this._addConnection(socket);
            }
            catch (err) {
                Logger.logDebug(Util.format('Could not connect (in background) to %s. Error: "%s"', endpoint, err.message));

                // While we were waiting for socket to connect, someone could call disconnect()
                if (this._state !== IgniteClient.STATE.CONNECTED) {
                    // If became not connected, stop this task
                    socket.disconnect();
                    return;
                }
            }
        }
    }

    _cleanUp() {
        this._legacyConnection = null;
        this._inactiveEndpoints = [];

        this._affinityAwarenessActive = false;
        this._connections = {};
        this._distributionMap = new Map();
        this._affinityTopologyVer = null;
    }

    _getAllConnections() {
        const allConnections = Object.values(this._connections);

        if (this._legacyConnection) {
            allConnections.push(this._legacyConnection);
        }

        return allConnections;
    }

    _addConnection(socket) {
        const nodeUUID = socket.nodeUUID;

        if (this._affinityAwarenessAllowed && nodeUUID) {
            if (nodeUUID in this._connections) {
                // This can happen if the same node has several IPs
                // We will keep more fresh connection alive
                this._connections[nodeUUID].disconnect();
            }
            this._connections[nodeUUID] = socket;
        }
        else {
            if (this._legacyConnection) {
                // We already have a legacy connection
                // We will keep more fresh connection alive
                this._legacyConnection.disconnect();
            }
            this._legacyConnection = socket;
        }
        // Remove the endpoint from _inactiveEndpoints
        const index = this._inactiveEndpoints.indexOf(socket.endpoint);
        if (index > -1) {
            this._inactiveEndpoints.splice(index, 1);
        }

        if (!this._affinityAwarenessActive &&
            this._getAllConnections().length >= 2) {
            this._affinityAwarenessActive = true;
        }
    }

    _removeConnection(socket) {
        if (socket.nodeUUID in this._connections) {
            delete this._connections[socket.nodeUUID];
            // Add the endpoint to _inactiveEndpoints
            this._inactiveEndpoints.push(socket.endpoint);
        }
        else if (this._legacyConnection == socket) {
            this._legacyConnection = null;
            // Add the endpoint to _inactiveEndpoints
            this._inactiveEndpoints.push(socket.endpoint);
        }

        if (this._affinityAwarenessActive &&
            this._getAllConnections().length < 2) {
            this._affinityAwarenessActive = false;
        }
    }

    async _onSocketDisconnect(socket, error = null) {
        this._removeConnection(socket);

        if (this._getAllConnections().length != 0) {
            // We had more than one connection before this disconnection
            this._runBackgroundConnect();
            return;
        }

        try {
            await this._reconnect();
        }
        catch (err) {
            this._cleanUp();
        }
    }

    /** Affinity Awareness methods */

    async _affinitySend(opCode, payloadWriter, payloadReader, affinityHint) {
        let connection = await this._chooseConnection(affinityHint);

        while (true) {
            Logger.logDebug('Endpoint chosen: ' + connection.endpoint);

            try {
                await connection.sendRequest(opCode, payloadWriter, payloadReader);
                return;
            }
            catch (err) {
                if (!(err instanceof Errors.LostConnectionError)) {
                    throw err;
                }

                Logger.logDebug(connection.endpoint + ' is unavailable');

                this._removeConnection(connection);

                if (this._getAllConnections().length == 0) {
                    throw new Errors.LostConnectionError('Cluster is unavailable');
                }
            }

            connection = this._getRandomConnection();
            Logger.logDebug('Node has been chosen randomly');
        }
    }

    async _chooseConnection(affinityHint) {
        const cacheId = affinityHint.cacheId;

        if (!this._distributionMap.has(cacheId)) {
            Logger.logDebug('Distribution map does not have info for the cache ' + cacheId);
            Logger.logDebug('Node has been chosen randomly');
            // We are not awaiting here in order to not increase latency of requests
            this._getCachePartitions(cacheId);
            return this._getRandomConnection();
        }

        const cacheAffinityMap = this._distributionMap.get(cacheId);

        const nodeId = await this._determineNodeId(cacheAffinityMap,
                                                   affinityHint.key,
                                                   affinityHint.keyType);

        if (nodeId in this._connections) {
            Logger.logDebug('Node has been chosen by affinity');
            return this._connections[nodeId];
        }

        Logger.logDebug('Node has been chosen randomly');
        return this._getRandomConnection();
    }

    async _determineNodeId(cacheAffinityMap, key, keyType) {
        const partitionMap = cacheAffinityMap.partitionMapping;

        if (partitionMap.size == 0) {
            return null;
        }

        const keyAffinityMap = cacheAffinityMap.keyConfig;

        const affinityKeyInfo = await this._affinityKeyInfo(key, keyType);

        let affinityKey = affinityKeyInfo.key;
        let affinityKeyTypeCode = affinityKeyInfo.typeCode;

        if ('typeId' in affinityKeyInfo && keyAffinityMap.has(affinityKeyInfo.typeId)) {
            const affinityKeyTypeId = keyAffinityMap.get(affinityKeyInfo.typeId);

            if (affinityKey instanceof BinaryObject &&
                affinityKey._fields.has(affinityKeyTypeId)) {
                const field = affinityKey._fields.get(affinityKeyTypeId);
                affinityKey = await field.getValue();
                affinityKeyTypeCode = field.typeCode;
            }
        }

        const keyHash = await BinaryUtils.hashCode(affinityKey, this._communicator, affinityKeyTypeCode);
        const partition = AffinityAwarenessUtils.RendezvousAffinityFunction.calcPartition(keyHash, partitionMap.size);
        Logger.logDebug('Partition = ' + partition);

        const nodeId = partitionMap.get(partition);
        Logger.logDebug('Node ID = ' + nodeId);

        return nodeId;
    }

    async _affinityKeyInfo(key, keyType) {
        let typeCode = BinaryUtils.getTypeCode(keyType ? keyType : BinaryUtils.calcObjectType(key));

        if (typeCode == BinaryUtils.TYPE_CODE.BINARY_OBJECT) {
            return {'key': key, 'typeCode': typeCode, 'typeId': key._getTypeId()};
        }

        if (typeCode == BinaryUtils.TYPE_CODE.COMPLEX_OBJECT) {
            const binObj = await BinaryObject.fromObject(key, keyType);
            typeCode = BinaryUtils.TYPE_CODE.BINARY_OBJECT;

            return {'key': binObj, 'typeCode': typeCode, 'typeId': binObj._getTypeId()};
        }

        return {'key': key, 'typeCode': typeCode};
    }

    async _onAffinityTopologyChange(newVersion) {
        if (!this._versionIsNewer(newVersion)) {
            return;
        }

        Logger.logDebug('New topology version reported: ' + newVersion);

        this._affinityTopologyVer = newVersion;
        this._distributionMap = new Map();

        this._runBackgroundConnect();
    }

    async _getCachePartitions(cacheId) {
        Logger.logDebug('Getting cache partitions info...');

        try {
            await this.send(
                BinaryUtils.OPERATION.CACHE_PARTITIONS,
                async (payload) => {
                    // We always request partition map for one cache
                    payload.writeInteger(1);
                    payload.writeInteger(cacheId);
                },
                this._handleCachePartitions.bind(this));
        }
        catch (err) {
            Logger.logDebug('Could not get partitions info: ' + err.message);
        }
    }

    async _handleCachePartitions(payload) {
        const affinityTopologyVer = new AffinityAwarenessUtils.AffinityTopologyVersion(payload);

        if (this._versionIsNewer(affinityTopologyVer)) {
            this._distributionMap = new Map();
            this._affinityTopologyVer = affinityTopologyVer;
            Logger.logDebug('New affinity topology version: ' + affinityTopologyVer);
        } else if (this._versionIsOlder(affinityTopologyVer)) {
            return;
        }

        const groupsNum = payload.readInteger();

        for (let i = 0; i < groupsNum; i++) {
            const group = await AffinityAwarenessUtils.AffinityAwarenessCacheGroup.build(this._communicator, payload);
            // {partition -> nodeId}
            const partitionMapping = new Map();

            for (const [nodeId, partitions] of group.partitionMap) {
                for (const partition of partitions) {
                    partitionMapping.set(partition, nodeId);
                }
            }

            for (const [cacheId, config] of group.caches) {
                const cacheAffinityMap = new AffinityAwarenessUtils.CacheAffinityMap(cacheId, partitionMapping, config);
                this._distributionMap.set(cacheId, cacheAffinityMap);
            }
        }

        Logger.logDebug('Got cache partitions info');
    }

    _getRandomConnection() {
        const allConnections = this._getAllConnections();
        return allConnections[this._getRandomInt(allConnections.length)];
    }

    _changeState(state, reason = null) {
        if (Logger.debug) {
            Logger.logDebug(Util.format('Router state: %s -> %s'),
                this._getState(this._state),
                this._getState(state));
        }
        if (this._state !== state) {
            this._state = state;
            if (this._onStateChanged) {
                this._onStateChanged(state, reason);
            }
        }
    }

    _getState(state) {
        switch (state) {
            case IgniteClient.STATE.DISCONNECTED:
                return 'DISCONNECTED';
            case IgniteClient.STATE.CONNECTING:
                return 'CONNECTING';
            case IgniteClient.STATE.CONNECTED:
                return 'CONNECTED';
            default:
                return 'UNKNOWN';
        }
    }

    _versionIsNewer(version) {
        return this._affinityTopologyVer === null ||
               this._affinityTopologyVer.compareTo(version) < 0;
    }

    _versionIsOlder(version) {
        return this._affinityTopologyVer !== null &&
               this._affinityTopologyVer.compareTo(version) > 0;
    }

    // Returns a random integer between 0 and max - 1
    _getRandomInt(max) {
        if (max === 0) {
            return 0;
        }
        return Math.floor(Math.random() * max);
    }

    _sleep(milliseconds) {
        return new Promise(resolve => setTimeout(resolve, milliseconds));
    }
}

module.exports = Router;
