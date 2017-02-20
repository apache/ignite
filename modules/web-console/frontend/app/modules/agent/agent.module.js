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

import angular from 'angular';
import io from 'socket.io-client'; // eslint-disable-line no-unused-vars

const maskNull = (val) => _.isEmpty(val) ? 'null' : val;

class IgniteAgentMonitor {
    constructor(socketFactory, $root, $q, $state, $modal, Messages) {
        this._scope = $root.$new();

        $root.$watch('user', () => {
            this._scope.user = $root.user;
        });

        $root.$on('$stateChangeStart', () => {
            this.stopWatch();
        });

        // Pre-fetch modal dialogs.
        this._downloadAgentModal = $modal({
            scope: this._scope,
            templateUrl: '/templates/agent-download.html',
            show: false,
            backdrop: 'static',
            keyboard: false
        });

        const _modalHide = this._downloadAgentModal.hide;

        /**
         * Special dialog hide function.
         */
        this._downloadAgentModal.hide = () => {
            Messages.hideAlert();

            _modalHide();
        };

        /**
         * Close dialog and go by specified link.
         */
        this._scope.back = () => {
            this.stopWatch();

            if (this._scope.backState)
                this._scope.$$postDigest(() => $state.go(this._scope.backState));
        };

        this._scope.hasAgents = null;
        this._scope.showModal = false;

        /**
         * @type {Socket}
         */
        this._socket = null;

        this._socketFactory = socketFactory;

        this._$q = $q;

        this.Messages = Messages;
    }

    /**
     * @private
     */
    checkModal() {
        if (this._scope.showModal && !this._scope.hasAgents)
            this._downloadAgentModal.$promise.then(this._downloadAgentModal.show);
        else if ((this._scope.hasAgents || !this._scope.showModal) && this._downloadAgentModal.$isShown)
            this._downloadAgentModal.hide();
    }

    /**
     * @returns {Promise}
     */
    awaitAgent() {
        if (this._scope.hasAgents)
            return this._$q.when();

        const latch = this._$q.defer();

        const offConnected = this._scope.$on('agent:watch', (event, state) => {
            if (state !== 'DISCONNECTED')
                offConnected();

            if (state === 'CONNECTED')
                return latch.resolve();

            if (state === 'STOPPED')
                return latch.reject('Agent watch stopped.');
        });

        return latch.promise;
    }

    init() {
        if (this._socket)
            return;

        this._socket = this._socketFactory();

        const disconnectFn = () => {
            this._scope.hasAgents = false;

            this.checkModal();

            this._scope.$broadcast('agent:watch', 'DISCONNECTED');
        };

        this._socket.on('connect_error', disconnectFn);
        this._socket.on('disconnect', disconnectFn);

        this._socket.on('agent:count', ({count}) => {
            this._scope.hasAgents = count > 0;

            this.checkModal();

            this._scope.$broadcast('agent:watch', this._scope.hasAgents ? 'CONNECTED' : 'DISCONNECTED');
        });
    }

    /**
     * @param {Object} back
     * @returns {Promise}
     */
    startWatch(back) {
        this._scope.backState = back.state;
        this._scope.backText = back.text;

        this._scope.agentGoal = back.goal;

        if (back.onDisconnect) {
            this._scope.offDisconnect = this._scope.$on('agent:watch', (e, state) =>
                state === 'DISCONNECTED' && back.onDisconnect());
        }

        this._scope.showModal = true;

        // Remove blinking on init.
        if (this._scope.hasAgents !== null)
            this.checkModal();

        return this.awaitAgent();
    }

    /**
     *
     * @param {String} event
     * @param {Object} [args]
     * @returns {Promise}
     * @private
     */
    _emit(event, ...args) {
        if (!this._socket)
            return this._$q.reject('Failed to connect to server');

        const latch = this._$q.defer();

        const onDisconnect = () => {
            this._socket.removeListener('disconnect', onDisconnect);

            latch.reject('Connection to server was closed');
        };

        this._socket.on('disconnect', onDisconnect);

        args.push((err, res) => {
            this._socket.removeListener('disconnect', onDisconnect);

            if (err)
                latch.reject(err);

            latch.resolve(res);
        });

        this._socket.emit(event, ...args);

        return latch.promise;
    }

    drivers() {
        return this._emit('schemaImport:drivers');
    }

    /**
     *
     * @param {Object} preset
     * @returns {Promise}
     */
    schemas(preset) {
        return this._emit('schemaImport:schemas', preset);
    }

    /**
     *
     * @param {Object} preset
     * @returns {Promise}
     */
    tables(preset) {
        return this._emit('schemaImport:tables', preset);
    }

    /**
     * @param {Object} err
     */
    showNodeError(err) {
        if (this._scope.showModal) {
            this._downloadAgentModal.$promise.then(this._downloadAgentModal.show);

            this.Messages.showError(err);
        }
    }

    /**
     *
     * @param {String} event
     * @param {Object} [args]
     * @returns {Promise}
     * @private
     */
    _rest(event, ...args) {
        return this._downloadAgentModal.$promise
            .then(() => this._emit(event, ...args));
    }

    /**
     * @param {Boolean} [attr]
     * @param {Boolean} [mtr]
     * @returns {Promise}
     */
    topology(attr, mtr) {
        return this._rest('node:topology', !!attr, !!mtr);
    }

    /**
     * @param {String} nid Node id.
     * @param {int} [queryId]
     * @returns {Promise}
     */
    queryClose(nid, queryId) {
        return this._rest('node:query:close', nid, queryId);
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} local Flag whether to execute query locally.
     * @param {int} pageSize
     * @returns {Promise}
     */
    query(nid, cacheName, query, nonCollocatedJoins, local, pageSize) {
        return this._rest('node:query', nid, maskNull(cacheName), maskNull(query), nonCollocatedJoins, local, pageSize)
            .then(({result}) => {
                if (_.isEmpty(result.key))
                    return result.value;

                return Promise.reject(result.key);
            });
    }

    /**
     * @param {String} nid Node id.
     * @param {String} cacheName Cache name.
     * @param {String} [query] Query if null then scan query.
     * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
     * @param {Boolean} local Flag whether to execute query locally.
     * @returns {Promise}
     */
    queryGetAll(nid, cacheName, query, nonCollocatedJoins, local) {
        return this._rest('node:query:getAll', nid, maskNull(cacheName), maskNull(query), nonCollocatedJoins, local);
    }

    /**
     * @param {String} nid Node id.
     * @param {int} queryId
     * @param {int} pageSize
     * @returns {Promise}
     */
    next(nid, queryId, pageSize) {
        return this._rest('node:query:fetch', nid, queryId, pageSize)
            .then(({result}) => result);
    }

    /**
     * @param {String} [cacheName] Cache name.
     * @returns {Promise}
     */
    metadata(cacheName) {
        return this._rest('node:cache:metadata', maskNull(cacheName));
    }

    stopWatch() {
        this._scope.showModal = false;

        this.checkModal();

        this._scope.offDisconnect && this._scope.offDisconnect();

        this._scope.$broadcast('agent:watch', 'STOPPED');
    }
}

IgniteAgentMonitor.$inject = ['igniteSocketFactory', '$rootScope', '$q', '$state', '$modal', 'IgniteMessages'];

angular
    .module('ignite-console.agent', [

    ])
    .service('IgniteAgentMonitor', IgniteAgentMonitor);
