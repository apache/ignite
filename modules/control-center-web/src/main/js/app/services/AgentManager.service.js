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

export default ['AgentManager', ['socketFactory', '$rootScope', '$q', '$state', '$modal', (socketFactory, $root, $q, $state, $modal) => {
    const scope = $root.$new();

    // Pre-fetch modal dialogs.
    const downloadAgentModal = $modal({
        scope,
        templateUrl: '/templates/agent-download.html',
        show: false,
        backdrop: 'static'
    });

    const _modalHide = downloadAgentModal.hide;

    /**
     * Special dialog hide function.
     */
    downloadAgentModal.hide = () => {
        //$common.hideAlert();

        _modalHide();
    };

    /**
     * Close dialog and go by specified link.
     */
    scope.back = () => {
        downloadAgentModal.hide();

        if (scope.backState)
            $state.go(scope.backState);
    };

    scope.downloadAgent = () => {
        const lnk = document.createElement('a');

        lnk.setAttribute('href', '/api/v1/agent/download/zip');
        lnk.setAttribute('target', '_self');
        lnk.setAttribute('download', null);
        lnk.style.display = 'none';

        document.body.appendChild(lnk);

        lnk.click();

        document.body.removeChild(lnk);
    };

    scope.hasAgents = true;
    scope.showModal = false;

    let agentLatch = null;

    const _checkModal = () => {
        if (!scope.hasAgents && scope.showModal)
            downloadAgentModal.$promise.then(downloadAgentModal.show);
        else if ((scope.hasAgents || !scope.showModal) && downloadAgentModal.$isShown)
            downloadAgentModal.hide();
    };

    return {
        init() {
            const socket = socketFactory({});

            socket.on('agent:count', ({count}) => {
                console.log('agent:count', count);

                scope.hasAgents = count > 0;

                _checkModal();

                if (scope.hasAgents && agentLatch) {
                    agentLatch.resolve();

                    agentLatch = null;
                }
            });

            socket.on('disconnect', () => {
                console.log('agent:disconnect');

                scope.hasAgents = false;

                _checkModal();
            });

            $root.$on('$stateChangeStart', () => {
                scope.showModal = false;
            });
        },
        startAgentWatch(back) {
            scope.backState = back.state;
            scope.backText = back.text;

            scope.agentGoal = back.goal;

            scope.showModal = true;

            _checkModal();

            if (scope.hasAgents)
                return $q.when();

            agentLatch = $q.defer();

            return agentLatch.promise;
        },
        emit(event, ...args) {

        },
        stopAgentWatch() {
            scope.showModal = false;

            _checkModal();
        }
    };
}]];
