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

export default class {
    static $inject = ['AgentManager'];

    constructor(agentMgr) {
        const ctrl = this;

        ctrl.counter = 1;

        ctrl.cluster = null;
        ctrl.clusters = [];

        agentMgr.connectionSbj.subscribe({
            next: ({cluster, clusters}) => {
                if (_.isEmpty(clusters))
                    return ctrl.clusters.length = 0;

                const removed = _.differenceBy(ctrl.clusters, clusters, 'id');

                if (_.nonEmpty(removed))
                    _.pullAll(ctrl.clusters, removed);

                const added = _.differenceBy(clusters, ctrl.clusters, 'id');

                _.forEach(added, (cluster) => {
                    ctrl.clusters.push({
                        id: cluster.id,
                        connected: true,
                        click: () => {
                            if (cluster.id === _.get(ctrl, 'cluster.id'))
                                return;

                            if (_.get(ctrl, 'cluster.connected')) {
                                agentMgr.saveToStorage(cluster);

                                window.open(window.location.href, '_blank');
                            }
                            else
                                ctrl.cluster = _.find(ctrl.clusters, {id: cluster.id});
                        }
                    });
                });

                ctrl.cluster = cluster;
            }
        });
    }
}
