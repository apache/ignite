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
import {of} from 'rxjs';
import {distinctUntilChanged, switchMap} from 'rxjs/operators';

export default class NoDataCmpCtrl {
    static $inject = ['AgentManager', 'AgentModal'];

    connectionState$ = this.AgentManager.connectionSbj.pipe(
        switchMap((sbj) => {
            if (!_.isNil(sbj.cluster) && sbj.cluster.active === false)
                return of('CLUSTER_INACTIVE');

            return of(sbj.state);
        }),
        distinctUntilChanged()
    );

    backText = 'Close';

    constructor(private AgentManager, private AgentModal) {}

    openAgentMissingDialog() {
        this.AgentModal.agentDisconnected(this.backText, '.');
    }

    openNodeMissingDialog() {
        this.AgentModal.clusterDisconnected(this.backText, '.');
    }
}
