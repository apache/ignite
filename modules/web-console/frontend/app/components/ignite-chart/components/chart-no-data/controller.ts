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

import {merge} from 'rxjs';
import {tap, pluck, distinctUntilChanged} from 'rxjs/operators';

import {WellKnownOperationStatus} from 'app/types';
import {IgniteChartController} from '../../controller';

const BLANK_STATUS = new Set([WellKnownOperationStatus.ERROR, WellKnownOperationStatus.WAITING]);

export default class IgniteChartNoDataCtrl implements ng.IOnChanges, ng.IOnDestroy {
    static $inject = ['AgentManager'];

    constructor(private AgentManager) {}

    igniteChart: IgniteChartController;

    handleClusterInactive: boolean;

    connectionState$ = this.AgentManager.connectionSbj.pipe(
        pluck('state'),
        distinctUntilChanged(),
        tap((state) => {
            if (state === 'AGENT_DISCONNECTED')
                this.destroyChart();
        })
    );

    cluster$ = this.AgentManager.connectionSbj.pipe(
        pluck('cluster'),
        distinctUntilChanged(),
        tap((cluster) => {
            if (!cluster && !this.AgentManager.isDemoMode()) {
                this.destroyChart();
                return;
            }

            if (!!cluster && cluster.active === false && this.handleClusterInactive)
                this.destroyChart();

        })
    );

    subsribers$ = merge(
        this.connectionState$,
        this.cluster$
    ).subscribe();

    $onChanges(changes) {
        if (changes.resultDataStatus && BLANK_STATUS.has(changes.resultDataStatus.currentValue))
            this.destroyChart();
    }

    $onDestroy() {
        this.subsribers$.unsubscribe();
    }

    destroyChart() {
        if (this.igniteChart && this.igniteChart.chart) {
            this.igniteChart.chart.destroy();
            this.igniteChart.config = null;
            this.igniteChart.chart = null;
        }
    }
}
