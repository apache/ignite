

import {merge} from 'rxjs';
import {tap, pluck, distinctUntilChanged} from 'rxjs/operators';

import {WellKnownOperationStatus} from 'app/types';
import {IgniteChartController} from '../../controller';

const BLANK_STATUS = new Set([WellKnownOperationStatus.ERROR, WellKnownOperationStatus.WAITING]);

export default class IgniteChartNoDataCtrl {
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
