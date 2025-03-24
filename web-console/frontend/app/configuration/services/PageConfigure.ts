

import cloneDeep from 'lodash/cloneDeep';

import {merge, timer} from 'rxjs';
import {take, tap, ignoreElements, filter, map, pluck} from 'rxjs/operators';

import {ofType} from '../store/effects';

import {default as ConfigureState} from './ConfigureState';
import {default as ConfigSelectors} from '../store/selectors';

export default class PageConfigure {
    static $inject = ['ConfigureState', 'ConfigSelectors'];

    constructor(private ConfigureState: ConfigureState, private ConfigSelectors: ConfigSelectors) {}

    getClusterConfiguration({clusterID, isDemo}: {clusterID: string, isDemo: boolean}) {
        return merge(
            timer(1).pipe(
                take(1),
                tap(() => this.ConfigureState.dispatchAction({type: 'LOAD_COMPLETE_CONFIGURATION', clusterID, isDemo})),
                ignoreElements()
            ),
            this.ConfigureState.actions$.pipe(
                ofType('LOAD_COMPLETE_CONFIGURATION_ERR'),
                take(1),
                pluck('error'),
                map((e) => Promise.reject(e))
            ),
            this.ConfigureState.state$.pipe(
                this.ConfigSelectors.selectCompleteClusterConfiguration({clusterID, isDemo}),
                filter((c) => c.__isComplete),
                take(1),
                map((data) => ({...data, clusters: [cloneDeep(data.cluster)]}))
            )
        )
        .pipe(take(1))
        .toPromise();
    }
}
