

import {reducer, devTools} from './reduxDevtoolsIntegration';
import {AppStore} from '.';
import {filter, withLatestFrom, tap, skip} from 'rxjs/operators';

run.$inject = ['Store'];

export function run(store: AppStore) {
    if (devTools) {
        devTools.subscribe((e) => {
            if (e.type === 'DISPATCH' && e.state) store.dispatch(e);
        });

        const ignoredActions = new Set([
        ]);

        store.actions$.pipe(
            filter((e) => e.type !== 'DISPATCH'),
            withLatestFrom(store.state$.pipe(skip(1))),
            tap(([action, state]) => {
                if (ignoredActions.has(action.type)) return;
                devTools.send(action, state);
                console.log(action);
            })
        ).subscribe();

        store.addReducer(reducer);
    }
}
