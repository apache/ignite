

import {Subject, BehaviorSubject} from 'rxjs';
import {tap, scan} from 'rxjs/operators';

export default class ConfigureState {
    actions$: Subject<{type: string}>;

    constructor() {
        this.actions$ = new Subject();
        this.state$ = new BehaviorSubject({});
        this._combinedReducer = (state, action) => state;

        const reducer = (state = {}, action) => {
            try {
                return this._combinedReducer(state, action);
            }
            catch (e) {
                console.error(e);

                return state;
            }
        };

        this.actions$.pipe(
            scan(reducer, {}),
            tap((v) => this.state$.next(v))
        ).subscribe();
    }

    addReducer(combineFn) {
        const old = this._combinedReducer;

        this._combinedReducer = (state, action) => combineFn(old(state, action), action);

        return this;
    }

    dispatchAction(action) {
        if (typeof action === 'function')
            return action((a) => this.actions$.next(a), () => this.state$.getValue());

        this.actions$.next(action);

        return action;
    }
}
