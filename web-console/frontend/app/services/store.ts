

import {BehaviorSubject, Subject, merge} from 'rxjs';
import {scan, tap} from 'rxjs/operators';

interface Reducer<State, Actions> {
    (state: State, action: Actions): State
}

export class Store<Actions, State> {
    static $inject = ['$injector'];

    actions$: Subject<Actions>;
    state$: BehaviorSubject<State>;
    private _reducers: Array<Reducer<State, Actions>>;

    constructor(private $injector: ng.auto.IInjectorService) {
        this.$injector = $injector;

        this.actions$ = new Subject();
        this.state$ = new BehaviorSubject({});
        this.actions$.pipe(
            scan((state, action) => this._reducers.reduce((state, reducer) => reducer(state, action), state), void 0),
            tap((state) => this.state$.next(state))
        ).subscribe();
        this._reducers = [(state) => state];
    }

    dispatch(action: Actions) {
        this.actions$.next(action);
    }

    addReducer<T extends keyof State, U = State[T]>(path: T, reducer: Reducer<U, Actions>): void
    addReducer(reducer: Reducer<State, Actions>): void
    addReducer(...args) {
        if (typeof args[0] === 'string') {
            const [path, reducer] = args;
            this._reducers = [
                ...this._reducers,
                (state = {}, action) => {
                    const pathState = reducer(state[path], action);
                    // Don't update root state if child state changes
                    return state[path] !== pathState ? {...state, [path]: pathState} : state;
                }
            ];
        } else {
            const [reducer] = args;
            this._reducers = [...this._reducers, reducer];
        }
    }

    addEffects(EffectsClass) {
        const instance = this.$injector.instantiate(EffectsClass);
        return merge(
            ...Object.keys(instance).filter((k) => k.endsWith('Effect$')).map((k) => instance[k]),
        ).pipe(tap((a) => this.dispatch(a))).subscribe();
    }
}
