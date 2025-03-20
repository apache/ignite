

import {Store} from '../services/store';

import {UIState, uiReducer} from './reducers/ui';
import {UIActions} from './actions/ui';
import {UIEffects} from './effects/ui';

import {UserActions} from './actions/user';

export * from './actions/ui';
export * from './reducers/ui';
export * from './selectors/ui';

export * from './actions/user';

export {ofType} from './ofType';

export type State = {
    ui: UIState,
};

export type Actions =
    | UserActions
    | UIActions;

export type AppStore = Store<Actions, State>;

register.$inject = ['Store'];
export function register(store: AppStore) {
    store.addReducer('ui', uiReducer);
    store.addEffects(UIEffects);
}
