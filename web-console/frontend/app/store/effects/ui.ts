

import {AppStore, USER, ofType, hideNavigationMenuItem, showNavigationMenuItem} from '..';
import {map} from 'rxjs/operators';

export class UIEffects {
    static $inject = ['Store'];
    constructor(private store: AppStore) {}

    toggleQueriesNavItemEffect$ = this.store.actions$.pipe(
        ofType(USER),
        map((action) => {
            const QUERY_LABEL = 'Queries';
            return action.user.becomeUsed ? hideNavigationMenuItem(QUERY_LABEL) : showNavigationMenuItem(QUERY_LABEL);
        })
    )
}
