

import {UIRouter} from '@uirouter/angularjs';
import {dialogState} from '../../utils/dialogState';

registerState.$inject = ['$uiRouter'];

export function registerState(router: UIRouter) {
    router.stateRegistry.register({
        ...dialogState('dialog-admin-create-user'),
        name: 'base.settings.admin.createUser',
        url: '/create-user'
    });
}
