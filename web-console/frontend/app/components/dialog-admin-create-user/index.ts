

import component from './component';
import {registerState} from './state';

export default angular.module('ignite-console.dialog-admin-create-user', [])
    .run(registerState)
    .component('dialogAdminCreateUser', component);
