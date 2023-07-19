

import {component} from './component';
import {state} from './state';

export default angular.module('ignite-console.page-signup-confirmation', [])
    .run(state)
    .component('pageSignupConfirmation', component);
