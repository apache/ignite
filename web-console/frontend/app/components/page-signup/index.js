

import angular from 'angular';
import component from './component';
import {registerState} from './run';

export default angular
    .module('ignite-console.page-signup', [
        'ui.router',
        'ignite-console.user',
        'ignite-console.form-signup'
    ])
    .component('pageSignup', component)
    .run(registerState);
