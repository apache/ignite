

import angular from 'angular';
import component from './component';
import {registerState} from './run';

export default angular
    .module('ignite-console.page-signin', [
        'ui.router',
        'ignite-console.user'
    ])
    .component('pageSignin', component)
    .run(registerState);
