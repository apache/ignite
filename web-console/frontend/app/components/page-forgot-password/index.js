

import angular from 'angular';
import component from './component';
import {registerState} from './run';

export default angular
    .module('ignite-console.page-forgot-password', [
        'ui.router',
        'ignite-console.user'
    ])
    .component('pageForgotPassword', component)
    .run(registerState);
