

import './style.scss';

import templateUrl from './template.tpl.pug';
import controller from './controller';

export default angular
    .module('ignite-console.list-of-registered-users', [])
    .component('igniteListOfRegisteredUsers', {
        controller,
        templateUrl
    });
