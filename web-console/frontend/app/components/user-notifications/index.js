

import angular from 'angular';

import userNotifications from './service';

import './style.scss';

export default angular
    .module('ignite-console.user-notifications', [])
    .service('UserNotifications', userNotifications);
