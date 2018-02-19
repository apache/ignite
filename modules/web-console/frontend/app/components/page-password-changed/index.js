/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

import angular from 'angular';

import template from './template.pug';
import controller from './controller';

import './style.scss';

export default angular
    .module('ignite-console.page-password-changed', [
    ])
    .component('pagePasswordChanged', {
        template,
        controller
    })
    .config(['$stateProvider', ($stateProvider) => {
        $stateProvider.state('password.send', {
            url: '/changed',
            component: 'pagePasswordChanged',
            tfMetaTags: {
                title: 'Password send'
            },
            unsaved: true
        });
    }]);
