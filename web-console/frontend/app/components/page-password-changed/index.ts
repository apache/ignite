

import angular from 'angular';

import template from './template.pug';
import controller from './controller';
import publicTemplate from '../../../views/public.pug';
import {StateRegistry} from '@uirouter/angularjs';

import './style.scss';

export default angular
    .module('ignite-console.page-password-changed', [
    ])
    .component('pagePasswordChanged', {
        template,
        controller
    })
    .run(['$stateRegistry', '$translate', ($state: StateRegistry, $translate: ng.translate.ITranslateService) => {
        $state.register({
            name: 'password.send',
            url: '/changed',
            views: {
                '@': {
                    template: publicTemplate
                },
                'page@password.send': {
                    component: 'pagePasswordChanged'
                }
            },
            tfMetaTags: {
                title: $translate.instant('passwordChangedPage.documentTitle')
            },
            unsaved: true
        });
    }]);
