

import angular from 'angular';
import {UIRouter} from '@uirouter/angularjs';

import _ from 'lodash';

import template from './template.pug';
import controller from './controller';
import publicTemplate from '../../../views/public.pug';

import './style.scss';

export default angular
    .module('ignite-console.page-password-reset', [
    ])
    .component('pagePasswordReset', {
        template,
        controller
    })
    .run(['$uiRouter', '$translate', (router: UIRouter, $translate: ng.translate.ITranslateService) => {
        // set up the states
        router.stateRegistry.register({
            name: 'password',
            url: '/password',
            abstract: true,
            template: '<ui-view></ui-view>'
        });
        router.stateRegistry.register({
            name: 'password.reset',
            url: '/reset?{email}{token}',
            views: {
                '@': {
                    template: publicTemplate
                },
                'page@password.reset': {
                    component: 'pagePasswordReset'
                }
            },
            redirectTo: (trans) => {
                if (_.isEmpty(trans.params('to').token))
                    return 'signin';

                return true;
            },
            unsaved: true,
            tfMetaTags: {
                title: $translate.instant('passwordReset.documentTitle')
            }
        });
    }]);
