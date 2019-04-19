/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {UIRouter, LazyLoadResult} from '@uirouter/angularjs';
import {default as configurationIcon} from './icons/configuration.icon.svg';
import {default as IconsService} from '../components/ignite-icon/service';
import {navigationMenuItem, AppStore} from '../store';

export default angular
    .module('ignite-console.configuration-lazy', [])
    .run(['$uiRouter', '$injector', function($uiRouter: UIRouter, $injector: ng.auto.IInjectorService) {
        $uiRouter.stateRegistry.register({
            name: 'base.configuration.**',
            url: '/configuration',
            async lazyLoad($transition$) {
                const module = await import(/* webpackChunkName: "configuration" */'./index');
                $injector.loadNewModules([module.default.name]);
                return [] as LazyLoadResult;
            }
        });
    }])
    .run(['IgniteIcon', (icons: IconsService) => { icons.registerIcons({configuration: configurationIcon}); }])
    .run(['Store', (store: AppStore) => {
        store.dispatch(navigationMenuItem({
            activeSref: 'base.configuration.**',
            icon: 'configuration',
            label: 'Configuration',
            order: 1,
            sref: 'base.configuration.overview'
        }));
    }])
    .config(['DefaultStateProvider', (DefaultState) => {
        DefaultState.setRedirectTo(() => 'base.configuration.overview');
    }]);
