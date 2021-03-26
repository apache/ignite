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

import {ServiceBootstrapComponent} from './components/serviceBootstrap';
import {platformBrowserDynamic} from '@angular/platform-browser-dynamic';
import {downgradeModule, downgradeComponent } from '@angular/upgrade/static';
import {StaticProvider} from '@angular/core';

export const downgradeModuleFactory = (ngModule) => {
    const boostrapFn = (extraProviders: StaticProvider[]) => {
        const platformRef = platformBrowserDynamic(extraProviders);
        return platformRef.bootstrapModule(ngModule);
    };
    const downgradedModule = downgradeModule(boostrapFn);

    angular
        .module(downgradedModule)
        .directive('serviceBootstrap', downgradeComponent({component: ServiceBootstrapComponent}))
        .run(['$compile', '$rootScope', ($compile: ng.ICompileService, $root: ng.IRootScopeService) => {
            $compile('<service-bootstrap></service-bootstrap>')($root);
        }]);

    return downgradedModule;
};
