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

import angular from 'angular';

import igniteConsole from './app/app';
import configurationLazyModule from './app/configuration/index.lazy';
import consoleLazyModule from './app/console/index.lazy';
import datasourceLazyModule from './app/datasource/index.lazy';
import datasetsLazyModule from './app/datasets/index.lazy';
import {IgniteWebConsoleModule} from './app-angular';
import {downgradeModuleFactory} from './app-angular/downgrade';

angular.bootstrap(document, [
	igniteConsole.name,
    
    datasourceLazyModule.name,
    
	configurationLazyModule.name,
    
	consoleLazyModule.name, 

	datasetsLazyModule.name,
	
	downgradeModuleFactory(IgniteWebConsoleModule)
], {strictDi: true});
