

import angular from 'angular';

import igniteConsole from './app/app';
import configurationLazyModule from './app/configuration/index.lazy';
import consoleLazyModule from './app/console/index.lazy';
import datasourceLazyModule from './app/datasource/index.lazy';
import datasetsLazyModule from './app/datasets/index.lazy';
import igfsLazyModule from './app/igfs/index.lazy';
import {IgniteWebConsoleModule} from './app-angular';
import {downgradeModuleFactory} from './app-angular/downgrade';

angular.bootstrap(document, [
	igniteConsole.name,
    
    datasourceLazyModule.name,
    
	configurationLazyModule.name,
    
	consoleLazyModule.name, 

	datasetsLazyModule.name,

	igfsLazyModule.name,
	
	downgradeModuleFactory(IgniteWebConsoleModule)
], {strictDi: true});
