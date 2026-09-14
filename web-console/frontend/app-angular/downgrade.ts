
import angular from 'angular';
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
