import angular from 'angular';

import {UIRouter, LazyLoadResult} from '@uirouter/angularjs';
import {default as consoleIcon} from './icons/console.icon.svg';
import {default as IconsService} from '../components/ignite-icon/service';
import {navigationMenuItem, AppStore} from '../store';

export default angular
    .module('ignite-console.console-lazy', [])
    .run(['$uiRouter', '$injector', function($uiRouter: UIRouter, $injector: ng.auto.IInjectorService) {
        $uiRouter.stateRegistry.register({
            name: 'base.console.**',
            url: '/console',
            async lazyLoad($transition$) {
                const module = await import(/* webpackChunkName: "console" */'./index');
                $injector.loadNewModules([module.default.name]);
                return [] as LazyLoadResult;
            }
        });
    }])
    .run(['IgniteIcon', (icons: IconsService) => { icons.registerIcons({console: consoleIcon}); }])
    .run(['Store', (store: AppStore) => {
        store.dispatch(navigationMenuItem({
            activeSref: 'base.console.**',
            icon: 'console',
            label: 'Console',
            order: 3,
            sref: 'base.console.overview'
        }));
    }])
    .config(['DefaultStateProvider', (DefaultState) => {
        DefaultState.setRedirectTo(() => 'base.console.overview');
    }]);
