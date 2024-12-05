

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
