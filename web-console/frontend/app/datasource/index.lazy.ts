import angular from 'angular';

import {UIRouter, LazyLoadResult} from '@uirouter/angularjs';
import {default as datasourceIcon} from './icons/datasource.icon.svg';
import {default as IconsService} from '../components/ignite-icon/service';
import {navigationMenuItem, AppStore} from '../store';

export default angular
    .module('ignite-console.datasource-lazy', [])
    .run(['$uiRouter', '$injector', function($uiRouter: UIRouter, $injector: ng.auto.IInjectorService) {
        $uiRouter.stateRegistry.register({
            name: 'base.datasource.**',
            url: '/datasource',
            async lazyLoad($transition$) {
                const module = await import(/* webpackChunkName: "datasource" */'./index');
                $injector.loadNewModules([module.default.name]);
                return [] as LazyLoadResult;
            }
        });
    }])
    .run(['IgniteIcon', (icons: IconsService) => { icons.registerIcons({datasource: datasourceIcon}); }])
    .run(['Store', (store: AppStore) => {
        store.dispatch(navigationMenuItem({
            activeSref: 'base.datasource.**',
            icon: 'datasource',
            label: 'Datasource',
            order: 4,
            sref: 'base.datasource.overview'
        }));
    }])
    ;
