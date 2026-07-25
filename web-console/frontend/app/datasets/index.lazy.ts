import angular from 'angular';
import {UIRouter, LazyLoadResult} from '@uirouter/angularjs';
import {default as datasetsIcon} from './icons/datasets.icon.svg';
import {default as IconsService} from '../components/ignite-icon/service';
import {navigationMenuItem, AppStore} from '../store';

export default angular
    .module('ignite-console.datasets-lazy', [])
    .run(['$uiRouter', '$injector', function($uiRouter: UIRouter, $injector: ng.auto.IInjectorService) {
        $uiRouter.stateRegistry.register({
            name: 'base.datasets.**',
            url: '/datasets',
            async lazyLoad($transition$) {
                const module = await import(/* webpackChunkName: "datasets" */'./index');
                $injector.loadNewModules([module.default.name]);
                return [] as LazyLoadResult;
            }
        });
    }])
    .run(['IgniteIcon', (icons: IconsService) => { icons.registerIcons({datasets: datasetsIcon}); }])
    .run(['Store', (store: AppStore) => {
        store.dispatch(navigationMenuItem({
            activeSref: 'base.datasets.**',
            icon: 'datasets',
            label: 'Datasets',
            order: 4,
            sref: 'base.datasets.overview'
        }));
    }])
    ;
