import angular from 'angular';
import {UIRouter, LazyLoadResult} from '@uirouter/angularjs';
import {default as igfsIcon} from './icons/igfs.icon.svg';
import {default as IconsService} from '../components/ignite-icon/service';
import {navigationMenuItem, AppStore} from '../store';

export default angular
    .module('ignite-console.igfs-lazy', [])
    .run(['$uiRouter', '$injector', function($uiRouter: UIRouter, $injector: ng.auto.IInjectorService) {
        $uiRouter.stateRegistry.register({
            name: 'base.igfs.**',
            url: '/igfs',
            async lazyLoad($transition$) {
                const module = await import(/* webpackChunkName: "igfs" */'./index');
                $injector.loadNewModules([module.default.name]);
                return [] as LazyLoadResult;
            }
        });
    }])
    .run(['IgniteIcon', (icons: IconsService) => { icons.registerIcons({igfs: igfsIcon}); }])
    .run(['Store', (store: AppStore) => {
        store.dispatch(navigationMenuItem({
            activeSref: 'base.igfs.**',
            icon: 'igfs',
            label: 'File Storages',
            order: 5,
            sref: 'base.igfs.overview'
        }));
    }])
    ;
