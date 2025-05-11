

import angular from 'angular';

import IgniteLoadingDirective from './loading.directive';
import IgniteLoadingService from './loading.service';

angular
    .module('ignite-console.loading', [])
    .directive('igniteLoading', IgniteLoadingDirective)
    .service('IgniteLoading', IgniteLoadingService);
