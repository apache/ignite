

import angular from 'angular';

import directive from './directive';
import service from './service';
import './style.scss';

export default angular
    .module('ignite-console.ignite-icon', [])
    .service('IgniteIcon', service)
    .directive('igniteIcon', directive);
