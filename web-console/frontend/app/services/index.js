

import angular from 'angular';
import IgniteVersion from './Version.service';
import {default as DefaultState} from './DefaultState';

export default angular
    .module('ignite-console.services', [])
    .provider('DefaultState', DefaultState)
    .service('IgniteVersion', IgniteVersion);
