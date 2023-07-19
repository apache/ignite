

import angular from 'angular';

import directive from './directive';
import {default as transcludeToBody} from './transcludeToBody.directive';
import stripFilter from './strip.filter';

export default angular
    .module('ignite-console.bs-select-menu', [])
    .directive('bssmTranscludeToBody', transcludeToBody)
    .directive('bsSelectMenu', directive)
    .filter('bsSelectStrip', stripFilter);
