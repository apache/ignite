

import angular from 'angular';

import directive from './directive';

export default angular
    .module('ignite-console.protect-from-bs-select-render', [])
    .directive('protectFromBsSelectRender', directive);
