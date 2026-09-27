

import angular from 'angular';

import uiGridColumnResizer from './directive';

export default angular
    .module('ignite-console.ui-grid-column-resizer', [])
    .directive('uiGridColumnResizer', uiGridColumnResizer);
