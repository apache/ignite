

import angular from 'angular';
import component from './component';
import transclude from './transcludeDirective';

export default angular
    .module('ignite-console.panel-collapsible', [])
    .directive('panelCollapsibleTransclude', transclude)
    .component('panelCollapsible', component);
