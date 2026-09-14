

import angular from 'angular';
import component from './component';
import decorator from './decorator';

export default angular
    .module('ignite-console.ui-grid', [])
    .component('igniteGridTable', component)
    .decorator('uiGridSelectionRowHeaderButtonsDirective', decorator);
