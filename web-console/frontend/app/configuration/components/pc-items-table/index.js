

import angular from 'angular';
import component from './component';
import decorator from './decorator';

export default angular
    .module('ignite-console.page-configure.items-table', ['ui.grid'])
    .decorator('uiGridSelectionRowHeaderButtonsDirective', decorator)
    .component('pcItemsTable', component);
