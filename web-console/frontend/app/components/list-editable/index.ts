

import angular from 'angular';

import component from './component';
import listEditableCols from './components/list-editable-cols';
import transclude from './components/list-editable-transclude';
import listEditableOneWay from './components/list-editable-one-way';
import addItemButton from './components/list-editable-add-item-button';
import saveOnChanges from './components/list-editable-save-on-changes';

export default angular
    .module('ignite-console.list-editable', [
        addItemButton.name,
        listEditableCols.name,
        listEditableOneWay.name,
        transclude.name,
        saveOnChanges.name
    ])
    .component('listEditable', component);
