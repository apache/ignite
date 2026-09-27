

import angular from 'angular';
import component from './component';

export default angular
.module('list-editable.add-item-button', [])
.directive('listEditableAddItemButtonHasItemsButton', () => (scope, el) => scope.$on('$destroy', () => el.remove()))
.component('listEditableAddItemButton', component);
