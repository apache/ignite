

import angular from 'angular';

import cols from './cols.directive';
import row from './row.directive';

export default angular
    .module('list-editable-cols', [])
    .directive('listEditableCols', cols)
    .directive('listEditableItemView', row)
    .directive('listEditableItemEdit', row);
