

import angular from 'angular';

import directive from './directive';

export default angular
    .module('ignite-console.list-editable.one-way', [])
    .directive('listEditableOneWay', directive);
