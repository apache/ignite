

import angular from 'angular';
import {ngModel, listEditableTransclude} from './directives';

export default angular
    .module('list-editable.save-on-changes', [])
    .directive('ngModel', ngModel)
    .directive('listEditableTransclude', listEditableTransclude);
