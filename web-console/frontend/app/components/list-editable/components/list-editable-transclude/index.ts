

import angular from 'angular';
import {listEditableTransclude} from './directive';

export default angular
    .module('list-editable.transclude', [])
    .directive('listEditableTransclude', listEditableTransclude);
