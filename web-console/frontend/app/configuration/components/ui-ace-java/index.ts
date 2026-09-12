

import angular from 'angular';

import UiAceJavaDirective from './ui-ace-java.directive';

export default angular.module('ignite-console.ui-ace-java', [
    'ignite-console.services',
    'ignite-console.configuration.generator'
])
.directive('igniteUiAceJava', UiAceJavaDirective);
