

import angular from 'angular';

import UiAceSpringDirective from './ui-ace-spring.directive';

export default angular.module('ignite-console.ui-ace-spring', [
    'ignite-console.services',
    'ignite-console.configuration.generator'
])
.directive('igniteUiAceSpring', UiAceSpringDirective);
