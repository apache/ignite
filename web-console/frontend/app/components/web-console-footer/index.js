

import angular from 'angular';
import component from './component';

export default angular
    .module('ignite-console.web-console-footer', [])
    .component('webConsoleFooter', component)
    .directive('webConsoleFooterPageBottom', function() {
        return {
            restrict: 'C',
            link(scope, el) {
                el.parent().addClass('wrapper-public');
                scope.$on('$destroy', () => el.parent().removeClass('wrapper-public'));
            }
        };
    });
