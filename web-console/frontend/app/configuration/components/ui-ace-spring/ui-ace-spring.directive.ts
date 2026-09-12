

import template from './ui-ace-spring.pug';
import IgniteUiAceSpring from './ui-ace-spring.controller';

export default function() {
    return {
        priority: 1,
        restrict: 'E',
        scope: {
            master: '=',
            detail: '='
        },
        bindToController: {
            data: '=?ngModel',
            generator: '@',
            client: '@'
        },
        template,
        controller: IgniteUiAceSpring,
        controllerAs: 'ctrl',
        require: {
            ctrl: 'igniteUiAceSpring',
            igniteUiAceTabs: '?^igniteUiAceTabs',
            formCtrl: '?^form',
            ngModelCtrl: '?ngModel'
        }
    };
}
