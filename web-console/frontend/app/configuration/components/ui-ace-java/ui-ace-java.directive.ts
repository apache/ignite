

import template from './ui-ace-java.pug';
import IgniteUiAceJava from './ui-ace-java.controller';

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
        controller: IgniteUiAceJava,
        controllerAs: 'ctrl',
        require: {
            ctrl: 'igniteUiAceJava',
            igniteUiAceTabs: '?^igniteUiAceTabs',
            formCtrl: '?^form',
            ngModelCtrl: '?ngModel'
        }
    };
}
