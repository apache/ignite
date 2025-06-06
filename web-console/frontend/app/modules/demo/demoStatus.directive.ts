

import {DemoService} from 'app/modules/demo/Demo.module';

/**
 * Use this directive along with ng-ref when you can't inject Demo into scope
 */
export function directive(): ng.IDirective {
    return {
        controller: class DemoStatus {
            static $inject = ['Demo'];
            constructor(private Demo: DemoService) {}
            get enabled() {return this.Demo.enabled;}
        },
        restrict: 'A'
    };
}
