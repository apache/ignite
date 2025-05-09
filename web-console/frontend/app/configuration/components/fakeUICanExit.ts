

import {TransitionService} from '@uirouter/angularjs';

export class FakeUiCanExitController {
    static $inject = ['$element', '$transitions'];
    static CALLBACK_NAME = 'uiCanExit';

    /** Name of state to listen exit from */
    fromState: string;

    constructor(private $element: JQLite, private $transitions: TransitionService) {}

    $onInit() {
        const data = this.$element.data();
        const {CALLBACK_NAME} = this.constructor;

        const controllerWithCallback = Object.keys(data)
            .map((key) => data[key])
            .find((controller) => controller[CALLBACK_NAME]);

        if (!controllerWithCallback)
            return;

        this.off = this.$transitions.onBefore({from: this.fromState}, (...args) => {
            return controllerWithCallback[CALLBACK_NAME](...args);
        });
    }

    $onDestroy() {
        if (this.off)
            this.off();

        this.$element = null;
    }
}

export default function fakeUiCanExit() {
    return {
        bindToController: {
            fromState: '@fakeUiCanExit'
        },
        controller: FakeUiCanExitController
    };
}
