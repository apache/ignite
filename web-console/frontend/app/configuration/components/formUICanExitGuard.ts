

import {default as ConfigChangesGuard} from '../services/ConfigChangesGuard';

class FormUICanExitGuardController {
    static $inject = ['$element', 'ConfigChangesGuard'];

    constructor(private $element: JQLite, private ConfigChangesGuard: ConfigChangesGuard) {}

    $onDestroy() {
        this.$element = null;
    }

    $onInit() {
        const data = this.$element.data();
        const controller = Object.keys(data)
            .map((key) => data[key])
            .find(this._itQuacks);

        if (!controller)
            return;

        controller.uiCanExit = ($transition$) => {
            const options = $transition$.options();

            if (options.custom.justIDUpdate || options.redirectedFrom)
                return true;

            $transition$.onSuccess({}, controller.reset);

            return this.ConfigChangesGuard.guard(...controller.getValuesToCompare());
        };
    }

    _itQuacks(controller) {
        return controller.reset instanceof Function &&
            controller.getValuesToCompare instanceof Function &&
            !controller.uiCanExit;
    }
}

export default function formUiCanExitGuard() {
    return {
        priority: 10,
        controller: FormUICanExitGuardController
    };
}
