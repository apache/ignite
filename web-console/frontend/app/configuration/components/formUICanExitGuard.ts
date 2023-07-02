/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
