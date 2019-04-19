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
