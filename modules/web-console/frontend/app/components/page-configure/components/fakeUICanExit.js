/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export class FakeUiCanExitController {
    static $inject = ['$element', '$transitions'];
    static CALLBACK_NAME = 'uiCanExit';

    /** @type {string} Name of state to listen exit from */
    fromState;

    /**
     * @param {JQLite} $element
     * @param {import('@uirouter/angularjs').TransitionService} $transitions
     */
    constructor($element, $transitions) {
        this.$element = $element;
        this.$transitions = $transitions;
    }

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
