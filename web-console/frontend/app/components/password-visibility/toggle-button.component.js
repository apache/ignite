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

import {PasswordVisibilityRoot} from './root.directive';

class Controller {
    /** @type {PasswordVisibilityRoot} */
    visibilityRoot;

    toggleVisibility() {
        this.visibilityRoot.toggleVisibility();
    }
    get isVisible() {
        return this.visibilityRoot.isVisible;
    }
}

export const component = {
    template: `
        <button
            type='button'
            ng-click='$ctrl.toggleVisibility()'
            bs-tooltip='{title: (($ctrl.isVisible ? "formField.passwordVisibility.hide" : "formField.passwordVisibility.show")|translate)}'
            data-placement='top'
        >
            <svg ignite-icon='eyeOpened' class='password-visibility__icon-visible'></svg>
            <svg ignite-icon='eyeClosed' class='password-visibility__icon-hidden'></svg>
        </button>
    `,
    require: {
        visibilityRoot: '^passwordVisibilityRoot'
    },
    controller: Controller
};
