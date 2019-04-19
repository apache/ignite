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

import {IInputErrorNotifier} from '../../types';

const scrollIntoView = (() => {
    if (HTMLElement.prototype.scrollIntoViewIfNeeded)
        return (el: HTMLElement) => {el.scrollIntoViewIfNeeded();};
    return (el: HTMLElement) => {
        try {
            el.scrollIntoView({block: 'center'});
        } catch (e) {
            el.scrollIntoView();
        }
    };
})();

/**
 * Brings user attention to invalid form fields.
 * Use IgniteFormUtils.triggerValidation to trigger the event.
 */
export function directive($timeout) {
    return {
        require: ['ngModel', '?^^bsCollapseTarget', '?^^igniteFormField', '?formFieldSize', '?^^panelCollapsible'],
        link(scope, el, attr, [ngModel, bsCollapseTarget, igniteFormField, formFieldSize, panelCollapsible]) {
            const formFieldController: IInputErrorNotifier = igniteFormField || formFieldSize;

            let onBlur;

            scope.$on('$destroy', () => {
                el[0].removeEventListener('blur', onBlur);
                onBlur = null;
            });

            const off = scope.$on('$showValidationError', (e, target) => {
                if (target !== ngModel)
                    return;

                ngModel.$setTouched();

                bsCollapseTarget && bsCollapseTarget.open();
                panelCollapsible && panelCollapsible.open();

                if (!onBlur && formFieldController) {
                    onBlur = () => formFieldController.hideError();

                    el[0].addEventListener('blur', onBlur, {passive: true});
                }

                $timeout(() => {
                    scrollIntoView(el[0]);

                    if (!attr.bsSelect)
                        $timeout(() => el[0].focus(), 100);

                    formFieldController && formFieldController.notifyAboutError();
                });
            });
        }
    };
}

directive.$inject = ['$timeout'];
