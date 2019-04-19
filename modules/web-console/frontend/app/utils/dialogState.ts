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

import {UIRouter, StateDeclaration, Transition} from '@uirouter/angularjs';

export function dialogState(component: string): Partial<StateDeclaration> {
    let dialog: mgcrea.ngStrap.modal.IModal | undefined;
    let hide: (() => void) | undefined;

    onEnter.$inject = ['$transition$'];

    function onEnter(transition: Transition) {
        const modal = transition.injector().get<mgcrea.ngStrap.modal.IModalService>('$modal');
        const router = transition.injector().get<UIRouter>('$uiRouter');

        dialog = modal({
            template: `
                <${component}
                    class='modal center modal--ignite theme--ignite'
                    tabindex='-1'
                    role='dialog'
                    on-hide=$hide()
                ></${component}>
            `,
            onHide(modal) {
                modal.destroy();
            }
        });

        hide = dialog.hide;

        dialog.hide = () => router.stateService.go('.^');
    }

    return {
        onEnter,
        onExit() {
            if (hide) hide();
            dialog = hide = void 0;
        }
    };
}
