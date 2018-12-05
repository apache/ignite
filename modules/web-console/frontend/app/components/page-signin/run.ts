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

import {UIRouter} from '@uirouter/angularjs';
import {IIgniteNg1StateDeclaration} from 'app/types';

export function registerState($uiRouter: UIRouter) {
    const state: IIgniteNg1StateDeclaration = {
        url: '/signin',
        name: 'signin',
        component: 'pageSignin',
        unsaved: true,
        redirectTo: (trans) => {
            const skipStates = new Set(['signup', 'forgotPassword', 'landing']);

            if (skipStates.has(trans.from().name))
                return;

            return trans.injector().get('User').read()
                .then(() => {
                    try {
                        const {name, params} = JSON.parse(localStorage.getItem('lastStateChangeSuccess'));

                        const restored = trans.router.stateService.target(name, params);

                        return restored.valid() ? restored : 'default-state';
                    } catch (ignored) {
                        return 'default-state';
                    }
                })
                .catch(() => true);
        },
        tfMetaTags: {
            title: 'Sign In'
        }
    };

    $uiRouter.stateRegistry.register(state);
}

registerState.$inject = ['$uiRouter'];
