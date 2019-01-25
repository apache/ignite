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

import {RejectType, UIRouter, Transition, HookMatchCriteria} from '@uirouter/angularjs';

const isPromise = (object): object is Promise<any> => object && typeof object.then === 'function';
const match: HookMatchCriteria = {
    to(state) {
        return state.data && state.data.errorState;
    }
};
const go = ($transition: Transition) => $transition.router.stateService.go(
    $transition.to().data.errorState,
    $transition.params(),
    {location: 'replace'}
);

const getResolvePromises = ($transition: Transition) => $transition.getResolveTokens()
    .filter((token) => typeof token === 'string')
    .map((token) => $transition.injector().getAsync(token))
    .filter(isPromise);

/**
 * Global transition hook that redirects to data.errorState if:
 * 1. Transition throws an error.
 * 2. Any resolve promise throws an error. onError does not work for this case if resolvePolicy is set to 'NOWAIT'.
 */
export const errorState = ($uiRouter: UIRouter) => {
    $uiRouter.transitionService.onError(match, ($transition) => {
        if ($transition.error().type !== RejectType.ERROR)
            return;

        go($transition);
    });

    $uiRouter.transitionService.onStart(match, ($transition) => {
        Promise.all(getResolvePromises($transition)).catch((e) => go($transition));
    });
};

errorState.$inject = ['$uiRouter'];
