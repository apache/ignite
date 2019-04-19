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

import {UIRouter} from '@uirouter/angularjs';

registerInterceptor.$inject = ['$httpProvider'];

export function registerInterceptor(http: ng.IHttpProvider) {
    emailConfirmationInterceptor.$inject = ['$q', '$injector'];

    function emailConfirmationInterceptor($q: ng.IQService, $injector: ng.auto.IInjectorService): ng.IHttpInterceptor {
        return {
            responseError(res) {
                if (res.status === 403 && res.data && res.data.errorCode === 10104)
                    $injector.get<UIRouter>('$uiRouter').stateService.go('signup-confirmation', {email: res.data.email});

                return $q.reject(res);
            }
        };
    }

    http.interceptors.push(emailConfirmationInterceptor as ng.IHttpInterceptorFactory);
}
