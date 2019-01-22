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
