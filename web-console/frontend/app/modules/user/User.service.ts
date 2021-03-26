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

import {ReplaySubject, Subject} from 'rxjs';
import {StateService} from '@uirouter/angularjs';
import {default as MessagesFactory} from 'app/services/Messages.service';
import {DemoService} from 'app/modules/demo/Demo.module';

export type User = {
    id: string,
    admin: boolean,
    country: string,
    email: string,
    phone?: string,
    company?: string,
    firstName: string,
    lastName: string,
    lastActivity: string,
    lastLogin: string,
    registered: string,
    token: string
} | null

UserFactory.$inject = ['$q', '$injector', 'Demo', '$state', '$http', 'IgniteMessages'];

export default function UserFactory(
    $q: ng.IQService,
    $injector: ng.auto.IInjectorService,
    Demo: DemoService,
    $state: StateService,
    $http: ng.IHttpService,
    IgniteMessages: ReturnType<typeof MessagesFactory>
) {
    let user: ng.IPromise<User>;

    const current$ = new ReplaySubject<User>(1);
    const created$ = new Subject<User>();

    return {
        current$,
        created$,
        /**
         * @returns {ng.IPromise<User>}
         */
        load() {
            return user = $http.get('/api/v1/user')
                .then(({data}) => {
                    current$.next(data);
                    return data;
                })
                .catch(({data}) => {
                    user = null;

                    return $q.reject(data);
                });
        },
        read() {
            if (user)
                return user;

            return this.load();
        },
        clean() {
            current$.next(null);

            delete Demo.enabled;

            sessionStorage.removeItem('demoMode');
        },

        async save(user: Partial<User>): Promise<User> {
            try {
                const {data: updatedUser} = await $http.post<User>('/api/v1/profile/save', user);
                current$.next(updatedUser);

                IgniteMessages.showInfo('Profile saved.');

                return updatedUser;
            }
            catch (e) {
                IgniteMessages.showError('Failed to save profile: ', e);
            }
        }
    };
}

export type UserService = ReturnType<typeof UserFactory>
