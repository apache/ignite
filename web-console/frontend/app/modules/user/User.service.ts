

import {ReplaySubject, Subject} from 'rxjs';
import {StateService} from '@uirouter/angularjs';
import {default as MessagesFactory} from 'app/services/Messages.service';
import {DemoService} from 'app/modules/demo/Demo.module';
import {User as IUser} from 'app/types';


export type User = IUser | null

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
            user = $http.get('/api/v1/user')
                .then(({data}) => {
                    current$.next(data);
                    return data;
                })
                .catch(({data}) => {
                    user = null;

                    return $q.reject(data);
                });
            return user;
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
