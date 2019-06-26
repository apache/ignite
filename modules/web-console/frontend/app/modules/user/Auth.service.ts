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

import {StateService} from '@uirouter/angularjs';
import MessagesFactory from '../../services/Messages.service';
import {service as GettingsStartedFactory} from '../../modules/getting-started/GettingStarted.provider';
import {UserService} from './User.service';

type SignupUserInfo = {
    email: string,
    password: string,
    firstName: string,
    lastName: string,
    company: string,
    country: string,
};

type SigninUserInfo = {
    email: string,
    password: string,
    activationToken?: string
};

type AuthActions = 'signin' | 'signup' | 'password/forgot';

type AuthOptions = SigninUserInfo|SignupUserInfo|{email:string};

export default class AuthService {
    static $inject = ['$http', '$state', '$window', 'IgniteMessages', 'gettingStarted', 'User'];

    constructor(
        private $http: ng.IHttpService,
        private $state: StateService,
        private $window: ng.IWindowService,
        private Messages: ReturnType<typeof MessagesFactory>,
        private gettingStarted: ReturnType<typeof GettingsStartedFactory>,
        private User: UserService
    ) {}

    signup(userInfo: SignupUserInfo, loginAfterSignup: boolean = true) {
        return this._auth('signup', userInfo, loginAfterSignup);
    }

    signin(signinInfo: SigninUserInfo) {
        return this._auth('signin', signinInfo);
    }

    remindPassword(email: string) {
        return this._auth('password/forgot', {email}).then(() => this.$state.go('password.send'));
    }

    // TODO IGNITE-7994: Remove _auth and move API calls to corresponding methods
    /**
     * Performs the REST API call.
     */
    private _auth(action: AuthActions, userInfo: AuthOptions) {
        return this.$http.post('/api/v1/' + action, userInfo)
            .then(() => {
                if (action === 'password/forgot')
                    return;

                return this.User.read()
                    .then((user) => {
                        this.User.current$.next(user);
                        this.$state.go('default-state');
                        this.gettingStarted.tryShow();
                    });
            });
    }

    logout() {
        return this.$http.post('/api/v1/logout')
            .catch((e) => this.Messages.showError(e))
            .finally(() => {
                this.User.clean();

                this.$window.open(this.$state.href('signin'), '_self');
            });
    }

    async resendSignupConfirmation(email: string) {
        try {
            return await this.$http.post('/api/v1/activation/resend', {email});
        }
        catch (err) {
            throw err.data;
        }
    }
}
