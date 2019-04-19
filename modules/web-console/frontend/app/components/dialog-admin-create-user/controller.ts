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

import Auth from '../../modules/user/Auth.service';
import MessagesFactory from '../../services/Messages.service';
import FormUtilsFactoryFactory from '../../services/FormUtils.service';
import LoadingServiceFactory from '../../modules/loading/loading.service';
import {ISignupData} from '../form-signup';

export class DialogAdminCreateUser {
    close: ng.ICompiledExpression;

    form: ng.IFormController;

    data: ISignupData = {
        email: null,
        password: null,
        firstName: null,
        lastName: null,
        company: null,
        country: null
    };

    serverError: string | null = null;

    static $inject = ['Auth', 'IgniteMessages', 'IgniteFormUtils', 'IgniteLoading'];

    constructor(
        private Auth: Auth,
        private IgniteMessages: ReturnType<typeof MessagesFactory>,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactoryFactory>,
        private loading: ReturnType<typeof LoadingServiceFactory>
    ) {}

    canSubmitForm(form: DialogAdminCreateUser['form']) {
        return form.$error.server ? true : !form.$invalid;
    }

    setServerError(error: DialogAdminCreateUser['serverError']) {
        this.serverError = error;
    }

    createUser() {
        this.IgniteFormUtils.triggerValidation(this.form);

        this.setServerError(null);

        if (!this.canSubmitForm(this.form))
            return;

        this.loading.start('createUser');

        this.Auth.signup(this.data, false)
            .then(() => {
                this.IgniteMessages.showInfo(`User ${this.data.email} created`);
                this.close({});
            })
            .catch((res) => {
                this.loading.finish('createUser');
                this.IgniteMessages.showError(null, res.data);
                this.setServerError(res.data);
            });
    }
}
