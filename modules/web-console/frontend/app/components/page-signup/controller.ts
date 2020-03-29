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

import Auth from '../../modules/user/Auth.service';
import MessagesFactory from '../../services/Messages.service';
import FormUtilsFactoryFactory from '../../services/FormUtils.service';
import {ISignupData} from '../form-signup';
import {get, eq, pipe} from 'lodash/fp';

const EMAIL_NOT_CONFIRMED_ERROR_CODE = 10104;
const isEmailConfirmationError = pipe(get('data.errorCode'), eq(EMAIL_NOT_CONFIRMED_ERROR_CODE));

export default class PageSignup implements ng.IPostLink {
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

    isLoading = false;

    static $inject = ['Auth', 'IgniteMessages', 'IgniteFormUtils', '$element'];

    constructor(
        private Auth: Auth,
        private IgniteMessages: ReturnType<typeof MessagesFactory>,
        private IgniteFormUtils: ReturnType<typeof FormUtilsFactoryFactory>,
        private el: JQLite
    ) {}

    $postLink() {
        this.el.addClass('public-page');
    }

    canSubmitForm(form: PageSignup['form']) {
        return form.$error.server ? true : !form.$invalid;
    }

    setServerError(error: PageSignup['serverError']) {
        this.serverError = error;
    }

    signup() {
        this.isLoading = true;

        this.IgniteFormUtils.triggerValidation(this.form);

        this.setServerError(null);

        if (!this.canSubmitForm(this.form)) {
            this.isLoading = false;
            return;
        }


        return this.Auth.signup(this.data)
            .catch((res) => {
                if (isEmailConfirmationError(res))
                    return;

                this.IgniteMessages.showError(null, res.data);
                this.setServerError(res.data);
            })
            .finally(() => this.isLoading = false);
    }
}
