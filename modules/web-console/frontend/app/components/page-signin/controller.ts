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

import AuthService from 'app/modules/user/Auth.service';

import {PageSigninStateParams} from './run';

interface ISiginData {
    email: string,
    password: string
}

interface ISigninFormController extends ng.IFormController {
    email: ng.INgModelController,
    password: ng.INgModelController
}

export default class PageSignIn implements ng.IPostLink {
    activationToken?: PageSigninStateParams['activationToken'];

    data: ISiginData = {
        email: null,
        password: null
    };

    form: ISigninFormController;

    serverError: string = null;

    isLoading = false;

    static $inject = ['Auth', 'IgniteMessages', 'IgniteFormUtils', '$element'];

    constructor(private Auth: AuthService, private IgniteMessages, private IgniteFormUtils, private el: JQLite) {}

    canSubmitForm(form: ISigninFormController) {
        return form.$error.server ? true : !form.$invalid;
    }

    $postLink() {
        this.el.addClass('public-page');
        this.form.email.$validators.server = () => !this.serverError;
        this.form.password.$validators.server = () => !this.serverError;
    }

    setServerError(error: string) {
        this.serverError = error;
        this.form.email.$validate();
        this.form.password.$validate();
    }

    signin() {
        this.isLoading = true;

        this.IgniteFormUtils.triggerValidation(this.form);

        this.setServerError(null);

        if (!this.canSubmitForm(this.form)) {
            this.isLoading = false;
            return;
        }

        return this.Auth.signin(this.data.email, this.data.password, this.activationToken).catch((res) => {
            this.IgniteMessages.showError(null, res.data.errorMessage ? res.data.errorMessage : res.data);

            this.setServerError(res.data);

            this.IgniteFormUtils.triggerValidation(this.form);

            this.isLoading = false;
        });
    }
}
