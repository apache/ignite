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

interface ISiginData {
    email: string,
    password: string
}

interface ISigninFormController extends ng.IFormController {
    email: ng.INgModelController,
    password: ng.INgModelController
}

export default class {
    data: ISiginData = {
        email: null,
        password: null
    };

    form: ISigninFormController;

    serverError: string = null;

    static $inject = ['Auth', 'IgniteMessages', 'IgniteFormUtils'];

    constructor(private Auth: AuthService, private IgniteMessages, private IgniteFormUtils) {}

    canSubmitForm(form: ISigninFormController) {
        return form.$error.server ? true : !form.$invalid;
    }

    $postLink() {
        this.form.email.$validators.server = () => !this.serverError;
        this.form.password.$validators.server = () => !this.serverError;
    }

    setServerError(error: string) {
        this.serverError = error;
        this.form.email.$validate();
        this.form.password.$validate();
    }

    signin() {
        this.IgniteFormUtils.triggerValidation(this.form);

        this.setServerError(null);

        if (!this.canSubmitForm(this.form))
            return;

        return this.Auth.signin(this.data.email, this.data.password).catch((res) => {
            this.IgniteMessages.showError(null, res.data);
            this.setServerError(res.data);
        });
    }
}
