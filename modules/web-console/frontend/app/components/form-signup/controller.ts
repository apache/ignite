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

import CountriesService from '../../services/Countries.service';
import {ISignupFormController} from '.';

export class FormSignup implements ng.IPostLink, ng.IOnDestroy, ng.IOnChanges {
    static $inject = ['IgniteCountries'];

    constructor(private Countries: ReturnType<typeof CountriesService>) {}

    countries = this.Countries.getAll();

    innerForm: ISignupFormController;
    outerForm: ng.IFormController;
    ngModel: ng.INgModelController;
    serverError: string | null = null;

    $postLink() {
        this.outerForm.$addControl(this.innerForm);
        this.innerForm.email.$validators.server = () => !this.serverError;
    }

    $onDestroy() {
        this.outerForm.$removeControl(this.innerForm);
    }

    $onChanges(changes: {serverError: ng.IChangesObject<FormSignup['serverError']>}) {
        if (changes.serverError && this.innerForm)
            this.innerForm.email.$validate();
    }
}
