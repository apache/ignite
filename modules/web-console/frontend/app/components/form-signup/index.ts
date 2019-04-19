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

import {component} from './component';

export default angular.module('ignite-console.form-signup', [])
    .component('formSignup', component);

export interface ISignupData {
    email: string,
    password: string,
    firstName: string,
    lastName: string,
    phone?: string,
    company: string,
    country: string
}

export interface ISignupFormController extends ng.IFormController {
    email: ng.INgModelController,
    password: ng.INgModelController,
    firstName: ng.INgModelController,
    lastName: ng.INgModelController,
    phone: ng.INgModelController,
    company: ng.INgModelController,
    country: ng.INgModelController
}
