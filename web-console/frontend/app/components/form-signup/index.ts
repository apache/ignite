

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
