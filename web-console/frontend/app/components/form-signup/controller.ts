

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
