

import {Component, Input, Inject} from '@angular/core';
import {FormFieldErrorStyles} from './errorStyles.provider';
import {VALIDATION_MESSAGES} from './validationMessages.provider';

@Component({
    selector: 'form-field-errors',
    templateUrl: './errors.template.html',
    styleUrls: ['./errors.style.url.scss']
})
export class FormFieldErrors<T extends {[errorType: string]: string}> {
    @Input()
    errorStyle: FormFieldErrorStyles;

    @Input()
    extraErrorMessages: T = {} as T;

    @Input()
    errorType: keyof T;

    static parameters = [[new Inject(VALIDATION_MESSAGES)]];

    constructor(private defaultMessages: VALIDATION_MESSAGES) {}
}
