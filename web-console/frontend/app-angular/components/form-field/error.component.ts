

import {Component, Input, ViewChild, Inject, ElementRef, AfterViewInit, TemplateRef} from '@angular/core';
import {FormField} from './formField.component';

@Component({
    selector: 'form-field-error',
    template: `<ng-template #errorTemplate><ng-content></ng-content></ng-template>`
})
export class FormFieldError implements AfterViewInit {
    @Input()
    error: string;

    @ViewChild('errorTemplate', {static: false})
    template: TemplateRef<any>;

    static parameters = [[new Inject(ElementRef)], [new Inject(FormField)]];

    constructor(private ref: ElementRef, private formField: FormField) {}

    ngAfterViewInit() {
        this.formField.addExtraErrorMessage(this.error, this.template);
    }
}
