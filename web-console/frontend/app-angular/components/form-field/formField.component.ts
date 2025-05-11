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

import {Component, AfterViewInit, Inject, Input, ContentChild, HostBinding, Injectable, TemplateRef} from '@angular/core';
import {FormFieldErrorStyles, FormFieldRequiredMarkerStyles} from './index';
import {FormFieldHint} from './hint.component';
import {FormControlName, FormControl} from '@angular/forms';
import './formField.component.scss';

@Injectable({
    providedIn: 'root'
})
export class FORM_FIELD_OPTIONS {
    requiredMarkerStyle: FormFieldRequiredMarkerStyles = FormFieldRequiredMarkerStyles.REQUIRED;
    errorStyle: FormFieldErrorStyles = FormFieldErrorStyles.ICON
}

@Component({
    selector: 'form-field',
    templateUrl: './formField.template.html',
    styleUrls: [`./formField.style.url.scss`]
})
export class FormField implements AfterViewInit {
    static parameters = [[new Inject(FORM_FIELD_OPTIONS)]];

    constructor(options: FORM_FIELD_OPTIONS) {
        this.errorStyle = options.errorStyle;
        this.requiredMarkerStyle = options.requiredMarkerStyle;
    }

    @Input()
    errorStyle: FormFieldErrorStyles;

    @Input()
    requiredMarkerStyle: FormFieldRequiredMarkerStyles;

    extraMessages = {};

    @ContentChild(FormControlName, {static: false})
    control: FormControlName;

    @ContentChild(FormFieldHint, {static: false})
    hint: FormFieldHint;

    @HostBinding('class.form-field__required')
    isRequired: boolean;

    @HostBinding('class.form-field__optional')
    isOptional: boolean;

    @HostBinding('class.form-field__icon-error')
    get isIconError() {
        return this.errorStyle === FormFieldErrorStyles.ICON;
    }

    @HostBinding('class.form-field__inline-error')
    get isInlineError() {
        return this.errorStyle === FormFieldErrorStyles.INLINE;
    }

    ngAfterViewInit() {
        // setTimeout fixes ExpressionChangedAfterItHasBeenCheckedError
        setTimeout(() => {
            const hasRequired: boolean = this.control && this.control.control && this.control.control.validator && this.control.control.validator({}).required;
            this.isOptional = this.requiredMarkerStyle === FormFieldRequiredMarkerStyles.OPTIONAL && !hasRequired;
            this.isRequired = this.requiredMarkerStyle === FormFieldRequiredMarkerStyles.REQUIRED && hasRequired;
        }, 0);
    }

    _getErrorType(control: FormControl): string {
        return control.errors ? Object.entries(control.errors).filter(([key, invalid]) => invalid).map(([key]) => key).pop() : void 0;
    }

    addExtraErrorMessage(key: string, message: TemplateRef<any>) {
        this.extraMessages = {
            ...this.extraMessages,
            [key]: message
        };
    }
}
