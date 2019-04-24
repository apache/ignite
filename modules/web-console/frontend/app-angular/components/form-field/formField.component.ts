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

import {Component, AfterViewInit, Inject, Input, ContentChild, HostBinding, Injectable} from '@angular/core';
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
    template: `
        <ng-template #errors>
            <form-field-errors
                *ngIf='(control?.dirty || control?.touched) && control?.invalid'
                [errorStyle]='errorStyle'
                [errorType]='_getErrorType(control?.control)'
                [extraErrorMessages]='extraMessages'
            ></form-field-errors>
        </ng-template>
        <div class="angular-form-field__label">
            <ng-content select="label"></ng-content>
            <form-field-tooltip *ngIf='hint' [content]='hint.popper'></form-field-tooltip>
        </div>
        <div class="angular-form-field__input" [attr.data-overlay-items-count]='overlayEl.childElementCount'>
            <ng-content></ng-content>
        </div>
        <div class="input-overlay" #overlayEl>
            <ng-container *ngIf='errorStyle === "icon"'>
                <ng-container *ngTemplateOutlet='errors'></ng-container>
            </ng-container>
            <ng-content select='[formFieldOverlay]'></ng-content>
        </div>
        <ng-container *ngIf='errorStyle === "inline"'>
            <ng-container *ngTemplateOutlet='errors'></ng-container>
        </ng-container>
    `,
    styles: [`
        .angular-form-field__input {
            position: relative;
        }
        .input-overlay {
            display: grid;
        }
    `]
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

    @ContentChild(FormControlName)
    control: FormControlName;

    @ContentChild(FormFieldHint)
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

    addExtraErrorMessage(key, message) {
        this.extraMessages = {
            ...this.extraMessages,
            [key]: message
        };
    }
}
