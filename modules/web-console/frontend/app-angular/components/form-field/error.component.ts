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

import {Component, Input, ViewChild, Inject, ElementRef, AfterViewInit, TemplateRef} from '@angular/core';
import {FormField} from './formField.component';

@Component({
    selector: 'form-field-error',
    template: `<ng-template #errorTemplate><ng-content></ng-content></ng-template>`
})
export class FormFieldError implements AfterViewInit {
    @Input()
    error: string;

    @ViewChild('errorTemplate')
    template: TemplateRef<any>;

    static parameters = [[new Inject(ElementRef)], [new Inject(FormField)]];

    constructor(private ref: ElementRef, private formField: FormField) {}

    ngAfterViewInit() {
        this.formField.addExtraErrorMessage(this.error, this.template);
    }
}
