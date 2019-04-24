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

import {Component, Input, Inject} from '@angular/core';
import {FormFieldErrorStyles} from './errorStyles.provider';
import {VALIDATION_MESSAGES} from './validationMessages.provider';

@Component({
    selector: 'form-field-errors',
    template: `
        <ng-template #validationMessage>
            <ng-template *ngIf='extraErrorMessages[errorType]' [ngTemplateOutlet]='extraErrorMessages[errorType]'></ng-template>
            <ng-container *ngIf='!extraErrorMessages[errorType] && defaultMessages[errorType]'>{{defaultMessages[errorType]}}</ng-container>
            <ng-container *ngIf='!extraErrorMessages[errorType] && !defaultMessages[errorType]'>Value is invalid: {{errorType}}</ng-container>
        </ng-template>
        <div *ngIf='errorStyle === "inline"' class='inline'>
            <ng-container *ngTemplateOutlet='validationMessage'></ng-container>
        </div>
        <div *ngIf='errorStyle === "icon"' class='icon'>
            <popper-content #popper>
                <ng-container *ngTemplateOutlet='validationMessage'></ng-container>
            </popper-content>
            <ignite-icon
                name='attention'
                [popper]='popper'
                popperApplyClass='ignite-popper,ignite-popper__error'
                popperTrigger='hover'
                popperPlacement='top'
                popperAppendTo='body'
            ></ignite-icon>
        </div>
    `,
    styles: [`
        :host {
            display: block;
        }
        .inline {
            padding: 5px 10px 0;
            color: #ee2b27;
            font-size: 12px;
            line-height: 14px;
        }
        .icon {
            color: #ee2b27;
            width: 100%;
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
        }
    `]
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
