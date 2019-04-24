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

import {Component, Input} from '@angular/core';
import {PopperContent} from 'ngx-popper';

@Component({
    selector: 'form-field-tooltip',
    template: `
        <ignite-icon
            name='info'
            [popper]='content'
            popperApplyClass='ignite-popper,ignite-popper__tooltip'
            popperTrigger='hover'
            popperAppendTo='body'
        ></ignite-icon>
    `,
    styles: [`
        :host {
            display: inline-flex;
        }
        ignite-icon {
            color: #0067b9;
        }
    `]
})
export class FormFieldTooltip {
    @Input()
    content: PopperContent
}
