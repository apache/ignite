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

import {Component, ViewChild} from '@angular/core';
import {PopperContent} from 'ngx-popper';

@Component({
    selector: 'form-field-hint',
    template: `
        <popper-content>
            <ng-content></ng-content>
        </popper-content>
    `
})
export class FormFieldHint {
    @ViewChild(PopperContent, {static: true})
    popper: PopperContent
}
