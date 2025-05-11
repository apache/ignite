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

import {Injectable, Inject} from '@angular/core';
import { TranslateService } from '@ngx-translate/core';

@Injectable({
    providedIn: 'root'
})
export class VALIDATION_MESSAGES {

    static parameters = [[new Inject(TranslateService)]];

    required;
    email;
    passwordMatch;

    constructor(translate: TranslateService) {
        translate.get('validationMessages.required').subscribe((val) => this.required = val);
        translate.get('validationMessages.email').subscribe((val) => this.email = val);
        translate.get('validationMessages.passwordMatch').subscribe((val) => this.passwordMatch = val);
    }

}
