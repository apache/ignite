/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {FormField} from './FormField';
import {t} from 'testcafe';

export class ModalInput {
    constructor() {
        this.valueInput = new FormField({ id: 'inputDialogFieldInput' });
    }

    async enterValue(value) {
        await t.typeText(this.valueInput.control, value);
    }

    async confirm() {
        await t.click('#copy-btn-confirm');
    }

    async cancel() {
        await t.click('#copy-btn-cancel');
    }

    async close() {
        await t.click('.modal .close');
    }
}
