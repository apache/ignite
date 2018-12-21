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

import {Selector, t} from 'testcafe';

const body = Selector('.modal').withText('Confirmation').find('.modal-body');
const confirmButton = Selector('#confirm-btn-ok');
const cancelButton = Selector('#confirm-btn-cancel');
const closeButton = Selector('.modal').withText('Confirmation').find('.modal .close');

export const confirmation = {
    body,
    confirmButton,
    cancelButton,
    closeButton,
    async confirm() {
        await t.click(confirmButton);
    },
    async cancel() {
        await t.click(cancelButton);
    },
    async close() {
        await t.click(closeButton);
    }
};
