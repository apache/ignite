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

import {spy} from 'sinon';
import {assert} from 'chai';
import {FakeUiCanExitController} from './fakeUICanExit';

suite('Page configuration fakeUIcanExit directive', () => {
    test('It unsubscribes from state events when destroyed', () => {
        const $element = {data: () => [{uiCanExit: () => {}}]};
        const off = spy();
        const $transitions = {onBefore: () => off};
        const i = new FakeUiCanExitController($element, $transitions);
        i.$onInit();
        i.$onDestroy();
        assert.ok(off.calledOnce, 'Calls off when destroyed');
    });
});
