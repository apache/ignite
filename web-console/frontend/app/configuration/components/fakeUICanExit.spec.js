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
