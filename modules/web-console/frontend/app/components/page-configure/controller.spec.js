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

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {spy} from 'sinon';

import Controller from './controller';

const mocks = () => new Map([
    ['$scope', {
        $on: spy()
    }],
    ['PageConfigure', {
        onStateEnterRedirect: spy()
    }]
]);

suite('page-configure component controller', () => {
    test('State change success redirect', () => {
        const c = new Controller(...mocks().values());
        c.$onInit();
        c.$scope.$on.getCall(0).args[1](null, {name: 'base.items'});
        assert.isOk(
            c.PageConfigure.onStateEnterRedirect.calledOnce,
            'calls PageConfigure.onStateEnterRedirect every $stateChangeSuccess'
        );
        assert.deepEqual(
            c.PageConfigure.onStateEnterRedirect.getCall(0).args,
            [{name: 'base.items'}],
            'calls PageConfigure.onStateEnterRedirect with correct arguments'
        );
    });
});
