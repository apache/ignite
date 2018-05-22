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

import 'mocha';
import {assert} from 'chai';
import angular from 'angular';
import module from './index';

const INDETERMINATE_CLASS = 'progress-line__indeterminate';
const COMPLETE_CLASS = 'progress-line__complete';

suite('progress-line', () => {
    let $scope;
    let $compile;

    setup(() => {
        angular.module('test', [module.name]);
        angular.mock.module('test');
        angular.mock.inject((_$rootScope_, _$compile_) => {
            $compile = _$compile_;
            $scope = _$rootScope_.$new();
        });
    });

    test('Progress states', () => {
        $scope.progress = -1;
        const el = angular.element(`<progress-line value='progress'></progress-line>`);

        $compile(el)($scope);
        $scope.$digest();

        assert.isTrue(
            el[0].classList.contains(INDETERMINATE_CLASS),
            'Adds indeterminate class for indeterminate state'
        );

        assert.isFalse(
            el[0].classList.contains(COMPLETE_CLASS),
            'Does not have complete class when in indeterminate state'
        );

        $scope.progress = 1;
        $scope.$digest();

        assert.isFalse(
            el[0].classList.contains(INDETERMINATE_CLASS),
            'Does not has indeterminate class when in finished state'
        );

        assert.isTrue(
            el[0].classList.contains(COMPLETE_CLASS),
            'Adds complete class when in finished state'
        );
    });
});
