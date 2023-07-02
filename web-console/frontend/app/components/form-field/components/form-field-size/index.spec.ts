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

import 'mocha';
import {assert} from 'chai';
import componentModule from '../../index';

suite('form-field-size', () => {
    let $scope: ng.IScope;
    let $compile: ng.ICompileService;

    setup(() => {
        angular.module('test', [componentModule.name]);
        angular.mock.module('test');
        angular.mock.inject((_$rootScope_: ng.IScope, _$compile_: ng.ICompileService) => {
            $compile = _$compile_;
            $scope = _$rootScope_.$new();
        });
    });

    test('Switch editor measure', async() => {
        $scope.model = 1;
        const el = angular.element(`
            <form-field-size
                ng-model='model'
                id='period'
                name='period'
                label='period:'
                size-type='time'
                size-scale-label='sec'
                min='0'
                ng-ref='ctrl'
            />
        `);
        $compile(el)($scope);
        $scope.$digest();
        const ctrl = $scope.ctrl;
        assert.equal(ctrl.sizeScale.label, 'sec', 'Sec should be detected as default field measure');
        ctrl.sizeScale = {label: 'hour', translationId: 'hour', value: 60 * 60};
        $scope.$digest();
        assert.equal($scope.model, 60 * 60, 'Model value is recalculated on measure switch');
    });
});
