/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import 'mocha';
import {assert} from 'chai';
import angular from 'angular';
import directive from './match.directive';

/**
 * @param {HTMLInputElement} el
 * @returns {ng.INgModelController}
 */
const ngModel = (el) => angular.element(el).data().$ngModelController;

suite('ignite-match', () => {
    /** @type {ng.IScope} */
    let $scope;
    /** @type {ng.ICompileService} */
    let $compile;

    setup(() => {
        angular.module('test', []).directive('igniteMatch', directive);
        angular.mock.module('test');
        angular.mock.inject((_$rootScope_, _$compile_) => {
            $compile = _$compile_;
            $scope = _$rootScope_.$new();
        });
    });

    test('Matching', () => {
        const el = angular.element(`
            <input type="password" ng-model="data.master"/>
            <input type="password" ng-model="data.slave" ignite-match="data.master"/>
        `);

        const setValue = (el, value) => {
            ngModel(el).$setViewValue(value, 'input');
            $scope.$digest();
        };

        $scope.data = {};
        $compile(el)($scope);
        $scope.$digest();

        // const [master, , slave] = el;
        // For some reason, this code not work after Babel, replaced with 'old' syntax.
        const master = el[0];
        const slave = el[2];

        setValue(slave, '123');
        $scope.$digest();

        assert.isTrue(
            slave.classList.contains('ng-invalid-mismatch'),
            `Invalidates if slave input changes value and it doesn't match master value`
        );
        assert.equal(
            $scope.data.slave,
            '123',
            'Allows invalid value into model'
        );

        setValue(master, '123');

        assert.isFalse(
            slave.classList.contains('ng-invalid-mismatch'),
            `Runs validation on master value change`
        );
    });
});
