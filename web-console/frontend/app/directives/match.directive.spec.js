

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
