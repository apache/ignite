

import 'mocha';
import {assert} from 'chai';
import angular from 'angular';
import componentModule from './index';

suite('bs-select-menu', () => {
    /** @type {ng.IScope} */
    let $scope;
    /** @type {ng.ICompileService} */
    let $compile;

    setup(() => {
        angular.module('test', [componentModule.name]);
        angular.mock.module('test');
        angular.mock.inject((_$rootScope_, _$compile_) => {
            $compile = _$compile_;
            $scope = _$rootScope_.$new();
        });
    });

    test('Create/destroy', () => {
        $scope.$matches = [];
        $scope.show = false;
        const el = angular.element(`
            <div ng-if='show'>
                <bs-select-menu></bs-select-menu>
            </div>
        `);

        const overlay = () => document.body.querySelector('.bssm-click-overlay');

        $compile(el)($scope);
        $scope.$digest();
        assert.notOk(overlay(), 'No overlay on init');

        $scope.show = true;
        $scope.$isShown = true;
        $scope.$digest();
        assert.ok(overlay(), 'Adds overlay to body on show');

        $scope.show = false;
        $scope.$digest();
        assert.notOk(overlay(), 'Removes overlay when element is removed from DOM');

        $scope.show = true;
        $scope.$isShown = false;
        $scope.$digest();
        assert.notOk(overlay(), 'Removes overlay menu is closed');
    });
});
