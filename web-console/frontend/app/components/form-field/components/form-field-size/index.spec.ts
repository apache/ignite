

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
