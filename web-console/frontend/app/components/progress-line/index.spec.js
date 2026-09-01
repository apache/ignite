

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
