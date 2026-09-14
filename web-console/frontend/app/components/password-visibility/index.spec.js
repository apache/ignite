

import 'mocha';
import {assert} from 'chai';
import angular from 'angular';
import module from './index';

const PASSWORD_VISIBLE_CLASS = 'password-visibility__password-visible';

suite('password-visibility', () => {
    /** @type {ng.IScope} */
    let $scope;
    /** @type {ng.ICompileService} */
    let $compile;

    angular.module('test', [module.name]);

    setup(() => {
        angular.module('test', [module.name]);
        angular.mock.module('test');
        angular.mock.inject((_$rootScope_, _$compile_) => {
            $compile = _$compile_;
            $scope = _$rootScope_.$new();
        });
    });

    test('Visibility toggle', () => {
        const el = angular.element(`
            <div password-visibility-root on-password-visibility-toggle='visible = $event'>
                <password-visibility-toggle-button></password-visibility-toggle-button>
            </div>
        `);
        $compile(el)($scope);
        const toggleButton = el.find('password-visibility-toggle-button').children()[0];
        $scope.$digest();

        assert.isFalse(el.hasClass(PASSWORD_VISIBLE_CLASS), 'Password is hidden by default');

        toggleButton.click();
        $scope.$digest();

        assert.isTrue(el.hasClass(PASSWORD_VISIBLE_CLASS), 'Password is visible after click on toggle button');
        assert.equal(true, $scope.visible, 'Event emits current visibility value');

        toggleButton.click();
        $scope.$digest();

        assert.isFalse(el.hasClass(PASSWORD_VISIBLE_CLASS), 'Password is hidden again after two clicks on button');
    });
});
