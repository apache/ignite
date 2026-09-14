

import 'mocha';
import {assert} from 'chai';
import {spy} from 'sinon';
import {ListEditableAddItemButton as Ctrl} from './component';

suite('list-editable-add-item-button component', () => {
    test('has addItem method with correct locals', () => {
        const i = new Ctrl();
        i._listEditable = {
            ngModel: {
                editListItem: spy()
            }
        };
        i._listEditable.ngModel.editListItem.bind = spy(() => i._listEditable.ngModel.editListItem);
        i._addItem = spy();
        i.addItem();
        assert.isOk(i._addItem.calledOnce);
        assert.deepEqual(i._addItem.lastCall.args[0].$edit, i._listEditable.ngModel.editListItem );
    });

    test('inserts button after list-editable', () => {
        Ctrl.hasItemsTemplate = 'tpl';
        const $scope = {};
        const clone = {
            insertAfter: spy()
        };
        const $transclude = spy((scope, attach) => attach(clone));
        const $compile = spy(() => $transclude);
        const i = new Ctrl($compile, $scope);
        i._listEditable = {
            ngModel: {
                editListItem: spy(),
                $element: {}
            }
        };
        i.$postLink();
        assert.isOk($compile.calledOnce);
        assert.equal($compile.lastCall.args[0], Ctrl.hasItemsTemplate);
        assert.equal($transclude.lastCall.args[0], $scope);
        assert.equal(clone.insertAfter.lastCall.args[0], i._listEditable.$element);
    });

    test('exposes hasItems getter', () => {
        const i = new Ctrl();
        i._listEditable = {
            ngModel: {
                $isEmpty: spy((v) => !v.length),
                $viewValue: [1, 2, 3]
            }
        };
        assert.isOk(i.hasItems);
        i._listEditable.ngModel.$viewValue = [];
        assert.isNotOk(i.hasItems);
    });
});
