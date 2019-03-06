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
