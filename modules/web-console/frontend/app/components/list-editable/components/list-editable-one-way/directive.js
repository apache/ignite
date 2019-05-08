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

import isMatch from 'lodash/isMatch';
import {default as ListEditableController} from '../../controller';

/** @type {ng.IDirectiveFactory} */
export default function listEditableOneWay() {
    return {
        require: {
            list: 'listEditable'
        },
        bindToController: {
            onItemChange: '&?',
            onItemRemove: '&?'
        },
        controller: class Controller {
            /** @type {ListEditableController} */
            list;
            /** @type {ng.ICompiledExpression} onItemChange */
            onItemChange;
            /** @type {ng.ICompiledExpression} onItemRemove */
            onItemRemove;

            static $inject = ['$scope'];
            /**
             * @param {ng.IScope} $scope
             */
            constructor($scope) {
                this.$scope = $scope;
            }
            $onInit() {
                this.list.save = (item, index) => {
                    if (!isMatch(this.list.ngModel.$viewValue[index], item)) this.onItemChange({$event: item});
                };
                this.list.remove = (index) => this.onItemRemove({$event: this.list.ngModel.$viewValue[index]});
            }
        }
    };
}
