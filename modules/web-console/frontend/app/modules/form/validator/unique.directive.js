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

import {ListEditableTransclude} from 'app/components/list-editable/components/list-editable-transclude/directive';
import isNumber from 'lodash/fp/isNumber';
import get from 'lodash/fp/get';

class Controller {
    /** @type {ng.INgModelController} */
    ngModel;
    /** @type {ListEditableTransclude} */
    listEditableTransclude;
    /** @type {Array} */
    items;
    /** @type {string?} */
    key;
    /** @type {Array<string>} */
    skip;

    static $inject = ['$scope'];

    /**
     * @param {ng.IScope} $scope
     */
    constructor($scope) {
        this.$scope = $scope;
    }

    $onInit() {
        const isNew = this.key && this.key.startsWith('new');
        const shouldNotSkip = (item) => get(this.skip[0], item) !== get(...this.skip);

        this.ngModel.$validators.igniteUnique = (value) => {
            const matches = (item) => (this.key ? item[this.key] : item) === value;

            if (!this.skip) {
                // Return true in case if array not exist, array empty.
                if (!this.items || !this.items.length)
                    return true;

                const idx = this.items.findIndex(matches);

                // In case of new element check all items.
                if (isNew)
                    return idx < 0;

                // Case for new component list editable.
                const $index = this.listEditableTransclude
                    ? this.listEditableTransclude.$index
                    : isNumber(this.$scope.$index) ? this.$scope.$index : void 0;

                // Check for $index in case of editing in-place.
                return (isNumber($index) && (idx < 0 || $index === idx));
            }
            // TODO: converge both branches, use $index as idKey
            return !(this.items || []).filter(shouldNotSkip).some(matches);
        };
    }

    $onChanges(changes) {
        this.ngModel.$validate();
    }
}

export default () => {
    return {
        controller: Controller,
        require: {
            ngModel: 'ngModel',
            listEditableTransclude: '?^listEditableTransclude'
        },
        bindToController: {
            items: '<igniteUnique',
            key: '@?igniteUniqueProperty',
            skip: '<?igniteUniqueSkip'
        }
    };
};
