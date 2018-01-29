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

import _ from 'lodash';

export default class {
    static $inject = ['$animate', '$element', '$transclude'];

    constructor($animate, $element, $transclude) {
        $animate.enabled($element, false);

        this.hasItemView = $transclude.isSlotFilled('itemView');

        this._cache = {};
    }

    $index(item, $index) {
        if (item._id)
            return item._id;

        return $index;
    }

    $onInit() {
        this.ngModel.$isEmpty = (value) => {
            return !Array.isArray(value) || !value.length;
        };
    }

    save(data, idx) {
        this.ngModel.$setViewValue(this.ngModel.$viewValue.map((v, i) => i === idx ? data : v));
    }

    revert(idx) {
        delete this._cache[idx];
    }

    remove(idx) {
        this.ngModel.$setViewValue(this.ngModel.$viewValue.filter((v, i) => i !== idx));
    }

    isEditView(idx) {
        return this._cache.hasOwnProperty(idx) || _.isEmpty(this.ngModel.$viewValue[idx]);
    }

    getEditView(idx) {
        return this._cache[idx];
    }

    startEditView(idx) {
        this._cache[idx] = _.clone(this.ngModel.$viewValue[idx]);
    }

    stopEditView(data, idx, form) {
        delete this._cache[idx];

        if (form.$pristine)
            return;

        if (form.$valid)
            this.save(data, idx);
        else
            this.revert(idx);
    }
}
