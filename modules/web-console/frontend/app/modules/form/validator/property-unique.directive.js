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

/**
 * @param {ng.IParseService} $parse
 */
export default function factory($parse) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} el
     * @param {ng.IAttributes} attrs
     * @param {[ng.INgModelController]} [ngModel]
     */
    const link = (scope, el, attrs, [ngModel]) => {
        if (_.isUndefined(attrs.ignitePropertyUnique) || !attrs.ignitePropertyUnique)
            return;

        ngModel.$validators.ignitePropertyUnique = (value) => {
            const arr = $parse(attrs.ignitePropertyUnique)(scope);

            // Return true in case if array not exist, array empty.
            if (!value || !arr || !arr.length)
                return true;

            const key = value.split('=')[0];
            const idx = _.findIndex(arr, (item) => item.split('=')[0] === key);

            // In case of new element check all items.
            if (attrs.name === 'new')
                return idx < 0;

            // Check for $index in case of editing in-place.
            return (_.isNumber(scope.$index) && (idx < 0 || scope.$index === idx));
        };
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}

factory.$inject = ['$parse'];
