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

export default ['igniteIgfsPathUnique', ['$parse', ($parse) => {
    const link = (scope, el, attrs, [ngModel]) => {
        if (typeof attrs.igniteIgfsPathUnique === 'undefined' || !attrs.igniteIgfsPathUnique)
            return;

        ngModel.$validators.igniteIgfsPathUnique = (value) => {
            const arr = $parse(attrs.igniteIgfsPathUnique)(scope);

            // Return true in case if array not exist, array empty.
            if (!value || !arr || !arr.length)
                return true;

            const idx = _.findIndex(arr, (item) => item.path === value);

            // In case of new element check all items.
            if (attrs.name === 'newPath')
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
}]];
