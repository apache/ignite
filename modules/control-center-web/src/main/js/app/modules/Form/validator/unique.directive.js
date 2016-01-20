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

export default ['igniteUnique', ['$parse', ($parse) => {
    const link = (scope, el, attrs, [ngModel]) => {
        if (typeof attrs.igniteUnique === 'undefined' || !attrs.igniteUnique)
            return;

        ngModel.$validators.igniteUnique = (value) => {
            const arr = $parse(attrs.igniteUnique)(scope);

            // Return true in case if array not exist, array empty, or value is unique.
            return !(arr && (arr.length > 0) && !!~arr.indexOf(value));
        };
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}]];
