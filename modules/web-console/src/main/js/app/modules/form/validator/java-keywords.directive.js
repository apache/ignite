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

export default ['javaKeywords', ['JavaTypes', (JavaTypes) => {
    const link = (scope, el, attrs, [ngModel]) => {
        if (_.isUndefined(attrs.javaKeywords) || !attrs.javaKeywords)
            return;

        const packageOnly = attrs.javaPackageName === 'package-only';

        ngModel.$validators.javaKeywords = (value) => {
            if (value) {
                if (!JavaTypes.validIdentifier(value) || (!packageOnly && !JavaTypes.packageSpecified(value)))
                    return true;

                return _.findIndex(value.split('.'), JavaTypes.isKeywords) < 0;
            }

            return true;
        };
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}]];
