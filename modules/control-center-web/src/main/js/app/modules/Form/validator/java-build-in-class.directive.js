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

import JAVA_CLASSES from 'app/data/java-classes.json!';
import JAVA_FULLNAME_CLASSES from 'app/data/java-fullname-classes.json!';

export default ['javaBuildInClass', [() => {
    const link = (scope, el, attrs, [ngModel]) => {
        const validate = (isValid) => {
            ngModel.$setValidity('javaBuildInClass', isValid);
        };

        if (typeof attrs.javaBuildInClass === 'undefined' || !attrs.javaBuildInClass)
            return;

        ngModel.$parsers.push((value) => {
            if (!value)
                validate(true);
            else {
                const jclasses = JAVA_CLASSES.filter((key) => value === key).length;
                const jfclasses = JAVA_FULLNAME_CLASSES.filter((key) => value === key).length;

                validate(!(jclasses || jfclasses));
            }

            return value;
        });
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel']
    };
}]];
