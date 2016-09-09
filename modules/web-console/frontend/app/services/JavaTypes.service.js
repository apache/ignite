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

// Java built-in class names.
import JAVA_CLASSES from '../data/java-classes.json';

// Java build-in primitive.
import JAVA_PRIMITIVES from '../data/java-primitives.json';

import JAVA_KEYWORDS from '../data/java-keywords.json';

export default ['JavaTypes', function() {
    return {
        /**
         * @param {String} clsName Class name to check.
         * @returns boolean 'true' if given class name non a Java built-in type.
         */
        nonBuiltInClass(clsName) {
            return _.isNil(_.find(JAVA_CLASSES, (clazz) => clsName === clazz.short || clsName === clazz.full));
        },
        /**
         * @param clsName Class name to check.
         * @returns Full class name for java build-in types or source class otherwise.
         */
        fullClassName(clsName) {
            const type = _.find(JAVA_CLASSES, (clazz) => clsName === clazz.short);

            return type ? type.full : clsName;
        },
        /**
         * @param {String} value text to check.
         * @returns boolean 'true' if given text is valid Java identifier.
         */
        validIdentifier(value) {
            const regexp = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$][a-zA-Z0-9_$]*)$/igm;

            return value === '' || regexp.test(value);
        },
        /**
         * @param {String} value text to check.
         * @returns boolean 'true' if given text is valid Java package.
         */
        validPackage(value) {
            const regexp = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$][a-zA-Z0-9_$]*(\.?\*)?)$/igm;

            return value === '' || regexp.test(value);
        },
        /**
         * @param {String} value text to check.
         * @returns boolean 'true' if given text is valid Java UUID value.
         */
        validUUID(value) {
            const regexp = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/igm;

            return value === '' || regexp.test(value);
        },
        /**
         * @param {String} value text to check.
         * @returns boolean 'true' if given text is a Java type with package.
         */
        packageSpecified(value) {
            return value.split('.').length >= 2;
        },
        /**
         * @param {String} value text to check.
         * @returns boolean 'true' if given text non Java keyword.
         */
        isKeywords(value) {
            return _.includes(JAVA_KEYWORDS, value);
        },
        /**
         * @param {String} clsName Class name to check.
         * @returns {boolean} 'true' if givent class name is java primitive.
         */
        isJavaPrimitive(clsName) {
            return _.includes(JAVA_PRIMITIVES, clsName);
        }
    };
}];
