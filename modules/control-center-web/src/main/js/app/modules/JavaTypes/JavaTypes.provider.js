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

import angular from 'angular';

// Java built-in short class names.
import JAVA_CLASSES from 'app/data/java-classes.json!';

// Java built-in full class names.
import JAVA_FULLNAME_CLASSES from 'app/data/java-fullname-classes.json!';

import JAVA_KEYWORDS from 'app/data/java-keywords.json!';

angular
    .module('ignite-console.JavaTypes', [])
    .provider('JavaTypes', function() {
        this.$get = [function() {
            return {
                /**
                 * @param cls Class name to check.
                 * @returns boolean 'true' if given class name non a Java built-in type.
                 */
                nonBuiltInClass(cls) {
                    return !(_.includes(JAVA_CLASSES, cls) || _.includes(JAVA_FULLNAME_CLASSES, cls));
                },
                /**
                 * @param value text to check.
                 * @returns boolean 'true' if given text is valid Java identifier.
                 */
                validIdentifier(value) {
                    const regexp = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$][a-zA-Z0-9_$]*)$/igm;

                    return value === '' || regexp.test(value);
                },
                /**
                 * @param value text to check.
                 * @returns boolean 'true' if given text is valid Java package.
                 */
                validPackage(value) {
                    const regexp = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$][a-zA-Z0-9_$]*(\.?\*)?)$/igm;

                    return value === '' || regexp.test(value);
                },
                /**
                 * @param value text to check.
                 * @returns boolean 'true' if given text non Java keyword.
                 */
                isKeywords(value) {
                    return _.includes(JAVA_KEYWORDS, value);
                }
            };
        }];
    });
