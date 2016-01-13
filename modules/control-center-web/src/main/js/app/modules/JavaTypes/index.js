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

angular
    .module('ignite-console.JavaTypes', [])
    .provider('JavaTypes', function() {
        // Java built-in short class names.
        const _classes = [
            'BigDecimal', 'Boolean', 'Byte', 'Date', 'Double', 'Float', 'Integer', 'Long', 'Short', 'String', 'Time', 'Timestamp', 'UUID'
        ];

        // TODO use later const _types = [
        //    'BigDecimal', 'boolean', 'Boolean', 'byte', 'Byte', 'Date', 'double', 'Double', 'float', 'Float',
        //    'int', 'Integer', 'long', 'Long', 'short', 'Short', 'String', 'Time', 'Timestamp', 'UUID'
        // ];

        // Java built-in full class names.
        const _fullNameClasses = [
            'java.math.BigDecimal', 'java.lang.Boolean', 'java.lang.Byte', 'java.sql.Date', 'java.lang.Double',
            'java.lang.Float', 'java.lang.Integer', 'java.lang.Long', 'java.lang.Short', 'java.lang.String',
            'java.sql.Time', 'java.sql.Timestamp', 'java.util.UUID'
        ];

        this.$get = [function() {
            return {
                /**
                 * @param cls Class name to check.
                 * @returns 'true' if given class name is a java build-in type.
                 */
                isBuildInClass(cls) {
                    return _.contains(_classes, cls) || _.contains(_fullNameClasses, cls);
                }
            };
        }];
    });
