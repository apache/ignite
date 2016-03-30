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

// Entry point for common data structures.
const $dataStructures = {};

// Java build-in primitive.
$dataStructures.JAVA_BUILD_IN_PRIMITIVES = ['boolean', 'byte', 'double', 'float', 'int', 'long', 'short'];

// Pairs of Java build-in classes.
$dataStructures.JAVA_BUILD_IN_CLASSES = [
    {short: 'BigDecimal', full: 'java.math.BigDecimal'},
    {short: 'Boolean', full: 'java.lang.Boolean'},
    {short: 'Byte', full: 'java.lang.Byte'},
    {short: 'Date', full: 'java.sql.Date'},
    {short: 'Double', full: 'java.lang.Double'},
    {short: 'Float', full: 'java.lang.Float'},
    {short: 'Integer', full: 'java.lang.Integer'},
    {short: 'Long', full: 'java.lang.Long'},
    {short: 'Object', full: 'java.lang.Object'},
    {short: 'Short', full: 'java.lang.Short'},
    {short: 'String', full: 'java.lang.String'},
    {short: 'Time', full: 'java.sql.Time'},
    {short: 'Timestamp', full: 'java.sql.Timestamp'},
    {short: 'UUID', full: 'java.util.UUID'}
];

/**
 * @param clsName Class name to check.
 * @returns 'true' if given class name is a java build-in type.
 */
$dataStructures.isJavaBuiltInClass = function (clsName) {
    if ($commonUtils.isDefined(clsName)) {
        for (var i = 0; i < $dataStructures.JAVA_BUILD_IN_CLASSES.length; i++) {
            var jbic = $dataStructures.JAVA_BUILD_IN_CLASSES[i];

            if (clsName == jbic.short || clsName == jbic.full)
                return true;
        }
    }

    return false;
};

/**
 *
 * @param clsName Class name to check.
 * @returns {boolean} 'true' if givent class name is java primitive.
 */
$dataStructures.isJavaPrimitive = function (clsName) {
    return _.includes($dataStructures.JAVA_BUILD_IN_PRIMITIVES, clsName);
};

/**
 * @param clsName Class name to check.
 * @returns Full class name for java build-in types or source class otherwise.
 */
$dataStructures.fullClassName = function (clsName) {
    if ($commonUtils.isDefined(clsName)) {
        for (var i = 0; i < $dataStructures.JAVA_BUILD_IN_CLASSES.length; i++) {
            var jbic = $dataStructures.JAVA_BUILD_IN_CLASSES[i];

            if (clsName == jbic.short)
                return jbic.full;
        }
    }

    return clsName;
};

export default $dataStructures;
