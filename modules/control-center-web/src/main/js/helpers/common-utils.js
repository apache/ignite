/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

// Entry point for common utils.
$commonUtils = {};

/**
 * @param v Value to check.
 * @returns {boolean} 'true' if value defined.
 */
$commonUtils.isDefined = function (v) {
    return !(v === undefined || v === null);
};

/**
 * @param v Value to check.
 * @returns {boolean} 'true' if value defined and not empty string.
 */
$commonUtils.isDefinedAndNotEmpty = function (v) {
    var defined = $commonUtils.isDefined(v);

    if (defined && (typeof(v) == 'string' ||
        Object.prototype.toString.call(v) === '[object Array]'))
        defined = v.length > 0;

    return defined;
};

/**
 * @param obj Object to check.
 * @param props Properties names.
 * @returns {boolean} 'true' if object contains at least one from specified properties.
 */
$commonUtils.hasProperty = function (obj, props) {
    for (var propName in props) {
        if (props.hasOwnProperty(propName)) {
            if (obj[propName])
                return true;
        }
    }

    return false;
};

/**
 * @param obj Object to check.
 * @param props Array of properties names.
 * @returns {boolean} 'true' if
 */
$commonUtils.hasAtLeastOneProperty = function (obj, props) {
    if (obj && props) {
        return _.findIndex(props, function (prop) {
                return $commonUtils.isDefined(obj[prop]);
            }) >= 0;
    }

    return false;
};

/**
 * Convert some name to valid java name.
 *
 * @param prefix To append to java name.
 * @param name to convert.
 * @returns {string} Valid java name.
 */
$commonUtils.toJavaName = function (prefix, name) {
    var javaName = name ? name.replace(/[^A-Za-z_0-9]+/, '_') : 'dflt';

    return prefix + javaName.charAt(0).toLocaleUpperCase() + javaName.slice(1);
};

$commonUtils.randomString = function (len) {
    var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var possibleLen = possible.length;

    var res = '';

    for (var i = 0; i < len; i++)
        res += possible.charAt(Math.floor(Math.random() * possibleLen));

    return res;
};

// For server side we should export Java code generation entry point.
if (typeof window === 'undefined') {
    module.exports = $commonUtils;
}
