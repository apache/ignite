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

// Java build-in primitives.
import JAVA_PRIMITIVES from '../data/java-primitives.json';

// Java keywords.
import JAVA_KEYWORDS from '../data/java-keywords.json';

// Regular expression to check Java identifier.
const VALID_IDENTIFIER = /^[a-zA-Z_$][a-zA-Z0-9_$]*$/im;

// Regular expression to check Java class name.
const VALID_CLASS_NAME = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$][a-zA-Z0-9_$]*)$/im;

// Regular expression to check Java package.
const VALID_PACKAGE = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$][a-zA-Z0-9_$]*(\.?\*)?)$/im;

// Regular expression to check UUID string representation.
const VALID_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/im;

/**
 * Utility service for various check on java types.
 */
export default class JavaTypes {
    static $inject = ['IgniteClusterDefaults', 'IgniteCacheDefaults', 'IgniteIGFSDefaults'];

    constructor(clusterDflts, cacheDflts, igfsDflts) {
        this.enumClasses = _.uniq(this._enumClassesAcc(_.merge(clusterDflts, cacheDflts, igfsDflts), []));
        this.shortEnumClasses = _.map(this.enumClasses, (cls) => this.shortClassName(cls));
    }

    /**
     * Collects recursive enum classes.
     *
     * @param root Root object.
     * @param classes Collected classes.
     * @return {Array.<String>}
     * @private
     */
    _enumClassesAcc(root, classes) {
        return _.reduce(root, (acc, val, key) => {
            if (key === 'clsName')
                acc.push(val);
            else if (_.isObject(val))
                this._enumClassesAcc(val, acc);

            return acc;
        }, classes);
    }

    /**
     * Check if class name is non enum class in Ignite configuration.
     *
     * @param clsName
     * @return {boolean}
     */
    nonEnum(clsName) {
        return !_.includes(this.shortEnumClasses, clsName) && !_.includes(this.enumClasses, clsName);
    }

    /**
     * @param clsName {String} Class name to check.
     * @returns {boolean} 'true' if provided class name is a not Java built in class.
     */
    nonBuiltInClass(clsName) {
        return _.isNil(_.find(JAVA_CLASSES, (clazz) => clsName === clazz.short || clsName === clazz.full));
    }

    /**
     * @param clsName Class name to check.
     * @returns {String} Full class name for java build-in types or source class otherwise.
     */
    fullClassName(clsName) {
        const type = _.find(JAVA_CLASSES, (clazz) => clsName === clazz.short);

        return type ? type.full : clsName;
    }

    /**
     * Extract class name from full class name.
     *
     * @param clsName full class name.
     * @return {String} Class name.
     */
    shortClassName(clsName) {
        const dotIdx = clsName.lastIndexOf('.');

        return dotIdx > 0 ? clsName.substr(dotIdx + 1) : clsName;
    }

    /**
     * @param value {String} Value text to check.
     * @returns {boolean} 'true' if given text is valid Java class name.
     */
    validIdentifier(value) {
        return !!(value && VALID_IDENTIFIER.test(value));
    }

    /**
     * @param value {String} Value text to check.
     * @returns {boolean} 'true' if given text is valid Java class name.
     */
    validClassName(value) {
        return !!(value && VALID_CLASS_NAME.test(value));
    }

    /**
     * @param value {String} Value text to check.
     * @returns {boolean} 'true' if given text is valid Java package.
     */
    validPackage(value) {
        return !!(value && VALID_PACKAGE.test(value));
    }

    /**
     * @param value {String} Value text to check.
     * @returns {boolean} 'true' if given text is valid Java UUID value.
     */
    validUUID(value) {
        return !!(value && VALID_UUID.test(value));
    }

    /**
     * @param value {String} Value text to check.
     * @returns {boolean} 'true' if given text is a Java type with package.
     */
    packageSpecified(value) {
        return value.split('.').length >= 2;
    }

    /**
     * @param value {String} Value text to check.
     * @returns {boolean} 'true' if given value is one of Java reserved keywords.
     */
    isKeyword(value) {
        return !!(value && _.includes(JAVA_KEYWORDS, value.toLowerCase()));
    }

    /**
     * @param {String} clsName Class name to check.
     * @returns {boolean} 'true' if given class name is java primitive.
     */
    isPrimitive(clsName) {
        return _.includes(JAVA_PRIMITIVES, clsName);
    }

    /**
     * Convert some name to valid java name.
     *
     * @param prefix To append to java name.
     * @param name to convert.
     * @returns {string} Valid java name.
     */
    toJavaName(prefix, name) {
        const javaName = name ? this.shortClassName(name).replace(/[^A-Za-z_0-9]+/g, '_') : 'dflt';

        return prefix + javaName.charAt(0).toLocaleUpperCase() + javaName.slice(1);
    }
}
