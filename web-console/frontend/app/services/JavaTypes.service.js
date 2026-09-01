

import _ from 'lodash';
import includes from 'lodash/includes';
import isNil from 'lodash/isNil';
import find from 'lodash/find';

// Java built-in class names.
import JAVA_CLASSES from '../data/java-classes.json';

// Java build-in primitives.
import JAVA_PRIMITIVES from '../data/java-primitives.json';

// Java keywords.
import JAVA_KEYWORDS from '../data/java-keywords.json';

// Extended list of Java built-in class names.
const JAVA_CLASS_STRINGS = JAVA_CLASSES.slice();

// Regular expression to check Java identifier.
const VALID_IDENTIFIER = /^[a-zA-Z_$\u4e00-\u9fa5][a-zA-Z0-9_$\u4e00-\u9fa5]*$/im;

// Regular expression to check Java class name.
const VALID_CLASS_NAME = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$\u4e00-\u9fa5][a-zA-Z0-9_$\u4e00-\u9fa5]*)$/im;

// Regular expression to check Java package.
const VALID_PACKAGE = /^(([a-zA-Z_$][a-zA-Z0-9_$]*)\.)*([a-zA-Z_$][a-zA-Z0-9_$]*(\.?\*)?)$/im;

// Regular expression to check UUID string representation.
const VALID_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/im;



/**
 * Utility service for various check on java types.
 */
export default class JavaTypes {
    constructor() {
        JAVA_CLASS_STRINGS.push({short: 'byte[]', full: 'byte[]', stringValue: '[B'});
    }

    /**
     * @param clsName {String} Class name to check.
     * @returns {boolean} 'true' if provided class name is a not Java built in class.
     */
    nonBuiltInClass(clsName) {
        return isNil(find(JAVA_CLASSES, (clazz) => clsName === clazz.short || clsName === clazz.full));
    }

    /**
     * @param clsName Class name to check.
     * @returns {String} Full class name for java build-in types or source class otherwise.
     */
    fullClassName(clsName) {
        const type = find(JAVA_CLASSES, (clazz) => clsName === clazz.short);

        return type ? type.full : clsName;
    }

    /**
     * @param clsName Class name to check.
     * @returns {String} Full class name string presentation for java build-in types or source class otherwise.
     */
    stringClassName(clsName) {
        const type = _.find(JAVA_CLASS_STRINGS, (clazz) => clsName === clazz.short);

        return type ? type.stringValue || type.full : clsName;
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
        return !!(value && includes(JAVA_KEYWORDS, value.toLowerCase()));
    }

    /**
     * @param {String} clsName Class name to check.
     * @returns {boolean} 'true' if given class name is java primitive.
     */
    isPrimitive(clsName) {
        return includes(JAVA_PRIMITIVES, clsName);
    }

    isAllUpperCase(str) {
        return /^[A-Z_]+$/.test(str);
    }

    /**
     * Convert some name to valid java name.
     *
     * @param prefix To append to java name.
     * @param name to convert.
     * @returns {string} Valid java name.
     */
    toJavaName(prefix, name) {
        const javaName = name ? this.shortClassName(name).replace(/[^A-Za-z_0-9\u4e00-\u9fa5]+/g, '_') : 'dflt';

        return prefix + javaName.charAt(0).toLocaleUpperCase() + javaName.slice(1);
    }

    /**
     * @param {String} s var name to check.
     * @returns {boolean} 'true' if given class name is java or sql ident.
     */
    isValidJavaIdentifier(s) {
        return this.validIdentifier(s) && !this.isKeyword(s) && this.nonBuiltInClass(s);
        //&& SqlTypes.validIdentifier(s) && !SqlTypes.isKeyword(s);
    }

    toJavaIdentifier(name) {
        if (_.isEmpty(name))
            return 'DB';

        const len = name.length;

        let ident = '';

        let capitalizeNext = true;
        let lastCh = ' '
        for (let i = 0; i < len; i++) {
            const ch = name.charAt(i);
            if ((ch === ' ' || ch === '_') && !('A'<=lastCh && lastCh<='Z'))
                capitalizeNext = true;
            else if (ch === '-') {
                ident += '_';
                capitalizeNext = true;
            }
            else if (capitalizeNext) {
                ident += ch.toLocaleUpperCase();

                capitalizeNext = false;
            }
            else
                ident += ch;
            lastCh = ch;
        }

        return ident;
    }

    toJavaClassName(name) {
        if (this.isAllUpperCase(name))
            return name;
        const clazzName = this.toJavaIdentifier(name);

        if (this.isValidJavaIdentifier(clazzName))
            return clazzName;

        return 'Class' + clazzName;
    }

    toJavaFieldName(dbName) {
        if (this.isAllUpperCase(dbName))
            return dbName;
        const javaName = this.toJavaIdentifier(dbName);

        const fieldName = javaName.charAt(0).toLocaleLowerCase() + javaName.slice(1);

        if (this.isValidJavaIdentifier(fieldName))
            return fieldName;

        return 'field' + javaName;
    }

    /**
     * @param clsName Class name to check.
     * @param additionalClasses List of classes to check as builtin.
     * @returns {Boolean} 'true' if given class name is a java build-in type.
     */
    isJavaBuiltInClass(clsName, additionalClasses=null) {
        if (!clsName || clsName.trim().length === 0)
            return false;

        return _.includes(this.javaBuiltInClasses(), clsName) 
            || _.includes(this.javaBuiltInFullNameClasses(), clsName)
            || (_.isArray(additionalClasses) && _.includes(additionalClasses, clsName));
    }

    javaBuiltInBaseClasses() {
        return _.map(JAVA_CLASSES, (option) => {
            return option.short
        });
    }

    javaBuiltInClasses() {
        return _.map(JAVA_CLASS_STRINGS, (option) => {
            return option.short
        });
    }
    javaBuiltInFullNameClasses() {
        return _.map(JAVA_CLASS_STRINGS, (option) => {
            return option.full
        });
    }

    mkJavaClassesOptions() {
        return _.map(JAVA_CLASS_STRINGS, (option) => {
            return {value: option.full, label: option.short};
        });
    }

    mkJavaBuiltInTypesOptions() {
        let baseType = _.map(JAVA_PRIMITIVES, (option) => {
            return {value: option, label: option};
        });
        let classes = _.map(JAVA_CLASSES, (option) => {
            return {value: option.short, label: option.short};
        });
        return _.concat(baseType, classes,[{value: '[B', label: 'byte[]'}]);
    }
}
