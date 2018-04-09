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

'use strict';

const ArgumentChecker = require('./internal/ArgumentChecker');

/**
 * Supported Ignite type codes for primitive (simple) types.
 * @typedef ObjectType.PRIMITIVE_TYPE
 * @enum
 * @readonly
 * @property BYTE 1
 * @property SHORT 2
 * @property INTEGER 3
 * @property LONG 4
 * @property FLOAT 5
 * @property DOUBLE 6
 * @property CHAR 7
 * @property BOOLEAN 8
 * @property STRING 9
 * @property DATE 11
 * @property BYTE_ARRAY 12
 * @property SHORT_ARRAY 13
 * @property INTEGER_ARRAY 14
 * @property LONG_ARRAY 15
 * @property FLOAT_ARRAY 16
 * @property DOUBLE_ARRAY 17
 * @property CHAR_ARRAY 18
 * @property BOOLEAN_ARRAY 19
 * @property STRING_ARRAY 20
 * @property DATE_ARRAY 22
 */
const PRIMITIVE_TYPE = Object.freeze({
    BYTE : 1,
    SHORT : 2,
    INTEGER : 3,
    LONG : 4,
    FLOAT : 5,
    DOUBLE : 6,
    CHAR : 7,
    BOOLEAN : 8,
    STRING : 9,
    DATE : 11,
    BYTE_ARRAY : 12,
    SHORT_ARRAY : 13,
    INTEGER_ARRAY : 14,
    LONG_ARRAY : 15,
    FLOAT_ARRAY : 16,
    DOUBLE_ARRAY : 17,
    CHAR_ARRAY : 18,
    BOOLEAN_ARRAY : 19,
    STRING_ARRAY : 20,
    DATE_ARRAY : 22
});

/**
 * Supported Ignite type codes for non-primitive (composite) types.
 * @typedef ObjectType.COMPOSITE_TYPE
 * @enum
 * @readonly
 * @property OBJECT_ARRAY 23
 * @property MAP 25
 * @property NULL 101
 * @property COMPLEX_OBJECT 103
 */
const COMPOSITE_TYPE = Object.freeze({
    OBJECT_ARRAY : 23,
    MAP : 25,
    NULL : 101,
    COMPLEX_OBJECT : 103
});

/** 
 * Base class representing a type of Ignite object.
 *
 * The class has no public constructor. Only subclasses may be instantiated.
 *
 * There are two groups of Ignite object types:
 *
 * - Primitive (simple) types. To fully describe such a type it is enough to specify
 * Ignite type code {@link ObjectType.PRIMITIVE_TYPE} only.
 *
 * - Non-primitive (composite) types. To fully describe such a type
 * Ignite type code {@link ObjectType.COMPOSITE_TYPE} with additional information should be specified.
 * Eg. a kind of map or a kind of collection.
 *
 * This class helps the Ignite client to make a mapping between JavaScript types
 * and types used by Ignite.
 *
 * In many methods the Ignite client does not require to directly specify an object type.
 * In this case the Ignite client tries to make automatic mapping between JavaScript types
 * and Ignite object types according to the following mapping table:
 * <pre>
 *      JavaScript type              to / from        type code ({@link ObjectType.PRIMITIVE_TYPE} and {@link ObjectType.COMPOSITE_TYPE})
 *      null                         to / from        NULL
 *      number                       to / from        DOUBLE
 *      Array of number              to / from        DOUBLE_ARRAY
 *      string                       to / from        STRING
 *      Array of string              to / from        STRING_ARRAY
 *      boolean                      to / from        BOOLEAN
 *      Array of boolean             to / from        BOOLEAN_ARRAY
 *      Date                         to / from        DATE
 *      Array of Date                to / from        DATE_ARRAY
 *      Map                          to / from        MAP (HASH_MAP)
 *      any other Object             to /  -          COMPLEX_OBJECT
 *      Array of any other Object    to /  -          OBJECT_ARRAY
 * </pre>
 * Note: type of an array content is determined by the type of the first element of the array
 * (empty array has no automatic mapping).
 *
 * All other JavaScript types have no automatic mapping.
 *
 * @hideconstructor
 */

class ObjectType {
    static get PRIMITIVE_TYPE() {
        return PRIMITIVE_TYPE;
    }

    static get COMPOSITE_TYPE() {
        return COMPOSITE_TYPE;
    }    

    /** Private methods */

    constructor(typeCode) {
        this._typeCode = typeCode;
    }
}

/**
 * Base class representing a non-primitive (composite) type of Ignite object.
 *
 * The class has no public constructor. Only subclasses may be instantiated.
 *
 * @hideconstructor
 * @extends ObjectType
 */
class CompositeType extends ObjectType {
}

/**
 * Supported kinds of map.
 * @typedef MapObjectType.MAP_SUBTYPE
 * @enum
 * @readonly
 * @property HASH_MAP 1
 * @property LINKED_HASH_MAP 2
 */
const MAP_SUBTYPE = Object.freeze({
    HASH_MAP : 1,
    LINKED_HASH_MAP : 2
});

/**
 * Class representing a map type of Ignite object.
 *
 * It is described by COMPOSITE_TYPE.MAP {@link ObjectType.COMPOSITE_TYPE}
 * and one of {@link MapObjectType.MAP_SUBTYPE}.
 *
 * @extends CompositeType
 */
class MapObjectType extends CompositeType {
    static get MAP_SUBTYPE() {
        return MAP_SUBTYPE;
    }

    /**
     * Public constructor.
     *
     * Optionally specifies a kind of map and types of keys and values in the map.
     *
     * If a kind of map is not specified, MAP_SUBTYPE.HASH_MAP is assumed.
     *
     * If key and/or value type is not specified then during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     * 
     * @param {integer} [mapSubType=MAP_SUBTYPE.HASH_MAP] - map subtype, one of the {@link MapObjectType.MAP_SUBTYPE} constants.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [keyType=null] - type of the keys in the map:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [valueType=null] - type of the values in the map:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return {MapObjectType} - new MapObjectType instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(mapSubType = MapObjectType.MAP_SUBTYPE.HASH_MAP, keyType = null, valueType = null) {
        super(COMPOSITE_TYPE.MAP);
        const BinaryUtils = require('./internal/BinaryUtils');
        ArgumentChecker.hasValueFrom(mapSubType, 'mapSubType', false, MapObjectType.MAP_SUBTYPE);
        this._mapSubType = mapSubType;
        BinaryUtils.checkObjectType(keyType, 'keyType');
        BinaryUtils.checkObjectType(valueType, 'valueType');
        this._keyType = keyType;
        this._valueType = valueType;
    }
}

/**
 * Class representing a complex type of Ignite object.
 *
 * It is described by COMPOSITE_TYPE.COMPLEX_OBJECT {@link ObjectType.COMPOSITE_TYPE},
 * a name of the complex type and a JavaScript Object which is mapped to/from the Ignite complex type.
 *
 * @extends CompositeType
 */
class ComplexObjectType extends CompositeType {

    /**
     * Public constructor.
     *
     * Specifies a JavaScript class which will be mapped to/from the complex type.
     * This specification is done using an instance of the JavaScript Object.
     *
     * If an object of the complex type is going to be received (deserialized),
     * the JavaScript Object must have a constructor without parameters or with optional parameters only.
     *
     * The JavaScript Object defines a set of fields of the complex type.
     *
     * By default, the fields have no types specified. It means during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * A type of any field may be specified later by setFieldType() method.
     *
     * By default, the name of the complex type is the name of the JavaScript Object.
     * The name may be explicitely specified using an optional parameter in the constructor.
     * 
     * @param {object} jsObject - instance of JavaScript Object which will be mapped to/from this complex type.
     * @param {string} [typeName=null] - name of the complex type.
     *
     * @return {ComplexObjectType} - new ComplexObjectType instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(jsObject, typeName = null) {
        super(COMPOSITE_TYPE.COMPLEX_OBJECT);
        this._template = jsObject;
        this._objectConstructor = jsObject && jsObject.constructor ?
            jsObject.constructor : Object;
        if (!typeName) {
            typeName = this._objectConstructor.name;
        }
        this._typeName = typeName;
        this._fields = new Map();
    }

    /**
     * Specifies a type of the field in the complex type.
     *
     * If the type is not specified then during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     * 
     * @param {string} fieldName - name of the field.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} fieldType - type of the field:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     *
     * @return {ComplexObjectType} - the same instance of the ComplexObjectType.
     *
     * @throws {IgniteClientError} if error.
     */
    setFieldType(fieldName, fieldType) {
        const BinaryUtils = require('./internal/BinaryUtils');
        BinaryUtils.checkObjectType(fieldType, 'fieldType');
        this._fields.set(fieldName, fieldType);
        return this;
    }

    /** Private methods */

    /**
     * @ignore
     */
    _getFields() {
        return this._fields ? this._fields.entries() : null;
    }

    /**
     * @ignore
     */
    _getFieldType(fieldName) {
        return this._fields.get(fieldName);
    }
}

/**
 * Class representing an array of Ignite complex type objects.
 *
 * It is described by COMPOSITE_TYPE.OBJECT_ARRAY {@link ObjectType.COMPOSITE_TYPE}
 * and by a concrete {@link ComplexObjectType} instance which defines an element of the array.
 *
 * @extends CompositeType
 */
class ObjectArrayType extends CompositeType {

    /**
     * Public constructor.
     *
     * @param {ComplexObjectType} elementType - type of the array element
     *
     * @return {ObjectArrayType} - new ObjectArrayType instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(elementType) {
        super(COMPOSITE_TYPE.OBJECT_ARRAY);
        ArgumentChecker.hasType(elementType, 'elementType', false, ComplexObjectType);
        this._elementType = elementType;
    }
}

module.exports.ObjectType = ObjectType;
module.exports.CompositeType = CompositeType;
module.exports.MapObjectType = MapObjectType;
module.exports.ComplexObjectType = ComplexObjectType;
module.exports.ObjectArrayType = ObjectArrayType;
