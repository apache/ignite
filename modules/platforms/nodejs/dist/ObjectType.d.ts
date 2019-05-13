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
 * In many methods the Ignite client does not require to directly specify a type of Ignite object.
 * In this case the Ignite client tries to make automatic mapping between JavaScript types
 * and Ignite object types according to the following mapping tables:
 *
 * ----------------------------------------------------------------------------
 *
 * DEFAULT MAPPING FROM JavaScript type TO Ignite type code.
 *
 * This mapping is used when an application does not explicitly specify an Ignite type
 * for a field and is writing data to that field.
 *
 * <pre>
 * | JavaScript type           | Ignite type code      |
 * | ------------------------- | ----------------------|
 * | number                    | DOUBLE                |
 * | boolean                   | BOOLEAN               |
 * | string                    | STRING                |
 * | Date                      | DATE                  |
 * | Timestamp*                | TIMESTAMP             |
 * | EnumItem*                 | ENUM                  |
 * | Decimal**                 | DECIMAL               |
 * | BinaryObject*             | COMPLEX_OBJECT        |
 * | Array of number           | DOUBLE_ARRAY          |
 * | Array of boolean          | BOOLEAN_ARRAY         |
 * | Array of string           | STRING_ARRAY          |
 * | Array of Date             | DATE_ARRAY            |
 * | Array of Timestamp*       | TIMESTAMP_ARRAY       |
 * | Array of EnumItem*        | ENUM_ARRAY            |
 * | Array of Decimal**        | DECIMAL_ARRAY         |
 * | Array of BinaryObject*    | OBJECT_ARRAY          |
 * | Array of any other Object | OBJECT_ARRAY          |
 * | Set                       | COLLECTION (HASH_SET) |
 * | Map                       | MAP (HASH_MAP)        |
 * | any other Object          | COMPLEX_OBJECT        |
 * </pre>
 *
 * Type of an array content is determined by the type of the first element of the array.
 * Empty array has no default mapping.
 *
 * All other JavaScript types have no default mapping.
 *
 * ----------------------------------------------------------------------------
 *
 * DEFAULT MAPPING FROM Ignite type code TO JavaScript type.
 *
 * This mapping is used when an application does not explicitly specify an Ignite type
 * for a field and is reading data from that field.
 *
 * <pre>
 * | Ignite type code             | JavaScript type                       |
 * | ---------------------------- | --------------------------------------|
 * | BYTE                         | number                                |
 * | SHORT                        | number                                |
 * | INTEGER                      | number                                |
 * | LONG                         | number                                |
 * | FLOAT                        | number                                |
 * | DOUBLE                       | number                                |
 * | DECIMAL                      | Decimal**                             |
 * | BOOLEAN                      | boolean                               |
 * | STRING                       | string                                |
 * | CHAR                         | string (one character)                |
 * | UUID                         | Array of number (16 numbers)          |
 * | DATE                         | Date                                  |
 * | TIME                         | Date                                  |
 * | TIMESTAMP                    | Timestamp*                            |
 * | ENUM                         | EnumItem*                             |
 * | COMPLEX_OBJECT               | BinaryObject*                         |
 * | BYTE_ARRAY                   | Array of number                       |
 * | SHORT_ARRAY                  | Array of number                       |
 * | INTEGER_ARRAY                | Array of number                       |
 * | LONG_ARRAY                   | Array of number                       |
 * | FLOAT_ARRAY                  | Array of number                       |
 * | DOUBLE_ARRAY                 | Array of number                       |
 * | DECIMAL_ARRAY                | Array of Decimal**                    |
 * | BOOLEAN_ARRAY                | Array of boolean                      |
 * | STRING_ARRAY                 | Array of string                       |
 * | CHAR_ARRAY                   | Array of string (one character)       |
 * | UUID_ARRAY                   | Array of Array of number (16 numbers) |
 * | DATE_ARRAY                   | Array of Date                         |
 * | TIME_ARRAY                   | Array of Date                         |
 * | TIMESTAMP_ARRAY              | Array of Timestamp*                   |
 * | ENUM_ARRAY                   | Array of EnumItem*                    |
 * | OBJECT_ARRAY                 | Array                                 |
 * | COLLECTION (USER_COL)        | Array                                 |
 * | COLLECTION (ARR_LIST)        | Array                                 |
 * | COLLECTION (LINKED_LIST)     | Array                                 |
 * | COLLECTION (SINGLETON_LIST)  | Array                                 |
 * | COLLECTION (HASH_SET)        | Set                                   |
 * | COLLECTION (LINKED_HASH_SET) | Set                                   |
 * | COLLECTION (USER_SET)        | Set                                   |
 * | MAP (HASH_MAP)               | Map                                   |
 * | MAP (LINKED_HASH_MAP)        | Map                                   |
 * | NULL                         | null                                  |
 * </pre>
 *
 * ----------------------------------------------------------------------------
 *
 * RETURNED JavaScript types WHEN READING DATA OF THE SPECIFIED Ignite type code.
 *
 * When an application explicitly specifies an Ignite type for a field
 * and is reading data from that field - the following JavaScript types
 * are returned for every concrete Ignite type code -
 *
 * SEE THE PREVIOUS TABLE with the following additional comments:
 *
 * - for COMPLEX_OBJECT the Ignite Client returns a JavaScript Object
 * which is defined by the specified {@link ComplexObjectType}.
 *
 * - the returned Map for MAP is defined by the specified {@link MapObjectType}.
 *
 * - the returned Set or Array for COLLECTION is defined by the specified {@link CollectionObjectType}.
 *
 * - the returned Array for OBJECT_ARRAY is defined by the specified {@link ObjectArrayType}.
 *
 * - NULL cannot be specified as a type of a field but JavaScript null may be returned
 * as a value of a field.
 *
 * ----------------------------------------------------------------------------
 *
 * ALLOWED JavaScript types WHEN WRITING DATA OF THE SPECIFIED Ignite type code.
 *
 * When an application explicitly specifies an Ignite type for a field
 * and is writing data to that field - the following JavaScript types
 * are allowed for every concrete Ignite type code -
 *
 * SEE THE PREVIOUS TABLE with the following additional comments:
 *
 * - for COMPLEX_OBJECT the Ignite Client allows a JavaScript Object
 * which is defined by the specified {@link ComplexObjectType}.
 *
 * - the allowed Map for MAP is defined by the specified {@link MapObjectType}.
 *
 * - the allowed Set or Array for COLLECTION is defined by the specified {@link CollectionObjectType}.
 *
 * - the allowed Array for OBJECT_ARRAY is defined by the specified {@link ObjectArrayType}.
 *
 * - NULL cannot be specified as a type of a field but JavaScript null is allowed
 * as value of a field (but not as a key/value in a cache) or as a value of Array/Set/Map element
 * for all Ignite types, except BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, CHAR, BOOLEAN.
 *
 * - for all *_ARRAY Ignite types an empty JavaScript Array is allowed.
 *
 * ----------------------------------------------------------------------------
 *
 * COMMENTS TO ALL TABLES
 *
 * JavaScript type - is a JavaScript primitive or a JavaScript Object
 * ({@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures})
 *
 * (*) Timestamp, EnumItem and BinaryObject - are JavaScript Objects introduced by the Ignite client.
 *
 * (**) Decimal - is an external JavaScript Object exported into the Ignite client
 * ({@link https://github.com/MikeMcl/decimal.js})
 *
 * Ignite type code - is the type code of an Ignite primitive type ({@link ObjectType.PRIMITIVE_TYPE})
 * or an Ignite composite type ({@link ObjectType.COMPOSITE_TYPE}).
 *
 * ----------------------------------------------------------------------------
 *
 * @hideconstructor
 */
export declare class ObjectType {
    protected _typeCode: any;
    readonly typeCode: any;
    static readonly PRIMITIVE_TYPE: Readonly<{
        BYTE: number;
        SHORT: number;
        INTEGER: number;
        LONG: number;
        FLOAT: number;
        DOUBLE: number;
        CHAR: number;
        BOOLEAN: number;
        STRING: number;
        UUID: number;
        DATE: number;
        BYTE_ARRAY: number;
        SHORT_ARRAY: number;
        INTEGER_ARRAY: number;
        LONG_ARRAY: number;
        FLOAT_ARRAY: number;
        DOUBLE_ARRAY: number;
        CHAR_ARRAY: number;
        BOOLEAN_ARRAY: number;
        STRING_ARRAY: number;
        UUID_ARRAY: number;
        DATE_ARRAY: number;
        ENUM: number;
        ENUM_ARRAY: number;
        DECIMAL: number;
        DECIMAL_ARRAY: number;
        TIMESTAMP: number;
        TIMESTAMP_ARRAY: number;
        TIME: number;
        TIME_ARRAY: number;
    }>;
    static readonly COMPOSITE_TYPE: Readonly<{
        OBJECT_ARRAY: number;
        COLLECTION: number;
        MAP: number;
        NULL: number;
        COMPLEX_OBJECT: number;
    }>;
    /** Private methods */
    constructor(typeCode: any);
}
/**
 * Base class representing a non-primitive (composite) type of Ignite object.
 *
 * The class has no public constructor. Only subclasses may be instantiated.
 *
 * @hideconstructor
 * @extends ObjectType
 */
export declare class CompositeType extends ObjectType {
}
/**
 * Class representing a map type of Ignite object.
 *
 * It is described by COMPOSITE_TYPE.MAP {@link ObjectType.COMPOSITE_TYPE}
 * and one of {@link MapObjectType.MAP_SUBTYPE}.
 *
 * @extends CompositeType
 */
export declare class MapObjectType extends CompositeType {
    static readonly MAP_SUBTYPE: Readonly<{
        HASH_MAP: number;
        LINKED_HASH_MAP: number;
    }>;
    private _subType;
    private _keyType;
    private _valueType;
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
     * @param {MapObjectType.MAP_SUBTYPE} [mapSubType=MAP_SUBTYPE.HASH_MAP] - map subtype, one of the
     *   {@link MapObjectType.MAP_SUBTYPE} constants.
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
    constructor(mapSubType?: number, keyType?: any, valueType?: any);
}
/**
 * Class representing a collection type of Ignite object.
 *
 * It is described by COMPOSITE_TYPE.COLLECTION {@link ObjectType.COMPOSITE_TYPE}
 * and one of {@link CollectionObjectType.COLLECTION_SUBTYPE}.
 *
 * @extends CompositeType
 */
export declare class CollectionObjectType extends CompositeType {
    static readonly COLLECTION_SUBTYPE: Readonly<{
        USER_SET: number;
        USER_COL: number;
        ARRAY_LIST: number;
        LINKED_LIST: number;
        HASH_SET: number;
        LINKED_HASH_SET: number;
        SINGLETON_LIST: number;
    }>;
    private _subType;
    private _elementType;
    /**
     * Public constructor.
     *
     * Specifies a kind of collection
     * and optionally specifies a type of elements in the collection.
     *
     * If the type of elements is not specified then during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {CollectionObjectType.COLLECTION_SUBTYPE} collectionSubType - collection subtype, one of the
     *  {@link CollectionObjectType.COLLECTION_SUBTYPE} constants.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [elementType=null] - type of elements in the collection:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return {CollectionObjectType} - new CollectionObjectType instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(collectionSubType: any, elementType?: any);
    /** Private methods */
    /**
     * @ignore
     */
    static _isSet(subType: any): boolean;
    /**
     * @ignore
     */
    _isSet(): boolean;
}
/**
 * Class representing an array type of Ignite objects.
 *
 * It is described by COMPOSITE_TYPE.OBJECT_ARRAY {@link ObjectType.COMPOSITE_TYPE}.
 *
 * @extends CompositeType
 */
export declare class ObjectArrayType extends CompositeType {
    private _elementType;
    readonly elementType: any;
    /**
     * Public constructor.
     *
     * Optionally specifies a type of elements in the array.
     *
     * If the type of elements is not specified then during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [elementType=null] - type of the array element:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return {ObjectArrayType} - new ObjectArrayType instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(elementType?: any);
}
/**
 * Class representing a complex type of Ignite object.
 *
 * It is described by COMPOSITE_TYPE.COMPLEX_OBJECT {@link ObjectType.COMPOSITE_TYPE},
 * by a name of the complex type and by a JavaScript Object which is mapped to/from the Ignite complex type.
 *
 * @extends CompositeType
 */
export declare class ComplexObjectType extends CompositeType {
    private _template;
    private _objectConstructor;
    private _typeName?;
    private _fields;
    readonly typeName: string;
    /**
     * Public constructor.
     *
     * Specifies a JavaScript Object type which will be mapped to/from the complex type.
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
     * The name may be explicitely specified using optional typeName parameter in the constructor.
     *
     * @param {object} jsObject - instance of JavaScript Object which will be mapped to/from this complex type.
     * @param {string} [typeName] - name of the complex type.
     *
     * @return {ComplexObjectType} - new ComplexObjectType instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(jsObject: any, typeName?: any);
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
     *   - or null (means the type is not specified).
     *
     * @return {ComplexObjectType} - the same instance of the ComplexObjectType.
     *
     * @throws {IgniteClientError} if error.
     */
    setFieldType(fieldName: any, fieldType: any): this;
    /** Private methods */
    /**
     * @ignore
     */
    _getFieldType(fieldName: any): any;
}
