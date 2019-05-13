/**
 * Class representing a complex Ignite object in the binary form.
 *
 * It corresponds to COMPOSITE_TYPE.COMPLEX_OBJECT {@link ObjectType.COMPOSITE_TYPE},
 * has mandatory type Id, which corresponds to a name of the complex type,
 * and includes optional fields.
 *
 * An instance of the BinaryObject can be obtained/created by the following ways:
 *   - returned by the client when a complex object is received from Ignite cache
 * and is not deserialized to another JavaScript object.
 *   - created using the public constructor. Fields may be added to such an instance using setField() method.
 *   - created from a JavaScript object using static fromObject() method.
 */
export declare class BinaryObject {
    private _modified;
    private _fields;
    private _hasSchema;
    private _hasRawData;
    private _compactFooter;
    private _buffer;
    private _typeBuilder;
    private _schemaOffset;
    private _startPos;
    private _length;
    private _offsetType;
    /**
     * Creates an instance of the BinaryObject without any fields.
     *
     * Fields may be added later using setField() method.
     *
     * @param {string} typeName - name of the complex type to generate the type Id.
     *
     * @return {BinaryObject} - new BinaryObject instance.
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(typeName: any);
    /**
     * Creates an instance of the BinaryObject from the specified instance of JavaScript Object.
     *
     * All fields of the JavaScript Object instance with their values are added to the BinaryObject.
     * Fields may be added or removed later using setField() and removeField() methods.
     *
     * If complexObjectType parameter is specified, then the type Id is taken from it.
     * Otherwise, the type Id is generated from the name of the JavaScript Object.
     *
     * @async
     *
     * @param {object} jsObject - instance of JavaScript Object
     *   which adds and initializes the fields of the BinaryObject instance.
     * @param {ComplexObjectType} [complexObjectType] - instance of complex type definition
     *   which specifies non-standard mapping of the fields of the BinaryObject instance
     *   to/from the Ignite types.
     *
     * @return {BinaryObject} - new BinaryObject instance.
     *
     * @throws {IgniteClientError} if error.
     */
    static fromObject(jsObject: any, complexObjectType?: any): Promise<BinaryObject>;
    /**
     * Sets the new value of the specified field.
     * Adds the specified field, if it did not exist before.
     *
     * Optionally, specifies a type of the field.
     * If the type is not specified then during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {string} fieldName - name of the field.
     * @param {*} fieldValue - new value of the field.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [fieldType] - type of the field:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified.
     *
     * @return {BinaryObject} - the same instance of BinaryObject
     *
     * @throws {IgniteClientError} if error.
     */
    setField(fieldName: any, fieldValue: any, fieldType?: any): this;
    /**
     * Removes the specified field.
     * Does nothing if the field does not exist.
     *
     * @param {string} fieldName - name of the field.
     *
     * @return {BinaryObject} - the same instance of BinaryObject
     *
     * @throws {IgniteClientError} if error.
     */
    removeField(fieldName: any): this;
    /**
     * Checks if the specified field exists in this BinaryObject instance.
     *
     * @param {string} fieldName - name of the field.
     *
     * @return {boolean} - true if exists, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    hasField(fieldName: any): boolean;
    /**
     * Returns a value of the specified field.
     *
     * Optionally, specifies a type of the field.
     * If the type is not specified then the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @async
     *
     * @param {string} fieldName - name of the field.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [fieldType] - type of the field:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified.
     *
     * @return {*} - value of the field or JavaScript undefined if the field does not exist.
     *
     * @throws {IgniteClientError} if error.
     */
    getField(fieldName: any, fieldType?: any): Promise<any>;
    /**
     * Deserializes this BinaryObject instance into an instance of the specified complex object type.
     *
     * @async
     *
     * @param {ComplexObjectType} complexObjectType - instance of class representing complex object type.
     *
     * @return {object} - instance of the JavaScript object
     *   which corresponds to the specified complex object type.
     *
     * @throws {IgniteClientError} if error.
     */
    toObject(complexObjectType: any): Promise<any>;
    /**
     * Returns type name of this BinaryObject instance.
     *
     * @return {string} - type name.
     */
    getTypeName(): any;
    /**
     * Returns names of all fields of this BinaryObject instance.
     *
     * @return {Array<string>} - names of all fields.
     *
     * @throws {IgniteClientError} if error.
     */
    getFieldNames(): any;
    /** Private methods */
    /**
     * @ignore
     */
    static _isFlagSet(flags: any, flag: any): boolean;
    /**
     * @ignore
     */
    static _fromBuffer(communicator: any, buffer: any): Promise<BinaryObject>;
    /**
     * @ignore
     */
    _write(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _writeHeader(): void;
    /**
     * @ignore
     */
    _read(communicator: any): Promise<void>;
    /**
     * @ignore
     */
    _readHeader(communicator: any): Promise<void>;
}
/**
 * @ignore
 */
export declare class BinaryObjectField {
    private _name;
    private _id;
    private _value;
    private _type;
    private _typeCode;
    private _buffer;
    private _offset;
    private _communicator;
    private _length;
    constructor(name: any, value?: any, type?: any);
    readonly id: any;
    readonly typeCode: any;
    getValue(type?: any): Promise<any>;
    getOffsetType(headerStartPos: any): number;
    static _fromBuffer(communicator: any, buffer: any, offset: any, length: any, id: any): BinaryObjectField;
    _writeValue(communicator: any, buffer: any, expectedTypeCode: any): Promise<void>;
    _writeOffset(buffer: any, headerStartPos: any, offsetType: any): void;
}
