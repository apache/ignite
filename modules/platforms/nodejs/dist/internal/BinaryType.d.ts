export declare class BinaryType {
    private _name;
    private _id;
    private _fields;
    private _schemas;
    private _isEnum;
    private _enumValues;
    constructor(name?: string);
    readonly id: number;
    readonly name: string;
    readonly fields: any[];
    getField(fieldId: any): any;
    hasField(fieldId: any): boolean;
    removeField(fieldId: any): boolean;
    setField(field: any): void;
    hasSchema(schemaId: any): boolean;
    addSchema(schema: any): void;
    getSchema(schemaId: any): any;
    merge(binaryType: any, binarySchema: any): void;
    clone(): BinaryType;
    isValid(): boolean;
    static _calculateId(name: any): number;
    _write(buffer: any): Promise<void>;
    _writeEnum(buffer: any): Promise<void>;
    _read(buffer: any): Promise<void>;
    _readEnum(buffer: any): Promise<void>;
}
export declare class BinaryField {
    private _id;
    private _name;
    private _typeCode;
    constructor(name: any, typeCode: any);
    readonly id: number;
    readonly name: string;
    readonly typeCode: number;
    isValid(): boolean;
    static _calculateId(name: any): number;
    _write(buffer: any): Promise<void>;
    _read(buffer: any): Promise<void>;
}
export declare class BinaryTypeBuilder {
    private _type;
    private _schema;
    private _fromStorage;
    readonly schema: any;
    static fromTypeName(typeName: any): BinaryTypeBuilder;
    static fromTypeId(communicator: any, typeId: any, schemaId: any): Promise<BinaryTypeBuilder>;
    static fromObject(jsObject: any, complexObjectType?: any): BinaryTypeBuilder;
    static fromComplexObjectType(complexObjectType: any, jsObject: any): BinaryTypeBuilder;
    getTypeId(): any;
    getTypeName(): any;
    getSchemaId(): any;
    getFields(): any;
    getField(fieldId: any): any;
    setField(fieldName: any, fieldTypeCode?: any): void;
    removeField(fieldName: any): void;
    finalize(communicator: any): Promise<void>;
    constructor();
    _fromComplexObjectType(complexObjectType: any, jsObject: any): void;
    _init(typeName: any): void;
    _beforeModify(): void;
    _setFields(complexObjectType: any, objectTemplate: any, jsObject: any): void;
}
