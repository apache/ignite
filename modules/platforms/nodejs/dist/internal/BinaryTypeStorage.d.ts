import { BinaryType } from '../internal';
export declare class BinaryTypeStorage {
    private _communicator;
    private _types;
    private static _complexObjectTypes;
    constructor(communicator: any);
    static getByComplexObjectType(complexObjectType: any): any;
    static setByComplexObjectType(complexObjectType: any, type: any, schema: any): void;
    static readonly complexObjectTypes: Map<any, any>;
    addType(binaryType: any, binarySchema: any): Promise<void>;
    getType(typeId: any, schemaId?: any): Promise<any>;
    /** Private methods */
    _getBinaryType(typeId: any): Promise<BinaryType>;
    _putBinaryType(binaryType: any): Promise<void>;
}
