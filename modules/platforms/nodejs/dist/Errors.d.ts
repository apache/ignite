/**
 * Base Ignite client error class.
 */
export declare class IgniteClientError extends Error {
    constructor(message: any);
    /**
     * Ignite client does not support one of the specified or received data types.
     * @ignore
     */
    static unsupportedTypeError(type: any): IgniteClientError;
    /**
     * The real type of data is not equal to the specified one.
     * @ignore
     */
    static typeCastError(fromType: any, toType: any): IgniteClientError;
    /**
     * The real value can not be cast to the specified type.
     * @ignore
     */
    static valueCastError(value: any, toType: any): IgniteClientError;
    /**
     * An illegal or inappropriate argument has been passed to the API method.
     * @ignore
     */
    static illegalArgumentError(message: any): IgniteClientError;
    /**
     * Ignite client internal error.
     * @ignore
     */
    static internalError(message?: any): IgniteClientError;
    /**
     * Serialization/deserialization errors.
     * @ignore
     */
    static serializationError(serialize: any, message?: any): IgniteClientError;
    /**
     * EnumItem serialization/deserialization errors.
     * @ignore
     */
    static enumSerializationError(serialize: any, message?: any): IgniteClientError;
}
/**
 * Ignite server returns error for the requested operation.
 * @extends IgniteClientError
 */
export declare class OperationError extends IgniteClientError {
    constructor(message: any);
}
/**
 * Ignite client is not in an appropriate state for the requested operation.
 * @extends IgniteClientError
 */
export declare class IllegalStateError extends IgniteClientError {
    constructor(message?: any);
}
/**
 * The requested operation is not completed due to the connection lost.
 * @extends IgniteClientError
 */
export declare class LostConnectionError extends IgniteClientError {
    constructor(message?: any);
}
