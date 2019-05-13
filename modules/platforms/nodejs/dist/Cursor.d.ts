/// <reference types="node" />
import { BinaryCommunicator, CacheEntry } from './internal';
/**
 * Class representing a cursor to obtain results of SQL and Scan query operations.
 *
 * The class has no public constructor. An instance of this class is obtained
 * via query() method of {@link CacheClient} objects.
 * One instance of this class returns results of one SQL or Scan query operation.
 *
 * @hideconstructor
 */
export declare class Cursor {
    protected _values: any;
    protected _valueIndex: number;
    protected _hasNext: boolean;
    protected _id: any;
    protected _communicator: BinaryCommunicator;
    protected _operation: any;
    protected _buffer: Buffer;
    protected _keyType: any;
    protected _valueType: any;
    /**
     * Returns one element (cache entry - key-value pair) from the query results.
     *
     * Every new call returns the next cache entry from the query results.
     * If the method returns null, no more entries are available.
     *
     * @async
     *
     * @return {Promise<CacheEntry>} - a cache entry (key-value pair).
     */
    getValue(): Promise<any>;
    /**
     * Checks if more elements are available in the query results.
     *
     * @return {boolean} - true if more cache entries are available, false otherwise.
     */
    hasMore(): boolean;
    /**
     * Returns all elements (cache entries - key-value pairs) from the query results.
     *
     * May be used instead of getValue() method if the number of returned entries
     * is relatively small and will not cause memory utilization issues.
     *
     * @async
     *
     * @return {Promise<Array<CacheEntry>>} - all cache entries (key-value pairs)
     *   returned by SQL or Scan query.
     */
    getAll(): Promise<any[]>;
    /**
     * Closes the cursor. Obtaining elements from the results is not possible after this.
     *
     * This method should be called if no more elements are needed.
     * It is not neccessary to call it if all elements have been already obtained.
     *
     * @async
     */
    close(): Promise<void>;
    /** Private methods */
    /**
     * @ignore
     */
    constructor(communicator: any, operation: any, buffer: any, keyType?: any, valueType?: any);
    /**
     * @ignore
     */
    _getNext(): Promise<void>;
    /**
     * @ignore
     */
    _getValues(): Promise<any>;
    /**
     * @ignore
     */
    _write(buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _readId(buffer: any): void;
    /**
     * @ignore
     */
    _readRow(buffer: any): Promise<CacheEntry | Array<any>>;
    /**
     * @ignore
     */
    _read(buffer: any): Promise<void>;
}
/**
 * Class representing a cursor to obtain results of SQL Fields query operation.
 *
 * The class has no public constructor. An instance of this class is obtained
 * via query() method of {@link CacheClient} objects.
 * One instance of this class returns results of one SQL Fields query operation.
 *
 * @hideconstructor
 * @extends Cursor
 */
export declare class SqlFieldsCursor extends Cursor {
    private _fieldNames;
    private _fieldTypes;
    private _fieldCount;
    /**
     * Returns one element (array with values of the fields) from the query results.
     *
     * Every new call returns the next element from the query results.
     * If the method returns null, no more elements are available.
     *
     * @async
     *
     * @return {Promise<Array<*>>} - array with values of the fields requested by the query.
     *
     */
    getValue(): Promise<any>;
    /**
     * Returns all elements (arrays with values of the fields) from the query results.
     *
     * May be used instead of getValue() method if the number of returned elements
     * is relatively small and will not cause memory utilization issues.
     *
     * @async
     *
     * @return {Promise<Array<Array<*>>>} - all results returned by SQL Fields query.
     *   Every element of the array is an array with values of the fields requested by the query.
     *
     */
    getAll(): Promise<any[]>;
    /**
     * Returns names of the fields which were requested in the SQL Fields query.
     *
     * Empty array is returned if "include field names" flag was false in the query.
     *
     * @return {Array<string>} - field names.
     *   The order of names corresponds to the order of field values returned in the results of the query.
     */
    getFieldNames(): any;
    /**
     * Specifies types of the fields returned by the SQL Fields query.
     *
     * By default, a type of every field is not specified that means during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {...ObjectType.PRIMITIVE_TYPE | CompositeType} fieldTypes - types of the returned fields.
     *   The order of types must correspond the order of field values returned in the results of the query.
     *   A type of every field can be:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (means the type is not specified)
     *
     * @return {SqlFieldsCursor} - the same instance of the SqlFieldsCursor.
     */
    setFieldTypes(...fieldTypes: any[]): this;
    /** Private methods */
    /**
     * @ignore
     */
    constructor(communicator: any, buffer: any);
    /**
     * @ignore
     */
    _readFieldNames(buffer: any, includeFieldNames: any): Promise<void>;
    /**
     * @ignore
     */
    _readRow(buffer: any): Promise<any[]>;
}
