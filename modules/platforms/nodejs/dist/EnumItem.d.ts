/**
 * Class representing an item of Ignite enum type.
 *
 * The item is defined by:
 *   - type Id (mandatory) - Id of the Ignite enum type.
 *   - ordinal (optional) - ordinal of the item in the Ignite enum type.
 *   - name (optional) - name of the item (field name in the Ignite enum type).
 *   - value (optional) - value of the item.
 * Usually, at least one from the optional ordinal, name or value must be specified
 * in order to use an instance of this class in Ignite operations.
 *
 * To distinguish one item from another, the Ignite client analyzes the optional fields in the following order:
 * ordinal, name, value.
 */
export declare class EnumItem {
    private _typeId;
    private _ordinal;
    private _name;
    private _value;
    /**
     * Public constructor.
     *
     * @param {number} typeId - Id of the Ignite enum type.
     *
     * @return {EnumItem} - new EnumItem instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(typeId: any);
    /**
     * Returns Id of the Ignite enum type.
     *
     * @return {number} - Id of the enum type.
     */
    getTypeId(): number;
    /**
     * Updates Id of the Ignite enum type.
     *
     * @param {number} typeId - new Id of the Ignite enum type.
     *
     * @return {EnumItem} - the same instance of EnumItem
     *
     * @throws {IgniteClientError} if error.
     */
    setTypeId(typeId: any): this;
    /**
     * Returns ordinal of the item in the Ignite enum type
     * or null if ordinal is not set.
     *
     * @return {number} - ordinal of the item in the Ignite enum type.
     */
    getOrdinal(): number;
    /**
     * Sets or updates ordinal of the item in the Ignite enum type.
     *
     * @param {number} ordinal - ordinal of the item in the Ignite enum type.
     *
     * @return {EnumItem} - the same instance of EnumItem
     *
     * @throws {IgniteClientError} if error.
     */
    setOrdinal(ordinal: any): this;
    /**
     * Returns name of the item
     * or null if name is not set.
     *
     * @return {string} - name of the item.
     */
    getName(): string;
    /**
     * Sets or updates name of the item.
     *
     * @param {string} name - name of the item.
     *
     * @return {EnumItem} - the same instance of EnumItem
     *
     * @throws {IgniteClientError} if error.
     */
    setName(name: any): this;
    /**
     * Returns value of the item
     * or null if value is not set.
     *
     * @return {number} - value of the item.
     */
    getValue(): number;
    /**
     * Sets or updates value of the item.
     *
     * @param {number} value - value of the item.
     *
     * @return {EnumItem} - the same instance of EnumItem
     *
     * @throws {IgniteClientError} if error.
     */
    setValue(value: any): this;
    /** Private methods */
    /**
     * @ignore
     */
    _write(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _read(communicator: any, buffer: any): Promise<void>;
    /**
     * @ignore
     */
    _getType(communicator: any, typeId: any): Promise<any>;
}
