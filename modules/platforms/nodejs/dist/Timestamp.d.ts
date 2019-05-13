/**
 * Class representing an Ignite timestamp type.
 *
 * The timestamp extends the standard JavaScript {@link Date} Object and consists of:
 *   - time  - the number of milliseconds since January 1, 1970, 00:00:00 UTC,
 *     methods of the JavaScript {@link Date} Object can be used to operate with the time.
 *   - nanoseconds - fraction of the last millisecond in the range from 0 to 999999 nanoseconds,
 *     this class specifies additional methods to operate with the nanoseconds.
 * @extends Date
 */
export declare class Timestamp extends Date {
    private _nanos;
    /**
     * Public constructor.
     *
     * @param {number} time - integer value representing the number of milliseconds since January 1, 1970, 00:00:00 UTC.
     * @param {number} nanos - integer value representing the nanoseconds of the last millisecond,
     *                         should be in the range from 0 to 999999.
     *
     * @return {Timestamp} - new Timestamp instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(time: any, nanos: any);
    /**
     * Returns the nanoseconds of the last millisecond from the timestamp.
     *
     * @return {number} - nanoseconds of the last millisecond.
     */
    getNanos(): number;
    /**
     * Updates the nanoseconds of the last millisecond in the timestamp.
     *
     * @param {number} nanos - new value for the nanoseconds of the last millisecond,
     *                         should be in the range from 0 to 999999.
     *
     * @return {Timestamp} - the same instance of Timestamp
     *
     * @throws {IgniteClientError} if error.
     */
    setNanos(nanos: any): this;
}
