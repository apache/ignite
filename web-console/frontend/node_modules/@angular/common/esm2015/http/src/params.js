/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * A codec for encoding and decoding parameters in URLs.
 *
 * Used by `HttpParams`.
 *
 * \@publicApi
 *
 * @record
 */
export function HttpParameterCodec() { }
if (false) {
    /**
     * @param {?} key
     * @return {?}
     */
    HttpParameterCodec.prototype.encodeKey = function (key) { };
    /**
     * @param {?} value
     * @return {?}
     */
    HttpParameterCodec.prototype.encodeValue = function (value) { };
    /**
     * @param {?} key
     * @return {?}
     */
    HttpParameterCodec.prototype.decodeKey = function (key) { };
    /**
     * @param {?} value
     * @return {?}
     */
    HttpParameterCodec.prototype.decodeValue = function (value) { };
}
/**
 * Provides encoding and decoding of URL parameter and query-string values.
 *
 * Serializes and parses URL parameter keys and values to encode and decode them.
 * If you pass URL query parameters without encoding,
 * the query parameters can be misinterpreted at the receiving end.
 *
 *
 * \@publicApi
 */
export class HttpUrlEncodingCodec {
    /**
     * Encodes a key name for a URL parameter or query-string.
     * @param {?} key The key name.
     * @return {?} The encoded key name.
     */
    encodeKey(key) { return standardEncoding(key); }
    /**
     * Encodes the value of a URL parameter or query-string.
     * @param {?} value The value.
     * @return {?} The encoded value.
     */
    encodeValue(value) { return standardEncoding(value); }
    /**
     * Decodes an encoded URL parameter or query-string key.
     * @param {?} key The encoded key name.
     * @return {?} The decoded key name.
     */
    decodeKey(key) { return decodeURIComponent(key); }
    /**
     * Decodes an encoded URL parameter or query-string value.
     * @param {?} value The encoded value.
     * @return {?} The decoded value.
     */
    decodeValue(value) { return decodeURIComponent(value); }
}
/**
 * @param {?} rawParams
 * @param {?} codec
 * @return {?}
 */
function paramParser(rawParams, codec) {
    /** @type {?} */
    const map = new Map();
    if (rawParams.length > 0) {
        /** @type {?} */
        const params = rawParams.split('&');
        params.forEach((/**
         * @param {?} param
         * @return {?}
         */
        (param) => {
            /** @type {?} */
            const eqIdx = param.indexOf('=');
            const [key, val] = eqIdx == -1 ?
                [codec.decodeKey(param), ''] :
                [codec.decodeKey(param.slice(0, eqIdx)), codec.decodeValue(param.slice(eqIdx + 1))];
            /** @type {?} */
            const list = map.get(key) || [];
            list.push(val);
            map.set(key, list);
        }));
    }
    return map;
}
/**
 * @param {?} v
 * @return {?}
 */
function standardEncoding(v) {
    return encodeURIComponent(v)
        .replace(/%40/gi, '@')
        .replace(/%3A/gi, ':')
        .replace(/%24/gi, '$')
        .replace(/%2C/gi, ',')
        .replace(/%3B/gi, ';')
        .replace(/%2B/gi, '+')
        .replace(/%3D/gi, '=')
        .replace(/%3F/gi, '?')
        .replace(/%2F/gi, '/');
}
/**
 * @record
 */
function Update() { }
if (false) {
    /** @type {?} */
    Update.prototype.param;
    /** @type {?|undefined} */
    Update.prototype.value;
    /** @type {?} */
    Update.prototype.op;
}
/**
 * Options used to construct an `HttpParams` instance.
 *
 * \@publicApi
 * @record
 */
export function HttpParamsOptions() { }
if (false) {
    /**
     * String representation of the HTTP parameters in URL-query-string format.
     * Mutually exclusive with `fromObject`.
     * @type {?|undefined}
     */
    HttpParamsOptions.prototype.fromString;
    /**
     * Object map of the HTTP parameters. Mutually exclusive with `fromString`.
     * @type {?|undefined}
     */
    HttpParamsOptions.prototype.fromObject;
    /**
     * Encoding codec used to parse and serialize the parameters.
     * @type {?|undefined}
     */
    HttpParamsOptions.prototype.encoder;
}
/**
 * An HTTP request/response body that represents serialized parameters,
 * per the MIME type `application/x-www-form-urlencoded`.
 *
 * This class is immutable; all mutation operations return a new instance.
 *
 * \@publicApi
 */
export class HttpParams {
    /**
     * @param {?=} options
     */
    constructor(options = (/** @type {?} */ ({}))) {
        this.updates = null;
        this.cloneFrom = null;
        this.encoder = options.encoder || new HttpUrlEncodingCodec();
        if (!!options.fromString) {
            if (!!options.fromObject) {
                throw new Error(`Cannot specify both fromString and fromObject.`);
            }
            this.map = paramParser(options.fromString, this.encoder);
        }
        else if (!!options.fromObject) {
            this.map = new Map();
            Object.keys(options.fromObject).forEach((/**
             * @param {?} key
             * @return {?}
             */
            key => {
                /** @type {?} */
                const value = ((/** @type {?} */ (options.fromObject)))[key];
                (/** @type {?} */ (this.map)).set(key, Array.isArray(value) ? value : [value]);
            }));
        }
        else {
            this.map = null;
        }
    }
    /**
     * Reports whether the body includes one or more values for a given parameter.
     * @param {?} param The parameter name.
     * @return {?} True if the parameter has one or more values,
     * false if it has no value or is not present.
     */
    has(param) {
        this.init();
        return (/** @type {?} */ (this.map)).has(param);
    }
    /**
     * Retrieves the first value for a parameter.
     * @param {?} param The parameter name.
     * @return {?} The first value of the given parameter,
     * or `null` if the parameter is not present.
     */
    get(param) {
        this.init();
        /** @type {?} */
        const res = (/** @type {?} */ (this.map)).get(param);
        return !!res ? res[0] : null;
    }
    /**
     * Retrieves all values for a  parameter.
     * @param {?} param The parameter name.
     * @return {?} All values in a string array,
     * or `null` if the parameter not present.
     */
    getAll(param) {
        this.init();
        return (/** @type {?} */ (this.map)).get(param) || null;
    }
    /**
     * Retrieves all the parameters for this body.
     * @return {?} The parameter names in a string array.
     */
    keys() {
        this.init();
        return Array.from((/** @type {?} */ (this.map)).keys());
    }
    /**
     * Appends a new value to existing values for a parameter.
     * @param {?} param The parameter name.
     * @param {?} value The new value to add.
     * @return {?} A new body with the appended value.
     */
    append(param, value) { return this.clone({ param, value, op: 'a' }); }
    /**
     * Replaces the value for a parameter.
     * @param {?} param The parameter name.
     * @param {?} value The new value.
     * @return {?} A new body with the new value.
     */
    set(param, value) { return this.clone({ param, value, op: 's' }); }
    /**
     * Removes a given value or all values from a parameter.
     * @param {?} param The parameter name.
     * @param {?=} value The value to remove, if provided.
     * @return {?} A new body with the given value removed, or with all values
     * removed if no value is specified.
     */
    delete(param, value) { return this.clone({ param, value, op: 'd' }); }
    /**
     * Serializes the body to an encoded string, where key-value pairs (separated by `=`) are
     * separated by `&`s.
     * @return {?}
     */
    toString() {
        this.init();
        return this.keys()
            .map((/**
         * @param {?} key
         * @return {?}
         */
        key => {
            /** @type {?} */
            const eKey = this.encoder.encodeKey(key);
            return (/** @type {?} */ ((/** @type {?} */ (this.map)).get(key))).map((/**
             * @param {?} value
             * @return {?}
             */
            value => eKey + '=' + this.encoder.encodeValue(value)))
                .join('&');
        }))
            .join('&');
    }
    /**
     * @private
     * @param {?} update
     * @return {?}
     */
    clone(update) {
        /** @type {?} */
        const clone = new HttpParams((/** @type {?} */ ({ encoder: this.encoder })));
        clone.cloneFrom = this.cloneFrom || this;
        clone.updates = (this.updates || []).concat([update]);
        return clone;
    }
    /**
     * @private
     * @return {?}
     */
    init() {
        if (this.map === null) {
            this.map = new Map();
        }
        if (this.cloneFrom !== null) {
            this.cloneFrom.init();
            this.cloneFrom.keys().forEach((/**
             * @param {?} key
             * @return {?}
             */
            key => (/** @type {?} */ (this.map)).set(key, (/** @type {?} */ ((/** @type {?} */ ((/** @type {?} */ (this.cloneFrom)).map)).get(key))))));
            (/** @type {?} */ (this.updates)).forEach((/**
             * @param {?} update
             * @return {?}
             */
            update => {
                switch (update.op) {
                    case 'a':
                    case 's':
                        /** @type {?} */
                        const base = (update.op === 'a' ? (/** @type {?} */ (this.map)).get(update.param) : undefined) || [];
                        base.push((/** @type {?} */ (update.value)));
                        (/** @type {?} */ (this.map)).set(update.param, base);
                        break;
                    case 'd':
                        if (update.value !== undefined) {
                            /** @type {?} */
                            let base = (/** @type {?} */ (this.map)).get(update.param) || [];
                            /** @type {?} */
                            const idx = base.indexOf(update.value);
                            if (idx !== -1) {
                                base.splice(idx, 1);
                            }
                            if (base.length > 0) {
                                (/** @type {?} */ (this.map)).set(update.param, base);
                            }
                            else {
                                (/** @type {?} */ (this.map)).delete(update.param);
                            }
                        }
                        else {
                            (/** @type {?} */ (this.map)).delete(update.param);
                            break;
                        }
                }
            }));
            this.cloneFrom = this.updates = null;
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    HttpParams.prototype.map;
    /**
     * @type {?}
     * @private
     */
    HttpParams.prototype.encoder;
    /**
     * @type {?}
     * @private
     */
    HttpParams.prototype.updates;
    /**
     * @type {?}
     * @private
     */
    HttpParams.prototype.cloneFrom;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGFyYW1zLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL2h0dHAvc3JjL3BhcmFtcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQWVBLHdDQU1DOzs7Ozs7SUFMQyw0REFBK0I7Ozs7O0lBQy9CLGdFQUFtQzs7Ozs7SUFFbkMsNERBQStCOzs7OztJQUMvQixnRUFBbUM7Ozs7Ozs7Ozs7OztBQWFyQyxNQUFNLE9BQU8sb0JBQW9COzs7Ozs7SUFNL0IsU0FBUyxDQUFDLEdBQVcsSUFBWSxPQUFPLGdCQUFnQixDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQzs7Ozs7O0lBT2hFLFdBQVcsQ0FBQyxLQUFhLElBQVksT0FBTyxnQkFBZ0IsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7OztJQU90RSxTQUFTLENBQUMsR0FBVyxJQUFZLE9BQU8sa0JBQWtCLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFPbEUsV0FBVyxDQUFDLEtBQWEsSUFBSSxPQUFPLGtCQUFrQixDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQztDQUNqRTs7Ozs7O0FBR0QsU0FBUyxXQUFXLENBQUMsU0FBaUIsRUFBRSxLQUF5Qjs7VUFDekQsR0FBRyxHQUFHLElBQUksR0FBRyxFQUFvQjtJQUN2QyxJQUFJLFNBQVMsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFOztjQUNsQixNQUFNLEdBQWEsU0FBUyxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUM7UUFDN0MsTUFBTSxDQUFDLE9BQU87Ozs7UUFBQyxDQUFDLEtBQWEsRUFBRSxFQUFFOztrQkFDekIsS0FBSyxHQUFHLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDO2tCQUMxQixDQUFDLEdBQUcsRUFBRSxHQUFHLENBQUMsR0FBYSxLQUFLLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQztn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OztnQ0FDMUIsSUFBSSxHQUFHLG1CQUFBLElBQUksQ0FBQyxHQUFHLEVBQUUsQ0FBQyxHQUFHLENBQUMsTUFBTSxDQUFDLEtBQUssQ0FBQyxJQUFJLEVBQUU7O2tDQUN2QyxHQUFHLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDOzRCQUN0QyxJQUFJLEdBQUcsS0FBSyxDQUFDLENBQUMsRUFBRTtnQ0FDZCxJQUFJLENBQUMsTUFBTSxDQUFDLEdBQUcsRUFBRSxDQUFDLENBQUMsQ0FBQzs2QkFDckI7NEJBQ0QsSUFBSSxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRTtnQ0FDbkIsbUJBQUEsSUFBSSxDQUFDLEdBQUcsRUFBRSxDQUFDLEdBQUcsQ0FBQyxNQUFNLENBQUMsS0FBSyxFQUFFLElBQUksQ0FBQyxDQUFDOzZCQUNwQztpQ0FBTTtnQ0FDTCxtQkFBQSxJQUFJLENBQUMsR0FBRyxFQUFFLENBQUMsTUFBTSxDQUFDLE1BQU0sQ0FBQyxLQUFLLENBQUMsQ0FBQzs2QkFDakM7eUJBQ0Y7NkJBQU07NEJBQ0wsbUJBQUEsSUFBSSxDQUFDLEdBQUcsRUFBRSxDQUFDLE1BQU0sQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLENBQUM7NEJBQ2hDLE1BQU07eUJBQ1A7aUJBQ0o7WUFDSCxDQUFDLEVBQUMsQ0FBQztZQUNILElBQUksQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDLE9BQU8sR0FBRyxJQUFJLENBQUM7U0FDdEM7SUFDSCxDQUFDO0NBQ0Y7Ozs7OztJQXJKQyx5QkFBd0M7Ozs7O0lBQ3hDLDZCQUFvQzs7Ozs7SUFDcEMsNkJBQXNDOzs7OztJQUN0QywrQkFBMEMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8qKlxuICogQSBjb2RlYyBmb3IgZW5jb2RpbmcgYW5kIGRlY29kaW5nIHBhcmFtZXRlcnMgaW4gVVJMcy5cbiAqXG4gKiBVc2VkIGJ5IGBIdHRwUGFyYW1zYC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKiovXG5leHBvcnQgaW50ZXJmYWNlIEh0dHBQYXJhbWV0ZXJDb2RlYyB7XG4gIGVuY29kZUtleShrZXk6IHN0cmluZyk6IHN0cmluZztcbiAgZW5jb2RlVmFsdWUodmFsdWU6IHN0cmluZyk6IHN0cmluZztcblxuICBkZWNvZGVLZXkoa2V5OiBzdHJpbmcpOiBzdHJpbmc7XG4gIGRlY29kZVZhbHVlKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmc7XG59XG5cbi8qKlxuICogUHJvdmlkZXMgZW5jb2RpbmcgYW5kIGRlY29kaW5nIG9mIFVSTCBwYXJhbWV0ZXIgYW5kIHF1ZXJ5LXN0cmluZyB2YWx1ZXMuXG4gKlxuICogU2VyaWFsaXplcyBhbmQgcGFyc2VzIFVSTCBwYXJhbWV0ZXIga2V5cyBhbmQgdmFsdWVzIHRvIGVuY29kZSBhbmQgZGVjb2RlIHRoZW0uXG4gKiBJZiB5b3UgcGFzcyBVUkwgcXVlcnkgcGFyYW1ldGVycyB3aXRob3V0IGVuY29kaW5nLFxuICogdGhlIHF1ZXJ5IHBhcmFtZXRlcnMgY2FuIGJlIG1pc2ludGVycHJldGVkIGF0IHRoZSByZWNlaXZpbmcgZW5kLlxuICpcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBIdHRwVXJsRW5jb2RpbmdDb2RlYyBpbXBsZW1lbnRzIEh0dHBQYXJhbWV0ZXJDb2RlYyB7XG4gIC8qKlxuICAgKiBFbmNvZGVzIGEga2V5IG5hbWUgZm9yIGEgVVJMIHBhcmFtZXRlciBvciBxdWVyeS1zdHJpbmcuXG4gICAqIEBwYXJhbSBrZXkgVGhlIGtleSBuYW1lLlxuICAgKiBAcmV0dXJucyBUaGUgZW5jb2RlZCBrZXkgbmFtZS5cbiAgICovXG4gIGVuY29kZUtleShrZXk6IHN0cmluZyk6IHN0cmluZyB7IHJldHVybiBzdGFuZGFyZEVuY29kaW5nKGtleSk7IH1cblxuICAvKipcbiAgICogRW5jb2RlcyB0aGUgdmFsdWUgb2YgYSBVUkwgcGFyYW1ldGVyIG9yIHF1ZXJ5LXN0cmluZy5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSB2YWx1ZS5cbiAgICogQHJldHVybnMgVGhlIGVuY29kZWQgdmFsdWUuXG4gICAqL1xuICBlbmNvZGVWYWx1ZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nIHsgcmV0dXJuIHN0YW5kYXJkRW5jb2RpbmcodmFsdWUpOyB9XG5cbiAgLyoqXG4gICAqIERlY29kZXMgYW4gZW5jb2RlZCBVUkwgcGFyYW1ldGVyIG9yIHF1ZXJ5LXN0cmluZyBrZXkuXG4gICAqIEBwYXJhbSBrZXkgVGhlIGVuY29kZWQga2V5IG5hbWUuXG4gICAqIEByZXR1cm5zIFRoZSBkZWNvZGVkIGtleSBuYW1lLlxuICAgKi9cbiAgZGVjb2RlS2V5KGtleTogc3RyaW5nKTogc3RyaW5nIHsgcmV0dXJuIGRlY29kZVVSSUNvbXBvbmVudChrZXkpOyB9XG5cbiAgLyoqXG4gICAqIERlY29kZXMgYW4gZW5jb2RlZCBVUkwgcGFyYW1ldGVyIG9yIHF1ZXJ5LXN0cmluZyB2YWx1ZS5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSBlbmNvZGVkIHZhbHVlLlxuICAgKiBAcmV0dXJucyBUaGUgZGVjb2RlZCB2YWx1ZS5cbiAgICovXG4gIGRlY29kZVZhbHVlKHZhbHVlOiBzdHJpbmcpIHsgcmV0dXJuIGRlY29kZVVSSUNvbXBvbmVudCh2YWx1ZSk7IH1cbn1cblxuXG5mdW5jdGlvbiBwYXJhbVBhcnNlcihyYXdQYXJhbXM6IHN0cmluZywgY29kZWM6IEh0dHBQYXJhbWV0ZXJDb2RlYyk6IE1hcDxzdHJpbmcsIHN0cmluZ1tdPiB7XG4gIGNvbnN0IG1hcCA9IG5ldyBNYXA8c3RyaW5nLCBzdHJpbmdbXT4oKTtcbiAgaWYgKHJhd1BhcmFtcy5sZW5ndGggPiAwKSB7XG4gICAgY29uc3QgcGFyYW1zOiBzdHJpbmdbXSA9IHJhd1BhcmFtcy5zcGxpdCgnJicpO1xuICAgIHBhcmFtcy5mb3JFYWNoKChwYXJhbTogc3RyaW5nKSA9PiB7XG4gICAgICBjb25zdCBlcUlkeCA9IHBhcmFtLmluZGV4T2YoJz0nKTtcbiAgICAgIGNvbnN0IFtrZXksIHZhbF06IHN0cmluZ1tdID0gZXFJZHggPT0gLTEgP1xuICAgICAgICAgIFtjb2RlYy5kZWNvZGVLZXkocGFyYW0pLCAnJ10gOlxuICAgICAgICAgIFtjb2RlYy5kZWNvZGVLZXkocGFyYW0uc2xpY2UoMCwgZXFJZHgpKSwgY29kZWMuZGVjb2RlVmFsdWUocGFyYW0uc2xpY2UoZXFJZHggKyAxKSldO1xuICAgICAgY29uc3QgbGlzdCA9IG1hcC5nZXQoa2V5KSB8fCBbXTtcbiAgICAgIGxpc3QucHVzaCh2YWwpO1xuICAgICAgbWFwLnNldChrZXksIGxpc3QpO1xuICAgIH0pO1xuICB9XG4gIHJldHVybiBtYXA7XG59XG5mdW5jdGlvbiBzdGFuZGFyZEVuY29kaW5nKHY6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBlbmNvZGVVUklDb21wb25lbnQodilcbiAgICAgIC5yZXBsYWNlKC8lNDAvZ2ksICdAJylcbiAgICAgIC5yZXBsYWNlKC8lM0EvZ2ksICc6JylcbiAgICAgIC5yZXBsYWNlKC8lMjQvZ2ksICckJylcbiAgICAgIC5yZXBsYWNlKC8lMkMvZ2ksICcsJylcbiAgICAgIC5yZXBsYWNlKC8lM0IvZ2ksICc7JylcbiAgICAgIC5yZXBsYWNlKC8lMkIvZ2ksICcrJylcbiAgICAgIC5yZXBsYWNlKC8lM0QvZ2ksICc9JylcbiAgICAgIC5yZXBsYWNlKC8lM0YvZ2ksICc/JylcbiAgICAgIC5yZXBsYWNlKC8lMkYvZ2ksICcvJyk7XG59XG5cbmludGVyZmFjZSBVcGRhdGUge1xuICBwYXJhbTogc3RyaW5nO1xuICB2YWx1ZT86IHN0cmluZztcbiAgb3A6ICdhJ3wnZCd8J3MnO1xufVxuXG4vKiogT3B0aW9ucyB1c2VkIHRvIGNvbnN0cnVjdCBhbiBgSHR0cFBhcmFtc2AgaW5zdGFuY2UuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgaW50ZXJmYWNlIEh0dHBQYXJhbXNPcHRpb25zIHtcbiAgLyoqXG4gICAqIFN0cmluZyByZXByZXNlbnRhdGlvbiBvZiB0aGUgSFRUUCBwYXJhbWV0ZXJzIGluIFVSTC1xdWVyeS1zdHJpbmcgZm9ybWF0LlxuICAgKiBNdXR1YWxseSBleGNsdXNpdmUgd2l0aCBgZnJvbU9iamVjdGAuXG4gICAqL1xuICBmcm9tU3RyaW5nPzogc3RyaW5nO1xuXG4gIC8qKiBPYmplY3QgbWFwIG9mIHRoZSBIVFRQIHBhcmFtZXRlcnMuIE11dHVhbGx5IGV4Y2x1c2l2ZSB3aXRoIGBmcm9tU3RyaW5nYC4gKi9cbiAgZnJvbU9iamVjdD86IHtbcGFyYW06IHN0cmluZ106IHN0cmluZyB8IFJlYWRvbmx5QXJyYXk8c3RyaW5nPn07XG5cbiAgLyoqIEVuY29kaW5nIGNvZGVjIHVzZWQgdG8gcGFyc2UgYW5kIHNlcmlhbGl6ZSB0aGUgcGFyYW1ldGVycy4gKi9cbiAgZW5jb2Rlcj86IEh0dHBQYXJhbWV0ZXJDb2RlYztcbn1cblxuLyoqXG4gKiBBbiBIVFRQIHJlcXVlc3QvcmVzcG9uc2UgYm9keSB0aGF0IHJlcHJlc2VudHMgc2VyaWFsaXplZCBwYXJhbWV0ZXJzLFxuICogcGVyIHRoZSBNSU1FIHR5cGUgYGFwcGxpY2F0aW9uL3gtd3d3LWZvcm0tdXJsZW5jb2RlZGAuXG4gKlxuICogVGhpcyBjbGFzcyBpcyBpbW11dGFibGU7IGFsbCBtdXRhdGlvbiBvcGVyYXRpb25zIHJldHVybiBhIG5ldyBpbnN0YW5jZS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBIdHRwUGFyYW1zIHtcbiAgcHJpdmF0ZSBtYXA6IE1hcDxzdHJpbmcsIHN0cmluZ1tdPnxudWxsO1xuICBwcml2YXRlIGVuY29kZXI6IEh0dHBQYXJhbWV0ZXJDb2RlYztcbiAgcHJpdmF0ZSB1cGRhdGVzOiBVcGRhdGVbXXxudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBjbG9uZUZyb206IEh0dHBQYXJhbXN8bnVsbCA9IG51bGw7XG5cbiAgY29uc3RydWN0b3Iob3B0aW9uczogSHR0cFBhcmFtc09wdGlvbnMgPSB7fSBhcyBIdHRwUGFyYW1zT3B0aW9ucykge1xuICAgIHRoaXMuZW5jb2RlciA9IG9wdGlvbnMuZW5jb2RlciB8fCBuZXcgSHR0cFVybEVuY29kaW5nQ29kZWMoKTtcbiAgICBpZiAoISFvcHRpb25zLmZyb21TdHJpbmcpIHtcbiAgICAgIGlmICghIW9wdGlvbnMuZnJvbU9iamVjdCkge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYENhbm5vdCBzcGVjaWZ5IGJvdGggZnJvbVN0cmluZyBhbmQgZnJvbU9iamVjdC5gKTtcbiAgICAgIH1cbiAgICAgIHRoaXMubWFwID0gcGFyYW1QYXJzZXIob3B0aW9ucy5mcm9tU3RyaW5nLCB0aGlzLmVuY29kZXIpO1xuICAgIH0gZWxzZSBpZiAoISFvcHRpb25zLmZyb21PYmplY3QpIHtcbiAgICAgIHRoaXMubWFwID0gbmV3IE1hcDxzdHJpbmcsIHN0cmluZ1tdPigpO1xuICAgICAgT2JqZWN0LmtleXMob3B0aW9ucy5mcm9tT2JqZWN0KS5mb3JFYWNoKGtleSA9PiB7XG4gICAgICAgIGNvbnN0IHZhbHVlID0gKG9wdGlvbnMuZnJvbU9iamVjdCBhcyBhbnkpW2tleV07XG4gICAgICAgIHRoaXMubWFwICEuc2V0KGtleSwgQXJyYXkuaXNBcnJheSh2YWx1ZSkgPyB2YWx1ZSA6IFt2YWx1ZV0pO1xuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMubWFwID0gbnVsbDtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogUmVwb3J0cyB3aGV0aGVyIHRoZSBib2R5IGluY2x1ZGVzIG9uZSBvciBtb3JlIHZhbHVlcyBmb3IgYSBnaXZlbiBwYXJhbWV0ZXIuXG4gICAqIEBwYXJhbSBwYXJhbSBUaGUgcGFyYW1ldGVyIG5hbWUuXG4gICAqIEByZXR1cm5zIFRydWUgaWYgdGhlIHBhcmFtZXRlciBoYXMgb25lIG9yIG1vcmUgdmFsdWVzLFxuICAgKiBmYWxzZSBpZiBpdCBoYXMgbm8gdmFsdWUgb3IgaXMgbm90IHByZXNlbnQuXG4gICAqL1xuICBoYXMocGFyYW06IHN0cmluZyk6IGJvb2xlYW4ge1xuICAgIHRoaXMuaW5pdCgpO1xuICAgIHJldHVybiB0aGlzLm1hcCAhLmhhcyhwYXJhbSk7XG4gIH1cblxuICAvKipcbiAgICogUmV0cmlldmVzIHRoZSBmaXJzdCB2YWx1ZSBmb3IgYSBwYXJhbWV0ZXIuXG4gICAqIEBwYXJhbSBwYXJhbSBUaGUgcGFyYW1ldGVyIG5hbWUuXG4gICAqIEByZXR1cm5zIFRoZSBmaXJzdCB2YWx1ZSBvZiB0aGUgZ2l2ZW4gcGFyYW1ldGVyLFxuICAgKiBvciBgbnVsbGAgaWYgdGhlIHBhcmFtZXRlciBpcyBub3QgcHJlc2VudC5cbiAgICovXG4gIGdldChwYXJhbTogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIHRoaXMuaW5pdCgpO1xuICAgIGNvbnN0IHJlcyA9IHRoaXMubWFwICEuZ2V0KHBhcmFtKTtcbiAgICByZXR1cm4gISFyZXMgPyByZXNbMF0gOiBudWxsO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyBhbGwgdmFsdWVzIGZvciBhICBwYXJhbWV0ZXIuXG4gICAqIEBwYXJhbSBwYXJhbSBUaGUgcGFyYW1ldGVyIG5hbWUuXG4gICAqIEByZXR1cm5zIEFsbCB2YWx1ZXMgaW4gYSBzdHJpbmcgYXJyYXksXG4gICAqIG9yIGBudWxsYCBpZiB0aGUgcGFyYW1ldGVyIG5vdCBwcmVzZW50LlxuICAgKi9cbiAgZ2V0QWxsKHBhcmFtOiBzdHJpbmcpOiBzdHJpbmdbXXxudWxsIHtcbiAgICB0aGlzLmluaXQoKTtcbiAgICByZXR1cm4gdGhpcy5tYXAgIS5nZXQocGFyYW0pIHx8IG51bGw7XG4gIH1cblxuICAvKipcbiAgICogUmV0cmlldmVzIGFsbCB0aGUgcGFyYW1ldGVycyBmb3IgdGhpcyBib2R5LlxuICAgKiBAcmV0dXJucyBUaGUgcGFyYW1ldGVyIG5hbWVzIGluIGEgc3RyaW5nIGFycmF5LlxuICAgKi9cbiAga2V5cygpOiBzdHJpbmdbXSB7XG4gICAgdGhpcy5pbml0KCk7XG4gICAgcmV0dXJuIEFycmF5LmZyb20odGhpcy5tYXAgIS5rZXlzKCkpO1xuICB9XG5cbiAgLyoqXG4gICAqIEFwcGVuZHMgYSBuZXcgdmFsdWUgdG8gZXhpc3RpbmcgdmFsdWVzIGZvciBhIHBhcmFtZXRlci5cbiAgICogQHBhcmFtIHBhcmFtIFRoZSBwYXJhbWV0ZXIgbmFtZS5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSBuZXcgdmFsdWUgdG8gYWRkLlxuICAgKiBAcmV0dXJuIEEgbmV3IGJvZHkgd2l0aCB0aGUgYXBwZW5kZWQgdmFsdWUuXG4gICAqL1xuICBhcHBlbmQocGFyYW06IHN0cmluZywgdmFsdWU6IHN0cmluZyk6IEh0dHBQYXJhbXMgeyByZXR1cm4gdGhpcy5jbG9uZSh7cGFyYW0sIHZhbHVlLCBvcDogJ2EnfSk7IH1cblxuICAvKipcbiAgICogUmVwbGFjZXMgdGhlIHZhbHVlIGZvciBhIHBhcmFtZXRlci5cbiAgICogQHBhcmFtIHBhcmFtIFRoZSBwYXJhbWV0ZXIgbmFtZS5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSBuZXcgdmFsdWUuXG4gICAqIEByZXR1cm4gQSBuZXcgYm9keSB3aXRoIHRoZSBuZXcgdmFsdWUuXG4gICAqL1xuICBzZXQocGFyYW06IHN0cmluZywgdmFsdWU6IHN0cmluZyk6IEh0dHBQYXJhbXMgeyByZXR1cm4gdGhpcy5jbG9uZSh7cGFyYW0sIHZhbHVlLCBvcDogJ3MnfSk7IH1cblxuICAvKipcbiAgICogUmVtb3ZlcyBhIGdpdmVuIHZhbHVlIG9yIGFsbCB2YWx1ZXMgZnJvbSBhIHBhcmFtZXRlci5cbiAgICogQHBhcmFtIHBhcmFtIFRoZSBwYXJhbWV0ZXIgbmFtZS5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSB2YWx1ZSB0byByZW1vdmUsIGlmIHByb3ZpZGVkLlxuICAgKiBAcmV0dXJuIEEgbmV3IGJvZHkgd2l0aCB0aGUgZ2l2ZW4gdmFsdWUgcmVtb3ZlZCwgb3Igd2l0aCBhbGwgdmFsdWVzXG4gICAqIHJlbW92ZWQgaWYgbm8gdmFsdWUgaXMgc3BlY2lmaWVkLlxuICAgKi9cbiAgZGVsZXRlIChwYXJhbTogc3RyaW5nLCB2YWx1ZT86IHN0cmluZyk6IEh0dHBQYXJhbXMgeyByZXR1cm4gdGhpcy5jbG9uZSh7cGFyYW0sIHZhbHVlLCBvcDogJ2QnfSk7IH1cblxuICAvKipcbiAgICogU2VyaWFsaXplcyB0aGUgYm9keSB0byBhbiBlbmNvZGVkIHN0cmluZywgd2hlcmUga2V5LXZhbHVlIHBhaXJzIChzZXBhcmF0ZWQgYnkgYD1gKSBhcmVcbiAgICogc2VwYXJhdGVkIGJ5IGAmYHMuXG4gICAqL1xuICB0b1N0cmluZygpOiBzdHJpbmcge1xuICAgIHRoaXMuaW5pdCgpO1xuICAgIHJldHVybiB0aGlzLmtleXMoKVxuICAgICAgICAubWFwKGtleSA9PiB7XG4gICAgICAgICAgY29uc3QgZUtleSA9IHRoaXMuZW5jb2Rlci5lbmNvZGVLZXkoa2V5KTtcbiAgICAgICAgICByZXR1cm4gdGhpcy5tYXAgIS5nZXQoa2V5KSAhLm1hcCh2YWx1ZSA9PiBlS2V5ICsgJz0nICsgdGhpcy5lbmNvZGVyLmVuY29kZVZhbHVlKHZhbHVlKSlcbiAgICAgICAgICAgICAgLmpvaW4oJyYnKTtcbiAgICAgICAgfSlcbiAgICAgICAgLmpvaW4oJyYnKTtcbiAgfVxuXG4gIHByaXZhdGUgY2xvbmUodXBkYXRlOiBVcGRhdGUpOiBIdHRwUGFyYW1zIHtcbiAgICBjb25zdCBjbG9uZSA9IG5ldyBIdHRwUGFyYW1zKHsgZW5jb2RlcjogdGhpcy5lbmNvZGVyIH0gYXMgSHR0cFBhcmFtc09wdGlvbnMpO1xuICAgIGNsb25lLmNsb25lRnJvbSA9IHRoaXMuY2xvbmVGcm9tIHx8IHRoaXM7XG4gICAgY2xvbmUudXBkYXRlcyA9ICh0aGlzLnVwZGF0ZXMgfHwgW10pLmNvbmNhdChbdXBkYXRlXSk7XG4gICAgcmV0dXJuIGNsb25lO1xuICB9XG5cbiAgcHJpdmF0ZSBpbml0KCkge1xuICAgIGlmICh0aGlzLm1hcCA9PT0gbnVsbCkge1xuICAgICAgdGhpcy5tYXAgPSBuZXcgTWFwPHN0cmluZywgc3RyaW5nW10+KCk7XG4gICAgfVxuICAgIGlmICh0aGlzLmNsb25lRnJvbSAhPT0gbnVsbCkge1xuICAgICAgdGhpcy5jbG9uZUZyb20uaW5pdCgpO1xuICAgICAgdGhpcy5jbG9uZUZyb20ua2V5cygpLmZvckVhY2goa2V5ID0+IHRoaXMubWFwICEuc2V0KGtleSwgdGhpcy5jbG9uZUZyb20gIS5tYXAgIS5nZXQoa2V5KSAhKSk7XG4gICAgICB0aGlzLnVwZGF0ZXMgIS5mb3JFYWNoKHVwZGF0ZSA9PiB7XG4gICAgICAgIHN3aXRjaCAodXBkYXRlLm9wKSB7XG4gICAgICAgICAgY2FzZSAnYSc6XG4gICAgICAgICAgY2FzZSAncyc6XG4gICAgICAgICAgICBjb25zdCBiYXNlID0gKHVwZGF0ZS5vcCA9PT0gJ2EnID8gdGhpcy5tYXAgIS5nZXQodXBkYXRlLnBhcmFtKSA6IHVuZGVmaW5lZCkgfHwgW107XG4gICAgICAgICAgICBiYXNlLnB1c2godXBkYXRlLnZhbHVlICEpO1xuICAgICAgICAgICAgdGhpcy5tYXAgIS5zZXQodXBkYXRlLnBhcmFtLCBiYXNlKTtcbiAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgIGNhc2UgJ2QnOlxuICAgICAgICAgICAgaWYgKHVwZGF0ZS52YWx1ZSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICAgIGxldCBiYXNlID0gdGhpcy5tYXAgIS5nZXQodXBkYXRlLnBhcmFtKSB8fCBbXTtcbiAgICAgICAgICAgICAgY29uc3QgaWR4ID0gYmFzZS5pbmRleE9mKHVwZGF0ZS52YWx1ZSk7XG4gICAgICAgICAgICAgIGlmIChpZHggIT09IC0xKSB7XG4gICAgICAgICAgICAgICAgYmFzZS5zcGxpY2UoaWR4LCAxKTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICBpZiAoYmFzZS5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgICAgICAgdGhpcy5tYXAgIS5zZXQodXBkYXRlLnBhcmFtLCBiYXNlKTtcbiAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICB0aGlzLm1hcCAhLmRlbGV0ZSh1cGRhdGUucGFyYW0pO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICB0aGlzLm1hcCAhLmRlbGV0ZSh1cGRhdGUucGFyYW0pO1xuICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgICB0aGlzLmNsb25lRnJvbSA9IHRoaXMudXBkYXRlcyA9IG51bGw7XG4gICAgfVxuICB9XG59XG4iXX0=