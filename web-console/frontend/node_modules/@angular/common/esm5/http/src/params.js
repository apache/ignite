/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
/**
 * Provides encoding and decoding of URL parameter and query-string values.
 *
 * Serializes and parses URL parameter keys and values to encode and decode them.
 * If you pass URL query parameters without encoding,
 * the query parameters can be misinterpreted at the receiving end.
 *
 *
 * @publicApi
 */
var HttpUrlEncodingCodec = /** @class */ (function () {
    function HttpUrlEncodingCodec() {
    }
    /**
     * Encodes a key name for a URL parameter or query-string.
     * @param key The key name.
     * @returns The encoded key name.
     */
    HttpUrlEncodingCodec.prototype.encodeKey = function (key) { return standardEncoding(key); };
    /**
     * Encodes the value of a URL parameter or query-string.
     * @param value The value.
     * @returns The encoded value.
     */
    HttpUrlEncodingCodec.prototype.encodeValue = function (value) { return standardEncoding(value); };
    /**
     * Decodes an encoded URL parameter or query-string key.
     * @param key The encoded key name.
     * @returns The decoded key name.
     */
    HttpUrlEncodingCodec.prototype.decodeKey = function (key) { return decodeURIComponent(key); };
    /**
     * Decodes an encoded URL parameter or query-string value.
     * @param value The encoded value.
     * @returns The decoded value.
     */
    HttpUrlEncodingCodec.prototype.decodeValue = function (value) { return decodeURIComponent(value); };
    return HttpUrlEncodingCodec;
}());
export { HttpUrlEncodingCodec };
function paramParser(rawParams, codec) {
    var map = new Map();
    if (rawParams.length > 0) {
        var params = rawParams.split('&');
        params.forEach(function (param) {
            var eqIdx = param.indexOf('=');
            var _a = tslib_1.__read(eqIdx == -1 ?
                [codec.decodeKey(param), ''] :
                [codec.decodeKey(param.slice(0, eqIdx)), codec.decodeValue(param.slice(eqIdx + 1))], 2), key = _a[0], val = _a[1];
            var list = map.get(key) || [];
            list.push(val);
            map.set(key, list);
        });
    }
    return map;
}
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
 * An HTTP request/response body that represents serialized parameters,
 * per the MIME type `application/x-www-form-urlencoded`.
 *
 * This class is immutable; all mutation operations return a new instance.
 *
 * @publicApi
 */
var HttpParams = /** @class */ (function () {
    function HttpParams(options) {
        var _this = this;
        if (options === void 0) { options = {}; }
        this.updates = null;
        this.cloneFrom = null;
        this.encoder = options.encoder || new HttpUrlEncodingCodec();
        if (!!options.fromString) {
            if (!!options.fromObject) {
                throw new Error("Cannot specify both fromString and fromObject.");
            }
            this.map = paramParser(options.fromString, this.encoder);
        }
        else if (!!options.fromObject) {
            this.map = new Map();
            Object.keys(options.fromObject).forEach(function (key) {
                var value = options.fromObject[key];
                _this.map.set(key, Array.isArray(value) ? value : [value]);
            });
        }
        else {
            this.map = null;
        }
    }
    /**
     * Reports whether the body includes one or more values for a given parameter.
     * @param param The parameter name.
     * @returns True if the parameter has one or more values,
     * false if it has no value or is not present.
     */
    HttpParams.prototype.has = function (param) {
        this.init();
        return this.map.has(param);
    };
    /**
     * Retrieves the first value for a parameter.
     * @param param The parameter name.
     * @returns The first value of the given parameter,
     * or `null` if the parameter is not present.
     */
    HttpParams.prototype.get = function (param) {
        this.init();
        var res = this.map.get(param);
        return !!res ? res[0] : null;
    };
    /**
     * Retrieves all values for a  parameter.
     * @param param The parameter name.
     * @returns All values in a string array,
     * or `null` if the parameter not present.
     */
    HttpParams.prototype.getAll = function (param) {
        this.init();
        return this.map.get(param) || null;
    };
    /**
     * Retrieves all the parameters for this body.
     * @returns The parameter names in a string array.
     */
    HttpParams.prototype.keys = function () {
        this.init();
        return Array.from(this.map.keys());
    };
    /**
     * Appends a new value to existing values for a parameter.
     * @param param The parameter name.
     * @param value The new value to add.
     * @return A new body with the appended value.
     */
    HttpParams.prototype.append = function (param, value) { return this.clone({ param: param, value: value, op: 'a' }); };
    /**
     * Replaces the value for a parameter.
     * @param param The parameter name.
     * @param value The new value.
     * @return A new body with the new value.
     */
    HttpParams.prototype.set = function (param, value) { return this.clone({ param: param, value: value, op: 's' }); };
    /**
     * Removes a given value or all values from a parameter.
     * @param param The parameter name.
     * @param value The value to remove, if provided.
     * @return A new body with the given value removed, or with all values
     * removed if no value is specified.
     */
    HttpParams.prototype.delete = function (param, value) { return this.clone({ param: param, value: value, op: 'd' }); };
    /**
     * Serializes the body to an encoded string, where key-value pairs (separated by `=`) are
     * separated by `&`s.
     */
    HttpParams.prototype.toString = function () {
        var _this = this;
        this.init();
        return this.keys()
            .map(function (key) {
            var eKey = _this.encoder.encodeKey(key);
            return _this.map.get(key).map(function (value) { return eKey + '=' + _this.encoder.encodeValue(value); })
                .join('&');
        })
            .join('&');
    };
    HttpParams.prototype.clone = function (update) {
        var clone = new HttpParams({ encoder: this.encoder });
        clone.cloneFrom = this.cloneFrom || this;
        clone.updates = (this.updates || []).concat([update]);
        return clone;
    };
    HttpParams.prototype.init = function () {
        var _this = this;
        if (this.map === null) {
            this.map = new Map();
        }
        if (this.cloneFrom !== null) {
            this.cloneFrom.init();
            this.cloneFrom.keys().forEach(function (key) { return _this.map.set(key, _this.cloneFrom.map.get(key)); });
            this.updates.forEach(function (update) {
                switch (update.op) {
                    case 'a':
                    case 's':
                        var base = (update.op === 'a' ? _this.map.get(update.param) : undefined) || [];
                        base.push(update.value);
                        _this.map.set(update.param, base);
                        break;
                    case 'd':
                        if (update.value !== undefined) {
                            var base_1 = _this.map.get(update.param) || [];
                            var idx = base_1.indexOf(update.value);
                            if (idx !== -1) {
                                base_1.splice(idx, 1);
                            }
                            if (base_1.length > 0) {
                                _this.map.set(update.param, base_1);
                            }
                            else {
                                _this.map.delete(update.param);
                            }
                        }
                        else {
                            _this.map.delete(update.param);
                            break;
                        }
                }
            });
            this.cloneFrom = this.updates = null;
        }
    };
    return HttpParams;
}());
export { HttpParams };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGFyYW1zLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL2h0dHAvc3JjL3BhcmFtcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBaUJIOzs7Ozs7Ozs7R0FTRztBQUNIO0lBQUE7SUE0QkEsQ0FBQztJQTNCQzs7OztPQUlHO0lBQ0gsd0NBQVMsR0FBVCxVQUFVLEdBQVcsSUFBWSxPQUFPLGdCQUFnQixDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUVoRTs7OztPQUlHO0lBQ0gsMENBQVcsR0FBWCxVQUFZLEtBQWEsSUFBWSxPQUFPLGdCQUFnQixDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUV0RTs7OztPQUlHO0lBQ0gsd0NBQVMsR0FBVCxVQUFVLEdBQVcsSUFBWSxPQUFPLGtCQUFrQixDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUVsRTs7OztPQUlHO0lBQ0gsMENBQVcsR0FBWCxVQUFZLEtBQWEsSUFBSSxPQUFPLGtCQUFrQixDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUNsRSwyQkFBQztBQUFELENBQUMsQUE1QkQsSUE0QkM7O0FBR0QsU0FBUyxXQUFXLENBQUMsU0FBaUIsRUFBRSxLQUF5QjtJQUMvRCxJQUFNLEdBQUcsR0FBRyxJQUFJLEdBQUcsRUFBb0IsQ0FBQztJQUN4QyxJQUFJLFNBQVMsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFO1FBQ3hCLElBQU0sTUFBTSxHQUFhLFNBQVMsQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLENBQUM7UUFDOUMsTUFBTSxDQUFDLE9BQU8sQ0FBQyxVQUFDLEtBQWE7WUFDM0IsSUFBTSxLQUFLLEdBQUcsS0FBSyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsQ0FBQztZQUMzQixJQUFBOzt1R0FFaUYsRUFGaEYsV0FBRyxFQUFFLFdBRTJF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LEdBQUssQ0FBQyxHQUFHLENBQUMsTUFBTSxDQUFDLEtBQUssRUFBRSxNQUFJLENBQUMsQ0FBQzs2QkFDcEM7aUNBQU07Z0NBQ0wsS0FBSSxDQUFDLEdBQUssQ0FBQyxNQUFNLENBQUMsTUFBTSxDQUFDLEtBQUssQ0FBQyxDQUFDOzZCQUNqQzt5QkFDRjs2QkFBTTs0QkFDTCxLQUFJLENBQUMsR0FBSyxDQUFDLE1BQU0sQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLENBQUM7NEJBQ2hDLE1BQU07eUJBQ1A7aUJBQ0o7WUFDSCxDQUFDLENBQUMsQ0FBQztZQUNILElBQUksQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDLE9BQU8sR0FBRyxJQUFJLENBQUM7U0FDdEM7SUFDSCxDQUFDO0lBQ0gsaUJBQUM7QUFBRCxDQUFDLEFBdEpELElBc0pDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vKipcbiAqIEEgY29kZWMgZm9yIGVuY29kaW5nIGFuZCBkZWNvZGluZyBwYXJhbWV0ZXJzIGluIFVSTHMuXG4gKlxuICogVXNlZCBieSBgSHR0cFBhcmFtc2AuXG4gKlxuICogQHB1YmxpY0FwaVxuICoqL1xuZXhwb3J0IGludGVyZmFjZSBIdHRwUGFyYW1ldGVyQ29kZWMge1xuICBlbmNvZGVLZXkoa2V5OiBzdHJpbmcpOiBzdHJpbmc7XG4gIGVuY29kZVZhbHVlKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmc7XG5cbiAgZGVjb2RlS2V5KGtleTogc3RyaW5nKTogc3RyaW5nO1xuICBkZWNvZGVWYWx1ZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nO1xufVxuXG4vKipcbiAqIFByb3ZpZGVzIGVuY29kaW5nIGFuZCBkZWNvZGluZyBvZiBVUkwgcGFyYW1ldGVyIGFuZCBxdWVyeS1zdHJpbmcgdmFsdWVzLlxuICpcbiAqIFNlcmlhbGl6ZXMgYW5kIHBhcnNlcyBVUkwgcGFyYW1ldGVyIGtleXMgYW5kIHZhbHVlcyB0byBlbmNvZGUgYW5kIGRlY29kZSB0aGVtLlxuICogSWYgeW91IHBhc3MgVVJMIHF1ZXJ5IHBhcmFtZXRlcnMgd2l0aG91dCBlbmNvZGluZyxcbiAqIHRoZSBxdWVyeSBwYXJhbWV0ZXJzIGNhbiBiZSBtaXNpbnRlcnByZXRlZCBhdCB0aGUgcmVjZWl2aW5nIGVuZC5cbiAqXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgSHR0cFVybEVuY29kaW5nQ29kZWMgaW1wbGVtZW50cyBIdHRwUGFyYW1ldGVyQ29kZWMge1xuICAvKipcbiAgICogRW5jb2RlcyBhIGtleSBuYW1lIGZvciBhIFVSTCBwYXJhbWV0ZXIgb3IgcXVlcnktc3RyaW5nLlxuICAgKiBAcGFyYW0ga2V5IFRoZSBrZXkgbmFtZS5cbiAgICogQHJldHVybnMgVGhlIGVuY29kZWQga2V5IG5hbWUuXG4gICAqL1xuICBlbmNvZGVLZXkoa2V5OiBzdHJpbmcpOiBzdHJpbmcgeyByZXR1cm4gc3RhbmRhcmRFbmNvZGluZyhrZXkpOyB9XG5cbiAgLyoqXG4gICAqIEVuY29kZXMgdGhlIHZhbHVlIG9mIGEgVVJMIHBhcmFtZXRlciBvciBxdWVyeS1zdHJpbmcuXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgdmFsdWUuXG4gICAqIEByZXR1cm5zIFRoZSBlbmNvZGVkIHZhbHVlLlxuICAgKi9cbiAgZW5jb2RlVmFsdWUodmFsdWU6IHN0cmluZyk6IHN0cmluZyB7IHJldHVybiBzdGFuZGFyZEVuY29kaW5nKHZhbHVlKTsgfVxuXG4gIC8qKlxuICAgKiBEZWNvZGVzIGFuIGVuY29kZWQgVVJMIHBhcmFtZXRlciBvciBxdWVyeS1zdHJpbmcga2V5LlxuICAgKiBAcGFyYW0ga2V5IFRoZSBlbmNvZGVkIGtleSBuYW1lLlxuICAgKiBAcmV0dXJucyBUaGUgZGVjb2RlZCBrZXkgbmFtZS5cbiAgICovXG4gIGRlY29kZUtleShrZXk6IHN0cmluZyk6IHN0cmluZyB7IHJldHVybiBkZWNvZGVVUklDb21wb25lbnQoa2V5KTsgfVxuXG4gIC8qKlxuICAgKiBEZWNvZGVzIGFuIGVuY29kZWQgVVJMIHBhcmFtZXRlciBvciBxdWVyeS1zdHJpbmcgdmFsdWUuXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgZW5jb2RlZCB2YWx1ZS5cbiAgICogQHJldHVybnMgVGhlIGRlY29kZWQgdmFsdWUuXG4gICAqL1xuICBkZWNvZGVWYWx1ZSh2YWx1ZTogc3RyaW5nKSB7IHJldHVybiBkZWNvZGVVUklDb21wb25lbnQodmFsdWUpOyB9XG59XG5cblxuZnVuY3Rpb24gcGFyYW1QYXJzZXIocmF3UGFyYW1zOiBzdHJpbmcsIGNvZGVjOiBIdHRwUGFyYW1ldGVyQ29kZWMpOiBNYXA8c3RyaW5nLCBzdHJpbmdbXT4ge1xuICBjb25zdCBtYXAgPSBuZXcgTWFwPHN0cmluZywgc3RyaW5nW10+KCk7XG4gIGlmIChyYXdQYXJhbXMubGVuZ3RoID4gMCkge1xuICAgIGNvbnN0IHBhcmFtczogc3RyaW5nW10gPSByYXdQYXJhbXMuc3BsaXQoJyYnKTtcbiAgICBwYXJhbXMuZm9yRWFjaCgocGFyYW06IHN0cmluZykgPT4ge1xuICAgICAgY29uc3QgZXFJZHggPSBwYXJhbS5pbmRleE9mKCc9Jyk7XG4gICAgICBjb25zdCBba2V5LCB2YWxdOiBzdHJpbmdbXSA9IGVxSWR4ID09IC0xID9cbiAgICAgICAgICBbY29kZWMuZGVjb2RlS2V5KHBhcmFtKSwgJyddIDpcbiAgICAgICAgICBbY29kZWMuZGVjb2RlS2V5KHBhcmFtLnNsaWNlKDAsIGVxSWR4KSksIGNvZGVjLmRlY29kZVZhbHVlKHBhcmFtLnNsaWNlKGVxSWR4ICsgMSkpXTtcbiAgICAgIGNvbnN0IGxpc3QgPSBtYXAuZ2V0KGtleSkgfHwgW107XG4gICAgICBsaXN0LnB1c2godmFsKTtcbiAgICAgIG1hcC5zZXQoa2V5LCBsaXN0KTtcbiAgICB9KTtcbiAgfVxuICByZXR1cm4gbWFwO1xufVxuZnVuY3Rpb24gc3RhbmRhcmRFbmNvZGluZyh2OiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gZW5jb2RlVVJJQ29tcG9uZW50KHYpXG4gICAgICAucmVwbGFjZSgvJTQwL2dpLCAnQCcpXG4gICAgICAucmVwbGFjZSgvJTNBL2dpLCAnOicpXG4gICAgICAucmVwbGFjZSgvJTI0L2dpLCAnJCcpXG4gICAgICAucmVwbGFjZSgvJTJDL2dpLCAnLCcpXG4gICAgICAucmVwbGFjZSgvJTNCL2dpLCAnOycpXG4gICAgICAucmVwbGFjZSgvJTJCL2dpLCAnKycpXG4gICAgICAucmVwbGFjZSgvJTNEL2dpLCAnPScpXG4gICAgICAucmVwbGFjZSgvJTNGL2dpLCAnPycpXG4gICAgICAucmVwbGFjZSgvJTJGL2dpLCAnLycpO1xufVxuXG5pbnRlcmZhY2UgVXBkYXRlIHtcbiAgcGFyYW06IHN0cmluZztcbiAgdmFsdWU/OiBzdHJpbmc7XG4gIG9wOiAnYSd8J2QnfCdzJztcbn1cblxuLyoqIE9wdGlvbnMgdXNlZCB0byBjb25zdHJ1Y3QgYW4gYEh0dHBQYXJhbXNgIGluc3RhbmNlLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBIdHRwUGFyYW1zT3B0aW9ucyB7XG4gIC8qKlxuICAgKiBTdHJpbmcgcmVwcmVzZW50YXRpb24gb2YgdGhlIEhUVFAgcGFyYW1ldGVycyBpbiBVUkwtcXVlcnktc3RyaW5nIGZvcm1hdC5cbiAgICogTXV0dWFsbHkgZXhjbHVzaXZlIHdpdGggYGZyb21PYmplY3RgLlxuICAgKi9cbiAgZnJvbVN0cmluZz86IHN0cmluZztcblxuICAvKiogT2JqZWN0IG1hcCBvZiB0aGUgSFRUUCBwYXJhbWV0ZXJzLiBNdXR1YWxseSBleGNsdXNpdmUgd2l0aCBgZnJvbVN0cmluZ2AuICovXG4gIGZyb21PYmplY3Q/OiB7W3BhcmFtOiBzdHJpbmddOiBzdHJpbmcgfCBSZWFkb25seUFycmF5PHN0cmluZz59O1xuXG4gIC8qKiBFbmNvZGluZyBjb2RlYyB1c2VkIHRvIHBhcnNlIGFuZCBzZXJpYWxpemUgdGhlIHBhcmFtZXRlcnMuICovXG4gIGVuY29kZXI/OiBIdHRwUGFyYW1ldGVyQ29kZWM7XG59XG5cbi8qKlxuICogQW4gSFRUUCByZXF1ZXN0L3Jlc3BvbnNlIGJvZHkgdGhhdCByZXByZXNlbnRzIHNlcmlhbGl6ZWQgcGFyYW1ldGVycyxcbiAqIHBlciB0aGUgTUlNRSB0eXBlIGBhcHBsaWNhdGlvbi94LXd3dy1mb3JtLXVybGVuY29kZWRgLlxuICpcbiAqIFRoaXMgY2xhc3MgaXMgaW1tdXRhYmxlOyBhbGwgbXV0YXRpb24gb3BlcmF0aW9ucyByZXR1cm4gYSBuZXcgaW5zdGFuY2UuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgSHR0cFBhcmFtcyB7XG4gIHByaXZhdGUgbWFwOiBNYXA8c3RyaW5nLCBzdHJpbmdbXT58bnVsbDtcbiAgcHJpdmF0ZSBlbmNvZGVyOiBIdHRwUGFyYW1ldGVyQ29kZWM7XG4gIHByaXZhdGUgdXBkYXRlczogVXBkYXRlW118bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgY2xvbmVGcm9tOiBIdHRwUGFyYW1zfG51bGwgPSBudWxsO1xuXG4gIGNvbnN0cnVjdG9yKG9wdGlvbnM6IEh0dHBQYXJhbXNPcHRpb25zID0ge30gYXMgSHR0cFBhcmFtc09wdGlvbnMpIHtcbiAgICB0aGlzLmVuY29kZXIgPSBvcHRpb25zLmVuY29kZXIgfHwgbmV3IEh0dHBVcmxFbmNvZGluZ0NvZGVjKCk7XG4gICAgaWYgKCEhb3B0aW9ucy5mcm9tU3RyaW5nKSB7XG4gICAgICBpZiAoISFvcHRpb25zLmZyb21PYmplY3QpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBDYW5ub3Qgc3BlY2lmeSBib3RoIGZyb21TdHJpbmcgYW5kIGZyb21PYmplY3QuYCk7XG4gICAgICB9XG4gICAgICB0aGlzLm1hcCA9IHBhcmFtUGFyc2VyKG9wdGlvbnMuZnJvbVN0cmluZywgdGhpcy5lbmNvZGVyKTtcbiAgICB9IGVsc2UgaWYgKCEhb3B0aW9ucy5mcm9tT2JqZWN0KSB7XG4gICAgICB0aGlzLm1hcCA9IG5ldyBNYXA8c3RyaW5nLCBzdHJpbmdbXT4oKTtcbiAgICAgIE9iamVjdC5rZXlzKG9wdGlvbnMuZnJvbU9iamVjdCkuZm9yRWFjaChrZXkgPT4ge1xuICAgICAgICBjb25zdCB2YWx1ZSA9IChvcHRpb25zLmZyb21PYmplY3QgYXMgYW55KVtrZXldO1xuICAgICAgICB0aGlzLm1hcCAhLnNldChrZXksIEFycmF5LmlzQXJyYXkodmFsdWUpID8gdmFsdWUgOiBbdmFsdWVdKTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLm1hcCA9IG51bGw7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFJlcG9ydHMgd2hldGhlciB0aGUgYm9keSBpbmNsdWRlcyBvbmUgb3IgbW9yZSB2YWx1ZXMgZm9yIGEgZ2l2ZW4gcGFyYW1ldGVyLlxuICAgKiBAcGFyYW0gcGFyYW0gVGhlIHBhcmFtZXRlciBuYW1lLlxuICAgKiBAcmV0dXJucyBUcnVlIGlmIHRoZSBwYXJhbWV0ZXIgaGFzIG9uZSBvciBtb3JlIHZhbHVlcyxcbiAgICogZmFsc2UgaWYgaXQgaGFzIG5vIHZhbHVlIG9yIGlzIG5vdCBwcmVzZW50LlxuICAgKi9cbiAgaGFzKHBhcmFtOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICB0aGlzLmluaXQoKTtcbiAgICByZXR1cm4gdGhpcy5tYXAgIS5oYXMocGFyYW0pO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyB0aGUgZmlyc3QgdmFsdWUgZm9yIGEgcGFyYW1ldGVyLlxuICAgKiBAcGFyYW0gcGFyYW0gVGhlIHBhcmFtZXRlciBuYW1lLlxuICAgKiBAcmV0dXJucyBUaGUgZmlyc3QgdmFsdWUgb2YgdGhlIGdpdmVuIHBhcmFtZXRlcixcbiAgICogb3IgYG51bGxgIGlmIHRoZSBwYXJhbWV0ZXIgaXMgbm90IHByZXNlbnQuXG4gICAqL1xuICBnZXQocGFyYW06IHN0cmluZyk6IHN0cmluZ3xudWxsIHtcbiAgICB0aGlzLmluaXQoKTtcbiAgICBjb25zdCByZXMgPSB0aGlzLm1hcCAhLmdldChwYXJhbSk7XG4gICAgcmV0dXJuICEhcmVzID8gcmVzWzBdIDogbnVsbDtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXRyaWV2ZXMgYWxsIHZhbHVlcyBmb3IgYSAgcGFyYW1ldGVyLlxuICAgKiBAcGFyYW0gcGFyYW0gVGhlIHBhcmFtZXRlciBuYW1lLlxuICAgKiBAcmV0dXJucyBBbGwgdmFsdWVzIGluIGEgc3RyaW5nIGFycmF5LFxuICAgKiBvciBgbnVsbGAgaWYgdGhlIHBhcmFtZXRlciBub3QgcHJlc2VudC5cbiAgICovXG4gIGdldEFsbChwYXJhbTogc3RyaW5nKTogc3RyaW5nW118bnVsbCB7XG4gICAgdGhpcy5pbml0KCk7XG4gICAgcmV0dXJuIHRoaXMubWFwICEuZ2V0KHBhcmFtKSB8fCBudWxsO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyBhbGwgdGhlIHBhcmFtZXRlcnMgZm9yIHRoaXMgYm9keS5cbiAgICogQHJldHVybnMgVGhlIHBhcmFtZXRlciBuYW1lcyBpbiBhIHN0cmluZyBhcnJheS5cbiAgICovXG4gIGtleXMoKTogc3RyaW5nW10ge1xuICAgIHRoaXMuaW5pdCgpO1xuICAgIHJldHVybiBBcnJheS5mcm9tKHRoaXMubWFwICEua2V5cygpKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBBcHBlbmRzIGEgbmV3IHZhbHVlIHRvIGV4aXN0aW5nIHZhbHVlcyBmb3IgYSBwYXJhbWV0ZXIuXG4gICAqIEBwYXJhbSBwYXJhbSBUaGUgcGFyYW1ldGVyIG5hbWUuXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgbmV3IHZhbHVlIHRvIGFkZC5cbiAgICogQHJldHVybiBBIG5ldyBib2R5IHdpdGggdGhlIGFwcGVuZGVkIHZhbHVlLlxuICAgKi9cbiAgYXBwZW5kKHBhcmFtOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcpOiBIdHRwUGFyYW1zIHsgcmV0dXJuIHRoaXMuY2xvbmUoe3BhcmFtLCB2YWx1ZSwgb3A6ICdhJ30pOyB9XG5cbiAgLyoqXG4gICAqIFJlcGxhY2VzIHRoZSB2YWx1ZSBmb3IgYSBwYXJhbWV0ZXIuXG4gICAqIEBwYXJhbSBwYXJhbSBUaGUgcGFyYW1ldGVyIG5hbWUuXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgbmV3IHZhbHVlLlxuICAgKiBAcmV0dXJuIEEgbmV3IGJvZHkgd2l0aCB0aGUgbmV3IHZhbHVlLlxuICAgKi9cbiAgc2V0KHBhcmFtOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcpOiBIdHRwUGFyYW1zIHsgcmV0dXJuIHRoaXMuY2xvbmUoe3BhcmFtLCB2YWx1ZSwgb3A6ICdzJ30pOyB9XG5cbiAgLyoqXG4gICAqIFJlbW92ZXMgYSBnaXZlbiB2YWx1ZSBvciBhbGwgdmFsdWVzIGZyb20gYSBwYXJhbWV0ZXIuXG4gICAqIEBwYXJhbSBwYXJhbSBUaGUgcGFyYW1ldGVyIG5hbWUuXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgdmFsdWUgdG8gcmVtb3ZlLCBpZiBwcm92aWRlZC5cbiAgICogQHJldHVybiBBIG5ldyBib2R5IHdpdGggdGhlIGdpdmVuIHZhbHVlIHJlbW92ZWQsIG9yIHdpdGggYWxsIHZhbHVlc1xuICAgKiByZW1vdmVkIGlmIG5vIHZhbHVlIGlzIHNwZWNpZmllZC5cbiAgICovXG4gIGRlbGV0ZSAocGFyYW06IHN0cmluZywgdmFsdWU/OiBzdHJpbmcpOiBIdHRwUGFyYW1zIHsgcmV0dXJuIHRoaXMuY2xvbmUoe3BhcmFtLCB2YWx1ZSwgb3A6ICdkJ30pOyB9XG5cbiAgLyoqXG4gICAqIFNlcmlhbGl6ZXMgdGhlIGJvZHkgdG8gYW4gZW5jb2RlZCBzdHJpbmcsIHdoZXJlIGtleS12YWx1ZSBwYWlycyAoc2VwYXJhdGVkIGJ5IGA9YCkgYXJlXG4gICAqIHNlcGFyYXRlZCBieSBgJmBzLlxuICAgKi9cbiAgdG9TdHJpbmcoKTogc3RyaW5nIHtcbiAgICB0aGlzLmluaXQoKTtcbiAgICByZXR1cm4gdGhpcy5rZXlzKClcbiAgICAgICAgLm1hcChrZXkgPT4ge1xuICAgICAgICAgIGNvbnN0IGVLZXkgPSB0aGlzLmVuY29kZXIuZW5jb2RlS2V5KGtleSk7XG4gICAgICAgICAgcmV0dXJuIHRoaXMubWFwICEuZ2V0KGtleSkgIS5tYXAodmFsdWUgPT4gZUtleSArICc9JyArIHRoaXMuZW5jb2Rlci5lbmNvZGVWYWx1ZSh2YWx1ZSkpXG4gICAgICAgICAgICAgIC5qb2luKCcmJyk7XG4gICAgICAgIH0pXG4gICAgICAgIC5qb2luKCcmJyk7XG4gIH1cblxuICBwcml2YXRlIGNsb25lKHVwZGF0ZTogVXBkYXRlKTogSHR0cFBhcmFtcyB7XG4gICAgY29uc3QgY2xvbmUgPSBuZXcgSHR0cFBhcmFtcyh7IGVuY29kZXI6IHRoaXMuZW5jb2RlciB9IGFzIEh0dHBQYXJhbXNPcHRpb25zKTtcbiAgICBjbG9uZS5jbG9uZUZyb20gPSB0aGlzLmNsb25lRnJvbSB8fCB0aGlzO1xuICAgIGNsb25lLnVwZGF0ZXMgPSAodGhpcy51cGRhdGVzIHx8IFtdKS5jb25jYXQoW3VwZGF0ZV0pO1xuICAgIHJldHVybiBjbG9uZTtcbiAgfVxuXG4gIHByaXZhdGUgaW5pdCgpIHtcbiAgICBpZiAodGhpcy5tYXAgPT09IG51bGwpIHtcbiAgICAgIHRoaXMubWFwID0gbmV3IE1hcDxzdHJpbmcsIHN0cmluZ1tdPigpO1xuICAgIH1cbiAgICBpZiAodGhpcy5jbG9uZUZyb20gIT09IG51bGwpIHtcbiAgICAgIHRoaXMuY2xvbmVGcm9tLmluaXQoKTtcbiAgICAgIHRoaXMuY2xvbmVGcm9tLmtleXMoKS5mb3JFYWNoKGtleSA9PiB0aGlzLm1hcCAhLnNldChrZXksIHRoaXMuY2xvbmVGcm9tICEubWFwICEuZ2V0KGtleSkgISkpO1xuICAgICAgdGhpcy51cGRhdGVzICEuZm9yRWFjaCh1cGRhdGUgPT4ge1xuICAgICAgICBzd2l0Y2ggKHVwZGF0ZS5vcCkge1xuICAgICAgICAgIGNhc2UgJ2EnOlxuICAgICAgICAgIGNhc2UgJ3MnOlxuICAgICAgICAgICAgY29uc3QgYmFzZSA9ICh1cGRhdGUub3AgPT09ICdhJyA/IHRoaXMubWFwICEuZ2V0KHVwZGF0ZS5wYXJhbSkgOiB1bmRlZmluZWQpIHx8IFtdO1xuICAgICAgICAgICAgYmFzZS5wdXNoKHVwZGF0ZS52YWx1ZSAhKTtcbiAgICAgICAgICAgIHRoaXMubWFwICEuc2V0KHVwZGF0ZS5wYXJhbSwgYmFzZSk7XG4gICAgICAgICAgICBicmVhaztcbiAgICAgICAgICBjYXNlICdkJzpcbiAgICAgICAgICAgIGlmICh1cGRhdGUudmFsdWUgIT09IHVuZGVmaW5lZCkge1xuICAgICAgICAgICAgICBsZXQgYmFzZSA9IHRoaXMubWFwICEuZ2V0KHVwZGF0ZS5wYXJhbSkgfHwgW107XG4gICAgICAgICAgICAgIGNvbnN0IGlkeCA9IGJhc2UuaW5kZXhPZih1cGRhdGUudmFsdWUpO1xuICAgICAgICAgICAgICBpZiAoaWR4ICE9PSAtMSkge1xuICAgICAgICAgICAgICAgIGJhc2Uuc3BsaWNlKGlkeCwgMSk7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgaWYgKGJhc2UubGVuZ3RoID4gMCkge1xuICAgICAgICAgICAgICAgIHRoaXMubWFwICEuc2V0KHVwZGF0ZS5wYXJhbSwgYmFzZSk7XG4gICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgdGhpcy5tYXAgIS5kZWxldGUodXBkYXRlLnBhcmFtKTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgdGhpcy5tYXAgIS5kZWxldGUodXBkYXRlLnBhcmFtKTtcbiAgICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgICAgdGhpcy5jbG9uZUZyb20gPSB0aGlzLnVwZGF0ZXMgPSBudWxsO1xuICAgIH1cbiAgfVxufVxuIl19