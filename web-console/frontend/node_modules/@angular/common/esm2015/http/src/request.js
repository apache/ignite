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
import { HttpHeaders } from './headers';
import { HttpParams } from './params';
/**
 * Construction interface for `HttpRequest`s.
 *
 * All values are optional and will override default values if provided.
 * @record
 */
function HttpRequestInit() { }
if (false) {
    /** @type {?|undefined} */
    HttpRequestInit.prototype.headers;
    /** @type {?|undefined} */
    HttpRequestInit.prototype.reportProgress;
    /** @type {?|undefined} */
    HttpRequestInit.prototype.params;
    /** @type {?|undefined} */
    HttpRequestInit.prototype.responseType;
    /** @type {?|undefined} */
    HttpRequestInit.prototype.withCredentials;
}
/**
 * Determine whether the given HTTP method may include a body.
 * @param {?} method
 * @return {?}
 */
function mightHaveBody(method) {
    switch (method) {
        case 'DELETE':
        case 'GET':
        case 'HEAD':
        case 'OPTIONS':
        case 'JSONP':
            return false;
        default:
            return true;
    }
}
/**
 * Safely assert whether the given value is an ArrayBuffer.
 *
 * In some execution environments ArrayBuffer is not defined.
 * @param {?} value
 * @return {?}
 */
function isArrayBuffer(value) {
    return typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer;
}
/**
 * Safely assert whether the given value is a Blob.
 *
 * In some execution environments Blob is not defined.
 * @param {?} value
 * @return {?}
 */
function isBlob(value) {
    return typeof Blob !== 'undefined' && value instanceof Blob;
}
/**
 * Safely assert whether the given value is a FormData instance.
 *
 * In some execution environments FormData is not defined.
 * @param {?} value
 * @return {?}
 */
function isFormData(value) {
    return typeof FormData !== 'undefined' && value instanceof FormData;
}
/**
 * An outgoing HTTP request with an optional typed body.
 *
 * `HttpRequest` represents an outgoing request, including URL, method,
 * headers, body, and other request configuration options. Instances should be
 * assumed to be immutable. To modify a `HttpRequest`, the `clone`
 * method should be used.
 *
 * \@publicApi
 * @template T
 */
export class HttpRequest {
    /**
     * @param {?} method
     * @param {?} url
     * @param {?=} third
     * @param {?=} fourth
     */
    constructor(method, url, third, fourth) {
        this.url = url;
        /**
         * The request body, or `null` if one isn't set.
         *
         * Bodies are not enforced to be immutable, as they can include a reference to any
         * user-defined data type. However, interceptors should take care to preserve
         * idempotence by treating them as such.
         */
        this.body = null;
        /**
         * Whether this request should be made in a way that exposes progress events.
         *
         * Progress events are expensive (change detection runs on each event) and so
         * they should only be requested if the consumer intends to monitor them.
         */
        this.reportProgress = false;
        /**
         * Whether this request should be sent with outgoing credentials (cookies).
         */
        this.withCredentials = false;
        /**
         * The expected response type of the server.
         *
         * This is used to parse the response appropriately before returning it to
         * the requestee.
         */
        this.responseType = 'json';
        this.method = method.toUpperCase();
        // Next, need to figure out which argument holds the HttpRequestInit
        // options, if any.
        /** @type {?} */
        let options;
        // Check whether a body argument is expected. The only valid way to omit
        // the body argument is to use a known no-body method like GET.
        if (mightHaveBody(this.method) || !!fourth) {
            // Body is the third argument, options are the fourth.
            this.body = (third !== undefined) ? (/** @type {?} */ (third)) : null;
            options = fourth;
        }
        else {
            // No body required, options are the third argument. The body stays null.
            options = (/** @type {?} */ (third));
        }
        // If options have been passed, interpret them.
        if (options) {
            // Normalize reportProgress and withCredentials.
            this.reportProgress = !!options.reportProgress;
            this.withCredentials = !!options.withCredentials;
            // Override default response type of 'json' if one is provided.
            if (!!options.responseType) {
                this.responseType = options.responseType;
            }
            // Override headers if they're provided.
            if (!!options.headers) {
                this.headers = options.headers;
            }
            if (!!options.params) {
                this.params = options.params;
            }
        }
        // If no headers have been passed in, construct a new HttpHeaders instance.
        if (!this.headers) {
            this.headers = new HttpHeaders();
        }
        // If no parameters have been passed in, construct a new HttpUrlEncodedParams instance.
        if (!this.params) {
            this.params = new HttpParams();
            this.urlWithParams = url;
        }
        else {
            // Encode the parameters to a string in preparation for inclusion in the URL.
            /** @type {?} */
            const params = this.params.toString();
            if (params.length === 0) {
                // No parameters, the visible URL is just the URL given at creation time.
                this.urlWithParams = url;
            }
            else {
                // Does the URL already have query parameters? Look for '?'.
                /** @type {?} */
                const qIdx = url.indexOf('?');
                // There are 3 cases to handle:
                // 1) No existing parameters -> append '?' followed by params.
                // 2) '?' exists and is followed by existing query string ->
                //    append '&' followed by params.
                // 3) '?' exists at the end of the url -> append params directly.
                // This basically amounts to determining the character, if any, with
                // which to join the URL and parameters.
                /** @type {?} */
                const sep = qIdx === -1 ? '?' : (qIdx < url.length - 1 ? '&' : '');
                this.urlWithParams = url + sep + params;
            }
        }
    }
    /**
     * Transform the free-form body into a serialized format suitable for
     * transmission to the server.
     * @return {?}
     */
    serializeBody() {
        // If no body is present, no need to serialize it.
        if (this.body === null) {
            return null;
        }
        // Check whether the body is already in a serialized form. If so,
        // it can just be returned directly.
        if (isArrayBuffer(this.body) || isBlob(this.body) || isFormData(this.body) ||
            typeof this.body === 'string') {
            return this.body;
        }
        // Check whether the body is an instance of HttpUrlEncodedParams.
        if (this.body instanceof HttpParams) {
            return this.body.toString();
        }
        // Check whether the body is an object or array, and serialize with JSON if so.
        if (typeof this.body === 'object' || typeof this.body === 'boolean' ||
            Array.isArray(this.body)) {
            return JSON.stringify(this.body);
        }
        // Fall back on toString() for everything else.
        return ((/** @type {?} */ (this.body))).toString();
    }
    /**
     * Examine the body and attempt to infer an appropriate MIME type
     * for it.
     *
     * If no such type can be inferred, this method will return `null`.
     * @return {?}
     */
    detectContentTypeHeader() {
        // An empty body has no content type.
        if (this.body === null) {
            return null;
        }
        // FormData bodies rely on the browser's content type assignment.
        if (isFormData(this.body)) {
            return null;
        }
        // Blobs usually have their own content type. If it doesn't, then
        // no type can be inferred.
        if (isBlob(this.body)) {
            return this.body.type || null;
        }
        // Array buffers have unknown contents and thus no type can be inferred.
        if (isArrayBuffer(this.body)) {
            return null;
        }
        // Technically, strings could be a form of JSON data, but it's safe enough
        // to assume they're plain strings.
        if (typeof this.body === 'string') {
            return 'text/plain';
        }
        // `HttpUrlEncodedParams` has its own content-type.
        if (this.body instanceof HttpParams) {
            return 'application/x-www-form-urlencoded;charset=UTF-8';
        }
        // Arrays, objects, and numbers will be encoded as JSON.
        if (typeof this.body === 'object' || typeof this.body === 'number' ||
            Array.isArray(this.body)) {
            return 'application/json';
        }
        // No type could be inferred.
        return null;
    }
    /**
     * @param {?=} update
     * @return {?}
     */
    clone(update = {}) {
        // For method, url, and responseType, take the current value unless
        // it is overridden in the update hash.
        /** @type {?} */
        const method = update.method || this.method;
        /** @type {?} */
        const url = update.url || this.url;
        /** @type {?} */
        const responseType = update.responseType || this.responseType;
        // The body is somewhat special - a `null` value in update.body means
        // whatever current body is present is being overridden with an empty
        // body, whereas an `undefined` value in update.body implies no
        // override.
        /** @type {?} */
        const body = (update.body !== undefined) ? update.body : this.body;
        // Carefully handle the boolean options to differentiate between
        // `false` and `undefined` in the update args.
        /** @type {?} */
        const withCredentials = (update.withCredentials !== undefined) ? update.withCredentials : this.withCredentials;
        /** @type {?} */
        const reportProgress = (update.reportProgress !== undefined) ? update.reportProgress : this.reportProgress;
        // Headers and params may be appended to if `setHeaders` or
        // `setParams` are used.
        /** @type {?} */
        let headers = update.headers || this.headers;
        /** @type {?} */
        let params = update.params || this.params;
        // Check whether the caller has asked to add headers.
        if (update.setHeaders !== undefined) {
            // Set every requested header.
            headers =
                Object.keys(update.setHeaders)
                    .reduce((/**
                 * @param {?} headers
                 * @param {?} name
                 * @return {?}
                 */
                (headers, name) => headers.set(name, (/** @type {?} */ (update.setHeaders))[name])), headers);
        }
        // Check whether the caller has asked to set params.
        if (update.setParams) {
            // Set every requested param.
            params = Object.keys(update.setParams)
                .reduce((/**
             * @param {?} params
             * @param {?} param
             * @return {?}
             */
            (params, param) => params.set(param, (/** @type {?} */ (update.setParams))[param])), params);
        }
        // Finally, construct the new HttpRequest using the pieces from above.
        return new HttpRequest(method, url, body, {
            params, headers, reportProgress, responseType, withCredentials,
        });
    }
}
if (false) {
    /**
     * The request body, or `null` if one isn't set.
     *
     * Bodies are not enforced to be immutable, as they can include a reference to any
     * user-defined data type. However, interceptors should take care to preserve
     * idempotence by treating them as such.
     * @type {?}
     */
    HttpRequest.prototype.body;
    /**
     * Outgoing headers for this request.
     * @type {?}
     */
    HttpRequest.prototype.headers;
    /**
     * Whether this request should be made in a way that exposes progress events.
     *
     * Progress events are expensive (change detection runs on each event) and so
     * they should only be requested if the consumer intends to monitor them.
     * @type {?}
     */
    HttpRequest.prototype.reportProgress;
    /**
     * Whether this request should be sent with outgoing credentials (cookies).
     * @type {?}
     */
    HttpRequest.prototype.withCredentials;
    /**
     * The expected response type of the server.
     *
     * This is used to parse the response appropriately before returning it to
     * the requestee.
     * @type {?}
     */
    HttpRequest.prototype.responseType;
    /**
     * The outgoing HTTP request method.
     * @type {?}
     */
    HttpRequest.prototype.method;
    /**
     * Outgoing URL parameters.
     * @type {?}
     */
    HttpRequest.prototype.params;
    /**
     * The outgoing URL with all URL parameters set.
     * @type {?}
     */
    HttpRequest.prototype.urlWithParams;
    /** @type {?} */
    HttpRequest.prototype.url;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVxdWVzdC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9odHRwL3NyYy9yZXF1ZXN0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLFdBQVcsQ0FBQztBQUN0QyxPQUFPLEVBQUMsVUFBVSxFQUFDLE1BQU0sVUFBVSxDQUFDOzs7Ozs7O0FBT3BDLDhCQU1DOzs7SUFMQyxrQ0FBc0I7O0lBQ3RCLHlDQUF5Qjs7SUFDekIsaUNBQW9COztJQUNwQix1Q0FBa0Q7O0lBQ2xELDBDQUEwQjs7Ozs7OztBQU01QixTQUFTLGFBQWEsQ0FBQyxNQUFjO0lBQ25DLFFBQVEsTUFBTSxFQUFFO1FBQ2QsS0FBSyxRQUFRLENBQUM7UUFDZCxLQUFLLEtBQUssQ0FBQztRQUNYLEtBQUssTUFBTSxDQUFDO1FBQ1osS0FBSyxTQUFTLENBQUM7UUFDZixLQUFLLE9BQU87WUFDVixPQUFPLEtBQUssQ0FBQztRQUNmO1lBQ0UsT0FBTyxJQUFJLENBQUM7S0FDZjtBQUNILENBQUM7Ozs7Ozs7O0FBT0QsU0FBUyxhQUFhLENBQUMsS0FBVTtJQUMvQixPQUFPLE9BQU8sV0FBVyxLQUFLLFdBQVcsSUFBSSxLQUFLLFlBQVksV0FBVyxDQUFDO0FBQzVFLENBQUM7Ozs7Ozs7O0FBT0QsU0FBUyxNQUFNLENBQUMsS0FBVTtJQUN4QixPQUFPLE9BQU8sSUFBSSxLQUFLLFdBQVcsSUFBSSxLQUFLLFlBQVksSUFBSSxDQUFDO0FBQzlELENBQUM7Ozs7Ozs7O0FBT0QsU0FBUyxVQUFVLENBQUMsS0FBVTtJQUM1QixPQUFPLE9BQU8sUUFBUSxLQUFLLFdBQVcsSUFBSSxLQUFLLFlBQVksUUFBUSxDQUFDO0FBQ3RFLENBQUM7Ozs7Ozs7Ozs7OztBQVlELE1BQU0sT0FBTyxXQUFXOzs7Ozs7O0lBMEV0QixZQUNJLE1BQWMsRUFBVyxHQUFXLEVBQUUsS0FN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QUFDLENBQUM7SUFDNUIsQ0FBQztDQUNGOzs7Ozs7Ozs7O0lBN1NDLDJCQUE2Qjs7Ozs7SUFNN0IsOEJBQWdDOzs7Ozs7OztJQVFoQyxxQ0FBeUM7Ozs7O0lBS3pDLHNDQUEwQzs7Ozs7Ozs7SUFRMUMsbUNBQW1FOzs7OztJQUtuRSw2QkFBd0I7Ozs7O0lBTXhCLDZCQUE4Qjs7Ozs7SUFLOUIsb0NBQStCOztJQXdCWCwwQkFBb0IiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SHR0cEhlYWRlcnN9IGZyb20gJy4vaGVhZGVycyc7XG5pbXBvcnQge0h0dHBQYXJhbXN9IGZyb20gJy4vcGFyYW1zJztcblxuLyoqXG4gKiBDb25zdHJ1Y3Rpb24gaW50ZXJmYWNlIGZvciBgSHR0cFJlcXVlc3Rgcy5cbiAqXG4gKiBBbGwgdmFsdWVzIGFyZSBvcHRpb25hbCBhbmQgd2lsbCBvdmVycmlkZSBkZWZhdWx0IHZhbHVlcyBpZiBwcm92aWRlZC5cbiAqL1xuaW50ZXJmYWNlIEh0dHBSZXF1ZXN0SW5pdCB7XG4gIGhlYWRlcnM/OiBIdHRwSGVhZGVycztcbiAgcmVwb3J0UHJvZ3Jlc3M/OiBib29sZWFuO1xuICBwYXJhbXM/OiBIdHRwUGFyYW1zO1xuICByZXNwb25zZVR5cGU/OiAnYXJyYXlidWZmZXInfCdibG9iJ3wnanNvbid8J3RleHQnO1xuICB3aXRoQ3JlZGVudGlhbHM/OiBib29sZWFuO1xufVxuXG4vKipcbiAqIERldGVybWluZSB3aGV0aGVyIHRoZSBnaXZlbiBIVFRQIG1ldGhvZCBtYXkgaW5jbHVkZSBhIGJvZHkuXG4gKi9cbmZ1bmN0aW9uIG1pZ2h0SGF2ZUJvZHkobWV0aG9kOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgc3dpdGNoIChtZXRob2QpIHtcbiAgICBjYXNlICdERUxFVEUnOlxuICAgIGNhc2UgJ0dFVCc6XG4gICAgY2FzZSAnSEVBRCc6XG4gICAgY2FzZSAnT1BUSU9OUyc6XG4gICAgY2FzZSAnSlNPTlAnOlxuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgfVxufVxuXG4vKipcbiAqIFNhZmVseSBhc3NlcnQgd2hldGhlciB0aGUgZ2l2ZW4gdmFsdWUgaXMgYW4gQXJyYXlCdWZmZXIuXG4gKlxuICogSW4gc29tZSBleGVjdXRpb24gZW52aXJvbm1lbnRzIEFycmF5QnVmZmVyIGlzIG5vdCBkZWZpbmVkLlxuICovXG5mdW5jdGlvbiBpc0FycmF5QnVmZmVyKHZhbHVlOiBhbnkpOiB2YWx1ZSBpcyBBcnJheUJ1ZmZlciB7XG4gIHJldHVybiB0eXBlb2YgQXJyYXlCdWZmZXIgIT09ICd1bmRlZmluZWQnICYmIHZhbHVlIGluc3RhbmNlb2YgQXJyYXlCdWZmZXI7XG59XG5cbi8qKlxuICogU2FmZWx5IGFzc2VydCB3aGV0aGVyIHRoZSBnaXZlbiB2YWx1ZSBpcyBhIEJsb2IuXG4gKlxuICogSW4gc29tZSBleGVjdXRpb24gZW52aXJvbm1lbnRzIEJsb2IgaXMgbm90IGRlZmluZWQuXG4gKi9cbmZ1bmN0aW9uIGlzQmxvYih2YWx1ZTogYW55KTogdmFsdWUgaXMgQmxvYiB7XG4gIHJldHVybiB0eXBlb2YgQmxvYiAhPT0gJ3VuZGVmaW5lZCcgJiYgdmFsdWUgaW5zdGFuY2VvZiBCbG9iO1xufVxuXG4vKipcbiAqIFNhZmVseSBhc3NlcnQgd2hldGhlciB0aGUgZ2l2ZW4gdmFsdWUgaXMgYSBGb3JtRGF0YSBpbnN0YW5jZS5cbiAqXG4gKiBJbiBzb21lIGV4ZWN1dGlvbiBlbnZpcm9ubWVudHMgRm9ybURhdGEgaXMgbm90IGRlZmluZWQuXG4gKi9cbmZ1bmN0aW9uIGlzRm9ybURhdGEodmFsdWU6IGFueSk6IHZhbHVlIGlzIEZvcm1EYXRhIHtcbiAgcmV0dXJuIHR5cGVvZiBGb3JtRGF0YSAhPT0gJ3VuZGVmaW5lZCcgJiYgdmFsdWUgaW5zdGFuY2VvZiBGb3JtRGF0YTtcbn1cblxuLyoqXG4gKiBBbiBvdXRnb2luZyBIVFRQIHJlcXVlc3Qgd2l0aCBhbiBvcHRpb25hbCB0eXBlZCBib2R5LlxuICpcbiAqIGBIdHRwUmVxdWVzdGAgcmVwcmVzZW50cyBhbiBvdXRnb2luZyByZXF1ZXN0LCBpbmNsdWRpbmcgVVJMLCBtZXRob2QsXG4gKiBoZWFkZXJzLCBib2R5LCBhbmQgb3RoZXIgcmVxdWVzdCBjb25maWd1cmF0aW9uIG9wdGlvbnMuIEluc3RhbmNlcyBzaG91bGQgYmVcbiAqIGFzc3VtZWQgdG8gYmUgaW1tdXRhYmxlLiBUbyBtb2RpZnkgYSBgSHR0cFJlcXVlc3RgLCB0aGUgYGNsb25lYFxuICogbWV0aG9kIHNob3VsZCBiZSB1c2VkLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIEh0dHBSZXF1ZXN0PFQ+IHtcbiAgLyoqXG4gICAqIFRoZSByZXF1ZXN0IGJvZHksIG9yIGBudWxsYCBpZiBvbmUgaXNuJ3Qgc2V0LlxuICAgKlxuICAgKiBCb2RpZXMgYXJlIG5vdCBlbmZvcmNlZCB0byBiZSBpbW11dGFibGUsIGFzIHRoZXkgY2FuIGluY2x1ZGUgYSByZWZlcmVuY2UgdG8gYW55XG4gICAqIHVzZXItZGVmaW5lZCBkYXRhIHR5cGUuIEhvd2V2ZXIsIGludGVyY2VwdG9ycyBzaG91bGQgdGFrZSBjYXJlIHRvIHByZXNlcnZlXG4gICAqIGlkZW1wb3RlbmNlIGJ5IHRyZWF0aW5nIHRoZW0gYXMgc3VjaC5cbiAgICovXG4gIHJlYWRvbmx5IGJvZHk6IFR8bnVsbCA9IG51bGw7XG5cbiAgLyoqXG4gICAqIE91dGdvaW5nIGhlYWRlcnMgZm9yIHRoaXMgcmVxdWVzdC5cbiAgICovXG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICByZWFkb25seSBoZWFkZXJzICE6IEh0dHBIZWFkZXJzO1xuXG4gIC8qKlxuICAgKiBXaGV0aGVyIHRoaXMgcmVxdWVzdCBzaG91bGQgYmUgbWFkZSBpbiBhIHdheSB0aGF0IGV4cG9zZXMgcHJvZ3Jlc3MgZXZlbnRzLlxuICAgKlxuICAgKiBQcm9ncmVzcyBldmVudHMgYXJlIGV4cGVuc2l2ZSAoY2hhbmdlIGRldGVjdGlvbiBydW5zIG9uIGVhY2ggZXZlbnQpIGFuZCBzb1xuICAgKiB0aGV5IHNob3VsZCBvbmx5IGJlIHJlcXVlc3RlZCBpZiB0aGUgY29uc3VtZXIgaW50ZW5kcyB0byBtb25pdG9yIHRoZW0uXG4gICAqL1xuICByZWFkb25seSByZXBvcnRQcm9ncmVzczogYm9vbGVhbiA9IGZhbHNlO1xuXG4gIC8qKlxuICAgKiBXaGV0aGVyIHRoaXMgcmVxdWVzdCBzaG91bGQgYmUgc2VudCB3aXRoIG91dGdvaW5nIGNyZWRlbnRpYWxzIChjb29raWVzKS5cbiAgICovXG4gIHJlYWRvbmx5IHdpdGhDcmVkZW50aWFsczogYm9vbGVhbiA9IGZhbHNlO1xuXG4gIC8qKlxuICAgKiBUaGUgZXhwZWN0ZWQgcmVzcG9uc2UgdHlwZSBvZiB0aGUgc2VydmVyLlxuICAgKlxuICAgKiBUaGlzIGlzIHVzZWQgdG8gcGFyc2UgdGhlIHJlc3BvbnNlIGFwcHJvcHJpYXRlbHkgYmVmb3JlIHJldHVybmluZyBpdCB0b1xuICAgKiB0aGUgcmVxdWVzdGVlLlxuICAgKi9cbiAgcmVhZG9ubHkgcmVzcG9uc2VUeXBlOiAnYXJyYXlidWZmZXInfCdibG9iJ3wnanNvbid8J3RleHQnID0gJ2pzb24nO1xuXG4gIC8qKlxuICAgKiBUaGUgb3V0Z29pbmcgSFRUUCByZXF1ZXN0IG1ldGhvZC5cbiAgICovXG4gIHJlYWRvbmx5IG1ldGhvZDogc3RyaW5nO1xuXG4gIC8qKlxuICAgKiBPdXRnb2luZyBVUkwgcGFyYW1ldGVycy5cbiAgICovXG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICByZWFkb25seSBwYXJhbXMgITogSHR0cFBhcmFtcztcblxuICAvKipcbiAgICogVGhlIG91dGdvaW5nIFVSTCB3aXRoIGFsbCBVUkwgcGFyYW1ldGVycyBzZXQuXG4gICAqL1xuICByZWFkb25seSB1cmxXaXRoUGFyYW1zOiBzdHJpbmc7XG5cbiAgY29uc3RydWN0b3IobWV0aG9kOiAnREVMRVRFJ3wnR0VUJ3wnSEVBRCd8J0pTT05QJ3wnT1BUSU9OUycsIHVybDogc3RyaW5nLCBpbml0Pzoge1xuICAgIGhlYWRlcnM/OiBIdHRwSGVhZGVycyxcbiAgICByZXBvcnRQcm9ncmVzcz86IGJvb2xlYW4sXG4gICAgcGFyYW1zPzogSHR0cFBhcmFtcyxcbiAgICByZXNwb25zZVR5cGU/OiAnYXJyYXlidWZmZXInfCdibG9iJ3wnanNvbid8J3RleHQnLFxuICAgIHdpdGhDcmVkZW50aWFscz86IGJvb2xlYW4sXG4gIH0pO1xuICBjb25zdHJ1Y3RvcihtZXRob2Q6ICdQT1NUJ3wnUFVUJ3wnUEFUQ0gnLCB1cmw6IHN0cmluZywgYm9keTogVHxudWxsLCBpbml0Pzoge1xuICAgIGhlYWRlcnM/OiBIdHRwSGVhZGVycyxcbiAgICByZXBvcnRQcm9ncmVzcz86IGJvb2xlYW4sXG4gICAgcGFyYW1zPzogSHR0cFBhcmFtcyxcbiAgICByZXNwb25zZVR5cGU/OiAnYXJyYXlidWZmZXInfCdibG9iJ3wnanNvbid8J3RleHQnLFxuICAgIHdpdGhDcmVkZW50aWFscz86IGJvb2xlYW4sXG4gIH0pO1xuICBjb25zdHJ1Y3RvcihtZXRob2Q6IHN0cmluZywgdXJsOiBzdHJpbmcsIGJvZHk6IFR8bnVsbCwgaW5pdD86IHtcbiAgICBoZWFkZXJzPzogSHR0cEhlYWRlcnMsXG4gICAgcmVwb3J0UHJvZ3Jlc3M/OiBib29sZWFuLFxuICAgIHBhcmFtcz86IEh0dHBQYXJhbXMsXG4gICAgcmVzcG9uc2VUeXBlPzogJ2FycmF5YnVmZmVyJ3wnYmxvYid8J2pzb24nfCd0ZXh0JyxcbiAgICB3aXRoQ3JlZGVudGlhbHM/OiBib29sZWFuLFxuICB9KTtcbiAgY29uc3RydWN0b3IoXG4gICAgICBtZXRob2Q6IHN0cmluZywgcmVhZG9ubHkgdXJsOiBzdHJpbmcsIHRoaXJkPzogVHx7XG4gICAgICAgIGhlYWRlcnM/OiBIdHRwSGVhZGVycyxcbiAgICAgICAgcmVwb3J0UHJvZ3Jlc3M/OiBib29sZWFuLFxuICAgICAgICBwYXJhbXM/OiBIdHRwUGFyYW1zLFxuICAgICAgICByZXNwb25zZVR5cGU/OiAnYXJyYXlidWZmZXInfCdibG9iJ3wnanNvbid8J3RleHQnLFxuICAgICAgICB3aXRoQ3JlZGVudGlhbHM/OiBib29sZWFuLFxuICAgICAgfXxudWxsLFxuICAgICAgZm91cnRoPzoge1xuICAgICAgICBoZWFkZXJzPzogSHR0cEhlYWRlcnMsXG4gICAgICAgIHJlcG9ydFByb2dyZXNzPzogYm9vbGVhbixcbiAgICAgICAgcGFyYW1zPzogSHR0cFBhcmFtcyxcbiAgICAgICAgcmVzcG9uc2VUeXBlPzogJ2FycmF5YnVmZmVyJ3wnYmxvYid8J2pzb24nfCd0ZXh0JyxcbiAgICAgICAgd2l0aENyZWRlbnRpYWxzPzogYm9vbGVhbixcbiAgICAgIH0pIHtcbiAgICB0aGlzLm1ldGhvZCA9IG1ldGhvZC50b1VwcGVyQ2FzZSgpO1xuICAgIC8vIE5leHQsIG5lZWQgdG8gZmlndXJlIG91dCB3aGljaCBhcmd1bWVudCBob2xkcyB0aGUgSHR0cFJlcXVlc3RJbml0XG4gICAgLy8gb3B0aW9ucywgaWYgYW55LlxuICAgIGxldCBvcHRpb25zOiBIdHRwUmVxdWVzdEluaXR8dW5kZWZpbmVkO1xuXG4gICAgLy8gQ2hlY2sgd2hldGhlciBhIGJvZHkgYXJndW1lbnQgaXMgZXhwZWN0ZWQuIFRoZSBvbmx5IHZhbGlkIHdheSB0byBvbWl0XG4gICAgLy8gdGhlIGJvZHkgYXJndW1lbnQgaXMgdG8gdXNlIGEga25vd24gbm8tYm9keSBtZXRob2QgbGlrZSBHRVQuXG4gICAgaWYgKG1pZ2h0SGF2ZUJvZHkodGhpcy5tZXRob2QpIHx8ICEhZm91cnRoKSB7XG4gICAgICAvLyBCb2R5IGlzIHRoZSB0aGlyZCBhcmd1bWVudCwgb3B0aW9ucyBhcmUgdGhlIGZvdXJ0aC5cbiAgICAgIHRoaXMuYm9keSA9ICh0aGlyZCAhPT0gdW5kZWZpbmVkKSA/IHRoaXJkIGFzIFQgOiBudWxsO1xuICAgICAgb3B0aW9ucyA9IGZvdXJ0aDtcbiAgICB9IGVsc2Uge1xuICAgICAgLy8gTm8gYm9keSByZXF1aXJlZCwgb3B0aW9ucyBhcmUgdGhlIHRoaXJkIGFyZ3VtZW50LiBUaGUgYm9keSBzdGF5cyBudWxsLlxuICAgICAgb3B0aW9ucyA9IHRoaXJkIGFzIEh0dHBSZXF1ZXN0SW5pdDtcbiAgICB9XG5cbiAgICAvLyBJZiBvcHRpb25zIGhhdmUgYmVlbiBwYXNzZWQsIGludGVycHJldCB0aGVtLlxuICAgIGlmIChvcHRpb25zKSB7XG4gICAgICAvLyBOb3JtYWxpemUgcmVwb3J0UHJvZ3Jlc3MgYW5kIHdpdGhDcmVkZW50aWFscy5cbiAgICAgIHRoaXMucmVwb3J0UHJvZ3Jlc3MgPSAhIW9wdGlvbnMucmVwb3J0UHJvZ3Jlc3M7XG4gICAgICB0aGlzLndpdGhDcmVkZW50aWFscyA9ICEhb3B0aW9ucy53aXRoQ3JlZGVudGlhbHM7XG5cbiAgICAgIC8vIE92ZXJyaWRlIGRlZmF1bHQgcmVzcG9uc2UgdHlwZSBvZiAnanNvbicgaWYgb25lIGlzIHByb3ZpZGVkLlxuICAgICAgaWYgKCEhb3B0aW9ucy5yZXNwb25zZVR5cGUpIHtcbiAgICAgICAgdGhpcy5yZXNwb25zZVR5cGUgPSBvcHRpb25zLnJlc3BvbnNlVHlwZTtcbiAgICAgIH1cblxuICAgICAgLy8gT3ZlcnJpZGUgaGVhZGVycyBpZiB0aGV5J3JlIHByb3ZpZGVkLlxuICAgICAgaWYgKCEhb3B0aW9ucy5oZWFkZXJzKSB7XG4gICAgICAgIHRoaXMuaGVhZGVycyA9IG9wdGlvbnMuaGVhZGVycztcbiAgICAgIH1cblxuICAgICAgaWYgKCEhb3B0aW9ucy5wYXJhbXMpIHtcbiAgICAgICAgdGhpcy5wYXJhbXMgPSBvcHRpb25zLnBhcmFtcztcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBJZiBubyBoZWFkZXJzIGhhdmUgYmVlbiBwYXNzZWQgaW4sIGNvbnN0cnVjdCBhIG5ldyBIdHRwSGVhZGVycyBpbnN0YW5jZS5cbiAgICBpZiAoIXRoaXMuaGVhZGVycykge1xuICAgICAgdGhpcy5oZWFkZXJzID0gbmV3IEh0dHBIZWFkZXJzKCk7XG4gICAgfVxuXG4gICAgLy8gSWYgbm8gcGFyYW1ldGVycyBoYXZlIGJlZW4gcGFzc2VkIGluLCBjb25zdHJ1Y3QgYSBuZXcgSHR0cFVybEVuY29kZWRQYXJhbXMgaW5zdGFuY2UuXG4gICAgaWYgKCF0aGlzLnBhcmFtcykge1xuICAgICAgdGhpcy5wYXJhbXMgPSBuZXcgSHR0cFBhcmFtcygpO1xuICAgICAgdGhpcy51cmxXaXRoUGFyYW1zID0gdXJsO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBFbmNvZGUgdGhlIHBhcmFtZXRlcnMgdG8gYSBzdHJpbmcgaW4gcHJlcGFyYXRpb24gZm9yIGluY2x1c2lvbiBpbiB0aGUgVVJMLlxuICAgICAgY29uc3QgcGFyYW1zID0gdGhpcy5wYXJhbXMudG9TdHJpbmcoKTtcbiAgICAgIGlmIChwYXJhbXMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIC8vIE5vIHBhcmFtZXRlcnMsIHRoZSB2aXNpYmxlIFVSTCBpcyBqdXN0IHRoZSBVUkwgZ2l2ZW4gYXQgY3JlYXRpb24gdGltZS5cbiAgICAgICAgdGhpcy51cmxXaXRoUGFyYW1zID0gdXJsO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgLy8gRG9lcyB0aGUgVVJMIGFscmVhZHkgaGF2ZSBxdWVyeSBwYXJhbWV0ZXJzPyBMb29rIGZvciAnPycuXG4gICAgICAgIGNvbnN0IHFJZHggPSB1cmwuaW5kZXhPZignPycpO1xuICAgICAgICAvLyBUaGVyZSBhcmUgMyBjYXNlcyB0byBoYW5kbGU6XG4gICAgICAgIC8vIDEpIE5vIGV4aXN0aW5nIHBhcmFtZXRlcnMgLT4gYXBwZW5kICc/JyBmb2xsb3dlZCBieSBwYXJhbXMuXG4gICAgICAgIC8vIDIpICc/JyBleGlzdHMgYW5kIGlzIGZvbGxvd2VkIGJ5IGV4aXN0aW5nIHF1ZXJ5IHN0cmluZyAtPlxuICAgICAgICAvLyAgICBhcHBlbmQgJyYnIGZvbGxvd2VkIGJ5IHBhcmFtcy5cbiAgICAgICAgLy8gMykgJz8nIGV4aXN0cyBhdCB0aGUgZW5kIG9mIHRoZSB1cmwgLT4gYXBwZW5kIHBhcmFtcyBkaXJlY3RseS5cbiAgICAgICAgLy8gVGhpcyBiYXNpY2FsbHkgYW1vdW50cyB0byBkZXRlcm1pbmluZyB0aGUgY2hhcmFjdGVyLCBpZiBhbnksIHdpdGhcbiAgICAgICAgLy8gd2hpY2ggdG8gam9pbiB0aGUgVVJMIGFuZCBwYXJhbWV0ZXJzLlxuICAgICAgICBjb25zdCBzZXA6IHN0cmluZyA9IHFJZHggPT09IC0xID8gJz8nIDogKHFJZHggPCB1cmwubGVuZ3RoIC0gMSA/ICcmJyA6ICcnKTtcbiAgICAgICAgdGhpcy51cmxXaXRoUGFyYW1zID0gdXJsICsgc2VwICsgcGFyYW1zO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBUcmFuc2Zvcm0gdGhlIGZyZWUtZm9ybSBib2R5IGludG8gYSBzZXJpYWxpemVkIGZvcm1hdCBzdWl0YWJsZSBmb3JcbiAgICogdHJhbnNtaXNzaW9uIHRvIHRoZSBzZXJ2ZXIuXG4gICAqL1xuICBzZXJpYWxpemVCb2R5KCk6IEFycmF5QnVmZmVyfEJsb2J8Rm9ybURhdGF8c3RyaW5nfG51bGwge1xuICAgIC8vIElmIG5vIGJvZHkgaXMgcHJlc2VudCwgbm8gbmVlZCB0byBzZXJpYWxpemUgaXQuXG4gICAgaWYgKHRoaXMuYm9keSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIC8vIENoZWNrIHdoZXRoZXIgdGhlIGJvZHkgaXMgYWxyZWFkeSBpbiBhIHNlcmlhbGl6ZWQgZm9ybS4gSWYgc28sXG4gICAgLy8gaXQgY2FuIGp1c3QgYmUgcmV0dXJuZWQgZGlyZWN0bHkuXG4gICAgaWYgKGlzQXJyYXlCdWZmZXIodGhpcy5ib2R5KSB8fCBpc0Jsb2IodGhpcy5ib2R5KSB8fCBpc0Zvcm1EYXRhKHRoaXMuYm9keSkgfHxcbiAgICAgICAgdHlwZW9mIHRoaXMuYm9keSA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHJldHVybiB0aGlzLmJvZHk7XG4gICAgfVxuICAgIC8vIENoZWNrIHdoZXRoZXIgdGhlIGJvZHkgaXMgYW4gaW5zdGFuY2Ugb2YgSHR0cFVybEVuY29kZWRQYXJhbXMuXG4gICAgaWYgKHRoaXMuYm9keSBpbnN0YW5jZW9mIEh0dHBQYXJhbXMpIHtcbiAgICAgIHJldHVybiB0aGlzLmJvZHkudG9TdHJpbmcoKTtcbiAgICB9XG4gICAgLy8gQ2hlY2sgd2hldGhlciB0aGUgYm9keSBpcyBhbiBvYmplY3Qgb3IgYXJyYXksIGFuZCBzZXJpYWxpemUgd2l0aCBKU09OIGlmIHNvLlxuICAgIGlmICh0eXBlb2YgdGhpcy5ib2R5ID09PSAnb2JqZWN0JyB8fCB0eXBlb2YgdGhpcy5ib2R5ID09PSAnYm9vbGVhbicgfHxcbiAgICAgICAgQXJyYXkuaXNBcnJheSh0aGlzLmJvZHkpKSB7XG4gICAgICByZXR1cm4gSlNPTi5zdHJpbmdpZnkodGhpcy5ib2R5KTtcbiAgICB9XG4gICAgLy8gRmFsbCBiYWNrIG9uIHRvU3RyaW5nKCkgZm9yIGV2ZXJ5dGhpbmcgZWxzZS5cbiAgICByZXR1cm4gKHRoaXMuYm9keSBhcyBhbnkpLnRvU3RyaW5nKCk7XG4gIH1cblxuICAvKipcbiAgICogRXhhbWluZSB0aGUgYm9keSBhbmQgYXR0ZW1wdCB0byBpbmZlciBhbiBhcHByb3ByaWF0ZSBNSU1FIHR5cGVcbiAgICogZm9yIGl0LlxuICAgKlxuICAgKiBJZiBubyBzdWNoIHR5cGUgY2FuIGJlIGluZmVycmVkLCB0aGlzIG1ldGhvZCB3aWxsIHJldHVybiBgbnVsbGAuXG4gICAqL1xuICBkZXRlY3RDb250ZW50VHlwZUhlYWRlcigpOiBzdHJpbmd8bnVsbCB7XG4gICAgLy8gQW4gZW1wdHkgYm9keSBoYXMgbm8gY29udGVudCB0eXBlLlxuICAgIGlmICh0aGlzLmJvZHkgPT09IG51bGwpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICAvLyBGb3JtRGF0YSBib2RpZXMgcmVseSBvbiB0aGUgYnJvd3NlcidzIGNvbnRlbnQgdHlwZSBhc3NpZ25tZW50LlxuICAgIGlmIChpc0Zvcm1EYXRhKHRoaXMuYm9keSkpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICAvLyBCbG9icyB1c3VhbGx5IGhhdmUgdGhlaXIgb3duIGNvbnRlbnQgdHlwZS4gSWYgaXQgZG9lc24ndCwgdGhlblxuICAgIC8vIG5vIHR5cGUgY2FuIGJlIGluZmVycmVkLlxuICAgIGlmIChpc0Jsb2IodGhpcy5ib2R5KSkge1xuICAgICAgcmV0dXJuIHRoaXMuYm9keS50eXBlIHx8IG51bGw7XG4gICAgfVxuICAgIC8vIEFycmF5IGJ1ZmZlcnMgaGF2ZSB1bmtub3duIGNvbnRlbnRzIGFuZCB0aHVzIG5vIHR5cGUgY2FuIGJlIGluZmVycmVkLlxuICAgIGlmIChpc0FycmF5QnVmZmVyKHRoaXMuYm9keSkpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICAvLyBUZWNobmljYWxseSwgc3RyaW5ncyBjb3VsZCBiZSBhIGZvcm0gb2YgSlNPTiBkYXRhLCBidXQgaXQncyBzYWZlIGVub3VnaFxuICAgIC8vIHRvIGFzc3VtZSB0aGV5J3JlIHBsYWluIHN0cmluZ3MuXG4gICAgaWYgKHR5cGVvZiB0aGlzLmJvZHkgPT09ICdzdHJpbmcnKSB7XG4gICAgICByZXR1cm4gJ3RleHQvcGxhaW4nO1xuICAgIH1cbiAgICAvLyBgSHR0cFVybEVuY29kZWRQYXJhbXNgIGhhcyBpdHMgb3duIGNvbnRlbnQtdHlwZS5cbiAgICBpZiAodGhpcy5ib2R5IGluc3RhbmNlb2YgSHR0cFBhcmFtcykge1xuICAgICAgcmV0dXJuICdhcHBsaWNhdGlvbi94LXd3dy1mb3JtLXVybGVuY29kZWQ7Y2hhcnNldD1VVEYtOCc7XG4gICAgfVxuICAgIC8vIEFycmF5cywgb2JqZWN0cywgYW5kIG51bWJlcnMgd2lsbCBiZSBlbmNvZGVkIGFzIEpTT04uXG4gICAgaWYgKHR5cGVvZiB0aGlzLmJvZHkgPT09ICdvYmplY3QnIHx8IHR5cGVvZiB0aGlzLmJvZHkgPT09ICdudW1iZXInIHx8XG4gICAgICAgIEFycmF5LmlzQXJyYXkodGhpcy5ib2R5KSkge1xuICAgICAgcmV0dXJuICdhcHBsaWNhdGlvbi9qc29uJztcbiAgICB9XG4gICAgLy8gTm8gdHlwZSBjb3VsZCBiZSBpbmZlcnJlZC5cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGNsb25lKCk6IEh0dHBSZXF1ZXN0PFQ+O1xuICBjbG9uZSh1cGRhdGU6IHtcbiAgICBoZWFkZXJzPzogSHR0cEhlYWRlcnMsXG4gICAgcmVwb3J0UHJvZ3Jlc3M/OiBib29sZWFuLFxuICAgIHBhcmFtcz86IEh0dHBQYXJhbXMsXG4gICAgcmVzcG9uc2VUeXBlPzogJ2FycmF5YnVmZmVyJ3wnYmxvYid8J2pzb24nfCd0ZXh0JyxcbiAgICB3aXRoQ3JlZGVudGlhbHM/OiBib29sZWFuLFxuICAgIGJvZHk/OiBUfG51bGwsXG4gICAgbWV0aG9kPzogc3RyaW5nLFxuICAgIHVybD86IHN0cmluZyxcbiAgICBzZXRIZWFkZXJzPzoge1tuYW1lOiBzdHJpbmddOiBzdHJpbmcgfCBzdHJpbmdbXX0sXG4gICAgc2V0UGFyYW1zPzoge1twYXJhbTogc3RyaW5nXTogc3RyaW5nfSxcbiAgfSk6IEh0dHBSZXF1ZXN0PFQ+O1xuICBjbG9uZTxWPih1cGRhdGU6IHtcbiAgICBoZWFkZXJzPzogSHR0cEhlYWRlcnMsXG4gICAgcmVwb3J0UHJvZ3Jlc3M/OiBib29sZWFuLFxuICAgIHBhcmFtcz86IEh0dHBQYXJhbXMsXG4gICAgcmVzcG9uc2VUeXBlPzogJ2FycmF5YnVmZmVyJ3wnYmxvYid8J2pzb24nfCd0ZXh0JyxcbiAgICB3aXRoQ3JlZGVudGlhbHM/OiBib29sZWFuLFxuICAgIGJvZHk/OiBWfG51bGwsXG4gICAgbWV0aG9kPzogc3RyaW5nLFxuICAgIHVybD86IHN0cmluZyxcbiAgICBzZXRIZWFkZXJzPzoge1tuYW1lOiBzdHJpbmddOiBzdHJpbmcgfCBzdHJpbmdbXX0sXG4gICAgc2V0UGFyYW1zPzoge1twYXJhbTogc3RyaW5nXTogc3RyaW5nfSxcbiAgfSk6IEh0dHBSZXF1ZXN0PFY+O1xuICBjbG9uZSh1cGRhdGU6IHtcbiAgICBoZWFkZXJzPzogSHR0cEhlYWRlcnMsXG4gICAgcmVwb3J0UHJvZ3Jlc3M/OiBib29sZWFuLFxuICAgIHBhcmFtcz86IEh0dHBQYXJhbXMsXG4gICAgcmVzcG9uc2VUeXBlPzogJ2FycmF5YnVmZmVyJ3wnYmxvYid8J2pzb24nfCd0ZXh0JyxcbiAgICB3aXRoQ3JlZGVudGlhbHM/OiBib29sZWFuLFxuICAgIGJvZHk/OiBhbnl8bnVsbCxcbiAgICBtZXRob2Q/OiBzdHJpbmcsXG4gICAgdXJsPzogc3RyaW5nLFxuICAgIHNldEhlYWRlcnM/OiB7W25hbWU6IHN0cmluZ106IHN0cmluZyB8IHN0cmluZ1tdfSxcbiAgICBzZXRQYXJhbXM/OiB7W3BhcmFtOiBzdHJpbmddOiBzdHJpbmd9O1xuICB9ID0ge30pOiBIdHRwUmVxdWVzdDxhbnk+IHtcbiAgICAvLyBGb3IgbWV0aG9kLCB1cmwsIGFuZCByZXNwb25zZVR5cGUsIHRha2UgdGhlIGN1cnJlbnQgdmFsdWUgdW5sZXNzXG4gICAgLy8gaXQgaXMgb3ZlcnJpZGRlbiBpbiB0aGUgdXBkYXRlIGhhc2guXG4gICAgY29uc3QgbWV0aG9kID0gdXBkYXRlLm1ldGhvZCB8fCB0aGlzLm1ldGhvZDtcbiAgICBjb25zdCB1cmwgPSB1cGRhdGUudXJsIHx8IHRoaXMudXJsO1xuICAgIGNvbnN0IHJlc3BvbnNlVHlwZSA9IHVwZGF0ZS5yZXNwb25zZVR5cGUgfHwgdGhpcy5yZXNwb25zZVR5cGU7XG5cbiAgICAvLyBUaGUgYm9keSBpcyBzb21ld2hhdCBzcGVjaWFsIC0gYSBgbnVsbGAgdmFsdWUgaW4gdXBkYXRlLmJvZHkgbWVhbnNcbiAgICAvLyB3aGF0ZXZlciBjdXJyZW50IGJvZHkgaXMgcHJlc2VudCBpcyBiZWluZyBvdmVycmlkZGVuIHdpdGggYW4gZW1wdHlcbiAgICAvLyBib2R5LCB3aGVyZWFzIGFuIGB1bmRlZmluZWRgIHZhbHVlIGluIHVwZGF0ZS5ib2R5IGltcGxpZXMgbm9cbiAgICAvLyBvdmVycmlkZS5cbiAgICBjb25zdCBib2R5ID0gKHVwZGF0ZS5ib2R5ICE9PSB1bmRlZmluZWQpID8gdXBkYXRlLmJvZHkgOiB0aGlzLmJvZHk7XG5cbiAgICAvLyBDYXJlZnVsbHkgaGFuZGxlIHRoZSBib29sZWFuIG9wdGlvbnMgdG8gZGlmZmVyZW50aWF0ZSBiZXR3ZWVuXG4gICAgLy8gYGZhbHNlYCBhbmQgYHVuZGVmaW5lZGAgaW4gdGhlIHVwZGF0ZSBhcmdzLlxuICAgIGNvbnN0IHdpdGhDcmVkZW50aWFscyA9XG4gICAgICAgICh1cGRhdGUud2l0aENyZWRlbnRpYWxzICE9PSB1bmRlZmluZWQpID8gdXBkYXRlLndpdGhDcmVkZW50aWFscyA6IHRoaXMud2l0aENyZWRlbnRpYWxzO1xuICAgIGNvbnN0IHJlcG9ydFByb2dyZXNzID1cbiAgICAgICAgKHVwZGF0ZS5yZXBvcnRQcm9ncmVzcyAhPT0gdW5kZWZpbmVkKSA/IHVwZGF0ZS5yZXBvcnRQcm9ncmVzcyA6IHRoaXMucmVwb3J0UHJvZ3Jlc3M7XG5cbiAgICAvLyBIZWFkZXJzIGFuZCBwYXJhbXMgbWF5IGJlIGFwcGVuZGVkIHRvIGlmIGBzZXRIZWFkZXJzYCBvclxuICAgIC8vIGBzZXRQYXJhbXNgIGFyZSB1c2VkLlxuICAgIGxldCBoZWFkZXJzID0gdXBkYXRlLmhlYWRlcnMgfHwgdGhpcy5oZWFkZXJzO1xuICAgIGxldCBwYXJhbXMgPSB1cGRhdGUucGFyYW1zIHx8IHRoaXMucGFyYW1zO1xuXG4gICAgLy8gQ2hlY2sgd2hldGhlciB0aGUgY2FsbGVyIGhhcyBhc2tlZCB0byBhZGQgaGVhZGVycy5cbiAgICBpZiAodXBkYXRlLnNldEhlYWRlcnMgIT09IHVuZGVmaW5lZCkge1xuICAgICAgLy8gU2V0IGV2ZXJ5IHJlcXVlc3RlZCBoZWFkZXIuXG4gICAgICBoZWFkZXJzID1cbiAgICAgICAgICBPYmplY3Qua2V5cyh1cGRhdGUuc2V0SGVhZGVycylcbiAgICAgICAgICAgICAgLnJlZHVjZSgoaGVhZGVycywgbmFtZSkgPT4gaGVhZGVycy5zZXQobmFtZSwgdXBkYXRlLnNldEhlYWRlcnMgIVtuYW1lXSksIGhlYWRlcnMpO1xuICAgIH1cblxuICAgIC8vIENoZWNrIHdoZXRoZXIgdGhlIGNhbGxlciBoYXMgYXNrZWQgdG8gc2V0IHBhcmFtcy5cbiAgICBpZiAodXBkYXRlLnNldFBhcmFtcykge1xuICAgICAgLy8gU2V0IGV2ZXJ5IHJlcXVlc3RlZCBwYXJhbS5cbiAgICAgIHBhcmFtcyA9IE9iamVjdC5rZXlzKHVwZGF0ZS5zZXRQYXJhbXMpXG4gICAgICAgICAgICAgICAgICAgLnJlZHVjZSgocGFyYW1zLCBwYXJhbSkgPT4gcGFyYW1zLnNldChwYXJhbSwgdXBkYXRlLnNldFBhcmFtcyAhW3BhcmFtXSksIHBhcmFtcyk7XG4gICAgfVxuXG4gICAgLy8gRmluYWxseSwgY29uc3RydWN0IHRoZSBuZXcgSHR0cFJlcXVlc3QgdXNpbmcgdGhlIHBpZWNlcyBmcm9tIGFib3ZlLlxuICAgIHJldHVybiBuZXcgSHR0cFJlcXVlc3QoXG4gICAgICAgIG1ldGhvZCwgdXJsLCBib2R5LCB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcGFyYW1zLCBoZWFkZXJzLCByZXBvcnRQcm9ncmVzcywgcmVzcG9uc2VUeXBlLCB3aXRoQ3JlZGVudGlhbHMsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgfVxufVxuIl19