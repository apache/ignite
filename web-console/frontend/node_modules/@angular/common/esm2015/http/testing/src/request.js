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
import { HttpErrorResponse, HttpHeaders, HttpResponse } from '@angular/common/http';
/**
 * A mock requests that was received and is ready to be answered.
 *
 * This interface allows access to the underlying `HttpRequest`, and allows
 * responding with `HttpEvent`s or `HttpErrorResponse`s.
 *
 * \@publicApi
 */
export class TestRequest {
    /**
     * @param {?} request
     * @param {?} observer
     */
    constructor(request, observer) {
        this.request = request;
        this.observer = observer;
        /**
         * \@internal set by `HttpClientTestingBackend`
         */
        this._cancelled = false;
    }
    /**
     * Whether the request was cancelled after it was sent.
     * @return {?}
     */
    get cancelled() { return this._cancelled; }
    /**
     * Resolve the request by returning a body plus additional HTTP information (such as response
     * headers) if provided.
     * If the request specifies an expected body type, the body is converted into the requested type.
     * Otherwise, the body is converted to `JSON` by default.
     *
     * Both successful and unsuccessful responses can be delivered via `flush()`.
     * @param {?} body
     * @param {?=} opts
     * @return {?}
     */
    flush(body, opts = {}) {
        if (this.cancelled) {
            throw new Error(`Cannot flush a cancelled request.`);
        }
        /** @type {?} */
        const url = this.request.urlWithParams;
        /** @type {?} */
        const headers = (opts.headers instanceof HttpHeaders) ? opts.headers : new HttpHeaders(opts.headers);
        body = _maybeConvertBody(this.request.responseType, body);
        /** @type {?} */
        let statusText = opts.statusText;
        /** @type {?} */
        let status = opts.status !== undefined ? opts.status : 200;
        if (opts.status === undefined) {
            if (body === null) {
                status = 204;
                statusText = statusText || 'No Content';
            }
            else {
                statusText = statusText || 'OK';
            }
        }
        if (statusText === undefined) {
            throw new Error('statusText is required when setting a custom status.');
        }
        if (status >= 200 && status < 300) {
            this.observer.next(new HttpResponse({ body, headers, status, statusText, url }));
            this.observer.complete();
        }
        else {
            this.observer.error(new HttpErrorResponse({ error: body, headers, status, statusText, url }));
        }
    }
    /**
     * Resolve the request by returning an `ErrorEvent` (e.g. simulating a network failure).
     * @param {?} error
     * @param {?=} opts
     * @return {?}
     */
    error(error, opts = {}) {
        if (this.cancelled) {
            throw new Error(`Cannot return an error for a cancelled request.`);
        }
        if (opts.status && opts.status >= 200 && opts.status < 300) {
            throw new Error(`error() called with a successful status.`);
        }
        /** @type {?} */
        const headers = (opts.headers instanceof HttpHeaders) ? opts.headers : new HttpHeaders(opts.headers);
        this.observer.error(new HttpErrorResponse({
            error,
            headers,
            status: opts.status || 0,
            statusText: opts.statusText || '',
            url: this.request.urlWithParams,
        }));
    }
    /**
     * Deliver an arbitrary `HttpEvent` (such as a progress event) on the response stream for this
     * request.
     * @param {?} event
     * @return {?}
     */
    event(event) {
        if (this.cancelled) {
            throw new Error(`Cannot send events to a cancelled request.`);
        }
        this.observer.next(event);
    }
}
if (false) {
    /**
     * \@internal set by `HttpClientTestingBackend`
     * @type {?}
     */
    TestRequest.prototype._cancelled;
    /** @type {?} */
    TestRequest.prototype.request;
    /**
     * @type {?}
     * @private
     */
    TestRequest.prototype.observer;
}
/**
 * Helper function to convert a response body to an ArrayBuffer.
 * @param {?} body
 * @return {?}
 */
function _toArrayBufferBody(body) {
    if (typeof ArrayBuffer === 'undefined') {
        throw new Error('ArrayBuffer responses are not supported on this platform.');
    }
    if (body instanceof ArrayBuffer) {
        return body;
    }
    throw new Error('Automatic conversion to ArrayBuffer is not supported for response type.');
}
/**
 * Helper function to convert a response body to a Blob.
 * @param {?} body
 * @return {?}
 */
function _toBlob(body) {
    if (typeof Blob === 'undefined') {
        throw new Error('Blob responses are not supported on this platform.');
    }
    if (body instanceof Blob) {
        return body;
    }
    if (ArrayBuffer && body instanceof ArrayBuffer) {
        return new Blob([body]);
    }
    throw new Error('Automatic conversion to Blob is not supported for response type.');
}
/**
 * Helper function to convert a response body to JSON data.
 * @param {?} body
 * @param {?=} format
 * @return {?}
 */
function _toJsonBody(body, format = 'JSON') {
    if (typeof ArrayBuffer !== 'undefined' && body instanceof ArrayBuffer) {
        throw new Error(`Automatic conversion to ${format} is not supported for ArrayBuffers.`);
    }
    if (typeof Blob !== 'undefined' && body instanceof Blob) {
        throw new Error(`Automatic conversion to ${format} is not supported for Blobs.`);
    }
    if (typeof body === 'string' || typeof body === 'number' || typeof body === 'object' ||
        Array.isArray(body)) {
        return body;
    }
    throw new Error(`Automatic conversion to ${format} is not supported for response type.`);
}
/**
 * Helper function to convert a response body to a string.
 * @param {?} body
 * @return {?}
 */
function _toTextBody(body) {
    if (typeof body === 'string') {
        return body;
    }
    if (typeof ArrayBuffer !== 'undefined' && body instanceof ArrayBuffer) {
        throw new Error('Automatic conversion to text is not supported for ArrayBuffers.');
    }
    if (typeof Blob !== 'undefined' && body instanceof Blob) {
        throw new Error('Automatic conversion to text is not supported for Blobs.');
    }
    return JSON.stringify(_toJsonBody(body, 'text'));
}
/**
 * Convert a response body to the requested type.
 * @param {?} responseType
 * @param {?} body
 * @return {?}
 */
function _maybeConvertBody(responseType, body) {
    if (body === null) {
        return null;
    }
    switch (responseType) {
        case 'arraybuffer':
            return _toArrayBufferBody(body);
        case 'blob':
            return _toBlob(body);
        case 'json':
            return _toJsonBody(body);
        case 'text':
            return _toTextBody(body);
        default:
            throw new Error(`Unsupported responseType: ${responseType}`);
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVxdWVzdC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9odHRwL3Rlc3Rpbmcvc3JjL3JlcXVlc3QudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsaUJBQWlCLEVBQWEsV0FBVyxFQUFlLFlBQVksRUFBQyxNQUFNLHNCQUFzQixDQUFDOzs7Ozs7Ozs7QUFXMUcsTUFBTSxPQUFPLFdBQVc7Ozs7O0lBV3RCLFlBQW1CLE9BQXlCLEVBQVUsUUFBa0M7UUFBckUsWUFBTyxHQUFQLE9BQU8sQ0FBa0I7UUFBVSxhQUFRLEdBQVIsUUFBUSxDQUEwQjs7OztRQUZ4RixlQUFVLEdBQUcsS0FBSyxDQUFDO0lBRXdFLENBQUM7Ozs7O0lBUDVGLElBQUksU0FBUyxLQUFjLE9BQU8sSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUM7Ozs7Ozs7Ozs7OztJQWlCcEQsS0FBSyxDQUFDLElBQThFLEVBQUUsT0FJbEYsRUFBRTtRQUNKLElBQUksSUFBSSxDQUFDLFNBQVMsRUFBRTtZQUNsQixNQUFNLElBQUksS0FBSyxDQUFDLG1DQUFtQyxDQUFDLENBQUM7U0FDdEQ7O2NBQ0ssR0FBRyxHQUFHLElBQUksQ0FBQyxPQUFPLENBQUMsYUFBYTs7Y0FDaEMsT0FBTyxHQUNULENBQUMsSUFBSSxDQUFDLE9BQU8sWUFBWSxXQUFXLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsSUFBSSxXQUFXLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQztRQUN4RixJQUFJLEdBQUcsaUJBQWlCLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxZQUFZLEVBQUUsSUFBSSxDQUFDLENBQUM7O1lBQ3RELFVBQVUsR0FBcUIsSUFBSSxDQUFDLFVBQVU7O1lBQzlDLE1BQU0sR0FBVyxJQUFJLENBQUMsTUFBTSxLQUFLLFNBQVMsQ0FBQyxD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QUFJLElBQUksWUFBWSxXQUFXLEVBQUU7UUFDckUsTUFBTSxJQUFJLEtBQUssQ0FBQyxpRUFBaUUsQ0FBQyxDQUFDO0tBQ3BGO0lBQ0QsSUFBSSxPQUFPLElBQUksS0FBSyxXQUFXLElBQUksSUFBSSxZQUFZLElBQUksRUFBRTtRQUN2RCxNQUFNLElBQUksS0FBSyxDQUFDLDBEQUEwRCxDQUFDLENBQUM7S0FDN0U7SUFDRCxPQUFPLElBQUksQ0FBQyxTQUFTLENBQUMsV0FBVyxDQUFDLElBQUksRUFBRSxNQUFNLENBQUMsQ0FBQyxDQUFDO0FBQ25ELENBQUM7Ozs7Ozs7QUFLRCxTQUFTLGlCQUFpQixDQUN0QixZQUFvQixFQUFFLElBQ3dCO0lBRWhELElBQUksSUFBSSxLQUFLLElBQUksRUFBRTtRQUNqQixPQUFPLElBQUksQ0FBQztLQUNiO0lBQ0QsUUFBUSxZQUFZLEVBQUU7UUFDcEIsS0FBSyxhQUFhO1lBQ2hCLE9BQU8sa0JBQWtCLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDbEMsS0FBSyxNQUFNO1lBQ1QsT0FBTyxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDdkIsS0FBSyxNQUFNO1lBQ1QsT0FBTyxXQUFXLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDM0IsS0FBSyxNQUFNO1lBQ1QsT0FBTyxXQUFXLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDM0I7WUFDRSxNQUFNLElBQUksS0FBSyxDQUFDLDZCQUE2QixZQUFZLEVBQUUsQ0FBQyxDQUFDO0tBQ2hFO0FBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtIdHRwRXJyb3JSZXNwb25zZSwgSHR0cEV2ZW50LCBIdHRwSGVhZGVycywgSHR0cFJlcXVlc3QsIEh0dHBSZXNwb25zZX0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uL2h0dHAnO1xuaW1wb3J0IHtPYnNlcnZlcn0gZnJvbSAncnhqcyc7XG5cbi8qKlxuICogQSBtb2NrIHJlcXVlc3RzIHRoYXQgd2FzIHJlY2VpdmVkIGFuZCBpcyByZWFkeSB0byBiZSBhbnN3ZXJlZC5cbiAqXG4gKiBUaGlzIGludGVyZmFjZSBhbGxvd3MgYWNjZXNzIHRvIHRoZSB1bmRlcmx5aW5nIGBIdHRwUmVxdWVzdGAsIGFuZCBhbGxvd3NcbiAqIHJlc3BvbmRpbmcgd2l0aCBgSHR0cEV2ZW50YHMgb3IgYEh0dHBFcnJvclJlc3BvbnNlYHMuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgVGVzdFJlcXVlc3Qge1xuICAvKipcbiAgICogV2hldGhlciB0aGUgcmVxdWVzdCB3YXMgY2FuY2VsbGVkIGFmdGVyIGl0IHdhcyBzZW50LlxuICAgKi9cbiAgZ2V0IGNhbmNlbGxlZCgpOiBib29sZWFuIHsgcmV0dXJuIHRoaXMuX2NhbmNlbGxlZDsgfVxuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWwgc2V0IGJ5IGBIdHRwQ2xpZW50VGVzdGluZ0JhY2tlbmRgXG4gICAqL1xuICBfY2FuY2VsbGVkID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IocHVibGljIHJlcXVlc3Q6IEh0dHBSZXF1ZXN0PGFueT4sIHByaXZhdGUgb2JzZXJ2ZXI6IE9ic2VydmVyPEh0dHBFdmVudDxhbnk+Pikge31cblxuICAvKipcbiAgICogUmVzb2x2ZSB0aGUgcmVxdWVzdCBieSByZXR1cm5pbmcgYSBib2R5IHBsdXMgYWRkaXRpb25hbCBIVFRQIGluZm9ybWF0aW9uIChzdWNoIGFzIHJlc3BvbnNlXG4gICAqIGhlYWRlcnMpIGlmIHByb3ZpZGVkLlxuICAgKiBJZiB0aGUgcmVxdWVzdCBzcGVjaWZpZXMgYW4gZXhwZWN0ZWQgYm9keSB0eXBlLCB0aGUgYm9keSBpcyBjb252ZXJ0ZWQgaW50byB0aGUgcmVxdWVzdGVkIHR5cGUuXG4gICAqIE90aGVyd2lzZSwgdGhlIGJvZHkgaXMgY29udmVydGVkIHRvIGBKU09OYCBieSBkZWZhdWx0LlxuICAgKlxuICAgKiBCb3RoIHN1Y2Nlc3NmdWwgYW5kIHVuc3VjY2Vzc2Z1bCByZXNwb25zZXMgY2FuIGJlIGRlbGl2ZXJlZCB2aWEgYGZsdXNoKClgLlxuICAgKi9cbiAgZmx1c2goYm9keTogQXJyYXlCdWZmZXJ8QmxvYnxzdHJpbmd8bnVtYmVyfE9iamVjdHwoc3RyaW5nfG51bWJlcnxPYmplY3R8bnVsbClbXXxudWxsLCBvcHRzOiB7XG4gICAgaGVhZGVycz86IEh0dHBIZWFkZXJzIHwge1tuYW1lOiBzdHJpbmddOiBzdHJpbmcgfCBzdHJpbmdbXX0sXG4gICAgc3RhdHVzPzogbnVtYmVyLFxuICAgIHN0YXR1c1RleHQ/OiBzdHJpbmcsXG4gIH0gPSB7fSk6IHZvaWQge1xuICAgIGlmICh0aGlzLmNhbmNlbGxlZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBDYW5ub3QgZmx1c2ggYSBjYW5jZWxsZWQgcmVxdWVzdC5gKTtcbiAgICB9XG4gICAgY29uc3QgdXJsID0gdGhpcy5yZXF1ZXN0LnVybFdpdGhQYXJhbXM7XG4gICAgY29uc3QgaGVhZGVycyA9XG4gICAgICAgIChvcHRzLmhlYWRlcnMgaW5zdGFuY2VvZiBIdHRwSGVhZGVycykgPyBvcHRzLmhlYWRlcnMgOiBuZXcgSHR0cEhlYWRlcnMob3B0cy5oZWFkZXJzKTtcbiAgICBib2R5ID0gX21heWJlQ29udmVydEJvZHkodGhpcy5yZXF1ZXN0LnJlc3BvbnNlVHlwZSwgYm9keSk7XG4gICAgbGV0IHN0YXR1c1RleHQ6IHN0cmluZ3x1bmRlZmluZWQgPSBvcHRzLnN0YXR1c1RleHQ7XG4gICAgbGV0IHN0YXR1czogbnVtYmVyID0gb3B0cy5zdGF0dXMgIT09IHVuZGVmaW5lZCA/IG9wdHMuc3RhdHVzIDogMjAwO1xuICAgIGlmIChvcHRzLnN0YXR1cyA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBpZiAoYm9keSA9PT0gbnVsbCkge1xuICAgICAgICBzdGF0dXMgPSAyMDQ7XG4gICAgICAgIHN0YXR1c1RleHQgPSBzdGF0dXNUZXh0IHx8ICdObyBDb250ZW50JztcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHN0YXR1c1RleHQgPSBzdGF0dXNUZXh0IHx8ICdPSyc7XG4gICAgICB9XG4gICAgfVxuICAgIGlmIChzdGF0dXNUZXh0ID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignc3RhdHVzVGV4dCBpcyByZXF1aXJlZCB3aGVuIHNldHRpbmcgYSBjdXN0b20gc3RhdHVzLicpO1xuICAgIH1cbiAgICBpZiAoc3RhdHVzID49IDIwMCAmJiBzdGF0dXMgPCAzMDApIHtcbiAgICAgIHRoaXMub2JzZXJ2ZXIubmV4dChuZXcgSHR0cFJlc3BvbnNlPGFueT4oe2JvZHksIGhlYWRlcnMsIHN0YXR1cywgc3RhdHVzVGV4dCwgdXJsfSkpO1xuICAgICAgdGhpcy5vYnNlcnZlci5jb21wbGV0ZSgpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLm9ic2VydmVyLmVycm9yKG5ldyBIdHRwRXJyb3JSZXNwb25zZSh7ZXJyb3I6IGJvZHksIGhlYWRlcnMsIHN0YXR1cywgc3RhdHVzVGV4dCwgdXJsfSkpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBSZXNvbHZlIHRoZSByZXF1ZXN0IGJ5IHJldHVybmluZyBhbiBgRXJyb3JFdmVudGAgKGUuZy4gc2ltdWxhdGluZyBhIG5ldHdvcmsgZmFpbHVyZSkuXG4gICAqL1xuICBlcnJvcihlcnJvcjogRXJyb3JFdmVudCwgb3B0czoge1xuICAgIGhlYWRlcnM/OiBIdHRwSGVhZGVycyB8IHtbbmFtZTogc3RyaW5nXTogc3RyaW5nIHwgc3RyaW5nW119LFxuICAgIHN0YXR1cz86IG51bWJlcixcbiAgICBzdGF0dXNUZXh0Pzogc3RyaW5nLFxuICB9ID0ge30pOiB2b2lkIHtcbiAgICBpZiAodGhpcy5jYW5jZWxsZWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgQ2Fubm90IHJldHVybiBhbiBlcnJvciBmb3IgYSBjYW5jZWxsZWQgcmVxdWVzdC5gKTtcbiAgICB9XG4gICAgaWYgKG9wdHMuc3RhdHVzICYmIG9wdHMuc3RhdHVzID49IDIwMCAmJiBvcHRzLnN0YXR1cyA8IDMwMCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBlcnJvcigpIGNhbGxlZCB3aXRoIGEgc3VjY2Vzc2Z1bCBzdGF0dXMuYCk7XG4gICAgfVxuICAgIGNvbnN0IGhlYWRlcnMgPVxuICAgICAgICAob3B0cy5oZWFkZXJzIGluc3RhbmNlb2YgSHR0cEhlYWRlcnMpID8gb3B0cy5oZWFkZXJzIDogbmV3IEh0dHBIZWFkZXJzKG9wdHMuaGVhZGVycyk7XG4gICAgdGhpcy5vYnNlcnZlci5lcnJvcihuZXcgSHR0cEVycm9yUmVzcG9uc2Uoe1xuICAgICAgZXJyb3IsXG4gICAgICBoZWFkZXJzLFxuICAgICAgc3RhdHVzOiBvcHRzLnN0YXR1cyB8fCAwLFxuICAgICAgc3RhdHVzVGV4dDogb3B0cy5zdGF0dXNUZXh0IHx8ICcnLFxuICAgICAgdXJsOiB0aGlzLnJlcXVlc3QudXJsV2l0aFBhcmFtcyxcbiAgICB9KSk7XG4gIH1cblxuICAvKipcbiAgICogRGVsaXZlciBhbiBhcmJpdHJhcnkgYEh0dHBFdmVudGAgKHN1Y2ggYXMgYSBwcm9ncmVzcyBldmVudCkgb24gdGhlIHJlc3BvbnNlIHN0cmVhbSBmb3IgdGhpc1xuICAgKiByZXF1ZXN0LlxuICAgKi9cbiAgZXZlbnQoZXZlbnQ6IEh0dHBFdmVudDxhbnk+KTogdm9pZCB7XG4gICAgaWYgKHRoaXMuY2FuY2VsbGVkKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYENhbm5vdCBzZW5kIGV2ZW50cyB0byBhIGNhbmNlbGxlZCByZXF1ZXN0LmApO1xuICAgIH1cbiAgICB0aGlzLm9ic2VydmVyLm5leHQoZXZlbnQpO1xuICB9XG59XG5cblxuLyoqXG4gKiBIZWxwZXIgZnVuY3Rpb24gdG8gY29udmVydCBhIHJlc3BvbnNlIGJvZHkgdG8gYW4gQXJyYXlCdWZmZXIuXG4gKi9cbmZ1bmN0aW9uIF90b0FycmF5QnVmZmVyQm9keShcbiAgICBib2R5OiBBcnJheUJ1ZmZlciB8IEJsb2IgfCBzdHJpbmcgfCBudW1iZXIgfCBPYmplY3QgfFxuICAgIChzdHJpbmcgfCBudW1iZXIgfCBPYmplY3QgfCBudWxsKVtdKTogQXJyYXlCdWZmZXIge1xuICBpZiAodHlwZW9mIEFycmF5QnVmZmVyID09PSAndW5kZWZpbmVkJykge1xuICAgIHRocm93IG5ldyBFcnJvcignQXJyYXlCdWZmZXIgcmVzcG9uc2VzIGFyZSBub3Qgc3VwcG9ydGVkIG9uIHRoaXMgcGxhdGZvcm0uJyk7XG4gIH1cbiAgaWYgKGJvZHkgaW5zdGFuY2VvZiBBcnJheUJ1ZmZlcikge1xuICAgIHJldHVybiBib2R5O1xuICB9XG4gIHRocm93IG5ldyBFcnJvcignQXV0b21hdGljIGNvbnZlcnNpb24gdG8gQXJyYXlCdWZmZXIgaXMgbm90IHN1cHBvcnRlZCBmb3IgcmVzcG9uc2UgdHlwZS4nKTtcbn1cblxuLyoqXG4gKiBIZWxwZXIgZnVuY3Rpb24gdG8gY29udmVydCBhIHJlc3BvbnNlIGJvZHkgdG8gYSBCbG9iLlxuICovXG5mdW5jdGlvbiBfdG9CbG9iKFxuICAgIGJvZHk6IEFycmF5QnVmZmVyIHwgQmxvYiB8IHN0cmluZyB8IG51bWJlciB8IE9iamVjdCB8XG4gICAgKHN0cmluZyB8IG51bWJlciB8IE9iamVjdCB8IG51bGwpW10pOiBCbG9iIHtcbiAgaWYgKHR5cGVvZiBCbG9iID09PSAndW5kZWZpbmVkJykge1xuICAgIHRocm93IG5ldyBFcnJvcignQmxvYiByZXNwb25zZXMgYXJlIG5vdCBzdXBwb3J0ZWQgb24gdGhpcyBwbGF0Zm9ybS4nKTtcbiAgfVxuICBpZiAoYm9keSBpbnN0YW5jZW9mIEJsb2IpIHtcbiAgICByZXR1cm4gYm9keTtcbiAgfVxuICBpZiAoQXJyYXlCdWZmZXIgJiYgYm9keSBpbnN0YW5jZW9mIEFycmF5QnVmZmVyKSB7XG4gICAgcmV0dXJuIG5ldyBCbG9iKFtib2R5XSk7XG4gIH1cbiAgdGhyb3cgbmV3IEVycm9yKCdBdXRvbWF0aWMgY29udmVyc2lvbiB0byBCbG9iIGlzIG5vdCBzdXBwb3J0ZWQgZm9yIHJlc3BvbnNlIHR5cGUuJyk7XG59XG5cbi8qKlxuICogSGVscGVyIGZ1bmN0aW9uIHRvIGNvbnZlcnQgYSByZXNwb25zZSBib2R5IHRvIEpTT04gZGF0YS5cbiAqL1xuZnVuY3Rpb24gX3RvSnNvbkJvZHkoXG4gICAgYm9keTogQXJyYXlCdWZmZXIgfCBCbG9iIHwgc3RyaW5nIHwgbnVtYmVyIHwgT2JqZWN0IHwgKHN0cmluZyB8IG51bWJlciB8IE9iamVjdCB8IG51bGwpW10sXG4gICAgZm9ybWF0OiBzdHJpbmcgPSAnSlNPTicpOiBPYmplY3R8c3RyaW5nfG51bWJlcnwoT2JqZWN0IHwgc3RyaW5nIHwgbnVtYmVyKVtdIHtcbiAgaWYgKHR5cGVvZiBBcnJheUJ1ZmZlciAhPT0gJ3VuZGVmaW5lZCcgJiYgYm9keSBpbnN0YW5jZW9mIEFycmF5QnVmZmVyKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKGBBdXRvbWF0aWMgY29udmVyc2lvbiB0byAke2Zvcm1hdH0gaXMgbm90IHN1cHBvcnRlZCBmb3IgQXJyYXlCdWZmZXJzLmApO1xuICB9XG4gIGlmICh0eXBlb2YgQmxvYiAhPT0gJ3VuZGVmaW5lZCcgJiYgYm9keSBpbnN0YW5jZW9mIEJsb2IpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoYEF1dG9tYXRpYyBjb252ZXJzaW9uIHRvICR7Zm9ybWF0fSBpcyBub3Qgc3VwcG9ydGVkIGZvciBCbG9icy5gKTtcbiAgfVxuICBpZiAodHlwZW9mIGJvZHkgPT09ICdzdHJpbmcnIHx8IHR5cGVvZiBib2R5ID09PSAnbnVtYmVyJyB8fCB0eXBlb2YgYm9keSA9PT0gJ29iamVjdCcgfHxcbiAgICAgIEFycmF5LmlzQXJyYXkoYm9keSkpIHtcbiAgICByZXR1cm4gYm9keTtcbiAgfVxuICB0aHJvdyBuZXcgRXJyb3IoYEF1dG9tYXRpYyBjb252ZXJzaW9uIHRvICR7Zm9ybWF0fSBpcyBub3Qgc3VwcG9ydGVkIGZvciByZXNwb25zZSB0eXBlLmApO1xufVxuXG4vKipcbiAqIEhlbHBlciBmdW5jdGlvbiB0byBjb252ZXJ0IGEgcmVzcG9uc2UgYm9keSB0byBhIHN0cmluZy5cbiAqL1xuZnVuY3Rpb24gX3RvVGV4dEJvZHkoXG4gICAgYm9keTogQXJyYXlCdWZmZXIgfCBCbG9iIHwgc3RyaW5nIHwgbnVtYmVyIHwgT2JqZWN0IHxcbiAgICAoc3RyaW5nIHwgbnVtYmVyIHwgT2JqZWN0IHwgbnVsbClbXSk6IHN0cmluZyB7XG4gIGlmICh0eXBlb2YgYm9keSA9PT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gYm9keTtcbiAgfVxuICBpZiAodHlwZW9mIEFycmF5QnVmZmVyICE9PSAndW5kZWZpbmVkJyAmJiBib2R5IGluc3RhbmNlb2YgQXJyYXlCdWZmZXIpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ0F1dG9tYXRpYyBjb252ZXJzaW9uIHRvIHRleHQgaXMgbm90IHN1cHBvcnRlZCBmb3IgQXJyYXlCdWZmZXJzLicpO1xuICB9XG4gIGlmICh0eXBlb2YgQmxvYiAhPT0gJ3VuZGVmaW5lZCcgJiYgYm9keSBpbnN0YW5jZW9mIEJsb2IpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ0F1dG9tYXRpYyBjb252ZXJzaW9uIHRvIHRleHQgaXMgbm90IHN1cHBvcnRlZCBmb3IgQmxvYnMuJyk7XG4gIH1cbiAgcmV0dXJuIEpTT04uc3RyaW5naWZ5KF90b0pzb25Cb2R5KGJvZHksICd0ZXh0JykpO1xufVxuXG4vKipcbiAqIENvbnZlcnQgYSByZXNwb25zZSBib2R5IHRvIHRoZSByZXF1ZXN0ZWQgdHlwZS5cbiAqL1xuZnVuY3Rpb24gX21heWJlQ29udmVydEJvZHkoXG4gICAgcmVzcG9uc2VUeXBlOiBzdHJpbmcsIGJvZHk6IEFycmF5QnVmZmVyIHwgQmxvYiB8IHN0cmluZyB8IG51bWJlciB8IE9iamVjdCB8XG4gICAgICAgIChzdHJpbmcgfCBudW1iZXIgfCBPYmplY3QgfCBudWxsKVtdIHwgbnVsbCk6IEFycmF5QnVmZmVyfEJsb2J8c3RyaW5nfG51bWJlcnxPYmplY3R8XG4gICAgKHN0cmluZyB8IG51bWJlciB8IE9iamVjdCB8IG51bGwpW118bnVsbCB7XG4gIGlmIChib2R5ID09PSBudWxsKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgc3dpdGNoIChyZXNwb25zZVR5cGUpIHtcbiAgICBjYXNlICdhcnJheWJ1ZmZlcic6XG4gICAgICByZXR1cm4gX3RvQXJyYXlCdWZmZXJCb2R5KGJvZHkpO1xuICAgIGNhc2UgJ2Jsb2InOlxuICAgICAgcmV0dXJuIF90b0Jsb2IoYm9keSk7XG4gICAgY2FzZSAnanNvbic6XG4gICAgICByZXR1cm4gX3RvSnNvbkJvZHkoYm9keSk7XG4gICAgY2FzZSAndGV4dCc6XG4gICAgICByZXR1cm4gX3RvVGV4dEJvZHkoYm9keSk7XG4gICAgZGVmYXVsdDpcbiAgICAgIHRocm93IG5ldyBFcnJvcihgVW5zdXBwb3J0ZWQgcmVzcG9uc2VUeXBlOiAke3Jlc3BvbnNlVHlwZX1gKTtcbiAgfVxufVxuIl19