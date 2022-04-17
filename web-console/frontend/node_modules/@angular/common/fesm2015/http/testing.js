/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { HttpHeaders, HttpResponse, HttpErrorResponse, HttpEventType, HttpClientModule, HttpBackend } from '@angular/common/http';
import { Injectable, NgModule } from '@angular/core';
import { Observable } from 'rxjs';

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
 * Defines a matcher for requests based on URL, method, or both.
 *
 * \@publicApi
 * @record
 */
function RequestMatch() { }
if (false) {
    /** @type {?|undefined} */
    RequestMatch.prototype.method;
    /** @type {?|undefined} */
    RequestMatch.prototype.url;
}
/**
 * Controller to be injected into tests, that allows for mocking and flushing
 * of requests.
 *
 * \@publicApi
 * @abstract
 */
class HttpTestingController {
}
if (false) {
    /**
     * Search for requests that match the given parameter, without any expectations.
     * @abstract
     * @param {?} match
     * @return {?}
     */
    HttpTestingController.prototype.match = function (match) { };
    /**
     * Expect that a single request has been made which matches the given URL, and return its
     * mock.
     *
     * If no such request has been made, or more than one such request has been made, fail with an
     * error message including the given request description, if any.
     * @abstract
     * @param {?} url
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectOne = function (url, description) { };
    /**
     * Expect that a single request has been made which matches the given parameters, and return
     * its mock.
     *
     * If no such request has been made, or more than one such request has been made, fail with an
     * error message including the given request description, if any.
     * @abstract
     * @param {?} params
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectOne = function (params, description) { };
    /**
     * Expect that a single request has been made which matches the given predicate function, and
     * return its mock.
     *
     * If no such request has been made, or more than one such request has been made, fail with an
     * error message including the given request description, if any.
     * @abstract
     * @param {?} matchFn
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectOne = function (matchFn, description) { };
    /**
     * Expect that a single request has been made which matches the given condition, and return
     * its mock.
     *
     * If no such request has been made, or more than one such request has been made, fail with an
     * error message including the given request description, if any.
     * @abstract
     * @param {?} match
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectOne = function (match, description) { };
    /**
     * Expect that no requests have been made which match the given URL.
     *
     * If a matching request has been made, fail with an error message including the given request
     * description, if any.
     * @abstract
     * @param {?} url
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectNone = function (url, description) { };
    /**
     * Expect that no requests have been made which match the given parameters.
     *
     * If a matching request has been made, fail with an error message including the given request
     * description, if any.
     * @abstract
     * @param {?} params
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectNone = function (params, description) { };
    /**
     * Expect that no requests have been made which match the given predicate function.
     *
     * If a matching request has been made, fail with an error message including the given request
     * description, if any.
     * @abstract
     * @param {?} matchFn
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectNone = function (matchFn, description) { };
    /**
     * Expect that no requests have been made which match the given condition.
     *
     * If a matching request has been made, fail with an error message including the given request
     * description, if any.
     * @abstract
     * @param {?} match
     * @param {?=} description
     * @return {?}
     */
    HttpTestingController.prototype.expectNone = function (match, description) { };
    /**
     * Verify that no unmatched requests are outstanding.
     *
     * If any requests are outstanding, fail with an error message indicating which requests were not
     * handled.
     *
     * If `ignoreCancelled` is not set (the default), `verify()` will also fail if cancelled requests
     * were not explicitly matched.
     * @abstract
     * @param {?=} opts
     * @return {?}
     */
    HttpTestingController.prototype.verify = function (opts) { };
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * A mock requests that was received and is ready to be answered.
 *
 * This interface allows access to the underlying `HttpRequest`, and allows
 * responding with `HttpEvent`s or `HttpErrorResponse`s.
 *
 * \@publicApi
 */
class TestRequest {
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

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * A testing backend for `HttpClient` which both acts as an `HttpBackend`
 * and as the `HttpTestingController`.
 *
 * `HttpClientTestingBackend` works by keeping a list of all open requests.
 * As requests come in, they're added to the list. Users can assert that specific
 * requests were made and then flush them. In the end, a verify() method asserts
 * that no unexpected requests were made.
 *
 *
 */
class HttpClientTestingBackend {
    constructor() {
        /**
         * List of pending requests which have not yet been expected.
         */
        this.open = [];
    }
    /**
     * Handle an incoming request by queueing it in the list of open requests.
     * @param {?} req
     * @return {?}
     */
    handle(req) {
        return new Observable((/**
         * @param {?} observer
         * @return {?}
         */
        (observer) => {
            /** @type {?} */
            const testReq = new TestRequest(req, observer);
            this.open.push(testReq);
            observer.next((/** @type {?} */ ({ type: HttpEventType.Sent })));
            return (/**
             * @return {?}
             */
            () => { testReq._cancelled = true; });
        }));
    }
    /**
     * Helper function to search for requests in the list of open requests.
     * @private
     * @param {?} match
     * @return {?}
     */
    _match(match) {
        if (typeof match === 'string') {
            return this.open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => testReq.request.urlWithParams === match));
        }
        else if (typeof match === 'function') {
            return this.open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => match(testReq.request)));
        }
        else {
            return this.open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => (!match.method || testReq.request.method === match.method.toUpperCase()) &&
                (!match.url || testReq.request.urlWithParams === match.url)));
        }
    }
    /**
     * Search for requests in the list of open requests, and return all that match
     * without asserting anything about the number of matches.
     * @param {?} match
     * @return {?}
     */
    match(match) {
        /** @type {?} */
        const results = this._match(match);
        results.forEach((/**
         * @param {?} result
         * @return {?}
         */
        result => {
            /** @type {?} */
            const index = this.open.indexOf(result);
            if (index !== -1) {
                this.open.splice(index, 1);
            }
        }));
        return results;
    }
    /**
     * Expect that a single outstanding request matches the given matcher, and return
     * it.
     *
     * Requests returned through this API will no longer be in the list of open requests,
     * and thus will not match twice.
     * @param {?} match
     * @param {?=} description
     * @return {?}
     */
    expectOne(match, description) {
        description = description || this.descriptionFromMatcher(match);
        /** @type {?} */
        const matches = this.match(match);
        if (matches.length > 1) {
            throw new Error(`Expected one matching request for criteria "${description}", found ${matches.length} requests.`);
        }
        if (matches.length === 0) {
            throw new Error(`Expected one matching request for criteria "${description}", found none.`);
        }
        return matches[0];
    }
    /**
     * Expect that no outstanding requests match the given matcher, and throw an error
     * if any do.
     * @param {?} match
     * @param {?=} description
     * @return {?}
     */
    expectNone(match, description) {
        description = description || this.descriptionFromMatcher(match);
        /** @type {?} */
        const matches = this.match(match);
        if (matches.length > 0) {
            throw new Error(`Expected zero matching requests for criteria "${description}", found ${matches.length}.`);
        }
    }
    /**
     * Validate that there are no outstanding requests.
     * @param {?=} opts
     * @return {?}
     */
    verify(opts = {}) {
        /** @type {?} */
        let open = this.open;
        // It's possible that some requests may be cancelled, and this is expected.
        // The user can ask to ignore open requests which have been cancelled.
        if (opts.ignoreCancelled) {
            open = open.filter((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => !testReq.cancelled));
        }
        if (open.length > 0) {
            // Show the methods and URLs of open requests in the error, for convenience.
            /** @type {?} */
            const requests = open.map((/**
             * @param {?} testReq
             * @return {?}
             */
            testReq => {
                /** @type {?} */
                const url = testReq.request.urlWithParams.split('?')[0];
                /** @type {?} */
                const method = testReq.request.method;
                return `${method} ${url}`;
            }))
                .join(', ');
            throw new Error(`Expected no open requests, found ${open.length}: ${requests}`);
        }
    }
    /**
     * @private
     * @param {?} matcher
     * @return {?}
     */
    descriptionFromMatcher(matcher) {
        if (typeof matcher === 'string') {
            return `Match URL: ${matcher}`;
        }
        else if (typeof matcher === 'object') {
            /** @type {?} */
            const method = matcher.method || '(any)';
            /** @type {?} */
            const url = matcher.url || '(any)';
            return `Match method: ${method}, URL: ${url}`;
        }
        else {
            return `Match by function: ${matcher.name}`;
        }
    }
}
HttpClientTestingBackend.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * List of pending requests which have not yet been expected.
     * @type {?}
     * @private
     */
    HttpClientTestingBackend.prototype.open;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Configures `HttpClientTestingBackend` as the `HttpBackend` used by `HttpClient`.
 *
 * Inject `HttpTestingController` to expect and flush requests in your tests.
 *
 * \@publicApi
 */
class HttpClientTestingModule {
}
HttpClientTestingModule.decorators = [
    { type: NgModule, args: [{
                imports: [
                    HttpClientModule,
                ],
                providers: [
                    HttpClientTestingBackend,
                    { provide: HttpBackend, useExisting: HttpClientTestingBackend },
                    { provide: HttpTestingController, useExisting: HttpClientTestingBackend },
                ],
            },] }
];

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * Generated bundle index. Do not edit.
 */

export { HttpClientTestingBackend as ɵangular_packages_common_http_testing_testing_a, HttpTestingController, HttpClientTestingModule, TestRequest };
//# sourceMappingURL=testing.js.map
