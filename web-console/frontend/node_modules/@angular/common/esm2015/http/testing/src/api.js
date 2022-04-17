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
export function RequestMatch() { }
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
export class HttpTestingController {
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXBpLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL2h0dHAvdGVzdGluZy9zcmMvYXBpLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBaUJBLGtDQUdDOzs7SUFGQyw4QkFBZ0I7O0lBQ2hCLDJCQUFhOzs7Ozs7Ozs7QUFTZixNQUFNLE9BQWdCLHFCQUFxQjtDQXdGMUM7Ozs7Ozs7O0lBcEZDLDZEQUErRjs7Ozs7Ozs7Ozs7O0lBUy9GLDRFQUFtRTs7Ozs7Ozs7Ozs7O0lBU25FLCtFQUE0RTs7Ozs7Ozs7Ozs7O0lBUzVFLGdGQUNnQjs7Ozs7Ozs7Ozs7O0lBU2hCLDhFQUV1Qzs7Ozs7Ozs7Ozs7SUFRdkMsNkVBQTZEOzs7Ozs7Ozs7OztJQVE3RCxnRkFBc0U7Ozs7Ozs7Ozs7O0lBUXRFLGlGQUErRjs7Ozs7Ozs7Ozs7SUFRL0YsK0VBQ2lHOzs7Ozs7Ozs7Ozs7O0lBV2pHLDZEQUEwRCIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtIdHRwUmVxdWVzdH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uL2h0dHAnO1xuXG5pbXBvcnQge1Rlc3RSZXF1ZXN0fSBmcm9tICcuL3JlcXVlc3QnO1xuXG4vKipcbiAqIERlZmluZXMgYSBtYXRjaGVyIGZvciByZXF1ZXN0cyBiYXNlZCBvbiBVUkwsIG1ldGhvZCwgb3IgYm90aC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgUmVxdWVzdE1hdGNoIHtcbiAgbWV0aG9kPzogc3RyaW5nO1xuICB1cmw/OiBzdHJpbmc7XG59XG5cbi8qKlxuICogQ29udHJvbGxlciB0byBiZSBpbmplY3RlZCBpbnRvIHRlc3RzLCB0aGF0IGFsbG93cyBmb3IgbW9ja2luZyBhbmQgZmx1c2hpbmdcbiAqIG9mIHJlcXVlc3RzLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIEh0dHBUZXN0aW5nQ29udHJvbGxlciB7XG4gIC8qKlxuICAgKiBTZWFyY2ggZm9yIHJlcXVlc3RzIHRoYXQgbWF0Y2ggdGhlIGdpdmVuIHBhcmFtZXRlciwgd2l0aG91dCBhbnkgZXhwZWN0YXRpb25zLlxuICAgKi9cbiAgYWJzdHJhY3QgbWF0Y2gobWF0Y2g6IHN0cmluZ3xSZXF1ZXN0TWF0Y2h8KChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pKTogVGVzdFJlcXVlc3RbXTtcblxuICAvKipcbiAgICogRXhwZWN0IHRoYXQgYSBzaW5nbGUgcmVxdWVzdCBoYXMgYmVlbiBtYWRlIHdoaWNoIG1hdGNoZXMgdGhlIGdpdmVuIFVSTCwgYW5kIHJldHVybiBpdHNcbiAgICogbW9jay5cbiAgICpcbiAgICogSWYgbm8gc3VjaCByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIG9yIG1vcmUgdGhhbiBvbmUgc3VjaCByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIGZhaWwgd2l0aCBhblxuICAgKiBlcnJvciBtZXNzYWdlIGluY2x1ZGluZyB0aGUgZ2l2ZW4gcmVxdWVzdCBkZXNjcmlwdGlvbiwgaWYgYW55LlxuICAgKi9cbiAgYWJzdHJhY3QgZXhwZWN0T25lKHVybDogc3RyaW5nLCBkZXNjcmlwdGlvbj86IHN0cmluZyk6IFRlc3RSZXF1ZXN0O1xuXG4gIC8qKlxuICAgKiBFeHBlY3QgdGhhdCBhIHNpbmdsZSByZXF1ZXN0IGhhcyBiZWVuIG1hZGUgd2hpY2ggbWF0Y2hlcyB0aGUgZ2l2ZW4gcGFyYW1ldGVycywgYW5kIHJldHVyblxuICAgKiBpdHMgbW9jay5cbiAgICpcbiAgICogSWYgbm8gc3VjaCByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIG9yIG1vcmUgdGhhbiBvbmUgc3VjaCByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIGZhaWwgd2l0aCBhblxuICAgKiBlcnJvciBtZXNzYWdlIGluY2x1ZGluZyB0aGUgZ2l2ZW4gcmVxdWVzdCBkZXNjcmlwdGlvbiwgaWYgYW55LlxuICAgKi9cbiAgYWJzdHJhY3QgZXhwZWN0T25lKHBhcmFtczogUmVxdWVzdE1hdGNoLCBkZXNjcmlwdGlvbj86IHN0cmluZyk6IFRlc3RSZXF1ZXN0O1xuXG4gIC8qKlxuICAgKiBFeHBlY3QgdGhhdCBhIHNpbmdsZSByZXF1ZXN0IGhhcyBiZWVuIG1hZGUgd2hpY2ggbWF0Y2hlcyB0aGUgZ2l2ZW4gcHJlZGljYXRlIGZ1bmN0aW9uLCBhbmRcbiAgICogcmV0dXJuIGl0cyBtb2NrLlxuICAgKlxuICAgKiBJZiBubyBzdWNoIHJlcXVlc3QgaGFzIGJlZW4gbWFkZSwgb3IgbW9yZSB0aGFuIG9uZSBzdWNoIHJlcXVlc3QgaGFzIGJlZW4gbWFkZSwgZmFpbCB3aXRoIGFuXG4gICAqIGVycm9yIG1lc3NhZ2UgaW5jbHVkaW5nIHRoZSBnaXZlbiByZXF1ZXN0IGRlc2NyaXB0aW9uLCBpZiBhbnkuXG4gICAqL1xuICBhYnN0cmFjdCBleHBlY3RPbmUobWF0Y2hGbjogKChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pLCBkZXNjcmlwdGlvbj86IHN0cmluZyk6XG4gICAgICBUZXN0UmVxdWVzdDtcblxuICAvKipcbiAgICogRXhwZWN0IHRoYXQgYSBzaW5nbGUgcmVxdWVzdCBoYXMgYmVlbiBtYWRlIHdoaWNoIG1hdGNoZXMgdGhlIGdpdmVuIGNvbmRpdGlvbiwgYW5kIHJldHVyblxuICAgKiBpdHMgbW9jay5cbiAgICpcbiAgICogSWYgbm8gc3VjaCByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIG9yIG1vcmUgdGhhbiBvbmUgc3VjaCByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIGZhaWwgd2l0aCBhblxuICAgKiBlcnJvciBtZXNzYWdlIGluY2x1ZGluZyB0aGUgZ2l2ZW4gcmVxdWVzdCBkZXNjcmlwdGlvbiwgaWYgYW55LlxuICAgKi9cbiAgYWJzdHJhY3QgZXhwZWN0T25lKFxuICAgICAgbWF0Y2g6IHN0cmluZ3xSZXF1ZXN0TWF0Y2h8KChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pLFxuICAgICAgZGVzY3JpcHRpb24/OiBzdHJpbmcpOiBUZXN0UmVxdWVzdDtcblxuICAvKipcbiAgICogRXhwZWN0IHRoYXQgbm8gcmVxdWVzdHMgaGF2ZSBiZWVuIG1hZGUgd2hpY2ggbWF0Y2ggdGhlIGdpdmVuIFVSTC5cbiAgICpcbiAgICogSWYgYSBtYXRjaGluZyByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIGZhaWwgd2l0aCBhbiBlcnJvciBtZXNzYWdlIGluY2x1ZGluZyB0aGUgZ2l2ZW4gcmVxdWVzdFxuICAgKiBkZXNjcmlwdGlvbiwgaWYgYW55LlxuICAgKi9cbiAgYWJzdHJhY3QgZXhwZWN0Tm9uZSh1cmw6IHN0cmluZywgZGVzY3JpcHRpb24/OiBzdHJpbmcpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBFeHBlY3QgdGhhdCBubyByZXF1ZXN0cyBoYXZlIGJlZW4gbWFkZSB3aGljaCBtYXRjaCB0aGUgZ2l2ZW4gcGFyYW1ldGVycy5cbiAgICpcbiAgICogSWYgYSBtYXRjaGluZyByZXF1ZXN0IGhhcyBiZWVuIG1hZGUsIGZhaWwgd2l0aCBhbiBlcnJvciBtZXNzYWdlIGluY2x1ZGluZyB0aGUgZ2l2ZW4gcmVxdWVzdFxuICAgKiBkZXNjcmlwdGlvbiwgaWYgYW55LlxuICAgKi9cbiAgYWJzdHJhY3QgZXhwZWN0Tm9uZShwYXJhbXM6IFJlcXVlc3RNYXRjaCwgZGVzY3JpcHRpb24/OiBzdHJpbmcpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBFeHBlY3QgdGhhdCBubyByZXF1ZXN0cyBoYXZlIGJlZW4gbWFkZSB3aGljaCBtYXRjaCB0aGUgZ2l2ZW4gcHJlZGljYXRlIGZ1bmN0aW9uLlxuICAgKlxuICAgKiBJZiBhIG1hdGNoaW5nIHJlcXVlc3QgaGFzIGJlZW4gbWFkZSwgZmFpbCB3aXRoIGFuIGVycm9yIG1lc3NhZ2UgaW5jbHVkaW5nIHRoZSBnaXZlbiByZXF1ZXN0XG4gICAqIGRlc2NyaXB0aW9uLCBpZiBhbnkuXG4gICAqL1xuICBhYnN0cmFjdCBleHBlY3ROb25lKG1hdGNoRm46ICgocmVxOiBIdHRwUmVxdWVzdDxhbnk+KSA9PiBib29sZWFuKSwgZGVzY3JpcHRpb24/OiBzdHJpbmcpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBFeHBlY3QgdGhhdCBubyByZXF1ZXN0cyBoYXZlIGJlZW4gbWFkZSB3aGljaCBtYXRjaCB0aGUgZ2l2ZW4gY29uZGl0aW9uLlxuICAgKlxuICAgKiBJZiBhIG1hdGNoaW5nIHJlcXVlc3QgaGFzIGJlZW4gbWFkZSwgZmFpbCB3aXRoIGFuIGVycm9yIG1lc3NhZ2UgaW5jbHVkaW5nIHRoZSBnaXZlbiByZXF1ZXN0XG4gICAqIGRlc2NyaXB0aW9uLCBpZiBhbnkuXG4gICAqL1xuICBhYnN0cmFjdCBleHBlY3ROb25lKFxuICAgICAgbWF0Y2g6IHN0cmluZ3xSZXF1ZXN0TWF0Y2h8KChyZXE6IEh0dHBSZXF1ZXN0PGFueT4pID0+IGJvb2xlYW4pLCBkZXNjcmlwdGlvbj86IHN0cmluZyk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIFZlcmlmeSB0aGF0IG5vIHVubWF0Y2hlZCByZXF1ZXN0cyBhcmUgb3V0c3RhbmRpbmcuXG4gICAqXG4gICAqIElmIGFueSByZXF1ZXN0cyBhcmUgb3V0c3RhbmRpbmcsIGZhaWwgd2l0aCBhbiBlcnJvciBtZXNzYWdlIGluZGljYXRpbmcgd2hpY2ggcmVxdWVzdHMgd2VyZSBub3RcbiAgICogaGFuZGxlZC5cbiAgICpcbiAgICogSWYgYGlnbm9yZUNhbmNlbGxlZGAgaXMgbm90IHNldCAodGhlIGRlZmF1bHQpLCBgdmVyaWZ5KClgIHdpbGwgYWxzbyBmYWlsIGlmIGNhbmNlbGxlZCByZXF1ZXN0c1xuICAgKiB3ZXJlIG5vdCBleHBsaWNpdGx5IG1hdGNoZWQuXG4gICAqL1xuICBhYnN0cmFjdCB2ZXJpZnkob3B0cz86IHtpZ25vcmVDYW5jZWxsZWQ/OiBib29sZWFufSk6IHZvaWQ7XG59XG4iXX0=