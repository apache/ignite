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
 * Transforms an `HttpRequest` into a stream of `HttpEvent`s, one of which will likely be a
 * `HttpResponse`.
 *
 * `HttpHandler` is injectable. When injected, the handler instance dispatches requests to the
 * first interceptor in the chain, which dispatches to the second, etc, eventually reaching the
 * `HttpBackend`.
 *
 * In an `HttpInterceptor`, the `HttpHandler` parameter is the next interceptor in the chain.
 *
 * \@publicApi
 * @abstract
 */
export class HttpHandler {
}
if (false) {
    /**
     * @abstract
     * @param {?} req
     * @return {?}
     */
    HttpHandler.prototype.handle = function (req) { };
}
/**
 * A final `HttpHandler` which will dispatch the request via browser HTTP APIs to a backend.
 *
 * Interceptors sit between the `HttpClient` interface and the `HttpBackend`.
 *
 * When injected, `HttpBackend` dispatches requests directly to the backend, without going
 * through the interceptor chain.
 *
 * \@publicApi
 * @abstract
 */
export class HttpBackend {
}
if (false) {
    /**
     * @abstract
     * @param {?} req
     * @return {?}
     */
    HttpBackend.prototype.handle = function (req) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYmFja2VuZC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9odHRwL3NyYy9iYWNrZW5kLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQXdCQSxNQUFNLE9BQWdCLFdBQVc7Q0FFaEM7Ozs7Ozs7SUFEQyxrREFBbUU7Ozs7Ozs7Ozs7Ozs7QUFhckUsTUFBTSxPQUFnQixXQUFXO0NBRWhDOzs7Ozs7O0lBREMsa0RBQW1FIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge09ic2VydmFibGV9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHtIdHRwUmVxdWVzdH0gZnJvbSAnLi9yZXF1ZXN0JztcbmltcG9ydCB7SHR0cEV2ZW50fSBmcm9tICcuL3Jlc3BvbnNlJztcblxuLyoqXG4gKiBUcmFuc2Zvcm1zIGFuIGBIdHRwUmVxdWVzdGAgaW50byBhIHN0cmVhbSBvZiBgSHR0cEV2ZW50YHMsIG9uZSBvZiB3aGljaCB3aWxsIGxpa2VseSBiZSBhXG4gKiBgSHR0cFJlc3BvbnNlYC5cbiAqXG4gKiBgSHR0cEhhbmRsZXJgIGlzIGluamVjdGFibGUuIFdoZW4gaW5qZWN0ZWQsIHRoZSBoYW5kbGVyIGluc3RhbmNlIGRpc3BhdGNoZXMgcmVxdWVzdHMgdG8gdGhlXG4gKiBmaXJzdCBpbnRlcmNlcHRvciBpbiB0aGUgY2hhaW4sIHdoaWNoIGRpc3BhdGNoZXMgdG8gdGhlIHNlY29uZCwgZXRjLCBldmVudHVhbGx5IHJlYWNoaW5nIHRoZVxuICogYEh0dHBCYWNrZW5kYC5cbiAqXG4gKiBJbiBhbiBgSHR0cEludGVyY2VwdG9yYCwgdGhlIGBIdHRwSGFuZGxlcmAgcGFyYW1ldGVyIGlzIHRoZSBuZXh0IGludGVyY2VwdG9yIGluIHRoZSBjaGFpbi5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBIdHRwSGFuZGxlciB7XG4gIGFic3RyYWN0IGhhbmRsZShyZXE6IEh0dHBSZXF1ZXN0PGFueT4pOiBPYnNlcnZhYmxlPEh0dHBFdmVudDxhbnk+Pjtcbn1cblxuLyoqXG4gKiBBIGZpbmFsIGBIdHRwSGFuZGxlcmAgd2hpY2ggd2lsbCBkaXNwYXRjaCB0aGUgcmVxdWVzdCB2aWEgYnJvd3NlciBIVFRQIEFQSXMgdG8gYSBiYWNrZW5kLlxuICpcbiAqIEludGVyY2VwdG9ycyBzaXQgYmV0d2VlbiB0aGUgYEh0dHBDbGllbnRgIGludGVyZmFjZSBhbmQgdGhlIGBIdHRwQmFja2VuZGAuXG4gKlxuICogV2hlbiBpbmplY3RlZCwgYEh0dHBCYWNrZW5kYCBkaXNwYXRjaGVzIHJlcXVlc3RzIGRpcmVjdGx5IHRvIHRoZSBiYWNrZW5kLCB3aXRob3V0IGdvaW5nXG4gKiB0aHJvdWdoIHRoZSBpbnRlcmNlcHRvciBjaGFpbi5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBIdHRwQmFja2VuZCBpbXBsZW1lbnRzIEh0dHBIYW5kbGVyIHtcbiAgYWJzdHJhY3QgaGFuZGxlKHJlcTogSHR0cFJlcXVlc3Q8YW55Pik6IE9ic2VydmFibGU8SHR0cEV2ZW50PGFueT4+O1xufVxuIl19