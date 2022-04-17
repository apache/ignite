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
 * Injectable completer that allows signaling completion of an asynchronous test. Used internally.
 */
export class AsyncTestCompleter {
    constructor() {
        this._promise = new Promise((/**
         * @param {?} res
         * @param {?} rej
         * @return {?}
         */
        (res, rej) => {
            this._resolve = res;
            this._reject = rej;
        }));
    }
    /**
     * @param {?=} value
     * @return {?}
     */
    done(value) { this._resolve(value); }
    /**
     * @param {?=} error
     * @param {?=} stackTrace
     * @return {?}
     */
    fail(error, stackTrace) { this._reject(error); }
    /**
     * @return {?}
     */
    get promise() { return this._promise; }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    AsyncTestCompleter.prototype._resolve;
    /**
     * @type {?}
     * @private
     */
    AsyncTestCompleter.prototype._reject;
    /**
     * @type {?}
     * @private
     */
    AsyncTestCompleter.prototype._promise;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXN5bmNfdGVzdF9jb21wbGV0ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3Rlc3Rpbmcvc3JjL2FzeW5jX3Rlc3RfY29tcGxldGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7O0FBV0EsTUFBTSxPQUFPLGtCQUFrQjtJQUEvQjtRQUtVLGFBQVEsR0FBaUIsSUFBSSxPQUFPOzs7OztRQUFDLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxFQUFFO1lBQ3hELElBQUksQ0FBQyxRQUFRLEdBQUcsR0FBRyxDQUFDO1lBQ3BCLElBQUksQ0FBQyxPQUFPLEdBQUcsR0FBRyxDQUFDO1FBQ3JCLENBQUMsRUFBQyxDQUFDO0lBTUwsQ0FBQzs7Ozs7SUFMQyxJQUFJLENBQUMsS0FBVyxJQUFJLElBQUksQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFFM0MsSUFBSSxDQUFDLEtBQVcsRUFBRSxVQUFtQixJQUFJLElBQUksQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7O0lBRS9ELElBQUksT0FBTyxLQUFtQixPQUFPLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDO0NBQ3REOzs7Ozs7SUFaQyxzQ0FBMEM7Ozs7O0lBRTFDLHFDQUFzQzs7Ozs7SUFDdEMsc0NBR0ciLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8qKlxuICogSW5qZWN0YWJsZSBjb21wbGV0ZXIgdGhhdCBhbGxvd3Mgc2lnbmFsaW5nIGNvbXBsZXRpb24gb2YgYW4gYXN5bmNocm9ub3VzIHRlc3QuIFVzZWQgaW50ZXJuYWxseS5cbiAqL1xuZXhwb3J0IGNsYXNzIEFzeW5jVGVzdENvbXBsZXRlciB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9yZXNvbHZlICE6IChyZXN1bHQ6IGFueSkgPT4gdm9pZDtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX3JlamVjdCAhOiAoZXJyOiBhbnkpID0+IHZvaWQ7XG4gIHByaXZhdGUgX3Byb21pc2U6IFByb21pc2U8YW55PiA9IG5ldyBQcm9taXNlKChyZXMsIHJlaikgPT4ge1xuICAgIHRoaXMuX3Jlc29sdmUgPSByZXM7XG4gICAgdGhpcy5fcmVqZWN0ID0gcmVqO1xuICB9KTtcbiAgZG9uZSh2YWx1ZT86IGFueSkgeyB0aGlzLl9yZXNvbHZlKHZhbHVlKTsgfVxuXG4gIGZhaWwoZXJyb3I/OiBhbnksIHN0YWNrVHJhY2U/OiBzdHJpbmcpIHsgdGhpcy5fcmVqZWN0KGVycm9yKTsgfVxuXG4gIGdldCBwcm9taXNlKCk6IFByb21pc2U8YW55PiB7IHJldHVybiB0aGlzLl9wcm9taXNlOyB9XG59XG4iXX0=