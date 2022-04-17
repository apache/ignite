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
import { getDebugContext, getErrorLogger, getOriginalError } from './errors';
/**
 * Provides a hook for centralized exception handling.
 *
 * The default implementation of `ErrorHandler` prints error messages to the `console`. To
 * intercept error handling, write a custom exception handler that replaces this default as
 * appropriate for your app.
 *
 * \@usageNotes
 * ### Example
 *
 * ```
 * class MyErrorHandler implements ErrorHandler {
 *   handleError(error) {
 *     // do something with the exception
 *   }
 * }
 *
 * \@NgModule({
 *   providers: [{provide: ErrorHandler, useClass: MyErrorHandler}]
 * })
 * class MyModule {}
 * ```
 *
 * \@publicApi
 */
export class ErrorHandler {
    constructor() {
        /**
         * \@internal
         */
        this._console = console;
    }
    /**
     * @param {?} error
     * @return {?}
     */
    handleError(error) {
        /** @type {?} */
        const originalError = this._findOriginalError(error);
        /** @type {?} */
        const context = this._findContext(error);
        // Note: Browser consoles show the place from where console.error was called.
        // We can use this to give users additional information about the error.
        /** @type {?} */
        const errorLogger = getErrorLogger(error);
        errorLogger(this._console, `ERROR`, error);
        if (originalError) {
            errorLogger(this._console, `ORIGINAL ERROR`, originalError);
        }
        if (context) {
            errorLogger(this._console, 'ERROR CONTEXT', context);
        }
    }
    /**
     * \@internal
     * @param {?} error
     * @return {?}
     */
    _findContext(error) {
        if (error) {
            return getDebugContext(error) ? getDebugContext(error) :
                this._findContext(getOriginalError(error));
        }
        return null;
    }
    /**
     * \@internal
     * @param {?} error
     * @return {?}
     */
    _findOriginalError(error) {
        /** @type {?} */
        let e = getOriginalError(error);
        while (e && getOriginalError(e)) {
            e = getOriginalError(e);
        }
        return e;
    }
}
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    ErrorHandler.prototype._console;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXJyb3JfaGFuZGxlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2Vycm9yX2hhbmRsZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsZUFBZSxFQUFFLGNBQWMsRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLFVBQVUsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUE2QjNFLE1BQU0sT0FBTyxZQUFZO0lBQXpCOzs7O1FBSUUsYUFBUSxHQUFZLE9BQU8sQ0FBQztJQXFDOUIsQ0FBQzs7Ozs7SUFuQ0MsV0FBVyxDQUFDLEtBQVU7O2NBQ2QsYUFBYSxHQUFHLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxLQUFLLENBQUM7O2NBQzlDLE9BQU8sR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLEtBQUssQ0FBQzs7OztjQUdsQyxXQUFXLEdBQUcsY0FBYyxDQUFDLEtBQUssQ0FBQztRQUV6QyxXQUFXLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRSxPQUFPLEVBQUUsS0FBSyxDQUFDLENBQUM7UUFDM0MsSUFBSSxhQUFhLEVBQUU7WUFDakIsV0FBVyxDQUFDLElBQUksQ0FBQyxRQUFRLEVBQUUsZ0JBQWdCLEVBQUUsYUFBYSxDQUFDLENBQUM7U0FDN0Q7UUFDRCxJQUFJLE9BQU8sRUFBRTtZQUNYLFdBQVcsQ0FBQyxJQUFJLENBQUMsUUFBUSxFQUFFLGVBQWUsRUFBRSxPQUFPLENBQUMsQ0FBQztTQUN0RDtJQUNILENBQUM7Ozs7OztJQUdELFlBQVksQ0FBQyxLQUFVO1FBQ3JCLElBQUksS0FBSyxFQUFFO1lBQ1QsT0FBTyxlQUFlLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLGVBQWUsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDO2dCQUN4QixJQUFJLENBQUMsWUFBWSxDQUFDLGdCQUFnQixDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUM7U0FDNUU7UUFFRCxPQUFPLElBQUksQ0FBQztJQUNkLENBQUM7Ozs7OztJQUdELGtCQUFrQixDQUFDLEtBQVk7O1lBQ3pCLENBQUMsR0FBRyxnQkFBZ0IsQ0FBQyxLQUFLLENBQUM7UUFDL0IsT0FBTyxDQUFDLElBQUksZ0JBQWdCLENBQUMsQ0FBQyxDQUFDLEVBQUU7WUFDL0IsQ0FBQyxHQUFHLGdCQUFnQixDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQ3pCO1FBRUQsT0FBTyxDQUFDLENBQUM7SUFDWCxDQUFDO0NBQ0Y7Ozs7OztJQXJDQyxnQ0FBNEIiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Z2V0RGVidWdDb250ZXh0LCBnZXRFcnJvckxvZ2dlciwgZ2V0T3JpZ2luYWxFcnJvcn0gZnJvbSAnLi9lcnJvcnMnO1xuXG5cblxuLyoqXG4gKiBQcm92aWRlcyBhIGhvb2sgZm9yIGNlbnRyYWxpemVkIGV4Y2VwdGlvbiBoYW5kbGluZy5cbiAqXG4gKiBUaGUgZGVmYXVsdCBpbXBsZW1lbnRhdGlvbiBvZiBgRXJyb3JIYW5kbGVyYCBwcmludHMgZXJyb3IgbWVzc2FnZXMgdG8gdGhlIGBjb25zb2xlYC4gVG9cbiAqIGludGVyY2VwdCBlcnJvciBoYW5kbGluZywgd3JpdGUgYSBjdXN0b20gZXhjZXB0aW9uIGhhbmRsZXIgdGhhdCByZXBsYWNlcyB0aGlzIGRlZmF1bHQgYXNcbiAqIGFwcHJvcHJpYXRlIGZvciB5b3VyIGFwcC5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogIyMjIEV4YW1wbGVcbiAqXG4gKiBgYGBcbiAqIGNsYXNzIE15RXJyb3JIYW5kbGVyIGltcGxlbWVudHMgRXJyb3JIYW5kbGVyIHtcbiAqICAgaGFuZGxlRXJyb3IoZXJyb3IpIHtcbiAqICAgICAvLyBkbyBzb21ldGhpbmcgd2l0aCB0aGUgZXhjZXB0aW9uXG4gKiAgIH1cbiAqIH1cbiAqXG4gKiBATmdNb2R1bGUoe1xuICogICBwcm92aWRlcnM6IFt7cHJvdmlkZTogRXJyb3JIYW5kbGVyLCB1c2VDbGFzczogTXlFcnJvckhhbmRsZXJ9XVxuICogfSlcbiAqIGNsYXNzIE15TW9kdWxlIHt9XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBFcnJvckhhbmRsZXIge1xuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICBfY29uc29sZTogQ29uc29sZSA9IGNvbnNvbGU7XG5cbiAgaGFuZGxlRXJyb3IoZXJyb3I6IGFueSk6IHZvaWQge1xuICAgIGNvbnN0IG9yaWdpbmFsRXJyb3IgPSB0aGlzLl9maW5kT3JpZ2luYWxFcnJvcihlcnJvcik7XG4gICAgY29uc3QgY29udGV4dCA9IHRoaXMuX2ZpbmRDb250ZXh0KGVycm9yKTtcbiAgICAvLyBOb3RlOiBCcm93c2VyIGNvbnNvbGVzIHNob3cgdGhlIHBsYWNlIGZyb20gd2hlcmUgY29uc29sZS5lcnJvciB3YXMgY2FsbGVkLlxuICAgIC8vIFdlIGNhbiB1c2UgdGhpcyB0byBnaXZlIHVzZXJzIGFkZGl0aW9uYWwgaW5mb3JtYXRpb24gYWJvdXQgdGhlIGVycm9yLlxuICAgIGNvbnN0IGVycm9yTG9nZ2VyID0gZ2V0RXJyb3JMb2dnZXIoZXJyb3IpO1xuXG4gICAgZXJyb3JMb2dnZXIodGhpcy5fY29uc29sZSwgYEVSUk9SYCwgZXJyb3IpO1xuICAgIGlmIChvcmlnaW5hbEVycm9yKSB7XG4gICAgICBlcnJvckxvZ2dlcih0aGlzLl9jb25zb2xlLCBgT1JJR0lOQUwgRVJST1JgLCBvcmlnaW5hbEVycm9yKTtcbiAgICB9XG4gICAgaWYgKGNvbnRleHQpIHtcbiAgICAgIGVycm9yTG9nZ2VyKHRoaXMuX2NvbnNvbGUsICdFUlJPUiBDT05URVhUJywgY29udGV4dCk7XG4gICAgfVxuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfZmluZENvbnRleHQoZXJyb3I6IGFueSk6IGFueSB7XG4gICAgaWYgKGVycm9yKSB7XG4gICAgICByZXR1cm4gZ2V0RGVidWdDb250ZXh0KGVycm9yKSA/IGdldERlYnVnQ29udGV4dChlcnJvcikgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLl9maW5kQ29udGV4dChnZXRPcmlnaW5hbEVycm9yKGVycm9yKSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9maW5kT3JpZ2luYWxFcnJvcihlcnJvcjogRXJyb3IpOiBhbnkge1xuICAgIGxldCBlID0gZ2V0T3JpZ2luYWxFcnJvcihlcnJvcik7XG4gICAgd2hpbGUgKGUgJiYgZ2V0T3JpZ2luYWxFcnJvcihlKSkge1xuICAgICAgZSA9IGdldE9yaWdpbmFsRXJyb3IoZSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIGU7XG4gIH1cbn1cbiJdfQ==