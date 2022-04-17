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
/** @type {?} */
export const PLATFORM_BROWSER_ID = 'browser';
/** @type {?} */
export const PLATFORM_SERVER_ID = 'server';
/** @type {?} */
export const PLATFORM_WORKER_APP_ID = 'browserWorkerApp';
/** @type {?} */
export const PLATFORM_WORKER_UI_ID = 'browserWorkerUi';
/**
 * Returns whether a platform id represents a browser platform.
 * \@publicApi
 * @param {?} platformId
 * @return {?}
 */
export function isPlatformBrowser(platformId) {
    return platformId === PLATFORM_BROWSER_ID;
}
/**
 * Returns whether a platform id represents a server platform.
 * \@publicApi
 * @param {?} platformId
 * @return {?}
 */
export function isPlatformServer(platformId) {
    return platformId === PLATFORM_SERVER_ID;
}
/**
 * Returns whether a platform id represents a web worker app platform.
 * \@publicApi
 * @param {?} platformId
 * @return {?}
 */
export function isPlatformWorkerApp(platformId) {
    return platformId === PLATFORM_WORKER_APP_ID;
}
/**
 * Returns whether a platform id represents a web worker UI platform.
 * \@publicApi
 * @param {?} platformId
 * @return {?}
 */
export function isPlatformWorkerUi(platformId) {
    return platformId === PLATFORM_WORKER_UI_ID;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGxhdGZvcm1faWQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vc3JjL3BsYXRmb3JtX2lkLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7OztBQVFBLE1BQU0sT0FBTyxtQkFBbUIsR0FBRyxTQUFTOztBQUM1QyxNQUFNLE9BQU8sa0JBQWtCLEdBQUcsUUFBUTs7QUFDMUMsTUFBTSxPQUFPLHNCQUFzQixHQUFHLGtCQUFrQjs7QUFDeEQsTUFBTSxPQUFPLHFCQUFxQixHQUFHLGlCQUFpQjs7Ozs7OztBQU10RCxNQUFNLFVBQVUsaUJBQWlCLENBQUMsVUFBa0I7SUFDbEQsT0FBTyxVQUFVLEtBQUssbUJBQW1CLENBQUM7QUFDNUMsQ0FBQzs7Ozs7OztBQU1ELE1BQU0sVUFBVSxnQkFBZ0IsQ0FBQyxVQUFrQjtJQUNqRCxPQUFPLFVBQVUsS0FBSyxrQkFBa0IsQ0FBQztBQUMzQyxDQUFDOzs7Ozs7O0FBTUQsTUFBTSxVQUFVLG1CQUFtQixDQUFDLFVBQWtCO0lBQ3BELE9BQU8sVUFBVSxLQUFLLHNCQUFzQixDQUFDO0FBQy9DLENBQUM7Ozs7Ozs7QUFNRCxNQUFNLFVBQVUsa0JBQWtCLENBQUMsVUFBa0I7SUFDbkQsT0FBTyxVQUFVLEtBQUsscUJBQXFCLENBQUM7QUFDOUMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuZXhwb3J0IGNvbnN0IFBMQVRGT1JNX0JST1dTRVJfSUQgPSAnYnJvd3Nlcic7XG5leHBvcnQgY29uc3QgUExBVEZPUk1fU0VSVkVSX0lEID0gJ3NlcnZlcic7XG5leHBvcnQgY29uc3QgUExBVEZPUk1fV09SS0VSX0FQUF9JRCA9ICdicm93c2VyV29ya2VyQXBwJztcbmV4cG9ydCBjb25zdCBQTEFURk9STV9XT1JLRVJfVUlfSUQgPSAnYnJvd3NlcldvcmtlclVpJztcblxuLyoqXG4gKiBSZXR1cm5zIHdoZXRoZXIgYSBwbGF0Zm9ybSBpZCByZXByZXNlbnRzIGEgYnJvd3NlciBwbGF0Zm9ybS5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzUGxhdGZvcm1Ccm93c2VyKHBsYXRmb3JtSWQ6IE9iamVjdCk6IGJvb2xlYW4ge1xuICByZXR1cm4gcGxhdGZvcm1JZCA9PT0gUExBVEZPUk1fQlJPV1NFUl9JRDtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHdoZXRoZXIgYSBwbGF0Zm9ybSBpZCByZXByZXNlbnRzIGEgc2VydmVyIHBsYXRmb3JtLlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNQbGF0Zm9ybVNlcnZlcihwbGF0Zm9ybUlkOiBPYmplY3QpOiBib29sZWFuIHtcbiAgcmV0dXJuIHBsYXRmb3JtSWQgPT09IFBMQVRGT1JNX1NFUlZFUl9JRDtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHdoZXRoZXIgYSBwbGF0Zm9ybSBpZCByZXByZXNlbnRzIGEgd2ViIHdvcmtlciBhcHAgcGxhdGZvcm0uXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc1BsYXRmb3JtV29ya2VyQXBwKHBsYXRmb3JtSWQ6IE9iamVjdCk6IGJvb2xlYW4ge1xuICByZXR1cm4gcGxhdGZvcm1JZCA9PT0gUExBVEZPUk1fV09SS0VSX0FQUF9JRDtcbn1cblxuLyoqXG4gKiBSZXR1cm5zIHdoZXRoZXIgYSBwbGF0Zm9ybSBpZCByZXByZXNlbnRzIGEgd2ViIHdvcmtlciBVSSBwbGF0Zm9ybS5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzUGxhdGZvcm1Xb3JrZXJVaShwbGF0Zm9ybUlkOiBPYmplY3QpOiBib29sZWFuIHtcbiAgcmV0dXJuIHBsYXRmb3JtSWQgPT09IFBMQVRGT1JNX1dPUktFUl9VSV9JRDtcbn1cbiJdfQ==