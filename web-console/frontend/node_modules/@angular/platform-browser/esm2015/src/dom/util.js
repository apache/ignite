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
import { ɵglobal as global } from '@angular/core';
/** @type {?} */
const CAMEL_CASE_REGEXP = /([A-Z])/g;
/** @type {?} */
const DASH_CASE_REGEXP = /-([a-z])/g;
/**
 * @param {?} input
 * @return {?}
 */
export function camelCaseToDashCase(input) {
    return input.replace(CAMEL_CASE_REGEXP, (/**
     * @param {...?} m
     * @return {?}
     */
    (...m) => '-' + m[1].toLowerCase()));
}
/**
 * @param {?} input
 * @return {?}
 */
export function dashCaseToCamelCase(input) {
    return input.replace(DASH_CASE_REGEXP, (/**
     * @param {...?} m
     * @return {?}
     */
    (...m) => m[1].toUpperCase()));
}
/**
 * Exports the value under a given `name` in the global property `ng`. For example `ng.probe` if
 * `name` is `'probe'`.
 * @param {?} name Name under which it will be exported. Keep in mind this will be a property of the
 * global `ng` object.
 * @param {?} value The value to export.
 * @return {?}
 */
export function exportNgVar(name, value) {
    if (typeof COMPILED === 'undefined' || !COMPILED) {
        // Note: we can't export `ng` when using closure enhanced optimization as:
        // - closure declares globals itself for minified names, which sometimes clobber our `ng` global
        // - we can't declare a closure extern as the namespace `ng` is already used within Google
        //   for typings for angularJS (via `goog.provide('ng....')`).
        /** @type {?} */
        const ng = global['ng'] = ((/** @type {?} */ (global['ng']))) || {};
        ng[name] = value;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXIvc3JjL2RvbS91dGlsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLE9BQU8sSUFBSSxNQUFNLEVBQUMsTUFBTSxlQUFlLENBQUM7O01BRTFDLGlCQUFpQixHQUFHLFVBQVU7O01BQzlCLGdCQUFnQixHQUFHLFdBQVc7Ozs7O0FBR3BDLE1BQU0sVUFBVSxtQkFBbUIsQ0FBQyxLQUFhO0lBQy9DLE9BQU8sS0FBSyxDQUFDLE9BQU8sQ0FBQyxpQkFBaUI7Ozs7SUFBRSxDQUFDLEdBQUcsQ0FBVyxFQUFFLEVBQUUsQ0FBQyxHQUFHLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLFdBQVcsRUFBRSxFQUFDLENBQUM7QUFDeEYsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsbUJBQW1CLENBQUMsS0FBYTtJQUMvQyxPQUFPLEtBQUssQ0FBQyxPQUFPLENBQUMsZ0JBQWdCOzs7O0lBQUUsQ0FBQyxHQUFHLENBQVcsRUFBRSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLFdBQVcsRUFBRSxFQUFDLENBQUM7QUFDakYsQ0FBQzs7Ozs7Ozs7O0FBU0QsTUFBTSxVQUFVLFdBQVcsQ0FBQyxJQUFZLEVBQUUsS0FBVTtJQUNsRCxJQUFJLE9BQU8sUUFBUSxLQUFLLFdBQVcsSUFBSSxDQUFDLFFBQVEsRUFBRTs7Ozs7O2NBSzFDLEVBQUUsR0FBRyxNQUFNLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxtQkFBQSxNQUFNLENBQUMsSUFBSSxDQUFDLEVBQW1DLENBQUMsSUFBSSxFQUFFO1FBQ2pGLEVBQUUsQ0FBQyxJQUFJLENBQUMsR0FBRyxLQUFLLENBQUM7S0FDbEI7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge8m1Z2xvYmFsIGFzIGdsb2JhbH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmNvbnN0IENBTUVMX0NBU0VfUkVHRVhQID0gLyhbQS1aXSkvZztcbmNvbnN0IERBU0hfQ0FTRV9SRUdFWFAgPSAvLShbYS16XSkvZztcblxuXG5leHBvcnQgZnVuY3Rpb24gY2FtZWxDYXNlVG9EYXNoQ2FzZShpbnB1dDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGlucHV0LnJlcGxhY2UoQ0FNRUxfQ0FTRV9SRUdFWFAsICguLi5tOiBzdHJpbmdbXSkgPT4gJy0nICsgbVsxXS50b0xvd2VyQ2FzZSgpKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRhc2hDYXNlVG9DYW1lbENhc2UoaW5wdXQ6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBpbnB1dC5yZXBsYWNlKERBU0hfQ0FTRV9SRUdFWFAsICguLi5tOiBzdHJpbmdbXSkgPT4gbVsxXS50b1VwcGVyQ2FzZSgpKTtcbn1cblxuLyoqXG4gKiBFeHBvcnRzIHRoZSB2YWx1ZSB1bmRlciBhIGdpdmVuIGBuYW1lYCBpbiB0aGUgZ2xvYmFsIHByb3BlcnR5IGBuZ2AuIEZvciBleGFtcGxlIGBuZy5wcm9iZWAgaWZcbiAqIGBuYW1lYCBpcyBgJ3Byb2JlJ2AuXG4gKiBAcGFyYW0gbmFtZSBOYW1lIHVuZGVyIHdoaWNoIGl0IHdpbGwgYmUgZXhwb3J0ZWQuIEtlZXAgaW4gbWluZCB0aGlzIHdpbGwgYmUgYSBwcm9wZXJ0eSBvZiB0aGVcbiAqIGdsb2JhbCBgbmdgIG9iamVjdC5cbiAqIEBwYXJhbSB2YWx1ZSBUaGUgdmFsdWUgdG8gZXhwb3J0LlxuICovXG5leHBvcnQgZnVuY3Rpb24gZXhwb3J0TmdWYXIobmFtZTogc3RyaW5nLCB2YWx1ZTogYW55KTogdm9pZCB7XG4gIGlmICh0eXBlb2YgQ09NUElMRUQgPT09ICd1bmRlZmluZWQnIHx8ICFDT01QSUxFRCkge1xuICAgIC8vIE5vdGU6IHdlIGNhbid0IGV4cG9ydCBgbmdgIHdoZW4gdXNpbmcgY2xvc3VyZSBlbmhhbmNlZCBvcHRpbWl6YXRpb24gYXM6XG4gICAgLy8gLSBjbG9zdXJlIGRlY2xhcmVzIGdsb2JhbHMgaXRzZWxmIGZvciBtaW5pZmllZCBuYW1lcywgd2hpY2ggc29tZXRpbWVzIGNsb2JiZXIgb3VyIGBuZ2AgZ2xvYmFsXG4gICAgLy8gLSB3ZSBjYW4ndCBkZWNsYXJlIGEgY2xvc3VyZSBleHRlcm4gYXMgdGhlIG5hbWVzcGFjZSBgbmdgIGlzIGFscmVhZHkgdXNlZCB3aXRoaW4gR29vZ2xlXG4gICAgLy8gICBmb3IgdHlwaW5ncyBmb3IgYW5ndWxhckpTICh2aWEgYGdvb2cucHJvdmlkZSgnbmcuLi4uJylgKS5cbiAgICBjb25zdCBuZyA9IGdsb2JhbFsnbmcnXSA9IChnbG9iYWxbJ25nJ10gYXN7W2tleTogc3RyaW5nXTogYW55fSB8IHVuZGVmaW5lZCkgfHwge307XG4gICAgbmdbbmFtZV0gPSB2YWx1ZTtcbiAgfVxufVxuIl19