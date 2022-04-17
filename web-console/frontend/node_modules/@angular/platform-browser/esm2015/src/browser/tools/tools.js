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
import { exportNgVar } from '../../dom/util';
import { AngularProfiler } from './common_tools';
/** @type {?} */
const PROFILER_GLOBAL_NAME = 'profiler';
/**
 * Enabled Angular debug tools that are accessible via your browser's
 * developer console.
 *
 * Usage:
 *
 * 1. Open developer console (e.g. in Chrome Ctrl + Shift + j)
 * 1. Type `ng.` (usually the console will show auto-complete suggestion)
 * 1. Try the change detection profiler `ng.profiler.timeChangeDetection()`
 *    then hit Enter.
 *
 * \@publicApi
 * @template T
 * @param {?} ref
 * @return {?}
 */
export function enableDebugTools(ref) {
    exportNgVar(PROFILER_GLOBAL_NAME, new AngularProfiler(ref));
    return ref;
}
/**
 * Disables Angular tools.
 *
 * \@publicApi
 * @return {?}
 */
export function disableDebugTools() {
    exportNgVar(PROFILER_GLOBAL_NAME, null);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidG9vbHMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9wbGF0Zm9ybS1icm93c2VyL3NyYy9icm93c2VyL3Rvb2xzL3Rvb2xzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBQzNDLE9BQU8sRUFBQyxlQUFlLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQzs7TUFFekMsb0JBQW9CLEdBQUcsVUFBVTs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFldkMsTUFBTSxVQUFVLGdCQUFnQixDQUFJLEdBQW9CO0lBQ3RELFdBQVcsQ0FBQyxvQkFBb0IsRUFBRSxJQUFJLGVBQWUsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDO0lBQzVELE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQzs7Ozs7OztBQU9ELE1BQU0sVUFBVSxpQkFBaUI7SUFDL0IsV0FBVyxDQUFDLG9CQUFvQixFQUFFLElBQUksQ0FBQyxDQUFDO0FBQzFDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcG9uZW50UmVmfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7ZXhwb3J0TmdWYXJ9IGZyb20gJy4uLy4uL2RvbS91dGlsJztcbmltcG9ydCB7QW5ndWxhclByb2ZpbGVyfSBmcm9tICcuL2NvbW1vbl90b29scyc7XG5cbmNvbnN0IFBST0ZJTEVSX0dMT0JBTF9OQU1FID0gJ3Byb2ZpbGVyJztcblxuLyoqXG4gKiBFbmFibGVkIEFuZ3VsYXIgZGVidWcgdG9vbHMgdGhhdCBhcmUgYWNjZXNzaWJsZSB2aWEgeW91ciBicm93c2VyJ3NcbiAqIGRldmVsb3BlciBjb25zb2xlLlxuICpcbiAqIFVzYWdlOlxuICpcbiAqIDEuIE9wZW4gZGV2ZWxvcGVyIGNvbnNvbGUgKGUuZy4gaW4gQ2hyb21lIEN0cmwgKyBTaGlmdCArIGopXG4gKiAxLiBUeXBlIGBuZy5gICh1c3VhbGx5IHRoZSBjb25zb2xlIHdpbGwgc2hvdyBhdXRvLWNvbXBsZXRlIHN1Z2dlc3Rpb24pXG4gKiAxLiBUcnkgdGhlIGNoYW5nZSBkZXRlY3Rpb24gcHJvZmlsZXIgYG5nLnByb2ZpbGVyLnRpbWVDaGFuZ2VEZXRlY3Rpb24oKWBcbiAqICAgIHRoZW4gaGl0IEVudGVyLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGVuYWJsZURlYnVnVG9vbHM8VD4ocmVmOiBDb21wb25lbnRSZWY8VD4pOiBDb21wb25lbnRSZWY8VD4ge1xuICBleHBvcnROZ1ZhcihQUk9GSUxFUl9HTE9CQUxfTkFNRSwgbmV3IEFuZ3VsYXJQcm9maWxlcihyZWYpKTtcbiAgcmV0dXJuIHJlZjtcbn1cblxuLyoqXG4gKiBEaXNhYmxlcyBBbmd1bGFyIHRvb2xzLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGRpc2FibGVEZWJ1Z1Rvb2xzKCk6IHZvaWQge1xuICBleHBvcnROZ1ZhcihQUk9GSUxFUl9HTE9CQUxfTkFNRSwgbnVsbCk7XG59XG4iXX0=