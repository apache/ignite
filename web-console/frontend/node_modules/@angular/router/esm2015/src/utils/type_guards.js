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
import { UrlTree } from '../url_tree';
/**
 * Simple function check, but generic so type inference will flow. Example:
 *
 * function product(a: number, b: number) {
 *   return a * b;
 * }
 *
 * if (isFunction<product>(fn)) {
 *   return fn(1, 2);
 * } else {
 *   throw "Must provide the `product` function";
 * }
 * @template T
 * @param {?} v
 * @return {?}
 */
export function isFunction(v) {
    return typeof v === 'function';
}
/**
 * @param {?} v
 * @return {?}
 */
export function isBoolean(v) {
    return typeof v === 'boolean';
}
/**
 * @param {?} v
 * @return {?}
 */
export function isUrlTree(v) {
    return v instanceof UrlTree;
}
/**
 * @param {?} guard
 * @return {?}
 */
export function isCanLoad(guard) {
    return guard && isFunction(guard.canLoad);
}
/**
 * @param {?} guard
 * @return {?}
 */
export function isCanActivate(guard) {
    return guard && isFunction(guard.canActivate);
}
/**
 * @param {?} guard
 * @return {?}
 */
export function isCanActivateChild(guard) {
    return guard && isFunction(guard.canActivateChild);
}
/**
 * @template T
 * @param {?} guard
 * @return {?}
 */
export function isCanDeactivate(guard) {
    return guard && isFunction(guard.canDeactivate);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHlwZV9ndWFyZHMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9yb3V0ZXIvc3JjL3V0aWxzL3R5cGVfZ3VhcmRzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUFDLE9BQU8sRUFBQyxNQUFNLGFBQWEsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFlcEMsTUFBTSxVQUFVLFVBQVUsQ0FBSSxDQUFNO0lBQ2xDLE9BQU8sT0FBTyxDQUFDLEtBQUssVUFBVSxDQUFDO0FBQ2pDLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLFNBQVMsQ0FBQyxDQUFNO0lBQzlCLE9BQU8sT0FBTyxDQUFDLEtBQUssU0FBUyxDQUFDO0FBQ2hDLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLFNBQVMsQ0FBQyxDQUFNO0lBQzlCLE9BQU8sQ0FBQyxZQUFZLE9BQU8sQ0FBQztBQUM5QixDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxTQUFTLENBQUMsS0FBVTtJQUNsQyxPQUFPLEtBQUssSUFBSSxVQUFVLENBQVUsS0FBSyxDQUFDLE9BQU8sQ0FBQyxDQUFDO0FBQ3JELENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLGFBQWEsQ0FBQyxLQUFVO0lBQ3RDLE9BQU8sS0FBSyxJQUFJLFVBQVUsQ0FBYyxLQUFLLENBQUMsV0FBVyxDQUFDLENBQUM7QUFDN0QsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsa0JBQWtCLENBQUMsS0FBVTtJQUMzQyxPQUFPLEtBQUssSUFBSSxVQUFVLENBQW1CLEtBQUssQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDO0FBQ3ZFLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxlQUFlLENBQUksS0FBVTtJQUMzQyxPQUFPLEtBQUssSUFBSSxVQUFVLENBQW1CLEtBQUssQ0FBQyxhQUFhLENBQUMsQ0FBQztBQUNwRSxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NhbkFjdGl2YXRlLCBDYW5BY3RpdmF0ZUNoaWxkLCBDYW5EZWFjdGl2YXRlLCBDYW5Mb2FkfSBmcm9tICcuLi9pbnRlcmZhY2VzJztcbmltcG9ydCB7VXJsVHJlZX0gZnJvbSAnLi4vdXJsX3RyZWUnO1xuXG4vKipcbiAqIFNpbXBsZSBmdW5jdGlvbiBjaGVjaywgYnV0IGdlbmVyaWMgc28gdHlwZSBpbmZlcmVuY2Ugd2lsbCBmbG93LiBFeGFtcGxlOlxuICpcbiAqIGZ1bmN0aW9uIHByb2R1Y3QoYTogbnVtYmVyLCBiOiBudW1iZXIpIHtcbiAqICAgcmV0dXJuIGEgKiBiO1xuICogfVxuICpcbiAqIGlmIChpc0Z1bmN0aW9uPHByb2R1Y3Q+KGZuKSkge1xuICogICByZXR1cm4gZm4oMSwgMik7XG4gKiB9IGVsc2Uge1xuICogICB0aHJvdyBcIk11c3QgcHJvdmlkZSB0aGUgYHByb2R1Y3RgIGZ1bmN0aW9uXCI7XG4gKiB9XG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc0Z1bmN0aW9uPFQ+KHY6IGFueSk6IHYgaXMgVCB7XG4gIHJldHVybiB0eXBlb2YgdiA9PT0gJ2Z1bmN0aW9uJztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQm9vbGVhbih2OiBhbnkpOiB2IGlzIGJvb2xlYW4ge1xuICByZXR1cm4gdHlwZW9mIHYgPT09ICdib29sZWFuJztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzVXJsVHJlZSh2OiBhbnkpOiB2IGlzIFVybFRyZWUge1xuICByZXR1cm4gdiBpbnN0YW5jZW9mIFVybFRyZWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0NhbkxvYWQoZ3VhcmQ6IGFueSk6IGd1YXJkIGlzIENhbkxvYWQge1xuICByZXR1cm4gZ3VhcmQgJiYgaXNGdW5jdGlvbjxDYW5Mb2FkPihndWFyZC5jYW5Mb2FkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQ2FuQWN0aXZhdGUoZ3VhcmQ6IGFueSk6IGd1YXJkIGlzIENhbkFjdGl2YXRlIHtcbiAgcmV0dXJuIGd1YXJkICYmIGlzRnVuY3Rpb248Q2FuQWN0aXZhdGU+KGd1YXJkLmNhbkFjdGl2YXRlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQ2FuQWN0aXZhdGVDaGlsZChndWFyZDogYW55KTogZ3VhcmQgaXMgQ2FuQWN0aXZhdGVDaGlsZCB7XG4gIHJldHVybiBndWFyZCAmJiBpc0Z1bmN0aW9uPENhbkFjdGl2YXRlQ2hpbGQ+KGd1YXJkLmNhbkFjdGl2YXRlQ2hpbGQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNDYW5EZWFjdGl2YXRlPFQ+KGd1YXJkOiBhbnkpOiBndWFyZCBpcyBDYW5EZWFjdGl2YXRlPFQ+IHtcbiAgcmV0dXJuIGd1YXJkICYmIGlzRnVuY3Rpb248Q2FuRGVhY3RpdmF0ZTxUPj4oZ3VhcmQuY2FuRGVhY3RpdmF0ZSk7XG59XG4iXX0=