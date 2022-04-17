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
 * @param {?} validator
 * @return {?}
 */
export function normalizeValidator(validator) {
    if (((/** @type {?} */ (validator))).validate) {
        return (/**
         * @param {?} c
         * @return {?}
         */
        (c) => ((/** @type {?} */ (validator))).validate(c));
    }
    else {
        return (/** @type {?} */ (validator));
    }
}
/**
 * @param {?} validator
 * @return {?}
 */
export function normalizeAsyncValidator(validator) {
    if (((/** @type {?} */ (validator))).validate) {
        return (/**
         * @param {?} c
         * @return {?}
         */
        (c) => ((/** @type {?} */ (validator))).validate(c));
    }
    else {
        return (/** @type {?} */ (validator));
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibm9ybWFsaXplX3ZhbGlkYXRvci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2Zvcm1zL3NyYy9kaXJlY3RpdmVzL25vcm1hbGl6ZV92YWxpZGF0b3IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7O0FBV0EsTUFBTSxVQUFVLGtCQUFrQixDQUFDLFNBQWtDO0lBQ25FLElBQUksQ0FBQyxtQkFBVyxTQUFTLEVBQUEsQ0FBQyxDQUFDLFFBQVEsRUFBRTtRQUNuQzs7OztRQUFPLENBQUMsQ0FBa0IsRUFBRSxFQUFFLENBQUMsQ0FBQyxtQkFBVyxTQUFTLEVBQUEsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUMsRUFBQztLQUNuRTtTQUFNO1FBQ0wsT0FBTyxtQkFBYSxTQUFTLEVBQUEsQ0FBQztLQUMvQjtBQUNILENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLHVCQUF1QixDQUFDLFNBQTRDO0lBRWxGLElBQUksQ0FBQyxtQkFBZ0IsU0FBUyxFQUFBLENBQUMsQ0FBQyxRQUFRLEVBQUU7UUFDeEM7Ozs7UUFBTyxDQUFDLENBQWtCLEVBQUUsRUFBRSxDQUFDLENBQUMsbUJBQWdCLFNBQVMsRUFBQSxDQUFDLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxFQUFDO0tBQ3hFO1NBQU07UUFDTCxPQUFPLG1CQUFrQixTQUFTLEVBQUEsQ0FBQztLQUNwQztBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7QWJzdHJhY3RDb250cm9sfSBmcm9tICcuLi9tb2RlbCc7XG5pbXBvcnQge0FzeW5jVmFsaWRhdG9yLCBBc3luY1ZhbGlkYXRvckZuLCBWYWxpZGF0b3IsIFZhbGlkYXRvckZufSBmcm9tICcuL3ZhbGlkYXRvcnMnO1xuXG5leHBvcnQgZnVuY3Rpb24gbm9ybWFsaXplVmFsaWRhdG9yKHZhbGlkYXRvcjogVmFsaWRhdG9yRm4gfCBWYWxpZGF0b3IpOiBWYWxpZGF0b3JGbiB7XG4gIGlmICgoPFZhbGlkYXRvcj52YWxpZGF0b3IpLnZhbGlkYXRlKSB7XG4gICAgcmV0dXJuIChjOiBBYnN0cmFjdENvbnRyb2wpID0+ICg8VmFsaWRhdG9yPnZhbGlkYXRvcikudmFsaWRhdGUoYyk7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIDxWYWxpZGF0b3JGbj52YWxpZGF0b3I7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG5vcm1hbGl6ZUFzeW5jVmFsaWRhdG9yKHZhbGlkYXRvcjogQXN5bmNWYWxpZGF0b3JGbiB8IEFzeW5jVmFsaWRhdG9yKTpcbiAgICBBc3luY1ZhbGlkYXRvckZuIHtcbiAgaWYgKCg8QXN5bmNWYWxpZGF0b3I+dmFsaWRhdG9yKS52YWxpZGF0ZSkge1xuICAgIHJldHVybiAoYzogQWJzdHJhY3RDb250cm9sKSA9PiAoPEFzeW5jVmFsaWRhdG9yPnZhbGlkYXRvcikudmFsaWRhdGUoYyk7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIDxBc3luY1ZhbGlkYXRvckZuPnZhbGlkYXRvcjtcbiAgfVxufVxuIl19