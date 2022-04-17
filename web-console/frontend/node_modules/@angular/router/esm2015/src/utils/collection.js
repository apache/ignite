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
import { ɵisObservable as isObservable, ɵisPromise as isPromise } from '@angular/core';
import { from, of } from 'rxjs';
import { concatAll, last as lastValue, map } from 'rxjs/operators';
import { PRIMARY_OUTLET } from '../shared';
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
export function shallowEqualArrays(a, b) {
    if (a.length !== b.length)
        return false;
    for (let i = 0; i < a.length; ++i) {
        if (!shallowEqual(a[i], b[i]))
            return false;
    }
    return true;
}
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
export function shallowEqual(a, b) {
    // Casting Object.keys return values to include `undefined` as there are some cases
    // in IE 11 where this can happen. Cannot provide a test because the behavior only
    // exists in certain circumstances in IE 11, therefore doing this cast ensures the
    // logic is correct for when this edge case is hit.
    /** @type {?} */
    const k1 = (/** @type {?} */ (Object.keys(a)));
    /** @type {?} */
    const k2 = (/** @type {?} */ (Object.keys(b)));
    if (!k1 || !k2 || k1.length != k2.length) {
        return false;
    }
    /** @type {?} */
    let key;
    for (let i = 0; i < k1.length; i++) {
        key = k1[i];
        if (a[key] !== b[key]) {
            return false;
        }
    }
    return true;
}
/**
 * Flattens single-level nested arrays.
 * @template T
 * @param {?} arr
 * @return {?}
 */
export function flatten(arr) {
    return Array.prototype.concat.apply([], arr);
}
/**
 * Return the last element of an array.
 * @template T
 * @param {?} a
 * @return {?}
 */
export function last(a) {
    return a.length > 0 ? a[a.length - 1] : null;
}
/**
 * Verifys all booleans in an array are `true`.
 * @param {?} bools
 * @return {?}
 */
export function and(bools) {
    return !bools.some((/**
     * @param {?} v
     * @return {?}
     */
    v => !v));
}
/**
 * @template K, V
 * @param {?} map
 * @param {?} callback
 * @return {?}
 */
export function forEach(map, callback) {
    for (const prop in map) {
        if (map.hasOwnProperty(prop)) {
            callback(map[prop], prop);
        }
    }
}
/**
 * @template A, B
 * @param {?} obj
 * @param {?} fn
 * @return {?}
 */
export function waitForMap(obj, fn) {
    if (Object.keys(obj).length === 0) {
        return of({});
    }
    /** @type {?} */
    const waitHead = [];
    /** @type {?} */
    const waitTail = [];
    /** @type {?} */
    const res = {};
    forEach(obj, (/**
     * @param {?} a
     * @param {?} k
     * @return {?}
     */
    (a, k) => {
        /** @type {?} */
        const mapped = fn(k, a).pipe(map((/**
         * @param {?} r
         * @return {?}
         */
        (r) => res[k] = r)));
        if (k === PRIMARY_OUTLET) {
            waitHead.push(mapped);
        }
        else {
            waitTail.push(mapped);
        }
    }));
    // Closure compiler has problem with using spread operator here. So we use "Array.concat".
    // Note that we also need to cast the new promise because TypeScript cannot infer the type
    // when calling the "of" function through "Function.apply"
    return ((/** @type {?} */ (of.apply(null, waitHead.concat(waitTail)))))
        .pipe(concatAll(), lastValue(), map((/**
     * @return {?}
     */
    () => res)));
}
/**
 * @template T
 * @param {?} value
 * @return {?}
 */
export function wrapIntoObservable(value) {
    if (isObservable(value)) {
        return value;
    }
    if (isPromise(value)) {
        // Use `Promise.resolve()` to wrap promise-like instances.
        // Required ie when a Resolver returns a AngularJS `$q` promise to correctly trigger the
        // change detection.
        return from(Promise.resolve(value));
    }
    return of(value);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29sbGVjdGlvbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvdXRpbHMvY29sbGVjdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBa0IsYUFBYSxJQUFJLFlBQVksRUFBRSxVQUFVLElBQUksU0FBUyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQ3RHLE9BQU8sRUFBYSxJQUFJLEVBQUUsRUFBRSxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQzNDLE9BQU8sRUFBQyxTQUFTLEVBQUUsSUFBSSxJQUFJLFNBQVMsRUFBRSxHQUFHLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUVqRSxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sV0FBVyxDQUFDOzs7Ozs7QUFFekMsTUFBTSxVQUFVLGtCQUFrQixDQUFDLENBQVEsRUFBRSxDQUFRO0lBQ25ELElBQUksQ0FBQyxDQUFDLE1BQU0sS0FBSyxDQUFDLENBQUMsTUFBTTtRQUFFLE9BQU8sS0FBSyxDQUFDO0lBQ3hDLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxDQUFDLENBQUMsTUFBTSxFQUFFLEVBQUUsQ0FBQyxFQUFFO1FBQ2pDLElBQUksQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztZQUFFLE9BQU8sS0FBSyxDQUFDO0tBQzdDO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsWUFBWSxDQUFDLENBQXFCLEVBQUUsQ0FBcUI7Ozs7OztVQUtqRSxFQUFFLEdBQUcsbUJBQUEsTUFBTSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsRUFBd0I7O1VBQzNDLEVBQUUsR0FBRyxtQkFBQSxNQUFNLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxFQUF3QjtJQUNqRCxJ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LFVBQVUsa0JBQWtCLENBQUksS0FBb0M7SUFDeEUsSUFBSSxZQUFZLENBQUMsS0FBSyxDQUFDLEVBQUU7UUFDdkIsT0FBTyxLQUFLLENBQUM7S0FDZDtJQUVELElBQUksU0FBUyxDQUFDLEtBQUssQ0FBQyxFQUFFO1FBQ3BCLDBEQUEwRDtRQUMxRCx3RkFBd0Y7UUFDeEYsb0JBQW9CO1FBQ3BCLE9BQU8sSUFBSSxDQUFDLE9BQU8sQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztLQUNyQztJQUVELE9BQU8sRUFBRSxDQUFFLEtBQUssQ0FBQyxDQUFDO0FBQ3BCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7TmdNb2R1bGVGYWN0b3J5LCDJtWlzT2JzZXJ2YWJsZSBhcyBpc09ic2VydmFibGUsIMm1aXNQcm9taXNlIGFzIGlzUHJvbWlzZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge09ic2VydmFibGUsIGZyb20sIG9mIH0gZnJvbSAncnhqcyc7XG5pbXBvcnQge2NvbmNhdEFsbCwgbGFzdCBhcyBsYXN0VmFsdWUsIG1hcH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuXG5pbXBvcnQge1BSSU1BUllfT1VUTEVUfSBmcm9tICcuLi9zaGFyZWQnO1xuXG5leHBvcnQgZnVuY3Rpb24gc2hhbGxvd0VxdWFsQXJyYXlzKGE6IGFueVtdLCBiOiBhbnlbXSk6IGJvb2xlYW4ge1xuICBpZiAoYS5sZW5ndGggIT09IGIubGVuZ3RoKSByZXR1cm4gZmFsc2U7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYS5sZW5ndGg7ICsraSkge1xuICAgIGlmICghc2hhbGxvd0VxdWFsKGFbaV0sIGJbaV0pKSByZXR1cm4gZmFsc2U7XG4gIH1cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzaGFsbG93RXF1YWwoYToge1t4OiBzdHJpbmddOiBhbnl9LCBiOiB7W3g6IHN0cmluZ106IGFueX0pOiBib29sZWFuIHtcbiAgLy8gQ2FzdGluZyBPYmplY3Qua2V5cyByZXR1cm4gdmFsdWVzIHRvIGluY2x1ZGUgYHVuZGVmaW5lZGAgYXMgdGhlcmUgYXJlIHNvbWUgY2FzZXNcbiAgLy8gaW4gSUUgMTEgd2hlcmUgdGhpcyBjYW4gaGFwcGVuLiBDYW5ub3QgcHJvdmlkZSBhIHRlc3QgYmVjYXVzZSB0aGUgYmVoYXZpb3Igb25seVxuICAvLyBleGlzdHMgaW4gY2VydGFpbiBjaXJjdW1zdGFuY2VzIGluIElFIDExLCB0aGVyZWZvcmUgZG9pbmcgdGhpcyBjYXN0IGVuc3VyZXMgdGhlXG4gIC8vIGxvZ2ljIGlzIGNvcnJlY3QgZm9yIHdoZW4gdGhpcyBlZGdlIGNhc2UgaXMgaGl0LlxuICBjb25zdCBrMSA9IE9iamVjdC5rZXlzKGEpIGFzIHN0cmluZ1tdIHwgdW5kZWZpbmVkO1xuICBjb25zdCBrMiA9IE9iamVjdC5rZXlzKGIpIGFzIHN0cmluZ1tdIHwgdW5kZWZpbmVkO1xuICBpZiAoIWsxIHx8ICFrMiB8fCBrMS5sZW5ndGggIT0gazIubGVuZ3RoKSB7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG4gIGxldCBrZXk6IHN0cmluZztcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBrMS5sZW5ndGg7IGkrKykge1xuICAgIGtleSA9IGsxW2ldO1xuICAgIGlmIChhW2tleV0gIT09IGJba2V5XSkge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuLyoqXG4gKiBGbGF0dGVucyBzaW5nbGUtbGV2ZWwgbmVzdGVkIGFycmF5cy5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGZsYXR0ZW48VD4oYXJyOiBUW11bXSk6IFRbXSB7XG4gIHJldHVybiBBcnJheS5wcm90b3R5cGUuY29uY2F0LmFwcGx5KFtdLCBhcnIpO1xufVxuXG4vKipcbiAqIFJldHVybiB0aGUgbGFzdCBlbGVtZW50IG9mIGFuIGFycmF5LlxuICovXG5leHBvcnQgZnVuY3Rpb24gbGFzdDxUPihhOiBUW10pOiBUfG51bGwge1xuICByZXR1cm4gYS5sZW5ndGggPiAwID8gYVthLmxlbmd0aCAtIDFdIDogbnVsbDtcbn1cblxuLyoqXG4gKiBWZXJpZnlzIGFsbCBib29sZWFucyBpbiBhbiBhcnJheSBhcmUgYHRydWVgLlxuICovXG5leHBvcnQgZnVuY3Rpb24gYW5kKGJvb2xzOiBib29sZWFuW10pOiBib29sZWFuIHtcbiAgcmV0dXJuICFib29scy5zb21lKHYgPT4gIXYpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZm9yRWFjaDxLLCBWPihtYXA6IHtba2V5OiBzdHJpbmddOiBWfSwgY2FsbGJhY2s6ICh2OiBWLCBrOiBzdHJpbmcpID0+IHZvaWQpOiB2b2lkIHtcbiAgZm9yIChjb25zdCBwcm9wIGluIG1hcCkge1xuICAgIGlmIChtYXAuaGFzT3duUHJvcGVydHkocHJvcCkpIHtcbiAgICAgIGNhbGxiYWNrKG1hcFtwcm9wXSwgcHJvcCk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB3YWl0Rm9yTWFwPEEsIEI+KFxuICAgIG9iajoge1trOiBzdHJpbmddOiBBfSwgZm46IChrOiBzdHJpbmcsIGE6IEEpID0+IE9ic2VydmFibGU8Qj4pOiBPYnNlcnZhYmxlPHtbazogc3RyaW5nXTogQn0+IHtcbiAgaWYgKE9iamVjdC5rZXlzKG9iaikubGVuZ3RoID09PSAwKSB7XG4gICAgcmV0dXJuIG9mICh7fSk7XG4gIH1cblxuICBjb25zdCB3YWl0SGVhZDogT2JzZXJ2YWJsZTxCPltdID0gW107XG4gIGNvbnN0IHdhaXRUYWlsOiBPYnNlcnZhYmxlPEI+W10gPSBbXTtcbiAgY29uc3QgcmVzOiB7W2s6IHN0cmluZ106IEJ9ID0ge307XG5cbiAgZm9yRWFjaChvYmosIChhOiBBLCBrOiBzdHJpbmcpID0+IHtcbiAgICBjb25zdCBtYXBwZWQgPSBmbihrLCBhKS5waXBlKG1hcCgocjogQikgPT4gcmVzW2tdID0gcikpO1xuICAgIGlmIChrID09PSBQUklNQVJZX09VVExFVCkge1xuICAgICAgd2FpdEhlYWQucHVzaChtYXBwZWQpO1xuICAgIH0gZWxzZSB7XG4gICAgICB3YWl0VGFpbC5wdXNoKG1hcHBlZCk7XG4gICAgfVxuICB9KTtcblxuICAvLyBDbG9zdXJlIGNvbXBpbGVyIGhhcyBwcm9ibGVtIHdpdGggdXNpbmcgc3ByZWFkIG9wZXJhdG9yIGhlcmUuIFNvIHdlIHVzZSBcIkFycmF5LmNvbmNhdFwiLlxuICAvLyBOb3RlIHRoYXQgd2UgYWxzbyBuZWVkIHRvIGNhc3QgdGhlIG5ldyBwcm9taXNlIGJlY2F1c2UgVHlwZVNjcmlwdCBjYW5ub3QgaW5mZXIgdGhlIHR5cGVcbiAgLy8gd2hlbiBjYWxsaW5nIHRoZSBcIm9mXCIgZnVuY3Rpb24gdGhyb3VnaCBcIkZ1bmN0aW9uLmFwcGx5XCJcbiAgcmV0dXJuIChvZiAuYXBwbHkobnVsbCwgd2FpdEhlYWQuY29uY2F0KHdhaXRUYWlsKSkgYXMgT2JzZXJ2YWJsZTxPYnNlcnZhYmxlPEI+PilcbiAgICAgIC5waXBlKGNvbmNhdEFsbCgpLCBsYXN0VmFsdWUoKSwgbWFwKCgpID0+IHJlcykpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gd3JhcEludG9PYnNlcnZhYmxlPFQ+KHZhbHVlOiBUIHwgUHJvbWlzZTxUPnwgT2JzZXJ2YWJsZTxUPik6IE9ic2VydmFibGU8VD4ge1xuICBpZiAoaXNPYnNlcnZhYmxlKHZhbHVlKSkge1xuICAgIHJldHVybiB2YWx1ZTtcbiAgfVxuXG4gIGlmIChpc1Byb21pc2UodmFsdWUpKSB7XG4gICAgLy8gVXNlIGBQcm9taXNlLnJlc29sdmUoKWAgdG8gd3JhcCBwcm9taXNlLWxpa2UgaW5zdGFuY2VzLlxuICAgIC8vIFJlcXVpcmVkIGllIHdoZW4gYSBSZXNvbHZlciByZXR1cm5zIGEgQW5ndWxhckpTIGAkcWAgcHJvbWlzZSB0byBjb3JyZWN0bHkgdHJpZ2dlciB0aGVcbiAgICAvLyBjaGFuZ2UgZGV0ZWN0aW9uLlxuICAgIHJldHVybiBmcm9tKFByb21pc2UucmVzb2x2ZSh2YWx1ZSkpO1xuICB9XG5cbiAgcmV0dXJuIG9mICh2YWx1ZSk7XG59XG4iXX0=