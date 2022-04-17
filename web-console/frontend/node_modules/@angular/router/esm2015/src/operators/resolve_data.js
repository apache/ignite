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
import { from, of } from 'rxjs';
import { concatMap, last, map, mergeMap, reduce } from 'rxjs/operators';
import { inheritedParamsDataResolve } from '../router_state';
import { wrapIntoObservable } from '../utils/collection';
import { getToken } from '../utils/preactivation';
/**
 * @param {?} paramsInheritanceStrategy
 * @param {?} moduleInjector
 * @return {?}
 */
export function resolveData(paramsInheritanceStrategy, moduleInjector) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(mergeMap((/**
         * @param {?} t
         * @return {?}
         */
        t => {
            const { targetSnapshot, guards: { canActivateChecks } } = t;
            if (!canActivateChecks.length) {
                return of(t);
            }
            return from(canActivateChecks)
                .pipe(concatMap((/**
             * @param {?} check
             * @return {?}
             */
            check => runResolve(check.route, (/** @type {?} */ (targetSnapshot)), paramsInheritanceStrategy, moduleInjector))), reduce((/**
             * @param {?} _
             * @param {?} __
             * @return {?}
             */
            (_, __) => _)), map((/**
             * @param {?} _
             * @return {?}
             */
            _ => t)));
        })));
    });
}
/**
 * @param {?} futureARS
 * @param {?} futureRSS
 * @param {?} paramsInheritanceStrategy
 * @param {?} moduleInjector
 * @return {?}
 */
function runResolve(futureARS, futureRSS, paramsInheritanceStrategy, moduleInjector) {
    /** @type {?} */
    const resolve = futureARS._resolve;
    return resolveNode(resolve, futureARS, futureRSS, moduleInjector)
        .pipe(map((/**
     * @param {?} resolvedData
     * @return {?}
     */
    (resolvedData) => {
        futureARS._resolvedData = resolvedData;
        futureARS.data = Object.assign({}, futureARS.data, inheritedParamsDataResolve(futureARS, paramsInheritanceStrategy).resolve);
        return null;
    })));
}
/**
 * @param {?} resolve
 * @param {?} futureARS
 * @param {?} futureRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function resolveNode(resolve, futureARS, futureRSS, moduleInjector) {
    /** @type {?} */
    const keys = Object.keys(resolve);
    if (keys.length === 0) {
        return of({});
    }
    if (keys.length === 1) {
        /** @type {?} */
        const key = keys[0];
        return getResolver(resolve[key], futureARS, futureRSS, moduleInjector)
            .pipe(map((/**
         * @param {?} value
         * @return {?}
         */
        (value) => { return { [key]: value }; })));
    }
    /** @type {?} */
    const data = {};
    /** @type {?} */
    const runningResolvers$ = from(keys).pipe(mergeMap((/**
     * @param {?} key
     * @return {?}
     */
    (key) => {
        return getResolver(resolve[key], futureARS, futureRSS, moduleInjector)
            .pipe(map((/**
         * @param {?} value
         * @return {?}
         */
        (value) => {
            data[key] = value;
            return value;
        })));
    })));
    return runningResolvers$.pipe(last(), map((/**
     * @return {?}
     */
    () => data)));
}
/**
 * @param {?} injectionToken
 * @param {?} futureARS
 * @param {?} futureRSS
 * @param {?} moduleInjector
 * @return {?}
 */
function getResolver(injectionToken, futureARS, futureRSS, moduleInjector) {
    /** @type {?} */
    const resolver = getToken(injectionToken, futureARS, moduleInjector);
    return resolver.resolve ? wrapIntoObservable(resolver.resolve(futureARS, futureRSS)) :
        wrapIntoObservable(resolver(futureARS, futureRSS));
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb2x2ZV9kYXRhLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcm91dGVyL3NyYy9vcGVyYXRvcnMvcmVzb2x2ZV9kYXRhLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBU0EsT0FBTyxFQUF1QyxJQUFJLEVBQUUsRUFBRSxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQ3JFLE9BQU8sRUFBQyxTQUFTLEVBQUUsSUFBSSxFQUFFLEdBQUcsRUFBRSxRQUFRLEVBQUUsTUFBTSxFQUFDLE1BQU0sZ0JBQWdCLENBQUM7QUFJdEUsT0FBTyxFQUE4QywwQkFBMEIsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3hHLE9BQU8sRUFBQyxrQkFBa0IsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBRXZELE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSx3QkFBd0IsQ0FBQzs7Ozs7O0FBRWhELE1BQU0sVUFBVSxXQUFXLENBQ3ZCLHlCQUFpRCxFQUNqRCxjQUF3QjtJQUMxQjs7OztJQUFPLFVBQVMsTUFBd0M7UUFDdEQsT0FBTyxNQUFNLENBQUMsSUFBSSxDQUFDLFFBQVE7Ozs7UUFBQyxDQUFDLENBQUMsRUFBRTtrQkFDeEIsRUFBQyxjQUFjLEVBQUUsTUFBTSxFQUFFLEVBQUMsaUJBQWlCLEVBQUMsRUFBQyxHQUFHLENBQUM7WUFFdkQsSUFBSSxDQUFDLGlCQUFpQixDQUFDLE1BQU0sRUFBRTtnQkFDN0IsT0FBTyxFQUFFLENBQUUsQ0FBQyxDQUFDLENBQUM7YUFDZjtZQUVELE9BQU8sSUFBSSxDQUFDLGlCQUFpQixDQUFDO2lCQUN6QixJQUFJLENBQ0QsU0FBUzs7OztZQUNMLEtBQUssQ0FBQyxFQUFFLENBQUMsVUFBVSxDQUNmLEtBQUssQ0FBQyxLQUFLLEVBQUUsbUJBQUEsY0FBYyxFQUFFLEVBQUUseUJBQXlCLEVBQUUsY0FBYyxDQUFDLEVBQUMsRUFDbEYsTUFBTTs7Ozs7WUFBQyxDQUFD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WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0b3J9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHtNb25vVHlwZU9wZXJhdG9yRnVuY3Rpb24sIE9ic2VydmFibGUsIGZyb20sIG9mIH0gZnJvbSAncnhqcyc7XG5pbXBvcnQge2NvbmNhdE1hcCwgbGFzdCwgbWFwLCBtZXJnZU1hcCwgcmVkdWNlfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7UmVzb2x2ZURhdGF9IGZyb20gJy4uL2NvbmZpZyc7XG5pbXBvcnQge05hdmlnYXRpb25UcmFuc2l0aW9ufSBmcm9tICcuLi9yb3V0ZXInO1xuaW1wb3J0IHtBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBSb3V0ZXJTdGF0ZVNuYXBzaG90LCBpbmhlcml0ZWRQYXJhbXNEYXRhUmVzb2x2ZX0gZnJvbSAnLi4vcm91dGVyX3N0YXRlJztcbmltcG9ydCB7d3JhcEludG9PYnNlcnZhYmxlfSBmcm9tICcuLi91dGlscy9jb2xsZWN0aW9uJztcblxuaW1wb3J0IHtnZXRUb2tlbn0gZnJvbSAnLi4vdXRpbHMvcHJlYWN0aXZhdGlvbic7XG5cbmV4cG9ydCBmdW5jdGlvbiByZXNvbHZlRGF0YShcbiAgICBwYXJhbXNJbmhlcml0YW5jZVN0cmF0ZWd5OiAnZW1wdHlPbmx5JyB8ICdhbHdheXMnLFxuICAgIG1vZHVsZUluamVjdG9yOiBJbmplY3Rvcik6IE1vbm9UeXBlT3BlcmF0b3JGdW5jdGlvbjxOYXZpZ2F0aW9uVHJhbnNpdGlvbj4ge1xuICByZXR1cm4gZnVuY3Rpb24oc291cmNlOiBPYnNlcnZhYmxlPE5hdmlnYXRpb25UcmFuc2l0aW9uPikge1xuICAgIHJldHVybiBzb3VyY2UucGlwZShtZXJnZU1hcCh0ID0+IHtcbiAgICAgIGNvbnN0IHt0YXJnZXRTbmFwc2hvdCwgZ3VhcmRzOiB7Y2FuQWN0aXZhdGVDaGVja3N9fSA9IHQ7XG5cbiAgICAgIGlmICghY2FuQWN0aXZhdGVDaGVja3MubGVuZ3RoKSB7XG4gICAgICAgIHJldHVybiBvZiAodCk7XG4gICAgICB9XG5cbiAgICAgIHJldHVybiBmcm9tKGNhbkFjdGl2YXRlQ2hlY2tzKVxuICAgICAgICAgIC5waXBlKFxuICAgICAgICAgICAgICBjb25jYXRNYXAoXG4gICAgICAgICAgICAgICAgICBjaGVjayA9PiBydW5SZXNvbHZlKFxuICAgICAgICAgICAgICAgICAgICAgIGNoZWNrLnJvdXRlLCB0YXJnZXRTbmFwc2hvdCAhLCBwYXJhbXNJbmhlcml0YW5jZVN0cmF0ZWd5LCBtb2R1bGVJbmplY3RvcikpLFxuICAgICAgICAgICAgICByZWR1Y2UoKF86IGFueSwgX186IGFueSkgPT4gXyksIG1hcChfID0+IHQpKTtcbiAgICB9KSk7XG4gIH07XG59XG5cbmZ1bmN0aW9uIHJ1blJlc29sdmUoXG4gICAgZnV0dXJlQVJTOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90LCBmdXR1cmVSU1M6IFJvdXRlclN0YXRlU25hcHNob3QsXG4gICAgcGFyYW1zSW5oZXJpdGFuY2VTdHJhdGVneTogJ2VtcHR5T25seScgfCAnYWx3YXlzJywgbW9kdWxlSW5qZWN0b3I6IEluamVjdG9yKSB7XG4gIGNvbnN0IHJlc29sdmUgPSBmdXR1cmVBUlMuX3Jlc29sdmU7XG4gIHJldHVybiByZXNvbHZlTm9kZShyZXNvbHZlLCBmdXR1cmVBUlMsIGZ1dHVyZVJTUywgbW9kdWxlSW5qZWN0b3IpXG4gICAgICAucGlwZShtYXAoKHJlc29sdmVkRGF0YTogYW55KSA9PiB7XG4gICAgICAgIGZ1dHVyZUFSUy5fcmVzb2x2ZWREYXRhID0gcmVzb2x2ZWREYXRhO1xuICAgICAgICBmdXR1cmVBUlMuZGF0YSA9IHtcbiAgICAgICAgICAgIC4uLmZ1dHVyZUFSUy5kYXRhLFxuICAgICAgICAgICAgLi4uaW5oZXJpdGVkUGFyYW1zRGF0YVJlc29sdmUoZnV0dXJlQVJTLCBwYXJhbXNJbmhlcml0YW5jZVN0cmF0ZWd5KS5yZXNvbHZlfTtcbiAgICAgICAgcmV0dXJuIG51bGw7XG4gICAgICB9KSk7XG59XG5cbmZ1bmN0aW9uIHJlc29sdmVOb2RlKFxuICAgIHJlc29sdmU6IFJlc29sdmVEYXRhLCBmdXR1cmVBUlM6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsIGZ1dHVyZVJTUzogUm91dGVyU3RhdGVTbmFwc2hvdCxcbiAgICBtb2R1bGVJbmplY3RvcjogSW5qZWN0b3IpOiBPYnNlcnZhYmxlPGFueT4ge1xuICBjb25zdCBrZXlzID0gT2JqZWN0LmtleXMocmVzb2x2ZSk7XG4gIGlmIChrZXlzLmxlbmd0aCA9PT0gMCkge1xuICAgIHJldHVybiBvZiAoe30pO1xuICB9XG4gIGlmIChrZXlzLmxlbmd0aCA9PT0gMSkge1xuICAgIGNvbnN0IGtleSA9IGtleXNbMF07XG4gICAgcmV0dXJuIGdldFJlc29sdmVyKHJlc29sdmVba2V5XSwgZnV0dXJlQVJTLCBmdXR1cmVSU1MsIG1vZHVsZUluamVjdG9yKVxuICAgICAgICAucGlwZShtYXAoKHZhbHVlOiBhbnkpID0+IHsgcmV0dXJuIHtba2V5XTogdmFsdWV9OyB9KSk7XG4gIH1cbiAgY29uc3QgZGF0YToge1trOiBzdHJpbmddOiBhbnl9ID0ge307XG4gIGNvbnN0IHJ1bm5pbmdSZXNvbHZlcnMkID0gZnJvbShrZXlzKS5waXBlKG1lcmdlTWFwKChrZXk6IHN0cmluZykgPT4ge1xuICAgIHJldHVybiBnZXRSZXNvbHZlcihyZXNvbHZlW2tleV0sIGZ1dHVyZUFSUywgZnV0dXJlUlNTLCBtb2R1bGVJbmplY3RvcilcbiAgICAgICAgLnBpcGUobWFwKCh2YWx1ZTogYW55KSA9PiB7XG4gICAgICAgICAgZGF0YVtrZXldID0gdmFsdWU7XG4gICAgICAgICAgcmV0dXJuIHZhbHVlO1xuICAgICAgICB9KSk7XG4gIH0pKTtcbiAgcmV0dXJuIHJ1bm5pbmdSZXNvbHZlcnMkLnBpcGUobGFzdCgpLCBtYXAoKCkgPT4gZGF0YSkpO1xufVxuXG5mdW5jdGlvbiBnZXRSZXNvbHZlcihcbiAgICBpbmplY3Rpb25Ub2tlbjogYW55LCBmdXR1cmVBUlM6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsIGZ1dHVyZVJTUzogUm91dGVyU3RhdGVTbmFwc2hvdCxcbiAgICBtb2R1bGVJbmplY3RvcjogSW5qZWN0b3IpOiBPYnNlcnZhYmxlPGFueT4ge1xuICBjb25zdCByZXNvbHZlciA9IGdldFRva2VuKGluamVjdGlvblRva2VuLCBmdXR1cmVBUlMsIG1vZHVsZUluamVjdG9yKTtcbiAgcmV0dXJuIHJlc29sdmVyLnJlc29sdmUgPyB3cmFwSW50b09ic2VydmFibGUocmVzb2x2ZXIucmVzb2x2ZShmdXR1cmVBUlMsIGZ1dHVyZVJTUykpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB3cmFwSW50b09ic2VydmFibGUocmVzb2x2ZXIoZnV0dXJlQVJTLCBmdXR1cmVSU1MpKTtcbn0iXX0=