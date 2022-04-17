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
import { combineLatest } from 'rxjs';
import { filter, map, scan, startWith, switchMap, take } from 'rxjs/operators';
import { isUrlTree } from '../utils/type_guards';
/** @type {?} */
const INITIAL_VALUE = Symbol('INITIAL_VALUE');
/**
 * @return {?}
 */
export function prioritizedGuardValue() {
    return switchMap((/**
     * @param {?} obs
     * @return {?}
     */
    obs => {
        return (/** @type {?} */ (combineLatest(...obs.map((/**
         * @param {?} o
         * @return {?}
         */
        o => o.pipe(take(1), startWith((/** @type {?} */ (INITIAL_VALUE)))))))
            .pipe(scan((/**
         * @param {?} acc
         * @param {?} list
         * @return {?}
         */
        (acc, list) => {
            /** @type {?} */
            let isPending = false;
            return list.reduce((/**
             * @param {?} innerAcc
             * @param {?} val
             * @param {?} i
             * @return {?}
             */
            (innerAcc, val, i) => {
                if (innerAcc !== INITIAL_VALUE)
                    return innerAcc;
                // Toggle pending flag if any values haven't been set yet
                if (val === INITIAL_VALUE)
                    isPending = true;
                // Any other return values are only valid if we haven't yet hit a pending call.
                // This guarantees that in the case of a guard at the bottom of the tree that
                // returns a redirect, we will wait for the higher priority guard at the top to
                // finish before performing the redirect.
                if (!isPending) {
                    // Early return when we hit a `false` value as that should always cancel
                    // navigation
                    if (val === false)
                        return val;
                    if (i === list.length - 1 || isUrlTree(val)) {
                        return val;
                    }
                }
                return innerAcc;
            }), acc);
        }), INITIAL_VALUE), filter((/**
         * @param {?} item
         * @return {?}
         */
        item => item !== INITIAL_VALUE)), map((/**
         * @param {?} item
         * @return {?}
         */
        item => isUrlTree(item) ? item : item === true)), //
        take(1))));
    }));
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJpb3JpdGl6ZWRfZ3VhcmRfdmFsdWUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9yb3V0ZXIvc3JjL29wZXJhdG9ycy9wcmlvcml0aXplZF9ndWFyZF92YWx1ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBK0IsYUFBYSxFQUFDLE1BQU0sTUFBTSxDQUFDO0FBQ2pFLE9BQU8sRUFBQyxNQUFNLEVBQUUsR0FBRyxFQUFFLElBQUksRUFBRSxTQUFTLEVBQUUsU0FBUyxFQUFFLElBQUksRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBRzdFLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxzQkFBc0IsQ0FBQzs7TUFFekMsYUFBYSxHQUFHLE1BQU0sQ0FBQyxlQUFlLENBQUM7Ozs7QUFHN0MsTUFBTSxVQUFVLHFCQUFxQjtJQUVuQyxPQUFPLFNBQVM7Ozs7SUFBQyxHQUFHLENBQUMsRUFBRTtRQUNyQixPQUFPLG1CQUFBLGFBQWEsQ0FDVCxHQUFHLEdBQUcsQ0FBQyxHQUFHOzs7O1FBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsRUFBRSxTQUFTLENBQUMsbUJBQUEsYUFBYSxFQUFrQixDQUFDLENBQUMsRUFBQyxDQUFDO2FBQ25GLElBQUksQ0FDRCxJQUFJOzs7OztRQUNBLENBQUMsR0FBbUIsRUFBRSxJQUFzQixFQUFFLEVBQUU7O2dCQUMxQyxTQUFTLEdBQUcsS0FBSztZQUNyQixPQUFPLElBQUksQ0FBQyxNQUFNOzs7Ozs7WUFBQyxDQUFDLFFBQVEsRUFBRSxHQUFHLEVBQUUsQ0FBUyxFQUFFLEVBQUU7Z0JBQzlDLElBQUksUUFBUSxLQUFLLGFBQWE7b0JBQUUsT0FBTyxRQUFRLENBQUM7Z0JBRWhELHlEQUF5RDtnQkFDekQsSUFBSSxHQUFHLEtBQUssYUFBYTtvQkFBRSxTQUFTLEdBQUcsSUFBSSxDQUFDO2dCQUU1QywrRUFBK0U7Z0JBQy9FLDZFQUE2RTtnQkFDN0UsK0VBQStFO2dCQUMvRSx5Q0FBeUM7Z0JBQ3pDLElBQUksQ0FBQyxTQUFTLEVBQUU7b0JBQ2Qsd0VBQXdFO29CQUN4RSxhQUFhO29CQUNiLElBQUksR0FBRyxLQUFLLEtBQUs7d0JBQUUsT0FBTyxHQUFHLENBQUM7b0JBRTlCLElBQUksQ0FBQyxLQUFLLElBQUksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxJQUFJLFNBQVMsQ0FBQyxHQUFHLENBQUMsRUFBRTt3QkFDM0MsT0FBTyxHQUFHLENBQUM7cUJBQ1o7aUJBQ0Y7Z0JBRUQsT0FBTyxRQUFRLENBQUM7WUFDbEIsQ0FBQyxHQUFFLEdBQUcsQ0FBQyxDQUFDO1FBQ1YsQ0FBQyxHQUNELGFBQWEsQ0FBQyxFQUNsQixNQUFNOzs7O1FBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxJQUFJLEtBQUssYUFBYSxFQUFDLEVBQ3RDLEdBQUc7Ozs7UUFBQyxJQUFJLENBQUMsRUFBRSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxJQUFJLEtBQUssSUFBSSxFQUFDLEVBQUcsRUFBRTtRQUN4RCxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBK0IsQ0FBQztJQUNsRCxDQUFDLEVBQUMsQ0FBQztBQUNMLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7T2JzZXJ2YWJsZSwgT3BlcmF0b3JGdW5jdGlvbiwgY29tYmluZUxhdGVzdH0gZnJvbSAncnhqcyc7XG5pbXBvcnQge2ZpbHRlciwgbWFwLCBzY2FuLCBzdGFydFdpdGgsIHN3aXRjaE1hcCwgdGFrZX0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuXG5pbXBvcnQge1VybFRyZWV9IGZyb20gJy4uL3VybF90cmVlJztcbmltcG9ydCB7aXNVcmxUcmVlfSBmcm9tICcuLi91dGlscy90eXBlX2d1YXJkcyc7XG5cbmNvbnN0IElOSVRJQUxfVkFMVUUgPSBTeW1ib2woJ0lOSVRJQUxfVkFMVUUnKTtcbmRlY2xhcmUgdHlwZSBJTlRFUklNX1ZBTFVFUyA9IHR5cGVvZiBJTklUSUFMX1ZBTFVFIHwgYm9vbGVhbiB8IFVybFRyZWU7XG5cbmV4cG9ydCBmdW5jdGlvbiBwcmlvcml0aXplZEd1YXJkVmFsdWUoKTpcbiAgICBPcGVyYXRvckZ1bmN0aW9uPE9ic2VydmFibGU8Ym9vbGVhbnxVcmxUcmVlPltdLCBib29sZWFufFVybFRyZWU+IHtcbiAgcmV0dXJuIHN3aXRjaE1hcChvYnMgPT4ge1xuICAgIHJldHVybiBjb21iaW5lTGF0ZXN0KFxuICAgICAgICAgICAgICAgLi4ub2JzLm1hcChvID0+IG8ucGlwZSh0YWtlKDEpLCBzdGFydFdpdGgoSU5JVElBTF9WQUxVRSBhcyBJTlRFUklNX1ZBTFVFUykpKSlcbiAgICAgICAgLnBpcGUoXG4gICAgICAgICAgICBzY2FuKFxuICAgICAgICAgICAgICAgIChhY2M6IElOVEVSSU1fVkFMVUVTLCBsaXN0OiBJTlRFUklNX1ZBTFVFU1tdKSA9PiB7XG4gICAgICAgICAgICAgICAgICBsZXQgaXNQZW5kaW5nID0gZmFsc2U7XG4gICAgICAgICAgICAgICAgICByZXR1cm4gbGlzdC5yZWR1Y2UoKGlubmVyQWNjLCB2YWwsIGk6IG51bWJlcikgPT4ge1xuICAgICAgICAgICAgICAgICAgICBpZiAoaW5uZXJBY2MgIT09IElOSVRJQUxfVkFMVUUpIHJldHVybiBpbm5lckFjYztcblxuICAgICAgICAgICAgICAgICAgICAvLyBUb2dnbGUgcGVuZGluZyBmbGFnIGlmIGFueSB2YWx1ZXMgaGF2ZW4ndCBiZWVuIHNldCB5ZXRcbiAgICAgICAgICAgICAgICAgICAgaWYgKHZhbCA9PT0gSU5JVElBTF9WQUxVRSkgaXNQZW5kaW5nID0gdHJ1ZTtcblxuICAgICAgICAgICAgICAgICAgICAvLyBBbnkgb3RoZXIgcmV0dXJuIHZhbHVlcyBhcmUgb25seSB2YWxpZCBpZiB3ZSBoYXZlbid0IHlldCBoaXQgYSBwZW5kaW5nIGNhbGwuXG4gICAgICAgICAgICAgICAgICAgIC8vIFRoaXMgZ3VhcmFudGVlcyB0aGF0IGluIHRoZSBjYXNlIG9mIGEgZ3VhcmQgYXQgdGhlIGJvdHRvbSBvZiB0aGUgdHJlZSB0aGF0XG4gICAgICAgICAgICAgICAgICAgIC8vIHJldHVybnMgYSByZWRpcmVjdCwgd2Ugd2lsbCB3YWl0IGZvciB0aGUgaGlnaGVyIHByaW9yaXR5IGd1YXJkIGF0IHRoZSB0b3AgdG9cbiAgICAgICAgICAgICAgICAgICAgLy8gZmluaXNoIGJlZm9yZSBwZXJmb3JtaW5nIHRoZSByZWRpcmVjdC5cbiAgICAgICAgICAgICAgICAgICAgaWYgKCFpc1BlbmRpbmcpIHtcbiAgICAgICAgICAgICAgICAgICAgICAvLyBFYXJseSByZXR1cm4gd2hlbiB3ZSBoaXQgYSBgZmFsc2VgIHZhbHVlIGFzIHRoYXQgc2hvdWxkIGFsd2F5cyBjYW5jZWxcbiAgICAgICAgICAgICAgICAgICAgICAvLyBuYXZpZ2F0aW9uXG4gICAgICAgICAgICAgICAgICAgICAgaWYgKHZhbCA9PT0gZmFsc2UpIHJldHVybiB2YWw7XG5cbiAgICAgICAgICAgICAgICAgICAgICBpZiAoaSA9PT0gbGlzdC5sZW5ndGggLSAxIHx8IGlzVXJsVHJlZSh2YWwpKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gdmFsO1xuICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBpbm5lckFjYztcbiAgICAgICAgICAgICAgICAgIH0sIGFjYyk7XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBJTklUSUFMX1ZBTFVFKSxcbiAgICAgICAgICAgIGZpbHRlcihpdGVtID0+IGl0ZW0gIT09IElOSVRJQUxfVkFMVUUpLFxuICAgICAgICAgICAgbWFwKGl0ZW0gPT4gaXNVcmxUcmVlKGl0ZW0pID8gaXRlbSA6IGl0ZW0gPT09IHRydWUpLCAgLy9cbiAgICAgICAgICAgIHRha2UoMSkpIGFzIE9ic2VydmFibGU8Ym9vbGVhbnxVcmxUcmVlPjtcbiAgfSk7XG59XG4iXX0=