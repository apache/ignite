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
import { from } from 'rxjs';
import { map, switchMap } from 'rxjs/operators';
/**
 * Perform a side effect through a switchMap for every emission on the source Observable,
 * but return an Observable that is identical to the source. It's essentially the same as
 * the `tap` operator, but if the side effectful `next` function returns an ObservableInput,
 * it will wait before continuing with the original value.
 * @template T
 * @param {?} next
 * @return {?}
 */
export function switchTap(next) {
    return (/**
     * @param {?} source
     * @return {?}
     */
    function (source) {
        return source.pipe(switchMap((/**
         * @param {?} v
         * @return {?}
         */
        v => {
            /** @type {?} */
            const nextResult = next(v);
            if (nextResult) {
                return from(nextResult).pipe(map((/**
                 * @return {?}
                 */
                () => v)));
            }
            return from([v]);
        })));
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3dpdGNoX3RhcC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3JvdXRlci9zcmMvb3BlcmF0b3JzL3N3aXRjaF90YXAudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQTRDLElBQUksRUFBQyxNQUFNLE1BQU0sQ0FBQztBQUNyRSxPQUFPLEVBQUMsR0FBRyxFQUFFLFNBQVMsRUFBQyxNQUFNLGdCQUFnQixDQUFDOzs7Ozs7Ozs7O0FBUTlDLE1BQU0sVUFBVSxTQUFTLENBQUksSUFBeUM7SUFFcEU7Ozs7SUFBTyxVQUFTLE1BQU07UUFDcEIsT0FBTyxNQUFNLENBQUMsSUFBSSxDQUFDLFNBQVM7Ozs7UUFBQyxDQUFDLENBQUMsRUFBRTs7a0JBQ3pCLFVBQVUsR0FBRyxJQUFJLENBQUMsQ0FBQyxDQUFDO1lBQzFCLElBQUksVUFBVSxFQUFFO2dCQUNkLE9BQU8sSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLElBQUksQ0FBQyxHQUFHOzs7Z0JBQUMsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFDLENBQUMsQ0FBQzthQUM1QztZQUNELE9BQU8sSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNuQixDQUFDLEVBQUMsQ0FBQyxDQUFDO0lBQ04sQ0FBQyxFQUFDO0FBQ0osQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtNb25vVHlwZU9wZXJhdG9yRnVuY3Rpb24sIE9ic2VydmFibGVJbnB1dCwgZnJvbX0gZnJvbSAncnhqcyc7XG5pbXBvcnQge21hcCwgc3dpdGNoTWFwfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbi8qKlxuICogUGVyZm9ybSBhIHNpZGUgZWZmZWN0IHRocm91Z2ggYSBzd2l0Y2hNYXAgZm9yIGV2ZXJ5IGVtaXNzaW9uIG9uIHRoZSBzb3VyY2UgT2JzZXJ2YWJsZSxcbiAqIGJ1dCByZXR1cm4gYW4gT2JzZXJ2YWJsZSB0aGF0IGlzIGlkZW50aWNhbCB0byB0aGUgc291cmNlLiBJdCdzIGVzc2VudGlhbGx5IHRoZSBzYW1lIGFzXG4gKiB0aGUgYHRhcGAgb3BlcmF0b3IsIGJ1dCBpZiB0aGUgc2lkZSBlZmZlY3RmdWwgYG5leHRgIGZ1bmN0aW9uIHJldHVybnMgYW4gT2JzZXJ2YWJsZUlucHV0LFxuICogaXQgd2lsbCB3YWl0IGJlZm9yZSBjb250aW51aW5nIHdpdGggdGhlIG9yaWdpbmFsIHZhbHVlLlxuICovXG5leHBvcnQgZnVuY3Rpb24gc3dpdGNoVGFwPFQ+KG5leHQ6ICh4OiBUKSA9PiB2b2lkfE9ic2VydmFibGVJbnB1dDxhbnk+KTpcbiAgICBNb25vVHlwZU9wZXJhdG9yRnVuY3Rpb248VD4ge1xuICByZXR1cm4gZnVuY3Rpb24oc291cmNlKSB7XG4gICAgcmV0dXJuIHNvdXJjZS5waXBlKHN3aXRjaE1hcCh2ID0+IHtcbiAgICAgIGNvbnN0IG5leHRSZXN1bHQgPSBuZXh0KHYpO1xuICAgICAgaWYgKG5leHRSZXN1bHQpIHtcbiAgICAgICAgcmV0dXJuIGZyb20obmV4dFJlc3VsdCkucGlwZShtYXAoKCkgPT4gdikpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIGZyb20oW3ZdKTtcbiAgICB9KSk7XG4gIH07XG59XG4iXX0=