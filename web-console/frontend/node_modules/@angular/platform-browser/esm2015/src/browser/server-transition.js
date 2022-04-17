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
import { DOCUMENT } from '@angular/common';
import { APP_INITIALIZER, ApplicationInitStatus, InjectionToken, Injector } from '@angular/core';
import { getDOM } from '../dom/dom_adapter';
/**
 * An id that identifies a particular application being bootstrapped, that should
 * match across the client/server boundary.
 * @type {?}
 */
export const TRANSITION_ID = new InjectionToken('TRANSITION_ID');
/**
 * @param {?} transitionId
 * @param {?} document
 * @param {?} injector
 * @return {?}
 */
export function appInitializerFactory(transitionId, document, injector) {
    return (/**
     * @return {?}
     */
    () => {
        // Wait for all application initializers to be completed before removing the styles set by
        // the server.
        injector.get(ApplicationInitStatus).donePromise.then((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const dom = getDOM();
            /** @type {?} */
            const styles = Array.prototype.slice.apply(dom.querySelectorAll(document, `style[ng-transition]`));
            styles.filter((/**
             * @param {?} el
             * @return {?}
             */
            el => dom.getAttribute(el, 'ng-transition') === transitionId))
                .forEach((/**
             * @param {?} el
             * @return {?}
             */
            el => dom.remove(el)));
        }));
    });
}
/** @type {?} */
export const SERVER_TRANSITION_PROVIDERS = [
    {
        provide: APP_INITIALIZER,
        useFactory: appInitializerFactory,
        deps: [TRANSITION_ID, DOCUMENT, Injector],
        multi: true
    },
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VydmVyLXRyYW5zaXRpb24uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9wbGF0Zm9ybS1icm93c2VyL3NyYy9icm93c2VyL3NlcnZlci10cmFuc2l0aW9uLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3pDLE9BQU8sRUFBQyxlQUFlLEVBQUUscUJBQXFCLEVBQVUsY0FBYyxFQUFFLFFBQVEsRUFBaUIsTUFBTSxlQUFlLENBQUM7QUFFdkgsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLG9CQUFvQixDQUFDOzs7Ozs7QUFNMUMsTUFBTSxPQUFPLGFBQWEsR0FBRyxJQUFJLGNBQWMsQ0FBQyxlQUFlLENBQUM7Ozs7Ozs7QUFFaEUsTUFBTSxVQUFVLHFCQUFxQixDQUFDLFlBQW9CLEVBQUUsUUFBYSxFQUFFLFFBQWtCO0lBQzNGOzs7SUFBTyxHQUFHLEVBQUU7UUFDViwwRkFBMEY7UUFDMUYsY0FBYztRQUNkLFFBQVEsQ0FBQyxHQUFHLENBQUMscUJBQXFCLENBQUMsQ0FBQyxXQUFXLENBQUMsSUFBSTs7O1FBQUMsR0FBRyxFQUFFOztrQkFDbEQsR0FBRyxHQUFHLE1BQU0sRUFBRTs7a0JBQ2QsTUFBTSxHQUNSLEtBQUssQ0FBQyxTQUFTLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsZ0JBQWdCLENBQUMsUUFBUSxFQUFFLHNCQUFzQixDQUFDLENBQUM7WUFDdkYsTUFBTSxDQUFDLE1BQU07Ozs7WUFBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEdBQUcsQ0FBQyxZQUFZLENBQUMsRUFBRSxFQUFFLGVBQWUsQ0FBQyxLQUFLLFlBQVksRUFBQztpQkFDdEUsT0FBTzs7OztZQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsR0FBRyxDQUFDLE1BQU0sQ0FBQyxFQUFFLENBQUMsRUFBQyxDQUFDO1FBQ3JDLENBQUMsRUFBQyxDQUFDO0lBQ0wsQ0FBQyxFQUFDO0FBQ0osQ0FBQzs7QUFFRCxNQUFNLE9BQU8sMkJBQTJCLEdBQXFCO0lBQzNEO1FBQ0UsT0FBTyxFQUFFLGVBQWU7UUFDeEIsVUFBVSxFQUFFLHFCQUFxQjtRQUNqQyxJQUFJLEVBQUUsQ0FBQyxhQUFhLEVBQUUsUUFBUSxFQUFFLFFBQVEsQ0FBQztRQUN6QyxLQUFLLEVBQUUsSUFBSTtLQUNaO0NBQ0YiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RE9DVU1FTlR9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQge0FQUF9JTklUSUFMSVpFUiwgQXBwbGljYXRpb25Jbml0U3RhdHVzLCBJbmplY3QsIEluamVjdGlvblRva2VuLCBJbmplY3RvciwgU3RhdGljUHJvdmlkZXJ9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge2dldERPTX0gZnJvbSAnLi4vZG9tL2RvbV9hZGFwdGVyJztcblxuLyoqXG4gKiBBbiBpZCB0aGF0IGlkZW50aWZpZXMgYSBwYXJ0aWN1bGFyIGFwcGxpY2F0aW9uIGJlaW5nIGJvb3RzdHJhcHBlZCwgdGhhdCBzaG91bGRcbiAqIG1hdGNoIGFjcm9zcyB0aGUgY2xpZW50L3NlcnZlciBib3VuZGFyeS5cbiAqL1xuZXhwb3J0IGNvbnN0IFRSQU5TSVRJT05fSUQgPSBuZXcgSW5qZWN0aW9uVG9rZW4oJ1RSQU5TSVRJT05fSUQnKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGFwcEluaXRpYWxpemVyRmFjdG9yeSh0cmFuc2l0aW9uSWQ6IHN0cmluZywgZG9jdW1lbnQ6IGFueSwgaW5qZWN0b3I6IEluamVjdG9yKSB7XG4gIHJldHVybiAoKSA9PiB7XG4gICAgLy8gV2FpdCBmb3IgYWxsIGFwcGxpY2F0aW9uIGluaXRpYWxpemVycyB0byBiZSBjb21wbGV0ZWQgYmVmb3JlIHJlbW92aW5nIHRoZSBzdHlsZXMgc2V0IGJ5XG4gICAgLy8gdGhlIHNlcnZlci5cbiAgICBpbmplY3Rvci5nZXQoQXBwbGljYXRpb25Jbml0U3RhdHVzKS5kb25lUHJvbWlzZS50aGVuKCgpID0+IHtcbiAgICAgIGNvbnN0IGRvbSA9IGdldERPTSgpO1xuICAgICAgY29uc3Qgc3R5bGVzOiBhbnlbXSA9XG4gICAgICAgICAgQXJyYXkucHJvdG90eXBlLnNsaWNlLmFwcGx5KGRvbS5xdWVyeVNlbGVjdG9yQWxsKGRvY3VtZW50LCBgc3R5bGVbbmctdHJhbnNpdGlvbl1gKSk7XG4gICAgICBzdHlsZXMuZmlsdGVyKGVsID0+IGRvbS5nZXRBdHRyaWJ1dGUoZWwsICduZy10cmFuc2l0aW9uJykgPT09IHRyYW5zaXRpb25JZClcbiAgICAgICAgICAuZm9yRWFjaChlbCA9PiBkb20ucmVtb3ZlKGVsKSk7XG4gICAgfSk7XG4gIH07XG59XG5cbmV4cG9ydCBjb25zdCBTRVJWRVJfVFJBTlNJVElPTl9QUk9WSURFUlM6IFN0YXRpY1Byb3ZpZGVyW10gPSBbXG4gIHtcbiAgICBwcm92aWRlOiBBUFBfSU5JVElBTElaRVIsXG4gICAgdXNlRmFjdG9yeTogYXBwSW5pdGlhbGl6ZXJGYWN0b3J5LFxuICAgIGRlcHM6IFtUUkFOU0lUSU9OX0lELCBET0NVTUVOVCwgSW5qZWN0b3JdLFxuICAgIG11bHRpOiB0cnVlXG4gIH0sXG5dO1xuIl19