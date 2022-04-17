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
 * \@description
 *
 * Provides a way to customize when activated routes get reused.
 *
 * \@publicApi
 * @abstract
 */
export class RouteReuseStrategy {
}
if (false) {
    /**
     * Determines if this route (and its subtree) should be detached to be reused later
     * @abstract
     * @param {?} route
     * @return {?}
     */
    RouteReuseStrategy.prototype.shouldDetach = function (route) { };
    /**
     * Stores the detached route.
     *
     * Storing a `null` value should erase the previously stored value.
     * @abstract
     * @param {?} route
     * @param {?} handle
     * @return {?}
     */
    RouteReuseStrategy.prototype.store = function (route, handle) { };
    /**
     * Determines if this route (and its subtree) should be reattached
     * @abstract
     * @param {?} route
     * @return {?}
     */
    RouteReuseStrategy.prototype.shouldAttach = function (route) { };
    /**
     * Retrieves the previously stored route
     * @abstract
     * @param {?} route
     * @return {?}
     */
    RouteReuseStrategy.prototype.retrieve = function (route) { };
    /**
     * Determines if a route should be reused
     * @abstract
     * @param {?} future
     * @param {?} curr
     * @return {?}
     */
    RouteReuseStrategy.prototype.shouldReuseRoute = function (future, curr) { };
}
/**
 * Does not detach any subtrees. Reuses routes as long as their route config is the same.
 */
export class DefaultRouteReuseStrategy {
    /**
     * @param {?} route
     * @return {?}
     */
    shouldDetach(route) { return false; }
    /**
     * @param {?} route
     * @param {?} detachedTree
     * @return {?}
     */
    store(route, detachedTree) { }
    /**
     * @param {?} route
     * @return {?}
     */
    shouldAttach(route) { return false; }
    /**
     * @param {?} route
     * @return {?}
     */
    retrieve(route) { return null; }
    /**
     * @param {?} future
     * @param {?} curr
     * @return {?}
     */
    shouldReuseRoute(future, curr) {
        return future.routeConfig === curr.routeConfig;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicm91dGVfcmV1c2Vfc3RyYXRlZ3kuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9yb3V0ZXIvc3JjL3JvdXRlX3JldXNlX3N0cmF0ZWd5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUF3Q0EsTUFBTSxPQUFnQixrQkFBa0I7Q0FtQnZDOzs7Ozs7OztJQWpCQyxpRUFBOEQ7Ozs7Ozs7Ozs7SUFPOUQsa0VBQXNGOzs7Ozs7O0lBR3RGLGlFQUE4RDs7Ozs7OztJQUc5RCw2REFBMkU7Ozs7Ozs7O0lBRzNFLDRFQUFpRzs7Ozs7QUFNbkcsTUFBTSxPQUFPLHlCQUF5Qjs7Ozs7SUFDcEMsWUFBWSxDQUFDLEtBQTZCLElBQWEsT0FBTyxLQUFLLENBQUMsQ0FBQyxDQUFDOzs7Ozs7SUFDdEUsS0FBSyxDQUFDLEtBQTZCLEVBQUUsWUFBaUMsSUFBUyxDQUFDOzs7OztJQUNoRixZQUFZLENBQUMsS0FBNkIsSUFBYSxPQUFPLEtBQUssQ0FBQyxDQUFDLENBQUM7Ozs7O0lBQ3RFLFFBQVEsQ0FBQyxLQUE2QixJQUE4QixPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7Ozs7OztJQUNsRixnQkFBZ0IsQ0FBQyxNQUE4QixFQUFFLElBQTRCO1FBQzNFLE9BQU8sTUFBTSxDQUFDLFdBQVcsS0FBSyxJQUFJLENBQUMsV0FBVyxDQUFDO0lBQ2pELENBQUM7Q0FDRiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21wb25lbnRSZWZ9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge091dGxldENvbnRleHR9IGZyb20gJy4vcm91dGVyX291dGxldF9jb250ZXh0JztcbmltcG9ydCB7QWN0aXZhdGVkUm91dGUsIEFjdGl2YXRlZFJvdXRlU25hcHNob3R9IGZyb20gJy4vcm91dGVyX3N0YXRlJztcbmltcG9ydCB7VHJlZU5vZGV9IGZyb20gJy4vdXRpbHMvdHJlZSc7XG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogUmVwcmVzZW50cyB0aGUgZGV0YWNoZWQgcm91dGUgdHJlZS5cbiAqXG4gKiBUaGlzIGlzIGFuIG9wYXF1ZSB2YWx1ZSB0aGUgcm91dGVyIHdpbGwgZ2l2ZSB0byBhIGN1c3RvbSByb3V0ZSByZXVzZSBzdHJhdGVneVxuICogdG8gc3RvcmUgYW5kIHJldHJpZXZlIGxhdGVyIG9uLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IHR5cGUgRGV0YWNoZWRSb3V0ZUhhbmRsZSA9IHt9O1xuXG4vKiogQGludGVybmFsICovXG5leHBvcnQgdHlwZSBEZXRhY2hlZFJvdXRlSGFuZGxlSW50ZXJuYWwgPSB7XG4gIGNvbnRleHRzOiBNYXA8c3RyaW5nLCBPdXRsZXRDb250ZXh0PixcbiAgY29tcG9uZW50UmVmOiBDb21wb25lbnRSZWY8YW55PixcbiAgcm91dGU6IFRyZWVOb2RlPEFjdGl2YXRlZFJvdXRlPixcbn07XG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogUHJvdmlkZXMgYSB3YXkgdG8gY3VzdG9taXplIHdoZW4gYWN0aXZhdGVkIHJvdXRlcyBnZXQgcmV1c2VkLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIFJvdXRlUmV1c2VTdHJhdGVneSB7XG4gIC8qKiBEZXRlcm1pbmVzIGlmIHRoaXMgcm91dGUgKGFuZCBpdHMgc3VidHJlZSkgc2hvdWxkIGJlIGRldGFjaGVkIHRvIGJlIHJldXNlZCBsYXRlciAqL1xuICBhYnN0cmFjdCBzaG91bGREZXRhY2gocm91dGU6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QpOiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBTdG9yZXMgdGhlIGRldGFjaGVkIHJvdXRlLlxuICAgKlxuICAgKiBTdG9yaW5nIGEgYG51bGxgIHZhbHVlIHNob3VsZCBlcmFzZSB0aGUgcHJldmlvdXNseSBzdG9yZWQgdmFsdWUuXG4gICAqL1xuICBhYnN0cmFjdCBzdG9yZShyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgaGFuZGxlOiBEZXRhY2hlZFJvdXRlSGFuZGxlfG51bGwpOiB2b2lkO1xuXG4gIC8qKiBEZXRlcm1pbmVzIGlmIHRoaXMgcm91dGUgKGFuZCBpdHMgc3VidHJlZSkgc2hvdWxkIGJlIHJlYXR0YWNoZWQgKi9cbiAgYWJzdHJhY3Qgc2hvdWxkQXR0YWNoKHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90KTogYm9vbGVhbjtcblxuICAvKiogUmV0cmlldmVzIHRoZSBwcmV2aW91c2x5IHN0b3JlZCByb3V0ZSAqL1xuICBhYnN0cmFjdCByZXRyaWV2ZShyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCk6IERldGFjaGVkUm91dGVIYW5kbGV8bnVsbDtcblxuICAvKiogRGV0ZXJtaW5lcyBpZiBhIHJvdXRlIHNob3VsZCBiZSByZXVzZWQgKi9cbiAgYWJzdHJhY3Qgc2hvdWxkUmV1c2VSb3V0ZShmdXR1cmU6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsIGN1cnI6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QpOiBib29sZWFuO1xufVxuXG4vKipcbiAqIERvZXMgbm90IGRldGFjaCBhbnkgc3VidHJlZXMuIFJldXNlcyByb3V0ZXMgYXMgbG9uZyBhcyB0aGVpciByb3V0ZSBjb25maWcgaXMgdGhlIHNhbWUuXG4gKi9cbmV4cG9ydCBjbGFzcyBEZWZhdWx0Um91dGVSZXVzZVN0cmF0ZWd5IGltcGxlbWVudHMgUm91dGVSZXVzZVN0cmF0ZWd5IHtcbiAgc2hvdWxkRGV0YWNoKHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90KTogYm9vbGVhbiB7IHJldHVybiBmYWxzZTsgfVxuICBzdG9yZShyb3V0ZTogQWN0aXZhdGVkUm91dGVTbmFwc2hvdCwgZGV0YWNoZWRUcmVlOiBEZXRhY2hlZFJvdXRlSGFuZGxlKTogdm9pZCB7fVxuICBzaG91bGRBdHRhY2gocm91dGU6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QpOiBib29sZWFuIHsgcmV0dXJuIGZhbHNlOyB9XG4gIHJldHJpZXZlKHJvdXRlOiBBY3RpdmF0ZWRSb3V0ZVNuYXBzaG90KTogRGV0YWNoZWRSb3V0ZUhhbmRsZXxudWxsIHsgcmV0dXJuIG51bGw7IH1cbiAgc2hvdWxkUmV1c2VSb3V0ZShmdXR1cmU6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QsIGN1cnI6IEFjdGl2YXRlZFJvdXRlU25hcHNob3QpOiBib29sZWFuIHtcbiAgICByZXR1cm4gZnV0dXJlLnJvdXRlQ29uZmlnID09PSBjdXJyLnJvdXRlQ29uZmlnO1xuICB9XG59XG4iXX0=