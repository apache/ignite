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
import { injectTemplateRef as render3InjectTemplateRef } from '../render3/view_engine_compatibility';
import { noop } from '../util/noop';
import { ElementRef } from './element_ref';
/**
 * Represents an embedded template that can be used to instantiate embedded views.
 * To instantiate embedded views based on a template, use the `ViewContainerRef`
 * method `createEmbeddedView()`.
 *
 * Access a `TemplateRef` instance by placing a directive on an `<ng-template>`
 * element (or directive prefixed with `*`). The `TemplateRef` for the embedded view
 * is injected into the constructor of the directive,
 * using the `TemplateRef` token.
 *
 * You can also use a `Query` to find a `TemplateRef` associated with
 * a component or a directive.
 *
 * @see `ViewContainerRef`
 * @see [Navigate the Component Tree with DI](guide/dependency-injection-navtree)
 *
 * \@publicApi
 * @abstract
 * @template C
 */
export class TemplateRef {
}
/**
 * \@internal
 * @nocollapse
 */
TemplateRef.__NG_ELEMENT_ID__ = (/**
 * @return {?}
 */
() => SWITCH_TEMPLATE_REF_FACTORY(TemplateRef, ElementRef));
if (false) {
    /**
     * \@internal
     * @nocollapse
     * @type {?}
     */
    TemplateRef.__NG_ELEMENT_ID__;
    /**
     * The anchor element in the parent view for this embedded view.
     *
     * The data-binding and injection contexts of embedded views created from this `TemplateRef`
     * inherit from the contexts of this location.
     *
     * Typically new embedded views are attached to the view container of this location, but in
     * advanced use-cases, the view can be attached to a different container while keeping the
     * data-binding and injection context from the original location.
     *
     * @abstract
     * @return {?}
     */
    TemplateRef.prototype.elementRef = function () { };
    /**
     * Instantiates an embedded view based on this template,
     * and attaches it to the view container.
     * @abstract
     * @param {?} context The data-binding context of the embedded view, as declared
     * in the `<ng-template>` usage.
     * @return {?} The new embedded view object.
     */
    TemplateRef.prototype.createEmbeddedView = function (context) { };
}
/** @type {?} */
export const SWITCH_TEMPLATE_REF_FACTORY__POST_R3__ = render3InjectTemplateRef;
/** @type {?} */
const SWITCH_TEMPLATE_REF_FACTORY__PRE_R3__ = noop;
/** @type {?} */
const SWITCH_TEMPLATE_REF_FACTORY = SWITCH_TEMPLATE_REF_FACTORY__PRE_R3__;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVtcGxhdGVfcmVmLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvbGlua2VyL3RlbXBsYXRlX3JlZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxpQkFBaUIsSUFBSSx3QkFBd0IsRUFBQyxNQUFNLHNDQUFzQyxDQUFDO0FBQ25HLE9BQU8sRUFBQyxJQUFJLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFFbEMsT0FBTyxFQUFDLFVBQVUsRUFBQyxNQUFNLGVBQWUsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBc0J6QyxNQUFNLE9BQWdCLFdBQVc7Ozs7OztBQTRCeEIsNkJBQWlCOzs7QUFDVyxHQUFHLEVBQUUsQ0FBQywyQkFBMkIsQ0FBQyxXQUFXLEVBQUUsVUFBVSxDQUFDLEVBQUE7Ozs7Ozs7SUFEN0YsOEJBQzZGOzs7Ozs7Ozs7Ozs7OztJQWhCN0YsbURBQXNDOzs7Ozs7Ozs7SUFTdEMsa0VBQTREOzs7QUFVOUQsTUFBTSxPQUFPLHNDQUFzQyxHQUFHLHdCQUF3Qjs7TUFDeEUscUNBQXFDLEdBQUcsSUFBSTs7TUFDNUMsMkJBQTJCLEdBQzdCLHFDQUFxQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtpbmplY3RUZW1wbGF0ZVJlZiBhcyByZW5kZXIzSW5qZWN0VGVtcGxhdGVSZWZ9IGZyb20gJy4uL3JlbmRlcjMvdmlld19lbmdpbmVfY29tcGF0aWJpbGl0eSc7XG5pbXBvcnQge25vb3B9IGZyb20gJy4uL3V0aWwvbm9vcCc7XG5cbmltcG9ydCB7RWxlbWVudFJlZn0gZnJvbSAnLi9lbGVtZW50X3JlZic7XG5pbXBvcnQge0VtYmVkZGVkVmlld1JlZn0gZnJvbSAnLi92aWV3X3JlZic7XG5cblxuLyoqXG4gKiBSZXByZXNlbnRzIGFuIGVtYmVkZGVkIHRlbXBsYXRlIHRoYXQgY2FuIGJlIHVzZWQgdG8gaW5zdGFudGlhdGUgZW1iZWRkZWQgdmlld3MuXG4gKiBUbyBpbnN0YW50aWF0ZSBlbWJlZGRlZCB2aWV3cyBiYXNlZCBvbiBhIHRlbXBsYXRlLCB1c2UgdGhlIGBWaWV3Q29udGFpbmVyUmVmYFxuICogbWV0aG9kIGBjcmVhdGVFbWJlZGRlZFZpZXcoKWAuXG4gKlxuICogQWNjZXNzIGEgYFRlbXBsYXRlUmVmYCBpbnN0YW5jZSBieSBwbGFjaW5nIGEgZGlyZWN0aXZlIG9uIGFuIGA8bmctdGVtcGxhdGU+YFxuICogZWxlbWVudCAob3IgZGlyZWN0aXZlIHByZWZpeGVkIHdpdGggYCpgKS4gVGhlIGBUZW1wbGF0ZVJlZmAgZm9yIHRoZSBlbWJlZGRlZCB2aWV3XG4gKiBpcyBpbmplY3RlZCBpbnRvIHRoZSBjb25zdHJ1Y3RvciBvZiB0aGUgZGlyZWN0aXZlLFxuICogdXNpbmcgdGhlIGBUZW1wbGF0ZVJlZmAgdG9rZW4uXG4gKlxuICogWW91IGNhbiBhbHNvIHVzZSBhIGBRdWVyeWAgdG8gZmluZCBhIGBUZW1wbGF0ZVJlZmAgYXNzb2NpYXRlZCB3aXRoXG4gKiBhIGNvbXBvbmVudCBvciBhIGRpcmVjdGl2ZS5cbiAqXG4gKiBAc2VlIGBWaWV3Q29udGFpbmVyUmVmYFxuICogQHNlZSBbTmF2aWdhdGUgdGhlIENvbXBvbmVudCBUcmVlIHdpdGggREldKGd1aWRlL2RlcGVuZGVuY3ktaW5qZWN0aW9uLW5hdnRyZWUpXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgVGVtcGxhdGVSZWY8Qz4ge1xuICAvKipcbiAgICogVGhlIGFuY2hvciBlbGVtZW50IGluIHRoZSBwYXJlbnQgdmlldyBmb3IgdGhpcyBlbWJlZGRlZCB2aWV3LlxuICAgKlxuICAgKiBUaGUgZGF0YS1iaW5kaW5nIGFuZCBpbmplY3Rpb24gY29udGV4dHMgb2YgZW1iZWRkZWQgdmlld3MgY3JlYXRlZCBmcm9tIHRoaXMgYFRlbXBsYXRlUmVmYFxuICAgKiBpbmhlcml0IGZyb20gdGhlIGNvbnRleHRzIG9mIHRoaXMgbG9jYXRpb24uXG4gICAqXG4gICAqIFR5cGljYWxseSBuZXcgZW1iZWRkZWQgdmlld3MgYXJlIGF0dGFjaGVkIHRvIHRoZSB2aWV3IGNvbnRhaW5lciBvZiB0aGlzIGxvY2F0aW9uLCBidXQgaW5cbiAgICogYWR2YW5jZWQgdXNlLWNhc2VzLCB0aGUgdmlldyBjYW4gYmUgYXR0YWNoZWQgdG8gYSBkaWZmZXJlbnQgY29udGFpbmVyIHdoaWxlIGtlZXBpbmcgdGhlXG4gICAqIGRhdGEtYmluZGluZyBhbmQgaW5qZWN0aW9uIGNvbnRleHQgZnJvbSB0aGUgb3JpZ2luYWwgbG9jYXRpb24uXG4gICAqXG4gICAqL1xuICAvLyBUT0RPKGkpOiByZW5hbWUgdG8gYW5jaG9yIG9yIGxvY2F0aW9uXG4gIGFic3RyYWN0IGdldCBlbGVtZW50UmVmKCk6IEVsZW1lbnRSZWY7XG5cbiAgLyoqXG4gICAqIEluc3RhbnRpYXRlcyBhbiBlbWJlZGRlZCB2aWV3IGJhc2VkIG9uIHRoaXMgdGVtcGxhdGUsXG4gICAqIGFuZCBhdHRhY2hlcyBpdCB0byB0aGUgdmlldyBjb250YWluZXIuXG4gICAqIEBwYXJhbSBjb250ZXh0IFRoZSBkYXRhLWJpbmRpbmcgY29udGV4dCBvZiB0aGUgZW1iZWRkZWQgdmlldywgYXMgZGVjbGFyZWRcbiAgICogaW4gdGhlIGA8bmctdGVtcGxhdGU+YCB1c2FnZS5cbiAgICogQHJldHVybnMgVGhlIG5ldyBlbWJlZGRlZCB2aWV3IG9iamVjdC5cbiAgICovXG4gIGFic3RyYWN0IGNyZWF0ZUVtYmVkZGVkVmlldyhjb250ZXh0OiBDKTogRW1iZWRkZWRWaWV3UmVmPEM+O1xuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICogQG5vY29sbGFwc2VcbiAgICovXG4gIHN0YXRpYyBfX05HX0VMRU1FTlRfSURfXzpcbiAgICAgICgpID0+IFRlbXBsYXRlUmVmPGFueT58IG51bGwgPSAoKSA9PiBTV0lUQ0hfVEVNUExBVEVfUkVGX0ZBQ1RPUlkoVGVtcGxhdGVSZWYsIEVsZW1lbnRSZWYpXG59XG5cbmV4cG9ydCBjb25zdCBTV0lUQ0hfVEVNUExBVEVfUkVGX0ZBQ1RPUllfX1BPU1RfUjNfXyA9IHJlbmRlcjNJbmplY3RUZW1wbGF0ZVJlZjtcbmNvbnN0IFNXSVRDSF9URU1QTEFURV9SRUZfRkFDVE9SWV9fUFJFX1IzX18gPSBub29wO1xuY29uc3QgU1dJVENIX1RFTVBMQVRFX1JFRl9GQUNUT1JZOiB0eXBlb2YgcmVuZGVyM0luamVjdFRlbXBsYXRlUmVmID1cbiAgICBTV0lUQ0hfVEVNUExBVEVfUkVGX0ZBQ1RPUllfX1BSRV9SM19fO1xuIl19