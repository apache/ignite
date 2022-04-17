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
import { injectChangeDetectorRef as render3InjectChangeDetectorRef } from '../render3/view_engine_compatibility';
/**
 * Base class for Angular Views, provides change detection functionality.
 * A change-detection tree collects all views that are to be checked for changes.
 * Use the methods to add and remove views from the tree, initiate change-detection,
 * and explicitly mark views as _dirty_, meaning that they have changed and need to be rerendered.
 *
 * \@usageNotes
 *
 * The following examples demonstrate how to modify default change-detection behavior
 * to perform explicit detection when needed.
 *
 * ### Use `markForCheck()` with `CheckOnce` strategy
 *
 * The following example sets the `OnPush` change-detection strategy for a component
 * (`CheckOnce`, rather than the default `CheckAlways`), then forces a second check
 * after an interval. See [live demo](http://plnkr.co/edit/GC512b?p=preview).
 *
 * <code-example path="core/ts/change_detect/change-detection.ts"
 * region="mark-for-check"></code-example>
 *
 * ### Detach change detector to limit how often check occurs
 *
 * The following example defines a component with a large list of read-only data
 * that is expected to change constantly, many times per second.
 * To improve performance, we want to check and update the list
 * less often than the changes actually occur. To do that, we detach
 * the component's change detector and perform an explicit local check every five seconds.
 *
 * <code-example path="core/ts/change_detect/change-detection.ts" region="detach"></code-example>
 *
 *
 * ### Reattaching a detached component
 *
 * The following example creates a component displaying live data.
 * The component detaches its change detector from the main change detector tree
 * when the `live` property is set to false, and reattaches it when the property
 * becomes true.
 *
 * <code-example path="core/ts/change_detect/change-detection.ts" region="reattach"></code-example>
 *
 * \@publicApi
 * @abstract
 */
export class ChangeDetectorRef {
}
/**
 * \@internal
 * @nocollapse
 */
ChangeDetectorRef.__NG_ELEMENT_ID__ = (/**
 * @return {?}
 */
() => SWITCH_CHANGE_DETECTOR_REF_FACTORY());
if (false) {
    /**
     * \@internal
     * @nocollapse
     * @type {?}
     */
    ChangeDetectorRef.__NG_ELEMENT_ID__;
    /**
     * When a view uses the {\@link ChangeDetectionStrategy#OnPush OnPush} (checkOnce)
     * change detection strategy, explicitly marks the view as changed so that
     * it can be checked again.
     *
     * Components are normally marked as dirty (in need of rerendering) when inputs
     * have changed or events have fired in the view. Call this method to ensure that
     * a component is checked even if these triggers have not occured.
     *
     * <!-- TODO: Add a link to a chapter on OnPush components -->
     *
     * @abstract
     * @return {?}
     */
    ChangeDetectorRef.prototype.markForCheck = function () { };
    /**
     * Detaches this view from the change-detection tree.
     * A detached view is  not checked until it is reattached.
     * Use in combination with `detectChanges()` to implement local change detection checks.
     *
     * Detached views are not checked during change detection runs until they are
     * re-attached, even if they are marked as dirty.
     *
     * <!-- TODO: Add a link to a chapter on detach/reattach/local digest -->
     * <!-- TODO: Add a live demo once ref.detectChanges is merged into master -->
     *
     * @abstract
     * @return {?}
     */
    ChangeDetectorRef.prototype.detach = function () { };
    /**
     * Checks this view and its children. Use in combination with {\@link ChangeDetectorRef#detach
     * detach}
     * to implement local change detection checks.
     *
     * <!-- TODO: Add a link to a chapter on detach/reattach/local digest -->
     * <!-- TODO: Add a live demo once ref.detectChanges is merged into master -->
     *
     * @abstract
     * @return {?}
     */
    ChangeDetectorRef.prototype.detectChanges = function () { };
    /**
     * Checks the change detector and its children, and throws if any changes are detected.
     *
     * Use in development mode to verify that running change detection doesn't introduce
     * other changes.
     * @abstract
     * @return {?}
     */
    ChangeDetectorRef.prototype.checkNoChanges = function () { };
    /**
     * Re-attaches the previously detached view to the change detection tree.
     * Views are attached to the tree by default.
     *
     * <!-- TODO: Add a link to a chapter on detach/reattach/local digest -->
     *
     * @abstract
     * @return {?}
     */
    ChangeDetectorRef.prototype.reattach = function () { };
}
/** @type {?} */
export const SWITCH_CHANGE_DETECTOR_REF_FACTORY__POST_R3__ = render3InjectChangeDetectorRef;
/** @type {?} */
const SWITCH_CHANGE_DETECTOR_REF_FACTORY__PRE_R3__ = (/**
 * @param {...?} args
 * @return {?}
 */
(...args) => { });
const ɵ0 = SWITCH_CHANGE_DETECTOR_REF_FACTORY__PRE_R3__;
/** @type {?} */
const SWITCH_CHANGE_DETECTOR_REF_FACTORY = SWITCH_CHANGE_DETECTOR_REF_FACTORY__PRE_R3__;
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2hhbmdlX2RldGVjdG9yX3JlZi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL2NoYW5nZV9kZXRlY3Rpb24vY2hhbmdlX2RldGVjdG9yX3JlZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyx1QkFBdUIsSUFBSSw4QkFBOEIsRUFBQyxNQUFNLHNDQUFzQyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQTRDL0csTUFBTSxPQUFnQixpQkFBaUI7Ozs7OztBQTZEOUIsbUNBQWlCOzs7QUFBNEIsR0FBRyxFQUFFLENBQUMsa0NBQWtDLEVBQUUsRUFBQzs7Ozs7OztJQUEvRixvQ0FBK0Y7Ozs7Ozs7Ozs7Ozs7OztJQWhEL0YsMkRBQThCOzs7Ozs7Ozs7Ozs7Ozs7SUFjOUIscURBQXdCOzs7Ozs7Ozs7Ozs7SUFXeEIsNERBQStCOzs7Ozs7Ozs7SUFRL0IsNkRBQWdDOzs7Ozs7Ozs7O0lBU2hDLHVEQUEwQjs7O0FBVzVCLE1BQU0sT0FBTyw2Q0FBNkMsR0FBRyw4QkFBOEI7O01BQ3JGLDRDQUE0Qzs7OztBQUFHLENBQUMsR0FBRyxJQUFXLEVBQU8sRUFBRSxHQUFFLENBQUMsQ0FBQTs7O01BQzFFLGtDQUFrQyxHQUNwQyw0Q0FBNEMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7aW5qZWN0Q2hhbmdlRGV0ZWN0b3JSZWYgYXMgcmVuZGVyM0luamVjdENoYW5nZURldGVjdG9yUmVmfSBmcm9tICcuLi9yZW5kZXIzL3ZpZXdfZW5naW5lX2NvbXBhdGliaWxpdHknO1xuXG4vKipcbiAqIEJhc2UgY2xhc3MgZm9yIEFuZ3VsYXIgVmlld3MsIHByb3ZpZGVzIGNoYW5nZSBkZXRlY3Rpb24gZnVuY3Rpb25hbGl0eS5cbiAqIEEgY2hhbmdlLWRldGVjdGlvbiB0cmVlIGNvbGxlY3RzIGFsbCB2aWV3cyB0aGF0IGFyZSB0byBiZSBjaGVja2VkIGZvciBjaGFuZ2VzLlxuICogVXNlIHRoZSBtZXRob2RzIHRvIGFkZCBhbmQgcmVtb3ZlIHZpZXdzIGZyb20gdGhlIHRyZWUsIGluaXRpYXRlIGNoYW5nZS1kZXRlY3Rpb24sXG4gKiBhbmQgZXhwbGljaXRseSBtYXJrIHZpZXdzIGFzIF9kaXJ0eV8sIG1lYW5pbmcgdGhhdCB0aGV5IGhhdmUgY2hhbmdlZCBhbmQgbmVlZCB0byBiZSByZXJlbmRlcmVkLlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogVGhlIGZvbGxvd2luZyBleGFtcGxlcyBkZW1vbnN0cmF0ZSBob3cgdG8gbW9kaWZ5IGRlZmF1bHQgY2hhbmdlLWRldGVjdGlvbiBiZWhhdmlvclxuICogdG8gcGVyZm9ybSBleHBsaWNpdCBkZXRlY3Rpb24gd2hlbiBuZWVkZWQuXG4gKlxuICogIyMjIFVzZSBgbWFya0ZvckNoZWNrKClgIHdpdGggYENoZWNrT25jZWAgc3RyYXRlZ3lcbiAqXG4gKiBUaGUgZm9sbG93aW5nIGV4YW1wbGUgc2V0cyB0aGUgYE9uUHVzaGAgY2hhbmdlLWRldGVjdGlvbiBzdHJhdGVneSBmb3IgYSBjb21wb25lbnRcbiAqIChgQ2hlY2tPbmNlYCwgcmF0aGVyIHRoYW4gdGhlIGRlZmF1bHQgYENoZWNrQWx3YXlzYCksIHRoZW4gZm9yY2VzIGEgc2Vjb25kIGNoZWNrXG4gKiBhZnRlciBhbiBpbnRlcnZhbC4gU2VlIFtsaXZlIGRlbW9dKGh0dHA6Ly9wbG5rci5jby9lZGl0L0dDNTEyYj9wPXByZXZpZXcpLlxuICpcbiAqIDxjb2RlLWV4YW1wbGUgcGF0aD1cImNvcmUvdHMvY2hhbmdlX2RldGVjdC9jaGFuZ2UtZGV0ZWN0aW9uLnRzXCJcbiAqIHJlZ2lvbj1cIm1hcmstZm9yLWNoZWNrXCI+PC9jb2RlLWV4YW1wbGU+XG4gKlxuICogIyMjIERldGFjaCBjaGFuZ2UgZGV0ZWN0b3IgdG8gbGltaXQgaG93IG9mdGVuIGNoZWNrIG9jY3Vyc1xuICpcbiAqIFRoZSBmb2xsb3dpbmcgZXhhbXBsZSBkZWZpbmVzIGEgY29tcG9uZW50IHdpdGggYSBsYXJnZSBsaXN0IG9mIHJlYWQtb25seSBkYXRhXG4gKiB0aGF0IGlzIGV4cGVjdGVkIHRvIGNoYW5nZSBjb25zdGFudGx5LCBtYW55IHRpbWVzIHBlciBzZWNvbmQuXG4gKiBUbyBpbXByb3ZlIHBlcmZvcm1hbmNlLCB3ZSB3YW50IHRvIGNoZWNrIGFuZCB1cGRhdGUgdGhlIGxpc3RcbiAqIGxlc3Mgb2Z0ZW4gdGhhbiB0aGUgY2hhbmdlcyBhY3R1YWxseSBvY2N1ci4gVG8gZG8gdGhhdCwgd2UgZGV0YWNoXG4gKiB0aGUgY29tcG9uZW50J3MgY2hhbmdlIGRldGVjdG9yIGFuZCBwZXJmb3JtIGFuIGV4cGxpY2l0IGxvY2FsIGNoZWNrIGV2ZXJ5IGZpdmUgc2Vjb25kcy5cbiAqXG4gKiA8Y29kZS1leGFtcGxlIHBhdGg9XCJjb3JlL3RzL2NoYW5nZV9kZXRlY3QvY2hhbmdlLWRldGVjdGlvbi50c1wiIHJlZ2lvbj1cImRldGFjaFwiPjwvY29kZS1leGFtcGxlPlxuICpcbiAqXG4gKiAjIyMgUmVhdHRhY2hpbmcgYSBkZXRhY2hlZCBjb21wb25lbnRcbiAqXG4gKiBUaGUgZm9sbG93aW5nIGV4YW1wbGUgY3JlYXRlcyBhIGNvbXBvbmVudCBkaXNwbGF5aW5nIGxpdmUgZGF0YS5cbiAqIFRoZSBjb21wb25lbnQgZGV0YWNoZXMgaXRzIGNoYW5nZSBkZXRlY3RvciBmcm9tIHRoZSBtYWluIGNoYW5nZSBkZXRlY3RvciB0cmVlXG4gKiB3aGVuIHRoZSBgbGl2ZWAgcHJvcGVydHkgaXMgc2V0IHRvIGZhbHNlLCBhbmQgcmVhdHRhY2hlcyBpdCB3aGVuIHRoZSBwcm9wZXJ0eVxuICogYmVjb21lcyB0cnVlLlxuICpcbiAqIDxjb2RlLWV4YW1wbGUgcGF0aD1cImNvcmUvdHMvY2hhbmdlX2RldGVjdC9jaGFuZ2UtZGV0ZWN0aW9uLnRzXCIgcmVnaW9uPVwicmVhdHRhY2hcIj48L2NvZGUtZXhhbXBsZT5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBDaGFuZ2VEZXRlY3RvclJlZiB7XG4gIC8qKlxuICAgKiBXaGVuIGEgdmlldyB1c2VzIHRoZSB7QGxpbmsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kjT25QdXNoIE9uUHVzaH0gKGNoZWNrT25jZSlcbiAgICogY2hhbmdlIGRldGVjdGlvbiBzdHJhdGVneSwgZXhwbGljaXRseSBtYXJrcyB0aGUgdmlldyBhcyBjaGFuZ2VkIHNvIHRoYXRcbiAgICogaXQgY2FuIGJlIGNoZWNrZWQgYWdhaW4uXG4gICAqXG4gICAqIENvbXBvbmVudHMgYXJlIG5vcm1hbGx5IG1hcmtlZCBhcyBkaXJ0eSAoaW4gbmVlZCBvZiByZXJlbmRlcmluZykgd2hlbiBpbnB1dHNcbiAgICogaGF2ZSBjaGFuZ2VkIG9yIGV2ZW50cyBoYXZlIGZpcmVkIGluIHRoZSB2aWV3LiBDYWxsIHRoaXMgbWV0aG9kIHRvIGVuc3VyZSB0aGF0XG4gICAqIGEgY29tcG9uZW50IGlzIGNoZWNrZWQgZXZlbiBpZiB0aGVzZSB0cmlnZ2VycyBoYXZlIG5vdCBvY2N1cmVkLlxuICAgKlxuICAgKiA8IS0tIFRPRE86IEFkZCBhIGxpbmsgdG8gYSBjaGFwdGVyIG9uIE9uUHVzaCBjb21wb25lbnRzIC0tPlxuICAgKlxuICAgKi9cbiAgYWJzdHJhY3QgbWFya0ZvckNoZWNrKCk6IHZvaWQ7XG5cbiAgLyoqXG4gICAqIERldGFjaGVzIHRoaXMgdmlldyBmcm9tIHRoZSBjaGFuZ2UtZGV0ZWN0aW9uIHRyZWUuXG4gICAqIEEgZGV0YWNoZWQgdmlldyBpcyAgbm90IGNoZWNrZWQgdW50aWwgaXQgaXMgcmVhdHRhY2hlZC5cbiAgICogVXNlIGluIGNvbWJpbmF0aW9uIHdpdGggYGRldGVjdENoYW5nZXMoKWAgdG8gaW1wbGVtZW50IGxvY2FsIGNoYW5nZSBkZXRlY3Rpb24gY2hlY2tzLlxuICAgKlxuICAgKiBEZXRhY2hlZCB2aWV3cyBhcmUgbm90IGNoZWNrZWQgZHVyaW5nIGNoYW5nZSBkZXRlY3Rpb24gcnVucyB1bnRpbCB0aGV5IGFyZVxuICAgKiByZS1hdHRhY2hlZCwgZXZlbiBpZiB0aGV5IGFyZSBtYXJrZWQgYXMgZGlydHkuXG4gICAqXG4gICAqIDwhLS0gVE9ETzogQWRkIGEgbGluayB0byBhIGNoYXB0ZXIgb24gZGV0YWNoL3JlYXR0YWNoL2xvY2FsIGRpZ2VzdCAtLT5cbiAgICogPCEtLSBUT0RPOiBBZGQgYSBsaXZlIGRlbW8gb25jZSByZWYuZGV0ZWN0Q2hhbmdlcyBpcyBtZXJnZWQgaW50byBtYXN0ZXIgLS0+XG4gICAqXG4gICAqL1xuICBhYnN0cmFjdCBkZXRhY2goKTogdm9pZDtcblxuICAvKipcbiAgICogQ2hlY2tzIHRoaXMgdmlldyBhbmQgaXRzIGNoaWxkcmVuLiBVc2UgaW4gY29tYmluYXRpb24gd2l0aCB7QGxpbmsgQ2hhbmdlRGV0ZWN0b3JSZWYjZGV0YWNoXG4gICAqIGRldGFjaH1cbiAgICogdG8gaW1wbGVtZW50IGxvY2FsIGNoYW5nZSBkZXRlY3Rpb24gY2hlY2tzLlxuICAgKlxuICAgKiA8IS0tIFRPRE86IEFkZCBhIGxpbmsgdG8gYSBjaGFwdGVyIG9uIGRldGFjaC9yZWF0dGFjaC9sb2NhbCBkaWdlc3QgLS0+XG4gICAqIDwhLS0gVE9ETzogQWRkIGEgbGl2ZSBkZW1vIG9uY2UgcmVmLmRldGVjdENoYW5nZXMgaXMgbWVyZ2VkIGludG8gbWFzdGVyIC0tPlxuICAgKlxuICAgKi9cbiAgYWJzdHJhY3QgZGV0ZWN0Q2hhbmdlcygpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBDaGVja3MgdGhlIGNoYW5nZSBkZXRlY3RvciBhbmQgaXRzIGNoaWxkcmVuLCBhbmQgdGhyb3dzIGlmIGFueSBjaGFuZ2VzIGFyZSBkZXRlY3RlZC5cbiAgICpcbiAgICogVXNlIGluIGRldmVsb3BtZW50IG1vZGUgdG8gdmVyaWZ5IHRoYXQgcnVubmluZyBjaGFuZ2UgZGV0ZWN0aW9uIGRvZXNuJ3QgaW50cm9kdWNlXG4gICAqIG90aGVyIGNoYW5nZXMuXG4gICAqL1xuICBhYnN0cmFjdCBjaGVja05vQ2hhbmdlcygpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBSZS1hdHRhY2hlcyB0aGUgcHJldmlvdXNseSBkZXRhY2hlZCB2aWV3IHRvIHRoZSBjaGFuZ2UgZGV0ZWN0aW9uIHRyZWUuXG4gICAqIFZpZXdzIGFyZSBhdHRhY2hlZCB0byB0aGUgdHJlZSBieSBkZWZhdWx0LlxuICAgKlxuICAgKiA8IS0tIFRPRE86IEFkZCBhIGxpbmsgdG8gYSBjaGFwdGVyIG9uIGRldGFjaC9yZWF0dGFjaC9sb2NhbCBkaWdlc3QgLS0+XG4gICAqXG4gICAqL1xuICBhYnN0cmFjdCByZWF0dGFjaCgpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICogQG5vY29sbGFwc2VcbiAgICovXG4gIHN0YXRpYyBfX05HX0VMRU1FTlRfSURfXzogKCkgPT4gQ2hhbmdlRGV0ZWN0b3JSZWYgPSAoKSA9PiBTV0lUQ0hfQ0hBTkdFX0RFVEVDVE9SX1JFRl9GQUNUT1JZKCk7XG59XG5cblxuXG5leHBvcnQgY29uc3QgU1dJVENIX0NIQU5HRV9ERVRFQ1RPUl9SRUZfRkFDVE9SWV9fUE9TVF9SM19fID0gcmVuZGVyM0luamVjdENoYW5nZURldGVjdG9yUmVmO1xuY29uc3QgU1dJVENIX0NIQU5HRV9ERVRFQ1RPUl9SRUZfRkFDVE9SWV9fUFJFX1IzX18gPSAoLi4uYXJnczogYW55W10pOiBhbnkgPT4ge307XG5jb25zdCBTV0lUQ0hfQ0hBTkdFX0RFVEVDVE9SX1JFRl9GQUNUT1JZOiB0eXBlb2YgcmVuZGVyM0luamVjdENoYW5nZURldGVjdG9yUmVmID1cbiAgICBTV0lUQ0hfQ0hBTkdFX0RFVEVDVE9SX1JFRl9GQUNUT1JZX19QUkVfUjNfXztcbiJdfQ==