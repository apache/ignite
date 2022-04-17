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
import { ChangeDetectorRef } from '../change_detection/change_detector_ref';
/**
 * Represents an Angular [view](guide/glossary#view),
 * specifically the [host view](guide/glossary#view-tree) that is defined by a component.
 * Also serves as the base class
 * that adds destroy methods for [embedded views](guide/glossary#view-tree).
 *
 * @see `EmbeddedViewRef`
 *
 * \@publicApi
 * @abstract
 */
export class ViewRef extends ChangeDetectorRef {
}
if (false) {
    /**
     * Destroys this view and all of the data structures associated with it.
     * @abstract
     * @return {?}
     */
    ViewRef.prototype.destroy = function () { };
    /**
     * Reports whether this view has been destroyed.
     * @abstract
     * @return {?} True after the `destroy()` method has been called, false otherwise.
     */
    ViewRef.prototype.destroyed = function () { };
    /**
     * A lifecycle hook that provides additional developer-defined cleanup
     * functionality for views.
     * @abstract
     * @param {?} callback A handler function that cleans up developer-defined data
     * associated with a view. Called when the `destroy()` method is invoked.
     * @return {?}
     */
    ViewRef.prototype.onDestroy = function (callback) { };
}
/**
 * Represents an Angular [view](guide/glossary#view) in a view container.
 * An [embedded view](guide/glossary#view-tree) can be referenced from a component
 * other than the hosting component whose template defines it, or it can be defined
 * independently by a `TemplateRef`.
 *
 * Properties of elements in a view can change, but the structure (number and order) of elements in
 * a view cannot. Change the structure of elements by inserting, moving, or
 * removing nested views in a view container.
 *
 * @see `ViewContainerRef`
 *
 * \@usageNotes
 *
 * The following template breaks down into two separate `TemplateRef` instances,
 * an outer one and an inner one.
 *
 * ```
 * Count: {{items.length}}
 * <ul>
 *   <li *ngFor="let  item of items">{{item}}</li>
 * </ul>
 * ```
 *
 * This is the outer `TemplateRef`:
 *
 * ```
 * Count: {{items.length}}
 * <ul>
 *   <ng-template ngFor let-item [ngForOf]="items"></ng-template>
 * </ul>
 * ```
 *
 * This is the inner `TemplateRef`:
 *
 * ```
 *   <li>{{item}}</li>
 * ```
 *
 * The outer and inner `TemplateRef` instances are assembled into views as follows:
 *
 * ```
 * <!-- ViewRef: outer-0 -->
 * Count: 2
 * <ul>
 *   <ng-template view-container-ref></ng-template>
 *   <!-- ViewRef: inner-1 --><li>first</li><!-- /ViewRef: inner-1 -->
 *   <!-- ViewRef: inner-2 --><li>second</li><!-- /ViewRef: inner-2 -->
 * </ul>
 * <!-- /ViewRef: outer-0 -->
 * ```
 * \@publicApi
 * @abstract
 * @template C
 */
export class EmbeddedViewRef extends ViewRef {
}
if (false) {
    /**
     * The context for this view, inherited from the anchor element.
     * @abstract
     * @return {?}
     */
    EmbeddedViewRef.prototype.context = function () { };
    /**
     * The root nodes for this embedded view.
     * @abstract
     * @return {?}
     */
    EmbeddedViewRef.prototype.rootNodes = function () { };
}
/**
 * @record
 */
export function InternalViewRef() { }
if (false) {
    /**
     * @return {?}
     */
    InternalViewRef.prototype.detachFromAppRef = function () { };
    /**
     * @param {?} appRef
     * @return {?}
     */
    InternalViewRef.prototype.attachToAppRef = function (appRef) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlld19yZWYuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9saW5rZXIvdmlld19yZWYudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFTQSxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSx5Q0FBeUMsQ0FBQzs7Ozs7Ozs7Ozs7O0FBWTFFLE1BQU0sT0FBZ0IsT0FBUSxTQUFRLGlCQUFpQjtDQW1CdEQ7Ozs7Ozs7SUFmQyw0Q0FBeUI7Ozs7OztJQU16Qiw4Q0FBa0M7Ozs7Ozs7OztJQVFsQyxzREFBOEQ7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQXdEaEUsTUFBTSxPQUFnQixlQUFtQixTQUFRLE9BQU87Q0FVdkQ7Ozs7Ozs7SUFOQyxvREFBMEI7Ozs7OztJQUsxQixzREFBZ0M7Ozs7O0FBR2xDLHFDQUdDOzs7OztJQUZDLDZEQUF5Qjs7Ozs7SUFDekIsaUVBQTZDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0FwcGxpY2F0aW9uUmVmfSBmcm9tICcuLi9hcHBsaWNhdGlvbl9yZWYnO1xuaW1wb3J0IHtDaGFuZ2VEZXRlY3RvclJlZn0gZnJvbSAnLi4vY2hhbmdlX2RldGVjdGlvbi9jaGFuZ2VfZGV0ZWN0b3JfcmVmJztcblxuLyoqXG4gKiBSZXByZXNlbnRzIGFuIEFuZ3VsYXIgW3ZpZXddKGd1aWRlL2dsb3NzYXJ5I3ZpZXcpLFxuICogc3BlY2lmaWNhbGx5IHRoZSBbaG9zdCB2aWV3XShndWlkZS9nbG9zc2FyeSN2aWV3LXRyZWUpIHRoYXQgaXMgZGVmaW5lZCBieSBhIGNvbXBvbmVudC5cbiAqIEFsc28gc2VydmVzIGFzIHRoZSBiYXNlIGNsYXNzXG4gKiB0aGF0IGFkZHMgZGVzdHJveSBtZXRob2RzIGZvciBbZW1iZWRkZWQgdmlld3NdKGd1aWRlL2dsb3NzYXJ5I3ZpZXctdHJlZSkuXG4gKlxuICogQHNlZSBgRW1iZWRkZWRWaWV3UmVmYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIFZpZXdSZWYgZXh0ZW5kcyBDaGFuZ2VEZXRlY3RvclJlZiB7XG4gIC8qKlxuICAgKiBEZXN0cm95cyB0aGlzIHZpZXcgYW5kIGFsbCBvZiB0aGUgZGF0YSBzdHJ1Y3R1cmVzIGFzc29jaWF0ZWQgd2l0aCBpdC5cbiAgICovXG4gIGFic3RyYWN0IGRlc3Ryb3koKTogdm9pZDtcblxuICAvKipcbiAgICogUmVwb3J0cyB3aGV0aGVyIHRoaXMgdmlldyBoYXMgYmVlbiBkZXN0cm95ZWQuXG4gICAqIEByZXR1cm5zIFRydWUgYWZ0ZXIgdGhlIGBkZXN0cm95KClgIG1ldGhvZCBoYXMgYmVlbiBjYWxsZWQsIGZhbHNlIG90aGVyd2lzZS5cbiAgICovXG4gIGFic3RyYWN0IGdldCBkZXN0cm95ZWQoKTogYm9vbGVhbjtcblxuICAvKipcbiAgICogQSBsaWZlY3ljbGUgaG9vayB0aGF0IHByb3ZpZGVzIGFkZGl0aW9uYWwgZGV2ZWxvcGVyLWRlZmluZWQgY2xlYW51cFxuICAgKiBmdW5jdGlvbmFsaXR5IGZvciB2aWV3cy5cbiAgICogQHBhcmFtIGNhbGxiYWNrIEEgaGFuZGxlciBmdW5jdGlvbiB0aGF0IGNsZWFucyB1cCBkZXZlbG9wZXItZGVmaW5lZCBkYXRhXG4gICAqIGFzc29jaWF0ZWQgd2l0aCBhIHZpZXcuIENhbGxlZCB3aGVuIHRoZSBgZGVzdHJveSgpYCBtZXRob2QgaXMgaW52b2tlZC5cbiAgICovXG4gIGFic3RyYWN0IG9uRGVzdHJveShjYWxsYmFjazogRnVuY3Rpb24pOiBhbnkgLyoqIFRPRE8gIzkxMDAgKi87XG59XG5cbi8qKlxuICogUmVwcmVzZW50cyBhbiBBbmd1bGFyIFt2aWV3XShndWlkZS9nbG9zc2FyeSN2aWV3KSBpbiBhIHZpZXcgY29udGFpbmVyLlxuICogQW4gW2VtYmVkZGVkIHZpZXddKGd1aWRlL2dsb3NzYXJ5I3ZpZXctdHJlZSkgY2FuIGJlIHJlZmVyZW5jZWQgZnJvbSBhIGNvbXBvbmVudFxuICogb3RoZXIgdGhhbiB0aGUgaG9zdGluZyBjb21wb25lbnQgd2hvc2UgdGVtcGxhdGUgZGVmaW5lcyBpdCwgb3IgaXQgY2FuIGJlIGRlZmluZWRcbiAqIGluZGVwZW5kZW50bHkgYnkgYSBgVGVtcGxhdGVSZWZgLlxuICpcbiAqIFByb3BlcnRpZXMgb2YgZWxlbWVudHMgaW4gYSB2aWV3IGNhbiBjaGFuZ2UsIGJ1dCB0aGUgc3RydWN0dXJlIChudW1iZXIgYW5kIG9yZGVyKSBvZiBlbGVtZW50cyBpblxuICogYSB2aWV3IGNhbm5vdC4gQ2hhbmdlIHRoZSBzdHJ1Y3R1cmUgb2YgZWxlbWVudHMgYnkgaW5zZXJ0aW5nLCBtb3ZpbmcsIG9yXG4gKiByZW1vdmluZyBuZXN0ZWQgdmlld3MgaW4gYSB2aWV3IGNvbnRhaW5lci5cbiAqXG4gKiBAc2VlIGBWaWV3Q29udGFpbmVyUmVmYFxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogVGhlIGZvbGxvd2luZyB0ZW1wbGF0ZSBicmVha3MgZG93biBpbnRvIHR3byBzZXBhcmF0ZSBgVGVtcGxhdGVSZWZgIGluc3RhbmNlcyxcbiAqIGFuIG91dGVyIG9uZSBhbmQgYW4gaW5uZXIgb25lLlxuICpcbiAqIGBgYFxuICogQ291bnQ6IHt7aXRlbXMubGVuZ3RofX1cbiAqIDx1bD5cbiAqICAgPGxpICpuZ0Zvcj1cImxldCAgaXRlbSBvZiBpdGVtc1wiPnt7aXRlbX19PC9saT5cbiAqIDwvdWw+XG4gKiBgYGBcbiAqXG4gKiBUaGlzIGlzIHRoZSBvdXRlciBgVGVtcGxhdGVSZWZgOlxuICpcbiAqIGBgYFxuICogQ291bnQ6IHt7aXRlbXMubGVuZ3RofX1cbiAqIDx1bD5cbiAqICAgPG5nLXRlbXBsYXRlIG5nRm9yIGxldC1pdGVtIFtuZ0Zvck9mXT1cIml0ZW1zXCI+PC9uZy10ZW1wbGF0ZT5cbiAqIDwvdWw+XG4gKiBgYGBcbiAqXG4gKiBUaGlzIGlzIHRoZSBpbm5lciBgVGVtcGxhdGVSZWZgOlxuICpcbiAqIGBgYFxuICogICA8bGk+e3tpdGVtfX08L2xpPlxuICogYGBgXG4gKlxuICogVGhlIG91dGVyIGFuZCBpbm5lciBgVGVtcGxhdGVSZWZgIGluc3RhbmNlcyBhcmUgYXNzZW1ibGVkIGludG8gdmlld3MgYXMgZm9sbG93czpcbiAqXG4gKiBgYGBcbiAqIDwhLS0gVmlld1JlZjogb3V0ZXItMCAtLT5cbiAqIENvdW50OiAyXG4gKiA8dWw+XG4gKiAgIDxuZy10ZW1wbGF0ZSB2aWV3LWNvbnRhaW5lci1yZWY+PC9uZy10ZW1wbGF0ZT5cbiAqICAgPCEtLSBWaWV3UmVmOiBpbm5lci0xIC0tPjxsaT5maXJzdDwvbGk+PCEtLSAvVmlld1JlZjogaW5uZXItMSAtLT5cbiAqICAgPCEtLSBWaWV3UmVmOiBpbm5lci0yIC0tPjxsaT5zZWNvbmQ8L2xpPjwhLS0gL1ZpZXdSZWY6IGlubmVyLTIgLS0+XG4gKiA8L3VsPlxuICogPCEtLSAvVmlld1JlZjogb3V0ZXItMCAtLT5cbiAqIGBgYFxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgRW1iZWRkZWRWaWV3UmVmPEM+IGV4dGVuZHMgVmlld1JlZiB7XG4gIC8qKlxuICAgKiBUaGUgY29udGV4dCBmb3IgdGhpcyB2aWV3LCBpbmhlcml0ZWQgZnJvbSB0aGUgYW5jaG9yIGVsZW1lbnQuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgY29udGV4dCgpOiBDO1xuXG4gIC8qKlxuICAgKiBUaGUgcm9vdCBub2RlcyBmb3IgdGhpcyBlbWJlZGRlZCB2aWV3LlxuICAgKi9cbiAgYWJzdHJhY3QgZ2V0IHJvb3ROb2RlcygpOiBhbnlbXTtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBJbnRlcm5hbFZpZXdSZWYgZXh0ZW5kcyBWaWV3UmVmIHtcbiAgZGV0YWNoRnJvbUFwcFJlZigpOiB2b2lkO1xuICBhdHRhY2hUb0FwcFJlZihhcHBSZWY6IEFwcGxpY2F0aW9uUmVmKTogdm9pZDtcbn1cbiJdfQ==