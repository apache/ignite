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
import { injectViewContainerRef as render3InjectViewContainerRef } from '../render3/view_engine_compatibility';
import { noop } from '../util/noop';
import { ElementRef } from './element_ref';
/**
 * Represents a container where one or more views can be attached to a component.
 *
 * Can contain *host views* (created by instantiating a
 * component with the `createComponent()` method), and *embedded views*
 * (created by instantiating a `TemplateRef` with the `createEmbeddedView()` method).
 *
 * A view container instance can contain other view containers,
 * creating a [view hierarchy](guide/glossary#view-tree).
 *
 * @see `ComponentRef`
 * @see `EmbeddedViewRef`
 *
 * \@publicApi
 * @abstract
 */
export class ViewContainerRef {
}
/**
 * \@internal
 * @nocollapse
 */
ViewContainerRef.__NG_ELEMENT_ID__ = (/**
 * @return {?}
 */
() => SWITCH_VIEW_CONTAINER_REF_FACTORY(ViewContainerRef, ElementRef));
if (false) {
    /**
     * \@internal
     * @nocollapse
     * @type {?}
     */
    ViewContainerRef.__NG_ELEMENT_ID__;
    /**
     * Anchor element that specifies the location of this container in the containing view.
     * Each view container can have only one anchor element, and each anchor element
     * can have only a single view container.
     *
     * Root elements of views attached to this container become siblings of the anchor element in
     * the rendered view.
     *
     * Access the `ViewContainerRef` of an element by placing a `Directive` injected
     * with `ViewContainerRef` on the element, or use a `ViewChild` query.
     *
     * <!-- TODO: rename to anchorElement -->
     * @abstract
     * @return {?}
     */
    ViewContainerRef.prototype.element = function () { };
    /**
     * The [dependency injector](guide/glossary#injector) for this view container.
     * @abstract
     * @return {?}
     */
    ViewContainerRef.prototype.injector = function () { };
    /**
     * @deprecated No replacement
     * @abstract
     * @return {?}
     */
    ViewContainerRef.prototype.parentInjector = function () { };
    /**
     * Destroys all views in this container.
     * @abstract
     * @return {?}
     */
    ViewContainerRef.prototype.clear = function () { };
    /**
     * Retrieves a view from this container.
     * @abstract
     * @param {?} index The 0-based index of the view to retrieve.
     * @return {?} The `ViewRef` instance, or null if the index is out of range.
     */
    ViewContainerRef.prototype.get = function (index) { };
    /**
     * Reports how many views are currently attached to this container.
     * @abstract
     * @return {?} The number of views.
     */
    ViewContainerRef.prototype.length = function () { };
    /**
     * Instantiates an embedded view and inserts it
     * into this container.
     * @abstract
     * @template C
     * @param {?} templateRef The HTML template that defines the view.
     * @param {?=} context
     * @param {?=} index The 0-based index at which to insert the new view into this container.
     * If not specified, appends the new view as the last entry.
     *
     * @return {?} The `ViewRef` instance for the newly created view.
     */
    ViewContainerRef.prototype.createEmbeddedView = function (templateRef, context, index) { };
    /**
     * Instantiates a single component and inserts its host view into this container.
     *
     * @abstract
     * @template C
     * @param {?} componentFactory The factory to use.
     * @param {?=} index The index at which to insert the new component's host view into this container.
     * If not specified, appends the new view as the last entry.
     * @param {?=} injector The injector to use as the parent for the new component.
     * @param {?=} projectableNodes
     * @param {?=} ngModule
     *
     * @return {?} The new component instance, containing the host view.
     *
     */
    ViewContainerRef.prototype.createComponent = function (componentFactory, index, injector, projectableNodes, ngModule) { };
    /**
     * Inserts a view into this container.
     * @abstract
     * @param {?} viewRef The view to insert.
     * @param {?=} index The 0-based index at which to insert the view.
     * If not specified, appends the new view as the last entry.
     * @return {?} The inserted `ViewRef` instance.
     *
     */
    ViewContainerRef.prototype.insert = function (viewRef, index) { };
    /**
     * Moves a view to a new location in this container.
     * @abstract
     * @param {?} viewRef The view to move.
     * @param {?} currentIndex
     * @return {?} The moved `ViewRef` instance.
     */
    ViewContainerRef.prototype.move = function (viewRef, currentIndex) { };
    /**
     * Returns the index of a view within the current container.
     * @abstract
     * @param {?} viewRef The view to query.
     * @return {?} The 0-based index of the view's position in this container,
     * or `-1` if this container doesn't contain the view.
     */
    ViewContainerRef.prototype.indexOf = function (viewRef) { };
    /**
     * Destroys a view attached to this container
     * @abstract
     * @param {?=} index The 0-based index of the view to destroy.
     * If not specified, the last view in the container is removed.
     * @return {?}
     */
    ViewContainerRef.prototype.remove = function (index) { };
    /**
     * Detaches a view from this container without destroying it.
     * Use along with `insert()` to move a view within the current container.
     * @abstract
     * @param {?=} index The 0-based index of the view to detach.
     * If not specified, the last view in the container is detached.
     * @return {?}
     */
    ViewContainerRef.prototype.detach = function (index) { };
}
/** @type {?} */
export const SWITCH_VIEW_CONTAINER_REF_FACTORY__POST_R3__ = render3InjectViewContainerRef;
/** @type {?} */
const SWITCH_VIEW_CONTAINER_REF_FACTORY__PRE_R3__ = noop;
/** @type {?} */
const SWITCH_VIEW_CONTAINER_REF_FACTORY = SWITCH_VIEW_CONTAINER_REF_FACTORY__PRE_R3__;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlld19jb250YWluZXJfcmVmLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvbGlua2VyL3ZpZXdfY29udGFpbmVyX3JlZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVNBLE9BQU8sRUFBQyxzQkFBc0IsSUFBSSw2QkFBNkIsRUFBQyxNQUFNLHNDQUFzQyxDQUFDO0FBQzdHLE9BQU8sRUFBQyxJQUFJLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFHbEMsT0FBTyxFQUFDLFVBQVUsRUFBQyxNQUFNLGVBQWUsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFxQnpDLE1BQU0sT0FBZ0IsZ0JBQWdCOzs7Ozs7QUFvSDdCLGtDQUFpQjs7O0FBQ0ssR0FBRyxFQUFFLENBQUMsaUNBQWlDLENBQUMsZ0JBQWdCLEVBQUUsVUFBVSxDQUFDLEVBQUE7Ozs7Ozs7SUFEbEcsbUNBQ2tHOzs7Ozs7Ozs7Ozs7Ozs7O0lBdkdsRyxxREFBbUM7Ozs7OztJQUtuQyxzREFBa0M7Ozs7OztJQUdsQyw0REFBd0M7Ozs7OztJQUt4QyxtREFBdUI7Ozs7Ozs7SUFPdkIsc0RBQTBDOzs7Ozs7SUFNMUMsb0RBQThCOzs7Ozs7Ozs7Ozs7O0lBVzlCLDJGQUN1Qjs7Ozs7Ozs7Ozs7Ozs7OztJQWV2QiwwSEFFOEU7Ozs7Ozs7Ozs7SUFVOUUsa0VBQTJEOzs7Ozs7OztJQVEzRCx1RUFBK0Q7Ozs7Ozs7O0lBUS9ELDREQUEyQzs7Ozs7Ozs7SUFPM0MseURBQXNDOzs7Ozs7Ozs7SUFRdEMseURBQThDOzs7QUFVaEQsTUFBTSxPQUFPLDRDQUE0QyxHQUFHLDZCQUE2Qjs7TUFDbkYsMkNBQTJDLEdBQUcsSUFBSTs7TUFDbEQsaUNBQWlDLEdBQ25DLDJDQUEyQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtJbmplY3Rvcn0gZnJvbSAnLi4vZGkvaW5qZWN0b3InO1xuaW1wb3J0IHtpbmplY3RWaWV3Q29udGFpbmVyUmVmIGFzIHJlbmRlcjNJbmplY3RWaWV3Q29udGFpbmVyUmVmfSBmcm9tICcuLi9yZW5kZXIzL3ZpZXdfZW5naW5lX2NvbXBhdGliaWxpdHknO1xuaW1wb3J0IHtub29wfSBmcm9tICcuLi91dGlsL25vb3AnO1xuXG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnksIENvbXBvbmVudFJlZn0gZnJvbSAnLi9jb21wb25lbnRfZmFjdG9yeSc7XG5pbXBvcnQge0VsZW1lbnRSZWZ9IGZyb20gJy4vZWxlbWVudF9yZWYnO1xuaW1wb3J0IHtOZ01vZHVsZVJlZn0gZnJvbSAnLi9uZ19tb2R1bGVfZmFjdG9yeSc7XG5pbXBvcnQge1RlbXBsYXRlUmVmfSBmcm9tICcuL3RlbXBsYXRlX3JlZic7XG5pbXBvcnQge0VtYmVkZGVkVmlld1JlZiwgVmlld1JlZn0gZnJvbSAnLi92aWV3X3JlZic7XG5cblxuLyoqXG4gKiBSZXByZXNlbnRzIGEgY29udGFpbmVyIHdoZXJlIG9uZSBvciBtb3JlIHZpZXdzIGNhbiBiZSBhdHRhY2hlZCB0byBhIGNvbXBvbmVudC5cbiAqXG4gKiBDYW4gY29udGFpbiAqaG9zdCB2aWV3cyogKGNyZWF0ZWQgYnkgaW5zdGFudGlhdGluZyBhXG4gKiBjb21wb25lbnQgd2l0aCB0aGUgYGNyZWF0ZUNvbXBvbmVudCgpYCBtZXRob2QpLCBhbmQgKmVtYmVkZGVkIHZpZXdzKlxuICogKGNyZWF0ZWQgYnkgaW5zdGFudGlhdGluZyBhIGBUZW1wbGF0ZVJlZmAgd2l0aCB0aGUgYGNyZWF0ZUVtYmVkZGVkVmlldygpYCBtZXRob2QpLlxuICpcbiAqIEEgdmlldyBjb250YWluZXIgaW5zdGFuY2UgY2FuIGNvbnRhaW4gb3RoZXIgdmlldyBjb250YWluZXJzLFxuICogY3JlYXRpbmcgYSBbdmlldyBoaWVyYXJjaHldKGd1aWRlL2dsb3NzYXJ5I3ZpZXctdHJlZSkuXG4gKlxuICogQHNlZSBgQ29tcG9uZW50UmVmYFxuICogQHNlZSBgRW1iZWRkZWRWaWV3UmVmYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIFZpZXdDb250YWluZXJSZWYge1xuICAvKipcbiAgICogQW5jaG9yIGVsZW1lbnQgdGhhdCBzcGVjaWZpZXMgdGhlIGxvY2F0aW9uIG9mIHRoaXMgY29udGFpbmVyIGluIHRoZSBjb250YWluaW5nIHZpZXcuXG4gICAqIEVhY2ggdmlldyBjb250YWluZXIgY2FuIGhhdmUgb25seSBvbmUgYW5jaG9yIGVsZW1lbnQsIGFuZCBlYWNoIGFuY2hvciBlbGVtZW50XG4gICAqIGNhbiBoYXZlIG9ubHkgYSBzaW5nbGUgdmlldyBjb250YWluZXIuXG4gICAqXG4gICAqIFJvb3QgZWxlbWVudHMgb2Ygdmlld3MgYXR0YWNoZWQgdG8gdGhpcyBjb250YWluZXIgYmVjb21lIHNpYmxpbmdzIG9mIHRoZSBhbmNob3IgZWxlbWVudCBpblxuICAgKiB0aGUgcmVuZGVyZWQgdmlldy5cbiAgICpcbiAgICogQWNjZXNzIHRoZSBgVmlld0NvbnRhaW5lclJlZmAgb2YgYW4gZWxlbWVudCBieSBwbGFjaW5nIGEgYERpcmVjdGl2ZWAgaW5qZWN0ZWRcbiAgICogd2l0aCBgVmlld0NvbnRhaW5lclJlZmAgb24gdGhlIGVsZW1lbnQsIG9yIHVzZSBhIGBWaWV3Q2hpbGRgIHF1ZXJ5LlxuICAgKlxuICAgKiA8IS0tIFRPRE86IHJlbmFtZSB0byBhbmNob3JFbGVtZW50IC0tPlxuICAgKi9cbiAgYWJzdHJhY3QgZ2V0IGVsZW1lbnQoKTogRWxlbWVudFJlZjtcblxuICAvKipcbiAgICogVGhlIFtkZXBlbmRlbmN5IGluamVjdG9yXShndWlkZS9nbG9zc2FyeSNpbmplY3RvcikgZm9yIHRoaXMgdmlldyBjb250YWluZXIuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgaW5qZWN0b3IoKTogSW5qZWN0b3I7XG5cbiAgLyoqIEBkZXByZWNhdGVkIE5vIHJlcGxhY2VtZW50ICovXG4gIGFic3RyYWN0IGdldCBwYXJlbnRJbmplY3RvcigpOiBJbmplY3RvcjtcblxuICAvKipcbiAgICogRGVzdHJveXMgYWxsIHZpZXdzIGluIHRoaXMgY29udGFpbmVyLlxuICAgKi9cbiAgYWJzdHJhY3QgY2xlYXIoKTogdm9pZDtcblxuICAvKipcbiAgICogUmV0cmlldmVzIGEgdmlldyBmcm9tIHRoaXMgY29udGFpbmVyLlxuICAgKiBAcGFyYW0gaW5kZXggVGhlIDAtYmFzZWQgaW5kZXggb2YgdGhlIHZpZXcgdG8gcmV0cmlldmUuXG4gICAqIEByZXR1cm5zIFRoZSBgVmlld1JlZmAgaW5zdGFuY2UsIG9yIG51bGwgaWYgdGhlIGluZGV4IGlzIG91dCBvZiByYW5nZS5cbiAgICovXG4gIGFic3RyYWN0IGdldChpbmRleDogbnVtYmVyKTogVmlld1JlZnxudWxsO1xuXG4gIC8qKlxuICAgKiBSZXBvcnRzIGhvdyBtYW55IHZpZXdzIGFyZSBjdXJyZW50bHkgYXR0YWNoZWQgdG8gdGhpcyBjb250YWluZXIuXG4gICAqIEByZXR1cm5zIFRoZSBudW1iZXIgb2Ygdmlld3MuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgbGVuZ3RoKCk6IG51bWJlcjtcblxuICAvKipcbiAgICogSW5zdGFudGlhdGVzIGFuIGVtYmVkZGVkIHZpZXcgYW5kIGluc2VydHMgaXRcbiAgICogaW50byB0aGlzIGNvbnRhaW5lci5cbiAgICogQHBhcmFtIHRlbXBsYXRlUmVmIFRoZSBIVE1MIHRlbXBsYXRlIHRoYXQgZGVmaW5lcyB0aGUgdmlldy5cbiAgICogQHBhcmFtIGluZGV4IFRoZSAwLWJhc2VkIGluZGV4IGF0IHdoaWNoIHRvIGluc2VydCB0aGUgbmV3IHZpZXcgaW50byB0aGlzIGNvbnRhaW5lci5cbiAgICogSWYgbm90IHNwZWNpZmllZCwgYXBwZW5kcyB0aGUgbmV3IHZpZXcgYXMgdGhlIGxhc3QgZW50cnkuXG4gICAqXG4gICAqIEByZXR1cm5zIFRoZSBgVmlld1JlZmAgaW5zdGFuY2UgZm9yIHRoZSBuZXdseSBjcmVhdGVkIHZpZXcuXG4gICAqL1xuICBhYnN0cmFjdCBjcmVhdGVFbWJlZGRlZFZpZXc8Qz4odGVtcGxhdGVSZWY6IFRlbXBsYXRlUmVmPEM+LCBjb250ZXh0PzogQywgaW5kZXg/OiBudW1iZXIpOlxuICAgICAgRW1iZWRkZWRWaWV3UmVmPEM+O1xuXG4gIC8qKlxuICAgKiBJbnN0YW50aWF0ZXMgYSBzaW5nbGUgY29tcG9uZW50IGFuZCBpbnNlcnRzIGl0cyBob3N0IHZpZXcgaW50byB0aGlzIGNvbnRhaW5lci5cbiAgICpcbiAgICogQHBhcmFtIGNvbXBvbmVudEZhY3RvcnkgVGhlIGZhY3RvcnkgdG8gdXNlLlxuICAgKiBAcGFyYW0gaW5kZXggVGhlIGluZGV4IGF0IHdoaWNoIHRvIGluc2VydCB0aGUgbmV3IGNvbXBvbmVudCdzIGhvc3QgdmlldyBpbnRvIHRoaXMgY29udGFpbmVyLlxuICAgKiBJZiBub3Qgc3BlY2lmaWVkLCBhcHBlbmRzIHRoZSBuZXcgdmlldyBhcyB0aGUgbGFzdCBlbnRyeS5cbiAgICogQHBhcmFtIGluamVjdG9yIFRoZSBpbmplY3RvciB0byB1c2UgYXMgdGhlIHBhcmVudCBmb3IgdGhlIG5ldyBjb21wb25lbnQuXG4gICAqIEBwYXJhbSBwcm9qZWN0YWJsZU5vZGVzXG4gICAqIEBwYXJhbSBuZ01vZHVsZVxuICAgKlxuICAgKiBAcmV0dXJucyBUaGUgbmV3IGNvbXBvbmVudCBpbnN0YW5jZSwgY29udGFpbmluZyB0aGUgaG9zdCB2aWV3LlxuICAgKlxuICAgKi9cbiAgYWJzdHJhY3QgY3JlYXRlQ29tcG9uZW50PEM+KFxuICAgICAgY29tcG9uZW50RmFjdG9yeTogQ29tcG9uZW50RmFjdG9yeTxDPiwgaW5kZXg/OiBudW1iZXIsIGluamVjdG9yPzogSW5qZWN0b3IsXG4gICAgICBwcm9qZWN0YWJsZU5vZGVzPzogYW55W11bXSwgbmdNb2R1bGU/OiBOZ01vZHVsZVJlZjxhbnk+KTogQ29tcG9uZW50UmVmPEM+O1xuXG4gIC8qKlxuICAgKiBJbnNlcnRzIGEgdmlldyBpbnRvIHRoaXMgY29udGFpbmVyLlxuICAgKiBAcGFyYW0gdmlld1JlZiBUaGUgdmlldyB0byBpbnNlcnQuXG4gICAqIEBwYXJhbSBpbmRleCBUaGUgMC1iYXNlZCBpbmRleCBhdCB3aGljaCB0byBpbnNlcnQgdGhlIHZpZXcuXG4gICAqIElmIG5vdCBzcGVjaWZpZWQsIGFwcGVuZHMgdGhlIG5ldyB2aWV3IGFzIHRoZSBsYXN0IGVudHJ5LlxuICAgKiBAcmV0dXJucyBUaGUgaW5zZXJ0ZWQgYFZpZXdSZWZgIGluc3RhbmNlLlxuICAgKlxuICAgKi9cbiAgYWJzdHJhY3QgaW5zZXJ0KHZpZXdSZWY6IFZpZXdSZWYsIGluZGV4PzogbnVtYmVyKTogVmlld1JlZjtcblxuICAvKipcbiAgICogTW92ZXMgYSB2aWV3IHRvIGEgbmV3IGxvY2F0aW9uIGluIHRoaXMgY29udGFpbmVyLlxuICAgKiBAcGFyYW0gdmlld1JlZiBUaGUgdmlldyB0byBtb3ZlLlxuICAgKiBAcGFyYW0gaW5kZXggVGhlIDAtYmFzZWQgaW5kZXggb2YgdGhlIG5ldyBsb2NhdGlvbi5cbiAgICogQHJldHVybnMgVGhlIG1vdmVkIGBWaWV3UmVmYCBpbnN0YW5jZS5cbiAgICovXG4gIGFic3RyYWN0IG1vdmUodmlld1JlZjogVmlld1JlZiwgY3VycmVudEluZGV4OiBudW1iZXIpOiBWaWV3UmVmO1xuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRoZSBpbmRleCBvZiBhIHZpZXcgd2l0aGluIHRoZSBjdXJyZW50IGNvbnRhaW5lci5cbiAgICogQHBhcmFtIHZpZXdSZWYgVGhlIHZpZXcgdG8gcXVlcnkuXG4gICAqIEByZXR1cm5zIFRoZSAwLWJhc2VkIGluZGV4IG9mIHRoZSB2aWV3J3MgcG9zaXRpb24gaW4gdGhpcyBjb250YWluZXIsXG4gICAqIG9yIGAtMWAgaWYgdGhpcyBjb250YWluZXIgZG9lc24ndCBjb250YWluIHRoZSB2aWV3LlxuICAgKi9cbiAgYWJzdHJhY3QgaW5kZXhPZih2aWV3UmVmOiBWaWV3UmVmKTogbnVtYmVyO1xuXG4gIC8qKlxuICAgKiBEZXN0cm95cyBhIHZpZXcgYXR0YWNoZWQgdG8gdGhpcyBjb250YWluZXJcbiAgICogQHBhcmFtIGluZGV4IFRoZSAwLWJhc2VkIGluZGV4IG9mIHRoZSB2aWV3IHRvIGRlc3Ryb3kuXG4gICAqIElmIG5vdCBzcGVjaWZpZWQsIHRoZSBsYXN0IHZpZXcgaW4gdGhlIGNvbnRhaW5lciBpcyByZW1vdmVkLlxuICAgKi9cbiAgYWJzdHJhY3QgcmVtb3ZlKGluZGV4PzogbnVtYmVyKTogdm9pZDtcblxuICAvKipcbiAgICogRGV0YWNoZXMgYSB2aWV3IGZyb20gdGhpcyBjb250YWluZXIgd2l0aG91dCBkZXN0cm95aW5nIGl0LlxuICAgKiBVc2UgYWxvbmcgd2l0aCBgaW5zZXJ0KClgIHRvIG1vdmUgYSB2aWV3IHdpdGhpbiB0aGUgY3VycmVudCBjb250YWluZXIuXG4gICAqIEBwYXJhbSBpbmRleCBUaGUgMC1iYXNlZCBpbmRleCBvZiB0aGUgdmlldyB0byBkZXRhY2guXG4gICAqIElmIG5vdCBzcGVjaWZpZWQsIHRoZSBsYXN0IHZpZXcgaW4gdGhlIGNvbnRhaW5lciBpcyBkZXRhY2hlZC5cbiAgICovXG4gIGFic3RyYWN0IGRldGFjaChpbmRleD86IG51bWJlcik6IFZpZXdSZWZ8bnVsbDtcblxuICAvKipcbiAgICogQGludGVybmFsXG4gICAqIEBub2NvbGxhcHNlXG4gICAqL1xuICBzdGF0aWMgX19OR19FTEVNRU5UX0lEX186XG4gICAgICAoKSA9PiBWaWV3Q29udGFpbmVyUmVmID0gKCkgPT4gU1dJVENIX1ZJRVdfQ09OVEFJTkVSX1JFRl9GQUNUT1JZKFZpZXdDb250YWluZXJSZWYsIEVsZW1lbnRSZWYpXG59XG5cbmV4cG9ydCBjb25zdCBTV0lUQ0hfVklFV19DT05UQUlORVJfUkVGX0ZBQ1RPUllfX1BPU1RfUjNfXyA9IHJlbmRlcjNJbmplY3RWaWV3Q29udGFpbmVyUmVmO1xuY29uc3QgU1dJVENIX1ZJRVdfQ09OVEFJTkVSX1JFRl9GQUNUT1JZX19QUkVfUjNfXyA9IG5vb3A7XG5jb25zdCBTV0lUQ0hfVklFV19DT05UQUlORVJfUkVGX0ZBQ1RPUlk6IHR5cGVvZiByZW5kZXIzSW5qZWN0Vmlld0NvbnRhaW5lclJlZiA9XG4gICAgU1dJVENIX1ZJRVdfQ09OVEFJTkVSX1JFRl9GQUNUT1JZX19QUkVfUjNfXztcbiJdfQ==