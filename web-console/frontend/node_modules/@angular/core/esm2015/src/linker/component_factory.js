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
 * Represents a component created by a `ComponentFactory`.
 * Provides access to the component instance and related objects,
 * and provides the means of destroying the instance.
 *
 * \@publicApi
 * @abstract
 * @template C
 */
export class ComponentRef {
}
if (false) {
    /**
     * The host or anchor [element](guide/glossary#element) for this component instance.
     * @abstract
     * @return {?}
     */
    ComponentRef.prototype.location = function () { };
    /**
     * The [dependency injector](guide/glossary#injector) for this component instance.
     * @abstract
     * @return {?}
     */
    ComponentRef.prototype.injector = function () { };
    /**
     * This component instance.
     * @abstract
     * @return {?}
     */
    ComponentRef.prototype.instance = function () { };
    /**
     * The [host view](guide/glossary#view-tree) defined by the template
     * for this component instance.
     * @abstract
     * @return {?}
     */
    ComponentRef.prototype.hostView = function () { };
    /**
     * The change detector for this component instance.
     * @abstract
     * @return {?}
     */
    ComponentRef.prototype.changeDetectorRef = function () { };
    /**
     * The type of this component (as created by a `ComponentFactory` class).
     * @abstract
     * @return {?}
     */
    ComponentRef.prototype.componentType = function () { };
    /**
     * Destroys the component instance and all of the data structures associated with it.
     * @abstract
     * @return {?}
     */
    ComponentRef.prototype.destroy = function () { };
    /**
     * A lifecycle hook that provides additional developer-defined cleanup
     * functionality for the component.
     * @abstract
     * @param {?} callback A handler function that cleans up developer-defined data
     * associated with this component. Called when the `destroy()` method is invoked.
     * @return {?}
     */
    ComponentRef.prototype.onDestroy = function (callback) { };
}
/**
 * Base class for a factory that can create a component dynamically.
 * Instantiate a factory for a given type of component with `resolveComponentFactory()`.
 * Use the resulting `ComponentFactory.create()` method to create a component of that type.
 *
 * @see [Dynamic Components](guide/dynamic-component-loader)
 *
 * \@publicApi
 * @abstract
 * @template C
 */
export class ComponentFactory {
}
if (false) {
    /**
     * The component's HTML selector.
     * @abstract
     * @return {?}
     */
    ComponentFactory.prototype.selector = function () { };
    /**
     * The type of component the factory will create.
     * @abstract
     * @return {?}
     */
    ComponentFactory.prototype.componentType = function () { };
    /**
     * Selector for all <ng-content> elements in the component.
     * @abstract
     * @return {?}
     */
    ComponentFactory.prototype.ngContentSelectors = function () { };
    /**
     * The inputs of the component.
     * @abstract
     * @return {?}
     */
    ComponentFactory.prototype.inputs = function () { };
    /**
     * The outputs of the component.
     * @abstract
     * @return {?}
     */
    ComponentFactory.prototype.outputs = function () { };
    /**
     * Creates a new component.
     * @abstract
     * @param {?} injector
     * @param {?=} projectableNodes
     * @param {?=} rootSelectorOrNode
     * @param {?=} ngModule
     * @return {?}
     */
    ComponentFactory.prototype.create = function (injector, projectableNodes, rootSelectorOrNode, ngModule) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcG9uZW50X2ZhY3RvcnkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9saW5rZXIvY29tcG9uZW50X2ZhY3RvcnkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUF1QkEsTUFBTSxPQUFnQixZQUFZO0NBNENqQzs7Ozs7OztJQXhDQyxrREFBb0M7Ozs7OztJQUtwQyxrREFBa0M7Ozs7OztJQUtsQyxrREFBMkI7Ozs7Ozs7SUFNM0Isa0RBQWlDOzs7Ozs7SUFLakMsMkRBQW9EOzs7Ozs7SUFLcEQsdURBQXdDOzs7Ozs7SUFLeEMsaURBQXlCOzs7Ozs7Ozs7SUFRekIsMkRBQTZDOzs7Ozs7Ozs7Ozs7O0FBWS9DLE1BQU0sT0FBZ0IsZ0JBQWdCO0NBMkJyQzs7Ozs7OztJQXZCQyxzREFBZ0M7Ozs7OztJQUloQywyREFBd0M7Ozs7OztJQUl4QyxnRUFBNEM7Ozs7OztJQUk1QyxvREFBa0U7Ozs7OztJQUlsRSxxREFBbUU7Ozs7Ozs7Ozs7SUFJbkUsNEdBRWtEIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NoYW5nZURldGVjdG9yUmVmfSBmcm9tICcuLi9jaGFuZ2VfZGV0ZWN0aW9uL2NoYW5nZV9kZXRlY3Rpb24nO1xuaW1wb3J0IHtJbmplY3Rvcn0gZnJvbSAnLi4vZGkvaW5qZWN0b3InO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5cbmltcG9ydCB7RWxlbWVudFJlZn0gZnJvbSAnLi9lbGVtZW50X3JlZic7XG5pbXBvcnQge05nTW9kdWxlUmVmfSBmcm9tICcuL25nX21vZHVsZV9mYWN0b3J5JztcbmltcG9ydCB7Vmlld1JlZn0gZnJvbSAnLi92aWV3X3JlZic7XG5cbi8qKlxuICogUmVwcmVzZW50cyBhIGNvbXBvbmVudCBjcmVhdGVkIGJ5IGEgYENvbXBvbmVudEZhY3RvcnlgLlxuICogUHJvdmlkZXMgYWNjZXNzIHRvIHRoZSBjb21wb25lbnQgaW5zdGFuY2UgYW5kIHJlbGF0ZWQgb2JqZWN0cyxcbiAqIGFuZCBwcm92aWRlcyB0aGUgbWVhbnMgb2YgZGVzdHJveWluZyB0aGUgaW5zdGFuY2UuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgQ29tcG9uZW50UmVmPEM+IHtcbiAgLyoqXG4gICAqIFRoZSBob3N0IG9yIGFuY2hvciBbZWxlbWVudF0oZ3VpZGUvZ2xvc3NhcnkjZWxlbWVudCkgZm9yIHRoaXMgY29tcG9uZW50IGluc3RhbmNlLlxuICAgKi9cbiAgYWJzdHJhY3QgZ2V0IGxvY2F0aW9uKCk6IEVsZW1lbnRSZWY7XG5cbiAgLyoqXG4gICAqIFRoZSBbZGVwZW5kZW5jeSBpbmplY3Rvcl0oZ3VpZGUvZ2xvc3NhcnkjaW5qZWN0b3IpIGZvciB0aGlzIGNvbXBvbmVudCBpbnN0YW5jZS5cbiAgICovXG4gIGFic3RyYWN0IGdldCBpbmplY3RvcigpOiBJbmplY3RvcjtcblxuICAvKipcbiAgICogVGhpcyBjb21wb25lbnQgaW5zdGFuY2UuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgaW5zdGFuY2UoKTogQztcblxuICAvKipcbiAgICogVGhlIFtob3N0IHZpZXddKGd1aWRlL2dsb3NzYXJ5I3ZpZXctdHJlZSkgZGVmaW5lZCBieSB0aGUgdGVtcGxhdGVcbiAgICogZm9yIHRoaXMgY29tcG9uZW50IGluc3RhbmNlLlxuICAgKi9cbiAgYWJzdHJhY3QgZ2V0IGhvc3RWaWV3KCk6IFZpZXdSZWY7XG5cbiAgLyoqXG4gICAqIFRoZSBjaGFuZ2UgZGV0ZWN0b3IgZm9yIHRoaXMgY29tcG9uZW50IGluc3RhbmNlLlxuICAgKi9cbiAgYWJzdHJhY3QgZ2V0IGNoYW5nZURldGVjdG9yUmVmKCk6IENoYW5nZURldGVjdG9yUmVmO1xuXG4gIC8qKlxuICAgKiBUaGUgdHlwZSBvZiB0aGlzIGNvbXBvbmVudCAoYXMgY3JlYXRlZCBieSBhIGBDb21wb25lbnRGYWN0b3J5YCBjbGFzcykuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgY29tcG9uZW50VHlwZSgpOiBUeXBlPGFueT47XG5cbiAgLyoqXG4gICAqIERlc3Ryb3lzIHRoZSBjb21wb25lbnQgaW5zdGFuY2UgYW5kIGFsbCBvZiB0aGUgZGF0YSBzdHJ1Y3R1cmVzIGFzc29jaWF0ZWQgd2l0aCBpdC5cbiAgICovXG4gIGFic3RyYWN0IGRlc3Ryb3koKTogdm9pZDtcblxuICAvKipcbiAgICogQSBsaWZlY3ljbGUgaG9vayB0aGF0IHByb3ZpZGVzIGFkZGl0aW9uYWwgZGV2ZWxvcGVyLWRlZmluZWQgY2xlYW51cFxuICAgKiBmdW5jdGlvbmFsaXR5IGZvciB0aGUgY29tcG9uZW50LlxuICAgKiBAcGFyYW0gY2FsbGJhY2sgQSBoYW5kbGVyIGZ1bmN0aW9uIHRoYXQgY2xlYW5zIHVwIGRldmVsb3Blci1kZWZpbmVkIGRhdGFcbiAgICogYXNzb2NpYXRlZCB3aXRoIHRoaXMgY29tcG9uZW50LiBDYWxsZWQgd2hlbiB0aGUgYGRlc3Ryb3koKWAgbWV0aG9kIGlzIGludm9rZWQuXG4gICAqL1xuICBhYnN0cmFjdCBvbkRlc3Ryb3koY2FsbGJhY2s6IEZ1bmN0aW9uKTogdm9pZDtcbn1cblxuLyoqXG4gKiBCYXNlIGNsYXNzIGZvciBhIGZhY3RvcnkgdGhhdCBjYW4gY3JlYXRlIGEgY29tcG9uZW50IGR5bmFtaWNhbGx5LlxuICogSW5zdGFudGlhdGUgYSBmYWN0b3J5IGZvciBhIGdpdmVuIHR5cGUgb2YgY29tcG9uZW50IHdpdGggYHJlc29sdmVDb21wb25lbnRGYWN0b3J5KClgLlxuICogVXNlIHRoZSByZXN1bHRpbmcgYENvbXBvbmVudEZhY3RvcnkuY3JlYXRlKClgIG1ldGhvZCB0byBjcmVhdGUgYSBjb21wb25lbnQgb2YgdGhhdCB0eXBlLlxuICpcbiAqIEBzZWUgW0R5bmFtaWMgQ29tcG9uZW50c10oZ3VpZGUvZHluYW1pYy1jb21wb25lbnQtbG9hZGVyKVxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIENvbXBvbmVudEZhY3Rvcnk8Qz4ge1xuICAvKipcbiAgICogVGhlIGNvbXBvbmVudCdzIEhUTUwgc2VsZWN0b3IuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgc2VsZWN0b3IoKTogc3RyaW5nO1xuICAvKipcbiAgICogVGhlIHR5cGUgb2YgY29tcG9uZW50IHRoZSBmYWN0b3J5IHdpbGwgY3JlYXRlLlxuICAgKi9cbiAgYWJzdHJhY3QgZ2V0IGNvbXBvbmVudFR5cGUoKTogVHlwZTxhbnk+O1xuICAvKipcbiAgICogU2VsZWN0b3IgZm9yIGFsbCA8bmctY29udGVudD4gZWxlbWVudHMgaW4gdGhlIGNvbXBvbmVudC5cbiAgICovXG4gIGFic3RyYWN0IGdldCBuZ0NvbnRlbnRTZWxlY3RvcnMoKTogc3RyaW5nW107XG4gIC8qKlxuICAgKiBUaGUgaW5wdXRzIG9mIHRoZSBjb21wb25lbnQuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgaW5wdXRzKCk6IHtwcm9wTmFtZTogc3RyaW5nLCB0ZW1wbGF0ZU5hbWU6IHN0cmluZ31bXTtcbiAgLyoqXG4gICAqIFRoZSBvdXRwdXRzIG9mIHRoZSBjb21wb25lbnQuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgb3V0cHV0cygpOiB7cHJvcE5hbWU6IHN0cmluZywgdGVtcGxhdGVOYW1lOiBzdHJpbmd9W107XG4gIC8qKlxuICAgKiBDcmVhdGVzIGEgbmV3IGNvbXBvbmVudC5cbiAgICovXG4gIGFic3RyYWN0IGNyZWF0ZShcbiAgICAgIGluamVjdG9yOiBJbmplY3RvciwgcHJvamVjdGFibGVOb2Rlcz86IGFueVtdW10sIHJvb3RTZWxlY3Rvck9yTm9kZT86IHN0cmluZ3xhbnksXG4gICAgICBuZ01vZHVsZT86IE5nTW9kdWxlUmVmPGFueT4pOiBDb21wb25lbnRSZWY8Qz47XG59XG4iXX0=