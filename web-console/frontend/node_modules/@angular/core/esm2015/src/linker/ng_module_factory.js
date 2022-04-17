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
 * Represents an instance of an NgModule created via a {\@link NgModuleFactory}.
 *
 * `NgModuleRef` provides access to the NgModule Instance as well other objects related to this
 * NgModule Instance.
 *
 * \@publicApi
 * @abstract
 * @template T
 */
export class NgModuleRef {
}
if (false) {
    /**
     * The injector that contains all of the providers of the NgModule.
     * @abstract
     * @return {?}
     */
    NgModuleRef.prototype.injector = function () { };
    /**
     * The ComponentFactoryResolver to get hold of the ComponentFactories
     * declared in the `entryComponents` property of the module.
     * @abstract
     * @return {?}
     */
    NgModuleRef.prototype.componentFactoryResolver = function () { };
    /**
     * The NgModule instance.
     * @abstract
     * @return {?}
     */
    NgModuleRef.prototype.instance = function () { };
    /**
     * Destroys the module instance and all of the data structures associated with it.
     * @abstract
     * @return {?}
     */
    NgModuleRef.prototype.destroy = function () { };
    /**
     * Allows to register a callback that will be called when the module is destroyed.
     * @abstract
     * @param {?} callback
     * @return {?}
     */
    NgModuleRef.prototype.onDestroy = function (callback) { };
}
/**
 * @record
 * @template T
 */
export function InternalNgModuleRef() { }
if (false) {
    /** @type {?} */
    InternalNgModuleRef.prototype._bootstrapComponents;
}
/**
 * \@publicApi
 * @abstract
 * @template T
 */
export class NgModuleFactory {
}
if (false) {
    /**
     * @abstract
     * @return {?}
     */
    NgModuleFactory.prototype.moduleType = function () { };
    /**
     * @abstract
     * @param {?} parentInjector
     * @return {?}
     */
    NgModuleFactory.prototype.create = function (parentInjector) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbW9kdWxlX2ZhY3RvcnkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9saW5rZXIvbmdfbW9kdWxlX2ZhY3RvcnkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBc0JBLE1BQU0sT0FBZ0IsV0FBVztDQTBCaEM7Ozs7Ozs7SUF0QkMsaURBQWtDOzs7Ozs7O0lBTWxDLGlFQUFrRTs7Ozs7O0lBS2xFLGlEQUEyQjs7Ozs7O0lBSzNCLGdEQUF5Qjs7Ozs7OztJQUt6QiwwREFBK0M7Ozs7OztBQUdqRCx5Q0FJQzs7O0lBREMsbURBQWtDOzs7Ozs7O0FBTXBDLE1BQU0sT0FBZ0IsZUFBZTtDQUdwQzs7Ozs7O0lBRkMsdURBQW1DOzs7Ozs7SUFDbkMsaUVBQStEIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdG9yfSBmcm9tICcuLi9kaS9pbmplY3Rvcic7XG5pbXBvcnQge1R5cGV9IGZyb20gJy4uL2ludGVyZmFjZS90eXBlJztcblxuaW1wb3J0IHtDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXJ9IGZyb20gJy4vY29tcG9uZW50X2ZhY3RvcnlfcmVzb2x2ZXInO1xuXG5cbi8qKlxuICogUmVwcmVzZW50cyBhbiBpbnN0YW5jZSBvZiBhbiBOZ01vZHVsZSBjcmVhdGVkIHZpYSBhIHtAbGluayBOZ01vZHVsZUZhY3Rvcnl9LlxuICpcbiAqIGBOZ01vZHVsZVJlZmAgcHJvdmlkZXMgYWNjZXNzIHRvIHRoZSBOZ01vZHVsZSBJbnN0YW5jZSBhcyB3ZWxsIG90aGVyIG9iamVjdHMgcmVsYXRlZCB0byB0aGlzXG4gKiBOZ01vZHVsZSBJbnN0YW5jZS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBOZ01vZHVsZVJlZjxUPiB7XG4gIC8qKlxuICAgKiBUaGUgaW5qZWN0b3IgdGhhdCBjb250YWlucyBhbGwgb2YgdGhlIHByb3ZpZGVycyBvZiB0aGUgTmdNb2R1bGUuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgaW5qZWN0b3IoKTogSW5qZWN0b3I7XG5cbiAgLyoqXG4gICAqIFRoZSBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIgdG8gZ2V0IGhvbGQgb2YgdGhlIENvbXBvbmVudEZhY3Rvcmllc1xuICAgKiBkZWNsYXJlZCBpbiB0aGUgYGVudHJ5Q29tcG9uZW50c2AgcHJvcGVydHkgb2YgdGhlIG1vZHVsZS5cbiAgICovXG4gIGFic3RyYWN0IGdldCBjb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIoKTogQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyO1xuXG4gIC8qKlxuICAgKiBUaGUgTmdNb2R1bGUgaW5zdGFuY2UuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgaW5zdGFuY2UoKTogVDtcblxuICAvKipcbiAgICogRGVzdHJveXMgdGhlIG1vZHVsZSBpbnN0YW5jZSBhbmQgYWxsIG9mIHRoZSBkYXRhIHN0cnVjdHVyZXMgYXNzb2NpYXRlZCB3aXRoIGl0LlxuICAgKi9cbiAgYWJzdHJhY3QgZGVzdHJveSgpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBBbGxvd3MgdG8gcmVnaXN0ZXIgYSBjYWxsYmFjayB0aGF0IHdpbGwgYmUgY2FsbGVkIHdoZW4gdGhlIG1vZHVsZSBpcyBkZXN0cm95ZWQuXG4gICAqL1xuICBhYnN0cmFjdCBvbkRlc3Ryb3koY2FsbGJhY2s6ICgpID0+IHZvaWQpOiB2b2lkO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIEludGVybmFsTmdNb2R1bGVSZWY8VD4gZXh0ZW5kcyBOZ01vZHVsZVJlZjxUPiB7XG4gIC8vIE5vdGU6IHdlIGFyZSB1c2luZyB0aGUgcHJlZml4IF8gYXMgTmdNb2R1bGVEYXRhIGlzIGFuIE5nTW9kdWxlUmVmIGFuZCB0aGVyZWZvcmUgZGlyZWN0bHlcbiAgLy8gZXhwb3NlZCB0byB0aGUgdXNlci5cbiAgX2Jvb3RzdHJhcENvbXBvbmVudHM6IFR5cGU8YW55PltdO1xufVxuXG4vKipcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIE5nTW9kdWxlRmFjdG9yeTxUPiB7XG4gIGFic3RyYWN0IGdldCBtb2R1bGVUeXBlKCk6IFR5cGU8VD47XG4gIGFic3RyYWN0IGNyZWF0ZShwYXJlbnRJbmplY3RvcjogSW5qZWN0b3J8bnVsbCk6IE5nTW9kdWxlUmVmPFQ+O1xufVxuIl19