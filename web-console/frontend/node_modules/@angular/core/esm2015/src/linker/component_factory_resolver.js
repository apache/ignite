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
import { stringify } from '../util/stringify';
import { ComponentFactory } from './component_factory';
/**
 * @param {?} component
 * @return {?}
 */
export function noComponentFactoryError(component) {
    /** @type {?} */
    const error = Error(`No component factory found for ${stringify(component)}. Did you add it to @NgModule.entryComponents?`);
    ((/** @type {?} */ (error)))[ERROR_COMPONENT] = component;
    return error;
}
/** @type {?} */
const ERROR_COMPONENT = 'ngComponent';
/**
 * @param {?} error
 * @return {?}
 */
export function getComponent(error) {
    return ((/** @type {?} */ (error)))[ERROR_COMPONENT];
}
class _NullComponentFactoryResolver {
    /**
     * @template T
     * @param {?} component
     * @return {?}
     */
    resolveComponentFactory(component) {
        throw noComponentFactoryError(component);
    }
}
/**
 * A simple registry that maps `Components` to generated `ComponentFactory` classes
 * that can be used to create instances of components.
 * Use to obtain the factory for a given component type,
 * then use the factory's `create()` method to create a component of that type.
 *
 * @see [Dynamic Components](guide/dynamic-component-loader)
 * \@publicApi
 * @abstract
 */
export class ComponentFactoryResolver {
}
ComponentFactoryResolver.NULL = new _NullComponentFactoryResolver();
if (false) {
    /** @type {?} */
    ComponentFactoryResolver.NULL;
    /**
     * Retrieves the factory object that creates a component of the given type.
     * @abstract
     * @template T
     * @param {?} component The component type.
     * @return {?}
     */
    ComponentFactoryResolver.prototype.resolveComponentFactory = function (component) { };
}
export class CodegenComponentFactoryResolver {
    /**
     * @param {?} factories
     * @param {?} _parent
     * @param {?} _ngModule
     */
    constructor(factories, _parent, _ngModule) {
        this._parent = _parent;
        this._ngModule = _ngModule;
        this._factories = new Map();
        for (let i = 0; i < factories.length; i++) {
            /** @type {?} */
            const factory = factories[i];
            this._factories.set(factory.componentType, factory);
        }
    }
    /**
     * @template T
     * @param {?} component
     * @return {?}
     */
    resolveComponentFactory(component) {
        /** @type {?} */
        let factory = this._factories.get(component);
        if (!factory && this._parent) {
            factory = this._parent.resolveComponentFactory(component);
        }
        if (!factory) {
            throw noComponentFactoryError(component);
        }
        return new ComponentFactoryBoundToModule(factory, this._ngModule);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    CodegenComponentFactoryResolver.prototype._factories;
    /**
     * @type {?}
     * @private
     */
    CodegenComponentFactoryResolver.prototype._parent;
    /**
     * @type {?}
     * @private
     */
    CodegenComponentFactoryResolver.prototype._ngModule;
}
/**
 * @template C
 */
export class ComponentFactoryBoundToModule extends ComponentFactory {
    /**
     * @param {?} factory
     * @param {?} ngModule
     */
    constructor(factory, ngModule) {
        super();
        this.factory = factory;
        this.ngModule = ngModule;
        this.selector = factory.selector;
        this.componentType = factory.componentType;
        this.ngContentSelectors = factory.ngContentSelectors;
        this.inputs = factory.inputs;
        this.outputs = factory.outputs;
    }
    /**
     * @param {?} injector
     * @param {?=} projectableNodes
     * @param {?=} rootSelectorOrNode
     * @param {?=} ngModule
     * @return {?}
     */
    create(injector, projectableNodes, rootSelectorOrNode, ngModule) {
        return this.factory.create(injector, projectableNodes, rootSelectorOrNode, ngModule || this.ngModule);
    }
}
if (false) {
    /** @type {?} */
    ComponentFactoryBoundToModule.prototype.selector;
    /** @type {?} */
    ComponentFactoryBoundToModule.prototype.componentType;
    /** @type {?} */
    ComponentFactoryBoundToModule.prototype.ngContentSelectors;
    /** @type {?} */
    ComponentFactoryBoundToModule.prototype.inputs;
    /** @type {?} */
    ComponentFactoryBoundToModule.prototype.outputs;
    /**
     * @type {?}
     * @private
     */
    ComponentFactoryBoundToModule.prototype.factory;
    /**
     * @type {?}
     * @private
     */
    ComponentFactoryBoundToModule.prototype.ngModule;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcG9uZW50X2ZhY3RvcnlfcmVzb2x2ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9saW5rZXIvY29tcG9uZW50X2ZhY3RvcnlfcmVzb2x2ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFVQSxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFNUMsT0FBTyxFQUFDLGdCQUFnQixFQUFlLE1BQU0scUJBQXFCLENBQUM7Ozs7O0FBR25FLE1BQU0sVUFBVSx1QkFBdUIsQ0FBQyxTQUFtQjs7VUFDbkQsS0FBSyxHQUFHLEtBQUssQ0FDZixrQ0FBa0MsU0FBUyxDQUFDLFNBQVMsQ0FBQyxnREFBZ0QsQ0FBQztJQUMzRyxDQUFDLG1CQUFBLEtBQUssRUFBTyxDQUFDLENBQUMsZUFBZSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBQzVDLE9BQU8sS0FBSyxDQUFDO0FBQ2YsQ0FBQzs7TUFFSyxlQUFlLEdBQUcsYUFBYTs7Ozs7QUFFckMsTUFBTSxVQUFVLFlBQVksQ0FBQyxLQUFZO0lBQ3ZDLE9BQU8sQ0FBQyxtQkFBQSxLQUFLLEVBQU8sQ0FBQyxDQUFDLGVBQWUsQ0FBQyxDQUFDO0FBQ3pDLENBQUM7QUFHRCxNQUFNLDZCQUE2Qjs7Ozs7O0lBQ2pDLHVCQUF1QixDQUFJLFNBQW9DO1FBQzdELE1BQU0sdUJBQXVCLENBQUMsU0FBUyxDQUFDLENBQUM7SUFDM0MsQ0FBQztDQUNGOzs7Ozs7Ozs7OztBQVdELE1BQU0sT0FBZ0Isd0JBQXdCOztBQUNyQyw2QkFBSSxHQUE2QixJQUFJLDZCQUE2QixFQUFFLENBQUM7OztJQUE1RSw4QkFBNEU7Ozs7Ozs7O0lBSzVFLHNGQUE2RTs7QUFHL0UsTUFBTSxPQUFPLCtCQUErQjs7Ozs7O0lBRzFDLFlBQ0ksU0FBa0MsRUFBVSxPQUFpQyxFQUNyRSxTQUEyQjtRQURTLFlBQU8sR0FBUCxPQUFPLENBQTBCO1FBQ3JFLGNBQVMsR0FBVCxTQUFTLENBQWtCO1FBSi9CLGVBQVUsR0FBRyxJQUFJLEdBQUcsRUFBOEIsQ0FBQztRQUt6RCxLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsU0FBUyxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTs7a0JBQ25DLE9BQU8sR0FBRyxTQUFTLENBQUMsQ0FBQyxDQUFDO1lBQzVCLElBQUksQ0FBQyxVQUFVLENBQUMsR0FBRyxDQUFDLE9BQU8sQ0FBQyxhQUFhLEVBQUUsT0FBTyxDQUFDLENBQUM7U0FDckQ7SUFDSCxDQUFDOzs7Ozs7SUFFRCx1QkFBdUIsQ0FBSSxTQUFvQzs7WUFDekQsT0FBTyxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsR0FBRyxDQUFDLFNBQVMsQ0FBQztRQUM1QyxJQUFJLENBQUMsT0FBTyxJQUFJLElBQUksQ0FBQyxPQUFPLEVBQUU7WUFDNUIsT0FBTyxHQUFHLElBQUksQ0FBQyxPQUFPLENBQUMsdUJBQXVCLENBQUMsU0FBUyxDQUFDLENBQUM7U0FDM0Q7UUFDRCxJQUFJLENBQUMsT0FBTyxFQUFFO1lBQ1osTUFBTSx1QkFBdUIsQ0FBQyxTQUFTLENBQUMsQ0FBQztTQUMxQztRQUNELE9BQU8sSUFBSSw2QkFBNkIsQ0FBQyxPQUFPLEVBQUUsSUFBSSxDQUFDLFNBQVMsQ0FBQyxDQUFDO0lBQ3BFLENBQUM7Q0FDRjs7Ozs7O0lBckJDLHFEQUEyRDs7Ozs7SUFHbkIsa0RBQXlDOzs7OztJQUM3RSxvREFBbUM7Ozs7O0FBbUJ6QyxNQUFNLE9BQU8sNkJBQWlDLFNBQVEsZ0JBQW1COzs7OztJQU92RSxZQUFvQixPQUE0QixFQUFVLFFBQTBCO1FBQ2xGLEtBQUssRUFBRSxDQUFDO1FBRFUsWUFBTyxHQUFQLE9BQU8sQ0FBcUI7UUFBVSxhQUFRLEdBQVIsUUFBUSxDQUFrQjtRQUVsRixJQUFJLENBQUMsUUFBUSxHQUFHLE9BQU8sQ0FBQyxRQUFRLENBQUM7UUFDakMsSUFBSSxDQUFDLGFBQWEsR0FBRyxPQUFPLENBQUMsYUFBYSxDQUFDO1FBQzNDLElBQUksQ0FBQyxrQkFBa0IsR0FBRyxPQUFPLENBQUMsa0JBQWtCLENBQUM7UUFDckQsSUFBSSxDQUFDLE1BQU0sR0FBRyxPQUFPLENBQUMsTUFBTSxDQUFDO1FBQzdCLElBQUksQ0FBQyxPQUFPLEdBQUcsT0FBTyxDQUFDLE9BQU8sQ0FBQztJQUNqQyxDQUFDOzs7Ozs7OztJQUVELE1BQU0sQ0FDRixRQUFrQixFQUFFLGdCQUEwQixFQUFFLGtCQUErQixFQUMvRSxRQUEyQjtRQUM3QixPQUFPLElBQUksQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUN0QixRQUFRLEVBQUUsZ0JBQWdCLEVBQUUsa0JBQWtCLEVBQUUsUUFBUSxJQUFJLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUNqRixDQUFDO0NBQ0Y7OztJQXJCQyxpREFBMEI7O0lBQzFCLHNEQUFrQzs7SUFDbEMsMkRBQXNDOztJQUN0QywrQ0FBNEQ7O0lBQzVELGdEQUE2RDs7Ozs7SUFFakQsZ0RBQW9DOzs7OztJQUFFLGlEQUFrQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtJbmplY3Rvcn0gZnJvbSAnLi4vZGkvaW5qZWN0b3InO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge3N0cmluZ2lmeX0gZnJvbSAnLi4vdXRpbC9zdHJpbmdpZnknO1xuXG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnksIENvbXBvbmVudFJlZn0gZnJvbSAnLi9jb21wb25lbnRfZmFjdG9yeSc7XG5pbXBvcnQge05nTW9kdWxlUmVmfSBmcm9tICcuL25nX21vZHVsZV9mYWN0b3J5JztcblxuZXhwb3J0IGZ1bmN0aW9uIG5vQ29tcG9uZW50RmFjdG9yeUVycm9yKGNvbXBvbmVudDogRnVuY3Rpb24pIHtcbiAgY29uc3QgZXJyb3IgPSBFcnJvcihcbiAgICAgIGBObyBjb21wb25lbnQgZmFjdG9yeSBmb3VuZCBmb3IgJHtzdHJpbmdpZnkoY29tcG9uZW50KX0uIERpZCB5b3UgYWRkIGl0IHRvIEBOZ01vZHVsZS5lbnRyeUNvbXBvbmVudHM/YCk7XG4gIChlcnJvciBhcyBhbnkpW0VSUk9SX0NPTVBPTkVOVF0gPSBjb21wb25lbnQ7XG4gIHJldHVybiBlcnJvcjtcbn1cblxuY29uc3QgRVJST1JfQ09NUE9ORU5UID0gJ25nQ29tcG9uZW50JztcblxuZXhwb3J0IGZ1bmN0aW9uIGdldENvbXBvbmVudChlcnJvcjogRXJyb3IpOiBUeXBlPGFueT4ge1xuICByZXR1cm4gKGVycm9yIGFzIGFueSlbRVJST1JfQ09NUE9ORU5UXTtcbn1cblxuXG5jbGFzcyBfTnVsbENvbXBvbmVudEZhY3RvcnlSZXNvbHZlciBpbXBsZW1lbnRzIENvbXBvbmVudEZhY3RvcnlSZXNvbHZlciB7XG4gIHJlc29sdmVDb21wb25lbnRGYWN0b3J5PFQ+KGNvbXBvbmVudDoge25ldyAoLi4uYXJnczogYW55W10pOiBUfSk6IENvbXBvbmVudEZhY3Rvcnk8VD4ge1xuICAgIHRocm93IG5vQ29tcG9uZW50RmFjdG9yeUVycm9yKGNvbXBvbmVudCk7XG4gIH1cbn1cblxuLyoqXG4gKiBBIHNpbXBsZSByZWdpc3RyeSB0aGF0IG1hcHMgYENvbXBvbmVudHNgIHRvIGdlbmVyYXRlZCBgQ29tcG9uZW50RmFjdG9yeWAgY2xhc3Nlc1xuICogdGhhdCBjYW4gYmUgdXNlZCB0byBjcmVhdGUgaW5zdGFuY2VzIG9mIGNvbXBvbmVudHMuXG4gKiBVc2UgdG8gb2J0YWluIHRoZSBmYWN0b3J5IGZvciBhIGdpdmVuIGNvbXBvbmVudCB0eXBlLFxuICogdGhlbiB1c2UgdGhlIGZhY3RvcnkncyBgY3JlYXRlKClgIG1ldGhvZCB0byBjcmVhdGUgYSBjb21wb25lbnQgb2YgdGhhdCB0eXBlLlxuICpcbiAqIEBzZWUgW0R5bmFtaWMgQ29tcG9uZW50c10oZ3VpZGUvZHluYW1pYy1jb21wb25lbnQtbG9hZGVyKVxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyIHtcbiAgc3RhdGljIE5VTEw6IENvbXBvbmVudEZhY3RvcnlSZXNvbHZlciA9IG5ldyBfTnVsbENvbXBvbmVudEZhY3RvcnlSZXNvbHZlcigpO1xuICAvKipcbiAgICogUmV0cmlldmVzIHRoZSBmYWN0b3J5IG9iamVjdCB0aGF0IGNyZWF0ZXMgYSBjb21wb25lbnQgb2YgdGhlIGdpdmVuIHR5cGUuXG4gICAqIEBwYXJhbSBjb21wb25lbnQgVGhlIGNvbXBvbmVudCB0eXBlLlxuICAgKi9cbiAgYWJzdHJhY3QgcmVzb2x2ZUNvbXBvbmVudEZhY3Rvcnk8VD4oY29tcG9uZW50OiBUeXBlPFQ+KTogQ29tcG9uZW50RmFjdG9yeTxUPjtcbn1cblxuZXhwb3J0IGNsYXNzIENvZGVnZW5Db21wb25lbnRGYWN0b3J5UmVzb2x2ZXIgaW1wbGVtZW50cyBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIge1xuICBwcml2YXRlIF9mYWN0b3JpZXMgPSBuZXcgTWFwPGFueSwgQ29tcG9uZW50RmFjdG9yeTxhbnk+PigpO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgZmFjdG9yaWVzOiBDb21wb25lbnRGYWN0b3J5PGFueT5bXSwgcHJpdmF0ZSBfcGFyZW50OiBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIsXG4gICAgICBwcml2YXRlIF9uZ01vZHVsZTogTmdNb2R1bGVSZWY8YW55Pikge1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgZmFjdG9yaWVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBmYWN0b3J5ID0gZmFjdG9yaWVzW2ldO1xuICAgICAgdGhpcy5fZmFjdG9yaWVzLnNldChmYWN0b3J5LmNvbXBvbmVudFR5cGUsIGZhY3RvcnkpO1xuICAgIH1cbiAgfVxuXG4gIHJlc29sdmVDb21wb25lbnRGYWN0b3J5PFQ+KGNvbXBvbmVudDoge25ldyAoLi4uYXJnczogYW55W10pOiBUfSk6IENvbXBvbmVudEZhY3Rvcnk8VD4ge1xuICAgIGxldCBmYWN0b3J5ID0gdGhpcy5fZmFjdG9yaWVzLmdldChjb21wb25lbnQpO1xuICAgIGlmICghZmFjdG9yeSAmJiB0aGlzLl9wYXJlbnQpIHtcbiAgICAgIGZhY3RvcnkgPSB0aGlzLl9wYXJlbnQucmVzb2x2ZUNvbXBvbmVudEZhY3RvcnkoY29tcG9uZW50KTtcbiAgICB9XG4gICAgaWYgKCFmYWN0b3J5KSB7XG4gICAgICB0aHJvdyBub0NvbXBvbmVudEZhY3RvcnlFcnJvcihjb21wb25lbnQpO1xuICAgIH1cbiAgICByZXR1cm4gbmV3IENvbXBvbmVudEZhY3RvcnlCb3VuZFRvTW9kdWxlKGZhY3RvcnksIHRoaXMuX25nTW9kdWxlKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ29tcG9uZW50RmFjdG9yeUJvdW5kVG9Nb2R1bGU8Qz4gZXh0ZW5kcyBDb21wb25lbnRGYWN0b3J5PEM+IHtcbiAgcmVhZG9ubHkgc2VsZWN0b3I6IHN0cmluZztcbiAgcmVhZG9ubHkgY29tcG9uZW50VHlwZTogVHlwZTxhbnk+O1xuICByZWFkb25seSBuZ0NvbnRlbnRTZWxlY3RvcnM6IHN0cmluZ1tdO1xuICByZWFkb25seSBpbnB1dHM6IHtwcm9wTmFtZTogc3RyaW5nLCB0ZW1wbGF0ZU5hbWU6IHN0cmluZ31bXTtcbiAgcmVhZG9ubHkgb3V0cHV0czoge3Byb3BOYW1lOiBzdHJpbmcsIHRlbXBsYXRlTmFtZTogc3RyaW5nfVtdO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZmFjdG9yeTogQ29tcG9uZW50RmFjdG9yeTxDPiwgcHJpdmF0ZSBuZ01vZHVsZTogTmdNb2R1bGVSZWY8YW55Pikge1xuICAgIHN1cGVyKCk7XG4gICAgdGhpcy5zZWxlY3RvciA9IGZhY3Rvcnkuc2VsZWN0b3I7XG4gICAgdGhpcy5jb21wb25lbnRUeXBlID0gZmFjdG9yeS5jb21wb25lbnRUeXBlO1xuICAgIHRoaXMubmdDb250ZW50U2VsZWN0b3JzID0gZmFjdG9yeS5uZ0NvbnRlbnRTZWxlY3RvcnM7XG4gICAgdGhpcy5pbnB1dHMgPSBmYWN0b3J5LmlucHV0cztcbiAgICB0aGlzLm91dHB1dHMgPSBmYWN0b3J5Lm91dHB1dHM7XG4gIH1cblxuICBjcmVhdGUoXG4gICAgICBpbmplY3RvcjogSW5qZWN0b3IsIHByb2plY3RhYmxlTm9kZXM/OiBhbnlbXVtdLCByb290U2VsZWN0b3JPck5vZGU/OiBzdHJpbmd8YW55LFxuICAgICAgbmdNb2R1bGU/OiBOZ01vZHVsZVJlZjxhbnk+KTogQ29tcG9uZW50UmVmPEM+IHtcbiAgICByZXR1cm4gdGhpcy5mYWN0b3J5LmNyZWF0ZShcbiAgICAgICAgaW5qZWN0b3IsIHByb2plY3RhYmxlTm9kZXMsIHJvb3RTZWxlY3Rvck9yTm9kZSwgbmdNb2R1bGUgfHwgdGhpcy5uZ01vZHVsZSk7XG4gIH1cbn1cbiJdfQ==