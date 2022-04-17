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
import { Injector } from '../di/injector';
import { INJECTOR } from '../di/injector_compatibility';
import { InjectFlags } from '../di/interface/injector';
import { createInjector } from '../di/r3_injector';
import { ComponentFactoryResolver as viewEngine_ComponentFactoryResolver } from '../linker/component_factory_resolver';
import { NgModuleFactory as viewEngine_NgModuleFactory, NgModuleRef as viewEngine_NgModuleRef } from '../linker/ng_module_factory';
import { registerNgModuleType } from '../linker/ng_module_factory_registration';
import { assertDefined } from '../util/assert';
import { stringify } from '../util/stringify';
import { ComponentFactoryResolver } from './component_ref';
import { getNgLocaleIdDef, getNgModuleDef } from './definition';
import { setLocaleId } from './i18n';
import { maybeUnwrapFn } from './util/misc_utils';
/**
 * @record
 * @template T
 */
export function NgModuleType() { }
if (false) {
    /** @type {?} */
    NgModuleType.prototype.ngModuleDef;
}
/** @type {?} */
const COMPONENT_FACTORY_RESOLVER = {
    provide: viewEngine_ComponentFactoryResolver,
    useClass: ComponentFactoryResolver,
    deps: [viewEngine_NgModuleRef],
};
/**
 * @template T
 */
export class NgModuleRef extends viewEngine_NgModuleRef {
    /**
     * @param {?} ngModuleType
     * @param {?} _parent
     */
    constructor(ngModuleType, _parent) {
        super();
        this._parent = _parent;
        // tslint:disable-next-line:require-internal-with-underscore
        this._bootstrapComponents = [];
        this.injector = this;
        this.destroyCbs = [];
        /** @type {?} */
        /** @nocollapse */ const ngModuleDef = getNgModuleDef(ngModuleType);
        ngDevMode && assertDefined(ngModuleDef, `NgModule '${stringify(ngModuleType)}' is not a subtype of 'NgModuleType'.`);
        /** @type {?} */
        const ngLocaleIdDef = getNgLocaleIdDef(ngModuleType);
        if (ngLocaleIdDef) {
            setLocaleId(ngLocaleIdDef);
        }
        this._bootstrapComponents = maybeUnwrapFn((/** @type {?} */ (ngModuleDef)).bootstrap);
        /** @type {?} */
        const additionalProviders = [
            {
                provide: viewEngine_NgModuleRef,
                useValue: this,
            },
            COMPONENT_FACTORY_RESOLVER
        ];
        this._r3Injector = (/** @type {?} */ (createInjector(ngModuleType, _parent, additionalProviders, stringify(ngModuleType))));
        this.instance = this.get(ngModuleType);
    }
    /**
     * @param {?} token
     * @param {?=} notFoundValue
     * @param {?=} injectFlags
     * @return {?}
     */
    get(token, notFoundValue = Injector.THROW_IF_NOT_FOUND, injectFlags = InjectFlags.Default) {
        if (token === Injector || token === viewEngine_NgModuleRef || token === INJECTOR) {
            return this;
        }
        return this._r3Injector.get(token, notFoundValue, injectFlags);
    }
    /**
     * @return {?}
     */
    get componentFactoryResolver() {
        return this.get(viewEngine_ComponentFactoryResolver);
    }
    /**
     * @return {?}
     */
    destroy() {
        ngDevMode && assertDefined(this.destroyCbs, 'NgModule already destroyed');
        /** @type {?} */
        const injector = this._r3Injector;
        !injector.destroyed && injector.destroy();
        (/** @type {?} */ (this.destroyCbs)).forEach((/**
         * @param {?} fn
         * @return {?}
         */
        fn => fn()));
        this.destroyCbs = null;
    }
    /**
     * @param {?} callback
     * @return {?}
     */
    onDestroy(callback) {
        ngDevMode && assertDefined(this.destroyCbs, 'NgModule already destroyed');
        (/** @type {?} */ (this.destroyCbs)).push(callback);
    }
}
if (false) {
    /** @type {?} */
    NgModuleRef.prototype._bootstrapComponents;
    /** @type {?} */
    NgModuleRef.prototype._r3Injector;
    /** @type {?} */
    NgModuleRef.prototype.injector;
    /** @type {?} */
    NgModuleRef.prototype.instance;
    /** @type {?} */
    NgModuleRef.prototype.destroyCbs;
    /** @type {?} */
    NgModuleRef.prototype._parent;
}
/**
 * @template T
 */
export class NgModuleFactory extends viewEngine_NgModuleFactory {
    /**
     * @param {?} moduleType
     */
    constructor(moduleType) {
        super();
        this.moduleType = moduleType;
        /** @type {?} */
        /** @nocollapse */ const ngModuleDef = getNgModuleDef(moduleType);
        if (ngModuleDef !== null) {
            // Register the NgModule with Angular's module registry. The location (and hence timing) of
            // this call is critical to ensure this works correctly (modules get registered when expected)
            // without bloating bundles (modules are registered when otherwise not referenced).
            //
            // In View Engine, registration occurs in the .ngfactory.js file as a side effect. This has
            // several practical consequences:
            //
            // - If an .ngfactory file is not imported from, the module won't be registered (and can be
            //   tree shaken).
            // - If an .ngfactory file is imported from, the module will be registered even if an instance
            //   is not actually created (via `create` below).
            // - Since an .ngfactory file in View Engine references the .ngfactory files of the NgModule's
            //   imports,
            //
            // In Ivy, things are a bit different. .ngfactory files still exist for compatibility, but are
            // not a required API to use - there are other ways to obtain an NgModuleFactory for a given
            // NgModule. Thus, relying on a side effect in the .ngfactory file is not sufficient. Instead,
            // the side effect of registration is added here, in the constructor of NgModuleFactory,
            // ensuring no matter how a factory is created, the module is registered correctly.
            //
            // An alternative would be to include the registration side effect inline following the actual
            // NgModule definition. This also has the correct timing, but breaks tree-shaking - modules
            // will be registered and retained even if they're otherwise never referenced.
            registerNgModuleType((/** @type {?} */ (moduleType)));
        }
    }
    /**
     * @param {?} parentInjector
     * @return {?}
     */
    create(parentInjector) {
        return new NgModuleRef(this.moduleType, parentInjector);
    }
}
if (false) {
    /** @type {?} */
    NgModuleFactory.prototype.moduleType;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbW9kdWxlX3JlZi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvbmdfbW9kdWxlX3JlZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUN4QyxPQUFPLEVBQUMsUUFBUSxFQUFDLE1BQU0sOEJBQThCLENBQUM7QUFDdEQsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBRXJELE9BQU8sRUFBYSxjQUFjLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUU3RCxPQUFPLEVBQUMsd0JBQXdCLElBQUksbUNBQW1DLEVBQUMsTUFBTSxzQ0FBc0MsQ0FBQztBQUNySCxPQUFPLEVBQXNCLGVBQWUsSUFBSSwwQkFBMEIsRUFBRSxXQUFXLElBQUksc0JBQXNCLEVBQUMsTUFBTSw2QkFBNkIsQ0FBQztBQUN0SixPQUFPLEVBQUMsb0JBQW9CLEVBQUMsTUFBTSwwQ0FBMEMsQ0FBQztBQUU5RSxPQUFPLEVBQUMsYUFBYSxFQUFDLE1BQU0sZ0JBQWdCLENBQUM7QUFDN0MsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBRTVDLE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3pELE9BQU8sRUFBQyxnQkFBZ0IsRUFBRSxjQUFjLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFDOUQsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUNuQyxPQUFPLEVBQUMsYUFBYSxFQUFDLE1BQU0sbUJBQW1CLENBQUM7Ozs7O0FBRWhELGtDQUF1Rjs7O0lBQTlCLG1DQUE0Qjs7O01BRS9FLDBCQUEwQixHQUFtQjtJQUNqRCxPQUFPLEVBQUUsbUNBQW1DO0lBQzVDLFFBQVEsRUFBRSx3QkFBd0I7SUFDbEMsSUFBSSxFQUFF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QWMsQ0FBQyxVQUFVLENBQUM7UUFDOUMsSUFBSSxXQUFXLEtBQUssSUFBSSxFQUFFO1lBQ3hCLDJGQUEyRjtZQUMzRiw4RkFBOEY7WUFDOUYsbUZBQW1GO1lBQ25GLEVBQUU7WUFDRiwyRkFBMkY7WUFDM0Ysa0NBQWtDO1lBQ2xDLEVBQUU7WUFDRiwyRkFBMkY7WUFDM0Ysa0JBQWtCO1lBQ2xCLDhGQUE4RjtZQUM5RixrREFBa0Q7WUFDbEQsOEZBQThGO1lBQzlGLGFBQWE7WUFDYixFQUFFO1lBQ0YsOEZBQThGO1lBQzlGLDRGQUE0RjtZQUM1Riw4RkFBOEY7WUFDOUYsd0ZBQXdGO1lBQ3hGLG1GQUFtRjtZQUNuRixFQUFFO1lBQ0YsOEZBQThGO1lBQzlGLDJGQUEyRjtZQUMzRiw4RUFBOEU7WUFDOUUsb0JBQW9CLENBQUMsbUJBQUEsVUFBVSxFQUFnQixDQUFDLENBQUM7U0FDbEQ7SUFDSCxDQUFDOzs7OztJQUVELE1BQU0sQ0FBQyxjQUE2QjtRQUNsQyxPQUFPLElBQUksV0FBVyxDQUFDLElBQUksQ0FBQyxVQUFVLEVBQUUsY0FBYyxDQUFDLENBQUM7SUFDMUQsQ0FBQztDQUNGOzs7SUFuQ2EscUNBQTBCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdG9yfSBmcm9tICcuLi9kaS9pbmplY3Rvcic7XG5pbXBvcnQge0lOSkVDVE9SfSBmcm9tICcuLi9kaS9pbmplY3Rvcl9jb21wYXRpYmlsaXR5JztcbmltcG9ydCB7SW5qZWN0RmxhZ3N9IGZyb20gJy4uL2RpL2ludGVyZmFjZS9pbmplY3Rvcic7XG5pbXBvcnQge1N0YXRpY1Byb3ZpZGVyfSBmcm9tICcuLi9kaS9pbnRlcmZhY2UvcHJvdmlkZXInO1xuaW1wb3J0IHtSM0luamVjdG9yLCBjcmVhdGVJbmplY3Rvcn0gZnJvbSAnLi4vZGkvcjNfaW5qZWN0b3InO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnlSZXNvbHZlciBhcyB2aWV3RW5naW5lX0NvbXBvbmVudEZhY3RvcnlSZXNvbHZlcn0gZnJvbSAnLi4vbGlua2VyL2NvbXBvbmVudF9mYWN0b3J5X3Jlc29sdmVyJztcbmltcG9ydCB7SW50ZXJuYWxOZ01vZHVsZVJlZiwgTmdNb2R1bGVGYWN0b3J5IGFzIHZpZXdFbmdpbmVfTmdNb2R1bGVGYWN0b3J5LCBOZ01vZHVsZVJlZiBhcyB2aWV3RW5naW5lX05nTW9kdWxlUmVmfSBmcm9tICcuLi9saW5rZXIvbmdfbW9kdWxlX2ZhY3RvcnknO1xuaW1wb3J0IHtyZWdpc3Rlck5nTW9kdWxlVHlwZX0gZnJvbSAnLi4vbGlua2VyL25nX21vZHVsZV9mYWN0b3J5X3JlZ2lzdHJhdGlvbic7XG5pbXBvcnQge05nTW9kdWxlRGVmfSBmcm9tICcuLi9tZXRhZGF0YS9uZ19tb2R1bGUnO1xuaW1wb3J0IHthc3NlcnREZWZpbmVkfSBmcm9tICcuLi91dGlsL2Fzc2VydCc7XG5pbXBvcnQge3N0cmluZ2lmeX0gZnJvbSAnLi4vdXRpbC9zdHJpbmdpZnknO1xuXG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnlSZXNvbHZlcn0gZnJvbSAnLi9jb21wb25lbnRfcmVmJztcbmltcG9ydCB7Z2V0TmdMb2NhbGVJZERlZiwgZ2V0TmdNb2R1bGVEZWZ9IGZyb20gJy4vZGVmaW5pdGlvbic7XG5pbXBvcnQge3NldExvY2FsZUlkfSBmcm9tICcuL2kxOG4nO1xuaW1wb3J0IHttYXliZVVud3JhcEZufSBmcm9tICcuL3V0aWwvbWlzY191dGlscyc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgTmdNb2R1bGVUeXBlPFQgPSBhbnk+IGV4dGVuZHMgVHlwZTxUPiB7IG5nTW9kdWxlRGVmOiBOZ01vZHVsZURlZjxUPjsgfVxuXG5jb25zdCBDT01QT05FTlRfRkFDVE9SWV9SRVNPTFZFUjogU3RhdGljUHJvdmlkZXIgPSB7XG4gIHByb3ZpZGU6IHZpZXdFbmdpbmVfQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLFxuICB1c2VDbGFzczogQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLFxuICBkZXBzOiBbdmlld0VuZ2luZV9OZ01vZHVsZVJlZl0sXG59O1xuXG5leHBvcnQgY2xhc3MgTmdNb2R1bGVSZWY8VD4gZXh0ZW5kcyB2aWV3RW5naW5lX05nTW9kdWxlUmVmPFQ+IGltcGxlbWVudHMgSW50ZXJuYWxOZ01vZHVsZVJlZjxUPiB7XG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpyZXF1aXJlLWludGVybmFsLXdpdGgtdW5kZXJzY29yZVxuICBfYm9vdHN0cmFwQ29tcG9uZW50czogVHlwZTxhbnk+W10gPSBbXTtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOnJlcXVpcmUtaW50ZXJuYWwtd2l0aC11bmRlcnNjb3JlXG4gIF9yM0luamVjdG9yOiBSM0luamVjdG9yO1xuICBpbmplY3RvcjogSW5qZWN0b3IgPSB0aGlzO1xuICBpbnN0YW5jZTogVDtcbiAgZGVzdHJveUNiczogKCgpID0+IHZvaWQpW118bnVsbCA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKG5nTW9kdWxlVHlwZTogVHlwZTxUPiwgcHVibGljIF9wYXJlbnQ6IEluamVjdG9yfG51bGwpIHtcbiAgICBzdXBlcigpO1xuICAgIGNvbnN0IG5nTW9kdWxlRGVmID0gZ2V0TmdNb2R1bGVEZWYobmdNb2R1bGVUeXBlKTtcbiAgICBuZ0Rldk1vZGUgJiYgYXNzZXJ0RGVmaW5lZChcbiAgICAgICAgICAgICAgICAgICAgIG5nTW9kdWxlRGVmLFxuICAgICAgICAgICAgICAgICAgICAgYE5nTW9kdWxlICcke3N0cmluZ2lmeShuZ01vZHVsZVR5cGUpfScgaXMgbm90IGEgc3VidHlwZSBvZiAnTmdNb2R1bGVUeXBlJy5gKTtcblxuICAgIGNvbnN0IG5nTG9jYWxlSWREZWYgPSBnZXROZ0xvY2FsZUlkRGVmKG5nTW9kdWxlVHlwZSk7XG4gICAgaWYgKG5nTG9jYWxlSWREZWYpIHtcbiAgICAgIHNldExvY2FsZUlkKG5nTG9jYWxlSWREZWYpO1xuICAgIH1cblxuICAgIHRoaXMuX2Jvb3RzdHJhcENvbXBvbmVudHMgPSBtYXliZVVud3JhcEZuKG5nTW9kdWxlRGVmICEuYm9vdHN0cmFwKTtcbiAgICBjb25zdCBhZGRpdGlvbmFsUHJvdmlkZXJzOiBTdGF0aWNQcm92aWRlcltdID0gW1xuICAgICAge1xuICAgICAgICBwcm92aWRlOiB2aWV3RW5naW5lX05nTW9kdWxlUmVmLFxuICAgICAgICB1c2VWYWx1ZTogdGhpcyxcbiAgICAgIH0sXG4gICAgICBDT01QT05FTlRfRkFDVE9SWV9SRVNPTFZFUlxuICAgIF07XG4gICAgdGhpcy5fcjNJbmplY3RvciA9IGNyZWF0ZUluamVjdG9yKFxuICAgICAgICBuZ01vZHVsZVR5cGUsIF9wYXJlbnQsIGFkZGl0aW9uYWxQcm92aWRlcnMsIHN0cmluZ2lmeShuZ01vZHVsZVR5cGUpKSBhcyBSM0luamVjdG9yO1xuICAgIHRoaXMuaW5zdGFuY2UgPSB0aGlzLmdldChuZ01vZHVsZVR5cGUpO1xuICB9XG5cbiAgZ2V0KHRva2VuOiBhbnksIG5vdEZvdW5kVmFsdWU6IGFueSA9IEluamVjdG9yLlRIUk9XX0lGX05PVF9GT1VORCxcbiAgICAgIGluamVjdEZsYWdzOiBJbmplY3RGbGFncyA9IEluamVjdEZsYWdzLkRlZmF1bHQpOiBhbnkge1xuICAgIGlmICh0b2tlbiA9PT0gSW5qZWN0b3IgfHwgdG9rZW4gPT09IHZpZXdFbmdpbmVfTmdNb2R1bGVSZWYgfHwgdG9rZW4gPT09IElOSkVDVE9SKSB7XG4gICAgICByZXR1cm4gdGhpcztcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX3IzSW5qZWN0b3IuZ2V0KHRva2VuLCBub3RGb3VuZFZhbHVlLCBpbmplY3RGbGFncyk7XG4gIH1cblxuICBnZXQgY29tcG9uZW50RmFjdG9yeVJlc29sdmVyKCk6IHZpZXdFbmdpbmVfQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyIHtcbiAgICByZXR1cm4gdGhpcy5nZXQodmlld0VuZ2luZV9Db21wb25lbnRGYWN0b3J5UmVzb2x2ZXIpO1xuICB9XG5cbiAgZGVzdHJveSgpOiB2b2lkIHtcbiAgICBuZ0Rldk1vZGUgJiYgYXNzZXJ0RGVmaW5lZCh0aGlzLmRlc3Ryb3lDYnMsICdOZ01vZHVsZSBhbHJlYWR5IGRlc3Ryb3llZCcpO1xuICAgIGNvbnN0IGluamVjdG9yID0gdGhpcy5fcjNJbmplY3RvcjtcbiAgICAhaW5qZWN0b3IuZGVzdHJveWVkICYmIGluamVjdG9yLmRlc3Ryb3koKTtcbiAgICB0aGlzLmRlc3Ryb3lDYnMgIS5mb3JFYWNoKGZuID0+IGZuKCkpO1xuICAgIHRoaXMuZGVzdHJveUNicyA9IG51bGw7XG4gIH1cbiAgb25EZXN0cm95KGNhbGxiYWNrOiAoKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgbmdEZXZNb2RlICYmIGFzc2VydERlZmluZWQodGhpcy5kZXN0cm95Q2JzLCAnTmdNb2R1bGUgYWxyZWFkeSBkZXN0cm95ZWQnKTtcbiAgICB0aGlzLmRlc3Ryb3lDYnMgIS5wdXNoKGNhbGxiYWNrKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTmdNb2R1bGVGYWN0b3J5PFQ+IGV4dGVuZHMgdmlld0VuZ2luZV9OZ01vZHVsZUZhY3Rvcnk8VD4ge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgbW9kdWxlVHlwZTogVHlwZTxUPikge1xuICAgIHN1cGVyKCk7XG5cbiAgICBjb25zdCBuZ01vZHVsZURlZiA9IGdldE5nTW9kdWxlRGVmKG1vZHVsZVR5cGUpO1xuICAgIGlmIChuZ01vZHVsZURlZiAhPT0gbnVsbCkge1xuICAgICAgLy8gUmVnaXN0ZXIgdGhlIE5nTW9kdWxlIHdpdGggQW5ndWxhcidzIG1vZHVsZSByZWdpc3RyeS4gVGhlIGxvY2F0aW9uIChhbmQgaGVuY2UgdGltaW5nKSBvZlxuICAgICAgLy8gdGhpcyBjYWxsIGlzIGNyaXRpY2FsIHRvIGVuc3VyZSB0aGlzIHdvcmtzIGNvcnJlY3RseSAobW9kdWxlcyBnZXQgcmVnaXN0ZXJlZCB3aGVuIGV4cGVjdGVkKVxuICAgICAgLy8gd2l0aG91dCBibG9hdGluZyBidW5kbGVzIChtb2R1bGVzIGFyZSByZWdpc3RlcmVkIHdoZW4gb3RoZXJ3aXNlIG5vdCByZWZlcmVuY2VkKS5cbiAgICAgIC8vXG4gICAgICAvLyBJbiBWaWV3IEVuZ2luZSwgcmVnaXN0cmF0aW9uIG9jY3VycyBpbiB0aGUgLm5nZmFjdG9yeS5qcyBmaWxlIGFzIGEgc2lkZSBlZmZlY3QuIFRoaXMgaGFzXG4gICAgICAvLyBzZXZlcmFsIHByYWN0aWNhbCBjb25zZXF1ZW5jZXM6XG4gICAgICAvL1xuICAgICAgLy8gLSBJZiBhbiAubmdmYWN0b3J5IGZpbGUgaXMgbm90IGltcG9ydGVkIGZyb20sIHRoZSBtb2R1bGUgd29uJ3QgYmUgcmVnaXN0ZXJlZCAoYW5kIGNhbiBiZVxuICAgICAgLy8gICB0cmVlIHNoYWtlbikuXG4gICAgICAvLyAtIElmIGFuIC5uZ2ZhY3RvcnkgZmlsZSBpcyBpbXBvcnRlZCBmcm9tLCB0aGUgbW9kdWxlIHdpbGwgYmUgcmVnaXN0ZXJlZCBldmVuIGlmIGFuIGluc3RhbmNlXG4gICAgICAvLyAgIGlzIG5vdCBhY3R1YWxseSBjcmVhdGVkICh2aWEgYGNyZWF0ZWAgYmVsb3cpLlxuICAgICAgLy8gLSBTaW5jZSBhbiAubmdmYWN0b3J5IGZpbGUgaW4gVmlldyBFbmdpbmUgcmVmZXJlbmNlcyB0aGUgLm5nZmFjdG9yeSBmaWxlcyBvZiB0aGUgTmdNb2R1bGUnc1xuICAgICAgLy8gICBpbXBvcnRzLFxuICAgICAgLy9cbiAgICAgIC8vIEluIEl2eSwgdGhpbmdzIGFyZSBhIGJpdCBkaWZmZXJlbnQuIC5uZ2ZhY3RvcnkgZmlsZXMgc3RpbGwgZXhpc3QgZm9yIGNvbXBhdGliaWxpdHksIGJ1dCBhcmVcbiAgICAgIC8vIG5vdCBhIHJlcXVpcmVkIEFQSSB0byB1c2UgLSB0aGVyZSBhcmUgb3RoZXIgd2F5cyB0byBvYnRhaW4gYW4gTmdNb2R1bGVGYWN0b3J5IGZvciBhIGdpdmVuXG4gICAgICAvLyBOZ01vZHVsZS4gVGh1cywgcmVseWluZyBvbiBhIHNpZGUgZWZmZWN0IGluIHRoZSAubmdmYWN0b3J5IGZpbGUgaXMgbm90IHN1ZmZpY2llbnQuIEluc3RlYWQsXG4gICAgICAvLyB0aGUgc2lkZSBlZmZlY3Qgb2YgcmVnaXN0cmF0aW9uIGlzIGFkZGVkIGhlcmUsIGluIHRoZSBjb25zdHJ1Y3RvciBvZiBOZ01vZHVsZUZhY3RvcnksXG4gICAgICAvLyBlbnN1cmluZyBubyBtYXR0ZXIgaG93IGEgZmFjdG9yeSBpcyBjcmVhdGVkLCB0aGUgbW9kdWxlIGlzIHJlZ2lzdGVyZWQgY29ycmVjdGx5LlxuICAgICAgLy9cbiAgICAgIC8vIEFuIGFsdGVybmF0aXZlIHdvdWxkIGJlIHRvIGluY2x1ZGUgdGhlIHJlZ2lzdHJhdGlvbiBzaWRlIGVmZmVjdCBpbmxpbmUgZm9sbG93aW5nIHRoZSBhY3R1YWxcbiAgICAgIC8vIE5nTW9kdWxlIGRlZmluaXRpb24uIFRoaXMgYWxzbyBoYXMgdGhlIGNvcnJlY3QgdGltaW5nLCBidXQgYnJlYWtzIHRyZWUtc2hha2luZyAtIG1vZHVsZXNcbiAgICAgIC8vIHdpbGwgYmUgcmVnaXN0ZXJlZCBhbmQgcmV0YWluZWQgZXZlbiBpZiB0aGV5J3JlIG90aGVyd2lzZSBuZXZlciByZWZlcmVuY2VkLlxuICAgICAgcmVnaXN0ZXJOZ01vZHVsZVR5cGUobW9kdWxlVHlwZSBhcyBOZ01vZHVsZVR5cGUpO1xuICAgIH1cbiAgfVxuXG4gIGNyZWF0ZShwYXJlbnRJbmplY3RvcjogSW5qZWN0b3J8bnVsbCk6IHZpZXdFbmdpbmVfTmdNb2R1bGVSZWY8VD4ge1xuICAgIHJldHVybiBuZXcgTmdNb2R1bGVSZWYodGhpcy5tb2R1bGVUeXBlLCBwYXJlbnRJbmplY3Rvcik7XG4gIH1cbn1cbiJdfQ==