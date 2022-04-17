/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
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
var COMPONENT_FACTORY_RESOLVER = {
    provide: viewEngine_ComponentFactoryResolver,
    useClass: ComponentFactoryResolver,
    deps: [viewEngine_NgModuleRef],
};
var NgModuleRef = /** @class */ (function (_super) {
    tslib_1.__extends(NgModuleRef, _super);
    function NgModuleRef(ngModuleType, _parent) {
        var _this = _super.call(this) || this;
        _this._parent = _parent;
        // tslint:disable-next-line:require-internal-with-underscore
        _this._bootstrapComponents = [];
        _this.injector = _this;
        _this.destroyCbs = [];
        var ngModuleDef = getNgModuleDef(ngModuleType);
        ngDevMode && assertDefined(ngModuleDef, "NgModule '" + stringify(ngModuleType) + "' is not a subtype of 'NgModuleType'.");
        var ngLocaleIdDef = getNgLocaleIdDef(ngModuleType);
        if (ngLocaleIdDef) {
            setLocaleId(ngLocaleIdDef);
        }
        _this._bootstrapComponents = maybeUnwrapFn(ngModuleDef.bootstrap);
        var additionalProviders = [
            {
                provide: viewEngine_NgModuleRef,
                useValue: _this,
            },
            COMPONENT_FACTORY_RESOLVER
        ];
        _this._r3Injector = createInjector(ngModuleType, _parent, additionalProviders, stringify(ngModuleType));
        _this.instance = _this.get(ngModuleType);
        return _this;
    }
    NgModuleRef.prototype.get = function (token, notFoundValue, injectFlags) {
        if (notFoundValue === void 0) { notFoundValue = Injector.THROW_IF_NOT_FOUND; }
        if (injectFlags === void 0) { injectFlags = InjectFlags.Default; }
        if (token === Injector || token === viewEngine_NgModuleRef || token === INJECTOR) {
            return this;
        }
        return this._r3Injector.get(token, notFoundValue, injectFlags);
    };
    Object.defineProperty(NgModuleRef.prototype, "componentFactoryResolver", {
        get: function () {
            return this.get(viewEngine_ComponentFactoryResolver);
        },
        enumerable: true,
        configurable: true
    });
    NgModuleRef.prototype.destroy = function () {
        ngDevMode && assertDefined(this.destroyCbs, 'NgModule already destroyed');
        var injector = this._r3Injector;
        !injector.destroyed && injector.destroy();
        this.destroyCbs.forEach(function (fn) { return fn(); });
        this.destroyCbs = null;
    };
    NgModuleRef.prototype.onDestroy = function (callback) {
        ngDevMode && assertDefined(this.destroyCbs, 'NgModule already destroyed');
        this.destroyCbs.push(callback);
    };
    return NgModuleRef;
}(viewEngine_NgModuleRef));
export { NgModuleRef };
var NgModuleFactory = /** @class */ (function (_super) {
    tslib_1.__extends(NgModuleFactory, _super);
    function NgModuleFactory(moduleType) {
        var _this = _super.call(this) || this;
        _this.moduleType = moduleType;
        var ngModuleDef = getNgModuleDef(moduleType);
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
            registerNgModuleType(moduleType);
        }
        return _this;
    }
    NgModuleFactory.prototype.create = function (parentInjector) {
        return new NgModuleRef(this.moduleType, parentInjector);
    };
    return NgModuleFactory;
}(viewEngine_NgModuleFactory));
export { NgModuleFactory };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbW9kdWxlX3JlZi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvbmdfbW9kdWxlX3JlZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGdCQUFnQixDQUFDO0FBQ3hDLE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSw4QkFBOEIsQ0FBQztBQUN0RCxPQUFPLEVBQUMsV0FBVyxFQUFDLE1BQU0sMEJBQTBCLENBQUM7QUFFckQsT0FBTyxFQUFhLGNBQWMsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBRTdELE9BQU8sRUFBQyx3QkFBd0IsSUFBSSxtQ0FBbUMsRUFBQyxNQUFNLHNDQUFzQyxDQUFDO0FBQ3JILE9BQU8sRUFBc0IsZUFBZSxJQUFJLDBCQUEwQixFQUFFLFdBQVcsSUFBSSxzQkFBc0IsRUFBQyxNQUFNLDZCQUE2QixDQUFDO0FBQ3RKLE9BQU8sRUFBQyxvQkFBb0IsRUFBQyxNQUFNLDBDQUEwQyxDQUFDO0FBRTlFLE9BQU8sRUFBQyxhQUFhLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUM3QyxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFFNUMsT0FBTyxFQUFDLHdCQUF3QixFQUFDLE1BQU0saUJBQWlCLENBQUM7QUFDekQsT0FBTyxFQUFDLGdCQUFnQixFQUFFLGNBQWMsRUFBQyxNQUFNLGNBQWMsQ0FBQztBQUM5RCxPQUFPLEVBQUMsV0FBVyxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBQ25DLE9BQU8sRUFBQyxhQUFhLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUloRCxJQUFNLDBCQUEwQixHQUFtQjtJQUNqRCxPQUFPLEVBQUUsbUNBQW1DO0lBQzVDLFFBQVEsRUFBRSx3QkFBd0I7SUFDbEMsSUFBSSxFQUFFLENBQUMsc0JBQXNCLENB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QyxVQUFVLENBQUMsQ0FBQztRQUMvQyxJQUFJLFdBQVcsS0FBSyxJQUFJLEVBQUU7WUFDeEIsMkZBQTJGO1lBQzNGLDhGQUE4RjtZQUM5RixtRkFBbUY7WUFDbkYsRUFBRTtZQUNGLDJGQUEyRjtZQUMzRixrQ0FBa0M7WUFDbEMsRUFBRTtZQUNGLDJGQUEyRjtZQUMzRixrQkFBa0I7WUFDbEIsOEZBQThGO1lBQzlGLGtEQUFrRDtZQUNsRCw4RkFBOEY7WUFDOUYsYUFBYTtZQUNiLEVBQUU7WUFDRiw4RkFBOEY7WUFDOUYsNEZBQTRGO1lBQzVGLDhGQUE4RjtZQUM5Rix3RkFBd0Y7WUFDeEYsbUZBQW1GO1lBQ25GLEVBQUU7WUFDRiw4RkFBOEY7WUFDOUYsMkZBQTJGO1lBQzNGLDhFQUE4RTtZQUM5RSxvQkFBb0IsQ0FBQyxVQUEwQixDQUFDLENBQUM7U0FDbEQ7O0lBQ0gsQ0FBQztJQUVELGdDQUFNLEdBQU4sVUFBTyxjQUE2QjtRQUNsQyxPQUFPLElBQUksV0FBVyxDQUFDLElBQUksQ0FBQyxVQUFVLEVBQUUsY0FBYyxDQUFDLENBQUM7SUFDMUQsQ0FBQztJQUNILHNCQUFDO0FBQUQsQ0FBQyxBQXBDRCxDQUF3QywwQkFBMEIsR0FvQ2pFIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdG9yfSBmcm9tICcuLi9kaS9pbmplY3Rvcic7XG5pbXBvcnQge0lOSkVDVE9SfSBmcm9tICcuLi9kaS9pbmplY3Rvcl9jb21wYXRpYmlsaXR5JztcbmltcG9ydCB7SW5qZWN0RmxhZ3N9IGZyb20gJy4uL2RpL2ludGVyZmFjZS9pbmplY3Rvcic7XG5pbXBvcnQge1N0YXRpY1Byb3ZpZGVyfSBmcm9tICcuLi9kaS9pbnRlcmZhY2UvcHJvdmlkZXInO1xuaW1wb3J0IHtSM0luamVjdG9yLCBjcmVhdGVJbmplY3Rvcn0gZnJvbSAnLi4vZGkvcjNfaW5qZWN0b3InO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9pbnRlcmZhY2UvdHlwZSc7XG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnlSZXNvbHZlciBhcyB2aWV3RW5naW5lX0NvbXBvbmVudEZhY3RvcnlSZXNvbHZlcn0gZnJvbSAnLi4vbGlua2VyL2NvbXBvbmVudF9mYWN0b3J5X3Jlc29sdmVyJztcbmltcG9ydCB7SW50ZXJuYWxOZ01vZHVsZVJlZiwgTmdNb2R1bGVGYWN0b3J5IGFzIHZpZXdFbmdpbmVfTmdNb2R1bGVGYWN0b3J5LCBOZ01vZHVsZVJlZiBhcyB2aWV3RW5naW5lX05nTW9kdWxlUmVmfSBmcm9tICcuLi9saW5rZXIvbmdfbW9kdWxlX2ZhY3RvcnknO1xuaW1wb3J0IHtyZWdpc3Rlck5nTW9kdWxlVHlwZX0gZnJvbSAnLi4vbGlua2VyL25nX21vZHVsZV9mYWN0b3J5X3JlZ2lzdHJhdGlvbic7XG5pbXBvcnQge05nTW9kdWxlRGVmfSBmcm9tICcuLi9tZXRhZGF0YS9uZ19tb2R1bGUnO1xuaW1wb3J0IHthc3NlcnREZWZpbmVkfSBmcm9tICcuLi91dGlsL2Fzc2VydCc7XG5pbXBvcnQge3N0cmluZ2lmeX0gZnJvbSAnLi4vdXRpbC9zdHJpbmdpZnknO1xuXG5pbXBvcnQge0NvbXBvbmVudEZhY3RvcnlSZXNvbHZlcn0gZnJvbSAnLi9jb21wb25lbnRfcmVmJztcbmltcG9ydCB7Z2V0TmdMb2NhbGVJZERlZiwgZ2V0TmdNb2R1bGVEZWZ9IGZyb20gJy4vZGVmaW5pdGlvbic7XG5pbXBvcnQge3NldExvY2FsZUlkfSBmcm9tICcuL2kxOG4nO1xuaW1wb3J0IHttYXliZVVud3JhcEZufSBmcm9tICcuL3V0aWwvbWlzY191dGlscyc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgTmdNb2R1bGVUeXBlPFQgPSBhbnk+IGV4dGVuZHMgVHlwZTxUPiB7IG5nTW9kdWxlRGVmOiBOZ01vZHVsZURlZjxUPjsgfVxuXG5jb25zdCBDT01QT05FTlRfRkFDVE9SWV9SRVNPTFZFUjogU3RhdGljUHJvdmlkZXIgPSB7XG4gIHByb3ZpZGU6IHZpZXdFbmdpbmVfQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLFxuICB1c2VDbGFzczogQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLFxuICBkZXBzOiBbdmlld0VuZ2luZV9OZ01vZHVsZVJlZl0sXG59O1xuXG5leHBvcnQgY2xhc3MgTmdNb2R1bGVSZWY8VD4gZXh0ZW5kcyB2aWV3RW5naW5lX05nTW9kdWxlUmVmPFQ+IGltcGxlbWVudHMgSW50ZXJuYWxOZ01vZHVsZVJlZjxUPiB7XG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpyZXF1aXJlLWludGVybmFsLXdpdGgtdW5kZXJzY29yZVxuICBfYm9vdHN0cmFwQ29tcG9uZW50czogVHlwZTxhbnk+W10gPSBbXTtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOnJlcXVpcmUtaW50ZXJuYWwtd2l0aC11bmRlcnNjb3JlXG4gIF9yM0luamVjdG9yOiBSM0luamVjdG9yO1xuICBpbmplY3RvcjogSW5qZWN0b3IgPSB0aGlzO1xuICBpbnN0YW5jZTogVDtcbiAgZGVzdHJveUNiczogKCgpID0+IHZvaWQpW118bnVsbCA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKG5nTW9kdWxlVHlwZTogVHlwZTxUPiwgcHVibGljIF9wYXJlbnQ6IEluamVjdG9yfG51bGwpIHtcbiAgICBzdXBlcigpO1xuICAgIGNvbnN0IG5nTW9kdWxlRGVmID0gZ2V0TmdNb2R1bGVEZWYobmdNb2R1bGVUeXBlKTtcbiAgICBuZ0Rldk1vZGUgJiYgYXNzZXJ0RGVmaW5lZChcbiAgICAgICAgICAgICAgICAgICAgIG5nTW9kdWxlRGVmLFxuICAgICAgICAgICAgICAgICAgICAgYE5nTW9kdWxlICcke3N0cmluZ2lmeShuZ01vZHVsZVR5cGUpfScgaXMgbm90IGEgc3VidHlwZSBvZiAnTmdNb2R1bGVUeXBlJy5gKTtcblxuICAgIGNvbnN0IG5nTG9jYWxlSWREZWYgPSBnZXROZ0xvY2FsZUlkRGVmKG5nTW9kdWxlVHlwZSk7XG4gICAgaWYgKG5nTG9jYWxlSWREZWYpIHtcbiAgICAgIHNldExvY2FsZUlkKG5nTG9jYWxlSWREZWYpO1xuICAgIH1cblxuICAgIHRoaXMuX2Jvb3RzdHJhcENvbXBvbmVudHMgPSBtYXliZVVud3JhcEZuKG5nTW9kdWxlRGVmICEuYm9vdHN0cmFwKTtcbiAgICBjb25zdCBhZGRpdGlvbmFsUHJvdmlkZXJzOiBTdGF0aWNQcm92aWRlcltdID0gW1xuICAgICAge1xuICAgICAgICBwcm92aWRlOiB2aWV3RW5naW5lX05nTW9kdWxlUmVmLFxuICAgICAgICB1c2VWYWx1ZTogdGhpcyxcbiAgICAgIH0sXG4gICAgICBDT01QT05FTlRfRkFDVE9SWV9SRVNPTFZFUlxuICAgIF07XG4gICAgdGhpcy5fcjNJbmplY3RvciA9IGNyZWF0ZUluamVjdG9yKFxuICAgICAgICBuZ01vZHVsZVR5cGUsIF9wYXJlbnQsIGFkZGl0aW9uYWxQcm92aWRlcnMsIHN0cmluZ2lmeShuZ01vZHVsZVR5cGUpKSBhcyBSM0luamVjdG9yO1xuICAgIHRoaXMuaW5zdGFuY2UgPSB0aGlzLmdldChuZ01vZHVsZVR5cGUpO1xuICB9XG5cbiAgZ2V0KHRva2VuOiBhbnksIG5vdEZvdW5kVmFsdWU6IGFueSA9IEluamVjdG9yLlRIUk9XX0lGX05PVF9GT1VORCxcbiAgICAgIGluamVjdEZsYWdzOiBJbmplY3RGbGFncyA9IEluamVjdEZsYWdzLkRlZmF1bHQpOiBhbnkge1xuICAgIGlmICh0b2tlbiA9PT0gSW5qZWN0b3IgfHwgdG9rZW4gPT09IHZpZXdFbmdpbmVfTmdNb2R1bGVSZWYgfHwgdG9rZW4gPT09IElOSkVDVE9SKSB7XG4gICAgICByZXR1cm4gdGhpcztcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX3IzSW5qZWN0b3IuZ2V0KHRva2VuLCBub3RGb3VuZFZhbHVlLCBpbmplY3RGbGFncyk7XG4gIH1cblxuICBnZXQgY29tcG9uZW50RmFjdG9yeVJlc29sdmVyKCk6IHZpZXdFbmdpbmVfQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyIHtcbiAgICByZXR1cm4gdGhpcy5nZXQodmlld0VuZ2luZV9Db21wb25lbnRGYWN0b3J5UmVzb2x2ZXIpO1xuICB9XG5cbiAgZGVzdHJveSgpOiB2b2lkIHtcbiAgICBuZ0Rldk1vZGUgJiYgYXNzZXJ0RGVmaW5lZCh0aGlzLmRlc3Ryb3lDYnMsICdOZ01vZHVsZSBhbHJlYWR5IGRlc3Ryb3llZCcpO1xuICAgIGNvbnN0IGluamVjdG9yID0gdGhpcy5fcjNJbmplY3RvcjtcbiAgICAhaW5qZWN0b3IuZGVzdHJveWVkICYmIGluamVjdG9yLmRlc3Ryb3koKTtcbiAgICB0aGlzLmRlc3Ryb3lDYnMgIS5mb3JFYWNoKGZuID0+IGZuKCkpO1xuICAgIHRoaXMuZGVzdHJveUNicyA9IG51bGw7XG4gIH1cbiAgb25EZXN0cm95KGNhbGxiYWNrOiAoKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgbmdEZXZNb2RlICYmIGFzc2VydERlZmluZWQodGhpcy5kZXN0cm95Q2JzLCAnTmdNb2R1bGUgYWxyZWFkeSBkZXN0cm95ZWQnKTtcbiAgICB0aGlzLmRlc3Ryb3lDYnMgIS5wdXNoKGNhbGxiYWNrKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgTmdNb2R1bGVGYWN0b3J5PFQ+IGV4dGVuZHMgdmlld0VuZ2luZV9OZ01vZHVsZUZhY3Rvcnk8VD4ge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgbW9kdWxlVHlwZTogVHlwZTxUPikge1xuICAgIHN1cGVyKCk7XG5cbiAgICBjb25zdCBuZ01vZHVsZURlZiA9IGdldE5nTW9kdWxlRGVmKG1vZHVsZVR5cGUpO1xuICAgIGlmIChuZ01vZHVsZURlZiAhPT0gbnVsbCkge1xuICAgICAgLy8gUmVnaXN0ZXIgdGhlIE5nTW9kdWxlIHdpdGggQW5ndWxhcidzIG1vZHVsZSByZWdpc3RyeS4gVGhlIGxvY2F0aW9uIChhbmQgaGVuY2UgdGltaW5nKSBvZlxuICAgICAgLy8gdGhpcyBjYWxsIGlzIGNyaXRpY2FsIHRvIGVuc3VyZSB0aGlzIHdvcmtzIGNvcnJlY3RseSAobW9kdWxlcyBnZXQgcmVnaXN0ZXJlZCB3aGVuIGV4cGVjdGVkKVxuICAgICAgLy8gd2l0aG91dCBibG9hdGluZyBidW5kbGVzIChtb2R1bGVzIGFyZSByZWdpc3RlcmVkIHdoZW4gb3RoZXJ3aXNlIG5vdCByZWZlcmVuY2VkKS5cbiAgICAgIC8vXG4gICAgICAvLyBJbiBWaWV3IEVuZ2luZSwgcmVnaXN0cmF0aW9uIG9jY3VycyBpbiB0aGUgLm5nZmFjdG9yeS5qcyBmaWxlIGFzIGEgc2lkZSBlZmZlY3QuIFRoaXMgaGFzXG4gICAgICAvLyBzZXZlcmFsIHByYWN0aWNhbCBjb25zZXF1ZW5jZXM6XG4gICAgICAvL1xuICAgICAgLy8gLSBJZiBhbiAubmdmYWN0b3J5IGZpbGUgaXMgbm90IGltcG9ydGVkIGZyb20sIHRoZSBtb2R1bGUgd29uJ3QgYmUgcmVnaXN0ZXJlZCAoYW5kIGNhbiBiZVxuICAgICAgLy8gICB0cmVlIHNoYWtlbikuXG4gICAgICAvLyAtIElmIGFuIC5uZ2ZhY3RvcnkgZmlsZSBpcyBpbXBvcnRlZCBmcm9tLCB0aGUgbW9kdWxlIHdpbGwgYmUgcmVnaXN0ZXJlZCBldmVuIGlmIGFuIGluc3RhbmNlXG4gICAgICAvLyAgIGlzIG5vdCBhY3R1YWxseSBjcmVhdGVkICh2aWEgYGNyZWF0ZWAgYmVsb3cpLlxuICAgICAgLy8gLSBTaW5jZSBhbiAubmdmYWN0b3J5IGZpbGUgaW4gVmlldyBFbmdpbmUgcmVmZXJlbmNlcyB0aGUgLm5nZmFjdG9yeSBmaWxlcyBvZiB0aGUgTmdNb2R1bGUnc1xuICAgICAgLy8gICBpbXBvcnRzLFxuICAgICAgLy9cbiAgICAgIC8vIEluIEl2eSwgdGhpbmdzIGFyZSBhIGJpdCBkaWZmZXJlbnQuIC5uZ2ZhY3RvcnkgZmlsZXMgc3RpbGwgZXhpc3QgZm9yIGNvbXBhdGliaWxpdHksIGJ1dCBhcmVcbiAgICAgIC8vIG5vdCBhIHJlcXVpcmVkIEFQSSB0byB1c2UgLSB0aGVyZSBhcmUgb3RoZXIgd2F5cyB0byBvYnRhaW4gYW4gTmdNb2R1bGVGYWN0b3J5IGZvciBhIGdpdmVuXG4gICAgICAvLyBOZ01vZHVsZS4gVGh1cywgcmVseWluZyBvbiBhIHNpZGUgZWZmZWN0IGluIHRoZSAubmdmYWN0b3J5IGZpbGUgaXMgbm90IHN1ZmZpY2llbnQuIEluc3RlYWQsXG4gICAgICAvLyB0aGUgc2lkZSBlZmZlY3Qgb2YgcmVnaXN0cmF0aW9uIGlzIGFkZGVkIGhlcmUsIGluIHRoZSBjb25zdHJ1Y3RvciBvZiBOZ01vZHVsZUZhY3RvcnksXG4gICAgICAvLyBlbnN1cmluZyBubyBtYXR0ZXIgaG93IGEgZmFjdG9yeSBpcyBjcmVhdGVkLCB0aGUgbW9kdWxlIGlzIHJlZ2lzdGVyZWQgY29ycmVjdGx5LlxuICAgICAgLy9cbiAgICAgIC8vIEFuIGFsdGVybmF0aXZlIHdvdWxkIGJlIHRvIGluY2x1ZGUgdGhlIHJlZ2lzdHJhdGlvbiBzaWRlIGVmZmVjdCBpbmxpbmUgZm9sbG93aW5nIHRoZSBhY3R1YWxcbiAgICAgIC8vIE5nTW9kdWxlIGRlZmluaXRpb24uIFRoaXMgYWxzbyBoYXMgdGhlIGNvcnJlY3QgdGltaW5nLCBidXQgYnJlYWtzIHRyZWUtc2hha2luZyAtIG1vZHVsZXNcbiAgICAgIC8vIHdpbGwgYmUgcmVnaXN0ZXJlZCBhbmQgcmV0YWluZWQgZXZlbiBpZiB0aGV5J3JlIG90aGVyd2lzZSBuZXZlciByZWZlcmVuY2VkLlxuICAgICAgcmVnaXN0ZXJOZ01vZHVsZVR5cGUobW9kdWxlVHlwZSBhcyBOZ01vZHVsZVR5cGUpO1xuICAgIH1cbiAgfVxuXG4gIGNyZWF0ZShwYXJlbnRJbmplY3RvcjogSW5qZWN0b3J8bnVsbCk6IHZpZXdFbmdpbmVfTmdNb2R1bGVSZWY8VD4ge1xuICAgIHJldHVybiBuZXcgTmdNb2R1bGVSZWYodGhpcy5tb2R1bGVUeXBlLCBwYXJlbnRJbmplY3Rvcik7XG4gIH1cbn1cbiJdfQ==