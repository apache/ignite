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
import { Injectable } from '../di/injectable';
import { InjectionToken } from '../di/injection_token';
import { ComponentFactory as ComponentFactoryR3 } from '../render3/component_ref';
import { getComponentDef, getNgModuleDef } from '../render3/definition';
import { NgModuleFactory as NgModuleFactoryR3 } from '../render3/ng_module_ref';
import { maybeUnwrapFn } from '../render3/util/misc_utils';
/**
 * Combination of NgModuleFactory and ComponentFactorys.
 *
 * \@publicApi
 * @template T
 */
export class ModuleWithComponentFactories {
    /**
     * @param {?} ngModuleFactory
     * @param {?} componentFactories
     */
    constructor(ngModuleFactory, componentFactories) {
        this.ngModuleFactory = ngModuleFactory;
        this.componentFactories = componentFactories;
    }
}
if (false) {
    /** @type {?} */
    ModuleWithComponentFactories.prototype.ngModuleFactory;
    /** @type {?} */
    ModuleWithComponentFactories.prototype.componentFactories;
}
/**
 * @return {?}
 */
function _throwError() {
    throw new Error(`Runtime compiler is not loaded`);
}
/** @type {?} */
const Compiler_compileModuleSync__PRE_R3__ = (/** @type {?} */ (_throwError));
/** @type {?} */
export const Compiler_compileModuleSync__POST_R3__ = (/**
 * @template T
 * @param {?} moduleType
 * @return {?}
 */
function (moduleType) {
    return new NgModuleFactoryR3(moduleType);
});
/** @type {?} */
const Compiler_compileModuleSync = Compiler_compileModuleSync__PRE_R3__;
/** @type {?} */
const Compiler_compileModuleAsync__PRE_R3__ = (/** @type {?} */ (_throwError));
/** @type {?} */
export const Compiler_compileModuleAsync__POST_R3__ = (/**
 * @template T
 * @param {?} moduleType
 * @return {?}
 */
function (moduleType) {
    return Promise.resolve(Compiler_compileModuleSync__POST_R3__(moduleType));
});
/** @type {?} */
const Compiler_compileModuleAsync = Compiler_compileModuleAsync__PRE_R3__;
/** @type {?} */
const Compiler_compileModuleAndAllComponentsSync__PRE_R3__ = (/** @type {?} */ (_throwError));
/** @type {?} */
export const Compiler_compileModuleAndAllComponentsSync__POST_R3__ = (/**
 * @template T
 * @param {?} moduleType
 * @return {?}
 */
function (moduleType) {
    /** @type {?} */
    const ngModuleFactory = Compiler_compileModuleSync__POST_R3__(moduleType);
    /** @type {?} */
    const moduleDef = (/** @type {?} */ (getNgModuleDef(moduleType)));
    /** @type {?} */
    const componentFactories = maybeUnwrapFn(moduleDef.declarations)
        .reduce((/**
     * @param {?} factories
     * @param {?} declaration
     * @return {?}
     */
    (factories, declaration) => {
        /** @type {?} */
        const componentDef = getComponentDef(declaration);
        componentDef && factories.push(new ComponentFactoryR3(componentDef));
        return factories;
    }), (/** @type {?} */ ([])));
    return new ModuleWithComponentFactories(ngModuleFactory, componentFactories);
});
/** @type {?} */
const Compiler_compileModuleAndAllComponentsSync = Compiler_compileModuleAndAllComponentsSync__PRE_R3__;
/** @type {?} */
const Compiler_compileModuleAndAllComponentsAsync__PRE_R3__ = (/** @type {?} */ (_throwError));
/** @type {?} */
export const Compiler_compileModuleAndAllComponentsAsync__POST_R3__ = (/**
 * @template T
 * @param {?} moduleType
 * @return {?}
 */
function (moduleType) {
    return Promise.resolve(Compiler_compileModuleAndAllComponentsSync__POST_R3__(moduleType));
});
/** @type {?} */
const Compiler_compileModuleAndAllComponentsAsync = Compiler_compileModuleAndAllComponentsAsync__PRE_R3__;
/**
 * Low-level service for running the angular compiler during runtime
 * to create {\@link ComponentFactory}s, which
 * can later be used to create and render a Component instance.
 *
 * Each `\@NgModule` provides an own `Compiler` to its injector,
 * that will use the directives/pipes of the ng module for compilation
 * of components.
 *
 * \@publicApi
 */
export class Compiler {
    constructor() {
        /**
         * Compiles the given NgModule and all of its components. All templates of the components listed
         * in `entryComponents` have to be inlined.
         */
        this.compileModuleSync = Compiler_compileModuleSync;
        /**
         * Compiles the given NgModule and all of its components
         */
        this.compileModuleAsync = Compiler_compileModuleAsync;
        /**
         * Same as {\@link #compileModuleSync} but also creates ComponentFactories for all components.
         */
        this.compileModuleAndAllComponentsSync = Compiler_compileModuleAndAllComponentsSync;
        /**
         * Same as {\@link #compileModuleAsync} but also creates ComponentFactories for all components.
         */
        this.compileModuleAndAllComponentsAsync = Compiler_compileModuleAndAllComponentsAsync;
    }
    /**
     * Clears all caches.
     * @return {?}
     */
    clearCache() { }
    /**
     * Clears the cache for the given component/ngModule.
     * @param {?} type
     * @return {?}
     */
    clearCacheFor(type) { }
    /**
     * Returns the id for a given NgModule, if one is defined and known to the compiler.
     * @param {?} moduleType
     * @return {?}
     */
    getModuleId(moduleType) { return undefined; }
}
Compiler.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * Compiles the given NgModule and all of its components. All templates of the components listed
     * in `entryComponents` have to be inlined.
     * @type {?}
     */
    Compiler.prototype.compileModuleSync;
    /**
     * Compiles the given NgModule and all of its components
     * @type {?}
     */
    Compiler.prototype.compileModuleAsync;
    /**
     * Same as {\@link #compileModuleSync} but also creates ComponentFactories for all components.
     * @type {?}
     */
    Compiler.prototype.compileModuleAndAllComponentsSync;
    /**
     * Same as {\@link #compileModuleAsync} but also creates ComponentFactories for all components.
     * @type {?}
     */
    Compiler.prototype.compileModuleAndAllComponentsAsync;
}
/**
 * Token to provide CompilerOptions in the platform injector.
 *
 * \@publicApi
 * @type {?}
 */
export const COMPILER_OPTIONS = new InjectionToken('compilerOptions');
/**
 * A factory for creating a Compiler
 *
 * \@publicApi
 * @abstract
 */
export class CompilerFactory {
}
if (false) {
    /**
     * @abstract
     * @param {?=} options
     * @return {?}
     */
    CompilerFactory.prototype.createCompiler = function (options) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9saW5rZXIvY29tcGlsZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsVUFBVSxFQUFDLE1BQU0sa0JBQWtCLENBQUM7QUFDNUMsT0FBTyxFQUFDLGNBQWMsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBS3JELE9BQU8sRUFBQyxnQkFBZ0IsSUFBSSxrQkFBa0IsRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBQ2hGLE9BQU8sRUFBQyxlQUFlLEVBQUUsY0FBYyxFQUFDLE1BQU0sdUJBQXVCLENBQUM7QUFDdEUsT0FBTyxFQUFDLGVBQWUsSUFBSSxpQkFBaUIsRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBQzlFLE9BQU8sRUFBQyxhQUFhLEVBQUMsTUFBTSw0QkFBNEIsQ0FBQzs7Ozs7OztBQVl6RCxNQUFNLE9BQU8sNEJBQTRCOzs7OztJQUN2QyxZQUNXLGVBQW1DLEVBQ25DLGtCQUEyQztRQUQzQyxvQkFBZSxHQUFmLGVBQWUsQ0FBb0I7UUFDbkMsdUJBQWtCLEdBQWxCLGtCQUFrQixDQUF5QjtJQUFHLENBQUM7Q0FDM0Q7OztJQUZLLHVEQUEwQzs7SUFDMUMsMERBQWtEOzs7OztBQUl4RCxTQUFTLFdBQVc7SUFDbEIsTUFBTSxJQUFJLEtBQUssQ0FBQyxnQ0FBZ0MsQ0FBQyxDQUFDO0FBQ3BELENBQUM7O01BRUssb0NBQW9DLEdBQ3RDLG1CQUFBLFdBQVcsRUFBTzs7QUFDdEIsTUFBTSxPQUFPLHFDQUFxQzs7Ozs7QUFDekIsVUFBWSxVQUFtQjtJQUN0RCxPQUFPLElBQUksaUJBQWlCLENBQUMsVUFBVSxDQUFDLENBQUM7QUFDM0MsQ0FBQyxDQUFBOztNQUNLLDBCQUEwQixHQUFHLG9DQUFvQzs7TUFFakUscUNBQXFDLEdBQ1QsbUJBQUEsV0FBVyxFQUFPOztBQUNwRCxNQUFNLE9BQU8sc0NBQXNDOzs7OztBQUNqQixVQUFZLFVBQW1CO0lBQy9ELE9BQU8sT0FBTyxDQUFDLE9BQU8sQ0FBQyxxQ0FBcUMsQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDO0FBQzVFLENBQUMsQ0FBQTs7TUFDSywyQkFBMkIsR0FBRyxxQ0FBcUM7O01BRW5FLG9EQUFvRCxHQUNwQixtQkFBQSxXQUFXLEVBQU87O0FBQ3hELE1BQU0sT0FBTyxxREFBcUQ7Ozs7O0FBQzVCLFVBQVksVUFBbUI7O1VBRTdELGVBQWUsR0FBRyxxQ0FBcUMsQ0FBQyxVQUFVLENBQUM7O1VBQ25FLFNBQVMsR0FBRyxtQkFBQSxjQUFjLENBQUMsVUFBVSxDQUFDLEVBQUU7O1VBQ3hDLGtCQUFrQixHQUNwQixhQUFhLENBQUMsU0FBUyxDQUFDLFlBQVksQ0FBQztTQUNoQyxNQUFNOzs7OztJQUFDLENBQUMsU0FBa0MsRUFBRSxXQUFzQixFQUFFLEVBQUU7O2NBQy9ELFlBQVksR0FBRyxlQUFlLENBQUMsV0FBVyxDQUFDO1FBQ2pELFlBQVksSUFBSSxTQUFTLENBQUMsSUFBSSxDQUFDLElBQUksa0JBQWtCLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztRQUNyRSxPQUFPLFNBQVMsQ0FBQztJQUNuQixDQUFDLEdBQUUsbUJBQUEsRUFBRSxFQUEyQixDQUFDO0lBQ3pDLE9BQU8sSUFBSSw0QkFBNEIsQ0FBQyxlQUFlLEVBQUUsa0JBQWtCLENBQUMsQ0FBQztBQUMvRSxDQUFDLENBQUE7O01BQ0ssMENBQTBDLEdBQzVDLG9EQUFvRDs7TUFFbEQscURBQXFELEdBQ1osbUJBQUEsV0FBVyxFQUFPOztBQUNqRSxNQUFNLE9BQU8sc0RBQXNEOzs7OztBQUNwQixVQUFZLFVBQW1CO0lBRTVFLE9BQU8sT0FBTyxDQUFDLE9BQU8sQ0FBQyxxREFBcUQsQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDO0FBQzVGLENBQUMsQ0FBQTs7TUFDSywyQ0FBMkMsR0FDN0MscURBQXFEOzs7Ozs7Ozs7Ozs7QUFjekQsTUFBTSxPQUFPLFFBQVE7SUFEckI7Ozs7O1FBTUUsc0JBQWlCLEdBQW1ELDBCQUEwQixDQUFDOzs7O1FBSy9GLHVCQUFrQixHQUM0QywyQkFBMkIsQ0FBQzs7OztRQUsxRixzQ0FBaUMsR0FDN0IsMENBQTBDLENBQUM7Ozs7UUFLL0MsdUNBQWtDLEdBQ2EsMkNBQTJDLENBQUM7SUFnQjdGLENBQUM7Ozs7O0lBWEMsVUFBVSxLQUFVLENBQUM7Ozs7OztJQUtyQixhQUFhLENBQUMsSUFBZSxJQUFHLENBQUM7Ozs7OztJQUtqQyxXQUFXLENBQUMsVUFBcUIsSUFBc0IsT0FBTyxTQUFTLENBQUMsQ0FBQyxDQUFDOzs7WUF2QzNFLFVBQVU7Ozs7Ozs7O0lBTVQscUNBQStGOzs7OztJQUsvRixzQ0FDMEY7Ozs7O0lBSzFGLHFEQUMrQzs7Ozs7SUFLL0Msc0RBQzJGOzs7Ozs7OztBQW9DN0YsTUFBTSxPQUFPLGdCQUFnQixHQUFHLElBQUksY0FBYyxDQUFvQixpQkFBaUIsQ0FBQzs7Ozs7OztBQU94RixNQUFNLE9BQWdCLGVBQWU7Q0FFcEM7Ozs7Ozs7SUFEQyxrRUFBK0QiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0YWJsZX0gZnJvbSAnLi4vZGkvaW5qZWN0YWJsZSc7XG5pbXBvcnQge0luamVjdGlvblRva2VufSBmcm9tICcuLi9kaS9pbmplY3Rpb25fdG9rZW4nO1xuaW1wb3J0IHtTdGF0aWNQcm92aWRlcn0gZnJvbSAnLi4vZGkvaW50ZXJmYWNlL3Byb3ZpZGVyJztcbmltcG9ydCB7TWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3l9IGZyb20gJy4uL2kxOG4vdG9rZW5zJztcbmltcG9ydCB7VHlwZX0gZnJvbSAnLi4vaW50ZXJmYWNlL3R5cGUnO1xuaW1wb3J0IHtWaWV3RW5jYXBzdWxhdGlvbn0gZnJvbSAnLi4vbWV0YWRhdGEnO1xuaW1wb3J0IHtDb21wb25lbnRGYWN0b3J5IGFzIENvbXBvbmVudEZhY3RvcnlSM30gZnJvbSAnLi4vcmVuZGVyMy9jb21wb25lbnRfcmVmJztcbmltcG9ydCB7Z2V0Q29tcG9uZW50RGVmLCBnZXROZ01vZHVsZURlZn0gZnJvbSAnLi4vcmVuZGVyMy9kZWZpbml0aW9uJztcbmltcG9ydCB7TmdNb2R1bGVGYWN0b3J5IGFzIE5nTW9kdWxlRmFjdG9yeVIzfSBmcm9tICcuLi9yZW5kZXIzL25nX21vZHVsZV9yZWYnO1xuaW1wb3J0IHttYXliZVVud3JhcEZufSBmcm9tICcuLi9yZW5kZXIzL3V0aWwvbWlzY191dGlscyc7XG5cbmltcG9ydCB7Q29tcG9uZW50RmFjdG9yeX0gZnJvbSAnLi9jb21wb25lbnRfZmFjdG9yeSc7XG5pbXBvcnQge05nTW9kdWxlRmFjdG9yeX0gZnJvbSAnLi9uZ19tb2R1bGVfZmFjdG9yeSc7XG5cblxuXG4vKipcbiAqIENvbWJpbmF0aW9uIG9mIE5nTW9kdWxlRmFjdG9yeSBhbmQgQ29tcG9uZW50RmFjdG9yeXMuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgTW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllczxUPiB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIG5nTW9kdWxlRmFjdG9yeTogTmdNb2R1bGVGYWN0b3J5PFQ+LFxuICAgICAgcHVibGljIGNvbXBvbmVudEZhY3RvcmllczogQ29tcG9uZW50RmFjdG9yeTxhbnk+W10pIHt9XG59XG5cblxuZnVuY3Rpb24gX3Rocm93RXJyb3IoKSB7XG4gIHRocm93IG5ldyBFcnJvcihgUnVudGltZSBjb21waWxlciBpcyBub3QgbG9hZGVkYCk7XG59XG5cbmNvbnN0IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVTeW5jX19QUkVfUjNfXzogPFQ+KG1vZHVsZVR5cGU6IFR5cGU8VD4pID0+IE5nTW9kdWxlRmFjdG9yeTxUPiA9XG4gICAgX3Rocm93RXJyb3IgYXMgYW55O1xuZXhwb3J0IGNvbnN0IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVTeW5jX19QT1NUX1IzX186IDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PlxuICAgIE5nTW9kdWxlRmFjdG9yeTxUPiA9IGZ1bmN0aW9uPFQ+KG1vZHVsZVR5cGU6IFR5cGU8VD4pOiBOZ01vZHVsZUZhY3Rvcnk8VD4ge1xuICByZXR1cm4gbmV3IE5nTW9kdWxlRmFjdG9yeVIzKG1vZHVsZVR5cGUpO1xufTtcbmNvbnN0IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVTeW5jID0gQ29tcGlsZXJfY29tcGlsZU1vZHVsZVN5bmNfX1BSRV9SM19fO1xuXG5jb25zdCBDb21waWxlcl9jb21waWxlTW9kdWxlQXN5bmNfX1BSRV9SM19fOiA8VD4obW9kdWxlVHlwZTogVHlwZTxUPikgPT5cbiAgICBQcm9taXNlPE5nTW9kdWxlRmFjdG9yeTxUPj4gPSBfdGhyb3dFcnJvciBhcyBhbnk7XG5leHBvcnQgY29uc3QgQ29tcGlsZXJfY29tcGlsZU1vZHVsZUFzeW5jX19QT1NUX1IzX186IDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PlxuICAgIFByb21pc2U8TmdNb2R1bGVGYWN0b3J5PFQ+PiA9IGZ1bmN0aW9uPFQ+KG1vZHVsZVR5cGU6IFR5cGU8VD4pOiBQcm9taXNlPE5nTW9kdWxlRmFjdG9yeTxUPj4ge1xuICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKENvbXBpbGVyX2NvbXBpbGVNb2R1bGVTeW5jX19QT1NUX1IzX18obW9kdWxlVHlwZSkpO1xufTtcbmNvbnN0IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVBc3luYyA9IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVBc3luY19fUFJFX1IzX187XG5cbmNvbnN0IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzU3luY19fUFJFX1IzX186IDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PlxuICAgIE1vZHVsZVdpdGhDb21wb25lbnRGYWN0b3JpZXM8VD4gPSBfdGhyb3dFcnJvciBhcyBhbnk7XG5leHBvcnQgY29uc3QgQ29tcGlsZXJfY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHNTeW5jX19QT1NUX1IzX186IDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PlxuICAgIE1vZHVsZVdpdGhDb21wb25lbnRGYWN0b3JpZXM8VD4gPSBmdW5jdGlvbjxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KTpcbiAgICAgICAgTW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllczxUPiB7XG4gIGNvbnN0IG5nTW9kdWxlRmFjdG9yeSA9IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVTeW5jX19QT1NUX1IzX18obW9kdWxlVHlwZSk7XG4gIGNvbnN0IG1vZHVsZURlZiA9IGdldE5nTW9kdWxlRGVmKG1vZHVsZVR5cGUpICE7XG4gIGNvbnN0IGNvbXBvbmVudEZhY3RvcmllcyA9XG4gICAgICBtYXliZVVud3JhcEZuKG1vZHVsZURlZi5kZWNsYXJhdGlvbnMpXG4gICAgICAgICAgLnJlZHVjZSgoZmFjdG9yaWVzOiBDb21wb25lbnRGYWN0b3J5PGFueT5bXSwgZGVjbGFyYXRpb246IFR5cGU8YW55PikgPT4ge1xuICAgICAgICAgICAgY29uc3QgY29tcG9uZW50RGVmID0gZ2V0Q29tcG9uZW50RGVmKGRlY2xhcmF0aW9uKTtcbiAgICAgICAgICAgIGNvbXBvbmVudERlZiAmJiBmYWN0b3JpZXMucHVzaChuZXcgQ29tcG9uZW50RmFjdG9yeVIzKGNvbXBvbmVudERlZikpO1xuICAgICAgICAgICAgcmV0dXJuIGZhY3RvcmllcztcbiAgICAgICAgICB9LCBbXSBhcyBDb21wb25lbnRGYWN0b3J5PGFueT5bXSk7XG4gIHJldHVybiBuZXcgTW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllcyhuZ01vZHVsZUZhY3RvcnksIGNvbXBvbmVudEZhY3Rvcmllcyk7XG59O1xuY29uc3QgQ29tcGlsZXJfY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHNTeW5jID1cbiAgICBDb21waWxlcl9jb21waWxlTW9kdWxlQW5kQWxsQ29tcG9uZW50c1N5bmNfX1BSRV9SM19fO1xuXG5jb25zdCBDb21waWxlcl9jb21waWxlTW9kdWxlQW5kQWxsQ29tcG9uZW50c0FzeW5jX19QUkVfUjNfXzogPFQ+KG1vZHVsZVR5cGU6IFR5cGU8VD4pID0+XG4gICAgUHJvbWlzZTxNb2R1bGVXaXRoQ29tcG9uZW50RmFjdG9yaWVzPFQ+PiA9IF90aHJvd0Vycm9yIGFzIGFueTtcbmV4cG9ydCBjb25zdCBDb21waWxlcl9jb21waWxlTW9kdWxlQW5kQWxsQ29tcG9uZW50c0FzeW5jX19QT1NUX1IzX186IDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PlxuICAgIFByb21pc2U8TW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllczxUPj4gPSBmdW5jdGlvbjxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KTpcbiAgICAgICAgUHJvbWlzZTxNb2R1bGVXaXRoQ29tcG9uZW50RmFjdG9yaWVzPFQ+PiB7XG4gIHJldHVybiBQcm9taXNlLnJlc29sdmUoQ29tcGlsZXJfY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHNTeW5jX19QT1NUX1IzX18obW9kdWxlVHlwZSkpO1xufTtcbmNvbnN0IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmMgPVxuICAgIENvbXBpbGVyX2NvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmNfX1BSRV9SM19fO1xuXG4vKipcbiAqIExvdy1sZXZlbCBzZXJ2aWNlIGZvciBydW5uaW5nIHRoZSBhbmd1bGFyIGNvbXBpbGVyIGR1cmluZyBydW50aW1lXG4gKiB0byBjcmVhdGUge0BsaW5rIENvbXBvbmVudEZhY3Rvcnl9cywgd2hpY2hcbiAqIGNhbiBsYXRlciBiZSB1c2VkIHRvIGNyZWF0ZSBhbmQgcmVuZGVyIGEgQ29tcG9uZW50IGluc3RhbmNlLlxuICpcbiAqIEVhY2ggYEBOZ01vZHVsZWAgcHJvdmlkZXMgYW4gb3duIGBDb21waWxlcmAgdG8gaXRzIGluamVjdG9yLFxuICogdGhhdCB3aWxsIHVzZSB0aGUgZGlyZWN0aXZlcy9waXBlcyBvZiB0aGUgbmcgbW9kdWxlIGZvciBjb21waWxhdGlvblxuICogb2YgY29tcG9uZW50cy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBDb21waWxlciB7XG4gIC8qKlxuICAgKiBDb21waWxlcyB0aGUgZ2l2ZW4gTmdNb2R1bGUgYW5kIGFsbCBvZiBpdHMgY29tcG9uZW50cy4gQWxsIHRlbXBsYXRlcyBvZiB0aGUgY29tcG9uZW50cyBsaXN0ZWRcbiAgICogaW4gYGVudHJ5Q29tcG9uZW50c2AgaGF2ZSB0byBiZSBpbmxpbmVkLlxuICAgKi9cbiAgY29tcGlsZU1vZHVsZVN5bmM6IDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PiBOZ01vZHVsZUZhY3Rvcnk8VD4gPSBDb21waWxlcl9jb21waWxlTW9kdWxlU3luYztcblxuICAvKipcbiAgICogQ29tcGlsZXMgdGhlIGdpdmVuIE5nTW9kdWxlIGFuZCBhbGwgb2YgaXRzIGNvbXBvbmVudHNcbiAgICovXG4gIGNvbXBpbGVNb2R1bGVBc3luYzpcbiAgICAgIDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PiBQcm9taXNlPE5nTW9kdWxlRmFjdG9yeTxUPj4gPSBDb21waWxlcl9jb21waWxlTW9kdWxlQXN5bmM7XG5cbiAgLyoqXG4gICAqIFNhbWUgYXMge0BsaW5rICNjb21waWxlTW9kdWxlU3luY30gYnV0IGFsc28gY3JlYXRlcyBDb21wb25lbnRGYWN0b3JpZXMgZm9yIGFsbCBjb21wb25lbnRzLlxuICAgKi9cbiAgY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHNTeW5jOiA8VD4obW9kdWxlVHlwZTogVHlwZTxUPikgPT4gTW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllczxUPiA9XG4gICAgICBDb21waWxlcl9jb21waWxlTW9kdWxlQW5kQWxsQ29tcG9uZW50c1N5bmM7XG5cbiAgLyoqXG4gICAqIFNhbWUgYXMge0BsaW5rICNjb21waWxlTW9kdWxlQXN5bmN9IGJ1dCBhbHNvIGNyZWF0ZXMgQ29tcG9uZW50RmFjdG9yaWVzIGZvciBhbGwgY29tcG9uZW50cy5cbiAgICovXG4gIGNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmM6IDxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KSA9PlxuICAgICAgUHJvbWlzZTxNb2R1bGVXaXRoQ29tcG9uZW50RmFjdG9yaWVzPFQ+PiA9IENvbXBpbGVyX2NvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmM7XG5cbiAgLyoqXG4gICAqIENsZWFycyBhbGwgY2FjaGVzLlxuICAgKi9cbiAgY2xlYXJDYWNoZSgpOiB2b2lkIHt9XG5cbiAgLyoqXG4gICAqIENsZWFycyB0aGUgY2FjaGUgZm9yIHRoZSBnaXZlbiBjb21wb25lbnQvbmdNb2R1bGUuXG4gICAqL1xuICBjbGVhckNhY2hlRm9yKHR5cGU6IFR5cGU8YW55Pikge31cblxuICAvKipcbiAgICogUmV0dXJucyB0aGUgaWQgZm9yIGEgZ2l2ZW4gTmdNb2R1bGUsIGlmIG9uZSBpcyBkZWZpbmVkIGFuZCBrbm93biB0byB0aGUgY29tcGlsZXIuXG4gICAqL1xuICBnZXRNb2R1bGVJZChtb2R1bGVUeXBlOiBUeXBlPGFueT4pOiBzdHJpbmd8dW5kZWZpbmVkIHsgcmV0dXJuIHVuZGVmaW5lZDsgfVxufVxuXG4vKipcbiAqIE9wdGlvbnMgZm9yIGNyZWF0aW5nIGEgY29tcGlsZXJcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCB0eXBlIENvbXBpbGVyT3B0aW9ucyA9IHtcbiAgdXNlSml0PzogYm9vbGVhbixcbiAgZGVmYXVsdEVuY2Fwc3VsYXRpb24/OiBWaWV3RW5jYXBzdWxhdGlvbixcbiAgcHJvdmlkZXJzPzogU3RhdGljUHJvdmlkZXJbXSxcbiAgbWlzc2luZ1RyYW5zbGF0aW9uPzogTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3ksXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM/OiBib29sZWFuLFxufTtcblxuLyoqXG4gKiBUb2tlbiB0byBwcm92aWRlIENvbXBpbGVyT3B0aW9ucyBpbiB0aGUgcGxhdGZvcm0gaW5qZWN0b3IuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY29uc3QgQ09NUElMRVJfT1BUSU9OUyA9IG5ldyBJbmplY3Rpb25Ub2tlbjxDb21waWxlck9wdGlvbnNbXT4oJ2NvbXBpbGVyT3B0aW9ucycpO1xuXG4vKipcbiAqIEEgZmFjdG9yeSBmb3IgY3JlYXRpbmcgYSBDb21waWxlclxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGFic3RyYWN0IGNsYXNzIENvbXBpbGVyRmFjdG9yeSB7XG4gIGFic3RyYWN0IGNyZWF0ZUNvbXBpbGVyKG9wdGlvbnM/OiBDb21waWxlck9wdGlvbnNbXSk6IENvbXBpbGVyO1xufVxuIl19