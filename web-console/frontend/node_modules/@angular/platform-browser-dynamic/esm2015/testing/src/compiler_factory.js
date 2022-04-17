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
import { CompileReflector, DirectiveResolver, ERROR_COMPONENT_TYPE, NgModuleResolver, PipeResolver } from '@angular/compiler';
import { MockDirectiveResolver, MockNgModuleResolver, MockPipeResolver } from '@angular/compiler/testing';
import { Component, Directive, NgModule, Pipe, ɵstringify as stringify } from '@angular/core';
import { MetadataOverrider } from './metadata_overrider';
/** @type {?} */
export const COMPILER_PROVIDERS = [
    { provide: MockPipeResolver, deps: [CompileReflector] },
    { provide: PipeResolver, useExisting: MockPipeResolver },
    { provide: MockDirectiveResolver, deps: [CompileReflector] },
    { provide: DirectiveResolver, useExisting: MockDirectiveResolver },
    { provide: MockNgModuleResolver, deps: [CompileReflector] },
    { provide: NgModuleResolver, useExisting: MockNgModuleResolver },
];
export class TestingCompilerFactoryImpl {
    /**
     * @param {?} _injector
     * @param {?} _compilerFactory
     */
    constructor(_injector, _compilerFactory) {
        this._injector = _injector;
        this._compilerFactory = _compilerFactory;
    }
    /**
     * @param {?} options
     * @return {?}
     */
    createTestingCompiler(options) {
        /** @type {?} */
        const compiler = (/** @type {?} */ (this._compilerFactory.createCompiler(options)));
        return new TestingCompilerImpl(compiler, compiler.injector.get(MockDirectiveResolver), compiler.injector.get(MockPipeResolver), compiler.injector.get(MockNgModuleResolver));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    TestingCompilerFactoryImpl.prototype._injector;
    /**
     * @type {?}
     * @private
     */
    TestingCompilerFactoryImpl.prototype._compilerFactory;
}
export class TestingCompilerImpl {
    /**
     * @param {?} _compiler
     * @param {?} _directiveResolver
     * @param {?} _pipeResolver
     * @param {?} _moduleResolver
     */
    constructor(_compiler, _directiveResolver, _pipeResolver, _moduleResolver) {
        this._compiler = _compiler;
        this._directiveResolver = _directiveResolver;
        this._pipeResolver = _pipeResolver;
        this._moduleResolver = _moduleResolver;
        this._overrider = new MetadataOverrider();
    }
    /**
     * @return {?}
     */
    get injector() { return this._compiler.injector; }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleSync(moduleType) {
        return this._compiler.compileModuleSync(moduleType);
    }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleAsync(moduleType) {
        return this._compiler.compileModuleAsync(moduleType);
    }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleAndAllComponentsSync(moduleType) {
        return this._compiler.compileModuleAndAllComponentsSync(moduleType);
    }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleAndAllComponentsAsync(moduleType) {
        return this._compiler.compileModuleAndAllComponentsAsync(moduleType);
    }
    /**
     * @template T
     * @param {?} component
     * @return {?}
     */
    getComponentFactory(component) {
        return this._compiler.getComponentFactory(component);
    }
    /**
     * @param {?} type
     * @return {?}
     */
    checkOverrideAllowed(type) {
        if (this._compiler.hasAotSummary(type)) {
            throw new Error(`${stringify(type)} was AOT compiled, so its metadata cannot be changed.`);
        }
    }
    /**
     * @param {?} ngModule
     * @param {?} override
     * @return {?}
     */
    overrideModule(ngModule, override) {
        this.checkOverrideAllowed(ngModule);
        /** @type {?} */
        const oldMetadata = this._moduleResolver.resolve(ngModule, false);
        this._moduleResolver.setNgModule(ngModule, this._overrider.overrideMetadata(NgModule, oldMetadata, override));
        this.clearCacheFor(ngModule);
    }
    /**
     * @param {?} directive
     * @param {?} override
     * @return {?}
     */
    overrideDirective(directive, override) {
        this.checkOverrideAllowed(directive);
        /** @type {?} */
        const oldMetadata = this._directiveResolver.resolve(directive, false);
        this._directiveResolver.setDirective(directive, this._overrider.overrideMetadata(Directive, (/** @type {?} */ (oldMetadata)), override));
        this.clearCacheFor(directive);
    }
    /**
     * @param {?} component
     * @param {?} override
     * @return {?}
     */
    overrideComponent(component, override) {
        this.checkOverrideAllowed(component);
        /** @type {?} */
        const oldMetadata = this._directiveResolver.resolve(component, false);
        this._directiveResolver.setDirective(component, this._overrider.overrideMetadata(Component, (/** @type {?} */ (oldMetadata)), override));
        this.clearCacheFor(component);
    }
    /**
     * @param {?} pipe
     * @param {?} override
     * @return {?}
     */
    overridePipe(pipe, override) {
        this.checkOverrideAllowed(pipe);
        /** @type {?} */
        const oldMetadata = this._pipeResolver.resolve(pipe, false);
        this._pipeResolver.setPipe(pipe, this._overrider.overrideMetadata(Pipe, oldMetadata, override));
        this.clearCacheFor(pipe);
    }
    /**
     * @param {?} summaries
     * @return {?}
     */
    loadAotSummaries(summaries) { this._compiler.loadAotSummaries(summaries); }
    /**
     * @return {?}
     */
    clearCache() { this._compiler.clearCache(); }
    /**
     * @param {?} type
     * @return {?}
     */
    clearCacheFor(type) { this._compiler.clearCacheFor(type); }
    /**
     * @param {?} error
     * @return {?}
     */
    getComponentFromError(error) { return ((/** @type {?} */ (error)))[ERROR_COMPONENT_TYPE] || null; }
    /**
     * @param {?} moduleType
     * @return {?}
     */
    getModuleId(moduleType) {
        return this._moduleResolver.resolve(moduleType, true).id;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    TestingCompilerImpl.prototype._overrider;
    /**
     * @type {?}
     * @private
     */
    TestingCompilerImpl.prototype._compiler;
    /**
     * @type {?}
     * @private
     */
    TestingCompilerImpl.prototype._directiveResolver;
    /**
     * @type {?}
     * @private
     */
    TestingCompilerImpl.prototype._pipeResolver;
    /**
     * @type {?}
     * @private
     */
    TestingCompilerImpl.prototype._moduleResolver;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZXJfZmFjdG9yeS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXItZHluYW1pYy90ZXN0aW5nL3NyYy9jb21waWxlcl9mYWN0b3J5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGdCQUFnQixFQUFFLGlCQUFpQixFQUFFLG9CQUFvQixFQUFFLGdCQUFnQixFQUFFLFlBQVksRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBQzVILE9BQU8sRUFBQyxxQkFBcUIsRUFBRSxvQkFBb0IsRUFBRSxnQkFBZ0IsRUFBQyxNQUFNLDJCQUEyQixDQUFDO0FBQ3hHLE9BQU8sRUFBbUMsU0FBUyxFQUFvQixTQUFTLEVBQTBDLFFBQVEsRUFBbUIsSUFBSSxFQUF3QixVQUFVLElBQUksU0FBUyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBSS9OLE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLHNCQUFzQixDQUFDOztBQUV2RCxNQUFNLE9BQU8sa0JBQWtCLEdBQXFCO0lBQ2xELEVBQUMsT0FBTyxFQUFFLGdCQUFnQixFQUFFLElBQUksRUFBRSxDQUFDLGdCQUFnQixDQUFDLEVBQUM7SUFDckQsRUFBQyxPQUFPLEVBQUUsWUFBWSxFQUFFLFdBQVcsRUFBRSxnQkFBZ0IsRUFBQztJQUN0RCxFQUFDLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxJQUFJLEVBQUUsQ0FBQyxnQkFBZ0IsQ0FBQyxFQUFDO0lBQzFELEVBQUMsT0FBTyxFQUFFLGlCQUFpQixFQUFFLFdBQVcsRUFBRSxxQkFBcUIsRUFBQztJQUNoRSxFQUFDLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxJQUFJLEVBQUUsQ0FBQyxnQkFBZ0IsQ0FBQyxFQUFDO0lBQ3pELEVBQUMsT0FBTyxFQUFFLGdCQUFnQixFQUFFLFdBQVcsRUFBRSxvQkFBb0IsRUFBQztDQUMvRDtBQUVELE1BQU0sT0FB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SUFDM0IsQ0FBQzs7Ozs7SUFDRCxnQkFBZ0IsQ0FBQyxTQUFzQixJQUFJLElBQUksQ0FBQyxTQUFTLENBQUMsZ0JBQWdCLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7O0lBQ3hGLFVBQVUsS0FBVyxJQUFJLENBQUMsU0FBUyxDQUFDLFVBQVUsRUFBRSxDQUFDLENBQUMsQ0FBQzs7Ozs7SUFDbkQsYUFBYSxDQUFDLElBQWUsSUFBSSxJQUFJLENBQUMsU0FBUyxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7O0lBRXRFLHFCQUFxQixDQUFDLEtBQVksSUFBSSxPQUFPLENBQUMsbUJBQUEsS0FBSyxFQUFPLENBQUMsQ0FBQyxvQkFBb0IsQ0FBQyxJQUFJLElBQUksQ0FBQyxDQUFDLENBQUM7Ozs7O0lBRTVGLFdBQVcsQ0FBQyxVQUFxQjtRQUMvQixPQUFPLElBQUksQ0FBQyxlQUFlLENBQUMsT0FBTyxDQUFDLFVBQVUsRUFBRSxJQUFJLENBQUMsQ0FBQyxFQUFFLENBQUM7SUFDM0QsQ0FBQztDQUNGOzs7Ozs7SUFwRUMseUNBQTZDOzs7OztJQUV6Qyx3Q0FBK0I7Ozs7O0lBQUUsaURBQWlEOzs7OztJQUNsRiw0Q0FBdUM7Ozs7O0lBQUUsOENBQTZDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVSZWZsZWN0b3IsIERpcmVjdGl2ZVJlc29sdmVyLCBFUlJPUl9DT01QT05FTlRfVFlQRSwgTmdNb2R1bGVSZXNvbHZlciwgUGlwZVJlc29sdmVyfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5pbXBvcnQge01vY2tEaXJlY3RpdmVSZXNvbHZlciwgTW9ja05nTW9kdWxlUmVzb2x2ZXIsIE1vY2tQaXBlUmVzb2x2ZXJ9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyL3Rlc3RpbmcnO1xuaW1wb3J0IHtDb21waWxlckZhY3RvcnksIENvbXBpbGVyT3B0aW9ucywgQ29tcG9uZW50LCBDb21wb25lbnRGYWN0b3J5LCBEaXJlY3RpdmUsIEluamVjdG9yLCBNb2R1bGVXaXRoQ29tcG9uZW50RmFjdG9yaWVzLCBOZ01vZHVsZSwgTmdNb2R1bGVGYWN0b3J5LCBQaXBlLCBTdGF0aWNQcm92aWRlciwgVHlwZSwgybVzdHJpbmdpZnkgYXMgc3RyaW5naWZ5fSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7TWV0YWRhdGFPdmVycmlkZSwgybVUZXN0aW5nQ29tcGlsZXIgYXMgVGVzdGluZ0NvbXBpbGVyLCDJtVRlc3RpbmdDb21waWxlckZhY3RvcnkgYXMgVGVzdGluZ0NvbXBpbGVyRmFjdG9yeX0gZnJvbSAnQGFuZ3VsYXIvY29yZS90ZXN0aW5nJztcbmltcG9ydCB7ybVDb21waWxlckltcGwgYXMgQ29tcGlsZXJJbXBsfSBmcm9tICdAYW5ndWxhci9wbGF0Zm9ybS1icm93c2VyLWR5bmFtaWMnO1xuXG5pbXBvcnQge01ldGFkYXRhT3ZlcnJpZGVyfSBmcm9tICcuL21ldGFkYXRhX292ZXJyaWRlcic7XG5cbmV4cG9ydCBjb25zdCBDT01QSUxFUl9QUk9WSURFUlM6IFN0YXRpY1Byb3ZpZGVyW10gPSBbXG4gIHtwcm92aWRlOiBNb2NrUGlwZVJlc29sdmVyLCBkZXBzOiBbQ29tcGlsZVJlZmxlY3Rvcl19LFxuICB7cHJvdmlkZTogUGlwZVJlc29sdmVyLCB1c2VFeGlzdGluZzogTW9ja1BpcGVSZXNvbHZlcn0sXG4gIHtwcm92aWRlOiBNb2NrRGlyZWN0aXZlUmVzb2x2ZXIsIGRlcHM6IFtDb21waWxlUmVmbGVjdG9yXX0sXG4gIHtwcm92aWRlOiBEaXJlY3RpdmVSZXNvbHZlciwgdXNlRXhpc3Rpbmc6IE1vY2tEaXJlY3RpdmVSZXNvbHZlcn0sXG4gIHtwcm92aWRlOiBNb2NrTmdNb2R1bGVSZXNvbHZlciwgZGVwczogW0NvbXBpbGVSZWZsZWN0b3JdfSxcbiAge3Byb3ZpZGU6IE5nTW9kdWxlUmVzb2x2ZXIsIHVzZUV4aXN0aW5nOiBNb2NrTmdNb2R1bGVSZXNvbHZlcn0sXG5dO1xuXG5leHBvcnQgY2xhc3MgVGVzdGluZ0NvbXBpbGVyRmFjdG9yeUltcGwgaW1wbGVtZW50cyBUZXN0aW5nQ29tcGlsZXJGYWN0b3J5IHtcbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfaW5qZWN0b3I6IEluamVjdG9yLCBwcml2YXRlIF9jb21waWxlckZhY3Rvcnk6IENvbXBpbGVyRmFjdG9yeSkge31cblxuICBjcmVhdGVUZXN0aW5nQ29tcGlsZXIob3B0aW9uczogQ29tcGlsZXJPcHRpb25zW10pOiBUZXN0aW5nQ29tcGlsZXIge1xuICAgIGNvbnN0IGNvbXBpbGVyID0gPENvbXBpbGVySW1wbD50aGlzLl9jb21waWxlckZhY3RvcnkuY3JlYXRlQ29tcGlsZXIob3B0aW9ucyk7XG4gICAgcmV0dXJuIG5ldyBUZXN0aW5nQ29tcGlsZXJJbXBsKFxuICAgICAgICBjb21waWxlciwgY29tcGlsZXIuaW5qZWN0b3IuZ2V0KE1vY2tEaXJlY3RpdmVSZXNvbHZlciksXG4gICAgICAgIGNvbXBpbGVyLmluamVjdG9yLmdldChNb2NrUGlwZVJlc29sdmVyKSwgY29tcGlsZXIuaW5qZWN0b3IuZ2V0KE1vY2tOZ01vZHVsZVJlc29sdmVyKSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFRlc3RpbmdDb21waWxlckltcGwgaW1wbGVtZW50cyBUZXN0aW5nQ29tcGlsZXIge1xuICBwcml2YXRlIF9vdmVycmlkZXIgPSBuZXcgTWV0YWRhdGFPdmVycmlkZXIoKTtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9jb21waWxlcjogQ29tcGlsZXJJbXBsLCBwcml2YXRlIF9kaXJlY3RpdmVSZXNvbHZlcjogTW9ja0RpcmVjdGl2ZVJlc29sdmVyLFxuICAgICAgcHJpdmF0ZSBfcGlwZVJlc29sdmVyOiBNb2NrUGlwZVJlc29sdmVyLCBwcml2YXRlIF9tb2R1bGVSZXNvbHZlcjogTW9ja05nTW9kdWxlUmVzb2x2ZXIpIHt9XG4gIGdldCBpbmplY3RvcigpOiBJbmplY3RvciB7IHJldHVybiB0aGlzLl9jb21waWxlci5pbmplY3RvcjsgfVxuXG4gIGNvbXBpbGVNb2R1bGVTeW5jPFQ+KG1vZHVsZVR5cGU6IFR5cGU8VD4pOiBOZ01vZHVsZUZhY3Rvcnk8VD4ge1xuICAgIHJldHVybiB0aGlzLl9jb21waWxlci5jb21waWxlTW9kdWxlU3luYyhtb2R1bGVUeXBlKTtcbiAgfVxuXG4gIGNvbXBpbGVNb2R1bGVBc3luYzxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KTogUHJvbWlzZTxOZ01vZHVsZUZhY3Rvcnk8VD4+IHtcbiAgICByZXR1cm4gdGhpcy5fY29tcGlsZXIuY29tcGlsZU1vZHVsZUFzeW5jKG1vZHVsZVR5cGUpO1xuICB9XG4gIGNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzU3luYzxUPihtb2R1bGVUeXBlOiBUeXBlPFQ+KTogTW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllczxUPiB7XG4gICAgcmV0dXJuIHRoaXMuX2NvbXBpbGVyLmNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzU3luYyhtb2R1bGVUeXBlKTtcbiAgfVxuXG4gIGNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmM8VD4obW9kdWxlVHlwZTogVHlwZTxUPik6XG4gICAgICBQcm9taXNlPE1vZHVsZVdpdGhDb21wb25lbnRGYWN0b3JpZXM8VD4+IHtcbiAgICByZXR1cm4gdGhpcy5fY29tcGlsZXIuY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHNBc3luYyhtb2R1bGVUeXBlKTtcbiAgfVxuXG4gIGdldENvbXBvbmVudEZhY3Rvcnk8VD4oY29tcG9uZW50OiBUeXBlPFQ+KTogQ29tcG9uZW50RmFjdG9yeTxUPiB7XG4gICAgcmV0dXJuIHRoaXMuX2NvbXBpbGVyLmdldENvbXBvbmVudEZhY3RvcnkoY29tcG9uZW50KTtcbiAgfVxuXG4gIGNoZWNrT3ZlcnJpZGVBbGxvd2VkKHR5cGU6IFR5cGU8YW55Pikge1xuICAgIGlmICh0aGlzLl9jb21waWxlci5oYXNBb3RTdW1tYXJ5KHR5cGUpKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYCR7c3RyaW5naWZ5KHR5cGUpfSB3YXMgQU9UIGNvbXBpbGVkLCBzbyBpdHMgbWV0YWRhdGEgY2Fubm90IGJlIGNoYW5nZWQuYCk7XG4gICAgfVxuICB9XG5cbiAgb3ZlcnJpZGVNb2R1bGUobmdNb2R1bGU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8TmdNb2R1bGU+KTogdm9pZCB7XG4gICAgdGhpcy5jaGVja092ZXJyaWRlQWxsb3dlZChuZ01vZHVsZSk7XG4gICAgY29uc3Qgb2xkTWV0YWRhdGEgPSB0aGlzLl9tb2R1bGVSZXNvbHZlci5yZXNvbHZlKG5nTW9kdWxlLCBmYWxzZSk7XG4gICAgdGhpcy5fbW9kdWxlUmVzb2x2ZXIuc2V0TmdNb2R1bGUoXG4gICAgICAgIG5nTW9kdWxlLCB0aGlzLl9vdmVycmlkZXIub3ZlcnJpZGVNZXRhZGF0YShOZ01vZHVsZSwgb2xkTWV0YWRhdGEsIG92ZXJyaWRlKSk7XG4gICAgdGhpcy5jbGVhckNhY2hlRm9yKG5nTW9kdWxlKTtcbiAgfVxuICBvdmVycmlkZURpcmVjdGl2ZShkaXJlY3RpdmU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8RGlyZWN0aXZlPik6IHZvaWQge1xuICAgIHRoaXMuY2hlY2tPdmVycmlkZUFsbG93ZWQoZGlyZWN0aXZlKTtcbiAgICBjb25zdCBvbGRNZXRhZGF0YSA9IHRoaXMuX2RpcmVjdGl2ZVJlc29sdmVyLnJlc29sdmUoZGlyZWN0aXZlLCBmYWxzZSk7XG4gICAgdGhpcy5fZGlyZWN0aXZlUmVzb2x2ZXIuc2V0RGlyZWN0aXZlKFxuICAgICAgICBkaXJlY3RpdmUsIHRoaXMuX292ZXJyaWRlci5vdmVycmlkZU1ldGFkYXRhKERpcmVjdGl2ZSwgb2xkTWV0YWRhdGEgISwgb3ZlcnJpZGUpKTtcbiAgICB0aGlzLmNsZWFyQ2FjaGVGb3IoZGlyZWN0aXZlKTtcbiAgfVxuICBvdmVycmlkZUNvbXBvbmVudChjb21wb25lbnQ6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8Q29tcG9uZW50Pik6IHZvaWQge1xuICAgIHRoaXMuY2hlY2tPdmVycmlkZUFsbG93ZWQoY29tcG9uZW50KTtcbiAgICBjb25zdCBvbGRNZXRhZGF0YSA9IHRoaXMuX2RpcmVjdGl2ZVJlc29sdmVyLnJlc29sdmUoY29tcG9uZW50LCBmYWxzZSk7XG4gICAgdGhpcy5fZGlyZWN0aXZlUmVzb2x2ZXIuc2V0RGlyZWN0aXZlKFxuICAgICAgICBjb21wb25lbnQsIHRoaXMuX292ZXJyaWRlci5vdmVycmlkZU1ldGFkYXRhKENvbXBvbmVudCwgb2xkTWV0YWRhdGEgISwgb3ZlcnJpZGUpKTtcbiAgICB0aGlzLmNsZWFyQ2FjaGVGb3IoY29tcG9uZW50KTtcbiAgfVxuICBvdmVycmlkZVBpcGUocGlwZTogVHlwZTxhbnk+LCBvdmVycmlkZTogTWV0YWRhdGFPdmVycmlkZTxQaXBlPik6IHZvaWQge1xuICAgIHRoaXMuY2hlY2tPdmVycmlkZUFsbG93ZWQocGlwZSk7XG4gICAgY29uc3Qgb2xkTWV0YWRhdGEgPSB0aGlzLl9waXBlUmVzb2x2ZXIucmVzb2x2ZShwaXBlLCBmYWxzZSk7XG4gICAgdGhpcy5fcGlwZVJlc29sdmVyLnNldFBpcGUocGlwZSwgdGhpcy5fb3ZlcnJpZGVyLm92ZXJyaWRlTWV0YWRhdGEoUGlwZSwgb2xkTWV0YWRhdGEsIG92ZXJyaWRlKSk7XG4gICAgdGhpcy5jbGVhckNhY2hlRm9yKHBpcGUpO1xuICB9XG4gIGxvYWRBb3RTdW1tYXJpZXMoc3VtbWFyaWVzOiAoKSA9PiBhbnlbXSkgeyB0aGlzLl9jb21waWxlci5sb2FkQW90U3VtbWFyaWVzKHN1bW1hcmllcyk7IH1cbiAgY2xlYXJDYWNoZSgpOiB2b2lkIHsgdGhpcy5fY29tcGlsZXIuY2xlYXJDYWNoZSgpOyB9XG4gIGNsZWFyQ2FjaGVGb3IodHlwZTogVHlwZTxhbnk+KSB7IHRoaXMuX2NvbXBpbGVyLmNsZWFyQ2FjaGVGb3IodHlwZSk7IH1cblxuICBnZXRDb21wb25lbnRGcm9tRXJyb3IoZXJyb3I6IEVycm9yKSB7IHJldHVybiAoZXJyb3IgYXMgYW55KVtFUlJPUl9DT01QT05FTlRfVFlQRV0gfHwgbnVsbDsgfVxuXG4gIGdldE1vZHVsZUlkKG1vZHVsZVR5cGU6IFR5cGU8YW55Pik6IHN0cmluZ3x1bmRlZmluZWQge1xuICAgIHJldHVybiB0aGlzLl9tb2R1bGVSZXNvbHZlci5yZXNvbHZlKG1vZHVsZVR5cGUsIHRydWUpLmlkO1xuICB9XG59XG4iXX0=