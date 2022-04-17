/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
// The formatter and CI disagree on how this import statement should be formatted. Both try to keep
// it on one line, too, which has gotten very hard to read & manage. So disable the formatter for
// this statement only.
// clang-format off
import { InjectFlags, Injector, NgZone, ɵRender3ComponentFactory as ComponentFactory, ɵflushModuleScopingQueueAsMuchAsPossible as flushModuleScopingQueueAsMuchAsPossible, ɵresetCompiledComponents as resetCompiledComponents, ɵstringify as stringify, } from '@angular/core';
// clang-format on
import { ComponentFixture } from './component_fixture';
import { ComponentFixtureAutoDetect, ComponentFixtureNoNgZone, TestComponentRenderer } from './test_bed_common';
import { R3TestBedCompiler } from './r3_test_bed_compiler';
var _nextRootElementId = 0;
var UNDEFINED = Symbol('UNDEFINED');
/**
 * @description
 * Configures and initializes environment for unit testing and provides methods for
 * creating components and services in unit tests.
 *
 * TestBed is the primary api for writing unit tests for Angular applications and libraries.
 *
 * Note: Use `TestBed` in tests. It will be set to either `TestBedViewEngine` or `TestBedRender3`
 * according to the compiler used.
 */
var TestBedRender3 = /** @class */ (function () {
    function TestBedRender3() {
        // Properties
        this.platform = null;
        this.ngModule = null;
        this._compiler = null;
        this._testModuleRef = null;
        this._activeFixtures = [];
        this._globalCompilationChecked = false;
    }
    /**
     * Initialize the environment for testing with a compiler factory, a PlatformRef, and an
     * angular module. These are common to every test in the suite.
     *
     * This may only be called once, to set up the common providers for the current test
     * suite on the current platform. If you absolutely need to change the providers,
     * first use `resetTestEnvironment`.
     *
     * Test modules and platforms for individual platforms are available from
     * '@angular/<platform_name>/testing'.
     *
     * @publicApi
     */
    TestBedRender3.initTestEnvironment = function (ngModule, platform, aotSummaries) {
        var testBed = _getTestBedRender3();
        testBed.initTestEnvironment(ngModule, platform, aotSummaries);
        return testBed;
    };
    /**
     * Reset the providers for the test injector.
     *
     * @publicApi
     */
    TestBedRender3.resetTestEnvironment = function () { _getTestBedRender3().resetTestEnvironment(); };
    TestBedRender3.configureCompiler = function (config) {
        _getTestBedRender3().configureCompiler(config);
        return TestBedRender3;
    };
    /**
     * Allows overriding default providers, directives, pipes, modules of the test injector,
     * which are defined in test_injector.js
     */
    TestBedRender3.configureTestingModule = function (moduleDef) {
        _getTestBedRender3().configureTestingModule(moduleDef);
        return TestBedRender3;
    };
    /**
     * Compile components with a `templateUrl` for the test's NgModule.
     * It is necessary to call this function
     * as fetching urls is asynchronous.
     */
    TestBedRender3.compileComponents = function () { return _getTestBedRender3().compileComponents(); };
    TestBedRender3.overrideModule = function (ngModule, override) {
        _getTestBedRender3().overrideModule(ngModule, override);
        return TestBedRender3;
    };
    TestBedRender3.overrideComponent = function (component, override) {
        _getTestBedRender3().overrideComponent(component, override);
        return TestBedRender3;
    };
    TestBedRender3.overrideDirective = function (directive, override) {
        _getTestBedRender3().overrideDirective(directive, override);
        return TestBedRender3;
    };
    TestBedRender3.overridePipe = function (pipe, override) {
        _getTestBedRender3().overridePipe(pipe, override);
        return TestBedRender3;
    };
    TestBedRender3.overrideTemplate = function (component, template) {
        _getTestBedRender3().overrideComponent(component, { set: { template: template, templateUrl: null } });
        return TestBedRender3;
    };
    /**
     * Overrides the template of the given component, compiling the template
     * in the context of the TestingModule.
     *
     * Note: This works for JIT and AOTed components as well.
     */
    TestBedRender3.overrideTemplateUsingTestingModule = function (component, template) {
        _getTestBedRender3().overrideTemplateUsingTestingModule(component, template);
        return TestBedRender3;
    };
    TestBedRender3.overrideProvider = function (token, provider) {
        _getTestBedRender3().overrideProvider(token, provider);
        return TestBedRender3;
    };
    TestBedRender3.get = function (token, notFoundValue, flags) {
        if (notFoundValue === void 0) { notFoundValue = Injector.THROW_IF_NOT_FOUND; }
        if (flags === void 0) { flags = InjectFlags.Default; }
        return _getTestBedRender3().get(token, notFoundValue);
    };
    TestBedRender3.createComponent = function (component) {
        return _getTestBedRender3().createComponent(component);
    };
    TestBedRender3.resetTestingModule = function () {
        _getTestBedRender3().resetTestingModule();
        return TestBedRender3;
    };
    /**
     * Initialize the environment for testing with a compiler factory, a PlatformRef, and an
     * angular module. These are common to every test in the suite.
     *
     * This may only be called once, to set up the common providers for the current test
     * suite on the current platform. If you absolutely need to change the providers,
     * first use `resetTestEnvironment`.
     *
     * Test modules and platforms for individual platforms are available from
     * '@angular/<platform_name>/testing'.
     *
     * @publicApi
     */
    TestBedRender3.prototype.initTestEnvironment = function (ngModule, platform, aotSummaries) {
        if (this.platform || this.ngModule) {
            throw new Error('Cannot set base providers because it has already been called');
        }
        this.platform = platform;
        this.ngModule = ngModule;
        this._compiler = new R3TestBedCompiler(this.platform, this.ngModule);
    };
    /**
     * Reset the providers for the test injector.
     *
     * @publicApi
     */
    TestBedRender3.prototype.resetTestEnvironment = function () {
        this.resetTestingModule();
        this._compiler = null;
        this.platform = null;
        this.ngModule = null;
    };
    TestBedRender3.prototype.resetTestingModule = function () {
        this.checkGlobalCompilationFinished();
        resetCompiledComponents();
        if (this._compiler !== null) {
            this.compiler.restoreOriginalState();
        }
        this._compiler = new R3TestBedCompiler(this.platform, this.ngModule);
        this._testModuleRef = null;
        this.destroyActiveFixtures();
    };
    TestBedRender3.prototype.configureCompiler = function (config) {
        if (config.useJit != null) {
            throw new Error('the Render3 compiler JiT mode is not configurable !');
        }
        if (config.providers !== undefined) {
            this.compiler.setCompilerProviders(config.providers);
        }
    };
    TestBedRender3.prototype.configureTestingModule = function (moduleDef) {
        this.assertNotInstantiated('R3TestBed.configureTestingModule', 'configure the test module');
        this.compiler.configureTestingModule(moduleDef);
    };
    TestBedRender3.prototype.compileComponents = function () { return this.compiler.compileComponents(); };
    TestBedRender3.prototype.get = function (token, notFoundValue, flags) {
        if (notFoundValue === void 0) { notFoundValue = Injector.THROW_IF_NOT_FOUND; }
        if (flags === void 0) { flags = InjectFlags.Default; }
        if (token === TestBedRender3) {
            return this;
        }
        var result = this.testModuleRef.injector.get(token, UNDEFINED, flags);
        return result === UNDEFINED ? this.compiler.injector.get(token, notFoundValue, flags) : result;
    };
    TestBedRender3.prototype.execute = function (tokens, fn, context) {
        var _this = this;
        var params = tokens.map(function (t) { return _this.get(t); });
        return fn.apply(context, params);
    };
    TestBedRender3.prototype.overrideModule = function (ngModule, override) {
        this.assertNotInstantiated('overrideModule', 'override module metadata');
        this.compiler.overrideModule(ngModule, override);
    };
    TestBedRender3.prototype.overrideComponent = function (component, override) {
        this.assertNotInstantiated('overrideComponent', 'override component metadata');
        this.compiler.overrideComponent(component, override);
    };
    TestBedRender3.prototype.overrideTemplateUsingTestingModule = function (component, template) {
        this.assertNotInstantiated('R3TestBed.overrideTemplateUsingTestingModule', 'Cannot override template when the test module has already been instantiated');
        this.compiler.overrideTemplateUsingTestingModule(component, template);
    };
    TestBedRender3.prototype.overrideDirective = function (directive, override) {
        this.assertNotInstantiated('overrideDirective', 'override directive metadata');
        this.compiler.overrideDirective(directive, override);
    };
    TestBedRender3.prototype.overridePipe = function (pipe, override) {
        this.assertNotInstantiated('overridePipe', 'override pipe metadata');
        this.compiler.overridePipe(pipe, override);
    };
    /**
     * Overwrites all providers for the given token with the given provider definition.
     */
    TestBedRender3.prototype.overrideProvider = function (token, provider) {
        this.compiler.overrideProvider(token, provider);
    };
    TestBedRender3.prototype.createComponent = function (type) {
        var _this = this;
        var testComponentRenderer = this.get(TestComponentRenderer);
        var rootElId = "root-ng-internal-isolated-" + _nextRootElementId++;
        testComponentRenderer.insertRootElement(rootElId);
        var componentDef = type.ngComponentDef;
        if (!componentDef) {
            throw new Error("It looks like '" + stringify(type) + "' has not been IVY compiled - it has no 'ngComponentDef' field");
        }
        // TODO: Don't cast as `any`, proper type is boolean[]
        var noNgZone = this.get(ComponentFixtureNoNgZone, false);
        // TODO: Don't cast as `any`, proper type is boolean[]
        var autoDetect = this.get(ComponentFixtureAutoDetect, false);
        var ngZone = noNgZone ? null : this.get(NgZone, null);
        var componentFactory = new ComponentFactory(componentDef);
        var initComponent = function () {
            var componentRef = componentFactory.create(Injector.NULL, [], "#" + rootElId, _this.testModuleRef);
            return new ComponentFixture(componentRef, ngZone, autoDetect);
        };
        var fixture = ngZone ? ngZone.run(initComponent) : initComponent();
        this._activeFixtures.push(fixture);
        return fixture;
    };
    Object.defineProperty(TestBedRender3.prototype, "compiler", {
        get: function () {
            if (this._compiler === null) {
                throw new Error("Need to call TestBed.initTestEnvironment() first");
            }
            return this._compiler;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TestBedRender3.prototype, "testModuleRef", {
        get: function () {
            if (this._testModuleRef === null) {
                this._testModuleRef = this.compiler.finalize();
            }
            return this._testModuleRef;
        },
        enumerable: true,
        configurable: true
    });
    TestBedRender3.prototype.assertNotInstantiated = function (methodName, methodDescription) {
        if (this._testModuleRef !== null) {
            throw new Error("Cannot " + methodDescription + " when the test module has already been instantiated. " +
                ("Make sure you are not using `inject` before `" + methodName + "`."));
        }
    };
    /**
     * Check whether the module scoping queue should be flushed, and flush it if needed.
     *
     * When the TestBed is reset, it clears the JIT module compilation queue, cancelling any
     * in-progress module compilation. This creates a potential hazard - the very first time the
     * TestBed is initialized (or if it's reset without being initialized), there may be pending
     * compilations of modules declared in global scope. These compilations should be finished.
     *
     * To ensure that globally declared modules have their components scoped properly, this function
     * is called whenever TestBed is initialized or reset. The _first_ time that this happens, prior
     * to any other operations, the scoping queue is flushed.
     */
    TestBedRender3.prototype.checkGlobalCompilationFinished = function () {
        // Checking _testNgModuleRef is null should not be necessary, but is left in as an additional
        // guard that compilations queued in tests (after instantiation) are never flushed accidentally.
        if (!this._globalCompilationChecked && this._testModuleRef === null) {
            flushModuleScopingQueueAsMuchAsPossible();
        }
        this._globalCompilationChecked = true;
    };
    TestBedRender3.prototype.destroyActiveFixtures = function () {
        this._activeFixtures.forEach(function (fixture) {
            try {
                fixture.destroy();
            }
            catch (e) {
                console.error('Error during cleanup of component', {
                    component: fixture.componentInstance,
                    stacktrace: e,
                });
            }
        });
        this._activeFixtures = [];
    };
    return TestBedRender3;
}());
export { TestBedRender3 };
var testBed;
export function _getTestBedRender3() {
    return testBed = testBed || new TestBedRender3();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicjNfdGVzdF9iZWQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3Rlc3Rpbmcvc3JjL3IzX3Rlc3RfYmVkLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILG1HQUFtRztBQUNuRyxpR0FBaUc7QUFDakcsdUJBQXVCO0FBQ3ZCLG1CQUFtQjtBQUNuQixPQUFPLEVBR0wsV0FBVyxFQUVYLFFBQVEsRUFFUixNQUFNLEVBSU4sd0JBQXdCLElBQUksZ0JBQWdCLEVBRTVDLHdDQUF3QyxJQUFJLHVDQUF1QyxFQUNuRix3QkFBd0IsSUFBSSx1QkFBdUIsRUFDbkQsVUFBVSxJQUFJLFNBQVMsR0FDeEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsa0JBQWtCO0FBRWxCLE9BQU8sRUFBQyxnQkFBZ0IsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBR3JELE9BQU8sRUFBQywwQkFBMEIsRUFBRSx3QkFBd0IsRUFBaUIscUJBQXFCLEVBQXFCLE1BQU0sbUJBQW1CLENBQUM7QUFDakosT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFFekQsSUFBSSxrQkFBa0IsR0FBRyxDQUFDLENBQUM7QUFFM0IsSUFBTSxTQUFTLEdBQVcsTUFBTSxDQUFDLFdBQVcsQ0FBQyxDQUFDO0FBRTlDOzs7Ozs7Ozs7R0FTRztBQUNIO0lBQUE7UUF5SEUsYUFBYTtRQUViLGFBQVEsR0FBZ0IsSUFBTSxDQUFDO1FBQy9CLGFBQVEsR0FBMEIsSUFBTSxDQUFDO1FBRWpDLGNBQVMsR0FBMkIsSUFBSSxDQUFDO1FBQ3pDLG1CQUFjLEdBQTBCLElBQUksQ0FBQztRQUU3QyxvQkFBZSxHQUE0QixFQUFFLENBQUM7UUFDOUMsOEJBQXlCLEdBQUcsS0FBSyxDQUFDO0lBMk01QyxDQUFDO0lBNVVDOzs7Ozs7Ozs7Ozs7T0FZRztJQUNJLGtDQUFtQixHQUExQixVQUNJ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Z2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8vIFRoZSBmb3JtYXR0ZXIgYW5kIENJIGRpc2FncmVlIG9uIGhvdyB0aGlzIGltcG9ydCBzdGF0ZW1lbnQgc2hvdWxkIGJlIGZvcm1hdHRlZC4gQm90aCB0cnkgdG8ga2VlcFxuLy8gaXQgb24gb25lIGxpbmUsIHRvbywgd2hpY2ggaGFzIGdvdHRlbiB2ZXJ5IGhhcmQgdG8gcmVhZCAmIG1hbmFnZS4gU28gZGlzYWJsZSB0aGUgZm9ybWF0dGVyIGZvclxuLy8gdGhpcyBzdGF0ZW1lbnQgb25seS5cbi8vIGNsYW5nLWZvcm1hdCBvZmZcbmltcG9ydCB7XG4gIENvbXBvbmVudCxcbiAgRGlyZWN0aXZlLFxuICBJbmplY3RGbGFncyxcbiAgSW5qZWN0aW9uVG9rZW4sXG4gIEluamVjdG9yLFxuICBOZ01vZHVsZSxcbiAgTmdab25lLFxuICBQaXBlLFxuICBQbGF0Zm9ybVJlZixcbiAgVHlwZSxcbiAgybVSZW5kZXIzQ29tcG9uZW50RmFjdG9yeSBhcyBDb21wb25lbnRGYWN0b3J5LFxuICDJtVJlbmRlcjNOZ01vZHVsZVJlZiBhcyBOZ01vZHVsZVJlZixcbiAgybVmbHVzaE1vZHVsZVNjb3BpbmdRdWV1ZUFzTXVjaEFzUG9zc2libGUgYXMgZmx1c2hNb2R1bGVTY29waW5nUXVldWVBc011Y2hBc1Bvc3NpYmxlLFxuICDJtXJlc2V0Q29tcGlsZWRDb21wb25lbnRzIGFzIHJlc2V0Q29tcGlsZWRDb21wb25lbnRzLFxuICDJtXN0cmluZ2lmeSBhcyBzdHJpbmdpZnksXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuLy8gY2xhbmctZm9ybWF0IG9uXG5cbmltcG9ydCB7Q29tcG9uZW50Rml4dHVyZX0gZnJvbSAnLi9jb21wb25lbnRfZml4dHVyZSc7XG5pbXBvcnQge01ldGFkYXRhT3ZlcnJpZGV9IGZyb20gJy4vbWV0YWRhdGFfb3ZlcnJpZGUnO1xuaW1wb3J0IHtUZXN0QmVkfSBmcm9tICcuL3Rlc3RfYmVkJztcbmltcG9ydCB7Q29tcG9uZW50Rml4dHVyZUF1dG9EZXRlY3QsIENvbXBvbmVudEZpeHR1cmVOb05nWm9uZSwgVGVzdEJlZFN0YXRpYywgVGVzdENvbXBvbmVudFJlbmRlcmVyLCBUZXN0TW9kdWxlTWV0YWRhdGF9IGZyb20gJy4vdGVzdF9iZWRfY29tbW9uJztcbmltcG9ydCB7UjNUZXN0QmVkQ29tcGlsZXJ9IGZyb20gJy4vcjNfdGVzdF9iZWRfY29tcGlsZXInO1xuXG5sZXQgX25leHRSb290RWxlbWVudElkID0gMDtcblxuY29uc3QgVU5ERUZJTkVEOiBTeW1ib2wgPSBTeW1ib2woJ1VOREVGSU5FRCcpO1xuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICogQ29uZmlndXJlcyBhbmQgaW5pdGlhbGl6ZXMgZW52aXJvbm1lbnQgZm9yIHVuaXQgdGVzdGluZyBhbmQgcHJvdmlkZXMgbWV0aG9kcyBmb3JcbiAqIGNyZWF0aW5nIGNvbXBvbmVudHMgYW5kIHNlcnZpY2VzIGluIHVuaXQgdGVzdHMuXG4gKlxuICogVGVzdEJlZCBpcyB0aGUgcHJpbWFyeSBhcGkgZm9yIHdyaXRpbmcgdW5pdCB0ZXN0cyBmb3IgQW5ndWxhciBhcHBsaWNhdGlvbnMgYW5kIGxpYnJhcmllcy5cbiAqXG4gKiBOb3RlOiBVc2UgYFRlc3RCZWRgIGluIHRlc3RzLiBJdCB3aWxsIGJlIHNldCB0byBlaXRoZXIgYFRlc3RCZWRWaWV3RW5naW5lYCBvciBgVGVzdEJlZFJlbmRlcjNgXG4gKiBhY2NvcmRpbmcgdG8gdGhlIGNvbXBpbGVyIHVzZWQuXG4gKi9cbmV4cG9ydCBjbGFzcyBUZXN0QmVkUmVuZGVyMyBpbXBsZW1lbnRzIEluamVjdG9yLCBUZXN0QmVkIHtcbiAgLyoqXG4gICAqIEluaXRpYWxpemUgdGhlIGVudmlyb25tZW50IGZvciB0ZXN0aW5nIHdpdGggYSBjb21waWxlciBmYWN0b3J5LCBhIFBsYXRmb3JtUmVmLCBhbmQgYW5cbiAgICogYW5ndWxhciBtb2R1bGUuIFRoZXNlIGFyZSBjb21tb24gdG8gZXZlcnkgdGVzdCBpbiB0aGUgc3VpdGUuXG4gICAqXG4gICAqIFRoaXMgbWF5IG9ubHkgYmUgY2FsbGVkIG9uY2UsIHRvIHNldCB1cCB0aGUgY29tbW9uIHByb3ZpZGVycyBmb3IgdGhlIGN1cnJlbnQgdGVzdFxuICAgKiBzdWl0ZSBvbiB0aGUgY3VycmVudCBwbGF0Zm9ybS4gSWYgeW91IGFic29sdXRlbHkgbmVlZCB0byBjaGFuZ2UgdGhlIHByb3ZpZGVycyxcbiAgICogZmlyc3QgdXNlIGByZXNldFRlc3RFbnZpcm9ubWVudGAuXG4gICAqXG4gICAqIFRlc3QgbW9kdWxlcyBhbmQgcGxhdGZvcm1zIGZvciBpbmRpdmlkdWFsIHBsYXRmb3JtcyBhcmUgYXZhaWxhYmxlIGZyb21cbiAgICogJ0Bhbmd1bGFyLzxwbGF0Zm9ybV9uYW1lPi90ZXN0aW5nJy5cbiAgICpcbiAgICogQHB1YmxpY0FwaVxuICAgKi9cbiAgc3RhdGljIGluaXRUZXN0RW52aXJvbm1lbnQoXG4gICAgICBuZ01vZHVsZTogVHlwZTxhbnk+fFR5cGU8YW55PltdLCBwbGF0Zm9ybTogUGxhdGZvcm1SZWYsIGFvdFN1bW1hcmllcz86ICgpID0+IGFueVtdKTogVGVzdEJlZCB7XG4gICAgY29uc3QgdGVzdEJlZCA9IF9nZXRUZXN0QmVkUmVuZGVyMygpO1xuICAgIHRlc3RCZWQuaW5pdFRlc3RFbnZpcm9ubWVudChuZ01vZHVsZSwgcGxhdGZvcm0sIGFvdFN1bW1hcmllcyk7XG4gICAgcmV0dXJuIHRlc3RCZWQ7XG4gIH1cblxuICAvKipcbiAgICogUmVzZXQgdGhlIHByb3ZpZGVycyBmb3IgdGhlIHRlc3QgaW5qZWN0b3IuXG4gICAqXG4gICAqIEBwdWJsaWNBcGlcbiAgICovXG4gIHN0YXRpYyByZXNldFRlc3RFbnZpcm9ubWVudCgpOiB2b2lkIHsgX2dldFRlc3RCZWRSZW5kZXIzKCkucmVzZXRUZXN0RW52aXJvbm1lbnQoKTsgfVxuXG4gIHN0YXRpYyBjb25maWd1cmVDb21waWxlcihjb25maWc6IHtwcm92aWRlcnM/OiBhbnlbXTsgdXNlSml0PzogYm9vbGVhbjt9KTogVGVzdEJlZFN0YXRpYyB7XG4gICAgX2dldFRlc3RCZWRSZW5kZXIzKCkuY29uZmlndXJlQ29tcGlsZXIoY29uZmlnKTtcbiAgICByZXR1cm4gVGVzdEJlZFJlbmRlcjMgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICAvKipcbiAgICogQWxsb3dzIG92ZXJyaWRpbmcgZGVmYXVsdCBwcm92aWRlcnMsIGRpcmVjdGl2ZXMsIHBpcGVzLCBtb2R1bGVzIG9mIHRoZSB0ZXN0IGluamVjdG9yLFxuICAgKiB3aGljaCBhcmUgZGVmaW5lZCBpbiB0ZXN0X2luamVjdG9yLmpzXG4gICAqL1xuICBzdGF0aWMgY29uZmlndXJlVGVzdGluZ01vZHVsZShtb2R1bGVEZWY6IFRlc3RNb2R1bGVNZXRhZGF0YSk6IFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkUmVuZGVyMygpLmNvbmZpZ3VyZVRlc3RpbmdNb2R1bGUobW9kdWxlRGVmKTtcbiAgICByZXR1cm4gVGVzdEJlZFJlbmRlcjMgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICAvKipcbiAgICogQ29tcGlsZSBjb21wb25lbnRzIHdpdGggYSBgdGVtcGxhdGVVcmxgIGZvciB0aGUgdGVzdCdzIE5nTW9kdWxlLlxuICAgKiBJdCBpcyBuZWNlc3NhcnkgdG8gY2FsbCB0aGlzIGZ1bmN0aW9uXG4gICAqIGFzIGZldGNoaW5nIHVybHMgaXMgYXN5bmNocm9ub3VzLlxuICAgKi9cbiAgc3RhdGljIGNvbXBpbGVDb21wb25lbnRzKCk6IFByb21pc2U8YW55PiB7IHJldHVybiBfZ2V0VGVzdEJlZFJlbmRlcjMoKS5jb21waWxlQ29tcG9uZW50cygpOyB9XG5cbiAgc3RhdGljIG92ZXJyaWRlTW9kdWxlKG5nTW9kdWxlOiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPE5nTW9kdWxlPik6IFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkUmVuZGVyMygpLm92ZXJyaWRlTW9kdWxlKG5nTW9kdWxlLCBvdmVycmlkZSk7XG4gICAgcmV0dXJuIFRlc3RCZWRSZW5kZXIzIGFzIGFueSBhcyBUZXN0QmVkU3RhdGljO1xuICB9XG5cbiAgc3RhdGljIG92ZXJyaWRlQ29tcG9uZW50KGNvbXBvbmVudDogVHlwZTxhbnk+LCBvdmVycmlkZTogTWV0YWRhdGFPdmVycmlkZTxDb21wb25lbnQ+KTpcbiAgICAgIFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkUmVuZGVyMygpLm92ZXJyaWRlQ29tcG9uZW50KGNvbXBvbmVudCwgb3ZlcnJpZGUpO1xuICAgIHJldHVybiBUZXN0QmVkUmVuZGVyMyBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcbiAgfVxuXG4gIHN0YXRpYyBvdmVycmlkZURpcmVjdGl2ZShkaXJlY3RpdmU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8RGlyZWN0aXZlPik6XG4gICAgICBUZXN0QmVkU3RhdGljIHtcbiAgICBfZ2V0VGVzdEJlZFJlbmRlcjMoKS5vdmVycmlkZURpcmVjdGl2ZShkaXJlY3RpdmUsIG92ZXJyaWRlKTtcbiAgICByZXR1cm4gVGVzdEJlZFJlbmRlcjMgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICBzdGF0aWMgb3ZlcnJpZGVQaXBlKHBpcGU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8UGlwZT4pOiBUZXN0QmVkU3RhdGljIHtcbiAgICBfZ2V0VGVzdEJlZFJlbmRlcjMoKS5vdmVycmlkZVBpcGUocGlwZSwgb3ZlcnJpZGUpO1xuICAgIHJldHVybiBUZXN0QmVkUmVuZGVyMyBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcbiAgfVxuXG4gIHN0YXRpYyBvdmVycmlkZVRlbXBsYXRlKGNvbXBvbmVudDogVHlwZTxhbnk+LCB0ZW1wbGF0ZTogc3RyaW5nKTogVGVzdEJlZFN0YXRpYyB7XG4gICAgX2dldFRlc3RCZWRSZW5kZXIzKCkub3ZlcnJpZGVDb21wb25lbnQoY29tcG9uZW50LCB7c2V0OiB7dGVtcGxhdGUsIHRlbXBsYXRlVXJsOiBudWxsICF9fSk7XG4gICAgcmV0dXJuIFRlc3RCZWRSZW5kZXIzIGFzIGFueSBhcyBUZXN0QmVkU3RhdGljO1xuICB9XG5cbiAgLyoqXG4gICAqIE92ZXJyaWRlcyB0aGUgdGVtcGxhdGUgb2YgdGhlIGdpdmVuIGNvbXBvbmVudCwgY29tcGlsaW5nIHRoZSB0ZW1wbGF0ZVxuICAgKiBpbiB0aGUgY29udGV4dCBvZiB0aGUgVGVzdGluZ01vZHVsZS5cbiAgICpcbiAgICogTm90ZTogVGhpcyB3b3JrcyBmb3IgSklUIGFuZCBBT1RlZCBjb21wb25lbnRzIGFzIHdlbGwuXG4gICAqL1xuICBzdGF0aWMgb3ZlcnJpZGVUZW1wbGF0ZVVzaW5nVGVzdGluZ01vZHVsZShjb21wb25lbnQ6IFR5cGU8YW55PiwgdGVtcGxhdGU6IHN0cmluZyk6IFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkUmVuZGVyMygpLm92ZXJyaWRlVGVtcGxhdGVVc2luZ1Rlc3RpbmdNb2R1bGUoY29tcG9uZW50LCB0ZW1wbGF0ZSk7XG4gICAgcmV0dXJuIFRlc3RCZWRSZW5kZXIzIGFzIGFueSBhcyBUZXN0QmVkU3RhdGljO1xuICB9XG5cbiAgc3RhdGljIG92ZXJyaWRlUHJvdmlkZXIodG9rZW46IGFueSwgcHJvdmlkZXI6IHtcbiAgICB1c2VGYWN0b3J5OiBGdW5jdGlvbixcbiAgICBkZXBzOiBhbnlbXSxcbiAgfSk6IFRlc3RCZWRTdGF0aWM7XG4gIHN0YXRpYyBvdmVycmlkZVByb3ZpZGVyKHRva2VuOiBhbnksIHByb3ZpZGVyOiB7dXNlVmFsdWU6IGFueTt9KTogVGVzdEJlZFN0YXRpYztcbiAgc3RhdGljIG92ZXJyaWRlUHJvdmlkZXIodG9rZW46IGFueSwgcHJvdmlkZXI6IHtcbiAgICB1c2VGYWN0b3J5PzogRnVuY3Rpb24sXG4gICAgdXNlVmFsdWU/OiBhbnksXG4gICAgZGVwcz86IGFueVtdLFxuICB9KTogVGVzdEJlZFN0YXRpYyB7XG4gICAgX2dldFRlc3RCZWRSZW5kZXIzKCkub3ZlcnJpZGVQcm92aWRlcih0b2tlbiwgcHJvdmlkZXIpO1xuICAgIHJldHVybiBUZXN0QmVkUmVuZGVyMyBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcbiAgfVxuXG4gIHN0YXRpYyBnZXQ8VD4odG9rZW46IFR5cGU8VD58SW5qZWN0aW9uVG9rZW48VD4sIG5vdEZvdW5kVmFsdWU/OiBULCBmbGFncz86IEluamVjdEZsYWdzKTogYW55O1xuICAvKipcbiAgICogQGRlcHJlY2F0ZWQgZnJvbSB2OC4wLjAgdXNlIFR5cGU8VD4gb3IgSW5qZWN0aW9uVG9rZW48VD5cbiAgICovXG4gIHN0YXRpYyBnZXQodG9rZW46IGFueSwgbm90Rm91bmRWYWx1ZT86IGFueSk6IGFueTtcbiAgc3RhdGljIGdldChcbiAgICAgIHRva2VuOiBhbnksIG5vdEZvdW5kVmFsdWU6IGFueSA9IEluamVjdG9yLlRIUk9XX0lGX05PVF9GT1VORCxcbiAgICAgIGZsYWdzOiBJbmplY3RGbGFncyA9IEluamVjdEZsYWdzLkRlZmF1bHQpOiBhbnkge1xuICAgIHJldHVybiBfZ2V0VGVzdEJlZFJlbmRlcjMoKS5nZXQodG9rZW4sIG5vdEZvdW5kVmFsdWUpO1xuICB9XG5cbiAgc3RhdGljIGNyZWF0ZUNvbXBvbmVudDxUPihjb21wb25lbnQ6IFR5cGU8VD4pOiBDb21wb25lbnRGaXh0dXJlPFQ+IHtcbiAgICByZXR1cm4gX2dldFRlc3RCZWRSZW5kZXIzKCkuY3JlYXRlQ29tcG9uZW50KGNvbXBvbmVudCk7XG4gIH1cblxuICBzdGF0aWMgcmVzZXRUZXN0aW5nTW9kdWxlKCk6IFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkUmVuZGVyMygpLnJlc2V0VGVzdGluZ01vZHVsZSgpO1xuICAgIHJldHVybiBUZXN0QmVkUmVuZGVyMyBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcbiAgfVxuXG4gIC8vIFByb3BlcnRpZXNcblxuICBwbGF0Zm9ybTogUGxhdGZvcm1SZWYgPSBudWxsICE7XG4gIG5nTW9kdWxlOiBUeXBlPGFueT58VHlwZTxhbnk+W10gPSBudWxsICE7XG5cbiAgcHJpdmF0ZSBfY29tcGlsZXI6IFIzVGVzdEJlZENvbXBpbGVyfG51bGwgPSBudWxsO1xuICBwcml2YXRlIF90ZXN0TW9kdWxlUmVmOiBOZ01vZHVsZVJlZjxhbnk+fG51bGwgPSBudWxsO1xuXG4gIHByaXZhdGUgX2FjdGl2ZUZpeHR1cmVzOiBDb21wb25lbnRGaXh0dXJlPGFueT5bXSA9IFtdO1xuICBwcml2YXRlIF9nbG9iYWxDb21waWxhdGlvbkNoZWNrZWQgPSBmYWxzZTtcblxuICAvKipcbiAgICogSW5pdGlhbGl6ZSB0aGUgZW52aXJvbm1lbnQgZm9yIHRlc3Rpbmcgd2l0aCBhIGNvbXBpbGVyIGZhY3RvcnksIGEgUGxhdGZvcm1SZWYsIGFuZCBhblxuICAgKiBhbmd1bGFyIG1vZHVsZS4gVGhlc2UgYXJlIGNvbW1vbiB0byBldmVyeSB0ZXN0IGluIHRoZSBzdWl0ZS5cbiAgICpcbiAgICogVGhpcyBtYXkgb25seSBiZSBjYWxsZWQgb25jZSwgdG8gc2V0IHVwIHRoZSBjb21tb24gcHJvdmlkZXJzIGZvciB0aGUgY3VycmVudCB0ZXN0XG4gICAqIHN1aXRlIG9uIHRoZSBjdXJyZW50IHBsYXRmb3JtLiBJZiB5b3UgYWJzb2x1dGVseSBuZWVkIHRvIGNoYW5nZSB0aGUgcHJvdmlkZXJzLFxuICAgKiBmaXJzdCB1c2UgYHJlc2V0VGVzdEVudmlyb25tZW50YC5cbiAgICpcbiAgICogVGVzdCBtb2R1bGVzIGFuZCBwbGF0Zm9ybXMgZm9yIGluZGl2aWR1YWwgcGxhdGZvcm1zIGFyZSBhdmFpbGFibGUgZnJvbVxuICAgKiAnQGFuZ3VsYXIvPHBsYXRmb3JtX25hbWU+L3Rlc3RpbmcnLlxuICAgKlxuICAgKiBAcHVibGljQXBpXG4gICAqL1xuICBpbml0VGVzdEVudmlyb25tZW50KFxuICAgICAgbmdNb2R1bGU6IFR5cGU8YW55PnxUeXBlPGFueT5bXSwgcGxhdGZvcm06IFBsYXRmb3JtUmVmLCBhb3RTdW1tYXJpZXM/OiAoKSA9PiBhbnlbXSk6IHZvaWQge1xuICAgIGlmICh0aGlzLnBsYXRmb3JtIHx8IHRoaXMubmdNb2R1bGUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignQ2Fubm90IHNldCBiYXNlIHByb3ZpZGVycyBiZWNhdXNlIGl0IGhhcyBhbHJlYWR5IGJlZW4gY2FsbGVkJyk7XG4gICAgfVxuICAgIHRoaXMucGxhdGZvcm0gPSBwbGF0Zm9ybTtcbiAgICB0aGlzLm5nTW9kdWxlID0gbmdNb2R1bGU7XG4gICAgdGhpcy5fY29tcGlsZXIgPSBuZXcgUjNUZXN0QmVkQ29tcGlsZXIodGhpcy5wbGF0Zm9ybSwgdGhpcy5uZ01vZHVsZSk7XG4gIH1cblxuICAvKipcbiAgICogUmVzZXQgdGhlIHByb3ZpZGVycyBmb3IgdGhlIHRlc3QgaW5qZWN0b3IuXG4gICAqXG4gICAqIEBwdWJsaWNBcGlcbiAgICovXG4gIHJlc2V0VGVzdEVudmlyb25tZW50KCk6IHZvaWQge1xuICAgIHRoaXMucmVzZXRUZXN0aW5nTW9kdWxlKCk7XG4gICAgdGhpcy5fY29tcGlsZXIgPSBudWxsO1xuICAgIHRoaXMucGxhdGZvcm0gPSBudWxsICE7XG4gICAgdGhpcy5uZ01vZHVsZSA9IG51bGwgITtcbiAgfVxuXG4gIHJlc2V0VGVzdGluZ01vZHVsZSgpOiB2b2lkIHtcbiAgICB0aGlzLmNoZWNrR2xvYmFsQ29tcGlsYXRpb25GaW5pc2hlZCgpO1xuICAgIHJlc2V0Q29tcGlsZWRDb21wb25lbnRzKCk7XG4gICAgaWYgKHRoaXMuX2NvbXBpbGVyICE9PSBudWxsKSB7XG4gICAgICB0aGlzLmNvbXBpbGVyLnJlc3RvcmVPcmlnaW5hbFN0YXRlKCk7XG4gICAgfVxuICAgIHRoaXMuX2NvbXBpbGVyID0gbmV3IFIzVGVzdEJlZENvbXBpbGVyKHRoaXMucGxhdGZvcm0sIHRoaXMubmdNb2R1bGUpO1xuICAgIHRoaXMuX3Rlc3RNb2R1bGVSZWYgPSBudWxsO1xuICAgIHRoaXMuZGVzdHJveUFjdGl2ZUZpeHR1cmVzKCk7XG4gIH1cblxuICBjb25maWd1cmVDb21waWxlcihjb25maWc6IHtwcm92aWRlcnM/OiBhbnlbXTsgdXNlSml0PzogYm9vbGVhbjt9KTogdm9pZCB7XG4gICAgaWYgKGNvbmZpZy51c2VKaXQgIT0gbnVsbCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCd0aGUgUmVuZGVyMyBjb21waWxlciBKaVQgbW9kZSBpcyBub3QgY29uZmlndXJhYmxlICEnKTtcbiAgICB9XG5cbiAgICBpZiAoY29uZmlnLnByb3ZpZGVycyAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICB0aGlzLmNvbXBpbGVyLnNldENvbXBpbGVyUHJvdmlkZXJzKGNvbmZpZy5wcm92aWRlcnMpO1xuICAgIH1cbiAgfVxuXG4gIGNvbmZpZ3VyZVRlc3RpbmdNb2R1bGUobW9kdWxlRGVmOiBUZXN0TW9kdWxlTWV0YWRhdGEpOiB2b2lkIHtcbiAgICB0aGlzLmFzc2VydE5vdEluc3RhbnRpYXRlZCgnUjNUZXN0QmVkLmNvbmZpZ3VyZVRlc3RpbmdNb2R1bGUnLCAnY29uZmlndXJlIHRoZSB0ZXN0IG1vZHVsZScpO1xuICAgIHRoaXMuY29tcGlsZXIuY29uZmlndXJlVGVzdGluZ01vZHVsZShtb2R1bGVEZWYpO1xuICB9XG5cbiAgY29tcGlsZUNvbXBvbmVudHMoKTogUHJvbWlzZTxhbnk+IHsgcmV0dXJuIHRoaXMuY29tcGlsZXIuY29tcGlsZUNvbXBvbmVudHMoKTsgfVxuXG4gIGdldDxUPih0b2tlbjogVHlwZTxUPnxJbmplY3Rpb25Ub2tlbjxUPiwgbm90Rm91bmRWYWx1ZT86IFQsIGZsYWdzPzogSW5qZWN0RmxhZ3MpOiBhbnk7XG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCBmcm9tIHY4LjAuMCB1c2UgVHlwZTxUPiBvciBJbmplY3Rpb25Ub2tlbjxUPlxuICAgKi9cbiAgZ2V0KHRva2VuOiBhbnksIG5vdEZvdW5kVmFsdWU/OiBhbnkpOiBhbnk7XG4gIGdldCh0b2tlbjogYW55LCBub3RGb3VuZFZhbHVlOiBhbnkgPSBJbmplY3Rvci5USFJPV19JRl9OT1RfRk9VTkQsXG4gICAgICBmbGFnczogSW5qZWN0RmxhZ3MgPSBJbmplY3RGbGFncy5EZWZhdWx0KTogYW55IHtcbiAgICBpZiAodG9rZW4gPT09IFRlc3RCZWRSZW5kZXIzKSB7XG4gICAgICByZXR1cm4gdGhpcztcbiAgICB9XG4gICAgY29uc3QgcmVzdWx0ID0gdGhpcy50ZXN0TW9kdWxlUmVmLmluamVjdG9yLmdldCh0b2tlbiwgVU5ERUZJTkVELCBmbGFncyk7XG4gICAgcmV0dXJuIHJlc3VsdCA9PT0gVU5ERUZJTkVEID8gdGhpcy5jb21waWxlci5pbmplY3Rvci5nZXQodG9rZW4sIG5vdEZvdW5kVmFsdWUsIGZsYWdzKSA6IHJlc3VsdDtcbiAgfVxuXG4gIGV4ZWN1dGUodG9rZW5zOiBhbnlbXSwgZm46IEZ1bmN0aW9uLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICBjb25zdCBwYXJhbXMgPSB0b2tlbnMubWFwKHQgPT4gdGhpcy5nZXQodCkpO1xuICAgIHJldHVybiBmbi5hcHBseShjb250ZXh0LCBwYXJhbXMpO1xuICB9XG5cbiAgb3ZlcnJpZGVNb2R1bGUobmdNb2R1bGU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8TmdNb2R1bGU+KTogdm9pZCB7XG4gICAgdGhpcy5hc3NlcnROb3RJbnN0YW50aWF0ZWQoJ292ZXJyaWRlTW9kdWxlJywgJ292ZXJyaWRlIG1vZHVsZSBtZXRhZGF0YScpO1xuICAgIHRoaXMuY29tcGlsZXIub3ZlcnJpZGVNb2R1bGUobmdNb2R1bGUsIG92ZXJyaWRlKTtcbiAgfVxuXG4gIG92ZXJyaWRlQ29tcG9uZW50KGNvbXBvbmVudDogVHlwZTxhbnk+LCBvdmVycmlkZTogTWV0YWRhdGFPdmVycmlkZTxDb21wb25lbnQ+KTogdm9pZCB7XG4gICAgdGhpcy5hc3NlcnROb3RJbnN0YW50aWF0ZWQoJ292ZXJyaWRlQ29tcG9uZW50JywgJ292ZXJyaWRlIGNvbXBvbmVudCBtZXRhZGF0YScpO1xuICAgIHRoaXMuY29tcGlsZXIub3ZlcnJpZGVDb21wb25lbnQoY29tcG9uZW50LCBvdmVycmlkZSk7XG4gIH1cblxuICBvdmVycmlkZVRlbXBsYXRlVXNpbmdUZXN0aW5nTW9kdWxlKGNvbXBvbmVudDogVHlwZTxhbnk+LCB0ZW1wbGF0ZTogc3RyaW5nKTogdm9pZCB7XG4gICAgdGhpcy5hc3NlcnROb3RJbnN0YW50aWF0ZWQoXG4gICAgICAgICdSM1Rlc3RCZWQub3ZlcnJpZGVUZW1wbGF0ZVVzaW5nVGVzdGluZ01vZHVsZScsXG4gICAgICAgICdDYW5ub3Qgb3ZlcnJpZGUgdGVtcGxhdGUgd2hlbiB0aGUgdGVzdCBtb2R1bGUgaGFzIGFscmVhZHkgYmVlbiBpbnN0YW50aWF0ZWQnKTtcbiAgICB0aGlzLmNvbXBpbGVyLm92ZXJyaWRlVGVtcGxhdGVVc2luZ1Rlc3RpbmdNb2R1bGUoY29tcG9uZW50LCB0ZW1wbGF0ZSk7XG4gIH1cblxuICBvdmVycmlkZURpcmVjdGl2ZShkaXJlY3RpdmU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8RGlyZWN0aXZlPik6IHZvaWQge1xuICAgIHRoaXMuYXNzZXJ0Tm90SW5zdGFudGlhdGVkKCdvdmVycmlkZURpcmVjdGl2ZScsICdvdmVycmlkZSBkaXJlY3RpdmUgbWV0YWRhdGEnKTtcbiAgICB0aGlzLmNvbXBpbGVyLm92ZXJyaWRlRGlyZWN0aXZlKGRpcmVjdGl2ZSwgb3ZlcnJpZGUpO1xuICB9XG5cbiAgb3ZlcnJpZGVQaXBlKHBpcGU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8UGlwZT4pOiB2b2lkIHtcbiAgICB0aGlzLmFzc2VydE5vdEluc3RhbnRpYXRlZCgnb3ZlcnJpZGVQaXBlJywgJ292ZXJyaWRlIHBpcGUgbWV0YWRhdGEnKTtcbiAgICB0aGlzLmNvbXBpbGVyLm92ZXJyaWRlUGlwZShwaXBlLCBvdmVycmlkZSk7XG4gIH1cblxuICAvKipcbiAgICogT3ZlcndyaXRlcyBhbGwgcHJvdmlkZXJzIGZvciB0aGUgZ2l2ZW4gdG9rZW4gd2l0aCB0aGUgZ2l2ZW4gcHJvdmlkZXIgZGVmaW5pdGlvbi5cbiAgICovXG4gIG92ZXJyaWRlUHJvdmlkZXIodG9rZW46IGFueSwgcHJvdmlkZXI6IHt1c2VGYWN0b3J5PzogRnVuY3Rpb24sIHVzZVZhbHVlPzogYW55LCBkZXBzPzogYW55W119KTpcbiAgICAgIHZvaWQge1xuICAgIHRoaXMuY29tcGlsZXIub3ZlcnJpZGVQcm92aWRlcih0b2tlbiwgcHJvdmlkZXIpO1xuICB9XG5cbiAgY3JlYXRlQ29tcG9uZW50PFQ+KHR5cGU6IFR5cGU8VD4pOiBDb21wb25lbnRGaXh0dXJlPFQ+IHtcbiAgICBjb25zdCB0ZXN0Q29tcG9uZW50UmVuZGVyZXI6IFRlc3RDb21wb25lbnRSZW5kZXJlciA9IHRoaXMuZ2V0KFRlc3RDb21wb25lbnRSZW5kZXJlcik7XG4gICAgY29uc3Qgcm9vdEVsSWQgPSBgcm9vdC1uZy1pbnRlcm5hbC1pc29sYXRlZC0ke19uZXh0Um9vdEVsZW1lbnRJZCsrfWA7XG4gICAgdGVzdENvbXBvbmVudFJlbmRlcmVyLmluc2VydFJvb3RFbGVtZW50KHJvb3RFbElkKTtcblxuICAgIGNvbnN0IGNvbXBvbmVudERlZiA9ICh0eXBlIGFzIGFueSkubmdDb21wb25lbnREZWY7XG5cbiAgICBpZiAoIWNvbXBvbmVudERlZikge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBJdCBsb29rcyBsaWtlICcke3N0cmluZ2lmeSh0eXBlKX0nIGhhcyBub3QgYmVlbiBJVlkgY29tcGlsZWQgLSBpdCBoYXMgbm8gJ25nQ29tcG9uZW50RGVmJyBmaWVsZGApO1xuICAgIH1cblxuICAgIC8vIFRPRE86IERvbid0IGNhc3QgYXMgYGFueWAsIHByb3BlciB0eXBlIGlzIGJvb2xlYW5bXVxuICAgIGNvbnN0IG5vTmdab25lID0gdGhpcy5nZXQoQ29tcG9uZW50Rml4dHVyZU5vTmdab25lIGFzIGFueSwgZmFsc2UpO1xuICAgIC8vIFRPRE86IERvbid0IGNhc3QgYXMgYGFueWAsIHByb3BlciB0eXBlIGlzIGJvb2xlYW5bXVxuICAgIGNvbnN0IGF1dG9EZXRlY3Q6IGJvb2xlYW4gPSB0aGlzLmdldChDb21wb25lbnRGaXh0dXJlQXV0b0RldGVjdCBhcyBhbnksIGZhbHNlKTtcbiAgICBjb25zdCBuZ1pvbmU6IE5nWm9uZXxudWxsID0gbm9OZ1pvbmUgPyBudWxsIDogdGhpcy5nZXQoTmdab25lIGFzIFR5cGU8Tmdab25lfG51bGw+LCBudWxsKTtcbiAgICBjb25zdCBjb21wb25lbnRGYWN0b3J5ID0gbmV3IENvbXBvbmVudEZhY3RvcnkoY29tcG9uZW50RGVmKTtcbiAgICBjb25zdCBpbml0Q29tcG9uZW50ID0gKCkgPT4ge1xuICAgICAgY29uc3QgY29tcG9uZW50UmVmID1cbiAgICAgICAgICBjb21wb25lbnRGYWN0b3J5LmNyZWF0ZShJbmplY3Rvci5OVUxMLCBbXSwgYCMke3Jvb3RFbElkfWAsIHRoaXMudGVzdE1vZHVsZVJlZik7XG4gICAgICByZXR1cm4gbmV3IENvbXBvbmVudEZpeHR1cmU8YW55Pihjb21wb25lbnRSZWYsIG5nWm9uZSwgYXV0b0RldGVjdCk7XG4gICAgfTtcbiAgICBjb25zdCBmaXh0dXJlID0gbmdab25lID8gbmdab25lLnJ1bihpbml0Q29tcG9uZW50KSA6IGluaXRDb21wb25lbnQoKTtcbiAgICB0aGlzLl9hY3RpdmVGaXh0dXJlcy5wdXNoKGZpeHR1cmUpO1xuICAgIHJldHVybiBmaXh0dXJlO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXQgY29tcGlsZXIoKTogUjNUZXN0QmVkQ29tcGlsZXIge1xuICAgIGlmICh0aGlzLl9jb21waWxlciA9PT0gbnVsbCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBOZWVkIHRvIGNhbGwgVGVzdEJlZC5pbml0VGVzdEVudmlyb25tZW50KCkgZmlyc3RgKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX2NvbXBpbGVyO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXQgdGVzdE1vZHVsZVJlZigpOiBOZ01vZHVsZVJlZjxhbnk+IHtcbiAgICBpZiAodGhpcy5fdGVzdE1vZHVsZVJlZiA9PT0gbnVsbCkge1xuICAgICAgdGhpcy5fdGVzdE1vZHVsZVJlZiA9IHRoaXMuY29tcGlsZXIuZmluYWxpemUoKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX3Rlc3RNb2R1bGVSZWY7XG4gIH1cblxuICBwcml2YXRlIGFzc2VydE5vdEluc3RhbnRpYXRlZChtZXRob2ROYW1lOiBzdHJpbmcsIG1ldGhvZERlc2NyaXB0aW9uOiBzdHJpbmcpIHtcbiAgICBpZiAodGhpcy5fdGVzdE1vZHVsZVJlZiAhPT0gbnVsbCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBDYW5ub3QgJHttZXRob2REZXNjcmlwdGlvbn0gd2hlbiB0aGUgdGVzdCBtb2R1bGUgaGFzIGFscmVhZHkgYmVlbiBpbnN0YW50aWF0ZWQuIGAgK1xuICAgICAgICAgIGBNYWtlIHN1cmUgeW91IGFyZSBub3QgdXNpbmcgXFxgaW5qZWN0XFxgIGJlZm9yZSBcXGAke21ldGhvZE5hbWV9XFxgLmApO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBDaGVjayB3aGV0aGVyIHRoZSBtb2R1bGUgc2NvcGluZyBxdWV1ZSBzaG91bGQgYmUgZmx1c2hlZCwgYW5kIGZsdXNoIGl0IGlmIG5lZWRlZC5cbiAgICpcbiAgICogV2hlbiB0aGUgVGVzdEJlZCBpcyByZXNldCwgaXQgY2xlYXJzIHRoZSBKSVQgbW9kdWxlIGNvbXBpbGF0aW9uIHF1ZXVlLCBjYW5jZWxsaW5nIGFueVxuICAgKiBpbi1wcm9ncmVzcyBtb2R1bGUgY29tcGlsYXRpb24uIFRoaXMgY3JlYXRlcyBhIHBvdGVudGlhbCBoYXphcmQgLSB0aGUgdmVyeSBmaXJzdCB0aW1lIHRoZVxuICAgKiBUZXN0QmVkIGlzIGluaXRpYWxpemVkIChvciBpZiBpdCdzIHJlc2V0IHdpdGhvdXQgYmVpbmcgaW5pdGlhbGl6ZWQpLCB0aGVyZSBtYXkgYmUgcGVuZGluZ1xuICAgKiBjb21waWxhdGlvbnMgb2YgbW9kdWxlcyBkZWNsYXJlZCBpbiBnbG9iYWwgc2NvcGUuIFRoZXNlIGNvbXBpbGF0aW9ucyBzaG91bGQgYmUgZmluaXNoZWQuXG4gICAqXG4gICAqIFRvIGVuc3VyZSB0aGF0IGdsb2JhbGx5IGRlY2xhcmVkIG1vZHVsZXMgaGF2ZSB0aGVpciBjb21wb25lbnRzIHNjb3BlZCBwcm9wZXJseSwgdGhpcyBmdW5jdGlvblxuICAgKiBpcyBjYWxsZWQgd2hlbmV2ZXIgVGVzdEJlZCBpcyBpbml0aWFsaXplZCBvciByZXNldC4gVGhlIF9maXJzdF8gdGltZSB0aGF0IHRoaXMgaGFwcGVucywgcHJpb3JcbiAgICogdG8gYW55IG90aGVyIG9wZXJhdGlvbnMsIHRoZSBzY29waW5nIHF1ZXVlIGlzIGZsdXNoZWQuXG4gICAqL1xuICBwcml2YXRlIGNoZWNrR2xvYmFsQ29tcGlsYXRpb25GaW5pc2hlZCgpOiB2b2lkIHtcbiAgICAvLyBDaGVja2luZyBfdGVzdE5nTW9kdWxlUmVmIGlzIG51bGwgc2hvdWxkIG5vdCBiZSBuZWNlc3NhcnksIGJ1dCBpcyBsZWZ0IGluIGFzIGFuIGFkZGl0aW9uYWxcbiAgICAvLyBndWFyZCB0aGF0IGNvbXBpbGF0aW9ucyBxdWV1ZWQgaW4gdGVzdHMgKGFmdGVyIGluc3RhbnRpYXRpb24pIGFyZSBuZXZlciBmbHVzaGVkIGFjY2lkZW50YWxseS5cbiAgICBpZiAoIXRoaXMuX2dsb2JhbENvbXBpbGF0aW9uQ2hlY2tlZCAmJiB0aGlzLl90ZXN0TW9kdWxlUmVmID09PSBudWxsKSB7XG4gICAgICBmbHVzaE1vZHVsZVNjb3BpbmdRdWV1ZUFzTXVjaEFzUG9zc2libGUoKTtcbiAgICB9XG4gICAgdGhpcy5fZ2xvYmFsQ29tcGlsYXRpb25DaGVja2VkID0gdHJ1ZTtcbiAgfVxuXG4gIHByaXZhdGUgZGVzdHJveUFjdGl2ZUZpeHR1cmVzKCk6IHZvaWQge1xuICAgIHRoaXMuX2FjdGl2ZUZpeHR1cmVzLmZvckVhY2goKGZpeHR1cmUpID0+IHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGZpeHR1cmUuZGVzdHJveSgpO1xuICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICBjb25zb2xlLmVycm9yKCdFcnJvciBkdXJpbmcgY2xlYW51cCBvZiBjb21wb25lbnQnLCB7XG4gICAgICAgICAgY29tcG9uZW50OiBmaXh0dXJlLmNvbXBvbmVudEluc3RhbmNlLFxuICAgICAgICAgIHN0YWNrdHJhY2U6IGUsXG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH0pO1xuICAgIHRoaXMuX2FjdGl2ZUZpeHR1cmVzID0gW107XG4gIH1cbn1cblxubGV0IHRlc3RCZWQ6IFRlc3RCZWRSZW5kZXIzO1xuXG5leHBvcnQgZnVuY3Rpb24gX2dldFRlc3RCZWRSZW5kZXIzKCk6IFRlc3RCZWRSZW5kZXIzIHtcbiAgcmV0dXJuIHRlc3RCZWQgPSB0ZXN0QmVkIHx8IG5ldyBUZXN0QmVkUmVuZGVyMygpO1xufVxuIl19