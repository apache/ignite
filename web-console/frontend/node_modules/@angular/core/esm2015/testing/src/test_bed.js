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
import { ApplicationInitStatus, Component, InjectFlags, Injector, NgModule, NgZone, Optional, SkipSelf, ɵAPP_ROOT as APP_ROOT, ɵclearOverrides as clearOverrides, ɵgetInjectableDef as getInjectableDef, ɵivyEnabled as ivyEnabled, ɵoverrideComponentView as overrideComponentView, ɵoverrideProvider as overrideProvider, ɵstringify as stringify } from '@angular/core';
import { AsyncTestCompleter } from './async_test_completer';
import { ComponentFixture } from './component_fixture';
import { TestBedRender3, _getTestBedRender3 } from './r3_test_bed';
import { ComponentFixtureAutoDetect, ComponentFixtureNoNgZone, TestComponentRenderer } from './test_bed_common';
import { TestingCompilerFactory } from './test_compiler';
/** @type {?} */
const UNDEFINED = new Object();
/** @type {?} */
let _nextRootElementId = 0;
// WARNING: interface has both a type and a value, skipping emit
/**
 * \@description
 * Configures and initializes environment for unit testing and provides methods for
 * creating components and services in unit tests.
 *
 * `TestBed` is the primary api for writing unit tests for Angular applications and libraries.
 *
 * Note: Use `TestBed` in tests. It will be set to either `TestBedViewEngine` or `TestBedRender3`
 * according to the compiler used.
 */
export class TestBedViewEngine {
    constructor() {
        this._instantiated = false;
        this._compiler = (/** @type {?} */ (null));
        this._moduleRef = (/** @type {?} */ (null));
        this._moduleFactory = (/** @type {?} */ (null));
        this._compilerOptions = [];
        this._moduleOverrides = [];
        this._componentOverrides = [];
        this._directiveOverrides = [];
        this._pipeOverrides = [];
        this._providers = [];
        this._declarations = [];
        this._imports = [];
        this._schemas = [];
        this._activeFixtures = [];
        this._testEnvAotSummaries = (/**
         * @return {?}
         */
        () => []);
        this._aotSummaries = [];
        this._templateOverrides = [];
        this._isRoot = true;
        this._rootProviderOverrides = [];
        this.platform = (/** @type {?} */ (null));
        this.ngModule = (/** @type {?} */ (null));
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
     * '\@angular/<platform_name>/testing'.
     * @param {?} ngModule
     * @param {?} platform
     * @param {?=} aotSummaries
     * @return {?}
     */
    static initTestEnvironment(ngModule, platform, aotSummaries) {
        /** @type {?} */
        const testBed = _getTestBedViewEngine();
        testBed.initTestEnvironment(ngModule, platform, aotSummaries);
        return testBed;
    }
    /**
     * Reset the providers for the test injector.
     * @return {?}
     */
    static resetTestEnvironment() { _getTestBedViewEngine().resetTestEnvironment(); }
    /**
     * @return {?}
     */
    static resetTestingModule() {
        _getTestBedViewEngine().resetTestingModule();
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * Allows overriding default compiler providers and settings
     * which are defined in test_injector.js
     * @param {?} config
     * @return {?}
     */
    static configureCompiler(config) {
        _getTestBedViewEngine().configureCompiler(config);
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * Allows overriding default providers, directives, pipes, modules of the test injector,
     * which are defined in test_injector.js
     * @param {?} moduleDef
     * @return {?}
     */
    static configureTestingModule(moduleDef) {
        _getTestBedViewEngine().configureTestingModule(moduleDef);
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * Compile components with a `templateUrl` for the test's NgModule.
     * It is necessary to call this function
     * as fetching urls is asynchronous.
     * @return {?}
     */
    static compileComponents() { return getTestBed().compileComponents(); }
    /**
     * @param {?} ngModule
     * @param {?} override
     * @return {?}
     */
    static overrideModule(ngModule, override) {
        _getTestBedViewEngine().overrideModule(ngModule, override);
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * @param {?} component
     * @param {?} override
     * @return {?}
     */
    static overrideComponent(component, override) {
        _getTestBedViewEngine().overrideComponent(component, override);
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * @param {?} directive
     * @param {?} override
     * @return {?}
     */
    static overrideDirective(directive, override) {
        _getTestBedViewEngine().overrideDirective(directive, override);
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * @param {?} pipe
     * @param {?} override
     * @return {?}
     */
    static overridePipe(pipe, override) {
        _getTestBedViewEngine().overridePipe(pipe, override);
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * @param {?} component
     * @param {?} template
     * @return {?}
     */
    static overrideTemplate(component, template) {
        _getTestBedViewEngine().overrideComponent(component, { set: { template, templateUrl: (/** @type {?} */ (null)) } });
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * Overrides the template of the given component, compiling the template
     * in the context of the TestingModule.
     *
     * Note: This works for JIT and AOTed components as well.
     * @param {?} component
     * @param {?} template
     * @return {?}
     */
    static overrideTemplateUsingTestingModule(component, template) {
        _getTestBedViewEngine().overrideTemplateUsingTestingModule(component, template);
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * @param {?} token
     * @param {?} provider
     * @return {?}
     */
    static overrideProvider(token, provider) {
        _getTestBedViewEngine().overrideProvider(token, (/** @type {?} */ (provider)));
        return (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
    }
    /**
     * @param {?} token
     * @param {?=} notFoundValue
     * @param {?=} flags
     * @return {?}
     */
    static get(token, notFoundValue = Injector.THROW_IF_NOT_FOUND, flags = InjectFlags.Default) {
        return _getTestBedViewEngine().get(token, notFoundValue, flags);
    }
    /**
     * @template T
     * @param {?} component
     * @return {?}
     */
    static createComponent(component) {
        return _getTestBedViewEngine().createComponent(component);
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
     * '\@angular/<platform_name>/testing'.
     * @param {?} ngModule
     * @param {?} platform
     * @param {?=} aotSummaries
     * @return {?}
     */
    initTestEnvironment(ngModule, platform, aotSummaries) {
        if (this.platform || this.ngModule) {
            throw new Error('Cannot set base providers because it has already been called');
        }
        this.platform = platform;
        this.ngModule = ngModule;
        if (aotSummaries) {
            this._testEnvAotSummaries = aotSummaries;
        }
    }
    /**
     * Reset the providers for the test injector.
     * @return {?}
     */
    resetTestEnvironment() {
        this.resetTestingModule();
        this.platform = (/** @type {?} */ (null));
        this.ngModule = (/** @type {?} */ (null));
        this._testEnvAotSummaries = (/**
         * @return {?}
         */
        () => []);
    }
    /**
     * @return {?}
     */
    resetTestingModule() {
        clearOverrides();
        this._aotSummaries = [];
        this._templateOverrides = [];
        this._compiler = (/** @type {?} */ (null));
        this._moduleOverrides = [];
        this._componentOverrides = [];
        this._directiveOverrides = [];
        this._pipeOverrides = [];
        this._isRoot = true;
        this._rootProviderOverrides = [];
        this._moduleRef = (/** @type {?} */ (null));
        this._moduleFactory = (/** @type {?} */ (null));
        this._compilerOptions = [];
        this._providers = [];
        this._declarations = [];
        this._imports = [];
        this._schemas = [];
        this._instantiated = false;
        this._activeFixtures.forEach((/**
         * @param {?} fixture
         * @return {?}
         */
        (fixture) => {
            try {
                fixture.destroy();
            }
            catch (e) {
                console.error('Error during cleanup of component', {
                    component: fixture.componentInstance,
                    stacktrace: e,
                });
            }
        }));
        this._activeFixtures = [];
    }
    /**
     * @param {?} config
     * @return {?}
     */
    configureCompiler(config) {
        this._assertNotInstantiated('TestBed.configureCompiler', 'configure the compiler');
        this._compilerOptions.push(config);
    }
    /**
     * @param {?} moduleDef
     * @return {?}
     */
    configureTestingModule(moduleDef) {
        this._assertNotInstantiated('TestBed.configureTestingModule', 'configure the test module');
        if (moduleDef.providers) {
            this._providers.push(...moduleDef.providers);
        }
        if (moduleDef.declarations) {
            this._declarations.push(...moduleDef.declarations);
        }
        if (moduleDef.imports) {
            this._imports.push(...moduleDef.imports);
        }
        if (moduleDef.schemas) {
            this._schemas.push(...moduleDef.schemas);
        }
        if (moduleDef.aotSummaries) {
            this._aotSummaries.push(moduleDef.aotSummaries);
        }
    }
    /**
     * @return {?}
     */
    compileComponents() {
        if (this._moduleFactory || this._instantiated) {
            return Promise.resolve(null);
        }
        /** @type {?} */
        const moduleType = this._createCompilerAndModule();
        return this._compiler.compileModuleAndAllComponentsAsync(moduleType)
            .then((/**
         * @param {?} moduleAndComponentFactories
         * @return {?}
         */
        (moduleAndComponentFactories) => {
            this._moduleFactory = moduleAndComponentFactories.ngModuleFactory;
        }));
    }
    /**
     * @private
     * @return {?}
     */
    _initIfNeeded() {
        if (this._instantiated) {
            return;
        }
        if (!this._moduleFactory) {
            try {
                /** @type {?} */
                const moduleType = this._createCompilerAndModule();
                this._moduleFactory =
                    this._compiler.compileModuleAndAllComponentsSync(moduleType).ngModuleFactory;
            }
            catch (e) {
                /** @type {?} */
                const errorCompType = this._compiler.getComponentFromError(e);
                if (errorCompType) {
                    throw new Error(`This test module uses the component ${stringify(errorCompType)} which is using a "templateUrl" or "styleUrls", but they were never compiled. ` +
                        `Please call "TestBed.compileComponents" before your test.`);
                }
                else {
                    throw e;
                }
            }
        }
        for (const { component, templateOf } of this._templateOverrides) {
            /** @type {?} */
            const compFactory = this._compiler.getComponentFactory(templateOf);
            overrideComponentView(component, compFactory);
        }
        /** @type {?} */
        const ngZone = new NgZone({ enableLongStackTrace: true });
        /** @type {?} */
        const providers = [{ provide: NgZone, useValue: ngZone }];
        /** @type {?} */
        const ngZoneInjector = Injector.create({
            providers: providers,
            parent: this.platform.injector,
            name: this._moduleFactory.moduleType.name
        });
        this._moduleRef = this._moduleFactory.create(ngZoneInjector);
        // ApplicationInitStatus.runInitializers() is marked @internal to core. So casting to any
        // before accessing it.
        ((/** @type {?} */ (this._moduleRef.injector.get(ApplicationInitStatus)))).runInitializers();
        this._instantiated = true;
    }
    /**
     * @private
     * @return {?}
     */
    _createCompilerAndModule() {
        /** @type {?} */
        const providers = this._providers.concat([{ provide: TestBed, useValue: this }]);
        /** @type {?} */
        const declarations = [...this._declarations, ...this._templateOverrides.map((/**
             * @param {?} entry
             * @return {?}
             */
            entry => entry.templateOf))];
        /** @type {?} */
        const rootScopeImports = [];
        /** @type {?} */
        const rootProviderOverrides = this._rootProviderOverrides;
        if (this._isRoot) {
            class RootScopeModule {
            }
            RootScopeModule.decorators = [
                { type: NgModule, args: [{
                            providers: [
                                ...rootProviderOverrides,
                            ],
                            jit: true,
                        },] },
            ];
            rootScopeImports.push(RootScopeModule);
        }
        providers.push({ provide: APP_ROOT, useValue: this._isRoot });
        /** @type {?} */
        const imports = [rootScopeImports, this.ngModule, this._imports];
        /** @type {?} */
        const schemas = this._schemas;
        class DynamicTestModule {
        }
        DynamicTestModule.decorators = [
            { type: NgModule, args: [{ providers, declarations, imports, schemas, jit: true },] },
        ];
        /** @type {?} */
        const compilerFactory = this.platform.injector.get(TestingCompilerFactory);
        this._compiler = compilerFactory.createTestingCompiler(this._compilerOptions);
        for (const summary of [this._testEnvAotSummaries, ...this._aotSummaries]) {
            this._compiler.loadAotSummaries(summary);
        }
        this._moduleOverrides.forEach((/**
         * @param {?} entry
         * @return {?}
         */
        (entry) => this._compiler.overrideModule(entry[0], entry[1])));
        this._componentOverrides.forEach((/**
         * @param {?} entry
         * @return {?}
         */
        (entry) => this._compiler.overrideComponent(entry[0], entry[1])));
        this._directiveOverrides.forEach((/**
         * @param {?} entry
         * @return {?}
         */
        (entry) => this._compiler.overrideDirective(entry[0], entry[1])));
        this._pipeOverrides.forEach((/**
         * @param {?} entry
         * @return {?}
         */
        (entry) => this._compiler.overridePipe(entry[0], entry[1])));
        return DynamicTestModule;
    }
    /**
     * @private
     * @param {?} methodName
     * @param {?} methodDescription
     * @return {?}
     */
    _assertNotInstantiated(methodName, methodDescription) {
        if (this._instantiated) {
            throw new Error(`Cannot ${methodDescription} when the test module has already been instantiated. ` +
                `Make sure you are not using \`inject\` before \`${methodName}\`.`);
        }
    }
    /**
     * @param {?} token
     * @param {?=} notFoundValue
     * @param {?=} flags
     * @return {?}
     */
    get(token, notFoundValue = Injector.THROW_IF_NOT_FOUND, flags = InjectFlags.Default) {
        this._initIfNeeded();
        if (token === TestBed) {
            return this;
        }
        // Tests can inject things from the ng module and from the compiler,
        // but the ng module can't inject things from the compiler and vice versa.
        /** @type {?} */
        const result = this._moduleRef.injector.get(token, UNDEFINED, flags);
        return result === UNDEFINED ? this._compiler.injector.get(token, notFoundValue, flags) : result;
    }
    /**
     * @param {?} tokens
     * @param {?} fn
     * @param {?=} context
     * @return {?}
     */
    execute(tokens, fn, context) {
        this._initIfNeeded();
        /** @type {?} */
        const params = tokens.map((/**
         * @param {?} t
         * @return {?}
         */
        t => this.get(t)));
        return fn.apply(context, params);
    }
    /**
     * @param {?} ngModule
     * @param {?} override
     * @return {?}
     */
    overrideModule(ngModule, override) {
        this._assertNotInstantiated('overrideModule', 'override module metadata');
        this._moduleOverrides.push([ngModule, override]);
    }
    /**
     * @param {?} component
     * @param {?} override
     * @return {?}
     */
    overrideComponent(component, override) {
        this._assertNotInstantiated('overrideComponent', 'override component metadata');
        this._componentOverrides.push([component, override]);
    }
    /**
     * @param {?} directive
     * @param {?} override
     * @return {?}
     */
    overrideDirective(directive, override) {
        this._assertNotInstantiated('overrideDirective', 'override directive metadata');
        this._directiveOverrides.push([directive, override]);
    }
    /**
     * @param {?} pipe
     * @param {?} override
     * @return {?}
     */
    overridePipe(pipe, override) {
        this._assertNotInstantiated('overridePipe', 'override pipe metadata');
        this._pipeOverrides.push([pipe, override]);
    }
    /**
     * @param {?} token
     * @param {?} provider
     * @return {?}
     */
    overrideProvider(token, provider) {
        this.overrideProviderImpl(token, provider);
    }
    /**
     * @private
     * @param {?} token
     * @param {?} provider
     * @param {?=} deprecated
     * @return {?}
     */
    overrideProviderImpl(token, provider, deprecated = false) {
        /** @type {?} */
        let def = null;
        if (typeof token !== 'string' && (def = getInjectableDef(token)) && def.providedIn === 'root') {
            if (provider.useFactory) {
                this._rootProviderOverrides.push({ provide: token, useFactory: provider.useFactory, deps: provider.deps || [] });
            }
            else {
                this._rootProviderOverrides.push({ provide: token, useValue: provider.useValue });
            }
        }
        /** @type {?} */
        let flags = 0;
        /** @type {?} */
        let value;
        if (provider.useFactory) {
            flags |= 1024 /* TypeFactoryProvider */;
            value = provider.useFactory;
        }
        else {
            flags |= 256 /* TypeValueProvider */;
            value = provider.useValue;
        }
        /** @type {?} */
        const deps = (provider.deps || []).map((/**
         * @param {?} dep
         * @return {?}
         */
        (dep) => {
            /** @type {?} */
            let depFlags = 0 /* None */;
            /** @type {?} */
            let depToken;
            if (Array.isArray(dep)) {
                dep.forEach((/**
                 * @param {?} entry
                 * @return {?}
                 */
                (entry) => {
                    if (entry instanceof Optional) {
                        depFlags |= 2 /* Optional */;
                    }
                    else if (entry instanceof SkipSelf) {
                        depFlags |= 1 /* SkipSelf */;
                    }
                    else {
                        depToken = entry;
                    }
                }));
            }
            else {
                depToken = dep;
            }
            return [depFlags, depToken];
        }));
        overrideProvider({ token, flags, deps, value, deprecatedBehavior: deprecated });
    }
    /**
     * @param {?} component
     * @param {?} template
     * @return {?}
     */
    overrideTemplateUsingTestingModule(component, template) {
        this._assertNotInstantiated('overrideTemplateUsingTestingModule', 'override template');
        class OverrideComponent {
        }
        OverrideComponent.decorators = [
            { type: Component, args: [{ selector: 'empty', template, jit: true },] },
        ];
        this._templateOverrides.push({ component, templateOf: OverrideComponent });
    }
    /**
     * @template T
     * @param {?} component
     * @return {?}
     */
    createComponent(component) {
        this._initIfNeeded();
        /** @type {?} */
        const componentFactory = this._compiler.getComponentFactory(component);
        if (!componentFactory) {
            throw new Error(`Cannot create the component ${stringify(component)} as it was not imported into the testing module!`);
        }
        // TODO: Don't cast as `any`, proper type is boolean[]
        /** @type {?} */
        const noNgZone = this.get((/** @type {?} */ (ComponentFixtureNoNgZone)), false);
        // TODO: Don't cast as `any`, proper type is boolean[]
        /** @type {?} */
        const autoDetect = this.get((/** @type {?} */ (ComponentFixtureAutoDetect)), false);
        /** @type {?} */
        const ngZone = noNgZone ? null : this.get((/** @type {?} */ (NgZone)), null);
        /** @type {?} */
        const testComponentRenderer = this.get(TestComponentRenderer);
        /** @type {?} */
        const rootElId = `root${_nextRootElementId++}`;
        testComponentRenderer.insertRootElement(rootElId);
        /** @type {?} */
        const initComponent = (/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const componentRef = componentFactory.create(Injector.NULL, [], `#${rootElId}`, this._moduleRef);
            return new ComponentFixture(componentRef, ngZone, autoDetect);
        });
        /** @type {?} */
        const fixture = !ngZone ? initComponent() : ngZone.run(initComponent);
        this._activeFixtures.push(fixture);
        return fixture;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._instantiated;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._compiler;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._moduleRef;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._moduleFactory;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._compilerOptions;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._moduleOverrides;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._componentOverrides;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._directiveOverrides;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._pipeOverrides;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._providers;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._declarations;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._imports;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._schemas;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._activeFixtures;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._testEnvAotSummaries;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._aotSummaries;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._templateOverrides;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._isRoot;
    /**
     * @type {?}
     * @private
     */
    TestBedViewEngine.prototype._rootProviderOverrides;
    /** @type {?} */
    TestBedViewEngine.prototype.platform;
    /** @type {?} */
    TestBedViewEngine.prototype.ngModule;
}
/**
 * \@description
 * Configures and initializes environment for unit testing and provides methods for
 * creating components and services in unit tests.
 *
 * `TestBed` is the primary api for writing unit tests for Angular applications and libraries.
 *
 * Note: Use `TestBed` in tests. It will be set to either `TestBedViewEngine` or `TestBedRender3`
 * according to the compiler used.
 *
 * \@publicApi
 * @type {?}
 */
export const TestBed = ivyEnabled ? (/** @type {?} */ ((/** @type {?} */ (TestBedRender3)))) : (/** @type {?} */ ((/** @type {?} */ (TestBedViewEngine))));
/**
 * Returns a singleton of the applicable `TestBed`.
 *
 * It will be either an instance of `TestBedViewEngine` or `TestBedRender3`.
 *
 * \@publicApi
 * @type {?}
 */
export const getTestBed = ivyEnabled ? _getTestBedRender3 : _getTestBedViewEngine;
/** @type {?} */
let testBed;
/**
 * @return {?}
 */
function _getTestBedViewEngine() {
    return testBed = testBed || new TestBedViewEngine();
}
/**
 * Allows injecting dependencies in `beforeEach()` and `it()`.
 *
 * Example:
 *
 * ```
 * beforeEach(inject([Dependency, AClass], (dep, object) => {
 *   // some code that uses `dep` and `object`
 *   // ...
 * }));
 *
 * it('...', inject([AClass], (object) => {
 *   object.doSomething();
 *   expect(...);
 * })
 * ```
 *
 * Notes:
 * - inject is currently a function because of some Traceur limitation the syntax should
 * eventually
 *   becomes `it('...', \@Inject (object: AClass, async: AsyncTestCompleter) => { ... });`
 *
 * \@publicApi
 * @param {?} tokens
 * @param {?} fn
 * @return {?}
 */
export function inject(tokens, fn) {
    /** @type {?} */
    const testBed = getTestBed();
    if (tokens.indexOf(AsyncTestCompleter) >= 0) {
        // Not using an arrow function to preserve context passed from call site
        return (/**
         * @this {?}
         * @return {?}
         */
        function () {
            // Return an async test method that returns a Promise if AsyncTestCompleter is one of
            // the injected tokens.
            return testBed.compileComponents().then((/**
             * @return {?}
             */
            () => {
                /** @type {?} */
                const completer = testBed.get(AsyncTestCompleter);
                testBed.execute(tokens, fn, this);
                return completer.promise;
            }));
        });
    }
    else {
        // Not using an arrow function to preserve context passed from call site
        return (/**
         * @this {?}
         * @return {?}
         */
        function () { return testBed.execute(tokens, fn, this); });
    }
}
/**
 * \@publicApi
 */
export class InjectSetupWrapper {
    /**
     * @param {?} _moduleDef
     */
    constructor(_moduleDef) {
        this._moduleDef = _moduleDef;
    }
    /**
     * @private
     * @return {?}
     */
    _addModule() {
        /** @type {?} */
        const moduleDef = this._moduleDef();
        if (moduleDef) {
            getTestBed().configureTestingModule(moduleDef);
        }
    }
    /**
     * @param {?} tokens
     * @param {?} fn
     * @return {?}
     */
    inject(tokens, fn) {
        /** @type {?} */
        const self = this;
        // Not using an arrow function to preserve context passed from call site
        return (/**
         * @this {?}
         * @return {?}
         */
        function () {
            self._addModule();
            return inject(tokens, fn).call(this);
        });
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    InjectSetupWrapper.prototype._moduleDef;
}
/**
 * @param {?} moduleDef
 * @param {?=} fn
 * @return {?}
 */
export function withModule(moduleDef, fn) {
    if (fn) {
        // Not using an arrow function to preserve context passed from call site
        return (/**
         * @this {?}
         * @return {?}
         */
        function () {
            /** @type {?} */
            const testBed = getTestBed();
            if (moduleDef) {
                testBed.configureTestingModule(moduleDef);
            }
            return fn.apply(this);
        });
    }
    return new InjectSetupWrapper((/**
     * @return {?}
     */
    () => moduleDef));
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVzdF9iZWQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3Rlc3Rpbmcvc3JjL3Rlc3RfYmVkLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLHFCQUFxQixFQUFtQixTQUFTLEVBQWEsV0FBVyxFQUFrQixRQUFRLEVBQUUsUUFBUSxFQUFnQyxNQUFNLEVBQUUsUUFBUSxFQUErQyxRQUFRLEVBQXdCLFNBQVMsSUFBSSxRQUFRLEVBQWtELGVBQWUsSUFBSSxjQUFjLEVBQUUsaUJBQWlCLElBQUksZ0JBQWdCLEVBQUUsV0FBVyxJQUFJLFVBQVUsRUFBRSxzQkFBc0IsSUFBSSxxQkFBcUIsRUFBRSxpQkFBaUIsSUFBSSxnQkFBZ0IsRUFBRSxVQUFVLElBQUksU0FBUyxFQUFrQixNQUFNLGVBQWUsQ0FBQztBQUV2akIsT0FBTyxFQUFDLGtCQUFrQixFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFDMUQsT0FBTyxFQUFDLGdCQUFnQixFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFFckQsT0FBTyxFQUFDLGNBQWMsRUFBRSxrQkFBa0IsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUNqRSxPQUFPLEVBQUMsMEJBQTBCLEVBQUUsd0JBQXdCLEVBQWlCLHFCQUFxQixFQUFxQixNQUFNLG1CQUFtQixDQUFDO0FBQ2pKLE9BQU8sRUFBa0Isc0JBQXNCLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQzs7TUFHbEUsU0FBUyxHQUFHLElBQUksTUFBTSxFQUFFOztJQUcxQixrQkFBa0IsR0FBRyxDQUFDOzs7Ozs7Ozs7Ozs7QUFxRjFCLE1BQU0sT0FBTyxpQkFBaUI7SUFBOUI7UUFnSVUsa0JBQWEsR0FBWSxLQUFLLENBQUM7UUFFL0IsY0FBUyxHQUFvQixtQkFBQSxJQUFJLEVBQUUsQ0FBQztRQUNwQyxlQUFVLEdBQXFCLG1CQUFBLElBQUksRUFBRSxDQUFDO1FBQ3RDLG1CQUFj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WUFDNUIsSUFBSSxTQUFTLEVBQUU7Z0JBQ2IsT0FBTyxDQUFDLHNCQUFzQixDQUFDLFNBQVMsQ0FBQyxDQUFDO2FBQzNDO1lBQ0QsT0FBTyxFQUFFLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDO1FBQ3hCLENBQUMsRUFBQztLQUNIO0lBQ0QsT0FBTyxJQUFJLGtCQUFrQjs7O0lBQUMsR0FBRyxFQUFFLENBQUMsU0FBUyxFQUFDLENBQUM7QUFDakQsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtBcHBsaWNhdGlvbkluaXRTdGF0dXMsIENvbXBpbGVyT3B0aW9ucywgQ29tcG9uZW50LCBEaXJlY3RpdmUsIEluamVjdEZsYWdzLCBJbmplY3Rpb25Ub2tlbiwgSW5qZWN0b3IsIE5nTW9kdWxlLCBOZ01vZHVsZUZhY3RvcnksIE5nTW9kdWxlUmVmLCBOZ1pvbmUsIE9wdGlvbmFsLCBQaXBlLCBQbGF0Zm9ybVJlZiwgUHJvdmlkZXIsIFNjaGVtYU1ldGFkYXRhLCBTa2lwU2VsZiwgU3RhdGljUHJvdmlkZXIsIFR5cGUsIMm1QVBQX1JPT1QgYXMgQVBQX1JPT1QsIMm1RGVwRmxhZ3MgYXMgRGVwRmxhZ3MsIMm1Tm9kZUZsYWdzIGFzIE5vZGVGbGFncywgybVjbGVhck92ZXJyaWRlcyBhcyBjbGVhck92ZXJyaWRlcywgybVnZXRJbmplY3RhYmxlRGVmIGFzIGdldEluamVjdGFibGVEZWYsIMm1aXZ5RW5hYmxlZCBhcyBpdnlFbmFibGVkLCDJtW92ZXJyaWRlQ29tcG9uZW50VmlldyBhcyBvdmVycmlkZUNvbXBvbmVudFZpZXcsIMm1b3ZlcnJpZGVQcm92aWRlciBhcyBvdmVycmlkZVByb3ZpZGVyLCDJtXN0cmluZ2lmeSBhcyBzdHJpbmdpZnksIMm1ybVJbmplY3RhYmxlRGVmfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtBc3luY1Rlc3RDb21wbGV0ZXJ9IGZyb20gJy4vYXN5bmNfdGVzdF9jb21wbGV0ZXInO1xuaW1wb3J0IHtDb21wb25lbnRGaXh0dXJlfSBmcm9tICcuL2NvbXBvbmVudF9maXh0dXJlJztcbmltcG9ydCB7TWV0YWRhdGFPdmVycmlkZX0gZnJvbSAnLi9tZXRhZGF0YV9vdmVycmlkZSc7XG5pbXBvcnQge1Rlc3RCZWRSZW5kZXIzLCBfZ2V0VGVzdEJlZFJlbmRlcjN9IGZyb20gJy4vcjNfdGVzdF9iZWQnO1xuaW1wb3J0IHtDb21wb25lbnRGaXh0dXJlQXV0b0RldGVjdCwgQ29tcG9uZW50Rml4dHVyZU5vTmdab25lLCBUZXN0QmVkU3RhdGljLCBUZXN0Q29tcG9uZW50UmVuZGVyZXIsIFRlc3RNb2R1bGVNZXRhZGF0YX0gZnJvbSAnLi90ZXN0X2JlZF9jb21tb24nO1xuaW1wb3J0IHtUZXN0aW5nQ29tcGlsZXIsIFRlc3RpbmdDb21waWxlckZhY3Rvcnl9IGZyb20gJy4vdGVzdF9jb21waWxlcic7XG5cblxuY29uc3QgVU5ERUZJTkVEID0gbmV3IE9iamVjdCgpO1xuXG5cbmxldCBfbmV4dFJvb3RFbGVtZW50SWQgPSAwO1xuXG4vKipcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBUZXN0QmVkIHtcbiAgcGxhdGZvcm06IFBsYXRmb3JtUmVmO1xuXG4gIG5nTW9kdWxlOiBUeXBlPGFueT58VHlwZTxhbnk+W107XG5cbiAgLyoqXG4gICAqIEluaXRpYWxpemUgdGhlIGVudmlyb25tZW50IGZvciB0ZXN0aW5nIHdpdGggYSBjb21waWxlciBmYWN0b3J5LCBhIFBsYXRmb3JtUmVmLCBhbmQgYW5cbiAgICogYW5ndWxhciBtb2R1bGUuIFRoZXNlIGFyZSBjb21tb24gdG8gZXZlcnkgdGVzdCBpbiB0aGUgc3VpdGUuXG4gICAqXG4gICAqIFRoaXMgbWF5IG9ubHkgYmUgY2FsbGVkIG9uY2UsIHRvIHNldCB1cCB0aGUgY29tbW9uIHByb3ZpZGVycyBmb3IgdGhlIGN1cnJlbnQgdGVzdFxuICAgKiBzdWl0ZSBvbiB0aGUgY3VycmVudCBwbGF0Zm9ybS4gSWYgeW91IGFic29sdXRlbHkgbmVlZCB0byBjaGFuZ2UgdGhlIHByb3ZpZGVycyxcbiAgICogZmlyc3QgdXNlIGByZXNldFRlc3RFbnZpcm9ubWVudGAuXG4gICAqXG4gICAqIFRlc3QgbW9kdWxlcyBhbmQgcGxhdGZvcm1zIGZvciBpbmRpdmlkdWFsIHBsYXRmb3JtcyBhcmUgYXZhaWxhYmxlIGZyb21cbiAgICogJ0Bhbmd1bGFyLzxwbGF0Zm9ybV9uYW1lPi90ZXN0aW5nJy5cbiAgICovXG4gIGluaXRUZXN0RW52aXJvbm1lbnQoXG4gICAgICBuZ01vZHVsZTogVHlwZTxhbnk+fFR5cGU8YW55PltdLCBwbGF0Zm9ybTogUGxhdGZvcm1SZWYsIGFvdFN1bW1hcmllcz86ICgpID0+IGFueVtdKTogdm9pZDtcblxuICAvKipcbiAgICogUmVzZXQgdGhlIHByb3ZpZGVycyBmb3IgdGhlIHRlc3QgaW5qZWN0b3IuXG4gICAqL1xuICByZXNldFRlc3RFbnZpcm9ubWVudCgpOiB2b2lkO1xuXG4gIHJlc2V0VGVzdGluZ01vZHVsZSgpOiB2b2lkO1xuXG4gIGNvbmZpZ3VyZUNvbXBpbGVyKGNvbmZpZzoge3Byb3ZpZGVycz86IGFueVtdLCB1c2VKaXQ/OiBib29sZWFufSk6IHZvaWQ7XG5cbiAgY29uZmlndXJlVGVzdGluZ01vZHVsZShtb2R1bGVEZWY6IFRlc3RNb2R1bGVNZXRhZGF0YSk6IHZvaWQ7XG5cbiAgY29tcGlsZUNvbXBvbmVudHMoKTogUHJvbWlzZTxhbnk+O1xuXG4gIGdldDxUPih0b2tlbjogVHlwZTxUPnxJbmplY3Rpb25Ub2tlbjxUPiwgbm90Rm91bmRWYWx1ZT86IFQsIGZsYWdzPzogSW5qZWN0RmxhZ3MpOiBhbnk7XG5cbiAgLy8gVE9ETzogc3dpdGNoIGJhY2sgdG8gb2ZmaWNpYWwgZGVwcmVjYXRpb24gbWFya2VyIG9uY2UgVFNMaW50IGlzc3VlIGlzIHJlc29sdmVkXG4gIC8vIGh0dHBzOi8vZ2l0aHViLmNvbS9wYWxhbnRpci90c2xpbnQvaXNzdWVzLzQ1MjJcbiAgLyoqXG4gICAqIGRlcHJlY2F0ZWQgZnJvbSB2OC4wLjAgdXNlIFR5cGU8VD4gb3IgSW5qZWN0aW9uVG9rZW48VD5cbiAgICogVGhpcyBkb2VzIG5vdCB1c2UgdGhlIGRlcHJlY2F0ZWQganNkb2MgdGFnIG9uIHB1cnBvc2VcbiAgICogYmVjYXVzZSBpdCByZW5kZXJzIGFsbCBvdmVybG9hZHMgYXMgZGVwcmVjYXRlZCBpbiBUU0xpbnRcbiAgICogZHVlIHRvIGh0dHBzOi8vZ2l0aHViLmNvbS9wYWxhbnRpci90c2xpbnQvaXNzdWVzLzQ1MjIuXG4gICAqL1xuICBnZXQodG9rZW46IGFueSwgbm90Rm91bmRWYWx1ZT86IGFueSk6IGFueTtcblxuICBleGVjdXRlKHRva2VuczogYW55W10sIGZuOiBGdW5jdGlvbiwgY29udGV4dD86IGFueSk6IGFueTtcblxuICBvdmVycmlkZU1vZHVsZShuZ01vZHVsZTogVHlwZTxhbnk+LCBvdmVycmlkZTogTWV0YWRhdGFPdmVycmlkZTxOZ01vZHVsZT4pOiB2b2lkO1xuXG4gIG92ZXJyaWRlQ29tcG9uZW50KGNvbXBvbmVudDogVHlwZTxhbnk+LCBvdmVycmlkZTogTWV0YWRhdGFPdmVycmlkZTxDb21wb25lbnQ+KTogdm9pZDtcblxuICBvdmVycmlkZURpcmVjdGl2ZShkaXJlY3RpdmU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8RGlyZWN0aXZlPik6IHZvaWQ7XG5cbiAgb3ZlcnJpZGVQaXBlKHBpcGU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8UGlwZT4pOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBPdmVyd3JpdGVzIGFsbCBwcm92aWRlcnMgZm9yIHRoZSBnaXZlbiB0b2tlbiB3aXRoIHRoZSBnaXZlbiBwcm92aWRlciBkZWZpbml0aW9uLlxuICAgKi9cbiAgb3ZlcnJpZGVQcm92aWRlcih0b2tlbjogYW55LCBwcm92aWRlcjoge1xuICAgIHVzZUZhY3Rvcnk6IEZ1bmN0aW9uLFxuICAgIGRlcHM6IGFueVtdLFxuICB9KTogdm9pZDtcbiAgb3ZlcnJpZGVQcm92aWRlcih0b2tlbjogYW55LCBwcm92aWRlcjoge3VzZVZhbHVlOiBhbnk7fSk6IHZvaWQ7XG4gIG92ZXJyaWRlUHJvdmlkZXIodG9rZW46IGFueSwgcHJvdmlkZXI6IHt1c2VGYWN0b3J5PzogRnVuY3Rpb24sIHVzZVZhbHVlPzogYW55LCBkZXBzPzogYW55W119KTpcbiAgICAgIHZvaWQ7XG5cbiAgb3ZlcnJpZGVUZW1wbGF0ZVVzaW5nVGVzdGluZ01vZHVsZShjb21wb25lbnQ6IFR5cGU8YW55PiwgdGVtcGxhdGU6IHN0cmluZyk6IHZvaWQ7XG5cbiAgY3JlYXRlQ29tcG9uZW50PFQ+KGNvbXBvbmVudDogVHlwZTxUPik6IENvbXBvbmVudEZpeHR1cmU8VD47XG59XG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKiBDb25maWd1cmVzIGFuZCBpbml0aWFsaXplcyBlbnZpcm9ubWVudCBmb3IgdW5pdCB0ZXN0aW5nIGFuZCBwcm92aWRlcyBtZXRob2RzIGZvclxuICogY3JlYXRpbmcgY29tcG9uZW50cyBhbmQgc2VydmljZXMgaW4gdW5pdCB0ZXN0cy5cbiAqXG4gKiBgVGVzdEJlZGAgaXMgdGhlIHByaW1hcnkgYXBpIGZvciB3cml0aW5nIHVuaXQgdGVzdHMgZm9yIEFuZ3VsYXIgYXBwbGljYXRpb25zIGFuZCBsaWJyYXJpZXMuXG4gKlxuICogTm90ZTogVXNlIGBUZXN0QmVkYCBpbiB0ZXN0cy4gSXQgd2lsbCBiZSBzZXQgdG8gZWl0aGVyIGBUZXN0QmVkVmlld0VuZ2luZWAgb3IgYFRlc3RCZWRSZW5kZXIzYFxuICogYWNjb3JkaW5nIHRvIHRoZSBjb21waWxlciB1c2VkLlxuICovXG5leHBvcnQgY2xhc3MgVGVzdEJlZFZpZXdFbmdpbmUgaW1wbGVtZW50cyBJbmplY3RvciwgVGVzdEJlZCB7XG4gIC8qKlxuICAgKiBJbml0aWFsaXplIHRoZSBlbnZpcm9ubWVudCBmb3IgdGVzdGluZyB3aXRoIGEgY29tcGlsZXIgZmFjdG9yeSwgYSBQbGF0Zm9ybVJlZiwgYW5kIGFuXG4gICAqIGFuZ3VsYXIgbW9kdWxlLiBUaGVzZSBhcmUgY29tbW9uIHRvIGV2ZXJ5IHRlc3QgaW4gdGhlIHN1aXRlLlxuICAgKlxuICAgKiBUaGlzIG1heSBvbmx5IGJlIGNhbGxlZCBvbmNlLCB0byBzZXQgdXAgdGhlIGNvbW1vbiBwcm92aWRlcnMgZm9yIHRoZSBjdXJyZW50IHRlc3RcbiAgICogc3VpdGUgb24gdGhlIGN1cnJlbnQgcGxhdGZvcm0uIElmIHlvdSBhYnNvbHV0ZWx5IG5lZWQgdG8gY2hhbmdlIHRoZSBwcm92aWRlcnMsXG4gICAqIGZpcnN0IHVzZSBgcmVzZXRUZXN0RW52aXJvbm1lbnRgLlxuICAgKlxuICAgKiBUZXN0IG1vZHVsZXMgYW5kIHBsYXRmb3JtcyBmb3IgaW5kaXZpZHVhbCBwbGF0Zm9ybXMgYXJlIGF2YWlsYWJsZSBmcm9tXG4gICAqICdAYW5ndWxhci88cGxhdGZvcm1fbmFtZT4vdGVzdGluZycuXG4gICAqL1xuICBzdGF0aWMgaW5pdFRlc3RFbnZpcm9ubWVudChcbiAgICAgIG5nTW9kdWxlOiBUeXBlPGFueT58VHlwZTxhbnk+W10sIHBsYXRmb3JtOiBQbGF0Zm9ybVJlZixcbiAgICAgIGFvdFN1bW1hcmllcz86ICgpID0+IGFueVtdKTogVGVzdEJlZFZpZXdFbmdpbmUge1xuICAgIGNvbnN0IHRlc3RCZWQgPSBfZ2V0VGVzdEJlZFZpZXdFbmdpbmUoKTtcbiAgICB0ZXN0QmVkLmluaXRUZXN0RW52aXJvbm1lbnQobmdNb2R1bGUsIHBsYXRmb3JtLCBhb3RTdW1tYXJpZXMpO1xuICAgIHJldHVybiB0ZXN0QmVkO1xuICB9XG5cbiAgLyoqXG4gICAqIFJlc2V0IHRoZSBwcm92aWRlcnMgZm9yIHRoZSB0ZXN0IGluamVjdG9yLlxuICAgKi9cbiAgc3RhdGljIHJlc2V0VGVzdEVudmlyb25tZW50KCk6IHZvaWQgeyBfZ2V0VGVzdEJlZFZpZXdFbmdpbmUoKS5yZXNldFRlc3RFbnZpcm9ubWVudCgpOyB9XG5cbiAgc3RhdGljIHJlc2V0VGVzdGluZ01vZHVsZSgpOiBUZXN0QmVkU3RhdGljIHtcbiAgICBfZ2V0VGVzdEJlZFZpZXdFbmdpbmUoKS5yZXNldFRlc3RpbmdNb2R1bGUoKTtcbiAgICByZXR1cm4gVGVzdEJlZFZpZXdFbmdpbmUgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICAvKipcbiAgICogQWxsb3dzIG92ZXJyaWRpbmcgZGVmYXVsdCBjb21waWxlciBwcm92aWRlcnMgYW5kIHNldHRpbmdzXG4gICAqIHdoaWNoIGFyZSBkZWZpbmVkIGluIHRlc3RfaW5qZWN0b3IuanNcbiAgICovXG4gIHN0YXRpYyBjb25maWd1cmVDb21waWxlcihjb25maWc6IHtwcm92aWRlcnM/OiBhbnlbXTsgdXNlSml0PzogYm9vbGVhbjt9KTogVGVzdEJlZFN0YXRpYyB7XG4gICAgX2dldFRlc3RCZWRWaWV3RW5naW5lKCkuY29uZmlndXJlQ29tcGlsZXIoY29uZmlnKTtcbiAgICByZXR1cm4gVGVzdEJlZFZpZXdFbmdpbmUgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICAvKipcbiAgICogQWxsb3dzIG92ZXJyaWRpbmcgZGVmYXVsdCBwcm92aWRlcnMsIGRpcmVjdGl2ZXMsIHBpcGVzLCBtb2R1bGVzIG9mIHRoZSB0ZXN0IGluamVjdG9yLFxuICAgKiB3aGljaCBhcmUgZGVmaW5lZCBpbiB0ZXN0X2luamVjdG9yLmpzXG4gICAqL1xuICBzdGF0aWMgY29uZmlndXJlVGVzdGluZ01vZHVsZShtb2R1bGVEZWY6IFRlc3RNb2R1bGVNZXRhZGF0YSk6IFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkVmlld0VuZ2luZSgpLmNvbmZpZ3VyZVRlc3RpbmdNb2R1bGUobW9kdWxlRGVmKTtcbiAgICByZXR1cm4gVGVzdEJlZFZpZXdFbmdpbmUgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICAvKipcbiAgICogQ29tcGlsZSBjb21wb25lbnRzIHdpdGggYSBgdGVtcGxhdGVVcmxgIGZvciB0aGUgdGVzdCdzIE5nTW9kdWxlLlxuICAgKiBJdCBpcyBuZWNlc3NhcnkgdG8gY2FsbCB0aGlzIGZ1bmN0aW9uXG4gICAqIGFzIGZldGNoaW5nIHVybHMgaXMgYXN5bmNocm9ub3VzLlxuICAgKi9cbiAgc3RhdGljIGNvbXBpbGVDb21wb25lbnRzKCk6IFByb21pc2U8YW55PiB7IHJldHVybiBnZXRUZXN0QmVkKCkuY29tcGlsZUNvbXBvbmVudHMoKTsgfVxuXG4gIHN0YXRpYyBvdmVycmlkZU1vZHVsZShuZ01vZHVsZTogVHlwZTxhbnk+LCBvdmVycmlkZTogTWV0YWRhdGFPdmVycmlkZTxOZ01vZHVsZT4pOiBUZXN0QmVkU3RhdGljIHtcbiAgICBfZ2V0VGVzdEJlZFZpZXdFbmdpbmUoKS5vdmVycmlkZU1vZHVsZShuZ01vZHVsZSwgb3ZlcnJpZGUpO1xuICAgIHJldHVybiBUZXN0QmVkVmlld0VuZ2luZSBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcbiAgfVxuXG4gIHN0YXRpYyBvdmVycmlkZUNvbXBvbmVudChjb21wb25lbnQ6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8Q29tcG9uZW50Pik6XG4gICAgICBUZXN0QmVkU3RhdGljIHtcbiAgICBfZ2V0VGVzdEJlZFZpZXdFbmdpbmUoKS5vdmVycmlkZUNvbXBvbmVudChjb21wb25lbnQsIG92ZXJyaWRlKTtcbiAgICByZXR1cm4gVGVzdEJlZFZpZXdFbmdpbmUgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICBzdGF0aWMgb3ZlcnJpZGVEaXJlY3RpdmUoZGlyZWN0aXZlOiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPERpcmVjdGl2ZT4pOlxuICAgICAgVGVzdEJlZFN0YXRpYyB7XG4gICAgX2dldFRlc3RCZWRWaWV3RW5naW5lKCkub3ZlcnJpZGVEaXJlY3RpdmUoZGlyZWN0aXZlLCBvdmVycmlkZSk7XG4gICAgcmV0dXJuIFRlc3RCZWRWaWV3RW5naW5lIGFzIGFueSBhcyBUZXN0QmVkU3RhdGljO1xuICB9XG5cbiAgc3RhdGljIG92ZXJyaWRlUGlwZShwaXBlOiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPFBpcGU+KTogVGVzdEJlZFN0YXRpYyB7XG4gICAgX2dldFRlc3RCZWRWaWV3RW5naW5lKCkub3ZlcnJpZGVQaXBlKHBpcGUsIG92ZXJyaWRlKTtcbiAgICByZXR1cm4gVGVzdEJlZFZpZXdFbmdpbmUgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWM7XG4gIH1cblxuICBzdGF0aWMgb3ZlcnJpZGVUZW1wbGF0ZShjb21wb25lbnQ6IFR5cGU8YW55PiwgdGVtcGxhdGU6IHN0cmluZyk6IFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkVmlld0VuZ2luZSgpLm92ZXJyaWRlQ29tcG9uZW50KGNvbXBvbmVudCwge3NldDoge3RlbXBsYXRlLCB0ZW1wbGF0ZVVybDogbnVsbCAhfX0pO1xuICAgIHJldHVybiBUZXN0QmVkVmlld0VuZ2luZSBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcbiAgfVxuXG4gIC8qKlxuICAgKiBPdmVycmlkZXMgdGhlIHRlbXBsYXRlIG9mIHRoZSBnaXZlbiBjb21wb25lbnQsIGNvbXBpbGluZyB0aGUgdGVtcGxhdGVcbiAgICogaW4gdGhlIGNvbnRleHQgb2YgdGhlIFRlc3RpbmdNb2R1bGUuXG4gICAqXG4gICAqIE5vdGU6IFRoaXMgd29ya3MgZm9yIEpJVCBhbmQgQU9UZWQgY29tcG9uZW50cyBhcyB3ZWxsLlxuICAgKi9cbiAgc3RhdGljIG92ZXJyaWRlVGVtcGxhdGVVc2luZ1Rlc3RpbmdNb2R1bGUoY29tcG9uZW50OiBUeXBlPGFueT4sIHRlbXBsYXRlOiBzdHJpbmcpOiBUZXN0QmVkU3RhdGljIHtcbiAgICBfZ2V0VGVzdEJlZFZpZXdFbmdpbmUoKS5vdmVycmlkZVRlbXBsYXRlVXNpbmdUZXN0aW5nTW9kdWxlKGNvbXBvbmVudCwgdGVtcGxhdGUpO1xuICAgIHJldHVybiBUZXN0QmVkVmlld0VuZ2luZSBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcbiAgfVxuXG4gIC8qKlxuICAgKiBPdmVyd3JpdGVzIGFsbCBwcm92aWRlcnMgZm9yIHRoZSBnaXZlbiB0b2tlbiB3aXRoIHRoZSBnaXZlbiBwcm92aWRlciBkZWZpbml0aW9uLlxuICAgKlxuICAgKiBOb3RlOiBUaGlzIHdvcmtzIGZvciBKSVQgYW5kIEFPVGVkIGNvbXBvbmVudHMgYXMgd2VsbC5cbiAgICovXG4gIHN0YXRpYyBvdmVycmlkZVByb3ZpZGVyKHRva2VuOiBhbnksIHByb3ZpZGVyOiB7XG4gICAgdXNlRmFjdG9yeTogRnVuY3Rpb24sXG4gICAgZGVwczogYW55W10sXG4gIH0pOiBUZXN0QmVkU3RhdGljO1xuICBzdGF0aWMgb3ZlcnJpZGVQcm92aWRlcih0b2tlbjogYW55LCBwcm92aWRlcjoge3VzZVZhbHVlOiBhbnk7fSk6IFRlc3RCZWRTdGF0aWM7XG4gIHN0YXRpYyBvdmVycmlkZVByb3ZpZGVyKHRva2VuOiBhbnksIHByb3ZpZGVyOiB7XG4gICAgdXNlRmFjdG9yeT86IEZ1bmN0aW9uLFxuICAgIHVzZVZhbHVlPzogYW55LFxuICAgIGRlcHM/OiBhbnlbXSxcbiAgfSk6IFRlc3RCZWRTdGF0aWMge1xuICAgIF9nZXRUZXN0QmVkVmlld0VuZ2luZSgpLm92ZXJyaWRlUHJvdmlkZXIodG9rZW4sIHByb3ZpZGVyIGFzIGFueSk7XG4gICAgcmV0dXJuIFRlc3RCZWRWaWV3RW5naW5lIGFzIGFueSBhcyBUZXN0QmVkU3RhdGljO1xuICB9XG5cbiAgc3RhdGljIGdldDxUPih0b2tlbjogVHlwZTxUPnxJbmplY3Rpb25Ub2tlbjxUPiwgbm90Rm91bmRWYWx1ZT86IFQsIGZsYWdzPzogSW5qZWN0RmxhZ3MpOiBhbnk7XG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCBmcm9tIHY4LjAuMCB1c2UgVHlwZTxUPiBvciBJbmplY3Rpb25Ub2tlbjxUPlxuICAgKiBAc3VwcHJlc3Mge2R1cGxpY2F0ZX1cbiAgICovXG4gIHN0YXRpYyBnZXQodG9rZW46IGFueSwgbm90Rm91bmRWYWx1ZT86IGFueSk6IGFueTtcbiAgc3RhdGljIGdldChcbiAgICAgIHRva2VuOiBhbnksIG5vdEZvdW5kVmFsdWU6IGFueSA9IEluamVjdG9yLlRIUk9XX0lGX05PVF9GT1VORCxcbiAgICAgIGZsYWdzOiBJbmplY3RGbGFncyA9IEluamVjdEZsYWdzLkRlZmF1bHQpOiBhbnkge1xuICAgIHJldHVybiBfZ2V0VGVzdEJlZFZpZXdFbmdpbmUoKS5nZXQodG9rZW4sIG5vdEZvdW5kVmFsdWUsIGZsYWdzKTtcbiAgfVxuXG4gIHN0YXRpYyBjcmVhdGVDb21wb25lbnQ8VD4oY29tcG9uZW50OiBUeXBlPFQ+KTogQ29tcG9uZW50Rml4dHVyZTxUPiB7XG4gICAgcmV0dXJuIF9nZXRUZXN0QmVkVmlld0VuZ2luZSgpLmNyZWF0ZUNvbXBvbmVudChjb21wb25lbnQpO1xuICB9XG5cbiAgcHJpdmF0ZSBfaW5zdGFudGlhdGVkOiBib29sZWFuID0gZmFsc2U7XG5cbiAgcHJpdmF0ZSBfY29tcGlsZXI6IFRlc3RpbmdDb21waWxlciA9IG51bGwgITtcbiAgcHJpdmF0ZSBfbW9kdWxlUmVmOiBOZ01vZHVsZVJlZjxhbnk+ID0gbnVsbCAhO1xuICBwcml2YXRlIF9tb2R1bGVGYWN0b3J5OiBOZ01vZHVsZUZhY3Rvcnk8YW55PiA9IG51bGwgITtcblxuICBwcml2YXRlIF9jb21waWxlck9wdGlvbnM6IENvbXBpbGVyT3B0aW9uc1tdID0gW107XG5cbiAgcHJpdmF0ZSBfbW9kdWxlT3ZlcnJpZGVzOiBbVHlwZTxhbnk+LCBNZXRhZGF0YU92ZXJyaWRlPE5nTW9kdWxlPl1bXSA9IFtdO1xuICBwcml2YXRlIF9jb21wb25lbnRPdmVycmlkZXM6IFtUeXBlPGFueT4sIE1ldGFkYXRhT3ZlcnJpZGU8Q29tcG9uZW50Pl1bXSA9IFtdO1xuICBwcml2YXRlIF9kaXJlY3RpdmVPdmVycmlkZXM6IFtUeXBlPGFueT4sIE1ldGFkYXRhT3ZlcnJpZGU8RGlyZWN0aXZlPl1bXSA9IFtdO1xuICBwcml2YXRlIF9waXBlT3ZlcnJpZGVzOiBbVHlwZTxhbnk+LCBNZXRhZGF0YU92ZXJyaWRlPFBpcGU+XVtdID0gW107XG5cbiAgcHJpdmF0ZSBfcHJvdmlkZXJzOiBQcm92aWRlcltdID0gW107XG4gIHByaXZhdGUgX2RlY2xhcmF0aW9uczogQXJyYXk8VHlwZTxhbnk+fGFueVtdfGFueT4gPSBbXTtcbiAgcHJpdmF0ZSBfaW1wb3J0czogQXJyYXk8VHlwZTxhbnk+fGFueVtdfGFueT4gPSBbXTtcbiAgcHJpdmF0ZSBfc2NoZW1hczogQXJyYXk8U2NoZW1hTWV0YWRhdGF8YW55W10+ID0gW107XG4gIHByaXZhdGUgX2FjdGl2ZUZpeHR1cmVzOiBDb21wb25lbnRGaXh0dXJlPGFueT5bXSA9IFtdO1xuXG4gIHByaXZhdGUgX3Rlc3RFbnZBb3RTdW1tYXJpZXM6ICgpID0+IGFueVtdID0gKCkgPT4gW107XG4gIHByaXZhdGUgX2FvdFN1bW1hcmllczogQXJyYXk8KCkgPT4gYW55W10+ID0gW107XG4gIHByaXZhdGUgX3RlbXBsYXRlT3ZlcnJpZGVzOiBBcnJheTx7Y29tcG9uZW50OiBUeXBlPGFueT4sIHRlbXBsYXRlT2Y6IFR5cGU8YW55Pn0+ID0gW107XG5cbiAgcHJpdmF0ZSBfaXNSb290OiBib29sZWFuID0gdHJ1ZTtcbiAgcHJpdmF0ZSBfcm9vdFByb3ZpZGVyT3ZlcnJpZGVzOiBQcm92aWRlcltdID0gW107XG5cbiAgcGxhdGZvcm06IFBsYXRmb3JtUmVmID0gbnVsbCAhO1xuXG4gIG5nTW9kdWxlOiBUeXBlPGFueT58VHlwZTxhbnk+W10gPSBudWxsICE7XG5cbiAgLyoqXG4gICAqIEluaXRpYWxpemUgdGhlIGVudmlyb25tZW50IGZvciB0ZXN0aW5nIHdpdGggYSBjb21waWxlciBmYWN0b3J5LCBhIFBsYXRmb3JtUmVmLCBhbmQgYW5cbiAgICogYW5ndWxhciBtb2R1bGUuIFRoZXNlIGFyZSBjb21tb24gdG8gZXZlcnkgdGVzdCBpbiB0aGUgc3VpdGUuXG4gICAqXG4gICAqIFRoaXMgbWF5IG9ubHkgYmUgY2FsbGVkIG9uY2UsIHRvIHNldCB1cCB0aGUgY29tbW9uIHByb3ZpZGVycyBmb3IgdGhlIGN1cnJlbnQgdGVzdFxuICAgKiBzdWl0ZSBvbiB0aGUgY3VycmVudCBwbGF0Zm9ybS4gSWYgeW91IGFic29sdXRlbHkgbmVlZCB0byBjaGFuZ2UgdGhlIHByb3ZpZGVycyxcbiAgICogZmlyc3QgdXNlIGByZXNldFRlc3RFbnZpcm9ubWVudGAuXG4gICAqXG4gICAqIFRlc3QgbW9kdWxlcyBhbmQgcGxhdGZvcm1zIGZvciBpbmRpdmlkdWFsIHBsYXRmb3JtcyBhcmUgYXZhaWxhYmxlIGZyb21cbiAgICogJ0Bhbmd1bGFyLzxwbGF0Zm9ybV9uYW1lPi90ZXN0aW5nJy5cbiAgICovXG4gIGluaXRUZXN0RW52aXJvbm1lbnQoXG4gICAgICBuZ01vZHVsZTogVHlwZTxhbnk+fFR5cGU8YW55PltdLCBwbGF0Zm9ybTogUGxhdGZvcm1SZWYsIGFvdFN1bW1hcmllcz86ICgpID0+IGFueVtdKTogdm9pZCB7XG4gICAgaWYgKHRoaXMucGxhdGZvcm0gfHwgdGhpcy5uZ01vZHVsZSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdDYW5ub3Qgc2V0IGJhc2UgcHJvdmlkZXJzIGJlY2F1c2UgaXQgaGFzIGFscmVhZHkgYmVlbiBjYWxsZWQnKTtcbiAgICB9XG4gICAgdGhpcy5wbGF0Zm9ybSA9IHBsYXRmb3JtO1xuICAgIHRoaXMubmdNb2R1bGUgPSBuZ01vZHVsZTtcbiAgICBpZiAoYW90U3VtbWFyaWVzKSB7XG4gICAgICB0aGlzLl90ZXN0RW52QW90U3VtbWFyaWVzID0gYW90U3VtbWFyaWVzO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBSZXNldCB0aGUgcHJvdmlkZXJzIGZvciB0aGUgdGVzdCBpbmplY3Rvci5cbiAgICovXG4gIHJlc2V0VGVzdEVudmlyb25tZW50KCk6IHZvaWQge1xuICAgIHRoaXMucmVzZXRUZXN0aW5nTW9kdWxlKCk7XG4gICAgdGhpcy5wbGF0Zm9ybSA9IG51bGwgITtcbiAgICB0aGlzLm5nTW9kdWxlID0gbnVsbCAhO1xuICAgIHRoaXMuX3Rlc3RFbnZBb3RTdW1tYXJpZXMgPSAoKSA9PiBbXTtcbiAgfVxuXG4gIHJlc2V0VGVzdGluZ01vZHVsZSgpOiB2b2lkIHtcbiAgICBjbGVhck92ZXJyaWRlcygpO1xuICAgIHRoaXMuX2FvdFN1bW1hcmllcyA9IFtdO1xuICAgIHRoaXMuX3RlbXBsYXRlT3ZlcnJpZGVzID0gW107XG4gICAgdGhpcy5fY29tcGlsZXIgPSBudWxsICE7XG4gICAgdGhpcy5fbW9kdWxlT3ZlcnJpZGVzID0gW107XG4gICAgdGhpcy5fY29tcG9uZW50T3ZlcnJpZGVzID0gW107XG4gICAgdGhpcy5fZGlyZWN0aXZlT3ZlcnJpZGVzID0gW107XG4gICAgdGhpcy5fcGlwZU92ZXJyaWRlcyA9IFtdO1xuXG4gICAgdGhpcy5faXNSb290ID0gdHJ1ZTtcbiAgICB0aGlzLl9yb290UHJvdmlkZXJPdmVycmlkZXMgPSBbXTtcblxuICAgIHRoaXMuX21vZHVsZVJlZiA9IG51bGwgITtcbiAgICB0aGlzLl9tb2R1bGVGYWN0b3J5ID0gbnVsbCAhO1xuICAgIHRoaXMuX2NvbXBpbGVyT3B0aW9ucyA9IFtdO1xuICAgIHRoaXMuX3Byb3ZpZGVycyA9IFtdO1xuICAgIHRoaXMuX2RlY2xhcmF0aW9ucyA9IFtdO1xuICAgIHRoaXMuX2ltcG9ydHMgPSBbXTtcbiAgICB0aGlzLl9zY2hlbWFzID0gW107XG4gICAgdGhpcy5faW5zdGFudGlhdGVkID0gZmFsc2U7XG4gICAgdGhpcy5fYWN0aXZlRml4dHVyZXMuZm9yRWFjaCgoZml4dHVyZSkgPT4ge1xuICAgICAgdHJ5IHtcbiAgICAgICAgZml4dHVyZS5kZXN0cm95KCk7XG4gICAgICB9IGNhdGNoIChlKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoJ0Vycm9yIGR1cmluZyBjbGVhbnVwIG9mIGNvbXBvbmVudCcsIHtcbiAgICAgICAgICBjb21wb25lbnQ6IGZpeHR1cmUuY29tcG9uZW50SW5zdGFuY2UsXG4gICAgICAgICAgc3RhY2t0cmFjZTogZSxcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfSk7XG4gICAgdGhpcy5fYWN0aXZlRml4dHVyZXMgPSBbXTtcbiAgfVxuXG4gIGNvbmZpZ3VyZUNvbXBpbGVyKGNvbmZpZzoge3Byb3ZpZGVycz86IGFueVtdLCB1c2VKaXQ/OiBib29sZWFufSk6IHZvaWQge1xuICAgIHRoaXMuX2Fzc2VydE5vdEluc3RhbnRpYXRlZCgnVGVzdEJlZC5jb25maWd1cmVDb21waWxlcicsICdjb25maWd1cmUgdGhlIGNvbXBpbGVyJyk7XG4gICAgdGhpcy5fY29tcGlsZXJPcHRpb25zLnB1c2goY29uZmlnKTtcbiAgfVxuXG4gIGNvbmZpZ3VyZVRlc3RpbmdNb2R1bGUobW9kdWxlRGVmOiBUZXN0TW9kdWxlTWV0YWRhdGEpOiB2b2lkIHtcbiAgICB0aGlzLl9hc3NlcnROb3RJbnN0YW50aWF0ZWQoJ1Rlc3RCZWQuY29uZmlndXJlVGVzdGluZ01vZHVsZScsICdjb25maWd1cmUgdGhlIHRlc3QgbW9kdWxlJyk7XG4gICAgaWYgKG1vZHVsZURlZi5wcm92aWRlcnMpIHtcbiAgICAgIHRoaXMuX3Byb3ZpZGVycy5wdXNoKC4uLm1vZHVsZURlZi5wcm92aWRlcnMpO1xuICAgIH1cbiAgICBpZiAobW9kdWxlRGVmLmRlY2xhcmF0aW9ucykge1xuICAgICAgdGhpcy5fZGVjbGFyYXRpb25zLnB1c2goLi4ubW9kdWxlRGVmLmRlY2xhcmF0aW9ucyk7XG4gICAgfVxuICAgIGlmIChtb2R1bGVEZWYuaW1wb3J0cykge1xuICAgICAgdGhpcy5faW1wb3J0cy5wdXNoKC4uLm1vZHVsZURlZi5pbXBvcnRzKTtcbiAgICB9XG4gICAgaWYgKG1vZHVsZURlZi5zY2hlbWFzKSB7XG4gICAgICB0aGlzLl9zY2hlbWFzLnB1c2goLi4ubW9kdWxlRGVmLnNjaGVtYXMpO1xuICAgIH1cbiAgICBpZiAobW9kdWxlRGVmLmFvdFN1bW1hcmllcykge1xuICAgICAgdGhpcy5fYW90U3VtbWFyaWVzLnB1c2gobW9kdWxlRGVmLmFvdFN1bW1hcmllcyk7XG4gICAgfVxuICB9XG5cbiAgY29tcGlsZUNvbXBvbmVudHMoKTogUHJvbWlzZTxhbnk+IHtcbiAgICBpZiAodGhpcy5fbW9kdWxlRmFjdG9yeSB8fCB0aGlzLl9pbnN0YW50aWF0ZWQpIHtcbiAgICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUobnVsbCk7XG4gICAgfVxuXG4gICAgY29uc3QgbW9kdWxlVHlwZSA9IHRoaXMuX2NyZWF0ZUNvbXBpbGVyQW5kTW9kdWxlKCk7XG4gICAgcmV0dXJuIHRoaXMuX2NvbXBpbGVyLmNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmMobW9kdWxlVHlwZSlcbiAgICAgICAgLnRoZW4oKG1vZHVsZUFuZENvbXBvbmVudEZhY3RvcmllcykgPT4ge1xuICAgICAgICAgIHRoaXMuX21vZHVsZUZhY3RvcnkgPSBtb2R1bGVBbmRDb21wb25lbnRGYWN0b3JpZXMubmdNb2R1bGVGYWN0b3J5O1xuICAgICAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgX2luaXRJZk5lZWRlZCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5faW5zdGFudGlhdGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmICghdGhpcy5fbW9kdWxlRmFjdG9yeSkge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgbW9kdWxlVHlwZSA9IHRoaXMuX2NyZWF0ZUNvbXBpbGVyQW5kTW9kdWxlKCk7XG4gICAgICAgIHRoaXMuX21vZHVsZUZhY3RvcnkgPVxuICAgICAgICAgICAgdGhpcy5fY29tcGlsZXIuY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHNTeW5jKG1vZHVsZVR5cGUpLm5nTW9kdWxlRmFjdG9yeTtcbiAgICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgY29uc3QgZXJyb3JDb21wVHlwZSA9IHRoaXMuX2NvbXBpbGVyLmdldENvbXBvbmVudEZyb21FcnJvcihlKTtcbiAgICAgICAgaWYgKGVycm9yQ29tcFR5cGUpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgICAgIGBUaGlzIHRlc3QgbW9kdWxlIHVzZXMgdGhlIGNvbXBvbmVudCAke3N0cmluZ2lmeShlcnJvckNvbXBUeXBlKX0gd2hpY2ggaXMgdXNpbmcgYSBcInRlbXBsYXRlVXJsXCIgb3IgXCJzdHlsZVVybHNcIiwgYnV0IHRoZXkgd2VyZSBuZXZlciBjb21waWxlZC4gYCArXG4gICAgICAgICAgICAgIGBQbGVhc2UgY2FsbCBcIlRlc3RCZWQuY29tcGlsZUNvbXBvbmVudHNcIiBiZWZvcmUgeW91ciB0ZXN0LmApO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRocm93IGU7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gICAgZm9yIChjb25zdCB7Y29tcG9uZW50LCB0ZW1wbGF0ZU9mfSBvZiB0aGlzLl90ZW1wbGF0ZU92ZXJyaWRlcykge1xuICAgICAgY29uc3QgY29tcEZhY3RvcnkgPSB0aGlzLl9jb21waWxlci5nZXRDb21wb25lbnRGYWN0b3J5KHRlbXBsYXRlT2YpO1xuICAgICAgb3ZlcnJpZGVDb21wb25lbnRWaWV3KGNvbXBvbmVudCwgY29tcEZhY3RvcnkpO1xuICAgIH1cblxuICAgIGNvbnN0IG5nWm9uZSA9IG5ldyBOZ1pvbmUoe2VuYWJsZUxvbmdTdGFja1RyYWNlOiB0cnVlfSk7XG4gICAgY29uc3QgcHJvdmlkZXJzOiBTdGF0aWNQcm92aWRlcltdID0gW3twcm92aWRlOiBOZ1pvbmUsIHVzZVZhbHVlOiBuZ1pvbmV9XTtcbiAgICBjb25zdCBuZ1pvbmVJbmplY3RvciA9IEluamVjdG9yLmNyZWF0ZSh7XG4gICAgICBwcm92aWRlcnM6IHByb3ZpZGVycyxcbiAgICAgIHBhcmVudDogdGhpcy5wbGF0Zm9ybS5pbmplY3RvcixcbiAgICAgIG5hbWU6IHRoaXMuX21vZHVsZUZhY3RvcnkubW9kdWxlVHlwZS5uYW1lXG4gICAgfSk7XG4gICAgdGhpcy5fbW9kdWxlUmVmID0gdGhpcy5fbW9kdWxlRmFjdG9yeS5jcmVhdGUobmdab25lSW5qZWN0b3IpO1xuICAgIC8vIEFwcGxpY2F0aW9uSW5pdFN0YXR1cy5ydW5Jbml0aWFsaXplcnMoKSBpcyBtYXJrZWQgQGludGVybmFsIHRvIGNvcmUuIFNvIGNhc3RpbmcgdG8gYW55XG4gICAgLy8gYmVmb3JlIGFjY2Vzc2luZyBpdC5cbiAgICAodGhpcy5fbW9kdWxlUmVmLmluamVjdG9yLmdldChBcHBsaWNhdGlvbkluaXRTdGF0dXMpIGFzIGFueSkucnVuSW5pdGlhbGl6ZXJzKCk7XG4gICAgdGhpcy5faW5zdGFudGlhdGVkID0gdHJ1ZTtcbiAgfVxuXG4gIHByaXZhdGUgX2NyZWF0ZUNvbXBpbGVyQW5kTW9kdWxlKCk6IFR5cGU8YW55PiB7XG4gICAgY29uc3QgcHJvdmlkZXJzID0gdGhpcy5fcHJvdmlkZXJzLmNvbmNhdChbe3Byb3ZpZGU6IFRlc3RCZWQsIHVzZVZhbHVlOiB0aGlzfV0pO1xuICAgIGNvbnN0IGRlY2xhcmF0aW9ucyA9XG4gICAgICAgIFsuLi50aGlzLl9kZWNsYXJhdGlvbnMsIC4uLnRoaXMuX3RlbXBsYXRlT3ZlcnJpZGVzLm1hcChlbnRyeSA9PiBlbnRyeS50ZW1wbGF0ZU9mKV07XG5cbiAgICBjb25zdCByb290U2NvcGVJbXBvcnRzID0gW107XG4gICAgY29uc3Qgcm9vdFByb3ZpZGVyT3ZlcnJpZGVzID0gdGhpcy5fcm9vdFByb3ZpZGVyT3ZlcnJpZGVzO1xuICAgIGlmICh0aGlzLl9pc1Jvb3QpIHtcbiAgICAgIEBOZ01vZHVsZSh7XG4gICAgICAgIHByb3ZpZGVyczogW1xuICAgICAgICAgIC4uLnJvb3RQcm92aWRlck92ZXJyaWRlcyxcbiAgICAgICAgXSxcbiAgICAgICAgaml0OiB0cnVlLFxuICAgICAgfSlcbiAgICAgIGNsYXNzIFJvb3RTY29wZU1vZHVsZSB7XG4gICAgICB9XG4gICAgICByb290U2NvcGVJbXBvcnRzLnB1c2goUm9vdFNjb3BlTW9kdWxlKTtcbiAgICB9XG4gICAgcHJvdmlkZXJzLnB1c2goe3Byb3ZpZGU6IEFQUF9ST09ULCB1c2VWYWx1ZTogdGhpcy5faXNSb290fSk7XG5cbiAgICBjb25zdCBpbXBvcnRzID0gW3Jvb3RTY29wZUltcG9ydHMsIHRoaXMubmdNb2R1bGUsIHRoaXMuX2ltcG9ydHNdO1xuICAgIGNvbnN0IHNjaGVtYXMgPSB0aGlzLl9zY2hlbWFzO1xuXG4gICAgQE5nTW9kdWxlKHtwcm92aWRlcnMsIGRlY2xhcmF0aW9ucywgaW1wb3J0cywgc2NoZW1hcywgaml0OiB0cnVlfSlcbiAgICBjbGFzcyBEeW5hbWljVGVzdE1vZHVsZSB7XG4gICAgfVxuXG4gICAgY29uc3QgY29tcGlsZXJGYWN0b3J5OiBUZXN0aW5nQ29tcGlsZXJGYWN0b3J5ID1cbiAgICAgICAgdGhpcy5wbGF0Zm9ybS5pbmplY3Rvci5nZXQoVGVzdGluZ0NvbXBpbGVyRmFjdG9yeSk7XG4gICAgdGhpcy5fY29tcGlsZXIgPSBjb21waWxlckZhY3RvcnkuY3JlYXRlVGVzdGluZ0NvbXBpbGVyKHRoaXMuX2NvbXBpbGVyT3B0aW9ucyk7XG4gICAgZm9yIChjb25zdCBzdW1tYXJ5IG9mIFt0aGlzLl90ZXN0RW52QW90U3VtbWFyaWVzLCAuLi50aGlzLl9hb3RTdW1tYXJpZXNdKSB7XG4gICAgICB0aGlzLl9jb21waWxlci5sb2FkQW90U3VtbWFyaWVzKHN1bW1hcnkpO1xuICAgIH1cbiAgICB0aGlzLl9tb2R1bGVPdmVycmlkZXMuZm9yRWFjaCgoZW50cnkpID0+IHRoaXMuX2NvbXBpbGVyLm92ZXJyaWRlTW9kdWxlKGVudHJ5WzBdLCBlbnRyeVsxXSkpO1xuICAgIHRoaXMuX2NvbXBvbmVudE92ZXJyaWRlcy5mb3JFYWNoKFxuICAgICAgICAoZW50cnkpID0+IHRoaXMuX2NvbXBpbGVyLm92ZXJyaWRlQ29tcG9uZW50KGVudHJ5WzBdLCBlbnRyeVsxXSkpO1xuICAgIHRoaXMuX2RpcmVjdGl2ZU92ZXJyaWRlcy5mb3JFYWNoKFxuICAgICAgICAoZW50cnkpID0+IHRoaXMuX2NvbXBpbGVyLm92ZXJyaWRlRGlyZWN0aXZlKGVudHJ5WzBdLCBlbnRyeVsxXSkpO1xuICAgIHRoaXMuX3BpcGVPdmVycmlkZXMuZm9yRWFjaCgoZW50cnkpID0+IHRoaXMuX2NvbXBpbGVyLm92ZXJyaWRlUGlwZShlbnRyeVswXSwgZW50cnlbMV0pKTtcbiAgICByZXR1cm4gRHluYW1pY1Rlc3RNb2R1bGU7XG4gIH1cblxuICBwcml2YXRlIF9hc3NlcnROb3RJbnN0YW50aWF0ZWQobWV0aG9kTmFtZTogc3RyaW5nLCBtZXRob2REZXNjcmlwdGlvbjogc3RyaW5nKSB7XG4gICAgaWYgKHRoaXMuX2luc3RhbnRpYXRlZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBDYW5ub3QgJHttZXRob2REZXNjcmlwdGlvbn0gd2hlbiB0aGUgdGVzdCBtb2R1bGUgaGFzIGFscmVhZHkgYmVlbiBpbnN0YW50aWF0ZWQuIGAgK1xuICAgICAgICAgIGBNYWtlIHN1cmUgeW91IGFyZSBub3QgdXNpbmcgXFxgaW5qZWN0XFxgIGJlZm9yZSBcXGAke21ldGhvZE5hbWV9XFxgLmApO1xuICAgIH1cbiAgfVxuXG4gIGdldDxUPih0b2tlbjogVHlwZTxUPnxJbmplY3Rpb25Ub2tlbjxUPiwgbm90Rm91bmRWYWx1ZT86IFQsIGZsYWdzPzogSW5qZWN0RmxhZ3MpOiBhbnk7XG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCBmcm9tIHY4LjAuMCB1c2UgVHlwZTxUPiBvciBJbmplY3Rpb25Ub2tlbjxUPlxuICAgKi9cbiAgZ2V0KHRva2VuOiBhbnksIG5vdEZvdW5kVmFsdWU/OiBhbnkpOiBhbnk7XG4gIGdldCh0b2tlbjogYW55LCBub3RGb3VuZFZhbHVlOiBhbnkgPSBJbmplY3Rvci5USFJPV19JRl9OT1RfRk9VTkQsXG4gICAgICBmbGFnczogSW5qZWN0RmxhZ3MgPSBJbmplY3RGbGFncy5EZWZhdWx0KTogYW55IHtcbiAgICB0aGlzLl9pbml0SWZOZWVkZWQoKTtcbiAgICBpZiAodG9rZW4gPT09IFRlc3RCZWQpIHtcbiAgICAgIHJldHVybiB0aGlzO1xuICAgIH1cbiAgICAvLyBUZXN0cyBjYW4gaW5qZWN0IHRoaW5ncyBmcm9tIHRoZSBuZyBtb2R1bGUgYW5kIGZyb20gdGhlIGNvbXBpbGVyLFxuICAgIC8vIGJ1dCB0aGUgbmcgbW9kdWxlIGNhbid0IGluamVjdCB0aGluZ3MgZnJvbSB0aGUgY29tcGlsZXIgYW5kIHZpY2UgdmVyc2EuXG4gICAgY29uc3QgcmVzdWx0ID0gdGhpcy5fbW9kdWxlUmVmLmluamVjdG9yLmdldCh0b2tlbiwgVU5ERUZJTkVELCBmbGFncyk7XG4gICAgcmV0dXJuIHJlc3VsdCA9PT0gVU5ERUZJTkVEID8gdGhpcy5fY29tcGlsZXIuaW5qZWN0b3IuZ2V0KHRva2VuLCBub3RGb3VuZFZhbHVlLCBmbGFncykgOiByZXN1bHQ7XG4gIH1cblxuICBleGVjdXRlKHRva2VuczogYW55W10sIGZuOiBGdW5jdGlvbiwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgdGhpcy5faW5pdElmTmVlZGVkKCk7XG4gICAgY29uc3QgcGFyYW1zID0gdG9rZW5zLm1hcCh0ID0+IHRoaXMuZ2V0KHQpKTtcbiAgICByZXR1cm4gZm4uYXBwbHkoY29udGV4dCwgcGFyYW1zKTtcbiAgfVxuXG4gIG92ZXJyaWRlTW9kdWxlKG5nTW9kdWxlOiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPE5nTW9kdWxlPik6IHZvaWQge1xuICAgIHRoaXMuX2Fzc2VydE5vdEluc3RhbnRpYXRlZCgnb3ZlcnJpZGVNb2R1bGUnLCAnb3ZlcnJpZGUgbW9kdWxlIG1ldGFkYXRhJyk7XG4gICAgdGhpcy5fbW9kdWxlT3ZlcnJpZGVzLnB1c2goW25nTW9kdWxlLCBvdmVycmlkZV0pO1xuICB9XG5cbiAgb3ZlcnJpZGVDb21wb25lbnQoY29tcG9uZW50OiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPENvbXBvbmVudD4pOiB2b2lkIHtcbiAgICB0aGlzLl9hc3NlcnROb3RJbnN0YW50aWF0ZWQoJ292ZXJyaWRlQ29tcG9uZW50JywgJ292ZXJyaWRlIGNvbXBvbmVudCBtZXRhZGF0YScpO1xuICAgIHRoaXMuX2NvbXBvbmVudE92ZXJyaWRlcy5wdXNoKFtjb21wb25lbnQsIG92ZXJyaWRlXSk7XG4gIH1cblxuICBvdmVycmlkZURpcmVjdGl2ZShkaXJlY3RpdmU6IFR5cGU8YW55Piwgb3ZlcnJpZGU6IE1ldGFkYXRhT3ZlcnJpZGU8RGlyZWN0aXZlPik6IHZvaWQge1xuICAgIHRoaXMuX2Fzc2VydE5vdEluc3RhbnRpYXRlZCgnb3ZlcnJpZGVEaXJlY3RpdmUnLCAnb3ZlcnJpZGUgZGlyZWN0aXZlIG1ldGFkYXRhJyk7XG4gICAgdGhpcy5fZGlyZWN0aXZlT3ZlcnJpZGVzLnB1c2goW2RpcmVjdGl2ZSwgb3ZlcnJpZGVdKTtcbiAgfVxuXG4gIG92ZXJyaWRlUGlwZShwaXBlOiBUeXBlPGFueT4sIG92ZXJyaWRlOiBNZXRhZGF0YU92ZXJyaWRlPFBpcGU+KTogdm9pZCB7XG4gICAgdGhpcy5fYXNzZXJ0Tm90SW5zdGFudGlhdGVkKCdvdmVycmlkZVBpcGUnLCAnb3ZlcnJpZGUgcGlwZSBtZXRhZGF0YScpO1xuICAgIHRoaXMuX3BpcGVPdmVycmlkZXMucHVzaChbcGlwZSwgb3ZlcnJpZGVdKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBPdmVyd3JpdGVzIGFsbCBwcm92aWRlcnMgZm9yIHRoZSBnaXZlbiB0b2tlbiB3aXRoIHRoZSBnaXZlbiBwcm92aWRlciBkZWZpbml0aW9uLlxuICAgKi9cbiAgb3ZlcnJpZGVQcm92aWRlcih0b2tlbjogYW55LCBwcm92aWRlcjoge1xuICAgIHVzZUZhY3Rvcnk6IEZ1bmN0aW9uLFxuICAgIGRlcHM6IGFueVtdLFxuICB9KTogdm9pZDtcbiAgb3ZlcnJpZGVQcm92aWRlcih0b2tlbjogYW55LCBwcm92aWRlcjoge3VzZVZhbHVlOiBhbnk7fSk6IHZvaWQ7XG4gIG92ZXJyaWRlUHJvdmlkZXIodG9rZW46IGFueSwgcHJvdmlkZXI6IHt1c2VGYWN0b3J5PzogRnVuY3Rpb24sIHVzZVZhbHVlPzogYW55LCBkZXBzPzogYW55W119KTpcbiAgICAgIHZvaWQge1xuICAgIHRoaXMub3ZlcnJpZGVQcm92aWRlckltcGwodG9rZW4sIHByb3ZpZGVyKTtcbiAgfVxuXG4gIHByaXZhdGUgb3ZlcnJpZGVQcm92aWRlckltcGwoXG4gICAgICB0b2tlbjogYW55LCBwcm92aWRlcjoge1xuICAgICAgICB1c2VGYWN0b3J5PzogRnVuY3Rpb24sXG4gICAgICAgIHVzZVZhbHVlPzogYW55LFxuICAgICAgICBkZXBzPzogYW55W10sXG4gICAgICB9LFxuICAgICAgZGVwcmVjYXRlZCA9IGZhbHNlKTogdm9pZCB7XG4gICAgbGV0IGRlZjogybXJtUluamVjdGFibGVEZWY8YW55PnxudWxsID0gbnVsbDtcbiAgICBpZiAodHlwZW9mIHRva2VuICE9PSAnc3RyaW5nJyAmJiAoZGVmID0gZ2V0SW5qZWN0YWJsZURlZih0b2tlbikpICYmIGRlZi5wcm92aWRlZEluID09PSAncm9vdCcpIHtcbiAgICAgIGlmIChwcm92aWRlci51c2VGYWN0b3J5KSB7XG4gICAgICAgIHRoaXMuX3Jvb3RQcm92aWRlck92ZXJyaWRlcy5wdXNoKFxuICAgICAgICAgICAge3Byb3ZpZGU6IHRva2VuLCB1c2VGYWN0b3J5OiBwcm92aWRlci51c2VGYWN0b3J5LCBkZXBzOiBwcm92aWRlci5kZXBzIHx8IFtdfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLl9yb290UHJvdmlkZXJPdmVycmlkZXMucHVzaCh7cHJvdmlkZTogdG9rZW4sIHVzZVZhbHVlOiBwcm92aWRlci51c2VWYWx1ZX0pO1xuICAgICAgfVxuICAgIH1cbiAgICBsZXQgZmxhZ3M6IE5vZGVGbGFncyA9IDA7XG4gICAgbGV0IHZhbHVlOiBhbnk7XG4gICAgaWYgKHByb3ZpZGVyLnVzZUZhY3RvcnkpIHtcbiAgICAgIGZsYWdzIHw9IE5vZGVGbGFncy5UeXBlRmFjdG9yeVByb3ZpZGVyO1xuICAgICAgdmFsdWUgPSBwcm92aWRlci51c2VGYWN0b3J5O1xuICAgIH0gZWxzZSB7XG4gICAgICBmbGFncyB8PSBOb2RlRmxhZ3MuVHlwZVZhbHVlUHJvdmlkZXI7XG4gICAgICB2YWx1ZSA9IHByb3ZpZGVyLnVzZVZhbHVlO1xuICAgIH1cbiAgICBjb25zdCBkZXBzID0gKHByb3ZpZGVyLmRlcHMgfHwgW10pLm1hcCgoZGVwKSA9PiB7XG4gICAgICBsZXQgZGVwRmxhZ3M6IERlcEZsYWdzID0gRGVwRmxhZ3MuTm9uZTtcbiAgICAgIGxldCBkZXBUb2tlbjogYW55O1xuICAgICAgaWYgKEFycmF5LmlzQXJyYXkoZGVwKSkge1xuICAgICAgICBkZXAuZm9yRWFjaCgoZW50cnk6IGFueSkgPT4ge1xuICAgICAgICAgIGlmIChlbnRyeSBpbnN0YW5jZW9mIE9wdGlvbmFsKSB7XG4gICAgICAgICAgICBkZXBGbGFncyB8PSBEZXBGbGFncy5PcHRpb25hbDtcbiAgICAgICAgICB9IGVsc2UgaWYgKGVudHJ5IGluc3RhbmNlb2YgU2tpcFNlbGYpIHtcbiAgICAgICAgICAgIGRlcEZsYWdzIHw9IERlcEZsYWdzLlNraXBTZWxmO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBkZXBUb2tlbiA9IGVudHJ5O1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBkZXBUb2tlbiA9IGRlcDtcbiAgICAgIH1cbiAgICAgIHJldHVybiBbZGVwRmxhZ3MsIGRlcFRva2VuXTtcbiAgICB9KTtcbiAgICBvdmVycmlkZVByb3ZpZGVyKHt0b2tlbiwgZmxhZ3MsIGRlcHMsIHZhbHVlLCBkZXByZWNhdGVkQmVoYXZpb3I6IGRlcHJlY2F0ZWR9KTtcbiAgfVxuXG4gIG92ZXJyaWRlVGVtcGxhdGVVc2luZ1Rlc3RpbmdNb2R1bGUoY29tcG9uZW50OiBUeXBlPGFueT4sIHRlbXBsYXRlOiBzdHJpbmcpIHtcbiAgICB0aGlzLl9hc3NlcnROb3RJbnN0YW50aWF0ZWQoJ292ZXJyaWRlVGVtcGxhdGVVc2luZ1Rlc3RpbmdNb2R1bGUnLCAnb3ZlcnJpZGUgdGVtcGxhdGUnKTtcblxuICAgIEBDb21wb25lbnQoe3NlbGVjdG9yOiAnZW1wdHknLCB0ZW1wbGF0ZSwgaml0OiB0cnVlfSlcbiAgICBjbGFzcyBPdmVycmlkZUNvbXBvbmVudCB7XG4gICAgfVxuXG4gICAgdGhpcy5fdGVtcGxhdGVPdmVycmlkZXMucHVzaCh7Y29tcG9uZW50LCB0ZW1wbGF0ZU9mOiBPdmVycmlkZUNvbXBvbmVudH0pO1xuICB9XG5cbiAgY3JlYXRlQ29tcG9uZW50PFQ+KGNvbXBvbmVudDogVHlwZTxUPik6IENvbXBvbmVudEZpeHR1cmU8VD4ge1xuICAgIHRoaXMuX2luaXRJZk5lZWRlZCgpO1xuICAgIGNvbnN0IGNvbXBvbmVudEZhY3RvcnkgPSB0aGlzLl9jb21waWxlci5nZXRDb21wb25lbnRGYWN0b3J5KGNvbXBvbmVudCk7XG5cbiAgICBpZiAoIWNvbXBvbmVudEZhY3RvcnkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgQ2Fubm90IGNyZWF0ZSB0aGUgY29tcG9uZW50ICR7c3RyaW5naWZ5KGNvbXBvbmVudCl9IGFzIGl0IHdhcyBub3QgaW1wb3J0ZWQgaW50byB0aGUgdGVzdGluZyBtb2R1bGUhYCk7XG4gICAgfVxuXG4gICAgLy8gVE9ETzogRG9uJ3QgY2FzdCBhcyBgYW55YCwgcHJvcGVyIHR5cGUgaXMgYm9vbGVhbltdXG4gICAgY29uc3Qgbm9OZ1pvbmUgPSB0aGlzLmdldChDb21wb25lbnRGaXh0dXJlTm9OZ1pvbmUgYXMgYW55LCBmYWxzZSk7XG4gICAgLy8gVE9ETzogRG9uJ3QgY2FzdCBhcyBgYW55YCwgcHJvcGVyIHR5cGUgaXMgYm9vbGVhbltdXG4gICAgY29uc3QgYXV0b0RldGVjdDogYm9vbGVhbiA9IHRoaXMuZ2V0KENvbXBvbmVudEZpeHR1cmVBdXRvRGV0ZWN0IGFzIGFueSwgZmFsc2UpO1xuICAgIGNvbnN0IG5nWm9uZTogTmdab25lfG51bGwgPSBub05nWm9uZSA/IG51bGwgOiB0aGlzLmdldChOZ1pvbmUgYXMgVHlwZTxOZ1pvbmV8bnVsbD4sIG51bGwpO1xuICAgIGNvbnN0IHRlc3RDb21wb25lbnRSZW5kZXJlcjogVGVzdENvbXBvbmVudFJlbmRlcmVyID0gdGhpcy5nZXQoVGVzdENvbXBvbmVudFJlbmRlcmVyKTtcbiAgICBjb25zdCByb290RWxJZCA9IGByb290JHtfbmV4dFJvb3RFbGVtZW50SWQrK31gO1xuICAgIHRlc3RDb21wb25lbnRSZW5kZXJlci5pbnNlcnRSb290RWxlbWVudChyb290RWxJZCk7XG5cbiAgICBjb25zdCBpbml0Q29tcG9uZW50ID0gKCkgPT4ge1xuICAgICAgY29uc3QgY29tcG9uZW50UmVmID1cbiAgICAgICAgICBjb21wb25lbnRGYWN0b3J5LmNyZWF0ZShJbmplY3Rvci5OVUxMLCBbXSwgYCMke3Jvb3RFbElkfWAsIHRoaXMuX21vZHVsZVJlZik7XG4gICAgICByZXR1cm4gbmV3IENvbXBvbmVudEZpeHR1cmU8VD4oY29tcG9uZW50UmVmLCBuZ1pvbmUsIGF1dG9EZXRlY3QpO1xuICAgIH07XG5cbiAgICBjb25zdCBmaXh0dXJlID0gIW5nWm9uZSA/IGluaXRDb21wb25lbnQoKSA6IG5nWm9uZS5ydW4oaW5pdENvbXBvbmVudCk7XG4gICAgdGhpcy5fYWN0aXZlRml4dHVyZXMucHVzaChmaXh0dXJlKTtcbiAgICByZXR1cm4gZml4dHVyZTtcbiAgfVxufVxuXG4vKipcbiAqIEBkZXNjcmlwdGlvblxuICogQ29uZmlndXJlcyBhbmQgaW5pdGlhbGl6ZXMgZW52aXJvbm1lbnQgZm9yIHVuaXQgdGVzdGluZyBhbmQgcHJvdmlkZXMgbWV0aG9kcyBmb3JcbiAqIGNyZWF0aW5nIGNvbXBvbmVudHMgYW5kIHNlcnZpY2VzIGluIHVuaXQgdGVzdHMuXG4gKlxuICogYFRlc3RCZWRgIGlzIHRoZSBwcmltYXJ5IGFwaSBmb3Igd3JpdGluZyB1bml0IHRlc3RzIGZvciBBbmd1bGFyIGFwcGxpY2F0aW9ucyBhbmQgbGlicmFyaWVzLlxuICpcbiAqIE5vdGU6IFVzZSBgVGVzdEJlZGAgaW4gdGVzdHMuIEl0IHdpbGwgYmUgc2V0IHRvIGVpdGhlciBgVGVzdEJlZFZpZXdFbmdpbmVgIG9yIGBUZXN0QmVkUmVuZGVyM2BcbiAqIGFjY29yZGluZyB0byB0aGUgY29tcGlsZXIgdXNlZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBUZXN0QmVkOiBUZXN0QmVkU3RhdGljID1cbiAgICBpdnlFbmFibGVkID8gVGVzdEJlZFJlbmRlcjMgYXMgYW55IGFzIFRlc3RCZWRTdGF0aWMgOiBUZXN0QmVkVmlld0VuZ2luZSBhcyBhbnkgYXMgVGVzdEJlZFN0YXRpYztcblxuLyoqXG4gKiBSZXR1cm5zIGEgc2luZ2xldG9uIG9mIHRoZSBhcHBsaWNhYmxlIGBUZXN0QmVkYC5cbiAqXG4gKiBJdCB3aWxsIGJlIGVpdGhlciBhbiBpbnN0YW5jZSBvZiBgVGVzdEJlZFZpZXdFbmdpbmVgIG9yIGBUZXN0QmVkUmVuZGVyM2AuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY29uc3QgZ2V0VGVzdEJlZDogKCkgPT4gVGVzdEJlZCA9IGl2eUVuYWJsZWQgPyBfZ2V0VGVzdEJlZFJlbmRlcjMgOiBfZ2V0VGVzdEJlZFZpZXdFbmdpbmU7XG5cbmxldCB0ZXN0QmVkOiBUZXN0QmVkVmlld0VuZ2luZTtcblxuZnVuY3Rpb24gX2dldFRlc3RCZWRWaWV3RW5naW5lKCk6IFRlc3RCZWRWaWV3RW5naW5lIHtcbiAgcmV0dXJuIHRlc3RCZWQgPSB0ZXN0QmVkIHx8IG5ldyBUZXN0QmVkVmlld0VuZ2luZSgpO1xufVxuXG4vKipcbiAqIEFsbG93cyBpbmplY3RpbmcgZGVwZW5kZW5jaWVzIGluIGBiZWZvcmVFYWNoKClgIGFuZCBgaXQoKWAuXG4gKlxuICogRXhhbXBsZTpcbiAqXG4gKiBgYGBcbiAqIGJlZm9yZUVhY2goaW5qZWN0KFtEZXBlbmRlbmN5LCBBQ2xhc3NdLCAoZGVwLCBvYmplY3QpID0+IHtcbiAqICAgLy8gc29tZSBjb2RlIHRoYXQgdXNlcyBgZGVwYCBhbmQgYG9iamVjdGBcbiAqICAgLy8gLi4uXG4gKiB9KSk7XG4gKlxuICogaXQoJy4uLicsIGluamVjdChbQUNsYXNzXSwgKG9iamVjdCkgPT4ge1xuICogICBvYmplY3QuZG9Tb21ldGhpbmcoKTtcbiAqICAgZXhwZWN0KC4uLik7XG4gKiB9KVxuICogYGBgXG4gKlxuICogTm90ZXM6XG4gKiAtIGluamVjdCBpcyBjdXJyZW50bHkgYSBmdW5jdGlvbiBiZWNhdXNlIG9mIHNvbWUgVHJhY2V1ciBsaW1pdGF0aW9uIHRoZSBzeW50YXggc2hvdWxkXG4gKiBldmVudHVhbGx5XG4gKiAgIGJlY29tZXMgYGl0KCcuLi4nLCBASW5qZWN0IChvYmplY3Q6IEFDbGFzcywgYXN5bmM6IEFzeW5jVGVzdENvbXBsZXRlcikgPT4geyAuLi4gfSk7YFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGluamVjdCh0b2tlbnM6IGFueVtdLCBmbjogRnVuY3Rpb24pOiAoKSA9PiBhbnkge1xuICBjb25zdCB0ZXN0QmVkID0gZ2V0VGVzdEJlZCgpO1xuICBpZiAodG9rZW5zLmluZGV4T2YoQXN5bmNUZXN0Q29tcGxldGVyKSA+PSAwKSB7XG4gICAgLy8gTm90IHVzaW5nIGFuIGFycm93IGZ1bmN0aW9uIHRvIHByZXNlcnZlIGNvbnRleHQgcGFzc2VkIGZyb20gY2FsbCBzaXRlXG4gICAgcmV0dXJuIGZ1bmN0aW9uKHRoaXM6IHVua25vd24pIHtcbiAgICAgIC8vIFJldHVybiBhbiBhc3luYyB0ZXN0IG1ldGhvZCB0aGF0IHJldHVybnMgYSBQcm9taXNlIGlmIEFzeW5jVGVzdENvbXBsZXRlciBpcyBvbmUgb2ZcbiAgICAgIC8vIHRoZSBpbmplY3RlZCB0b2tlbnMuXG4gICAgICByZXR1cm4gdGVzdEJlZC5jb21waWxlQ29tcG9uZW50cygpLnRoZW4oKCkgPT4ge1xuICAgICAgICBjb25zdCBjb21wbGV0ZXI6IEFzeW5jVGVzdENvbXBsZXRlciA9IHRlc3RCZWQuZ2V0KEFzeW5jVGVzdENvbXBsZXRlcik7XG4gICAgICAgIHRlc3RCZWQuZXhlY3V0ZSh0b2tlbnMsIGZuLCB0aGlzKTtcbiAgICAgICAgcmV0dXJuIGNvbXBsZXRlci5wcm9taXNlO1xuICAgICAgfSk7XG4gICAgfTtcbiAgfSBlbHNlIHtcbiAgICAvLyBOb3QgdXNpbmcgYW4gYXJyb3cgZnVuY3Rpb24gdG8gcHJlc2VydmUgY29udGV4dCBwYXNzZWQgZnJvbSBjYWxsIHNpdGVcbiAgICByZXR1cm4gZnVuY3Rpb24odGhpczogdW5rbm93bikgeyByZXR1cm4gdGVzdEJlZC5leGVjdXRlKHRva2VucywgZm4sIHRoaXMpOyB9O1xuICB9XG59XG5cbi8qKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgSW5qZWN0U2V0dXBXcmFwcGVyIHtcbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfbW9kdWxlRGVmOiAoKSA9PiBUZXN0TW9kdWxlTWV0YWRhdGEpIHt9XG5cbiAgcHJpdmF0ZSBfYWRkTW9kdWxlKCkge1xuICAgIGNvbnN0IG1vZHVsZURlZiA9IHRoaXMuX21vZHVsZURlZigpO1xuICAgIGlmIChtb2R1bGVEZWYpIHtcbiAgICAgIGdldFRlc3RCZWQoKS5jb25maWd1cmVUZXN0aW5nTW9kdWxlKG1vZHVsZURlZik7XG4gICAgfVxuICB9XG5cbiAgaW5qZWN0KHRva2VuczogYW55W10sIGZuOiBGdW5jdGlvbik6ICgpID0+IGFueSB7XG4gICAgY29uc3Qgc2VsZiA9IHRoaXM7XG4gICAgLy8gTm90IHVzaW5nIGFuIGFycm93IGZ1bmN0aW9uIHRvIHByZXNlcnZlIGNvbnRleHQgcGFzc2VkIGZyb20gY2FsbCBzaXRlXG4gICAgcmV0dXJuIGZ1bmN0aW9uKHRoaXM6IHVua25vd24pIHtcbiAgICAgIHNlbGYuX2FkZE1vZHVsZSgpO1xuICAgICAgcmV0dXJuIGluamVjdCh0b2tlbnMsIGZuKS5jYWxsKHRoaXMpO1xuICAgIH07XG4gIH1cbn1cblxuLyoqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiB3aXRoTW9kdWxlKG1vZHVsZURlZjogVGVzdE1vZHVsZU1ldGFkYXRhKTogSW5qZWN0U2V0dXBXcmFwcGVyO1xuZXhwb3J0IGZ1bmN0aW9uIHdpdGhNb2R1bGUobW9kdWxlRGVmOiBUZXN0TW9kdWxlTWV0YWRhdGEsIGZuOiBGdW5jdGlvbik6ICgpID0+IGFueTtcbmV4cG9ydCBmdW5jdGlvbiB3aXRoTW9kdWxlKG1vZHVsZURlZjogVGVzdE1vZHVsZU1ldGFkYXRhLCBmbj86IEZ1bmN0aW9uIHwgbnVsbCk6ICgoKSA9PiBhbnkpfFxuICAgIEluamVjdFNldHVwV3JhcHBlciB7XG4gIGlmIChmbikge1xuICAgIC8vIE5vdCB1c2luZyBhbiBhcnJvdyBmdW5jdGlvbiB0byBwcmVzZXJ2ZSBjb250ZXh0IHBhc3NlZCBmcm9tIGNhbGwgc2l0ZVxuICAgIHJldHVybiBmdW5jdGlvbih0aGlzOiB1bmtub3duKSB7XG4gICAgICBjb25zdCB0ZXN0QmVkID0gZ2V0VGVzdEJlZCgpO1xuICAgICAgaWYgKG1vZHVsZURlZikge1xuICAgICAgICB0ZXN0QmVkLmNvbmZpZ3VyZVRlc3RpbmdNb2R1bGUobW9kdWxlRGVmKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBmbi5hcHBseSh0aGlzKTtcbiAgICB9O1xuICB9XG4gIHJldHVybiBuZXcgSW5qZWN0U2V0dXBXcmFwcGVyKCgpID0+IG1vZHVsZURlZik7XG59XG4iXX0=