/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { getUrlScheme, syntaxError, Identifiers, JitCompiler, ProviderMeta, CompileReflector, ResourceLoader, JitSummaryResolver, SummaryResolver, Lexer, Parser, HtmlParser, I18NHtmlParser, CompilerConfig, TemplateParser, ElementSchemaRegistry, JitEvaluator, DirectiveNormalizer, UrlResolver, CompileMetadataResolver, NgModuleResolver, DirectiveResolver, PipeResolver, StaticSymbolCache, StyleCompiler, ViewCompiler, NgModuleCompiler, DomElementSchemaRegistry } from '@angular/compiler';
import { ɵReflectionCapabilities, ɵstringify, ANALYZE_FOR_ENTRY_COMPONENTS, ElementRef, NgModuleRef, ViewContainerRef, ChangeDetectorRef, Renderer2, QueryList, TemplateRef, ɵCodegenComponentFactoryResolver, ComponentFactoryResolver, ComponentFactory, ComponentRef, NgModuleFactory, ɵcmf, ɵmod, ɵmpd, ɵregisterModuleFactory, Injector, ViewEncapsulation, ChangeDetectionStrategy, SecurityContext, LOCALE_ID, TRANSLATIONS_FORMAT, ɵinlineInterpolate, ɵinterpolate, ɵEMPTY_ARRAY, ɵEMPTY_MAP, Renderer, ɵvid, ɵeld, ɵand, ɵted, ɵdid, ɵprd, ɵqud, ɵpad, ɵpod, ɵppd, ɵpid, ɵnov, ɵncd, ɵunv, ɵcrt, ɵccf, InjectionToken, PACKAGE_ROOT_URL, Compiler, ɵConsole, MissingTranslationStrategy, Optional, Inject, TRANSLATIONS, isDevMode, createPlatformFactory, platformCore, COMPILER_OPTIONS, CompilerFactory, Injectable, PLATFORM_ID, ɵglobal, Version } from '@angular/core';
import { __spread, __extends, __decorate } from 'tslib';
import { ɵPLATFORM_BROWSER_ID } from '@angular/common';
import { ɵINTERNAL_BROWSER_PLATFORM_PROVIDERS } from '@angular/platform-browser';

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
var MODULE_SUFFIX = '';
var builtinExternalReferences = createBuiltinExternalReferencesMap();
var JitReflector = /** @class */ (function () {
    function JitReflector() {
        this.reflectionCapabilities = new ɵReflectionCapabilities();
    }
    JitReflector.prototype.componentModuleUrl = function (type, cmpMetadata) {
        var moduleId = cmpMetadata.moduleId;
        if (typeof moduleId === 'string') {
            var scheme = getUrlScheme(moduleId);
            return scheme ? moduleId : "package:" + moduleId + MODULE_SUFFIX;
        }
        else if (moduleId !== null && moduleId !== void 0) {
            throw syntaxError("moduleId should be a string in \"" + ɵstringify(type) + "\". See https://goo.gl/wIDDiL for more information.\n" +
                "If you're using Webpack you should inline the template and the styles, see https://goo.gl/X2J8zc.");
        }
        return "./" + ɵstringify(type);
    };
    JitReflector.prototype.parameters = function (typeOrFunc) {
        return this.reflectionCapabilities.parameters(typeOrFunc);
    };
    JitReflector.prototype.tryAnnotations = function (typeOrFunc) { return this.annotations(typeOrFunc); };
    JitReflector.prototype.annotations = function (typeOrFunc) {
        return this.reflectionCapabilities.annotations(typeOrFunc);
    };
    JitReflector.prototype.shallowAnnotations = function (typeOrFunc) {
        throw new Error('Not supported in JIT mode');
    };
    JitReflector.prototype.propMetadata = function (typeOrFunc) {
        return this.reflectionCapabilities.propMetadata(typeOrFunc);
    };
    JitReflector.prototype.hasLifecycleHook = function (type, lcProperty) {
        return this.reflectionCapabilities.hasLifecycleHook(type, lcProperty);
    };
    JitReflector.prototype.guards = function (type) { return this.reflectionCapabilities.guards(type); };
    JitReflector.prototype.resolveExternalReference = function (ref) {
        return builtinExternalReferences.get(ref) || ref.runtime;
    };
    return JitReflector;
}());
function createBuiltinExternalReferencesMap() {
    var map = new Map();
    map.set(Identifiers.ANALYZE_FOR_ENTRY_COMPONENTS, ANALYZE_FOR_ENTRY_COMPONENTS);
    map.set(Identifiers.ElementRef, ElementRef);
    map.set(Identifiers.NgModuleRef, NgModuleRef);
    map.set(Identifiers.ViewContainerRef, ViewContainerRef);
    map.set(Identifiers.ChangeDetectorRef, ChangeDetectorRef);
    map.set(Identifiers.Renderer2, Renderer2);
    map.set(Identifiers.QueryList, QueryList);
    map.set(Identifiers.TemplateRef, TemplateRef);
    map.set(Identifiers.CodegenComponentFactoryResolver, ɵCodegenComponentFactoryResolver);
    map.set(Identifiers.ComponentFactoryResolver, ComponentFactoryResolver);
    map.set(Identifiers.ComponentFactory, ComponentFactory);
    map.set(Identifiers.ComponentRef, ComponentRef);
    map.set(Identifiers.NgModuleFactory, NgModuleFactory);
    map.set(Identifiers.createModuleFactory, ɵcmf);
    map.set(Identifiers.moduleDef, ɵmod);
    map.set(Identifiers.moduleProviderDef, ɵmpd);
    map.set(Identifiers.RegisterModuleFactoryFn, ɵregisterModuleFactory);
    map.set(Identifiers.Injector, Injector);
    map.set(Identifiers.ViewEncapsulation, ViewEncapsulation);
    map.set(Identifiers.ChangeDetectionStrategy, ChangeDetectionStrategy);
    map.set(Identifiers.SecurityContext, SecurityContext);
    map.set(Identifiers.LOCALE_ID, LOCALE_ID);
    map.set(Identifiers.TRANSLATIONS_FORMAT, TRANSLATIONS_FORMAT);
    map.set(Identifiers.inlineInterpolate, ɵinlineInterpolate);
    map.set(Identifiers.interpolate, ɵinterpolate);
    map.set(Identifiers.EMPTY_ARRAY, ɵEMPTY_ARRAY);
    map.set(Identifiers.EMPTY_MAP, ɵEMPTY_MAP);
    map.set(Identifiers.Renderer, Renderer);
    map.set(Identifiers.viewDef, ɵvid);
    map.set(Identifiers.elementDef, ɵeld);
    map.set(Identifiers.anchorDef, ɵand);
    map.set(Identifiers.textDef, ɵted);
    map.set(Identifiers.directiveDef, ɵdid);
    map.set(Identifiers.providerDef, ɵprd);
    map.set(Identifiers.queryDef, ɵqud);
    map.set(Identifiers.pureArrayDef, ɵpad);
    map.set(Identifiers.pureObjectDef, ɵpod);
    map.set(Identifiers.purePipeDef, ɵppd);
    map.set(Identifiers.pipeDef, ɵpid);
    map.set(Identifiers.nodeValue, ɵnov);
    map.set(Identifiers.ngContentDef, ɵncd);
    map.set(Identifiers.unwrapValue, ɵunv);
    map.set(Identifiers.createRendererType2, ɵcrt);
    map.set(Identifiers.createComponentFactory, ɵccf);
    return map;
}

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
var ERROR_COLLECTOR_TOKEN = new InjectionToken('ErrorCollector');
/**
 * A default provider for {@link PACKAGE_ROOT_URL} that maps to '/'.
 */
var DEFAULT_PACKAGE_URL_PROVIDER = {
    provide: PACKAGE_ROOT_URL,
    useValue: '/'
};
var _NO_RESOURCE_LOADER = {
    get: function (url) {
        throw new Error("No ResourceLoader implementation has been provided. Can't read the url \"" + url + "\"");
    }
};
var baseHtmlParser = new InjectionToken('HtmlParser');
var CompilerImpl = /** @class */ (function () {
    function CompilerImpl(injector, _metadataResolver, templateParser, styleCompiler, viewCompiler, ngModuleCompiler, summaryResolver, compileReflector, jitEvaluator, compilerConfig, console) {
        this._metadataResolver = _metadataResolver;
        this._delegate = new JitCompiler(_metadataResolver, templateParser, styleCompiler, viewCompiler, ngModuleCompiler, summaryResolver, compileReflector, jitEvaluator, compilerConfig, console, this.getExtraNgModuleProviders.bind(this));
        this.injector = injector;
    }
    CompilerImpl.prototype.getExtraNgModuleProviders = function () {
        return [this._metadataResolver.getProviderMetadata(new ProviderMeta(Compiler, { useValue: this }))];
    };
    CompilerImpl.prototype.compileModuleSync = function (moduleType) {
        return this._delegate.compileModuleSync(moduleType);
    };
    CompilerImpl.prototype.compileModuleAsync = function (moduleType) {
        return this._delegate.compileModuleAsync(moduleType);
    };
    CompilerImpl.prototype.compileModuleAndAllComponentsSync = function (moduleType) {
        var result = this._delegate.compileModuleAndAllComponentsSync(moduleType);
        return {
            ngModuleFactory: result.ngModuleFactory,
            componentFactories: result.componentFactories,
        };
    };
    CompilerImpl.prototype.compileModuleAndAllComponentsAsync = function (moduleType) {
        return this._delegate.compileModuleAndAllComponentsAsync(moduleType)
            .then(function (result) { return ({
            ngModuleFactory: result.ngModuleFactory,
            componentFactories: result.componentFactories,
        }); });
    };
    CompilerImpl.prototype.loadAotSummaries = function (summaries) { this._delegate.loadAotSummaries(summaries); };
    CompilerImpl.prototype.hasAotSummary = function (ref) { return this._delegate.hasAotSummary(ref); };
    CompilerImpl.prototype.getComponentFactory = function (component) {
        return this._delegate.getComponentFactory(component);
    };
    CompilerImpl.prototype.clearCache = function () { this._delegate.clearCache(); };
    CompilerImpl.prototype.clearCacheFor = function (type) { this._delegate.clearCacheFor(type); };
    CompilerImpl.prototype.getModuleId = function (moduleType) {
        var meta = this._metadataResolver.getNgModuleMetadata(moduleType);
        return meta && meta.id || undefined;
    };
    return CompilerImpl;
}());
/**
 * A set of providers that provide `JitCompiler` and its dependencies to use for
 * template compilation.
 */
var COMPILER_PROVIDERS = [
    { provide: CompileReflector, useValue: new JitReflector() },
    { provide: ResourceLoader, useValue: _NO_RESOURCE_LOADER },
    { provide: JitSummaryResolver, deps: [] },
    { provide: SummaryResolver, useExisting: JitSummaryResolver },
    { provide: ɵConsole, deps: [] },
    { provide: Lexer, deps: [] },
    { provide: Parser, deps: [Lexer] },
    {
        provide: baseHtmlParser,
        useClass: HtmlParser,
        deps: [],
    },
    {
        provide: I18NHtmlParser,
        useFactory: function (parser, translations, format, config, console) {
            translations = translations || '';
            var missingTranslation = translations ? config.missingTranslation : MissingTranslationStrategy.Ignore;
            return new I18NHtmlParser(parser, translations, format, missingTranslation, console);
        },
        deps: [
            baseHtmlParser,
            [new Optional(), new Inject(TRANSLATIONS)],
            [new Optional(), new Inject(TRANSLATIONS_FORMAT)],
            [CompilerConfig],
            [ɵConsole],
        ]
    },
    {
        provide: HtmlParser,
        useExisting: I18NHtmlParser,
    },
    {
        provide: TemplateParser, deps: [CompilerConfig, CompileReflector,
            Parser, ElementSchemaRegistry,
            I18NHtmlParser, ɵConsole]
    },
    { provide: JitEvaluator, useClass: JitEvaluator, deps: [] },
    { provide: DirectiveNormalizer, deps: [ResourceLoader, UrlResolver, HtmlParser, CompilerConfig] },
    { provide: CompileMetadataResolver, deps: [CompilerConfig, HtmlParser, NgModuleResolver,
            DirectiveResolver, PipeResolver,
            SummaryResolver,
            ElementSchemaRegistry,
            DirectiveNormalizer, ɵConsole,
            [Optional, StaticSymbolCache],
            CompileReflector,
            [Optional, ERROR_COLLECTOR_TOKEN]] },
    DEFAULT_PACKAGE_URL_PROVIDER,
    { provide: StyleCompiler, deps: [UrlResolver] },
    { provide: ViewCompiler, deps: [CompileReflector] },
    { provide: NgModuleCompiler, deps: [CompileReflector] },
    { provide: CompilerConfig, useValue: new CompilerConfig() },
    { provide: Compiler, useClass: CompilerImpl, deps: [Injector, CompileMetadataResolver,
            TemplateParser, StyleCompiler,
            ViewCompiler, NgModuleCompiler,
            SummaryResolver, CompileReflector, JitEvaluator, CompilerConfig,
            ɵConsole] },
    { provide: DomElementSchemaRegistry, deps: [] },
    { provide: ElementSchemaRegistry, useExisting: DomElementSchemaRegistry },
    { provide: UrlResolver, deps: [PACKAGE_ROOT_URL] },
    { provide: DirectiveResolver, deps: [CompileReflector] },
    { provide: PipeResolver, deps: [CompileReflector] },
    { provide: NgModuleResolver, deps: [CompileReflector] },
];
/**
 * @publicApi
 */
var JitCompilerFactory = /** @class */ (function () {
    /* @internal */
    function JitCompilerFactory(defaultOptions) {
        var compilerOptions = {
            useJit: true,
            defaultEncapsulation: ViewEncapsulation.Emulated,
            missingTranslation: MissingTranslationStrategy.Warning,
        };
        this._defaultOptions = __spread([compilerOptions], defaultOptions);
    }
    JitCompilerFactory.prototype.createCompiler = function (options) {
        if (options === void 0) { options = []; }
        var opts = _mergeOptions(this._defaultOptions.concat(options));
        var injector = Injector.create([
            COMPILER_PROVIDERS, {
                provide: CompilerConfig,
                useFactory: function () {
                    return new CompilerConfig({
                        // let explicit values from the compiler options overwrite options
                        // from the app providers
                        useJit: opts.useJit,
                        jitDevMode: isDevMode(),
                        // let explicit values from the compiler options overwrite options
                        // from the app providers
                        defaultEncapsulation: opts.defaultEncapsulation,
                        missingTranslation: opts.missingTranslation,
                        preserveWhitespaces: opts.preserveWhitespaces,
                    });
                },
                deps: []
            },
            opts.providers
        ]);
        return injector.get(Compiler);
    };
    return JitCompilerFactory;
}());
function _mergeOptions(optionsArr) {
    return {
        useJit: _lastDefined(optionsArr.map(function (options) { return options.useJit; })),
        defaultEncapsulation: _lastDefined(optionsArr.map(function (options) { return options.defaultEncapsulation; })),
        providers: _mergeArrays(optionsArr.map(function (options) { return options.providers; })),
        missingTranslation: _lastDefined(optionsArr.map(function (options) { return options.missingTranslation; })),
        preserveWhitespaces: _lastDefined(optionsArr.map(function (options) { return options.preserveWhitespaces; })),
    };
}
function _lastDefined(args) {
    for (var i = args.length - 1; i >= 0; i--) {
        if (args[i] !== undefined) {
            return args[i];
        }
    }
    return undefined;
}
function _mergeArrays(parts) {
    var result = [];
    parts.forEach(function (part) { return part && result.push.apply(result, __spread(part)); });
    return result;
}

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
var ɵ0 = {};
/**
 * A platform that included corePlatform and the compiler.
 *
 * @publicApi
 */
var platformCoreDynamic = createPlatformFactory(platformCore, 'coreDynamic', [
    { provide: COMPILER_OPTIONS, useValue: ɵ0, multi: true },
    { provide: CompilerFactory, useClass: JitCompilerFactory, deps: [COMPILER_OPTIONS] },
]);

var ResourceLoaderImpl = /** @class */ (function (_super) {
    __extends(ResourceLoaderImpl, _super);
    function ResourceLoaderImpl() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    ResourceLoaderImpl.prototype.get = function (url) {
        var resolve;
        var reject;
        var promise = new Promise(function (res, rej) {
            resolve = res;
            reject = rej;
        });
        var xhr = new XMLHttpRequest();
        xhr.open('GET', url, true);
        xhr.responseType = 'text';
        xhr.onload = function () {
            // responseText is the old-school way of retrieving response (supported by IE8 & 9)
            // response/responseType properties were introduced in ResourceLoader Level2 spec (supported
            // by IE10)
            var response = xhr.response || xhr.responseText;
            // normalize IE9 bug (http://bugs.jquery.com/ticket/1450)
            var status = xhr.status === 1223 ? 204 : xhr.status;
            // fix status code when it is 0 (0 status is undocumented).
            // Occurs when accessing file resources or on Android 4.1 stock browser
            // while retrieving files from application cache.
            if (status === 0) {
                status = response ? 200 : 0;
            }
            if (200 <= status && status <= 300) {
                resolve(response);
            }
            else {
                reject("Failed to load " + url);
            }
        };
        xhr.onerror = function () { reject("Failed to load " + url); };
        xhr.send();
        return promise;
    };
    ResourceLoaderImpl = __decorate([
        Injectable()
    ], ResourceLoaderImpl);
    return ResourceLoaderImpl;
}(ResourceLoader));

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
var ɵ0$1 = { providers: [{ provide: ResourceLoader, useClass: ResourceLoaderImpl, deps: [] }] }, ɵ1 = ɵPLATFORM_BROWSER_ID;
/**
 * @publicApi
 */
var INTERNAL_BROWSER_DYNAMIC_PLATFORM_PROVIDERS = [
    ɵINTERNAL_BROWSER_PLATFORM_PROVIDERS,
    {
        provide: COMPILER_OPTIONS,
        useValue: ɵ0$1,
        multi: true
    },
    { provide: PLATFORM_ID, useValue: ɵ1 },
];

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * An implementation of ResourceLoader that uses a template cache to avoid doing an actual
 * ResourceLoader.
 *
 * The template cache needs to be built and loaded into window.$templateCache
 * via a separate mechanism.
 *
 * @publicApi
 */
var CachedResourceLoader = /** @class */ (function (_super) {
    __extends(CachedResourceLoader, _super);
    function CachedResourceLoader() {
        var _this = _super.call(this) || this;
        _this._cache = ɵglobal.$templateCache;
        if (_this._cache == null) {
            throw new Error('CachedResourceLoader: Template cache was not found in $templateCache.');
        }
        return _this;
    }
    CachedResourceLoader.prototype.get = function (url) {
        if (this._cache.hasOwnProperty(url)) {
            return Promise.resolve(this._cache[url]);
        }
        else {
            return Promise.reject('CachedResourceLoader: Did not find cached template for ' + url);
        }
    };
    return CachedResourceLoader;
}(ResourceLoader));

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * @publicApi
 */
var VERSION = new Version('8.2.14');

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * @publicApi
 */
var RESOURCE_CACHE_PROVIDER = [{ provide: ResourceLoader, useClass: CachedResourceLoader, deps: [] }];
/**
 * @publicApi
 */
var platformBrowserDynamic = createPlatformFactory(platformCoreDynamic, 'browserDynamic', INTERNAL_BROWSER_DYNAMIC_PLATFORM_PROVIDERS);

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
// This file only reexports content of the `src` folder. Keep it that way.

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */

/**
 * Generated bundle index. Do not edit.
 */

export { CachedResourceLoader as ɵangular_packages_platform_browser_dynamic_platform_browser_dynamic_a, RESOURCE_CACHE_PROVIDER, platformBrowserDynamic, VERSION, JitCompilerFactory, CompilerImpl as ɵCompilerImpl, platformCoreDynamic as ɵplatformCoreDynamic, INTERNAL_BROWSER_DYNAMIC_PLATFORM_PROVIDERS as ɵINTERNAL_BROWSER_DYNAMIC_PLATFORM_PROVIDERS, ResourceLoaderImpl as ɵResourceLoaderImpl };
//# sourceMappingURL=platform-browser-dynamic.js.map
