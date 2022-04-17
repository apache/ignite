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
import { Compiler, Inject, InjectionToken, Optional, PACKAGE_ROOT_URL, TRANSLATIONS, isDevMode, ɵConsole as Console, ViewEncapsulation, Injector, TRANSLATIONS_FORMAT, MissingTranslationStrategy, } from '@angular/core';
import { StaticSymbolCache, JitCompiler, ProviderMeta, I18NHtmlParser, ViewCompiler, CompileMetadataResolver, UrlResolver, TemplateParser, NgModuleCompiler, JitEvaluator, JitSummaryResolver, SummaryResolver, StyleCompiler, PipeResolver, ElementSchemaRegistry, DomElementSchemaRegistry, ResourceLoader, NgModuleResolver, HtmlParser, CompileReflector, CompilerConfig, DirectiveNormalizer, DirectiveResolver, Lexer, Parser } from '@angular/compiler';
import { JitReflector } from './compiler_reflector';
/** @type {?} */
export const ERROR_COLLECTOR_TOKEN = new InjectionToken('ErrorCollector');
/**
 * A default provider for {\@link PACKAGE_ROOT_URL} that maps to '/'.
 * @type {?}
 */
export const DEFAULT_PACKAGE_URL_PROVIDER = {
    provide: PACKAGE_ROOT_URL,
    useValue: '/'
};
/** @type {?} */
const _NO_RESOURCE_LOADER = {
    /**
     * @param {?} url
     * @return {?}
     */
    get(url) {
        throw new Error(`No ResourceLoader implementation has been provided. Can't read the url "${url}"`);
    }
};
/** @type {?} */
const baseHtmlParser = new InjectionToken('HtmlParser');
export class CompilerImpl {
    /**
     * @param {?} injector
     * @param {?} _metadataResolver
     * @param {?} templateParser
     * @param {?} styleCompiler
     * @param {?} viewCompiler
     * @param {?} ngModuleCompiler
     * @param {?} summaryResolver
     * @param {?} compileReflector
     * @param {?} jitEvaluator
     * @param {?} compilerConfig
     * @param {?} console
     */
    constructor(injector, _metadataResolver, templateParser, styleCompiler, viewCompiler, ngModuleCompiler, summaryResolver, compileReflector, jitEvaluator, compilerConfig, console) {
        this._metadataResolver = _metadataResolver;
        this._delegate = new JitCompiler(_metadataResolver, templateParser, styleCompiler, viewCompiler, ngModuleCompiler, summaryResolver, compileReflector, jitEvaluator, compilerConfig, console, this.getExtraNgModuleProviders.bind(this));
        this.injector = injector;
    }
    /**
     * @private
     * @return {?}
     */
    getExtraNgModuleProviders() {
        return [this._metadataResolver.getProviderMetadata(new ProviderMeta(Compiler, { useValue: this }))];
    }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleSync(moduleType) {
        return (/** @type {?} */ (this._delegate.compileModuleSync(moduleType)));
    }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleAsync(moduleType) {
        return (/** @type {?} */ (this._delegate.compileModuleAsync(moduleType)));
    }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleAndAllComponentsSync(moduleType) {
        /** @type {?} */
        const result = this._delegate.compileModuleAndAllComponentsSync(moduleType);
        return {
            ngModuleFactory: (/** @type {?} */ (result.ngModuleFactory)),
            componentFactories: (/** @type {?} */ (result.componentFactories)),
        };
    }
    /**
     * @template T
     * @param {?} moduleType
     * @return {?}
     */
    compileModuleAndAllComponentsAsync(moduleType) {
        return this._delegate.compileModuleAndAllComponentsAsync(moduleType)
            .then((/**
         * @param {?} result
         * @return {?}
         */
        (result) => ({
            ngModuleFactory: (/** @type {?} */ (result.ngModuleFactory)),
            componentFactories: (/** @type {?} */ (result.componentFactories)),
        })));
    }
    /**
     * @param {?} summaries
     * @return {?}
     */
    loadAotSummaries(summaries) { this._delegate.loadAotSummaries(summaries); }
    /**
     * @param {?} ref
     * @return {?}
     */
    hasAotSummary(ref) { return this._delegate.hasAotSummary(ref); }
    /**
     * @template T
     * @param {?} component
     * @return {?}
     */
    getComponentFactory(component) {
        return (/** @type {?} */ (this._delegate.getComponentFactory(component)));
    }
    /**
     * @return {?}
     */
    clearCache() { this._delegate.clearCache(); }
    /**
     * @param {?} type
     * @return {?}
     */
    clearCacheFor(type) { this._delegate.clearCacheFor(type); }
    /**
     * @param {?} moduleType
     * @return {?}
     */
    getModuleId(moduleType) {
        /** @type {?} */
        const meta = this._metadataResolver.getNgModuleMetadata(moduleType);
        return meta && meta.id || undefined;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    CompilerImpl.prototype._delegate;
    /** @type {?} */
    CompilerImpl.prototype.injector;
    /**
     * @type {?}
     * @private
     */
    CompilerImpl.prototype._metadataResolver;
}
/**
 * A set of providers that provide `JitCompiler` and its dependencies to use for
 * template compilation.
 * @type {?}
 */
export const COMPILER_PROVIDERS = (/** @type {?} */ ([
    { provide: CompileReflector, useValue: new JitReflector() },
    { provide: ResourceLoader, useValue: _NO_RESOURCE_LOADER },
    { provide: JitSummaryResolver, deps: [] },
    { provide: SummaryResolver, useExisting: JitSummaryResolver },
    { provide: Console, deps: [] },
    { provide: Lexer, deps: [] },
    { provide: Parser, deps: [Lexer] },
    {
        provide: baseHtmlParser,
        useClass: HtmlParser,
        deps: [],
    },
    {
        provide: I18NHtmlParser,
        useFactory: (/**
         * @param {?} parser
         * @param {?} translations
         * @param {?} format
         * @param {?} config
         * @param {?} console
         * @return {?}
         */
        (parser, translations, format, config, console) => {
            translations = translations || '';
            /** @type {?} */
            const missingTranslation = translations ? (/** @type {?} */ (config.missingTranslation)) : MissingTranslationStrategy.Ignore;
            return new I18NHtmlParser(parser, translations, format, missingTranslation, console);
        }),
        deps: [
            baseHtmlParser,
            [new Optional(), new Inject(TRANSLATIONS)],
            [new Optional(), new Inject(TRANSLATIONS_FORMAT)],
            [CompilerConfig],
            [Console],
        ]
    },
    {
        provide: HtmlParser,
        useExisting: I18NHtmlParser,
    },
    {
        provide: TemplateParser, deps: [CompilerConfig, CompileReflector,
            Parser, ElementSchemaRegistry,
            I18NHtmlParser, Console]
    },
    { provide: JitEvaluator, useClass: JitEvaluator, deps: [] },
    { provide: DirectiveNormalizer, deps: [ResourceLoader, UrlResolver, HtmlParser, CompilerConfig] },
    { provide: CompileMetadataResolver, deps: [CompilerConfig, HtmlParser, NgModuleResolver,
            DirectiveResolver, PipeResolver,
            SummaryResolver,
            ElementSchemaRegistry,
            DirectiveNormalizer, Console,
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
            Console] },
    { provide: DomElementSchemaRegistry, deps: [] },
    { provide: ElementSchemaRegistry, useExisting: DomElementSchemaRegistry },
    { provide: UrlResolver, deps: [PACKAGE_ROOT_URL] },
    { provide: DirectiveResolver, deps: [CompileReflector] },
    { provide: PipeResolver, deps: [CompileReflector] },
    { provide: NgModuleResolver, deps: [CompileReflector] },
]));
/**
 * \@publicApi
 */
export class JitCompilerFactory {
    /* @internal */
    /**
     * @param {?} defaultOptions
     */
    constructor(defaultOptions) {
        /** @type {?} */
        const compilerOptions = {
            useJit: true,
            defaultEncapsulation: ViewEncapsulation.Emulated,
            missingTranslation: MissingTranslationStrategy.Warning,
        };
        this._defaultOptions = [compilerOptions, ...defaultOptions];
    }
    /**
     * @param {?=} options
     * @return {?}
     */
    createCompiler(options = []) {
        /** @type {?} */
        const opts = _mergeOptions(this._defaultOptions.concat(options));
        /** @type {?} */
        const injector = Injector.create([
            COMPILER_PROVIDERS, {
                provide: CompilerConfig,
                useFactory: (/**
                 * @return {?}
                 */
                () => {
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
                }),
                deps: []
            },
            (/** @type {?} */ (opts.providers))
        ]);
        return injector.get(Compiler);
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    JitCompilerFactory.prototype._defaultOptions;
}
/**
 * @param {?} optionsArr
 * @return {?}
 */
function _mergeOptions(optionsArr) {
    return {
        useJit: _lastDefined(optionsArr.map((/**
         * @param {?} options
         * @return {?}
         */
        options => options.useJit))),
        defaultEncapsulation: _lastDefined(optionsArr.map((/**
         * @param {?} options
         * @return {?}
         */
        options => options.defaultEncapsulation))),
        providers: _mergeArrays(optionsArr.map((/**
         * @param {?} options
         * @return {?}
         */
        options => (/** @type {?} */ (options.providers))))),
        missingTranslation: _lastDefined(optionsArr.map((/**
         * @param {?} options
         * @return {?}
         */
        options => options.missingTranslation))),
        preserveWhitespaces: _lastDefined(optionsArr.map((/**
         * @param {?} options
         * @return {?}
         */
        options => options.preserveWhitespaces))),
    };
}
/**
 * @template T
 * @param {?} args
 * @return {?}
 */
function _lastDefined(args) {
    for (let i = args.length - 1; i >= 0; i--) {
        if (args[i] !== undefined) {
            return args[i];
        }
    }
    return undefined;
}
/**
 * @param {?} parts
 * @return {?}
 */
function _mergeArrays(parts) {
    /** @type {?} */
    const result = [];
    parts.forEach((/**
     * @param {?} part
     * @return {?}
     */
    (part) => part && result.push(...part)));
    return result;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZXJfZmFjdG9yeS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXItZHluYW1pYy9zcmMvY29tcGlsZXJfZmFjdG9yeS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxRQUFRLEVBQW9GLE1BQU0sRUFBRSxjQUFjLEVBQUUsUUFBUSxFQUFFLGdCQUFnQixFQUFrQixZQUFZLEVBQVEsU0FBUyxFQUFFLFFBQVEsSUFBSSxPQUFPLEVBQUUsaUJBQWlCLEVBQUUsUUFBUSxFQUFtQixtQkFBbUIsRUFBRSwwQkFBMEIsR0FBRSxNQUFNLGVBQWUsQ0FBQztBQUVqVixPQUFPLEVBQUMsaUJBQWlCLEVBQUUsV0FBVyxFQUFFLFlBQVksRUFBRSxjQUFjLEVBQUUsWUFBWSxFQUFFLHVCQUF1QixFQUFFLFdBQVcsRUFBRSxjQUFjLEVBQUUsZ0JBQWdCLEVBQUUsWUFBWSxFQUFFLGtCQUFrQixFQUFFLGVBQWUsRUFBRSxhQUFhLEVBQUUsWUFBWSxFQUFFLHFCQUFxQixFQUFFLHdCQUF3QixFQUFFLGNBQWMsRUFBRSxnQkFBZ0IsRUFBRSxVQUFVLEVBQUUsZ0JBQWdCLEVBQUUsY0FBYyxFQUFFLG1CQUFtQixFQUFFLGlCQUFpQixFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUU3YixPQUFPLEVBQUMsWUFBWSxFQUFDLE1BQU0sc0JBQXNCLENBQUM7O0FBRWxELE1BQU0sT0FBTyxxQkFBcUIsR0FBRyxJQUFJLGNBQWMsQ0FBQyxnQkFBZ0IsQ0FBQzs7Ozs7QUFLekUsTUFBTSxPQUFPLDRCQUE0QixHQUFHO0lBQzFDLE9BQU8sRUFBRSxnQkFBZ0I7SUFDekIsUUFBUSxFQUFFLEdBQUc7Q0FDZDs7TUFFSyxtQkFBbUIsR0FBbUI7Ozs7O0lBQzFDLEdBQUcsQ0FBQyxHQUFXO1FBQ1gsTUFBTSxJQUFJLEtBQUss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eXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVyLCBDb21waWxlckZhY3RvcnksIENvbXBvbmVudEZhY3RvcnksIENvbXBpbGVyT3B0aW9ucywgTW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllcywgSW5qZWN0LCBJbmplY3Rpb25Ub2tlbiwgT3B0aW9uYWwsIFBBQ0tBR0VfUk9PVF9VUkwsIFN0YXRpY1Byb3ZpZGVyLCBUUkFOU0xBVElPTlMsIFR5cGUsIGlzRGV2TW9kZSwgybVDb25zb2xlIGFzIENvbnNvbGUsIFZpZXdFbmNhcHN1bGF0aW9uLCBJbmplY3RvciwgTmdNb2R1bGVGYWN0b3J5LCBUUkFOU0xBVElPTlNfRk9STUFULCBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSx9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge1N0YXRpY1N5bWJvbENhY2hlLCBKaXRDb21waWxlciwgUHJvdmlkZXJNZXRhLCBJMThOSHRtbFBhcnNlciwgVmlld0NvbXBpbGVyLCBDb21waWxlTWV0YWRhdGFSZXNvbHZlciwgVXJsUmVzb2x2ZXIsIFRlbXBsYXRlUGFyc2VyLCBOZ01vZHVsZUNvbXBpbGVyLCBKaXRFdmFsdWF0b3IsIEppdFN1bW1hcnlSZXNvbHZlciwgU3VtbWFyeVJlc29sdmVyLCBTdHlsZUNvbXBpbGVyLCBQaXBlUmVzb2x2ZXIsIEVsZW1lbnRTY2hlbWFSZWdpc3RyeSwgRG9tRWxlbWVudFNjaGVtYVJlZ2lzdHJ5LCBSZXNvdXJjZUxvYWRlciwgTmdNb2R1bGVSZXNvbHZlciwgSHRtbFBhcnNlciwgQ29tcGlsZVJlZmxlY3RvciwgQ29tcGlsZXJDb25maWcsIERpcmVjdGl2ZU5vcm1hbGl6ZXIsIERpcmVjdGl2ZVJlc29sdmVyLCBMZXhlciwgUGFyc2VyfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5cbmltcG9ydCB7Sml0UmVmbGVjdG9yfSBmcm9tICcuL2NvbXBpbGVyX3JlZmxlY3Rvcic7XG5cbmV4cG9ydCBjb25zdCBFUlJPUl9DT0xMRUNUT1JfVE9LRU4gPSBuZXcgSW5qZWN0aW9uVG9rZW4oJ0Vycm9yQ29sbGVjdG9yJyk7XG5cbi8qKlxuICogQSBkZWZhdWx0IHByb3ZpZGVyIGZvciB7QGxpbmsgUEFDS0FHRV9ST09UX1VSTH0gdGhhdCBtYXBzIHRvICcvJy5cbiAqL1xuZXhwb3J0IGNvbnN0IERFRkFVTFRfUEFDS0FHRV9VUkxfUFJPVklERVIgPSB7XG4gIHByb3ZpZGU6IFBBQ0tBR0VfUk9PVF9VUkwsXG4gIHVzZVZhbHVlOiAnLydcbn07XG5cbmNvbnN0IF9OT19SRVNPVVJDRV9MT0FERVI6IFJlc291cmNlTG9hZGVyID0ge1xuICBnZXQodXJsOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZz57XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgYE5vIFJlc291cmNlTG9hZGVyIGltcGxlbWVudGF0aW9uIGhhcyBiZWVuIHByb3ZpZGVkLiBDYW4ndCByZWFkIHRoZSB1cmwgXCIke3VybH1cImApO31cbn07XG5cbmNvbnN0IGJhc2VIdG1sUGFyc2VyID0gbmV3IEluamVjdGlvblRva2VuKCdIdG1sUGFyc2VyJyk7XG5cbmV4cG9ydCBjbGFzcyBDb21waWxlckltcGwgaW1wbGVtZW50cyBDb21waWxlciB7XG4gIHByaXZhdGUgX2RlbGVnYXRlOiBKaXRDb21waWxlcjtcbiAgcHVibGljIHJlYWRvbmx5IGluamVjdG9yOiBJbmplY3RvcjtcbiAgY29uc3RydWN0b3IoXG4gICAgICBpbmplY3RvcjogSW5qZWN0b3IsIHByaXZhdGUgX21ldGFkYXRhUmVzb2x2ZXI6IENvbXBpbGVNZXRhZGF0YVJlc29sdmVyLFxuICAgICAgdGVtcGxhdGVQYXJzZXI6IFRlbXBsYXRlUGFyc2VyLCBzdHlsZUNvbXBpbGVyOiBTdHlsZUNvbXBpbGVyLCB2aWV3Q29tcGlsZXI6IFZpZXdDb21waWxlcixcbiAgICAgIG5nTW9kdWxlQ29tcGlsZXI6IE5nTW9kdWxlQ29tcGlsZXIsIHN1bW1hcnlSZXNvbHZlcjogU3VtbWFyeVJlc29sdmVyPFR5cGU8YW55Pj4sXG4gICAgICBjb21waWxlUmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yLCBqaXRFdmFsdWF0b3I6IEppdEV2YWx1YXRvcixcbiAgICAgIGNvbXBpbGVyQ29uZmlnOiBDb21waWxlckNvbmZpZywgY29uc29sZTogQ29uc29sZSkge1xuICAgIHRoaXMuX2RlbGVnYXRlID0gbmV3IEppdENvbXBpbGVyKFxuICAgICAgICBfbWV0YWRhdGFSZXNvbHZlciwgdGVtcGxhdGVQYXJzZXIsIHN0eWxlQ29tcGlsZXIsIHZpZXdDb21waWxlciwgbmdNb2R1bGVDb21waWxlcixcbiAgICAgICAgc3VtbWFyeVJlc29sdmVyLCBjb21waWxlUmVmbGVjdG9yLCBqaXRFdmFsdWF0b3IsIGNvbXBpbGVyQ29uZmlnLCBjb25zb2xlLFxuICAgICAgICB0aGlzLmdldEV4dHJhTmdNb2R1bGVQcm92aWRlcnMuYmluZCh0aGlzKSk7XG4gICAgdGhpcy5pbmplY3RvciA9IGluamVjdG9yO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRFeHRyYU5nTW9kdWxlUHJvdmlkZXJzKCkge1xuICAgIHJldHVybiBbdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXRQcm92aWRlck1ldGFkYXRhKFxuICAgICAgICBuZXcgUHJvdmlkZXJNZXRhKENvbXBpbGVyLCB7dXNlVmFsdWU6IHRoaXN9KSldO1xuICB9XG5cbiAgY29tcGlsZU1vZHVsZVN5bmM8VD4obW9kdWxlVHlwZTogVHlwZTxUPik6IE5nTW9kdWxlRmFjdG9yeTxUPiB7XG4gICAgcmV0dXJuIHRoaXMuX2RlbGVnYXRlLmNvbXBpbGVNb2R1bGVTeW5jKG1vZHVsZVR5cGUpIGFzIE5nTW9kdWxlRmFjdG9yeTxUPjtcbiAgfVxuICBjb21waWxlTW9kdWxlQXN5bmM8VD4obW9kdWxlVHlwZTogVHlwZTxUPik6IFByb21pc2U8TmdNb2R1bGVGYWN0b3J5PFQ+PiB7XG4gICAgcmV0dXJuIHRoaXMuX2RlbGVnYXRlLmNvbXBpbGVNb2R1bGVBc3luYyhtb2R1bGVUeXBlKSBhcyBQcm9taXNlPE5nTW9kdWxlRmFjdG9yeTxUPj47XG4gIH1cbiAgY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHNTeW5jPFQ+KG1vZHVsZVR5cGU6IFR5cGU8VD4pOiBNb2R1bGVXaXRoQ29tcG9uZW50RmFjdG9yaWVzPFQ+IHtcbiAgICBjb25zdCByZXN1bHQgPSB0aGlzLl9kZWxlZ2F0ZS5jb21waWxlTW9kdWxlQW5kQWxsQ29tcG9uZW50c1N5bmMobW9kdWxlVHlwZSk7XG4gICAgcmV0dXJuIHtcbiAgICAgIG5nTW9kdWxlRmFjdG9yeTogcmVzdWx0Lm5nTW9kdWxlRmFjdG9yeSBhcyBOZ01vZHVsZUZhY3Rvcnk8VD4sXG4gICAgICBjb21wb25lbnRGYWN0b3JpZXM6IHJlc3VsdC5jb21wb25lbnRGYWN0b3JpZXMgYXMgQ29tcG9uZW50RmFjdG9yeTxhbnk+W10sXG4gICAgfTtcbiAgfVxuICBjb21waWxlTW9kdWxlQW5kQWxsQ29tcG9uZW50c0FzeW5jPFQ+KG1vZHVsZVR5cGU6IFR5cGU8VD4pOlxuICAgICAgUHJvbWlzZTxNb2R1bGVXaXRoQ29tcG9uZW50RmFjdG9yaWVzPFQ+PiB7XG4gICAgcmV0dXJuIHRoaXMuX2RlbGVnYXRlLmNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmMobW9kdWxlVHlwZSlcbiAgICAgICAgLnRoZW4oKHJlc3VsdCkgPT4gKHtcbiAgICAgICAgICAgICAgICBuZ01vZHVsZUZhY3Rvcnk6IHJlc3VsdC5uZ01vZHVsZUZhY3RvcnkgYXMgTmdNb2R1bGVGYWN0b3J5PFQ+LFxuICAgICAgICAgICAgICAgIGNvbXBvbmVudEZhY3RvcmllczogcmVzdWx0LmNvbXBvbmVudEZhY3RvcmllcyBhcyBDb21wb25lbnRGYWN0b3J5PGFueT5bXSxcbiAgICAgICAgICAgICAgfSkpO1xuICB9XG4gIGxvYWRBb3RTdW1tYXJpZXMoc3VtbWFyaWVzOiAoKSA9PiBhbnlbXSkgeyB0aGlzLl9kZWxlZ2F0ZS5sb2FkQW90U3VtbWFyaWVzKHN1bW1hcmllcyk7IH1cbiAgaGFzQW90U3VtbWFyeShyZWY6IFR5cGU8YW55Pik6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5fZGVsZWdhdGUuaGFzQW90U3VtbWFyeShyZWYpOyB9XG4gIGdldENvbXBvbmVudEZhY3Rvcnk8VD4oY29tcG9uZW50OiBUeXBlPFQ+KTogQ29tcG9uZW50RmFjdG9yeTxUPiB7XG4gICAgcmV0dXJuIHRoaXMuX2RlbGVnYXRlLmdldENvbXBvbmVudEZhY3RvcnkoY29tcG9uZW50KSBhcyBDb21wb25lbnRGYWN0b3J5PFQ+O1xuICB9XG4gIGNsZWFyQ2FjaGUoKTogdm9pZCB7IHRoaXMuX2RlbGVnYXRlLmNsZWFyQ2FjaGUoKTsgfVxuICBjbGVhckNhY2hlRm9yKHR5cGU6IFR5cGU8YW55PikgeyB0aGlzLl9kZWxlZ2F0ZS5jbGVhckNhY2hlRm9yKHR5cGUpOyB9XG4gIGdldE1vZHVsZUlkKG1vZHVsZVR5cGU6IFR5cGU8YW55Pik6IHN0cmluZ3x1bmRlZmluZWQge1xuICAgIGNvbnN0IG1ldGEgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldE5nTW9kdWxlTWV0YWRhdGEobW9kdWxlVHlwZSk7XG4gICAgcmV0dXJuIG1ldGEgJiYgbWV0YS5pZCB8fCB1bmRlZmluZWQ7XG4gIH1cbn1cblxuLyoqXG4gKiBBIHNldCBvZiBwcm92aWRlcnMgdGhhdCBwcm92aWRlIGBKaXRDb21waWxlcmAgYW5kIGl0cyBkZXBlbmRlbmNpZXMgdG8gdXNlIGZvclxuICogdGVtcGxhdGUgY29tcGlsYXRpb24uXG4gKi9cbmV4cG9ydCBjb25zdCBDT01QSUxFUl9QUk9WSURFUlMgPSA8U3RhdGljUHJvdmlkZXJbXT5bXG4gIHtwcm92aWRlOiBDb21waWxlUmVmbGVjdG9yLCB1c2VWYWx1ZTogbmV3IEppdFJlZmxlY3RvcigpfSxcbiAge3Byb3ZpZGU6IFJlc291cmNlTG9hZGVyLCB1c2VWYWx1ZTogX05PX1JFU09VUkNFX0xPQURFUn0sXG4gIHtwcm92aWRlOiBKaXRTdW1tYXJ5UmVzb2x2ZXIsIGRlcHM6IFtdfSxcbiAge3Byb3ZpZGU6IFN1bW1hcnlSZXNvbHZlciwgdXNlRXhpc3Rpbmc6IEppdFN1bW1hcnlSZXNvbHZlcn0sXG4gIHtwcm92aWRlOiBDb25zb2xlLCBkZXBzOiBbXX0sXG4gIHtwcm92aWRlOiBMZXhlciwgZGVwczogW119LFxuICB7cHJvdmlkZTogUGFyc2VyLCBkZXBzOiBbTGV4ZXJdfSxcbiAge1xuICAgIHByb3ZpZGU6IGJhc2VIdG1sUGFyc2VyLFxuICAgIHVzZUNsYXNzOiBIdG1sUGFyc2VyLFxuICAgIGRlcHM6IFtdLFxuICB9LFxuICB7XG4gICAgcHJvdmlkZTogSTE4Tkh0bWxQYXJzZXIsXG4gICAgdXNlRmFjdG9yeTogKHBhcnNlcjogSHRtbFBhcnNlciwgdHJhbnNsYXRpb25zOiBzdHJpbmcgfCBudWxsLCBmb3JtYXQ6IHN0cmluZyxcbiAgICAgICAgICAgICAgICAgY29uZmlnOiBDb21waWxlckNvbmZpZywgY29uc29sZTogQ29uc29sZSkgPT4ge1xuICAgICAgdHJhbnNsYXRpb25zID0gdHJhbnNsYXRpb25zIHx8ICcnO1xuICAgICAgY29uc3QgbWlzc2luZ1RyYW5zbGF0aW9uID1cbiAgICAgICAgICB0cmFuc2xhdGlvbnMgPyBjb25maWcubWlzc2luZ1RyYW5zbGF0aW9uICEgOiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneS5JZ25vcmU7XG4gICAgICByZXR1cm4gbmV3IEkxOE5IdG1sUGFyc2VyKHBhcnNlciwgdHJhbnNsYXRpb25zLCBmb3JtYXQsIG1pc3NpbmdUcmFuc2xhdGlvbiwgY29uc29sZSk7XG4gICAgfSxcbiAgICBkZXBzOiBbXG4gICAgICBiYXNlSHRtbFBhcnNlcixcbiAgICAgIFtuZXcgT3B0aW9uYWwoKSwgbmV3IEluamVjdChUUkFOU0xBVElPTlMpXSxcbiAgICAgIFtuZXcgT3B0aW9uYWwoKSwgbmV3IEluamVjdChUUkFOU0xBVElPTlNfRk9STUFUKV0sXG4gICAgICBbQ29tcGlsZXJDb25maWddLFxuICAgICAgW0NvbnNvbGVdLFxuICAgIF1cbiAgfSxcbiAge1xuICAgIHByb3ZpZGU6IEh0bWxQYXJzZXIsXG4gICAgdXNlRXhpc3Rpbmc6IEkxOE5IdG1sUGFyc2VyLFxuICB9LFxuICB7XG4gICAgcHJvdmlkZTogVGVtcGxhdGVQYXJzZXIsIGRlcHM6IFtDb21waWxlckNvbmZpZywgQ29tcGlsZVJlZmxlY3RvcixcbiAgICBQYXJzZXIsIEVsZW1lbnRTY2hlbWFSZWdpc3RyeSxcbiAgICBJMThOSHRtbFBhcnNlciwgQ29uc29sZV1cbiAgfSxcbiAgeyBwcm92aWRlOiBKaXRFdmFsdWF0b3IsIHVzZUNsYXNzOiBKaXRFdmFsdWF0b3IsIGRlcHM6IFtdIH0sXG4gIHsgcHJvdmlkZTogRGlyZWN0aXZlTm9ybWFsaXplciwgZGVwczogW1Jlc291cmNlTG9hZGVyLCBVcmxSZXNvbHZlciwgSHRtbFBhcnNlciwgQ29tcGlsZXJDb25maWddfSxcbiAgeyBwcm92aWRlOiBDb21waWxlTWV0YWRhdGFSZXNvbHZlciwgZGVwczogW0NvbXBpbGVyQ29uZmlnLCBIdG1sUGFyc2VyLCBOZ01vZHVsZVJlc29sdmVyLFxuICAgICAgICAgICAgICAgICAgICAgIERpcmVjdGl2ZVJlc29sdmVyLCBQaXBlUmVzb2x2ZXIsXG4gICAgICAgICAgICAgICAgICAgICAgU3VtbWFyeVJlc29sdmVyLFxuICAgICAgICAgICAgICAgICAgICAgIEVsZW1lbnRTY2hlbWFSZWdpc3RyeSxcbiAgICAgICAgICAgICAgICAgICAgICBEaXJlY3RpdmVOb3JtYWxpemVyLCBDb25zb2xlLFxuICAgICAgICAgICAgICAgICAgICAgIFtPcHRpb25hbCwgU3RhdGljU3ltYm9sQ2FjaGVdLFxuICAgICAgICAgICAgICAgICAgICAgIENvbXBpbGVSZWZsZWN0b3IsXG4gICAgICAgICAgICAgICAgICAgICAgW09wdGlvbmFsLCBFUlJPUl9DT0xMRUNUT1JfVE9LRU5dXX0sXG4gIERFRkFVTFRfUEFDS0FHRV9VUkxfUFJPVklERVIsXG4gIHsgcHJvdmlkZTogU3R5bGVDb21waWxlciwgZGVwczogW1VybFJlc29sdmVyXX0sXG4gIHsgcHJvdmlkZTogVmlld0NvbXBpbGVyLCBkZXBzOiBbQ29tcGlsZVJlZmxlY3Rvcl19LFxuICB7IHByb3ZpZGU6IE5nTW9kdWxlQ29tcGlsZXIsIGRlcHM6IFtDb21waWxlUmVmbGVjdG9yXSB9LFxuICB7IHByb3ZpZGU6IENvbXBpbGVyQ29uZmlnLCB1c2VWYWx1ZTogbmV3IENvbXBpbGVyQ29uZmlnKCl9LFxuICB7IHByb3ZpZGU6IENvbXBpbGVyLCB1c2VDbGFzczogQ29tcGlsZXJJbXBsLCBkZXBzOiBbSW5qZWN0b3IsIENvbXBpbGVNZXRhZGF0YVJlc29sdmVyLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBUZW1wbGF0ZVBhcnNlciwgU3R5bGVDb21waWxlcixcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgVmlld0NvbXBpbGVyLCBOZ01vZHVsZUNvbXBpbGVyLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBTdW1tYXJ5UmVzb2x2ZXIsIENvbXBpbGVSZWZsZWN0b3IsIEppdEV2YWx1YXRvciwgQ29tcGlsZXJDb25maWcsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIENvbnNvbGVdfSxcbiAgeyBwcm92aWRlOiBEb21FbGVtZW50U2NoZW1hUmVnaXN0cnksIGRlcHM6IFtdfSxcbiAgeyBwcm92aWRlOiBFbGVtZW50U2NoZW1hUmVnaXN0cnksIHVzZUV4aXN0aW5nOiBEb21FbGVtZW50U2NoZW1hUmVnaXN0cnl9LFxuICB7IHByb3ZpZGU6IFVybFJlc29sdmVyLCBkZXBzOiBbUEFDS0FHRV9ST09UX1VSTF19LFxuICB7IHByb3ZpZGU6IERpcmVjdGl2ZVJlc29sdmVyLCBkZXBzOiBbQ29tcGlsZVJlZmxlY3Rvcl19LFxuICB7IHByb3ZpZGU6IFBpcGVSZXNvbHZlciwgZGVwczogW0NvbXBpbGVSZWZsZWN0b3JdfSxcbiAgeyBwcm92aWRlOiBOZ01vZHVsZVJlc29sdmVyLCBkZXBzOiBbQ29tcGlsZVJlZmxlY3Rvcl19LFxuXTtcblxuLyoqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBKaXRDb21waWxlckZhY3RvcnkgaW1wbGVtZW50cyBDb21waWxlckZhY3Rvcnkge1xuICBwcml2YXRlIF9kZWZhdWx0T3B0aW9uczogQ29tcGlsZXJPcHRpb25zW107XG5cbiAgLyogQGludGVybmFsICovXG4gIGNvbnN0cnVjdG9yKGRlZmF1bHRPcHRpb25zOiBDb21waWxlck9wdGlvbnNbXSkge1xuICAgIGNvbnN0IGNvbXBpbGVyT3B0aW9uczogQ29tcGlsZXJPcHRpb25zID0ge1xuICAgICAgdXNlSml0OiB0cnVlLFxuICAgICAgZGVmYXVsdEVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLkVtdWxhdGVkLFxuICAgICAgbWlzc2luZ1RyYW5zbGF0aW9uOiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneS5XYXJuaW5nLFxuICAgIH07XG5cbiAgICB0aGlzLl9kZWZhdWx0T3B0aW9ucyA9IFtjb21waWxlck9wdGlvbnMsIC4uLmRlZmF1bHRPcHRpb25zXTtcbiAgfVxuICBjcmVhdGVDb21waWxlcihvcHRpb25zOiBDb21waWxlck9wdGlvbnNbXSA9IFtdKTogQ29tcGlsZXIge1xuICAgIGNvbnN0IG9wdHMgPSBfbWVyZ2VPcHRpb25zKHRoaXMuX2RlZmF1bHRPcHRpb25zLmNvbmNhdChvcHRpb25zKSk7XG4gICAgY29uc3QgaW5qZWN0b3IgPSBJbmplY3Rvci5jcmVhdGUoW1xuICAgICAgQ09NUElMRVJfUFJPVklERVJTLCB7XG4gICAgICAgIHByb3ZpZGU6IENvbXBpbGVyQ29uZmlnLFxuICAgICAgICB1c2VGYWN0b3J5OiAoKSA9PiB7XG4gICAgICAgICAgcmV0dXJuIG5ldyBDb21waWxlckNvbmZpZyh7XG4gICAgICAgICAgICAvLyBsZXQgZXhwbGljaXQgdmFsdWVzIGZyb20gdGhlIGNvbXBpbGVyIG9wdGlvbnMgb3ZlcndyaXRlIG9wdGlvbnNcbiAgICAgICAgICAgIC8vIGZyb20gdGhlIGFwcCBwcm92aWRlcnNcbiAgICAgICAgICAgIHVzZUppdDogb3B0cy51c2VKaXQsXG4gICAgICAgICAgICBqaXREZXZNb2RlOiBpc0Rldk1vZGUoKSxcbiAgICAgICAgICAgIC8vIGxldCBleHBsaWNpdCB2YWx1ZXMgZnJvbSB0aGUgY29tcGlsZXIgb3B0aW9ucyBvdmVyd3JpdGUgb3B0aW9uc1xuICAgICAgICAgICAgLy8gZnJvbSB0aGUgYXBwIHByb3ZpZGVyc1xuICAgICAgICAgICAgZGVmYXVsdEVuY2Fwc3VsYXRpb246IG9wdHMuZGVmYXVsdEVuY2Fwc3VsYXRpb24sXG4gICAgICAgICAgICBtaXNzaW5nVHJhbnNsYXRpb246IG9wdHMubWlzc2luZ1RyYW5zbGF0aW9uLFxuICAgICAgICAgICAgcHJlc2VydmVXaGl0ZXNwYWNlczogb3B0cy5wcmVzZXJ2ZVdoaXRlc3BhY2VzLFxuICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgICAgICBkZXBzOiBbXVxuICAgICAgfSxcbiAgICAgIG9wdHMucHJvdmlkZXJzICFcbiAgICBdKTtcbiAgICByZXR1cm4gaW5qZWN0b3IuZ2V0KENvbXBpbGVyKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBfbWVyZ2VPcHRpb25zKG9wdGlvbnNBcnI6IENvbXBpbGVyT3B0aW9uc1tdKTogQ29tcGlsZXJPcHRpb25zIHtcbiAgcmV0dXJuIHtcbiAgICB1c2VKaXQ6IF9sYXN0RGVmaW5lZChvcHRpb25zQXJyLm1hcChvcHRpb25zID0+IG9wdGlvbnMudXNlSml0KSksXG4gICAgZGVmYXVsdEVuY2Fwc3VsYXRpb246IF9sYXN0RGVmaW5lZChvcHRpb25zQXJyLm1hcChvcHRpb25zID0+IG9wdGlvbnMuZGVmYXVsdEVuY2Fwc3VsYXRpb24pKSxcbiAgICBwcm92aWRlcnM6IF9tZXJnZUFycmF5cyhvcHRpb25zQXJyLm1hcChvcHRpb25zID0+IG9wdGlvbnMucHJvdmlkZXJzICEpKSxcbiAgICBtaXNzaW5nVHJhbnNsYXRpb246IF9sYXN0RGVmaW5lZChvcHRpb25zQXJyLm1hcChvcHRpb25zID0+IG9wdGlvbnMubWlzc2luZ1RyYW5zbGF0aW9uKSksXG4gICAgcHJlc2VydmVXaGl0ZXNwYWNlczogX2xhc3REZWZpbmVkKG9wdGlvbnNBcnIubWFwKG9wdGlvbnMgPT4gb3B0aW9ucy5wcmVzZXJ2ZVdoaXRlc3BhY2VzKSksXG4gIH07XG59XG5cbmZ1bmN0aW9uIF9sYXN0RGVmaW5lZDxUPihhcmdzOiBUW10pOiBUfHVuZGVmaW5lZCB7XG4gIGZvciAobGV0IGkgPSBhcmdzLmxlbmd0aCAtIDE7IGkgPj0gMDsgaS0tKSB7XG4gICAgaWYgKGFyZ3NbaV0gIT09IHVuZGVmaW5lZCkge1xuICAgICAgcmV0dXJuIGFyZ3NbaV07XG4gICAgfVxuICB9XG4gIHJldHVybiB1bmRlZmluZWQ7XG59XG5cbmZ1bmN0aW9uIF9tZXJnZUFycmF5cyhwYXJ0czogYW55W11bXSk6IGFueVtdIHtcbiAgY29uc3QgcmVzdWx0OiBhbnlbXSA9IFtdO1xuICBwYXJ0cy5mb3JFYWNoKChwYXJ0KSA9PiBwYXJ0ICYmIHJlc3VsdC5wdXNoKC4uLnBhcnQpKTtcbiAgcmV0dXJuIHJlc3VsdDtcbn1cbiJdfQ==