/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { StaticSymbol } from './aot/static_symbol';
import { ngfactoryFilePath } from './aot/util';
import { assertArrayOfStrings, assertInterpolationSymbols } from './assertions';
import * as cpl from './compile_metadata';
import { ChangeDetectionStrategy, Type, ViewEncapsulation, createAttribute, createComponent, createHost, createInject, createInjectable, createInjectionToken, createNgModule, createOptional, createSelf, createSkipSelf } from './core';
import { findLast } from './directive_resolver';
import { Identifiers } from './identifiers';
import { getAllLifecycleHooks } from './lifecycle_reflector';
import { CssSelector } from './selector';
import { SyncAsync, ValueTransformer, isPromise, noUndefined, resolveForwardRef, stringify, syntaxError, visitValue } from './util';
export var ERROR_COMPONENT_TYPE = 'ngComponentType';
// Design notes:
// - don't lazily create metadata:
//   For some metadata, we need to do async work sometimes,
//   so the user has to kick off this loading.
//   But we want to report errors even when the async work is
//   not required to check that the user would have been able
//   to wait correctly.
var CompileMetadataResolver = /** @class */ (function () {
    function CompileMetadataResolver(_config, _htmlParser, _ngModuleResolver, _directiveResolver, _pipeResolver, _summaryResolver, _schemaRegistry, _directiveNormalizer, _console, _staticSymbolCache, _reflector, _errorCollector) {
        this._config = _config;
        this._htmlParser = _htmlParser;
        this._ngModuleResolver = _ngModuleResolver;
        this._directiveResolver = _directiveResolver;
        this._pipeResolver = _pipeResolver;
        this._summaryResolver = _summaryResolver;
        this._schemaRegistry = _schemaRegistry;
        this._directiveNormalizer = _directiveNormalizer;
        this._console = _console;
        this._staticSymbolCache = _staticSymbolCache;
        this._reflector = _reflector;
        this._errorCollector = _errorCollector;
        this._nonNormalizedDirectiveCache = new Map();
        this._directiveCache = new Map();
        this._summaryCache = new Map();
        this._pipeCache = new Map();
        this._ngModuleCache = new Map();
        this._ngModuleOfTypes = new Map();
        this._shallowModuleCache = new Map();
    }
    CompileMetadataResolver.prototype.getReflector = function () { return this._reflector; };
    CompileMetadataResolver.prototype.clearCacheFor = function (type) {
        var dirMeta = this._directiveCache.get(type);
        this._directiveCache.delete(type);
        this._nonNormalizedDirectiveCache.delete(type);
        this._summaryCache.delete(type);
        this._pipeCache.delete(type);
        this._ngModuleOfTypes.delete(type);
        // Clear all of the NgModule as they contain transitive information!
        this._ngModuleCache.clear();
        if (dirMeta) {
            this._directiveNormalizer.clearCacheFor(dirMeta);
        }
    };
    CompileMetadataResolver.prototype.clearCache = function () {
        this._directiveCache.clear();
        this._nonNormalizedDirectiveCache.clear();
        this._summaryCache.clear();
        this._pipeCache.clear();
        this._ngModuleCache.clear();
        this._ngModuleOfTypes.clear();
        this._directiveNormalizer.clearCache();
    };
    CompileMetadataResolver.prototype._createProxyClass = function (baseType, name) {
        var delegate = null;
        var proxyClass = function () {
            if (!delegate) {
                throw new Error("Illegal state: Class " + name + " for type " + stringify(baseType) + " is not compiled yet!");
            }
            return delegate.apply(this, arguments);
        };
        proxyClass.setDelegate = function (d) {
            delegate = d;
            proxyClass.prototype = d.prototype;
        };
        // Make stringify work correctly
        proxyClass.overriddenName = name;
        return proxyClass;
    };
    CompileMetadataResolver.prototype.getGeneratedClass = function (dirType, name) {
        if (dirType instanceof StaticSymbol) {
            return this._staticSymbolCache.get(ngfactoryFilePath(dirType.filePath), name);
        }
        else {
            return this._createProxyClass(dirType, name);
        }
    };
    CompileMetadataResolver.prototype.getComponentViewClass = function (dirType) {
        return this.getGeneratedClass(dirType, cpl.viewClassName(dirType, 0));
    };
    CompileMetadataResolver.prototype.getHostComponentViewClass = function (dirType) {
        return this.getGeneratedClass(dirType, cpl.hostViewClassName(dirType));
    };
    CompileMetadataResolver.prototype.getHostComponentType = function (dirType) {
        var name = cpl.identifierName({ reference: dirType }) + "_Host";
        if (dirType instanceof StaticSymbol) {
            return this._staticSymbolCache.get(dirType.filePath, name);
        }
        return this._createProxyClass(dirType, name);
    };
    CompileMetadataResolver.prototype.getRendererType = function (dirType) {
        if (dirType instanceof StaticSymbol) {
            return this._staticSymbolCache.get(ngfactoryFilePath(dirType.filePath), cpl.rendererTypeName(dirType));
        }
        else {
            // returning an object as proxy,
            // that we fill later during runtime compilation.
            return {};
        }
    };
    CompileMetadataResolver.prototype.getComponentFactory = function (selector, dirType, inputs, outputs) {
        if (dirType instanceof StaticSymbol) {
            return this._staticSymbolCache.get(ngfactoryFilePath(dirType.filePath), cpl.componentFactoryName(dirType));
        }
        else {
            var hostView = this.getHostComponentViewClass(dirType);
            // Note: ngContentSelectors will be filled later once the template is
            // loaded.
            var createComponentFactory = this._reflector.resolveExternalReference(Identifiers.createComponentFactory);
            return createComponentFactory(selector, dirType, hostView, inputs, outputs, []);
        }
    };
    CompileMetadataResolver.prototype.initComponentFactory = function (factory, ngContentSelectors) {
        var _a;
        if (!(factory instanceof StaticSymbol)) {
            (_a = factory.ngContentSelectors).push.apply(_a, tslib_1.__spread(ngContentSelectors));
        }
    };
    CompileMetadataResolver.prototype._loadSummary = function (type, kind) {
        var typeSummary = this._summaryCache.get(type);
        if (!typeSummary) {
            var summary = this._summaryResolver.resolveSummary(type);
            typeSummary = summary ? summary.type : null;
            this._summaryCache.set(type, typeSummary || null);
        }
        return typeSummary && typeSummary.summaryKind === kind ? typeSummary : null;
    };
    CompileMetadataResolver.prototype.getHostComponentMetadata = function (compMeta, hostViewType) {
        var hostType = this.getHostComponentType(compMeta.type.reference);
        if (!hostViewType) {
            hostViewType = this.getHostComponentViewClass(hostType);
        }
        // Note: ! is ok here as this method should only be called with normalized directive
        // metadata, which always fills in the selector.
        var template = CssSelector.parse(compMeta.selector)[0].getMatchingElementTemplate();
        var templateUrl = '';
        var htmlAst = this._htmlParser.parse(template, templateUrl);
        return cpl.CompileDirectiveMetadata.create({
            isHost: true,
            type: { reference: hostType, diDeps: [], lifecycleHooks: [] },
            template: new cpl.CompileTemplateMetadata({
                encapsulation: ViewEncapsulation.None,
                template: template,
                templateUrl: templateUrl,
                htmlAst: htmlAst,
                styles: [],
                styleUrls: [],
                ngContentSelectors: [],
                animations: [],
                isInline: true,
                externalStylesheets: [],
                interpolation: null,
                preserveWhitespaces: false,
            }),
            exportAs: null,
            changeDetection: ChangeDetectionStrategy.Default,
            inputs: [],
            outputs: [],
            host: {},
            isComponent: true,
            selector: '*',
            providers: [],
            viewProviders: [],
            queries: [],
            guards: {},
            viewQueries: [],
            componentViewType: hostViewType,
            rendererType: { id: '__Host__', encapsulation: ViewEncapsulation.None, styles: [], data: {} },
            entryComponents: [],
            componentFactory: null
        });
    };
    CompileMetadataResolver.prototype.loadDirectiveMetadata = function (ngModuleType, directiveType, isSync) {
        var _this = this;
        if (this._directiveCache.has(directiveType)) {
            return null;
        }
        directiveType = resolveForwardRef(directiveType);
        var _a = this.getNonNormalizedDirectiveMetadata(directiveType), annotation = _a.annotation, metadata = _a.metadata;
        var createDirectiveMetadata = function (templateMetadata) {
            var normalizedDirMeta = new cpl.CompileDirectiveMetadata({
                isHost: false,
                type: metadata.type,
                isComponent: metadata.isComponent,
                selector: metadata.selector,
                exportAs: metadata.exportAs,
                changeDetection: metadata.changeDetection,
                inputs: metadata.inputs,
                outputs: metadata.outputs,
                hostListeners: metadata.hostListeners,
                hostProperties: metadata.hostProperties,
                hostAttributes: metadata.hostAttributes,
                providers: metadata.providers,
                viewProviders: metadata.viewProviders,
                queries: metadata.queries,
                guards: metadata.guards,
                viewQueries: metadata.viewQueries,
                entryComponents: metadata.entryComponents,
                componentViewType: metadata.componentViewType,
                rendererType: metadata.rendererType,
                componentFactory: metadata.componentFactory,
                template: templateMetadata
            });
            if (templateMetadata) {
                _this.initComponentFactory(metadata.componentFactory, templateMetadata.ngContentSelectors);
            }
            _this._directiveCache.set(directiveType, normalizedDirMeta);
            _this._summaryCache.set(directiveType, normalizedDirMeta.toSummary());
            return null;
        };
        if (metadata.isComponent) {
            var template = metadata.template;
            var templateMeta = this._directiveNormalizer.normalizeTemplate({
                ngModuleType: ngModuleType,
                componentType: directiveType,
                moduleUrl: this._reflector.componentModuleUrl(directiveType, annotation),
                encapsulation: template.encapsulation,
                template: template.template,
                templateUrl: template.templateUrl,
                styles: template.styles,
                styleUrls: template.styleUrls,
                animations: template.animations,
                interpolation: template.interpolation,
                preserveWhitespaces: template.preserveWhitespaces
            });
            if (isPromise(templateMeta) && isSync) {
                this._reportError(componentStillLoadingError(directiveType), directiveType);
                return null;
            }
            return SyncAsync.then(templateMeta, createDirectiveMetadata);
        }
        else {
            // directive
            createDirectiveMetadata(null);
            return null;
        }
    };
    CompileMetadataResolver.prototype.getNonNormalizedDirectiveMetadata = function (directiveType) {
        var _this = this;
        directiveType = resolveForwardRef(directiveType);
        if (!directiveType) {
            return null;
        }
        var cacheEntry = this._nonNormalizedDirectiveCache.get(directiveType);
        if (cacheEntry) {
            return cacheEntry;
        }
        var dirMeta = this._directiveResolver.resolve(directiveType, false);
        if (!dirMeta) {
            return null;
        }
        var nonNormalizedTemplateMetadata = undefined;
        if (createComponent.isTypeOf(dirMeta)) {
            // component
            var compMeta = dirMeta;
            assertArrayOfStrings('styles', compMeta.styles);
            assertArrayOfStrings('styleUrls', compMeta.styleUrls);
            assertInterpolationSymbols('interpolation', compMeta.interpolation);
            var animations = compMeta.animations;
            nonNormalizedTemplateMetadata = new cpl.CompileTemplateMetadata({
                encapsulation: noUndefined(compMeta.encapsulation),
                template: noUndefined(compMeta.template),
                templateUrl: noUndefined(compMeta.templateUrl),
                htmlAst: null,
                styles: compMeta.styles || [],
                styleUrls: compMeta.styleUrls || [],
                animations: animations || [],
                interpolation: noUndefined(compMeta.interpolation),
                isInline: !!compMeta.template,
                externalStylesheets: [],
                ngContentSelectors: [],
                preserveWhitespaces: noUndefined(dirMeta.preserveWhitespaces),
            });
        }
        var changeDetectionStrategy = null;
        var viewProviders = [];
        var entryComponentMetadata = [];
        var selector = dirMeta.selector;
        if (createComponent.isTypeOf(dirMeta)) {
            // Component
            var compMeta = dirMeta;
            changeDetectionStrategy = compMeta.changeDetection;
            if (compMeta.viewProviders) {
                viewProviders = this._getProvidersMetadata(compMeta.viewProviders, entryComponentMetadata, "viewProviders for \"" + stringifyType(directiveType) + "\"", [], directiveType);
            }
            if (compMeta.entryComponents) {
                entryComponentMetadata = flattenAndDedupeArray(compMeta.entryComponents)
                    .map(function (type) { return _this._getEntryComponentMetadata(type); })
                    .concat(entryComponentMetadata);
            }
            if (!selector) {
                selector = this._schemaRegistry.getDefaultComponentElementName();
            }
        }
        else {
            // Directive
            if (!selector) {
                this._reportError(syntaxError("Directive " + stringifyType(directiveType) + " has no selector, please add it!"), directiveType);
                selector = 'error';
            }
        }
        var providers = [];
        if (dirMeta.providers != null) {
            providers = this._getProvidersMetadata(dirMeta.providers, entryComponentMetadata, "providers for \"" + stringifyType(directiveType) + "\"", [], directiveType);
        }
        var queries = [];
        var viewQueries = [];
        if (dirMeta.queries != null) {
            queries = this._getQueriesMetadata(dirMeta.queries, false, directiveType);
            viewQueries = this._getQueriesMetadata(dirMeta.queries, true, directiveType);
        }
        var metadata = cpl.CompileDirectiveMetadata.create({
            isHost: false,
            selector: selector,
            exportAs: noUndefined(dirMeta.exportAs),
            isComponent: !!nonNormalizedTemplateMetadata,
            type: this._getTypeMetadata(directiveType),
            template: nonNormalizedTemplateMetadata,
            changeDetection: changeDetectionStrategy,
            inputs: dirMeta.inputs || [],
            outputs: dirMeta.outputs || [],
            host: dirMeta.host || {},
            providers: providers || [],
            viewProviders: viewProviders || [],
            queries: queries || [],
            guards: dirMeta.guards || {},
            viewQueries: viewQueries || [],
            entryComponents: entryComponentMetadata,
            componentViewType: nonNormalizedTemplateMetadata ? this.getComponentViewClass(directiveType) :
                null,
            rendererType: nonNormalizedTemplateMetadata ? this.getRendererType(directiveType) : null,
            componentFactory: null
        });
        if (nonNormalizedTemplateMetadata) {
            metadata.componentFactory =
                this.getComponentFactory(selector, directiveType, metadata.inputs, metadata.outputs);
        }
        cacheEntry = { metadata: metadata, annotation: dirMeta };
        this._nonNormalizedDirectiveCache.set(directiveType, cacheEntry);
        return cacheEntry;
    };
    /**
     * Gets the metadata for the given directive.
     * This assumes `loadNgModuleDirectiveAndPipeMetadata` has been called first.
     */
    CompileMetadataResolver.prototype.getDirectiveMetadata = function (directiveType) {
        var dirMeta = this._directiveCache.get(directiveType);
        if (!dirMeta) {
            this._reportError(syntaxError("Illegal state: getDirectiveMetadata can only be called after loadNgModuleDirectiveAndPipeMetadata for a module that declares it. Directive " + stringifyType(directiveType) + "."), directiveType);
        }
        return dirMeta;
    };
    CompileMetadataResolver.prototype.getDirectiveSummary = function (dirType) {
        var dirSummary = this._loadSummary(dirType, cpl.CompileSummaryKind.Directive);
        if (!dirSummary) {
            this._reportError(syntaxError("Illegal state: Could not load the summary for directive " + stringifyType(dirType) + "."), dirType);
        }
        return dirSummary;
    };
    CompileMetadataResolver.prototype.isDirective = function (type) {
        return !!this._loadSummary(type, cpl.CompileSummaryKind.Directive) ||
            this._directiveResolver.isDirective(type);
    };
    CompileMetadataResolver.prototype.isPipe = function (type) {
        return !!this._loadSummary(type, cpl.CompileSummaryKind.Pipe) ||
            this._pipeResolver.isPipe(type);
    };
    CompileMetadataResolver.prototype.isNgModule = function (type) {
        return !!this._loadSummary(type, cpl.CompileSummaryKind.NgModule) ||
            this._ngModuleResolver.isNgModule(type);
    };
    CompileMetadataResolver.prototype.getNgModuleSummary = function (moduleType, alreadyCollecting) {
        if (alreadyCollecting === void 0) { alreadyCollecting = null; }
        var moduleSummary = this._loadSummary(moduleType, cpl.CompileSummaryKind.NgModule);
        if (!moduleSummary) {
            var moduleMeta = this.getNgModuleMetadata(moduleType, false, alreadyCollecting);
            moduleSummary = moduleMeta ? moduleMeta.toSummary() : null;
            if (moduleSummary) {
                this._summaryCache.set(moduleType, moduleSummary);
            }
        }
        return moduleSummary;
    };
    /**
     * Loads the declared directives and pipes of an NgModule.
     */
    CompileMetadataResolver.prototype.loadNgModuleDirectiveAndPipeMetadata = function (moduleType, isSync, throwIfNotFound) {
        var _this = this;
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        var ngModule = this.getNgModuleMetadata(moduleType, throwIfNotFound);
        var loading = [];
        if (ngModule) {
            ngModule.declaredDirectives.forEach(function (id) {
                var promise = _this.loadDirectiveMetadata(moduleType, id.reference, isSync);
                if (promise) {
                    loading.push(promise);
                }
            });
            ngModule.declaredPipes.forEach(function (id) { return _this._loadPipeMetadata(id.reference); });
        }
        return Promise.all(loading);
    };
    CompileMetadataResolver.prototype.getShallowModuleMetadata = function (moduleType) {
        var compileMeta = this._shallowModuleCache.get(moduleType);
        if (compileMeta) {
            return compileMeta;
        }
        var ngModuleMeta = findLast(this._reflector.shallowAnnotations(moduleType), createNgModule.isTypeOf);
        compileMeta = {
            type: this._getTypeMetadata(moduleType),
            rawExports: ngModuleMeta.exports,
            rawImports: ngModuleMeta.imports,
            rawProviders: ngModuleMeta.providers,
        };
        this._shallowModuleCache.set(moduleType, compileMeta);
        return compileMeta;
    };
    CompileMetadataResolver.prototype.getNgModuleMetadata = function (moduleType, throwIfNotFound, alreadyCollecting) {
        var _this = this;
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        if (alreadyCollecting === void 0) { alreadyCollecting = null; }
        moduleType = resolveForwardRef(moduleType);
        var compileMeta = this._ngModuleCache.get(moduleType);
        if (compileMeta) {
            return compileMeta;
        }
        var meta = this._ngModuleResolver.resolve(moduleType, throwIfNotFound);
        if (!meta) {
            return null;
        }
        var declaredDirectives = [];
        var exportedNonModuleIdentifiers = [];
        var declaredPipes = [];
        var importedModules = [];
        var exportedModules = [];
        var providers = [];
        var entryComponents = [];
        var bootstrapComponents = [];
        var schemas = [];
        if (meta.imports) {
            flattenAndDedupeArray(meta.imports).forEach(function (importedType) {
                var importedModuleType = undefined;
                if (isValidType(importedType)) {
                    importedModuleType = importedType;
                }
                else if (importedType && importedType.ngModule) {
                    var moduleWithProviders = importedType;
                    importedModuleType = moduleWithProviders.ngModule;
                    if (moduleWithProviders.providers) {
                        providers.push.apply(providers, tslib_1.__spread(_this._getProvidersMetadata(moduleWithProviders.providers, entryComponents, "provider for the NgModule '" + stringifyType(importedModuleType) + "'", [], importedType)));
                    }
                }
                if (importedModuleType) {
                    if (_this._checkSelfImport(moduleType, importedModuleType))
                        return;
                    if (!alreadyCollecting)
                        alreadyCollecting = new Set();
                    if (alreadyCollecting.has(importedModuleType)) {
                        _this._reportError(syntaxError(_this._getTypeDescriptor(importedModuleType) + " '" + stringifyType(importedType) + "' is imported recursively by the module '" + stringifyType(moduleType) + "'."), moduleType);
                        return;
                    }
                    alreadyCollecting.add(importedModuleType);
                    var importedModuleSummary = _this.getNgModuleSummary(importedModuleType, alreadyCollecting);
                    alreadyCollecting.delete(importedModuleType);
                    if (!importedModuleSummary) {
                        _this._reportError(syntaxError("Unexpected " + _this._getTypeDescriptor(importedType) + " '" + stringifyType(importedType) + "' imported by the module '" + stringifyType(moduleType) + "'. Please add a @NgModule annotation."), moduleType);
                        return;
                    }
                    importedModules.push(importedModuleSummary);
                }
                else {
                    _this._reportError(syntaxError("Unexpected value '" + stringifyType(importedType) + "' imported by the module '" + stringifyType(moduleType) + "'"), moduleType);
                    return;
                }
            });
        }
        if (meta.exports) {
            flattenAndDedupeArray(meta.exports).forEach(function (exportedType) {
                if (!isValidType(exportedType)) {
                    _this._reportError(syntaxError("Unexpected value '" + stringifyType(exportedType) + "' exported by the module '" + stringifyType(moduleType) + "'"), moduleType);
                    return;
                }
                if (!alreadyCollecting)
                    alreadyCollecting = new Set();
                if (alreadyCollecting.has(exportedType)) {
                    _this._reportError(syntaxError(_this._getTypeDescriptor(exportedType) + " '" + stringify(exportedType) + "' is exported recursively by the module '" + stringifyType(moduleType) + "'"), moduleType);
                    return;
                }
                alreadyCollecting.add(exportedType);
                var exportedModuleSummary = _this.getNgModuleSummary(exportedType, alreadyCollecting);
                alreadyCollecting.delete(exportedType);
                if (exportedModuleSummary) {
                    exportedModules.push(exportedModuleSummary);
                }
                else {
                    exportedNonModuleIdentifiers.push(_this._getIdentifierMetadata(exportedType));
                }
            });
        }
        // Note: This will be modified later, so we rely on
        // getting a new instance every time!
        var transitiveModule = this._getTransitiveNgModuleMetadata(importedModules, exportedModules);
        if (meta.declarations) {
            flattenAndDedupeArray(meta.declarations).forEach(function (declaredType) {
                if (!isValidType(declaredType)) {
                    _this._reportError(syntaxError("Unexpected value '" + stringifyType(declaredType) + "' declared by the module '" + stringifyType(moduleType) + "'"), moduleType);
                    return;
                }
                var declaredIdentifier = _this._getIdentifierMetadata(declaredType);
                if (_this.isDirective(declaredType)) {
                    transitiveModule.addDirective(declaredIdentifier);
                    declaredDirectives.push(declaredIdentifier);
                    _this._addTypeToModule(declaredType, moduleType);
                }
                else if (_this.isPipe(declaredType)) {
                    transitiveModule.addPipe(declaredIdentifier);
                    transitiveModule.pipes.push(declaredIdentifier);
                    declaredPipes.push(declaredIdentifier);
                    _this._addTypeToModule(declaredType, moduleType);
                }
                else {
                    _this._reportError(syntaxError("Unexpected " + _this._getTypeDescriptor(declaredType) + " '" + stringifyType(declaredType) + "' declared by the module '" + stringifyType(moduleType) + "'. Please add a @Pipe/@Directive/@Component annotation."), moduleType);
                    return;
                }
            });
        }
        var exportedDirectives = [];
        var exportedPipes = [];
        exportedNonModuleIdentifiers.forEach(function (exportedId) {
            if (transitiveModule.directivesSet.has(exportedId.reference)) {
                exportedDirectives.push(exportedId);
                transitiveModule.addExportedDirective(exportedId);
            }
            else if (transitiveModule.pipesSet.has(exportedId.reference)) {
                exportedPipes.push(exportedId);
                transitiveModule.addExportedPipe(exportedId);
            }
            else {
                _this._reportError(syntaxError("Can't export " + _this._getTypeDescriptor(exportedId.reference) + " " + stringifyType(exportedId.reference) + " from " + stringifyType(moduleType) + " as it was neither declared nor imported!"), moduleType);
                return;
            }
        });
        // The providers of the module have to go last
        // so that they overwrite any other provider we already added.
        if (meta.providers) {
            providers.push.apply(providers, tslib_1.__spread(this._getProvidersMetadata(meta.providers, entryComponents, "provider for the NgModule '" + stringifyType(moduleType) + "'", [], moduleType)));
        }
        if (meta.entryComponents) {
            entryComponents.push.apply(entryComponents, tslib_1.__spread(flattenAndDedupeArray(meta.entryComponents)
                .map(function (type) { return _this._getEntryComponentMetadata(type); })));
        }
        if (meta.bootstrap) {
            flattenAndDedupeArray(meta.bootstrap).forEach(function (type) {
                if (!isValidType(type)) {
                    _this._reportError(syntaxError("Unexpected value '" + stringifyType(type) + "' used in the bootstrap property of module '" + stringifyType(moduleType) + "'"), moduleType);
                    return;
                }
                bootstrapComponents.push(_this._getIdentifierMetadata(type));
            });
        }
        entryComponents.push.apply(entryComponents, tslib_1.__spread(bootstrapComponents.map(function (type) { return _this._getEntryComponentMetadata(type.reference); })));
        if (meta.schemas) {
            schemas.push.apply(schemas, tslib_1.__spread(flattenAndDedupeArray(meta.schemas)));
        }
        compileMeta = new cpl.CompileNgModuleMetadata({
            type: this._getTypeMetadata(moduleType),
            providers: providers,
            entryComponents: entryComponents,
            bootstrapComponents: bootstrapComponents,
            schemas: schemas,
            declaredDirectives: declaredDirectives,
            exportedDirectives: exportedDirectives,
            declaredPipes: declaredPipes,
            exportedPipes: exportedPipes,
            importedModules: importedModules,
            exportedModules: exportedModules,
            transitiveModule: transitiveModule,
            id: meta.id || null,
        });
        entryComponents.forEach(function (id) { return transitiveModule.addEntryComponent(id); });
        providers.forEach(function (provider) { return transitiveModule.addProvider(provider, compileMeta.type); });
        transitiveModule.addModule(compileMeta.type);
        this._ngModuleCache.set(moduleType, compileMeta);
        return compileMeta;
    };
    CompileMetadataResolver.prototype._checkSelfImport = function (moduleType, importedModuleType) {
        if (moduleType === importedModuleType) {
            this._reportError(syntaxError("'" + stringifyType(moduleType) + "' module can't import itself"), moduleType);
            return true;
        }
        return false;
    };
    CompileMetadataResolver.prototype._getTypeDescriptor = function (type) {
        if (isValidType(type)) {
            if (this.isDirective(type)) {
                return 'directive';
            }
            if (this.isPipe(type)) {
                return 'pipe';
            }
            if (this.isNgModule(type)) {
                return 'module';
            }
        }
        if (type.provide) {
            return 'provider';
        }
        return 'value';
    };
    CompileMetadataResolver.prototype._addTypeToModule = function (type, moduleType) {
        var oldModule = this._ngModuleOfTypes.get(type);
        if (oldModule && oldModule !== moduleType) {
            this._reportError(syntaxError("Type " + stringifyType(type) + " is part of the declarations of 2 modules: " + stringifyType(oldModule) + " and " + stringifyType(moduleType) + "! " +
                ("Please consider moving " + stringifyType(type) + " to a higher module that imports " + stringifyType(oldModule) + " and " + stringifyType(moduleType) + ". ") +
                ("You can also create a new NgModule that exports and includes " + stringifyType(type) + " then import that NgModule in " + stringifyType(oldModule) + " and " + stringifyType(moduleType) + ".")), moduleType);
            return;
        }
        this._ngModuleOfTypes.set(type, moduleType);
    };
    CompileMetadataResolver.prototype._getTransitiveNgModuleMetadata = function (importedModules, exportedModules) {
        // collect `providers` / `entryComponents` from all imported and all exported modules
        var result = new cpl.TransitiveCompileNgModuleMetadata();
        var modulesByToken = new Map();
        importedModules.concat(exportedModules).forEach(function (modSummary) {
            modSummary.modules.forEach(function (mod) { return result.addModule(mod); });
            modSummary.entryComponents.forEach(function (comp) { return result.addEntryComponent(comp); });
            var addedTokens = new Set();
            modSummary.providers.forEach(function (entry) {
                var tokenRef = cpl.tokenReference(entry.provider.token);
                var prevModules = modulesByToken.get(tokenRef);
                if (!prevModules) {
                    prevModules = new Set();
                    modulesByToken.set(tokenRef, prevModules);
                }
                var moduleRef = entry.module.reference;
                // Note: the providers of one module may still contain multiple providers
                // per token (e.g. for multi providers), and we need to preserve these.
                if (addedTokens.has(tokenRef) || !prevModules.has(moduleRef)) {
                    prevModules.add(moduleRef);
                    addedTokens.add(tokenRef);
                    result.addProvider(entry.provider, entry.module);
                }
            });
        });
        exportedModules.forEach(function (modSummary) {
            modSummary.exportedDirectives.forEach(function (id) { return result.addExportedDirective(id); });
            modSummary.exportedPipes.forEach(function (id) { return result.addExportedPipe(id); });
        });
        importedModules.forEach(function (modSummary) {
            modSummary.exportedDirectives.forEach(function (id) { return result.addDirective(id); });
            modSummary.exportedPipes.forEach(function (id) { return result.addPipe(id); });
        });
        return result;
    };
    CompileMetadataResolver.prototype._getIdentifierMetadata = function (type) {
        type = resolveForwardRef(type);
        return { reference: type };
    };
    CompileMetadataResolver.prototype.isInjectable = function (type) {
        var annotations = this._reflector.tryAnnotations(type);
        return annotations.some(function (ann) { return createInjectable.isTypeOf(ann); });
    };
    CompileMetadataResolver.prototype.getInjectableSummary = function (type) {
        return {
            summaryKind: cpl.CompileSummaryKind.Injectable,
            type: this._getTypeMetadata(type, null, false)
        };
    };
    CompileMetadataResolver.prototype.getInjectableMetadata = function (type, dependencies, throwOnUnknownDeps) {
        if (dependencies === void 0) { dependencies = null; }
        if (throwOnUnknownDeps === void 0) { throwOnUnknownDeps = true; }
        var typeSummary = this._loadSummary(type, cpl.CompileSummaryKind.Injectable);
        var typeMetadata = typeSummary ?
            typeSummary.type :
            this._getTypeMetadata(type, dependencies, throwOnUnknownDeps);
        var annotations = this._reflector.annotations(type).filter(function (ann) { return createInjectable.isTypeOf(ann); });
        if (annotations.length === 0) {
            return null;
        }
        var meta = annotations[annotations.length - 1];
        return {
            symbol: type,
            type: typeMetadata,
            providedIn: meta.providedIn,
            useValue: meta.useValue,
            useClass: meta.useClass,
            useExisting: meta.useExisting,
            useFactory: meta.useFactory,
            deps: meta.deps,
        };
    };
    CompileMetadataResolver.prototype._getTypeMetadata = function (type, dependencies, throwOnUnknownDeps) {
        if (dependencies === void 0) { dependencies = null; }
        if (throwOnUnknownDeps === void 0) { throwOnUnknownDeps = true; }
        var identifier = this._getIdentifierMetadata(type);
        return {
            reference: identifier.reference,
            diDeps: this._getDependenciesMetadata(identifier.reference, dependencies, throwOnUnknownDeps),
            lifecycleHooks: getAllLifecycleHooks(this._reflector, identifier.reference),
        };
    };
    CompileMetadataResolver.prototype._getFactoryMetadata = function (factory, dependencies) {
        if (dependencies === void 0) { dependencies = null; }
        factory = resolveForwardRef(factory);
        return { reference: factory, diDeps: this._getDependenciesMetadata(factory, dependencies) };
    };
    /**
     * Gets the metadata for the given pipe.
     * This assumes `loadNgModuleDirectiveAndPipeMetadata` has been called first.
     */
    CompileMetadataResolver.prototype.getPipeMetadata = function (pipeType) {
        var pipeMeta = this._pipeCache.get(pipeType);
        if (!pipeMeta) {
            this._reportError(syntaxError("Illegal state: getPipeMetadata can only be called after loadNgModuleDirectiveAndPipeMetadata for a module that declares it. Pipe " + stringifyType(pipeType) + "."), pipeType);
        }
        return pipeMeta || null;
    };
    CompileMetadataResolver.prototype.getPipeSummary = function (pipeType) {
        var pipeSummary = this._loadSummary(pipeType, cpl.CompileSummaryKind.Pipe);
        if (!pipeSummary) {
            this._reportError(syntaxError("Illegal state: Could not load the summary for pipe " + stringifyType(pipeType) + "."), pipeType);
        }
        return pipeSummary;
    };
    CompileMetadataResolver.prototype.getOrLoadPipeMetadata = function (pipeType) {
        var pipeMeta = this._pipeCache.get(pipeType);
        if (!pipeMeta) {
            pipeMeta = this._loadPipeMetadata(pipeType);
        }
        return pipeMeta;
    };
    CompileMetadataResolver.prototype._loadPipeMetadata = function (pipeType) {
        pipeType = resolveForwardRef(pipeType);
        var pipeAnnotation = this._pipeResolver.resolve(pipeType);
        var pipeMeta = new cpl.CompilePipeMetadata({
            type: this._getTypeMetadata(pipeType),
            name: pipeAnnotation.name,
            pure: !!pipeAnnotation.pure
        });
        this._pipeCache.set(pipeType, pipeMeta);
        this._summaryCache.set(pipeType, pipeMeta.toSummary());
        return pipeMeta;
    };
    CompileMetadataResolver.prototype._getDependenciesMetadata = function (typeOrFunc, dependencies, throwOnUnknownDeps) {
        var _this = this;
        if (throwOnUnknownDeps === void 0) { throwOnUnknownDeps = true; }
        var hasUnknownDeps = false;
        var params = dependencies || this._reflector.parameters(typeOrFunc) || [];
        var dependenciesMetadata = params.map(function (param) {
            var isAttribute = false;
            var isHost = false;
            var isSelf = false;
            var isSkipSelf = false;
            var isOptional = false;
            var token = null;
            if (Array.isArray(param)) {
                param.forEach(function (paramEntry) {
                    if (createHost.isTypeOf(paramEntry)) {
                        isHost = true;
                    }
                    else if (createSelf.isTypeOf(paramEntry)) {
                        isSelf = true;
                    }
                    else if (createSkipSelf.isTypeOf(paramEntry)) {
                        isSkipSelf = true;
                    }
                    else if (createOptional.isTypeOf(paramEntry)) {
                        isOptional = true;
                    }
                    else if (createAttribute.isTypeOf(paramEntry)) {
                        isAttribute = true;
                        token = paramEntry.attributeName;
                    }
                    else if (createInject.isTypeOf(paramEntry)) {
                        token = paramEntry.token;
                    }
                    else if (createInjectionToken.isTypeOf(paramEntry) ||
                        paramEntry instanceof StaticSymbol) {
                        token = paramEntry;
                    }
                    else if (isValidType(paramEntry) && token == null) {
                        token = paramEntry;
                    }
                });
            }
            else {
                token = param;
            }
            if (token == null) {
                hasUnknownDeps = true;
                return {};
            }
            return {
                isAttribute: isAttribute,
                isHost: isHost,
                isSelf: isSelf,
                isSkipSelf: isSkipSelf,
                isOptional: isOptional,
                token: _this._getTokenMetadata(token)
            };
        });
        if (hasUnknownDeps) {
            var depsTokens = dependenciesMetadata.map(function (dep) { return dep.token ? stringifyType(dep.token) : '?'; }).join(', ');
            var message = "Can't resolve all parameters for " + stringifyType(typeOrFunc) + ": (" + depsTokens + ").";
            if (throwOnUnknownDeps || this._config.strictInjectionParameters) {
                this._reportError(syntaxError(message), typeOrFunc);
            }
            else {
                this._console.warn("Warning: " + message + " This will become an error in Angular v6.x");
            }
        }
        return dependenciesMetadata;
    };
    CompileMetadataResolver.prototype._getTokenMetadata = function (token) {
        token = resolveForwardRef(token);
        var compileToken;
        if (typeof token === 'string') {
            compileToken = { value: token };
        }
        else {
            compileToken = { identifier: { reference: token } };
        }
        return compileToken;
    };
    CompileMetadataResolver.prototype._getProvidersMetadata = function (providers, targetEntryComponents, debugInfo, compileProviders, type) {
        var _this = this;
        if (compileProviders === void 0) { compileProviders = []; }
        providers.forEach(function (provider, providerIdx) {
            if (Array.isArray(provider)) {
                _this._getProvidersMetadata(provider, targetEntryComponents, debugInfo, compileProviders);
            }
            else {
                provider = resolveForwardRef(provider);
                var providerMeta = undefined;
                if (provider && typeof provider === 'object' && provider.hasOwnProperty('provide')) {
                    _this._validateProvider(provider);
                    providerMeta = new cpl.ProviderMeta(provider.provide, provider);
                }
                else if (isValidType(provider)) {
                    providerMeta = new cpl.ProviderMeta(provider, { useClass: provider });
                }
                else if (provider === void 0) {
                    _this._reportError(syntaxError("Encountered undefined provider! Usually this means you have a circular dependencies. This might be caused by using 'barrel' index.ts files."));
                    return;
                }
                else {
                    var providersInfo = providers.reduce(function (soFar, seenProvider, seenProviderIdx) {
                        if (seenProviderIdx < providerIdx) {
                            soFar.push("" + stringifyType(seenProvider));
                        }
                        else if (seenProviderIdx == providerIdx) {
                            soFar.push("?" + stringifyType(seenProvider) + "?");
                        }
                        else if (seenProviderIdx == providerIdx + 1) {
                            soFar.push('...');
                        }
                        return soFar;
                    }, [])
                        .join(', ');
                    _this._reportError(syntaxError("Invalid " + (debugInfo ? debugInfo : 'provider') + " - only instances of Provider and Type are allowed, got: [" + providersInfo + "]"), type);
                    return;
                }
                if (providerMeta.token ===
                    _this._reflector.resolveExternalReference(Identifiers.ANALYZE_FOR_ENTRY_COMPONENTS)) {
                    targetEntryComponents.push.apply(targetEntryComponents, tslib_1.__spread(_this._getEntryComponentsFromProvider(providerMeta, type)));
                }
                else {
                    compileProviders.push(_this.getProviderMetadata(providerMeta));
                }
            }
        });
        return compileProviders;
    };
    CompileMetadataResolver.prototype._validateProvider = function (provider) {
        if (provider.hasOwnProperty('useClass') && provider.useClass == null) {
            this._reportError(syntaxError("Invalid provider for " + stringifyType(provider.provide) + ". useClass cannot be " + provider.useClass + ".\n           Usually it happens when:\n           1. There's a circular dependency (might be caused by using index.ts (barrel) files).\n           2. Class was used before it was declared. Use forwardRef in this case."));
        }
    };
    CompileMetadataResolver.prototype._getEntryComponentsFromProvider = function (provider, type) {
        var _this = this;
        var components = [];
        var collectedIdentifiers = [];
        if (provider.useFactory || provider.useExisting || provider.useClass) {
            this._reportError(syntaxError("The ANALYZE_FOR_ENTRY_COMPONENTS token only supports useValue!"), type);
            return [];
        }
        if (!provider.multi) {
            this._reportError(syntaxError("The ANALYZE_FOR_ENTRY_COMPONENTS token only supports 'multi = true'!"), type);
            return [];
        }
        extractIdentifiers(provider.useValue, collectedIdentifiers);
        collectedIdentifiers.forEach(function (identifier) {
            var entry = _this._getEntryComponentMetadata(identifier.reference, false);
            if (entry) {
                components.push(entry);
            }
        });
        return components;
    };
    CompileMetadataResolver.prototype._getEntryComponentMetadata = function (dirType, throwIfNotFound) {
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        var dirMeta = this.getNonNormalizedDirectiveMetadata(dirType);
        if (dirMeta && dirMeta.metadata.isComponent) {
            return { componentType: dirType, componentFactory: dirMeta.metadata.componentFactory };
        }
        var dirSummary = this._loadSummary(dirType, cpl.CompileSummaryKind.Directive);
        if (dirSummary && dirSummary.isComponent) {
            return { componentType: dirType, componentFactory: dirSummary.componentFactory };
        }
        if (throwIfNotFound) {
            throw syntaxError(dirType.name + " cannot be used as an entry component.");
        }
        return null;
    };
    CompileMetadataResolver.prototype._getInjectableTypeMetadata = function (type, dependencies) {
        if (dependencies === void 0) { dependencies = null; }
        var typeSummary = this._loadSummary(type, cpl.CompileSummaryKind.Injectable);
        if (typeSummary) {
            return typeSummary.type;
        }
        return this._getTypeMetadata(type, dependencies);
    };
    CompileMetadataResolver.prototype.getProviderMetadata = function (provider) {
        var compileDeps = undefined;
        var compileTypeMetadata = null;
        var compileFactoryMetadata = null;
        var token = this._getTokenMetadata(provider.token);
        if (provider.useClass) {
            compileTypeMetadata =
                this._getInjectableTypeMetadata(provider.useClass, provider.dependencies);
            compileDeps = compileTypeMetadata.diDeps;
            if (provider.token === provider.useClass) {
                // use the compileTypeMetadata as it contains information about lifecycleHooks...
                token = { identifier: compileTypeMetadata };
            }
        }
        else if (provider.useFactory) {
            compileFactoryMetadata = this._getFactoryMetadata(provider.useFactory, provider.dependencies);
            compileDeps = compileFactoryMetadata.diDeps;
        }
        return {
            token: token,
            useClass: compileTypeMetadata,
            useValue: provider.useValue,
            useFactory: compileFactoryMetadata,
            useExisting: provider.useExisting ? this._getTokenMetadata(provider.useExisting) : undefined,
            deps: compileDeps,
            multi: provider.multi
        };
    };
    CompileMetadataResolver.prototype._getQueriesMetadata = function (queries, isViewQuery, directiveType) {
        var _this = this;
        var res = [];
        Object.keys(queries).forEach(function (propertyName) {
            var query = queries[propertyName];
            if (query.isViewQuery === isViewQuery) {
                res.push(_this._getQueryMetadata(query, propertyName, directiveType));
            }
        });
        return res;
    };
    CompileMetadataResolver.prototype._queryVarBindings = function (selector) { return selector.split(/\s*,\s*/); };
    CompileMetadataResolver.prototype._getQueryMetadata = function (q, propertyName, typeOrFunc) {
        var _this = this;
        var selectors;
        if (typeof q.selector === 'string') {
            selectors =
                this._queryVarBindings(q.selector).map(function (varName) { return _this._getTokenMetadata(varName); });
        }
        else {
            if (!q.selector) {
                this._reportError(syntaxError("Can't construct a query for the property \"" + propertyName + "\" of \"" + stringifyType(typeOrFunc) + "\" since the query selector wasn't defined."), typeOrFunc);
                selectors = [];
            }
            else {
                selectors = [this._getTokenMetadata(q.selector)];
            }
        }
        return {
            selectors: selectors,
            first: q.first,
            descendants: q.descendants, propertyName: propertyName,
            read: q.read ? this._getTokenMetadata(q.read) : null,
            static: q.static
        };
    };
    CompileMetadataResolver.prototype._reportError = function (error, type, otherType) {
        if (this._errorCollector) {
            this._errorCollector(error, type);
            if (otherType) {
                this._errorCollector(error, otherType);
            }
        }
        else {
            throw error;
        }
    };
    return CompileMetadataResolver;
}());
export { CompileMetadataResolver };
function flattenArray(tree, out) {
    if (out === void 0) { out = []; }
    if (tree) {
        for (var i = 0; i < tree.length; i++) {
            var item = resolveForwardRef(tree[i]);
            if (Array.isArray(item)) {
                flattenArray(item, out);
            }
            else {
                out.push(item);
            }
        }
    }
    return out;
}
function dedupeArray(array) {
    if (array) {
        return Array.from(new Set(array));
    }
    return [];
}
function flattenAndDedupeArray(tree) {
    return dedupeArray(flattenArray(tree));
}
function isValidType(value) {
    return (value instanceof StaticSymbol) || (value instanceof Type);
}
function extractIdentifiers(value, targetIdentifiers) {
    visitValue(value, new _CompileValueConverter(), targetIdentifiers);
}
var _CompileValueConverter = /** @class */ (function (_super) {
    tslib_1.__extends(_CompileValueConverter, _super);
    function _CompileValueConverter() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    _CompileValueConverter.prototype.visitOther = function (value, targetIdentifiers) {
        targetIdentifiers.push({ reference: value });
    };
    return _CompileValueConverter;
}(ValueTransformer));
function stringifyType(type) {
    if (type instanceof StaticSymbol) {
        return type.name + " in " + type.filePath;
    }
    else {
        return stringify(type);
    }
}
/**
 * Indicates that a component is still being loaded in a synchronous compile.
 */
function componentStillLoadingError(compType) {
    var error = Error("Can't compile synchronously as " + stringify(compType) + " is still being loaded!");
    error[ERROR_COMPONENT_TYPE] = compType;
    return error;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWV0YWRhdGFfcmVzb2x2ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvbWV0YWRhdGFfcmVzb2x2ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQyxZQUFZLEVBQW9CLE1BQU0scUJBQXFCLENBQUM7QUFDcEUsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sWUFBWSxDQUFDO0FBQzdDLE9BQU8sRUFBQyxvQkFBb0IsRUFBRSwwQkFBMEIsRUFBQyxNQUFNLGNBQWMsQ0FBQztBQUM5RSxPQUFPLEtBQUssR0FBRyxNQUFNLG9CQUFvQixDQUFDO0FBRzFDLE9BQU8sRUFBQyx1QkFBdUIsRUFBMEYsSUFBSSxFQUFFLGlCQUFpQixFQUFFLGVBQWUsRUFBRSxlQUFlLEVBQUUsVUFBVSxFQUFFLFlBQVksRUFBRSxnQkFBZ0IsRUFBRSxvQkFBb0IsRUFBRSxjQUFjLEVBQUUsY0FBYyxFQUFFLFVBQVUsRUFBRSxjQUFjLEVBQUMsTUFBTSxRQUFRLENBQUM7QUFFaFUsT0FBTyxFQUFvQixRQUFRLEVBQUMsTUFBTSxzQkFBc0IsQ0FBQztBQUNqRSxPQUFPLEVBQUMsV0FBVyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQzFDLE9BQU8sRUFBQyxvQkFBb0IsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBSzNELE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSxZQUFZLENBQUM7QUFFdkMsT0FBTyxFQUFVLFNBQVMsRUFBRSxnQkFBZ0IsRUFBRSxTQUFTLEVBQUUsV0FBVyxFQUFFLGlCQUFpQixFQUFFLFNBQVMsRUFBRSxXQUFXLEVBQUUsVUFBVSxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBSTNJLE1BQU0sQ0FBQyxJQUFNLG9CQUFvQixHQUFHLGlCQUFpQixDQUFDO0FBRXRELGdCQUFnQjtBQUNoQixrQ0FBa0M7QUFDbEMsMkRBQTJE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QUFTLGtCQUFrQixDQUFDLEtBQVUsRUFBRSxpQkFBa0Q7SUFDeEYsVUFBVSxDQUFDLEtBQUssRUFBRSxJQUFJLHNCQUFzQixFQUFFLEVBQUUsaUJBQWlCLENBQUMsQ0FBQztBQUNyRSxDQUFDO0FBRUQ7SUFBcUMsa0RBQWdCO0lBQXJEOztJQUlBLENBQUM7SUFIQywyQ0FBVSxHQUFWLFVBQVcsS0FBVSxFQUFFLGlCQUFrRDtRQUN2RSxpQkFBaUIsQ0FBQyxJQUFJLENBQUMsRUFBQyxTQUFTLEVBQUUsS0FBSyxFQUFDLENBQUMsQ0FBQztJQUM3QyxDQUFDO0lBQ0gsNkJBQUM7QUFBRCxDQUFDLEFBSkQsQ0FBcUMsZ0JBQWdCLEdBSXBEO0FBRUQsU0FBUyxhQUFhLENBQUMsSUFBUztJQUM5QixJQUFJLElBQUksWUFBWSxZQUFZLEVBQUU7UUFDaEMsT0FBVSxJQUFJLENBQUMsSUFBSSxZQUFPLElBQUksQ0FBQyxRQUFVLENBQUM7S0FDM0M7U0FBTTtRQUNMLE9BQU8sU0FBUyxDQUFDLElBQUksQ0FBQyxDQUFDO0tBQ3hCO0FBQ0gsQ0FBQztBQUVEOztHQUVHO0FBQ0gsU0FBUywwQkFBMEIsQ0FBQyxRQUFjO0lBQ2hELElBQU0sS0FBSyxHQUNQLEtBQUssQ0FBQyxvQ0FBa0MsU0FBUyxDQUFDLFFBQVEsQ0FBQyw0QkFBeUIsQ0FBQyxDQUFDO0lBQ3pGLEtBQWEsQ0FBQyxvQkFBb0IsQ0FBQyxHQUFHLFFBQVEsQ0FBQztJQUNoRCxPQUFPLEtBQUssQ0FBQztBQUNmLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2xDYWNoZX0gZnJvbSAnLi9hb3Qvc3RhdGljX3N5bWJvbCc7XG5pbXBvcnQge25nZmFjdG9yeUZpbGVQYXRofSBmcm9tICcuL2FvdC91dGlsJztcbmltcG9ydCB7YXNzZXJ0QXJyYXlPZlN0cmluZ3MsIGFzc2VydEludGVycG9sYXRpb25TeW1ib2xzfSBmcm9tICcuL2Fzc2VydGlvbnMnO1xuaW1wb3J0ICogYXMgY3BsIGZyb20gJy4vY29tcGlsZV9tZXRhZGF0YSc7XG5pbXBvcnQge0NvbXBpbGVSZWZsZWN0b3J9IGZyb20gJy4vY29tcGlsZV9yZWZsZWN0b3InO1xuaW1wb3J0IHtDb21waWxlckNvbmZpZ30gZnJvbSAnLi9jb25maWcnO1xuaW1wb3J0IHtDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSwgQ29tcG9uZW50LCBEaXJlY3RpdmUsIEluamVjdGFibGUsIE1vZHVsZVdpdGhQcm92aWRlcnMsIFByb3ZpZGVyLCBRdWVyeSwgU2NoZW1hTWV0YWRhdGEsIFR5cGUsIFZpZXdFbmNhcHN1bGF0aW9uLCBjcmVhdGVBdHRyaWJ1dGUsIGNyZWF0ZUNvbXBvbmVudCwgY3JlYXRlSG9zdCwgY3JlYXRlSW5qZWN0LCBjcmVhdGVJbmplY3RhYmxlLCBjcmVhdGVJbmplY3Rpb25Ub2tlbiwgY3JlYXRlTmdNb2R1bGUsIGNyZWF0ZU9wdGlvbmFsLCBjcmVhdGVTZWxmLCBjcmVhdGVTa2lwU2VsZn0gZnJvbSAnLi9jb3JlJztcbmltcG9ydCB7RGlyZWN0aXZlTm9ybWFsaXplcn0gZnJvbSAnLi9kaXJlY3RpdmVfbm9ybWFsaXplcic7XG5pbXBvcnQge0RpcmVjdGl2ZVJlc29sdmVyLCBmaW5kTGFzdH0gZnJvbSAnLi9kaXJlY3RpdmVfcmVzb2x2ZXInO1xuaW1wb3J0IHtJZGVudGlmaWVyc30gZnJvbSAnLi9pZGVudGlmaWVycyc7XG5pbXBvcnQge2dldEFsbExpZmVjeWNsZUhvb2tzfSBmcm9tICcuL2xpZmVjeWNsZV9yZWZsZWN0b3InO1xuaW1wb3J0IHtIdG1sUGFyc2VyfSBmcm9tICcuL21sX3BhcnNlci9odG1sX3BhcnNlcic7XG5pbXBvcnQge05nTW9kdWxlUmVzb2x2ZXJ9IGZyb20gJy4vbmdfbW9kdWxlX3Jlc29sdmVyJztcbmltcG9ydCB7UGlwZVJlc29sdmVyfSBmcm9tICcuL3BpcGVfcmVzb2x2ZXInO1xuaW1wb3J0IHtFbGVtZW50U2NoZW1hUmVnaXN0cnl9IGZyb20gJy4vc2NoZW1hL2VsZW1lbnRfc2NoZW1hX3JlZ2lzdHJ5JztcbmltcG9ydCB7Q3NzU2VsZWN0b3J9IGZyb20gJy4vc2VsZWN0b3InO1xuaW1wb3J0IHtTdW1tYXJ5UmVzb2x2ZXJ9IGZyb20gJy4vc3VtbWFyeV9yZXNvbHZlcic7XG5pbXBvcnQge0NvbnNvbGUsIFN5bmNBc3luYywgVmFsdWVUcmFuc2Zvcm1lciwgaXNQcm9taXNlLCBub1VuZGVmaW5lZCwgcmVzb2x2ZUZvcndhcmRSZWYsIHN0cmluZ2lmeSwgc3ludGF4RXJyb3IsIHZpc2l0VmFsdWV9IGZyb20gJy4vdXRpbCc7XG5cbmV4cG9ydCB0eXBlIEVycm9yQ29sbGVjdG9yID0gKGVycm9yOiBhbnksIHR5cGU/OiBhbnkpID0+IHZvaWQ7XG5cbmV4cG9ydCBjb25zdCBFUlJPUl9DT01QT05FTlRfVFlQRSA9ICduZ0NvbXBvbmVudFR5cGUnO1xuXG4vLyBEZXNpZ24gbm90ZXM6XG4vLyAtIGRvbid0IGxhemlseSBjcmVhdGUgbWV0YWRhdGE6XG4vLyAgIEZvciBzb21lIG1ldGFkYXRhLCB3ZSBuZWVkIHRvIGRvIGFzeW5jIHdvcmsgc29tZXRpbWVzLFxuLy8gICBzbyB0aGUgdXNlciBoYXMgdG8ga2ljayBvZmYgdGhpcyBsb2FkaW5nLlxuLy8gICBCdXQgd2Ugd2FudCB0byByZXBvcnQgZXJyb3JzIGV2ZW4gd2hlbiB0aGUgYXN5bmMgd29yayBpc1xuLy8gICBub3QgcmVxdWlyZWQgdG8gY2hlY2sgdGhhdCB0aGUgdXNlciB3b3VsZCBoYXZlIGJlZW4gYWJsZVxuLy8gICB0byB3YWl0IGNvcnJlY3RseS5cbmV4cG9ydCBjbGFzcyBDb21waWxlTWV0YWRhdGFSZXNvbHZlciB7XG4gIHByaXZhdGUgX25vbk5vcm1hbGl6ZWREaXJlY3RpdmVDYWNoZSA9XG4gICAgICBuZXcgTWFwPFR5cGUsIHthbm5vdGF0aW9uOiBEaXJlY3RpdmUsIG1ldGFkYXRhOiBjcGwuQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhfT4oKTtcbiAgcHJpdmF0ZSBfZGlyZWN0aXZlQ2FjaGUgPSBuZXcgTWFwPFR5cGUsIGNwbC5Db21waWxlRGlyZWN0aXZlTWV0YWRhdGE+KCk7XG4gIHByaXZhdGUgX3N1bW1hcnlDYWNoZSA9IG5ldyBNYXA8VHlwZSwgY3BsLkNvbXBpbGVUeXBlU3VtbWFyeXxudWxsPigpO1xuICBwcml2YXRlIF9waXBlQ2FjaGUgPSBuZXcgTWFwPFR5cGUsIGNwbC5Db21waWxlUGlwZU1ldGFkYXRhPigpO1xuICBwcml2YXRlIF9uZ01vZHVsZUNhY2hlID0gbmV3IE1hcDxUeXBlLCBjcGwuQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGE+KCk7XG4gIHByaXZhdGUgX25nTW9kdWxlT2ZUeXBlcyA9IG5ldyBNYXA8VHlwZSwgVHlwZT4oKTtcbiAgcHJpdmF0ZSBfc2hhbGxvd01vZHVsZUNhY2hlID0gbmV3IE1hcDxUeXBlLCBjcGwuQ29tcGlsZVNoYWxsb3dNb2R1bGVNZXRhZGF0YT4oKTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgX2NvbmZpZzogQ29tcGlsZXJDb25maWcsIHByaXZhdGUgX2h0bWxQYXJzZXI6IEh0bWxQYXJzZXIsXG4gICAgICBwcml2YXRlIF9uZ01vZHVsZVJlc29sdmVyOiBOZ01vZHVsZVJlc29sdmVyLCBwcml2YXRlIF9kaXJlY3RpdmVSZXNvbHZlcjogRGlyZWN0aXZlUmVzb2x2ZXIsXG4gICAgICBwcml2YXRlIF9waXBlUmVzb2x2ZXI6IFBpcGVSZXNvbHZlciwgcHJpdmF0ZSBfc3VtbWFyeVJlc29sdmVyOiBTdW1tYXJ5UmVzb2x2ZXI8YW55PixcbiAgICAgIHByaXZhdGUgX3NjaGVtYVJlZ2lzdHJ5OiBFbGVtZW50U2NoZW1hUmVnaXN0cnksXG4gICAgICBwcml2YXRlIF9kaXJlY3RpdmVOb3JtYWxpemVyOiBEaXJlY3RpdmVOb3JtYWxpemVyLCBwcml2YXRlIF9jb25zb2xlOiBDb25zb2xlLFxuICAgICAgcHJpdmF0ZSBfc3RhdGljU3ltYm9sQ2FjaGU6IFN0YXRpY1N5bWJvbENhY2hlLCBwcml2YXRlIF9yZWZsZWN0b3I6IENvbXBpbGVSZWZsZWN0b3IsXG4gICAgICBwcml2YXRlIF9lcnJvckNvbGxlY3Rvcj86IEVycm9yQ29sbGVjdG9yKSB7fVxuXG4gIGdldFJlZmxlY3RvcigpOiBDb21waWxlUmVmbGVjdG9yIHsgcmV0dXJuIHRoaXMuX3JlZmxlY3RvcjsgfVxuXG4gIGNsZWFyQ2FjaGVGb3IodHlwZTogVHlwZSkge1xuICAgIGNvbnN0IGRpck1ldGEgPSB0aGlzLl9kaXJlY3RpdmVDYWNoZS5nZXQodHlwZSk7XG4gICAgdGhpcy5fZGlyZWN0aXZlQ2FjaGUuZGVsZXRlKHR5cGUpO1xuICAgIHRoaXMuX25vbk5vcm1hbGl6ZWREaXJlY3RpdmVDYWNoZS5kZWxldGUodHlwZSk7XG4gICAgdGhpcy5fc3VtbWFyeUNhY2hlLmRlbGV0ZSh0eXBlKTtcbiAgICB0aGlzLl9waXBlQ2FjaGUuZGVsZXRlKHR5cGUpO1xuICAgIHRoaXMuX25nTW9kdWxlT2ZUeXBlcy5kZWxldGUodHlwZSk7XG4gICAgLy8gQ2xlYXIgYWxsIG9mIHRoZSBOZ01vZHVsZSBhcyB0aGV5IGNvbnRhaW4gdHJhbnNpdGl2ZSBpbmZvcm1hdGlvbiFcbiAgICB0aGlzLl9uZ01vZHVsZUNhY2hlLmNsZWFyKCk7XG4gICAgaWYgKGRpck1ldGEpIHtcbiAgICAgIHRoaXMuX2RpcmVjdGl2ZU5vcm1hbGl6ZXIuY2xlYXJDYWNoZUZvcihkaXJNZXRhKTtcbiAgICB9XG4gIH1cblxuICBjbGVhckNhY2hlKCk6IHZvaWQge1xuICAgIHRoaXMuX2RpcmVjdGl2ZUNhY2hlLmNsZWFyKCk7XG4gICAgdGhpcy5fbm9uTm9ybWFsaXplZERpcmVjdGl2ZUNhY2hlLmNsZWFyKCk7XG4gICAgdGhpcy5fc3VtbWFyeUNhY2hlLmNsZWFyKCk7XG4gICAgdGhpcy5fcGlwZUNhY2hlLmNsZWFyKCk7XG4gICAgdGhpcy5fbmdNb2R1bGVDYWNoZS5jbGVhcigpO1xuICAgIHRoaXMuX25nTW9kdWxlT2ZUeXBlcy5jbGVhcigpO1xuICAgIHRoaXMuX2RpcmVjdGl2ZU5vcm1hbGl6ZXIuY2xlYXJDYWNoZSgpO1xuICB9XG5cbiAgcHJpdmF0ZSBfY3JlYXRlUHJveHlDbGFzcyhiYXNlVHlwZTogYW55LCBuYW1lOiBzdHJpbmcpOiBjcGwuUHJveHlDbGFzcyB7XG4gICAgbGV0IGRlbGVnYXRlOiBhbnkgPSBudWxsO1xuICAgIGNvbnN0IHByb3h5Q2xhc3M6IGNwbC5Qcm94eUNsYXNzID0gPGFueT5mdW5jdGlvbih0aGlzOiB1bmtub3duKSB7XG4gICAgICBpZiAoIWRlbGVnYXRlKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICAgIGBJbGxlZ2FsIHN0YXRlOiBDbGFzcyAke25hbWV9IGZvciB0eXBlICR7c3RyaW5naWZ5KGJhc2VUeXBlKX0gaXMgbm90IGNvbXBpbGVkIHlldCFgKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBkZWxlZ2F0ZS5hcHBseSh0aGlzLCBhcmd1bWVudHMpO1xuICAgIH07XG4gICAgcHJveHlDbGFzcy5zZXREZWxlZ2F0ZSA9IChkKSA9PiB7XG4gICAgICBkZWxlZ2F0ZSA9IGQ7XG4gICAgICAoPGFueT5wcm94eUNsYXNzKS5wcm90b3R5cGUgPSBkLnByb3RvdHlwZTtcbiAgICB9O1xuICAgIC8vIE1ha2Ugc3RyaW5naWZ5IHdvcmsgY29ycmVjdGx5XG4gICAgKDxhbnk+cHJveHlDbGFzcykub3ZlcnJpZGRlbk5hbWUgPSBuYW1lO1xuICAgIHJldHVybiBwcm94eUNsYXNzO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRHZW5lcmF0ZWRDbGFzcyhkaXJUeXBlOiBhbnksIG5hbWU6IHN0cmluZyk6IFN0YXRpY1N5bWJvbHxjcGwuUHJveHlDbGFzcyB7XG4gICAgaWYgKGRpclR5cGUgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgIHJldHVybiB0aGlzLl9zdGF0aWNTeW1ib2xDYWNoZS5nZXQobmdmYWN0b3J5RmlsZVBhdGgoZGlyVHlwZS5maWxlUGF0aCksIG5hbWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdGhpcy5fY3JlYXRlUHJveHlDbGFzcyhkaXJUeXBlLCBuYW1lKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGdldENvbXBvbmVudFZpZXdDbGFzcyhkaXJUeXBlOiBhbnkpOiBTdGF0aWNTeW1ib2x8Y3BsLlByb3h5Q2xhc3Mge1xuICAgIHJldHVybiB0aGlzLmdldEdlbmVyYXRlZENsYXNzKGRpclR5cGUsIGNwbC52aWV3Q2xhc3NOYW1lKGRpclR5cGUsIDApKTtcbiAgfVxuXG4gIGdldEhvc3RDb21wb25lbnRWaWV3Q2xhc3MoZGlyVHlwZTogYW55KTogU3RhdGljU3ltYm9sfGNwbC5Qcm94eUNsYXNzIHtcbiAgICByZXR1cm4gdGhpcy5nZXRHZW5lcmF0ZWRDbGFzcyhkaXJUeXBlLCBjcGwuaG9zdFZpZXdDbGFzc05hbWUoZGlyVHlwZSkpO1xuICB9XG5cbiAgZ2V0SG9zdENvbXBvbmVudFR5cGUoZGlyVHlwZTogYW55KTogU3RhdGljU3ltYm9sfGNwbC5Qcm94eUNsYXNzIHtcbiAgICBjb25zdCBuYW1lID0gYCR7Y3BsLmlkZW50aWZpZXJOYW1lKHtyZWZlcmVuY2U6IGRpclR5cGV9KX1fSG9zdGA7XG4gICAgaWYgKGRpclR5cGUgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgIHJldHVybiB0aGlzLl9zdGF0aWNTeW1ib2xDYWNoZS5nZXQoZGlyVHlwZS5maWxlUGF0aCwgbmFtZSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMuX2NyZWF0ZVByb3h5Q2xhc3MoZGlyVHlwZSwgbmFtZSk7XG4gIH1cblxuICBwcml2YXRlIGdldFJlbmRlcmVyVHlwZShkaXJUeXBlOiBhbnkpOiBTdGF0aWNTeW1ib2x8b2JqZWN0IHtcbiAgICBpZiAoZGlyVHlwZSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkge1xuICAgICAgcmV0dXJuIHRoaXMuX3N0YXRpY1N5bWJvbENhY2hlLmdldChcbiAgICAgICAgICBuZ2ZhY3RvcnlGaWxlUGF0aChkaXJUeXBlLmZpbGVQYXRoKSwgY3BsLnJlbmRlcmVyVHlwZU5hbWUoZGlyVHlwZSkpO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyByZXR1cm5pbmcgYW4gb2JqZWN0IGFzIHByb3h5LFxuICAgICAgLy8gdGhhdCB3ZSBmaWxsIGxhdGVyIGR1cmluZyBydW50aW1lIGNvbXBpbGF0aW9uLlxuICAgICAgcmV0dXJuIDxhbnk+e307XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBnZXRDb21wb25lbnRGYWN0b3J5KFxuICAgICAgc2VsZWN0b3I6IHN0cmluZywgZGlyVHlwZTogYW55LCBpbnB1dHM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9fG51bGwsXG4gICAgICBvdXRwdXRzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSk6IFN0YXRpY1N5bWJvbHxvYmplY3Qge1xuICAgIGlmIChkaXJUeXBlIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgICByZXR1cm4gdGhpcy5fc3RhdGljU3ltYm9sQ2FjaGUuZ2V0KFxuICAgICAgICAgIG5nZmFjdG9yeUZpbGVQYXRoKGRpclR5cGUuZmlsZVBhdGgpLCBjcGwuY29tcG9uZW50RmFjdG9yeU5hbWUoZGlyVHlwZSkpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBob3N0VmlldyA9IHRoaXMuZ2V0SG9zdENvbXBvbmVudFZpZXdDbGFzcyhkaXJUeXBlKTtcbiAgICAgIC8vIE5vdGU6IG5nQ29udGVudFNlbGVjdG9ycyB3aWxsIGJlIGZpbGxlZCBsYXRlciBvbmNlIHRoZSB0ZW1wbGF0ZSBpc1xuICAgICAgLy8gbG9hZGVkLlxuICAgICAgY29uc3QgY3JlYXRlQ29tcG9uZW50RmFjdG9yeSA9XG4gICAgICAgICAgdGhpcy5fcmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5jcmVhdGVDb21wb25lbnRGYWN0b3J5KTtcbiAgICAgIHJldHVybiBjcmVhdGVDb21wb25lbnRGYWN0b3J5KHNlbGVjdG9yLCBkaXJUeXBlLCA8YW55Pmhvc3RWaWV3LCBpbnB1dHMsIG91dHB1dHMsIFtdKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGluaXRDb21wb25lbnRGYWN0b3J5KGZhY3Rvcnk6IFN0YXRpY1N5bWJvbHxvYmplY3QsIG5nQ29udGVudFNlbGVjdG9yczogc3RyaW5nW10pIHtcbiAgICBpZiAoIShmYWN0b3J5IGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSkge1xuICAgICAgKGZhY3RvcnkgYXMgYW55KS5uZ0NvbnRlbnRTZWxlY3RvcnMucHVzaCguLi5uZ0NvbnRlbnRTZWxlY3RvcnMpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgX2xvYWRTdW1tYXJ5KHR5cGU6IGFueSwga2luZDogY3BsLkNvbXBpbGVTdW1tYXJ5S2luZCk6IGNwbC5Db21waWxlVHlwZVN1bW1hcnl8bnVsbCB7XG4gICAgbGV0IHR5cGVTdW1tYXJ5ID0gdGhpcy5fc3VtbWFyeUNhY2hlLmdldCh0eXBlKTtcbiAgICBpZiAoIXR5cGVTdW1tYXJ5KSB7XG4gICAgICBjb25zdCBzdW1tYXJ5ID0gdGhpcy5fc3VtbWFyeVJlc29sdmVyLnJlc29sdmVTdW1tYXJ5KHR5cGUpO1xuICAgICAgdHlwZVN1bW1hcnkgPSBzdW1tYXJ5ID8gc3VtbWFyeS50eXBlIDogbnVsbDtcbiAgICAgIHRoaXMuX3N1bW1hcnlDYWNoZS5zZXQodHlwZSwgdHlwZVN1bW1hcnkgfHwgbnVsbCk7XG4gICAgfVxuICAgIHJldHVybiB0eXBlU3VtbWFyeSAmJiB0eXBlU3VtbWFyeS5zdW1tYXJ5S2luZCA9PT0ga2luZCA/IHR5cGVTdW1tYXJ5IDogbnVsbDtcbiAgfVxuXG4gIGdldEhvc3RDb21wb25lbnRNZXRhZGF0YShcbiAgICAgIGNvbXBNZXRhOiBjcGwuQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhLFxuICAgICAgaG9zdFZpZXdUeXBlPzogU3RhdGljU3ltYm9sfGNwbC5Qcm94eUNsYXNzKTogY3BsLkNvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSB7XG4gICAgY29uc3QgaG9zdFR5cGUgPSB0aGlzLmdldEhvc3RDb21wb25lbnRUeXBlKGNvbXBNZXRhLnR5cGUucmVmZXJlbmNlKTtcbiAgICBpZiAoIWhvc3RWaWV3VHlwZSkge1xuICAgICAgaG9zdFZpZXdUeXBlID0gdGhpcy5nZXRIb3N0Q29tcG9uZW50Vmlld0NsYXNzKGhvc3RUeXBlKTtcbiAgICB9XG4gICAgLy8gTm90ZTogISBpcyBvayBoZXJlIGFzIHRoaXMgbWV0aG9kIHNob3VsZCBvbmx5IGJlIGNhbGxlZCB3aXRoIG5vcm1hbGl6ZWQgZGlyZWN0aXZlXG4gICAgLy8gbWV0YWRhdGEsIHdoaWNoIGFsd2F5cyBmaWxscyBpbiB0aGUgc2VsZWN0b3IuXG4gICAgY29uc3QgdGVtcGxhdGUgPSBDc3NTZWxlY3Rvci5wYXJzZShjb21wTWV0YS5zZWxlY3RvciAhKVswXS5nZXRNYXRjaGluZ0VsZW1lbnRUZW1wbGF0ZSgpO1xuICAgIGNvbnN0IHRlbXBsYXRlVXJsID0gJyc7XG4gICAgY29uc3QgaHRtbEFzdCA9IHRoaXMuX2h0bWxQYXJzZXIucGFyc2UodGVtcGxhdGUsIHRlbXBsYXRlVXJsKTtcbiAgICByZXR1cm4gY3BsLkNvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YS5jcmVhdGUoe1xuICAgICAgaXNIb3N0OiB0cnVlLFxuICAgICAgdHlwZToge3JlZmVyZW5jZTogaG9zdFR5cGUsIGRpRGVwczogW10sIGxpZmVjeWNsZUhvb2tzOiBbXX0sXG4gICAgICB0ZW1wbGF0ZTogbmV3IGNwbC5Db21waWxlVGVtcGxhdGVNZXRhZGF0YSh7XG4gICAgICAgIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gICAgICAgIHRlbXBsYXRlLFxuICAgICAgICB0ZW1wbGF0ZVVybCxcbiAgICAgICAgaHRtbEFzdCxcbiAgICAgICAgc3R5bGVzOiBbXSxcbiAgICAgICAgc3R5bGVVcmxzOiBbXSxcbiAgICAgICAgbmdDb250ZW50U2VsZWN0b3JzOiBbXSxcbiAgICAgICAgYW5pbWF0aW9uczogW10sXG4gICAgICAgIGlzSW5saW5lOiB0cnVlLFxuICAgICAgICBleHRlcm5hbFN0eWxlc2hlZXRzOiBbXSxcbiAgICAgICAgaW50ZXJwb2xhdGlvbjogbnVsbCxcbiAgICAgICAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gICAgICB9KSxcbiAgICAgIGV4cG9ydEFzOiBudWxsLFxuICAgICAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5EZWZhdWx0LFxuICAgICAgaW5wdXRzOiBbXSxcbiAgICAgIG91dHB1dHM6IFtdLFxuICAgICAgaG9zdDoge30sXG4gICAgICBpc0NvbXBvbmVudDogdHJ1ZSxcbiAgICAgIHNlbGVjdG9yOiAnKicsXG4gICAgICBwcm92aWRlcnM6IFtdLFxuICAgICAgdmlld1Byb3ZpZGVyczogW10sXG4gICAgICBxdWVyaWVzOiBbXSxcbiAgICAgIGd1YXJkczoge30sXG4gICAgICB2aWV3UXVlcmllczogW10sXG4gICAgICBjb21wb25lbnRWaWV3VHlwZTogaG9zdFZpZXdUeXBlLFxuICAgICAgcmVuZGVyZXJUeXBlOlxuICAgICAgICAgIHtpZDogJ19fSG9zdF9fJywgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSwgc3R5bGVzOiBbXSwgZGF0YToge319IGFzIG9iamVjdCxcbiAgICAgIGVudHJ5Q29tcG9uZW50czogW10sXG4gICAgICBjb21wb25lbnRGYWN0b3J5OiBudWxsXG4gICAgfSk7XG4gIH1cblxuICBsb2FkRGlyZWN0aXZlTWV0YWRhdGEobmdNb2R1bGVUeXBlOiBhbnksIGRpcmVjdGl2ZVR5cGU6IGFueSwgaXNTeW5jOiBib29sZWFuKTogU3luY0FzeW5jPG51bGw+IHtcbiAgICBpZiAodGhpcy5fZGlyZWN0aXZlQ2FjaGUuaGFzKGRpcmVjdGl2ZVR5cGUpKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgZGlyZWN0aXZlVHlwZSA9IHJlc29sdmVGb3J3YXJkUmVmKGRpcmVjdGl2ZVR5cGUpO1xuICAgIGNvbnN0IHthbm5vdGF0aW9uLCBtZXRhZGF0YX0gPSB0aGlzLmdldE5vbk5vcm1hbGl6ZWREaXJlY3RpdmVNZXRhZGF0YShkaXJlY3RpdmVUeXBlKSAhO1xuXG4gICAgY29uc3QgY3JlYXRlRGlyZWN0aXZlTWV0YWRhdGEgPSAodGVtcGxhdGVNZXRhZGF0YTogY3BsLkNvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhIHwgbnVsbCkgPT4ge1xuICAgICAgY29uc3Qgbm9ybWFsaXplZERpck1ldGEgPSBuZXcgY3BsLkNvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSh7XG4gICAgICAgIGlzSG9zdDogZmFsc2UsXG4gICAgICAgIHR5cGU6IG1ldGFkYXRhLnR5cGUsXG4gICAgICAgIGlzQ29tcG9uZW50OiBtZXRhZGF0YS5pc0NvbXBvbmVudCxcbiAgICAgICAgc2VsZWN0b3I6IG1ldGFkYXRhLnNlbGVjdG9yLFxuICAgICAgICBleHBvcnRBczogbWV0YWRhdGEuZXhwb3J0QXMsXG4gICAgICAgIGNoYW5nZURldGVjdGlvbjogbWV0YWRhdGEuY2hhbmdlRGV0ZWN0aW9uLFxuICAgICAgICBpbnB1dHM6IG1ldGFkYXRhLmlucHV0cyxcbiAgICAgICAgb3V0cHV0czogbWV0YWRhdGEub3V0cHV0cyxcbiAgICAgICAgaG9zdExpc3RlbmVyczogbWV0YWRhdGEuaG9zdExpc3RlbmVycyxcbiAgICAgICAgaG9zdFByb3BlcnRpZXM6IG1ldGFkYXRhLmhvc3RQcm9wZXJ0aWVzLFxuICAgICAgICBob3N0QXR0cmlidXRlczogbWV0YWRhdGEuaG9zdEF0dHJpYnV0ZXMsXG4gICAgICAgIHByb3ZpZGVyczogbWV0YWRhdGEucHJvdmlkZXJzLFxuICAgICAgICB2aWV3UHJvdmlkZXJzOiBtZXRhZGF0YS52aWV3UHJvdmlkZXJzLFxuICAgICAgICBxdWVyaWVzOiBtZXRhZGF0YS5xdWVyaWVzLFxuICAgICAgICBndWFyZHM6IG1ldGFkYXRhLmd1YXJkcyxcbiAgICAgICAgdmlld1F1ZXJpZXM6IG1ldGFkYXRhLnZpZXdRdWVyaWVzLFxuICAgICAgICBlbnRyeUNvbXBvbmVudHM6IG1ldGFkYXRhLmVudHJ5Q29tcG9uZW50cyxcbiAgICAgICAgY29tcG9uZW50Vmlld1R5cGU6IG1ldGFkYXRhLmNvbXBvbmVudFZpZXdUeXBlLFxuICAgICAgICByZW5kZXJlclR5cGU6IG1ldGFkYXRhLnJlbmRlcmVyVHlwZSxcbiAgICAgICAgY29tcG9uZW50RmFjdG9yeTogbWV0YWRhdGEuY29tcG9uZW50RmFjdG9yeSxcbiAgICAgICAgdGVtcGxhdGU6IHRlbXBsYXRlTWV0YWRhdGFcbiAgICAgIH0pO1xuICAgICAgaWYgKHRlbXBsYXRlTWV0YWRhdGEpIHtcbiAgICAgICAgdGhpcy5pbml0Q29tcG9uZW50RmFjdG9yeShtZXRhZGF0YS5jb21wb25lbnRGYWN0b3J5ICEsIHRlbXBsYXRlTWV0YWRhdGEubmdDb250ZW50U2VsZWN0b3JzKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuX2RpcmVjdGl2ZUNhY2hlLnNldChkaXJlY3RpdmVUeXBlLCBub3JtYWxpemVkRGlyTWV0YSk7XG4gICAgICB0aGlzLl9zdW1tYXJ5Q2FjaGUuc2V0KGRpcmVjdGl2ZVR5cGUsIG5vcm1hbGl6ZWREaXJNZXRhLnRvU3VtbWFyeSgpKTtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH07XG5cbiAgICBpZiAobWV0YWRhdGEuaXNDb21wb25lbnQpIHtcbiAgICAgIGNvbnN0IHRlbXBsYXRlID0gbWV0YWRhdGEudGVtcGxhdGUgITtcbiAgICAgIGNvbnN0IHRlbXBsYXRlTWV0YSA9IHRoaXMuX2RpcmVjdGl2ZU5vcm1hbGl6ZXIubm9ybWFsaXplVGVtcGxhdGUoe1xuICAgICAgICBuZ01vZHVsZVR5cGUsXG4gICAgICAgIGNvbXBvbmVudFR5cGU6IGRpcmVjdGl2ZVR5cGUsXG4gICAgICAgIG1vZHVsZVVybDogdGhpcy5fcmVmbGVjdG9yLmNvbXBvbmVudE1vZHVsZVVybChkaXJlY3RpdmVUeXBlLCBhbm5vdGF0aW9uKSxcbiAgICAgICAgZW5jYXBzdWxhdGlvbjogdGVtcGxhdGUuZW5jYXBzdWxhdGlvbixcbiAgICAgICAgdGVtcGxhdGU6IHRlbXBsYXRlLnRlbXBsYXRlLFxuICAgICAgICB0ZW1wbGF0ZVVybDogdGVtcGxhdGUudGVtcGxhdGVVcmwsXG4gICAgICAgIHN0eWxlczogdGVtcGxhdGUuc3R5bGVzLFxuICAgICAgICBzdHlsZVVybHM6IHRlbXBsYXRlLnN0eWxlVXJscyxcbiAgICAgICAgYW5pbWF0aW9uczogdGVtcGxhdGUuYW5pbWF0aW9ucyxcbiAgICAgICAgaW50ZXJwb2xhdGlvbjogdGVtcGxhdGUuaW50ZXJwb2xhdGlvbixcbiAgICAgICAgcHJlc2VydmVXaGl0ZXNwYWNlczogdGVtcGxhdGUucHJlc2VydmVXaGl0ZXNwYWNlc1xuICAgICAgfSk7XG4gICAgICBpZiAoaXNQcm9taXNlKHRlbXBsYXRlTWV0YSkgJiYgaXNTeW5jKSB7XG4gICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKGNvbXBvbmVudFN0aWxsTG9hZGluZ0Vycm9yKGRpcmVjdGl2ZVR5cGUpLCBkaXJlY3RpdmVUeXBlKTtcbiAgICAgICAgcmV0dXJuIG51bGw7XG4gICAgICB9XG4gICAgICByZXR1cm4gU3luY0FzeW5jLnRoZW4odGVtcGxhdGVNZXRhLCBjcmVhdGVEaXJlY3RpdmVNZXRhZGF0YSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIGRpcmVjdGl2ZVxuICAgICAgY3JlYXRlRGlyZWN0aXZlTWV0YWRhdGEobnVsbCk7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gIH1cblxuICBnZXROb25Ob3JtYWxpemVkRGlyZWN0aXZlTWV0YWRhdGEoZGlyZWN0aXZlVHlwZTogYW55KTpcbiAgICAgIHthbm5vdGF0aW9uOiBEaXJlY3RpdmUsIG1ldGFkYXRhOiBjcGwuQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhfXxudWxsIHtcbiAgICBkaXJlY3RpdmVUeXBlID0gcmVzb2x2ZUZvcndhcmRSZWYoZGlyZWN0aXZlVHlwZSk7XG4gICAgaWYgKCFkaXJlY3RpdmVUeXBlKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgbGV0IGNhY2hlRW50cnkgPSB0aGlzLl9ub25Ob3JtYWxpemVkRGlyZWN0aXZlQ2FjaGUuZ2V0KGRpcmVjdGl2ZVR5cGUpO1xuICAgIGlmIChjYWNoZUVudHJ5KSB7XG4gICAgICByZXR1cm4gY2FjaGVFbnRyeTtcbiAgICB9XG4gICAgY29uc3QgZGlyTWV0YSA9IHRoaXMuX2RpcmVjdGl2ZVJlc29sdmVyLnJlc29sdmUoZGlyZWN0aXZlVHlwZSwgZmFsc2UpO1xuICAgIGlmICghZGlyTWV0YSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGxldCBub25Ob3JtYWxpemVkVGVtcGxhdGVNZXRhZGF0YTogY3BsLkNvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhID0gdW5kZWZpbmVkICE7XG5cbiAgICBpZiAoY3JlYXRlQ29tcG9uZW50LmlzVHlwZU9mKGRpck1ldGEpKSB7XG4gICAgICAvLyBjb21wb25lbnRcbiAgICAgIGNvbnN0IGNvbXBNZXRhID0gZGlyTWV0YSBhcyBDb21wb25lbnQ7XG4gICAgICBhc3NlcnRBcnJheU9mU3RyaW5ncygnc3R5bGVzJywgY29tcE1ldGEuc3R5bGVzKTtcbiAgICAgIGFzc2VydEFycmF5T2ZTdHJpbmdzKCdzdHlsZVVybHMnLCBjb21wTWV0YS5zdHlsZVVybHMpO1xuICAgICAgYXNzZXJ0SW50ZXJwb2xhdGlvblN5bWJvbHMoJ2ludGVycG9sYXRpb24nLCBjb21wTWV0YS5pbnRlcnBvbGF0aW9uKTtcblxuICAgICAgY29uc3QgYW5pbWF0aW9ucyA9IGNvbXBNZXRhLmFuaW1hdGlvbnM7XG5cbiAgICAgIG5vbk5vcm1hbGl6ZWRUZW1wbGF0ZU1ldGFkYXRhID0gbmV3IGNwbC5Db21waWxlVGVtcGxhdGVNZXRhZGF0YSh7XG4gICAgICAgIGVuY2Fwc3VsYXRpb246IG5vVW5kZWZpbmVkKGNvbXBNZXRhLmVuY2Fwc3VsYXRpb24pLFxuICAgICAgICB0ZW1wbGF0ZTogbm9VbmRlZmluZWQoY29tcE1ldGEudGVtcGxhdGUpLFxuICAgICAgICB0ZW1wbGF0ZVVybDogbm9VbmRlZmluZWQoY29tcE1ldGEudGVtcGxhdGVVcmwpLFxuICAgICAgICBodG1sQXN0OiBudWxsLFxuICAgICAgICBzdHlsZXM6IGNvbXBNZXRhLnN0eWxlcyB8fCBbXSxcbiAgICAgICAgc3R5bGVVcmxzOiBjb21wTWV0YS5zdHlsZVVybHMgfHwgW10sXG4gICAgICAgIGFuaW1hdGlvbnM6IGFuaW1hdGlvbnMgfHwgW10sXG4gICAgICAgIGludGVycG9sYXRpb246IG5vVW5kZWZpbmVkKGNvbXBNZXRhLmludGVycG9sYXRpb24pLFxuICAgICAgICBpc0lubGluZTogISFjb21wTWV0YS50ZW1wbGF0ZSxcbiAgICAgICAgZXh0ZXJuYWxTdHlsZXNoZWV0czogW10sXG4gICAgICAgIG5nQ29udGVudFNlbGVjdG9yczogW10sXG4gICAgICAgIHByZXNlcnZlV2hpdGVzcGFjZXM6IG5vVW5kZWZpbmVkKGRpck1ldGEucHJlc2VydmVXaGl0ZXNwYWNlcyksXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBsZXQgY2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3k6IENoYW5nZURldGVjdGlvblN0cmF0ZWd5ID0gbnVsbCAhO1xuICAgIGxldCB2aWV3UHJvdmlkZXJzOiBjcGwuQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSA9IFtdO1xuICAgIGxldCBlbnRyeUNvbXBvbmVudE1ldGFkYXRhOiBjcGwuQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXSA9IFtdO1xuICAgIGxldCBzZWxlY3RvciA9IGRpck1ldGEuc2VsZWN0b3I7XG5cbiAgICBpZiAoY3JlYXRlQ29tcG9uZW50LmlzVHlwZU9mKGRpck1ldGEpKSB7XG4gICAgICAvLyBDb21wb25lbnRcbiAgICAgIGNvbnN0IGNvbXBNZXRhID0gZGlyTWV0YSBhcyBDb21wb25lbnQ7XG4gICAgICBjaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSA9IGNvbXBNZXRhLmNoYW5nZURldGVjdGlvbiAhO1xuICAgICAgaWYgKGNvbXBNZXRhLnZpZXdQcm92aWRlcnMpIHtcbiAgICAgICAgdmlld1Byb3ZpZGVycyA9IHRoaXMuX2dldFByb3ZpZGVyc01ldGFkYXRhKFxuICAgICAgICAgICAgY29tcE1ldGEudmlld1Byb3ZpZGVycywgZW50cnlDb21wb25lbnRNZXRhZGF0YSxcbiAgICAgICAgICAgIGB2aWV3UHJvdmlkZXJzIGZvciBcIiR7c3RyaW5naWZ5VHlwZShkaXJlY3RpdmVUeXBlKX1cImAsIFtdLCBkaXJlY3RpdmVUeXBlKTtcbiAgICAgIH1cbiAgICAgIGlmIChjb21wTWV0YS5lbnRyeUNvbXBvbmVudHMpIHtcbiAgICAgICAgZW50cnlDb21wb25lbnRNZXRhZGF0YSA9IGZsYXR0ZW5BbmREZWR1cGVBcnJheShjb21wTWV0YS5lbnRyeUNvbXBvbmVudHMpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLm1hcCgodHlwZSkgPT4gdGhpcy5fZ2V0RW50cnlDb21wb25lbnRNZXRhZGF0YSh0eXBlKSAhKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC5jb25jYXQoZW50cnlDb21wb25lbnRNZXRhZGF0YSk7XG4gICAgICB9XG4gICAgICBpZiAoIXNlbGVjdG9yKSB7XG4gICAgICAgIHNlbGVjdG9yID0gdGhpcy5fc2NoZW1hUmVnaXN0cnkuZ2V0RGVmYXVsdENvbXBvbmVudEVsZW1lbnROYW1lKCk7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIERpcmVjdGl2ZVxuICAgICAgaWYgKCFzZWxlY3Rvcikge1xuICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICAgIGBEaXJlY3RpdmUgJHtzdHJpbmdpZnlUeXBlKGRpcmVjdGl2ZVR5cGUpfSBoYXMgbm8gc2VsZWN0b3IsIHBsZWFzZSBhZGQgaXQhYCksXG4gICAgICAgICAgICBkaXJlY3RpdmVUeXBlKTtcbiAgICAgICAgc2VsZWN0b3IgPSAnZXJyb3InO1xuICAgICAgfVxuICAgIH1cblxuICAgIGxldCBwcm92aWRlcnM6IGNwbC5Db21waWxlUHJvdmlkZXJNZXRhZGF0YVtdID0gW107XG4gICAgaWYgKGRpck1ldGEucHJvdmlkZXJzICE9IG51bGwpIHtcbiAgICAgIHByb3ZpZGVycyA9IHRoaXMuX2dldFByb3ZpZGVyc01ldGFkYXRhKFxuICAgICAgICAgIGRpck1ldGEucHJvdmlkZXJzLCBlbnRyeUNvbXBvbmVudE1ldGFkYXRhLFxuICAgICAgICAgIGBwcm92aWRlcnMgZm9yIFwiJHtzdHJpbmdpZnlUeXBlKGRpcmVjdGl2ZVR5cGUpfVwiYCwgW10sIGRpcmVjdGl2ZVR5cGUpO1xuICAgIH1cbiAgICBsZXQgcXVlcmllczogY3BsLkNvbXBpbGVRdWVyeU1ldGFkYXRhW10gPSBbXTtcbiAgICBsZXQgdmlld1F1ZXJpZXM6IGNwbC5Db21waWxlUXVlcnlNZXRhZGF0YVtdID0gW107XG4gICAgaWYgKGRpck1ldGEucXVlcmllcyAhPSBudWxsKSB7XG4gICAgICBxdWVyaWVzID0gdGhpcy5fZ2V0UXVlcmllc01ldGFkYXRhKGRpck1ldGEucXVlcmllcywgZmFsc2UsIGRpcmVjdGl2ZVR5cGUpO1xuICAgICAgdmlld1F1ZXJpZXMgPSB0aGlzLl9nZXRRdWVyaWVzTWV0YWRhdGEoZGlyTWV0YS5xdWVyaWVzLCB0cnVlLCBkaXJlY3RpdmVUeXBlKTtcbiAgICB9XG5cbiAgICBjb25zdCBtZXRhZGF0YSA9IGNwbC5Db21waWxlRGlyZWN0aXZlTWV0YWRhdGEuY3JlYXRlKHtcbiAgICAgIGlzSG9zdDogZmFsc2UsXG4gICAgICBzZWxlY3Rvcjogc2VsZWN0b3IsXG4gICAgICBleHBvcnRBczogbm9VbmRlZmluZWQoZGlyTWV0YS5leHBvcnRBcyksXG4gICAgICBpc0NvbXBvbmVudDogISFub25Ob3JtYWxpemVkVGVtcGxhdGVNZXRhZGF0YSxcbiAgICAgIHR5cGU6IHRoaXMuX2dldFR5cGVNZXRhZGF0YShkaXJlY3RpdmVUeXBlKSxcbiAgICAgIHRlbXBsYXRlOiBub25Ob3JtYWxpemVkVGVtcGxhdGVNZXRhZGF0YSxcbiAgICAgIGNoYW5nZURldGVjdGlvbjogY2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gICAgICBpbnB1dHM6IGRpck1ldGEuaW5wdXRzIHx8IFtdLFxuICAgICAgb3V0cHV0czogZGlyTWV0YS5vdXRwdXRzIHx8IFtdLFxuICAgICAgaG9zdDogZGlyTWV0YS5ob3N0IHx8IHt9LFxuICAgICAgcHJvdmlkZXJzOiBwcm92aWRlcnMgfHwgW10sXG4gICAgICB2aWV3UHJvdmlkZXJzOiB2aWV3UHJvdmlkZXJzIHx8IFtdLFxuICAgICAgcXVlcmllczogcXVlcmllcyB8fCBbXSxcbiAgICAgIGd1YXJkczogZGlyTWV0YS5ndWFyZHMgfHwge30sXG4gICAgICB2aWV3UXVlcmllczogdmlld1F1ZXJpZXMgfHwgW10sXG4gICAgICBlbnRyeUNvbXBvbmVudHM6IGVudHJ5Q29tcG9uZW50TWV0YWRhdGEsXG4gICAgICBjb21wb25lbnRWaWV3VHlwZTogbm9uTm9ybWFsaXplZFRlbXBsYXRlTWV0YWRhdGEgPyB0aGlzLmdldENvbXBvbmVudFZpZXdDbGFzcyhkaXJlY3RpdmVUeXBlKSA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBudWxsLFxuICAgICAgcmVuZGVyZXJUeXBlOiBub25Ob3JtYWxpemVkVGVtcGxhdGVNZXRhZGF0YSA/IHRoaXMuZ2V0UmVuZGVyZXJUeXBlKGRpcmVjdGl2ZVR5cGUpIDogbnVsbCxcbiAgICAgIGNvbXBvbmVudEZhY3Rvcnk6IG51bGxcbiAgICB9KTtcbiAgICBpZiAobm9uTm9ybWFsaXplZFRlbXBsYXRlTWV0YWRhdGEpIHtcbiAgICAgIG1ldGFkYXRhLmNvbXBvbmVudEZhY3RvcnkgPVxuICAgICAgICAgIHRoaXMuZ2V0Q29tcG9uZW50RmFjdG9yeShzZWxlY3RvciwgZGlyZWN0aXZlVHlwZSwgbWV0YWRhdGEuaW5wdXRzLCBtZXRhZGF0YS5vdXRwdXRzKTtcbiAgICB9XG4gICAgY2FjaGVFbnRyeSA9IHttZXRhZGF0YSwgYW5ub3RhdGlvbjogZGlyTWV0YX07XG4gICAgdGhpcy5fbm9uTm9ybWFsaXplZERpcmVjdGl2ZUNhY2hlLnNldChkaXJlY3RpdmVUeXBlLCBjYWNoZUVudHJ5KTtcbiAgICByZXR1cm4gY2FjaGVFbnRyeTtcbiAgfVxuXG4gIC8qKlxuICAgKiBHZXRzIHRoZSBtZXRhZGF0YSBmb3IgdGhlIGdpdmVuIGRpcmVjdGl2ZS5cbiAgICogVGhpcyBhc3N1bWVzIGBsb2FkTmdNb2R1bGVEaXJlY3RpdmVBbmRQaXBlTWV0YWRhdGFgIGhhcyBiZWVuIGNhbGxlZCBmaXJzdC5cbiAgICovXG4gIGdldERpcmVjdGl2ZU1ldGFkYXRhKGRpcmVjdGl2ZVR5cGU6IGFueSk6IGNwbC5Db21waWxlRGlyZWN0aXZlTWV0YWRhdGEge1xuICAgIGNvbnN0IGRpck1ldGEgPSB0aGlzLl9kaXJlY3RpdmVDYWNoZS5nZXQoZGlyZWN0aXZlVHlwZSkgITtcbiAgICBpZiAoIWRpck1ldGEpIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICBgSWxsZWdhbCBzdGF0ZTogZ2V0RGlyZWN0aXZlTWV0YWRhdGEgY2FuIG9ubHkgYmUgY2FsbGVkIGFmdGVyIGxvYWROZ01vZHVsZURpcmVjdGl2ZUFuZFBpcGVNZXRhZGF0YSBmb3IgYSBtb2R1bGUgdGhhdCBkZWNsYXJlcyBpdC4gRGlyZWN0aXZlICR7c3RyaW5naWZ5VHlwZShkaXJlY3RpdmVUeXBlKX0uYCksXG4gICAgICAgICAgZGlyZWN0aXZlVHlwZSk7XG4gICAgfVxuICAgIHJldHVybiBkaXJNZXRhO1xuICB9XG5cbiAgZ2V0RGlyZWN0aXZlU3VtbWFyeShkaXJUeXBlOiBhbnkpOiBjcGwuQ29tcGlsZURpcmVjdGl2ZVN1bW1hcnkge1xuICAgIGNvbnN0IGRpclN1bW1hcnkgPVxuICAgICAgICA8Y3BsLkNvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5PnRoaXMuX2xvYWRTdW1tYXJ5KGRpclR5cGUsIGNwbC5Db21waWxlU3VtbWFyeUtpbmQuRGlyZWN0aXZlKTtcbiAgICBpZiAoIWRpclN1bW1hcnkpIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICBgSWxsZWdhbCBzdGF0ZTogQ291bGQgbm90IGxvYWQgdGhlIHN1bW1hcnkgZm9yIGRpcmVjdGl2ZSAke3N0cmluZ2lmeVR5cGUoZGlyVHlwZSl9LmApLFxuICAgICAgICAgIGRpclR5cGUpO1xuICAgIH1cbiAgICByZXR1cm4gZGlyU3VtbWFyeTtcbiAgfVxuXG4gIGlzRGlyZWN0aXZlKHR5cGU6IGFueSkge1xuICAgIHJldHVybiAhIXRoaXMuX2xvYWRTdW1tYXJ5KHR5cGUsIGNwbC5Db21waWxlU3VtbWFyeUtpbmQuRGlyZWN0aXZlKSB8fFxuICAgICAgICB0aGlzLl9kaXJlY3RpdmVSZXNvbHZlci5pc0RpcmVjdGl2ZSh0eXBlKTtcbiAgfVxuXG4gIGlzUGlwZSh0eXBlOiBhbnkpIHtcbiAgICByZXR1cm4gISF0aGlzLl9sb2FkU3VtbWFyeSh0eXBlLCBjcGwuQ29tcGlsZVN1bW1hcnlLaW5kLlBpcGUpIHx8XG4gICAgICAgIHRoaXMuX3BpcGVSZXNvbHZlci5pc1BpcGUodHlwZSk7XG4gIH1cblxuICBpc05nTW9kdWxlKHR5cGU6IGFueSkge1xuICAgIHJldHVybiAhIXRoaXMuX2xvYWRTdW1tYXJ5KHR5cGUsIGNwbC5Db21waWxlU3VtbWFyeUtpbmQuTmdNb2R1bGUpIHx8XG4gICAgICAgIHRoaXMuX25nTW9kdWxlUmVzb2x2ZXIuaXNOZ01vZHVsZSh0eXBlKTtcbiAgfVxuXG4gIGdldE5nTW9kdWxlU3VtbWFyeShtb2R1bGVUeXBlOiBhbnksIGFscmVhZHlDb2xsZWN0aW5nOiBTZXQ8YW55PnxudWxsID0gbnVsbCk6XG4gICAgICBjcGwuQ29tcGlsZU5nTW9kdWxlU3VtbWFyeXxudWxsIHtcbiAgICBsZXQgbW9kdWxlU3VtbWFyeTogY3BsLkNvbXBpbGVOZ01vZHVsZVN1bW1hcnl8bnVsbCA9XG4gICAgICAgIDxjcGwuQ29tcGlsZU5nTW9kdWxlU3VtbWFyeT50aGlzLl9sb2FkU3VtbWFyeShtb2R1bGVUeXBlLCBjcGwuQ29tcGlsZVN1bW1hcnlLaW5kLk5nTW9kdWxlKTtcbiAgICBpZiAoIW1vZHVsZVN1bW1hcnkpIHtcbiAgICAgIGNvbnN0IG1vZHVsZU1ldGEgPSB0aGlzLmdldE5nTW9kdWxlTWV0YWRhdGEobW9kdWxlVHlwZSwgZmFsc2UsIGFscmVhZHlDb2xsZWN0aW5nKTtcbiAgICAgIG1vZHVsZVN1bW1hcnkgPSBtb2R1bGVNZXRhID8gbW9kdWxlTWV0YS50b1N1bW1hcnkoKSA6IG51bGw7XG4gICAgICBpZiAobW9kdWxlU3VtbWFyeSkge1xuICAgICAgICB0aGlzLl9zdW1tYXJ5Q2FjaGUuc2V0KG1vZHVsZVR5cGUsIG1vZHVsZVN1bW1hcnkpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbW9kdWxlU3VtbWFyeTtcbiAgfVxuXG4gIC8qKlxuICAgKiBMb2FkcyB0aGUgZGVjbGFyZWQgZGlyZWN0aXZlcyBhbmQgcGlwZXMgb2YgYW4gTmdNb2R1bGUuXG4gICAqL1xuICBsb2FkTmdNb2R1bGVEaXJlY3RpdmVBbmRQaXBlTWV0YWRhdGEobW9kdWxlVHlwZTogYW55LCBpc1N5bmM6IGJvb2xlYW4sIHRocm93SWZOb3RGb3VuZCA9IHRydWUpOlxuICAgICAgUHJvbWlzZTxhbnk+IHtcbiAgICBjb25zdCBuZ01vZHVsZSA9IHRoaXMuZ2V0TmdNb2R1bGVNZXRhZGF0YShtb2R1bGVUeXBlLCB0aHJvd0lmTm90Rm91bmQpO1xuICAgIGNvbnN0IGxvYWRpbmc6IFByb21pc2U8YW55PltdID0gW107XG4gICAgaWYgKG5nTW9kdWxlKSB7XG4gICAgICBuZ01vZHVsZS5kZWNsYXJlZERpcmVjdGl2ZXMuZm9yRWFjaCgoaWQpID0+IHtcbiAgICAgICAgY29uc3QgcHJvbWlzZSA9IHRoaXMubG9hZERpcmVjdGl2ZU1ldGFkYXRhKG1vZHVsZVR5cGUsIGlkLnJlZmVyZW5jZSwgaXNTeW5jKTtcbiAgICAgICAgaWYgKHByb21pc2UpIHtcbiAgICAgICAgICBsb2FkaW5nLnB1c2gocHJvbWlzZSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgICAgbmdNb2R1bGUuZGVjbGFyZWRQaXBlcy5mb3JFYWNoKChpZCkgPT4gdGhpcy5fbG9hZFBpcGVNZXRhZGF0YShpZC5yZWZlcmVuY2UpKTtcbiAgICB9XG4gICAgcmV0dXJuIFByb21pc2UuYWxsKGxvYWRpbmcpO1xuICB9XG5cbiAgZ2V0U2hhbGxvd01vZHVsZU1ldGFkYXRhKG1vZHVsZVR5cGU6IGFueSk6IGNwbC5Db21waWxlU2hhbGxvd01vZHVsZU1ldGFkYXRhfG51bGwge1xuICAgIGxldCBjb21waWxlTWV0YSA9IHRoaXMuX3NoYWxsb3dNb2R1bGVDYWNoZS5nZXQobW9kdWxlVHlwZSk7XG4gICAgaWYgKGNvbXBpbGVNZXRhKSB7XG4gICAgICByZXR1cm4gY29tcGlsZU1ldGE7XG4gICAgfVxuXG4gICAgY29uc3QgbmdNb2R1bGVNZXRhID1cbiAgICAgICAgZmluZExhc3QodGhpcy5fcmVmbGVjdG9yLnNoYWxsb3dBbm5vdGF0aW9ucyhtb2R1bGVUeXBlKSwgY3JlYXRlTmdNb2R1bGUuaXNUeXBlT2YpO1xuXG4gICAgY29tcGlsZU1ldGEgPSB7XG4gICAgICB0eXBlOiB0aGlzLl9nZXRUeXBlTWV0YWRhdGEobW9kdWxlVHlwZSksXG4gICAgICByYXdFeHBvcnRzOiBuZ01vZHVsZU1ldGEuZXhwb3J0cyxcbiAgICAgIHJhd0ltcG9ydHM6IG5nTW9kdWxlTWV0YS5pbXBvcnRzLFxuICAgICAgcmF3UHJvdmlkZXJzOiBuZ01vZHVsZU1ldGEucHJvdmlkZXJzLFxuICAgIH07XG5cbiAgICB0aGlzLl9zaGFsbG93TW9kdWxlQ2FjaGUuc2V0KG1vZHVsZVR5cGUsIGNvbXBpbGVNZXRhKTtcbiAgICByZXR1cm4gY29tcGlsZU1ldGE7XG4gIH1cblxuICBnZXROZ01vZHVsZU1ldGFkYXRhKFxuICAgICAgbW9kdWxlVHlwZTogYW55LCB0aHJvd0lmTm90Rm91bmQgPSB0cnVlLFxuICAgICAgYWxyZWFkeUNvbGxlY3Rpbmc6IFNldDxhbnk+fG51bGwgPSBudWxsKTogY3BsLkNvbXBpbGVOZ01vZHVsZU1ldGFkYXRhfG51bGwge1xuICAgIG1vZHVsZVR5cGUgPSByZXNvbHZlRm9yd2FyZFJlZihtb2R1bGVUeXBlKTtcbiAgICBsZXQgY29tcGlsZU1ldGEgPSB0aGlzLl9uZ01vZHVsZUNhY2hlLmdldChtb2R1bGVUeXBlKTtcbiAgICBpZiAoY29tcGlsZU1ldGEpIHtcbiAgICAgIHJldHVybiBjb21waWxlTWV0YTtcbiAgICB9XG4gICAgY29uc3QgbWV0YSA9IHRoaXMuX25nTW9kdWxlUmVzb2x2ZXIucmVzb2x2ZShtb2R1bGVUeXBlLCB0aHJvd0lmTm90Rm91bmQpO1xuICAgIGlmICghbWV0YSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGNvbnN0IGRlY2xhcmVkRGlyZWN0aXZlczogY3BsLkNvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXSA9IFtdO1xuICAgIGNvbnN0IGV4cG9ydGVkTm9uTW9kdWxlSWRlbnRpZmllcnM6IGNwbC5Db21waWxlSWRlbnRpZmllck1ldGFkYXRhW10gPSBbXTtcbiAgICBjb25zdCBkZWNsYXJlZFBpcGVzOiBjcGwuQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdID0gW107XG4gICAgY29uc3QgaW1wb3J0ZWRNb2R1bGVzOiBjcGwuQ29tcGlsZU5nTW9kdWxlU3VtbWFyeVtdID0gW107XG4gICAgY29uc3QgZXhwb3J0ZWRNb2R1bGVzOiBjcGwuQ29tcGlsZU5nTW9kdWxlU3VtbWFyeVtdID0gW107XG4gICAgY29uc3QgcHJvdmlkZXJzOiBjcGwuQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSA9IFtdO1xuICAgIGNvbnN0IGVudHJ5Q29tcG9uZW50czogY3BsLkNvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW10gPSBbXTtcbiAgICBjb25zdCBib290c3RyYXBDb21wb25lbnRzOiBjcGwuQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdID0gW107XG4gICAgY29uc3Qgc2NoZW1hczogU2NoZW1hTWV0YWRhdGFbXSA9IFtdO1xuXG4gICAgaWYgKG1ldGEuaW1wb3J0cykge1xuICAgICAgZmxhdHRlbkFuZERlZHVwZUFycmF5KG1ldGEuaW1wb3J0cykuZm9yRWFjaCgoaW1wb3J0ZWRUeXBlKSA9PiB7XG4gICAgICAgIGxldCBpbXBvcnRlZE1vZHVsZVR5cGU6IFR5cGUgPSB1bmRlZmluZWQgITtcbiAgICAgICAgaWYgKGlzVmFsaWRUeXBlKGltcG9ydGVkVHlwZSkpIHtcbiAgICAgICAgICBpbXBvcnRlZE1vZHVsZVR5cGUgPSBpbXBvcnRlZFR5cGU7XG4gICAgICAgIH0gZWxzZSBpZiAoaW1wb3J0ZWRUeXBlICYmIGltcG9ydGVkVHlwZS5uZ01vZHVsZSkge1xuICAgICAgICAgIGNvbnN0IG1vZHVsZVdpdGhQcm92aWRlcnM6IE1vZHVsZVdpdGhQcm92aWRlcnMgPSBpbXBvcnRlZFR5cGU7XG4gICAgICAgICAgaW1wb3J0ZWRNb2R1bGVUeXBlID0gbW9kdWxlV2l0aFByb3ZpZGVycy5uZ01vZHVsZTtcbiAgICAgICAgICBpZiAobW9kdWxlV2l0aFByb3ZpZGVycy5wcm92aWRlcnMpIHtcbiAgICAgICAgICAgIHByb3ZpZGVycy5wdXNoKC4uLnRoaXMuX2dldFByb3ZpZGVyc01ldGFkYXRhKFxuICAgICAgICAgICAgICAgIG1vZHVsZVdpdGhQcm92aWRlcnMucHJvdmlkZXJzLCBlbnRyeUNvbXBvbmVudHMsXG4gICAgICAgICAgICAgICAgYHByb3ZpZGVyIGZvciB0aGUgTmdNb2R1bGUgJyR7c3RyaW5naWZ5VHlwZShpbXBvcnRlZE1vZHVsZVR5cGUpfSdgLCBbXSxcbiAgICAgICAgICAgICAgICBpbXBvcnRlZFR5cGUpKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cblxuICAgICAgICBpZiAoaW1wb3J0ZWRNb2R1bGVUeXBlKSB7XG4gICAgICAgICAgaWYgKHRoaXMuX2NoZWNrU2VsZkltcG9ydChtb2R1bGVUeXBlLCBpbXBvcnRlZE1vZHVsZVR5cGUpKSByZXR1cm47XG4gICAgICAgICAgaWYgKCFhbHJlYWR5Q29sbGVjdGluZykgYWxyZWFkeUNvbGxlY3RpbmcgPSBuZXcgU2V0KCk7XG4gICAgICAgICAgaWYgKGFscmVhZHlDb2xsZWN0aW5nLmhhcyhpbXBvcnRlZE1vZHVsZVR5cGUpKSB7XG4gICAgICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICAgICAgYCR7dGhpcy5fZ2V0VHlwZURlc2NyaXB0b3IoaW1wb3J0ZWRNb2R1bGVUeXBlKX0gJyR7c3RyaW5naWZ5VHlwZShpbXBvcnRlZFR5cGUpfScgaXMgaW1wb3J0ZWQgcmVjdXJzaXZlbHkgYnkgdGhlIG1vZHVsZSAnJHtzdHJpbmdpZnlUeXBlKG1vZHVsZVR5cGUpfScuYCksXG4gICAgICAgICAgICAgICAgbW9kdWxlVHlwZSk7XG4gICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgfVxuICAgICAgICAgIGFscmVhZHlDb2xsZWN0aW5nLmFkZChpbXBvcnRlZE1vZHVsZVR5cGUpO1xuICAgICAgICAgIGNvbnN0IGltcG9ydGVkTW9kdWxlU3VtbWFyeSA9XG4gICAgICAgICAgICAgIHRoaXMuZ2V0TmdNb2R1bGVTdW1tYXJ5KGltcG9ydGVkTW9kdWxlVHlwZSwgYWxyZWFkeUNvbGxlY3RpbmcpO1xuICAgICAgICAgIGFscmVhZHlDb2xsZWN0aW5nLmRlbGV0ZShpbXBvcnRlZE1vZHVsZVR5cGUpO1xuICAgICAgICAgIGlmICghaW1wb3J0ZWRNb2R1bGVTdW1tYXJ5KSB7XG4gICAgICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICAgICAgYFVuZXhwZWN0ZWQgJHt0aGlzLl9nZXRUeXBlRGVzY3JpcHRvcihpbXBvcnRlZFR5cGUpfSAnJHtzdHJpbmdpZnlUeXBlKGltcG9ydGVkVHlwZSl9JyBpbXBvcnRlZCBieSB0aGUgbW9kdWxlICcke3N0cmluZ2lmeVR5cGUobW9kdWxlVHlwZSl9Jy4gUGxlYXNlIGFkZCBhIEBOZ01vZHVsZSBhbm5vdGF0aW9uLmApLFxuICAgICAgICAgICAgICAgIG1vZHVsZVR5cGUpO1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpbXBvcnRlZE1vZHVsZXMucHVzaChpbXBvcnRlZE1vZHVsZVN1bW1hcnkpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICAgIGBVbmV4cGVjdGVkIHZhbHVlICcke3N0cmluZ2lmeVR5cGUoaW1wb3J0ZWRUeXBlKX0nIGltcG9ydGVkIGJ5IHRoZSBtb2R1bGUgJyR7c3RyaW5naWZ5VHlwZShtb2R1bGVUeXBlKX0nYCksXG4gICAgICAgICAgICAgIG1vZHVsZVR5cGUpO1xuICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuXG4gICAgaWYgKG1ldGEuZXhwb3J0cykge1xuICAgICAgZmxhdHRlbkFuZERlZHVwZUFycmF5KG1ldGEuZXhwb3J0cykuZm9yRWFjaCgoZXhwb3J0ZWRUeXBlKSA9PiB7XG4gICAgICAgIGlmICghaXNWYWxpZFR5cGUoZXhwb3J0ZWRUeXBlKSkge1xuICAgICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICAgIGBVbmV4cGVjdGVkIHZhbHVlICcke3N0cmluZ2lmeVR5cGUoZXhwb3J0ZWRUeXBlKX0nIGV4cG9ydGVkIGJ5IHRoZSBtb2R1bGUgJyR7c3RyaW5naWZ5VHlwZShtb2R1bGVUeXBlKX0nYCksXG4gICAgICAgICAgICAgIG1vZHVsZVR5cGUpO1xuICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoIWFscmVhZHlDb2xsZWN0aW5nKSBhbHJlYWR5Q29sbGVjdGluZyA9IG5ldyBTZXQoKTtcbiAgICAgICAgaWYgKGFscmVhZHlDb2xsZWN0aW5nLmhhcyhleHBvcnRlZFR5cGUpKSB7XG4gICAgICAgICAgdGhpcy5fcmVwb3J0RXJyb3IoXG4gICAgICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICAgICAgYCR7dGhpcy5fZ2V0VHlwZURlc2NyaXB0b3IoZXhwb3J0ZWRUeXBlKX0gJyR7c3RyaW5naWZ5KGV4cG9ydGVkVHlwZSl9JyBpcyBleHBvcnRlZCByZWN1cnNpdmVseSBieSB0aGUgbW9kdWxlICcke3N0cmluZ2lmeVR5cGUobW9kdWxlVHlwZSl9J2ApLFxuICAgICAgICAgICAgICBtb2R1bGVUeXBlKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgYWxyZWFkeUNvbGxlY3RpbmcuYWRkKGV4cG9ydGVkVHlwZSk7XG4gICAgICAgIGNvbnN0IGV4cG9ydGVkTW9kdWxlU3VtbWFyeSA9IHRoaXMuZ2V0TmdNb2R1bGVTdW1tYXJ5KGV4cG9ydGVkVHlwZSwgYWxyZWFkeUNvbGxlY3RpbmcpO1xuICAgICAgICBhbHJlYWR5Q29sbGVjdGluZy5kZWxldGUoZXhwb3J0ZWRUeXBlKTtcbiAgICAgICAgaWYgKGV4cG9ydGVkTW9kdWxlU3VtbWFyeSkge1xuICAgICAgICAgIGV4cG9ydGVkTW9kdWxlcy5wdXNoKGV4cG9ydGVkTW9kdWxlU3VtbWFyeSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgZXhwb3J0ZWROb25Nb2R1bGVJZGVudGlmaWVycy5wdXNoKHRoaXMuX2dldElkZW50aWZpZXJNZXRhZGF0YShleHBvcnRlZFR5cGUpKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuXG4gICAgLy8gTm90ZTogVGhpcyB3aWxsIGJlIG1vZGlmaWVkIGxhdGVyLCBzbyB3ZSByZWx5IG9uXG4gICAgLy8gZ2V0dGluZyBhIG5ldyBpbnN0YW5jZSBldmVyeSB0aW1lIVxuICAgIGNvbnN0IHRyYW5zaXRpdmVNb2R1bGUgPSB0aGlzLl9nZXRUcmFuc2l0aXZlTmdNb2R1bGVNZXRhZGF0YShpbXBvcnRlZE1vZHVsZXMsIGV4cG9ydGVkTW9kdWxlcyk7XG4gICAgaWYgKG1ldGEuZGVjbGFyYXRpb25zKSB7XG4gICAgICBmbGF0dGVuQW5kRGVkdXBlQXJyYXkobWV0YS5kZWNsYXJhdGlvbnMpLmZvckVhY2goKGRlY2xhcmVkVHlwZSkgPT4ge1xuICAgICAgICBpZiAoIWlzVmFsaWRUeXBlKGRlY2xhcmVkVHlwZSkpIHtcbiAgICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICAgICAgc3ludGF4RXJyb3IoXG4gICAgICAgICAgICAgICAgICBgVW5leHBlY3RlZCB2YWx1ZSAnJHtzdHJpbmdpZnlUeXBlKGRlY2xhcmVkVHlwZSl9JyBkZWNsYXJlZCBieSB0aGUgbW9kdWxlICcke3N0cmluZ2lmeVR5cGUobW9kdWxlVHlwZSl9J2ApLFxuICAgICAgICAgICAgICBtb2R1bGVUeXBlKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgY29uc3QgZGVjbGFyZWRJZGVudGlmaWVyID0gdGhpcy5fZ2V0SWRlbnRpZmllck1ldGFkYXRhKGRlY2xhcmVkVHlwZSk7XG4gICAgICAgIGlmICh0aGlzLmlzRGlyZWN0aXZlKGRlY2xhcmVkVHlwZSkpIHtcbiAgICAgICAgICB0cmFuc2l0aXZlTW9kdWxlLmFkZERpcmVjdGl2ZShkZWNsYXJlZElkZW50aWZpZXIpO1xuICAgICAgICAgIGRlY2xhcmVkRGlyZWN0aXZlcy5wdXNoKGRlY2xhcmVkSWRlbnRpZmllcik7XG4gICAgICAgICAgdGhpcy5fYWRkVHlwZVRvTW9kdWxlKGRlY2xhcmVkVHlwZSwgbW9kdWxlVHlwZSk7XG4gICAgICAgIH0gZWxzZSBpZiAodGhpcy5pc1BpcGUoZGVjbGFyZWRUeXBlKSkge1xuICAgICAgICAgIHRyYW5zaXRpdmVNb2R1bGUuYWRkUGlwZShkZWNsYXJlZElkZW50aWZpZXIpO1xuICAgICAgICAgIHRyYW5zaXRpdmVNb2R1bGUucGlwZXMucHVzaChkZWNsYXJlZElkZW50aWZpZXIpO1xuICAgICAgICAgIGRlY2xhcmVkUGlwZXMucHVzaChkZWNsYXJlZElkZW50aWZpZXIpO1xuICAgICAgICAgIHRoaXMuX2FkZFR5cGVUb01vZHVsZShkZWNsYXJlZFR5cGUsIG1vZHVsZVR5cGUpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICAgIGBVbmV4cGVjdGVkICR7dGhpcy5fZ2V0VHlwZURlc2NyaXB0b3IoZGVjbGFyZWRUeXBlKX0gJyR7c3RyaW5naWZ5VHlwZShkZWNsYXJlZFR5cGUpfScgZGVjbGFyZWQgYnkgdGhlIG1vZHVsZSAnJHtzdHJpbmdpZnlUeXBlKG1vZHVsZVR5cGUpfScuIFBsZWFzZSBhZGQgYSBAUGlwZS9ARGlyZWN0aXZlL0BDb21wb25lbnQgYW5ub3RhdGlvbi5gKSxcbiAgICAgICAgICAgICAgbW9kdWxlVHlwZSk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBjb25zdCBleHBvcnRlZERpcmVjdGl2ZXM6IGNwbC5Db21waWxlSWRlbnRpZmllck1ldGFkYXRhW10gPSBbXTtcbiAgICBjb25zdCBleHBvcnRlZFBpcGVzOiBjcGwuQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdID0gW107XG4gICAgZXhwb3J0ZWROb25Nb2R1bGVJZGVudGlmaWVycy5mb3JFYWNoKChleHBvcnRlZElkKSA9PiB7XG4gICAgICBpZiAodHJhbnNpdGl2ZU1vZHVsZS5kaXJlY3RpdmVzU2V0LmhhcyhleHBvcnRlZElkLnJlZmVyZW5jZSkpIHtcbiAgICAgICAgZXhwb3J0ZWREaXJlY3RpdmVzLnB1c2goZXhwb3J0ZWRJZCk7XG4gICAgICAgIHRyYW5zaXRpdmVNb2R1bGUuYWRkRXhwb3J0ZWREaXJlY3RpdmUoZXhwb3J0ZWRJZCk7XG4gICAgICB9IGVsc2UgaWYgKHRyYW5zaXRpdmVNb2R1bGUucGlwZXNTZXQuaGFzKGV4cG9ydGVkSWQucmVmZXJlbmNlKSkge1xuICAgICAgICBleHBvcnRlZFBpcGVzLnB1c2goZXhwb3J0ZWRJZCk7XG4gICAgICAgIHRyYW5zaXRpdmVNb2R1bGUuYWRkRXhwb3J0ZWRQaXBlKGV4cG9ydGVkSWQpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5fcmVwb3J0RXJyb3IoXG4gICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICBgQ2FuJ3QgZXhwb3J0ICR7dGhpcy5fZ2V0VHlwZURlc2NyaXB0b3IoZXhwb3J0ZWRJZC5yZWZlcmVuY2UpfSAke3N0cmluZ2lmeVR5cGUoZXhwb3J0ZWRJZC5yZWZlcmVuY2UpfSBmcm9tICR7c3RyaW5naWZ5VHlwZShtb2R1bGVUeXBlKX0gYXMgaXQgd2FzIG5laXRoZXIgZGVjbGFyZWQgbm9yIGltcG9ydGVkIWApLFxuICAgICAgICAgICAgbW9kdWxlVHlwZSk7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIC8vIFRoZSBwcm92aWRlcnMgb2YgdGhlIG1vZHVsZSBoYXZlIHRvIGdvIGxhc3RcbiAgICAvLyBzbyB0aGF0IHRoZXkgb3ZlcndyaXRlIGFueSBvdGhlciBwcm92aWRlciB3ZSBhbHJlYWR5IGFkZGVkLlxuICAgIGlmIChtZXRhLnByb3ZpZGVycykge1xuICAgICAgcHJvdmlkZXJzLnB1c2goLi4udGhpcy5fZ2V0UHJvdmlkZXJzTWV0YWRhdGEoXG4gICAgICAgICAgbWV0YS5wcm92aWRlcnMsIGVudHJ5Q29tcG9uZW50cyxcbiAgICAgICAgICBgcHJvdmlkZXIgZm9yIHRoZSBOZ01vZHVsZSAnJHtzdHJpbmdpZnlUeXBlKG1vZHVsZVR5cGUpfSdgLCBbXSwgbW9kdWxlVHlwZSkpO1xuICAgIH1cblxuICAgIGlmIChtZXRhLmVudHJ5Q29tcG9uZW50cykge1xuICAgICAgZW50cnlDb21wb25lbnRzLnB1c2goLi4uZmxhdHRlbkFuZERlZHVwZUFycmF5KG1ldGEuZW50cnlDb21wb25lbnRzKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC5tYXAodHlwZSA9PiB0aGlzLl9nZXRFbnRyeUNvbXBvbmVudE1ldGFkYXRhKHR5cGUpICEpKTtcbiAgICB9XG5cbiAgICBpZiAobWV0YS5ib290c3RyYXApIHtcbiAgICAgIGZsYXR0ZW5BbmREZWR1cGVBcnJheShtZXRhLmJvb3RzdHJhcCkuZm9yRWFjaCh0eXBlID0+IHtcbiAgICAgICAgaWYgKCFpc1ZhbGlkVHlwZSh0eXBlKSkge1xuICAgICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICAgIGBVbmV4cGVjdGVkIHZhbHVlICcke3N0cmluZ2lmeVR5cGUodHlwZSl9JyB1c2VkIGluIHRoZSBib290c3RyYXAgcHJvcGVydHkgb2YgbW9kdWxlICcke3N0cmluZ2lmeVR5cGUobW9kdWxlVHlwZSl9J2ApLFxuICAgICAgICAgICAgICBtb2R1bGVUeXBlKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgYm9vdHN0cmFwQ29tcG9uZW50cy5wdXNoKHRoaXMuX2dldElkZW50aWZpZXJNZXRhZGF0YSh0eXBlKSk7XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBlbnRyeUNvbXBvbmVudHMucHVzaChcbiAgICAgICAgLi4uYm9vdHN0cmFwQ29tcG9uZW50cy5tYXAodHlwZSA9PiB0aGlzLl9nZXRFbnRyeUNvbXBvbmVudE1ldGFkYXRhKHR5cGUucmVmZXJlbmNlKSAhKSk7XG5cbiAgICBpZiAobWV0YS5zY2hlbWFzKSB7XG4gICAgICBzY2hlbWFzLnB1c2goLi4uZmxhdHRlbkFuZERlZHVwZUFycmF5KG1ldGEuc2NoZW1hcykpO1xuICAgIH1cblxuICAgIGNvbXBpbGVNZXRhID0gbmV3IGNwbC5Db21waWxlTmdNb2R1bGVNZXRhZGF0YSh7XG4gICAgICB0eXBlOiB0aGlzLl9nZXRUeXBlTWV0YWRhdGEobW9kdWxlVHlwZSksXG4gICAgICBwcm92aWRlcnMsXG4gICAgICBlbnRyeUNvbXBvbmVudHMsXG4gICAgICBib290c3RyYXBDb21wb25lbnRzLFxuICAgICAgc2NoZW1hcyxcbiAgICAgIGRlY2xhcmVkRGlyZWN0aXZlcyxcbiAgICAgIGV4cG9ydGVkRGlyZWN0aXZlcyxcbiAgICAgIGRlY2xhcmVkUGlwZXMsXG4gICAgICBleHBvcnRlZFBpcGVzLFxuICAgICAgaW1wb3J0ZWRNb2R1bGVzLFxuICAgICAgZXhwb3J0ZWRNb2R1bGVzLFxuICAgICAgdHJhbnNpdGl2ZU1vZHVsZSxcbiAgICAgIGlkOiBtZXRhLmlkIHx8IG51bGwsXG4gICAgfSk7XG5cbiAgICBlbnRyeUNvbXBvbmVudHMuZm9yRWFjaCgoaWQpID0+IHRyYW5zaXRpdmVNb2R1bGUuYWRkRW50cnlDb21wb25lbnQoaWQpKTtcbiAgICBwcm92aWRlcnMuZm9yRWFjaCgocHJvdmlkZXIpID0+IHRyYW5zaXRpdmVNb2R1bGUuYWRkUHJvdmlkZXIocHJvdmlkZXIsIGNvbXBpbGVNZXRhICEudHlwZSkpO1xuICAgIHRyYW5zaXRpdmVNb2R1bGUuYWRkTW9kdWxlKGNvbXBpbGVNZXRhLnR5cGUpO1xuICAgIHRoaXMuX25nTW9kdWxlQ2FjaGUuc2V0KG1vZHVsZVR5cGUsIGNvbXBpbGVNZXRhKTtcbiAgICByZXR1cm4gY29tcGlsZU1ldGE7XG4gIH1cblxuICBwcml2YXRlIF9jaGVja1NlbGZJbXBvcnQobW9kdWxlVHlwZTogVHlwZSwgaW1wb3J0ZWRNb2R1bGVUeXBlOiBUeXBlKTogYm9vbGVhbiB7XG4gICAgaWYgKG1vZHVsZVR5cGUgPT09IGltcG9ydGVkTW9kdWxlVHlwZSkge1xuICAgICAgdGhpcy5fcmVwb3J0RXJyb3IoXG4gICAgICAgICAgc3ludGF4RXJyb3IoYCcke3N0cmluZ2lmeVR5cGUobW9kdWxlVHlwZSl9JyBtb2R1bGUgY2FuJ3QgaW1wb3J0IGl0c2VsZmApLCBtb2R1bGVUeXBlKTtcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIH1cbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBwcml2YXRlIF9nZXRUeXBlRGVzY3JpcHRvcih0eXBlOiBUeXBlKTogc3RyaW5nIHtcbiAgICBpZiAoaXNWYWxpZFR5cGUodHlwZSkpIHtcbiAgICAgIGlmICh0aGlzLmlzRGlyZWN0aXZlKHR5cGUpKSB7XG4gICAgICAgIHJldHVybiAnZGlyZWN0aXZlJztcbiAgICAgIH1cblxuICAgICAgaWYgKHRoaXMuaXNQaXBlKHR5cGUpKSB7XG4gICAgICAgIHJldHVybiAncGlwZSc7XG4gICAgICB9XG5cbiAgICAgIGlmICh0aGlzLmlzTmdNb2R1bGUodHlwZSkpIHtcbiAgICAgICAgcmV0dXJuICdtb2R1bGUnO1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmICgodHlwZSBhcyBhbnkpLnByb3ZpZGUpIHtcbiAgICAgIHJldHVybiAncHJvdmlkZXInO1xuICAgIH1cblxuICAgIHJldHVybiAndmFsdWUnO1xuICB9XG5cblxuICBwcml2YXRlIF9hZGRUeXBlVG9Nb2R1bGUodHlwZTogVHlwZSwgbW9kdWxlVHlwZTogVHlwZSkge1xuICAgIGNvbnN0IG9sZE1vZHVsZSA9IHRoaXMuX25nTW9kdWxlT2ZUeXBlcy5nZXQodHlwZSk7XG4gICAgaWYgKG9sZE1vZHVsZSAmJiBvbGRNb2R1bGUgIT09IG1vZHVsZVR5cGUpIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICBgVHlwZSAke3N0cmluZ2lmeVR5cGUodHlwZSl9IGlzIHBhcnQgb2YgdGhlIGRlY2xhcmF0aW9ucyBvZiAyIG1vZHVsZXM6ICR7c3RyaW5naWZ5VHlwZShvbGRNb2R1bGUpfSBhbmQgJHtzdHJpbmdpZnlUeXBlKG1vZHVsZVR5cGUpfSEgYCArXG4gICAgICAgICAgICAgIGBQbGVhc2UgY29uc2lkZXIgbW92aW5nICR7c3RyaW5naWZ5VHlwZSh0eXBlKX0gdG8gYSBoaWdoZXIgbW9kdWxlIHRoYXQgaW1wb3J0cyAke3N0cmluZ2lmeVR5cGUob2xkTW9kdWxlKX0gYW5kICR7c3RyaW5naWZ5VHlwZShtb2R1bGVUeXBlKX0uIGAgK1xuICAgICAgICAgICAgICBgWW91IGNhbiBhbHNvIGNyZWF0ZSBhIG5ldyBOZ01vZHVsZSB0aGF0IGV4cG9ydHMgYW5kIGluY2x1ZGVzICR7c3RyaW5naWZ5VHlwZSh0eXBlKX0gdGhlbiBpbXBvcnQgdGhhdCBOZ01vZHVsZSBpbiAke3N0cmluZ2lmeVR5cGUob2xkTW9kdWxlKX0gYW5kICR7c3RyaW5naWZ5VHlwZShtb2R1bGVUeXBlKX0uYCksXG4gICAgICAgICAgbW9kdWxlVHlwZSk7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuX25nTW9kdWxlT2ZUeXBlcy5zZXQodHlwZSwgbW9kdWxlVHlwZSk7XG4gIH1cblxuICBwcml2YXRlIF9nZXRUcmFuc2l0aXZlTmdNb2R1bGVNZXRhZGF0YShcbiAgICAgIGltcG9ydGVkTW9kdWxlczogY3BsLkNvbXBpbGVOZ01vZHVsZVN1bW1hcnlbXSxcbiAgICAgIGV4cG9ydGVkTW9kdWxlczogY3BsLkNvbXBpbGVOZ01vZHVsZVN1bW1hcnlbXSk6IGNwbC5UcmFuc2l0aXZlQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEge1xuICAgIC8vIGNvbGxlY3QgYHByb3ZpZGVyc2AgLyBgZW50cnlDb21wb25lbnRzYCBmcm9tIGFsbCBpbXBvcnRlZCBhbmQgYWxsIGV4cG9ydGVkIG1vZHVsZXNcbiAgICBjb25zdCByZXN1bHQgPSBuZXcgY3BsLlRyYW5zaXRpdmVDb21waWxlTmdNb2R1bGVNZXRhZGF0YSgpO1xuICAgIGNvbnN0IG1vZHVsZXNCeVRva2VuID0gbmV3IE1hcDxhbnksIFNldDxhbnk+PigpO1xuICAgIGltcG9ydGVkTW9kdWxlcy5jb25jYXQoZXhwb3J0ZWRNb2R1bGVzKS5mb3JFYWNoKChtb2RTdW1tYXJ5KSA9PiB7XG4gICAgICBtb2RTdW1tYXJ5Lm1vZHVsZXMuZm9yRWFjaCgobW9kKSA9PiByZXN1bHQuYWRkTW9kdWxlKG1vZCkpO1xuICAgICAgbW9kU3VtbWFyeS5lbnRyeUNvbXBvbmVudHMuZm9yRWFjaCgoY29tcCkgPT4gcmVzdWx0LmFkZEVudHJ5Q29tcG9uZW50KGNvbXApKTtcbiAgICAgIGNvbnN0IGFkZGVkVG9rZW5zID0gbmV3IFNldDxhbnk+KCk7XG4gICAgICBtb2RTdW1tYXJ5LnByb3ZpZGVycy5mb3JFYWNoKChlbnRyeSkgPT4ge1xuICAgICAgICBjb25zdCB0b2tlblJlZiA9IGNwbC50b2tlblJlZmVyZW5jZShlbnRyeS5wcm92aWRlci50b2tlbik7XG4gICAgICAgIGxldCBwcmV2TW9kdWxlcyA9IG1vZHVsZXNCeVRva2VuLmdldCh0b2tlblJlZik7XG4gICAgICAgIGlmICghcHJldk1vZHVsZXMpIHtcbiAgICAgICAgICBwcmV2TW9kdWxlcyA9IG5ldyBTZXQ8YW55PigpO1xuICAgICAgICAgIG1vZHVsZXNCeVRva2VuLnNldCh0b2tlblJlZiwgcHJldk1vZHVsZXMpO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IG1vZHVsZVJlZiA9IGVudHJ5Lm1vZHVsZS5yZWZlcmVuY2U7XG4gICAgICAgIC8vIE5vdGU6IHRoZSBwcm92aWRlcnMgb2Ygb25lIG1vZHVsZSBtYXkgc3RpbGwgY29udGFpbiBtdWx0aXBsZSBwcm92aWRlcnNcbiAgICAgICAgLy8gcGVyIHRva2VuIChlLmcuIGZvciBtdWx0aSBwcm92aWRlcnMpLCBhbmQgd2UgbmVlZCB0byBwcmVzZXJ2ZSB0aGVzZS5cbiAgICAgICAgaWYgKGFkZGVkVG9rZW5zLmhhcyh0b2tlblJlZikgfHwgIXByZXZNb2R1bGVzLmhhcyhtb2R1bGVSZWYpKSB7XG4gICAgICAgICAgcHJldk1vZHVsZXMuYWRkKG1vZHVsZVJlZik7XG4gICAgICAgICAgYWRkZWRUb2tlbnMuYWRkKHRva2VuUmVmKTtcbiAgICAgICAgICByZXN1bHQuYWRkUHJvdmlkZXIoZW50cnkucHJvdmlkZXIsIGVudHJ5Lm1vZHVsZSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH0pO1xuICAgIGV4cG9ydGVkTW9kdWxlcy5mb3JFYWNoKChtb2RTdW1tYXJ5KSA9PiB7XG4gICAgICBtb2RTdW1tYXJ5LmV4cG9ydGVkRGlyZWN0aXZlcy5mb3JFYWNoKChpZCkgPT4gcmVzdWx0LmFkZEV4cG9ydGVkRGlyZWN0aXZlKGlkKSk7XG4gICAgICBtb2RTdW1tYXJ5LmV4cG9ydGVkUGlwZXMuZm9yRWFjaCgoaWQpID0+IHJlc3VsdC5hZGRFeHBvcnRlZFBpcGUoaWQpKTtcbiAgICB9KTtcbiAgICBpbXBvcnRlZE1vZHVsZXMuZm9yRWFjaCgobW9kU3VtbWFyeSkgPT4ge1xuICAgICAgbW9kU3VtbWFyeS5leHBvcnRlZERpcmVjdGl2ZXMuZm9yRWFjaCgoaWQpID0+IHJlc3VsdC5hZGREaXJlY3RpdmUoaWQpKTtcbiAgICAgIG1vZFN1bW1hcnkuZXhwb3J0ZWRQaXBlcy5mb3JFYWNoKChpZCkgPT4gcmVzdWx0LmFkZFBpcGUoaWQpKTtcbiAgICB9KTtcbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0SWRlbnRpZmllck1ldGFkYXRhKHR5cGU6IFR5cGUpOiBjcGwuQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YSB7XG4gICAgdHlwZSA9IHJlc29sdmVGb3J3YXJkUmVmKHR5cGUpO1xuICAgIHJldHVybiB7cmVmZXJlbmNlOiB0eXBlfTtcbiAgfVxuXG4gIGlzSW5qZWN0YWJsZSh0eXBlOiBhbnkpOiBib29sZWFuIHtcbiAgICBjb25zdCBhbm5vdGF0aW9ucyA9IHRoaXMuX3JlZmxlY3Rvci50cnlBbm5vdGF0aW9ucyh0eXBlKTtcbiAgICByZXR1cm4gYW5ub3RhdGlvbnMuc29tZShhbm4gPT4gY3JlYXRlSW5qZWN0YWJsZS5pc1R5cGVPZihhbm4pKTtcbiAgfVxuXG4gIGdldEluamVjdGFibGVTdW1tYXJ5KHR5cGU6IGFueSk6IGNwbC5Db21waWxlVHlwZVN1bW1hcnkge1xuICAgIHJldHVybiB7XG4gICAgICBzdW1tYXJ5S2luZDogY3BsLkNvbXBpbGVTdW1tYXJ5S2luZC5JbmplY3RhYmxlLFxuICAgICAgdHlwZTogdGhpcy5fZ2V0VHlwZU1ldGFkYXRhKHR5cGUsIG51bGwsIGZhbHNlKVxuICAgIH07XG4gIH1cblxuICBnZXRJbmplY3RhYmxlTWV0YWRhdGEoXG4gICAgICB0eXBlOiBhbnksIGRlcGVuZGVuY2llczogYW55W118bnVsbCA9IG51bGwsXG4gICAgICB0aHJvd09uVW5rbm93bkRlcHM6IGJvb2xlYW4gPSB0cnVlKTogY3BsLkNvbXBpbGVJbmplY3RhYmxlTWV0YWRhdGF8bnVsbCB7XG4gICAgY29uc3QgdHlwZVN1bW1hcnkgPSB0aGlzLl9sb2FkU3VtbWFyeSh0eXBlLCBjcGwuQ29tcGlsZVN1bW1hcnlLaW5kLkluamVjdGFibGUpO1xuICAgIGNvbnN0IHR5cGVNZXRhZGF0YSA9IHR5cGVTdW1tYXJ5ID9cbiAgICAgICAgdHlwZVN1bW1hcnkudHlwZSA6XG4gICAgICAgIHRoaXMuX2dldFR5cGVNZXRhZGF0YSh0eXBlLCBkZXBlbmRlbmNpZXMsIHRocm93T25Vbmtub3duRGVwcyk7XG5cbiAgICBjb25zdCBhbm5vdGF0aW9uczogSW5qZWN0YWJsZVtdID1cbiAgICAgICAgdGhpcy5fcmVmbGVjdG9yLmFubm90YXRpb25zKHR5cGUpLmZpbHRlcihhbm4gPT4gY3JlYXRlSW5qZWN0YWJsZS5pc1R5cGVPZihhbm4pKTtcblxuICAgIGlmIChhbm5vdGF0aW9ucy5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIGNvbnN0IG1ldGEgPSBhbm5vdGF0aW9uc1thbm5vdGF0aW9ucy5sZW5ndGggLSAxXTtcbiAgICByZXR1cm4ge1xuICAgICAgc3ltYm9sOiB0eXBlLFxuICAgICAgdHlwZTogdHlwZU1ldGFkYXRhLFxuICAgICAgcHJvdmlkZWRJbjogbWV0YS5wcm92aWRlZEluLFxuICAgICAgdXNlVmFsdWU6IG1ldGEudXNlVmFsdWUsXG4gICAgICB1c2VDbGFzczogbWV0YS51c2VDbGFzcyxcbiAgICAgIHVzZUV4aXN0aW5nOiBtZXRhLnVzZUV4aXN0aW5nLFxuICAgICAgdXNlRmFjdG9yeTogbWV0YS51c2VGYWN0b3J5LFxuICAgICAgZGVwczogbWV0YS5kZXBzLFxuICAgIH07XG4gIH1cblxuICBwcml2YXRlIF9nZXRUeXBlTWV0YWRhdGEodHlwZTogVHlwZSwgZGVwZW5kZW5jaWVzOiBhbnlbXXxudWxsID0gbnVsbCwgdGhyb3dPblVua25vd25EZXBzID0gdHJ1ZSk6XG4gICAgICBjcGwuQ29tcGlsZVR5cGVNZXRhZGF0YSB7XG4gICAgY29uc3QgaWRlbnRpZmllciA9IHRoaXMuX2dldElkZW50aWZpZXJNZXRhZGF0YSh0eXBlKTtcbiAgICByZXR1cm4ge1xuICAgICAgcmVmZXJlbmNlOiBpZGVudGlmaWVyLnJlZmVyZW5jZSxcbiAgICAgIGRpRGVwczogdGhpcy5fZ2V0RGVwZW5kZW5jaWVzTWV0YWRhdGEoaWRlbnRpZmllci5yZWZlcmVuY2UsIGRlcGVuZGVuY2llcywgdGhyb3dPblVua25vd25EZXBzKSxcbiAgICAgIGxpZmVjeWNsZUhvb2tzOiBnZXRBbGxMaWZlY3ljbGVIb29rcyh0aGlzLl9yZWZsZWN0b3IsIGlkZW50aWZpZXIucmVmZXJlbmNlKSxcbiAgICB9O1xuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0RmFjdG9yeU1ldGFkYXRhKGZhY3Rvcnk6IEZ1bmN0aW9uLCBkZXBlbmRlbmNpZXM6IGFueVtdfG51bGwgPSBudWxsKTpcbiAgICAgIGNwbC5Db21waWxlRmFjdG9yeU1ldGFkYXRhIHtcbiAgICBmYWN0b3J5ID0gcmVzb2x2ZUZvcndhcmRSZWYoZmFjdG9yeSk7XG4gICAgcmV0dXJuIHtyZWZlcmVuY2U6IGZhY3RvcnksIGRpRGVwczogdGhpcy5fZ2V0RGVwZW5kZW5jaWVzTWV0YWRhdGEoZmFjdG9yeSwgZGVwZW5kZW5jaWVzKX07XG4gIH1cblxuICAvKipcbiAgICogR2V0cyB0aGUgbWV0YWRhdGEgZm9yIHRoZSBnaXZlbiBwaXBlLlxuICAgKiBUaGlzIGFzc3VtZXMgYGxvYWROZ01vZHVsZURpcmVjdGl2ZUFuZFBpcGVNZXRhZGF0YWAgaGFzIGJlZW4gY2FsbGVkIGZpcnN0LlxuICAgKi9cbiAgZ2V0UGlwZU1ldGFkYXRhKHBpcGVUeXBlOiBhbnkpOiBjcGwuQ29tcGlsZVBpcGVNZXRhZGF0YXxudWxsIHtcbiAgICBjb25zdCBwaXBlTWV0YSA9IHRoaXMuX3BpcGVDYWNoZS5nZXQocGlwZVR5cGUpO1xuICAgIGlmICghcGlwZU1ldGEpIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICBgSWxsZWdhbCBzdGF0ZTogZ2V0UGlwZU1ldGFkYXRhIGNhbiBvbmx5IGJlIGNhbGxlZCBhZnRlciBsb2FkTmdNb2R1bGVEaXJlY3RpdmVBbmRQaXBlTWV0YWRhdGEgZm9yIGEgbW9kdWxlIHRoYXQgZGVjbGFyZXMgaXQuIFBpcGUgJHtzdHJpbmdpZnlUeXBlKHBpcGVUeXBlKX0uYCksXG4gICAgICAgICAgcGlwZVR5cGUpO1xuICAgIH1cbiAgICByZXR1cm4gcGlwZU1ldGEgfHwgbnVsbDtcbiAgfVxuXG4gIGdldFBpcGVTdW1tYXJ5KHBpcGVUeXBlOiBhbnkpOiBjcGwuQ29tcGlsZVBpcGVTdW1tYXJ5IHtcbiAgICBjb25zdCBwaXBlU3VtbWFyeSA9XG4gICAgICAgIDxjcGwuQ29tcGlsZVBpcGVTdW1tYXJ5PnRoaXMuX2xvYWRTdW1tYXJ5KHBpcGVUeXBlLCBjcGwuQ29tcGlsZVN1bW1hcnlLaW5kLlBpcGUpO1xuICAgIGlmICghcGlwZVN1bW1hcnkpIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICBgSWxsZWdhbCBzdGF0ZTogQ291bGQgbm90IGxvYWQgdGhlIHN1bW1hcnkgZm9yIHBpcGUgJHtzdHJpbmdpZnlUeXBlKHBpcGVUeXBlKX0uYCksXG4gICAgICAgICAgcGlwZVR5cGUpO1xuICAgIH1cbiAgICByZXR1cm4gcGlwZVN1bW1hcnk7XG4gIH1cblxuICBnZXRPckxvYWRQaXBlTWV0YWRhdGEocGlwZVR5cGU6IGFueSk6IGNwbC5Db21waWxlUGlwZU1ldGFkYXRhIHtcbiAgICBsZXQgcGlwZU1ldGEgPSB0aGlzLl9waXBlQ2FjaGUuZ2V0KHBpcGVUeXBlKTtcbiAgICBpZiAoIXBpcGVNZXRhKSB7XG4gICAgICBwaXBlTWV0YSA9IHRoaXMuX2xvYWRQaXBlTWV0YWRhdGEocGlwZVR5cGUpO1xuICAgIH1cbiAgICByZXR1cm4gcGlwZU1ldGE7XG4gIH1cblxuICBwcml2YXRlIF9sb2FkUGlwZU1ldGFkYXRhKHBpcGVUeXBlOiBhbnkpOiBjcGwuQ29tcGlsZVBpcGVNZXRhZGF0YSB7XG4gICAgcGlwZVR5cGUgPSByZXNvbHZlRm9yd2FyZFJlZihwaXBlVHlwZSk7XG4gICAgY29uc3QgcGlwZUFubm90YXRpb24gPSB0aGlzLl9waXBlUmVzb2x2ZXIucmVzb2x2ZShwaXBlVHlwZSkgITtcblxuICAgIGNvbnN0IHBpcGVNZXRhID0gbmV3IGNwbC5Db21waWxlUGlwZU1ldGFkYXRhKHtcbiAgICAgIHR5cGU6IHRoaXMuX2dldFR5cGVNZXRhZGF0YShwaXBlVHlwZSksXG4gICAgICBuYW1lOiBwaXBlQW5ub3RhdGlvbi5uYW1lLFxuICAgICAgcHVyZTogISFwaXBlQW5ub3RhdGlvbi5wdXJlXG4gICAgfSk7XG4gICAgdGhpcy5fcGlwZUNhY2hlLnNldChwaXBlVHlwZSwgcGlwZU1ldGEpO1xuICAgIHRoaXMuX3N1bW1hcnlDYWNoZS5zZXQocGlwZVR5cGUsIHBpcGVNZXRhLnRvU3VtbWFyeSgpKTtcbiAgICByZXR1cm4gcGlwZU1ldGE7XG4gIH1cblxuICBwcml2YXRlIF9nZXREZXBlbmRlbmNpZXNNZXRhZGF0YShcbiAgICAgIHR5cGVPckZ1bmM6IFR5cGV8RnVuY3Rpb24sIGRlcGVuZGVuY2llczogYW55W118bnVsbCxcbiAgICAgIHRocm93T25Vbmtub3duRGVwcyA9IHRydWUpOiBjcGwuQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhW10ge1xuICAgIGxldCBoYXNVbmtub3duRGVwcyA9IGZhbHNlO1xuICAgIGNvbnN0IHBhcmFtcyA9IGRlcGVuZGVuY2llcyB8fCB0aGlzLl9yZWZsZWN0b3IucGFyYW1ldGVycyh0eXBlT3JGdW5jKSB8fCBbXTtcblxuICAgIGNvbnN0IGRlcGVuZGVuY2llc01ldGFkYXRhOiBjcGwuQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhW10gPSBwYXJhbXMubWFwKChwYXJhbSkgPT4ge1xuICAgICAgbGV0IGlzQXR0cmlidXRlID0gZmFsc2U7XG4gICAgICBsZXQgaXNIb3N0ID0gZmFsc2U7XG4gICAgICBsZXQgaXNTZWxmID0gZmFsc2U7XG4gICAgICBsZXQgaXNTa2lwU2VsZiA9IGZhbHNlO1xuICAgICAgbGV0IGlzT3B0aW9uYWwgPSBmYWxzZTtcbiAgICAgIGxldCB0b2tlbjogYW55ID0gbnVsbDtcbiAgICAgIGlmIChBcnJheS5pc0FycmF5KHBhcmFtKSkge1xuICAgICAgICBwYXJhbS5mb3JFYWNoKChwYXJhbUVudHJ5OiBhbnkpID0+IHtcbiAgICAgICAgICBpZiAoY3JlYXRlSG9zdC5pc1R5cGVPZihwYXJhbUVudHJ5KSkge1xuICAgICAgICAgICAgaXNIb3N0ID0gdHJ1ZTtcbiAgICAgICAgICB9IGVsc2UgaWYgKGNyZWF0ZVNlbGYuaXNUeXBlT2YocGFyYW1FbnRyeSkpIHtcbiAgICAgICAgICAgIGlzU2VsZiA9IHRydWU7XG4gICAgICAgICAgfSBlbHNlIGlmIChjcmVhdGVTa2lwU2VsZi5pc1R5cGVPZihwYXJhbUVudHJ5KSkge1xuICAgICAgICAgICAgaXNTa2lwU2VsZiA9IHRydWU7XG4gICAgICAgICAgfSBlbHNlIGlmIChjcmVhdGVPcHRpb25hbC5pc1R5cGVPZihwYXJhbUVudHJ5KSkge1xuICAgICAgICAgICAgaXNPcHRpb25hbCA9IHRydWU7XG4gICAgICAgICAgfSBlbHNlIGlmIChjcmVhdGVBdHRyaWJ1dGUuaXNUeXBlT2YocGFyYW1FbnRyeSkpIHtcbiAgICAgICAgICAgIGlzQXR0cmlidXRlID0gdHJ1ZTtcbiAgICAgICAgICAgIHRva2VuID0gKHBhcmFtRW50cnkgYXMgYW55KS5hdHRyaWJ1dGVOYW1lO1xuICAgICAgICAgIH0gZWxzZSBpZiAoY3JlYXRlSW5qZWN0LmlzVHlwZU9mKHBhcmFtRW50cnkpKSB7XG4gICAgICAgICAgICB0b2tlbiA9IChwYXJhbUVudHJ5IGFzIGFueSkudG9rZW47XG4gICAgICAgICAgfSBlbHNlIGlmIChcbiAgICAgICAgICAgICAgY3JlYXRlSW5qZWN0aW9uVG9rZW4uaXNUeXBlT2YocGFyYW1FbnRyeSkgfHxcbiAgICAgICAgICAgICAgKHBhcmFtRW50cnkgYXMgYW55KSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkge1xuICAgICAgICAgICAgdG9rZW4gPSBwYXJhbUVudHJ5O1xuICAgICAgICAgIH0gZWxzZSBpZiAoaXNWYWxpZFR5cGUocGFyYW1FbnRyeSkgJiYgdG9rZW4gPT0gbnVsbCkge1xuICAgICAgICAgICAgdG9rZW4gPSBwYXJhbUVudHJ5O1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0b2tlbiA9IHBhcmFtO1xuICAgICAgfVxuICAgICAgaWYgKHRva2VuID09IG51bGwpIHtcbiAgICAgICAgaGFzVW5rbm93bkRlcHMgPSB0cnVlO1xuICAgICAgICByZXR1cm4ge307XG4gICAgICB9XG5cbiAgICAgIHJldHVybiB7XG4gICAgICAgIGlzQXR0cmlidXRlLFxuICAgICAgICBpc0hvc3QsXG4gICAgICAgIGlzU2VsZixcbiAgICAgICAgaXNTa2lwU2VsZixcbiAgICAgICAgaXNPcHRpb25hbCxcbiAgICAgICAgdG9rZW46IHRoaXMuX2dldFRva2VuTWV0YWRhdGEodG9rZW4pXG4gICAgICB9O1xuXG4gICAgfSk7XG5cbiAgICBpZiAoaGFzVW5rbm93bkRlcHMpIHtcbiAgICAgIGNvbnN0IGRlcHNUb2tlbnMgPVxuICAgICAgICAgIGRlcGVuZGVuY2llc01ldGFkYXRhLm1hcCgoZGVwKSA9PiBkZXAudG9rZW4gPyBzdHJpbmdpZnlUeXBlKGRlcC50b2tlbikgOiAnPycpLmpvaW4oJywgJyk7XG4gICAgICBjb25zdCBtZXNzYWdlID1cbiAgICAgICAgICBgQ2FuJ3QgcmVzb2x2ZSBhbGwgcGFyYW1ldGVycyBmb3IgJHtzdHJpbmdpZnlUeXBlKHR5cGVPckZ1bmMpfTogKCR7ZGVwc1Rva2Vuc30pLmA7XG4gICAgICBpZiAodGhyb3dPblVua25vd25EZXBzIHx8IHRoaXMuX2NvbmZpZy5zdHJpY3RJbmplY3Rpb25QYXJhbWV0ZXJzKSB7XG4gICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKHN5bnRheEVycm9yKG1lc3NhZ2UpLCB0eXBlT3JGdW5jKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuX2NvbnNvbGUud2FybihgV2FybmluZzogJHttZXNzYWdlfSBUaGlzIHdpbGwgYmVjb21lIGFuIGVycm9yIGluIEFuZ3VsYXIgdjYueGApO1xuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiBkZXBlbmRlbmNpZXNNZXRhZGF0YTtcbiAgfVxuXG4gIHByaXZhdGUgX2dldFRva2VuTWV0YWRhdGEodG9rZW46IGFueSk6IGNwbC5Db21waWxlVG9rZW5NZXRhZGF0YSB7XG4gICAgdG9rZW4gPSByZXNvbHZlRm9yd2FyZFJlZih0b2tlbik7XG4gICAgbGV0IGNvbXBpbGVUb2tlbjogY3BsLkNvbXBpbGVUb2tlbk1ldGFkYXRhO1xuICAgIGlmICh0eXBlb2YgdG9rZW4gPT09ICdzdHJpbmcnKSB7XG4gICAgICBjb21waWxlVG9rZW4gPSB7dmFsdWU6IHRva2VufTtcbiAgICB9IGVsc2Uge1xuICAgICAgY29tcGlsZVRva2VuID0ge2lkZW50aWZpZXI6IHtyZWZlcmVuY2U6IHRva2VufX07XG4gICAgfVxuICAgIHJldHVybiBjb21waWxlVG9rZW47XG4gIH1cblxuICBwcml2YXRlIF9nZXRQcm92aWRlcnNNZXRhZGF0YShcbiAgICAgIHByb3ZpZGVyczogUHJvdmlkZXJbXSwgdGFyZ2V0RW50cnlDb21wb25lbnRzOiBjcGwuQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXSxcbiAgICAgIGRlYnVnSW5mbz86IHN0cmluZywgY29tcGlsZVByb3ZpZGVyczogY3BsLkNvbXBpbGVQcm92aWRlck1ldGFkYXRhW10gPSBbXSxcbiAgICAgIHR5cGU/OiBhbnkpOiBjcGwuQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSB7XG4gICAgcHJvdmlkZXJzLmZvckVhY2goKHByb3ZpZGVyOiBhbnksIHByb3ZpZGVySWR4OiBudW1iZXIpID0+IHtcbiAgICAgIGlmIChBcnJheS5pc0FycmF5KHByb3ZpZGVyKSkge1xuICAgICAgICB0aGlzLl9nZXRQcm92aWRlcnNNZXRhZGF0YShwcm92aWRlciwgdGFyZ2V0RW50cnlDb21wb25lbnRzLCBkZWJ1Z0luZm8sIGNvbXBpbGVQcm92aWRlcnMpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcHJvdmlkZXIgPSByZXNvbHZlRm9yd2FyZFJlZihwcm92aWRlcik7XG4gICAgICAgIGxldCBwcm92aWRlck1ldGE6IGNwbC5Qcm92aWRlck1ldGEgPSB1bmRlZmluZWQgITtcbiAgICAgICAgaWYgKHByb3ZpZGVyICYmIHR5cGVvZiBwcm92aWRlciA9PT0gJ29iamVjdCcgJiYgcHJvdmlkZXIuaGFzT3duUHJvcGVydHkoJ3Byb3ZpZGUnKSkge1xuICAgICAgICAgIHRoaXMuX3ZhbGlkYXRlUHJvdmlkZXIocHJvdmlkZXIpO1xuICAgICAgICAgIHByb3ZpZGVyTWV0YSA9IG5ldyBjcGwuUHJvdmlkZXJNZXRhKHByb3ZpZGVyLnByb3ZpZGUsIHByb3ZpZGVyKTtcbiAgICAgICAgfSBlbHNlIGlmIChpc1ZhbGlkVHlwZShwcm92aWRlcikpIHtcbiAgICAgICAgICBwcm92aWRlck1ldGEgPSBuZXcgY3BsLlByb3ZpZGVyTWV0YShwcm92aWRlciwge3VzZUNsYXNzOiBwcm92aWRlcn0pO1xuICAgICAgICB9IGVsc2UgaWYgKHByb3ZpZGVyID09PSB2b2lkIDApIHtcbiAgICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgYEVuY291bnRlcmVkIHVuZGVmaW5lZCBwcm92aWRlciEgVXN1YWxseSB0aGlzIG1lYW5zIHlvdSBoYXZlIGEgY2lyY3VsYXIgZGVwZW5kZW5jaWVzLiBUaGlzIG1pZ2h0IGJlIGNhdXNlZCBieSB1c2luZyAnYmFycmVsJyBpbmRleC50cyBmaWxlcy5gKSk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnN0IHByb3ZpZGVyc0luZm8gPVxuICAgICAgICAgICAgICAoPHN0cmluZ1tdPnByb3ZpZGVycy5yZWR1Y2UoXG4gICAgICAgICAgICAgICAgICAgKHNvRmFyOiBzdHJpbmdbXSwgc2VlblByb3ZpZGVyOiBhbnksIHNlZW5Qcm92aWRlcklkeDogbnVtYmVyKSA9PiB7XG4gICAgICAgICAgICAgICAgICAgICBpZiAoc2VlblByb3ZpZGVySWR4IDwgcHJvdmlkZXJJZHgpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgc29GYXIucHVzaChgJHtzdHJpbmdpZnlUeXBlKHNlZW5Qcm92aWRlcil9YCk7XG4gICAgICAgICAgICAgICAgICAgICB9IGVsc2UgaWYgKHNlZW5Qcm92aWRlcklkeCA9PSBwcm92aWRlcklkeCkge1xuICAgICAgICAgICAgICAgICAgICAgICBzb0Zhci5wdXNoKGA/JHtzdHJpbmdpZnlUeXBlKHNlZW5Qcm92aWRlcil9P2ApO1xuICAgICAgICAgICAgICAgICAgICAgfSBlbHNlIGlmIChzZWVuUHJvdmlkZXJJZHggPT0gcHJvdmlkZXJJZHggKyAxKSB7XG4gICAgICAgICAgICAgICAgICAgICAgIHNvRmFyLnB1c2goJy4uLicpO1xuICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIHNvRmFyO1xuICAgICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgICAgW10pKVxuICAgICAgICAgICAgICAgICAgLmpvaW4oJywgJyk7XG4gICAgICAgICAgdGhpcy5fcmVwb3J0RXJyb3IoXG4gICAgICAgICAgICAgIHN5bnRheEVycm9yKFxuICAgICAgICAgICAgICAgICAgYEludmFsaWQgJHtkZWJ1Z0luZm8gPyBkZWJ1Z0luZm8gOiAncHJvdmlkZXInfSAtIG9ubHkgaW5zdGFuY2VzIG9mIFByb3ZpZGVyIGFuZCBUeXBlIGFyZSBhbGxvd2VkLCBnb3Q6IFske3Byb3ZpZGVyc0luZm99XWApLFxuICAgICAgICAgICAgICB0eXBlKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgICAgaWYgKHByb3ZpZGVyTWV0YS50b2tlbiA9PT1cbiAgICAgICAgICAgIHRoaXMuX3JlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuQU5BTFlaRV9GT1JfRU5UUllfQ09NUE9ORU5UUykpIHtcbiAgICAgICAgICB0YXJnZXRFbnRyeUNvbXBvbmVudHMucHVzaCguLi50aGlzLl9nZXRFbnRyeUNvbXBvbmVudHNGcm9tUHJvdmlkZXIocHJvdmlkZXJNZXRhLCB0eXBlKSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29tcGlsZVByb3ZpZGVycy5wdXNoKHRoaXMuZ2V0UHJvdmlkZXJNZXRhZGF0YShwcm92aWRlck1ldGEpKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0pO1xuICAgIHJldHVybiBjb21waWxlUHJvdmlkZXJzO1xuICB9XG5cbiAgcHJpdmF0ZSBfdmFsaWRhdGVQcm92aWRlcihwcm92aWRlcjogYW55KTogdm9pZCB7XG4gICAgaWYgKHByb3ZpZGVyLmhhc093blByb3BlcnR5KCd1c2VDbGFzcycpICYmIHByb3ZpZGVyLnVzZUNsYXNzID09IG51bGwpIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKHN5bnRheEVycm9yKFxuICAgICAgICAgIGBJbnZhbGlkIHByb3ZpZGVyIGZvciAke3N0cmluZ2lmeVR5cGUocHJvdmlkZXIucHJvdmlkZSl9LiB1c2VDbGFzcyBjYW5ub3QgYmUgJHtwcm92aWRlci51c2VDbGFzc30uXG4gICAgICAgICAgIFVzdWFsbHkgaXQgaGFwcGVucyB3aGVuOlxuICAgICAgICAgICAxLiBUaGVyZSdzIGEgY2lyY3VsYXIgZGVwZW5kZW5jeSAobWlnaHQgYmUgY2F1c2VkIGJ5IHVzaW5nIGluZGV4LnRzIChiYXJyZWwpIGZpbGVzKS5cbiAgICAgICAgICAgMi4gQ2xhc3Mgd2FzIHVzZWQgYmVmb3JlIGl0IHdhcyBkZWNsYXJlZC4gVXNlIGZvcndhcmRSZWYgaW4gdGhpcyBjYXNlLmApKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9nZXRFbnRyeUNvbXBvbmVudHNGcm9tUHJvdmlkZXIocHJvdmlkZXI6IGNwbC5Qcm92aWRlck1ldGEsIHR5cGU/OiBhbnkpOlxuICAgICAgY3BsLkNvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW10ge1xuICAgIGNvbnN0IGNvbXBvbmVudHM6IGNwbC5Db21waWxlRW50cnlDb21wb25lbnRNZXRhZGF0YVtdID0gW107XG4gICAgY29uc3QgY29sbGVjdGVkSWRlbnRpZmllcnM6IGNwbC5Db21waWxlSWRlbnRpZmllck1ldGFkYXRhW10gPSBbXTtcblxuICAgIGlmIChwcm92aWRlci51c2VGYWN0b3J5IHx8IHByb3ZpZGVyLnVzZUV4aXN0aW5nIHx8IHByb3ZpZGVyLnVzZUNsYXNzKSB7XG4gICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICBzeW50YXhFcnJvcihgVGhlIEFOQUxZWkVfRk9SX0VOVFJZX0NPTVBPTkVOVFMgdG9rZW4gb25seSBzdXBwb3J0cyB1c2VWYWx1ZSFgKSwgdHlwZSk7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgaWYgKCFwcm92aWRlci5tdWx0aSkge1xuICAgICAgdGhpcy5fcmVwb3J0RXJyb3IoXG4gICAgICAgICAgc3ludGF4RXJyb3IoYFRoZSBBTkFMWVpFX0ZPUl9FTlRSWV9DT01QT05FTlRTIHRva2VuIG9ubHkgc3VwcG9ydHMgJ211bHRpID0gdHJ1ZSchYCksXG4gICAgICAgICAgdHlwZSk7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuXG4gICAgZXh0cmFjdElkZW50aWZpZXJzKHByb3ZpZGVyLnVzZVZhbHVlLCBjb2xsZWN0ZWRJZGVudGlmaWVycyk7XG4gICAgY29sbGVjdGVkSWRlbnRpZmllcnMuZm9yRWFjaCgoaWRlbnRpZmllcikgPT4ge1xuICAgICAgY29uc3QgZW50cnkgPSB0aGlzLl9nZXRFbnRyeUNvbXBvbmVudE1ldGFkYXRhKGlkZW50aWZpZXIucmVmZXJlbmNlLCBmYWxzZSk7XG4gICAgICBpZiAoZW50cnkpIHtcbiAgICAgICAgY29tcG9uZW50cy5wdXNoKGVudHJ5KTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gY29tcG9uZW50cztcbiAgfVxuXG4gIHByaXZhdGUgX2dldEVudHJ5Q29tcG9uZW50TWV0YWRhdGEoZGlyVHlwZTogYW55LCB0aHJvd0lmTm90Rm91bmQgPSB0cnVlKTpcbiAgICAgIGNwbC5Db21waWxlRW50cnlDb21wb25lbnRNZXRhZGF0YXxudWxsIHtcbiAgICBjb25zdCBkaXJNZXRhID0gdGhpcy5nZXROb25Ob3JtYWxpemVkRGlyZWN0aXZlTWV0YWRhdGEoZGlyVHlwZSk7XG4gICAgaWYgKGRpck1ldGEgJiYgZGlyTWV0YS5tZXRhZGF0YS5pc0NvbXBvbmVudCkge1xuICAgICAgcmV0dXJuIHtjb21wb25lbnRUeXBlOiBkaXJUeXBlLCBjb21wb25lbnRGYWN0b3J5OiBkaXJNZXRhLm1ldGFkYXRhLmNvbXBvbmVudEZhY3RvcnkgIX07XG4gICAgfVxuICAgIGNvbnN0IGRpclN1bW1hcnkgPVxuICAgICAgICA8Y3BsLkNvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5PnRoaXMuX2xvYWRTdW1tYXJ5KGRpclR5cGUsIGNwbC5Db21waWxlU3VtbWFyeUtpbmQuRGlyZWN0aXZlKTtcbiAgICBpZiAoZGlyU3VtbWFyeSAmJiBkaXJTdW1tYXJ5LmlzQ29tcG9uZW50KSB7XG4gICAgICByZXR1cm4ge2NvbXBvbmVudFR5cGU6IGRpclR5cGUsIGNvbXBvbmVudEZhY3Rvcnk6IGRpclN1bW1hcnkuY29tcG9uZW50RmFjdG9yeSAhfTtcbiAgICB9XG4gICAgaWYgKHRocm93SWZOb3RGb3VuZCkge1xuICAgICAgdGhyb3cgc3ludGF4RXJyb3IoYCR7ZGlyVHlwZS5uYW1lfSBjYW5ub3QgYmUgdXNlZCBhcyBhbiBlbnRyeSBjb21wb25lbnQuYCk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0SW5qZWN0YWJsZVR5cGVNZXRhZGF0YSh0eXBlOiBUeXBlLCBkZXBlbmRlbmNpZXM6IGFueVtdfG51bGwgPSBudWxsKTpcbiAgICAgIGNwbC5Db21waWxlVHlwZU1ldGFkYXRhIHtcbiAgICBjb25zdCB0eXBlU3VtbWFyeSA9IHRoaXMuX2xvYWRTdW1tYXJ5KHR5cGUsIGNwbC5Db21waWxlU3VtbWFyeUtpbmQuSW5qZWN0YWJsZSk7XG4gICAgaWYgKHR5cGVTdW1tYXJ5KSB7XG4gICAgICByZXR1cm4gdHlwZVN1bW1hcnkudHlwZTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX2dldFR5cGVNZXRhZGF0YSh0eXBlLCBkZXBlbmRlbmNpZXMpO1xuICB9XG5cbiAgZ2V0UHJvdmlkZXJNZXRhZGF0YShwcm92aWRlcjogY3BsLlByb3ZpZGVyTWV0YSk6IGNwbC5Db21waWxlUHJvdmlkZXJNZXRhZGF0YSB7XG4gICAgbGV0IGNvbXBpbGVEZXBzOiBjcGwuQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhW10gPSB1bmRlZmluZWQgITtcbiAgICBsZXQgY29tcGlsZVR5cGVNZXRhZGF0YTogY3BsLkNvbXBpbGVUeXBlTWV0YWRhdGEgPSBudWxsICE7XG4gICAgbGV0IGNvbXBpbGVGYWN0b3J5TWV0YWRhdGE6IGNwbC5Db21waWxlRmFjdG9yeU1ldGFkYXRhID0gbnVsbCAhO1xuICAgIGxldCB0b2tlbjogY3BsLkNvbXBpbGVUb2tlbk1ldGFkYXRhID0gdGhpcy5fZ2V0VG9rZW5NZXRhZGF0YShwcm92aWRlci50b2tlbik7XG5cbiAgICBpZiAocHJvdmlkZXIudXNlQ2xhc3MpIHtcbiAgICAgIGNvbXBpbGVUeXBlTWV0YWRhdGEgPVxuICAgICAgICAgIHRoaXMuX2dldEluamVjdGFibGVUeXBlTWV0YWRhdGEocHJvdmlkZXIudXNlQ2xhc3MsIHByb3ZpZGVyLmRlcGVuZGVuY2llcyk7XG4gICAgICBjb21waWxlRGVwcyA9IGNvbXBpbGVUeXBlTWV0YWRhdGEuZGlEZXBzO1xuICAgICAgaWYgKHByb3ZpZGVyLnRva2VuID09PSBwcm92aWRlci51c2VDbGFzcykge1xuICAgICAgICAvLyB1c2UgdGhlIGNvbXBpbGVUeXBlTWV0YWRhdGEgYXMgaXQgY29udGFpbnMgaW5mb3JtYXRpb24gYWJvdXQgbGlmZWN5Y2xlSG9va3MuLi5cbiAgICAgICAgdG9rZW4gPSB7aWRlbnRpZmllcjogY29tcGlsZVR5cGVNZXRhZGF0YX07XG4gICAgICB9XG4gICAgfSBlbHNlIGlmIChwcm92aWRlci51c2VGYWN0b3J5KSB7XG4gICAgICBjb21waWxlRmFjdG9yeU1ldGFkYXRhID0gdGhpcy5fZ2V0RmFjdG9yeU1ldGFkYXRhKHByb3ZpZGVyLnVzZUZhY3RvcnksIHByb3ZpZGVyLmRlcGVuZGVuY2llcyk7XG4gICAgICBjb21waWxlRGVwcyA9IGNvbXBpbGVGYWN0b3J5TWV0YWRhdGEuZGlEZXBzO1xuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICB0b2tlbjogdG9rZW4sXG4gICAgICB1c2VDbGFzczogY29tcGlsZVR5cGVNZXRhZGF0YSxcbiAgICAgIHVzZVZhbHVlOiBwcm92aWRlci51c2VWYWx1ZSxcbiAgICAgIHVzZUZhY3Rvcnk6IGNvbXBpbGVGYWN0b3J5TWV0YWRhdGEsXG4gICAgICB1c2VFeGlzdGluZzogcHJvdmlkZXIudXNlRXhpc3RpbmcgPyB0aGlzLl9nZXRUb2tlbk1ldGFkYXRhKHByb3ZpZGVyLnVzZUV4aXN0aW5nKSA6IHVuZGVmaW5lZCxcbiAgICAgIGRlcHM6IGNvbXBpbGVEZXBzLFxuICAgICAgbXVsdGk6IHByb3ZpZGVyLm11bHRpXG4gICAgfTtcbiAgfVxuXG4gIHByaXZhdGUgX2dldFF1ZXJpZXNNZXRhZGF0YShcbiAgICAgIHF1ZXJpZXM6IHtba2V5OiBzdHJpbmddOiBRdWVyeX0sIGlzVmlld1F1ZXJ5OiBib29sZWFuLFxuICAgICAgZGlyZWN0aXZlVHlwZTogVHlwZSk6IGNwbC5Db21waWxlUXVlcnlNZXRhZGF0YVtdIHtcbiAgICBjb25zdCByZXM6IGNwbC5Db21waWxlUXVlcnlNZXRhZGF0YVtdID0gW107XG5cbiAgICBPYmplY3Qua2V5cyhxdWVyaWVzKS5mb3JFYWNoKChwcm9wZXJ0eU5hbWU6IHN0cmluZykgPT4ge1xuICAgICAgY29uc3QgcXVlcnkgPSBxdWVyaWVzW3Byb3BlcnR5TmFtZV07XG4gICAgICBpZiAocXVlcnkuaXNWaWV3UXVlcnkgPT09IGlzVmlld1F1ZXJ5KSB7XG4gICAgICAgIHJlcy5wdXNoKHRoaXMuX2dldFF1ZXJ5TWV0YWRhdGEocXVlcnksIHByb3BlcnR5TmFtZSwgZGlyZWN0aXZlVHlwZSkpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgcmV0dXJuIHJlcztcbiAgfVxuXG4gIHByaXZhdGUgX3F1ZXJ5VmFyQmluZGluZ3Moc2VsZWN0b3I6IGFueSk6IHN0cmluZ1tdIHsgcmV0dXJuIHNlbGVjdG9yLnNwbGl0KC9cXHMqLFxccyovKTsgfVxuXG4gIHByaXZhdGUgX2dldFF1ZXJ5TWV0YWRhdGEocTogUXVlcnksIHByb3BlcnR5TmFtZTogc3RyaW5nLCB0eXBlT3JGdW5jOiBUeXBlfEZ1bmN0aW9uKTpcbiAgICAgIGNwbC5Db21waWxlUXVlcnlNZXRhZGF0YSB7XG4gICAgbGV0IHNlbGVjdG9yczogY3BsLkNvbXBpbGVUb2tlbk1ldGFkYXRhW107XG4gICAgaWYgKHR5cGVvZiBxLnNlbGVjdG9yID09PSAnc3RyaW5nJykge1xuICAgICAgc2VsZWN0b3JzID1cbiAgICAgICAgICB0aGlzLl9xdWVyeVZhckJpbmRpbmdzKHEuc2VsZWN0b3IpLm1hcCh2YXJOYW1lID0+IHRoaXMuX2dldFRva2VuTWV0YWRhdGEodmFyTmFtZSkpO1xuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoIXEuc2VsZWN0b3IpIHtcbiAgICAgICAgdGhpcy5fcmVwb3J0RXJyb3IoXG4gICAgICAgICAgICBzeW50YXhFcnJvcihcbiAgICAgICAgICAgICAgICBgQ2FuJ3QgY29uc3RydWN0IGEgcXVlcnkgZm9yIHRoZSBwcm9wZXJ0eSBcIiR7cHJvcGVydHlOYW1lfVwiIG9mIFwiJHtzdHJpbmdpZnlUeXBlKHR5cGVPckZ1bmMpfVwiIHNpbmNlIHRoZSBxdWVyeSBzZWxlY3RvciB3YXNuJ3QgZGVmaW5lZC5gKSxcbiAgICAgICAgICAgIHR5cGVPckZ1bmMpO1xuICAgICAgICBzZWxlY3RvcnMgPSBbXTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHNlbGVjdG9ycyA9IFt0aGlzLl9nZXRUb2tlbk1ldGFkYXRhKHEuc2VsZWN0b3IpXTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4ge1xuICAgICAgc2VsZWN0b3JzLFxuICAgICAgZmlyc3Q6IHEuZmlyc3QsXG4gICAgICBkZXNjZW5kYW50czogcS5kZXNjZW5kYW50cywgcHJvcGVydHlOYW1lLFxuICAgICAgcmVhZDogcS5yZWFkID8gdGhpcy5fZ2V0VG9rZW5NZXRhZGF0YShxLnJlYWQpIDogbnVsbCAhLFxuICAgICAgc3RhdGljOiBxLnN0YXRpY1xuICAgIH07XG4gIH1cblxuICBwcml2YXRlIF9yZXBvcnRFcnJvcihlcnJvcjogYW55LCB0eXBlPzogYW55LCBvdGhlclR5cGU/OiBhbnkpIHtcbiAgICBpZiAodGhpcy5fZXJyb3JDb2xsZWN0b3IpIHtcbiAgICAgIHRoaXMuX2Vycm9yQ29sbGVjdG9yKGVycm9yLCB0eXBlKTtcbiAgICAgIGlmIChvdGhlclR5cGUpIHtcbiAgICAgICAgdGhpcy5fZXJyb3JDb2xsZWN0b3IoZXJyb3IsIG90aGVyVHlwZSk7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIHRocm93IGVycm9yO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBmbGF0dGVuQXJyYXkodHJlZTogYW55W10sIG91dDogQXJyYXk8YW55PiA9IFtdKTogQXJyYXk8YW55PiB7XG4gIGlmICh0cmVlKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0cmVlLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBpdGVtID0gcmVzb2x2ZUZvcndhcmRSZWYodHJlZVtpXSk7XG4gICAgICBpZiAoQXJyYXkuaXNBcnJheShpdGVtKSkge1xuICAgICAgICBmbGF0dGVuQXJyYXkoaXRlbSwgb3V0KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIG91dC5wdXNoKGl0ZW0pO1xuICAgICAgfVxuICAgIH1cbiAgfVxuICByZXR1cm4gb3V0O1xufVxuXG5mdW5jdGlvbiBkZWR1cGVBcnJheShhcnJheTogYW55W10pOiBBcnJheTxhbnk+IHtcbiAgaWYgKGFycmF5KSB7XG4gICAgcmV0dXJuIEFycmF5LmZyb20obmV3IFNldChhcnJheSkpO1xuICB9XG4gIHJldHVybiBbXTtcbn1cblxuZnVuY3Rpb24gZmxhdHRlbkFuZERlZHVwZUFycmF5KHRyZWU6IGFueVtdKTogQXJyYXk8YW55PiB7XG4gIHJldHVybiBkZWR1cGVBcnJheShmbGF0dGVuQXJyYXkodHJlZSkpO1xufVxuXG5mdW5jdGlvbiBpc1ZhbGlkVHlwZSh2YWx1ZTogYW55KTogYm9vbGVhbiB7XG4gIHJldHVybiAodmFsdWUgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHx8ICh2YWx1ZSBpbnN0YW5jZW9mIFR5cGUpO1xufVxuXG5mdW5jdGlvbiBleHRyYWN0SWRlbnRpZmllcnModmFsdWU6IGFueSwgdGFyZ2V0SWRlbnRpZmllcnM6IGNwbC5Db21waWxlSWRlbnRpZmllck1ldGFkYXRhW10pIHtcbiAgdmlzaXRWYWx1ZSh2YWx1ZSwgbmV3IF9Db21waWxlVmFsdWVDb252ZXJ0ZXIoKSwgdGFyZ2V0SWRlbnRpZmllcnMpO1xufVxuXG5jbGFzcyBfQ29tcGlsZVZhbHVlQ29udmVydGVyIGV4dGVuZHMgVmFsdWVUcmFuc2Zvcm1lciB7XG4gIHZpc2l0T3RoZXIodmFsdWU6IGFueSwgdGFyZ2V0SWRlbnRpZmllcnM6IGNwbC5Db21waWxlSWRlbnRpZmllck1ldGFkYXRhW10pOiBhbnkge1xuICAgIHRhcmdldElkZW50aWZpZXJzLnB1c2goe3JlZmVyZW5jZTogdmFsdWV9KTtcbiAgfVxufVxuXG5mdW5jdGlvbiBzdHJpbmdpZnlUeXBlKHR5cGU6IGFueSk6IHN0cmluZyB7XG4gIGlmICh0eXBlIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgcmV0dXJuIGAke3R5cGUubmFtZX0gaW4gJHt0eXBlLmZpbGVQYXRofWA7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIHN0cmluZ2lmeSh0eXBlKTtcbiAgfVxufVxuXG4vKipcbiAqIEluZGljYXRlcyB0aGF0IGEgY29tcG9uZW50IGlzIHN0aWxsIGJlaW5nIGxvYWRlZCBpbiBhIHN5bmNocm9ub3VzIGNvbXBpbGUuXG4gKi9cbmZ1bmN0aW9uIGNvbXBvbmVudFN0aWxsTG9hZGluZ0Vycm9yKGNvbXBUeXBlOiBUeXBlKSB7XG4gIGNvbnN0IGVycm9yID1cbiAgICAgIEVycm9yKGBDYW4ndCBjb21waWxlIHN5bmNocm9ub3VzbHkgYXMgJHtzdHJpbmdpZnkoY29tcFR5cGUpfSBpcyBzdGlsbCBiZWluZyBsb2FkZWQhYCk7XG4gIChlcnJvciBhcyBhbnkpW0VSUk9SX0NPTVBPTkVOVF9UWVBFXSA9IGNvbXBUeXBlO1xuICByZXR1cm4gZXJyb3I7XG59XG4iXX0=