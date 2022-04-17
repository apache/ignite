/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/provider_analyzer", ["require", "exports", "tslib", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/identifiers", "@angular/compiler/src/parse_util", "@angular/compiler/src/template_parser/template_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var identifiers_1 = require("@angular/compiler/src/identifiers");
    var parse_util_1 = require("@angular/compiler/src/parse_util");
    var template_ast_1 = require("@angular/compiler/src/template_parser/template_ast");
    var ProviderError = /** @class */ (function (_super) {
        tslib_1.__extends(ProviderError, _super);
        function ProviderError(message, span) {
            return _super.call(this, span, message) || this;
        }
        return ProviderError;
    }(parse_util_1.ParseError));
    exports.ProviderError = ProviderError;
    var ProviderViewContext = /** @class */ (function () {
        function ProviderViewContext(reflector, component) {
            var _this = this;
            this.reflector = reflector;
            this.component = component;
            this.errors = [];
            this.viewQueries = _getViewQueries(component);
            this.viewProviders = new Map();
            component.viewProviders.forEach(function (provider) {
                if (_this.viewProviders.get(compile_metadata_1.tokenReference(provider.token)) == null) {
                    _this.viewProviders.set(compile_metadata_1.tokenReference(provider.token), true);
                }
            });
        }
        return ProviderViewContext;
    }());
    exports.ProviderViewContext = ProviderViewContext;
    var ProviderElementContext = /** @class */ (function () {
        function ProviderElementContext(viewContext, _parent, _isViewRoot, _directiveAsts, attrs, refs, isTemplate, contentQueryStartId, _sourceSpan) {
            var _this = this;
            this.viewContext = viewContext;
            this._parent = _parent;
            this._isViewRoot = _isViewRoot;
            this._directiveAsts = _directiveAsts;
            this._sourceSpan = _sourceSpan;
            this._transformedProviders = new Map();
            this._seenProviders = new Map();
            this._queriedTokens = new Map();
            this.transformedHasViewContainer = false;
            this._attrs = {};
            attrs.forEach(function (attrAst) { return _this._attrs[attrAst.name] = attrAst.value; });
            var directivesMeta = _directiveAsts.map(function (directiveAst) { return directiveAst.directive; });
            this._allProviders =
                _resolveProvidersFromDirectives(directivesMeta, _sourceSpan, viewContext.errors);
            this._contentQueries = _getContentQueries(contentQueryStartId, directivesMeta);
            Array.from(this._allProviders.values()).forEach(function (provider) {
                _this._addQueryReadsTo(provider.token, provider.token, _this._queriedTokens);
            });
            if (isTemplate) {
                var templateRefId = identifiers_1.createTokenForExternalReference(this.viewContext.reflector, identifiers_1.Identifiers.TemplateRef);
                this._addQueryReadsTo(templateRefId, templateRefId, this._queriedTokens);
            }
            refs.forEach(function (refAst) {
                var defaultQueryValue = refAst.value ||
                    identifiers_1.createTokenForExternalReference(_this.viewContext.reflector, identifiers_1.Identifiers.ElementRef);
                _this._addQueryReadsTo({ value: refAst.name }, defaultQueryValue, _this._queriedTokens);
            });
            if (this._queriedTokens.get(this.viewContext.reflector.resolveExternalReference(identifiers_1.Identifiers.ViewContainerRef))) {
                this.transformedHasViewContainer = true;
            }
            // create the providers that we know are eager first
            Array.from(this._allProviders.values()).forEach(function (provider) {
                var eager = provider.eager || _this._queriedTokens.get(compile_metadata_1.tokenReference(provider.token));
                if (eager) {
                    _this._getOrCreateLocalProvider(provider.providerType, provider.token, true);
                }
            });
        }
        ProviderElementContext.prototype.afterElement = function () {
            var _this = this;
            // collect lazy providers
            Array.from(this._allProviders.values()).forEach(function (provider) {
                _this._getOrCreateLocalProvider(provider.providerType, provider.token, false);
            });
        };
        Object.defineProperty(ProviderElementContext.prototype, "transformProviders", {
            get: function () {
                // Note: Maps keep their insertion order.
                var lazyProviders = [];
                var eagerProviders = [];
                this._transformedProviders.forEach(function (provider) {
                    if (provider.eager) {
                        eagerProviders.push(provider);
                    }
                    else {
                        lazyProviders.push(provider);
                    }
                });
                return lazyProviders.concat(eagerProviders);
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(ProviderElementContext.prototype, "transformedDirectiveAsts", {
            get: function () {
                var sortedProviderTypes = this.transformProviders.map(function (provider) { return provider.token.identifier; });
                var sortedDirectives = this._directiveAsts.slice();
                sortedDirectives.sort(function (dir1, dir2) { return sortedProviderTypes.indexOf(dir1.directive.type) -
                    sortedProviderTypes.indexOf(dir2.directive.type); });
                return sortedDirectives;
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(ProviderElementContext.prototype, "queryMatches", {
            get: function () {
                var allMatches = [];
                this._queriedTokens.forEach(function (matches) { allMatches.push.apply(allMatches, tslib_1.__spread(matches)); });
                return allMatches;
            },
            enumerable: true,
            configurable: true
        });
        ProviderElementContext.prototype._addQueryReadsTo = function (token, defaultValue, queryReadTokens) {
            this._getQueriesFor(token).forEach(function (query) {
                var queryValue = query.meta.read || defaultValue;
                var tokenRef = compile_metadata_1.tokenReference(queryValue);
                var queryMatches = queryReadTokens.get(tokenRef);
                if (!queryMatches) {
                    queryMatches = [];
                    queryReadTokens.set(tokenRef, queryMatches);
                }
                queryMatches.push({ queryId: query.queryId, value: queryValue });
            });
        };
        ProviderElementContext.prototype._getQueriesFor = function (token) {
            var result = [];
            var currentEl = this;
            var distance = 0;
            var queries;
            while (currentEl !== null) {
                queries = currentEl._contentQueries.get(compile_metadata_1.tokenReference(token));
                if (queries) {
                    result.push.apply(result, tslib_1.__spread(queries.filter(function (query) { return query.meta.descendants || distance <= 1; })));
                }
                if (currentEl._directiveAsts.length > 0) {
                    distance++;
                }
                currentEl = currentEl._parent;
            }
            queries = this.viewContext.viewQueries.get(compile_metadata_1.tokenReference(token));
            if (queries) {
                result.push.apply(result, tslib_1.__spread(queries));
            }
            return result;
        };
        ProviderElementContext.prototype._getOrCreateLocalProvider = function (requestingProviderType, token, eager) {
            var _this = this;
            var resolvedProvider = this._allProviders.get(compile_metadata_1.tokenReference(token));
            if (!resolvedProvider || ((requestingProviderType === template_ast_1.ProviderAstType.Directive ||
                requestingProviderType === template_ast_1.ProviderAstType.PublicService) &&
                resolvedProvider.providerType === template_ast_1.ProviderAstType.PrivateService) ||
                ((requestingProviderType === template_ast_1.ProviderAstType.PrivateService ||
                    requestingProviderType === template_ast_1.ProviderAstType.PublicService) &&
                    resolvedProvider.providerType === template_ast_1.ProviderAstType.Builtin)) {
                return null;
            }
            var transformedProviderAst = this._transformedProviders.get(compile_metadata_1.tokenReference(token));
            if (transformedProviderAst) {
                return transformedProviderAst;
            }
            if (this._seenProviders.get(compile_metadata_1.tokenReference(token)) != null) {
                this.viewContext.errors.push(new ProviderError("Cannot instantiate cyclic dependency! " + compile_metadata_1.tokenName(token), this._sourceSpan));
                return null;
            }
            this._seenProviders.set(compile_metadata_1.tokenReference(token), true);
            var transformedProviders = resolvedProvider.providers.map(function (provider) {
                var transformedUseValue = provider.useValue;
                var transformedUseExisting = provider.useExisting;
                var transformedDeps = undefined;
                if (provider.useExisting != null) {
                    var existingDiDep = _this._getDependency(resolvedProvider.providerType, { token: provider.useExisting }, eager);
                    if (existingDiDep.token != null) {
                        transformedUseExisting = existingDiDep.token;
                    }
                    else {
                        transformedUseExisting = null;
                        transformedUseValue = existingDiDep.value;
                    }
                }
                else if (provider.useFactory) {
                    var deps = provider.deps || provider.useFactory.diDeps;
                    transformedDeps =
                        deps.map(function (dep) { return _this._getDependency(resolvedProvider.providerType, dep, eager); });
                }
                else if (provider.useClass) {
                    var deps = provider.deps || provider.useClass.diDeps;
                    transformedDeps =
                        deps.map(function (dep) { return _this._getDependency(resolvedProvider.providerType, dep, eager); });
                }
                return _transformProvider(provider, {
                    useExisting: transformedUseExisting,
                    useValue: transformedUseValue,
                    deps: transformedDeps
                });
            });
            transformedProviderAst =
                _transformProviderAst(resolvedProvider, { eager: eager, providers: transformedProviders });
            this._transformedProviders.set(compile_metadata_1.tokenReference(token), transformedProviderAst);
            return transformedProviderAst;
        };
        ProviderElementContext.prototype._getLocalDependency = function (requestingProviderType, dep, eager) {
            if (eager === void 0) { eager = false; }
            if (dep.isAttribute) {
                var attrValue = this._attrs[dep.token.value];
                return { isValue: true, value: attrValue == null ? null : attrValue };
            }
            if (dep.token != null) {
                // access builtints
                if ((requestingProviderType === template_ast_1.ProviderAstType.Directive ||
                    requestingProviderType === template_ast_1.ProviderAstType.Component)) {
                    if (compile_metadata_1.tokenReference(dep.token) ===
                        this.viewContext.reflector.resolveExternalReference(identifiers_1.Identifiers.Renderer) ||
                        compile_metadata_1.tokenReference(dep.token) ===
                            this.viewContext.reflector.resolveExternalReference(identifiers_1.Identifiers.ElementRef) ||
                        compile_metadata_1.tokenReference(dep.token) ===
                            this.viewContext.reflector.resolveExternalReference(identifiers_1.Identifiers.ChangeDetectorRef) ||
                        compile_metadata_1.tokenReference(dep.token) ===
                            this.viewContext.reflector.resolveExternalReference(identifiers_1.Identifiers.TemplateRef)) {
                        return dep;
                    }
                    if (compile_metadata_1.tokenReference(dep.token) ===
                        this.viewContext.reflector.resolveExternalReference(identifiers_1.Identifiers.ViewContainerRef)) {
                        this.transformedHasViewContainer = true;
                    }
                }
                // access the injector
                if (compile_metadata_1.tokenReference(dep.token) ===
                    this.viewContext.reflector.resolveExternalReference(identifiers_1.Identifiers.Injector)) {
                    return dep;
                }
                // access providers
                if (this._getOrCreateLocalProvider(requestingProviderType, dep.token, eager) != null) {
                    return dep;
                }
            }
            return null;
        };
        ProviderElementContext.prototype._getDependency = function (requestingProviderType, dep, eager) {
            if (eager === void 0) { eager = false; }
            var currElement = this;
            var currEager = eager;
            var result = null;
            if (!dep.isSkipSelf) {
                result = this._getLocalDependency(requestingProviderType, dep, eager);
            }
            if (dep.isSelf) {
                if (!result && dep.isOptional) {
                    result = { isValue: true, value: null };
                }
            }
            else {
                // check parent elements
                while (!result && currElement._parent) {
                    var prevElement = currElement;
                    currElement = currElement._parent;
                    if (prevElement._isViewRoot) {
                        currEager = false;
                    }
                    result = currElement._getLocalDependency(template_ast_1.ProviderAstType.PublicService, dep, currEager);
                }
                // check @Host restriction
                if (!result) {
                    if (!dep.isHost || this.viewContext.component.isHost ||
                        this.viewContext.component.type.reference === compile_metadata_1.tokenReference(dep.token) ||
                        this.viewContext.viewProviders.get(compile_metadata_1.tokenReference(dep.token)) != null) {
                        result = dep;
                    }
                    else {
                        result = dep.isOptional ? { isValue: true, value: null } : null;
                    }
                }
            }
            if (!result) {
                this.viewContext.errors.push(new ProviderError("No provider for " + compile_metadata_1.tokenName(dep.token), this._sourceSpan));
            }
            return result;
        };
        return ProviderElementContext;
    }());
    exports.ProviderElementContext = ProviderElementContext;
    var NgModuleProviderAnalyzer = /** @class */ (function () {
        function NgModuleProviderAnalyzer(reflector, ngModule, extraProviders, sourceSpan) {
            var _this = this;
            this.reflector = reflector;
            this._transformedProviders = new Map();
            this._seenProviders = new Map();
            this._errors = [];
            this._allProviders = new Map();
            ngModule.transitiveModule.modules.forEach(function (ngModuleType) {
                var ngModuleProvider = { token: { identifier: ngModuleType }, useClass: ngModuleType };
                _resolveProviders([ngModuleProvider], template_ast_1.ProviderAstType.PublicService, true, sourceSpan, _this._errors, _this._allProviders, /* isModule */ true);
            });
            _resolveProviders(ngModule.transitiveModule.providers.map(function (entry) { return entry.provider; }).concat(extraProviders), template_ast_1.ProviderAstType.PublicService, false, sourceSpan, this._errors, this._allProviders, 
            /* isModule */ false);
        }
        NgModuleProviderAnalyzer.prototype.parse = function () {
            var _this = this;
            Array.from(this._allProviders.values()).forEach(function (provider) {
                _this._getOrCreateLocalProvider(provider.token, provider.eager);
            });
            if (this._errors.length > 0) {
                var errorString = this._errors.join('\n');
                throw new Error("Provider parse errors:\n" + errorString);
            }
            // Note: Maps keep their insertion order.
            var lazyProviders = [];
            var eagerProviders = [];
            this._transformedProviders.forEach(function (provider) {
                if (provider.eager) {
                    eagerProviders.push(provider);
                }
                else {
                    lazyProviders.push(provider);
                }
            });
            return lazyProviders.concat(eagerProviders);
        };
        NgModuleProviderAnalyzer.prototype._getOrCreateLocalProvider = function (token, eager) {
            var _this = this;
            var resolvedProvider = this._allProviders.get(compile_metadata_1.tokenReference(token));
            if (!resolvedProvider) {
                return null;
            }
            var transformedProviderAst = this._transformedProviders.get(compile_metadata_1.tokenReference(token));
            if (transformedProviderAst) {
                return transformedProviderAst;
            }
            if (this._seenProviders.get(compile_metadata_1.tokenReference(token)) != null) {
                this._errors.push(new ProviderError("Cannot instantiate cyclic dependency! " + compile_metadata_1.tokenName(token), resolvedProvider.sourceSpan));
                return null;
            }
            this._seenProviders.set(compile_metadata_1.tokenReference(token), true);
            var transformedProviders = resolvedProvider.providers.map(function (provider) {
                var transformedUseValue = provider.useValue;
                var transformedUseExisting = provider.useExisting;
                var transformedDeps = undefined;
                if (provider.useExisting != null) {
                    var existingDiDep = _this._getDependency({ token: provider.useExisting }, eager, resolvedProvider.sourceSpan);
                    if (existingDiDep.token != null) {
                        transformedUseExisting = existingDiDep.token;
                    }
                    else {
                        transformedUseExisting = null;
                        transformedUseValue = existingDiDep.value;
                    }
                }
                else if (provider.useFactory) {
                    var deps = provider.deps || provider.useFactory.diDeps;
                    transformedDeps =
                        deps.map(function (dep) { return _this._getDependency(dep, eager, resolvedProvider.sourceSpan); });
                }
                else if (provider.useClass) {
                    var deps = provider.deps || provider.useClass.diDeps;
                    transformedDeps =
                        deps.map(function (dep) { return _this._getDependency(dep, eager, resolvedProvider.sourceSpan); });
                }
                return _transformProvider(provider, {
                    useExisting: transformedUseExisting,
                    useValue: transformedUseValue,
                    deps: transformedDeps
                });
            });
            transformedProviderAst =
                _transformProviderAst(resolvedProvider, { eager: eager, providers: transformedProviders });
            this._transformedProviders.set(compile_metadata_1.tokenReference(token), transformedProviderAst);
            return transformedProviderAst;
        };
        NgModuleProviderAnalyzer.prototype._getDependency = function (dep, eager, requestorSourceSpan) {
            if (eager === void 0) { eager = false; }
            var foundLocal = false;
            if (!dep.isSkipSelf && dep.token != null) {
                // access the injector
                if (compile_metadata_1.tokenReference(dep.token) ===
                    this.reflector.resolveExternalReference(identifiers_1.Identifiers.Injector) ||
                    compile_metadata_1.tokenReference(dep.token) ===
                        this.reflector.resolveExternalReference(identifiers_1.Identifiers.ComponentFactoryResolver)) {
                    foundLocal = true;
                    // access providers
                }
                else if (this._getOrCreateLocalProvider(dep.token, eager) != null) {
                    foundLocal = true;
                }
            }
            return dep;
        };
        return NgModuleProviderAnalyzer;
    }());
    exports.NgModuleProviderAnalyzer = NgModuleProviderAnalyzer;
    function _transformProvider(provider, _a) {
        var useExisting = _a.useExisting, useValue = _a.useValue, deps = _a.deps;
        return {
            token: provider.token,
            useClass: provider.useClass,
            useExisting: useExisting,
            useFactory: provider.useFactory,
            useValue: useValue,
            deps: deps,
            multi: provider.multi
        };
    }
    function _transformProviderAst(provider, _a) {
        var eager = _a.eager, providers = _a.providers;
        return new template_ast_1.ProviderAst(provider.token, provider.multiProvider, provider.eager || eager, providers, provider.providerType, provider.lifecycleHooks, provider.sourceSpan, provider.isModule);
    }
    function _resolveProvidersFromDirectives(directives, sourceSpan, targetErrors) {
        var providersByToken = new Map();
        directives.forEach(function (directive) {
            var dirProvider = { token: { identifier: directive.type }, useClass: directive.type };
            _resolveProviders([dirProvider], directive.isComponent ? template_ast_1.ProviderAstType.Component : template_ast_1.ProviderAstType.Directive, true, sourceSpan, targetErrors, providersByToken, /* isModule */ false);
        });
        // Note: directives need to be able to overwrite providers of a component!
        var directivesWithComponentFirst = directives.filter(function (dir) { return dir.isComponent; }).concat(directives.filter(function (dir) { return !dir.isComponent; }));
        directivesWithComponentFirst.forEach(function (directive) {
            _resolveProviders(directive.providers, template_ast_1.ProviderAstType.PublicService, false, sourceSpan, targetErrors, providersByToken, /* isModule */ false);
            _resolveProviders(directive.viewProviders, template_ast_1.ProviderAstType.PrivateService, false, sourceSpan, targetErrors, providersByToken, /* isModule */ false);
        });
        return providersByToken;
    }
    function _resolveProviders(providers, providerType, eager, sourceSpan, targetErrors, targetProvidersByToken, isModule) {
        providers.forEach(function (provider) {
            var resolvedProvider = targetProvidersByToken.get(compile_metadata_1.tokenReference(provider.token));
            if (resolvedProvider != null && !!resolvedProvider.multiProvider !== !!provider.multi) {
                targetErrors.push(new ProviderError("Mixing multi and non multi provider is not possible for token " + compile_metadata_1.tokenName(resolvedProvider.token), sourceSpan));
            }
            if (!resolvedProvider) {
                var lifecycleHooks = provider.token.identifier &&
                    provider.token.identifier.lifecycleHooks ?
                    provider.token.identifier.lifecycleHooks :
                    [];
                var isUseValue = !(provider.useClass || provider.useExisting || provider.useFactory);
                resolvedProvider = new template_ast_1.ProviderAst(provider.token, !!provider.multi, eager || isUseValue, [provider], providerType, lifecycleHooks, sourceSpan, isModule);
                targetProvidersByToken.set(compile_metadata_1.tokenReference(provider.token), resolvedProvider);
            }
            else {
                if (!provider.multi) {
                    resolvedProvider.providers.length = 0;
                }
                resolvedProvider.providers.push(provider);
            }
        });
    }
    function _getViewQueries(component) {
        // Note: queries start with id 1 so we can use the number in a Bloom filter!
        var viewQueryId = 1;
        var viewQueries = new Map();
        if (component.viewQueries) {
            component.viewQueries.forEach(function (query) { return _addQueryToTokenMap(viewQueries, { meta: query, queryId: viewQueryId++ }); });
        }
        return viewQueries;
    }
    function _getContentQueries(contentQueryStartId, directives) {
        var contentQueryId = contentQueryStartId;
        var contentQueries = new Map();
        directives.forEach(function (directive, directiveIndex) {
            if (directive.queries) {
                directive.queries.forEach(function (query) { return _addQueryToTokenMap(contentQueries, { meta: query, queryId: contentQueryId++ }); });
            }
        });
        return contentQueries;
    }
    function _addQueryToTokenMap(map, query) {
        query.meta.selectors.forEach(function (token) {
            var entry = map.get(compile_metadata_1.tokenReference(token));
            if (!entry) {
                entry = [];
                map.set(compile_metadata_1.tokenReference(token), entry);
            }
            entry.push(query);
        });
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJvdmlkZXJfYW5hbHl6ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvcHJvdmlkZXJfYW5hbHl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBR0gsMkVBQWdRO0lBRWhRLGlFQUEyRTtJQUMzRSwrREFBeUQ7SUFDekQsbUZBQTZIO0lBRTdIO1FBQW1DLHlDQUFVO1FBQzNDLHVCQUFZLE9BQWUsRUFBRSxJQUFxQjttQkFBSSxrQkFBTSxJQUFJLEVBQUUsT0FBTyxDQUFDO1FBQUUsQ0FBQztRQUMvRSxvQkFBQztJQUFELENBQUMsQUFGRCxDQUFtQyx1QkFBVSxHQUU1QztJQUZZLHNDQUFhO0lBUzFCO1FBV0UsNkJBQW1CLFNBQTJCLEVBQVMsU0FBbUM7WUFBMUYsaUJBUUM7WUFSa0IsY0FBUyxHQUFULFNBQVMsQ0FBa0I7WUFBUyxjQUFTLEdBQVQsU0FBUyxDQUEwQjtZQUYxRixXQUFNLEdBQW9CLEVBQUUsQ0FBQztZQUczQixJQUFJLENBQUMsV0FBVyxHQUFHLGVBQWUsQ0FBQyxTQUFTLENBQUMsQ0FBQztZQUM5QyxJQUFJLENBQUMsYUFBYSxHQUFHLElBQUksR0FBRyxFQUFnQixDQUFDO1lBQzdDLFNBQVMsQ0FBQyxhQUFhLENBQUMsT0FBTyxDQUFDLFVBQUMsUUFBUTtnQkFDdkMsSUFBSSxLQUFJLENBQUMsYUFBYSxDQUFDLEdBQUcsQ0FBQyxpQ0FBYyxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQyxJQUFJLElBQUksRUFBRTtvQkFDbEUsS0FBSSxDQUFDLGFBQWEsQ0FBQyxHQUFHLENBQUMsaUNBQWMsQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLEVBQUUsSUFBSSxDQUFDLENBQUM7aUJBQzlEO1lBQ0gsQ0FBQyxDQUFDLENBQUM7UUFDTCxDQUFDO1FBQ0gsMEJBQUM7SUFBRCxDQUFDLEFBcEJELElBb0JD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LFVBQUMsU0FBUyxFQUFFLGNBQWM7WUFDM0MsSUFBSSxTQUFTLENBQUMsT0FBTyxFQUFFO2dCQUNyQixTQUFTLENBQUMsT0FBTyxDQUFDLE9BQU8sQ0FDckIsVUFBQyxLQUFLLElBQUssT0FBQSxtQkFBbUIsQ0FBQyxjQUFjLEVBQUUsRUFBQyxJQUFJLEVBQUUsS0FBSyxFQUFFLE9BQU8sRUFBRSxjQUFjLEVBQUUsRUFBQyxDQUFDLEVBQTdFLENBQTZFLENBQUMsQ0FBQzthQUMvRjtRQUNILENBQUMsQ0FBQyxDQUFDO1FBQ0gsT0FBTyxjQUFjLENBQUM7SUFDeEIsQ0FBQztJQUVELFNBQVMsbUJBQW1CLENBQUMsR0FBNEIsRUFBRSxLQUFrQjtRQUMzRSxLQUFLLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxPQUFPLENBQUMsVUFBQyxLQUEyQjtZQUN2RCxJQUFJLEtBQUssR0FBRyxHQUFHLENBQUMsR0FBRyxDQUFDLGlDQUFjLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztZQUMzQyxJQUFJLENBQUMsS0FBSyxFQUFFO2dCQUNWLEtBQUssR0FBRyxFQUFFLENBQUM7Z0JBQ1gsR0FBRyxDQUFDLEdBQUcsQ0FBQyxpQ0FBYyxDQUFDLEtBQUssQ0FBQyxFQUFFLEtBQUssQ0FBQyxDQUFDO2FBQ3ZDO1lBQ0QsS0FBSyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUNwQixDQUFDLENBQUMsQ0FBQztJQUNMLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cblxuaW1wb3J0IHtDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGEsIENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgQ29tcGlsZURpcmVjdGl2ZVN1bW1hcnksIENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhLCBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSwgQ29tcGlsZVF1ZXJ5TWV0YWRhdGEsIENvbXBpbGVUb2tlbk1ldGFkYXRhLCBDb21waWxlVHlwZU1ldGFkYXRhLCB0b2tlbk5hbWUsIHRva2VuUmVmZXJlbmNlfSBmcm9tICcuL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuL2NvbXBpbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7SWRlbnRpZmllcnMsIGNyZWF0ZVRva2VuRm9yRXh0ZXJuYWxSZWZlcmVuY2V9IGZyb20gJy4vaWRlbnRpZmllcnMnO1xuaW1wb3J0IHtQYXJzZUVycm9yLCBQYXJzZVNvdXJjZVNwYW59IGZyb20gJy4vcGFyc2VfdXRpbCc7XG5pbXBvcnQge0F0dHJBc3QsIERpcmVjdGl2ZUFzdCwgUHJvdmlkZXJBc3QsIFByb3ZpZGVyQXN0VHlwZSwgUXVlcnlNYXRjaCwgUmVmZXJlbmNlQXN0fSBmcm9tICcuL3RlbXBsYXRlX3BhcnNlci90ZW1wbGF0ZV9hc3QnO1xuXG5leHBvcnQgY2xhc3MgUHJvdmlkZXJFcnJvciBleHRlbmRzIFBhcnNlRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihtZXNzYWdlOiBzdHJpbmcsIHNwYW46IFBhcnNlU291cmNlU3BhbikgeyBzdXBlcihzcGFuLCBtZXNzYWdlKTsgfVxufVxuXG5leHBvcnQgaW50ZXJmYWNlIFF1ZXJ5V2l0aElkIHtcbiAgbWV0YTogQ29tcGlsZVF1ZXJ5TWV0YWRhdGE7XG4gIHF1ZXJ5SWQ6IG51bWJlcjtcbn1cblxuZXhwb3J0IGNsYXNzIFByb3ZpZGVyVmlld0NvbnRleHQge1xuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICB2aWV3UXVlcmllczogTWFwPGFueSwgUXVlcnlXaXRoSWRbXT47XG4gIC8qKlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIHZpZXdQcm92aWRlcnM6IE1hcDxhbnksIGJvb2xlYW4+O1xuICBlcnJvcnM6IFByb3ZpZGVyRXJyb3JbXSA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKHB1YmxpYyByZWZsZWN0b3I6IENvbXBpbGVSZWZsZWN0b3IsIHB1YmxpYyBjb21wb25lbnQ6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSkge1xuICAgIHRoaXMudmlld1F1ZXJpZXMgPSBfZ2V0Vmlld1F1ZXJpZXMoY29tcG9uZW50KTtcbiAgICB0aGlzLnZpZXdQcm92aWRlcnMgPSBuZXcgTWFwPGFueSwgYm9vbGVhbj4oKTtcbiAgICBjb21wb25lbnQudmlld1Byb3ZpZGVycy5mb3JFYWNoKChwcm92aWRlcikgPT4ge1xuICAgICAgaWYgKHRoaXMudmlld1Byb3ZpZGVycy5nZXQodG9rZW5SZWZlcmVuY2UocHJvdmlkZXIudG9rZW4pKSA9PSBudWxsKSB7XG4gICAgICAgIHRoaXMudmlld1Byb3ZpZGVycy5zZXQodG9rZW5SZWZlcmVuY2UocHJvdmlkZXIudG9rZW4pLCB0cnVlKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgUHJvdmlkZXJFbGVtZW50Q29udGV4dCB7XG4gIHByaXZhdGUgX2NvbnRlbnRRdWVyaWVzOiBNYXA8YW55LCBRdWVyeVdpdGhJZFtdPjtcblxuICBwcml2YXRlIF90cmFuc2Zvcm1lZFByb3ZpZGVycyA9IG5ldyBNYXA8YW55LCBQcm92aWRlckFzdD4oKTtcbiAgcHJpdmF0ZSBfc2VlblByb3ZpZGVycyA9IG5ldyBNYXA8YW55LCBib29sZWFuPigpO1xuICBwcml2YXRlIF9hbGxQcm92aWRlcnM6IE1hcDxhbnksIFByb3ZpZGVyQXN0PjtcbiAgcHJpdmF0ZSBfYXR0cnM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBwcml2YXRlIF9xdWVyaWVkVG9rZW5zID0gbmV3IE1hcDxhbnksIFF1ZXJ5TWF0Y2hbXT4oKTtcblxuICBwdWJsaWMgcmVhZG9ubHkgdHJhbnNmb3JtZWRIYXNWaWV3Q29udGFpbmVyOiBib29sZWFuID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgdmlld0NvbnRleHQ6IFByb3ZpZGVyVmlld0NvbnRleHQsIHByaXZhdGUgX3BhcmVudDogUHJvdmlkZXJFbGVtZW50Q29udGV4dCxcbiAgICAgIHByaXZhdGUgX2lzVmlld1Jvb3Q6IGJvb2xlYW4sIHByaXZhdGUgX2RpcmVjdGl2ZUFzdHM6IERpcmVjdGl2ZUFzdFtdLCBhdHRyczogQXR0ckFzdFtdLFxuICAgICAgcmVmczogUmVmZXJlbmNlQXN0W10sIGlzVGVtcGxhdGU6IGJvb2xlYW4sIGNvbnRlbnRRdWVyeVN0YXJ0SWQ6IG51bWJlcixcbiAgICAgIHByaXZhdGUgX3NvdXJjZVNwYW46IFBhcnNlU291cmNlU3Bhbikge1xuICAgIHRoaXMuX2F0dHJzID0ge307XG4gICAgYXR0cnMuZm9yRWFjaCgoYXR0ckFzdCkgPT4gdGhpcy5fYXR0cnNbYXR0ckFzdC5uYW1lXSA9IGF0dHJBc3QudmFsdWUpO1xuICAgIGNvbnN0IGRpcmVjdGl2ZXNNZXRhID0gX2RpcmVjdGl2ZUFzdHMubWFwKGRpcmVjdGl2ZUFzdCA9PiBkaXJlY3RpdmVBc3QuZGlyZWN0aXZlKTtcbiAgICB0aGlzLl9hbGxQcm92aWRlcnMgPVxuICAgICAgICBfcmVzb2x2ZVByb3ZpZGVyc0Zyb21EaXJlY3RpdmVzKGRpcmVjdGl2ZXNNZXRhLCBfc291cmNlU3Bhbiwgdmlld0NvbnRleHQuZXJyb3JzKTtcbiAgICB0aGlzLl9jb250ZW50UXVlcmllcyA9IF9nZXRDb250ZW50UXVlcmllcyhjb250ZW50UXVlcnlTdGFydElkLCBkaXJlY3RpdmVzTWV0YSk7XG4gICAgQXJyYXkuZnJvbSh0aGlzLl9hbGxQcm92aWRlcnMudmFsdWVzKCkpLmZvckVhY2goKHByb3ZpZGVyKSA9PiB7XG4gICAgICB0aGlzLl9hZGRRdWVyeVJlYWRzVG8ocHJvdmlkZXIudG9rZW4sIHByb3ZpZGVyLnRva2VuLCB0aGlzLl9xdWVyaWVkVG9rZW5zKTtcbiAgICB9KTtcbiAgICBpZiAoaXNUZW1wbGF0ZSkge1xuICAgICAgY29uc3QgdGVtcGxhdGVSZWZJZCA9XG4gICAgICAgICAgY3JlYXRlVG9rZW5Gb3JFeHRlcm5hbFJlZmVyZW5jZSh0aGlzLnZpZXdDb250ZXh0LnJlZmxlY3RvciwgSWRlbnRpZmllcnMuVGVtcGxhdGVSZWYpO1xuICAgICAgdGhpcy5fYWRkUXVlcnlSZWFkc1RvKHRlbXBsYXRlUmVmSWQsIHRlbXBsYXRlUmVmSWQsIHRoaXMuX3F1ZXJpZWRUb2tlbnMpO1xuICAgIH1cbiAgICByZWZzLmZvckVhY2goKHJlZkFzdCkgPT4ge1xuICAgICAgbGV0IGRlZmF1bHRRdWVyeVZhbHVlID0gcmVmQXN0LnZhbHVlIHx8XG4gICAgICAgICAgY3JlYXRlVG9rZW5Gb3JFeHRlcm5hbFJlZmVyZW5jZSh0aGlzLnZpZXdDb250ZXh0LnJlZmxlY3RvciwgSWRlbnRpZmllcnMuRWxlbWVudFJlZik7XG4gICAgICB0aGlzLl9hZGRRdWVyeVJlYWRzVG8oe3ZhbHVlOiByZWZBc3QubmFtZX0sIGRlZmF1bHRRdWVyeVZhbHVlLCB0aGlzLl9xdWVyaWVkVG9rZW5zKTtcbiAgICB9KTtcbiAgICBpZiAodGhpcy5fcXVlcmllZFRva2Vucy5nZXQoXG4gICAgICAgICAgICB0aGlzLnZpZXdDb250ZXh0LnJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuVmlld0NvbnRhaW5lclJlZikpKSB7XG4gICAgICB0aGlzLnRyYW5zZm9ybWVkSGFzVmlld0NvbnRhaW5lciA9IHRydWU7XG4gICAgfVxuXG4gICAgLy8gY3JlYXRlIHRoZSBwcm92aWRlcnMgdGhhdCB3ZSBrbm93IGFyZSBlYWdlciBmaXJzdFxuICAgIEFycmF5LmZyb20odGhpcy5fYWxsUHJvdmlkZXJzLnZhbHVlcygpKS5mb3JFYWNoKChwcm92aWRlcikgPT4ge1xuICAgICAgY29uc3QgZWFnZXIgPSBwcm92aWRlci5lYWdlciB8fCB0aGlzLl9xdWVyaWVkVG9rZW5zLmdldCh0b2tlblJlZmVyZW5jZShwcm92aWRlci50b2tlbikpO1xuICAgICAgaWYgKGVhZ2VyKSB7XG4gICAgICAgIHRoaXMuX2dldE9yQ3JlYXRlTG9jYWxQcm92aWRlcihwcm92aWRlci5wcm92aWRlclR5cGUsIHByb3ZpZGVyLnRva2VuLCB0cnVlKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIGFmdGVyRWxlbWVudCgpIHtcbiAgICAvLyBjb2xsZWN0IGxhenkgcHJvdmlkZXJzXG4gICAgQXJyYXkuZnJvbSh0aGlzLl9hbGxQcm92aWRlcnMudmFsdWVzKCkpLmZvckVhY2goKHByb3ZpZGVyKSA9PiB7XG4gICAgICB0aGlzLl9nZXRPckNyZWF0ZUxvY2FsUHJvdmlkZXIocHJvdmlkZXIucHJvdmlkZXJUeXBlLCBwcm92aWRlci50b2tlbiwgZmFsc2UpO1xuICAgIH0pO1xuICB9XG5cbiAgZ2V0IHRyYW5zZm9ybVByb3ZpZGVycygpOiBQcm92aWRlckFzdFtdIHtcbiAgICAvLyBOb3RlOiBNYXBzIGtlZXAgdGhlaXIgaW5zZXJ0aW9uIG9yZGVyLlxuICAgIGNvbnN0IGxhenlQcm92aWRlcnM6IFByb3ZpZGVyQXN0W10gPSBbXTtcbiAgICBjb25zdCBlYWdlclByb3ZpZGVyczogUHJvdmlkZXJBc3RbXSA9IFtdO1xuICAgIHRoaXMuX3RyYW5zZm9ybWVkUHJvdmlkZXJzLmZvckVhY2gocHJvdmlkZXIgPT4ge1xuICAgICAgaWYgKHByb3ZpZGVyLmVhZ2VyKSB7XG4gICAgICAgIGVhZ2VyUHJvdmlkZXJzLnB1c2gocHJvdmlkZXIpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbGF6eVByb3ZpZGVycy5wdXNoKHByb3ZpZGVyKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gbGF6eVByb3ZpZGVycy5jb25jYXQoZWFnZXJQcm92aWRlcnMpO1xuICB9XG5cbiAgZ2V0IHRyYW5zZm9ybWVkRGlyZWN0aXZlQXN0cygpOiBEaXJlY3RpdmVBc3RbXSB7XG4gICAgY29uc3Qgc29ydGVkUHJvdmlkZXJUeXBlcyA9IHRoaXMudHJhbnNmb3JtUHJvdmlkZXJzLm1hcChwcm92aWRlciA9PiBwcm92aWRlci50b2tlbi5pZGVudGlmaWVyKTtcbiAgICBjb25zdCBzb3J0ZWREaXJlY3RpdmVzID0gdGhpcy5fZGlyZWN0aXZlQXN0cy5zbGljZSgpO1xuICAgIHNvcnRlZERpcmVjdGl2ZXMuc29ydChcbiAgICAgICAgKGRpcjEsIGRpcjIpID0+IHNvcnRlZFByb3ZpZGVyVHlwZXMuaW5kZXhPZihkaXIxLmRpcmVjdGl2ZS50eXBlKSAtXG4gICAgICAgICAgICBzb3J0ZWRQcm92aWRlclR5cGVzLmluZGV4T2YoZGlyMi5kaXJlY3RpdmUudHlwZSkpO1xuICAgIHJldHVybiBzb3J0ZWREaXJlY3RpdmVzO1xuICB9XG5cbiAgZ2V0IHF1ZXJ5TWF0Y2hlcygpOiBRdWVyeU1hdGNoW10ge1xuICAgIGNvbnN0IGFsbE1hdGNoZXM6IFF1ZXJ5TWF0Y2hbXSA9IFtdO1xuICAgIHRoaXMuX3F1ZXJpZWRUb2tlbnMuZm9yRWFjaCgobWF0Y2hlczogUXVlcnlNYXRjaFtdKSA9PiB7IGFsbE1hdGNoZXMucHVzaCguLi5tYXRjaGVzKTsgfSk7XG4gICAgcmV0dXJuIGFsbE1hdGNoZXM7XG4gIH1cblxuICBwcml2YXRlIF9hZGRRdWVyeVJlYWRzVG8oXG4gICAgICB0b2tlbjogQ29tcGlsZVRva2VuTWV0YWRhdGEsIGRlZmF1bHRWYWx1ZTogQ29tcGlsZVRva2VuTWV0YWRhdGEsXG4gICAgICBxdWVyeVJlYWRUb2tlbnM6IE1hcDxhbnksIFF1ZXJ5TWF0Y2hbXT4pIHtcbiAgICB0aGlzLl9nZXRRdWVyaWVzRm9yKHRva2VuKS5mb3JFYWNoKChxdWVyeSkgPT4ge1xuICAgICAgY29uc3QgcXVlcnlWYWx1ZSA9IHF1ZXJ5Lm1ldGEucmVhZCB8fCBkZWZhdWx0VmFsdWU7XG4gICAgICBjb25zdCB0b2tlblJlZiA9IHRva2VuUmVmZXJlbmNlKHF1ZXJ5VmFsdWUpO1xuICAgICAgbGV0IHF1ZXJ5TWF0Y2hlcyA9IHF1ZXJ5UmVhZFRva2Vucy5nZXQodG9rZW5SZWYpO1xuICAgICAgaWYgKCFxdWVyeU1hdGNoZXMpIHtcbiAgICAgICAgcXVlcnlNYXRjaGVzID0gW107XG4gICAgICAgIHF1ZXJ5UmVhZFRva2Vucy5zZXQodG9rZW5SZWYsIHF1ZXJ5TWF0Y2hlcyk7XG4gICAgICB9XG4gICAgICBxdWVyeU1hdGNoZXMucHVzaCh7cXVlcnlJZDogcXVlcnkucXVlcnlJZCwgdmFsdWU6IHF1ZXJ5VmFsdWV9KTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgX2dldFF1ZXJpZXNGb3IodG9rZW46IENvbXBpbGVUb2tlbk1ldGFkYXRhKTogUXVlcnlXaXRoSWRbXSB7XG4gICAgY29uc3QgcmVzdWx0OiBRdWVyeVdpdGhJZFtdID0gW107XG4gICAgbGV0IGN1cnJlbnRFbDogUHJvdmlkZXJFbGVtZW50Q29udGV4dCA9IHRoaXM7XG4gICAgbGV0IGRpc3RhbmNlID0gMDtcbiAgICBsZXQgcXVlcmllczogUXVlcnlXaXRoSWRbXXx1bmRlZmluZWQ7XG4gICAgd2hpbGUgKGN1cnJlbnRFbCAhPT0gbnVsbCkge1xuICAgICAgcXVlcmllcyA9IGN1cnJlbnRFbC5fY29udGVudFF1ZXJpZXMuZ2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSk7XG4gICAgICBpZiAocXVlcmllcykge1xuICAgICAgICByZXN1bHQucHVzaCguLi5xdWVyaWVzLmZpbHRlcigocXVlcnkpID0+IHF1ZXJ5Lm1ldGEuZGVzY2VuZGFudHMgfHwgZGlzdGFuY2UgPD0gMSkpO1xuICAgICAgfVxuICAgICAgaWYgKGN1cnJlbnRFbC5fZGlyZWN0aXZlQXN0cy5sZW5ndGggPiAwKSB7XG4gICAgICAgIGRpc3RhbmNlKys7XG4gICAgICB9XG4gICAgICBjdXJyZW50RWwgPSBjdXJyZW50RWwuX3BhcmVudDtcbiAgICB9XG4gICAgcXVlcmllcyA9IHRoaXMudmlld0NvbnRleHQudmlld1F1ZXJpZXMuZ2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSk7XG4gICAgaWYgKHF1ZXJpZXMpIHtcbiAgICAgIHJlc3VsdC5wdXNoKC4uLnF1ZXJpZXMpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cblxuICBwcml2YXRlIF9nZXRPckNyZWF0ZUxvY2FsUHJvdmlkZXIoXG4gICAgICByZXF1ZXN0aW5nUHJvdmlkZXJUeXBlOiBQcm92aWRlckFzdFR5cGUsIHRva2VuOiBDb21waWxlVG9rZW5NZXRhZGF0YSxcbiAgICAgIGVhZ2VyOiBib29sZWFuKTogUHJvdmlkZXJBc3R8bnVsbCB7XG4gICAgY29uc3QgcmVzb2x2ZWRQcm92aWRlciA9IHRoaXMuX2FsbFByb3ZpZGVycy5nZXQodG9rZW5SZWZlcmVuY2UodG9rZW4pKTtcbiAgICBpZiAoIXJlc29sdmVkUHJvdmlkZXIgfHwgKChyZXF1ZXN0aW5nUHJvdmlkZXJUeXBlID09PSBQcm92aWRlckFzdFR5cGUuRGlyZWN0aXZlIHx8XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVxdWVzdGluZ1Byb3ZpZGVyVHlwZSA9PT0gUHJvdmlkZXJBc3RUeXBlLlB1YmxpY1NlcnZpY2UpICYmXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXNvbHZlZFByb3ZpZGVyLnByb3ZpZGVyVHlwZSA9PT0gUHJvdmlkZXJBc3RUeXBlLlByaXZhdGVTZXJ2aWNlKSB8fFxuICAgICAgICAoKHJlcXVlc3RpbmdQcm92aWRlclR5cGUgPT09IFByb3ZpZGVyQXN0VHlwZS5Qcml2YXRlU2VydmljZSB8fFxuICAgICAgICAgIHJlcXVlc3RpbmdQcm92aWRlclR5cGUgPT09IFByb3ZpZGVyQXN0VHlwZS5QdWJsaWNTZXJ2aWNlKSAmJlxuICAgICAgICAgcmVzb2x2ZWRQcm92aWRlci5wcm92aWRlclR5cGUgPT09IFByb3ZpZGVyQXN0VHlwZS5CdWlsdGluKSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGxldCB0cmFuc2Zvcm1lZFByb3ZpZGVyQXN0ID0gdGhpcy5fdHJhbnNmb3JtZWRQcm92aWRlcnMuZ2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSk7XG4gICAgaWYgKHRyYW5zZm9ybWVkUHJvdmlkZXJBc3QpIHtcbiAgICAgIHJldHVybiB0cmFuc2Zvcm1lZFByb3ZpZGVyQXN0O1xuICAgIH1cbiAgICBpZiAodGhpcy5fc2VlblByb3ZpZGVycy5nZXQodG9rZW5SZWZlcmVuY2UodG9rZW4pKSAhPSBudWxsKSB7XG4gICAgICB0aGlzLnZpZXdDb250ZXh0LmVycm9ycy5wdXNoKG5ldyBQcm92aWRlckVycm9yKFxuICAgICAgICAgIGBDYW5ub3QgaW5zdGFudGlhdGUgY3ljbGljIGRlcGVuZGVuY3khICR7dG9rZW5OYW1lKHRva2VuKX1gLCB0aGlzLl9zb3VyY2VTcGFuKSk7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgdGhpcy5fc2VlblByb3ZpZGVycy5zZXQodG9rZW5SZWZlcmVuY2UodG9rZW4pLCB0cnVlKTtcbiAgICBjb25zdCB0cmFuc2Zvcm1lZFByb3ZpZGVycyA9IHJlc29sdmVkUHJvdmlkZXIucHJvdmlkZXJzLm1hcCgocHJvdmlkZXIpID0+IHtcbiAgICAgIGxldCB0cmFuc2Zvcm1lZFVzZVZhbHVlID0gcHJvdmlkZXIudXNlVmFsdWU7XG4gICAgICBsZXQgdHJhbnNmb3JtZWRVc2VFeGlzdGluZyA9IHByb3ZpZGVyLnVzZUV4aXN0aW5nICE7XG4gICAgICBsZXQgdHJhbnNmb3JtZWREZXBzOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGFbXSA9IHVuZGVmaW5lZCAhO1xuICAgICAgaWYgKHByb3ZpZGVyLnVzZUV4aXN0aW5nICE9IG51bGwpIHtcbiAgICAgICAgY29uc3QgZXhpc3RpbmdEaURlcCA9IHRoaXMuX2dldERlcGVuZGVuY3koXG4gICAgICAgICAgICByZXNvbHZlZFByb3ZpZGVyLnByb3ZpZGVyVHlwZSwge3Rva2VuOiBwcm92aWRlci51c2VFeGlzdGluZ30sIGVhZ2VyKSAhO1xuICAgICAgICBpZiAoZXhpc3RpbmdEaURlcC50b2tlbiAhPSBudWxsKSB7XG4gICAgICAgICAgdHJhbnNmb3JtZWRVc2VFeGlzdGluZyA9IGV4aXN0aW5nRGlEZXAudG9rZW47XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdHJhbnNmb3JtZWRVc2VFeGlzdGluZyA9IG51bGwgITtcbiAgICAgICAgICB0cmFuc2Zvcm1lZFVzZVZhbHVlID0gZXhpc3RpbmdEaURlcC52YWx1ZTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIGlmIChwcm92aWRlci51c2VGYWN0b3J5KSB7XG4gICAgICAgIGNvbnN0IGRlcHMgPSBwcm92aWRlci5kZXBzIHx8IHByb3ZpZGVyLnVzZUZhY3RvcnkuZGlEZXBzO1xuICAgICAgICB0cmFuc2Zvcm1lZERlcHMgPVxuICAgICAgICAgICAgZGVwcy5tYXAoKGRlcCkgPT4gdGhpcy5fZ2V0RGVwZW5kZW5jeShyZXNvbHZlZFByb3ZpZGVyLnByb3ZpZGVyVHlwZSwgZGVwLCBlYWdlcikgISk7XG4gICAgICB9IGVsc2UgaWYgKHByb3ZpZGVyLnVzZUNsYXNzKSB7XG4gICAgICAgIGNvbnN0IGRlcHMgPSBwcm92aWRlci5kZXBzIHx8IHByb3ZpZGVyLnVzZUNsYXNzLmRpRGVwcztcbiAgICAgICAgdHJhbnNmb3JtZWREZXBzID1cbiAgICAgICAgICAgIGRlcHMubWFwKChkZXApID0+IHRoaXMuX2dldERlcGVuZGVuY3kocmVzb2x2ZWRQcm92aWRlci5wcm92aWRlclR5cGUsIGRlcCwgZWFnZXIpICEpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIF90cmFuc2Zvcm1Qcm92aWRlcihwcm92aWRlciwge1xuICAgICAgICB1c2VFeGlzdGluZzogdHJhbnNmb3JtZWRVc2VFeGlzdGluZyxcbiAgICAgICAgdXNlVmFsdWU6IHRyYW5zZm9ybWVkVXNlVmFsdWUsXG4gICAgICAgIGRlcHM6IHRyYW5zZm9ybWVkRGVwc1xuICAgICAgfSk7XG4gICAgfSk7XG4gICAgdHJhbnNmb3JtZWRQcm92aWRlckFzdCA9XG4gICAgICAgIF90cmFuc2Zvcm1Qcm92aWRlckFzdChyZXNvbHZlZFByb3ZpZGVyLCB7ZWFnZXI6IGVhZ2VyLCBwcm92aWRlcnM6IHRyYW5zZm9ybWVkUHJvdmlkZXJzfSk7XG4gICAgdGhpcy5fdHJhbnNmb3JtZWRQcm92aWRlcnMuc2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSwgdHJhbnNmb3JtZWRQcm92aWRlckFzdCk7XG4gICAgcmV0dXJuIHRyYW5zZm9ybWVkUHJvdmlkZXJBc3Q7XG4gIH1cblxuICBwcml2YXRlIF9nZXRMb2NhbERlcGVuZGVuY3koXG4gICAgICByZXF1ZXN0aW5nUHJvdmlkZXJUeXBlOiBQcm92aWRlckFzdFR5cGUsIGRlcDogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhLFxuICAgICAgZWFnZXI6IGJvb2xlYW4gPSBmYWxzZSk6IENvbXBpbGVEaURlcGVuZGVuY3lNZXRhZGF0YXxudWxsIHtcbiAgICBpZiAoZGVwLmlzQXR0cmlidXRlKSB7XG4gICAgICBjb25zdCBhdHRyVmFsdWUgPSB0aGlzLl9hdHRyc1tkZXAudG9rZW4gIS52YWx1ZV07XG4gICAgICByZXR1cm4ge2lzVmFsdWU6IHRydWUsIHZhbHVlOiBhdHRyVmFsdWUgPT0gbnVsbCA/IG51bGwgOiBhdHRyVmFsdWV9O1xuICAgIH1cblxuICAgIGlmIChkZXAudG9rZW4gIT0gbnVsbCkge1xuICAgICAgLy8gYWNjZXNzIGJ1aWx0aW50c1xuICAgICAgaWYgKChyZXF1ZXN0aW5nUHJvdmlkZXJUeXBlID09PSBQcm92aWRlckFzdFR5cGUuRGlyZWN0aXZlIHx8XG4gICAgICAgICAgIHJlcXVlc3RpbmdQcm92aWRlclR5cGUgPT09IFByb3ZpZGVyQXN0VHlwZS5Db21wb25lbnQpKSB7XG4gICAgICAgIGlmICh0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgICAgICAgIHRoaXMudmlld0NvbnRleHQucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5SZW5kZXJlcikgfHxcbiAgICAgICAgICAgIHRva2VuUmVmZXJlbmNlKGRlcC50b2tlbikgPT09XG4gICAgICAgICAgICAgICAgdGhpcy52aWV3Q29udGV4dC5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKElkZW50aWZpZXJzLkVsZW1lbnRSZWYpIHx8XG4gICAgICAgICAgICB0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgICAgICAgIHRoaXMudmlld0NvbnRleHQucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShcbiAgICAgICAgICAgICAgICAgICAgSWRlbnRpZmllcnMuQ2hhbmdlRGV0ZWN0b3JSZWYpIHx8XG4gICAgICAgICAgICB0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgICAgICAgIHRoaXMudmlld0NvbnRleHQucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5UZW1wbGF0ZVJlZikpIHtcbiAgICAgICAgICByZXR1cm4gZGVwO1xuICAgICAgICB9XG4gICAgICAgIGlmICh0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgICAgdGhpcy52aWV3Q29udGV4dC5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKElkZW50aWZpZXJzLlZpZXdDb250YWluZXJSZWYpKSB7XG4gICAgICAgICAgKHRoaXMgYXN7dHJhbnNmb3JtZWRIYXNWaWV3Q29udGFpbmVyOiBib29sZWFufSkudHJhbnNmb3JtZWRIYXNWaWV3Q29udGFpbmVyID0gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgLy8gYWNjZXNzIHRoZSBpbmplY3RvclxuICAgICAgaWYgKHRva2VuUmVmZXJlbmNlKGRlcC50b2tlbikgPT09XG4gICAgICAgICAgdGhpcy52aWV3Q29udGV4dC5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKElkZW50aWZpZXJzLkluamVjdG9yKSkge1xuICAgICAgICByZXR1cm4gZGVwO1xuICAgICAgfVxuICAgICAgLy8gYWNjZXNzIHByb3ZpZGVyc1xuICAgICAgaWYgKHRoaXMuX2dldE9yQ3JlYXRlTG9jYWxQcm92aWRlcihyZXF1ZXN0aW5nUHJvdmlkZXJUeXBlLCBkZXAudG9rZW4sIGVhZ2VyKSAhPSBudWxsKSB7XG4gICAgICAgIHJldHVybiBkZXA7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0RGVwZW5kZW5jeShcbiAgICAgIHJlcXVlc3RpbmdQcm92aWRlclR5cGU6IFByb3ZpZGVyQXN0VHlwZSwgZGVwOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGEsXG4gICAgICBlYWdlcjogYm9vbGVhbiA9IGZhbHNlKTogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhfG51bGwge1xuICAgIGxldCBjdXJyRWxlbWVudDogUHJvdmlkZXJFbGVtZW50Q29udGV4dCA9IHRoaXM7XG4gICAgbGV0IGN1cnJFYWdlcjogYm9vbGVhbiA9IGVhZ2VyO1xuICAgIGxldCByZXN1bHQ6IENvbXBpbGVEaURlcGVuZGVuY3lNZXRhZGF0YXxudWxsID0gbnVsbDtcbiAgICBpZiAoIWRlcC5pc1NraXBTZWxmKSB7XG4gICAgICByZXN1bHQgPSB0aGlzLl9nZXRMb2NhbERlcGVuZGVuY3kocmVxdWVzdGluZ1Byb3ZpZGVyVHlwZSwgZGVwLCBlYWdlcik7XG4gICAgfVxuICAgIGlmIChkZXAuaXNTZWxmKSB7XG4gICAgICBpZiAoIXJlc3VsdCAmJiBkZXAuaXNPcHRpb25hbCkge1xuICAgICAgICByZXN1bHQgPSB7aXNWYWx1ZTogdHJ1ZSwgdmFsdWU6IG51bGx9O1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICAvLyBjaGVjayBwYXJlbnQgZWxlbWVudHNcbiAgICAgIHdoaWxlICghcmVzdWx0ICYmIGN1cnJFbGVtZW50Ll9wYXJlbnQpIHtcbiAgICAgICAgY29uc3QgcHJldkVsZW1lbnQgPSBjdXJyRWxlbWVudDtcbiAgICAgICAgY3VyckVsZW1lbnQgPSBjdXJyRWxlbWVudC5fcGFyZW50O1xuICAgICAgICBpZiAocHJldkVsZW1lbnQuX2lzVmlld1Jvb3QpIHtcbiAgICAgICAgICBjdXJyRWFnZXIgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICByZXN1bHQgPSBjdXJyRWxlbWVudC5fZ2V0TG9jYWxEZXBlbmRlbmN5KFByb3ZpZGVyQXN0VHlwZS5QdWJsaWNTZXJ2aWNlLCBkZXAsIGN1cnJFYWdlcik7XG4gICAgICB9XG4gICAgICAvLyBjaGVjayBASG9zdCByZXN0cmljdGlvblxuICAgICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgICAgaWYgKCFkZXAuaXNIb3N0IHx8IHRoaXMudmlld0NvbnRleHQuY29tcG9uZW50LmlzSG9zdCB8fFxuICAgICAgICAgICAgdGhpcy52aWV3Q29udGV4dC5jb21wb25lbnQudHlwZS5yZWZlcmVuY2UgPT09IHRva2VuUmVmZXJlbmNlKGRlcC50b2tlbiAhKSB8fFxuICAgICAgICAgICAgdGhpcy52aWV3Q29udGV4dC52aWV3UHJvdmlkZXJzLmdldCh0b2tlblJlZmVyZW5jZShkZXAudG9rZW4gISkpICE9IG51bGwpIHtcbiAgICAgICAgICByZXN1bHQgPSBkZXA7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmVzdWx0ID0gZGVwLmlzT3B0aW9uYWwgPyB7aXNWYWx1ZTogdHJ1ZSwgdmFsdWU6IG51bGx9IDogbnVsbDtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgICBpZiAoIXJlc3VsdCkge1xuICAgICAgdGhpcy52aWV3Q29udGV4dC5lcnJvcnMucHVzaChcbiAgICAgICAgICBuZXcgUHJvdmlkZXJFcnJvcihgTm8gcHJvdmlkZXIgZm9yICR7dG9rZW5OYW1lKGRlcC50b2tlbiEpfWAsIHRoaXMuX3NvdXJjZVNwYW4pKTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxufVxuXG5cbmV4cG9ydCBjbGFzcyBOZ01vZHVsZVByb3ZpZGVyQW5hbHl6ZXIge1xuICBwcml2YXRlIF90cmFuc2Zvcm1lZFByb3ZpZGVycyA9IG5ldyBNYXA8YW55LCBQcm92aWRlckFzdD4oKTtcbiAgcHJpdmF0ZSBfc2VlblByb3ZpZGVycyA9IG5ldyBNYXA8YW55LCBib29sZWFuPigpO1xuICBwcml2YXRlIF9hbGxQcm92aWRlcnM6IE1hcDxhbnksIFByb3ZpZGVyQXN0PjtcbiAgcHJpdmF0ZSBfZXJyb3JzOiBQcm92aWRlckVycm9yW10gPSBbXTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yLCBuZ01vZHVsZTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsXG4gICAgICBleHRyYVByb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSwgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuKSB7XG4gICAgdGhpcy5fYWxsUHJvdmlkZXJzID0gbmV3IE1hcDxhbnksIFByb3ZpZGVyQXN0PigpO1xuICAgIG5nTW9kdWxlLnRyYW5zaXRpdmVNb2R1bGUubW9kdWxlcy5mb3JFYWNoKChuZ01vZHVsZVR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGEpID0+IHtcbiAgICAgIGNvbnN0IG5nTW9kdWxlUHJvdmlkZXIgPSB7dG9rZW46IHtpZGVudGlmaWVyOiBuZ01vZHVsZVR5cGV9LCB1c2VDbGFzczogbmdNb2R1bGVUeXBlfTtcbiAgICAgIF9yZXNvbHZlUHJvdmlkZXJzKFxuICAgICAgICAgIFtuZ01vZHVsZVByb3ZpZGVyXSwgUHJvdmlkZXJBc3RUeXBlLlB1YmxpY1NlcnZpY2UsIHRydWUsIHNvdXJjZVNwYW4sIHRoaXMuX2Vycm9ycyxcbiAgICAgICAgICB0aGlzLl9hbGxQcm92aWRlcnMsIC8qIGlzTW9kdWxlICovIHRydWUpO1xuICAgIH0pO1xuICAgIF9yZXNvbHZlUHJvdmlkZXJzKFxuICAgICAgICBuZ01vZHVsZS50cmFuc2l0aXZlTW9kdWxlLnByb3ZpZGVycy5tYXAoZW50cnkgPT4gZW50cnkucHJvdmlkZXIpLmNvbmNhdChleHRyYVByb3ZpZGVycyksXG4gICAgICAgIFByb3ZpZGVyQXN0VHlwZS5QdWJsaWNTZXJ2aWNlLCBmYWxzZSwgc291cmNlU3BhbiwgdGhpcy5fZXJyb3JzLCB0aGlzLl9hbGxQcm92aWRlcnMsXG4gICAgICAgIC8qIGlzTW9kdWxlICovIGZhbHNlKTtcbiAgfVxuXG4gIHBhcnNlKCk6IFByb3ZpZGVyQXN0W10ge1xuICAgIEFycmF5LmZyb20odGhpcy5fYWxsUHJvdmlkZXJzLnZhbHVlcygpKS5mb3JFYWNoKChwcm92aWRlcikgPT4ge1xuICAgICAgdGhpcy5fZ2V0T3JDcmVhdGVMb2NhbFByb3ZpZGVyKHByb3ZpZGVyLnRva2VuLCBwcm92aWRlci5lYWdlcik7XG4gICAgfSk7XG4gICAgaWYgKHRoaXMuX2Vycm9ycy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zdCBlcnJvclN0cmluZyA9IHRoaXMuX2Vycm9ycy5qb2luKCdcXG4nKTtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgUHJvdmlkZXIgcGFyc2UgZXJyb3JzOlxcbiR7ZXJyb3JTdHJpbmd9YCk7XG4gICAgfVxuICAgIC8vIE5vdGU6IE1hcHMga2VlcCB0aGVpciBpbnNlcnRpb24gb3JkZXIuXG4gICAgY29uc3QgbGF6eVByb3ZpZGVyczogUHJvdmlkZXJBc3RbXSA9IFtdO1xuICAgIGNvbnN0IGVhZ2VyUHJvdmlkZXJzOiBQcm92aWRlckFzdFtdID0gW107XG4gICAgdGhpcy5fdHJhbnNmb3JtZWRQcm92aWRlcnMuZm9yRWFjaChwcm92aWRlciA9PiB7XG4gICAgICBpZiAocHJvdmlkZXIuZWFnZXIpIHtcbiAgICAgICAgZWFnZXJQcm92aWRlcnMucHVzaChwcm92aWRlcik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBsYXp5UHJvdmlkZXJzLnB1c2gocHJvdmlkZXIpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIHJldHVybiBsYXp5UHJvdmlkZXJzLmNvbmNhdChlYWdlclByb3ZpZGVycyk7XG4gIH1cblxuICBwcml2YXRlIF9nZXRPckNyZWF0ZUxvY2FsUHJvdmlkZXIodG9rZW46IENvbXBpbGVUb2tlbk1ldGFkYXRhLCBlYWdlcjogYm9vbGVhbik6IFByb3ZpZGVyQXN0fG51bGwge1xuICAgIGNvbnN0IHJlc29sdmVkUHJvdmlkZXIgPSB0aGlzLl9hbGxQcm92aWRlcnMuZ2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSk7XG4gICAgaWYgKCFyZXNvbHZlZFByb3ZpZGVyKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgbGV0IHRyYW5zZm9ybWVkUHJvdmlkZXJBc3QgPSB0aGlzLl90cmFuc2Zvcm1lZFByb3ZpZGVycy5nZXQodG9rZW5SZWZlcmVuY2UodG9rZW4pKTtcbiAgICBpZiAodHJhbnNmb3JtZWRQcm92aWRlckFzdCkge1xuICAgICAgcmV0dXJuIHRyYW5zZm9ybWVkUHJvdmlkZXJBc3Q7XG4gICAgfVxuICAgIGlmICh0aGlzLl9zZWVuUHJvdmlkZXJzLmdldCh0b2tlblJlZmVyZW5jZSh0b2tlbikpICE9IG51bGwpIHtcbiAgICAgIHRoaXMuX2Vycm9ycy5wdXNoKG5ldyBQcm92aWRlckVycm9yKFxuICAgICAgICAgIGBDYW5ub3QgaW5zdGFudGlhdGUgY3ljbGljIGRlcGVuZGVuY3khICR7dG9rZW5OYW1lKHRva2VuKX1gLFxuICAgICAgICAgIHJlc29sdmVkUHJvdmlkZXIuc291cmNlU3BhbikpO1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIHRoaXMuX3NlZW5Qcm92aWRlcnMuc2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSwgdHJ1ZSk7XG4gICAgY29uc3QgdHJhbnNmb3JtZWRQcm92aWRlcnMgPSByZXNvbHZlZFByb3ZpZGVyLnByb3ZpZGVycy5tYXAoKHByb3ZpZGVyKSA9PiB7XG4gICAgICBsZXQgdHJhbnNmb3JtZWRVc2VWYWx1ZSA9IHByb3ZpZGVyLnVzZVZhbHVlO1xuICAgICAgbGV0IHRyYW5zZm9ybWVkVXNlRXhpc3RpbmcgPSBwcm92aWRlci51c2VFeGlzdGluZyAhO1xuICAgICAgbGV0IHRyYW5zZm9ybWVkRGVwczogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhW10gPSB1bmRlZmluZWQgITtcbiAgICAgIGlmIChwcm92aWRlci51c2VFeGlzdGluZyAhPSBudWxsKSB7XG4gICAgICAgIGNvbnN0IGV4aXN0aW5nRGlEZXAgPVxuICAgICAgICAgICAgdGhpcy5fZ2V0RGVwZW5kZW5jeSh7dG9rZW46IHByb3ZpZGVyLnVzZUV4aXN0aW5nfSwgZWFnZXIsIHJlc29sdmVkUHJvdmlkZXIuc291cmNlU3Bhbik7XG4gICAgICAgIGlmIChleGlzdGluZ0RpRGVwLnRva2VuICE9IG51bGwpIHtcbiAgICAgICAgICB0cmFuc2Zvcm1lZFVzZUV4aXN0aW5nID0gZXhpc3RpbmdEaURlcC50b2tlbjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0cmFuc2Zvcm1lZFVzZUV4aXN0aW5nID0gbnVsbCAhO1xuICAgICAgICAgIHRyYW5zZm9ybWVkVXNlVmFsdWUgPSBleGlzdGluZ0RpRGVwLnZhbHVlO1xuICAgICAgICB9XG4gICAgICB9IGVsc2UgaWYgKHByb3ZpZGVyLnVzZUZhY3RvcnkpIHtcbiAgICAgICAgY29uc3QgZGVwcyA9IHByb3ZpZGVyLmRlcHMgfHwgcHJvdmlkZXIudXNlRmFjdG9yeS5kaURlcHM7XG4gICAgICAgIHRyYW5zZm9ybWVkRGVwcyA9XG4gICAgICAgICAgICBkZXBzLm1hcCgoZGVwKSA9PiB0aGlzLl9nZXREZXBlbmRlbmN5KGRlcCwgZWFnZXIsIHJlc29sdmVkUHJvdmlkZXIuc291cmNlU3BhbikpO1xuICAgICAgfSBlbHNlIGlmIChwcm92aWRlci51c2VDbGFzcykge1xuICAgICAgICBjb25zdCBkZXBzID0gcHJvdmlkZXIuZGVwcyB8fCBwcm92aWRlci51c2VDbGFzcy5kaURlcHM7XG4gICAgICAgIHRyYW5zZm9ybWVkRGVwcyA9XG4gICAgICAgICAgICBkZXBzLm1hcCgoZGVwKSA9PiB0aGlzLl9nZXREZXBlbmRlbmN5KGRlcCwgZWFnZXIsIHJlc29sdmVkUHJvdmlkZXIuc291cmNlU3BhbikpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIF90cmFuc2Zvcm1Qcm92aWRlcihwcm92aWRlciwge1xuICAgICAgICB1c2VFeGlzdGluZzogdHJhbnNmb3JtZWRVc2VFeGlzdGluZyxcbiAgICAgICAgdXNlVmFsdWU6IHRyYW5zZm9ybWVkVXNlVmFsdWUsXG4gICAgICAgIGRlcHM6IHRyYW5zZm9ybWVkRGVwc1xuICAgICAgfSk7XG4gICAgfSk7XG4gICAgdHJhbnNmb3JtZWRQcm92aWRlckFzdCA9XG4gICAgICAgIF90cmFuc2Zvcm1Qcm92aWRlckFzdChyZXNvbHZlZFByb3ZpZGVyLCB7ZWFnZXI6IGVhZ2VyLCBwcm92aWRlcnM6IHRyYW5zZm9ybWVkUHJvdmlkZXJzfSk7XG4gICAgdGhpcy5fdHJhbnNmb3JtZWRQcm92aWRlcnMuc2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSwgdHJhbnNmb3JtZWRQcm92aWRlckFzdCk7XG4gICAgcmV0dXJuIHRyYW5zZm9ybWVkUHJvdmlkZXJBc3Q7XG4gIH1cblxuICBwcml2YXRlIF9nZXREZXBlbmRlbmN5KFxuICAgICAgZGVwOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGEsIGVhZ2VyOiBib29sZWFuID0gZmFsc2UsXG4gICAgICByZXF1ZXN0b3JTb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGEge1xuICAgIGxldCBmb3VuZExvY2FsID0gZmFsc2U7XG4gICAgaWYgKCFkZXAuaXNTa2lwU2VsZiAmJiBkZXAudG9rZW4gIT0gbnVsbCkge1xuICAgICAgLy8gYWNjZXNzIHRoZSBpbmplY3RvclxuICAgICAgaWYgKHRva2VuUmVmZXJlbmNlKGRlcC50b2tlbikgPT09XG4gICAgICAgICAgICAgIHRoaXMucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5JbmplY3RvcikgfHxcbiAgICAgICAgICB0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgICAgICB0aGlzLnJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyKSkge1xuICAgICAgICBmb3VuZExvY2FsID0gdHJ1ZTtcbiAgICAgICAgLy8gYWNjZXNzIHByb3ZpZGVyc1xuICAgICAgfSBlbHNlIGlmICh0aGlzLl9nZXRPckNyZWF0ZUxvY2FsUHJvdmlkZXIoZGVwLnRva2VuLCBlYWdlcikgIT0gbnVsbCkge1xuICAgICAgICBmb3VuZExvY2FsID0gdHJ1ZTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIGRlcDtcbiAgfVxufVxuXG5mdW5jdGlvbiBfdHJhbnNmb3JtUHJvdmlkZXIoXG4gICAgcHJvdmlkZXI6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhLFxuICAgIHt1c2VFeGlzdGluZywgdXNlVmFsdWUsIGRlcHN9OlxuICAgICAgICB7dXNlRXhpc3Rpbmc6IENvbXBpbGVUb2tlbk1ldGFkYXRhLCB1c2VWYWx1ZTogYW55LCBkZXBzOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGFbXX0pIHtcbiAgcmV0dXJuIHtcbiAgICB0b2tlbjogcHJvdmlkZXIudG9rZW4sXG4gICAgdXNlQ2xhc3M6IHByb3ZpZGVyLnVzZUNsYXNzLFxuICAgIHVzZUV4aXN0aW5nOiB1c2VFeGlzdGluZyxcbiAgICB1c2VGYWN0b3J5OiBwcm92aWRlci51c2VGYWN0b3J5LFxuICAgIHVzZVZhbHVlOiB1c2VWYWx1ZSxcbiAgICBkZXBzOiBkZXBzLFxuICAgIG11bHRpOiBwcm92aWRlci5tdWx0aVxuICB9O1xufVxuXG5mdW5jdGlvbiBfdHJhbnNmb3JtUHJvdmlkZXJBc3QoXG4gICAgcHJvdmlkZXI6IFByb3ZpZGVyQXN0LFxuICAgIHtlYWdlciwgcHJvdmlkZXJzfToge2VhZ2VyOiBib29sZWFuLCBwcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW119KTogUHJvdmlkZXJBc3Qge1xuICByZXR1cm4gbmV3IFByb3ZpZGVyQXN0KFxuICAgICAgcHJvdmlkZXIudG9rZW4sIHByb3ZpZGVyLm11bHRpUHJvdmlkZXIsIHByb3ZpZGVyLmVhZ2VyIHx8IGVhZ2VyLCBwcm92aWRlcnMsXG4gICAgICBwcm92aWRlci5wcm92aWRlclR5cGUsIHByb3ZpZGVyLmxpZmVjeWNsZUhvb2tzLCBwcm92aWRlci5zb3VyY2VTcGFuLCBwcm92aWRlci5pc01vZHVsZSk7XG59XG5cbmZ1bmN0aW9uIF9yZXNvbHZlUHJvdmlkZXJzRnJvbURpcmVjdGl2ZXMoXG4gICAgZGlyZWN0aXZlczogQ29tcGlsZURpcmVjdGl2ZVN1bW1hcnlbXSwgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLFxuICAgIHRhcmdldEVycm9yczogUGFyc2VFcnJvcltdKTogTWFwPGFueSwgUHJvdmlkZXJBc3Q+IHtcbiAgY29uc3QgcHJvdmlkZXJzQnlUb2tlbiA9IG5ldyBNYXA8YW55LCBQcm92aWRlckFzdD4oKTtcbiAgZGlyZWN0aXZlcy5mb3JFYWNoKChkaXJlY3RpdmUpID0+IHtcbiAgICBjb25zdCBkaXJQcm92aWRlcjpcbiAgICAgICAgQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGEgPSB7dG9rZW46IHtpZGVudGlmaWVyOiBkaXJlY3RpdmUudHlwZX0sIHVzZUNsYXNzOiBkaXJlY3RpdmUudHlwZX07XG4gICAgX3Jlc29sdmVQcm92aWRlcnMoXG4gICAgICAgIFtkaXJQcm92aWRlcl0sXG4gICAgICAgIGRpcmVjdGl2ZS5pc0NvbXBvbmVudCA/IFByb3ZpZGVyQXN0VHlwZS5Db21wb25lbnQgOiBQcm92aWRlckFzdFR5cGUuRGlyZWN0aXZlLCB0cnVlLFxuICAgICAgICBzb3VyY2VTcGFuLCB0YXJnZXRFcnJvcnMsIHByb3ZpZGVyc0J5VG9rZW4sIC8qIGlzTW9kdWxlICovIGZhbHNlKTtcbiAgfSk7XG5cbiAgLy8gTm90ZTogZGlyZWN0aXZlcyBuZWVkIHRvIGJlIGFibGUgdG8gb3ZlcndyaXRlIHByb3ZpZGVycyBvZiBhIGNvbXBvbmVudCFcbiAgY29uc3QgZGlyZWN0aXZlc1dpdGhDb21wb25lbnRGaXJzdCA9XG4gICAgICBkaXJlY3RpdmVzLmZpbHRlcihkaXIgPT4gZGlyLmlzQ29tcG9uZW50KS5jb25jYXQoZGlyZWN0aXZlcy5maWx0ZXIoZGlyID0+ICFkaXIuaXNDb21wb25lbnQpKTtcbiAgZGlyZWN0aXZlc1dpdGhDb21wb25lbnRGaXJzdC5mb3JFYWNoKChkaXJlY3RpdmUpID0+IHtcbiAgICBfcmVzb2x2ZVByb3ZpZGVycyhcbiAgICAgICAgZGlyZWN0aXZlLnByb3ZpZGVycywgUHJvdmlkZXJBc3RUeXBlLlB1YmxpY1NlcnZpY2UsIGZhbHNlLCBzb3VyY2VTcGFuLCB0YXJnZXRFcnJvcnMsXG4gICAgICAgIHByb3ZpZGVyc0J5VG9rZW4sIC8qIGlzTW9kdWxlICovIGZhbHNlKTtcbiAgICBfcmVzb2x2ZVByb3ZpZGVycyhcbiAgICAgICAgZGlyZWN0aXZlLnZpZXdQcm92aWRlcnMsIFByb3ZpZGVyQXN0VHlwZS5Qcml2YXRlU2VydmljZSwgZmFsc2UsIHNvdXJjZVNwYW4sIHRhcmdldEVycm9ycyxcbiAgICAgICAgcHJvdmlkZXJzQnlUb2tlbiwgLyogaXNNb2R1bGUgKi8gZmFsc2UpO1xuICB9KTtcbiAgcmV0dXJuIHByb3ZpZGVyc0J5VG9rZW47XG59XG5cbmZ1bmN0aW9uIF9yZXNvbHZlUHJvdmlkZXJzKFxuICAgIHByb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSwgcHJvdmlkZXJUeXBlOiBQcm92aWRlckFzdFR5cGUsIGVhZ2VyOiBib29sZWFuLFxuICAgIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhbiwgdGFyZ2V0RXJyb3JzOiBQYXJzZUVycm9yW10sXG4gICAgdGFyZ2V0UHJvdmlkZXJzQnlUb2tlbjogTWFwPGFueSwgUHJvdmlkZXJBc3Q+LCBpc01vZHVsZTogYm9vbGVhbikge1xuICBwcm92aWRlcnMuZm9yRWFjaCgocHJvdmlkZXIpID0+IHtcbiAgICBsZXQgcmVzb2x2ZWRQcm92aWRlciA9IHRhcmdldFByb3ZpZGVyc0J5VG9rZW4uZ2V0KHRva2VuUmVmZXJlbmNlKHByb3ZpZGVyLnRva2VuKSk7XG4gICAgaWYgKHJlc29sdmVkUHJvdmlkZXIgIT0gbnVsbCAmJiAhIXJlc29sdmVkUHJvdmlkZXIubXVsdGlQcm92aWRlciAhPT0gISFwcm92aWRlci5tdWx0aSkge1xuICAgICAgdGFyZ2V0RXJyb3JzLnB1c2gobmV3IFByb3ZpZGVyRXJyb3IoXG4gICAgICAgICAgYE1peGluZyBtdWx0aSBhbmQgbm9uIG11bHRpIHByb3ZpZGVyIGlzIG5vdCBwb3NzaWJsZSBmb3IgdG9rZW4gJHt0b2tlbk5hbWUocmVzb2x2ZWRQcm92aWRlci50b2tlbil9YCxcbiAgICAgICAgICBzb3VyY2VTcGFuKSk7XG4gICAgfVxuICAgIGlmICghcmVzb2x2ZWRQcm92aWRlcikge1xuICAgICAgY29uc3QgbGlmZWN5Y2xlSG9va3MgPSBwcm92aWRlci50b2tlbi5pZGVudGlmaWVyICYmXG4gICAgICAgICAgICAgICg8Q29tcGlsZVR5cGVNZXRhZGF0YT5wcm92aWRlci50b2tlbi5pZGVudGlmaWVyKS5saWZlY3ljbGVIb29rcyA/XG4gICAgICAgICAgKDxDb21waWxlVHlwZU1ldGFkYXRhPnByb3ZpZGVyLnRva2VuLmlkZW50aWZpZXIpLmxpZmVjeWNsZUhvb2tzIDpcbiAgICAgICAgICBbXTtcbiAgICAgIGNvbnN0IGlzVXNlVmFsdWUgPSAhKHByb3ZpZGVyLnVzZUNsYXNzIHx8IHByb3ZpZGVyLnVzZUV4aXN0aW5nIHx8IHByb3ZpZGVyLnVzZUZhY3RvcnkpO1xuICAgICAgcmVzb2x2ZWRQcm92aWRlciA9IG5ldyBQcm92aWRlckFzdChcbiAgICAgICAgICBwcm92aWRlci50b2tlbiwgISFwcm92aWRlci5tdWx0aSwgZWFnZXIgfHwgaXNVc2VWYWx1ZSwgW3Byb3ZpZGVyXSwgcHJvdmlkZXJUeXBlLFxuICAgICAgICAgIGxpZmVjeWNsZUhvb2tzLCBzb3VyY2VTcGFuLCBpc01vZHVsZSk7XG4gICAgICB0YXJnZXRQcm92aWRlcnNCeVRva2VuLnNldCh0b2tlblJlZmVyZW5jZShwcm92aWRlci50b2tlbiksIHJlc29sdmVkUHJvdmlkZXIpO1xuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoIXByb3ZpZGVyLm11bHRpKSB7XG4gICAgICAgIHJlc29sdmVkUHJvdmlkZXIucHJvdmlkZXJzLmxlbmd0aCA9IDA7XG4gICAgICB9XG4gICAgICByZXNvbHZlZFByb3ZpZGVyLnByb3ZpZGVycy5wdXNoKHByb3ZpZGVyKTtcbiAgICB9XG4gIH0pO1xufVxuXG5cbmZ1bmN0aW9uIF9nZXRWaWV3UXVlcmllcyhjb21wb25lbnQ6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSk6IE1hcDxhbnksIFF1ZXJ5V2l0aElkW10+IHtcbiAgLy8gTm90ZTogcXVlcmllcyBzdGFydCB3aXRoIGlkIDEgc28gd2UgY2FuIHVzZSB0aGUgbnVtYmVyIGluIGEgQmxvb20gZmlsdGVyIVxuICBsZXQgdmlld1F1ZXJ5SWQgPSAxO1xuICBjb25zdCB2aWV3UXVlcmllcyA9IG5ldyBNYXA8YW55LCBRdWVyeVdpdGhJZFtdPigpO1xuICBpZiAoY29tcG9uZW50LnZpZXdRdWVyaWVzKSB7XG4gICAgY29tcG9uZW50LnZpZXdRdWVyaWVzLmZvckVhY2goXG4gICAgICAgIChxdWVyeSkgPT4gX2FkZFF1ZXJ5VG9Ub2tlbk1hcCh2aWV3UXVlcmllcywge21ldGE6IHF1ZXJ5LCBxdWVyeUlkOiB2aWV3UXVlcnlJZCsrfSkpO1xuICB9XG4gIHJldHVybiB2aWV3UXVlcmllcztcbn1cblxuZnVuY3Rpb24gX2dldENvbnRlbnRRdWVyaWVzKFxuICAgIGNvbnRlbnRRdWVyeVN0YXJ0SWQ6IG51bWJlciwgZGlyZWN0aXZlczogQ29tcGlsZURpcmVjdGl2ZVN1bW1hcnlbXSk6IE1hcDxhbnksIFF1ZXJ5V2l0aElkW10+IHtcbiAgbGV0IGNvbnRlbnRRdWVyeUlkID0gY29udGVudFF1ZXJ5U3RhcnRJZDtcbiAgY29uc3QgY29udGVudFF1ZXJpZXMgPSBuZXcgTWFwPGFueSwgUXVlcnlXaXRoSWRbXT4oKTtcbiAgZGlyZWN0aXZlcy5mb3JFYWNoKChkaXJlY3RpdmUsIGRpcmVjdGl2ZUluZGV4KSA9PiB7XG4gICAgaWYgKGRpcmVjdGl2ZS5xdWVyaWVzKSB7XG4gICAgICBkaXJlY3RpdmUucXVlcmllcy5mb3JFYWNoKFxuICAgICAgICAgIChxdWVyeSkgPT4gX2FkZFF1ZXJ5VG9Ub2tlbk1hcChjb250ZW50UXVlcmllcywge21ldGE6IHF1ZXJ5LCBxdWVyeUlkOiBjb250ZW50UXVlcnlJZCsrfSkpO1xuICAgIH1cbiAgfSk7XG4gIHJldHVybiBjb250ZW50UXVlcmllcztcbn1cblxuZnVuY3Rpb24gX2FkZFF1ZXJ5VG9Ub2tlbk1hcChtYXA6IE1hcDxhbnksIFF1ZXJ5V2l0aElkW10+LCBxdWVyeTogUXVlcnlXaXRoSWQpIHtcbiAgcXVlcnkubWV0YS5zZWxlY3RvcnMuZm9yRWFjaCgodG9rZW46IENvbXBpbGVUb2tlbk1ldGFkYXRhKSA9PiB7XG4gICAgbGV0IGVudHJ5ID0gbWFwLmdldCh0b2tlblJlZmVyZW5jZSh0b2tlbikpO1xuICAgIGlmICghZW50cnkpIHtcbiAgICAgIGVudHJ5ID0gW107XG4gICAgICBtYXAuc2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSwgZW50cnkpO1xuICAgIH1cbiAgICBlbnRyeS5wdXNoKHF1ZXJ5KTtcbiAgfSk7XG59XG4iXX0=