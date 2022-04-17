/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { tokenName, tokenReference } from './compile_metadata';
import { Identifiers, createTokenForExternalReference } from './identifiers';
import { ParseError } from './parse_util';
import { ProviderAst, ProviderAstType } from './template_parser/template_ast';
var ProviderError = /** @class */ (function (_super) {
    tslib_1.__extends(ProviderError, _super);
    function ProviderError(message, span) {
        return _super.call(this, span, message) || this;
    }
    return ProviderError;
}(ParseError));
export { ProviderError };
var ProviderViewContext = /** @class */ (function () {
    function ProviderViewContext(reflector, component) {
        var _this = this;
        this.reflector = reflector;
        this.component = component;
        this.errors = [];
        this.viewQueries = _getViewQueries(component);
        this.viewProviders = new Map();
        component.viewProviders.forEach(function (provider) {
            if (_this.viewProviders.get(tokenReference(provider.token)) == null) {
                _this.viewProviders.set(tokenReference(provider.token), true);
            }
        });
    }
    return ProviderViewContext;
}());
export { ProviderViewContext };
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
            var templateRefId = createTokenForExternalReference(this.viewContext.reflector, Identifiers.TemplateRef);
            this._addQueryReadsTo(templateRefId, templateRefId, this._queriedTokens);
        }
        refs.forEach(function (refAst) {
            var defaultQueryValue = refAst.value ||
                createTokenForExternalReference(_this.viewContext.reflector, Identifiers.ElementRef);
            _this._addQueryReadsTo({ value: refAst.name }, defaultQueryValue, _this._queriedTokens);
        });
        if (this._queriedTokens.get(this.viewContext.reflector.resolveExternalReference(Identifiers.ViewContainerRef))) {
            this.transformedHasViewContainer = true;
        }
        // create the providers that we know are eager first
        Array.from(this._allProviders.values()).forEach(function (provider) {
            var eager = provider.eager || _this._queriedTokens.get(tokenReference(provider.token));
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
            var tokenRef = tokenReference(queryValue);
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
            queries = currentEl._contentQueries.get(tokenReference(token));
            if (queries) {
                result.push.apply(result, tslib_1.__spread(queries.filter(function (query) { return query.meta.descendants || distance <= 1; })));
            }
            if (currentEl._directiveAsts.length > 0) {
                distance++;
            }
            currentEl = currentEl._parent;
        }
        queries = this.viewContext.viewQueries.get(tokenReference(token));
        if (queries) {
            result.push.apply(result, tslib_1.__spread(queries));
        }
        return result;
    };
    ProviderElementContext.prototype._getOrCreateLocalProvider = function (requestingProviderType, token, eager) {
        var _this = this;
        var resolvedProvider = this._allProviders.get(tokenReference(token));
        if (!resolvedProvider || ((requestingProviderType === ProviderAstType.Directive ||
            requestingProviderType === ProviderAstType.PublicService) &&
            resolvedProvider.providerType === ProviderAstType.PrivateService) ||
            ((requestingProviderType === ProviderAstType.PrivateService ||
                requestingProviderType === ProviderAstType.PublicService) &&
                resolvedProvider.providerType === ProviderAstType.Builtin)) {
            return null;
        }
        var transformedProviderAst = this._transformedProviders.get(tokenReference(token));
        if (transformedProviderAst) {
            return transformedProviderAst;
        }
        if (this._seenProviders.get(tokenReference(token)) != null) {
            this.viewContext.errors.push(new ProviderError("Cannot instantiate cyclic dependency! " + tokenName(token), this._sourceSpan));
            return null;
        }
        this._seenProviders.set(tokenReference(token), true);
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
        this._transformedProviders.set(tokenReference(token), transformedProviderAst);
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
            if ((requestingProviderType === ProviderAstType.Directive ||
                requestingProviderType === ProviderAstType.Component)) {
                if (tokenReference(dep.token) ===
                    this.viewContext.reflector.resolveExternalReference(Identifiers.Renderer) ||
                    tokenReference(dep.token) ===
                        this.viewContext.reflector.resolveExternalReference(Identifiers.ElementRef) ||
                    tokenReference(dep.token) ===
                        this.viewContext.reflector.resolveExternalReference(Identifiers.ChangeDetectorRef) ||
                    tokenReference(dep.token) ===
                        this.viewContext.reflector.resolveExternalReference(Identifiers.TemplateRef)) {
                    return dep;
                }
                if (tokenReference(dep.token) ===
                    this.viewContext.reflector.resolveExternalReference(Identifiers.ViewContainerRef)) {
                    this.transformedHasViewContainer = true;
                }
            }
            // access the injector
            if (tokenReference(dep.token) ===
                this.viewContext.reflector.resolveExternalReference(Identifiers.Injector)) {
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
                result = currElement._getLocalDependency(ProviderAstType.PublicService, dep, currEager);
            }
            // check @Host restriction
            if (!result) {
                if (!dep.isHost || this.viewContext.component.isHost ||
                    this.viewContext.component.type.reference === tokenReference(dep.token) ||
                    this.viewContext.viewProviders.get(tokenReference(dep.token)) != null) {
                    result = dep;
                }
                else {
                    result = dep.isOptional ? { isValue: true, value: null } : null;
                }
            }
        }
        if (!result) {
            this.viewContext.errors.push(new ProviderError("No provider for " + tokenName(dep.token), this._sourceSpan));
        }
        return result;
    };
    return ProviderElementContext;
}());
export { ProviderElementContext };
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
            _resolveProviders([ngModuleProvider], ProviderAstType.PublicService, true, sourceSpan, _this._errors, _this._allProviders, /* isModule */ true);
        });
        _resolveProviders(ngModule.transitiveModule.providers.map(function (entry) { return entry.provider; }).concat(extraProviders), ProviderAstType.PublicService, false, sourceSpan, this._errors, this._allProviders, 
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
        var resolvedProvider = this._allProviders.get(tokenReference(token));
        if (!resolvedProvider) {
            return null;
        }
        var transformedProviderAst = this._transformedProviders.get(tokenReference(token));
        if (transformedProviderAst) {
            return transformedProviderAst;
        }
        if (this._seenProviders.get(tokenReference(token)) != null) {
            this._errors.push(new ProviderError("Cannot instantiate cyclic dependency! " + tokenName(token), resolvedProvider.sourceSpan));
            return null;
        }
        this._seenProviders.set(tokenReference(token), true);
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
        this._transformedProviders.set(tokenReference(token), transformedProviderAst);
        return transformedProviderAst;
    };
    NgModuleProviderAnalyzer.prototype._getDependency = function (dep, eager, requestorSourceSpan) {
        if (eager === void 0) { eager = false; }
        var foundLocal = false;
        if (!dep.isSkipSelf && dep.token != null) {
            // access the injector
            if (tokenReference(dep.token) ===
                this.reflector.resolveExternalReference(Identifiers.Injector) ||
                tokenReference(dep.token) ===
                    this.reflector.resolveExternalReference(Identifiers.ComponentFactoryResolver)) {
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
export { NgModuleProviderAnalyzer };
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
    return new ProviderAst(provider.token, provider.multiProvider, provider.eager || eager, providers, provider.providerType, provider.lifecycleHooks, provider.sourceSpan, provider.isModule);
}
function _resolveProvidersFromDirectives(directives, sourceSpan, targetErrors) {
    var providersByToken = new Map();
    directives.forEach(function (directive) {
        var dirProvider = { token: { identifier: directive.type }, useClass: directive.type };
        _resolveProviders([dirProvider], directive.isComponent ? ProviderAstType.Component : ProviderAstType.Directive, true, sourceSpan, targetErrors, providersByToken, /* isModule */ false);
    });
    // Note: directives need to be able to overwrite providers of a component!
    var directivesWithComponentFirst = directives.filter(function (dir) { return dir.isComponent; }).concat(directives.filter(function (dir) { return !dir.isComponent; }));
    directivesWithComponentFirst.forEach(function (directive) {
        _resolveProviders(directive.providers, ProviderAstType.PublicService, false, sourceSpan, targetErrors, providersByToken, /* isModule */ false);
        _resolveProviders(directive.viewProviders, ProviderAstType.PrivateService, false, sourceSpan, targetErrors, providersByToken, /* isModule */ false);
    });
    return providersByToken;
}
function _resolveProviders(providers, providerType, eager, sourceSpan, targetErrors, targetProvidersByToken, isModule) {
    providers.forEach(function (provider) {
        var resolvedProvider = targetProvidersByToken.get(tokenReference(provider.token));
        if (resolvedProvider != null && !!resolvedProvider.multiProvider !== !!provider.multi) {
            targetErrors.push(new ProviderError("Mixing multi and non multi provider is not possible for token " + tokenName(resolvedProvider.token), sourceSpan));
        }
        if (!resolvedProvider) {
            var lifecycleHooks = provider.token.identifier &&
                provider.token.identifier.lifecycleHooks ?
                provider.token.identifier.lifecycleHooks :
                [];
            var isUseValue = !(provider.useClass || provider.useExisting || provider.useFactory);
            resolvedProvider = new ProviderAst(provider.token, !!provider.multi, eager || isUseValue, [provider], providerType, lifecycleHooks, sourceSpan, isModule);
            targetProvidersByToken.set(tokenReference(provider.token), resolvedProvider);
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
        var entry = map.get(tokenReference(token));
        if (!entry) {
            entry = [];
            map.set(tokenReference(token), entry);
        }
        entry.push(query);
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJvdmlkZXJfYW5hbHl6ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvcHJvdmlkZXJfYW5hbHl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUdILE9BQU8sRUFBb00sU0FBUyxFQUFFLGNBQWMsRUFBQyxNQUFNLG9CQUFvQixDQUFDO0FBRWhRLE9BQU8sRUFBQyxXQUFXLEVBQUUsK0JBQStCLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFDM0UsT0FBTyxFQUFDLFVBQVUsRUFBa0IsTUFBTSxjQUFjLENBQUM7QUFDekQsT0FBTyxFQUF3QixXQUFXLEVBQUUsZUFBZSxFQUEyQixNQUFNLGdDQUFnQyxDQUFDO0FBRTdIO0lBQW1DLHlDQUFVO0lBQzNDLHVCQUFZLE9BQWUsRUFBRSxJQUFxQjtlQUFJLGtCQUFNLElBQUksRUFBRSxPQUFPLENBQUM7SUFBRSxDQUFDO0lBQy9FLG9CQUFDO0FBQUQsQ0FBQyxBQUZELENBQW1DLFVBQVUsR0FFNUM7O0FBT0Q7SUFXRSw2QkFBbUIsU0FBMkIsRUFBUyxTQUFtQztRQUExRixpQkFRQztRQVJrQixjQUFTLEdBQVQsU0FBUyxDQUFrQjtRQUFTLGNBQVMsR0FBVCxTQUFTLENBQTBCO1FBRjFGLFdBQU0sR0FBb0IsRUFBRSxDQUFDO1FBRzNCLElBQUksQ0FBQyxXQUFXLEdBQUcsZUFBZSxDQUFDLFNBQVMsQ0FBQyxDQUFDO1FBQzlDLElBQUksQ0FBQyxhQUFhLEdBQUcsSUFBSSxHQUFHLEVBQWdCLENBQUM7UUFDN0MsU0FBUyxDQUFDLGFBQWEsQ0FBQyxPQUFPLENBQUMsVUFBQyxRQUFRO1lBQ3ZDLElBQUksS0FBSSxDQUFDLGFBQWEsQ0FBQyxHQUFHLENBQUMsY0FBYyxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQyxJQUFJLElBQUksRUFBRTtnQkFDbEUsS0FBSSxDQUFDLGFBQWEs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TyxDQUNyQixVQUFDLEtBQUssSUFBSyxPQUFBLG1CQUFtQixDQUFDLGNBQWMsRUFBRSxFQUFDLElBQUksRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLGNBQWMsRUFBRSxFQUFDLENBQUMsRUFBN0UsQ0FBNkUsQ0FBQyxDQUFDO1NBQy9GO0lBQ0gsQ0FBQyxDQUFDLENBQUM7SUFDSCxPQUFPLGNBQWMsQ0FBQztBQUN4QixDQUFDO0FBRUQsU0FBUyxtQkFBbUIsQ0FBQyxHQUE0QixFQUFFLEtBQWtCO0lBQzNFLEtBQUssQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLE9BQU8sQ0FBQyxVQUFDLEtBQTJCO1FBQ3ZELElBQUksS0FBSyxHQUFHLEdBQUcsQ0FBQyxHQUFHLENBQUMsY0FBYyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUM7UUFDM0MsSUFBSSxDQUFDLEtBQUssRUFBRTtZQUNWLEtBQUssR0FBRyxFQUFFLENBQUM7WUFDWCxHQUFHLENBQUMsR0FBRyxDQUFDLGNBQWMsQ0FBQyxLQUFLLENBQUMsRUFBRSxLQUFLLENBQUMsQ0FBQztTQUN2QztRQUNELEtBQUssQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDcEIsQ0FBQyxDQUFDLENBQUM7QUFDTCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5cbmltcG9ydCB7Q29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhLCBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsIENvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5LCBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSwgQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGEsIENvbXBpbGVRdWVyeU1ldGFkYXRhLCBDb21waWxlVG9rZW5NZXRhZGF0YSwgQ29tcGlsZVR5cGVNZXRhZGF0YSwgdG9rZW5OYW1lLCB0b2tlblJlZmVyZW5jZX0gZnJvbSAnLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCB7Q29tcGlsZVJlZmxlY3Rvcn0gZnJvbSAnLi9jb21waWxlX3JlZmxlY3Rvcic7XG5pbXBvcnQge0lkZW50aWZpZXJzLCBjcmVhdGVUb2tlbkZvckV4dGVybmFsUmVmZXJlbmNlfSBmcm9tICcuL2lkZW50aWZpZXJzJztcbmltcG9ydCB7UGFyc2VFcnJvciwgUGFyc2VTb3VyY2VTcGFufSBmcm9tICcuL3BhcnNlX3V0aWwnO1xuaW1wb3J0IHtBdHRyQXN0LCBEaXJlY3RpdmVBc3QsIFByb3ZpZGVyQXN0LCBQcm92aWRlckFzdFR5cGUsIFF1ZXJ5TWF0Y2gsIFJlZmVyZW5jZUFzdH0gZnJvbSAnLi90ZW1wbGF0ZV9wYXJzZXIvdGVtcGxhdGVfYXN0JztcblxuZXhwb3J0IGNsYXNzIFByb3ZpZGVyRXJyb3IgZXh0ZW5kcyBQYXJzZUVycm9yIHtcbiAgY29uc3RydWN0b3IobWVzc2FnZTogc3RyaW5nLCBzcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHsgc3VwZXIoc3BhbiwgbWVzc2FnZSk7IH1cbn1cblxuZXhwb3J0IGludGVyZmFjZSBRdWVyeVdpdGhJZCB7XG4gIG1ldGE6IENvbXBpbGVRdWVyeU1ldGFkYXRhO1xuICBxdWVyeUlkOiBudW1iZXI7XG59XG5cbmV4cG9ydCBjbGFzcyBQcm92aWRlclZpZXdDb250ZXh0IHtcbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgdmlld1F1ZXJpZXM6IE1hcDxhbnksIFF1ZXJ5V2l0aElkW10+O1xuICAvKipcbiAgICogQGludGVybmFsXG4gICAqL1xuICB2aWV3UHJvdmlkZXJzOiBNYXA8YW55LCBib29sZWFuPjtcbiAgZXJyb3JzOiBQcm92aWRlckVycm9yW10gPSBbXTtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yLCBwdWJsaWMgY29tcG9uZW50OiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEpIHtcbiAgICB0aGlzLnZpZXdRdWVyaWVzID0gX2dldFZpZXdRdWVyaWVzKGNvbXBvbmVudCk7XG4gICAgdGhpcy52aWV3UHJvdmlkZXJzID0gbmV3IE1hcDxhbnksIGJvb2xlYW4+KCk7XG4gICAgY29tcG9uZW50LnZpZXdQcm92aWRlcnMuZm9yRWFjaCgocHJvdmlkZXIpID0+IHtcbiAgICAgIGlmICh0aGlzLnZpZXdQcm92aWRlcnMuZ2V0KHRva2VuUmVmZXJlbmNlKHByb3ZpZGVyLnRva2VuKSkgPT0gbnVsbCkge1xuICAgICAgICB0aGlzLnZpZXdQcm92aWRlcnMuc2V0KHRva2VuUmVmZXJlbmNlKHByb3ZpZGVyLnRva2VuKSwgdHJ1ZSk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFByb3ZpZGVyRWxlbWVudENvbnRleHQge1xuICBwcml2YXRlIF9jb250ZW50UXVlcmllczogTWFwPGFueSwgUXVlcnlXaXRoSWRbXT47XG5cbiAgcHJpdmF0ZSBfdHJhbnNmb3JtZWRQcm92aWRlcnMgPSBuZXcgTWFwPGFueSwgUHJvdmlkZXJBc3Q+KCk7XG4gIHByaXZhdGUgX3NlZW5Qcm92aWRlcnMgPSBuZXcgTWFwPGFueSwgYm9vbGVhbj4oKTtcbiAgcHJpdmF0ZSBfYWxsUHJvdmlkZXJzOiBNYXA8YW55LCBQcm92aWRlckFzdD47XG4gIHByaXZhdGUgX2F0dHJzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgcHJpdmF0ZSBfcXVlcmllZFRva2VucyA9IG5ldyBNYXA8YW55LCBRdWVyeU1hdGNoW10+KCk7XG5cbiAgcHVibGljIHJlYWRvbmx5IHRyYW5zZm9ybWVkSGFzVmlld0NvbnRhaW5lcjogYm9vbGVhbiA9IGZhbHNlO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIHZpZXdDb250ZXh0OiBQcm92aWRlclZpZXdDb250ZXh0LCBwcml2YXRlIF9wYXJlbnQ6IFByb3ZpZGVyRWxlbWVudENvbnRleHQsXG4gICAgICBwcml2YXRlIF9pc1ZpZXdSb290OiBib29sZWFuLCBwcml2YXRlIF9kaXJlY3RpdmVBc3RzOiBEaXJlY3RpdmVBc3RbXSwgYXR0cnM6IEF0dHJBc3RbXSxcbiAgICAgIHJlZnM6IFJlZmVyZW5jZUFzdFtdLCBpc1RlbXBsYXRlOiBib29sZWFuLCBjb250ZW50UXVlcnlTdGFydElkOiBudW1iZXIsXG4gICAgICBwcml2YXRlIF9zb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4pIHtcbiAgICB0aGlzLl9hdHRycyA9IHt9O1xuICAgIGF0dHJzLmZvckVhY2goKGF0dHJBc3QpID0+IHRoaXMuX2F0dHJzW2F0dHJBc3QubmFtZV0gPSBhdHRyQXN0LnZhbHVlKTtcbiAgICBjb25zdCBkaXJlY3RpdmVzTWV0YSA9IF9kaXJlY3RpdmVBc3RzLm1hcChkaXJlY3RpdmVBc3QgPT4gZGlyZWN0aXZlQXN0LmRpcmVjdGl2ZSk7XG4gICAgdGhpcy5fYWxsUHJvdmlkZXJzID1cbiAgICAgICAgX3Jlc29sdmVQcm92aWRlcnNGcm9tRGlyZWN0aXZlcyhkaXJlY3RpdmVzTWV0YSwgX3NvdXJjZVNwYW4sIHZpZXdDb250ZXh0LmVycm9ycyk7XG4gICAgdGhpcy5fY29udGVudFF1ZXJpZXMgPSBfZ2V0Q29udGVudFF1ZXJpZXMoY29udGVudFF1ZXJ5U3RhcnRJZCwgZGlyZWN0aXZlc01ldGEpO1xuICAgIEFycmF5LmZyb20odGhpcy5fYWxsUHJvdmlkZXJzLnZhbHVlcygpKS5mb3JFYWNoKChwcm92aWRlcikgPT4ge1xuICAgICAgdGhpcy5fYWRkUXVlcnlSZWFkc1RvKHByb3ZpZGVyLnRva2VuLCBwcm92aWRlci50b2tlbiwgdGhpcy5fcXVlcmllZFRva2Vucyk7XG4gICAgfSk7XG4gICAgaWYgKGlzVGVtcGxhdGUpIHtcbiAgICAgIGNvbnN0IHRlbXBsYXRlUmVmSWQgPVxuICAgICAgICAgIGNyZWF0ZVRva2VuRm9yRXh0ZXJuYWxSZWZlcmVuY2UodGhpcy52aWV3Q29udGV4dC5yZWZsZWN0b3IsIElkZW50aWZpZXJzLlRlbXBsYXRlUmVmKTtcbiAgICAgIHRoaXMuX2FkZFF1ZXJ5UmVhZHNUbyh0ZW1wbGF0ZVJlZklkLCB0ZW1wbGF0ZVJlZklkLCB0aGlzLl9xdWVyaWVkVG9rZW5zKTtcbiAgICB9XG4gICAgcmVmcy5mb3JFYWNoKChyZWZBc3QpID0+IHtcbiAgICAgIGxldCBkZWZhdWx0UXVlcnlWYWx1ZSA9IHJlZkFzdC52YWx1ZSB8fFxuICAgICAgICAgIGNyZWF0ZVRva2VuRm9yRXh0ZXJuYWxSZWZlcmVuY2UodGhpcy52aWV3Q29udGV4dC5yZWZsZWN0b3IsIElkZW50aWZpZXJzLkVsZW1lbnRSZWYpO1xuICAgICAgdGhpcy5fYWRkUXVlcnlSZWFkc1RvKHt2YWx1ZTogcmVmQXN0Lm5hbWV9LCBkZWZhdWx0UXVlcnlWYWx1ZSwgdGhpcy5fcXVlcmllZFRva2Vucyk7XG4gICAgfSk7XG4gICAgaWYgKHRoaXMuX3F1ZXJpZWRUb2tlbnMuZ2V0KFxuICAgICAgICAgICAgdGhpcy52aWV3Q29udGV4dC5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKElkZW50aWZpZXJzLlZpZXdDb250YWluZXJSZWYpKSkge1xuICAgICAgdGhpcy50cmFuc2Zvcm1lZEhhc1ZpZXdDb250YWluZXIgPSB0cnVlO1xuICAgIH1cblxuICAgIC8vIGNyZWF0ZSB0aGUgcHJvdmlkZXJzIHRoYXQgd2Uga25vdyBhcmUgZWFnZXIgZmlyc3RcbiAgICBBcnJheS5mcm9tKHRoaXMuX2FsbFByb3ZpZGVycy52YWx1ZXMoKSkuZm9yRWFjaCgocHJvdmlkZXIpID0+IHtcbiAgICAgIGNvbnN0IGVhZ2VyID0gcHJvdmlkZXIuZWFnZXIgfHwgdGhpcy5fcXVlcmllZFRva2Vucy5nZXQodG9rZW5SZWZlcmVuY2UocHJvdmlkZXIudG9rZW4pKTtcbiAgICAgIGlmIChlYWdlcikge1xuICAgICAgICB0aGlzLl9nZXRPckNyZWF0ZUxvY2FsUHJvdmlkZXIocHJvdmlkZXIucHJvdmlkZXJUeXBlLCBwcm92aWRlci50b2tlbiwgdHJ1ZSk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICBhZnRlckVsZW1lbnQoKSB7XG4gICAgLy8gY29sbGVjdCBsYXp5IHByb3ZpZGVyc1xuICAgIEFycmF5LmZyb20odGhpcy5fYWxsUHJvdmlkZXJzLnZhbHVlcygpKS5mb3JFYWNoKChwcm92aWRlcikgPT4ge1xuICAgICAgdGhpcy5fZ2V0T3JDcmVhdGVMb2NhbFByb3ZpZGVyKHByb3ZpZGVyLnByb3ZpZGVyVHlwZSwgcHJvdmlkZXIudG9rZW4sIGZhbHNlKTtcbiAgICB9KTtcbiAgfVxuXG4gIGdldCB0cmFuc2Zvcm1Qcm92aWRlcnMoKTogUHJvdmlkZXJBc3RbXSB7XG4gICAgLy8gTm90ZTogTWFwcyBrZWVwIHRoZWlyIGluc2VydGlvbiBvcmRlci5cbiAgICBjb25zdCBsYXp5UHJvdmlkZXJzOiBQcm92aWRlckFzdFtdID0gW107XG4gICAgY29uc3QgZWFnZXJQcm92aWRlcnM6IFByb3ZpZGVyQXN0W10gPSBbXTtcbiAgICB0aGlzLl90cmFuc2Zvcm1lZFByb3ZpZGVycy5mb3JFYWNoKHByb3ZpZGVyID0+IHtcbiAgICAgIGlmIChwcm92aWRlci5lYWdlcikge1xuICAgICAgICBlYWdlclByb3ZpZGVycy5wdXNoKHByb3ZpZGVyKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGxhenlQcm92aWRlcnMucHVzaChwcm92aWRlcik7XG4gICAgICB9XG4gICAgfSk7XG4gICAgcmV0dXJuIGxhenlQcm92aWRlcnMuY29uY2F0KGVhZ2VyUHJvdmlkZXJzKTtcbiAgfVxuXG4gIGdldCB0cmFuc2Zvcm1lZERpcmVjdGl2ZUFzdHMoKTogRGlyZWN0aXZlQXN0W10ge1xuICAgIGNvbnN0IHNvcnRlZFByb3ZpZGVyVHlwZXMgPSB0aGlzLnRyYW5zZm9ybVByb3ZpZGVycy5tYXAocHJvdmlkZXIgPT4gcHJvdmlkZXIudG9rZW4uaWRlbnRpZmllcik7XG4gICAgY29uc3Qgc29ydGVkRGlyZWN0aXZlcyA9IHRoaXMuX2RpcmVjdGl2ZUFzdHMuc2xpY2UoKTtcbiAgICBzb3J0ZWREaXJlY3RpdmVzLnNvcnQoXG4gICAgICAgIChkaXIxLCBkaXIyKSA9PiBzb3J0ZWRQcm92aWRlclR5cGVzLmluZGV4T2YoZGlyMS5kaXJlY3RpdmUudHlwZSkgLVxuICAgICAgICAgICAgc29ydGVkUHJvdmlkZXJUeXBlcy5pbmRleE9mKGRpcjIuZGlyZWN0aXZlLnR5cGUpKTtcbiAgICByZXR1cm4gc29ydGVkRGlyZWN0aXZlcztcbiAgfVxuXG4gIGdldCBxdWVyeU1hdGNoZXMoKTogUXVlcnlNYXRjaFtdIHtcbiAgICBjb25zdCBhbGxNYXRjaGVzOiBRdWVyeU1hdGNoW10gPSBbXTtcbiAgICB0aGlzLl9xdWVyaWVkVG9rZW5zLmZvckVhY2goKG1hdGNoZXM6IFF1ZXJ5TWF0Y2hbXSkgPT4geyBhbGxNYXRjaGVzLnB1c2goLi4ubWF0Y2hlcyk7IH0pO1xuICAgIHJldHVybiBhbGxNYXRjaGVzO1xuICB9XG5cbiAgcHJpdmF0ZSBfYWRkUXVlcnlSZWFkc1RvKFxuICAgICAgdG9rZW46IENvbXBpbGVUb2tlbk1ldGFkYXRhLCBkZWZhdWx0VmFsdWU6IENvbXBpbGVUb2tlbk1ldGFkYXRhLFxuICAgICAgcXVlcnlSZWFkVG9rZW5zOiBNYXA8YW55LCBRdWVyeU1hdGNoW10+KSB7XG4gICAgdGhpcy5fZ2V0UXVlcmllc0Zvcih0b2tlbikuZm9yRWFjaCgocXVlcnkpID0+IHtcbiAgICAgIGNvbnN0IHF1ZXJ5VmFsdWUgPSBxdWVyeS5tZXRhLnJlYWQgfHwgZGVmYXVsdFZhbHVlO1xuICAgICAgY29uc3QgdG9rZW5SZWYgPSB0b2tlblJlZmVyZW5jZShxdWVyeVZhbHVlKTtcbiAgICAgIGxldCBxdWVyeU1hdGNoZXMgPSBxdWVyeVJlYWRUb2tlbnMuZ2V0KHRva2VuUmVmKTtcbiAgICAgIGlmICghcXVlcnlNYXRjaGVzKSB7XG4gICAgICAgIHF1ZXJ5TWF0Y2hlcyA9IFtdO1xuICAgICAgICBxdWVyeVJlYWRUb2tlbnMuc2V0KHRva2VuUmVmLCBxdWVyeU1hdGNoZXMpO1xuICAgICAgfVxuICAgICAgcXVlcnlNYXRjaGVzLnB1c2goe3F1ZXJ5SWQ6IHF1ZXJ5LnF1ZXJ5SWQsIHZhbHVlOiBxdWVyeVZhbHVlfSk7XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIF9nZXRRdWVyaWVzRm9yKHRva2VuOiBDb21waWxlVG9rZW5NZXRhZGF0YSk6IFF1ZXJ5V2l0aElkW10ge1xuICAgIGNvbnN0IHJlc3VsdDogUXVlcnlXaXRoSWRbXSA9IFtdO1xuICAgIGxldCBjdXJyZW50RWw6IFByb3ZpZGVyRWxlbWVudENvbnRleHQgPSB0aGlzO1xuICAgIGxldCBkaXN0YW5jZSA9IDA7XG4gICAgbGV0IHF1ZXJpZXM6IFF1ZXJ5V2l0aElkW118dW5kZWZpbmVkO1xuICAgIHdoaWxlIChjdXJyZW50RWwgIT09IG51bGwpIHtcbiAgICAgIHF1ZXJpZXMgPSBjdXJyZW50RWwuX2NvbnRlbnRRdWVyaWVzLmdldCh0b2tlblJlZmVyZW5jZSh0b2tlbikpO1xuICAgICAgaWYgKHF1ZXJpZXMpIHtcbiAgICAgICAgcmVzdWx0LnB1c2goLi4ucXVlcmllcy5maWx0ZXIoKHF1ZXJ5KSA9PiBxdWVyeS5tZXRhLmRlc2NlbmRhbnRzIHx8IGRpc3RhbmNlIDw9IDEpKTtcbiAgICAgIH1cbiAgICAgIGlmIChjdXJyZW50RWwuX2RpcmVjdGl2ZUFzdHMubGVuZ3RoID4gMCkge1xuICAgICAgICBkaXN0YW5jZSsrO1xuICAgICAgfVxuICAgICAgY3VycmVudEVsID0gY3VycmVudEVsLl9wYXJlbnQ7XG4gICAgfVxuICAgIHF1ZXJpZXMgPSB0aGlzLnZpZXdDb250ZXh0LnZpZXdRdWVyaWVzLmdldCh0b2tlblJlZmVyZW5jZSh0b2tlbikpO1xuICAgIGlmIChxdWVyaWVzKSB7XG4gICAgICByZXN1bHQucHVzaCguLi5xdWVyaWVzKTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG5cbiAgcHJpdmF0ZSBfZ2V0T3JDcmVhdGVMb2NhbFByb3ZpZGVyKFxuICAgICAgcmVxdWVzdGluZ1Byb3ZpZGVyVHlwZTogUHJvdmlkZXJBc3RUeXBlLCB0b2tlbjogQ29tcGlsZVRva2VuTWV0YWRhdGEsXG4gICAgICBlYWdlcjogYm9vbGVhbik6IFByb3ZpZGVyQXN0fG51bGwge1xuICAgIGNvbnN0IHJlc29sdmVkUHJvdmlkZXIgPSB0aGlzLl9hbGxQcm92aWRlcnMuZ2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSk7XG4gICAgaWYgKCFyZXNvbHZlZFByb3ZpZGVyIHx8ICgocmVxdWVzdGluZ1Byb3ZpZGVyVHlwZSA9PT0gUHJvdmlkZXJBc3RUeXBlLkRpcmVjdGl2ZSB8fFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJlcXVlc3RpbmdQcm92aWRlclR5cGUgPT09IFByb3ZpZGVyQXN0VHlwZS5QdWJsaWNTZXJ2aWNlKSAmJlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVzb2x2ZWRQcm92aWRlci5wcm92aWRlclR5cGUgPT09IFByb3ZpZGVyQXN0VHlwZS5Qcml2YXRlU2VydmljZSkgfHxcbiAgICAgICAgKChyZXF1ZXN0aW5nUHJvdmlkZXJUeXBlID09PSBQcm92aWRlckFzdFR5cGUuUHJpdmF0ZVNlcnZpY2UgfHxcbiAgICAgICAgICByZXF1ZXN0aW5nUHJvdmlkZXJUeXBlID09PSBQcm92aWRlckFzdFR5cGUuUHVibGljU2VydmljZSkgJiZcbiAgICAgICAgIHJlc29sdmVkUHJvdmlkZXIucHJvdmlkZXJUeXBlID09PSBQcm92aWRlckFzdFR5cGUuQnVpbHRpbikpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICBsZXQgdHJhbnNmb3JtZWRQcm92aWRlckFzdCA9IHRoaXMuX3RyYW5zZm9ybWVkUHJvdmlkZXJzLmdldCh0b2tlblJlZmVyZW5jZSh0b2tlbikpO1xuICAgIGlmICh0cmFuc2Zvcm1lZFByb3ZpZGVyQXN0KSB7XG4gICAgICByZXR1cm4gdHJhbnNmb3JtZWRQcm92aWRlckFzdDtcbiAgICB9XG4gICAgaWYgKHRoaXMuX3NlZW5Qcm92aWRlcnMuZ2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSkgIT0gbnVsbCkge1xuICAgICAgdGhpcy52aWV3Q29udGV4dC5lcnJvcnMucHVzaChuZXcgUHJvdmlkZXJFcnJvcihcbiAgICAgICAgICBgQ2Fubm90IGluc3RhbnRpYXRlIGN5Y2xpYyBkZXBlbmRlbmN5ISAke3Rva2VuTmFtZSh0b2tlbil9YCwgdGhpcy5fc291cmNlU3BhbikpO1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIHRoaXMuX3NlZW5Qcm92aWRlcnMuc2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSwgdHJ1ZSk7XG4gICAgY29uc3QgdHJhbnNmb3JtZWRQcm92aWRlcnMgPSByZXNvbHZlZFByb3ZpZGVyLnByb3ZpZGVycy5tYXAoKHByb3ZpZGVyKSA9PiB7XG4gICAgICBsZXQgdHJhbnNmb3JtZWRVc2VWYWx1ZSA9IHByb3ZpZGVyLnVzZVZhbHVlO1xuICAgICAgbGV0IHRyYW5zZm9ybWVkVXNlRXhpc3RpbmcgPSBwcm92aWRlci51c2VFeGlzdGluZyAhO1xuICAgICAgbGV0IHRyYW5zZm9ybWVkRGVwczogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhW10gPSB1bmRlZmluZWQgITtcbiAgICAgIGlmIChwcm92aWRlci51c2VFeGlzdGluZyAhPSBudWxsKSB7XG4gICAgICAgIGNvbnN0IGV4aXN0aW5nRGlEZXAgPSB0aGlzLl9nZXREZXBlbmRlbmN5KFxuICAgICAgICAgICAgcmVzb2x2ZWRQcm92aWRlci5wcm92aWRlclR5cGUsIHt0b2tlbjogcHJvdmlkZXIudXNlRXhpc3Rpbmd9LCBlYWdlcikgITtcbiAgICAgICAgaWYgKGV4aXN0aW5nRGlEZXAudG9rZW4gIT0gbnVsbCkge1xuICAgICAgICAgIHRyYW5zZm9ybWVkVXNlRXhpc3RpbmcgPSBleGlzdGluZ0RpRGVwLnRva2VuO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRyYW5zZm9ybWVkVXNlRXhpc3RpbmcgPSBudWxsICE7XG4gICAgICAgICAgdHJhbnNmb3JtZWRVc2VWYWx1ZSA9IGV4aXN0aW5nRGlEZXAudmFsdWU7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSBpZiAocHJvdmlkZXIudXNlRmFjdG9yeSkge1xuICAgICAgICBjb25zdCBkZXBzID0gcHJvdmlkZXIuZGVwcyB8fCBwcm92aWRlci51c2VGYWN0b3J5LmRpRGVwcztcbiAgICAgICAgdHJhbnNmb3JtZWREZXBzID1cbiAgICAgICAgICAgIGRlcHMubWFwKChkZXApID0+IHRoaXMuX2dldERlcGVuZGVuY3kocmVzb2x2ZWRQcm92aWRlci5wcm92aWRlclR5cGUsIGRlcCwgZWFnZXIpICEpO1xuICAgICAgfSBlbHNlIGlmIChwcm92aWRlci51c2VDbGFzcykge1xuICAgICAgICBjb25zdCBkZXBzID0gcHJvdmlkZXIuZGVwcyB8fCBwcm92aWRlci51c2VDbGFzcy5kaURlcHM7XG4gICAgICAgIHRyYW5zZm9ybWVkRGVwcyA9XG4gICAgICAgICAgICBkZXBzLm1hcCgoZGVwKSA9PiB0aGlzLl9nZXREZXBlbmRlbmN5KHJlc29sdmVkUHJvdmlkZXIucHJvdmlkZXJUeXBlLCBkZXAsIGVhZ2VyKSAhKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBfdHJhbnNmb3JtUHJvdmlkZXIocHJvdmlkZXIsIHtcbiAgICAgICAgdXNlRXhpc3Rpbmc6IHRyYW5zZm9ybWVkVXNlRXhpc3RpbmcsXG4gICAgICAgIHVzZVZhbHVlOiB0cmFuc2Zvcm1lZFVzZVZhbHVlLFxuICAgICAgICBkZXBzOiB0cmFuc2Zvcm1lZERlcHNcbiAgICAgIH0pO1xuICAgIH0pO1xuICAgIHRyYW5zZm9ybWVkUHJvdmlkZXJBc3QgPVxuICAgICAgICBfdHJhbnNmb3JtUHJvdmlkZXJBc3QocmVzb2x2ZWRQcm92aWRlciwge2VhZ2VyOiBlYWdlciwgcHJvdmlkZXJzOiB0cmFuc2Zvcm1lZFByb3ZpZGVyc30pO1xuICAgIHRoaXMuX3RyYW5zZm9ybWVkUHJvdmlkZXJzLnNldCh0b2tlblJlZmVyZW5jZSh0b2tlbiksIHRyYW5zZm9ybWVkUHJvdmlkZXJBc3QpO1xuICAgIHJldHVybiB0cmFuc2Zvcm1lZFByb3ZpZGVyQXN0O1xuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0TG9jYWxEZXBlbmRlbmN5KFxuICAgICAgcmVxdWVzdGluZ1Byb3ZpZGVyVHlwZTogUHJvdmlkZXJBc3RUeXBlLCBkZXA6IENvbXBpbGVEaURlcGVuZGVuY3lNZXRhZGF0YSxcbiAgICAgIGVhZ2VyOiBib29sZWFuID0gZmFsc2UpOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGF8bnVsbCB7XG4gICAgaWYgKGRlcC5pc0F0dHJpYnV0ZSkge1xuICAgICAgY29uc3QgYXR0clZhbHVlID0gdGhpcy5fYXR0cnNbZGVwLnRva2VuICEudmFsdWVdO1xuICAgICAgcmV0dXJuIHtpc1ZhbHVlOiB0cnVlLCB2YWx1ZTogYXR0clZhbHVlID09IG51bGwgPyBudWxsIDogYXR0clZhbHVlfTtcbiAgICB9XG5cbiAgICBpZiAoZGVwLnRva2VuICE9IG51bGwpIHtcbiAgICAgIC8vIGFjY2VzcyBidWlsdGludHNcbiAgICAgIGlmICgocmVxdWVzdGluZ1Byb3ZpZGVyVHlwZSA9PT0gUHJvdmlkZXJBc3RUeXBlLkRpcmVjdGl2ZSB8fFxuICAgICAgICAgICByZXF1ZXN0aW5nUHJvdmlkZXJUeXBlID09PSBQcm92aWRlckFzdFR5cGUuQ29tcG9uZW50KSkge1xuICAgICAgICBpZiAodG9rZW5SZWZlcmVuY2UoZGVwLnRva2VuKSA9PT1cbiAgICAgICAgICAgICAgICB0aGlzLnZpZXdDb250ZXh0LnJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuUmVuZGVyZXIpIHx8XG4gICAgICAgICAgICB0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgICAgICAgIHRoaXMudmlld0NvbnRleHQucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5FbGVtZW50UmVmKSB8fFxuICAgICAgICAgICAgdG9rZW5SZWZlcmVuY2UoZGVwLnRva2VuKSA9PT1cbiAgICAgICAgICAgICAgICB0aGlzLnZpZXdDb250ZXh0LnJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoXG4gICAgICAgICAgICAgICAgICAgIElkZW50aWZpZXJzLkNoYW5nZURldGVjdG9yUmVmKSB8fFxuICAgICAgICAgICAgdG9rZW5SZWZlcmVuY2UoZGVwLnRva2VuKSA9PT1cbiAgICAgICAgICAgICAgICB0aGlzLnZpZXdDb250ZXh0LnJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuVGVtcGxhdGVSZWYpKSB7XG4gICAgICAgICAgcmV0dXJuIGRlcDtcbiAgICAgICAgfVxuICAgICAgICBpZiAodG9rZW5SZWZlcmVuY2UoZGVwLnRva2VuKSA9PT1cbiAgICAgICAgICAgIHRoaXMudmlld0NvbnRleHQucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5WaWV3Q29udGFpbmVyUmVmKSkge1xuICAgICAgICAgICh0aGlzIGFze3RyYW5zZm9ybWVkSGFzVmlld0NvbnRhaW5lcjogYm9vbGVhbn0pLnRyYW5zZm9ybWVkSGFzVmlld0NvbnRhaW5lciA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIC8vIGFjY2VzcyB0aGUgaW5qZWN0b3JcbiAgICAgIGlmICh0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgIHRoaXMudmlld0NvbnRleHQucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5JbmplY3RvcikpIHtcbiAgICAgICAgcmV0dXJuIGRlcDtcbiAgICAgIH1cbiAgICAgIC8vIGFjY2VzcyBwcm92aWRlcnNcbiAgICAgIGlmICh0aGlzLl9nZXRPckNyZWF0ZUxvY2FsUHJvdmlkZXIocmVxdWVzdGluZ1Byb3ZpZGVyVHlwZSwgZGVwLnRva2VuLCBlYWdlcikgIT0gbnVsbCkge1xuICAgICAgICByZXR1cm4gZGVwO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHByaXZhdGUgX2dldERlcGVuZGVuY3koXG4gICAgICByZXF1ZXN0aW5nUHJvdmlkZXJUeXBlOiBQcm92aWRlckFzdFR5cGUsIGRlcDogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhLFxuICAgICAgZWFnZXI6IGJvb2xlYW4gPSBmYWxzZSk6IENvbXBpbGVEaURlcGVuZGVuY3lNZXRhZGF0YXxudWxsIHtcbiAgICBsZXQgY3VyckVsZW1lbnQ6IFByb3ZpZGVyRWxlbWVudENvbnRleHQgPSB0aGlzO1xuICAgIGxldCBjdXJyRWFnZXI6IGJvb2xlYW4gPSBlYWdlcjtcbiAgICBsZXQgcmVzdWx0OiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGF8bnVsbCA9IG51bGw7XG4gICAgaWYgKCFkZXAuaXNTa2lwU2VsZikge1xuICAgICAgcmVzdWx0ID0gdGhpcy5fZ2V0TG9jYWxEZXBlbmRlbmN5KHJlcXVlc3RpbmdQcm92aWRlclR5cGUsIGRlcCwgZWFnZXIpO1xuICAgIH1cbiAgICBpZiAoZGVwLmlzU2VsZikge1xuICAgICAgaWYgKCFyZXN1bHQgJiYgZGVwLmlzT3B0aW9uYWwpIHtcbiAgICAgICAgcmVzdWx0ID0ge2lzVmFsdWU6IHRydWUsIHZhbHVlOiBudWxsfTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgLy8gY2hlY2sgcGFyZW50IGVsZW1lbnRzXG4gICAgICB3aGlsZSAoIXJlc3VsdCAmJiBjdXJyRWxlbWVudC5fcGFyZW50KSB7XG4gICAgICAgIGNvbnN0IHByZXZFbGVtZW50ID0gY3VyckVsZW1lbnQ7XG4gICAgICAgIGN1cnJFbGVtZW50ID0gY3VyckVsZW1lbnQuX3BhcmVudDtcbiAgICAgICAgaWYgKHByZXZFbGVtZW50Ll9pc1ZpZXdSb290KSB7XG4gICAgICAgICAgY3VyckVhZ2VyID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgcmVzdWx0ID0gY3VyckVsZW1lbnQuX2dldExvY2FsRGVwZW5kZW5jeShQcm92aWRlckFzdFR5cGUuUHVibGljU2VydmljZSwgZGVwLCBjdXJyRWFnZXIpO1xuICAgICAgfVxuICAgICAgLy8gY2hlY2sgQEhvc3QgcmVzdHJpY3Rpb25cbiAgICAgIGlmICghcmVzdWx0KSB7XG4gICAgICAgIGlmICghZGVwLmlzSG9zdCB8fCB0aGlzLnZpZXdDb250ZXh0LmNvbXBvbmVudC5pc0hvc3QgfHxcbiAgICAgICAgICAgIHRoaXMudmlld0NvbnRleHQuY29tcG9uZW50LnR5cGUucmVmZXJlbmNlID09PSB0b2tlblJlZmVyZW5jZShkZXAudG9rZW4gISkgfHxcbiAgICAgICAgICAgIHRoaXMudmlld0NvbnRleHQudmlld1Byb3ZpZGVycy5nZXQodG9rZW5SZWZlcmVuY2UoZGVwLnRva2VuICEpKSAhPSBudWxsKSB7XG4gICAgICAgICAgcmVzdWx0ID0gZGVwO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJlc3VsdCA9IGRlcC5pc09wdGlvbmFsID8ge2lzVmFsdWU6IHRydWUsIHZhbHVlOiBudWxsfSA6IG51bGw7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgIHRoaXMudmlld0NvbnRleHQuZXJyb3JzLnB1c2goXG4gICAgICAgICAgbmV3IFByb3ZpZGVyRXJyb3IoYE5vIHByb3ZpZGVyIGZvciAke3Rva2VuTmFtZShkZXAudG9rZW4hKX1gLCB0aGlzLl9zb3VyY2VTcGFuKSk7XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cbn1cblxuXG5leHBvcnQgY2xhc3MgTmdNb2R1bGVQcm92aWRlckFuYWx5emVyIHtcbiAgcHJpdmF0ZSBfdHJhbnNmb3JtZWRQcm92aWRlcnMgPSBuZXcgTWFwPGFueSwgUHJvdmlkZXJBc3Q+KCk7XG4gIHByaXZhdGUgX3NlZW5Qcm92aWRlcnMgPSBuZXcgTWFwPGFueSwgYm9vbGVhbj4oKTtcbiAgcHJpdmF0ZSBfYWxsUHJvdmlkZXJzOiBNYXA8YW55LCBQcm92aWRlckFzdD47XG4gIHByaXZhdGUgX2Vycm9yczogUHJvdmlkZXJFcnJvcltdID0gW107XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIHJlZmxlY3RvcjogQ29tcGlsZVJlZmxlY3RvciwgbmdNb2R1bGU6IENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhLFxuICAgICAgZXh0cmFQcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW10sIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3Bhbikge1xuICAgIHRoaXMuX2FsbFByb3ZpZGVycyA9IG5ldyBNYXA8YW55LCBQcm92aWRlckFzdD4oKTtcbiAgICBuZ01vZHVsZS50cmFuc2l0aXZlTW9kdWxlLm1vZHVsZXMuZm9yRWFjaCgobmdNb2R1bGVUeXBlOiBDb21waWxlVHlwZU1ldGFkYXRhKSA9PiB7XG4gICAgICBjb25zdCBuZ01vZHVsZVByb3ZpZGVyID0ge3Rva2VuOiB7aWRlbnRpZmllcjogbmdNb2R1bGVUeXBlfSwgdXNlQ2xhc3M6IG5nTW9kdWxlVHlwZX07XG4gICAgICBfcmVzb2x2ZVByb3ZpZGVycyhcbiAgICAgICAgICBbbmdNb2R1bGVQcm92aWRlcl0sIFByb3ZpZGVyQXN0VHlwZS5QdWJsaWNTZXJ2aWNlLCB0cnVlLCBzb3VyY2VTcGFuLCB0aGlzLl9lcnJvcnMsXG4gICAgICAgICAgdGhpcy5fYWxsUHJvdmlkZXJzLCAvKiBpc01vZHVsZSAqLyB0cnVlKTtcbiAgICB9KTtcbiAgICBfcmVzb2x2ZVByb3ZpZGVycyhcbiAgICAgICAgbmdNb2R1bGUudHJhbnNpdGl2ZU1vZHVsZS5wcm92aWRlcnMubWFwKGVudHJ5ID0+IGVudHJ5LnByb3ZpZGVyKS5jb25jYXQoZXh0cmFQcm92aWRlcnMpLFxuICAgICAgICBQcm92aWRlckFzdFR5cGUuUHVibGljU2VydmljZSwgZmFsc2UsIHNvdXJjZVNwYW4sIHRoaXMuX2Vycm9ycywgdGhpcy5fYWxsUHJvdmlkZXJzLFxuICAgICAgICAvKiBpc01vZHVsZSAqLyBmYWxzZSk7XG4gIH1cblxuICBwYXJzZSgpOiBQcm92aWRlckFzdFtdIHtcbiAgICBBcnJheS5mcm9tKHRoaXMuX2FsbFByb3ZpZGVycy52YWx1ZXMoKSkuZm9yRWFjaCgocHJvdmlkZXIpID0+IHtcbiAgICAgIHRoaXMuX2dldE9yQ3JlYXRlTG9jYWxQcm92aWRlcihwcm92aWRlci50b2tlbiwgcHJvdmlkZXIuZWFnZXIpO1xuICAgIH0pO1xuICAgIGlmICh0aGlzLl9lcnJvcnMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgZXJyb3JTdHJpbmcgPSB0aGlzLl9lcnJvcnMuam9pbignXFxuJyk7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYFByb3ZpZGVyIHBhcnNlIGVycm9yczpcXG4ke2Vycm9yU3RyaW5nfWApO1xuICAgIH1cbiAgICAvLyBOb3RlOiBNYXBzIGtlZXAgdGhlaXIgaW5zZXJ0aW9uIG9yZGVyLlxuICAgIGNvbnN0IGxhenlQcm92aWRlcnM6IFByb3ZpZGVyQXN0W10gPSBbXTtcbiAgICBjb25zdCBlYWdlclByb3ZpZGVyczogUHJvdmlkZXJBc3RbXSA9IFtdO1xuICAgIHRoaXMuX3RyYW5zZm9ybWVkUHJvdmlkZXJzLmZvckVhY2gocHJvdmlkZXIgPT4ge1xuICAgICAgaWYgKHByb3ZpZGVyLmVhZ2VyKSB7XG4gICAgICAgIGVhZ2VyUHJvdmlkZXJzLnB1c2gocHJvdmlkZXIpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbGF6eVByb3ZpZGVycy5wdXNoKHByb3ZpZGVyKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gbGF6eVByb3ZpZGVycy5jb25jYXQoZWFnZXJQcm92aWRlcnMpO1xuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0T3JDcmVhdGVMb2NhbFByb3ZpZGVyKHRva2VuOiBDb21waWxlVG9rZW5NZXRhZGF0YSwgZWFnZXI6IGJvb2xlYW4pOiBQcm92aWRlckFzdHxudWxsIHtcbiAgICBjb25zdCByZXNvbHZlZFByb3ZpZGVyID0gdGhpcy5fYWxsUHJvdmlkZXJzLmdldCh0b2tlblJlZmVyZW5jZSh0b2tlbikpO1xuICAgIGlmICghcmVzb2x2ZWRQcm92aWRlcikge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGxldCB0cmFuc2Zvcm1lZFByb3ZpZGVyQXN0ID0gdGhpcy5fdHJhbnNmb3JtZWRQcm92aWRlcnMuZ2V0KHRva2VuUmVmZXJlbmNlKHRva2VuKSk7XG4gICAgaWYgKHRyYW5zZm9ybWVkUHJvdmlkZXJBc3QpIHtcbiAgICAgIHJldHVybiB0cmFuc2Zvcm1lZFByb3ZpZGVyQXN0O1xuICAgIH1cbiAgICBpZiAodGhpcy5fc2VlblByb3ZpZGVycy5nZXQodG9rZW5SZWZlcmVuY2UodG9rZW4pKSAhPSBudWxsKSB7XG4gICAgICB0aGlzLl9lcnJvcnMucHVzaChuZXcgUHJvdmlkZXJFcnJvcihcbiAgICAgICAgICBgQ2Fubm90IGluc3RhbnRpYXRlIGN5Y2xpYyBkZXBlbmRlbmN5ISAke3Rva2VuTmFtZSh0b2tlbil9YCxcbiAgICAgICAgICByZXNvbHZlZFByb3ZpZGVyLnNvdXJjZVNwYW4pKTtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICB0aGlzLl9zZWVuUHJvdmlkZXJzLnNldCh0b2tlblJlZmVyZW5jZSh0b2tlbiksIHRydWUpO1xuICAgIGNvbnN0IHRyYW5zZm9ybWVkUHJvdmlkZXJzID0gcmVzb2x2ZWRQcm92aWRlci5wcm92aWRlcnMubWFwKChwcm92aWRlcikgPT4ge1xuICAgICAgbGV0IHRyYW5zZm9ybWVkVXNlVmFsdWUgPSBwcm92aWRlci51c2VWYWx1ZTtcbiAgICAgIGxldCB0cmFuc2Zvcm1lZFVzZUV4aXN0aW5nID0gcHJvdmlkZXIudXNlRXhpc3RpbmcgITtcbiAgICAgIGxldCB0cmFuc2Zvcm1lZERlcHM6IENvbXBpbGVEaURlcGVuZGVuY3lNZXRhZGF0YVtdID0gdW5kZWZpbmVkICE7XG4gICAgICBpZiAocHJvdmlkZXIudXNlRXhpc3RpbmcgIT0gbnVsbCkge1xuICAgICAgICBjb25zdCBleGlzdGluZ0RpRGVwID1cbiAgICAgICAgICAgIHRoaXMuX2dldERlcGVuZGVuY3koe3Rva2VuOiBwcm92aWRlci51c2VFeGlzdGluZ30sIGVhZ2VyLCByZXNvbHZlZFByb3ZpZGVyLnNvdXJjZVNwYW4pO1xuICAgICAgICBpZiAoZXhpc3RpbmdEaURlcC50b2tlbiAhPSBudWxsKSB7XG4gICAgICAgICAgdHJhbnNmb3JtZWRVc2VFeGlzdGluZyA9IGV4aXN0aW5nRGlEZXAudG9rZW47XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdHJhbnNmb3JtZWRVc2VFeGlzdGluZyA9IG51bGwgITtcbiAgICAgICAgICB0cmFuc2Zvcm1lZFVzZVZhbHVlID0gZXhpc3RpbmdEaURlcC52YWx1ZTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIGlmIChwcm92aWRlci51c2VGYWN0b3J5KSB7XG4gICAgICAgIGNvbnN0IGRlcHMgPSBwcm92aWRlci5kZXBzIHx8IHByb3ZpZGVyLnVzZUZhY3RvcnkuZGlEZXBzO1xuICAgICAgICB0cmFuc2Zvcm1lZERlcHMgPVxuICAgICAgICAgICAgZGVwcy5tYXAoKGRlcCkgPT4gdGhpcy5fZ2V0RGVwZW5kZW5jeShkZXAsIGVhZ2VyLCByZXNvbHZlZFByb3ZpZGVyLnNvdXJjZVNwYW4pKTtcbiAgICAgIH0gZWxzZSBpZiAocHJvdmlkZXIudXNlQ2xhc3MpIHtcbiAgICAgICAgY29uc3QgZGVwcyA9IHByb3ZpZGVyLmRlcHMgfHwgcHJvdmlkZXIudXNlQ2xhc3MuZGlEZXBzO1xuICAgICAgICB0cmFuc2Zvcm1lZERlcHMgPVxuICAgICAgICAgICAgZGVwcy5tYXAoKGRlcCkgPT4gdGhpcy5fZ2V0RGVwZW5kZW5jeShkZXAsIGVhZ2VyLCByZXNvbHZlZFByb3ZpZGVyLnNvdXJjZVNwYW4pKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBfdHJhbnNmb3JtUHJvdmlkZXIocHJvdmlkZXIsIHtcbiAgICAgICAgdXNlRXhpc3Rpbmc6IHRyYW5zZm9ybWVkVXNlRXhpc3RpbmcsXG4gICAgICAgIHVzZVZhbHVlOiB0cmFuc2Zvcm1lZFVzZVZhbHVlLFxuICAgICAgICBkZXBzOiB0cmFuc2Zvcm1lZERlcHNcbiAgICAgIH0pO1xuICAgIH0pO1xuICAgIHRyYW5zZm9ybWVkUHJvdmlkZXJBc3QgPVxuICAgICAgICBfdHJhbnNmb3JtUHJvdmlkZXJBc3QocmVzb2x2ZWRQcm92aWRlciwge2VhZ2VyOiBlYWdlciwgcHJvdmlkZXJzOiB0cmFuc2Zvcm1lZFByb3ZpZGVyc30pO1xuICAgIHRoaXMuX3RyYW5zZm9ybWVkUHJvdmlkZXJzLnNldCh0b2tlblJlZmVyZW5jZSh0b2tlbiksIHRyYW5zZm9ybWVkUHJvdmlkZXJBc3QpO1xuICAgIHJldHVybiB0cmFuc2Zvcm1lZFByb3ZpZGVyQXN0O1xuICB9XG5cbiAgcHJpdmF0ZSBfZ2V0RGVwZW5kZW5jeShcbiAgICAgIGRlcDogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhLCBlYWdlcjogYm9vbGVhbiA9IGZhbHNlLFxuICAgICAgcmVxdWVzdG9yU291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuKTogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhIHtcbiAgICBsZXQgZm91bmRMb2NhbCA9IGZhbHNlO1xuICAgIGlmICghZGVwLmlzU2tpcFNlbGYgJiYgZGVwLnRva2VuICE9IG51bGwpIHtcbiAgICAgIC8vIGFjY2VzcyB0aGUgaW5qZWN0b3JcbiAgICAgIGlmICh0b2tlblJlZmVyZW5jZShkZXAudG9rZW4pID09PVxuICAgICAgICAgICAgICB0aGlzLnJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuSW5qZWN0b3IpIHx8XG4gICAgICAgICAgdG9rZW5SZWZlcmVuY2UoZGVwLnRva2VuKSA9PT1cbiAgICAgICAgICAgICAgdGhpcy5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKElkZW50aWZpZXJzLkNvbXBvbmVudEZhY3RvcnlSZXNvbHZlcikpIHtcbiAgICAgICAgZm91bmRMb2NhbCA9IHRydWU7XG4gICAgICAgIC8vIGFjY2VzcyBwcm92aWRlcnNcbiAgICAgIH0gZWxzZSBpZiAodGhpcy5fZ2V0T3JDcmVhdGVMb2NhbFByb3ZpZGVyKGRlcC50b2tlbiwgZWFnZXIpICE9IG51bGwpIHtcbiAgICAgICAgZm91bmRMb2NhbCA9IHRydWU7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBkZXA7XG4gIH1cbn1cblxuZnVuY3Rpb24gX3RyYW5zZm9ybVByb3ZpZGVyKFxuICAgIHByb3ZpZGVyOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSxcbiAgICB7dXNlRXhpc3RpbmcsIHVzZVZhbHVlLCBkZXBzfTpcbiAgICAgICAge3VzZUV4aXN0aW5nOiBDb21waWxlVG9rZW5NZXRhZGF0YSwgdXNlVmFsdWU6IGFueSwgZGVwczogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhW119KSB7XG4gIHJldHVybiB7XG4gICAgdG9rZW46IHByb3ZpZGVyLnRva2VuLFxuICAgIHVzZUNsYXNzOiBwcm92aWRlci51c2VDbGFzcyxcbiAgICB1c2VFeGlzdGluZzogdXNlRXhpc3RpbmcsXG4gICAgdXNlRmFjdG9yeTogcHJvdmlkZXIudXNlRmFjdG9yeSxcbiAgICB1c2VWYWx1ZTogdXNlVmFsdWUsXG4gICAgZGVwczogZGVwcyxcbiAgICBtdWx0aTogcHJvdmlkZXIubXVsdGlcbiAgfTtcbn1cblxuZnVuY3Rpb24gX3RyYW5zZm9ybVByb3ZpZGVyQXN0KFxuICAgIHByb3ZpZGVyOiBQcm92aWRlckFzdCxcbiAgICB7ZWFnZXIsIHByb3ZpZGVyc306IHtlYWdlcjogYm9vbGVhbiwgcHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdfSk6IFByb3ZpZGVyQXN0IHtcbiAgcmV0dXJuIG5ldyBQcm92aWRlckFzdChcbiAgICAgIHByb3ZpZGVyLnRva2VuLCBwcm92aWRlci5tdWx0aVByb3ZpZGVyLCBwcm92aWRlci5lYWdlciB8fCBlYWdlciwgcHJvdmlkZXJzLFxuICAgICAgcHJvdmlkZXIucHJvdmlkZXJUeXBlLCBwcm92aWRlci5saWZlY3ljbGVIb29rcywgcHJvdmlkZXIuc291cmNlU3BhbiwgcHJvdmlkZXIuaXNNb2R1bGUpO1xufVxuXG5mdW5jdGlvbiBfcmVzb2x2ZVByb3ZpZGVyc0Zyb21EaXJlY3RpdmVzKFxuICAgIGRpcmVjdGl2ZXM6IENvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5W10sIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhbixcbiAgICB0YXJnZXRFcnJvcnM6IFBhcnNlRXJyb3JbXSk6IE1hcDxhbnksIFByb3ZpZGVyQXN0PiB7XG4gIGNvbnN0IHByb3ZpZGVyc0J5VG9rZW4gPSBuZXcgTWFwPGFueSwgUHJvdmlkZXJBc3Q+KCk7XG4gIGRpcmVjdGl2ZXMuZm9yRWFjaCgoZGlyZWN0aXZlKSA9PiB7XG4gICAgY29uc3QgZGlyUHJvdmlkZXI6XG4gICAgICAgIENvbXBpbGVQcm92aWRlck1ldGFkYXRhID0ge3Rva2VuOiB7aWRlbnRpZmllcjogZGlyZWN0aXZlLnR5cGV9LCB1c2VDbGFzczogZGlyZWN0aXZlLnR5cGV9O1xuICAgIF9yZXNvbHZlUHJvdmlkZXJzKFxuICAgICAgICBbZGlyUHJvdmlkZXJdLFxuICAgICAgICBkaXJlY3RpdmUuaXNDb21wb25lbnQgPyBQcm92aWRlckFzdFR5cGUuQ29tcG9uZW50IDogUHJvdmlkZXJBc3RUeXBlLkRpcmVjdGl2ZSwgdHJ1ZSxcbiAgICAgICAgc291cmNlU3BhbiwgdGFyZ2V0RXJyb3JzLCBwcm92aWRlcnNCeVRva2VuLCAvKiBpc01vZHVsZSAqLyBmYWxzZSk7XG4gIH0pO1xuXG4gIC8vIE5vdGU6IGRpcmVjdGl2ZXMgbmVlZCB0byBiZSBhYmxlIHRvIG92ZXJ3cml0ZSBwcm92aWRlcnMgb2YgYSBjb21wb25lbnQhXG4gIGNvbnN0IGRpcmVjdGl2ZXNXaXRoQ29tcG9uZW50Rmlyc3QgPVxuICAgICAgZGlyZWN0aXZlcy5maWx0ZXIoZGlyID0+IGRpci5pc0NvbXBvbmVudCkuY29uY2F0KGRpcmVjdGl2ZXMuZmlsdGVyKGRpciA9PiAhZGlyLmlzQ29tcG9uZW50KSk7XG4gIGRpcmVjdGl2ZXNXaXRoQ29tcG9uZW50Rmlyc3QuZm9yRWFjaCgoZGlyZWN0aXZlKSA9PiB7XG4gICAgX3Jlc29sdmVQcm92aWRlcnMoXG4gICAgICAgIGRpcmVjdGl2ZS5wcm92aWRlcnMsIFByb3ZpZGVyQXN0VHlwZS5QdWJsaWNTZXJ2aWNlLCBmYWxzZSwgc291cmNlU3BhbiwgdGFyZ2V0RXJyb3JzLFxuICAgICAgICBwcm92aWRlcnNCeVRva2VuLCAvKiBpc01vZHVsZSAqLyBmYWxzZSk7XG4gICAgX3Jlc29sdmVQcm92aWRlcnMoXG4gICAgICAgIGRpcmVjdGl2ZS52aWV3UHJvdmlkZXJzLCBQcm92aWRlckFzdFR5cGUuUHJpdmF0ZVNlcnZpY2UsIGZhbHNlLCBzb3VyY2VTcGFuLCB0YXJnZXRFcnJvcnMsXG4gICAgICAgIHByb3ZpZGVyc0J5VG9rZW4sIC8qIGlzTW9kdWxlICovIGZhbHNlKTtcbiAgfSk7XG4gIHJldHVybiBwcm92aWRlcnNCeVRva2VuO1xufVxuXG5mdW5jdGlvbiBfcmVzb2x2ZVByb3ZpZGVycyhcbiAgICBwcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW10sIHByb3ZpZGVyVHlwZTogUHJvdmlkZXJBc3RUeXBlLCBlYWdlcjogYm9vbGVhbixcbiAgICBzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW4sIHRhcmdldEVycm9yczogUGFyc2VFcnJvcltdLFxuICAgIHRhcmdldFByb3ZpZGVyc0J5VG9rZW46IE1hcDxhbnksIFByb3ZpZGVyQXN0PiwgaXNNb2R1bGU6IGJvb2xlYW4pIHtcbiAgcHJvdmlkZXJzLmZvckVhY2goKHByb3ZpZGVyKSA9PiB7XG4gICAgbGV0IHJlc29sdmVkUHJvdmlkZXIgPSB0YXJnZXRQcm92aWRlcnNCeVRva2VuLmdldCh0b2tlblJlZmVyZW5jZShwcm92aWRlci50b2tlbikpO1xuICAgIGlmIChyZXNvbHZlZFByb3ZpZGVyICE9IG51bGwgJiYgISFyZXNvbHZlZFByb3ZpZGVyLm11bHRpUHJvdmlkZXIgIT09ICEhcHJvdmlkZXIubXVsdGkpIHtcbiAgICAgIHRhcmdldEVycm9ycy5wdXNoKG5ldyBQcm92aWRlckVycm9yKFxuICAgICAgICAgIGBNaXhpbmcgbXVsdGkgYW5kIG5vbiBtdWx0aSBwcm92aWRlciBpcyBub3QgcG9zc2libGUgZm9yIHRva2VuICR7dG9rZW5OYW1lKHJlc29sdmVkUHJvdmlkZXIudG9rZW4pfWAsXG4gICAgICAgICAgc291cmNlU3BhbikpO1xuICAgIH1cbiAgICBpZiAoIXJlc29sdmVkUHJvdmlkZXIpIHtcbiAgICAgIGNvbnN0IGxpZmVjeWNsZUhvb2tzID0gcHJvdmlkZXIudG9rZW4uaWRlbnRpZmllciAmJlxuICAgICAgICAgICAgICAoPENvbXBpbGVUeXBlTWV0YWRhdGE+cHJvdmlkZXIudG9rZW4uaWRlbnRpZmllcikubGlmZWN5Y2xlSG9va3MgP1xuICAgICAgICAgICg8Q29tcGlsZVR5cGVNZXRhZGF0YT5wcm92aWRlci50b2tlbi5pZGVudGlmaWVyKS5saWZlY3ljbGVIb29rcyA6XG4gICAgICAgICAgW107XG4gICAgICBjb25zdCBpc1VzZVZhbHVlID0gIShwcm92aWRlci51c2VDbGFzcyB8fCBwcm92aWRlci51c2VFeGlzdGluZyB8fCBwcm92aWRlci51c2VGYWN0b3J5KTtcbiAgICAgIHJlc29sdmVkUHJvdmlkZXIgPSBuZXcgUHJvdmlkZXJBc3QoXG4gICAgICAgICAgcHJvdmlkZXIudG9rZW4sICEhcHJvdmlkZXIubXVsdGksIGVhZ2VyIHx8IGlzVXNlVmFsdWUsIFtwcm92aWRlcl0sIHByb3ZpZGVyVHlwZSxcbiAgICAgICAgICBsaWZlY3ljbGVIb29rcywgc291cmNlU3BhbiwgaXNNb2R1bGUpO1xuICAgICAgdGFyZ2V0UHJvdmlkZXJzQnlUb2tlbi5zZXQodG9rZW5SZWZlcmVuY2UocHJvdmlkZXIudG9rZW4pLCByZXNvbHZlZFByb3ZpZGVyKTtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKCFwcm92aWRlci5tdWx0aSkge1xuICAgICAgICByZXNvbHZlZFByb3ZpZGVyLnByb3ZpZGVycy5sZW5ndGggPSAwO1xuICAgICAgfVxuICAgICAgcmVzb2x2ZWRQcm92aWRlci5wcm92aWRlcnMucHVzaChwcm92aWRlcik7XG4gICAgfVxuICB9KTtcbn1cblxuXG5mdW5jdGlvbiBfZ2V0Vmlld1F1ZXJpZXMoY29tcG9uZW50OiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEpOiBNYXA8YW55LCBRdWVyeVdpdGhJZFtdPiB7XG4gIC8vIE5vdGU6IHF1ZXJpZXMgc3RhcnQgd2l0aCBpZCAxIHNvIHdlIGNhbiB1c2UgdGhlIG51bWJlciBpbiBhIEJsb29tIGZpbHRlciFcbiAgbGV0IHZpZXdRdWVyeUlkID0gMTtcbiAgY29uc3Qgdmlld1F1ZXJpZXMgPSBuZXcgTWFwPGFueSwgUXVlcnlXaXRoSWRbXT4oKTtcbiAgaWYgKGNvbXBvbmVudC52aWV3UXVlcmllcykge1xuICAgIGNvbXBvbmVudC52aWV3UXVlcmllcy5mb3JFYWNoKFxuICAgICAgICAocXVlcnkpID0+IF9hZGRRdWVyeVRvVG9rZW5NYXAodmlld1F1ZXJpZXMsIHttZXRhOiBxdWVyeSwgcXVlcnlJZDogdmlld1F1ZXJ5SWQrK30pKTtcbiAgfVxuICByZXR1cm4gdmlld1F1ZXJpZXM7XG59XG5cbmZ1bmN0aW9uIF9nZXRDb250ZW50UXVlcmllcyhcbiAgICBjb250ZW50UXVlcnlTdGFydElkOiBudW1iZXIsIGRpcmVjdGl2ZXM6IENvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5W10pOiBNYXA8YW55LCBRdWVyeVdpdGhJZFtdPiB7XG4gIGxldCBjb250ZW50UXVlcnlJZCA9IGNvbnRlbnRRdWVyeVN0YXJ0SWQ7XG4gIGNvbnN0IGNvbnRlbnRRdWVyaWVzID0gbmV3IE1hcDxhbnksIFF1ZXJ5V2l0aElkW10+KCk7XG4gIGRpcmVjdGl2ZXMuZm9yRWFjaCgoZGlyZWN0aXZlLCBkaXJlY3RpdmVJbmRleCkgPT4ge1xuICAgIGlmIChkaXJlY3RpdmUucXVlcmllcykge1xuICAgICAgZGlyZWN0aXZlLnF1ZXJpZXMuZm9yRWFjaChcbiAgICAgICAgICAocXVlcnkpID0+IF9hZGRRdWVyeVRvVG9rZW5NYXAoY29udGVudFF1ZXJpZXMsIHttZXRhOiBxdWVyeSwgcXVlcnlJZDogY29udGVudFF1ZXJ5SWQrK30pKTtcbiAgICB9XG4gIH0pO1xuICByZXR1cm4gY29udGVudFF1ZXJpZXM7XG59XG5cbmZ1bmN0aW9uIF9hZGRRdWVyeVRvVG9rZW5NYXAobWFwOiBNYXA8YW55LCBRdWVyeVdpdGhJZFtdPiwgcXVlcnk6IFF1ZXJ5V2l0aElkKSB7XG4gIHF1ZXJ5Lm1ldGEuc2VsZWN0b3JzLmZvckVhY2goKHRva2VuOiBDb21waWxlVG9rZW5NZXRhZGF0YSkgPT4ge1xuICAgIGxldCBlbnRyeSA9IG1hcC5nZXQodG9rZW5SZWZlcmVuY2UodG9rZW4pKTtcbiAgICBpZiAoIWVudHJ5KSB7XG4gICAgICBlbnRyeSA9IFtdO1xuICAgICAgbWFwLnNldCh0b2tlblJlZmVyZW5jZSh0b2tlbiksIGVudHJ5KTtcbiAgICB9XG4gICAgZW50cnkucHVzaChxdWVyeSk7XG4gIH0pO1xufVxuIl19