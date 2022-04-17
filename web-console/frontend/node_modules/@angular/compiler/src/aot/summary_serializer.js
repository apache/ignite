(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/aot/summary_serializer", ["require", "exports", "tslib", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/util", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/aot/static_symbol_resolver", "@angular/compiler/src/aot/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var o = require("@angular/compiler/src/output/output_ast");
    var util_1 = require("@angular/compiler/src/util");
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var static_symbol_resolver_1 = require("@angular/compiler/src/aot/static_symbol_resolver");
    var util_2 = require("@angular/compiler/src/aot/util");
    function serializeSummaries(srcFileName, forJitCtx, summaryResolver, symbolResolver, symbols, types, createExternalSymbolReexports) {
        if (createExternalSymbolReexports === void 0) { createExternalSymbolReexports = false; }
        var toJsonSerializer = new ToJsonSerializer(symbolResolver, summaryResolver, srcFileName);
        // for symbols, we use everything except for the class metadata itself
        // (we keep the statics though), as the class metadata is contained in the
        // CompileTypeSummary.
        symbols.forEach(function (resolvedSymbol) { return toJsonSerializer.addSummary({ symbol: resolvedSymbol.symbol, metadata: resolvedSymbol.metadata }); });
        // Add type summaries.
        types.forEach(function (_a) {
            var summary = _a.summary, metadata = _a.metadata;
            toJsonSerializer.addSummary({ symbol: summary.type.reference, metadata: undefined, type: summary });
        });
        var _a = toJsonSerializer.serialize(createExternalSymbolReexports), json = _a.json, exportAs = _a.exportAs;
        if (forJitCtx) {
            var forJitSerializer_1 = new ForJitSerializer(forJitCtx, symbolResolver, summaryResolver);
            types.forEach(function (_a) {
                var summary = _a.summary, metadata = _a.metadata;
                forJitSerializer_1.addSourceType(summary, metadata);
            });
            toJsonSerializer.unprocessedSymbolSummariesBySymbol.forEach(function (summary) {
                if (summaryResolver.isLibraryFile(summary.symbol.filePath) && summary.type) {
                    forJitSerializer_1.addLibType(summary.type);
                }
            });
            forJitSerializer_1.serialize(exportAs);
        }
        return { json: json, exportAs: exportAs };
    }
    exports.serializeSummaries = serializeSummaries;
    function deserializeSummaries(symbolCache, summaryResolver, libraryFileName, json) {
        var deserializer = new FromJsonDeserializer(symbolCache, summaryResolver);
        return deserializer.deserialize(libraryFileName, json);
    }
    exports.deserializeSummaries = deserializeSummaries;
    function createForJitStub(outputCtx, reference) {
        return createSummaryForJitFunction(outputCtx, reference, o.NULL_EXPR);
    }
    exports.createForJitStub = createForJitStub;
    function createSummaryForJitFunction(outputCtx, reference, value) {
        var fnName = util_2.summaryForJitName(reference.name);
        outputCtx.statements.push(o.fn([], [new o.ReturnStatement(value)], new o.ArrayType(o.DYNAMIC_TYPE)).toDeclStmt(fnName, [
            o.StmtModifier.Final, o.StmtModifier.Exported
        ]));
    }
    var ToJsonSerializer = /** @class */ (function (_super) {
        tslib_1.__extends(ToJsonSerializer, _super);
        function ToJsonSerializer(symbolResolver, summaryResolver, srcFileName) {
            var _this = _super.call(this) || this;
            _this.symbolResolver = symbolResolver;
            _this.summaryResolver = summaryResolver;
            _this.srcFileName = srcFileName;
            // Note: This only contains symbols without members.
            _this.symbols = [];
            _this.indexBySymbol = new Map();
            _this.reexportedBy = new Map();
            // This now contains a `__symbol: number` in the place of
            // StaticSymbols, but otherwise has the same shape as the original objects.
            _this.processedSummaryBySymbol = new Map();
            _this.processedSummaries = [];
            _this.unprocessedSymbolSummariesBySymbol = new Map();
            _this.moduleName = symbolResolver.getKnownModuleName(srcFileName);
            return _this;
        }
        ToJsonSerializer.prototype.addSummary = function (summary) {
            var _this = this;
            var unprocessedSummary = this.unprocessedSymbolSummariesBySymbol.get(summary.symbol);
            var processedSummary = this.processedSummaryBySymbol.get(summary.symbol);
            if (!unprocessedSummary) {
                unprocessedSummary = { symbol: summary.symbol, metadata: undefined };
                this.unprocessedSymbolSummariesBySymbol.set(summary.symbol, unprocessedSummary);
                processedSummary = { symbol: this.processValue(summary.symbol, 0 /* None */) };
                this.processedSummaries.push(processedSummary);
                this.processedSummaryBySymbol.set(summary.symbol, processedSummary);
            }
            if (!unprocessedSummary.metadata && summary.metadata) {
                var metadata_1 = summary.metadata || {};
                if (metadata_1.__symbolic === 'class') {
                    // For classes, we keep everything except their class decorators.
                    // We need to keep e.g. the ctor args, method names, method decorators
                    // so that the class can be extended in another compilation unit.
                    // We don't keep the class decorators as
                    // 1) they refer to data
                    //   that should not cause a rebuild of downstream compilation units
                    //   (e.g. inline templates of @Component, or @NgModule.declarations)
                    // 2) their data is already captured in TypeSummaries, e.g. DirectiveSummary.
                    var clone_1 = {};
                    Object.keys(metadata_1).forEach(function (propName) {
                        if (propName !== 'decorators') {
                            clone_1[propName] = metadata_1[propName];
                        }
                    });
                    metadata_1 = clone_1;
                }
                else if (isCall(metadata_1)) {
                    if (!isFunctionCall(metadata_1) && !isMethodCallOnVariable(metadata_1)) {
                        // Don't store complex calls as we won't be able to simplify them anyways later on.
                        metadata_1 = {
                            __symbolic: 'error',
                            message: 'Complex function calls are not supported.',
                        };
                    }
                }
                // Note: We need to keep storing ctor calls for e.g.
                // `export const x = new InjectionToken(...)`
                unprocessedSummary.metadata = metadata_1;
                processedSummary.metadata = this.processValue(metadata_1, 1 /* ResolveValue */);
                if (metadata_1 instanceof static_symbol_1.StaticSymbol &&
                    this.summaryResolver.isLibraryFile(metadata_1.filePath)) {
                    var declarationSymbol = this.symbols[this.indexBySymbol.get(metadata_1)];
                    if (!util_2.isLoweredSymbol(declarationSymbol.name)) {
                        // Note: symbols that were introduced during codegen in the user file can have a reexport
                        // if a user used `export *`. However, we can't rely on this as tsickle will change
                        // `export *` into named exports, using only the information from the typechecker.
                        // As we introduce the new symbols after typecheck, Tsickle does not know about them,
                        // and omits them when expanding `export *`.
                        // So we have to keep reexporting these symbols manually via .ngfactory files.
                        this.reexportedBy.set(declarationSymbol, summary.symbol);
                    }
                }
            }
            if (!unprocessedSummary.type && summary.type) {
                unprocessedSummary.type = summary.type;
                // Note: We don't add the summaries of all referenced symbols as for the ResolvedSymbols,
                // as the type summaries already contain the transitive data that they require
                // (in a minimal way).
                processedSummary.type = this.processValue(summary.type, 0 /* None */);
                // except for reexported directives / pipes, so we need to store
                // their summaries explicitly.
                if (summary.type.summaryKind === compile_metadata_1.CompileSummaryKind.NgModule) {
                    var ngModuleSummary = summary.type;
                    ngModuleSummary.exportedDirectives.concat(ngModuleSummary.exportedPipes).forEach(function (id) {
                        var symbol = id.reference;
                        if (_this.summaryResolver.isLibraryFile(symbol.filePath) &&
                            !_this.unprocessedSymbolSummariesBySymbol.has(symbol)) {
                            var summary_1 = _this.summaryResolver.resolveSummary(symbol);
                            if (summary_1) {
                                _this.addSummary(summary_1);
                            }
                        }
                    });
                }
            }
        };
        /**
         * @param createExternalSymbolReexports Whether external static symbols should be re-exported.
         * This can be enabled if external symbols should be re-exported by the current module in
         * order to avoid dynamically generated module dependencies which can break strict dependency
         * enforcements (as in Google3). Read more here: https://github.com/angular/angular/issues/25644
         */
        ToJsonSerializer.prototype.serialize = function (createExternalSymbolReexports) {
            var _this = this;
            var exportAs = [];
            var json = JSON.stringify({
                moduleName: this.moduleName,
                summaries: this.processedSummaries,
                symbols: this.symbols.map(function (symbol, index) {
                    symbol.assertNoMembers();
                    var importAs = undefined;
                    if (_this.summaryResolver.isLibraryFile(symbol.filePath)) {
                        var reexportSymbol = _this.reexportedBy.get(symbol);
                        if (reexportSymbol) {
                            // In case the given external static symbol is already manually exported by the
                            // user, we just proxy the external static symbol reference to the manual export.
                            // This ensures that the AOT compiler imports the external symbol through the
                            // user export and does not introduce another dependency which is not needed.
                            importAs = _this.indexBySymbol.get(reexportSymbol);
                        }
                        else if (createExternalSymbolReexports) {
                            // In this case, the given external static symbol is *not* manually exported by
                            // the user, and we manually create a re-export in the factory file so that we
                            // don't introduce another module dependency. This is useful when running within
                            // Bazel so that the AOT compiler does not introduce any module dependencies
                            // which can break the strict dependency enforcement. (e.g. as in Google3)
                            // Read more about this here: https://github.com/angular/angular/issues/25644
                            var summary = _this.unprocessedSymbolSummariesBySymbol.get(symbol);
                            if (!summary || !summary.metadata || summary.metadata.__symbolic !== 'interface') {
                                importAs = symbol.name + "_" + index;
                                exportAs.push({ symbol: symbol, exportAs: importAs });
                            }
                        }
                    }
                    return {
                        __symbol: index,
                        name: symbol.name,
                        filePath: _this.summaryResolver.toSummaryFileName(symbol.filePath, _this.srcFileName),
                        importAs: importAs
                    };
                })
            });
            return { json: json, exportAs: exportAs };
        };
        ToJsonSerializer.prototype.processValue = function (value, flags) {
            return util_1.visitValue(value, this, flags);
        };
        ToJsonSerializer.prototype.visitOther = function (value, context) {
            if (value instanceof static_symbol_1.StaticSymbol) {
                var baseSymbol = this.symbolResolver.getStaticSymbol(value.filePath, value.name);
                var index = this.visitStaticSymbol(baseSymbol, context);
                return { __symbol: index, members: value.members };
            }
        };
        /**
         * Strip line and character numbers from ngsummaries.
         * Emitting them causes white spaces changes to retrigger upstream
         * recompilations in bazel.
         * TODO: find out a way to have line and character numbers in errors without
         * excessive recompilation in bazel.
         */
        ToJsonSerializer.prototype.visitStringMap = function (map, context) {
            if (map['__symbolic'] === 'resolved') {
                return util_1.visitValue(map['symbol'], this, context);
            }
            if (map['__symbolic'] === 'error') {
                delete map['line'];
                delete map['character'];
            }
            return _super.prototype.visitStringMap.call(this, map, context);
        };
        /**
         * Returns null if the options.resolveValue is true, and the summary for the symbol
         * resolved to a type or could not be resolved.
         */
        ToJsonSerializer.prototype.visitStaticSymbol = function (baseSymbol, flags) {
            var index = this.indexBySymbol.get(baseSymbol);
            var summary = null;
            if (flags & 1 /* ResolveValue */ &&
                this.summaryResolver.isLibraryFile(baseSymbol.filePath)) {
                if (this.unprocessedSymbolSummariesBySymbol.has(baseSymbol)) {
                    // the summary for this symbol was already added
                    // -> nothing to do.
                    return index;
                }
                summary = this.loadSummary(baseSymbol);
                if (summary && summary.metadata instanceof static_symbol_1.StaticSymbol) {
                    // The summary is a reexport
                    index = this.visitStaticSymbol(summary.metadata, flags);
                    // reset the summary as it is just a reexport, so we don't want to store it.
                    summary = null;
                }
            }
            else if (index != null) {
                // Note: == on purpose to compare with undefined!
                // No summary and the symbol is already added -> nothing to do.
                return index;
            }
            // Note: == on purpose to compare with undefined!
            if (index == null) {
                index = this.symbols.length;
                this.symbols.push(baseSymbol);
            }
            this.indexBySymbol.set(baseSymbol, index);
            if (summary) {
                this.addSummary(summary);
            }
            return index;
        };
        ToJsonSerializer.prototype.loadSummary = function (symbol) {
            var summary = this.summaryResolver.resolveSummary(symbol);
            if (!summary) {
                // some symbols might originate from a plain typescript library
                // that just exported .d.ts and .metadata.json files, i.e. where no summary
                // files were created.
                var resolvedSymbol = this.symbolResolver.resolveSymbol(symbol);
                if (resolvedSymbol) {
                    summary = { symbol: resolvedSymbol.symbol, metadata: resolvedSymbol.metadata };
                }
            }
            return summary;
        };
        return ToJsonSerializer;
    }(util_1.ValueTransformer));
    var ForJitSerializer = /** @class */ (function () {
        function ForJitSerializer(outputCtx, symbolResolver, summaryResolver) {
            this.outputCtx = outputCtx;
            this.symbolResolver = symbolResolver;
            this.summaryResolver = summaryResolver;
            this.data = [];
        }
        ForJitSerializer.prototype.addSourceType = function (summary, metadata) {
            this.data.push({ summary: summary, metadata: metadata, isLibrary: false });
        };
        ForJitSerializer.prototype.addLibType = function (summary) {
            this.data.push({ summary: summary, metadata: null, isLibrary: true });
        };
        ForJitSerializer.prototype.serialize = function (exportAsArr) {
            var e_1, _a, e_2, _b, e_3, _c;
            var _this = this;
            var exportAsBySymbol = new Map();
            try {
                for (var exportAsArr_1 = tslib_1.__values(exportAsArr), exportAsArr_1_1 = exportAsArr_1.next(); !exportAsArr_1_1.done; exportAsArr_1_1 = exportAsArr_1.next()) {
                    var _d = exportAsArr_1_1.value, symbol = _d.symbol, exportAs = _d.exportAs;
                    exportAsBySymbol.set(symbol, exportAs);
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (exportAsArr_1_1 && !exportAsArr_1_1.done && (_a = exportAsArr_1.return)) _a.call(exportAsArr_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
            var ngModuleSymbols = new Set();
            try {
                for (var _e = tslib_1.__values(this.data), _f = _e.next(); !_f.done; _f = _e.next()) {
                    var _g = _f.value, summary = _g.summary, metadata = _g.metadata, isLibrary = _g.isLibrary;
                    if (summary.summaryKind === compile_metadata_1.CompileSummaryKind.NgModule) {
                        // collect the symbols that refer to NgModule classes.
                        // Note: we can't just rely on `summary.type.summaryKind` to determine this as
                        // we don't add the summaries of all referenced symbols when we serialize type summaries.
                        // See serializeSummaries for details.
                        ngModuleSymbols.add(summary.type.reference);
                        var modSummary = summary;
                        try {
                            for (var _h = (e_3 = void 0, tslib_1.__values(modSummary.modules)), _j = _h.next(); !_j.done; _j = _h.next()) {
                                var mod = _j.value;
                                ngModuleSymbols.add(mod.reference);
                            }
                        }
                        catch (e_3_1) { e_3 = { error: e_3_1 }; }
                        finally {
                            try {
                                if (_j && !_j.done && (_c = _h.return)) _c.call(_h);
                            }
                            finally { if (e_3) throw e_3.error; }
                        }
                    }
                    if (!isLibrary) {
                        var fnName = util_2.summaryForJitName(summary.type.reference.name);
                        createSummaryForJitFunction(this.outputCtx, summary.type.reference, this.serializeSummaryWithDeps(summary, metadata));
                    }
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (_f && !_f.done && (_b = _e.return)) _b.call(_e);
                }
                finally { if (e_2) throw e_2.error; }
            }
            ngModuleSymbols.forEach(function (ngModuleSymbol) {
                if (_this.summaryResolver.isLibraryFile(ngModuleSymbol.filePath)) {
                    var exportAs = exportAsBySymbol.get(ngModuleSymbol) || ngModuleSymbol.name;
                    var jitExportAsName = util_2.summaryForJitName(exportAs);
                    _this.outputCtx.statements.push(o.variable(jitExportAsName)
                        .set(_this.serializeSummaryRef(ngModuleSymbol))
                        .toDeclStmt(null, [o.StmtModifier.Exported]));
                }
            });
        };
        ForJitSerializer.prototype.serializeSummaryWithDeps = function (summary, metadata) {
            var _this = this;
            var expressions = [this.serializeSummary(summary)];
            var providers = [];
            if (metadata instanceof compile_metadata_1.CompileNgModuleMetadata) {
                expressions.push.apply(expressions, tslib_1.__spread(
                // For directives / pipes, we only add the declared ones,
                // and rely on transitively importing NgModules to get the transitive
                // summaries.
                metadata.declaredDirectives.concat(metadata.declaredPipes)
                    .map(function (type) { return type.reference; })
                    // For modules,
                    // we also add the summaries for modules
                    // from libraries.
                    // This is ok as we produce reexports for all transitive modules.
                    .concat(metadata.transitiveModule.modules.map(function (type) { return type.reference; })
                    .filter(function (ref) { return ref !== metadata.type.reference; }))
                    .map(function (ref) { return _this.serializeSummaryRef(ref); })));
                // Note: We don't use `NgModuleSummary.providers`, as that one is transitive,
                // and we already have transitive modules.
                providers = metadata.providers;
            }
            else if (summary.summaryKind === compile_metadata_1.CompileSummaryKind.Directive) {
                var dirSummary = summary;
                providers = dirSummary.providers.concat(dirSummary.viewProviders);
            }
            // Note: We can't just refer to the `ngsummary.ts` files for `useClass` providers (as we do for
            // declaredDirectives / declaredPipes), as we allow
            // providers without ctor arguments to skip the `@Injectable` decorator,
            // i.e. we didn't generate .ngsummary.ts files for these.
            expressions.push.apply(expressions, tslib_1.__spread(providers.filter(function (provider) { return !!provider.useClass; }).map(function (provider) { return _this.serializeSummary({
                summaryKind: compile_metadata_1.CompileSummaryKind.Injectable, type: provider.useClass
            }); })));
            return o.literalArr(expressions);
        };
        ForJitSerializer.prototype.serializeSummaryRef = function (typeSymbol) {
            var jitImportedSymbol = this.symbolResolver.getStaticSymbol(util_2.summaryForJitFileName(typeSymbol.filePath), util_2.summaryForJitName(typeSymbol.name));
            return this.outputCtx.importExpr(jitImportedSymbol);
        };
        ForJitSerializer.prototype.serializeSummary = function (data) {
            var outputCtx = this.outputCtx;
            var Transformer = /** @class */ (function () {
                function Transformer() {
                }
                Transformer.prototype.visitArray = function (arr, context) {
                    var _this = this;
                    return o.literalArr(arr.map(function (entry) { return util_1.visitValue(entry, _this, context); }));
                };
                Transformer.prototype.visitStringMap = function (map, context) {
                    var _this = this;
                    return new o.LiteralMapExpr(Object.keys(map).map(function (key) { return new o.LiteralMapEntry(key, util_1.visitValue(map[key], _this, context), false); }));
                };
                Transformer.prototype.visitPrimitive = function (value, context) { return o.literal(value); };
                Transformer.prototype.visitOther = function (value, context) {
                    if (value instanceof static_symbol_1.StaticSymbol) {
                        return outputCtx.importExpr(value);
                    }
                    else {
                        throw new Error("Illegal State: Encountered value " + value);
                    }
                };
                return Transformer;
            }());
            return util_1.visitValue(data, new Transformer(), null);
        };
        return ForJitSerializer;
    }());
    var FromJsonDeserializer = /** @class */ (function (_super) {
        tslib_1.__extends(FromJsonDeserializer, _super);
        function FromJsonDeserializer(symbolCache, summaryResolver) {
            var _this = _super.call(this) || this;
            _this.symbolCache = symbolCache;
            _this.summaryResolver = summaryResolver;
            return _this;
        }
        FromJsonDeserializer.prototype.deserialize = function (libraryFileName, json) {
            var _this = this;
            var data = JSON.parse(json);
            var allImportAs = [];
            this.symbols = data.symbols.map(function (serializedSymbol) { return _this.symbolCache.get(_this.summaryResolver.fromSummaryFileName(serializedSymbol.filePath, libraryFileName), serializedSymbol.name); });
            data.symbols.forEach(function (serializedSymbol, index) {
                var symbol = _this.symbols[index];
                var importAs = serializedSymbol.importAs;
                if (typeof importAs === 'number') {
                    allImportAs.push({ symbol: symbol, importAs: _this.symbols[importAs] });
                }
                else if (typeof importAs === 'string') {
                    allImportAs.push({ symbol: symbol, importAs: _this.symbolCache.get(util_2.ngfactoryFilePath(libraryFileName), importAs) });
                }
            });
            var summaries = util_1.visitValue(data.summaries, this, null);
            return { moduleName: data.moduleName, summaries: summaries, importAs: allImportAs };
        };
        FromJsonDeserializer.prototype.visitStringMap = function (map, context) {
            if ('__symbol' in map) {
                var baseSymbol = this.symbols[map['__symbol']];
                var members = map['members'];
                return members.length ? this.symbolCache.get(baseSymbol.filePath, baseSymbol.name, members) :
                    baseSymbol;
            }
            else {
                return _super.prototype.visitStringMap.call(this, map, context);
            }
        };
        return FromJsonDeserializer;
    }(util_1.ValueTransformer));
    function isCall(metadata) {
        return metadata && metadata.__symbolic === 'call';
    }
    function isFunctionCall(metadata) {
        return isCall(metadata) && static_symbol_resolver_1.unwrapResolvedMetadata(metadata.expression) instanceof static_symbol_1.StaticSymbol;
    }
    function isMethodCallOnVariable(metadata) {
        return isCall(metadata) && metadata.expression && metadata.expression.__symbolic === 'select' &&
            static_symbol_resolver_1.unwrapResolvedMetadata(metadata.expression.expression) instanceof static_symbol_1.StaticSymbol;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3VtbWFyeV9zZXJpYWxpemVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2FvdC9zdW1tYXJ5X3NlcmlhbGl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7O0lBQUE7Ozs7OztPQU1HO0lBQ0gsMkVBQWtQO0lBQ2xQLDJEQUEwQztJQUUxQyxtREFBa0Y7SUFFbEYseUVBQWdFO0lBQ2hFLDJGQUE0RztJQUM1Ryx1REFBb0c7SUFFcEcsU0FBZ0Isa0JBQWtCLENBQzlCLFdBQW1CLEVBQUUsU0FBK0IsRUFDcEQsZUFBOEMsRUFBRSxjQUFvQyxFQUNwRixPQUErQixFQUFFLEtBSTlCLEVBQ0gsNkJBQ1M7UUFEVCw4Q0FBQSxFQUFBLHFDQUNTO1FBQ1gsSUFBTSxnQkFBZ0IsR0FBRyxJQUFJLGdCQUFnQixDQUFDLGNBQWMsRUFBRSxlQUFlLEVBQUUsV0FBVyxDQUFDLENBQUM7UUFFNUYsc0VBQXNFO1FBQ3RFLDBFQUEwRTtRQUMxRSxzQkFBc0I7UUFDdEIsT0FBTyxDQUFDLE9BQU8sQ0FDWCxVQUFDLGNBQWMsSUFBSyxPQUFBLGdCQUFnQixDQUFDLFVBQVUsQ0FDM0MsRUFBQyxNQUFNLEVBQUUsY0FBYyxDQUFDLE1BQU0sRUFBRSxRQUFRLEVBQUUsY0FBYyxDQUFDLFFBQVEsRUFBQyxDQUFDLEVBRG5ELENBQ21ELENBQUMsQ0FBQztRQUU3RSxzQkFBc0I7UUFDdEIsS0FBSyxDQUFDLE9BQU8sQ0FBQyxVQUFDLEVBQW1CO2dCQUFsQixvQkFBTyxFQUFFLHNCQUFRO1lBQy9CLGdCQUFnQixDQUFDLFVBQVUsQ0FDdkIsRUFBQyxNQUFNLEVBQUUsT0FBTyxDQUFDLElBQUksQ0FBQyxTQUFTLEVBQUUsUUFBUSxFQUFFLFNBQVMsRUFBRSxJQUFJLEVBQUUsT0FBTyxFQUFDLENBQUMsQ0FBQztRQUM1RSxDQUFDLENBQUMsQ0FBQztRQUNHLElBQUEsOERBQTRF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QixDQUFDLFFBQWE7UUFDM0MsT0FBTyxNQUFNLENBQUMsUUFBUSxDQUFDLElBQUksUUFBUSxDQUFDLFVBQVUsSUFBSSxRQUFRLENBQUMsVUFBVSxDQUFDLFVBQVUsS0FBSyxRQUFRO1lBQ3pGLCtDQUFzQixDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsVUFBVSxDQUFDLFlBQVksNEJBQVksQ0FBQztJQUNyRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuaW1wb3J0IHtDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsIENvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5LCBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSwgQ29tcGlsZU5nTW9kdWxlU3VtbWFyeSwgQ29tcGlsZVBpcGVNZXRhZGF0YSwgQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGEsIENvbXBpbGVTdW1tYXJ5S2luZCwgQ29tcGlsZVR5cGVNZXRhZGF0YSwgQ29tcGlsZVR5cGVTdW1tYXJ5fSBmcm9tICcuLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCAqIGFzIG8gZnJvbSAnLi4vb3V0cHV0L291dHB1dF9hc3QnO1xuaW1wb3J0IHtTdW1tYXJ5LCBTdW1tYXJ5UmVzb2x2ZXJ9IGZyb20gJy4uL3N1bW1hcnlfcmVzb2x2ZXInO1xuaW1wb3J0IHtPdXRwdXRDb250ZXh0LCBWYWx1ZVRyYW5zZm9ybWVyLCBWYWx1ZVZpc2l0b3IsIHZpc2l0VmFsdWV9IGZyb20gJy4uL3V0aWwnO1xuXG5pbXBvcnQge1N0YXRpY1N5bWJvbCwgU3RhdGljU3ltYm9sQ2FjaGV9IGZyb20gJy4vc3RhdGljX3N5bWJvbCc7XG5pbXBvcnQge1Jlc29sdmVkU3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2xSZXNvbHZlciwgdW53cmFwUmVzb2x2ZWRNZXRhZGF0YX0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sX3Jlc29sdmVyJztcbmltcG9ydCB7aXNMb3dlcmVkU3ltYm9sLCBuZ2ZhY3RvcnlGaWxlUGF0aCwgc3VtbWFyeUZvckppdEZpbGVOYW1lLCBzdW1tYXJ5Rm9ySml0TmFtZX0gZnJvbSAnLi91dGlsJztcblxuZXhwb3J0IGZ1bmN0aW9uIHNlcmlhbGl6ZVN1bW1hcmllcyhcbiAgICBzcmNGaWxlTmFtZTogc3RyaW5nLCBmb3JKaXRDdHg6IE91dHB1dENvbnRleHQgfCBudWxsLFxuICAgIHN1bW1hcnlSZXNvbHZlcjogU3VtbWFyeVJlc29sdmVyPFN0YXRpY1N5bWJvbD4sIHN5bWJvbFJlc29sdmVyOiBTdGF0aWNTeW1ib2xSZXNvbHZlcixcbiAgICBzeW1ib2xzOiBSZXNvbHZlZFN0YXRpY1N5bWJvbFtdLCB0eXBlczoge1xuICAgICAgc3VtbWFyeTogQ29tcGlsZVR5cGVTdW1tYXJ5LFxuICAgICAgbWV0YWRhdGE6IENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhIHwgQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhIHwgQ29tcGlsZVBpcGVNZXRhZGF0YSB8XG4gICAgICAgICAgQ29tcGlsZVR5cGVNZXRhZGF0YVxuICAgIH1bXSxcbiAgICBjcmVhdGVFeHRlcm5hbFN5bWJvbFJlZXhwb3J0cyA9XG4gICAgICAgIGZhbHNlKToge2pzb246IHN0cmluZywgZXhwb3J0QXM6IHtzeW1ib2w6IFN0YXRpY1N5bWJvbCwgZXhwb3J0QXM6IHN0cmluZ31bXX0ge1xuICBjb25zdCB0b0pzb25TZXJpYWxpemVyID0gbmV3IFRvSnNvblNlcmlhbGl6ZXIoc3ltYm9sUmVzb2x2ZXIsIHN1bW1hcnlSZXNvbHZlciwgc3JjRmlsZU5hbWUpO1xuXG4gIC8vIGZvciBzeW1ib2xzLCB3ZSB1c2UgZXZlcnl0aGluZyBleGNlcHQgZm9yIHRoZSBjbGFzcyBtZXRhZGF0YSBpdHNlbGZcbiAgLy8gKHdlIGtlZXAgdGhlIHN0YXRpY3MgdGhvdWdoKSwgYXMgdGhlIGNsYXNzIG1ldGFkYXRhIGlzIGNvbnRhaW5lZCBpbiB0aGVcbiAgLy8gQ29tcGlsZVR5cGVTdW1tYXJ5LlxuICBzeW1ib2xzLmZvckVhY2goXG4gICAgICAocmVzb2x2ZWRTeW1ib2wpID0+IHRvSnNvblNlcmlhbGl6ZXIuYWRkU3VtbWFyeShcbiAgICAgICAgICB7c3ltYm9sOiByZXNvbHZlZFN5bWJvbC5zeW1ib2wsIG1ldGFkYXRhOiByZXNvbHZlZFN5bWJvbC5tZXRhZGF0YX0pKTtcblxuICAvLyBBZGQgdHlwZSBzdW1tYXJpZXMuXG4gIHR5cGVzLmZvckVhY2goKHtzdW1tYXJ5LCBtZXRhZGF0YX0pID0+IHtcbiAgICB0b0pzb25TZXJpYWxpemVyLmFkZFN1bW1hcnkoXG4gICAgICAgIHtzeW1ib2w6IHN1bW1hcnkudHlwZS5yZWZlcmVuY2UsIG1ldGFkYXRhOiB1bmRlZmluZWQsIHR5cGU6IHN1bW1hcnl9KTtcbiAgfSk7XG4gIGNvbnN0IHtqc29uLCBleHBvcnRBc30gPSB0b0pzb25TZXJpYWxpemVyLnNlcmlhbGl6ZShjcmVhdGVFeHRlcm5hbFN5bWJvbFJlZXhwb3J0cyk7XG4gIGlmIChmb3JKaXRDdHgpIHtcbiAgICBjb25zdCBmb3JKaXRTZXJpYWxpemVyID0gbmV3IEZvckppdFNlcmlhbGl6ZXIoZm9ySml0Q3R4LCBzeW1ib2xSZXNvbHZlciwgc3VtbWFyeVJlc29sdmVyKTtcbiAgICB0eXBlcy5mb3JFYWNoKCh7c3VtbWFyeSwgbWV0YWRhdGF9KSA9PiB7IGZvckppdFNlcmlhbGl6ZXIuYWRkU291cmNlVHlwZShzdW1tYXJ5LCBtZXRhZGF0YSk7IH0pO1xuICAgIHRvSnNvblNlcmlhbGl6ZXIudW5wcm9jZXNzZWRTeW1ib2xTdW1tYXJpZXNCeVN5bWJvbC5mb3JFYWNoKChzdW1tYXJ5KSA9PiB7XG4gICAgICBpZiAoc3VtbWFyeVJlc29sdmVyLmlzTGlicmFyeUZpbGUoc3VtbWFyeS5zeW1ib2wuZmlsZVBhdGgpICYmIHN1bW1hcnkudHlwZSkge1xuICAgICAgICBmb3JKaXRTZXJpYWxpemVyLmFkZExpYlR5cGUoc3VtbWFyeS50eXBlKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICBmb3JKaXRTZXJpYWxpemVyLnNlcmlhbGl6ZShleHBvcnRBcyk7XG4gIH1cbiAgcmV0dXJuIHtqc29uLCBleHBvcnRBc307XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkZXNlcmlhbGl6ZVN1bW1hcmllcyhcbiAgICBzeW1ib2xDYWNoZTogU3RhdGljU3ltYm9sQ2FjaGUsIHN1bW1hcnlSZXNvbHZlcjogU3VtbWFyeVJlc29sdmVyPFN0YXRpY1N5bWJvbD4sXG4gICAgbGlicmFyeUZpbGVOYW1lOiBzdHJpbmcsIGpzb246IHN0cmluZyk6IHtcbiAgbW9kdWxlTmFtZTogc3RyaW5nIHwgbnVsbCxcbiAgc3VtbWFyaWVzOiBTdW1tYXJ5PFN0YXRpY1N5bWJvbD5bXSxcbiAgaW1wb3J0QXM6IHtzeW1ib2w6IFN0YXRpY1N5bWJvbCwgaW1wb3J0QXM6IFN0YXRpY1N5bWJvbH1bXVxufSB7XG4gIGNvbnN0IGRlc2VyaWFsaXplciA9IG5ldyBGcm9tSnNvbkRlc2VyaWFsaXplcihzeW1ib2xDYWNoZSwgc3VtbWFyeVJlc29sdmVyKTtcbiAgcmV0dXJuIGRlc2VyaWFsaXplci5kZXNlcmlhbGl6ZShsaWJyYXJ5RmlsZU5hbWUsIGpzb24pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlRm9ySml0U3R1YihvdXRwdXRDdHg6IE91dHB1dENvbnRleHQsIHJlZmVyZW5jZTogU3RhdGljU3ltYm9sKSB7XG4gIHJldHVybiBjcmVhdGVTdW1tYXJ5Rm9ySml0RnVuY3Rpb24ob3V0cHV0Q3R4LCByZWZlcmVuY2UsIG8uTlVMTF9FWFBSKTtcbn1cblxuZnVuY3Rpb24gY3JlYXRlU3VtbWFyeUZvckppdEZ1bmN0aW9uKFxuICAgIG91dHB1dEN0eDogT3V0cHV0Q29udGV4dCwgcmVmZXJlbmNlOiBTdGF0aWNTeW1ib2wsIHZhbHVlOiBvLkV4cHJlc3Npb24pIHtcbiAgY29uc3QgZm5OYW1lID0gc3VtbWFyeUZvckppdE5hbWUocmVmZXJlbmNlLm5hbWUpO1xuICBvdXRwdXRDdHguc3RhdGVtZW50cy5wdXNoKFxuICAgICAgby5mbihbXSwgW25ldyBvLlJldHVyblN0YXRlbWVudCh2YWx1ZSldLCBuZXcgby5BcnJheVR5cGUoby5EWU5BTUlDX1RZUEUpKS50b0RlY2xTdG10KGZuTmFtZSwgW1xuICAgICAgICBvLlN0bXRNb2RpZmllci5GaW5hbCwgby5TdG10TW9kaWZpZXIuRXhwb3J0ZWRcbiAgICAgIF0pKTtcbn1cblxuY29uc3QgZW51bSBTZXJpYWxpemF0aW9uRmxhZ3Mge1xuICBOb25lID0gMCxcbiAgUmVzb2x2ZVZhbHVlID0gMSxcbn1cblxuY2xhc3MgVG9Kc29uU2VyaWFsaXplciBleHRlbmRzIFZhbHVlVHJhbnNmb3JtZXIge1xuICAvLyBOb3RlOiBUaGlzIG9ubHkgY29udGFpbnMgc3ltYm9scyB3aXRob3V0IG1lbWJlcnMuXG4gIHByaXZhdGUgc3ltYm9sczogU3RhdGljU3ltYm9sW10gPSBbXTtcbiAgcHJpdmF0ZSBpbmRleEJ5U3ltYm9sID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIG51bWJlcj4oKTtcbiAgcHJpdmF0ZSByZWV4cG9ydGVkQnkgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgU3RhdGljU3ltYm9sPigpO1xuICAvLyBUaGlzIG5vdyBjb250YWlucyBhIGBfX3N5bWJvbDogbnVtYmVyYCBpbiB0aGUgcGxhY2Ugb2ZcbiAgLy8gU3RhdGljU3ltYm9scywgYnV0IG90aGVyd2lzZSBoYXMgdGhlIHNhbWUgc2hhcGUgYXMgdGhlIG9yaWdpbmFsIG9iamVjdHMuXG4gIHByaXZhdGUgcHJvY2Vzc2VkU3VtbWFyeUJ5U3ltYm9sID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIGFueT4oKTtcbiAgcHJpdmF0ZSBwcm9jZXNzZWRTdW1tYXJpZXM6IGFueVtdID0gW107XG4gIHByaXZhdGUgbW9kdWxlTmFtZTogc3RyaW5nfG51bGw7XG5cbiAgdW5wcm9jZXNzZWRTeW1ib2xTdW1tYXJpZXNCeVN5bWJvbCA9IG5ldyBNYXA8U3RhdGljU3ltYm9sLCBTdW1tYXJ5PFN0YXRpY1N5bWJvbD4+KCk7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIHN5bWJvbFJlc29sdmVyOiBTdGF0aWNTeW1ib2xSZXNvbHZlcixcbiAgICAgIHByaXZhdGUgc3VtbWFyeVJlc29sdmVyOiBTdW1tYXJ5UmVzb2x2ZXI8U3RhdGljU3ltYm9sPiwgcHJpdmF0ZSBzcmNGaWxlTmFtZTogc3RyaW5nKSB7XG4gICAgc3VwZXIoKTtcbiAgICB0aGlzLm1vZHVsZU5hbWUgPSBzeW1ib2xSZXNvbHZlci5nZXRLbm93bk1vZHVsZU5hbWUoc3JjRmlsZU5hbWUpO1xuICB9XG5cbiAgYWRkU3VtbWFyeShzdW1tYXJ5OiBTdW1tYXJ5PFN0YXRpY1N5bWJvbD4pIHtcbiAgICBsZXQgdW5wcm9jZXNzZWRTdW1tYXJ5ID0gdGhpcy51bnByb2Nlc3NlZFN5bWJvbFN1bW1hcmllc0J5U3ltYm9sLmdldChzdW1tYXJ5LnN5bWJvbCk7XG4gICAgbGV0IHByb2Nlc3NlZFN1bW1hcnkgPSB0aGlzLnByb2Nlc3NlZFN1bW1hcnlCeVN5bWJvbC5nZXQoc3VtbWFyeS5zeW1ib2wpO1xuICAgIGlmICghdW5wcm9jZXNzZWRTdW1tYXJ5KSB7XG4gICAgICB1bnByb2Nlc3NlZFN1bW1hcnkgPSB7c3ltYm9sOiBzdW1tYXJ5LnN5bWJvbCwgbWV0YWRhdGE6IHVuZGVmaW5lZH07XG4gICAgICB0aGlzLnVucHJvY2Vzc2VkU3ltYm9sU3VtbWFyaWVzQnlTeW1ib2wuc2V0KHN1bW1hcnkuc3ltYm9sLCB1bnByb2Nlc3NlZFN1bW1hcnkpO1xuICAgICAgcHJvY2Vzc2VkU3VtbWFyeSA9IHtzeW1ib2w6IHRoaXMucHJvY2Vzc1ZhbHVlKHN1bW1hcnkuc3ltYm9sLCBTZXJpYWxpemF0aW9uRmxhZ3MuTm9uZSl9O1xuICAgICAgdGhpcy5wcm9jZXNzZWRTdW1tYXJpZXMucHVzaChwcm9jZXNzZWRTdW1tYXJ5KTtcbiAgICAgIHRoaXMucHJvY2Vzc2VkU3VtbWFyeUJ5U3ltYm9sLnNldChzdW1tYXJ5LnN5bWJvbCwgcHJvY2Vzc2VkU3VtbWFyeSk7XG4gICAgfVxuICAgIGlmICghdW5wcm9jZXNzZWRTdW1tYXJ5Lm1ldGFkYXRhICYmIHN1bW1hcnkubWV0YWRhdGEpIHtcbiAgICAgIGxldCBtZXRhZGF0YSA9IHN1bW1hcnkubWV0YWRhdGEgfHwge307XG4gICAgICBpZiAobWV0YWRhdGEuX19zeW1ib2xpYyA9PT0gJ2NsYXNzJykge1xuICAgICAgICAvLyBGb3IgY2xhc3Nlcywgd2Uga2VlcCBldmVyeXRoaW5nIGV4Y2VwdCB0aGVpciBjbGFzcyBkZWNvcmF0b3JzLlxuICAgICAgICAvLyBXZSBuZWVkIHRvIGtlZXAgZS5nLiB0aGUgY3RvciBhcmdzLCBtZXRob2QgbmFtZXMsIG1ldGhvZCBkZWNvcmF0b3JzXG4gICAgICAgIC8vIHNvIHRoYXQgdGhlIGNsYXNzIGNhbiBiZSBleHRlbmRlZCBpbiBhbm90aGVyIGNvbXBpbGF0aW9uIHVuaXQuXG4gICAgICAgIC8vIFdlIGRvbid0IGtlZXAgdGhlIGNsYXNzIGRlY29yYXRvcnMgYXNcbiAgICAgICAgLy8gMSkgdGhleSByZWZlciB0byBkYXRhXG4gICAgICAgIC8vICAgdGhhdCBzaG91bGQgbm90IGNhdXNlIGEgcmVidWlsZCBvZiBkb3duc3RyZWFtIGNvbXBpbGF0aW9uIHVuaXRzXG4gICAgICAgIC8vICAgKGUuZy4gaW5saW5lIHRlbXBsYXRlcyBvZiBAQ29tcG9uZW50LCBvciBATmdNb2R1bGUuZGVjbGFyYXRpb25zKVxuICAgICAgICAvLyAyKSB0aGVpciBkYXRhIGlzIGFscmVhZHkgY2FwdHVyZWQgaW4gVHlwZVN1bW1hcmllcywgZS5nLiBEaXJlY3RpdmVTdW1tYXJ5LlxuICAgICAgICBjb25zdCBjbG9uZToge1trZXk6IHN0cmluZ106IGFueX0gPSB7fTtcbiAgICAgICAgT2JqZWN0LmtleXMobWV0YWRhdGEpLmZvckVhY2goKHByb3BOYW1lKSA9PiB7XG4gICAgICAgICAgaWYgKHByb3BOYW1lICE9PSAnZGVjb3JhdG9ycycpIHtcbiAgICAgICAgICAgIGNsb25lW3Byb3BOYW1lXSA9IG1ldGFkYXRhW3Byb3BOYW1lXTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICBtZXRhZGF0YSA9IGNsb25lO1xuICAgICAgfSBlbHNlIGlmIChpc0NhbGwobWV0YWRhdGEpKSB7XG4gICAgICAgIGlmICghaXNGdW5jdGlvbkNhbGwobWV0YWRhdGEpICYmICFpc01ldGhvZENhbGxPblZhcmlhYmxlKG1ldGFkYXRhKSkge1xuICAgICAgICAgIC8vIERvbid0IHN0b3JlIGNvbXBsZXggY2FsbHMgYXMgd2Ugd29uJ3QgYmUgYWJsZSB0byBzaW1wbGlmeSB0aGVtIGFueXdheXMgbGF0ZXIgb24uXG4gICAgICAgICAgbWV0YWRhdGEgPSB7XG4gICAgICAgICAgICBfX3N5bWJvbGljOiAnZXJyb3InLFxuICAgICAgICAgICAgbWVzc2FnZTogJ0NvbXBsZXggZnVuY3Rpb24gY2FsbHMgYXJlIG5vdCBzdXBwb3J0ZWQuJyxcbiAgICAgICAgICB9O1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICAvLyBOb3RlOiBXZSBuZWVkIHRvIGtlZXAgc3RvcmluZyBjdG9yIGNhbGxzIGZvciBlLmcuXG4gICAgICAvLyBgZXhwb3J0IGNvbnN0IHggPSBuZXcgSW5qZWN0aW9uVG9rZW4oLi4uKWBcbiAgICAgIHVucHJvY2Vzc2VkU3VtbWFyeS5tZXRhZGF0YSA9IG1ldGFkYXRhO1xuICAgICAgcHJvY2Vzc2VkU3VtbWFyeS5tZXRhZGF0YSA9IHRoaXMucHJvY2Vzc1ZhbHVlKG1ldGFkYXRhLCBTZXJpYWxpemF0aW9uRmxhZ3MuUmVzb2x2ZVZhbHVlKTtcbiAgICAgIGlmIChtZXRhZGF0YSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCAmJlxuICAgICAgICAgIHRoaXMuc3VtbWFyeVJlc29sdmVyLmlzTGlicmFyeUZpbGUobWV0YWRhdGEuZmlsZVBhdGgpKSB7XG4gICAgICAgIGNvbnN0IGRlY2xhcmF0aW9uU3ltYm9sID0gdGhpcy5zeW1ib2xzW3RoaXMuaW5kZXhCeVN5bWJvbC5nZXQobWV0YWRhdGEpICFdO1xuICAgICAgICBpZiAoIWlzTG93ZXJlZFN5bWJvbChkZWNsYXJhdGlvblN5bWJvbC5uYW1lKSkge1xuICAgICAgICAgIC8vIE5vdGU6IHN5bWJvbHMgdGhhdCB3ZXJlIGludHJvZHVjZWQgZHVyaW5nIGNvZGVnZW4gaW4gdGhlIHVzZXIgZmlsZSBjYW4gaGF2ZSBhIHJlZXhwb3J0XG4gICAgICAgICAgLy8gaWYgYSB1c2VyIHVzZWQgYGV4cG9ydCAqYC4gSG93ZXZlciwgd2UgY2FuJ3QgcmVseSBvbiB0aGlzIGFzIHRzaWNrbGUgd2lsbCBjaGFuZ2VcbiAgICAgICAgICAvLyBgZXhwb3J0ICpgIGludG8gbmFtZWQgZXhwb3J0cywgdXNpbmcgb25seSB0aGUgaW5mb3JtYXRpb24gZnJvbSB0aGUgdHlwZWNoZWNrZXIuXG4gICAgICAgICAgLy8gQXMgd2UgaW50cm9kdWNlIHRoZSBuZXcgc3ltYm9scyBhZnRlciB0eXBlY2hlY2ssIFRzaWNrbGUgZG9lcyBub3Qga25vdyBhYm91dCB0aGVtLFxuICAgICAgICAgIC8vIGFuZCBvbWl0cyB0aGVtIHdoZW4gZXhwYW5kaW5nIGBleHBvcnQgKmAuXG4gICAgICAgICAgLy8gU28gd2UgaGF2ZSB0byBrZWVwIHJlZXhwb3J0aW5nIHRoZXNlIHN5bWJvbHMgbWFudWFsbHkgdmlhIC5uZ2ZhY3RvcnkgZmlsZXMuXG4gICAgICAgICAgdGhpcy5yZWV4cG9ydGVkQnkuc2V0KGRlY2xhcmF0aW9uU3ltYm9sLCBzdW1tYXJ5LnN5bWJvbCk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKCF1bnByb2Nlc3NlZFN1bW1hcnkudHlwZSAmJiBzdW1tYXJ5LnR5cGUpIHtcbiAgICAgIHVucHJvY2Vzc2VkU3VtbWFyeS50eXBlID0gc3VtbWFyeS50eXBlO1xuICAgICAgLy8gTm90ZTogV2UgZG9uJ3QgYWRkIHRoZSBzdW1tYXJpZXMgb2YgYWxsIHJlZmVyZW5jZWQgc3ltYm9scyBhcyBmb3IgdGhlIFJlc29sdmVkU3ltYm9scyxcbiAgICAgIC8vIGFzIHRoZSB0eXBlIHN1bW1hcmllcyBhbHJlYWR5IGNvbnRhaW4gdGhlIHRyYW5zaXRpdmUgZGF0YSB0aGF0IHRoZXkgcmVxdWlyZVxuICAgICAgLy8gKGluIGEgbWluaW1hbCB3YXkpLlxuICAgICAgcHJvY2Vzc2VkU3VtbWFyeS50eXBlID0gdGhpcy5wcm9jZXNzVmFsdWUoc3VtbWFyeS50eXBlLCBTZXJpYWxpemF0aW9uRmxhZ3MuTm9uZSk7XG4gICAgICAvLyBleGNlcHQgZm9yIHJlZXhwb3J0ZWQgZGlyZWN0aXZlcyAvIHBpcGVzLCBzbyB3ZSBuZWVkIHRvIHN0b3JlXG4gICAgICAvLyB0aGVpciBzdW1tYXJpZXMgZXhwbGljaXRseS5cbiAgICAgIGlmIChzdW1tYXJ5LnR5cGUuc3VtbWFyeUtpbmQgPT09IENvbXBpbGVTdW1tYXJ5S2luZC5OZ01vZHVsZSkge1xuICAgICAgICBjb25zdCBuZ01vZHVsZVN1bW1hcnkgPSA8Q29tcGlsZU5nTW9kdWxlU3VtbWFyeT5zdW1tYXJ5LnR5cGU7XG4gICAgICAgIG5nTW9kdWxlU3VtbWFyeS5leHBvcnRlZERpcmVjdGl2ZXMuY29uY2F0KG5nTW9kdWxlU3VtbWFyeS5leHBvcnRlZFBpcGVzKS5mb3JFYWNoKChpZCkgPT4ge1xuICAgICAgICAgIGNvbnN0IHN5bWJvbDogU3RhdGljU3ltYm9sID0gaWQucmVmZXJlbmNlO1xuICAgICAgICAgIGlmICh0aGlzLnN1bW1hcnlSZXNvbHZlci5pc0xpYnJhcnlGaWxlKHN5bWJvbC5maWxlUGF0aCkgJiZcbiAgICAgICAgICAgICAgIXRoaXMudW5wcm9jZXNzZWRTeW1ib2xTdW1tYXJpZXNCeVN5bWJvbC5oYXMoc3ltYm9sKSkge1xuICAgICAgICAgICAgY29uc3Qgc3VtbWFyeSA9IHRoaXMuc3VtbWFyeVJlc29sdmVyLnJlc29sdmVTdW1tYXJ5KHN5bWJvbCk7XG4gICAgICAgICAgICBpZiAoc3VtbWFyeSkge1xuICAgICAgICAgICAgICB0aGlzLmFkZFN1bW1hcnkoc3VtbWFyeSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIGNyZWF0ZUV4dGVybmFsU3ltYm9sUmVleHBvcnRzIFdoZXRoZXIgZXh0ZXJuYWwgc3RhdGljIHN5bWJvbHMgc2hvdWxkIGJlIHJlLWV4cG9ydGVkLlxuICAgKiBUaGlzIGNhbiBiZSBlbmFibGVkIGlmIGV4dGVybmFsIHN5bWJvbHMgc2hvdWxkIGJlIHJlLWV4cG9ydGVkIGJ5IHRoZSBjdXJyZW50IG1vZHVsZSBpblxuICAgKiBvcmRlciB0byBhdm9pZCBkeW5hbWljYWxseSBnZW5lcmF0ZWQgbW9kdWxlIGRlcGVuZGVuY2llcyB3aGljaCBjYW4gYnJlYWsgc3RyaWN0IGRlcGVuZGVuY3lcbiAgICogZW5mb3JjZW1lbnRzIChhcyBpbiBHb29nbGUzKS4gUmVhZCBtb3JlIGhlcmU6IGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIvaXNzdWVzLzI1NjQ0XG4gICAqL1xuICBzZXJpYWxpemUoY3JlYXRlRXh0ZXJuYWxTeW1ib2xSZWV4cG9ydHM6IGJvb2xlYW4pOlxuICAgICAge2pzb246IHN0cmluZywgZXhwb3J0QXM6IHtzeW1ib2w6IFN0YXRpY1N5bWJvbCwgZXhwb3J0QXM6IHN0cmluZ31bXX0ge1xuICAgIGNvbnN0IGV4cG9ydEFzOiB7c3ltYm9sOiBTdGF0aWNTeW1ib2wsIGV4cG9ydEFzOiBzdHJpbmd9W10gPSBbXTtcbiAgICBjb25zdCBqc29uID0gSlNPTi5zdHJpbmdpZnkoe1xuICAgICAgbW9kdWxlTmFtZTogdGhpcy5tb2R1bGVOYW1lLFxuICAgICAgc3VtbWFyaWVzOiB0aGlzLnByb2Nlc3NlZFN1bW1hcmllcyxcbiAgICAgIHN5bWJvbHM6IHRoaXMuc3ltYm9scy5tYXAoKHN5bWJvbCwgaW5kZXgpID0+IHtcbiAgICAgICAgc3ltYm9sLmFzc2VydE5vTWVtYmVycygpO1xuICAgICAgICBsZXQgaW1wb3J0QXM6IHN0cmluZ3xudW1iZXIgPSB1bmRlZmluZWQgITtcbiAgICAgICAgaWYgKHRoaXMuc3VtbWFyeVJlc29sdmVyLmlzTGlicmFyeUZpbGUoc3ltYm9sLmZpbGVQYXRoKSkge1xuICAgICAgICAgIGNvbnN0IHJlZXhwb3J0U3ltYm9sID0gdGhpcy5yZWV4cG9ydGVkQnkuZ2V0KHN5bWJvbCk7XG4gICAgICAgICAgaWYgKHJlZXhwb3J0U3ltYm9sKSB7XG4gICAgICAgICAgICAvLyBJbiBjYXNlIHRoZSBnaXZlbiBleHRlcm5hbCBzdGF0aWMgc3ltYm9sIGlzIGFscmVhZHkgbWFudWFsbHkgZXhwb3J0ZWQgYnkgdGhlXG4gICAgICAgICAgICAvLyB1c2VyLCB3ZSBqdXN0IHByb3h5IHRoZSBleHRlcm5hbCBzdGF0aWMgc3ltYm9sIHJlZmVyZW5jZSB0byB0aGUgbWFudWFsIGV4cG9ydC5cbiAgICAgICAgICAgIC8vIFRoaXMgZW5zdXJlcyB0aGF0IHRoZSBBT1QgY29tcGlsZXIgaW1wb3J0cyB0aGUgZXh0ZXJuYWwgc3ltYm9sIHRocm91Z2ggdGhlXG4gICAgICAgICAgICAvLyB1c2VyIGV4cG9ydCBhbmQgZG9lcyBub3QgaW50cm9kdWNlIGFub3RoZXIgZGVwZW5kZW5jeSB3aGljaCBpcyBub3QgbmVlZGVkLlxuICAgICAgICAgICAgaW1wb3J0QXMgPSB0aGlzLmluZGV4QnlTeW1ib2wuZ2V0KHJlZXhwb3J0U3ltYm9sKSAhO1xuICAgICAgICAgIH0gZWxzZSBpZiAoY3JlYXRlRXh0ZXJuYWxTeW1ib2xSZWV4cG9ydHMpIHtcbiAgICAgICAgICAgIC8vIEluIHRoaXMgY2FzZSwgdGhlIGdpdmVuIGV4dGVybmFsIHN0YXRpYyBzeW1ib2wgaXMgKm5vdCogbWFudWFsbHkgZXhwb3J0ZWQgYnlcbiAgICAgICAgICAgIC8vIHRoZSB1c2VyLCBhbmQgd2UgbWFudWFsbHkgY3JlYXRlIGEgcmUtZXhwb3J0IGluIHRoZSBmYWN0b3J5IGZpbGUgc28gdGhhdCB3ZVxuICAgICAgICAgICAgLy8gZG9uJ3QgaW50cm9kdWNlIGFub3RoZXIgbW9kdWxlIGRlcGVuZGVuY3kuIFRoaXMgaXMgdXNlZnVsIHdoZW4gcnVubmluZyB3aXRoaW5cbiAgICAgICAgICAgIC8vIEJhemVsIHNvIHRoYXQgdGhlIEFPVCBjb21waWxlciBkb2VzIG5vdCBpbnRyb2R1Y2UgYW55IG1vZHVsZSBkZXBlbmRlbmNpZXNcbiAgICAgICAgICAgIC8vIHdoaWNoIGNhbiBicmVhayB0aGUgc3RyaWN0IGRlcGVuZGVuY3kgZW5mb3JjZW1lbnQuIChlLmcuIGFzIGluIEdvb2dsZTMpXG4gICAgICAgICAgICAvLyBSZWFkIG1vcmUgYWJvdXQgdGhpcyBoZXJlOiBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyL2lzc3Vlcy8yNTY0NFxuICAgICAgICAgICAgY29uc3Qgc3VtbWFyeSA9IHRoaXMudW5wcm9jZXNzZWRTeW1ib2xTdW1tYXJpZXNCeVN5bWJvbC5nZXQoc3ltYm9sKTtcbiAgICAgICAgICAgIGlmICghc3VtbWFyeSB8fCAhc3VtbWFyeS5tZXRhZGF0YSB8fCBzdW1tYXJ5Lm1ldGFkYXRhLl9fc3ltYm9saWMgIT09ICdpbnRlcmZhY2UnKSB7XG4gICAgICAgICAgICAgIGltcG9ydEFzID0gYCR7c3ltYm9sLm5hbWV9XyR7aW5kZXh9YDtcbiAgICAgICAgICAgICAgZXhwb3J0QXMucHVzaCh7c3ltYm9sLCBleHBvcnRBczogaW1wb3J0QXN9KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICBfX3N5bWJvbDogaW5kZXgsXG4gICAgICAgICAgbmFtZTogc3ltYm9sLm5hbWUsXG4gICAgICAgICAgZmlsZVBhdGg6IHRoaXMuc3VtbWFyeVJlc29sdmVyLnRvU3VtbWFyeUZpbGVOYW1lKHN5bWJvbC5maWxlUGF0aCwgdGhpcy5zcmNGaWxlTmFtZSksXG4gICAgICAgICAgaW1wb3J0QXM6IGltcG9ydEFzXG4gICAgICAgIH07XG4gICAgICB9KVxuICAgIH0pO1xuICAgIHJldHVybiB7anNvbiwgZXhwb3J0QXN9O1xuICB9XG5cbiAgcHJpdmF0ZSBwcm9jZXNzVmFsdWUodmFsdWU6IGFueSwgZmxhZ3M6IFNlcmlhbGl6YXRpb25GbGFncyk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0VmFsdWUodmFsdWUsIHRoaXMsIGZsYWdzKTtcbiAgfVxuXG4gIHZpc2l0T3RoZXIodmFsdWU6IGFueSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICBpZiAodmFsdWUgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgIGxldCBiYXNlU3ltYm9sID0gdGhpcy5zeW1ib2xSZXNvbHZlci5nZXRTdGF0aWNTeW1ib2wodmFsdWUuZmlsZVBhdGgsIHZhbHVlLm5hbWUpO1xuICAgICAgY29uc3QgaW5kZXggPSB0aGlzLnZpc2l0U3RhdGljU3ltYm9sKGJhc2VTeW1ib2wsIGNvbnRleHQpO1xuICAgICAgcmV0dXJuIHtfX3N5bWJvbDogaW5kZXgsIG1lbWJlcnM6IHZhbHVlLm1lbWJlcnN9O1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBTdHJpcCBsaW5lIGFuZCBjaGFyYWN0ZXIgbnVtYmVycyBmcm9tIG5nc3VtbWFyaWVzLlxuICAgKiBFbWl0dGluZyB0aGVtIGNhdXNlcyB3aGl0ZSBzcGFjZXMgY2hhbmdlcyB0byByZXRyaWdnZXIgdXBzdHJlYW1cbiAgICogcmVjb21waWxhdGlvbnMgaW4gYmF6ZWwuXG4gICAqIFRPRE86IGZpbmQgb3V0IGEgd2F5IHRvIGhhdmUgbGluZSBhbmQgY2hhcmFjdGVyIG51bWJlcnMgaW4gZXJyb3JzIHdpdGhvdXRcbiAgICogZXhjZXNzaXZlIHJlY29tcGlsYXRpb24gaW4gYmF6ZWwuXG4gICAqL1xuICB2aXNpdFN0cmluZ01hcChtYXA6IHtba2V5OiBzdHJpbmddOiBhbnl9LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIGlmIChtYXBbJ19fc3ltYm9saWMnXSA9PT0gJ3Jlc29sdmVkJykge1xuICAgICAgcmV0dXJuIHZpc2l0VmFsdWUobWFwWydzeW1ib2wnXSwgdGhpcywgY29udGV4dCk7XG4gICAgfVxuICAgIGlmIChtYXBbJ19fc3ltYm9saWMnXSA9PT0gJ2Vycm9yJykge1xuICAgICAgZGVsZXRlIG1hcFsnbGluZSddO1xuICAgICAgZGVsZXRlIG1hcFsnY2hhcmFjdGVyJ107XG4gICAgfVxuICAgIHJldHVybiBzdXBlci52aXNpdFN0cmluZ01hcChtYXAsIGNvbnRleHQpO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgbnVsbCBpZiB0aGUgb3B0aW9ucy5yZXNvbHZlVmFsdWUgaXMgdHJ1ZSwgYW5kIHRoZSBzdW1tYXJ5IGZvciB0aGUgc3ltYm9sXG4gICAqIHJlc29sdmVkIHRvIGEgdHlwZSBvciBjb3VsZCBub3QgYmUgcmVzb2x2ZWQuXG4gICAqL1xuICBwcml2YXRlIHZpc2l0U3RhdGljU3ltYm9sKGJhc2VTeW1ib2w6IFN0YXRpY1N5bWJvbCwgZmxhZ3M6IFNlcmlhbGl6YXRpb25GbGFncyk6IG51bWJlciB7XG4gICAgbGV0IGluZGV4OiBudW1iZXJ8dW5kZWZpbmVkfG51bGwgPSB0aGlzLmluZGV4QnlTeW1ib2wuZ2V0KGJhc2VTeW1ib2wpO1xuICAgIGxldCBzdW1tYXJ5OiBTdW1tYXJ5PFN0YXRpY1N5bWJvbD58bnVsbCA9IG51bGw7XG4gICAgaWYgKGZsYWdzICYgU2VyaWFsaXphdGlvbkZsYWdzLlJlc29sdmVWYWx1ZSAmJlxuICAgICAgICB0aGlzLnN1bW1hcnlSZXNvbHZlci5pc0xpYnJhcnlGaWxlKGJhc2VTeW1ib2wuZmlsZVBhdGgpKSB7XG4gICAgICBpZiAodGhpcy51bnByb2Nlc3NlZFN5bWJvbFN1bW1hcmllc0J5U3ltYm9sLmhhcyhiYXNlU3ltYm9sKSkge1xuICAgICAgICAvLyB0aGUgc3VtbWFyeSBmb3IgdGhpcyBzeW1ib2wgd2FzIGFscmVhZHkgYWRkZWRcbiAgICAgICAgLy8gLT4gbm90aGluZyB0byBkby5cbiAgICAgICAgcmV0dXJuIGluZGV4ICE7XG4gICAgICB9XG4gICAgICBzdW1tYXJ5ID0gdGhpcy5sb2FkU3VtbWFyeShiYXNlU3ltYm9sKTtcbiAgICAgIGlmIChzdW1tYXJ5ICYmIHN1bW1hcnkubWV0YWRhdGEgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgICAgLy8gVGhlIHN1bW1hcnkgaXMgYSByZWV4cG9ydFxuICAgICAgICBpbmRleCA9IHRoaXMudmlzaXRTdGF0aWNTeW1ib2woc3VtbWFyeS5tZXRhZGF0YSwgZmxhZ3MpO1xuICAgICAgICAvLyByZXNldCB0aGUgc3VtbWFyeSBhcyBpdCBpcyBqdXN0IGEgcmVleHBvcnQsIHNvIHdlIGRvbid0IHdhbnQgdG8gc3RvcmUgaXQuXG4gICAgICAgIHN1bW1hcnkgPSBudWxsO1xuICAgICAgfVxuICAgIH0gZWxzZSBpZiAoaW5kZXggIT0gbnVsbCkge1xuICAgICAgLy8gTm90ZTogPT0gb24gcHVycG9zZSB0byBjb21wYXJlIHdpdGggdW5kZWZpbmVkIVxuICAgICAgLy8gTm8gc3VtbWFyeSBhbmQgdGhlIHN5bWJvbCBpcyBhbHJlYWR5IGFkZGVkIC0+IG5vdGhpbmcgdG8gZG8uXG4gICAgICByZXR1cm4gaW5kZXg7XG4gICAgfVxuICAgIC8vIE5vdGU6ID09IG9uIHB1cnBvc2UgdG8gY29tcGFyZSB3aXRoIHVuZGVmaW5lZCFcbiAgICBpZiAoaW5kZXggPT0gbnVsbCkge1xuICAgICAgaW5kZXggPSB0aGlzLnN5bWJvbHMubGVuZ3RoO1xuICAgICAgdGhpcy5zeW1ib2xzLnB1c2goYmFzZVN5bWJvbCk7XG4gICAgfVxuICAgIHRoaXMuaW5kZXhCeVN5bWJvbC5zZXQoYmFzZVN5bWJvbCwgaW5kZXgpO1xuICAgIGlmIChzdW1tYXJ5KSB7XG4gICAgICB0aGlzLmFkZFN1bW1hcnkoc3VtbWFyeSk7XG4gICAgfVxuICAgIHJldHVybiBpbmRleDtcbiAgfVxuXG4gIHByaXZhdGUgbG9hZFN1bW1hcnkoc3ltYm9sOiBTdGF0aWNTeW1ib2wpOiBTdW1tYXJ5PFN0YXRpY1N5bWJvbD58bnVsbCB7XG4gICAgbGV0IHN1bW1hcnkgPSB0aGlzLnN1bW1hcnlSZXNvbHZlci5yZXNvbHZlU3VtbWFyeShzeW1ib2wpO1xuICAgIGlmICghc3VtbWFyeSkge1xuICAgICAgLy8gc29tZSBzeW1ib2xzIG1pZ2h0IG9yaWdpbmF0ZSBmcm9tIGEgcGxhaW4gdHlwZXNjcmlwdCBsaWJyYXJ5XG4gICAgICAvLyB0aGF0IGp1c3QgZXhwb3J0ZWQgLmQudHMgYW5kIC5tZXRhZGF0YS5qc29uIGZpbGVzLCBpLmUuIHdoZXJlIG5vIHN1bW1hcnlcbiAgICAgIC8vIGZpbGVzIHdlcmUgY3JlYXRlZC5cbiAgICAgIGNvbnN0IHJlc29sdmVkU3ltYm9sID0gdGhpcy5zeW1ib2xSZXNvbHZlci5yZXNvbHZlU3ltYm9sKHN5bWJvbCk7XG4gICAgICBpZiAocmVzb2x2ZWRTeW1ib2wpIHtcbiAgICAgICAgc3VtbWFyeSA9IHtzeW1ib2w6IHJlc29sdmVkU3ltYm9sLnN5bWJvbCwgbWV0YWRhdGE6IHJlc29sdmVkU3ltYm9sLm1ldGFkYXRhfTtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIHN1bW1hcnk7XG4gIH1cbn1cblxuY2xhc3MgRm9ySml0U2VyaWFsaXplciB7XG4gIHByaXZhdGUgZGF0YTogQXJyYXk8e1xuICAgIHN1bW1hcnk6IENvbXBpbGVUeXBlU3VtbWFyeSxcbiAgICBtZXRhZGF0YTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGF8Q29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhfENvbXBpbGVQaXBlTWV0YWRhdGF8XG4gICAgQ29tcGlsZVR5cGVNZXRhZGF0YXxudWxsLFxuICAgIGlzTGlicmFyeTogYm9vbGVhblxuICB9PiA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBvdXRwdXRDdHg6IE91dHB1dENvbnRleHQsIHByaXZhdGUgc3ltYm9sUmVzb2x2ZXI6IFN0YXRpY1N5bWJvbFJlc29sdmVyLFxuICAgICAgcHJpdmF0ZSBzdW1tYXJ5UmVzb2x2ZXI6IFN1bW1hcnlSZXNvbHZlcjxTdGF0aWNTeW1ib2w+KSB7fVxuXG4gIGFkZFNvdXJjZVR5cGUoXG4gICAgICBzdW1tYXJ5OiBDb21waWxlVHlwZVN1bW1hcnksIG1ldGFkYXRhOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YXxDb21waWxlRGlyZWN0aXZlTWV0YWRhdGF8XG4gICAgICBDb21waWxlUGlwZU1ldGFkYXRhfENvbXBpbGVUeXBlTWV0YWRhdGEpIHtcbiAgICB0aGlzLmRhdGEucHVzaCh7c3VtbWFyeSwgbWV0YWRhdGEsIGlzTGlicmFyeTogZmFsc2V9KTtcbiAgfVxuXG4gIGFkZExpYlR5cGUoc3VtbWFyeTogQ29tcGlsZVR5cGVTdW1tYXJ5KSB7XG4gICAgdGhpcy5kYXRhLnB1c2goe3N1bW1hcnksIG1ldGFkYXRhOiBudWxsLCBpc0xpYnJhcnk6IHRydWV9KTtcbiAgfVxuXG4gIHNlcmlhbGl6ZShleHBvcnRBc0Fycjoge3N5bWJvbDogU3RhdGljU3ltYm9sLCBleHBvcnRBczogc3RyaW5nfVtdKTogdm9pZCB7XG4gICAgY29uc3QgZXhwb3J0QXNCeVN5bWJvbCA9IG5ldyBNYXA8U3RhdGljU3ltYm9sLCBzdHJpbmc+KCk7XG4gICAgZm9yIChjb25zdCB7c3ltYm9sLCBleHBvcnRBc30gb2YgZXhwb3J0QXNBcnIpIHtcbiAgICAgIGV4cG9ydEFzQnlTeW1ib2wuc2V0KHN5bWJvbCwgZXhwb3J0QXMpO1xuICAgIH1cbiAgICBjb25zdCBuZ01vZHVsZVN5bWJvbHMgPSBuZXcgU2V0PFN0YXRpY1N5bWJvbD4oKTtcblxuICAgIGZvciAoY29uc3Qge3N1bW1hcnksIG1ldGFkYXRhLCBpc0xpYnJhcnl9IG9mIHRoaXMuZGF0YSkge1xuICAgICAgaWYgKHN1bW1hcnkuc3VtbWFyeUtpbmQgPT09IENvbXBpbGVTdW1tYXJ5S2luZC5OZ01vZHVsZSkge1xuICAgICAgICAvLyBjb2xsZWN0IHRoZSBzeW1ib2xzIHRoYXQgcmVmZXIgdG8gTmdNb2R1bGUgY2xhc3Nlcy5cbiAgICAgICAgLy8gTm90ZTogd2UgY2FuJ3QganVzdCByZWx5IG9uIGBzdW1tYXJ5LnR5cGUuc3VtbWFyeUtpbmRgIHRvIGRldGVybWluZSB0aGlzIGFzXG4gICAgICAgIC8vIHdlIGRvbid0IGFkZCB0aGUgc3VtbWFyaWVzIG9mIGFsbCByZWZlcmVuY2VkIHN5bWJvbHMgd2hlbiB3ZSBzZXJpYWxpemUgdHlwZSBzdW1tYXJpZXMuXG4gICAgICAgIC8vIFNlZSBzZXJpYWxpemVTdW1tYXJpZXMgZm9yIGRldGFpbHMuXG4gICAgICAgIG5nTW9kdWxlU3ltYm9scy5hZGQoc3VtbWFyeS50eXBlLnJlZmVyZW5jZSk7XG4gICAgICAgIGNvbnN0IG1vZFN1bW1hcnkgPSA8Q29tcGlsZU5nTW9kdWxlU3VtbWFyeT5zdW1tYXJ5O1xuICAgICAgICBmb3IgKGNvbnN0IG1vZCBvZiBtb2RTdW1tYXJ5Lm1vZHVsZXMpIHtcbiAgICAgICAgICBuZ01vZHVsZVN5bWJvbHMuYWRkKG1vZC5yZWZlcmVuY2UpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICBpZiAoIWlzTGlicmFyeSkge1xuICAgICAgICBjb25zdCBmbk5hbWUgPSBzdW1tYXJ5Rm9ySml0TmFtZShzdW1tYXJ5LnR5cGUucmVmZXJlbmNlLm5hbWUpO1xuICAgICAgICBjcmVhdGVTdW1tYXJ5Rm9ySml0RnVuY3Rpb24oXG4gICAgICAgICAgICB0aGlzLm91dHB1dEN0eCwgc3VtbWFyeS50eXBlLnJlZmVyZW5jZSxcbiAgICAgICAgICAgIHRoaXMuc2VyaWFsaXplU3VtbWFyeVdpdGhEZXBzKHN1bW1hcnksIG1ldGFkYXRhICEpKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBuZ01vZHVsZVN5bWJvbHMuZm9yRWFjaCgobmdNb2R1bGVTeW1ib2wpID0+IHtcbiAgICAgIGlmICh0aGlzLnN1bW1hcnlSZXNvbHZlci5pc0xpYnJhcnlGaWxlKG5nTW9kdWxlU3ltYm9sLmZpbGVQYXRoKSkge1xuICAgICAgICBsZXQgZXhwb3J0QXMgPSBleHBvcnRBc0J5U3ltYm9sLmdldChuZ01vZHVsZVN5bWJvbCkgfHwgbmdNb2R1bGVTeW1ib2wubmFtZTtcbiAgICAgICAgY29uc3Qgaml0RXhwb3J0QXNOYW1lID0gc3VtbWFyeUZvckppdE5hbWUoZXhwb3J0QXMpO1xuICAgICAgICB0aGlzLm91dHB1dEN0eC5zdGF0ZW1lbnRzLnB1c2goby52YXJpYWJsZShqaXRFeHBvcnRBc05hbWUpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLnNldCh0aGlzLnNlcmlhbGl6ZVN1bW1hcnlSZWYobmdNb2R1bGVTeW1ib2wpKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC50b0RlY2xTdG10KG51bGwsIFtvLlN0bXRNb2RpZmllci5FeHBvcnRlZF0pKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgc2VyaWFsaXplU3VtbWFyeVdpdGhEZXBzKFxuICAgICAgc3VtbWFyeTogQ29tcGlsZVR5cGVTdW1tYXJ5LCBtZXRhZGF0YTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGF8Q29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhfFxuICAgICAgQ29tcGlsZVBpcGVNZXRhZGF0YXxDb21waWxlVHlwZU1ldGFkYXRhKTogby5FeHByZXNzaW9uIHtcbiAgICBjb25zdCBleHByZXNzaW9uczogby5FeHByZXNzaW9uW10gPSBbdGhpcy5zZXJpYWxpemVTdW1tYXJ5KHN1bW1hcnkpXTtcbiAgICBsZXQgcHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdID0gW107XG4gICAgaWYgKG1ldGFkYXRhIGluc3RhbmNlb2YgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEpIHtcbiAgICAgIGV4cHJlc3Npb25zLnB1c2goLi4uXG4gICAgICAgICAgICAgICAgICAgICAgIC8vIEZvciBkaXJlY3RpdmVzIC8gcGlwZXMsIHdlIG9ubHkgYWRkIHRoZSBkZWNsYXJlZCBvbmVzLFxuICAgICAgICAgICAgICAgICAgICAgICAvLyBhbmQgcmVseSBvbiB0cmFuc2l0aXZlbHkgaW1wb3J0aW5nIE5nTW9kdWxlcyB0byBnZXQgdGhlIHRyYW5zaXRpdmVcbiAgICAgICAgICAgICAgICAgICAgICAgLy8gc3VtbWFyaWVzLlxuICAgICAgICAgICAgICAgICAgICAgICBtZXRhZGF0YS5kZWNsYXJlZERpcmVjdGl2ZXMuY29uY2F0KG1ldGFkYXRhLmRlY2xhcmVkUGlwZXMpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAubWFwKHR5cGUgPT4gdHlwZS5yZWZlcmVuY2UpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBGb3IgbW9kdWxlcyxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIHdlIGFsc28gYWRkIHRoZSBzdW1tYXJpZXMgZm9yIG1vZHVsZXNcbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIGZyb20gbGlicmFyaWVzLlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gVGhpcyBpcyBvayBhcyB3ZSBwcm9kdWNlIHJlZXhwb3J0cyBmb3IgYWxsIHRyYW5zaXRpdmUgbW9kdWxlcy5cbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC5jb25jYXQobWV0YWRhdGEudHJhbnNpdGl2ZU1vZHVsZS5tb2R1bGVzLm1hcCh0eXBlID0+IHR5cGUucmVmZXJlbmNlKVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLmZpbHRlcihyZWYgPT4gcmVmICE9PSBtZXRhZGF0YS50eXBlLnJlZmVyZW5jZSkpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAubWFwKChyZWYpID0+IHRoaXMuc2VyaWFsaXplU3VtbWFyeVJlZihyZWYpKSk7XG4gICAgICAvLyBOb3RlOiBXZSBkb24ndCB1c2UgYE5nTW9kdWxlU3VtbWFyeS5wcm92aWRlcnNgLCBhcyB0aGF0IG9uZSBpcyB0cmFuc2l0aXZlLFxuICAgICAgLy8gYW5kIHdlIGFscmVhZHkgaGF2ZSB0cmFuc2l0aXZlIG1vZHVsZXMuXG4gICAgICBwcm92aWRlcnMgPSBtZXRhZGF0YS5wcm92aWRlcnM7XG4gICAgfSBlbHNlIGlmIChzdW1tYXJ5LnN1bW1hcnlLaW5kID09PSBDb21waWxlU3VtbWFyeUtpbmQuRGlyZWN0aXZlKSB7XG4gICAgICBjb25zdCBkaXJTdW1tYXJ5ID0gPENvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5PnN1bW1hcnk7XG4gICAgICBwcm92aWRlcnMgPSBkaXJTdW1tYXJ5LnByb3ZpZGVycy5jb25jYXQoZGlyU3VtbWFyeS52aWV3UHJvdmlkZXJzKTtcbiAgICB9XG4gICAgLy8gTm90ZTogV2UgY2FuJ3QganVzdCByZWZlciB0byB0aGUgYG5nc3VtbWFyeS50c2AgZmlsZXMgZm9yIGB1c2VDbGFzc2AgcHJvdmlkZXJzIChhcyB3ZSBkbyBmb3JcbiAgICAvLyBkZWNsYXJlZERpcmVjdGl2ZXMgLyBkZWNsYXJlZFBpcGVzKSwgYXMgd2UgYWxsb3dcbiAgICAvLyBwcm92aWRlcnMgd2l0aG91dCBjdG9yIGFyZ3VtZW50cyB0byBza2lwIHRoZSBgQEluamVjdGFibGVgIGRlY29yYXRvcixcbiAgICAvLyBpLmUuIHdlIGRpZG4ndCBnZW5lcmF0ZSAubmdzdW1tYXJ5LnRzIGZpbGVzIGZvciB0aGVzZS5cbiAgICBleHByZXNzaW9ucy5wdXNoKFxuICAgICAgICAuLi5wcm92aWRlcnMuZmlsdGVyKHByb3ZpZGVyID0+ICEhcHJvdmlkZXIudXNlQ2xhc3MpLm1hcChwcm92aWRlciA9PiB0aGlzLnNlcmlhbGl6ZVN1bW1hcnkoe1xuICAgICAgICAgIHN1bW1hcnlLaW5kOiBDb21waWxlU3VtbWFyeUtpbmQuSW5qZWN0YWJsZSwgdHlwZTogcHJvdmlkZXIudXNlQ2xhc3NcbiAgICAgICAgfSBhcyBDb21waWxlVHlwZVN1bW1hcnkpKSk7XG4gICAgcmV0dXJuIG8ubGl0ZXJhbEFycihleHByZXNzaW9ucyk7XG4gIH1cblxuICBwcml2YXRlIHNlcmlhbGl6ZVN1bW1hcnlSZWYodHlwZVN5bWJvbDogU3RhdGljU3ltYm9sKTogby5FeHByZXNzaW9uIHtcbiAgICBjb25zdCBqaXRJbXBvcnRlZFN5bWJvbCA9IHRoaXMuc3ltYm9sUmVzb2x2ZXIuZ2V0U3RhdGljU3ltYm9sKFxuICAgICAgICBzdW1tYXJ5Rm9ySml0RmlsZU5hbWUodHlwZVN5bWJvbC5maWxlUGF0aCksIHN1bW1hcnlGb3JKaXROYW1lKHR5cGVTeW1ib2wubmFtZSkpO1xuICAgIHJldHVybiB0aGlzLm91dHB1dEN0eC5pbXBvcnRFeHByKGppdEltcG9ydGVkU3ltYm9sKTtcbiAgfVxuXG4gIHByaXZhdGUgc2VyaWFsaXplU3VtbWFyeShkYXRhOiB7W2tleTogc3RyaW5nXTogYW55fSk6IG8uRXhwcmVzc2lvbiB7XG4gICAgY29uc3Qgb3V0cHV0Q3R4ID0gdGhpcy5vdXRwdXRDdHg7XG5cbiAgICBjbGFzcyBUcmFuc2Zvcm1lciBpbXBsZW1lbnRzIFZhbHVlVmlzaXRvciB7XG4gICAgICB2aXNpdEFycmF5KGFycjogYW55W10sIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgICAgIHJldHVybiBvLmxpdGVyYWxBcnIoYXJyLm1hcChlbnRyeSA9PiB2aXNpdFZhbHVlKGVudHJ5LCB0aGlzLCBjb250ZXh0KSkpO1xuICAgICAgfVxuICAgICAgdmlzaXRTdHJpbmdNYXAobWFwOiB7W2tleTogc3RyaW5nXTogYW55fSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICAgICAgcmV0dXJuIG5ldyBvLkxpdGVyYWxNYXBFeHByKE9iamVjdC5rZXlzKG1hcCkubWFwKFxuICAgICAgICAgICAgKGtleSkgPT4gbmV3IG8uTGl0ZXJhbE1hcEVudHJ5KGtleSwgdmlzaXRWYWx1ZShtYXBba2V5XSwgdGhpcywgY29udGV4dCksIGZhbHNlKSkpO1xuICAgICAgfVxuICAgICAgdmlzaXRQcmltaXRpdmUodmFsdWU6IGFueSwgY29udGV4dDogYW55KTogYW55IHsgcmV0dXJuIG8ubGl0ZXJhbCh2YWx1ZSk7IH1cbiAgICAgIHZpc2l0T3RoZXIodmFsdWU6IGFueSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICAgICAgaWYgKHZhbHVlIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgICAgICAgcmV0dXJuIG91dHB1dEN0eC5pbXBvcnRFeHByKHZhbHVlKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoYElsbGVnYWwgU3RhdGU6IEVuY291bnRlcmVkIHZhbHVlICR7dmFsdWV9YCk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gdmlzaXRWYWx1ZShkYXRhLCBuZXcgVHJhbnNmb3JtZXIoKSwgbnVsbCk7XG4gIH1cbn1cblxuY2xhc3MgRnJvbUpzb25EZXNlcmlhbGl6ZXIgZXh0ZW5kcyBWYWx1ZVRyYW5zZm9ybWVyIHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgc3ltYm9scyAhOiBTdGF0aWNTeW1ib2xbXTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgc3ltYm9sQ2FjaGU6IFN0YXRpY1N5bWJvbENhY2hlLFxuICAgICAgcHJpdmF0ZSBzdW1tYXJ5UmVzb2x2ZXI6IFN1bW1hcnlSZXNvbHZlcjxTdGF0aWNTeW1ib2w+KSB7XG4gICAgc3VwZXIoKTtcbiAgfVxuXG4gIGRlc2VyaWFsaXplKGxpYnJhcnlGaWxlTmFtZTogc3RyaW5nLCBqc29uOiBzdHJpbmcpOiB7XG4gICAgbW9kdWxlTmFtZTogc3RyaW5nIHwgbnVsbCxcbiAgICBzdW1tYXJpZXM6IFN1bW1hcnk8U3RhdGljU3ltYm9sPltdLFxuICAgIGltcG9ydEFzOiB7c3ltYm9sOiBTdGF0aWNTeW1ib2wsIGltcG9ydEFzOiBTdGF0aWNTeW1ib2x9W11cbiAgfSB7XG4gICAgY29uc3QgZGF0YToge21vZHVsZU5hbWU6IHN0cmluZyB8IG51bGwsIHN1bW1hcmllczogYW55W10sIHN5bWJvbHM6IGFueVtdfSA9IEpTT04ucGFyc2UoanNvbik7XG4gICAgY29uc3QgYWxsSW1wb3J0QXM6IHtzeW1ib2w6IFN0YXRpY1N5bWJvbCwgaW1wb3J0QXM6IFN0YXRpY1N5bWJvbH1bXSA9IFtdO1xuICAgIHRoaXMuc3ltYm9scyA9IGRhdGEuc3ltYm9scy5tYXAoXG4gICAgICAgIChzZXJpYWxpemVkU3ltYm9sKSA9PiB0aGlzLnN5bWJvbENhY2hlLmdldChcbiAgICAgICAgICAgIHRoaXMuc3VtbWFyeVJlc29sdmVyLmZyb21TdW1tYXJ5RmlsZU5hbWUoc2VyaWFsaXplZFN5bWJvbC5maWxlUGF0aCwgbGlicmFyeUZpbGVOYW1lKSxcbiAgICAgICAgICAgIHNlcmlhbGl6ZWRTeW1ib2wubmFtZSkpO1xuICAgIGRhdGEuc3ltYm9scy5mb3JFYWNoKChzZXJpYWxpemVkU3ltYm9sLCBpbmRleCkgPT4ge1xuICAgICAgY29uc3Qgc3ltYm9sID0gdGhpcy5zeW1ib2xzW2luZGV4XTtcbiAgICAgIGNvbnN0IGltcG9ydEFzID0gc2VyaWFsaXplZFN5bWJvbC5pbXBvcnRBcztcbiAgICAgIGlmICh0eXBlb2YgaW1wb3J0QXMgPT09ICdudW1iZXInKSB7XG4gICAgICAgIGFsbEltcG9ydEFzLnB1c2goe3N5bWJvbCwgaW1wb3J0QXM6IHRoaXMuc3ltYm9sc1tpbXBvcnRBc119KTtcbiAgICAgIH0gZWxzZSBpZiAodHlwZW9mIGltcG9ydEFzID09PSAnc3RyaW5nJykge1xuICAgICAgICBhbGxJbXBvcnRBcy5wdXNoKFxuICAgICAgICAgICAge3N5bWJvbCwgaW1wb3J0QXM6IHRoaXMuc3ltYm9sQ2FjaGUuZ2V0KG5nZmFjdG9yeUZpbGVQYXRoKGxpYnJhcnlGaWxlTmFtZSksIGltcG9ydEFzKX0pO1xuICAgICAgfVxuICAgIH0pO1xuICAgIGNvbnN0IHN1bW1hcmllcyA9IHZpc2l0VmFsdWUoZGF0YS5zdW1tYXJpZXMsIHRoaXMsIG51bGwpIGFzIFN1bW1hcnk8U3RhdGljU3ltYm9sPltdO1xuICAgIHJldHVybiB7bW9kdWxlTmFtZTogZGF0YS5tb2R1bGVOYW1lLCBzdW1tYXJpZXMsIGltcG9ydEFzOiBhbGxJbXBvcnRBc307XG4gIH1cblxuICB2aXNpdFN0cmluZ01hcChtYXA6IHtba2V5OiBzdHJpbmddOiBhbnl9LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIGlmICgnX19zeW1ib2wnIGluIG1hcCkge1xuICAgICAgY29uc3QgYmFzZVN5bWJvbCA9IHRoaXMuc3ltYm9sc1ttYXBbJ19fc3ltYm9sJ11dO1xuICAgICAgY29uc3QgbWVtYmVycyA9IG1hcFsnbWVtYmVycyddO1xuICAgICAgcmV0dXJuIG1lbWJlcnMubGVuZ3RoID8gdGhpcy5zeW1ib2xDYWNoZS5nZXQoYmFzZVN5bWJvbC5maWxlUGF0aCwgYmFzZVN5bWJvbC5uYW1lLCBtZW1iZXJzKSA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICBiYXNlU3ltYm9sO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gc3VwZXIudmlzaXRTdHJpbmdNYXAobWFwLCBjb250ZXh0KTtcbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNDYWxsKG1ldGFkYXRhOiBhbnkpOiBib29sZWFuIHtcbiAgcmV0dXJuIG1ldGFkYXRhICYmIG1ldGFkYXRhLl9fc3ltYm9saWMgPT09ICdjYWxsJztcbn1cblxuZnVuY3Rpb24gaXNGdW5jdGlvbkNhbGwobWV0YWRhdGE6IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gaXNDYWxsKG1ldGFkYXRhKSAmJiB1bndyYXBSZXNvbHZlZE1ldGFkYXRhKG1ldGFkYXRhLmV4cHJlc3Npb24pIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sO1xufVxuXG5mdW5jdGlvbiBpc01ldGhvZENhbGxPblZhcmlhYmxlKG1ldGFkYXRhOiBhbnkpOiBib29sZWFuIHtcbiAgcmV0dXJuIGlzQ2FsbChtZXRhZGF0YSkgJiYgbWV0YWRhdGEuZXhwcmVzc2lvbiAmJiBtZXRhZGF0YS5leHByZXNzaW9uLl9fc3ltYm9saWMgPT09ICdzZWxlY3QnICYmXG4gICAgICB1bndyYXBSZXNvbHZlZE1ldGFkYXRhKG1ldGFkYXRhLmV4cHJlc3Npb24uZXhwcmVzc2lvbikgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2w7XG59XG4iXX0=