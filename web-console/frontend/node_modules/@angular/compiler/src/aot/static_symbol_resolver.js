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
        define("@angular/compiler/src/aot/static_symbol_resolver", ["require", "exports", "tslib", "@angular/compiler/src/util", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/aot/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var util_1 = require("@angular/compiler/src/util");
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var util_2 = require("@angular/compiler/src/aot/util");
    var TS = /^(?!.*\.d\.ts$).*\.ts$/;
    var ResolvedStaticSymbol = /** @class */ (function () {
        function ResolvedStaticSymbol(symbol, metadata) {
            this.symbol = symbol;
            this.metadata = metadata;
        }
        return ResolvedStaticSymbol;
    }());
    exports.ResolvedStaticSymbol = ResolvedStaticSymbol;
    var SUPPORTED_SCHEMA_VERSION = 4;
    /**
     * This class is responsible for loading metadata per symbol,
     * and normalizing references between symbols.
     *
     * Internally, it only uses symbols without members,
     * and deduces the values for symbols with members based
     * on these symbols.
     */
    var StaticSymbolResolver = /** @class */ (function () {
        function StaticSymbolResolver(host, staticSymbolCache, summaryResolver, errorRecorder) {
            this.host = host;
            this.staticSymbolCache = staticSymbolCache;
            this.summaryResolver = summaryResolver;
            this.errorRecorder = errorRecorder;
            this.metadataCache = new Map();
            // Note: this will only contain StaticSymbols without members!
            this.resolvedSymbols = new Map();
            this.resolvedFilePaths = new Set();
            // Note: this will only contain StaticSymbols without members!
            this.importAs = new Map();
            this.symbolResourcePaths = new Map();
            this.symbolFromFile = new Map();
            this.knownFileNameToModuleNames = new Map();
        }
        StaticSymbolResolver.prototype.resolveSymbol = function (staticSymbol) {
            if (staticSymbol.members.length > 0) {
                return this._resolveSymbolMembers(staticSymbol);
            }
            // Note: always ask for a summary first,
            // as we might have read shallow metadata via a .d.ts file
            // for the symbol.
            var resultFromSummary = this._resolveSymbolFromSummary(staticSymbol);
            if (resultFromSummary) {
                return resultFromSummary;
            }
            var resultFromCache = this.resolvedSymbols.get(staticSymbol);
            if (resultFromCache) {
                return resultFromCache;
            }
            // Note: Some users use libraries that were not compiled with ngc, i.e. they don't
            // have summaries, only .d.ts files. So we always need to check both, the summary
            // and metadata.
            this._createSymbolsOf(staticSymbol.filePath);
            return this.resolvedSymbols.get(staticSymbol);
        };
        /**
         * getImportAs produces a symbol that can be used to import the given symbol.
         * The import might be different than the symbol if the symbol is exported from
         * a library with a summary; in which case we want to import the symbol from the
         * ngfactory re-export instead of directly to avoid introducing a direct dependency
         * on an otherwise indirect dependency.
         *
         * @param staticSymbol the symbol for which to generate a import symbol
         */
        StaticSymbolResolver.prototype.getImportAs = function (staticSymbol, useSummaries) {
            if (useSummaries === void 0) { useSummaries = true; }
            if (staticSymbol.members.length) {
                var baseSymbol = this.getStaticSymbol(staticSymbol.filePath, staticSymbol.name);
                var baseImportAs = this.getImportAs(baseSymbol, useSummaries);
                return baseImportAs ?
                    this.getStaticSymbol(baseImportAs.filePath, baseImportAs.name, staticSymbol.members) :
                    null;
            }
            var summarizedFileName = util_2.stripSummaryForJitFileSuffix(staticSymbol.filePath);
            if (summarizedFileName !== staticSymbol.filePath) {
                var summarizedName = util_2.stripSummaryForJitNameSuffix(staticSymbol.name);
                var baseSymbol = this.getStaticSymbol(summarizedFileName, summarizedName, staticSymbol.members);
                var baseImportAs = this.getImportAs(baseSymbol, useSummaries);
                return baseImportAs ?
                    this.getStaticSymbol(util_2.summaryForJitFileName(baseImportAs.filePath), util_2.summaryForJitName(baseImportAs.name), baseSymbol.members) :
                    null;
            }
            var result = (useSummaries && this.summaryResolver.getImportAs(staticSymbol)) || null;
            if (!result) {
                result = this.importAs.get(staticSymbol);
            }
            return result;
        };
        /**
         * getResourcePath produces the path to the original location of the symbol and should
         * be used to determine the relative location of resource references recorded in
         * symbol metadata.
         */
        StaticSymbolResolver.prototype.getResourcePath = function (staticSymbol) {
            return this.symbolResourcePaths.get(staticSymbol) || staticSymbol.filePath;
        };
        /**
         * getTypeArity returns the number of generic type parameters the given symbol
         * has. If the symbol is not a type the result is null.
         */
        StaticSymbolResolver.prototype.getTypeArity = function (staticSymbol) {
            // If the file is a factory/ngsummary file, don't resolve the symbol as doing so would
            // cause the metadata for an factory/ngsummary file to be loaded which doesn't exist.
            // All references to generated classes must include the correct arity whenever
            // generating code.
            if (util_2.isGeneratedFile(staticSymbol.filePath)) {
                return null;
            }
            var resolvedSymbol = unwrapResolvedMetadata(this.resolveSymbol(staticSymbol));
            while (resolvedSymbol && resolvedSymbol.metadata instanceof static_symbol_1.StaticSymbol) {
                resolvedSymbol = unwrapResolvedMetadata(this.resolveSymbol(resolvedSymbol.metadata));
            }
            return (resolvedSymbol && resolvedSymbol.metadata && resolvedSymbol.metadata.arity) || null;
        };
        StaticSymbolResolver.prototype.getKnownModuleName = function (filePath) {
            return this.knownFileNameToModuleNames.get(filePath) || null;
        };
        StaticSymbolResolver.prototype.recordImportAs = function (sourceSymbol, targetSymbol) {
            sourceSymbol.assertNoMembers();
            targetSymbol.assertNoMembers();
            this.importAs.set(sourceSymbol, targetSymbol);
        };
        StaticSymbolResolver.prototype.recordModuleNameForFileName = function (fileName, moduleName) {
            this.knownFileNameToModuleNames.set(fileName, moduleName);
        };
        /**
         * Invalidate all information derived from the given file.
         *
         * @param fileName the file to invalidate
         */
        StaticSymbolResolver.prototype.invalidateFile = function (fileName) {
            var e_1, _a;
            this.metadataCache.delete(fileName);
            this.resolvedFilePaths.delete(fileName);
            var symbols = this.symbolFromFile.get(fileName);
            if (symbols) {
                this.symbolFromFile.delete(fileName);
                try {
                    for (var symbols_1 = tslib_1.__values(symbols), symbols_1_1 = symbols_1.next(); !symbols_1_1.done; symbols_1_1 = symbols_1.next()) {
                        var symbol = symbols_1_1.value;
                        this.resolvedSymbols.delete(symbol);
                        this.importAs.delete(symbol);
                        this.symbolResourcePaths.delete(symbol);
                    }
                }
                catch (e_1_1) { e_1 = { error: e_1_1 }; }
                finally {
                    try {
                        if (symbols_1_1 && !symbols_1_1.done && (_a = symbols_1.return)) _a.call(symbols_1);
                    }
                    finally { if (e_1) throw e_1.error; }
                }
            }
        };
        /** @internal */
        StaticSymbolResolver.prototype.ignoreErrorsFor = function (cb) {
            var recorder = this.errorRecorder;
            this.errorRecorder = function () { };
            try {
                return cb();
            }
            finally {
                this.errorRecorder = recorder;
            }
        };
        StaticSymbolResolver.prototype._resolveSymbolMembers = function (staticSymbol) {
            var members = staticSymbol.members;
            var baseResolvedSymbol = this.resolveSymbol(this.getStaticSymbol(staticSymbol.filePath, staticSymbol.name));
            if (!baseResolvedSymbol) {
                return null;
            }
            var baseMetadata = unwrapResolvedMetadata(baseResolvedSymbol.metadata);
            if (baseMetadata instanceof static_symbol_1.StaticSymbol) {
                return new ResolvedStaticSymbol(staticSymbol, this.getStaticSymbol(baseMetadata.filePath, baseMetadata.name, members));
            }
            else if (baseMetadata && baseMetadata.__symbolic === 'class') {
                if (baseMetadata.statics && members.length === 1) {
                    return new ResolvedStaticSymbol(staticSymbol, baseMetadata.statics[members[0]]);
                }
            }
            else {
                var value = baseMetadata;
                for (var i = 0; i < members.length && value; i++) {
                    value = value[members[i]];
                }
                return new ResolvedStaticSymbol(staticSymbol, value);
            }
            return null;
        };
        StaticSymbolResolver.prototype._resolveSymbolFromSummary = function (staticSymbol) {
            var summary = this.summaryResolver.resolveSummary(staticSymbol);
            return summary ? new ResolvedStaticSymbol(staticSymbol, summary.metadata) : null;
        };
        /**
         * getStaticSymbol produces a Type whose metadata is known but whose implementation is not loaded.
         * All types passed to the StaticResolver should be pseudo-types returned by this method.
         *
         * @param declarationFile the absolute path of the file where the symbol is declared
         * @param name the name of the type.
         * @param members a symbol for a static member of the named type
         */
        StaticSymbolResolver.prototype.getStaticSymbol = function (declarationFile, name, members) {
            return this.staticSymbolCache.get(declarationFile, name, members);
        };
        /**
         * hasDecorators checks a file's metadata for the presence of decorators without evaluating the
         * metadata.
         *
         * @param filePath the absolute path to examine for decorators.
         * @returns true if any class in the file has a decorator.
         */
        StaticSymbolResolver.prototype.hasDecorators = function (filePath) {
            var metadata = this.getModuleMetadata(filePath);
            if (metadata['metadata']) {
                return Object.keys(metadata['metadata']).some(function (metadataKey) {
                    var entry = metadata['metadata'][metadataKey];
                    return entry && entry.__symbolic === 'class' && entry.decorators;
                });
            }
            return false;
        };
        StaticSymbolResolver.prototype.getSymbolsOf = function (filePath) {
            var summarySymbols = this.summaryResolver.getSymbolsOf(filePath);
            if (summarySymbols) {
                return summarySymbols;
            }
            // Note: Some users use libraries that were not compiled with ngc, i.e. they don't
            // have summaries, only .d.ts files, but `summaryResolver.isLibraryFile` returns true.
            this._createSymbolsOf(filePath);
            var metadataSymbols = [];
            this.resolvedSymbols.forEach(function (resolvedSymbol) {
                if (resolvedSymbol.symbol.filePath === filePath) {
                    metadataSymbols.push(resolvedSymbol.symbol);
                }
            });
            return metadataSymbols;
        };
        StaticSymbolResolver.prototype._createSymbolsOf = function (filePath) {
            var e_2, _a;
            var _this = this;
            if (this.resolvedFilePaths.has(filePath)) {
                return;
            }
            this.resolvedFilePaths.add(filePath);
            var resolvedSymbols = [];
            var metadata = this.getModuleMetadata(filePath);
            if (metadata['importAs']) {
                // Index bundle indices should use the importAs module name defined
                // in the bundle.
                this.knownFileNameToModuleNames.set(filePath, metadata['importAs']);
            }
            // handle the symbols in one of the re-export location
            if (metadata['exports']) {
                var _loop_1 = function (moduleExport) {
                    // handle the symbols in the list of explicitly re-exported symbols.
                    if (moduleExport.export) {
                        moduleExport.export.forEach(function (exportSymbol) {
                            var symbolName;
                            if (typeof exportSymbol === 'string') {
                                symbolName = exportSymbol;
                            }
                            else {
                                symbolName = exportSymbol.as;
                            }
                            symbolName = unescapeIdentifier(symbolName);
                            var symName = symbolName;
                            if (typeof exportSymbol !== 'string') {
                                symName = unescapeIdentifier(exportSymbol.name);
                            }
                            var resolvedModule = _this.resolveModule(moduleExport.from, filePath);
                            if (resolvedModule) {
                                var targetSymbol = _this.getStaticSymbol(resolvedModule, symName);
                                var sourceSymbol = _this.getStaticSymbol(filePath, symbolName);
                                resolvedSymbols.push(_this.createExport(sourceSymbol, targetSymbol));
                            }
                        });
                    }
                    else {
                        // handle the symbols via export * directives.
                        var resolvedModule = this_1.resolveModule(moduleExport.from, filePath);
                        if (resolvedModule) {
                            var nestedExports = this_1.getSymbolsOf(resolvedModule);
                            nestedExports.forEach(function (targetSymbol) {
                                var sourceSymbol = _this.getStaticSymbol(filePath, targetSymbol.name);
                                resolvedSymbols.push(_this.createExport(sourceSymbol, targetSymbol));
                            });
                        }
                    }
                };
                var this_1 = this;
                try {
                    for (var _b = tslib_1.__values(metadata['exports']), _c = _b.next(); !_c.done; _c = _b.next()) {
                        var moduleExport = _c.value;
                        _loop_1(moduleExport);
                    }
                }
                catch (e_2_1) { e_2 = { error: e_2_1 }; }
                finally {
                    try {
                        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                    }
                    finally { if (e_2) throw e_2.error; }
                }
            }
            // handle the actual metadata. Has to be after the exports
            // as there might be collisions in the names, and we want the symbols
            // of the current module to win ofter reexports.
            if (metadata['metadata']) {
                // handle direct declarations of the symbol
                var topLevelSymbolNames_1 = new Set(Object.keys(metadata['metadata']).map(unescapeIdentifier));
                var origins_1 = metadata['origins'] || {};
                Object.keys(metadata['metadata']).forEach(function (metadataKey) {
                    var symbolMeta = metadata['metadata'][metadataKey];
                    var name = unescapeIdentifier(metadataKey);
                    var symbol = _this.getStaticSymbol(filePath, name);
                    var origin = origins_1.hasOwnProperty(metadataKey) && origins_1[metadataKey];
                    if (origin) {
                        // If the symbol is from a bundled index, use the declaration location of the
                        // symbol so relative references (such as './my.html') will be calculated
                        // correctly.
                        var originFilePath = _this.resolveModule(origin, filePath);
                        if (!originFilePath) {
                            _this.reportError(new Error("Couldn't resolve original symbol for " + origin + " from " + _this.host.getOutputName(filePath)));
                        }
                        else {
                            _this.symbolResourcePaths.set(symbol, originFilePath);
                        }
                    }
                    resolvedSymbols.push(_this.createResolvedSymbol(symbol, filePath, topLevelSymbolNames_1, symbolMeta));
                });
            }
            resolvedSymbols.forEach(function (resolvedSymbol) { return _this.resolvedSymbols.set(resolvedSymbol.symbol, resolvedSymbol); });
            this.symbolFromFile.set(filePath, resolvedSymbols.map(function (resolvedSymbol) { return resolvedSymbol.symbol; }));
        };
        StaticSymbolResolver.prototype.createResolvedSymbol = function (sourceSymbol, topLevelPath, topLevelSymbolNames, metadata) {
            var _this = this;
            // For classes that don't have Angular summaries / metadata,
            // we only keep their arity, but nothing else
            // (e.g. their constructor parameters).
            // We do this to prevent introducing deep imports
            // as we didn't generate .ngfactory.ts files with proper reexports.
            var isTsFile = TS.test(sourceSymbol.filePath);
            if (this.summaryResolver.isLibraryFile(sourceSymbol.filePath) && !isTsFile && metadata &&
                metadata['__symbolic'] === 'class') {
                var transformedMeta_1 = { __symbolic: 'class', arity: metadata.arity };
                return new ResolvedStaticSymbol(sourceSymbol, transformedMeta_1);
            }
            var _originalFileMemo;
            var getOriginalName = function () {
                if (!_originalFileMemo) {
                    // Guess what the original file name is from the reference. If it has a `.d.ts` extension
                    // replace it with `.ts`. If it already has `.ts` just leave it in place. If it doesn't have
                    // .ts or .d.ts, append `.ts'. Also, if it is in `node_modules`, trim the `node_module`
                    // location as it is not important to finding the file.
                    _originalFileMemo =
                        _this.host.getOutputName(topLevelPath.replace(/((\.ts)|(\.d\.ts)|)$/, '.ts')
                            .replace(/^.*node_modules[/\\]/, ''));
                }
                return _originalFileMemo;
            };
            var self = this;
            var ReferenceTransformer = /** @class */ (function (_super) {
                tslib_1.__extends(ReferenceTransformer, _super);
                function ReferenceTransformer() {
                    return _super !== null && _super.apply(this, arguments) || this;
                }
                ReferenceTransformer.prototype.visitStringMap = function (map, functionParams) {
                    var symbolic = map['__symbolic'];
                    if (symbolic === 'function') {
                        var oldLen = functionParams.length;
                        functionParams.push.apply(functionParams, tslib_1.__spread((map['parameters'] || [])));
                        var result = _super.prototype.visitStringMap.call(this, map, functionParams);
                        functionParams.length = oldLen;
                        return result;
                    }
                    else if (symbolic === 'reference') {
                        var module = map['module'];
                        var name_1 = map['name'] ? unescapeIdentifier(map['name']) : map['name'];
                        if (!name_1) {
                            return null;
                        }
                        var filePath = void 0;
                        if (module) {
                            filePath = self.resolveModule(module, sourceSymbol.filePath);
                            if (!filePath) {
                                return {
                                    __symbolic: 'error',
                                    message: "Could not resolve " + module + " relative to " + self.host.getMetadataFor(sourceSymbol.filePath) + ".",
                                    line: map['line'],
                                    character: map['character'],
                                    fileName: getOriginalName()
                                };
                            }
                            return {
                                __symbolic: 'resolved',
                                symbol: self.getStaticSymbol(filePath, name_1),
                                line: map['line'],
                                character: map['character'],
                                fileName: getOriginalName()
                            };
                        }
                        else if (functionParams.indexOf(name_1) >= 0) {
                            // reference to a function parameter
                            return { __symbolic: 'reference', name: name_1 };
                        }
                        else {
                            if (topLevelSymbolNames.has(name_1)) {
                                return self.getStaticSymbol(topLevelPath, name_1);
                            }
                            // ambient value
                            null;
                        }
                    }
                    else if (symbolic === 'error') {
                        return tslib_1.__assign({}, map, { fileName: getOriginalName() });
                    }
                    else {
                        return _super.prototype.visitStringMap.call(this, map, functionParams);
                    }
                };
                return ReferenceTransformer;
            }(util_1.ValueTransformer));
            var transformedMeta = util_1.visitValue(metadata, new ReferenceTransformer(), []);
            var unwrappedTransformedMeta = unwrapResolvedMetadata(transformedMeta);
            if (unwrappedTransformedMeta instanceof static_symbol_1.StaticSymbol) {
                return this.createExport(sourceSymbol, unwrappedTransformedMeta);
            }
            return new ResolvedStaticSymbol(sourceSymbol, transformedMeta);
        };
        StaticSymbolResolver.prototype.createExport = function (sourceSymbol, targetSymbol) {
            sourceSymbol.assertNoMembers();
            targetSymbol.assertNoMembers();
            if (this.summaryResolver.isLibraryFile(sourceSymbol.filePath) &&
                this.summaryResolver.isLibraryFile(targetSymbol.filePath)) {
                // This case is for an ng library importing symbols from a plain ts library
                // transitively.
                // Note: We rely on the fact that we discover symbols in the direction
                // from source files to library files
                this.importAs.set(targetSymbol, this.getImportAs(sourceSymbol) || sourceSymbol);
            }
            return new ResolvedStaticSymbol(sourceSymbol, targetSymbol);
        };
        StaticSymbolResolver.prototype.reportError = function (error, context, path) {
            if (this.errorRecorder) {
                this.errorRecorder(error, (context && context.filePath) || path);
            }
            else {
                throw error;
            }
        };
        /**
         * @param module an absolute path to a module file.
         */
        StaticSymbolResolver.prototype.getModuleMetadata = function (module) {
            var moduleMetadata = this.metadataCache.get(module);
            if (!moduleMetadata) {
                var moduleMetadatas = this.host.getMetadataFor(module);
                if (moduleMetadatas) {
                    var maxVersion_1 = -1;
                    moduleMetadatas.forEach(function (md) {
                        if (md && md['version'] > maxVersion_1) {
                            maxVersion_1 = md['version'];
                            moduleMetadata = md;
                        }
                    });
                }
                if (!moduleMetadata) {
                    moduleMetadata =
                        { __symbolic: 'module', version: SUPPORTED_SCHEMA_VERSION, module: module, metadata: {} };
                }
                if (moduleMetadata['version'] != SUPPORTED_SCHEMA_VERSION) {
                    var errorMessage = moduleMetadata['version'] == 2 ?
                        "Unsupported metadata version " + moduleMetadata['version'] + " for module " + module + ". This module should be compiled with a newer version of ngc" :
                        "Metadata version mismatch for module " + this.host.getOutputName(module) + ", found version " + moduleMetadata['version'] + ", expected " + SUPPORTED_SCHEMA_VERSION;
                    this.reportError(new Error(errorMessage));
                }
                this.metadataCache.set(module, moduleMetadata);
            }
            return moduleMetadata;
        };
        StaticSymbolResolver.prototype.getSymbolByModule = function (module, symbolName, containingFile) {
            var filePath = this.resolveModule(module, containingFile);
            if (!filePath) {
                this.reportError(new Error("Could not resolve module " + module + (containingFile ? ' relative to ' +
                    this.host.getOutputName(containingFile) : '')));
                return this.getStaticSymbol("ERROR:" + module, symbolName);
            }
            return this.getStaticSymbol(filePath, symbolName);
        };
        StaticSymbolResolver.prototype.resolveModule = function (module, containingFile) {
            try {
                return this.host.moduleNameToFileName(module, containingFile);
            }
            catch (e) {
                console.error("Could not resolve module '" + module + "' relative to file " + containingFile);
                this.reportError(e, undefined, containingFile);
            }
            return null;
        };
        return StaticSymbolResolver;
    }());
    exports.StaticSymbolResolver = StaticSymbolResolver;
    // Remove extra underscore from escaped identifier.
    // See https://github.com/Microsoft/TypeScript/blob/master/src/compiler/utilities.ts
    function unescapeIdentifier(identifier) {
        return identifier.startsWith('___') ? identifier.substr(1) : identifier;
    }
    exports.unescapeIdentifier = unescapeIdentifier;
    function unwrapResolvedMetadata(metadata) {
        if (metadata && metadata.__symbolic === 'resolved') {
            return metadata.symbol;
        }
        return metadata;
    }
    exports.unwrapResolvedMetadata = unwrapResolvedMetadata;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RhdGljX3N5bWJvbF9yZXNvbHZlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9hb3Qvc3RhdGljX3N5bWJvbF9yZXNvbHZlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFHSCxtREFBcUQ7SUFFckQseUVBQWdFO0lBQ2hFLHVEQUE2STtJQUU3SSxJQUFNLEVBQUUsR0FBRyx3QkFBd0IsQ0FBQztJQUVwQztRQUNFLDhCQUFtQixNQUFvQixFQUFTLFFBQWE7WUFBMUMsV0FBTSxHQUFOLE1BQU0sQ0FBYztZQUFTLGFBQVEsR0FBUixRQUFRLENBQUs7UUFBRyxDQUFDO1FBQ25FLDJCQUFDO0lBQUQsQ0FBQyxBQUZELElBRUM7SUFGWSxvREFBb0I7SUFtQ2pDLElBQU0sd0JBQXdCLEdBQUcsQ0FBQyxDQUFDO0lBRW5DOzs7Ozs7O09BT0c7SUFDSDtRQVdFLDhCQUNZLElBQThCLEVBQVUsaUJBQW9DLEVBQzVFLGVBQThDLEVBQzlDLGFBQXVEO1lBRnZELFNBQUksR0FBSixJQUFJLENBQTBCO1lBQVUsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUFtQjtZQUM1RSxvQkFBZSxHQUFmLGVBQWUsQ0FBK0I7WUFDOUMsa0JBQWEsR0FBYixhQUFhLENBQTBDO1lBYjNELGtCQUFhLEdBQUcsSUFBSSxHQUFHLEVBQWdDLENBQUM7WUFDaEUsOERBQThEO1lBQ3RELG9CQUFlLEdBQUcsSUFBSSxHQUFHLEVBQXNDLENBQUM7WUFDaEUsc0JBQWlCLEdBQUcsSUFBSSxHQUFHLEVBQVUsQ0FBQztZQUM5Qyw4REFBOEQ7WUFDdEQsYUFBUSxHQUFHLElBQUksR0FBRyxFQUE4QixDQUFDO1lBQ2pELHdCQUFtQixHQUFHLElBQUksR0FBRyxFQUF3QixDQUFDO1lBQ3RELG1CQUFjLEdBQUcsSUFBSSxHQUFHLEVBQTBCLENBQUM7WUFDbkQsK0JBQTBC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SUFBSSxRQUFRLENBQUMsVUFBVSxLQUFLLFVBQVUsRUFBRTtZQUNsRCxPQUFPLFFBQVEsQ0FBQyxNQUFNLENBQUM7U0FDeEI7UUFDRCxPQUFPLFFBQVEsQ0FBQztJQUNsQixDQUFDO0lBTEQsd0RBS0MiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U3VtbWFyeVJlc29sdmVyfSBmcm9tICcuLi9zdW1tYXJ5X3Jlc29sdmVyJztcbmltcG9ydCB7VmFsdWVUcmFuc2Zvcm1lciwgdmlzaXRWYWx1ZX0gZnJvbSAnLi4vdXRpbCc7XG5cbmltcG9ydCB7U3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2xDYWNoZX0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sJztcbmltcG9ydCB7aXNHZW5lcmF0ZWRGaWxlLCBzdHJpcFN1bW1hcnlGb3JKaXRGaWxlU3VmZml4LCBzdHJpcFN1bW1hcnlGb3JKaXROYW1lU3VmZml4LCBzdW1tYXJ5Rm9ySml0RmlsZU5hbWUsIHN1bW1hcnlGb3JKaXROYW1lfSBmcm9tICcuL3V0aWwnO1xuXG5jb25zdCBUUyA9IC9eKD8hLipcXC5kXFwudHMkKS4qXFwudHMkLztcblxuZXhwb3J0IGNsYXNzIFJlc29sdmVkU3RhdGljU3ltYm9sIHtcbiAgY29uc3RydWN0b3IocHVibGljIHN5bWJvbDogU3RhdGljU3ltYm9sLCBwdWJsaWMgbWV0YWRhdGE6IGFueSkge31cbn1cblxuLyoqXG4gKiBUaGUgaG9zdCBvZiB0aGUgU3ltYm9sUmVzb2x2ZXJIb3N0IGRpc2Nvbm5lY3RzIHRoZSBpbXBsZW1lbnRhdGlvbiBmcm9tIFR5cGVTY3JpcHQgLyBvdGhlclxuICogbGFuZ3VhZ2VcbiAqIHNlcnZpY2VzIGFuZCBmcm9tIHVuZGVybHlpbmcgZmlsZSBzeXN0ZW1zLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFN0YXRpY1N5bWJvbFJlc29sdmVySG9zdCB7XG4gIC8qKlxuICAgKiBSZXR1cm4gYSBNb2R1bGVNZXRhZGF0YSBmb3IgdGhlIGdpdmVuIG1vZHVsZS5cbiAgICogQW5ndWxhciBDTEkgd2lsbCBwcm9kdWNlIHRoaXMgbWV0YWRhdGEgZm9yIGEgbW9kdWxlIHdoZW5ldmVyIGEgLmQudHMgZmlsZXMgaXNcbiAgICogcHJvZHVjZWQgYW5kIHRoZSBtb2R1bGUgaGFzIGV4cG9ydGVkIHZhcmlhYmxlcyBvciBjbGFzc2VzIHdpdGggZGVjb3JhdG9ycy4gTW9kdWxlIG1ldGFkYXRhIGNhblxuICAgKiBhbHNvIGJlIHByb2R1Y2VkIGRpcmVjdGx5IGZyb20gVHlwZVNjcmlwdCBzb3VyY2VzIGJ5IHVzaW5nIE1ldGFkYXRhQ29sbGVjdG9yIGluIHRvb2xzL21ldGFkYXRhLlxuICAgKlxuICAgKiBAcGFyYW0gbW9kdWxlUGF0aCBpcyBhIHN0cmluZyBpZGVudGlmaWVyIGZvciBhIG1vZHVsZSBhcyBhbiBhYnNvbHV0ZSBwYXRoLlxuICAgKiBAcmV0dXJucyB0aGUgbWV0YWRhdGEgZm9yIHRoZSBnaXZlbiBtb2R1bGUuXG4gICAqL1xuICBnZXRNZXRhZGF0YUZvcihtb2R1bGVQYXRoOiBzdHJpbmcpOiB7W2tleTogc3RyaW5nXTogYW55fVtdfHVuZGVmaW5lZDtcblxuICAvKipcbiAgICogQ29udmVydHMgYSBtb2R1bGUgbmFtZSB0aGF0IGlzIHVzZWQgaW4gYW4gYGltcG9ydGAgdG8gYSBmaWxlIHBhdGguXG4gICAqIEkuZS5cbiAgICogYHBhdGgvdG8vY29udGFpbmluZ0ZpbGUudHNgIGNvbnRhaW5pbmcgYGltcG9ydCB7Li4ufSBmcm9tICdtb2R1bGUtbmFtZSdgLlxuICAgKi9cbiAgbW9kdWxlTmFtZVRvRmlsZU5hbWUobW9kdWxlTmFtZTogc3RyaW5nLCBjb250YWluaW5nRmlsZT86IHN0cmluZyk6IHN0cmluZ3xudWxsO1xuXG4gIC8qKlxuICAgKiBHZXQgYSBmaWxlIHN1aXRhYmxlIGZvciBkaXNwbGF5IHRvIHRoZSB1c2VyIHRoYXQgc2hvdWxkIGJlIHJlbGF0aXZlIHRvIHRoZSBwcm9qZWN0IGRpcmVjdG9yeVxuICAgKiBvciB0aGUgY3VycmVudCBkaXJlY3RvcnkuXG4gICAqL1xuICBnZXRPdXRwdXROYW1lKGZpbGVQYXRoOiBzdHJpbmcpOiBzdHJpbmc7XG59XG5cbmNvbnN0IFNVUFBPUlRFRF9TQ0hFTUFfVkVSU0lPTiA9IDQ7XG5cbi8qKlxuICogVGhpcyBjbGFzcyBpcyByZXNwb25zaWJsZSBmb3IgbG9hZGluZyBtZXRhZGF0YSBwZXIgc3ltYm9sLFxuICogYW5kIG5vcm1hbGl6aW5nIHJlZmVyZW5jZXMgYmV0d2VlbiBzeW1ib2xzLlxuICpcbiAqIEludGVybmFsbHksIGl0IG9ubHkgdXNlcyBzeW1ib2xzIHdpdGhvdXQgbWVtYmVycyxcbiAqIGFuZCBkZWR1Y2VzIHRoZSB2YWx1ZXMgZm9yIHN5bWJvbHMgd2l0aCBtZW1iZXJzIGJhc2VkXG4gKiBvbiB0aGVzZSBzeW1ib2xzLlxuICovXG5leHBvcnQgY2xhc3MgU3RhdGljU3ltYm9sUmVzb2x2ZXIge1xuICBwcml2YXRlIG1ldGFkYXRhQ2FjaGUgPSBuZXcgTWFwPHN0cmluZywge1trZXk6IHN0cmluZ106IGFueX0+KCk7XG4gIC8vIE5vdGU6IHRoaXMgd2lsbCBvbmx5IGNvbnRhaW4gU3RhdGljU3ltYm9scyB3aXRob3V0IG1lbWJlcnMhXG4gIHByaXZhdGUgcmVzb2x2ZWRTeW1ib2xzID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIFJlc29sdmVkU3RhdGljU3ltYm9sPigpO1xuICBwcml2YXRlIHJlc29sdmVkRmlsZVBhdGhzID0gbmV3IFNldDxzdHJpbmc+KCk7XG4gIC8vIE5vdGU6IHRoaXMgd2lsbCBvbmx5IGNvbnRhaW4gU3RhdGljU3ltYm9scyB3aXRob3V0IG1lbWJlcnMhXG4gIHByaXZhdGUgaW1wb3J0QXMgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgU3RhdGljU3ltYm9sPigpO1xuICBwcml2YXRlIHN5bWJvbFJlc291cmNlUGF0aHMgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgc3RyaW5nPigpO1xuICBwcml2YXRlIHN5bWJvbEZyb21GaWxlID0gbmV3IE1hcDxzdHJpbmcsIFN0YXRpY1N5bWJvbFtdPigpO1xuICBwcml2YXRlIGtub3duRmlsZU5hbWVUb01vZHVsZU5hbWVzID0gbmV3IE1hcDxzdHJpbmcsIHN0cmluZz4oKTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgaG9zdDogU3RhdGljU3ltYm9sUmVzb2x2ZXJIb3N0LCBwcml2YXRlIHN0YXRpY1N5bWJvbENhY2hlOiBTdGF0aWNTeW1ib2xDYWNoZSxcbiAgICAgIHByaXZhdGUgc3VtbWFyeVJlc29sdmVyOiBTdW1tYXJ5UmVzb2x2ZXI8U3RhdGljU3ltYm9sPixcbiAgICAgIHByaXZhdGUgZXJyb3JSZWNvcmRlcj86IChlcnJvcjogYW55LCBmaWxlTmFtZT86IHN0cmluZykgPT4gdm9pZCkge31cblxuICByZXNvbHZlU3ltYm9sKHN0YXRpY1N5bWJvbDogU3RhdGljU3ltYm9sKTogUmVzb2x2ZWRTdGF0aWNTeW1ib2wge1xuICAgIGlmIChzdGF0aWNTeW1ib2wubWVtYmVycy5sZW5ndGggPiAwKSB7XG4gICAgICByZXR1cm4gdGhpcy5fcmVzb2x2ZVN5bWJvbE1lbWJlcnMoc3RhdGljU3ltYm9sKSAhO1xuICAgIH1cbiAgICAvLyBOb3RlOiBhbHdheXMgYXNrIGZvciBhIHN1bW1hcnkgZmlyc3QsXG4gICAgLy8gYXMgd2UgbWlnaHQgaGF2ZSByZWFkIHNoYWxsb3cgbWV0YWRhdGEgdmlhIGEgLmQudHMgZmlsZVxuICAgIC8vIGZvciB0aGUgc3ltYm9sLlxuICAgIGNvbnN0IHJlc3VsdEZyb21TdW1tYXJ5ID0gdGhpcy5fcmVzb2x2ZVN5bWJvbEZyb21TdW1tYXJ5KHN0YXRpY1N5bWJvbCkgITtcbiAgICBpZiAocmVzdWx0RnJvbVN1bW1hcnkpIHtcbiAgICAgIHJldHVybiByZXN1bHRGcm9tU3VtbWFyeTtcbiAgICB9XG4gICAgY29uc3QgcmVzdWx0RnJvbUNhY2hlID0gdGhpcy5yZXNvbHZlZFN5bWJvbHMuZ2V0KHN0YXRpY1N5bWJvbCk7XG4gICAgaWYgKHJlc3VsdEZyb21DYWNoZSkge1xuICAgICAgcmV0dXJuIHJlc3VsdEZyb21DYWNoZTtcbiAgICB9XG4gICAgLy8gTm90ZTogU29tZSB1c2VycyB1c2UgbGlicmFyaWVzIHRoYXQgd2VyZSBub3QgY29tcGlsZWQgd2l0aCBuZ2MsIGkuZS4gdGhleSBkb24ndFxuICAgIC8vIGhhdmUgc3VtbWFyaWVzLCBvbmx5IC5kLnRzIGZpbGVzLiBTbyB3ZSBhbHdheXMgbmVlZCB0byBjaGVjayBib3RoLCB0aGUgc3VtbWFyeVxuICAgIC8vIGFuZCBtZXRhZGF0YS5cbiAgICB0aGlzLl9jcmVhdGVTeW1ib2xzT2Yoc3RhdGljU3ltYm9sLmZpbGVQYXRoKTtcbiAgICByZXR1cm4gdGhpcy5yZXNvbHZlZFN5bWJvbHMuZ2V0KHN0YXRpY1N5bWJvbCkgITtcbiAgfVxuXG4gIC8qKlxuICAgKiBnZXRJbXBvcnRBcyBwcm9kdWNlcyBhIHN5bWJvbCB0aGF0IGNhbiBiZSB1c2VkIHRvIGltcG9ydCB0aGUgZ2l2ZW4gc3ltYm9sLlxuICAgKiBUaGUgaW1wb3J0IG1pZ2h0IGJlIGRpZmZlcmVudCB0aGFuIHRoZSBzeW1ib2wgaWYgdGhlIHN5bWJvbCBpcyBleHBvcnRlZCBmcm9tXG4gICAqIGEgbGlicmFyeSB3aXRoIGEgc3VtbWFyeTsgaW4gd2hpY2ggY2FzZSB3ZSB3YW50IHRvIGltcG9ydCB0aGUgc3ltYm9sIGZyb20gdGhlXG4gICAqIG5nZmFjdG9yeSByZS1leHBvcnQgaW5zdGVhZCBvZiBkaXJlY3RseSB0byBhdm9pZCBpbnRyb2R1Y2luZyBhIGRpcmVjdCBkZXBlbmRlbmN5XG4gICAqIG9uIGFuIG90aGVyd2lzZSBpbmRpcmVjdCBkZXBlbmRlbmN5LlxuICAgKlxuICAgKiBAcGFyYW0gc3RhdGljU3ltYm9sIHRoZSBzeW1ib2wgZm9yIHdoaWNoIHRvIGdlbmVyYXRlIGEgaW1wb3J0IHN5bWJvbFxuICAgKi9cbiAgZ2V0SW1wb3J0QXMoc3RhdGljU3ltYm9sOiBTdGF0aWNTeW1ib2wsIHVzZVN1bW1hcmllczogYm9vbGVhbiA9IHRydWUpOiBTdGF0aWNTeW1ib2x8bnVsbCB7XG4gICAgaWYgKHN0YXRpY1N5bWJvbC5tZW1iZXJzLmxlbmd0aCkge1xuICAgICAgY29uc3QgYmFzZVN5bWJvbCA9IHRoaXMuZ2V0U3RhdGljU3ltYm9sKHN0YXRpY1N5bWJvbC5maWxlUGF0aCwgc3RhdGljU3ltYm9sLm5hbWUpO1xuICAgICAgY29uc3QgYmFzZUltcG9ydEFzID0gdGhpcy5nZXRJbXBvcnRBcyhiYXNlU3ltYm9sLCB1c2VTdW1tYXJpZXMpO1xuICAgICAgcmV0dXJuIGJhc2VJbXBvcnRBcyA/XG4gICAgICAgICAgdGhpcy5nZXRTdGF0aWNTeW1ib2woYmFzZUltcG9ydEFzLmZpbGVQYXRoLCBiYXNlSW1wb3J0QXMubmFtZSwgc3RhdGljU3ltYm9sLm1lbWJlcnMpIDpcbiAgICAgICAgICBudWxsO1xuICAgIH1cbiAgICBjb25zdCBzdW1tYXJpemVkRmlsZU5hbWUgPSBzdHJpcFN1bW1hcnlGb3JKaXRGaWxlU3VmZml4KHN0YXRpY1N5bWJvbC5maWxlUGF0aCk7XG4gICAgaWYgKHN1bW1hcml6ZWRGaWxlTmFtZSAhPT0gc3RhdGljU3ltYm9sLmZpbGVQYXRoKSB7XG4gICAgICBjb25zdCBzdW1tYXJpemVkTmFtZSA9IHN0cmlwU3VtbWFyeUZvckppdE5hbWVTdWZmaXgoc3RhdGljU3ltYm9sLm5hbWUpO1xuICAgICAgY29uc3QgYmFzZVN5bWJvbCA9XG4gICAgICAgICAgdGhpcy5nZXRTdGF0aWNTeW1ib2woc3VtbWFyaXplZEZpbGVOYW1lLCBzdW1tYXJpemVkTmFtZSwgc3RhdGljU3ltYm9sLm1lbWJlcnMpO1xuICAgICAgY29uc3QgYmFzZUltcG9ydEFzID0gdGhpcy5nZXRJbXBvcnRBcyhiYXNlU3ltYm9sLCB1c2VTdW1tYXJpZXMpO1xuICAgICAgcmV0dXJuIGJhc2VJbXBvcnRBcyA/XG4gICAgICAgICAgdGhpcy5nZXRTdGF0aWNTeW1ib2woXG4gICAgICAgICAgICAgIHN1bW1hcnlGb3JKaXRGaWxlTmFtZShiYXNlSW1wb3J0QXMuZmlsZVBhdGgpLCBzdW1tYXJ5Rm9ySml0TmFtZShiYXNlSW1wb3J0QXMubmFtZSksXG4gICAgICAgICAgICAgIGJhc2VTeW1ib2wubWVtYmVycykgOlxuICAgICAgICAgIG51bGw7XG4gICAgfVxuICAgIGxldCByZXN1bHQgPSAodXNlU3VtbWFyaWVzICYmIHRoaXMuc3VtbWFyeVJlc29sdmVyLmdldEltcG9ydEFzKHN0YXRpY1N5bWJvbCkpIHx8IG51bGw7XG4gICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgIHJlc3VsdCA9IHRoaXMuaW1wb3J0QXMuZ2V0KHN0YXRpY1N5bWJvbCkgITtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIC8qKlxuICAgKiBnZXRSZXNvdXJjZVBhdGggcHJvZHVjZXMgdGhlIHBhdGggdG8gdGhlIG9yaWdpbmFsIGxvY2F0aW9uIG9mIHRoZSBzeW1ib2wgYW5kIHNob3VsZFxuICAgKiBiZSB1c2VkIHRvIGRldGVybWluZSB0aGUgcmVsYXRpdmUgbG9jYXRpb24gb2YgcmVzb3VyY2UgcmVmZXJlbmNlcyByZWNvcmRlZCBpblxuICAgKiBzeW1ib2wgbWV0YWRhdGEuXG4gICAqL1xuICBnZXRSZXNvdXJjZVBhdGgoc3RhdGljU3ltYm9sOiBTdGF0aWNTeW1ib2wpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLnN5bWJvbFJlc291cmNlUGF0aHMuZ2V0KHN0YXRpY1N5bWJvbCkgfHwgc3RhdGljU3ltYm9sLmZpbGVQYXRoO1xuICB9XG5cbiAgLyoqXG4gICAqIGdldFR5cGVBcml0eSByZXR1cm5zIHRoZSBudW1iZXIgb2YgZ2VuZXJpYyB0eXBlIHBhcmFtZXRlcnMgdGhlIGdpdmVuIHN5bWJvbFxuICAgKiBoYXMuIElmIHRoZSBzeW1ib2wgaXMgbm90IGEgdHlwZSB0aGUgcmVzdWx0IGlzIG51bGwuXG4gICAqL1xuICBnZXRUeXBlQXJpdHkoc3RhdGljU3ltYm9sOiBTdGF0aWNTeW1ib2wpOiBudW1iZXJ8bnVsbCB7XG4gICAgLy8gSWYgdGhlIGZpbGUgaXMgYSBmYWN0b3J5L25nc3VtbWFyeSBmaWxlLCBkb24ndCByZXNvbHZlIHRoZSBzeW1ib2wgYXMgZG9pbmcgc28gd291bGRcbiAgICAvLyBjYXVzZSB0aGUgbWV0YWRhdGEgZm9yIGFuIGZhY3RvcnkvbmdzdW1tYXJ5IGZpbGUgdG8gYmUgbG9hZGVkIHdoaWNoIGRvZXNuJ3QgZXhpc3QuXG4gICAgLy8gQWxsIHJlZmVyZW5jZXMgdG8gZ2VuZXJhdGVkIGNsYXNzZXMgbXVzdCBpbmNsdWRlIHRoZSBjb3JyZWN0IGFyaXR5IHdoZW5ldmVyXG4gICAgLy8gZ2VuZXJhdGluZyBjb2RlLlxuICAgIGlmIChpc0dlbmVyYXRlZEZpbGUoc3RhdGljU3ltYm9sLmZpbGVQYXRoKSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGxldCByZXNvbHZlZFN5bWJvbCA9IHVud3JhcFJlc29sdmVkTWV0YWRhdGEodGhpcy5yZXNvbHZlU3ltYm9sKHN0YXRpY1N5bWJvbCkpO1xuICAgIHdoaWxlIChyZXNvbHZlZFN5bWJvbCAmJiByZXNvbHZlZFN5bWJvbC5tZXRhZGF0YSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkge1xuICAgICAgcmVzb2x2ZWRTeW1ib2wgPSB1bndyYXBSZXNvbHZlZE1ldGFkYXRhKHRoaXMucmVzb2x2ZVN5bWJvbChyZXNvbHZlZFN5bWJvbC5tZXRhZGF0YSkpO1xuICAgIH1cbiAgICByZXR1cm4gKHJlc29sdmVkU3ltYm9sICYmIHJlc29sdmVkU3ltYm9sLm1ldGFkYXRhICYmIHJlc29sdmVkU3ltYm9sLm1ldGFkYXRhLmFyaXR5KSB8fCBudWxsO1xuICB9XG5cbiAgZ2V0S25vd25Nb2R1bGVOYW1lKGZpbGVQYXRoOiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgcmV0dXJuIHRoaXMua25vd25GaWxlTmFtZVRvTW9kdWxlTmFtZXMuZ2V0KGZpbGVQYXRoKSB8fCBudWxsO1xuICB9XG5cbiAgcmVjb3JkSW1wb3J0QXMoc291cmNlU3ltYm9sOiBTdGF0aWNTeW1ib2wsIHRhcmdldFN5bWJvbDogU3RhdGljU3ltYm9sKSB7XG4gICAgc291cmNlU3ltYm9sLmFzc2VydE5vTWVtYmVycygpO1xuICAgIHRhcmdldFN5bWJvbC5hc3NlcnROb01lbWJlcnMoKTtcbiAgICB0aGlzLmltcG9ydEFzLnNldChzb3VyY2VTeW1ib2wsIHRhcmdldFN5bWJvbCk7XG4gIH1cblxuICByZWNvcmRNb2R1bGVOYW1lRm9yRmlsZU5hbWUoZmlsZU5hbWU6IHN0cmluZywgbW9kdWxlTmFtZTogc3RyaW5nKSB7XG4gICAgdGhpcy5rbm93bkZpbGVOYW1lVG9Nb2R1bGVOYW1lcy5zZXQoZmlsZU5hbWUsIG1vZHVsZU5hbWUpO1xuICB9XG5cbiAgLyoqXG4gICAqIEludmFsaWRhdGUgYWxsIGluZm9ybWF0aW9uIGRlcml2ZWQgZnJvbSB0aGUgZ2l2ZW4gZmlsZS5cbiAgICpcbiAgICogQHBhcmFtIGZpbGVOYW1lIHRoZSBmaWxlIHRvIGludmFsaWRhdGVcbiAgICovXG4gIGludmFsaWRhdGVGaWxlKGZpbGVOYW1lOiBzdHJpbmcpIHtcbiAgICB0aGlzLm1ldGFkYXRhQ2FjaGUuZGVsZXRlKGZpbGVOYW1lKTtcbiAgICB0aGlzLnJlc29sdmVkRmlsZVBhdGhzLmRlbGV0ZShmaWxlTmFtZSk7XG4gICAgY29uc3Qgc3ltYm9scyA9IHRoaXMuc3ltYm9sRnJvbUZpbGUuZ2V0KGZpbGVOYW1lKTtcbiAgICBpZiAoc3ltYm9scykge1xuICAgICAgdGhpcy5zeW1ib2xGcm9tRmlsZS5kZWxldGUoZmlsZU5hbWUpO1xuICAgICAgZm9yIChjb25zdCBzeW1ib2wgb2Ygc3ltYm9scykge1xuICAgICAgICB0aGlzLnJlc29sdmVkU3ltYm9scy5kZWxldGUoc3ltYm9sKTtcbiAgICAgICAgdGhpcy5pbXBvcnRBcy5kZWxldGUoc3ltYm9sKTtcbiAgICAgICAgdGhpcy5zeW1ib2xSZXNvdXJjZVBhdGhzLmRlbGV0ZShzeW1ib2wpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgaWdub3JlRXJyb3JzRm9yPFQ+KGNiOiAoKSA9PiBUKSB7XG4gICAgY29uc3QgcmVjb3JkZXIgPSB0aGlzLmVycm9yUmVjb3JkZXI7XG4gICAgdGhpcy5lcnJvclJlY29yZGVyID0gKCkgPT4ge307XG4gICAgdHJ5IHtcbiAgICAgIHJldHVybiBjYigpO1xuICAgIH0gZmluYWxseSB7XG4gICAgICB0aGlzLmVycm9yUmVjb3JkZXIgPSByZWNvcmRlcjtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9yZXNvbHZlU3ltYm9sTWVtYmVycyhzdGF0aWNTeW1ib2w6IFN0YXRpY1N5bWJvbCk6IFJlc29sdmVkU3RhdGljU3ltYm9sfG51bGwge1xuICAgIGNvbnN0IG1lbWJlcnMgPSBzdGF0aWNTeW1ib2wubWVtYmVycztcbiAgICBjb25zdCBiYXNlUmVzb2x2ZWRTeW1ib2wgPVxuICAgICAgICB0aGlzLnJlc29sdmVTeW1ib2wodGhpcy5nZXRTdGF0aWNTeW1ib2woc3RhdGljU3ltYm9sLmZpbGVQYXRoLCBzdGF0aWNTeW1ib2wubmFtZSkpO1xuICAgIGlmICghYmFzZVJlc29sdmVkU3ltYm9sKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgbGV0IGJhc2VNZXRhZGF0YSA9IHVud3JhcFJlc29sdmVkTWV0YWRhdGEoYmFzZVJlc29sdmVkU3ltYm9sLm1ldGFkYXRhKTtcbiAgICBpZiAoYmFzZU1ldGFkYXRhIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgICByZXR1cm4gbmV3IFJlc29sdmVkU3RhdGljU3ltYm9sKFxuICAgICAgICAgIHN0YXRpY1N5bWJvbCwgdGhpcy5nZXRTdGF0aWNTeW1ib2woYmFzZU1ldGFkYXRhLmZpbGVQYXRoLCBiYXNlTWV0YWRhdGEubmFtZSwgbWVtYmVycykpO1xuICAgIH0gZWxzZSBpZiAoYmFzZU1ldGFkYXRhICYmIGJhc2VNZXRhZGF0YS5fX3N5bWJvbGljID09PSAnY2xhc3MnKSB7XG4gICAgICBpZiAoYmFzZU1ldGFkYXRhLnN0YXRpY3MgJiYgbWVtYmVycy5sZW5ndGggPT09IDEpIHtcbiAgICAgICAgcmV0dXJuIG5ldyBSZXNvbHZlZFN0YXRpY1N5bWJvbChzdGF0aWNTeW1ib2wsIGJhc2VNZXRhZGF0YS5zdGF0aWNzW21lbWJlcnNbMF1dKTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgbGV0IHZhbHVlID0gYmFzZU1ldGFkYXRhO1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBtZW1iZXJzLmxlbmd0aCAmJiB2YWx1ZTsgaSsrKSB7XG4gICAgICAgIHZhbHVlID0gdmFsdWVbbWVtYmVyc1tpXV07XG4gICAgICB9XG4gICAgICByZXR1cm4gbmV3IFJlc29sdmVkU3RhdGljU3ltYm9sKHN0YXRpY1N5bWJvbCwgdmFsdWUpO1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHByaXZhdGUgX3Jlc29sdmVTeW1ib2xGcm9tU3VtbWFyeShzdGF0aWNTeW1ib2w6IFN0YXRpY1N5bWJvbCk6IFJlc29sdmVkU3RhdGljU3ltYm9sfG51bGwge1xuICAgIGNvbnN0IHN1bW1hcnkgPSB0aGlzLnN1bW1hcnlSZXNvbHZlci5yZXNvbHZlU3VtbWFyeShzdGF0aWNTeW1ib2wpO1xuICAgIHJldHVybiBzdW1tYXJ5ID8gbmV3IFJlc29sdmVkU3RhdGljU3ltYm9sKHN0YXRpY1N5bWJvbCwgc3VtbWFyeS5tZXRhZGF0YSkgOiBudWxsO1xuICB9XG5cbiAgLyoqXG4gICAqIGdldFN0YXRpY1N5bWJvbCBwcm9kdWNlcyBhIFR5cGUgd2hvc2UgbWV0YWRhdGEgaXMga25vd24gYnV0IHdob3NlIGltcGxlbWVudGF0aW9uIGlzIG5vdCBsb2FkZWQuXG4gICAqIEFsbCB0eXBlcyBwYXNzZWQgdG8gdGhlIFN0YXRpY1Jlc29sdmVyIHNob3VsZCBiZSBwc2V1ZG8tdHlwZXMgcmV0dXJuZWQgYnkgdGhpcyBtZXRob2QuXG4gICAqXG4gICAqIEBwYXJhbSBkZWNsYXJhdGlvbkZpbGUgdGhlIGFic29sdXRlIHBhdGggb2YgdGhlIGZpbGUgd2hlcmUgdGhlIHN5bWJvbCBpcyBkZWNsYXJlZFxuICAgKiBAcGFyYW0gbmFtZSB0aGUgbmFtZSBvZiB0aGUgdHlwZS5cbiAgICogQHBhcmFtIG1lbWJlcnMgYSBzeW1ib2wgZm9yIGEgc3RhdGljIG1lbWJlciBvZiB0aGUgbmFtZWQgdHlwZVxuICAgKi9cbiAgZ2V0U3RhdGljU3ltYm9sKGRlY2xhcmF0aW9uRmlsZTogc3RyaW5nLCBuYW1lOiBzdHJpbmcsIG1lbWJlcnM/OiBzdHJpbmdbXSk6IFN0YXRpY1N5bWJvbCB7XG4gICAgcmV0dXJuIHRoaXMuc3RhdGljU3ltYm9sQ2FjaGUuZ2V0KGRlY2xhcmF0aW9uRmlsZSwgbmFtZSwgbWVtYmVycyk7XG4gIH1cblxuICAvKipcbiAgICogaGFzRGVjb3JhdG9ycyBjaGVja3MgYSBmaWxlJ3MgbWV0YWRhdGEgZm9yIHRoZSBwcmVzZW5jZSBvZiBkZWNvcmF0b3JzIHdpdGhvdXQgZXZhbHVhdGluZyB0aGVcbiAgICogbWV0YWRhdGEuXG4gICAqXG4gICAqIEBwYXJhbSBmaWxlUGF0aCB0aGUgYWJzb2x1dGUgcGF0aCB0byBleGFtaW5lIGZvciBkZWNvcmF0b3JzLlxuICAgKiBAcmV0dXJucyB0cnVlIGlmIGFueSBjbGFzcyBpbiB0aGUgZmlsZSBoYXMgYSBkZWNvcmF0b3IuXG4gICAqL1xuICBoYXNEZWNvcmF0b3JzKGZpbGVQYXRoOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICBjb25zdCBtZXRhZGF0YSA9IHRoaXMuZ2V0TW9kdWxlTWV0YWRhdGEoZmlsZVBhdGgpO1xuICAgIGlmIChtZXRhZGF0YVsnbWV0YWRhdGEnXSkge1xuICAgICAgcmV0dXJuIE9iamVjdC5rZXlzKG1ldGFkYXRhWydtZXRhZGF0YSddKS5zb21lKChtZXRhZGF0YUtleSkgPT4ge1xuICAgICAgICBjb25zdCBlbnRyeSA9IG1ldGFkYXRhWydtZXRhZGF0YSddW21ldGFkYXRhS2V5XTtcbiAgICAgICAgcmV0dXJuIGVudHJ5ICYmIGVudHJ5Ll9fc3ltYm9saWMgPT09ICdjbGFzcycgJiYgZW50cnkuZGVjb3JhdG9ycztcbiAgICAgIH0pO1xuICAgIH1cbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBnZXRTeW1ib2xzT2YoZmlsZVBhdGg6IHN0cmluZyk6IFN0YXRpY1N5bWJvbFtdIHtcbiAgICBjb25zdCBzdW1tYXJ5U3ltYm9scyA9IHRoaXMuc3VtbWFyeVJlc29sdmVyLmdldFN5bWJvbHNPZihmaWxlUGF0aCk7XG4gICAgaWYgKHN1bW1hcnlTeW1ib2xzKSB7XG4gICAgICByZXR1cm4gc3VtbWFyeVN5bWJvbHM7XG4gICAgfVxuICAgIC8vIE5vdGU6IFNvbWUgdXNlcnMgdXNlIGxpYnJhcmllcyB0aGF0IHdlcmUgbm90IGNvbXBpbGVkIHdpdGggbmdjLCBpLmUuIHRoZXkgZG9uJ3RcbiAgICAvLyBoYXZlIHN1bW1hcmllcywgb25seSAuZC50cyBmaWxlcywgYnV0IGBzdW1tYXJ5UmVzb2x2ZXIuaXNMaWJyYXJ5RmlsZWAgcmV0dXJucyB0cnVlLlxuICAgIHRoaXMuX2NyZWF0ZVN5bWJvbHNPZihmaWxlUGF0aCk7XG4gICAgY29uc3QgbWV0YWRhdGFTeW1ib2xzOiBTdGF0aWNTeW1ib2xbXSA9IFtdO1xuICAgIHRoaXMucmVzb2x2ZWRTeW1ib2xzLmZvckVhY2goKHJlc29sdmVkU3ltYm9sKSA9PiB7XG4gICAgICBpZiAocmVzb2x2ZWRTeW1ib2wuc3ltYm9sLmZpbGVQYXRoID09PSBmaWxlUGF0aCkge1xuICAgICAgICBtZXRhZGF0YVN5bWJvbHMucHVzaChyZXNvbHZlZFN5bWJvbC5zeW1ib2wpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIHJldHVybiBtZXRhZGF0YVN5bWJvbHM7XG4gIH1cblxuICBwcml2YXRlIF9jcmVhdGVTeW1ib2xzT2YoZmlsZVBhdGg6IHN0cmluZykge1xuICAgIGlmICh0aGlzLnJlc29sdmVkRmlsZVBhdGhzLmhhcyhmaWxlUGF0aCkpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5yZXNvbHZlZEZpbGVQYXRocy5hZGQoZmlsZVBhdGgpO1xuICAgIGNvbnN0IHJlc29sdmVkU3ltYm9sczogUmVzb2x2ZWRTdGF0aWNTeW1ib2xbXSA9IFtdO1xuICAgIGNvbnN0IG1ldGFkYXRhID0gdGhpcy5nZXRNb2R1bGVNZXRhZGF0YShmaWxlUGF0aCk7XG4gICAgaWYgKG1ldGFkYXRhWydpbXBvcnRBcyddKSB7XG4gICAgICAvLyBJbmRleCBidW5kbGUgaW5kaWNlcyBzaG91bGQgdXNlIHRoZSBpbXBvcnRBcyBtb2R1bGUgbmFtZSBkZWZpbmVkXG4gICAgICAvLyBpbiB0aGUgYnVuZGxlLlxuICAgICAgdGhpcy5rbm93bkZpbGVOYW1lVG9Nb2R1bGVOYW1lcy5zZXQoZmlsZVBhdGgsIG1ldGFkYXRhWydpbXBvcnRBcyddKTtcbiAgICB9XG4gICAgLy8gaGFuZGxlIHRoZSBzeW1ib2xzIGluIG9uZSBvZiB0aGUgcmUtZXhwb3J0IGxvY2F0aW9uXG4gICAgaWYgKG1ldGFkYXRhWydleHBvcnRzJ10pIHtcbiAgICAgIGZvciAoY29uc3QgbW9kdWxlRXhwb3J0IG9mIG1ldGFkYXRhWydleHBvcnRzJ10pIHtcbiAgICAgICAgLy8gaGFuZGxlIHRoZSBzeW1ib2xzIGluIHRoZSBsaXN0IG9mIGV4cGxpY2l0bHkgcmUtZXhwb3J0ZWQgc3ltYm9scy5cbiAgICAgICAgaWYgKG1vZHVsZUV4cG9ydC5leHBvcnQpIHtcbiAgICAgICAgICBtb2R1bGVFeHBvcnQuZXhwb3J0LmZvckVhY2goKGV4cG9ydFN5bWJvbDogYW55KSA9PiB7XG4gICAgICAgICAgICBsZXQgc3ltYm9sTmFtZTogc3RyaW5nO1xuICAgICAgICAgICAgaWYgKHR5cGVvZiBleHBvcnRTeW1ib2wgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgICAgICAgIHN5bWJvbE5hbWUgPSBleHBvcnRTeW1ib2w7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICBzeW1ib2xOYW1lID0gZXhwb3J0U3ltYm9sLmFzO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgc3ltYm9sTmFtZSA9IHVuZXNjYXBlSWRlbnRpZmllcihzeW1ib2xOYW1lKTtcbiAgICAgICAgICAgIGxldCBzeW1OYW1lID0gc3ltYm9sTmFtZTtcbiAgICAgICAgICAgIGlmICh0eXBlb2YgZXhwb3J0U3ltYm9sICE9PSAnc3RyaW5nJykge1xuICAgICAgICAgICAgICBzeW1OYW1lID0gdW5lc2NhcGVJZGVudGlmaWVyKGV4cG9ydFN5bWJvbC5uYW1lKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGNvbnN0IHJlc29sdmVkTW9kdWxlID0gdGhpcy5yZXNvbHZlTW9kdWxlKG1vZHVsZUV4cG9ydC5mcm9tLCBmaWxlUGF0aCk7XG4gICAgICAgICAgICBpZiAocmVzb2x2ZWRNb2R1bGUpIHtcbiAgICAgICAgICAgICAgY29uc3QgdGFyZ2V0U3ltYm9sID0gdGhpcy5nZXRTdGF0aWNTeW1ib2wocmVzb2x2ZWRNb2R1bGUsIHN5bU5hbWUpO1xuICAgICAgICAgICAgICBjb25zdCBzb3VyY2VTeW1ib2wgPSB0aGlzLmdldFN0YXRpY1N5bWJvbChmaWxlUGF0aCwgc3ltYm9sTmFtZSk7XG4gICAgICAgICAgICAgIHJlc29sdmVkU3ltYm9scy5wdXNoKHRoaXMuY3JlYXRlRXhwb3J0KHNvdXJjZVN5bWJvbCwgdGFyZ2V0U3ltYm9sKSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgLy8gaGFuZGxlIHRoZSBzeW1ib2xzIHZpYSBleHBvcnQgKiBkaXJlY3RpdmVzLlxuICAgICAgICAgIGNvbnN0IHJlc29sdmVkTW9kdWxlID0gdGhpcy5yZXNvbHZlTW9kdWxlKG1vZHVsZUV4cG9ydC5mcm9tLCBmaWxlUGF0aCk7XG4gICAgICAgICAgaWYgKHJlc29sdmVkTW9kdWxlKSB7XG4gICAgICAgICAgICBjb25zdCBuZXN0ZWRFeHBvcnRzID0gdGhpcy5nZXRTeW1ib2xzT2YocmVzb2x2ZWRNb2R1bGUpO1xuICAgICAgICAgICAgbmVzdGVkRXhwb3J0cy5mb3JFYWNoKCh0YXJnZXRTeW1ib2wpID0+IHtcbiAgICAgICAgICAgICAgY29uc3Qgc291cmNlU3ltYm9sID0gdGhpcy5nZXRTdGF0aWNTeW1ib2woZmlsZVBhdGgsIHRhcmdldFN5bWJvbC5uYW1lKTtcbiAgICAgICAgICAgICAgcmVzb2x2ZWRTeW1ib2xzLnB1c2godGhpcy5jcmVhdGVFeHBvcnQoc291cmNlU3ltYm9sLCB0YXJnZXRTeW1ib2wpKTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIC8vIGhhbmRsZSB0aGUgYWN0dWFsIG1ldGFkYXRhLiBIYXMgdG8gYmUgYWZ0ZXIgdGhlIGV4cG9ydHNcbiAgICAvLyBhcyB0aGVyZSBtaWdodCBiZSBjb2xsaXNpb25zIGluIHRoZSBuYW1lcywgYW5kIHdlIHdhbnQgdGhlIHN5bWJvbHNcbiAgICAvLyBvZiB0aGUgY3VycmVudCBtb2R1bGUgdG8gd2luIG9mdGVyIHJlZXhwb3J0cy5cbiAgICBpZiAobWV0YWRhdGFbJ21ldGFkYXRhJ10pIHtcbiAgICAgIC8vIGhhbmRsZSBkaXJlY3QgZGVjbGFyYXRpb25zIG9mIHRoZSBzeW1ib2xcbiAgICAgIGNvbnN0IHRvcExldmVsU3ltYm9sTmFtZXMgPVxuICAgICAgICAgIG5ldyBTZXQ8c3RyaW5nPihPYmplY3Qua2V5cyhtZXRhZGF0YVsnbWV0YWRhdGEnXSkubWFwKHVuZXNjYXBlSWRlbnRpZmllcikpO1xuICAgICAgY29uc3Qgb3JpZ2luczoge1tpbmRleDogc3RyaW5nXTogc3RyaW5nfSA9IG1ldGFkYXRhWydvcmlnaW5zJ10gfHwge307XG4gICAgICBPYmplY3Qua2V5cyhtZXRhZGF0YVsnbWV0YWRhdGEnXSkuZm9yRWFjaCgobWV0YWRhdGFLZXkpID0+IHtcbiAgICAgICAgY29uc3Qgc3ltYm9sTWV0YSA9IG1ldGFkYXRhWydtZXRhZGF0YSddW21ldGFkYXRhS2V5XTtcbiAgICAgICAgY29uc3QgbmFtZSA9IHVuZXNjYXBlSWRlbnRpZmllcihtZXRhZGF0YUtleSk7XG5cbiAgICAgICAgY29uc3Qgc3ltYm9sID0gdGhpcy5nZXRTdGF0aWNTeW1ib2woZmlsZVBhdGgsIG5hbWUpO1xuXG4gICAgICAgIGNvbnN0IG9yaWdpbiA9IG9yaWdpbnMuaGFzT3duUHJvcGVydHkobWV0YWRhdGFLZXkpICYmIG9yaWdpbnNbbWV0YWRhdGFLZXldO1xuICAgICAgICBpZiAob3JpZ2luKSB7XG4gICAgICAgICAgLy8gSWYgdGhlIHN5bWJvbCBpcyBmcm9tIGEgYnVuZGxlZCBpbmRleCwgdXNlIHRoZSBkZWNsYXJhdGlvbiBsb2NhdGlvbiBvZiB0aGVcbiAgICAgICAgICAvLyBzeW1ib2wgc28gcmVsYXRpdmUgcmVmZXJlbmNlcyAoc3VjaCBhcyAnLi9teS5odG1sJykgd2lsbCBiZSBjYWxjdWxhdGVkXG4gICAgICAgICAgLy8gY29ycmVjdGx5LlxuICAgICAgICAgIGNvbnN0IG9yaWdpbkZpbGVQYXRoID0gdGhpcy5yZXNvbHZlTW9kdWxlKG9yaWdpbiwgZmlsZVBhdGgpO1xuICAgICAgICAgIGlmICghb3JpZ2luRmlsZVBhdGgpIHtcbiAgICAgICAgICAgIHRoaXMucmVwb3J0RXJyb3IobmV3IEVycm9yKFxuICAgICAgICAgICAgICAgIGBDb3VsZG4ndCByZXNvbHZlIG9yaWdpbmFsIHN5bWJvbCBmb3IgJHtvcmlnaW59IGZyb20gJHt0aGlzLmhvc3QuZ2V0T3V0cHV0TmFtZShmaWxlUGF0aCl9YCkpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICB0aGlzLnN5bWJvbFJlc291cmNlUGF0aHMuc2V0KHN5bWJvbCwgb3JpZ2luRmlsZVBhdGgpO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICByZXNvbHZlZFN5bWJvbHMucHVzaChcbiAgICAgICAgICAgIHRoaXMuY3JlYXRlUmVzb2x2ZWRTeW1ib2woc3ltYm9sLCBmaWxlUGF0aCwgdG9wTGV2ZWxTeW1ib2xOYW1lcywgc3ltYm9sTWV0YSkpO1xuICAgICAgfSk7XG4gICAgfVxuICAgIHJlc29sdmVkU3ltYm9scy5mb3JFYWNoKFxuICAgICAgICAocmVzb2x2ZWRTeW1ib2wpID0+IHRoaXMucmVzb2x2ZWRTeW1ib2xzLnNldChyZXNvbHZlZFN5bWJvbC5zeW1ib2wsIHJlc29sdmVkU3ltYm9sKSk7XG4gICAgdGhpcy5zeW1ib2xGcm9tRmlsZS5zZXQoZmlsZVBhdGgsIHJlc29sdmVkU3ltYm9scy5tYXAocmVzb2x2ZWRTeW1ib2wgPT4gcmVzb2x2ZWRTeW1ib2wuc3ltYm9sKSk7XG4gIH1cblxuICBwcml2YXRlIGNyZWF0ZVJlc29sdmVkU3ltYm9sKFxuICAgICAgc291cmNlU3ltYm9sOiBTdGF0aWNTeW1ib2wsIHRvcExldmVsUGF0aDogc3RyaW5nLCB0b3BMZXZlbFN5bWJvbE5hbWVzOiBTZXQ8c3RyaW5nPixcbiAgICAgIG1ldGFkYXRhOiBhbnkpOiBSZXNvbHZlZFN0YXRpY1N5bWJvbCB7XG4gICAgLy8gRm9yIGNsYXNzZXMgdGhhdCBkb24ndCBoYXZlIEFuZ3VsYXIgc3VtbWFyaWVzIC8gbWV0YWRhdGEsXG4gICAgLy8gd2Ugb25seSBrZWVwIHRoZWlyIGFyaXR5LCBidXQgbm90aGluZyBlbHNlXG4gICAgLy8gKGUuZy4gdGhlaXIgY29uc3RydWN0b3IgcGFyYW1ldGVycykuXG4gICAgLy8gV2UgZG8gdGhpcyB0byBwcmV2ZW50IGludHJvZHVjaW5nIGRlZXAgaW1wb3J0c1xuICAgIC8vIGFzIHdlIGRpZG4ndCBnZW5lcmF0ZSAubmdmYWN0b3J5LnRzIGZpbGVzIHdpdGggcHJvcGVyIHJlZXhwb3J0cy5cbiAgICBjb25zdCBpc1RzRmlsZSA9IFRTLnRlc3Qoc291cmNlU3ltYm9sLmZpbGVQYXRoKTtcbiAgICBpZiAodGhpcy5zdW1tYXJ5UmVzb2x2ZXIuaXNMaWJyYXJ5RmlsZShzb3VyY2VTeW1ib2wuZmlsZVBhdGgpICYmICFpc1RzRmlsZSAmJiBtZXRhZGF0YSAmJlxuICAgICAgICBtZXRhZGF0YVsnX19zeW1ib2xpYyddID09PSAnY2xhc3MnKSB7XG4gICAgICBjb25zdCB0cmFuc2Zvcm1lZE1ldGEgPSB7X19zeW1ib2xpYzogJ2NsYXNzJywgYXJpdHk6IG1ldGFkYXRhLmFyaXR5fTtcbiAgICAgIHJldHVybiBuZXcgUmVzb2x2ZWRTdGF0aWNTeW1ib2woc291cmNlU3ltYm9sLCB0cmFuc2Zvcm1lZE1ldGEpO1xuICAgIH1cblxuICAgIGxldCBfb3JpZ2luYWxGaWxlTWVtbzogc3RyaW5nfHVuZGVmaW5lZDtcbiAgICBjb25zdCBnZXRPcmlnaW5hbE5hbWU6ICgpID0+IHN0cmluZyA9ICgpID0+IHtcbiAgICAgIGlmICghX29yaWdpbmFsRmlsZU1lbW8pIHtcbiAgICAgICAgLy8gR3Vlc3Mgd2hhdCB0aGUgb3JpZ2luYWwgZmlsZSBuYW1lIGlzIGZyb20gdGhlIHJlZmVyZW5jZS4gSWYgaXQgaGFzIGEgYC5kLnRzYCBleHRlbnNpb25cbiAgICAgICAgLy8gcmVwbGFjZSBpdCB3aXRoIGAudHNgLiBJZiBpdCBhbHJlYWR5IGhhcyBgLnRzYCBqdXN0IGxlYXZlIGl0IGluIHBsYWNlLiBJZiBpdCBkb2Vzbid0IGhhdmVcbiAgICAgICAgLy8gLnRzIG9yIC5kLnRzLCBhcHBlbmQgYC50cycuIEFsc28sIGlmIGl0IGlzIGluIGBub2RlX21vZHVsZXNgLCB0cmltIHRoZSBgbm9kZV9tb2R1bGVgXG4gICAgICAgIC8vIGxvY2F0aW9uIGFzIGl0IGlzIG5vdCBpbXBvcnRhbnQgdG8gZmluZGluZyB0aGUgZmlsZS5cbiAgICAgICAgX29yaWdpbmFsRmlsZU1lbW8gPVxuICAgICAgICAgICAgdGhpcy5ob3N0LmdldE91dHB1dE5hbWUodG9wTGV2ZWxQYXRoLnJlcGxhY2UoLygoXFwudHMpfChcXC5kXFwudHMpfCkkLywgJy50cycpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLnJlcGxhY2UoL14uKm5vZGVfbW9kdWxlc1svXFxcXF0vLCAnJykpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIF9vcmlnaW5hbEZpbGVNZW1vO1xuICAgIH07XG5cbiAgICBjb25zdCBzZWxmID0gdGhpcztcblxuICAgIGNsYXNzIFJlZmVyZW5jZVRyYW5zZm9ybWVyIGV4dGVuZHMgVmFsdWVUcmFuc2Zvcm1lciB7XG4gICAgICB2aXNpdFN0cmluZ01hcChtYXA6IHtba2V5OiBzdHJpbmddOiBhbnl9LCBmdW5jdGlvblBhcmFtczogc3RyaW5nW10pOiBhbnkge1xuICAgICAgICBjb25zdCBzeW1ib2xpYyA9IG1hcFsnX19zeW1ib2xpYyddO1xuICAgICAgICBpZiAoc3ltYm9saWMgPT09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgICBjb25zdCBvbGRMZW4gPSBmdW5jdGlvblBhcmFtcy5sZW5ndGg7XG4gICAgICAgICAgZnVuY3Rpb25QYXJhbXMucHVzaCguLi4obWFwWydwYXJhbWV0ZXJzJ10gfHwgW10pKTtcbiAgICAgICAgICBjb25zdCByZXN1bHQgPSBzdXBlci52aXNpdFN0cmluZ01hcChtYXAsIGZ1bmN0aW9uUGFyYW1zKTtcbiAgICAgICAgICBmdW5jdGlvblBhcmFtcy5sZW5ndGggPSBvbGRMZW47XG4gICAgICAgICAgcmV0dXJuIHJlc3VsdDtcbiAgICAgICAgfSBlbHNlIGlmIChzeW1ib2xpYyA9PT0gJ3JlZmVyZW5jZScpIHtcbiAgICAgICAgICBjb25zdCBtb2R1bGUgPSBtYXBbJ21vZHVsZSddO1xuICAgICAgICAgIGNvbnN0IG5hbWUgPSBtYXBbJ25hbWUnXSA/IHVuZXNjYXBlSWRlbnRpZmllcihtYXBbJ25hbWUnXSkgOiBtYXBbJ25hbWUnXTtcbiAgICAgICAgICBpZiAoIW5hbWUpIHtcbiAgICAgICAgICAgIHJldHVybiBudWxsO1xuICAgICAgICAgIH1cbiAgICAgICAgICBsZXQgZmlsZVBhdGg6IHN0cmluZztcbiAgICAgICAgICBpZiAobW9kdWxlKSB7XG4gICAgICAgICAgICBmaWxlUGF0aCA9IHNlbGYucmVzb2x2ZU1vZHVsZShtb2R1bGUsIHNvdXJjZVN5bWJvbC5maWxlUGF0aCkgITtcbiAgICAgICAgICAgIGlmICghZmlsZVBhdGgpIHtcbiAgICAgICAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgICAgICBfX3N5bWJvbGljOiAnZXJyb3InLFxuICAgICAgICAgICAgICAgIG1lc3NhZ2U6IGBDb3VsZCBub3QgcmVzb2x2ZSAke21vZHVsZX0gcmVsYXRpdmUgdG8gJHtcbiAgICAgICAgICAgICAgICAgICAgc2VsZi5ob3N0LmdldE1ldGFkYXRhRm9yKHNvdXJjZVN5bWJvbC5maWxlUGF0aCl9LmAsXG4gICAgICAgICAgICAgICAgbGluZTogbWFwWydsaW5lJ10sXG4gICAgICAgICAgICAgICAgY2hhcmFjdGVyOiBtYXBbJ2NoYXJhY3RlciddLFxuICAgICAgICAgICAgICAgIGZpbGVOYW1lOiBnZXRPcmlnaW5hbE5hbWUoKVxuICAgICAgICAgICAgICB9O1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgICAgX19zeW1ib2xpYzogJ3Jlc29sdmVkJyxcbiAgICAgICAgICAgICAgc3ltYm9sOiBzZWxmLmdldFN0YXRpY1N5bWJvbChmaWxlUGF0aCwgbmFtZSksXG4gICAgICAgICAgICAgIGxpbmU6IG1hcFsnbGluZSddLFxuICAgICAgICAgICAgICBjaGFyYWN0ZXI6IG1hcFsnY2hhcmFjdGVyJ10sXG4gICAgICAgICAgICAgIGZpbGVOYW1lOiBnZXRPcmlnaW5hbE5hbWUoKVxuICAgICAgICAgICAgfTtcbiAgICAgICAgICB9IGVsc2UgaWYgKGZ1bmN0aW9uUGFyYW1zLmluZGV4T2YobmFtZSkgPj0gMCkge1xuICAgICAgICAgICAgLy8gcmVmZXJlbmNlIHRvIGEgZnVuY3Rpb24gcGFyYW1ldGVyXG4gICAgICAgICAgICByZXR1cm4ge19fc3ltYm9saWM6ICdyZWZlcmVuY2UnLCBuYW1lOiBuYW1lfTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgaWYgKHRvcExldmVsU3ltYm9sTmFtZXMuaGFzKG5hbWUpKSB7XG4gICAgICAgICAgICAgIHJldHVybiBzZWxmLmdldFN0YXRpY1N5bWJvbCh0b3BMZXZlbFBhdGgsIG5hbWUpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgLy8gYW1iaWVudCB2YWx1ZVxuICAgICAgICAgICAgbnVsbDtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSBpZiAoc3ltYm9saWMgPT09ICdlcnJvcicpIHtcbiAgICAgICAgICByZXR1cm4gey4uLm1hcCwgZmlsZU5hbWU6IGdldE9yaWdpbmFsTmFtZSgpfTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gc3VwZXIudmlzaXRTdHJpbmdNYXAobWFwLCBmdW5jdGlvblBhcmFtcyk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3QgdHJhbnNmb3JtZWRNZXRhID0gdmlzaXRWYWx1ZShtZXRhZGF0YSwgbmV3IFJlZmVyZW5jZVRyYW5zZm9ybWVyKCksIFtdKTtcbiAgICBsZXQgdW53cmFwcGVkVHJhbnNmb3JtZWRNZXRhID0gdW53cmFwUmVzb2x2ZWRNZXRhZGF0YSh0cmFuc2Zvcm1lZE1ldGEpO1xuICAgIGlmICh1bndyYXBwZWRUcmFuc2Zvcm1lZE1ldGEgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgIHJldHVybiB0aGlzLmNyZWF0ZUV4cG9ydChzb3VyY2VTeW1ib2wsIHVud3JhcHBlZFRyYW5zZm9ybWVkTWV0YSk7XG4gICAgfVxuICAgIHJldHVybiBuZXcgUmVzb2x2ZWRTdGF0aWNTeW1ib2woc291cmNlU3ltYm9sLCB0cmFuc2Zvcm1lZE1ldGEpO1xuICB9XG5cbiAgcHJpdmF0ZSBjcmVhdGVFeHBvcnQoc291cmNlU3ltYm9sOiBTdGF0aWNTeW1ib2wsIHRhcmdldFN5bWJvbDogU3RhdGljU3ltYm9sKTpcbiAgICAgIFJlc29sdmVkU3RhdGljU3ltYm9sIHtcbiAgICBzb3VyY2VTeW1ib2wuYXNzZXJ0Tm9NZW1iZXJzKCk7XG4gICAgdGFyZ2V0U3ltYm9sLmFzc2VydE5vTWVtYmVycygpO1xuICAgIGlmICh0aGlzLnN1bW1hcnlSZXNvbHZlci5pc0xpYnJhcnlGaWxlKHNvdXJjZVN5bWJvbC5maWxlUGF0aCkgJiZcbiAgICAgICAgdGhpcy5zdW1tYXJ5UmVzb2x2ZXIuaXNMaWJyYXJ5RmlsZSh0YXJnZXRTeW1ib2wuZmlsZVBhdGgpKSB7XG4gICAgICAvLyBUaGlzIGNhc2UgaXMgZm9yIGFuIG5nIGxpYnJhcnkgaW1wb3J0aW5nIHN5bWJvbHMgZnJvbSBhIHBsYWluIHRzIGxpYnJhcnlcbiAgICAgIC8vIHRyYW5zaXRpdmVseS5cbiAgICAgIC8vIE5vdGU6IFdlIHJlbHkgb24gdGhlIGZhY3QgdGhhdCB3ZSBkaXNjb3ZlciBzeW1ib2xzIGluIHRoZSBkaXJlY3Rpb25cbiAgICAgIC8vIGZyb20gc291cmNlIGZpbGVzIHRvIGxpYnJhcnkgZmlsZXNcbiAgICAgIHRoaXMuaW1wb3J0QXMuc2V0KHRhcmdldFN5bWJvbCwgdGhpcy5nZXRJbXBvcnRBcyhzb3VyY2VTeW1ib2wpIHx8IHNvdXJjZVN5bWJvbCk7XG4gICAgfVxuICAgIHJldHVybiBuZXcgUmVzb2x2ZWRTdGF0aWNTeW1ib2woc291cmNlU3ltYm9sLCB0YXJnZXRTeW1ib2wpO1xuICB9XG5cbiAgcHJpdmF0ZSByZXBvcnRFcnJvcihlcnJvcjogRXJyb3IsIGNvbnRleHQ/OiBTdGF0aWNTeW1ib2wsIHBhdGg/OiBzdHJpbmcpIHtcbiAgICBpZiAodGhpcy5lcnJvclJlY29yZGVyKSB7XG4gICAgICB0aGlzLmVycm9yUmVjb3JkZXIoZXJyb3IsIChjb250ZXh0ICYmIGNvbnRleHQuZmlsZVBhdGgpIHx8IHBhdGgpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aHJvdyBlcnJvcjtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIG1vZHVsZSBhbiBhYnNvbHV0ZSBwYXRoIHRvIGEgbW9kdWxlIGZpbGUuXG4gICAqL1xuICBwcml2YXRlIGdldE1vZHVsZU1ldGFkYXRhKG1vZHVsZTogc3RyaW5nKToge1trZXk6IHN0cmluZ106IGFueX0ge1xuICAgIGxldCBtb2R1bGVNZXRhZGF0YSA9IHRoaXMubWV0YWRhdGFDYWNoZS5nZXQobW9kdWxlKTtcbiAgICBpZiAoIW1vZHVsZU1ldGFkYXRhKSB7XG4gICAgICBjb25zdCBtb2R1bGVNZXRhZGF0YXMgPSB0aGlzLmhvc3QuZ2V0TWV0YWRhdGFGb3IobW9kdWxlKTtcbiAgICAgIGlmIChtb2R1bGVNZXRhZGF0YXMpIHtcbiAgICAgICAgbGV0IG1heFZlcnNpb24gPSAtMTtcbiAgICAgICAgbW9kdWxlTWV0YWRhdGFzLmZvckVhY2goKG1kKSA9PiB7XG4gICAgICAgICAgaWYgKG1kICYmIG1kWyd2ZXJzaW9uJ10gPiBtYXhWZXJzaW9uKSB7XG4gICAgICAgICAgICBtYXhWZXJzaW9uID0gbWRbJ3ZlcnNpb24nXTtcbiAgICAgICAgICAgIG1vZHVsZU1ldGFkYXRhID0gbWQ7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICAgIGlmICghbW9kdWxlTWV0YWRhdGEpIHtcbiAgICAgICAgbW9kdWxlTWV0YWRhdGEgPVxuICAgICAgICAgICAge19fc3ltYm9saWM6ICdtb2R1bGUnLCB2ZXJzaW9uOiBTVVBQT1JURURfU0NIRU1BX1ZFUlNJT04sIG1vZHVsZTogbW9kdWxlLCBtZXRhZGF0YToge319O1xuICAgICAgfVxuICAgICAgaWYgKG1vZHVsZU1ldGFkYXRhWyd2ZXJzaW9uJ10gIT0gU1VQUE9SVEVEX1NDSEVNQV9WRVJTSU9OKSB7XG4gICAgICAgIGNvbnN0IGVycm9yTWVzc2FnZSA9IG1vZHVsZU1ldGFkYXRhWyd2ZXJzaW9uJ10gPT0gMiA/XG4gICAgICAgICAgICBgVW5zdXBwb3J0ZWQgbWV0YWRhdGEgdmVyc2lvbiAke21vZHVsZU1ldGFkYXRhWyd2ZXJzaW9uJ119IGZvciBtb2R1bGUgJHttb2R1bGV9LiBUaGlzIG1vZHVsZSBzaG91bGQgYmUgY29tcGlsZWQgd2l0aCBhIG5ld2VyIHZlcnNpb24gb2YgbmdjYCA6XG4gICAgICAgICAgICBgTWV0YWRhdGEgdmVyc2lvbiBtaXNtYXRjaCBmb3IgbW9kdWxlICR7dGhpcy5ob3N0LmdldE91dHB1dE5hbWUobW9kdWxlKX0sIGZvdW5kIHZlcnNpb24gJHttb2R1bGVNZXRhZGF0YVsndmVyc2lvbiddfSwgZXhwZWN0ZWQgJHtTVVBQT1JURURfU0NIRU1BX1ZFUlNJT059YDtcbiAgICAgICAgdGhpcy5yZXBvcnRFcnJvcihuZXcgRXJyb3IoZXJyb3JNZXNzYWdlKSk7XG4gICAgICB9XG4gICAgICB0aGlzLm1ldGFkYXRhQ2FjaGUuc2V0KG1vZHVsZSwgbW9kdWxlTWV0YWRhdGEpO1xuICAgIH1cbiAgICByZXR1cm4gbW9kdWxlTWV0YWRhdGE7XG4gIH1cblxuXG4gIGdldFN5bWJvbEJ5TW9kdWxlKG1vZHVsZTogc3RyaW5nLCBzeW1ib2xOYW1lOiBzdHJpbmcsIGNvbnRhaW5pbmdGaWxlPzogc3RyaW5nKTogU3RhdGljU3ltYm9sIHtcbiAgICBjb25zdCBmaWxlUGF0aCA9IHRoaXMucmVzb2x2ZU1vZHVsZShtb2R1bGUsIGNvbnRhaW5pbmdGaWxlKTtcbiAgICBpZiAoIWZpbGVQYXRoKSB7XG4gICAgICB0aGlzLnJlcG9ydEVycm9yKFxuICAgICAgICAgIG5ldyBFcnJvcihgQ291bGQgbm90IHJlc29sdmUgbW9kdWxlICR7bW9kdWxlfSR7Y29udGFpbmluZ0ZpbGUgPyAnIHJlbGF0aXZlIHRvICcgK1xuICAgICAgICAgICAgdGhpcy5ob3N0LmdldE91dHB1dE5hbWUoY29udGFpbmluZ0ZpbGUpIDogJyd9YCkpO1xuICAgICAgcmV0dXJuIHRoaXMuZ2V0U3RhdGljU3ltYm9sKGBFUlJPUjoke21vZHVsZX1gLCBzeW1ib2xOYW1lKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuZ2V0U3RhdGljU3ltYm9sKGZpbGVQYXRoLCBzeW1ib2xOYW1lKTtcbiAgfVxuXG4gIHByaXZhdGUgcmVzb2x2ZU1vZHVsZShtb2R1bGU6IHN0cmluZywgY29udGFpbmluZ0ZpbGU/OiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgdHJ5IHtcbiAgICAgIHJldHVybiB0aGlzLmhvc3QubW9kdWxlTmFtZVRvRmlsZU5hbWUobW9kdWxlLCBjb250YWluaW5nRmlsZSk7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgY29uc29sZS5lcnJvcihgQ291bGQgbm90IHJlc29sdmUgbW9kdWxlICcke21vZHVsZX0nIHJlbGF0aXZlIHRvIGZpbGUgJHtjb250YWluaW5nRmlsZX1gKTtcbiAgICAgIHRoaXMucmVwb3J0RXJyb3IoZSwgdW5kZWZpbmVkLCBjb250YWluaW5nRmlsZSk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbi8vIFJlbW92ZSBleHRyYSB1bmRlcnNjb3JlIGZyb20gZXNjYXBlZCBpZGVudGlmaWVyLlxuLy8gU2VlIGh0dHBzOi8vZ2l0aHViLmNvbS9NaWNyb3NvZnQvVHlwZVNjcmlwdC9ibG9iL21hc3Rlci9zcmMvY29tcGlsZXIvdXRpbGl0aWVzLnRzXG5leHBvcnQgZnVuY3Rpb24gdW5lc2NhcGVJZGVudGlmaWVyKGlkZW50aWZpZXI6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBpZGVudGlmaWVyLnN0YXJ0c1dpdGgoJ19fXycpID8gaWRlbnRpZmllci5zdWJzdHIoMSkgOiBpZGVudGlmaWVyO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdW53cmFwUmVzb2x2ZWRNZXRhZGF0YShtZXRhZGF0YTogYW55KTogYW55IHtcbiAgaWYgKG1ldGFkYXRhICYmIG1ldGFkYXRhLl9fc3ltYm9saWMgPT09ICdyZXNvbHZlZCcpIHtcbiAgICByZXR1cm4gbWV0YWRhdGEuc3ltYm9sO1xuICB9XG4gIHJldHVybiBtZXRhZGF0YTtcbn1cbiJdfQ==