/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { CompileNgModuleMetadata, CompileSummaryKind } from '../compile_metadata';
import * as o from '../output/output_ast';
import { ValueTransformer, visitValue } from '../util';
import { StaticSymbol } from './static_symbol';
import { unwrapResolvedMetadata } from './static_symbol_resolver';
import { isLoweredSymbol, ngfactoryFilePath, summaryForJitFileName, summaryForJitName } from './util';
export function serializeSummaries(srcFileName, forJitCtx, summaryResolver, symbolResolver, symbols, types, createExternalSymbolReexports = false) {
    const toJsonSerializer = new ToJsonSerializer(symbolResolver, summaryResolver, srcFileName);
    // for symbols, we use everything except for the class metadata itself
    // (we keep the statics though), as the class metadata is contained in the
    // CompileTypeSummary.
    symbols.forEach((resolvedSymbol) => toJsonSerializer.addSummary({ symbol: resolvedSymbol.symbol, metadata: resolvedSymbol.metadata }));
    // Add type summaries.
    types.forEach(({ summary, metadata }) => {
        toJsonSerializer.addSummary({ symbol: summary.type.reference, metadata: undefined, type: summary });
    });
    const { json, exportAs } = toJsonSerializer.serialize(createExternalSymbolReexports);
    if (forJitCtx) {
        const forJitSerializer = new ForJitSerializer(forJitCtx, symbolResolver, summaryResolver);
        types.forEach(({ summary, metadata }) => { forJitSerializer.addSourceType(summary, metadata); });
        toJsonSerializer.unprocessedSymbolSummariesBySymbol.forEach((summary) => {
            if (summaryResolver.isLibraryFile(summary.symbol.filePath) && summary.type) {
                forJitSerializer.addLibType(summary.type);
            }
        });
        forJitSerializer.serialize(exportAs);
    }
    return { json, exportAs };
}
export function deserializeSummaries(symbolCache, summaryResolver, libraryFileName, json) {
    const deserializer = new FromJsonDeserializer(symbolCache, summaryResolver);
    return deserializer.deserialize(libraryFileName, json);
}
export function createForJitStub(outputCtx, reference) {
    return createSummaryForJitFunction(outputCtx, reference, o.NULL_EXPR);
}
function createSummaryForJitFunction(outputCtx, reference, value) {
    const fnName = summaryForJitName(reference.name);
    outputCtx.statements.push(o.fn([], [new o.ReturnStatement(value)], new o.ArrayType(o.DYNAMIC_TYPE)).toDeclStmt(fnName, [
        o.StmtModifier.Final, o.StmtModifier.Exported
    ]));
}
class ToJsonSerializer extends ValueTransformer {
    constructor(symbolResolver, summaryResolver, srcFileName) {
        super();
        this.symbolResolver = symbolResolver;
        this.summaryResolver = summaryResolver;
        this.srcFileName = srcFileName;
        // Note: This only contains symbols without members.
        this.symbols = [];
        this.indexBySymbol = new Map();
        this.reexportedBy = new Map();
        // This now contains a `__symbol: number` in the place of
        // StaticSymbols, but otherwise has the same shape as the original objects.
        this.processedSummaryBySymbol = new Map();
        this.processedSummaries = [];
        this.unprocessedSymbolSummariesBySymbol = new Map();
        this.moduleName = symbolResolver.getKnownModuleName(srcFileName);
    }
    addSummary(summary) {
        let unprocessedSummary = this.unprocessedSymbolSummariesBySymbol.get(summary.symbol);
        let processedSummary = this.processedSummaryBySymbol.get(summary.symbol);
        if (!unprocessedSummary) {
            unprocessedSummary = { symbol: summary.symbol, metadata: undefined };
            this.unprocessedSymbolSummariesBySymbol.set(summary.symbol, unprocessedSummary);
            processedSummary = { symbol: this.processValue(summary.symbol, 0 /* None */) };
            this.processedSummaries.push(processedSummary);
            this.processedSummaryBySymbol.set(summary.symbol, processedSummary);
        }
        if (!unprocessedSummary.metadata && summary.metadata) {
            let metadata = summary.metadata || {};
            if (metadata.__symbolic === 'class') {
                // For classes, we keep everything except their class decorators.
                // We need to keep e.g. the ctor args, method names, method decorators
                // so that the class can be extended in another compilation unit.
                // We don't keep the class decorators as
                // 1) they refer to data
                //   that should not cause a rebuild of downstream compilation units
                //   (e.g. inline templates of @Component, or @NgModule.declarations)
                // 2) their data is already captured in TypeSummaries, e.g. DirectiveSummary.
                const clone = {};
                Object.keys(metadata).forEach((propName) => {
                    if (propName !== 'decorators') {
                        clone[propName] = metadata[propName];
                    }
                });
                metadata = clone;
            }
            else if (isCall(metadata)) {
                if (!isFunctionCall(metadata) && !isMethodCallOnVariable(metadata)) {
                    // Don't store complex calls as we won't be able to simplify them anyways later on.
                    metadata = {
                        __symbolic: 'error',
                        message: 'Complex function calls are not supported.',
                    };
                }
            }
            // Note: We need to keep storing ctor calls for e.g.
            // `export const x = new InjectionToken(...)`
            unprocessedSummary.metadata = metadata;
            processedSummary.metadata = this.processValue(metadata, 1 /* ResolveValue */);
            if (metadata instanceof StaticSymbol &&
                this.summaryResolver.isLibraryFile(metadata.filePath)) {
                const declarationSymbol = this.symbols[this.indexBySymbol.get(metadata)];
                if (!isLoweredSymbol(declarationSymbol.name)) {
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
            if (summary.type.summaryKind === CompileSummaryKind.NgModule) {
                const ngModuleSummary = summary.type;
                ngModuleSummary.exportedDirectives.concat(ngModuleSummary.exportedPipes).forEach((id) => {
                    const symbol = id.reference;
                    if (this.summaryResolver.isLibraryFile(symbol.filePath) &&
                        !this.unprocessedSymbolSummariesBySymbol.has(symbol)) {
                        const summary = this.summaryResolver.resolveSummary(symbol);
                        if (summary) {
                            this.addSummary(summary);
                        }
                    }
                });
            }
        }
    }
    /**
     * @param createExternalSymbolReexports Whether external static symbols should be re-exported.
     * This can be enabled if external symbols should be re-exported by the current module in
     * order to avoid dynamically generated module dependencies which can break strict dependency
     * enforcements (as in Google3). Read more here: https://github.com/angular/angular/issues/25644
     */
    serialize(createExternalSymbolReexports) {
        const exportAs = [];
        const json = JSON.stringify({
            moduleName: this.moduleName,
            summaries: this.processedSummaries,
            symbols: this.symbols.map((symbol, index) => {
                symbol.assertNoMembers();
                let importAs = undefined;
                if (this.summaryResolver.isLibraryFile(symbol.filePath)) {
                    const reexportSymbol = this.reexportedBy.get(symbol);
                    if (reexportSymbol) {
                        // In case the given external static symbol is already manually exported by the
                        // user, we just proxy the external static symbol reference to the manual export.
                        // This ensures that the AOT compiler imports the external symbol through the
                        // user export and does not introduce another dependency which is not needed.
                        importAs = this.indexBySymbol.get(reexportSymbol);
                    }
                    else if (createExternalSymbolReexports) {
                        // In this case, the given external static symbol is *not* manually exported by
                        // the user, and we manually create a re-export in the factory file so that we
                        // don't introduce another module dependency. This is useful when running within
                        // Bazel so that the AOT compiler does not introduce any module dependencies
                        // which can break the strict dependency enforcement. (e.g. as in Google3)
                        // Read more about this here: https://github.com/angular/angular/issues/25644
                        const summary = this.unprocessedSymbolSummariesBySymbol.get(symbol);
                        if (!summary || !summary.metadata || summary.metadata.__symbolic !== 'interface') {
                            importAs = `${symbol.name}_${index}`;
                            exportAs.push({ symbol, exportAs: importAs });
                        }
                    }
                }
                return {
                    __symbol: index,
                    name: symbol.name,
                    filePath: this.summaryResolver.toSummaryFileName(symbol.filePath, this.srcFileName),
                    importAs: importAs
                };
            })
        });
        return { json, exportAs };
    }
    processValue(value, flags) {
        return visitValue(value, this, flags);
    }
    visitOther(value, context) {
        if (value instanceof StaticSymbol) {
            let baseSymbol = this.symbolResolver.getStaticSymbol(value.filePath, value.name);
            const index = this.visitStaticSymbol(baseSymbol, context);
            return { __symbol: index, members: value.members };
        }
    }
    /**
     * Strip line and character numbers from ngsummaries.
     * Emitting them causes white spaces changes to retrigger upstream
     * recompilations in bazel.
     * TODO: find out a way to have line and character numbers in errors without
     * excessive recompilation in bazel.
     */
    visitStringMap(map, context) {
        if (map['__symbolic'] === 'resolved') {
            return visitValue(map['symbol'], this, context);
        }
        if (map['__symbolic'] === 'error') {
            delete map['line'];
            delete map['character'];
        }
        return super.visitStringMap(map, context);
    }
    /**
     * Returns null if the options.resolveValue is true, and the summary for the symbol
     * resolved to a type or could not be resolved.
     */
    visitStaticSymbol(baseSymbol, flags) {
        let index = this.indexBySymbol.get(baseSymbol);
        let summary = null;
        if (flags & 1 /* ResolveValue */ &&
            this.summaryResolver.isLibraryFile(baseSymbol.filePath)) {
            if (this.unprocessedSymbolSummariesBySymbol.has(baseSymbol)) {
                // the summary for this symbol was already added
                // -> nothing to do.
                return index;
            }
            summary = this.loadSummary(baseSymbol);
            if (summary && summary.metadata instanceof StaticSymbol) {
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
    }
    loadSummary(symbol) {
        let summary = this.summaryResolver.resolveSummary(symbol);
        if (!summary) {
            // some symbols might originate from a plain typescript library
            // that just exported .d.ts and .metadata.json files, i.e. where no summary
            // files were created.
            const resolvedSymbol = this.symbolResolver.resolveSymbol(symbol);
            if (resolvedSymbol) {
                summary = { symbol: resolvedSymbol.symbol, metadata: resolvedSymbol.metadata };
            }
        }
        return summary;
    }
}
class ForJitSerializer {
    constructor(outputCtx, symbolResolver, summaryResolver) {
        this.outputCtx = outputCtx;
        this.symbolResolver = symbolResolver;
        this.summaryResolver = summaryResolver;
        this.data = [];
    }
    addSourceType(summary, metadata) {
        this.data.push({ summary, metadata, isLibrary: false });
    }
    addLibType(summary) {
        this.data.push({ summary, metadata: null, isLibrary: true });
    }
    serialize(exportAsArr) {
        const exportAsBySymbol = new Map();
        for (const { symbol, exportAs } of exportAsArr) {
            exportAsBySymbol.set(symbol, exportAs);
        }
        const ngModuleSymbols = new Set();
        for (const { summary, metadata, isLibrary } of this.data) {
            if (summary.summaryKind === CompileSummaryKind.NgModule) {
                // collect the symbols that refer to NgModule classes.
                // Note: we can't just rely on `summary.type.summaryKind` to determine this as
                // we don't add the summaries of all referenced symbols when we serialize type summaries.
                // See serializeSummaries for details.
                ngModuleSymbols.add(summary.type.reference);
                const modSummary = summary;
                for (const mod of modSummary.modules) {
                    ngModuleSymbols.add(mod.reference);
                }
            }
            if (!isLibrary) {
                const fnName = summaryForJitName(summary.type.reference.name);
                createSummaryForJitFunction(this.outputCtx, summary.type.reference, this.serializeSummaryWithDeps(summary, metadata));
            }
        }
        ngModuleSymbols.forEach((ngModuleSymbol) => {
            if (this.summaryResolver.isLibraryFile(ngModuleSymbol.filePath)) {
                let exportAs = exportAsBySymbol.get(ngModuleSymbol) || ngModuleSymbol.name;
                const jitExportAsName = summaryForJitName(exportAs);
                this.outputCtx.statements.push(o.variable(jitExportAsName)
                    .set(this.serializeSummaryRef(ngModuleSymbol))
                    .toDeclStmt(null, [o.StmtModifier.Exported]));
            }
        });
    }
    serializeSummaryWithDeps(summary, metadata) {
        const expressions = [this.serializeSummary(summary)];
        let providers = [];
        if (metadata instanceof CompileNgModuleMetadata) {
            expressions.push(...
            // For directives / pipes, we only add the declared ones,
            // and rely on transitively importing NgModules to get the transitive
            // summaries.
            metadata.declaredDirectives.concat(metadata.declaredPipes)
                .map(type => type.reference)
                // For modules,
                // we also add the summaries for modules
                // from libraries.
                // This is ok as we produce reexports for all transitive modules.
                .concat(metadata.transitiveModule.modules.map(type => type.reference)
                .filter(ref => ref !== metadata.type.reference))
                .map((ref) => this.serializeSummaryRef(ref)));
            // Note: We don't use `NgModuleSummary.providers`, as that one is transitive,
            // and we already have transitive modules.
            providers = metadata.providers;
        }
        else if (summary.summaryKind === CompileSummaryKind.Directive) {
            const dirSummary = summary;
            providers = dirSummary.providers.concat(dirSummary.viewProviders);
        }
        // Note: We can't just refer to the `ngsummary.ts` files for `useClass` providers (as we do for
        // declaredDirectives / declaredPipes), as we allow
        // providers without ctor arguments to skip the `@Injectable` decorator,
        // i.e. we didn't generate .ngsummary.ts files for these.
        expressions.push(...providers.filter(provider => !!provider.useClass).map(provider => this.serializeSummary({
            summaryKind: CompileSummaryKind.Injectable, type: provider.useClass
        })));
        return o.literalArr(expressions);
    }
    serializeSummaryRef(typeSymbol) {
        const jitImportedSymbol = this.symbolResolver.getStaticSymbol(summaryForJitFileName(typeSymbol.filePath), summaryForJitName(typeSymbol.name));
        return this.outputCtx.importExpr(jitImportedSymbol);
    }
    serializeSummary(data) {
        const outputCtx = this.outputCtx;
        class Transformer {
            visitArray(arr, context) {
                return o.literalArr(arr.map(entry => visitValue(entry, this, context)));
            }
            visitStringMap(map, context) {
                return new o.LiteralMapExpr(Object.keys(map).map((key) => new o.LiteralMapEntry(key, visitValue(map[key], this, context), false)));
            }
            visitPrimitive(value, context) { return o.literal(value); }
            visitOther(value, context) {
                if (value instanceof StaticSymbol) {
                    return outputCtx.importExpr(value);
                }
                else {
                    throw new Error(`Illegal State: Encountered value ${value}`);
                }
            }
        }
        return visitValue(data, new Transformer(), null);
    }
}
class FromJsonDeserializer extends ValueTransformer {
    constructor(symbolCache, summaryResolver) {
        super();
        this.symbolCache = symbolCache;
        this.summaryResolver = summaryResolver;
    }
    deserialize(libraryFileName, json) {
        const data = JSON.parse(json);
        const allImportAs = [];
        this.symbols = data.symbols.map((serializedSymbol) => this.symbolCache.get(this.summaryResolver.fromSummaryFileName(serializedSymbol.filePath, libraryFileName), serializedSymbol.name));
        data.symbols.forEach((serializedSymbol, index) => {
            const symbol = this.symbols[index];
            const importAs = serializedSymbol.importAs;
            if (typeof importAs === 'number') {
                allImportAs.push({ symbol, importAs: this.symbols[importAs] });
            }
            else if (typeof importAs === 'string') {
                allImportAs.push({ symbol, importAs: this.symbolCache.get(ngfactoryFilePath(libraryFileName), importAs) });
            }
        });
        const summaries = visitValue(data.summaries, this, null);
        return { moduleName: data.moduleName, summaries, importAs: allImportAs };
    }
    visitStringMap(map, context) {
        if ('__symbol' in map) {
            const baseSymbol = this.symbols[map['__symbol']];
            const members = map['members'];
            return members.length ? this.symbolCache.get(baseSymbol.filePath, baseSymbol.name, members) :
                baseSymbol;
        }
        else {
            return super.visitStringMap(map, context);
        }
    }
}
function isCall(metadata) {
    return metadata && metadata.__symbolic === 'call';
}
function isFunctionCall(metadata) {
    return isCall(metadata) && unwrapResolvedMetadata(metadata.expression) instanceof StaticSymbol;
}
function isMethodCallOnVariable(metadata) {
    return isCall(metadata) && metadata.expression && metadata.expression.__symbolic === 'select' &&
        unwrapResolvedMetadata(metadata.expression.expression) instanceof StaticSymbol;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3VtbWFyeV9zZXJpYWxpemVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2FvdC9zdW1tYXJ5X3NlcmlhbGl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBQ0gsT0FBTyxFQUFvRCx1QkFBdUIsRUFBd0Usa0JBQWtCLEVBQTBDLE1BQU0scUJBQXFCLENBQUM7QUFDbFAsT0FBTyxLQUFLLENBQUMsTUFBTSxzQkFBc0IsQ0FBQztBQUUxQyxPQUFPLEVBQWdCLGdCQUFnQixFQUFnQixVQUFVLEVBQUMsTUFBTSxTQUFTLENBQUM7QUFFbEYsT0FBTyxFQUFDLFlBQVksRUFBb0IsTUFBTSxpQkFBaUIsQ0FBQztBQUNoRSxPQUFPLEVBQTZDLHNCQUFzQixFQUFDLE1BQU0sMEJBQTBCLENBQUM7QUFDNUcsT0FBTyxFQUFDLGVBQWUsRUFBRSxpQkFBaUIsRUFBRSxxQkFBcUIsRUFBRSxpQkFBaUIsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUVwRyxNQUFNLFVBQVUsa0JBQWtCLENBQzlCLFdBQW1CLEVBQUUsU0FBK0IsRUFDcEQsZUFBOEMsRUFBRSxjQUFvQyxFQUNwRixPQUErQixFQUFFLEtBSTlCLEVBQ0gsNkJBQTZCLEdBQ3pCLEtBQUs7SUFDWCxNQUFNLGdCQUFnQixHQUFHLElBQUksZ0JBQWdCLENBQUMsY0FBYyxFQUFFLGVBQWUsRUFBRSxXQUFXLENBQUMsQ0FBQztJQUU1RixzRUFBc0U7SUFDdEUsMEVBQTBFO0lBQzFFLHNCQUFzQjtJQUN0QixPQUFPLENBQUMsT0FBTyxDQUNYLENBQUMsY0FBYyxFQUFFLEVBQUUsQ0FBQyxnQkFBZ0IsQ0FBQyxVQUFVLENBQzNDLEVBQUMsTUFBTSxFQUFFLGNBQWMsQ0FBQyxNQUFNLEVBQUUsUUFBUSxFQUFFLGNBQWMsQ0FBQyxRQUFRLEVBQUMsQ0FBQyxDQUFDLENBQUM7SUFFN0Usc0JBQXNCO0lBQ3RC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LE9BQU8sR0FBRyxHQUFHLENBQUMsU0FBUyxDQUFDLENBQUM7WUFDL0IsT0FBTyxPQUFPLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsV0FBVyxDQUFDLEdBQUcsQ0FBQyxVQUFVLENBQUMsUUFBUSxFQUFFLFVBQVUsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLENBQUMsQ0FBQztnQkFDckUsVUFBVSxDQUFDO1NBQ3BDO2FBQU07WUFDTCxPQUFPLEtBQUssQ0FBQyxjQUFjLENBQUMsR0FBRyxFQUFFLE9BQU8sQ0FBQyxDQUFDO1NBQzNDO0lBQ0gsQ0FBQztDQUNGO0FBRUQsU0FBUyxNQUFNLENBQUMsUUFBYTtJQUMzQixPQUFPLFFBQVEsSUFBSSxRQUFRLENBQUMsVUFBVSxLQUFLLE1BQU0sQ0FBQztBQUNwRCxDQUFDO0FBRUQsU0FBUyxjQUFjLENBQUMsUUFBYTtJQUNuQyxPQUFPLE1BQU0sQ0FBQyxRQUFRLENBQUMsSUFBSSxzQkFBc0IsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLFlBQVksWUFBWSxDQUFDO0FBQ2pHLENBQUM7QUFFRCxTQUFTLHNCQUFzQixDQUFDLFFBQWE7SUFDM0MsT0FBTyxNQUFNLENBQUMsUUFBUSxDQUFDLElBQUksUUFBUSxDQUFDLFVBQVUsSUFBSSxRQUFRLENBQUMsVUFBVSxDQUFDLFVBQVUsS0FBSyxRQUFRO1FBQ3pGLHNCQUFzQixDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsVUFBVSxDQUFDLFlBQVksWUFBWSxDQUFDO0FBQ3JGLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5pbXBvcnQge0NvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgQ29tcGlsZURpcmVjdGl2ZVN1bW1hcnksIENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhLCBDb21waWxlTmdNb2R1bGVTdW1tYXJ5LCBDb21waWxlUGlwZU1ldGFkYXRhLCBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSwgQ29tcGlsZVN1bW1hcnlLaW5kLCBDb21waWxlVHlwZU1ldGFkYXRhLCBDb21waWxlVHlwZVN1bW1hcnl9IGZyb20gJy4uL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuLi9vdXRwdXQvb3V0cHV0X2FzdCc7XG5pbXBvcnQge1N1bW1hcnksIFN1bW1hcnlSZXNvbHZlcn0gZnJvbSAnLi4vc3VtbWFyeV9yZXNvbHZlcic7XG5pbXBvcnQge091dHB1dENvbnRleHQsIFZhbHVlVHJhbnNmb3JtZXIsIFZhbHVlVmlzaXRvciwgdmlzaXRWYWx1ZX0gZnJvbSAnLi4vdXRpbCc7XG5cbmltcG9ydCB7U3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2xDYWNoZX0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sJztcbmltcG9ydCB7UmVzb2x2ZWRTdGF0aWNTeW1ib2wsIFN0YXRpY1N5bWJvbFJlc29sdmVyLCB1bndyYXBSZXNvbHZlZE1ldGFkYXRhfSBmcm9tICcuL3N0YXRpY19zeW1ib2xfcmVzb2x2ZXInO1xuaW1wb3J0IHtpc0xvd2VyZWRTeW1ib2wsIG5nZmFjdG9yeUZpbGVQYXRoLCBzdW1tYXJ5Rm9ySml0RmlsZU5hbWUsIHN1bW1hcnlGb3JKaXROYW1lfSBmcm9tICcuL3V0aWwnO1xuXG5leHBvcnQgZnVuY3Rpb24gc2VyaWFsaXplU3VtbWFyaWVzKFxuICAgIHNyY0ZpbGVOYW1lOiBzdHJpbmcsIGZvckppdEN0eDogT3V0cHV0Q29udGV4dCB8IG51bGwsXG4gICAgc3VtbWFyeVJlc29sdmVyOiBTdW1tYXJ5UmVzb2x2ZXI8U3RhdGljU3ltYm9sPiwgc3ltYm9sUmVzb2x2ZXI6IFN0YXRpY1N5bWJvbFJlc29sdmVyLFxuICAgIHN5bWJvbHM6IFJlc29sdmVkU3RhdGljU3ltYm9sW10sIHR5cGVzOiB7XG4gICAgICBzdW1tYXJ5OiBDb21waWxlVHlwZVN1bW1hcnksXG4gICAgICBtZXRhZGF0YTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEgfCBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEgfCBDb21waWxlUGlwZU1ldGFkYXRhIHxcbiAgICAgICAgICBDb21waWxlVHlwZU1ldGFkYXRhXG4gICAgfVtdLFxuICAgIGNyZWF0ZUV4dGVybmFsU3ltYm9sUmVleHBvcnRzID1cbiAgICAgICAgZmFsc2UpOiB7anNvbjogc3RyaW5nLCBleHBvcnRBczoge3N5bWJvbDogU3RhdGljU3ltYm9sLCBleHBvcnRBczogc3RyaW5nfVtdfSB7XG4gIGNvbnN0IHRvSnNvblNlcmlhbGl6ZXIgPSBuZXcgVG9Kc29uU2VyaWFsaXplcihzeW1ib2xSZXNvbHZlciwgc3VtbWFyeVJlc29sdmVyLCBzcmNGaWxlTmFtZSk7XG5cbiAgLy8gZm9yIHN5bWJvbHMsIHdlIHVzZSBldmVyeXRoaW5nIGV4Y2VwdCBmb3IgdGhlIGNsYXNzIG1ldGFkYXRhIGl0c2VsZlxuICAvLyAod2Uga2VlcCB0aGUgc3RhdGljcyB0aG91Z2gpLCBhcyB0aGUgY2xhc3MgbWV0YWRhdGEgaXMgY29udGFpbmVkIGluIHRoZVxuICAvLyBDb21waWxlVHlwZVN1bW1hcnkuXG4gIHN5bWJvbHMuZm9yRWFjaChcbiAgICAgIChyZXNvbHZlZFN5bWJvbCkgPT4gdG9Kc29uU2VyaWFsaXplci5hZGRTdW1tYXJ5KFxuICAgICAgICAgIHtzeW1ib2w6IHJlc29sdmVkU3ltYm9sLnN5bWJvbCwgbWV0YWRhdGE6IHJlc29sdmVkU3ltYm9sLm1ldGFkYXRhfSkpO1xuXG4gIC8vIEFkZCB0eXBlIHN1bW1hcmllcy5cbiAgdHlwZXMuZm9yRWFjaCgoe3N1bW1hcnksIG1ldGFkYXRhfSkgPT4ge1xuICAgIHRvSnNvblNlcmlhbGl6ZXIuYWRkU3VtbWFyeShcbiAgICAgICAge3N5bWJvbDogc3VtbWFyeS50eXBlLnJlZmVyZW5jZSwgbWV0YWRhdGE6IHVuZGVmaW5lZCwgdHlwZTogc3VtbWFyeX0pO1xuICB9KTtcbiAgY29uc3Qge2pzb24sIGV4cG9ydEFzfSA9IHRvSnNvblNlcmlhbGl6ZXIuc2VyaWFsaXplKGNyZWF0ZUV4dGVybmFsU3ltYm9sUmVleHBvcnRzKTtcbiAgaWYgKGZvckppdEN0eCkge1xuICAgIGNvbnN0IGZvckppdFNlcmlhbGl6ZXIgPSBuZXcgRm9ySml0U2VyaWFsaXplcihmb3JKaXRDdHgsIHN5bWJvbFJlc29sdmVyLCBzdW1tYXJ5UmVzb2x2ZXIpO1xuICAgIHR5cGVzLmZvckVhY2goKHtzdW1tYXJ5LCBtZXRhZGF0YX0pID0+IHsgZm9ySml0U2VyaWFsaXplci5hZGRTb3VyY2VUeXBlKHN1bW1hcnksIG1ldGFkYXRhKTsgfSk7XG4gICAgdG9Kc29uU2VyaWFsaXplci51bnByb2Nlc3NlZFN5bWJvbFN1bW1hcmllc0J5U3ltYm9sLmZvckVhY2goKHN1bW1hcnkpID0+IHtcbiAgICAgIGlmIChzdW1tYXJ5UmVzb2x2ZXIuaXNMaWJyYXJ5RmlsZShzdW1tYXJ5LnN5bWJvbC5maWxlUGF0aCkgJiYgc3VtbWFyeS50eXBlKSB7XG4gICAgICAgIGZvckppdFNlcmlhbGl6ZXIuYWRkTGliVHlwZShzdW1tYXJ5LnR5cGUpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIGZvckppdFNlcmlhbGl6ZXIuc2VyaWFsaXplKGV4cG9ydEFzKTtcbiAgfVxuICByZXR1cm4ge2pzb24sIGV4cG9ydEFzfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlc2VyaWFsaXplU3VtbWFyaWVzKFxuICAgIHN5bWJvbENhY2hlOiBTdGF0aWNTeW1ib2xDYWNoZSwgc3VtbWFyeVJlc29sdmVyOiBTdW1tYXJ5UmVzb2x2ZXI8U3RhdGljU3ltYm9sPixcbiAgICBsaWJyYXJ5RmlsZU5hbWU6IHN0cmluZywganNvbjogc3RyaW5nKToge1xuICBtb2R1bGVOYW1lOiBzdHJpbmcgfCBudWxsLFxuICBzdW1tYXJpZXM6IFN1bW1hcnk8U3RhdGljU3ltYm9sPltdLFxuICBpbXBvcnRBczoge3N5bWJvbDogU3RhdGljU3ltYm9sLCBpbXBvcnRBczogU3RhdGljU3ltYm9sfVtdXG59IHtcbiAgY29uc3QgZGVzZXJpYWxpemVyID0gbmV3IEZyb21Kc29uRGVzZXJpYWxpemVyKHN5bWJvbENhY2hlLCBzdW1tYXJ5UmVzb2x2ZXIpO1xuICByZXR1cm4gZGVzZXJpYWxpemVyLmRlc2VyaWFsaXplKGxpYnJhcnlGaWxlTmFtZSwganNvbik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVGb3JKaXRTdHViKG91dHB1dEN0eDogT3V0cHV0Q29udGV4dCwgcmVmZXJlbmNlOiBTdGF0aWNTeW1ib2wpIHtcbiAgcmV0dXJuIGNyZWF0ZVN1bW1hcnlGb3JKaXRGdW5jdGlvbihvdXRwdXRDdHgsIHJlZmVyZW5jZSwgby5OVUxMX0VYUFIpO1xufVxuXG5mdW5jdGlvbiBjcmVhdGVTdW1tYXJ5Rm9ySml0RnVuY3Rpb24oXG4gICAgb3V0cHV0Q3R4OiBPdXRwdXRDb250ZXh0LCByZWZlcmVuY2U6IFN0YXRpY1N5bWJvbCwgdmFsdWU6IG8uRXhwcmVzc2lvbikge1xuICBjb25zdCBmbk5hbWUgPSBzdW1tYXJ5Rm9ySml0TmFtZShyZWZlcmVuY2UubmFtZSk7XG4gIG91dHB1dEN0eC5zdGF0ZW1lbnRzLnB1c2goXG4gICAgICBvLmZuKFtdLCBbbmV3IG8uUmV0dXJuU3RhdGVtZW50KHZhbHVlKV0sIG5ldyBvLkFycmF5VHlwZShvLkRZTkFNSUNfVFlQRSkpLnRvRGVjbFN0bXQoZm5OYW1lLCBbXG4gICAgICAgIG8uU3RtdE1vZGlmaWVyLkZpbmFsLCBvLlN0bXRNb2RpZmllci5FeHBvcnRlZFxuICAgICAgXSkpO1xufVxuXG5jb25zdCBlbnVtIFNlcmlhbGl6YXRpb25GbGFncyB7XG4gIE5vbmUgPSAwLFxuICBSZXNvbHZlVmFsdWUgPSAxLFxufVxuXG5jbGFzcyBUb0pzb25TZXJpYWxpemVyIGV4dGVuZHMgVmFsdWVUcmFuc2Zvcm1lciB7XG4gIC8vIE5vdGU6IFRoaXMgb25seSBjb250YWlucyBzeW1ib2xzIHdpdGhvdXQgbWVtYmVycy5cbiAgcHJpdmF0ZSBzeW1ib2xzOiBTdGF0aWNTeW1ib2xbXSA9IFtdO1xuICBwcml2YXRlIGluZGV4QnlTeW1ib2wgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgbnVtYmVyPigpO1xuICBwcml2YXRlIHJlZXhwb3J0ZWRCeSA9IG5ldyBNYXA8U3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2w+KCk7XG4gIC8vIFRoaXMgbm93IGNvbnRhaW5zIGEgYF9fc3ltYm9sOiBudW1iZXJgIGluIHRoZSBwbGFjZSBvZlxuICAvLyBTdGF0aWNTeW1ib2xzLCBidXQgb3RoZXJ3aXNlIGhhcyB0aGUgc2FtZSBzaGFwZSBhcyB0aGUgb3JpZ2luYWwgb2JqZWN0cy5cbiAgcHJpdmF0ZSBwcm9jZXNzZWRTdW1tYXJ5QnlTeW1ib2wgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgYW55PigpO1xuICBwcml2YXRlIHByb2Nlc3NlZFN1bW1hcmllczogYW55W10gPSBbXTtcbiAgcHJpdmF0ZSBtb2R1bGVOYW1lOiBzdHJpbmd8bnVsbDtcblxuICB1bnByb2Nlc3NlZFN5bWJvbFN1bW1hcmllc0J5U3ltYm9sID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIFN1bW1hcnk8U3RhdGljU3ltYm9sPj4oKTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgc3ltYm9sUmVzb2x2ZXI6IFN0YXRpY1N5bWJvbFJlc29sdmVyLFxuICAgICAgcHJpdmF0ZSBzdW1tYXJ5UmVzb2x2ZXI6IFN1bW1hcnlSZXNvbHZlcjxTdGF0aWNTeW1ib2w+LCBwcml2YXRlIHNyY0ZpbGVOYW1lOiBzdHJpbmcpIHtcbiAgICBzdXBlcigpO1xuICAgIHRoaXMubW9kdWxlTmFtZSA9IHN5bWJvbFJlc29sdmVyLmdldEtub3duTW9kdWxlTmFtZShzcmNGaWxlTmFtZSk7XG4gIH1cblxuICBhZGRTdW1tYXJ5KHN1bW1hcnk6IFN1bW1hcnk8U3RhdGljU3ltYm9sPikge1xuICAgIGxldCB1bnByb2Nlc3NlZFN1bW1hcnkgPSB0aGlzLnVucHJvY2Vzc2VkU3ltYm9sU3VtbWFyaWVzQnlTeW1ib2wuZ2V0KHN1bW1hcnkuc3ltYm9sKTtcbiAgICBsZXQgcHJvY2Vzc2VkU3VtbWFyeSA9IHRoaXMucHJvY2Vzc2VkU3VtbWFyeUJ5U3ltYm9sLmdldChzdW1tYXJ5LnN5bWJvbCk7XG4gICAgaWYgKCF1bnByb2Nlc3NlZFN1bW1hcnkpIHtcbiAgICAgIHVucHJvY2Vzc2VkU3VtbWFyeSA9IHtzeW1ib2w6IHN1bW1hcnkuc3ltYm9sLCBtZXRhZGF0YTogdW5kZWZpbmVkfTtcbiAgICAgIHRoaXMudW5wcm9jZXNzZWRTeW1ib2xTdW1tYXJpZXNCeVN5bWJvbC5zZXQoc3VtbWFyeS5zeW1ib2wsIHVucHJvY2Vzc2VkU3VtbWFyeSk7XG4gICAgICBwcm9jZXNzZWRTdW1tYXJ5ID0ge3N5bWJvbDogdGhpcy5wcm9jZXNzVmFsdWUoc3VtbWFyeS5zeW1ib2wsIFNlcmlhbGl6YXRpb25GbGFncy5Ob25lKX07XG4gICAgICB0aGlzLnByb2Nlc3NlZFN1bW1hcmllcy5wdXNoKHByb2Nlc3NlZFN1bW1hcnkpO1xuICAgICAgdGhpcy5wcm9jZXNzZWRTdW1tYXJ5QnlTeW1ib2wuc2V0KHN1bW1hcnkuc3ltYm9sLCBwcm9jZXNzZWRTdW1tYXJ5KTtcbiAgICB9XG4gICAgaWYgKCF1bnByb2Nlc3NlZFN1bW1hcnkubWV0YWRhdGEgJiYgc3VtbWFyeS5tZXRhZGF0YSkge1xuICAgICAgbGV0IG1ldGFkYXRhID0gc3VtbWFyeS5tZXRhZGF0YSB8fCB7fTtcbiAgICAgIGlmIChtZXRhZGF0YS5fX3N5bWJvbGljID09PSAnY2xhc3MnKSB7XG4gICAgICAgIC8vIEZvciBjbGFzc2VzLCB3ZSBrZWVwIGV2ZXJ5dGhpbmcgZXhjZXB0IHRoZWlyIGNsYXNzIGRlY29yYXRvcnMuXG4gICAgICAgIC8vIFdlIG5lZWQgdG8ga2VlcCBlLmcuIHRoZSBjdG9yIGFyZ3MsIG1ldGhvZCBuYW1lcywgbWV0aG9kIGRlY29yYXRvcnNcbiAgICAgICAgLy8gc28gdGhhdCB0aGUgY2xhc3MgY2FuIGJlIGV4dGVuZGVkIGluIGFub3RoZXIgY29tcGlsYXRpb24gdW5pdC5cbiAgICAgICAgLy8gV2UgZG9uJ3Qga2VlcCB0aGUgY2xhc3MgZGVjb3JhdG9ycyBhc1xuICAgICAgICAvLyAxKSB0aGV5IHJlZmVyIHRvIGRhdGFcbiAgICAgICAgLy8gICB0aGF0IHNob3VsZCBub3QgY2F1c2UgYSByZWJ1aWxkIG9mIGRvd25zdHJlYW0gY29tcGlsYXRpb24gdW5pdHNcbiAgICAgICAgLy8gICAoZS5nLiBpbmxpbmUgdGVtcGxhdGVzIG9mIEBDb21wb25lbnQsIG9yIEBOZ01vZHVsZS5kZWNsYXJhdGlvbnMpXG4gICAgICAgIC8vIDIpIHRoZWlyIGRhdGEgaXMgYWxyZWFkeSBjYXB0dXJlZCBpbiBUeXBlU3VtbWFyaWVzLCBlLmcuIERpcmVjdGl2ZVN1bW1hcnkuXG4gICAgICAgIGNvbnN0IGNsb25lOiB7W2tleTogc3RyaW5nXTogYW55fSA9IHt9O1xuICAgICAgICBPYmplY3Qua2V5cyhtZXRhZGF0YSkuZm9yRWFjaCgocHJvcE5hbWUpID0+IHtcbiAgICAgICAgICBpZiAocHJvcE5hbWUgIT09ICdkZWNvcmF0b3JzJykge1xuICAgICAgICAgICAgY2xvbmVbcHJvcE5hbWVdID0gbWV0YWRhdGFbcHJvcE5hbWVdO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIG1ldGFkYXRhID0gY2xvbmU7XG4gICAgICB9IGVsc2UgaWYgKGlzQ2FsbChtZXRhZGF0YSkpIHtcbiAgICAgICAgaWYgKCFpc0Z1bmN0aW9uQ2FsbChtZXRhZGF0YSkgJiYgIWlzTWV0aG9kQ2FsbE9uVmFyaWFibGUobWV0YWRhdGEpKSB7XG4gICAgICAgICAgLy8gRG9uJ3Qgc3RvcmUgY29tcGxleCBjYWxscyBhcyB3ZSB3b24ndCBiZSBhYmxlIHRvIHNpbXBsaWZ5IHRoZW0gYW55d2F5cyBsYXRlciBvbi5cbiAgICAgICAgICBtZXRhZGF0YSA9IHtcbiAgICAgICAgICAgIF9fc3ltYm9saWM6ICdlcnJvcicsXG4gICAgICAgICAgICBtZXNzYWdlOiAnQ29tcGxleCBmdW5jdGlvbiBjYWxscyBhcmUgbm90IHN1cHBvcnRlZC4nLFxuICAgICAgICAgIH07XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIC8vIE5vdGU6IFdlIG5lZWQgdG8ga2VlcCBzdG9yaW5nIGN0b3IgY2FsbHMgZm9yIGUuZy5cbiAgICAgIC8vIGBleHBvcnQgY29uc3QgeCA9IG5ldyBJbmplY3Rpb25Ub2tlbiguLi4pYFxuICAgICAgdW5wcm9jZXNzZWRTdW1tYXJ5Lm1ldGFkYXRhID0gbWV0YWRhdGE7XG4gICAgICBwcm9jZXNzZWRTdW1tYXJ5Lm1ldGFkYXRhID0gdGhpcy5wcm9jZXNzVmFsdWUobWV0YWRhdGEsIFNlcmlhbGl6YXRpb25GbGFncy5SZXNvbHZlVmFsdWUpO1xuICAgICAgaWYgKG1ldGFkYXRhIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sICYmXG4gICAgICAgICAgdGhpcy5zdW1tYXJ5UmVzb2x2ZXIuaXNMaWJyYXJ5RmlsZShtZXRhZGF0YS5maWxlUGF0aCkpIHtcbiAgICAgICAgY29uc3QgZGVjbGFyYXRpb25TeW1ib2wgPSB0aGlzLnN5bWJvbHNbdGhpcy5pbmRleEJ5U3ltYm9sLmdldChtZXRhZGF0YSkgIV07XG4gICAgICAgIGlmICghaXNMb3dlcmVkU3ltYm9sKGRlY2xhcmF0aW9uU3ltYm9sLm5hbWUpKSB7XG4gICAgICAgICAgLy8gTm90ZTogc3ltYm9scyB0aGF0IHdlcmUgaW50cm9kdWNlZCBkdXJpbmcgY29kZWdlbiBpbiB0aGUgdXNlciBmaWxlIGNhbiBoYXZlIGEgcmVleHBvcnRcbiAgICAgICAgICAvLyBpZiBhIHVzZXIgdXNlZCBgZXhwb3J0ICpgLiBIb3dldmVyLCB3ZSBjYW4ndCByZWx5IG9uIHRoaXMgYXMgdHNpY2tsZSB3aWxsIGNoYW5nZVxuICAgICAgICAgIC8vIGBleHBvcnQgKmAgaW50byBuYW1lZCBleHBvcnRzLCB1c2luZyBvbmx5IHRoZSBpbmZvcm1hdGlvbiBmcm9tIHRoZSB0eXBlY2hlY2tlci5cbiAgICAgICAgICAvLyBBcyB3ZSBpbnRyb2R1Y2UgdGhlIG5ldyBzeW1ib2xzIGFmdGVyIHR5cGVjaGVjaywgVHNpY2tsZSBkb2VzIG5vdCBrbm93IGFib3V0IHRoZW0sXG4gICAgICAgICAgLy8gYW5kIG9taXRzIHRoZW0gd2hlbiBleHBhbmRpbmcgYGV4cG9ydCAqYC5cbiAgICAgICAgICAvLyBTbyB3ZSBoYXZlIHRvIGtlZXAgcmVleHBvcnRpbmcgdGhlc2Ugc3ltYm9scyBtYW51YWxseSB2aWEgLm5nZmFjdG9yeSBmaWxlcy5cbiAgICAgICAgICB0aGlzLnJlZXhwb3J0ZWRCeS5zZXQoZGVjbGFyYXRpb25TeW1ib2wsIHN1bW1hcnkuc3ltYm9sKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgICBpZiAoIXVucHJvY2Vzc2VkU3VtbWFyeS50eXBlICYmIHN1bW1hcnkudHlwZSkge1xuICAgICAgdW5wcm9jZXNzZWRTdW1tYXJ5LnR5cGUgPSBzdW1tYXJ5LnR5cGU7XG4gICAgICAvLyBOb3RlOiBXZSBkb24ndCBhZGQgdGhlIHN1bW1hcmllcyBvZiBhbGwgcmVmZXJlbmNlZCBzeW1ib2xzIGFzIGZvciB0aGUgUmVzb2x2ZWRTeW1ib2xzLFxuICAgICAgLy8gYXMgdGhlIHR5cGUgc3VtbWFyaWVzIGFscmVhZHkgY29udGFpbiB0aGUgdHJhbnNpdGl2ZSBkYXRhIHRoYXQgdGhleSByZXF1aXJlXG4gICAgICAvLyAoaW4gYSBtaW5pbWFsIHdheSkuXG4gICAgICBwcm9jZXNzZWRTdW1tYXJ5LnR5cGUgPSB0aGlzLnByb2Nlc3NWYWx1ZShzdW1tYXJ5LnR5cGUsIFNlcmlhbGl6YXRpb25GbGFncy5Ob25lKTtcbiAgICAgIC8vIGV4Y2VwdCBmb3IgcmVleHBvcnRlZCBkaXJlY3RpdmVzIC8gcGlwZXMsIHNvIHdlIG5lZWQgdG8gc3RvcmVcbiAgICAgIC8vIHRoZWlyIHN1bW1hcmllcyBleHBsaWNpdGx5LlxuICAgICAgaWYgKHN1bW1hcnkudHlwZS5zdW1tYXJ5S2luZCA9PT0gQ29tcGlsZVN1bW1hcnlLaW5kLk5nTW9kdWxlKSB7XG4gICAgICAgIGNvbnN0IG5nTW9kdWxlU3VtbWFyeSA9IDxDb21waWxlTmdNb2R1bGVTdW1tYXJ5PnN1bW1hcnkudHlwZTtcbiAgICAgICAgbmdNb2R1bGVTdW1tYXJ5LmV4cG9ydGVkRGlyZWN0aXZlcy5jb25jYXQobmdNb2R1bGVTdW1tYXJ5LmV4cG9ydGVkUGlwZXMpLmZvckVhY2goKGlkKSA9PiB7XG4gICAgICAgICAgY29uc3Qgc3ltYm9sOiBTdGF0aWNTeW1ib2wgPSBpZC5yZWZlcmVuY2U7XG4gICAgICAgICAgaWYgKHRoaXMuc3VtbWFyeVJlc29sdmVyLmlzTGlicmFyeUZpbGUoc3ltYm9sLmZpbGVQYXRoKSAmJlxuICAgICAgICAgICAgICAhdGhpcy51bnByb2Nlc3NlZFN5bWJvbFN1bW1hcmllc0J5U3ltYm9sLmhhcyhzeW1ib2wpKSB7XG4gICAgICAgICAgICBjb25zdCBzdW1tYXJ5ID0gdGhpcy5zdW1tYXJ5UmVzb2x2ZXIucmVzb2x2ZVN1bW1hcnkoc3ltYm9sKTtcbiAgICAgICAgICAgIGlmIChzdW1tYXJ5KSB7XG4gICAgICAgICAgICAgIHRoaXMuYWRkU3VtbWFyeShzdW1tYXJ5KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBAcGFyYW0gY3JlYXRlRXh0ZXJuYWxTeW1ib2xSZWV4cG9ydHMgV2hldGhlciBleHRlcm5hbCBzdGF0aWMgc3ltYm9scyBzaG91bGQgYmUgcmUtZXhwb3J0ZWQuXG4gICAqIFRoaXMgY2FuIGJlIGVuYWJsZWQgaWYgZXh0ZXJuYWwgc3ltYm9scyBzaG91bGQgYmUgcmUtZXhwb3J0ZWQgYnkgdGhlIGN1cnJlbnQgbW9kdWxlIGluXG4gICAqIG9yZGVyIHRvIGF2b2lkIGR5bmFtaWNhbGx5IGdlbmVyYXRlZCBtb2R1bGUgZGVwZW5kZW5jaWVzIHdoaWNoIGNhbiBicmVhayBzdHJpY3QgZGVwZW5kZW5jeVxuICAgKiBlbmZvcmNlbWVudHMgKGFzIGluIEdvb2dsZTMpLiBSZWFkIG1vcmUgaGVyZTogaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci9pc3N1ZXMvMjU2NDRcbiAgICovXG4gIHNlcmlhbGl6ZShjcmVhdGVFeHRlcm5hbFN5bWJvbFJlZXhwb3J0czogYm9vbGVhbik6XG4gICAgICB7anNvbjogc3RyaW5nLCBleHBvcnRBczoge3N5bWJvbDogU3RhdGljU3ltYm9sLCBleHBvcnRBczogc3RyaW5nfVtdfSB7XG4gICAgY29uc3QgZXhwb3J0QXM6IHtzeW1ib2w6IFN0YXRpY1N5bWJvbCwgZXhwb3J0QXM6IHN0cmluZ31bXSA9IFtdO1xuICAgIGNvbnN0IGpzb24gPSBKU09OLnN0cmluZ2lmeSh7XG4gICAgICBtb2R1bGVOYW1lOiB0aGlzLm1vZHVsZU5hbWUsXG4gICAgICBzdW1tYXJpZXM6IHRoaXMucHJvY2Vzc2VkU3VtbWFyaWVzLFxuICAgICAgc3ltYm9sczogdGhpcy5zeW1ib2xzLm1hcCgoc3ltYm9sLCBpbmRleCkgPT4ge1xuICAgICAgICBzeW1ib2wuYXNzZXJ0Tm9NZW1iZXJzKCk7XG4gICAgICAgIGxldCBpbXBvcnRBczogc3RyaW5nfG51bWJlciA9IHVuZGVmaW5lZCAhO1xuICAgICAgICBpZiAodGhpcy5zdW1tYXJ5UmVzb2x2ZXIuaXNMaWJyYXJ5RmlsZShzeW1ib2wuZmlsZVBhdGgpKSB7XG4gICAgICAgICAgY29uc3QgcmVleHBvcnRTeW1ib2wgPSB0aGlzLnJlZXhwb3J0ZWRCeS5nZXQoc3ltYm9sKTtcbiAgICAgICAgICBpZiAocmVleHBvcnRTeW1ib2wpIHtcbiAgICAgICAgICAgIC8vIEluIGNhc2UgdGhlIGdpdmVuIGV4dGVybmFsIHN0YXRpYyBzeW1ib2wgaXMgYWxyZWFkeSBtYW51YWxseSBleHBvcnRlZCBieSB0aGVcbiAgICAgICAgICAgIC8vIHVzZXIsIHdlIGp1c3QgcHJveHkgdGhlIGV4dGVybmFsIHN0YXRpYyBzeW1ib2wgcmVmZXJlbmNlIHRvIHRoZSBtYW51YWwgZXhwb3J0LlxuICAgICAgICAgICAgLy8gVGhpcyBlbnN1cmVzIHRoYXQgdGhlIEFPVCBjb21waWxlciBpbXBvcnRzIHRoZSBleHRlcm5hbCBzeW1ib2wgdGhyb3VnaCB0aGVcbiAgICAgICAgICAgIC8vIHVzZXIgZXhwb3J0IGFuZCBkb2VzIG5vdCBpbnRyb2R1Y2UgYW5vdGhlciBkZXBlbmRlbmN5IHdoaWNoIGlzIG5vdCBuZWVkZWQuXG4gICAgICAgICAgICBpbXBvcnRBcyA9IHRoaXMuaW5kZXhCeVN5bWJvbC5nZXQocmVleHBvcnRTeW1ib2wpICE7XG4gICAgICAgICAgfSBlbHNlIGlmIChjcmVhdGVFeHRlcm5hbFN5bWJvbFJlZXhwb3J0cykge1xuICAgICAgICAgICAgLy8gSW4gdGhpcyBjYXNlLCB0aGUgZ2l2ZW4gZXh0ZXJuYWwgc3RhdGljIHN5bWJvbCBpcyAqbm90KiBtYW51YWxseSBleHBvcnRlZCBieVxuICAgICAgICAgICAgLy8gdGhlIHVzZXIsIGFuZCB3ZSBtYW51YWxseSBjcmVhdGUgYSByZS1leHBvcnQgaW4gdGhlIGZhY3RvcnkgZmlsZSBzbyB0aGF0IHdlXG4gICAgICAgICAgICAvLyBkb24ndCBpbnRyb2R1Y2UgYW5vdGhlciBtb2R1bGUgZGVwZW5kZW5jeS4gVGhpcyBpcyB1c2VmdWwgd2hlbiBydW5uaW5nIHdpdGhpblxuICAgICAgICAgICAgLy8gQmF6ZWwgc28gdGhhdCB0aGUgQU9UIGNvbXBpbGVyIGRvZXMgbm90IGludHJvZHVjZSBhbnkgbW9kdWxlIGRlcGVuZGVuY2llc1xuICAgICAgICAgICAgLy8gd2hpY2ggY2FuIGJyZWFrIHRoZSBzdHJpY3QgZGVwZW5kZW5jeSBlbmZvcmNlbWVudC4gKGUuZy4gYXMgaW4gR29vZ2xlMylcbiAgICAgICAgICAgIC8vIFJlYWQgbW9yZSBhYm91dCB0aGlzIGhlcmU6IGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIvaXNzdWVzLzI1NjQ0XG4gICAgICAgICAgICBjb25zdCBzdW1tYXJ5ID0gdGhpcy51bnByb2Nlc3NlZFN5bWJvbFN1bW1hcmllc0J5U3ltYm9sLmdldChzeW1ib2wpO1xuICAgICAgICAgICAgaWYgKCFzdW1tYXJ5IHx8ICFzdW1tYXJ5Lm1ldGFkYXRhIHx8IHN1bW1hcnkubWV0YWRhdGEuX19zeW1ib2xpYyAhPT0gJ2ludGVyZmFjZScpIHtcbiAgICAgICAgICAgICAgaW1wb3J0QXMgPSBgJHtzeW1ib2wubmFtZX1fJHtpbmRleH1gO1xuICAgICAgICAgICAgICBleHBvcnRBcy5wdXNoKHtzeW1ib2wsIGV4cG9ydEFzOiBpbXBvcnRBc30pO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICByZXR1cm4ge1xuICAgICAgICAgIF9fc3ltYm9sOiBpbmRleCxcbiAgICAgICAgICBuYW1lOiBzeW1ib2wubmFtZSxcbiAgICAgICAgICBmaWxlUGF0aDogdGhpcy5zdW1tYXJ5UmVzb2x2ZXIudG9TdW1tYXJ5RmlsZU5hbWUoc3ltYm9sLmZpbGVQYXRoLCB0aGlzLnNyY0ZpbGVOYW1lKSxcbiAgICAgICAgICBpbXBvcnRBczogaW1wb3J0QXNcbiAgICAgICAgfTtcbiAgICAgIH0pXG4gICAgfSk7XG4gICAgcmV0dXJuIHtqc29uLCBleHBvcnRBc307XG4gIH1cblxuICBwcml2YXRlIHByb2Nlc3NWYWx1ZSh2YWx1ZTogYW55LCBmbGFnczogU2VyaWFsaXphdGlvbkZsYWdzKTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRWYWx1ZSh2YWx1ZSwgdGhpcywgZmxhZ3MpO1xuICB9XG5cbiAgdmlzaXRPdGhlcih2YWx1ZTogYW55LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkge1xuICAgICAgbGV0IGJhc2VTeW1ib2wgPSB0aGlzLnN5bWJvbFJlc29sdmVyLmdldFN0YXRpY1N5bWJvbCh2YWx1ZS5maWxlUGF0aCwgdmFsdWUubmFtZSk7XG4gICAgICBjb25zdCBpbmRleCA9IHRoaXMudmlzaXRTdGF0aWNTeW1ib2woYmFzZVN5bWJvbCwgY29udGV4dCk7XG4gICAgICByZXR1cm4ge19fc3ltYm9sOiBpbmRleCwgbWVtYmVyczogdmFsdWUubWVtYmVyc307XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFN0cmlwIGxpbmUgYW5kIGNoYXJhY3RlciBudW1iZXJzIGZyb20gbmdzdW1tYXJpZXMuXG4gICAqIEVtaXR0aW5nIHRoZW0gY2F1c2VzIHdoaXRlIHNwYWNlcyBjaGFuZ2VzIHRvIHJldHJpZ2dlciB1cHN0cmVhbVxuICAgKiByZWNvbXBpbGF0aW9ucyBpbiBiYXplbC5cbiAgICogVE9ETzogZmluZCBvdXQgYSB3YXkgdG8gaGF2ZSBsaW5lIGFuZCBjaGFyYWN0ZXIgbnVtYmVycyBpbiBlcnJvcnMgd2l0aG91dFxuICAgKiBleGNlc3NpdmUgcmVjb21waWxhdGlvbiBpbiBiYXplbC5cbiAgICovXG4gIHZpc2l0U3RyaW5nTWFwKG1hcDoge1trZXk6IHN0cmluZ106IGFueX0sIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgaWYgKG1hcFsnX19zeW1ib2xpYyddID09PSAncmVzb2x2ZWQnKSB7XG4gICAgICByZXR1cm4gdmlzaXRWYWx1ZShtYXBbJ3N5bWJvbCddLCB0aGlzLCBjb250ZXh0KTtcbiAgICB9XG4gICAgaWYgKG1hcFsnX19zeW1ib2xpYyddID09PSAnZXJyb3InKSB7XG4gICAgICBkZWxldGUgbWFwWydsaW5lJ107XG4gICAgICBkZWxldGUgbWFwWydjaGFyYWN0ZXInXTtcbiAgICB9XG4gICAgcmV0dXJuIHN1cGVyLnZpc2l0U3RyaW5nTWFwKG1hcCwgY29udGV4dCk7XG4gIH1cblxuICAvKipcbiAgICogUmV0dXJucyBudWxsIGlmIHRoZSBvcHRpb25zLnJlc29sdmVWYWx1ZSBpcyB0cnVlLCBhbmQgdGhlIHN1bW1hcnkgZm9yIHRoZSBzeW1ib2xcbiAgICogcmVzb2x2ZWQgdG8gYSB0eXBlIG9yIGNvdWxkIG5vdCBiZSByZXNvbHZlZC5cbiAgICovXG4gIHByaXZhdGUgdmlzaXRTdGF0aWNTeW1ib2woYmFzZVN5bWJvbDogU3RhdGljU3ltYm9sLCBmbGFnczogU2VyaWFsaXphdGlvbkZsYWdzKTogbnVtYmVyIHtcbiAgICBsZXQgaW5kZXg6IG51bWJlcnx1bmRlZmluZWR8bnVsbCA9IHRoaXMuaW5kZXhCeVN5bWJvbC5nZXQoYmFzZVN5bWJvbCk7XG4gICAgbGV0IHN1bW1hcnk6IFN1bW1hcnk8U3RhdGljU3ltYm9sPnxudWxsID0gbnVsbDtcbiAgICBpZiAoZmxhZ3MgJiBTZXJpYWxpemF0aW9uRmxhZ3MuUmVzb2x2ZVZhbHVlICYmXG4gICAgICAgIHRoaXMuc3VtbWFyeVJlc29sdmVyLmlzTGlicmFyeUZpbGUoYmFzZVN5bWJvbC5maWxlUGF0aCkpIHtcbiAgICAgIGlmICh0aGlzLnVucHJvY2Vzc2VkU3ltYm9sU3VtbWFyaWVzQnlTeW1ib2wuaGFzKGJhc2VTeW1ib2wpKSB7XG4gICAgICAgIC8vIHRoZSBzdW1tYXJ5IGZvciB0aGlzIHN5bWJvbCB3YXMgYWxyZWFkeSBhZGRlZFxuICAgICAgICAvLyAtPiBub3RoaW5nIHRvIGRvLlxuICAgICAgICByZXR1cm4gaW5kZXggITtcbiAgICAgIH1cbiAgICAgIHN1bW1hcnkgPSB0aGlzLmxvYWRTdW1tYXJ5KGJhc2VTeW1ib2wpO1xuICAgICAgaWYgKHN1bW1hcnkgJiYgc3VtbWFyeS5tZXRhZGF0YSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkge1xuICAgICAgICAvLyBUaGUgc3VtbWFyeSBpcyBhIHJlZXhwb3J0XG4gICAgICAgIGluZGV4ID0gdGhpcy52aXNpdFN0YXRpY1N5bWJvbChzdW1tYXJ5Lm1ldGFkYXRhLCBmbGFncyk7XG4gICAgICAgIC8vIHJlc2V0IHRoZSBzdW1tYXJ5IGFzIGl0IGlzIGp1c3QgYSByZWV4cG9ydCwgc28gd2UgZG9uJ3Qgd2FudCB0byBzdG9yZSBpdC5cbiAgICAgICAgc3VtbWFyeSA9IG51bGw7XG4gICAgICB9XG4gICAgfSBlbHNlIGlmIChpbmRleCAhPSBudWxsKSB7XG4gICAgICAvLyBOb3RlOiA9PSBvbiBwdXJwb3NlIHRvIGNvbXBhcmUgd2l0aCB1bmRlZmluZWQhXG4gICAgICAvLyBObyBzdW1tYXJ5IGFuZCB0aGUgc3ltYm9sIGlzIGFscmVhZHkgYWRkZWQgLT4gbm90aGluZyB0byBkby5cbiAgICAgIHJldHVybiBpbmRleDtcbiAgICB9XG4gICAgLy8gTm90ZTogPT0gb24gcHVycG9zZSB0byBjb21wYXJlIHdpdGggdW5kZWZpbmVkIVxuICAgIGlmIChpbmRleCA9PSBudWxsKSB7XG4gICAgICBpbmRleCA9IHRoaXMuc3ltYm9scy5sZW5ndGg7XG4gICAgICB0aGlzLnN5bWJvbHMucHVzaChiYXNlU3ltYm9sKTtcbiAgICB9XG4gICAgdGhpcy5pbmRleEJ5U3ltYm9sLnNldChiYXNlU3ltYm9sLCBpbmRleCk7XG4gICAgaWYgKHN1bW1hcnkpIHtcbiAgICAgIHRoaXMuYWRkU3VtbWFyeShzdW1tYXJ5KTtcbiAgICB9XG4gICAgcmV0dXJuIGluZGV4O1xuICB9XG5cbiAgcHJpdmF0ZSBsb2FkU3VtbWFyeShzeW1ib2w6IFN0YXRpY1N5bWJvbCk6IFN1bW1hcnk8U3RhdGljU3ltYm9sPnxudWxsIHtcbiAgICBsZXQgc3VtbWFyeSA9IHRoaXMuc3VtbWFyeVJlc29sdmVyLnJlc29sdmVTdW1tYXJ5KHN5bWJvbCk7XG4gICAgaWYgKCFzdW1tYXJ5KSB7XG4gICAgICAvLyBzb21lIHN5bWJvbHMgbWlnaHQgb3JpZ2luYXRlIGZyb20gYSBwbGFpbiB0eXBlc2NyaXB0IGxpYnJhcnlcbiAgICAgIC8vIHRoYXQganVzdCBleHBvcnRlZCAuZC50cyBhbmQgLm1ldGFkYXRhLmpzb24gZmlsZXMsIGkuZS4gd2hlcmUgbm8gc3VtbWFyeVxuICAgICAgLy8gZmlsZXMgd2VyZSBjcmVhdGVkLlxuICAgICAgY29uc3QgcmVzb2x2ZWRTeW1ib2wgPSB0aGlzLnN5bWJvbFJlc29sdmVyLnJlc29sdmVTeW1ib2woc3ltYm9sKTtcbiAgICAgIGlmIChyZXNvbHZlZFN5bWJvbCkge1xuICAgICAgICBzdW1tYXJ5ID0ge3N5bWJvbDogcmVzb2x2ZWRTeW1ib2wuc3ltYm9sLCBtZXRhZGF0YTogcmVzb2x2ZWRTeW1ib2wubWV0YWRhdGF9O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gc3VtbWFyeTtcbiAgfVxufVxuXG5jbGFzcyBGb3JKaXRTZXJpYWxpemVyIHtcbiAgcHJpdmF0ZSBkYXRhOiBBcnJheTx7XG4gICAgc3VtbWFyeTogQ29tcGlsZVR5cGVTdW1tYXJ5LFxuICAgIG1ldGFkYXRhOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YXxDb21waWxlRGlyZWN0aXZlTWV0YWRhdGF8Q29tcGlsZVBpcGVNZXRhZGF0YXxcbiAgICBDb21waWxlVHlwZU1ldGFkYXRhfG51bGwsXG4gICAgaXNMaWJyYXJ5OiBib29sZWFuXG4gIH0+ID0gW107XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIG91dHB1dEN0eDogT3V0cHV0Q29udGV4dCwgcHJpdmF0ZSBzeW1ib2xSZXNvbHZlcjogU3RhdGljU3ltYm9sUmVzb2x2ZXIsXG4gICAgICBwcml2YXRlIHN1bW1hcnlSZXNvbHZlcjogU3VtbWFyeVJlc29sdmVyPFN0YXRpY1N5bWJvbD4pIHt9XG5cbiAgYWRkU291cmNlVHlwZShcbiAgICAgIHN1bW1hcnk6IENvbXBpbGVUeXBlU3VtbWFyeSwgbWV0YWRhdGE6IENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhfENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YXxcbiAgICAgIENvbXBpbGVQaXBlTWV0YWRhdGF8Q29tcGlsZVR5cGVNZXRhZGF0YSkge1xuICAgIHRoaXMuZGF0YS5wdXNoKHtzdW1tYXJ5LCBtZXRhZGF0YSwgaXNMaWJyYXJ5OiBmYWxzZX0pO1xuICB9XG5cbiAgYWRkTGliVHlwZShzdW1tYXJ5OiBDb21waWxlVHlwZVN1bW1hcnkpIHtcbiAgICB0aGlzLmRhdGEucHVzaCh7c3VtbWFyeSwgbWV0YWRhdGE6IG51bGwsIGlzTGlicmFyeTogdHJ1ZX0pO1xuICB9XG5cbiAgc2VyaWFsaXplKGV4cG9ydEFzQXJyOiB7c3ltYm9sOiBTdGF0aWNTeW1ib2wsIGV4cG9ydEFzOiBzdHJpbmd9W10pOiB2b2lkIHtcbiAgICBjb25zdCBleHBvcnRBc0J5U3ltYm9sID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIHN0cmluZz4oKTtcbiAgICBmb3IgKGNvbnN0IHtzeW1ib2wsIGV4cG9ydEFzfSBvZiBleHBvcnRBc0Fycikge1xuICAgICAgZXhwb3J0QXNCeVN5bWJvbC5zZXQoc3ltYm9sLCBleHBvcnRBcyk7XG4gICAgfVxuICAgIGNvbnN0IG5nTW9kdWxlU3ltYm9scyA9IG5ldyBTZXQ8U3RhdGljU3ltYm9sPigpO1xuXG4gICAgZm9yIChjb25zdCB7c3VtbWFyeSwgbWV0YWRhdGEsIGlzTGlicmFyeX0gb2YgdGhpcy5kYXRhKSB7XG4gICAgICBpZiAoc3VtbWFyeS5zdW1tYXJ5S2luZCA9PT0gQ29tcGlsZVN1bW1hcnlLaW5kLk5nTW9kdWxlKSB7XG4gICAgICAgIC8vIGNvbGxlY3QgdGhlIHN5bWJvbHMgdGhhdCByZWZlciB0byBOZ01vZHVsZSBjbGFzc2VzLlxuICAgICAgICAvLyBOb3RlOiB3ZSBjYW4ndCBqdXN0IHJlbHkgb24gYHN1bW1hcnkudHlwZS5zdW1tYXJ5S2luZGAgdG8gZGV0ZXJtaW5lIHRoaXMgYXNcbiAgICAgICAgLy8gd2UgZG9uJ3QgYWRkIHRoZSBzdW1tYXJpZXMgb2YgYWxsIHJlZmVyZW5jZWQgc3ltYm9scyB3aGVuIHdlIHNlcmlhbGl6ZSB0eXBlIHN1bW1hcmllcy5cbiAgICAgICAgLy8gU2VlIHNlcmlhbGl6ZVN1bW1hcmllcyBmb3IgZGV0YWlscy5cbiAgICAgICAgbmdNb2R1bGVTeW1ib2xzLmFkZChzdW1tYXJ5LnR5cGUucmVmZXJlbmNlKTtcbiAgICAgICAgY29uc3QgbW9kU3VtbWFyeSA9IDxDb21waWxlTmdNb2R1bGVTdW1tYXJ5PnN1bW1hcnk7XG4gICAgICAgIGZvciAoY29uc3QgbW9kIG9mIG1vZFN1bW1hcnkubW9kdWxlcykge1xuICAgICAgICAgIG5nTW9kdWxlU3ltYm9scy5hZGQobW9kLnJlZmVyZW5jZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGlmICghaXNMaWJyYXJ5KSB7XG4gICAgICAgIGNvbnN0IGZuTmFtZSA9IHN1bW1hcnlGb3JKaXROYW1lKHN1bW1hcnkudHlwZS5yZWZlcmVuY2UubmFtZSk7XG4gICAgICAgIGNyZWF0ZVN1bW1hcnlGb3JKaXRGdW5jdGlvbihcbiAgICAgICAgICAgIHRoaXMub3V0cHV0Q3R4LCBzdW1tYXJ5LnR5cGUucmVmZXJlbmNlLFxuICAgICAgICAgICAgdGhpcy5zZXJpYWxpemVTdW1tYXJ5V2l0aERlcHMoc3VtbWFyeSwgbWV0YWRhdGEgISkpO1xuICAgICAgfVxuICAgIH1cblxuICAgIG5nTW9kdWxlU3ltYm9scy5mb3JFYWNoKChuZ01vZHVsZVN5bWJvbCkgPT4ge1xuICAgICAgaWYgKHRoaXMuc3VtbWFyeVJlc29sdmVyLmlzTGlicmFyeUZpbGUobmdNb2R1bGVTeW1ib2wuZmlsZVBhdGgpKSB7XG4gICAgICAgIGxldCBleHBvcnRBcyA9IGV4cG9ydEFzQnlTeW1ib2wuZ2V0KG5nTW9kdWxlU3ltYm9sKSB8fCBuZ01vZHVsZVN5bWJvbC5uYW1lO1xuICAgICAgICBjb25zdCBqaXRFeHBvcnRBc05hbWUgPSBzdW1tYXJ5Rm9ySml0TmFtZShleHBvcnRBcyk7XG4gICAgICAgIHRoaXMub3V0cHV0Q3R4LnN0YXRlbWVudHMucHVzaChvLnZhcmlhYmxlKGppdEV4cG9ydEFzTmFtZSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAuc2V0KHRoaXMuc2VyaWFsaXplU3VtbWFyeVJlZihuZ01vZHVsZVN5bWJvbCkpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLnRvRGVjbFN0bXQobnVsbCwgW28uU3RtdE1vZGlmaWVyLkV4cG9ydGVkXSkpO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBzZXJpYWxpemVTdW1tYXJ5V2l0aERlcHMoXG4gICAgICBzdW1tYXJ5OiBDb21waWxlVHlwZVN1bW1hcnksIG1ldGFkYXRhOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YXxDb21waWxlRGlyZWN0aXZlTWV0YWRhdGF8XG4gICAgICBDb21waWxlUGlwZU1ldGFkYXRhfENvbXBpbGVUeXBlTWV0YWRhdGEpOiBvLkV4cHJlc3Npb24ge1xuICAgIGNvbnN0IGV4cHJlc3Npb25zOiBvLkV4cHJlc3Npb25bXSA9IFt0aGlzLnNlcmlhbGl6ZVN1bW1hcnkoc3VtbWFyeSldO1xuICAgIGxldCBwcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW10gPSBbXTtcbiAgICBpZiAobWV0YWRhdGEgaW5zdGFuY2VvZiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSkge1xuICAgICAgZXhwcmVzc2lvbnMucHVzaCguLi5cbiAgICAgICAgICAgICAgICAgICAgICAgLy8gRm9yIGRpcmVjdGl2ZXMgLyBwaXBlcywgd2Ugb25seSBhZGQgdGhlIGRlY2xhcmVkIG9uZXMsXG4gICAgICAgICAgICAgICAgICAgICAgIC8vIGFuZCByZWx5IG9uIHRyYW5zaXRpdmVseSBpbXBvcnRpbmcgTmdNb2R1bGVzIHRvIGdldCB0aGUgdHJhbnNpdGl2ZVxuICAgICAgICAgICAgICAgICAgICAgICAvLyBzdW1tYXJpZXMuXG4gICAgICAgICAgICAgICAgICAgICAgIG1ldGFkYXRhLmRlY2xhcmVkRGlyZWN0aXZlcy5jb25jYXQobWV0YWRhdGEuZGVjbGFyZWRQaXBlcylcbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC5tYXAodHlwZSA9PiB0eXBlLnJlZmVyZW5jZSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC8vIEZvciBtb2R1bGVzLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gd2UgYWxzbyBhZGQgdGhlIHN1bW1hcmllcyBmb3IgbW9kdWxlc1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gZnJvbSBsaWJyYXJpZXMuXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAvLyBUaGlzIGlzIG9rIGFzIHdlIHByb2R1Y2UgcmVleHBvcnRzIGZvciBhbGwgdHJhbnNpdGl2ZSBtb2R1bGVzLlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgLmNvbmNhdChtZXRhZGF0YS50cmFuc2l0aXZlTW9kdWxlLm1vZHVsZXMubWFwKHR5cGUgPT4gdHlwZS5yZWZlcmVuY2UpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAuZmlsdGVyKHJlZiA9PiByZWYgIT09IG1ldGFkYXRhLnR5cGUucmVmZXJlbmNlKSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgIC5tYXAoKHJlZikgPT4gdGhpcy5zZXJpYWxpemVTdW1tYXJ5UmVmKHJlZikpKTtcbiAgICAgIC8vIE5vdGU6IFdlIGRvbid0IHVzZSBgTmdNb2R1bGVTdW1tYXJ5LnByb3ZpZGVyc2AsIGFzIHRoYXQgb25lIGlzIHRyYW5zaXRpdmUsXG4gICAgICAvLyBhbmQgd2UgYWxyZWFkeSBoYXZlIHRyYW5zaXRpdmUgbW9kdWxlcy5cbiAgICAgIHByb3ZpZGVycyA9IG1ldGFkYXRhLnByb3ZpZGVycztcbiAgICB9IGVsc2UgaWYgKHN1bW1hcnkuc3VtbWFyeUtpbmQgPT09IENvbXBpbGVTdW1tYXJ5S2luZC5EaXJlY3RpdmUpIHtcbiAgICAgIGNvbnN0IGRpclN1bW1hcnkgPSA8Q29tcGlsZURpcmVjdGl2ZVN1bW1hcnk+c3VtbWFyeTtcbiAgICAgIHByb3ZpZGVycyA9IGRpclN1bW1hcnkucHJvdmlkZXJzLmNvbmNhdChkaXJTdW1tYXJ5LnZpZXdQcm92aWRlcnMpO1xuICAgIH1cbiAgICAvLyBOb3RlOiBXZSBjYW4ndCBqdXN0IHJlZmVyIHRvIHRoZSBgbmdzdW1tYXJ5LnRzYCBmaWxlcyBmb3IgYHVzZUNsYXNzYCBwcm92aWRlcnMgKGFzIHdlIGRvIGZvclxuICAgIC8vIGRlY2xhcmVkRGlyZWN0aXZlcyAvIGRlY2xhcmVkUGlwZXMpLCBhcyB3ZSBhbGxvd1xuICAgIC8vIHByb3ZpZGVycyB3aXRob3V0IGN0b3IgYXJndW1lbnRzIHRvIHNraXAgdGhlIGBASW5qZWN0YWJsZWAgZGVjb3JhdG9yLFxuICAgIC8vIGkuZS4gd2UgZGlkbid0IGdlbmVyYXRlIC5uZ3N1bW1hcnkudHMgZmlsZXMgZm9yIHRoZXNlLlxuICAgIGV4cHJlc3Npb25zLnB1c2goXG4gICAgICAgIC4uLnByb3ZpZGVycy5maWx0ZXIocHJvdmlkZXIgPT4gISFwcm92aWRlci51c2VDbGFzcykubWFwKHByb3ZpZGVyID0+IHRoaXMuc2VyaWFsaXplU3VtbWFyeSh7XG4gICAgICAgICAgc3VtbWFyeUtpbmQ6IENvbXBpbGVTdW1tYXJ5S2luZC5JbmplY3RhYmxlLCB0eXBlOiBwcm92aWRlci51c2VDbGFzc1xuICAgICAgICB9IGFzIENvbXBpbGVUeXBlU3VtbWFyeSkpKTtcbiAgICByZXR1cm4gby5saXRlcmFsQXJyKGV4cHJlc3Npb25zKTtcbiAgfVxuXG4gIHByaXZhdGUgc2VyaWFsaXplU3VtbWFyeVJlZih0eXBlU3ltYm9sOiBTdGF0aWNTeW1ib2wpOiBvLkV4cHJlc3Npb24ge1xuICAgIGNvbnN0IGppdEltcG9ydGVkU3ltYm9sID0gdGhpcy5zeW1ib2xSZXNvbHZlci5nZXRTdGF0aWNTeW1ib2woXG4gICAgICAgIHN1bW1hcnlGb3JKaXRGaWxlTmFtZSh0eXBlU3ltYm9sLmZpbGVQYXRoKSwgc3VtbWFyeUZvckppdE5hbWUodHlwZVN5bWJvbC5uYW1lKSk7XG4gICAgcmV0dXJuIHRoaXMub3V0cHV0Q3R4LmltcG9ydEV4cHIoaml0SW1wb3J0ZWRTeW1ib2wpO1xuICB9XG5cbiAgcHJpdmF0ZSBzZXJpYWxpemVTdW1tYXJ5KGRhdGE6IHtba2V5OiBzdHJpbmddOiBhbnl9KTogby5FeHByZXNzaW9uIHtcbiAgICBjb25zdCBvdXRwdXRDdHggPSB0aGlzLm91dHB1dEN0eDtcblxuICAgIGNsYXNzIFRyYW5zZm9ybWVyIGltcGxlbWVudHMgVmFsdWVWaXNpdG9yIHtcbiAgICAgIHZpc2l0QXJyYXkoYXJyOiBhbnlbXSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICAgICAgcmV0dXJuIG8ubGl0ZXJhbEFycihhcnIubWFwKGVudHJ5ID0+IHZpc2l0VmFsdWUoZW50cnksIHRoaXMsIGNvbnRleHQpKSk7XG4gICAgICB9XG4gICAgICB2aXNpdFN0cmluZ01hcChtYXA6IHtba2V5OiBzdHJpbmddOiBhbnl9LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgICAgICByZXR1cm4gbmV3IG8uTGl0ZXJhbE1hcEV4cHIoT2JqZWN0LmtleXMobWFwKS5tYXAoXG4gICAgICAgICAgICAoa2V5KSA9PiBuZXcgby5MaXRlcmFsTWFwRW50cnkoa2V5LCB2aXNpdFZhbHVlKG1hcFtrZXldLCB0aGlzLCBjb250ZXh0KSwgZmFsc2UpKSk7XG4gICAgICB9XG4gICAgICB2aXNpdFByaW1pdGl2ZSh2YWx1ZTogYW55LCBjb250ZXh0OiBhbnkpOiBhbnkgeyByZXR1cm4gby5saXRlcmFsKHZhbHVlKTsgfVxuICAgICAgdmlzaXRPdGhlcih2YWx1ZTogYW55LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgICAgICBpZiAodmFsdWUgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgICAgICByZXR1cm4gb3V0cHV0Q3R4LmltcG9ydEV4cHIodmFsdWUpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcihgSWxsZWdhbCBTdGF0ZTogRW5jb3VudGVyZWQgdmFsdWUgJHt2YWx1ZX1gKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiB2aXNpdFZhbHVlKGRhdGEsIG5ldyBUcmFuc2Zvcm1lcigpLCBudWxsKTtcbiAgfVxufVxuXG5jbGFzcyBGcm9tSnNvbkRlc2VyaWFsaXplciBleHRlbmRzIFZhbHVlVHJhbnNmb3JtZXIge1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBzeW1ib2xzICE6IFN0YXRpY1N5bWJvbFtdO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBzeW1ib2xDYWNoZTogU3RhdGljU3ltYm9sQ2FjaGUsXG4gICAgICBwcml2YXRlIHN1bW1hcnlSZXNvbHZlcjogU3VtbWFyeVJlc29sdmVyPFN0YXRpY1N5bWJvbD4pIHtcbiAgICBzdXBlcigpO1xuICB9XG5cbiAgZGVzZXJpYWxpemUobGlicmFyeUZpbGVOYW1lOiBzdHJpbmcsIGpzb246IHN0cmluZyk6IHtcbiAgICBtb2R1bGVOYW1lOiBzdHJpbmcgfCBudWxsLFxuICAgIHN1bW1hcmllczogU3VtbWFyeTxTdGF0aWNTeW1ib2w+W10sXG4gICAgaW1wb3J0QXM6IHtzeW1ib2w6IFN0YXRpY1N5bWJvbCwgaW1wb3J0QXM6IFN0YXRpY1N5bWJvbH1bXVxuICB9IHtcbiAgICBjb25zdCBkYXRhOiB7bW9kdWxlTmFtZTogc3RyaW5nIHwgbnVsbCwgc3VtbWFyaWVzOiBhbnlbXSwgc3ltYm9sczogYW55W119ID0gSlNPTi5wYXJzZShqc29uKTtcbiAgICBjb25zdCBhbGxJbXBvcnRBczoge3N5bWJvbDogU3RhdGljU3ltYm9sLCBpbXBvcnRBczogU3RhdGljU3ltYm9sfVtdID0gW107XG4gICAgdGhpcy5zeW1ib2xzID0gZGF0YS5zeW1ib2xzLm1hcChcbiAgICAgICAgKHNlcmlhbGl6ZWRTeW1ib2wpID0+IHRoaXMuc3ltYm9sQ2FjaGUuZ2V0KFxuICAgICAgICAgICAgdGhpcy5zdW1tYXJ5UmVzb2x2ZXIuZnJvbVN1bW1hcnlGaWxlTmFtZShzZXJpYWxpemVkU3ltYm9sLmZpbGVQYXRoLCBsaWJyYXJ5RmlsZU5hbWUpLFxuICAgICAgICAgICAgc2VyaWFsaXplZFN5bWJvbC5uYW1lKSk7XG4gICAgZGF0YS5zeW1ib2xzLmZvckVhY2goKHNlcmlhbGl6ZWRTeW1ib2wsIGluZGV4KSA9PiB7XG4gICAgICBjb25zdCBzeW1ib2wgPSB0aGlzLnN5bWJvbHNbaW5kZXhdO1xuICAgICAgY29uc3QgaW1wb3J0QXMgPSBzZXJpYWxpemVkU3ltYm9sLmltcG9ydEFzO1xuICAgICAgaWYgKHR5cGVvZiBpbXBvcnRBcyA9PT0gJ251bWJlcicpIHtcbiAgICAgICAgYWxsSW1wb3J0QXMucHVzaCh7c3ltYm9sLCBpbXBvcnRBczogdGhpcy5zeW1ib2xzW2ltcG9ydEFzXX0pO1xuICAgICAgfSBlbHNlIGlmICh0eXBlb2YgaW1wb3J0QXMgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIGFsbEltcG9ydEFzLnB1c2goXG4gICAgICAgICAgICB7c3ltYm9sLCBpbXBvcnRBczogdGhpcy5zeW1ib2xDYWNoZS5nZXQobmdmYWN0b3J5RmlsZVBhdGgobGlicmFyeUZpbGVOYW1lKSwgaW1wb3J0QXMpfSk7XG4gICAgICB9XG4gICAgfSk7XG4gICAgY29uc3Qgc3VtbWFyaWVzID0gdmlzaXRWYWx1ZShkYXRhLnN1bW1hcmllcywgdGhpcywgbnVsbCkgYXMgU3VtbWFyeTxTdGF0aWNTeW1ib2w+W107XG4gICAgcmV0dXJuIHttb2R1bGVOYW1lOiBkYXRhLm1vZHVsZU5hbWUsIHN1bW1hcmllcywgaW1wb3J0QXM6IGFsbEltcG9ydEFzfTtcbiAgfVxuXG4gIHZpc2l0U3RyaW5nTWFwKG1hcDoge1trZXk6IHN0cmluZ106IGFueX0sIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgaWYgKCdfX3N5bWJvbCcgaW4gbWFwKSB7XG4gICAgICBjb25zdCBiYXNlU3ltYm9sID0gdGhpcy5zeW1ib2xzW21hcFsnX19zeW1ib2wnXV07XG4gICAgICBjb25zdCBtZW1iZXJzID0gbWFwWydtZW1iZXJzJ107XG4gICAgICByZXR1cm4gbWVtYmVycy5sZW5ndGggPyB0aGlzLnN5bWJvbENhY2hlLmdldChiYXNlU3ltYm9sLmZpbGVQYXRoLCBiYXNlU3ltYm9sLm5hbWUsIG1lbWJlcnMpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGJhc2VTeW1ib2w7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiBzdXBlci52aXNpdFN0cmluZ01hcChtYXAsIGNvbnRleHQpO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBpc0NhbGwobWV0YWRhdGE6IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gbWV0YWRhdGEgJiYgbWV0YWRhdGEuX19zeW1ib2xpYyA9PT0gJ2NhbGwnO1xufVxuXG5mdW5jdGlvbiBpc0Z1bmN0aW9uQ2FsbChtZXRhZGF0YTogYW55KTogYm9vbGVhbiB7XG4gIHJldHVybiBpc0NhbGwobWV0YWRhdGEpICYmIHVud3JhcFJlc29sdmVkTWV0YWRhdGEobWV0YWRhdGEuZXhwcmVzc2lvbikgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2w7XG59XG5cbmZ1bmN0aW9uIGlzTWV0aG9kQ2FsbE9uVmFyaWFibGUobWV0YWRhdGE6IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gaXNDYWxsKG1ldGFkYXRhKSAmJiBtZXRhZGF0YS5leHByZXNzaW9uICYmIG1ldGFkYXRhLmV4cHJlc3Npb24uX19zeW1ib2xpYyA9PT0gJ3NlbGVjdCcgJiZcbiAgICAgIHVud3JhcFJlc29sdmVkTWV0YWRhdGEobWV0YWRhdGEuZXhwcmVzc2lvbi5leHByZXNzaW9uKSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbDtcbn1cbiJdfQ==