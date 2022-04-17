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
        define("@angular/compiler/src/jit/compiler", ["require", "exports", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/constant_pool", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/output/output_interpreter", "@angular/compiler/src/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var constant_pool_1 = require("@angular/compiler/src/constant_pool");
    var ir = require("@angular/compiler/src/output/output_ast");
    var output_interpreter_1 = require("@angular/compiler/src/output/output_interpreter");
    var util_1 = require("@angular/compiler/src/util");
    /**
     * An internal module of the Angular compiler that begins with component types,
     * extracts templates, and eventually produces a compiled version of the component
     * ready for linking into an application.
     *
     * @security  When compiling templates at runtime, you must ensure that the entire template comes
     * from a trusted source. Attacker-controlled data introduced by a template could expose your
     * application to XSS risks.  For more detail, see the [Security Guide](http://g.co/ng/security).
     */
    var JitCompiler = /** @class */ (function () {
        function JitCompiler(_metadataResolver, _templateParser, _styleCompiler, _viewCompiler, _ngModuleCompiler, _summaryResolver, _reflector, _jitEvaluator, _compilerConfig, _console, getExtraNgModuleProviders) {
            this._metadataResolver = _metadataResolver;
            this._templateParser = _templateParser;
            this._styleCompiler = _styleCompiler;
            this._viewCompiler = _viewCompiler;
            this._ngModuleCompiler = _ngModuleCompiler;
            this._summaryResolver = _summaryResolver;
            this._reflector = _reflector;
            this._jitEvaluator = _jitEvaluator;
            this._compilerConfig = _compilerConfig;
            this._console = _console;
            this.getExtraNgModuleProviders = getExtraNgModuleProviders;
            this._compiledTemplateCache = new Map();
            this._compiledHostTemplateCache = new Map();
            this._compiledDirectiveWrapperCache = new Map();
            this._compiledNgModuleCache = new Map();
            this._sharedStylesheetCount = 0;
            this._addedAotSummaries = new Set();
        }
        JitCompiler.prototype.compileModuleSync = function (moduleType) {
            return util_1.SyncAsync.assertSync(this._compileModuleAndComponents(moduleType, true));
        };
        JitCompiler.prototype.compileModuleAsync = function (moduleType) {
            return Promise.resolve(this._compileModuleAndComponents(moduleType, false));
        };
        JitCompiler.prototype.compileModuleAndAllComponentsSync = function (moduleType) {
            return util_1.SyncAsync.assertSync(this._compileModuleAndAllComponents(moduleType, true));
        };
        JitCompiler.prototype.compileModuleAndAllComponentsAsync = function (moduleType) {
            return Promise.resolve(this._compileModuleAndAllComponents(moduleType, false));
        };
        JitCompiler.prototype.getComponentFactory = function (component) {
            var summary = this._metadataResolver.getDirectiveSummary(component);
            return summary.componentFactory;
        };
        JitCompiler.prototype.loadAotSummaries = function (summaries) {
            this.clearCache();
            this._addAotSummaries(summaries);
        };
        JitCompiler.prototype._addAotSummaries = function (fn) {
            if (this._addedAotSummaries.has(fn)) {
                return;
            }
            this._addedAotSummaries.add(fn);
            var summaries = fn();
            for (var i = 0; i < summaries.length; i++) {
                var entry = summaries[i];
                if (typeof entry === 'function') {
                    this._addAotSummaries(entry);
                }
                else {
                    var summary = entry;
                    this._summaryResolver.addSummary({ symbol: summary.type.reference, metadata: null, type: summary });
                }
            }
        };
        JitCompiler.prototype.hasAotSummary = function (ref) { return !!this._summaryResolver.resolveSummary(ref); };
        JitCompiler.prototype._filterJitIdentifiers = function (ids) {
            var _this = this;
            return ids.map(function (mod) { return mod.reference; }).filter(function (ref) { return !_this.hasAotSummary(ref); });
        };
        JitCompiler.prototype._compileModuleAndComponents = function (moduleType, isSync) {
            var _this = this;
            return util_1.SyncAsync.then(this._loadModules(moduleType, isSync), function () {
                _this._compileComponents(moduleType, null);
                return _this._compileModule(moduleType);
            });
        };
        JitCompiler.prototype._compileModuleAndAllComponents = function (moduleType, isSync) {
            var _this = this;
            return util_1.SyncAsync.then(this._loadModules(moduleType, isSync), function () {
                var componentFactories = [];
                _this._compileComponents(moduleType, componentFactories);
                return {
                    ngModuleFactory: _this._compileModule(moduleType),
                    componentFactories: componentFactories
                };
            });
        };
        JitCompiler.prototype._loadModules = function (mainModule, isSync) {
            var _this = this;
            var loading = [];
            var mainNgModule = this._metadataResolver.getNgModuleMetadata(mainModule);
            // Note: for runtime compilation, we want to transitively compile all modules,
            // so we also need to load the declared directives / pipes for all nested modules.
            this._filterJitIdentifiers(mainNgModule.transitiveModule.modules).forEach(function (nestedNgModule) {
                // getNgModuleMetadata only returns null if the value passed in is not an NgModule
                var moduleMeta = _this._metadataResolver.getNgModuleMetadata(nestedNgModule);
                _this._filterJitIdentifiers(moduleMeta.declaredDirectives).forEach(function (ref) {
                    var promise = _this._metadataResolver.loadDirectiveMetadata(moduleMeta.type.reference, ref, isSync);
                    if (promise) {
                        loading.push(promise);
                    }
                });
                _this._filterJitIdentifiers(moduleMeta.declaredPipes)
                    .forEach(function (ref) { return _this._metadataResolver.getOrLoadPipeMetadata(ref); });
            });
            return util_1.SyncAsync.all(loading);
        };
        JitCompiler.prototype._compileModule = function (moduleType) {
            var ngModuleFactory = this._compiledNgModuleCache.get(moduleType);
            if (!ngModuleFactory) {
                var moduleMeta = this._metadataResolver.getNgModuleMetadata(moduleType);
                // Always provide a bound Compiler
                var extraProviders = this.getExtraNgModuleProviders(moduleMeta.type.reference);
                var outputCtx = createOutputContext();
                var compileResult = this._ngModuleCompiler.compile(outputCtx, moduleMeta, extraProviders);
                ngModuleFactory = this._interpretOrJit(compile_metadata_1.ngModuleJitUrl(moduleMeta), outputCtx.statements)[compileResult.ngModuleFactoryVar];
                this._compiledNgModuleCache.set(moduleMeta.type.reference, ngModuleFactory);
            }
            return ngModuleFactory;
        };
        /**
         * @internal
         */
        JitCompiler.prototype._compileComponents = function (mainModule, allComponentFactories) {
            var _this = this;
            var ngModule = this._metadataResolver.getNgModuleMetadata(mainModule);
            var moduleByJitDirective = new Map();
            var templates = new Set();
            var transJitModules = this._filterJitIdentifiers(ngModule.transitiveModule.modules);
            transJitModules.forEach(function (localMod) {
                var localModuleMeta = _this._metadataResolver.getNgModuleMetadata(localMod);
                _this._filterJitIdentifiers(localModuleMeta.declaredDirectives).forEach(function (dirRef) {
                    moduleByJitDirective.set(dirRef, localModuleMeta);
                    var dirMeta = _this._metadataResolver.getDirectiveMetadata(dirRef);
                    if (dirMeta.isComponent) {
                        templates.add(_this._createCompiledTemplate(dirMeta, localModuleMeta));
                        if (allComponentFactories) {
                            var template = _this._createCompiledHostTemplate(dirMeta.type.reference, localModuleMeta);
                            templates.add(template);
                            allComponentFactories.push(dirMeta.componentFactory);
                        }
                    }
                });
            });
            transJitModules.forEach(function (localMod) {
                var localModuleMeta = _this._metadataResolver.getNgModuleMetadata(localMod);
                _this._filterJitIdentifiers(localModuleMeta.declaredDirectives).forEach(function (dirRef) {
                    var dirMeta = _this._metadataResolver.getDirectiveMetadata(dirRef);
                    if (dirMeta.isComponent) {
                        dirMeta.entryComponents.forEach(function (entryComponentType) {
                            var moduleMeta = moduleByJitDirective.get(entryComponentType.componentType);
                            templates.add(_this._createCompiledHostTemplate(entryComponentType.componentType, moduleMeta));
                        });
                    }
                });
                localModuleMeta.entryComponents.forEach(function (entryComponentType) {
                    if (!_this.hasAotSummary(entryComponentType.componentType)) {
                        var moduleMeta = moduleByJitDirective.get(entryComponentType.componentType);
                        templates.add(_this._createCompiledHostTemplate(entryComponentType.componentType, moduleMeta));
                    }
                });
            });
            templates.forEach(function (template) { return _this._compileTemplate(template); });
        };
        JitCompiler.prototype.clearCacheFor = function (type) {
            this._compiledNgModuleCache.delete(type);
            this._metadataResolver.clearCacheFor(type);
            this._compiledHostTemplateCache.delete(type);
            var compiledTemplate = this._compiledTemplateCache.get(type);
            if (compiledTemplate) {
                this._compiledTemplateCache.delete(type);
            }
        };
        JitCompiler.prototype.clearCache = function () {
            // Note: don't clear the _addedAotSummaries, as they don't change!
            this._metadataResolver.clearCache();
            this._compiledTemplateCache.clear();
            this._compiledHostTemplateCache.clear();
            this._compiledNgModuleCache.clear();
        };
        JitCompiler.prototype._createCompiledHostTemplate = function (compType, ngModule) {
            if (!ngModule) {
                throw new Error("Component " + util_1.stringify(compType) + " is not part of any NgModule or the module has not been imported into your module.");
            }
            var compiledTemplate = this._compiledHostTemplateCache.get(compType);
            if (!compiledTemplate) {
                var compMeta = this._metadataResolver.getDirectiveMetadata(compType);
                assertComponent(compMeta);
                var hostMeta = this._metadataResolver.getHostComponentMetadata(compMeta, compMeta.componentFactory.viewDefFactory);
                compiledTemplate =
                    new CompiledTemplate(true, compMeta.type, hostMeta, ngModule, [compMeta.type]);
                this._compiledHostTemplateCache.set(compType, compiledTemplate);
            }
            return compiledTemplate;
        };
        JitCompiler.prototype._createCompiledTemplate = function (compMeta, ngModule) {
            var compiledTemplate = this._compiledTemplateCache.get(compMeta.type.reference);
            if (!compiledTemplate) {
                assertComponent(compMeta);
                compiledTemplate = new CompiledTemplate(false, compMeta.type, compMeta, ngModule, ngModule.transitiveModule.directives);
                this._compiledTemplateCache.set(compMeta.type.reference, compiledTemplate);
            }
            return compiledTemplate;
        };
        JitCompiler.prototype._compileTemplate = function (template) {
            var _this = this;
            if (template.isCompiled) {
                return;
            }
            var compMeta = template.compMeta;
            var externalStylesheetsByModuleUrl = new Map();
            var outputContext = createOutputContext();
            var componentStylesheet = this._styleCompiler.compileComponent(outputContext, compMeta);
            compMeta.template.externalStylesheets.forEach(function (stylesheetMeta) {
                var compiledStylesheet = _this._styleCompiler.compileStyles(createOutputContext(), compMeta, stylesheetMeta);
                externalStylesheetsByModuleUrl.set(stylesheetMeta.moduleUrl, compiledStylesheet);
            });
            this._resolveStylesCompileResult(componentStylesheet, externalStylesheetsByModuleUrl);
            var pipes = template.ngModule.transitiveModule.pipes.map(function (pipe) { return _this._metadataResolver.getPipeSummary(pipe.reference); });
            var _a = this._parseTemplate(compMeta, template.ngModule, template.directives), parsedTemplate = _a.template, usedPipes = _a.pipes;
            var compileResult = this._viewCompiler.compileComponent(outputContext, compMeta, parsedTemplate, ir.variable(componentStylesheet.stylesVar), usedPipes);
            var evalResult = this._interpretOrJit(compile_metadata_1.templateJitUrl(template.ngModule.type, template.compMeta), outputContext.statements);
            var viewClass = evalResult[compileResult.viewClassVar];
            var rendererType = evalResult[compileResult.rendererTypeVar];
            template.compiled(viewClass, rendererType);
        };
        JitCompiler.prototype._parseTemplate = function (compMeta, ngModule, directiveIdentifiers) {
            var _this = this;
            // Note: ! is ok here as components always have a template.
            var preserveWhitespaces = compMeta.template.preserveWhitespaces;
            var directives = directiveIdentifiers.map(function (dir) { return _this._metadataResolver.getDirectiveSummary(dir.reference); });
            var pipes = ngModule.transitiveModule.pipes.map(function (pipe) { return _this._metadataResolver.getPipeSummary(pipe.reference); });
            return this._templateParser.parse(compMeta, compMeta.template.htmlAst, directives, pipes, ngModule.schemas, compile_metadata_1.templateSourceUrl(ngModule.type, compMeta, compMeta.template), preserveWhitespaces);
        };
        JitCompiler.prototype._resolveStylesCompileResult = function (result, externalStylesheetsByModuleUrl) {
            var _this = this;
            result.dependencies.forEach(function (dep, i) {
                var nestedCompileResult = externalStylesheetsByModuleUrl.get(dep.moduleUrl);
                var nestedStylesArr = _this._resolveAndEvalStylesCompileResult(nestedCompileResult, externalStylesheetsByModuleUrl);
                dep.setValue(nestedStylesArr);
            });
        };
        JitCompiler.prototype._resolveAndEvalStylesCompileResult = function (result, externalStylesheetsByModuleUrl) {
            this._resolveStylesCompileResult(result, externalStylesheetsByModuleUrl);
            return this._interpretOrJit(compile_metadata_1.sharedStylesheetJitUrl(result.meta, this._sharedStylesheetCount++), result.outputCtx.statements)[result.stylesVar];
        };
        JitCompiler.prototype._interpretOrJit = function (sourceUrl, statements) {
            if (!this._compilerConfig.useJit) {
                return output_interpreter_1.interpretStatements(statements, this._reflector);
            }
            else {
                return this._jitEvaluator.evaluateStatements(sourceUrl, statements, this._reflector, this._compilerConfig.jitDevMode);
            }
        };
        return JitCompiler;
    }());
    exports.JitCompiler = JitCompiler;
    var CompiledTemplate = /** @class */ (function () {
        function CompiledTemplate(isHost, compType, compMeta, ngModule, directives) {
            this.isHost = isHost;
            this.compType = compType;
            this.compMeta = compMeta;
            this.ngModule = ngModule;
            this.directives = directives;
            this._viewClass = null;
            this.isCompiled = false;
        }
        CompiledTemplate.prototype.compiled = function (viewClass, rendererType) {
            this._viewClass = viewClass;
            this.compMeta.componentViewType.setDelegate(viewClass);
            for (var prop in rendererType) {
                this.compMeta.rendererType[prop] = rendererType[prop];
            }
            this.isCompiled = true;
        };
        return CompiledTemplate;
    }());
    function assertComponent(meta) {
        if (!meta.isComponent) {
            throw new Error("Could not compile '" + compile_metadata_1.identifierName(meta.type) + "' because it is not a component.");
        }
    }
    function createOutputContext() {
        var importExpr = function (symbol) {
            return ir.importExpr({ name: compile_metadata_1.identifierName(symbol), moduleName: null, runtime: symbol });
        };
        return { statements: [], genFilePath: '', importExpr: importExpr, constantPool: new constant_pool_1.ConstantPool() };
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaml0L2NvbXBpbGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgsMkVBQWtVO0lBR2xVLHFFQUE4QztJQUk5Qyw0REFBMkM7SUFDM0Msc0ZBQWlFO0lBTWpFLG1EQUFxRTtJQVFyRTs7Ozs7Ozs7T0FRRztJQUNIO1FBUUUscUJBQ1ksaUJBQTBDLEVBQVUsZUFBK0IsRUFDbkYsY0FBNkIsRUFBVSxhQUEyQixFQUNsRSxpQkFBbUMsRUFBVSxnQkFBdUMsRUFDcEYsVUFBNEIsRUFBVSxhQUEyQixFQUNqRSxlQUErQixFQUFVLFFBQWlCLEVBQzFELHlCQUF1RTtZQUx2RSxzQkFBaUIsR0FBakIsaUJBQWlCLENBQXlCO1lBQVUsb0JBQWUsR0FBZixlQUFlLENBQWdCO1lBQ25GLG1CQUFjLEdBQWQsY0FBYyxDQUFlO1lBQVUsa0JBQWEsR0FBYixhQUFhLENBQWM7WUFDbEUsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUFrQjtZQUFVLHFCQUFnQixHQUFoQixnQkFBZ0IsQ0FBdUI7WUFDcEYsZUFBVSxHQUFWLFVBQVUsQ0FBa0I7WUFBVSxrQkFBYSxHQUFiLGFBQWEsQ0FBYztZQUNqRSxvQkFBZSxHQUFmLGVBQWUsQ0FBZ0I7WUFBVSxhQUFRLEdBQVIsUUFBUSxDQUFTO1lBQzFELDhCQUF5QixHQUF6Qix5QkFBeUIsQ0FBOEM7WUFiM0UsMkJBQXNCLEdBQUcsSUFBSSxHQUFHLEVBQTBCLENBQUM7WUFDM0QsK0JBQTBCLEdBQUcsSUFBSSxHQUFHLEVBQTBCLENBQUM7WUFDL0QsbUNBQThCLEdBQUcsSUFBSSxHQUFHLEVBQWMsQ0FBQztZQUN2RCwyQkFBc0IsR0FBRyxJQUFJLEdBQUcsRUFBZ0IsQ0FBQztZQUNqRCwyQkFBc0IsR0FBRyxDQUFDLENBQUM7WUFDM0IsdUJB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WSxDQUFDLElBQUksQ0FBQyxDQUFDO2FBQzlEO1lBQ0QsSUFBSSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUM7UUFDekIsQ0FBQztRQUNILHVCQUFDO0lBQUQsQ0FBQyxBQWpCRCxJQWlCQztJQUVELFNBQVMsZUFBZSxDQUFDLElBQThCO1FBQ3JELElBQUksQ0FBQyxJQUFJLENBQUMsV0FBVyxFQUFFO1lBQ3JCLE1BQU0sSUFBSSxLQUFLLENBQ1gsd0JBQXNCLGlDQUFjLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxxQ0FBa0MsQ0FBQyxDQUFDO1NBQ3hGO0lBQ0gsQ0FBQztJQUVELFNBQVMsbUJBQW1CO1FBQzFCLElBQU0sVUFBVSxHQUFHLFVBQUMsTUFBVztZQUMzQixPQUFBLEVBQUUsQ0FBQyxVQUFVLENBQUMsRUFBQyxJQUFJLEVBQUUsaUNBQWMsQ0FBQyxNQUFNLENBQUMsRUFBRSxVQUFVLEVBQUUsSUFBSSxFQUFFLE9BQU8sRUFBRSxNQUFNLEVBQUMsQ0FBQztRQUFoRixDQUFnRixDQUFDO1FBQ3JGLE9BQU8sRUFBQyxVQUFVLEVBQUUsRUFBRSxFQUFFLFdBQVcsRUFBRSxFQUFFLEVBQUUsVUFBVSxZQUFBLEVBQUUsWUFBWSxFQUFFLElBQUksNEJBQVksRUFBRSxFQUFDLENBQUM7SUFDekYsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsIENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEsIENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhLCBDb21waWxlUGlwZVN1bW1hcnksIENvbXBpbGVQcm92aWRlck1ldGFkYXRhLCBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhLCBDb21waWxlVHlwZVN1bW1hcnksIFByb3ZpZGVyTWV0YSwgUHJveHlDbGFzcywgaWRlbnRpZmllck5hbWUsIG5nTW9kdWxlSml0VXJsLCBzaGFyZWRTdHlsZXNoZWV0Sml0VXJsLCB0ZW1wbGF0ZUppdFVybCwgdGVtcGxhdGVTb3VyY2VVcmx9IGZyb20gJy4uL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuLi9jb21waWxlX3JlZmxlY3Rvcic7XG5pbXBvcnQge0NvbXBpbGVyQ29uZmlnfSBmcm9tICcuLi9jb25maWcnO1xuaW1wb3J0IHtDb25zdGFudFBvb2x9IGZyb20gJy4uL2NvbnN0YW50X3Bvb2wnO1xuaW1wb3J0IHtUeXBlfSBmcm9tICcuLi9jb3JlJztcbmltcG9ydCB7Q29tcGlsZU1ldGFkYXRhUmVzb2x2ZXJ9IGZyb20gJy4uL21ldGFkYXRhX3Jlc29sdmVyJztcbmltcG9ydCB7TmdNb2R1bGVDb21waWxlcn0gZnJvbSAnLi4vbmdfbW9kdWxlX2NvbXBpbGVyJztcbmltcG9ydCAqIGFzIGlyIGZyb20gJy4uL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7aW50ZXJwcmV0U3RhdGVtZW50c30gZnJvbSAnLi4vb3V0cHV0L291dHB1dF9pbnRlcnByZXRlcic7XG5pbXBvcnQge0ppdEV2YWx1YXRvcn0gZnJvbSAnLi4vb3V0cHV0L291dHB1dF9qaXQnO1xuaW1wb3J0IHtDb21waWxlZFN0eWxlc2hlZXQsIFN0eWxlQ29tcGlsZXJ9IGZyb20gJy4uL3N0eWxlX2NvbXBpbGVyJztcbmltcG9ydCB7U3VtbWFyeVJlc29sdmVyfSBmcm9tICcuLi9zdW1tYXJ5X3Jlc29sdmVyJztcbmltcG9ydCB7VGVtcGxhdGVBc3R9IGZyb20gJy4uL3RlbXBsYXRlX3BhcnNlci90ZW1wbGF0ZV9hc3QnO1xuaW1wb3J0IHtUZW1wbGF0ZVBhcnNlcn0gZnJvbSAnLi4vdGVtcGxhdGVfcGFyc2VyL3RlbXBsYXRlX3BhcnNlcic7XG5pbXBvcnQge0NvbnNvbGUsIE91dHB1dENvbnRleHQsIFN5bmNBc3luYywgc3RyaW5naWZ5fSBmcm9tICcuLi91dGlsJztcbmltcG9ydCB7Vmlld0NvbXBpbGVyfSBmcm9tICcuLi92aWV3X2NvbXBpbGVyL3ZpZXdfY29tcGlsZXInO1xuXG5leHBvcnQgaW50ZXJmYWNlIE1vZHVsZVdpdGhDb21wb25lbnRGYWN0b3JpZXMge1xuICBuZ01vZHVsZUZhY3Rvcnk6IG9iamVjdDtcbiAgY29tcG9uZW50RmFjdG9yaWVzOiBvYmplY3RbXTtcbn1cblxuLyoqXG4gKiBBbiBpbnRlcm5hbCBtb2R1bGUgb2YgdGhlIEFuZ3VsYXIgY29tcGlsZXIgdGhhdCBiZWdpbnMgd2l0aCBjb21wb25lbnQgdHlwZXMsXG4gKiBleHRyYWN0cyB0ZW1wbGF0ZXMsIGFuZCBldmVudHVhbGx5IHByb2R1Y2VzIGEgY29tcGlsZWQgdmVyc2lvbiBvZiB0aGUgY29tcG9uZW50XG4gKiByZWFkeSBmb3IgbGlua2luZyBpbnRvIGFuIGFwcGxpY2F0aW9uLlxuICpcbiAqIEBzZWN1cml0eSAgV2hlbiBjb21waWxpbmcgdGVtcGxhdGVzIGF0IHJ1bnRpbWUsIHlvdSBtdXN0IGVuc3VyZSB0aGF0IHRoZSBlbnRpcmUgdGVtcGxhdGUgY29tZXNcbiAqIGZyb20gYSB0cnVzdGVkIHNvdXJjZS4gQXR0YWNrZXItY29udHJvbGxlZCBkYXRhIGludHJvZHVjZWQgYnkgYSB0ZW1wbGF0ZSBjb3VsZCBleHBvc2UgeW91clxuICogYXBwbGljYXRpb24gdG8gWFNTIHJpc2tzLiAgRm9yIG1vcmUgZGV0YWlsLCBzZWUgdGhlIFtTZWN1cml0eSBHdWlkZV0oaHR0cDovL2cuY28vbmcvc2VjdXJpdHkpLlxuICovXG5leHBvcnQgY2xhc3MgSml0Q29tcGlsZXIge1xuICBwcml2YXRlIF9jb21waWxlZFRlbXBsYXRlQ2FjaGUgPSBuZXcgTWFwPFR5cGUsIENvbXBpbGVkVGVtcGxhdGU+KCk7XG4gIHByaXZhdGUgX2NvbXBpbGVkSG9zdFRlbXBsYXRlQ2FjaGUgPSBuZXcgTWFwPFR5cGUsIENvbXBpbGVkVGVtcGxhdGU+KCk7XG4gIHByaXZhdGUgX2NvbXBpbGVkRGlyZWN0aXZlV3JhcHBlckNhY2hlID0gbmV3IE1hcDxUeXBlLCBUeXBlPigpO1xuICBwcml2YXRlIF9jb21waWxlZE5nTW9kdWxlQ2FjaGUgPSBuZXcgTWFwPFR5cGUsIG9iamVjdD4oKTtcbiAgcHJpdmF0ZSBfc2hhcmVkU3R5bGVzaGVldENvdW50ID0gMDtcbiAgcHJpdmF0ZSBfYWRkZWRBb3RTdW1tYXJpZXMgPSBuZXcgU2V0PCgpID0+IGFueVtdPigpO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBfbWV0YWRhdGFSZXNvbHZlcjogQ29tcGlsZU1ldGFkYXRhUmVzb2x2ZXIsIHByaXZhdGUgX3RlbXBsYXRlUGFyc2VyOiBUZW1wbGF0ZVBhcnNlcixcbiAgICAgIHByaXZhdGUgX3N0eWxlQ29tcGlsZXI6IFN0eWxlQ29tcGlsZXIsIHByaXZhdGUgX3ZpZXdDb21waWxlcjogVmlld0NvbXBpbGVyLFxuICAgICAgcHJpdmF0ZSBfbmdNb2R1bGVDb21waWxlcjogTmdNb2R1bGVDb21waWxlciwgcHJpdmF0ZSBfc3VtbWFyeVJlc29sdmVyOiBTdW1tYXJ5UmVzb2x2ZXI8VHlwZT4sXG4gICAgICBwcml2YXRlIF9yZWZsZWN0b3I6IENvbXBpbGVSZWZsZWN0b3IsIHByaXZhdGUgX2ppdEV2YWx1YXRvcjogSml0RXZhbHVhdG9yLFxuICAgICAgcHJpdmF0ZSBfY29tcGlsZXJDb25maWc6IENvbXBpbGVyQ29uZmlnLCBwcml2YXRlIF9jb25zb2xlOiBDb25zb2xlLFxuICAgICAgcHJpdmF0ZSBnZXRFeHRyYU5nTW9kdWxlUHJvdmlkZXJzOiAobmdNb2R1bGU6IGFueSkgPT4gQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSkge31cblxuICBjb21waWxlTW9kdWxlU3luYyhtb2R1bGVUeXBlOiBUeXBlKTogb2JqZWN0IHtcbiAgICByZXR1cm4gU3luY0FzeW5jLmFzc2VydFN5bmModGhpcy5fY29tcGlsZU1vZHVsZUFuZENvbXBvbmVudHMobW9kdWxlVHlwZSwgdHJ1ZSkpO1xuICB9XG5cbiAgY29tcGlsZU1vZHVsZUFzeW5jKG1vZHVsZVR5cGU6IFR5cGUpOiBQcm9taXNlPG9iamVjdD4ge1xuICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUodGhpcy5fY29tcGlsZU1vZHVsZUFuZENvbXBvbmVudHMobW9kdWxlVHlwZSwgZmFsc2UpKTtcbiAgfVxuXG4gIGNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzU3luYyhtb2R1bGVUeXBlOiBUeXBlKTogTW9kdWxlV2l0aENvbXBvbmVudEZhY3RvcmllcyB7XG4gICAgcmV0dXJuIFN5bmNBc3luYy5hc3NlcnRTeW5jKHRoaXMuX2NvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzKG1vZHVsZVR5cGUsIHRydWUpKTtcbiAgfVxuXG4gIGNvbXBpbGVNb2R1bGVBbmRBbGxDb21wb25lbnRzQXN5bmMobW9kdWxlVHlwZTogVHlwZSk6IFByb21pc2U8TW9kdWxlV2l0aENvbXBvbmVudEZhY3Rvcmllcz4ge1xuICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUodGhpcy5fY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHMobW9kdWxlVHlwZSwgZmFsc2UpKTtcbiAgfVxuXG4gIGdldENvbXBvbmVudEZhY3RvcnkoY29tcG9uZW50OiBUeXBlKTogb2JqZWN0IHtcbiAgICBjb25zdCBzdW1tYXJ5ID0gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVTdW1tYXJ5KGNvbXBvbmVudCk7XG4gICAgcmV0dXJuIHN1bW1hcnkuY29tcG9uZW50RmFjdG9yeSBhcyBvYmplY3Q7XG4gIH1cblxuICBsb2FkQW90U3VtbWFyaWVzKHN1bW1hcmllczogKCkgPT4gYW55W10pIHtcbiAgICB0aGlzLmNsZWFyQ2FjaGUoKTtcbiAgICB0aGlzLl9hZGRBb3RTdW1tYXJpZXMoc3VtbWFyaWVzKTtcbiAgfVxuXG4gIHByaXZhdGUgX2FkZEFvdFN1bW1hcmllcyhmbjogKCkgPT4gYW55W10pIHtcbiAgICBpZiAodGhpcy5fYWRkZWRBb3RTdW1tYXJpZXMuaGFzKGZuKSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLl9hZGRlZEFvdFN1bW1hcmllcy5hZGQoZm4pO1xuICAgIGNvbnN0IHN1bW1hcmllcyA9IGZuKCk7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBzdW1tYXJpZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IGVudHJ5ID0gc3VtbWFyaWVzW2ldO1xuICAgICAgaWYgKHR5cGVvZiBlbnRyeSA9PT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICB0aGlzLl9hZGRBb3RTdW1tYXJpZXMoZW50cnkpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc3Qgc3VtbWFyeSA9IGVudHJ5IGFzIENvbXBpbGVUeXBlU3VtbWFyeTtcbiAgICAgICAgdGhpcy5fc3VtbWFyeVJlc29sdmVyLmFkZFN1bW1hcnkoXG4gICAgICAgICAgICB7c3ltYm9sOiBzdW1tYXJ5LnR5cGUucmVmZXJlbmNlLCBtZXRhZGF0YTogbnVsbCwgdHlwZTogc3VtbWFyeX0pO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIGhhc0FvdFN1bW1hcnkocmVmOiBUeXBlKSB7IHJldHVybiAhIXRoaXMuX3N1bW1hcnlSZXNvbHZlci5yZXNvbHZlU3VtbWFyeShyZWYpOyB9XG5cbiAgcHJpdmF0ZSBfZmlsdGVySml0SWRlbnRpZmllcnMoaWRzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10pOiBhbnlbXSB7XG4gICAgcmV0dXJuIGlkcy5tYXAobW9kID0+IG1vZC5yZWZlcmVuY2UpLmZpbHRlcigocmVmKSA9PiAhdGhpcy5oYXNBb3RTdW1tYXJ5KHJlZikpO1xuICB9XG5cbiAgcHJpdmF0ZSBfY29tcGlsZU1vZHVsZUFuZENvbXBvbmVudHMobW9kdWxlVHlwZTogVHlwZSwgaXNTeW5jOiBib29sZWFuKTogU3luY0FzeW5jPG9iamVjdD4ge1xuICAgIHJldHVybiBTeW5jQXN5bmMudGhlbih0aGlzLl9sb2FkTW9kdWxlcyhtb2R1bGVUeXBlLCBpc1N5bmMpLCAoKSA9PiB7XG4gICAgICB0aGlzLl9jb21waWxlQ29tcG9uZW50cyhtb2R1bGVUeXBlLCBudWxsKTtcbiAgICAgIHJldHVybiB0aGlzLl9jb21waWxlTW9kdWxlKG1vZHVsZVR5cGUpO1xuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfY29tcGlsZU1vZHVsZUFuZEFsbENvbXBvbmVudHMobW9kdWxlVHlwZTogVHlwZSwgaXNTeW5jOiBib29sZWFuKTpcbiAgICAgIFN5bmNBc3luYzxNb2R1bGVXaXRoQ29tcG9uZW50RmFjdG9yaWVzPiB7XG4gICAgcmV0dXJuIFN5bmNBc3luYy50aGVuKHRoaXMuX2xvYWRNb2R1bGVzKG1vZHVsZVR5cGUsIGlzU3luYyksICgpID0+IHtcbiAgICAgIGNvbnN0IGNvbXBvbmVudEZhY3Rvcmllczogb2JqZWN0W10gPSBbXTtcbiAgICAgIHRoaXMuX2NvbXBpbGVDb21wb25lbnRzKG1vZHVsZVR5cGUsIGNvbXBvbmVudEZhY3Rvcmllcyk7XG4gICAgICByZXR1cm4ge1xuICAgICAgICBuZ01vZHVsZUZhY3Rvcnk6IHRoaXMuX2NvbXBpbGVNb2R1bGUobW9kdWxlVHlwZSksXG4gICAgICAgIGNvbXBvbmVudEZhY3RvcmllczogY29tcG9uZW50RmFjdG9yaWVzXG4gICAgICB9O1xuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfbG9hZE1vZHVsZXMobWFpbk1vZHVsZTogYW55LCBpc1N5bmM6IGJvb2xlYW4pOiBTeW5jQXN5bmM8YW55PiB7XG4gICAgY29uc3QgbG9hZGluZzogUHJvbWlzZTxhbnk+W10gPSBbXTtcbiAgICBjb25zdCBtYWluTmdNb2R1bGUgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldE5nTW9kdWxlTWV0YWRhdGEobWFpbk1vZHVsZSkgITtcbiAgICAvLyBOb3RlOiBmb3IgcnVudGltZSBjb21waWxhdGlvbiwgd2Ugd2FudCB0byB0cmFuc2l0aXZlbHkgY29tcGlsZSBhbGwgbW9kdWxlcyxcbiAgICAvLyBzbyB3ZSBhbHNvIG5lZWQgdG8gbG9hZCB0aGUgZGVjbGFyZWQgZGlyZWN0aXZlcyAvIHBpcGVzIGZvciBhbGwgbmVzdGVkIG1vZHVsZXMuXG4gICAgdGhpcy5fZmlsdGVySml0SWRlbnRpZmllcnMobWFpbk5nTW9kdWxlLnRyYW5zaXRpdmVNb2R1bGUubW9kdWxlcykuZm9yRWFjaCgobmVzdGVkTmdNb2R1bGUpID0+IHtcbiAgICAgIC8vIGdldE5nTW9kdWxlTWV0YWRhdGEgb25seSByZXR1cm5zIG51bGwgaWYgdGhlIHZhbHVlIHBhc3NlZCBpbiBpcyBub3QgYW4gTmdNb2R1bGVcbiAgICAgIGNvbnN0IG1vZHVsZU1ldGEgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldE5nTW9kdWxlTWV0YWRhdGEobmVzdGVkTmdNb2R1bGUpICE7XG4gICAgICB0aGlzLl9maWx0ZXJKaXRJZGVudGlmaWVycyhtb2R1bGVNZXRhLmRlY2xhcmVkRGlyZWN0aXZlcykuZm9yRWFjaCgocmVmKSA9PiB7XG4gICAgICAgIGNvbnN0IHByb21pc2UgPVxuICAgICAgICAgICAgdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5sb2FkRGlyZWN0aXZlTWV0YWRhdGEobW9kdWxlTWV0YS50eXBlLnJlZmVyZW5jZSwgcmVmLCBpc1N5bmMpO1xuICAgICAgICBpZiAocHJvbWlzZSkge1xuICAgICAgICAgIGxvYWRpbmcucHVzaChwcm9taXNlKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgICB0aGlzLl9maWx0ZXJKaXRJZGVudGlmaWVycyhtb2R1bGVNZXRhLmRlY2xhcmVkUGlwZXMpXG4gICAgICAgICAgLmZvckVhY2goKHJlZikgPT4gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXRPckxvYWRQaXBlTWV0YWRhdGEocmVmKSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIFN5bmNBc3luYy5hbGwobG9hZGluZyk7XG4gIH1cblxuICBwcml2YXRlIF9jb21waWxlTW9kdWxlKG1vZHVsZVR5cGU6IFR5cGUpOiBvYmplY3Qge1xuICAgIGxldCBuZ01vZHVsZUZhY3RvcnkgPSB0aGlzLl9jb21waWxlZE5nTW9kdWxlQ2FjaGUuZ2V0KG1vZHVsZVR5cGUpICE7XG4gICAgaWYgKCFuZ01vZHVsZUZhY3RvcnkpIHtcbiAgICAgIGNvbnN0IG1vZHVsZU1ldGEgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldE5nTW9kdWxlTWV0YWRhdGEobW9kdWxlVHlwZSkgITtcbiAgICAgIC8vIEFsd2F5cyBwcm92aWRlIGEgYm91bmQgQ29tcGlsZXJcbiAgICAgIGNvbnN0IGV4dHJhUHJvdmlkZXJzID0gdGhpcy5nZXRFeHRyYU5nTW9kdWxlUHJvdmlkZXJzKG1vZHVsZU1ldGEudHlwZS5yZWZlcmVuY2UpO1xuICAgICAgY29uc3Qgb3V0cHV0Q3R4ID0gY3JlYXRlT3V0cHV0Q29udGV4dCgpO1xuICAgICAgY29uc3QgY29tcGlsZVJlc3VsdCA9IHRoaXMuX25nTW9kdWxlQ29tcGlsZXIuY29tcGlsZShvdXRwdXRDdHgsIG1vZHVsZU1ldGEsIGV4dHJhUHJvdmlkZXJzKTtcbiAgICAgIG5nTW9kdWxlRmFjdG9yeSA9IHRoaXMuX2ludGVycHJldE9ySml0KFxuICAgICAgICAgIG5nTW9kdWxlSml0VXJsKG1vZHVsZU1ldGEpLCBvdXRwdXRDdHguc3RhdGVtZW50cylbY29tcGlsZVJlc3VsdC5uZ01vZHVsZUZhY3RvcnlWYXJdO1xuICAgICAgdGhpcy5fY29tcGlsZWROZ01vZHVsZUNhY2hlLnNldChtb2R1bGVNZXRhLnR5cGUucmVmZXJlbmNlLCBuZ01vZHVsZUZhY3RvcnkpO1xuICAgIH1cbiAgICByZXR1cm4gbmdNb2R1bGVGYWN0b3J5O1xuICB9XG5cbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgX2NvbXBpbGVDb21wb25lbnRzKG1haW5Nb2R1bGU6IFR5cGUsIGFsbENvbXBvbmVudEZhY3Rvcmllczogb2JqZWN0W118bnVsbCkge1xuICAgIGNvbnN0IG5nTW9kdWxlID0gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXROZ01vZHVsZU1ldGFkYXRhKG1haW5Nb2R1bGUpICE7XG4gICAgY29uc3QgbW9kdWxlQnlKaXREaXJlY3RpdmUgPSBuZXcgTWFwPGFueSwgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGE+KCk7XG4gICAgY29uc3QgdGVtcGxhdGVzID0gbmV3IFNldDxDb21waWxlZFRlbXBsYXRlPigpO1xuXG4gICAgY29uc3QgdHJhbnNKaXRNb2R1bGVzID0gdGhpcy5fZmlsdGVySml0SWRlbnRpZmllcnMobmdNb2R1bGUudHJhbnNpdGl2ZU1vZHVsZS5tb2R1bGVzKTtcbiAgICB0cmFuc0ppdE1vZHVsZXMuZm9yRWFjaCgobG9jYWxNb2QpID0+IHtcbiAgICAgIGNvbnN0IGxvY2FsTW9kdWxlTWV0YSA9IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0TmdNb2R1bGVNZXRhZGF0YShsb2NhbE1vZCkgITtcbiAgICAgIHRoaXMuX2ZpbHRlckppdElkZW50aWZpZXJzKGxvY2FsTW9kdWxlTWV0YS5kZWNsYXJlZERpcmVjdGl2ZXMpLmZvckVhY2goKGRpclJlZikgPT4ge1xuICAgICAgICBtb2R1bGVCeUppdERpcmVjdGl2ZS5zZXQoZGlyUmVmLCBsb2NhbE1vZHVsZU1ldGEpO1xuICAgICAgICBjb25zdCBkaXJNZXRhID0gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVNZXRhZGF0YShkaXJSZWYpO1xuICAgICAgICBpZiAoZGlyTWV0YS5pc0NvbXBvbmVudCkge1xuICAgICAgICAgIHRlbXBsYXRlcy5hZGQodGhpcy5fY3JlYXRlQ29tcGlsZWRUZW1wbGF0ZShkaXJNZXRhLCBsb2NhbE1vZHVsZU1ldGEpKTtcbiAgICAgICAgICBpZiAoYWxsQ29tcG9uZW50RmFjdG9yaWVzKSB7XG4gICAgICAgICAgICBjb25zdCB0ZW1wbGF0ZSA9XG4gICAgICAgICAgICAgICAgdGhpcy5fY3JlYXRlQ29tcGlsZWRIb3N0VGVtcGxhdGUoZGlyTWV0YS50eXBlLnJlZmVyZW5jZSwgbG9jYWxNb2R1bGVNZXRhKTtcbiAgICAgICAgICAgIHRlbXBsYXRlcy5hZGQodGVtcGxhdGUpO1xuICAgICAgICAgICAgYWxsQ29tcG9uZW50RmFjdG9yaWVzLnB1c2goZGlyTWV0YS5jb21wb25lbnRGYWN0b3J5IGFzIG9iamVjdCk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9KTtcbiAgICB0cmFuc0ppdE1vZHVsZXMuZm9yRWFjaCgobG9jYWxNb2QpID0+IHtcbiAgICAgIGNvbnN0IGxvY2FsTW9kdWxlTWV0YSA9IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0TmdNb2R1bGVNZXRhZGF0YShsb2NhbE1vZCkgITtcbiAgICAgIHRoaXMuX2ZpbHRlckppdElkZW50aWZpZXJzKGxvY2FsTW9kdWxlTWV0YS5kZWNsYXJlZERpcmVjdGl2ZXMpLmZvckVhY2goKGRpclJlZikgPT4ge1xuICAgICAgICBjb25zdCBkaXJNZXRhID0gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVNZXRhZGF0YShkaXJSZWYpO1xuICAgICAgICBpZiAoZGlyTWV0YS5pc0NvbXBvbmVudCkge1xuICAgICAgICAgIGRpck1ldGEuZW50cnlDb21wb25lbnRzLmZvckVhY2goKGVudHJ5Q29tcG9uZW50VHlwZSkgPT4ge1xuICAgICAgICAgICAgY29uc3QgbW9kdWxlTWV0YSA9IG1vZHVsZUJ5Sml0RGlyZWN0aXZlLmdldChlbnRyeUNvbXBvbmVudFR5cGUuY29tcG9uZW50VHlwZSkgITtcbiAgICAgICAgICAgIHRlbXBsYXRlcy5hZGQoXG4gICAgICAgICAgICAgICAgdGhpcy5fY3JlYXRlQ29tcGlsZWRIb3N0VGVtcGxhdGUoZW50cnlDb21wb25lbnRUeXBlLmNvbXBvbmVudFR5cGUsIG1vZHVsZU1ldGEpKTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgICBsb2NhbE1vZHVsZU1ldGEuZW50cnlDb21wb25lbnRzLmZvckVhY2goKGVudHJ5Q29tcG9uZW50VHlwZSkgPT4ge1xuICAgICAgICBpZiAoIXRoaXMuaGFzQW90U3VtbWFyeShlbnRyeUNvbXBvbmVudFR5cGUuY29tcG9uZW50VHlwZSkpIHtcbiAgICAgICAgICBjb25zdCBtb2R1bGVNZXRhID0gbW9kdWxlQnlKaXREaXJlY3RpdmUuZ2V0KGVudHJ5Q29tcG9uZW50VHlwZS5jb21wb25lbnRUeXBlKSAhO1xuICAgICAgICAgIHRlbXBsYXRlcy5hZGQoXG4gICAgICAgICAgICAgIHRoaXMuX2NyZWF0ZUNvbXBpbGVkSG9zdFRlbXBsYXRlKGVudHJ5Q29tcG9uZW50VHlwZS5jb21wb25lbnRUeXBlLCBtb2R1bGVNZXRhKSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH0pO1xuICAgIHRlbXBsYXRlcy5mb3JFYWNoKCh0ZW1wbGF0ZSkgPT4gdGhpcy5fY29tcGlsZVRlbXBsYXRlKHRlbXBsYXRlKSk7XG4gIH1cblxuICBjbGVhckNhY2hlRm9yKHR5cGU6IFR5cGUpIHtcbiAgICB0aGlzLl9jb21waWxlZE5nTW9kdWxlQ2FjaGUuZGVsZXRlKHR5cGUpO1xuICAgIHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuY2xlYXJDYWNoZUZvcih0eXBlKTtcbiAgICB0aGlzLl9jb21waWxlZEhvc3RUZW1wbGF0ZUNhY2hlLmRlbGV0ZSh0eXBlKTtcbiAgICBjb25zdCBjb21waWxlZFRlbXBsYXRlID0gdGhpcy5fY29tcGlsZWRUZW1wbGF0ZUNhY2hlLmdldCh0eXBlKTtcbiAgICBpZiAoY29tcGlsZWRUZW1wbGF0ZSkge1xuICAgICAgdGhpcy5fY29tcGlsZWRUZW1wbGF0ZUNhY2hlLmRlbGV0ZSh0eXBlKTtcbiAgICB9XG4gIH1cblxuICBjbGVhckNhY2hlKCk6IHZvaWQge1xuICAgIC8vIE5vdGU6IGRvbid0IGNsZWFyIHRoZSBfYWRkZWRBb3RTdW1tYXJpZXMsIGFzIHRoZXkgZG9uJ3QgY2hhbmdlIVxuICAgIHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuY2xlYXJDYWNoZSgpO1xuICAgIHRoaXMuX2NvbXBpbGVkVGVtcGxhdGVDYWNoZS5jbGVhcigpO1xuICAgIHRoaXMuX2NvbXBpbGVkSG9zdFRlbXBsYXRlQ2FjaGUuY2xlYXIoKTtcbiAgICB0aGlzLl9jb21waWxlZE5nTW9kdWxlQ2FjaGUuY2xlYXIoKTtcbiAgfVxuXG4gIHByaXZhdGUgX2NyZWF0ZUNvbXBpbGVkSG9zdFRlbXBsYXRlKGNvbXBUeXBlOiBUeXBlLCBuZ01vZHVsZTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEpOlxuICAgICAgQ29tcGlsZWRUZW1wbGF0ZSB7XG4gICAgaWYgKCFuZ01vZHVsZSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBDb21wb25lbnQgJHtzdHJpbmdpZnkoY29tcFR5cGUpfSBpcyBub3QgcGFydCBvZiBhbnkgTmdNb2R1bGUgb3IgdGhlIG1vZHVsZSBoYXMgbm90IGJlZW4gaW1wb3J0ZWQgaW50byB5b3VyIG1vZHVsZS5gKTtcbiAgICB9XG4gICAgbGV0IGNvbXBpbGVkVGVtcGxhdGUgPSB0aGlzLl9jb21waWxlZEhvc3RUZW1wbGF0ZUNhY2hlLmdldChjb21wVHlwZSk7XG4gICAgaWYgKCFjb21waWxlZFRlbXBsYXRlKSB7XG4gICAgICBjb25zdCBjb21wTWV0YSA9IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0RGlyZWN0aXZlTWV0YWRhdGEoY29tcFR5cGUpO1xuICAgICAgYXNzZXJ0Q29tcG9uZW50KGNvbXBNZXRhKTtcblxuICAgICAgY29uc3QgaG9zdE1ldGEgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldEhvc3RDb21wb25lbnRNZXRhZGF0YShcbiAgICAgICAgICBjb21wTWV0YSwgKGNvbXBNZXRhLmNvbXBvbmVudEZhY3RvcnkgYXMgYW55KS52aWV3RGVmRmFjdG9yeSk7XG4gICAgICBjb21waWxlZFRlbXBsYXRlID1cbiAgICAgICAgICBuZXcgQ29tcGlsZWRUZW1wbGF0ZSh0cnVlLCBjb21wTWV0YS50eXBlLCBob3N0TWV0YSwgbmdNb2R1bGUsIFtjb21wTWV0YS50eXBlXSk7XG4gICAgICB0aGlzLl9jb21waWxlZEhvc3RUZW1wbGF0ZUNhY2hlLnNldChjb21wVHlwZSwgY29tcGlsZWRUZW1wbGF0ZSk7XG4gICAgfVxuICAgIHJldHVybiBjb21waWxlZFRlbXBsYXRlO1xuICB9XG5cbiAgcHJpdmF0ZSBfY3JlYXRlQ29tcGlsZWRUZW1wbGF0ZShcbiAgICAgIGNvbXBNZXRhOiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsIG5nTW9kdWxlOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSk6IENvbXBpbGVkVGVtcGxhdGUge1xuICAgIGxldCBjb21waWxlZFRlbXBsYXRlID0gdGhpcy5fY29tcGlsZWRUZW1wbGF0ZUNhY2hlLmdldChjb21wTWV0YS50eXBlLnJlZmVyZW5jZSk7XG4gICAgaWYgKCFjb21waWxlZFRlbXBsYXRlKSB7XG4gICAgICBhc3NlcnRDb21wb25lbnQoY29tcE1ldGEpO1xuICAgICAgY29tcGlsZWRUZW1wbGF0ZSA9IG5ldyBDb21waWxlZFRlbXBsYXRlKFxuICAgICAgICAgIGZhbHNlLCBjb21wTWV0YS50eXBlLCBjb21wTWV0YSwgbmdNb2R1bGUsIG5nTW9kdWxlLnRyYW5zaXRpdmVNb2R1bGUuZGlyZWN0aXZlcyk7XG4gICAgICB0aGlzLl9jb21waWxlZFRlbXBsYXRlQ2FjaGUuc2V0KGNvbXBNZXRhLnR5cGUucmVmZXJlbmNlLCBjb21waWxlZFRlbXBsYXRlKTtcbiAgICB9XG4gICAgcmV0dXJuIGNvbXBpbGVkVGVtcGxhdGU7XG4gIH1cblxuICBwcml2YXRlIF9jb21waWxlVGVtcGxhdGUodGVtcGxhdGU6IENvbXBpbGVkVGVtcGxhdGUpIHtcbiAgICBpZiAodGVtcGxhdGUuaXNDb21waWxlZCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCBjb21wTWV0YSA9IHRlbXBsYXRlLmNvbXBNZXRhO1xuICAgIGNvbnN0IGV4dGVybmFsU3R5bGVzaGVldHNCeU1vZHVsZVVybCA9IG5ldyBNYXA8c3RyaW5nLCBDb21waWxlZFN0eWxlc2hlZXQ+KCk7XG4gICAgY29uc3Qgb3V0cHV0Q29udGV4dCA9IGNyZWF0ZU91dHB1dENvbnRleHQoKTtcbiAgICBjb25zdCBjb21wb25lbnRTdHlsZXNoZWV0ID0gdGhpcy5fc3R5bGVDb21waWxlci5jb21waWxlQ29tcG9uZW50KG91dHB1dENvbnRleHQsIGNvbXBNZXRhKTtcbiAgICBjb21wTWV0YS50ZW1wbGF0ZSAhLmV4dGVybmFsU3R5bGVzaGVldHMuZm9yRWFjaCgoc3R5bGVzaGVldE1ldGEpID0+IHtcbiAgICAgIGNvbnN0IGNvbXBpbGVkU3R5bGVzaGVldCA9XG4gICAgICAgICAgdGhpcy5fc3R5bGVDb21waWxlci5jb21waWxlU3R5bGVzKGNyZWF0ZU91dHB1dENvbnRleHQoKSwgY29tcE1ldGEsIHN0eWxlc2hlZXRNZXRhKTtcbiAgICAgIGV4dGVybmFsU3R5bGVzaGVldHNCeU1vZHVsZVVybC5zZXQoc3R5bGVzaGVldE1ldGEubW9kdWxlVXJsICEsIGNvbXBpbGVkU3R5bGVzaGVldCk7XG4gICAgfSk7XG4gICAgdGhpcy5fcmVzb2x2ZVN0eWxlc0NvbXBpbGVSZXN1bHQoY29tcG9uZW50U3R5bGVzaGVldCwgZXh0ZXJuYWxTdHlsZXNoZWV0c0J5TW9kdWxlVXJsKTtcbiAgICBjb25zdCBwaXBlcyA9IHRlbXBsYXRlLm5nTW9kdWxlLnRyYW5zaXRpdmVNb2R1bGUucGlwZXMubWFwKFxuICAgICAgICBwaXBlID0+IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0UGlwZVN1bW1hcnkocGlwZS5yZWZlcmVuY2UpKTtcbiAgICBjb25zdCB7dGVtcGxhdGU6IHBhcnNlZFRlbXBsYXRlLCBwaXBlczogdXNlZFBpcGVzfSA9XG4gICAgICAgIHRoaXMuX3BhcnNlVGVtcGxhdGUoY29tcE1ldGEsIHRlbXBsYXRlLm5nTW9kdWxlLCB0ZW1wbGF0ZS5kaXJlY3RpdmVzKTtcbiAgICBjb25zdCBjb21waWxlUmVzdWx0ID0gdGhpcy5fdmlld0NvbXBpbGVyLmNvbXBpbGVDb21wb25lbnQoXG4gICAgICAgIG91dHB1dENvbnRleHQsIGNvbXBNZXRhLCBwYXJzZWRUZW1wbGF0ZSwgaXIudmFyaWFibGUoY29tcG9uZW50U3R5bGVzaGVldC5zdHlsZXNWYXIpLFxuICAgICAgICB1c2VkUGlwZXMpO1xuICAgIGNvbnN0IGV2YWxSZXN1bHQgPSB0aGlzLl9pbnRlcnByZXRPckppdChcbiAgICAgICAgdGVtcGxhdGVKaXRVcmwodGVtcGxhdGUubmdNb2R1bGUudHlwZSwgdGVtcGxhdGUuY29tcE1ldGEpLCBvdXRwdXRDb250ZXh0LnN0YXRlbWVudHMpO1xuICAgIGNvbnN0IHZpZXdDbGFzcyA9IGV2YWxSZXN1bHRbY29tcGlsZVJlc3VsdC52aWV3Q2xhc3NWYXJdO1xuICAgIGNvbnN0IHJlbmRlcmVyVHlwZSA9IGV2YWxSZXN1bHRbY29tcGlsZVJlc3VsdC5yZW5kZXJlclR5cGVWYXJdO1xuICAgIHRlbXBsYXRlLmNvbXBpbGVkKHZpZXdDbGFzcywgcmVuZGVyZXJUeXBlKTtcbiAgfVxuXG4gIHByaXZhdGUgX3BhcnNlVGVtcGxhdGUoXG4gICAgICBjb21wTWV0YTogQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhLCBuZ01vZHVsZTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsXG4gICAgICBkaXJlY3RpdmVJZGVudGlmaWVyczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdKTpcbiAgICAgIHt0ZW1wbGF0ZTogVGVtcGxhdGVBc3RbXSwgcGlwZXM6IENvbXBpbGVQaXBlU3VtbWFyeVtdfSB7XG4gICAgLy8gTm90ZTogISBpcyBvayBoZXJlIGFzIGNvbXBvbmVudHMgYWx3YXlzIGhhdmUgYSB0ZW1wbGF0ZS5cbiAgICBjb25zdCBwcmVzZXJ2ZVdoaXRlc3BhY2VzID0gY29tcE1ldGEudGVtcGxhdGUgIS5wcmVzZXJ2ZVdoaXRlc3BhY2VzO1xuICAgIGNvbnN0IGRpcmVjdGl2ZXMgPVxuICAgICAgICBkaXJlY3RpdmVJZGVudGlmaWVycy5tYXAoZGlyID0+IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0RGlyZWN0aXZlU3VtbWFyeShkaXIucmVmZXJlbmNlKSk7XG4gICAgY29uc3QgcGlwZXMgPSBuZ01vZHVsZS50cmFuc2l0aXZlTW9kdWxlLnBpcGVzLm1hcChcbiAgICAgICAgcGlwZSA9PiB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldFBpcGVTdW1tYXJ5KHBpcGUucmVmZXJlbmNlKSk7XG4gICAgcmV0dXJuIHRoaXMuX3RlbXBsYXRlUGFyc2VyLnBhcnNlKFxuICAgICAgICBjb21wTWV0YSwgY29tcE1ldGEudGVtcGxhdGUgIS5odG1sQXN0ICEsIGRpcmVjdGl2ZXMsIHBpcGVzLCBuZ01vZHVsZS5zY2hlbWFzLFxuICAgICAgICB0ZW1wbGF0ZVNvdXJjZVVybChuZ01vZHVsZS50eXBlLCBjb21wTWV0YSwgY29tcE1ldGEudGVtcGxhdGUgISksIHByZXNlcnZlV2hpdGVzcGFjZXMpO1xuICB9XG5cbiAgcHJpdmF0ZSBfcmVzb2x2ZVN0eWxlc0NvbXBpbGVSZXN1bHQoXG4gICAgICByZXN1bHQ6IENvbXBpbGVkU3R5bGVzaGVldCwgZXh0ZXJuYWxTdHlsZXNoZWV0c0J5TW9kdWxlVXJsOiBNYXA8c3RyaW5nLCBDb21waWxlZFN0eWxlc2hlZXQ+KSB7XG4gICAgcmVzdWx0LmRlcGVuZGVuY2llcy5mb3JFYWNoKChkZXAsIGkpID0+IHtcbiAgICAgIGNvbnN0IG5lc3RlZENvbXBpbGVSZXN1bHQgPSBleHRlcm5hbFN0eWxlc2hlZXRzQnlNb2R1bGVVcmwuZ2V0KGRlcC5tb2R1bGVVcmwpICE7XG4gICAgICBjb25zdCBuZXN0ZWRTdHlsZXNBcnIgPSB0aGlzLl9yZXNvbHZlQW5kRXZhbFN0eWxlc0NvbXBpbGVSZXN1bHQoXG4gICAgICAgICAgbmVzdGVkQ29tcGlsZVJlc3VsdCwgZXh0ZXJuYWxTdHlsZXNoZWV0c0J5TW9kdWxlVXJsKTtcbiAgICAgIGRlcC5zZXRWYWx1ZShuZXN0ZWRTdHlsZXNBcnIpO1xuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfcmVzb2x2ZUFuZEV2YWxTdHlsZXNDb21waWxlUmVzdWx0KFxuICAgICAgcmVzdWx0OiBDb21waWxlZFN0eWxlc2hlZXQsXG4gICAgICBleHRlcm5hbFN0eWxlc2hlZXRzQnlNb2R1bGVVcmw6IE1hcDxzdHJpbmcsIENvbXBpbGVkU3R5bGVzaGVldD4pOiBzdHJpbmdbXSB7XG4gICAgdGhpcy5fcmVzb2x2ZVN0eWxlc0NvbXBpbGVSZXN1bHQocmVzdWx0LCBleHRlcm5hbFN0eWxlc2hlZXRzQnlNb2R1bGVVcmwpO1xuICAgIHJldHVybiB0aGlzLl9pbnRlcnByZXRPckppdChcbiAgICAgICAgc2hhcmVkU3R5bGVzaGVldEppdFVybChyZXN1bHQubWV0YSwgdGhpcy5fc2hhcmVkU3R5bGVzaGVldENvdW50KyspLFxuICAgICAgICByZXN1bHQub3V0cHV0Q3R4LnN0YXRlbWVudHMpW3Jlc3VsdC5zdHlsZXNWYXJdO1xuICB9XG5cbiAgcHJpdmF0ZSBfaW50ZXJwcmV0T3JKaXQoc291cmNlVXJsOiBzdHJpbmcsIHN0YXRlbWVudHM6IGlyLlN0YXRlbWVudFtdKTogYW55IHtcbiAgICBpZiAoIXRoaXMuX2NvbXBpbGVyQ29uZmlnLnVzZUppdCkge1xuICAgICAgcmV0dXJuIGludGVycHJldFN0YXRlbWVudHMoc3RhdGVtZW50cywgdGhpcy5fcmVmbGVjdG9yKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMuX2ppdEV2YWx1YXRvci5ldmFsdWF0ZVN0YXRlbWVudHMoXG4gICAgICAgICAgc291cmNlVXJsLCBzdGF0ZW1lbnRzLCB0aGlzLl9yZWZsZWN0b3IsIHRoaXMuX2NvbXBpbGVyQ29uZmlnLmppdERldk1vZGUpO1xuICAgIH1cbiAgfVxufVxuXG5jbGFzcyBDb21waWxlZFRlbXBsYXRlIHtcbiAgcHJpdmF0ZSBfdmlld0NsYXNzOiBGdW5jdGlvbiA9IG51bGwgITtcbiAgaXNDb21waWxlZCA9IGZhbHNlO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIGlzSG9zdDogYm9vbGVhbiwgcHVibGljIGNvbXBUeXBlOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhLFxuICAgICAgcHVibGljIGNvbXBNZXRhOiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsIHB1YmxpYyBuZ01vZHVsZTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsXG4gICAgICBwdWJsaWMgZGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdKSB7fVxuXG4gIGNvbXBpbGVkKHZpZXdDbGFzczogRnVuY3Rpb24sIHJlbmRlcmVyVHlwZTogYW55KSB7XG4gICAgdGhpcy5fdmlld0NsYXNzID0gdmlld0NsYXNzO1xuICAgICg8UHJveHlDbGFzcz50aGlzLmNvbXBNZXRhLmNvbXBvbmVudFZpZXdUeXBlKS5zZXREZWxlZ2F0ZSh2aWV3Q2xhc3MpO1xuICAgIGZvciAobGV0IHByb3AgaW4gcmVuZGVyZXJUeXBlKSB7XG4gICAgICAoPGFueT50aGlzLmNvbXBNZXRhLnJlbmRlcmVyVHlwZSlbcHJvcF0gPSByZW5kZXJlclR5cGVbcHJvcF07XG4gICAgfVxuICAgIHRoaXMuaXNDb21waWxlZCA9IHRydWU7XG4gIH1cbn1cblxuZnVuY3Rpb24gYXNzZXJ0Q29tcG9uZW50KG1ldGE6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSkge1xuICBpZiAoIW1ldGEuaXNDb21wb25lbnQpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIGBDb3VsZCBub3QgY29tcGlsZSAnJHtpZGVudGlmaWVyTmFtZShtZXRhLnR5cGUpfScgYmVjYXVzZSBpdCBpcyBub3QgYSBjb21wb25lbnQuYCk7XG4gIH1cbn1cblxuZnVuY3Rpb24gY3JlYXRlT3V0cHV0Q29udGV4dCgpOiBPdXRwdXRDb250ZXh0IHtcbiAgY29uc3QgaW1wb3J0RXhwciA9IChzeW1ib2w6IGFueSkgPT5cbiAgICAgIGlyLmltcG9ydEV4cHIoe25hbWU6IGlkZW50aWZpZXJOYW1lKHN5bWJvbCksIG1vZHVsZU5hbWU6IG51bGwsIHJ1bnRpbWU6IHN5bWJvbH0pO1xuICByZXR1cm4ge3N0YXRlbWVudHM6IFtdLCBnZW5GaWxlUGF0aDogJycsIGltcG9ydEV4cHIsIGNvbnN0YW50UG9vbDogbmV3IENvbnN0YW50UG9vbCgpfTtcbn1cbiJdfQ==