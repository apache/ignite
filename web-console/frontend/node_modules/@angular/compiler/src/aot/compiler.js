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
        define("@angular/compiler/src/aot/compiler", ["require", "exports", "tslib", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/constant_pool", "@angular/compiler/src/core", "@angular/compiler/src/i18n/message_bundle", "@angular/compiler/src/identifiers", "@angular/compiler/src/ml_parser/html_parser", "@angular/compiler/src/ml_parser/html_whitespaces", "@angular/compiler/src/ml_parser/interpolation_config", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/render3/r3_module_compiler", "@angular/compiler/src/render3/r3_pipe_compiler", "@angular/compiler/src/render3/r3_template_transform", "@angular/compiler/src/render3/view/compiler", "@angular/compiler/src/schema/dom_element_schema_registry", "@angular/compiler/src/template_parser/binding_parser", "@angular/compiler/src/util", "@angular/compiler/src/aot/generated_file", "@angular/compiler/src/aot/lazy_routes", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/aot/summary_serializer", "@angular/compiler/src/aot/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var constant_pool_1 = require("@angular/compiler/src/constant_pool");
    var core_1 = require("@angular/compiler/src/core");
    var message_bundle_1 = require("@angular/compiler/src/i18n/message_bundle");
    var identifiers_1 = require("@angular/compiler/src/identifiers");
    var html_parser_1 = require("@angular/compiler/src/ml_parser/html_parser");
    var html_whitespaces_1 = require("@angular/compiler/src/ml_parser/html_whitespaces");
    var interpolation_config_1 = require("@angular/compiler/src/ml_parser/interpolation_config");
    var o = require("@angular/compiler/src/output/output_ast");
    var r3_module_compiler_1 = require("@angular/compiler/src/render3/r3_module_compiler");
    var r3_pipe_compiler_1 = require("@angular/compiler/src/render3/r3_pipe_compiler");
    var r3_template_transform_1 = require("@angular/compiler/src/render3/r3_template_transform");
    var compiler_1 = require("@angular/compiler/src/render3/view/compiler");
    var dom_element_schema_registry_1 = require("@angular/compiler/src/schema/dom_element_schema_registry");
    var binding_parser_1 = require("@angular/compiler/src/template_parser/binding_parser");
    var util_1 = require("@angular/compiler/src/util");
    var generated_file_1 = require("@angular/compiler/src/aot/generated_file");
    var lazy_routes_1 = require("@angular/compiler/src/aot/lazy_routes");
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var summary_serializer_1 = require("@angular/compiler/src/aot/summary_serializer");
    var util_2 = require("@angular/compiler/src/aot/util");
    var AotCompiler = /** @class */ (function () {
        function AotCompiler(_config, _options, _host, reflector, _metadataResolver, _templateParser, _styleCompiler, _viewCompiler, _typeCheckCompiler, _ngModuleCompiler, _injectableCompiler, _outputEmitter, _summaryResolver, _symbolResolver) {
            this._config = _config;
            this._options = _options;
            this._host = _host;
            this.reflector = reflector;
            this._metadataResolver = _metadataResolver;
            this._templateParser = _templateParser;
            this._styleCompiler = _styleCompiler;
            this._viewCompiler = _viewCompiler;
            this._typeCheckCompiler = _typeCheckCompiler;
            this._ngModuleCompiler = _ngModuleCompiler;
            this._injectableCompiler = _injectableCompiler;
            this._outputEmitter = _outputEmitter;
            this._summaryResolver = _summaryResolver;
            this._symbolResolver = _symbolResolver;
            this._templateAstCache = new Map();
            this._analyzedFiles = new Map();
            this._analyzedFilesForInjectables = new Map();
        }
        AotCompiler.prototype.clearCache = function () { this._metadataResolver.clearCache(); };
        AotCompiler.prototype.analyzeModulesSync = function (rootFiles) {
            var _this = this;
            var analyzeResult = analyzeAndValidateNgModules(rootFiles, this._host, this._symbolResolver, this._metadataResolver);
            analyzeResult.ngModules.forEach(function (ngModule) { return _this._metadataResolver.loadNgModuleDirectiveAndPipeMetadata(ngModule.type.reference, true); });
            return analyzeResult;
        };
        AotCompiler.prototype.analyzeModulesAsync = function (rootFiles) {
            var _this = this;
            var analyzeResult = analyzeAndValidateNgModules(rootFiles, this._host, this._symbolResolver, this._metadataResolver);
            return Promise
                .all(analyzeResult.ngModules.map(function (ngModule) { return _this._metadataResolver.loadNgModuleDirectiveAndPipeMetadata(ngModule.type.reference, false); }))
                .then(function () { return analyzeResult; });
        };
        AotCompiler.prototype._analyzeFile = function (fileName) {
            var analyzedFile = this._analyzedFiles.get(fileName);
            if (!analyzedFile) {
                analyzedFile =
                    analyzeFile(this._host, this._symbolResolver, this._metadataResolver, fileName);
                this._analyzedFiles.set(fileName, analyzedFile);
            }
            return analyzedFile;
        };
        AotCompiler.prototype._analyzeFileForInjectables = function (fileName) {
            var analyzedFile = this._analyzedFilesForInjectables.get(fileName);
            if (!analyzedFile) {
                analyzedFile = analyzeFileForInjectables(this._host, this._symbolResolver, this._metadataResolver, fileName);
                this._analyzedFilesForInjectables.set(fileName, analyzedFile);
            }
            return analyzedFile;
        };
        AotCompiler.prototype.findGeneratedFileNames = function (fileName) {
            var _this = this;
            var genFileNames = [];
            var file = this._analyzeFile(fileName);
            // Make sure we create a .ngfactory if we have a injectable/directive/pipe/NgModule
            // or a reference to a non source file.
            // Note: This is overestimating the required .ngfactory files as the real calculation is harder.
            // Only do this for StubEmitFlags.Basic, as adding a type check block
            // does not change this file (as we generate type check blocks based on NgModules).
            if (this._options.allowEmptyCodegenFiles || file.directives.length || file.pipes.length ||
                file.injectables.length || file.ngModules.length || file.exportsNonSourceFiles) {
                genFileNames.push(util_2.ngfactoryFilePath(file.fileName, true));
                if (this._options.enableSummariesForJit) {
                    genFileNames.push(util_2.summaryForJitFileName(file.fileName, true));
                }
            }
            var fileSuffix = util_2.normalizeGenFileSuffix(util_2.splitTypescriptSuffix(file.fileName, true)[1]);
            file.directives.forEach(function (dirSymbol) {
                var compMeta = _this._metadataResolver.getNonNormalizedDirectiveMetadata(dirSymbol).metadata;
                if (!compMeta.isComponent) {
                    return;
                }
                // Note: compMeta is a component and therefore template is non null.
                compMeta.template.styleUrls.forEach(function (styleUrl) {
                    var normalizedUrl = _this._host.resourceNameToFileName(styleUrl, file.fileName);
                    if (!normalizedUrl) {
                        throw util_1.syntaxError("Couldn't resolve resource " + styleUrl + " relative to " + file.fileName);
                    }
                    var needsShim = (compMeta.template.encapsulation ||
                        _this._config.defaultEncapsulation) === core_1.ViewEncapsulation.Emulated;
                    genFileNames.push(_stylesModuleUrl(normalizedUrl, needsShim, fileSuffix));
                    if (_this._options.allowEmptyCodegenFiles) {
                        genFileNames.push(_stylesModuleUrl(normalizedUrl, !needsShim, fileSuffix));
                    }
                });
            });
            return genFileNames;
        };
        AotCompiler.prototype.emitBasicStub = function (genFileName, originalFileName) {
            var outputCtx = this._createOutputContext(genFileName);
            if (genFileName.endsWith('.ngfactory.ts')) {
                if (!originalFileName) {
                    throw new Error("Assertion error: require the original file for .ngfactory.ts stubs. File: " + genFileName);
                }
                var originalFile = this._analyzeFile(originalFileName);
                this._createNgFactoryStub(outputCtx, originalFile, 1 /* Basic */);
            }
            else if (genFileName.endsWith('.ngsummary.ts')) {
                if (this._options.enableSummariesForJit) {
                    if (!originalFileName) {
                        throw new Error("Assertion error: require the original file for .ngsummary.ts stubs. File: " + genFileName);
                    }
                    var originalFile = this._analyzeFile(originalFileName);
                    _createEmptyStub(outputCtx);
                    originalFile.ngModules.forEach(function (ngModule) {
                        // create exports that user code can reference
                        summary_serializer_1.createForJitStub(outputCtx, ngModule.type.reference);
                    });
                }
            }
            else if (genFileName.endsWith('.ngstyle.ts')) {
                _createEmptyStub(outputCtx);
            }
            // Note: for the stubs, we don't need a property srcFileUrl,
            // as later on in emitAllImpls we will create the proper GeneratedFiles with the
            // correct srcFileUrl.
            // This is good as e.g. for .ngstyle.ts files we can't derive
            // the url of components based on the genFileUrl.
            return this._codegenSourceModule('unknown', outputCtx);
        };
        AotCompiler.prototype.emitTypeCheckStub = function (genFileName, originalFileName) {
            var originalFile = this._analyzeFile(originalFileName);
            var outputCtx = this._createOutputContext(genFileName);
            if (genFileName.endsWith('.ngfactory.ts')) {
                this._createNgFactoryStub(outputCtx, originalFile, 2 /* TypeCheck */);
            }
            return outputCtx.statements.length > 0 ?
                this._codegenSourceModule(originalFile.fileName, outputCtx) :
                null;
        };
        AotCompiler.prototype.loadFilesAsync = function (fileNames, tsFiles) {
            var _this = this;
            var files = fileNames.map(function (fileName) { return _this._analyzeFile(fileName); });
            var loadingPromises = [];
            files.forEach(function (file) { return file.ngModules.forEach(function (ngModule) {
                return loadingPromises.push(_this._metadataResolver.loadNgModuleDirectiveAndPipeMetadata(ngModule.type.reference, false));
            }); });
            var analyzedInjectables = tsFiles.map(function (tsFile) { return _this._analyzeFileForInjectables(tsFile); });
            return Promise.all(loadingPromises).then(function (_) { return ({
                analyzedModules: mergeAndValidateNgFiles(files),
                analyzedInjectables: analyzedInjectables,
            }); });
        };
        AotCompiler.prototype.loadFilesSync = function (fileNames, tsFiles) {
            var _this = this;
            var files = fileNames.map(function (fileName) { return _this._analyzeFile(fileName); });
            files.forEach(function (file) { return file.ngModules.forEach(function (ngModule) { return _this._metadataResolver.loadNgModuleDirectiveAndPipeMetadata(ngModule.type.reference, true); }); });
            var analyzedInjectables = tsFiles.map(function (tsFile) { return _this._analyzeFileForInjectables(tsFile); });
            return {
                analyzedModules: mergeAndValidateNgFiles(files),
                analyzedInjectables: analyzedInjectables,
            };
        };
        AotCompiler.prototype._createNgFactoryStub = function (outputCtx, file, emitFlags) {
            var _this = this;
            var componentId = 0;
            file.ngModules.forEach(function (ngModuleMeta, ngModuleIndex) {
                // Note: the code below needs to executed for StubEmitFlags.Basic and StubEmitFlags.TypeCheck,
                // so we don't change the .ngfactory file too much when adding the type-check block.
                // create exports that user code can reference
                _this._ngModuleCompiler.createStub(outputCtx, ngModuleMeta.type.reference);
                // add references to the symbols from the metadata.
                // These can be used by the type check block for components,
                // and they also cause TypeScript to include these files into the program too,
                // which will make them part of the analyzedFiles.
                var externalReferences = tslib_1.__spread(ngModuleMeta.transitiveModule.directives.map(function (d) { return d.reference; }), ngModuleMeta.transitiveModule.pipes.map(function (d) { return d.reference; }), ngModuleMeta.importedModules.map(function (m) { return m.type.reference; }), ngModuleMeta.exportedModules.map(function (m) { return m.type.reference; }), _this._externalIdentifierReferences([identifiers_1.Identifiers.TemplateRef, identifiers_1.Identifiers.ElementRef]));
                var externalReferenceVars = new Map();
                externalReferences.forEach(function (ref, typeIndex) {
                    externalReferenceVars.set(ref, "_decl" + ngModuleIndex + "_" + typeIndex);
                });
                externalReferenceVars.forEach(function (varName, reference) {
                    outputCtx.statements.push(o.variable(varName)
                        .set(o.NULL_EXPR.cast(o.DYNAMIC_TYPE))
                        .toDeclStmt(o.expressionType(outputCtx.importExpr(reference, /* typeParams */ null, /* useSummaries */ false))));
                });
                if (emitFlags & 2 /* TypeCheck */) {
                    // add the type-check block for all components of the NgModule
                    ngModuleMeta.declaredDirectives.forEach(function (dirId) {
                        var compMeta = _this._metadataResolver.getDirectiveMetadata(dirId.reference);
                        if (!compMeta.isComponent) {
                            return;
                        }
                        componentId++;
                        _this._createTypeCheckBlock(outputCtx, compMeta.type.reference.name + "_Host_" + componentId, ngModuleMeta, _this._metadataResolver.getHostComponentMetadata(compMeta), [compMeta.type], externalReferenceVars);
                        _this._createTypeCheckBlock(outputCtx, compMeta.type.reference.name + "_" + componentId, ngModuleMeta, compMeta, ngModuleMeta.transitiveModule.directives, externalReferenceVars);
                    });
                }
            });
            if (outputCtx.statements.length === 0) {
                _createEmptyStub(outputCtx);
            }
        };
        AotCompiler.prototype._externalIdentifierReferences = function (references) {
            var e_1, _a;
            var result = [];
            try {
                for (var references_1 = tslib_1.__values(references), references_1_1 = references_1.next(); !references_1_1.done; references_1_1 = references_1.next()) {
                    var reference = references_1_1.value;
                    var token = identifiers_1.createTokenForExternalReference(this.reflector, reference);
                    if (token.identifier) {
                        result.push(token.identifier.reference);
                    }
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (references_1_1 && !references_1_1.done && (_a = references_1.return)) _a.call(references_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
            return result;
        };
        AotCompiler.prototype._createTypeCheckBlock = function (ctx, componentId, moduleMeta, compMeta, directives, externalReferenceVars) {
            var _a;
            var _b = this._parseTemplate(compMeta, moduleMeta, directives), parsedTemplate = _b.template, usedPipes = _b.pipes;
            (_a = ctx.statements).push.apply(_a, tslib_1.__spread(this._typeCheckCompiler.compileComponent(componentId, compMeta, parsedTemplate, usedPipes, externalReferenceVars, ctx)));
        };
        AotCompiler.prototype.emitMessageBundle = function (analyzeResult, locale) {
            var _this = this;
            var errors = [];
            var htmlParser = new html_parser_1.HtmlParser();
            // TODO(vicb): implicit tags & attributes
            var messageBundle = new message_bundle_1.MessageBundle(htmlParser, [], {}, locale);
            analyzeResult.files.forEach(function (file) {
                var compMetas = [];
                file.directives.forEach(function (directiveType) {
                    var dirMeta = _this._metadataResolver.getDirectiveMetadata(directiveType);
                    if (dirMeta && dirMeta.isComponent) {
                        compMetas.push(dirMeta);
                    }
                });
                compMetas.forEach(function (compMeta) {
                    var html = compMeta.template.template;
                    // Template URL points to either an HTML or TS file depending on whether
                    // the file is used with `templateUrl:` or `template:`, respectively.
                    var templateUrl = compMeta.template.templateUrl;
                    var interpolationConfig = interpolation_config_1.InterpolationConfig.fromArray(compMeta.template.interpolation);
                    errors.push.apply(errors, tslib_1.__spread(messageBundle.updateFromTemplate(html, templateUrl, interpolationConfig)));
                });
            });
            if (errors.length) {
                throw new Error(errors.map(function (e) { return e.toString(); }).join('\n'));
            }
            return messageBundle;
        };
        AotCompiler.prototype.emitAllPartialModules = function (_a, r3Files) {
            var _this = this;
            var ngModuleByPipeOrDirective = _a.ngModuleByPipeOrDirective, files = _a.files;
            var contextMap = new Map();
            var getContext = function (fileName) {
                if (!contextMap.has(fileName)) {
                    contextMap.set(fileName, _this._createOutputContext(fileName));
                }
                return contextMap.get(fileName);
            };
            files.forEach(function (file) { return _this._compilePartialModule(file.fileName, ngModuleByPipeOrDirective, file.directives, file.pipes, file.ngModules, file.injectables, getContext(file.fileName)); });
            r3Files.forEach(function (file) { return _this._compileShallowModules(file.fileName, file.shallowModules, getContext(file.fileName)); });
            return Array.from(contextMap.values())
                .map(function (context) { return ({
                fileName: context.genFilePath,
                statements: tslib_1.__spread(context.constantPool.statements, context.statements),
            }); });
        };
        AotCompiler.prototype._compileShallowModules = function (fileName, shallowModules, context) {
            var _this = this;
            shallowModules.forEach(function (module) { return r3_module_compiler_1.compileNgModuleFromRender2(context, module, _this._injectableCompiler); });
        };
        AotCompiler.prototype._compilePartialModule = function (fileName, ngModuleByPipeOrDirective, directives, pipes, ngModules, injectables, context) {
            var _this = this;
            var errors = [];
            var schemaRegistry = new dom_element_schema_registry_1.DomElementSchemaRegistry();
            var hostBindingParser = new binding_parser_1.BindingParser(this._templateParser.expressionParser, interpolation_config_1.DEFAULT_INTERPOLATION_CONFIG, schemaRegistry, [], errors);
            // Process all components and directives
            directives.forEach(function (directiveType) {
                var directiveMetadata = _this._metadataResolver.getDirectiveMetadata(directiveType);
                if (directiveMetadata.isComponent) {
                    var module = ngModuleByPipeOrDirective.get(directiveType);
                    module ||
                        util_1.error("Cannot determine the module for component '" + compile_metadata_1.identifierName(directiveMetadata.type) + "'");
                    var htmlAst = directiveMetadata.template.htmlAst;
                    var preserveWhitespaces = directiveMetadata.template.preserveWhitespaces;
                    if (!preserveWhitespaces) {
                        htmlAst = html_whitespaces_1.removeWhitespaces(htmlAst);
                    }
                    var render3Ast = r3_template_transform_1.htmlAstToRender3Ast(htmlAst.rootNodes, hostBindingParser);
                    // Map of StaticType by directive selectors
                    var directiveTypeBySel_1 = new Map();
                    var directives_1 = module.transitiveModule.directives.map(function (dir) { return _this._metadataResolver.getDirectiveSummary(dir.reference); });
                    directives_1.forEach(function (directive) {
                        if (directive.selector) {
                            directiveTypeBySel_1.set(directive.selector, directive.type.reference);
                        }
                    });
                    // Map of StaticType by pipe names
                    var pipeTypeByName_1 = new Map();
                    var pipes_1 = module.transitiveModule.pipes.map(function (pipe) { return _this._metadataResolver.getPipeSummary(pipe.reference); });
                    pipes_1.forEach(function (pipe) { pipeTypeByName_1.set(pipe.name, pipe.type.reference); });
                    compiler_1.compileComponentFromRender2(context, directiveMetadata, render3Ast, _this.reflector, hostBindingParser, directiveTypeBySel_1, pipeTypeByName_1);
                }
                else {
                    compiler_1.compileDirectiveFromRender2(context, directiveMetadata, _this.reflector, hostBindingParser);
                }
            });
            pipes.forEach(function (pipeType) {
                var pipeMetadata = _this._metadataResolver.getPipeMetadata(pipeType);
                if (pipeMetadata) {
                    r3_pipe_compiler_1.compilePipeFromRender2(context, pipeMetadata, _this.reflector);
                }
            });
            injectables.forEach(function (injectable) { return _this._injectableCompiler.compile(injectable, context); });
        };
        AotCompiler.prototype.emitAllPartialModules2 = function (files) {
            var _this = this;
            // Using reduce like this is a select many pattern (where map is a select pattern)
            return files.reduce(function (r, file) {
                r.push.apply(r, tslib_1.__spread(_this._emitPartialModule2(file.fileName, file.injectables)));
                return r;
            }, []);
        };
        AotCompiler.prototype._emitPartialModule2 = function (fileName, injectables) {
            var _this = this;
            var context = this._createOutputContext(fileName);
            injectables.forEach(function (injectable) { return _this._injectableCompiler.compile(injectable, context); });
            if (context.statements && context.statements.length > 0) {
                return [{ fileName: fileName, statements: tslib_1.__spread(context.constantPool.statements, context.statements) }];
            }
            return [];
        };
        AotCompiler.prototype.emitAllImpls = function (analyzeResult) {
            var _this = this;
            var ngModuleByPipeOrDirective = analyzeResult.ngModuleByPipeOrDirective, files = analyzeResult.files;
            var sourceModules = files.map(function (file) { return _this._compileImplFile(file.fileName, ngModuleByPipeOrDirective, file.directives, file.pipes, file.ngModules, file.injectables); });
            return compile_metadata_1.flatten(sourceModules);
        };
        AotCompiler.prototype._compileImplFile = function (srcFileUrl, ngModuleByPipeOrDirective, directives, pipes, ngModules, injectables) {
            var _this = this;
            var fileSuffix = util_2.normalizeGenFileSuffix(util_2.splitTypescriptSuffix(srcFileUrl, true)[1]);
            var generatedFiles = [];
            var outputCtx = this._createOutputContext(util_2.ngfactoryFilePath(srcFileUrl, true));
            generatedFiles.push.apply(generatedFiles, tslib_1.__spread(this._createSummary(srcFileUrl, directives, pipes, ngModules, injectables, outputCtx)));
            // compile all ng modules
            ngModules.forEach(function (ngModuleMeta) { return _this._compileModule(outputCtx, ngModuleMeta); });
            // compile components
            directives.forEach(function (dirType) {
                var compMeta = _this._metadataResolver.getDirectiveMetadata(dirType);
                if (!compMeta.isComponent) {
                    return;
                }
                var ngModule = ngModuleByPipeOrDirective.get(dirType);
                if (!ngModule) {
                    throw new Error("Internal Error: cannot determine the module for component " + compile_metadata_1.identifierName(compMeta.type) + "!");
                }
                // compile styles
                var componentStylesheet = _this._styleCompiler.compileComponent(outputCtx, compMeta);
                // Note: compMeta is a component and therefore template is non null.
                compMeta.template.externalStylesheets.forEach(function (stylesheetMeta) {
                    // Note: fill non shim and shim style files as they might
                    // be shared by component with and without ViewEncapsulation.
                    var shim = _this._styleCompiler.needsStyleShim(compMeta);
                    generatedFiles.push(_this._codegenStyles(srcFileUrl, compMeta, stylesheetMeta, shim, fileSuffix));
                    if (_this._options.allowEmptyCodegenFiles) {
                        generatedFiles.push(_this._codegenStyles(srcFileUrl, compMeta, stylesheetMeta, !shim, fileSuffix));
                    }
                });
                // compile components
                var compViewVars = _this._compileComponent(outputCtx, compMeta, ngModule, ngModule.transitiveModule.directives, componentStylesheet, fileSuffix);
                _this._compileComponentFactory(outputCtx, compMeta, ngModule, fileSuffix);
            });
            if (outputCtx.statements.length > 0 || this._options.allowEmptyCodegenFiles) {
                var srcModule = this._codegenSourceModule(srcFileUrl, outputCtx);
                generatedFiles.unshift(srcModule);
            }
            return generatedFiles;
        };
        AotCompiler.prototype._createSummary = function (srcFileName, directives, pipes, ngModules, injectables, ngFactoryCtx) {
            var _this = this;
            var symbolSummaries = this._symbolResolver.getSymbolsOf(srcFileName)
                .map(function (symbol) { return _this._symbolResolver.resolveSymbol(symbol); });
            var typeData = tslib_1.__spread(ngModules.map(function (meta) { return ({
                summary: _this._metadataResolver.getNgModuleSummary(meta.type.reference),
                metadata: _this._metadataResolver.getNgModuleMetadata(meta.type.reference)
            }); }), directives.map(function (ref) { return ({
                summary: _this._metadataResolver.getDirectiveSummary(ref),
                metadata: _this._metadataResolver.getDirectiveMetadata(ref)
            }); }), pipes.map(function (ref) { return ({
                summary: _this._metadataResolver.getPipeSummary(ref),
                metadata: _this._metadataResolver.getPipeMetadata(ref)
            }); }), injectables.map(function (ref) { return ({
                summary: _this._metadataResolver.getInjectableSummary(ref.symbol),
                metadata: _this._metadataResolver.getInjectableSummary(ref.symbol).type
            }); }));
            var forJitOutputCtx = this._options.enableSummariesForJit ?
                this._createOutputContext(util_2.summaryForJitFileName(srcFileName, true)) :
                null;
            var _a = summary_serializer_1.serializeSummaries(srcFileName, forJitOutputCtx, this._summaryResolver, this._symbolResolver, symbolSummaries, typeData, this._options.createExternalSymbolFactoryReexports), json = _a.json, exportAs = _a.exportAs;
            exportAs.forEach(function (entry) {
                ngFactoryCtx.statements.push(o.variable(entry.exportAs).set(ngFactoryCtx.importExpr(entry.symbol)).toDeclStmt(null, [
                    o.StmtModifier.Exported
                ]));
            });
            var summaryJson = new generated_file_1.GeneratedFile(srcFileName, util_2.summaryFileName(srcFileName), json);
            var result = [summaryJson];
            if (forJitOutputCtx) {
                result.push(this._codegenSourceModule(srcFileName, forJitOutputCtx));
            }
            return result;
        };
        AotCompiler.prototype._compileModule = function (outputCtx, ngModule) {
            var providers = [];
            if (this._options.locale) {
                var normalizedLocale = this._options.locale.replace(/_/g, '-');
                providers.push({
                    token: identifiers_1.createTokenForExternalReference(this.reflector, identifiers_1.Identifiers.LOCALE_ID),
                    useValue: normalizedLocale,
                });
            }
            if (this._options.i18nFormat) {
                providers.push({
                    token: identifiers_1.createTokenForExternalReference(this.reflector, identifiers_1.Identifiers.TRANSLATIONS_FORMAT),
                    useValue: this._options.i18nFormat
                });
            }
            this._ngModuleCompiler.compile(outputCtx, ngModule, providers);
        };
        AotCompiler.prototype._compileComponentFactory = function (outputCtx, compMeta, ngModule, fileSuffix) {
            var hostMeta = this._metadataResolver.getHostComponentMetadata(compMeta);
            var hostViewFactoryVar = this._compileComponent(outputCtx, hostMeta, ngModule, [compMeta.type], null, fileSuffix)
                .viewClassVar;
            var compFactoryVar = compile_metadata_1.componentFactoryName(compMeta.type.reference);
            var inputsExprs = [];
            for (var propName in compMeta.inputs) {
                var templateName = compMeta.inputs[propName];
                // Don't quote so that the key gets minified...
                inputsExprs.push(new o.LiteralMapEntry(propName, o.literal(templateName), false));
            }
            var outputsExprs = [];
            for (var propName in compMeta.outputs) {
                var templateName = compMeta.outputs[propName];
                // Don't quote so that the key gets minified...
                outputsExprs.push(new o.LiteralMapEntry(propName, o.literal(templateName), false));
            }
            outputCtx.statements.push(o.variable(compFactoryVar)
                .set(o.importExpr(identifiers_1.Identifiers.createComponentFactory).callFn([
                o.literal(compMeta.selector), outputCtx.importExpr(compMeta.type.reference),
                o.variable(hostViewFactoryVar), new o.LiteralMapExpr(inputsExprs),
                new o.LiteralMapExpr(outputsExprs),
                o.literalArr(compMeta.template.ngContentSelectors.map(function (selector) { return o.literal(selector); }))
            ]))
                .toDeclStmt(o.importType(identifiers_1.Identifiers.ComponentFactory, [o.expressionType(outputCtx.importExpr(compMeta.type.reference))], [o.TypeModifier.Const]), [o.StmtModifier.Final, o.StmtModifier.Exported]));
        };
        AotCompiler.prototype._compileComponent = function (outputCtx, compMeta, ngModule, directiveIdentifiers, componentStyles, fileSuffix) {
            var _a = this._parseTemplate(compMeta, ngModule, directiveIdentifiers), parsedTemplate = _a.template, usedPipes = _a.pipes;
            var stylesExpr = componentStyles ? o.variable(componentStyles.stylesVar) : o.literalArr([]);
            var viewResult = this._viewCompiler.compileComponent(outputCtx, compMeta, parsedTemplate, stylesExpr, usedPipes);
            if (componentStyles) {
                _resolveStyleStatements(this._symbolResolver, componentStyles, this._styleCompiler.needsStyleShim(compMeta), fileSuffix);
            }
            return viewResult;
        };
        AotCompiler.prototype._parseTemplate = function (compMeta, ngModule, directiveIdentifiers) {
            var _this = this;
            if (this._templateAstCache.has(compMeta.type.reference)) {
                return this._templateAstCache.get(compMeta.type.reference);
            }
            var preserveWhitespaces = compMeta.template.preserveWhitespaces;
            var directives = directiveIdentifiers.map(function (dir) { return _this._metadataResolver.getDirectiveSummary(dir.reference); });
            var pipes = ngModule.transitiveModule.pipes.map(function (pipe) { return _this._metadataResolver.getPipeSummary(pipe.reference); });
            var result = this._templateParser.parse(compMeta, compMeta.template.htmlAst, directives, pipes, ngModule.schemas, compile_metadata_1.templateSourceUrl(ngModule.type, compMeta, compMeta.template), preserveWhitespaces);
            this._templateAstCache.set(compMeta.type.reference, result);
            return result;
        };
        AotCompiler.prototype._createOutputContext = function (genFilePath) {
            var _this = this;
            var importExpr = function (symbol, typeParams, useSummaries) {
                if (typeParams === void 0) { typeParams = null; }
                if (useSummaries === void 0) { useSummaries = true; }
                if (!(symbol instanceof static_symbol_1.StaticSymbol)) {
                    throw new Error("Internal error: unknown identifier " + JSON.stringify(symbol));
                }
                var arity = _this._symbolResolver.getTypeArity(symbol) || 0;
                var _a = _this._symbolResolver.getImportAs(symbol, useSummaries) || symbol, filePath = _a.filePath, name = _a.name, members = _a.members;
                var importModule = _this._fileNameToModuleName(filePath, genFilePath);
                // It should be good enough to compare filePath to genFilePath and if they are equal
                // there is a self reference. However, ngfactory files generate to .ts but their
                // symbols have .d.ts so a simple compare is insufficient. They should be canonical
                // and is tracked by #17705.
                var selfReference = _this._fileNameToModuleName(genFilePath, genFilePath);
                var moduleName = importModule === selfReference ? null : importModule;
                // If we are in a type expression that refers to a generic type then supply
                // the required type parameters. If there were not enough type parameters
                // supplied, supply any as the type. Outside a type expression the reference
                // should not supply type parameters and be treated as a simple value reference
                // to the constructor function itself.
                var suppliedTypeParams = typeParams || [];
                var missingTypeParamsCount = arity - suppliedTypeParams.length;
                var allTypeParams = suppliedTypeParams.concat(new Array(missingTypeParamsCount).fill(o.DYNAMIC_TYPE));
                return members.reduce(function (expr, memberName) { return expr.prop(memberName); }, o.importExpr(new o.ExternalReference(moduleName, name, null), allTypeParams));
            };
            return { statements: [], genFilePath: genFilePath, importExpr: importExpr, constantPool: new constant_pool_1.ConstantPool() };
        };
        AotCompiler.prototype._fileNameToModuleName = function (importedFilePath, containingFilePath) {
            return this._summaryResolver.getKnownModuleName(importedFilePath) ||
                this._symbolResolver.getKnownModuleName(importedFilePath) ||
                this._host.fileNameToModuleName(importedFilePath, containingFilePath);
        };
        AotCompiler.prototype._codegenStyles = function (srcFileUrl, compMeta, stylesheetMetadata, isShimmed, fileSuffix) {
            var outputCtx = this._createOutputContext(_stylesModuleUrl(stylesheetMetadata.moduleUrl, isShimmed, fileSuffix));
            var compiledStylesheet = this._styleCompiler.compileStyles(outputCtx, compMeta, stylesheetMetadata, isShimmed);
            _resolveStyleStatements(this._symbolResolver, compiledStylesheet, isShimmed, fileSuffix);
            return this._codegenSourceModule(srcFileUrl, outputCtx);
        };
        AotCompiler.prototype._codegenSourceModule = function (srcFileUrl, ctx) {
            return new generated_file_1.GeneratedFile(srcFileUrl, ctx.genFilePath, ctx.statements);
        };
        AotCompiler.prototype.listLazyRoutes = function (entryRoute, analyzedModules) {
            var e_2, _a, e_3, _b;
            var self = this;
            if (entryRoute) {
                var symbol = lazy_routes_1.parseLazyRoute(entryRoute, this.reflector).referencedModule;
                return visitLazyRoute(symbol);
            }
            else if (analyzedModules) {
                var allLazyRoutes = [];
                try {
                    for (var _c = tslib_1.__values(analyzedModules.ngModules), _d = _c.next(); !_d.done; _d = _c.next()) {
                        var ngModule = _d.value;
                        var lazyRoutes = lazy_routes_1.listLazyRoutes(ngModule, this.reflector);
                        try {
                            for (var lazyRoutes_1 = (e_3 = void 0, tslib_1.__values(lazyRoutes)), lazyRoutes_1_1 = lazyRoutes_1.next(); !lazyRoutes_1_1.done; lazyRoutes_1_1 = lazyRoutes_1.next()) {
                                var lazyRoute = lazyRoutes_1_1.value;
                                allLazyRoutes.push(lazyRoute);
                            }
                        }
                        catch (e_3_1) { e_3 = { error: e_3_1 }; }
                        finally {
                            try {
                                if (lazyRoutes_1_1 && !lazyRoutes_1_1.done && (_b = lazyRoutes_1.return)) _b.call(lazyRoutes_1);
                            }
                            finally { if (e_3) throw e_3.error; }
                        }
                    }
                }
                catch (e_2_1) { e_2 = { error: e_2_1 }; }
                finally {
                    try {
                        if (_d && !_d.done && (_a = _c.return)) _a.call(_c);
                    }
                    finally { if (e_2) throw e_2.error; }
                }
                return allLazyRoutes;
            }
            else {
                throw new Error("Either route or analyzedModules has to be specified!");
            }
            function visitLazyRoute(symbol, seenRoutes, allLazyRoutes) {
                var e_4, _a;
                if (seenRoutes === void 0) { seenRoutes = new Set(); }
                if (allLazyRoutes === void 0) { allLazyRoutes = []; }
                // Support pointing to default exports, but stop recursing there,
                // as the StaticReflector does not yet support default exports.
                if (seenRoutes.has(symbol) || !symbol.name) {
                    return allLazyRoutes;
                }
                seenRoutes.add(symbol);
                var lazyRoutes = lazy_routes_1.listLazyRoutes(self._metadataResolver.getNgModuleMetadata(symbol, true), self.reflector);
                try {
                    for (var lazyRoutes_2 = tslib_1.__values(lazyRoutes), lazyRoutes_2_1 = lazyRoutes_2.next(); !lazyRoutes_2_1.done; lazyRoutes_2_1 = lazyRoutes_2.next()) {
                        var lazyRoute = lazyRoutes_2_1.value;
                        allLazyRoutes.push(lazyRoute);
                        visitLazyRoute(lazyRoute.referencedModule, seenRoutes, allLazyRoutes);
                    }
                }
                catch (e_4_1) { e_4 = { error: e_4_1 }; }
                finally {
                    try {
                        if (lazyRoutes_2_1 && !lazyRoutes_2_1.done && (_a = lazyRoutes_2.return)) _a.call(lazyRoutes_2);
                    }
                    finally { if (e_4) throw e_4.error; }
                }
                return allLazyRoutes;
            }
        };
        return AotCompiler;
    }());
    exports.AotCompiler = AotCompiler;
    function _createEmptyStub(outputCtx) {
        // Note: We need to produce at least one import statement so that
        // TypeScript knows that the file is an es6 module. Otherwise our generated
        // exports / imports won't be emitted properly by TypeScript.
        outputCtx.statements.push(o.importExpr(identifiers_1.Identifiers.ComponentFactory).toStmt());
    }
    function _resolveStyleStatements(symbolResolver, compileResult, needsShim, fileSuffix) {
        compileResult.dependencies.forEach(function (dep) {
            dep.setValue(symbolResolver.getStaticSymbol(_stylesModuleUrl(dep.moduleUrl, needsShim, fileSuffix), dep.name));
        });
    }
    function _stylesModuleUrl(stylesheetUrl, shim, suffix) {
        return "" + stylesheetUrl + (shim ? '.shim' : '') + ".ngstyle" + suffix;
    }
    function analyzeNgModules(fileNames, host, staticSymbolResolver, metadataResolver) {
        var files = _analyzeFilesIncludingNonProgramFiles(fileNames, host, staticSymbolResolver, metadataResolver);
        return mergeAnalyzedFiles(files);
    }
    exports.analyzeNgModules = analyzeNgModules;
    function analyzeAndValidateNgModules(fileNames, host, staticSymbolResolver, metadataResolver) {
        return validateAnalyzedModules(analyzeNgModules(fileNames, host, staticSymbolResolver, metadataResolver));
    }
    exports.analyzeAndValidateNgModules = analyzeAndValidateNgModules;
    function validateAnalyzedModules(analyzedModules) {
        if (analyzedModules.symbolsMissingModule && analyzedModules.symbolsMissingModule.length) {
            var messages = analyzedModules.symbolsMissingModule.map(function (s) {
                return "Cannot determine the module for class " + s.name + " in " + s.filePath + "! Add " + s.name + " to the NgModule to fix it.";
            });
            throw util_1.syntaxError(messages.join('\n'));
        }
        return analyzedModules;
    }
    // Analyzes all of the program files,
    // including files that are not part of the program
    // but are referenced by an NgModule.
    function _analyzeFilesIncludingNonProgramFiles(fileNames, host, staticSymbolResolver, metadataResolver) {
        var seenFiles = new Set();
        var files = [];
        var visitFile = function (fileName) {
            if (seenFiles.has(fileName) || !host.isSourceFile(fileName)) {
                return false;
            }
            seenFiles.add(fileName);
            var analyzedFile = analyzeFile(host, staticSymbolResolver, metadataResolver, fileName);
            files.push(analyzedFile);
            analyzedFile.ngModules.forEach(function (ngModule) {
                ngModule.transitiveModule.modules.forEach(function (modMeta) { return visitFile(modMeta.reference.filePath); });
            });
        };
        fileNames.forEach(function (fileName) { return visitFile(fileName); });
        return files;
    }
    function analyzeFile(host, staticSymbolResolver, metadataResolver, fileName) {
        var directives = [];
        var pipes = [];
        var injectables = [];
        var ngModules = [];
        var hasDecorators = staticSymbolResolver.hasDecorators(fileName);
        var exportsNonSourceFiles = false;
        // Don't analyze .d.ts files that have no decorators as a shortcut
        // to speed up the analysis. This prevents us from
        // resolving the references in these files.
        // Note: exportsNonSourceFiles is only needed when compiling with summaries,
        // which is not the case when .d.ts files are treated as input files.
        if (!fileName.endsWith('.d.ts') || hasDecorators) {
            staticSymbolResolver.getSymbolsOf(fileName).forEach(function (symbol) {
                var resolvedSymbol = staticSymbolResolver.resolveSymbol(symbol);
                var symbolMeta = resolvedSymbol.metadata;
                if (!symbolMeta || symbolMeta.__symbolic === 'error') {
                    return;
                }
                var isNgSymbol = false;
                if (symbolMeta.__symbolic === 'class') {
                    if (metadataResolver.isDirective(symbol)) {
                        isNgSymbol = true;
                        directives.push(symbol);
                    }
                    else if (metadataResolver.isPipe(symbol)) {
                        isNgSymbol = true;
                        pipes.push(symbol);
                    }
                    else if (metadataResolver.isNgModule(symbol)) {
                        var ngModule = metadataResolver.getNgModuleMetadata(symbol, false);
                        if (ngModule) {
                            isNgSymbol = true;
                            ngModules.push(ngModule);
                        }
                    }
                    else if (metadataResolver.isInjectable(symbol)) {
                        isNgSymbol = true;
                        var injectable = metadataResolver.getInjectableMetadata(symbol, null, false);
                        if (injectable) {
                            injectables.push(injectable);
                        }
                    }
                }
                if (!isNgSymbol) {
                    exportsNonSourceFiles =
                        exportsNonSourceFiles || isValueExportingNonSourceFile(host, symbolMeta);
                }
            });
        }
        return {
            fileName: fileName, directives: directives, pipes: pipes, ngModules: ngModules, injectables: injectables, exportsNonSourceFiles: exportsNonSourceFiles,
        };
    }
    exports.analyzeFile = analyzeFile;
    function analyzeFileForInjectables(host, staticSymbolResolver, metadataResolver, fileName) {
        var injectables = [];
        var shallowModules = [];
        if (staticSymbolResolver.hasDecorators(fileName)) {
            staticSymbolResolver.getSymbolsOf(fileName).forEach(function (symbol) {
                var resolvedSymbol = staticSymbolResolver.resolveSymbol(symbol);
                var symbolMeta = resolvedSymbol.metadata;
                if (!symbolMeta || symbolMeta.__symbolic === 'error') {
                    return;
                }
                if (symbolMeta.__symbolic === 'class') {
                    if (metadataResolver.isInjectable(symbol)) {
                        var injectable = metadataResolver.getInjectableMetadata(symbol, null, false);
                        if (injectable) {
                            injectables.push(injectable);
                        }
                    }
                    else if (metadataResolver.isNgModule(symbol)) {
                        var module = metadataResolver.getShallowModuleMetadata(symbol);
                        if (module) {
                            shallowModules.push(module);
                        }
                    }
                }
            });
        }
        return { fileName: fileName, injectables: injectables, shallowModules: shallowModules };
    }
    exports.analyzeFileForInjectables = analyzeFileForInjectables;
    function isValueExportingNonSourceFile(host, metadata) {
        var exportsNonSourceFiles = false;
        var Visitor = /** @class */ (function () {
            function Visitor() {
            }
            Visitor.prototype.visitArray = function (arr, context) {
                var _this = this;
                arr.forEach(function (v) { return util_1.visitValue(v, _this, context); });
            };
            Visitor.prototype.visitStringMap = function (map, context) {
                var _this = this;
                Object.keys(map).forEach(function (key) { return util_1.visitValue(map[key], _this, context); });
            };
            Visitor.prototype.visitPrimitive = function (value, context) { };
            Visitor.prototype.visitOther = function (value, context) {
                if (value instanceof static_symbol_1.StaticSymbol && !host.isSourceFile(value.filePath)) {
                    exportsNonSourceFiles = true;
                }
            };
            return Visitor;
        }());
        util_1.visitValue(metadata, new Visitor(), null);
        return exportsNonSourceFiles;
    }
    function mergeAnalyzedFiles(analyzedFiles) {
        var allNgModules = [];
        var ngModuleByPipeOrDirective = new Map();
        var allPipesAndDirectives = new Set();
        analyzedFiles.forEach(function (af) {
            af.ngModules.forEach(function (ngModule) {
                allNgModules.push(ngModule);
                ngModule.declaredDirectives.forEach(function (d) { return ngModuleByPipeOrDirective.set(d.reference, ngModule); });
                ngModule.declaredPipes.forEach(function (p) { return ngModuleByPipeOrDirective.set(p.reference, ngModule); });
            });
            af.directives.forEach(function (d) { return allPipesAndDirectives.add(d); });
            af.pipes.forEach(function (p) { return allPipesAndDirectives.add(p); });
        });
        var symbolsMissingModule = [];
        allPipesAndDirectives.forEach(function (ref) {
            if (!ngModuleByPipeOrDirective.has(ref)) {
                symbolsMissingModule.push(ref);
            }
        });
        return {
            ngModules: allNgModules,
            ngModuleByPipeOrDirective: ngModuleByPipeOrDirective,
            symbolsMissingModule: symbolsMissingModule,
            files: analyzedFiles
        };
    }
    exports.mergeAnalyzedFiles = mergeAnalyzedFiles;
    function mergeAndValidateNgFiles(files) {
        return validateAnalyzedModules(mergeAnalyzedFiles(files));
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvYW90L2NvbXBpbGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILDJFQUFrWDtJQUVsWCxxRUFBOEM7SUFDOUMsbURBQTBDO0lBQzFDLDRFQUFxRDtJQUNyRCxpRUFBNEU7SUFJNUUsMkVBQW9EO0lBQ3BELHFGQUFnRTtJQUNoRSw2RkFBb0c7SUFHcEcsMkRBQTBDO0lBRTFDLHVGQUE0RjtJQUM1RixtRkFBb0Y7SUFDcEYsNkZBQXFFO0lBQ3JFLHdFQUE4STtJQUM5SSx3R0FBK0U7SUFHL0UsdUZBQWdFO0lBR2hFLG1EQUFvRjtJQU1wRiwyRUFBK0M7SUFDL0MscUVBQXdFO0lBR3hFLHlFQUE2QztJQUU3QyxtRkFBMEU7SUFDMUUsdURBQWdJO0lBSWhJO1FBTUUscUJBQ1ksT0FBdUIsRUFBVSxRQUE0QixFQUM3RCxLQUFzQixFQUFXLFNBQTBCLEVBQzNELGlCQUEwQyxFQUFVLGVBQStCLEVBQ25GLGNBQTZCLEVBQVUsYUFBMkIsRUFDbEUsa0JBQXFDLEVBQVUsaUJBQW1DLEVBQ2xGLG1CQUF1QyxFQUFVLGNBQTZCLEVBQzlFLGdCQUErQyxFQUMvQyxlQUFxQztZQVByQyxZQUFPLEdBQVAsT0FBTyxDQUFnQjtZQUFVLGFBQVEsR0FBUixRQUFRLENBQW9CO1lBQzdELFVBQUssR0FBTCxLQUFLLENBQWlCO1lBQVcsY0FBUyxHQUFULFNBQVMsQ0FBaUI7WUFDM0Qsc0JBQWlCLEdBQWpCLGlCQUFpQixDQUF5QjtZQUFVLG9CQUFlLEdBQWYsZUFBZSxDQUFnQjtZQUNuRixtQkFBYyxHQUFkLGNBQWMsQ0FBZTtZQUFVLGtCQUFhLEdBQWIsYUFBYSxDQUFjO1lBQ2xFLHVCQUFrQixHQUFsQixrQkFBa0IsQ0FBbUI7WUFBVSxzQkFBaUIsR0FBakIsaUJBQWlCLENB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b0QsQ0FBQyxDQUFDO1lBQzVGLENBQUMsQ0FBQyxDQUFDO1lBQ0gsRUFBRSxDQUFDLFVBQVUsQ0FBQyxPQUFPLENBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxxQkFBcUIsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLEVBQTVCLENBQTRCLENBQUMsQ0FBQztZQUN6RCxFQUFFLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxVQUFBLENBQUMsSUFBSSxPQUFBLHFCQUFxQixDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsRUFBNUIsQ0FBNEIsQ0FBQyxDQUFDO1FBQ3RELENBQUMsQ0FBQyxDQUFDO1FBRUgsSUFBTSxvQkFBb0IsR0FBbUIsRUFBRSxDQUFDO1FBQ2hELHFCQUFxQixDQUFDLE9BQU8sQ0FBQyxVQUFBLEdBQUc7WUFDL0IsSUFBSSxDQUFDLHlCQUF5QixDQUFDLEdBQUcsQ0FBQyxHQUFHLENBQUMsRUFBRTtnQkFDdkMsb0JBQW9CLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDO2FBQ2hDO1FBQ0gsQ0FBQyxDQUFDLENBQUM7UUFDSCxPQUFPO1lBQ0wsU0FBUyxFQUFFLFlBQVk7WUFDdkIseUJBQXlCLDJCQUFBO1lBQ3pCLG9CQUFvQixzQkFBQTtZQUNwQixLQUFLLEVBQUUsYUFBYTtTQUNyQixDQUFDO0lBQ0osQ0FBQztJQTVCRCxnREE0QkM7SUFFRCxTQUFTLHVCQUF1QixDQUFDLEtBQXVCO1FBQ3RELE9BQU8sdUJBQXVCLENBQUMsa0JBQWtCLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztJQUM1RCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YSwgQ29tcGlsZUluamVjdGFibGVNZXRhZGF0YSwgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsIENvbXBpbGVQaXBlTWV0YWRhdGEsIENvbXBpbGVQaXBlU3VtbWFyeSwgQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGEsIENvbXBpbGVTaGFsbG93TW9kdWxlTWV0YWRhdGEsIENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEsIENvbXBpbGVUeXBlTWV0YWRhdGEsIENvbXBpbGVUeXBlU3VtbWFyeSwgY29tcG9uZW50RmFjdG9yeU5hbWUsIGZsYXR0ZW4sIGlkZW50aWZpZXJOYW1lLCB0ZW1wbGF0ZVNvdXJjZVVybH0gZnJvbSAnLi4vY29tcGlsZV9tZXRhZGF0YSc7XG5pbXBvcnQge0NvbXBpbGVyQ29uZmlnfSBmcm9tICcuLi9jb25maWcnO1xuaW1wb3J0IHtDb25zdGFudFBvb2x9IGZyb20gJy4uL2NvbnN0YW50X3Bvb2wnO1xuaW1wb3J0IHtWaWV3RW5jYXBzdWxhdGlvbn0gZnJvbSAnLi4vY29yZSc7XG5pbXBvcnQge01lc3NhZ2VCdW5kbGV9IGZyb20gJy4uL2kxOG4vbWVzc2FnZV9idW5kbGUnO1xuaW1wb3J0IHtJZGVudGlmaWVycywgY3JlYXRlVG9rZW5Gb3JFeHRlcm5hbFJlZmVyZW5jZX0gZnJvbSAnLi4vaWRlbnRpZmllcnMnO1xuaW1wb3J0IHtJbmplY3RhYmxlQ29tcGlsZXJ9IGZyb20gJy4uL2luamVjdGFibGVfY29tcGlsZXInO1xuaW1wb3J0IHtDb21waWxlTWV0YWRhdGFSZXNvbHZlcn0gZnJvbSAnLi4vbWV0YWRhdGFfcmVzb2x2ZXInO1xuaW1wb3J0ICogYXMgaHRtbCBmcm9tICcuLi9tbF9wYXJzZXIvYXN0JztcbmltcG9ydCB7SHRtbFBhcnNlcn0gZnJvbSAnLi4vbWxfcGFyc2VyL2h0bWxfcGFyc2VyJztcbmltcG9ydCB7cmVtb3ZlV2hpdGVzcGFjZXN9IGZyb20gJy4uL21sX3BhcnNlci9odG1sX3doaXRlc3BhY2VzJztcbmltcG9ydCB7REVGQVVMVF9JTlRFUlBPTEFUSU9OX0NPTkZJRywgSW50ZXJwb2xhdGlvbkNvbmZpZ30gZnJvbSAnLi4vbWxfcGFyc2VyL2ludGVycG9sYXRpb25fY29uZmlnJztcbmltcG9ydCB7TmdNb2R1bGVDb21waWxlcn0gZnJvbSAnLi4vbmdfbW9kdWxlX2NvbXBpbGVyJztcbmltcG9ydCB7T3V0cHV0RW1pdHRlcn0gZnJvbSAnLi4vb3V0cHV0L2Fic3RyYWN0X2VtaXR0ZXInO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuLi9vdXRwdXQvb3V0cHV0X2FzdCc7XG5pbXBvcnQge1BhcnNlRXJyb3J9IGZyb20gJy4uL3BhcnNlX3V0aWwnO1xuaW1wb3J0IHtjb21waWxlTmdNb2R1bGVGcm9tUmVuZGVyMiBhcyBjb21waWxlUjNNb2R1bGV9IGZyb20gJy4uL3JlbmRlcjMvcjNfbW9kdWxlX2NvbXBpbGVyJztcbmltcG9ydCB7Y29tcGlsZVBpcGVGcm9tUmVuZGVyMiBhcyBjb21waWxlUjNQaXBlfSBmcm9tICcuLi9yZW5kZXIzL3IzX3BpcGVfY29tcGlsZXInO1xuaW1wb3J0IHtodG1sQXN0VG9SZW5kZXIzQXN0fSBmcm9tICcuLi9yZW5kZXIzL3IzX3RlbXBsYXRlX3RyYW5zZm9ybSc7XG5pbXBvcnQge2NvbXBpbGVDb21wb25lbnRGcm9tUmVuZGVyMiBhcyBjb21waWxlUjNDb21wb25lbnQsIGNvbXBpbGVEaXJlY3RpdmVGcm9tUmVuZGVyMiBhcyBjb21waWxlUjNEaXJlY3RpdmV9IGZyb20gJy4uL3JlbmRlcjMvdmlldy9jb21waWxlcic7XG5pbXBvcnQge0RvbUVsZW1lbnRTY2hlbWFSZWdpc3RyeX0gZnJvbSAnLi4vc2NoZW1hL2RvbV9lbGVtZW50X3NjaGVtYV9yZWdpc3RyeSc7XG5pbXBvcnQge0NvbXBpbGVkU3R5bGVzaGVldCwgU3R5bGVDb21waWxlcn0gZnJvbSAnLi4vc3R5bGVfY29tcGlsZXInO1xuaW1wb3J0IHtTdW1tYXJ5UmVzb2x2ZXJ9IGZyb20gJy4uL3N1bW1hcnlfcmVzb2x2ZXInO1xuaW1wb3J0IHtCaW5kaW5nUGFyc2VyfSBmcm9tICcuLi90ZW1wbGF0ZV9wYXJzZXIvYmluZGluZ19wYXJzZXInO1xuaW1wb3J0IHtUZW1wbGF0ZUFzdH0gZnJvbSAnLi4vdGVtcGxhdGVfcGFyc2VyL3RlbXBsYXRlX2FzdCc7XG5pbXBvcnQge1RlbXBsYXRlUGFyc2VyfSBmcm9tICcuLi90ZW1wbGF0ZV9wYXJzZXIvdGVtcGxhdGVfcGFyc2VyJztcbmltcG9ydCB7T3V0cHV0Q29udGV4dCwgVmFsdWVWaXNpdG9yLCBlcnJvciwgc3ludGF4RXJyb3IsIHZpc2l0VmFsdWV9IGZyb20gJy4uL3V0aWwnO1xuaW1wb3J0IHtUeXBlQ2hlY2tDb21waWxlcn0gZnJvbSAnLi4vdmlld19jb21waWxlci90eXBlX2NoZWNrX2NvbXBpbGVyJztcbmltcG9ydCB7Vmlld0NvbXBpbGVSZXN1bHQsIFZpZXdDb21waWxlcn0gZnJvbSAnLi4vdmlld19jb21waWxlci92aWV3X2NvbXBpbGVyJztcblxuaW1wb3J0IHtBb3RDb21waWxlckhvc3R9IGZyb20gJy4vY29tcGlsZXJfaG9zdCc7XG5pbXBvcnQge0FvdENvbXBpbGVyT3B0aW9uc30gZnJvbSAnLi9jb21waWxlcl9vcHRpb25zJztcbmltcG9ydCB7R2VuZXJhdGVkRmlsZX0gZnJvbSAnLi9nZW5lcmF0ZWRfZmlsZSc7XG5pbXBvcnQge0xhenlSb3V0ZSwgbGlzdExhenlSb3V0ZXMsIHBhcnNlTGF6eVJvdXRlfSBmcm9tICcuL2xhenlfcm91dGVzJztcbmltcG9ydCB7UGFydGlhbE1vZHVsZX0gZnJvbSAnLi9wYXJ0aWFsX21vZHVsZSc7XG5pbXBvcnQge1N0YXRpY1JlZmxlY3Rvcn0gZnJvbSAnLi9zdGF0aWNfcmVmbGVjdG9yJztcbmltcG9ydCB7U3RhdGljU3ltYm9sfSBmcm9tICcuL3N0YXRpY19zeW1ib2wnO1xuaW1wb3J0IHtTdGF0aWNTeW1ib2xSZXNvbHZlcn0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sX3Jlc29sdmVyJztcbmltcG9ydCB7Y3JlYXRlRm9ySml0U3R1Yiwgc2VyaWFsaXplU3VtbWFyaWVzfSBmcm9tICcuL3N1bW1hcnlfc2VyaWFsaXplcic7XG5pbXBvcnQge25nZmFjdG9yeUZpbGVQYXRoLCBub3JtYWxpemVHZW5GaWxlU3VmZml4LCBzcGxpdFR5cGVzY3JpcHRTdWZmaXgsIHN1bW1hcnlGaWxlTmFtZSwgc3VtbWFyeUZvckppdEZpbGVOYW1lfSBmcm9tICcuL3V0aWwnO1xuXG5jb25zdCBlbnVtIFN0dWJFbWl0RmxhZ3MgeyBCYXNpYyA9IDEgPDwgMCwgVHlwZUNoZWNrID0gMSA8PCAxLCBBbGwgPSBUeXBlQ2hlY2sgfCBCYXNpYyB9XG5cbmV4cG9ydCBjbGFzcyBBb3RDb21waWxlciB7XG4gIHByaXZhdGUgX3RlbXBsYXRlQXN0Q2FjaGUgPVxuICAgICAgbmV3IE1hcDxTdGF0aWNTeW1ib2wsIHt0ZW1wbGF0ZTogVGVtcGxhdGVBc3RbXSwgcGlwZXM6IENvbXBpbGVQaXBlU3VtbWFyeVtdfT4oKTtcbiAgcHJpdmF0ZSBfYW5hbHl6ZWRGaWxlcyA9IG5ldyBNYXA8c3RyaW5nLCBOZ0FuYWx5emVkRmlsZT4oKTtcbiAgcHJpdmF0ZSBfYW5hbHl6ZWRGaWxlc0ZvckluamVjdGFibGVzID0gbmV3IE1hcDxzdHJpbmcsIE5nQW5hbHl6ZWRGaWxlV2l0aEluamVjdGFibGVzPigpO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBfY29uZmlnOiBDb21waWxlckNvbmZpZywgcHJpdmF0ZSBfb3B0aW9uczogQW90Q29tcGlsZXJPcHRpb25zLFxuICAgICAgcHJpdmF0ZSBfaG9zdDogQW90Q29tcGlsZXJIb3N0LCByZWFkb25seSByZWZsZWN0b3I6IFN0YXRpY1JlZmxlY3RvcixcbiAgICAgIHByaXZhdGUgX21ldGFkYXRhUmVzb2x2ZXI6IENvbXBpbGVNZXRhZGF0YVJlc29sdmVyLCBwcml2YXRlIF90ZW1wbGF0ZVBhcnNlcjogVGVtcGxhdGVQYXJzZXIsXG4gICAgICBwcml2YXRlIF9zdHlsZUNvbXBpbGVyOiBTdHlsZUNvbXBpbGVyLCBwcml2YXRlIF92aWV3Q29tcGlsZXI6IFZpZXdDb21waWxlcixcbiAgICAgIHByaXZhdGUgX3R5cGVDaGVja0NvbXBpbGVyOiBUeXBlQ2hlY2tDb21waWxlciwgcHJpdmF0ZSBfbmdNb2R1bGVDb21waWxlcjogTmdNb2R1bGVDb21waWxlcixcbiAgICAgIHByaXZhdGUgX2luamVjdGFibGVDb21waWxlcjogSW5qZWN0YWJsZUNvbXBpbGVyLCBwcml2YXRlIF9vdXRwdXRFbWl0dGVyOiBPdXRwdXRFbWl0dGVyLFxuICAgICAgcHJpdmF0ZSBfc3VtbWFyeVJlc29sdmVyOiBTdW1tYXJ5UmVzb2x2ZXI8U3RhdGljU3ltYm9sPixcbiAgICAgIHByaXZhdGUgX3N5bWJvbFJlc29sdmVyOiBTdGF0aWNTeW1ib2xSZXNvbHZlcikge31cblxuICBjbGVhckNhY2hlKCkgeyB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmNsZWFyQ2FjaGUoKTsgfVxuXG4gIGFuYWx5emVNb2R1bGVzU3luYyhyb290RmlsZXM6IHN0cmluZ1tdKTogTmdBbmFseXplZE1vZHVsZXMge1xuICAgIGNvbnN0IGFuYWx5emVSZXN1bHQgPSBhbmFseXplQW5kVmFsaWRhdGVOZ01vZHVsZXMoXG4gICAgICAgIHJvb3RGaWxlcywgdGhpcy5faG9zdCwgdGhpcy5fc3ltYm9sUmVzb2x2ZXIsIHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIpO1xuICAgIGFuYWx5emVSZXN1bHQubmdNb2R1bGVzLmZvckVhY2goXG4gICAgICAgIG5nTW9kdWxlID0+IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIubG9hZE5nTW9kdWxlRGlyZWN0aXZlQW5kUGlwZU1ldGFkYXRhKFxuICAgICAgICAgICAgbmdNb2R1bGUudHlwZS5yZWZlcmVuY2UsIHRydWUpKTtcbiAgICByZXR1cm4gYW5hbHl6ZVJlc3VsdDtcbiAgfVxuXG4gIGFuYWx5emVNb2R1bGVzQXN5bmMocm9vdEZpbGVzOiBzdHJpbmdbXSk6IFByb21pc2U8TmdBbmFseXplZE1vZHVsZXM+IHtcbiAgICBjb25zdCBhbmFseXplUmVzdWx0ID0gYW5hbHl6ZUFuZFZhbGlkYXRlTmdNb2R1bGVzKFxuICAgICAgICByb290RmlsZXMsIHRoaXMuX2hvc3QsIHRoaXMuX3N5bWJvbFJlc29sdmVyLCB0aGlzLl9tZXRhZGF0YVJlc29sdmVyKTtcbiAgICByZXR1cm4gUHJvbWlzZVxuICAgICAgICAuYWxsKGFuYWx5emVSZXN1bHQubmdNb2R1bGVzLm1hcChcbiAgICAgICAgICAgIG5nTW9kdWxlID0+IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIubG9hZE5nTW9kdWxlRGlyZWN0aXZlQW5kUGlwZU1ldGFkYXRhKFxuICAgICAgICAgICAgICAgIG5nTW9kdWxlLnR5cGUucmVmZXJlbmNlLCBmYWxzZSkpKVxuICAgICAgICAudGhlbigoKSA9PiBhbmFseXplUmVzdWx0KTtcbiAgfVxuXG4gIHByaXZhdGUgX2FuYWx5emVGaWxlKGZpbGVOYW1lOiBzdHJpbmcpOiBOZ0FuYWx5emVkRmlsZSB7XG4gICAgbGV0IGFuYWx5emVkRmlsZSA9IHRoaXMuX2FuYWx5emVkRmlsZXMuZ2V0KGZpbGVOYW1lKTtcbiAgICBpZiAoIWFuYWx5emVkRmlsZSkge1xuICAgICAgYW5hbHl6ZWRGaWxlID1cbiAgICAgICAgICBhbmFseXplRmlsZSh0aGlzLl9ob3N0LCB0aGlzLl9zeW1ib2xSZXNvbHZlciwgdGhpcy5fbWV0YWRhdGFSZXNvbHZlciwgZmlsZU5hbWUpO1xuICAgICAgdGhpcy5fYW5hbHl6ZWRGaWxlcy5zZXQoZmlsZU5hbWUsIGFuYWx5emVkRmlsZSk7XG4gICAgfVxuICAgIHJldHVybiBhbmFseXplZEZpbGU7XG4gIH1cblxuICBwcml2YXRlIF9hbmFseXplRmlsZUZvckluamVjdGFibGVzKGZpbGVOYW1lOiBzdHJpbmcpOiBOZ0FuYWx5emVkRmlsZVdpdGhJbmplY3RhYmxlcyB7XG4gICAgbGV0IGFuYWx5emVkRmlsZSA9IHRoaXMuX2FuYWx5emVkRmlsZXNGb3JJbmplY3RhYmxlcy5nZXQoZmlsZU5hbWUpO1xuICAgIGlmICghYW5hbHl6ZWRGaWxlKSB7XG4gICAgICBhbmFseXplZEZpbGUgPSBhbmFseXplRmlsZUZvckluamVjdGFibGVzKFxuICAgICAgICAgIHRoaXMuX2hvc3QsIHRoaXMuX3N5bWJvbFJlc29sdmVyLCB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLCBmaWxlTmFtZSk7XG4gICAgICB0aGlzLl9hbmFseXplZEZpbGVzRm9ySW5qZWN0YWJsZXMuc2V0KGZpbGVOYW1lLCBhbmFseXplZEZpbGUpO1xuICAgIH1cbiAgICByZXR1cm4gYW5hbHl6ZWRGaWxlO1xuICB9XG5cbiAgZmluZEdlbmVyYXRlZEZpbGVOYW1lcyhmaWxlTmFtZTogc3RyaW5nKTogc3RyaW5nW10ge1xuICAgIGNvbnN0IGdlbkZpbGVOYW1lczogc3RyaW5nW10gPSBbXTtcbiAgICBjb25zdCBmaWxlID0gdGhpcy5fYW5hbHl6ZUZpbGUoZmlsZU5hbWUpO1xuICAgIC8vIE1ha2Ugc3VyZSB3ZSBjcmVhdGUgYSAubmdmYWN0b3J5IGlmIHdlIGhhdmUgYSBpbmplY3RhYmxlL2RpcmVjdGl2ZS9waXBlL05nTW9kdWxlXG4gICAgLy8gb3IgYSByZWZlcmVuY2UgdG8gYSBub24gc291cmNlIGZpbGUuXG4gICAgLy8gTm90ZTogVGhpcyBpcyBvdmVyZXN0aW1hdGluZyB0aGUgcmVxdWlyZWQgLm5nZmFjdG9yeSBmaWxlcyBhcyB0aGUgcmVhbCBjYWxjdWxhdGlvbiBpcyBoYXJkZXIuXG4gICAgLy8gT25seSBkbyB0aGlzIGZvciBTdHViRW1pdEZsYWdzLkJhc2ljLCBhcyBhZGRpbmcgYSB0eXBlIGNoZWNrIGJsb2NrXG4gICAgLy8gZG9lcyBub3QgY2hhbmdlIHRoaXMgZmlsZSAoYXMgd2UgZ2VuZXJhdGUgdHlwZSBjaGVjayBibG9ja3MgYmFzZWQgb24gTmdNb2R1bGVzKS5cbiAgICBpZiAodGhpcy5fb3B0aW9ucy5hbGxvd0VtcHR5Q29kZWdlbkZpbGVzIHx8IGZpbGUuZGlyZWN0aXZlcy5sZW5ndGggfHwgZmlsZS5waXBlcy5sZW5ndGggfHxcbiAgICAgICAgZmlsZS5pbmplY3RhYmxlcy5sZW5ndGggfHwgZmlsZS5uZ01vZHVsZXMubGVuZ3RoIHx8IGZpbGUuZXhwb3J0c05vblNvdXJjZUZpbGVzKSB7XG4gICAgICBnZW5GaWxlTmFtZXMucHVzaChuZ2ZhY3RvcnlGaWxlUGF0aChmaWxlLmZpbGVOYW1lLCB0cnVlKSk7XG4gICAgICBpZiAodGhpcy5fb3B0aW9ucy5lbmFibGVTdW1tYXJpZXNGb3JKaXQpIHtcbiAgICAgICAgZ2VuRmlsZU5hbWVzLnB1c2goc3VtbWFyeUZvckppdEZpbGVOYW1lKGZpbGUuZmlsZU5hbWUsIHRydWUpKTtcbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3QgZmlsZVN1ZmZpeCA9IG5vcm1hbGl6ZUdlbkZpbGVTdWZmaXgoc3BsaXRUeXBlc2NyaXB0U3VmZml4KGZpbGUuZmlsZU5hbWUsIHRydWUpWzFdKTtcbiAgICBmaWxlLmRpcmVjdGl2ZXMuZm9yRWFjaCgoZGlyU3ltYm9sKSA9PiB7XG4gICAgICBjb25zdCBjb21wTWV0YSA9XG4gICAgICAgICAgdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXROb25Ob3JtYWxpemVkRGlyZWN0aXZlTWV0YWRhdGEoZGlyU3ltYm9sKSAhLm1ldGFkYXRhO1xuICAgICAgaWYgKCFjb21wTWV0YS5pc0NvbXBvbmVudCkge1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgICAvLyBOb3RlOiBjb21wTWV0YSBpcyBhIGNvbXBvbmVudCBhbmQgdGhlcmVmb3JlIHRlbXBsYXRlIGlzIG5vbiBudWxsLlxuICAgICAgY29tcE1ldGEudGVtcGxhdGUgIS5zdHlsZVVybHMuZm9yRWFjaCgoc3R5bGVVcmwpID0+IHtcbiAgICAgICAgY29uc3Qgbm9ybWFsaXplZFVybCA9IHRoaXMuX2hvc3QucmVzb3VyY2VOYW1lVG9GaWxlTmFtZShzdHlsZVVybCwgZmlsZS5maWxlTmFtZSk7XG4gICAgICAgIGlmICghbm9ybWFsaXplZFVybCkge1xuICAgICAgICAgIHRocm93IHN5bnRheEVycm9yKGBDb3VsZG4ndCByZXNvbHZlIHJlc291cmNlICR7c3R5bGVVcmx9IHJlbGF0aXZlIHRvICR7ZmlsZS5maWxlTmFtZX1gKTtcbiAgICAgICAgfVxuICAgICAgICBjb25zdCBuZWVkc1NoaW0gPSAoY29tcE1ldGEudGVtcGxhdGUgIS5lbmNhcHN1bGF0aW9uIHx8XG4gICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLl9jb25maWcuZGVmYXVsdEVuY2Fwc3VsYXRpb24pID09PSBWaWV3RW5jYXBzdWxhdGlvbi5FbXVsYXRlZDtcbiAgICAgICAgZ2VuRmlsZU5hbWVzLnB1c2goX3N0eWxlc01vZHVsZVVybChub3JtYWxpemVkVXJsLCBuZWVkc1NoaW0sIGZpbGVTdWZmaXgpKTtcbiAgICAgICAgaWYgKHRoaXMuX29wdGlvbnMuYWxsb3dFbXB0eUNvZGVnZW5GaWxlcykge1xuICAgICAgICAgIGdlbkZpbGVOYW1lcy5wdXNoKF9zdHlsZXNNb2R1bGVVcmwobm9ybWFsaXplZFVybCwgIW5lZWRzU2hpbSwgZmlsZVN1ZmZpeCkpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9KTtcbiAgICByZXR1cm4gZ2VuRmlsZU5hbWVzO1xuICB9XG5cbiAgZW1pdEJhc2ljU3R1YihnZW5GaWxlTmFtZTogc3RyaW5nLCBvcmlnaW5hbEZpbGVOYW1lPzogc3RyaW5nKTogR2VuZXJhdGVkRmlsZSB7XG4gICAgY29uc3Qgb3V0cHV0Q3R4ID0gdGhpcy5fY3JlYXRlT3V0cHV0Q29udGV4dChnZW5GaWxlTmFtZSk7XG4gICAgaWYgKGdlbkZpbGVOYW1lLmVuZHNXaXRoKCcubmdmYWN0b3J5LnRzJykpIHtcbiAgICAgIGlmICghb3JpZ2luYWxGaWxlTmFtZSkge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgICBgQXNzZXJ0aW9uIGVycm9yOiByZXF1aXJlIHRoZSBvcmlnaW5hbCBmaWxlIGZvciAubmdmYWN0b3J5LnRzIHN0dWJzLiBGaWxlOiAke2dlbkZpbGVOYW1lfWApO1xuICAgICAgfVxuICAgICAgY29uc3Qgb3JpZ2luYWxGaWxlID0gdGhpcy5fYW5hbHl6ZUZpbGUob3JpZ2luYWxGaWxlTmFtZSk7XG4gICAgICB0aGlzLl9jcmVhdGVOZ0ZhY3RvcnlTdHViKG91dHB1dEN0eCwgb3JpZ2luYWxGaWxlLCBTdHViRW1pdEZsYWdzLkJhc2ljKTtcbiAgICB9IGVsc2UgaWYgKGdlbkZpbGVOYW1lLmVuZHNXaXRoKCcubmdzdW1tYXJ5LnRzJykpIHtcbiAgICAgIGlmICh0aGlzLl9vcHRpb25zLmVuYWJsZVN1bW1hcmllc0ZvckppdCkge1xuICAgICAgICBpZiAoIW9yaWdpbmFsRmlsZU5hbWUpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgICAgIGBBc3NlcnRpb24gZXJyb3I6IHJlcXVpcmUgdGhlIG9yaWdpbmFsIGZpbGUgZm9yIC5uZ3N1bW1hcnkudHMgc3R1YnMuIEZpbGU6ICR7Z2VuRmlsZU5hbWV9YCk7XG4gICAgICAgIH1cbiAgICAgICAgY29uc3Qgb3JpZ2luYWxGaWxlID0gdGhpcy5fYW5hbHl6ZUZpbGUob3JpZ2luYWxGaWxlTmFtZSk7XG4gICAgICAgIF9jcmVhdGVFbXB0eVN0dWIob3V0cHV0Q3R4KTtcbiAgICAgICAgb3JpZ2luYWxGaWxlLm5nTW9kdWxlcy5mb3JFYWNoKG5nTW9kdWxlID0+IHtcbiAgICAgICAgICAvLyBjcmVhdGUgZXhwb3J0cyB0aGF0IHVzZXIgY29kZSBjYW4gcmVmZXJlbmNlXG4gICAgICAgICAgY3JlYXRlRm9ySml0U3R1YihvdXRwdXRDdHgsIG5nTW9kdWxlLnR5cGUucmVmZXJlbmNlKTtcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgfSBlbHNlIGlmIChnZW5GaWxlTmFtZS5lbmRzV2l0aCgnLm5nc3R5bGUudHMnKSkge1xuICAgICAgX2NyZWF0ZUVtcHR5U3R1YihvdXRwdXRDdHgpO1xuICAgIH1cbiAgICAvLyBOb3RlOiBmb3IgdGhlIHN0dWJzLCB3ZSBkb24ndCBuZWVkIGEgcHJvcGVydHkgc3JjRmlsZVVybCxcbiAgICAvLyBhcyBsYXRlciBvbiBpbiBlbWl0QWxsSW1wbHMgd2Ugd2lsbCBjcmVhdGUgdGhlIHByb3BlciBHZW5lcmF0ZWRGaWxlcyB3aXRoIHRoZVxuICAgIC8vIGNvcnJlY3Qgc3JjRmlsZVVybC5cbiAgICAvLyBUaGlzIGlzIGdvb2QgYXMgZS5nLiBmb3IgLm5nc3R5bGUudHMgZmlsZXMgd2UgY2FuJ3QgZGVyaXZlXG4gICAgLy8gdGhlIHVybCBvZiBjb21wb25lbnRzIGJhc2VkIG9uIHRoZSBnZW5GaWxlVXJsLlxuICAgIHJldHVybiB0aGlzLl9jb2RlZ2VuU291cmNlTW9kdWxlKCd1bmtub3duJywgb3V0cHV0Q3R4KTtcbiAgfVxuXG4gIGVtaXRUeXBlQ2hlY2tTdHViKGdlbkZpbGVOYW1lOiBzdHJpbmcsIG9yaWdpbmFsRmlsZU5hbWU6IHN0cmluZyk6IEdlbmVyYXRlZEZpbGV8bnVsbCB7XG4gICAgY29uc3Qgb3JpZ2luYWxGaWxlID0gdGhpcy5fYW5hbHl6ZUZpbGUob3JpZ2luYWxGaWxlTmFtZSk7XG4gICAgY29uc3Qgb3V0cHV0Q3R4ID0gdGhpcy5fY3JlYXRlT3V0cHV0Q29udGV4dChnZW5GaWxlTmFtZSk7XG4gICAgaWYgKGdlbkZpbGVOYW1lLmVuZHNXaXRoKCcubmdmYWN0b3J5LnRzJykpIHtcbiAgICAgIHRoaXMuX2NyZWF0ZU5nRmFjdG9yeVN0dWIob3V0cHV0Q3R4LCBvcmlnaW5hbEZpbGUsIFN0dWJFbWl0RmxhZ3MuVHlwZUNoZWNrKTtcbiAgICB9XG4gICAgcmV0dXJuIG91dHB1dEN0eC5zdGF0ZW1lbnRzLmxlbmd0aCA+IDAgP1xuICAgICAgICB0aGlzLl9jb2RlZ2VuU291cmNlTW9kdWxlKG9yaWdpbmFsRmlsZS5maWxlTmFtZSwgb3V0cHV0Q3R4KSA6XG4gICAgICAgIG51bGw7XG4gIH1cblxuICBsb2FkRmlsZXNBc3luYyhmaWxlTmFtZXM6IHN0cmluZ1tdLCB0c0ZpbGVzOiBzdHJpbmdbXSk6IFByb21pc2U8XG4gICAgICB7YW5hbHl6ZWRNb2R1bGVzOiBOZ0FuYWx5emVkTW9kdWxlcywgYW5hbHl6ZWRJbmplY3RhYmxlczogTmdBbmFseXplZEZpbGVXaXRoSW5qZWN0YWJsZXNbXX0+IHtcbiAgICBjb25zdCBmaWxlcyA9IGZpbGVOYW1lcy5tYXAoZmlsZU5hbWUgPT4gdGhpcy5fYW5hbHl6ZUZpbGUoZmlsZU5hbWUpKTtcbiAgICBjb25zdCBsb2FkaW5nUHJvbWlzZXM6IFByb21pc2U8TmdBbmFseXplZE1vZHVsZXM+W10gPSBbXTtcbiAgICBmaWxlcy5mb3JFYWNoKFxuICAgICAgICBmaWxlID0+IGZpbGUubmdNb2R1bGVzLmZvckVhY2goXG4gICAgICAgICAgICBuZ01vZHVsZSA9PlxuICAgICAgICAgICAgICAgIGxvYWRpbmdQcm9taXNlcy5wdXNoKHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIubG9hZE5nTW9kdWxlRGlyZWN0aXZlQW5kUGlwZU1ldGFkYXRhKFxuICAgICAgICAgICAgICAgICAgICBuZ01vZHVsZS50eXBlLnJlZmVyZW5jZSwgZmFsc2UpKSkpO1xuICAgIGNvbnN0IGFuYWx5emVkSW5qZWN0YWJsZXMgPSB0c0ZpbGVzLm1hcCh0c0ZpbGUgPT4gdGhpcy5fYW5hbHl6ZUZpbGVGb3JJbmplY3RhYmxlcyh0c0ZpbGUpKTtcbiAgICByZXR1cm4gUHJvbWlzZS5hbGwobG9hZGluZ1Byb21pc2VzKS50aGVuKF8gPT4gKHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYW5hbHl6ZWRNb2R1bGVzOiBtZXJnZUFuZFZhbGlkYXRlTmdGaWxlcyhmaWxlcyksXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGFuYWx5emVkSW5qZWN0YWJsZXM6IGFuYWx5emVkSW5qZWN0YWJsZXMsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KSk7XG4gIH1cblxuICBsb2FkRmlsZXNTeW5jKGZpbGVOYW1lczogc3RyaW5nW10sIHRzRmlsZXM6IHN0cmluZ1tdKTpcbiAgICAgIHthbmFseXplZE1vZHVsZXM6IE5nQW5hbHl6ZWRNb2R1bGVzLCBhbmFseXplZEluamVjdGFibGVzOiBOZ0FuYWx5emVkRmlsZVdpdGhJbmplY3RhYmxlc1tdfSB7XG4gICAgY29uc3QgZmlsZXMgPSBmaWxlTmFtZXMubWFwKGZpbGVOYW1lID0+IHRoaXMuX2FuYWx5emVGaWxlKGZpbGVOYW1lKSk7XG4gICAgZmlsZXMuZm9yRWFjaChcbiAgICAgICAgZmlsZSA9PiBmaWxlLm5nTW9kdWxlcy5mb3JFYWNoKFxuICAgICAgICAgICAgbmdNb2R1bGUgPT4gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5sb2FkTmdNb2R1bGVEaXJlY3RpdmVBbmRQaXBlTWV0YWRhdGEoXG4gICAgICAgICAgICAgICAgbmdNb2R1bGUudHlwZS5yZWZlcmVuY2UsIHRydWUpKSk7XG4gICAgY29uc3QgYW5hbHl6ZWRJbmplY3RhYmxlcyA9IHRzRmlsZXMubWFwKHRzRmlsZSA9PiB0aGlzLl9hbmFseXplRmlsZUZvckluamVjdGFibGVzKHRzRmlsZSkpO1xuICAgIHJldHVybiB7XG4gICAgICBhbmFseXplZE1vZHVsZXM6IG1lcmdlQW5kVmFsaWRhdGVOZ0ZpbGVzKGZpbGVzKSxcbiAgICAgIGFuYWx5emVkSW5qZWN0YWJsZXM6IGFuYWx5emVkSW5qZWN0YWJsZXMsXG4gICAgfTtcbiAgfVxuXG4gIHByaXZhdGUgX2NyZWF0ZU5nRmFjdG9yeVN0dWIoXG4gICAgICBvdXRwdXRDdHg6IE91dHB1dENvbnRleHQsIGZpbGU6IE5nQW5hbHl6ZWRGaWxlLCBlbWl0RmxhZ3M6IFN0dWJFbWl0RmxhZ3MpIHtcbiAgICBsZXQgY29tcG9uZW50SWQgPSAwO1xuICAgIGZpbGUubmdNb2R1bGVzLmZvckVhY2goKG5nTW9kdWxlTWV0YSwgbmdNb2R1bGVJbmRleCkgPT4ge1xuICAgICAgLy8gTm90ZTogdGhlIGNvZGUgYmVsb3cgbmVlZHMgdG8gZXhlY3V0ZWQgZm9yIFN0dWJFbWl0RmxhZ3MuQmFzaWMgYW5kIFN0dWJFbWl0RmxhZ3MuVHlwZUNoZWNrLFxuICAgICAgLy8gc28gd2UgZG9uJ3QgY2hhbmdlIHRoZSAubmdmYWN0b3J5IGZpbGUgdG9vIG11Y2ggd2hlbiBhZGRpbmcgdGhlIHR5cGUtY2hlY2sgYmxvY2suXG5cbiAgICAgIC8vIGNyZWF0ZSBleHBvcnRzIHRoYXQgdXNlciBjb2RlIGNhbiByZWZlcmVuY2VcbiAgICAgIHRoaXMuX25nTW9kdWxlQ29tcGlsZXIuY3JlYXRlU3R1YihvdXRwdXRDdHgsIG5nTW9kdWxlTWV0YS50eXBlLnJlZmVyZW5jZSk7XG5cbiAgICAgIC8vIGFkZCByZWZlcmVuY2VzIHRvIHRoZSBzeW1ib2xzIGZyb20gdGhlIG1ldGFkYXRhLlxuICAgICAgLy8gVGhlc2UgY2FuIGJlIHVzZWQgYnkgdGhlIHR5cGUgY2hlY2sgYmxvY2sgZm9yIGNvbXBvbmVudHMsXG4gICAgICAvLyBhbmQgdGhleSBhbHNvIGNhdXNlIFR5cGVTY3JpcHQgdG8gaW5jbHVkZSB0aGVzZSBmaWxlcyBpbnRvIHRoZSBwcm9ncmFtIHRvbyxcbiAgICAgIC8vIHdoaWNoIHdpbGwgbWFrZSB0aGVtIHBhcnQgb2YgdGhlIGFuYWx5emVkRmlsZXMuXG4gICAgICBjb25zdCBleHRlcm5hbFJlZmVyZW5jZXM6IFN0YXRpY1N5bWJvbFtdID0gW1xuICAgICAgICAvLyBBZGQgcmVmZXJlbmNlcyB0aGF0IGFyZSBhdmFpbGFibGUgZnJvbSBhbGwgdGhlIG1vZHVsZXMgYW5kIGltcG9ydHMuXG4gICAgICAgIC4uLm5nTW9kdWxlTWV0YS50cmFuc2l0aXZlTW9kdWxlLmRpcmVjdGl2ZXMubWFwKGQgPT4gZC5yZWZlcmVuY2UpLFxuICAgICAgICAuLi5uZ01vZHVsZU1ldGEudHJhbnNpdGl2ZU1vZHVsZS5waXBlcy5tYXAoZCA9PiBkLnJlZmVyZW5jZSksXG4gICAgICAgIC4uLm5nTW9kdWxlTWV0YS5pbXBvcnRlZE1vZHVsZXMubWFwKG0gPT4gbS50eXBlLnJlZmVyZW5jZSksXG4gICAgICAgIC4uLm5nTW9kdWxlTWV0YS5leHBvcnRlZE1vZHVsZXMubWFwKG0gPT4gbS50eXBlLnJlZmVyZW5jZSksXG5cbiAgICAgICAgLy8gQWRkIHJlZmVyZW5jZXMgdGhhdCBtaWdodCBiZSBpbnNlcnRlZCBieSB0aGUgdGVtcGxhdGUgY29tcGlsZXIuXG4gICAgICAgIC4uLnRoaXMuX2V4dGVybmFsSWRlbnRpZmllclJlZmVyZW5jZXMoW0lkZW50aWZpZXJzLlRlbXBsYXRlUmVmLCBJZGVudGlmaWVycy5FbGVtZW50UmVmXSksXG4gICAgICBdO1xuXG4gICAgICBjb25zdCBleHRlcm5hbFJlZmVyZW5jZVZhcnMgPSBuZXcgTWFwPGFueSwgc3RyaW5nPigpO1xuICAgICAgZXh0ZXJuYWxSZWZlcmVuY2VzLmZvckVhY2goKHJlZiwgdHlwZUluZGV4KSA9PiB7XG4gICAgICAgIGV4dGVybmFsUmVmZXJlbmNlVmFycy5zZXQocmVmLCBgX2RlY2wke25nTW9kdWxlSW5kZXh9XyR7dHlwZUluZGV4fWApO1xuICAgICAgfSk7XG4gICAgICBleHRlcm5hbFJlZmVyZW5jZVZhcnMuZm9yRWFjaCgodmFyTmFtZSwgcmVmZXJlbmNlKSA9PiB7XG4gICAgICAgIG91dHB1dEN0eC5zdGF0ZW1lbnRzLnB1c2goXG4gICAgICAgICAgICBvLnZhcmlhYmxlKHZhck5hbWUpXG4gICAgICAgICAgICAgICAgLnNldChvLk5VTExfRVhQUi5jYXN0KG8uRFlOQU1JQ19UWVBFKSlcbiAgICAgICAgICAgICAgICAudG9EZWNsU3RtdChvLmV4cHJlc3Npb25UeXBlKG91dHB1dEN0eC5pbXBvcnRFeHByKFxuICAgICAgICAgICAgICAgICAgICByZWZlcmVuY2UsIC8qIHR5cGVQYXJhbXMgKi8gbnVsbCwgLyogdXNlU3VtbWFyaWVzICovIGZhbHNlKSkpKTtcbiAgICAgIH0pO1xuXG4gICAgICBpZiAoZW1pdEZsYWdzICYgU3R1YkVtaXRGbGFncy5UeXBlQ2hlY2spIHtcbiAgICAgICAgLy8gYWRkIHRoZSB0eXBlLWNoZWNrIGJsb2NrIGZvciBhbGwgY29tcG9uZW50cyBvZiB0aGUgTmdNb2R1bGVcbiAgICAgICAgbmdNb2R1bGVNZXRhLmRlY2xhcmVkRGlyZWN0aXZlcy5mb3JFYWNoKChkaXJJZCkgPT4ge1xuICAgICAgICAgIGNvbnN0IGNvbXBNZXRhID0gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVNZXRhZGF0YShkaXJJZC5yZWZlcmVuY2UpO1xuICAgICAgICAgIGlmICghY29tcE1ldGEuaXNDb21wb25lbnQpIHtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICB9XG4gICAgICAgICAgY29tcG9uZW50SWQrKztcbiAgICAgICAgICB0aGlzLl9jcmVhdGVUeXBlQ2hlY2tCbG9jayhcbiAgICAgICAgICAgICAgb3V0cHV0Q3R4LCBgJHtjb21wTWV0YS50eXBlLnJlZmVyZW5jZS5uYW1lfV9Ib3N0XyR7Y29tcG9uZW50SWR9YCwgbmdNb2R1bGVNZXRhLFxuICAgICAgICAgICAgICB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldEhvc3RDb21wb25lbnRNZXRhZGF0YShjb21wTWV0YSksIFtjb21wTWV0YS50eXBlXSxcbiAgICAgICAgICAgICAgZXh0ZXJuYWxSZWZlcmVuY2VWYXJzKTtcbiAgICAgICAgICB0aGlzLl9jcmVhdGVUeXBlQ2hlY2tCbG9jayhcbiAgICAgICAgICAgICAgb3V0cHV0Q3R4LCBgJHtjb21wTWV0YS50eXBlLnJlZmVyZW5jZS5uYW1lfV8ke2NvbXBvbmVudElkfWAsIG5nTW9kdWxlTWV0YSwgY29tcE1ldGEsXG4gICAgICAgICAgICAgIG5nTW9kdWxlTWV0YS50cmFuc2l0aXZlTW9kdWxlLmRpcmVjdGl2ZXMsIGV4dGVybmFsUmVmZXJlbmNlVmFycyk7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgaWYgKG91dHB1dEN0eC5zdGF0ZW1lbnRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgX2NyZWF0ZUVtcHR5U3R1YihvdXRwdXRDdHgpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgX2V4dGVybmFsSWRlbnRpZmllclJlZmVyZW5jZXMocmVmZXJlbmNlczogby5FeHRlcm5hbFJlZmVyZW5jZVtdKTogU3RhdGljU3ltYm9sW10ge1xuICAgIGNvbnN0IHJlc3VsdDogU3RhdGljU3ltYm9sW10gPSBbXTtcbiAgICBmb3IgKGxldCByZWZlcmVuY2Ugb2YgcmVmZXJlbmNlcykge1xuICAgICAgY29uc3QgdG9rZW4gPSBjcmVhdGVUb2tlbkZvckV4dGVybmFsUmVmZXJlbmNlKHRoaXMucmVmbGVjdG9yLCByZWZlcmVuY2UpO1xuICAgICAgaWYgKHRva2VuLmlkZW50aWZpZXIpIHtcbiAgICAgICAgcmVzdWx0LnB1c2godG9rZW4uaWRlbnRpZmllci5yZWZlcmVuY2UpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBfY3JlYXRlVHlwZUNoZWNrQmxvY2soXG4gICAgICBjdHg6IE91dHB1dENvbnRleHQsIGNvbXBvbmVudElkOiBzdHJpbmcsIG1vZHVsZU1ldGE6IENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhLFxuICAgICAgY29tcE1ldGE6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgZGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdLFxuICAgICAgZXh0ZXJuYWxSZWZlcmVuY2VWYXJzOiBNYXA8YW55LCBzdHJpbmc+KSB7XG4gICAgY29uc3Qge3RlbXBsYXRlOiBwYXJzZWRUZW1wbGF0ZSwgcGlwZXM6IHVzZWRQaXBlc30gPVxuICAgICAgICB0aGlzLl9wYXJzZVRlbXBsYXRlKGNvbXBNZXRhLCBtb2R1bGVNZXRhLCBkaXJlY3RpdmVzKTtcbiAgICBjdHguc3RhdGVtZW50cy5wdXNoKC4uLnRoaXMuX3R5cGVDaGVja0NvbXBpbGVyLmNvbXBpbGVDb21wb25lbnQoXG4gICAgICAgIGNvbXBvbmVudElkLCBjb21wTWV0YSwgcGFyc2VkVGVtcGxhdGUsIHVzZWRQaXBlcywgZXh0ZXJuYWxSZWZlcmVuY2VWYXJzLCBjdHgpKTtcbiAgfVxuXG4gIGVtaXRNZXNzYWdlQnVuZGxlKGFuYWx5emVSZXN1bHQ6IE5nQW5hbHl6ZWRNb2R1bGVzLCBsb2NhbGU6IHN0cmluZ3xudWxsKTogTWVzc2FnZUJ1bmRsZSB7XG4gICAgY29uc3QgZXJyb3JzOiBQYXJzZUVycm9yW10gPSBbXTtcbiAgICBjb25zdCBodG1sUGFyc2VyID0gbmV3IEh0bWxQYXJzZXIoKTtcblxuICAgIC8vIFRPRE8odmljYik6IGltcGxpY2l0IHRhZ3MgJiBhdHRyaWJ1dGVzXG4gICAgY29uc3QgbWVzc2FnZUJ1bmRsZSA9IG5ldyBNZXNzYWdlQnVuZGxlKGh0bWxQYXJzZXIsIFtdLCB7fSwgbG9jYWxlKTtcblxuICAgIGFuYWx5emVSZXN1bHQuZmlsZXMuZm9yRWFjaChmaWxlID0+IHtcbiAgICAgIGNvbnN0IGNvbXBNZXRhczogQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhW10gPSBbXTtcbiAgICAgIGZpbGUuZGlyZWN0aXZlcy5mb3JFYWNoKGRpcmVjdGl2ZVR5cGUgPT4ge1xuICAgICAgICBjb25zdCBkaXJNZXRhID0gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVNZXRhZGF0YShkaXJlY3RpdmVUeXBlKTtcbiAgICAgICAgaWYgKGRpck1ldGEgJiYgZGlyTWV0YS5pc0NvbXBvbmVudCkge1xuICAgICAgICAgIGNvbXBNZXRhcy5wdXNoKGRpck1ldGEpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICAgIGNvbXBNZXRhcy5mb3JFYWNoKGNvbXBNZXRhID0+IHtcbiAgICAgICAgY29uc3QgaHRtbCA9IGNvbXBNZXRhLnRlbXBsYXRlICEudGVtcGxhdGUgITtcbiAgICAgICAgLy8gVGVtcGxhdGUgVVJMIHBvaW50cyB0byBlaXRoZXIgYW4gSFRNTCBvciBUUyBmaWxlIGRlcGVuZGluZyBvbiB3aGV0aGVyXG4gICAgICAgIC8vIHRoZSBmaWxlIGlzIHVzZWQgd2l0aCBgdGVtcGxhdGVVcmw6YCBvciBgdGVtcGxhdGU6YCwgcmVzcGVjdGl2ZWx5LlxuICAgICAgICBjb25zdCB0ZW1wbGF0ZVVybCA9IGNvbXBNZXRhLnRlbXBsYXRlICEudGVtcGxhdGVVcmwgITtcbiAgICAgICAgY29uc3QgaW50ZXJwb2xhdGlvbkNvbmZpZyA9XG4gICAgICAgICAgICBJbnRlcnBvbGF0aW9uQ29uZmlnLmZyb21BcnJheShjb21wTWV0YS50ZW1wbGF0ZSAhLmludGVycG9sYXRpb24pO1xuICAgICAgICBlcnJvcnMucHVzaCguLi5tZXNzYWdlQnVuZGxlLnVwZGF0ZUZyb21UZW1wbGF0ZShodG1sLCB0ZW1wbGF0ZVVybCwgaW50ZXJwb2xhdGlvbkNvbmZpZykgISk7XG4gICAgICB9KTtcbiAgICB9KTtcblxuICAgIGlmIChlcnJvcnMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoZXJyb3JzLm1hcChlID0+IGUudG9TdHJpbmcoKSkuam9pbignXFxuJykpO1xuICAgIH1cblxuICAgIHJldHVybiBtZXNzYWdlQnVuZGxlO1xuICB9XG5cbiAgZW1pdEFsbFBhcnRpYWxNb2R1bGVzKFxuICAgICAge25nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmUsIGZpbGVzfTogTmdBbmFseXplZE1vZHVsZXMsXG4gICAgICByM0ZpbGVzOiBOZ0FuYWx5emVkRmlsZVdpdGhJbmplY3RhYmxlc1tdKTogUGFydGlhbE1vZHVsZVtdIHtcbiAgICBjb25zdCBjb250ZXh0TWFwID0gbmV3IE1hcDxzdHJpbmcsIE91dHB1dENvbnRleHQ+KCk7XG5cbiAgICBjb25zdCBnZXRDb250ZXh0ID0gKGZpbGVOYW1lOiBzdHJpbmcpOiBPdXRwdXRDb250ZXh0ID0+IHtcbiAgICAgIGlmICghY29udGV4dE1hcC5oYXMoZmlsZU5hbWUpKSB7XG4gICAgICAgIGNvbnRleHRNYXAuc2V0KGZpbGVOYW1lLCB0aGlzLl9jcmVhdGVPdXRwdXRDb250ZXh0KGZpbGVOYW1lKSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gY29udGV4dE1hcC5nZXQoZmlsZU5hbWUpICE7XG4gICAgfTtcblxuICAgIGZpbGVzLmZvckVhY2goXG4gICAgICAgIGZpbGUgPT4gdGhpcy5fY29tcGlsZVBhcnRpYWxNb2R1bGUoXG4gICAgICAgICAgICBmaWxlLmZpbGVOYW1lLCBuZ01vZHVsZUJ5UGlwZU9yRGlyZWN0aXZlLCBmaWxlLmRpcmVjdGl2ZXMsIGZpbGUucGlwZXMsIGZpbGUubmdNb2R1bGVzLFxuICAgICAgICAgICAgZmlsZS5pbmplY3RhYmxlcywgZ2V0Q29udGV4dChmaWxlLmZpbGVOYW1lKSkpO1xuICAgIHIzRmlsZXMuZm9yRWFjaChcbiAgICAgICAgZmlsZSA9PiB0aGlzLl9jb21waWxlU2hhbGxvd01vZHVsZXMoXG4gICAgICAgICAgICBmaWxlLmZpbGVOYW1lLCBmaWxlLnNoYWxsb3dNb2R1bGVzLCBnZXRDb250ZXh0KGZpbGUuZmlsZU5hbWUpKSk7XG5cbiAgICByZXR1cm4gQXJyYXkuZnJvbShjb250ZXh0TWFwLnZhbHVlcygpKVxuICAgICAgICAubWFwKGNvbnRleHQgPT4gKHtcbiAgICAgICAgICAgICAgIGZpbGVOYW1lOiBjb250ZXh0LmdlbkZpbGVQYXRoLFxuICAgICAgICAgICAgICAgc3RhdGVtZW50czogWy4uLmNvbnRleHQuY29uc3RhbnRQb29sLnN0YXRlbWVudHMsIC4uLmNvbnRleHQuc3RhdGVtZW50c10sXG4gICAgICAgICAgICAgfSkpO1xuICB9XG5cbiAgcHJpdmF0ZSBfY29tcGlsZVNoYWxsb3dNb2R1bGVzKFxuICAgICAgZmlsZU5hbWU6IHN0cmluZywgc2hhbGxvd01vZHVsZXM6IENvbXBpbGVTaGFsbG93TW9kdWxlTWV0YWRhdGFbXSxcbiAgICAgIGNvbnRleHQ6IE91dHB1dENvbnRleHQpOiB2b2lkIHtcbiAgICBzaGFsbG93TW9kdWxlcy5mb3JFYWNoKG1vZHVsZSA9PiBjb21waWxlUjNNb2R1bGUoY29udGV4dCwgbW9kdWxlLCB0aGlzLl9pbmplY3RhYmxlQ29tcGlsZXIpKTtcbiAgfVxuXG4gIHByaXZhdGUgX2NvbXBpbGVQYXJ0aWFsTW9kdWxlKFxuICAgICAgZmlsZU5hbWU6IHN0cmluZywgbmdNb2R1bGVCeVBpcGVPckRpcmVjdGl2ZTogTWFwPFN0YXRpY1N5bWJvbCwgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGE+LFxuICAgICAgZGlyZWN0aXZlczogU3RhdGljU3ltYm9sW10sIHBpcGVzOiBTdGF0aWNTeW1ib2xbXSwgbmdNb2R1bGVzOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YVtdLFxuICAgICAgaW5qZWN0YWJsZXM6IENvbXBpbGVJbmplY3RhYmxlTWV0YWRhdGFbXSwgY29udGV4dDogT3V0cHV0Q29udGV4dCk6IHZvaWQge1xuICAgIGNvbnN0IGVycm9yczogUGFyc2VFcnJvcltdID0gW107XG5cbiAgICBjb25zdCBzY2hlbWFSZWdpc3RyeSA9IG5ldyBEb21FbGVtZW50U2NoZW1hUmVnaXN0cnkoKTtcbiAgICBjb25zdCBob3N0QmluZGluZ1BhcnNlciA9IG5ldyBCaW5kaW5nUGFyc2VyKFxuICAgICAgICB0aGlzLl90ZW1wbGF0ZVBhcnNlci5leHByZXNzaW9uUGFyc2VyLCBERUZBVUxUX0lOVEVSUE9MQVRJT05fQ09ORklHLCBzY2hlbWFSZWdpc3RyeSwgW10sXG4gICAgICAgIGVycm9ycyk7XG5cbiAgICAvLyBQcm9jZXNzIGFsbCBjb21wb25lbnRzIGFuZCBkaXJlY3RpdmVzXG4gICAgZGlyZWN0aXZlcy5mb3JFYWNoKGRpcmVjdGl2ZVR5cGUgPT4ge1xuICAgICAgY29uc3QgZGlyZWN0aXZlTWV0YWRhdGEgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldERpcmVjdGl2ZU1ldGFkYXRhKGRpcmVjdGl2ZVR5cGUpO1xuICAgICAgaWYgKGRpcmVjdGl2ZU1ldGFkYXRhLmlzQ29tcG9uZW50KSB7XG4gICAgICAgIGNvbnN0IG1vZHVsZSA9IG5nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmUuZ2V0KGRpcmVjdGl2ZVR5cGUpICE7XG4gICAgICAgIG1vZHVsZSB8fFxuICAgICAgICAgICAgZXJyb3IoXG4gICAgICAgICAgICAgICAgYENhbm5vdCBkZXRlcm1pbmUgdGhlIG1vZHVsZSBmb3IgY29tcG9uZW50ICcke2lkZW50aWZpZXJOYW1lKGRpcmVjdGl2ZU1ldGFkYXRhLnR5cGUpfSdgKTtcblxuICAgICAgICBsZXQgaHRtbEFzdCA9IGRpcmVjdGl2ZU1ldGFkYXRhLnRlbXBsYXRlICEuaHRtbEFzdCAhO1xuICAgICAgICBjb25zdCBwcmVzZXJ2ZVdoaXRlc3BhY2VzID0gZGlyZWN0aXZlTWV0YWRhdGEgIS50ZW1wbGF0ZSAhLnByZXNlcnZlV2hpdGVzcGFjZXM7XG5cbiAgICAgICAgaWYgKCFwcmVzZXJ2ZVdoaXRlc3BhY2VzKSB7XG4gICAgICAgICAgaHRtbEFzdCA9IHJlbW92ZVdoaXRlc3BhY2VzKGh0bWxBc3QpO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IHJlbmRlcjNBc3QgPSBodG1sQXN0VG9SZW5kZXIzQXN0KGh0bWxBc3Qucm9vdE5vZGVzLCBob3N0QmluZGluZ1BhcnNlcik7XG5cbiAgICAgICAgLy8gTWFwIG9mIFN0YXRpY1R5cGUgYnkgZGlyZWN0aXZlIHNlbGVjdG9yc1xuICAgICAgICBjb25zdCBkaXJlY3RpdmVUeXBlQnlTZWwgPSBuZXcgTWFwPHN0cmluZywgYW55PigpO1xuXG4gICAgICAgIGNvbnN0IGRpcmVjdGl2ZXMgPSBtb2R1bGUudHJhbnNpdGl2ZU1vZHVsZS5kaXJlY3RpdmVzLm1hcChcbiAgICAgICAgICAgIGRpciA9PiB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldERpcmVjdGl2ZVN1bW1hcnkoZGlyLnJlZmVyZW5jZSkpO1xuXG4gICAgICAgIGRpcmVjdGl2ZXMuZm9yRWFjaChkaXJlY3RpdmUgPT4ge1xuICAgICAgICAgIGlmIChkaXJlY3RpdmUuc2VsZWN0b3IpIHtcbiAgICAgICAgICAgIGRpcmVjdGl2ZVR5cGVCeVNlbC5zZXQoZGlyZWN0aXZlLnNlbGVjdG9yLCBkaXJlY3RpdmUudHlwZS5yZWZlcmVuY2UpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG5cbiAgICAgICAgLy8gTWFwIG9mIFN0YXRpY1R5cGUgYnkgcGlwZSBuYW1lc1xuICAgICAgICBjb25zdCBwaXBlVHlwZUJ5TmFtZSA9IG5ldyBNYXA8c3RyaW5nLCBhbnk+KCk7XG5cbiAgICAgICAgY29uc3QgcGlwZXMgPSBtb2R1bGUudHJhbnNpdGl2ZU1vZHVsZS5waXBlcy5tYXAoXG4gICAgICAgICAgICBwaXBlID0+IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0UGlwZVN1bW1hcnkocGlwZS5yZWZlcmVuY2UpKTtcblxuICAgICAgICBwaXBlcy5mb3JFYWNoKHBpcGUgPT4geyBwaXBlVHlwZUJ5TmFtZS5zZXQocGlwZS5uYW1lLCBwaXBlLnR5cGUucmVmZXJlbmNlKTsgfSk7XG5cbiAgICAgICAgY29tcGlsZVIzQ29tcG9uZW50KFxuICAgICAgICAgICAgY29udGV4dCwgZGlyZWN0aXZlTWV0YWRhdGEsIHJlbmRlcjNBc3QsIHRoaXMucmVmbGVjdG9yLCBob3N0QmluZGluZ1BhcnNlcixcbiAgICAgICAgICAgIGRpcmVjdGl2ZVR5cGVCeVNlbCwgcGlwZVR5cGVCeU5hbWUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29tcGlsZVIzRGlyZWN0aXZlKGNvbnRleHQsIGRpcmVjdGl2ZU1ldGFkYXRhLCB0aGlzLnJlZmxlY3RvciwgaG9zdEJpbmRpbmdQYXJzZXIpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgcGlwZXMuZm9yRWFjaChwaXBlVHlwZSA9PiB7XG4gICAgICBjb25zdCBwaXBlTWV0YWRhdGEgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldFBpcGVNZXRhZGF0YShwaXBlVHlwZSk7XG4gICAgICBpZiAocGlwZU1ldGFkYXRhKSB7XG4gICAgICAgIGNvbXBpbGVSM1BpcGUoY29udGV4dCwgcGlwZU1ldGFkYXRhLCB0aGlzLnJlZmxlY3Rvcik7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBpbmplY3RhYmxlcy5mb3JFYWNoKGluamVjdGFibGUgPT4gdGhpcy5faW5qZWN0YWJsZUNvbXBpbGVyLmNvbXBpbGUoaW5qZWN0YWJsZSwgY29udGV4dCkpO1xuICB9XG5cbiAgZW1pdEFsbFBhcnRpYWxNb2R1bGVzMihmaWxlczogTmdBbmFseXplZEZpbGVXaXRoSW5qZWN0YWJsZXNbXSk6IFBhcnRpYWxNb2R1bGVbXSB7XG4gICAgLy8gVXNpbmcgcmVkdWNlIGxpa2UgdGhpcyBpcyBhIHNlbGVjdCBtYW55IHBhdHRlcm4gKHdoZXJlIG1hcCBpcyBhIHNlbGVjdCBwYXR0ZXJuKVxuICAgIHJldHVybiBmaWxlcy5yZWR1Y2U8UGFydGlhbE1vZHVsZVtdPigociwgZmlsZSkgPT4ge1xuICAgICAgci5wdXNoKC4uLnRoaXMuX2VtaXRQYXJ0aWFsTW9kdWxlMihmaWxlLmZpbGVOYW1lLCBmaWxlLmluamVjdGFibGVzKSk7XG4gICAgICByZXR1cm4gcjtcbiAgICB9LCBbXSk7XG4gIH1cblxuICBwcml2YXRlIF9lbWl0UGFydGlhbE1vZHVsZTIoZmlsZU5hbWU6IHN0cmluZywgaW5qZWN0YWJsZXM6IENvbXBpbGVJbmplY3RhYmxlTWV0YWRhdGFbXSk6XG4gICAgICBQYXJ0aWFsTW9kdWxlW10ge1xuICAgIGNvbnN0IGNvbnRleHQgPSB0aGlzLl9jcmVhdGVPdXRwdXRDb250ZXh0KGZpbGVOYW1lKTtcblxuICAgIGluamVjdGFibGVzLmZvckVhY2goaW5qZWN0YWJsZSA9PiB0aGlzLl9pbmplY3RhYmxlQ29tcGlsZXIuY29tcGlsZShpbmplY3RhYmxlLCBjb250ZXh0KSk7XG5cbiAgICBpZiAoY29udGV4dC5zdGF0ZW1lbnRzICYmIGNvbnRleHQuc3RhdGVtZW50cy5sZW5ndGggPiAwKSB7XG4gICAgICByZXR1cm4gW3tmaWxlTmFtZSwgc3RhdGVtZW50czogWy4uLmNvbnRleHQuY29uc3RhbnRQb29sLnN0YXRlbWVudHMsIC4uLmNvbnRleHQuc3RhdGVtZW50c119XTtcbiAgICB9XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgZW1pdEFsbEltcGxzKGFuYWx5emVSZXN1bHQ6IE5nQW5hbHl6ZWRNb2R1bGVzKTogR2VuZXJhdGVkRmlsZVtdIHtcbiAgICBjb25zdCB7bmdNb2R1bGVCeVBpcGVPckRpcmVjdGl2ZSwgZmlsZXN9ID0gYW5hbHl6ZVJlc3VsdDtcbiAgICBjb25zdCBzb3VyY2VNb2R1bGVzID0gZmlsZXMubWFwKFxuICAgICAgICBmaWxlID0+IHRoaXMuX2NvbXBpbGVJbXBsRmlsZShcbiAgICAgICAgICAgIGZpbGUuZmlsZU5hbWUsIG5nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmUsIGZpbGUuZGlyZWN0aXZlcywgZmlsZS5waXBlcywgZmlsZS5uZ01vZHVsZXMsXG4gICAgICAgICAgICBmaWxlLmluamVjdGFibGVzKSk7XG4gICAgcmV0dXJuIGZsYXR0ZW4oc291cmNlTW9kdWxlcyk7XG4gIH1cblxuICBwcml2YXRlIF9jb21waWxlSW1wbEZpbGUoXG4gICAgICBzcmNGaWxlVXJsOiBzdHJpbmcsIG5nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmU6IE1hcDxTdGF0aWNTeW1ib2wsIENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhPixcbiAgICAgIGRpcmVjdGl2ZXM6IFN0YXRpY1N5bWJvbFtdLCBwaXBlczogU3RhdGljU3ltYm9sW10sIG5nTW9kdWxlczogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGFbXSxcbiAgICAgIGluamVjdGFibGVzOiBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhW10pOiBHZW5lcmF0ZWRGaWxlW10ge1xuICAgIGNvbnN0IGZpbGVTdWZmaXggPSBub3JtYWxpemVHZW5GaWxlU3VmZml4KHNwbGl0VHlwZXNjcmlwdFN1ZmZpeChzcmNGaWxlVXJsLCB0cnVlKVsxXSk7XG4gICAgY29uc3QgZ2VuZXJhdGVkRmlsZXM6IEdlbmVyYXRlZEZpbGVbXSA9IFtdO1xuXG4gICAgY29uc3Qgb3V0cHV0Q3R4ID0gdGhpcy5fY3JlYXRlT3V0cHV0Q29udGV4dChuZ2ZhY3RvcnlGaWxlUGF0aChzcmNGaWxlVXJsLCB0cnVlKSk7XG5cbiAgICBnZW5lcmF0ZWRGaWxlcy5wdXNoKFxuICAgICAgICAuLi50aGlzLl9jcmVhdGVTdW1tYXJ5KHNyY0ZpbGVVcmwsIGRpcmVjdGl2ZXMsIHBpcGVzLCBuZ01vZHVsZXMsIGluamVjdGFibGVzLCBvdXRwdXRDdHgpKTtcblxuICAgIC8vIGNvbXBpbGUgYWxsIG5nIG1vZHVsZXNcbiAgICBuZ01vZHVsZXMuZm9yRWFjaCgobmdNb2R1bGVNZXRhKSA9PiB0aGlzLl9jb21waWxlTW9kdWxlKG91dHB1dEN0eCwgbmdNb2R1bGVNZXRhKSk7XG5cbiAgICAvLyBjb21waWxlIGNvbXBvbmVudHNcbiAgICBkaXJlY3RpdmVzLmZvckVhY2goKGRpclR5cGUpID0+IHtcbiAgICAgIGNvbnN0IGNvbXBNZXRhID0gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVNZXRhZGF0YSg8YW55PmRpclR5cGUpO1xuICAgICAgaWYgKCFjb21wTWV0YS5pc0NvbXBvbmVudCkge1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgICBjb25zdCBuZ01vZHVsZSA9IG5nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmUuZ2V0KGRpclR5cGUpO1xuICAgICAgaWYgKCFuZ01vZHVsZSkge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgICBgSW50ZXJuYWwgRXJyb3I6IGNhbm5vdCBkZXRlcm1pbmUgdGhlIG1vZHVsZSBmb3IgY29tcG9uZW50ICR7aWRlbnRpZmllck5hbWUoY29tcE1ldGEudHlwZSl9IWApO1xuICAgICAgfVxuXG4gICAgICAvLyBjb21waWxlIHN0eWxlc1xuICAgICAgY29uc3QgY29tcG9uZW50U3R5bGVzaGVldCA9IHRoaXMuX3N0eWxlQ29tcGlsZXIuY29tcGlsZUNvbXBvbmVudChvdXRwdXRDdHgsIGNvbXBNZXRhKTtcbiAgICAgIC8vIE5vdGU6IGNvbXBNZXRhIGlzIGEgY29tcG9uZW50IGFuZCB0aGVyZWZvcmUgdGVtcGxhdGUgaXMgbm9uIG51bGwuXG4gICAgICBjb21wTWV0YS50ZW1wbGF0ZSAhLmV4dGVybmFsU3R5bGVzaGVldHMuZm9yRWFjaCgoc3R5bGVzaGVldE1ldGEpID0+IHtcbiAgICAgICAgLy8gTm90ZTogZmlsbCBub24gc2hpbSBhbmQgc2hpbSBzdHlsZSBmaWxlcyBhcyB0aGV5IG1pZ2h0XG4gICAgICAgIC8vIGJlIHNoYXJlZCBieSBjb21wb25lbnQgd2l0aCBhbmQgd2l0aG91dCBWaWV3RW5jYXBzdWxhdGlvbi5cbiAgICAgICAgY29uc3Qgc2hpbSA9IHRoaXMuX3N0eWxlQ29tcGlsZXIubmVlZHNTdHlsZVNoaW0oY29tcE1ldGEpO1xuICAgICAgICBnZW5lcmF0ZWRGaWxlcy5wdXNoKFxuICAgICAgICAgICAgdGhpcy5fY29kZWdlblN0eWxlcyhzcmNGaWxlVXJsLCBjb21wTWV0YSwgc3R5bGVzaGVldE1ldGEsIHNoaW0sIGZpbGVTdWZmaXgpKTtcbiAgICAgICAgaWYgKHRoaXMuX29wdGlvbnMuYWxsb3dFbXB0eUNvZGVnZW5GaWxlcykge1xuICAgICAgICAgIGdlbmVyYXRlZEZpbGVzLnB1c2goXG4gICAgICAgICAgICAgIHRoaXMuX2NvZGVnZW5TdHlsZXMoc3JjRmlsZVVybCwgY29tcE1ldGEsIHN0eWxlc2hlZXRNZXRhLCAhc2hpbSwgZmlsZVN1ZmZpeCkpO1xuICAgICAgICB9XG4gICAgICB9KTtcblxuICAgICAgLy8gY29tcGlsZSBjb21wb25lbnRzXG4gICAgICBjb25zdCBjb21wVmlld1ZhcnMgPSB0aGlzLl9jb21waWxlQ29tcG9uZW50KFxuICAgICAgICAgIG91dHB1dEN0eCwgY29tcE1ldGEsIG5nTW9kdWxlLCBuZ01vZHVsZS50cmFuc2l0aXZlTW9kdWxlLmRpcmVjdGl2ZXMsIGNvbXBvbmVudFN0eWxlc2hlZXQsXG4gICAgICAgICAgZmlsZVN1ZmZpeCk7XG4gICAgICB0aGlzLl9jb21waWxlQ29tcG9uZW50RmFjdG9yeShvdXRwdXRDdHgsIGNvbXBNZXRhLCBuZ01vZHVsZSwgZmlsZVN1ZmZpeCk7XG4gICAgfSk7XG4gICAgaWYgKG91dHB1dEN0eC5zdGF0ZW1lbnRzLmxlbmd0aCA+IDAgfHwgdGhpcy5fb3B0aW9ucy5hbGxvd0VtcHR5Q29kZWdlbkZpbGVzKSB7XG4gICAgICBjb25zdCBzcmNNb2R1bGUgPSB0aGlzLl9jb2RlZ2VuU291cmNlTW9kdWxlKHNyY0ZpbGVVcmwsIG91dHB1dEN0eCk7XG4gICAgICBnZW5lcmF0ZWRGaWxlcy51bnNoaWZ0KHNyY01vZHVsZSk7XG4gICAgfVxuICAgIHJldHVybiBnZW5lcmF0ZWRGaWxlcztcbiAgfVxuXG4gIHByaXZhdGUgX2NyZWF0ZVN1bW1hcnkoXG4gICAgICBzcmNGaWxlTmFtZTogc3RyaW5nLCBkaXJlY3RpdmVzOiBTdGF0aWNTeW1ib2xbXSwgcGlwZXM6IFN0YXRpY1N5bWJvbFtdLFxuICAgICAgbmdNb2R1bGVzOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YVtdLCBpbmplY3RhYmxlczogQ29tcGlsZUluamVjdGFibGVNZXRhZGF0YVtdLFxuICAgICAgbmdGYWN0b3J5Q3R4OiBPdXRwdXRDb250ZXh0KTogR2VuZXJhdGVkRmlsZVtdIHtcbiAgICBjb25zdCBzeW1ib2xTdW1tYXJpZXMgPSB0aGlzLl9zeW1ib2xSZXNvbHZlci5nZXRTeW1ib2xzT2Yoc3JjRmlsZU5hbWUpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC5tYXAoc3ltYm9sID0+IHRoaXMuX3N5bWJvbFJlc29sdmVyLnJlc29sdmVTeW1ib2woc3ltYm9sKSk7XG4gICAgY29uc3QgdHlwZURhdGE6IHtcbiAgICAgIHN1bW1hcnk6IENvbXBpbGVUeXBlU3VtbWFyeSxcbiAgICAgIG1ldGFkYXRhOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSB8IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSB8IENvbXBpbGVQaXBlTWV0YWRhdGEgfFxuICAgICAgICAgIENvbXBpbGVUeXBlTWV0YWRhdGFcbiAgICB9W10gPVxuICAgICAgICBbXG4gICAgICAgICAgLi4ubmdNb2R1bGVzLm1hcChcbiAgICAgICAgICAgICAgbWV0YSA9PiAoe1xuICAgICAgICAgICAgICAgIHN1bW1hcnk6IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0TmdNb2R1bGVTdW1tYXJ5KG1ldGEudHlwZS5yZWZlcmVuY2UpICEsXG4gICAgICAgICAgICAgICAgbWV0YWRhdGE6IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0TmdNb2R1bGVNZXRhZGF0YShtZXRhLnR5cGUucmVmZXJlbmNlKSAhXG4gICAgICAgICAgICAgIH0pKSxcbiAgICAgICAgICAuLi5kaXJlY3RpdmVzLm1hcChyZWYgPT4gKHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHN1bW1hcnk6IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0RGlyZWN0aXZlU3VtbWFyeShyZWYpICEsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICBtZXRhZGF0YTogdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVNZXRhZGF0YShyZWYpICFcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KSksXG4gICAgICAgICAgLi4ucGlwZXMubWFwKHJlZiA9PiAoe1xuICAgICAgICAgICAgICAgICAgICAgICAgIHN1bW1hcnk6IHRoaXMuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0UGlwZVN1bW1hcnkocmVmKSAhLFxuICAgICAgICAgICAgICAgICAgICAgICAgIG1ldGFkYXRhOiB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldFBpcGVNZXRhZGF0YShyZWYpICFcbiAgICAgICAgICAgICAgICAgICAgICAgfSkpLFxuICAgICAgICAgIC4uLmluamVjdGFibGVzLm1hcChcbiAgICAgICAgICAgICAgcmVmID0+ICh7XG4gICAgICAgICAgICAgICAgc3VtbWFyeTogdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXRJbmplY3RhYmxlU3VtbWFyeShyZWYuc3ltYm9sKSAhLFxuICAgICAgICAgICAgICAgIG1ldGFkYXRhOiB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldEluamVjdGFibGVTdW1tYXJ5KHJlZi5zeW1ib2wpICEudHlwZVxuICAgICAgICAgICAgICB9KSlcbiAgICAgICAgXTtcbiAgICBjb25zdCBmb3JKaXRPdXRwdXRDdHggPSB0aGlzLl9vcHRpb25zLmVuYWJsZVN1bW1hcmllc0ZvckppdCA/XG4gICAgICAgIHRoaXMuX2NyZWF0ZU91dHB1dENvbnRleHQoc3VtbWFyeUZvckppdEZpbGVOYW1lKHNyY0ZpbGVOYW1lLCB0cnVlKSkgOlxuICAgICAgICBudWxsO1xuICAgIGNvbnN0IHtqc29uLCBleHBvcnRBc30gPSBzZXJpYWxpemVTdW1tYXJpZXMoXG4gICAgICAgIHNyY0ZpbGVOYW1lLCBmb3JKaXRPdXRwdXRDdHgsIHRoaXMuX3N1bW1hcnlSZXNvbHZlciwgdGhpcy5fc3ltYm9sUmVzb2x2ZXIsIHN5bWJvbFN1bW1hcmllcyxcbiAgICAgICAgdHlwZURhdGEsIHRoaXMuX29wdGlvbnMuY3JlYXRlRXh0ZXJuYWxTeW1ib2xGYWN0b3J5UmVleHBvcnRzKTtcbiAgICBleHBvcnRBcy5mb3JFYWNoKChlbnRyeSkgPT4ge1xuICAgICAgbmdGYWN0b3J5Q3R4LnN0YXRlbWVudHMucHVzaChcbiAgICAgICAgICBvLnZhcmlhYmxlKGVudHJ5LmV4cG9ydEFzKS5zZXQobmdGYWN0b3J5Q3R4LmltcG9ydEV4cHIoZW50cnkuc3ltYm9sKSkudG9EZWNsU3RtdChudWxsLCBbXG4gICAgICAgICAgICBvLlN0bXRNb2RpZmllci5FeHBvcnRlZFxuICAgICAgICAgIF0pKTtcbiAgICB9KTtcbiAgICBjb25zdCBzdW1tYXJ5SnNvbiA9IG5ldyBHZW5lcmF0ZWRGaWxlKHNyY0ZpbGVOYW1lLCBzdW1tYXJ5RmlsZU5hbWUoc3JjRmlsZU5hbWUpLCBqc29uKTtcbiAgICBjb25zdCByZXN1bHQgPSBbc3VtbWFyeUpzb25dO1xuICAgIGlmIChmb3JKaXRPdXRwdXRDdHgpIHtcbiAgICAgIHJlc3VsdC5wdXNoKHRoaXMuX2NvZGVnZW5Tb3VyY2VNb2R1bGUoc3JjRmlsZU5hbWUsIGZvckppdE91dHB1dEN0eCkpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBfY29tcGlsZU1vZHVsZShvdXRwdXRDdHg6IE91dHB1dENvbnRleHQsIG5nTW9kdWxlOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSk6IHZvaWQge1xuICAgIGNvbnN0IHByb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSA9IFtdO1xuXG4gICAgaWYgKHRoaXMuX29wdGlvbnMubG9jYWxlKSB7XG4gICAgICBjb25zdCBub3JtYWxpemVkTG9jYWxlID0gdGhpcy5fb3B0aW9ucy5sb2NhbGUucmVwbGFjZSgvXy9nLCAnLScpO1xuICAgICAgcHJvdmlkZXJzLnB1c2goe1xuICAgICAgICB0b2tlbjogY3JlYXRlVG9rZW5Gb3JFeHRlcm5hbFJlZmVyZW5jZSh0aGlzLnJlZmxlY3RvciwgSWRlbnRpZmllcnMuTE9DQUxFX0lEKSxcbiAgICAgICAgdXNlVmFsdWU6IG5vcm1hbGl6ZWRMb2NhbGUsXG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBpZiAodGhpcy5fb3B0aW9ucy5pMThuRm9ybWF0KSB7XG4gICAgICBwcm92aWRlcnMucHVzaCh7XG4gICAgICAgIHRva2VuOiBjcmVhdGVUb2tlbkZvckV4dGVybmFsUmVmZXJlbmNlKHRoaXMucmVmbGVjdG9yLCBJZGVudGlmaWVycy5UUkFOU0xBVElPTlNfRk9STUFUKSxcbiAgICAgICAgdXNlVmFsdWU6IHRoaXMuX29wdGlvbnMuaTE4bkZvcm1hdFxuICAgICAgfSk7XG4gICAgfVxuXG4gICAgdGhpcy5fbmdNb2R1bGVDb21waWxlci5jb21waWxlKG91dHB1dEN0eCwgbmdNb2R1bGUsIHByb3ZpZGVycyk7XG4gIH1cblxuICBwcml2YXRlIF9jb21waWxlQ29tcG9uZW50RmFjdG9yeShcbiAgICAgIG91dHB1dEN0eDogT3V0cHV0Q29udGV4dCwgY29tcE1ldGE6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSxcbiAgICAgIG5nTW9kdWxlOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSwgZmlsZVN1ZmZpeDogc3RyaW5nKTogdm9pZCB7XG4gICAgY29uc3QgaG9zdE1ldGEgPSB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldEhvc3RDb21wb25lbnRNZXRhZGF0YShjb21wTWV0YSk7XG4gICAgY29uc3QgaG9zdFZpZXdGYWN0b3J5VmFyID1cbiAgICAgICAgdGhpcy5fY29tcGlsZUNvbXBvbmVudChvdXRwdXRDdHgsIGhvc3RNZXRhLCBuZ01vZHVsZSwgW2NvbXBNZXRhLnR5cGVdLCBudWxsLCBmaWxlU3VmZml4KVxuICAgICAgICAgICAgLnZpZXdDbGFzc1ZhcjtcbiAgICBjb25zdCBjb21wRmFjdG9yeVZhciA9IGNvbXBvbmVudEZhY3RvcnlOYW1lKGNvbXBNZXRhLnR5cGUucmVmZXJlbmNlKTtcbiAgICBjb25zdCBpbnB1dHNFeHByczogby5MaXRlcmFsTWFwRW50cnlbXSA9IFtdO1xuICAgIGZvciAobGV0IHByb3BOYW1lIGluIGNvbXBNZXRhLmlucHV0cykge1xuICAgICAgY29uc3QgdGVtcGxhdGVOYW1lID0gY29tcE1ldGEuaW5wdXRzW3Byb3BOYW1lXTtcbiAgICAgIC8vIERvbid0IHF1b3RlIHNvIHRoYXQgdGhlIGtleSBnZXRzIG1pbmlmaWVkLi4uXG4gICAgICBpbnB1dHNFeHBycy5wdXNoKG5ldyBvLkxpdGVyYWxNYXBFbnRyeShwcm9wTmFtZSwgby5saXRlcmFsKHRlbXBsYXRlTmFtZSksIGZhbHNlKSk7XG4gICAgfVxuICAgIGNvbnN0IG91dHB1dHNFeHByczogby5MaXRlcmFsTWFwRW50cnlbXSA9IFtdO1xuICAgIGZvciAobGV0IHByb3BOYW1lIGluIGNvbXBNZXRhLm91dHB1dHMpIHtcbiAgICAgIGNvbnN0IHRlbXBsYXRlTmFtZSA9IGNvbXBNZXRhLm91dHB1dHNbcHJvcE5hbWVdO1xuICAgICAgLy8gRG9uJ3QgcXVvdGUgc28gdGhhdCB0aGUga2V5IGdldHMgbWluaWZpZWQuLi5cbiAgICAgIG91dHB1dHNFeHBycy5wdXNoKG5ldyBvLkxpdGVyYWxNYXBFbnRyeShwcm9wTmFtZSwgby5saXRlcmFsKHRlbXBsYXRlTmFtZSksIGZhbHNlKSk7XG4gICAgfVxuXG4gICAgb3V0cHV0Q3R4LnN0YXRlbWVudHMucHVzaChcbiAgICAgICAgby52YXJpYWJsZShjb21wRmFjdG9yeVZhcilcbiAgICAgICAgICAgIC5zZXQoby5pbXBvcnRFeHByKElkZW50aWZpZXJzLmNyZWF0ZUNvbXBvbmVudEZhY3RvcnkpLmNhbGxGbihbXG4gICAgICAgICAgICAgIG8ubGl0ZXJhbChjb21wTWV0YS5zZWxlY3RvciksIG91dHB1dEN0eC5pbXBvcnRFeHByKGNvbXBNZXRhLnR5cGUucmVmZXJlbmNlKSxcbiAgICAgICAgICAgICAgby52YXJpYWJsZShob3N0Vmlld0ZhY3RvcnlWYXIpLCBuZXcgby5MaXRlcmFsTWFwRXhwcihpbnB1dHNFeHBycyksXG4gICAgICAgICAgICAgIG5ldyBvLkxpdGVyYWxNYXBFeHByKG91dHB1dHNFeHBycyksXG4gICAgICAgICAgICAgIG8ubGl0ZXJhbEFycihcbiAgICAgICAgICAgICAgICAgIGNvbXBNZXRhLnRlbXBsYXRlICEubmdDb250ZW50U2VsZWN0b3JzLm1hcChzZWxlY3RvciA9PiBvLmxpdGVyYWwoc2VsZWN0b3IpKSlcbiAgICAgICAgICAgIF0pKVxuICAgICAgICAgICAgLnRvRGVjbFN0bXQoXG4gICAgICAgICAgICAgICAgby5pbXBvcnRUeXBlKFxuICAgICAgICAgICAgICAgICAgICBJZGVudGlmaWVycy5Db21wb25lbnRGYWN0b3J5LFxuICAgICAgICAgICAgICAgICAgICBbby5leHByZXNzaW9uVHlwZShvdXRwdXRDdHguaW1wb3J0RXhwcihjb21wTWV0YS50eXBlLnJlZmVyZW5jZSkpICFdLFxuICAgICAgICAgICAgICAgICAgICBbby5UeXBlTW9kaWZpZXIuQ29uc3RdKSxcbiAgICAgICAgICAgICAgICBbby5TdG10TW9kaWZpZXIuRmluYWwsIG8uU3RtdE1vZGlmaWVyLkV4cG9ydGVkXSkpO1xuICB9XG5cbiAgcHJpdmF0ZSBfY29tcGlsZUNvbXBvbmVudChcbiAgICAgIG91dHB1dEN0eDogT3V0cHV0Q29udGV4dCwgY29tcE1ldGE6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSxcbiAgICAgIG5nTW9kdWxlOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSwgZGlyZWN0aXZlSWRlbnRpZmllcnM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXSxcbiAgICAgIGNvbXBvbmVudFN0eWxlczogQ29tcGlsZWRTdHlsZXNoZWV0fG51bGwsIGZpbGVTdWZmaXg6IHN0cmluZyk6IFZpZXdDb21waWxlUmVzdWx0IHtcbiAgICBjb25zdCB7dGVtcGxhdGU6IHBhcnNlZFRlbXBsYXRlLCBwaXBlczogdXNlZFBpcGVzfSA9XG4gICAgICAgIHRoaXMuX3BhcnNlVGVtcGxhdGUoY29tcE1ldGEsIG5nTW9kdWxlLCBkaXJlY3RpdmVJZGVudGlmaWVycyk7XG4gICAgY29uc3Qgc3R5bGVzRXhwciA9IGNvbXBvbmVudFN0eWxlcyA/IG8udmFyaWFibGUoY29tcG9uZW50U3R5bGVzLnN0eWxlc1ZhcikgOiBvLmxpdGVyYWxBcnIoW10pO1xuICAgIGNvbnN0IHZpZXdSZXN1bHQgPSB0aGlzLl92aWV3Q29tcGlsZXIuY29tcGlsZUNvbXBvbmVudChcbiAgICAgICAgb3V0cHV0Q3R4LCBjb21wTWV0YSwgcGFyc2VkVGVtcGxhdGUsIHN0eWxlc0V4cHIsIHVzZWRQaXBlcyk7XG4gICAgaWYgKGNvbXBvbmVudFN0eWxlcykge1xuICAgICAgX3Jlc29sdmVTdHlsZVN0YXRlbWVudHMoXG4gICAgICAgICAgdGhpcy5fc3ltYm9sUmVzb2x2ZXIsIGNvbXBvbmVudFN0eWxlcywgdGhpcy5fc3R5bGVDb21waWxlci5uZWVkc1N0eWxlU2hpbShjb21wTWV0YSksXG4gICAgICAgICAgZmlsZVN1ZmZpeCk7XG4gICAgfVxuICAgIHJldHVybiB2aWV3UmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBfcGFyc2VUZW1wbGF0ZShcbiAgICAgIGNvbXBNZXRhOiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsIG5nTW9kdWxlOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSxcbiAgICAgIGRpcmVjdGl2ZUlkZW50aWZpZXJzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10pOlxuICAgICAge3RlbXBsYXRlOiBUZW1wbGF0ZUFzdFtdLCBwaXBlczogQ29tcGlsZVBpcGVTdW1tYXJ5W119IHtcbiAgICBpZiAodGhpcy5fdGVtcGxhdGVBc3RDYWNoZS5oYXMoY29tcE1ldGEudHlwZS5yZWZlcmVuY2UpKSB7XG4gICAgICByZXR1cm4gdGhpcy5fdGVtcGxhdGVBc3RDYWNoZS5nZXQoY29tcE1ldGEudHlwZS5yZWZlcmVuY2UpICE7XG4gICAgfVxuICAgIGNvbnN0IHByZXNlcnZlV2hpdGVzcGFjZXMgPSBjb21wTWV0YSAhLnRlbXBsYXRlICEucHJlc2VydmVXaGl0ZXNwYWNlcztcbiAgICBjb25zdCBkaXJlY3RpdmVzID1cbiAgICAgICAgZGlyZWN0aXZlSWRlbnRpZmllcnMubWFwKGRpciA9PiB0aGlzLl9tZXRhZGF0YVJlc29sdmVyLmdldERpcmVjdGl2ZVN1bW1hcnkoZGlyLnJlZmVyZW5jZSkpO1xuICAgIGNvbnN0IHBpcGVzID0gbmdNb2R1bGUudHJhbnNpdGl2ZU1vZHVsZS5waXBlcy5tYXAoXG4gICAgICAgIHBpcGUgPT4gdGhpcy5fbWV0YWRhdGFSZXNvbHZlci5nZXRQaXBlU3VtbWFyeShwaXBlLnJlZmVyZW5jZSkpO1xuICAgIGNvbnN0IHJlc3VsdCA9IHRoaXMuX3RlbXBsYXRlUGFyc2VyLnBhcnNlKFxuICAgICAgICBjb21wTWV0YSwgY29tcE1ldGEudGVtcGxhdGUgIS5odG1sQXN0ICEsIGRpcmVjdGl2ZXMsIHBpcGVzLCBuZ01vZHVsZS5zY2hlbWFzLFxuICAgICAgICB0ZW1wbGF0ZVNvdXJjZVVybChuZ01vZHVsZS50eXBlLCBjb21wTWV0YSwgY29tcE1ldGEudGVtcGxhdGUgISksIHByZXNlcnZlV2hpdGVzcGFjZXMpO1xuICAgIHRoaXMuX3RlbXBsYXRlQXN0Q2FjaGUuc2V0KGNvbXBNZXRhLnR5cGUucmVmZXJlbmNlLCByZXN1bHQpO1xuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICBwcml2YXRlIF9jcmVhdGVPdXRwdXRDb250ZXh0KGdlbkZpbGVQYXRoOiBzdHJpbmcpOiBPdXRwdXRDb250ZXh0IHtcbiAgICBjb25zdCBpbXBvcnRFeHByID1cbiAgICAgICAgKHN5bWJvbDogU3RhdGljU3ltYm9sLCB0eXBlUGFyYW1zOiBvLlR5cGVbXSB8IG51bGwgPSBudWxsLFxuICAgICAgICAgdXNlU3VtbWFyaWVzOiBib29sZWFuID0gdHJ1ZSkgPT4ge1xuICAgICAgICAgIGlmICghKHN5bWJvbCBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkpIHtcbiAgICAgICAgICAgIHRocm93IG5ldyBFcnJvcihgSW50ZXJuYWwgZXJyb3I6IHVua25vd24gaWRlbnRpZmllciAke0pTT04uc3RyaW5naWZ5KHN5bWJvbCl9YCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNvbnN0IGFyaXR5ID0gdGhpcy5fc3ltYm9sUmVzb2x2ZXIuZ2V0VHlwZUFyaXR5KHN5bWJvbCkgfHwgMDtcbiAgICAgICAgICBjb25zdCB7ZmlsZVBhdGgsIG5hbWUsIG1lbWJlcnN9ID1cbiAgICAgICAgICAgICAgdGhpcy5fc3ltYm9sUmVzb2x2ZXIuZ2V0SW1wb3J0QXMoc3ltYm9sLCB1c2VTdW1tYXJpZXMpIHx8IHN5bWJvbDtcbiAgICAgICAgICBjb25zdCBpbXBvcnRNb2R1bGUgPSB0aGlzLl9maWxlTmFtZVRvTW9kdWxlTmFtZShmaWxlUGF0aCwgZ2VuRmlsZVBhdGgpO1xuXG4gICAgICAgICAgLy8gSXQgc2hvdWxkIGJlIGdvb2QgZW5vdWdoIHRvIGNvbXBhcmUgZmlsZVBhdGggdG8gZ2VuRmlsZVBhdGggYW5kIGlmIHRoZXkgYXJlIGVxdWFsXG4gICAgICAgICAgLy8gdGhlcmUgaXMgYSBzZWxmIHJlZmVyZW5jZS4gSG93ZXZlciwgbmdmYWN0b3J5IGZpbGVzIGdlbmVyYXRlIHRvIC50cyBidXQgdGhlaXJcbiAgICAgICAgICAvLyBzeW1ib2xzIGhhdmUgLmQudHMgc28gYSBzaW1wbGUgY29tcGFyZSBpcyBpbnN1ZmZpY2llbnQuIFRoZXkgc2hvdWxkIGJlIGNhbm9uaWNhbFxuICAgICAgICAgIC8vIGFuZCBpcyB0cmFja2VkIGJ5ICMxNzcwNS5cbiAgICAgICAgICBjb25zdCBzZWxmUmVmZXJlbmNlID0gdGhpcy5fZmlsZU5hbWVUb01vZHVsZU5hbWUoZ2VuRmlsZVBhdGgsIGdlbkZpbGVQYXRoKTtcbiAgICAgICAgICBjb25zdCBtb2R1bGVOYW1lID0gaW1wb3J0TW9kdWxlID09PSBzZWxmUmVmZXJlbmNlID8gbnVsbCA6IGltcG9ydE1vZHVsZTtcblxuICAgICAgICAgIC8vIElmIHdlIGFyZSBpbiBhIHR5cGUgZXhwcmVzc2lvbiB0aGF0IHJlZmVycyB0byBhIGdlbmVyaWMgdHlwZSB0aGVuIHN1cHBseVxuICAgICAgICAgIC8vIHRoZSByZXF1aXJlZCB0eXBlIHBhcmFtZXRlcnMuIElmIHRoZXJlIHdlcmUgbm90IGVub3VnaCB0eXBlIHBhcmFtZXRlcnNcbiAgICAgICAgICAvLyBzdXBwbGllZCwgc3VwcGx5IGFueSBhcyB0aGUgdHlwZS4gT3V0c2lkZSBhIHR5cGUgZXhwcmVzc2lvbiB0aGUgcmVmZXJlbmNlXG4gICAgICAgICAgLy8gc2hvdWxkIG5vdCBzdXBwbHkgdHlwZSBwYXJhbWV0ZXJzIGFuZCBiZSB0cmVhdGVkIGFzIGEgc2ltcGxlIHZhbHVlIHJlZmVyZW5jZVxuICAgICAgICAgIC8vIHRvIHRoZSBjb25zdHJ1Y3RvciBmdW5jdGlvbiBpdHNlbGYuXG4gICAgICAgICAgY29uc3Qgc3VwcGxpZWRUeXBlUGFyYW1zID0gdHlwZVBhcmFtcyB8fCBbXTtcbiAgICAgICAgICBjb25zdCBtaXNzaW5nVHlwZVBhcmFtc0NvdW50ID0gYXJpdHkgLSBzdXBwbGllZFR5cGVQYXJhbXMubGVuZ3RoO1xuICAgICAgICAgIGNvbnN0IGFsbFR5cGVQYXJhbXMgPVxuICAgICAgICAgICAgICBzdXBwbGllZFR5cGVQYXJhbXMuY29uY2F0KG5ldyBBcnJheShtaXNzaW5nVHlwZVBhcmFtc0NvdW50KS5maWxsKG8uRFlOQU1JQ19UWVBFKSk7XG4gICAgICAgICAgcmV0dXJuIG1lbWJlcnMucmVkdWNlKFxuICAgICAgICAgICAgICAoZXhwciwgbWVtYmVyTmFtZSkgPT4gZXhwci5wcm9wKG1lbWJlck5hbWUpLFxuICAgICAgICAgICAgICA8by5FeHByZXNzaW9uPm8uaW1wb3J0RXhwcihcbiAgICAgICAgICAgICAgICAgIG5ldyBvLkV4dGVybmFsUmVmZXJlbmNlKG1vZHVsZU5hbWUsIG5hbWUsIG51bGwpLCBhbGxUeXBlUGFyYW1zKSk7XG4gICAgICAgIH07XG5cbiAgICByZXR1cm4ge3N0YXRlbWVudHM6IFtdLCBnZW5GaWxlUGF0aCwgaW1wb3J0RXhwciwgY29uc3RhbnRQb29sOiBuZXcgQ29uc3RhbnRQb29sKCl9O1xuICB9XG5cbiAgcHJpdmF0ZSBfZmlsZU5hbWVUb01vZHVsZU5hbWUoaW1wb3J0ZWRGaWxlUGF0aDogc3RyaW5nLCBjb250YWluaW5nRmlsZVBhdGg6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuX3N1bW1hcnlSZXNvbHZlci5nZXRLbm93bk1vZHVsZU5hbWUoaW1wb3J0ZWRGaWxlUGF0aCkgfHxcbiAgICAgICAgdGhpcy5fc3ltYm9sUmVzb2x2ZXIuZ2V0S25vd25Nb2R1bGVOYW1lKGltcG9ydGVkRmlsZVBhdGgpIHx8XG4gICAgICAgIHRoaXMuX2hvc3QuZmlsZU5hbWVUb01vZHVsZU5hbWUoaW1wb3J0ZWRGaWxlUGF0aCwgY29udGFpbmluZ0ZpbGVQYXRoKTtcbiAgfVxuXG4gIHByaXZhdGUgX2NvZGVnZW5TdHlsZXMoXG4gICAgICBzcmNGaWxlVXJsOiBzdHJpbmcsIGNvbXBNZXRhOiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsXG4gICAgICBzdHlsZXNoZWV0TWV0YWRhdGE6IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEsIGlzU2hpbW1lZDogYm9vbGVhbixcbiAgICAgIGZpbGVTdWZmaXg6IHN0cmluZyk6IEdlbmVyYXRlZEZpbGUge1xuICAgIGNvbnN0IG91dHB1dEN0eCA9IHRoaXMuX2NyZWF0ZU91dHB1dENvbnRleHQoXG4gICAgICAgIF9zdHlsZXNNb2R1bGVVcmwoc3R5bGVzaGVldE1ldGFkYXRhLm1vZHVsZVVybCAhLCBpc1NoaW1tZWQsIGZpbGVTdWZmaXgpKTtcbiAgICBjb25zdCBjb21waWxlZFN0eWxlc2hlZXQgPVxuICAgICAgICB0aGlzLl9zdHlsZUNvbXBpbGVyLmNvbXBpbGVTdHlsZXMob3V0cHV0Q3R4LCBjb21wTWV0YSwgc3R5bGVzaGVldE1ldGFkYXRhLCBpc1NoaW1tZWQpO1xuICAgIF9yZXNvbHZlU3R5bGVTdGF0ZW1lbnRzKHRoaXMuX3N5bWJvbFJlc29sdmVyLCBjb21waWxlZFN0eWxlc2hlZXQsIGlzU2hpbW1lZCwgZmlsZVN1ZmZpeCk7XG4gICAgcmV0dXJuIHRoaXMuX2NvZGVnZW5Tb3VyY2VNb2R1bGUoc3JjRmlsZVVybCwgb3V0cHV0Q3R4KTtcbiAgfVxuXG4gIHByaXZhdGUgX2NvZGVnZW5Tb3VyY2VNb2R1bGUoc3JjRmlsZVVybDogc3RyaW5nLCBjdHg6IE91dHB1dENvbnRleHQpOiBHZW5lcmF0ZWRGaWxlIHtcbiAgICByZXR1cm4gbmV3IEdlbmVyYXRlZEZpbGUoc3JjRmlsZVVybCwgY3R4LmdlbkZpbGVQYXRoLCBjdHguc3RhdGVtZW50cyk7XG4gIH1cblxuICBsaXN0TGF6eVJvdXRlcyhlbnRyeVJvdXRlPzogc3RyaW5nLCBhbmFseXplZE1vZHVsZXM/OiBOZ0FuYWx5emVkTW9kdWxlcyk6IExhenlSb3V0ZVtdIHtcbiAgICBjb25zdCBzZWxmID0gdGhpcztcbiAgICBpZiAoZW50cnlSb3V0ZSkge1xuICAgICAgY29uc3Qgc3ltYm9sID0gcGFyc2VMYXp5Um91dGUoZW50cnlSb3V0ZSwgdGhpcy5yZWZsZWN0b3IpLnJlZmVyZW5jZWRNb2R1bGU7XG4gICAgICByZXR1cm4gdmlzaXRMYXp5Um91dGUoc3ltYm9sKTtcbiAgICB9IGVsc2UgaWYgKGFuYWx5emVkTW9kdWxlcykge1xuICAgICAgY29uc3QgYWxsTGF6eVJvdXRlczogTGF6eVJvdXRlW10gPSBbXTtcbiAgICAgIGZvciAoY29uc3QgbmdNb2R1bGUgb2YgYW5hbHl6ZWRNb2R1bGVzLm5nTW9kdWxlcykge1xuICAgICAgICBjb25zdCBsYXp5Um91dGVzID0gbGlzdExhenlSb3V0ZXMobmdNb2R1bGUsIHRoaXMucmVmbGVjdG9yKTtcbiAgICAgICAgZm9yIChjb25zdCBsYXp5Um91dGUgb2YgbGF6eVJvdXRlcykge1xuICAgICAgICAgIGFsbExhenlSb3V0ZXMucHVzaChsYXp5Um91dGUpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gYWxsTGF6eVJvdXRlcztcbiAgICB9IGVsc2Uge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBFaXRoZXIgcm91dGUgb3IgYW5hbHl6ZWRNb2R1bGVzIGhhcyB0byBiZSBzcGVjaWZpZWQhYCk7XG4gICAgfVxuXG4gICAgZnVuY3Rpb24gdmlzaXRMYXp5Um91dGUoXG4gICAgICAgIHN5bWJvbDogU3RhdGljU3ltYm9sLCBzZWVuUm91dGVzID0gbmV3IFNldDxTdGF0aWNTeW1ib2w+KCksXG4gICAgICAgIGFsbExhenlSb3V0ZXM6IExhenlSb3V0ZVtdID0gW10pOiBMYXp5Um91dGVbXSB7XG4gICAgICAvLyBTdXBwb3J0IHBvaW50aW5nIHRvIGRlZmF1bHQgZXhwb3J0cywgYnV0IHN0b3AgcmVjdXJzaW5nIHRoZXJlLFxuICAgICAgLy8gYXMgdGhlIFN0YXRpY1JlZmxlY3RvciBkb2VzIG5vdCB5ZXQgc3VwcG9ydCBkZWZhdWx0IGV4cG9ydHMuXG4gICAgICBpZiAoc2VlblJvdXRlcy5oYXMoc3ltYm9sKSB8fCAhc3ltYm9sLm5hbWUpIHtcbiAgICAgICAgcmV0dXJuIGFsbExhenlSb3V0ZXM7XG4gICAgICB9XG4gICAgICBzZWVuUm91dGVzLmFkZChzeW1ib2wpO1xuICAgICAgY29uc3QgbGF6eVJvdXRlcyA9IGxpc3RMYXp5Um91dGVzKFxuICAgICAgICAgIHNlbGYuX21ldGFkYXRhUmVzb2x2ZXIuZ2V0TmdNb2R1bGVNZXRhZGF0YShzeW1ib2wsIHRydWUpICEsIHNlbGYucmVmbGVjdG9yKTtcbiAgICAgIGZvciAoY29uc3QgbGF6eVJvdXRlIG9mIGxhenlSb3V0ZXMpIHtcbiAgICAgICAgYWxsTGF6eVJvdXRlcy5wdXNoKGxhenlSb3V0ZSk7XG4gICAgICAgIHZpc2l0TGF6eVJvdXRlKGxhenlSb3V0ZS5yZWZlcmVuY2VkTW9kdWxlLCBzZWVuUm91dGVzLCBhbGxMYXp5Um91dGVzKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBhbGxMYXp5Um91dGVzO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBfY3JlYXRlRW1wdHlTdHViKG91dHB1dEN0eDogT3V0cHV0Q29udGV4dCkge1xuICAvLyBOb3RlOiBXZSBuZWVkIHRvIHByb2R1Y2UgYXQgbGVhc3Qgb25lIGltcG9ydCBzdGF0ZW1lbnQgc28gdGhhdFxuICAvLyBUeXBlU2NyaXB0IGtub3dzIHRoYXQgdGhlIGZpbGUgaXMgYW4gZXM2IG1vZHVsZS4gT3RoZXJ3aXNlIG91ciBnZW5lcmF0ZWRcbiAgLy8gZXhwb3J0cyAvIGltcG9ydHMgd29uJ3QgYmUgZW1pdHRlZCBwcm9wZXJseSBieSBUeXBlU2NyaXB0LlxuICBvdXRwdXRDdHguc3RhdGVtZW50cy5wdXNoKG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5Db21wb25lbnRGYWN0b3J5KS50b1N0bXQoKSk7XG59XG5cblxuZnVuY3Rpb24gX3Jlc29sdmVTdHlsZVN0YXRlbWVudHMoXG4gICAgc3ltYm9sUmVzb2x2ZXI6IFN0YXRpY1N5bWJvbFJlc29sdmVyLCBjb21waWxlUmVzdWx0OiBDb21waWxlZFN0eWxlc2hlZXQsIG5lZWRzU2hpbTogYm9vbGVhbixcbiAgICBmaWxlU3VmZml4OiBzdHJpbmcpOiB2b2lkIHtcbiAgY29tcGlsZVJlc3VsdC5kZXBlbmRlbmNpZXMuZm9yRWFjaCgoZGVwKSA9PiB7XG4gICAgZGVwLnNldFZhbHVlKHN5bWJvbFJlc29sdmVyLmdldFN0YXRpY1N5bWJvbChcbiAgICAgICAgX3N0eWxlc01vZHVsZVVybChkZXAubW9kdWxlVXJsLCBuZWVkc1NoaW0sIGZpbGVTdWZmaXgpLCBkZXAubmFtZSkpO1xuICB9KTtcbn1cblxuZnVuY3Rpb24gX3N0eWxlc01vZHVsZVVybChzdHlsZXNoZWV0VXJsOiBzdHJpbmcsIHNoaW06IGJvb2xlYW4sIHN1ZmZpeDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGAke3N0eWxlc2hlZXRVcmx9JHtzaGltID8gJy5zaGltJyA6ICcnfS5uZ3N0eWxlJHtzdWZmaXh9YDtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBOZ0FuYWx5emVkTW9kdWxlcyB7XG4gIG5nTW9kdWxlczogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGFbXTtcbiAgbmdNb2R1bGVCeVBpcGVPckRpcmVjdGl2ZTogTWFwPFN0YXRpY1N5bWJvbCwgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGE+O1xuICBmaWxlczogTmdBbmFseXplZEZpbGVbXTtcbiAgc3ltYm9sc01pc3NpbmdNb2R1bGU/OiBTdGF0aWNTeW1ib2xbXTtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBOZ0FuYWx5emVkRmlsZVdpdGhJbmplY3RhYmxlcyB7XG4gIGZpbGVOYW1lOiBzdHJpbmc7XG4gIGluamVjdGFibGVzOiBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhW107XG4gIHNoYWxsb3dNb2R1bGVzOiBDb21waWxlU2hhbGxvd01vZHVsZU1ldGFkYXRhW107XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgTmdBbmFseXplZEZpbGUge1xuICBmaWxlTmFtZTogc3RyaW5nO1xuICBkaXJlY3RpdmVzOiBTdGF0aWNTeW1ib2xbXTtcbiAgcGlwZXM6IFN0YXRpY1N5bWJvbFtdO1xuICBuZ01vZHVsZXM6IENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhW107XG4gIGluamVjdGFibGVzOiBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhW107XG4gIGV4cG9ydHNOb25Tb3VyY2VGaWxlczogYm9vbGVhbjtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBOZ0FuYWx5emVNb2R1bGVzSG9zdCB7IGlzU291cmNlRmlsZShmaWxlUGF0aDogc3RyaW5nKTogYm9vbGVhbjsgfVxuXG5leHBvcnQgZnVuY3Rpb24gYW5hbHl6ZU5nTW9kdWxlcyhcbiAgICBmaWxlTmFtZXM6IHN0cmluZ1tdLCBob3N0OiBOZ0FuYWx5emVNb2R1bGVzSG9zdCwgc3RhdGljU3ltYm9sUmVzb2x2ZXI6IFN0YXRpY1N5bWJvbFJlc29sdmVyLFxuICAgIG1ldGFkYXRhUmVzb2x2ZXI6IENvbXBpbGVNZXRhZGF0YVJlc29sdmVyKTogTmdBbmFseXplZE1vZHVsZXMge1xuICBjb25zdCBmaWxlcyA9IF9hbmFseXplRmlsZXNJbmNsdWRpbmdOb25Qcm9ncmFtRmlsZXMoXG4gICAgICBmaWxlTmFtZXMsIGhvc3QsIHN0YXRpY1N5bWJvbFJlc29sdmVyLCBtZXRhZGF0YVJlc29sdmVyKTtcbiAgcmV0dXJuIG1lcmdlQW5hbHl6ZWRGaWxlcyhmaWxlcyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhbmFseXplQW5kVmFsaWRhdGVOZ01vZHVsZXMoXG4gICAgZmlsZU5hbWVzOiBzdHJpbmdbXSwgaG9zdDogTmdBbmFseXplTW9kdWxlc0hvc3QsIHN0YXRpY1N5bWJvbFJlc29sdmVyOiBTdGF0aWNTeW1ib2xSZXNvbHZlcixcbiAgICBtZXRhZGF0YVJlc29sdmVyOiBDb21waWxlTWV0YWRhdGFSZXNvbHZlcik6IE5nQW5hbHl6ZWRNb2R1bGVzIHtcbiAgcmV0dXJuIHZhbGlkYXRlQW5hbHl6ZWRNb2R1bGVzKFxuICAgICAgYW5hbHl6ZU5nTW9kdWxlcyhmaWxlTmFtZXMsIGhvc3QsIHN0YXRpY1N5bWJvbFJlc29sdmVyLCBtZXRhZGF0YVJlc29sdmVyKSk7XG59XG5cbmZ1bmN0aW9uIHZhbGlkYXRlQW5hbHl6ZWRNb2R1bGVzKGFuYWx5emVkTW9kdWxlczogTmdBbmFseXplZE1vZHVsZXMpOiBOZ0FuYWx5emVkTW9kdWxlcyB7XG4gIGlmIChhbmFseXplZE1vZHVsZXMuc3ltYm9sc01pc3NpbmdNb2R1bGUgJiYgYW5hbHl6ZWRNb2R1bGVzLnN5bWJvbHNNaXNzaW5nTW9kdWxlLmxlbmd0aCkge1xuICAgIGNvbnN0IG1lc3NhZ2VzID0gYW5hbHl6ZWRNb2R1bGVzLnN5bWJvbHNNaXNzaW5nTW9kdWxlLm1hcChcbiAgICAgICAgcyA9PlxuICAgICAgICAgICAgYENhbm5vdCBkZXRlcm1pbmUgdGhlIG1vZHVsZSBmb3IgY2xhc3MgJHtzLm5hbWV9IGluICR7cy5maWxlUGF0aH0hIEFkZCAke3MubmFtZX0gdG8gdGhlIE5nTW9kdWxlIHRvIGZpeCBpdC5gKTtcbiAgICB0aHJvdyBzeW50YXhFcnJvcihtZXNzYWdlcy5qb2luKCdcXG4nKSk7XG4gIH1cbiAgcmV0dXJuIGFuYWx5emVkTW9kdWxlcztcbn1cblxuLy8gQW5hbHl6ZXMgYWxsIG9mIHRoZSBwcm9ncmFtIGZpbGVzLFxuLy8gaW5jbHVkaW5nIGZpbGVzIHRoYXQgYXJlIG5vdCBwYXJ0IG9mIHRoZSBwcm9ncmFtXG4vLyBidXQgYXJlIHJlZmVyZW5jZWQgYnkgYW4gTmdNb2R1bGUuXG5mdW5jdGlvbiBfYW5hbHl6ZUZpbGVzSW5jbHVkaW5nTm9uUHJvZ3JhbUZpbGVzKFxuICAgIGZpbGVOYW1lczogc3RyaW5nW10sIGhvc3Q6IE5nQW5hbHl6ZU1vZHVsZXNIb3N0LCBzdGF0aWNTeW1ib2xSZXNvbHZlcjogU3RhdGljU3ltYm9sUmVzb2x2ZXIsXG4gICAgbWV0YWRhdGFSZXNvbHZlcjogQ29tcGlsZU1ldGFkYXRhUmVzb2x2ZXIpOiBOZ0FuYWx5emVkRmlsZVtdIHtcbiAgY29uc3Qgc2VlbkZpbGVzID0gbmV3IFNldDxzdHJpbmc+KCk7XG4gIGNvbnN0IGZpbGVzOiBOZ0FuYWx5emVkRmlsZVtdID0gW107XG5cbiAgY29uc3QgdmlzaXRGaWxlID0gKGZpbGVOYW1lOiBzdHJpbmcpID0+IHtcbiAgICBpZiAoc2VlbkZpbGVzLmhhcyhmaWxlTmFtZSkgfHwgIWhvc3QuaXNTb3VyY2VGaWxlKGZpbGVOYW1lKSkge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICBzZWVuRmlsZXMuYWRkKGZpbGVOYW1lKTtcbiAgICBjb25zdCBhbmFseXplZEZpbGUgPSBhbmFseXplRmlsZShob3N0LCBzdGF0aWNTeW1ib2xSZXNvbHZlciwgbWV0YWRhdGFSZXNvbHZlciwgZmlsZU5hbWUpO1xuICAgIGZpbGVzLnB1c2goYW5hbHl6ZWRGaWxlKTtcbiAgICBhbmFseXplZEZpbGUubmdNb2R1bGVzLmZvckVhY2gobmdNb2R1bGUgPT4ge1xuICAgICAgbmdNb2R1bGUudHJhbnNpdGl2ZU1vZHVsZS5tb2R1bGVzLmZvckVhY2gobW9kTWV0YSA9PiB2aXNpdEZpbGUobW9kTWV0YS5yZWZlcmVuY2UuZmlsZVBhdGgpKTtcbiAgICB9KTtcbiAgfTtcbiAgZmlsZU5hbWVzLmZvckVhY2goKGZpbGVOYW1lKSA9PiB2aXNpdEZpbGUoZmlsZU5hbWUpKTtcbiAgcmV0dXJuIGZpbGVzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYW5hbHl6ZUZpbGUoXG4gICAgaG9zdDogTmdBbmFseXplTW9kdWxlc0hvc3QsIHN0YXRpY1N5bWJvbFJlc29sdmVyOiBTdGF0aWNTeW1ib2xSZXNvbHZlcixcbiAgICBtZXRhZGF0YVJlc29sdmVyOiBDb21waWxlTWV0YWRhdGFSZXNvbHZlciwgZmlsZU5hbWU6IHN0cmluZyk6IE5nQW5hbHl6ZWRGaWxlIHtcbiAgY29uc3QgZGlyZWN0aXZlczogU3RhdGljU3ltYm9sW10gPSBbXTtcbiAgY29uc3QgcGlwZXM6IFN0YXRpY1N5bWJvbFtdID0gW107XG4gIGNvbnN0IGluamVjdGFibGVzOiBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhW10gPSBbXTtcbiAgY29uc3QgbmdNb2R1bGVzOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YVtdID0gW107XG4gIGNvbnN0IGhhc0RlY29yYXRvcnMgPSBzdGF0aWNTeW1ib2xSZXNvbHZlci5oYXNEZWNvcmF0b3JzKGZpbGVOYW1lKTtcbiAgbGV0IGV4cG9ydHNOb25Tb3VyY2VGaWxlcyA9IGZhbHNlO1xuICAvLyBEb24ndCBhbmFseXplIC5kLnRzIGZpbGVzIHRoYXQgaGF2ZSBubyBkZWNvcmF0b3JzIGFzIGEgc2hvcnRjdXRcbiAgLy8gdG8gc3BlZWQgdXAgdGhlIGFuYWx5c2lzLiBUaGlzIHByZXZlbnRzIHVzIGZyb21cbiAgLy8gcmVzb2x2aW5nIHRoZSByZWZlcmVuY2VzIGluIHRoZXNlIGZpbGVzLlxuICAvLyBOb3RlOiBleHBvcnRzTm9uU291cmNlRmlsZXMgaXMgb25seSBuZWVkZWQgd2hlbiBjb21waWxpbmcgd2l0aCBzdW1tYXJpZXMsXG4gIC8vIHdoaWNoIGlzIG5vdCB0aGUgY2FzZSB3aGVuIC5kLnRzIGZpbGVzIGFyZSB0cmVhdGVkIGFzIGlucHV0IGZpbGVzLlxuICBpZiAoIWZpbGVOYW1lLmVuZHNXaXRoKCcuZC50cycpIHx8IGhhc0RlY29yYXRvcnMpIHtcbiAgICBzdGF0aWNTeW1ib2xSZXNvbHZlci5nZXRTeW1ib2xzT2YoZmlsZU5hbWUpLmZvckVhY2goKHN5bWJvbCkgPT4ge1xuICAgICAgY29uc3QgcmVzb2x2ZWRTeW1ib2wgPSBzdGF0aWNTeW1ib2xSZXNvbHZlci5yZXNvbHZlU3ltYm9sKHN5bWJvbCk7XG4gICAgICBjb25zdCBzeW1ib2xNZXRhID0gcmVzb2x2ZWRTeW1ib2wubWV0YWRhdGE7XG4gICAgICBpZiAoIXN5bWJvbE1ldGEgfHwgc3ltYm9sTWV0YS5fX3N5bWJvbGljID09PSAnZXJyb3InKSB7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICAgIGxldCBpc05nU3ltYm9sID0gZmFsc2U7XG4gICAgICBpZiAoc3ltYm9sTWV0YS5fX3N5bWJvbGljID09PSAnY2xhc3MnKSB7XG4gICAgICAgIGlmIChtZXRhZGF0YVJlc29sdmVyLmlzRGlyZWN0aXZlKHN5bWJvbCkpIHtcbiAgICAgICAgICBpc05nU3ltYm9sID0gdHJ1ZTtcbiAgICAgICAgICBkaXJlY3RpdmVzLnB1c2goc3ltYm9sKTtcbiAgICAgICAgfSBlbHNlIGlmIChtZXRhZGF0YVJlc29sdmVyLmlzUGlwZShzeW1ib2wpKSB7XG4gICAgICAgICAgaXNOZ1N5bWJvbCA9IHRydWU7XG4gICAgICAgICAgcGlwZXMucHVzaChzeW1ib2wpO1xuICAgICAgICB9IGVsc2UgaWYgKG1ldGFkYXRhUmVzb2x2ZXIuaXNOZ01vZHVsZShzeW1ib2wpKSB7XG4gICAgICAgICAgY29uc3QgbmdNb2R1bGUgPSBtZXRhZGF0YVJlc29sdmVyLmdldE5nTW9kdWxlTWV0YWRhdGEoc3ltYm9sLCBmYWxzZSk7XG4gICAgICAgICAgaWYgKG5nTW9kdWxlKSB7XG4gICAgICAgICAgICBpc05nU3ltYm9sID0gdHJ1ZTtcbiAgICAgICAgICAgIG5nTW9kdWxlcy5wdXNoKG5nTW9kdWxlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSBpZiAobWV0YWRhdGFSZXNvbHZlci5pc0luamVjdGFibGUoc3ltYm9sKSkge1xuICAgICAgICAgIGlzTmdTeW1ib2wgPSB0cnVlO1xuICAgICAgICAgIGNvbnN0IGluamVjdGFibGUgPSBtZXRhZGF0YVJlc29sdmVyLmdldEluamVjdGFibGVNZXRhZGF0YShzeW1ib2wsIG51bGwsIGZhbHNlKTtcbiAgICAgICAgICBpZiAoaW5qZWN0YWJsZSkge1xuICAgICAgICAgICAgaW5qZWN0YWJsZXMucHVzaChpbmplY3RhYmxlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGlmICghaXNOZ1N5bWJvbCkge1xuICAgICAgICBleHBvcnRzTm9uU291cmNlRmlsZXMgPVxuICAgICAgICAgICAgZXhwb3J0c05vblNvdXJjZUZpbGVzIHx8IGlzVmFsdWVFeHBvcnRpbmdOb25Tb3VyY2VGaWxlKGhvc3QsIHN5bWJvbE1ldGEpO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG4gIHJldHVybiB7XG4gICAgICBmaWxlTmFtZSwgZGlyZWN0aXZlcywgcGlwZXMsIG5nTW9kdWxlcywgaW5qZWN0YWJsZXMsIGV4cG9ydHNOb25Tb3VyY2VGaWxlcyxcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFuYWx5emVGaWxlRm9ySW5qZWN0YWJsZXMoXG4gICAgaG9zdDogTmdBbmFseXplTW9kdWxlc0hvc3QsIHN0YXRpY1N5bWJvbFJlc29sdmVyOiBTdGF0aWNTeW1ib2xSZXNvbHZlcixcbiAgICBtZXRhZGF0YVJlc29sdmVyOiBDb21waWxlTWV0YWRhdGFSZXNvbHZlciwgZmlsZU5hbWU6IHN0cmluZyk6IE5nQW5hbHl6ZWRGaWxlV2l0aEluamVjdGFibGVzIHtcbiAgY29uc3QgaW5qZWN0YWJsZXM6IENvbXBpbGVJbmplY3RhYmxlTWV0YWRhdGFbXSA9IFtdO1xuICBjb25zdCBzaGFsbG93TW9kdWxlczogQ29tcGlsZVNoYWxsb3dNb2R1bGVNZXRhZGF0YVtdID0gW107XG4gIGlmIChzdGF0aWNTeW1ib2xSZXNvbHZlci5oYXNEZWNvcmF0b3JzKGZpbGVOYW1lKSkge1xuICAgIHN0YXRpY1N5bWJvbFJlc29sdmVyLmdldFN5bWJvbHNPZihmaWxlTmFtZSkuZm9yRWFjaCgoc3ltYm9sKSA9PiB7XG4gICAgICBjb25zdCByZXNvbHZlZFN5bWJvbCA9IHN0YXRpY1N5bWJvbFJlc29sdmVyLnJlc29sdmVTeW1ib2woc3ltYm9sKTtcbiAgICAgIGNvbnN0IHN5bWJvbE1ldGEgPSByZXNvbHZlZFN5bWJvbC5tZXRhZGF0YTtcbiAgICAgIGlmICghc3ltYm9sTWV0YSB8fCBzeW1ib2xNZXRhLl9fc3ltYm9saWMgPT09ICdlcnJvcicpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgICAgaWYgKHN5bWJvbE1ldGEuX19zeW1ib2xpYyA9PT0gJ2NsYXNzJykge1xuICAgICAgICBpZiAobWV0YWRhdGFSZXNvbHZlci5pc0luamVjdGFibGUoc3ltYm9sKSkge1xuICAgICAgICAgIGNvbnN0IGluamVjdGFibGUgPSBtZXRhZGF0YVJlc29sdmVyLmdldEluamVjdGFibGVNZXRhZGF0YShzeW1ib2wsIG51bGwsIGZhbHNlKTtcbiAgICAgICAgICBpZiAoaW5qZWN0YWJsZSkge1xuICAgICAgICAgICAgaW5qZWN0YWJsZXMucHVzaChpbmplY3RhYmxlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSBpZiAobWV0YWRhdGFSZXNvbHZlci5pc05nTW9kdWxlKHN5bWJvbCkpIHtcbiAgICAgICAgICBjb25zdCBtb2R1bGUgPSBtZXRhZGF0YVJlc29sdmVyLmdldFNoYWxsb3dNb2R1bGVNZXRhZGF0YShzeW1ib2wpO1xuICAgICAgICAgIGlmIChtb2R1bGUpIHtcbiAgICAgICAgICAgIHNoYWxsb3dNb2R1bGVzLnB1c2gobW9kdWxlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuICByZXR1cm4ge2ZpbGVOYW1lLCBpbmplY3RhYmxlcywgc2hhbGxvd01vZHVsZXN9O1xufVxuXG5mdW5jdGlvbiBpc1ZhbHVlRXhwb3J0aW5nTm9uU291cmNlRmlsZShob3N0OiBOZ0FuYWx5emVNb2R1bGVzSG9zdCwgbWV0YWRhdGE6IGFueSk6IGJvb2xlYW4ge1xuICBsZXQgZXhwb3J0c05vblNvdXJjZUZpbGVzID0gZmFsc2U7XG5cbiAgY2xhc3MgVmlzaXRvciBpbXBsZW1lbnRzIFZhbHVlVmlzaXRvciB7XG4gICAgdmlzaXRBcnJheShhcnI6IGFueVtdLCBjb250ZXh0OiBhbnkpOiBhbnkgeyBhcnIuZm9yRWFjaCh2ID0+IHZpc2l0VmFsdWUodiwgdGhpcywgY29udGV4dCkpOyB9XG4gICAgdmlzaXRTdHJpbmdNYXAobWFwOiB7W2tleTogc3RyaW5nXTogYW55fSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICAgIE9iamVjdC5rZXlzKG1hcCkuZm9yRWFjaCgoa2V5KSA9PiB2aXNpdFZhbHVlKG1hcFtrZXldLCB0aGlzLCBjb250ZXh0KSk7XG4gICAgfVxuICAgIHZpc2l0UHJpbWl0aXZlKHZhbHVlOiBhbnksIGNvbnRleHQ6IGFueSk6IGFueSB7fVxuICAgIHZpc2l0T3RoZXIodmFsdWU6IGFueSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCAmJiAhaG9zdC5pc1NvdXJjZUZpbGUodmFsdWUuZmlsZVBhdGgpKSB7XG4gICAgICAgIGV4cG9ydHNOb25Tb3VyY2VGaWxlcyA9IHRydWU7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgdmlzaXRWYWx1ZShtZXRhZGF0YSwgbmV3IFZpc2l0b3IoKSwgbnVsbCk7XG4gIHJldHVybiBleHBvcnRzTm9uU291cmNlRmlsZXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBtZXJnZUFuYWx5emVkRmlsZXMoYW5hbHl6ZWRGaWxlczogTmdBbmFseXplZEZpbGVbXSk6IE5nQW5hbHl6ZWRNb2R1bGVzIHtcbiAgY29uc3QgYWxsTmdNb2R1bGVzOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YVtdID0gW107XG4gIGNvbnN0IG5nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmUgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGE+KCk7XG4gIGNvbnN0IGFsbFBpcGVzQW5kRGlyZWN0aXZlcyA9IG5ldyBTZXQ8U3RhdGljU3ltYm9sPigpO1xuXG4gIGFuYWx5emVkRmlsZXMuZm9yRWFjaChhZiA9PiB7XG4gICAgYWYubmdNb2R1bGVzLmZvckVhY2gobmdNb2R1bGUgPT4ge1xuICAgICAgYWxsTmdNb2R1bGVzLnB1c2gobmdNb2R1bGUpO1xuICAgICAgbmdNb2R1bGUuZGVjbGFyZWREaXJlY3RpdmVzLmZvckVhY2goXG4gICAgICAgICAgZCA9PiBuZ01vZHVsZUJ5UGlwZU9yRGlyZWN0aXZlLnNldChkLnJlZmVyZW5jZSwgbmdNb2R1bGUpKTtcbiAgICAgIG5nTW9kdWxlLmRlY2xhcmVkUGlwZXMuZm9yRWFjaChwID0+IG5nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmUuc2V0KHAucmVmZXJlbmNlLCBuZ01vZHVsZSkpO1xuICAgIH0pO1xuICAgIGFmLmRpcmVjdGl2ZXMuZm9yRWFjaChkID0+IGFsbFBpcGVzQW5kRGlyZWN0aXZlcy5hZGQoZCkpO1xuICAgIGFmLnBpcGVzLmZvckVhY2gocCA9PiBhbGxQaXBlc0FuZERpcmVjdGl2ZXMuYWRkKHApKTtcbiAgfSk7XG5cbiAgY29uc3Qgc3ltYm9sc01pc3NpbmdNb2R1bGU6IFN0YXRpY1N5bWJvbFtdID0gW107XG4gIGFsbFBpcGVzQW5kRGlyZWN0aXZlcy5mb3JFYWNoKHJlZiA9PiB7XG4gICAgaWYgKCFuZ01vZHVsZUJ5UGlwZU9yRGlyZWN0aXZlLmhhcyhyZWYpKSB7XG4gICAgICBzeW1ib2xzTWlzc2luZ01vZHVsZS5wdXNoKHJlZik7XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuIHtcbiAgICBuZ01vZHVsZXM6IGFsbE5nTW9kdWxlcyxcbiAgICBuZ01vZHVsZUJ5UGlwZU9yRGlyZWN0aXZlLFxuICAgIHN5bWJvbHNNaXNzaW5nTW9kdWxlLFxuICAgIGZpbGVzOiBhbmFseXplZEZpbGVzXG4gIH07XG59XG5cbmZ1bmN0aW9uIG1lcmdlQW5kVmFsaWRhdGVOZ0ZpbGVzKGZpbGVzOiBOZ0FuYWx5emVkRmlsZVtdKTogTmdBbmFseXplZE1vZHVsZXMge1xuICByZXR1cm4gdmFsaWRhdGVBbmFseXplZE1vZHVsZXMobWVyZ2VBbmFseXplZEZpbGVzKGZpbGVzKSk7XG59XG4iXX0=