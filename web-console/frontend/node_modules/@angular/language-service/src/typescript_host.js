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
        define("@angular/language-service/src/typescript_host", ["require", "exports", "tslib", "@angular/compiler", "@angular/compiler-cli/src/language_services", "@angular/core", "fs", "path", "typescript", "@angular/language-service/src/language_service", "@angular/language-service/src/reflector_host", "@angular/language-service/src/types"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compiler_1 = require("@angular/compiler");
    var language_services_1 = require("@angular/compiler-cli/src/language_services");
    var core_1 = require("@angular/core");
    var fs = require("fs");
    var path = require("path");
    var ts = require("typescript");
    var language_service_1 = require("@angular/language-service/src/language_service");
    var reflector_host_1 = require("@angular/language-service/src/reflector_host");
    var types_1 = require("@angular/language-service/src/types");
    /**
     * Create a `LanguageServiceHost`
     */
    function createLanguageServiceFromTypescript(host, service) {
        var ngHost = new TypeScriptServiceHost(host, service);
        var ngServer = language_service_1.createLanguageService(ngHost);
        return ngServer;
    }
    exports.createLanguageServiceFromTypescript = createLanguageServiceFromTypescript;
    /**
     * The language service never needs the normalized versions of the metadata. To avoid parsing
     * the content and resolving references, return an empty file. This also allows normalizing
     * template that are syntatically incorrect which is required to provide completions in
     * syntactically incorrect templates.
     */
    var DummyHtmlParser = /** @class */ (function (_super) {
        tslib_1.__extends(DummyHtmlParser, _super);
        function DummyHtmlParser() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        DummyHtmlParser.prototype.parse = function () { return new compiler_1.ParseTreeResult([], []); };
        return DummyHtmlParser;
    }(compiler_1.HtmlParser));
    exports.DummyHtmlParser = DummyHtmlParser;
    /**
     * Avoid loading resources in the language servcie by using a dummy loader.
     */
    var DummyResourceLoader = /** @class */ (function (_super) {
        tslib_1.__extends(DummyResourceLoader, _super);
        function DummyResourceLoader() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        DummyResourceLoader.prototype.get = function (url) { return Promise.resolve(''); };
        return DummyResourceLoader;
    }(compiler_1.ResourceLoader));
    exports.DummyResourceLoader = DummyResourceLoader;
    /**
     * An implementation of a `LanguageServiceHost` for a TypeScript project.
     *
     * The `TypeScriptServiceHost` implements the Angular `LanguageServiceHost` using
     * the TypeScript language services.
     *
     * @publicApi
     */
    var TypeScriptServiceHost = /** @class */ (function () {
        function TypeScriptServiceHost(host, tsService) {
            this.host = host;
            this.tsService = tsService;
            this._staticSymbolCache = new compiler_1.StaticSymbolCache();
            this.modulesOutOfDate = true;
            this.fileToComponent = new Map();
            this.collectedErrors = new Map();
            this.fileVersions = new Map();
        }
        Object.defineProperty(TypeScriptServiceHost.prototype, "resolver", {
            /**
             * Angular LanguageServiceHost implementation
             */
            get: function () {
                var _this = this;
                this.validate();
                var result = this._resolver;
                if (!result) {
                    var moduleResolver = new compiler_1.NgModuleResolver(this.reflector);
                    var directiveResolver = new compiler_1.DirectiveResolver(this.reflector);
                    var pipeResolver = new compiler_1.PipeResolver(this.reflector);
                    var elementSchemaRegistry = new compiler_1.DomElementSchemaRegistry();
                    var resourceLoader = new DummyResourceLoader();
                    var urlResolver = compiler_1.createOfflineCompileUrlResolver();
                    var htmlParser = new DummyHtmlParser();
                    // This tracks the CompileConfig in codegen.ts. Currently these options
                    // are hard-coded.
                    var config = new compiler_1.CompilerConfig({ defaultEncapsulation: core_1.ViewEncapsulation.Emulated, useJit: false });
                    var directiveNormalizer = new compiler_1.DirectiveNormalizer(resourceLoader, urlResolver, htmlParser, config);
                    result = this._resolver = new compiler_1.CompileMetadataResolver(config, htmlParser, moduleResolver, directiveResolver, pipeResolver, new compiler_1.JitSummaryResolver(), elementSchemaRegistry, directiveNormalizer, new core_1.ɵConsole(), this._staticSymbolCache, this.reflector, function (error, type) { return _this.collectError(error, type && type.filePath); });
                }
                return result;
            },
            enumerable: true,
            configurable: true
        });
        TypeScriptServiceHost.prototype.getTemplateReferences = function () {
            this.ensureTemplateMap();
            return this.templateReferences || [];
        };
        TypeScriptServiceHost.prototype.getTemplateAt = function (fileName, position) {
            var sourceFile = this.getSourceFile(fileName);
            if (sourceFile) {
                this.context = sourceFile.fileName;
                var node = this.findNode(sourceFile, position);
                if (node) {
                    return this.getSourceFromNode(fileName, this.host.getScriptVersion(sourceFile.fileName), node);
                }
            }
            else {
                this.ensureTemplateMap();
                // TODO: Cannocalize the file?
                var componentType = this.fileToComponent.get(fileName);
                if (componentType) {
                    return this.getSourceFromType(fileName, this.host.getScriptVersion(fileName), componentType);
                }
            }
            return undefined;
        };
        TypeScriptServiceHost.prototype.getAnalyzedModules = function () {
            this.updateAnalyzedModules();
            return this.ensureAnalyzedModules();
        };
        TypeScriptServiceHost.prototype.ensureAnalyzedModules = function () {
            var analyzedModules = this.analyzedModules;
            if (!analyzedModules) {
                if (this.host.getScriptFileNames().length === 0) {
                    analyzedModules = {
                        files: [],
                        ngModuleByPipeOrDirective: new Map(),
                        ngModules: [],
                    };
                }
                else {
                    var analyzeHost = { isSourceFile: function (filePath) { return true; } };
                    var programFiles = this.program.getSourceFiles().map(function (sf) { return sf.fileName; });
                    analyzedModules =
                        compiler_1.analyzeNgModules(programFiles, analyzeHost, this.staticSymbolResolver, this.resolver);
                }
                this.analyzedModules = analyzedModules;
            }
            return analyzedModules;
        };
        TypeScriptServiceHost.prototype.getTemplates = function (fileName) {
            var _this = this;
            this.ensureTemplateMap();
            var componentType = this.fileToComponent.get(fileName);
            if (componentType) {
                var templateSource = this.getTemplateAt(fileName, 0);
                if (templateSource) {
                    return [templateSource];
                }
            }
            else {
                var version_1 = this.host.getScriptVersion(fileName);
                var result_1 = [];
                // Find each template string in the file
                var visit_1 = function (child) {
                    var templateSource = _this.getSourceFromNode(fileName, version_1, child);
                    if (templateSource) {
                        result_1.push(templateSource);
                    }
                    else {
                        ts.forEachChild(child, visit_1);
                    }
                };
                var sourceFile = this.getSourceFile(fileName);
                if (sourceFile) {
                    this.context = sourceFile.path || sourceFile.fileName;
                    ts.forEachChild(sourceFile, visit_1);
                }
                return result_1.length ? result_1 : undefined;
            }
        };
        TypeScriptServiceHost.prototype.getDeclarations = function (fileName) {
            var _this = this;
            var result = [];
            var sourceFile = this.getSourceFile(fileName);
            if (sourceFile) {
                var visit_2 = function (child) {
                    var declaration = _this.getDeclarationFromNode(sourceFile, child);
                    if (declaration) {
                        result.push(declaration);
                    }
                    else {
                        ts.forEachChild(child, visit_2);
                    }
                };
                ts.forEachChild(sourceFile, visit_2);
            }
            return result;
        };
        TypeScriptServiceHost.prototype.getSourceFile = function (fileName) {
            return this.tsService.getProgram().getSourceFile(fileName);
        };
        TypeScriptServiceHost.prototype.updateAnalyzedModules = function () {
            this.validate();
            if (this.modulesOutOfDate) {
                this.analyzedModules = null;
                this._reflector = null;
                this.templateReferences = null;
                this.fileToComponent.clear();
                this.ensureAnalyzedModules();
                this.modulesOutOfDate = false;
            }
        };
        Object.defineProperty(TypeScriptServiceHost.prototype, "program", {
            get: function () { return this.tsService.getProgram(); },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(TypeScriptServiceHost.prototype, "checker", {
            get: function () {
                var checker = this._checker;
                if (!checker) {
                    checker = this._checker = this.program.getTypeChecker();
                }
                return checker;
            },
            enumerable: true,
            configurable: true
        });
        TypeScriptServiceHost.prototype.validate = function () {
            var e_1, _a;
            var _this = this;
            var program = this.program;
            if (this.lastProgram !== program) {
                // Invalidate file that have changed in the static symbol resolver
                var invalidateFile = function (fileName) {
                    return _this._staticSymbolResolver.invalidateFile(fileName);
                };
                this.clearCaches();
                var seen_1 = new Set();
                try {
                    for (var _b = tslib_1.__values(this.program.getSourceFiles()), _c = _b.next(); !_c.done; _c = _b.next()) {
                        var sourceFile = _c.value;
                        var fileName = sourceFile.fileName;
                        seen_1.add(fileName);
                        var version = this.host.getScriptVersion(fileName);
                        var lastVersion = this.fileVersions.get(fileName);
                        if (version != lastVersion) {
                            this.fileVersions.set(fileName, version);
                            if (this._staticSymbolResolver) {
                                invalidateFile(fileName);
                            }
                        }
                    }
                }
                catch (e_1_1) { e_1 = { error: e_1_1 }; }
                finally {
                    try {
                        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                    }
                    finally { if (e_1) throw e_1.error; }
                }
                // Remove file versions that are no longer in the file and invalidate them.
                var missing = Array.from(this.fileVersions.keys()).filter(function (f) { return !seen_1.has(f); });
                missing.forEach(function (f) { return _this.fileVersions.delete(f); });
                if (this._staticSymbolResolver) {
                    missing.forEach(invalidateFile);
                }
                this.lastProgram = program;
            }
        };
        TypeScriptServiceHost.prototype.clearCaches = function () {
            this._checker = null;
            this._resolver = null;
            this.collectedErrors.clear();
            this.modulesOutOfDate = true;
        };
        TypeScriptServiceHost.prototype.ensureTemplateMap = function () {
            var e_2, _a, e_3, _b;
            if (!this.templateReferences) {
                var templateReference = [];
                var ngModuleSummary = this.getAnalyzedModules();
                var urlResolver = compiler_1.createOfflineCompileUrlResolver();
                try {
                    for (var _c = tslib_1.__values(ngModuleSummary.ngModules), _d = _c.next(); !_d.done; _d = _c.next()) {
                        var module_1 = _d.value;
                        try {
                            for (var _e = (e_3 = void 0, tslib_1.__values(module_1.declaredDirectives)), _f = _e.next(); !_f.done; _f = _e.next()) {
                                var directive = _f.value;
                                var metadata = this.resolver.getNonNormalizedDirectiveMetadata(directive.reference).metadata;
                                if (metadata.isComponent && metadata.template && metadata.template.templateUrl) {
                                    var templateName = urlResolver.resolve(this.reflector.componentModuleUrl(directive.reference), metadata.template.templateUrl);
                                    this.fileToComponent.set(templateName, directive.reference);
                                    templateReference.push(templateName);
                                }
                            }
                        }
                        catch (e_3_1) { e_3 = { error: e_3_1 }; }
                        finally {
                            try {
                                if (_f && !_f.done && (_b = _e.return)) _b.call(_e);
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
                this.templateReferences = templateReference;
            }
        };
        TypeScriptServiceHost.prototype.getSourceFromDeclaration = function (fileName, version, source, span, type, declaration, node, sourceFile) {
            var queryCache = undefined;
            var t = this;
            if (declaration) {
                return {
                    version: version,
                    source: source,
                    span: span,
                    type: type,
                    get members() {
                        return language_services_1.getClassMembersFromDeclaration(t.program, t.checker, sourceFile, declaration);
                    },
                    get query() {
                        if (!queryCache) {
                            var pipes_1 = [];
                            var templateInfo = t.getTemplateAstAtPosition(fileName, node.getStart());
                            if (templateInfo) {
                                pipes_1 = templateInfo.pipes;
                            }
                            queryCache = language_services_1.getSymbolQuery(t.program, t.checker, sourceFile, function () { return language_services_1.getPipesTable(sourceFile, t.program, t.checker, pipes_1); });
                        }
                        return queryCache;
                    }
                };
            }
        };
        TypeScriptServiceHost.prototype.getSourceFromNode = function (fileName, version, node) {
            var result = undefined;
            var t = this;
            switch (node.kind) {
                case ts.SyntaxKind.NoSubstitutionTemplateLiteral:
                case ts.SyntaxKind.StringLiteral:
                    var _a = tslib_1.__read(this.getTemplateClassDeclFromNode(node), 2), declaration = _a[0], decorator = _a[1];
                    if (declaration && declaration.name) {
                        var sourceFile = this.getSourceFile(fileName);
                        if (sourceFile) {
                            return this.getSourceFromDeclaration(fileName, version, this.stringOf(node) || '', shrink(spanOf(node)), this.reflector.getStaticSymbol(sourceFile.fileName, declaration.name.text), declaration, node, sourceFile);
                        }
                    }
                    break;
            }
            return result;
        };
        TypeScriptServiceHost.prototype.getSourceFromType = function (fileName, version, type) {
            var result = undefined;
            var declaration = this.getTemplateClassFromStaticSymbol(type);
            if (declaration) {
                var snapshot = this.host.getScriptSnapshot(fileName);
                if (snapshot) {
                    var source = snapshot.getText(0, snapshot.getLength());
                    result = this.getSourceFromDeclaration(fileName, version, source, { start: 0, end: source.length }, type, declaration, declaration, declaration.getSourceFile());
                }
            }
            return result;
        };
        Object.defineProperty(TypeScriptServiceHost.prototype, "reflectorHost", {
            get: function () {
                var _this = this;
                var result = this._reflectorHost;
                if (!result) {
                    if (!this.context) {
                        // Make up a context by finding the first script and using that as the base dir.
                        var scriptFileNames = this.host.getScriptFileNames();
                        if (0 === scriptFileNames.length) {
                            throw new Error('Internal error: no script file names found');
                        }
                        this.context = scriptFileNames[0];
                    }
                    // Use the file context's directory as the base directory.
                    // The host's getCurrentDirectory() is not reliable as it is always "" in
                    // tsserver. We don't need the exact base directory, just one that contains
                    // a source file.
                    var source = this.tsService.getProgram().getSourceFile(this.context);
                    if (!source) {
                        throw new Error('Internal error: no context could be determined');
                    }
                    var tsConfigPath = findTsConfig(source.fileName);
                    var basePath = path.dirname(tsConfigPath || this.context);
                    var options = { basePath: basePath, genDir: basePath };
                    var compilerOptions = this.host.getCompilationSettings();
                    if (compilerOptions && compilerOptions.baseUrl) {
                        options.baseUrl = compilerOptions.baseUrl;
                    }
                    if (compilerOptions && compilerOptions.paths) {
                        options.paths = compilerOptions.paths;
                    }
                    result = this._reflectorHost =
                        new reflector_host_1.ReflectorHost(function () { return _this.tsService.getProgram(); }, this.host, options);
                }
                return result;
            },
            enumerable: true,
            configurable: true
        });
        TypeScriptServiceHost.prototype.collectError = function (error, filePath) {
            if (filePath) {
                var errors = this.collectedErrors.get(filePath);
                if (!errors) {
                    errors = [];
                    this.collectedErrors.set(filePath, errors);
                }
                errors.push(error);
            }
        };
        Object.defineProperty(TypeScriptServiceHost.prototype, "staticSymbolResolver", {
            get: function () {
                var _this = this;
                var result = this._staticSymbolResolver;
                if (!result) {
                    this._summaryResolver = new compiler_1.AotSummaryResolver({
                        loadSummary: function (filePath) { return null; },
                        isSourceFile: function (sourceFilePath) { return true; },
                        toSummaryFileName: function (sourceFilePath) { return sourceFilePath; },
                        fromSummaryFileName: function (filePath) { return filePath; },
                    }, this._staticSymbolCache);
                    result = this._staticSymbolResolver = new compiler_1.StaticSymbolResolver(this.reflectorHost, this._staticSymbolCache, this._summaryResolver, function (e, filePath) { return _this.collectError(e, filePath); });
                }
                return result;
            },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(TypeScriptServiceHost.prototype, "reflector", {
            get: function () {
                var _this = this;
                var result = this._reflector;
                if (!result) {
                    var ssr = this.staticSymbolResolver;
                    result = this._reflector = new compiler_1.StaticReflector(this._summaryResolver, ssr, [], [], function (e, filePath) { return _this.collectError(e, filePath); });
                }
                return result;
            },
            enumerable: true,
            configurable: true
        });
        TypeScriptServiceHost.prototype.getTemplateClassFromStaticSymbol = function (type) {
            var source = this.getSourceFile(type.filePath);
            if (source) {
                var declarationNode = ts.forEachChild(source, function (child) {
                    if (child.kind === ts.SyntaxKind.ClassDeclaration) {
                        var classDeclaration = child;
                        if (classDeclaration.name != null && classDeclaration.name.text === type.name) {
                            return classDeclaration;
                        }
                    }
                });
                return declarationNode;
            }
            return undefined;
        };
        /**
         * Given a template string node, see if it is an Angular template string, and if so return the
         * containing class.
         */
        TypeScriptServiceHost.prototype.getTemplateClassDeclFromNode = function (currentToken) {
            // Verify we are in a 'template' property assignment, in an object literal, which is an call
            // arg, in a decorator
            var parentNode = currentToken.parent; // PropertyAssignment
            if (!parentNode) {
                return TypeScriptServiceHost.missingTemplate;
            }
            if (parentNode.kind !== ts.SyntaxKind.PropertyAssignment) {
                return TypeScriptServiceHost.missingTemplate;
            }
            else {
                // TODO: Is this different for a literal, i.e. a quoted property name like "template"?
                if (parentNode.name.text !== 'template') {
                    return TypeScriptServiceHost.missingTemplate;
                }
            }
            parentNode = parentNode.parent; // ObjectLiteralExpression
            if (!parentNode || parentNode.kind !== ts.SyntaxKind.ObjectLiteralExpression) {
                return TypeScriptServiceHost.missingTemplate;
            }
            parentNode = parentNode.parent; // CallExpression
            if (!parentNode || parentNode.kind !== ts.SyntaxKind.CallExpression) {
                return TypeScriptServiceHost.missingTemplate;
            }
            var callTarget = parentNode.expression;
            var decorator = parentNode.parent; // Decorator
            if (!decorator || decorator.kind !== ts.SyntaxKind.Decorator) {
                return TypeScriptServiceHost.missingTemplate;
            }
            var declaration = decorator.parent; // ClassDeclaration
            if (!declaration || declaration.kind !== ts.SyntaxKind.ClassDeclaration) {
                return TypeScriptServiceHost.missingTemplate;
            }
            return [declaration, callTarget];
        };
        TypeScriptServiceHost.prototype.getCollectedErrors = function (defaultSpan, sourceFile) {
            var errors = this.collectedErrors.get(sourceFile.fileName);
            return (errors && errors.map(function (e) {
                var line = e.line || (e.position && e.position.line);
                var column = e.column || (e.position && e.position.column);
                var span = spanAt(sourceFile, line, column) || defaultSpan;
                if (compiler_1.isFormattedError(e)) {
                    return errorToDiagnosticWithChain(e, span);
                }
                return { message: e.message, span: span };
            })) ||
                [];
        };
        TypeScriptServiceHost.prototype.getDeclarationFromNode = function (sourceFile, node) {
            var e_4, _a;
            if (node.kind == ts.SyntaxKind.ClassDeclaration && node.decorators &&
                node.name) {
                try {
                    for (var _b = tslib_1.__values(node.decorators), _c = _b.next(); !_c.done; _c = _b.next()) {
                        var decorator = _c.value;
                        if (decorator.expression && decorator.expression.kind == ts.SyntaxKind.CallExpression) {
                            var classDeclaration = node;
                            if (classDeclaration.name) {
                                var call = decorator.expression;
                                var target = call.expression;
                                var type = this.checker.getTypeAtLocation(target);
                                if (type) {
                                    var staticSymbol = this.reflector.getStaticSymbol(sourceFile.fileName, classDeclaration.name.text);
                                    try {
                                        if (this.resolver.isDirective(staticSymbol)) {
                                            var metadata = this.resolver.getNonNormalizedDirectiveMetadata(staticSymbol).metadata;
                                            var declarationSpan = spanOf(target);
                                            return {
                                                type: staticSymbol,
                                                declarationSpan: declarationSpan,
                                                metadata: metadata,
                                                errors: this.getCollectedErrors(declarationSpan, sourceFile)
                                            };
                                        }
                                    }
                                    catch (e) {
                                        if (e.message) {
                                            this.collectError(e, sourceFile.fileName);
                                            var declarationSpan = spanOf(target);
                                            return {
                                                type: staticSymbol,
                                                declarationSpan: declarationSpan,
                                                errors: this.getCollectedErrors(declarationSpan, sourceFile)
                                            };
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch (e_4_1) { e_4 = { error: e_4_1 }; }
                finally {
                    try {
                        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                    }
                    finally { if (e_4) throw e_4.error; }
                }
            }
        };
        TypeScriptServiceHost.prototype.stringOf = function (node) {
            switch (node.kind) {
                case ts.SyntaxKind.NoSubstitutionTemplateLiteral:
                    return node.text;
                case ts.SyntaxKind.StringLiteral:
                    return node.text;
            }
        };
        TypeScriptServiceHost.prototype.findNode = function (sourceFile, position) {
            function find(node) {
                if (position >= node.getStart() && position < node.getEnd()) {
                    return ts.forEachChild(node, find) || node;
                }
            }
            return find(sourceFile);
        };
        TypeScriptServiceHost.prototype.getTemplateAstAtPosition = function (fileName, position) {
            var template = this.getTemplateAt(fileName, position);
            if (template) {
                var astResult = this.getTemplateAst(template, fileName);
                if (astResult && astResult.htmlAst && astResult.templateAst && astResult.directive &&
                    astResult.directives && astResult.pipes && astResult.expressionParser)
                    return {
                        position: position,
                        fileName: fileName,
                        template: template,
                        htmlAst: astResult.htmlAst,
                        directive: astResult.directive,
                        directives: astResult.directives,
                        pipes: astResult.pipes,
                        templateAst: astResult.templateAst,
                        expressionParser: astResult.expressionParser
                    };
            }
            return undefined;
        };
        TypeScriptServiceHost.prototype.getTemplateAst = function (template, contextFile) {
            var _this = this;
            var result = undefined;
            try {
                var resolvedMetadata = this.resolver.getNonNormalizedDirectiveMetadata(template.type);
                var metadata = resolvedMetadata && resolvedMetadata.metadata;
                if (metadata) {
                    var rawHtmlParser = new compiler_1.HtmlParser();
                    var htmlParser = new compiler_1.I18NHtmlParser(rawHtmlParser);
                    var expressionParser = new compiler_1.Parser(new compiler_1.Lexer());
                    var config = new compiler_1.CompilerConfig();
                    var parser = new compiler_1.TemplateParser(config, this.resolver.getReflector(), expressionParser, new compiler_1.DomElementSchemaRegistry(), htmlParser, null, []);
                    var htmlResult = htmlParser.parse(template.source, '', { tokenizeExpansionForms: true });
                    var analyzedModules = this.getAnalyzedModules();
                    var errors = undefined;
                    var ngModule = analyzedModules.ngModuleByPipeOrDirective.get(template.type);
                    if (!ngModule) {
                        // Reported by the the declaration diagnostics.
                        ngModule = findSuitableDefaultModule(analyzedModules);
                    }
                    if (ngModule) {
                        var directives = ngModule.transitiveModule.directives
                            .map(function (d) { return _this.resolver.getNonNormalizedDirectiveMetadata(d.reference); })
                            .filter(function (d) { return d; })
                            .map(function (d) { return d.metadata.toSummary(); });
                        var pipes = ngModule.transitiveModule.pipes.map(function (p) { return _this.resolver.getOrLoadPipeMetadata(p.reference).toSummary(); });
                        var schemas = ngModule.schemas;
                        var parseResult = parser.tryParseHtml(htmlResult, metadata, directives, pipes, schemas);
                        result = {
                            htmlAst: htmlResult.rootNodes,
                            templateAst: parseResult.templateAst,
                            directive: metadata, directives: directives, pipes: pipes,
                            parseErrors: parseResult.errors, expressionParser: expressionParser, errors: errors
                        };
                    }
                }
            }
            catch (e) {
                var span = template.span;
                if (e.fileName == contextFile) {
                    span = template.query.getSpanAt(e.line, e.column) || span;
                }
                result = { errors: [{ kind: types_1.DiagnosticKind.Error, message: e.message, span: span }] };
            }
            return result || {};
        };
        TypeScriptServiceHost.missingTemplate = [undefined, undefined];
        return TypeScriptServiceHost;
    }());
    exports.TypeScriptServiceHost = TypeScriptServiceHost;
    function findSuitableDefaultModule(modules) {
        var e_5, _a;
        var result = undefined;
        var resultSize = 0;
        try {
            for (var _b = tslib_1.__values(modules.ngModules), _c = _b.next(); !_c.done; _c = _b.next()) {
                var module_2 = _c.value;
                var moduleSize = module_2.transitiveModule.directives.length;
                if (moduleSize > resultSize) {
                    result = module_2;
                    resultSize = moduleSize;
                }
            }
        }
        catch (e_5_1) { e_5 = { error: e_5_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_5) throw e_5.error; }
        }
        return result;
    }
    function findTsConfig(fileName) {
        var dir = path.dirname(fileName);
        while (fs.existsSync(dir)) {
            var candidate = path.join(dir, 'tsconfig.json');
            if (fs.existsSync(candidate))
                return candidate;
            var parentDir = path.dirname(dir);
            if (parentDir === dir)
                break;
            dir = parentDir;
        }
    }
    function spanOf(node) {
        return { start: node.getStart(), end: node.getEnd() };
    }
    function shrink(span, offset) {
        if (offset == null)
            offset = 1;
        return { start: span.start + offset, end: span.end - offset };
    }
    function spanAt(sourceFile, line, column) {
        if (line != null && column != null) {
            var position_1 = ts.getPositionOfLineAndCharacter(sourceFile, line, column);
            var findChild = function findChild(node) {
                if (node.kind > ts.SyntaxKind.LastToken && node.pos <= position_1 && node.end > position_1) {
                    var betterNode = ts.forEachChild(node, findChild);
                    return betterNode || node;
                }
            };
            var node = ts.forEachChild(sourceFile, findChild);
            if (node) {
                return { start: node.getStart(), end: node.getEnd() };
            }
        }
    }
    function convertChain(chain) {
        return { message: chain.message, next: chain.next ? convertChain(chain.next) : undefined };
    }
    function errorToDiagnosticWithChain(error, span) {
        return { message: error.chain ? convertChain(error.chain) : error.message, span: span };
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHlwZXNjcmlwdF9ob3N0LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvbGFuZ3VhZ2Utc2VydmljZS9zcmMvdHlwZXNjcmlwdF9ob3N0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILDhDQUFvaUI7SUFDcGlCLGlGQUEySTtJQUMzSSxzQ0FBcUU7SUFDckUsdUJBQXlCO0lBQ3pCLDJCQUE2QjtJQUM3QiwrQkFBaUM7SUFHakMsbUZBQXlEO0lBQ3pELCtFQUErQztJQUMvQyw2REFBME47SUFJMU47O09BRUc7SUFDSCxTQUFnQixtQ0FBbUMsQ0FDL0MsSUFBNEIsRUFBRSxPQUEyQjtRQUMzRCxJQUFNLE1BQU0sR0FBRyxJQUFJLHFCQUFxQixDQUFDLElBQUksRUFBRSxPQUFPLENBQUMsQ0FBQztRQUN4RCxJQUFNLFFBQVEsR0FBRyx3Q0FBcUIsQ0FBQyxNQUFNLENBQUMsQ0FBQztRQUMvQyxPQUFPLFFBQVEsQ0FBQztJQUNsQixDQUFDO0lBTEQsa0ZBS0M7SUFFRDs7Ozs7T0FLRztJQUNIO1FBQXFDLDJDQUFVO1FBQS9DOztRQUVBLENBQUM7UUFEQywrQkFBSyxHQUFMLGNBQTJCLE9BQU8sSUFBSSwwQkFBZSxDQUFDLEVBQUUsRUFBRSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDbEUsc0JBQUM7SUFBRCxDQUFDLEFBRkQsQ0FBcUMscUJBQVUsR0FFOUM7SUFGWSwwQ0FBZTtJQUk1Qjs7T0FFRztJQUNIO1FBQXlDLCtDQUFjO1FBQXZEOztRQUVBLENBQUM7UUFEQyxpQ0FBRyxHQUFILFVBQUksR0FBVyxJQUFxQixPQUFPLE9BQU8sQ0FBQyxPQUFPLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ25FLDBCQUFDO0lBQUQsQ0FBQyxBQUZELENBQXlDLHlCQUFjLEdBRXREO0lBRlksa0RBQW1CO0lBSWhDOzs7Ozs7O09BT0c7SUFD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SUFBSSxHQUFHLEVBQUUsQ0FBQyxZQUFZLENBQUMsVUFBVSxFQUFFLFNBQVMsQ0FBQyxDQUFDO1lBQ3BELElBQUksSUFBSSxFQUFFO2dCQUNSLE9BQU8sRUFBQyxLQUFLLEVBQUUsSUFBSSxDQUFDLFFBQVEsRUFBRSxFQUFFLEdBQUcsRUFBRSxJQUFJLENBQUMsTUFBTSxFQUFFLEVBQUMsQ0FBQzthQUNyRDtTQUNGO0lBQ0gsQ0FBQztJQUVELFNBQVMsWUFBWSxDQUFDLEtBQTRCO1FBQ2hELE9BQU8sRUFBQyxPQUFPLEVBQUUsS0FBSyxDQUFDLE9BQU8sRUFBRSxJQUFJLEVBQUUsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsWUFBWSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsU0FBUyxFQUFDLENBQUM7SUFDM0YsQ0FBQztJQUVELFNBQVMsMEJBQTBCLENBQUMsS0FBcUIsRUFBRSxJQUFVO1FBQ25FLE9BQU8sRUFBQyxPQUFPLEVBQUUsS0FBSyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsWUFBWSxDQUFDLEtBQUssQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLE9BQU8sRUFBRSxJQUFJLE1BQUEsRUFBQyxDQUFDO0lBQ2xGLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7QW90U3VtbWFyeVJlc29sdmVyLCBDb21waWxlTWV0YWRhdGFSZXNvbHZlciwgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsIENvbXBpbGVQaXBlU3VtbWFyeSwgQ29tcGlsZXJDb25maWcsIERpcmVjdGl2ZU5vcm1hbGl6ZXIsIERpcmVjdGl2ZVJlc29sdmVyLCBEb21FbGVtZW50U2NoZW1hUmVnaXN0cnksIEZvcm1hdHRlZEVycm9yLCBGb3JtYXR0ZWRNZXNzYWdlQ2hhaW4sIEh0bWxQYXJzZXIsIEkxOE5IdG1sUGFyc2VyLCBKaXRTdW1tYXJ5UmVzb2x2ZXIsIExleGVyLCBOZ0FuYWx5emVkTW9kdWxlcywgTmdNb2R1bGVSZXNvbHZlciwgUGFyc2VUcmVlUmVzdWx0LCBQYXJzZXIsIFBpcGVSZXNvbHZlciwgUmVzb3VyY2VMb2FkZXIsIFN0YXRpY1JlZmxlY3RvciwgU3RhdGljU3ltYm9sLCBTdGF0aWNTeW1ib2xDYWNoZSwgU3RhdGljU3ltYm9sUmVzb2x2ZXIsIFRlbXBsYXRlUGFyc2VyLCBhbmFseXplTmdNb2R1bGVzLCBjcmVhdGVPZmZsaW5lQ29tcGlsZVVybFJlc29sdmVyLCBpc0Zvcm1hdHRlZEVycm9yfSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5pbXBvcnQge0NvbXBpbGVyT3B0aW9ucywgZ2V0Q2xhc3NNZW1iZXJzRnJvbURlY2xhcmF0aW9uLCBnZXRQaXBlc1RhYmxlLCBnZXRTeW1ib2xRdWVyeX0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXItY2xpL3NyYy9sYW5ndWFnZV9zZXJ2aWNlcyc7XG5pbXBvcnQge1ZpZXdFbmNhcHN1bGF0aW9uLCDJtUNvbnNvbGUgYXMgQ29uc29sZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgKiBhcyBmcyBmcm9tICdmcyc7XG5pbXBvcnQgKiBhcyBwYXRoIGZyb20gJ3BhdGgnO1xuaW1wb3J0ICogYXMgdHMgZnJvbSAndHlwZXNjcmlwdCc7XG5cbmltcG9ydCB7QXN0UmVzdWx0LCBUZW1wbGF0ZUluZm99IGZyb20gJy4vY29tbW9uJztcbmltcG9ydCB7Y3JlYXRlTGFuZ3VhZ2VTZXJ2aWNlfSBmcm9tICcuL2xhbmd1YWdlX3NlcnZpY2UnO1xuaW1wb3J0IHtSZWZsZWN0b3JIb3N0fSBmcm9tICcuL3JlZmxlY3Rvcl9ob3N0JztcbmltcG9ydCB7RGVjbGFyYXRpb24sIERlY2xhcmF0aW9uRXJyb3IsIERlY2xhcmF0aW9ucywgRGlhZ25vc3RpYywgRGlhZ25vc3RpY0tpbmQsIERpYWdub3N0aWNNZXNzYWdlQ2hhaW4sIExhbmd1YWdlU2VydmljZSwgTGFuZ3VhZ2VTZXJ2aWNlSG9zdCwgU3BhbiwgU3ltYm9sLCBTeW1ib2xRdWVyeSwgVGVtcGxhdGVTb3VyY2UsIFRlbXBsYXRlU291cmNlc30gZnJvbSAnLi90eXBlcyc7XG5cblxuXG4vKipcbiAqIENyZWF0ZSBhIGBMYW5ndWFnZVNlcnZpY2VIb3N0YFxuICovXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlTGFuZ3VhZ2VTZXJ2aWNlRnJvbVR5cGVzY3JpcHQoXG4gICAgaG9zdDogdHMuTGFuZ3VhZ2VTZXJ2aWNlSG9zdCwgc2VydmljZTogdHMuTGFuZ3VhZ2VTZXJ2aWNlKTogTGFuZ3VhZ2VTZXJ2aWNlIHtcbiAgY29uc3QgbmdIb3N0ID0gbmV3IFR5cGVTY3JpcHRTZXJ2aWNlSG9zdChob3N0LCBzZXJ2aWNlKTtcbiAgY29uc3QgbmdTZXJ2ZXIgPSBjcmVhdGVMYW5ndWFnZVNlcnZpY2UobmdIb3N0KTtcbiAgcmV0dXJuIG5nU2VydmVyO1xufVxuXG4vKipcbiAqIFRoZSBsYW5ndWFnZSBzZXJ2aWNlIG5ldmVyIG5lZWRzIHRoZSBub3JtYWxpemVkIHZlcnNpb25zIG9mIHRoZSBtZXRhZGF0YS4gVG8gYXZvaWQgcGFyc2luZ1xuICogdGhlIGNvbnRlbnQgYW5kIHJlc29sdmluZyByZWZlcmVuY2VzLCByZXR1cm4gYW4gZW1wdHkgZmlsZS4gVGhpcyBhbHNvIGFsbG93cyBub3JtYWxpemluZ1xuICogdGVtcGxhdGUgdGhhdCBhcmUgc3ludGF0aWNhbGx5IGluY29ycmVjdCB3aGljaCBpcyByZXF1aXJlZCB0byBwcm92aWRlIGNvbXBsZXRpb25zIGluXG4gKiBzeW50YWN0aWNhbGx5IGluY29ycmVjdCB0ZW1wbGF0ZXMuXG4gKi9cbmV4cG9ydCBjbGFzcyBEdW1teUh0bWxQYXJzZXIgZXh0ZW5kcyBIdG1sUGFyc2VyIHtcbiAgcGFyc2UoKTogUGFyc2VUcmVlUmVzdWx0IHsgcmV0dXJuIG5ldyBQYXJzZVRyZWVSZXN1bHQoW10sIFtdKTsgfVxufVxuXG4vKipcbiAqIEF2b2lkIGxvYWRpbmcgcmVzb3VyY2VzIGluIHRoZSBsYW5ndWFnZSBzZXJ2Y2llIGJ5IHVzaW5nIGEgZHVtbXkgbG9hZGVyLlxuICovXG5leHBvcnQgY2xhc3MgRHVtbXlSZXNvdXJjZUxvYWRlciBleHRlbmRzIFJlc291cmNlTG9hZGVyIHtcbiAgZ2V0KHVybDogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmc+IHsgcmV0dXJuIFByb21pc2UucmVzb2x2ZSgnJyk7IH1cbn1cblxuLyoqXG4gKiBBbiBpbXBsZW1lbnRhdGlvbiBvZiBhIGBMYW5ndWFnZVNlcnZpY2VIb3N0YCBmb3IgYSBUeXBlU2NyaXB0IHByb2plY3QuXG4gKlxuICogVGhlIGBUeXBlU2NyaXB0U2VydmljZUhvc3RgIGltcGxlbWVudHMgdGhlIEFuZ3VsYXIgYExhbmd1YWdlU2VydmljZUhvc3RgIHVzaW5nXG4gKiB0aGUgVHlwZVNjcmlwdCBsYW5ndWFnZSBzZXJ2aWNlcy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBUeXBlU2NyaXB0U2VydmljZUhvc3QgaW1wbGVtZW50cyBMYW5ndWFnZVNlcnZpY2VIb3N0IHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX3Jlc29sdmVyICE6IENvbXBpbGVNZXRhZGF0YVJlc29sdmVyIHwgbnVsbDtcbiAgcHJpdmF0ZSBfc3RhdGljU3ltYm9sQ2FjaGUgPSBuZXcgU3RhdGljU3ltYm9sQ2FjaGUoKTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX3N1bW1hcnlSZXNvbHZlciAhOiBBb3RTdW1tYXJ5UmVzb2x2ZXI7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9zdGF0aWNTeW1ib2xSZXNvbHZlciAhOiBTdGF0aWNTeW1ib2xSZXNvbHZlcjtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX3JlZmxlY3RvciAhOiBTdGF0aWNSZWZsZWN0b3IgfCBudWxsO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfcmVmbGVjdG9ySG9zdCAhOiBSZWZsZWN0b3JIb3N0O1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfY2hlY2tlciAhOiB0cy5UeXBlQ2hlY2tlciB8IG51bGw7XG4gIHByaXZhdGUgY29udGV4dDogc3RyaW5nfHVuZGVmaW5lZDtcbiAgcHJpdmF0ZSBsYXN0UHJvZ3JhbTogdHMuUHJvZ3JhbXx1bmRlZmluZWQ7XG4gIHByaXZhdGUgbW9kdWxlc091dE9mRGF0ZTogYm9vbGVhbiA9IHRydWU7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIGFuYWx5emVkTW9kdWxlcyAhOiBOZ0FuYWx5emVkTW9kdWxlcyB8IG51bGw7XG4gIHByaXZhdGUgZmlsZVRvQ29tcG9uZW50ID0gbmV3IE1hcDxzdHJpbmcsIFN0YXRpY1N5bWJvbD4oKTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgdGVtcGxhdGVSZWZlcmVuY2VzICE6IHN0cmluZ1tdIHwgbnVsbDtcbiAgcHJpdmF0ZSBjb2xsZWN0ZWRFcnJvcnMgPSBuZXcgTWFwPHN0cmluZywgYW55W10+KCk7XG4gIHByaXZhdGUgZmlsZVZlcnNpb25zID0gbmV3IE1hcDxzdHJpbmcsIHN0cmluZz4oKTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGhvc3Q6IHRzLkxhbmd1YWdlU2VydmljZUhvc3QsIHByaXZhdGUgdHNTZXJ2aWNlOiB0cy5MYW5ndWFnZVNlcnZpY2UpIHt9XG5cbiAgLyoqXG4gICAqIEFuZ3VsYXIgTGFuZ3VhZ2VTZXJ2aWNlSG9zdCBpbXBsZW1lbnRhdGlvblxuICAgKi9cbiAgZ2V0IHJlc29sdmVyKCk6IENvbXBpbGVNZXRhZGF0YVJlc29sdmVyIHtcbiAgICB0aGlzLnZhbGlkYXRlKCk7XG4gICAgbGV0IHJlc3VsdCA9IHRoaXMuX3Jlc29sdmVyO1xuICAgIGlmICghcmVzdWx0KSB7XG4gICAgICBjb25zdCBtb2R1bGVSZXNvbHZlciA9IG5ldyBOZ01vZHVsZVJlc29sdmVyKHRoaXMucmVmbGVjdG9yKTtcbiAgICAgIGNvbnN0IGRpcmVjdGl2ZVJlc29sdmVyID0gbmV3IERpcmVjdGl2ZVJlc29sdmVyKHRoaXMucmVmbGVjdG9yKTtcbiAgICAgIGNvbnN0IHBpcGVSZXNvbHZlciA9IG5ldyBQaXBlUmVzb2x2ZXIodGhpcy5yZWZsZWN0b3IpO1xuICAgICAgY29uc3QgZWxlbWVudFNjaGVtYVJlZ2lzdHJ5ID0gbmV3IERvbUVsZW1lbnRTY2hlbWFSZWdpc3RyeSgpO1xuICAgICAgY29uc3QgcmVzb3VyY2VMb2FkZXIgPSBuZXcgRHVtbXlSZXNvdXJjZUxvYWRlcigpO1xuICAgICAgY29uc3QgdXJsUmVzb2x2ZXIgPSBjcmVhdGVPZmZsaW5lQ29tcGlsZVVybFJlc29sdmVyKCk7XG4gICAgICBjb25zdCBodG1sUGFyc2VyID0gbmV3IER1bW15SHRtbFBhcnNlcigpO1xuICAgICAgLy8gVGhpcyB0cmFja3MgdGhlIENvbXBpbGVDb25maWcgaW4gY29kZWdlbi50cy4gQ3VycmVudGx5IHRoZXNlIG9wdGlvbnNcbiAgICAgIC8vIGFyZSBoYXJkLWNvZGVkLlxuICAgICAgY29uc3QgY29uZmlnID1cbiAgICAgICAgICBuZXcgQ29tcGlsZXJDb25maWcoe2RlZmF1bHRFbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5FbXVsYXRlZCwgdXNlSml0OiBmYWxzZX0pO1xuICAgICAgY29uc3QgZGlyZWN0aXZlTm9ybWFsaXplciA9XG4gICAgICAgICAgbmV3IERpcmVjdGl2ZU5vcm1hbGl6ZXIocmVzb3VyY2VMb2FkZXIsIHVybFJlc29sdmVyLCBodG1sUGFyc2VyLCBjb25maWcpO1xuXG4gICAgICByZXN1bHQgPSB0aGlzLl9yZXNvbHZlciA9IG5ldyBDb21waWxlTWV0YWRhdGFSZXNvbHZlcihcbiAgICAgICAgICBjb25maWcsIGh0bWxQYXJzZXIsIG1vZHVsZVJlc29sdmVyLCBkaXJlY3RpdmVSZXNvbHZlciwgcGlwZVJlc29sdmVyLFxuICAgICAgICAgIG5ldyBKaXRTdW1tYXJ5UmVzb2x2ZXIoKSwgZWxlbWVudFNjaGVtYVJlZ2lzdHJ5LCBkaXJlY3RpdmVOb3JtYWxpemVyLCBuZXcgQ29uc29sZSgpLFxuICAgICAgICAgIHRoaXMuX3N0YXRpY1N5bWJvbENhY2hlLCB0aGlzLnJlZmxlY3RvcixcbiAgICAgICAgICAoZXJyb3IsIHR5cGUpID0+IHRoaXMuY29sbGVjdEVycm9yKGVycm9yLCB0eXBlICYmIHR5cGUuZmlsZVBhdGgpKTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIGdldFRlbXBsYXRlUmVmZXJlbmNlcygpOiBzdHJpbmdbXSB7XG4gICAgdGhpcy5lbnN1cmVUZW1wbGF0ZU1hcCgpO1xuICAgIHJldHVybiB0aGlzLnRlbXBsYXRlUmVmZXJlbmNlcyB8fCBbXTtcbiAgfVxuXG4gIGdldFRlbXBsYXRlQXQoZmlsZU5hbWU6IHN0cmluZywgcG9zaXRpb246IG51bWJlcik6IFRlbXBsYXRlU291cmNlfHVuZGVmaW5lZCB7XG4gICAgbGV0IHNvdXJjZUZpbGUgPSB0aGlzLmdldFNvdXJjZUZpbGUoZmlsZU5hbWUpO1xuICAgIGlmIChzb3VyY2VGaWxlKSB7XG4gICAgICB0aGlzLmNvbnRleHQgPSBzb3VyY2VGaWxlLmZpbGVOYW1lO1xuICAgICAgbGV0IG5vZGUgPSB0aGlzLmZpbmROb2RlKHNvdXJjZUZpbGUsIHBvc2l0aW9uKTtcbiAgICAgIGlmIChub2RlKSB7XG4gICAgICAgIHJldHVybiB0aGlzLmdldFNvdXJjZUZyb21Ob2RlKFxuICAgICAgICAgICAgZmlsZU5hbWUsIHRoaXMuaG9zdC5nZXRTY3JpcHRWZXJzaW9uKHNvdXJjZUZpbGUuZmlsZU5hbWUpLCBub2RlKTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5lbnN1cmVUZW1wbGF0ZU1hcCgpO1xuICAgICAgLy8gVE9ETzogQ2Fubm9jYWxpemUgdGhlIGZpbGU/XG4gICAgICBjb25zdCBjb21wb25lbnRUeXBlID0gdGhpcy5maWxlVG9Db21wb25lbnQuZ2V0KGZpbGVOYW1lKTtcbiAgICAgIGlmIChjb21wb25lbnRUeXBlKSB7XG4gICAgICAgIHJldHVybiB0aGlzLmdldFNvdXJjZUZyb21UeXBlKFxuICAgICAgICAgICAgZmlsZU5hbWUsIHRoaXMuaG9zdC5nZXRTY3JpcHRWZXJzaW9uKGZpbGVOYW1lKSwgY29tcG9uZW50VHlwZSk7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiB1bmRlZmluZWQ7XG4gIH1cblxuICBnZXRBbmFseXplZE1vZHVsZXMoKTogTmdBbmFseXplZE1vZHVsZXMge1xuICAgIHRoaXMudXBkYXRlQW5hbHl6ZWRNb2R1bGVzKCk7XG4gICAgcmV0dXJuIHRoaXMuZW5zdXJlQW5hbHl6ZWRNb2R1bGVzKCk7XG4gIH1cblxuICBwcml2YXRlIGVuc3VyZUFuYWx5emVkTW9kdWxlcygpOiBOZ0FuYWx5emVkTW9kdWxlcyB7XG4gICAgbGV0IGFuYWx5emVkTW9kdWxlcyA9IHRoaXMuYW5hbHl6ZWRNb2R1bGVzO1xuICAgIGlmICghYW5hbHl6ZWRNb2R1bGVzKSB7XG4gICAgICBpZiAodGhpcy5ob3N0LmdldFNjcmlwdEZpbGVOYW1lcygpLmxlbmd0aCA9PT0gMCkge1xuICAgICAgICBhbmFseXplZE1vZHVsZXMgPSB7XG4gICAgICAgICAgZmlsZXM6IFtdLFxuICAgICAgICAgIG5nTW9kdWxlQnlQaXBlT3JEaXJlY3RpdmU6IG5ldyBNYXAoKSxcbiAgICAgICAgICBuZ01vZHVsZXM6IFtdLFxuICAgICAgICB9O1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc3QgYW5hbHl6ZUhvc3QgPSB7aXNTb3VyY2VGaWxlKGZpbGVQYXRoOiBzdHJpbmcpIHsgcmV0dXJuIHRydWU7IH19O1xuICAgICAgICBjb25zdCBwcm9ncmFtRmlsZXMgPSB0aGlzLnByb2dyYW0gIS5nZXRTb3VyY2VGaWxlcygpLm1hcChzZiA9PiBzZi5maWxlTmFtZSk7XG4gICAgICAgIGFuYWx5emVkTW9kdWxlcyA9XG4gICAgICAgICAgICBhbmFseXplTmdNb2R1bGVzKHByb2dyYW1GaWxlcywgYW5hbHl6ZUhvc3QsIHRoaXMuc3RhdGljU3ltYm9sUmVzb2x2ZXIsIHRoaXMucmVzb2x2ZXIpO1xuICAgICAgfVxuICAgICAgdGhpcy5hbmFseXplZE1vZHVsZXMgPSBhbmFseXplZE1vZHVsZXM7XG4gICAgfVxuICAgIHJldHVybiBhbmFseXplZE1vZHVsZXM7XG4gIH1cblxuICBnZXRUZW1wbGF0ZXMoZmlsZU5hbWU6IHN0cmluZyk6IFRlbXBsYXRlU291cmNlcyB7XG4gICAgdGhpcy5lbnN1cmVUZW1wbGF0ZU1hcCgpO1xuICAgIGNvbnN0IGNvbXBvbmVudFR5cGUgPSB0aGlzLmZpbGVUb0NvbXBvbmVudC5nZXQoZmlsZU5hbWUpO1xuICAgIGlmIChjb21wb25lbnRUeXBlKSB7XG4gICAgICBjb25zdCB0ZW1wbGF0ZVNvdXJjZSA9IHRoaXMuZ2V0VGVtcGxhdGVBdChmaWxlTmFtZSwgMCk7XG4gICAgICBpZiAodGVtcGxhdGVTb3VyY2UpIHtcbiAgICAgICAgcmV0dXJuIFt0ZW1wbGF0ZVNvdXJjZV07XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIGxldCB2ZXJzaW9uID0gdGhpcy5ob3N0LmdldFNjcmlwdFZlcnNpb24oZmlsZU5hbWUpO1xuICAgICAgbGV0IHJlc3VsdDogVGVtcGxhdGVTb3VyY2VbXSA9IFtdO1xuXG4gICAgICAvLyBGaW5kIGVhY2ggdGVtcGxhdGUgc3RyaW5nIGluIHRoZSBmaWxlXG4gICAgICBsZXQgdmlzaXQgPSAoY2hpbGQ6IHRzLk5vZGUpID0+IHtcbiAgICAgICAgbGV0IHRlbXBsYXRlU291cmNlID0gdGhpcy5nZXRTb3VyY2VGcm9tTm9kZShmaWxlTmFtZSwgdmVyc2lvbiwgY2hpbGQpO1xuICAgICAgICBpZiAodGVtcGxhdGVTb3VyY2UpIHtcbiAgICAgICAgICByZXN1bHQucHVzaCh0ZW1wbGF0ZVNvdXJjZSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdHMuZm9yRWFjaENoaWxkKGNoaWxkLCB2aXNpdCk7XG4gICAgICAgIH1cbiAgICAgIH07XG5cbiAgICAgIGxldCBzb3VyY2VGaWxlID0gdGhpcy5nZXRTb3VyY2VGaWxlKGZpbGVOYW1lKTtcbiAgICAgIGlmIChzb3VyY2VGaWxlKSB7XG4gICAgICAgIHRoaXMuY29udGV4dCA9IChzb3VyY2VGaWxlIGFzIGFueSkucGF0aCB8fCBzb3VyY2VGaWxlLmZpbGVOYW1lO1xuICAgICAgICB0cy5mb3JFYWNoQ2hpbGQoc291cmNlRmlsZSwgdmlzaXQpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIHJlc3VsdC5sZW5ndGggPyByZXN1bHQgOiB1bmRlZmluZWQ7XG4gICAgfVxuICB9XG5cbiAgZ2V0RGVjbGFyYXRpb25zKGZpbGVOYW1lOiBzdHJpbmcpOiBEZWNsYXJhdGlvbnMge1xuICAgIGNvbnN0IHJlc3VsdDogRGVjbGFyYXRpb25zID0gW107XG4gICAgY29uc3Qgc291cmNlRmlsZSA9IHRoaXMuZ2V0U291cmNlRmlsZShmaWxlTmFtZSk7XG4gICAgaWYgKHNvdXJjZUZpbGUpIHtcbiAgICAgIGxldCB2aXNpdCA9IChjaGlsZDogdHMuTm9kZSkgPT4ge1xuICAgICAgICBsZXQgZGVjbGFyYXRpb24gPSB0aGlzLmdldERlY2xhcmF0aW9uRnJvbU5vZGUoc291cmNlRmlsZSwgY2hpbGQpO1xuICAgICAgICBpZiAoZGVjbGFyYXRpb24pIHtcbiAgICAgICAgICByZXN1bHQucHVzaChkZWNsYXJhdGlvbik7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdHMuZm9yRWFjaENoaWxkKGNoaWxkLCB2aXNpdCk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICB0cy5mb3JFYWNoQ2hpbGQoc291cmNlRmlsZSwgdmlzaXQpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgZ2V0U291cmNlRmlsZShmaWxlTmFtZTogc3RyaW5nKTogdHMuU291cmNlRmlsZXx1bmRlZmluZWQge1xuICAgIHJldHVybiB0aGlzLnRzU2VydmljZS5nZXRQcm9ncmFtKCkgIS5nZXRTb3VyY2VGaWxlKGZpbGVOYW1lKTtcbiAgfVxuXG4gIHVwZGF0ZUFuYWx5emVkTW9kdWxlcygpIHtcbiAgICB0aGlzLnZhbGlkYXRlKCk7XG4gICAgaWYgKHRoaXMubW9kdWxlc091dE9mRGF0ZSkge1xuICAgICAgdGhpcy5hbmFseXplZE1vZHVsZXMgPSBudWxsO1xuICAgICAgdGhpcy5fcmVmbGVjdG9yID0gbnVsbDtcbiAgICAgIHRoaXMudGVtcGxhdGVSZWZlcmVuY2VzID0gbnVsbDtcbiAgICAgIHRoaXMuZmlsZVRvQ29tcG9uZW50LmNsZWFyKCk7XG4gICAgICB0aGlzLmVuc3VyZUFuYWx5emVkTW9kdWxlcygpO1xuICAgICAgdGhpcy5tb2R1bGVzT3V0T2ZEYXRlID0gZmFsc2U7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBnZXQgcHJvZ3JhbSgpIHsgcmV0dXJuIHRoaXMudHNTZXJ2aWNlLmdldFByb2dyYW0oKTsgfVxuXG4gIHByaXZhdGUgZ2V0IGNoZWNrZXIoKSB7XG4gICAgbGV0IGNoZWNrZXIgPSB0aGlzLl9jaGVja2VyO1xuICAgIGlmICghY2hlY2tlcikge1xuICAgICAgY2hlY2tlciA9IHRoaXMuX2NoZWNrZXIgPSB0aGlzLnByb2dyYW0gIS5nZXRUeXBlQ2hlY2tlcigpO1xuICAgIH1cbiAgICByZXR1cm4gY2hlY2tlcjtcbiAgfVxuXG4gIHByaXZhdGUgdmFsaWRhdGUoKSB7XG4gICAgY29uc3QgcHJvZ3JhbSA9IHRoaXMucHJvZ3JhbTtcbiAgICBpZiAodGhpcy5sYXN0UHJvZ3JhbSAhPT0gcHJvZ3JhbSkge1xuICAgICAgLy8gSW52YWxpZGF0ZSBmaWxlIHRoYXQgaGF2ZSBjaGFuZ2VkIGluIHRoZSBzdGF0aWMgc3ltYm9sIHJlc29sdmVyXG4gICAgICBjb25zdCBpbnZhbGlkYXRlRmlsZSA9IChmaWxlTmFtZTogc3RyaW5nKSA9PlxuICAgICAgICAgIHRoaXMuX3N0YXRpY1N5bWJvbFJlc29sdmVyLmludmFsaWRhdGVGaWxlKGZpbGVOYW1lKTtcbiAgICAgIHRoaXMuY2xlYXJDYWNoZXMoKTtcbiAgICAgIGNvbnN0IHNlZW4gPSBuZXcgU2V0PHN0cmluZz4oKTtcbiAgICAgIGZvciAobGV0IHNvdXJjZUZpbGUgb2YgdGhpcy5wcm9ncmFtICEuZ2V0U291cmNlRmlsZXMoKSkge1xuICAgICAgICBjb25zdCBmaWxlTmFtZSA9IHNvdXJjZUZpbGUuZmlsZU5hbWU7XG4gICAgICAgIHNlZW4uYWRkKGZpbGVOYW1lKTtcbiAgICAgICAgY29uc3QgdmVyc2lvbiA9IHRoaXMuaG9zdC5nZXRTY3JpcHRWZXJzaW9uKGZpbGVOYW1lKTtcbiAgICAgICAgY29uc3QgbGFzdFZlcnNpb24gPSB0aGlzLmZpbGVWZXJzaW9ucy5nZXQoZmlsZU5hbWUpO1xuICAgICAgICBpZiAodmVyc2lvbiAhPSBsYXN0VmVyc2lvbikge1xuICAgICAgICAgIHRoaXMuZmlsZVZlcnNpb25zLnNldChmaWxlTmFtZSwgdmVyc2lvbik7XG4gICAgICAgICAgaWYgKHRoaXMuX3N0YXRpY1N5bWJvbFJlc29sdmVyKSB7XG4gICAgICAgICAgICBpbnZhbGlkYXRlRmlsZShmaWxlTmFtZSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIC8vIFJlbW92ZSBmaWxlIHZlcnNpb25zIHRoYXQgYXJlIG5vIGxvbmdlciBpbiB0aGUgZmlsZSBhbmQgaW52YWxpZGF0ZSB0aGVtLlxuICAgICAgY29uc3QgbWlzc2luZyA9IEFycmF5LmZyb20odGhpcy5maWxlVmVyc2lvbnMua2V5cygpKS5maWx0ZXIoZiA9PiAhc2Vlbi5oYXMoZikpO1xuICAgICAgbWlzc2luZy5mb3JFYWNoKGYgPT4gdGhpcy5maWxlVmVyc2lvbnMuZGVsZXRlKGYpKTtcbiAgICAgIGlmICh0aGlzLl9zdGF0aWNTeW1ib2xSZXNvbHZlcikge1xuICAgICAgICBtaXNzaW5nLmZvckVhY2goaW52YWxpZGF0ZUZpbGUpO1xuICAgICAgfVxuXG4gICAgICB0aGlzLmxhc3RQcm9ncmFtID0gcHJvZ3JhbTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGNsZWFyQ2FjaGVzKCkge1xuICAgIHRoaXMuX2NoZWNrZXIgPSBudWxsO1xuICAgIHRoaXMuX3Jlc29sdmVyID0gbnVsbDtcbiAgICB0aGlzLmNvbGxlY3RlZEVycm9ycy5jbGVhcigpO1xuICAgIHRoaXMubW9kdWxlc091dE9mRGF0ZSA9IHRydWU7XG4gIH1cblxuICBwcml2YXRlIGVuc3VyZVRlbXBsYXRlTWFwKCkge1xuICAgIGlmICghdGhpcy50ZW1wbGF0ZVJlZmVyZW5jZXMpIHtcbiAgICAgIGNvbnN0IHRlbXBsYXRlUmVmZXJlbmNlOiBzdHJpbmdbXSA9IFtdO1xuICAgICAgY29uc3QgbmdNb2R1bGVTdW1tYXJ5ID0gdGhpcy5nZXRBbmFseXplZE1vZHVsZXMoKTtcbiAgICAgIGNvbnN0IHVybFJlc29sdmVyID0gY3JlYXRlT2ZmbGluZUNvbXBpbGVVcmxSZXNvbHZlcigpO1xuICAgICAgZm9yIChjb25zdCBtb2R1bGUgb2YgbmdNb2R1bGVTdW1tYXJ5Lm5nTW9kdWxlcykge1xuICAgICAgICBmb3IgKGNvbnN0IGRpcmVjdGl2ZSBvZiBtb2R1bGUuZGVjbGFyZWREaXJlY3RpdmVzKSB7XG4gICAgICAgICAgY29uc3Qge21ldGFkYXRhfSA9IHRoaXMucmVzb2x2ZXIuZ2V0Tm9uTm9ybWFsaXplZERpcmVjdGl2ZU1ldGFkYXRhKGRpcmVjdGl2ZS5yZWZlcmVuY2UpICE7XG4gICAgICAgICAgaWYgKG1ldGFkYXRhLmlzQ29tcG9uZW50ICYmIG1ldGFkYXRhLnRlbXBsYXRlICYmIG1ldGFkYXRhLnRlbXBsYXRlLnRlbXBsYXRlVXJsKSB7XG4gICAgICAgICAgICBjb25zdCB0ZW1wbGF0ZU5hbWUgPSB1cmxSZXNvbHZlci5yZXNvbHZlKFxuICAgICAgICAgICAgICAgIHRoaXMucmVmbGVjdG9yLmNvbXBvbmVudE1vZHVsZVVybChkaXJlY3RpdmUucmVmZXJlbmNlKSxcbiAgICAgICAgICAgICAgICBtZXRhZGF0YS50ZW1wbGF0ZS50ZW1wbGF0ZVVybCk7XG4gICAgICAgICAgICB0aGlzLmZpbGVUb0NvbXBvbmVudC5zZXQodGVtcGxhdGVOYW1lLCBkaXJlY3RpdmUucmVmZXJlbmNlKTtcbiAgICAgICAgICAgIHRlbXBsYXRlUmVmZXJlbmNlLnB1c2godGVtcGxhdGVOYW1lKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHRoaXMudGVtcGxhdGVSZWZlcmVuY2VzID0gdGVtcGxhdGVSZWZlcmVuY2U7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBnZXRTb3VyY2VGcm9tRGVjbGFyYXRpb24oXG4gICAgICBmaWxlTmFtZTogc3RyaW5nLCB2ZXJzaW9uOiBzdHJpbmcsIHNvdXJjZTogc3RyaW5nLCBzcGFuOiBTcGFuLCB0eXBlOiBTdGF0aWNTeW1ib2wsXG4gICAgICBkZWNsYXJhdGlvbjogdHMuQ2xhc3NEZWNsYXJhdGlvbiwgbm9kZTogdHMuTm9kZSwgc291cmNlRmlsZTogdHMuU291cmNlRmlsZSk6IFRlbXBsYXRlU291cmNlXG4gICAgICB8dW5kZWZpbmVkIHtcbiAgICBsZXQgcXVlcnlDYWNoZTogU3ltYm9sUXVlcnl8dW5kZWZpbmVkID0gdW5kZWZpbmVkO1xuICAgIGNvbnN0IHQgPSB0aGlzO1xuICAgIGlmIChkZWNsYXJhdGlvbikge1xuICAgICAgcmV0dXJuIHtcbiAgICAgICAgdmVyc2lvbixcbiAgICAgICAgc291cmNlLFxuICAgICAgICBzcGFuLFxuICAgICAgICB0eXBlLFxuICAgICAgICBnZXQgbWVtYmVycygpIHtcbiAgICAgICAgICByZXR1cm4gZ2V0Q2xhc3NNZW1iZXJzRnJvbURlY2xhcmF0aW9uKHQucHJvZ3JhbSAhLCB0LmNoZWNrZXIsIHNvdXJjZUZpbGUsIGRlY2xhcmF0aW9uKTtcbiAgICAgICAgfSxcbiAgICAgICAgZ2V0IHF1ZXJ5KCkge1xuICAgICAgICAgIGlmICghcXVlcnlDYWNoZSkge1xuICAgICAgICAgICAgbGV0IHBpcGVzOiBDb21waWxlUGlwZVN1bW1hcnlbXSA9IFtdO1xuICAgICAgICAgICAgY29uc3QgdGVtcGxhdGVJbmZvID0gdC5nZXRUZW1wbGF0ZUFzdEF0UG9zaXRpb24oZmlsZU5hbWUsIG5vZGUuZ2V0U3RhcnQoKSk7XG4gICAgICAgICAgICBpZiAodGVtcGxhdGVJbmZvKSB7XG4gICAgICAgICAgICAgIHBpcGVzID0gdGVtcGxhdGVJbmZvLnBpcGVzO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgcXVlcnlDYWNoZSA9IGdldFN5bWJvbFF1ZXJ5KFxuICAgICAgICAgICAgICAgIHQucHJvZ3JhbSAhLCB0LmNoZWNrZXIsIHNvdXJjZUZpbGUsXG4gICAgICAgICAgICAgICAgKCkgPT4gZ2V0UGlwZXNUYWJsZShzb3VyY2VGaWxlLCB0LnByb2dyYW0gISwgdC5jaGVja2VyLCBwaXBlcykpO1xuICAgICAgICAgIH1cbiAgICAgICAgICByZXR1cm4gcXVlcnlDYWNoZTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGdldFNvdXJjZUZyb21Ob2RlKGZpbGVOYW1lOiBzdHJpbmcsIHZlcnNpb246IHN0cmluZywgbm9kZTogdHMuTm9kZSk6IFRlbXBsYXRlU291cmNlXG4gICAgICB8dW5kZWZpbmVkIHtcbiAgICBsZXQgcmVzdWx0OiBUZW1wbGF0ZVNvdXJjZXx1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG4gICAgY29uc3QgdCA9IHRoaXM7XG4gICAgc3dpdGNoIChub2RlLmtpbmQpIHtcbiAgICAgIGNhc2UgdHMuU3ludGF4S2luZC5Ob1N1YnN0aXR1dGlvblRlbXBsYXRlTGl0ZXJhbDpcbiAgICAgIGNhc2UgdHMuU3ludGF4S2luZC5TdHJpbmdMaXRlcmFsOlxuICAgICAgICBsZXQgW2RlY2xhcmF0aW9uLCBkZWNvcmF0b3JdID0gdGhpcy5nZXRUZW1wbGF0ZUNsYXNzRGVjbEZyb21Ob2RlKG5vZGUpO1xuICAgICAgICBpZiAoZGVjbGFyYXRpb24gJiYgZGVjbGFyYXRpb24ubmFtZSkge1xuICAgICAgICAgIGNvbnN0IHNvdXJjZUZpbGUgPSB0aGlzLmdldFNvdXJjZUZpbGUoZmlsZU5hbWUpO1xuICAgICAgICAgIGlmIChzb3VyY2VGaWxlKSB7XG4gICAgICAgICAgICByZXR1cm4gdGhpcy5nZXRTb3VyY2VGcm9tRGVjbGFyYXRpb24oXG4gICAgICAgICAgICAgICAgZmlsZU5hbWUsIHZlcnNpb24sIHRoaXMuc3RyaW5nT2Yobm9kZSkgfHwgJycsIHNocmluayhzcGFuT2Yobm9kZSkpLFxuICAgICAgICAgICAgICAgIHRoaXMucmVmbGVjdG9yLmdldFN0YXRpY1N5bWJvbChzb3VyY2VGaWxlLmZpbGVOYW1lLCBkZWNsYXJhdGlvbi5uYW1lLnRleHQpLFxuICAgICAgICAgICAgICAgIGRlY2xhcmF0aW9uLCBub2RlLCBzb3VyY2VGaWxlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICBwcml2YXRlIGdldFNvdXJjZUZyb21UeXBlKGZpbGVOYW1lOiBzdHJpbmcsIHZlcnNpb246IHN0cmluZywgdHlwZTogU3RhdGljU3ltYm9sKTogVGVtcGxhdGVTb3VyY2VcbiAgICAgIHx1bmRlZmluZWQge1xuICAgIGxldCByZXN1bHQ6IFRlbXBsYXRlU291cmNlfHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgICBjb25zdCBkZWNsYXJhdGlvbiA9IHRoaXMuZ2V0VGVtcGxhdGVDbGFzc0Zyb21TdGF0aWNTeW1ib2wodHlwZSk7XG4gICAgaWYgKGRlY2xhcmF0aW9uKSB7XG4gICAgICBjb25zdCBzbmFwc2hvdCA9IHRoaXMuaG9zdC5nZXRTY3JpcHRTbmFwc2hvdChmaWxlTmFtZSk7XG4gICAgICBpZiAoc25hcHNob3QpIHtcbiAgICAgICAgY29uc3Qgc291cmNlID0gc25hcHNob3QuZ2V0VGV4dCgwLCBzbmFwc2hvdC5nZXRMZW5ndGgoKSk7XG4gICAgICAgIHJlc3VsdCA9IHRoaXMuZ2V0U291cmNlRnJvbURlY2xhcmF0aW9uKFxuICAgICAgICAgICAgZmlsZU5hbWUsIHZlcnNpb24sIHNvdXJjZSwge3N0YXJ0OiAwLCBlbmQ6IHNvdXJjZS5sZW5ndGh9LCB0eXBlLCBkZWNsYXJhdGlvbixcbiAgICAgICAgICAgIGRlY2xhcmF0aW9uLCBkZWNsYXJhdGlvbi5nZXRTb3VyY2VGaWxlKCkpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBnZXQgcmVmbGVjdG9ySG9zdCgpOiBSZWZsZWN0b3JIb3N0IHtcbiAgICBsZXQgcmVzdWx0ID0gdGhpcy5fcmVmbGVjdG9ySG9zdDtcbiAgICBpZiAoIXJlc3VsdCkge1xuICAgICAgaWYgKCF0aGlzLmNvbnRleHQpIHtcbiAgICAgICAgLy8gTWFrZSB1cCBhIGNvbnRleHQgYnkgZmluZGluZyB0aGUgZmlyc3Qgc2NyaXB0IGFuZCB1c2luZyB0aGF0IGFzIHRoZSBiYXNlIGRpci5cbiAgICAgICAgY29uc3Qgc2NyaXB0RmlsZU5hbWVzID0gdGhpcy5ob3N0LmdldFNjcmlwdEZpbGVOYW1lcygpO1xuICAgICAgICBpZiAoMCA9PT0gc2NyaXB0RmlsZU5hbWVzLmxlbmd0aCkge1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcignSW50ZXJuYWwgZXJyb3I6IG5vIHNjcmlwdCBmaWxlIG5hbWVzIGZvdW5kJyk7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy5jb250ZXh0ID0gc2NyaXB0RmlsZU5hbWVzWzBdO1xuICAgICAgfVxuXG4gICAgICAvLyBVc2UgdGhlIGZpbGUgY29udGV4dCdzIGRpcmVjdG9yeSBhcyB0aGUgYmFzZSBkaXJlY3RvcnkuXG4gICAgICAvLyBUaGUgaG9zdCdzIGdldEN1cnJlbnREaXJlY3RvcnkoKSBpcyBub3QgcmVsaWFibGUgYXMgaXQgaXMgYWx3YXlzIFwiXCIgaW5cbiAgICAgIC8vIHRzc2VydmVyLiBXZSBkb24ndCBuZWVkIHRoZSBleGFjdCBiYXNlIGRpcmVjdG9yeSwganVzdCBvbmUgdGhhdCBjb250YWluc1xuICAgICAgLy8gYSBzb3VyY2UgZmlsZS5cbiAgICAgIGNvbnN0IHNvdXJjZSA9IHRoaXMudHNTZXJ2aWNlLmdldFByb2dyYW0oKSAhLmdldFNvdXJjZUZpbGUodGhpcy5jb250ZXh0KTtcbiAgICAgIGlmICghc291cmNlKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcignSW50ZXJuYWwgZXJyb3I6IG5vIGNvbnRleHQgY291bGQgYmUgZGV0ZXJtaW5lZCcpO1xuICAgICAgfVxuXG4gICAgICBjb25zdCB0c0NvbmZpZ1BhdGggPSBmaW5kVHNDb25maWcoc291cmNlLmZpbGVOYW1lKTtcbiAgICAgIGNvbnN0IGJhc2VQYXRoID0gcGF0aC5kaXJuYW1lKHRzQ29uZmlnUGF0aCB8fCB0aGlzLmNvbnRleHQpO1xuICAgICAgY29uc3Qgb3B0aW9uczogQ29tcGlsZXJPcHRpb25zID0ge2Jhc2VQYXRoLCBnZW5EaXI6IGJhc2VQYXRofTtcbiAgICAgIGNvbnN0IGNvbXBpbGVyT3B0aW9ucyA9IHRoaXMuaG9zdC5nZXRDb21waWxhdGlvblNldHRpbmdzKCk7XG4gICAgICBpZiAoY29tcGlsZXJPcHRpb25zICYmIGNvbXBpbGVyT3B0aW9ucy5iYXNlVXJsKSB7XG4gICAgICAgIG9wdGlvbnMuYmFzZVVybCA9IGNvbXBpbGVyT3B0aW9ucy5iYXNlVXJsO1xuICAgICAgfVxuICAgICAgaWYgKGNvbXBpbGVyT3B0aW9ucyAmJiBjb21waWxlck9wdGlvbnMucGF0aHMpIHtcbiAgICAgICAgb3B0aW9ucy5wYXRocyA9IGNvbXBpbGVyT3B0aW9ucy5wYXRocztcbiAgICAgIH1cbiAgICAgIHJlc3VsdCA9IHRoaXMuX3JlZmxlY3Rvckhvc3QgPVxuICAgICAgICAgIG5ldyBSZWZsZWN0b3JIb3N0KCgpID0+IHRoaXMudHNTZXJ2aWNlLmdldFByb2dyYW0oKSAhLCB0aGlzLmhvc3QsIG9wdGlvbnMpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBjb2xsZWN0RXJyb3IoZXJyb3I6IGFueSwgZmlsZVBhdGg6IHN0cmluZ3xudWxsKSB7XG4gICAgaWYgKGZpbGVQYXRoKSB7XG4gICAgICBsZXQgZXJyb3JzID0gdGhpcy5jb2xsZWN0ZWRFcnJvcnMuZ2V0KGZpbGVQYXRoKTtcbiAgICAgIGlmICghZXJyb3JzKSB7XG4gICAgICAgIGVycm9ycyA9IFtdO1xuICAgICAgICB0aGlzLmNvbGxlY3RlZEVycm9ycy5zZXQoZmlsZVBhdGgsIGVycm9ycyk7XG4gICAgICB9XG4gICAgICBlcnJvcnMucHVzaChlcnJvcik7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBnZXQgc3RhdGljU3ltYm9sUmVzb2x2ZXIoKTogU3RhdGljU3ltYm9sUmVzb2x2ZXIge1xuICAgIGxldCByZXN1bHQgPSB0aGlzLl9zdGF0aWNTeW1ib2xSZXNvbHZlcjtcbiAgICBpZiAoIXJlc3VsdCkge1xuICAgICAgdGhpcy5fc3VtbWFyeVJlc29sdmVyID0gbmV3IEFvdFN1bW1hcnlSZXNvbHZlcihcbiAgICAgICAgICB7XG4gICAgICAgICAgICBsb2FkU3VtbWFyeShmaWxlUGF0aDogc3RyaW5nKSB7IHJldHVybiBudWxsOyB9LFxuICAgICAgICAgICAgaXNTb3VyY2VGaWxlKHNvdXJjZUZpbGVQYXRoOiBzdHJpbmcpIHsgcmV0dXJuIHRydWU7IH0sXG4gICAgICAgICAgICB0b1N1bW1hcnlGaWxlTmFtZShzb3VyY2VGaWxlUGF0aDogc3RyaW5nKSB7IHJldHVybiBzb3VyY2VGaWxlUGF0aDsgfSxcbiAgICAgICAgICAgIGZyb21TdW1tYXJ5RmlsZU5hbWUoZmlsZVBhdGg6IHN0cmluZyk6IHN0cmluZ3tyZXR1cm4gZmlsZVBhdGg7fSxcbiAgICAgICAgICB9LFxuICAgICAgICAgIHRoaXMuX3N0YXRpY1N5bWJvbENhY2hlKTtcbiAgICAgIHJlc3VsdCA9IHRoaXMuX3N0YXRpY1N5bWJvbFJlc29sdmVyID0gbmV3IFN0YXRpY1N5bWJvbFJlc29sdmVyKFxuICAgICAgICAgIHRoaXMucmVmbGVjdG9ySG9zdCBhcyBhbnksIHRoaXMuX3N0YXRpY1N5bWJvbENhY2hlLCB0aGlzLl9zdW1tYXJ5UmVzb2x2ZXIsXG4gICAgICAgICAgKGUsIGZpbGVQYXRoKSA9PiB0aGlzLmNvbGxlY3RFcnJvcihlLCBmaWxlUGF0aCAhKSk7XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICBwcml2YXRlIGdldCByZWZsZWN0b3IoKTogU3RhdGljUmVmbGVjdG9yIHtcbiAgICBsZXQgcmVzdWx0ID0gdGhpcy5fcmVmbGVjdG9yO1xuICAgIGlmICghcmVzdWx0KSB7XG4gICAgICBjb25zdCBzc3IgPSB0aGlzLnN0YXRpY1N5bWJvbFJlc29sdmVyO1xuICAgICAgcmVzdWx0ID0gdGhpcy5fcmVmbGVjdG9yID0gbmV3IFN0YXRpY1JlZmxlY3RvcihcbiAgICAgICAgICB0aGlzLl9zdW1tYXJ5UmVzb2x2ZXIsIHNzciwgW10sIFtdLCAoZSwgZmlsZVBhdGgpID0+IHRoaXMuY29sbGVjdEVycm9yKGUsIGZpbGVQYXRoICEpKTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIHByaXZhdGUgZ2V0VGVtcGxhdGVDbGFzc0Zyb21TdGF0aWNTeW1ib2wodHlwZTogU3RhdGljU3ltYm9sKTogdHMuQ2xhc3NEZWNsYXJhdGlvbnx1bmRlZmluZWQge1xuICAgIGNvbnN0IHNvdXJjZSA9IHRoaXMuZ2V0U291cmNlRmlsZSh0eXBlLmZpbGVQYXRoKTtcbiAgICBpZiAoc291cmNlKSB7XG4gICAgICBjb25zdCBkZWNsYXJhdGlvbk5vZGUgPSB0cy5mb3JFYWNoQ2hpbGQoc291cmNlLCBjaGlsZCA9PiB7XG4gICAgICAgIGlmIChjaGlsZC5raW5kID09PSB0cy5TeW50YXhLaW5kLkNsYXNzRGVjbGFyYXRpb24pIHtcbiAgICAgICAgICBjb25zdCBjbGFzc0RlY2xhcmF0aW9uID0gY2hpbGQgYXMgdHMuQ2xhc3NEZWNsYXJhdGlvbjtcbiAgICAgICAgICBpZiAoY2xhc3NEZWNsYXJhdGlvbi5uYW1lICE9IG51bGwgJiYgY2xhc3NEZWNsYXJhdGlvbi5uYW1lLnRleHQgPT09IHR5cGUubmFtZSkge1xuICAgICAgICAgICAgcmV0dXJuIGNsYXNzRGVjbGFyYXRpb247XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9KTtcbiAgICAgIHJldHVybiBkZWNsYXJhdGlvbk5vZGUgYXMgdHMuQ2xhc3NEZWNsYXJhdGlvbjtcbiAgICB9XG5cbiAgICByZXR1cm4gdW5kZWZpbmVkO1xuICB9XG5cbiAgcHJpdmF0ZSBzdGF0aWMgbWlzc2luZ1RlbXBsYXRlOiBbdHMuQ2xhc3NEZWNsYXJhdGlvbiB8IHVuZGVmaW5lZCwgdHMuRXhwcmVzc2lvbnx1bmRlZmluZWRdID1cbiAgICAgIFt1bmRlZmluZWQsIHVuZGVmaW5lZF07XG5cbiAgLyoqXG4gICAqIEdpdmVuIGEgdGVtcGxhdGUgc3RyaW5nIG5vZGUsIHNlZSBpZiBpdCBpcyBhbiBBbmd1bGFyIHRlbXBsYXRlIHN0cmluZywgYW5kIGlmIHNvIHJldHVybiB0aGVcbiAgICogY29udGFpbmluZyBjbGFzcy5cbiAgICovXG4gIHByaXZhdGUgZ2V0VGVtcGxhdGVDbGFzc0RlY2xGcm9tTm9kZShjdXJyZW50VG9rZW46IHRzLk5vZGUpOlxuICAgICAgW3RzLkNsYXNzRGVjbGFyYXRpb24gfCB1bmRlZmluZWQsIHRzLkV4cHJlc3Npb258dW5kZWZpbmVkXSB7XG4gICAgLy8gVmVyaWZ5IHdlIGFyZSBpbiBhICd0ZW1wbGF0ZScgcHJvcGVydHkgYXNzaWdubWVudCwgaW4gYW4gb2JqZWN0IGxpdGVyYWwsIHdoaWNoIGlzIGFuIGNhbGxcbiAgICAvLyBhcmcsIGluIGEgZGVjb3JhdG9yXG4gICAgbGV0IHBhcmVudE5vZGUgPSBjdXJyZW50VG9rZW4ucGFyZW50OyAgLy8gUHJvcGVydHlBc3NpZ25tZW50XG4gICAgaWYgKCFwYXJlbnROb2RlKSB7XG4gICAgICByZXR1cm4gVHlwZVNjcmlwdFNlcnZpY2VIb3N0Lm1pc3NpbmdUZW1wbGF0ZTtcbiAgICB9XG4gICAgaWYgKHBhcmVudE5vZGUua2luZCAhPT0gdHMuU3ludGF4S2luZC5Qcm9wZXJ0eUFzc2lnbm1lbnQpIHtcbiAgICAgIHJldHVybiBUeXBlU2NyaXB0U2VydmljZUhvc3QubWlzc2luZ1RlbXBsYXRlO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBUT0RPOiBJcyB0aGlzIGRpZmZlcmVudCBmb3IgYSBsaXRlcmFsLCBpLmUuIGEgcXVvdGVkIHByb3BlcnR5IG5hbWUgbGlrZSBcInRlbXBsYXRlXCI/XG4gICAgICBpZiAoKHBhcmVudE5vZGUgYXMgYW55KS5uYW1lLnRleHQgIT09ICd0ZW1wbGF0ZScpIHtcbiAgICAgICAgcmV0dXJuIFR5cGVTY3JpcHRTZXJ2aWNlSG9zdC5taXNzaW5nVGVtcGxhdGU7XG4gICAgICB9XG4gICAgfVxuICAgIHBhcmVudE5vZGUgPSBwYXJlbnROb2RlLnBhcmVudDsgIC8vIE9iamVjdExpdGVyYWxFeHByZXNzaW9uXG4gICAgaWYgKCFwYXJlbnROb2RlIHx8IHBhcmVudE5vZGUua2luZCAhPT0gdHMuU3ludGF4S2luZC5PYmplY3RMaXRlcmFsRXhwcmVzc2lvbikge1xuICAgICAgcmV0dXJuIFR5cGVTY3JpcHRTZXJ2aWNlSG9zdC5taXNzaW5nVGVtcGxhdGU7XG4gICAgfVxuXG4gICAgcGFyZW50Tm9kZSA9IHBhcmVudE5vZGUucGFyZW50OyAgLy8gQ2FsbEV4cHJlc3Npb25cbiAgICBpZiAoIXBhcmVudE5vZGUgfHwgcGFyZW50Tm9kZS5raW5kICE9PSB0cy5TeW50YXhLaW5kLkNhbGxFeHByZXNzaW9uKSB7XG4gICAgICByZXR1cm4gVHlwZVNjcmlwdFNlcnZpY2VIb3N0Lm1pc3NpbmdUZW1wbGF0ZTtcbiAgICB9XG4gICAgY29uc3QgY2FsbFRhcmdldCA9ICg8dHMuQ2FsbEV4cHJlc3Npb24+cGFyZW50Tm9kZSkuZXhwcmVzc2lvbjtcblxuICAgIGxldCBkZWNvcmF0b3IgPSBwYXJlbnROb2RlLnBhcmVudDsgIC8vIERlY29yYXRvclxuICAgIGlmICghZGVjb3JhdG9yIHx8IGRlY29yYXRvci5raW5kICE9PSB0cy5TeW50YXhLaW5kLkRlY29yYXRvcikge1xuICAgICAgcmV0dXJuIFR5cGVTY3JpcHRTZXJ2aWNlSG9zdC5taXNzaW5nVGVtcGxhdGU7XG4gICAgfVxuXG4gICAgbGV0IGRlY2xhcmF0aW9uID0gPHRzLkNsYXNzRGVjbGFyYXRpb24+ZGVjb3JhdG9yLnBhcmVudDsgIC8vIENsYXNzRGVjbGFyYXRpb25cbiAgICBpZiAoIWRlY2xhcmF0aW9uIHx8IGRlY2xhcmF0aW9uLmtpbmQgIT09IHRzLlN5bnRheEtpbmQuQ2xhc3NEZWNsYXJhdGlvbikge1xuICAgICAgcmV0dXJuIFR5cGVTY3JpcHRTZXJ2aWNlSG9zdC5taXNzaW5nVGVtcGxhdGU7XG4gICAgfVxuICAgIHJldHVybiBbZGVjbGFyYXRpb24sIGNhbGxUYXJnZXRdO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRDb2xsZWN0ZWRFcnJvcnMoZGVmYXVsdFNwYW46IFNwYW4sIHNvdXJjZUZpbGU6IHRzLlNvdXJjZUZpbGUpOiBEZWNsYXJhdGlvbkVycm9yW10ge1xuICAgIGNvbnN0IGVycm9ycyA9IHRoaXMuY29sbGVjdGVkRXJyb3JzLmdldChzb3VyY2VGaWxlLmZpbGVOYW1lKTtcbiAgICByZXR1cm4gKGVycm9ycyAmJiBlcnJvcnMubWFwKChlOiBhbnkpID0+IHtcbiAgICAgICAgICAgICBjb25zdCBsaW5lID0gZS5saW5lIHx8IChlLnBvc2l0aW9uICYmIGUucG9zaXRpb24ubGluZSk7XG4gICAgICAgICAgICAgY29uc3QgY29sdW1uID0gZS5jb2x1bW4gfHwgKGUucG9zaXRpb24gJiYgZS5wb3NpdGlvbi5jb2x1bW4pO1xuICAgICAgICAgICAgIGNvbnN0IHNwYW4gPSBzcGFuQXQoc291cmNlRmlsZSwgbGluZSwgY29sdW1uKSB8fCBkZWZhdWx0U3BhbjtcbiAgICAgICAgICAgICBpZiAoaXNGb3JtYXR0ZWRFcnJvcihlKSkge1xuICAgICAgICAgICAgICAgcmV0dXJuIGVycm9yVG9EaWFnbm9zdGljV2l0aENoYWluKGUsIHNwYW4pO1xuICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICByZXR1cm4ge21lc3NhZ2U6IGUubWVzc2FnZSwgc3Bhbn07XG4gICAgICAgICAgIH0pKSB8fFxuICAgICAgICBbXTtcbiAgfVxuXG4gIHByaXZhdGUgZ2V0RGVjbGFyYXRpb25Gcm9tTm9kZShzb3VyY2VGaWxlOiB0cy5Tb3VyY2VGaWxlLCBub2RlOiB0cy5Ob2RlKTogRGVjbGFyYXRpb258dW5kZWZpbmVkIHtcbiAgICBpZiAobm9kZS5raW5kID09IHRzLlN5bnRheEtpbmQuQ2xhc3NEZWNsYXJhdGlvbiAmJiBub2RlLmRlY29yYXRvcnMgJiZcbiAgICAgICAgKG5vZGUgYXMgdHMuQ2xhc3NEZWNsYXJhdGlvbikubmFtZSkge1xuICAgICAgZm9yIChjb25zdCBkZWNvcmF0b3Igb2Ygbm9kZS5kZWNvcmF0b3JzKSB7XG4gICAgICAgIGlmIChkZWNvcmF0b3IuZXhwcmVzc2lvbiAmJiBkZWNvcmF0b3IuZXhwcmVzc2lvbi5raW5kID09IHRzLlN5bnRheEtpbmQuQ2FsbEV4cHJlc3Npb24pIHtcbiAgICAgICAgICBjb25zdCBjbGFzc0RlY2xhcmF0aW9uID0gbm9kZSBhcyB0cy5DbGFzc0RlY2xhcmF0aW9uO1xuICAgICAgICAgIGlmIChjbGFzc0RlY2xhcmF0aW9uLm5hbWUpIHtcbiAgICAgICAgICAgIGNvbnN0IGNhbGwgPSBkZWNvcmF0b3IuZXhwcmVzc2lvbiBhcyB0cy5DYWxsRXhwcmVzc2lvbjtcbiAgICAgICAgICAgIGNvbnN0IHRhcmdldCA9IGNhbGwuZXhwcmVzc2lvbjtcbiAgICAgICAgICAgIGNvbnN0IHR5cGUgPSB0aGlzLmNoZWNrZXIuZ2V0VHlwZUF0TG9jYXRpb24odGFyZ2V0KTtcbiAgICAgICAgICAgIGlmICh0eXBlKSB7XG4gICAgICAgICAgICAgIGNvbnN0IHN0YXRpY1N5bWJvbCA9XG4gICAgICAgICAgICAgICAgICB0aGlzLnJlZmxlY3Rvci5nZXRTdGF0aWNTeW1ib2woc291cmNlRmlsZS5maWxlTmFtZSwgY2xhc3NEZWNsYXJhdGlvbi5uYW1lLnRleHQpO1xuICAgICAgICAgICAgICB0cnkge1xuICAgICAgICAgICAgICAgIGlmICh0aGlzLnJlc29sdmVyLmlzRGlyZWN0aXZlKHN0YXRpY1N5bWJvbCBhcyBhbnkpKSB7XG4gICAgICAgICAgICAgICAgICBjb25zdCB7bWV0YWRhdGF9ID1cbiAgICAgICAgICAgICAgICAgICAgICB0aGlzLnJlc29sdmVyLmdldE5vbk5vcm1hbGl6ZWREaXJlY3RpdmVNZXRhZGF0YShzdGF0aWNTeW1ib2wgYXMgYW55KSAhO1xuICAgICAgICAgICAgICAgICAgY29uc3QgZGVjbGFyYXRpb25TcGFuID0gc3Bhbk9mKHRhcmdldCk7XG4gICAgICAgICAgICAgICAgICByZXR1cm4ge1xuICAgICAgICAgICAgICAgICAgICB0eXBlOiBzdGF0aWNTeW1ib2wsXG4gICAgICAgICAgICAgICAgICAgIGRlY2xhcmF0aW9uU3BhbixcbiAgICAgICAgICAgICAgICAgICAgbWV0YWRhdGEsXG4gICAgICAgICAgICAgICAgICAgIGVycm9yczogdGhpcy5nZXRDb2xsZWN0ZWRFcnJvcnMoZGVjbGFyYXRpb25TcGFuLCBzb3VyY2VGaWxlKVxuICAgICAgICAgICAgICAgICAgfTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgICAgICAgICBpZiAoZS5tZXNzYWdlKSB7XG4gICAgICAgICAgICAgICAgICB0aGlzLmNvbGxlY3RFcnJvcihlLCBzb3VyY2VGaWxlLmZpbGVOYW1lKTtcbiAgICAgICAgICAgICAgICAgIGNvbnN0IGRlY2xhcmF0aW9uU3BhbiA9IHNwYW5PZih0YXJnZXQpO1xuICAgICAgICAgICAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgICAgICAgICAgdHlwZTogc3RhdGljU3ltYm9sLFxuICAgICAgICAgICAgICAgICAgICBkZWNsYXJhdGlvblNwYW4sXG4gICAgICAgICAgICAgICAgICAgIGVycm9yczogdGhpcy5nZXRDb2xsZWN0ZWRFcnJvcnMoZGVjbGFyYXRpb25TcGFuLCBzb3VyY2VGaWxlKVxuICAgICAgICAgICAgICAgICAgfTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHN0cmluZ09mKG5vZGU6IHRzLk5vZGUpOiBzdHJpbmd8dW5kZWZpbmVkIHtcbiAgICBzd2l0Y2ggKG5vZGUua2luZCkge1xuICAgICAgY2FzZSB0cy5TeW50YXhLaW5kLk5vU3Vic3RpdHV0aW9uVGVtcGxhdGVMaXRlcmFsOlxuICAgICAgICByZXR1cm4gKDx0cy5MaXRlcmFsRXhwcmVzc2lvbj5ub2RlKS50ZXh0O1xuICAgICAgY2FzZSB0cy5TeW50YXhLaW5kLlN0cmluZ0xpdGVyYWw6XG4gICAgICAgIHJldHVybiAoPHRzLlN0cmluZ0xpdGVyYWw+bm9kZSkudGV4dDtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGZpbmROb2RlKHNvdXJjZUZpbGU6IHRzLlNvdXJjZUZpbGUsIHBvc2l0aW9uOiBudW1iZXIpOiB0cy5Ob2RlfHVuZGVmaW5lZCB7XG4gICAgZnVuY3Rpb24gZmluZChub2RlOiB0cy5Ob2RlKTogdHMuTm9kZXx1bmRlZmluZWQge1xuICAgICAgaWYgKHBvc2l0aW9uID49IG5vZGUuZ2V0U3RhcnQoKSAmJiBwb3NpdGlvbiA8IG5vZGUuZ2V0RW5kKCkpIHtcbiAgICAgICAgcmV0dXJuIHRzLmZvckVhY2hDaGlsZChub2RlLCBmaW5kKSB8fCBub2RlO1xuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiBmaW5kKHNvdXJjZUZpbGUpO1xuICB9XG5cbiAgZ2V0VGVtcGxhdGVBc3RBdFBvc2l0aW9uKGZpbGVOYW1lOiBzdHJpbmcsIHBvc2l0aW9uOiBudW1iZXIpOiBUZW1wbGF0ZUluZm98dW5kZWZpbmVkIHtcbiAgICBsZXQgdGVtcGxhdGUgPSB0aGlzLmdldFRlbXBsYXRlQXQoZmlsZU5hbWUsIHBvc2l0aW9uKTtcbiAgICBpZiAodGVtcGxhdGUpIHtcbiAgICAgIGxldCBhc3RSZXN1bHQgPSB0aGlzLmdldFRlbXBsYXRlQXN0KHRlbXBsYXRlLCBmaWxlTmFtZSk7XG4gICAgICBpZiAoYXN0UmVzdWx0ICYmIGFzdFJlc3VsdC5odG1sQXN0ICYmIGFzdFJlc3VsdC50ZW1wbGF0ZUFzdCAmJiBhc3RSZXN1bHQuZGlyZWN0aXZlICYmXG4gICAgICAgICAgYXN0UmVzdWx0LmRpcmVjdGl2ZXMgJiYgYXN0UmVzdWx0LnBpcGVzICYmIGFzdFJlc3VsdC5leHByZXNzaW9uUGFyc2VyKVxuICAgICAgICByZXR1cm4ge1xuICAgICAgICAgIHBvc2l0aW9uLFxuICAgICAgICAgIGZpbGVOYW1lLFxuICAgICAgICAgIHRlbXBsYXRlLFxuICAgICAgICAgIGh0bWxBc3Q6IGFzdFJlc3VsdC5odG1sQXN0LFxuICAgICAgICAgIGRpcmVjdGl2ZTogYXN0UmVzdWx0LmRpcmVjdGl2ZSxcbiAgICAgICAgICBkaXJlY3RpdmVzOiBhc3RSZXN1bHQuZGlyZWN0aXZlcyxcbiAgICAgICAgICBwaXBlczogYXN0UmVzdWx0LnBpcGVzLFxuICAgICAgICAgIHRlbXBsYXRlQXN0OiBhc3RSZXN1bHQudGVtcGxhdGVBc3QsXG4gICAgICAgICAgZXhwcmVzc2lvblBhcnNlcjogYXN0UmVzdWx0LmV4cHJlc3Npb25QYXJzZXJcbiAgICAgICAgfTtcbiAgICB9XG4gICAgcmV0dXJuIHVuZGVmaW5lZDtcbiAgfVxuXG4gIGdldFRlbXBsYXRlQXN0KHRlbXBsYXRlOiBUZW1wbGF0ZVNvdXJjZSwgY29udGV4dEZpbGU6IHN0cmluZyk6IEFzdFJlc3VsdCB7XG4gICAgbGV0IHJlc3VsdDogQXN0UmVzdWx0fHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVzb2x2ZWRNZXRhZGF0YSA9XG4gICAgICAgICAgdGhpcy5yZXNvbHZlci5nZXROb25Ob3JtYWxpemVkRGlyZWN0aXZlTWV0YWRhdGEodGVtcGxhdGUudHlwZSBhcyBhbnkpO1xuICAgICAgY29uc3QgbWV0YWRhdGEgPSByZXNvbHZlZE1ldGFkYXRhICYmIHJlc29sdmVkTWV0YWRhdGEubWV0YWRhdGE7XG4gICAgICBpZiAobWV0YWRhdGEpIHtcbiAgICAgICAgY29uc3QgcmF3SHRtbFBhcnNlciA9IG5ldyBIdG1sUGFyc2VyKCk7XG4gICAgICAgIGNvbnN0IGh0bWxQYXJzZXIgPSBuZXcgSTE4Tkh0bWxQYXJzZXIocmF3SHRtbFBhcnNlcik7XG4gICAgICAgIGNvbnN0IGV4cHJlc3Npb25QYXJzZXIgPSBuZXcgUGFyc2VyKG5ldyBMZXhlcigpKTtcbiAgICAgICAgY29uc3QgY29uZmlnID0gbmV3IENvbXBpbGVyQ29uZmlnKCk7XG4gICAgICAgIGNvbnN0IHBhcnNlciA9IG5ldyBUZW1wbGF0ZVBhcnNlcihcbiAgICAgICAgICAgIGNvbmZpZywgdGhpcy5yZXNvbHZlci5nZXRSZWZsZWN0b3IoKSwgZXhwcmVzc2lvblBhcnNlciwgbmV3IERvbUVsZW1lbnRTY2hlbWFSZWdpc3RyeSgpLFxuICAgICAgICAgICAgaHRtbFBhcnNlciwgbnVsbCAhLCBbXSk7XG4gICAgICAgIGNvbnN0IGh0bWxSZXN1bHQgPSBodG1sUGFyc2VyLnBhcnNlKHRlbXBsYXRlLnNvdXJjZSwgJycsIHt0b2tlbml6ZUV4cGFuc2lvbkZvcm1zOiB0cnVlfSk7XG4gICAgICAgIGNvbnN0IGFuYWx5emVkTW9kdWxlcyA9IHRoaXMuZ2V0QW5hbHl6ZWRNb2R1bGVzKCk7XG4gICAgICAgIGxldCBlcnJvcnM6IERpYWdub3N0aWNbXXx1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG4gICAgICAgIGxldCBuZ01vZHVsZSA9IGFuYWx5emVkTW9kdWxlcy5uZ01vZHVsZUJ5UGlwZU9yRGlyZWN0aXZlLmdldCh0ZW1wbGF0ZS50eXBlKTtcbiAgICAgICAgaWYgKCFuZ01vZHVsZSkge1xuICAgICAgICAgIC8vIFJlcG9ydGVkIGJ5IHRoZSB0aGUgZGVjbGFyYXRpb24gZGlhZ25vc3RpY3MuXG4gICAgICAgICAgbmdNb2R1bGUgPSBmaW5kU3VpdGFibGVEZWZhdWx0TW9kdWxlKGFuYWx5emVkTW9kdWxlcyk7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5nTW9kdWxlKSB7XG4gICAgICAgICAgY29uc3QgZGlyZWN0aXZlcyA9XG4gICAgICAgICAgICAgIG5nTW9kdWxlLnRyYW5zaXRpdmVNb2R1bGUuZGlyZWN0aXZlc1xuICAgICAgICAgICAgICAgICAgLm1hcChkID0+IHRoaXMucmVzb2x2ZXIuZ2V0Tm9uTm9ybWFsaXplZERpcmVjdGl2ZU1ldGFkYXRhKGQucmVmZXJlbmNlKSlcbiAgICAgICAgICAgICAgICAgIC5maWx0ZXIoZCA9PiBkKVxuICAgICAgICAgICAgICAgICAgLm1hcChkID0+IGQgIS5tZXRhZGF0YS50b1N1bW1hcnkoKSk7XG4gICAgICAgICAgY29uc3QgcGlwZXMgPSBuZ01vZHVsZS50cmFuc2l0aXZlTW9kdWxlLnBpcGVzLm1hcChcbiAgICAgICAgICAgICAgcCA9PiB0aGlzLnJlc29sdmVyLmdldE9yTG9hZFBpcGVNZXRhZGF0YShwLnJlZmVyZW5jZSkudG9TdW1tYXJ5KCkpO1xuICAgICAgICAgIGNvbnN0IHNjaGVtYXMgPSBuZ01vZHVsZS5zY2hlbWFzO1xuICAgICAgICAgIGNvbnN0IHBhcnNlUmVzdWx0ID0gcGFyc2VyLnRyeVBhcnNlSHRtbChodG1sUmVzdWx0LCBtZXRhZGF0YSwgZGlyZWN0aXZlcywgcGlwZXMsIHNjaGVtYXMpO1xuICAgICAgICAgIHJlc3VsdCA9IHtcbiAgICAgICAgICAgIGh0bWxBc3Q6IGh0bWxSZXN1bHQucm9vdE5vZGVzLFxuICAgICAgICAgICAgdGVtcGxhdGVBc3Q6IHBhcnNlUmVzdWx0LnRlbXBsYXRlQXN0LFxuICAgICAgICAgICAgZGlyZWN0aXZlOiBtZXRhZGF0YSwgZGlyZWN0aXZlcywgcGlwZXMsXG4gICAgICAgICAgICBwYXJzZUVycm9yczogcGFyc2VSZXN1bHQuZXJyb3JzLCBleHByZXNzaW9uUGFyc2VyLCBlcnJvcnNcbiAgICAgICAgICB9O1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgbGV0IHNwYW4gPSB0ZW1wbGF0ZS5zcGFuO1xuICAgICAgaWYgKGUuZmlsZU5hbWUgPT0gY29udGV4dEZpbGUpIHtcbiAgICAgICAgc3BhbiA9IHRlbXBsYXRlLnF1ZXJ5LmdldFNwYW5BdChlLmxpbmUsIGUuY29sdW1uKSB8fCBzcGFuO1xuICAgICAgfVxuICAgICAgcmVzdWx0ID0ge2Vycm9yczogW3traW5kOiBEaWFnbm9zdGljS2luZC5FcnJvciwgbWVzc2FnZTogZS5tZXNzYWdlLCBzcGFufV19O1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0IHx8IHt9O1xuICB9XG59XG5cbmZ1bmN0aW9uIGZpbmRTdWl0YWJsZURlZmF1bHRNb2R1bGUobW9kdWxlczogTmdBbmFseXplZE1vZHVsZXMpOiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YXx1bmRlZmluZWQge1xuICBsZXQgcmVzdWx0OiBDb21waWxlTmdNb2R1bGVNZXRhZGF0YXx1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG4gIGxldCByZXN1bHRTaXplID0gMDtcbiAgZm9yIChjb25zdCBtb2R1bGUgb2YgbW9kdWxlcy5uZ01vZHVsZXMpIHtcbiAgICBjb25zdCBtb2R1bGVTaXplID0gbW9kdWxlLnRyYW5zaXRpdmVNb2R1bGUuZGlyZWN0aXZlcy5sZW5ndGg7XG4gICAgaWYgKG1vZHVsZVNpemUgPiByZXN1bHRTaXplKSB7XG4gICAgICByZXN1bHQgPSBtb2R1bGU7XG4gICAgICByZXN1bHRTaXplID0gbW9kdWxlU2l6ZTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIHJlc3VsdDtcbn1cblxuZnVuY3Rpb24gZmluZFRzQ29uZmlnKGZpbGVOYW1lOiBzdHJpbmcpOiBzdHJpbmd8dW5kZWZpbmVkIHtcbiAgbGV0IGRpciA9IHBhdGguZGlybmFtZShmaWxlTmFtZSk7XG4gIHdoaWxlIChmcy5leGlzdHNTeW5jKGRpcikpIHtcbiAgICBjb25zdCBjYW5kaWRhdGUgPSBwYXRoLmpvaW4oZGlyLCAndHNjb25maWcuanNvbicpO1xuICAgIGlmIChmcy5leGlzdHNTeW5jKGNhbmRpZGF0ZSkpIHJldHVybiBjYW5kaWRhdGU7XG4gICAgY29uc3QgcGFyZW50RGlyID0gcGF0aC5kaXJuYW1lKGRpcik7XG4gICAgaWYgKHBhcmVudERpciA9PT0gZGlyKSBicmVhaztcbiAgICBkaXIgPSBwYXJlbnREaXI7XG4gIH1cbn1cblxuZnVuY3Rpb24gc3Bhbk9mKG5vZGU6IHRzLk5vZGUpOiBTcGFuIHtcbiAgcmV0dXJuIHtzdGFydDogbm9kZS5nZXRTdGFydCgpLCBlbmQ6IG5vZGUuZ2V0RW5kKCl9O1xufVxuXG5mdW5jdGlvbiBzaHJpbmsoc3BhbjogU3Bhbiwgb2Zmc2V0PzogbnVtYmVyKSB7XG4gIGlmIChvZmZzZXQgPT0gbnVsbCkgb2Zmc2V0ID0gMTtcbiAgcmV0dXJuIHtzdGFydDogc3Bhbi5zdGFydCArIG9mZnNldCwgZW5kOiBzcGFuLmVuZCAtIG9mZnNldH07XG59XG5cbmZ1bmN0aW9uIHNwYW5BdChzb3VyY2VGaWxlOiB0cy5Tb3VyY2VGaWxlLCBsaW5lOiBudW1iZXIsIGNvbHVtbjogbnVtYmVyKTogU3Bhbnx1bmRlZmluZWQge1xuICBpZiAobGluZSAhPSBudWxsICYmIGNvbHVtbiAhPSBudWxsKSB7XG4gICAgY29uc3QgcG9zaXRpb24gPSB0cy5nZXRQb3NpdGlvbk9mTGluZUFuZENoYXJhY3Rlcihzb3VyY2VGaWxlLCBsaW5lLCBjb2x1bW4pO1xuICAgIGNvbnN0IGZpbmRDaGlsZCA9IGZ1bmN0aW9uIGZpbmRDaGlsZChub2RlOiB0cy5Ob2RlKTogdHMuTm9kZSB8IHVuZGVmaW5lZCB7XG4gICAgICBpZiAobm9kZS5raW5kID4gdHMuU3ludGF4S2luZC5MYXN0VG9rZW4gJiYgbm9kZS5wb3MgPD0gcG9zaXRpb24gJiYgbm9kZS5lbmQgPiBwb3NpdGlvbikge1xuICAgICAgICBjb25zdCBiZXR0ZXJOb2RlID0gdHMuZm9yRWFjaENoaWxkKG5vZGUsIGZpbmRDaGlsZCk7XG4gICAgICAgIHJldHVybiBiZXR0ZXJOb2RlIHx8IG5vZGU7XG4gICAgICB9XG4gICAgfTtcblxuICAgIGNvbnN0IG5vZGUgPSB0cy5mb3JFYWNoQ2hpbGQoc291cmNlRmlsZSwgZmluZENoaWxkKTtcbiAgICBpZiAobm9kZSkge1xuICAgICAgcmV0dXJuIHtzdGFydDogbm9kZS5nZXRTdGFydCgpLCBlbmQ6IG5vZGUuZ2V0RW5kKCl9O1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBjb252ZXJ0Q2hhaW4oY2hhaW46IEZvcm1hdHRlZE1lc3NhZ2VDaGFpbik6IERpYWdub3N0aWNNZXNzYWdlQ2hhaW4ge1xuICByZXR1cm4ge21lc3NhZ2U6IGNoYWluLm1lc3NhZ2UsIG5leHQ6IGNoYWluLm5leHQgPyBjb252ZXJ0Q2hhaW4oY2hhaW4ubmV4dCkgOiB1bmRlZmluZWR9O1xufVxuXG5mdW5jdGlvbiBlcnJvclRvRGlhZ25vc3RpY1dpdGhDaGFpbihlcnJvcjogRm9ybWF0dGVkRXJyb3IsIHNwYW46IFNwYW4pOiBEZWNsYXJhdGlvbkVycm9yIHtcbiAgcmV0dXJuIHttZXNzYWdlOiBlcnJvci5jaGFpbiA/IGNvbnZlcnRDaGFpbihlcnJvci5jaGFpbikgOiBlcnJvci5tZXNzYWdlLCBzcGFufTtcbn1cbiJdfQ==