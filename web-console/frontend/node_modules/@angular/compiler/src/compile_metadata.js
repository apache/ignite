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
        define("@angular/compiler/src/compile_metadata", ["require", "exports", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var util_1 = require("@angular/compiler/src/util");
    // group 0: "[prop] or (event) or @trigger"
    // group 1: "prop" from "[prop]"
    // group 2: "event" from "(event)"
    // group 3: "@trigger" from "@trigger"
    var HOST_REG_EXP = /^(?:(?:\[([^\]]+)\])|(?:\(([^\)]+)\)))|(\@[-\w]+)$/;
    function sanitizeIdentifier(name) {
        return name.replace(/\W/g, '_');
    }
    exports.sanitizeIdentifier = sanitizeIdentifier;
    var _anonymousTypeIndex = 0;
    function identifierName(compileIdentifier) {
        if (!compileIdentifier || !compileIdentifier.reference) {
            return null;
        }
        var ref = compileIdentifier.reference;
        if (ref instanceof static_symbol_1.StaticSymbol) {
            return ref.name;
        }
        if (ref['__anonymousType']) {
            return ref['__anonymousType'];
        }
        var identifier = util_1.stringify(ref);
        if (identifier.indexOf('(') >= 0) {
            // case: anonymous functions!
            identifier = "anonymous_" + _anonymousTypeIndex++;
            ref['__anonymousType'] = identifier;
        }
        else {
            identifier = sanitizeIdentifier(identifier);
        }
        return identifier;
    }
    exports.identifierName = identifierName;
    function identifierModuleUrl(compileIdentifier) {
        var ref = compileIdentifier.reference;
        if (ref instanceof static_symbol_1.StaticSymbol) {
            return ref.filePath;
        }
        // Runtime type
        return "./" + util_1.stringify(ref);
    }
    exports.identifierModuleUrl = identifierModuleUrl;
    function viewClassName(compType, embeddedTemplateIndex) {
        return "View_" + identifierName({ reference: compType }) + "_" + embeddedTemplateIndex;
    }
    exports.viewClassName = viewClassName;
    function rendererTypeName(compType) {
        return "RenderType_" + identifierName({ reference: compType });
    }
    exports.rendererTypeName = rendererTypeName;
    function hostViewClassName(compType) {
        return "HostView_" + identifierName({ reference: compType });
    }
    exports.hostViewClassName = hostViewClassName;
    function componentFactoryName(compType) {
        return identifierName({ reference: compType }) + "NgFactory";
    }
    exports.componentFactoryName = componentFactoryName;
    var CompileSummaryKind;
    (function (CompileSummaryKind) {
        CompileSummaryKind[CompileSummaryKind["Pipe"] = 0] = "Pipe";
        CompileSummaryKind[CompileSummaryKind["Directive"] = 1] = "Directive";
        CompileSummaryKind[CompileSummaryKind["NgModule"] = 2] = "NgModule";
        CompileSummaryKind[CompileSummaryKind["Injectable"] = 3] = "Injectable";
    })(CompileSummaryKind = exports.CompileSummaryKind || (exports.CompileSummaryKind = {}));
    function tokenName(token) {
        return token.value != null ? sanitizeIdentifier(token.value) : identifierName(token.identifier);
    }
    exports.tokenName = tokenName;
    function tokenReference(token) {
        if (token.identifier != null) {
            return token.identifier.reference;
        }
        else {
            return token.value;
        }
    }
    exports.tokenReference = tokenReference;
    /**
     * Metadata about a stylesheet
     */
    var CompileStylesheetMetadata = /** @class */ (function () {
        function CompileStylesheetMetadata(_a) {
            var _b = _a === void 0 ? {} : _a, moduleUrl = _b.moduleUrl, styles = _b.styles, styleUrls = _b.styleUrls;
            this.moduleUrl = moduleUrl || null;
            this.styles = _normalizeArray(styles);
            this.styleUrls = _normalizeArray(styleUrls);
        }
        return CompileStylesheetMetadata;
    }());
    exports.CompileStylesheetMetadata = CompileStylesheetMetadata;
    /**
     * Metadata regarding compilation of a template.
     */
    var CompileTemplateMetadata = /** @class */ (function () {
        function CompileTemplateMetadata(_a) {
            var encapsulation = _a.encapsulation, template = _a.template, templateUrl = _a.templateUrl, htmlAst = _a.htmlAst, styles = _a.styles, styleUrls = _a.styleUrls, externalStylesheets = _a.externalStylesheets, animations = _a.animations, ngContentSelectors = _a.ngContentSelectors, interpolation = _a.interpolation, isInline = _a.isInline, preserveWhitespaces = _a.preserveWhitespaces;
            this.encapsulation = encapsulation;
            this.template = template;
            this.templateUrl = templateUrl;
            this.htmlAst = htmlAst;
            this.styles = _normalizeArray(styles);
            this.styleUrls = _normalizeArray(styleUrls);
            this.externalStylesheets = _normalizeArray(externalStylesheets);
            this.animations = animations ? flatten(animations) : [];
            this.ngContentSelectors = ngContentSelectors || [];
            if (interpolation && interpolation.length != 2) {
                throw new Error("'interpolation' should have a start and an end symbol.");
            }
            this.interpolation = interpolation;
            this.isInline = isInline;
            this.preserveWhitespaces = preserveWhitespaces;
        }
        CompileTemplateMetadata.prototype.toSummary = function () {
            return {
                ngContentSelectors: this.ngContentSelectors,
                encapsulation: this.encapsulation,
                styles: this.styles,
                animations: this.animations
            };
        };
        return CompileTemplateMetadata;
    }());
    exports.CompileTemplateMetadata = CompileTemplateMetadata;
    /**
     * Metadata regarding compilation of a directive.
     */
    var CompileDirectiveMetadata = /** @class */ (function () {
        function CompileDirectiveMetadata(_a) {
            var isHost = _a.isHost, type = _a.type, isComponent = _a.isComponent, selector = _a.selector, exportAs = _a.exportAs, changeDetection = _a.changeDetection, inputs = _a.inputs, outputs = _a.outputs, hostListeners = _a.hostListeners, hostProperties = _a.hostProperties, hostAttributes = _a.hostAttributes, providers = _a.providers, viewProviders = _a.viewProviders, queries = _a.queries, guards = _a.guards, viewQueries = _a.viewQueries, entryComponents = _a.entryComponents, template = _a.template, componentViewType = _a.componentViewType, rendererType = _a.rendererType, componentFactory = _a.componentFactory;
            this.isHost = !!isHost;
            this.type = type;
            this.isComponent = isComponent;
            this.selector = selector;
            this.exportAs = exportAs;
            this.changeDetection = changeDetection;
            this.inputs = inputs;
            this.outputs = outputs;
            this.hostListeners = hostListeners;
            this.hostProperties = hostProperties;
            this.hostAttributes = hostAttributes;
            this.providers = _normalizeArray(providers);
            this.viewProviders = _normalizeArray(viewProviders);
            this.queries = _normalizeArray(queries);
            this.guards = guards;
            this.viewQueries = _normalizeArray(viewQueries);
            this.entryComponents = _normalizeArray(entryComponents);
            this.template = template;
            this.componentViewType = componentViewType;
            this.rendererType = rendererType;
            this.componentFactory = componentFactory;
        }
        CompileDirectiveMetadata.create = function (_a) {
            var isHost = _a.isHost, type = _a.type, isComponent = _a.isComponent, selector = _a.selector, exportAs = _a.exportAs, changeDetection = _a.changeDetection, inputs = _a.inputs, outputs = _a.outputs, host = _a.host, providers = _a.providers, viewProviders = _a.viewProviders, queries = _a.queries, guards = _a.guards, viewQueries = _a.viewQueries, entryComponents = _a.entryComponents, template = _a.template, componentViewType = _a.componentViewType, rendererType = _a.rendererType, componentFactory = _a.componentFactory;
            var hostListeners = {};
            var hostProperties = {};
            var hostAttributes = {};
            if (host != null) {
                Object.keys(host).forEach(function (key) {
                    var value = host[key];
                    var matches = key.match(HOST_REG_EXP);
                    if (matches === null) {
                        hostAttributes[key] = value;
                    }
                    else if (matches[1] != null) {
                        hostProperties[matches[1]] = value;
                    }
                    else if (matches[2] != null) {
                        hostListeners[matches[2]] = value;
                    }
                });
            }
            var inputsMap = {};
            if (inputs != null) {
                inputs.forEach(function (bindConfig) {
                    // canonical syntax: `dirProp: elProp`
                    // if there is no `:`, use dirProp = elProp
                    var parts = util_1.splitAtColon(bindConfig, [bindConfig, bindConfig]);
                    inputsMap[parts[0]] = parts[1];
                });
            }
            var outputsMap = {};
            if (outputs != null) {
                outputs.forEach(function (bindConfig) {
                    // canonical syntax: `dirProp: elProp`
                    // if there is no `:`, use dirProp = elProp
                    var parts = util_1.splitAtColon(bindConfig, [bindConfig, bindConfig]);
                    outputsMap[parts[0]] = parts[1];
                });
            }
            return new CompileDirectiveMetadata({
                isHost: isHost,
                type: type,
                isComponent: !!isComponent, selector: selector, exportAs: exportAs, changeDetection: changeDetection,
                inputs: inputsMap,
                outputs: outputsMap,
                hostListeners: hostListeners,
                hostProperties: hostProperties,
                hostAttributes: hostAttributes,
                providers: providers,
                viewProviders: viewProviders,
                queries: queries,
                guards: guards,
                viewQueries: viewQueries,
                entryComponents: entryComponents,
                template: template,
                componentViewType: componentViewType,
                rendererType: rendererType,
                componentFactory: componentFactory,
            });
        };
        CompileDirectiveMetadata.prototype.toSummary = function () {
            return {
                summaryKind: CompileSummaryKind.Directive,
                type: this.type,
                isComponent: this.isComponent,
                selector: this.selector,
                exportAs: this.exportAs,
                inputs: this.inputs,
                outputs: this.outputs,
                hostListeners: this.hostListeners,
                hostProperties: this.hostProperties,
                hostAttributes: this.hostAttributes,
                providers: this.providers,
                viewProviders: this.viewProviders,
                queries: this.queries,
                guards: this.guards,
                viewQueries: this.viewQueries,
                entryComponents: this.entryComponents,
                changeDetection: this.changeDetection,
                template: this.template && this.template.toSummary(),
                componentViewType: this.componentViewType,
                rendererType: this.rendererType,
                componentFactory: this.componentFactory
            };
        };
        return CompileDirectiveMetadata;
    }());
    exports.CompileDirectiveMetadata = CompileDirectiveMetadata;
    var CompilePipeMetadata = /** @class */ (function () {
        function CompilePipeMetadata(_a) {
            var type = _a.type, name = _a.name, pure = _a.pure;
            this.type = type;
            this.name = name;
            this.pure = !!pure;
        }
        CompilePipeMetadata.prototype.toSummary = function () {
            return {
                summaryKind: CompileSummaryKind.Pipe,
                type: this.type,
                name: this.name,
                pure: this.pure
            };
        };
        return CompilePipeMetadata;
    }());
    exports.CompilePipeMetadata = CompilePipeMetadata;
    var CompileShallowModuleMetadata = /** @class */ (function () {
        function CompileShallowModuleMetadata() {
        }
        return CompileShallowModuleMetadata;
    }());
    exports.CompileShallowModuleMetadata = CompileShallowModuleMetadata;
    /**
     * Metadata regarding compilation of a module.
     */
    var CompileNgModuleMetadata = /** @class */ (function () {
        function CompileNgModuleMetadata(_a) {
            var type = _a.type, providers = _a.providers, declaredDirectives = _a.declaredDirectives, exportedDirectives = _a.exportedDirectives, declaredPipes = _a.declaredPipes, exportedPipes = _a.exportedPipes, entryComponents = _a.entryComponents, bootstrapComponents = _a.bootstrapComponents, importedModules = _a.importedModules, exportedModules = _a.exportedModules, schemas = _a.schemas, transitiveModule = _a.transitiveModule, id = _a.id;
            this.type = type || null;
            this.declaredDirectives = _normalizeArray(declaredDirectives);
            this.exportedDirectives = _normalizeArray(exportedDirectives);
            this.declaredPipes = _normalizeArray(declaredPipes);
            this.exportedPipes = _normalizeArray(exportedPipes);
            this.providers = _normalizeArray(providers);
            this.entryComponents = _normalizeArray(entryComponents);
            this.bootstrapComponents = _normalizeArray(bootstrapComponents);
            this.importedModules = _normalizeArray(importedModules);
            this.exportedModules = _normalizeArray(exportedModules);
            this.schemas = _normalizeArray(schemas);
            this.id = id || null;
            this.transitiveModule = transitiveModule || null;
        }
        CompileNgModuleMetadata.prototype.toSummary = function () {
            var module = this.transitiveModule;
            return {
                summaryKind: CompileSummaryKind.NgModule,
                type: this.type,
                entryComponents: module.entryComponents,
                providers: module.providers,
                modules: module.modules,
                exportedDirectives: module.exportedDirectives,
                exportedPipes: module.exportedPipes
            };
        };
        return CompileNgModuleMetadata;
    }());
    exports.CompileNgModuleMetadata = CompileNgModuleMetadata;
    var TransitiveCompileNgModuleMetadata = /** @class */ (function () {
        function TransitiveCompileNgModuleMetadata() {
            this.directivesSet = new Set();
            this.directives = [];
            this.exportedDirectivesSet = new Set();
            this.exportedDirectives = [];
            this.pipesSet = new Set();
            this.pipes = [];
            this.exportedPipesSet = new Set();
            this.exportedPipes = [];
            this.modulesSet = new Set();
            this.modules = [];
            this.entryComponentsSet = new Set();
            this.entryComponents = [];
            this.providers = [];
        }
        TransitiveCompileNgModuleMetadata.prototype.addProvider = function (provider, module) {
            this.providers.push({ provider: provider, module: module });
        };
        TransitiveCompileNgModuleMetadata.prototype.addDirective = function (id) {
            if (!this.directivesSet.has(id.reference)) {
                this.directivesSet.add(id.reference);
                this.directives.push(id);
            }
        };
        TransitiveCompileNgModuleMetadata.prototype.addExportedDirective = function (id) {
            if (!this.exportedDirectivesSet.has(id.reference)) {
                this.exportedDirectivesSet.add(id.reference);
                this.exportedDirectives.push(id);
            }
        };
        TransitiveCompileNgModuleMetadata.prototype.addPipe = function (id) {
            if (!this.pipesSet.has(id.reference)) {
                this.pipesSet.add(id.reference);
                this.pipes.push(id);
            }
        };
        TransitiveCompileNgModuleMetadata.prototype.addExportedPipe = function (id) {
            if (!this.exportedPipesSet.has(id.reference)) {
                this.exportedPipesSet.add(id.reference);
                this.exportedPipes.push(id);
            }
        };
        TransitiveCompileNgModuleMetadata.prototype.addModule = function (id) {
            if (!this.modulesSet.has(id.reference)) {
                this.modulesSet.add(id.reference);
                this.modules.push(id);
            }
        };
        TransitiveCompileNgModuleMetadata.prototype.addEntryComponent = function (ec) {
            if (!this.entryComponentsSet.has(ec.componentType)) {
                this.entryComponentsSet.add(ec.componentType);
                this.entryComponents.push(ec);
            }
        };
        return TransitiveCompileNgModuleMetadata;
    }());
    exports.TransitiveCompileNgModuleMetadata = TransitiveCompileNgModuleMetadata;
    function _normalizeArray(obj) {
        return obj || [];
    }
    var ProviderMeta = /** @class */ (function () {
        function ProviderMeta(token, _a) {
            var useClass = _a.useClass, useValue = _a.useValue, useExisting = _a.useExisting, useFactory = _a.useFactory, deps = _a.deps, multi = _a.multi;
            this.token = token;
            this.useClass = useClass || null;
            this.useValue = useValue;
            this.useExisting = useExisting;
            this.useFactory = useFactory || null;
            this.dependencies = deps || null;
            this.multi = !!multi;
        }
        return ProviderMeta;
    }());
    exports.ProviderMeta = ProviderMeta;
    function flatten(list) {
        return list.reduce(function (flat, item) {
            var flatItem = Array.isArray(item) ? flatten(item) : item;
            return flat.concat(flatItem);
        }, []);
    }
    exports.flatten = flatten;
    function jitSourceUrl(url) {
        // Note: We need 3 "/" so that ng shows up as a separate domain
        // in the chrome dev tools.
        return url.replace(/(\w+:\/\/[\w:-]+)?(\/+)?/, 'ng:///');
    }
    function templateSourceUrl(ngModuleType, compMeta, templateMeta) {
        var url;
        if (templateMeta.isInline) {
            if (compMeta.type.reference instanceof static_symbol_1.StaticSymbol) {
                // Note: a .ts file might contain multiple components with inline templates,
                // so we need to give them unique urls, as these will be used for sourcemaps.
                url = compMeta.type.reference.filePath + "." + compMeta.type.reference.name + ".html";
            }
            else {
                url = identifierName(ngModuleType) + "/" + identifierName(compMeta.type) + ".html";
            }
        }
        else {
            url = templateMeta.templateUrl;
        }
        return compMeta.type.reference instanceof static_symbol_1.StaticSymbol ? url : jitSourceUrl(url);
    }
    exports.templateSourceUrl = templateSourceUrl;
    function sharedStylesheetJitUrl(meta, id) {
        var pathParts = meta.moduleUrl.split(/\/\\/g);
        var baseName = pathParts[pathParts.length - 1];
        return jitSourceUrl("css/" + id + baseName + ".ngstyle.js");
    }
    exports.sharedStylesheetJitUrl = sharedStylesheetJitUrl;
    function ngModuleJitUrl(moduleMeta) {
        return jitSourceUrl(identifierName(moduleMeta.type) + "/module.ngfactory.js");
    }
    exports.ngModuleJitUrl = ngModuleJitUrl;
    function templateJitUrl(ngModuleType, compMeta) {
        return jitSourceUrl(identifierName(ngModuleType) + "/" + identifierName(compMeta.type) + ".ngfactory.js");
    }
    exports.templateJitUrl = templateJitUrl;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZV9tZXRhZGF0YS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9jb21waWxlX21ldGFkYXRhLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgseUVBQWlEO0lBSWpELG1EQUErQztJQUUvQywyQ0FBMkM7SUFDM0MsZ0NBQWdDO0lBQ2hDLGtDQUFrQztJQUNsQyxzQ0FBc0M7SUFDdEMsSUFBTSxZQUFZLEdBQUcsb0RBQW9ELENBQUM7SUFFMUUsU0FBZ0Isa0JBQWtCLENBQUMsSUFBWTtRQUM3QyxPQUFPLElBQUksQ0FBQyxPQUFPLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxDQUFDO0lBQ2xDLENBQUM7SUFGRCxnREFFQztJQUVELElBQUksbUJBQW1CLEdBQUcsQ0FBQyxDQUFDO0lBRTVCLFNBQWdCLGNBQWMsQ0FBQyxpQkFBK0Q7UUFFNUYsSUFBSSxDQUFDLGlCQUFpQixJQUFJLENBQUMsaUJBQWlCLENBQUMsU0FBUyxFQUFFO1lBQ3RELE9BQU8sSUFBSSxDQUFDO1NBQ2I7UUFDRCxJQUFNLEdBQUcsR0FBRyxpQkFBaUIsQ0FBQyxTQUFTLENBQUM7UUFDeEMsSUFBSSxHQUFHLFlBQVksNEJBQVksRUFBRTtZQUMvQixPQUFPLEdBQUcsQ0FBQyxJQUFJLENBQUM7U0FDakI7UUFDRCxJQUFJLEdBQUcsQ0FBQyxpQkFBaUIsQ0FBQyxFQUFFO1lBQzFCLE9BQU8sR0FBRyxDQUFDLGlCQUFpQixDQUFDLENBQUM7U0FDL0I7UUFDRCxJQUFJLFVBQVUsR0FBRyxnQkFBUyxDQUFDLEdBQUcsQ0FBQyxDQUFDO1FBQ2hDLElBQUksVUFBVSxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDaEMsNkJBQTZCO1lBQzdCLFVBQVUsR0FBRyxlQUFhLG1CQUFtQixFQUFJLENBQUM7WUFDbEQsR0FBRyxDQUFD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LDhDQWdCQztJQUVELFNBQWdCLHNCQUFzQixDQUFDLElBQStCLEVBQUUsRUFBVTtRQUNoRixJQUFNLFNBQVMsR0FBRyxJQUFJLENBQUMsU0FBVyxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztRQUNsRCxJQUFNLFFBQVEsR0FBRyxTQUFTLENBQUMsU0FBUyxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUMsQ0FBQztRQUNqRCxPQUFPLFlBQVksQ0FBQyxTQUFPLEVBQUUsR0FBRyxRQUFRLGdCQUFhLENBQUMsQ0FBQztJQUN6RCxDQUFDO0lBSkQsd0RBSUM7SUFFRCxTQUFnQixjQUFjLENBQUMsVUFBbUM7UUFDaEUsT0FBTyxZQUFZLENBQUksY0FBYyxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMseUJBQXNCLENBQUMsQ0FBQztJQUNoRixDQUFDO0lBRkQsd0NBRUM7SUFFRCxTQUFnQixjQUFjLENBQzFCLFlBQXVDLEVBQUUsUUFBa0M7UUFDN0UsT0FBTyxZQUFZLENBQ1osY0FBYyxDQUFDLFlBQVksQ0FBQyxTQUFJLGNBQWMsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLGtCQUFlLENBQUMsQ0FBQztJQUN2RixDQUFDO0lBSkQsd0NBSUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U3RhdGljU3ltYm9sfSBmcm9tICcuL2FvdC9zdGF0aWNfc3ltYm9sJztcbmltcG9ydCB7Q2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIFNjaGVtYU1ldGFkYXRhLCBUeXBlLCBWaWV3RW5jYXBzdWxhdGlvbn0gZnJvbSAnLi9jb3JlJztcbmltcG9ydCB7TGlmZWN5Y2xlSG9va3N9IGZyb20gJy4vbGlmZWN5Y2xlX3JlZmxlY3Rvcic7XG5pbXBvcnQge1BhcnNlVHJlZVJlc3VsdCBhcyBIdG1sUGFyc2VUcmVlUmVzdWx0fSBmcm9tICcuL21sX3BhcnNlci9wYXJzZXInO1xuaW1wb3J0IHtzcGxpdEF0Q29sb24sIHN0cmluZ2lmeX0gZnJvbSAnLi91dGlsJztcblxuLy8gZ3JvdXAgMDogXCJbcHJvcF0gb3IgKGV2ZW50KSBvciBAdHJpZ2dlclwiXG4vLyBncm91cCAxOiBcInByb3BcIiBmcm9tIFwiW3Byb3BdXCJcbi8vIGdyb3VwIDI6IFwiZXZlbnRcIiBmcm9tIFwiKGV2ZW50KVwiXG4vLyBncm91cCAzOiBcIkB0cmlnZ2VyXCIgZnJvbSBcIkB0cmlnZ2VyXCJcbmNvbnN0IEhPU1RfUkVHX0VYUCA9IC9eKD86KD86XFxbKFteXFxdXSspXFxdKXwoPzpcXCgoW15cXCldKylcXCkpKXwoXFxAWy1cXHddKykkLztcblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplSWRlbnRpZmllcihuYW1lOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gbmFtZS5yZXBsYWNlKC9cXFcvZywgJ18nKTtcbn1cblxubGV0IF9hbm9ueW1vdXNUeXBlSW5kZXggPSAwO1xuXG5leHBvcnQgZnVuY3Rpb24gaWRlbnRpZmllck5hbWUoY29tcGlsZUlkZW50aWZpZXI6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEgfCBudWxsIHwgdW5kZWZpbmVkKTpcbiAgICBzdHJpbmd8bnVsbCB7XG4gIGlmICghY29tcGlsZUlkZW50aWZpZXIgfHwgIWNvbXBpbGVJZGVudGlmaWVyLnJlZmVyZW5jZSkge1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIGNvbnN0IHJlZiA9IGNvbXBpbGVJZGVudGlmaWVyLnJlZmVyZW5jZTtcbiAgaWYgKHJlZiBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkge1xuICAgIHJldHVybiByZWYubmFtZTtcbiAgfVxuICBpZiAocmVmWydfX2Fub255bW91c1R5cGUnXSkge1xuICAgIHJldHVybiByZWZbJ19fYW5vbnltb3VzVHlwZSddO1xuICB9XG4gIGxldCBpZGVudGlmaWVyID0gc3RyaW5naWZ5KHJlZik7XG4gIGlmIChpZGVudGlmaWVyLmluZGV4T2YoJygnKSA+PSAwKSB7XG4gICAgLy8gY2FzZTogYW5vbnltb3VzIGZ1bmN0aW9ucyFcbiAgICBpZGVudGlmaWVyID0gYGFub255bW91c18ke19hbm9ueW1vdXNUeXBlSW5kZXgrK31gO1xuICAgIHJlZlsnX19hbm9ueW1vdXNUeXBlJ10gPSBpZGVudGlmaWVyO1xuICB9IGVsc2Uge1xuICAgIGlkZW50aWZpZXIgPSBzYW5pdGl6ZUlkZW50aWZpZXIoaWRlbnRpZmllcik7XG4gIH1cbiAgcmV0dXJuIGlkZW50aWZpZXI7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpZGVudGlmaWVyTW9kdWxlVXJsKGNvbXBpbGVJZGVudGlmaWVyOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhKTogc3RyaW5nIHtcbiAgY29uc3QgcmVmID0gY29tcGlsZUlkZW50aWZpZXIucmVmZXJlbmNlO1xuICBpZiAocmVmIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgcmV0dXJuIHJlZi5maWxlUGF0aDtcbiAgfVxuICAvLyBSdW50aW1lIHR5cGVcbiAgcmV0dXJuIGAuLyR7c3RyaW5naWZ5KHJlZil9YDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZpZXdDbGFzc05hbWUoY29tcFR5cGU6IGFueSwgZW1iZWRkZWRUZW1wbGF0ZUluZGV4OiBudW1iZXIpOiBzdHJpbmcge1xuICByZXR1cm4gYFZpZXdfJHtpZGVudGlmaWVyTmFtZSh7cmVmZXJlbmNlOiBjb21wVHlwZX0pfV8ke2VtYmVkZGVkVGVtcGxhdGVJbmRleH1gO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcmVuZGVyZXJUeXBlTmFtZShjb21wVHlwZTogYW55KTogc3RyaW5nIHtcbiAgcmV0dXJuIGBSZW5kZXJUeXBlXyR7aWRlbnRpZmllck5hbWUoe3JlZmVyZW5jZTogY29tcFR5cGV9KX1gO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaG9zdFZpZXdDbGFzc05hbWUoY29tcFR5cGU6IGFueSk6IHN0cmluZyB7XG4gIHJldHVybiBgSG9zdFZpZXdfJHtpZGVudGlmaWVyTmFtZSh7cmVmZXJlbmNlOiBjb21wVHlwZX0pfWA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjb21wb25lbnRGYWN0b3J5TmFtZShjb21wVHlwZTogYW55KTogc3RyaW5nIHtcbiAgcmV0dXJuIGAke2lkZW50aWZpZXJOYW1lKHtyZWZlcmVuY2U6IGNvbXBUeXBlfSl9TmdGYWN0b3J5YDtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBQcm94eUNsYXNzIHsgc2V0RGVsZWdhdGUoZGVsZWdhdGU6IGFueSk6IHZvaWQ7IH1cblxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhIHsgcmVmZXJlbmNlOiBhbnk7IH1cblxuZXhwb3J0IGVudW0gQ29tcGlsZVN1bW1hcnlLaW5kIHtcbiAgUGlwZSxcbiAgRGlyZWN0aXZlLFxuICBOZ01vZHVsZSxcbiAgSW5qZWN0YWJsZVxufVxuXG4vKipcbiAqIEEgQ29tcGlsZVN1bW1hcnkgaXMgdGhlIGRhdGEgbmVlZGVkIHRvIHVzZSBhIGRpcmVjdGl2ZSAvIHBpcGUgLyBtb2R1bGVcbiAqIGluIG90aGVyIG1vZHVsZXMgLyBjb21wb25lbnRzLiBIb3dldmVyLCB0aGlzIGRhdGEgaXMgbm90IGVub3VnaCB0byBjb21waWxlXG4gKiB0aGUgZGlyZWN0aXZlIC8gbW9kdWxlIGl0c2VsZi5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBDb21waWxlVHlwZVN1bW1hcnkge1xuICBzdW1tYXJ5S2luZDogQ29tcGlsZVN1bW1hcnlLaW5kfG51bGw7XG4gIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGE7XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhIHtcbiAgaXNBdHRyaWJ1dGU/OiBib29sZWFuO1xuICBpc1NlbGY/OiBib29sZWFuO1xuICBpc0hvc3Q/OiBib29sZWFuO1xuICBpc1NraXBTZWxmPzogYm9vbGVhbjtcbiAgaXNPcHRpb25hbD86IGJvb2xlYW47XG4gIGlzVmFsdWU/OiBib29sZWFuO1xuICB0b2tlbj86IENvbXBpbGVUb2tlbk1ldGFkYXRhO1xuICB2YWx1ZT86IGFueTtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSB7XG4gIHRva2VuOiBDb21waWxlVG9rZW5NZXRhZGF0YTtcbiAgdXNlQ2xhc3M/OiBDb21waWxlVHlwZU1ldGFkYXRhO1xuICB1c2VWYWx1ZT86IGFueTtcbiAgdXNlRXhpc3Rpbmc/OiBDb21waWxlVG9rZW5NZXRhZGF0YTtcbiAgdXNlRmFjdG9yeT86IENvbXBpbGVGYWN0b3J5TWV0YWRhdGE7XG4gIGRlcHM/OiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGFbXTtcbiAgbXVsdGk/OiBib29sZWFuO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVGYWN0b3J5TWV0YWRhdGEgZXh0ZW5kcyBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhIHtcbiAgZGlEZXBzOiBDb21waWxlRGlEZXBlbmRlbmN5TWV0YWRhdGFbXTtcbiAgcmVmZXJlbmNlOiBhbnk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0b2tlbk5hbWUodG9rZW46IENvbXBpbGVUb2tlbk1ldGFkYXRhKSB7XG4gIHJldHVybiB0b2tlbi52YWx1ZSAhPSBudWxsID8gc2FuaXRpemVJZGVudGlmaWVyKHRva2VuLnZhbHVlKSA6IGlkZW50aWZpZXJOYW1lKHRva2VuLmlkZW50aWZpZXIpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdG9rZW5SZWZlcmVuY2UodG9rZW46IENvbXBpbGVUb2tlbk1ldGFkYXRhKSB7XG4gIGlmICh0b2tlbi5pZGVudGlmaWVyICE9IG51bGwpIHtcbiAgICByZXR1cm4gdG9rZW4uaWRlbnRpZmllci5yZWZlcmVuY2U7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIHRva2VuLnZhbHVlO1xuICB9XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ29tcGlsZVRva2VuTWV0YWRhdGEge1xuICB2YWx1ZT86IGFueTtcbiAgaWRlbnRpZmllcj86IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGF8Q29tcGlsZVR5cGVNZXRhZGF0YTtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhIHtcbiAgc3ltYm9sOiBTdGF0aWNTeW1ib2w7XG4gIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGE7XG5cbiAgcHJvdmlkZWRJbj86IFN0YXRpY1N5bWJvbDtcblxuICB1c2VWYWx1ZT86IGFueTtcbiAgdXNlQ2xhc3M/OiBTdGF0aWNTeW1ib2w7XG4gIHVzZUV4aXN0aW5nPzogU3RhdGljU3ltYm9sO1xuICB1c2VGYWN0b3J5PzogU3RhdGljU3ltYm9sO1xuICBkZXBzPzogYW55W107XG59XG5cbi8qKlxuICogTWV0YWRhdGEgcmVnYXJkaW5nIGNvbXBpbGF0aW9uIG9mIGEgdHlwZS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBDb21waWxlVHlwZU1ldGFkYXRhIGV4dGVuZHMgQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YSB7XG4gIGRpRGVwczogQ29tcGlsZURpRGVwZW5kZW5jeU1ldGFkYXRhW107XG4gIGxpZmVjeWNsZUhvb2tzOiBMaWZlY3ljbGVIb29rc1tdO1xuICByZWZlcmVuY2U6IGFueTtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlUXVlcnlNZXRhZGF0YSB7XG4gIHNlbGVjdG9yczogQXJyYXk8Q29tcGlsZVRva2VuTWV0YWRhdGE+O1xuICBkZXNjZW5kYW50czogYm9vbGVhbjtcbiAgZmlyc3Q6IGJvb2xlYW47XG4gIHByb3BlcnR5TmFtZTogc3RyaW5nO1xuICByZWFkOiBDb21waWxlVG9rZW5NZXRhZGF0YTtcbiAgc3RhdGljPzogYm9vbGVhbjtcbn1cblxuLyoqXG4gKiBNZXRhZGF0YSBhYm91dCBhIHN0eWxlc2hlZXRcbiAqL1xuZXhwb3J0IGNsYXNzIENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEge1xuICBtb2R1bGVVcmw6IHN0cmluZ3xudWxsO1xuICBzdHlsZXM6IHN0cmluZ1tdO1xuICBzdHlsZVVybHM6IHN0cmluZ1tdO1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHttb2R1bGVVcmwsIHN0eWxlcyxcbiAgICAgICBzdHlsZVVybHN9OiB7bW9kdWxlVXJsPzogc3RyaW5nLCBzdHlsZXM/OiBzdHJpbmdbXSwgc3R5bGVVcmxzPzogc3RyaW5nW119ID0ge30pIHtcbiAgICB0aGlzLm1vZHVsZVVybCA9IG1vZHVsZVVybCB8fCBudWxsO1xuICAgIHRoaXMuc3R5bGVzID0gX25vcm1hbGl6ZUFycmF5KHN0eWxlcyk7XG4gICAgdGhpcy5zdHlsZVVybHMgPSBfbm9ybWFsaXplQXJyYXkoc3R5bGVVcmxzKTtcbiAgfVxufVxuXG4vKipcbiAqIFN1bW1hcnkgTWV0YWRhdGEgcmVnYXJkaW5nIGNvbXBpbGF0aW9uIG9mIGEgdGVtcGxhdGUuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgQ29tcGlsZVRlbXBsYXRlU3VtbWFyeSB7XG4gIG5nQ29udGVudFNlbGVjdG9yczogc3RyaW5nW107XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9ufG51bGw7XG4gIHN0eWxlczogc3RyaW5nW107XG4gIGFuaW1hdGlvbnM6IGFueVtdfG51bGw7XG59XG5cbi8qKlxuICogTWV0YWRhdGEgcmVnYXJkaW5nIGNvbXBpbGF0aW9uIG9mIGEgdGVtcGxhdGUuXG4gKi9cbmV4cG9ydCBjbGFzcyBDb21waWxlVGVtcGxhdGVNZXRhZGF0YSB7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9ufG51bGw7XG4gIHRlbXBsYXRlOiBzdHJpbmd8bnVsbDtcbiAgdGVtcGxhdGVVcmw6IHN0cmluZ3xudWxsO1xuICBodG1sQXN0OiBIdG1sUGFyc2VUcmVlUmVzdWx0fG51bGw7XG4gIGlzSW5saW5lOiBib29sZWFuO1xuICBzdHlsZXM6IHN0cmluZ1tdO1xuICBzdHlsZVVybHM6IHN0cmluZ1tdO1xuICBleHRlcm5hbFN0eWxlc2hlZXRzOiBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhW107XG4gIGFuaW1hdGlvbnM6IGFueVtdO1xuICBuZ0NvbnRlbnRTZWxlY3RvcnM6IHN0cmluZ1tdO1xuICBpbnRlcnBvbGF0aW9uOiBbc3RyaW5nLCBzdHJpbmddfG51bGw7XG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGJvb2xlYW47XG4gIGNvbnN0cnVjdG9yKHtlbmNhcHN1bGF0aW9uLCB0ZW1wbGF0ZSwgdGVtcGxhdGVVcmwsIGh0bWxBc3QsIHN0eWxlcywgc3R5bGVVcmxzLFxuICAgICAgICAgICAgICAgZXh0ZXJuYWxTdHlsZXNoZWV0cywgYW5pbWF0aW9ucywgbmdDb250ZW50U2VsZWN0b3JzLCBpbnRlcnBvbGF0aW9uLCBpc0lubGluZSxcbiAgICAgICAgICAgICAgIHByZXNlcnZlV2hpdGVzcGFjZXN9OiB7XG4gICAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24gfCBudWxsLFxuICAgIHRlbXBsYXRlOiBzdHJpbmd8bnVsbCxcbiAgICB0ZW1wbGF0ZVVybDogc3RyaW5nfG51bGwsXG4gICAgaHRtbEFzdDogSHRtbFBhcnNlVHJlZVJlc3VsdHxudWxsLFxuICAgIHN0eWxlczogc3RyaW5nW10sXG4gICAgc3R5bGVVcmxzOiBzdHJpbmdbXSxcbiAgICBleHRlcm5hbFN0eWxlc2hlZXRzOiBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhW10sXG4gICAgbmdDb250ZW50U2VsZWN0b3JzOiBzdHJpbmdbXSxcbiAgICBhbmltYXRpb25zOiBhbnlbXSxcbiAgICBpbnRlcnBvbGF0aW9uOiBbc3RyaW5nLCBzdHJpbmddfG51bGwsXG4gICAgaXNJbmxpbmU6IGJvb2xlYW4sXG4gICAgcHJlc2VydmVXaGl0ZXNwYWNlczogYm9vbGVhblxuICB9KSB7XG4gICAgdGhpcy5lbmNhcHN1bGF0aW9uID0gZW5jYXBzdWxhdGlvbjtcbiAgICB0aGlzLnRlbXBsYXRlID0gdGVtcGxhdGU7XG4gICAgdGhpcy50ZW1wbGF0ZVVybCA9IHRlbXBsYXRlVXJsO1xuICAgIHRoaXMuaHRtbEFzdCA9IGh0bWxBc3Q7XG4gICAgdGhpcy5zdHlsZXMgPSBfbm9ybWFsaXplQXJyYXkoc3R5bGVzKTtcbiAgICB0aGlzLnN0eWxlVXJscyA9IF9ub3JtYWxpemVBcnJheShzdHlsZVVybHMpO1xuICAgIHRoaXMuZXh0ZXJuYWxTdHlsZXNoZWV0cyA9IF9ub3JtYWxpemVBcnJheShleHRlcm5hbFN0eWxlc2hlZXRzKTtcbiAgICB0aGlzLmFuaW1hdGlvbnMgPSBhbmltYXRpb25zID8gZmxhdHRlbihhbmltYXRpb25zKSA6IFtdO1xuICAgIHRoaXMubmdDb250ZW50U2VsZWN0b3JzID0gbmdDb250ZW50U2VsZWN0b3JzIHx8IFtdO1xuICAgIGlmIChpbnRlcnBvbGF0aW9uICYmIGludGVycG9sYXRpb24ubGVuZ3RoICE9IDIpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgJ2ludGVycG9sYXRpb24nIHNob3VsZCBoYXZlIGEgc3RhcnQgYW5kIGFuIGVuZCBzeW1ib2wuYCk7XG4gICAgfVxuICAgIHRoaXMuaW50ZXJwb2xhdGlvbiA9IGludGVycG9sYXRpb247XG4gICAgdGhpcy5pc0lubGluZSA9IGlzSW5saW5lO1xuICAgIHRoaXMucHJlc2VydmVXaGl0ZXNwYWNlcyA9IHByZXNlcnZlV2hpdGVzcGFjZXM7XG4gIH1cblxuICB0b1N1bW1hcnkoKTogQ29tcGlsZVRlbXBsYXRlU3VtbWFyeSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIG5nQ29udGVudFNlbGVjdG9yczogdGhpcy5uZ0NvbnRlbnRTZWxlY3RvcnMsXG4gICAgICBlbmNhcHN1bGF0aW9uOiB0aGlzLmVuY2Fwc3VsYXRpb24sXG4gICAgICBzdHlsZXM6IHRoaXMuc3R5bGVzLFxuICAgICAgYW5pbWF0aW9uczogdGhpcy5hbmltYXRpb25zXG4gICAgfTtcbiAgfVxufVxuXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhIHtcbiAgY29tcG9uZW50VHlwZTogYW55O1xuICBjb21wb25lbnRGYWN0b3J5OiBTdGF0aWNTeW1ib2x8b2JqZWN0O1xufVxuXG4vLyBOb3RlOiBUaGlzIHNob3VsZCBvbmx5IHVzZSBpbnRlcmZhY2VzIGFzIG5lc3RlZCBkYXRhIHR5cGVzXG4vLyBhcyB3ZSBuZWVkIHRvIGJlIGFibGUgdG8gc2VyaWFsaXplIHRoaXMgZnJvbS90byBKU09OIVxuZXhwb3J0IGludGVyZmFjZSBDb21waWxlRGlyZWN0aXZlU3VtbWFyeSBleHRlbmRzIENvbXBpbGVUeXBlU3VtbWFyeSB7XG4gIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGE7XG4gIGlzQ29tcG9uZW50OiBib29sZWFuO1xuICBzZWxlY3Rvcjogc3RyaW5nfG51bGw7XG4gIGV4cG9ydEFzOiBzdHJpbmd8bnVsbDtcbiAgaW5wdXRzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgb3V0cHV0czoge1trZXk6IHN0cmluZ106IHN0cmluZ307XG4gIGhvc3RMaXN0ZW5lcnM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBob3N0UHJvcGVydGllczoge1trZXk6IHN0cmluZ106IHN0cmluZ307XG4gIGhvc3RBdHRyaWJ1dGVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgcHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdO1xuICB2aWV3UHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdO1xuICBxdWVyaWVzOiBDb21waWxlUXVlcnlNZXRhZGF0YVtdO1xuICBndWFyZHM6IHtba2V5OiBzdHJpbmddOiBhbnl9O1xuICB2aWV3UXVlcmllczogQ29tcGlsZVF1ZXJ5TWV0YWRhdGFbXTtcbiAgZW50cnlDb21wb25lbnRzOiBDb21waWxlRW50cnlDb21wb25lbnRNZXRhZGF0YVtdO1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5fG51bGw7XG4gIHRlbXBsYXRlOiBDb21waWxlVGVtcGxhdGVTdW1tYXJ5fG51bGw7XG4gIGNvbXBvbmVudFZpZXdUeXBlOiBTdGF0aWNTeW1ib2x8UHJveHlDbGFzc3xudWxsO1xuICByZW5kZXJlclR5cGU6IFN0YXRpY1N5bWJvbHxvYmplY3R8bnVsbDtcbiAgY29tcG9uZW50RmFjdG9yeTogU3RhdGljU3ltYm9sfG9iamVjdHxudWxsO1xufVxuXG4vKipcbiAqIE1ldGFkYXRhIHJlZ2FyZGluZyBjb21waWxhdGlvbiBvZiBhIGRpcmVjdGl2ZS5cbiAqL1xuZXhwb3J0IGNsYXNzIENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSB7XG4gIHN0YXRpYyBjcmVhdGUoe2lzSG9zdCwgdHlwZSwgaXNDb21wb25lbnQsIHNlbGVjdG9yLCBleHBvcnRBcywgY2hhbmdlRGV0ZWN0aW9uLCBpbnB1dHMsIG91dHB1dHMsXG4gICAgICAgICAgICAgICAgIGhvc3QsIHByb3ZpZGVycywgdmlld1Byb3ZpZGVycywgcXVlcmllcywgZ3VhcmRzLCB2aWV3UXVlcmllcywgZW50cnlDb21wb25lbnRzLFxuICAgICAgICAgICAgICAgICB0ZW1wbGF0ZSwgY29tcG9uZW50Vmlld1R5cGUsIHJlbmRlcmVyVHlwZSwgY29tcG9uZW50RmFjdG9yeX06IHtcbiAgICBpc0hvc3Q6IGJvb2xlYW4sXG4gICAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YSxcbiAgICBpc0NvbXBvbmVudDogYm9vbGVhbixcbiAgICBzZWxlY3Rvcjogc3RyaW5nfG51bGwsXG4gICAgZXhwb3J0QXM6IHN0cmluZ3xudWxsLFxuICAgIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3l8bnVsbCxcbiAgICBpbnB1dHM6IHN0cmluZ1tdLFxuICAgIG91dHB1dHM6IHN0cmluZ1tdLFxuICAgIGhvc3Q6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9LFxuICAgIHByb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSxcbiAgICB2aWV3UHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdLFxuICAgIHF1ZXJpZXM6IENvbXBpbGVRdWVyeU1ldGFkYXRhW10sXG4gICAgZ3VhcmRzOiB7W2tleTogc3RyaW5nXTogYW55fTtcbiAgICB2aWV3UXVlcmllczogQ29tcGlsZVF1ZXJ5TWV0YWRhdGFbXSxcbiAgICBlbnRyeUNvbXBvbmVudHM6IENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW10sXG4gICAgdGVtcGxhdGU6IENvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhLFxuICAgIGNvbXBvbmVudFZpZXdUeXBlOiBTdGF0aWNTeW1ib2x8UHJveHlDbGFzc3xudWxsLFxuICAgIHJlbmRlcmVyVHlwZTogU3RhdGljU3ltYm9sfG9iamVjdHxudWxsLFxuICAgIGNvbXBvbmVudEZhY3Rvcnk6IFN0YXRpY1N5bWJvbHxvYmplY3R8bnVsbCxcbiAgfSk6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSB7XG4gICAgY29uc3QgaG9zdExpc3RlbmVyczoge1trZXk6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgICBjb25zdCBob3N0UHJvcGVydGllczoge1trZXk6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgICBjb25zdCBob3N0QXR0cmlidXRlczoge1trZXk6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgICBpZiAoaG9zdCAhPSBudWxsKSB7XG4gICAgICBPYmplY3Qua2V5cyhob3N0KS5mb3JFYWNoKGtleSA9PiB7XG4gICAgICAgIGNvbnN0IHZhbHVlID0gaG9zdFtrZXldO1xuICAgICAgICBjb25zdCBtYXRjaGVzID0ga2V5Lm1hdGNoKEhPU1RfUkVHX0VYUCk7XG4gICAgICAgIGlmIChtYXRjaGVzID09PSBudWxsKSB7XG4gICAgICAgICAgaG9zdEF0dHJpYnV0ZXNba2V5XSA9IHZhbHVlO1xuICAgICAgICB9IGVsc2UgaWYgKG1hdGNoZXNbMV0gIT0gbnVsbCkge1xuICAgICAgICAgIGhvc3RQcm9wZXJ0aWVzW21hdGNoZXNbMV1dID0gdmFsdWU7XG4gICAgICAgIH0gZWxzZSBpZiAobWF0Y2hlc1syXSAhPSBudWxsKSB7XG4gICAgICAgICAgaG9zdExpc3RlbmVyc1ttYXRjaGVzWzJdXSA9IHZhbHVlO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gICAgY29uc3QgaW5wdXRzTWFwOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICAgIGlmIChpbnB1dHMgIT0gbnVsbCkge1xuICAgICAgaW5wdXRzLmZvckVhY2goKGJpbmRDb25maWc6IHN0cmluZykgPT4ge1xuICAgICAgICAvLyBjYW5vbmljYWwgc3ludGF4OiBgZGlyUHJvcDogZWxQcm9wYFxuICAgICAgICAvLyBpZiB0aGVyZSBpcyBubyBgOmAsIHVzZSBkaXJQcm9wID0gZWxQcm9wXG4gICAgICAgIGNvbnN0IHBhcnRzID0gc3BsaXRBdENvbG9uKGJpbmRDb25maWcsIFtiaW5kQ29uZmlnLCBiaW5kQ29uZmlnXSk7XG4gICAgICAgIGlucHV0c01hcFtwYXJ0c1swXV0gPSBwYXJ0c1sxXTtcbiAgICAgIH0pO1xuICAgIH1cbiAgICBjb25zdCBvdXRwdXRzTWFwOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICAgIGlmIChvdXRwdXRzICE9IG51bGwpIHtcbiAgICAgIG91dHB1dHMuZm9yRWFjaCgoYmluZENvbmZpZzogc3RyaW5nKSA9PiB7XG4gICAgICAgIC8vIGNhbm9uaWNhbCBzeW50YXg6IGBkaXJQcm9wOiBlbFByb3BgXG4gICAgICAgIC8vIGlmIHRoZXJlIGlzIG5vIGA6YCwgdXNlIGRpclByb3AgPSBlbFByb3BcbiAgICAgICAgY29uc3QgcGFydHMgPSBzcGxpdEF0Q29sb24oYmluZENvbmZpZywgW2JpbmRDb25maWcsIGJpbmRDb25maWddKTtcbiAgICAgICAgb3V0cHV0c01hcFtwYXJ0c1swXV0gPSBwYXJ0c1sxXTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJldHVybiBuZXcgQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhKHtcbiAgICAgIGlzSG9zdCxcbiAgICAgIHR5cGUsXG4gICAgICBpc0NvbXBvbmVudDogISFpc0NvbXBvbmVudCwgc2VsZWN0b3IsIGV4cG9ydEFzLCBjaGFuZ2VEZXRlY3Rpb24sXG4gICAgICBpbnB1dHM6IGlucHV0c01hcCxcbiAgICAgIG91dHB1dHM6IG91dHB1dHNNYXAsXG4gICAgICBob3N0TGlzdGVuZXJzLFxuICAgICAgaG9zdFByb3BlcnRpZXMsXG4gICAgICBob3N0QXR0cmlidXRlcyxcbiAgICAgIHByb3ZpZGVycyxcbiAgICAgIHZpZXdQcm92aWRlcnMsXG4gICAgICBxdWVyaWVzLFxuICAgICAgZ3VhcmRzLFxuICAgICAgdmlld1F1ZXJpZXMsXG4gICAgICBlbnRyeUNvbXBvbmVudHMsXG4gICAgICB0ZW1wbGF0ZSxcbiAgICAgIGNvbXBvbmVudFZpZXdUeXBlLFxuICAgICAgcmVuZGVyZXJUeXBlLFxuICAgICAgY29tcG9uZW50RmFjdG9yeSxcbiAgICB9KTtcbiAgfVxuICBpc0hvc3Q6IGJvb2xlYW47XG4gIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGE7XG4gIGlzQ29tcG9uZW50OiBib29sZWFuO1xuICBzZWxlY3Rvcjogc3RyaW5nfG51bGw7XG4gIGV4cG9ydEFzOiBzdHJpbmd8bnVsbDtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneXxudWxsO1xuICBpbnB1dHM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBvdXRwdXRzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgaG9zdExpc3RlbmVyczoge1trZXk6IHN0cmluZ106IHN0cmluZ307XG4gIGhvc3RQcm9wZXJ0aWVzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfTtcbiAgaG9zdEF0dHJpYnV0ZXM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuICBwcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW107XG4gIHZpZXdQcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW107XG4gIHF1ZXJpZXM6IENvbXBpbGVRdWVyeU1ldGFkYXRhW107XG4gIGd1YXJkczoge1trZXk6IHN0cmluZ106IGFueX07XG4gIHZpZXdRdWVyaWVzOiBDb21waWxlUXVlcnlNZXRhZGF0YVtdO1xuICBlbnRyeUNvbXBvbmVudHM6IENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW107XG5cbiAgdGVtcGxhdGU6IENvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhfG51bGw7XG5cbiAgY29tcG9uZW50Vmlld1R5cGU6IFN0YXRpY1N5bWJvbHxQcm94eUNsYXNzfG51bGw7XG4gIHJlbmRlcmVyVHlwZTogU3RhdGljU3ltYm9sfG9iamVjdHxudWxsO1xuICBjb21wb25lbnRGYWN0b3J5OiBTdGF0aWNTeW1ib2x8b2JqZWN0fG51bGw7XG5cbiAgY29uc3RydWN0b3Ioe2lzSG9zdCxcbiAgICAgICAgICAgICAgIHR5cGUsXG4gICAgICAgICAgICAgICBpc0NvbXBvbmVudCxcbiAgICAgICAgICAgICAgIHNlbGVjdG9yLFxuICAgICAgICAgICAgICAgZXhwb3J0QXMsXG4gICAgICAgICAgICAgICBjaGFuZ2VEZXRlY3Rpb24sXG4gICAgICAgICAgICAgICBpbnB1dHMsXG4gICAgICAgICAgICAgICBvdXRwdXRzLFxuICAgICAgICAgICAgICAgaG9zdExpc3RlbmVycyxcbiAgICAgICAgICAgICAgIGhvc3RQcm9wZXJ0aWVzLFxuICAgICAgICAgICAgICAgaG9zdEF0dHJpYnV0ZXMsXG4gICAgICAgICAgICAgICBwcm92aWRlcnMsXG4gICAgICAgICAgICAgICB2aWV3UHJvdmlkZXJzLFxuICAgICAgICAgICAgICAgcXVlcmllcyxcbiAgICAgICAgICAgICAgIGd1YXJkcyxcbiAgICAgICAgICAgICAgIHZpZXdRdWVyaWVzLFxuICAgICAgICAgICAgICAgZW50cnlDb21wb25lbnRzLFxuICAgICAgICAgICAgICAgdGVtcGxhdGUsXG4gICAgICAgICAgICAgICBjb21wb25lbnRWaWV3VHlwZSxcbiAgICAgICAgICAgICAgIHJlbmRlcmVyVHlwZSxcbiAgICAgICAgICAgICAgIGNvbXBvbmVudEZhY3Rvcnl9OiB7XG4gICAgaXNIb3N0OiBib29sZWFuLFxuICAgIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGEsXG4gICAgaXNDb21wb25lbnQ6IGJvb2xlYW4sXG4gICAgc2VsZWN0b3I6IHN0cmluZ3xudWxsLFxuICAgIGV4cG9ydEFzOiBzdHJpbmd8bnVsbCxcbiAgICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5fG51bGwsXG4gICAgaW5wdXRzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSxcbiAgICBvdXRwdXRzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSxcbiAgICBob3N0TGlzdGVuZXJzOiB7W2tleTogc3RyaW5nXTogc3RyaW5nfSxcbiAgICBob3N0UHJvcGVydGllczoge1trZXk6IHN0cmluZ106IHN0cmluZ30sXG4gICAgaG9zdEF0dHJpYnV0ZXM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9LFxuICAgIHByb3ZpZGVyczogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGFbXSxcbiAgICB2aWV3UHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdLFxuICAgIHF1ZXJpZXM6IENvbXBpbGVRdWVyeU1ldGFkYXRhW10sXG4gICAgZ3VhcmRzOiB7W2tleTogc3RyaW5nXTogYW55fSxcbiAgICB2aWV3UXVlcmllczogQ29tcGlsZVF1ZXJ5TWV0YWRhdGFbXSxcbiAgICBlbnRyeUNvbXBvbmVudHM6IENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW10sXG4gICAgdGVtcGxhdGU6IENvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhfG51bGwsXG4gICAgY29tcG9uZW50Vmlld1R5cGU6IFN0YXRpY1N5bWJvbHxQcm94eUNsYXNzfG51bGwsXG4gICAgcmVuZGVyZXJUeXBlOiBTdGF0aWNTeW1ib2x8b2JqZWN0fG51bGwsXG4gICAgY29tcG9uZW50RmFjdG9yeTogU3RhdGljU3ltYm9sfG9iamVjdHxudWxsLFxuICB9KSB7XG4gICAgdGhpcy5pc0hvc3QgPSAhIWlzSG9zdDtcbiAgICB0aGlzLnR5cGUgPSB0eXBlO1xuICAgIHRoaXMuaXNDb21wb25lbnQgPSBpc0NvbXBvbmVudDtcbiAgICB0aGlzLnNlbGVjdG9yID0gc2VsZWN0b3I7XG4gICAgdGhpcy5leHBvcnRBcyA9IGV4cG9ydEFzO1xuICAgIHRoaXMuY2hhbmdlRGV0ZWN0aW9uID0gY2hhbmdlRGV0ZWN0aW9uO1xuICAgIHRoaXMuaW5wdXRzID0gaW5wdXRzO1xuICAgIHRoaXMub3V0cHV0cyA9IG91dHB1dHM7XG4gICAgdGhpcy5ob3N0TGlzdGVuZXJzID0gaG9zdExpc3RlbmVycztcbiAgICB0aGlzLmhvc3RQcm9wZXJ0aWVzID0gaG9zdFByb3BlcnRpZXM7XG4gICAgdGhpcy5ob3N0QXR0cmlidXRlcyA9IGhvc3RBdHRyaWJ1dGVzO1xuICAgIHRoaXMucHJvdmlkZXJzID0gX25vcm1hbGl6ZUFycmF5KHByb3ZpZGVycyk7XG4gICAgdGhpcy52aWV3UHJvdmlkZXJzID0gX25vcm1hbGl6ZUFycmF5KHZpZXdQcm92aWRlcnMpO1xuICAgIHRoaXMucXVlcmllcyA9IF9ub3JtYWxpemVBcnJheShxdWVyaWVzKTtcbiAgICB0aGlzLmd1YXJkcyA9IGd1YXJkcztcbiAgICB0aGlzLnZpZXdRdWVyaWVzID0gX25vcm1hbGl6ZUFycmF5KHZpZXdRdWVyaWVzKTtcbiAgICB0aGlzLmVudHJ5Q29tcG9uZW50cyA9IF9ub3JtYWxpemVBcnJheShlbnRyeUNvbXBvbmVudHMpO1xuICAgIHRoaXMudGVtcGxhdGUgPSB0ZW1wbGF0ZTtcblxuICAgIHRoaXMuY29tcG9uZW50Vmlld1R5cGUgPSBjb21wb25lbnRWaWV3VHlwZTtcbiAgICB0aGlzLnJlbmRlcmVyVHlwZSA9IHJlbmRlcmVyVHlwZTtcbiAgICB0aGlzLmNvbXBvbmVudEZhY3RvcnkgPSBjb21wb25lbnRGYWN0b3J5O1xuICB9XG5cbiAgdG9TdW1tYXJ5KCk6IENvbXBpbGVEaXJlY3RpdmVTdW1tYXJ5IHtcbiAgICByZXR1cm4ge1xuICAgICAgc3VtbWFyeUtpbmQ6IENvbXBpbGVTdW1tYXJ5S2luZC5EaXJlY3RpdmUsXG4gICAgICB0eXBlOiB0aGlzLnR5cGUsXG4gICAgICBpc0NvbXBvbmVudDogdGhpcy5pc0NvbXBvbmVudCxcbiAgICAgIHNlbGVjdG9yOiB0aGlzLnNlbGVjdG9yLFxuICAgICAgZXhwb3J0QXM6IHRoaXMuZXhwb3J0QXMsXG4gICAgICBpbnB1dHM6IHRoaXMuaW5wdXRzLFxuICAgICAgb3V0cHV0czogdGhpcy5vdXRwdXRzLFxuICAgICAgaG9zdExpc3RlbmVyczogdGhpcy5ob3N0TGlzdGVuZXJzLFxuICAgICAgaG9zdFByb3BlcnRpZXM6IHRoaXMuaG9zdFByb3BlcnRpZXMsXG4gICAgICBob3N0QXR0cmlidXRlczogdGhpcy5ob3N0QXR0cmlidXRlcyxcbiAgICAgIHByb3ZpZGVyczogdGhpcy5wcm92aWRlcnMsXG4gICAgICB2aWV3UHJvdmlkZXJzOiB0aGlzLnZpZXdQcm92aWRlcnMsXG4gICAgICBxdWVyaWVzOiB0aGlzLnF1ZXJpZXMsXG4gICAgICBndWFyZHM6IHRoaXMuZ3VhcmRzLFxuICAgICAgdmlld1F1ZXJpZXM6IHRoaXMudmlld1F1ZXJpZXMsXG4gICAgICBlbnRyeUNvbXBvbmVudHM6IHRoaXMuZW50cnlDb21wb25lbnRzLFxuICAgICAgY2hhbmdlRGV0ZWN0aW9uOiB0aGlzLmNoYW5nZURldGVjdGlvbixcbiAgICAgIHRlbXBsYXRlOiB0aGlzLnRlbXBsYXRlICYmIHRoaXMudGVtcGxhdGUudG9TdW1tYXJ5KCksXG4gICAgICBjb21wb25lbnRWaWV3VHlwZTogdGhpcy5jb21wb25lbnRWaWV3VHlwZSxcbiAgICAgIHJlbmRlcmVyVHlwZTogdGhpcy5yZW5kZXJlclR5cGUsXG4gICAgICBjb21wb25lbnRGYWN0b3J5OiB0aGlzLmNvbXBvbmVudEZhY3RvcnlcbiAgICB9O1xuICB9XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ29tcGlsZVBpcGVTdW1tYXJ5IGV4dGVuZHMgQ29tcGlsZVR5cGVTdW1tYXJ5IHtcbiAgdHlwZTogQ29tcGlsZVR5cGVNZXRhZGF0YTtcbiAgbmFtZTogc3RyaW5nO1xuICBwdXJlOiBib29sZWFuO1xufVxuXG5leHBvcnQgY2xhc3MgQ29tcGlsZVBpcGVNZXRhZGF0YSB7XG4gIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGE7XG4gIG5hbWU6IHN0cmluZztcbiAgcHVyZTogYm9vbGVhbjtcblxuICBjb25zdHJ1Y3Rvcih7dHlwZSwgbmFtZSwgcHVyZX06IHtcbiAgICB0eXBlOiBDb21waWxlVHlwZU1ldGFkYXRhLFxuICAgIG5hbWU6IHN0cmluZyxcbiAgICBwdXJlOiBib29sZWFuLFxuICB9KSB7XG4gICAgdGhpcy50eXBlID0gdHlwZTtcbiAgICB0aGlzLm5hbWUgPSBuYW1lO1xuICAgIHRoaXMucHVyZSA9ICEhcHVyZTtcbiAgfVxuXG4gIHRvU3VtbWFyeSgpOiBDb21waWxlUGlwZVN1bW1hcnkge1xuICAgIHJldHVybiB7XG4gICAgICBzdW1tYXJ5S2luZDogQ29tcGlsZVN1bW1hcnlLaW5kLlBpcGUsXG4gICAgICB0eXBlOiB0aGlzLnR5cGUsXG4gICAgICBuYW1lOiB0aGlzLm5hbWUsXG4gICAgICBwdXJlOiB0aGlzLnB1cmVcbiAgICB9O1xuICB9XG59XG5cbi8vIE5vdGU6IFRoaXMgc2hvdWxkIG9ubHkgdXNlIGludGVyZmFjZXMgYXMgbmVzdGVkIGRhdGEgdHlwZXNcbi8vIGFzIHdlIG5lZWQgdG8gYmUgYWJsZSB0byBzZXJpYWxpemUgdGhpcyBmcm9tL3RvIEpTT04hXG5leHBvcnQgaW50ZXJmYWNlIENvbXBpbGVOZ01vZHVsZVN1bW1hcnkgZXh0ZW5kcyBDb21waWxlVHlwZVN1bW1hcnkge1xuICB0eXBlOiBDb21waWxlVHlwZU1ldGFkYXRhO1xuXG4gIC8vIE5vdGU6IFRoaXMgaXMgdHJhbnNpdGl2ZSBvdmVyIHRoZSBleHBvcnRlZCBtb2R1bGVzLlxuICBleHBvcnRlZERpcmVjdGl2ZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXTtcbiAgLy8gTm90ZTogVGhpcyBpcyB0cmFuc2l0aXZlIG92ZXIgdGhlIGV4cG9ydGVkIG1vZHVsZXMuXG4gIGV4cG9ydGVkUGlwZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXTtcblxuICAvLyBOb3RlOiBUaGlzIGlzIHRyYW5zaXRpdmUuXG4gIGVudHJ5Q29tcG9uZW50czogQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXTtcbiAgLy8gTm90ZTogVGhpcyBpcyB0cmFuc2l0aXZlLlxuICBwcm92aWRlcnM6IHtwcm92aWRlcjogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGEsIG1vZHVsZTogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YX1bXTtcbiAgLy8gTm90ZTogVGhpcyBpcyB0cmFuc2l0aXZlLlxuICBtb2R1bGVzOiBDb21waWxlVHlwZU1ldGFkYXRhW107XG59XG5cbmV4cG9ydCBjbGFzcyBDb21waWxlU2hhbGxvd01vZHVsZU1ldGFkYXRhIHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHR5cGUgITogQ29tcGlsZVR5cGVNZXRhZGF0YTtcblxuICByYXdFeHBvcnRzOiBhbnk7XG4gIHJhd0ltcG9ydHM6IGFueTtcbiAgcmF3UHJvdmlkZXJzOiBhbnk7XG59XG5cbi8qKlxuICogTWV0YWRhdGEgcmVnYXJkaW5nIGNvbXBpbGF0aW9uIG9mIGEgbW9kdWxlLlxuICovXG5leHBvcnQgY2xhc3MgQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEge1xuICB0eXBlOiBDb21waWxlVHlwZU1ldGFkYXRhO1xuICBkZWNsYXJlZERpcmVjdGl2ZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXTtcbiAgZXhwb3J0ZWREaXJlY3RpdmVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW107XG4gIGRlY2xhcmVkUGlwZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXTtcblxuICBleHBvcnRlZFBpcGVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW107XG4gIGVudHJ5Q29tcG9uZW50czogQ29tcGlsZUVudHJ5Q29tcG9uZW50TWV0YWRhdGFbXTtcbiAgYm9vdHN0cmFwQ29tcG9uZW50czogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdO1xuICBwcm92aWRlcnM6IENvbXBpbGVQcm92aWRlck1ldGFkYXRhW107XG5cbiAgaW1wb3J0ZWRNb2R1bGVzOiBDb21waWxlTmdNb2R1bGVTdW1tYXJ5W107XG4gIGV4cG9ydGVkTW9kdWxlczogQ29tcGlsZU5nTW9kdWxlU3VtbWFyeVtdO1xuICBzY2hlbWFzOiBTY2hlbWFNZXRhZGF0YVtdO1xuICBpZDogc3RyaW5nfG51bGw7XG5cbiAgdHJhbnNpdGl2ZU1vZHVsZTogVHJhbnNpdGl2ZUNvbXBpbGVOZ01vZHVsZU1ldGFkYXRhO1xuXG4gIGNvbnN0cnVjdG9yKHt0eXBlLCBwcm92aWRlcnMsIGRlY2xhcmVkRGlyZWN0aXZlcywgZXhwb3J0ZWREaXJlY3RpdmVzLCBkZWNsYXJlZFBpcGVzLFxuICAgICAgICAgICAgICAgZXhwb3J0ZWRQaXBlcywgZW50cnlDb21wb25lbnRzLCBib290c3RyYXBDb21wb25lbnRzLCBpbXBvcnRlZE1vZHVsZXMsXG4gICAgICAgICAgICAgICBleHBvcnRlZE1vZHVsZXMsIHNjaGVtYXMsIHRyYW5zaXRpdmVNb2R1bGUsIGlkfToge1xuICAgIHR5cGU6IENvbXBpbGVUeXBlTWV0YWRhdGEsXG4gICAgcHJvdmlkZXJzOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YVtdLFxuICAgIGRlY2xhcmVkRGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdLFxuICAgIGV4cG9ydGVkRGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdLFxuICAgIGRlY2xhcmVkUGlwZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXSxcbiAgICBleHBvcnRlZFBpcGVzOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhW10sXG4gICAgZW50cnlDb21wb25lbnRzOiBDb21waWxlRW50cnlDb21wb25lbnRNZXRhZGF0YVtdLFxuICAgIGJvb3RzdHJhcENvbXBvbmVudHM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXSxcbiAgICBpbXBvcnRlZE1vZHVsZXM6IENvbXBpbGVOZ01vZHVsZVN1bW1hcnlbXSxcbiAgICBleHBvcnRlZE1vZHVsZXM6IENvbXBpbGVOZ01vZHVsZVN1bW1hcnlbXSxcbiAgICB0cmFuc2l0aXZlTW9kdWxlOiBUcmFuc2l0aXZlQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsXG4gICAgc2NoZW1hczogU2NoZW1hTWV0YWRhdGFbXSxcbiAgICBpZDogc3RyaW5nfG51bGxcbiAgfSkge1xuICAgIHRoaXMudHlwZSA9IHR5cGUgfHwgbnVsbDtcbiAgICB0aGlzLmRlY2xhcmVkRGlyZWN0aXZlcyA9IF9ub3JtYWxpemVBcnJheShkZWNsYXJlZERpcmVjdGl2ZXMpO1xuICAgIHRoaXMuZXhwb3J0ZWREaXJlY3RpdmVzID0gX25vcm1hbGl6ZUFycmF5KGV4cG9ydGVkRGlyZWN0aXZlcyk7XG4gICAgdGhpcy5kZWNsYXJlZFBpcGVzID0gX25vcm1hbGl6ZUFycmF5KGRlY2xhcmVkUGlwZXMpO1xuICAgIHRoaXMuZXhwb3J0ZWRQaXBlcyA9IF9ub3JtYWxpemVBcnJheShleHBvcnRlZFBpcGVzKTtcbiAgICB0aGlzLnByb3ZpZGVycyA9IF9ub3JtYWxpemVBcnJheShwcm92aWRlcnMpO1xuICAgIHRoaXMuZW50cnlDb21wb25lbnRzID0gX25vcm1hbGl6ZUFycmF5KGVudHJ5Q29tcG9uZW50cyk7XG4gICAgdGhpcy5ib290c3RyYXBDb21wb25lbnRzID0gX25vcm1hbGl6ZUFycmF5KGJvb3RzdHJhcENvbXBvbmVudHMpO1xuICAgIHRoaXMuaW1wb3J0ZWRNb2R1bGVzID0gX25vcm1hbGl6ZUFycmF5KGltcG9ydGVkTW9kdWxlcyk7XG4gICAgdGhpcy5leHBvcnRlZE1vZHVsZXMgPSBfbm9ybWFsaXplQXJyYXkoZXhwb3J0ZWRNb2R1bGVzKTtcbiAgICB0aGlzLnNjaGVtYXMgPSBfbm9ybWFsaXplQXJyYXkoc2NoZW1hcyk7XG4gICAgdGhpcy5pZCA9IGlkIHx8IG51bGw7XG4gICAgdGhpcy50cmFuc2l0aXZlTW9kdWxlID0gdHJhbnNpdGl2ZU1vZHVsZSB8fCBudWxsO1xuICB9XG5cbiAgdG9TdW1tYXJ5KCk6IENvbXBpbGVOZ01vZHVsZVN1bW1hcnkge1xuICAgIGNvbnN0IG1vZHVsZSA9IHRoaXMudHJhbnNpdGl2ZU1vZHVsZSAhO1xuICAgIHJldHVybiB7XG4gICAgICBzdW1tYXJ5S2luZDogQ29tcGlsZVN1bW1hcnlLaW5kLk5nTW9kdWxlLFxuICAgICAgdHlwZTogdGhpcy50eXBlLFxuICAgICAgZW50cnlDb21wb25lbnRzOiBtb2R1bGUuZW50cnlDb21wb25lbnRzLFxuICAgICAgcHJvdmlkZXJzOiBtb2R1bGUucHJvdmlkZXJzLFxuICAgICAgbW9kdWxlczogbW9kdWxlLm1vZHVsZXMsXG4gICAgICBleHBvcnRlZERpcmVjdGl2ZXM6IG1vZHVsZS5leHBvcnRlZERpcmVjdGl2ZXMsXG4gICAgICBleHBvcnRlZFBpcGVzOiBtb2R1bGUuZXhwb3J0ZWRQaXBlc1xuICAgIH07XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIFRyYW5zaXRpdmVDb21waWxlTmdNb2R1bGVNZXRhZGF0YSB7XG4gIGRpcmVjdGl2ZXNTZXQgPSBuZXcgU2V0PGFueT4oKTtcbiAgZGlyZWN0aXZlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdID0gW107XG4gIGV4cG9ydGVkRGlyZWN0aXZlc1NldCA9IG5ldyBTZXQ8YW55PigpO1xuICBleHBvcnRlZERpcmVjdGl2ZXM6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGFbXSA9IFtdO1xuICBwaXBlc1NldCA9IG5ldyBTZXQ8YW55PigpO1xuICBwaXBlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdID0gW107XG4gIGV4cG9ydGVkUGlwZXNTZXQgPSBuZXcgU2V0PGFueT4oKTtcbiAgZXhwb3J0ZWRQaXBlczogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YVtdID0gW107XG4gIG1vZHVsZXNTZXQgPSBuZXcgU2V0PGFueT4oKTtcbiAgbW9kdWxlczogQ29tcGlsZVR5cGVNZXRhZGF0YVtdID0gW107XG4gIGVudHJ5Q29tcG9uZW50c1NldCA9IG5ldyBTZXQ8YW55PigpO1xuICBlbnRyeUNvbXBvbmVudHM6IENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhW10gPSBbXTtcblxuICBwcm92aWRlcnM6IHtwcm92aWRlcjogQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGEsIG1vZHVsZTogQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YX1bXSA9IFtdO1xuXG4gIGFkZFByb3ZpZGVyKHByb3ZpZGVyOiBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSwgbW9kdWxlOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhKSB7XG4gICAgdGhpcy5wcm92aWRlcnMucHVzaCh7cHJvdmlkZXI6IHByb3ZpZGVyLCBtb2R1bGU6IG1vZHVsZX0pO1xuICB9XG5cbiAgYWRkRGlyZWN0aXZlKGlkOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhKSB7XG4gICAgaWYgKCF0aGlzLmRpcmVjdGl2ZXNTZXQuaGFzKGlkLnJlZmVyZW5jZSkpIHtcbiAgICAgIHRoaXMuZGlyZWN0aXZlc1NldC5hZGQoaWQucmVmZXJlbmNlKTtcbiAgICAgIHRoaXMuZGlyZWN0aXZlcy5wdXNoKGlkKTtcbiAgICB9XG4gIH1cbiAgYWRkRXhwb3J0ZWREaXJlY3RpdmUoaWQ6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEpIHtcbiAgICBpZiAoIXRoaXMuZXhwb3J0ZWREaXJlY3RpdmVzU2V0LmhhcyhpZC5yZWZlcmVuY2UpKSB7XG4gICAgICB0aGlzLmV4cG9ydGVkRGlyZWN0aXZlc1NldC5hZGQoaWQucmVmZXJlbmNlKTtcbiAgICAgIHRoaXMuZXhwb3J0ZWREaXJlY3RpdmVzLnB1c2goaWQpO1xuICAgIH1cbiAgfVxuICBhZGRQaXBlKGlkOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhKSB7XG4gICAgaWYgKCF0aGlzLnBpcGVzU2V0LmhhcyhpZC5yZWZlcmVuY2UpKSB7XG4gICAgICB0aGlzLnBpcGVzU2V0LmFkZChpZC5yZWZlcmVuY2UpO1xuICAgICAgdGhpcy5waXBlcy5wdXNoKGlkKTtcbiAgICB9XG4gIH1cbiAgYWRkRXhwb3J0ZWRQaXBlKGlkOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhKSB7XG4gICAgaWYgKCF0aGlzLmV4cG9ydGVkUGlwZXNTZXQuaGFzKGlkLnJlZmVyZW5jZSkpIHtcbiAgICAgIHRoaXMuZXhwb3J0ZWRQaXBlc1NldC5hZGQoaWQucmVmZXJlbmNlKTtcbiAgICAgIHRoaXMuZXhwb3J0ZWRQaXBlcy5wdXNoKGlkKTtcbiAgICB9XG4gIH1cbiAgYWRkTW9kdWxlKGlkOiBDb21waWxlVHlwZU1ldGFkYXRhKSB7XG4gICAgaWYgKCF0aGlzLm1vZHVsZXNTZXQuaGFzKGlkLnJlZmVyZW5jZSkpIHtcbiAgICAgIHRoaXMubW9kdWxlc1NldC5hZGQoaWQucmVmZXJlbmNlKTtcbiAgICAgIHRoaXMubW9kdWxlcy5wdXNoKGlkKTtcbiAgICB9XG4gIH1cbiAgYWRkRW50cnlDb21wb25lbnQoZWM6IENvbXBpbGVFbnRyeUNvbXBvbmVudE1ldGFkYXRhKSB7XG4gICAgaWYgKCF0aGlzLmVudHJ5Q29tcG9uZW50c1NldC5oYXMoZWMuY29tcG9uZW50VHlwZSkpIHtcbiAgICAgIHRoaXMuZW50cnlDb21wb25lbnRzU2V0LmFkZChlYy5jb21wb25lbnRUeXBlKTtcbiAgICAgIHRoaXMuZW50cnlDb21wb25lbnRzLnB1c2goZWMpO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBfbm9ybWFsaXplQXJyYXkob2JqOiBhbnlbXSB8IHVuZGVmaW5lZCB8IG51bGwpOiBhbnlbXSB7XG4gIHJldHVybiBvYmogfHwgW107XG59XG5cbmV4cG9ydCBjbGFzcyBQcm92aWRlck1ldGEge1xuICB0b2tlbjogYW55O1xuICB1c2VDbGFzczogVHlwZXxudWxsO1xuICB1c2VWYWx1ZTogYW55O1xuICB1c2VFeGlzdGluZzogYW55O1xuICB1c2VGYWN0b3J5OiBGdW5jdGlvbnxudWxsO1xuICBkZXBlbmRlbmNpZXM6IE9iamVjdFtdfG51bGw7XG4gIG11bHRpOiBib29sZWFuO1xuXG4gIGNvbnN0cnVjdG9yKHRva2VuOiBhbnksIHt1c2VDbGFzcywgdXNlVmFsdWUsIHVzZUV4aXN0aW5nLCB1c2VGYWN0b3J5LCBkZXBzLCBtdWx0aX06IHtcbiAgICB1c2VDbGFzcz86IFR5cGUsXG4gICAgdXNlVmFsdWU/OiBhbnksXG4gICAgdXNlRXhpc3Rpbmc/OiBhbnksXG4gICAgdXNlRmFjdG9yeT86IEZ1bmN0aW9ufG51bGwsXG4gICAgZGVwcz86IE9iamVjdFtdfG51bGwsXG4gICAgbXVsdGk/OiBib29sZWFuXG4gIH0pIHtcbiAgICB0aGlzLnRva2VuID0gdG9rZW47XG4gICAgdGhpcy51c2VDbGFzcyA9IHVzZUNsYXNzIHx8IG51bGw7XG4gICAgdGhpcy51c2VWYWx1ZSA9IHVzZVZhbHVlO1xuICAgIHRoaXMudXNlRXhpc3RpbmcgPSB1c2VFeGlzdGluZztcbiAgICB0aGlzLnVzZUZhY3RvcnkgPSB1c2VGYWN0b3J5IHx8IG51bGw7XG4gICAgdGhpcy5kZXBlbmRlbmNpZXMgPSBkZXBzIHx8IG51bGw7XG4gICAgdGhpcy5tdWx0aSA9ICEhbXVsdGk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZsYXR0ZW48VD4obGlzdDogQXJyYXk8VHxUW10+KTogVFtdIHtcbiAgcmV0dXJuIGxpc3QucmVkdWNlKChmbGF0OiBhbnlbXSwgaXRlbTogVCB8IFRbXSk6IFRbXSA9PiB7XG4gICAgY29uc3QgZmxhdEl0ZW0gPSBBcnJheS5pc0FycmF5KGl0ZW0pID8gZmxhdHRlbihpdGVtKSA6IGl0ZW07XG4gICAgcmV0dXJuICg8VFtdPmZsYXQpLmNvbmNhdChmbGF0SXRlbSk7XG4gIH0sIFtdKTtcbn1cblxuZnVuY3Rpb24gaml0U291cmNlVXJsKHVybDogc3RyaW5nKSB7XG4gIC8vIE5vdGU6IFdlIG5lZWQgMyBcIi9cIiBzbyB0aGF0IG5nIHNob3dzIHVwIGFzIGEgc2VwYXJhdGUgZG9tYWluXG4gIC8vIGluIHRoZSBjaHJvbWUgZGV2IHRvb2xzLlxuICByZXR1cm4gdXJsLnJlcGxhY2UoLyhcXHcrOlxcL1xcL1tcXHc6LV0rKT8oXFwvKyk/LywgJ25nOi8vLycpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdGVtcGxhdGVTb3VyY2VVcmwoXG4gICAgbmdNb2R1bGVUeXBlOiBDb21waWxlSWRlbnRpZmllck1ldGFkYXRhLCBjb21wTWV0YToge3R5cGU6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGF9LFxuICAgIHRlbXBsYXRlTWV0YToge2lzSW5saW5lOiBib29sZWFuLCB0ZW1wbGF0ZVVybDogc3RyaW5nIHwgbnVsbH0pIHtcbiAgbGV0IHVybDogc3RyaW5nO1xuICBpZiAodGVtcGxhdGVNZXRhLmlzSW5saW5lKSB7XG4gICAgaWYgKGNvbXBNZXRhLnR5cGUucmVmZXJlbmNlIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgICAvLyBOb3RlOiBhIC50cyBmaWxlIG1pZ2h0IGNvbnRhaW4gbXVsdGlwbGUgY29tcG9uZW50cyB3aXRoIGlubGluZSB0ZW1wbGF0ZXMsXG4gICAgICAvLyBzbyB3ZSBuZWVkIHRvIGdpdmUgdGhlbSB1bmlxdWUgdXJscywgYXMgdGhlc2Ugd2lsbCBiZSB1c2VkIGZvciBzb3VyY2VtYXBzLlxuICAgICAgdXJsID0gYCR7Y29tcE1ldGEudHlwZS5yZWZlcmVuY2UuZmlsZVBhdGh9LiR7Y29tcE1ldGEudHlwZS5yZWZlcmVuY2UubmFtZX0uaHRtbGA7XG4gICAgfSBlbHNlIHtcbiAgICAgIHVybCA9IGAke2lkZW50aWZpZXJOYW1lKG5nTW9kdWxlVHlwZSl9LyR7aWRlbnRpZmllck5hbWUoY29tcE1ldGEudHlwZSl9Lmh0bWxgO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICB1cmwgPSB0ZW1wbGF0ZU1ldGEudGVtcGxhdGVVcmwgITtcbiAgfVxuICByZXR1cm4gY29tcE1ldGEudHlwZS5yZWZlcmVuY2UgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wgPyB1cmwgOiBqaXRTb3VyY2VVcmwodXJsKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNoYXJlZFN0eWxlc2hlZXRKaXRVcmwobWV0YTogQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YSwgaWQ6IG51bWJlcikge1xuICBjb25zdCBwYXRoUGFydHMgPSBtZXRhLm1vZHVsZVVybCAhLnNwbGl0KC9cXC9cXFxcL2cpO1xuICBjb25zdCBiYXNlTmFtZSA9IHBhdGhQYXJ0c1twYXRoUGFydHMubGVuZ3RoIC0gMV07XG4gIHJldHVybiBqaXRTb3VyY2VVcmwoYGNzcy8ke2lkfSR7YmFzZU5hbWV9Lm5nc3R5bGUuanNgKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG5nTW9kdWxlSml0VXJsKG1vZHVsZU1ldGE6IENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhKTogc3RyaW5nIHtcbiAgcmV0dXJuIGppdFNvdXJjZVVybChgJHtpZGVudGlmaWVyTmFtZShtb2R1bGVNZXRhLnR5cGUpfS9tb2R1bGUubmdmYWN0b3J5LmpzYCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0ZW1wbGF0ZUppdFVybChcbiAgICBuZ01vZHVsZVR5cGU6IENvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGEsIGNvbXBNZXRhOiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEpOiBzdHJpbmcge1xuICByZXR1cm4gaml0U291cmNlVXJsKFxuICAgICAgYCR7aWRlbnRpZmllck5hbWUobmdNb2R1bGVUeXBlKX0vJHtpZGVudGlmaWVyTmFtZShjb21wTWV0YS50eXBlKX0ubmdmYWN0b3J5LmpzYCk7XG59XG4iXX0=