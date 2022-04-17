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
        define("@angular/compiler/src/aot/static_reflector", ["require", "exports", "tslib", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/core", "@angular/compiler/src/util", "@angular/compiler/src/aot/formatted_error", "@angular/compiler/src/aot/static_symbol"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var core_1 = require("@angular/compiler/src/core");
    var util_1 = require("@angular/compiler/src/util");
    var formatted_error_1 = require("@angular/compiler/src/aot/formatted_error");
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var ANGULAR_CORE = '@angular/core';
    var ANGULAR_ROUTER = '@angular/router';
    var HIDDEN_KEY = /^\$.*\$$/;
    var IGNORE = {
        __symbolic: 'ignore'
    };
    var USE_VALUE = 'useValue';
    var PROVIDE = 'provide';
    var REFERENCE_SET = new Set([USE_VALUE, 'useFactory', 'data', 'id', 'loadChildren']);
    var TYPEGUARD_POSTFIX = 'TypeGuard';
    var USE_IF = 'UseIf';
    function shouldIgnore(value) {
        return value && value.__symbolic == 'ignore';
    }
    /**
     * A static reflector implements enough of the Reflector API that is necessary to compile
     * templates statically.
     */
    var StaticReflector = /** @class */ (function () {
        function StaticReflector(summaryResolver, symbolResolver, knownMetadataClasses, knownMetadataFunctions, errorRecorder) {
            var _this = this;
            if (knownMetadataClasses === void 0) { knownMetadataClasses = []; }
            if (knownMetadataFunctions === void 0) { knownMetadataFunctions = []; }
            this.summaryResolver = summaryResolver;
            this.symbolResolver = symbolResolver;
            this.errorRecorder = errorRecorder;
            this.annotationCache = new Map();
            this.shallowAnnotationCache = new Map();
            this.propertyCache = new Map();
            this.parameterCache = new Map();
            this.methodCache = new Map();
            this.staticCache = new Map();
            this.conversionMap = new Map();
            this.resolvedExternalReferences = new Map();
            this.annotationForParentClassWithSummaryKind = new Map();
            this.initializeConversionMap();
            knownMetadataClasses.forEach(function (kc) { return _this._registerDecoratorOrConstructor(_this.getStaticSymbol(kc.filePath, kc.name), kc.ctor); });
            knownMetadataFunctions.forEach(function (kf) { return _this._registerFunction(_this.getStaticSymbol(kf.filePath, kf.name), kf.fn); });
            this.annotationForParentClassWithSummaryKind.set(compile_metadata_1.CompileSummaryKind.Directive, [core_1.createDirective, core_1.createComponent]);
            this.annotationForParentClassWithSummaryKind.set(compile_metadata_1.CompileSummaryKind.Pipe, [core_1.createPipe]);
            this.annotationForParentClassWithSummaryKind.set(compile_metadata_1.CompileSummaryKind.NgModule, [core_1.createNgModule]);
            this.annotationForParentClassWithSummaryKind.set(compile_metadata_1.CompileSummaryKind.Injectable, [core_1.createInjectable, core_1.createPipe, core_1.createDirective, core_1.createComponent, core_1.createNgModule]);
        }
        StaticReflector.prototype.componentModuleUrl = function (typeOrFunc) {
            var staticSymbol = this.findSymbolDeclaration(typeOrFunc);
            return this.symbolResolver.getResourcePath(staticSymbol);
        };
        StaticReflector.prototype.resolveExternalReference = function (ref, containingFile) {
            var key = undefined;
            if (!containingFile) {
                key = ref.moduleName + ":" + ref.name;
                var declarationSymbol_1 = this.resolvedExternalReferences.get(key);
                if (declarationSymbol_1)
                    return declarationSymbol_1;
            }
            var refSymbol = this.symbolResolver.getSymbolByModule(ref.moduleName, ref.name, containingFile);
            var declarationSymbol = this.findSymbolDeclaration(refSymbol);
            if (!containingFile) {
                this.symbolResolver.recordModuleNameForFileName(refSymbol.filePath, ref.moduleName);
                this.symbolResolver.recordImportAs(declarationSymbol, refSymbol);
            }
            if (key) {
                this.resolvedExternalReferences.set(key, declarationSymbol);
            }
            return declarationSymbol;
        };
        StaticReflector.prototype.findDeclaration = function (moduleUrl, name, containingFile) {
            return this.findSymbolDeclaration(this.symbolResolver.getSymbolByModule(moduleUrl, name, containingFile));
        };
        StaticReflector.prototype.tryFindDeclaration = function (moduleUrl, name, containingFile) {
            var _this = this;
            return this.symbolResolver.ignoreErrorsFor(function () { return _this.findDeclaration(moduleUrl, name, containingFile); });
        };
        StaticReflector.prototype.findSymbolDeclaration = function (symbol) {
            var resolvedSymbol = this.symbolResolver.resolveSymbol(symbol);
            if (resolvedSymbol) {
                var resolvedMetadata = resolvedSymbol.metadata;
                if (resolvedMetadata && resolvedMetadata.__symbolic === 'resolved') {
                    resolvedMetadata = resolvedMetadata.symbol;
                }
                if (resolvedMetadata instanceof static_symbol_1.StaticSymbol) {
                    return this.findSymbolDeclaration(resolvedSymbol.metadata);
                }
            }
            return symbol;
        };
        StaticReflector.prototype.tryAnnotations = function (type) {
            var originalRecorder = this.errorRecorder;
            this.errorRecorder = function (error, fileName) { };
            try {
                return this.annotations(type);
            }
            finally {
                this.errorRecorder = originalRecorder;
            }
        };
        StaticReflector.prototype.annotations = function (type) {
            var _this = this;
            return this._annotations(type, function (type, decorators) { return _this.simplify(type, decorators); }, this.annotationCache);
        };
        StaticReflector.prototype.shallowAnnotations = function (type) {
            var _this = this;
            return this._annotations(type, function (type, decorators) { return _this.simplify(type, decorators, true); }, this.shallowAnnotationCache);
        };
        StaticReflector.prototype._annotations = function (type, simplify, annotationCache) {
            var annotations = annotationCache.get(type);
            if (!annotations) {
                annotations = [];
                var classMetadata = this.getTypeMetadata(type);
                var parentType = this.findParentType(type, classMetadata);
                if (parentType) {
                    var parentAnnotations = this.annotations(parentType);
                    annotations.push.apply(annotations, tslib_1.__spread(parentAnnotations));
                }
                var ownAnnotations_1 = [];
                if (classMetadata['decorators']) {
                    ownAnnotations_1 = simplify(type, classMetadata['decorators']);
                    if (ownAnnotations_1) {
                        annotations.push.apply(annotations, tslib_1.__spread(ownAnnotations_1));
                    }
                }
                if (parentType && !this.summaryResolver.isLibraryFile(type.filePath) &&
                    this.summaryResolver.isLibraryFile(parentType.filePath)) {
                    var summary = this.summaryResolver.resolveSummary(parentType);
                    if (summary && summary.type) {
                        var requiredAnnotationTypes = this.annotationForParentClassWithSummaryKind.get(summary.type.summaryKind);
                        var typeHasRequiredAnnotation = requiredAnnotationTypes.some(function (requiredType) { return ownAnnotations_1.some(function (ann) { return requiredType.isTypeOf(ann); }); });
                        if (!typeHasRequiredAnnotation) {
                            this.reportError(formatMetadataError(metadataError("Class " + type.name + " in " + type.filePath + " extends from a " + compile_metadata_1.CompileSummaryKind[summary.type.summaryKind] + " in another compilation unit without duplicating the decorator", 
                            /* summary */ undefined, "Please add a " + requiredAnnotationTypes.map(function (type) { return type.ngMetadataName; }).join(' or ') + " decorator to the class"), type), type);
                        }
                    }
                }
                annotationCache.set(type, annotations.filter(function (ann) { return !!ann; }));
            }
            return annotations;
        };
        StaticReflector.prototype.propMetadata = function (type) {
            var _this = this;
            var propMetadata = this.propertyCache.get(type);
            if (!propMetadata) {
                var classMetadata = this.getTypeMetadata(type);
                propMetadata = {};
                var parentType = this.findParentType(type, classMetadata);
                if (parentType) {
                    var parentPropMetadata_1 = this.propMetadata(parentType);
                    Object.keys(parentPropMetadata_1).forEach(function (parentProp) {
                        propMetadata[parentProp] = parentPropMetadata_1[parentProp];
                    });
                }
                var members_1 = classMetadata['members'] || {};
                Object.keys(members_1).forEach(function (propName) {
                    var propData = members_1[propName];
                    var prop = propData
                        .find(function (a) { return a['__symbolic'] == 'property' || a['__symbolic'] == 'method'; });
                    var decorators = [];
                    if (propMetadata[propName]) {
                        decorators.push.apply(decorators, tslib_1.__spread(propMetadata[propName]));
                    }
                    propMetadata[propName] = decorators;
                    if (prop && prop['decorators']) {
                        decorators.push.apply(decorators, tslib_1.__spread(_this.simplify(type, prop['decorators'])));
                    }
                });
                this.propertyCache.set(type, propMetadata);
            }
            return propMetadata;
        };
        StaticReflector.prototype.parameters = function (type) {
            var _this = this;
            if (!(type instanceof static_symbol_1.StaticSymbol)) {
                this.reportError(new Error("parameters received " + JSON.stringify(type) + " which is not a StaticSymbol"), type);
                return [];
            }
            try {
                var parameters_1 = this.parameterCache.get(type);
                if (!parameters_1) {
                    var classMetadata = this.getTypeMetadata(type);
                    var parentType = this.findParentType(type, classMetadata);
                    var members = classMetadata ? classMetadata['members'] : null;
                    var ctorData = members ? members['__ctor__'] : null;
                    if (ctorData) {
                        var ctor = ctorData.find(function (a) { return a['__symbolic'] == 'constructor'; });
                        var rawParameterTypes = ctor['parameters'] || [];
                        var parameterDecorators_1 = this.simplify(type, ctor['parameterDecorators'] || []);
                        parameters_1 = [];
                        rawParameterTypes.forEach(function (rawParamType, index) {
                            var nestedResult = [];
                            var paramType = _this.trySimplify(type, rawParamType);
                            if (paramType)
                                nestedResult.push(paramType);
                            var decorators = parameterDecorators_1 ? parameterDecorators_1[index] : null;
                            if (decorators) {
                                nestedResult.push.apply(nestedResult, tslib_1.__spread(decorators));
                            }
                            parameters_1.push(nestedResult);
                        });
                    }
                    else if (parentType) {
                        parameters_1 = this.parameters(parentType);
                    }
                    if (!parameters_1) {
                        parameters_1 = [];
                    }
                    this.parameterCache.set(type, parameters_1);
                }
                return parameters_1;
            }
            catch (e) {
                console.error("Failed on type " + JSON.stringify(type) + " with error " + e);
                throw e;
            }
        };
        StaticReflector.prototype._methodNames = function (type) {
            var methodNames = this.methodCache.get(type);
            if (!methodNames) {
                var classMetadata = this.getTypeMetadata(type);
                methodNames = {};
                var parentType = this.findParentType(type, classMetadata);
                if (parentType) {
                    var parentMethodNames_1 = this._methodNames(parentType);
                    Object.keys(parentMethodNames_1).forEach(function (parentProp) {
                        methodNames[parentProp] = parentMethodNames_1[parentProp];
                    });
                }
                var members_2 = classMetadata['members'] || {};
                Object.keys(members_2).forEach(function (propName) {
                    var propData = members_2[propName];
                    var isMethod = propData.some(function (a) { return a['__symbolic'] == 'method'; });
                    methodNames[propName] = methodNames[propName] || isMethod;
                });
                this.methodCache.set(type, methodNames);
            }
            return methodNames;
        };
        StaticReflector.prototype._staticMembers = function (type) {
            var staticMembers = this.staticCache.get(type);
            if (!staticMembers) {
                var classMetadata = this.getTypeMetadata(type);
                var staticMemberData = classMetadata['statics'] || {};
                staticMembers = Object.keys(staticMemberData);
                this.staticCache.set(type, staticMembers);
            }
            return staticMembers;
        };
        StaticReflector.prototype.findParentType = function (type, classMetadata) {
            var parentType = this.trySimplify(type, classMetadata['extends']);
            if (parentType instanceof static_symbol_1.StaticSymbol) {
                return parentType;
            }
        };
        StaticReflector.prototype.hasLifecycleHook = function (type, lcProperty) {
            if (!(type instanceof static_symbol_1.StaticSymbol)) {
                this.reportError(new Error("hasLifecycleHook received " + JSON.stringify(type) + " which is not a StaticSymbol"), type);
            }
            try {
                return !!this._methodNames(type)[lcProperty];
            }
            catch (e) {
                console.error("Failed on type " + JSON.stringify(type) + " with error " + e);
                throw e;
            }
        };
        StaticReflector.prototype.guards = function (type) {
            var e_1, _a;
            if (!(type instanceof static_symbol_1.StaticSymbol)) {
                this.reportError(new Error("guards received " + JSON.stringify(type) + " which is not a StaticSymbol"), type);
                return {};
            }
            var staticMembers = this._staticMembers(type);
            var result = {};
            try {
                for (var staticMembers_1 = tslib_1.__values(staticMembers), staticMembers_1_1 = staticMembers_1.next(); !staticMembers_1_1.done; staticMembers_1_1 = staticMembers_1.next()) {
                    var name_1 = staticMembers_1_1.value;
                    if (name_1.endsWith(TYPEGUARD_POSTFIX)) {
                        var property = name_1.substr(0, name_1.length - TYPEGUARD_POSTFIX.length);
                        var value = void 0;
                        if (property.endsWith(USE_IF)) {
                            property = name_1.substr(0, property.length - USE_IF.length);
                            value = USE_IF;
                        }
                        else {
                            value = this.getStaticSymbol(type.filePath, type.name, [name_1]);
                        }
                        result[property] = value;
                    }
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (staticMembers_1_1 && !staticMembers_1_1.done && (_a = staticMembers_1.return)) _a.call(staticMembers_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
            return result;
        };
        StaticReflector.prototype._registerDecoratorOrConstructor = function (type, ctor) {
            this.conversionMap.set(type, function (context, args) { return new (ctor.bind.apply(ctor, tslib_1.__spread([void 0], args)))(); });
        };
        StaticReflector.prototype._registerFunction = function (type, fn) {
            this.conversionMap.set(type, function (context, args) { return fn.apply(undefined, args); });
        };
        StaticReflector.prototype.initializeConversionMap = function () {
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Injectable'), core_1.createInjectable);
            this.injectionToken = this.findDeclaration(ANGULAR_CORE, 'InjectionToken');
            this.opaqueToken = this.findDeclaration(ANGULAR_CORE, 'OpaqueToken');
            this.ROUTES = this.tryFindDeclaration(ANGULAR_ROUTER, 'ROUTES');
            this.ANALYZE_FOR_ENTRY_COMPONENTS =
                this.findDeclaration(ANGULAR_CORE, 'ANALYZE_FOR_ENTRY_COMPONENTS');
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Host'), core_1.createHost);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Self'), core_1.createSelf);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'SkipSelf'), core_1.createSkipSelf);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Inject'), core_1.createInject);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Optional'), core_1.createOptional);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Attribute'), core_1.createAttribute);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'ContentChild'), core_1.createContentChild);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'ContentChildren'), core_1.createContentChildren);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'ViewChild'), core_1.createViewChild);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'ViewChildren'), core_1.createViewChildren);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Input'), core_1.createInput);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Output'), core_1.createOutput);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Pipe'), core_1.createPipe);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'HostBinding'), core_1.createHostBinding);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'HostListener'), core_1.createHostListener);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Directive'), core_1.createDirective);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Component'), core_1.createComponent);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'NgModule'), core_1.createNgModule);
            // Note: Some metadata classes can be used directly with Provider.deps.
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Host'), core_1.createHost);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Self'), core_1.createSelf);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'SkipSelf'), core_1.createSkipSelf);
            this._registerDecoratorOrConstructor(this.findDeclaration(ANGULAR_CORE, 'Optional'), core_1.createOptional);
        };
        /**
         * getStaticSymbol produces a Type whose metadata is known but whose implementation is not loaded.
         * All types passed to the StaticResolver should be pseudo-types returned by this method.
         *
         * @param declarationFile the absolute path of the file where the symbol is declared
         * @param name the name of the type.
         */
        StaticReflector.prototype.getStaticSymbol = function (declarationFile, name, members) {
            return this.symbolResolver.getStaticSymbol(declarationFile, name, members);
        };
        /**
         * Simplify but discard any errors
         */
        StaticReflector.prototype.trySimplify = function (context, value) {
            var originalRecorder = this.errorRecorder;
            this.errorRecorder = function (error, fileName) { };
            var result = this.simplify(context, value);
            this.errorRecorder = originalRecorder;
            return result;
        };
        /** @internal */
        StaticReflector.prototype.simplify = function (context, value, lazy) {
            if (lazy === void 0) { lazy = false; }
            var self = this;
            var scope = BindingScope.empty;
            var calling = new Map();
            var rootContext = context;
            function simplifyInContext(context, value, depth, references) {
                function resolveReferenceValue(staticSymbol) {
                    var resolvedSymbol = self.symbolResolver.resolveSymbol(staticSymbol);
                    return resolvedSymbol ? resolvedSymbol.metadata : null;
                }
                function simplifyEagerly(value) {
                    return simplifyInContext(context, value, depth, 0);
                }
                function simplifyLazily(value) {
                    return simplifyInContext(context, value, depth, references + 1);
                }
                function simplifyNested(nestedContext, value) {
                    if (nestedContext === context) {
                        // If the context hasn't changed let the exception propagate unmodified.
                        return simplifyInContext(nestedContext, value, depth + 1, references);
                    }
                    try {
                        return simplifyInContext(nestedContext, value, depth + 1, references);
                    }
                    catch (e) {
                        if (isMetadataError(e)) {
                            // Propagate the message text up but add a message to the chain that explains how we got
                            // here.
                            // e.chain implies e.symbol
                            var summaryMsg = e.chain ? 'references \'' + e.symbol.name + '\'' : errorSummary(e);
                            var summary = "'" + nestedContext.name + "' " + summaryMsg;
                            var chain = { message: summary, position: e.position, next: e.chain };
                            // TODO(chuckj): retrieve the position information indirectly from the collectors node
                            // map if the metadata is from a .ts file.
                            self.error({
                                message: e.message,
                                advise: e.advise,
                                context: e.context, chain: chain,
                                symbol: nestedContext
                            }, context);
                        }
                        else {
                            // It is probably an internal error.
                            throw e;
                        }
                    }
                }
                function simplifyCall(functionSymbol, targetFunction, args, targetExpression) {
                    if (targetFunction && targetFunction['__symbolic'] == 'function') {
                        if (calling.get(functionSymbol)) {
                            self.error({
                                message: 'Recursion is not supported',
                                summary: "called '" + functionSymbol.name + "' recursively",
                                value: targetFunction
                            }, functionSymbol);
                        }
                        try {
                            var value_1 = targetFunction['value'];
                            if (value_1 && (depth != 0 || value_1.__symbolic != 'error')) {
                                var parameters = targetFunction['parameters'];
                                var defaults = targetFunction.defaults;
                                args = args.map(function (arg) { return simplifyNested(context, arg); })
                                    .map(function (arg) { return shouldIgnore(arg) ? undefined : arg; });
                                if (defaults && defaults.length > args.length) {
                                    args.push.apply(args, tslib_1.__spread(defaults.slice(args.length).map(function (value) { return simplify(value); })));
                                }
                                calling.set(functionSymbol, true);
                                var functionScope = BindingScope.build();
                                for (var i = 0; i < parameters.length; i++) {
                                    functionScope.define(parameters[i], args[i]);
                                }
                                var oldScope = scope;
                                var result_1;
                                try {
                                    scope = functionScope.done();
                                    result_1 = simplifyNested(functionSymbol, value_1);
                                }
                                finally {
                                    scope = oldScope;
                                }
                                return result_1;
                            }
                        }
                        finally {
                            calling.delete(functionSymbol);
                        }
                    }
                    if (depth === 0) {
                        // If depth is 0 we are evaluating the top level expression that is describing element
                        // decorator. In this case, it is a decorator we don't understand, such as a custom
                        // non-angular decorator, and we should just ignore it.
                        return IGNORE;
                    }
                    var position = undefined;
                    if (targetExpression && targetExpression.__symbolic == 'resolved') {
                        var line = targetExpression.line;
                        var character = targetExpression.character;
                        var fileName = targetExpression.fileName;
                        if (fileName != null && line != null && character != null) {
                            position = { fileName: fileName, line: line, column: character };
                        }
                    }
                    self.error({
                        message: FUNCTION_CALL_NOT_SUPPORTED,
                        context: functionSymbol,
                        value: targetFunction, position: position
                    }, context);
                }
                function simplify(expression) {
                    var e_2, _a, e_3, _b;
                    if (isPrimitive(expression)) {
                        return expression;
                    }
                    if (expression instanceof Array) {
                        var result_2 = [];
                        try {
                            for (var _c = tslib_1.__values(expression), _d = _c.next(); !_d.done; _d = _c.next()) {
                                var item = _d.value;
                                // Check for a spread expression
                                if (item && item.__symbolic === 'spread') {
                                    // We call with references as 0 because we require the actual value and cannot
                                    // tolerate a reference here.
                                    var spreadArray = simplifyEagerly(item.expression);
                                    if (Array.isArray(spreadArray)) {
                                        try {
                                            for (var spreadArray_1 = (e_3 = void 0, tslib_1.__values(spreadArray)), spreadArray_1_1 = spreadArray_1.next(); !spreadArray_1_1.done; spreadArray_1_1 = spreadArray_1.next()) {
                                                var spreadItem = spreadArray_1_1.value;
                                                result_2.push(spreadItem);
                                            }
                                        }
                                        catch (e_3_1) { e_3 = { error: e_3_1 }; }
                                        finally {
                                            try {
                                                if (spreadArray_1_1 && !spreadArray_1_1.done && (_b = spreadArray_1.return)) _b.call(spreadArray_1);
                                            }
                                            finally { if (e_3) throw e_3.error; }
                                        }
                                        continue;
                                    }
                                }
                                var value_2 = simplify(item);
                                if (shouldIgnore(value_2)) {
                                    continue;
                                }
                                result_2.push(value_2);
                            }
                        }
                        catch (e_2_1) { e_2 = { error: e_2_1 }; }
                        finally {
                            try {
                                if (_d && !_d.done && (_a = _c.return)) _a.call(_c);
                            }
                            finally { if (e_2) throw e_2.error; }
                        }
                        return result_2;
                    }
                    if (expression instanceof static_symbol_1.StaticSymbol) {
                        // Stop simplification at builtin symbols or if we are in a reference context and
                        // the symbol doesn't have members.
                        if (expression === self.injectionToken || self.conversionMap.has(expression) ||
                            (references > 0 && !expression.members.length)) {
                            return expression;
                        }
                        else {
                            var staticSymbol = expression;
                            var declarationValue = resolveReferenceValue(staticSymbol);
                            if (declarationValue != null) {
                                return simplifyNested(staticSymbol, declarationValue);
                            }
                            else {
                                return staticSymbol;
                            }
                        }
                    }
                    if (expression) {
                        if (expression['__symbolic']) {
                            var staticSymbol = void 0;
                            switch (expression['__symbolic']) {
                                case 'binop':
                                    var left = simplify(expression['left']);
                                    if (shouldIgnore(left))
                                        return left;
                                    var right = simplify(expression['right']);
                                    if (shouldIgnore(right))
                                        return right;
                                    switch (expression['operator']) {
                                        case '&&':
                                            return left && right;
                                        case '||':
                                            return left || right;
                                        case '|':
                                            return left | right;
                                        case '^':
                                            return left ^ right;
                                        case '&':
                                            return left & right;
                                        case '==':
                                            return left == right;
                                        case '!=':
                                            return left != right;
                                        case '===':
                                            return left === right;
                                        case '!==':
                                            return left !== right;
                                        case '<':
                                            return left < right;
                                        case '>':
                                            return left > right;
                                        case '<=':
                                            return left <= right;
                                        case '>=':
                                            return left >= right;
                                        case '<<':
                                            return left << right;
                                        case '>>':
                                            return left >> right;
                                        case '+':
                                            return left + right;
                                        case '-':
                                            return left - right;
                                        case '*':
                                            return left * right;
                                        case '/':
                                            return left / right;
                                        case '%':
                                            return left % right;
                                    }
                                    return null;
                                case 'if':
                                    var condition = simplify(expression['condition']);
                                    return condition ? simplify(expression['thenExpression']) :
                                        simplify(expression['elseExpression']);
                                case 'pre':
                                    var operand = simplify(expression['operand']);
                                    if (shouldIgnore(operand))
                                        return operand;
                                    switch (expression['operator']) {
                                        case '+':
                                            return operand;
                                        case '-':
                                            return -operand;
                                        case '!':
                                            return !operand;
                                        case '~':
                                            return ~operand;
                                    }
                                    return null;
                                case 'index':
                                    var indexTarget = simplifyEagerly(expression['expression']);
                                    var index = simplifyEagerly(expression['index']);
                                    if (indexTarget && isPrimitive(index))
                                        return indexTarget[index];
                                    return null;
                                case 'select':
                                    var member = expression['member'];
                                    var selectContext = context;
                                    var selectTarget = simplify(expression['expression']);
                                    if (selectTarget instanceof static_symbol_1.StaticSymbol) {
                                        var members = selectTarget.members.concat(member);
                                        selectContext =
                                            self.getStaticSymbol(selectTarget.filePath, selectTarget.name, members);
                                        var declarationValue = resolveReferenceValue(selectContext);
                                        if (declarationValue != null) {
                                            return simplifyNested(selectContext, declarationValue);
                                        }
                                        else {
                                            return selectContext;
                                        }
                                    }
                                    if (selectTarget && isPrimitive(member))
                                        return simplifyNested(selectContext, selectTarget[member]);
                                    return null;
                                case 'reference':
                                    // Note: This only has to deal with variable references, as symbol references have
                                    // been converted into 'resolved'
                                    // in the StaticSymbolResolver.
                                    var name_2 = expression['name'];
                                    var localValue = scope.resolve(name_2);
                                    if (localValue != BindingScope.missing) {
                                        return localValue;
                                    }
                                    break;
                                case 'resolved':
                                    try {
                                        return simplify(expression.symbol);
                                    }
                                    catch (e) {
                                        // If an error is reported evaluating the symbol record the position of the
                                        // reference in the error so it can
                                        // be reported in the error message generated from the exception.
                                        if (isMetadataError(e) && expression.fileName != null &&
                                            expression.line != null && expression.character != null) {
                                            e.position = {
                                                fileName: expression.fileName,
                                                line: expression.line,
                                                column: expression.character
                                            };
                                        }
                                        throw e;
                                    }
                                case 'class':
                                    return context;
                                case 'function':
                                    return context;
                                case 'new':
                                case 'call':
                                    // Determine if the function is a built-in conversion
                                    staticSymbol = simplifyInContext(context, expression['expression'], depth + 1, /* references */ 0);
                                    if (staticSymbol instanceof static_symbol_1.StaticSymbol) {
                                        if (staticSymbol === self.injectionToken || staticSymbol === self.opaqueToken) {
                                            // if somebody calls new InjectionToken, don't create an InjectionToken,
                                            // but rather return the symbol to which the InjectionToken is assigned to.
                                            // OpaqueToken is supported too as it is required by the language service to
                                            // support v4 and prior versions of Angular.
                                            return context;
                                        }
                                        var argExpressions = expression['arguments'] || [];
                                        var converter = self.conversionMap.get(staticSymbol);
                                        if (converter) {
                                            var args = argExpressions.map(function (arg) { return simplifyNested(context, arg); })
                                                .map(function (arg) { return shouldIgnore(arg) ? undefined : arg; });
                                            return converter(context, args);
                                        }
                                        else {
                                            // Determine if the function is one we can simplify.
                                            var targetFunction = resolveReferenceValue(staticSymbol);
                                            return simplifyCall(staticSymbol, targetFunction, argExpressions, expression['expression']);
                                        }
                                    }
                                    return IGNORE;
                                case 'error':
                                    var message = expression.message;
                                    if (expression['line'] != null) {
                                        self.error({
                                            message: message,
                                            context: expression.context,
                                            value: expression,
                                            position: {
                                                fileName: expression['fileName'],
                                                line: expression['line'],
                                                column: expression['character']
                                            }
                                        }, context);
                                    }
                                    else {
                                        self.error({ message: message, context: expression.context }, context);
                                    }
                                    return IGNORE;
                                case 'ignore':
                                    return expression;
                            }
                            return null;
                        }
                        return mapStringMap(expression, function (value, name) {
                            if (REFERENCE_SET.has(name)) {
                                if (name === USE_VALUE && PROVIDE in expression) {
                                    // If this is a provider expression, check for special tokens that need the value
                                    // during analysis.
                                    var provide = simplify(expression.provide);
                                    if (provide === self.ROUTES || provide == self.ANALYZE_FOR_ENTRY_COMPONENTS) {
                                        return simplify(value);
                                    }
                                }
                                return simplifyLazily(value);
                            }
                            return simplify(value);
                        });
                    }
                    return IGNORE;
                }
                return simplify(value);
            }
            var result;
            try {
                result = simplifyInContext(context, value, 0, lazy ? 1 : 0);
            }
            catch (e) {
                if (this.errorRecorder) {
                    this.reportError(e, context);
                }
                else {
                    throw formatMetadataError(e, context);
                }
            }
            if (shouldIgnore(result)) {
                return undefined;
            }
            return result;
        };
        StaticReflector.prototype.getTypeMetadata = function (type) {
            var resolvedSymbol = this.symbolResolver.resolveSymbol(type);
            return resolvedSymbol && resolvedSymbol.metadata ? resolvedSymbol.metadata :
                { __symbolic: 'class' };
        };
        StaticReflector.prototype.reportError = function (error, context, path) {
            if (this.errorRecorder) {
                this.errorRecorder(formatMetadataError(error, context), (context && context.filePath) || path);
            }
            else {
                throw error;
            }
        };
        StaticReflector.prototype.error = function (_a, reportingContext) {
            var message = _a.message, summary = _a.summary, advise = _a.advise, position = _a.position, context = _a.context, value = _a.value, symbol = _a.symbol, chain = _a.chain;
            this.reportError(metadataError(message, summary, advise, position, symbol, context, chain), reportingContext);
        };
        return StaticReflector;
    }());
    exports.StaticReflector = StaticReflector;
    var METADATA_ERROR = 'ngMetadataError';
    function metadataError(message, summary, advise, position, symbol, context, chain) {
        var error = util_1.syntaxError(message);
        error[METADATA_ERROR] = true;
        if (advise)
            error.advise = advise;
        if (position)
            error.position = position;
        if (summary)
            error.summary = summary;
        if (context)
            error.context = context;
        if (chain)
            error.chain = chain;
        if (symbol)
            error.symbol = symbol;
        return error;
    }
    function isMetadataError(error) {
        return !!error[METADATA_ERROR];
    }
    var REFERENCE_TO_NONEXPORTED_CLASS = 'Reference to non-exported class';
    var VARIABLE_NOT_INITIALIZED = 'Variable not initialized';
    var DESTRUCTURE_NOT_SUPPORTED = 'Destructuring not supported';
    var COULD_NOT_RESOLVE_TYPE = 'Could not resolve type';
    var FUNCTION_CALL_NOT_SUPPORTED = 'Function call not supported';
    var REFERENCE_TO_LOCAL_SYMBOL = 'Reference to a local symbol';
    var LAMBDA_NOT_SUPPORTED = 'Lambda not supported';
    function expandedMessage(message, context) {
        switch (message) {
            case REFERENCE_TO_NONEXPORTED_CLASS:
                if (context && context.className) {
                    return "References to a non-exported class are not supported in decorators but " + context.className + " was referenced.";
                }
                break;
            case VARIABLE_NOT_INITIALIZED:
                return 'Only initialized variables and constants can be referenced in decorators because the value of this variable is needed by the template compiler';
            case DESTRUCTURE_NOT_SUPPORTED:
                return 'Referencing an exported destructured variable or constant is not supported in decorators and this value is needed by the template compiler';
            case COULD_NOT_RESOLVE_TYPE:
                if (context && context.typeName) {
                    return "Could not resolve type " + context.typeName;
                }
                break;
            case FUNCTION_CALL_NOT_SUPPORTED:
                if (context && context.name) {
                    return "Function calls are not supported in decorators but '" + context.name + "' was called";
                }
                return 'Function calls are not supported in decorators';
            case REFERENCE_TO_LOCAL_SYMBOL:
                if (context && context.name) {
                    return "Reference to a local (non-exported) symbols are not supported in decorators but '" + context.name + "' was referenced";
                }
                break;
            case LAMBDA_NOT_SUPPORTED:
                return "Function expressions are not supported in decorators";
        }
        return message;
    }
    function messageAdvise(message, context) {
        switch (message) {
            case REFERENCE_TO_NONEXPORTED_CLASS:
                if (context && context.className) {
                    return "Consider exporting '" + context.className + "'";
                }
                break;
            case DESTRUCTURE_NOT_SUPPORTED:
                return 'Consider simplifying to avoid destructuring';
            case REFERENCE_TO_LOCAL_SYMBOL:
                if (context && context.name) {
                    return "Consider exporting '" + context.name + "'";
                }
                break;
            case LAMBDA_NOT_SUPPORTED:
                return "Consider changing the function expression into an exported function";
        }
        return undefined;
    }
    function errorSummary(error) {
        if (error.summary) {
            return error.summary;
        }
        switch (error.message) {
            case REFERENCE_TO_NONEXPORTED_CLASS:
                if (error.context && error.context.className) {
                    return "references non-exported class " + error.context.className;
                }
                break;
            case VARIABLE_NOT_INITIALIZED:
                return 'is not initialized';
            case DESTRUCTURE_NOT_SUPPORTED:
                return 'is a destructured variable';
            case COULD_NOT_RESOLVE_TYPE:
                return 'could not be resolved';
            case FUNCTION_CALL_NOT_SUPPORTED:
                if (error.context && error.context.name) {
                    return "calls '" + error.context.name + "'";
                }
                return "calls a function";
            case REFERENCE_TO_LOCAL_SYMBOL:
                if (error.context && error.context.name) {
                    return "references local variable " + error.context.name;
                }
                return "references a local variable";
        }
        return 'contains the error';
    }
    function mapStringMap(input, transform) {
        if (!input)
            return {};
        var result = {};
        Object.keys(input).forEach(function (key) {
            var value = transform(input[key], key);
            if (!shouldIgnore(value)) {
                if (HIDDEN_KEY.test(key)) {
                    Object.defineProperty(result, key, { enumerable: false, configurable: true, value: value });
                }
                else {
                    result[key] = value;
                }
            }
        });
        return result;
    }
    function isPrimitive(o) {
        return o === null || (typeof o !== 'function' && typeof o !== 'object');
    }
    var BindingScope = /** @class */ (function () {
        function BindingScope() {
        }
        BindingScope.build = function () {
            var current = new Map();
            return {
                define: function (name, value) {
                    current.set(name, value);
                    return this;
                },
                done: function () {
                    return current.size > 0 ? new PopulatedScope(current) : BindingScope.empty;
                }
            };
        };
        BindingScope.missing = {};
        BindingScope.empty = { resolve: function (name) { return BindingScope.missing; } };
        return BindingScope;
    }());
    var PopulatedScope = /** @class */ (function (_super) {
        tslib_1.__extends(PopulatedScope, _super);
        function PopulatedScope(bindings) {
            var _this = _super.call(this) || this;
            _this.bindings = bindings;
            return _this;
        }
        PopulatedScope.prototype.resolve = function (name) {
            return this.bindings.has(name) ? this.bindings.get(name) : BindingScope.missing;
        };
        return PopulatedScope;
    }(BindingScope));
    function formatMetadataMessageChain(chain, advise) {
        var expanded = expandedMessage(chain.message, chain.context);
        var nesting = chain.symbol ? " in '" + chain.symbol.name + "'" : '';
        var message = "" + expanded + nesting;
        var position = chain.position;
        var next = chain.next ?
            formatMetadataMessageChain(chain.next, advise) :
            advise ? { message: advise } : undefined;
        return { message: message, position: position, next: next };
    }
    function formatMetadataError(e, context) {
        if (isMetadataError(e)) {
            // Produce a formatted version of the and leaving enough information in the original error
            // to recover the formatting information to eventually produce a diagnostic error message.
            var position = e.position;
            var chain = {
                message: "Error during template compile of '" + context.name + "'",
                position: position,
                next: { message: e.message, next: e.chain, context: e.context, symbol: e.symbol }
            };
            var advise = e.advise || messageAdvise(e.message, e.context);
            return formatted_error_1.formattedError(formatMetadataMessageChain(chain, advise));
        }
        return e;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RhdGljX3JlZmxlY3Rvci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9hb3Qvc3RhdGljX3JlZmxlY3Rvci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCwyRUFBdUQ7SUFFdkQsbURBQWlXO0lBR2pXLG1EQUFvQztJQUVwQyw2RUFBd0U7SUFDeEUseUVBQTZDO0lBRzdDLElBQU0sWUFBWSxHQUFHLGVBQWUsQ0FBQztJQUNyQyxJQUFNLGNBQWMsR0FBRyxpQkFBaUIsQ0FBQztJQUV6QyxJQUFNLFVBQVUsR0FBRyxVQUFVLENBQUM7SUFFOUIsSUFBTSxNQUFNLEdBQUc7UUFDYixVQUFVLEVBQUUsUUFBUTtLQUNyQixDQUFDO0lBRUYsSUFBTSxTQUFTLEdBQUcsVUFBVSxDQUFDO0lBQzdCLElBQU0sT0FBTyxHQUFHLFNBQVMsQ0FBQztJQUMxQixJQUFNLGFBQWEsR0FBRyxJQUFJLEdBQUcsQ0FBQyxDQUFDLFNBQVMsRUFBRSxZQUFZLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxjQUFjLENBQUMsQ0FBQyxDQUFDO0lBQ3ZGLElBQU0saUJBQWlCLEdBQUcsV0FBVyxDQUFDO0lBQ3RDLElBQU0sTUFBTSxHQUFHLE9BQU8sQ0FBQztJQUV2QixTQUFTLFlBQVksQ0FBQyxLQUFVO1FBQzlCLE9BQU8sS0FBSyxJQUFJLEtBQUssQ0FBQyxVQUFVLElBQUksUUFBUSxDQUFDO0lBQy9DLENBQUM7SUFFRDs7O09BR0c7SUFDSDtRQW9CRSx5QkFDWSxlQUE4QyxFQUM5QyxjQUFvQyxFQUM1QyxvQkFBd0UsRUFDeEUsc0JBQXdFLEVBQ2hFLGFBQXVEO1lBTG5FLGlCQW1CQztZQWhCRyxxQ0FBQSxFQUFBLHlCQUF3RTtZQUN4RSx1Q0FBQSxFQUFBLDJCQUF3RTtZQUhoRSxvQkFBZSxHQUFmLGVBQWUsQ0FBK0I7WUFDOUMsbUJBQWMsR0FBZCxjQUFjLENBQXNCO1lBR3BDLGtCQUFhLEdBQWIsYUFBYSxDQUEwQztZQXhCM0Qsb0JBQWUsR0FBRyxJQUFJLEdBQUcsRUFBdUIsQ0FBQztZQUNqRCwyQkFBc0IsR0FBRyxJQUFJLEdBQUcsRUFBdUIsQ0FBQztZQUN4RCxrQkFBYSxHQUFHLElBQUksR0FBRyxFQUF3QyxDQUFDO1lBQ2hFLG1CQUFjLEdBQUcsSUFBSSxHQUFHLEVBQXVCLENBQUM7WUFDaEQsZ0JBQVcsR0FBRyxJQUFJLEdBQUcsRUFBMEMsQ0FBQztZQUNoRSxnQkFBVyxHQUFHLElBQUksR0FBRyxFQUEwQixDQUFDO1lBQ2hELGtCQUFhLEdBQUcsSUFBSSxHQUFHLEVBQTZELENBQUM7WUFDckYsK0JBQTBCLEdBQUcsSUFBSSxHQUFHLEVBQXdCLENBQUM7WUFTN0QsNENBQXVDLEdBQzNDLElBQUksR0FBRyxFQUE4QyxDQUFDO1lBUXhELElBQUksQ0FBQyx1QkFBdUIsRUFBRSxDQUFDO1lBQy9CLG9CQUFvQixDQUFDLE9BQU8sQ0FDeEIsVUFBQyxFQUFFLElBQUssT0FBQSxLQUFJLENBQUMsK0JBQStCLENBQ3hDLEtBQUksQ0FBQyxlQUFlLENBQUMsRUFBRSxDQUFDLFFBQVEsRUFBRSxFQUFFLENBQUMsSUFBSSxDQUFDLEVBQUUsRUFBRSxDQUFDLElBQUksQ0FBQyxFQURoRCxDQUNnRCxDQUFDLENBQUM7WUFDOUQsc0JBQXNCLENBQUMsT0FBTyxDQUMxQixVQUFDLEVBQUUsSUFBSyxPQUFBLEtBQUksQ0FBQyxpQkFBaUIsQ0FBQyxLQUFJLENBQUMsZUFBZSxDQUFDLEVBQUUsQ0FBQyxRQUFRLEVBQUUsRUFBRSxDQUFDLElBQUksQ0FBQyxFQUFFLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBekUsQ0FBeUUsQ0FBQyxDQUFDO1lBQ3ZGLElBQUksQ0FBQyx1Q0FBdUMsQ0FBQyxHQUFHLENBQzVDLHFDQUFrQixDQUFDLFNBQVMsRUFBRSxDQUFDLHNCQUFlLEVBQUUsc0JBQWUsQ0FBQyxDQUFDLENBQUM7WUFDdEUsSUFBSSxDQUFDLHVDQUF1QyxDQUFDLEdBQUcsQ0FBQyxxQ0FBa0IsQ0FBQyxJQUFJLEVBQUUsQ0FBQyxpQkFBVSxDQUFDLENBQUMsQ0FBQztZQUN4RixJQUFJLENBQUMsdUNBQXVDLENBQUMsR0FBRyxDQUFDLHFDQUFrQixDQUFDLFFBQVEsRUFBRSxDQUFDLHFCQUFjLENBQUMsQ0FBQyxDQUFDO1lBQ2hHLElBQUksQ0FBQyx1Q0FBdUMsQ0FBQyxHQUFHLENBQzVDLHFDQUFrQixDQUFDLFVBQVUsRUFDN0IsQ0FBQyx1QkFBZ0IsRUFBRSxpQkFBVSxFQUFFLHNCQUFlLEVBQUUsc0JBQWUsRUFBRSxxQkFBYyxDQUFDLENBQUMsQ0FBQztRQUN4RixDQUFDO1FBRUQsNENBQWtCLEdBQWxCLFVBQW1CLFVBQXdCO1lBQ3pDLElBQU0sWUFBWSxHQUFHLElBQUksQ0FBQyxxQkFBcUIsQ0FBQyxVQUFVLENBQUMsQ0FBQztZQUM1RCxPQUFPLElBQUksQ0FBQyxjQUFjLENBQUMsZUFBZSxDQUFDLFlBQVksQ0FBQyxDQUFDO1FBQzNELENBQUM7UUFFRCxrREFBd0IsR0FBeEIsVUFBeUIsR0FBd0IsRUFBRSxjQUF1QjtZQUN4RSxJQUFJLEdBQUcsR0FBcUIsU0FBUyxDQUFDO1lBQ3RDLElBQUksQ0FBQyxjQUFjLEVBQUU7Z0JBQ25CLEdBQUcsR0FBTSxHQUFHLENBQUMsVUFBVSxTQUFJLEdBQUcsQ0FBQyxJQUFNLENBQUM7Z0JBQ3RDLElBQU0sbUJBQWlCLEdBQUcsSUFBSSxDQUFDLDBCQUEwQixDQUFDLEdBQUcsQ0FBQyxHQUFHLENBQUMsQ0FBQztnQkFDbkUsSUFBSSxtQkFBaUI7b0JBQUUsT0FBTyxtQkFBaUIsQ0FBQzthQUNqRDtZQUNELElBQU0sU0FBUyxHQUNYLElBQUksQ0FBQyxjQUFjLENBQUMsaUJBQWlCLENBQUMsR0FBRyxDQUFDLFVBQVksRUFBRSxHQUFHLENBQUMsSUFBTSxFQUFFLGNBQWMsQ0FBQyxDQUFDO1lBQ3hGLElBQU0saUJBQWlCLEdBQUcsSUFBSSxDQUFDLHFCQUFxQixDQUFDLFNBQVMsQ0FBQyxDQUFDO1lBQ2hFLElBQUksQ0FBQyxjQUFjLEVBQUU7Z0JBQ25CLElBQUksQ0FBQyxjQUFjLENBQUMsMkJBQTJCLENBQUMsU0FBUyxDQUFDLFFBQVEsRUFBRSxHQUFHLENBQUMsVUFBWSxDQUFDLENBQUM7Z0JBQ3RGLElBQUksQ0FBQyxjQUFjLENBQUMsY0FBYyxDQUFDLGlCQUFpQixFQUFFLFNBQVMsQ0FBQyxDQUFDO2FBQ2xFO1lBQ0QsSUFBSSxHQUFHLEVBQUU7Z0JBQ1AsSUFBSSxDQUFDLDBCQUEwQixDQUFDLEdBQUcsQ0FBQyxHQUFHLEVBQUUsaUJBQWlCLENBQUMsQ0FBQzthQUM3RDtZQUNELE9BQU8saUJBQWlCLENBQUM7UUFDM0IsQ0FBQztRQUVELHlDQUFlLEdBQWYsVUFBZ0IsU0FBaUIsRUFBRSxJQUFZLEVBQUUsY0FBdUI7WUFDdEUsT0FBTyxJQUFJLENBQUMscUJBQXFCLENBQzdCLElBQUksQ0FBQyxjQUFjLENBQUMsaUJBQWlCLENBQUMsU0FBUyxFQUFFLElBQUksRUFBRSxjQUFjLENBQUMsQ0FBQyxDQUFDO1FBQzlFLENBQUM7UUFFRCw0Q0FBa0IsR0FBbEIsVUFBbUIsU0FBaUIsRUFBRSxJQUFZLEVBQUUsY0FBdUI7WUFBM0UsaUJBR0M7WUFGQyxPQUFPLElBQUksQ0FBQyxjQUFjLENBQUMsZUFBZSxDQUN0QyxjQUFNLE9BQUEsS0FBSSxDQUFDLGVBQWUsQ0FBQyxTQUFTLEVBQUUsSUFBSSxFQUFFLGNBQWMsQ0FBQyxFQUFyRCxDQUFxRCxDQUFDLENBQUM7UUFDbkUsQ0FBQztRQUVELCtDQUFxQixHQUFyQixVQUFzQixNQUFvQjtZQUN4QyxJQUFNLGNBQWMsR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLGFBQWEsQ0FBQyxNQUFNLENBQUMsQ0FBQztZQUNqRSxJQUFJLGNBQWMsRUFBRTtnQkFDbEIsSUFBSSxnQkFBZ0IsR0FBRyxjQUFjLENBQUMsUUFBUSxDQUFDO2dCQUMvQyxJQUFJLGdCQUFnQixJQUFJLGdCQUFnQixDQUFDLFVBQVUsS0FBSyxVQUFVLEVBQUU7b0JBQ2xFLGdCQUFnQixHQUFHLGdCQUFnQixDQUFDLE1BQU0sQ0FBQztpQkFDNUM7Z0JBQ0QsSUFBSSxnQkFBZ0IsWUFBWSw0QkFBWSxFQUFFO29CQUM1QyxPQUFPLElBQUksQ0FBQyxxQkFBcUIsQ0FBQyxjQUFjLENBQUMsUUFBUSxDQUFDLENBQUM7aUJBQzVEO2FBQ0Y7WUFDRCxPQUFPLE1BQU0sQ0FBQztRQUNoQixDQUFDO1FBRU0sd0NBQWMsR0FBckIsVUFBc0IsSUFBa0I7WUFDdEMsSUFBTSxnQkFBZ0IsR0FBRyxJQUFJLENBQUMsYUFBYSxDQUFDO1lBQzVDLElBQUksQ0FBQyxhQUFhLEdBQUcsVUFBQyxLQUFVLEVBQUUsUUFBaUIsSUFBTSxDQUFDLENBQUM7WUFDM0QsSUFBSTtnQkFDRixPQUFPLElBQUksQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLENBQUM7YUFDL0I7b0JBQVM7Z0JBQ1IsSUFBSSxDQUFDLGFBQWEsR0FBRyxnQkFBZ0IsQ0FBQzthQUN2QztRQUNILENBQUM7UUFFTSxxQ0FBVyxHQUFsQixVQUFtQixJQUFrQjtZQUFyQyxpQkFJQztZQUhDLE9BQU8sSUFBSSxDQUFDLFlBQVksQ0FDcEIsSUFBSSxFQUFFLFVBQUMsSUFBa0IsRUFBRSxVQUFlLElBQUssT0FBQSxLQUFJLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxVQUFVLENBQUMsRUFBL0IsQ0FBK0IsRUFDOUUsSUFBSSxDQUFDLGVBQWUsQ0FBQyxDQUFDO1FBQzVCLENBQUM7UUFFTSw0Q0FBa0IsR0FBekIsVUFBMEIsSUFBa0I7WUFBNUMsaUJBSUM7WUFIQyxPQUFPLElBQUksQ0FBQyxZQUFZLENBQ3BCLElBQUksRUFBRSxVQUFDLElBQWtCLEVBQUUsVUFBZSxJQUFLLE9BQUEsS0FBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLEVBQUUsVUFBVSxFQUFFLElBQUksQ0FBQyxFQUFyQyxDQUFxQyxFQUNwRixJQUFJLENBQUMsc0JBQXNCLENBQUMsQ0FBQztRQUNuQyxDQUFDO1FBRU8sc0NBQVksR0FBcEIsVUFDSSxJQUFrQixFQUFFLFFBQXNELEVBQzFFLGVBQXlDO1lBQzNDLElBQUksV0FBVyxHQUFHLGVBQWUsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDNUMsSUFBSSxDQUFDLFdBQVcsRUFBRTtnQkFDaEIsV0FBVyxHQUFHLEVBQUUsQ0FBQztnQkFDakIsSUFBTSxhQUFhLEdBQUcsSUFBSSxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFDakQsSUFBTSxVQUFVLEdBQUcsSUFBSSxDQUFDLGNBQWMsQ0FBQyxJQUFJLEVBQUUsYUFBYSxDQUFDLENBQUM7Z0JBQzVELElBQUksVUFBVSxFQUFFO29CQUNkLElBQU0saUJBQWlCLEdBQUcsSUFBSSxDQUFDLFdBQVcsQ0FBQyxVQUFVLENBQUMsQ0FBQztvQkFDdkQsV0FBVyxDQUFDLElBQUksT0FBaEIsV0FBVyxtQkFBUyxpQkFBaUIsR0FBRTtpQkFDeEM7Z0JBQ0QsSUFBSSxnQkFBYyxHQUFVLEVBQUUsQ0FBQztnQkFDL0IsSUFBSSxhQUFhLENBQUMsWUFBWSxDQUFDLEVBQUU7b0JBQy9CLGdCQUFjLEdBQUcsUUFBUSxDQUFDLElBQUksRUFBRSxhQUFhLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQztvQkFDN0QsSUFBSSxnQkFBYyxFQUFFO3dCQUNsQixXQUFXLENBQUMsSUFBSSxPQUFoQixXQUFXLG1CQUFTLGdCQUFjLEdBQUU7cUJBQ3JDO2lCQUNGO2dCQUNELElBQUksVUFBVSxJQUFJLENBQUMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxhQUFhLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQztvQkFDaEUsSUFBSSxDQUFDLGVBQWUsQ0FBQyxhQUFhLENBQUMsVUFBVSxDQUFDLFFBQVEsQ0FBQyxFQUFFO29CQUMzRCxJQUFNLE9BQU8sR0FBRyxJQUFJLENBQUMsZUFBZSxDQUFDLGNBQWMsQ0FBQyxVQUFVLENBQUMsQ0FBQztvQkFDaEUsSUFBSSxPQUFPLElBQUksT0FBTyxDQUFDLElBQUksRUFBRTt3QkFDM0IsSUFBTSx1QkFBdUIsR0FDekIsSUFBSSxDQUFDLHVDQUF1QyxDQUFDLEdBQUcsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLFdBQWEsQ0FBRyxDQUFDO3dCQUNuRixJQUFNLHlCQUF5QixHQUFHLHVCQUF1QixDQUFDLElBQUksQ0FDMUQsVUFBQyxZQUFZLElBQUssT0FBQSxnQkFBYyxDQUFDLElBQUksQ0FBQyxVQUFBLEdBQUcsSUFBSSxPQUFBLFlBQVksQ0FBQyxRQUFRLENBQUMsR0FBRyxDQUFDLEVBQTFCLENBQTBCLENBQUMsRUFBdEQsQ0FBc0QsQ0FBQyxDQUFDO3dCQUM5RSxJQUFJLENBQUMseUJBQXlCLEVBQUU7NEJBQzlCLElBQUksQ0FBQyxXQUFXLENBQ1osbUJBQW1CLENBQ2YsYUFBYSxDQUNULFdBQVMsSUFBSSxDQUFDLElBQUksWUFBTyxJQUFJLENBQUMsUUFBUSx3QkFBbUIscUNBQWtCLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxXQUFZLENBQUMsbUVBQWdFOzRCQUN0SyxhQUFhLENBQUMsU0FBUyxFQUN2QixrQkFBZ0IsdUJBQXVCLENBQUMsR0FBRyxDQUFDLFVBQUMsSUFBSSxJQUFLLE9BQUEsSUFBSSxDQUFDLGNBQWMsRUFBbkIsQ0FBbUIsQ0FBQyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsNEJBQXlCLENBQUMsRUFDckgsSUFBSSxDQUFDLEVBQ1QsSUFBSSxDQUFDLENBQUM7eUJBQ1g7cUJBQ0Y7aUJBQ0Y7Z0JBQ0QsZUFBZSxDQUFDLEdBQUcsQ0FBQyxJQUFJLEVBQUUsV0FBVyxDQUFDLE1BQU0sQ0FBQyxVQUFBLEdBQUcsSUFBSSxPQUFBLENBQUMsQ0FBQyxHQUFHLEVBQUwsQ0FBSyxDQUFDLENBQUMsQ0FBQzthQUM3RDtZQUNELE9BQU8sV0FBVyxDQUFDO1FBQ3JCLENBQUM7UUFFTSxzQ0FBWSxHQUFuQixVQUFvQixJQUFrQjtZQUF0QyxpQkE4QkM7WUE3QkMsSUFBSSxZQUFZLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDaEQsSUFBSSxDQUFDLFlBQVksRUFBRTtnQkFDakIsSUFBTSxhQUFhLEdBQUcsSUFBSSxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFDakQsWUFBWSxHQUFHLEVBQUUsQ0FBQztnQkFDbEIsSUFBTSxVQUFVLEdBQUcsSUFBSSxDQUFDLGNBQWMsQ0FBQyxJQUFJLEVBQUUsYUFBYSxDQUFDLENBQUM7Z0JBQzVELElBQUksVUFBVSxFQUFFO29CQUNkLElBQU0sb0JBQWtCLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxVQUFVLENBQUMsQ0FBQztvQkFDekQsTUFBTSxDQUFDLElBQUksQ0FBQyxvQkFBa0IsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFDLFVBQVU7d0JBQ2pELFlBQWMsQ0FBQyxVQUFVLENBQUMsR0FBRyxvQkFBa0IsQ0FBQyxVQUFVLENBQUMsQ0FBQztvQkFDOUQsQ0FBQyxDQUFDLENBQUM7aUJBQ0o7Z0JBRUQsSUFBTSxTQUFPLEdBQUcsYUFBYSxDQUFDLFNBQVMsQ0FBQyxJQUFJLEVBQUUsQ0FBQztnQkFDL0MsTUFBTSxDQUFDLElBQUksQ0FBQyxTQUFPLENBQUMsQ0FBQyxPQUFPLENBQUMsVUFBQyxRQUFRO29CQUNwQyxJQUFNLFFBQVEsR0FBRyxTQUFPLENBQUMsUUFBUSxDQUFDLENBQUM7b0JBQ25DLElBQU0sSUFBSSxHQUFXLFFBQVM7eUJBQ1osSUFBSSxDQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLFlBQVksQ0FBQyxJQUFJLFVBQVUsSUFBSSxDQUFDLENBQUMsWUFBWSxDQUFDLElBQUksUUFBUSxFQUE1RCxDQUE0RCxDQUFDLENBQUM7b0JBQzFGLElBQU0sVUFBVSxHQUFVLEVBQUUsQ0FBQztvQkFDN0IsSUFBSSxZQUFjLENBQUMsUUFBUSxDQUFDLEVBQUU7d0JBQzVCLFVBQVUsQ0FBQyxJQUFJLE9BQWYsVUFBVSxtQkFBUyxZQUFjLENBQUMsUUFBUSxDQUFDLEdBQUU7cUJBQzlDO29CQUNELFlBQWMsQ0FBQyxRQUFRLENBQUMsR0FBRyxVQUFVLENBQUM7b0JBQ3RDLElBQUksSUFBSSxJQUFJLElBQUksQ0FBQyxZQUFZLENBQUMsRUFBRTt3QkFDOUIsVUFBVSxDQUFDLElBQUksT0FBZixVQUFVLG1CQUFTLEtBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxFQUFFLElBQUksQ0FBQyxZQUFZLENBQUMsQ0FBQyxHQUFFO3FCQUM3RDtnQkFDSCxDQUFDLENBQUMsQ0FBQztnQkFDSCxJQUFJLENBQUMsYUFBYSxDQUFDLEdBQUcsQ0FBQyxJQUFJLEVBQUUsWUFBWSxDQUFDLENBQUM7YUFDNUM7WUFDRCxPQUFPLFlBQVksQ0FBQztRQUN0QixDQUFDO1FBRU0sb0NBQVUsR0FBakIsVUFBa0IsSUFBa0I7WUFBcEMsaUJBMENDO1lBekNDLElBQUksQ0FBQyxDQUFDLElBQUksWUFBWSw0QkFBWSxDQUFDLEVBQUU7Z0JBQ25DLElBQUksQ0FBQyxXQUFXLENBQ1osSUFBSSxLQUFLLENBQUMseUJBQXVCLElBQUksQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLGlDQUE4QixDQUFDLEVBQ3BGLElBQUksQ0FBQyxDQUFDO2dCQUNWLE9BQU8sRUFBRSxDQUFDO2FBQ1g7WUFDRCxJQUFJO2dCQUNGLElBQUksWUFBVSxHQUFHLElBQUksQ0FBQyxjQUFjLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxDQUFDO2dCQUMvQyxJQUFJLENBQUMsWUFBVSxFQUFFO29CQUNmLElBQU0sYUFBYSxHQUFHLElBQUksQ0FBQyxlQUFlLENBQUMsSUFBSSxDQUFDLENBQUM7b0JBQ2pELElBQU0sVUFBVSxHQUFHLElBQUksQ0FBQyxjQUFjLENBQUMsSUFBSSxFQUFFLGFBQWEsQ0FBQyxDQUFDO29CQUM1RCxJQUFNLE9BQU8sR0FBRyxhQUFhLENBQUMsQ0FBQyxDQUFDLGFBQWEsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO29CQUNoRSxJQUFNLFFBQVEsR0FBRyxPQUFPLENBQUMsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDO29CQUN0RCxJQUFJLFFBQVEsRUFBRTt3QkFDWixJQUFNLElBQUksR0FBVyxRQUFTLENBQUMsSUFBSSxDQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLFlBQVksQ0FBQyxJQUFJLGFBQWEsRUFBaEMsQ0FBZ0MsQ0FBQyxDQUFDO3dCQUMzRSxJQUFNLGlCQUFpQixHQUFVLElBQUksQ0FBQyxZQUFZLENBQUMsSUFBSSxFQUFFLENBQUM7d0JBQzFELElBQU0scUJBQW1CLEdBQVUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLHFCQUFxQixDQUFDLElBQUksRUFBRSxDQUFDLENBQUM7d0JBQzFGLFlBQVUsR0FBRyxFQUFFLENBQUM7d0JBQ2hCLGlCQUFpQixDQUFDLE9BQU8sQ0FBQyxVQUFDLFlBQVksRUFBRSxLQUFLOzRCQUM1QyxJQUFNLFlBQVksR0FBVSxFQUFFLENBQUM7NEJBQy9CLElBQU0sU0FBUyxHQUFHLEtBQUksQ0FBQyxXQUFXLENBQUMsSUFBSSxFQUFFLFlBQVksQ0FBQyxDQUFDOzRCQUN2RCxJQUFJLFNBQVM7Z0NBQUUsWUFBWSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQzs0QkFDNUMsSUFBTSxVQUFVLEdBQUcscUJBQW1CLENBQUMsQ0FBQyxDQUFDLHFCQUFtQixDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7NEJBQzNFLElBQUksVUFBVSxFQUFFO2dDQUNkLFlBQVksQ0FBQyxJQUFJLE9BQWpCLFlBQVksbUJBQVMsVUFBVSxHQUFFOzZCQUNsQzs0QkFDRCxZQUFZLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxDQUFDO3dCQUNsQyxDQUFDLENBQUMsQ0FBQztxQkFDSjt5QkFBTSxJQUFJLFVBQVUsRUFBRTt3QkFDckIsWUFBVSxHQUFHLElBQUksQ0FBQyxVQUFVLENBQUMsVUFBVSxDQUFDLENBQUM7cUJBQzFDO29CQUNELElBQUksQ0FBQyxZQUFVLEVBQUU7d0JBQ2YsWUFBVSxHQUFHLEVBQUUsQ0FBQztxQkFDakI7b0JBQ0QsSUFBSSxDQUFDLGNBQWMsQ0FBQyxHQUFHLENBQUMsSUFBSSxFQUFFLFlBQVUsQ0FBQyxDQUFDO2lCQUMzQztnQkFDRCxPQUFPLFlBQVUsQ0FBQzthQUNuQjtZQUFDLE9BQU8sQ0FBQyxFQUFFO2dCQUNWLE9BQU8sQ0FBQyxLQUFLLENBQUMsb0JBQWtCLElBQUksQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLG9CQUFlLENBQUcsQ0FBQyxDQUFDO2dCQUN4RSxNQUFNLENBQUMsQ0FBQzthQUNUO1FBQ0gsQ0FBQztRQUVPLHNDQUFZLEdBQXBCLFVBQXFCLElBQVM7WUFDNUIsSUFBSSxXQUFXLEdBQUcsSUFBSSxDQUFDLFdBQVcsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDN0MsSUFBSSxDQUFDLFdBQVcsRUFBRTtnQkFDaEIsSUFBTSxhQUFhLEdBQUcsSUFBSSxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFDakQsV0FBVyxHQUFHLEVBQUUsQ0FBQztnQkFDakIsSUFBTSxVQUFVLEdBQUcsSUFBSSxDQUFDLGNBQWMsQ0FBQyxJQUFJLEVBQUUsYUFBYSxDQUFDLENBQUM7Z0JBQzVELElBQUksVUFBVSxFQUFFO29CQUNkLElBQU0sbUJBQWlCLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxVQUFVLENBQUMsQ0FBQztvQkFDeEQsTUFBTSxDQUFDLElBQUksQ0FBQyxtQkFBaUIsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFDLFVBQVU7d0JBQ2hELFdBQWEsQ0FBQyxVQUFVLENBQUMsR0FBRyxtQkFBaUIsQ0FBQyxVQUFVLENBQUMsQ0FBQztvQkFDNUQsQ0FBQyxDQUFDLENBQUM7aUJBQ0o7Z0JBRUQsSUFBTSxTQUFPLEdBQUcsYUFBYSxDQUFDLFNBQVMsQ0FBQyxJQUFJLEVBQUUsQ0FBQztnQkFDL0MsTUFBTSxDQUFDLElBQUksQ0FBQyxTQUFPLENBQUMsQ0FBQyxPQUFPLENBQUMsVUFBQyxRQUFRO29CQUNwQyxJQUFNLFFBQVEsR0FBRyxTQUFPLENBQUMsUUFBUSxDQUFDLENBQUM7b0JBQ25DLElBQU0sUUFBUSxHQUFXLFFBQVMsQ0FBQyxJQUFJLENBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxDQUFDLENBQUMsWUFBWSxDQUFDLElBQUksUUFBUSxFQUEzQixDQUEyQixDQUFDLENBQUM7b0JBQzFFLFdBQWEsQ0FBQyxRQUFRLENBQUMsR0FBRyxXQUFhLENBQUMsUUFBUSxDQUFDLElBQUksUUFBUSxDQUFDO2dCQUNoRSxDQUFDLENBQUMsQ0FBQztnQkFDSCxJQUFJLENBQUMsV0FBVyxDQUFDLEdBQUcsQ0FBQyxJQUFJLEVBQUUsV0FBVyxDQUFDLENBQUM7YUFDekM7WUFDRCxPQUFPLFdBQVcsQ0FBQztRQUNyQixDQUFDO1FBRU8sd0NBQWMsR0FBdEIsVUFBdUIsSUFBa0I7WUFDdkMsSUFBSSxhQUFhLEdBQUcsSUFBSSxDQUFDLFdBQVcsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDL0MsSUFBSSxDQUFDLGFBQWEsRUFBRTtnQkFDbEIsSUFBTSxhQUFhLEdBQUcsSUFBSSxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFDakQsSUFBTSxnQkFBZ0IsR0FBRyxhQUFhLENBQUMsU0FBUyxDQUFDLElBQUksRUFBRSxDQUFDO2dCQUN4RCxhQUFhLEdBQUcsTUFBTSxDQUFDLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxDQUFDO2dCQUM5QyxJQUFJLENBQUMsV0FBVyxDQUFDLEdBQUcsQ0FBQyxJQUFJLEVBQUUsYUFBYSxDQUFDLENBQUM7YUFDM0M7WUFDRCxPQUFPLGFBQWEsQ0FBQztRQUN2QixDQUFDO1FBR08sd0NBQWMsR0FBdEIsVUFBdUIsSUFBa0IsRUFBRSxhQUFrQjtZQUMzRCxJQUFNLFVBQVUsR0FBRyxJQUFJLENBQUMsV0FBVyxDQUFDLElBQUksRUFBRSxhQUFhLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQztZQUNwRSxJQUFJLFVBQVUsWUFBWSw0QkFBWSxFQUFFO2dCQUN0QyxPQUFPLFVBQVUsQ0FBQzthQUNuQjtRQUNILENBQUM7UUFFRCwwQ0FBZ0IsR0FBaEIsVUFBaUIsSUFBUyxFQUFFLFVBQWtCO1lBQzVDLElBQUksQ0FBQyxDQUFDLElBQUksWUFBWSw0QkFBWSxDQUFDLEVBQUU7Z0JBQ25DLElBQUksQ0FBQyxXQUFXLENBQ1osSUFBSSxLQUFLLENBQ0wsK0JBQTZCLElBQUksQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLGlDQUE4QixDQUFDLEVBQ3BGLElBQUksQ0FBQyxDQUFDO2FBQ1g7WUFDRCxJQUFJO2dCQUNGLE9BQU8sQ0FBQyxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsSUFBSSxDQUFDLENBQUMsVUFBVSxDQUFDLENBQUM7YUFDOUM7WUFBQyxPQUFPLENBQUMsRUFBRTtnQkFDVixPQUFPLENBQUMsS0FBSyxDQUFDLG9CQUFrQixJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxvQkFBZSxDQUFHLENBQUMsQ0FBQztnQkFDeEUsTUFBTSxDQUFDLENBQUM7YUFDVDtRQUNILENBQUM7UUFFRCxnQ0FBTSxHQUFOLFVBQU8sSUFBUzs7WUFDZCxJQUFJLENBQUMsQ0FBQyxJQUFJLFlBQVksNEJBQVksQ0FBQyxFQUFFO2dCQUNuQyxJQUFJLENBQUMsV0FBVyxDQUNaLElBQUksS0FBSyxDQUFDLHFCQUFtQixJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxpQ0FBOEIsQ0FBQyxFQUFFLElBQUksQ0FBQyxDQUFDO2dCQUM1RixPQUFPLEVBQUUsQ0FBQzthQUNYO1lBQ0QsSUFBTSxhQUFhLEdBQUcsSUFBSSxDQUFDLGNBQWMsQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUNoRCxJQUFNLE1BQU0sR0FBa0MsRUFBRSxDQUFDOztnQkFDakQsS0FBaUIsSUFBQSxrQkFBQSxpQkFBQSxhQUFhLENBQUEsNENBQUEsdUVBQUU7b0JBQTNCLElBQUksTUFBSSwwQkFBQTtvQkFDWCxJQUFJLE1BQUksQ0FBQyxRQUFRLENBQUMsaUJBQWlCLENBQUMsRUFBRTt3QkFDcEMsSUFBSSxRQUFRLEdBQUcsTUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDLEVBQUUsTUFBSSxDQUFDLE1BQU0sR0FBRyxpQkFBaUIsQ0FBQyxNQUFNLENBQUMsQ0FBQzt3QkFDdEUsSUFBSSxLQUFLLFNBQUssQ0FBQzt3QkFDZixJQUFJLFFBQVEsQ0FBQyxRQUFRLENBQUMsTUFBTSxDQUFDLEVBQUU7NEJBQzdCLFFBQVEsR0FBRyxNQUFJLENBQUMsTUFBTSxDQUFDLENBQUMsRUFBRSxRQUFRLENBQUMsTUFBTSxHQUFHLE1BQU0sQ0FBQyxNQUFNLENBQUMsQ0FBQzs0QkFDM0QsS0FBSyxHQUFHLE1BQU0sQ0FBQzt5QkFDaEI7NkJBQU07NEJBQ0wsS0FBSyxHQUFHLElBQUksQ0FBQyxlQUFlLENBQUMsSUFBSSxDQUFDLFFBQVEsRUFBRSxJQUFJLENBQUMsSUFBSSxFQUFFLENBQUMsTUFBSSxDQUFDLENBQUMsQ0FBQzt5QkFDaEU7d0JBQ0QsTUFBTSxDQUFDLFFBQVEsQ0FBQyxHQUFHLEtBQUssQ0FBQztxQkFDMUI7aUJBQ0Y7Ozs7Ozs7OztZQUNELE9BQU8sTUFBTSxDQUFDO1FBQ2hCLENBQUM7UUFFTyx5REFBK0IsR0FBdkMsVUFBd0MsSUFBa0IsRUFBRSxJQUFTO1lBQ25FLElBQUksQ0FBQyxhQUFhLENBQUMsR0FBRyxDQUFDLElBQUksRUFBRSxVQUFDLE9BQXFCLEVBQUUsSUFBVyxJQUFLLFlBQUksSUFBSSxZQUFKLElBQUksNkJBQUksSUFBSSxPQUFoQixDQUFpQixDQUFDLENBQUM7UUFDMUYsQ0FBQztRQUVPLDJDQUFpQixHQUF6QixVQUEwQixJQUFrQixFQUFFLEVBQU87WUFDbkQsSUFBSSxDQUFDLGFBQWEsQ0FBQyxHQUFHLENBQUMsSUFBSSxFQUFFLFVBQUMsT0FBcUIsRUFBRSxJQUFXLElBQUssT0FBQSxFQUFFLENBQUMsS0FBSyxDQUFDLFNBQVMsRUFBRSxJQUFJLENBQUMsRUFBekIsQ0FBeUIsQ0FBQyxDQUFDO1FBQ2xHLENBQUM7UUFFTyxpREFBdUIsR0FBL0I7WUFDRSxJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLFlBQVksQ0FBQyxFQUFFLHVCQUFnQixDQUFDLENBQUM7WUFDeEUsSUFBSSxDQUFDLGNBQWMsR0FBRyxJQUFJLENBQUMsZUFBZSxDQUFDLFlBQVksRUFBRSxnQkFBZ0IsQ0FBQyxDQUFDO1lBQzNFLElBQUksQ0FBQyxXQUFXLEdBQUcsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsYUFBYSxDQUFDLENBQUM7WUFDckUsSUFBSSxDQUFDLE1BQU0sR0FBRyxJQUFJLENBQUMsa0JBQWtCLENBQUMsY0FBYyxFQUFFLFFBQVEsQ0FBQyxDQUFDO1lBQ2hFLElBQUksQ0FBQyw0QkFBNEI7Z0JBQzdCLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLDhCQUE4QixDQUFDLENBQUM7WUFFdkUsSUFBSSxDQUFDLCtCQUErQixDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLE1BQU0sQ0FBQyxFQUFFLGlCQUFVLENBQUMsQ0FBQztZQUM3RixJQUFJLENBQUMsK0JBQStCLENBQUMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsTUFBTSxDQUFDLEVBQUUsaUJBQVUsQ0FBQyxDQUFDO1lBQzdGLElBQUksQ0FBQywrQkFBK0IsQ0FDaEMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsVUFBVSxDQUFDLEVBQUUscUJBQWMsQ0FBQyxDQUFDO1lBQ3BFLElBQUksQ0FBQywrQkFBK0IsQ0FDaEMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsUUFBUSxDQUFDLEVBQUUsbUJBQVksQ0FBQyxDQUFDO1lBQ2hFLElBQUksQ0FBQywrQkFBK0IsQ0FDaEMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsVUFBVSxDQUFDLEVBQUUscUJBQWMsQ0FBQyxDQUFDO1lBQ3BFLElBQUksQ0FBQywrQkFBK0IsQ0FDaEMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsV0FBVyxDQUFDLEVBQUUsc0JBQWUsQ0FBQyxDQUFDO1lBQ3RFLElBQUksQ0FBQywrQkFBK0IsQ0FDaEMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsY0FBYyxDQUFDLEVBQUUseUJBQWtCLENBQUMsQ0FBQztZQUM1RSxJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLGlCQUFpQixDQUFDLEVBQUUsNEJBQXFCLENBQUMsQ0FBQztZQUNsRixJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLFdBQVcsQ0FBQyxFQUFFLHNCQUFlLENBQUMsQ0FBQztZQUN0RSxJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLGNBQWMsQ0FBQyxFQUFFLHlCQUFrQixDQUFDLENBQUM7WUFDNUUsSUFBSSxDQUFDLCtCQUErQixDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLE9BQU8sQ0FBQyxFQUFFLGtCQUFXLENBQUMsQ0FBQztZQUMvRixJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLFFBQVEsQ0FBQyxFQUFFLG1CQUFZLENBQUMsQ0FBQztZQUNoRSxJQUFJLENBQUMsK0JBQStCLENBQUMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsTUFBTSxDQUFDLEVBQUUsaUJBQVUsQ0FBQyxDQUFDO1lBQzdGLElBQUksQ0FBQywrQkFBK0IsQ0FDaEMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxZQUFZLEVBQUUsYUFBYSxDQUFDLEVBQUUsd0JBQWlCLENBQUMsQ0FBQztZQUMxRSxJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLGNBQWMsQ0FBQyxFQUFFLHlCQUFrQixDQUFDLENBQUM7WUFDNUUsSUFBSSxDQUFDLCtCQUErQixDQUNoQyxJQUFJLENBQUMsZUFBZSxDQUFDLFlBQVksRUFBRSxXQUFXLENBQUMsRUFBRSxzQkFBZSxDQUFDLENBQUM7WUFDdEUsSUFBSSxDQUFDLCtCQUErQixDQUNoQyxJQUFJLENBQUMsZUFBZSxDQUFDLFlBQVksRUFBRSxXQUFXLENBQUMsRUFBRSxzQkFBZSxDQUFDLENBQUM7WUFDdEUsSUFBSSxDQUFDLCtCQUErQixDQUNoQyxJQUFJLENBQUMsZUFBZSxDQUFDLFlBQVksRUFBRSxVQUFVLENBQUMsRUFBRSxxQkFBYyxDQUFDLENBQUM7WUFFcEUsdUVBQXVFO1lBQ3ZFLElBQUksQ0FBQywrQkFBK0IsQ0FBQyxJQUFJLENBQUMsZUFBZSxDQUFDLFlBQVksRUFBRSxNQUFNLENBQUMsRUFBRSxpQkFBVSxDQUFDLENBQUM7WUFDN0YsSUFBSSxDQUFDLCtCQUErQixDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLE1BQU0sQ0FBQyxFQUFFLGlCQUFVLENBQUMsQ0FBQztZQUM3RixJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLFVBQVUsQ0FBQyxFQUFFLHFCQUFjLENBQUMsQ0FBQztZQUNwRSxJQUFJLENBQUMsK0JBQStCLENBQ2hDLElBQUksQ0FBQyxlQUFlLENBQUMsWUFBWSxFQUFFLFVBQVUsQ0FBQyxFQUFFLHFCQUFjLENBQUMsQ0FBQztRQUN0RSxDQUFDO1FBRUQ7Ozs7OztXQU1HO1FBQ0gseUNBQWUsR0FBZixVQUFnQixlQUF1QixFQUFFLElBQVksRUFBRSxPQUFrQjtZQUN2RSxPQUFPLElBQUksQ0FBQyxjQUFjLENBQUMsZUFBZSxDQUFDLGVBQWUsRUFBRSxJQUFJLEVBQUUsT0FBTyxDQUFDLENBQUM7UUFDN0UsQ0FBQztRQUVEOztXQUVHO1FBQ0sscUNBQVcsR0FBbkIsVUFBb0IsT0FBcUIsRUFBRSxLQUFVO1lBQ25ELElBQU0sZ0JBQWdCLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQztZQUM1QyxJQUFJLENBQUMsYUFBYSxHQUFHLFVBQUMsS0FBVSxFQUFFLFFBQWlCLElBQU0sQ0FBQyxDQUFDO1lBQzNELElBQU0sTUFBTSxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsT0FBTyxFQUFFLEtBQUssQ0FBQyxDQUFDO1lBQzdDLElBQUksQ0FBQyxhQUFhLEdBQUcsZ0JBQWdCLENBQUM7WUFDdEMsT0FBTyxNQUFNLENBQUM7UUFDaEIsQ0FBQztRQUVELGdCQUFnQjtRQUNULGtDQUFRLEdBQWYsVUFBZ0IsT0FBcUIsRUFBRSxLQUFVLEVBQUUsSUFBcUI7WUFBckIscUJBQUEsRUFBQSxZQUFxQjtZQUN0RSxJQUFNLElBQUksR0FBRyxJQUFJLENBQUM7WUFDbEIsSUFBSSxLQUFLLEdBQUcsWUFBWSxDQUFDLEtBQUssQ0FBQztZQUMvQixJQUFNLE9BQU8sR0FBRyxJQUFJLEdBQUcsRUFBeUIsQ0FBQztZQUNqRCxJQUFNLFdBQVcsR0FBRyxPQUFPLENBQUM7WUFFNUIsU0FBUyxpQkFBaUIsQ0FDdEIsT0FBcUIsRUFBRSxLQUFVLEVBQUUsS0FBYSxFQUFFLFVBQWtCO2dCQUN0RSxTQUFTLHFCQUFxQixDQUFDLFlBQTBCO29CQUN2RCxJQUFNLGNBQWMsR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLGFBQWEsQ0FBQyxZQUFZLENBQUMsQ0FBQztvQkFDdkUsT0FBTyxjQUFjLENBQUMsQ0FBQyxDQUFDLGNBQWMsQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztnQkFDekQsQ0FBQztnQkFFRCxTQUFTLGVBQWUsQ0FBQyxLQUFVO29CQUNqQyxPQUFPLGlCQUFpQixDQUFDLE9BQU8sRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLENBQUMsQ0FBQyxDQUFDO2dCQUNyRCxDQUFDO2dCQUVELFNBQVMsY0FBYyxDQUFDLEtBQVU7b0JBQ2hDLE9BQU8saUJBQWlCLENBQUMsT0FBTyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsVUFBVSxHQUFHLENBQUMsQ0FBQyxDQUFDO2dCQUNsRSxDQUFDO2dCQUVELFNBQVMsY0FBYyxDQUFDLGFBQTJCLEVBQUUsS0FBVTtvQkFDN0QsSUFBSSxhQUFhLEtBQUssT0FBTyxFQUFFO3dCQUM3Qix3RUFBd0U7d0JBQ3hFLE9BQU8saUJBQWlCLENBQUMsYUFBYSxFQUFFLEtBQUssRUFBRSxLQUFLLEdBQUcsQ0FBQyxFQUFFLFVBQVUsQ0FBQyxDQUFDO3FCQUN2RTtvQkFDRCxJQUFJO3dCQUNGLE9BQU8saUJBQWlCLENBQUMsYUFBYSxFQUFFLEtBQUssRUFBRSxLQUFLLEdBQUcsQ0FBQyxFQUFFLFVBQVUsQ0FBQyxDQUFDO3FCQUN2RTtvQkFBQyxPQUFPLENBQUMsRUFBRTt3QkFDVixJQUFJLGVBQWUsQ0FBQyxDQUFDLENBQUMsRUFBRTs0QkFDdEIsd0ZBQXdGOzRCQUN4RixRQUFROzRCQUNSLDJCQUEyQjs0QkFDM0IsSUFBTSxVQUFVLEdBQUcsQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsZUFBZSxHQUFHLENBQUMsQ0FBQyxNQUFRLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQyxDQUFDLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQyxDQUFDOzRCQUN4RixJQUFNLE9BQU8sR0FBRyxNQUFJLGFBQWEsQ0FBQyxJQUFJLFVBQUssVUFBWSxDQUFDOzRCQUN4RCxJQUFNLEtBQUssR0FBRyxFQUFDLE9BQU8sRUFBRSxPQUFPLEVBQUUsUUFBUSxFQUFFLENBQUMsQ0FBQyxRQUFRLEVBQUUsSUFBSSxFQUFFLENBQUMsQ0FBQyxLQUFLLEVBQUMsQ0FBQzs0QkFDdEUsc0ZBQXNGOzRCQUN0RiwwQ0FBMEM7NEJBQzFDLElBQUksQ0FBQyxLQUFLLENBQ047Z0NBQ0UsT0FBTyxFQUFFLENBQUMsQ0FBQyxPQUFPO2dDQUNsQixNQUFNLEVBQUUsQ0FBQyxDQUFDLE1BQU07Z0NBQ2hCLE9BQU8sRUFBRSxDQUFDLENBQUMsT0FBTyxFQUFFLEtBQUssT0FBQTtnQ0FDekIsTUFBTSxFQUFFLGFBQWE7NkJBQ3RCLEVBQ0QsT0FBTyxDQUFDLENBQUM7eUJBQ2Q7NkJBQU07NEJBQ0wsb0NBQW9DOzRCQUNwQyxNQUFNLENBQUMsQ0FBQzt5QkFDVDtxQkFDRjtnQkFDSCxDQUFDO2dCQUVELFNBQVMsWUFBWSxDQUNqQixjQUE0QixFQUFFLGNBQW1CLEVBQUUsSUFBVyxFQUFFLGdCQUFxQjtvQkFDdkYsSUFBSSxjQUFjLElBQUksY0FBYyxDQUFDLFlBQVksQ0FBQyxJQUFJLFVBQVUsRUFBRTt3QkFDaEUsSUFBSSxPQUFPLENBQUMsR0FBRyxDQUFDLGNBQWMsQ0FBQyxFQUFFOzRCQUMvQixJQUFJLENBQUMsS0FBSyxDQUNOO2dDQUNFLE9BQU8sRUFBRSw0QkFBNEI7Z0NBQ3JDLE9BQU8sRUFBRSxhQUFXLGNBQWMsQ0FBQyxJQUFJLGtCQUFlO2dDQUN0RCxLQUFLLEVBQUUsY0FBYzs2QkFDdEIsRUFDRCxjQUFjLENBQUMsQ0FBQzt5QkFDckI7d0JBQ0QsSUFBSTs0QkFDRixJQUFNLE9BQUssR0FBRyxjQUFjLENBQUMsT0FBTyxDQUFDLENBQUM7NEJBQ3RDLElBQUksT0FBSyxJQUFJLENBQUMsS0FBSyxJQUFJLENBQUMsSUFBSSxPQUFLLENBQUMsVUFBVSxJQUFJLE9BQU8sQ0FBQyxFQUFFO2dDQUN4RCxJQUFNLFVBQVUsR0FBYSxjQUFjLENBQUMsWUFBWSxDQUFDLENBQUM7Z0NBQzFELElBQU0sUUFBUSxHQUFVLGNBQWMsQ0FBQyxRQUFRLENBQUM7Z0NBQ2hELElBQUksR0FBRyxJQUFJLENBQUMsR0FBRyxDQUFDLFVBQUEsR0FBRyxJQUFJLE9BQUEsY0FBYyxDQUFDLE9BQU8sRUFBRSxHQUFHLENBQUMsRUFBNUIsQ0FBNEIsQ0FBQztxQ0FDeEMsR0FBRyxDQUFDLFVBQUEsR0FBRyxJQUFJLE9BQUEsWUFBWSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLEdBQUcsRUFBbkMsQ0FBbUMsQ0FBQyxDQUFDO2dDQUM1RCxJQUFJLFFBQVEsSUFBSSxRQUFRLENBQUMsTUFBTSxHQUFHLElBQUksQ0FBQyxNQUFNLEVBQUU7b0NBQzdDLElBQUksQ0FBQyxJQUFJLE9BQVQsSUFBSSxtQkFBUyxRQUFRLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQyxHQUFHLENBQUMsVUFBQyxLQUFVLElBQUssT0FBQSxRQUFRLENBQUMsS0FBSyxDQUFDLEVBQWYsQ0FBZSxDQUFDLEdBQUU7aUNBQ2hGO2dDQUNELE9BQU8sQ0FBQyxHQUFHLENBQUMsY0FBYyxFQUFFLElBQUksQ0FBQyxDQUFDO2dDQUNsQyxJQUFNLGFBQWEsR0FBRyxZQUFZLENBQUMsS0FBSyxFQUFFLENBQUM7Z0NBQzNDLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxVQUFVLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO29DQUMxQyxhQUFhLENBQUMsTUFBTSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsRUFBRSxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztpQ0FDOUM7Z0NBQ0QsSUFBTSxRQUFRLEdBQUcsS0FBSyxDQUFDO2dDQUN2QixJQUFJLFFBQVcsQ0FBQztnQ0FDaEIsSUFBSTtvQ0FDRixLQUFLLEdBQUcsYUFBYSxDQUFDLElBQUksRUFBRSxDQUFDO29DQUM3QixRQUFNLEdBQUcsY0FBYyxDQUFDLGNBQWMsRUFBRSxPQUFLLENBQUMsQ0FBQztpQ0FDaEQ7d0NBQVM7b0NBQ1IsS0FBSyxHQUFHLFFBQVEsQ0FBQztpQ0FDbEI7Z0NBQ0QsT0FBTyxRQUFNLENBQUM7NkJBQ2Y7eUJBQ0Y7Z0NBQVM7NEJBQ1IsT0FBTyxDQUFDLE1BQU0sQ0FBQyxjQUFjLENBQUMsQ0FBQzt5QkFDaEM7cUJBQ0Y7b0JBRUQsSUFBSSxLQUFLLEtBQUssQ0FBQyxFQUFFO3dCQUNmLHNGQUFzRjt3QkFDdEYsbUZBQW1GO3dCQUNuRix1REFBdUQ7d0JBQ3ZELE9BQU8sTUFBTSxDQUFDO3FCQUNmO29CQUNELElBQUksUUFBUSxHQUF1QixTQUFTLENBQUM7b0JBQzdDLElBQUksZ0JBQWdCLElBQUksZ0JBQWdCLENBQUMsVUFBVSxJQUFJLFVBQVUsRUFBRTt3QkFDakUsSUFBTSxJQUFJLEdBQUcsZ0JBQWdCLENBQUMsSUFBSSxDQUFDO3dCQUNuQyxJQUFNLFNBQVMsR0FBRyxnQkFBZ0IsQ0FBQyxTQUFTLENBQUM7d0JBQzdDLElBQU0sUUFBUSxHQUFHLGdCQUFnQixDQUFDLFFBQVEsQ0FBQzt3QkFDM0MsSUFBSSxRQUFRLElBQUksSUFBSSxJQUFJLElBQUksSUFBSSxJQUFJLElBQUksU0FBUyxJQUFJLElBQUksRUFBRTs0QkFDekQsUUFBUSxHQUFHLEVBQUMsUUFBUSxVQUFBLEVBQUUsSUFBSSxNQUFBLEVBQUUsTUFBTSxFQUFFLFNBQVMsRUFBQyxDQUFDO3lCQUNoRDtxQkFDRjtvQkFDRCxJQUFJLENBQUMsS0FBSyxDQUNOO3dCQUNFLE9BQU8sRUFBRSwyQkFBMkI7d0JBQ3BDLE9BQU8sRUFBRSxjQUFjO3dCQUN2QixLQUFLLEVBQUUsY0FBYyxFQUFFLFFBQVEsVUFBQTtxQkFDaEMsRUFDRCxPQUFPLENBQUMsQ0FBQztnQkFDZixDQUFDO2dCQUVELFNBQVMsUUFBUSxDQUFDLFVBQWU7O29CQUMvQixJQUFJLFdBQVcsQ0FBQyxVQUFVLENBQUMsRUFBRTt3QkFDM0IsT0FBTyxVQUFVLENBQUM7cUJBQ25CO29CQUNELElBQUksVUFBVSxZQUFZLEtBQUssRUFBRTt3QkFDL0IsSUFBTSxRQUFNLEdBQVUsRUFBRSxDQUFDOzs0QkFDekIsS0FBbUIsSUFBQSxLQUFBLGlCQUFNLFVBQVcsQ0FBQSxnQkFBQSw0QkFBRTtnQ0FBakMsSUFBTSxJQUFJLFdBQUE7Z0NBQ2IsZ0NBQWdDO2dDQUNoQyxJQUFJLElBQUksSUFBSSxJQUFJLENBQUMsVUFBVSxLQUFLLFFBQVEsRUFBRTtvQ0FDeEMsOEVBQThFO29DQUM5RSw2QkFBNkI7b0NBQzdCLElBQU0sV0FBVyxHQUFHLGVBQWUsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUM7b0NBQ3JELElBQUksS0FBSyxDQUFDLE9BQU8sQ0FBQyxXQUFXLENBQUMsRUFBRTs7NENBQzlCLEtBQXlCLElBQUEsK0JBQUEsaUJBQUEsV0FBVyxDQUFBLENBQUEsd0NBQUEsaUVBQUU7Z0RBQWpDLElBQU0sVUFBVSx3QkFBQTtnREFDbkIsUUFBTSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsQ0FBQzs2Q0FDekI7Ozs7Ozs7Ozt3Q0FDRCxTQUFTO3FDQUNWO2lDQUNGO2dDQUNELElBQU0sT0FBSyxHQUFHLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQztnQ0FDN0IsSUFBSSxZQUFZLENBQUMsT0FBSyxDQUFDLEVBQUU7b0NBQ3ZCLFNBQVM7aUNBQ1Y7Z0NBQ0QsUUFBTSxDQUFDLElBQUksQ0FBQyxPQUFLLENBQUMsQ0FBQzs2QkFDcEI7Ozs7Ozs7Ozt3QkFDRCxPQUFPLFFBQU0sQ0FBQztxQkFDZjtvQkFDRCxJQUFJLFVBQVUsWUFBWSw0QkFBWSxFQUFFO3dCQUN0QyxpRkFBaUY7d0JBQ2pGLG1DQUFtQzt3QkFDbkMsSUFBSSxVQUFVLEtBQUssSUFBSSxDQUFDLGNBQWMsSUFBSSxJQUFJLENBQUMsYUFBYSxDQUFDLEdBQUcsQ0FBQyxVQUFVLENBQUM7NEJBQ3hFLENBQUMsVUFBVSxHQUFHLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLEVBQUU7NEJBQ2xELE9BQU8sVUFBVSxDQUFDO3lCQUNuQjs2QkFBTTs0QkFDTCxJQUFNLFlBQVksR0FBRyxVQUFVLENBQUM7NEJBQ2hDLElBQU0sZ0JBQWdCLEdBQUcscUJBQXFCLENBQUMsWUFBWSxDQUFDLENBQUM7NEJBQzdELElBQUksZ0JBQWdCLElBQUksSUFBSSxFQUFFO2dDQUM1QixPQUFPLGNBQWMsQ0FBQyxZQUFZLEVBQUUsZ0JBQWdCLENBQUMsQ0FBQzs2QkFDdkQ7aUNBQU07Z0NBQ0wsT0FBTyxZQUFZLENBQUM7NkJBQ3JCO3lCQUNGO3FCQUNGO29CQUNELElBQUksVUFBVSxFQUFFO3dCQUNkLElBQUksVUFBVSxDQUFDLFlBQVksQ0FBQyxFQUFFOzRCQUM1QixJQUFJLFlBQVksU0FBYyxDQUFDOzRCQUMvQixRQUFRLFVBQVUsQ0FBQyxZQUFZLENBQUMsRUFBRTtnQ0FDaEMsS0FBSyxPQUFPO29DQUNWLElBQUksSUFBSSxHQUFHLFFBQVEsQ0FBQyxVQUFVLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQztvQ0FDeEMsSUFBSSxZQUFZLENBQUMsSUFBSSxDQUFDO3dDQUFFLE9BQU8sSUFBSSxDQUFDO29DQUNwQyxJQUFJLEtBQUssR0FBRyxRQUFRLENBQUMsVUFBVSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUM7b0NBQzFDLElBQUksWUFBWSxDQUFDLEtBQUssQ0FBQzt3Q0FBRSxPQUFPLEtBQUssQ0FBQztvQ0FDdEMsUUFBUSxVQUFVLENBQUMsVUFBVSxDQUFDLEVBQUU7d0NBQzlCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssR0FBRzs0Q0FDTixPQUFPLElBQUksR0FBRyxLQUFLLENBQUM7d0NBQ3RCLEtBQUssR0FBRzs0Q0FDTixPQUFPLElBQUksR0FBRyxLQUFLLENBQUM7d0NBQ3RCLEtBQUssR0FBRzs0Q0FDTixPQUFPLElBQUksR0FBRyxLQUFLLENBQUM7d0NBQ3RCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssS0FBSzs0Q0FDUixPQUFPLElBQUksS0FBSyxLQUFLLENBQUM7d0NBQ3hCLEtBQUssS0FBSzs0Q0FDUixPQUFPLElBQUksS0FBSyxLQUFLLENBQUM7d0NBQ3hCLEtBQUssR0FBRzs0Q0FDTixPQUFPLElBQUksR0FBRyxLQUFLLENBQUM7d0NBQ3RCLEtBQUssR0FBRzs0Q0FDTixPQUFPLElBQUksR0FBRyxLQUFLLENBQUM7d0NBQ3RCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssSUFBSTs0Q0FDUCxPQUFPLElBQUksSUFBSSxLQUFLLENBQUM7d0NBQ3ZCLEtBQUssR0FBRzs0Q0FDTixPQUFPLElBQUksR0FBRyxLQUFLLENBQUM7d0NBQ3RCLEtBQUssR0FBRzs0Q0FDTixPQUFPLElBQUksR0FBRyxLQUFLLENBQUM7d0NBQ3RCLEtBQUssR0FB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RUFBRSxJQUFJLEVBQUUsQ0FBQyxDQUFDLEtBQUssRUFBRSxPQUFPLEVBQUUsQ0FBQyxDQUFDLE9BQU8sRUFBRSxNQUFNLEVBQUUsQ0FBQyxDQUFDLE1BQU0sRUFBQzthQUNoRixDQUFDO1lBQ0YsSUFBTSxNQUFNLEdBQUcsQ0FBQyxDQUFDLE1BQU0sSUFBSSxhQUFhLENBQUMsQ0FBQyxDQUFDLE9BQU8sRUFBRSxDQUFDLENBQUMsT0FBTyxDQUFDLENBQUM7WUFDL0QsT0FBTyxnQ0FBYyxDQUFDLDBCQUEwQixDQUFDLEtBQUssRUFBRSxNQUFNLENBQUMsQ0FBQyxDQUFDO1NBQ2xFO1FBQ0QsT0FBTyxDQUFDLENBQUM7SUFDWCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVTdW1tYXJ5S2luZH0gZnJvbSAnLi4vY29tcGlsZV9tZXRhZGF0YSc7XG5pbXBvcnQge0NvbXBpbGVSZWZsZWN0b3J9IGZyb20gJy4uL2NvbXBpbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7TWV0YWRhdGFGYWN0b3J5LCBjcmVhdGVBdHRyaWJ1dGUsIGNyZWF0ZUNvbXBvbmVudCwgY3JlYXRlQ29udGVudENoaWxkLCBjcmVhdGVDb250ZW50Q2hpbGRyZW4sIGNyZWF0ZURpcmVjdGl2ZSwgY3JlYXRlSG9zdCwgY3JlYXRlSG9zdEJpbmRpbmcsIGNyZWF0ZUhvc3RMaXN0ZW5lciwgY3JlYXRlSW5qZWN0LCBjcmVhdGVJbmplY3RhYmxlLCBjcmVhdGVJbnB1dCwgY3JlYXRlTmdNb2R1bGUsIGNyZWF0ZU9wdGlvbmFsLCBjcmVhdGVPdXRwdXQsIGNyZWF0ZVBpcGUsIGNyZWF0ZVNlbGYsIGNyZWF0ZVNraXBTZWxmLCBjcmVhdGVWaWV3Q2hpbGQsIGNyZWF0ZVZpZXdDaGlsZHJlbn0gZnJvbSAnLi4vY29yZSc7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4uL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7U3VtbWFyeVJlc29sdmVyfSBmcm9tICcuLi9zdW1tYXJ5X3Jlc29sdmVyJztcbmltcG9ydCB7c3ludGF4RXJyb3J9IGZyb20gJy4uL3V0aWwnO1xuXG5pbXBvcnQge0Zvcm1hdHRlZE1lc3NhZ2VDaGFpbiwgZm9ybWF0dGVkRXJyb3J9IGZyb20gJy4vZm9ybWF0dGVkX2Vycm9yJztcbmltcG9ydCB7U3RhdGljU3ltYm9sfSBmcm9tICcuL3N0YXRpY19zeW1ib2wnO1xuaW1wb3J0IHtTdGF0aWNTeW1ib2xSZXNvbHZlcn0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sX3Jlc29sdmVyJztcblxuY29uc3QgQU5HVUxBUl9DT1JFID0gJ0Bhbmd1bGFyL2NvcmUnO1xuY29uc3QgQU5HVUxBUl9ST1VURVIgPSAnQGFuZ3VsYXIvcm91dGVyJztcblxuY29uc3QgSElEREVOX0tFWSA9IC9eXFwkLipcXCQkLztcblxuY29uc3QgSUdOT1JFID0ge1xuICBfX3N5bWJvbGljOiAnaWdub3JlJ1xufTtcblxuY29uc3QgVVNFX1ZBTFVFID0gJ3VzZVZhbHVlJztcbmNvbnN0IFBST1ZJREUgPSAncHJvdmlkZSc7XG5jb25zdCBSRUZFUkVOQ0VfU0VUID0gbmV3IFNldChbVVNFX1ZBTFVFLCAndXNlRmFjdG9yeScsICdkYXRhJywgJ2lkJywgJ2xvYWRDaGlsZHJlbiddKTtcbmNvbnN0IFRZUEVHVUFSRF9QT1NURklYID0gJ1R5cGVHdWFyZCc7XG5jb25zdCBVU0VfSUYgPSAnVXNlSWYnO1xuXG5mdW5jdGlvbiBzaG91bGRJZ25vcmUodmFsdWU6IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gdmFsdWUgJiYgdmFsdWUuX19zeW1ib2xpYyA9PSAnaWdub3JlJztcbn1cblxuLyoqXG4gKiBBIHN0YXRpYyByZWZsZWN0b3IgaW1wbGVtZW50cyBlbm91Z2ggb2YgdGhlIFJlZmxlY3RvciBBUEkgdGhhdCBpcyBuZWNlc3NhcnkgdG8gY29tcGlsZVxuICogdGVtcGxhdGVzIHN0YXRpY2FsbHkuXG4gKi9cbmV4cG9ydCBjbGFzcyBTdGF0aWNSZWZsZWN0b3IgaW1wbGVtZW50cyBDb21waWxlUmVmbGVjdG9yIHtcbiAgcHJpdmF0ZSBhbm5vdGF0aW9uQ2FjaGUgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgYW55W10+KCk7XG4gIHByaXZhdGUgc2hhbGxvd0Fubm90YXRpb25DYWNoZSA9IG5ldyBNYXA8U3RhdGljU3ltYm9sLCBhbnlbXT4oKTtcbiAgcHJpdmF0ZSBwcm9wZXJ0eUNhY2hlID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIHtba2V5OiBzdHJpbmddOiBhbnlbXX0+KCk7XG4gIHByaXZhdGUgcGFyYW1ldGVyQ2FjaGUgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgYW55W10+KCk7XG4gIHByaXZhdGUgbWV0aG9kQ2FjaGUgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwge1trZXk6IHN0cmluZ106IGJvb2xlYW59PigpO1xuICBwcml2YXRlIHN0YXRpY0NhY2hlID0gbmV3IE1hcDxTdGF0aWNTeW1ib2wsIHN0cmluZ1tdPigpO1xuICBwcml2YXRlIGNvbnZlcnNpb25NYXAgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgKGNvbnRleHQ6IFN0YXRpY1N5bWJvbCwgYXJnczogYW55W10pID0+IGFueT4oKTtcbiAgcHJpdmF0ZSByZXNvbHZlZEV4dGVybmFsUmVmZXJlbmNlcyA9IG5ldyBNYXA8c3RyaW5nLCBTdGF0aWNTeW1ib2w+KCk7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIGluamVjdGlvblRva2VuICE6IFN0YXRpY1N5bWJvbDtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgb3BhcXVlVG9rZW4gITogU3RhdGljU3ltYm9sO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgUk9VVEVTICE6IFN0YXRpY1N5bWJvbDtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgQU5BTFlaRV9GT1JfRU5UUllfQ09NUE9ORU5UUyAhOiBTdGF0aWNTeW1ib2w7XG4gIHByaXZhdGUgYW5ub3RhdGlvbkZvclBhcmVudENsYXNzV2l0aFN1bW1hcnlLaW5kID1cbiAgICAgIG5ldyBNYXA8Q29tcGlsZVN1bW1hcnlLaW5kLCBNZXRhZGF0YUZhY3Rvcnk8YW55PltdPigpO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBzdW1tYXJ5UmVzb2x2ZXI6IFN1bW1hcnlSZXNvbHZlcjxTdGF0aWNTeW1ib2w+LFxuICAgICAgcHJpdmF0ZSBzeW1ib2xSZXNvbHZlcjogU3RhdGljU3ltYm9sUmVzb2x2ZXIsXG4gICAgICBrbm93bk1ldGFkYXRhQ2xhc3Nlczoge25hbWU6IHN0cmluZywgZmlsZVBhdGg6IHN0cmluZywgY3RvcjogYW55fVtdID0gW10sXG4gICAgICBrbm93bk1ldGFkYXRhRnVuY3Rpb25zOiB7bmFtZTogc3RyaW5nLCBmaWxlUGF0aDogc3RyaW5nLCBmbjogYW55fVtdID0gW10sXG4gICAgICBwcml2YXRlIGVycm9yUmVjb3JkZXI/OiAoZXJyb3I6IGFueSwgZmlsZU5hbWU/OiBzdHJpbmcpID0+IHZvaWQpIHtcbiAgICB0aGlzLmluaXRpYWxpemVDb252ZXJzaW9uTWFwKCk7XG4gICAga25vd25NZXRhZGF0YUNsYXNzZXMuZm9yRWFjaChcbiAgICAgICAgKGtjKSA9PiB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IoXG4gICAgICAgICAgICB0aGlzLmdldFN0YXRpY1N5bWJvbChrYy5maWxlUGF0aCwga2MubmFtZSksIGtjLmN0b3IpKTtcbiAgICBrbm93bk1ldGFkYXRhRnVuY3Rpb25zLmZvckVhY2goXG4gICAgICAgIChrZikgPT4gdGhpcy5fcmVnaXN0ZXJGdW5jdGlvbih0aGlzLmdldFN0YXRpY1N5bWJvbChrZi5maWxlUGF0aCwga2YubmFtZSksIGtmLmZuKSk7XG4gICAgdGhpcy5hbm5vdGF0aW9uRm9yUGFyZW50Q2xhc3NXaXRoU3VtbWFyeUtpbmQuc2V0KFxuICAgICAgICBDb21waWxlU3VtbWFyeUtpbmQuRGlyZWN0aXZlLCBbY3JlYXRlRGlyZWN0aXZlLCBjcmVhdGVDb21wb25lbnRdKTtcbiAgICB0aGlzLmFubm90YXRpb25Gb3JQYXJlbnRDbGFzc1dpdGhTdW1tYXJ5S2luZC5zZXQoQ29tcGlsZVN1bW1hcnlLaW5kLlBpcGUsIFtjcmVhdGVQaXBlXSk7XG4gICAgdGhpcy5hbm5vdGF0aW9uRm9yUGFyZW50Q2xhc3NXaXRoU3VtbWFyeUtpbmQuc2V0KENvbXBpbGVTdW1tYXJ5S2luZC5OZ01vZHVsZSwgW2NyZWF0ZU5nTW9kdWxlXSk7XG4gICAgdGhpcy5hbm5vdGF0aW9uRm9yUGFyZW50Q2xhc3NXaXRoU3VtbWFyeUtpbmQuc2V0KFxuICAgICAgICBDb21waWxlU3VtbWFyeUtpbmQuSW5qZWN0YWJsZSxcbiAgICAgICAgW2NyZWF0ZUluamVjdGFibGUsIGNyZWF0ZVBpcGUsIGNyZWF0ZURpcmVjdGl2ZSwgY3JlYXRlQ29tcG9uZW50LCBjcmVhdGVOZ01vZHVsZV0pO1xuICB9XG5cbiAgY29tcG9uZW50TW9kdWxlVXJsKHR5cGVPckZ1bmM6IFN0YXRpY1N5bWJvbCk6IHN0cmluZyB7XG4gICAgY29uc3Qgc3RhdGljU3ltYm9sID0gdGhpcy5maW5kU3ltYm9sRGVjbGFyYXRpb24odHlwZU9yRnVuYyk7XG4gICAgcmV0dXJuIHRoaXMuc3ltYm9sUmVzb2x2ZXIuZ2V0UmVzb3VyY2VQYXRoKHN0YXRpY1N5bWJvbCk7XG4gIH1cblxuICByZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UocmVmOiBvLkV4dGVybmFsUmVmZXJlbmNlLCBjb250YWluaW5nRmlsZT86IHN0cmluZyk6IFN0YXRpY1N5bWJvbCB7XG4gICAgbGV0IGtleTogc3RyaW5nfHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgICBpZiAoIWNvbnRhaW5pbmdGaWxlKSB7XG4gICAgICBrZXkgPSBgJHtyZWYubW9kdWxlTmFtZX06JHtyZWYubmFtZX1gO1xuICAgICAgY29uc3QgZGVjbGFyYXRpb25TeW1ib2wgPSB0aGlzLnJlc29sdmVkRXh0ZXJuYWxSZWZlcmVuY2VzLmdldChrZXkpO1xuICAgICAgaWYgKGRlY2xhcmF0aW9uU3ltYm9sKSByZXR1cm4gZGVjbGFyYXRpb25TeW1ib2w7XG4gICAgfVxuICAgIGNvbnN0IHJlZlN5bWJvbCA9XG4gICAgICAgIHRoaXMuc3ltYm9sUmVzb2x2ZXIuZ2V0U3ltYm9sQnlNb2R1bGUocmVmLm1vZHVsZU5hbWUgISwgcmVmLm5hbWUgISwgY29udGFpbmluZ0ZpbGUpO1xuICAgIGNvbnN0IGRlY2xhcmF0aW9uU3ltYm9sID0gdGhpcy5maW5kU3ltYm9sRGVjbGFyYXRpb24ocmVmU3ltYm9sKTtcbiAgICBpZiAoIWNvbnRhaW5pbmdGaWxlKSB7XG4gICAgICB0aGlzLnN5bWJvbFJlc29sdmVyLnJlY29yZE1vZHVsZU5hbWVGb3JGaWxlTmFtZShyZWZTeW1ib2wuZmlsZVBhdGgsIHJlZi5tb2R1bGVOYW1lICEpO1xuICAgICAgdGhpcy5zeW1ib2xSZXNvbHZlci5yZWNvcmRJbXBvcnRBcyhkZWNsYXJhdGlvblN5bWJvbCwgcmVmU3ltYm9sKTtcbiAgICB9XG4gICAgaWYgKGtleSkge1xuICAgICAgdGhpcy5yZXNvbHZlZEV4dGVybmFsUmVmZXJlbmNlcy5zZXQoa2V5LCBkZWNsYXJhdGlvblN5bWJvbCk7XG4gICAgfVxuICAgIHJldHVybiBkZWNsYXJhdGlvblN5bWJvbDtcbiAgfVxuXG4gIGZpbmREZWNsYXJhdGlvbihtb2R1bGVVcmw6IHN0cmluZywgbmFtZTogc3RyaW5nLCBjb250YWluaW5nRmlsZT86IHN0cmluZyk6IFN0YXRpY1N5bWJvbCB7XG4gICAgcmV0dXJuIHRoaXMuZmluZFN5bWJvbERlY2xhcmF0aW9uKFxuICAgICAgICB0aGlzLnN5bWJvbFJlc29sdmVyLmdldFN5bWJvbEJ5TW9kdWxlKG1vZHVsZVVybCwgbmFtZSwgY29udGFpbmluZ0ZpbGUpKTtcbiAgfVxuXG4gIHRyeUZpbmREZWNsYXJhdGlvbihtb2R1bGVVcmw6IHN0cmluZywgbmFtZTogc3RyaW5nLCBjb250YWluaW5nRmlsZT86IHN0cmluZyk6IFN0YXRpY1N5bWJvbCB7XG4gICAgcmV0dXJuIHRoaXMuc3ltYm9sUmVzb2x2ZXIuaWdub3JlRXJyb3JzRm9yKFxuICAgICAgICAoKSA9PiB0aGlzLmZpbmREZWNsYXJhdGlvbihtb2R1bGVVcmwsIG5hbWUsIGNvbnRhaW5pbmdGaWxlKSk7XG4gIH1cblxuICBmaW5kU3ltYm9sRGVjbGFyYXRpb24oc3ltYm9sOiBTdGF0aWNTeW1ib2wpOiBTdGF0aWNTeW1ib2wge1xuICAgIGNvbnN0IHJlc29sdmVkU3ltYm9sID0gdGhpcy5zeW1ib2xSZXNvbHZlci5yZXNvbHZlU3ltYm9sKHN5bWJvbCk7XG4gICAgaWYgKHJlc29sdmVkU3ltYm9sKSB7XG4gICAgICBsZXQgcmVzb2x2ZWRNZXRhZGF0YSA9IHJlc29sdmVkU3ltYm9sLm1ldGFkYXRhO1xuICAgICAgaWYgKHJlc29sdmVkTWV0YWRhdGEgJiYgcmVzb2x2ZWRNZXRhZGF0YS5fX3N5bWJvbGljID09PSAncmVzb2x2ZWQnKSB7XG4gICAgICAgIHJlc29sdmVkTWV0YWRhdGEgPSByZXNvbHZlZE1ldGFkYXRhLnN5bWJvbDtcbiAgICAgIH1cbiAgICAgIGlmIChyZXNvbHZlZE1ldGFkYXRhIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgICAgIHJldHVybiB0aGlzLmZpbmRTeW1ib2xEZWNsYXJhdGlvbihyZXNvbHZlZFN5bWJvbC5tZXRhZGF0YSk7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBzeW1ib2w7XG4gIH1cblxuICBwdWJsaWMgdHJ5QW5ub3RhdGlvbnModHlwZTogU3RhdGljU3ltYm9sKTogYW55W10ge1xuICAgIGNvbnN0IG9yaWdpbmFsUmVjb3JkZXIgPSB0aGlzLmVycm9yUmVjb3JkZXI7XG4gICAgdGhpcy5lcnJvclJlY29yZGVyID0gKGVycm9yOiBhbnksIGZpbGVOYW1lPzogc3RyaW5nKSA9PiB7fTtcbiAgICB0cnkge1xuICAgICAgcmV0dXJuIHRoaXMuYW5ub3RhdGlvbnModHlwZSk7XG4gICAgfSBmaW5hbGx5IHtcbiAgICAgIHRoaXMuZXJyb3JSZWNvcmRlciA9IG9yaWdpbmFsUmVjb3JkZXI7XG4gICAgfVxuICB9XG5cbiAgcHVibGljIGFubm90YXRpb25zKHR5cGU6IFN0YXRpY1N5bWJvbCk6IGFueVtdIHtcbiAgICByZXR1cm4gdGhpcy5fYW5ub3RhdGlvbnMoXG4gICAgICAgIHR5cGUsICh0eXBlOiBTdGF0aWNTeW1ib2wsIGRlY29yYXRvcnM6IGFueSkgPT4gdGhpcy5zaW1wbGlmeSh0eXBlLCBkZWNvcmF0b3JzKSxcbiAgICAgICAgdGhpcy5hbm5vdGF0aW9uQ2FjaGUpO1xuICB9XG5cbiAgcHVibGljIHNoYWxsb3dBbm5vdGF0aW9ucyh0eXBlOiBTdGF0aWNTeW1ib2wpOiBhbnlbXSB7XG4gICAgcmV0dXJuIHRoaXMuX2Fubm90YXRpb25zKFxuICAgICAgICB0eXBlLCAodHlwZTogU3RhdGljU3ltYm9sLCBkZWNvcmF0b3JzOiBhbnkpID0+IHRoaXMuc2ltcGxpZnkodHlwZSwgZGVjb3JhdG9ycywgdHJ1ZSksXG4gICAgICAgIHRoaXMuc2hhbGxvd0Fubm90YXRpb25DYWNoZSk7XG4gIH1cblxuICBwcml2YXRlIF9hbm5vdGF0aW9ucyhcbiAgICAgIHR5cGU6IFN0YXRpY1N5bWJvbCwgc2ltcGxpZnk6ICh0eXBlOiBTdGF0aWNTeW1ib2wsIGRlY29yYXRvcnM6IGFueSkgPT4gYW55LFxuICAgICAgYW5ub3RhdGlvbkNhY2hlOiBNYXA8U3RhdGljU3ltYm9sLCBhbnlbXT4pOiBhbnlbXSB7XG4gICAgbGV0IGFubm90YXRpb25zID0gYW5ub3RhdGlvbkNhY2hlLmdldCh0eXBlKTtcbiAgICBpZiAoIWFubm90YXRpb25zKSB7XG4gICAgICBhbm5vdGF0aW9ucyA9IFtdO1xuICAgICAgY29uc3QgY2xhc3NNZXRhZGF0YSA9IHRoaXMuZ2V0VHlwZU1ldGFkYXRhKHR5cGUpO1xuICAgICAgY29uc3QgcGFyZW50VHlwZSA9IHRoaXMuZmluZFBhcmVudFR5cGUodHlwZSwgY2xhc3NNZXRhZGF0YSk7XG4gICAgICBpZiAocGFyZW50VHlwZSkge1xuICAgICAgICBjb25zdCBwYXJlbnRBbm5vdGF0aW9ucyA9IHRoaXMuYW5ub3RhdGlvbnMocGFyZW50VHlwZSk7XG4gICAgICAgIGFubm90YXRpb25zLnB1c2goLi4ucGFyZW50QW5ub3RhdGlvbnMpO1xuICAgICAgfVxuICAgICAgbGV0IG93bkFubm90YXRpb25zOiBhbnlbXSA9IFtdO1xuICAgICAgaWYgKGNsYXNzTWV0YWRhdGFbJ2RlY29yYXRvcnMnXSkge1xuICAgICAgICBvd25Bbm5vdGF0aW9ucyA9IHNpbXBsaWZ5KHR5cGUsIGNsYXNzTWV0YWRhdGFbJ2RlY29yYXRvcnMnXSk7XG4gICAgICAgIGlmIChvd25Bbm5vdGF0aW9ucykge1xuICAgICAgICAgIGFubm90YXRpb25zLnB1c2goLi4ub3duQW5ub3RhdGlvbnMpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICBpZiAocGFyZW50VHlwZSAmJiAhdGhpcy5zdW1tYXJ5UmVzb2x2ZXIuaXNMaWJyYXJ5RmlsZSh0eXBlLmZpbGVQYXRoKSAmJlxuICAgICAgICAgIHRoaXMuc3VtbWFyeVJlc29sdmVyLmlzTGlicmFyeUZpbGUocGFyZW50VHlwZS5maWxlUGF0aCkpIHtcbiAgICAgICAgY29uc3Qgc3VtbWFyeSA9IHRoaXMuc3VtbWFyeVJlc29sdmVyLnJlc29sdmVTdW1tYXJ5KHBhcmVudFR5cGUpO1xuICAgICAgICBpZiAoc3VtbWFyeSAmJiBzdW1tYXJ5LnR5cGUpIHtcbiAgICAgICAgICBjb25zdCByZXF1aXJlZEFubm90YXRpb25UeXBlcyA9XG4gICAgICAgICAgICAgIHRoaXMuYW5ub3RhdGlvbkZvclBhcmVudENsYXNzV2l0aFN1bW1hcnlLaW5kLmdldChzdW1tYXJ5LnR5cGUuc3VtbWFyeUtpbmQgISkgITtcbiAgICAgICAgICBjb25zdCB0eXBlSGFzUmVxdWlyZWRBbm5vdGF0aW9uID0gcmVxdWlyZWRBbm5vdGF0aW9uVHlwZXMuc29tZShcbiAgICAgICAgICAgICAgKHJlcXVpcmVkVHlwZSkgPT4gb3duQW5ub3RhdGlvbnMuc29tZShhbm4gPT4gcmVxdWlyZWRUeXBlLmlzVHlwZU9mKGFubikpKTtcbiAgICAgICAgICBpZiAoIXR5cGVIYXNSZXF1aXJlZEFubm90YXRpb24pIHtcbiAgICAgICAgICAgIHRoaXMucmVwb3J0RXJyb3IoXG4gICAgICAgICAgICAgICAgZm9ybWF0TWV0YWRhdGFFcnJvcihcbiAgICAgICAgICAgICAgICAgICAgbWV0YWRhdGFFcnJvcihcbiAgICAgICAgICAgICAgICAgICAgICAgIGBDbGFzcyAke3R5cGUubmFtZX0gaW4gJHt0eXBlLmZpbGVQYXRofSBleHRlbmRzIGZyb20gYSAke0NvbXBpbGVTdW1tYXJ5S2luZFtzdW1tYXJ5LnR5cGUuc3VtbWFyeUtpbmQhXX0gaW4gYW5vdGhlciBjb21waWxhdGlvbiB1bml0IHdpdGhvdXQgZHVwbGljYXRpbmcgdGhlIGRlY29yYXRvcmAsXG4gICAgICAgICAgICAgICAgICAgICAgICAvKiBzdW1tYXJ5ICovIHVuZGVmaW5lZCxcbiAgICAgICAgICAgICAgICAgICAgICAgIGBQbGVhc2UgYWRkIGEgJHtyZXF1aXJlZEFubm90YXRpb25UeXBlcy5tYXAoKHR5cGUpID0+IHR5cGUubmdNZXRhZGF0YU5hbWUpLmpvaW4oJyBvciAnKX0gZGVjb3JhdG9yIHRvIHRoZSBjbGFzc2ApLFxuICAgICAgICAgICAgICAgICAgICB0eXBlKSxcbiAgICAgICAgICAgICAgICB0eXBlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGFubm90YXRpb25DYWNoZS5zZXQodHlwZSwgYW5ub3RhdGlvbnMuZmlsdGVyKGFubiA9PiAhIWFubikpO1xuICAgIH1cbiAgICByZXR1cm4gYW5ub3RhdGlvbnM7XG4gIH1cblxuICBwdWJsaWMgcHJvcE1ldGFkYXRhKHR5cGU6IFN0YXRpY1N5bWJvbCk6IHtba2V5OiBzdHJpbmddOiBhbnlbXX0ge1xuICAgIGxldCBwcm9wTWV0YWRhdGEgPSB0aGlzLnByb3BlcnR5Q2FjaGUuZ2V0KHR5cGUpO1xuICAgIGlmICghcHJvcE1ldGFkYXRhKSB7XG4gICAgICBjb25zdCBjbGFzc01ldGFkYXRhID0gdGhpcy5nZXRUeXBlTWV0YWRhdGEodHlwZSk7XG4gICAgICBwcm9wTWV0YWRhdGEgPSB7fTtcbiAgICAgIGNvbnN0IHBhcmVudFR5cGUgPSB0aGlzLmZpbmRQYXJlbnRUeXBlKHR5cGUsIGNsYXNzTWV0YWRhdGEpO1xuICAgICAgaWYgKHBhcmVudFR5cGUpIHtcbiAgICAgICAgY29uc3QgcGFyZW50UHJvcE1ldGFkYXRhID0gdGhpcy5wcm9wTWV0YWRhdGEocGFyZW50VHlwZSk7XG4gICAgICAgIE9iamVjdC5rZXlzKHBhcmVudFByb3BNZXRhZGF0YSkuZm9yRWFjaCgocGFyZW50UHJvcCkgPT4ge1xuICAgICAgICAgIHByb3BNZXRhZGF0YSAhW3BhcmVudFByb3BdID0gcGFyZW50UHJvcE1ldGFkYXRhW3BhcmVudFByb3BdO1xuICAgICAgICB9KTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgbWVtYmVycyA9IGNsYXNzTWV0YWRhdGFbJ21lbWJlcnMnXSB8fCB7fTtcbiAgICAgIE9iamVjdC5rZXlzKG1lbWJlcnMpLmZvckVhY2goKHByb3BOYW1lKSA9PiB7XG4gICAgICAgIGNvbnN0IHByb3BEYXRhID0gbWVtYmVyc1twcm9wTmFtZV07XG4gICAgICAgIGNvbnN0IHByb3AgPSAoPGFueVtdPnByb3BEYXRhKVxuICAgICAgICAgICAgICAgICAgICAgICAgIC5maW5kKGEgPT4gYVsnX19zeW1ib2xpYyddID09ICdwcm9wZXJ0eScgfHwgYVsnX19zeW1ib2xpYyddID09ICdtZXRob2QnKTtcbiAgICAgICAgY29uc3QgZGVjb3JhdG9yczogYW55W10gPSBbXTtcbiAgICAgICAgaWYgKHByb3BNZXRhZGF0YSAhW3Byb3BOYW1lXSkge1xuICAgICAgICAgIGRlY29yYXRvcnMucHVzaCguLi5wcm9wTWV0YWRhdGEgIVtwcm9wTmFtZV0pO1xuICAgICAgICB9XG4gICAgICAgIHByb3BNZXRhZGF0YSAhW3Byb3BOYW1lXSA9IGRlY29yYXRvcnM7XG4gICAgICAgIGlmIChwcm9wICYmIHByb3BbJ2RlY29yYXRvcnMnXSkge1xuICAgICAgICAgIGRlY29yYXRvcnMucHVzaCguLi50aGlzLnNpbXBsaWZ5KHR5cGUsIHByb3BbJ2RlY29yYXRvcnMnXSkpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICAgIHRoaXMucHJvcGVydHlDYWNoZS5zZXQodHlwZSwgcHJvcE1ldGFkYXRhKTtcbiAgICB9XG4gICAgcmV0dXJuIHByb3BNZXRhZGF0YTtcbiAgfVxuXG4gIHB1YmxpYyBwYXJhbWV0ZXJzKHR5cGU6IFN0YXRpY1N5bWJvbCk6IGFueVtdIHtcbiAgICBpZiAoISh0eXBlIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSkge1xuICAgICAgdGhpcy5yZXBvcnRFcnJvcihcbiAgICAgICAgICBuZXcgRXJyb3IoYHBhcmFtZXRlcnMgcmVjZWl2ZWQgJHtKU09OLnN0cmluZ2lmeSh0eXBlKX0gd2hpY2ggaXMgbm90IGEgU3RhdGljU3ltYm9sYCksXG4gICAgICAgICAgdHlwZSk7XG4gICAgICByZXR1cm4gW107XG4gICAgfVxuICAgIHRyeSB7XG4gICAgICBsZXQgcGFyYW1ldGVycyA9IHRoaXMucGFyYW1ldGVyQ2FjaGUuZ2V0KHR5cGUpO1xuICAgICAgaWYgKCFwYXJhbWV0ZXJzKSB7XG4gICAgICAgIGNvbnN0IGNsYXNzTWV0YWRhdGEgPSB0aGlzLmdldFR5cGVNZXRhZGF0YSh0eXBlKTtcbiAgICAgICAgY29uc3QgcGFyZW50VHlwZSA9IHRoaXMuZmluZFBhcmVudFR5cGUodHlwZSwgY2xhc3NNZXRhZGF0YSk7XG4gICAgICAgIGNvbnN0IG1lbWJlcnMgPSBjbGFzc01ldGFkYXRhID8gY2xhc3NNZXRhZGF0YVsnbWVtYmVycyddIDogbnVsbDtcbiAgICAgICAgY29uc3QgY3RvckRhdGEgPSBtZW1iZXJzID8gbWVtYmVyc1snX19jdG9yX18nXSA6IG51bGw7XG4gICAgICAgIGlmIChjdG9yRGF0YSkge1xuICAgICAgICAgIGNvbnN0IGN0b3IgPSAoPGFueVtdPmN0b3JEYXRhKS5maW5kKGEgPT4gYVsnX19zeW1ib2xpYyddID09ICdjb25zdHJ1Y3RvcicpO1xuICAgICAgICAgIGNvbnN0IHJhd1BhcmFtZXRlclR5cGVzID0gPGFueVtdPmN0b3JbJ3BhcmFtZXRlcnMnXSB8fCBbXTtcbiAgICAgICAgICBjb25zdCBwYXJhbWV0ZXJEZWNvcmF0b3JzID0gPGFueVtdPnRoaXMuc2ltcGxpZnkodHlwZSwgY3RvclsncGFyYW1ldGVyRGVjb3JhdG9ycyddIHx8IFtdKTtcbiAgICAgICAgICBwYXJhbWV0ZXJzID0gW107XG4gICAgICAgICAgcmF3UGFyYW1ldGVyVHlwZXMuZm9yRWFjaCgocmF3UGFyYW1UeXBlLCBpbmRleCkgPT4ge1xuICAgICAgICAgICAgY29uc3QgbmVzdGVkUmVzdWx0OiBhbnlbXSA9IFtdO1xuICAgICAgICAgICAgY29uc3QgcGFyYW1UeXBlID0gdGhpcy50cnlTaW1wbGlmeSh0eXBlLCByYXdQYXJhbVR5cGUpO1xuICAgICAgICAgICAgaWYgKHBhcmFtVHlwZSkgbmVzdGVkUmVzdWx0LnB1c2gocGFyYW1UeXBlKTtcbiAgICAgICAgICAgIGNvbnN0IGRlY29yYXRvcnMgPSBwYXJhbWV0ZXJEZWNvcmF0b3JzID8gcGFyYW1ldGVyRGVjb3JhdG9yc1tpbmRleF0gOiBudWxsO1xuICAgICAgICAgICAgaWYgKGRlY29yYXRvcnMpIHtcbiAgICAgICAgICAgICAgbmVzdGVkUmVzdWx0LnB1c2goLi4uZGVjb3JhdG9ycyk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBwYXJhbWV0ZXJzICEucHVzaChuZXN0ZWRSZXN1bHQpO1xuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKHBhcmVudFR5cGUpIHtcbiAgICAgICAgICBwYXJhbWV0ZXJzID0gdGhpcy5wYXJhbWV0ZXJzKHBhcmVudFR5cGUpO1xuICAgICAgICB9XG4gICAgICAgIGlmICghcGFyYW1ldGVycykge1xuICAgICAgICAgIHBhcmFtZXRlcnMgPSBbXTtcbiAgICAgICAgfVxuICAgICAgICB0aGlzLnBhcmFtZXRlckNhY2hlLnNldCh0eXBlLCBwYXJhbWV0ZXJzKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBwYXJhbWV0ZXJzO1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCBvbiB0eXBlICR7SlNPTi5zdHJpbmdpZnkodHlwZSl9IHdpdGggZXJyb3IgJHtlfWApO1xuICAgICAgdGhyb3cgZTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9tZXRob2ROYW1lcyh0eXBlOiBhbnkpOiB7W2tleTogc3RyaW5nXTogYm9vbGVhbn0ge1xuICAgIGxldCBtZXRob2ROYW1lcyA9IHRoaXMubWV0aG9kQ2FjaGUuZ2V0KHR5cGUpO1xuICAgIGlmICghbWV0aG9kTmFtZXMpIHtcbiAgICAgIGNvbnN0IGNsYXNzTWV0YWRhdGEgPSB0aGlzLmdldFR5cGVNZXRhZGF0YSh0eXBlKTtcbiAgICAgIG1ldGhvZE5hbWVzID0ge307XG4gICAgICBjb25zdCBwYXJlbnRUeXBlID0gdGhpcy5maW5kUGFyZW50VHlwZSh0eXBlLCBjbGFzc01ldGFkYXRhKTtcbiAgICAgIGlmIChwYXJlbnRUeXBlKSB7XG4gICAgICAgIGNvbnN0IHBhcmVudE1ldGhvZE5hbWVzID0gdGhpcy5fbWV0aG9kTmFtZXMocGFyZW50VHlwZSk7XG4gICAgICAgIE9iamVjdC5rZXlzKHBhcmVudE1ldGhvZE5hbWVzKS5mb3JFYWNoKChwYXJlbnRQcm9wKSA9PiB7XG4gICAgICAgICAgbWV0aG9kTmFtZXMgIVtwYXJlbnRQcm9wXSA9IHBhcmVudE1ldGhvZE5hbWVzW3BhcmVudFByb3BdO1xuICAgICAgICB9KTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgbWVtYmVycyA9IGNsYXNzTWV0YWRhdGFbJ21lbWJlcnMnXSB8fCB7fTtcbiAgICAgIE9iamVjdC5rZXlzKG1lbWJlcnMpLmZvckVhY2goKHByb3BOYW1lKSA9PiB7XG4gICAgICAgIGNvbnN0IHByb3BEYXRhID0gbWVtYmVyc1twcm9wTmFtZV07XG4gICAgICAgIGNvbnN0IGlzTWV0aG9kID0gKDxhbnlbXT5wcm9wRGF0YSkuc29tZShhID0+IGFbJ19fc3ltYm9saWMnXSA9PSAnbWV0aG9kJyk7XG4gICAgICAgIG1ldGhvZE5hbWVzICFbcHJvcE5hbWVdID0gbWV0aG9kTmFtZXMgIVtwcm9wTmFtZV0gfHwgaXNNZXRob2Q7XG4gICAgICB9KTtcbiAgICAgIHRoaXMubWV0aG9kQ2FjaGUuc2V0KHR5cGUsIG1ldGhvZE5hbWVzKTtcbiAgICB9XG4gICAgcmV0dXJuIG1ldGhvZE5hbWVzO1xuICB9XG5cbiAgcHJpdmF0ZSBfc3RhdGljTWVtYmVycyh0eXBlOiBTdGF0aWNTeW1ib2wpOiBzdHJpbmdbXSB7XG4gICAgbGV0IHN0YXRpY01lbWJlcnMgPSB0aGlzLnN0YXRpY0NhY2hlLmdldCh0eXBlKTtcbiAgICBpZiAoIXN0YXRpY01lbWJlcnMpIHtcbiAgICAgIGNvbnN0IGNsYXNzTWV0YWRhdGEgPSB0aGlzLmdldFR5cGVNZXRhZGF0YSh0eXBlKTtcbiAgICAgIGNvbnN0IHN0YXRpY01lbWJlckRhdGEgPSBjbGFzc01ldGFkYXRhWydzdGF0aWNzJ10gfHwge307XG4gICAgICBzdGF0aWNNZW1iZXJzID0gT2JqZWN0LmtleXMoc3RhdGljTWVtYmVyRGF0YSk7XG4gICAgICB0aGlzLnN0YXRpY0NhY2hlLnNldCh0eXBlLCBzdGF0aWNNZW1iZXJzKTtcbiAgICB9XG4gICAgcmV0dXJuIHN0YXRpY01lbWJlcnM7XG4gIH1cblxuXG4gIHByaXZhdGUgZmluZFBhcmVudFR5cGUodHlwZTogU3RhdGljU3ltYm9sLCBjbGFzc01ldGFkYXRhOiBhbnkpOiBTdGF0aWNTeW1ib2x8dW5kZWZpbmVkIHtcbiAgICBjb25zdCBwYXJlbnRUeXBlID0gdGhpcy50cnlTaW1wbGlmeSh0eXBlLCBjbGFzc01ldGFkYXRhWydleHRlbmRzJ10pO1xuICAgIGlmIChwYXJlbnRUeXBlIGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgICByZXR1cm4gcGFyZW50VHlwZTtcbiAgICB9XG4gIH1cblxuICBoYXNMaWZlY3ljbGVIb29rKHR5cGU6IGFueSwgbGNQcm9wZXJ0eTogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgaWYgKCEodHlwZSBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkpIHtcbiAgICAgIHRoaXMucmVwb3J0RXJyb3IoXG4gICAgICAgICAgbmV3IEVycm9yKFxuICAgICAgICAgICAgICBgaGFzTGlmZWN5Y2xlSG9vayByZWNlaXZlZCAke0pTT04uc3RyaW5naWZ5KHR5cGUpfSB3aGljaCBpcyBub3QgYSBTdGF0aWNTeW1ib2xgKSxcbiAgICAgICAgICB0eXBlKTtcbiAgICB9XG4gICAgdHJ5IHtcbiAgICAgIHJldHVybiAhIXRoaXMuX21ldGhvZE5hbWVzKHR5cGUpW2xjUHJvcGVydHldO1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIGNvbnNvbGUuZXJyb3IoYEZhaWxlZCBvbiB0eXBlICR7SlNPTi5zdHJpbmdpZnkodHlwZSl9IHdpdGggZXJyb3IgJHtlfWApO1xuICAgICAgdGhyb3cgZTtcbiAgICB9XG4gIH1cblxuICBndWFyZHModHlwZTogYW55KToge1trZXk6IHN0cmluZ106IFN0YXRpY1N5bWJvbH0ge1xuICAgIGlmICghKHR5cGUgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpKSB7XG4gICAgICB0aGlzLnJlcG9ydEVycm9yKFxuICAgICAgICAgIG5ldyBFcnJvcihgZ3VhcmRzIHJlY2VpdmVkICR7SlNPTi5zdHJpbmdpZnkodHlwZSl9IHdoaWNoIGlzIG5vdCBhIFN0YXRpY1N5bWJvbGApLCB0eXBlKTtcbiAgICAgIHJldHVybiB7fTtcbiAgICB9XG4gICAgY29uc3Qgc3RhdGljTWVtYmVycyA9IHRoaXMuX3N0YXRpY01lbWJlcnModHlwZSk7XG4gICAgY29uc3QgcmVzdWx0OiB7W2tleTogc3RyaW5nXTogU3RhdGljU3ltYm9sfSA9IHt9O1xuICAgIGZvciAobGV0IG5hbWUgb2Ygc3RhdGljTWVtYmVycykge1xuICAgICAgaWYgKG5hbWUuZW5kc1dpdGgoVFlQRUdVQVJEX1BPU1RGSVgpKSB7XG4gICAgICAgIGxldCBwcm9wZXJ0eSA9IG5hbWUuc3Vic3RyKDAsIG5hbWUubGVuZ3RoIC0gVFlQRUdVQVJEX1BPU1RGSVgubGVuZ3RoKTtcbiAgICAgICAgbGV0IHZhbHVlOiBhbnk7XG4gICAgICAgIGlmIChwcm9wZXJ0eS5lbmRzV2l0aChVU0VfSUYpKSB7XG4gICAgICAgICAgcHJvcGVydHkgPSBuYW1lLnN1YnN0cigwLCBwcm9wZXJ0eS5sZW5ndGggLSBVU0VfSUYubGVuZ3RoKTtcbiAgICAgICAgICB2YWx1ZSA9IFVTRV9JRjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB2YWx1ZSA9IHRoaXMuZ2V0U3RhdGljU3ltYm9sKHR5cGUuZmlsZVBhdGgsIHR5cGUubmFtZSwgW25hbWVdKTtcbiAgICAgICAgfVxuICAgICAgICByZXN1bHRbcHJvcGVydHldID0gdmFsdWU7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICBwcml2YXRlIF9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IodHlwZTogU3RhdGljU3ltYm9sLCBjdG9yOiBhbnkpOiB2b2lkIHtcbiAgICB0aGlzLmNvbnZlcnNpb25NYXAuc2V0KHR5cGUsIChjb250ZXh0OiBTdGF0aWNTeW1ib2wsIGFyZ3M6IGFueVtdKSA9PiBuZXcgY3RvciguLi5hcmdzKSk7XG4gIH1cblxuICBwcml2YXRlIF9yZWdpc3RlckZ1bmN0aW9uKHR5cGU6IFN0YXRpY1N5bWJvbCwgZm46IGFueSk6IHZvaWQge1xuICAgIHRoaXMuY29udmVyc2lvbk1hcC5zZXQodHlwZSwgKGNvbnRleHQ6IFN0YXRpY1N5bWJvbCwgYXJnczogYW55W10pID0+IGZuLmFwcGx5KHVuZGVmaW5lZCwgYXJncykpO1xuICB9XG5cbiAgcHJpdmF0ZSBpbml0aWFsaXplQ29udmVyc2lvbk1hcCgpOiB2b2lkIHtcbiAgICB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IoXG4gICAgICAgIHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ0luamVjdGFibGUnKSwgY3JlYXRlSW5qZWN0YWJsZSk7XG4gICAgdGhpcy5pbmplY3Rpb25Ub2tlbiA9IHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ0luamVjdGlvblRva2VuJyk7XG4gICAgdGhpcy5vcGFxdWVUb2tlbiA9IHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ09wYXF1ZVRva2VuJyk7XG4gICAgdGhpcy5ST1VURVMgPSB0aGlzLnRyeUZpbmREZWNsYXJhdGlvbihBTkdVTEFSX1JPVVRFUiwgJ1JPVVRFUycpO1xuICAgIHRoaXMuQU5BTFlaRV9GT1JfRU5UUllfQ09NUE9ORU5UUyA9XG4gICAgICAgIHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ0FOQUxZWkVfRk9SX0VOVFJZX0NPTVBPTkVOVFMnKTtcblxuICAgIHRoaXMuX3JlZ2lzdGVyRGVjb3JhdG9yT3JDb25zdHJ1Y3Rvcih0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdIb3N0JyksIGNyZWF0ZUhvc3QpO1xuICAgIHRoaXMuX3JlZ2lzdGVyRGVjb3JhdG9yT3JDb25zdHJ1Y3Rvcih0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdTZWxmJyksIGNyZWF0ZVNlbGYpO1xuICAgIHRoaXMuX3JlZ2lzdGVyRGVjb3JhdG9yT3JDb25zdHJ1Y3RvcihcbiAgICAgICAgdGhpcy5maW5kRGVjbGFyYXRpb24oQU5HVUxBUl9DT1JFLCAnU2tpcFNlbGYnKSwgY3JlYXRlU2tpcFNlbGYpO1xuICAgIHRoaXMuX3JlZ2lzdGVyRGVjb3JhdG9yT3JDb25zdHJ1Y3RvcihcbiAgICAgICAgdGhpcy5maW5kRGVjbGFyYXRpb24oQU5HVUxBUl9DT1JFLCAnSW5qZWN0JyksIGNyZWF0ZUluamVjdCk7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKFxuICAgICAgICB0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdPcHRpb25hbCcpLCBjcmVhdGVPcHRpb25hbCk7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKFxuICAgICAgICB0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdBdHRyaWJ1dGUnKSwgY3JlYXRlQXR0cmlidXRlKTtcbiAgICB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IoXG4gICAgICAgIHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ0NvbnRlbnRDaGlsZCcpLCBjcmVhdGVDb250ZW50Q2hpbGQpO1xuICAgIHRoaXMuX3JlZ2lzdGVyRGVjb3JhdG9yT3JDb25zdHJ1Y3RvcihcbiAgICAgICAgdGhpcy5maW5kRGVjbGFyYXRpb24oQU5HVUxBUl9DT1JFLCAnQ29udGVudENoaWxkcmVuJyksIGNyZWF0ZUNvbnRlbnRDaGlsZHJlbik7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKFxuICAgICAgICB0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdWaWV3Q2hpbGQnKSwgY3JlYXRlVmlld0NoaWxkKTtcbiAgICB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IoXG4gICAgICAgIHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ1ZpZXdDaGlsZHJlbicpLCBjcmVhdGVWaWV3Q2hpbGRyZW4pO1xuICAgIHRoaXMuX3JlZ2lzdGVyRGVjb3JhdG9yT3JDb25zdHJ1Y3Rvcih0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdJbnB1dCcpLCBjcmVhdGVJbnB1dCk7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKFxuICAgICAgICB0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdPdXRwdXQnKSwgY3JlYXRlT3V0cHV0KTtcbiAgICB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IodGhpcy5maW5kRGVjbGFyYXRpb24oQU5HVUxBUl9DT1JFLCAnUGlwZScpLCBjcmVhdGVQaXBlKTtcbiAgICB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IoXG4gICAgICAgIHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ0hvc3RCaW5kaW5nJyksIGNyZWF0ZUhvc3RCaW5kaW5nKTtcbiAgICB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IoXG4gICAgICAgIHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ0hvc3RMaXN0ZW5lcicpLCBjcmVhdGVIb3N0TGlzdGVuZXIpO1xuICAgIHRoaXMuX3JlZ2lzdGVyRGVjb3JhdG9yT3JDb25zdHJ1Y3RvcihcbiAgICAgICAgdGhpcy5maW5kRGVjbGFyYXRpb24oQU5HVUxBUl9DT1JFLCAnRGlyZWN0aXZlJyksIGNyZWF0ZURpcmVjdGl2ZSk7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKFxuICAgICAgICB0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdDb21wb25lbnQnKSwgY3JlYXRlQ29tcG9uZW50KTtcbiAgICB0aGlzLl9yZWdpc3RlckRlY29yYXRvck9yQ29uc3RydWN0b3IoXG4gICAgICAgIHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ05nTW9kdWxlJyksIGNyZWF0ZU5nTW9kdWxlKTtcblxuICAgIC8vIE5vdGU6IFNvbWUgbWV0YWRhdGEgY2xhc3NlcyBjYW4gYmUgdXNlZCBkaXJlY3RseSB3aXRoIFByb3ZpZGVyLmRlcHMuXG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ0hvc3QnKSwgY3JlYXRlSG9zdCk7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKHRoaXMuZmluZERlY2xhcmF0aW9uKEFOR1VMQVJfQ09SRSwgJ1NlbGYnKSwgY3JlYXRlU2VsZik7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKFxuICAgICAgICB0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdTa2lwU2VsZicpLCBjcmVhdGVTa2lwU2VsZik7XG4gICAgdGhpcy5fcmVnaXN0ZXJEZWNvcmF0b3JPckNvbnN0cnVjdG9yKFxuICAgICAgICB0aGlzLmZpbmREZWNsYXJhdGlvbihBTkdVTEFSX0NPUkUsICdPcHRpb25hbCcpLCBjcmVhdGVPcHRpb25hbCk7XG4gIH1cblxuICAvKipcbiAgICogZ2V0U3RhdGljU3ltYm9sIHByb2R1Y2VzIGEgVHlwZSB3aG9zZSBtZXRhZGF0YSBpcyBrbm93biBidXQgd2hvc2UgaW1wbGVtZW50YXRpb24gaXMgbm90IGxvYWRlZC5cbiAgICogQWxsIHR5cGVzIHBhc3NlZCB0byB0aGUgU3RhdGljUmVzb2x2ZXIgc2hvdWxkIGJlIHBzZXVkby10eXBlcyByZXR1cm5lZCBieSB0aGlzIG1ldGhvZC5cbiAgICpcbiAgICogQHBhcmFtIGRlY2xhcmF0aW9uRmlsZSB0aGUgYWJzb2x1dGUgcGF0aCBvZiB0aGUgZmlsZSB3aGVyZSB0aGUgc3ltYm9sIGlzIGRlY2xhcmVkXG4gICAqIEBwYXJhbSBuYW1lIHRoZSBuYW1lIG9mIHRoZSB0eXBlLlxuICAgKi9cbiAgZ2V0U3RhdGljU3ltYm9sKGRlY2xhcmF0aW9uRmlsZTogc3RyaW5nLCBuYW1lOiBzdHJpbmcsIG1lbWJlcnM/OiBzdHJpbmdbXSk6IFN0YXRpY1N5bWJvbCB7XG4gICAgcmV0dXJuIHRoaXMuc3ltYm9sUmVzb2x2ZXIuZ2V0U3RhdGljU3ltYm9sKGRlY2xhcmF0aW9uRmlsZSwgbmFtZSwgbWVtYmVycyk7XG4gIH1cblxuICAvKipcbiAgICogU2ltcGxpZnkgYnV0IGRpc2NhcmQgYW55IGVycm9yc1xuICAgKi9cbiAgcHJpdmF0ZSB0cnlTaW1wbGlmeShjb250ZXh0OiBTdGF0aWNTeW1ib2wsIHZhbHVlOiBhbnkpOiBhbnkge1xuICAgIGNvbnN0IG9yaWdpbmFsUmVjb3JkZXIgPSB0aGlzLmVycm9yUmVjb3JkZXI7XG4gICAgdGhpcy5lcnJvclJlY29yZGVyID0gKGVycm9yOiBhbnksIGZpbGVOYW1lPzogc3RyaW5nKSA9PiB7fTtcbiAgICBjb25zdCByZXN1bHQgPSB0aGlzLnNpbXBsaWZ5KGNvbnRleHQsIHZhbHVlKTtcbiAgICB0aGlzLmVycm9yUmVjb3JkZXIgPSBvcmlnaW5hbFJlY29yZGVyO1xuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIHB1YmxpYyBzaW1wbGlmeShjb250ZXh0OiBTdGF0aWNTeW1ib2wsIHZhbHVlOiBhbnksIGxhenk6IGJvb2xlYW4gPSBmYWxzZSk6IGFueSB7XG4gICAgY29uc3Qgc2VsZiA9IHRoaXM7XG4gICAgbGV0IHNjb3BlID0gQmluZGluZ1Njb3BlLmVtcHR5O1xuICAgIGNvbnN0IGNhbGxpbmcgPSBuZXcgTWFwPFN0YXRpY1N5bWJvbCwgYm9vbGVhbj4oKTtcbiAgICBjb25zdCByb290Q29udGV4dCA9IGNvbnRleHQ7XG5cbiAgICBmdW5jdGlvbiBzaW1wbGlmeUluQ29udGV4dChcbiAgICAgICAgY29udGV4dDogU3RhdGljU3ltYm9sLCB2YWx1ZTogYW55LCBkZXB0aDogbnVtYmVyLCByZWZlcmVuY2VzOiBudW1iZXIpOiBhbnkge1xuICAgICAgZnVuY3Rpb24gcmVzb2x2ZVJlZmVyZW5jZVZhbHVlKHN0YXRpY1N5bWJvbDogU3RhdGljU3ltYm9sKTogYW55IHtcbiAgICAgICAgY29uc3QgcmVzb2x2ZWRTeW1ib2wgPSBzZWxmLnN5bWJvbFJlc29sdmVyLnJlc29sdmVTeW1ib2woc3RhdGljU3ltYm9sKTtcbiAgICAgICAgcmV0dXJuIHJlc29sdmVkU3ltYm9sID8gcmVzb2x2ZWRTeW1ib2wubWV0YWRhdGEgOiBudWxsO1xuICAgICAgfVxuXG4gICAgICBmdW5jdGlvbiBzaW1wbGlmeUVhZ2VybHkodmFsdWU6IGFueSk6IGFueSB7XG4gICAgICAgIHJldHVybiBzaW1wbGlmeUluQ29udGV4dChjb250ZXh0LCB2YWx1ZSwgZGVwdGgsIDApO1xuICAgICAgfVxuXG4gICAgICBmdW5jdGlvbiBzaW1wbGlmeUxhemlseSh2YWx1ZTogYW55KTogYW55IHtcbiAgICAgICAgcmV0dXJuIHNpbXBsaWZ5SW5Db250ZXh0KGNvbnRleHQsIHZhbHVlLCBkZXB0aCwgcmVmZXJlbmNlcyArIDEpO1xuICAgICAgfVxuXG4gICAgICBmdW5jdGlvbiBzaW1wbGlmeU5lc3RlZChuZXN0ZWRDb250ZXh0OiBTdGF0aWNTeW1ib2wsIHZhbHVlOiBhbnkpOiBhbnkge1xuICAgICAgICBpZiAobmVzdGVkQ29udGV4dCA9PT0gY29udGV4dCkge1xuICAgICAgICAgIC8vIElmIHRoZSBjb250ZXh0IGhhc24ndCBjaGFuZ2VkIGxldCB0aGUgZXhjZXB0aW9uIHByb3BhZ2F0ZSB1bm1vZGlmaWVkLlxuICAgICAgICAgIHJldHVybiBzaW1wbGlmeUluQ29udGV4dChuZXN0ZWRDb250ZXh0LCB2YWx1ZSwgZGVwdGggKyAxLCByZWZlcmVuY2VzKTtcbiAgICAgICAgfVxuICAgICAgICB0cnkge1xuICAgICAgICAgIHJldHVybiBzaW1wbGlmeUluQ29udGV4dChuZXN0ZWRDb250ZXh0LCB2YWx1ZSwgZGVwdGggKyAxLCByZWZlcmVuY2VzKTtcbiAgICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICAgIGlmIChpc01ldGFkYXRhRXJyb3IoZSkpIHtcbiAgICAgICAgICAgIC8vIFByb3BhZ2F0ZSB0aGUgbWVzc2FnZSB0ZXh0IHVwIGJ1dCBhZGQgYSBtZXNzYWdlIHRvIHRoZSBjaGFpbiB0aGF0IGV4cGxhaW5zIGhvdyB3ZSBnb3RcbiAgICAgICAgICAgIC8vIGhlcmUuXG4gICAgICAgICAgICAvLyBlLmNoYWluIGltcGxpZXMgZS5zeW1ib2xcbiAgICAgICAgICAgIGNvbnN0IHN1bW1hcnlNc2cgPSBlLmNoYWluID8gJ3JlZmVyZW5jZXMgXFwnJyArIGUuc3ltYm9sICEubmFtZSArICdcXCcnIDogZXJyb3JTdW1tYXJ5KGUpO1xuICAgICAgICAgICAgY29uc3Qgc3VtbWFyeSA9IGAnJHtuZXN0ZWRDb250ZXh0Lm5hbWV9JyAke3N1bW1hcnlNc2d9YDtcbiAgICAgICAgICAgIGNvbnN0IGNoYWluID0ge21lc3NhZ2U6IHN1bW1hcnksIHBvc2l0aW9uOiBlLnBvc2l0aW9uLCBuZXh0OiBlLmNoYWlufTtcbiAgICAgICAgICAgIC8vIFRPRE8oY2h1Y2tqKTogcmV0cmlldmUgdGhlIHBvc2l0aW9uIGluZm9ybWF0aW9uIGluZGlyZWN0bHkgZnJvbSB0aGUgY29sbGVjdG9ycyBub2RlXG4gICAgICAgICAgICAvLyBtYXAgaWYgdGhlIG1ldGFkYXRhIGlzIGZyb20gYSAudHMgZmlsZS5cbiAgICAgICAgICAgIHNlbGYuZXJyb3IoXG4gICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgbWVzc2FnZTogZS5tZXNzYWdlLFxuICAgICAgICAgICAgICAgICAgYWR2aXNlOiBlLmFkdmlzZSxcbiAgICAgICAgICAgICAgICAgIGNvbnRleHQ6IGUuY29udGV4dCwgY2hhaW4sXG4gICAgICAgICAgICAgICAgICBzeW1ib2w6IG5lc3RlZENvbnRleHRcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGNvbnRleHQpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAvLyBJdCBpcyBwcm9iYWJseSBhbiBpbnRlcm5hbCBlcnJvci5cbiAgICAgICAgICAgIHRocm93IGU7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGZ1bmN0aW9uIHNpbXBsaWZ5Q2FsbChcbiAgICAgICAgICBmdW5jdGlvblN5bWJvbDogU3RhdGljU3ltYm9sLCB0YXJnZXRGdW5jdGlvbjogYW55LCBhcmdzOiBhbnlbXSwgdGFyZ2V0RXhwcmVzc2lvbjogYW55KSB7XG4gICAgICAgIGlmICh0YXJnZXRGdW5jdGlvbiAmJiB0YXJnZXRGdW5jdGlvblsnX19zeW1ib2xpYyddID09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgICBpZiAoY2FsbGluZy5nZXQoZnVuY3Rpb25TeW1ib2wpKSB7XG4gICAgICAgICAgICBzZWxmLmVycm9yKFxuICAgICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICAgIG1lc3NhZ2U6ICdSZWN1cnNpb24gaXMgbm90IHN1cHBvcnRlZCcsXG4gICAgICAgICAgICAgICAgICBzdW1tYXJ5OiBgY2FsbGVkICcke2Z1bmN0aW9uU3ltYm9sLm5hbWV9JyByZWN1cnNpdmVseWAsXG4gICAgICAgICAgICAgICAgICB2YWx1ZTogdGFyZ2V0RnVuY3Rpb25cbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGZ1bmN0aW9uU3ltYm9sKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIGNvbnN0IHZhbHVlID0gdGFyZ2V0RnVuY3Rpb25bJ3ZhbHVlJ107XG4gICAgICAgICAgICBpZiAodmFsdWUgJiYgKGRlcHRoICE9IDAgfHwgdmFsdWUuX19zeW1ib2xpYyAhPSAnZXJyb3InKSkge1xuICAgICAgICAgICAgICBjb25zdCBwYXJhbWV0ZXJzOiBzdHJpbmdbXSA9IHRhcmdldEZ1bmN0aW9uWydwYXJhbWV0ZXJzJ107XG4gICAgICAgICAgICAgIGNvbnN0IGRlZmF1bHRzOiBhbnlbXSA9IHRhcmdldEZ1bmN0aW9uLmRlZmF1bHRzO1xuICAgICAgICAgICAgICBhcmdzID0gYXJncy5tYXAoYXJnID0+IHNpbXBsaWZ5TmVzdGVkKGNvbnRleHQsIGFyZykpXG4gICAgICAgICAgICAgICAgICAgICAgICAgLm1hcChhcmcgPT4gc2hvdWxkSWdub3JlKGFyZykgPyB1bmRlZmluZWQgOiBhcmcpO1xuICAgICAgICAgICAgICBpZiAoZGVmYXVsdHMgJiYgZGVmYXVsdHMubGVuZ3RoID4gYXJncy5sZW5ndGgpIHtcbiAgICAgICAgICAgICAgICBhcmdzLnB1c2goLi4uZGVmYXVsdHMuc2xpY2UoYXJncy5sZW5ndGgpLm1hcCgodmFsdWU6IGFueSkgPT4gc2ltcGxpZnkodmFsdWUpKSk7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgY2FsbGluZy5zZXQoZnVuY3Rpb25TeW1ib2wsIHRydWUpO1xuICAgICAgICAgICAgICBjb25zdCBmdW5jdGlvblNjb3BlID0gQmluZGluZ1Njb3BlLmJ1aWxkKCk7XG4gICAgICAgICAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgcGFyYW1ldGVycy5sZW5ndGg7IGkrKykge1xuICAgICAgICAgICAgICAgIGZ1bmN0aW9uU2NvcGUuZGVmaW5lKHBhcmFtZXRlcnNbaV0sIGFyZ3NbaV0pO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIGNvbnN0IG9sZFNjb3BlID0gc2NvcGU7XG4gICAgICAgICAgICAgIGxldCByZXN1bHQ6IGFueTtcbiAgICAgICAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgICAgICBzY29wZSA9IGZ1bmN0aW9uU2NvcGUuZG9uZSgpO1xuICAgICAgICAgICAgICAgIHJlc3VsdCA9IHNpbXBsaWZ5TmVzdGVkKGZ1bmN0aW9uU3ltYm9sLCB2YWx1ZSk7XG4gICAgICAgICAgICAgIH0gZmluYWxseSB7XG4gICAgICAgICAgICAgICAgc2NvcGUgPSBvbGRTY29wZTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICByZXR1cm4gcmVzdWx0O1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH0gZmluYWxseSB7XG4gICAgICAgICAgICBjYWxsaW5nLmRlbGV0ZShmdW5jdGlvblN5bWJvbCk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG5cbiAgICAgICAgaWYgKGRlcHRoID09PSAwKSB7XG4gICAgICAgICAgLy8gSWYgZGVwdGggaXMgMCB3ZSBhcmUgZXZhbHVhdGluZyB0aGUgdG9wIGxldmVsIGV4cHJlc3Npb24gdGhhdCBpcyBkZXNjcmliaW5nIGVsZW1lbnRcbiAgICAgICAgICAvLyBkZWNvcmF0b3IuIEluIHRoaXMgY2FzZSwgaXQgaXMgYSBkZWNvcmF0b3Igd2UgZG9uJ3QgdW5kZXJzdGFuZCwgc3VjaCBhcyBhIGN1c3RvbVxuICAgICAgICAgIC8vIG5vbi1hbmd1bGFyIGRlY29yYXRvciwgYW5kIHdlIHNob3VsZCBqdXN0IGlnbm9yZSBpdC5cbiAgICAgICAgICByZXR1cm4gSUdOT1JFO1xuICAgICAgICB9XG4gICAgICAgIGxldCBwb3NpdGlvbjogUG9zaXRpb258dW5kZWZpbmVkID0gdW5kZWZpbmVkO1xuICAgICAgICBpZiAodGFyZ2V0RXhwcmVzc2lvbiAmJiB0YXJnZXRFeHByZXNzaW9uLl9fc3ltYm9saWMgPT0gJ3Jlc29sdmVkJykge1xuICAgICAgICAgIGNvbnN0IGxpbmUgPSB0YXJnZXRFeHByZXNzaW9uLmxpbmU7XG4gICAgICAgICAgY29uc3QgY2hhcmFjdGVyID0gdGFyZ2V0RXhwcmVzc2lvbi5jaGFyYWN0ZXI7XG4gICAgICAgICAgY29uc3QgZmlsZU5hbWUgPSB0YXJnZXRFeHByZXNzaW9uLmZpbGVOYW1lO1xuICAgICAgICAgIGlmIChmaWxlTmFtZSAhPSBudWxsICYmIGxpbmUgIT0gbnVsbCAmJiBjaGFyYWN0ZXIgIT0gbnVsbCkge1xuICAgICAgICAgICAgcG9zaXRpb24gPSB7ZmlsZU5hbWUsIGxpbmUsIGNvbHVtbjogY2hhcmFjdGVyfTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgc2VsZi5lcnJvcihcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgbWVzc2FnZTogRlVOQ1RJT05fQ0FMTF9OT1RfU1VQUE9SVEVELFxuICAgICAgICAgICAgICBjb250ZXh0OiBmdW5jdGlvblN5bWJvbCxcbiAgICAgICAgICAgICAgdmFsdWU6IHRhcmdldEZ1bmN0aW9uLCBwb3NpdGlvblxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIGNvbnRleHQpO1xuICAgICAgfVxuXG4gICAgICBmdW5jdGlvbiBzaW1wbGlmeShleHByZXNzaW9uOiBhbnkpOiBhbnkge1xuICAgICAgICBpZiAoaXNQcmltaXRpdmUoZXhwcmVzc2lvbikpIHtcbiAgICAgICAgICByZXR1cm4gZXhwcmVzc2lvbjtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZXhwcmVzc2lvbiBpbnN0YW5jZW9mIEFycmF5KSB7XG4gICAgICAgICAgY29uc3QgcmVzdWx0OiBhbnlbXSA9IFtdO1xuICAgICAgICAgIGZvciAoY29uc3QgaXRlbSBvZiAoPGFueT5leHByZXNzaW9uKSkge1xuICAgICAgICAgICAgLy8gQ2hlY2sgZm9yIGEgc3ByZWFkIGV4cHJlc3Npb25cbiAgICAgICAgICAgIGlmIChpdGVtICYmIGl0ZW0uX19zeW1ib2xpYyA9PT0gJ3NwcmVhZCcpIHtcbiAgICAgICAgICAgICAgLy8gV2UgY2FsbCB3aXRoIHJlZmVyZW5jZXMgYXMgMCBiZWNhdXNlIHdlIHJlcXVpcmUgdGhlIGFjdHVhbCB2YWx1ZSBhbmQgY2Fubm90XG4gICAgICAgICAgICAgIC8vIHRvbGVyYXRlIGEgcmVmZXJlbmNlIGhlcmUuXG4gICAgICAgICAgICAgIGNvbnN0IHNwcmVhZEFycmF5ID0gc2ltcGxpZnlFYWdlcmx5KGl0ZW0uZXhwcmVzc2lvbik7XG4gICAgICAgICAgICAgIGlmIChBcnJheS5pc0FycmF5KHNwcmVhZEFycmF5KSkge1xuICAgICAgICAgICAgICAgIGZvciAoY29uc3Qgc3ByZWFkSXRlbSBvZiBzcHJlYWRBcnJheSkge1xuICAgICAgICAgICAgICAgICAgcmVzdWx0LnB1c2goc3ByZWFkSXRlbSk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIGNvbnRpbnVlO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBjb25zdCB2YWx1ZSA9IHNpbXBsaWZ5KGl0ZW0pO1xuICAgICAgICAgICAgaWYgKHNob3VsZElnbm9yZSh2YWx1ZSkpIHtcbiAgICAgICAgICAgICAgY29udGludWU7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICByZXN1bHQucHVzaCh2YWx1ZSk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiByZXN1bHQ7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGV4cHJlc3Npb24gaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wpIHtcbiAgICAgICAgICAvLyBTdG9wIHNpbXBsaWZpY2F0aW9uIGF0IGJ1aWx0aW4gc3ltYm9scyBvciBpZiB3ZSBhcmUgaW4gYSByZWZlcmVuY2UgY29udGV4dCBhbmRcbiAgICAgICAgICAvLyB0aGUgc3ltYm9sIGRvZXNuJ3QgaGF2ZSBtZW1iZXJzLlxuICAgICAgICAgIGlmIChleHByZXNzaW9uID09PSBzZWxmLmluamVjdGlvblRva2VuIHx8IHNlbGYuY29udmVyc2lvbk1hcC5oYXMoZXhwcmVzc2lvbikgfHxcbiAgICAgICAgICAgICAgKHJlZmVyZW5jZXMgPiAwICYmICFleHByZXNzaW9uLm1lbWJlcnMubGVuZ3RoKSkge1xuICAgICAgICAgICAgcmV0dXJuIGV4cHJlc3Npb247XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIGNvbnN0IHN0YXRpY1N5bWJvbCA9IGV4cHJlc3Npb247XG4gICAgICAgICAgICBjb25zdCBkZWNsYXJhdGlvblZhbHVlID0gcmVzb2x2ZVJlZmVyZW5jZVZhbHVlKHN0YXRpY1N5bWJvbCk7XG4gICAgICAgICAgICBpZiAoZGVjbGFyYXRpb25WYWx1ZSAhPSBudWxsKSB7XG4gICAgICAgICAgICAgIHJldHVybiBzaW1wbGlmeU5lc3RlZChzdGF0aWNTeW1ib2wsIGRlY2xhcmF0aW9uVmFsdWUpO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgcmV0dXJuIHN0YXRpY1N5bWJvbDtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGV4cHJlc3Npb24pIHtcbiAgICAgICAgICBpZiAoZXhwcmVzc2lvblsnX19zeW1ib2xpYyddKSB7XG4gICAgICAgICAgICBsZXQgc3RhdGljU3ltYm9sOiBTdGF0aWNTeW1ib2w7XG4gICAgICAgICAgICBzd2l0Y2ggKGV4cHJlc3Npb25bJ19fc3ltYm9saWMnXSkge1xuICAgICAgICAgICAgICBjYXNlICdiaW5vcCc6XG4gICAgICAgICAgICAgICAgbGV0IGxlZnQgPSBzaW1wbGlmeShleHByZXNzaW9uWydsZWZ0J10pO1xuICAgICAgICAgICAgICAgIGlmIChzaG91bGRJZ25vcmUobGVmdCkpIHJldHVybiBsZWZ0O1xuICAgICAgICAgICAgICAgIGxldCByaWdodCA9IHNpbXBsaWZ5KGV4cHJlc3Npb25bJ3JpZ2h0J10pO1xuICAgICAgICAgICAgICAgIGlmIChzaG91bGRJZ25vcmUocmlnaHQpKSByZXR1cm4gcmlnaHQ7XG4gICAgICAgICAgICAgICAgc3dpdGNoIChleHByZXNzaW9uWydvcGVyYXRvciddKSB7XG4gICAgICAgICAgICAgICAgICBjYXNlICcmJic6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBsZWZ0ICYmIHJpZ2h0O1xuICAgICAgICAgICAgICAgICAgY2FzZSAnfHwnOlxuICAgICAgICAgICAgICAgICAgICByZXR1cm4gbGVmdCB8fCByaWdodDtcbiAgICAgICAgICAgICAgICAgIGNhc2UgJ3wnOlxuICAgICAgICAgICAgICAgICAgICByZXR1cm4gbGVmdCB8IHJpZ2h0O1xuICAgICAgICAgICAgICAgICAgY2FzZSAnXic6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBsZWZ0IF4gcmlnaHQ7XG4gICAgICAgICAgICAgICAgICBjYXNlICcmJzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgJiByaWdodDtcbiAgICAgICAgICAgICAgICAgIGNhc2UgJz09JzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgPT0gcmlnaHQ7XG4gICAgICAgICAgICAgICAgICBjYXNlICchPSc6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBsZWZ0ICE9IHJpZ2h0O1xuICAgICAgICAgICAgICAgICAgY2FzZSAnPT09JzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgPT09IHJpZ2h0O1xuICAgICAgICAgICAgICAgICAgY2FzZSAnIT09JzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgIT09IHJpZ2h0O1xuICAgICAgICAgICAgICAgICAgY2FzZSAnPCc6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBsZWZ0IDwgcmlnaHQ7XG4gICAgICAgICAgICAgICAgICBjYXNlICc+JzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgPiByaWdodDtcbiAgICAgICAgICAgICAgICAgIGNhc2UgJzw9JzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgPD0gcmlnaHQ7XG4gICAgICAgICAgICAgICAgICBjYXNlICc+PSc6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBsZWZ0ID49IHJpZ2h0O1xuICAgICAgICAgICAgICAgICAgY2FzZSAnPDwnOlxuICAgICAgICAgICAgICAgICAgICByZXR1cm4gbGVmdCA8PCByaWdodDtcbiAgICAgICAgICAgICAgICAgIGNhc2UgJz4+JzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgPj4gcmlnaHQ7XG4gICAgICAgICAgICAgICAgICBjYXNlICcrJzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgKyByaWdodDtcbiAgICAgICAgICAgICAgICAgIGNhc2UgJy0nOlxuICAgICAgICAgICAgICAgICAgICByZXR1cm4gbGVmdCAtIHJpZ2h0O1xuICAgICAgICAgICAgICAgICAgY2FzZSAnKic6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBsZWZ0ICogcmlnaHQ7XG4gICAgICAgICAgICAgICAgICBjYXNlICcvJzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGxlZnQgLyByaWdodDtcbiAgICAgICAgICAgICAgICAgIGNhc2UgJyUnOlxuICAgICAgICAgICAgICAgICAgICByZXR1cm4gbGVmdCAlIHJpZ2h0O1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICByZXR1cm4gbnVsbDtcbiAgICAgICAgICAgICAgY2FzZSAnaWYnOlxuICAgICAgICAgICAgICAgIGxldCBjb25kaXRpb24gPSBzaW1wbGlmeShleHByZXNzaW9uWydjb25kaXRpb24nXSk7XG4gICAgICAgICAgICAgICAgcmV0dXJuIGNvbmRpdGlvbiA/IHNpbXBsaWZ5KGV4cHJlc3Npb25bJ3RoZW5FeHByZXNzaW9uJ10pIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgc2ltcGxpZnkoZXhwcmVzc2lvblsnZWxzZUV4cHJlc3Npb24nXSk7XG4gICAgICAgICAgICAgIGNhc2UgJ3ByZSc6XG4gICAgICAgICAgICAgICAgbGV0IG9wZXJhbmQgPSBzaW1wbGlmeShleHByZXNzaW9uWydvcGVyYW5kJ10pO1xuICAgICAgICAgICAgICAgIGlmIChzaG91bGRJZ25vcmUob3BlcmFuZCkpIHJldHVybiBvcGVyYW5kO1xuICAgICAgICAgICAgICAgIHN3aXRjaCAoZXhwcmVzc2lvblsnb3BlcmF0b3InXSkge1xuICAgICAgICAgICAgICAgICAgY2FzZSAnKyc6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBvcGVyYW5kO1xuICAgICAgICAgICAgICAgICAgY2FzZSAnLSc6XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiAtb3BlcmFuZDtcbiAgICAgICAgICAgICAgICAgIGNhc2UgJyEnOlxuICAgICAgICAgICAgICAgICAgICByZXR1cm4gIW9wZXJhbmQ7XG4gICAgICAgICAgICAgICAgICBjYXNlICd+JzpcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIH5vcGVyYW5kO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICByZXR1cm4gbnVsbDtcbiAgICAgICAgICAgICAgY2FzZSAnaW5kZXgnOlxuICAgICAgICAgICAgICAgIGxldCBpbmRleFRhcmdldCA9IHNpbXBsaWZ5RWFnZXJseShleHByZXNzaW9uWydleHByZXNzaW9uJ10pO1xuICAgICAgICAgICAgICAgIGxldCBpbmRleCA9IHNpbXBsaWZ5RWFnZXJseShleHByZXNzaW9uWydpbmRleCddKTtcbiAgICAgICAgICAgICAgICBpZiAoaW5kZXhUYXJnZXQgJiYgaXNQcmltaXRpdmUoaW5kZXgpKSByZXR1cm4gaW5kZXhUYXJnZXRbaW5kZXhdO1xuICAgICAgICAgICAgICAgIHJldHVybiBudWxsO1xuICAgICAgICAgICAgICBjYXNlICdzZWxlY3QnOlxuICAgICAgICAgICAgICAgIGNvbnN0IG1lbWJlciA9IGV4cHJlc3Npb25bJ21lbWJlciddO1xuICAgICAgICAgICAgICAgIGxldCBzZWxlY3RDb250ZXh0ID0gY29udGV4dDtcbiAgICAgICAgICAgICAgICBsZXQgc2VsZWN0VGFyZ2V0ID0gc2ltcGxpZnkoZXhwcmVzc2lvblsnZXhwcmVzc2lvbiddKTtcbiAgICAgICAgICAgICAgICBpZiAoc2VsZWN0VGFyZ2V0IGluc3RhbmNlb2YgU3RhdGljU3ltYm9sKSB7XG4gICAgICAgICAgICAgICAgICBjb25zdCBtZW1iZXJzID0gc2VsZWN0VGFyZ2V0Lm1lbWJlcnMuY29uY2F0KG1lbWJlcik7XG4gICAgICAgICAgICAgICAgICBzZWxlY3RDb250ZXh0ID1cbiAgICAgICAgICAgICAgICAgICAgICBzZWxmLmdldFN0YXRpY1N5bWJvbChzZWxlY3RUYXJnZXQuZmlsZVBhdGgsIHNlbGVjdFRhcmdldC5uYW1lLCBtZW1iZXJzKTtcbiAgICAgICAgICAgICAgICAgIGNvbnN0IGRlY2xhcmF0aW9uVmFsdWUgPSByZXNvbHZlUmVmZXJlbmNlVmFsdWUoc2VsZWN0Q29udGV4dCk7XG4gICAgICAgICAgICAgICAgICBpZiAoZGVjbGFyYXRpb25WYWx1ZSAhPSBudWxsKSB7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBzaW1wbGlmeU5lc3RlZChzZWxlY3RDb250ZXh0LCBkZWNsYXJhdGlvblZhbHVlKTtcbiAgICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBzZWxlY3RDb250ZXh0O1xuICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBpZiAoc2VsZWN0VGFyZ2V0ICYmIGlzUHJpbWl0aXZlKG1lbWJlcikpXG4gICAgICAgICAgICAgICAgICByZXR1cm4gc2ltcGxpZnlOZXN0ZWQoc2VsZWN0Q29udGV4dCwgc2VsZWN0VGFyZ2V0W21lbWJlcl0pO1xuICAgICAgICAgICAgICAgIHJldHVybiBudWxsO1xuICAgICAgICAgICAgICBjYXNlICdyZWZlcmVuY2UnOlxuICAgICAgICAgICAgICAgIC8vIE5vdGU6IFRoaXMgb25seSBoYXMgdG8gZGVhbCB3aXRoIHZhcmlhYmxlIHJlZmVyZW5jZXMsIGFzIHN5bWJvbCByZWZlcmVuY2VzIGhhdmVcbiAgICAgICAgICAgICAgICAvLyBiZWVuIGNvbnZlcnRlZCBpbnRvICdyZXNvbHZlZCdcbiAgICAgICAgICAgICAgICAvLyBpbiB0aGUgU3RhdGljU3ltYm9sUmVzb2x2ZXIuXG4gICAgICAgICAgICAgICAgY29uc3QgbmFtZTogc3RyaW5nID0gZXhwcmVzc2lvblsnbmFtZSddO1xuICAgICAgICAgICAgICAgIGNvbnN0IGxvY2FsVmFsdWUgPSBzY29wZS5yZXNvbHZlKG5hbWUpO1xuICAgICAgICAgICAgICAgIGlmIChsb2NhbFZhbHVlICE9IEJpbmRpbmdTY29wZS5taXNzaW5nKSB7XG4gICAgICAgICAgICAgICAgICByZXR1cm4gbG9jYWxWYWx1ZTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgICAgIGNhc2UgJ3Jlc29sdmVkJzpcbiAgICAgICAgICAgICAgICB0cnkge1xuICAgICAgICAgICAgICAgICAgcmV0dXJuIHNpbXBsaWZ5KGV4cHJlc3Npb24uc3ltYm9sKTtcbiAgICAgICAgICAgICAgICB9IGNhdGNoIChlKSB7XG4gICAgICAgICAgICAgICAgICAvLyBJZiBhbiBlcnJvciBpcyByZXBvcnRlZCBldmFsdWF0aW5nIHRoZSBzeW1ib2wgcmVjb3JkIHRoZSBwb3NpdGlvbiBvZiB0aGVcbiAgICAgICAgICAgICAgICAgIC8vIHJlZmVyZW5jZSBpbiB0aGUgZXJyb3Igc28gaXQgY2FuXG4gICAgICAgICAgICAgICAgICAvLyBiZSByZXBvcnRlZCBpbiB0aGUgZXJyb3IgbWVzc2FnZSBnZW5lcmF0ZWQgZnJvbSB0aGUgZXhjZXB0aW9uLlxuICAgICAgICAgICAgICAgICAgaWYgKGlzTWV0YWRhdGFFcnJvcihlKSAmJiBleHByZXNzaW9uLmZpbGVOYW1lICE9IG51bGwgJiZcbiAgICAgICAgICAgICAgICAgICAgICBleHByZXNzaW9uLmxpbmUgIT0gbnVsbCAmJiBleHByZXNzaW9uLmNoYXJhY3RlciAhPSBudWxsKSB7XG4gICAgICAgICAgICAgICAgICAgIGUucG9zaXRpb24gPSB7XG4gICAgICAgICAgICAgICAgICAgICAgZmlsZU5hbWU6IGV4cHJlc3Npb24uZmlsZU5hbWUsXG4gICAgICAgICAgICAgICAgICAgICAgbGluZTogZXhwcmVzc2lvbi5saW5lLFxuICAgICAgICAgICAgICAgICAgICAgIGNvbHVtbjogZXhwcmVzc2lvbi5jaGFyYWN0ZXJcbiAgICAgICAgICAgICAgICAgICAgfTtcbiAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgIHRocm93IGU7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICBjYXNlICdjbGFzcyc6XG4gICAgICAgICAgICAgICAgcmV0dXJuIGNvbnRleHQ7XG4gICAgICAgICAgICAgIGNhc2UgJ2Z1bmN0aW9uJzpcbiAgICAgICAgICAgICAgICByZXR1cm4gY29udGV4dDtcbiAgICAgICAgICAgICAgY2FzZSAnbmV3JzpcbiAgICAgICAgICAgICAgY2FzZSAnY2FsbCc6XG4gICAgICAgICAgICAgICAgLy8gRGV0ZXJtaW5lIGlmIHRoZSBmdW5jdGlvbiBpcyBhIGJ1aWx0LWluIGNvbnZlcnNpb25cbiAgICAgICAgICAgICAgICBzdGF0aWNTeW1ib2wgPSBzaW1wbGlmeUluQ29udGV4dChcbiAgICAgICAgICAgICAgICAgICAgY29udGV4dCwgZXhwcmVzc2lvblsnZXhwcmVzc2lvbiddLCBkZXB0aCArIDEsIC8qIHJlZmVyZW5jZXMgKi8gMCk7XG4gICAgICAgICAgICAgICAgaWYgKHN0YXRpY1N5bWJvbCBpbnN0YW5jZW9mIFN0YXRpY1N5bWJvbCkge1xuICAgICAgICAgICAgICAgICAgaWYgKHN0YXRpY1N5bWJvbCA9PT0gc2VsZi5pbmplY3Rpb25Ub2tlbiB8fCBzdGF0aWNTeW1ib2wgPT09IHNlbGYub3BhcXVlVG9rZW4pIHtcbiAgICAgICAgICAgICAgICAgICAgLy8gaWYgc29tZWJvZHkgY2FsbHMgbmV3IEluamVjdGlvblRva2VuLCBkb24ndCBjcmVhdGUgYW4gSW5qZWN0aW9uVG9rZW4sXG4gICAgICAgICAgICAgICAgICAgIC8vIGJ1dCByYXRoZXIgcmV0dXJuIHRoZSBzeW1ib2wgdG8gd2hpY2ggdGhlIEluamVjdGlvblRva2VuIGlzIGFzc2lnbmVkIHRvLlxuXG4gICAgICAgICAgICAgICAgICAgIC8vIE9wYXF1ZVRva2VuIGlzIHN1cHBvcnRlZCB0b28gYXMgaXQgaXMgcmVxdWlyZWQgYnkgdGhlIGxhbmd1YWdlIHNlcnZpY2UgdG9cbiAgICAgICAgICAgICAgICAgICAgLy8gc3VwcG9ydCB2NCBhbmQgcHJpb3IgdmVyc2lvbnMgb2YgQW5ndWxhci5cbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuIGNvbnRleHQ7XG4gICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICBjb25zdCBhcmdFeHByZXNzaW9uczogYW55W10gPSBleHByZXNzaW9uWydhcmd1bWVudHMnXSB8fCBbXTtcbiAgICAgICAgICAgICAgICAgIGxldCBjb252ZXJ0ZXIgPSBzZWxmLmNvbnZlcnNpb25NYXAuZ2V0KHN0YXRpY1N5bWJvbCk7XG4gICAgICAgICAgICAgICAgICBpZiAoY29udmVydGVyKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnN0IGFyZ3MgPSBhcmdFeHByZXNzaW9ucy5tYXAoYXJnID0+IHNpbXBsaWZ5TmVzdGVkKGNvbnRleHQsIGFyZykpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLm1hcChhcmcgPT4gc2hvdWxkSWdub3JlKGFyZykgPyB1bmRlZmluZWQgOiBhcmcpO1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gY29udmVydGVyKGNvbnRleHQsIGFyZ3MpO1xuICAgICAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgLy8gRGV0ZXJtaW5lIGlmIHRoZSBmdW5jdGlvbiBpcyBvbmUgd2UgY2FuIHNpbXBsaWZ5LlxuICAgICAgICAgICAgICAgICAgICBjb25zdCB0YXJnZXRGdW5jdGlvbiA9IHJlc29sdmVSZWZlcmVuY2VWYWx1ZShzdGF0aWNTeW1ib2wpO1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gc2ltcGxpZnlDYWxsKFxuICAgICAgICAgICAgICAgICAgICAgICAgc3RhdGljU3ltYm9sLCB0YXJnZXRGdW5jdGlvbiwgYXJnRXhwcmVzc2lvbnMsIGV4cHJlc3Npb25bJ2V4cHJlc3Npb24nXSk7XG4gICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIHJldHVybiBJR05PUkU7XG4gICAgICAgICAgICAgIGNhc2UgJ2Vycm9yJzpcbiAgICAgICAgICAgICAgICBsZXQgbWVzc2FnZSA9IGV4cHJlc3Npb24ubWVzc2FnZTtcbiAgICAgICAgICAgICAgICBpZiAoZXhwcmVzc2lvblsnbGluZSddICE9IG51bGwpIHtcbiAgICAgICAgICAgICAgICAgIHNlbGYuZXJyb3IoXG4gICAgICAgICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgICAgICAgICAgbWVzc2FnZSxcbiAgICAgICAgICAgICAgICAgICAgICAgIGNvbnRleHQ6IGV4cHJlc3Npb24uY29udGV4dCxcbiAgICAgICAgICAgICAgICAgICAgICAgIHZhbHVlOiBleHByZXNzaW9uLFxuICAgICAgICAgICAgICAgICAgICAgICAgcG9zaXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgZmlsZU5hbWU6IGV4cHJlc3Npb25bJ2ZpbGVOYW1lJ10sXG4gICAgICAgICAgICAgICAgICAgICAgICAgIGxpbmU6IGV4cHJlc3Npb25bJ2xpbmUnXSxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgY29sdW1uOiBleHByZXNzaW9uWydjaGFyYWN0ZXInXVxuICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgICAgICAgY29udGV4dCk7XG4gICAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAgIHNlbGYuZXJyb3Ioe21lc3NhZ2UsIGNvbnRleHQ6IGV4cHJlc3Npb24uY29udGV4dH0sIGNvbnRleHQpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICByZXR1cm4gSUdOT1JFO1xuICAgICAgICAgICAgICBjYXNlICdpZ25vcmUnOlxuICAgICAgICAgICAgICAgIHJldHVybiBleHByZXNzaW9uO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgcmV0dXJuIG51bGw7XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiBtYXBTdHJpbmdNYXAoZXhwcmVzc2lvbiwgKHZhbHVlLCBuYW1lKSA9PiB7XG4gICAgICAgICAgICBpZiAoUkVGRVJFTkNFX1NFVC5oYXMobmFtZSkpIHtcbiAgICAgICAgICAgICAgaWYgKG5hbWUgPT09IFVTRV9WQUxVRSAmJiBQUk9WSURFIGluIGV4cHJlc3Npb24pIHtcbiAgICAgICAgICAgICAgICAvLyBJZiB0aGlzIGlzIGEgcHJvdmlkZXIgZXhwcmVzc2lvbiwgY2hlY2sgZm9yIHNwZWNpYWwgdG9rZW5zIHRoYXQgbmVlZCB0aGUgdmFsdWVcbiAgICAgICAgICAgICAgICAvLyBkdXJpbmcgYW5hbHlzaXMuXG4gICAgICAgICAgICAgICAgY29uc3QgcHJvdmlkZSA9IHNpbXBsaWZ5KGV4cHJlc3Npb24ucHJvdmlkZSk7XG4gICAgICAgICAgICAgICAgaWYgKHByb3ZpZGUgPT09IHNlbGYuUk9VVEVTIHx8IHByb3ZpZGUgPT0gc2VsZi5BTkFMWVpFX0ZPUl9FTlRSWV9DT01QT05FTlRTKSB7XG4gICAgICAgICAgICAgICAgICByZXR1cm4gc2ltcGxpZnkodmFsdWUpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICByZXR1cm4gc2ltcGxpZnlMYXppbHkodmFsdWUpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgcmV0dXJuIHNpbXBsaWZ5KHZhbHVlKTtcbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gSUdOT1JFO1xuICAgICAgfVxuXG4gICAgICByZXR1cm4gc2ltcGxpZnkodmFsdWUpO1xuICAgIH1cblxuICAgIGxldCByZXN1bHQ6IGFueTtcbiAgICB0cnkge1xuICAgICAgcmVzdWx0ID0gc2ltcGxpZnlJbkNvbnRleHQoY29udGV4dCwgdmFsdWUsIDAsIGxhenkgPyAxIDogMCk7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgaWYgKHRoaXMuZXJyb3JSZWNvcmRlcikge1xuICAgICAgICB0aGlzLnJlcG9ydEVycm9yKGUsIGNvbnRleHQpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhyb3cgZm9ybWF0TWV0YWRhdGFFcnJvcihlLCBjb250ZXh0KTtcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKHNob3VsZElnbm9yZShyZXN1bHQpKSB7XG4gICAgICByZXR1cm4gdW5kZWZpbmVkO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRUeXBlTWV0YWRhdGEodHlwZTogU3RhdGljU3ltYm9sKToge1trZXk6IHN0cmluZ106IGFueX0ge1xuICAgIGNvbnN0IHJlc29sdmVkU3ltYm9sID0gdGhpcy5zeW1ib2xSZXNvbHZlci5yZXNvbHZlU3ltYm9sKHR5cGUpO1xuICAgIHJldHVybiByZXNvbHZlZFN5bWJvbCAmJiByZXNvbHZlZFN5bWJvbC5tZXRhZGF0YSA/IHJlc29sdmVkU3ltYm9sLm1ldGFkYXRhIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB7X19zeW1ib2xpYzogJ2NsYXNzJ307XG4gIH1cblxuICBwcml2YXRlIHJlcG9ydEVycm9yKGVycm9yOiBFcnJvciwgY29udGV4dDogU3RhdGljU3ltYm9sLCBwYXRoPzogc3RyaW5nKSB7XG4gICAgaWYgKHRoaXMuZXJyb3JSZWNvcmRlcikge1xuICAgICAgdGhpcy5lcnJvclJlY29yZGVyKFxuICAgICAgICAgIGZvcm1hdE1ldGFkYXRhRXJyb3IoZXJyb3IsIGNvbnRleHQpLCAoY29udGV4dCAmJiBjb250ZXh0LmZpbGVQYXRoKSB8fCBwYXRoKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhyb3cgZXJyb3I7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBlcnJvcihcbiAgICAgIHttZXNzYWdlLCBzdW1tYXJ5LCBhZHZpc2UsIHBvc2l0aW9uLCBjb250ZXh0LCB2YWx1ZSwgc3ltYm9sLCBjaGFpbn06IHtcbiAgICAgICAgbWVzc2FnZTogc3RyaW5nLFxuICAgICAgICBzdW1tYXJ5Pzogc3RyaW5nLFxuICAgICAgICBhZHZpc2U/OiBzdHJpbmcsXG4gICAgICAgIHBvc2l0aW9uPzogUG9zaXRpb24sXG4gICAgICAgIGNvbnRleHQ/OiBhbnksXG4gICAgICAgIHZhbHVlPzogYW55LFxuICAgICAgICBzeW1ib2w/OiBTdGF0aWNTeW1ib2wsXG4gICAgICAgIGNoYWluPzogTWV0YWRhdGFNZXNzYWdlQ2hhaW5cbiAgICAgIH0sXG4gICAgICByZXBvcnRpbmdDb250ZXh0OiBTdGF0aWNTeW1ib2wpIHtcbiAgICB0aGlzLnJlcG9ydEVycm9yKFxuICAgICAgICBtZXRhZGF0YUVycm9yKG1lc3NhZ2UsIHN1bW1hcnksIGFkdmlzZSwgcG9zaXRpb24sIHN5bWJvbCwgY29udGV4dCwgY2hhaW4pLFxuICAgICAgICByZXBvcnRpbmdDb250ZXh0KTtcbiAgfVxufVxuXG5pbnRlcmZhY2UgUG9zaXRpb24ge1xuICBmaWxlTmFtZTogc3RyaW5nO1xuICBsaW5lOiBudW1iZXI7XG4gIGNvbHVtbjogbnVtYmVyO1xufVxuXG5pbnRlcmZhY2UgTWV0YWRhdGFNZXNzYWdlQ2hhaW4ge1xuICBtZXNzYWdlOiBzdHJpbmc7XG4gIHN1bW1hcnk/OiBzdHJpbmc7XG4gIHBvc2l0aW9uPzogUG9zaXRpb247XG4gIGNvbnRleHQ/OiBhbnk7XG4gIHN5bWJvbD86IFN0YXRpY1N5bWJvbDtcbiAgbmV4dD86IE1ldGFkYXRhTWVzc2FnZUNoYWluO1xufVxuXG50eXBlIE1ldGFkYXRhRXJyb3IgPSBFcnJvciAmIHtcbiAgcG9zaXRpb24/OiBQb3NpdGlvbjtcbiAgYWR2aXNlPzogc3RyaW5nO1xuICBzdW1tYXJ5Pzogc3RyaW5nO1xuICBjb250ZXh0PzogYW55O1xuICBzeW1ib2w/OiBTdGF0aWNTeW1ib2w7XG4gIGNoYWluPzogTWV0YWRhdGFNZXNzYWdlQ2hhaW47XG59O1xuXG5jb25zdCBNRVRBREFUQV9FUlJPUiA9ICduZ01ldGFkYXRhRXJyb3InO1xuXG5mdW5jdGlvbiBtZXRhZGF0YUVycm9yKFxuICAgIG1lc3NhZ2U6IHN0cmluZywgc3VtbWFyeT86IHN0cmluZywgYWR2aXNlPzogc3RyaW5nLCBwb3NpdGlvbj86IFBvc2l0aW9uLCBzeW1ib2w/OiBTdGF0aWNTeW1ib2wsXG4gICAgY29udGV4dD86IGFueSwgY2hhaW4/OiBNZXRhZGF0YU1lc3NhZ2VDaGFpbik6IE1ldGFkYXRhRXJyb3Ige1xuICBjb25zdCBlcnJvciA9IHN5bnRheEVycm9yKG1lc3NhZ2UpIGFzIE1ldGFkYXRhRXJyb3I7XG4gIChlcnJvciBhcyBhbnkpW01FVEFEQVRBX0VSUk9SXSA9IHRydWU7XG4gIGlmIChhZHZpc2UpIGVycm9yLmFkdmlzZSA9IGFkdmlzZTtcbiAgaWYgKHBvc2l0aW9uKSBlcnJvci5wb3NpdGlvbiA9IHBvc2l0aW9uO1xuICBpZiAoc3VtbWFyeSkgZXJyb3Iuc3VtbWFyeSA9IHN1bW1hcnk7XG4gIGlmIChjb250ZXh0KSBlcnJvci5jb250ZXh0ID0gY29udGV4dDtcbiAgaWYgKGNoYWluKSBlcnJvci5jaGFpbiA9IGNoYWluO1xuICBpZiAoc3ltYm9sKSBlcnJvci5zeW1ib2wgPSBzeW1ib2w7XG4gIHJldHVybiBlcnJvcjtcbn1cblxuZnVuY3Rpb24gaXNNZXRhZGF0YUVycm9yKGVycm9yOiBFcnJvcik6IGVycm9yIGlzIE1ldGFkYXRhRXJyb3Ige1xuICByZXR1cm4gISEoZXJyb3IgYXMgYW55KVtNRVRBREFUQV9FUlJPUl07XG59XG5cbmNvbnN0IFJFRkVSRU5DRV9UT19OT05FWFBPUlRFRF9DTEFTUyA9ICdSZWZlcmVuY2UgdG8gbm9uLWV4cG9ydGVkIGNsYXNzJztcbmNvbnN0IFZBUklBQkxFX05PVF9JTklUSUFMSVpFRCA9ICdWYXJpYWJsZSBub3QgaW5pdGlhbGl6ZWQnO1xuY29uc3QgREVTVFJVQ1RVUkVfTk9UX1NVUFBPUlRFRCA9ICdEZXN0cnVjdHVyaW5nIG5vdCBzdXBwb3J0ZWQnO1xuY29uc3QgQ09VTERfTk9UX1JFU09MVkVfVFlQRSA9ICdDb3VsZCBub3QgcmVzb2x2ZSB0eXBlJztcbmNvbnN0IEZVTkNUSU9OX0NBTExfTk9UX1NVUFBPUlRFRCA9ICdGdW5jdGlvbiBjYWxsIG5vdCBzdXBwb3J0ZWQnO1xuY29uc3QgUkVGRVJFTkNFX1RPX0xPQ0FMX1NZTUJPTCA9ICdSZWZlcmVuY2UgdG8gYSBsb2NhbCBzeW1ib2wnO1xuY29uc3QgTEFNQkRBX05PVF9TVVBQT1JURUQgPSAnTGFtYmRhIG5vdCBzdXBwb3J0ZWQnO1xuXG5mdW5jdGlvbiBleHBhbmRlZE1lc3NhZ2UobWVzc2FnZTogc3RyaW5nLCBjb250ZXh0OiBhbnkpOiBzdHJpbmcge1xuICBzd2l0Y2ggKG1lc3NhZ2UpIHtcbiAgICBjYXNlIFJFRkVSRU5DRV9UT19OT05FWFBPUlRFRF9DTEFTUzpcbiAgICAgIGlmIChjb250ZXh0ICYmIGNvbnRleHQuY2xhc3NOYW1lKSB7XG4gICAgICAgIHJldHVybiBgUmVmZXJlbmNlcyB0byBhIG5vbi1leHBvcnRlZCBjbGFzcyBhcmUgbm90IHN1cHBvcnRlZCBpbiBkZWNvcmF0b3JzIGJ1dCAke2NvbnRleHQuY2xhc3NOYW1lfSB3YXMgcmVmZXJlbmNlZC5gO1xuICAgICAgfVxuICAgICAgYnJlYWs7XG4gICAgY2FzZSBWQVJJQUJMRV9OT1RfSU5JVElBTElaRUQ6XG4gICAgICByZXR1cm4gJ09ubHkgaW5pdGlhbGl6ZWQgdmFyaWFibGVzIGFuZCBjb25zdGFudHMgY2FuIGJlIHJlZmVyZW5jZWQgaW4gZGVjb3JhdG9ycyBiZWNhdXNlIHRoZSB2YWx1ZSBvZiB0aGlzIHZhcmlhYmxlIGlzIG5lZWRlZCBieSB0aGUgdGVtcGxhdGUgY29tcGlsZXInO1xuICAgIGNhc2UgREVTVFJVQ1RVUkVfTk9UX1NVUFBPUlRFRDpcbiAgICAgIHJldHVybiAnUmVmZXJlbmNpbmcgYW4gZXhwb3J0ZWQgZGVzdHJ1Y3R1cmVkIHZhcmlhYmxlIG9yIGNvbnN0YW50IGlzIG5vdCBzdXBwb3J0ZWQgaW4gZGVjb3JhdG9ycyBhbmQgdGhpcyB2YWx1ZSBpcyBuZWVkZWQgYnkgdGhlIHRlbXBsYXRlIGNvbXBpbGVyJztcbiAgICBjYXNlIENPVUxEX05PVF9SRVNPTFZFX1RZUEU6XG4gICAgICBpZiAoY29udGV4dCAmJiBjb250ZXh0LnR5cGVOYW1lKSB7XG4gICAgICAgIHJldHVybiBgQ291bGQgbm90IHJlc29sdmUgdHlwZSAke2NvbnRleHQudHlwZU5hbWV9YDtcbiAgICAgIH1cbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgRlVOQ1RJT05fQ0FMTF9OT1RfU1VQUE9SVEVEOlxuICAgICAgaWYgKGNvbnRleHQgJiYgY29udGV4dC5uYW1lKSB7XG4gICAgICAgIHJldHVybiBgRnVuY3Rpb24gY2FsbHMgYXJlIG5vdCBzdXBwb3J0ZWQgaW4gZGVjb3JhdG9ycyBidXQgJyR7Y29udGV4dC5uYW1lfScgd2FzIGNhbGxlZGA7XG4gICAgICB9XG4gICAgICByZXR1cm4gJ0Z1bmN0aW9uIGNhbGxzIGFyZSBub3Qgc3VwcG9ydGVkIGluIGRlY29yYXRvcnMnO1xuICAgIGNhc2UgUkVGRVJFTkNFX1RPX0xPQ0FMX1NZTUJPTDpcbiAgICAgIGlmIChjb250ZXh0ICYmIGNvbnRleHQubmFtZSkge1xuICAgICAgICByZXR1cm4gYFJlZmVyZW5jZSB0byBhIGxvY2FsIChub24tZXhwb3J0ZWQpIHN5bWJvbHMgYXJlIG5vdCBzdXBwb3J0ZWQgaW4gZGVjb3JhdG9ycyBidXQgJyR7Y29udGV4dC5uYW1lfScgd2FzIHJlZmVyZW5jZWRgO1xuICAgICAgfVxuICAgICAgYnJlYWs7XG4gICAgY2FzZSBMQU1CREFfTk9UX1NVUFBPUlRFRDpcbiAgICAgIHJldHVybiBgRnVuY3Rpb24gZXhwcmVzc2lvbnMgYXJlIG5vdCBzdXBwb3J0ZWQgaW4gZGVjb3JhdG9yc2A7XG4gIH1cbiAgcmV0dXJuIG1lc3NhZ2U7XG59XG5cbmZ1bmN0aW9uIG1lc3NhZ2VBZHZpc2UobWVzc2FnZTogc3RyaW5nLCBjb250ZXh0OiBhbnkpOiBzdHJpbmd8dW5kZWZpbmVkIHtcbiAgc3dpdGNoIChtZXNzYWdlKSB7XG4gICAgY2FzZSBSRUZFUkVOQ0VfVE9fTk9ORVhQT1JURURfQ0xBU1M6XG4gICAgICBpZiAoY29udGV4dCAmJiBjb250ZXh0LmNsYXNzTmFtZSkge1xuICAgICAgICByZXR1cm4gYENvbnNpZGVyIGV4cG9ydGluZyAnJHtjb250ZXh0LmNsYXNzTmFtZX0nYDtcbiAgICAgIH1cbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgREVTVFJVQ1RVUkVfTk9UX1NVUFBPUlRFRDpcbiAgICAgIHJldHVybiAnQ29uc2lkZXIgc2ltcGxpZnlpbmcgdG8gYXZvaWQgZGVzdHJ1Y3R1cmluZyc7XG4gICAgY2FzZSBSRUZFUkVOQ0VfVE9fTE9DQUxfU1lNQk9MOlxuICAgICAgaWYgKGNvbnRleHQgJiYgY29udGV4dC5uYW1lKSB7XG4gICAgICAgIHJldHVybiBgQ29uc2lkZXIgZXhwb3J0aW5nICcke2NvbnRleHQubmFtZX0nYDtcbiAgICAgIH1cbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgTEFNQkRBX05PVF9TVVBQT1JURUQ6XG4gICAgICByZXR1cm4gYENvbnNpZGVyIGNoYW5naW5nIHRoZSBmdW5jdGlvbiBleHByZXNzaW9uIGludG8gYW4gZXhwb3J0ZWQgZnVuY3Rpb25gO1xuICB9XG4gIHJldHVybiB1bmRlZmluZWQ7XG59XG5cbmZ1bmN0aW9uIGVycm9yU3VtbWFyeShlcnJvcjogTWV0YWRhdGFFcnJvcik6IHN0cmluZyB7XG4gIGlmIChlcnJvci5zdW1tYXJ5KSB7XG4gICAgcmV0dXJuIGVycm9yLnN1bW1hcnk7XG4gIH1cbiAgc3dpdGNoIChlcnJvci5tZXNzYWdlKSB7XG4gICAgY2FzZSBSRUZFUkVOQ0VfVE9fTk9ORVhQT1JURURfQ0xBU1M6XG4gICAgICBpZiAoZXJyb3IuY29udGV4dCAmJiBlcnJvci5jb250ZXh0LmNsYXNzTmFtZSkge1xuICAgICAgICByZXR1cm4gYHJlZmVyZW5jZXMgbm9uLWV4cG9ydGVkIGNsYXNzICR7ZXJyb3IuY29udGV4dC5jbGFzc05hbWV9YDtcbiAgICAgIH1cbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgVkFSSUFCTEVfTk9UX0lOSVRJQUxJWkVEOlxuICAgICAgcmV0dXJuICdpcyBub3QgaW5pdGlhbGl6ZWQnO1xuICAgIGNhc2UgREVTVFJVQ1RVUkVfTk9UX1NVUFBPUlRFRDpcbiAgICAgIHJldHVybiAnaXMgYSBkZXN0cnVjdHVyZWQgdmFyaWFibGUnO1xuICAgIGNhc2UgQ09VTERfTk9UX1JFU09MVkVfVFlQRTpcbiAgICAgIHJldHVybiAnY291bGQgbm90IGJlIHJlc29sdmVkJztcbiAgICBjYXNlIEZVTkNUSU9OX0NBTExfTk9UX1NVUFBPUlRFRDpcbiAgICAgIGlmIChlcnJvci5jb250ZXh0ICYmIGVycm9yLmNvbnRleHQubmFtZSkge1xuICAgICAgICByZXR1cm4gYGNhbGxzICcke2Vycm9yLmNvbnRleHQubmFtZX0nYDtcbiAgICAgIH1cbiAgICAgIHJldHVybiBgY2FsbHMgYSBmdW5jdGlvbmA7XG4gICAgY2FzZSBSRUZFUkVOQ0VfVE9fTE9DQUxfU1lNQk9MOlxuICAgICAgaWYgKGVycm9yLmNvbnRleHQgJiYgZXJyb3IuY29udGV4dC5uYW1lKSB7XG4gICAgICAgIHJldHVybiBgcmVmZXJlbmNlcyBsb2NhbCB2YXJpYWJsZSAke2Vycm9yLmNvbnRleHQubmFtZX1gO1xuICAgICAgfVxuICAgICAgcmV0dXJuIGByZWZlcmVuY2VzIGEgbG9jYWwgdmFyaWFibGVgO1xuICB9XG4gIHJldHVybiAnY29udGFpbnMgdGhlIGVycm9yJztcbn1cblxuZnVuY3Rpb24gbWFwU3RyaW5nTWFwKGlucHV0OiB7W2tleTogc3RyaW5nXTogYW55fSwgdHJhbnNmb3JtOiAodmFsdWU6IGFueSwga2V5OiBzdHJpbmcpID0+IGFueSk6XG4gICAge1trZXk6IHN0cmluZ106IGFueX0ge1xuICBpZiAoIWlucHV0KSByZXR1cm4ge307XG4gIGNvbnN0IHJlc3VsdDoge1trZXk6IHN0cmluZ106IGFueX0gPSB7fTtcbiAgT2JqZWN0LmtleXMoaW5wdXQpLmZvckVhY2goKGtleSkgPT4ge1xuICAgIGNvbnN0IHZhbHVlID0gdHJhbnNmb3JtKGlucHV0W2tleV0sIGtleSk7XG4gICAgaWYgKCFzaG91bGRJZ25vcmUodmFsdWUpKSB7XG4gICAgICBpZiAoSElEREVOX0tFWS50ZXN0KGtleSkpIHtcbiAgICAgICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KHJlc3VsdCwga2V5LCB7ZW51bWVyYWJsZTogZmFsc2UsIGNvbmZpZ3VyYWJsZTogdHJ1ZSwgdmFsdWU6IHZhbHVlfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXN1bHRba2V5XSA9IHZhbHVlO1xuICAgICAgfVxuICAgIH1cbiAgfSk7XG4gIHJldHVybiByZXN1bHQ7XG59XG5cbmZ1bmN0aW9uIGlzUHJpbWl0aXZlKG86IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gbyA9PT0gbnVsbCB8fCAodHlwZW9mIG8gIT09ICdmdW5jdGlvbicgJiYgdHlwZW9mIG8gIT09ICdvYmplY3QnKTtcbn1cblxuaW50ZXJmYWNlIEJpbmRpbmdTY29wZUJ1aWxkZXIge1xuICBkZWZpbmUobmFtZTogc3RyaW5nLCB2YWx1ZTogYW55KTogQmluZGluZ1Njb3BlQnVpbGRlcjtcbiAgZG9uZSgpOiBCaW5kaW5nU2NvcGU7XG59XG5cbmFic3RyYWN0IGNsYXNzIEJpbmRpbmdTY29wZSB7XG4gIGFic3RyYWN0IHJlc29sdmUobmFtZTogc3RyaW5nKTogYW55O1xuICBwdWJsaWMgc3RhdGljIG1pc3NpbmcgPSB7fTtcbiAgcHVibGljIHN0YXRpYyBlbXB0eTogQmluZGluZ1Njb3BlID0ge3Jlc29sdmU6IG5hbWUgPT4gQmluZGluZ1Njb3BlLm1pc3Npbmd9O1xuXG4gIHB1YmxpYyBzdGF0aWMgYnVpbGQoKTogQmluZGluZ1Njb3BlQnVpbGRlciB7XG4gICAgY29uc3QgY3VycmVudCA9IG5ldyBNYXA8c3RyaW5nLCBhbnk+KCk7XG4gICAgcmV0dXJuIHtcbiAgICAgIGRlZmluZTogZnVuY3Rpb24obmFtZSwgdmFsdWUpIHtcbiAgICAgICAgY3VycmVudC5zZXQobmFtZSwgdmFsdWUpO1xuICAgICAgICByZXR1cm4gdGhpcztcbiAgICAgIH0sXG4gICAgICBkb25lOiBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuIGN1cnJlbnQuc2l6ZSA+IDAgPyBuZXcgUG9wdWxhdGVkU2NvcGUoY3VycmVudCkgOiBCaW5kaW5nU2NvcGUuZW1wdHk7XG4gICAgICB9XG4gICAgfTtcbiAgfVxufVxuXG5jbGFzcyBQb3B1bGF0ZWRTY29wZSBleHRlbmRzIEJpbmRpbmdTY29wZSB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgYmluZGluZ3M6IE1hcDxzdHJpbmcsIGFueT4pIHsgc3VwZXIoKTsgfVxuXG4gIHJlc29sdmUobmFtZTogc3RyaW5nKTogYW55IHtcbiAgICByZXR1cm4gdGhpcy5iaW5kaW5ncy5oYXMobmFtZSkgPyB0aGlzLmJpbmRpbmdzLmdldChuYW1lKSA6IEJpbmRpbmdTY29wZS5taXNzaW5nO1xuICB9XG59XG5cbmZ1bmN0aW9uIGZvcm1hdE1ldGFkYXRhTWVzc2FnZUNoYWluKFxuICAgIGNoYWluOiBNZXRhZGF0YU1lc3NhZ2VDaGFpbiwgYWR2aXNlOiBzdHJpbmcgfCB1bmRlZmluZWQpOiBGb3JtYXR0ZWRNZXNzYWdlQ2hhaW4ge1xuICBjb25zdCBleHBhbmRlZCA9IGV4cGFuZGVkTWVzc2FnZShjaGFpbi5tZXNzYWdlLCBjaGFpbi5jb250ZXh0KTtcbiAgY29uc3QgbmVzdGluZyA9IGNoYWluLnN5bWJvbCA/IGAgaW4gJyR7Y2hhaW4uc3ltYm9sLm5hbWV9J2AgOiAnJztcbiAgY29uc3QgbWVzc2FnZSA9IGAke2V4cGFuZGVkfSR7bmVzdGluZ31gO1xuICBjb25zdCBwb3NpdGlvbiA9IGNoYWluLnBvc2l0aW9uO1xuICBjb25zdCBuZXh0OiBGb3JtYXR0ZWRNZXNzYWdlQ2hhaW58dW5kZWZpbmVkID0gY2hhaW4ubmV4dCA/XG4gICAgICBmb3JtYXRNZXRhZGF0YU1lc3NhZ2VDaGFpbihjaGFpbi5uZXh0LCBhZHZpc2UpIDpcbiAgICAgIGFkdmlzZSA/IHttZXNzYWdlOiBhZHZpc2V9IDogdW5kZWZpbmVkO1xuICByZXR1cm4ge21lc3NhZ2UsIHBvc2l0aW9uLCBuZXh0fTtcbn1cblxuZnVuY3Rpb24gZm9ybWF0TWV0YWRhdGFFcnJvcihlOiBFcnJvciwgY29udGV4dDogU3RhdGljU3ltYm9sKTogRXJyb3Ige1xuICBpZiAoaXNNZXRhZGF0YUVycm9yKGUpKSB7XG4gICAgLy8gUHJvZHVjZSBhIGZvcm1hdHRlZCB2ZXJzaW9uIG9mIHRoZSBhbmQgbGVhdmluZyBlbm91Z2ggaW5mb3JtYXRpb24gaW4gdGhlIG9yaWdpbmFsIGVycm9yXG4gICAgLy8gdG8gcmVjb3ZlciB0aGUgZm9ybWF0dGluZyBpbmZvcm1hdGlvbiB0byBldmVudHVhbGx5IHByb2R1Y2UgYSBkaWFnbm9zdGljIGVycm9yIG1lc3NhZ2UuXG4gICAgY29uc3QgcG9zaXRpb24gPSBlLnBvc2l0aW9uO1xuICAgIGNvbnN0IGNoYWluOiBNZXRhZGF0YU1lc3NhZ2VDaGFpbiA9IHtcbiAgICAgIG1lc3NhZ2U6IGBFcnJvciBkdXJpbmcgdGVtcGxhdGUgY29tcGlsZSBvZiAnJHtjb250ZXh0Lm5hbWV9J2AsXG4gICAgICBwb3NpdGlvbjogcG9zaXRpb24sXG4gICAgICBuZXh0OiB7bWVzc2FnZTogZS5tZXNzYWdlLCBuZXh0OiBlLmNoYWluLCBjb250ZXh0OiBlLmNvbnRleHQsIHN5bWJvbDogZS5zeW1ib2x9XG4gICAgfTtcbiAgICBjb25zdCBhZHZpc2UgPSBlLmFkdmlzZSB8fCBtZXNzYWdlQWR2aXNlKGUubWVzc2FnZSwgZS5jb250ZXh0KTtcbiAgICByZXR1cm4gZm9ybWF0dGVkRXJyb3IoZm9ybWF0TWV0YWRhdGFNZXNzYWdlQ2hhaW4oY2hhaW4sIGFkdmlzZSkpO1xuICB9XG4gIHJldHVybiBlO1xufVxuIl19