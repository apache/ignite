/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { rendererTypeName, tokenReference, viewClassName } from '../compile_metadata';
import { BindingForm, EventHandlerVars, convertActionBinding, convertPropertyBinding, convertPropertyBindingBuiltins } from '../compiler_util/expression_converter';
import { ChangeDetectionStrategy } from '../core';
import { Identifiers } from '../identifiers';
import { LifecycleHooks } from '../lifecycle_reflector';
import { isNgContainer } from '../ml_parser/tags';
import * as o from '../output/output_ast';
import { convertValueToOutputAst } from '../output/value_util';
import { ElementAst, EmbeddedTemplateAst, NgContentAst, templateVisitAll } from '../template_parser/template_ast';
import { componentFactoryResolverProviderDef, depDef, lifecycleHookToNodeFlag, providerDef } from './provider_compiler';
var CLASS_ATTR = 'class';
var STYLE_ATTR = 'style';
var IMPLICIT_TEMPLATE_VAR = '\$implicit';
var ViewCompileResult = /** @class */ (function () {
    function ViewCompileResult(viewClassVar, rendererTypeVar) {
        this.viewClassVar = viewClassVar;
        this.rendererTypeVar = rendererTypeVar;
    }
    return ViewCompileResult;
}());
export { ViewCompileResult };
var ViewCompiler = /** @class */ (function () {
    function ViewCompiler(_reflector) {
        this._reflector = _reflector;
    }
    ViewCompiler.prototype.compileComponent = function (outputCtx, component, template, styles, usedPipes) {
        var _a;
        var _this = this;
        var embeddedViewCount = 0;
        var staticQueryIds = findStaticQueryIds(template);
        var renderComponentVarName = undefined;
        if (!component.isHost) {
            var template_1 = component.template;
            var customRenderData = [];
            if (template_1.animations && template_1.animations.length) {
                customRenderData.push(new o.LiteralMapEntry('animation', convertValueToOutputAst(outputCtx, template_1.animations), true));
            }
            var renderComponentVar = o.variable(rendererTypeName(component.type.reference));
            renderComponentVarName = renderComponentVar.name;
            outputCtx.statements.push(renderComponentVar
                .set(o.importExpr(Identifiers.createRendererType2).callFn([new o.LiteralMapExpr([
                    new o.LiteralMapEntry('encapsulation', o.literal(template_1.encapsulation), false),
                    new o.LiteralMapEntry('styles', styles, false),
                    new o.LiteralMapEntry('data', new o.LiteralMapExpr(customRenderData), false)
                ])]))
                .toDeclStmt(o.importType(Identifiers.RendererType2), [o.StmtModifier.Final, o.StmtModifier.Exported]));
        }
        var viewBuilderFactory = function (parent) {
            var embeddedViewIndex = embeddedViewCount++;
            return new ViewBuilder(_this._reflector, outputCtx, parent, component, embeddedViewIndex, usedPipes, staticQueryIds, viewBuilderFactory);
        };
        var visitor = viewBuilderFactory(null);
        visitor.visitAll([], template);
        (_a = outputCtx.statements).push.apply(_a, tslib_1.__spread(visitor.build()));
        return new ViewCompileResult(visitor.viewName, renderComponentVarName);
    };
    return ViewCompiler;
}());
export { ViewCompiler };
var LOG_VAR = o.variable('_l');
var VIEW_VAR = o.variable('_v');
var CHECK_VAR = o.variable('_ck');
var COMP_VAR = o.variable('_co');
var EVENT_NAME_VAR = o.variable('en');
var ALLOW_DEFAULT_VAR = o.variable("ad");
var ViewBuilder = /** @class */ (function () {
    function ViewBuilder(reflector, outputCtx, parent, component, embeddedViewIndex, usedPipes, staticQueryIds, viewBuilderFactory) {
        this.reflector = reflector;
        this.outputCtx = outputCtx;
        this.parent = parent;
        this.component = component;
        this.embeddedViewIndex = embeddedViewIndex;
        this.usedPipes = usedPipes;
        this.staticQueryIds = staticQueryIds;
        this.viewBuilderFactory = viewBuilderFactory;
        this.nodes = [];
        this.purePipeNodeIndices = Object.create(null);
        // Need Object.create so that we don't have builtin values...
        this.refNodeIndices = Object.create(null);
        this.variables = [];
        this.children = [];
        // TODO(tbosch): The old view compiler used to use an `any` type
        // for the context in any embedded view. We keep this behaivor for now
        // to be able to introduce the new view compiler without too many errors.
        this.compType = this.embeddedViewIndex > 0 ?
            o.DYNAMIC_TYPE :
            o.expressionType(outputCtx.importExpr(this.component.type.reference));
        this.viewName = viewClassName(this.component.type.reference, this.embeddedViewIndex);
    }
    ViewBuilder.prototype.visitAll = function (variables, astNodes) {
        var _this = this;
        this.variables = variables;
        // create the pipes for the pure pipes immediately, so that we know their indices.
        if (!this.parent) {
            this.usedPipes.forEach(function (pipe) {
                if (pipe.pure) {
                    _this.purePipeNodeIndices[pipe.name] = _this._createPipe(null, pipe);
                }
            });
        }
        if (!this.parent) {
            var queryIds_1 = staticViewQueryIds(this.staticQueryIds);
            this.component.viewQueries.forEach(function (query, queryIndex) {
                // Note: queries start with id 1 so we can use the number in a Bloom filter!
                var queryId = queryIndex + 1;
                var bindingType = query.first ? 0 /* First */ : 1 /* All */;
                var flags = 134217728 /* TypeViewQuery */ | calcStaticDynamicQueryFlags(queryIds_1, queryId, query);
                _this.nodes.push(function () { return ({
                    sourceSpan: null,
                    nodeFlags: flags,
                    nodeDef: o.importExpr(Identifiers.queryDef).callFn([
                        o.literal(flags), o.literal(queryId),
                        new o.LiteralMapExpr([new o.LiteralMapEntry(query.propertyName, o.literal(bindingType), false)])
                    ])
                }); });
            });
        }
        templateVisitAll(this, astNodes);
        if (this.parent && (astNodes.length === 0 || needsAdditionalRootNode(astNodes))) {
            // if the view is an embedded view, then we need to add an additional root node in some cases
            this.nodes.push(function () { return ({
                sourceSpan: null,
                nodeFlags: 1 /* TypeElement */,
                nodeDef: o.importExpr(Identifiers.anchorDef).callFn([
                    o.literal(0 /* None */), o.NULL_EXPR, o.NULL_EXPR, o.literal(0)
                ])
            }); });
        }
    };
    ViewBuilder.prototype.build = function (targetStatements) {
        if (targetStatements === void 0) { targetStatements = []; }
        this.children.forEach(function (child) { return child.build(targetStatements); });
        var _a = this._createNodeExpressions(), updateRendererStmts = _a.updateRendererStmts, updateDirectivesStmts = _a.updateDirectivesStmts, nodeDefExprs = _a.nodeDefExprs;
        var updateRendererFn = this._createUpdateFn(updateRendererStmts);
        var updateDirectivesFn = this._createUpdateFn(updateDirectivesStmts);
        var viewFlags = 0 /* None */;
        if (!this.parent && this.component.changeDetection === ChangeDetectionStrategy.OnPush) {
            viewFlags |= 2 /* OnPush */;
        }
        var viewFactory = new o.DeclareFunctionStmt(this.viewName, [new o.FnParam(LOG_VAR.name)], [new o.ReturnStatement(o.importExpr(Identifiers.viewDef).callFn([
                o.literal(viewFlags),
                o.literalArr(nodeDefExprs),
                updateDirectivesFn,
                updateRendererFn,
            ]))], o.importType(Identifiers.ViewDefinition), this.embeddedViewIndex === 0 ? [o.StmtModifier.Exported] : []);
        targetStatements.push(viewFactory);
        return targetStatements;
    };
    ViewBuilder.prototype._createUpdateFn = function (updateStmts) {
        var updateFn;
        if (updateStmts.length > 0) {
            var preStmts = [];
            if (!this.component.isHost && o.findReadVarNames(updateStmts).has(COMP_VAR.name)) {
                preStmts.push(COMP_VAR.set(VIEW_VAR.prop('component')).toDeclStmt(this.compType));
            }
            updateFn = o.fn([
                new o.FnParam(CHECK_VAR.name, o.INFERRED_TYPE),
                new o.FnParam(VIEW_VAR.name, o.INFERRED_TYPE)
            ], tslib_1.__spread(preStmts, updateStmts), o.INFERRED_TYPE);
        }
        else {
            updateFn = o.NULL_EXPR;
        }
        return updateFn;
    };
    ViewBuilder.prototype.visitNgContent = function (ast, context) {
        // ngContentDef(ngContentIndex: number, index: number): NodeDef;
        this.nodes.push(function () { return ({
            sourceSpan: ast.sourceSpan,
            nodeFlags: 8 /* TypeNgContent */,
            nodeDef: o.importExpr(Identifiers.ngContentDef).callFn([
                o.literal(ast.ngContentIndex), o.literal(ast.index)
            ])
        }); });
    };
    ViewBuilder.prototype.visitText = function (ast, context) {
        // Static text nodes have no check function
        var checkIndex = -1;
        this.nodes.push(function () { return ({
            sourceSpan: ast.sourceSpan,
            nodeFlags: 2 /* TypeText */,
            nodeDef: o.importExpr(Identifiers.textDef).callFn([
                o.literal(checkIndex),
                o.literal(ast.ngContentIndex),
                o.literalArr([o.literal(ast.value)]),
            ])
        }); });
    };
    ViewBuilder.prototype.visitBoundText = function (ast, context) {
        var _this = this;
        var nodeIndex = this.nodes.length;
        // reserve the space in the nodeDefs array
        this.nodes.push(null);
        var astWithSource = ast.value;
        var inter = astWithSource.ast;
        var updateRendererExpressions = inter.expressions.map(function (expr, bindingIndex) { return _this._preprocessUpdateExpression({ nodeIndex: nodeIndex, bindingIndex: bindingIndex, sourceSpan: ast.sourceSpan, context: COMP_VAR, value: expr }); });
        // Check index is the same as the node index during compilation
        // They might only differ at runtime
        var checkIndex = nodeIndex;
        this.nodes[nodeIndex] = function () { return ({
            sourceSpan: ast.sourceSpan,
            nodeFlags: 2 /* TypeText */,
            nodeDef: o.importExpr(Identifiers.textDef).callFn([
                o.literal(checkIndex),
                o.literal(ast.ngContentIndex),
                o.literalArr(inter.strings.map(function (s) { return o.literal(s); })),
            ]),
            updateRenderer: updateRendererExpressions
        }); };
    };
    ViewBuilder.prototype.visitEmbeddedTemplate = function (ast, context) {
        var _this = this;
        var nodeIndex = this.nodes.length;
        // reserve the space in the nodeDefs array
        this.nodes.push(null);
        var _a = this._visitElementOrTemplate(nodeIndex, ast), flags = _a.flags, queryMatchesExpr = _a.queryMatchesExpr, hostEvents = _a.hostEvents;
        var childVisitor = this.viewBuilderFactory(this);
        this.children.push(childVisitor);
        childVisitor.visitAll(ast.variables, ast.children);
        var childCount = this.nodes.length - nodeIndex - 1;
        // anchorDef(
        //   flags: NodeFlags, matchedQueries: [string, QueryValueType][], ngContentIndex: number,
        //   childCount: number, handleEventFn?: ElementHandleEventFn, templateFactory?:
        //   ViewDefinitionFactory): NodeDef;
        this.nodes[nodeIndex] = function () { return ({
            sourceSpan: ast.sourceSpan,
            nodeFlags: 1 /* TypeElement */ | flags,
            nodeDef: o.importExpr(Identifiers.anchorDef).callFn([
                o.literal(flags),
                queryMatchesExpr,
                o.literal(ast.ngContentIndex),
                o.literal(childCount),
                _this._createElementHandleEventFn(nodeIndex, hostEvents),
                o.variable(childVisitor.viewName),
            ])
        }); };
    };
    ViewBuilder.prototype.visitElement = function (ast, context) {
        var _this = this;
        var nodeIndex = this.nodes.length;
        // reserve the space in the nodeDefs array so we can add children
        this.nodes.push(null);
        // Using a null element name creates an anchor.
        var elName = isNgContainer(ast.name) ? null : ast.name;
        var _a = this._visitElementOrTemplate(nodeIndex, ast), flags = _a.flags, usedEvents = _a.usedEvents, queryMatchesExpr = _a.queryMatchesExpr, dirHostBindings = _a.hostBindings, hostEvents = _a.hostEvents;
        var inputDefs = [];
        var updateRendererExpressions = [];
        var outputDefs = [];
        if (elName) {
            var hostBindings = ast.inputs
                .map(function (inputAst) { return ({
                context: COMP_VAR,
                inputAst: inputAst,
                dirAst: null,
            }); })
                .concat(dirHostBindings);
            if (hostBindings.length) {
                updateRendererExpressions =
                    hostBindings.map(function (hostBinding, bindingIndex) { return _this._preprocessUpdateExpression({
                        context: hostBinding.context,
                        nodeIndex: nodeIndex,
                        bindingIndex: bindingIndex,
                        sourceSpan: hostBinding.inputAst.sourceSpan,
                        value: hostBinding.inputAst.value
                    }); });
                inputDefs = hostBindings.map(function (hostBinding) { return elementBindingDef(hostBinding.inputAst, hostBinding.dirAst); });
            }
            outputDefs = usedEvents.map(function (_a) {
                var _b = tslib_1.__read(_a, 2), target = _b[0], eventName = _b[1];
                return o.literalArr([o.literal(target), o.literal(eventName)]);
            });
        }
        templateVisitAll(this, ast.children);
        var childCount = this.nodes.length - nodeIndex - 1;
        var compAst = ast.directives.find(function (dirAst) { return dirAst.directive.isComponent; });
        var compRendererType = o.NULL_EXPR;
        var compView = o.NULL_EXPR;
        if (compAst) {
            compView = this.outputCtx.importExpr(compAst.directive.componentViewType);
            compRendererType = this.outputCtx.importExpr(compAst.directive.rendererType);
        }
        // Check index is the same as the node index during compilation
        // They might only differ at runtime
        var checkIndex = nodeIndex;
        this.nodes[nodeIndex] = function () { return ({
            sourceSpan: ast.sourceSpan,
            nodeFlags: 1 /* TypeElement */ | flags,
            nodeDef: o.importExpr(Identifiers.elementDef).callFn([
                o.literal(checkIndex),
                o.literal(flags),
                queryMatchesExpr,
                o.literal(ast.ngContentIndex),
                o.literal(childCount),
                o.literal(elName),
                elName ? fixedAttrsDef(ast) : o.NULL_EXPR,
                inputDefs.length ? o.literalArr(inputDefs) : o.NULL_EXPR,
                outputDefs.length ? o.literalArr(outputDefs) : o.NULL_EXPR,
                _this._createElementHandleEventFn(nodeIndex, hostEvents),
                compView,
                compRendererType,
            ]),
            updateRenderer: updateRendererExpressions
        }); };
    };
    ViewBuilder.prototype._visitElementOrTemplate = function (nodeIndex, ast) {
        var _this = this;
        var flags = 0 /* None */;
        if (ast.hasViewContainer) {
            flags |= 16777216 /* EmbeddedViews */;
        }
        var usedEvents = new Map();
        ast.outputs.forEach(function (event) {
            var _a = elementEventNameAndTarget(event, null), name = _a.name, target = _a.target;
            usedEvents.set(elementEventFullName(target, name), [target, name]);
        });
        ast.directives.forEach(function (dirAst) {
            dirAst.hostEvents.forEach(function (event) {
                var _a = elementEventNameAndTarget(event, dirAst), name = _a.name, target = _a.target;
                usedEvents.set(elementEventFullName(target, name), [target, name]);
            });
        });
        var hostBindings = [];
        var hostEvents = [];
        this._visitComponentFactoryResolverProvider(ast.directives);
        ast.providers.forEach(function (providerAst, providerIndex) {
            var dirAst = undefined;
            var dirIndex = undefined;
            ast.directives.forEach(function (localDirAst, i) {
                if (localDirAst.directive.type.reference === tokenReference(providerAst.token)) {
                    dirAst = localDirAst;
                    dirIndex = i;
                }
            });
            if (dirAst) {
                var _a = _this._visitDirective(providerAst, dirAst, dirIndex, nodeIndex, ast.references, ast.queryMatches, usedEvents, _this.staticQueryIds.get(ast)), dirHostBindings = _a.hostBindings, dirHostEvents = _a.hostEvents;
                hostBindings.push.apply(hostBindings, tslib_1.__spread(dirHostBindings));
                hostEvents.push.apply(hostEvents, tslib_1.__spread(dirHostEvents));
            }
            else {
                _this._visitProvider(providerAst, ast.queryMatches);
            }
        });
        var queryMatchExprs = [];
        ast.queryMatches.forEach(function (match) {
            var valueType = undefined;
            if (tokenReference(match.value) ===
                _this.reflector.resolveExternalReference(Identifiers.ElementRef)) {
                valueType = 0 /* ElementRef */;
            }
            else if (tokenReference(match.value) ===
                _this.reflector.resolveExternalReference(Identifiers.ViewContainerRef)) {
                valueType = 3 /* ViewContainerRef */;
            }
            else if (tokenReference(match.value) ===
                _this.reflector.resolveExternalReference(Identifiers.TemplateRef)) {
                valueType = 2 /* TemplateRef */;
            }
            if (valueType != null) {
                queryMatchExprs.push(o.literalArr([o.literal(match.queryId), o.literal(valueType)]));
            }
        });
        ast.references.forEach(function (ref) {
            var valueType = undefined;
            if (!ref.value) {
                valueType = 1 /* RenderElement */;
            }
            else if (tokenReference(ref.value) ===
                _this.reflector.resolveExternalReference(Identifiers.TemplateRef)) {
                valueType = 2 /* TemplateRef */;
            }
            if (valueType != null) {
                _this.refNodeIndices[ref.name] = nodeIndex;
                queryMatchExprs.push(o.literalArr([o.literal(ref.name), o.literal(valueType)]));
            }
        });
        ast.outputs.forEach(function (outputAst) {
            hostEvents.push({ context: COMP_VAR, eventAst: outputAst, dirAst: null });
        });
        return {
            flags: flags,
            usedEvents: Array.from(usedEvents.values()),
            queryMatchesExpr: queryMatchExprs.length ? o.literalArr(queryMatchExprs) : o.NULL_EXPR,
            hostBindings: hostBindings,
            hostEvents: hostEvents
        };
    };
    ViewBuilder.prototype._visitDirective = function (providerAst, dirAst, directiveIndex, elementNodeIndex, refs, queryMatches, usedEvents, queryIds) {
        var _this = this;
        var nodeIndex = this.nodes.length;
        // reserve the space in the nodeDefs array so we can add children
        this.nodes.push(null);
        dirAst.directive.queries.forEach(function (query, queryIndex) {
            var queryId = dirAst.contentQueryStartId + queryIndex;
            var flags = 67108864 /* TypeContentQuery */ | calcStaticDynamicQueryFlags(queryIds, queryId, query);
            var bindingType = query.first ? 0 /* First */ : 1 /* All */;
            _this.nodes.push(function () { return ({
                sourceSpan: dirAst.sourceSpan,
                nodeFlags: flags,
                nodeDef: o.importExpr(Identifiers.queryDef).callFn([
                    o.literal(flags), o.literal(queryId),
                    new o.LiteralMapExpr([new o.LiteralMapEntry(query.propertyName, o.literal(bindingType), false)])
                ]),
            }); });
        });
        // Note: the operation below might also create new nodeDefs,
        // but we don't want them to be a child of a directive,
        // as they might be a provider/pipe on their own.
        // I.e. we only allow queries as children of directives nodes.
        var childCount = this.nodes.length - nodeIndex - 1;
        var _a = this._visitProviderOrDirective(providerAst, queryMatches), flags = _a.flags, queryMatchExprs = _a.queryMatchExprs, providerExpr = _a.providerExpr, depsExpr = _a.depsExpr;
        refs.forEach(function (ref) {
            if (ref.value && tokenReference(ref.value) === tokenReference(providerAst.token)) {
                _this.refNodeIndices[ref.name] = nodeIndex;
                queryMatchExprs.push(o.literalArr([o.literal(ref.name), o.literal(4 /* Provider */)]));
            }
        });
        if (dirAst.directive.isComponent) {
            flags |= 32768 /* Component */;
        }
        var inputDefs = dirAst.inputs.map(function (inputAst, inputIndex) {
            var mapValue = o.literalArr([o.literal(inputIndex), o.literal(inputAst.directiveName)]);
            // Note: it's important to not quote the key so that we can capture renames by minifiers!
            return new o.LiteralMapEntry(inputAst.directiveName, mapValue, false);
        });
        var outputDefs = [];
        var dirMeta = dirAst.directive;
        Object.keys(dirMeta.outputs).forEach(function (propName) {
            var eventName = dirMeta.outputs[propName];
            if (usedEvents.has(eventName)) {
                // Note: it's important to not quote the key so that we can capture renames by minifiers!
                outputDefs.push(new o.LiteralMapEntry(propName, o.literal(eventName), false));
            }
        });
        var updateDirectiveExpressions = [];
        if (dirAst.inputs.length || (flags & (262144 /* DoCheck */ | 65536 /* OnInit */)) > 0) {
            updateDirectiveExpressions =
                dirAst.inputs.map(function (input, bindingIndex) { return _this._preprocessUpdateExpression({
                    nodeIndex: nodeIndex,
                    bindingIndex: bindingIndex,
                    sourceSpan: input.sourceSpan,
                    context: COMP_VAR,
                    value: input.value
                }); });
        }
        var dirContextExpr = o.importExpr(Identifiers.nodeValue).callFn([VIEW_VAR, o.literal(nodeIndex)]);
        var hostBindings = dirAst.hostProperties.map(function (inputAst) { return ({
            context: dirContextExpr,
            dirAst: dirAst,
            inputAst: inputAst,
        }); });
        var hostEvents = dirAst.hostEvents.map(function (hostEventAst) { return ({
            context: dirContextExpr,
            eventAst: hostEventAst, dirAst: dirAst,
        }); });
        // Check index is the same as the node index during compilation
        // They might only differ at runtime
        var checkIndex = nodeIndex;
        this.nodes[nodeIndex] = function () { return ({
            sourceSpan: dirAst.sourceSpan,
            nodeFlags: 16384 /* TypeDirective */ | flags,
            nodeDef: o.importExpr(Identifiers.directiveDef).callFn([
                o.literal(checkIndex),
                o.literal(flags),
                queryMatchExprs.length ? o.literalArr(queryMatchExprs) : o.NULL_EXPR,
                o.literal(childCount),
                providerExpr,
                depsExpr,
                inputDefs.length ? new o.LiteralMapExpr(inputDefs) : o.NULL_EXPR,
                outputDefs.length ? new o.LiteralMapExpr(outputDefs) : o.NULL_EXPR,
            ]),
            updateDirectives: updateDirectiveExpressions,
            directive: dirAst.directive.type,
        }); };
        return { hostBindings: hostBindings, hostEvents: hostEvents };
    };
    ViewBuilder.prototype._visitProvider = function (providerAst, queryMatches) {
        this._addProviderNode(this._visitProviderOrDirective(providerAst, queryMatches));
    };
    ViewBuilder.prototype._visitComponentFactoryResolverProvider = function (directives) {
        var componentDirMeta = directives.find(function (dirAst) { return dirAst.directive.isComponent; });
        if (componentDirMeta && componentDirMeta.directive.entryComponents.length) {
            var _a = componentFactoryResolverProviderDef(this.reflector, this.outputCtx, 8192 /* PrivateProvider */, componentDirMeta.directive.entryComponents), providerExpr = _a.providerExpr, depsExpr = _a.depsExpr, flags = _a.flags, tokenExpr = _a.tokenExpr;
            this._addProviderNode({
                providerExpr: providerExpr,
                depsExpr: depsExpr,
                flags: flags,
                tokenExpr: tokenExpr,
                queryMatchExprs: [],
                sourceSpan: componentDirMeta.sourceSpan
            });
        }
    };
    ViewBuilder.prototype._addProviderNode = function (data) {
        var nodeIndex = this.nodes.length;
        // providerDef(
        //   flags: NodeFlags, matchedQueries: [string, QueryValueType][], token:any,
        //   value: any, deps: ([DepFlags, any] | any)[]): NodeDef;
        this.nodes.push(function () { return ({
            sourceSpan: data.sourceSpan,
            nodeFlags: data.flags,
            nodeDef: o.importExpr(Identifiers.providerDef).callFn([
                o.literal(data.flags),
                data.queryMatchExprs.length ? o.literalArr(data.queryMatchExprs) : o.NULL_EXPR,
                data.tokenExpr, data.providerExpr, data.depsExpr
            ])
        }); });
    };
    ViewBuilder.prototype._visitProviderOrDirective = function (providerAst, queryMatches) {
        var flags = 0 /* None */;
        var queryMatchExprs = [];
        queryMatches.forEach(function (match) {
            if (tokenReference(match.value) === tokenReference(providerAst.token)) {
                queryMatchExprs.push(o.literalArr([o.literal(match.queryId), o.literal(4 /* Provider */)]));
            }
        });
        var _a = providerDef(this.outputCtx, providerAst), providerExpr = _a.providerExpr, depsExpr = _a.depsExpr, providerFlags = _a.flags, tokenExpr = _a.tokenExpr;
        return {
            flags: flags | providerFlags,
            queryMatchExprs: queryMatchExprs,
            providerExpr: providerExpr,
            depsExpr: depsExpr,
            tokenExpr: tokenExpr,
            sourceSpan: providerAst.sourceSpan
        };
    };
    ViewBuilder.prototype.getLocal = function (name) {
        if (name == EventHandlerVars.event.name) {
            return EventHandlerVars.event;
        }
        var currViewExpr = VIEW_VAR;
        for (var currBuilder = this; currBuilder; currBuilder = currBuilder.parent,
            currViewExpr = currViewExpr.prop('parent').cast(o.DYNAMIC_TYPE)) {
            // check references
            var refNodeIndex = currBuilder.refNodeIndices[name];
            if (refNodeIndex != null) {
                return o.importExpr(Identifiers.nodeValue).callFn([currViewExpr, o.literal(refNodeIndex)]);
            }
            // check variables
            var varAst = currBuilder.variables.find(function (varAst) { return varAst.name === name; });
            if (varAst) {
                var varValue = varAst.value || IMPLICIT_TEMPLATE_VAR;
                return currViewExpr.prop('context').prop(varValue);
            }
        }
        return null;
    };
    ViewBuilder.prototype.notifyImplicitReceiverUse = function () {
        // Not needed in View Engine as View Engine walks through the generated
        // expressions to figure out if the implicit receiver is used and needs
        // to be generated as part of the pre-update statements.
    };
    ViewBuilder.prototype._createLiteralArrayConverter = function (sourceSpan, argCount) {
        if (argCount === 0) {
            var valueExpr_1 = o.importExpr(Identifiers.EMPTY_ARRAY);
            return function () { return valueExpr_1; };
        }
        var checkIndex = this.nodes.length;
        this.nodes.push(function () { return ({
            sourceSpan: sourceSpan,
            nodeFlags: 32 /* TypePureArray */,
            nodeDef: o.importExpr(Identifiers.pureArrayDef).callFn([
                o.literal(checkIndex),
                o.literal(argCount),
            ])
        }); });
        return function (args) { return callCheckStmt(checkIndex, args); };
    };
    ViewBuilder.prototype._createLiteralMapConverter = function (sourceSpan, keys) {
        if (keys.length === 0) {
            var valueExpr_2 = o.importExpr(Identifiers.EMPTY_MAP);
            return function () { return valueExpr_2; };
        }
        var map = o.literalMap(keys.map(function (e, i) { return (tslib_1.__assign({}, e, { value: o.literal(i) })); }));
        var checkIndex = this.nodes.length;
        this.nodes.push(function () { return ({
            sourceSpan: sourceSpan,
            nodeFlags: 64 /* TypePureObject */,
            nodeDef: o.importExpr(Identifiers.pureObjectDef).callFn([
                o.literal(checkIndex),
                map,
            ])
        }); });
        return function (args) { return callCheckStmt(checkIndex, args); };
    };
    ViewBuilder.prototype._createPipeConverter = function (expression, name, argCount) {
        var pipe = this.usedPipes.find(function (pipeSummary) { return pipeSummary.name === name; });
        if (pipe.pure) {
            var checkIndex_1 = this.nodes.length;
            this.nodes.push(function () { return ({
                sourceSpan: expression.sourceSpan,
                nodeFlags: 128 /* TypePurePipe */,
                nodeDef: o.importExpr(Identifiers.purePipeDef).callFn([
                    o.literal(checkIndex_1),
                    o.literal(argCount),
                ])
            }); });
            // find underlying pipe in the component view
            var compViewExpr = VIEW_VAR;
            var compBuilder = this;
            while (compBuilder.parent) {
                compBuilder = compBuilder.parent;
                compViewExpr = compViewExpr.prop('parent').cast(o.DYNAMIC_TYPE);
            }
            var pipeNodeIndex = compBuilder.purePipeNodeIndices[name];
            var pipeValueExpr_1 = o.importExpr(Identifiers.nodeValue).callFn([compViewExpr, o.literal(pipeNodeIndex)]);
            return function (args) { return callUnwrapValue(expression.nodeIndex, expression.bindingIndex, callCheckStmt(checkIndex_1, [pipeValueExpr_1].concat(args))); };
        }
        else {
            var nodeIndex = this._createPipe(expression.sourceSpan, pipe);
            var nodeValueExpr_1 = o.importExpr(Identifiers.nodeValue).callFn([VIEW_VAR, o.literal(nodeIndex)]);
            return function (args) { return callUnwrapValue(expression.nodeIndex, expression.bindingIndex, nodeValueExpr_1.callMethod('transform', args)); };
        }
    };
    ViewBuilder.prototype._createPipe = function (sourceSpan, pipe) {
        var _this = this;
        var nodeIndex = this.nodes.length;
        var flags = 0 /* None */;
        pipe.type.lifecycleHooks.forEach(function (lifecycleHook) {
            // for pipes, we only support ngOnDestroy
            if (lifecycleHook === LifecycleHooks.OnDestroy) {
                flags |= lifecycleHookToNodeFlag(lifecycleHook);
            }
        });
        var depExprs = pipe.type.diDeps.map(function (diDep) { return depDef(_this.outputCtx, diDep); });
        // function pipeDef(
        //   flags: NodeFlags, ctor: any, deps: ([DepFlags, any] | any)[]): NodeDef
        this.nodes.push(function () { return ({
            sourceSpan: sourceSpan,
            nodeFlags: 16 /* TypePipe */,
            nodeDef: o.importExpr(Identifiers.pipeDef).callFn([
                o.literal(flags), _this.outputCtx.importExpr(pipe.type.reference), o.literalArr(depExprs)
            ])
        }); });
        return nodeIndex;
    };
    /**
     * For the AST in `UpdateExpression.value`:
     * - create nodes for pipes, literal arrays and, literal maps,
     * - update the AST to replace pipes, literal arrays and, literal maps with calls to check fn.
     *
     * WARNING: This might create new nodeDefs (for pipes and literal arrays and literal maps)!
     */
    ViewBuilder.prototype._preprocessUpdateExpression = function (expression) {
        var _this = this;
        return {
            nodeIndex: expression.nodeIndex,
            bindingIndex: expression.bindingIndex,
            sourceSpan: expression.sourceSpan,
            context: expression.context,
            value: convertPropertyBindingBuiltins({
                createLiteralArrayConverter: function (argCount) { return _this._createLiteralArrayConverter(expression.sourceSpan, argCount); },
                createLiteralMapConverter: function (keys) {
                    return _this._createLiteralMapConverter(expression.sourceSpan, keys);
                },
                createPipeConverter: function (name, argCount) {
                    return _this._createPipeConverter(expression, name, argCount);
                }
            }, expression.value)
        };
    };
    ViewBuilder.prototype._createNodeExpressions = function () {
        var self = this;
        var updateBindingCount = 0;
        var updateRendererStmts = [];
        var updateDirectivesStmts = [];
        var nodeDefExprs = this.nodes.map(function (factory, nodeIndex) {
            var _a = factory(), nodeDef = _a.nodeDef, nodeFlags = _a.nodeFlags, updateDirectives = _a.updateDirectives, updateRenderer = _a.updateRenderer, sourceSpan = _a.sourceSpan;
            if (updateRenderer) {
                updateRendererStmts.push.apply(updateRendererStmts, tslib_1.__spread(createUpdateStatements(nodeIndex, sourceSpan, updateRenderer, false)));
            }
            if (updateDirectives) {
                updateDirectivesStmts.push.apply(updateDirectivesStmts, tslib_1.__spread(createUpdateStatements(nodeIndex, sourceSpan, updateDirectives, (nodeFlags & (262144 /* DoCheck */ | 65536 /* OnInit */)) > 0)));
            }
            // We use a comma expression to call the log function before
            // the nodeDef function, but still use the result of the nodeDef function
            // as the value.
            // Note: We only add the logger to elements / text nodes,
            // so we don't generate too much code.
            var logWithNodeDef = nodeFlags & 3 /* CatRenderNode */ ?
                new o.CommaExpr([LOG_VAR.callFn([]).callFn([]), nodeDef]) :
                nodeDef;
            return o.applySourceSpanToExpressionIfNeeded(logWithNodeDef, sourceSpan);
        });
        return { updateRendererStmts: updateRendererStmts, updateDirectivesStmts: updateDirectivesStmts, nodeDefExprs: nodeDefExprs };
        function createUpdateStatements(nodeIndex, sourceSpan, expressions, allowEmptyExprs) {
            var updateStmts = [];
            var exprs = expressions.map(function (_a) {
                var sourceSpan = _a.sourceSpan, context = _a.context, value = _a.value;
                var bindingId = "" + updateBindingCount++;
                var nameResolver = context === COMP_VAR ? self : null;
                var _b = convertPropertyBinding(nameResolver, context, value, bindingId, BindingForm.General), stmts = _b.stmts, currValExpr = _b.currValExpr;
                updateStmts.push.apply(updateStmts, tslib_1.__spread(stmts.map(function (stmt) { return o.applySourceSpanToStatementIfNeeded(stmt, sourceSpan); })));
                return o.applySourceSpanToExpressionIfNeeded(currValExpr, sourceSpan);
            });
            if (expressions.length || allowEmptyExprs) {
                updateStmts.push(o.applySourceSpanToStatementIfNeeded(callCheckStmt(nodeIndex, exprs).toStmt(), sourceSpan));
            }
            return updateStmts;
        }
    };
    ViewBuilder.prototype._createElementHandleEventFn = function (nodeIndex, handlers) {
        var _this = this;
        var handleEventStmts = [];
        var handleEventBindingCount = 0;
        handlers.forEach(function (_a) {
            var context = _a.context, eventAst = _a.eventAst, dirAst = _a.dirAst;
            var bindingId = "" + handleEventBindingCount++;
            var nameResolver = context === COMP_VAR ? _this : null;
            var _b = convertActionBinding(nameResolver, context, eventAst.handler, bindingId), stmts = _b.stmts, allowDefault = _b.allowDefault;
            var trueStmts = stmts;
            if (allowDefault) {
                trueStmts.push(ALLOW_DEFAULT_VAR.set(allowDefault.and(ALLOW_DEFAULT_VAR)).toStmt());
            }
            var _c = elementEventNameAndTarget(eventAst, dirAst), eventTarget = _c.target, eventName = _c.name;
            var fullEventName = elementEventFullName(eventTarget, eventName);
            handleEventStmts.push(o.applySourceSpanToStatementIfNeeded(new o.IfStmt(o.literal(fullEventName).identical(EVENT_NAME_VAR), trueStmts), eventAst.sourceSpan));
        });
        var handleEventFn;
        if (handleEventStmts.length > 0) {
            var preStmts = [ALLOW_DEFAULT_VAR.set(o.literal(true)).toDeclStmt(o.BOOL_TYPE)];
            if (!this.component.isHost && o.findReadVarNames(handleEventStmts).has(COMP_VAR.name)) {
                preStmts.push(COMP_VAR.set(VIEW_VAR.prop('component')).toDeclStmt(this.compType));
            }
            handleEventFn = o.fn([
                new o.FnParam(VIEW_VAR.name, o.INFERRED_TYPE),
                new o.FnParam(EVENT_NAME_VAR.name, o.INFERRED_TYPE),
                new o.FnParam(EventHandlerVars.event.name, o.INFERRED_TYPE)
            ], tslib_1.__spread(preStmts, handleEventStmts, [new o.ReturnStatement(ALLOW_DEFAULT_VAR)]), o.INFERRED_TYPE);
        }
        else {
            handleEventFn = o.NULL_EXPR;
        }
        return handleEventFn;
    };
    ViewBuilder.prototype.visitDirective = function (ast, context) { };
    ViewBuilder.prototype.visitDirectiveProperty = function (ast, context) { };
    ViewBuilder.prototype.visitReference = function (ast, context) { };
    ViewBuilder.prototype.visitVariable = function (ast, context) { };
    ViewBuilder.prototype.visitEvent = function (ast, context) { };
    ViewBuilder.prototype.visitElementProperty = function (ast, context) { };
    ViewBuilder.prototype.visitAttr = function (ast, context) { };
    return ViewBuilder;
}());
function needsAdditionalRootNode(astNodes) {
    var lastAstNode = astNodes[astNodes.length - 1];
    if (lastAstNode instanceof EmbeddedTemplateAst) {
        return lastAstNode.hasViewContainer;
    }
    if (lastAstNode instanceof ElementAst) {
        if (isNgContainer(lastAstNode.name) && lastAstNode.children.length) {
            return needsAdditionalRootNode(lastAstNode.children);
        }
        return lastAstNode.hasViewContainer;
    }
    return lastAstNode instanceof NgContentAst;
}
function elementBindingDef(inputAst, dirAst) {
    var inputType = inputAst.type;
    switch (inputType) {
        case 1 /* Attribute */:
            return o.literalArr([
                o.literal(1 /* TypeElementAttribute */), o.literal(inputAst.name),
                o.literal(inputAst.securityContext)
            ]);
        case 0 /* Property */:
            return o.literalArr([
                o.literal(8 /* TypeProperty */), o.literal(inputAst.name),
                o.literal(inputAst.securityContext)
            ]);
        case 4 /* Animation */:
            var bindingType = 8 /* TypeProperty */ |
                (dirAst && dirAst.directive.isComponent ? 32 /* SyntheticHostProperty */ :
                    16 /* SyntheticProperty */);
            return o.literalArr([
                o.literal(bindingType), o.literal('@' + inputAst.name), o.literal(inputAst.securityContext)
            ]);
        case 2 /* Class */:
            return o.literalArr([o.literal(2 /* TypeElementClass */), o.literal(inputAst.name), o.NULL_EXPR]);
        case 3 /* Style */:
            return o.literalArr([
                o.literal(4 /* TypeElementStyle */), o.literal(inputAst.name), o.literal(inputAst.unit)
            ]);
        default:
            // This default case is not needed by TypeScript compiler, as the switch is exhaustive.
            // However Closure Compiler does not understand that and reports an error in typed mode.
            // The `throw new Error` below works around the problem, and the unexpected: never variable
            // makes sure tsc still checks this code is unreachable.
            var unexpected = inputType;
            throw new Error("unexpected " + unexpected);
    }
}
function fixedAttrsDef(elementAst) {
    var mapResult = Object.create(null);
    elementAst.attrs.forEach(function (attrAst) { mapResult[attrAst.name] = attrAst.value; });
    elementAst.directives.forEach(function (dirAst) {
        Object.keys(dirAst.directive.hostAttributes).forEach(function (name) {
            var value = dirAst.directive.hostAttributes[name];
            var prevValue = mapResult[name];
            mapResult[name] = prevValue != null ? mergeAttributeValue(name, prevValue, value) : value;
        });
    });
    // Note: We need to sort to get a defined output order
    // for tests and for caching generated artifacts...
    return o.literalArr(Object.keys(mapResult).sort().map(function (attrName) { return o.literalArr([o.literal(attrName), o.literal(mapResult[attrName])]); }));
}
function mergeAttributeValue(attrName, attrValue1, attrValue2) {
    if (attrName == CLASS_ATTR || attrName == STYLE_ATTR) {
        return attrValue1 + " " + attrValue2;
    }
    else {
        return attrValue2;
    }
}
function callCheckStmt(nodeIndex, exprs) {
    if (exprs.length > 10) {
        return CHECK_VAR.callFn([VIEW_VAR, o.literal(nodeIndex), o.literal(1 /* Dynamic */), o.literalArr(exprs)]);
    }
    else {
        return CHECK_VAR.callFn(tslib_1.__spread([VIEW_VAR, o.literal(nodeIndex), o.literal(0 /* Inline */)], exprs));
    }
}
function callUnwrapValue(nodeIndex, bindingIdx, expr) {
    return o.importExpr(Identifiers.unwrapValue).callFn([
        VIEW_VAR, o.literal(nodeIndex), o.literal(bindingIdx), expr
    ]);
}
export function findStaticQueryIds(nodes, result) {
    if (result === void 0) { result = new Map(); }
    nodes.forEach(function (node) {
        var staticQueryIds = new Set();
        var dynamicQueryIds = new Set();
        var queryMatches = undefined;
        if (node instanceof ElementAst) {
            findStaticQueryIds(node.children, result);
            node.children.forEach(function (child) {
                var childData = result.get(child);
                childData.staticQueryIds.forEach(function (queryId) { return staticQueryIds.add(queryId); });
                childData.dynamicQueryIds.forEach(function (queryId) { return dynamicQueryIds.add(queryId); });
            });
            queryMatches = node.queryMatches;
        }
        else if (node instanceof EmbeddedTemplateAst) {
            findStaticQueryIds(node.children, result);
            node.children.forEach(function (child) {
                var childData = result.get(child);
                childData.staticQueryIds.forEach(function (queryId) { return dynamicQueryIds.add(queryId); });
                childData.dynamicQueryIds.forEach(function (queryId) { return dynamicQueryIds.add(queryId); });
            });
            queryMatches = node.queryMatches;
        }
        if (queryMatches) {
            queryMatches.forEach(function (match) { return staticQueryIds.add(match.queryId); });
        }
        dynamicQueryIds.forEach(function (queryId) { return staticQueryIds.delete(queryId); });
        result.set(node, { staticQueryIds: staticQueryIds, dynamicQueryIds: dynamicQueryIds });
    });
    return result;
}
export function staticViewQueryIds(nodeStaticQueryIds) {
    var staticQueryIds = new Set();
    var dynamicQueryIds = new Set();
    Array.from(nodeStaticQueryIds.values()).forEach(function (entry) {
        entry.staticQueryIds.forEach(function (queryId) { return staticQueryIds.add(queryId); });
        entry.dynamicQueryIds.forEach(function (queryId) { return dynamicQueryIds.add(queryId); });
    });
    dynamicQueryIds.forEach(function (queryId) { return staticQueryIds.delete(queryId); });
    return { staticQueryIds: staticQueryIds, dynamicQueryIds: dynamicQueryIds };
}
function elementEventNameAndTarget(eventAst, dirAst) {
    if (eventAst.isAnimation) {
        return {
            name: "@" + eventAst.name + "." + eventAst.phase,
            target: dirAst && dirAst.directive.isComponent ? 'component' : null
        };
    }
    else {
        return eventAst;
    }
}
function calcStaticDynamicQueryFlags(queryIds, queryId, query) {
    var flags = 0 /* None */;
    // Note: We only make queries static that query for a single item.
    // This is because of backwards compatibility with the old view compiler...
    if (query.first && shouldResolveAsStaticQuery(queryIds, queryId, query)) {
        flags |= 268435456 /* StaticQuery */;
    }
    else {
        flags |= 536870912 /* DynamicQuery */;
    }
    return flags;
}
function shouldResolveAsStaticQuery(queryIds, queryId, query) {
    // If query.static has been set by the user, use that value to determine whether
    // the query is static. If none has been set, sort the query into static/dynamic
    // based on query results (i.e. dynamic if CD needs to run to get all results).
    return query.static ||
        query.static == null &&
            (queryIds.staticQueryIds.has(queryId) || !queryIds.dynamicQueryIds.has(queryId));
}
export function elementEventFullName(target, name) {
    return target ? target + ":" + name : name;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlld19jb21waWxlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy92aWV3X2NvbXBpbGVyL3ZpZXdfY29tcGlsZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBcUUsZ0JBQWdCLEVBQUUsY0FBYyxFQUFFLGFBQWEsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBRXhKLE9BQU8sRUFBQyxXQUFXLEVBQW9CLGdCQUFnQixFQUFpQixvQkFBb0IsRUFBRSxzQkFBc0IsRUFBRSw4QkFBOEIsRUFBQyxNQUFNLHVDQUF1QyxDQUFDO0FBQ25NLE9BQU8sRUFBNkIsdUJBQXVCLEVBQXlELE1BQU0sU0FBUyxDQUFDO0FBRXBJLE9BQU8sRUFBQyxXQUFXLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUMzQyxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sd0JBQXdCLENBQUM7QUFDdEQsT0FBTyxFQUFDLGFBQWEsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBQ2hELE9BQU8sS0FBSyxDQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFDMUMsT0FBTyxFQUFDLHVCQUF1QixFQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFFN0QsT0FBTyxFQUF5RyxVQUFVLEVBQUUsbUJBQW1CLEVBQUUsWUFBWSxFQUFxSCxnQkFBZ0IsRUFBQyxNQUFNLGlDQUFpQyxDQUFDO0FBRzNVLE9BQU8sRUFBQyxtQ0FBbUMsRUFBRSxNQUFNLEVBQUUsdUJBQXVCLEVBQUUsV0FBVyxFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFFdEgsSUFBTSxVQUFVLEdBQUcsT0FBTyxDQUFDO0FBQzNCLElBQU0sVUFBVSxHQUFHLE9BQU8sQ0FBQztBQUMzQixJQUFNLHFCQUFxQixHQUFHLFlBQVksQ0FBQztBQUUzQztJQUNFLDJCQUFtQixZQUFvQixFQUFTLGVBQXVCO1FBQXBELGlC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QTJCO0lBQ2xGLGdGQUFnRjtJQUNoRixnRkFBZ0Y7SUFDaEYsK0VBQStFO0lBQy9FLE9BQU8sS0FBSyxDQUFDLE1BQU07UUFDZixLQUFLLENBQUMsTUFBTSxJQUFJLElBQUk7WUFDcEIsQ0FBQyxRQUFRLENBQUMsY0FBYyxDQUFDLEdBQUcsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxlQUFlLENBQUMsR0FBRyxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUM7QUFDdkYsQ0FBQztBQUVELE1BQU0sVUFBVSxvQkFBb0IsQ0FBQyxNQUFxQixFQUFFLElBQVk7SUFDdEUsT0FBTyxNQUFNLENBQUMsQ0FBQyxDQUFJLE1BQU0sU0FBSSxJQUFNLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztBQUM3QyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgQ29tcGlsZVBpcGVTdW1tYXJ5LCBDb21waWxlUXVlcnlNZXRhZGF0YSwgcmVuZGVyZXJUeXBlTmFtZSwgdG9rZW5SZWZlcmVuY2UsIHZpZXdDbGFzc05hbWV9IGZyb20gJy4uL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuLi9jb21waWxlX3JlZmxlY3Rvcic7XG5pbXBvcnQge0JpbmRpbmdGb3JtLCBCdWlsdGluQ29udmVydGVyLCBFdmVudEhhbmRsZXJWYXJzLCBMb2NhbFJlc29sdmVyLCBjb252ZXJ0QWN0aW9uQmluZGluZywgY29udmVydFByb3BlcnR5QmluZGluZywgY29udmVydFByb3BlcnR5QmluZGluZ0J1aWx0aW5zfSBmcm9tICcuLi9jb21waWxlcl91dGlsL2V4cHJlc3Npb25fY29udmVydGVyJztcbmltcG9ydCB7QXJndW1lbnRUeXBlLCBCaW5kaW5nRmxhZ3MsIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LCBOb2RlRmxhZ3MsIFF1ZXJ5QmluZGluZ1R5cGUsIFF1ZXJ5VmFsdWVUeXBlLCBWaWV3RmxhZ3N9IGZyb20gJy4uL2NvcmUnO1xuaW1wb3J0IHtBU1QsIEFTVFdpdGhTb3VyY2UsIEludGVycG9sYXRpb259IGZyb20gJy4uL2V4cHJlc3Npb25fcGFyc2VyL2FzdCc7XG5pbXBvcnQge0lkZW50aWZpZXJzfSBmcm9tICcuLi9pZGVudGlmaWVycyc7XG5pbXBvcnQge0xpZmVjeWNsZUhvb2tzfSBmcm9tICcuLi9saWZlY3ljbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7aXNOZ0NvbnRhaW5lcn0gZnJvbSAnLi4vbWxfcGFyc2VyL3RhZ3MnO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuLi9vdXRwdXQvb3V0cHV0X2FzdCc7XG5pbXBvcnQge2NvbnZlcnRWYWx1ZVRvT3V0cHV0QXN0fSBmcm9tICcuLi9vdXRwdXQvdmFsdWVfdXRpbCc7XG5pbXBvcnQge1BhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5pbXBvcnQge0F0dHJBc3QsIEJvdW5kRGlyZWN0aXZlUHJvcGVydHlBc3QsIEJvdW5kRWxlbWVudFByb3BlcnR5QXN0LCBCb3VuZEV2ZW50QXN0LCBCb3VuZFRleHRBc3QsIERpcmVjdGl2ZUFzdCwgRWxlbWVudEFzdCwgRW1iZWRkZWRUZW1wbGF0ZUFzdCwgTmdDb250ZW50QXN0LCBQcm9wZXJ0eUJpbmRpbmdUeXBlLCBQcm92aWRlckFzdCwgUXVlcnlNYXRjaCwgUmVmZXJlbmNlQXN0LCBUZW1wbGF0ZUFzdCwgVGVtcGxhdGVBc3RWaXNpdG9yLCBUZXh0QXN0LCBWYXJpYWJsZUFzdCwgdGVtcGxhdGVWaXNpdEFsbH0gZnJvbSAnLi4vdGVtcGxhdGVfcGFyc2VyL3RlbXBsYXRlX2FzdCc7XG5pbXBvcnQge091dHB1dENvbnRleHR9IGZyb20gJy4uL3V0aWwnO1xuXG5pbXBvcnQge2NvbXBvbmVudEZhY3RvcnlSZXNvbHZlclByb3ZpZGVyRGVmLCBkZXBEZWYsIGxpZmVjeWNsZUhvb2tUb05vZGVGbGFnLCBwcm92aWRlckRlZn0gZnJvbSAnLi9wcm92aWRlcl9jb21waWxlcic7XG5cbmNvbnN0IENMQVNTX0FUVFIgPSAnY2xhc3MnO1xuY29uc3QgU1RZTEVfQVRUUiA9ICdzdHlsZSc7XG5jb25zdCBJTVBMSUNJVF9URU1QTEFURV9WQVIgPSAnXFwkaW1wbGljaXQnO1xuXG5leHBvcnQgY2xhc3MgVmlld0NvbXBpbGVSZXN1bHQge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgdmlld0NsYXNzVmFyOiBzdHJpbmcsIHB1YmxpYyByZW5kZXJlclR5cGVWYXI6IHN0cmluZykge31cbn1cblxuZXhwb3J0IGNsYXNzIFZpZXdDb21waWxlciB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX3JlZmxlY3RvcjogQ29tcGlsZVJlZmxlY3Rvcikge31cblxuICBjb21waWxlQ29tcG9uZW50KFxuICAgICAgb3V0cHV0Q3R4OiBPdXRwdXRDb250ZXh0LCBjb21wb25lbnQ6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgdGVtcGxhdGU6IFRlbXBsYXRlQXN0W10sXG4gICAgICBzdHlsZXM6IG8uRXhwcmVzc2lvbiwgdXNlZFBpcGVzOiBDb21waWxlUGlwZVN1bW1hcnlbXSk6IFZpZXdDb21waWxlUmVzdWx0IHtcbiAgICBsZXQgZW1iZWRkZWRWaWV3Q291bnQgPSAwO1xuICAgIGNvbnN0IHN0YXRpY1F1ZXJ5SWRzID0gZmluZFN0YXRpY1F1ZXJ5SWRzKHRlbXBsYXRlKTtcblxuICAgIGxldCByZW5kZXJDb21wb25lbnRWYXJOYW1lOiBzdHJpbmcgPSB1bmRlZmluZWQgITtcbiAgICBpZiAoIWNvbXBvbmVudC5pc0hvc3QpIHtcbiAgICAgIGNvbnN0IHRlbXBsYXRlID0gY29tcG9uZW50LnRlbXBsYXRlICE7XG4gICAgICBjb25zdCBjdXN0b21SZW5kZXJEYXRhOiBvLkxpdGVyYWxNYXBFbnRyeVtdID0gW107XG4gICAgICBpZiAodGVtcGxhdGUuYW5pbWF0aW9ucyAmJiB0ZW1wbGF0ZS5hbmltYXRpb25zLmxlbmd0aCkge1xuICAgICAgICBjdXN0b21SZW5kZXJEYXRhLnB1c2gobmV3IG8uTGl0ZXJhbE1hcEVudHJ5KFxuICAgICAgICAgICAgJ2FuaW1hdGlvbicsIGNvbnZlcnRWYWx1ZVRvT3V0cHV0QXN0KG91dHB1dEN0eCwgdGVtcGxhdGUuYW5pbWF0aW9ucyksIHRydWUpKTtcbiAgICAgIH1cblxuICAgICAgY29uc3QgcmVuZGVyQ29tcG9uZW50VmFyID0gby52YXJpYWJsZShyZW5kZXJlclR5cGVOYW1lKGNvbXBvbmVudC50eXBlLnJlZmVyZW5jZSkpO1xuICAgICAgcmVuZGVyQ29tcG9uZW50VmFyTmFtZSA9IHJlbmRlckNvbXBvbmVudFZhci5uYW1lICE7XG4gICAgICBvdXRwdXRDdHguc3RhdGVtZW50cy5wdXNoKFxuICAgICAgICAgIHJlbmRlckNvbXBvbmVudFZhclxuICAgICAgICAgICAgICAuc2V0KG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5jcmVhdGVSZW5kZXJlclR5cGUyKS5jYWxsRm4oW25ldyBvLkxpdGVyYWxNYXBFeHByKFtcbiAgICAgICAgICAgICAgICBuZXcgby5MaXRlcmFsTWFwRW50cnkoJ2VuY2Fwc3VsYXRpb24nLCBvLmxpdGVyYWwodGVtcGxhdGUuZW5jYXBzdWxhdGlvbiksIGZhbHNlKSxcbiAgICAgICAgICAgICAgICBuZXcgby5MaXRlcmFsTWFwRW50cnkoJ3N0eWxlcycsIHN0eWxlcywgZmFsc2UpLFxuICAgICAgICAgICAgICAgIG5ldyBvLkxpdGVyYWxNYXBFbnRyeSgnZGF0YScsIG5ldyBvLkxpdGVyYWxNYXBFeHByKGN1c3RvbVJlbmRlckRhdGEpLCBmYWxzZSlcbiAgICAgICAgICAgICAgXSldKSlcbiAgICAgICAgICAgICAgLnRvRGVjbFN0bXQoXG4gICAgICAgICAgICAgICAgICBvLmltcG9ydFR5cGUoSWRlbnRpZmllcnMuUmVuZGVyZXJUeXBlMiksXG4gICAgICAgICAgICAgICAgICBbby5TdG10TW9kaWZpZXIuRmluYWwsIG8uU3RtdE1vZGlmaWVyLkV4cG9ydGVkXSkpO1xuICAgIH1cblxuICAgIGNvbnN0IHZpZXdCdWlsZGVyRmFjdG9yeSA9IChwYXJlbnQ6IFZpZXdCdWlsZGVyIHwgbnVsbCk6IFZpZXdCdWlsZGVyID0+IHtcbiAgICAgIGNvbnN0IGVtYmVkZGVkVmlld0luZGV4ID0gZW1iZWRkZWRWaWV3Q291bnQrKztcbiAgICAgIHJldHVybiBuZXcgVmlld0J1aWxkZXIoXG4gICAgICAgICAgdGhpcy5fcmVmbGVjdG9yLCBvdXRwdXRDdHgsIHBhcmVudCwgY29tcG9uZW50LCBlbWJlZGRlZFZpZXdJbmRleCwgdXNlZFBpcGVzLFxuICAgICAgICAgIHN0YXRpY1F1ZXJ5SWRzLCB2aWV3QnVpbGRlckZhY3RvcnkpO1xuICAgIH07XG5cbiAgICBjb25zdCB2aXNpdG9yID0gdmlld0J1aWxkZXJGYWN0b3J5KG51bGwpO1xuICAgIHZpc2l0b3IudmlzaXRBbGwoW10sIHRlbXBsYXRlKTtcblxuICAgIG91dHB1dEN0eC5zdGF0ZW1lbnRzLnB1c2goLi4udmlzaXRvci5idWlsZCgpKTtcblxuICAgIHJldHVybiBuZXcgVmlld0NvbXBpbGVSZXN1bHQodmlzaXRvci52aWV3TmFtZSwgcmVuZGVyQ29tcG9uZW50VmFyTmFtZSk7XG4gIH1cbn1cblxuaW50ZXJmYWNlIFZpZXdCdWlsZGVyRmFjdG9yeSB7XG4gIChwYXJlbnQ6IFZpZXdCdWlsZGVyKTogVmlld0J1aWxkZXI7XG59XG5cbmludGVyZmFjZSBVcGRhdGVFeHByZXNzaW9uIHtcbiAgY29udGV4dDogby5FeHByZXNzaW9uO1xuICBub2RlSW5kZXg6IG51bWJlcjtcbiAgYmluZGluZ0luZGV4OiBudW1iZXI7XG4gIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhbjtcbiAgdmFsdWU6IEFTVDtcbn1cblxuY29uc3QgTE9HX1ZBUiA9IG8udmFyaWFibGUoJ19sJyk7XG5jb25zdCBWSUVXX1ZBUiA9IG8udmFyaWFibGUoJ192Jyk7XG5jb25zdCBDSEVDS19WQVIgPSBvLnZhcmlhYmxlKCdfY2snKTtcbmNvbnN0IENPTVBfVkFSID0gby52YXJpYWJsZSgnX2NvJyk7XG5jb25zdCBFVkVOVF9OQU1FX1ZBUiA9IG8udmFyaWFibGUoJ2VuJyk7XG5jb25zdCBBTExPV19ERUZBVUxUX1ZBUiA9IG8udmFyaWFibGUoYGFkYCk7XG5cbmNsYXNzIFZpZXdCdWlsZGVyIGltcGxlbWVudHMgVGVtcGxhdGVBc3RWaXNpdG9yLCBMb2NhbFJlc29sdmVyIHtcbiAgcHJpdmF0ZSBjb21wVHlwZTogby5UeXBlO1xuICBwcml2YXRlIG5vZGVzOiAoKCkgPT4ge1xuICAgIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhbiB8IG51bGwsXG4gICAgbm9kZURlZjogby5FeHByZXNzaW9uLFxuICAgIG5vZGVGbGFnczogTm9kZUZsYWdzLCB1cGRhdGVEaXJlY3RpdmVzPzogVXBkYXRlRXhwcmVzc2lvbltdLCB1cGRhdGVSZW5kZXJlcj86IFVwZGF0ZUV4cHJlc3Npb25bXVxuICB9KVtdID0gW107XG4gIHByaXZhdGUgcHVyZVBpcGVOb2RlSW5kaWNlczoge1twaXBlTmFtZTogc3RyaW5nXTogbnVtYmVyfSA9IE9iamVjdC5jcmVhdGUobnVsbCk7XG4gIC8vIE5lZWQgT2JqZWN0LmNyZWF0ZSBzbyB0aGF0IHdlIGRvbid0IGhhdmUgYnVpbHRpbiB2YWx1ZXMuLi5cbiAgcHJpdmF0ZSByZWZOb2RlSW5kaWNlczoge1tyZWZOYW1lOiBzdHJpbmddOiBudW1iZXJ9ID0gT2JqZWN0LmNyZWF0ZShudWxsKTtcbiAgcHJpdmF0ZSB2YXJpYWJsZXM6IFZhcmlhYmxlQXN0W10gPSBbXTtcbiAgcHJpdmF0ZSBjaGlsZHJlbjogVmlld0J1aWxkZXJbXSA9IFtdO1xuXG4gIHB1YmxpYyByZWFkb25seSB2aWV3TmFtZTogc3RyaW5nO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSByZWZsZWN0b3I6IENvbXBpbGVSZWZsZWN0b3IsIHByaXZhdGUgb3V0cHV0Q3R4OiBPdXRwdXRDb250ZXh0LFxuICAgICAgcHJpdmF0ZSBwYXJlbnQ6IFZpZXdCdWlsZGVyfG51bGwsIHByaXZhdGUgY29tcG9uZW50OiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsXG4gICAgICBwcml2YXRlIGVtYmVkZGVkVmlld0luZGV4OiBudW1iZXIsIHByaXZhdGUgdXNlZFBpcGVzOiBDb21waWxlUGlwZVN1bW1hcnlbXSxcbiAgICAgIHByaXZhdGUgc3RhdGljUXVlcnlJZHM6IE1hcDxUZW1wbGF0ZUFzdCwgU3RhdGljQW5kRHluYW1pY1F1ZXJ5SWRzPixcbiAgICAgIHByaXZhdGUgdmlld0J1aWxkZXJGYWN0b3J5OiBWaWV3QnVpbGRlckZhY3RvcnkpIHtcbiAgICAvLyBUT0RPKHRib3NjaCk6IFRoZSBvbGQgdmlldyBjb21waWxlciB1c2VkIHRvIHVzZSBhbiBgYW55YCB0eXBlXG4gICAgLy8gZm9yIHRoZSBjb250ZXh0IGluIGFueSBlbWJlZGRlZCB2aWV3LiBXZSBrZWVwIHRoaXMgYmVoYWl2b3IgZm9yIG5vd1xuICAgIC8vIHRvIGJlIGFibGUgdG8gaW50cm9kdWNlIHRoZSBuZXcgdmlldyBjb21waWxlciB3aXRob3V0IHRvbyBtYW55IGVycm9ycy5cbiAgICB0aGlzLmNvbXBUeXBlID0gdGhpcy5lbWJlZGRlZFZpZXdJbmRleCA+IDAgP1xuICAgICAgICBvLkRZTkFNSUNfVFlQRSA6XG4gICAgICAgIG8uZXhwcmVzc2lvblR5cGUob3V0cHV0Q3R4LmltcG9ydEV4cHIodGhpcy5jb21wb25lbnQudHlwZS5yZWZlcmVuY2UpKSAhO1xuICAgIHRoaXMudmlld05hbWUgPSB2aWV3Q2xhc3NOYW1lKHRoaXMuY29tcG9uZW50LnR5cGUucmVmZXJlbmNlLCB0aGlzLmVtYmVkZGVkVmlld0luZGV4KTtcbiAgfVxuXG4gIHZpc2l0QWxsKHZhcmlhYmxlczogVmFyaWFibGVBc3RbXSwgYXN0Tm9kZXM6IFRlbXBsYXRlQXN0W10pIHtcbiAgICB0aGlzLnZhcmlhYmxlcyA9IHZhcmlhYmxlcztcbiAgICAvLyBjcmVhdGUgdGhlIHBpcGVzIGZvciB0aGUgcHVyZSBwaXBlcyBpbW1lZGlhdGVseSwgc28gdGhhdCB3ZSBrbm93IHRoZWlyIGluZGljZXMuXG4gICAgaWYgKCF0aGlzLnBhcmVudCkge1xuICAgICAgdGhpcy51c2VkUGlwZXMuZm9yRWFjaCgocGlwZSkgPT4ge1xuICAgICAgICBpZiAocGlwZS5wdXJlKSB7XG4gICAgICAgICAgdGhpcy5wdXJlUGlwZU5vZGVJbmRpY2VzW3BpcGUubmFtZV0gPSB0aGlzLl9jcmVhdGVQaXBlKG51bGwsIHBpcGUpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICBpZiAoIXRoaXMucGFyZW50KSB7XG4gICAgICBjb25zdCBxdWVyeUlkcyA9IHN0YXRpY1ZpZXdRdWVyeUlkcyh0aGlzLnN0YXRpY1F1ZXJ5SWRzKTtcbiAgICAgIHRoaXMuY29tcG9uZW50LnZpZXdRdWVyaWVzLmZvckVhY2goKHF1ZXJ5LCBxdWVyeUluZGV4KSA9PiB7XG4gICAgICAgIC8vIE5vdGU6IHF1ZXJpZXMgc3RhcnQgd2l0aCBpZCAxIHNvIHdlIGNhbiB1c2UgdGhlIG51bWJlciBpbiBhIEJsb29tIGZpbHRlciFcbiAgICAgICAgY29uc3QgcXVlcnlJZCA9IHF1ZXJ5SW5kZXggKyAxO1xuICAgICAgICBjb25zdCBiaW5kaW5nVHlwZSA9IHF1ZXJ5LmZpcnN0ID8gUXVlcnlCaW5kaW5nVHlwZS5GaXJzdCA6IFF1ZXJ5QmluZGluZ1R5cGUuQWxsO1xuICAgICAgICBjb25zdCBmbGFncyA9XG4gICAgICAgICAgICBOb2RlRmxhZ3MuVHlwZVZpZXdRdWVyeSB8IGNhbGNTdGF0aWNEeW5hbWljUXVlcnlGbGFncyhxdWVyeUlkcywgcXVlcnlJZCwgcXVlcnkpO1xuICAgICAgICB0aGlzLm5vZGVzLnB1c2goKCkgPT4gKHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgc291cmNlU3BhbjogbnVsbCxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgbm9kZUZsYWdzOiBmbGFncyxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgbm9kZURlZjogby5pbXBvcnRFeHByKElkZW50aWZpZXJzLnF1ZXJ5RGVmKS5jYWxsRm4oW1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIG8ubGl0ZXJhbChmbGFncyksIG8ubGl0ZXJhbChxdWVyeUlkKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBuZXcgby5MaXRlcmFsTWFwRXhwcihbbmV3IG8uTGl0ZXJhbE1hcEVudHJ5KFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBxdWVyeS5wcm9wZXJ0eU5hbWUsIG8ubGl0ZXJhbChiaW5kaW5nVHlwZSksIGZhbHNlKV0pXG4gICAgICAgICAgICAgICAgICAgICAgICAgIF0pXG4gICAgICAgICAgICAgICAgICAgICAgICB9KSk7XG4gICAgICB9KTtcbiAgICB9XG4gICAgdGVtcGxhdGVWaXNpdEFsbCh0aGlzLCBhc3ROb2Rlcyk7XG4gICAgaWYgKHRoaXMucGFyZW50ICYmIChhc3ROb2Rlcy5sZW5ndGggPT09IDAgfHwgbmVlZHNBZGRpdGlvbmFsUm9vdE5vZGUoYXN0Tm9kZXMpKSkge1xuICAgICAgLy8gaWYgdGhlIHZpZXcgaXMgYW4gZW1iZWRkZWQgdmlldywgdGhlbiB3ZSBuZWVkIHRvIGFkZCBhbiBhZGRpdGlvbmFsIHJvb3Qgbm9kZSBpbiBzb21lIGNhc2VzXG4gICAgICB0aGlzLm5vZGVzLnB1c2goKCkgPT4gKHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHNvdXJjZVNwYW46IG51bGwsXG4gICAgICAgICAgICAgICAgICAgICAgICBub2RlRmxhZ3M6IE5vZGVGbGFncy5UeXBlRWxlbWVudCxcbiAgICAgICAgICAgICAgICAgICAgICAgIG5vZGVEZWY6IG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5hbmNob3JEZWYpLmNhbGxGbihbXG4gICAgICAgICAgICAgICAgICAgICAgICAgIG8ubGl0ZXJhbChOb2RlRmxhZ3MuTm9uZSksIG8uTlVMTF9FWFBSLCBvLk5VTExfRVhQUiwgby5saXRlcmFsKDApXG4gICAgICAgICAgICAgICAgICAgICAgICBdKVxuICAgICAgICAgICAgICAgICAgICAgIH0pKTtcbiAgICB9XG4gIH1cblxuICBidWlsZCh0YXJnZXRTdGF0ZW1lbnRzOiBvLlN0YXRlbWVudFtdID0gW10pOiBvLlN0YXRlbWVudFtdIHtcbiAgICB0aGlzLmNoaWxkcmVuLmZvckVhY2goKGNoaWxkKSA9PiBjaGlsZC5idWlsZCh0YXJnZXRTdGF0ZW1lbnRzKSk7XG5cbiAgICBjb25zdCB7dXBkYXRlUmVuZGVyZXJTdG10cywgdXBkYXRlRGlyZWN0aXZlc1N0bXRzLCBub2RlRGVmRXhwcnN9ID1cbiAgICAgICAgdGhpcy5fY3JlYXRlTm9kZUV4cHJlc3Npb25zKCk7XG5cbiAgICBjb25zdCB1cGRhdGVSZW5kZXJlckZuID0gdGhpcy5fY3JlYXRlVXBkYXRlRm4odXBkYXRlUmVuZGVyZXJTdG10cyk7XG4gICAgY29uc3QgdXBkYXRlRGlyZWN0aXZlc0ZuID0gdGhpcy5fY3JlYXRlVXBkYXRlRm4odXBkYXRlRGlyZWN0aXZlc1N0bXRzKTtcblxuXG4gICAgbGV0IHZpZXdGbGFncyA9IFZpZXdGbGFncy5Ob25lO1xuICAgIGlmICghdGhpcy5wYXJlbnQgJiYgdGhpcy5jb21wb25lbnQuY2hhbmdlRGV0ZWN0aW9uID09PSBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gpIHtcbiAgICAgIHZpZXdGbGFncyB8PSBWaWV3RmxhZ3MuT25QdXNoO1xuICAgIH1cbiAgICBjb25zdCB2aWV3RmFjdG9yeSA9IG5ldyBvLkRlY2xhcmVGdW5jdGlvblN0bXQoXG4gICAgICAgIHRoaXMudmlld05hbWUsIFtuZXcgby5GblBhcmFtKExPR19WQVIubmFtZSAhKV0sXG4gICAgICAgIFtuZXcgby5SZXR1cm5TdGF0ZW1lbnQoby5pbXBvcnRFeHByKElkZW50aWZpZXJzLnZpZXdEZWYpLmNhbGxGbihbXG4gICAgICAgICAgby5saXRlcmFsKHZpZXdGbGFncyksXG4gICAgICAgICAgby5saXRlcmFsQXJyKG5vZGVEZWZFeHBycyksXG4gICAgICAgICAgdXBkYXRlRGlyZWN0aXZlc0ZuLFxuICAgICAgICAgIHVwZGF0ZVJlbmRlcmVyRm4sXG4gICAgICAgIF0pKV0sXG4gICAgICAgIG8uaW1wb3J0VHlwZShJZGVudGlmaWVycy5WaWV3RGVmaW5pdGlvbiksXG4gICAgICAgIHRoaXMuZW1iZWRkZWRWaWV3SW5kZXggPT09IDAgPyBbby5TdG10TW9kaWZpZXIuRXhwb3J0ZWRdIDogW10pO1xuXG4gICAgdGFyZ2V0U3RhdGVtZW50cy5wdXNoKHZpZXdGYWN0b3J5KTtcbiAgICByZXR1cm4gdGFyZ2V0U3RhdGVtZW50cztcbiAgfVxuXG4gIHByaXZhdGUgX2NyZWF0ZVVwZGF0ZUZuKHVwZGF0ZVN0bXRzOiBvLlN0YXRlbWVudFtdKTogby5FeHByZXNzaW9uIHtcbiAgICBsZXQgdXBkYXRlRm46IG8uRXhwcmVzc2lvbjtcbiAgICBpZiAodXBkYXRlU3RtdHMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgcHJlU3RtdHM6IG8uU3RhdGVtZW50W10gPSBbXTtcbiAgICAgIGlmICghdGhpcy5jb21wb25lbnQuaXNIb3N0ICYmIG8uZmluZFJlYWRWYXJOYW1lcyh1cGRhdGVTdG10cykuaGFzKENPTVBfVkFSLm5hbWUgISkpIHtcbiAgICAgICAgcHJlU3RtdHMucHVzaChDT01QX1ZBUi5zZXQoVklFV19WQVIucHJvcCgnY29tcG9uZW50JykpLnRvRGVjbFN0bXQodGhpcy5jb21wVHlwZSkpO1xuICAgICAgfVxuICAgICAgdXBkYXRlRm4gPSBvLmZuKFxuICAgICAgICAgIFtcbiAgICAgICAgICAgIG5ldyBvLkZuUGFyYW0oQ0hFQ0tfVkFSLm5hbWUgISwgby5JTkZFUlJFRF9UWVBFKSxcbiAgICAgICAgICAgIG5ldyBvLkZuUGFyYW0oVklFV19WQVIubmFtZSAhLCBvLklORkVSUkVEX1RZUEUpXG4gICAgICAgICAgXSxcbiAgICAgICAgICBbLi4ucHJlU3RtdHMsIC4uLnVwZGF0ZVN0bXRzXSwgby5JTkZFUlJFRF9UWVBFKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdXBkYXRlRm4gPSBvLk5VTExfRVhQUjtcbiAgICB9XG4gICAgcmV0dXJuIHVwZGF0ZUZuO1xuICB9XG5cbiAgdmlzaXROZ0NvbnRlbnQoYXN0OiBOZ0NvbnRlbnRBc3QsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgLy8gbmdDb250ZW50RGVmKG5nQ29udGVudEluZGV4OiBudW1iZXIsIGluZGV4OiBudW1iZXIpOiBOb2RlRGVmO1xuICAgIHRoaXMubm9kZXMucHVzaCgoKSA9PiAoe1xuICAgICAgICAgICAgICAgICAgICAgIHNvdXJjZVNwYW46IGFzdC5zb3VyY2VTcGFuLFxuICAgICAgICAgICAgICAgICAgICAgIG5vZGVGbGFnczogTm9kZUZsYWdzLlR5cGVOZ0NvbnRlbnQsXG4gICAgICAgICAgICAgICAgICAgICAgbm9kZURlZjogby5pbXBvcnRFeHByKElkZW50aWZpZXJzLm5nQ29udGVudERlZikuY2FsbEZuKFtcbiAgICAgICAgICAgICAgICAgICAgICAgIG8ubGl0ZXJhbChhc3QubmdDb250ZW50SW5kZXgpLCBvLmxpdGVyYWwoYXN0LmluZGV4KVxuICAgICAgICAgICAgICAgICAgICAgIF0pXG4gICAgICAgICAgICAgICAgICAgIH0pKTtcbiAgfVxuXG4gIHZpc2l0VGV4dChhc3Q6IFRleHRBc3QsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgLy8gU3RhdGljIHRleHQgbm9kZXMgaGF2ZSBubyBjaGVjayBmdW5jdGlvblxuICAgIGNvbnN0IGNoZWNrSW5kZXggPSAtMTtcbiAgICB0aGlzLm5vZGVzLnB1c2goKCkgPT4gKHtcbiAgICAgICAgICAgICAgICAgICAgICBzb3VyY2VTcGFuOiBhc3Quc291cmNlU3BhbixcbiAgICAgICAgICAgICAgICAgICAgICBub2RlRmxhZ3M6IE5vZGVGbGFncy5UeXBlVGV4dCxcbiAgICAgICAgICAgICAgICAgICAgICBub2RlRGVmOiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMudGV4dERlZikuY2FsbEZuKFtcbiAgICAgICAgICAgICAgICAgICAgICAgIG8ubGl0ZXJhbChjaGVja0luZGV4KSxcbiAgICAgICAgICAgICAgICAgICAgICAgIG8ubGl0ZXJhbChhc3QubmdDb250ZW50SW5kZXgpLFxuICAgICAgICAgICAgICAgICAgICAgICAgby5saXRlcmFsQXJyKFtvLmxpdGVyYWwoYXN0LnZhbHVlKV0pLFxuICAgICAgICAgICAgICAgICAgICAgIF0pXG4gICAgICAgICAgICAgICAgICAgIH0pKTtcbiAgfVxuXG4gIHZpc2l0Qm91bmRUZXh0KGFzdDogQm91bmRUZXh0QXN0LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIGNvbnN0IG5vZGVJbmRleCA9IHRoaXMubm9kZXMubGVuZ3RoO1xuICAgIC8vIHJlc2VydmUgdGhlIHNwYWNlIGluIHRoZSBub2RlRGVmcyBhcnJheVxuICAgIHRoaXMubm9kZXMucHVzaChudWxsICEpO1xuXG4gICAgY29uc3QgYXN0V2l0aFNvdXJjZSA9IDxBU1RXaXRoU291cmNlPmFzdC52YWx1ZTtcbiAgICBjb25zdCBpbnRlciA9IDxJbnRlcnBvbGF0aW9uPmFzdFdpdGhTb3VyY2UuYXN0O1xuXG4gICAgY29uc3QgdXBkYXRlUmVuZGVyZXJFeHByZXNzaW9ucyA9IGludGVyLmV4cHJlc3Npb25zLm1hcChcbiAgICAgICAgKGV4cHIsIGJpbmRpbmdJbmRleCkgPT4gdGhpcy5fcHJlcHJvY2Vzc1VwZGF0ZUV4cHJlc3Npb24oXG4gICAgICAgICAgICB7bm9kZUluZGV4LCBiaW5kaW5nSW5kZXgsIHNvdXJjZVNwYW46IGFzdC5zb3VyY2VTcGFuLCBjb250ZXh0OiBDT01QX1ZBUiwgdmFsdWU6IGV4cHJ9KSk7XG5cbiAgICAvLyBDaGVjayBpbmRleCBpcyB0aGUgc2FtZSBhcyB0aGUgbm9kZSBpbmRleCBkdXJpbmcgY29tcGlsYXRpb25cbiAgICAvLyBUaGV5IG1pZ2h0IG9ubHkgZGlmZmVyIGF0IHJ1bnRpbWVcbiAgICBjb25zdCBjaGVja0luZGV4ID0gbm9kZUluZGV4O1xuXG4gICAgdGhpcy5ub2Rlc1tub2RlSW5kZXhdID0gKCkgPT4gKHtcbiAgICAgIHNvdXJjZVNwYW46IGFzdC5zb3VyY2VTcGFuLFxuICAgICAgbm9kZUZsYWdzOiBOb2RlRmxhZ3MuVHlwZVRleHQsXG4gICAgICBub2RlRGVmOiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMudGV4dERlZikuY2FsbEZuKFtcbiAgICAgICAgby5saXRlcmFsKGNoZWNrSW5kZXgpLFxuICAgICAgICBvLmxpdGVyYWwoYXN0Lm5nQ29udGVudEluZGV4KSxcbiAgICAgICAgby5saXRlcmFsQXJyKGludGVyLnN0cmluZ3MubWFwKHMgPT4gby5saXRlcmFsKHMpKSksXG4gICAgICBdKSxcbiAgICAgIHVwZGF0ZVJlbmRlcmVyOiB1cGRhdGVSZW5kZXJlckV4cHJlc3Npb25zXG4gICAgfSk7XG4gIH1cblxuICB2aXNpdEVtYmVkZGVkVGVtcGxhdGUoYXN0OiBFbWJlZGRlZFRlbXBsYXRlQXN0LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIGNvbnN0IG5vZGVJbmRleCA9IHRoaXMubm9kZXMubGVuZ3RoO1xuICAgIC8vIHJlc2VydmUgdGhlIHNwYWNlIGluIHRoZSBub2RlRGVmcyBhcnJheVxuICAgIHRoaXMubm9kZXMucHVzaChudWxsICEpO1xuXG4gICAgY29uc3Qge2ZsYWdzLCBxdWVyeU1hdGNoZXNFeHByLCBob3N0RXZlbnRzfSA9IHRoaXMuX3Zpc2l0RWxlbWVudE9yVGVtcGxhdGUobm9kZUluZGV4LCBhc3QpO1xuXG4gICAgY29uc3QgY2hpbGRWaXNpdG9yID0gdGhpcy52aWV3QnVpbGRlckZhY3RvcnkodGhpcyk7XG4gICAgdGhpcy5jaGlsZHJlbi5wdXNoKGNoaWxkVmlzaXRvcik7XG4gICAgY2hpbGRWaXNpdG9yLnZpc2l0QWxsKGFzdC52YXJpYWJsZXMsIGFzdC5jaGlsZHJlbik7XG5cbiAgICBjb25zdCBjaGlsZENvdW50ID0gdGhpcy5ub2Rlcy5sZW5ndGggLSBub2RlSW5kZXggLSAxO1xuXG4gICAgLy8gYW5jaG9yRGVmKFxuICAgIC8vICAgZmxhZ3M6IE5vZGVGbGFncywgbWF0Y2hlZFF1ZXJpZXM6IFtzdHJpbmcsIFF1ZXJ5VmFsdWVUeXBlXVtdLCBuZ0NvbnRlbnRJbmRleDogbnVtYmVyLFxuICAgIC8vICAgY2hpbGRDb3VudDogbnVtYmVyLCBoYW5kbGVFdmVudEZuPzogRWxlbWVudEhhbmRsZUV2ZW50Rm4sIHRlbXBsYXRlRmFjdG9yeT86XG4gICAgLy8gICBWaWV3RGVmaW5pdGlvbkZhY3RvcnkpOiBOb2RlRGVmO1xuICAgIHRoaXMubm9kZXNbbm9kZUluZGV4XSA9ICgpID0+ICh7XG4gICAgICBzb3VyY2VTcGFuOiBhc3Quc291cmNlU3BhbixcbiAgICAgIG5vZGVGbGFnczogTm9kZUZsYWdzLlR5cGVFbGVtZW50IHwgZmxhZ3MsXG4gICAgICBub2RlRGVmOiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMuYW5jaG9yRGVmKS5jYWxsRm4oW1xuICAgICAgICBvLmxpdGVyYWwoZmxhZ3MpLFxuICAgICAgICBxdWVyeU1hdGNoZXNFeHByLFxuICAgICAgICBvLmxpdGVyYWwoYXN0Lm5nQ29udGVudEluZGV4KSxcbiAgICAgICAgby5saXRlcmFsKGNoaWxkQ291bnQpLFxuICAgICAgICB0aGlzLl9jcmVhdGVFbGVtZW50SGFuZGxlRXZlbnRGbihub2RlSW5kZXgsIGhvc3RFdmVudHMpLFxuICAgICAgICBvLnZhcmlhYmxlKGNoaWxkVmlzaXRvci52aWV3TmFtZSksXG4gICAgICBdKVxuICAgIH0pO1xuICB9XG5cbiAgdmlzaXRFbGVtZW50KGFzdDogRWxlbWVudEFzdCwgY29udGV4dDogYW55KTogYW55IHtcbiAgICBjb25zdCBub2RlSW5kZXggPSB0aGlzLm5vZGVzLmxlbmd0aDtcbiAgICAvLyByZXNlcnZlIHRoZSBzcGFjZSBpbiB0aGUgbm9kZURlZnMgYXJyYXkgc28gd2UgY2FuIGFkZCBjaGlsZHJlblxuICAgIHRoaXMubm9kZXMucHVzaChudWxsICEpO1xuXG4gICAgLy8gVXNpbmcgYSBudWxsIGVsZW1lbnQgbmFtZSBjcmVhdGVzIGFuIGFuY2hvci5cbiAgICBjb25zdCBlbE5hbWU6IHN0cmluZ3xudWxsID0gaXNOZ0NvbnRhaW5lcihhc3QubmFtZSkgPyBudWxsIDogYXN0Lm5hbWU7XG5cbiAgICBjb25zdCB7ZmxhZ3MsIHVzZWRFdmVudHMsIHF1ZXJ5TWF0Y2hlc0V4cHIsIGhvc3RCaW5kaW5nczogZGlySG9zdEJpbmRpbmdzLCBob3N0RXZlbnRzfSA9XG4gICAgICAgIHRoaXMuX3Zpc2l0RWxlbWVudE9yVGVtcGxhdGUobm9kZUluZGV4LCBhc3QpO1xuXG4gICAgbGV0IGlucHV0RGVmczogby5FeHByZXNzaW9uW10gPSBbXTtcbiAgICBsZXQgdXBkYXRlUmVuZGVyZXJFeHByZXNzaW9uczogVXBkYXRlRXhwcmVzc2lvbltdID0gW107XG4gICAgbGV0IG91dHB1dERlZnM6IG8uRXhwcmVzc2lvbltdID0gW107XG4gICAgaWYgKGVsTmFtZSkge1xuICAgICAgY29uc3QgaG9zdEJpbmRpbmdzOiBhbnlbXSA9IGFzdC5pbnB1dHNcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLm1hcCgoaW5wdXRBc3QpID0+ICh7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjb250ZXh0OiBDT01QX1ZBUiBhcyBvLkV4cHJlc3Npb24sXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBpbnB1dEFzdCxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGRpckFzdDogbnVsbCBhcyBhbnksXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfSkpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC5jb25jYXQoZGlySG9zdEJpbmRpbmdzKTtcbiAgICAgIGlmIChob3N0QmluZGluZ3MubGVuZ3RoKSB7XG4gICAgICAgIHVwZGF0ZVJlbmRlcmVyRXhwcmVzc2lvbnMgPVxuICAgICAgICAgICAgaG9zdEJpbmRpbmdzLm1hcCgoaG9zdEJpbmRpbmcsIGJpbmRpbmdJbmRleCkgPT4gdGhpcy5fcHJlcHJvY2Vzc1VwZGF0ZUV4cHJlc3Npb24oe1xuICAgICAgICAgICAgICBjb250ZXh0OiBob3N0QmluZGluZy5jb250ZXh0LFxuICAgICAgICAgICAgICBub2RlSW5kZXgsXG4gICAgICAgICAgICAgIGJpbmRpbmdJbmRleCxcbiAgICAgICAgICAgICAgc291cmNlU3BhbjogaG9zdEJpbmRpbmcuaW5wdXRBc3Quc291cmNlU3BhbixcbiAgICAgICAgICAgICAgdmFsdWU6IGhvc3RCaW5kaW5nLmlucHV0QXN0LnZhbHVlXG4gICAgICAgICAgICB9KSk7XG4gICAgICAgIGlucHV0RGVmcyA9IGhvc3RCaW5kaW5ncy5tYXAoXG4gICAgICAgICAgICBob3N0QmluZGluZyA9PiBlbGVtZW50QmluZGluZ0RlZihob3N0QmluZGluZy5pbnB1dEFzdCwgaG9zdEJpbmRpbmcuZGlyQXN0KSk7XG4gICAgICB9XG4gICAgICBvdXRwdXREZWZzID0gdXNlZEV2ZW50cy5tYXAoXG4gICAgICAgICAgKFt0YXJnZXQsIGV2ZW50TmFtZV0pID0+IG8ubGl0ZXJhbEFycihbby5saXRlcmFsKHRhcmdldCksIG8ubGl0ZXJhbChldmVudE5hbWUpXSkpO1xuICAgIH1cblxuICAgIHRlbXBsYXRlVmlzaXRBbGwodGhpcywgYXN0LmNoaWxkcmVuKTtcblxuICAgIGNvbnN0IGNoaWxkQ291bnQgPSB0aGlzLm5vZGVzLmxlbmd0aCAtIG5vZGVJbmRleCAtIDE7XG5cbiAgICBjb25zdCBjb21wQXN0ID0gYXN0LmRpcmVjdGl2ZXMuZmluZChkaXJBc3QgPT4gZGlyQXN0LmRpcmVjdGl2ZS5pc0NvbXBvbmVudCk7XG4gICAgbGV0IGNvbXBSZW5kZXJlclR5cGUgPSBvLk5VTExfRVhQUiBhcyBvLkV4cHJlc3Npb247XG4gICAgbGV0IGNvbXBWaWV3ID0gby5OVUxMX0VYUFIgYXMgby5FeHByZXNzaW9uO1xuICAgIGlmIChjb21wQXN0KSB7XG4gICAgICBjb21wVmlldyA9IHRoaXMub3V0cHV0Q3R4LmltcG9ydEV4cHIoY29tcEFzdC5kaXJlY3RpdmUuY29tcG9uZW50Vmlld1R5cGUpO1xuICAgICAgY29tcFJlbmRlcmVyVHlwZSA9IHRoaXMub3V0cHV0Q3R4LmltcG9ydEV4cHIoY29tcEFzdC5kaXJlY3RpdmUucmVuZGVyZXJUeXBlKTtcbiAgICB9XG5cbiAgICAvLyBDaGVjayBpbmRleCBpcyB0aGUgc2FtZSBhcyB0aGUgbm9kZSBpbmRleCBkdXJpbmcgY29tcGlsYXRpb25cbiAgICAvLyBUaGV5IG1pZ2h0IG9ubHkgZGlmZmVyIGF0IHJ1bnRpbWVcbiAgICBjb25zdCBjaGVja0luZGV4ID0gbm9kZUluZGV4O1xuXG4gICAgdGhpcy5ub2Rlc1tub2RlSW5kZXhdID0gKCkgPT4gKHtcbiAgICAgIHNvdXJjZVNwYW46IGFzdC5zb3VyY2VTcGFuLFxuICAgICAgbm9kZUZsYWdzOiBOb2RlRmxhZ3MuVHlwZUVsZW1lbnQgfCBmbGFncyxcbiAgICAgIG5vZGVEZWY6IG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5lbGVtZW50RGVmKS5jYWxsRm4oW1xuICAgICAgICBvLmxpdGVyYWwoY2hlY2tJbmRleCksXG4gICAgICAgIG8ubGl0ZXJhbChmbGFncyksXG4gICAgICAgIHF1ZXJ5TWF0Y2hlc0V4cHIsXG4gICAgICAgIG8ubGl0ZXJhbChhc3QubmdDb250ZW50SW5kZXgpLFxuICAgICAgICBvLmxpdGVyYWwoY2hpbGRDb3VudCksXG4gICAgICAgIG8ubGl0ZXJhbChlbE5hbWUpLFxuICAgICAgICBlbE5hbWUgPyBmaXhlZEF0dHJzRGVmKGFzdCkgOiBvLk5VTExfRVhQUixcbiAgICAgICAgaW5wdXREZWZzLmxlbmd0aCA/IG8ubGl0ZXJhbEFycihpbnB1dERlZnMpIDogby5OVUxMX0VYUFIsXG4gICAgICAgIG91dHB1dERlZnMubGVuZ3RoID8gby5saXRlcmFsQXJyKG91dHB1dERlZnMpIDogby5OVUxMX0VYUFIsXG4gICAgICAgIHRoaXMuX2NyZWF0ZUVsZW1lbnRIYW5kbGVFdmVudEZuKG5vZGVJbmRleCwgaG9zdEV2ZW50cyksXG4gICAgICAgIGNvbXBWaWV3LFxuICAgICAgICBjb21wUmVuZGVyZXJUeXBlLFxuICAgICAgXSksXG4gICAgICB1cGRhdGVSZW5kZXJlcjogdXBkYXRlUmVuZGVyZXJFeHByZXNzaW9uc1xuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfdmlzaXRFbGVtZW50T3JUZW1wbGF0ZShub2RlSW5kZXg6IG51bWJlciwgYXN0OiB7XG4gICAgaGFzVmlld0NvbnRhaW5lcjogYm9vbGVhbixcbiAgICBvdXRwdXRzOiBCb3VuZEV2ZW50QXN0W10sXG4gICAgZGlyZWN0aXZlczogRGlyZWN0aXZlQXN0W10sXG4gICAgcHJvdmlkZXJzOiBQcm92aWRlckFzdFtdLFxuICAgIHJlZmVyZW5jZXM6IFJlZmVyZW5jZUFzdFtdLFxuICAgIHF1ZXJ5TWF0Y2hlczogUXVlcnlNYXRjaFtdXG4gIH0pOiB7XG4gICAgZmxhZ3M6IE5vZGVGbGFncyxcbiAgICB1c2VkRXZlbnRzOiBbc3RyaW5nIHwgbnVsbCwgc3RyaW5nXVtdLFxuICAgIHF1ZXJ5TWF0Y2hlc0V4cHI6IG8uRXhwcmVzc2lvbixcbiAgICBob3N0QmluZGluZ3M6XG4gICAgICAgIHtjb250ZXh0OiBvLkV4cHJlc3Npb24sIGlucHV0QXN0OiBCb3VuZEVsZW1lbnRQcm9wZXJ0eUFzdCwgZGlyQXN0OiBEaXJlY3RpdmVBc3R9W10sXG4gICAgaG9zdEV2ZW50czoge2NvbnRleHQ6IG8uRXhwcmVzc2lvbiwgZXZlbnRBc3Q6IEJvdW5kRXZlbnRBc3QsIGRpckFzdDogRGlyZWN0aXZlQXN0fVtdLFxuICB9IHtcbiAgICBsZXQgZmxhZ3MgPSBOb2RlRmxhZ3MuTm9uZTtcbiAgICBpZiAoYXN0Lmhhc1ZpZXdDb250YWluZXIpIHtcbiAgICAgIGZsYWdzIHw9IE5vZGVGbGFncy5FbWJlZGRlZFZpZXdzO1xuICAgIH1cbiAgICBjb25zdCB1c2VkRXZlbnRzID0gbmV3IE1hcDxzdHJpbmcsIFtzdHJpbmcgfCBudWxsLCBzdHJpbmddPigpO1xuICAgIGFzdC5vdXRwdXRzLmZvckVhY2goKGV2ZW50KSA9PiB7XG4gICAgICBjb25zdCB7bmFtZSwgdGFyZ2V0fSA9IGVsZW1lbnRFdmVudE5hbWVBbmRUYXJnZXQoZXZlbnQsIG51bGwpO1xuICAgICAgdXNlZEV2ZW50cy5zZXQoZWxlbWVudEV2ZW50RnVsbE5hbWUodGFyZ2V0LCBuYW1lKSwgW3RhcmdldCwgbmFtZV0pO1xuICAgIH0pO1xuICAgIGFzdC5kaXJlY3RpdmVzLmZvckVhY2goKGRpckFzdCkgPT4ge1xuICAgICAgZGlyQXN0Lmhvc3RFdmVudHMuZm9yRWFjaCgoZXZlbnQpID0+IHtcbiAgICAgICAgY29uc3Qge25hbWUsIHRhcmdldH0gPSBlbGVtZW50RXZlbnROYW1lQW5kVGFyZ2V0KGV2ZW50LCBkaXJBc3QpO1xuICAgICAgICB1c2VkRXZlbnRzLnNldChlbGVtZW50RXZlbnRGdWxsTmFtZSh0YXJnZXQsIG5hbWUpLCBbdGFyZ2V0LCBuYW1lXSk7XG4gICAgICB9KTtcbiAgICB9KTtcbiAgICBjb25zdCBob3N0QmluZGluZ3M6XG4gICAgICAgIHtjb250ZXh0OiBvLkV4cHJlc3Npb24sIGlucHV0QXN0OiBCb3VuZEVsZW1lbnRQcm9wZXJ0eUFzdCwgZGlyQXN0OiBEaXJlY3RpdmVBc3R9W10gPSBbXTtcbiAgICBjb25zdCBob3N0RXZlbnRzOiB7Y29udGV4dDogby5FeHByZXNzaW9uLCBldmVudEFzdDogQm91bmRFdmVudEFzdCwgZGlyQXN0OiBEaXJlY3RpdmVBc3R9W10gPSBbXTtcbiAgICB0aGlzLl92aXNpdENvbXBvbmVudEZhY3RvcnlSZXNvbHZlclByb3ZpZGVyKGFzdC5kaXJlY3RpdmVzKTtcblxuICAgIGFzdC5wcm92aWRlcnMuZm9yRWFjaCgocHJvdmlkZXJBc3QsIHByb3ZpZGVySW5kZXgpID0+IHtcbiAgICAgIGxldCBkaXJBc3Q6IERpcmVjdGl2ZUFzdCA9IHVuZGVmaW5lZCAhO1xuICAgICAgbGV0IGRpckluZGV4OiBudW1iZXIgPSB1bmRlZmluZWQgITtcbiAgICAgIGFzdC5kaXJlY3RpdmVzLmZvckVhY2goKGxvY2FsRGlyQXN0LCBpKSA9PiB7XG4gICAgICAgIGlmIChsb2NhbERpckFzdC5kaXJlY3RpdmUudHlwZS5yZWZlcmVuY2UgPT09IHRva2VuUmVmZXJlbmNlKHByb3ZpZGVyQXN0LnRva2VuKSkge1xuICAgICAgICAgIGRpckFzdCA9IGxvY2FsRGlyQXN0O1xuICAgICAgICAgIGRpckluZGV4ID0gaTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgICBpZiAoZGlyQXN0KSB7XG4gICAgICAgIGNvbnN0IHtob3N0QmluZGluZ3M6IGRpckhvc3RCaW5kaW5ncywgaG9zdEV2ZW50czogZGlySG9zdEV2ZW50c30gPSB0aGlzLl92aXNpdERpcmVjdGl2ZShcbiAgICAgICAgICAgIHByb3ZpZGVyQXN0LCBkaXJBc3QsIGRpckluZGV4LCBub2RlSW5kZXgsIGFzdC5yZWZlcmVuY2VzLCBhc3QucXVlcnlNYXRjaGVzLCB1c2VkRXZlbnRzLFxuICAgICAgICAgICAgdGhpcy5zdGF0aWNRdWVyeUlkcy5nZXQoPGFueT5hc3QpICEpO1xuICAgICAgICBob3N0QmluZGluZ3MucHVzaCguLi5kaXJIb3N0QmluZGluZ3MpO1xuICAgICAgICBob3N0RXZlbnRzLnB1c2goLi4uZGlySG9zdEV2ZW50cyk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLl92aXNpdFByb3ZpZGVyKHByb3ZpZGVyQXN0LCBhc3QucXVlcnlNYXRjaGVzKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGxldCBxdWVyeU1hdGNoRXhwcnM6IG8uRXhwcmVzc2lvbltdID0gW107XG4gICAgYXN0LnF1ZXJ5TWF0Y2hlcy5mb3JFYWNoKChtYXRjaCkgPT4ge1xuICAgICAgbGV0IHZhbHVlVHlwZTogUXVlcnlWYWx1ZVR5cGUgPSB1bmRlZmluZWQgITtcbiAgICAgIGlmICh0b2tlblJlZmVyZW5jZShtYXRjaC52YWx1ZSkgPT09XG4gICAgICAgICAgdGhpcy5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKElkZW50aWZpZXJzLkVsZW1lbnRSZWYpKSB7XG4gICAgICAgIHZhbHVlVHlwZSA9IFF1ZXJ5VmFsdWVUeXBlLkVsZW1lbnRSZWY7XG4gICAgICB9IGVsc2UgaWYgKFxuICAgICAgICAgIHRva2VuUmVmZXJlbmNlKG1hdGNoLnZhbHVlKSA9PT1cbiAgICAgICAgICB0aGlzLnJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuVmlld0NvbnRhaW5lclJlZikpIHtcbiAgICAgICAgdmFsdWVUeXBlID0gUXVlcnlWYWx1ZVR5cGUuVmlld0NvbnRhaW5lclJlZjtcbiAgICAgIH0gZWxzZSBpZiAoXG4gICAgICAgICAgdG9rZW5SZWZlcmVuY2UobWF0Y2gudmFsdWUpID09PVxuICAgICAgICAgIHRoaXMucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5UZW1wbGF0ZVJlZikpIHtcbiAgICAgICAgdmFsdWVUeXBlID0gUXVlcnlWYWx1ZVR5cGUuVGVtcGxhdGVSZWY7XG4gICAgICB9XG4gICAgICBpZiAodmFsdWVUeXBlICE9IG51bGwpIHtcbiAgICAgICAgcXVlcnlNYXRjaEV4cHJzLnB1c2goby5saXRlcmFsQXJyKFtvLmxpdGVyYWwobWF0Y2gucXVlcnlJZCksIG8ubGl0ZXJhbCh2YWx1ZVR5cGUpXSkpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIGFzdC5yZWZlcmVuY2VzLmZvckVhY2goKHJlZikgPT4ge1xuICAgICAgbGV0IHZhbHVlVHlwZTogUXVlcnlWYWx1ZVR5cGUgPSB1bmRlZmluZWQgITtcbiAgICAgIGlmICghcmVmLnZhbHVlKSB7XG4gICAgICAgIHZhbHVlVHlwZSA9IFF1ZXJ5VmFsdWVUeXBlLlJlbmRlckVsZW1lbnQ7XG4gICAgICB9IGVsc2UgaWYgKFxuICAgICAgICAgIHRva2VuUmVmZXJlbmNlKHJlZi52YWx1ZSkgPT09XG4gICAgICAgICAgdGhpcy5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKElkZW50aWZpZXJzLlRlbXBsYXRlUmVmKSkge1xuICAgICAgICB2YWx1ZVR5cGUgPSBRdWVyeVZhbHVlVHlwZS5UZW1wbGF0ZVJlZjtcbiAgICAgIH1cbiAgICAgIGlmICh2YWx1ZVR5cGUgIT0gbnVsbCkge1xuICAgICAgICB0aGlzLnJlZk5vZGVJbmRpY2VzW3JlZi5uYW1lXSA9IG5vZGVJbmRleDtcbiAgICAgICAgcXVlcnlNYXRjaEV4cHJzLnB1c2goby5saXRlcmFsQXJyKFtvLmxpdGVyYWwocmVmLm5hbWUpLCBvLmxpdGVyYWwodmFsdWVUeXBlKV0pKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICBhc3Qub3V0cHV0cy5mb3JFYWNoKChvdXRwdXRBc3QpID0+IHtcbiAgICAgIGhvc3RFdmVudHMucHVzaCh7Y29udGV4dDogQ09NUF9WQVIsIGV2ZW50QXN0OiBvdXRwdXRBc3QsIGRpckFzdDogbnVsbCAhfSk7XG4gICAgfSk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgZmxhZ3MsXG4gICAgICB1c2VkRXZlbnRzOiBBcnJheS5mcm9tKHVzZWRFdmVudHMudmFsdWVzKCkpLFxuICAgICAgcXVlcnlNYXRjaGVzRXhwcjogcXVlcnlNYXRjaEV4cHJzLmxlbmd0aCA/IG8ubGl0ZXJhbEFycihxdWVyeU1hdGNoRXhwcnMpIDogby5OVUxMX0VYUFIsXG4gICAgICBob3N0QmluZGluZ3MsXG4gICAgICBob3N0RXZlbnRzOiBob3N0RXZlbnRzXG4gICAgfTtcbiAgfVxuXG4gIHByaXZhdGUgX3Zpc2l0RGlyZWN0aXZlKFxuICAgICAgcHJvdmlkZXJBc3Q6IFByb3ZpZGVyQXN0LCBkaXJBc3Q6IERpcmVjdGl2ZUFzdCwgZGlyZWN0aXZlSW5kZXg6IG51bWJlcixcbiAgICAgIGVsZW1lbnROb2RlSW5kZXg6IG51bWJlciwgcmVmczogUmVmZXJlbmNlQXN0W10sIHF1ZXJ5TWF0Y2hlczogUXVlcnlNYXRjaFtdLFxuICAgICAgdXNlZEV2ZW50czogTWFwPHN0cmluZywgYW55PiwgcXVlcnlJZHM6IFN0YXRpY0FuZER5bmFtaWNRdWVyeUlkcyk6IHtcbiAgICBob3N0QmluZGluZ3M6XG4gICAgICAgIHtjb250ZXh0OiBvLkV4cHJlc3Npb24sIGlucHV0QXN0OiBCb3VuZEVsZW1lbnRQcm9wZXJ0eUFzdCwgZGlyQXN0OiBEaXJlY3RpdmVBc3R9W10sXG4gICAgaG9zdEV2ZW50czoge2NvbnRleHQ6IG8uRXhwcmVzc2lvbiwgZXZlbnRBc3Q6IEJvdW5kRXZlbnRBc3QsIGRpckFzdDogRGlyZWN0aXZlQXN0fVtdXG4gIH0ge1xuICAgIGNvbnN0IG5vZGVJbmRleCA9IHRoaXMubm9kZXMubGVuZ3RoO1xuICAgIC8vIHJlc2VydmUgdGhlIHNwYWNlIGluIHRoZSBub2RlRGVmcyBhcnJheSBzbyB3ZSBjYW4gYWRkIGNoaWxkcmVuXG4gICAgdGhpcy5ub2Rlcy5wdXNoKG51bGwgISk7XG5cbiAgICBkaXJBc3QuZGlyZWN0aXZlLnF1ZXJpZXMuZm9yRWFjaCgocXVlcnksIHF1ZXJ5SW5kZXgpID0+IHtcbiAgICAgIGNvbnN0IHF1ZXJ5SWQgPSBkaXJBc3QuY29udGVudFF1ZXJ5U3RhcnRJZCArIHF1ZXJ5SW5kZXg7XG4gICAgICBjb25zdCBmbGFncyA9XG4gICAgICAgICAgTm9kZUZsYWdzLlR5cGVDb250ZW50UXVlcnkgfCBjYWxjU3RhdGljRHluYW1pY1F1ZXJ5RmxhZ3MocXVlcnlJZHMsIHF1ZXJ5SWQsIHF1ZXJ5KTtcbiAgICAgIGNvbnN0IGJpbmRpbmdUeXBlID0gcXVlcnkuZmlyc3QgPyBRdWVyeUJpbmRpbmdUeXBlLkZpcnN0IDogUXVlcnlCaW5kaW5nVHlwZS5BbGw7XG4gICAgICB0aGlzLm5vZGVzLnB1c2goKCkgPT4gKHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHNvdXJjZVNwYW46IGRpckFzdC5zb3VyY2VTcGFuLFxuICAgICAgICAgICAgICAgICAgICAgICAgbm9kZUZsYWdzOiBmbGFncyxcbiAgICAgICAgICAgICAgICAgICAgICAgIG5vZGVEZWY6IG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5xdWVyeURlZikuY2FsbEZuKFtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgby5saXRlcmFsKGZsYWdzKSwgby5saXRlcmFsKHF1ZXJ5SWQpLFxuICAgICAgICAgICAgICAgICAgICAgICAgICBuZXcgby5MaXRlcmFsTWFwRXhwcihbbmV3IG8uTGl0ZXJhbE1hcEVudHJ5KFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcXVlcnkucHJvcGVydHlOYW1lLCBvLmxpdGVyYWwoYmluZGluZ1R5cGUpLCBmYWxzZSldKVxuICAgICAgICAgICAgICAgICAgICAgICAgXSksXG4gICAgICAgICAgICAgICAgICAgICAgfSkpO1xuICAgIH0pO1xuXG4gICAgLy8gTm90ZTogdGhlIG9wZXJhdGlvbiBiZWxvdyBtaWdodCBhbHNvIGNyZWF0ZSBuZXcgbm9kZURlZnMsXG4gICAgLy8gYnV0IHdlIGRvbid0IHdhbnQgdGhlbSB0byBiZSBhIGNoaWxkIG9mIGEgZGlyZWN0aXZlLFxuICAgIC8vIGFzIHRoZXkgbWlnaHQgYmUgYSBwcm92aWRlci9waXBlIG9uIHRoZWlyIG93bi5cbiAgICAvLyBJLmUuIHdlIG9ubHkgYWxsb3cgcXVlcmllcyBhcyBjaGlsZHJlbiBvZiBkaXJlY3RpdmVzIG5vZGVzLlxuICAgIGNvbnN0IGNoaWxkQ291bnQgPSB0aGlzLm5vZGVzLmxlbmd0aCAtIG5vZGVJbmRleCAtIDE7XG5cbiAgICBsZXQge2ZsYWdzLCBxdWVyeU1hdGNoRXhwcnMsIHByb3ZpZGVyRXhwciwgZGVwc0V4cHJ9ID1cbiAgICAgICAgdGhpcy5fdmlzaXRQcm92aWRlck9yRGlyZWN0aXZlKHByb3ZpZGVyQXN0LCBxdWVyeU1hdGNoZXMpO1xuXG4gICAgcmVmcy5mb3JFYWNoKChyZWYpID0+IHtcbiAgICAgIGlmIChyZWYudmFsdWUgJiYgdG9rZW5SZWZlcmVuY2UocmVmLnZhbHVlKSA9PT0gdG9rZW5SZWZlcmVuY2UocHJvdmlkZXJBc3QudG9rZW4pKSB7XG4gICAgICAgIHRoaXMucmVmTm9kZUluZGljZXNbcmVmLm5hbWVdID0gbm9kZUluZGV4O1xuICAgICAgICBxdWVyeU1hdGNoRXhwcnMucHVzaChcbiAgICAgICAgICAgIG8ubGl0ZXJhbEFycihbby5saXRlcmFsKHJlZi5uYW1lKSwgby5saXRlcmFsKFF1ZXJ5VmFsdWVUeXBlLlByb3ZpZGVyKV0pKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGlmIChkaXJBc3QuZGlyZWN0aXZlLmlzQ29tcG9uZW50KSB7XG4gICAgICBmbGFncyB8PSBOb2RlRmxhZ3MuQ29tcG9uZW50O1xuICAgIH1cblxuICAgIGNvbnN0IGlucHV0RGVmcyA9IGRpckFzdC5pbnB1dHMubWFwKChpbnB1dEFzdCwgaW5wdXRJbmRleCkgPT4ge1xuICAgICAgY29uc3QgbWFwVmFsdWUgPSBvLmxpdGVyYWxBcnIoW28ubGl0ZXJhbChpbnB1dEluZGV4KSwgby5saXRlcmFsKGlucHV0QXN0LmRpcmVjdGl2ZU5hbWUpXSk7XG4gICAgICAvLyBOb3RlOiBpdCdzIGltcG9ydGFudCB0byBub3QgcXVvdGUgdGhlIGtleSBzbyB0aGF0IHdlIGNhbiBjYXB0dXJlIHJlbmFtZXMgYnkgbWluaWZpZXJzIVxuICAgICAgcmV0dXJuIG5ldyBvLkxpdGVyYWxNYXBFbnRyeShpbnB1dEFzdC5kaXJlY3RpdmVOYW1lLCBtYXBWYWx1ZSwgZmFsc2UpO1xuICAgIH0pO1xuXG4gICAgY29uc3Qgb3V0cHV0RGVmczogby5MaXRlcmFsTWFwRW50cnlbXSA9IFtdO1xuICAgIGNvbnN0IGRpck1ldGEgPSBkaXJBc3QuZGlyZWN0aXZlO1xuICAgIE9iamVjdC5rZXlzKGRpck1ldGEub3V0cHV0cykuZm9yRWFjaCgocHJvcE5hbWUpID0+IHtcbiAgICAgIGNvbnN0IGV2ZW50TmFtZSA9IGRpck1ldGEub3V0cHV0c1twcm9wTmFtZV07XG4gICAgICBpZiAodXNlZEV2ZW50cy5oYXMoZXZlbnROYW1lKSkge1xuICAgICAgICAvLyBOb3RlOiBpdCdzIGltcG9ydGFudCB0byBub3QgcXVvdGUgdGhlIGtleSBzbyB0aGF0IHdlIGNhbiBjYXB0dXJlIHJlbmFtZXMgYnkgbWluaWZpZXJzIVxuICAgICAgICBvdXRwdXREZWZzLnB1c2gobmV3IG8uTGl0ZXJhbE1hcEVudHJ5KHByb3BOYW1lLCBvLmxpdGVyYWwoZXZlbnROYW1lKSwgZmFsc2UpKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICBsZXQgdXBkYXRlRGlyZWN0aXZlRXhwcmVzc2lvbnM6IFVwZGF0ZUV4cHJlc3Npb25bXSA9IFtdO1xuICAgIGlmIChkaXJBc3QuaW5wdXRzLmxlbmd0aCB8fCAoZmxhZ3MgJiAoTm9kZUZsYWdzLkRvQ2hlY2sgfCBOb2RlRmxhZ3MuT25Jbml0KSkgPiAwKSB7XG4gICAgICB1cGRhdGVEaXJlY3RpdmVFeHByZXNzaW9ucyA9XG4gICAgICAgICAgZGlyQXN0LmlucHV0cy5tYXAoKGlucHV0LCBiaW5kaW5nSW5kZXgpID0+IHRoaXMuX3ByZXByb2Nlc3NVcGRhdGVFeHByZXNzaW9uKHtcbiAgICAgICAgICAgIG5vZGVJbmRleCxcbiAgICAgICAgICAgIGJpbmRpbmdJbmRleCxcbiAgICAgICAgICAgIHNvdXJjZVNwYW46IGlucHV0LnNvdXJjZVNwYW4sXG4gICAgICAgICAgICBjb250ZXh0OiBDT01QX1ZBUixcbiAgICAgICAgICAgIHZhbHVlOiBpbnB1dC52YWx1ZVxuICAgICAgICAgIH0pKTtcbiAgICB9XG5cbiAgICBjb25zdCBkaXJDb250ZXh0RXhwciA9XG4gICAgICAgIG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5ub2RlVmFsdWUpLmNhbGxGbihbVklFV19WQVIsIG8ubGl0ZXJhbChub2RlSW5kZXgpXSk7XG4gICAgY29uc3QgaG9zdEJpbmRpbmdzID0gZGlyQXN0Lmhvc3RQcm9wZXJ0aWVzLm1hcCgoaW5wdXRBc3QpID0+ICh7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnRleHQ6IGRpckNvbnRleHRFeHByLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBkaXJBc3QsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlucHV0QXN0LFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfSkpO1xuICAgIGNvbnN0IGhvc3RFdmVudHMgPSBkaXJBc3QuaG9zdEV2ZW50cy5tYXAoKGhvc3RFdmVudEFzdCkgPT4gKHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgY29udGV4dDogZGlyQ29udGV4dEV4cHIsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV2ZW50QXN0OiBob3N0RXZlbnRBc3QsIGRpckFzdCxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pKTtcblxuICAgIC8vIENoZWNrIGluZGV4IGlzIHRoZSBzYW1lIGFzIHRoZSBub2RlIGluZGV4IGR1cmluZyBjb21waWxhdGlvblxuICAgIC8vIFRoZXkgbWlnaHQgb25seSBkaWZmZXIgYXQgcnVudGltZVxuICAgIGNvbnN0IGNoZWNrSW5kZXggPSBub2RlSW5kZXg7XG5cbiAgICB0aGlzLm5vZGVzW25vZGVJbmRleF0gPSAoKSA9PiAoe1xuICAgICAgc291cmNlU3BhbjogZGlyQXN0LnNvdXJjZVNwYW4sXG4gICAgICBub2RlRmxhZ3M6IE5vZGVGbGFncy5UeXBlRGlyZWN0aXZlIHwgZmxhZ3MsXG4gICAgICBub2RlRGVmOiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMuZGlyZWN0aXZlRGVmKS5jYWxsRm4oW1xuICAgICAgICBvLmxpdGVyYWwoY2hlY2tJbmRleCksXG4gICAgICAgIG8ubGl0ZXJhbChmbGFncyksXG4gICAgICAgIHF1ZXJ5TWF0Y2hFeHBycy5sZW5ndGggPyBvLmxpdGVyYWxBcnIocXVlcnlNYXRjaEV4cHJzKSA6IG8uTlVMTF9FWFBSLFxuICAgICAgICBvLmxpdGVyYWwoY2hpbGRDb3VudCksXG4gICAgICAgIHByb3ZpZGVyRXhwcixcbiAgICAgICAgZGVwc0V4cHIsXG4gICAgICAgIGlucHV0RGVmcy5sZW5ndGggPyBuZXcgby5MaXRlcmFsTWFwRXhwcihpbnB1dERlZnMpIDogby5OVUxMX0VYUFIsXG4gICAgICAgIG91dHB1dERlZnMubGVuZ3RoID8gbmV3IG8uTGl0ZXJhbE1hcEV4cHIob3V0cHV0RGVmcykgOiBvLk5VTExfRVhQUixcbiAgICAgIF0pLFxuICAgICAgdXBkYXRlRGlyZWN0aXZlczogdXBkYXRlRGlyZWN0aXZlRXhwcmVzc2lvbnMsXG4gICAgICBkaXJlY3RpdmU6IGRpckFzdC5kaXJlY3RpdmUudHlwZSxcbiAgICB9KTtcblxuICAgIHJldHVybiB7aG9zdEJpbmRpbmdzLCBob3N0RXZlbnRzfTtcbiAgfVxuXG4gIHByaXZhdGUgX3Zpc2l0UHJvdmlkZXIocHJvdmlkZXJBc3Q6IFByb3ZpZGVyQXN0LCBxdWVyeU1hdGNoZXM6IFF1ZXJ5TWF0Y2hbXSk6IHZvaWQge1xuICAgIHRoaXMuX2FkZFByb3ZpZGVyTm9kZSh0aGlzLl92aXNpdFByb3ZpZGVyT3JEaXJlY3RpdmUocHJvdmlkZXJBc3QsIHF1ZXJ5TWF0Y2hlcykpO1xuICB9XG5cbiAgcHJpdmF0ZSBfdmlzaXRDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXJQcm92aWRlcihkaXJlY3RpdmVzOiBEaXJlY3RpdmVBc3RbXSkge1xuICAgIGNvbnN0IGNvbXBvbmVudERpck1ldGEgPSBkaXJlY3RpdmVzLmZpbmQoZGlyQXN0ID0+IGRpckFzdC5kaXJlY3RpdmUuaXNDb21wb25lbnQpO1xuICAgIGlmIChjb21wb25lbnREaXJNZXRhICYmIGNvbXBvbmVudERpck1ldGEuZGlyZWN0aXZlLmVudHJ5Q29tcG9uZW50cy5sZW5ndGgpIHtcbiAgICAgIGNvbnN0IHtwcm92aWRlckV4cHIsIGRlcHNFeHByLCBmbGFncywgdG9rZW5FeHByfSA9IGNvbXBvbmVudEZhY3RvcnlSZXNvbHZlclByb3ZpZGVyRGVmKFxuICAgICAgICAgIHRoaXMucmVmbGVjdG9yLCB0aGlzLm91dHB1dEN0eCwgTm9kZUZsYWdzLlByaXZhdGVQcm92aWRlcixcbiAgICAgICAgICBjb21wb25lbnREaXJNZXRhLmRpcmVjdGl2ZS5lbnRyeUNvbXBvbmVudHMpO1xuICAgICAgdGhpcy5fYWRkUHJvdmlkZXJOb2RlKHtcbiAgICAgICAgcHJvdmlkZXJFeHByLFxuICAgICAgICBkZXBzRXhwcixcbiAgICAgICAgZmxhZ3MsXG4gICAgICAgIHRva2VuRXhwcixcbiAgICAgICAgcXVlcnlNYXRjaEV4cHJzOiBbXSxcbiAgICAgICAgc291cmNlU3BhbjogY29tcG9uZW50RGlyTWV0YS5zb3VyY2VTcGFuXG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIF9hZGRQcm92aWRlck5vZGUoZGF0YToge1xuICAgIGZsYWdzOiBOb2RlRmxhZ3MsXG4gICAgcXVlcnlNYXRjaEV4cHJzOiBvLkV4cHJlc3Npb25bXSxcbiAgICBwcm92aWRlckV4cHI6IG8uRXhwcmVzc2lvbixcbiAgICBkZXBzRXhwcjogby5FeHByZXNzaW9uLFxuICAgIHRva2VuRXhwcjogby5FeHByZXNzaW9uLFxuICAgIHNvdXJjZVNwYW46IFBhcnNlU291cmNlU3BhblxuICB9KSB7XG4gICAgY29uc3Qgbm9kZUluZGV4ID0gdGhpcy5ub2Rlcy5sZW5ndGg7XG4gICAgLy8gcHJvdmlkZXJEZWYoXG4gICAgLy8gICBmbGFnczogTm9kZUZsYWdzLCBtYXRjaGVkUXVlcmllczogW3N0cmluZywgUXVlcnlWYWx1ZVR5cGVdW10sIHRva2VuOmFueSxcbiAgICAvLyAgIHZhbHVlOiBhbnksIGRlcHM6IChbRGVwRmxhZ3MsIGFueV0gfCBhbnkpW10pOiBOb2RlRGVmO1xuICAgIHRoaXMubm9kZXMucHVzaChcbiAgICAgICAgKCkgPT4gKHtcbiAgICAgICAgICBzb3VyY2VTcGFuOiBkYXRhLnNvdXJjZVNwYW4sXG4gICAgICAgICAgbm9kZUZsYWdzOiBkYXRhLmZsYWdzLFxuICAgICAgICAgIG5vZGVEZWY6IG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5wcm92aWRlckRlZikuY2FsbEZuKFtcbiAgICAgICAgICAgIG8ubGl0ZXJhbChkYXRhLmZsYWdzKSxcbiAgICAgICAgICAgIGRhdGEucXVlcnlNYXRjaEV4cHJzLmxlbmd0aCA/IG8ubGl0ZXJhbEFycihkYXRhLnF1ZXJ5TWF0Y2hFeHBycykgOiBvLk5VTExfRVhQUixcbiAgICAgICAgICAgIGRhdGEudG9rZW5FeHByLCBkYXRhLnByb3ZpZGVyRXhwciwgZGF0YS5kZXBzRXhwclxuICAgICAgICAgIF0pXG4gICAgICAgIH0pKTtcbiAgfVxuXG4gIHByaXZhdGUgX3Zpc2l0UHJvdmlkZXJPckRpcmVjdGl2ZShwcm92aWRlckFzdDogUHJvdmlkZXJBc3QsIHF1ZXJ5TWF0Y2hlczogUXVlcnlNYXRjaFtdKToge1xuICAgIGZsYWdzOiBOb2RlRmxhZ3MsXG4gICAgdG9rZW5FeHByOiBvLkV4cHJlc3Npb24sXG4gICAgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLFxuICAgIHF1ZXJ5TWF0Y2hFeHByczogby5FeHByZXNzaW9uW10sXG4gICAgcHJvdmlkZXJFeHByOiBvLkV4cHJlc3Npb24sXG4gICAgZGVwc0V4cHI6IG8uRXhwcmVzc2lvblxuICB9IHtcbiAgICBsZXQgZmxhZ3MgPSBOb2RlRmxhZ3MuTm9uZTtcbiAgICBsZXQgcXVlcnlNYXRjaEV4cHJzOiBvLkV4cHJlc3Npb25bXSA9IFtdO1xuXG4gICAgcXVlcnlNYXRjaGVzLmZvckVhY2goKG1hdGNoKSA9PiB7XG4gICAgICBpZiAodG9rZW5SZWZlcmVuY2UobWF0Y2gudmFsdWUpID09PSB0b2tlblJlZmVyZW5jZShwcm92aWRlckFzdC50b2tlbikpIHtcbiAgICAgICAgcXVlcnlNYXRjaEV4cHJzLnB1c2goXG4gICAgICAgICAgICBvLmxpdGVyYWxBcnIoW28ubGl0ZXJhbChtYXRjaC5xdWVyeUlkKSwgby5saXRlcmFsKFF1ZXJ5VmFsdWVUeXBlLlByb3ZpZGVyKV0pKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICBjb25zdCB7cHJvdmlkZXJFeHByLCBkZXBzRXhwciwgZmxhZ3M6IHByb3ZpZGVyRmxhZ3MsIHRva2VuRXhwcn0gPVxuICAgICAgICBwcm92aWRlckRlZih0aGlzLm91dHB1dEN0eCwgcHJvdmlkZXJBc3QpO1xuICAgIHJldHVybiB7XG4gICAgICBmbGFnczogZmxhZ3MgfCBwcm92aWRlckZsYWdzLFxuICAgICAgcXVlcnlNYXRjaEV4cHJzLFxuICAgICAgcHJvdmlkZXJFeHByLFxuICAgICAgZGVwc0V4cHIsXG4gICAgICB0b2tlbkV4cHIsXG4gICAgICBzb3VyY2VTcGFuOiBwcm92aWRlckFzdC5zb3VyY2VTcGFuXG4gICAgfTtcbiAgfVxuXG4gIGdldExvY2FsKG5hbWU6IHN0cmluZyk6IG8uRXhwcmVzc2lvbnxudWxsIHtcbiAgICBpZiAobmFtZSA9PSBFdmVudEhhbmRsZXJWYXJzLmV2ZW50Lm5hbWUpIHtcbiAgICAgIHJldHVybiBFdmVudEhhbmRsZXJWYXJzLmV2ZW50O1xuICAgIH1cbiAgICBsZXQgY3VyclZpZXdFeHByOiBvLkV4cHJlc3Npb24gPSBWSUVXX1ZBUjtcbiAgICBmb3IgKGxldCBjdXJyQnVpbGRlcjogVmlld0J1aWxkZXJ8bnVsbCA9IHRoaXM7IGN1cnJCdWlsZGVyOyBjdXJyQnVpbGRlciA9IGN1cnJCdWlsZGVyLnBhcmVudCxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgY3VyclZpZXdFeHByID0gY3VyclZpZXdFeHByLnByb3AoJ3BhcmVudCcpLmNhc3Qoby5EWU5BTUlDX1RZUEUpKSB7XG4gICAgICAvLyBjaGVjayByZWZlcmVuY2VzXG4gICAgICBjb25zdCByZWZOb2RlSW5kZXggPSBjdXJyQnVpbGRlci5yZWZOb2RlSW5kaWNlc1tuYW1lXTtcbiAgICAgIGlmIChyZWZOb2RlSW5kZXggIT0gbnVsbCkge1xuICAgICAgICByZXR1cm4gby5pbXBvcnRFeHByKElkZW50aWZpZXJzLm5vZGVWYWx1ZSkuY2FsbEZuKFtjdXJyVmlld0V4cHIsIG8ubGl0ZXJhbChyZWZOb2RlSW5kZXgpXSk7XG4gICAgICB9XG5cbiAgICAgIC8vIGNoZWNrIHZhcmlhYmxlc1xuICAgICAgY29uc3QgdmFyQXN0ID0gY3VyckJ1aWxkZXIudmFyaWFibGVzLmZpbmQoKHZhckFzdCkgPT4gdmFyQXN0Lm5hbWUgPT09IG5hbWUpO1xuICAgICAgaWYgKHZhckFzdCkge1xuICAgICAgICBjb25zdCB2YXJWYWx1ZSA9IHZhckFzdC52YWx1ZSB8fCBJTVBMSUNJVF9URU1QTEFURV9WQVI7XG4gICAgICAgIHJldHVybiBjdXJyVmlld0V4cHIucHJvcCgnY29udGV4dCcpLnByb3AodmFyVmFsdWUpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIG5vdGlmeUltcGxpY2l0UmVjZWl2ZXJVc2UoKTogdm9pZCB7XG4gICAgLy8gTm90IG5lZWRlZCBpbiBWaWV3IEVuZ2luZSBhcyBWaWV3IEVuZ2luZSB3YWxrcyB0aHJvdWdoIHRoZSBnZW5lcmF0ZWRcbiAgICAvLyBleHByZXNzaW9ucyB0byBmaWd1cmUgb3V0IGlmIHRoZSBpbXBsaWNpdCByZWNlaXZlciBpcyB1c2VkIGFuZCBuZWVkc1xuICAgIC8vIHRvIGJlIGdlbmVyYXRlZCBhcyBwYXJ0IG9mIHRoZSBwcmUtdXBkYXRlIHN0YXRlbWVudHMuXG4gIH1cblxuICBwcml2YXRlIF9jcmVhdGVMaXRlcmFsQXJyYXlDb252ZXJ0ZXIoc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLCBhcmdDb3VudDogbnVtYmVyKTpcbiAgICAgIEJ1aWx0aW5Db252ZXJ0ZXIge1xuICAgIGlmIChhcmdDb3VudCA9PT0gMCkge1xuICAgICAgY29uc3QgdmFsdWVFeHByID0gby5pbXBvcnRFeHByKElkZW50aWZpZXJzLkVNUFRZX0FSUkFZKTtcbiAgICAgIHJldHVybiAoKSA9PiB2YWx1ZUV4cHI7XG4gICAgfVxuXG4gICAgY29uc3QgY2hlY2tJbmRleCA9IHRoaXMubm9kZXMubGVuZ3RoO1xuXG4gICAgdGhpcy5ub2Rlcy5wdXNoKCgpID0+ICh7XG4gICAgICAgICAgICAgICAgICAgICAgc291cmNlU3BhbixcbiAgICAgICAgICAgICAgICAgICAgICBub2RlRmxhZ3M6IE5vZGVGbGFncy5UeXBlUHVyZUFycmF5LFxuICAgICAgICAgICAgICAgICAgICAgIG5vZGVEZWY6IG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5wdXJlQXJyYXlEZWYpLmNhbGxGbihbXG4gICAgICAgICAgICAgICAgICAgICAgICBvLmxpdGVyYWwoY2hlY2tJbmRleCksXG4gICAgICAgICAgICAgICAgICAgICAgICBvLmxpdGVyYWwoYXJnQ291bnQpLFxuICAgICAgICAgICAgICAgICAgICAgIF0pXG4gICAgICAgICAgICAgICAgICAgIH0pKTtcblxuICAgIHJldHVybiAoYXJnczogby5FeHByZXNzaW9uW10pID0+IGNhbGxDaGVja1N0bXQoY2hlY2tJbmRleCwgYXJncyk7XG4gIH1cblxuICBwcml2YXRlIF9jcmVhdGVMaXRlcmFsTWFwQ29udmVydGVyKFxuICAgICAgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuLCBrZXlzOiB7a2V5OiBzdHJpbmcsIHF1b3RlZDogYm9vbGVhbn1bXSk6IEJ1aWx0aW5Db252ZXJ0ZXIge1xuICAgIGlmIChrZXlzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgY29uc3QgdmFsdWVFeHByID0gby5pbXBvcnRFeHByKElkZW50aWZpZXJzLkVNUFRZX01BUCk7XG4gICAgICByZXR1cm4gKCkgPT4gdmFsdWVFeHByO1xuICAgIH1cblxuICAgIGNvbnN0IG1hcCA9IG8ubGl0ZXJhbE1hcChrZXlzLm1hcCgoZSwgaSkgPT4gKHsuLi5lLCB2YWx1ZTogby5saXRlcmFsKGkpfSkpKTtcbiAgICBjb25zdCBjaGVja0luZGV4ID0gdGhpcy5ub2Rlcy5sZW5ndGg7XG4gICAgdGhpcy5ub2Rlcy5wdXNoKCgpID0+ICh7XG4gICAgICAgICAgICAgICAgICAgICAgc291cmNlU3BhbixcbiAgICAgICAgICAgICAgICAgICAgICBub2RlRmxhZ3M6IE5vZGVGbGFncy5UeXBlUHVyZU9iamVjdCxcbiAgICAgICAgICAgICAgICAgICAgICBub2RlRGVmOiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMucHVyZU9iamVjdERlZikuY2FsbEZuKFtcbiAgICAgICAgICAgICAgICAgICAgICAgIG8ubGl0ZXJhbChjaGVja0luZGV4KSxcbiAgICAgICAgICAgICAgICAgICAgICAgIG1hcCxcbiAgICAgICAgICAgICAgICAgICAgICBdKVxuICAgICAgICAgICAgICAgICAgICB9KSk7XG5cbiAgICByZXR1cm4gKGFyZ3M6IG8uRXhwcmVzc2lvbltdKSA9PiBjYWxsQ2hlY2tTdG10KGNoZWNrSW5kZXgsIGFyZ3MpO1xuICB9XG5cbiAgcHJpdmF0ZSBfY3JlYXRlUGlwZUNvbnZlcnRlcihleHByZXNzaW9uOiBVcGRhdGVFeHByZXNzaW9uLCBuYW1lOiBzdHJpbmcsIGFyZ0NvdW50OiBudW1iZXIpOlxuICAgICAgQnVpbHRpbkNvbnZlcnRlciB7XG4gICAgY29uc3QgcGlwZSA9IHRoaXMudXNlZFBpcGVzLmZpbmQoKHBpcGVTdW1tYXJ5KSA9PiBwaXBlU3VtbWFyeS5uYW1lID09PSBuYW1lKSAhO1xuICAgIGlmIChwaXBlLnB1cmUpIHtcbiAgICAgIGNvbnN0IGNoZWNrSW5kZXggPSB0aGlzLm5vZGVzLmxlbmd0aDtcbiAgICAgIHRoaXMubm9kZXMucHVzaCgoKSA9PiAoe1xuICAgICAgICAgICAgICAgICAgICAgICAgc291cmNlU3BhbjogZXhwcmVzc2lvbi5zb3VyY2VTcGFuLFxuICAgICAgICAgICAgICAgICAgICAgICAgbm9kZUZsYWdzOiBOb2RlRmxhZ3MuVHlwZVB1cmVQaXBlLFxuICAgICAgICAgICAgICAgICAgICAgICAgbm9kZURlZjogby5pbXBvcnRFeHByKElkZW50aWZpZXJzLnB1cmVQaXBlRGVmKS5jYWxsRm4oW1xuICAgICAgICAgICAgICAgICAgICAgICAgICBvLmxpdGVyYWwoY2hlY2tJbmRleCksXG4gICAgICAgICAgICAgICAgICAgICAgICAgIG8ubGl0ZXJhbChhcmdDb3VudCksXG4gICAgICAgICAgICAgICAgICAgICAgICBdKVxuICAgICAgICAgICAgICAgICAgICAgIH0pKTtcblxuICAgICAgLy8gZmluZCB1bmRlcmx5aW5nIHBpcGUgaW4gdGhlIGNvbXBvbmVudCB2aWV3XG4gICAgICBsZXQgY29tcFZpZXdFeHByOiBvLkV4cHJlc3Npb24gPSBWSUVXX1ZBUjtcbiAgICAgIGxldCBjb21wQnVpbGRlcjogVmlld0J1aWxkZXIgPSB0aGlzO1xuICAgICAgd2hpbGUgKGNvbXBCdWlsZGVyLnBhcmVudCkge1xuICAgICAgICBjb21wQnVpbGRlciA9IGNvbXBCdWlsZGVyLnBhcmVudDtcbiAgICAgICAgY29tcFZpZXdFeHByID0gY29tcFZpZXdFeHByLnByb3AoJ3BhcmVudCcpLmNhc3Qoby5EWU5BTUlDX1RZUEUpO1xuICAgICAgfVxuICAgICAgY29uc3QgcGlwZU5vZGVJbmRleCA9IGNvbXBCdWlsZGVyLnB1cmVQaXBlTm9kZUluZGljZXNbbmFtZV07XG4gICAgICBjb25zdCBwaXBlVmFsdWVFeHByOiBvLkV4cHJlc3Npb24gPVxuICAgICAgICAgIG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5ub2RlVmFsdWUpLmNhbGxGbihbY29tcFZpZXdFeHByLCBvLmxpdGVyYWwocGlwZU5vZGVJbmRleCldKTtcblxuICAgICAgcmV0dXJuIChhcmdzOiBvLkV4cHJlc3Npb25bXSkgPT4gY2FsbFVud3JhcFZhbHVlKFxuICAgICAgICAgICAgICAgICBleHByZXNzaW9uLm5vZGVJbmRleCwgZXhwcmVzc2lvbi5iaW5kaW5nSW5kZXgsXG4gICAgICAgICAgICAgICAgIGNhbGxDaGVja1N0bXQoY2hlY2tJbmRleCwgW3BpcGVWYWx1ZUV4cHJdLmNvbmNhdChhcmdzKSkpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBub2RlSW5kZXggPSB0aGlzLl9jcmVhdGVQaXBlKGV4cHJlc3Npb24uc291cmNlU3BhbiwgcGlwZSk7XG4gICAgICBjb25zdCBub2RlVmFsdWVFeHByID1cbiAgICAgICAgICBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMubm9kZVZhbHVlKS5jYWxsRm4oW1ZJRVdfVkFSLCBvLmxpdGVyYWwobm9kZUluZGV4KV0pO1xuXG4gICAgICByZXR1cm4gKGFyZ3M6IG8uRXhwcmVzc2lvbltdKSA9PiBjYWxsVW53cmFwVmFsdWUoXG4gICAgICAgICAgICAgICAgIGV4cHJlc3Npb24ubm9kZUluZGV4LCBleHByZXNzaW9uLmJpbmRpbmdJbmRleCxcbiAgICAgICAgICAgICAgICAgbm9kZVZhbHVlRXhwci5jYWxsTWV0aG9kKCd0cmFuc2Zvcm0nLCBhcmdzKSk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfY3JlYXRlUGlwZShzb3VyY2VTcGFuOiBQYXJzZVNvdXJjZVNwYW58bnVsbCwgcGlwZTogQ29tcGlsZVBpcGVTdW1tYXJ5KTogbnVtYmVyIHtcbiAgICBjb25zdCBub2RlSW5kZXggPSB0aGlzLm5vZGVzLmxlbmd0aDtcbiAgICBsZXQgZmxhZ3MgPSBOb2RlRmxhZ3MuTm9uZTtcbiAgICBwaXBlLnR5cGUubGlmZWN5Y2xlSG9va3MuZm9yRWFjaCgobGlmZWN5Y2xlSG9vaykgPT4ge1xuICAgICAgLy8gZm9yIHBpcGVzLCB3ZSBvbmx5IHN1cHBvcnQgbmdPbkRlc3Ryb3lcbiAgICAgIGlmIChsaWZlY3ljbGVIb29rID09PSBMaWZlY3ljbGVIb29rcy5PbkRlc3Ryb3kpIHtcbiAgICAgICAgZmxhZ3MgfD0gbGlmZWN5Y2xlSG9va1RvTm9kZUZsYWcobGlmZWN5Y2xlSG9vayk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICBjb25zdCBkZXBFeHBycyA9IHBpcGUudHlwZS5kaURlcHMubWFwKChkaURlcCkgPT4gZGVwRGVmKHRoaXMub3V0cHV0Q3R4LCBkaURlcCkpO1xuICAgIC8vIGZ1bmN0aW9uIHBpcGVEZWYoXG4gICAgLy8gICBmbGFnczogTm9kZUZsYWdzLCBjdG9yOiBhbnksIGRlcHM6IChbRGVwRmxhZ3MsIGFueV0gfCBhbnkpW10pOiBOb2RlRGVmXG4gICAgdGhpcy5ub2Rlcy5wdXNoKFxuICAgICAgICAoKSA9PiAoe1xuICAgICAgICAgIHNvdXJjZVNwYW4sXG4gICAgICAgICAgbm9kZUZsYWdzOiBOb2RlRmxhZ3MuVHlwZVBpcGUsXG4gICAgICAgICAgbm9kZURlZjogby5pbXBvcnRFeHByKElkZW50aWZpZXJzLnBpcGVEZWYpLmNhbGxGbihbXG4gICAgICAgICAgICBvLmxpdGVyYWwoZmxhZ3MpLCB0aGlzLm91dHB1dEN0eC5pbXBvcnRFeHByKHBpcGUudHlwZS5yZWZlcmVuY2UpLCBvLmxpdGVyYWxBcnIoZGVwRXhwcnMpXG4gICAgICAgICAgXSlcbiAgICAgICAgfSkpO1xuICAgIHJldHVybiBub2RlSW5kZXg7XG4gIH1cblxuICAvKipcbiAgICogRm9yIHRoZSBBU1QgaW4gYFVwZGF0ZUV4cHJlc3Npb24udmFsdWVgOlxuICAgKiAtIGNyZWF0ZSBub2RlcyBmb3IgcGlwZXMsIGxpdGVyYWwgYXJyYXlzIGFuZCwgbGl0ZXJhbCBtYXBzLFxuICAgKiAtIHVwZGF0ZSB0aGUgQVNUIHRvIHJlcGxhY2UgcGlwZXMsIGxpdGVyYWwgYXJyYXlzIGFuZCwgbGl0ZXJhbCBtYXBzIHdpdGggY2FsbHMgdG8gY2hlY2sgZm4uXG4gICAqXG4gICAqIFdBUk5JTkc6IFRoaXMgbWlnaHQgY3JlYXRlIG5ldyBub2RlRGVmcyAoZm9yIHBpcGVzIGFuZCBsaXRlcmFsIGFycmF5cyBhbmQgbGl0ZXJhbCBtYXBzKSFcbiAgICovXG4gIHByaXZhdGUgX3ByZXByb2Nlc3NVcGRhdGVFeHByZXNzaW9uKGV4cHJlc3Npb246IFVwZGF0ZUV4cHJlc3Npb24pOiBVcGRhdGVFeHByZXNzaW9uIHtcbiAgICByZXR1cm4ge1xuICAgICAgbm9kZUluZGV4OiBleHByZXNzaW9uLm5vZGVJbmRleCxcbiAgICAgIGJpbmRpbmdJbmRleDogZXhwcmVzc2lvbi5iaW5kaW5nSW5kZXgsXG4gICAgICBzb3VyY2VTcGFuOiBleHByZXNzaW9uLnNvdXJjZVNwYW4sXG4gICAgICBjb250ZXh0OiBleHByZXNzaW9uLmNvbnRleHQsXG4gICAgICB2YWx1ZTogY29udmVydFByb3BlcnR5QmluZGluZ0J1aWx0aW5zKFxuICAgICAgICAgIHtcbiAgICAgICAgICAgIGNyZWF0ZUxpdGVyYWxBcnJheUNvbnZlcnRlcjogKGFyZ0NvdW50OiBudW1iZXIpID0+IHRoaXMuX2NyZWF0ZUxpdGVyYWxBcnJheUNvbnZlcnRlcihcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGV4cHJlc3Npb24uc291cmNlU3BhbiwgYXJnQ291bnQpLFxuICAgICAgICAgICAgY3JlYXRlTGl0ZXJhbE1hcENvbnZlcnRlcjpcbiAgICAgICAgICAgICAgICAoa2V5czoge2tleTogc3RyaW5nLCBxdW90ZWQ6IGJvb2xlYW59W10pID0+XG4gICAgICAgICAgICAgICAgICAgIHRoaXMuX2NyZWF0ZUxpdGVyYWxNYXBDb252ZXJ0ZXIoZXhwcmVzc2lvbi5zb3VyY2VTcGFuLCBrZXlzKSxcbiAgICAgICAgICAgIGNyZWF0ZVBpcGVDb252ZXJ0ZXI6IChuYW1lOiBzdHJpbmcsIGFyZ0NvdW50OiBudW1iZXIpID0+XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5fY3JlYXRlUGlwZUNvbnZlcnRlcihleHByZXNzaW9uLCBuYW1lLCBhcmdDb3VudClcbiAgICAgICAgICB9LFxuICAgICAgICAgIGV4cHJlc3Npb24udmFsdWUpXG4gICAgfTtcbiAgfVxuXG4gIHByaXZhdGUgX2NyZWF0ZU5vZGVFeHByZXNzaW9ucygpOiB7XG4gICAgdXBkYXRlUmVuZGVyZXJTdG10czogby5TdGF0ZW1lbnRbXSxcbiAgICB1cGRhdGVEaXJlY3RpdmVzU3RtdHM6IG8uU3RhdGVtZW50W10sXG4gICAgbm9kZURlZkV4cHJzOiBvLkV4cHJlc3Npb25bXVxuICB9IHtcbiAgICBjb25zdCBzZWxmID0gdGhpcztcbiAgICBsZXQgdXBkYXRlQmluZGluZ0NvdW50ID0gMDtcbiAgICBjb25zdCB1cGRhdGVSZW5kZXJlclN0bXRzOiBvLlN0YXRlbWVudFtdID0gW107XG4gICAgY29uc3QgdXBkYXRlRGlyZWN0aXZlc1N0bXRzOiBvLlN0YXRlbWVudFtdID0gW107XG4gICAgY29uc3Qgbm9kZURlZkV4cHJzID0gdGhpcy5ub2Rlcy5tYXAoKGZhY3RvcnksIG5vZGVJbmRleCkgPT4ge1xuICAgICAgY29uc3Qge25vZGVEZWYsIG5vZGVGbGFncywgdXBkYXRlRGlyZWN0aXZlcywgdXBkYXRlUmVuZGVyZXIsIHNvdXJjZVNwYW59ID0gZmFjdG9yeSgpO1xuICAgICAgaWYgKHVwZGF0ZVJlbmRlcmVyKSB7XG4gICAgICAgIHVwZGF0ZVJlbmRlcmVyU3RtdHMucHVzaChcbiAgICAgICAgICAgIC4uLmNyZWF0ZVVwZGF0ZVN0YXRlbWVudHMobm9kZUluZGV4LCBzb3VyY2VTcGFuLCB1cGRhdGVSZW5kZXJlciwgZmFsc2UpKTtcbiAgICAgIH1cbiAgICAgIGlmICh1cGRhdGVEaXJlY3RpdmVzKSB7XG4gICAgICAgIHVwZGF0ZURpcmVjdGl2ZXNTdG10cy5wdXNoKC4uLmNyZWF0ZVVwZGF0ZVN0YXRlbWVudHMoXG4gICAgICAgICAgICBub2RlSW5kZXgsIHNvdXJjZVNwYW4sIHVwZGF0ZURpcmVjdGl2ZXMsXG4gICAgICAgICAgICAobm9kZUZsYWdzICYgKE5vZGVGbGFncy5Eb0NoZWNrIHwgTm9kZUZsYWdzLk9uSW5pdCkpID4gMCkpO1xuICAgICAgfVxuICAgICAgLy8gV2UgdXNlIGEgY29tbWEgZXhwcmVzc2lvbiB0byBjYWxsIHRoZSBsb2cgZnVuY3Rpb24gYmVmb3JlXG4gICAgICAvLyB0aGUgbm9kZURlZiBmdW5jdGlvbiwgYnV0IHN0aWxsIHVzZSB0aGUgcmVzdWx0IG9mIHRoZSBub2RlRGVmIGZ1bmN0aW9uXG4gICAgICAvLyBhcyB0aGUgdmFsdWUuXG4gICAgICAvLyBOb3RlOiBXZSBvbmx5IGFkZCB0aGUgbG9nZ2VyIHRvIGVsZW1lbnRzIC8gdGV4dCBub2RlcyxcbiAgICAgIC8vIHNvIHdlIGRvbid0IGdlbmVyYXRlIHRvbyBtdWNoIGNvZGUuXG4gICAgICBjb25zdCBsb2dXaXRoTm9kZURlZiA9IG5vZGVGbGFncyAmIE5vZGVGbGFncy5DYXRSZW5kZXJOb2RlID9cbiAgICAgICAgICBuZXcgby5Db21tYUV4cHIoW0xPR19WQVIuY2FsbEZuKFtdKS5jYWxsRm4oW10pLCBub2RlRGVmXSkgOlxuICAgICAgICAgIG5vZGVEZWY7XG4gICAgICByZXR1cm4gby5hcHBseVNvdXJjZVNwYW5Ub0V4cHJlc3Npb25JZk5lZWRlZChsb2dXaXRoTm9kZURlZiwgc291cmNlU3Bhbik7XG4gICAgfSk7XG4gICAgcmV0dXJuIHt1cGRhdGVSZW5kZXJlclN0bXRzLCB1cGRhdGVEaXJlY3RpdmVzU3RtdHMsIG5vZGVEZWZFeHByc307XG5cbiAgICBmdW5jdGlvbiBjcmVhdGVVcGRhdGVTdGF0ZW1lbnRzKFxuICAgICAgICBub2RlSW5kZXg6IG51bWJlciwgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuIHwgbnVsbCwgZXhwcmVzc2lvbnM6IFVwZGF0ZUV4cHJlc3Npb25bXSxcbiAgICAgICAgYWxsb3dFbXB0eUV4cHJzOiBib29sZWFuKTogby5TdGF0ZW1lbnRbXSB7XG4gICAgICBjb25zdCB1cGRhdGVTdG10czogby5TdGF0ZW1lbnRbXSA9IFtdO1xuICAgICAgY29uc3QgZXhwcnMgPSBleHByZXNzaW9ucy5tYXAoKHtzb3VyY2VTcGFuLCBjb250ZXh0LCB2YWx1ZX0pID0+IHtcbiAgICAgICAgY29uc3QgYmluZGluZ0lkID0gYCR7dXBkYXRlQmluZGluZ0NvdW50Kyt9YDtcbiAgICAgICAgY29uc3QgbmFtZVJlc29sdmVyID0gY29udGV4dCA9PT0gQ09NUF9WQVIgPyBzZWxmIDogbnVsbDtcbiAgICAgICAgY29uc3Qge3N0bXRzLCBjdXJyVmFsRXhwcn0gPVxuICAgICAgICAgICAgY29udmVydFByb3BlcnR5QmluZGluZyhuYW1lUmVzb2x2ZXIsIGNvbnRleHQsIHZhbHVlLCBiaW5kaW5nSWQsIEJpbmRpbmdGb3JtLkdlbmVyYWwpO1xuICAgICAgICB1cGRhdGVTdG10cy5wdXNoKC4uLnN0bXRzLm1hcChcbiAgICAgICAgICAgIChzdG10OiBvLlN0YXRlbWVudCkgPT4gby5hcHBseVNvdXJjZVNwYW5Ub1N0YXRlbWVudElmTmVlZGVkKHN0bXQsIHNvdXJjZVNwYW4pKSk7XG4gICAgICAgIHJldHVybiBvLmFwcGx5U291cmNlU3BhblRvRXhwcmVzc2lvbklmTmVlZGVkKGN1cnJWYWxFeHByLCBzb3VyY2VTcGFuKTtcbiAgICAgIH0pO1xuICAgICAgaWYgKGV4cHJlc3Npb25zLmxlbmd0aCB8fCBhbGxvd0VtcHR5RXhwcnMpIHtcbiAgICAgICAgdXBkYXRlU3RtdHMucHVzaChvLmFwcGx5U291cmNlU3BhblRvU3RhdGVtZW50SWZOZWVkZWQoXG4gICAgICAgICAgICBjYWxsQ2hlY2tTdG10KG5vZGVJbmRleCwgZXhwcnMpLnRvU3RtdCgpLCBzb3VyY2VTcGFuKSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gdXBkYXRlU3RtdHM7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBfY3JlYXRlRWxlbWVudEhhbmRsZUV2ZW50Rm4oXG4gICAgICBub2RlSW5kZXg6IG51bWJlcixcbiAgICAgIGhhbmRsZXJzOiB7Y29udGV4dDogby5FeHByZXNzaW9uLCBldmVudEFzdDogQm91bmRFdmVudEFzdCwgZGlyQXN0OiBEaXJlY3RpdmVBc3R9W10pIHtcbiAgICBjb25zdCBoYW5kbGVFdmVudFN0bXRzOiBvLlN0YXRlbWVudFtdID0gW107XG4gICAgbGV0IGhhbmRsZUV2ZW50QmluZGluZ0NvdW50ID0gMDtcbiAgICBoYW5kbGVycy5mb3JFYWNoKCh7Y29udGV4dCwgZXZlbnRBc3QsIGRpckFzdH0pID0+IHtcbiAgICAgIGNvbnN0IGJpbmRpbmdJZCA9IGAke2hhbmRsZUV2ZW50QmluZGluZ0NvdW50Kyt9YDtcbiAgICAgIGNvbnN0IG5hbWVSZXNvbHZlciA9IGNvbnRleHQgPT09IENPTVBfVkFSID8gdGhpcyA6IG51bGw7XG4gICAgICBjb25zdCB7c3RtdHMsIGFsbG93RGVmYXVsdH0gPVxuICAgICAgICAgIGNvbnZlcnRBY3Rpb25CaW5kaW5nKG5hbWVSZXNvbHZlciwgY29udGV4dCwgZXZlbnRBc3QuaGFuZGxlciwgYmluZGluZ0lkKTtcbiAgICAgIGNvbnN0IHRydWVTdG10cyA9IHN0bXRzO1xuICAgICAgaWYgKGFsbG93RGVmYXVsdCkge1xuICAgICAgICB0cnVlU3RtdHMucHVzaChBTExPV19ERUZBVUxUX1ZBUi5zZXQoYWxsb3dEZWZhdWx0LmFuZChBTExPV19ERUZBVUxUX1ZBUikpLnRvU3RtdCgpKTtcbiAgICAgIH1cbiAgICAgIGNvbnN0IHt0YXJnZXQ6IGV2ZW50VGFyZ2V0LCBuYW1lOiBldmVudE5hbWV9ID0gZWxlbWVudEV2ZW50TmFtZUFuZFRhcmdldChldmVudEFzdCwgZGlyQXN0KTtcbiAgICAgIGNvbnN0IGZ1bGxFdmVudE5hbWUgPSBlbGVtZW50RXZlbnRGdWxsTmFtZShldmVudFRhcmdldCwgZXZlbnROYW1lKTtcbiAgICAgIGhhbmRsZUV2ZW50U3RtdHMucHVzaChvLmFwcGx5U291cmNlU3BhblRvU3RhdGVtZW50SWZOZWVkZWQoXG4gICAgICAgICAgbmV3IG8uSWZTdG10KG8ubGl0ZXJhbChmdWxsRXZlbnROYW1lKS5pZGVudGljYWwoRVZFTlRfTkFNRV9WQVIpLCB0cnVlU3RtdHMpLFxuICAgICAgICAgIGV2ZW50QXN0LnNvdXJjZVNwYW4pKTtcbiAgICB9KTtcbiAgICBsZXQgaGFuZGxlRXZlbnRGbjogby5FeHByZXNzaW9uO1xuICAgIGlmIChoYW5kbGVFdmVudFN0bXRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnN0IHByZVN0bXRzOiBvLlN0YXRlbWVudFtdID1cbiAgICAgICAgICBbQUxMT1dfREVGQVVMVF9WQVIuc2V0KG8ubGl0ZXJhbCh0cnVlKSkudG9EZWNsU3RtdChvLkJPT0xfVFlQRSldO1xuICAgICAgaWYgKCF0aGlzLmNvbXBvbmVudC5pc0hvc3QgJiYgby5maW5kUmVhZFZhck5hbWVzKGhhbmRsZUV2ZW50U3RtdHMpLmhhcyhDT01QX1ZBUi5uYW1lICEpKSB7XG4gICAgICAgIHByZVN0bXRzLnB1c2goQ09NUF9WQVIuc2V0KFZJRVdfVkFSLnByb3AoJ2NvbXBvbmVudCcpKS50b0RlY2xTdG10KHRoaXMuY29tcFR5cGUpKTtcbiAgICAgIH1cbiAgICAgIGhhbmRsZUV2ZW50Rm4gPSBvLmZuKFxuICAgICAgICAgIFtcbiAgICAgICAgICAgIG5ldyBvLkZuUGFyYW0oVklFV19WQVIubmFtZSAhLCBvLklORkVSUkVEX1RZUEUpLFxuICAgICAgICAgICAgbmV3IG8uRm5QYXJhbShFVkVOVF9OQU1FX1ZBUi5uYW1lICEsIG8uSU5GRVJSRURfVFlQRSksXG4gICAgICAgICAgICBuZXcgby5GblBhcmFtKEV2ZW50SGFuZGxlclZhcnMuZXZlbnQubmFtZSAhLCBvLklORkVSUkVEX1RZUEUpXG4gICAgICAgICAgXSxcbiAgICAgICAgICBbLi4ucHJlU3RtdHMsIC4uLmhhbmRsZUV2ZW50U3RtdHMsIG5ldyBvLlJldHVyblN0YXRlbWVudChBTExPV19ERUZBVUxUX1ZBUildLFxuICAgICAgICAgIG8uSU5GRVJSRURfVFlQRSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGhhbmRsZUV2ZW50Rm4gPSBvLk5VTExfRVhQUjtcbiAgICB9XG4gICAgcmV0dXJuIGhhbmRsZUV2ZW50Rm47XG4gIH1cblxuICB2aXNpdERpcmVjdGl2ZShhc3Q6IERpcmVjdGl2ZUFzdCwgY29udGV4dDoge3VzZWRFdmVudHM6IFNldDxzdHJpbmc+fSk6IGFueSB7fVxuICB2aXNpdERpcmVjdGl2ZVByb3BlcnR5KGFzdDogQm91bmREaXJlY3RpdmVQcm9wZXJ0eUFzdCwgY29udGV4dDogYW55KTogYW55IHt9XG4gIHZpc2l0UmVmZXJlbmNlKGFzdDogUmVmZXJlbmNlQXN0LCBjb250ZXh0OiBhbnkpOiBhbnkge31cbiAgdmlzaXRWYXJpYWJsZShhc3Q6IFZhcmlhYmxlQXN0LCBjb250ZXh0OiBhbnkpOiBhbnkge31cbiAgdmlzaXRFdmVudChhc3Q6IEJvdW5kRXZlbnRBc3QsIGNvbnRleHQ6IGFueSk6IGFueSB7fVxuICB2aXNpdEVsZW1lbnRQcm9wZXJ0eShhc3Q6IEJvdW5kRWxlbWVudFByb3BlcnR5QXN0LCBjb250ZXh0OiBhbnkpOiBhbnkge31cbiAgdmlzaXRBdHRyKGFzdDogQXR0ckFzdCwgY29udGV4dDogYW55KTogYW55IHt9XG59XG5cbmZ1bmN0aW9uIG5lZWRzQWRkaXRpb25hbFJvb3ROb2RlKGFzdE5vZGVzOiBUZW1wbGF0ZUFzdFtdKTogYm9vbGVhbiB7XG4gIGNvbnN0IGxhc3RBc3ROb2RlID0gYXN0Tm9kZXNbYXN0Tm9kZXMubGVuZ3RoIC0gMV07XG4gIGlmIChsYXN0QXN0Tm9kZSBpbnN0YW5jZW9mIEVtYmVkZGVkVGVtcGxhdGVBc3QpIHtcbiAgICByZXR1cm4gbGFzdEFzdE5vZGUuaGFzVmlld0NvbnRhaW5lcjtcbiAgfVxuXG4gIGlmIChsYXN0QXN0Tm9kZSBpbnN0YW5jZW9mIEVsZW1lbnRBc3QpIHtcbiAgICBpZiAoaXNOZ0NvbnRhaW5lcihsYXN0QXN0Tm9kZS5uYW1lKSAmJiBsYXN0QXN0Tm9kZS5jaGlsZHJlbi5sZW5ndGgpIHtcbiAgICAgIHJldHVybiBuZWVkc0FkZGl0aW9uYWxSb290Tm9kZShsYXN0QXN0Tm9kZS5jaGlsZHJlbik7XG4gICAgfVxuICAgIHJldHVybiBsYXN0QXN0Tm9kZS5oYXNWaWV3Q29udGFpbmVyO1xuICB9XG5cbiAgcmV0dXJuIGxhc3RBc3ROb2RlIGluc3RhbmNlb2YgTmdDb250ZW50QXN0O1xufVxuXG5cbmZ1bmN0aW9uIGVsZW1lbnRCaW5kaW5nRGVmKGlucHV0QXN0OiBCb3VuZEVsZW1lbnRQcm9wZXJ0eUFzdCwgZGlyQXN0OiBEaXJlY3RpdmVBc3QpOiBvLkV4cHJlc3Npb24ge1xuICBjb25zdCBpbnB1dFR5cGUgPSBpbnB1dEFzdC50eXBlO1xuICBzd2l0Y2ggKGlucHV0VHlwZSkge1xuICAgIGNhc2UgUHJvcGVydHlCaW5kaW5nVHlwZS5BdHRyaWJ1dGU6XG4gICAgICByZXR1cm4gby5saXRlcmFsQXJyKFtcbiAgICAgICAgby5saXRlcmFsKEJpbmRpbmdGbGFncy5UeXBlRWxlbWVudEF0dHJpYnV0ZSksIG8ubGl0ZXJhbChpbnB1dEFzdC5uYW1lKSxcbiAgICAgICAgby5saXRlcmFsKGlucHV0QXN0LnNlY3VyaXR5Q29udGV4dClcbiAgICAgIF0pO1xuICAgIGNhc2UgUHJvcGVydHlCaW5kaW5nVHlwZS5Qcm9wZXJ0eTpcbiAgICAgIHJldHVybiBvLmxpdGVyYWxBcnIoW1xuICAgICAgICBvLmxpdGVyYWwoQmluZGluZ0ZsYWdzLlR5cGVQcm9wZXJ0eSksIG8ubGl0ZXJhbChpbnB1dEFzdC5uYW1lKSxcbiAgICAgICAgby5saXRlcmFsKGlucHV0QXN0LnNlY3VyaXR5Q29udGV4dClcbiAgICAgIF0pO1xuICAgIGNhc2UgUHJvcGVydHlCaW5kaW5nVHlwZS5BbmltYXRpb246XG4gICAgICBjb25zdCBiaW5kaW5nVHlwZSA9IEJpbmRpbmdGbGFncy5UeXBlUHJvcGVydHkgfFxuICAgICAgICAgIChkaXJBc3QgJiYgZGlyQXN0LmRpcmVjdGl2ZS5pc0NvbXBvbmVudCA/IEJpbmRpbmdGbGFncy5TeW50aGV0aWNIb3N0UHJvcGVydHkgOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIEJpbmRpbmdGbGFncy5TeW50aGV0aWNQcm9wZXJ0eSk7XG4gICAgICByZXR1cm4gby5saXRlcmFsQXJyKFtcbiAgICAgICAgby5saXRlcmFsKGJpbmRpbmdUeXBlKSwgby5saXRlcmFsKCdAJyArIGlucHV0QXN0Lm5hbWUpLCBvLmxpdGVyYWwoaW5wdXRBc3Quc2VjdXJpdHlDb250ZXh0KVxuICAgICAgXSk7XG4gICAgY2FzZSBQcm9wZXJ0eUJpbmRpbmdUeXBlLkNsYXNzOlxuICAgICAgcmV0dXJuIG8ubGl0ZXJhbEFycihcbiAgICAgICAgICBbby5saXRlcmFsKEJpbmRpbmdGbGFncy5UeXBlRWxlbWVudENsYXNzKSwgby5saXRlcmFsKGlucHV0QXN0Lm5hbWUpLCBvLk5VTExfRVhQUl0pO1xuICAgIGNhc2UgUHJvcGVydHlCaW5kaW5nVHlwZS5TdHlsZTpcbiAgICAgIHJldHVybiBvLmxpdGVyYWxBcnIoW1xuICAgICAgICBvLmxpdGVyYWwoQmluZGluZ0ZsYWdzLlR5cGVFbGVtZW50U3R5bGUpLCBvLmxpdGVyYWwoaW5wdXRBc3QubmFtZSksIG8ubGl0ZXJhbChpbnB1dEFzdC51bml0KVxuICAgICAgXSk7XG4gICAgZGVmYXVsdDpcbiAgICAgIC8vIFRoaXMgZGVmYXVsdCBjYXNlIGlzIG5vdCBuZWVkZWQgYnkgVHlwZVNjcmlwdCBjb21waWxlciwgYXMgdGhlIHN3aXRjaCBpcyBleGhhdXN0aXZlLlxuICAgICAgLy8gSG93ZXZlciBDbG9zdXJlIENvbXBpbGVyIGRvZXMgbm90IHVuZGVyc3RhbmQgdGhhdCBhbmQgcmVwb3J0cyBhbiBlcnJvciBpbiB0eXBlZCBtb2RlLlxuICAgICAgLy8gVGhlIGB0aHJvdyBuZXcgRXJyb3JgIGJlbG93IHdvcmtzIGFyb3VuZCB0aGUgcHJvYmxlbSwgYW5kIHRoZSB1bmV4cGVjdGVkOiBuZXZlciB2YXJpYWJsZVxuICAgICAgLy8gbWFrZXMgc3VyZSB0c2Mgc3RpbGwgY2hlY2tzIHRoaXMgY29kZSBpcyB1bnJlYWNoYWJsZS5cbiAgICAgIGNvbnN0IHVuZXhwZWN0ZWQ6IG5ldmVyID0gaW5wdXRUeXBlO1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGB1bmV4cGVjdGVkICR7dW5leHBlY3RlZH1gKTtcbiAgfVxufVxuXG5cbmZ1bmN0aW9uIGZpeGVkQXR0cnNEZWYoZWxlbWVudEFzdDogRWxlbWVudEFzdCk6IG8uRXhwcmVzc2lvbiB7XG4gIGNvbnN0IG1hcFJlc3VsdDoge1trZXk6IHN0cmluZ106IHN0cmluZ30gPSBPYmplY3QuY3JlYXRlKG51bGwpO1xuICBlbGVtZW50QXN0LmF0dHJzLmZvckVhY2goYXR0ckFzdCA9PiB7IG1hcFJlc3VsdFthdHRyQXN0Lm5hbWVdID0gYXR0ckFzdC52YWx1ZTsgfSk7XG4gIGVsZW1lbnRBc3QuZGlyZWN0aXZlcy5mb3JFYWNoKGRpckFzdCA9PiB7XG4gICAgT2JqZWN0LmtleXMoZGlyQXN0LmRpcmVjdGl2ZS5ob3N0QXR0cmlidXRlcykuZm9yRWFjaChuYW1lID0+IHtcbiAgICAgIGNvbnN0IHZhbHVlID0gZGlyQXN0LmRpcmVjdGl2ZS5ob3N0QXR0cmlidXRlc1tuYW1lXTtcbiAgICAgIGNvbnN0IHByZXZWYWx1ZSA9IG1hcFJlc3VsdFtuYW1lXTtcbiAgICAgIG1hcFJlc3VsdFtuYW1lXSA9IHByZXZWYWx1ZSAhPSBudWxsID8gbWVyZ2VBdHRyaWJ1dGVWYWx1ZShuYW1lLCBwcmV2VmFsdWUsIHZhbHVlKSA6IHZhbHVlO1xuICAgIH0pO1xuICB9KTtcbiAgLy8gTm90ZTogV2UgbmVlZCB0byBzb3J0IHRvIGdldCBhIGRlZmluZWQgb3V0cHV0IG9yZGVyXG4gIC8vIGZvciB0ZXN0cyBhbmQgZm9yIGNhY2hpbmcgZ2VuZXJhdGVkIGFydGlmYWN0cy4uLlxuICByZXR1cm4gby5saXRlcmFsQXJyKE9iamVjdC5rZXlzKG1hcFJlc3VsdCkuc29ydCgpLm1hcChcbiAgICAgIChhdHRyTmFtZSkgPT4gby5saXRlcmFsQXJyKFtvLmxpdGVyYWwoYXR0ck5hbWUpLCBvLmxpdGVyYWwobWFwUmVzdWx0W2F0dHJOYW1lXSldKSkpO1xufVxuXG5mdW5jdGlvbiBtZXJnZUF0dHJpYnV0ZVZhbHVlKGF0dHJOYW1lOiBzdHJpbmcsIGF0dHJWYWx1ZTE6IHN0cmluZywgYXR0clZhbHVlMjogc3RyaW5nKTogc3RyaW5nIHtcbiAgaWYgKGF0dHJOYW1lID09IENMQVNTX0FUVFIgfHwgYXR0ck5hbWUgPT0gU1RZTEVfQVRUUikge1xuICAgIHJldHVybiBgJHthdHRyVmFsdWUxfSAke2F0dHJWYWx1ZTJ9YDtcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gYXR0clZhbHVlMjtcbiAgfVxufVxuXG5mdW5jdGlvbiBjYWxsQ2hlY2tTdG10KG5vZGVJbmRleDogbnVtYmVyLCBleHByczogby5FeHByZXNzaW9uW10pOiBvLkV4cHJlc3Npb24ge1xuICBpZiAoZXhwcnMubGVuZ3RoID4gMTApIHtcbiAgICByZXR1cm4gQ0hFQ0tfVkFSLmNhbGxGbihcbiAgICAgICAgW1ZJRVdfVkFSLCBvLmxpdGVyYWwobm9kZUluZGV4KSwgby5saXRlcmFsKEFyZ3VtZW50VHlwZS5EeW5hbWljKSwgby5saXRlcmFsQXJyKGV4cHJzKV0pO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBDSEVDS19WQVIuY2FsbEZuKFxuICAgICAgICBbVklFV19WQVIsIG8ubGl0ZXJhbChub2RlSW5kZXgpLCBvLmxpdGVyYWwoQXJndW1lbnRUeXBlLklubGluZSksIC4uLmV4cHJzXSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2FsbFVud3JhcFZhbHVlKG5vZGVJbmRleDogbnVtYmVyLCBiaW5kaW5nSWR4OiBudW1iZXIsIGV4cHI6IG8uRXhwcmVzc2lvbik6IG8uRXhwcmVzc2lvbiB7XG4gIHJldHVybiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMudW53cmFwVmFsdWUpLmNhbGxGbihbXG4gICAgVklFV19WQVIsIG8ubGl0ZXJhbChub2RlSW5kZXgpLCBvLmxpdGVyYWwoYmluZGluZ0lkeCksIGV4cHJcbiAgXSk7XG59XG5cbmludGVyZmFjZSBTdGF0aWNBbmREeW5hbWljUXVlcnlJZHMge1xuICBzdGF0aWNRdWVyeUlkczogU2V0PG51bWJlcj47XG4gIGR5bmFtaWNRdWVyeUlkczogU2V0PG51bWJlcj47XG59XG5cblxuZXhwb3J0IGZ1bmN0aW9uIGZpbmRTdGF0aWNRdWVyeUlkcyhcbiAgICBub2RlczogVGVtcGxhdGVBc3RbXSwgcmVzdWx0ID0gbmV3IE1hcDxUZW1wbGF0ZUFzdCwgU3RhdGljQW5kRHluYW1pY1F1ZXJ5SWRzPigpKTpcbiAgICBNYXA8VGVtcGxhdGVBc3QsIFN0YXRpY0FuZER5bmFtaWNRdWVyeUlkcz4ge1xuICBub2Rlcy5mb3JFYWNoKChub2RlKSA9PiB7XG4gICAgY29uc3Qgc3RhdGljUXVlcnlJZHMgPSBuZXcgU2V0PG51bWJlcj4oKTtcbiAgICBjb25zdCBkeW5hbWljUXVlcnlJZHMgPSBuZXcgU2V0PG51bWJlcj4oKTtcbiAgICBsZXQgcXVlcnlNYXRjaGVzOiBRdWVyeU1hdGNoW10gPSB1bmRlZmluZWQgITtcbiAgICBpZiAobm9kZSBpbnN0YW5jZW9mIEVsZW1lbnRBc3QpIHtcbiAgICAgIGZpbmRTdGF0aWNRdWVyeUlkcyhub2RlLmNoaWxkcmVuLCByZXN1bHQpO1xuICAgICAgbm9kZS5jaGlsZHJlbi5mb3JFYWNoKChjaGlsZCkgPT4ge1xuICAgICAgICBjb25zdCBjaGlsZERhdGEgPSByZXN1bHQuZ2V0KGNoaWxkKSAhO1xuICAgICAgICBjaGlsZERhdGEuc3RhdGljUXVlcnlJZHMuZm9yRWFjaChxdWVyeUlkID0+IHN0YXRpY1F1ZXJ5SWRzLmFkZChxdWVyeUlkKSk7XG4gICAgICAgIGNoaWxkRGF0YS5keW5hbWljUXVlcnlJZHMuZm9yRWFjaChxdWVyeUlkID0+IGR5bmFtaWNRdWVyeUlkcy5hZGQocXVlcnlJZCkpO1xuICAgICAgfSk7XG4gICAgICBxdWVyeU1hdGNoZXMgPSBub2RlLnF1ZXJ5TWF0Y2hlcztcbiAgICB9IGVsc2UgaWYgKG5vZGUgaW5zdGFuY2VvZiBFbWJlZGRlZFRlbXBsYXRlQXN0KSB7XG4gICAgICBmaW5kU3RhdGljUXVlcnlJZHMobm9kZS5jaGlsZHJlbiwgcmVzdWx0KTtcbiAgICAgIG5vZGUuY2hpbGRyZW4uZm9yRWFjaCgoY2hpbGQpID0+IHtcbiAgICAgICAgY29uc3QgY2hpbGREYXRhID0gcmVzdWx0LmdldChjaGlsZCkgITtcbiAgICAgICAgY2hpbGREYXRhLnN0YXRpY1F1ZXJ5SWRzLmZvckVhY2gocXVlcnlJZCA9PiBkeW5hbWljUXVlcnlJZHMuYWRkKHF1ZXJ5SWQpKTtcbiAgICAgICAgY2hpbGREYXRhLmR5bmFtaWNRdWVyeUlkcy5mb3JFYWNoKHF1ZXJ5SWQgPT4gZHluYW1pY1F1ZXJ5SWRzLmFkZChxdWVyeUlkKSk7XG4gICAgICB9KTtcbiAgICAgIHF1ZXJ5TWF0Y2hlcyA9IG5vZGUucXVlcnlNYXRjaGVzO1xuICAgIH1cbiAgICBpZiAocXVlcnlNYXRjaGVzKSB7XG4gICAgICBxdWVyeU1hdGNoZXMuZm9yRWFjaCgobWF0Y2gpID0+IHN0YXRpY1F1ZXJ5SWRzLmFkZChtYXRjaC5xdWVyeUlkKSk7XG4gICAgfVxuICAgIGR5bmFtaWNRdWVyeUlkcy5mb3JFYWNoKHF1ZXJ5SWQgPT4gc3RhdGljUXVlcnlJZHMuZGVsZXRlKHF1ZXJ5SWQpKTtcbiAgICByZXN1bHQuc2V0KG5vZGUsIHtzdGF0aWNRdWVyeUlkcywgZHluYW1pY1F1ZXJ5SWRzfSk7XG4gIH0pO1xuICByZXR1cm4gcmVzdWx0O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc3RhdGljVmlld1F1ZXJ5SWRzKG5vZGVTdGF0aWNRdWVyeUlkczogTWFwPFRlbXBsYXRlQXN0LCBTdGF0aWNBbmREeW5hbWljUXVlcnlJZHM+KTpcbiAgICBTdGF0aWNBbmREeW5hbWljUXVlcnlJZHMge1xuICBjb25zdCBzdGF0aWNRdWVyeUlkcyA9IG5ldyBTZXQ8bnVtYmVyPigpO1xuICBjb25zdCBkeW5hbWljUXVlcnlJZHMgPSBuZXcgU2V0PG51bWJlcj4oKTtcbiAgQXJyYXkuZnJvbShub2RlU3RhdGljUXVlcnlJZHMudmFsdWVzKCkpLmZvckVhY2goKGVudHJ5KSA9PiB7XG4gICAgZW50cnkuc3RhdGljUXVlcnlJZHMuZm9yRWFjaChxdWVyeUlkID0+IHN0YXRpY1F1ZXJ5SWRzLmFkZChxdWVyeUlkKSk7XG4gICAgZW50cnkuZHluYW1pY1F1ZXJ5SWRzLmZvckVhY2gocXVlcnlJZCA9PiBkeW5hbWljUXVlcnlJZHMuYWRkKHF1ZXJ5SWQpKTtcbiAgfSk7XG4gIGR5bmFtaWNRdWVyeUlkcy5mb3JFYWNoKHF1ZXJ5SWQgPT4gc3RhdGljUXVlcnlJZHMuZGVsZXRlKHF1ZXJ5SWQpKTtcbiAgcmV0dXJuIHtzdGF0aWNRdWVyeUlkcywgZHluYW1pY1F1ZXJ5SWRzfTtcbn1cblxuZnVuY3Rpb24gZWxlbWVudEV2ZW50TmFtZUFuZFRhcmdldChcbiAgICBldmVudEFzdDogQm91bmRFdmVudEFzdCwgZGlyQXN0OiBEaXJlY3RpdmVBc3QgfCBudWxsKToge25hbWU6IHN0cmluZywgdGFyZ2V0OiBzdHJpbmcgfCBudWxsfSB7XG4gIGlmIChldmVudEFzdC5pc0FuaW1hdGlvbikge1xuICAgIHJldHVybiB7XG4gICAgICBuYW1lOiBgQCR7ZXZlbnRBc3QubmFtZX0uJHtldmVudEFzdC5waGFzZX1gLFxuICAgICAgdGFyZ2V0OiBkaXJBc3QgJiYgZGlyQXN0LmRpcmVjdGl2ZS5pc0NvbXBvbmVudCA/ICdjb21wb25lbnQnIDogbnVsbFxuICAgIH07XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIGV2ZW50QXN0O1xuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGNTdGF0aWNEeW5hbWljUXVlcnlGbGFncyhcbiAgICBxdWVyeUlkczogU3RhdGljQW5kRHluYW1pY1F1ZXJ5SWRzLCBxdWVyeUlkOiBudW1iZXIsIHF1ZXJ5OiBDb21waWxlUXVlcnlNZXRhZGF0YSkge1xuICBsZXQgZmxhZ3MgPSBOb2RlRmxhZ3MuTm9uZTtcbiAgLy8gTm90ZTogV2Ugb25seSBtYWtlIHF1ZXJpZXMgc3RhdGljIHRoYXQgcXVlcnkgZm9yIGEgc2luZ2xlIGl0ZW0uXG4gIC8vIFRoaXMgaXMgYmVjYXVzZSBvZiBiYWNrd2FyZHMgY29tcGF0aWJpbGl0eSB3aXRoIHRoZSBvbGQgdmlldyBjb21waWxlci4uLlxuICBpZiAocXVlcnkuZmlyc3QgJiYgc2hvdWxkUmVzb2x2ZUFzU3RhdGljUXVlcnkocXVlcnlJZHMsIHF1ZXJ5SWQsIHF1ZXJ5KSkge1xuICAgIGZsYWdzIHw9IE5vZGVGbGFncy5TdGF0aWNRdWVyeTtcbiAgfSBlbHNlIHtcbiAgICBmbGFncyB8PSBOb2RlRmxhZ3MuRHluYW1pY1F1ZXJ5O1xuICB9XG4gIHJldHVybiBmbGFncztcbn1cblxuZnVuY3Rpb24gc2hvdWxkUmVzb2x2ZUFzU3RhdGljUXVlcnkoXG4gICAgcXVlcnlJZHM6IFN0YXRpY0FuZER5bmFtaWNRdWVyeUlkcywgcXVlcnlJZDogbnVtYmVyLCBxdWVyeTogQ29tcGlsZVF1ZXJ5TWV0YWRhdGEpOiBib29sZWFuIHtcbiAgLy8gSWYgcXVlcnkuc3RhdGljIGhhcyBiZWVuIHNldCBieSB0aGUgdXNlciwgdXNlIHRoYXQgdmFsdWUgdG8gZGV0ZXJtaW5lIHdoZXRoZXJcbiAgLy8gdGhlIHF1ZXJ5IGlzIHN0YXRpYy4gSWYgbm9uZSBoYXMgYmVlbiBzZXQsIHNvcnQgdGhlIHF1ZXJ5IGludG8gc3RhdGljL2R5bmFtaWNcbiAgLy8gYmFzZWQgb24gcXVlcnkgcmVzdWx0cyAoaS5lLiBkeW5hbWljIGlmIENEIG5lZWRzIHRvIHJ1biB0byBnZXQgYWxsIHJlc3VsdHMpLlxuICByZXR1cm4gcXVlcnkuc3RhdGljIHx8XG4gICAgICBxdWVyeS5zdGF0aWMgPT0gbnVsbCAmJlxuICAgICAgKHF1ZXJ5SWRzLnN0YXRpY1F1ZXJ5SWRzLmhhcyhxdWVyeUlkKSB8fCAhcXVlcnlJZHMuZHluYW1pY1F1ZXJ5SWRzLmhhcyhxdWVyeUlkKSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBlbGVtZW50RXZlbnRGdWxsTmFtZSh0YXJnZXQ6IHN0cmluZyB8IG51bGwsIG5hbWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiB0YXJnZXQgPyBgJHt0YXJnZXR9OiR7bmFtZX1gIDogbmFtZTtcbn1cbiJdfQ==