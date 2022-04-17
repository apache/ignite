/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { checkAndUpdateElementDynamic, checkAndUpdateElementInline, createElement, listenToElementOutputs } from './element';
import { expressionChangedAfterItHasBeenCheckedError } from './errors';
import { appendNgContent } from './ng_content';
import { callLifecycleHooksChildrenFirst, checkAndUpdateDirectiveDynamic, checkAndUpdateDirectiveInline, createDirectiveInstance, createPipeInstance, createProviderInstance } from './provider';
import { checkAndUpdatePureExpressionDynamic, checkAndUpdatePureExpressionInline, createPureExpression } from './pure_expression';
import { checkAndUpdateQuery, createQuery } from './query';
import { createTemplateData, createViewContainerData } from './refs';
import { checkAndUpdateTextDynamic, checkAndUpdateTextInline, createText } from './text';
import { Services, asElementData, asQueryList, asTextData, shiftInitState } from './types';
import { NOOP, checkBindingNoChanges, isComponentView, markParentViewsForCheckProjectedViews, resolveDefinition, tokenKey } from './util';
import { detachProjectedView } from './view_attach';
export function viewDef(flags, nodes, updateDirectives, updateRenderer) {
    // clone nodes and set auto calculated values
    var viewBindingCount = 0;
    var viewDisposableCount = 0;
    var viewNodeFlags = 0;
    var viewRootNodeFlags = 0;
    var viewMatchedQueries = 0;
    var currentParent = null;
    var currentRenderParent = null;
    var currentElementHasPublicProviders = false;
    var currentElementHasPrivateProviders = false;
    var lastRenderRootNode = null;
    for (var i = 0; i < nodes.length; i++) {
        var node = nodes[i];
        node.nodeIndex = i;
        node.parent = currentParent;
        node.bindingIndex = viewBindingCount;
        node.outputIndex = viewDisposableCount;
        node.renderParent = currentRenderParent;
        viewNodeFlags |= node.flags;
        viewMatchedQueries |= node.matchedQueryIds;
        if (node.element) {
            var elDef = node.element;
            elDef.publicProviders =
                currentParent ? currentParent.element.publicProviders : Object.create(null);
            elDef.allProviders = elDef.publicProviders;
            // Note: We assume that all providers of an element are before any child element!
            currentElementHasPublicProviders = false;
            currentElementHasPrivateProviders = false;
            if (node.element.template) {
                viewMatchedQueries |= node.element.template.nodeMatchedQueries;
            }
        }
        validateNode(currentParent, node, nodes.length);
        viewBindingCount += node.bindings.length;
        viewDisposableCount += node.outputs.length;
        if (!currentRenderParent && (node.flags & 3 /* CatRenderNode */)) {
            lastRenderRootNode = node;
        }
        if (node.flags & 20224 /* CatProvider */) {
            if (!currentElementHasPublicProviders) {
                currentElementHasPublicProviders = true;
                // Use prototypical inheritance to not get O(n^2) complexity...
                currentParent.element.publicProviders =
                    Object.create(currentParent.element.publicProviders);
                currentParent.element.allProviders = currentParent.element.publicProviders;
            }
            var isPrivateService = (node.flags & 8192 /* PrivateProvider */) !== 0;
            var isComponent = (node.flags & 32768 /* Component */) !== 0;
            if (!isPrivateService || isComponent) {
                currentParent.element.publicProviders[tokenKey(node.provider.token)] = node;
            }
            else {
                if (!currentElementHasPrivateProviders) {
                    currentElementHasPrivateProviders = true;
                    // Use prototypical inheritance to not get O(n^2) complexity...
                    currentParent.element.allProviders =
                        Object.create(currentParent.element.publicProviders);
                }
                currentParent.element.allProviders[tokenKey(node.provider.token)] = node;
            }
            if (isComponent) {
                currentParent.element.componentProvider = node;
            }
        }
        if (currentParent) {
            currentParent.childFlags |= node.flags;
            currentParent.directChildFlags |= node.flags;
            currentParent.childMatchedQueries |= node.matchedQueryIds;
            if (node.element && node.element.template) {
                currentParent.childMatchedQueries |= node.element.template.nodeMatchedQueries;
            }
        }
        else {
            viewRootNodeFlags |= node.flags;
        }
        if (node.childCount > 0) {
            currentParent = node;
            if (!isNgContainer(node)) {
                currentRenderParent = node;
            }
        }
        else {
            // When the current node has no children, check if it is the last children of its parent.
            // When it is, propagate the flags up.
            // The loop is required because an element could be the last transitive children of several
            // elements. We loop to either the root or the highest opened element (= with remaining
            // children)
            while (currentParent && i === currentParent.nodeIndex + currentParent.childCount) {
                var newParent = currentParent.parent;
                if (newParent) {
                    newParent.childFlags |= currentParent.childFlags;
                    newParent.childMatchedQueries |= currentParent.childMatchedQueries;
                }
                currentParent = newParent;
                // We also need to update the render parent & account for ng-container
                if (currentParent && isNgContainer(currentParent)) {
                    currentRenderParent = currentParent.renderParent;
                }
                else {
                    currentRenderParent = currentParent;
                }
            }
        }
    }
    var handleEvent = function (view, nodeIndex, eventName, event) {
        return nodes[nodeIndex].element.handleEvent(view, eventName, event);
    };
    return {
        // Will be filled later...
        factory: null,
        nodeFlags: viewNodeFlags,
        rootNodeFlags: viewRootNodeFlags,
        nodeMatchedQueries: viewMatchedQueries, flags: flags,
        nodes: nodes,
        updateDirectives: updateDirectives || NOOP,
        updateRenderer: updateRenderer || NOOP, handleEvent: handleEvent,
        bindingCount: viewBindingCount,
        outputCount: viewDisposableCount, lastRenderRootNode: lastRenderRootNode
    };
}
function isNgContainer(node) {
    return (node.flags & 1 /* TypeElement */) !== 0 && node.element.name === null;
}
function validateNode(parent, node, nodeCount) {
    var template = node.element && node.element.template;
    if (template) {
        if (!template.lastRenderRootNode) {
            throw new Error("Illegal State: Embedded templates without nodes are not allowed!");
        }
        if (template.lastRenderRootNode &&
            template.lastRenderRootNode.flags & 16777216 /* EmbeddedViews */) {
            throw new Error("Illegal State: Last root node of a template can't have embedded views, at index " + node.nodeIndex + "!");
        }
    }
    if (node.flags & 20224 /* CatProvider */) {
        var parentFlags = parent ? parent.flags : 0;
        if ((parentFlags & 1 /* TypeElement */) === 0) {
            throw new Error("Illegal State: StaticProvider/Directive nodes need to be children of elements or anchors, at index " + node.nodeIndex + "!");
        }
    }
    if (node.query) {
        if (node.flags & 67108864 /* TypeContentQuery */ &&
            (!parent || (parent.flags & 16384 /* TypeDirective */) === 0)) {
            throw new Error("Illegal State: Content Query nodes need to be children of directives, at index " + node.nodeIndex + "!");
        }
        if (node.flags & 134217728 /* TypeViewQuery */ && parent) {
            throw new Error("Illegal State: View Query nodes have to be top level nodes, at index " + node.nodeIndex + "!");
        }
    }
    if (node.childCount) {
        var parentEnd = parent ? parent.nodeIndex + parent.childCount : nodeCount - 1;
        if (node.nodeIndex <= parentEnd && node.nodeIndex + node.childCount > parentEnd) {
            throw new Error("Illegal State: childCount of node leads outside of parent, at index " + node.nodeIndex + "!");
        }
    }
}
export function createEmbeddedView(parent, anchorDef, viewDef, context) {
    // embedded views are seen as siblings to the anchor, so we need
    // to get the parent of the anchor and use it as parentIndex.
    var view = createView(parent.root, parent.renderer, parent, anchorDef, viewDef);
    initView(view, parent.component, context);
    createViewNodes(view);
    return view;
}
export function createRootView(root, def, context) {
    var view = createView(root, root.renderer, null, null, def);
    initView(view, context, context);
    createViewNodes(view);
    return view;
}
export function createComponentView(parentView, nodeDef, viewDef, hostElement) {
    var rendererType = nodeDef.element.componentRendererType;
    var compRenderer;
    if (!rendererType) {
        compRenderer = parentView.root.renderer;
    }
    else {
        compRenderer = parentView.root.rendererFactory.createRenderer(hostElement, rendererType);
    }
    return createView(parentView.root, compRenderer, parentView, nodeDef.element.componentProvider, viewDef);
}
function createView(root, renderer, parent, parentNodeDef, def) {
    var nodes = new Array(def.nodes.length);
    var disposables = def.outputCount ? new Array(def.outputCount) : null;
    var view = {
        def: def,
        parent: parent,
        viewContainerParent: null, parentNodeDef: parentNodeDef,
        context: null,
        component: null, nodes: nodes,
        state: 13 /* CatInit */, root: root, renderer: renderer,
        oldValues: new Array(def.bindingCount), disposables: disposables,
        initIndex: -1
    };
    return view;
}
function initView(view, component, context) {
    view.component = component;
    view.context = context;
}
function createViewNodes(view) {
    var renderHost;
    if (isComponentView(view)) {
        var hostDef = view.parentNodeDef;
        renderHost = asElementData(view.parent, hostDef.parent.nodeIndex).renderElement;
    }
    var def = view.def;
    var nodes = view.nodes;
    for (var i = 0; i < def.nodes.length; i++) {
        var nodeDef = def.nodes[i];
        Services.setCurrentNode(view, i);
        var nodeData = void 0;
        switch (nodeDef.flags & 201347067 /* Types */) {
            case 1 /* TypeElement */:
                var el = createElement(view, renderHost, nodeDef);
                var componentView = undefined;
                if (nodeDef.flags & 33554432 /* ComponentView */) {
                    var compViewDef = resolveDefinition(nodeDef.element.componentView);
                    componentView = Services.createComponentView(view, nodeDef, compViewDef, el);
                }
                listenToElementOutputs(view, componentView, nodeDef, el);
                nodeData = {
                    renderElement: el,
                    componentView: componentView,
                    viewContainer: null,
                    template: nodeDef.element.template ? createTemplateData(view, nodeDef) : undefined
                };
                if (nodeDef.flags & 16777216 /* EmbeddedViews */) {
                    nodeData.viewContainer = createViewContainerData(view, nodeDef, nodeData);
                }
                break;
            case 2 /* TypeText */:
                nodeData = createText(view, renderHost, nodeDef);
                break;
            case 512 /* TypeClassProvider */:
            case 1024 /* TypeFactoryProvider */:
            case 2048 /* TypeUseExistingProvider */:
            case 256 /* TypeValueProvider */: {
                nodeData = nodes[i];
                if (!nodeData && !(nodeDef.flags & 4096 /* LazyProvider */)) {
                    var instance = createProviderInstance(view, nodeDef);
                    nodeData = { instance: instance };
                }
                break;
            }
            case 16 /* TypePipe */: {
                var instance = createPipeInstance(view, nodeDef);
                nodeData = { instance: instance };
                break;
            }
            case 16384 /* TypeDirective */: {
                nodeData = nodes[i];
                if (!nodeData) {
                    var instance = createDirectiveInstance(view, nodeDef);
                    nodeData = { instance: instance };
                }
                if (nodeDef.flags & 32768 /* Component */) {
                    var compView = asElementData(view, nodeDef.parent.nodeIndex).componentView;
                    initView(compView, nodeData.instance, nodeData.instance);
                }
                break;
            }
            case 32 /* TypePureArray */:
            case 64 /* TypePureObject */:
            case 128 /* TypePurePipe */:
                nodeData = createPureExpression(view, nodeDef);
                break;
            case 67108864 /* TypeContentQuery */:
            case 134217728 /* TypeViewQuery */:
                nodeData = createQuery();
                break;
            case 8 /* TypeNgContent */:
                appendNgContent(view, renderHost, nodeDef);
                // no runtime data needed for NgContent...
                nodeData = undefined;
                break;
        }
        nodes[i] = nodeData;
    }
    // Create the ViewData.nodes of component views after we created everything else,
    // so that e.g. ng-content works
    execComponentViewsAction(view, ViewAction.CreateViewNodes);
    // fill static content and view queries
    execQueriesAction(view, 67108864 /* TypeContentQuery */ | 134217728 /* TypeViewQuery */, 268435456 /* StaticQuery */, 0 /* CheckAndUpdate */);
}
export function checkNoChangesView(view) {
    markProjectedViewsForCheck(view);
    Services.updateDirectives(view, 1 /* CheckNoChanges */);
    execEmbeddedViewsAction(view, ViewAction.CheckNoChanges);
    Services.updateRenderer(view, 1 /* CheckNoChanges */);
    execComponentViewsAction(view, ViewAction.CheckNoChanges);
    // Note: We don't check queries for changes as we didn't do this in v2.x.
    // TODO(tbosch): investigate if we can enable the check again in v5.x with a nicer error message.
    view.state &= ~(64 /* CheckProjectedViews */ | 32 /* CheckProjectedView */);
}
export function checkAndUpdateView(view) {
    if (view.state & 1 /* BeforeFirstCheck */) {
        view.state &= ~1 /* BeforeFirstCheck */;
        view.state |= 2 /* FirstCheck */;
    }
    else {
        view.state &= ~2 /* FirstCheck */;
    }
    shiftInitState(view, 0 /* InitState_BeforeInit */, 256 /* InitState_CallingOnInit */);
    markProjectedViewsForCheck(view);
    Services.updateDirectives(view, 0 /* CheckAndUpdate */);
    execEmbeddedViewsAction(view, ViewAction.CheckAndUpdate);
    execQueriesAction(view, 67108864 /* TypeContentQuery */, 536870912 /* DynamicQuery */, 0 /* CheckAndUpdate */);
    var callInit = shiftInitState(view, 256 /* InitState_CallingOnInit */, 512 /* InitState_CallingAfterContentInit */);
    callLifecycleHooksChildrenFirst(view, 2097152 /* AfterContentChecked */ | (callInit ? 1048576 /* AfterContentInit */ : 0));
    Services.updateRenderer(view, 0 /* CheckAndUpdate */);
    execComponentViewsAction(view, ViewAction.CheckAndUpdate);
    execQueriesAction(view, 134217728 /* TypeViewQuery */, 536870912 /* DynamicQuery */, 0 /* CheckAndUpdate */);
    callInit = shiftInitState(view, 512 /* InitState_CallingAfterContentInit */, 768 /* InitState_CallingAfterViewInit */);
    callLifecycleHooksChildrenFirst(view, 8388608 /* AfterViewChecked */ | (callInit ? 4194304 /* AfterViewInit */ : 0));
    if (view.def.flags & 2 /* OnPush */) {
        view.state &= ~8 /* ChecksEnabled */;
    }
    view.state &= ~(64 /* CheckProjectedViews */ | 32 /* CheckProjectedView */);
    shiftInitState(view, 768 /* InitState_CallingAfterViewInit */, 1024 /* InitState_AfterInit */);
}
export function checkAndUpdateNode(view, nodeDef, argStyle, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) {
    if (argStyle === 0 /* Inline */) {
        return checkAndUpdateNodeInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9);
    }
    else {
        return checkAndUpdateNodeDynamic(view, nodeDef, v0);
    }
}
function markProjectedViewsForCheck(view) {
    var def = view.def;
    if (!(def.nodeFlags & 4 /* ProjectedTemplate */)) {
        return;
    }
    for (var i = 0; i < def.nodes.length; i++) {
        var nodeDef = def.nodes[i];
        if (nodeDef.flags & 4 /* ProjectedTemplate */) {
            var projectedViews = asElementData(view, i).template._projectedViews;
            if (projectedViews) {
                for (var i_1 = 0; i_1 < projectedViews.length; i_1++) {
                    var projectedView = projectedViews[i_1];
                    projectedView.state |= 32 /* CheckProjectedView */;
                    markParentViewsForCheckProjectedViews(projectedView, view);
                }
            }
        }
        else if ((nodeDef.childFlags & 4 /* ProjectedTemplate */) === 0) {
            // a parent with leafs
            // no child is a component,
            // then skip the children
            i += nodeDef.childCount;
        }
    }
}
function checkAndUpdateNodeInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) {
    switch (nodeDef.flags & 201347067 /* Types */) {
        case 1 /* TypeElement */:
            return checkAndUpdateElementInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9);
        case 2 /* TypeText */:
            return checkAndUpdateTextInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9);
        case 16384 /* TypeDirective */:
            return checkAndUpdateDirectiveInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9);
        case 32 /* TypePureArray */:
        case 64 /* TypePureObject */:
        case 128 /* TypePurePipe */:
            return checkAndUpdatePureExpressionInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9);
        default:
            throw 'unreachable';
    }
}
function checkAndUpdateNodeDynamic(view, nodeDef, values) {
    switch (nodeDef.flags & 201347067 /* Types */) {
        case 1 /* TypeElement */:
            return checkAndUpdateElementDynamic(view, nodeDef, values);
        case 2 /* TypeText */:
            return checkAndUpdateTextDynamic(view, nodeDef, values);
        case 16384 /* TypeDirective */:
            return checkAndUpdateDirectiveDynamic(view, nodeDef, values);
        case 32 /* TypePureArray */:
        case 64 /* TypePureObject */:
        case 128 /* TypePurePipe */:
            return checkAndUpdatePureExpressionDynamic(view, nodeDef, values);
        default:
            throw 'unreachable';
    }
}
export function checkNoChangesNode(view, nodeDef, argStyle, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) {
    if (argStyle === 0 /* Inline */) {
        checkNoChangesNodeInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9);
    }
    else {
        checkNoChangesNodeDynamic(view, nodeDef, v0);
    }
    // Returning false is ok here as we would have thrown in case of a change.
    return false;
}
function checkNoChangesNodeInline(view, nodeDef, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) {
    var bindLen = nodeDef.bindings.length;
    if (bindLen > 0)
        checkBindingNoChanges(view, nodeDef, 0, v0);
    if (bindLen > 1)
        checkBindingNoChanges(view, nodeDef, 1, v1);
    if (bindLen > 2)
        checkBindingNoChanges(view, nodeDef, 2, v2);
    if (bindLen > 3)
        checkBindingNoChanges(view, nodeDef, 3, v3);
    if (bindLen > 4)
        checkBindingNoChanges(view, nodeDef, 4, v4);
    if (bindLen > 5)
        checkBindingNoChanges(view, nodeDef, 5, v5);
    if (bindLen > 6)
        checkBindingNoChanges(view, nodeDef, 6, v6);
    if (bindLen > 7)
        checkBindingNoChanges(view, nodeDef, 7, v7);
    if (bindLen > 8)
        checkBindingNoChanges(view, nodeDef, 8, v8);
    if (bindLen > 9)
        checkBindingNoChanges(view, nodeDef, 9, v9);
}
function checkNoChangesNodeDynamic(view, nodeDef, values) {
    for (var i = 0; i < values.length; i++) {
        checkBindingNoChanges(view, nodeDef, i, values[i]);
    }
}
/**
 * Workaround https://github.com/angular/tsickle/issues/497
 * @suppress {misplacedTypeAnnotation}
 */
function checkNoChangesQuery(view, nodeDef) {
    var queryList = asQueryList(view, nodeDef.nodeIndex);
    if (queryList.dirty) {
        throw expressionChangedAfterItHasBeenCheckedError(Services.createDebugContext(view, nodeDef.nodeIndex), "Query " + nodeDef.query.id + " not dirty", "Query " + nodeDef.query.id + " dirty", (view.state & 1 /* BeforeFirstCheck */) !== 0);
    }
}
export function destroyView(view) {
    if (view.state & 128 /* Destroyed */) {
        return;
    }
    execEmbeddedViewsAction(view, ViewAction.Destroy);
    execComponentViewsAction(view, ViewAction.Destroy);
    callLifecycleHooksChildrenFirst(view, 131072 /* OnDestroy */);
    if (view.disposables) {
        for (var i = 0; i < view.disposables.length; i++) {
            view.disposables[i]();
        }
    }
    detachProjectedView(view);
    if (view.renderer.destroyNode) {
        destroyViewNodes(view);
    }
    if (isComponentView(view)) {
        view.renderer.destroy();
    }
    view.state |= 128 /* Destroyed */;
}
function destroyViewNodes(view) {
    var len = view.def.nodes.length;
    for (var i = 0; i < len; i++) {
        var def = view.def.nodes[i];
        if (def.flags & 1 /* TypeElement */) {
            view.renderer.destroyNode(asElementData(view, i).renderElement);
        }
        else if (def.flags & 2 /* TypeText */) {
            view.renderer.destroyNode(asTextData(view, i).renderText);
        }
        else if (def.flags & 67108864 /* TypeContentQuery */ || def.flags & 134217728 /* TypeViewQuery */) {
            asQueryList(view, i).destroy();
        }
    }
}
var ViewAction;
(function (ViewAction) {
    ViewAction[ViewAction["CreateViewNodes"] = 0] = "CreateViewNodes";
    ViewAction[ViewAction["CheckNoChanges"] = 1] = "CheckNoChanges";
    ViewAction[ViewAction["CheckNoChangesProjectedViews"] = 2] = "CheckNoChangesProjectedViews";
    ViewAction[ViewAction["CheckAndUpdate"] = 3] = "CheckAndUpdate";
    ViewAction[ViewAction["CheckAndUpdateProjectedViews"] = 4] = "CheckAndUpdateProjectedViews";
    ViewAction[ViewAction["Destroy"] = 5] = "Destroy";
})(ViewAction || (ViewAction = {}));
function execComponentViewsAction(view, action) {
    var def = view.def;
    if (!(def.nodeFlags & 33554432 /* ComponentView */)) {
        return;
    }
    for (var i = 0; i < def.nodes.length; i++) {
        var nodeDef = def.nodes[i];
        if (nodeDef.flags & 33554432 /* ComponentView */) {
            // a leaf
            callViewAction(asElementData(view, i).componentView, action);
        }
        else if ((nodeDef.childFlags & 33554432 /* ComponentView */) === 0) {
            // a parent with leafs
            // no child is a component,
            // then skip the children
            i += nodeDef.childCount;
        }
    }
}
function execEmbeddedViewsAction(view, action) {
    var def = view.def;
    if (!(def.nodeFlags & 16777216 /* EmbeddedViews */)) {
        return;
    }
    for (var i = 0; i < def.nodes.length; i++) {
        var nodeDef = def.nodes[i];
        if (nodeDef.flags & 16777216 /* EmbeddedViews */) {
            // a leaf
            var embeddedViews = asElementData(view, i).viewContainer._embeddedViews;
            for (var k = 0; k < embeddedViews.length; k++) {
                callViewAction(embeddedViews[k], action);
            }
        }
        else if ((nodeDef.childFlags & 16777216 /* EmbeddedViews */) === 0) {
            // a parent with leafs
            // no child is a component,
            // then skip the children
            i += nodeDef.childCount;
        }
    }
}
function callViewAction(view, action) {
    var viewState = view.state;
    switch (action) {
        case ViewAction.CheckNoChanges:
            if ((viewState & 128 /* Destroyed */) === 0) {
                if ((viewState & 12 /* CatDetectChanges */) === 12 /* CatDetectChanges */) {
                    checkNoChangesView(view);
                }
                else if (viewState & 64 /* CheckProjectedViews */) {
                    execProjectedViewsAction(view, ViewAction.CheckNoChangesProjectedViews);
                }
            }
            break;
        case ViewAction.CheckNoChangesProjectedViews:
            if ((viewState & 128 /* Destroyed */) === 0) {
                if (viewState & 32 /* CheckProjectedView */) {
                    checkNoChangesView(view);
                }
                else if (viewState & 64 /* CheckProjectedViews */) {
                    execProjectedViewsAction(view, action);
                }
            }
            break;
        case ViewAction.CheckAndUpdate:
            if ((viewState & 128 /* Destroyed */) === 0) {
                if ((viewState & 12 /* CatDetectChanges */) === 12 /* CatDetectChanges */) {
                    checkAndUpdateView(view);
                }
                else if (viewState & 64 /* CheckProjectedViews */) {
                    execProjectedViewsAction(view, ViewAction.CheckAndUpdateProjectedViews);
                }
            }
            break;
        case ViewAction.CheckAndUpdateProjectedViews:
            if ((viewState & 128 /* Destroyed */) === 0) {
                if (viewState & 32 /* CheckProjectedView */) {
                    checkAndUpdateView(view);
                }
                else if (viewState & 64 /* CheckProjectedViews */) {
                    execProjectedViewsAction(view, action);
                }
            }
            break;
        case ViewAction.Destroy:
            // Note: destroyView recurses over all views,
            // so we don't need to special case projected views here.
            destroyView(view);
            break;
        case ViewAction.CreateViewNodes:
            createViewNodes(view);
            break;
    }
}
function execProjectedViewsAction(view, action) {
    execEmbeddedViewsAction(view, action);
    execComponentViewsAction(view, action);
}
function execQueriesAction(view, queryFlags, staticDynamicQueryFlag, checkType) {
    if (!(view.def.nodeFlags & queryFlags) || !(view.def.nodeFlags & staticDynamicQueryFlag)) {
        return;
    }
    var nodeCount = view.def.nodes.length;
    for (var i = 0; i < nodeCount; i++) {
        var nodeDef = view.def.nodes[i];
        if ((nodeDef.flags & queryFlags) && (nodeDef.flags & staticDynamicQueryFlag)) {
            Services.setCurrentNode(view, nodeDef.nodeIndex);
            switch (checkType) {
                case 0 /* CheckAndUpdate */:
                    checkAndUpdateQuery(view, nodeDef);
                    break;
                case 1 /* CheckNoChanges */:
                    checkNoChangesQuery(view, nodeDef);
                    break;
            }
        }
        if (!(nodeDef.childFlags & queryFlags) || !(nodeDef.childFlags & staticDynamicQueryFlag)) {
            // no child has a matching query
            // then skip the children
            i += nodeDef.childCount;
        }
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlldy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvdmlldy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFJSCxPQUFPLEVBQUMsNEJBQTRCLEVBQUUsMkJBQTJCLEVBQUUsYUFBYSxFQUFFLHNCQUFzQixFQUFDLE1BQU0sV0FBVyxDQUFDO0FBQzNILE9BQU8sRUFBQywyQ0FBMkMsRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUNyRSxPQUFPLEVBQUMsZUFBZSxFQUFDLE1BQU0sY0FBYyxDQUFDO0FBQzdDLE9BQU8sRUFBQywrQkFBK0IsRUFBRSw4QkFBOEIsRUFBRSw2QkFBNkIsRUFBRSx1QkFBdUIsRUFBRSxrQkFBa0IsRUFBRSxzQkFBc0IsRUFBQyxNQUFNLFlBQVksQ0FBQztBQUMvTCxPQUFPLEVBQUMsbUNBQW1DLEVBQUUsa0NBQWtDLEVBQUUsb0JBQW9CLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUNoSSxPQUFPLEVBQUMsbUJBQW1CLEVBQUUsV0FBVyxFQUFDLE1BQU0sU0FBUyxDQUFDO0FBQ3pELE9BQU8sRUFBQyxrQkFBa0IsRUFBRSx1QkFBdUIsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUNuRSxPQUFPLEVBQUMseUJBQXlCLEVBQUUsd0JBQXdCLEVBQUUsVUFBVSxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBQ3ZGLE9BQU8sRUFBNkYsUUFBUSxFQUFtRixhQUFhLEVBQUUsV0FBVyxFQUFFLFVBQVUsRUFBRSxjQUFjLEVBQUMsTUFBTSxTQUFTLENBQUM7QUFDdFEsT0FBTyxFQUFDLElBQUksRUFBRSxxQkFBcUIsRUFBRSxlQUFlLEVBQUUscUNBQXFDLEVBQUUsaUJBQWlCLEVBQUUsUUFBUSxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBQ3hJLE9BQU8sRUFBQyxtQkFBbUIsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUVsRCxNQUFNLFVB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QUFTLEdBQUcsVUFBVSxDQUFDLElBQUksQ0FBQyxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsU0FBUyxHQUFHLHNCQUFzQixDQUFDLEVBQUU7UUFDeEYsT0FBTztLQUNSO0lBQ0QsSUFBTSxTQUFTLEdBQUcsSUFBSSxDQUFDLEdBQUcsQ0FBQyxLQUFLLENBQUMsTUFBTSxDQUFDO0lBQ3hDLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxTQUFTLEVBQUUsQ0FBQyxFQUFFLEVBQUU7UUFDbEMsSUFBTSxPQUFPLEdBQUcsSUFBSSxDQUFDLEdBQUcsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDbEMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxLQUFLLEdBQUcsVUFBVSxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsS0FBSyxHQUFHLHNCQUFzQixDQUFDLEVBQUU7WUFDNUUsUUFBUSxDQUFDLGNBQWMsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLFNBQVMsQ0FBQyxDQUFDO1lBQ2pELFFBQVEsU0FBUyxFQUFFO2dCQUNqQjtvQkFDRSxtQkFBbUIsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLENBQUM7b0JBQ25DLE1BQU07Z0JBQ1I7b0JBQ0UsbUJBQW1CLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxDQUFDO29CQUNuQyxNQUFNO2FBQ1Q7U0FDRjtRQUNELElBQUksQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFVLEdBQUcsVUFBVSxDQUFDLElBQUksQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFVLEdBQUcsc0JBQXNCLENBQUMsRUFBRTtZQUN4RixnQ0FBZ0M7WUFDaEMseUJBQXlCO1lBQ3pCLENBQUMsSUFBSSxPQUFPLENBQUMsVUFBVSxDQUFDO1NBQ3pCO0tBQ0Y7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1JlbmRlcmVyMn0gZnJvbSAnLi4vcmVuZGVyL2FwaSc7XG5cbmltcG9ydCB7Y2hlY2tBbmRVcGRhdGVFbGVtZW50RHluYW1pYywgY2hlY2tBbmRVcGRhdGVFbGVtZW50SW5saW5lLCBjcmVhdGVFbGVtZW50LCBsaXN0ZW5Ub0VsZW1lbnRPdXRwdXRzfSBmcm9tICcuL2VsZW1lbnQnO1xuaW1wb3J0IHtleHByZXNzaW9uQ2hhbmdlZEFmdGVySXRIYXNCZWVuQ2hlY2tlZEVycm9yfSBmcm9tICcuL2Vycm9ycyc7XG5pbXBvcnQge2FwcGVuZE5nQ29udGVudH0gZnJvbSAnLi9uZ19jb250ZW50JztcbmltcG9ydCB7Y2FsbExpZmVjeWNsZUhvb2tzQ2hpbGRyZW5GaXJzdCwgY2hlY2tBbmRVcGRhdGVEaXJlY3RpdmVEeW5hbWljLCBjaGVja0FuZFVwZGF0ZURpcmVjdGl2ZUlubGluZSwgY3JlYXRlRGlyZWN0aXZlSW5zdGFuY2UsIGNyZWF0ZVBpcGVJbnN0YW5jZSwgY3JlYXRlUHJvdmlkZXJJbnN0YW5jZX0gZnJvbSAnLi9wcm92aWRlcic7XG5pbXBvcnQge2NoZWNrQW5kVXBkYXRlUHVyZUV4cHJlc3Npb25EeW5hbWljLCBjaGVja0FuZFVwZGF0ZVB1cmVFeHByZXNzaW9uSW5saW5lLCBjcmVhdGVQdXJlRXhwcmVzc2lvbn0gZnJvbSAnLi9wdXJlX2V4cHJlc3Npb24nO1xuaW1wb3J0IHtjaGVja0FuZFVwZGF0ZVF1ZXJ5LCBjcmVhdGVRdWVyeX0gZnJvbSAnLi9xdWVyeSc7XG5pbXBvcnQge2NyZWF0ZVRlbXBsYXRlRGF0YSwgY3JlYXRlVmlld0NvbnRhaW5lckRhdGF9IGZyb20gJy4vcmVmcyc7XG5pbXBvcnQge2NoZWNrQW5kVXBkYXRlVGV4dER5bmFtaWMsIGNoZWNrQW5kVXBkYXRlVGV4dElubGluZSwgY3JlYXRlVGV4dH0gZnJvbSAnLi90ZXh0JztcbmltcG9ydCB7QXJndW1lbnRUeXBlLCBDaGVja1R5cGUsIEVsZW1lbnREYXRhLCBOb2RlRGF0YSwgTm9kZURlZiwgTm9kZUZsYWdzLCBQcm92aWRlckRhdGEsIFJvb3REYXRhLCBTZXJ2aWNlcywgVmlld0RhdGEsIFZpZXdEZWZpbml0aW9uLCBWaWV3RmxhZ3MsIFZpZXdIYW5kbGVFdmVudEZuLCBWaWV3U3RhdGUsIFZpZXdVcGRhdGVGbiwgYXNFbGVtZW50RGF0YSwgYXNRdWVyeUxpc3QsIGFzVGV4dERhdGEsIHNoaWZ0SW5pdFN0YXRlfSBmcm9tICcuL3R5cGVzJztcbmltcG9ydCB7Tk9PUCwgY2hlY2tCaW5kaW5nTm9DaGFuZ2VzLCBpc0NvbXBvbmVudFZpZXcsIG1hcmtQYXJlbnRWaWV3c0ZvckNoZWNrUHJvamVjdGVkVmlld3MsIHJlc29sdmVEZWZpbml0aW9uLCB0b2tlbktleX0gZnJvbSAnLi91dGlsJztcbmltcG9ydCB7ZGV0YWNoUHJvamVjdGVkVmlld30gZnJvbSAnLi92aWV3X2F0dGFjaCc7XG5cbmV4cG9ydCBmdW5jdGlvbiB2aWV3RGVmKFxuICAgIGZsYWdzOiBWaWV3RmxhZ3MsIG5vZGVzOiBOb2RlRGVmW10sIHVwZGF0ZURpcmVjdGl2ZXM/OiBudWxsIHwgVmlld1VwZGF0ZUZuLFxuICAgIHVwZGF0ZVJlbmRlcmVyPzogbnVsbCB8IFZpZXdVcGRhdGVGbik6IFZpZXdEZWZpbml0aW9uIHtcbiAgLy8gY2xvbmUgbm9kZXMgYW5kIHNldCBhdXRvIGNhbGN1bGF0ZWQgdmFsdWVzXG4gIGxldCB2aWV3QmluZGluZ0NvdW50ID0gMDtcbiAgbGV0IHZpZXdEaXNwb3NhYmxlQ291bnQgPSAwO1xuICBsZXQgdmlld05vZGVGbGFncyA9IDA7XG4gIGxldCB2aWV3Um9vdE5vZGVGbGFncyA9IDA7XG4gIGxldCB2aWV3TWF0Y2hlZFF1ZXJpZXMgPSAwO1xuICBsZXQgY3VycmVudFBhcmVudDogTm9kZURlZnxudWxsID0gbnVsbDtcbiAgbGV0IGN1cnJlbnRSZW5kZXJQYXJlbnQ6IE5vZGVEZWZ8bnVsbCA9IG51bGw7XG4gIGxldCBjdXJyZW50RWxlbWVudEhhc1B1YmxpY1Byb3ZpZGVycyA9IGZhbHNlO1xuICBsZXQgY3VycmVudEVsZW1lbnRIYXNQcml2YXRlUHJvdmlkZXJzID0gZmFsc2U7XG4gIGxldCBsYXN0UmVuZGVyUm9vdE5vZGU6IE5vZGVEZWZ8bnVsbCA9IG51bGw7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgbm9kZXMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBub2RlID0gbm9kZXNbaV07XG4gICAgbm9kZS5ub2RlSW5kZXggPSBpO1xuICAgIG5vZGUucGFyZW50ID0gY3VycmVudFBhcmVudDtcbiAgICBub2RlLmJpbmRpbmdJbmRleCA9IHZpZXdCaW5kaW5nQ291bnQ7XG4gICAgbm9kZS5vdXRwdXRJbmRleCA9IHZpZXdEaXNwb3NhYmxlQ291bnQ7XG4gICAgbm9kZS5yZW5kZXJQYXJlbnQgPSBjdXJyZW50UmVuZGVyUGFyZW50O1xuXG4gICAgdmlld05vZGVGbGFncyB8PSBub2RlLmZsYWdzO1xuICAgIHZpZXdNYXRjaGVkUXVlcmllcyB8PSBub2RlLm1hdGNoZWRRdWVyeUlkcztcblxuICAgIGlmIChub2RlLmVsZW1lbnQpIHtcbiAgICAgIGNvbnN0IGVsRGVmID0gbm9kZS5lbGVtZW50O1xuICAgICAgZWxEZWYucHVibGljUHJvdmlkZXJzID1cbiAgICAgICAgICBjdXJyZW50UGFyZW50ID8gY3VycmVudFBhcmVudC5lbGVtZW50ICEucHVibGljUHJvdmlkZXJzIDogT2JqZWN0LmNyZWF0ZShudWxsKTtcbiAgICAgIGVsRGVmLmFsbFByb3ZpZGVycyA9IGVsRGVmLnB1YmxpY1Byb3ZpZGVycztcbiAgICAgIC8vIE5vdGU6IFdlIGFzc3VtZSB0aGF0IGFsbCBwcm92aWRlcnMgb2YgYW4gZWxlbWVudCBhcmUgYmVmb3JlIGFueSBjaGlsZCBlbGVtZW50IVxuICAgICAgY3VycmVudEVsZW1lbnRIYXNQdWJsaWNQcm92aWRlcnMgPSBmYWxzZTtcbiAgICAgIGN1cnJlbnRFbGVtZW50SGFzUHJpdmF0ZVByb3ZpZGVycyA9IGZhbHNlO1xuXG4gICAgICBpZiAobm9kZS5lbGVtZW50LnRlbXBsYXRlKSB7XG4gICAgICAgIHZpZXdNYXRjaGVkUXVlcmllcyB8PSBub2RlLmVsZW1lbnQudGVtcGxhdGUubm9kZU1hdGNoZWRRdWVyaWVzO1xuICAgICAgfVxuICAgIH1cbiAgICB2YWxpZGF0ZU5vZGUoY3VycmVudFBhcmVudCwgbm9kZSwgbm9kZXMubGVuZ3RoKTtcblxuXG4gICAgdmlld0JpbmRpbmdDb3VudCArPSBub2RlLmJpbmRpbmdzLmxlbmd0aDtcbiAgICB2aWV3RGlzcG9zYWJsZUNvdW50ICs9IG5vZGUub3V0cHV0cy5sZW5ndGg7XG5cbiAgICBpZiAoIWN1cnJlbnRSZW5kZXJQYXJlbnQgJiYgKG5vZGUuZmxhZ3MgJiBOb2RlRmxhZ3MuQ2F0UmVuZGVyTm9kZSkpIHtcbiAgICAgIGxhc3RSZW5kZXJSb290Tm9kZSA9IG5vZGU7XG4gICAgfVxuXG4gICAgaWYgKG5vZGUuZmxhZ3MgJiBOb2RlRmxhZ3MuQ2F0UHJvdmlkZXIpIHtcbiAgICAgIGlmICghY3VycmVudEVsZW1lbnRIYXNQdWJsaWNQcm92aWRlcnMpIHtcbiAgICAgICAgY3VycmVudEVsZW1lbnRIYXNQdWJsaWNQcm92aWRlcnMgPSB0cnVlO1xuICAgICAgICAvLyBVc2UgcHJvdG90eXBpY2FsIGluaGVyaXRhbmNlIHRvIG5vdCBnZXQgTyhuXjIpIGNvbXBsZXhpdHkuLi5cbiAgICAgICAgY3VycmVudFBhcmVudCAhLmVsZW1lbnQgIS5wdWJsaWNQcm92aWRlcnMgPVxuICAgICAgICAgICAgT2JqZWN0LmNyZWF0ZShjdXJyZW50UGFyZW50ICEuZWxlbWVudCAhLnB1YmxpY1Byb3ZpZGVycyk7XG4gICAgICAgIGN1cnJlbnRQYXJlbnQgIS5lbGVtZW50ICEuYWxsUHJvdmlkZXJzID0gY3VycmVudFBhcmVudCAhLmVsZW1lbnQgIS5wdWJsaWNQcm92aWRlcnM7XG4gICAgICB9XG4gICAgICBjb25zdCBpc1ByaXZhdGVTZXJ2aWNlID0gKG5vZGUuZmxhZ3MgJiBOb2RlRmxhZ3MuUHJpdmF0ZVByb3ZpZGVyKSAhPT0gMDtcbiAgICAgIGNvbnN0IGlzQ29tcG9uZW50ID0gKG5vZGUuZmxhZ3MgJiBOb2RlRmxhZ3MuQ29tcG9uZW50KSAhPT0gMDtcbiAgICAgIGlmICghaXNQcml2YXRlU2VydmljZSB8fCBpc0NvbXBvbmVudCkge1xuICAgICAgICBjdXJyZW50UGFyZW50ICEuZWxlbWVudCAhLnB1YmxpY1Byb3ZpZGVycyAhW3Rva2VuS2V5KG5vZGUucHJvdmlkZXIgIS50b2tlbildID0gbm9kZTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGlmICghY3VycmVudEVsZW1lbnRIYXNQcml2YXRlUHJvdmlkZXJzKSB7XG4gICAgICAgICAgY3VycmVudEVsZW1lbnRIYXNQcml2YXRlUHJvdmlkZXJzID0gdHJ1ZTtcbiAgICAgICAgICAvLyBVc2UgcHJvdG90eXBpY2FsIGluaGVyaXRhbmNlIHRvIG5vdCBnZXQgTyhuXjIpIGNvbXBsZXhpdHkuLi5cbiAgICAgICAgICBjdXJyZW50UGFyZW50ICEuZWxlbWVudCAhLmFsbFByb3ZpZGVycyA9XG4gICAgICAgICAgICAgIE9iamVjdC5jcmVhdGUoY3VycmVudFBhcmVudCAhLmVsZW1lbnQgIS5wdWJsaWNQcm92aWRlcnMpO1xuICAgICAgICB9XG4gICAgICAgIGN1cnJlbnRQYXJlbnQgIS5lbGVtZW50ICEuYWxsUHJvdmlkZXJzICFbdG9rZW5LZXkobm9kZS5wcm92aWRlciAhLnRva2VuKV0gPSBub2RlO1xuICAgICAgfVxuICAgICAgaWYgKGlzQ29tcG9uZW50KSB7XG4gICAgICAgIGN1cnJlbnRQYXJlbnQgIS5lbGVtZW50ICEuY29tcG9uZW50UHJvdmlkZXIgPSBub2RlO1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChjdXJyZW50UGFyZW50KSB7XG4gICAgICBjdXJyZW50UGFyZW50LmNoaWxkRmxhZ3MgfD0gbm9kZS5mbGFncztcbiAgICAgIGN1cnJlbnRQYXJlbnQuZGlyZWN0Q2hpbGRGbGFncyB8PSBub2RlLmZsYWdzO1xuICAgICAgY3VycmVudFBhcmVudC5jaGlsZE1hdGNoZWRRdWVyaWVzIHw9IG5vZGUubWF0Y2hlZFF1ZXJ5SWRzO1xuICAgICAgaWYgKG5vZGUuZWxlbWVudCAmJiBub2RlLmVsZW1lbnQudGVtcGxhdGUpIHtcbiAgICAgICAgY3VycmVudFBhcmVudC5jaGlsZE1hdGNoZWRRdWVyaWVzIHw9IG5vZGUuZWxlbWVudC50ZW1wbGF0ZS5ub2RlTWF0Y2hlZFF1ZXJpZXM7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIHZpZXdSb290Tm9kZUZsYWdzIHw9IG5vZGUuZmxhZ3M7XG4gICAgfVxuXG4gICAgaWYgKG5vZGUuY2hpbGRDb3VudCA+IDApIHtcbiAgICAgIGN1cnJlbnRQYXJlbnQgPSBub2RlO1xuXG4gICAgICBpZiAoIWlzTmdDb250YWluZXIobm9kZSkpIHtcbiAgICAgICAgY3VycmVudFJlbmRlclBhcmVudCA9IG5vZGU7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIFdoZW4gdGhlIGN1cnJlbnQgbm9kZSBoYXMgbm8gY2hpbGRyZW4sIGNoZWNrIGlmIGl0IGlzIHRoZSBsYXN0IGNoaWxkcmVuIG9mIGl0cyBwYXJlbnQuXG4gICAgICAvLyBXaGVuIGl0IGlzLCBwcm9wYWdhdGUgdGhlIGZsYWdzIHVwLlxuICAgICAgLy8gVGhlIGxvb3AgaXMgcmVxdWlyZWQgYmVjYXVzZSBhbiBlbGVtZW50IGNvdWxkIGJlIHRoZSBsYXN0IHRyYW5zaXRpdmUgY2hpbGRyZW4gb2Ygc2V2ZXJhbFxuICAgICAgLy8gZWxlbWVudHMuIFdlIGxvb3AgdG8gZWl0aGVyIHRoZSByb290IG9yIHRoZSBoaWdoZXN0IG9wZW5lZCBlbGVtZW50ICg9IHdpdGggcmVtYWluaW5nXG4gICAgICAvLyBjaGlsZHJlbilcbiAgICAgIHdoaWxlIChjdXJyZW50UGFyZW50ICYmIGkgPT09IGN1cnJlbnRQYXJlbnQubm9kZUluZGV4ICsgY3VycmVudFBhcmVudC5jaGlsZENvdW50KSB7XG4gICAgICAgIGNvbnN0IG5ld1BhcmVudDogTm9kZURlZnxudWxsID0gY3VycmVudFBhcmVudC5wYXJlbnQ7XG4gICAgICAgIGlmIChuZXdQYXJlbnQpIHtcbiAgICAgICAgICBuZXdQYXJlbnQuY2hpbGRGbGFncyB8PSBjdXJyZW50UGFyZW50LmNoaWxkRmxhZ3M7XG4gICAgICAgICAgbmV3UGFyZW50LmNoaWxkTWF0Y2hlZFF1ZXJpZXMgfD0gY3VycmVudFBhcmVudC5jaGlsZE1hdGNoZWRRdWVyaWVzO1xuICAgICAgICB9XG4gICAgICAgIGN1cnJlbnRQYXJlbnQgPSBuZXdQYXJlbnQ7XG4gICAgICAgIC8vIFdlIGFsc28gbmVlZCB0byB1cGRhdGUgdGhlIHJlbmRlciBwYXJlbnQgJiBhY2NvdW50IGZvciBuZy1jb250YWluZXJcbiAgICAgICAgaWYgKGN1cnJlbnRQYXJlbnQgJiYgaXNOZ0NvbnRhaW5lcihjdXJyZW50UGFyZW50KSkge1xuICAgICAgICAgIGN1cnJlbnRSZW5kZXJQYXJlbnQgPSBjdXJyZW50UGFyZW50LnJlbmRlclBhcmVudDtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjdXJyZW50UmVuZGVyUGFyZW50ID0gY3VycmVudFBhcmVudDtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIGNvbnN0IGhhbmRsZUV2ZW50OiBWaWV3SGFuZGxlRXZlbnRGbiA9ICh2aWV3LCBub2RlSW5kZXgsIGV2ZW50TmFtZSwgZXZlbnQpID0+XG4gICAgICBub2Rlc1tub2RlSW5kZXhdLmVsZW1lbnQgIS5oYW5kbGVFdmVudCAhKHZpZXcsIGV2ZW50TmFtZSwgZXZlbnQpO1xuXG4gIHJldHVybiB7XG4gICAgLy8gV2lsbCBiZSBmaWxsZWQgbGF0ZXIuLi5cbiAgICBmYWN0b3J5OiBudWxsLFxuICAgIG5vZGVGbGFnczogdmlld05vZGVGbGFncyxcbiAgICByb290Tm9kZUZsYWdzOiB2aWV3Um9vdE5vZGVGbGFncyxcbiAgICBub2RlTWF0Y2hlZFF1ZXJpZXM6IHZpZXdNYXRjaGVkUXVlcmllcywgZmxhZ3MsXG4gICAgbm9kZXM6IG5vZGVzLFxuICAgIHVwZGF0ZURpcmVjdGl2ZXM6IHVwZGF0ZURpcmVjdGl2ZXMgfHwgTk9PUCxcbiAgICB1cGRhdGVSZW5kZXJlcjogdXBkYXRlUmVuZGVyZXIgfHwgTk9PUCwgaGFuZGxlRXZlbnQsXG4gICAgYmluZGluZ0NvdW50OiB2aWV3QmluZGluZ0NvdW50LFxuICAgIG91dHB1dENvdW50OiB2aWV3RGlzcG9zYWJsZUNvdW50LCBsYXN0UmVuZGVyUm9vdE5vZGVcbiAgfTtcbn1cblxuZnVuY3Rpb24gaXNOZ0NvbnRhaW5lcihub2RlOiBOb2RlRGVmKTogYm9vbGVhbiB7XG4gIHJldHVybiAobm9kZS5mbGFncyAmIE5vZGVGbGFncy5UeXBlRWxlbWVudCkgIT09IDAgJiYgbm9kZS5lbGVtZW50ICEubmFtZSA9PT0gbnVsbDtcbn1cblxuZnVuY3Rpb24gdmFsaWRhdGVOb2RlKHBhcmVudDogTm9kZURlZiB8IG51bGwsIG5vZGU6IE5vZGVEZWYsIG5vZGVDb3VudDogbnVtYmVyKSB7XG4gIGNvbnN0IHRlbXBsYXRlID0gbm9kZS5lbGVtZW50ICYmIG5vZGUuZWxlbWVudC50ZW1wbGF0ZTtcbiAgaWYgKHRlbXBsYXRlKSB7XG4gICAgaWYgKCF0ZW1wbGF0ZS5sYXN0UmVuZGVyUm9vdE5vZGUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgSWxsZWdhbCBTdGF0ZTogRW1iZWRkZWQgdGVtcGxhdGVzIHdpdGhvdXQgbm9kZXMgYXJlIG5vdCBhbGxvd2VkIWApO1xuICAgIH1cbiAgICBpZiAodGVtcGxhdGUubGFzdFJlbmRlclJvb3ROb2RlICYmXG4gICAgICAgIHRlbXBsYXRlLmxhc3RSZW5kZXJSb290Tm9kZS5mbGFncyAmIE5vZGVGbGFncy5FbWJlZGRlZFZpZXdzKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgYElsbGVnYWwgU3RhdGU6IExhc3Qgcm9vdCBub2RlIG9mIGEgdGVtcGxhdGUgY2FuJ3QgaGF2ZSBlbWJlZGRlZCB2aWV3cywgYXQgaW5kZXggJHtub2RlLm5vZGVJbmRleH0hYCk7XG4gICAgfVxuICB9XG4gIGlmIChub2RlLmZsYWdzICYgTm9kZUZsYWdzLkNhdFByb3ZpZGVyKSB7XG4gICAgY29uc3QgcGFyZW50RmxhZ3MgPSBwYXJlbnQgPyBwYXJlbnQuZmxhZ3MgOiAwO1xuICAgIGlmICgocGFyZW50RmxhZ3MgJiBOb2RlRmxhZ3MuVHlwZUVsZW1lbnQpID09PSAwKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgYElsbGVnYWwgU3RhdGU6IFN0YXRpY1Byb3ZpZGVyL0RpcmVjdGl2ZSBub2RlcyBuZWVkIHRvIGJlIGNoaWxkcmVuIG9mIGVsZW1lbnRzIG9yIGFuY2hvcnMsIGF0IGluZGV4ICR7bm9kZS5ub2RlSW5kZXh9IWApO1xuICAgIH1cbiAgfVxuICBpZiAobm9kZS5xdWVyeSkge1xuICAgIGlmIChub2RlLmZsYWdzICYgTm9kZUZsYWdzLlR5cGVDb250ZW50UXVlcnkgJiZcbiAgICAgICAgKCFwYXJlbnQgfHwgKHBhcmVudC5mbGFncyAmIE5vZGVGbGFncy5UeXBlRGlyZWN0aXZlKSA9PT0gMCkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgSWxsZWdhbCBTdGF0ZTogQ29udGVudCBRdWVyeSBub2RlcyBuZWVkIHRvIGJlIGNoaWxkcmVuIG9mIGRpcmVjdGl2ZXMsIGF0IGluZGV4ICR7bm9kZS5ub2RlSW5kZXh9IWApO1xuICAgIH1cbiAgICBpZiAobm9kZS5mbGFncyAmIE5vZGVGbGFncy5UeXBlVmlld1F1ZXJ5ICYmIHBhcmVudCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBJbGxlZ2FsIFN0YXRlOiBWaWV3IFF1ZXJ5IG5vZGVzIGhhdmUgdG8gYmUgdG9wIGxldmVsIG5vZGVzLCBhdCBpbmRleCAke25vZGUubm9kZUluZGV4fSFgKTtcbiAgICB9XG4gIH1cbiAgaWYgKG5vZGUuY2hpbGRDb3VudCkge1xuICAgIGNvbnN0IHBhcmVudEVuZCA9IHBhcmVudCA/IHBhcmVudC5ub2RlSW5kZXggKyBwYXJlbnQuY2hpbGRDb3VudCA6IG5vZGVDb3VudCAtIDE7XG4gICAgaWYgKG5vZGUubm9kZUluZGV4IDw9IHBhcmVudEVuZCAmJiBub2RlLm5vZGVJbmRleCArIG5vZGUuY2hpbGRDb3VudCA+IHBhcmVudEVuZCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICAgIGBJbGxlZ2FsIFN0YXRlOiBjaGlsZENvdW50IG9mIG5vZGUgbGVhZHMgb3V0c2lkZSBvZiBwYXJlbnQsIGF0IGluZGV4ICR7bm9kZS5ub2RlSW5kZXh9IWApO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlRW1iZWRkZWRWaWV3KFxuICAgIHBhcmVudDogVmlld0RhdGEsIGFuY2hvckRlZjogTm9kZURlZiwgdmlld0RlZjogVmlld0RlZmluaXRpb24sIGNvbnRleHQ/OiBhbnkpOiBWaWV3RGF0YSB7XG4gIC8vIGVtYmVkZGVkIHZpZXdzIGFyZSBzZWVuIGFzIHNpYmxpbmdzIHRvIHRoZSBhbmNob3IsIHNvIHdlIG5lZWRcbiAgLy8gdG8gZ2V0IHRoZSBwYXJlbnQgb2YgdGhlIGFuY2hvciBhbmQgdXNlIGl0IGFzIHBhcmVudEluZGV4LlxuICBjb25zdCB2aWV3ID0gY3JlYXRlVmlldyhwYXJlbnQucm9vdCwgcGFyZW50LnJlbmRlcmVyLCBwYXJlbnQsIGFuY2hvckRlZiwgdmlld0RlZik7XG4gIGluaXRWaWV3KHZpZXcsIHBhcmVudC5jb21wb25lbnQsIGNvbnRleHQpO1xuICBjcmVhdGVWaWV3Tm9kZXModmlldyk7XG4gIHJldHVybiB2aWV3O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlUm9vdFZpZXcocm9vdDogUm9vdERhdGEsIGRlZjogVmlld0RlZmluaXRpb24sIGNvbnRleHQ/OiBhbnkpOiBWaWV3RGF0YSB7XG4gIGNvbnN0IHZpZXcgPSBjcmVhdGVWaWV3KHJvb3QsIHJvb3QucmVuZGVyZXIsIG51bGwsIG51bGwsIGRlZik7XG4gIGluaXRWaWV3KHZpZXcsIGNvbnRleHQsIGNvbnRleHQpO1xuICBjcmVhdGVWaWV3Tm9kZXModmlldyk7XG4gIHJldHVybiB2aWV3O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlQ29tcG9uZW50VmlldyhcbiAgICBwYXJlbnRWaWV3OiBWaWV3RGF0YSwgbm9kZURlZjogTm9kZURlZiwgdmlld0RlZjogVmlld0RlZmluaXRpb24sIGhvc3RFbGVtZW50OiBhbnkpOiBWaWV3RGF0YSB7XG4gIGNvbnN0IHJlbmRlcmVyVHlwZSA9IG5vZGVEZWYuZWxlbWVudCAhLmNvbXBvbmVudFJlbmRlcmVyVHlwZTtcbiAgbGV0IGNvbXBSZW5kZXJlcjogUmVuZGVyZXIyO1xuICBpZiAoIXJlbmRlcmVyVHlwZSkge1xuICAgIGNvbXBSZW5kZXJlciA9IHBhcmVudFZpZXcucm9vdC5yZW5kZXJlcjtcbiAgfSBlbHNlIHtcbiAgICBjb21wUmVuZGVyZXIgPSBwYXJlbnRWaWV3LnJvb3QucmVuZGVyZXJGYWN0b3J5LmNyZWF0ZVJlbmRlcmVyKGhvc3RFbGVtZW50LCByZW5kZXJlclR5cGUpO1xuICB9XG4gIHJldHVybiBjcmVhdGVWaWV3KFxuICAgICAgcGFyZW50Vmlldy5yb290LCBjb21wUmVuZGVyZXIsIHBhcmVudFZpZXcsIG5vZGVEZWYuZWxlbWVudCAhLmNvbXBvbmVudFByb3ZpZGVyLCB2aWV3RGVmKTtcbn1cblxuZnVuY3Rpb24gY3JlYXRlVmlldyhcbiAgICByb290OiBSb290RGF0YSwgcmVuZGVyZXI6IFJlbmRlcmVyMiwgcGFyZW50OiBWaWV3RGF0YSB8IG51bGwsIHBhcmVudE5vZGVEZWY6IE5vZGVEZWYgfCBudWxsLFxuICAgIGRlZjogVmlld0RlZmluaXRpb24pOiBWaWV3RGF0YSB7XG4gIGNvbnN0IG5vZGVzOiBOb2RlRGF0YVtdID0gbmV3IEFycmF5KGRlZi5ub2Rlcy5sZW5ndGgpO1xuICBjb25zdCBkaXNwb3NhYmxlcyA9IGRlZi5vdXRwdXRDb3VudCA/IG5ldyBBcnJheShkZWYub3V0cHV0Q291bnQpIDogbnVsbDtcbiAgY29uc3QgdmlldzogVmlld0RhdGEgPSB7XG4gICAgZGVmLFxuICAgIHBhcmVudCxcbiAgICB2aWV3Q29udGFpbmVyUGFyZW50OiBudWxsLCBwYXJlbnROb2RlRGVmLFxuICAgIGNvbnRleHQ6IG51bGwsXG4gICAgY29tcG9uZW50OiBudWxsLCBub2RlcyxcbiAgICBzdGF0ZTogVmlld1N0YXRlLkNhdEluaXQsIHJvb3QsIHJlbmRlcmVyLFxuICAgIG9sZFZhbHVlczogbmV3IEFycmF5KGRlZi5iaW5kaW5nQ291bnQpLCBkaXNwb3NhYmxlcyxcbiAgICBpbml0SW5kZXg6IC0xXG4gIH07XG4gIHJldHVybiB2aWV3O1xufVxuXG5mdW5jdGlvbiBpbml0Vmlldyh2aWV3OiBWaWV3RGF0YSwgY29tcG9uZW50OiBhbnksIGNvbnRleHQ6IGFueSkge1xuICB2aWV3LmNvbXBvbmVudCA9IGNvbXBvbmVudDtcbiAgdmlldy5jb250ZXh0ID0gY29udGV4dDtcbn1cblxuZnVuY3Rpb24gY3JlYXRlVmlld05vZGVzKHZpZXc6IFZpZXdEYXRhKSB7XG4gIGxldCByZW5kZXJIb3N0OiBhbnk7XG4gIGlmIChpc0NvbXBvbmVudFZpZXcodmlldykpIHtcbiAgICBjb25zdCBob3N0RGVmID0gdmlldy5wYXJlbnROb2RlRGVmO1xuICAgIHJlbmRlckhvc3QgPSBhc0VsZW1lbnREYXRhKHZpZXcucGFyZW50ICEsIGhvc3REZWYgIS5wYXJlbnQgIS5ub2RlSW5kZXgpLnJlbmRlckVsZW1lbnQ7XG4gIH1cbiAgY29uc3QgZGVmID0gdmlldy5kZWY7XG4gIGNvbnN0IG5vZGVzID0gdmlldy5ub2RlcztcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBkZWYubm9kZXMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBub2RlRGVmID0gZGVmLm5vZGVzW2ldO1xuICAgIFNlcnZpY2VzLnNldEN1cnJlbnROb2RlKHZpZXcsIGkpO1xuICAgIGxldCBub2RlRGF0YTogYW55O1xuICAgIHN3aXRjaCAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlcykge1xuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZUVsZW1lbnQ6XG4gICAgICAgIGNvbnN0IGVsID0gY3JlYXRlRWxlbWVudCh2aWV3LCByZW5kZXJIb3N0LCBub2RlRGVmKSBhcyBhbnk7XG4gICAgICAgIGxldCBjb21wb25lbnRWaWV3OiBWaWV3RGF0YSA9IHVuZGVmaW5lZCAhO1xuICAgICAgICBpZiAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5Db21wb25lbnRWaWV3KSB7XG4gICAgICAgICAgY29uc3QgY29tcFZpZXdEZWYgPSByZXNvbHZlRGVmaW5pdGlvbihub2RlRGVmLmVsZW1lbnQgIS5jb21wb25lbnRWaWV3ICEpO1xuICAgICAgICAgIGNvbXBvbmVudFZpZXcgPSBTZXJ2aWNlcy5jcmVhdGVDb21wb25lbnRWaWV3KHZpZXcsIG5vZGVEZWYsIGNvbXBWaWV3RGVmLCBlbCk7XG4gICAgICAgIH1cbiAgICAgICAgbGlzdGVuVG9FbGVtZW50T3V0cHV0cyh2aWV3LCBjb21wb25lbnRWaWV3LCBub2RlRGVmLCBlbCk7XG4gICAgICAgIG5vZGVEYXRhID0gPEVsZW1lbnREYXRhPntcbiAgICAgICAgICByZW5kZXJFbGVtZW50OiBlbCxcbiAgICAgICAgICBjb21wb25lbnRWaWV3LFxuICAgICAgICAgIHZpZXdDb250YWluZXI6IG51bGwsXG4gICAgICAgICAgdGVtcGxhdGU6IG5vZGVEZWYuZWxlbWVudCAhLnRlbXBsYXRlID8gY3JlYXRlVGVtcGxhdGVEYXRhKHZpZXcsIG5vZGVEZWYpIDogdW5kZWZpbmVkXG4gICAgICAgIH07XG4gICAgICAgIGlmIChub2RlRGVmLmZsYWdzICYgTm9kZUZsYWdzLkVtYmVkZGVkVmlld3MpIHtcbiAgICAgICAgICBub2RlRGF0YS52aWV3Q29udGFpbmVyID0gY3JlYXRlVmlld0NvbnRhaW5lckRhdGEodmlldywgbm9kZURlZiwgbm9kZURhdGEpO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVRleHQ6XG4gICAgICAgIG5vZGVEYXRhID0gY3JlYXRlVGV4dCh2aWV3LCByZW5kZXJIb3N0LCBub2RlRGVmKSBhcyBhbnk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZUNsYXNzUHJvdmlkZXI6XG4gICAgICBjYXNlIE5vZGVGbGFncy5UeXBlRmFjdG9yeVByb3ZpZGVyOlxuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVVzZUV4aXN0aW5nUHJvdmlkZXI6XG4gICAgICBjYXNlIE5vZGVGbGFncy5UeXBlVmFsdWVQcm92aWRlcjoge1xuICAgICAgICBub2RlRGF0YSA9IG5vZGVzW2ldO1xuICAgICAgICBpZiAoIW5vZGVEYXRhICYmICEobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5MYXp5UHJvdmlkZXIpKSB7XG4gICAgICAgICAgY29uc3QgaW5zdGFuY2UgPSBjcmVhdGVQcm92aWRlckluc3RhbmNlKHZpZXcsIG5vZGVEZWYpO1xuICAgICAgICAgIG5vZGVEYXRhID0gPFByb3ZpZGVyRGF0YT57aW5zdGFuY2V9O1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVBpcGU6IHtcbiAgICAgICAgY29uc3QgaW5zdGFuY2UgPSBjcmVhdGVQaXBlSW5zdGFuY2Uodmlldywgbm9kZURlZik7XG4gICAgICAgIG5vZGVEYXRhID0gPFByb3ZpZGVyRGF0YT57aW5zdGFuY2V9O1xuICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVEaXJlY3RpdmU6IHtcbiAgICAgICAgbm9kZURhdGEgPSBub2Rlc1tpXTtcbiAgICAgICAgaWYgKCFub2RlRGF0YSkge1xuICAgICAgICAgIGNvbnN0IGluc3RhbmNlID0gY3JlYXRlRGlyZWN0aXZlSW5zdGFuY2Uodmlldywgbm9kZURlZik7XG4gICAgICAgICAgbm9kZURhdGEgPSA8UHJvdmlkZXJEYXRhPntpbnN0YW5jZX07XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuQ29tcG9uZW50KSB7XG4gICAgICAgICAgY29uc3QgY29tcFZpZXcgPSBhc0VsZW1lbnREYXRhKHZpZXcsIG5vZGVEZWYucGFyZW50ICEubm9kZUluZGV4KS5jb21wb25lbnRWaWV3O1xuICAgICAgICAgIGluaXRWaWV3KGNvbXBWaWV3LCBub2RlRGF0YS5pbnN0YW5jZSwgbm9kZURhdGEuaW5zdGFuY2UpO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVB1cmVBcnJheTpcbiAgICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlT2JqZWN0OlxuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVB1cmVQaXBlOlxuICAgICAgICBub2RlRGF0YSA9IGNyZWF0ZVB1cmVFeHByZXNzaW9uKHZpZXcsIG5vZGVEZWYpIGFzIGFueTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIE5vZGVGbGFncy5UeXBlQ29udGVudFF1ZXJ5OlxuICAgICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVZpZXdRdWVyeTpcbiAgICAgICAgbm9kZURhdGEgPSBjcmVhdGVRdWVyeSgpIGFzIGFueTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIE5vZGVGbGFncy5UeXBlTmdDb250ZW50OlxuICAgICAgICBhcHBlbmROZ0NvbnRlbnQodmlldywgcmVuZGVySG9zdCwgbm9kZURlZik7XG4gICAgICAgIC8vIG5vIHJ1bnRpbWUgZGF0YSBuZWVkZWQgZm9yIE5nQ29udGVudC4uLlxuICAgICAgICBub2RlRGF0YSA9IHVuZGVmaW5lZDtcbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIG5vZGVzW2ldID0gbm9kZURhdGE7XG4gIH1cbiAgLy8gQ3JlYXRlIHRoZSBWaWV3RGF0YS5ub2RlcyBvZiBjb21wb25lbnQgdmlld3MgYWZ0ZXIgd2UgY3JlYXRlZCBldmVyeXRoaW5nIGVsc2UsXG4gIC8vIHNvIHRoYXQgZS5nLiBuZy1jb250ZW50IHdvcmtzXG4gIGV4ZWNDb21wb25lbnRWaWV3c0FjdGlvbih2aWV3LCBWaWV3QWN0aW9uLkNyZWF0ZVZpZXdOb2Rlcyk7XG5cbiAgLy8gZmlsbCBzdGF0aWMgY29udGVudCBhbmQgdmlldyBxdWVyaWVzXG4gIGV4ZWNRdWVyaWVzQWN0aW9uKFxuICAgICAgdmlldywgTm9kZUZsYWdzLlR5cGVDb250ZW50UXVlcnkgfCBOb2RlRmxhZ3MuVHlwZVZpZXdRdWVyeSwgTm9kZUZsYWdzLlN0YXRpY1F1ZXJ5LFxuICAgICAgQ2hlY2tUeXBlLkNoZWNrQW5kVXBkYXRlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNoZWNrTm9DaGFuZ2VzVmlldyh2aWV3OiBWaWV3RGF0YSkge1xuICBtYXJrUHJvamVjdGVkVmlld3NGb3JDaGVjayh2aWV3KTtcbiAgU2VydmljZXMudXBkYXRlRGlyZWN0aXZlcyh2aWV3LCBDaGVja1R5cGUuQ2hlY2tOb0NoYW5nZXMpO1xuICBleGVjRW1iZWRkZWRWaWV3c0FjdGlvbih2aWV3LCBWaWV3QWN0aW9uLkNoZWNrTm9DaGFuZ2VzKTtcbiAgU2VydmljZXMudXBkYXRlUmVuZGVyZXIodmlldywgQ2hlY2tUeXBlLkNoZWNrTm9DaGFuZ2VzKTtcbiAgZXhlY0NvbXBvbmVudFZpZXdzQWN0aW9uKHZpZXcsIFZpZXdBY3Rpb24uQ2hlY2tOb0NoYW5nZXMpO1xuICAvLyBOb3RlOiBXZSBkb24ndCBjaGVjayBxdWVyaWVzIGZvciBjaGFuZ2VzIGFzIHdlIGRpZG4ndCBkbyB0aGlzIGluIHYyLnguXG4gIC8vIFRPRE8odGJvc2NoKTogaW52ZXN0aWdhdGUgaWYgd2UgY2FuIGVuYWJsZSB0aGUgY2hlY2sgYWdhaW4gaW4gdjUueCB3aXRoIGEgbmljZXIgZXJyb3IgbWVzc2FnZS5cbiAgdmlldy5zdGF0ZSAmPSB+KFZpZXdTdGF0ZS5DaGVja1Byb2plY3RlZFZpZXdzIHwgVmlld1N0YXRlLkNoZWNrUHJvamVjdGVkVmlldyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaGVja0FuZFVwZGF0ZVZpZXcodmlldzogVmlld0RhdGEpIHtcbiAgaWYgKHZpZXcuc3RhdGUgJiBWaWV3U3RhdGUuQmVmb3JlRmlyc3RDaGVjaykge1xuICAgIHZpZXcuc3RhdGUgJj0gflZpZXdTdGF0ZS5CZWZvcmVGaXJzdENoZWNrO1xuICAgIHZpZXcuc3RhdGUgfD0gVmlld1N0YXRlLkZpcnN0Q2hlY2s7XG4gIH0gZWxzZSB7XG4gICAgdmlldy5zdGF0ZSAmPSB+Vmlld1N0YXRlLkZpcnN0Q2hlY2s7XG4gIH1cbiAgc2hpZnRJbml0U3RhdGUodmlldywgVmlld1N0YXRlLkluaXRTdGF0ZV9CZWZvcmVJbml0LCBWaWV3U3RhdGUuSW5pdFN0YXRlX0NhbGxpbmdPbkluaXQpO1xuICBtYXJrUHJvamVjdGVkVmlld3NGb3JDaGVjayh2aWV3KTtcbiAgU2VydmljZXMudXBkYXRlRGlyZWN0aXZlcyh2aWV3LCBDaGVja1R5cGUuQ2hlY2tBbmRVcGRhdGUpO1xuICBleGVjRW1iZWRkZWRWaWV3c0FjdGlvbih2aWV3LCBWaWV3QWN0aW9uLkNoZWNrQW5kVXBkYXRlKTtcbiAgZXhlY1F1ZXJpZXNBY3Rpb24oXG4gICAgICB2aWV3LCBOb2RlRmxhZ3MuVHlwZUNvbnRlbnRRdWVyeSwgTm9kZUZsYWdzLkR5bmFtaWNRdWVyeSwgQ2hlY2tUeXBlLkNoZWNrQW5kVXBkYXRlKTtcbiAgbGV0IGNhbGxJbml0ID0gc2hpZnRJbml0U3RhdGUoXG4gICAgICB2aWV3LCBWaWV3U3RhdGUuSW5pdFN0YXRlX0NhbGxpbmdPbkluaXQsIFZpZXdTdGF0ZS5Jbml0U3RhdGVfQ2FsbGluZ0FmdGVyQ29udGVudEluaXQpO1xuICBjYWxsTGlmZWN5Y2xlSG9va3NDaGlsZHJlbkZpcnN0KFxuICAgICAgdmlldywgTm9kZUZsYWdzLkFmdGVyQ29udGVudENoZWNrZWQgfCAoY2FsbEluaXQgPyBOb2RlRmxhZ3MuQWZ0ZXJDb250ZW50SW5pdCA6IDApKTtcblxuICBTZXJ2aWNlcy51cGRhdGVSZW5kZXJlcih2aWV3LCBDaGVja1R5cGUuQ2hlY2tBbmRVcGRhdGUpO1xuXG4gIGV4ZWNDb21wb25lbnRWaWV3c0FjdGlvbih2aWV3LCBWaWV3QWN0aW9uLkNoZWNrQW5kVXBkYXRlKTtcbiAgZXhlY1F1ZXJpZXNBY3Rpb24oXG4gICAgICB2aWV3LCBOb2RlRmxhZ3MuVHlwZVZpZXdRdWVyeSwgTm9kZUZsYWdzLkR5bmFtaWNRdWVyeSwgQ2hlY2tUeXBlLkNoZWNrQW5kVXBkYXRlKTtcbiAgY2FsbEluaXQgPSBzaGlmdEluaXRTdGF0ZShcbiAgICAgIHZpZXcsIFZpZXdTdGF0ZS5Jbml0U3RhdGVfQ2FsbGluZ0FmdGVyQ29udGVudEluaXQsIFZpZXdTdGF0ZS5Jbml0U3RhdGVfQ2FsbGluZ0FmdGVyVmlld0luaXQpO1xuICBjYWxsTGlmZWN5Y2xlSG9va3NDaGlsZHJlbkZpcnN0KFxuICAgICAgdmlldywgTm9kZUZsYWdzLkFmdGVyVmlld0NoZWNrZWQgfCAoY2FsbEluaXQgPyBOb2RlRmxhZ3MuQWZ0ZXJWaWV3SW5pdCA6IDApKTtcblxuICBpZiAodmlldy5kZWYuZmxhZ3MgJiBWaWV3RmxhZ3MuT25QdXNoKSB7XG4gICAgdmlldy5zdGF0ZSAmPSB+Vmlld1N0YXRlLkNoZWNrc0VuYWJsZWQ7XG4gIH1cbiAgdmlldy5zdGF0ZSAmPSB+KFZpZXdTdGF0ZS5DaGVja1Byb2plY3RlZFZpZXdzIHwgVmlld1N0YXRlLkNoZWNrUHJvamVjdGVkVmlldyk7XG4gIHNoaWZ0SW5pdFN0YXRlKHZpZXcsIFZpZXdTdGF0ZS5Jbml0U3RhdGVfQ2FsbGluZ0FmdGVyVmlld0luaXQsIFZpZXdTdGF0ZS5Jbml0U3RhdGVfQWZ0ZXJJbml0KTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNoZWNrQW5kVXBkYXRlTm9kZShcbiAgICB2aWV3OiBWaWV3RGF0YSwgbm9kZURlZjogTm9kZURlZiwgYXJnU3R5bGU6IEFyZ3VtZW50VHlwZSwgdjA/OiBhbnksIHYxPzogYW55LCB2Mj86IGFueSxcbiAgICB2Mz86IGFueSwgdjQ/OiBhbnksIHY1PzogYW55LCB2Nj86IGFueSwgdjc/OiBhbnksIHY4PzogYW55LCB2OT86IGFueSk6IGJvb2xlYW4ge1xuICBpZiAoYXJnU3R5bGUgPT09IEFyZ3VtZW50VHlwZS5JbmxpbmUpIHtcbiAgICByZXR1cm4gY2hlY2tBbmRVcGRhdGVOb2RlSW5saW5lKHZpZXcsIG5vZGVEZWYsIHYwLCB2MSwgdjIsIHYzLCB2NCwgdjUsIHY2LCB2NywgdjgsIHY5KTtcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gY2hlY2tBbmRVcGRhdGVOb2RlRHluYW1pYyh2aWV3LCBub2RlRGVmLCB2MCk7XG4gIH1cbn1cblxuZnVuY3Rpb24gbWFya1Byb2plY3RlZFZpZXdzRm9yQ2hlY2sodmlldzogVmlld0RhdGEpIHtcbiAgY29uc3QgZGVmID0gdmlldy5kZWY7XG4gIGlmICghKGRlZi5ub2RlRmxhZ3MgJiBOb2RlRmxhZ3MuUHJvamVjdGVkVGVtcGxhdGUpKSB7XG4gICAgcmV0dXJuO1xuICB9XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgZGVmLm5vZGVzLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3Qgbm9kZURlZiA9IGRlZi5ub2Rlc1tpXTtcbiAgICBpZiAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5Qcm9qZWN0ZWRUZW1wbGF0ZSkge1xuICAgICAgY29uc3QgcHJvamVjdGVkVmlld3MgPSBhc0VsZW1lbnREYXRhKHZpZXcsIGkpLnRlbXBsYXRlLl9wcm9qZWN0ZWRWaWV3cztcbiAgICAgIGlmIChwcm9qZWN0ZWRWaWV3cykge1xuICAgICAgICBmb3IgKGxldCBpID0gMDsgaSA8IHByb2plY3RlZFZpZXdzLmxlbmd0aDsgaSsrKSB7XG4gICAgICAgICAgY29uc3QgcHJvamVjdGVkVmlldyA9IHByb2plY3RlZFZpZXdzW2ldO1xuICAgICAgICAgIHByb2plY3RlZFZpZXcuc3RhdGUgfD0gVmlld1N0YXRlLkNoZWNrUHJvamVjdGVkVmlldztcbiAgICAgICAgICBtYXJrUGFyZW50Vmlld3NGb3JDaGVja1Byb2plY3RlZFZpZXdzKHByb2plY3RlZFZpZXcsIHZpZXcpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSBlbHNlIGlmICgobm9kZURlZi5jaGlsZEZsYWdzICYgTm9kZUZsYWdzLlByb2plY3RlZFRlbXBsYXRlKSA9PT0gMCkge1xuICAgICAgLy8gYSBwYXJlbnQgd2l0aCBsZWFmc1xuICAgICAgLy8gbm8gY2hpbGQgaXMgYSBjb21wb25lbnQsXG4gICAgICAvLyB0aGVuIHNraXAgdGhlIGNoaWxkcmVuXG4gICAgICBpICs9IG5vZGVEZWYuY2hpbGRDb3VudDtcbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gY2hlY2tBbmRVcGRhdGVOb2RlSW5saW5lKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBub2RlRGVmOiBOb2RlRGVmLCB2MD86IGFueSwgdjE/OiBhbnksIHYyPzogYW55LCB2Mz86IGFueSwgdjQ/OiBhbnksIHY1PzogYW55LFxuICAgIHY2PzogYW55LCB2Nz86IGFueSwgdjg/OiBhbnksIHY5PzogYW55KTogYm9vbGVhbiB7XG4gIHN3aXRjaCAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlcykge1xuICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVFbGVtZW50OlxuICAgICAgcmV0dXJuIGNoZWNrQW5kVXBkYXRlRWxlbWVudElubGluZSh2aWV3LCBub2RlRGVmLCB2MCwgdjEsIHYyLCB2MywgdjQsIHY1LCB2NiwgdjcsIHY4LCB2OSk7XG4gICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVRleHQ6XG4gICAgICByZXR1cm4gY2hlY2tBbmRVcGRhdGVUZXh0SW5saW5lKHZpZXcsIG5vZGVEZWYsIHYwLCB2MSwgdjIsIHYzLCB2NCwgdjUsIHY2LCB2NywgdjgsIHY5KTtcbiAgICBjYXNlIE5vZGVGbGFncy5UeXBlRGlyZWN0aXZlOlxuICAgICAgcmV0dXJuIGNoZWNrQW5kVXBkYXRlRGlyZWN0aXZlSW5saW5lKHZpZXcsIG5vZGVEZWYsIHYwLCB2MSwgdjIsIHYzLCB2NCwgdjUsIHY2LCB2NywgdjgsIHY5KTtcbiAgICBjYXNlIE5vZGVGbGFncy5UeXBlUHVyZUFycmF5OlxuICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlT2JqZWN0OlxuICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlUGlwZTpcbiAgICAgIHJldHVybiBjaGVja0FuZFVwZGF0ZVB1cmVFeHByZXNzaW9uSW5saW5lKFxuICAgICAgICAgIHZpZXcsIG5vZGVEZWYsIHYwLCB2MSwgdjIsIHYzLCB2NCwgdjUsIHY2LCB2NywgdjgsIHY5KTtcbiAgICBkZWZhdWx0OlxuICAgICAgdGhyb3cgJ3VucmVhY2hhYmxlJztcbiAgfVxufVxuXG5mdW5jdGlvbiBjaGVja0FuZFVwZGF0ZU5vZGVEeW5hbWljKHZpZXc6IFZpZXdEYXRhLCBub2RlRGVmOiBOb2RlRGVmLCB2YWx1ZXM6IGFueVtdKTogYm9vbGVhbiB7XG4gIHN3aXRjaCAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlcykge1xuICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVFbGVtZW50OlxuICAgICAgcmV0dXJuIGNoZWNrQW5kVXBkYXRlRWxlbWVudER5bmFtaWModmlldywgbm9kZURlZiwgdmFsdWVzKTtcbiAgICBjYXNlIE5vZGVGbGFncy5UeXBlVGV4dDpcbiAgICAgIHJldHVybiBjaGVja0FuZFVwZGF0ZVRleHREeW5hbWljKHZpZXcsIG5vZGVEZWYsIHZhbHVlcyk7XG4gICAgY2FzZSBOb2RlRmxhZ3MuVHlwZURpcmVjdGl2ZTpcbiAgICAgIHJldHVybiBjaGVja0FuZFVwZGF0ZURpcmVjdGl2ZUR5bmFtaWModmlldywgbm9kZURlZiwgdmFsdWVzKTtcbiAgICBjYXNlIE5vZGVGbGFncy5UeXBlUHVyZUFycmF5OlxuICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlT2JqZWN0OlxuICAgIGNhc2UgTm9kZUZsYWdzLlR5cGVQdXJlUGlwZTpcbiAgICAgIHJldHVybiBjaGVja0FuZFVwZGF0ZVB1cmVFeHByZXNzaW9uRHluYW1pYyh2aWV3LCBub2RlRGVmLCB2YWx1ZXMpO1xuICAgIGRlZmF1bHQ6XG4gICAgICB0aHJvdyAndW5yZWFjaGFibGUnO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaGVja05vQ2hhbmdlc05vZGUoXG4gICAgdmlldzogVmlld0RhdGEsIG5vZGVEZWY6IE5vZGVEZWYsIGFyZ1N0eWxlOiBBcmd1bWVudFR5cGUsIHYwPzogYW55LCB2MT86IGFueSwgdjI/OiBhbnksXG4gICAgdjM/OiBhbnksIHY0PzogYW55LCB2NT86IGFueSwgdjY/OiBhbnksIHY3PzogYW55LCB2OD86IGFueSwgdjk/OiBhbnkpOiBhbnkge1xuICBpZiAoYXJnU3R5bGUgPT09IEFyZ3VtZW50VHlwZS5JbmxpbmUpIHtcbiAgICBjaGVja05vQ2hhbmdlc05vZGVJbmxpbmUodmlldywgbm9kZURlZiwgdjAsIHYxLCB2MiwgdjMsIHY0LCB2NSwgdjYsIHY3LCB2OCwgdjkpO1xuICB9IGVsc2Uge1xuICAgIGNoZWNrTm9DaGFuZ2VzTm9kZUR5bmFtaWModmlldywgbm9kZURlZiwgdjApO1xuICB9XG4gIC8vIFJldHVybmluZyBmYWxzZSBpcyBvayBoZXJlIGFzIHdlIHdvdWxkIGhhdmUgdGhyb3duIGluIGNhc2Ugb2YgYSBjaGFuZ2UuXG4gIHJldHVybiBmYWxzZTtcbn1cblxuZnVuY3Rpb24gY2hlY2tOb0NoYW5nZXNOb2RlSW5saW5lKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBub2RlRGVmOiBOb2RlRGVmLCB2MDogYW55LCB2MTogYW55LCB2MjogYW55LCB2MzogYW55LCB2NDogYW55LCB2NTogYW55LCB2NjogYW55LFxuICAgIHY3OiBhbnksIHY4OiBhbnksIHY5OiBhbnkpOiB2b2lkIHtcbiAgY29uc3QgYmluZExlbiA9IG5vZGVEZWYuYmluZGluZ3MubGVuZ3RoO1xuICBpZiAoYmluZExlbiA+IDApIGNoZWNrQmluZGluZ05vQ2hhbmdlcyh2aWV3LCBub2RlRGVmLCAwLCB2MCk7XG4gIGlmIChiaW5kTGVuID4gMSkgY2hlY2tCaW5kaW5nTm9DaGFuZ2VzKHZpZXcsIG5vZGVEZWYsIDEsIHYxKTtcbiAgaWYgKGJpbmRMZW4gPiAyKSBjaGVja0JpbmRpbmdOb0NoYW5nZXModmlldywgbm9kZURlZiwgMiwgdjIpO1xuICBpZiAoYmluZExlbiA+IDMpIGNoZWNrQmluZGluZ05vQ2hhbmdlcyh2aWV3LCBub2RlRGVmLCAzLCB2Myk7XG4gIGlmIChiaW5kTGVuID4gNCkgY2hlY2tCaW5kaW5nTm9DaGFuZ2VzKHZpZXcsIG5vZGVEZWYsIDQsIHY0KTtcbiAgaWYgKGJpbmRMZW4gPiA1KSBjaGVja0JpbmRpbmdOb0NoYW5nZXModmlldywgbm9kZURlZiwgNSwgdjUpO1xuICBpZiAoYmluZExlbiA+IDYpIGNoZWNrQmluZGluZ05vQ2hhbmdlcyh2aWV3LCBub2RlRGVmLCA2LCB2Nik7XG4gIGlmIChiaW5kTGVuID4gNykgY2hlY2tCaW5kaW5nTm9DaGFuZ2VzKHZpZXcsIG5vZGVEZWYsIDcsIHY3KTtcbiAgaWYgKGJpbmRMZW4gPiA4KSBjaGVja0JpbmRpbmdOb0NoYW5nZXModmlldywgbm9kZURlZiwgOCwgdjgpO1xuICBpZiAoYmluZExlbiA+IDkpIGNoZWNrQmluZGluZ05vQ2hhbmdlcyh2aWV3LCBub2RlRGVmLCA5LCB2OSk7XG59XG5cbmZ1bmN0aW9uIGNoZWNrTm9DaGFuZ2VzTm9kZUR5bmFtaWModmlldzogVmlld0RhdGEsIG5vZGVEZWY6IE5vZGVEZWYsIHZhbHVlczogYW55W10pOiB2b2lkIHtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCB2YWx1ZXMubGVuZ3RoOyBpKyspIHtcbiAgICBjaGVja0JpbmRpbmdOb0NoYW5nZXModmlldywgbm9kZURlZiwgaSwgdmFsdWVzW2ldKTtcbiAgfVxufVxuXG4vKipcbiAqIFdvcmthcm91bmQgaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvdHNpY2tsZS9pc3N1ZXMvNDk3XG4gKiBAc3VwcHJlc3Mge21pc3BsYWNlZFR5cGVBbm5vdGF0aW9ufVxuICovXG5mdW5jdGlvbiBjaGVja05vQ2hhbmdlc1F1ZXJ5KHZpZXc6IFZpZXdEYXRhLCBub2RlRGVmOiBOb2RlRGVmKSB7XG4gIGNvbnN0IHF1ZXJ5TGlzdCA9IGFzUXVlcnlMaXN0KHZpZXcsIG5vZGVEZWYubm9kZUluZGV4KTtcbiAgaWYgKHF1ZXJ5TGlzdC5kaXJ0eSkge1xuICAgIHRocm93IGV4cHJlc3Npb25DaGFuZ2VkQWZ0ZXJJdEhhc0JlZW5DaGVja2VkRXJyb3IoXG4gICAgICAgIFNlcnZpY2VzLmNyZWF0ZURlYnVnQ29udGV4dCh2aWV3LCBub2RlRGVmLm5vZGVJbmRleCksXG4gICAgICAgIGBRdWVyeSAke25vZGVEZWYucXVlcnkhLmlkfSBub3QgZGlydHlgLCBgUXVlcnkgJHtub2RlRGVmLnF1ZXJ5IS5pZH0gZGlydHlgLFxuICAgICAgICAodmlldy5zdGF0ZSAmIFZpZXdTdGF0ZS5CZWZvcmVGaXJzdENoZWNrKSAhPT0gMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlc3Ryb3lWaWV3KHZpZXc6IFZpZXdEYXRhKSB7XG4gIGlmICh2aWV3LnN0YXRlICYgVmlld1N0YXRlLkRlc3Ryb3llZCkge1xuICAgIHJldHVybjtcbiAgfVxuICBleGVjRW1iZWRkZWRWaWV3c0FjdGlvbih2aWV3LCBWaWV3QWN0aW9uLkRlc3Ryb3kpO1xuICBleGVjQ29tcG9uZW50Vmlld3NBY3Rpb24odmlldywgVmlld0FjdGlvbi5EZXN0cm95KTtcbiAgY2FsbExpZmVjeWNsZUhvb2tzQ2hpbGRyZW5GaXJzdCh2aWV3LCBOb2RlRmxhZ3MuT25EZXN0cm95KTtcbiAgaWYgKHZpZXcuZGlzcG9zYWJsZXMpIHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHZpZXcuZGlzcG9zYWJsZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHZpZXcuZGlzcG9zYWJsZXNbaV0oKTtcbiAgICB9XG4gIH1cbiAgZGV0YWNoUHJvamVjdGVkVmlldyh2aWV3KTtcbiAgaWYgKHZpZXcucmVuZGVyZXIuZGVzdHJveU5vZGUpIHtcbiAgICBkZXN0cm95Vmlld05vZGVzKHZpZXcpO1xuICB9XG4gIGlmIChpc0NvbXBvbmVudFZpZXcodmlldykpIHtcbiAgICB2aWV3LnJlbmRlcmVyLmRlc3Ryb3koKTtcbiAgfVxuICB2aWV3LnN0YXRlIHw9IFZpZXdTdGF0ZS5EZXN0cm95ZWQ7XG59XG5cbmZ1bmN0aW9uIGRlc3Ryb3lWaWV3Tm9kZXModmlldzogVmlld0RhdGEpIHtcbiAgY29uc3QgbGVuID0gdmlldy5kZWYubm9kZXMubGVuZ3RoO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGxlbjsgaSsrKSB7XG4gICAgY29uc3QgZGVmID0gdmlldy5kZWYubm9kZXNbaV07XG4gICAgaWYgKGRlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlRWxlbWVudCkge1xuICAgICAgdmlldy5yZW5kZXJlci5kZXN0cm95Tm9kZSAhKGFzRWxlbWVudERhdGEodmlldywgaSkucmVuZGVyRWxlbWVudCk7XG4gICAgfSBlbHNlIGlmIChkZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuVHlwZVRleHQpIHtcbiAgICAgIHZpZXcucmVuZGVyZXIuZGVzdHJveU5vZGUgIShhc1RleHREYXRhKHZpZXcsIGkpLnJlbmRlclRleHQpO1xuICAgIH0gZWxzZSBpZiAoZGVmLmZsYWdzICYgTm9kZUZsYWdzLlR5cGVDb250ZW50UXVlcnkgfHwgZGVmLmZsYWdzICYgTm9kZUZsYWdzLlR5cGVWaWV3UXVlcnkpIHtcbiAgICAgIGFzUXVlcnlMaXN0KHZpZXcsIGkpLmRlc3Ryb3koKTtcbiAgICB9XG4gIH1cbn1cblxuZW51bSBWaWV3QWN0aW9uIHtcbiAgQ3JlYXRlVmlld05vZGVzLFxuICBDaGVja05vQ2hhbmdlcyxcbiAgQ2hlY2tOb0NoYW5nZXNQcm9qZWN0ZWRWaWV3cyxcbiAgQ2hlY2tBbmRVcGRhdGUsXG4gIENoZWNrQW5kVXBkYXRlUHJvamVjdGVkVmlld3MsXG4gIERlc3Ryb3lcbn1cblxuZnVuY3Rpb24gZXhlY0NvbXBvbmVudFZpZXdzQWN0aW9uKHZpZXc6IFZpZXdEYXRhLCBhY3Rpb246IFZpZXdBY3Rpb24pIHtcbiAgY29uc3QgZGVmID0gdmlldy5kZWY7XG4gIGlmICghKGRlZi5ub2RlRmxhZ3MgJiBOb2RlRmxhZ3MuQ29tcG9uZW50VmlldykpIHtcbiAgICByZXR1cm47XG4gIH1cbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBkZWYubm9kZXMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBub2RlRGVmID0gZGVmLm5vZGVzW2ldO1xuICAgIGlmIChub2RlRGVmLmZsYWdzICYgTm9kZUZsYWdzLkNvbXBvbmVudFZpZXcpIHtcbiAgICAgIC8vIGEgbGVhZlxuICAgICAgY2FsbFZpZXdBY3Rpb24oYXNFbGVtZW50RGF0YSh2aWV3LCBpKS5jb21wb25lbnRWaWV3LCBhY3Rpb24pO1xuICAgIH0gZWxzZSBpZiAoKG5vZGVEZWYuY2hpbGRGbGFncyAmIE5vZGVGbGFncy5Db21wb25lbnRWaWV3KSA9PT0gMCkge1xuICAgICAgLy8gYSBwYXJlbnQgd2l0aCBsZWFmc1xuICAgICAgLy8gbm8gY2hpbGQgaXMgYSBjb21wb25lbnQsXG4gICAgICAvLyB0aGVuIHNraXAgdGhlIGNoaWxkcmVuXG4gICAgICBpICs9IG5vZGVEZWYuY2hpbGRDb3VudDtcbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gZXhlY0VtYmVkZGVkVmlld3NBY3Rpb24odmlldzogVmlld0RhdGEsIGFjdGlvbjogVmlld0FjdGlvbikge1xuICBjb25zdCBkZWYgPSB2aWV3LmRlZjtcbiAgaWYgKCEoZGVmLm5vZGVGbGFncyAmIE5vZGVGbGFncy5FbWJlZGRlZFZpZXdzKSkge1xuICAgIHJldHVybjtcbiAgfVxuICBmb3IgKGxldCBpID0gMDsgaSA8IGRlZi5ub2Rlcy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IG5vZGVEZWYgPSBkZWYubm9kZXNbaV07XG4gICAgaWYgKG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuRW1iZWRkZWRWaWV3cykge1xuICAgICAgLy8gYSBsZWFmXG4gICAgICBjb25zdCBlbWJlZGRlZFZpZXdzID0gYXNFbGVtZW50RGF0YSh2aWV3LCBpKS52aWV3Q29udGFpbmVyICEuX2VtYmVkZGVkVmlld3M7XG4gICAgICBmb3IgKGxldCBrID0gMDsgayA8IGVtYmVkZGVkVmlld3MubGVuZ3RoOyBrKyspIHtcbiAgICAgICAgY2FsbFZpZXdBY3Rpb24oZW1iZWRkZWRWaWV3c1trXSwgYWN0aW9uKTtcbiAgICAgIH1cbiAgICB9IGVsc2UgaWYgKChub2RlRGVmLmNoaWxkRmxhZ3MgJiBOb2RlRmxhZ3MuRW1iZWRkZWRWaWV3cykgPT09IDApIHtcbiAgICAgIC8vIGEgcGFyZW50IHdpdGggbGVhZnNcbiAgICAgIC8vIG5vIGNoaWxkIGlzIGEgY29tcG9uZW50LFxuICAgICAgLy8gdGhlbiBza2lwIHRoZSBjaGlsZHJlblxuICAgICAgaSArPSBub2RlRGVmLmNoaWxkQ291bnQ7XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIGNhbGxWaWV3QWN0aW9uKHZpZXc6IFZpZXdEYXRhLCBhY3Rpb246IFZpZXdBY3Rpb24pIHtcbiAgY29uc3Qgdmlld1N0YXRlID0gdmlldy5zdGF0ZTtcbiAgc3dpdGNoIChhY3Rpb24pIHtcbiAgICBjYXNlIFZpZXdBY3Rpb24uQ2hlY2tOb0NoYW5nZXM6XG4gICAgICBpZiAoKHZpZXdTdGF0ZSAmIFZpZXdTdGF0ZS5EZXN0cm95ZWQpID09PSAwKSB7XG4gICAgICAgIGlmICgodmlld1N0YXRlICYgVmlld1N0YXRlLkNhdERldGVjdENoYW5nZXMpID09PSBWaWV3U3RhdGUuQ2F0RGV0ZWN0Q2hhbmdlcykge1xuICAgICAgICAgIGNoZWNrTm9DaGFuZ2VzVmlldyh2aWV3KTtcbiAgICAgICAgfSBlbHNlIGlmICh2aWV3U3RhdGUgJiBWaWV3U3RhdGUuQ2hlY2tQcm9qZWN0ZWRWaWV3cykge1xuICAgICAgICAgIGV4ZWNQcm9qZWN0ZWRWaWV3c0FjdGlvbih2aWV3LCBWaWV3QWN0aW9uLkNoZWNrTm9DaGFuZ2VzUHJvamVjdGVkVmlld3MpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICBicmVhaztcbiAgICBjYXNlIFZpZXdBY3Rpb24uQ2hlY2tOb0NoYW5nZXNQcm9qZWN0ZWRWaWV3czpcbiAgICAgIGlmICgodmlld1N0YXRlICYgVmlld1N0YXRlLkRlc3Ryb3llZCkgPT09IDApIHtcbiAgICAgICAgaWYgKHZpZXdTdGF0ZSAmIFZpZXdTdGF0ZS5DaGVja1Byb2plY3RlZFZpZXcpIHtcbiAgICAgICAgICBjaGVja05vQ2hhbmdlc1ZpZXcodmlldyk7XG4gICAgICAgIH0gZWxzZSBpZiAodmlld1N0YXRlICYgVmlld1N0YXRlLkNoZWNrUHJvamVjdGVkVmlld3MpIHtcbiAgICAgICAgICBleGVjUHJvamVjdGVkVmlld3NBY3Rpb24odmlldywgYWN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgYnJlYWs7XG4gICAgY2FzZSBWaWV3QWN0aW9uLkNoZWNrQW5kVXBkYXRlOlxuICAgICAgaWYgKCh2aWV3U3RhdGUgJiBWaWV3U3RhdGUuRGVzdHJveWVkKSA9PT0gMCkge1xuICAgICAgICBpZiAoKHZpZXdTdGF0ZSAmIFZpZXdTdGF0ZS5DYXREZXRlY3RDaGFuZ2VzKSA9PT0gVmlld1N0YXRlLkNhdERldGVjdENoYW5nZXMpIHtcbiAgICAgICAgICBjaGVja0FuZFVwZGF0ZVZpZXcodmlldyk7XG4gICAgICAgIH0gZWxzZSBpZiAodmlld1N0YXRlICYgVmlld1N0YXRlLkNoZWNrUHJvamVjdGVkVmlld3MpIHtcbiAgICAgICAgICBleGVjUHJvamVjdGVkVmlld3NBY3Rpb24odmlldywgVmlld0FjdGlvbi5DaGVja0FuZFVwZGF0ZVByb2plY3RlZFZpZXdzKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgYnJlYWs7XG4gICAgY2FzZSBWaWV3QWN0aW9uLkNoZWNrQW5kVXBkYXRlUHJvamVjdGVkVmlld3M6XG4gICAgICBpZiAoKHZpZXdTdGF0ZSAmIFZpZXdTdGF0ZS5EZXN0cm95ZWQpID09PSAwKSB7XG4gICAgICAgIGlmICh2aWV3U3RhdGUgJiBWaWV3U3RhdGUuQ2hlY2tQcm9qZWN0ZWRWaWV3KSB7XG4gICAgICAgICAgY2hlY2tBbmRVcGRhdGVWaWV3KHZpZXcpO1xuICAgICAgICB9IGVsc2UgaWYgKHZpZXdTdGF0ZSAmIFZpZXdTdGF0ZS5DaGVja1Byb2plY3RlZFZpZXdzKSB7XG4gICAgICAgICAgZXhlY1Byb2plY3RlZFZpZXdzQWN0aW9uKHZpZXcsIGFjdGlvbik7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgVmlld0FjdGlvbi5EZXN0cm95OlxuICAgICAgLy8gTm90ZTogZGVzdHJveVZpZXcgcmVjdXJzZXMgb3ZlciBhbGwgdmlld3MsXG4gICAgICAvLyBzbyB3ZSBkb24ndCBuZWVkIHRvIHNwZWNpYWwgY2FzZSBwcm9qZWN0ZWQgdmlld3MgaGVyZS5cbiAgICAgIGRlc3Ryb3lWaWV3KHZpZXcpO1xuICAgICAgYnJlYWs7XG4gICAgY2FzZSBWaWV3QWN0aW9uLkNyZWF0ZVZpZXdOb2RlczpcbiAgICAgIGNyZWF0ZVZpZXdOb2Rlcyh2aWV3KTtcbiAgICAgIGJyZWFrO1xuICB9XG59XG5cbmZ1bmN0aW9uIGV4ZWNQcm9qZWN0ZWRWaWV3c0FjdGlvbih2aWV3OiBWaWV3RGF0YSwgYWN0aW9uOiBWaWV3QWN0aW9uKSB7XG4gIGV4ZWNFbWJlZGRlZFZpZXdzQWN0aW9uKHZpZXcsIGFjdGlvbik7XG4gIGV4ZWNDb21wb25lbnRWaWV3c0FjdGlvbih2aWV3LCBhY3Rpb24pO1xufVxuXG5mdW5jdGlvbiBleGVjUXVlcmllc0FjdGlvbihcbiAgICB2aWV3OiBWaWV3RGF0YSwgcXVlcnlGbGFnczogTm9kZUZsYWdzLCBzdGF0aWNEeW5hbWljUXVlcnlGbGFnOiBOb2RlRmxhZ3MsXG4gICAgY2hlY2tUeXBlOiBDaGVja1R5cGUpIHtcbiAgaWYgKCEodmlldy5kZWYubm9kZUZsYWdzICYgcXVlcnlGbGFncykgfHwgISh2aWV3LmRlZi5ub2RlRmxhZ3MgJiBzdGF0aWNEeW5hbWljUXVlcnlGbGFnKSkge1xuICAgIHJldHVybjtcbiAgfVxuICBjb25zdCBub2RlQ291bnQgPSB2aWV3LmRlZi5ub2Rlcy5sZW5ndGg7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgbm9kZUNvdW50OyBpKyspIHtcbiAgICBjb25zdCBub2RlRGVmID0gdmlldy5kZWYubm9kZXNbaV07XG4gICAgaWYgKChub2RlRGVmLmZsYWdzICYgcXVlcnlGbGFncykgJiYgKG5vZGVEZWYuZmxhZ3MgJiBzdGF0aWNEeW5hbWljUXVlcnlGbGFnKSkge1xuICAgICAgU2VydmljZXMuc2V0Q3VycmVudE5vZGUodmlldywgbm9kZURlZi5ub2RlSW5kZXgpO1xuICAgICAgc3dpdGNoIChjaGVja1R5cGUpIHtcbiAgICAgICAgY2FzZSBDaGVja1R5cGUuQ2hlY2tBbmRVcGRhdGU6XG4gICAgICAgICAgY2hlY2tBbmRVcGRhdGVRdWVyeSh2aWV3LCBub2RlRGVmKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgY2FzZSBDaGVja1R5cGUuQ2hlY2tOb0NoYW5nZXM6XG4gICAgICAgICAgY2hlY2tOb0NoYW5nZXNRdWVyeSh2aWV3LCBub2RlRGVmKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKCEobm9kZURlZi5jaGlsZEZsYWdzICYgcXVlcnlGbGFncykgfHwgIShub2RlRGVmLmNoaWxkRmxhZ3MgJiBzdGF0aWNEeW5hbWljUXVlcnlGbGFnKSkge1xuICAgICAgLy8gbm8gY2hpbGQgaGFzIGEgbWF0Y2hpbmcgcXVlcnlcbiAgICAgIC8vIHRoZW4gc2tpcCB0aGUgY2hpbGRyZW5cbiAgICAgIGkgKz0gbm9kZURlZi5jaGlsZENvdW50O1xuICAgIH1cbiAgfVxufVxuIl19