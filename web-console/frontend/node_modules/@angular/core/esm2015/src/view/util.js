/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { WrappedValue, devModeEqual } from '../change_detection/change_detection';
import { SOURCE } from '../di/injector_compatibility';
import { ViewEncapsulation } from '../metadata/view';
import { looseIdentical } from '../util/comparison';
import { stringify } from '../util/stringify';
import { expressionChangedAfterItHasBeenCheckedError } from './errors';
import { Services, asElementData, asTextData } from './types';
/** @type {?} */
export const NOOP = (/**
 * @return {?}
 */
() => { });
/** @type {?} */
const _tokenKeyCache = new Map();
/**
 * @param {?} token
 * @return {?}
 */
export function tokenKey(token) {
    /** @type {?} */
    let key = _tokenKeyCache.get(token);
    if (!key) {
        key = stringify(token) + '_' + _tokenKeyCache.size;
        _tokenKeyCache.set(token, key);
    }
    return key;
}
/**
 * @param {?} view
 * @param {?} nodeIdx
 * @param {?} bindingIdx
 * @param {?} value
 * @return {?}
 */
export function unwrapValue(view, nodeIdx, bindingIdx, value) {
    if (WrappedValue.isWrapped(value)) {
        value = WrappedValue.unwrap(value);
        /** @type {?} */
        const globalBindingIdx = view.def.nodes[nodeIdx].bindingIndex + bindingIdx;
        /** @type {?} */
        const oldValue = WrappedValue.unwrap(view.oldValues[globalBindingIdx]);
        view.oldValues[globalBindingIdx] = new WrappedValue(oldValue);
    }
    return value;
}
/** @type {?} */
const UNDEFINED_RENDERER_TYPE_ID = '$$undefined';
/** @type {?} */
const EMPTY_RENDERER_TYPE_ID = '$$empty';
// Attention: this function is called as top level function.
// Putting any logic in here will destroy closure tree shaking!
/**
 * @param {?} values
 * @return {?}
 */
export function createRendererType2(values) {
    return {
        id: UNDEFINED_RENDERER_TYPE_ID,
        styles: values.styles,
        encapsulation: values.encapsulation,
        data: values.data
    };
}
/** @type {?} */
let _renderCompCount = 0;
/**
 * @param {?=} type
 * @return {?}
 */
export function resolveRendererType2(type) {
    if (type && type.id === UNDEFINED_RENDERER_TYPE_ID) {
        // first time we see this RendererType2. Initialize it...
        /** @type {?} */
        const isFilled = ((type.encapsulation != null && type.encapsulation !== ViewEncapsulation.None) ||
            type.styles.length || Object.keys(type.data).length);
        if (isFilled) {
            type.id = `c${_renderCompCount++}`;
        }
        else {
            type.id = EMPTY_RENDERER_TYPE_ID;
        }
    }
    if (type && type.id === EMPTY_RENDERER_TYPE_ID) {
        type = null;
    }
    return type || null;
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} bindingIdx
 * @param {?} value
 * @return {?}
 */
export function checkBinding(view, def, bindingIdx, value) {
    /** @type {?} */
    const oldValues = view.oldValues;
    if ((view.state & 2 /* FirstCheck */) ||
        !looseIdentical(oldValues[def.bindingIndex + bindingIdx], value)) {
        return true;
    }
    return false;
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} bindingIdx
 * @param {?} value
 * @return {?}
 */
export function checkAndUpdateBinding(view, def, bindingIdx, value) {
    if (checkBinding(view, def, bindingIdx, value)) {
        view.oldValues[def.bindingIndex + bindingIdx] = value;
        return true;
    }
    return false;
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} bindingIdx
 * @param {?} value
 * @return {?}
 */
export function checkBindingNoChanges(view, def, bindingIdx, value) {
    /** @type {?} */
    const oldValue = view.oldValues[def.bindingIndex + bindingIdx];
    if ((view.state & 1 /* BeforeFirstCheck */) || !devModeEqual(oldValue, value)) {
        /** @type {?} */
        const bindingName = def.bindings[bindingIdx].name;
        throw expressionChangedAfterItHasBeenCheckedError(Services.createDebugContext(view, def.nodeIndex), `${bindingName}: ${oldValue}`, `${bindingName}: ${value}`, (view.state & 1 /* BeforeFirstCheck */) !== 0);
    }
}
/**
 * @param {?} view
 * @return {?}
 */
export function markParentViewsForCheck(view) {
    /** @type {?} */
    let currView = view;
    while (currView) {
        if (currView.def.flags & 2 /* OnPush */) {
            currView.state |= 8 /* ChecksEnabled */;
        }
        currView = currView.viewContainerParent || currView.parent;
    }
}
/**
 * @param {?} view
 * @param {?} endView
 * @return {?}
 */
export function markParentViewsForCheckProjectedViews(view, endView) {
    /** @type {?} */
    let currView = view;
    while (currView && currView !== endView) {
        currView.state |= 64 /* CheckProjectedViews */;
        currView = currView.viewContainerParent || currView.parent;
    }
}
/**
 * @param {?} view
 * @param {?} nodeIndex
 * @param {?} eventName
 * @param {?} event
 * @return {?}
 */
export function dispatchEvent(view, nodeIndex, eventName, event) {
    try {
        /** @type {?} */
        const nodeDef = view.def.nodes[nodeIndex];
        /** @type {?} */
        const startView = nodeDef.flags & 33554432 /* ComponentView */ ?
            asElementData(view, nodeIndex).componentView :
            view;
        markParentViewsForCheck(startView);
        return Services.handleEvent(view, nodeIndex, eventName, event);
    }
    catch (e) {
        // Attention: Don't rethrow, as it would cancel Observable subscriptions!
        view.root.errorHandler.handleError(e);
    }
}
/**
 * @param {?} view
 * @return {?}
 */
export function declaredViewContainer(view) {
    if (view.parent) {
        /** @type {?} */
        const parentView = view.parent;
        return asElementData(parentView, (/** @type {?} */ (view.parentNodeDef)).nodeIndex);
    }
    return null;
}
/**
 * for component views, this is the host element.
 * for embedded views, this is the index of the parent node
 * that contains the view container.
 * @param {?} view
 * @return {?}
 */
export function viewParentEl(view) {
    /** @type {?} */
    const parentView = view.parent;
    if (parentView) {
        return (/** @type {?} */ (view.parentNodeDef)).parent;
    }
    else {
        return null;
    }
}
/**
 * @param {?} view
 * @param {?} def
 * @return {?}
 */
export function renderNode(view, def) {
    switch (def.flags & 201347067 /* Types */) {
        case 1 /* TypeElement */:
            return asElementData(view, def.nodeIndex).renderElement;
        case 2 /* TypeText */:
            return asTextData(view, def.nodeIndex).renderText;
    }
}
/**
 * @param {?} target
 * @param {?} name
 * @return {?}
 */
export function elementEventFullName(target, name) {
    return target ? `${target}:${name}` : name;
}
/**
 * @param {?} view
 * @return {?}
 */
export function isComponentView(view) {
    return !!view.parent && !!((/** @type {?} */ (view.parentNodeDef)).flags & 32768 /* Component */);
}
/**
 * @param {?} view
 * @return {?}
 */
export function isEmbeddedView(view) {
    return !!view.parent && !((/** @type {?} */ (view.parentNodeDef)).flags & 32768 /* Component */);
}
/**
 * @param {?} queryId
 * @return {?}
 */
export function filterQueryId(queryId) {
    return 1 << (queryId % 32);
}
/**
 * @param {?} matchedQueriesDsl
 * @return {?}
 */
export function splitMatchedQueriesDsl(matchedQueriesDsl) {
    /** @type {?} */
    const matchedQueries = {};
    /** @type {?} */
    let matchedQueryIds = 0;
    /** @type {?} */
    const references = {};
    if (matchedQueriesDsl) {
        matchedQueriesDsl.forEach((/**
         * @param {?} __0
         * @return {?}
         */
        ([queryId, valueType]) => {
            if (typeof queryId === 'number') {
                matchedQueries[queryId] = valueType;
                matchedQueryIds |= filterQueryId(queryId);
            }
            else {
                references[queryId] = valueType;
            }
        }));
    }
    return { matchedQueries, references, matchedQueryIds };
}
/**
 * @param {?} deps
 * @param {?=} sourceName
 * @return {?}
 */
export function splitDepsDsl(deps, sourceName) {
    return deps.map((/**
     * @param {?} value
     * @return {?}
     */
    value => {
        /** @type {?} */
        let token;
        /** @type {?} */
        let flags;
        if (Array.isArray(value)) {
            [flags, token] = value;
        }
        else {
            flags = 0 /* None */;
            token = value;
        }
        if (token && (typeof token === 'function' || typeof token === 'object') && sourceName) {
            Object.defineProperty(token, SOURCE, { value: sourceName, configurable: true });
        }
        return { flags, token, tokenKey: tokenKey(token) };
    }));
}
/**
 * @param {?} view
 * @param {?} renderHost
 * @param {?} def
 * @return {?}
 */
export function getParentRenderElement(view, renderHost, def) {
    /** @type {?} */
    let renderParent = def.renderParent;
    if (renderParent) {
        if ((renderParent.flags & 1 /* TypeElement */) === 0 ||
            (renderParent.flags & 33554432 /* ComponentView */) === 0 ||
            ((/** @type {?} */ (renderParent.element)).componentRendererType &&
                (/** @type {?} */ ((/** @type {?} */ (renderParent.element)).componentRendererType)).encapsulation ===
                    ViewEncapsulation.Native)) {
            // only children of non components, or children of components with native encapsulation should
            // be attached.
            return asElementData(view, (/** @type {?} */ (def.renderParent)).nodeIndex).renderElement;
        }
    }
    else {
        return renderHost;
    }
}
/** @type {?} */
const DEFINITION_CACHE = new WeakMap();
/**
 * @template D
 * @param {?} factory
 * @return {?}
 */
export function resolveDefinition(factory) {
    /** @type {?} */
    let value = (/** @type {?} */ ((/** @type {?} */ (DEFINITION_CACHE.get(factory)))));
    if (!value) {
        value = factory((/**
         * @return {?}
         */
        () => NOOP));
        value.factory = factory;
        DEFINITION_CACHE.set(factory, value);
    }
    return value;
}
/**
 * @param {?} view
 * @return {?}
 */
export function rootRenderNodes(view) {
    /** @type {?} */
    const renderNodes = [];
    visitRootRenderNodes(view, 0 /* Collect */, undefined, undefined, renderNodes);
    return renderNodes;
}
/** @enum {number} */
const RenderNodeAction = {
    Collect: 0, AppendChild: 1, InsertBefore: 2, RemoveChild: 3,
};
export { RenderNodeAction };
/**
 * @param {?} view
 * @param {?} action
 * @param {?} parentNode
 * @param {?} nextSibling
 * @param {?=} target
 * @return {?}
 */
export function visitRootRenderNodes(view, action, parentNode, nextSibling, target) {
    // We need to re-compute the parent node in case the nodes have been moved around manually
    if (action === 3 /* RemoveChild */) {
        parentNode = view.renderer.parentNode(renderNode(view, (/** @type {?} */ (view.def.lastRenderRootNode))));
    }
    visitSiblingRenderNodes(view, action, 0, view.def.nodes.length - 1, parentNode, nextSibling, target);
}
/**
 * @param {?} view
 * @param {?} action
 * @param {?} startIndex
 * @param {?} endIndex
 * @param {?} parentNode
 * @param {?} nextSibling
 * @param {?=} target
 * @return {?}
 */
export function visitSiblingRenderNodes(view, action, startIndex, endIndex, parentNode, nextSibling, target) {
    for (let i = startIndex; i <= endIndex; i++) {
        /** @type {?} */
        const nodeDef = view.def.nodes[i];
        if (nodeDef.flags & (1 /* TypeElement */ | 2 /* TypeText */ | 8 /* TypeNgContent */)) {
            visitRenderNode(view, nodeDef, action, parentNode, nextSibling, target);
        }
        // jump to next sibling
        i += nodeDef.childCount;
    }
}
/**
 * @param {?} view
 * @param {?} ngContentIndex
 * @param {?} action
 * @param {?} parentNode
 * @param {?} nextSibling
 * @param {?=} target
 * @return {?}
 */
export function visitProjectedRenderNodes(view, ngContentIndex, action, parentNode, nextSibling, target) {
    /** @type {?} */
    let compView = view;
    while (compView && !isComponentView(compView)) {
        compView = compView.parent;
    }
    /** @type {?} */
    const hostView = (/** @type {?} */ (compView)).parent;
    /** @type {?} */
    const hostElDef = viewParentEl((/** @type {?} */ (compView)));
    /** @type {?} */
    const startIndex = (/** @type {?} */ (hostElDef)).nodeIndex + 1;
    /** @type {?} */
    const endIndex = (/** @type {?} */ (hostElDef)).nodeIndex + (/** @type {?} */ (hostElDef)).childCount;
    for (let i = startIndex; i <= endIndex; i++) {
        /** @type {?} */
        const nodeDef = (/** @type {?} */ (hostView)).def.nodes[i];
        if (nodeDef.ngContentIndex === ngContentIndex) {
            visitRenderNode((/** @type {?} */ (hostView)), nodeDef, action, parentNode, nextSibling, target);
        }
        // jump to next sibling
        i += nodeDef.childCount;
    }
    if (!(/** @type {?} */ (hostView)).parent) {
        // a root view
        /** @type {?} */
        const projectedNodes = view.root.projectableNodes[ngContentIndex];
        if (projectedNodes) {
            for (let i = 0; i < projectedNodes.length; i++) {
                execRenderNodeAction(view, projectedNodes[i], action, parentNode, nextSibling, target);
            }
        }
    }
}
/**
 * @param {?} view
 * @param {?} nodeDef
 * @param {?} action
 * @param {?} parentNode
 * @param {?} nextSibling
 * @param {?=} target
 * @return {?}
 */
function visitRenderNode(view, nodeDef, action, parentNode, nextSibling, target) {
    if (nodeDef.flags & 8 /* TypeNgContent */) {
        visitProjectedRenderNodes(view, (/** @type {?} */ (nodeDef.ngContent)).index, action, parentNode, nextSibling, target);
    }
    else {
        /** @type {?} */
        const rn = renderNode(view, nodeDef);
        if (action === 3 /* RemoveChild */ && (nodeDef.flags & 33554432 /* ComponentView */) &&
            (nodeDef.bindingFlags & 48 /* CatSyntheticProperty */)) {
            // Note: we might need to do both actions.
            if (nodeDef.bindingFlags & (16 /* SyntheticProperty */)) {
                execRenderNodeAction(view, rn, action, parentNode, nextSibling, target);
            }
            if (nodeDef.bindingFlags & (32 /* SyntheticHostProperty */)) {
                /** @type {?} */
                const compView = asElementData(view, nodeDef.nodeIndex).componentView;
                execRenderNodeAction(compView, rn, action, parentNode, nextSibling, target);
            }
        }
        else {
            execRenderNodeAction(view, rn, action, parentNode, nextSibling, target);
        }
        if (nodeDef.flags & 16777216 /* EmbeddedViews */) {
            /** @type {?} */
            const embeddedViews = (/** @type {?} */ (asElementData(view, nodeDef.nodeIndex).viewContainer))._embeddedViews;
            for (let k = 0; k < embeddedViews.length; k++) {
                visitRootRenderNodes(embeddedViews[k], action, parentNode, nextSibling, target);
            }
        }
        if (nodeDef.flags & 1 /* TypeElement */ && !(/** @type {?} */ (nodeDef.element)).name) {
            visitSiblingRenderNodes(view, action, nodeDef.nodeIndex + 1, nodeDef.nodeIndex + nodeDef.childCount, parentNode, nextSibling, target);
        }
    }
}
/**
 * @param {?} view
 * @param {?} renderNode
 * @param {?} action
 * @param {?} parentNode
 * @param {?} nextSibling
 * @param {?=} target
 * @return {?}
 */
function execRenderNodeAction(view, renderNode, action, parentNode, nextSibling, target) {
    /** @type {?} */
    const renderer = view.renderer;
    switch (action) {
        case 1 /* AppendChild */:
            renderer.appendChild(parentNode, renderNode);
            break;
        case 2 /* InsertBefore */:
            renderer.insertBefore(parentNode, renderNode, nextSibling);
            break;
        case 3 /* RemoveChild */:
            renderer.removeChild(parentNode, renderNode);
            break;
        case 0 /* Collect */:
            (/** @type {?} */ (target)).push(renderNode);
            break;
    }
}
/** @type {?} */
const NS_PREFIX_RE = /^:([^:]+):(.+)$/;
/**
 * @param {?} name
 * @return {?}
 */
export function splitNamespace(name) {
    if (name[0] === ':') {
        /** @type {?} */
        const match = (/** @type {?} */ (name.match(NS_PREFIX_RE)));
        return [match[1], match[2]];
    }
    return ['', name];
}
/**
 * @param {?} bindings
 * @return {?}
 */
export function calcBindingFlags(bindings) {
    /** @type {?} */
    let flags = 0;
    for (let i = 0; i < bindings.length; i++) {
        flags |= bindings[i].flags;
    }
    return flags;
}
/**
 * @param {?} valueCount
 * @param {?} constAndInterp
 * @return {?}
 */
export function interpolate(valueCount, constAndInterp) {
    /** @type {?} */
    let result = '';
    for (let i = 0; i < valueCount * 2; i = i + 2) {
        result = result + constAndInterp[i] + _toStringWithNull(constAndInterp[i + 1]);
    }
    return result + constAndInterp[valueCount * 2];
}
/**
 * @param {?} valueCount
 * @param {?} c0
 * @param {?} a1
 * @param {?} c1
 * @param {?=} a2
 * @param {?=} c2
 * @param {?=} a3
 * @param {?=} c3
 * @param {?=} a4
 * @param {?=} c4
 * @param {?=} a5
 * @param {?=} c5
 * @param {?=} a6
 * @param {?=} c6
 * @param {?=} a7
 * @param {?=} c7
 * @param {?=} a8
 * @param {?=} c8
 * @param {?=} a9
 * @param {?=} c9
 * @return {?}
 */
export function inlineInterpolate(valueCount, c0, a1, c1, a2, c2, a3, c3, a4, c4, a5, c5, a6, c6, a7, c7, a8, c8, a9, c9) {
    switch (valueCount) {
        case 1:
            return c0 + _toStringWithNull(a1) + c1;
        case 2:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2;
        case 3:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2 + _toStringWithNull(a3) +
                c3;
        case 4:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2 + _toStringWithNull(a3) +
                c3 + _toStringWithNull(a4) + c4;
        case 5:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2 + _toStringWithNull(a3) +
                c3 + _toStringWithNull(a4) + c4 + _toStringWithNull(a5) + c5;
        case 6:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2 + _toStringWithNull(a3) +
                c3 + _toStringWithNull(a4) + c4 + _toStringWithNull(a5) + c5 + _toStringWithNull(a6) + c6;
        case 7:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2 + _toStringWithNull(a3) +
                c3 + _toStringWithNull(a4) + c4 + _toStringWithNull(a5) + c5 + _toStringWithNull(a6) +
                c6 + _toStringWithNull(a7) + c7;
        case 8:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2 + _toStringWithNull(a3) +
                c3 + _toStringWithNull(a4) + c4 + _toStringWithNull(a5) + c5 + _toStringWithNull(a6) +
                c6 + _toStringWithNull(a7) + c7 + _toStringWithNull(a8) + c8;
        case 9:
            return c0 + _toStringWithNull(a1) + c1 + _toStringWithNull(a2) + c2 + _toStringWithNull(a3) +
                c3 + _toStringWithNull(a4) + c4 + _toStringWithNull(a5) + c5 + _toStringWithNull(a6) +
                c6 + _toStringWithNull(a7) + c7 + _toStringWithNull(a8) + c8 + _toStringWithNull(a9) + c9;
        default:
            throw new Error(`Does not support more than 9 expressions`);
    }
}
/**
 * @param {?} v
 * @return {?}
 */
function _toStringWithNull(v) {
    return v != null ? v.toString() : '';
}
/** @type {?} */
export const EMPTY_ARRAY = [];
/** @type {?} */
export const EMPTY_MAP = {};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxZQUFZLEVBQUUsWUFBWSxFQUFDLE1BQU0sc0NBQXNDLENBQUM7QUFDaEYsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLDhCQUE4QixDQUFDO0FBQ3BELE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLGtCQUFrQixDQUFDO0FBRW5ELE9BQU8sRUFBQyxjQUFjLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUNsRCxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDNUMsT0FBTyxFQUFDLDJDQUEyQyxFQUFDLE1BQU0sVUFBVSxDQUFDO0FBQ3JFLE9BQU8sRUFBNkgsUUFBUSxFQUF5RSxhQUFhLEVBQUUsVUFBVSxFQUFDLE1BQU0sU0FBUyxDQUFDOztBQUUvUCxNQUFNLE9BQU8sSUFBSTs7O0FBQVEsR0FBRyxFQUFFLEdBQUUsQ0FBQyxDQUFBOztNQUUzQixjQUFjLEdBQUcsSUFBSSxHQUFHLEVBQWU7Ozs7O0FBRTdDLE1BQU0sVUFBVSxRQUFRLENBQUMsS0FBVTs7UUFDN0IsR0FBRyxHQUFHLGNBQWMsQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDO0lBQ25DLElBQUksQ0FBQyxHQUFHLEVBQUU7UUFDUixHQUFHLEdBQUcsU0FBUyxDQUFDLEtBQUssQ0FBQyxHQUFHLEdBQUcsR0FBRyxjQUFjLENBQUMsSUFBSSxDQUFDO1FBQ25ELGNBQWMsQ0FBQyxHQUFHLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxDQUFDO0tBQ2hDO0lBQ0QsT0FBTyxHQUFHLENBQUM7QUFDYixDQUFDOzs7Ozs7OztBQUVELE1BQU0sVUFBVSxXQUFXLENBQUMsSUFBYyxFQUFFLE9BQWUsRUFBRSxVQUFrQixFQUFFLEtBQVU7SUFD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QUN2QyxDQUFDOztBQUVELE1BQU0sT0FBTyxXQUFXLEdBQVUsRUFBRTs7QUFDcEMsTUFBTSxPQUFPLFNBQVMsR0FBeUIsRUFBRSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtXcmFwcGVkVmFsdWUsIGRldk1vZGVFcXVhbH0gZnJvbSAnLi4vY2hhbmdlX2RldGVjdGlvbi9jaGFuZ2VfZGV0ZWN0aW9uJztcbmltcG9ydCB7U09VUkNFfSBmcm9tICcuLi9kaS9pbmplY3Rvcl9jb21wYXRpYmlsaXR5JztcbmltcG9ydCB7Vmlld0VuY2Fwc3VsYXRpb259IGZyb20gJy4uL21ldGFkYXRhL3ZpZXcnO1xuaW1wb3J0IHtSZW5kZXJlclR5cGUyfSBmcm9tICcuLi9yZW5kZXIvYXBpJztcbmltcG9ydCB7bG9vc2VJZGVudGljYWx9IGZyb20gJy4uL3V0aWwvY29tcGFyaXNvbic7XG5pbXBvcnQge3N0cmluZ2lmeX0gZnJvbSAnLi4vdXRpbC9zdHJpbmdpZnknO1xuaW1wb3J0IHtleHByZXNzaW9uQ2hhbmdlZEFmdGVySXRIYXNCZWVuQ2hlY2tlZEVycm9yfSBmcm9tICcuL2Vycm9ycyc7XG5pbXBvcnQge0JpbmRpbmdEZWYsIEJpbmRpbmdGbGFncywgRGVmaW5pdGlvbiwgRGVmaW5pdGlvbkZhY3RvcnksIERlcERlZiwgRGVwRmxhZ3MsIEVsZW1lbnREYXRhLCBOb2RlRGVmLCBOb2RlRmxhZ3MsIFF1ZXJ5VmFsdWVUeXBlLCBTZXJ2aWNlcywgVmlld0RhdGEsIFZpZXdEZWZpbml0aW9uLCBWaWV3RGVmaW5pdGlvbkZhY3RvcnksIFZpZXdGbGFncywgVmlld1N0YXRlLCBhc0VsZW1lbnREYXRhLCBhc1RleHREYXRhfSBmcm9tICcuL3R5cGVzJztcblxuZXhwb3J0IGNvbnN0IE5PT1A6IGFueSA9ICgpID0+IHt9O1xuXG5jb25zdCBfdG9rZW5LZXlDYWNoZSA9IG5ldyBNYXA8YW55LCBzdHJpbmc+KCk7XG5cbmV4cG9ydCBmdW5jdGlvbiB0b2tlbktleSh0b2tlbjogYW55KTogc3RyaW5nIHtcbiAgbGV0IGtleSA9IF90b2tlbktleUNhY2hlLmdldCh0b2tlbik7XG4gIGlmICgha2V5KSB7XG4gICAga2V5ID0gc3RyaW5naWZ5KHRva2VuKSArICdfJyArIF90b2tlbktleUNhY2hlLnNpemU7XG4gICAgX3Rva2VuS2V5Q2FjaGUuc2V0KHRva2VuLCBrZXkpO1xuICB9XG4gIHJldHVybiBrZXk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB1bndyYXBWYWx1ZSh2aWV3OiBWaWV3RGF0YSwgbm9kZUlkeDogbnVtYmVyLCBiaW5kaW5nSWR4OiBudW1iZXIsIHZhbHVlOiBhbnkpOiBhbnkge1xuICBpZiAoV3JhcHBlZFZhbHVlLmlzV3JhcHBlZCh2YWx1ZSkpIHtcbiAgICB2YWx1ZSA9IFdyYXBwZWRWYWx1ZS51bndyYXAodmFsdWUpO1xuICAgIGNvbnN0IGdsb2JhbEJpbmRpbmdJZHggPSB2aWV3LmRlZi5ub2Rlc1tub2RlSWR4XS5iaW5kaW5nSW5kZXggKyBiaW5kaW5nSWR4O1xuICAgIGNvbnN0IG9sZFZhbHVlID0gV3JhcHBlZFZhbHVlLnVud3JhcCh2aWV3Lm9sZFZhbHVlc1tnbG9iYWxCaW5kaW5nSWR4XSk7XG4gICAgdmlldy5vbGRWYWx1ZXNbZ2xvYmFsQmluZGluZ0lkeF0gPSBuZXcgV3JhcHBlZFZhbHVlKG9sZFZhbHVlKTtcbiAgfVxuICByZXR1cm4gdmFsdWU7XG59XG5cbmNvbnN0IFVOREVGSU5FRF9SRU5ERVJFUl9UWVBFX0lEID0gJyQkdW5kZWZpbmVkJztcbmNvbnN0IEVNUFRZX1JFTkRFUkVSX1RZUEVfSUQgPSAnJCRlbXB0eSc7XG5cbi8vIEF0dGVudGlvbjogdGhpcyBmdW5jdGlvbiBpcyBjYWxsZWQgYXMgdG9wIGxldmVsIGZ1bmN0aW9uLlxuLy8gUHV0dGluZyBhbnkgbG9naWMgaW4gaGVyZSB3aWxsIGRlc3Ryb3kgY2xvc3VyZSB0cmVlIHNoYWtpbmchXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlUmVuZGVyZXJUeXBlMih2YWx1ZXM6IHtcbiAgc3R5bGVzOiAoc3RyaW5nIHwgYW55W10pW10sXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLFxuICBkYXRhOiB7W2tpbmQ6IHN0cmluZ106IGFueVtdfVxufSk6IFJlbmRlcmVyVHlwZTIge1xuICByZXR1cm4ge1xuICAgIGlkOiBVTkRFRklORURfUkVOREVSRVJfVFlQRV9JRCxcbiAgICBzdHlsZXM6IHZhbHVlcy5zdHlsZXMsXG4gICAgZW5jYXBzdWxhdGlvbjogdmFsdWVzLmVuY2Fwc3VsYXRpb24sXG4gICAgZGF0YTogdmFsdWVzLmRhdGFcbiAgfTtcbn1cblxubGV0IF9yZW5kZXJDb21wQ291bnQgPSAwO1xuXG5leHBvcnQgZnVuY3Rpb24gcmVzb2x2ZVJlbmRlcmVyVHlwZTIodHlwZT86IFJlbmRlcmVyVHlwZTIgfCBudWxsKTogUmVuZGVyZXJUeXBlMnxudWxsIHtcbiAgaWYgKHR5cGUgJiYgdHlwZS5pZCA9PT0gVU5ERUZJTkVEX1JFTkRFUkVSX1RZUEVfSUQpIHtcbiAgICAvLyBmaXJzdCB0aW1lIHdlIHNlZSB0aGlzIFJlbmRlcmVyVHlwZTIuIEluaXRpYWxpemUgaXQuLi5cbiAgICBjb25zdCBpc0ZpbGxlZCA9XG4gICAgICAgICgodHlwZS5lbmNhcHN1bGF0aW9uICE9IG51bGwgJiYgdHlwZS5lbmNhcHN1bGF0aW9uICE9PSBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lKSB8fFxuICAgICAgICAgdHlwZS5zdHlsZXMubGVuZ3RoIHx8IE9iamVjdC5rZXlzKHR5cGUuZGF0YSkubGVuZ3RoKTtcbiAgICBpZiAoaXNGaWxsZWQpIHtcbiAgICAgIHR5cGUuaWQgPSBgYyR7X3JlbmRlckNvbXBDb3VudCsrfWA7XG4gICAgfSBlbHNlIHtcbiAgICAgIHR5cGUuaWQgPSBFTVBUWV9SRU5ERVJFUl9UWVBFX0lEO1xuICAgIH1cbiAgfVxuICBpZiAodHlwZSAmJiB0eXBlLmlkID09PSBFTVBUWV9SRU5ERVJFUl9UWVBFX0lEKSB7XG4gICAgdHlwZSA9IG51bGw7XG4gIH1cbiAgcmV0dXJuIHR5cGUgfHwgbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNoZWNrQmluZGluZyhcbiAgICB2aWV3OiBWaWV3RGF0YSwgZGVmOiBOb2RlRGVmLCBiaW5kaW5nSWR4OiBudW1iZXIsIHZhbHVlOiBhbnkpOiBib29sZWFuIHtcbiAgY29uc3Qgb2xkVmFsdWVzID0gdmlldy5vbGRWYWx1ZXM7XG4gIGlmICgodmlldy5zdGF0ZSAmIFZpZXdTdGF0ZS5GaXJzdENoZWNrKSB8fFxuICAgICAgIWxvb3NlSWRlbnRpY2FsKG9sZFZhbHVlc1tkZWYuYmluZGluZ0luZGV4ICsgYmluZGluZ0lkeF0sIHZhbHVlKSkge1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNoZWNrQW5kVXBkYXRlQmluZGluZyhcbiAgICB2aWV3OiBWaWV3RGF0YSwgZGVmOiBOb2RlRGVmLCBiaW5kaW5nSWR4OiBudW1iZXIsIHZhbHVlOiBhbnkpOiBib29sZWFuIHtcbiAgaWYgKGNoZWNrQmluZGluZyh2aWV3LCBkZWYsIGJpbmRpbmdJZHgsIHZhbHVlKSkge1xuICAgIHZpZXcub2xkVmFsdWVzW2RlZi5iaW5kaW5nSW5kZXggKyBiaW5kaW5nSWR4XSA9IHZhbHVlO1xuICAgIHJldHVybiB0cnVlO1xuICB9XG4gIHJldHVybiBmYWxzZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNoZWNrQmluZGluZ05vQ2hhbmdlcyhcbiAgICB2aWV3OiBWaWV3RGF0YSwgZGVmOiBOb2RlRGVmLCBiaW5kaW5nSWR4OiBudW1iZXIsIHZhbHVlOiBhbnkpIHtcbiAgY29uc3Qgb2xkVmFsdWUgPSB2aWV3Lm9sZFZhbHVlc1tkZWYuYmluZGluZ0luZGV4ICsgYmluZGluZ0lkeF07XG4gIGlmICgodmlldy5zdGF0ZSAmIFZpZXdTdGF0ZS5CZWZvcmVGaXJzdENoZWNrKSB8fCAhZGV2TW9kZUVxdWFsKG9sZFZhbHVlLCB2YWx1ZSkpIHtcbiAgICBjb25zdCBiaW5kaW5nTmFtZSA9IGRlZi5iaW5kaW5nc1tiaW5kaW5nSWR4XS5uYW1lO1xuICAgIHRocm93IGV4cHJlc3Npb25DaGFuZ2VkQWZ0ZXJJdEhhc0JlZW5DaGVja2VkRXJyb3IoXG4gICAgICAgIFNlcnZpY2VzLmNyZWF0ZURlYnVnQ29udGV4dCh2aWV3LCBkZWYubm9kZUluZGV4KSwgYCR7YmluZGluZ05hbWV9OiAke29sZFZhbHVlfWAsXG4gICAgICAgIGAke2JpbmRpbmdOYW1lfTogJHt2YWx1ZX1gLCAodmlldy5zdGF0ZSAmIFZpZXdTdGF0ZS5CZWZvcmVGaXJzdENoZWNrKSAhPT0gMCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG1hcmtQYXJlbnRWaWV3c0ZvckNoZWNrKHZpZXc6IFZpZXdEYXRhKSB7XG4gIGxldCBjdXJyVmlldzogVmlld0RhdGF8bnVsbCA9IHZpZXc7XG4gIHdoaWxlIChjdXJyVmlldykge1xuICAgIGlmIChjdXJyVmlldy5kZWYuZmxhZ3MgJiBWaWV3RmxhZ3MuT25QdXNoKSB7XG4gICAgICBjdXJyVmlldy5zdGF0ZSB8PSBWaWV3U3RhdGUuQ2hlY2tzRW5hYmxlZDtcbiAgICB9XG4gICAgY3VyclZpZXcgPSBjdXJyVmlldy52aWV3Q29udGFpbmVyUGFyZW50IHx8IGN1cnJWaWV3LnBhcmVudDtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gbWFya1BhcmVudFZpZXdzRm9yQ2hlY2tQcm9qZWN0ZWRWaWV3cyh2aWV3OiBWaWV3RGF0YSwgZW5kVmlldzogVmlld0RhdGEpIHtcbiAgbGV0IGN1cnJWaWV3OiBWaWV3RGF0YXxudWxsID0gdmlldztcbiAgd2hpbGUgKGN1cnJWaWV3ICYmIGN1cnJWaWV3ICE9PSBlbmRWaWV3KSB7XG4gICAgY3VyclZpZXcuc3RhdGUgfD0gVmlld1N0YXRlLkNoZWNrUHJvamVjdGVkVmlld3M7XG4gICAgY3VyclZpZXcgPSBjdXJyVmlldy52aWV3Q29udGFpbmVyUGFyZW50IHx8IGN1cnJWaWV3LnBhcmVudDtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZGlzcGF0Y2hFdmVudChcbiAgICB2aWV3OiBWaWV3RGF0YSwgbm9kZUluZGV4OiBudW1iZXIsIGV2ZW50TmFtZTogc3RyaW5nLCBldmVudDogYW55KTogYm9vbGVhbnx1bmRlZmluZWQge1xuICB0cnkge1xuICAgIGNvbnN0IG5vZGVEZWYgPSB2aWV3LmRlZi5ub2Rlc1tub2RlSW5kZXhdO1xuICAgIGNvbnN0IHN0YXJ0VmlldyA9IG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuQ29tcG9uZW50VmlldyA/XG4gICAgICAgIGFzRWxlbWVudERhdGEodmlldywgbm9kZUluZGV4KS5jb21wb25lbnRWaWV3IDpcbiAgICAgICAgdmlldztcbiAgICBtYXJrUGFyZW50Vmlld3NGb3JDaGVjayhzdGFydFZpZXcpO1xuICAgIHJldHVybiBTZXJ2aWNlcy5oYW5kbGVFdmVudCh2aWV3LCBub2RlSW5kZXgsIGV2ZW50TmFtZSwgZXZlbnQpO1xuICB9IGNhdGNoIChlKSB7XG4gICAgLy8gQXR0ZW50aW9uOiBEb24ndCByZXRocm93LCBhcyBpdCB3b3VsZCBjYW5jZWwgT2JzZXJ2YWJsZSBzdWJzY3JpcHRpb25zIVxuICAgIHZpZXcucm9vdC5lcnJvckhhbmRsZXIuaGFuZGxlRXJyb3IoZSk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlY2xhcmVkVmlld0NvbnRhaW5lcih2aWV3OiBWaWV3RGF0YSk6IEVsZW1lbnREYXRhfG51bGwge1xuICBpZiAodmlldy5wYXJlbnQpIHtcbiAgICBjb25zdCBwYXJlbnRWaWV3ID0gdmlldy5wYXJlbnQ7XG4gICAgcmV0dXJuIGFzRWxlbWVudERhdGEocGFyZW50Vmlldywgdmlldy5wYXJlbnROb2RlRGVmICEubm9kZUluZGV4KTtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuLyoqXG4gKiBmb3IgY29tcG9uZW50IHZpZXdzLCB0aGlzIGlzIHRoZSBob3N0IGVsZW1lbnQuXG4gKiBmb3IgZW1iZWRkZWQgdmlld3MsIHRoaXMgaXMgdGhlIGluZGV4IG9mIHRoZSBwYXJlbnQgbm9kZVxuICogdGhhdCBjb250YWlucyB0aGUgdmlldyBjb250YWluZXIuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiB2aWV3UGFyZW50RWwodmlldzogVmlld0RhdGEpOiBOb2RlRGVmfG51bGwge1xuICBjb25zdCBwYXJlbnRWaWV3ID0gdmlldy5wYXJlbnQ7XG4gIGlmIChwYXJlbnRWaWV3KSB7XG4gICAgcmV0dXJuIHZpZXcucGFyZW50Tm9kZURlZiAhLnBhcmVudDtcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcmVuZGVyTm9kZSh2aWV3OiBWaWV3RGF0YSwgZGVmOiBOb2RlRGVmKTogYW55IHtcbiAgc3dpdGNoIChkZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuVHlwZXMpIHtcbiAgICBjYXNlIE5vZGVGbGFncy5UeXBlRWxlbWVudDpcbiAgICAgIHJldHVybiBhc0VsZW1lbnREYXRhKHZpZXcsIGRlZi5ub2RlSW5kZXgpLnJlbmRlckVsZW1lbnQ7XG4gICAgY2FzZSBOb2RlRmxhZ3MuVHlwZVRleHQ6XG4gICAgICByZXR1cm4gYXNUZXh0RGF0YSh2aWV3LCBkZWYubm9kZUluZGV4KS5yZW5kZXJUZXh0O1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBlbGVtZW50RXZlbnRGdWxsTmFtZSh0YXJnZXQ6IHN0cmluZyB8IG51bGwsIG5hbWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiB0YXJnZXQgPyBgJHt0YXJnZXR9OiR7bmFtZX1gIDogbmFtZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQ29tcG9uZW50Vmlldyh2aWV3OiBWaWV3RGF0YSk6IGJvb2xlYW4ge1xuICByZXR1cm4gISF2aWV3LnBhcmVudCAmJiAhISh2aWV3LnBhcmVudE5vZGVEZWYgIS5mbGFncyAmIE5vZGVGbGFncy5Db21wb25lbnQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNFbWJlZGRlZFZpZXcodmlldzogVmlld0RhdGEpOiBib29sZWFuIHtcbiAgcmV0dXJuICEhdmlldy5wYXJlbnQgJiYgISh2aWV3LnBhcmVudE5vZGVEZWYgIS5mbGFncyAmIE5vZGVGbGFncy5Db21wb25lbnQpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZmlsdGVyUXVlcnlJZChxdWVyeUlkOiBudW1iZXIpOiBudW1iZXIge1xuICByZXR1cm4gMSA8PCAocXVlcnlJZCAlIDMyKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNwbGl0TWF0Y2hlZFF1ZXJpZXNEc2woXG4gICAgbWF0Y2hlZFF1ZXJpZXNEc2w6IFtzdHJpbmcgfCBudW1iZXIsIFF1ZXJ5VmFsdWVUeXBlXVtdIHwgbnVsbCk6IHtcbiAgbWF0Y2hlZFF1ZXJpZXM6IHtbcXVlcnlJZDogc3RyaW5nXTogUXVlcnlWYWx1ZVR5cGV9LFxuICByZWZlcmVuY2VzOiB7W3JlZklkOiBzdHJpbmddOiBRdWVyeVZhbHVlVHlwZX0sXG4gIG1hdGNoZWRRdWVyeUlkczogbnVtYmVyXG59IHtcbiAgY29uc3QgbWF0Y2hlZFF1ZXJpZXM6IHtbcXVlcnlJZDogc3RyaW5nXTogUXVlcnlWYWx1ZVR5cGV9ID0ge307XG4gIGxldCBtYXRjaGVkUXVlcnlJZHMgPSAwO1xuICBjb25zdCByZWZlcmVuY2VzOiB7W3JlZklkOiBzdHJpbmddOiBRdWVyeVZhbHVlVHlwZX0gPSB7fTtcbiAgaWYgKG1hdGNoZWRRdWVyaWVzRHNsKSB7XG4gICAgbWF0Y2hlZFF1ZXJpZXNEc2wuZm9yRWFjaCgoW3F1ZXJ5SWQsIHZhbHVlVHlwZV0pID0+IHtcbiAgICAgIGlmICh0eXBlb2YgcXVlcnlJZCA9PT0gJ251bWJlcicpIHtcbiAgICAgICAgbWF0Y2hlZFF1ZXJpZXNbcXVlcnlJZF0gPSB2YWx1ZVR5cGU7XG4gICAgICAgIG1hdGNoZWRRdWVyeUlkcyB8PSBmaWx0ZXJRdWVyeUlkKHF1ZXJ5SWQpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmVmZXJlbmNlc1txdWVyeUlkXSA9IHZhbHVlVHlwZTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuICByZXR1cm4ge21hdGNoZWRRdWVyaWVzLCByZWZlcmVuY2VzLCBtYXRjaGVkUXVlcnlJZHN9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc3BsaXREZXBzRHNsKGRlcHM6IChbRGVwRmxhZ3MsIGFueV0gfCBhbnkpW10sIHNvdXJjZU5hbWU/OiBzdHJpbmcpOiBEZXBEZWZbXSB7XG4gIHJldHVybiBkZXBzLm1hcCh2YWx1ZSA9PiB7XG4gICAgbGV0IHRva2VuOiBhbnk7XG4gICAgbGV0IGZsYWdzOiBEZXBGbGFncztcbiAgICBpZiAoQXJyYXkuaXNBcnJheSh2YWx1ZSkpIHtcbiAgICAgIFtmbGFncywgdG9rZW5dID0gdmFsdWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIGZsYWdzID0gRGVwRmxhZ3MuTm9uZTtcbiAgICAgIHRva2VuID0gdmFsdWU7XG4gICAgfVxuICAgIGlmICh0b2tlbiAmJiAodHlwZW9mIHRva2VuID09PSAnZnVuY3Rpb24nIHx8IHR5cGVvZiB0b2tlbiA9PT0gJ29iamVjdCcpICYmIHNvdXJjZU5hbWUpIHtcbiAgICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0b2tlbiwgU09VUkNFLCB7dmFsdWU6IHNvdXJjZU5hbWUsIGNvbmZpZ3VyYWJsZTogdHJ1ZX0pO1xuICAgIH1cbiAgICByZXR1cm4ge2ZsYWdzLCB0b2tlbiwgdG9rZW5LZXk6IHRva2VuS2V5KHRva2VuKX07XG4gIH0pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UGFyZW50UmVuZGVyRWxlbWVudCh2aWV3OiBWaWV3RGF0YSwgcmVuZGVySG9zdDogYW55LCBkZWY6IE5vZGVEZWYpOiBhbnkge1xuICBsZXQgcmVuZGVyUGFyZW50ID0gZGVmLnJlbmRlclBhcmVudDtcbiAgaWYgKHJlbmRlclBhcmVudCkge1xuICAgIGlmICgocmVuZGVyUGFyZW50LmZsYWdzICYgTm9kZUZsYWdzLlR5cGVFbGVtZW50KSA9PT0gMCB8fFxuICAgICAgICAocmVuZGVyUGFyZW50LmZsYWdzICYgTm9kZUZsYWdzLkNvbXBvbmVudFZpZXcpID09PSAwIHx8XG4gICAgICAgIChyZW5kZXJQYXJlbnQuZWxlbWVudCAhLmNvbXBvbmVudFJlbmRlcmVyVHlwZSAmJlxuICAgICAgICAgcmVuZGVyUGFyZW50LmVsZW1lbnQgIS5jb21wb25lbnRSZW5kZXJlclR5cGUgIS5lbmNhcHN1bGF0aW9uID09PVxuICAgICAgICAgICAgIFZpZXdFbmNhcHN1bGF0aW9uLk5hdGl2ZSkpIHtcbiAgICAgIC8vIG9ubHkgY2hpbGRyZW4gb2Ygbm9uIGNvbXBvbmVudHMsIG9yIGNoaWxkcmVuIG9mIGNvbXBvbmVudHMgd2l0aCBuYXRpdmUgZW5jYXBzdWxhdGlvbiBzaG91bGRcbiAgICAgIC8vIGJlIGF0dGFjaGVkLlxuICAgICAgcmV0dXJuIGFzRWxlbWVudERhdGEodmlldywgZGVmLnJlbmRlclBhcmVudCAhLm5vZGVJbmRleCkucmVuZGVyRWxlbWVudDtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIHJlbmRlckhvc3Q7XG4gIH1cbn1cblxuY29uc3QgREVGSU5JVElPTl9DQUNIRSA9IG5ldyBXZWFrTWFwPGFueSwgRGVmaW5pdGlvbjxhbnk+PigpO1xuXG5leHBvcnQgZnVuY3Rpb24gcmVzb2x2ZURlZmluaXRpb248RCBleHRlbmRzIERlZmluaXRpb248YW55Pj4oZmFjdG9yeTogRGVmaW5pdGlvbkZhY3Rvcnk8RD4pOiBEIHtcbiAgbGV0IHZhbHVlID0gREVGSU5JVElPTl9DQUNIRS5nZXQoZmFjdG9yeSkgIWFzIEQ7XG4gIGlmICghdmFsdWUpIHtcbiAgICB2YWx1ZSA9IGZhY3RvcnkoKCkgPT4gTk9PUCk7XG4gICAgdmFsdWUuZmFjdG9yeSA9IGZhY3Rvcnk7XG4gICAgREVGSU5JVElPTl9DQUNIRS5zZXQoZmFjdG9yeSwgdmFsdWUpO1xuICB9XG4gIHJldHVybiB2YWx1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJvb3RSZW5kZXJOb2Rlcyh2aWV3OiBWaWV3RGF0YSk6IGFueVtdIHtcbiAgY29uc3QgcmVuZGVyTm9kZXM6IGFueVtdID0gW107XG4gIHZpc2l0Um9vdFJlbmRlck5vZGVzKHZpZXcsIFJlbmRlck5vZGVBY3Rpb24uQ29sbGVjdCwgdW5kZWZpbmVkLCB1bmRlZmluZWQsIHJlbmRlck5vZGVzKTtcbiAgcmV0dXJuIHJlbmRlck5vZGVzO1xufVxuXG5leHBvcnQgY29uc3QgZW51bSBSZW5kZXJOb2RlQWN0aW9uIHtDb2xsZWN0LCBBcHBlbmRDaGlsZCwgSW5zZXJ0QmVmb3JlLCBSZW1vdmVDaGlsZH1cblxuZXhwb3J0IGZ1bmN0aW9uIHZpc2l0Um9vdFJlbmRlck5vZGVzKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBhY3Rpb246IFJlbmRlck5vZGVBY3Rpb24sIHBhcmVudE5vZGU6IGFueSwgbmV4dFNpYmxpbmc6IGFueSwgdGFyZ2V0PzogYW55W10pIHtcbiAgLy8gV2UgbmVlZCB0byByZS1jb21wdXRlIHRoZSBwYXJlbnQgbm9kZSBpbiBjYXNlIHRoZSBub2RlcyBoYXZlIGJlZW4gbW92ZWQgYXJvdW5kIG1hbnVhbGx5XG4gIGlmIChhY3Rpb24gPT09IFJlbmRlck5vZGVBY3Rpb24uUmVtb3ZlQ2hpbGQpIHtcbiAgICBwYXJlbnROb2RlID0gdmlldy5yZW5kZXJlci5wYXJlbnROb2RlKHJlbmRlck5vZGUodmlldywgdmlldy5kZWYubGFzdFJlbmRlclJvb3ROb2RlICEpKTtcbiAgfVxuICB2aXNpdFNpYmxpbmdSZW5kZXJOb2RlcyhcbiAgICAgIHZpZXcsIGFjdGlvbiwgMCwgdmlldy5kZWYubm9kZXMubGVuZ3RoIC0gMSwgcGFyZW50Tm9kZSwgbmV4dFNpYmxpbmcsIHRhcmdldCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2aXNpdFNpYmxpbmdSZW5kZXJOb2RlcyhcbiAgICB2aWV3OiBWaWV3RGF0YSwgYWN0aW9uOiBSZW5kZXJOb2RlQWN0aW9uLCBzdGFydEluZGV4OiBudW1iZXIsIGVuZEluZGV4OiBudW1iZXIsIHBhcmVudE5vZGU6IGFueSxcbiAgICBuZXh0U2libGluZzogYW55LCB0YXJnZXQ/OiBhbnlbXSkge1xuICBmb3IgKGxldCBpID0gc3RhcnRJbmRleDsgaSA8PSBlbmRJbmRleDsgaSsrKSB7XG4gICAgY29uc3Qgbm9kZURlZiA9IHZpZXcuZGVmLm5vZGVzW2ldO1xuICAgIGlmIChub2RlRGVmLmZsYWdzICYgKE5vZGVGbGFncy5UeXBlRWxlbWVudCB8IE5vZGVGbGFncy5UeXBlVGV4dCB8IE5vZGVGbGFncy5UeXBlTmdDb250ZW50KSkge1xuICAgICAgdmlzaXRSZW5kZXJOb2RlKHZpZXcsIG5vZGVEZWYsIGFjdGlvbiwgcGFyZW50Tm9kZSwgbmV4dFNpYmxpbmcsIHRhcmdldCk7XG4gICAgfVxuICAgIC8vIGp1bXAgdG8gbmV4dCBzaWJsaW5nXG4gICAgaSArPSBub2RlRGVmLmNoaWxkQ291bnQ7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZpc2l0UHJvamVjdGVkUmVuZGVyTm9kZXMoXG4gICAgdmlldzogVmlld0RhdGEsIG5nQ29udGVudEluZGV4OiBudW1iZXIsIGFjdGlvbjogUmVuZGVyTm9kZUFjdGlvbiwgcGFyZW50Tm9kZTogYW55LFxuICAgIG5leHRTaWJsaW5nOiBhbnksIHRhcmdldD86IGFueVtdKSB7XG4gIGxldCBjb21wVmlldzogVmlld0RhdGF8bnVsbCA9IHZpZXc7XG4gIHdoaWxlIChjb21wVmlldyAmJiAhaXNDb21wb25lbnRWaWV3KGNvbXBWaWV3KSkge1xuICAgIGNvbXBWaWV3ID0gY29tcFZpZXcucGFyZW50O1xuICB9XG4gIGNvbnN0IGhvc3RWaWV3ID0gY29tcFZpZXcgIS5wYXJlbnQ7XG4gIGNvbnN0IGhvc3RFbERlZiA9IHZpZXdQYXJlbnRFbChjb21wVmlldyAhKTtcbiAgY29uc3Qgc3RhcnRJbmRleCA9IGhvc3RFbERlZiAhLm5vZGVJbmRleCArIDE7XG4gIGNvbnN0IGVuZEluZGV4ID0gaG9zdEVsRGVmICEubm9kZUluZGV4ICsgaG9zdEVsRGVmICEuY2hpbGRDb3VudDtcbiAgZm9yIChsZXQgaSA9IHN0YXJ0SW5kZXg7IGkgPD0gZW5kSW5kZXg7IGkrKykge1xuICAgIGNvbnN0IG5vZGVEZWYgPSBob3N0VmlldyAhLmRlZi5ub2Rlc1tpXTtcbiAgICBpZiAobm9kZURlZi5uZ0NvbnRlbnRJbmRleCA9PT0gbmdDb250ZW50SW5kZXgpIHtcbiAgICAgIHZpc2l0UmVuZGVyTm9kZShob3N0VmlldyAhLCBub2RlRGVmLCBhY3Rpb24sIHBhcmVudE5vZGUsIG5leHRTaWJsaW5nLCB0YXJnZXQpO1xuICAgIH1cbiAgICAvLyBqdW1wIHRvIG5leHQgc2libGluZ1xuICAgIGkgKz0gbm9kZURlZi5jaGlsZENvdW50O1xuICB9XG4gIGlmICghaG9zdFZpZXcgIS5wYXJlbnQpIHtcbiAgICAvLyBhIHJvb3Qgdmlld1xuICAgIGNvbnN0IHByb2plY3RlZE5vZGVzID0gdmlldy5yb290LnByb2plY3RhYmxlTm9kZXNbbmdDb250ZW50SW5kZXhdO1xuICAgIGlmIChwcm9qZWN0ZWROb2Rlcykge1xuICAgICAgZm9yIChsZXQgaSA9IDA7IGkgPCBwcm9qZWN0ZWROb2Rlcy5sZW5ndGg7IGkrKykge1xuICAgICAgICBleGVjUmVuZGVyTm9kZUFjdGlvbih2aWV3LCBwcm9qZWN0ZWROb2Rlc1tpXSwgYWN0aW9uLCBwYXJlbnROb2RlLCBuZXh0U2libGluZywgdGFyZ2V0KTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gdmlzaXRSZW5kZXJOb2RlKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBub2RlRGVmOiBOb2RlRGVmLCBhY3Rpb246IFJlbmRlck5vZGVBY3Rpb24sIHBhcmVudE5vZGU6IGFueSwgbmV4dFNpYmxpbmc6IGFueSxcbiAgICB0YXJnZXQ/OiBhbnlbXSkge1xuICBpZiAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlTmdDb250ZW50KSB7XG4gICAgdmlzaXRQcm9qZWN0ZWRSZW5kZXJOb2RlcyhcbiAgICAgICAgdmlldywgbm9kZURlZi5uZ0NvbnRlbnQgIS5pbmRleCwgYWN0aW9uLCBwYXJlbnROb2RlLCBuZXh0U2libGluZywgdGFyZ2V0KTtcbiAgfSBlbHNlIHtcbiAgICBjb25zdCBybiA9IHJlbmRlck5vZGUodmlldywgbm9kZURlZik7XG4gICAgaWYgKGFjdGlvbiA9PT0gUmVuZGVyTm9kZUFjdGlvbi5SZW1vdmVDaGlsZCAmJiAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5Db21wb25lbnRWaWV3KSAmJlxuICAgICAgICAobm9kZURlZi5iaW5kaW5nRmxhZ3MgJiBCaW5kaW5nRmxhZ3MuQ2F0U3ludGhldGljUHJvcGVydHkpKSB7XG4gICAgICAvLyBOb3RlOiB3ZSBtaWdodCBuZWVkIHRvIGRvIGJvdGggYWN0aW9ucy5cbiAgICAgIGlmIChub2RlRGVmLmJpbmRpbmdGbGFncyAmIChCaW5kaW5nRmxhZ3MuU3ludGhldGljUHJvcGVydHkpKSB7XG4gICAgICAgIGV4ZWNSZW5kZXJOb2RlQWN0aW9uKHZpZXcsIHJuLCBhY3Rpb24sIHBhcmVudE5vZGUsIG5leHRTaWJsaW5nLCB0YXJnZXQpO1xuICAgICAgfVxuICAgICAgaWYgKG5vZGVEZWYuYmluZGluZ0ZsYWdzICYgKEJpbmRpbmdGbGFncy5TeW50aGV0aWNIb3N0UHJvcGVydHkpKSB7XG4gICAgICAgIGNvbnN0IGNvbXBWaWV3ID0gYXNFbGVtZW50RGF0YSh2aWV3LCBub2RlRGVmLm5vZGVJbmRleCkuY29tcG9uZW50VmlldztcbiAgICAgICAgZXhlY1JlbmRlck5vZGVBY3Rpb24oY29tcFZpZXcsIHJuLCBhY3Rpb24sIHBhcmVudE5vZGUsIG5leHRTaWJsaW5nLCB0YXJnZXQpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBleGVjUmVuZGVyTm9kZUFjdGlvbih2aWV3LCBybiwgYWN0aW9uLCBwYXJlbnROb2RlLCBuZXh0U2libGluZywgdGFyZ2V0KTtcbiAgICB9XG4gICAgaWYgKG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuRW1iZWRkZWRWaWV3cykge1xuICAgICAgY29uc3QgZW1iZWRkZWRWaWV3cyA9IGFzRWxlbWVudERhdGEodmlldywgbm9kZURlZi5ub2RlSW5kZXgpLnZpZXdDb250YWluZXIgIS5fZW1iZWRkZWRWaWV3cztcbiAgICAgIGZvciAobGV0IGsgPSAwOyBrIDwgZW1iZWRkZWRWaWV3cy5sZW5ndGg7IGsrKykge1xuICAgICAgICB2aXNpdFJvb3RSZW5kZXJOb2RlcyhlbWJlZGRlZFZpZXdzW2tdLCBhY3Rpb24sIHBhcmVudE5vZGUsIG5leHRTaWJsaW5nLCB0YXJnZXQpO1xuICAgICAgfVxuICAgIH1cbiAgICBpZiAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlRWxlbWVudCAmJiAhbm9kZURlZi5lbGVtZW50ICEubmFtZSkge1xuICAgICAgdmlzaXRTaWJsaW5nUmVuZGVyTm9kZXMoXG4gICAgICAgICAgdmlldywgYWN0aW9uLCBub2RlRGVmLm5vZGVJbmRleCArIDEsIG5vZGVEZWYubm9kZUluZGV4ICsgbm9kZURlZi5jaGlsZENvdW50LCBwYXJlbnROb2RlLFxuICAgICAgICAgIG5leHRTaWJsaW5nLCB0YXJnZXQpO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBleGVjUmVuZGVyTm9kZUFjdGlvbihcbiAgICB2aWV3OiBWaWV3RGF0YSwgcmVuZGVyTm9kZTogYW55LCBhY3Rpb246IFJlbmRlck5vZGVBY3Rpb24sIHBhcmVudE5vZGU6IGFueSwgbmV4dFNpYmxpbmc6IGFueSxcbiAgICB0YXJnZXQ/OiBhbnlbXSkge1xuICBjb25zdCByZW5kZXJlciA9IHZpZXcucmVuZGVyZXI7XG4gIHN3aXRjaCAoYWN0aW9uKSB7XG4gICAgY2FzZSBSZW5kZXJOb2RlQWN0aW9uLkFwcGVuZENoaWxkOlxuICAgICAgcmVuZGVyZXIuYXBwZW5kQ2hpbGQocGFyZW50Tm9kZSwgcmVuZGVyTm9kZSk7XG4gICAgICBicmVhaztcbiAgICBjYXNlIFJlbmRlck5vZGVBY3Rpb24uSW5zZXJ0QmVmb3JlOlxuICAgICAgcmVuZGVyZXIuaW5zZXJ0QmVmb3JlKHBhcmVudE5vZGUsIHJlbmRlck5vZGUsIG5leHRTaWJsaW5nKTtcbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgUmVuZGVyTm9kZUFjdGlvbi5SZW1vdmVDaGlsZDpcbiAgICAgIHJlbmRlcmVyLnJlbW92ZUNoaWxkKHBhcmVudE5vZGUsIHJlbmRlck5vZGUpO1xuICAgICAgYnJlYWs7XG4gICAgY2FzZSBSZW5kZXJOb2RlQWN0aW9uLkNvbGxlY3Q6XG4gICAgICB0YXJnZXQgIS5wdXNoKHJlbmRlck5vZGUpO1xuICAgICAgYnJlYWs7XG4gIH1cbn1cblxuY29uc3QgTlNfUFJFRklYX1JFID0gL146KFteOl0rKTooLispJC87XG5cbmV4cG9ydCBmdW5jdGlvbiBzcGxpdE5hbWVzcGFjZShuYW1lOiBzdHJpbmcpOiBzdHJpbmdbXSB7XG4gIGlmIChuYW1lWzBdID09PSAnOicpIHtcbiAgICBjb25zdCBtYXRjaCA9IG5hbWUubWF0Y2goTlNfUFJFRklYX1JFKSAhO1xuICAgIHJldHVybiBbbWF0Y2hbMV0sIG1hdGNoWzJdXTtcbiAgfVxuICByZXR1cm4gWycnLCBuYW1lXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbGNCaW5kaW5nRmxhZ3MoYmluZGluZ3M6IEJpbmRpbmdEZWZbXSk6IEJpbmRpbmdGbGFncyB7XG4gIGxldCBmbGFncyA9IDA7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmluZGluZ3MubGVuZ3RoOyBpKyspIHtcbiAgICBmbGFncyB8PSBiaW5kaW5nc1tpXS5mbGFncztcbiAgfVxuICByZXR1cm4gZmxhZ3M7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbnRlcnBvbGF0ZSh2YWx1ZUNvdW50OiBudW1iZXIsIGNvbnN0QW5kSW50ZXJwOiBzdHJpbmdbXSk6IHN0cmluZyB7XG4gIGxldCByZXN1bHQgPSAnJztcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCB2YWx1ZUNvdW50ICogMjsgaSA9IGkgKyAyKSB7XG4gICAgcmVzdWx0ID0gcmVzdWx0ICsgY29uc3RBbmRJbnRlcnBbaV0gKyBfdG9TdHJpbmdXaXRoTnVsbChjb25zdEFuZEludGVycFtpICsgMV0pO1xuICB9XG4gIHJldHVybiByZXN1bHQgKyBjb25zdEFuZEludGVycFt2YWx1ZUNvdW50ICogMl07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpbmxpbmVJbnRlcnBvbGF0ZShcbiAgICB2YWx1ZUNvdW50OiBudW1iZXIsIGMwOiBzdHJpbmcsIGExOiBhbnksIGMxOiBzdHJpbmcsIGEyPzogYW55LCBjMj86IHN0cmluZywgYTM/OiBhbnksXG4gICAgYzM/OiBzdHJpbmcsIGE0PzogYW55LCBjND86IHN0cmluZywgYTU/OiBhbnksIGM1Pzogc3RyaW5nLCBhNj86IGFueSwgYzY/OiBzdHJpbmcsIGE3PzogYW55LFxuICAgIGM3Pzogc3RyaW5nLCBhOD86IGFueSwgYzg/OiBzdHJpbmcsIGE5PzogYW55LCBjOT86IHN0cmluZyk6IHN0cmluZyB7XG4gIHN3aXRjaCAodmFsdWVDb3VudCkge1xuICAgIGNhc2UgMTpcbiAgICAgIHJldHVybiBjMCArIF90b1N0cmluZ1dpdGhOdWxsKGExKSArIGMxO1xuICAgIGNhc2UgMjpcbiAgICAgIHJldHVybiBjMCArIF90b1N0cmluZ1dpdGhOdWxsKGExKSArIGMxICsgX3RvU3RyaW5nV2l0aE51bGwoYTIpICsgYzI7XG4gICAgY2FzZSAzOlxuICAgICAgcmV0dXJuIGMwICsgX3RvU3RyaW5nV2l0aE51bGwoYTEpICsgYzEgKyBfdG9TdHJpbmdXaXRoTnVsbChhMikgKyBjMiArIF90b1N0cmluZ1dpdGhOdWxsKGEzKSArXG4gICAgICAgICAgYzM7XG4gICAgY2FzZSA0OlxuICAgICAgcmV0dXJuIGMwICsgX3RvU3RyaW5nV2l0aE51bGwoYTEpICsgYzEgKyBfdG9TdHJpbmdXaXRoTnVsbChhMikgKyBjMiArIF90b1N0cmluZ1dpdGhOdWxsKGEzKSArXG4gICAgICAgICAgYzMgKyBfdG9TdHJpbmdXaXRoTnVsbChhNCkgKyBjNDtcbiAgICBjYXNlIDU6XG4gICAgICByZXR1cm4gYzAgKyBfdG9TdHJpbmdXaXRoTnVsbChhMSkgKyBjMSArIF90b1N0cmluZ1dpdGhOdWxsKGEyKSArIGMyICsgX3RvU3RyaW5nV2l0aE51bGwoYTMpICtcbiAgICAgICAgICBjMyArIF90b1N0cmluZ1dpdGhOdWxsKGE0KSArIGM0ICsgX3RvU3RyaW5nV2l0aE51bGwoYTUpICsgYzU7XG4gICAgY2FzZSA2OlxuICAgICAgcmV0dXJuIGMwICsgX3RvU3RyaW5nV2l0aE51bGwoYTEpICsgYzEgKyBfdG9TdHJpbmdXaXRoTnVsbChhMikgKyBjMiArIF90b1N0cmluZ1dpdGhOdWxsKGEzKSArXG4gICAgICAgICAgYzMgKyBfdG9TdHJpbmdXaXRoTnVsbChhNCkgKyBjNCArIF90b1N0cmluZ1dpdGhOdWxsKGE1KSArIGM1ICsgX3RvU3RyaW5nV2l0aE51bGwoYTYpICsgYzY7XG4gICAgY2FzZSA3OlxuICAgICAgcmV0dXJuIGMwICsgX3RvU3RyaW5nV2l0aE51bGwoYTEpICsgYzEgKyBfdG9TdHJpbmdXaXRoTnVsbChhMikgKyBjMiArIF90b1N0cmluZ1dpdGhOdWxsKGEzKSArXG4gICAgICAgICAgYzMgKyBfdG9TdHJpbmdXaXRoTnVsbChhNCkgKyBjNCArIF90b1N0cmluZ1dpdGhOdWxsKGE1KSArIGM1ICsgX3RvU3RyaW5nV2l0aE51bGwoYTYpICtcbiAgICAgICAgICBjNiArIF90b1N0cmluZ1dpdGhOdWxsKGE3KSArIGM3O1xuICAgIGNhc2UgODpcbiAgICAgIHJldHVybiBjMCArIF90b1N0cmluZ1dpdGhOdWxsKGExKSArIGMxICsgX3RvU3RyaW5nV2l0aE51bGwoYTIpICsgYzIgKyBfdG9TdHJpbmdXaXRoTnVsbChhMykgK1xuICAgICAgICAgIGMzICsgX3RvU3RyaW5nV2l0aE51bGwoYTQpICsgYzQgKyBfdG9TdHJpbmdXaXRoTnVsbChhNSkgKyBjNSArIF90b1N0cmluZ1dpdGhOdWxsKGE2KSArXG4gICAgICAgICAgYzYgKyBfdG9TdHJpbmdXaXRoTnVsbChhNykgKyBjNyArIF90b1N0cmluZ1dpdGhOdWxsKGE4KSArIGM4O1xuICAgIGNhc2UgOTpcbiAgICAgIHJldHVybiBjMCArIF90b1N0cmluZ1dpdGhOdWxsKGExKSArIGMxICsgX3RvU3RyaW5nV2l0aE51bGwoYTIpICsgYzIgKyBfdG9TdHJpbmdXaXRoTnVsbChhMykgK1xuICAgICAgICAgIGMzICsgX3RvU3RyaW5nV2l0aE51bGwoYTQpICsgYzQgKyBfdG9TdHJpbmdXaXRoTnVsbChhNSkgKyBjNSArIF90b1N0cmluZ1dpdGhOdWxsKGE2KSArXG4gICAgICAgICAgYzYgKyBfdG9TdHJpbmdXaXRoTnVsbChhNykgKyBjNyArIF90b1N0cmluZ1dpdGhOdWxsKGE4KSArIGM4ICsgX3RvU3RyaW5nV2l0aE51bGwoYTkpICsgYzk7XG4gICAgZGVmYXVsdDpcbiAgICAgIHRocm93IG5ldyBFcnJvcihgRG9lcyBub3Qgc3VwcG9ydCBtb3JlIHRoYW4gOSBleHByZXNzaW9uc2ApO1xuICB9XG59XG5cbmZ1bmN0aW9uIF90b1N0cmluZ1dpdGhOdWxsKHY6IGFueSk6IHN0cmluZyB7XG4gIHJldHVybiB2ICE9IG51bGwgPyB2LnRvU3RyaW5nKCkgOiAnJztcbn1cblxuZXhwb3J0IGNvbnN0IEVNUFRZX0FSUkFZOiBhbnlbXSA9IFtdO1xuZXhwb3J0IGNvbnN0IEVNUFRZX01BUDoge1trZXk6IHN0cmluZ106IGFueX0gPSB7fTtcbiJdfQ==