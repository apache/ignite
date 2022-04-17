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
import { ViewEncapsulation } from '../metadata/view';
import { SecurityContext } from '../sanitization/security';
import { asElementData } from './types';
import { NOOP, calcBindingFlags, checkAndUpdateBinding, dispatchEvent, elementEventFullName, getParentRenderElement, resolveDefinition, resolveRendererType2, splitMatchedQueriesDsl, splitNamespace } from './util';
/**
 * @param {?} flags
 * @param {?} matchedQueriesDsl
 * @param {?} ngContentIndex
 * @param {?} childCount
 * @param {?=} handleEvent
 * @param {?=} templateFactory
 * @return {?}
 */
export function anchorDef(flags, matchedQueriesDsl, ngContentIndex, childCount, handleEvent, templateFactory) {
    flags |= 1 /* TypeElement */;
    const { matchedQueries, references, matchedQueryIds } = splitMatchedQueriesDsl(matchedQueriesDsl);
    /** @type {?} */
    const template = templateFactory ? resolveDefinition(templateFactory) : null;
    return {
        // will bet set by the view definition
        nodeIndex: -1,
        parent: null,
        renderParent: null,
        bindingIndex: -1,
        outputIndex: -1,
        // regular values
        flags,
        checkIndex: -1,
        childFlags: 0,
        directChildFlags: 0,
        childMatchedQueries: 0, matchedQueries, matchedQueryIds, references, ngContentIndex, childCount,
        bindings: [],
        bindingFlags: 0,
        outputs: [],
        element: {
            ns: null,
            name: null,
            attrs: null, template,
            componentProvider: null,
            componentView: null,
            componentRendererType: null,
            publicProviders: null,
            allProviders: null,
            handleEvent: handleEvent || NOOP
        },
        provider: null,
        text: null,
        query: null,
        ngContent: null
    };
}
/**
 * @param {?} checkIndex
 * @param {?} flags
 * @param {?} matchedQueriesDsl
 * @param {?} ngContentIndex
 * @param {?} childCount
 * @param {?} namespaceAndName
 * @param {?=} fixedAttrs
 * @param {?=} bindings
 * @param {?=} outputs
 * @param {?=} handleEvent
 * @param {?=} componentView
 * @param {?=} componentRendererType
 * @return {?}
 */
export function elementDef(checkIndex, flags, matchedQueriesDsl, ngContentIndex, childCount, namespaceAndName, fixedAttrs = [], bindings, outputs, handleEvent, componentView, componentRendererType) {
    if (!handleEvent) {
        handleEvent = NOOP;
    }
    const { matchedQueries, references, matchedQueryIds } = splitMatchedQueriesDsl(matchedQueriesDsl);
    /** @type {?} */
    let ns = (/** @type {?} */ (null));
    /** @type {?} */
    let name = (/** @type {?} */ (null));
    if (namespaceAndName) {
        [ns, name] = splitNamespace(namespaceAndName);
    }
    bindings = bindings || [];
    /** @type {?} */
    const bindingDefs = new Array(bindings.length);
    for (let i = 0; i < bindings.length; i++) {
        const [bindingFlags, namespaceAndName, suffixOrSecurityContext] = bindings[i];
        const [ns, name] = splitNamespace(namespaceAndName);
        /** @type {?} */
        let securityContext = (/** @type {?} */ (undefined));
        /** @type {?} */
        let suffix = (/** @type {?} */ (undefined));
        switch (bindingFlags & 15 /* Types */) {
            case 4 /* TypeElementStyle */:
                suffix = (/** @type {?} */ (suffixOrSecurityContext));
                break;
            case 1 /* TypeElementAttribute */:
            case 8 /* TypeProperty */:
                securityContext = (/** @type {?} */ (suffixOrSecurityContext));
                break;
        }
        bindingDefs[i] =
            { flags: bindingFlags, ns, name, nonMinifiedName: name, securityContext, suffix };
    }
    outputs = outputs || [];
    /** @type {?} */
    const outputDefs = new Array(outputs.length);
    for (let i = 0; i < outputs.length; i++) {
        const [target, eventName] = outputs[i];
        outputDefs[i] = {
            type: 0 /* ElementOutput */,
            target: (/** @type {?} */ (target)), eventName,
            propName: null
        };
    }
    fixedAttrs = fixedAttrs || [];
    /** @type {?} */
    const attrs = (/** @type {?} */ (fixedAttrs.map((/**
     * @param {?} __0
     * @return {?}
     */
    ([namespaceAndName, value]) => {
        const [ns, name] = splitNamespace(namespaceAndName);
        return [ns, name, value];
    }))));
    componentRendererType = resolveRendererType2(componentRendererType);
    if (componentView) {
        flags |= 33554432 /* ComponentView */;
    }
    flags |= 1 /* TypeElement */;
    return {
        // will bet set by the view definition
        nodeIndex: -1,
        parent: null,
        renderParent: null,
        bindingIndex: -1,
        outputIndex: -1,
        // regular values
        checkIndex,
        flags,
        childFlags: 0,
        directChildFlags: 0,
        childMatchedQueries: 0, matchedQueries, matchedQueryIds, references, ngContentIndex, childCount,
        bindings: bindingDefs,
        bindingFlags: calcBindingFlags(bindingDefs),
        outputs: outputDefs,
        element: {
            ns,
            name,
            attrs,
            template: null,
            // will bet set by the view definition
            componentProvider: null,
            componentView: componentView || null,
            componentRendererType: componentRendererType,
            publicProviders: null,
            allProviders: null,
            handleEvent: handleEvent || NOOP,
        },
        provider: null,
        text: null,
        query: null,
        ngContent: null
    };
}
/**
 * @param {?} view
 * @param {?} renderHost
 * @param {?} def
 * @return {?}
 */
export function createElement(view, renderHost, def) {
    /** @type {?} */
    const elDef = (/** @type {?} */ (def.element));
    /** @type {?} */
    const rootSelectorOrNode = view.root.selectorOrNode;
    /** @type {?} */
    const renderer = view.renderer;
    /** @type {?} */
    let el;
    if (view.parent || !rootSelectorOrNode) {
        if (elDef.name) {
            el = renderer.createElement(elDef.name, elDef.ns);
        }
        else {
            el = renderer.createComment('');
        }
        /** @type {?} */
        const parentEl = getParentRenderElement(view, renderHost, def);
        if (parentEl) {
            renderer.appendChild(parentEl, el);
        }
    }
    else {
        // when using native Shadow DOM, do not clear the root element contents to allow slot projection
        /** @type {?} */
        const preserveContent = (!!elDef.componentRendererType &&
            elDef.componentRendererType.encapsulation === ViewEncapsulation.ShadowDom);
        el = renderer.selectRootElement(rootSelectorOrNode, preserveContent);
    }
    if (elDef.attrs) {
        for (let i = 0; i < elDef.attrs.length; i++) {
            const [ns, name, value] = elDef.attrs[i];
            renderer.setAttribute(el, name, value, ns);
        }
    }
    return el;
}
/**
 * @param {?} view
 * @param {?} compView
 * @param {?} def
 * @param {?} el
 * @return {?}
 */
export function listenToElementOutputs(view, compView, def, el) {
    for (let i = 0; i < def.outputs.length; i++) {
        /** @type {?} */
        const output = def.outputs[i];
        /** @type {?} */
        const handleEventClosure = renderEventHandlerClosure(view, def.nodeIndex, elementEventFullName(output.target, output.eventName));
        /** @type {?} */
        let listenTarget = output.target;
        /** @type {?} */
        let listenerView = view;
        if (output.target === 'component') {
            listenTarget = null;
            listenerView = compView;
        }
        /** @type {?} */
        const disposable = (/** @type {?} */ (listenerView.renderer.listen(listenTarget || el, output.eventName, handleEventClosure)));
        (/** @type {?} */ (view.disposables))[def.outputIndex + i] = disposable;
    }
}
/**
 * @param {?} view
 * @param {?} index
 * @param {?} eventName
 * @return {?}
 */
function renderEventHandlerClosure(view, index, eventName) {
    return (/**
     * @param {?} event
     * @return {?}
     */
    (event) => dispatchEvent(view, index, eventName, event));
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} v0
 * @param {?} v1
 * @param {?} v2
 * @param {?} v3
 * @param {?} v4
 * @param {?} v5
 * @param {?} v6
 * @param {?} v7
 * @param {?} v8
 * @param {?} v9
 * @return {?}
 */
export function checkAndUpdateElementInline(view, def, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9) {
    /** @type {?} */
    const bindLen = def.bindings.length;
    /** @type {?} */
    let changed = false;
    if (bindLen > 0 && checkAndUpdateElementValue(view, def, 0, v0))
        changed = true;
    if (bindLen > 1 && checkAndUpdateElementValue(view, def, 1, v1))
        changed = true;
    if (bindLen > 2 && checkAndUpdateElementValue(view, def, 2, v2))
        changed = true;
    if (bindLen > 3 && checkAndUpdateElementValue(view, def, 3, v3))
        changed = true;
    if (bindLen > 4 && checkAndUpdateElementValue(view, def, 4, v4))
        changed = true;
    if (bindLen > 5 && checkAndUpdateElementValue(view, def, 5, v5))
        changed = true;
    if (bindLen > 6 && checkAndUpdateElementValue(view, def, 6, v6))
        changed = true;
    if (bindLen > 7 && checkAndUpdateElementValue(view, def, 7, v7))
        changed = true;
    if (bindLen > 8 && checkAndUpdateElementValue(view, def, 8, v8))
        changed = true;
    if (bindLen > 9 && checkAndUpdateElementValue(view, def, 9, v9))
        changed = true;
    return changed;
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} values
 * @return {?}
 */
export function checkAndUpdateElementDynamic(view, def, values) {
    /** @type {?} */
    let changed = false;
    for (let i = 0; i < values.length; i++) {
        if (checkAndUpdateElementValue(view, def, i, values[i]))
            changed = true;
    }
    return changed;
}
/**
 * @param {?} view
 * @param {?} def
 * @param {?} bindingIdx
 * @param {?} value
 * @return {?}
 */
function checkAndUpdateElementValue(view, def, bindingIdx, value) {
    if (!checkAndUpdateBinding(view, def, bindingIdx, value)) {
        return false;
    }
    /** @type {?} */
    const binding = def.bindings[bindingIdx];
    /** @type {?} */
    const elData = asElementData(view, def.nodeIndex);
    /** @type {?} */
    const renderNode = elData.renderElement;
    /** @type {?} */
    const name = (/** @type {?} */ (binding.name));
    switch (binding.flags & 15 /* Types */) {
        case 1 /* TypeElementAttribute */:
            setElementAttribute(view, binding, renderNode, binding.ns, name, value);
            break;
        case 2 /* TypeElementClass */:
            setElementClass(view, renderNode, name, value);
            break;
        case 4 /* TypeElementStyle */:
            setElementStyle(view, binding, renderNode, name, value);
            break;
        case 8 /* TypeProperty */:
            /** @type {?} */
            const bindView = (def.flags & 33554432 /* ComponentView */ &&
                binding.flags & 32 /* SyntheticHostProperty */) ?
                elData.componentView :
                view;
            setElementProperty(bindView, binding, renderNode, name, value);
            break;
    }
    return true;
}
/**
 * @param {?} view
 * @param {?} binding
 * @param {?} renderNode
 * @param {?} ns
 * @param {?} name
 * @param {?} value
 * @return {?}
 */
function setElementAttribute(view, binding, renderNode, ns, name, value) {
    /** @type {?} */
    const securityContext = binding.securityContext;
    /** @type {?} */
    let renderValue = securityContext ? view.root.sanitizer.sanitize(securityContext, value) : value;
    renderValue = renderValue != null ? renderValue.toString() : null;
    /** @type {?} */
    const renderer = view.renderer;
    if (value != null) {
        renderer.setAttribute(renderNode, name, renderValue, ns);
    }
    else {
        renderer.removeAttribute(renderNode, name, ns);
    }
}
/**
 * @param {?} view
 * @param {?} renderNode
 * @param {?} name
 * @param {?} value
 * @return {?}
 */
function setElementClass(view, renderNode, name, value) {
    /** @type {?} */
    const renderer = view.renderer;
    if (value) {
        renderer.addClass(renderNode, name);
    }
    else {
        renderer.removeClass(renderNode, name);
    }
}
/**
 * @param {?} view
 * @param {?} binding
 * @param {?} renderNode
 * @param {?} name
 * @param {?} value
 * @return {?}
 */
function setElementStyle(view, binding, renderNode, name, value) {
    /** @type {?} */
    let renderValue = view.root.sanitizer.sanitize(SecurityContext.STYLE, (/** @type {?} */ (value)));
    if (renderValue != null) {
        renderValue = renderValue.toString();
        /** @type {?} */
        const unit = binding.suffix;
        if (unit != null) {
            renderValue = renderValue + unit;
        }
    }
    else {
        renderValue = null;
    }
    /** @type {?} */
    const renderer = view.renderer;
    if (renderValue != null) {
        renderer.setStyle(renderNode, name, renderValue);
    }
    else {
        renderer.removeStyle(renderNode, name);
    }
}
/**
 * @param {?} view
 * @param {?} binding
 * @param {?} renderNode
 * @param {?} name
 * @param {?} value
 * @return {?}
 */
function setElementProperty(view, binding, renderNode, name, value) {
    /** @type {?} */
    const securityContext = binding.securityContext;
    /** @type {?} */
    let renderValue = securityContext ? view.root.sanitizer.sanitize(securityContext, value) : value;
    view.renderer.setProperty(renderNode, name, renderValue);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZWxlbWVudC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvZWxlbWVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLGtCQUFrQixDQUFDO0FBRW5ELE9BQU8sRUFBQyxlQUFlLEVBQUMsTUFBTSwwQkFBMEIsQ0FBQztBQUV6RCxPQUFPLEVBQTBKLGFBQWEsRUFBQyxNQUFNLFNBQVMsQ0FBQztBQUMvTCxPQUFPLEVBQUMsSUFBSSxFQUFFLGdCQUFnQixFQUFFLHFCQUFxQixFQUFFLGFBQWEsRUFBRSxvQkFBb0IsRUFBRSxzQkFBc0IsRUFBRSxpQkFBaUIsRUFBRSxvQkFBb0IsRUFBRSxzQkFBc0IsRUFBRSxjQUFjLEVBQUMsTUFBTSxRQUFRLENBQUM7Ozs7Ozs7Ozs7QUFFbk4sTUFBTSxVQUFVLFNBQVMsQ0FDckIsS0FBZ0IsRUFBRSxpQkFBNkQsRUFDL0UsY0FBNkIsRUFBRSxVQUFrQixFQUFFLFdBQXlDLEVBQzVGLGVBQXVDO0lBQ3pDLEtBQUssdUJBQXlCLENBQUM7VUFDekIsRUFBQyxjQUFjLEVBQUUsVUFBVSxFQUFFLGVBQWUsRUFBQyxHQUFHLHNCQUFzQixDQUFDLGlCQUFpQixDQUFDOztVQUN6RixRQUFRLEdBQUcsZUFBZSxDQUFDLENBQUMsQ0FBQyxpQkFBaUIsQ0FBQyxlQUFlLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSTtJQUU1RSxPQUFPOztRQUVMLFNBQVMsRUFBRSxDQUFDLENBQUM7UUFDYixNQUFNLEVBQUUsSUFBSTtRQUNaLFlBQVksRUFBRSxJQUFJO1FBQ2xCLFlBQVksRUFBRSxDQUFDLENBQUM7UUFDaEIsV0FBVyxFQUFFLENBQUMsQ0FBQztRQUNmLGlCQUFpQjtRQUNqQixLQUFLO1FBQ0wsVUFBVSxFQUFFLENBQUMsQ0FBQztRQUNkLFVBQVUsRUFBRSxDQUFDO1FBQ2Is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LENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLElBQUksT0FBTyxHQUFHLENBQUMsSUFBSSwwQkFBMEIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLElBQUksT0FBTyxHQUFHLENBQUMsSUFBSSwwQkFBMEIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLElBQUksT0FBTyxHQUFHLENBQUMsSUFBSSwwQkFBMEIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLElBQUksT0FBTyxHQUFHLENBQUMsSUFBSSwwQkFBMEIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLElBQUksT0FBTyxHQUFHLENBQUMsSUFBSSwwQkFBMEIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLElBQUksT0FBTyxHQUFHLENBQUMsSUFBSSwwQkFBMEIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLElBQUksT0FBTyxHQUFHLENBQUMsSUFBSSwwQkFBMEIsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFLENBQUM7UUFBRSxPQUFPLEdBQUcsSUFBSSxDQUFDO0lBQ2hGLE9BQU8sT0FBTyxDQUFDO0FBQ2pCLENBQUM7Ozs7Ozs7QUFFRCxNQUFNLFVBQVUsNEJBQTRCLENBQUMsSUFBYyxFQUFFLEdBQVksRUFBRSxNQUFhOztRQUNsRixPQUFPLEdBQUcsS0FBSztJQUNuQixLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsTUFBTSxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTtRQUN0QyxJQUFJLDBCQUEwQixDQUFDLElBQUksRUFBRSxHQUFHLEVBQUUsQ0FBQyxFQUFFLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQztZQUFFLE9BQU8sR0FBRyxJQUFJLENBQUM7S0FDekU7SUFDRCxPQUFPLE9BQU8sQ0FBQztBQUNqQixDQUFDOzs7Ozs7OztBQUVELFNBQVMsMEJBQTBCLENBQUMsSUFBYyxFQUFFLEdBQVksRUFBRSxVQUFrQixFQUFFLEtBQVU7SUFDOUYsSUFBSSxDQUFDLHFCQUFxQixDQUFDLElBQUksRUFBRSxHQUFHLEVBQUUsVUFBVSxFQUFFLEtBQUssQ0FBQyxFQUFFO1FBQ3hELE9BQU8sS0FBSyxDQUFDO0tBQ2Q7O1VBQ0ssT0FBTyxHQUFHLEdBQUcsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDOztVQUNsQyxNQUFNLEdBQUcsYUFBYSxDQUFDLElBQUksRUFBRSxHQUFHLENBQUMsU0FBUyxDQUFDOztVQUMzQyxVQUFVLEdBQUcsTUFBTSxDQUFDLGFBQWE7O1VBQ2pDLElBQUksR0FBRyxtQkFBQSxPQUFPLENBQUMsSUFBSSxFQUFFO0lBQzNCLFFBQVEsT0FBTyxDQUFDLEtBQUssaUJBQXFCLEVBQUU7UUFDMUM7WUFDRSxtQkFBbUIsQ0FBQyxJQUFJLEVBQUUsT0FBTyxFQUFFLFVBQVUsRUFBRSxPQUFPLENBQUMsRUFBRSxFQUFFLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQztZQUN4RSxNQUFNO1FBQ1I7WUFDRSxlQUFlLENBQUMsSUFBSSxFQUFFLFVBQVUsRUFBRSxJQUFJLEVBQUUsS0FBSyxDQUFDLENBQUM7WUFDL0MsTUFBTTtRQUNSO1lBQ0UsZUFBZSxDQUFDLElBQUksRUFBRSxPQUFPLEVBQUUsVUFBVSxFQUFFLElBQUksRUFBRSxLQUFLLENBQUMsQ0FBQztZQUN4RCxNQUFNO1FBQ1I7O2tCQUNRLFFBQVEsR0FBRyxDQUFDLEdBQUcsQ0FBQyxLQUFLLCtCQUEwQjtnQkFDbkMsT0FBTyxDQUFDLEtBQUssaUNBQXFDLENBQUMsQ0FBQyxDQUFDO2dCQUNuRSxNQUFNLENBQUMsYUFBYSxDQUFDLENBQUM7Z0JBQ3RCLElBQUk7WUFDUixrQkFBa0IsQ0FBQyxRQUFRLEVBQUUsT0FBTyxFQUFFLFVBQVUsRUFBRSxJQUFJLEVBQUUsS0FBSyxDQUFDLENBQUM7WUFDL0QsTUFBTTtLQUNUO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7Ozs7Ozs7O0FBRUQsU0FBUyxtQkFBbUIsQ0FDeEIsSUFBYyxFQUFFLE9BQW1CLEVBQUUsVUFBZSxFQUFFLEVBQWlCLEVBQUUsSUFBWSxFQUNyRixLQUFVOztVQUNOLGVBQWUsR0FBRyxPQUFPLENBQUMsZUFBZTs7UUFDM0MsV0FBVyxHQUFHLGVBQWUsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsUUFBUSxDQUFDLGVBQWUsRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSztJQUNoRyxXQUFXLEdBQUcsV0FBVyxJQUFJLElBQUksQ0FBQyxDQUFDLENBQUMsV0FBVyxDQUFDLFFBQVEsRUFBRSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUM7O1VBQzVELFFBQVEsR0FBRyxJQUFJLENBQUMsUUFBUTtJQUM5QixJQUFJLEtBQUssSUFBSSxJQUFJLEVBQUU7UUFDakIsUUFBUSxDQUFDLFlBQVksQ0FBQyxVQUFVLEVBQUUsSUFBSSxFQUFFLFdBQVcsRUFBRSxFQUFFLENBQUMsQ0FBQztLQUMxRDtTQUFNO1FBQ0wsUUFBUSxDQUFDLGVBQWUsQ0FBQyxVQUFVLEVBQUUsSUFBSSxFQUFFLEVBQUUsQ0FBQyxDQUFDO0tBQ2hEO0FBQ0gsQ0FBQzs7Ozs7Ozs7QUFFRCxTQUFTLGVBQWUsQ0FBQyxJQUFjLEVBQUUsVUFBZSxFQUFFLElBQVksRUFBRSxLQUFjOztVQUM5RSxRQUFRLEdBQUcsSUFBSSxDQUFDLFFBQVE7SUFDOUIsSUFBSSxLQUFLLEVBQUU7UUFDVCxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsRUFBRSxJQUFJLENBQUMsQ0FBQztLQUNyQztTQUFNO1FBQ0wsUUFBUSxDQUFDLFdBQVcsQ0FBQyxVQUFVLEVBQUUsSUFBSSxDQUFDLENBQUM7S0FDeEM7QUFDSCxDQUFDOzs7Ozs7Ozs7QUFFRCxTQUFTLGVBQWUsQ0FDcEIsSUFBYyxFQUFFLE9BQW1CLEVBQUUsVUFBZSxFQUFFLElBQVksRUFBRSxLQUFVOztRQUM1RSxXQUFXLEdBQ1gsSUFBSSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsUUFBUSxDQUFDLGVBQWUsQ0FBQyxLQUFLLEVBQUUsbUJBQUEsS0FBSyxFQUFjLENBQUM7SUFDNUUsSUFBSSxXQUFXLElBQUksSUFBSSxFQUFFO1FBQ3ZCLFdBQVcsR0FBRyxXQUFXLENBQUMsUUFBUSxFQUFFLENBQUM7O2NBQy9CLElBQUksR0FBRyxPQUFPLENBQUMsTUFBTTtRQUMzQixJQUFJLElBQUksSUFBSSxJQUFJLEVBQUU7WUFDaEIsV0FBVyxHQUFHLFdBQVcsR0FBRyxJQUFJLENBQUM7U0FDbEM7S0FDRjtTQUFNO1FBQ0wsV0FBVyxHQUFHLElBQUksQ0FBQztLQUNwQjs7VUFDSyxRQUFRLEdBQUcsSUFBSSxDQUFDLFFBQVE7SUFDOUIsSUFBSSxXQUFXLElBQUksSUFBSSxFQUFFO1FBQ3ZCLFFBQVEsQ0FBQyxRQUFRLENBQUMsVUFBVSxFQUFFLElBQUksRUFBRSxXQUFXLENBQUMsQ0FBQztLQUNsRDtTQUFNO1FBQ0wsUUFBUSxDQUFDLFdBQVcsQ0FBQyxVQUFVLEVBQUUsSUFBSSxDQUFDLENBQUM7S0FDeEM7QUFDSCxDQUFDOzs7Ozs7Ozs7QUFFRCxTQUFTLGtCQUFrQixDQUN2QixJQUFjLEVBQUUsT0FBbUIsRUFBRSxVQUFlLEVBQUUsSUFBWSxFQUFFLEtBQVU7O1VBQzFFLGVBQWUsR0FBRyxPQUFPLENBQUMsZUFBZTs7UUFDM0MsV0FBVyxHQUFHLGVBQWUsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsUUFBUSxDQUFDLGVBQWUsRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSztJQUNoRyxJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsQ0FBQyxVQUFVLEVBQUUsSUFBSSxFQUFFLFdBQVcsQ0FBQyxDQUFDO0FBQzNELENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Vmlld0VuY2Fwc3VsYXRpb259IGZyb20gJy4uL21ldGFkYXRhL3ZpZXcnO1xuaW1wb3J0IHtSZW5kZXJlclR5cGUyfSBmcm9tICcuLi9yZW5kZXIvYXBpJztcbmltcG9ydCB7U2VjdXJpdHlDb250ZXh0fSBmcm9tICcuLi9zYW5pdGl6YXRpb24vc2VjdXJpdHknO1xuXG5pbXBvcnQge0JpbmRpbmdEZWYsIEJpbmRpbmdGbGFncywgRWxlbWVudERhdGEsIEVsZW1lbnRIYW5kbGVFdmVudEZuLCBOb2RlRGVmLCBOb2RlRmxhZ3MsIE91dHB1dERlZiwgT3V0cHV0VHlwZSwgUXVlcnlWYWx1ZVR5cGUsIFZpZXdEYXRhLCBWaWV3RGVmaW5pdGlvbkZhY3RvcnksIGFzRWxlbWVudERhdGF9IGZyb20gJy4vdHlwZXMnO1xuaW1wb3J0IHtOT09QLCBjYWxjQmluZGluZ0ZsYWdzLCBjaGVja0FuZFVwZGF0ZUJpbmRpbmcsIGRpc3BhdGNoRXZlbnQsIGVsZW1lbnRFdmVudEZ1bGxOYW1lLCBnZXRQYXJlbnRSZW5kZXJFbGVtZW50LCByZXNvbHZlRGVmaW5pdGlvbiwgcmVzb2x2ZVJlbmRlcmVyVHlwZTIsIHNwbGl0TWF0Y2hlZFF1ZXJpZXNEc2wsIHNwbGl0TmFtZXNwYWNlfSBmcm9tICcuL3V0aWwnO1xuXG5leHBvcnQgZnVuY3Rpb24gYW5jaG9yRGVmKFxuICAgIGZsYWdzOiBOb2RlRmxhZ3MsIG1hdGNoZWRRdWVyaWVzRHNsOiBudWxsIHwgW3N0cmluZyB8IG51bWJlciwgUXVlcnlWYWx1ZVR5cGVdW10sXG4gICAgbmdDb250ZW50SW5kZXg6IG51bGwgfCBudW1iZXIsIGNoaWxkQ291bnQ6IG51bWJlciwgaGFuZGxlRXZlbnQ/OiBudWxsIHwgRWxlbWVudEhhbmRsZUV2ZW50Rm4sXG4gICAgdGVtcGxhdGVGYWN0b3J5PzogVmlld0RlZmluaXRpb25GYWN0b3J5KTogTm9kZURlZiB7XG4gIGZsYWdzIHw9IE5vZGVGbGFncy5UeXBlRWxlbWVudDtcbiAgY29uc3Qge21hdGNoZWRRdWVyaWVzLCByZWZlcmVuY2VzLCBtYXRjaGVkUXVlcnlJZHN9ID0gc3BsaXRNYXRjaGVkUXVlcmllc0RzbChtYXRjaGVkUXVlcmllc0RzbCk7XG4gIGNvbnN0IHRlbXBsYXRlID0gdGVtcGxhdGVGYWN0b3J5ID8gcmVzb2x2ZURlZmluaXRpb24odGVtcGxhdGVGYWN0b3J5KSA6IG51bGw7XG5cbiAgcmV0dXJuIHtcbiAgICAvLyB3aWxsIGJldCBzZXQgYnkgdGhlIHZpZXcgZGVmaW5pdGlvblxuICAgIG5vZGVJbmRleDogLTEsXG4gICAgcGFyZW50OiBudWxsLFxuICAgIHJlbmRlclBhcmVudDogbnVsbCxcbiAgICBiaW5kaW5nSW5kZXg6IC0xLFxuICAgIG91dHB1dEluZGV4OiAtMSxcbiAgICAvLyByZWd1bGFyIHZhbHVlc1xuICAgIGZsYWdzLFxuICAgIGNoZWNrSW5kZXg6IC0xLFxuICAgIGNoaWxkRmxhZ3M6IDAsXG4gICAgZGlyZWN0Q2hpbGRGbGFnczogMCxcbiAgICBjaGlsZE1hdGNoZWRRdWVyaWVzOiAwLCBtYXRjaGVkUXVlcmllcywgbWF0Y2hlZFF1ZXJ5SWRzLCByZWZlcmVuY2VzLCBuZ0NvbnRlbnRJbmRleCwgY2hpbGRDb3VudCxcbiAgICBiaW5kaW5nczogW10sXG4gICAgYmluZGluZ0ZsYWdzOiAwLFxuICAgIG91dHB1dHM6IFtdLFxuICAgIGVsZW1lbnQ6IHtcbiAgICAgIG5zOiBudWxsLFxuICAgICAgbmFtZTogbnVsbCxcbiAgICAgIGF0dHJzOiBudWxsLCB0ZW1wbGF0ZSxcbiAgICAgIGNvbXBvbmVudFByb3ZpZGVyOiBudWxsLFxuICAgICAgY29tcG9uZW50VmlldzogbnVsbCxcbiAgICAgIGNvbXBvbmVudFJlbmRlcmVyVHlwZTogbnVsbCxcbiAgICAgIHB1YmxpY1Byb3ZpZGVyczogbnVsbCxcbiAgICAgIGFsbFByb3ZpZGVyczogbnVsbCxcbiAgICAgIGhhbmRsZUV2ZW50OiBoYW5kbGVFdmVudCB8fCBOT09QXG4gICAgfSxcbiAgICBwcm92aWRlcjogbnVsbCxcbiAgICB0ZXh0OiBudWxsLFxuICAgIHF1ZXJ5OiBudWxsLFxuICAgIG5nQ29udGVudDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZWxlbWVudERlZihcbiAgICBjaGVja0luZGV4OiBudW1iZXIsIGZsYWdzOiBOb2RlRmxhZ3MsXG4gICAgbWF0Y2hlZFF1ZXJpZXNEc2w6IG51bGwgfCBbc3RyaW5nIHwgbnVtYmVyLCBRdWVyeVZhbHVlVHlwZV1bXSwgbmdDb250ZW50SW5kZXg6IG51bGwgfCBudW1iZXIsXG4gICAgY2hpbGRDb3VudDogbnVtYmVyLCBuYW1lc3BhY2VBbmROYW1lOiBzdHJpbmcgfCBudWxsLCBmaXhlZEF0dHJzOiBudWxsIHwgW3N0cmluZywgc3RyaW5nXVtdID0gW10sXG4gICAgYmluZGluZ3M/OiBudWxsIHwgW0JpbmRpbmdGbGFncywgc3RyaW5nLCBzdHJpbmcgfCBTZWN1cml0eUNvbnRleHQgfCBudWxsXVtdLFxuICAgIG91dHB1dHM/OiBudWxsIHwgKFtzdHJpbmcsIHN0cmluZ10pW10sIGhhbmRsZUV2ZW50PzogbnVsbCB8IEVsZW1lbnRIYW5kbGVFdmVudEZuLFxuICAgIGNvbXBvbmVudFZpZXc/OiBudWxsIHwgVmlld0RlZmluaXRpb25GYWN0b3J5LFxuICAgIGNvbXBvbmVudFJlbmRlcmVyVHlwZT86IFJlbmRlcmVyVHlwZTIgfCBudWxsKTogTm9kZURlZiB7XG4gIGlmICghaGFuZGxlRXZlbnQpIHtcbiAgICBoYW5kbGVFdmVudCA9IE5PT1A7XG4gIH1cbiAgY29uc3Qge21hdGNoZWRRdWVyaWVzLCByZWZlcmVuY2VzLCBtYXRjaGVkUXVlcnlJZHN9ID0gc3BsaXRNYXRjaGVkUXVlcmllc0RzbChtYXRjaGVkUXVlcmllc0RzbCk7XG4gIGxldCBuczogc3RyaW5nID0gbnVsbCAhO1xuICBsZXQgbmFtZTogc3RyaW5nID0gbnVsbCAhO1xuICBpZiAobmFtZXNwYWNlQW5kTmFtZSkge1xuICAgIFtucywgbmFtZV0gPSBzcGxpdE5hbWVzcGFjZShuYW1lc3BhY2VBbmROYW1lKTtcbiAgfVxuICBiaW5kaW5ncyA9IGJpbmRpbmdzIHx8IFtdO1xuICBjb25zdCBiaW5kaW5nRGVmczogQmluZGluZ0RlZltdID0gbmV3IEFycmF5KGJpbmRpbmdzLmxlbmd0aCk7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmluZGluZ3MubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBbYmluZGluZ0ZsYWdzLCBuYW1lc3BhY2VBbmROYW1lLCBzdWZmaXhPclNlY3VyaXR5Q29udGV4dF0gPSBiaW5kaW5nc1tpXTtcblxuICAgIGNvbnN0IFtucywgbmFtZV0gPSBzcGxpdE5hbWVzcGFjZShuYW1lc3BhY2VBbmROYW1lKTtcbiAgICBsZXQgc2VjdXJpdHlDb250ZXh0OiBTZWN1cml0eUNvbnRleHQgPSB1bmRlZmluZWQgITtcbiAgICBsZXQgc3VmZml4OiBzdHJpbmcgPSB1bmRlZmluZWQgITtcbiAgICBzd2l0Y2ggKGJpbmRpbmdGbGFncyAmIEJpbmRpbmdGbGFncy5UeXBlcykge1xuICAgICAgY2FzZSBCaW5kaW5nRmxhZ3MuVHlwZUVsZW1lbnRTdHlsZTpcbiAgICAgICAgc3VmZml4ID0gPHN0cmluZz5zdWZmaXhPclNlY3VyaXR5Q29udGV4dDtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIEJpbmRpbmdGbGFncy5UeXBlRWxlbWVudEF0dHJpYnV0ZTpcbiAgICAgIGNhc2UgQmluZGluZ0ZsYWdzLlR5cGVQcm9wZXJ0eTpcbiAgICAgICAgc2VjdXJpdHlDb250ZXh0ID0gPFNlY3VyaXR5Q29udGV4dD5zdWZmaXhPclNlY3VyaXR5Q29udGV4dDtcbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIGJpbmRpbmdEZWZzW2ldID1cbiAgICAgICAge2ZsYWdzOiBiaW5kaW5nRmxhZ3MsIG5zLCBuYW1lLCBub25NaW5pZmllZE5hbWU6IG5hbWUsIHNlY3VyaXR5Q29udGV4dCwgc3VmZml4fTtcbiAgfVxuICBvdXRwdXRzID0gb3V0cHV0cyB8fCBbXTtcbiAgY29uc3Qgb3V0cHV0RGVmczogT3V0cHV0RGVmW10gPSBuZXcgQXJyYXkob3V0cHV0cy5sZW5ndGgpO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IG91dHB1dHMubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBbdGFyZ2V0LCBldmVudE5hbWVdID0gb3V0cHV0c1tpXTtcbiAgICBvdXRwdXREZWZzW2ldID0ge1xuICAgICAgdHlwZTogT3V0cHV0VHlwZS5FbGVtZW50T3V0cHV0LFxuICAgICAgdGFyZ2V0OiA8YW55PnRhcmdldCwgZXZlbnROYW1lLFxuICAgICAgcHJvcE5hbWU6IG51bGxcbiAgICB9O1xuICB9XG4gIGZpeGVkQXR0cnMgPSBmaXhlZEF0dHJzIHx8IFtdO1xuICBjb25zdCBhdHRycyA9IDxbc3RyaW5nLCBzdHJpbmcsIHN0cmluZ11bXT5maXhlZEF0dHJzLm1hcCgoW25hbWVzcGFjZUFuZE5hbWUsIHZhbHVlXSkgPT4ge1xuICAgIGNvbnN0IFtucywgbmFtZV0gPSBzcGxpdE5hbWVzcGFjZShuYW1lc3BhY2VBbmROYW1lKTtcbiAgICByZXR1cm4gW25zLCBuYW1lLCB2YWx1ZV07XG4gIH0pO1xuICBjb21wb25lbnRSZW5kZXJlclR5cGUgPSByZXNvbHZlUmVuZGVyZXJUeXBlMihjb21wb25lbnRSZW5kZXJlclR5cGUpO1xuICBpZiAoY29tcG9uZW50Vmlldykge1xuICAgIGZsYWdzIHw9IE5vZGVGbGFncy5Db21wb25lbnRWaWV3O1xuICB9XG4gIGZsYWdzIHw9IE5vZGVGbGFncy5UeXBlRWxlbWVudDtcbiAgcmV0dXJuIHtcbiAgICAvLyB3aWxsIGJldCBzZXQgYnkgdGhlIHZpZXcgZGVmaW5pdGlvblxuICAgIG5vZGVJbmRleDogLTEsXG4gICAgcGFyZW50OiBudWxsLFxuICAgIHJlbmRlclBhcmVudDogbnVsbCxcbiAgICBiaW5kaW5nSW5kZXg6IC0xLFxuICAgIG91dHB1dEluZGV4OiAtMSxcbiAgICAvLyByZWd1bGFyIHZhbHVlc1xuICAgIGNoZWNrSW5kZXgsXG4gICAgZmxhZ3MsXG4gICAgY2hpbGRGbGFnczogMCxcbiAgICBkaXJlY3RDaGlsZEZsYWdzOiAwLFxuICAgIGNoaWxkTWF0Y2hlZFF1ZXJpZXM6IDAsIG1hdGNoZWRRdWVyaWVzLCBtYXRjaGVkUXVlcnlJZHMsIHJlZmVyZW5jZXMsIG5nQ29udGVudEluZGV4LCBjaGlsZENvdW50LFxuICAgIGJpbmRpbmdzOiBiaW5kaW5nRGVmcyxcbiAgICBiaW5kaW5nRmxhZ3M6IGNhbGNCaW5kaW5nRmxhZ3MoYmluZGluZ0RlZnMpLFxuICAgIG91dHB1dHM6IG91dHB1dERlZnMsXG4gICAgZWxlbWVudDoge1xuICAgICAgbnMsXG4gICAgICBuYW1lLFxuICAgICAgYXR0cnMsXG4gICAgICB0ZW1wbGF0ZTogbnVsbCxcbiAgICAgIC8vIHdpbGwgYmV0IHNldCBieSB0aGUgdmlldyBkZWZpbml0aW9uXG4gICAgICBjb21wb25lbnRQcm92aWRlcjogbnVsbCxcbiAgICAgIGNvbXBvbmVudFZpZXc6IGNvbXBvbmVudFZpZXcgfHwgbnVsbCxcbiAgICAgIGNvbXBvbmVudFJlbmRlcmVyVHlwZTogY29tcG9uZW50UmVuZGVyZXJUeXBlLFxuICAgICAgcHVibGljUHJvdmlkZXJzOiBudWxsLFxuICAgICAgYWxsUHJvdmlkZXJzOiBudWxsLFxuICAgICAgaGFuZGxlRXZlbnQ6IGhhbmRsZUV2ZW50IHx8IE5PT1AsXG4gICAgfSxcbiAgICBwcm92aWRlcjogbnVsbCxcbiAgICB0ZXh0OiBudWxsLFxuICAgIHF1ZXJ5OiBudWxsLFxuICAgIG5nQ29udGVudDogbnVsbFxuICB9O1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlRWxlbWVudCh2aWV3OiBWaWV3RGF0YSwgcmVuZGVySG9zdDogYW55LCBkZWY6IE5vZGVEZWYpOiBFbGVtZW50RGF0YSB7XG4gIGNvbnN0IGVsRGVmID0gZGVmLmVsZW1lbnQgITtcbiAgY29uc3Qgcm9vdFNlbGVjdG9yT3JOb2RlID0gdmlldy5yb290LnNlbGVjdG9yT3JOb2RlO1xuICBjb25zdCByZW5kZXJlciA9IHZpZXcucmVuZGVyZXI7XG4gIGxldCBlbDogYW55O1xuICBpZiAodmlldy5wYXJlbnQgfHwgIXJvb3RTZWxlY3Rvck9yTm9kZSkge1xuICAgIGlmIChlbERlZi5uYW1lKSB7XG4gICAgICBlbCA9IHJlbmRlcmVyLmNyZWF0ZUVsZW1lbnQoZWxEZWYubmFtZSwgZWxEZWYubnMpO1xuICAgIH0gZWxzZSB7XG4gICAgICBlbCA9IHJlbmRlcmVyLmNyZWF0ZUNvbW1lbnQoJycpO1xuICAgIH1cbiAgICBjb25zdCBwYXJlbnRFbCA9IGdldFBhcmVudFJlbmRlckVsZW1lbnQodmlldywgcmVuZGVySG9zdCwgZGVmKTtcbiAgICBpZiAocGFyZW50RWwpIHtcbiAgICAgIHJlbmRlcmVyLmFwcGVuZENoaWxkKHBhcmVudEVsLCBlbCk7XG4gICAgfVxuICB9IGVsc2Uge1xuICAgIC8vIHdoZW4gdXNpbmcgbmF0aXZlIFNoYWRvdyBET00sIGRvIG5vdCBjbGVhciB0aGUgcm9vdCBlbGVtZW50IGNvbnRlbnRzIHRvIGFsbG93IHNsb3QgcHJvamVjdGlvblxuICAgIGNvbnN0IHByZXNlcnZlQ29udGVudCA9XG4gICAgICAgICghIWVsRGVmLmNvbXBvbmVudFJlbmRlcmVyVHlwZSAmJlxuICAgICAgICAgZWxEZWYuY29tcG9uZW50UmVuZGVyZXJUeXBlLmVuY2Fwc3VsYXRpb24gPT09IFZpZXdFbmNhcHN1bGF0aW9uLlNoYWRvd0RvbSk7XG4gICAgZWwgPSByZW5kZXJlci5zZWxlY3RSb290RWxlbWVudChyb290U2VsZWN0b3JPck5vZGUsIHByZXNlcnZlQ29udGVudCk7XG4gIH1cbiAgaWYgKGVsRGVmLmF0dHJzKSB7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBlbERlZi5hdHRycy5sZW5ndGg7IGkrKykge1xuICAgICAgY29uc3QgW25zLCBuYW1lLCB2YWx1ZV0gPSBlbERlZi5hdHRyc1tpXTtcbiAgICAgIHJlbmRlcmVyLnNldEF0dHJpYnV0ZShlbCwgbmFtZSwgdmFsdWUsIG5zKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGVsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gbGlzdGVuVG9FbGVtZW50T3V0cHV0cyh2aWV3OiBWaWV3RGF0YSwgY29tcFZpZXc6IFZpZXdEYXRhLCBkZWY6IE5vZGVEZWYsIGVsOiBhbnkpIHtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBkZWYub3V0cHV0cy5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IG91dHB1dCA9IGRlZi5vdXRwdXRzW2ldO1xuICAgIGNvbnN0IGhhbmRsZUV2ZW50Q2xvc3VyZSA9IHJlbmRlckV2ZW50SGFuZGxlckNsb3N1cmUoXG4gICAgICAgIHZpZXcsIGRlZi5ub2RlSW5kZXgsIGVsZW1lbnRFdmVudEZ1bGxOYW1lKG91dHB1dC50YXJnZXQsIG91dHB1dC5ldmVudE5hbWUpKTtcbiAgICBsZXQgbGlzdGVuVGFyZ2V0OiAnd2luZG93J3wnZG9jdW1lbnQnfCdib2R5J3wnY29tcG9uZW50J3xudWxsID0gb3V0cHV0LnRhcmdldDtcbiAgICBsZXQgbGlzdGVuZXJWaWV3ID0gdmlldztcbiAgICBpZiAob3V0cHV0LnRhcmdldCA9PT0gJ2NvbXBvbmVudCcpIHtcbiAgICAgIGxpc3RlblRhcmdldCA9IG51bGw7XG4gICAgICBsaXN0ZW5lclZpZXcgPSBjb21wVmlldztcbiAgICB9XG4gICAgY29uc3QgZGlzcG9zYWJsZSA9XG4gICAgICAgIDxhbnk+bGlzdGVuZXJWaWV3LnJlbmRlcmVyLmxpc3RlbihsaXN0ZW5UYXJnZXQgfHwgZWwsIG91dHB1dC5ldmVudE5hbWUsIGhhbmRsZUV2ZW50Q2xvc3VyZSk7XG4gICAgdmlldy5kaXNwb3NhYmxlcyAhW2RlZi5vdXRwdXRJbmRleCArIGldID0gZGlzcG9zYWJsZTtcbiAgfVxufVxuXG5mdW5jdGlvbiByZW5kZXJFdmVudEhhbmRsZXJDbG9zdXJlKHZpZXc6IFZpZXdEYXRhLCBpbmRleDogbnVtYmVyLCBldmVudE5hbWU6IHN0cmluZykge1xuICByZXR1cm4gKGV2ZW50OiBhbnkpID0+IGRpc3BhdGNoRXZlbnQodmlldywgaW5kZXgsIGV2ZW50TmFtZSwgZXZlbnQpO1xufVxuXG5cbmV4cG9ydCBmdW5jdGlvbiBjaGVja0FuZFVwZGF0ZUVsZW1lbnRJbmxpbmUoXG4gICAgdmlldzogVmlld0RhdGEsIGRlZjogTm9kZURlZiwgdjA6IGFueSwgdjE6IGFueSwgdjI6IGFueSwgdjM6IGFueSwgdjQ6IGFueSwgdjU6IGFueSwgdjY6IGFueSxcbiAgICB2NzogYW55LCB2ODogYW55LCB2OTogYW55KTogYm9vbGVhbiB7XG4gIGNvbnN0IGJpbmRMZW4gPSBkZWYuYmluZGluZ3MubGVuZ3RoO1xuICBsZXQgY2hhbmdlZCA9IGZhbHNlO1xuICBpZiAoYmluZExlbiA+IDAgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCAwLCB2MCkpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDEgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCAxLCB2MSkpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDIgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCAyLCB2MikpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDMgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCAzLCB2MykpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDQgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCA0LCB2NCkpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDUgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCA1LCB2NSkpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDYgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCA2LCB2NikpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDcgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCA3LCB2NykpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDggJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCA4LCB2OCkpIGNoYW5nZWQgPSB0cnVlO1xuICBpZiAoYmluZExlbiA+IDkgJiYgY2hlY2tBbmRVcGRhdGVFbGVtZW50VmFsdWUodmlldywgZGVmLCA5LCB2OSkpIGNoYW5nZWQgPSB0cnVlO1xuICByZXR1cm4gY2hhbmdlZDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNoZWNrQW5kVXBkYXRlRWxlbWVudER5bmFtaWModmlldzogVmlld0RhdGEsIGRlZjogTm9kZURlZiwgdmFsdWVzOiBhbnlbXSk6IGJvb2xlYW4ge1xuICBsZXQgY2hhbmdlZCA9IGZhbHNlO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IHZhbHVlcy5sZW5ndGg7IGkrKykge1xuICAgIGlmIChjaGVja0FuZFVwZGF0ZUVsZW1lbnRWYWx1ZSh2aWV3LCBkZWYsIGksIHZhbHVlc1tpXSkpIGNoYW5nZWQgPSB0cnVlO1xuICB9XG4gIHJldHVybiBjaGFuZ2VkO1xufVxuXG5mdW5jdGlvbiBjaGVja0FuZFVwZGF0ZUVsZW1lbnRWYWx1ZSh2aWV3OiBWaWV3RGF0YSwgZGVmOiBOb2RlRGVmLCBiaW5kaW5nSWR4OiBudW1iZXIsIHZhbHVlOiBhbnkpIHtcbiAgaWYgKCFjaGVja0FuZFVwZGF0ZUJpbmRpbmcodmlldywgZGVmLCBiaW5kaW5nSWR4LCB2YWx1ZSkpIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbiAgY29uc3QgYmluZGluZyA9IGRlZi5iaW5kaW5nc1tiaW5kaW5nSWR4XTtcbiAgY29uc3QgZWxEYXRhID0gYXNFbGVtZW50RGF0YSh2aWV3LCBkZWYubm9kZUluZGV4KTtcbiAgY29uc3QgcmVuZGVyTm9kZSA9IGVsRGF0YS5yZW5kZXJFbGVtZW50O1xuICBjb25zdCBuYW1lID0gYmluZGluZy5uYW1lICE7XG4gIHN3aXRjaCAoYmluZGluZy5mbGFncyAmIEJpbmRpbmdGbGFncy5UeXBlcykge1xuICAgIGNhc2UgQmluZGluZ0ZsYWdzLlR5cGVFbGVtZW50QXR0cmlidXRlOlxuICAgICAgc2V0RWxlbWVudEF0dHJpYnV0ZSh2aWV3LCBiaW5kaW5nLCByZW5kZXJOb2RlLCBiaW5kaW5nLm5zLCBuYW1lLCB2YWx1ZSk7XG4gICAgICBicmVhaztcbiAgICBjYXNlIEJpbmRpbmdGbGFncy5UeXBlRWxlbWVudENsYXNzOlxuICAgICAgc2V0RWxlbWVudENsYXNzKHZpZXcsIHJlbmRlck5vZGUsIG5hbWUsIHZhbHVlKTtcbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgQmluZGluZ0ZsYWdzLlR5cGVFbGVtZW50U3R5bGU6XG4gICAgICBzZXRFbGVtZW50U3R5bGUodmlldywgYmluZGluZywgcmVuZGVyTm9kZSwgbmFtZSwgdmFsdWUpO1xuICAgICAgYnJlYWs7XG4gICAgY2FzZSBCaW5kaW5nRmxhZ3MuVHlwZVByb3BlcnR5OlxuICAgICAgY29uc3QgYmluZFZpZXcgPSAoZGVmLmZsYWdzICYgTm9kZUZsYWdzLkNvbXBvbmVudFZpZXcgJiZcbiAgICAgICAgICAgICAgICAgICAgICAgIGJpbmRpbmcuZmxhZ3MgJiBCaW5kaW5nRmxhZ3MuU3ludGhldGljSG9zdFByb3BlcnR5KSA/XG4gICAgICAgICAgZWxEYXRhLmNvbXBvbmVudFZpZXcgOlxuICAgICAgICAgIHZpZXc7XG4gICAgICBzZXRFbGVtZW50UHJvcGVydHkoYmluZFZpZXcsIGJpbmRpbmcsIHJlbmRlck5vZGUsIG5hbWUsIHZhbHVlKTtcbiAgICAgIGJyZWFrO1xuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5mdW5jdGlvbiBzZXRFbGVtZW50QXR0cmlidXRlKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBiaW5kaW5nOiBCaW5kaW5nRGVmLCByZW5kZXJOb2RlOiBhbnksIG5zOiBzdHJpbmcgfCBudWxsLCBuYW1lOiBzdHJpbmcsXG4gICAgdmFsdWU6IGFueSkge1xuICBjb25zdCBzZWN1cml0eUNvbnRleHQgPSBiaW5kaW5nLnNlY3VyaXR5Q29udGV4dDtcbiAgbGV0IHJlbmRlclZhbHVlID0gc2VjdXJpdHlDb250ZXh0ID8gdmlldy5yb290LnNhbml0aXplci5zYW5pdGl6ZShzZWN1cml0eUNvbnRleHQsIHZhbHVlKSA6IHZhbHVlO1xuICByZW5kZXJWYWx1ZSA9IHJlbmRlclZhbHVlICE9IG51bGwgPyByZW5kZXJWYWx1ZS50b1N0cmluZygpIDogbnVsbDtcbiAgY29uc3QgcmVuZGVyZXIgPSB2aWV3LnJlbmRlcmVyO1xuICBpZiAodmFsdWUgIT0gbnVsbCkge1xuICAgIHJlbmRlcmVyLnNldEF0dHJpYnV0ZShyZW5kZXJOb2RlLCBuYW1lLCByZW5kZXJWYWx1ZSwgbnMpO1xuICB9IGVsc2Uge1xuICAgIHJlbmRlcmVyLnJlbW92ZUF0dHJpYnV0ZShyZW5kZXJOb2RlLCBuYW1lLCBucyk7XG4gIH1cbn1cblxuZnVuY3Rpb24gc2V0RWxlbWVudENsYXNzKHZpZXc6IFZpZXdEYXRhLCByZW5kZXJOb2RlOiBhbnksIG5hbWU6IHN0cmluZywgdmFsdWU6IGJvb2xlYW4pIHtcbiAgY29uc3QgcmVuZGVyZXIgPSB2aWV3LnJlbmRlcmVyO1xuICBpZiAodmFsdWUpIHtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhyZW5kZXJOb2RlLCBuYW1lKTtcbiAgfSBlbHNlIHtcbiAgICByZW5kZXJlci5yZW1vdmVDbGFzcyhyZW5kZXJOb2RlLCBuYW1lKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBzZXRFbGVtZW50U3R5bGUoXG4gICAgdmlldzogVmlld0RhdGEsIGJpbmRpbmc6IEJpbmRpbmdEZWYsIHJlbmRlck5vZGU6IGFueSwgbmFtZTogc3RyaW5nLCB2YWx1ZTogYW55KSB7XG4gIGxldCByZW5kZXJWYWx1ZTogc3RyaW5nfG51bGwgPVxuICAgICAgdmlldy5yb290LnNhbml0aXplci5zYW5pdGl6ZShTZWN1cml0eUNvbnRleHQuU1RZTEUsIHZhbHVlIGFze30gfCBzdHJpbmcpO1xuICBpZiAocmVuZGVyVmFsdWUgIT0gbnVsbCkge1xuICAgIHJlbmRlclZhbHVlID0gcmVuZGVyVmFsdWUudG9TdHJpbmcoKTtcbiAgICBjb25zdCB1bml0ID0gYmluZGluZy5zdWZmaXg7XG4gICAgaWYgKHVuaXQgIT0gbnVsbCkge1xuICAgICAgcmVuZGVyVmFsdWUgPSByZW5kZXJWYWx1ZSArIHVuaXQ7XG4gICAgfVxuICB9IGVsc2Uge1xuICAgIHJlbmRlclZhbHVlID0gbnVsbDtcbiAgfVxuICBjb25zdCByZW5kZXJlciA9IHZpZXcucmVuZGVyZXI7XG4gIGlmIChyZW5kZXJWYWx1ZSAhPSBudWxsKSB7XG4gICAgcmVuZGVyZXIuc2V0U3R5bGUocmVuZGVyTm9kZSwgbmFtZSwgcmVuZGVyVmFsdWUpO1xuICB9IGVsc2Uge1xuICAgIHJlbmRlcmVyLnJlbW92ZVN0eWxlKHJlbmRlck5vZGUsIG5hbWUpO1xuICB9XG59XG5cbmZ1bmN0aW9uIHNldEVsZW1lbnRQcm9wZXJ0eShcbiAgICB2aWV3OiBWaWV3RGF0YSwgYmluZGluZzogQmluZGluZ0RlZiwgcmVuZGVyTm9kZTogYW55LCBuYW1lOiBzdHJpbmcsIHZhbHVlOiBhbnkpIHtcbiAgY29uc3Qgc2VjdXJpdHlDb250ZXh0ID0gYmluZGluZy5zZWN1cml0eUNvbnRleHQ7XG4gIGxldCByZW5kZXJWYWx1ZSA9IHNlY3VyaXR5Q29udGV4dCA/IHZpZXcucm9vdC5zYW5pdGl6ZXIuc2FuaXRpemUoc2VjdXJpdHlDb250ZXh0LCB2YWx1ZSkgOiB2YWx1ZTtcbiAgdmlldy5yZW5kZXJlci5zZXRQcm9wZXJ0eShyZW5kZXJOb2RlLCBuYW1lLCByZW5kZXJWYWx1ZSk7XG59XG4iXX0=