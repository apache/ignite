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
import { ElementRef } from '../linker/element_ref';
import { QueryList } from '../linker/query_list';
import { asElementData, asProviderData, asQueryList } from './types';
import { declaredViewContainer, filterQueryId, isEmbeddedView } from './util';
/**
 * @param {?} flags
 * @param {?} id
 * @param {?} bindings
 * @return {?}
 */
export function queryDef(flags, id, bindings) {
    /** @type {?} */
    let bindingDefs = [];
    for (let propName in bindings) {
        /** @type {?} */
        const bindingType = bindings[propName];
        bindingDefs.push({ propName, bindingType });
    }
    return {
        // will bet set by the view definition
        nodeIndex: -1,
        parent: null,
        renderParent: null,
        bindingIndex: -1,
        outputIndex: -1,
        // regular values
        // TODO(vicb): check
        checkIndex: -1, flags,
        childFlags: 0,
        directChildFlags: 0,
        childMatchedQueries: 0,
        ngContentIndex: -1,
        matchedQueries: {},
        matchedQueryIds: 0,
        references: {},
        childCount: 0,
        bindings: [],
        bindingFlags: 0,
        outputs: [],
        element: null,
        provider: null,
        text: null,
        query: { id, filterId: filterQueryId(id), bindings: bindingDefs },
        ngContent: null
    };
}
/**
 * @return {?}
 */
export function createQuery() {
    return new QueryList();
}
/**
 * @param {?} view
 * @return {?}
 */
export function dirtyParentQueries(view) {
    /** @type {?} */
    const queryIds = view.def.nodeMatchedQueries;
    while (view.parent && isEmbeddedView(view)) {
        /** @type {?} */
        let tplDef = (/** @type {?} */ (view.parentNodeDef));
        view = view.parent;
        // content queries
        /** @type {?} */
        const end = tplDef.nodeIndex + tplDef.childCount;
        for (let i = 0; i <= end; i++) {
            /** @type {?} */
            const nodeDef = view.def.nodes[i];
            if ((nodeDef.flags & 67108864 /* TypeContentQuery */) &&
                (nodeDef.flags & 536870912 /* DynamicQuery */) &&
                ((/** @type {?} */ (nodeDef.query)).filterId & queryIds) === (/** @type {?} */ (nodeDef.query)).filterId) {
                asQueryList(view, i).setDirty();
            }
            if ((nodeDef.flags & 1 /* TypeElement */ && i + nodeDef.childCount < tplDef.nodeIndex) ||
                !(nodeDef.childFlags & 67108864 /* TypeContentQuery */) ||
                !(nodeDef.childFlags & 536870912 /* DynamicQuery */)) {
                // skip elements that don't contain the template element or no query.
                i += nodeDef.childCount;
            }
        }
    }
    // view queries
    if (view.def.nodeFlags & 134217728 /* TypeViewQuery */) {
        for (let i = 0; i < view.def.nodes.length; i++) {
            /** @type {?} */
            const nodeDef = view.def.nodes[i];
            if ((nodeDef.flags & 134217728 /* TypeViewQuery */) && (nodeDef.flags & 536870912 /* DynamicQuery */)) {
                asQueryList(view, i).setDirty();
            }
            // only visit the root nodes
            i += nodeDef.childCount;
        }
    }
}
/**
 * @param {?} view
 * @param {?} nodeDef
 * @return {?}
 */
export function checkAndUpdateQuery(view, nodeDef) {
    /** @type {?} */
    const queryList = asQueryList(view, nodeDef.nodeIndex);
    if (!queryList.dirty) {
        return;
    }
    /** @type {?} */
    let directiveInstance;
    /** @type {?} */
    let newValues = (/** @type {?} */ (undefined));
    if (nodeDef.flags & 67108864 /* TypeContentQuery */) {
        /** @type {?} */
        const elementDef = (/** @type {?} */ ((/** @type {?} */ (nodeDef.parent)).parent));
        newValues = calcQueryValues(view, elementDef.nodeIndex, elementDef.nodeIndex + elementDef.childCount, (/** @type {?} */ (nodeDef.query)), []);
        directiveInstance = asProviderData(view, (/** @type {?} */ (nodeDef.parent)).nodeIndex).instance;
    }
    else if (nodeDef.flags & 134217728 /* TypeViewQuery */) {
        newValues = calcQueryValues(view, 0, view.def.nodes.length - 1, (/** @type {?} */ (nodeDef.query)), []);
        directiveInstance = view.component;
    }
    queryList.reset(newValues);
    /** @type {?} */
    const bindings = (/** @type {?} */ (nodeDef.query)).bindings;
    /** @type {?} */
    let notify = false;
    for (let i = 0; i < bindings.length; i++) {
        /** @type {?} */
        const binding = bindings[i];
        /** @type {?} */
        let boundValue;
        switch (binding.bindingType) {
            case 0 /* First */:
                boundValue = queryList.first;
                break;
            case 1 /* All */:
                boundValue = queryList;
                notify = true;
                break;
        }
        directiveInstance[binding.propName] = boundValue;
    }
    if (notify) {
        queryList.notifyOnChanges();
    }
}
/**
 * @param {?} view
 * @param {?} startIndex
 * @param {?} endIndex
 * @param {?} queryDef
 * @param {?} values
 * @return {?}
 */
function calcQueryValues(view, startIndex, endIndex, queryDef, values) {
    for (let i = startIndex; i <= endIndex; i++) {
        /** @type {?} */
        const nodeDef = view.def.nodes[i];
        /** @type {?} */
        const valueType = nodeDef.matchedQueries[queryDef.id];
        if (valueType != null) {
            values.push(getQueryValue(view, nodeDef, valueType));
        }
        if (nodeDef.flags & 1 /* TypeElement */ && (/** @type {?} */ (nodeDef.element)).template &&
            ((/** @type {?} */ ((/** @type {?} */ (nodeDef.element)).template)).nodeMatchedQueries & queryDef.filterId) ===
                queryDef.filterId) {
            /** @type {?} */
            const elementData = asElementData(view, i);
            // check embedded views that were attached at the place of their template,
            // but process child nodes first if some match the query (see issue #16568)
            if ((nodeDef.childMatchedQueries & queryDef.filterId) === queryDef.filterId) {
                calcQueryValues(view, i + 1, i + nodeDef.childCount, queryDef, values);
                i += nodeDef.childCount;
            }
            if (nodeDef.flags & 16777216 /* EmbeddedViews */) {
                /** @type {?} */
                const embeddedViews = (/** @type {?} */ (elementData.viewContainer))._embeddedViews;
                for (let k = 0; k < embeddedViews.length; k++) {
                    /** @type {?} */
                    const embeddedView = embeddedViews[k];
                    /** @type {?} */
                    const dvc = declaredViewContainer(embeddedView);
                    if (dvc && dvc === elementData) {
                        calcQueryValues(embeddedView, 0, embeddedView.def.nodes.length - 1, queryDef, values);
                    }
                }
            }
            /** @type {?} */
            const projectedViews = elementData.template._projectedViews;
            if (projectedViews) {
                for (let k = 0; k < projectedViews.length; k++) {
                    /** @type {?} */
                    const projectedView = projectedViews[k];
                    calcQueryValues(projectedView, 0, projectedView.def.nodes.length - 1, queryDef, values);
                }
            }
        }
        if ((nodeDef.childMatchedQueries & queryDef.filterId) !== queryDef.filterId) {
            // if no child matches the query, skip the children.
            i += nodeDef.childCount;
        }
    }
    return values;
}
/**
 * @param {?} view
 * @param {?} nodeDef
 * @param {?} queryValueType
 * @return {?}
 */
export function getQueryValue(view, nodeDef, queryValueType) {
    if (queryValueType != null) {
        // a match
        switch (queryValueType) {
            case 1 /* RenderElement */:
                return asElementData(view, nodeDef.nodeIndex).renderElement;
            case 0 /* ElementRef */:
                return new ElementRef(asElementData(view, nodeDef.nodeIndex).renderElement);
            case 2 /* TemplateRef */:
                return asElementData(view, nodeDef.nodeIndex).template;
            case 3 /* ViewContainerRef */:
                return asElementData(view, nodeDef.nodeIndex).viewContainer;
            case 4 /* Provider */:
                return asProviderData(view, nodeDef.nodeIndex).instance;
        }
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicXVlcnkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy92aWV3L3F1ZXJ5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFVBQVUsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBQ2pELE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxzQkFBc0IsQ0FBQztBQUUvQyxPQUFPLEVBQTRGLGFBQWEsRUFBRSxjQUFjLEVBQUUsV0FBVyxFQUFDLE1BQU0sU0FBUyxDQUFDO0FBQzlKLE9BQU8sRUFBQyxxQkFBcUIsRUFBRSxhQUFhLEVBQUUsY0FBYyxFQUFDLE1BQU0sUUFBUSxDQUFDOzs7Ozs7O0FBRTVFLE1BQU0sVUFBVSxRQUFRLENBQ3BCLEtBQWdCLEVBQUUsRUFBVSxFQUFFLFFBQWdEOztRQUM1RSxXQUFXLEdBQXNCLEVBQUU7SUFDdkMsS0FBSyxJQUFJLFFBQVEsSUFBSSxRQUFRLEVBQUU7O2NBQ3ZCLFdBQVcsR0FBRyxRQUFRLENBQUMsUUFBUSxDQUFDO1FBQ3RDLFdBQVcsQ0FBQyxJQUFJLENBQUMsRUFBQyxRQUFRLEVBQUUsV0FBVyxFQUFDLENBQUMsQ0FBQztLQUMzQztJQUVELE9BQU87O1FBRUwsU0FBUyxFQUFFLENBQUMsQ0FBQztRQUNiLE1BQU0sRUFBRSxJQUFJO1FBQ1osWUFBWSxFQUFFLElBQUk7UUFDbEIsWUFBWSxFQUFFLENBQUMsQ0FBQztRQUNoQixXQUFXLEVBQUUsQ0FBQyxDQUFDOzs7UUFHZixVQUFVLEVBQUUsQ0FBQyxDQUFDLEVBQUUsS0FBSztRQUNyQixVQUFVLEVBQUUsQ0FBQztRQUNiLGdCQUFnQixFQUFFLENBQUM7UUFDbkIsbUJBQW1CLEVBQUUsQ0FBQztRQUN0QixjQUFjLEVBQUUsQ0FBQyxDQUFDO1FBQ2xCLGNBQWMsRUFBRSxFQUFFO1FBQ2xCLGVBQWUsRUFBRSxDQUFDO1FBQ2xCLFVBQVUsRUFBRSxFQUFF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QyxLQUFLLENBQUMsTUFBTSxHQUFHLENBQUMsRUFBRSxRQUFRLEVBQUUsTUFBTSxDQUFDLENBQUM7aUJBQ3pGO2FBQ0Y7U0FDRjtRQUNELElBQUksQ0FBQyxPQUFPLENBQUMsbUJBQW1CLEdBQUcsUUFBUSxDQUFDLFFBQVEsQ0FBQyxLQUFLLFFBQVEsQ0FBQyxRQUFRLEVBQUU7WUFDM0Usb0RBQW9EO1lBQ3BELENBQUMsSUFBSSxPQUFPLENBQUMsVUFBVSxDQUFDO1NBQ3pCO0tBQ0Y7SUFDRCxPQUFPLE1BQU0sQ0FBQztBQUNoQixDQUFDOzs7Ozs7O0FBRUQsTUFBTSxVQUFVLGFBQWEsQ0FDekIsSUFBYyxFQUFFLE9BQWdCLEVBQUUsY0FBOEI7SUFDbEUsSUFBSSxjQUFjLElBQUksSUFBSSxFQUFFO1FBQzFCLFVBQVU7UUFDVixRQUFRLGNBQWMsRUFBRTtZQUN0QjtnQkFDRSxPQUFPLGFBQWEsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLFNBQVMsQ0FBQyxDQUFDLGFBQWEsQ0FBQztZQUM5RDtnQkFDRSxPQUFPLElBQUksVUFBVSxDQUFDLGFBQWEsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLFNBQVMsQ0FBQyxDQUFDLGFBQWEsQ0FBQyxDQUFDO1lBQzlFO2dCQUNFLE9BQU8sYUFBYSxDQUFDLElBQUksRUFBRSxPQUFPLENBQUMsU0FBUyxDQUFDLENBQUMsUUFBUSxDQUFDO1lBQ3pEO2dCQUNFLE9BQU8sYUFBYSxDQUFDLElBQUksRUFBRSxPQUFPLENBQUMsU0FBUyxDQUFDLENBQUMsYUFBYSxDQUFDO1lBQzlEO2dCQUNFLE9BQU8sY0FBYyxDQUFDLElBQUksRUFBRSxPQUFPLENBQUMsU0FBUyxDQUFDLENBQUMsUUFBUSxDQUFDO1NBQzNEO0tBQ0Y7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0VsZW1lbnRSZWZ9IGZyb20gJy4uL2xpbmtlci9lbGVtZW50X3JlZic7XG5pbXBvcnQge1F1ZXJ5TGlzdH0gZnJvbSAnLi4vbGlua2VyL3F1ZXJ5X2xpc3QnO1xuXG5pbXBvcnQge05vZGVEZWYsIE5vZGVGbGFncywgUXVlcnlCaW5kaW5nRGVmLCBRdWVyeUJpbmRpbmdUeXBlLCBRdWVyeURlZiwgUXVlcnlWYWx1ZVR5cGUsIFZpZXdEYXRhLCBhc0VsZW1lbnREYXRhLCBhc1Byb3ZpZGVyRGF0YSwgYXNRdWVyeUxpc3R9IGZyb20gJy4vdHlwZXMnO1xuaW1wb3J0IHtkZWNsYXJlZFZpZXdDb250YWluZXIsIGZpbHRlclF1ZXJ5SWQsIGlzRW1iZWRkZWRWaWV3fSBmcm9tICcuL3V0aWwnO1xuXG5leHBvcnQgZnVuY3Rpb24gcXVlcnlEZWYoXG4gICAgZmxhZ3M6IE5vZGVGbGFncywgaWQ6IG51bWJlciwgYmluZGluZ3M6IHtbcHJvcE5hbWU6IHN0cmluZ106IFF1ZXJ5QmluZGluZ1R5cGV9KTogTm9kZURlZiB7XG4gIGxldCBiaW5kaW5nRGVmczogUXVlcnlCaW5kaW5nRGVmW10gPSBbXTtcbiAgZm9yIChsZXQgcHJvcE5hbWUgaW4gYmluZGluZ3MpIHtcbiAgICBjb25zdCBiaW5kaW5nVHlwZSA9IGJpbmRpbmdzW3Byb3BOYW1lXTtcbiAgICBiaW5kaW5nRGVmcy5wdXNoKHtwcm9wTmFtZSwgYmluZGluZ1R5cGV9KTtcbiAgfVxuXG4gIHJldHVybiB7XG4gICAgLy8gd2lsbCBiZXQgc2V0IGJ5IHRoZSB2aWV3IGRlZmluaXRpb25cbiAgICBub2RlSW5kZXg6IC0xLFxuICAgIHBhcmVudDogbnVsbCxcbiAgICByZW5kZXJQYXJlbnQ6IG51bGwsXG4gICAgYmluZGluZ0luZGV4OiAtMSxcbiAgICBvdXRwdXRJbmRleDogLTEsXG4gICAgLy8gcmVndWxhciB2YWx1ZXNcbiAgICAvLyBUT0RPKHZpY2IpOiBjaGVja1xuICAgIGNoZWNrSW5kZXg6IC0xLCBmbGFncyxcbiAgICBjaGlsZEZsYWdzOiAwLFxuICAgIGRpcmVjdENoaWxkRmxhZ3M6IDAsXG4gICAgY2hpbGRNYXRjaGVkUXVlcmllczogMCxcbiAgICBuZ0NvbnRlbnRJbmRleDogLTEsXG4gICAgbWF0Y2hlZFF1ZXJpZXM6IHt9LFxuICAgIG1hdGNoZWRRdWVyeUlkczogMCxcbiAgICByZWZlcmVuY2VzOiB7fSxcbiAgICBjaGlsZENvdW50OiAwLFxuICAgIGJpbmRpbmdzOiBbXSxcbiAgICBiaW5kaW5nRmxhZ3M6IDAsXG4gICAgb3V0cHV0czogW10sXG4gICAgZWxlbWVudDogbnVsbCxcbiAgICBwcm92aWRlcjogbnVsbCxcbiAgICB0ZXh0OiBudWxsLFxuICAgIHF1ZXJ5OiB7aWQsIGZpbHRlcklkOiBmaWx0ZXJRdWVyeUlkKGlkKSwgYmluZGluZ3M6IGJpbmRpbmdEZWZzfSxcbiAgICBuZ0NvbnRlbnQ6IG51bGxcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNyZWF0ZVF1ZXJ5KCk6IFF1ZXJ5TGlzdDxhbnk+IHtcbiAgcmV0dXJuIG5ldyBRdWVyeUxpc3QoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRpcnR5UGFyZW50UXVlcmllcyh2aWV3OiBWaWV3RGF0YSkge1xuICBjb25zdCBxdWVyeUlkcyA9IHZpZXcuZGVmLm5vZGVNYXRjaGVkUXVlcmllcztcbiAgd2hpbGUgKHZpZXcucGFyZW50ICYmIGlzRW1iZWRkZWRWaWV3KHZpZXcpKSB7XG4gICAgbGV0IHRwbERlZiA9IHZpZXcucGFyZW50Tm9kZURlZiAhO1xuICAgIHZpZXcgPSB2aWV3LnBhcmVudDtcbiAgICAvLyBjb250ZW50IHF1ZXJpZXNcbiAgICBjb25zdCBlbmQgPSB0cGxEZWYubm9kZUluZGV4ICsgdHBsRGVmLmNoaWxkQ291bnQ7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPD0gZW5kOyBpKyspIHtcbiAgICAgIGNvbnN0IG5vZGVEZWYgPSB2aWV3LmRlZi5ub2Rlc1tpXTtcbiAgICAgIGlmICgobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlQ29udGVudFF1ZXJ5KSAmJlxuICAgICAgICAgIChub2RlRGVmLmZsYWdzICYgTm9kZUZsYWdzLkR5bmFtaWNRdWVyeSkgJiZcbiAgICAgICAgICAobm9kZURlZi5xdWVyeSAhLmZpbHRlcklkICYgcXVlcnlJZHMpID09PSBub2RlRGVmLnF1ZXJ5ICEuZmlsdGVySWQpIHtcbiAgICAgICAgYXNRdWVyeUxpc3QodmlldywgaSkuc2V0RGlydHkoKTtcbiAgICAgIH1cbiAgICAgIGlmICgobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlRWxlbWVudCAmJiBpICsgbm9kZURlZi5jaGlsZENvdW50IDwgdHBsRGVmLm5vZGVJbmRleCkgfHxcbiAgICAgICAgICAhKG5vZGVEZWYuY2hpbGRGbGFncyAmIE5vZGVGbGFncy5UeXBlQ29udGVudFF1ZXJ5KSB8fFxuICAgICAgICAgICEobm9kZURlZi5jaGlsZEZsYWdzICYgTm9kZUZsYWdzLkR5bmFtaWNRdWVyeSkpIHtcbiAgICAgICAgLy8gc2tpcCBlbGVtZW50cyB0aGF0IGRvbid0IGNvbnRhaW4gdGhlIHRlbXBsYXRlIGVsZW1lbnQgb3Igbm8gcXVlcnkuXG4gICAgICAgIGkgKz0gbm9kZURlZi5jaGlsZENvdW50O1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8vIHZpZXcgcXVlcmllc1xuICBpZiAodmlldy5kZWYubm9kZUZsYWdzICYgTm9kZUZsYWdzLlR5cGVWaWV3UXVlcnkpIHtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHZpZXcuZGVmLm5vZGVzLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBub2RlRGVmID0gdmlldy5kZWYubm9kZXNbaV07XG4gICAgICBpZiAoKG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuVHlwZVZpZXdRdWVyeSkgJiYgKG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuRHluYW1pY1F1ZXJ5KSkge1xuICAgICAgICBhc1F1ZXJ5TGlzdCh2aWV3LCBpKS5zZXREaXJ0eSgpO1xuICAgICAgfVxuICAgICAgLy8gb25seSB2aXNpdCB0aGUgcm9vdCBub2Rlc1xuICAgICAgaSArPSBub2RlRGVmLmNoaWxkQ291bnQ7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjaGVja0FuZFVwZGF0ZVF1ZXJ5KHZpZXc6IFZpZXdEYXRhLCBub2RlRGVmOiBOb2RlRGVmKSB7XG4gIGNvbnN0IHF1ZXJ5TGlzdCA9IGFzUXVlcnlMaXN0KHZpZXcsIG5vZGVEZWYubm9kZUluZGV4KTtcbiAgaWYgKCFxdWVyeUxpc3QuZGlydHkpIHtcbiAgICByZXR1cm47XG4gIH1cbiAgbGV0IGRpcmVjdGl2ZUluc3RhbmNlOiBhbnk7XG4gIGxldCBuZXdWYWx1ZXM6IGFueVtdID0gdW5kZWZpbmVkICE7XG4gIGlmIChub2RlRGVmLmZsYWdzICYgTm9kZUZsYWdzLlR5cGVDb250ZW50UXVlcnkpIHtcbiAgICBjb25zdCBlbGVtZW50RGVmID0gbm9kZURlZi5wYXJlbnQgIS5wYXJlbnQgITtcbiAgICBuZXdWYWx1ZXMgPSBjYWxjUXVlcnlWYWx1ZXMoXG4gICAgICAgIHZpZXcsIGVsZW1lbnREZWYubm9kZUluZGV4LCBlbGVtZW50RGVmLm5vZGVJbmRleCArIGVsZW1lbnREZWYuY2hpbGRDb3VudCwgbm9kZURlZi5xdWVyeSAhLFxuICAgICAgICBbXSk7XG4gICAgZGlyZWN0aXZlSW5zdGFuY2UgPSBhc1Byb3ZpZGVyRGF0YSh2aWV3LCBub2RlRGVmLnBhcmVudCAhLm5vZGVJbmRleCkuaW5zdGFuY2U7XG4gIH0gZWxzZSBpZiAobm9kZURlZi5mbGFncyAmIE5vZGVGbGFncy5UeXBlVmlld1F1ZXJ5KSB7XG4gICAgbmV3VmFsdWVzID0gY2FsY1F1ZXJ5VmFsdWVzKHZpZXcsIDAsIHZpZXcuZGVmLm5vZGVzLmxlbmd0aCAtIDEsIG5vZGVEZWYucXVlcnkgISwgW10pO1xuICAgIGRpcmVjdGl2ZUluc3RhbmNlID0gdmlldy5jb21wb25lbnQ7XG4gIH1cbiAgcXVlcnlMaXN0LnJlc2V0KG5ld1ZhbHVlcyk7XG4gIGNvbnN0IGJpbmRpbmdzID0gbm9kZURlZi5xdWVyeSAhLmJpbmRpbmdzO1xuICBsZXQgbm90aWZ5ID0gZmFsc2U7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgYmluZGluZ3MubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBiaW5kaW5nID0gYmluZGluZ3NbaV07XG4gICAgbGV0IGJvdW5kVmFsdWU6IGFueTtcbiAgICBzd2l0Y2ggKGJpbmRpbmcuYmluZGluZ1R5cGUpIHtcbiAgICAgIGNhc2UgUXVlcnlCaW5kaW5nVHlwZS5GaXJzdDpcbiAgICAgICAgYm91bmRWYWx1ZSA9IHF1ZXJ5TGlzdC5maXJzdDtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIFF1ZXJ5QmluZGluZ1R5cGUuQWxsOlxuICAgICAgICBib3VuZFZhbHVlID0gcXVlcnlMaXN0O1xuICAgICAgICBub3RpZnkgPSB0cnVlO1xuICAgICAgICBicmVhaztcbiAgICB9XG4gICAgZGlyZWN0aXZlSW5zdGFuY2VbYmluZGluZy5wcm9wTmFtZV0gPSBib3VuZFZhbHVlO1xuICB9XG4gIGlmIChub3RpZnkpIHtcbiAgICBxdWVyeUxpc3Qubm90aWZ5T25DaGFuZ2VzKCk7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2FsY1F1ZXJ5VmFsdWVzKFxuICAgIHZpZXc6IFZpZXdEYXRhLCBzdGFydEluZGV4OiBudW1iZXIsIGVuZEluZGV4OiBudW1iZXIsIHF1ZXJ5RGVmOiBRdWVyeURlZixcbiAgICB2YWx1ZXM6IGFueVtdKTogYW55W10ge1xuICBmb3IgKGxldCBpID0gc3RhcnRJbmRleDsgaSA8PSBlbmRJbmRleDsgaSsrKSB7XG4gICAgY29uc3Qgbm9kZURlZiA9IHZpZXcuZGVmLm5vZGVzW2ldO1xuICAgIGNvbnN0IHZhbHVlVHlwZSA9IG5vZGVEZWYubWF0Y2hlZFF1ZXJpZXNbcXVlcnlEZWYuaWRdO1xuICAgIGlmICh2YWx1ZVR5cGUgIT0gbnVsbCkge1xuICAgICAgdmFsdWVzLnB1c2goZ2V0UXVlcnlWYWx1ZSh2aWV3LCBub2RlRGVmLCB2YWx1ZVR5cGUpKTtcbiAgICB9XG4gICAgaWYgKG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuVHlwZUVsZW1lbnQgJiYgbm9kZURlZi5lbGVtZW50ICEudGVtcGxhdGUgJiZcbiAgICAgICAgKG5vZGVEZWYuZWxlbWVudCAhLnRlbXBsYXRlICEubm9kZU1hdGNoZWRRdWVyaWVzICYgcXVlcnlEZWYuZmlsdGVySWQpID09PVxuICAgICAgICAgICAgcXVlcnlEZWYuZmlsdGVySWQpIHtcbiAgICAgIGNvbnN0IGVsZW1lbnREYXRhID0gYXNFbGVtZW50RGF0YSh2aWV3LCBpKTtcbiAgICAgIC8vIGNoZWNrIGVtYmVkZGVkIHZpZXdzIHRoYXQgd2VyZSBhdHRhY2hlZCBhdCB0aGUgcGxhY2Ugb2YgdGhlaXIgdGVtcGxhdGUsXG4gICAgICAvLyBidXQgcHJvY2VzcyBjaGlsZCBub2RlcyBmaXJzdCBpZiBzb21lIG1hdGNoIHRoZSBxdWVyeSAoc2VlIGlzc3VlICMxNjU2OClcbiAgICAgIGlmICgobm9kZURlZi5jaGlsZE1hdGNoZWRRdWVyaWVzICYgcXVlcnlEZWYuZmlsdGVySWQpID09PSBxdWVyeURlZi5maWx0ZXJJZCkge1xuICAgICAgICBjYWxjUXVlcnlWYWx1ZXModmlldywgaSArIDEsIGkgKyBub2RlRGVmLmNoaWxkQ291bnQsIHF1ZXJ5RGVmLCB2YWx1ZXMpO1xuICAgICAgICBpICs9IG5vZGVEZWYuY2hpbGRDb3VudDtcbiAgICAgIH1cbiAgICAgIGlmIChub2RlRGVmLmZsYWdzICYgTm9kZUZsYWdzLkVtYmVkZGVkVmlld3MpIHtcbiAgICAgICAgY29uc3QgZW1iZWRkZWRWaWV3cyA9IGVsZW1lbnREYXRhLnZpZXdDb250YWluZXIgIS5fZW1iZWRkZWRWaWV3cztcbiAgICAgICAgZm9yIChsZXQgayA9IDA7IGsgPCBlbWJlZGRlZFZpZXdzLmxlbmd0aDsgaysrKSB7XG4gICAgICAgICAgY29uc3QgZW1iZWRkZWRWaWV3ID0gZW1iZWRkZWRWaWV3c1trXTtcbiAgICAgICAgICBjb25zdCBkdmMgPSBkZWNsYXJlZFZpZXdDb250YWluZXIoZW1iZWRkZWRWaWV3KTtcbiAgICAgICAgICBpZiAoZHZjICYmIGR2YyA9PT0gZWxlbWVudERhdGEpIHtcbiAgICAgICAgICAgIGNhbGNRdWVyeVZhbHVlcyhlbWJlZGRlZFZpZXcsIDAsIGVtYmVkZGVkVmlldy5kZWYubm9kZXMubGVuZ3RoIC0gMSwgcXVlcnlEZWYsIHZhbHVlcyk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG4gICAgICBjb25zdCBwcm9qZWN0ZWRWaWV3cyA9IGVsZW1lbnREYXRhLnRlbXBsYXRlLl9wcm9qZWN0ZWRWaWV3cztcbiAgICAgIGlmIChwcm9qZWN0ZWRWaWV3cykge1xuICAgICAgICBmb3IgKGxldCBrID0gMDsgayA8IHByb2plY3RlZFZpZXdzLmxlbmd0aDsgaysrKSB7XG4gICAgICAgICAgY29uc3QgcHJvamVjdGVkVmlldyA9IHByb2plY3RlZFZpZXdzW2tdO1xuICAgICAgICAgIGNhbGNRdWVyeVZhbHVlcyhwcm9qZWN0ZWRWaWV3LCAwLCBwcm9qZWN0ZWRWaWV3LmRlZi5ub2Rlcy5sZW5ndGggLSAxLCBxdWVyeURlZiwgdmFsdWVzKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgICBpZiAoKG5vZGVEZWYuY2hpbGRNYXRjaGVkUXVlcmllcyAmIHF1ZXJ5RGVmLmZpbHRlcklkKSAhPT0gcXVlcnlEZWYuZmlsdGVySWQpIHtcbiAgICAgIC8vIGlmIG5vIGNoaWxkIG1hdGNoZXMgdGhlIHF1ZXJ5LCBza2lwIHRoZSBjaGlsZHJlbi5cbiAgICAgIGkgKz0gbm9kZURlZi5jaGlsZENvdW50O1xuICAgIH1cbiAgfVxuICByZXR1cm4gdmFsdWVzO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0UXVlcnlWYWx1ZShcbiAgICB2aWV3OiBWaWV3RGF0YSwgbm9kZURlZjogTm9kZURlZiwgcXVlcnlWYWx1ZVR5cGU6IFF1ZXJ5VmFsdWVUeXBlKTogYW55IHtcbiAgaWYgKHF1ZXJ5VmFsdWVUeXBlICE9IG51bGwpIHtcbiAgICAvLyBhIG1hdGNoXG4gICAgc3dpdGNoIChxdWVyeVZhbHVlVHlwZSkge1xuICAgICAgY2FzZSBRdWVyeVZhbHVlVHlwZS5SZW5kZXJFbGVtZW50OlxuICAgICAgICByZXR1cm4gYXNFbGVtZW50RGF0YSh2aWV3LCBub2RlRGVmLm5vZGVJbmRleCkucmVuZGVyRWxlbWVudDtcbiAgICAgIGNhc2UgUXVlcnlWYWx1ZVR5cGUuRWxlbWVudFJlZjpcbiAgICAgICAgcmV0dXJuIG5ldyBFbGVtZW50UmVmKGFzRWxlbWVudERhdGEodmlldywgbm9kZURlZi5ub2RlSW5kZXgpLnJlbmRlckVsZW1lbnQpO1xuICAgICAgY2FzZSBRdWVyeVZhbHVlVHlwZS5UZW1wbGF0ZVJlZjpcbiAgICAgICAgcmV0dXJuIGFzRWxlbWVudERhdGEodmlldywgbm9kZURlZi5ub2RlSW5kZXgpLnRlbXBsYXRlO1xuICAgICAgY2FzZSBRdWVyeVZhbHVlVHlwZS5WaWV3Q29udGFpbmVyUmVmOlxuICAgICAgICByZXR1cm4gYXNFbGVtZW50RGF0YSh2aWV3LCBub2RlRGVmLm5vZGVJbmRleCkudmlld0NvbnRhaW5lcjtcbiAgICAgIGNhc2UgUXVlcnlWYWx1ZVR5cGUuUHJvdmlkZXI6XG4gICAgICAgIHJldHVybiBhc1Byb3ZpZGVyRGF0YSh2aWV3LCBub2RlRGVmLm5vZGVJbmRleCkuaW5zdGFuY2U7XG4gICAgfVxuICB9XG59XG4iXX0=