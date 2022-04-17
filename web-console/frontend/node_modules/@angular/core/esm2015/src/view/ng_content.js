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
import { getParentRenderElement, visitProjectedRenderNodes } from './util';
/**
 * @param {?} ngContentIndex
 * @param {?} index
 * @return {?}
 */
export function ngContentDef(ngContentIndex, index) {
    return {
        // will bet set by the view definition
        nodeIndex: -1,
        parent: null,
        renderParent: null,
        bindingIndex: -1,
        outputIndex: -1,
        // regular values
        checkIndex: -1,
        flags: 8 /* TypeNgContent */,
        childFlags: 0,
        directChildFlags: 0,
        childMatchedQueries: 0,
        matchedQueries: {},
        matchedQueryIds: 0,
        references: {}, ngContentIndex,
        childCount: 0,
        bindings: [],
        bindingFlags: 0,
        outputs: [],
        element: null,
        provider: null,
        text: null,
        query: null,
        ngContent: { index }
    };
}
/**
 * @param {?} view
 * @param {?} renderHost
 * @param {?} def
 * @return {?}
 */
export function appendNgContent(view, renderHost, def) {
    /** @type {?} */
    const parentEl = getParentRenderElement(view, renderHost, def);
    if (!parentEl) {
        // Nothing to do if there is no parent element.
        return;
    }
    /** @type {?} */
    const ngContentIndex = (/** @type {?} */ (def.ngContent)).index;
    visitProjectedRenderNodes(view, ngContentIndex, 1 /* AppendChild */, parentEl, null, undefined);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY29udGVudC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvbmdfY29udGVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVNBLE9BQU8sRUFBbUIsc0JBQXNCLEVBQUUseUJBQXlCLEVBQUMsTUFBTSxRQUFRLENBQUM7Ozs7OztBQUUzRixNQUFNLFVBQVUsWUFBWSxDQUFDLGNBQTZCLEVBQUUsS0FBYTtJQUN2RSxPQUFPOztRQUVMLFNBQVMsRUFBRSxDQUFDLENBQUM7UUFDYixNQUFNLEVBQUUsSUFBSTtRQUNaLFlBQVksRUFBRSxJQUFJO1FBQ2xCLFlBQVksRUFBRSxDQUFDLENBQUM7UUFDaEIsV0FBVyxFQUFFLENBQUMsQ0FBQzs7UUFFZixVQUFVLEVBQUUsQ0FBQyxDQUFDO1FBQ2QsS0FBSyx1QkFBeUI7UUFDOUIsVUFBVSxFQUFFLENBQUM7UUFDYixnQkFBZ0IsRUFBRSxDQUFDO1FBQ25CLG1CQUFtQixFQUFFLENBQUM7UUFDdEIsY0FBYyxFQUFFLEVBQUU7UUFDbEIsZUFBZSxFQUFFLENBQUM7UUFDbEIsVUFBVSxFQUFFLEVBQUUsRUFBRSxjQUFjO1FBQzlCLFVBQVUsRUFBRSxDQUFDO1FBQ2IsUUFBUSxFQUFFLEVBQUU7UUFDWixZQUFZLEVBQUUsQ0FBQztRQUNmLE9BQU8sRUFBRSxFQUFFO1FBQ1gsT0FBTyxFQUFFLElBQUk7UUFDYixRQUFRLEVBQUUsSUFBSTtRQUNkLElBQUksRUFBRSxJQUFJO1FBQ1YsS0FBSyxFQUFFLElBQUk7UUFDWCxTQUFTLEVBQUUsRUFBQyxLQUFLLEVBQUM7S0FDbkIsQ0FBQztBQUNKLENBQUM7Ozs7Ozs7QUFFRCxNQUFNLFVBQVUsZUFBZSxDQUFDLElBQWMsRUFBRSxVQUFlLEVBQUUsR0FBWTs7VUFDckUsUUFBUSxHQUFHLHNCQUFzQixDQUFDLElBQUksRUFBRSxVQUFVLEVBQUUsR0FBRyxDQUFDO0lBQzlELElBQUksQ0FBQyxRQUFRLEVBQUU7UUFDYiwrQ0FBK0M7UUFDL0MsT0FBTztLQUNSOztVQUNLLGNBQWMsR0FBRyxtQkFBQSxHQUFHLENBQUMsU0FBUyxFQUFFLENBQUMsS0FBSztJQUM1Qyx5QkFBeUIsQ0FDckIsSUFBSSxFQUFFLGNBQWMsdUJBQWdDLFFBQVEsRUFBRSxJQUFJLEVBQUUsU0FBUyxDQUFDLENBQUM7QUFDckYsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtOb2RlRGVmLCBOb2RlRmxhZ3MsIFZpZXdEYXRhfSBmcm9tICcuL3R5cGVzJztcbmltcG9ydCB7UmVuZGVyTm9kZUFjdGlvbiwgZ2V0UGFyZW50UmVuZGVyRWxlbWVudCwgdmlzaXRQcm9qZWN0ZWRSZW5kZXJOb2Rlc30gZnJvbSAnLi91dGlsJztcblxuZXhwb3J0IGZ1bmN0aW9uIG5nQ29udGVudERlZihuZ0NvbnRlbnRJbmRleDogbnVsbCB8IG51bWJlciwgaW5kZXg6IG51bWJlcik6IE5vZGVEZWYge1xuICByZXR1cm4ge1xuICAgIC8vIHdpbGwgYmV0IHNldCBieSB0aGUgdmlldyBkZWZpbml0aW9uXG4gICAgbm9kZUluZGV4OiAtMSxcbiAgICBwYXJlbnQ6IG51bGwsXG4gICAgcmVuZGVyUGFyZW50OiBudWxsLFxuICAgIGJpbmRpbmdJbmRleDogLTEsXG4gICAgb3V0cHV0SW5kZXg6IC0xLFxuICAgIC8vIHJlZ3VsYXIgdmFsdWVzXG4gICAgY2hlY2tJbmRleDogLTEsXG4gICAgZmxhZ3M6IE5vZGVGbGFncy5UeXBlTmdDb250ZW50LFxuICAgIGNoaWxkRmxhZ3M6IDAsXG4gICAgZGlyZWN0Q2hpbGRGbGFnczogMCxcbiAgICBjaGlsZE1hdGNoZWRRdWVyaWVzOiAwLFxuICAgIG1hdGNoZWRRdWVyaWVzOiB7fSxcbiAgICBtYXRjaGVkUXVlcnlJZHM6IDAsXG4gICAgcmVmZXJlbmNlczoge30sIG5nQ29udGVudEluZGV4LFxuICAgIGNoaWxkQ291bnQ6IDAsXG4gICAgYmluZGluZ3M6IFtdLFxuICAgIGJpbmRpbmdGbGFnczogMCxcbiAgICBvdXRwdXRzOiBbXSxcbiAgICBlbGVtZW50OiBudWxsLFxuICAgIHByb3ZpZGVyOiBudWxsLFxuICAgIHRleHQ6IG51bGwsXG4gICAgcXVlcnk6IG51bGwsXG4gICAgbmdDb250ZW50OiB7aW5kZXh9XG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhcHBlbmROZ0NvbnRlbnQodmlldzogVmlld0RhdGEsIHJlbmRlckhvc3Q6IGFueSwgZGVmOiBOb2RlRGVmKSB7XG4gIGNvbnN0IHBhcmVudEVsID0gZ2V0UGFyZW50UmVuZGVyRWxlbWVudCh2aWV3LCByZW5kZXJIb3N0LCBkZWYpO1xuICBpZiAoIXBhcmVudEVsKSB7XG4gICAgLy8gTm90aGluZyB0byBkbyBpZiB0aGVyZSBpcyBubyBwYXJlbnQgZWxlbWVudC5cbiAgICByZXR1cm47XG4gIH1cbiAgY29uc3QgbmdDb250ZW50SW5kZXggPSBkZWYubmdDb250ZW50ICEuaW5kZXg7XG4gIHZpc2l0UHJvamVjdGVkUmVuZGVyTm9kZXMoXG4gICAgICB2aWV3LCBuZ0NvbnRlbnRJbmRleCwgUmVuZGVyTm9kZUFjdGlvbi5BcHBlbmRDaGlsZCwgcGFyZW50RWwsIG51bGwsIHVuZGVmaW5lZCk7XG59XG4iXX0=