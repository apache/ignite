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
import { addToArray, removeFromArray } from '../util/array_utils';
import { Services } from './types';
import { declaredViewContainer, renderNode, visitRootRenderNodes } from './util';
/**
 * @param {?} parentView
 * @param {?} elementData
 * @param {?} viewIndex
 * @param {?} view
 * @return {?}
 */
export function attachEmbeddedView(parentView, elementData, viewIndex, view) {
    /** @type {?} */
    let embeddedViews = (/** @type {?} */ (elementData.viewContainer))._embeddedViews;
    if (viewIndex === null || viewIndex === undefined) {
        viewIndex = embeddedViews.length;
    }
    view.viewContainerParent = parentView;
    addToArray(embeddedViews, (/** @type {?} */ (viewIndex)), view);
    attachProjectedView(elementData, view);
    Services.dirtyParentQueries(view);
    /** @type {?} */
    const prevView = (/** @type {?} */ (viewIndex)) > 0 ? embeddedViews[(/** @type {?} */ (viewIndex)) - 1] : null;
    renderAttachEmbeddedView(elementData, prevView, view);
}
/**
 * @param {?} vcElementData
 * @param {?} view
 * @return {?}
 */
function attachProjectedView(vcElementData, view) {
    /** @type {?} */
    const dvcElementData = declaredViewContainer(view);
    if (!dvcElementData || dvcElementData === vcElementData ||
        view.state & 16 /* IsProjectedView */) {
        return;
    }
    // Note: For performance reasons, we
    // - add a view to template._projectedViews only 1x throughout its lifetime,
    //   and remove it not until the view is destroyed.
    //   (hard, as when a parent view is attached/detached we would need to attach/detach all
    //    nested projected views as well, even across component boundaries).
    // - don't track the insertion order of views in the projected views array
    //   (hard, as when the views of the same template are inserted different view containers)
    view.state |= 16 /* IsProjectedView */;
    /** @type {?} */
    let projectedViews = dvcElementData.template._projectedViews;
    if (!projectedViews) {
        projectedViews = dvcElementData.template._projectedViews = [];
    }
    projectedViews.push(view);
    // Note: we are changing the NodeDef here as we cannot calculate
    // the fact whether a template is used for projection during compilation.
    markNodeAsProjectedTemplate((/** @type {?} */ (view.parent)).def, (/** @type {?} */ (view.parentNodeDef)));
}
/**
 * @param {?} viewDef
 * @param {?} nodeDef
 * @return {?}
 */
function markNodeAsProjectedTemplate(viewDef, nodeDef) {
    if (nodeDef.flags & 4 /* ProjectedTemplate */) {
        return;
    }
    viewDef.nodeFlags |= 4 /* ProjectedTemplate */;
    nodeDef.flags |= 4 /* ProjectedTemplate */;
    /** @type {?} */
    let parentNodeDef = nodeDef.parent;
    while (parentNodeDef) {
        parentNodeDef.childFlags |= 4 /* ProjectedTemplate */;
        parentNodeDef = parentNodeDef.parent;
    }
}
/**
 * @param {?} elementData
 * @param {?=} viewIndex
 * @return {?}
 */
export function detachEmbeddedView(elementData, viewIndex) {
    /** @type {?} */
    const embeddedViews = (/** @type {?} */ (elementData.viewContainer))._embeddedViews;
    if (viewIndex == null || viewIndex >= embeddedViews.length) {
        viewIndex = embeddedViews.length - 1;
    }
    if (viewIndex < 0) {
        return null;
    }
    /** @type {?} */
    const view = embeddedViews[viewIndex];
    view.viewContainerParent = null;
    removeFromArray(embeddedViews, viewIndex);
    // See attachProjectedView for why we don't update projectedViews here.
    Services.dirtyParentQueries(view);
    renderDetachView(view);
    return view;
}
/**
 * @param {?} view
 * @return {?}
 */
export function detachProjectedView(view) {
    if (!(view.state & 16 /* IsProjectedView */)) {
        return;
    }
    /** @type {?} */
    const dvcElementData = declaredViewContainer(view);
    if (dvcElementData) {
        /** @type {?} */
        const projectedViews = dvcElementData.template._projectedViews;
        if (projectedViews) {
            removeFromArray(projectedViews, projectedViews.indexOf(view));
            Services.dirtyParentQueries(view);
        }
    }
}
/**
 * @param {?} elementData
 * @param {?} oldViewIndex
 * @param {?} newViewIndex
 * @return {?}
 */
export function moveEmbeddedView(elementData, oldViewIndex, newViewIndex) {
    /** @type {?} */
    const embeddedViews = (/** @type {?} */ (elementData.viewContainer))._embeddedViews;
    /** @type {?} */
    const view = embeddedViews[oldViewIndex];
    removeFromArray(embeddedViews, oldViewIndex);
    if (newViewIndex == null) {
        newViewIndex = embeddedViews.length;
    }
    addToArray(embeddedViews, newViewIndex, view);
    // Note: Don't need to change projectedViews as the order in there
    // as always invalid...
    Services.dirtyParentQueries(view);
    renderDetachView(view);
    /** @type {?} */
    const prevView = newViewIndex > 0 ? embeddedViews[newViewIndex - 1] : null;
    renderAttachEmbeddedView(elementData, prevView, view);
    return view;
}
/**
 * @param {?} elementData
 * @param {?} prevView
 * @param {?} view
 * @return {?}
 */
function renderAttachEmbeddedView(elementData, prevView, view) {
    /** @type {?} */
    const prevRenderNode = prevView ? renderNode(prevView, (/** @type {?} */ (prevView.def.lastRenderRootNode))) :
        elementData.renderElement;
    /** @type {?} */
    const parentNode = view.renderer.parentNode(prevRenderNode);
    /** @type {?} */
    const nextSibling = view.renderer.nextSibling(prevRenderNode);
    // Note: We can't check if `nextSibling` is present, as on WebWorkers it will always be!
    // However, browsers automatically do `appendChild` when there is no `nextSibling`.
    visitRootRenderNodes(view, 2 /* InsertBefore */, parentNode, nextSibling, undefined);
}
/**
 * @param {?} view
 * @return {?}
 */
export function renderDetachView(view) {
    visitRootRenderNodes(view, 3 /* RemoveChild */, null, null, undefined);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidmlld19hdHRhY2guanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy92aWV3L3ZpZXdfYXR0YWNoLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFVBQVUsRUFBRSxlQUFlLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUNoRSxPQUFPLEVBQWtDLFFBQVEsRUFBc0MsTUFBTSxTQUFTLENBQUM7QUFDdkcsT0FBTyxFQUFtQixxQkFBcUIsRUFBRSxVQUFVLEVBQUUsb0JBQW9CLEVBQUMsTUFBTSxRQUFRLENBQUM7Ozs7Ozs7O0FBRWpHLE1BQU0sVUFBVSxrQkFBa0IsQ0FDOUIsVUFBb0IsRUFBRSxXQUF3QixFQUFFLFNBQW9DLEVBQ3BGLElBQWM7O1FBQ1osYUFBYSxHQUFHLG1CQUFBLFdBQVcsQ0FBQyxhQUFhLEVBQUUsQ0FBQyxjQUFjO0lBQzlELElBQUksU0FBUyxLQUFLLElBQUksSUFBSSxTQUFTLEtBQUssU0FBUyxFQUFFO1FBQ2pELFNBQVMsR0FBRyxhQUFhLENBQUMsTUFBTSxDQUFDO0tBQ2xDO0lBQ0QsSUFBSSxDQUFDLG1CQUFtQixHQUFHLFVBQVUsQ0FBQztJQUN0QyxVQUFVLENBQUMsYUFBYSxFQUFFLG1CQUFBLFNBQVMsRUFBRSxFQUFFLElBQUksQ0FBQyxDQUFDO0lBQzdDLG1CQUFtQixDQUFDLFdBQVcsRUFBRSxJQUFJLENBQUMsQ0FBQztJQUV2QyxRQUFRLENBQUMsa0JBQWtCLENBQUMsSUFBSSxDQUFDLENBQUM7O1VBRTVCLFFBQVEsR0FBRyxtQkFBQSxTQUFTLEVBQUUsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLGFBQWEsQ0FBQyxtQkFBQSxTQUFTLEVBQUUsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSTtJQUN4RSx3QkFBd0IsQ0FBQyxXQUFXLEVBQUUsUUFBUSxFQUFFLElBQUksQ0FBQyxDQUFDO0FB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LG9CQUFvQixDQUFDLElBQUksd0JBQWlDLFVBQVUsRUFBRSxXQUFXLEVBQUUsU0FBUyxDQUFDLENBQUM7QUFDaEcsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsZ0JBQWdCLENBQUMsSUFBYztJQUM3QyxvQkFBb0IsQ0FBQyxJQUFJLHVCQUFnQyxJQUFJLEVBQUUsSUFBSSxFQUFFLFNBQVMsQ0FBQyxDQUFDO0FBQ2xGLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7YWRkVG9BcnJheSwgcmVtb3ZlRnJvbUFycmF5fSBmcm9tICcuLi91dGlsL2FycmF5X3V0aWxzJztcbmltcG9ydCB7RWxlbWVudERhdGEsIE5vZGVEZWYsIE5vZGVGbGFncywgU2VydmljZXMsIFZpZXdEYXRhLCBWaWV3RGVmaW5pdGlvbiwgVmlld1N0YXRlfSBmcm9tICcuL3R5cGVzJztcbmltcG9ydCB7UmVuZGVyTm9kZUFjdGlvbiwgZGVjbGFyZWRWaWV3Q29udGFpbmVyLCByZW5kZXJOb2RlLCB2aXNpdFJvb3RSZW5kZXJOb2Rlc30gZnJvbSAnLi91dGlsJztcblxuZXhwb3J0IGZ1bmN0aW9uIGF0dGFjaEVtYmVkZGVkVmlldyhcbiAgICBwYXJlbnRWaWV3OiBWaWV3RGF0YSwgZWxlbWVudERhdGE6IEVsZW1lbnREYXRhLCB2aWV3SW5kZXg6IG51bWJlciB8IHVuZGVmaW5lZCB8IG51bGwsXG4gICAgdmlldzogVmlld0RhdGEpIHtcbiAgbGV0IGVtYmVkZGVkVmlld3MgPSBlbGVtZW50RGF0YS52aWV3Q29udGFpbmVyICEuX2VtYmVkZGVkVmlld3M7XG4gIGlmICh2aWV3SW5kZXggPT09IG51bGwgfHwgdmlld0luZGV4ID09PSB1bmRlZmluZWQpIHtcbiAgICB2aWV3SW5kZXggPSBlbWJlZGRlZFZpZXdzLmxlbmd0aDtcbiAgfVxuICB2aWV3LnZpZXdDb250YWluZXJQYXJlbnQgPSBwYXJlbnRWaWV3O1xuICBhZGRUb0FycmF5KGVtYmVkZGVkVmlld3MsIHZpZXdJbmRleCAhLCB2aWV3KTtcbiAgYXR0YWNoUHJvamVjdGVkVmlldyhlbGVtZW50RGF0YSwgdmlldyk7XG5cbiAgU2VydmljZXMuZGlydHlQYXJlbnRRdWVyaWVzKHZpZXcpO1xuXG4gIGNvbnN0IHByZXZWaWV3ID0gdmlld0luZGV4ICEgPiAwID8gZW1iZWRkZWRWaWV3c1t2aWV3SW5kZXggISAtIDFdIDogbnVsbDtcbiAgcmVuZGVyQXR0YWNoRW1iZWRkZWRWaWV3KGVsZW1lbnREYXRhLCBwcmV2Vmlldywgdmlldyk7XG59XG5cbmZ1bmN0aW9uIGF0dGFjaFByb2plY3RlZFZpZXcodmNFbGVtZW50RGF0YTogRWxlbWVudERhdGEsIHZpZXc6IFZpZXdEYXRhKSB7XG4gIGNvbnN0IGR2Y0VsZW1lbnREYXRhID0gZGVjbGFyZWRWaWV3Q29udGFpbmVyKHZpZXcpO1xuICBpZiAoIWR2Y0VsZW1lbnREYXRhIHx8IGR2Y0VsZW1lbnREYXRhID09PSB2Y0VsZW1lbnREYXRhIHx8XG4gICAgICB2aWV3LnN0YXRlICYgVmlld1N0YXRlLklzUHJvamVjdGVkVmlldykge1xuICAgIHJldHVybjtcbiAgfVxuICAvLyBOb3RlOiBGb3IgcGVyZm9ybWFuY2UgcmVhc29ucywgd2VcbiAgLy8gLSBhZGQgYSB2aWV3IHRvIHRlbXBsYXRlLl9wcm9qZWN0ZWRWaWV3cyBvbmx5IDF4IHRocm91Z2hvdXQgaXRzIGxpZmV0aW1lLFxuICAvLyAgIGFuZCByZW1vdmUgaXQgbm90IHVudGlsIHRoZSB2aWV3IGlzIGRlc3Ryb3llZC5cbiAgLy8gICAoaGFyZCwgYXMgd2hlbiBhIHBhcmVudCB2aWV3IGlzIGF0dGFjaGVkL2RldGFjaGVkIHdlIHdvdWxkIG5lZWQgdG8gYXR0YWNoL2RldGFjaCBhbGxcbiAgLy8gICAgbmVzdGVkIHByb2plY3RlZCB2aWV3cyBhcyB3ZWxsLCBldmVuIGFjcm9zcyBjb21wb25lbnQgYm91bmRhcmllcykuXG4gIC8vIC0gZG9uJ3QgdHJhY2sgdGhlIGluc2VydGlvbiBvcmRlciBvZiB2aWV3cyBpbiB0aGUgcHJvamVjdGVkIHZpZXdzIGFycmF5XG4gIC8vICAgKGhhcmQsIGFzIHdoZW4gdGhlIHZpZXdzIG9mIHRoZSBzYW1lIHRlbXBsYXRlIGFyZSBpbnNlcnRlZCBkaWZmZXJlbnQgdmlldyBjb250YWluZXJzKVxuICB2aWV3LnN0YXRlIHw9IFZpZXdTdGF0ZS5Jc1Byb2plY3RlZFZpZXc7XG4gIGxldCBwcm9qZWN0ZWRWaWV3cyA9IGR2Y0VsZW1lbnREYXRhLnRlbXBsYXRlLl9wcm9qZWN0ZWRWaWV3cztcbiAgaWYgKCFwcm9qZWN0ZWRWaWV3cykge1xuICAgIHByb2plY3RlZFZpZXdzID0gZHZjRWxlbWVudERhdGEudGVtcGxhdGUuX3Byb2plY3RlZFZpZXdzID0gW107XG4gIH1cbiAgcHJvamVjdGVkVmlld3MucHVzaCh2aWV3KTtcbiAgLy8gTm90ZTogd2UgYXJlIGNoYW5naW5nIHRoZSBOb2RlRGVmIGhlcmUgYXMgd2UgY2Fubm90IGNhbGN1bGF0ZVxuICAvLyB0aGUgZmFjdCB3aGV0aGVyIGEgdGVtcGxhdGUgaXMgdXNlZCBmb3IgcHJvamVjdGlvbiBkdXJpbmcgY29tcGlsYXRpb24uXG4gIG1hcmtOb2RlQXNQcm9qZWN0ZWRUZW1wbGF0ZSh2aWV3LnBhcmVudCAhLmRlZiwgdmlldy5wYXJlbnROb2RlRGVmICEpO1xufVxuXG5mdW5jdGlvbiBtYXJrTm9kZUFzUHJvamVjdGVkVGVtcGxhdGUodmlld0RlZjogVmlld0RlZmluaXRpb24sIG5vZGVEZWY6IE5vZGVEZWYpIHtcbiAgaWYgKG5vZGVEZWYuZmxhZ3MgJiBOb2RlRmxhZ3MuUHJvamVjdGVkVGVtcGxhdGUpIHtcbiAgICByZXR1cm47XG4gIH1cbiAgdmlld0RlZi5ub2RlRmxhZ3MgfD0gTm9kZUZsYWdzLlByb2plY3RlZFRlbXBsYXRlO1xuICBub2RlRGVmLmZsYWdzIHw9IE5vZGVGbGFncy5Qcm9qZWN0ZWRUZW1wbGF0ZTtcbiAgbGV0IHBhcmVudE5vZGVEZWYgPSBub2RlRGVmLnBhcmVudDtcbiAgd2hpbGUgKHBhcmVudE5vZGVEZWYpIHtcbiAgICBwYXJlbnROb2RlRGVmLmNoaWxkRmxhZ3MgfD0gTm9kZUZsYWdzLlByb2plY3RlZFRlbXBsYXRlO1xuICAgIHBhcmVudE5vZGVEZWYgPSBwYXJlbnROb2RlRGVmLnBhcmVudDtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZGV0YWNoRW1iZWRkZWRWaWV3KGVsZW1lbnREYXRhOiBFbGVtZW50RGF0YSwgdmlld0luZGV4PzogbnVtYmVyKTogVmlld0RhdGF8bnVsbCB7XG4gIGNvbnN0IGVtYmVkZGVkVmlld3MgPSBlbGVtZW50RGF0YS52aWV3Q29udGFpbmVyICEuX2VtYmVkZGVkVmlld3M7XG4gIGlmICh2aWV3SW5kZXggPT0gbnVsbCB8fCB2aWV3SW5kZXggPj0gZW1iZWRkZWRWaWV3cy5sZW5ndGgpIHtcbiAgICB2aWV3SW5kZXggPSBlbWJlZGRlZFZpZXdzLmxlbmd0aCAtIDE7XG4gIH1cbiAgaWYgKHZpZXdJbmRleCA8IDApIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICBjb25zdCB2aWV3ID0gZW1iZWRkZWRWaWV3c1t2aWV3SW5kZXhdO1xuICB2aWV3LnZpZXdDb250YWluZXJQYXJlbnQgPSBudWxsO1xuICByZW1vdmVGcm9tQXJyYXkoZW1iZWRkZWRWaWV3cywgdmlld0luZGV4KTtcblxuICAvLyBTZWUgYXR0YWNoUHJvamVjdGVkVmlldyBmb3Igd2h5IHdlIGRvbid0IHVwZGF0ZSBwcm9qZWN0ZWRWaWV3cyBoZXJlLlxuICBTZXJ2aWNlcy5kaXJ0eVBhcmVudFF1ZXJpZXModmlldyk7XG5cbiAgcmVuZGVyRGV0YWNoVmlldyh2aWV3KTtcblxuICByZXR1cm4gdmlldztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRldGFjaFByb2plY3RlZFZpZXcodmlldzogVmlld0RhdGEpIHtcbiAgaWYgKCEodmlldy5zdGF0ZSAmIFZpZXdTdGF0ZS5Jc1Byb2plY3RlZFZpZXcpKSB7XG4gICAgcmV0dXJuO1xuICB9XG4gIGNvbnN0IGR2Y0VsZW1lbnREYXRhID0gZGVjbGFyZWRWaWV3Q29udGFpbmVyKHZpZXcpO1xuICBpZiAoZHZjRWxlbWVudERhdGEpIHtcbiAgICBjb25zdCBwcm9qZWN0ZWRWaWV3cyA9IGR2Y0VsZW1lbnREYXRhLnRlbXBsYXRlLl9wcm9qZWN0ZWRWaWV3cztcbiAgICBpZiAocHJvamVjdGVkVmlld3MpIHtcbiAgICAgIHJlbW92ZUZyb21BcnJheShwcm9qZWN0ZWRWaWV3cywgcHJvamVjdGVkVmlld3MuaW5kZXhPZih2aWV3KSk7XG4gICAgICBTZXJ2aWNlcy5kaXJ0eVBhcmVudFF1ZXJpZXModmlldyk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBtb3ZlRW1iZWRkZWRWaWV3KFxuICAgIGVsZW1lbnREYXRhOiBFbGVtZW50RGF0YSwgb2xkVmlld0luZGV4OiBudW1iZXIsIG5ld1ZpZXdJbmRleDogbnVtYmVyKTogVmlld0RhdGEge1xuICBjb25zdCBlbWJlZGRlZFZpZXdzID0gZWxlbWVudERhdGEudmlld0NvbnRhaW5lciAhLl9lbWJlZGRlZFZpZXdzO1xuICBjb25zdCB2aWV3ID0gZW1iZWRkZWRWaWV3c1tvbGRWaWV3SW5kZXhdO1xuICByZW1vdmVGcm9tQXJyYXkoZW1iZWRkZWRWaWV3cywgb2xkVmlld0luZGV4KTtcbiAgaWYgKG5ld1ZpZXdJbmRleCA9PSBudWxsKSB7XG4gICAgbmV3Vmlld0luZGV4ID0gZW1iZWRkZWRWaWV3cy5sZW5ndGg7XG4gIH1cbiAgYWRkVG9BcnJheShlbWJlZGRlZFZpZXdzLCBuZXdWaWV3SW5kZXgsIHZpZXcpO1xuXG4gIC8vIE5vdGU6IERvbid0IG5lZWQgdG8gY2hhbmdlIHByb2plY3RlZFZpZXdzIGFzIHRoZSBvcmRlciBpbiB0aGVyZVxuICAvLyBhcyBhbHdheXMgaW52YWxpZC4uLlxuXG4gIFNlcnZpY2VzLmRpcnR5UGFyZW50UXVlcmllcyh2aWV3KTtcblxuICByZW5kZXJEZXRhY2hWaWV3KHZpZXcpO1xuICBjb25zdCBwcmV2VmlldyA9IG5ld1ZpZXdJbmRleCA+IDAgPyBlbWJlZGRlZFZpZXdzW25ld1ZpZXdJbmRleCAtIDFdIDogbnVsbDtcbiAgcmVuZGVyQXR0YWNoRW1iZWRkZWRWaWV3KGVsZW1lbnREYXRhLCBwcmV2Vmlldywgdmlldyk7XG5cbiAgcmV0dXJuIHZpZXc7XG59XG5cbmZ1bmN0aW9uIHJlbmRlckF0dGFjaEVtYmVkZGVkVmlldyhcbiAgICBlbGVtZW50RGF0YTogRWxlbWVudERhdGEsIHByZXZWaWV3OiBWaWV3RGF0YSB8IG51bGwsIHZpZXc6IFZpZXdEYXRhKSB7XG4gIGNvbnN0IHByZXZSZW5kZXJOb2RlID0gcHJldlZpZXcgPyByZW5kZXJOb2RlKHByZXZWaWV3LCBwcmV2Vmlldy5kZWYubGFzdFJlbmRlclJvb3ROb2RlICEpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGVsZW1lbnREYXRhLnJlbmRlckVsZW1lbnQ7XG4gIGNvbnN0IHBhcmVudE5vZGUgPSB2aWV3LnJlbmRlcmVyLnBhcmVudE5vZGUocHJldlJlbmRlck5vZGUpO1xuICBjb25zdCBuZXh0U2libGluZyA9IHZpZXcucmVuZGVyZXIubmV4dFNpYmxpbmcocHJldlJlbmRlck5vZGUpO1xuICAvLyBOb3RlOiBXZSBjYW4ndCBjaGVjayBpZiBgbmV4dFNpYmxpbmdgIGlzIHByZXNlbnQsIGFzIG9uIFdlYldvcmtlcnMgaXQgd2lsbCBhbHdheXMgYmUhXG4gIC8vIEhvd2V2ZXIsIGJyb3dzZXJzIGF1dG9tYXRpY2FsbHkgZG8gYGFwcGVuZENoaWxkYCB3aGVuIHRoZXJlIGlzIG5vIGBuZXh0U2libGluZ2AuXG4gIHZpc2l0Um9vdFJlbmRlck5vZGVzKHZpZXcsIFJlbmRlck5vZGVBY3Rpb24uSW5zZXJ0QmVmb3JlLCBwYXJlbnROb2RlLCBuZXh0U2libGluZywgdW5kZWZpbmVkKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJlbmRlckRldGFjaFZpZXcodmlldzogVmlld0RhdGEpIHtcbiAgdmlzaXRSb290UmVuZGVyTm9kZXModmlldywgUmVuZGVyTm9kZUFjdGlvbi5SZW1vdmVDaGlsZCwgbnVsbCwgbnVsbCwgdW5kZWZpbmVkKTtcbn1cbiJdfQ==