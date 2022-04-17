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
import { DECLARATION_VIEW, T_HOST } from './interfaces/view';
import { getParentInjectorViewOffset } from './util/injector_utils';
/**
 * Unwraps a parent injector location number to find the view offset from the current injector,
 * then walks up the declaration view tree until the TNode of the parent injector is found.
 *
 * @param {?} location The location of the parent injector, which contains the view offset
 * @param {?} startView The LView instance from which to start walking up the view tree
 * @param {?} startTNode The TNode instance of the starting element
 * @return {?} The TNode of the parent injector
 */
export function getParentInjectorTNode(location, startView, startTNode) {
    if (startTNode.parent && startTNode.parent.injectorIndex !== -1) {
        // view offset is 0
        /** @type {?} */
        const injectorIndex = startTNode.parent.injectorIndex;
        /** @type {?} */
        let parentTNode = startTNode.parent;
        while (parentTNode.parent != null && injectorIndex == parentTNode.injectorIndex) {
            parentTNode = parentTNode.parent;
        }
        return parentTNode;
    }
    /** @type {?} */
    let viewOffset = getParentInjectorViewOffset(location);
    // view offset is 1
    /** @type {?} */
    let parentView = startView;
    /** @type {?} */
    let parentTNode = (/** @type {?} */ (startView[T_HOST]));
    // view offset is superior to 1
    while (viewOffset > 1) {
        parentView = (/** @type {?} */ (parentView[DECLARATION_VIEW]));
        parentTNode = (/** @type {?} */ (parentView[T_HOST]));
        viewOffset--;
    }
    return parentTNode;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibm9kZV91dGlsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9ub2RlX3V0aWwudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFVQSxPQUFPLEVBQUMsZ0JBQWdCLEVBQVMsTUFBTSxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDbEUsT0FBTyxFQUFDLDJCQUEyQixFQUFDLE1BQU0sdUJBQXVCLENBQUM7Ozs7Ozs7Ozs7QUFXbEUsTUFBTSxVQUFVLHNCQUFzQixDQUNsQyxRQUFrQyxFQUFFLFNBQWdCLEVBQUUsVUFBaUI7SUFFekUsSUFBSSxVQUFVLENBQUMsTUFBTSxJQUFJLFVBQVUsQ0FBQyxNQUFNLENBQUMsYUFBYSxLQUFLLENBQUMsQ0FBQyxFQUFFOzs7Y0FFekQsYUFBYSxHQUFHLFVBQVUsQ0FBQyxNQUFNLENBQUMsYUFBYTs7WUFDakQsV0FBVyxHQUFHLFVBQVUsQ0FBQyxNQUFNO1FBQ25DLE9BQU8sV0FBVyxDQUFDLE1BQU0sSUFBSSxJQUFJLElBQUksYUFBYSxJQUFJLFdBQVcsQ0FBQyxhQUFhLEVBQUU7WUFDL0UsV0FBVyxHQUFHLFdBQVcsQ0FBQyxNQUFNLENBQUM7U0FDbEM7UUFDRCxPQUFPLFdBQVcsQ0FBQztLQUNwQjs7UUFDRyxVQUFVLEdBQUcsMkJBQTJCLENBQUMsUUFBUSxDQUFDOzs7UUFFbEQsVUFBVSxHQUFHLFNBQVM7O1FBQ3RCLFdBQVcsR0FBRyxtQkFBQSxTQUFTLENBQUMsTUFBTSxDQUFDLEVBQWdCO0lBQ25ELCtCQUErQjtJQUMvQixPQUFPLFVBQVUsR0FBRyxDQUFDLEVBQUU7UUFDckIsVUFBVSxHQUFHLG1CQUFBLFVBQVUsQ0FBQyxnQkFBZ0IsQ0FBQyxFQUFFLENBQUM7UUFDNUMsV0FBVyxHQUFHLG1CQUFBLFVBQVUsQ0FBQyxNQUFNLENBQUMsRUFBZ0IsQ0FBQztRQUNqRCxVQUFVLEVBQUUsQ0FBQztLQUNkO0lBQ0QsT0FBTyxXQUFXLENBQUM7QUFDckIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtSZWxhdGl2ZUluamVjdG9yTG9jYXRpb259IGZyb20gJy4vaW50ZXJmYWNlcy9pbmplY3Rvcic7XG5pbXBvcnQge1RDb250YWluZXJOb2RlLCBURWxlbWVudE5vZGUsIFROb2RlfSBmcm9tICcuL2ludGVyZmFjZXMvbm9kZSc7XG5pbXBvcnQge0RFQ0xBUkFUSU9OX1ZJRVcsIExWaWV3LCBUX0hPU1R9IGZyb20gJy4vaW50ZXJmYWNlcy92aWV3JztcbmltcG9ydCB7Z2V0UGFyZW50SW5qZWN0b3JWaWV3T2Zmc2V0fSBmcm9tICcuL3V0aWwvaW5qZWN0b3JfdXRpbHMnO1xuXG4vKipcbiAqIFVud3JhcHMgYSBwYXJlbnQgaW5qZWN0b3IgbG9jYXRpb24gbnVtYmVyIHRvIGZpbmQgdGhlIHZpZXcgb2Zmc2V0IGZyb20gdGhlIGN1cnJlbnQgaW5qZWN0b3IsXG4gKiB0aGVuIHdhbGtzIHVwIHRoZSBkZWNsYXJhdGlvbiB2aWV3IHRyZWUgdW50aWwgdGhlIFROb2RlIG9mIHRoZSBwYXJlbnQgaW5qZWN0b3IgaXMgZm91bmQuXG4gKlxuICogQHBhcmFtIGxvY2F0aW9uIFRoZSBsb2NhdGlvbiBvZiB0aGUgcGFyZW50IGluamVjdG9yLCB3aGljaCBjb250YWlucyB0aGUgdmlldyBvZmZzZXRcbiAqIEBwYXJhbSBzdGFydFZpZXcgVGhlIExWaWV3IGluc3RhbmNlIGZyb20gd2hpY2ggdG8gc3RhcnQgd2Fsa2luZyB1cCB0aGUgdmlldyB0cmVlXG4gKiBAcGFyYW0gc3RhcnRUTm9kZSBUaGUgVE5vZGUgaW5zdGFuY2Ugb2YgdGhlIHN0YXJ0aW5nIGVsZW1lbnRcbiAqIEByZXR1cm5zIFRoZSBUTm9kZSBvZiB0aGUgcGFyZW50IGluamVjdG9yXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBnZXRQYXJlbnRJbmplY3RvclROb2RlKFxuICAgIGxvY2F0aW9uOiBSZWxhdGl2ZUluamVjdG9yTG9jYXRpb24sIHN0YXJ0VmlldzogTFZpZXcsIHN0YXJ0VE5vZGU6IFROb2RlKTogVEVsZW1lbnROb2RlfFxuICAgIFRDb250YWluZXJOb2RlfG51bGwge1xuICBpZiAoc3RhcnRUTm9kZS5wYXJlbnQgJiYgc3RhcnRUTm9kZS5wYXJlbnQuaW5qZWN0b3JJbmRleCAhPT0gLTEpIHtcbiAgICAvLyB2aWV3IG9mZnNldCBpcyAwXG4gICAgY29uc3QgaW5qZWN0b3JJbmRleCA9IHN0YXJ0VE5vZGUucGFyZW50LmluamVjdG9ySW5kZXg7XG4gICAgbGV0IHBhcmVudFROb2RlID0gc3RhcnRUTm9kZS5wYXJlbnQ7XG4gICAgd2hpbGUgKHBhcmVudFROb2RlLnBhcmVudCAhPSBudWxsICYmIGluamVjdG9ySW5kZXggPT0gcGFyZW50VE5vZGUuaW5qZWN0b3JJbmRleCkge1xuICAgICAgcGFyZW50VE5vZGUgPSBwYXJlbnRUTm9kZS5wYXJlbnQ7XG4gICAgfVxuICAgIHJldHVybiBwYXJlbnRUTm9kZTtcbiAgfVxuICBsZXQgdmlld09mZnNldCA9IGdldFBhcmVudEluamVjdG9yVmlld09mZnNldChsb2NhdGlvbik7XG4gIC8vIHZpZXcgb2Zmc2V0IGlzIDFcbiAgbGV0IHBhcmVudFZpZXcgPSBzdGFydFZpZXc7XG4gIGxldCBwYXJlbnRUTm9kZSA9IHN0YXJ0Vmlld1tUX0hPU1RdIGFzIFRFbGVtZW50Tm9kZTtcbiAgLy8gdmlldyBvZmZzZXQgaXMgc3VwZXJpb3IgdG8gMVxuICB3aGlsZSAodmlld09mZnNldCA+IDEpIHtcbiAgICBwYXJlbnRWaWV3ID0gcGFyZW50Vmlld1tERUNMQVJBVElPTl9WSUVXXSAhO1xuICAgIHBhcmVudFROb2RlID0gcGFyZW50Vmlld1tUX0hPU1RdIGFzIFRFbGVtZW50Tm9kZTtcbiAgICB2aWV3T2Zmc2V0LS07XG4gIH1cbiAgcmV0dXJuIHBhcmVudFROb2RlO1xufVxuIl19