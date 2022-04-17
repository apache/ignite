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
import { assertDefined, assertEqual, throwError } from '../util/assert';
import { getComponentDef, getNgModuleDef } from './definition';
import { isLContainer, isLView } from './interfaces/type_checks';
import { TVIEW } from './interfaces/view';
/**
 * @param {?} tNode
 * @param {?} lView
 * @return {?}
 */
export function assertTNodeForLView(tNode, lView) {
    tNode.hasOwnProperty('tView_') && assertEqual(((/** @type {?} */ ((/** @type {?} */ (tNode))))).tView_, lView[TVIEW], 'This TNode does not belong to this LView.');
}
/**
 * @param {?} actual
 * @param {?=} msg
 * @return {?}
 */
export function assertComponentType(actual, msg = 'Type passed in is not ComponentType, it does not have \'ngComponentDef\' property.') {
    if (!getComponentDef(actual)) {
        throwError(msg);
    }
}
/**
 * @param {?} actual
 * @param {?=} msg
 * @return {?}
 */
export function assertNgModuleType(actual, msg = 'Type passed in is not NgModuleType, it does not have \'ngModuleDef\' property.') {
    if (!getNgModuleDef(actual)) {
        throwError(msg);
    }
}
/**
 * @param {?} isParent
 * @return {?}
 */
export function assertPreviousIsParent(isParent) {
    assertEqual(isParent, true, 'previousOrParentTNode should be a parent');
}
/**
 * @param {?} tNode
 * @return {?}
 */
export function assertHasParent(tNode) {
    assertDefined(tNode, 'previousOrParentTNode should exist!');
    assertDefined((/** @type {?} */ (tNode)).parent, 'previousOrParentTNode should have a parent');
}
/**
 * @param {?} lView
 * @param {?} index
 * @param {?=} arr
 * @return {?}
 */
export function assertDataNext(lView, index, arr) {
    if (arr == null)
        arr = lView;
    assertEqual(arr.length, index, `index ${index} expected to be at the end of arr (length ${arr.length})`);
}
/**
 * @param {?} value
 * @return {?}
 */
export function assertLContainerOrUndefined(value) {
    value && assertEqual(isLContainer(value), true, 'Expecting LContainer or undefined or null');
}
/**
 * @param {?} value
 * @return {?}
 */
export function assertLContainer(value) {
    assertDefined(value, 'LContainer must be defined');
    assertEqual(isLContainer(value), true, 'Expecting LContainer');
}
/**
 * @param {?} value
 * @return {?}
 */
export function assertLViewOrUndefined(value) {
    value && assertEqual(isLView(value), true, 'Expecting LView or undefined or null');
}
/**
 * @param {?} value
 * @return {?}
 */
export function assertLView(value) {
    assertDefined(value, 'LView must be defined');
    assertEqual(isLView(value), true, 'Expecting LView');
}
/**
 * @param {?} tView
 * @param {?} errMessage
 * @return {?}
 */
export function assertFirstTemplatePass(tView, errMessage) {
    assertEqual(tView.firstTemplatePass, true, errMessage);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXNzZXJ0LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyMy9hc3NlcnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsYUFBYSxFQUFFLFdBQVcsRUFBRSxVQUFVLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUV0RSxPQUFPLEVBQUMsZUFBZSxFQUFFLGNBQWMsRUFBQyxNQUFNLGNBQWMsQ0FBQztBQUU3RCxPQUFPLEVBQUMsWUFBWSxFQUFFLE9BQU8sRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBQy9ELE9BQU8sRUFBUSxLQUFLLEVBQVEsTUFBTSxtQkFBbUIsQ0FBQzs7Ozs7O0FBRXRELE1BQU0sVUFBVSxtQkFBbUIsQ0FBQyxLQUFZLEVBQUUsS0FBWTtJQUM1RCxLQUFLLENBQUMsY0FBYyxDQUFDLFFBQVEsQ0FBQyxJQUFJLFdBQVcsQ0FDUCxDQUFDLG1CQUFBLG1CQUFBLEtBQUssRUFBTyxFQUFrQixDQUFDLENBQUMsTUFBTSxFQUFFLEtBQUssQ0FBQyxLQUFLLENBQUMsRUFDckQsMkNBQTJDLENBQUMsQ0FBQztBQUNyRixDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsbUJBQW1CLENBQy9CLE1BQVcsRUFDWCxNQUNJLG9GQUFvRjtJQUMxRixJQUFJLENBQUMsZUFBZSxDQUFDLE1BQU0sQ0FBQyxFQUFFO1FBQzVCLFVBQVUsQ0FBQyxHQUFHLENBQUMsQ0FBQztLQUNqQjtBQUNILENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxrQkFBa0IsQ0FDOUIsTUFBVyxFQUNYLE1BQ0ksZ0ZBQWdGO0lBQ3RGLElBQUksQ0FBQyxjQUFjLENBQUMsTUFBTSxDQUFDLEVBQUU7UUFDM0IsVUFBVSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0tBQ2pCO0FBQ0gsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsc0JBQXNCLENBQUMsUUFBaUI7SUFDdEQsV0FBVyxDQUFDLFFBQVEsRUFBRSxJQUFJLEVBQUUsMENBQTBDLENBQUMsQ0FBQztBQUMxRSxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxlQUFlLENBQUMsS0FBbUI7SUFDakQsYUFBYSxDQUFDLEtBQUssRUFBRSxxQ0FBcUMsQ0FBQyxDQUFDO0lBQzVELGFBQWEsQ0FBQyxtQkFBQSxLQUFLLEVBQUUsQ0FBQyxNQUFNLEVBQUUsNENBQTRDLENBQUMsQ0FBQztBQUM5RSxDQUFDOzs7Ozs7O0FBRUQsTUFBTSxVQUFVLGNBQWMsQ0FBQyxLQUFZLEVBQUUsS0FBYSxFQUFFLEdBQVc7SUFDckUsSUFBSSxHQUFHLElBQUksSUFBSTtRQUFFLEdBQUcsR0FBRyxLQUFLLENBQUM7SUFDN0IsV0FBVyxDQUNQLEdBQUcsQ0FBQyxNQUFNLEVBQUUsS0FBSyxFQUFFLFNBQVMsS0FBSyw2Q0FBNkMsR0FBRyxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUM7QUFDbkcsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsMkJBQTJCLENBQUMsS0FBVTtJQUNwRCxLQUFLLElBQUksV0FBVyxDQUFDLFlBQVksQ0FBQyxLQUFLLENBQUMsRUFBRSxJQUFJLEVBQUUsMkNBQTJDLENBQUMsQ0FBQztBQUMvRixDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxnQkFBZ0IsQ0FBQyxLQUFVO0lBQ3pDLGFBQWEsQ0FBQyxLQUFLLEVBQUUsNEJBQTRCLENBQUMsQ0FBQztJQUNuRCxXQUFXLENBQUMsWUFBWSxDQUFDLEtBQUssQ0FBQyxFQUFFLElBQUksRUFBRSxzQkFBc0IsQ0FBQyxDQUFDO0FBQ2pFLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLHNCQUFzQixDQUFDLEtBQVU7SUFDL0MsS0FBSyxJQUFJLFdBQVcsQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLEVBQUUsSUFBSSxFQUFFLHNDQUFzQyxDQUFDLENBQUM7QUFDckYsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsV0FBVyxDQUFDLEtBQVU7SUFDcEMsYUFBYSxDQUFDLEtBQUssRUFBRSx1QkFBdUIsQ0FBQyxDQUFDO0lBQzlDLFdBQVcsQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLEVBQUUsSUFBSSxFQUFFLGlCQUFpQixDQUFDLENBQUM7QUFDdkQsQ0FBQzs7Ozs7O0FBRUQsTUFBTSxVQUFVLHVCQUF1QixDQUFDLEtBQVksRUFBRSxVQUFrQjtJQUN0RSxXQUFXLENBQUMsS0FBSyxDQUFDLGlCQUFpQixFQUFFLElBQUksRUFBRSxVQUFVLENBQUMsQ0FBQztBQUN6RCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2Fzc2VydERlZmluZWQsIGFzc2VydEVxdWFsLCB0aHJvd0Vycm9yfSBmcm9tICcuLi91dGlsL2Fzc2VydCc7XG5cbmltcG9ydCB7Z2V0Q29tcG9uZW50RGVmLCBnZXROZ01vZHVsZURlZn0gZnJvbSAnLi9kZWZpbml0aW9uJztcbmltcG9ydCB7VE5vZGV9IGZyb20gJy4vaW50ZXJmYWNlcy9ub2RlJztcbmltcG9ydCB7aXNMQ29udGFpbmVyLCBpc0xWaWV3fSBmcm9tICcuL2ludGVyZmFjZXMvdHlwZV9jaGVja3MnO1xuaW1wb3J0IHtMVmlldywgVFZJRVcsIFRWaWV3fSBmcm9tICcuL2ludGVyZmFjZXMvdmlldyc7XG5cbmV4cG9ydCBmdW5jdGlvbiBhc3NlcnRUTm9kZUZvckxWaWV3KHROb2RlOiBUTm9kZSwgbFZpZXc6IExWaWV3KSB7XG4gIHROb2RlLmhhc093blByb3BlcnR5KCd0Vmlld18nKSAmJiBhc3NlcnRFcXVhbChcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAodE5vZGUgYXMgYW55IGFze3RWaWV3XzogVFZpZXd9KS50Vmlld18sIGxWaWV3W1RWSUVXXSxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAnVGhpcyBUTm9kZSBkb2VzIG5vdCBiZWxvbmcgdG8gdGhpcyBMVmlldy4nKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFzc2VydENvbXBvbmVudFR5cGUoXG4gICAgYWN0dWFsOiBhbnksXG4gICAgbXNnOiBzdHJpbmcgPVxuICAgICAgICAnVHlwZSBwYXNzZWQgaW4gaXMgbm90IENvbXBvbmVudFR5cGUsIGl0IGRvZXMgbm90IGhhdmUgXFwnbmdDb21wb25lbnREZWZcXCcgcHJvcGVydHkuJykge1xuICBpZiAoIWdldENvbXBvbmVudERlZihhY3R1YWwpKSB7XG4gICAgdGhyb3dFcnJvcihtc2cpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhc3NlcnROZ01vZHVsZVR5cGUoXG4gICAgYWN0dWFsOiBhbnksXG4gICAgbXNnOiBzdHJpbmcgPVxuICAgICAgICAnVHlwZSBwYXNzZWQgaW4gaXMgbm90IE5nTW9kdWxlVHlwZSwgaXQgZG9lcyBub3QgaGF2ZSBcXCduZ01vZHVsZURlZlxcJyBwcm9wZXJ0eS4nKSB7XG4gIGlmICghZ2V0TmdNb2R1bGVEZWYoYWN0dWFsKSkge1xuICAgIHRocm93RXJyb3IobXNnKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gYXNzZXJ0UHJldmlvdXNJc1BhcmVudChpc1BhcmVudDogYm9vbGVhbikge1xuICBhc3NlcnRFcXVhbChpc1BhcmVudCwgdHJ1ZSwgJ3ByZXZpb3VzT3JQYXJlbnRUTm9kZSBzaG91bGQgYmUgYSBwYXJlbnQnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFzc2VydEhhc1BhcmVudCh0Tm9kZTogVE5vZGUgfCBudWxsKSB7XG4gIGFzc2VydERlZmluZWQodE5vZGUsICdwcmV2aW91c09yUGFyZW50VE5vZGUgc2hvdWxkIGV4aXN0IScpO1xuICBhc3NlcnREZWZpbmVkKHROb2RlICEucGFyZW50LCAncHJldmlvdXNPclBhcmVudFROb2RlIHNob3VsZCBoYXZlIGEgcGFyZW50Jyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhc3NlcnREYXRhTmV4dChsVmlldzogTFZpZXcsIGluZGV4OiBudW1iZXIsIGFycj86IGFueVtdKSB7XG4gIGlmIChhcnIgPT0gbnVsbCkgYXJyID0gbFZpZXc7XG4gIGFzc2VydEVxdWFsKFxuICAgICAgYXJyLmxlbmd0aCwgaW5kZXgsIGBpbmRleCAke2luZGV4fSBleHBlY3RlZCB0byBiZSBhdCB0aGUgZW5kIG9mIGFyciAobGVuZ3RoICR7YXJyLmxlbmd0aH0pYCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhc3NlcnRMQ29udGFpbmVyT3JVbmRlZmluZWQodmFsdWU6IGFueSk6IHZvaWQge1xuICB2YWx1ZSAmJiBhc3NlcnRFcXVhbChpc0xDb250YWluZXIodmFsdWUpLCB0cnVlLCAnRXhwZWN0aW5nIExDb250YWluZXIgb3IgdW5kZWZpbmVkIG9yIG51bGwnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFzc2VydExDb250YWluZXIodmFsdWU6IGFueSk6IHZvaWQge1xuICBhc3NlcnREZWZpbmVkKHZhbHVlLCAnTENvbnRhaW5lciBtdXN0IGJlIGRlZmluZWQnKTtcbiAgYXNzZXJ0RXF1YWwoaXNMQ29udGFpbmVyKHZhbHVlKSwgdHJ1ZSwgJ0V4cGVjdGluZyBMQ29udGFpbmVyJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhc3NlcnRMVmlld09yVW5kZWZpbmVkKHZhbHVlOiBhbnkpOiB2b2lkIHtcbiAgdmFsdWUgJiYgYXNzZXJ0RXF1YWwoaXNMVmlldyh2YWx1ZSksIHRydWUsICdFeHBlY3RpbmcgTFZpZXcgb3IgdW5kZWZpbmVkIG9yIG51bGwnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFzc2VydExWaWV3KHZhbHVlOiBhbnkpIHtcbiAgYXNzZXJ0RGVmaW5lZCh2YWx1ZSwgJ0xWaWV3IG11c3QgYmUgZGVmaW5lZCcpO1xuICBhc3NlcnRFcXVhbChpc0xWaWV3KHZhbHVlKSwgdHJ1ZSwgJ0V4cGVjdGluZyBMVmlldycpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gYXNzZXJ0Rmlyc3RUZW1wbGF0ZVBhc3ModFZpZXc6IFRWaWV3LCBlcnJNZXNzYWdlOiBzdHJpbmcpIHtcbiAgYXNzZXJ0RXF1YWwodFZpZXcuZmlyc3RUZW1wbGF0ZVBhc3MsIHRydWUsIGVyck1lc3NhZ2UpO1xufVxuIl19