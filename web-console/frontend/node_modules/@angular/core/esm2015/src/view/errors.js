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
import { getDebugContext } from '../errors';
import { ERROR_DEBUG_CONTEXT, ERROR_LOGGER } from '../util/errors';
/**
 * @param {?} context
 * @param {?} oldValue
 * @param {?} currValue
 * @param {?} isFirstCheck
 * @return {?}
 */
export function expressionChangedAfterItHasBeenCheckedError(context, oldValue, currValue, isFirstCheck) {
    /** @type {?} */
    let msg = `ExpressionChangedAfterItHasBeenCheckedError: Expression has changed after it was checked. Previous value: '${oldValue}'. Current value: '${currValue}'.`;
    if (isFirstCheck) {
        msg +=
            ` It seems like the view has been created after its parent and its children have been dirty checked.` +
                ` Has it been created in a change detection hook ?`;
    }
    return viewDebugError(msg, context);
}
/**
 * @param {?} err
 * @param {?} context
 * @return {?}
 */
export function viewWrappedDebugError(err, context) {
    if (!(err instanceof Error)) {
        // errors that are not Error instances don't have a stack,
        // so it is ok to wrap them into a new Error object...
        err = new Error(err.toString());
    }
    _addDebugContext(err, context);
    return err;
}
/**
 * @param {?} msg
 * @param {?} context
 * @return {?}
 */
export function viewDebugError(msg, context) {
    /** @type {?} */
    const err = new Error(msg);
    _addDebugContext(err, context);
    return err;
}
/**
 * @param {?} err
 * @param {?} context
 * @return {?}
 */
function _addDebugContext(err, context) {
    ((/** @type {?} */ (err)))[ERROR_DEBUG_CONTEXT] = context;
    ((/** @type {?} */ (err)))[ERROR_LOGGER] = context.logError.bind(context);
}
/**
 * @param {?} err
 * @return {?}
 */
export function isViewDebugError(err) {
    return !!getDebugContext(err);
}
/**
 * @param {?} action
 * @return {?}
 */
export function viewDestroyedError(action) {
    return new Error(`ViewDestroyedError: Attempt to use a destroyed view: ${action}`);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXJyb3JzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvdmlldy9lcnJvcnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsZUFBZSxFQUFDLE1BQU0sV0FBVyxDQUFDO0FBQzFDLE9BQU8sRUFBQyxtQkFBbUIsRUFBRSxZQUFZLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQzs7Ozs7Ozs7QUFJakUsTUFBTSxVQUFVLDJDQUEyQyxDQUN2RCxPQUFxQixFQUFFLFFBQWEsRUFBRSxTQUFjLEVBQUUsWUFBcUI7O1FBQ3pFLEdBQUcsR0FDSCw4R0FBOEcsUUFBUSxzQkFBc0IsU0FBUyxJQUFJO0lBQzdKLElBQUksWUFBWSxFQUFFO1FBQ2hCLEdBQUc7WUFDQyxxR0FBcUc7Z0JBQ3JHLG1EQUFtRCxDQUFDO0tBQ3pEO0lBQ0QsT0FBTyxjQUFjLENBQUMsR0FBRyxFQUFFLE9BQU8sQ0FBQyxDQUFDO0FBQ3RDLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxxQkFBcUIsQ0FBQyxHQUFRLEVBQUUsT0FBcUI7SUFDbkUsSUFBSSxDQUFDLENBQUMsR0FBRyxZQUFZLEtBQUssQ0FBQyxFQUFFO1FBQzNCLDBEQUEwRDtRQUMxRCxzREFBc0Q7UUFDdEQsR0FBRyxHQUFHLElBQUksS0FBSyxDQUFDLEdBQUcsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxDQUFDO0tBQ2pDO0lBQ0QsZ0JBQWdCLENBQUMsR0FBRyxFQUFFLE9BQU8sQ0FBQyxDQUFDO0lBQy9CLE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQzs7Ozs7O0FBRUQsTUFBTSxVQUFVLGNBQWMsQ0FBQyxHQUFXLEVBQUUsT0FBcUI7O1VBQ3pELEdBQUcsR0FBRyxJQUFJLEtBQUssQ0FBQyxHQUFHLENBQUM7SUFDMUIsZ0JBQWdCLENBQUMsR0FBRyxFQUFFLE9BQU8sQ0FBQyxDQUFDO0lBQy9CLE9BQU8sR0FBRyxDQUFDO0FBQ2IsQ0FBQzs7Ozs7O0FBRUQsU0FBUyxnQkFBZ0IsQ0FBQyxHQUFVLEVBQUUsT0FBcUI7SUFDekQsQ0FBQyxtQkFBQSxHQUFHLEVBQU8sQ0FBQyxDQUFDLG1CQUFtQixDQUFDLEdBQUcsT0FBTyxDQUFDO0lBQzVDLENBQUMsbUJBQUEsR0FBRyxFQUFPLENBQUMsQ0FBQyxZQUFZLENBQUMsR0FBRyxPQUFPLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQztBQUM5RCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxnQkFBZ0IsQ0FBQyxHQUFVO0lBQ3pDLE9BQU8sQ0FBQyxDQUFDLGVBQWUsQ0FBQyxHQUFHLENBQUMsQ0FBQztBQUNoQyxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxrQkFBa0IsQ0FBQyxNQUFjO0lBQy9DLE9BQU8sSUFBSSxLQUFLLENBQUMsd0RBQXdELE1BQU0sRUFBRSxDQUFDLENBQUM7QUFDckYsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtnZXREZWJ1Z0NvbnRleHR9IGZyb20gJy4uL2Vycm9ycyc7XG5pbXBvcnQge0VSUk9SX0RFQlVHX0NPTlRFWFQsIEVSUk9SX0xPR0dFUn0gZnJvbSAnLi4vdXRpbC9lcnJvcnMnO1xuXG5pbXBvcnQge0RlYnVnQ29udGV4dH0gZnJvbSAnLi90eXBlcyc7XG5cbmV4cG9ydCBmdW5jdGlvbiBleHByZXNzaW9uQ2hhbmdlZEFmdGVySXRIYXNCZWVuQ2hlY2tlZEVycm9yKFxuICAgIGNvbnRleHQ6IERlYnVnQ29udGV4dCwgb2xkVmFsdWU6IGFueSwgY3VyclZhbHVlOiBhbnksIGlzRmlyc3RDaGVjazogYm9vbGVhbik6IEVycm9yIHtcbiAgbGV0IG1zZyA9XG4gICAgICBgRXhwcmVzc2lvbkNoYW5nZWRBZnRlckl0SGFzQmVlbkNoZWNrZWRFcnJvcjogRXhwcmVzc2lvbiBoYXMgY2hhbmdlZCBhZnRlciBpdCB3YXMgY2hlY2tlZC4gUHJldmlvdXMgdmFsdWU6ICcke29sZFZhbHVlfScuIEN1cnJlbnQgdmFsdWU6ICcke2N1cnJWYWx1ZX0nLmA7XG4gIGlmIChpc0ZpcnN0Q2hlY2spIHtcbiAgICBtc2cgKz1cbiAgICAgICAgYCBJdCBzZWVtcyBsaWtlIHRoZSB2aWV3IGhhcyBiZWVuIGNyZWF0ZWQgYWZ0ZXIgaXRzIHBhcmVudCBhbmQgaXRzIGNoaWxkcmVuIGhhdmUgYmVlbiBkaXJ0eSBjaGVja2VkLmAgK1xuICAgICAgICBgIEhhcyBpdCBiZWVuIGNyZWF0ZWQgaW4gYSBjaGFuZ2UgZGV0ZWN0aW9uIGhvb2sgP2A7XG4gIH1cbiAgcmV0dXJuIHZpZXdEZWJ1Z0Vycm9yKG1zZywgY29udGV4dCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2aWV3V3JhcHBlZERlYnVnRXJyb3IoZXJyOiBhbnksIGNvbnRleHQ6IERlYnVnQ29udGV4dCk6IEVycm9yIHtcbiAgaWYgKCEoZXJyIGluc3RhbmNlb2YgRXJyb3IpKSB7XG4gICAgLy8gZXJyb3JzIHRoYXQgYXJlIG5vdCBFcnJvciBpbnN0YW5jZXMgZG9uJ3QgaGF2ZSBhIHN0YWNrLFxuICAgIC8vIHNvIGl0IGlzIG9rIHRvIHdyYXAgdGhlbSBpbnRvIGEgbmV3IEVycm9yIG9iamVjdC4uLlxuICAgIGVyciA9IG5ldyBFcnJvcihlcnIudG9TdHJpbmcoKSk7XG4gIH1cbiAgX2FkZERlYnVnQ29udGV4dChlcnIsIGNvbnRleHQpO1xuICByZXR1cm4gZXJyO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdmlld0RlYnVnRXJyb3IobXNnOiBzdHJpbmcsIGNvbnRleHQ6IERlYnVnQ29udGV4dCk6IEVycm9yIHtcbiAgY29uc3QgZXJyID0gbmV3IEVycm9yKG1zZyk7XG4gIF9hZGREZWJ1Z0NvbnRleHQoZXJyLCBjb250ZXh0KTtcbiAgcmV0dXJuIGVycjtcbn1cblxuZnVuY3Rpb24gX2FkZERlYnVnQ29udGV4dChlcnI6IEVycm9yLCBjb250ZXh0OiBEZWJ1Z0NvbnRleHQpIHtcbiAgKGVyciBhcyBhbnkpW0VSUk9SX0RFQlVHX0NPTlRFWFRdID0gY29udGV4dDtcbiAgKGVyciBhcyBhbnkpW0VSUk9SX0xPR0dFUl0gPSBjb250ZXh0LmxvZ0Vycm9yLmJpbmQoY29udGV4dCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1ZpZXdEZWJ1Z0Vycm9yKGVycjogRXJyb3IpOiBib29sZWFuIHtcbiAgcmV0dXJuICEhZ2V0RGVidWdDb250ZXh0KGVycik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB2aWV3RGVzdHJveWVkRXJyb3IoYWN0aW9uOiBzdHJpbmcpOiBFcnJvciB7XG4gIHJldHVybiBuZXcgRXJyb3IoYFZpZXdEZXN0cm95ZWRFcnJvcjogQXR0ZW1wdCB0byB1c2UgYSBkZXN0cm95ZWQgdmlldzogJHthY3Rpb259YCk7XG59XG4iXX0=