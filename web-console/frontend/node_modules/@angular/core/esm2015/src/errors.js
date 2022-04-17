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
import { ERROR_DEBUG_CONTEXT, ERROR_LOGGER, ERROR_ORIGINAL_ERROR, ERROR_TYPE } from './util/errors';
/**
 * @param {?} error
 * @return {?}
 */
export function getType(error) {
    return ((/** @type {?} */ (error)))[ERROR_TYPE];
}
/**
 * @param {?} error
 * @return {?}
 */
export function getDebugContext(error) {
    return ((/** @type {?} */ (error)))[ERROR_DEBUG_CONTEXT];
}
/**
 * @param {?} error
 * @return {?}
 */
export function getOriginalError(error) {
    return ((/** @type {?} */ (error)))[ERROR_ORIGINAL_ERROR];
}
/**
 * @param {?} error
 * @return {?}
 */
export function getErrorLogger(error) {
    return ((/** @type {?} */ (error)))[ERROR_LOGGER] || defaultErrorLogger;
}
/**
 * @param {?} console
 * @param {...?} values
 * @return {?}
 */
function defaultErrorLogger(console, ...values) {
    ((/** @type {?} */ (console.error)))(...values);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXJyb3JzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvZXJyb3JzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLG1CQUFtQixFQUFFLFlBQVksRUFBRSxvQkFBb0IsRUFBRSxVQUFVLEVBQUMsTUFBTSxlQUFlLENBQUM7Ozs7O0FBR2xHLE1BQU0sVUFBVSxPQUFPLENBQUMsS0FBWTtJQUNsQyxPQUFPLENBQUMsbUJBQUEsS0FBSyxFQUFPLENBQUMsQ0FBQyxVQUFVLENBQUMsQ0FBQztBQUNwQyxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxlQUFlLENBQUMsS0FBWTtJQUMxQyxPQUFPLENBQUMsbUJBQUEsS0FBSyxFQUFPLENBQUMsQ0FBQyxtQkFBbUIsQ0FBQyxDQUFDO0FBQzdDLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLGdCQUFnQixDQUFDLEtBQVk7SUFDM0MsT0FBTyxDQUFDLG1CQUFBLEtBQUssRUFBTyxDQUFDLENBQUMsb0JBQW9CLENBQUMsQ0FBQztBQUM5QyxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxjQUFjLENBQUMsS0FBWTtJQUN6QyxPQUFPLENBQUMsbUJBQUEsS0FBSyxFQUFPLENBQUMsQ0FBQyxZQUFZLENBQUMsSUFBSSxrQkFBa0IsQ0FBQztBQUM1RCxDQUFDOzs7Ozs7QUFHRCxTQUFTLGtCQUFrQixDQUFDLE9BQWdCLEVBQUUsR0FBRyxNQUFhO0lBQzVELENBQUMsbUJBQUssT0FBTyxDQUFDLEtBQUssRUFBQSxDQUFDLENBQUMsR0FBRyxNQUFNLENBQUMsQ0FBQztBQUNsQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0VSUk9SX0RFQlVHX0NPTlRFWFQsIEVSUk9SX0xPR0dFUiwgRVJST1JfT1JJR0lOQUxfRVJST1IsIEVSUk9SX1RZUEV9IGZyb20gJy4vdXRpbC9lcnJvcnMnO1xuaW1wb3J0IHtEZWJ1Z0NvbnRleHR9IGZyb20gJy4vdmlldyc7XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRUeXBlKGVycm9yOiBFcnJvcik6IEZ1bmN0aW9uIHtcbiAgcmV0dXJuIChlcnJvciBhcyBhbnkpW0VSUk9SX1RZUEVdO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0RGVidWdDb250ZXh0KGVycm9yOiBFcnJvcik6IERlYnVnQ29udGV4dCB7XG4gIHJldHVybiAoZXJyb3IgYXMgYW55KVtFUlJPUl9ERUJVR19DT05URVhUXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldE9yaWdpbmFsRXJyb3IoZXJyb3I6IEVycm9yKTogRXJyb3Ige1xuICByZXR1cm4gKGVycm9yIGFzIGFueSlbRVJST1JfT1JJR0lOQUxfRVJST1JdO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0RXJyb3JMb2dnZXIoZXJyb3I6IEVycm9yKTogKGNvbnNvbGU6IENvbnNvbGUsIC4uLnZhbHVlczogYW55W10pID0+IHZvaWQge1xuICByZXR1cm4gKGVycm9yIGFzIGFueSlbRVJST1JfTE9HR0VSXSB8fCBkZWZhdWx0RXJyb3JMb2dnZXI7XG59XG5cblxuZnVuY3Rpb24gZGVmYXVsdEVycm9yTG9nZ2VyKGNvbnNvbGU6IENvbnNvbGUsIC4uLnZhbHVlczogYW55W10pIHtcbiAgKDxhbnk+Y29uc29sZS5lcnJvcikoLi4udmFsdWVzKTtcbn0iXX0=