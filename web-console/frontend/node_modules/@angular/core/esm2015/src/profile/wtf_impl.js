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
import { global } from '../util/global';
/**
 * A scope function for the Web Tracing Framework (WTF).
 *
 * \@publicApi
 * @deprecated the Web Tracing Framework is no longer supported in Angular
 * @record
 */
export function WtfScopeFn() { }
/**
 * @record
 */
function WTF() { }
if (false) {
    /** @type {?} */
    WTF.prototype.trace;
}
/**
 * @record
 */
function Trace() { }
if (false) {
    /** @type {?} */
    Trace.prototype.events;
    /**
     * @param {?} scope
     * @param {?} returnValue
     * @return {?}
     */
    Trace.prototype.leaveScope = function (scope, returnValue) { };
    /**
     * @param {?} rangeType
     * @param {?} action
     * @return {?}
     */
    Trace.prototype.beginTimeRange = function (rangeType, action) { };
    /**
     * @param {?} range
     * @return {?}
     */
    Trace.prototype.endTimeRange = function (range) { };
}
/**
 * @record
 */
export function Range() { }
/**
 * @record
 */
function Events() { }
if (false) {
    /**
     * @param {?} signature
     * @param {?} flags
     * @return {?}
     */
    Events.prototype.createScope = function (signature, flags) { };
}
/**
 * @record
 */
export function Scope() { }
/** @type {?} */
let trace;
/** @type {?} */
let events;
/**
 * @return {?}
 */
export function detectWTF() {
    /** @type {?} */
    const wtf = ((/** @type {?} */ (global)))['wtf'];
    if (wtf) {
        trace = wtf['trace'];
        if (trace) {
            events = trace['events'];
            return true;
        }
    }
    return false;
}
/**
 * @param {?} signature
 * @param {?=} flags
 * @return {?}
 */
export function createScope(signature, flags = null) {
    return events.createScope(signature, flags);
}
/**
 * @template T
 * @param {?} scope
 * @param {?=} returnValue
 * @return {?}
 */
export function leave(scope, returnValue) {
    trace.leaveScope(scope, returnValue);
    return returnValue;
}
/**
 * @param {?} rangeType
 * @param {?} action
 * @return {?}
 */
export function startTimeRange(rangeType, action) {
    return trace.beginTimeRange(rangeType, action);
}
/**
 * @param {?} range
 * @return {?}
 */
export function endTimeRange(range) {
    trace.endTimeRange(range);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoid3RmX2ltcGwuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9wcm9maWxlL3d0Zl9pbXBsLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLGdCQUFnQixDQUFDOzs7Ozs7OztBQVF0QyxnQ0FBOEQ7Ozs7QUFFOUQsa0JBRUM7OztJQURDLG9CQUFhOzs7OztBQUdmLG9CQUtDOzs7SUFKQyx1QkFBZTs7Ozs7O0lBQ2YsK0RBQWtFOzs7Ozs7SUFDbEUsa0VBQXlEOzs7OztJQUN6RCxvREFBa0Q7Ozs7O0FBR3BELDJCQUF5Qjs7OztBQUV6QixxQkFFQzs7Ozs7OztJQURDLCtEQUFrRDs7Ozs7QUFHcEQsMkJBQW1FOztJQUUvRCxLQUFZOztJQUNaLE1BQWM7Ozs7QUFFbEIsTUFBTSxVQUFVLFNBQVM7O1VBQ2pCLEdBQUcsR0FBUSxDQUFDLG1CQUFBLE1BQU0sRUFBTyxDQUFtQixDQUFDLEtBQUssQ0FBQztJQUN6RCxJQUFJLEdBQUcsRUFBRTtRQUNQLEtBQUssR0FBRyxHQUFHLENBQUMsT0FBTyxDQUFDLENBQUM7UUFDckIsSUFBSSxLQUFLLEVBQUU7WUFDVCxNQUFNLEdBQUcsS0FBSyxDQUFDLFFBQVEsQ0FBQyxDQUFDO1lBQ3pCLE9BQU8sSUFBSSxDQUFDO1NBQ2I7S0FDRjtJQUNELE9BQU8sS0FBSyxDQUFDO0FBQ2YsQ0FBQzs7Ozs7O0FBRUQsTUFBTSxVQUFVLFdBQVcsQ0FBQyxTQUFpQixFQUFFLFFBQWEsSUFBSTtJQUM5RCxPQUFPLE1BQU0sQ0FBQyxXQUFXLENBQUMsU0FBUyxFQUFFLEtBQUssQ0FBQyxDQUFDO0FBQzlDLENBQUM7Ozs7Ozs7QUFJRCxNQUFNLFVBQVUsS0FBSyxDQUFJLEtBQVksRUFBRSxXQUFpQjtJQUN0RCxLQUFLLENBQUMsVUFBVSxDQUFDLEtBQUssRUFBRSxXQUFXLENBQUMsQ0FBQztJQUNyQyxPQUFPLFdBQVcsQ0FBQztBQUNyQixDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsY0FBYyxDQUFDLFNBQWlCLEVBQUUsTUFBYztJQUM5RCxPQUFPLEtBQUssQ0FBQyxjQUFjLENBQUMsU0FBUyxFQUFFLE1BQU0sQ0FBQyxDQUFDO0FBQ2pELENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLFlBQVksQ0FBQyxLQUFZO0lBQ3ZDLEtBQUssQ0FBQyxZQUFZLENBQUMsS0FBSyxDQUFDLENBQUM7QUFDNUIsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtnbG9iYWx9IGZyb20gJy4uL3V0aWwvZ2xvYmFsJztcblxuLyoqXG4gKiBBIHNjb3BlIGZ1bmN0aW9uIGZvciB0aGUgV2ViIFRyYWNpbmcgRnJhbWV3b3JrIChXVEYpLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqIEBkZXByZWNhdGVkIHRoZSBXZWIgVHJhY2luZyBGcmFtZXdvcmsgaXMgbm8gbG9uZ2VyIHN1cHBvcnRlZCBpbiBBbmd1bGFyXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgV3RmU2NvcGVGbiB7IChhcmcwPzogYW55LCBhcmcxPzogYW55KTogYW55OyB9XG5cbmludGVyZmFjZSBXVEYge1xuICB0cmFjZTogVHJhY2U7XG59XG5cbmludGVyZmFjZSBUcmFjZSB7XG4gIGV2ZW50czogRXZlbnRzO1xuICBsZWF2ZVNjb3BlKHNjb3BlOiBTY29wZSwgcmV0dXJuVmFsdWU6IGFueSk6IGFueSAvKiogVE9ETyAjOTEwMCAqLztcbiAgYmVnaW5UaW1lUmFuZ2UocmFuZ2VUeXBlOiBzdHJpbmcsIGFjdGlvbjogc3RyaW5nKTogUmFuZ2U7XG4gIGVuZFRpbWVSYW5nZShyYW5nZTogUmFuZ2UpOiBhbnkgLyoqIFRPRE8gIzkxMDAgKi87XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgUmFuZ2Uge31cblxuaW50ZXJmYWNlIEV2ZW50cyB7XG4gIGNyZWF0ZVNjb3BlKHNpZ25hdHVyZTogc3RyaW5nLCBmbGFnczogYW55KTogU2NvcGU7XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgU2NvcGUgeyAoLi4uYXJnczogYW55W10gLyoqIFRPRE8gIzkxMDAgKi8pOiBhbnk7IH1cblxubGV0IHRyYWNlOiBUcmFjZTtcbmxldCBldmVudHM6IEV2ZW50cztcblxuZXhwb3J0IGZ1bmN0aW9uIGRldGVjdFdURigpOiBib29sZWFuIHtcbiAgY29uc3Qgd3RmOiBXVEYgPSAoZ2xvYmFsIGFzIGFueSAvKiogVE9ETyAjOTEwMCAqLylbJ3d0ZiddO1xuICBpZiAod3RmKSB7XG4gICAgdHJhY2UgPSB3dGZbJ3RyYWNlJ107XG4gICAgaWYgKHRyYWNlKSB7XG4gICAgICBldmVudHMgPSB0cmFjZVsnZXZlbnRzJ107XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGZhbHNlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlU2NvcGUoc2lnbmF0dXJlOiBzdHJpbmcsIGZsYWdzOiBhbnkgPSBudWxsKTogYW55IHtcbiAgcmV0dXJuIGV2ZW50cy5jcmVhdGVTY29wZShzaWduYXR1cmUsIGZsYWdzKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGxlYXZlPFQ+KHNjb3BlOiBTY29wZSk6IHZvaWQ7XG5leHBvcnQgZnVuY3Rpb24gbGVhdmU8VD4oc2NvcGU6IFNjb3BlLCByZXR1cm5WYWx1ZT86IFQpOiBUO1xuZXhwb3J0IGZ1bmN0aW9uIGxlYXZlPFQ+KHNjb3BlOiBTY29wZSwgcmV0dXJuVmFsdWU/OiBhbnkpOiBhbnkge1xuICB0cmFjZS5sZWF2ZVNjb3BlKHNjb3BlLCByZXR1cm5WYWx1ZSk7XG4gIHJldHVybiByZXR1cm5WYWx1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN0YXJ0VGltZVJhbmdlKHJhbmdlVHlwZTogc3RyaW5nLCBhY3Rpb246IHN0cmluZyk6IFJhbmdlIHtcbiAgcmV0dXJuIHRyYWNlLmJlZ2luVGltZVJhbmdlKHJhbmdlVHlwZSwgYWN0aW9uKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGVuZFRpbWVSYW5nZShyYW5nZTogUmFuZ2UpOiB2b2lkIHtcbiAgdHJhY2UuZW5kVGltZVJhbmdlKHJhbmdlKTtcbn1cbiJdfQ==