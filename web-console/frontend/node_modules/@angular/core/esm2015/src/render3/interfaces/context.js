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
/**
 * This property will be monkey-patched on elements, components and directives
 * @type {?}
 */
export const MONKEY_PATCH_KEY_NAME = '__ngContext__';
/**
 * The internal view context which is specific to a given DOM element, directive or
 * component instance. Each value in here (besides the LView and element node details)
 * can be present, null or undefined. If undefined then it implies the value has not been
 * looked up yet, otherwise, if null, then a lookup was executed and nothing was found.
 *
 * Each value will get filled when the respective value is examined within the getContext
 * function. The component, element and each directive instance will share the same instance
 * of the context.
 * @record
 */
export function LContext() { }
if (false) {
    /**
     * The component's parent view data.
     * @type {?}
     */
    LContext.prototype.lView;
    /**
     * The index instance of the node.
     * @type {?}
     */
    LContext.prototype.nodeIndex;
    /**
     * The instance of the DOM node that is attached to the lNode.
     * @type {?}
     */
    LContext.prototype.native;
    /**
     * The instance of the Component node.
     * @type {?}
     */
    LContext.prototype.component;
    /**
     * The list of active directives that exist on this element.
     * @type {?}
     */
    LContext.prototype.directives;
    /**
     * The map of local references (local reference name => element or directive instance) that exist
     * on this element.
     * @type {?}
     */
    LContext.prototype.localRefs;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29udGV4dC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvaW50ZXJmYWNlcy9jb250ZXh0LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7OztBQWVBLE1BQU0sT0FBTyxxQkFBcUIsR0FBRyxlQUFlOzs7Ozs7Ozs7Ozs7QUFZcEQsOEJBK0JDOzs7Ozs7SUEzQkMseUJBQWE7Ozs7O0lBS2IsNkJBQWtCOzs7OztJQUtsQiwwQkFBYzs7Ozs7SUFLZCw2QkFBNkI7Ozs7O0lBSzdCLDhCQUFpQzs7Ozs7O0lBTWpDLDZCQUErQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG5pbXBvcnQge1JOb2RlfSBmcm9tICcuL3JlbmRlcmVyJztcbmltcG9ydCB7TFZpZXd9IGZyb20gJy4vdmlldyc7XG5cbi8qKlxuICogVGhpcyBwcm9wZXJ0eSB3aWxsIGJlIG1vbmtleS1wYXRjaGVkIG9uIGVsZW1lbnRzLCBjb21wb25lbnRzIGFuZCBkaXJlY3RpdmVzXG4gKi9cbmV4cG9ydCBjb25zdCBNT05LRVlfUEFUQ0hfS0VZX05BTUUgPSAnX19uZ0NvbnRleHRfXyc7XG5cbi8qKlxuICogVGhlIGludGVybmFsIHZpZXcgY29udGV4dCB3aGljaCBpcyBzcGVjaWZpYyB0byBhIGdpdmVuIERPTSBlbGVtZW50LCBkaXJlY3RpdmUgb3JcbiAqIGNvbXBvbmVudCBpbnN0YW5jZS4gRWFjaCB2YWx1ZSBpbiBoZXJlIChiZXNpZGVzIHRoZSBMVmlldyBhbmQgZWxlbWVudCBub2RlIGRldGFpbHMpXG4gKiBjYW4gYmUgcHJlc2VudCwgbnVsbCBvciB1bmRlZmluZWQuIElmIHVuZGVmaW5lZCB0aGVuIGl0IGltcGxpZXMgdGhlIHZhbHVlIGhhcyBub3QgYmVlblxuICogbG9va2VkIHVwIHlldCwgb3RoZXJ3aXNlLCBpZiBudWxsLCB0aGVuIGEgbG9va3VwIHdhcyBleGVjdXRlZCBhbmQgbm90aGluZyB3YXMgZm91bmQuXG4gKlxuICogRWFjaCB2YWx1ZSB3aWxsIGdldCBmaWxsZWQgd2hlbiB0aGUgcmVzcGVjdGl2ZSB2YWx1ZSBpcyBleGFtaW5lZCB3aXRoaW4gdGhlIGdldENvbnRleHRcbiAqIGZ1bmN0aW9uLiBUaGUgY29tcG9uZW50LCBlbGVtZW50IGFuZCBlYWNoIGRpcmVjdGl2ZSBpbnN0YW5jZSB3aWxsIHNoYXJlIHRoZSBzYW1lIGluc3RhbmNlXG4gKiBvZiB0aGUgY29udGV4dC5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBMQ29udGV4dCB7XG4gIC8qKlxuICAgKiBUaGUgY29tcG9uZW50J3MgcGFyZW50IHZpZXcgZGF0YS5cbiAgICovXG4gIGxWaWV3OiBMVmlldztcblxuICAvKipcbiAgICogVGhlIGluZGV4IGluc3RhbmNlIG9mIHRoZSBub2RlLlxuICAgKi9cbiAgbm9kZUluZGV4OiBudW1iZXI7XG5cbiAgLyoqXG4gICAqIFRoZSBpbnN0YW5jZSBvZiB0aGUgRE9NIG5vZGUgdGhhdCBpcyBhdHRhY2hlZCB0byB0aGUgbE5vZGUuXG4gICAqL1xuICBuYXRpdmU6IFJOb2RlO1xuXG4gIC8qKlxuICAgKiBUaGUgaW5zdGFuY2Ugb2YgdGhlIENvbXBvbmVudCBub2RlLlxuICAgKi9cbiAgY29tcG9uZW50OiB7fXxudWxsfHVuZGVmaW5lZDtcblxuICAvKipcbiAgICogVGhlIGxpc3Qgb2YgYWN0aXZlIGRpcmVjdGl2ZXMgdGhhdCBleGlzdCBvbiB0aGlzIGVsZW1lbnQuXG4gICAqL1xuICBkaXJlY3RpdmVzOiBhbnlbXXxudWxsfHVuZGVmaW5lZDtcblxuICAvKipcbiAgICogVGhlIG1hcCBvZiBsb2NhbCByZWZlcmVuY2VzIChsb2NhbCByZWZlcmVuY2UgbmFtZSA9PiBlbGVtZW50IG9yIGRpcmVjdGl2ZSBpbnN0YW5jZSkgdGhhdCBleGlzdFxuICAgKiBvbiB0aGlzIGVsZW1lbnQuXG4gICAqL1xuICBsb2NhbFJlZnM6IHtba2V5OiBzdHJpbmddOiBhbnl9fG51bGx8dW5kZWZpbmVkO1xufVxuIl19