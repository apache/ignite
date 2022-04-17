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
import { global } from '../../util/global';
/**
 * Returns whether the values are different from a change detection stand point.
 *
 * Constraints are relaxed in checkNoChanges mode. See `devModeEqual` for details.
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
export function isDifferent(a, b) {
    // NaN is the only value that is not equal to itself so the first
    // test checks if both a and b are not NaN
    return !(a !== a && b !== b) && a !== b;
}
/**
 * Used for stringify render output in Ivy.
 * Important! This function is very performance-sensitive and we should
 * be extra careful not to introduce megamorphic reads in it.
 * @param {?} value
 * @return {?}
 */
export function renderStringify(value) {
    if (typeof value === 'string')
        return value;
    if (value == null)
        return '';
    return '' + value;
}
/**
 * Used to stringify a value so that it can be displayed in an error message.
 * Important! This function contains a megamorphic read and should only be
 * used for error messages.
 * @param {?} value
 * @return {?}
 */
export function stringifyForError(value) {
    if (typeof value === 'function')
        return value.name || value.toString();
    if (typeof value === 'object' && value != null && typeof value.type === 'function') {
        return value.type.name || value.type.toString();
    }
    return renderStringify(value);
}
const ɵ0 = /**
 * @return {?}
 */
() => (typeof requestAnimationFrame !== 'undefined' && requestAnimationFrame || // browser only
    setTimeout // everything else
).bind(global);
/** @type {?} */
export const defaultScheduler = ((ɵ0))();
/**
 *
 * \@codeGenApi
 * @param {?} element
 * @return {?}
 */
export function ɵɵresolveWindow(element) {
    return { name: 'window', target: element.ownerDocument.defaultView };
}
/**
 *
 * \@codeGenApi
 * @param {?} element
 * @return {?}
 */
export function ɵɵresolveDocument(element) {
    return { name: 'document', target: element.ownerDocument };
}
/**
 *
 * \@codeGenApi
 * @param {?} element
 * @return {?}
 */
export function ɵɵresolveBody(element) {
    return { name: 'body', target: element.ownerDocument.body };
}
/**
 * The special delimiter we use to separate property names, prefixes, and suffixes
 * in property binding metadata. See storeBindingMetadata().
 *
 * We intentionally use the Unicode "REPLACEMENT CHARACTER" (U+FFFD) as a delimiter
 * because it is a very uncommon character that is unlikely to be part of a user's
 * property names or interpolation strings. If it is in fact used in a property
 * binding, DebugElement.properties will not return the correct value for that
 * binding. However, there should be no runtime effect for real applications.
 *
 * This character is typically rendered as a question mark inside of a diamond.
 * See https://en.wikipedia.org/wiki/Specials_(Unicode_block)
 *
 * @type {?}
 */
export const INTERPOLATION_DELIMITER = `�`;
/**
 * Determines whether or not the given string is a property metadata string.
 * See storeBindingMetadata().
 * @param {?} str
 * @return {?}
 */
export function isPropMetadataString(str) {
    return str.indexOf(INTERPOLATION_DELIMITER) >= 0;
}
/**
 * Unwrap a value which might be behind a closure (for forward declaration reasons).
 * @template T
 * @param {?} value
 * @return {?}
 */
export function maybeUnwrapFn(value) {
    if (value instanceof Function) {
        return value();
    }
    else {
        return value;
    }
}
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWlzY191dGlscy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3JlbmRlcjMvdXRpbC9taXNjX3V0aWxzLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLG1CQUFtQixDQUFDOzs7Ozs7Ozs7QUFTekMsTUFBTSxVQUFVLFdBQVcsQ0FBQyxDQUFNLEVBQUUsQ0FBTTtJQUN4QyxpRUFBaUU7SUFDakUsMENBQTBDO0lBQzFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7QUFDMUMsQ0FBQzs7Ozs7Ozs7QUFPRCxNQUFNLFVBQVUsZUFBZSxDQUFDLEtBQVU7SUFDeEMsSUFBSSxPQUFPLEtBQUssS0FBSyxRQUFRO1FBQUUsT0FBTyxLQUFLLENBQUM7SUFDNUMsSUFBSSxLQUFLLElBQUksSUFBSTtRQUFFLE9BQU8sRUFBRSxDQUFDO0lBQzdCLE9BQU8sRUFBRSxHQUFHLEtBQUssQ0FBQztBQUNwQixDQUFDOzs7Ozs7OztBQVFELE1BQU0sVUFBVSxpQkFBaUIsQ0FBQyxLQUFVO0lBQzFDLElBQUksT0FBTyxLQUFLLEtBQUssVUFBVTtRQUFFLE9BQU8sS0FBSyxDQUFDLElBQUksSUFBSSxLQUFLLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDdkUsSUFBSSxPQUFPLEtBQUssS0FBSyxRQUFRLElBQUksS0FBSyxJQUFJLElBQUksSUFBSSxPQUFPLEtBQUssQ0FBQyxJQUFJLEtBQUssVUFBVSxFQUFFO1FBQ2xGLE9BQU8sS0FBSyxDQUFDLElBQUksQ0FBQyxJQUFJLElBQUksS0FBSyxDQUFDLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQztLQUNqRDtJQUVELE9BQU8sZUFBZSxDQUFDLEtBQUssQ0FBQyxDQUFDO0FBQ2hDLENBQUM7Ozs7QUFJSSxHQUFHLEVBQUUsQ0FDRCxDQUFDLE9BQU8scUJBQXFCLEtBQUssV0FBVyxJQUFJLHFCQUFxQixJQUFLLGVBQWU7SUFDekYsVUFBVSxDQUFFLGtCQUFrQjtDQUM3QixDQUFDLElBQUksQ0FBQyxNQUFNLENBQUM7O0FBSnhCLE1BQU0sT0FBTyxnQkFBZ0IsR0FDekIsTUFHcUIsRUFBRTs7Ozs7OztBQU0zQixNQUFNLFVBQVUsZUFBZSxDQUFDLE9BQTZDO0lBQzNFLE9BQU8sRUFBQyxJQUFJLEVBQUUsUUFBUSxFQUFFLE1BQU0sRUFBRSxPQUFPLENBQUMsYUFBYSxDQUFDLFdBQVcsRUFBQyxDQUFDO0FBQ3JFLENBQUM7Ozs7Ozs7QUFNRCxNQUFNLFVBQVUsaUJBQWlCLENBQUMsT0FBNkM7SUFDN0UsT0FBTyxFQUFDLElBQUksRUFBRSxVQUFVLEVBQUUsTUFBTSxFQUFFLE9BQU8sQ0FBQyxhQUFhLEVBQUMsQ0FBQztBQUMzRCxDQUFDOzs7Ozs7O0FBTUQsTUFBTSxVQUFVLGFBQWEsQ0FBQyxPQUE2QztJQUN6RSxPQUFPLEVBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsT0FBTyxDQUFDLGFBQWEsQ0FBQyxJQUFJLEVBQUMsQ0FBQztBQUM1RCxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7O0FBZ0JELE1BQU0sT0FBTyx1QkFBdUIsR0FBRyxHQUFHOzs7Ozs7O0FBTTFDLE1BQU0sVUFBVSxvQkFBb0IsQ0FBQyxHQUFXO0lBQzlDLE9BQU8sR0FBRyxDQUFDLE9BQU8sQ0FBQyx1QkFBdUIsQ0FBQyxJQUFJLENBQUMsQ0FBQztBQUNuRCxDQUFDOzs7Ozs7O0FBS0QsTUFBTSxVQUFVLGFBQWEsQ0FBSSxLQUFvQjtJQUNuRCxJQUFJLEtBQUssWUFBWSxRQUFRLEVBQUU7UUFDN0IsT0FBTyxLQUFLLEVBQUUsQ0FBQztLQUNoQjtTQUFNO1FBQ0wsT0FBTyxLQUFLLENBQUM7S0FDZDtBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Z2xvYmFsfSBmcm9tICcuLi8uLi91dGlsL2dsb2JhbCc7XG5pbXBvcnQge1JFbGVtZW50fSBmcm9tICcuLi9pbnRlcmZhY2VzL3JlbmRlcmVyJztcbmltcG9ydCB7Tk9fQ0hBTkdFfSBmcm9tICcuLi90b2tlbnMnO1xuXG4vKipcbiAqIFJldHVybnMgd2hldGhlciB0aGUgdmFsdWVzIGFyZSBkaWZmZXJlbnQgZnJvbSBhIGNoYW5nZSBkZXRlY3Rpb24gc3RhbmQgcG9pbnQuXG4gKlxuICogQ29uc3RyYWludHMgYXJlIHJlbGF4ZWQgaW4gY2hlY2tOb0NoYW5nZXMgbW9kZS4gU2VlIGBkZXZNb2RlRXF1YWxgIGZvciBkZXRhaWxzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNEaWZmZXJlbnQoYTogYW55LCBiOiBhbnkpOiBib29sZWFuIHtcbiAgLy8gTmFOIGlzIHRoZSBvbmx5IHZhbHVlIHRoYXQgaXMgbm90IGVxdWFsIHRvIGl0c2VsZiBzbyB0aGUgZmlyc3RcbiAgLy8gdGVzdCBjaGVja3MgaWYgYm90aCBhIGFuZCBiIGFyZSBub3QgTmFOXG4gIHJldHVybiAhKGEgIT09IGEgJiYgYiAhPT0gYikgJiYgYSAhPT0gYjtcbn1cblxuLyoqXG4gKiBVc2VkIGZvciBzdHJpbmdpZnkgcmVuZGVyIG91dHB1dCBpbiBJdnkuXG4gKiBJbXBvcnRhbnQhIFRoaXMgZnVuY3Rpb24gaXMgdmVyeSBwZXJmb3JtYW5jZS1zZW5zaXRpdmUgYW5kIHdlIHNob3VsZFxuICogYmUgZXh0cmEgY2FyZWZ1bCBub3QgdG8gaW50cm9kdWNlIG1lZ2Ftb3JwaGljIHJlYWRzIGluIGl0LlxuICovXG5leHBvcnQgZnVuY3Rpb24gcmVuZGVyU3RyaW5naWZ5KHZhbHVlOiBhbnkpOiBzdHJpbmcge1xuICBpZiAodHlwZW9mIHZhbHVlID09PSAnc3RyaW5nJykgcmV0dXJuIHZhbHVlO1xuICBpZiAodmFsdWUgPT0gbnVsbCkgcmV0dXJuICcnO1xuICByZXR1cm4gJycgKyB2YWx1ZTtcbn1cblxuXG4vKipcbiAqIFVzZWQgdG8gc3RyaW5naWZ5IGEgdmFsdWUgc28gdGhhdCBpdCBjYW4gYmUgZGlzcGxheWVkIGluIGFuIGVycm9yIG1lc3NhZ2UuXG4gKiBJbXBvcnRhbnQhIFRoaXMgZnVuY3Rpb24gY29udGFpbnMgYSBtZWdhbW9ycGhpYyByZWFkIGFuZCBzaG91bGQgb25seSBiZVxuICogdXNlZCBmb3IgZXJyb3IgbWVzc2FnZXMuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBzdHJpbmdpZnlGb3JFcnJvcih2YWx1ZTogYW55KTogc3RyaW5nIHtcbiAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gJ2Z1bmN0aW9uJykgcmV0dXJuIHZhbHVlLm5hbWUgfHwgdmFsdWUudG9TdHJpbmcoKTtcbiAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gJ29iamVjdCcgJiYgdmFsdWUgIT0gbnVsbCAmJiB0eXBlb2YgdmFsdWUudHlwZSA9PT0gJ2Z1bmN0aW9uJykge1xuICAgIHJldHVybiB2YWx1ZS50eXBlLm5hbWUgfHwgdmFsdWUudHlwZS50b1N0cmluZygpO1xuICB9XG5cbiAgcmV0dXJuIHJlbmRlclN0cmluZ2lmeSh2YWx1ZSk7XG59XG5cblxuZXhwb3J0IGNvbnN0IGRlZmF1bHRTY2hlZHVsZXIgPVxuICAgICgoKSA9PlxuICAgICAgICAgKHR5cGVvZiByZXF1ZXN0QW5pbWF0aW9uRnJhbWUgIT09ICd1bmRlZmluZWQnICYmIHJlcXVlc3RBbmltYXRpb25GcmFtZSB8fCAgLy8gYnJvd3NlciBvbmx5XG4gICAgICAgICAgc2V0VGltZW91dCAgLy8gZXZlcnl0aGluZyBlbHNlXG4gICAgICAgICAgKS5iaW5kKGdsb2JhbCkpKCk7XG5cbi8qKlxuICpcbiAqIEBjb2RlR2VuQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1cmVzb2x2ZVdpbmRvdyhlbGVtZW50OiBSRWxlbWVudCAmIHtvd25lckRvY3VtZW50OiBEb2N1bWVudH0pIHtcbiAgcmV0dXJuIHtuYW1lOiAnd2luZG93JywgdGFyZ2V0OiBlbGVtZW50Lm93bmVyRG9jdW1lbnQuZGVmYXVsdFZpZXd9O1xufVxuXG4vKipcbiAqXG4gKiBAY29kZUdlbkFwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXJlc29sdmVEb2N1bWVudChlbGVtZW50OiBSRWxlbWVudCAmIHtvd25lckRvY3VtZW50OiBEb2N1bWVudH0pIHtcbiAgcmV0dXJuIHtuYW1lOiAnZG9jdW1lbnQnLCB0YXJnZXQ6IGVsZW1lbnQub3duZXJEb2N1bWVudH07XG59XG5cbi8qKlxuICpcbiAqIEBjb2RlR2VuQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1cmVzb2x2ZUJvZHkoZWxlbWVudDogUkVsZW1lbnQgJiB7b3duZXJEb2N1bWVudDogRG9jdW1lbnR9KSB7XG4gIHJldHVybiB7bmFtZTogJ2JvZHknLCB0YXJnZXQ6IGVsZW1lbnQub3duZXJEb2N1bWVudC5ib2R5fTtcbn1cblxuLyoqXG4gKiBUaGUgc3BlY2lhbCBkZWxpbWl0ZXIgd2UgdXNlIHRvIHNlcGFyYXRlIHByb3BlcnR5IG5hbWVzLCBwcmVmaXhlcywgYW5kIHN1ZmZpeGVzXG4gKiBpbiBwcm9wZXJ0eSBiaW5kaW5nIG1ldGFkYXRhLiBTZWUgc3RvcmVCaW5kaW5nTWV0YWRhdGEoKS5cbiAqXG4gKiBXZSBpbnRlbnRpb25hbGx5IHVzZSB0aGUgVW5pY29kZSBcIlJFUExBQ0VNRU5UIENIQVJBQ1RFUlwiIChVK0ZGRkQpIGFzIGEgZGVsaW1pdGVyXG4gKiBiZWNhdXNlIGl0IGlzIGEgdmVyeSB1bmNvbW1vbiBjaGFyYWN0ZXIgdGhhdCBpcyB1bmxpa2VseSB0byBiZSBwYXJ0IG9mIGEgdXNlcidzXG4gKiBwcm9wZXJ0eSBuYW1lcyBvciBpbnRlcnBvbGF0aW9uIHN0cmluZ3MuIElmIGl0IGlzIGluIGZhY3QgdXNlZCBpbiBhIHByb3BlcnR5XG4gKiBiaW5kaW5nLCBEZWJ1Z0VsZW1lbnQucHJvcGVydGllcyB3aWxsIG5vdCByZXR1cm4gdGhlIGNvcnJlY3QgdmFsdWUgZm9yIHRoYXRcbiAqIGJpbmRpbmcuIEhvd2V2ZXIsIHRoZXJlIHNob3VsZCBiZSBubyBydW50aW1lIGVmZmVjdCBmb3IgcmVhbCBhcHBsaWNhdGlvbnMuXG4gKlxuICogVGhpcyBjaGFyYWN0ZXIgaXMgdHlwaWNhbGx5IHJlbmRlcmVkIGFzIGEgcXVlc3Rpb24gbWFyayBpbnNpZGUgb2YgYSBkaWFtb25kLlxuICogU2VlIGh0dHBzOi8vZW4ud2lraXBlZGlhLm9yZy93aWtpL1NwZWNpYWxzXyhVbmljb2RlX2Jsb2NrKVxuICpcbiAqL1xuZXhwb3J0IGNvbnN0IElOVEVSUE9MQVRJT05fREVMSU1JVEVSID0gYO+/vWA7XG5cbi8qKlxuICogRGV0ZXJtaW5lcyB3aGV0aGVyIG9yIG5vdCB0aGUgZ2l2ZW4gc3RyaW5nIGlzIGEgcHJvcGVydHkgbWV0YWRhdGEgc3RyaW5nLlxuICogU2VlIHN0b3JlQmluZGluZ01ldGFkYXRhKCkuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc1Byb3BNZXRhZGF0YVN0cmluZyhzdHI6IHN0cmluZyk6IGJvb2xlYW4ge1xuICByZXR1cm4gc3RyLmluZGV4T2YoSU5URVJQT0xBVElPTl9ERUxJTUlURVIpID49IDA7XG59XG5cbi8qKlxuICogVW53cmFwIGEgdmFsdWUgd2hpY2ggbWlnaHQgYmUgYmVoaW5kIGEgY2xvc3VyZSAoZm9yIGZvcndhcmQgZGVjbGFyYXRpb24gcmVhc29ucykuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBtYXliZVVud3JhcEZuPFQ+KHZhbHVlOiBUIHwgKCgpID0+IFQpKTogVCB7XG4gIGlmICh2YWx1ZSBpbnN0YW5jZW9mIEZ1bmN0aW9uKSB7XG4gICAgcmV0dXJuIHZhbHVlKCk7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIHZhbHVlO1xuICB9XG59XG4iXX0=