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
import '../util/ng_dev_mode';
/**
 * This file contains reuseable "empty" symbols that can be used as default return values
 * in different parts of the rendering code. Because the same symbols are returned, this
 * allows for identity checks against these values to be consistently used by the framework
 * code.
 * @type {?}
 */
export const EMPTY_OBJ = {};
/** @type {?} */
export const EMPTY_ARRAY = [];
// freezing the values prevents any code from accidentally inserting new values in
if (typeof ngDevMode !== 'undefined' && ngDevMode) {
    // These property accesses can be ignored because ngDevMode will be set to false
    // when optimizing code and the whole if statement will be dropped.
    // tslint:disable-next-line:no-toplevel-property-access
    Object.freeze(EMPTY_OBJ);
    // tslint:disable-next-line:no-toplevel-property-access
    Object.freeze(EMPTY_ARRAY);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZW1wdHkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9yZW5kZXIzL2VtcHR5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBT0EsT0FBTyxxQkFBcUIsQ0FBQzs7Ozs7Ozs7QUFTN0IsTUFBTSxPQUFPLFNBQVMsR0FBTyxFQUFFOztBQUMvQixNQUFNLE9BQU8sV0FBVyxHQUFVLEVBQUU7O0FBR3BDLElBQUksT0FBTyxTQUFTLEtBQUssV0FBVyxJQUFJLFNBQVMsRUFBRTtJQUNqRCxnRkFBZ0Y7SUFDaEYsbUVBQW1FO0lBQ25FLHVEQUF1RDtJQUN2RCxNQUFNLENBQUMsTUFBTSxDQUFDLFNBQVMsQ0FBQyxDQUFDO0lBQ3pCLHVEQUF1RDtJQUN2RCxNQUFNLENBQUMsTUFBTSxDQUFDLFdBQVcsQ0FBQyxDQUFDO0NBQzVCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4qIEBsaWNlbnNlXG4qIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuKlxuKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4qL1xuaW1wb3J0ICcuLi91dGlsL25nX2Rldl9tb2RlJztcblxuLyoqXG4gKiBUaGlzIGZpbGUgY29udGFpbnMgcmV1c2VhYmxlIFwiZW1wdHlcIiBzeW1ib2xzIHRoYXQgY2FuIGJlIHVzZWQgYXMgZGVmYXVsdCByZXR1cm4gdmFsdWVzXG4gKiBpbiBkaWZmZXJlbnQgcGFydHMgb2YgdGhlIHJlbmRlcmluZyBjb2RlLiBCZWNhdXNlIHRoZSBzYW1lIHN5bWJvbHMgYXJlIHJldHVybmVkLCB0aGlzXG4gKiBhbGxvd3MgZm9yIGlkZW50aXR5IGNoZWNrcyBhZ2FpbnN0IHRoZXNlIHZhbHVlcyB0byBiZSBjb25zaXN0ZW50bHkgdXNlZCBieSB0aGUgZnJhbWV3b3JrXG4gKiBjb2RlLlxuICovXG5cbmV4cG9ydCBjb25zdCBFTVBUWV9PQko6IHt9ID0ge307XG5leHBvcnQgY29uc3QgRU1QVFlfQVJSQVk6IGFueVtdID0gW107XG5cbi8vIGZyZWV6aW5nIHRoZSB2YWx1ZXMgcHJldmVudHMgYW55IGNvZGUgZnJvbSBhY2NpZGVudGFsbHkgaW5zZXJ0aW5nIG5ldyB2YWx1ZXMgaW5cbmlmICh0eXBlb2YgbmdEZXZNb2RlICE9PSAndW5kZWZpbmVkJyAmJiBuZ0Rldk1vZGUpIHtcbiAgLy8gVGhlc2UgcHJvcGVydHkgYWNjZXNzZXMgY2FuIGJlIGlnbm9yZWQgYmVjYXVzZSBuZ0Rldk1vZGUgd2lsbCBiZSBzZXQgdG8gZmFsc2VcbiAgLy8gd2hlbiBvcHRpbWl6aW5nIGNvZGUgYW5kIHRoZSB3aG9sZSBpZiBzdGF0ZW1lbnQgd2lsbCBiZSBkcm9wcGVkLlxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tdG9wbGV2ZWwtcHJvcGVydHktYWNjZXNzXG4gIE9iamVjdC5mcmVlemUoRU1QVFlfT0JKKTtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLXRvcGxldmVsLXByb3BlcnR5LWFjY2Vzc1xuICBPYmplY3QuZnJlZXplKEVNUFRZX0FSUkFZKTtcbn1cbiJdfQ==