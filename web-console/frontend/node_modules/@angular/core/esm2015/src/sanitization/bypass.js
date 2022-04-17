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
/** @type {?} */
const BRAND = '__SANITIZER_TRUSTED_BRAND__';
/** @enum {string} */
const BypassType = {
    Url: 'Url',
    Html: 'Html',
    ResourceUrl: 'ResourceUrl',
    Script: 'Script',
    Style: 'Style',
};
export { BypassType };
/**
 * A branded trusted string used with sanitization.
 *
 * See: {\@link TrustedHtmlString}, {\@link TrustedResourceUrlString}, {\@link TrustedScriptString},
 * {\@link TrustedStyleString}, {\@link TrustedUrlString}
 * @record
 */
export function TrustedString() { }
if (false) {
    /* Skipping unnamed member:
    [BRAND]: BypassType;*/
}
/**
 * A branded trusted string used with sanitization of `html` strings.
 *
 * See: {\@link bypassSanitizationTrustHtml} and {\@link htmlSanitizer}.
 * @record
 */
export function TrustedHtmlString() { }
if (false) {
    /* Skipping unnamed member:
    [BRAND]: BypassType.Html;*/
}
/**
 * A branded trusted string used with sanitization of `style` strings.
 *
 * See: {\@link bypassSanitizationTrustStyle} and {\@link styleSanitizer}.
 * @record
 */
export function TrustedStyleString() { }
if (false) {
    /* Skipping unnamed member:
    [BRAND]: BypassType.Style;*/
}
/**
 * A branded trusted string used with sanitization of `url` strings.
 *
 * See: {\@link bypassSanitizationTrustScript} and {\@link scriptSanitizer}.
 * @record
 */
export function TrustedScriptString() { }
if (false) {
    /* Skipping unnamed member:
    [BRAND]: BypassType.Script;*/
}
/**
 * A branded trusted string used with sanitization of `url` strings.
 *
 * See: {\@link bypassSanitizationTrustUrl} and {\@link urlSanitizer}.
 * @record
 */
export function TrustedUrlString() { }
if (false) {
    /* Skipping unnamed member:
    [BRAND]: BypassType.Url;*/
}
/**
 * A branded trusted string used with sanitization of `resourceUrl` strings.
 *
 * See: {\@link bypassSanitizationTrustResourceUrl} and {\@link resourceUrlSanitizer}.
 * @record
 */
export function TrustedResourceUrlString() { }
if (false) {
    /* Skipping unnamed member:
    [BRAND]: BypassType.ResourceUrl;*/
}
/**
 * @param {?} value
 * @param {?} type
 * @return {?}
 */
export function allowSanitizationBypass(value, type) {
    return (value instanceof String && ((/** @type {?} */ (value)))[BRAND] === type);
}
/**
 * Mark `html` string as trusted.
 *
 * This function wraps the trusted string in `String` and brands it in a way which makes it
 * recognizable to {\@link htmlSanitizer} to be trusted implicitly.
 *
 * @param {?} trustedHtml `html` string which needs to be implicitly trusted.
 * @return {?} a `html` `String` which has been branded to be implicitly trusted.
 */
export function bypassSanitizationTrustHtml(trustedHtml) {
    return bypassSanitizationTrustString(trustedHtml, "Html" /* Html */);
}
/**
 * Mark `style` string as trusted.
 *
 * This function wraps the trusted string in `String` and brands it in a way which makes it
 * recognizable to {\@link styleSanitizer} to be trusted implicitly.
 *
 * @param {?} trustedStyle `style` string which needs to be implicitly trusted.
 * @return {?} a `style` `String` which has been branded to be implicitly trusted.
 */
export function bypassSanitizationTrustStyle(trustedStyle) {
    return bypassSanitizationTrustString(trustedStyle, "Style" /* Style */);
}
/**
 * Mark `script` string as trusted.
 *
 * This function wraps the trusted string in `String` and brands it in a way which makes it
 * recognizable to {\@link scriptSanitizer} to be trusted implicitly.
 *
 * @param {?} trustedScript `script` string which needs to be implicitly trusted.
 * @return {?} a `script` `String` which has been branded to be implicitly trusted.
 */
export function bypassSanitizationTrustScript(trustedScript) {
    return bypassSanitizationTrustString(trustedScript, "Script" /* Script */);
}
/**
 * Mark `url` string as trusted.
 *
 * This function wraps the trusted string in `String` and brands it in a way which makes it
 * recognizable to {\@link urlSanitizer} to be trusted implicitly.
 *
 * @param {?} trustedUrl `url` string which needs to be implicitly trusted.
 * @return {?} a `url` `String` which has been branded to be implicitly trusted.
 */
export function bypassSanitizationTrustUrl(trustedUrl) {
    return bypassSanitizationTrustString(trustedUrl, "Url" /* Url */);
}
/**
 * Mark `url` string as trusted.
 *
 * This function wraps the trusted string in `String` and brands it in a way which makes it
 * recognizable to {\@link resourceUrlSanitizer} to be trusted implicitly.
 *
 * @param {?} trustedResourceUrl `url` string which needs to be implicitly trusted.
 * @return {?} a `url` `String` which has been branded to be implicitly trusted.
 */
export function bypassSanitizationTrustResourceUrl(trustedResourceUrl) {
    return bypassSanitizationTrustString(trustedResourceUrl, "ResourceUrl" /* ResourceUrl */);
}
/**
 * @param {?} trustedString
 * @param {?} mode
 * @return {?}
 */
function bypassSanitizationTrustString(trustedString, mode) {
    // tslint:disable-next-line
    /** @type {?} */
    const trusted = (/** @type {?} */ (new String(trustedString)));
    trusted[BRAND] = mode;
    return trusted;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYnlwYXNzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvc2FuaXRpemF0aW9uL2J5cGFzcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7TUFRTSxLQUFLLEdBQUcsNkJBQTZCOzs7SUFHekMsS0FBTSxLQUFLO0lBQ1gsTUFBTyxNQUFNO0lBQ2IsYUFBYyxhQUFhO0lBQzNCLFFBQVMsUUFBUTtJQUNqQixPQUFRLE9BQU87Ozs7Ozs7Ozs7QUFTakIsbUNBQXNFOzs7Ozs7Ozs7OztBQU90RSx1Q0FBc0Y7Ozs7Ozs7Ozs7O0FBT3RGLHdDQUF3Rjs7Ozs7Ozs7Ozs7QUFPeEYseUNBQTBGOzs7Ozs7Ozs7OztBQU8xRixzQ0FBb0Y7Ozs7Ozs7Ozs7O0FBT3BGLDhDQUFvRzs7Ozs7Ozs7OztBQUVwRyxNQUFNLFVBQVUsdUJBQXVCLENBQUMsS0FBVSxFQUFFLElBQWdCO0lBQ2xFLE9BQU8sQ0FBQyxLQUFLLFlBQVksTUFBTSxJQUFJLENBQUMsbUJBQUEsS0FBSyxFQUFzQixDQUFDLENBQUMsS0FBSyxDQUFDLEtBQUssSUFBSSxDQUFDLENBQUM7QUFDcEYsQ0FBQzs7Ozs7Ozs7OztBQVdELE1BQU0sVUFBVSwyQkFBMkIsQ0FBQyxXQUFtQjtJQUM3RCxPQUFPLDZCQUE2QixDQUFDLFdBQVcsb0JBQWtCLENBQUM7QUFDckUsQ0FBQzs7Ozs7Ozs7OztBQVVELE1BQU0sVUFBVSw0QkFBNEIsQ0FBQyxZQUFvQjtJQUMvRCxPQUFPLDZCQUE2QixDQUFDLFlBQVksc0JBQW1CLENBQUM7QUFDdkUsQ0FBQzs7Ozs7Ozs7OztBQVVELE1BQU0sVUFBVSw2QkFBNkIsQ0FBQyxhQUFxQjtJQUNqRSxPQUFPLDZCQUE2QixDQUFDLGFBQWEsd0JBQW9CLENBQUM7QUFDekUsQ0FBQzs7Ozs7Ozs7OztBQVVELE1BQU0sVUFBVSwwQkFBMEIsQ0FBQyxVQUFrQjtJQUMzRCxPQUFPLDZCQUE2QixDQUFDLFVBQVUsa0JBQWlCLENBQUM7QUFDbkUsQ0FBQzs7Ozs7Ozs7OztBQVVELE1BQU0sVUFBVSxrQ0FBa0MsQ0FBQyxrQkFBMEI7SUFFM0UsT0FBTyw2QkFBNkIsQ0FBQyxrQkFBa0Isa0NBQXlCLENBQUM7QUFDbkYsQ0FBQzs7Ozs7O0FBYUQsU0FBUyw2QkFBNkIsQ0FBQyxhQUFxQixFQUFFLElBQWdCOzs7VUFFdEUsT0FBTyxHQUFHLG1CQUFBLElBQUksTUFBTSxDQUFDLGFBQWEsQ0FBQyxFQUFpQjtJQUMxRCxPQUFPLENBQUMsS0FBSyxDQUFDLEdBQUcsSUFBSSxDQUFDO0lBQ3RCLE9BQU8sT0FBTyxDQUFDO0FBQ2pCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmNvbnN0IEJSQU5EID0gJ19fU0FOSVRJWkVSX1RSVVNURURfQlJBTkRfXyc7XG5cbmV4cG9ydCBjb25zdCBlbnVtIEJ5cGFzc1R5cGUge1xuICBVcmwgPSAnVXJsJyxcbiAgSHRtbCA9ICdIdG1sJyxcbiAgUmVzb3VyY2VVcmwgPSAnUmVzb3VyY2VVcmwnLFxuICBTY3JpcHQgPSAnU2NyaXB0JyxcbiAgU3R5bGUgPSAnU3R5bGUnLFxufVxuXG4vKipcbiAqIEEgYnJhbmRlZCB0cnVzdGVkIHN0cmluZyB1c2VkIHdpdGggc2FuaXRpemF0aW9uLlxuICpcbiAqIFNlZToge0BsaW5rIFRydXN0ZWRIdG1sU3RyaW5nfSwge0BsaW5rIFRydXN0ZWRSZXNvdXJjZVVybFN0cmluZ30sIHtAbGluayBUcnVzdGVkU2NyaXB0U3RyaW5nfSxcbiAqIHtAbGluayBUcnVzdGVkU3R5bGVTdHJpbmd9LCB7QGxpbmsgVHJ1c3RlZFVybFN0cmluZ31cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBUcnVzdGVkU3RyaW5nIGV4dGVuZHMgU3RyaW5nIHsgW0JSQU5EXTogQnlwYXNzVHlwZTsgfVxuXG4vKipcbiAqIEEgYnJhbmRlZCB0cnVzdGVkIHN0cmluZyB1c2VkIHdpdGggc2FuaXRpemF0aW9uIG9mIGBodG1sYCBzdHJpbmdzLlxuICpcbiAqIFNlZToge0BsaW5rIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0SHRtbH0gYW5kIHtAbGluayBodG1sU2FuaXRpemVyfS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBUcnVzdGVkSHRtbFN0cmluZyBleHRlbmRzIFRydXN0ZWRTdHJpbmcgeyBbQlJBTkRdOiBCeXBhc3NUeXBlLkh0bWw7IH1cblxuLyoqXG4gKiBBIGJyYW5kZWQgdHJ1c3RlZCBzdHJpbmcgdXNlZCB3aXRoIHNhbml0aXphdGlvbiBvZiBgc3R5bGVgIHN0cmluZ3MuXG4gKlxuICogU2VlOiB7QGxpbmsgYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RTdHlsZX0gYW5kIHtAbGluayBzdHlsZVNhbml0aXplcn0uXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgVHJ1c3RlZFN0eWxlU3RyaW5nIGV4dGVuZHMgVHJ1c3RlZFN0cmluZyB7IFtCUkFORF06IEJ5cGFzc1R5cGUuU3R5bGU7IH1cblxuLyoqXG4gKiBBIGJyYW5kZWQgdHJ1c3RlZCBzdHJpbmcgdXNlZCB3aXRoIHNhbml0aXphdGlvbiBvZiBgdXJsYCBzdHJpbmdzLlxuICpcbiAqIFNlZToge0BsaW5rIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0U2NyaXB0fSBhbmQge0BsaW5rIHNjcmlwdFNhbml0aXplcn0uXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgVHJ1c3RlZFNjcmlwdFN0cmluZyBleHRlbmRzIFRydXN0ZWRTdHJpbmcgeyBbQlJBTkRdOiBCeXBhc3NUeXBlLlNjcmlwdDsgfVxuXG4vKipcbiAqIEEgYnJhbmRlZCB0cnVzdGVkIHN0cmluZyB1c2VkIHdpdGggc2FuaXRpemF0aW9uIG9mIGB1cmxgIHN0cmluZ3MuXG4gKlxuICogU2VlOiB7QGxpbmsgYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RVcmx9IGFuZCB7QGxpbmsgdXJsU2FuaXRpemVyfS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBUcnVzdGVkVXJsU3RyaW5nIGV4dGVuZHMgVHJ1c3RlZFN0cmluZyB7IFtCUkFORF06IEJ5cGFzc1R5cGUuVXJsOyB9XG5cbi8qKlxuICogQSBicmFuZGVkIHRydXN0ZWQgc3RyaW5nIHVzZWQgd2l0aCBzYW5pdGl6YXRpb24gb2YgYHJlc291cmNlVXJsYCBzdHJpbmdzLlxuICpcbiAqIFNlZToge0BsaW5rIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0UmVzb3VyY2VVcmx9IGFuZCB7QGxpbmsgcmVzb3VyY2VVcmxTYW5pdGl6ZXJ9LlxuICovXG5leHBvcnQgaW50ZXJmYWNlIFRydXN0ZWRSZXNvdXJjZVVybFN0cmluZyBleHRlbmRzIFRydXN0ZWRTdHJpbmcgeyBbQlJBTkRdOiBCeXBhc3NUeXBlLlJlc291cmNlVXJsOyB9XG5cbmV4cG9ydCBmdW5jdGlvbiBhbGxvd1Nhbml0aXphdGlvbkJ5cGFzcyh2YWx1ZTogYW55LCB0eXBlOiBCeXBhc3NUeXBlKTogYm9vbGVhbiB7XG4gIHJldHVybiAodmFsdWUgaW5zdGFuY2VvZiBTdHJpbmcgJiYgKHZhbHVlIGFzIFRydXN0ZWRTdHlsZVN0cmluZylbQlJBTkRdID09PSB0eXBlKTtcbn1cblxuLyoqXG4gKiBNYXJrIGBodG1sYCBzdHJpbmcgYXMgdHJ1c3RlZC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIHdyYXBzIHRoZSB0cnVzdGVkIHN0cmluZyBpbiBgU3RyaW5nYCBhbmQgYnJhbmRzIGl0IGluIGEgd2F5IHdoaWNoIG1ha2VzIGl0XG4gKiByZWNvZ25pemFibGUgdG8ge0BsaW5rIGh0bWxTYW5pdGl6ZXJ9IHRvIGJlIHRydXN0ZWQgaW1wbGljaXRseS5cbiAqXG4gKiBAcGFyYW0gdHJ1c3RlZEh0bWwgYGh0bWxgIHN0cmluZyB3aGljaCBuZWVkcyB0byBiZSBpbXBsaWNpdGx5IHRydXN0ZWQuXG4gKiBAcmV0dXJucyBhIGBodG1sYCBgU3RyaW5nYCB3aGljaCBoYXMgYmVlbiBicmFuZGVkIHRvIGJlIGltcGxpY2l0bHkgdHJ1c3RlZC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0SHRtbCh0cnVzdGVkSHRtbDogc3RyaW5nKTogVHJ1c3RlZEh0bWxTdHJpbmcge1xuICByZXR1cm4gYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RTdHJpbmcodHJ1c3RlZEh0bWwsIEJ5cGFzc1R5cGUuSHRtbCk7XG59XG4vKipcbiAqIE1hcmsgYHN0eWxlYCBzdHJpbmcgYXMgdHJ1c3RlZC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIHdyYXBzIHRoZSB0cnVzdGVkIHN0cmluZyBpbiBgU3RyaW5nYCBhbmQgYnJhbmRzIGl0IGluIGEgd2F5IHdoaWNoIG1ha2VzIGl0XG4gKiByZWNvZ25pemFibGUgdG8ge0BsaW5rIHN0eWxlU2FuaXRpemVyfSB0byBiZSB0cnVzdGVkIGltcGxpY2l0bHkuXG4gKlxuICogQHBhcmFtIHRydXN0ZWRTdHlsZSBgc3R5bGVgIHN0cmluZyB3aGljaCBuZWVkcyB0byBiZSBpbXBsaWNpdGx5IHRydXN0ZWQuXG4gKiBAcmV0dXJucyBhIGBzdHlsZWAgYFN0cmluZ2Agd2hpY2ggaGFzIGJlZW4gYnJhbmRlZCB0byBiZSBpbXBsaWNpdGx5IHRydXN0ZWQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBieXBhc3NTYW5pdGl6YXRpb25UcnVzdFN0eWxlKHRydXN0ZWRTdHlsZTogc3RyaW5nKTogVHJ1c3RlZFN0eWxlU3RyaW5nIHtcbiAgcmV0dXJuIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0U3RyaW5nKHRydXN0ZWRTdHlsZSwgQnlwYXNzVHlwZS5TdHlsZSk7XG59XG4vKipcbiAqIE1hcmsgYHNjcmlwdGAgc3RyaW5nIGFzIHRydXN0ZWQuXG4gKlxuICogVGhpcyBmdW5jdGlvbiB3cmFwcyB0aGUgdHJ1c3RlZCBzdHJpbmcgaW4gYFN0cmluZ2AgYW5kIGJyYW5kcyBpdCBpbiBhIHdheSB3aGljaCBtYWtlcyBpdFxuICogcmVjb2duaXphYmxlIHRvIHtAbGluayBzY3JpcHRTYW5pdGl6ZXJ9IHRvIGJlIHRydXN0ZWQgaW1wbGljaXRseS5cbiAqXG4gKiBAcGFyYW0gdHJ1c3RlZFNjcmlwdCBgc2NyaXB0YCBzdHJpbmcgd2hpY2ggbmVlZHMgdG8gYmUgaW1wbGljaXRseSB0cnVzdGVkLlxuICogQHJldHVybnMgYSBgc2NyaXB0YCBgU3RyaW5nYCB3aGljaCBoYXMgYmVlbiBicmFuZGVkIHRvIGJlIGltcGxpY2l0bHkgdHJ1c3RlZC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0U2NyaXB0KHRydXN0ZWRTY3JpcHQ6IHN0cmluZyk6IFRydXN0ZWRTY3JpcHRTdHJpbmcge1xuICByZXR1cm4gYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RTdHJpbmcodHJ1c3RlZFNjcmlwdCwgQnlwYXNzVHlwZS5TY3JpcHQpO1xufVxuLyoqXG4gKiBNYXJrIGB1cmxgIHN0cmluZyBhcyB0cnVzdGVkLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gd3JhcHMgdGhlIHRydXN0ZWQgc3RyaW5nIGluIGBTdHJpbmdgIGFuZCBicmFuZHMgaXQgaW4gYSB3YXkgd2hpY2ggbWFrZXMgaXRcbiAqIHJlY29nbml6YWJsZSB0byB7QGxpbmsgdXJsU2FuaXRpemVyfSB0byBiZSB0cnVzdGVkIGltcGxpY2l0bHkuXG4gKlxuICogQHBhcmFtIHRydXN0ZWRVcmwgYHVybGAgc3RyaW5nIHdoaWNoIG5lZWRzIHRvIGJlIGltcGxpY2l0bHkgdHJ1c3RlZC5cbiAqIEByZXR1cm5zIGEgYHVybGAgYFN0cmluZ2Agd2hpY2ggaGFzIGJlZW4gYnJhbmRlZCB0byBiZSBpbXBsaWNpdGx5IHRydXN0ZWQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBieXBhc3NTYW5pdGl6YXRpb25UcnVzdFVybCh0cnVzdGVkVXJsOiBzdHJpbmcpOiBUcnVzdGVkVXJsU3RyaW5nIHtcbiAgcmV0dXJuIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0U3RyaW5nKHRydXN0ZWRVcmwsIEJ5cGFzc1R5cGUuVXJsKTtcbn1cbi8qKlxuICogTWFyayBgdXJsYCBzdHJpbmcgYXMgdHJ1c3RlZC5cbiAqXG4gKiBUaGlzIGZ1bmN0aW9uIHdyYXBzIHRoZSB0cnVzdGVkIHN0cmluZyBpbiBgU3RyaW5nYCBhbmQgYnJhbmRzIGl0IGluIGEgd2F5IHdoaWNoIG1ha2VzIGl0XG4gKiByZWNvZ25pemFibGUgdG8ge0BsaW5rIHJlc291cmNlVXJsU2FuaXRpemVyfSB0byBiZSB0cnVzdGVkIGltcGxpY2l0bHkuXG4gKlxuICogQHBhcmFtIHRydXN0ZWRSZXNvdXJjZVVybCBgdXJsYCBzdHJpbmcgd2hpY2ggbmVlZHMgdG8gYmUgaW1wbGljaXRseSB0cnVzdGVkLlxuICogQHJldHVybnMgYSBgdXJsYCBgU3RyaW5nYCB3aGljaCBoYXMgYmVlbiBicmFuZGVkIHRvIGJlIGltcGxpY2l0bHkgdHJ1c3RlZC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0UmVzb3VyY2VVcmwodHJ1c3RlZFJlc291cmNlVXJsOiBzdHJpbmcpOlxuICAgIFRydXN0ZWRSZXNvdXJjZVVybFN0cmluZyB7XG4gIHJldHVybiBieXBhc3NTYW5pdGl6YXRpb25UcnVzdFN0cmluZyh0cnVzdGVkUmVzb3VyY2VVcmwsIEJ5cGFzc1R5cGUuUmVzb3VyY2VVcmwpO1xufVxuXG5cbmZ1bmN0aW9uIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0U3RyaW5nKFxuICAgIHRydXN0ZWRTdHJpbmc6IHN0cmluZywgbW9kZTogQnlwYXNzVHlwZS5IdG1sKTogVHJ1c3RlZEh0bWxTdHJpbmc7XG5mdW5jdGlvbiBieXBhc3NTYW5pdGl6YXRpb25UcnVzdFN0cmluZyhcbiAgICB0cnVzdGVkU3RyaW5nOiBzdHJpbmcsIG1vZGU6IEJ5cGFzc1R5cGUuU3R5bGUpOiBUcnVzdGVkU3R5bGVTdHJpbmc7XG5mdW5jdGlvbiBieXBhc3NTYW5pdGl6YXRpb25UcnVzdFN0cmluZyhcbiAgICB0cnVzdGVkU3RyaW5nOiBzdHJpbmcsIG1vZGU6IEJ5cGFzc1R5cGUuU2NyaXB0KTogVHJ1c3RlZFNjcmlwdFN0cmluZztcbmZ1bmN0aW9uIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0U3RyaW5nKFxuICAgIHRydXN0ZWRTdHJpbmc6IHN0cmluZywgbW9kZTogQnlwYXNzVHlwZS5VcmwpOiBUcnVzdGVkVXJsU3RyaW5nO1xuZnVuY3Rpb24gYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RTdHJpbmcoXG4gICAgdHJ1c3RlZFN0cmluZzogc3RyaW5nLCBtb2RlOiBCeXBhc3NUeXBlLlJlc291cmNlVXJsKTogVHJ1c3RlZFJlc291cmNlVXJsU3RyaW5nO1xuZnVuY3Rpb24gYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RTdHJpbmcodHJ1c3RlZFN0cmluZzogc3RyaW5nLCBtb2RlOiBCeXBhc3NUeXBlKTogVHJ1c3RlZFN0cmluZyB7XG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZVxuICBjb25zdCB0cnVzdGVkID0gbmV3IFN0cmluZyh0cnVzdGVkU3RyaW5nKSBhcyBUcnVzdGVkU3RyaW5nO1xuICB0cnVzdGVkW0JSQU5EXSA9IG1vZGU7XG4gIHJldHVybiB0cnVzdGVkO1xufVxuIl19