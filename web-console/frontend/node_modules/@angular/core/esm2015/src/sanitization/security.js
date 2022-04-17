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
/** @enum {number} */
const SecurityContext = {
    NONE: 0,
    HTML: 1,
    STYLE: 2,
    SCRIPT: 3,
    URL: 4,
    RESOURCE_URL: 5,
};
export { SecurityContext };
SecurityContext[SecurityContext.NONE] = 'NONE';
SecurityContext[SecurityContext.HTML] = 'HTML';
SecurityContext[SecurityContext.STYLE] = 'STYLE';
SecurityContext[SecurityContext.SCRIPT] = 'SCRIPT';
SecurityContext[SecurityContext.URL] = 'URL';
SecurityContext[SecurityContext.RESOURCE_URL] = 'RESOURCE_URL';
/**
 * Sanitizer is used by the views to sanitize potentially dangerous values.
 *
 * \@publicApi
 * @abstract
 */
export class Sanitizer {
}
if (false) {
    /**
     * @abstract
     * @param {?} context
     * @param {?} value
     * @return {?}
     */
    Sanitizer.prototype.sanitize = function (context, value) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VjdXJpdHkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9zYW5pdGl6YXRpb24vc2VjdXJpdHkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7OztJQWtCRSxPQUFRO0lBQ1IsT0FBUTtJQUNSLFFBQVM7SUFDVCxTQUFVO0lBQ1YsTUFBTztJQUNQLGVBQWdCOzs7Ozs7Ozs7Ozs7Ozs7QUFRbEIsTUFBTSxPQUFnQixTQUFTO0NBRTlCOzs7Ozs7OztJQURDLDZEQUFnRiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBBIFNlY3VyaXR5Q29udGV4dCBtYXJrcyBhIGxvY2F0aW9uIHRoYXQgaGFzIGRhbmdlcm91cyBzZWN1cml0eSBpbXBsaWNhdGlvbnMsIGUuZy4gYSBET00gcHJvcGVydHlcbiAqIGxpa2UgYGlubmVySFRNTGAgdGhhdCBjb3VsZCBjYXVzZSBDcm9zcyBTaXRlIFNjcmlwdGluZyAoWFNTKSBzZWN1cml0eSBidWdzIHdoZW4gaW1wcm9wZXJseVxuICogaGFuZGxlZC5cbiAqXG4gKiBTZWUgRG9tU2FuaXRpemVyIGZvciBtb3JlIGRldGFpbHMgb24gc2VjdXJpdHkgaW4gQW5ndWxhciBhcHBsaWNhdGlvbnMuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZW51bSBTZWN1cml0eUNvbnRleHQge1xuICBOT05FID0gMCxcbiAgSFRNTCA9IDEsXG4gIFNUWUxFID0gMixcbiAgU0NSSVBUID0gMyxcbiAgVVJMID0gNCxcbiAgUkVTT1VSQ0VfVVJMID0gNSxcbn1cblxuLyoqXG4gKiBTYW5pdGl6ZXIgaXMgdXNlZCBieSB0aGUgdmlld3MgdG8gc2FuaXRpemUgcG90ZW50aWFsbHkgZGFuZ2Vyb3VzIHZhbHVlcy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBTYW5pdGl6ZXIge1xuICBhYnN0cmFjdCBzYW5pdGl6ZShjb250ZXh0OiBTZWN1cml0eUNvbnRleHQsIHZhbHVlOiB7fXxzdHJpbmd8bnVsbCk6IHN0cmluZ3xudWxsO1xufVxuIl19