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
import { DOCUMENT } from '@angular/common';
import { Inject, Injectable, SecurityContext, ɵ_sanitizeHtml as _sanitizeHtml, ɵ_sanitizeStyle as _sanitizeStyle, ɵ_sanitizeUrl as _sanitizeUrl } from '@angular/core';
export { SecurityContext };
/**
 * Marker interface for a value that's safe to use in a particular context.
 *
 * \@publicApi
 * @record
 */
export function SafeValue() { }
/**
 * Marker interface for a value that's safe to use as HTML.
 *
 * \@publicApi
 * @record
 */
export function SafeHtml() { }
/**
 * Marker interface for a value that's safe to use as style (CSS).
 *
 * \@publicApi
 * @record
 */
export function SafeStyle() { }
/**
 * Marker interface for a value that's safe to use as JavaScript.
 *
 * \@publicApi
 * @record
 */
export function SafeScript() { }
/**
 * Marker interface for a value that's safe to use as a URL linking to a document.
 *
 * \@publicApi
 * @record
 */
export function SafeUrl() { }
/**
 * Marker interface for a value that's safe to use as a URL to load executable code from.
 *
 * \@publicApi
 * @record
 */
export function SafeResourceUrl() { }
/**
 * DomSanitizer helps preventing Cross Site Scripting Security bugs (XSS) by sanitizing
 * values to be safe to use in the different DOM contexts.
 *
 * For example, when binding a URL in an `<a [href]="someValue">` hyperlink, `someValue` will be
 * sanitized so that an attacker cannot inject e.g. a `javascript:` URL that would execute code on
 * the website.
 *
 * In specific situations, it might be necessary to disable sanitization, for example if the
 * application genuinely needs to produce a `javascript:` style link with a dynamic value in it.
 * Users can bypass security by constructing a value with one of the `bypassSecurityTrust...`
 * methods, and then binding to that value from the template.
 *
 * These situations should be very rare, and extraordinary care must be taken to avoid creating a
 * Cross Site Scripting (XSS) security bug!
 *
 * When using `bypassSecurityTrust...`, make sure to call the method as early as possible and as
 * close as possible to the source of the value, to make it easy to verify no security bug is
 * created by its use.
 *
 * It is not required (and not recommended) to bypass security if the value is safe, e.g. a URL that
 * does not start with a suspicious protocol, or an HTML snippet that does not contain dangerous
 * code. The sanitizer leaves safe values intact.
 *
 * \@security Calling any of the `bypassSecurityTrust...` APIs disables Angular's built-in
 * sanitization for the value passed in. Carefully check and audit all values and code paths going
 * into this call. Make sure any user data is appropriately escaped for this security context.
 * For more detail, see the [Security Guide](http://g.co/ng/security).
 *
 * \@publicApi
 * @abstract
 */
export class DomSanitizer {
}
if (false) {
    /**
     * Sanitizes a value for use in the given SecurityContext.
     *
     * If value is trusted for the context, this method will unwrap the contained safe value and use
     * it directly. Otherwise, value will be sanitized to be safe in the given context, for example
     * by replacing URLs that have an unsafe protocol part (such as `javascript:`). The implementation
     * is responsible to make sure that the value can definitely be safely used in the given context.
     * @abstract
     * @param {?} context
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.sanitize = function (context, value) { };
    /**
     * Bypass security and trust the given value to be safe HTML. Only use this when the bound HTML
     * is unsafe (e.g. contains `<script>` tags) and the code should be executed. The sanitizer will
     * leave safe HTML intact, so in most situations this method should not be used.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustHtml = function (value) { };
    /**
     * Bypass security and trust the given value to be safe style value (CSS).
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustStyle = function (value) { };
    /**
     * Bypass security and trust the given value to be safe JavaScript.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustScript = function (value) { };
    /**
     * Bypass security and trust the given value to be a safe style URL, i.e. a value that can be used
     * in hyperlinks or `<img src>`.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustUrl = function (value) { };
    /**
     * Bypass security and trust the given value to be a safe resource URL, i.e. a location that may
     * be used to load executable code from, like `<script src>`, or `<iframe src>`.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustResourceUrl = function (value) { };
}
export class DomSanitizerImpl extends DomSanitizer {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        super();
        this._doc = _doc;
    }
    /**
     * @param {?} ctx
     * @param {?} value
     * @return {?}
     */
    sanitize(ctx, value) {
        if (value == null)
            return null;
        switch (ctx) {
            case SecurityContext.NONE:
                return (/** @type {?} */ (value));
            case SecurityContext.HTML:
                if (value instanceof SafeHtmlImpl)
                    return value.changingThisBreaksApplicationSecurity;
                this.checkNotSafeValue(value, 'HTML');
                return _sanitizeHtml(this._doc, String(value));
            case SecurityContext.STYLE:
                if (value instanceof SafeStyleImpl)
                    return value.changingThisBreaksApplicationSecurity;
                this.checkNotSafeValue(value, 'Style');
                return _sanitizeStyle((/** @type {?} */ (value)));
            case SecurityContext.SCRIPT:
                if (value instanceof SafeScriptImpl)
                    return value.changingThisBreaksApplicationSecurity;
                this.checkNotSafeValue(value, 'Script');
                throw new Error('unsafe value used in a script context');
            case SecurityContext.URL:
                if (value instanceof SafeResourceUrlImpl || value instanceof SafeUrlImpl) {
                    // Allow resource URLs in URL contexts, they are strictly more trusted.
                    return value.changingThisBreaksApplicationSecurity;
                }
                this.checkNotSafeValue(value, 'URL');
                return _sanitizeUrl(String(value));
            case SecurityContext.RESOURCE_URL:
                if (value instanceof SafeResourceUrlImpl) {
                    return value.changingThisBreaksApplicationSecurity;
                }
                this.checkNotSafeValue(value, 'ResourceURL');
                throw new Error('unsafe value used in a resource URL context (see http://g.co/ng/security#xss)');
            default:
                throw new Error(`Unexpected SecurityContext ${ctx} (see http://g.co/ng/security#xss)`);
        }
    }
    /**
     * @private
     * @param {?} value
     * @param {?} expectedType
     * @return {?}
     */
    checkNotSafeValue(value, expectedType) {
        if (value instanceof SafeValueImpl) {
            throw new Error(`Required a safe ${expectedType}, got a ${value.getTypeName()} ` +
                `(see http://g.co/ng/security#xss)`);
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustHtml(value) { return new SafeHtmlImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustStyle(value) { return new SafeStyleImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustScript(value) { return new SafeScriptImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustUrl(value) { return new SafeUrlImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustResourceUrl(value) {
        return new SafeResourceUrlImpl(value);
    }
}
DomSanitizerImpl.decorators = [
    { type: Injectable }
];
/** @nocollapse */
DomSanitizerImpl.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DomSanitizerImpl.prototype._doc;
}
/**
 * @abstract
 */
class SafeValueImpl {
    /**
     * @param {?} changingThisBreaksApplicationSecurity
     */
    constructor(changingThisBreaksApplicationSecurity) {
        this.changingThisBreaksApplicationSecurity = changingThisBreaksApplicationSecurity;
        // empty
    }
    /**
     * @return {?}
     */
    toString() {
        return `SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}` +
            ` (see http://g.co/ng/security#xss)`;
    }
}
if (false) {
    /** @type {?} */
    SafeValueImpl.prototype.changingThisBreaksApplicationSecurity;
    /**
     * @abstract
     * @return {?}
     */
    SafeValueImpl.prototype.getTypeName = function () { };
}
class SafeHtmlImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'HTML'; }
}
class SafeStyleImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'Style'; }
}
class SafeScriptImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'Script'; }
}
class SafeUrlImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'URL'; }
}
class SafeResourceUrlImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'ResourceURL'; }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG9tX3Nhbml0aXphdGlvbl9zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci9zcmMvc2VjdXJpdHkvZG9tX3Nhbml0aXphdGlvbl9zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3pDLE9BQU8sRUFBQyxNQUFNLEVBQUUsVUFBVSxFQUFhLGVBQWUsRUFBRSxjQUFjLElBQUksYUFBYSxFQUFFLGVBQWUsSUFBSSxjQUFjLEVBQUUsYUFBYSxJQUFJLFlBQVksRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUdoTCxPQUFPLEVBQUMsZUFBZSxFQUFDLENBQUM7Ozs7Ozs7QUFTekIsK0JBQTZCOzs7Ozs7O0FBTzdCLDhCQUE4Qzs7Ozs7OztBQU85QywrQkFBK0M7Ozs7Ozs7QUFPL0MsZ0NBQWdEOzs7Ozs7O0FBT2hELDZCQUE2Qzs7Ozs7OztBQU83QyxxQ0FBcUQ7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQWlDckQsTUFBTSxPQUFnQixZQUFZO0NBc0RqQzs7Ozs7Ozs7Ozs7Ozs7SUE3Q0MsZ0VBQXVGOzs7Ozs7Ozs7Ozs7SUFVdkYsc0VBQTBEOzs7Ozs7Ozs7O0lBUTFELHVFQUE0RDs7Ozs7Ozs7OztJQVE1RCx3RUFBOEQ7Ozs7Ozs7Ozs7O0lBUzlELHFFQUF3RDs7Ozs7Ozs7Ozs7SUFTeEQsNkVBQXdFOztBQUsxRSxNQUFNLE9BQU8sZ0JBQWlCLFNBQVEsWUFBWTs7OztJQUNoRCxZQUFzQyxJQUFTO1FBQUksS0FBSyxFQUFFLENBQUM7UUFBckIsU0FBSSxHQUFKLElBQUksQ0FBSztJQUFhLENBQUM7Ozs7OztJQUU3RCxRQUFRLENBQUMsR0FBb0IsRUFBRSxLQUE0QjtRQUN6RCxJQUFJLEtBQUssSUFBSSxJQUFJO1lBQUUsT0FBTyxJQUFJ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Z0NBQW1DOzs7OztBQXVEakQsTUFBZSxhQUFhOzs7O0lBQzFCLFlBQW1CLHFDQUE2QztRQUE3QywwQ0FBcUMsR0FBckMscUNBQXFDLENBQVE7UUFDOUQsUUFBUTtJQUNWLENBQUM7Ozs7SUFJRCxRQUFRO1FBQ04sT0FBTywwQ0FBMEMsSUFBSSxDQUFDLHFDQUFxQyxFQUFFO1lBQ3pGLG9DQUFvQyxDQUFDO0lBQzNDLENBQUM7Q0FDRjs7O0lBVmEsOERBQW9EOzs7OztJQUloRSxzREFBK0I7O0FBUWpDLE1BQU0sWUFBYSxTQUFRLGFBQWE7Ozs7SUFDdEMsV0FBVyxLQUFLLE9BQU8sTUFBTSxDQUFDLENBQUMsQ0FBQztDQUNqQztBQUNELE1BQU0sYUFBYyxTQUFRLGFBQWE7Ozs7SUFDdkMsV0FBVyxLQUFLLE9BQU8sT0FBTyxDQUFDLENBQUMsQ0FBQztDQUNsQztBQUNELE1BQU0sY0FBZSxTQUFRLGFBQWE7Ozs7SUFDeEMsV0FBVyxLQUFLLE9BQU8sUUFBUSxDQUFDLENBQUMsQ0FBQztDQUNuQztBQUNELE1BQU0sV0FBWSxTQUFRLGFBQWE7Ozs7SUFDckMsV0FBVyxLQUFLLE9BQU8sS0FBSyxDQUFDLENBQUMsQ0FBQztDQUNoQztBQUNELE1BQU0sbUJBQW9CLFNBQVEsYUFBYTs7OztJQUM3QyxXQUFXLEtBQUssT0FBTyxhQUFhLENBQUMsQ0FBQyxDQUFDO0NBQ3hDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0RPQ1VNRU5UfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHtJbmplY3QsIEluamVjdGFibGUsIFNhbml0aXplciwgU2VjdXJpdHlDb250ZXh0LCDJtV9zYW5pdGl6ZUh0bWwgYXMgX3Nhbml0aXplSHRtbCwgybVfc2FuaXRpemVTdHlsZSBhcyBfc2FuaXRpemVTdHlsZSwgybVfc2FuaXRpemVVcmwgYXMgX3Nhbml0aXplVXJsfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuXG5leHBvcnQge1NlY3VyaXR5Q29udGV4dH07XG5cblxuXG4vKipcbiAqIE1hcmtlciBpbnRlcmZhY2UgZm9yIGEgdmFsdWUgdGhhdCdzIHNhZmUgdG8gdXNlIGluIGEgcGFydGljdWxhciBjb250ZXh0LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBTYWZlVmFsdWUge31cblxuLyoqXG4gKiBNYXJrZXIgaW50ZXJmYWNlIGZvciBhIHZhbHVlIHRoYXQncyBzYWZlIHRvIHVzZSBhcyBIVE1MLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBTYWZlSHRtbCBleHRlbmRzIFNhZmVWYWx1ZSB7fVxuXG4vKipcbiAqIE1hcmtlciBpbnRlcmZhY2UgZm9yIGEgdmFsdWUgdGhhdCdzIHNhZmUgdG8gdXNlIGFzIHN0eWxlIChDU1MpLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBTYWZlU3R5bGUgZXh0ZW5kcyBTYWZlVmFsdWUge31cblxuLyoqXG4gKiBNYXJrZXIgaW50ZXJmYWNlIGZvciBhIHZhbHVlIHRoYXQncyBzYWZlIHRvIHVzZSBhcyBKYXZhU2NyaXB0LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBTYWZlU2NyaXB0IGV4dGVuZHMgU2FmZVZhbHVlIHt9XG5cbi8qKlxuICogTWFya2VyIGludGVyZmFjZSBmb3IgYSB2YWx1ZSB0aGF0J3Mgc2FmZSB0byB1c2UgYXMgYSBVUkwgbGlua2luZyB0byBhIGRvY3VtZW50LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBTYWZlVXJsIGV4dGVuZHMgU2FmZVZhbHVlIHt9XG5cbi8qKlxuICogTWFya2VyIGludGVyZmFjZSBmb3IgYSB2YWx1ZSB0aGF0J3Mgc2FmZSB0byB1c2UgYXMgYSBVUkwgdG8gbG9hZCBleGVjdXRhYmxlIGNvZGUgZnJvbS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgU2FmZVJlc291cmNlVXJsIGV4dGVuZHMgU2FmZVZhbHVlIHt9XG5cbi8qKlxuICogRG9tU2FuaXRpemVyIGhlbHBzIHByZXZlbnRpbmcgQ3Jvc3MgU2l0ZSBTY3JpcHRpbmcgU2VjdXJpdHkgYnVncyAoWFNTKSBieSBzYW5pdGl6aW5nXG4gKiB2YWx1ZXMgdG8gYmUgc2FmZSB0byB1c2UgaW4gdGhlIGRpZmZlcmVudCBET00gY29udGV4dHMuXG4gKlxuICogRm9yIGV4YW1wbGUsIHdoZW4gYmluZGluZyBhIFVSTCBpbiBhbiBgPGEgW2hyZWZdPVwic29tZVZhbHVlXCI+YCBoeXBlcmxpbmssIGBzb21lVmFsdWVgIHdpbGwgYmVcbiAqIHNhbml0aXplZCBzbyB0aGF0IGFuIGF0dGFja2VyIGNhbm5vdCBpbmplY3QgZS5nLiBhIGBqYXZhc2NyaXB0OmAgVVJMIHRoYXQgd291bGQgZXhlY3V0ZSBjb2RlIG9uXG4gKiB0aGUgd2Vic2l0ZS5cbiAqXG4gKiBJbiBzcGVjaWZpYyBzaXR1YXRpb25zLCBpdCBtaWdodCBiZSBuZWNlc3NhcnkgdG8gZGlzYWJsZSBzYW5pdGl6YXRpb24sIGZvciBleGFtcGxlIGlmIHRoZVxuICogYXBwbGljYXRpb24gZ2VudWluZWx5IG5lZWRzIHRvIHByb2R1Y2UgYSBgamF2YXNjcmlwdDpgIHN0eWxlIGxpbmsgd2l0aCBhIGR5bmFtaWMgdmFsdWUgaW4gaXQuXG4gKiBVc2VycyBjYW4gYnlwYXNzIHNlY3VyaXR5IGJ5IGNvbnN0cnVjdGluZyBhIHZhbHVlIHdpdGggb25lIG9mIHRoZSBgYnlwYXNzU2VjdXJpdHlUcnVzdC4uLmBcbiAqIG1ldGhvZHMsIGFuZCB0aGVuIGJpbmRpbmcgdG8gdGhhdCB2YWx1ZSBmcm9tIHRoZSB0ZW1wbGF0ZS5cbiAqXG4gKiBUaGVzZSBzaXR1YXRpb25zIHNob3VsZCBiZSB2ZXJ5IHJhcmUsIGFuZCBleHRyYW9yZGluYXJ5IGNhcmUgbXVzdCBiZSB0YWtlbiB0byBhdm9pZCBjcmVhdGluZyBhXG4gKiBDcm9zcyBTaXRlIFNjcmlwdGluZyAoWFNTKSBzZWN1cml0eSBidWchXG4gKlxuICogV2hlbiB1c2luZyBgYnlwYXNzU2VjdXJpdHlUcnVzdC4uLmAsIG1ha2Ugc3VyZSB0byBjYWxsIHRoZSBtZXRob2QgYXMgZWFybHkgYXMgcG9zc2libGUgYW5kIGFzXG4gKiBjbG9zZSBhcyBwb3NzaWJsZSB0byB0aGUgc291cmNlIG9mIHRoZSB2YWx1ZSwgdG8gbWFrZSBpdCBlYXN5IHRvIHZlcmlmeSBubyBzZWN1cml0eSBidWcgaXNcbiAqIGNyZWF0ZWQgYnkgaXRzIHVzZS5cbiAqXG4gKiBJdCBpcyBub3QgcmVxdWlyZWQgKGFuZCBub3QgcmVjb21tZW5kZWQpIHRvIGJ5cGFzcyBzZWN1cml0eSBpZiB0aGUgdmFsdWUgaXMgc2FmZSwgZS5nLiBhIFVSTCB0aGF0XG4gKiBkb2VzIG5vdCBzdGFydCB3aXRoIGEgc3VzcGljaW91cyBwcm90b2NvbCwgb3IgYW4gSFRNTCBzbmlwcGV0IHRoYXQgZG9lcyBub3QgY29udGFpbiBkYW5nZXJvdXNcbiAqIGNvZGUuIFRoZSBzYW5pdGl6ZXIgbGVhdmVzIHNhZmUgdmFsdWVzIGludGFjdC5cbiAqXG4gKiBAc2VjdXJpdHkgQ2FsbGluZyBhbnkgb2YgdGhlIGBieXBhc3NTZWN1cml0eVRydXN0Li4uYCBBUElzIGRpc2FibGVzIEFuZ3VsYXIncyBidWlsdC1pblxuICogc2FuaXRpemF0aW9uIGZvciB0aGUgdmFsdWUgcGFzc2VkIGluLiBDYXJlZnVsbHkgY2hlY2sgYW5kIGF1ZGl0IGFsbCB2YWx1ZXMgYW5kIGNvZGUgcGF0aHMgZ29pbmdcbiAqIGludG8gdGhpcyBjYWxsLiBNYWtlIHN1cmUgYW55IHVzZXIgZGF0YSBpcyBhcHByb3ByaWF0ZWx5IGVzY2FwZWQgZm9yIHRoaXMgc2VjdXJpdHkgY29udGV4dC5cbiAqIEZvciBtb3JlIGRldGFpbCwgc2VlIHRoZSBbU2VjdXJpdHkgR3VpZGVdKGh0dHA6Ly9nLmNvL25nL3NlY3VyaXR5KS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBEb21TYW5pdGl6ZXIgaW1wbGVtZW50cyBTYW5pdGl6ZXIge1xuICAvKipcbiAgICogU2FuaXRpemVzIGEgdmFsdWUgZm9yIHVzZSBpbiB0aGUgZ2l2ZW4gU2VjdXJpdHlDb250ZXh0LlxuICAgKlxuICAgKiBJZiB2YWx1ZSBpcyB0cnVzdGVkIGZvciB0aGUgY29udGV4dCwgdGhpcyBtZXRob2Qgd2lsbCB1bndyYXAgdGhlIGNvbnRhaW5lZCBzYWZlIHZhbHVlIGFuZCB1c2VcbiAgICogaXQgZGlyZWN0bHkuIE90aGVyd2lzZSwgdmFsdWUgd2lsbCBiZSBzYW5pdGl6ZWQgdG8gYmUgc2FmZSBpbiB0aGUgZ2l2ZW4gY29udGV4dCwgZm9yIGV4YW1wbGVcbiAgICogYnkgcmVwbGFjaW5nIFVSTHMgdGhhdCBoYXZlIGFuIHVuc2FmZSBwcm90b2NvbCBwYXJ0IChzdWNoIGFzIGBqYXZhc2NyaXB0OmApLiBUaGUgaW1wbGVtZW50YXRpb25cbiAgICogaXMgcmVzcG9uc2libGUgdG8gbWFrZSBzdXJlIHRoYXQgdGhlIHZhbHVlIGNhbiBkZWZpbml0ZWx5IGJlIHNhZmVseSB1c2VkIGluIHRoZSBnaXZlbiBjb250ZXh0LlxuICAgKi9cbiAgYWJzdHJhY3Qgc2FuaXRpemUoY29udGV4dDogU2VjdXJpdHlDb250ZXh0LCB2YWx1ZTogU2FmZVZhbHVlfHN0cmluZ3xudWxsKTogc3RyaW5nfG51bGw7XG5cbiAgLyoqXG4gICAqIEJ5cGFzcyBzZWN1cml0eSBhbmQgdHJ1c3QgdGhlIGdpdmVuIHZhbHVlIHRvIGJlIHNhZmUgSFRNTC4gT25seSB1c2UgdGhpcyB3aGVuIHRoZSBib3VuZCBIVE1MXG4gICAqIGlzIHVuc2FmZSAoZS5nLiBjb250YWlucyBgPHNjcmlwdD5gIHRhZ3MpIGFuZCB0aGUgY29kZSBzaG91bGQgYmUgZXhlY3V0ZWQuIFRoZSBzYW5pdGl6ZXIgd2lsbFxuICAgKiBsZWF2ZSBzYWZlIEhUTUwgaW50YWN0LCBzbyBpbiBtb3N0IHNpdHVhdGlvbnMgdGhpcyBtZXRob2Qgc2hvdWxkIG5vdCBiZSB1c2VkLlxuICAgKlxuICAgKiAqKldBUk5JTkc6KiogY2FsbGluZyB0aGlzIG1ldGhvZCB3aXRoIHVudHJ1c3RlZCB1c2VyIGRhdGEgZXhwb3NlcyB5b3VyIGFwcGxpY2F0aW9uIHRvIFhTU1xuICAgKiBzZWN1cml0eSByaXNrcyFcbiAgICovXG4gIGFic3RyYWN0IGJ5cGFzc1NlY3VyaXR5VHJ1c3RIdG1sKHZhbHVlOiBzdHJpbmcpOiBTYWZlSHRtbDtcblxuICAvKipcbiAgICogQnlwYXNzIHNlY3VyaXR5IGFuZCB0cnVzdCB0aGUgZ2l2ZW4gdmFsdWUgdG8gYmUgc2FmZSBzdHlsZSB2YWx1ZSAoQ1NTKS5cbiAgICpcbiAgICogKipXQVJOSU5HOioqIGNhbGxpbmcgdGhpcyBtZXRob2Qgd2l0aCB1bnRydXN0ZWQgdXNlciBkYXRhIGV4cG9zZXMgeW91ciBhcHBsaWNhdGlvbiB0byBYU1NcbiAgICogc2VjdXJpdHkgcmlza3MhXG4gICAqL1xuICBhYnN0cmFjdCBieXBhc3NTZWN1cml0eVRydXN0U3R5bGUodmFsdWU6IHN0cmluZyk6IFNhZmVTdHlsZTtcblxuICAvKipcbiAgICogQnlwYXNzIHNlY3VyaXR5IGFuZCB0cnVzdCB0aGUgZ2l2ZW4gdmFsdWUgdG8gYmUgc2FmZSBKYXZhU2NyaXB0LlxuICAgKlxuICAgKiAqKldBUk5JTkc6KiogY2FsbGluZyB0aGlzIG1ldGhvZCB3aXRoIHVudHJ1c3RlZCB1c2VyIGRhdGEgZXhwb3NlcyB5b3VyIGFwcGxpY2F0aW9uIHRvIFhTU1xuICAgKiBzZWN1cml0eSByaXNrcyFcbiAgICovXG4gIGFic3RyYWN0IGJ5cGFzc1NlY3VyaXR5VHJ1c3RTY3JpcHQodmFsdWU6IHN0cmluZyk6IFNhZmVTY3JpcHQ7XG5cbiAgLyoqXG4gICAqIEJ5cGFzcyBzZWN1cml0eSBhbmQgdHJ1c3QgdGhlIGdpdmVuIHZhbHVlIHRvIGJlIGEgc2FmZSBzdHlsZSBVUkwsIGkuZS4gYSB2YWx1ZSB0aGF0IGNhbiBiZSB1c2VkXG4gICAqIGluIGh5cGVybGlua3Mgb3IgYDxpbWcgc3JjPmAuXG4gICAqXG4gICAqICoqV0FSTklORzoqKiBjYWxsaW5nIHRoaXMgbWV0aG9kIHdpdGggdW50cnVzdGVkIHVzZXIgZGF0YSBleHBvc2VzIHlvdXIgYXBwbGljYXRpb24gdG8gWFNTXG4gICAqIHNlY3VyaXR5IHJpc2tzIVxuICAgKi9cbiAgYWJzdHJhY3QgYnlwYXNzU2VjdXJpdHlUcnVzdFVybCh2YWx1ZTogc3RyaW5nKTogU2FmZVVybDtcblxuICAvKipcbiAgICogQnlwYXNzIHNlY3VyaXR5IGFuZCB0cnVzdCB0aGUgZ2l2ZW4gdmFsdWUgdG8gYmUgYSBzYWZlIHJlc291cmNlIFVSTCwgaS5lLiBhIGxvY2F0aW9uIHRoYXQgbWF5XG4gICAqIGJlIHVzZWQgdG8gbG9hZCBleGVjdXRhYmxlIGNvZGUgZnJvbSwgbGlrZSBgPHNjcmlwdCBzcmM+YCwgb3IgYDxpZnJhbWUgc3JjPmAuXG4gICAqXG4gICAqICoqV0FSTklORzoqKiBjYWxsaW5nIHRoaXMgbWV0aG9kIHdpdGggdW50cnVzdGVkIHVzZXIgZGF0YSBleHBvc2VzIHlvdXIgYXBwbGljYXRpb24gdG8gWFNTXG4gICAqIHNlY3VyaXR5IHJpc2tzIVxuICAgKi9cbiAgYWJzdHJhY3QgYnlwYXNzU2VjdXJpdHlUcnVzdFJlc291cmNlVXJsKHZhbHVlOiBzdHJpbmcpOiBTYWZlUmVzb3VyY2VVcmw7XG59XG5cblxuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIERvbVNhbml0aXplckltcGwgZXh0ZW5kcyBEb21TYW5pdGl6ZXIge1xuICBjb25zdHJ1Y3RvcihASW5qZWN0KERPQ1VNRU5UKSBwcml2YXRlIF9kb2M6IGFueSkgeyBzdXBlcigpOyB9XG5cbiAgc2FuaXRpemUoY3R4OiBTZWN1cml0eUNvbnRleHQsIHZhbHVlOiBTYWZlVmFsdWV8c3RyaW5nfG51bGwpOiBzdHJpbmd8bnVsbCB7XG4gICAgaWYgKHZhbHVlID09IG51bGwpIHJldHVybiBudWxsO1xuICAgIHN3aXRjaCAoY3R4KSB7XG4gICAgICBjYXNlIFNlY3VyaXR5Q29udGV4dC5OT05FOlxuICAgICAgICByZXR1cm4gdmFsdWUgYXMgc3RyaW5nO1xuICAgICAgY2FzZSBTZWN1cml0eUNvbnRleHQuSFRNTDpcbiAgICAgICAgaWYgKHZhbHVlIGluc3RhbmNlb2YgU2FmZUh0bWxJbXBsKSByZXR1cm4gdmFsdWUuY2hhbmdpbmdUaGlzQnJlYWtzQXBwbGljYXRpb25TZWN1cml0eTtcbiAgICAgICAgdGhpcy5jaGVja05vdFNhZmVWYWx1ZSh2YWx1ZSwgJ0hUTUwnKTtcbiAgICAgICAgcmV0dXJuIF9zYW5pdGl6ZUh0bWwodGhpcy5fZG9jLCBTdHJpbmcodmFsdWUpKTtcbiAgICAgIGNhc2UgU2VjdXJpdHlDb250ZXh0LlNUWUxFOlxuICAgICAgICBpZiAodmFsdWUgaW5zdGFuY2VvZiBTYWZlU3R5bGVJbXBsKSByZXR1cm4gdmFsdWUuY2hhbmdpbmdUaGlzQnJlYWtzQXBwbGljYXRpb25TZWN1cml0eTtcbiAgICAgICAgdGhpcy5jaGVja05vdFNhZmVWYWx1ZSh2YWx1ZSwgJ1N0eWxlJyk7XG4gICAgICAgIHJldHVybiBfc2FuaXRpemVTdHlsZSh2YWx1ZSBhcyBzdHJpbmcpO1xuICAgICAgY2FzZSBTZWN1cml0eUNvbnRleHQuU0NSSVBUOlxuICAgICAgICBpZiAodmFsdWUgaW5zdGFuY2VvZiBTYWZlU2NyaXB0SW1wbCkgcmV0dXJuIHZhbHVlLmNoYW5naW5nVGhpc0JyZWFrc0FwcGxpY2F0aW9uU2VjdXJpdHk7XG4gICAgICAgIHRoaXMuY2hlY2tOb3RTYWZlVmFsdWUodmFsdWUsICdTY3JpcHQnKTtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKCd1bnNhZmUgdmFsdWUgdXNlZCBpbiBhIHNjcmlwdCBjb250ZXh0Jyk7XG4gICAgICBjYXNlIFNlY3VyaXR5Q29udGV4dC5VUkw6XG4gICAgICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIFNhZmVSZXNvdXJjZVVybEltcGwgfHwgdmFsdWUgaW5zdGFuY2VvZiBTYWZlVXJsSW1wbCkge1xuICAgICAgICAgIC8vIEFsbG93IHJlc291cmNlIFVSTHMgaW4gVVJMIGNvbnRleHRzLCB0aGV5IGFyZSBzdHJpY3RseSBtb3JlIHRydXN0ZWQuXG4gICAgICAgICAgcmV0dXJuIHZhbHVlLmNoYW5naW5nVGhpc0JyZWFrc0FwcGxpY2F0aW9uU2VjdXJpdHk7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy5jaGVja05vdFNhZmVWYWx1ZSh2YWx1ZSwgJ1VSTCcpO1xuICAgICAgICByZXR1cm4gX3Nhbml0aXplVXJsKFN0cmluZyh2YWx1ZSkpO1xuICAgICAgY2FzZSBTZWN1cml0eUNvbnRleHQuUkVTT1VSQ0VfVVJMOlxuICAgICAgICBpZiAodmFsdWUgaW5zdGFuY2VvZiBTYWZlUmVzb3VyY2VVcmxJbXBsKSB7XG4gICAgICAgICAgcmV0dXJuIHZhbHVlLmNoYW5naW5nVGhpc0JyZWFrc0FwcGxpY2F0aW9uU2VjdXJpdHk7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy5jaGVja05vdFNhZmVWYWx1ZSh2YWx1ZSwgJ1Jlc291cmNlVVJMJyk7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICAgICd1bnNhZmUgdmFsdWUgdXNlZCBpbiBhIHJlc291cmNlIFVSTCBjb250ZXh0IChzZWUgaHR0cDovL2cuY28vbmcvc2VjdXJpdHkjeHNzKScpO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBVbmV4cGVjdGVkIFNlY3VyaXR5Q29udGV4dCAke2N0eH0gKHNlZSBodHRwOi8vZy5jby9uZy9zZWN1cml0eSN4c3MpYCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjaGVja05vdFNhZmVWYWx1ZSh2YWx1ZTogYW55LCBleHBlY3RlZFR5cGU6IHN0cmluZykge1xuICAgIGlmICh2YWx1ZSBpbnN0YW5jZW9mIFNhZmVWYWx1ZUltcGwpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgUmVxdWlyZWQgYSBzYWZlICR7ZXhwZWN0ZWRUeXBlfSwgZ290IGEgJHt2YWx1ZS5nZXRUeXBlTmFtZSgpfSBgICtcbiAgICAgICAgICBgKHNlZSBodHRwOi8vZy5jby9uZy9zZWN1cml0eSN4c3MpYCk7XG4gICAgfVxuICB9XG5cbiAgYnlwYXNzU2VjdXJpdHlUcnVzdEh0bWwodmFsdWU6IHN0cmluZyk6IFNhZmVIdG1sIHsgcmV0dXJuIG5ldyBTYWZlSHRtbEltcGwodmFsdWUpOyB9XG4gIGJ5cGFzc1NlY3VyaXR5VHJ1c3RTdHlsZSh2YWx1ZTogc3RyaW5nKTogU2FmZVN0eWxlIHsgcmV0dXJuIG5ldyBTYWZlU3R5bGVJbXBsKHZhbHVlKTsgfVxuICBieXBhc3NTZWN1cml0eVRydXN0U2NyaXB0KHZhbHVlOiBzdHJpbmcpOiBTYWZlU2NyaXB0IHsgcmV0dXJuIG5ldyBTYWZlU2NyaXB0SW1wbCh2YWx1ZSk7IH1cbiAgYnlwYXNzU2VjdXJpdHlUcnVzdFVybCh2YWx1ZTogc3RyaW5nKTogU2FmZVVybCB7IHJldHVybiBuZXcgU2FmZVVybEltcGwodmFsdWUpOyB9XG4gIGJ5cGFzc1NlY3VyaXR5VHJ1c3RSZXNvdXJjZVVybCh2YWx1ZTogc3RyaW5nKTogU2FmZVJlc291cmNlVXJsIHtcbiAgICByZXR1cm4gbmV3IFNhZmVSZXNvdXJjZVVybEltcGwodmFsdWUpO1xuICB9XG59XG5cbmFic3RyYWN0IGNsYXNzIFNhZmVWYWx1ZUltcGwgaW1wbGVtZW50cyBTYWZlVmFsdWUge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgY2hhbmdpbmdUaGlzQnJlYWtzQXBwbGljYXRpb25TZWN1cml0eTogc3RyaW5nKSB7XG4gICAgLy8gZW1wdHlcbiAgfVxuXG4gIGFic3RyYWN0IGdldFR5cGVOYW1lKCk6IHN0cmluZztcblxuICB0b1N0cmluZygpIHtcbiAgICByZXR1cm4gYFNhZmVWYWx1ZSBtdXN0IHVzZSBbcHJvcGVydHldPWJpbmRpbmc6ICR7dGhpcy5jaGFuZ2luZ1RoaXNCcmVha3NBcHBsaWNhdGlvblNlY3VyaXR5fWAgK1xuICAgICAgICBgIChzZWUgaHR0cDovL2cuY28vbmcvc2VjdXJpdHkjeHNzKWA7XG4gIH1cbn1cblxuY2xhc3MgU2FmZUh0bWxJbXBsIGV4dGVuZHMgU2FmZVZhbHVlSW1wbCBpbXBsZW1lbnRzIFNhZmVIdG1sIHtcbiAgZ2V0VHlwZU5hbWUoKSB7IHJldHVybiAnSFRNTCc7IH1cbn1cbmNsYXNzIFNhZmVTdHlsZUltcGwgZXh0ZW5kcyBTYWZlVmFsdWVJbXBsIGltcGxlbWVudHMgU2FmZVN0eWxlIHtcbiAgZ2V0VHlwZU5hbWUoKSB7IHJldHVybiAnU3R5bGUnOyB9XG59XG5jbGFzcyBTYWZlU2NyaXB0SW1wbCBleHRlbmRzIFNhZmVWYWx1ZUltcGwgaW1wbGVtZW50cyBTYWZlU2NyaXB0IHtcbiAgZ2V0VHlwZU5hbWUoKSB7IHJldHVybiAnU2NyaXB0JzsgfVxufVxuY2xhc3MgU2FmZVVybEltcGwgZXh0ZW5kcyBTYWZlVmFsdWVJbXBsIGltcGxlbWVudHMgU2FmZVVybCB7XG4gIGdldFR5cGVOYW1lKCkgeyByZXR1cm4gJ1VSTCc7IH1cbn1cbmNsYXNzIFNhZmVSZXNvdXJjZVVybEltcGwgZXh0ZW5kcyBTYWZlVmFsdWVJbXBsIGltcGxlbWVudHMgU2FmZVJlc291cmNlVXJsIHtcbiAgZ2V0VHlwZU5hbWUoKSB7IHJldHVybiAnUmVzb3VyY2VVUkwnOyB9XG59XG4iXX0=