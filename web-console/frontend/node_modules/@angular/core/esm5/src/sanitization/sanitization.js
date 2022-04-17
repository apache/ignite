/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { SANITIZER } from '../render3/interfaces/view';
import { getLView } from '../render3/state';
import { renderStringify } from '../render3/util/misc_utils';
import { allowSanitizationBypass } from './bypass';
import { _sanitizeHtml as _sanitizeHtml } from './html_sanitizer';
import { SecurityContext } from './security';
import { _sanitizeStyle as _sanitizeStyle } from './style_sanitizer';
import { _sanitizeUrl as _sanitizeUrl } from './url_sanitizer';
/**
 * An `html` sanitizer which converts untrusted `html` **string** into trusted string by removing
 * dangerous content.
 *
 * This method parses the `html` and locates potentially dangerous content (such as urls and
 * javascript) and removes it.
 *
 * It is possible to mark a string as trusted by calling {@link bypassSanitizationTrustHtml}.
 *
 * @param unsafeHtml untrusted `html`, typically from the user.
 * @returns `html` string which is safe to display to user, because all of the dangerous javascript
 * and urls have been removed.
 *
 * @publicApi
 */
export function ɵɵsanitizeHtml(unsafeHtml) {
    var sanitizer = getSanitizer();
    if (sanitizer) {
        return sanitizer.sanitize(SecurityContext.HTML, unsafeHtml) || '';
    }
    if (allowSanitizationBypass(unsafeHtml, "Html" /* Html */)) {
        return unsafeHtml.toString();
    }
    return _sanitizeHtml(document, renderStringify(unsafeHtml));
}
/**
 * A `style` sanitizer which converts untrusted `style` **string** into trusted string by removing
 * dangerous content.
 *
 * This method parses the `style` and locates potentially dangerous content (such as urls and
 * javascript) and removes it.
 *
 * It is possible to mark a string as trusted by calling {@link bypassSanitizationTrustStyle}.
 *
 * @param unsafeStyle untrusted `style`, typically from the user.
 * @returns `style` string which is safe to bind to the `style` properties, because all of the
 * dangerous javascript and urls have been removed.
 *
 * @publicApi
 */
export function ɵɵsanitizeStyle(unsafeStyle) {
    var sanitizer = getSanitizer();
    if (sanitizer) {
        return sanitizer.sanitize(SecurityContext.STYLE, unsafeStyle) || '';
    }
    if (allowSanitizationBypass(unsafeStyle, "Style" /* Style */)) {
        return unsafeStyle.toString();
    }
    return _sanitizeStyle(renderStringify(unsafeStyle));
}
/**
 * A `url` sanitizer which converts untrusted `url` **string** into trusted string by removing
 * dangerous
 * content.
 *
 * This method parses the `url` and locates potentially dangerous content (such as javascript) and
 * removes it.
 *
 * It is possible to mark a string as trusted by calling {@link bypassSanitizationTrustUrl}.
 *
 * @param unsafeUrl untrusted `url`, typically from the user.
 * @returns `url` string which is safe to bind to the `src` properties such as `<img src>`, because
 * all of the dangerous javascript has been removed.
 *
 * @publicApi
 */
export function ɵɵsanitizeUrl(unsafeUrl) {
    var sanitizer = getSanitizer();
    if (sanitizer) {
        return sanitizer.sanitize(SecurityContext.URL, unsafeUrl) || '';
    }
    if (allowSanitizationBypass(unsafeUrl, "Url" /* Url */)) {
        return unsafeUrl.toString();
    }
    return _sanitizeUrl(renderStringify(unsafeUrl));
}
/**
 * A `url` sanitizer which only lets trusted `url`s through.
 *
 * This passes only `url`s marked trusted by calling {@link bypassSanitizationTrustResourceUrl}.
 *
 * @param unsafeResourceUrl untrusted `url`, typically from the user.
 * @returns `url` string which is safe to bind to the `src` properties such as `<img src>`, because
 * only trusted `url`s have been allowed to pass.
 *
 * @publicApi
 */
export function ɵɵsanitizeResourceUrl(unsafeResourceUrl) {
    var sanitizer = getSanitizer();
    if (sanitizer) {
        return sanitizer.sanitize(SecurityContext.RESOURCE_URL, unsafeResourceUrl) || '';
    }
    if (allowSanitizationBypass(unsafeResourceUrl, "ResourceUrl" /* ResourceUrl */)) {
        return unsafeResourceUrl.toString();
    }
    throw new Error('unsafe value used in a resource URL context (see http://g.co/ng/security#xss)');
}
/**
 * A `script` sanitizer which only lets trusted javascript through.
 *
 * This passes only `script`s marked trusted by calling {@link
 * bypassSanitizationTrustScript}.
 *
 * @param unsafeScript untrusted `script`, typically from the user.
 * @returns `url` string which is safe to bind to the `<script>` element such as `<img src>`,
 * because only trusted `scripts` have been allowed to pass.
 *
 * @publicApi
 */
export function ɵɵsanitizeScript(unsafeScript) {
    var sanitizer = getSanitizer();
    if (sanitizer) {
        return sanitizer.sanitize(SecurityContext.SCRIPT, unsafeScript) || '';
    }
    if (allowSanitizationBypass(unsafeScript, "Script" /* Script */)) {
        return unsafeScript.toString();
    }
    throw new Error('unsafe value used in a script context');
}
/**
 * Detects which sanitizer to use for URL property, based on tag name and prop name.
 *
 * The rules are based on the RESOURCE_URL context config from
 * `packages/compiler/src/schema/dom_security_schema.ts`.
 * If tag and prop names don't match Resource URL schema, use URL sanitizer.
 */
export function getUrlSanitizer(tag, prop) {
    if ((prop === 'src' && (tag === 'embed' || tag === 'frame' || tag === 'iframe' ||
        tag === 'media' || tag === 'script')) ||
        (prop === 'href' && (tag === 'base' || tag === 'link'))) {
        return ɵɵsanitizeResourceUrl;
    }
    return ɵɵsanitizeUrl;
}
/**
 * Sanitizes URL, selecting sanitizer function based on tag and property names.
 *
 * This function is used in case we can't define security context at compile time, when only prop
 * name is available. This happens when we generate host bindings for Directives/Components. The
 * host element is unknown at compile time, so we defer calculation of specific sanitizer to
 * runtime.
 *
 * @param unsafeUrl untrusted `url`, typically from the user.
 * @param tag target element tag name.
 * @param prop name of the property that contains the value.
 * @returns `url` string which is safe to bind.
 *
 * @publicApi
 */
export function ɵɵsanitizeUrlOrResourceUrl(unsafeUrl, tag, prop) {
    return getUrlSanitizer(tag, prop)(unsafeUrl);
}
/**
 * The default style sanitizer will handle sanitization for style properties by
 * sanitizing any CSS property that can include a `url` value (usually image-based properties)
 *
 * @publicApi
 */
export var ɵɵdefaultStyleSanitizer = function (prop, value, mode) {
    mode = mode || 3 /* ValidateAndSanitize */;
    var doSanitizeValue = true;
    if (mode & 1 /* ValidateProperty */) {
        doSanitizeValue = prop === 'background-image' || prop === 'background' ||
            prop === 'border-image' || prop === 'filter' || prop === 'list-style' ||
            prop === 'list-style-image' || prop === 'clip-path';
    }
    if (mode & 2 /* SanitizeOnly */) {
        return doSanitizeValue ? ɵɵsanitizeStyle(value) : value;
    }
    else {
        return doSanitizeValue;
    }
};
export function validateAgainstEventProperties(name) {
    if (name.toLowerCase().startsWith('on')) {
        var msg = "Binding to event property '" + name + "' is disallowed for security reasons, " +
            ("please use (" + name.slice(2) + ")=...") +
            ("\nIf '" + name + "' is a directive input, make sure the directive is imported by the") +
            " current module.";
        throw new Error(msg);
    }
}
export function validateAgainstEventAttributes(name) {
    if (name.toLowerCase().startsWith('on')) {
        var msg = "Binding to event attribute '" + name + "' is disallowed for security reasons, " +
            ("please use (" + name.slice(2) + ")=...");
        throw new Error(msg);
    }
}
function getSanitizer() {
    var lView = getLView();
    return lView && lView[SANITIZER];
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2FuaXRpemF0aW9uLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvc2FuaXRpemF0aW9uL3Nhbml0aXphdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFFSCxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sNEJBQTRCLENBQUM7QUFDckQsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGtCQUFrQixDQUFDO0FBQzFDLE9BQU8sRUFBQyxlQUFlLEVBQUMsTUFBTSw0QkFBNEIsQ0FBQztBQUUzRCxPQUFPLEVBQWEsdUJBQXVCLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFDN0QsT0FBTyxFQUFDLGFBQWEsSUFBSSxhQUFhLEVBQUMsTUFBTSxrQkFBa0IsQ0FBQztBQUNoRSxPQUFPLEVBQVksZUFBZSxFQUFDLE1BQU0sWUFBWSxDQUFDO0FBQ3RELE9BQU8sRUFBcUMsY0FBYyxJQUFJLGNBQWMsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBQ3ZHLE9BQU8sRUFBQyxZQUFZLElBQUksWUFBWSxFQUFDLE1BQU0saUJBQWlCLENBQUM7QUFJN0Q7Ozs7Ozs7Ozs7Ozs7O0dBY0c7QUFDSCxNQUFNLFVBQVUsY0FBYyxDQUFDLFVBQWU7SUFDNUMsSUFBTSxTQUFTLEdBQUcsWUFBWSxFQUFFLENBQUM7SUFDakMsSUFBSSxTQUFTLEVBQUU7UUFDYixPQUFPLFNBQVMsQ0FBQyxRQUFRLENBQUMsZUFBZSxDQUFDLElBQUksRUFBRSxVQUFVLENBQUMsSUFBSSxFQUFFLENBQUM7S0FDbkU7SUFDRCxJQUFJLHVCQUF1QixDQUFDLFVBQVUsb0JBQWtCLEVBQUU7UUFDeEQsT0FBTyxVQUFVLENBQUMsUUFBUSxFQUFFLENBQUM7S0FDOUI7SUFDRCxPQUFPLGFBQWEsQ0FBQyxRQUFRLEVBQUUsZUFBZSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUM7QUFDOUQsQ0FBQztB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LElBQU0sR0FBRyxHQUFHLGlDQUErQixJQUFJLDJDQUF3QzthQUNuRixpQkFBZSxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxVQUFPLENBQUEsQ0FBQztRQUN4QyxNQUFNLElBQUksS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDO0tBQ3RCO0FBQ0gsQ0FBQztBQUVELFNBQVMsWUFBWTtJQUNuQixJQUFNLEtBQUssR0FBRyxRQUFRLEVBQUUsQ0FBQztJQUN6QixPQUFPLEtBQUssSUFBSSxLQUFLLENBQUMsU0FBUyxDQUFDLENBQUM7QUFDbkMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtTQU5JVElaRVJ9IGZyb20gJy4uL3JlbmRlcjMvaW50ZXJmYWNlcy92aWV3JztcbmltcG9ydCB7Z2V0TFZpZXd9IGZyb20gJy4uL3JlbmRlcjMvc3RhdGUnO1xuaW1wb3J0IHtyZW5kZXJTdHJpbmdpZnl9IGZyb20gJy4uL3JlbmRlcjMvdXRpbC9taXNjX3V0aWxzJztcblxuaW1wb3J0IHtCeXBhc3NUeXBlLCBhbGxvd1Nhbml0aXphdGlvbkJ5cGFzc30gZnJvbSAnLi9ieXBhc3MnO1xuaW1wb3J0IHtfc2FuaXRpemVIdG1sIGFzIF9zYW5pdGl6ZUh0bWx9IGZyb20gJy4vaHRtbF9zYW5pdGl6ZXInO1xuaW1wb3J0IHtTYW5pdGl6ZXIsIFNlY3VyaXR5Q29udGV4dH0gZnJvbSAnLi9zZWN1cml0eSc7XG5pbXBvcnQge1N0eWxlU2FuaXRpemVGbiwgU3R5bGVTYW5pdGl6ZU1vZGUsIF9zYW5pdGl6ZVN0eWxlIGFzIF9zYW5pdGl6ZVN0eWxlfSBmcm9tICcuL3N0eWxlX3Nhbml0aXplcic7XG5pbXBvcnQge19zYW5pdGl6ZVVybCBhcyBfc2FuaXRpemVVcmx9IGZyb20gJy4vdXJsX3Nhbml0aXplcic7XG5cblxuXG4vKipcbiAqIEFuIGBodG1sYCBzYW5pdGl6ZXIgd2hpY2ggY29udmVydHMgdW50cnVzdGVkIGBodG1sYCAqKnN0cmluZyoqIGludG8gdHJ1c3RlZCBzdHJpbmcgYnkgcmVtb3ZpbmdcbiAqIGRhbmdlcm91cyBjb250ZW50LlxuICpcbiAqIFRoaXMgbWV0aG9kIHBhcnNlcyB0aGUgYGh0bWxgIGFuZCBsb2NhdGVzIHBvdGVudGlhbGx5IGRhbmdlcm91cyBjb250ZW50IChzdWNoIGFzIHVybHMgYW5kXG4gKiBqYXZhc2NyaXB0KSBhbmQgcmVtb3ZlcyBpdC5cbiAqXG4gKiBJdCBpcyBwb3NzaWJsZSB0byBtYXJrIGEgc3RyaW5nIGFzIHRydXN0ZWQgYnkgY2FsbGluZyB7QGxpbmsgYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RIdG1sfS5cbiAqXG4gKiBAcGFyYW0gdW5zYWZlSHRtbCB1bnRydXN0ZWQgYGh0bWxgLCB0eXBpY2FsbHkgZnJvbSB0aGUgdXNlci5cbiAqIEByZXR1cm5zIGBodG1sYCBzdHJpbmcgd2hpY2ggaXMgc2FmZSB0byBkaXNwbGF5IHRvIHVzZXIsIGJlY2F1c2UgYWxsIG9mIHRoZSBkYW5nZXJvdXMgamF2YXNjcmlwdFxuICogYW5kIHVybHMgaGF2ZSBiZWVuIHJlbW92ZWQuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXNhbml0aXplSHRtbCh1bnNhZmVIdG1sOiBhbnkpOiBzdHJpbmcge1xuICBjb25zdCBzYW5pdGl6ZXIgPSBnZXRTYW5pdGl6ZXIoKTtcbiAgaWYgKHNhbml0aXplcikge1xuICAgIHJldHVybiBzYW5pdGl6ZXIuc2FuaXRpemUoU2VjdXJpdHlDb250ZXh0LkhUTUwsIHVuc2FmZUh0bWwpIHx8ICcnO1xuICB9XG4gIGlmIChhbGxvd1Nhbml0aXphdGlvbkJ5cGFzcyh1bnNhZmVIdG1sLCBCeXBhc3NUeXBlLkh0bWwpKSB7XG4gICAgcmV0dXJuIHVuc2FmZUh0bWwudG9TdHJpbmcoKTtcbiAgfVxuICByZXR1cm4gX3Nhbml0aXplSHRtbChkb2N1bWVudCwgcmVuZGVyU3RyaW5naWZ5KHVuc2FmZUh0bWwpKTtcbn1cblxuLyoqXG4gKiBBIGBzdHlsZWAgc2FuaXRpemVyIHdoaWNoIGNvbnZlcnRzIHVudHJ1c3RlZCBgc3R5bGVgICoqc3RyaW5nKiogaW50byB0cnVzdGVkIHN0cmluZyBieSByZW1vdmluZ1xuICogZGFuZ2Vyb3VzIGNvbnRlbnQuXG4gKlxuICogVGhpcyBtZXRob2QgcGFyc2VzIHRoZSBgc3R5bGVgIGFuZCBsb2NhdGVzIHBvdGVudGlhbGx5IGRhbmdlcm91cyBjb250ZW50IChzdWNoIGFzIHVybHMgYW5kXG4gKiBqYXZhc2NyaXB0KSBhbmQgcmVtb3ZlcyBpdC5cbiAqXG4gKiBJdCBpcyBwb3NzaWJsZSB0byBtYXJrIGEgc3RyaW5nIGFzIHRydXN0ZWQgYnkgY2FsbGluZyB7QGxpbmsgYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RTdHlsZX0uXG4gKlxuICogQHBhcmFtIHVuc2FmZVN0eWxlIHVudHJ1c3RlZCBgc3R5bGVgLCB0eXBpY2FsbHkgZnJvbSB0aGUgdXNlci5cbiAqIEByZXR1cm5zIGBzdHlsZWAgc3RyaW5nIHdoaWNoIGlzIHNhZmUgdG8gYmluZCB0byB0aGUgYHN0eWxlYCBwcm9wZXJ0aWVzLCBiZWNhdXNlIGFsbCBvZiB0aGVcbiAqIGRhbmdlcm91cyBqYXZhc2NyaXB0IGFuZCB1cmxzIGhhdmUgYmVlbiByZW1vdmVkLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIMm1ybVzYW5pdGl6ZVN0eWxlKHVuc2FmZVN0eWxlOiBhbnkpOiBzdHJpbmcge1xuICBjb25zdCBzYW5pdGl6ZXIgPSBnZXRTYW5pdGl6ZXIoKTtcbiAgaWYgKHNhbml0aXplcikge1xuICAgIHJldHVybiBzYW5pdGl6ZXIuc2FuaXRpemUoU2VjdXJpdHlDb250ZXh0LlNUWUxFLCB1bnNhZmVTdHlsZSkgfHwgJyc7XG4gIH1cbiAgaWYgKGFsbG93U2FuaXRpemF0aW9uQnlwYXNzKHVuc2FmZVN0eWxlLCBCeXBhc3NUeXBlLlN0eWxlKSkge1xuICAgIHJldHVybiB1bnNhZmVTdHlsZS50b1N0cmluZygpO1xuICB9XG4gIHJldHVybiBfc2FuaXRpemVTdHlsZShyZW5kZXJTdHJpbmdpZnkodW5zYWZlU3R5bGUpKTtcbn1cblxuLyoqXG4gKiBBIGB1cmxgIHNhbml0aXplciB3aGljaCBjb252ZXJ0cyB1bnRydXN0ZWQgYHVybGAgKipzdHJpbmcqKiBpbnRvIHRydXN0ZWQgc3RyaW5nIGJ5IHJlbW92aW5nXG4gKiBkYW5nZXJvdXNcbiAqIGNvbnRlbnQuXG4gKlxuICogVGhpcyBtZXRob2QgcGFyc2VzIHRoZSBgdXJsYCBhbmQgbG9jYXRlcyBwb3RlbnRpYWxseSBkYW5nZXJvdXMgY29udGVudCAoc3VjaCBhcyBqYXZhc2NyaXB0KSBhbmRcbiAqIHJlbW92ZXMgaXQuXG4gKlxuICogSXQgaXMgcG9zc2libGUgdG8gbWFyayBhIHN0cmluZyBhcyB0cnVzdGVkIGJ5IGNhbGxpbmcge0BsaW5rIGJ5cGFzc1Nhbml0aXphdGlvblRydXN0VXJsfS5cbiAqXG4gKiBAcGFyYW0gdW5zYWZlVXJsIHVudHJ1c3RlZCBgdXJsYCwgdHlwaWNhbGx5IGZyb20gdGhlIHVzZXIuXG4gKiBAcmV0dXJucyBgdXJsYCBzdHJpbmcgd2hpY2ggaXMgc2FmZSB0byBiaW5kIHRvIHRoZSBgc3JjYCBwcm9wZXJ0aWVzIHN1Y2ggYXMgYDxpbWcgc3JjPmAsIGJlY2F1c2VcbiAqIGFsbCBvZiB0aGUgZGFuZ2Vyb3VzIGphdmFzY3JpcHQgaGFzIGJlZW4gcmVtb3ZlZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1c2FuaXRpemVVcmwodW5zYWZlVXJsOiBhbnkpOiBzdHJpbmcge1xuICBjb25zdCBzYW5pdGl6ZXIgPSBnZXRTYW5pdGl6ZXIoKTtcbiAgaWYgKHNhbml0aXplcikge1xuICAgIHJldHVybiBzYW5pdGl6ZXIuc2FuaXRpemUoU2VjdXJpdHlDb250ZXh0LlVSTCwgdW5zYWZlVXJsKSB8fCAnJztcbiAgfVxuICBpZiAoYWxsb3dTYW5pdGl6YXRpb25CeXBhc3ModW5zYWZlVXJsLCBCeXBhc3NUeXBlLlVybCkpIHtcbiAgICByZXR1cm4gdW5zYWZlVXJsLnRvU3RyaW5nKCk7XG4gIH1cbiAgcmV0dXJuIF9zYW5pdGl6ZVVybChyZW5kZXJTdHJpbmdpZnkodW5zYWZlVXJsKSk7XG59XG5cbi8qKlxuICogQSBgdXJsYCBzYW5pdGl6ZXIgd2hpY2ggb25seSBsZXRzIHRydXN0ZWQgYHVybGBzIHRocm91Z2guXG4gKlxuICogVGhpcyBwYXNzZXMgb25seSBgdXJsYHMgbWFya2VkIHRydXN0ZWQgYnkgY2FsbGluZyB7QGxpbmsgYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RSZXNvdXJjZVVybH0uXG4gKlxuICogQHBhcmFtIHVuc2FmZVJlc291cmNlVXJsIHVudHJ1c3RlZCBgdXJsYCwgdHlwaWNhbGx5IGZyb20gdGhlIHVzZXIuXG4gKiBAcmV0dXJucyBgdXJsYCBzdHJpbmcgd2hpY2ggaXMgc2FmZSB0byBiaW5kIHRvIHRoZSBgc3JjYCBwcm9wZXJ0aWVzIHN1Y2ggYXMgYDxpbWcgc3JjPmAsIGJlY2F1c2VcbiAqIG9ubHkgdHJ1c3RlZCBgdXJsYHMgaGF2ZSBiZWVuIGFsbG93ZWQgdG8gcGFzcy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1c2FuaXRpemVSZXNvdXJjZVVybCh1bnNhZmVSZXNvdXJjZVVybDogYW55KTogc3RyaW5nIHtcbiAgY29uc3Qgc2FuaXRpemVyID0gZ2V0U2FuaXRpemVyKCk7XG4gIGlmIChzYW5pdGl6ZXIpIHtcbiAgICByZXR1cm4gc2FuaXRpemVyLnNhbml0aXplKFNlY3VyaXR5Q29udGV4dC5SRVNPVVJDRV9VUkwsIHVuc2FmZVJlc291cmNlVXJsKSB8fCAnJztcbiAgfVxuICBpZiAoYWxsb3dTYW5pdGl6YXRpb25CeXBhc3ModW5zYWZlUmVzb3VyY2VVcmwsIEJ5cGFzc1R5cGUuUmVzb3VyY2VVcmwpKSB7XG4gICAgcmV0dXJuIHVuc2FmZVJlc291cmNlVXJsLnRvU3RyaW5nKCk7XG4gIH1cbiAgdGhyb3cgbmV3IEVycm9yKCd1bnNhZmUgdmFsdWUgdXNlZCBpbiBhIHJlc291cmNlIFVSTCBjb250ZXh0IChzZWUgaHR0cDovL2cuY28vbmcvc2VjdXJpdHkjeHNzKScpO1xufVxuXG4vKipcbiAqIEEgYHNjcmlwdGAgc2FuaXRpemVyIHdoaWNoIG9ubHkgbGV0cyB0cnVzdGVkIGphdmFzY3JpcHQgdGhyb3VnaC5cbiAqXG4gKiBUaGlzIHBhc3NlcyBvbmx5IGBzY3JpcHRgcyBtYXJrZWQgdHJ1c3RlZCBieSBjYWxsaW5nIHtAbGlua1xuICogYnlwYXNzU2FuaXRpemF0aW9uVHJ1c3RTY3JpcHR9LlxuICpcbiAqIEBwYXJhbSB1bnNhZmVTY3JpcHQgdW50cnVzdGVkIGBzY3JpcHRgLCB0eXBpY2FsbHkgZnJvbSB0aGUgdXNlci5cbiAqIEByZXR1cm5zIGB1cmxgIHN0cmluZyB3aGljaCBpcyBzYWZlIHRvIGJpbmQgdG8gdGhlIGA8c2NyaXB0PmAgZWxlbWVudCBzdWNoIGFzIGA8aW1nIHNyYz5gLFxuICogYmVjYXVzZSBvbmx5IHRydXN0ZWQgYHNjcmlwdHNgIGhhdmUgYmVlbiBhbGxvd2VkIHRvIHBhc3MuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gybXJtXNhbml0aXplU2NyaXB0KHVuc2FmZVNjcmlwdDogYW55KTogc3RyaW5nIHtcbiAgY29uc3Qgc2FuaXRpemVyID0gZ2V0U2FuaXRpemVyKCk7XG4gIGlmIChzYW5pdGl6ZXIpIHtcbiAgICByZXR1cm4gc2FuaXRpemVyLnNhbml0aXplKFNlY3VyaXR5Q29udGV4dC5TQ1JJUFQsIHVuc2FmZVNjcmlwdCkgfHwgJyc7XG4gIH1cbiAgaWYgKGFsbG93U2FuaXRpemF0aW9uQnlwYXNzKHVuc2FmZVNjcmlwdCwgQnlwYXNzVHlwZS5TY3JpcHQpKSB7XG4gICAgcmV0dXJuIHVuc2FmZVNjcmlwdC50b1N0cmluZygpO1xuICB9XG4gIHRocm93IG5ldyBFcnJvcigndW5zYWZlIHZhbHVlIHVzZWQgaW4gYSBzY3JpcHQgY29udGV4dCcpO1xufVxuXG4vKipcbiAqIERldGVjdHMgd2hpY2ggc2FuaXRpemVyIHRvIHVzZSBmb3IgVVJMIHByb3BlcnR5LCBiYXNlZCBvbiB0YWcgbmFtZSBhbmQgcHJvcCBuYW1lLlxuICpcbiAqIFRoZSBydWxlcyBhcmUgYmFzZWQgb24gdGhlIFJFU09VUkNFX1VSTCBjb250ZXh0IGNvbmZpZyBmcm9tXG4gKiBgcGFja2FnZXMvY29tcGlsZXIvc3JjL3NjaGVtYS9kb21fc2VjdXJpdHlfc2NoZW1hLnRzYC5cbiAqIElmIHRhZyBhbmQgcHJvcCBuYW1lcyBkb24ndCBtYXRjaCBSZXNvdXJjZSBVUkwgc2NoZW1hLCB1c2UgVVJMIHNhbml0aXplci5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldFVybFNhbml0aXplcih0YWc6IHN0cmluZywgcHJvcDogc3RyaW5nKSB7XG4gIGlmICgocHJvcCA9PT0gJ3NyYycgJiYgKHRhZyA9PT0gJ2VtYmVkJyB8fCB0YWcgPT09ICdmcmFtZScgfHwgdGFnID09PSAnaWZyYW1lJyB8fFxuICAgICAgICAgICAgICAgICAgICAgICAgICB0YWcgPT09ICdtZWRpYScgfHwgdGFnID09PSAnc2NyaXB0JykpIHx8XG4gICAgICAocHJvcCA9PT0gJ2hyZWYnICYmICh0YWcgPT09ICdiYXNlJyB8fCB0YWcgPT09ICdsaW5rJykpKSB7XG4gICAgcmV0dXJuIMm1ybVzYW5pdGl6ZVJlc291cmNlVXJsO1xuICB9XG4gIHJldHVybiDJtcm1c2FuaXRpemVVcmw7XG59XG5cbi8qKlxuICogU2FuaXRpemVzIFVSTCwgc2VsZWN0aW5nIHNhbml0aXplciBmdW5jdGlvbiBiYXNlZCBvbiB0YWcgYW5kIHByb3BlcnR5IG5hbWVzLlxuICpcbiAqIFRoaXMgZnVuY3Rpb24gaXMgdXNlZCBpbiBjYXNlIHdlIGNhbid0IGRlZmluZSBzZWN1cml0eSBjb250ZXh0IGF0IGNvbXBpbGUgdGltZSwgd2hlbiBvbmx5IHByb3BcbiAqIG5hbWUgaXMgYXZhaWxhYmxlLiBUaGlzIGhhcHBlbnMgd2hlbiB3ZSBnZW5lcmF0ZSBob3N0IGJpbmRpbmdzIGZvciBEaXJlY3RpdmVzL0NvbXBvbmVudHMuIFRoZVxuICogaG9zdCBlbGVtZW50IGlzIHVua25vd24gYXQgY29tcGlsZSB0aW1lLCBzbyB3ZSBkZWZlciBjYWxjdWxhdGlvbiBvZiBzcGVjaWZpYyBzYW5pdGl6ZXIgdG9cbiAqIHJ1bnRpbWUuXG4gKlxuICogQHBhcmFtIHVuc2FmZVVybCB1bnRydXN0ZWQgYHVybGAsIHR5cGljYWxseSBmcm9tIHRoZSB1c2VyLlxuICogQHBhcmFtIHRhZyB0YXJnZXQgZWxlbWVudCB0YWcgbmFtZS5cbiAqIEBwYXJhbSBwcm9wIG5hbWUgb2YgdGhlIHByb3BlcnR5IHRoYXQgY29udGFpbnMgdGhlIHZhbHVlLlxuICogQHJldHVybnMgYHVybGAgc3RyaW5nIHdoaWNoIGlzIHNhZmUgdG8gYmluZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiDJtcm1c2FuaXRpemVVcmxPclJlc291cmNlVXJsKHVuc2FmZVVybDogYW55LCB0YWc6IHN0cmluZywgcHJvcDogc3RyaW5nKTogYW55IHtcbiAgcmV0dXJuIGdldFVybFNhbml0aXplcih0YWcsIHByb3ApKHVuc2FmZVVybCk7XG59XG5cbi8qKlxuICogVGhlIGRlZmF1bHQgc3R5bGUgc2FuaXRpemVyIHdpbGwgaGFuZGxlIHNhbml0aXphdGlvbiBmb3Igc3R5bGUgcHJvcGVydGllcyBieVxuICogc2FuaXRpemluZyBhbnkgQ1NTIHByb3BlcnR5IHRoYXQgY2FuIGluY2x1ZGUgYSBgdXJsYCB2YWx1ZSAodXN1YWxseSBpbWFnZS1iYXNlZCBwcm9wZXJ0aWVzKVxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNvbnN0IMm1ybVkZWZhdWx0U3R5bGVTYW5pdGl6ZXIgPVxuICAgIChmdW5jdGlvbihwcm9wOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmd8bnVsbCwgbW9kZT86IFN0eWxlU2FuaXRpemVNb2RlKTogc3RyaW5nIHwgYm9vbGVhbiB8IG51bGwge1xuICAgICAgbW9kZSA9IG1vZGUgfHwgU3R5bGVTYW5pdGl6ZU1vZGUuVmFsaWRhdGVBbmRTYW5pdGl6ZTtcbiAgICAgIGxldCBkb1Nhbml0aXplVmFsdWUgPSB0cnVlO1xuICAgICAgaWYgKG1vZGUgJiBTdHlsZVNhbml0aXplTW9kZS5WYWxpZGF0ZVByb3BlcnR5KSB7XG4gICAgICAgIGRvU2FuaXRpemVWYWx1ZSA9IHByb3AgPT09ICdiYWNrZ3JvdW5kLWltYWdlJyB8fCBwcm9wID09PSAnYmFja2dyb3VuZCcgfHxcbiAgICAgICAgICAgIHByb3AgPT09ICdib3JkZXItaW1hZ2UnIHx8IHByb3AgPT09ICdmaWx0ZXInIHx8IHByb3AgPT09ICdsaXN0LXN0eWxlJyB8fFxuICAgICAgICAgICAgcHJvcCA9PT0gJ2xpc3Qtc3R5bGUtaW1hZ2UnIHx8IHByb3AgPT09ICdjbGlwLXBhdGgnO1xuICAgICAgfVxuXG4gICAgICBpZiAobW9kZSAmIFN0eWxlU2FuaXRpemVNb2RlLlNhbml0aXplT25seSkge1xuICAgICAgICByZXR1cm4gZG9TYW5pdGl6ZVZhbHVlID8gybXJtXNhbml0aXplU3R5bGUodmFsdWUpIDogdmFsdWU7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gZG9TYW5pdGl6ZVZhbHVlO1xuICAgICAgfVxuICAgIH0gYXMgU3R5bGVTYW5pdGl6ZUZuKTtcblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlQWdhaW5zdEV2ZW50UHJvcGVydGllcyhuYW1lOiBzdHJpbmcpIHtcbiAgaWYgKG5hbWUudG9Mb3dlckNhc2UoKS5zdGFydHNXaXRoKCdvbicpKSB7XG4gICAgY29uc3QgbXNnID0gYEJpbmRpbmcgdG8gZXZlbnQgcHJvcGVydHkgJyR7bmFtZX0nIGlzIGRpc2FsbG93ZWQgZm9yIHNlY3VyaXR5IHJlYXNvbnMsIGAgK1xuICAgICAgICBgcGxlYXNlIHVzZSAoJHtuYW1lLnNsaWNlKDIpfSk9Li4uYCArXG4gICAgICAgIGBcXG5JZiAnJHtuYW1lfScgaXMgYSBkaXJlY3RpdmUgaW5wdXQsIG1ha2Ugc3VyZSB0aGUgZGlyZWN0aXZlIGlzIGltcG9ydGVkIGJ5IHRoZWAgK1xuICAgICAgICBgIGN1cnJlbnQgbW9kdWxlLmA7XG4gICAgdGhyb3cgbmV3IEVycm9yKG1zZyk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHZhbGlkYXRlQWdhaW5zdEV2ZW50QXR0cmlidXRlcyhuYW1lOiBzdHJpbmcpIHtcbiAgaWYgKG5hbWUudG9Mb3dlckNhc2UoKS5zdGFydHNXaXRoKCdvbicpKSB7XG4gICAgY29uc3QgbXNnID0gYEJpbmRpbmcgdG8gZXZlbnQgYXR0cmlidXRlICcke25hbWV9JyBpcyBkaXNhbGxvd2VkIGZvciBzZWN1cml0eSByZWFzb25zLCBgICtcbiAgICAgICAgYHBsZWFzZSB1c2UgKCR7bmFtZS5zbGljZSgyKX0pPS4uLmA7XG4gICAgdGhyb3cgbmV3IEVycm9yKG1zZyk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0U2FuaXRpemVyKCk6IFNhbml0aXplcnxudWxsIHtcbiAgY29uc3QgbFZpZXcgPSBnZXRMVmlldygpO1xuICByZXR1cm4gbFZpZXcgJiYgbFZpZXdbU0FOSVRJWkVSXTtcbn1cbiJdfQ==