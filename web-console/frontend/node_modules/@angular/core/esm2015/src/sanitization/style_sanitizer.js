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
import { isDevMode } from '../util/is_dev_mode';
import { _sanitizeUrl } from './url_sanitizer';
/**
 * Regular expression for safe style values.
 *
 * Quotes (" and ') are allowed, but a check must be done elsewhere to ensure they're balanced.
 *
 * ',' allows multiple values to be assigned to the same property (e.g. background-attachment or
 * font-family) and hence could allow multiple values to get injected, but that should pose no risk
 * of XSS.
 *
 * The function expression checks only for XSS safety, not for CSS validity.
 *
 * This regular expression was taken from the Closure sanitization library, and augmented for
 * transformation values.
 * @type {?}
 */
const VALUES = '[-,."\'%_!# a-zA-Z0-9]+';
/** @type {?} */
const TRANSFORMATION_FNS = '(?:matrix|translate|scale|rotate|skew|perspective)(?:X|Y|Z|3d)?';
/** @type {?} */
const COLOR_FNS = '(?:rgb|hsl)a?';
/** @type {?} */
const GRADIENTS = '(?:repeating-)?(?:linear|radial)-gradient';
/** @type {?} */
const CSS3_FNS = '(?:calc|attr)';
/** @type {?} */
const FN_ARGS = '\\([-0-9.%, #a-zA-Z]+\\)';
/** @type {?} */
const SAFE_STYLE_VALUE = new RegExp(`^(${VALUES}|` +
    `(?:${TRANSFORMATION_FNS}|${COLOR_FNS}|${GRADIENTS}|${CSS3_FNS})` +
    `${FN_ARGS})$`, 'g');
/**
 * Matches a `url(...)` value with an arbitrary argument as long as it does
 * not contain parentheses.
 *
 * The URL value still needs to be sanitized separately.
 *
 * `url(...)` values are a very common use case, e.g. for `background-image`. With carefully crafted
 * CSS style rules, it is possible to construct an information leak with `url` values in CSS, e.g.
 * by observing whether scroll bars are displayed, or character ranges used by a font face
 * definition.
 *
 * Angular only allows binding CSS values (as opposed to entire CSS rules), so it is unlikely that
 * binding a URL value without further cooperation from the page will cause an information leak, and
 * if so, it is just a leak, not a full blown XSS vulnerability.
 *
 * Given the common use case, low likelihood of attack vector, and low impact of an attack, this
 * code is permissive and allows URLs that sanitize otherwise.
 * @type {?}
 */
const URL_RE = /^url\(([^)]+)\)$/;
/**
 * Checks that quotes (" and ') are properly balanced inside a string. Assumes
 * that neither escape (\) nor any other character that could result in
 * breaking out of a string parsing context are allowed;
 * see http://www.w3.org/TR/css3-syntax/#string-token-diagram.
 *
 * This code was taken from the Closure sanitization library.
 * @param {?} value
 * @return {?}
 */
function hasBalancedQuotes(value) {
    /** @type {?} */
    let outsideSingle = true;
    /** @type {?} */
    let outsideDouble = true;
    for (let i = 0; i < value.length; i++) {
        /** @type {?} */
        const c = value.charAt(i);
        if (c === '\'' && outsideDouble) {
            outsideSingle = !outsideSingle;
        }
        else if (c === '"' && outsideSingle) {
            outsideDouble = !outsideDouble;
        }
    }
    return outsideSingle && outsideDouble;
}
/**
 * Sanitizes the given untrusted CSS style property value (i.e. not an entire object, just a single
 * value) and returns a value that is safe to use in a browser environment.
 * @param {?} value
 * @return {?}
 */
export function _sanitizeStyle(value) {
    value = String(value).trim(); // Make sure it's actually a string.
    if (!value)
        return '';
    // Single url(...) values are supported, but only for URLs that sanitize cleanly. See above for
    // reasoning behind this.
    /** @type {?} */
    const urlMatch = value.match(URL_RE);
    if ((urlMatch && _sanitizeUrl(urlMatch[1]) === urlMatch[1]) ||
        value.match(SAFE_STYLE_VALUE) && hasBalancedQuotes(value)) {
        return value; // Safe style values.
    }
    if (isDevMode()) {
        console.warn(`WARNING: sanitizing unsafe style value ${value} (see http://g.co/ng/security#xss).`);
    }
    return 'unsafe';
}
/** @enum {number} */
const StyleSanitizeMode = {
    /** Just check to see if the property is required to be sanitized or not */
    ValidateProperty: 1,
    /** Skip checking the property; just sanitize the value */
    SanitizeOnly: 2,
    /** Check the property and (if true) then sanitize the value */
    ValidateAndSanitize: 3,
};
export { StyleSanitizeMode };
/**
 * Used to intercept and sanitize style values before they are written to the renderer.
 *
 * This function is designed to be called in two modes. When a value is not provided
 * then the function will return a boolean whether a property will be sanitized later.
 * If a value is provided then the sanitized version of that will be returned.
 * @record
 */
export function StyleSanitizeFn() { }
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGVfc2FuaXRpemVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvc2FuaXRpemF0aW9uL3N0eWxlX3Nhbml0aXplci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUM5QyxPQUFPLEVBQUMsWUFBWSxFQUFDLE1BQU0saUJBQWlCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7TUFpQnZDLE1BQU0sR0FBRyx5QkFBeUI7O01BQ2xDLGtCQUFrQixHQUFHLGlFQUFpRTs7TUFDdEYsU0FBUyxHQUFHLGVBQWU7O01BQzNCLFNBQVMsR0FBRywyQ0FBMkM7O01BQ3ZELFFBQVEsR0FBRyxlQUFlOztNQUMxQixPQUFPLEdBQUcsMEJBQTBCOztNQUNwQyxnQkFBZ0IsR0FBRyxJQUFJLE1BQU0sQ0FDL0IsS0FBSyxNQUFNLEdBQUc7SUFDVixNQUFNLGtCQUFrQixJQUFJLFNBQVMsSUFBSSxTQUFTLElBQUksUUFBUSxHQUFHO0lBQ2pFLEdBQUcsT0FBTyxJQUFJLEVBQ2xCLEdBQUcsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7TUFvQkYsTUFBTSxHQUFHLGtCQUFrQjs7Ozs7Ozs7Ozs7QUFVakMsU0FBUyxpQkFBaUIsQ0FBQyxLQUFhOztRQUNsQyxhQUFhLEdBQUcsSUFBSTs7UUFDcEIsYUFBYSxHQUFHLElBQUk7SUFDeEIsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLEtBQUssQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7O2NBQy9CLENBQUMsR0FBRyxLQUFLLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQztRQUN6QixJQUFJLENBQUMsS0FBSyxJQUFJLElBQUksYUFBYSxFQUFFO1lBQy9CLGFBQWEsR0FBRyxDQUFDLGFBQWEsQ0FBQztTQUNoQzthQUFNLElBQUksQ0FBQyxLQUFLLEdBQUcsSUFBSSxhQUFhLEVBQUU7WUFDckMsYUFBYSxHQUFHLENBQUMsYUFBYSxDQUFDO1NBQ2hDO0tBQ0Y7SUFDRCxPQUFPLGFBQWEsSUFBSSxhQUFhLENBQUM7QUFDeEMsQ0FBQzs7Ozs7OztBQU1ELE1BQU0sVUFBVSxjQUFjLENBQUMsS0FBYTtJQUMxQyxLQUFLLEdBQUcsTUFBTSxDQUFDLEtBQUssQ0FBQyxDQUFDLElBQUksRUFBRSxDQUFDLENBQUUsb0NBQW9DO0lBQ25FLElBQUksQ0FBQyxLQUFLO1FBQUUsT0FBTyxFQUFFLENBQUM7Ozs7VUFJaEIsUUFBUSxHQUFHLEtBQUssQ0FBQyxLQUFLLENBQUMsTUFBTSxDQUFDO0lBQ3BDLElBQUksQ0FBQyxRQUFRLElBQUksWUFBWSxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUMsQ0FBQyxLQUFLLFFBQVEsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUN2RCxLQUFLLENBQUMsS0FBSyxDQUFDLGdCQUFnQixDQUFDLElBQUksaUJBQWlCLENBQUMsS0FBSyxDQUFDLEVBQUU7UUFDN0QsT0FBTyxLQUFLLENBQUMsQ0FBRSxxQkFBcUI7S0FDckM7SUFFRCxJQUFJLFNBQVMsRUFBRSxFQUFFO1FBQ2YsT0FBTyxDQUFDLElBQUksQ0FDUiwwQ0FBMEMsS0FBSyxxQ0FBcUMsQ0FBQyxDQUFDO0tBQzNGO0lBRUQsT0FBTyxRQUFRLENBQUM7QUFDbEIsQ0FBQzs7O0lBbUJDLDJFQUEyRTtJQUMzRSxtQkFBdUI7SUFDdkIsMERBQTBEO0lBQzFELGVBQW1CO0lBQ25CLCtEQUErRDtJQUMvRCxzQkFBMEI7Ozs7Ozs7Ozs7O0FBVTVCLHFDQUVDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2lzRGV2TW9kZX0gZnJvbSAnLi4vdXRpbC9pc19kZXZfbW9kZSc7XG5pbXBvcnQge19zYW5pdGl6ZVVybH0gZnJvbSAnLi91cmxfc2FuaXRpemVyJztcblxuXG4vKipcbiAqIFJlZ3VsYXIgZXhwcmVzc2lvbiBmb3Igc2FmZSBzdHlsZSB2YWx1ZXMuXG4gKlxuICogUXVvdGVzIChcIiBhbmQgJykgYXJlIGFsbG93ZWQsIGJ1dCBhIGNoZWNrIG11c3QgYmUgZG9uZSBlbHNld2hlcmUgdG8gZW5zdXJlIHRoZXkncmUgYmFsYW5jZWQuXG4gKlxuICogJywnIGFsbG93cyBtdWx0aXBsZSB2YWx1ZXMgdG8gYmUgYXNzaWduZWQgdG8gdGhlIHNhbWUgcHJvcGVydHkgKGUuZy4gYmFja2dyb3VuZC1hdHRhY2htZW50IG9yXG4gKiBmb250LWZhbWlseSkgYW5kIGhlbmNlIGNvdWxkIGFsbG93IG11bHRpcGxlIHZhbHVlcyB0byBnZXQgaW5qZWN0ZWQsIGJ1dCB0aGF0IHNob3VsZCBwb3NlIG5vIHJpc2tcbiAqIG9mIFhTUy5cbiAqXG4gKiBUaGUgZnVuY3Rpb24gZXhwcmVzc2lvbiBjaGVja3Mgb25seSBmb3IgWFNTIHNhZmV0eSwgbm90IGZvciBDU1MgdmFsaWRpdHkuXG4gKlxuICogVGhpcyByZWd1bGFyIGV4cHJlc3Npb24gd2FzIHRha2VuIGZyb20gdGhlIENsb3N1cmUgc2FuaXRpemF0aW9uIGxpYnJhcnksIGFuZCBhdWdtZW50ZWQgZm9yXG4gKiB0cmFuc2Zvcm1hdGlvbiB2YWx1ZXMuXG4gKi9cbmNvbnN0IFZBTFVFUyA9ICdbLSwuXCJcXCclXyEjIGEtekEtWjAtOV0rJztcbmNvbnN0IFRSQU5TRk9STUFUSU9OX0ZOUyA9ICcoPzptYXRyaXh8dHJhbnNsYXRlfHNjYWxlfHJvdGF0ZXxza2V3fHBlcnNwZWN0aXZlKSg/Olh8WXxafDNkKT8nO1xuY29uc3QgQ09MT1JfRk5TID0gJyg/OnJnYnxoc2wpYT8nO1xuY29uc3QgR1JBRElFTlRTID0gJyg/OnJlcGVhdGluZy0pPyg/OmxpbmVhcnxyYWRpYWwpLWdyYWRpZW50JztcbmNvbnN0IENTUzNfRk5TID0gJyg/OmNhbGN8YXR0ciknO1xuY29uc3QgRk5fQVJHUyA9ICdcXFxcKFstMC05LiUsICNhLXpBLVpdK1xcXFwpJztcbmNvbnN0IFNBRkVfU1RZTEVfVkFMVUUgPSBuZXcgUmVnRXhwKFxuICAgIGBeKCR7VkFMVUVTfXxgICtcbiAgICAgICAgYCg/OiR7VFJBTlNGT1JNQVRJT05fRk5TfXwke0NPTE9SX0ZOU318JHtHUkFESUVOVFN9fCR7Q1NTM19GTlN9KWAgK1xuICAgICAgICBgJHtGTl9BUkdTfSkkYCxcbiAgICAnZycpO1xuXG4vKipcbiAqIE1hdGNoZXMgYSBgdXJsKC4uLilgIHZhbHVlIHdpdGggYW4gYXJiaXRyYXJ5IGFyZ3VtZW50IGFzIGxvbmcgYXMgaXQgZG9lc1xuICogbm90IGNvbnRhaW4gcGFyZW50aGVzZXMuXG4gKlxuICogVGhlIFVSTCB2YWx1ZSBzdGlsbCBuZWVkcyB0byBiZSBzYW5pdGl6ZWQgc2VwYXJhdGVseS5cbiAqXG4gKiBgdXJsKC4uLilgIHZhbHVlcyBhcmUgYSB2ZXJ5IGNvbW1vbiB1c2UgY2FzZSwgZS5nLiBmb3IgYGJhY2tncm91bmQtaW1hZ2VgLiBXaXRoIGNhcmVmdWxseSBjcmFmdGVkXG4gKiBDU1Mgc3R5bGUgcnVsZXMsIGl0IGlzIHBvc3NpYmxlIHRvIGNvbnN0cnVjdCBhbiBpbmZvcm1hdGlvbiBsZWFrIHdpdGggYHVybGAgdmFsdWVzIGluIENTUywgZS5nLlxuICogYnkgb2JzZXJ2aW5nIHdoZXRoZXIgc2Nyb2xsIGJhcnMgYXJlIGRpc3BsYXllZCwgb3IgY2hhcmFjdGVyIHJhbmdlcyB1c2VkIGJ5IGEgZm9udCBmYWNlXG4gKiBkZWZpbml0aW9uLlxuICpcbiAqIEFuZ3VsYXIgb25seSBhbGxvd3MgYmluZGluZyBDU1MgdmFsdWVzIChhcyBvcHBvc2VkIHRvIGVudGlyZSBDU1MgcnVsZXMpLCBzbyBpdCBpcyB1bmxpa2VseSB0aGF0XG4gKiBiaW5kaW5nIGEgVVJMIHZhbHVlIHdpdGhvdXQgZnVydGhlciBjb29wZXJhdGlvbiBmcm9tIHRoZSBwYWdlIHdpbGwgY2F1c2UgYW4gaW5mb3JtYXRpb24gbGVhaywgYW5kXG4gKiBpZiBzbywgaXQgaXMganVzdCBhIGxlYWssIG5vdCBhIGZ1bGwgYmxvd24gWFNTIHZ1bG5lcmFiaWxpdHkuXG4gKlxuICogR2l2ZW4gdGhlIGNvbW1vbiB1c2UgY2FzZSwgbG93IGxpa2VsaWhvb2Qgb2YgYXR0YWNrIHZlY3RvciwgYW5kIGxvdyBpbXBhY3Qgb2YgYW4gYXR0YWNrLCB0aGlzXG4gKiBjb2RlIGlzIHBlcm1pc3NpdmUgYW5kIGFsbG93cyBVUkxzIHRoYXQgc2FuaXRpemUgb3RoZXJ3aXNlLlxuICovXG5jb25zdCBVUkxfUkUgPSAvXnVybFxcKChbXildKylcXCkkLztcblxuLyoqXG4gKiBDaGVja3MgdGhhdCBxdW90ZXMgKFwiIGFuZCAnKSBhcmUgcHJvcGVybHkgYmFsYW5jZWQgaW5zaWRlIGEgc3RyaW5nLiBBc3N1bWVzXG4gKiB0aGF0IG5laXRoZXIgZXNjYXBlIChcXCkgbm9yIGFueSBvdGhlciBjaGFyYWN0ZXIgdGhhdCBjb3VsZCByZXN1bHQgaW5cbiAqIGJyZWFraW5nIG91dCBvZiBhIHN0cmluZyBwYXJzaW5nIGNvbnRleHQgYXJlIGFsbG93ZWQ7XG4gKiBzZWUgaHR0cDovL3d3dy53My5vcmcvVFIvY3NzMy1zeW50YXgvI3N0cmluZy10b2tlbi1kaWFncmFtLlxuICpcbiAqIFRoaXMgY29kZSB3YXMgdGFrZW4gZnJvbSB0aGUgQ2xvc3VyZSBzYW5pdGl6YXRpb24gbGlicmFyeS5cbiAqL1xuZnVuY3Rpb24gaGFzQmFsYW5jZWRRdW90ZXModmFsdWU6IHN0cmluZykge1xuICBsZXQgb3V0c2lkZVNpbmdsZSA9IHRydWU7XG4gIGxldCBvdXRzaWRlRG91YmxlID0gdHJ1ZTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCB2YWx1ZS5sZW5ndGg7IGkrKykge1xuICAgIGNvbnN0IGMgPSB2YWx1ZS5jaGFyQXQoaSk7XG4gICAgaWYgKGMgPT09ICdcXCcnICYmIG91dHNpZGVEb3VibGUpIHtcbiAgICAgIG91dHNpZGVTaW5nbGUgPSAhb3V0c2lkZVNpbmdsZTtcbiAgICB9IGVsc2UgaWYgKGMgPT09ICdcIicgJiYgb3V0c2lkZVNpbmdsZSkge1xuICAgICAgb3V0c2lkZURvdWJsZSA9ICFvdXRzaWRlRG91YmxlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gb3V0c2lkZVNpbmdsZSAmJiBvdXRzaWRlRG91YmxlO1xufVxuXG4vKipcbiAqIFNhbml0aXplcyB0aGUgZ2l2ZW4gdW50cnVzdGVkIENTUyBzdHlsZSBwcm9wZXJ0eSB2YWx1ZSAoaS5lLiBub3QgYW4gZW50aXJlIG9iamVjdCwganVzdCBhIHNpbmdsZVxuICogdmFsdWUpIGFuZCByZXR1cm5zIGEgdmFsdWUgdGhhdCBpcyBzYWZlIHRvIHVzZSBpbiBhIGJyb3dzZXIgZW52aXJvbm1lbnQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBfc2FuaXRpemVTdHlsZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nIHtcbiAgdmFsdWUgPSBTdHJpbmcodmFsdWUpLnRyaW0oKTsgIC8vIE1ha2Ugc3VyZSBpdCdzIGFjdHVhbGx5IGEgc3RyaW5nLlxuICBpZiAoIXZhbHVlKSByZXR1cm4gJyc7XG5cbiAgLy8gU2luZ2xlIHVybCguLi4pIHZhbHVlcyBhcmUgc3VwcG9ydGVkLCBidXQgb25seSBmb3IgVVJMcyB0aGF0IHNhbml0aXplIGNsZWFubHkuIFNlZSBhYm92ZSBmb3JcbiAgLy8gcmVhc29uaW5nIGJlaGluZCB0aGlzLlxuICBjb25zdCB1cmxNYXRjaCA9IHZhbHVlLm1hdGNoKFVSTF9SRSk7XG4gIGlmICgodXJsTWF0Y2ggJiYgX3Nhbml0aXplVXJsKHVybE1hdGNoWzFdKSA9PT0gdXJsTWF0Y2hbMV0pIHx8XG4gICAgICB2YWx1ZS5tYXRjaChTQUZFX1NUWUxFX1ZBTFVFKSAmJiBoYXNCYWxhbmNlZFF1b3Rlcyh2YWx1ZSkpIHtcbiAgICByZXR1cm4gdmFsdWU7ICAvLyBTYWZlIHN0eWxlIHZhbHVlcy5cbiAgfVxuXG4gIGlmIChpc0Rldk1vZGUoKSkge1xuICAgIGNvbnNvbGUud2FybihcbiAgICAgICAgYFdBUk5JTkc6IHNhbml0aXppbmcgdW5zYWZlIHN0eWxlIHZhbHVlICR7dmFsdWV9IChzZWUgaHR0cDovL2cuY28vbmcvc2VjdXJpdHkjeHNzKS5gKTtcbiAgfVxuXG4gIHJldHVybiAndW5zYWZlJztcbn1cblxuXG4vKipcbiAqIEEgc2VyaWVzIG9mIGZsYWdzIHRvIGluc3RydWN0IGEgc3R5bGUgc2FuaXRpemVyIHRvIGVpdGhlciB2YWxpZGF0ZVxuICogb3Igc2FuaXRpemUgYSB2YWx1ZS5cbiAqXG4gKiBCZWNhdXNlIHNhbml0aXphdGlvbiBpcyBkZXBlbmRlbnQgb24gdGhlIHN0eWxlIHByb3BlcnR5IChpLmUuIHN0eWxlXG4gKiBzYW5pdGl6YXRpb24gZm9yIGB3aWR0aGAgaXMgbXVjaCBkaWZmZXJlbnQgdGhhbiBmb3IgYGJhY2tncm91bmQtaW1hZ2VgKVxuICogdGhlIHNhbml0aXphdGlvbiBmdW5jdGlvbiAoZS5nLiBgU3R5bGVTYW5pdGl6ZXJGbmApIG5lZWRzIHRvIGNoZWNrIGFcbiAqIHByb3BlcnR5IHZhbHVlIGZpcnN0IGJlZm9yZSBpdCBhY3R1YWxseSBzYW5pdGl6ZXMgYW55IHZhbHVlcy5cbiAqXG4gKiBUaGlzIGVudW0gZXhpc3QgdG8gYWxsb3cgYSBzdHlsZSBzYW5pdGl6YXRpb24gZnVuY3Rpb24gdG8gZWl0aGVyIG9ubHlcbiAqIGRvIHZhbGlkYXRpb24gKGNoZWNrIHRoZSBwcm9wZXJ0eSB0byBzZWUgd2hldGhlciBhIHZhbHVlIHdpbGwgYmVcbiAqIHNhbml0aXplZCBvciBub3QpIG9yIHRvIHNhbml0aXplIHRoZSB2YWx1ZSAob3IgYm90aCkuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY29uc3QgZW51bSBTdHlsZVNhbml0aXplTW9kZSB7XG4gIC8qKiBKdXN0IGNoZWNrIHRvIHNlZSBpZiB0aGUgcHJvcGVydHkgaXMgcmVxdWlyZWQgdG8gYmUgc2FuaXRpemVkIG9yIG5vdCAqL1xuICBWYWxpZGF0ZVByb3BlcnR5ID0gMGIwMSxcbiAgLyoqIFNraXAgY2hlY2tpbmcgdGhlIHByb3BlcnR5OyBqdXN0IHNhbml0aXplIHRoZSB2YWx1ZSAqL1xuICBTYW5pdGl6ZU9ubHkgPSAwYjEwLFxuICAvKiogQ2hlY2sgdGhlIHByb3BlcnR5IGFuZCAoaWYgdHJ1ZSkgdGhlbiBzYW5pdGl6ZSB0aGUgdmFsdWUgKi9cbiAgVmFsaWRhdGVBbmRTYW5pdGl6ZSA9IDBiMTEsXG59XG5cbi8qKlxuICogVXNlZCB0byBpbnRlcmNlcHQgYW5kIHNhbml0aXplIHN0eWxlIHZhbHVlcyBiZWZvcmUgdGhleSBhcmUgd3JpdHRlbiB0byB0aGUgcmVuZGVyZXIuXG4gKlxuICogVGhpcyBmdW5jdGlvbiBpcyBkZXNpZ25lZCB0byBiZSBjYWxsZWQgaW4gdHdvIG1vZGVzLiBXaGVuIGEgdmFsdWUgaXMgbm90IHByb3ZpZGVkXG4gKiB0aGVuIHRoZSBmdW5jdGlvbiB3aWxsIHJldHVybiBhIGJvb2xlYW4gd2hldGhlciBhIHByb3BlcnR5IHdpbGwgYmUgc2FuaXRpemVkIGxhdGVyLlxuICogSWYgYSB2YWx1ZSBpcyBwcm92aWRlZCB0aGVuIHRoZSBzYW5pdGl6ZWQgdmVyc2lvbiBvZiB0aGF0IHdpbGwgYmUgcmV0dXJuZWQuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgU3R5bGVTYW5pdGl6ZUZuIHtcbiAgKHByb3A6IHN0cmluZywgdmFsdWU6IHN0cmluZ3xudWxsLCBtb2RlPzogU3R5bGVTYW5pdGl6ZU1vZGUpOiBhbnk7XG59XG4iXX0=