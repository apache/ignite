/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { isDevMode } from '../util/is_dev_mode';
/**
 * A pattern that recognizes a commonly useful subset of URLs that are safe.
 *
 * This regular expression matches a subset of URLs that will not cause script
 * execution if used in URL context within a HTML document. Specifically, this
 * regular expression matches if (comment from here on and regex copied from
 * Soy's EscapingConventions):
 * (1) Either an allowed protocol (http, https, mailto or ftp).
 * (2) or no protocol.  A protocol must be followed by a colon. The below
 *     allows that by allowing colons only after one of the characters [/?#].
 *     A colon after a hash (#) must be in the fragment.
 *     Otherwise, a colon after a (?) must be in a query.
 *     Otherwise, a colon after a single solidus (/) must be in a path.
 *     Otherwise, a colon after a double solidus (//) must be in the authority
 *     (before port).
 *
 * The pattern disallows &, used in HTML entity declarations before
 * one of the characters in [/?#]. This disallows HTML entities used in the
 * protocol name, which should never happen, e.g. "h&#116;tp" for "http".
 * It also disallows HTML entities in the first path part of a relative path,
 * e.g. "foo&lt;bar/baz".  Our existing escaping functions should not produce
 * that. More importantly, it disallows masking of a colon,
 * e.g. "javascript&#58;...".
 *
 * This regular expression was taken from the Closure sanitization library.
 */
var SAFE_URL_PATTERN = /^(?:(?:https?|mailto|ftp|tel|file):|[^&:/?#]*(?:[/?#]|$))/gi;
/* A pattern that matches safe srcset values */
var SAFE_SRCSET_PATTERN = /^(?:(?:https?|file):|[^&:/?#]*(?:[/?#]|$))/gi;
/** A pattern that matches safe data URLs. Only matches image, video and audio types. */
var DATA_URL_PATTERN = /^data:(?:image\/(?:bmp|gif|jpeg|jpg|png|tiff|webp)|video\/(?:mpeg|mp4|ogg|webm)|audio\/(?:mp3|oga|ogg|opus));base64,[a-z0-9+\/]+=*$/i;
export function _sanitizeUrl(url) {
    url = String(url);
    if (url.match(SAFE_URL_PATTERN) || url.match(DATA_URL_PATTERN))
        return url;
    if (isDevMode()) {
        console.warn("WARNING: sanitizing unsafe URL value " + url + " (see http://g.co/ng/security#xss)");
    }
    return 'unsafe:' + url;
}
export function sanitizeSrcset(srcset) {
    srcset = String(srcset);
    return srcset.split(',').map(function (srcset) { return _sanitizeUrl(srcset.trim()); }).join(', ');
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXJsX3Nhbml0aXplci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3Nhbml0aXphdGlvbi91cmxfc2FuaXRpemVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUU5Qzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztHQXlCRztBQUNILElBQU0sZ0JBQWdCLEdBQUcsNkRBQTZELENBQUM7QUFFdkYsK0NBQStDO0FBQy9DLElBQU0sbUJBQW1CLEdBQUcsOENBQThDLENBQUM7QUFFM0Usd0ZBQXdGO0FBQ3hGLElBQU0sZ0JBQWdCLEdBQ2xCLHNJQUFzSSxDQUFDO0FBRTNJLE1BQU0sVUFBVSxZQUFZLENBQUMsR0FBVztJQUN0QyxHQUFHLEdBQUcsTUFBTSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0lBQ2xCLElBQUksR0FBRyxDQUFDLEtBQUssQ0FBQyxnQkFBZ0IsQ0FBQyxJQUFJLEdBQUcsQ0FBQyxLQUFLLENBQUMsZ0JBQWdCLENBQUM7UUFBRSxPQUFPLEdBQUcsQ0FBQztJQUUzRSxJQUFJLFNBQVMsRUFBRSxFQUFFO1FBQ2YsT0FBTyxDQUFDLElBQUksQ0FBQywwQ0FBd0MsR0FBRyx1Q0FBb0MsQ0FBQyxDQUFDO0tBQy9GO0lBRUQsT0FBTyxTQUFTLEdBQUcsR0FBRyxDQUFDO0FBQ3pCLENBQUM7QUFFRCxNQUFNLFVBQVUsY0FBYyxDQUFDLE1BQWM7SUFDM0MsTUFBTSxHQUFHLE1BQU0sQ0FBQyxNQUFNLENBQUMsQ0FBQztJQUN4QixPQUFPLE1BQU0sQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLENBQUMsR0FBRyxDQUFDLFVBQUMsTUFBTSxJQUFLLE9BQUEsWUFBWSxDQUFDLE1BQU0sQ0FBQyxJQUFJLEVBQUUsQ0FBQyxFQUEzQixDQUEyQixDQUFDLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDO0FBQ25GLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7aXNEZXZNb2RlfSBmcm9tICcuLi91dGlsL2lzX2Rldl9tb2RlJztcblxuLyoqXG4gKiBBIHBhdHRlcm4gdGhhdCByZWNvZ25pemVzIGEgY29tbW9ubHkgdXNlZnVsIHN1YnNldCBvZiBVUkxzIHRoYXQgYXJlIHNhZmUuXG4gKlxuICogVGhpcyByZWd1bGFyIGV4cHJlc3Npb24gbWF0Y2hlcyBhIHN1YnNldCBvZiBVUkxzIHRoYXQgd2lsbCBub3QgY2F1c2Ugc2NyaXB0XG4gKiBleGVjdXRpb24gaWYgdXNlZCBpbiBVUkwgY29udGV4dCB3aXRoaW4gYSBIVE1MIGRvY3VtZW50LiBTcGVjaWZpY2FsbHksIHRoaXNcbiAqIHJlZ3VsYXIgZXhwcmVzc2lvbiBtYXRjaGVzIGlmIChjb21tZW50IGZyb20gaGVyZSBvbiBhbmQgcmVnZXggY29waWVkIGZyb21cbiAqIFNveSdzIEVzY2FwaW5nQ29udmVudGlvbnMpOlxuICogKDEpIEVpdGhlciBhbiBhbGxvd2VkIHByb3RvY29sIChodHRwLCBodHRwcywgbWFpbHRvIG9yIGZ0cCkuXG4gKiAoMikgb3Igbm8gcHJvdG9jb2wuICBBIHByb3RvY29sIG11c3QgYmUgZm9sbG93ZWQgYnkgYSBjb2xvbi4gVGhlIGJlbG93XG4gKiAgICAgYWxsb3dzIHRoYXQgYnkgYWxsb3dpbmcgY29sb25zIG9ubHkgYWZ0ZXIgb25lIG9mIHRoZSBjaGFyYWN0ZXJzIFsvPyNdLlxuICogICAgIEEgY29sb24gYWZ0ZXIgYSBoYXNoICgjKSBtdXN0IGJlIGluIHRoZSBmcmFnbWVudC5cbiAqICAgICBPdGhlcndpc2UsIGEgY29sb24gYWZ0ZXIgYSAoPykgbXVzdCBiZSBpbiBhIHF1ZXJ5LlxuICogICAgIE90aGVyd2lzZSwgYSBjb2xvbiBhZnRlciBhIHNpbmdsZSBzb2xpZHVzICgvKSBtdXN0IGJlIGluIGEgcGF0aC5cbiAqICAgICBPdGhlcndpc2UsIGEgY29sb24gYWZ0ZXIgYSBkb3VibGUgc29saWR1cyAoLy8pIG11c3QgYmUgaW4gdGhlIGF1dGhvcml0eVxuICogICAgIChiZWZvcmUgcG9ydCkuXG4gKlxuICogVGhlIHBhdHRlcm4gZGlzYWxsb3dzICYsIHVzZWQgaW4gSFRNTCBlbnRpdHkgZGVjbGFyYXRpb25zIGJlZm9yZVxuICogb25lIG9mIHRoZSBjaGFyYWN0ZXJzIGluIFsvPyNdLiBUaGlzIGRpc2FsbG93cyBIVE1MIGVudGl0aWVzIHVzZWQgaW4gdGhlXG4gKiBwcm90b2NvbCBuYW1lLCB3aGljaCBzaG91bGQgbmV2ZXIgaGFwcGVuLCBlLmcuIFwiaCYjMTE2O3RwXCIgZm9yIFwiaHR0cFwiLlxuICogSXQgYWxzbyBkaXNhbGxvd3MgSFRNTCBlbnRpdGllcyBpbiB0aGUgZmlyc3QgcGF0aCBwYXJ0IG9mIGEgcmVsYXRpdmUgcGF0aCxcbiAqIGUuZy4gXCJmb28mbHQ7YmFyL2JhelwiLiAgT3VyIGV4aXN0aW5nIGVzY2FwaW5nIGZ1bmN0aW9ucyBzaG91bGQgbm90IHByb2R1Y2VcbiAqIHRoYXQuIE1vcmUgaW1wb3J0YW50bHksIGl0IGRpc2FsbG93cyBtYXNraW5nIG9mIGEgY29sb24sXG4gKiBlLmcuIFwiamF2YXNjcmlwdCYjNTg7Li4uXCIuXG4gKlxuICogVGhpcyByZWd1bGFyIGV4cHJlc3Npb24gd2FzIHRha2VuIGZyb20gdGhlIENsb3N1cmUgc2FuaXRpemF0aW9uIGxpYnJhcnkuXG4gKi9cbmNvbnN0IFNBRkVfVVJMX1BBVFRFUk4gPSAvXig/Oig/Omh0dHBzP3xtYWlsdG98ZnRwfHRlbHxmaWxlKTp8W14mOi8/I10qKD86Wy8/I118JCkpL2dpO1xuXG4vKiBBIHBhdHRlcm4gdGhhdCBtYXRjaGVzIHNhZmUgc3Jjc2V0IHZhbHVlcyAqL1xuY29uc3QgU0FGRV9TUkNTRVRfUEFUVEVSTiA9IC9eKD86KD86aHR0cHM/fGZpbGUpOnxbXiY6Lz8jXSooPzpbLz8jXXwkKSkvZ2k7XG5cbi8qKiBBIHBhdHRlcm4gdGhhdCBtYXRjaGVzIHNhZmUgZGF0YSBVUkxzLiBPbmx5IG1hdGNoZXMgaW1hZ2UsIHZpZGVvIGFuZCBhdWRpbyB0eXBlcy4gKi9cbmNvbnN0IERBVEFfVVJMX1BBVFRFUk4gPVxuICAgIC9eZGF0YTooPzppbWFnZVxcLyg/OmJtcHxnaWZ8anBlZ3xqcGd8cG5nfHRpZmZ8d2VicCl8dmlkZW9cXC8oPzptcGVnfG1wNHxvZ2d8d2VibSl8YXVkaW9cXC8oPzptcDN8b2dhfG9nZ3xvcHVzKSk7YmFzZTY0LFthLXowLTkrXFwvXSs9KiQvaTtcblxuZXhwb3J0IGZ1bmN0aW9uIF9zYW5pdGl6ZVVybCh1cmw6IHN0cmluZyk6IHN0cmluZyB7XG4gIHVybCA9IFN0cmluZyh1cmwpO1xuICBpZiAodXJsLm1hdGNoKFNBRkVfVVJMX1BBVFRFUk4pIHx8IHVybC5tYXRjaChEQVRBX1VSTF9QQVRURVJOKSkgcmV0dXJuIHVybDtcblxuICBpZiAoaXNEZXZNb2RlKCkpIHtcbiAgICBjb25zb2xlLndhcm4oYFdBUk5JTkc6IHNhbml0aXppbmcgdW5zYWZlIFVSTCB2YWx1ZSAke3VybH0gKHNlZSBodHRwOi8vZy5jby9uZy9zZWN1cml0eSN4c3MpYCk7XG4gIH1cblxuICByZXR1cm4gJ3Vuc2FmZTonICsgdXJsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2FuaXRpemVTcmNzZXQoc3Jjc2V0OiBzdHJpbmcpOiBzdHJpbmcge1xuICBzcmNzZXQgPSBTdHJpbmcoc3Jjc2V0KTtcbiAgcmV0dXJuIHNyY3NldC5zcGxpdCgnLCcpLm1hcCgoc3Jjc2V0KSA9PiBfc2FuaXRpemVVcmwoc3Jjc2V0LnRyaW0oKSkpLmpvaW4oJywgJyk7XG59XG4iXX0=