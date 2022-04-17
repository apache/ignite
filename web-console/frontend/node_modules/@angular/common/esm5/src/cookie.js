/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
export function parseCookieValue(cookieStr, name) {
    var e_1, _a;
    name = encodeURIComponent(name);
    try {
        for (var _b = tslib_1.__values(cookieStr.split(';')), _c = _b.next(); !_c.done; _c = _b.next()) {
            var cookie = _c.value;
            var eqIndex = cookie.indexOf('=');
            var _d = tslib_1.__read(eqIndex == -1 ? [cookie, ''] : [cookie.slice(0, eqIndex), cookie.slice(eqIndex + 1)], 2), cookieName = _d[0], cookieValue = _d[1];
            if (cookieName.trim() === name) {
                return decodeURIComponent(cookieValue);
            }
        }
    }
    catch (e_1_1) { e_1 = { error: e_1_1 }; }
    finally {
        try {
            if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
        }
        finally { if (e_1) throw e_1.error; }
    }
    return null;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29va2llLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9jb29raWUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE1BQU0sVUFBVSxnQkFBZ0IsQ0FBQyxTQUFpQixFQUFFLElBQVk7O0lBQzlELElBQUksR0FBRyxrQkFBa0IsQ0FBQyxJQUFJLENBQUMsQ0FBQzs7UUFDaEMsS0FBcUIsSUFBQSxLQUFBLGlCQUFBLFNBQVMsQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLENBQUEsZ0JBQUEsNEJBQUU7WUFBdEMsSUFBTSxNQUFNLFdBQUE7WUFDZixJQUFNLE9BQU8sR0FBRyxNQUFNLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxDQUFDO1lBQzlCLElBQUEsNEdBQ2tGLEVBRGpGLGtCQUFVLEVBQUUsbUJBQ3FFLENBQUM7WUFDekYsSUFBSSxVQUFVLENBQUMsSUFBSSxFQUFFLEtBQUssSUFBSSxFQUFFO2dCQUM5QixPQUFPLGtCQUFrQixDQUFDLFdBQVcsQ0FBQyxDQUFDO2FBQ3hDO1NBQ0Y7Ozs7Ozs7OztJQUNELE9BQU8sSUFBSSxDQUFDO0FBQ2QsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlQ29va2llVmFsdWUoY29va2llU3RyOiBzdHJpbmcsIG5hbWU6IHN0cmluZyk6IHN0cmluZ3xudWxsIHtcbiAgbmFtZSA9IGVuY29kZVVSSUNvbXBvbmVudChuYW1lKTtcbiAgZm9yIChjb25zdCBjb29raWUgb2YgY29va2llU3RyLnNwbGl0KCc7JykpIHtcbiAgICBjb25zdCBlcUluZGV4ID0gY29va2llLmluZGV4T2YoJz0nKTtcbiAgICBjb25zdCBbY29va2llTmFtZSwgY29va2llVmFsdWVdOiBzdHJpbmdbXSA9XG4gICAgICAgIGVxSW5kZXggPT0gLTEgPyBbY29va2llLCAnJ10gOiBbY29va2llLnNsaWNlKDAsIGVxSW5kZXgpLCBjb29raWUuc2xpY2UoZXFJbmRleCArIDEpXTtcbiAgICBpZiAoY29va2llTmFtZS50cmltKCkgPT09IG5hbWUpIHtcbiAgICAgIHJldHVybiBkZWNvZGVVUklDb21wb25lbnQoY29va2llVmFsdWUpO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cbiJdfQ==