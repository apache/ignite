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
 * @param {?} cookieStr
 * @param {?} name
 * @return {?}
 */
export function parseCookieValue(cookieStr, name) {
    name = encodeURIComponent(name);
    for (const cookie of cookieStr.split(';')) {
        /** @type {?} */
        const eqIndex = cookie.indexOf('=');
        const [cookieName, cookieValue] = eqIndex == -1 ? [cookie, ''] : [cookie.slice(0, eqIndex), cookie.slice(eqIndex + 1)];
        if (cookieName.trim() === name) {
            return decodeURIComponent(cookieValue);
        }
    }
    return null;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29va2llLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9jb29raWUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7OztBQVFBLE1BQU0sVUFBVSxnQkFBZ0IsQ0FBQyxTQUFpQixFQUFFLElBQVk7SUFDOUQsSUFBSSxHQUFHLGtCQUFrQixDQUFDLElBQUksQ0FBQyxDQUFDO0lBQ2hDLEtBQUssTUFBTSxNQUFNLElBQUksU0FBUyxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsRUFBRTs7Y0FDbkMsT0FBTyxHQUFHLE1BQU0sQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDO2NBQzdCLENBQUMsVUFBVSxFQUFFLFdBQVcsQ0FBQyxHQUMzQixPQUFPLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsTUFBTSxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxLQUFLLENBQUMsQ0FBQyxFQUFFLE9BQU8sQ0FBQyxFQUFFLE1BQU0sQ0FBQyxLQUFLLENBQUMsT0FBTyxHQUFHLENBQUMsQ0FBQyxDQUFDO1FBQ3hGLElBQUksVUFBVSxDQUFDLElBQUksRUFBRSxLQUFLLElBQUksRUFBRTtZQUM5QixPQUFPLGtCQUFrQixDQUFDLFdBQVcsQ0FBQyxDQUFDO1NBQ3hDO0tBQ0Y7SUFDRCxPQUFPLElBQUksQ0FBQztBQUNkLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUNvb2tpZVZhbHVlKGNvb2tpZVN0cjogc3RyaW5nLCBuYW1lOiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gIG5hbWUgPSBlbmNvZGVVUklDb21wb25lbnQobmFtZSk7XG4gIGZvciAoY29uc3QgY29va2llIG9mIGNvb2tpZVN0ci5zcGxpdCgnOycpKSB7XG4gICAgY29uc3QgZXFJbmRleCA9IGNvb2tpZS5pbmRleE9mKCc9Jyk7XG4gICAgY29uc3QgW2Nvb2tpZU5hbWUsIGNvb2tpZVZhbHVlXTogc3RyaW5nW10gPVxuICAgICAgICBlcUluZGV4ID09IC0xID8gW2Nvb2tpZSwgJyddIDogW2Nvb2tpZS5zbGljZSgwLCBlcUluZGV4KSwgY29va2llLnNsaWNlKGVxSW5kZXggKyAxKV07XG4gICAgaWYgKGNvb2tpZU5hbWUudHJpbSgpID09PSBuYW1lKSB7XG4gICAgICByZXR1cm4gZGVjb2RlVVJJQ29tcG9uZW50KGNvb2tpZVZhbHVlKTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG4iXX0=