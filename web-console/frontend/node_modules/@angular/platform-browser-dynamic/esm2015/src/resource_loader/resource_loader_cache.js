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
import { ResourceLoader } from '@angular/compiler';
import { ɵglobal as global } from '@angular/core';
/**
 * An implementation of ResourceLoader that uses a template cache to avoid doing an actual
 * ResourceLoader.
 *
 * The template cache needs to be built and loaded into window.$templateCache
 * via a separate mechanism.
 *
 * \@publicApi
 */
export class CachedResourceLoader extends ResourceLoader {
    constructor() {
        super();
        this._cache = ((/** @type {?} */ (global))).$templateCache;
        if (this._cache == null) {
            throw new Error('CachedResourceLoader: Template cache was not found in $templateCache.');
        }
    }
    /**
     * @param {?} url
     * @return {?}
     */
    get(url) {
        if (this._cache.hasOwnProperty(url)) {
            return Promise.resolve(this._cache[url]);
        }
        else {
            return (/** @type {?} */ (Promise.reject('CachedResourceLoader: Did not find cached template for ' + url)));
        }
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    CachedResourceLoader.prototype._cache;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb3VyY2VfbG9hZGVyX2NhY2hlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci1keW5hbWljL3NyYy9yZXNvdXJjZV9sb2FkZXIvcmVzb3VyY2VfbG9hZGVyX2NhY2hlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGNBQWMsRUFBQyxNQUFNLG1CQUFtQixDQUFDO0FBQ2pELE9BQU8sRUFBQyxPQUFPLElBQUksTUFBTSxFQUFDLE1BQU0sZUFBZSxDQUFDOzs7Ozs7Ozs7O0FBV2hELE1BQU0sT0FBTyxvQkFBcUIsU0FBUSxjQUFjO0lBR3REO1FBQ0UsS0FBSyxFQUFFLENBQUM7UUFDUixJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsbUJBQUssTUFBTSxFQUFBLENBQUMsQ0FBQyxjQUFjLENBQUM7UUFDM0MsSUFBSSxJQUFJLENBQUMsTUFBTSxJQUFJLElBQUksRUFBRTtZQUN2QixNQUFNLElBQUksS0FBSyxDQUFDLHVFQUF1RSxDQUFDLENBQUM7U0FDMUY7SUFDSCxDQUFDOzs7OztJQUVELEdBQUcsQ0FBQyxHQUFXO1FBQ2IsSUFBSSxJQUFJLENBQUMsTUFBTSxDQUFDLGNBQWMsQ0FBQyxHQUFHLENBQUMsRUFBRTtZQUNuQyxPQUFPLE9BQU8sQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDO1NBQzFDO2FBQU07WUFDTCxPQUFPLG1CQUFjLE9BQU8sQ0FBQyxNQUFNLENBQy9CLHlEQUF5RCxHQUFHLEdBQUcsQ0FBQyxFQUFBLENBQUM7U0FDdEU7SUFDSCxDQUFDO0NBQ0Y7Ozs7OztJQWxCQyxzQ0FBd0MiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7UmVzb3VyY2VMb2FkZXJ9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyJztcbmltcG9ydCB7ybVnbG9iYWwgYXMgZ2xvYmFsfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuLyoqXG4gKiBBbiBpbXBsZW1lbnRhdGlvbiBvZiBSZXNvdXJjZUxvYWRlciB0aGF0IHVzZXMgYSB0ZW1wbGF0ZSBjYWNoZSB0byBhdm9pZCBkb2luZyBhbiBhY3R1YWxcbiAqIFJlc291cmNlTG9hZGVyLlxuICpcbiAqIFRoZSB0ZW1wbGF0ZSBjYWNoZSBuZWVkcyB0byBiZSBidWlsdCBhbmQgbG9hZGVkIGludG8gd2luZG93LiR0ZW1wbGF0ZUNhY2hlXG4gKiB2aWEgYSBzZXBhcmF0ZSBtZWNoYW5pc20uXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgQ2FjaGVkUmVzb3VyY2VMb2FkZXIgZXh0ZW5kcyBSZXNvdXJjZUxvYWRlciB7XG4gIHByaXZhdGUgX2NhY2hlOiB7W3VybDogc3RyaW5nXTogc3RyaW5nfTtcblxuICBjb25zdHJ1Y3RvcigpIHtcbiAgICBzdXBlcigpO1xuICAgIHRoaXMuX2NhY2hlID0gKDxhbnk+Z2xvYmFsKS4kdGVtcGxhdGVDYWNoZTtcbiAgICBpZiAodGhpcy5fY2FjaGUgPT0gbnVsbCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdDYWNoZWRSZXNvdXJjZUxvYWRlcjogVGVtcGxhdGUgY2FjaGUgd2FzIG5vdCBmb3VuZCBpbiAkdGVtcGxhdGVDYWNoZS4nKTtcbiAgICB9XG4gIH1cblxuICBnZXQodXJsOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmICh0aGlzLl9jYWNoZS5oYXNPd25Qcm9wZXJ0eSh1cmwpKSB7XG4gICAgICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKHRoaXMuX2NhY2hlW3VybF0pO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gPFByb21pc2U8YW55Pj5Qcm9taXNlLnJlamVjdChcbiAgICAgICAgICAnQ2FjaGVkUmVzb3VyY2VMb2FkZXI6IERpZCBub3QgZmluZCBjYWNoZWQgdGVtcGxhdGUgZm9yICcgKyB1cmwpO1xuICAgIH1cbiAgfVxufVxuIl19