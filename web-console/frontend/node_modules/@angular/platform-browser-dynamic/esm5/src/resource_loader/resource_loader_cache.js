/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { ResourceLoader } from '@angular/compiler';
import { ɵglobal as global } from '@angular/core';
/**
 * An implementation of ResourceLoader that uses a template cache to avoid doing an actual
 * ResourceLoader.
 *
 * The template cache needs to be built and loaded into window.$templateCache
 * via a separate mechanism.
 *
 * @publicApi
 */
var CachedResourceLoader = /** @class */ (function (_super) {
    tslib_1.__extends(CachedResourceLoader, _super);
    function CachedResourceLoader() {
        var _this = _super.call(this) || this;
        _this._cache = global.$templateCache;
        if (_this._cache == null) {
            throw new Error('CachedResourceLoader: Template cache was not found in $templateCache.');
        }
        return _this;
    }
    CachedResourceLoader.prototype.get = function (url) {
        if (this._cache.hasOwnProperty(url)) {
            return Promise.resolve(this._cache[url]);
        }
        else {
            return Promise.reject('CachedResourceLoader: Did not find cached template for ' + url);
        }
    };
    return CachedResourceLoader;
}(ResourceLoader));
export { CachedResourceLoader };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVzb3VyY2VfbG9hZGVyX2NhY2hlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci1keW5hbWljL3NyYy9yZXNvdXJjZV9sb2FkZXIvcmVzb3VyY2VfbG9hZGVyX2NhY2hlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsY0FBYyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDakQsT0FBTyxFQUFDLE9BQU8sSUFBSSxNQUFNLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFaEQ7Ozs7Ozs7O0dBUUc7QUFDSDtJQUEwQyxnREFBYztJQUd0RDtRQUFBLFlBQ0UsaUJBQU8sU0FLUjtRQUpDLEtBQUksQ0FBQyxNQUFNLEdBQVMsTUFBTyxDQUFDLGNBQWMsQ0FBQztRQUMzQyxJQUFJLEtBQUksQ0FBQyxNQUFNLElBQUksSUFBSSxFQUFFO1lBQ3ZCLE1BQU0sSUFBSSxLQUFLLENBQUMsdUVBQXVFLENBQUMsQ0FBQztTQUMxRjs7SUFDSCxDQUFDO0lBRUQsa0NBQUcsR0FBSCxVQUFJLEdBQVc7UUFDYixJQUFJLElBQUksQ0FBQyxNQUFNLENBQUMsY0FBYyxDQUFDLEdBQUcsQ0FBQyxFQUFFO1lBQ25DLE9BQU8sT0FBTyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUM7U0FDMUM7YUFBTTtZQUNMLE9BQXFCLE9BQU8sQ0FBQyxNQUFNLENBQy9CLHlEQUF5RCxHQUFHLEdBQUcsQ0FBQyxDQUFDO1NBQ3RFO0lBQ0gsQ0FBQztJQUNILDJCQUFDO0FBQUQsQ0FBQyxBQW5CRCxDQUEwQyxjQUFjLEdBbUJ2RCIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtSZXNvdXJjZUxvYWRlcn0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuaW1wb3J0IHvJtWdsb2JhbCBhcyBnbG9iYWx9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG4vKipcbiAqIEFuIGltcGxlbWVudGF0aW9uIG9mIFJlc291cmNlTG9hZGVyIHRoYXQgdXNlcyBhIHRlbXBsYXRlIGNhY2hlIHRvIGF2b2lkIGRvaW5nIGFuIGFjdHVhbFxuICogUmVzb3VyY2VMb2FkZXIuXG4gKlxuICogVGhlIHRlbXBsYXRlIGNhY2hlIG5lZWRzIHRvIGJlIGJ1aWx0IGFuZCBsb2FkZWQgaW50byB3aW5kb3cuJHRlbXBsYXRlQ2FjaGVcbiAqIHZpYSBhIHNlcGFyYXRlIG1lY2hhbmlzbS5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjbGFzcyBDYWNoZWRSZXNvdXJjZUxvYWRlciBleHRlbmRzIFJlc291cmNlTG9hZGVyIHtcbiAgcHJpdmF0ZSBfY2FjaGU6IHtbdXJsOiBzdHJpbmddOiBzdHJpbmd9O1xuXG4gIGNvbnN0cnVjdG9yKCkge1xuICAgIHN1cGVyKCk7XG4gICAgdGhpcy5fY2FjaGUgPSAoPGFueT5nbG9iYWwpLiR0ZW1wbGF0ZUNhY2hlO1xuICAgIGlmICh0aGlzLl9jYWNoZSA9PSBudWxsKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0NhY2hlZFJlc291cmNlTG9hZGVyOiBUZW1wbGF0ZSBjYWNoZSB3YXMgbm90IGZvdW5kIGluICR0ZW1wbGF0ZUNhY2hlLicpO1xuICAgIH1cbiAgfVxuXG4gIGdldCh1cmw6IHN0cmluZyk6IFByb21pc2U8c3RyaW5nPiB7XG4gICAgaWYgKHRoaXMuX2NhY2hlLmhhc093blByb3BlcnR5KHVybCkpIHtcbiAgICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUodGhpcy5fY2FjaGVbdXJsXSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiA8UHJvbWlzZTxhbnk+PlByb21pc2UucmVqZWN0KFxuICAgICAgICAgICdDYWNoZWRSZXNvdXJjZUxvYWRlcjogRGlkIG5vdCBmaW5kIGNhY2hlZCB0ZW1wbGF0ZSBmb3IgJyArIHVybCk7XG4gICAgfVxuICB9XG59XG4iXX0=