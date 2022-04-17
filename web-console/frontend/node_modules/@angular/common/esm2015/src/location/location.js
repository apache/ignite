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
import { EventEmitter, Injectable } from '@angular/core';
import { LocationStrategy } from './location_strategy';
import { PlatformLocation } from './platform_location';
/**
 * \@publicApi
 * @record
 */
export function PopStateEvent() { }
if (false) {
    /** @type {?|undefined} */
    PopStateEvent.prototype.pop;
    /** @type {?|undefined} */
    PopStateEvent.prototype.state;
    /** @type {?|undefined} */
    PopStateEvent.prototype.type;
    /** @type {?|undefined} */
    PopStateEvent.prototype.url;
}
/**
 * \@description
 *
 * A service that applications can use to interact with a browser's URL.
 *
 * Depending on the `LocationStrategy` used, `Location` persists
 * to the URL's path or the URL's hash segment.
 *
 * \@usageNotes
 *
 * It's better to use the `Router#navigate` service to trigger route changes. Use
 * `Location` only if you need to interact with or create normalized URLs outside of
 * routing.
 *
 * `Location` is responsible for normalizing the URL against the application's base href.
 * A normalized URL is absolute from the URL host, includes the application's base href, and has no
 * trailing slash:
 * - `/my/app/user/123` is normalized
 * - `my/app/user/123` **is not** normalized
 * - `/my/app/user/123/` **is not** normalized
 *
 * ### Example
 *
 * <code-example path='common/location/ts/path_location_component.ts'
 * region='LocationComponent'></code-example>
 *
 * \@publicApi
 */
export class Location {
    /**
     * @param {?} platformStrategy
     * @param {?} platformLocation
     */
    constructor(platformStrategy, platformLocation) {
        /**
         * \@internal
         */
        this._subject = new EventEmitter();
        /**
         * \@internal
         */
        this._urlChangeListeners = [];
        this._platformStrategy = platformStrategy;
        /** @type {?} */
        const browserBaseHref = this._platformStrategy.getBaseHref();
        this._platformLocation = platformLocation;
        this._baseHref = Location.stripTrailingSlash(_stripIndexHtml(browserBaseHref));
        this._platformStrategy.onPopState((/**
         * @param {?} ev
         * @return {?}
         */
        (ev) => {
            this._subject.emit({
                'url': this.path(true),
                'pop': true,
                'state': ev.state,
                'type': ev.type,
            });
        }));
    }
    /**
     * Normalizes the URL path for this location.
     *
     * @param {?=} includeHash True to include an anchor fragment in the path.
     *
     * @return {?} The normalized URL path.
     */
    // TODO: vsavkin. Remove the boolean flag and always include hash once the deprecated router is
    // removed.
    path(includeHash = false) {
        return this.normalize(this._platformStrategy.path(includeHash));
    }
    /**
     * Reports the current state of the location history.
     * @return {?} The current value of the `history.state` object.
     */
    getState() { return this._platformLocation.getState(); }
    /**
     * Normalizes the given path and compares to the current normalized path.
     *
     * @param {?} path The given URL path.
     * @param {?=} query Query parameters.
     *
     * @return {?} True if the given URL path is equal to the current normalized path, false
     * otherwise.
     */
    isCurrentPathEqualTo(path, query = '') {
        return this.path() == this.normalize(path + Location.normalizeQueryParams(query));
    }
    /**
     * Normalizes a URL path by stripping any trailing slashes.
     *
     * @param {?} url String representing a URL.
     *
     * @return {?} The normalized URL string.
     */
    normalize(url) {
        return Location.stripTrailingSlash(_stripBaseHref(this._baseHref, _stripIndexHtml(url)));
    }
    /**
     * Normalizes an external URL path.
     * If the given URL doesn't begin with a leading slash (`'/'`), adds one
     * before normalizing. Adds a hash if `HashLocationStrategy` is
     * in use, or the `APP_BASE_HREF` if the `PathLocationStrategy` is in use.
     *
     * @param {?} url String representing a URL.
     *
     * @return {?} A normalized platform-specific URL.
     */
    prepareExternalUrl(url) {
        if (url && url[0] !== '/') {
            url = '/' + url;
        }
        return this._platformStrategy.prepareExternalUrl(url);
    }
    // TODO: rename this method to pushState
    /**
     * Changes the browser's URL to a normalized version of a given URL, and pushes a
     * new item onto the platform's history.
     *
     * @param {?} path  URL path to normalize.
     * @param {?=} query Query parameters.
     * @param {?=} state Location history state.
     *
     * @return {?}
     */
    go(path, query = '', state = null) {
        this._platformStrategy.pushState(state, '', path, query);
        this._notifyUrlChangeListeners(this.prepareExternalUrl(path + Location.normalizeQueryParams(query)), state);
    }
    /**
     * Changes the browser's URL to a normalized version of the given URL, and replaces
     * the top item on the platform's history stack.
     *
     * @param {?} path  URL path to normalize.
     * @param {?=} query Query parameters.
     * @param {?=} state Location history state.
     * @return {?}
     */
    replaceState(path, query = '', state = null) {
        this._platformStrategy.replaceState(state, '', path, query);
        this._notifyUrlChangeListeners(this.prepareExternalUrl(path + Location.normalizeQueryParams(query)), state);
    }
    /**
     * Navigates forward in the platform's history.
     * @return {?}
     */
    forward() { this._platformStrategy.forward(); }
    /**
     * Navigates back in the platform's history.
     * @return {?}
     */
    back() { this._platformStrategy.back(); }
    /**
     * Registers a URL change listener. Use to catch updates performed by the Angular
     * framework that are not detectible through "popstate" or "hashchange" events.
     *
     * @param {?} fn The change handler function, which take a URL and a location history state.
     * @return {?}
     */
    onUrlChange(fn) {
        this._urlChangeListeners.push(fn);
        this.subscribe((/**
         * @param {?} v
         * @return {?}
         */
        v => { this._notifyUrlChangeListeners(v.url, v.state); }));
    }
    /**
     * \@internal
     * @param {?=} url
     * @param {?=} state
     * @return {?}
     */
    _notifyUrlChangeListeners(url = '', state) {
        this._urlChangeListeners.forEach((/**
         * @param {?} fn
         * @return {?}
         */
        fn => fn(url, state)));
    }
    /**
     * Subscribes to the platform's `popState` events.
     *
     * @param {?} onNext
     * @param {?=} onThrow
     * @param {?=} onReturn
     * @return {?} Subscribed events.
     */
    subscribe(onNext, onThrow, onReturn) {
        return this._subject.subscribe({ next: onNext, error: onThrow, complete: onReturn });
    }
    /**
     * Normalizes URL parameters by prepending with `?` if needed.
     *
     * @param {?} params String of URL parameters.
     *
     * @return {?} The normalized URL parameters string.
     */
    static normalizeQueryParams(params) {
        return params && params[0] !== '?' ? '?' + params : params;
    }
    /**
     * Joins two parts of a URL with a slash if needed.
     *
     * @param {?} start  URL string
     * @param {?} end    URL string
     *
     *
     * @return {?} The joined URL string.
     */
    static joinWithSlash(start, end) {
        if (start.length == 0) {
            return end;
        }
        if (end.length == 0) {
            return start;
        }
        /** @type {?} */
        let slashes = 0;
        if (start.endsWith('/')) {
            slashes++;
        }
        if (end.startsWith('/')) {
            slashes++;
        }
        if (slashes == 2) {
            return start + end.substring(1);
        }
        if (slashes == 1) {
            return start + end;
        }
        return start + '/' + end;
    }
    /**
     * Removes a trailing slash from a URL string if needed.
     * Looks for the first occurrence of either `#`, `?`, or the end of the
     * line as `/` characters and removes the trailing slash if one exists.
     *
     * @param {?} url URL string.
     *
     * @return {?} The URL string, modified if needed.
     */
    static stripTrailingSlash(url) {
        /** @type {?} */
        const match = url.match(/#|\?|$/);
        /** @type {?} */
        const pathEndIdx = match && match.index || url.length;
        /** @type {?} */
        const droppedSlashIdx = pathEndIdx - (url[pathEndIdx - 1] === '/' ? 1 : 0);
        return url.slice(0, droppedSlashIdx) + url.slice(pathEndIdx);
    }
}
Location.decorators = [
    { type: Injectable }
];
/** @nocollapse */
Location.ctorParameters = () => [
    { type: LocationStrategy },
    { type: PlatformLocation }
];
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    Location.prototype._subject;
    /**
     * \@internal
     * @type {?}
     */
    Location.prototype._baseHref;
    /**
     * \@internal
     * @type {?}
     */
    Location.prototype._platformStrategy;
    /**
     * \@internal
     * @type {?}
     */
    Location.prototype._platformLocation;
    /**
     * \@internal
     * @type {?}
     */
    Location.prototype._urlChangeListeners;
}
/**
 * @param {?} baseHref
 * @param {?} url
 * @return {?}
 */
function _stripBaseHref(baseHref, url) {
    return baseHref && url.startsWith(baseHref) ? url.substring(baseHref.length) : url;
}
/**
 * @param {?} url
 * @return {?}
 */
function _stripIndexHtml(url) {
    return url.replace(/\/index.html$/, '');
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9jYXRpb24uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vc3JjL2xvY2F0aW9uL2xvY2F0aW9uLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLFlBQVksRUFBRSxVQUFVLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFHdkQsT0FBTyxFQUFDLGdCQUFnQixFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFDckQsT0FBTyxFQUFDLGdCQUFnQixFQUFDLE1BQU0scUJBQXFCLENBQUM7Ozs7O0FBR3JELG1DQUtDOzs7SUFKQyw0QkFBYzs7SUFDZCw4QkFBWTs7SUFDWiw2QkFBYzs7SUFDZCw0QkFBYTs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBZ0NmLE1BQU0sT0FBTyxRQUFROzs7OztJQVluQixZQUFZLGdCQUFrQyxFQUFFLGdCQUFrQzs7OztRQVZsRixhQUFRLEdBQXNCLElBQUksWUFBWSxFQUFFLENBQUM7Ozs7UUFRakQsd0JBQW1CLEdBQThDLEVBQUUsQ0FBQztRQUdsRSxJQUFJLENBQUMsaUJBQWlCLEdBQUcsZ0JBQWdCLENBQUM7O2NBQ3BDLGVBQWUsR0FBRyxJQUFJLENBQUMsaUJBQWlCLENBQUMsV0FBVyxFQUFFO1FBQzVELElBQUksQ0FBQyxpQkFBaUIsR0FBRyxnQkFBZ0IsQ0FBQztRQUMxQyxJQUFJLENBQUMsU0FBUyxHQUFHLFFBQVEsQ0FBQyxrQkFBa0IsQ0FBQyxlQUFlLENBQUMsZUFBZSxDQUFDLENBQUMsQ0FBQztRQUMvRSxJQUFJLENBQUMsaUJBQWlCLENBQUMsVUFBVTs7OztRQUFDLENBQUMsRUFBRSxFQUFFLEVBQUU7WUFDdkMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUM7Z0JBQ2pCLEtBQUssRUFBRSxJQUFJLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQztnQkFDdEIsS0FBSyxFQUFFLElBQUk7Z0JBQ1gs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Yy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RXZlbnRFbWl0dGVyLCBJbmplY3RhYmxlfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7U3Vic2NyaXB0aW9uTGlrZX0gZnJvbSAncnhqcyc7XG5cbmltcG9ydCB7TG9jYXRpb25TdHJhdGVneX0gZnJvbSAnLi9sb2NhdGlvbl9zdHJhdGVneSc7XG5pbXBvcnQge1BsYXRmb3JtTG9jYXRpb259IGZyb20gJy4vcGxhdGZvcm1fbG9jYXRpb24nO1xuXG4vKiogQHB1YmxpY0FwaSAqL1xuZXhwb3J0IGludGVyZmFjZSBQb3BTdGF0ZUV2ZW50IHtcbiAgcG9wPzogYm9vbGVhbjtcbiAgc3RhdGU/OiBhbnk7XG4gIHR5cGU/OiBzdHJpbmc7XG4gIHVybD86IHN0cmluZztcbn1cblxuLyoqXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBBIHNlcnZpY2UgdGhhdCBhcHBsaWNhdGlvbnMgY2FuIHVzZSB0byBpbnRlcmFjdCB3aXRoIGEgYnJvd3NlcidzIFVSTC5cbiAqXG4gKiBEZXBlbmRpbmcgb24gdGhlIGBMb2NhdGlvblN0cmF0ZWd5YCB1c2VkLCBgTG9jYXRpb25gIHBlcnNpc3RzXG4gKiB0byB0aGUgVVJMJ3MgcGF0aCBvciB0aGUgVVJMJ3MgaGFzaCBzZWdtZW50LlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKlxuICogSXQncyBiZXR0ZXIgdG8gdXNlIHRoZSBgUm91dGVyI25hdmlnYXRlYCBzZXJ2aWNlIHRvIHRyaWdnZXIgcm91dGUgY2hhbmdlcy4gVXNlXG4gKiBgTG9jYXRpb25gIG9ubHkgaWYgeW91IG5lZWQgdG8gaW50ZXJhY3Qgd2l0aCBvciBjcmVhdGUgbm9ybWFsaXplZCBVUkxzIG91dHNpZGUgb2ZcbiAqIHJvdXRpbmcuXG4gKlxuICogYExvY2F0aW9uYCBpcyByZXNwb25zaWJsZSBmb3Igbm9ybWFsaXppbmcgdGhlIFVSTCBhZ2FpbnN0IHRoZSBhcHBsaWNhdGlvbidzIGJhc2UgaHJlZi5cbiAqIEEgbm9ybWFsaXplZCBVUkwgaXMgYWJzb2x1dGUgZnJvbSB0aGUgVVJMIGhvc3QsIGluY2x1ZGVzIHRoZSBhcHBsaWNhdGlvbidzIGJhc2UgaHJlZiwgYW5kIGhhcyBub1xuICogdHJhaWxpbmcgc2xhc2g6XG4gKiAtIGAvbXkvYXBwL3VzZXIvMTIzYCBpcyBub3JtYWxpemVkXG4gKiAtIGBteS9hcHAvdXNlci8xMjNgICoqaXMgbm90Kiogbm9ybWFsaXplZFxuICogLSBgL215L2FwcC91c2VyLzEyMy9gICoqaXMgbm90Kiogbm9ybWFsaXplZFxuICpcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogPGNvZGUtZXhhbXBsZSBwYXRoPSdjb21tb24vbG9jYXRpb24vdHMvcGF0aF9sb2NhdGlvbl9jb21wb25lbnQudHMnXG4gKiByZWdpb249J0xvY2F0aW9uQ29tcG9uZW50Jz48L2NvZGUtZXhhbXBsZT5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBMb2NhdGlvbiB7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3N1YmplY3Q6IEV2ZW50RW1pdHRlcjxhbnk+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuICAvKiogQGludGVybmFsICovXG4gIF9iYXNlSHJlZjogc3RyaW5nO1xuICAvKiogQGludGVybmFsICovXG4gIF9wbGF0Zm9ybVN0cmF0ZWd5OiBMb2NhdGlvblN0cmF0ZWd5O1xuICAvKiogQGludGVybmFsICovXG4gIF9wbGF0Zm9ybUxvY2F0aW9uOiBQbGF0Zm9ybUxvY2F0aW9uO1xuICAvKiogQGludGVybmFsICovXG4gIF91cmxDaGFuZ2VMaXN0ZW5lcnM6ICgodXJsOiBzdHJpbmcsIHN0YXRlOiB1bmtub3duKSA9PiB2b2lkKVtdID0gW107XG5cbiAgY29uc3RydWN0b3IocGxhdGZvcm1TdHJhdGVneTogTG9jYXRpb25TdHJhdGVneSwgcGxhdGZvcm1Mb2NhdGlvbjogUGxhdGZvcm1Mb2NhdGlvbikge1xuICAgIHRoaXMuX3BsYXRmb3JtU3RyYXRlZ3kgPSBwbGF0Zm9ybVN0cmF0ZWd5O1xuICAgIGNvbnN0IGJyb3dzZXJCYXNlSHJlZiA9IHRoaXMuX3BsYXRmb3JtU3RyYXRlZ3kuZ2V0QmFzZUhyZWYoKTtcbiAgICB0aGlzLl9wbGF0Zm9ybUxvY2F0aW9uID0gcGxhdGZvcm1Mb2NhdGlvbjtcbiAgICB0aGlzLl9iYXNlSHJlZiA9IExvY2F0aW9uLnN0cmlwVHJhaWxpbmdTbGFzaChfc3RyaXBJbmRleEh0bWwoYnJvd3NlckJhc2VIcmVmKSk7XG4gICAgdGhpcy5fcGxhdGZvcm1TdHJhdGVneS5vblBvcFN0YXRlKChldikgPT4ge1xuICAgICAgdGhpcy5fc3ViamVjdC5lbWl0KHtcbiAgICAgICAgJ3VybCc6IHRoaXMucGF0aCh0cnVlKSxcbiAgICAgICAgJ3BvcCc6IHRydWUsXG4gICAgICAgICdzdGF0ZSc6IGV2LnN0YXRlLFxuICAgICAgICAndHlwZSc6IGV2LnR5cGUsXG4gICAgICB9KTtcbiAgICB9KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBOb3JtYWxpemVzIHRoZSBVUkwgcGF0aCBmb3IgdGhpcyBsb2NhdGlvbi5cbiAgICpcbiAgICogQHBhcmFtIGluY2x1ZGVIYXNoIFRydWUgdG8gaW5jbHVkZSBhbiBhbmNob3IgZnJhZ21lbnQgaW4gdGhlIHBhdGguXG4gICAqXG4gICAqIEByZXR1cm5zIFRoZSBub3JtYWxpemVkIFVSTCBwYXRoLlxuICAgKi9cbiAgLy8gVE9ETzogdnNhdmtpbi4gUmVtb3ZlIHRoZSBib29sZWFuIGZsYWcgYW5kIGFsd2F5cyBpbmNsdWRlIGhhc2ggb25jZSB0aGUgZGVwcmVjYXRlZCByb3V0ZXIgaXNcbiAgLy8gcmVtb3ZlZC5cbiAgcGF0aChpbmNsdWRlSGFzaDogYm9vbGVhbiA9IGZhbHNlKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5ub3JtYWxpemUodGhpcy5fcGxhdGZvcm1TdHJhdGVneS5wYXRoKGluY2x1ZGVIYXNoKSk7XG4gIH1cblxuICAvKipcbiAgICogUmVwb3J0cyB0aGUgY3VycmVudCBzdGF0ZSBvZiB0aGUgbG9jYXRpb24gaGlzdG9yeS5cbiAgICogQHJldHVybnMgVGhlIGN1cnJlbnQgdmFsdWUgb2YgdGhlIGBoaXN0b3J5LnN0YXRlYCBvYmplY3QuXG4gICAqL1xuICBnZXRTdGF0ZSgpOiB1bmtub3duIHsgcmV0dXJuIHRoaXMuX3BsYXRmb3JtTG9jYXRpb24uZ2V0U3RhdGUoKTsgfVxuXG4gIC8qKlxuICAgKiBOb3JtYWxpemVzIHRoZSBnaXZlbiBwYXRoIGFuZCBjb21wYXJlcyB0byB0aGUgY3VycmVudCBub3JtYWxpemVkIHBhdGguXG4gICAqXG4gICAqIEBwYXJhbSBwYXRoIFRoZSBnaXZlbiBVUkwgcGF0aC5cbiAgICogQHBhcmFtIHF1ZXJ5IFF1ZXJ5IHBhcmFtZXRlcnMuXG4gICAqXG4gICAqIEByZXR1cm5zIFRydWUgaWYgdGhlIGdpdmVuIFVSTCBwYXRoIGlzIGVxdWFsIHRvIHRoZSBjdXJyZW50IG5vcm1hbGl6ZWQgcGF0aCwgZmFsc2VcbiAgICogb3RoZXJ3aXNlLlxuICAgKi9cbiAgaXNDdXJyZW50UGF0aEVxdWFsVG8ocGF0aDogc3RyaW5nLCBxdWVyeTogc3RyaW5nID0gJycpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5wYXRoKCkgPT0gdGhpcy5ub3JtYWxpemUocGF0aCArIExvY2F0aW9uLm5vcm1hbGl6ZVF1ZXJ5UGFyYW1zKHF1ZXJ5KSk7XG4gIH1cblxuICAvKipcbiAgICogTm9ybWFsaXplcyBhIFVSTCBwYXRoIGJ5IHN0cmlwcGluZyBhbnkgdHJhaWxpbmcgc2xhc2hlcy5cbiAgICpcbiAgICogQHBhcmFtIHVybCBTdHJpbmcgcmVwcmVzZW50aW5nIGEgVVJMLlxuICAgKlxuICAgKiBAcmV0dXJucyBUaGUgbm9ybWFsaXplZCBVUkwgc3RyaW5nLlxuICAgKi9cbiAgbm9ybWFsaXplKHVybDogc3RyaW5nKTogc3RyaW5nIHtcbiAgICByZXR1cm4gTG9jYXRpb24uc3RyaXBUcmFpbGluZ1NsYXNoKF9zdHJpcEJhc2VIcmVmKHRoaXMuX2Jhc2VIcmVmLCBfc3RyaXBJbmRleEh0bWwodXJsKSkpO1xuICB9XG5cbiAgLyoqXG4gICAqIE5vcm1hbGl6ZXMgYW4gZXh0ZXJuYWwgVVJMIHBhdGguXG4gICAqIElmIHRoZSBnaXZlbiBVUkwgZG9lc24ndCBiZWdpbiB3aXRoIGEgbGVhZGluZyBzbGFzaCAoYCcvJ2ApLCBhZGRzIG9uZVxuICAgKiBiZWZvcmUgbm9ybWFsaXppbmcuIEFkZHMgYSBoYXNoIGlmIGBIYXNoTG9jYXRpb25TdHJhdGVneWAgaXNcbiAgICogaW4gdXNlLCBvciB0aGUgYEFQUF9CQVNFX0hSRUZgIGlmIHRoZSBgUGF0aExvY2F0aW9uU3RyYXRlZ3lgIGlzIGluIHVzZS5cbiAgICpcbiAgICogQHBhcmFtIHVybCBTdHJpbmcgcmVwcmVzZW50aW5nIGEgVVJMLlxuICAgKlxuICAgKiBAcmV0dXJucyAgQSBub3JtYWxpemVkIHBsYXRmb3JtLXNwZWNpZmljIFVSTC5cbiAgICovXG4gIHByZXBhcmVFeHRlcm5hbFVybCh1cmw6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgaWYgKHVybCAmJiB1cmxbMF0gIT09ICcvJykge1xuICAgICAgdXJsID0gJy8nICsgdXJsO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy5fcGxhdGZvcm1TdHJhdGVneS5wcmVwYXJlRXh0ZXJuYWxVcmwodXJsKTtcbiAgfVxuXG4gIC8vIFRPRE86IHJlbmFtZSB0aGlzIG1ldGhvZCB0byBwdXNoU3RhdGVcbiAgLyoqXG4gICAqIENoYW5nZXMgdGhlIGJyb3dzZXIncyBVUkwgdG8gYSBub3JtYWxpemVkIHZlcnNpb24gb2YgYSBnaXZlbiBVUkwsIGFuZCBwdXNoZXMgYVxuICAgKiBuZXcgaXRlbSBvbnRvIHRoZSBwbGF0Zm9ybSdzIGhpc3RvcnkuXG4gICAqXG4gICAqIEBwYXJhbSBwYXRoICBVUkwgcGF0aCB0byBub3JtYWxpemUuXG4gICAqIEBwYXJhbSBxdWVyeSBRdWVyeSBwYXJhbWV0ZXJzLlxuICAgKiBAcGFyYW0gc3RhdGUgTG9jYXRpb24gaGlzdG9yeSBzdGF0ZS5cbiAgICpcbiAgICovXG4gIGdvKHBhdGg6IHN0cmluZywgcXVlcnk6IHN0cmluZyA9ICcnLCBzdGF0ZTogYW55ID0gbnVsbCk6IHZvaWQge1xuICAgIHRoaXMuX3BsYXRmb3JtU3RyYXRlZ3kucHVzaFN0YXRlKHN0YXRlLCAnJywgcGF0aCwgcXVlcnkpO1xuICAgIHRoaXMuX25vdGlmeVVybENoYW5nZUxpc3RlbmVycyhcbiAgICAgICAgdGhpcy5wcmVwYXJlRXh0ZXJuYWxVcmwocGF0aCArIExvY2F0aW9uLm5vcm1hbGl6ZVF1ZXJ5UGFyYW1zKHF1ZXJ5KSksIHN0YXRlKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBDaGFuZ2VzIHRoZSBicm93c2VyJ3MgVVJMIHRvIGEgbm9ybWFsaXplZCB2ZXJzaW9uIG9mIHRoZSBnaXZlbiBVUkwsIGFuZCByZXBsYWNlc1xuICAgKiB0aGUgdG9wIGl0ZW0gb24gdGhlIHBsYXRmb3JtJ3MgaGlzdG9yeSBzdGFjay5cbiAgICpcbiAgICogQHBhcmFtIHBhdGggIFVSTCBwYXRoIHRvIG5vcm1hbGl6ZS5cbiAgICogQHBhcmFtIHF1ZXJ5IFF1ZXJ5IHBhcmFtZXRlcnMuXG4gICAqIEBwYXJhbSBzdGF0ZSBMb2NhdGlvbiBoaXN0b3J5IHN0YXRlLlxuICAgKi9cbiAgcmVwbGFjZVN0YXRlKHBhdGg6IHN0cmluZywgcXVlcnk6IHN0cmluZyA9ICcnLCBzdGF0ZTogYW55ID0gbnVsbCk6IHZvaWQge1xuICAgIHRoaXMuX3BsYXRmb3JtU3RyYXRlZ3kucmVwbGFjZVN0YXRlKHN0YXRlLCAnJywgcGF0aCwgcXVlcnkpO1xuICAgIHRoaXMuX25vdGlmeVVybENoYW5nZUxpc3RlbmVycyhcbiAgICAgICAgdGhpcy5wcmVwYXJlRXh0ZXJuYWxVcmwocGF0aCArIExvY2F0aW9uLm5vcm1hbGl6ZVF1ZXJ5UGFyYW1zKHF1ZXJ5KSksIHN0YXRlKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBOYXZpZ2F0ZXMgZm9yd2FyZCBpbiB0aGUgcGxhdGZvcm0ncyBoaXN0b3J5LlxuICAgKi9cbiAgZm9yd2FyZCgpOiB2b2lkIHsgdGhpcy5fcGxhdGZvcm1TdHJhdGVneS5mb3J3YXJkKCk7IH1cblxuICAvKipcbiAgICogTmF2aWdhdGVzIGJhY2sgaW4gdGhlIHBsYXRmb3JtJ3MgaGlzdG9yeS5cbiAgICovXG4gIGJhY2soKTogdm9pZCB7IHRoaXMuX3BsYXRmb3JtU3RyYXRlZ3kuYmFjaygpOyB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyBhIFVSTCBjaGFuZ2UgbGlzdGVuZXIuIFVzZSB0byBjYXRjaCB1cGRhdGVzIHBlcmZvcm1lZCBieSB0aGUgQW5ndWxhclxuICAgKiBmcmFtZXdvcmsgdGhhdCBhcmUgbm90IGRldGVjdGlibGUgdGhyb3VnaCBcInBvcHN0YXRlXCIgb3IgXCJoYXNoY2hhbmdlXCIgZXZlbnRzLlxuICAgKlxuICAgKiBAcGFyYW0gZm4gVGhlIGNoYW5nZSBoYW5kbGVyIGZ1bmN0aW9uLCB3aGljaCB0YWtlIGEgVVJMIGFuZCBhIGxvY2F0aW9uIGhpc3Rvcnkgc3RhdGUuXG4gICAqL1xuICBvblVybENoYW5nZShmbjogKHVybDogc3RyaW5nLCBzdGF0ZTogdW5rbm93bikgPT4gdm9pZCkge1xuICAgIHRoaXMuX3VybENoYW5nZUxpc3RlbmVycy5wdXNoKGZuKTtcbiAgICB0aGlzLnN1YnNjcmliZSh2ID0+IHsgdGhpcy5fbm90aWZ5VXJsQ2hhbmdlTGlzdGVuZXJzKHYudXJsLCB2LnN0YXRlKTsgfSk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9ub3RpZnlVcmxDaGFuZ2VMaXN0ZW5lcnModXJsOiBzdHJpbmcgPSAnJywgc3RhdGU6IHVua25vd24pIHtcbiAgICB0aGlzLl91cmxDaGFuZ2VMaXN0ZW5lcnMuZm9yRWFjaChmbiA9PiBmbih1cmwsIHN0YXRlKSk7XG4gIH1cblxuICAvKipcbiAgICogU3Vic2NyaWJlcyB0byB0aGUgcGxhdGZvcm0ncyBgcG9wU3RhdGVgIGV2ZW50cy5cbiAgICpcbiAgICogQHBhcmFtIHZhbHVlIEV2ZW50IHRoYXQgaXMgdHJpZ2dlcmVkIHdoZW4gdGhlIHN0YXRlIGhpc3RvcnkgY2hhbmdlcy5cbiAgICogQHBhcmFtIGV4Y2VwdGlvbiBUaGUgZXhjZXB0aW9uIHRvIHRocm93LlxuICAgKlxuICAgKiBAcmV0dXJucyBTdWJzY3JpYmVkIGV2ZW50cy5cbiAgICovXG4gIHN1YnNjcmliZShcbiAgICAgIG9uTmV4dDogKHZhbHVlOiBQb3BTdGF0ZUV2ZW50KSA9PiB2b2lkLCBvblRocm93PzogKChleGNlcHRpb246IGFueSkgPT4gdm9pZCl8bnVsbCxcbiAgICAgIG9uUmV0dXJuPzogKCgpID0+IHZvaWQpfG51bGwpOiBTdWJzY3JpcHRpb25MaWtlIHtcbiAgICByZXR1cm4gdGhpcy5fc3ViamVjdC5zdWJzY3JpYmUoe25leHQ6IG9uTmV4dCwgZXJyb3I6IG9uVGhyb3csIGNvbXBsZXRlOiBvblJldHVybn0pO1xuICB9XG5cbiAgLyoqXG4gICAqIE5vcm1hbGl6ZXMgVVJMIHBhcmFtZXRlcnMgYnkgcHJlcGVuZGluZyB3aXRoIGA/YCBpZiBuZWVkZWQuXG4gICAqXG4gICAqIEBwYXJhbSAgcGFyYW1zIFN0cmluZyBvZiBVUkwgcGFyYW1ldGVycy5cbiAgICpcbiAgICogQHJldHVybnMgVGhlIG5vcm1hbGl6ZWQgVVJMIHBhcmFtZXRlcnMgc3RyaW5nLlxuICAgKi9cbiAgcHVibGljIHN0YXRpYyBub3JtYWxpemVRdWVyeVBhcmFtcyhwYXJhbXM6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHBhcmFtcyAmJiBwYXJhbXNbMF0gIT09ICc/JyA/ICc/JyArIHBhcmFtcyA6IHBhcmFtcztcbiAgfVxuXG4gIC8qKlxuICAgKiBKb2lucyB0d28gcGFydHMgb2YgYSBVUkwgd2l0aCBhIHNsYXNoIGlmIG5lZWRlZC5cbiAgICpcbiAgICogQHBhcmFtIHN0YXJ0ICBVUkwgc3RyaW5nXG4gICAqIEBwYXJhbSBlbmQgICAgVVJMIHN0cmluZ1xuICAgKlxuICAgKlxuICAgKiBAcmV0dXJucyBUaGUgam9pbmVkIFVSTCBzdHJpbmcuXG4gICAqL1xuICBwdWJsaWMgc3RhdGljIGpvaW5XaXRoU2xhc2goc3RhcnQ6IHN0cmluZywgZW5kOiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIGlmIChzdGFydC5sZW5ndGggPT0gMCkge1xuICAgICAgcmV0dXJuIGVuZDtcbiAgICB9XG4gICAgaWYgKGVuZC5sZW5ndGggPT0gMCkge1xuICAgICAgcmV0dXJuIHN0YXJ0O1xuICAgIH1cbiAgICBsZXQgc2xhc2hlcyA9IDA7XG4gICAgaWYgKHN0YXJ0LmVuZHNXaXRoKCcvJykpIHtcbiAgICAgIHNsYXNoZXMrKztcbiAgICB9XG4gICAgaWYgKGVuZC5zdGFydHNXaXRoKCcvJykpIHtcbiAgICAgIHNsYXNoZXMrKztcbiAgICB9XG4gICAgaWYgKHNsYXNoZXMgPT0gMikge1xuICAgICAgcmV0dXJuIHN0YXJ0ICsgZW5kLnN1YnN0cmluZygxKTtcbiAgICB9XG4gICAgaWYgKHNsYXNoZXMgPT0gMSkge1xuICAgICAgcmV0dXJuIHN0YXJ0ICsgZW5kO1xuICAgIH1cbiAgICByZXR1cm4gc3RhcnQgKyAnLycgKyBlbmQ7XG4gIH1cblxuICAvKipcbiAgICogUmVtb3ZlcyBhIHRyYWlsaW5nIHNsYXNoIGZyb20gYSBVUkwgc3RyaW5nIGlmIG5lZWRlZC5cbiAgICogTG9va3MgZm9yIHRoZSBmaXJzdCBvY2N1cnJlbmNlIG9mIGVpdGhlciBgI2AsIGA/YCwgb3IgdGhlIGVuZCBvZiB0aGVcbiAgICogbGluZSBhcyBgL2AgY2hhcmFjdGVycyBhbmQgcmVtb3ZlcyB0aGUgdHJhaWxpbmcgc2xhc2ggaWYgb25lIGV4aXN0cy5cbiAgICpcbiAgICogQHBhcmFtIHVybCBVUkwgc3RyaW5nLlxuICAgKlxuICAgKiBAcmV0dXJucyBUaGUgVVJMIHN0cmluZywgbW9kaWZpZWQgaWYgbmVlZGVkLlxuICAgKi9cbiAgcHVibGljIHN0YXRpYyBzdHJpcFRyYWlsaW5nU2xhc2godXJsOiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIGNvbnN0IG1hdGNoID0gdXJsLm1hdGNoKC8jfFxcP3wkLyk7XG4gICAgY29uc3QgcGF0aEVuZElkeCA9IG1hdGNoICYmIG1hdGNoLmluZGV4IHx8IHVybC5sZW5ndGg7XG4gICAgY29uc3QgZHJvcHBlZFNsYXNoSWR4ID0gcGF0aEVuZElkeCAtICh1cmxbcGF0aEVuZElkeCAtIDFdID09PSAnLycgPyAxIDogMCk7XG4gICAgcmV0dXJuIHVybC5zbGljZSgwLCBkcm9wcGVkU2xhc2hJZHgpICsgdXJsLnNsaWNlKHBhdGhFbmRJZHgpO1xuICB9XG59XG5cbmZ1bmN0aW9uIF9zdHJpcEJhc2VIcmVmKGJhc2VIcmVmOiBzdHJpbmcsIHVybDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGJhc2VIcmVmICYmIHVybC5zdGFydHNXaXRoKGJhc2VIcmVmKSA/IHVybC5zdWJzdHJpbmcoYmFzZUhyZWYubGVuZ3RoKSA6IHVybDtcbn1cblxuZnVuY3Rpb24gX3N0cmlwSW5kZXhIdG1sKHVybDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIHVybC5yZXBsYWNlKC9cXC9pbmRleC5odG1sJC8sICcnKTtcbn1cbiJdfQ==