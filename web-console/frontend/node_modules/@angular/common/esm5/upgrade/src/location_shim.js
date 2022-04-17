/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { deepEqual, isAnchor, isPromise } from './utils';
var PATH_MATCH = /^([^?#]*)(\?([^#]*))?(#(.*))?$/;
var DOUBLE_SLASH_REGEX = /^\s*[\\/]{2,}/;
var IGNORE_URI_REGEXP = /^\s*(javascript|mailto):/i;
var DEFAULT_PORTS = {
    'http:': 80,
    'https:': 443,
    'ftp:': 21
};
/**
 * Location service that provides a drop-in replacement for the $location service
 * provided in AngularJS.
 *
 * @see [Using the Angular Unified Location Service](guide/upgrade#using-the-unified-angular-location-service)
 *
 * @publicApi
 */
var $locationShim = /** @class */ (function () {
    function $locationShim($injector, location, platformLocation, urlCodec, locationStrategy) {
        var _this = this;
        this.location = location;
        this.platformLocation = platformLocation;
        this.urlCodec = urlCodec;
        this.locationStrategy = locationStrategy;
        this.initalizing = true;
        this.updateBrowser = false;
        this.$$absUrl = '';
        this.$$url = '';
        this.$$host = '';
        this.$$replace = false;
        this.$$path = '';
        this.$$search = '';
        this.$$hash = '';
        this.$$changeListeners = [];
        this.cachedState = null;
        this.lastBrowserUrl = '';
        // This variable should be used *only* inside the cacheState function.
        this.lastCachedState = null;
        var initialUrl = this.browserUrl();
        var parsedUrl = this.urlCodec.parse(initialUrl);
        if (typeof parsedUrl === 'string') {
            throw 'Invalid URL';
        }
        this.$$protocol = parsedUrl.protocol;
        this.$$host = parsedUrl.hostname;
        this.$$port = parseInt(parsedUrl.port) || DEFAULT_PORTS[parsedUrl.protocol] || null;
        this.$$parseLinkUrl(initialUrl, initialUrl);
        this.cacheState();
        this.$$state = this.browserState();
        if (isPromise($injector)) {
            $injector.then(function ($i) { return _this.initialize($i); });
        }
        else {
            this.initialize($injector);
        }
    }
    $locationShim.prototype.initialize = function ($injector) {
        var _this = this;
        var $rootScope = $injector.get('$rootScope');
        var $rootElement = $injector.get('$rootElement');
        $rootElement.on('click', function (event) {
            if (event.ctrlKey || event.metaKey || event.shiftKey || event.which === 2 ||
                event.button === 2) {
                return;
            }
            var elm = event.target;
            // traverse the DOM up to find first A tag
            while (elm && elm.nodeName.toLowerCase() !== 'a') {
                // ignore rewriting if no A tag (reached root element, or no parent - removed from document)
                if (elm === $rootElement[0] || !(elm = elm.parentNode)) {
                    return;
                }
            }
            if (!isAnchor(elm)) {
                return;
            }
            var absHref = elm.href;
            var relHref = elm.getAttribute('href');
            // Ignore when url is started with javascript: or mailto:
            if (IGNORE_URI_REGEXP.test(absHref)) {
                return;
            }
            if (absHref && !elm.getAttribute('target') && !event.isDefaultPrevented()) {
                if (_this.$$parseLinkUrl(absHref, relHref)) {
                    // We do a preventDefault for all urls that are part of the AngularJS application,
                    // in html5mode and also without, so that we are able to abort navigation without
                    // getting double entries in the location history.
                    event.preventDefault();
                    // update location manually
                    if (_this.absUrl() !== _this.browserUrl()) {
                        $rootScope.$apply();
                    }
                }
            }
        });
        this.location.onUrlChange(function (newUrl, newState) {
            var oldUrl = _this.absUrl();
            var oldState = _this.$$state;
            _this.$$parse(newUrl);
            newUrl = _this.absUrl();
            _this.$$state = newState;
            var defaultPrevented = $rootScope.$broadcast('$locationChangeStart', newUrl, oldUrl, newState, oldState)
                .defaultPrevented;
            // if the location was changed by a `$locationChangeStart` handler then stop
            // processing this location change
            if (_this.absUrl() !== newUrl)
                return;
            // If default was prevented, set back to old state. This is the state that was locally
            // cached in the $location service.
            if (defaultPrevented) {
                _this.$$parse(oldUrl);
                _this.state(oldState);
                _this.setBrowserUrlWithFallback(oldUrl, false, oldState);
            }
            else {
                _this.initalizing = false;
                $rootScope.$broadcast('$locationChangeSuccess', newUrl, oldUrl, newState, oldState);
                _this.resetBrowserUpdate();
            }
            if (!$rootScope.$$phase) {
                $rootScope.$digest();
            }
        });
        // update browser
        $rootScope.$watch(function () {
            if (_this.initalizing || _this.updateBrowser) {
                _this.updateBrowser = false;
                var oldUrl_1 = _this.browserUrl();
                var newUrl = _this.absUrl();
                var oldState_1 = _this.browserState();
                var currentReplace_1 = _this.$$replace;
                var urlOrStateChanged_1 = !_this.urlCodec.areEqual(oldUrl_1, newUrl) || oldState_1 !== _this.$$state;
                // Fire location changes one time to on initialization. This must be done on the
                // next tick (thus inside $evalAsync()) in order for listeners to be registered
                // before the event fires. Mimicing behavior from $locationWatch:
                // https://github.com/angular/angular.js/blob/master/src/ng/location.js#L983
                if (_this.initalizing || urlOrStateChanged_1) {
                    _this.initalizing = false;
                    $rootScope.$evalAsync(function () {
                        // Get the new URL again since it could have changed due to async update
                        var newUrl = _this.absUrl();
                        var defaultPrevented = $rootScope
                            .$broadcast('$locationChangeStart', newUrl, oldUrl_1, _this.$$state, oldState_1)
                            .defaultPrevented;
                        // if the location was changed by a `$locationChangeStart` handler then stop
                        // processing this location change
                        if (_this.absUrl() !== newUrl)
                            return;
                        if (defaultPrevented) {
                            _this.$$parse(oldUrl_1);
                            _this.$$state = oldState_1;
                        }
                        else {
                            // This block doesn't run when initalizing because it's going to perform the update to
                            // the URL which shouldn't be needed when initalizing.
                            if (urlOrStateChanged_1) {
                                _this.setBrowserUrlWithFallback(newUrl, currentReplace_1, oldState_1 === _this.$$state ? null : _this.$$state);
                                _this.$$replace = false;
                            }
                            $rootScope.$broadcast('$locationChangeSuccess', newUrl, oldUrl_1, _this.$$state, oldState_1);
                        }
                    });
                }
            }
            _this.$$replace = false;
        });
    };
    $locationShim.prototype.resetBrowserUpdate = function () {
        this.$$replace = false;
        this.$$state = this.browserState();
        this.updateBrowser = false;
        this.lastBrowserUrl = this.browserUrl();
    };
    $locationShim.prototype.browserUrl = function (url, replace, state) {
        // In modern browsers `history.state` is `null` by default; treating it separately
        // from `undefined` would cause `$browser.url('/foo')` to change `history.state`
        // to undefined via `pushState`. Instead, let's change `undefined` to `null` here.
        if (typeof state === 'undefined') {
            state = null;
        }
        // setter
        if (url) {
            var sameState = this.lastHistoryState === state;
            // Normalize the inputted URL
            url = this.urlCodec.parse(url).href;
            // Don't change anything if previous and current URLs and states match.
            if (this.lastBrowserUrl === url && sameState) {
                return this;
            }
            this.lastBrowserUrl = url;
            this.lastHistoryState = state;
            // Remove server base from URL as the Angular APIs for updating URL require
            // it to be the path+.
            url = this.stripBaseUrl(this.getServerBase(), url) || url;
            // Set the URL
            if (replace) {
                this.locationStrategy.replaceState(state, '', url, '');
            }
            else {
                this.locationStrategy.pushState(state, '', url, '');
            }
            this.cacheState();
            return this;
            // getter
        }
        else {
            return this.platformLocation.href;
        }
    };
    $locationShim.prototype.cacheState = function () {
        // This should be the only place in $browser where `history.state` is read.
        this.cachedState = this.platformLocation.getState();
        if (typeof this.cachedState === 'undefined') {
            this.cachedState = null;
        }
        // Prevent callbacks fo fire twice if both hashchange & popstate were fired.
        if (deepEqual(this.cachedState, this.lastCachedState)) {
            this.cachedState = this.lastCachedState;
        }
        this.lastCachedState = this.cachedState;
        this.lastHistoryState = this.cachedState;
    };
    /**
     * This function emulates the $browser.state() function from AngularJS. It will cause
     * history.state to be cached unless changed with deep equality check.
     */
    $locationShim.prototype.browserState = function () { return this.cachedState; };
    $locationShim.prototype.stripBaseUrl = function (base, url) {
        if (url.startsWith(base)) {
            return url.substr(base.length);
        }
        return undefined;
    };
    $locationShim.prototype.getServerBase = function () {
        var _a = this.platformLocation, protocol = _a.protocol, hostname = _a.hostname, port = _a.port;
        var baseHref = this.locationStrategy.getBaseHref();
        var url = protocol + "//" + hostname + (port ? ':' + port : '') + (baseHref || '/');
        return url.endsWith('/') ? url : url + '/';
    };
    $locationShim.prototype.parseAppUrl = function (url) {
        if (DOUBLE_SLASH_REGEX.test(url)) {
            throw new Error("Bad Path - URL cannot start with double slashes: " + url);
        }
        var prefixed = (url.charAt(0) !== '/');
        if (prefixed) {
            url = '/' + url;
        }
        var match = this.urlCodec.parse(url, this.getServerBase());
        if (typeof match === 'string') {
            throw new Error("Bad URL - Cannot parse URL: " + url);
        }
        var path = prefixed && match.pathname.charAt(0) === '/' ? match.pathname.substring(1) : match.pathname;
        this.$$path = this.urlCodec.decodePath(path);
        this.$$search = this.urlCodec.decodeSearch(match.search);
        this.$$hash = this.urlCodec.decodeHash(match.hash);
        // make sure path starts with '/';
        if (this.$$path && this.$$path.charAt(0) !== '/') {
            this.$$path = '/' + this.$$path;
        }
    };
    /**
     * Registers listeners for URL changes. This API is used to catch updates performed by the
     * AngularJS framework. These changes are a subset of the `$locationChangeStart` and
     * `$locationChangeSuccess` events which fire when AngularJS updates its internally-referenced
     * version of the browser URL.
     *
     * It's possible for `$locationChange` events to happen, but for the browser URL
     * (window.location) to remain unchanged. This `onChange` callback will fire only when AngularJS
     * actually updates the browser URL (window.location).
     *
     * @param fn The callback function that is triggered for the listener when the URL changes.
     * @param err The callback function that is triggered when an error occurs.
     */
    $locationShim.prototype.onChange = function (fn, err) {
        if (err === void 0) { err = function (e) { }; }
        this.$$changeListeners.push([fn, err]);
    };
    /** @internal */
    $locationShim.prototype.$$notifyChangeListeners = function (url, state, oldUrl, oldState) {
        if (url === void 0) { url = ''; }
        if (oldUrl === void 0) { oldUrl = ''; }
        this.$$changeListeners.forEach(function (_a) {
            var _b = tslib_1.__read(_a, 2), fn = _b[0], err = _b[1];
            try {
                fn(url, state, oldUrl, oldState);
            }
            catch (e) {
                err(e);
            }
        });
    };
    /**
     * Parses the provided URL, and sets the current URL to the parsed result.
     *
     * @param url The URL string.
     */
    $locationShim.prototype.$$parse = function (url) {
        var pathUrl;
        if (url.startsWith('/')) {
            pathUrl = url;
        }
        else {
            // Remove protocol & hostname if URL starts with it
            pathUrl = this.stripBaseUrl(this.getServerBase(), url);
        }
        if (typeof pathUrl === 'undefined') {
            throw new Error("Invalid url \"" + url + "\", missing path prefix \"" + this.getServerBase() + "\".");
        }
        this.parseAppUrl(pathUrl);
        if (!this.$$path) {
            this.$$path = '/';
        }
        this.composeUrls();
    };
    /**
     * Parses the provided URL and its relative URL.
     *
     * @param url The full URL string.
     * @param relHref A URL string relative to the full URL string.
     */
    $locationShim.prototype.$$parseLinkUrl = function (url, relHref) {
        // When relHref is passed, it should be a hash and is handled separately
        if (relHref && relHref[0] === '#') {
            this.hash(relHref.slice(1));
            return true;
        }
        var rewrittenUrl;
        var appUrl = this.stripBaseUrl(this.getServerBase(), url);
        if (typeof appUrl !== 'undefined') {
            rewrittenUrl = this.getServerBase() + appUrl;
        }
        else if (this.getServerBase() === url + '/') {
            rewrittenUrl = this.getServerBase();
        }
        // Set the URL
        if (rewrittenUrl) {
            this.$$parse(rewrittenUrl);
        }
        return !!rewrittenUrl;
    };
    $locationShim.prototype.setBrowserUrlWithFallback = function (url, replace, state) {
        var oldUrl = this.url();
        var oldState = this.$$state;
        try {
            this.browserUrl(url, replace, state);
            // Make sure $location.state() returns referentially identical (not just deeply equal)
            // state object; this makes possible quick checking if the state changed in the digest
            // loop. Checking deep equality would be too expensive.
            this.$$state = this.browserState();
            this.$$notifyChangeListeners(url, state, oldUrl, oldState);
        }
        catch (e) {
            // Restore old values if pushState fails
            this.url(oldUrl);
            this.$$state = oldState;
            throw e;
        }
    };
    $locationShim.prototype.composeUrls = function () {
        this.$$url = this.urlCodec.normalize(this.$$path, this.$$search, this.$$hash);
        this.$$absUrl = this.getServerBase() + this.$$url.substr(1); // remove '/' from front of URL
        this.updateBrowser = true;
    };
    /**
     * Retrieves the full URL representation with all segments encoded according to
     * rules specified in
     * [RFC 3986](http://www.ietf.org/rfc/rfc3986.txt).
     *
     *
     * ```js
     * // given URL http://example.com/#/some/path?foo=bar&baz=xoxo
     * let absUrl = $location.absUrl();
     * // => "http://example.com/#/some/path?foo=bar&baz=xoxo"
     * ```
     */
    $locationShim.prototype.absUrl = function () { return this.$$absUrl; };
    $locationShim.prototype.url = function (url) {
        if (typeof url === 'string') {
            if (!url.length) {
                url = '/';
            }
            var match = PATH_MATCH.exec(url);
            if (!match)
                return this;
            if (match[1] || url === '')
                this.path(this.urlCodec.decodePath(match[1]));
            if (match[2] || match[1] || url === '')
                this.search(match[3] || '');
            this.hash(match[5] || '');
            // Chainable method
            return this;
        }
        return this.$$url;
    };
    /**
     * Retrieves the protocol of the current URL.
     *
     * ```js
     * // given URL http://example.com/#/some/path?foo=bar&baz=xoxo
     * let protocol = $location.protocol();
     * // => "http"
     * ```
     */
    $locationShim.prototype.protocol = function () { return this.$$protocol; };
    /**
     * Retrieves the protocol of the current URL.
     *
     * In contrast to the non-AngularJS version `location.host` which returns `hostname:port`, this
     * returns the `hostname` portion only.
     *
     *
     * ```js
     * // given URL http://example.com/#/some/path?foo=bar&baz=xoxo
     * let host = $location.host();
     * // => "example.com"
     *
     * // given URL http://user:password@example.com:8080/#/some/path?foo=bar&baz=xoxo
     * host = $location.host();
     * // => "example.com"
     * host = location.host;
     * // => "example.com:8080"
     * ```
     */
    $locationShim.prototype.host = function () { return this.$$host; };
    /**
     * Retrieves the port of the current URL.
     *
     * ```js
     * // given URL http://example.com/#/some/path?foo=bar&baz=xoxo
     * let port = $location.port();
     * // => 80
     * ```
     */
    $locationShim.prototype.port = function () { return this.$$port; };
    $locationShim.prototype.path = function (path) {
        if (typeof path === 'undefined') {
            return this.$$path;
        }
        // null path converts to empty string. Prepend with "/" if needed.
        path = path !== null ? path.toString() : '';
        path = path.charAt(0) === '/' ? path : '/' + path;
        this.$$path = path;
        this.composeUrls();
        return this;
    };
    $locationShim.prototype.search = function (search, paramValue) {
        switch (arguments.length) {
            case 0:
                return this.$$search;
            case 1:
                if (typeof search === 'string' || typeof search === 'number') {
                    this.$$search = this.urlCodec.decodeSearch(search.toString());
                }
                else if (typeof search === 'object' && search !== null) {
                    // Copy the object so it's never mutated
                    search = tslib_1.__assign({}, search);
                    // remove object undefined or null properties
                    for (var key in search) {
                        if (search[key] == null)
                            delete search[key];
                    }
                    this.$$search = search;
                }
                else {
                    throw new Error('LocationProvider.search(): First argument must be a string or an object.');
                }
                break;
            default:
                if (typeof search === 'string') {
                    var currentSearch = this.search();
                    if (typeof paramValue === 'undefined' || paramValue === null) {
                        delete currentSearch[search];
                        return this.search(currentSearch);
                    }
                    else {
                        currentSearch[search] = paramValue;
                        return this.search(currentSearch);
                    }
                }
        }
        this.composeUrls();
        return this;
    };
    $locationShim.prototype.hash = function (hash) {
        if (typeof hash === 'undefined') {
            return this.$$hash;
        }
        this.$$hash = hash !== null ? hash.toString() : '';
        this.composeUrls();
        return this;
    };
    /**
     * Changes to `$location` during the current `$digest` will replace the current
     * history record, instead of adding a new one.
     */
    $locationShim.prototype.replace = function () {
        this.$$replace = true;
        return this;
    };
    $locationShim.prototype.state = function (state) {
        if (typeof state === 'undefined') {
            return this.$$state;
        }
        this.$$state = state;
        return this;
    };
    return $locationShim;
}());
export { $locationShim };
/**
 * The factory function used to create an instance of the `$locationShim` in Angular,
 * and provides an API-compatiable `$locationProvider` for AngularJS.
 *
 * @publicApi
 */
var $locationShimProvider = /** @class */ (function () {
    function $locationShimProvider(ngUpgrade, location, platformLocation, urlCodec, locationStrategy) {
        this.ngUpgrade = ngUpgrade;
        this.location = location;
        this.platformLocation = platformLocation;
        this.urlCodec = urlCodec;
        this.locationStrategy = locationStrategy;
    }
    /**
     * Factory method that returns an instance of the $locationShim
     */
    $locationShimProvider.prototype.$get = function () {
        return new $locationShim(this.ngUpgrade.$injector, this.location, this.platformLocation, this.urlCodec, this.locationStrategy);
    };
    /**
     * Stub method used to keep API compatible with AngularJS. This setting is configured through
     * the LocationUpgradeModule's `config` method in your Angular app.
     */
    $locationShimProvider.prototype.hashPrefix = function (prefix) {
        throw new Error('Configure LocationUpgrade through LocationUpgradeModule.config method.');
    };
    /**
     * Stub method used to keep API compatible with AngularJS. This setting is configured through
     * the LocationUpgradeModule's `config` method in your Angular app.
     */
    $locationShimProvider.prototype.html5Mode = function (mode) {
        throw new Error('Configure LocationUpgrade through LocationUpgradeModule.config method.');
    };
    return $locationShimProvider;
}());
export { $locationShimProvider };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9jYXRpb25fc2hpbS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi91cGdyYWRlL3NyYy9sb2NhdGlvbl9zaGltLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFNSCxPQUFPLEVBQUMsU0FBUyxFQUFFLFFBQVEsRUFBRSxTQUFTLEVBQUMsTUFBTSxTQUFTLENBQUM7QUFFdkQsSUFBTSxVQUFVLEdBQUcsZ0NBQWdDLENBQUM7QUFDcEQsSUFBTSxrQkFBa0IsR0FBRyxlQUFlLENBQUM7QUFDM0MsSUFBTSxpQkFBaUIsR0FBRywyQkFBMkIsQ0FBQztBQUN0RCxJQUFNLGFBQWEsR0FBNEI7SUFDN0MsT0FBTyxFQUFFLEVBQUU7SUFDWCxRQUFRLEVBQUUsR0FBRztJQUNiLE1BQU0sRUFBRSxFQUFFO0NBQ1gsQ0FBQztBQUVGOzs7Ozs7O0dBT0c7QUFDSDtJQXVCRSx1QkFDSSxTQUFjLEVBQVUsUUFBa0IsRUFBVSxnQkFBa0MsRUFDOUUsUUFBa0IsRUFBVSxnQkFBa0M7UUFGMUUsaUJBd0JDO1FBdkIyQixhQUFRLEdBQVIsUUFBUSxDQUFVO1FBQVUscUJBQWdCLEdBQWhCLGdCQUFnQixDQUFrQjtRQUM5RSxhQUFRLEdBQVIsUUFBUSxDQUFVO1FBQVUscUJBQWdCLEdBQWhCLGdCQUFnQixDQUFrQjtRQXhCbEUsZ0JBQVcsR0FBRyxJQUFJLENBQUM7UUFDbkIsa0JBQWEsR0FBRyxLQUFLLENBQUM7UUFDdEIsYUFBUSxHQUFXLEVBQUUsQ0FBQztRQUN0QixVQUFLLEdBQVcsRUFBRSxDQUFDO1FBRW5CLFdBQU0sR0FBVyxFQUFFLENBQUM7UUFFcEIsY0FBUyxHQUFZLEtBQUssQ0FBQztRQUMzQixXQUFNLEdBQVcsRUFBRSxDQUFDO1FBQ3BCLGFBQVEsR0FBUSxFQUFFLENBQUM7UUFDbkIsV0FBTSxHQUFXLEVBQUUsQ0FBQztRQUVwQixzQkFB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LHdFQUF3RSxDQUFDLENBQUM7SUFDNUYsQ0FBQztJQUVEOzs7T0FHRztJQUNILHlDQUFTLEdBQVQsVUFBVSxJQUFVO1FBQ2xCLE1BQU0sSUFBSSxLQUFLLENBQUMsd0VBQXdFLENBQUMsQ0FBQztJQUM1RixDQUFDO0lBQ0gsNEJBQUM7QUFBRCxDQUFDLEFBOUJELElBOEJDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0xvY2F0aW9uLCBMb2NhdGlvblN0cmF0ZWd5LCBQbGF0Zm9ybUxvY2F0aW9ufSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHtVcGdyYWRlTW9kdWxlfSBmcm9tICdAYW5ndWxhci91cGdyYWRlL3N0YXRpYyc7XG5cbmltcG9ydCB7VXJsQ29kZWN9IGZyb20gJy4vcGFyYW1zJztcbmltcG9ydCB7ZGVlcEVxdWFsLCBpc0FuY2hvciwgaXNQcm9taXNlfSBmcm9tICcuL3V0aWxzJztcblxuY29uc3QgUEFUSF9NQVRDSCA9IC9eKFtePyNdKikoXFw/KFteI10qKSk/KCMoLiopKT8kLztcbmNvbnN0IERPVUJMRV9TTEFTSF9SRUdFWCA9IC9eXFxzKltcXFxcL117Mix9LztcbmNvbnN0IElHTk9SRV9VUklfUkVHRVhQID0gL15cXHMqKGphdmFzY3JpcHR8bWFpbHRvKTovaTtcbmNvbnN0IERFRkFVTFRfUE9SVFM6IHtba2V5OiBzdHJpbmddOiBudW1iZXJ9ID0ge1xuICAnaHR0cDonOiA4MCxcbiAgJ2h0dHBzOic6IDQ0MyxcbiAgJ2Z0cDonOiAyMVxufTtcblxuLyoqXG4gKiBMb2NhdGlvbiBzZXJ2aWNlIHRoYXQgcHJvdmlkZXMgYSBkcm9wLWluIHJlcGxhY2VtZW50IGZvciB0aGUgJGxvY2F0aW9uIHNlcnZpY2VcbiAqIHByb3ZpZGVkIGluIEFuZ3VsYXJKUy5cbiAqXG4gKiBAc2VlIFtVc2luZyB0aGUgQW5ndWxhciBVbmlmaWVkIExvY2F0aW9uIFNlcnZpY2VdKGd1aWRlL3VwZ3JhZGUjdXNpbmctdGhlLXVuaWZpZWQtYW5ndWxhci1sb2NhdGlvbi1zZXJ2aWNlKVxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzICRsb2NhdGlvblNoaW0ge1xuICBwcml2YXRlIGluaXRhbGl6aW5nID0gdHJ1ZTtcbiAgcHJpdmF0ZSB1cGRhdGVCcm93c2VyID0gZmFsc2U7XG4gIHByaXZhdGUgJCRhYnNVcmw6IHN0cmluZyA9ICcnO1xuICBwcml2YXRlICQkdXJsOiBzdHJpbmcgPSAnJztcbiAgcHJpdmF0ZSAkJHByb3RvY29sOiBzdHJpbmc7XG4gIHByaXZhdGUgJCRob3N0OiBzdHJpbmcgPSAnJztcbiAgcHJpdmF0ZSAkJHBvcnQ6IG51bWJlcnxudWxsO1xuICBwcml2YXRlICQkcmVwbGFjZTogYm9vbGVhbiA9IGZhbHNlO1xuICBwcml2YXRlICQkcGF0aDogc3RyaW5nID0gJyc7XG4gIHByaXZhdGUgJCRzZWFyY2g6IGFueSA9ICcnO1xuICBwcml2YXRlICQkaGFzaDogc3RyaW5nID0gJyc7XG4gIHByaXZhdGUgJCRzdGF0ZTogdW5rbm93bjtcbiAgcHJpdmF0ZSAkJGNoYW5nZUxpc3RlbmVyczogW1xuICAgICgodXJsOiBzdHJpbmcsIHN0YXRlOiB1bmtub3duLCBvbGRVcmw6IHN0cmluZywgb2xkU3RhdGU6IHVua25vd24sIGVycj86IChlOiBFcnJvcikgPT4gdm9pZCkgPT5cbiAgICAgICAgIHZvaWQpLFxuICAgIChlOiBFcnJvcikgPT4gdm9pZFxuICBdW10gPSBbXTtcblxuICBwcml2YXRlIGNhY2hlZFN0YXRlOiB1bmtub3duID0gbnVsbDtcblxuXG5cbiAgY29uc3RydWN0b3IoXG4gICAgICAkaW5qZWN0b3I6IGFueSwgcHJpdmF0ZSBsb2NhdGlvbjogTG9jYXRpb24sIHByaXZhdGUgcGxhdGZvcm1Mb2NhdGlvbjogUGxhdGZvcm1Mb2NhdGlvbixcbiAgICAgIHByaXZhdGUgdXJsQ29kZWM6IFVybENvZGVjLCBwcml2YXRlIGxvY2F0aW9uU3RyYXRlZ3k6IExvY2F0aW9uU3RyYXRlZ3kpIHtcbiAgICBjb25zdCBpbml0aWFsVXJsID0gdGhpcy5icm93c2VyVXJsKCk7XG5cbiAgICBsZXQgcGFyc2VkVXJsID0gdGhpcy51cmxDb2RlYy5wYXJzZShpbml0aWFsVXJsKTtcblxuICAgIGlmICh0eXBlb2YgcGFyc2VkVXJsID09PSAnc3RyaW5nJykge1xuICAgICAgdGhyb3cgJ0ludmFsaWQgVVJMJztcbiAgICB9XG5cbiAgICB0aGlzLiQkcHJvdG9jb2wgPSBwYXJzZWRVcmwucHJvdG9jb2w7XG4gICAgdGhpcy4kJGhvc3QgPSBwYXJzZWRVcmwuaG9zdG5hbWU7XG4gICAgdGhpcy4kJHBvcnQgPSBwYXJzZUludChwYXJzZWRVcmwucG9ydCkgfHwgREVGQVVMVF9QT1JUU1twYXJzZWRVcmwucHJvdG9jb2xdIHx8IG51bGw7XG5cbiAgICB0aGlzLiQkcGFyc2VMaW5rVXJsKGluaXRpYWxVcmwsIGluaXRpYWxVcmwpO1xuICAgIHRoaXMuY2FjaGVTdGF0ZSgpO1xuICAgIHRoaXMuJCRzdGF0ZSA9IHRoaXMuYnJvd3NlclN0YXRlKCk7XG5cbiAgICBpZiAoaXNQcm9taXNlKCRpbmplY3RvcikpIHtcbiAgICAgICRpbmplY3Rvci50aGVuKCRpID0+IHRoaXMuaW5pdGlhbGl6ZSgkaSkpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmluaXRpYWxpemUoJGluamVjdG9yKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGluaXRpYWxpemUoJGluamVjdG9yOiBhbnkpIHtcbiAgICBjb25zdCAkcm9vdFNjb3BlID0gJGluamVjdG9yLmdldCgnJHJvb3RTY29wZScpO1xuICAgIGNvbnN0ICRyb290RWxlbWVudCA9ICRpbmplY3Rvci5nZXQoJyRyb290RWxlbWVudCcpO1xuXG4gICAgJHJvb3RFbGVtZW50Lm9uKCdjbGljaycsIChldmVudDogYW55KSA9PiB7XG4gICAgICBpZiAoZXZlbnQuY3RybEtleSB8fCBldmVudC5tZXRhS2V5IHx8IGV2ZW50LnNoaWZ0S2V5IHx8IGV2ZW50LndoaWNoID09PSAyIHx8XG4gICAgICAgICAgZXZlbnQuYnV0dG9uID09PSAyKSB7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cblxuICAgICAgbGV0IGVsbTogKE5vZGUgJiBQYXJlbnROb2RlKXxudWxsID0gZXZlbnQudGFyZ2V0O1xuXG4gICAgICAvLyB0cmF2ZXJzZSB0aGUgRE9NIHVwIHRvIGZpbmQgZmlyc3QgQSB0YWdcbiAgICAgIHdoaWxlIChlbG0gJiYgZWxtLm5vZGVOYW1lLnRvTG93ZXJDYXNlKCkgIT09ICdhJykge1xuICAgICAgICAvLyBpZ25vcmUgcmV3cml0aW5nIGlmIG5vIEEgdGFnIChyZWFjaGVkIHJvb3QgZWxlbWVudCwgb3Igbm8gcGFyZW50IC0gcmVtb3ZlZCBmcm9tIGRvY3VtZW50KVxuICAgICAgICBpZiAoZWxtID09PSAkcm9vdEVsZW1lbnRbMF0gfHwgIShlbG0gPSBlbG0ucGFyZW50Tm9kZSkpIHtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgaWYgKCFpc0FuY2hvcihlbG0pKSB7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cblxuICAgICAgY29uc3QgYWJzSHJlZiA9IGVsbS5ocmVmO1xuICAgICAgY29uc3QgcmVsSHJlZiA9IGVsbS5nZXRBdHRyaWJ1dGUoJ2hyZWYnKTtcblxuICAgICAgLy8gSWdub3JlIHdoZW4gdXJsIGlzIHN0YXJ0ZWQgd2l0aCBqYXZhc2NyaXB0OiBvciBtYWlsdG86XG4gICAgICBpZiAoSUdOT1JFX1VSSV9SRUdFWFAudGVzdChhYnNIcmVmKSkge1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG5cbiAgICAgIGlmIChhYnNIcmVmICYmICFlbG0uZ2V0QXR0cmlidXRlKCd0YXJnZXQnKSAmJiAhZXZlbnQuaXNEZWZhdWx0UHJldmVudGVkKCkpIHtcbiAgICAgICAgaWYgKHRoaXMuJCRwYXJzZUxpbmtVcmwoYWJzSHJlZiwgcmVsSHJlZikpIHtcbiAgICAgICAgICAvLyBXZSBkbyBhIHByZXZlbnREZWZhdWx0IGZvciBhbGwgdXJscyB0aGF0IGFyZSBwYXJ0IG9mIHRoZSBBbmd1bGFySlMgYXBwbGljYXRpb24sXG4gICAgICAgICAgLy8gaW4gaHRtbDVtb2RlIGFuZCBhbHNvIHdpdGhvdXQsIHNvIHRoYXQgd2UgYXJlIGFibGUgdG8gYWJvcnQgbmF2aWdhdGlvbiB3aXRob3V0XG4gICAgICAgICAgLy8gZ2V0dGluZyBkb3VibGUgZW50cmllcyBpbiB0aGUgbG9jYXRpb24gaGlzdG9yeS5cbiAgICAgICAgICBldmVudC5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICAgIC8vIHVwZGF0ZSBsb2NhdGlvbiBtYW51YWxseVxuICAgICAgICAgIGlmICh0aGlzLmFic1VybCgpICE9PSB0aGlzLmJyb3dzZXJVcmwoKSkge1xuICAgICAgICAgICAgJHJvb3RTY29wZS4kYXBwbHkoKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9KTtcblxuICAgIHRoaXMubG9jYXRpb24ub25VcmxDaGFuZ2UoKG5ld1VybCwgbmV3U3RhdGUpID0+IHtcbiAgICAgIGxldCBvbGRVcmwgPSB0aGlzLmFic1VybCgpO1xuICAgICAgbGV0IG9sZFN0YXRlID0gdGhpcy4kJHN0YXRlO1xuICAgICAgdGhpcy4kJHBhcnNlKG5ld1VybCk7XG4gICAgICBuZXdVcmwgPSB0aGlzLmFic1VybCgpO1xuICAgICAgdGhpcy4kJHN0YXRlID0gbmV3U3RhdGU7XG4gICAgICBjb25zdCBkZWZhdWx0UHJldmVudGVkID1cbiAgICAgICAgICAkcm9vdFNjb3BlLiRicm9hZGNhc3QoJyRsb2NhdGlvbkNoYW5nZVN0YXJ0JywgbmV3VXJsLCBvbGRVcmwsIG5ld1N0YXRlLCBvbGRTdGF0ZSlcbiAgICAgICAgICAgICAgLmRlZmF1bHRQcmV2ZW50ZWQ7XG5cbiAgICAgIC8vIGlmIHRoZSBsb2NhdGlvbiB3YXMgY2hhbmdlZCBieSBhIGAkbG9jYXRpb25DaGFuZ2VTdGFydGAgaGFuZGxlciB0aGVuIHN0b3BcbiAgICAgIC8vIHByb2Nlc3NpbmcgdGhpcyBsb2NhdGlvbiBjaGFuZ2VcbiAgICAgIGlmICh0aGlzLmFic1VybCgpICE9PSBuZXdVcmwpIHJldHVybjtcblxuICAgICAgLy8gSWYgZGVmYXVsdCB3YXMgcHJldmVudGVkLCBzZXQgYmFjayB0byBvbGQgc3RhdGUuIFRoaXMgaXMgdGhlIHN0YXRlIHRoYXQgd2FzIGxvY2FsbHlcbiAgICAgIC8vIGNhY2hlZCBpbiB0aGUgJGxvY2F0aW9uIHNlcnZpY2UuXG4gICAgICBpZiAoZGVmYXVsdFByZXZlbnRlZCkge1xuICAgICAgICB0aGlzLiQkcGFyc2Uob2xkVXJsKTtcbiAgICAgICAgdGhpcy5zdGF0ZShvbGRTdGF0ZSk7XG4gICAgICAgIHRoaXMuc2V0QnJvd3NlclVybFdpdGhGYWxsYmFjayhvbGRVcmwsIGZhbHNlLCBvbGRTdGF0ZSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLmluaXRhbGl6aW5nID0gZmFsc2U7XG4gICAgICAgICRyb290U2NvcGUuJGJyb2FkY2FzdCgnJGxvY2F0aW9uQ2hhbmdlU3VjY2VzcycsIG5ld1VybCwgb2xkVXJsLCBuZXdTdGF0ZSwgb2xkU3RhdGUpO1xuICAgICAgICB0aGlzLnJlc2V0QnJvd3NlclVwZGF0ZSgpO1xuICAgICAgfVxuICAgICAgaWYgKCEkcm9vdFNjb3BlLiQkcGhhc2UpIHtcbiAgICAgICAgJHJvb3RTY29wZS4kZGlnZXN0KCk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICAvLyB1cGRhdGUgYnJvd3NlclxuICAgICRyb290U2NvcGUuJHdhdGNoKCgpID0+IHtcbiAgICAgIGlmICh0aGlzLmluaXRhbGl6aW5nIHx8IHRoaXMudXBkYXRlQnJvd3Nlcikge1xuICAgICAgICB0aGlzLnVwZGF0ZUJyb3dzZXIgPSBmYWxzZTtcblxuICAgICAgICBjb25zdCBvbGRVcmwgPSB0aGlzLmJyb3dzZXJVcmwoKTtcbiAgICAgICAgY29uc3QgbmV3VXJsID0gdGhpcy5hYnNVcmwoKTtcbiAgICAgICAgY29uc3Qgb2xkU3RhdGUgPSB0aGlzLmJyb3dzZXJTdGF0ZSgpO1xuICAgICAgICBsZXQgY3VycmVudFJlcGxhY2UgPSB0aGlzLiQkcmVwbGFjZTtcblxuICAgICAgICBjb25zdCB1cmxPclN0YXRlQ2hhbmdlZCA9XG4gICAgICAgICAgICAhdGhpcy51cmxDb2RlYy5hcmVFcXVhbChvbGRVcmwsIG5ld1VybCkgfHwgb2xkU3RhdGUgIT09IHRoaXMuJCRzdGF0ZTtcblxuICAgICAgICAvLyBGaXJlIGxvY2F0aW9uIGNoYW5nZXMgb25lIHRpbWUgdG8gb24gaW5pdGlhbGl6YXRpb24uIFRoaXMgbXVzdCBiZSBkb25lIG9uIHRoZVxuICAgICAgICAvLyBuZXh0IHRpY2sgKHRodXMgaW5zaWRlICRldmFsQXN5bmMoKSkgaW4gb3JkZXIgZm9yIGxpc3RlbmVycyB0byBiZSByZWdpc3RlcmVkXG4gICAgICAgIC8vIGJlZm9yZSB0aGUgZXZlbnQgZmlyZXMuIE1pbWljaW5nIGJlaGF2aW9yIGZyb20gJGxvY2F0aW9uV2F0Y2g6XG4gICAgICAgIC8vIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIuanMvYmxvYi9tYXN0ZXIvc3JjL25nL2xvY2F0aW9uLmpzI0w5ODNcbiAgICAgICAgaWYgKHRoaXMuaW5pdGFsaXppbmcgfHwgdXJsT3JTdGF0ZUNoYW5nZWQpIHtcbiAgICAgICAgICB0aGlzLmluaXRhbGl6aW5nID0gZmFsc2U7XG5cbiAgICAgICAgICAkcm9vdFNjb3BlLiRldmFsQXN5bmMoKCkgPT4ge1xuICAgICAgICAgICAgLy8gR2V0IHRoZSBuZXcgVVJMIGFnYWluIHNpbmNlIGl0IGNvdWxkIGhhdmUgY2hhbmdlZCBkdWUgdG8gYXN5bmMgdXBkYXRlXG4gICAgICAgICAgICBjb25zdCBuZXdVcmwgPSB0aGlzLmFic1VybCgpO1xuICAgICAgICAgICAgY29uc3QgZGVmYXVsdFByZXZlbnRlZCA9XG4gICAgICAgICAgICAgICAgJHJvb3RTY29wZVxuICAgICAgICAgICAgICAgICAgICAuJGJyb2FkY2FzdCgnJGxvY2F0aW9uQ2hhbmdlU3RhcnQnLCBuZXdVcmwsIG9sZFVybCwgdGhpcy4kJHN0YXRlLCBvbGRTdGF0ZSlcbiAgICAgICAgICAgICAgICAgICAgLmRlZmF1bHRQcmV2ZW50ZWQ7XG5cbiAgICAgICAgICAgIC8vIGlmIHRoZSBsb2NhdGlvbiB3YXMgY2hhbmdlZCBieSBhIGAkbG9jYXRpb25DaGFuZ2VTdGFydGAgaGFuZGxlciB0aGVuIHN0b3BcbiAgICAgICAgICAgIC8vIHByb2Nlc3NpbmcgdGhpcyBsb2NhdGlvbiBjaGFuZ2VcbiAgICAgICAgICAgIGlmICh0aGlzLmFic1VybCgpICE9PSBuZXdVcmwpIHJldHVybjtcblxuICAgICAgICAgICAgaWYgKGRlZmF1bHRQcmV2ZW50ZWQpIHtcbiAgICAgICAgICAgICAgdGhpcy4kJHBhcnNlKG9sZFVybCk7XG4gICAgICAgICAgICAgIHRoaXMuJCRzdGF0ZSA9IG9sZFN0YXRlO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgLy8gVGhpcyBibG9jayBkb2Vzbid0IHJ1biB3aGVuIGluaXRhbGl6aW5nIGJlY2F1c2UgaXQncyBnb2luZyB0byBwZXJmb3JtIHRoZSB1cGRhdGUgdG9cbiAgICAgICAgICAgICAgLy8gdGhlIFVSTCB3aGljaCBzaG91bGRuJ3QgYmUgbmVlZGVkIHdoZW4gaW5pdGFsaXppbmcuXG4gICAgICAgICAgICAgIGlmICh1cmxPclN0YXRlQ2hhbmdlZCkge1xuICAgICAgICAgICAgICAgIHRoaXMuc2V0QnJvd3NlclVybFdpdGhGYWxsYmFjayhcbiAgICAgICAgICAgICAgICAgICAgbmV3VXJsLCBjdXJyZW50UmVwbGFjZSwgb2xkU3RhdGUgPT09IHRoaXMuJCRzdGF0ZSA/IG51bGwgOiB0aGlzLiQkc3RhdGUpO1xuICAgICAgICAgICAgICAgIHRoaXMuJCRyZXBsYWNlID0gZmFsc2U7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgJHJvb3RTY29wZS4kYnJvYWRjYXN0KFxuICAgICAgICAgICAgICAgICAgJyRsb2NhdGlvbkNoYW5nZVN1Y2Nlc3MnLCBuZXdVcmwsIG9sZFVybCwgdGhpcy4kJHN0YXRlLCBvbGRTdGF0ZSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHRoaXMuJCRyZXBsYWNlID0gZmFsc2U7XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIHJlc2V0QnJvd3NlclVwZGF0ZSgpIHtcbiAgICB0aGlzLiQkcmVwbGFjZSA9IGZhbHNlO1xuICAgIHRoaXMuJCRzdGF0ZSA9IHRoaXMuYnJvd3NlclN0YXRlKCk7XG4gICAgdGhpcy51cGRhdGVCcm93c2VyID0gZmFsc2U7XG4gICAgdGhpcy5sYXN0QnJvd3NlclVybCA9IHRoaXMuYnJvd3NlclVybCgpO1xuICB9XG5cbiAgcHJpdmF0ZSBsYXN0SGlzdG9yeVN0YXRlOiB1bmtub3duO1xuICBwcml2YXRlIGxhc3RCcm93c2VyVXJsOiBzdHJpbmcgPSAnJztcbiAgcHJpdmF0ZSBicm93c2VyVXJsKCk6IHN0cmluZztcbiAgcHJpdmF0ZSBicm93c2VyVXJsKHVybDogc3RyaW5nLCByZXBsYWNlPzogYm9vbGVhbiwgc3RhdGU/OiB1bmtub3duKTogdGhpcztcbiAgcHJpdmF0ZSBicm93c2VyVXJsKHVybD86IHN0cmluZywgcmVwbGFjZT86IGJvb2xlYW4sIHN0YXRlPzogdW5rbm93bikge1xuICAgIC8vIEluIG1vZGVybiBicm93c2VycyBgaGlzdG9yeS5zdGF0ZWAgaXMgYG51bGxgIGJ5IGRlZmF1bHQ7IHRyZWF0aW5nIGl0IHNlcGFyYXRlbHlcbiAgICAvLyBmcm9tIGB1bmRlZmluZWRgIHdvdWxkIGNhdXNlIGAkYnJvd3Nlci51cmwoJy9mb28nKWAgdG8gY2hhbmdlIGBoaXN0b3J5LnN0YXRlYFxuICAgIC8vIHRvIHVuZGVmaW5lZCB2aWEgYHB1c2hTdGF0ZWAuIEluc3RlYWQsIGxldCdzIGNoYW5nZSBgdW5kZWZpbmVkYCB0byBgbnVsbGAgaGVyZS5cbiAgICBpZiAodHlwZW9mIHN0YXRlID09PSAndW5kZWZpbmVkJykge1xuICAgICAgc3RhdGUgPSBudWxsO1xuICAgIH1cblxuICAgIC8vIHNldHRlclxuICAgIGlmICh1cmwpIHtcbiAgICAgIGxldCBzYW1lU3RhdGUgPSB0aGlzLmxhc3RIaXN0b3J5U3RhdGUgPT09IHN0YXRlO1xuXG4gICAgICAvLyBOb3JtYWxpemUgdGhlIGlucHV0dGVkIFVSTFxuICAgICAgdXJsID0gdGhpcy51cmxDb2RlYy5wYXJzZSh1cmwpLmhyZWY7XG5cbiAgICAgIC8vIERvbid0IGNoYW5nZSBhbnl0aGluZyBpZiBwcmV2aW91cyBhbmQgY3VycmVudCBVUkxzIGFuZCBzdGF0ZXMgbWF0Y2guXG4gICAgICBpZiAodGhpcy5sYXN0QnJvd3NlclVybCA9PT0gdXJsICYmIHNhbWVTdGF0ZSkge1xuICAgICAgICByZXR1cm4gdGhpcztcbiAgICAgIH1cbiAgICAgIHRoaXMubGFzdEJyb3dzZXJVcmwgPSB1cmw7XG4gICAgICB0aGlzLmxhc3RIaXN0b3J5U3RhdGUgPSBzdGF0ZTtcblxuICAgICAgLy8gUmVtb3ZlIHNlcnZlciBiYXNlIGZyb20gVVJMIGFzIHRoZSBBbmd1bGFyIEFQSXMgZm9yIHVwZGF0aW5nIFVSTCByZXF1aXJlXG4gICAgICAvLyBpdCB0byBiZSB0aGUgcGF0aCsuXG4gICAgICB1cmwgPSB0aGlzLnN0cmlwQmFzZVVybCh0aGlzLmdldFNlcnZlckJhc2UoKSwgdXJsKSB8fCB1cmw7XG5cbiAgICAgIC8vIFNldCB0aGUgVVJMXG4gICAgICBpZiAocmVwbGFjZSkge1xuICAgICAgICB0aGlzLmxvY2F0aW9uU3RyYXRlZ3kucmVwbGFjZVN0YXRlKHN0YXRlLCAnJywgdXJsLCAnJyk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLmxvY2F0aW9uU3RyYXRlZ3kucHVzaFN0YXRlKHN0YXRlLCAnJywgdXJsLCAnJyk7XG4gICAgICB9XG5cbiAgICAgIHRoaXMuY2FjaGVTdGF0ZSgpO1xuXG4gICAgICByZXR1cm4gdGhpcztcbiAgICAgIC8vIGdldHRlclxuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdGhpcy5wbGF0Zm9ybUxvY2F0aW9uLmhyZWY7XG4gICAgfVxuICB9XG5cbiAgLy8gVGhpcyB2YXJpYWJsZSBzaG91bGQgYmUgdXNlZCAqb25seSogaW5zaWRlIHRoZSBjYWNoZVN0YXRlIGZ1bmN0aW9uLlxuICBwcml2YXRlIGxhc3RDYWNoZWRTdGF0ZTogdW5rbm93biA9IG51bGw7XG4gIHByaXZhdGUgY2FjaGVTdGF0ZSgpIHtcbiAgICAvLyBUaGlzIHNob3VsZCBiZSB0aGUgb25seSBwbGFjZSBpbiAkYnJvd3NlciB3aGVyZSBgaGlzdG9yeS5zdGF0ZWAgaXMgcmVhZC5cbiAgICB0aGlzLmNhY2hlZFN0YXRlID0gdGhpcy5wbGF0Zm9ybUxvY2F0aW9uLmdldFN0YXRlKCk7XG4gICAgaWYgKHR5cGVvZiB0aGlzLmNhY2hlZFN0YXRlID09PSAndW5kZWZpbmVkJykge1xuICAgICAgdGhpcy5jYWNoZWRTdGF0ZSA9IG51bGw7XG4gICAgfVxuXG4gICAgLy8gUHJldmVudCBjYWxsYmFja3MgZm8gZmlyZSB0d2ljZSBpZiBib3RoIGhhc2hjaGFuZ2UgJiBwb3BzdGF0ZSB3ZXJlIGZpcmVkLlxuICAgIGlmIChkZWVwRXF1YWwodGhpcy5jYWNoZWRTdGF0ZSwgdGhpcy5sYXN0Q2FjaGVkU3RhdGUpKSB7XG4gICAgICB0aGlzLmNhY2hlZFN0YXRlID0gdGhpcy5sYXN0Q2FjaGVkU3RhdGU7XG4gICAgfVxuXG4gICAgdGhpcy5sYXN0Q2FjaGVkU3RhdGUgPSB0aGlzLmNhY2hlZFN0YXRlO1xuICAgIHRoaXMubGFzdEhpc3RvcnlTdGF0ZSA9IHRoaXMuY2FjaGVkU3RhdGU7XG4gIH1cblxuICAvKipcbiAgICogVGhpcyBmdW5jdGlvbiBlbXVsYXRlcyB0aGUgJGJyb3dzZXIuc3RhdGUoKSBmdW5jdGlvbiBmcm9tIEFuZ3VsYXJKUy4gSXQgd2lsbCBjYXVzZVxuICAgKiBoaXN0b3J5LnN0YXRlIHRvIGJlIGNhY2hlZCB1bmxlc3MgY2hhbmdlZCB3aXRoIGRlZXAgZXF1YWxpdHkgY2hlY2suXG4gICAqL1xuICBwcml2YXRlIGJyb3dzZXJTdGF0ZSgpOiB1bmtub3duIHsgcmV0dXJuIHRoaXMuY2FjaGVkU3RhdGU7IH1cblxuICBwcml2YXRlIHN0cmlwQmFzZVVybChiYXNlOiBzdHJpbmcsIHVybDogc3RyaW5nKSB7XG4gICAgaWYgKHVybC5zdGFydHNXaXRoKGJhc2UpKSB7XG4gICAgICByZXR1cm4gdXJsLnN1YnN0cihiYXNlLmxlbmd0aCk7XG4gICAgfVxuICAgIHJldHVybiB1bmRlZmluZWQ7XG4gIH1cblxuICBwcml2YXRlIGdldFNlcnZlckJhc2UoKSB7XG4gICAgY29uc3Qge3Byb3RvY29sLCBob3N0bmFtZSwgcG9ydH0gPSB0aGlzLnBsYXRmb3JtTG9jYXRpb247XG4gICAgY29uc3QgYmFzZUhyZWYgPSB0aGlzLmxvY2F0aW9uU3RyYXRlZ3kuZ2V0QmFzZUhyZWYoKTtcbiAgICBsZXQgdXJsID0gYCR7cHJvdG9jb2x9Ly8ke2hvc3RuYW1lfSR7cG9ydCA/ICc6JyArIHBvcnQgOiAnJ30ke2Jhc2VIcmVmIHx8ICcvJ31gO1xuICAgIHJldHVybiB1cmwuZW5kc1dpdGgoJy8nKSA/IHVybCA6IHVybCArICcvJztcbiAgfVxuXG4gIHByaXZhdGUgcGFyc2VBcHBVcmwodXJsOiBzdHJpbmcpIHtcbiAgICBpZiAoRE9VQkxFX1NMQVNIX1JFR0VYLnRlc3QodXJsKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBCYWQgUGF0aCAtIFVSTCBjYW5ub3Qgc3RhcnQgd2l0aCBkb3VibGUgc2xhc2hlczogJHt1cmx9YCk7XG4gICAgfVxuXG4gICAgbGV0IHByZWZpeGVkID0gKHVybC5jaGFyQXQoMCkgIT09ICcvJyk7XG4gICAgaWYgKHByZWZpeGVkKSB7XG4gICAgICB1cmwgPSAnLycgKyB1cmw7XG4gICAgfVxuICAgIGxldCBtYXRjaCA9IHRoaXMudXJsQ29kZWMucGFyc2UodXJsLCB0aGlzLmdldFNlcnZlckJhc2UoKSk7XG4gICAgaWYgKHR5cGVvZiBtYXRjaCA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgQmFkIFVSTCAtIENhbm5vdCBwYXJzZSBVUkw6ICR7dXJsfWApO1xuICAgIH1cbiAgICBsZXQgcGF0aCA9XG4gICAgICAgIHByZWZpeGVkICYmIG1hdGNoLnBhdGhuYW1lLmNoYXJBdCgwKSA9PT0gJy8nID8gbWF0Y2gucGF0aG5hbWUuc3Vic3RyaW5nKDEpIDogbWF0Y2gucGF0aG5hbWU7XG4gICAgdGhpcy4kJHBhdGggPSB0aGlzLnVybENvZGVjLmRlY29kZVBhdGgocGF0aCk7XG4gICAgdGhpcy4kJHNlYXJjaCA9IHRoaXMudXJsQ29kZWMuZGVjb2RlU2VhcmNoKG1hdGNoLnNlYXJjaCk7XG4gICAgdGhpcy4kJGhhc2ggPSB0aGlzLnVybENvZGVjLmRlY29kZUhhc2gobWF0Y2guaGFzaCk7XG5cbiAgICAvLyBtYWtlIHN1cmUgcGF0aCBzdGFydHMgd2l0aCAnLyc7XG4gICAgaWYgKHRoaXMuJCRwYXRoICYmIHRoaXMuJCRwYXRoLmNoYXJBdCgwKSAhPT0gJy8nKSB7XG4gICAgICB0aGlzLiQkcGF0aCA9ICcvJyArIHRoaXMuJCRwYXRoO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgbGlzdGVuZXJzIGZvciBVUkwgY2hhbmdlcy4gVGhpcyBBUEkgaXMgdXNlZCB0byBjYXRjaCB1cGRhdGVzIHBlcmZvcm1lZCBieSB0aGVcbiAgICogQW5ndWxhckpTIGZyYW1ld29yay4gVGhlc2UgY2hhbmdlcyBhcmUgYSBzdWJzZXQgb2YgdGhlIGAkbG9jYXRpb25DaGFuZ2VTdGFydGAgYW5kXG4gICAqIGAkbG9jYXRpb25DaGFuZ2VTdWNjZXNzYCBldmVudHMgd2hpY2ggZmlyZSB3aGVuIEFuZ3VsYXJKUyB1cGRhdGVzIGl0cyBpbnRlcm5hbGx5LXJlZmVyZW5jZWRcbiAgICogdmVyc2lvbiBvZiB0aGUgYnJvd3NlciBVUkwuXG4gICAqXG4gICAqIEl0J3MgcG9zc2libGUgZm9yIGAkbG9jYXRpb25DaGFuZ2VgIGV2ZW50cyB0byBoYXBwZW4sIGJ1dCBmb3IgdGhlIGJyb3dzZXIgVVJMXG4gICAqICh3aW5kb3cubG9jYXRpb24pIHRvIHJlbWFpbiB1bmNoYW5nZWQuIFRoaXMgYG9uQ2hhbmdlYCBjYWxsYmFjayB3aWxsIGZpcmUgb25seSB3aGVuIEFuZ3VsYXJKU1xuICAgKiBhY3R1YWxseSB1cGRhdGVzIHRoZSBicm93c2VyIFVSTCAod2luZG93LmxvY2F0aW9uKS5cbiAgICpcbiAgICogQHBhcmFtIGZuIFRoZSBjYWxsYmFjayBmdW5jdGlvbiB0aGF0IGlzIHRyaWdnZXJlZCBmb3IgdGhlIGxpc3RlbmVyIHdoZW4gdGhlIFVSTCBjaGFuZ2VzLlxuICAgKiBAcGFyYW0gZXJyIFRoZSBjYWxsYmFjayBmdW5jdGlvbiB0aGF0IGlzIHRyaWdnZXJlZCB3aGVuIGFuIGVycm9yIG9jY3Vycy5cbiAgICovXG4gIG9uQ2hhbmdlKFxuICAgICAgZm46ICh1cmw6IHN0cmluZywgc3RhdGU6IHVua25vd24sIG9sZFVybDogc3RyaW5nLCBvbGRTdGF0ZTogdW5rbm93bikgPT4gdm9pZCxcbiAgICAgIGVycjogKGU6IEVycm9yKSA9PiB2b2lkID0gKGU6IEVycm9yKSA9PiB7fSkge1xuICAgIHRoaXMuJCRjaGFuZ2VMaXN0ZW5lcnMucHVzaChbZm4sIGVycl0pO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICAkJG5vdGlmeUNoYW5nZUxpc3RlbmVycyhcbiAgICAgIHVybDogc3RyaW5nID0gJycsIHN0YXRlOiB1bmtub3duLCBvbGRVcmw6IHN0cmluZyA9ICcnLCBvbGRTdGF0ZTogdW5rbm93bikge1xuICAgIHRoaXMuJCRjaGFuZ2VMaXN0ZW5lcnMuZm9yRWFjaCgoW2ZuLCBlcnJdKSA9PiB7XG4gICAgICB0cnkge1xuICAgICAgICBmbih1cmwsIHN0YXRlLCBvbGRVcmwsIG9sZFN0YXRlKTtcbiAgICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgZXJyKGUpO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgLyoqXG4gICAqIFBhcnNlcyB0aGUgcHJvdmlkZWQgVVJMLCBhbmQgc2V0cyB0aGUgY3VycmVudCBVUkwgdG8gdGhlIHBhcnNlZCByZXN1bHQuXG4gICAqXG4gICAqIEBwYXJhbSB1cmwgVGhlIFVSTCBzdHJpbmcuXG4gICAqL1xuICAkJHBhcnNlKHVybDogc3RyaW5nKSB7XG4gICAgbGV0IHBhdGhVcmw6IHN0cmluZ3x1bmRlZmluZWQ7XG4gICAgaWYgKHVybC5zdGFydHNXaXRoKCcvJykpIHtcbiAgICAgIHBhdGhVcmwgPSB1cmw7XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIFJlbW92ZSBwcm90b2NvbCAmIGhvc3RuYW1lIGlmIFVSTCBzdGFydHMgd2l0aCBpdFxuICAgICAgcGF0aFVybCA9IHRoaXMuc3RyaXBCYXNlVXJsKHRoaXMuZ2V0U2VydmVyQmFzZSgpLCB1cmwpO1xuICAgIH1cbiAgICBpZiAodHlwZW9mIHBhdGhVcmwgPT09ICd1bmRlZmluZWQnKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYEludmFsaWQgdXJsIFwiJHt1cmx9XCIsIG1pc3NpbmcgcGF0aCBwcmVmaXggXCIke3RoaXMuZ2V0U2VydmVyQmFzZSgpfVwiLmApO1xuICAgIH1cblxuICAgIHRoaXMucGFyc2VBcHBVcmwocGF0aFVybCk7XG5cbiAgICBpZiAoIXRoaXMuJCRwYXRoKSB7XG4gICAgICB0aGlzLiQkcGF0aCA9ICcvJztcbiAgICB9XG4gICAgdGhpcy5jb21wb3NlVXJscygpO1xuICB9XG5cbiAgLyoqXG4gICAqIFBhcnNlcyB0aGUgcHJvdmlkZWQgVVJMIGFuZCBpdHMgcmVsYXRpdmUgVVJMLlxuICAgKlxuICAgKiBAcGFyYW0gdXJsIFRoZSBmdWxsIFVSTCBzdHJpbmcuXG4gICAqIEBwYXJhbSByZWxIcmVmIEEgVVJMIHN0cmluZyByZWxhdGl2ZSB0byB0aGUgZnVsbCBVUkwgc3RyaW5nLlxuICAgKi9cbiAgJCRwYXJzZUxpbmtVcmwodXJsOiBzdHJpbmcsIHJlbEhyZWY/OiBzdHJpbmd8bnVsbCk6IGJvb2xlYW4ge1xuICAgIC8vIFdoZW4gcmVsSHJlZiBpcyBwYXNzZWQsIGl0IHNob3VsZCBiZSBhIGhhc2ggYW5kIGlzIGhhbmRsZWQgc2VwYXJhdGVseVxuICAgIGlmIChyZWxIcmVmICYmIHJlbEhyZWZbMF0gPT09ICcjJykge1xuICAgICAgdGhpcy5oYXNoKHJlbEhyZWYuc2xpY2UoMSkpO1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfVxuICAgIGxldCByZXdyaXR0ZW5Vcmw7XG4gICAgbGV0IGFwcFVybCA9IHRoaXMuc3RyaXBCYXNlVXJsKHRoaXMuZ2V0U2VydmVyQmFzZSgpLCB1cmwpO1xuICAgIGlmICh0eXBlb2YgYXBwVXJsICE9PSAndW5kZWZpbmVkJykge1xuICAgICAgcmV3cml0dGVuVXJsID0gdGhpcy5nZXRTZXJ2ZXJCYXNlKCkgKyBhcHBVcmw7XG4gICAgfSBlbHNlIGlmICh0aGlzLmdldFNlcnZlckJhc2UoKSA9PT0gdXJsICsgJy8nKSB7XG4gICAgICByZXdyaXR0ZW5VcmwgPSB0aGlzLmdldFNlcnZlckJhc2UoKTtcbiAgICB9XG4gICAgLy8gU2V0IHRoZSBVUkxcbiAgICBpZiAocmV3cml0dGVuVXJsKSB7XG4gICAgICB0aGlzLiQkcGFyc2UocmV3cml0dGVuVXJsKTtcbiAgICB9XG4gICAgcmV0dXJuICEhcmV3cml0dGVuVXJsO1xuICB9XG5cbiAgcHJpdmF0ZSBzZXRCcm93c2VyVXJsV2l0aEZhbGxiYWNrKHVybDogc3RyaW5nLCByZXBsYWNlOiBib29sZWFuLCBzdGF0ZTogdW5rbm93bikge1xuICAgIGNvbnN0IG9sZFVybCA9IHRoaXMudXJsKCk7XG4gICAgY29uc3Qgb2xkU3RhdGUgPSB0aGlzLiQkc3RhdGU7XG4gICAgdHJ5IHtcbiAgICAgIHRoaXMuYnJvd3NlclVybCh1cmwsIHJlcGxhY2UsIHN0YXRlKTtcblxuICAgICAgLy8gTWFrZSBzdXJlICRsb2NhdGlvbi5zdGF0ZSgpIHJldHVybnMgcmVmZXJlbnRpYWxseSBpZGVudGljYWwgKG5vdCBqdXN0IGRlZXBseSBlcXVhbClcbiAgICAgIC8vIHN0YXRlIG9iamVjdDsgdGhpcyBtYWtlcyBwb3NzaWJsZSBxdWljayBjaGVja2luZyBpZiB0aGUgc3RhdGUgY2hhbmdlZCBpbiB0aGUgZGlnZXN0XG4gICAgICAvLyBsb29wLiBDaGVja2luZyBkZWVwIGVxdWFsaXR5IHdvdWxkIGJlIHRvbyBleHBlbnNpdmUuXG4gICAgICB0aGlzLiQkc3RhdGUgPSB0aGlzLmJyb3dzZXJTdGF0ZSgpO1xuICAgICAgdGhpcy4kJG5vdGlmeUNoYW5nZUxpc3RlbmVycyh1cmwsIHN0YXRlLCBvbGRVcmwsIG9sZFN0YXRlKTtcbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICAvLyBSZXN0b3JlIG9sZCB2YWx1ZXMgaWYgcHVzaFN0YXRlIGZhaWxzXG4gICAgICB0aGlzLnVybChvbGRVcmwpO1xuICAgICAgdGhpcy4kJHN0YXRlID0gb2xkU3RhdGU7XG5cbiAgICAgIHRocm93IGU7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjb21wb3NlVXJscygpIHtcbiAgICB0aGlzLiQkdXJsID0gdGhpcy51cmxDb2RlYy5ub3JtYWxpemUodGhpcy4kJHBhdGgsIHRoaXMuJCRzZWFyY2gsIHRoaXMuJCRoYXNoKTtcbiAgICB0aGlzLiQkYWJzVXJsID0gdGhpcy5nZXRTZXJ2ZXJCYXNlKCkgKyB0aGlzLiQkdXJsLnN1YnN0cigxKTsgIC8vIHJlbW92ZSAnLycgZnJvbSBmcm9udCBvZiBVUkxcbiAgICB0aGlzLnVwZGF0ZUJyb3dzZXIgPSB0cnVlO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyB0aGUgZnVsbCBVUkwgcmVwcmVzZW50YXRpb24gd2l0aCBhbGwgc2VnbWVudHMgZW5jb2RlZCBhY2NvcmRpbmcgdG9cbiAgICogcnVsZXMgc3BlY2lmaWVkIGluXG4gICAqIFtSRkMgMzk4Nl0oaHR0cDovL3d3dy5pZXRmLm9yZy9yZmMvcmZjMzk4Ni50eHQpLlxuICAgKlxuICAgKlxuICAgKiBgYGBqc1xuICAgKiAvLyBnaXZlbiBVUkwgaHR0cDovL2V4YW1wbGUuY29tLyMvc29tZS9wYXRoP2Zvbz1iYXImYmF6PXhveG9cbiAgICogbGV0IGFic1VybCA9ICRsb2NhdGlvbi5hYnNVcmwoKTtcbiAgICogLy8gPT4gXCJodHRwOi8vZXhhbXBsZS5jb20vIy9zb21lL3BhdGg/Zm9vPWJhciZiYXo9eG94b1wiXG4gICAqIGBgYFxuICAgKi9cbiAgYWJzVXJsKCk6IHN0cmluZyB7IHJldHVybiB0aGlzLiQkYWJzVXJsOyB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyB0aGUgY3VycmVudCBVUkwsIG9yIHNldHMgYSBuZXcgVVJMLiBXaGVuIHNldHRpbmcgYSBVUkwsXG4gICAqIGNoYW5nZXMgdGhlIHBhdGgsIHNlYXJjaCwgYW5kIGhhc2gsIGFuZCByZXR1cm5zIGEgcmVmZXJlbmNlIHRvIGl0cyBvd24gaW5zdGFuY2UuXG4gICAqXG4gICAqIGBgYGpzXG4gICAqIC8vIGdpdmVuIFVSTCBodHRwOi8vZXhhbXBsZS5jb20vIy9zb21lL3BhdGg/Zm9vPWJhciZiYXo9eG94b1xuICAgKiBsZXQgdXJsID0gJGxvY2F0aW9uLnVybCgpO1xuICAgKiAvLyA9PiBcIi9zb21lL3BhdGg/Zm9vPWJhciZiYXo9eG94b1wiXG4gICAqIGBgYFxuICAgKi9cbiAgdXJsKCk6IHN0cmluZztcbiAgdXJsKHVybDogc3RyaW5nKTogdGhpcztcbiAgdXJsKHVybD86IHN0cmluZyk6IHN0cmluZ3x0aGlzIHtcbiAgICBpZiAodHlwZW9mIHVybCA9PT0gJ3N0cmluZycpIHtcbiAgICAgIGlmICghdXJsLmxlbmd0aCkge1xuICAgICAgICB1cmwgPSAnLyc7XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IG1hdGNoID0gUEFUSF9NQVRDSC5leGVjKHVybCk7XG4gICAgICBpZiAoIW1hdGNoKSByZXR1cm4gdGhpcztcbiAgICAgIGlmIChtYXRjaFsxXSB8fCB1cmwgPT09ICcnKSB0aGlzLnBhdGgodGhpcy51cmxDb2RlYy5kZWNvZGVQYXRoKG1hdGNoWzFdKSk7XG4gICAgICBpZiAobWF0Y2hbMl0gfHwgbWF0Y2hbMV0gfHwgdXJsID09PSAnJykgdGhpcy5zZWFyY2gobWF0Y2hbM10gfHwgJycpO1xuICAgICAgdGhpcy5oYXNoKG1hdGNoWzVdIHx8ICcnKTtcblxuICAgICAgLy8gQ2hhaW5hYmxlIG1ldGhvZFxuICAgICAgcmV0dXJuIHRoaXM7XG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMuJCR1cmw7XG4gIH1cblxuICAvKipcbiAgICogUmV0cmlldmVzIHRoZSBwcm90b2NvbCBvZiB0aGUgY3VycmVudCBVUkwuXG4gICAqXG4gICAqIGBgYGpzXG4gICAqIC8vIGdpdmVuIFVSTCBodHRwOi8vZXhhbXBsZS5jb20vIy9zb21lL3BhdGg/Zm9vPWJhciZiYXo9eG94b1xuICAgKiBsZXQgcHJvdG9jb2wgPSAkbG9jYXRpb24ucHJvdG9jb2woKTtcbiAgICogLy8gPT4gXCJodHRwXCJcbiAgICogYGBgXG4gICAqL1xuICBwcm90b2NvbCgpOiBzdHJpbmcgeyByZXR1cm4gdGhpcy4kJHByb3RvY29sOyB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyB0aGUgcHJvdG9jb2wgb2YgdGhlIGN1cnJlbnQgVVJMLlxuICAgKlxuICAgKiBJbiBjb250cmFzdCB0byB0aGUgbm9uLUFuZ3VsYXJKUyB2ZXJzaW9uIGBsb2NhdGlvbi5ob3N0YCB3aGljaCByZXR1cm5zIGBob3N0bmFtZTpwb3J0YCwgdGhpc1xuICAgKiByZXR1cm5zIHRoZSBgaG9zdG5hbWVgIHBvcnRpb24gb25seS5cbiAgICpcbiAgICpcbiAgICogYGBganNcbiAgICogLy8gZ2l2ZW4gVVJMIGh0dHA6Ly9leGFtcGxlLmNvbS8jL3NvbWUvcGF0aD9mb289YmFyJmJhej14b3hvXG4gICAqIGxldCBob3N0ID0gJGxvY2F0aW9uLmhvc3QoKTtcbiAgICogLy8gPT4gXCJleGFtcGxlLmNvbVwiXG4gICAqXG4gICAqIC8vIGdpdmVuIFVSTCBodHRwOi8vdXNlcjpwYXNzd29yZEBleGFtcGxlLmNvbTo4MDgwLyMvc29tZS9wYXRoP2Zvbz1iYXImYmF6PXhveG9cbiAgICogaG9zdCA9ICRsb2NhdGlvbi5ob3N0KCk7XG4gICAqIC8vID0+IFwiZXhhbXBsZS5jb21cIlxuICAgKiBob3N0ID0gbG9jYXRpb24uaG9zdDtcbiAgICogLy8gPT4gXCJleGFtcGxlLmNvbTo4MDgwXCJcbiAgICogYGBgXG4gICAqL1xuICBob3N0KCk6IHN0cmluZyB7IHJldHVybiB0aGlzLiQkaG9zdDsgfVxuXG4gIC8qKlxuICAgKiBSZXRyaWV2ZXMgdGhlIHBvcnQgb2YgdGhlIGN1cnJlbnQgVVJMLlxuICAgKlxuICAgKiBgYGBqc1xuICAgKiAvLyBnaXZlbiBVUkwgaHR0cDovL2V4YW1wbGUuY29tLyMvc29tZS9wYXRoP2Zvbz1iYXImYmF6PXhveG9cbiAgICogbGV0IHBvcnQgPSAkbG9jYXRpb24ucG9ydCgpO1xuICAgKiAvLyA9PiA4MFxuICAgKiBgYGBcbiAgICovXG4gIHBvcnQoKTogbnVtYmVyfG51bGwgeyByZXR1cm4gdGhpcy4kJHBvcnQ7IH1cblxuICAvKipcbiAgICogUmV0cmlldmVzIHRoZSBwYXRoIG9mIHRoZSBjdXJyZW50IFVSTCwgb3IgY2hhbmdlcyB0aGUgcGF0aCBhbmQgcmV0dXJucyBhIHJlZmVyZW5jZSB0byBpdHMgb3duXG4gICAqIGluc3RhbmNlLlxuICAgKlxuICAgKiBQYXRocyBzaG91bGQgYWx3YXlzIGJlZ2luIHdpdGggZm9yd2FyZCBzbGFzaCAoLykuIFRoaXMgbWV0aG9kIGFkZHMgdGhlIGZvcndhcmQgc2xhc2hcbiAgICogaWYgaXQgaXMgbWlzc2luZy5cbiAgICpcbiAgICogYGBganNcbiAgICogLy8gZ2l2ZW4gVVJMIGh0dHA6Ly9leGFtcGxlLmNvbS8jL3NvbWUvcGF0aD9mb289YmFyJmJhej14b3hvXG4gICAqIGxldCBwYXRoID0gJGxvY2F0aW9uLnBhdGgoKTtcbiAgICogLy8gPT4gXCIvc29tZS9wYXRoXCJcbiAgICogYGBgXG4gICAqL1xuICBwYXRoKCk6IHN0cmluZztcbiAgcGF0aChwYXRoOiBzdHJpbmd8bnVtYmVyfG51bGwpOiB0aGlzO1xuICBwYXRoKHBhdGg/OiBzdHJpbmd8bnVtYmVyfG51bGwpOiBzdHJpbmd8dGhpcyB7XG4gICAgaWYgKHR5cGVvZiBwYXRoID09PSAndW5kZWZpbmVkJykge1xuICAgICAgcmV0dXJuIHRoaXMuJCRwYXRoO1xuICAgIH1cblxuICAgIC8vIG51bGwgcGF0aCBjb252ZXJ0cyB0byBlbXB0eSBzdHJpbmcuIFByZXBlbmQgd2l0aCBcIi9cIiBpZiBuZWVkZWQuXG4gICAgcGF0aCA9IHBhdGggIT09IG51bGwgPyBwYXRoLnRvU3RyaW5nKCkgOiAnJztcbiAgICBwYXRoID0gcGF0aC5jaGFyQXQoMCkgPT09ICcvJyA/IHBhdGggOiAnLycgKyBwYXRoO1xuXG4gICAgdGhpcy4kJHBhdGggPSBwYXRoO1xuXG4gICAgdGhpcy5jb21wb3NlVXJscygpO1xuICAgIHJldHVybiB0aGlzO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyBhIG1hcCBvZiB0aGUgc2VhcmNoIHBhcmFtZXRlcnMgb2YgdGhlIGN1cnJlbnQgVVJMLCBvciBjaGFuZ2VzIGEgc2VhcmNoIFxuICAgKiBwYXJ0IGFuZCByZXR1cm5zIGEgcmVmZXJlbmNlIHRvIGl0cyBvd24gaW5zdGFuY2UuXG4gICAqXG4gICAqXG4gICAqIGBgYGpzXG4gICAqIC8vIGdpdmVuIFVSTCBodHRwOi8vZXhhbXBsZS5jb20vIy9zb21lL3BhdGg/Zm9vPWJhciZiYXo9eG94b1xuICAgKiBsZXQgc2VhcmNoT2JqZWN0ID0gJGxvY2F0aW9uLnNlYXJjaCgpO1xuICAgKiAvLyA9PiB7Zm9vOiAnYmFyJywgYmF6OiAneG94byd9XG4gICAqXG4gICAqIC8vIHNldCBmb28gdG8gJ3lpcGVlJ1xuICAgKiAkbG9jYXRpb24uc2VhcmNoKCdmb28nLCAneWlwZWUnKTtcbiAgICogLy8gJGxvY2F0aW9uLnNlYXJjaCgpID0+IHtmb286ICd5aXBlZScsIGJhejogJ3hveG8nfVxuICAgKiBgYGBcbiAgICpcbiAgICogQHBhcmFtIHtzdHJpbmd8T2JqZWN0LjxzdHJpbmc+fE9iamVjdC48QXJyYXkuPHN0cmluZz4+fSBzZWFyY2ggTmV3IHNlYXJjaCBwYXJhbXMgLSBzdHJpbmcgb3JcbiAgICogaGFzaCBvYmplY3QuXG4gICAqXG4gICAqIFdoZW4gY2FsbGVkIHdpdGggYSBzaW5nbGUgYXJndW1lbnQgdGhlIG1ldGhvZCBhY3RzIGFzIGEgc2V0dGVyLCBzZXR0aW5nIHRoZSBgc2VhcmNoYCBjb21wb25lbnRcbiAgICogb2YgYCRsb2NhdGlvbmAgdG8gdGhlIHNwZWNpZmllZCB2YWx1ZS5cbiAgICpcbiAgICogSWYgdGhlIGFyZ3VtZW50IGlzIGEgaGFzaCBvYmplY3QgY29udGFpbmluZyBhbiBhcnJheSBvZiB2YWx1ZXMsIHRoZXNlIHZhbHVlcyB3aWxsIGJlIGVuY29kZWRcbiAgICogYXMgZHVwbGljYXRlIHNlYXJjaCBwYXJhbWV0ZXJzIGluIHRoZSBVUkwuXG4gICAqXG4gICAqIEBwYXJhbSB7KHN0cmluZ3xOdW1iZXJ8QXJyYXk8c3RyaW5nPnxib29sZWFuKT19IHBhcmFtVmFsdWUgSWYgYHNlYXJjaGAgaXMgYSBzdHJpbmcgb3IgbnVtYmVyLCB0aGVuIGBwYXJhbVZhbHVlYFxuICAgKiB3aWxsIG92ZXJyaWRlIG9ubHkgYSBzaW5nbGUgc2VhcmNoIHByb3BlcnR5LlxuICAgKlxuICAgKiBJZiBgcGFyYW1WYWx1ZWAgaXMgYW4gYXJyYXksIGl0IHdpbGwgb3ZlcnJpZGUgdGhlIHByb3BlcnR5IG9mIHRoZSBgc2VhcmNoYCBjb21wb25lbnQgb2ZcbiAgICogYCRsb2NhdGlvbmAgc3BlY2lmaWVkIHZpYSB0aGUgZmlyc3QgYXJndW1lbnQuXG4gICAqXG4gICAqIElmIGBwYXJhbVZhbHVlYCBpcyBgbnVsbGAsIHRoZSBwcm9wZXJ0eSBzcGVjaWZpZWQgdmlhIHRoZSBmaXJzdCBhcmd1bWVudCB3aWxsIGJlIGRlbGV0ZWQuXG4gICAqXG4gICAqIElmIGBwYXJhbVZhbHVlYCBpcyBgdHJ1ZWAsIHRoZSBwcm9wZXJ0eSBzcGVjaWZpZWQgdmlhIHRoZSBmaXJzdCBhcmd1bWVudCB3aWxsIGJlIGFkZGVkIHdpdGggbm9cbiAgICogdmFsdWUgbm9yIHRyYWlsaW5nIGVxdWFsIHNpZ24uXG4gICAqXG4gICAqIEByZXR1cm4ge09iamVjdH0gVGhlIHBhcnNlZCBgc2VhcmNoYCBvYmplY3Qgb2YgdGhlIGN1cnJlbnQgVVJMLCBvciB0aGUgY2hhbmdlZCBgc2VhcmNoYCBvYmplY3QuXG4gICAqL1xuICBzZWFyY2goKToge1trZXk6IHN0cmluZ106IHVua25vd259O1xuICBzZWFyY2goc2VhcmNoOiBzdHJpbmd8bnVtYmVyfHtba2V5OiBzdHJpbmddOiB1bmtub3dufSk6IHRoaXM7XG4gIHNlYXJjaChcbiAgICAgIHNlYXJjaDogc3RyaW5nfG51bWJlcnx7W2tleTogc3RyaW5nXTogdW5rbm93bn0sXG4gICAgICBwYXJhbVZhbHVlOiBudWxsfHVuZGVmaW5lZHxzdHJpbmd8bnVtYmVyfGJvb2xlYW58c3RyaW5nW10pOiB0aGlzO1xuICBzZWFyY2goXG4gICAgICBzZWFyY2g/OiBzdHJpbmd8bnVtYmVyfHtba2V5OiBzdHJpbmddOiB1bmtub3dufSxcbiAgICAgIHBhcmFtVmFsdWU/OiBudWxsfHVuZGVmaW5lZHxzdHJpbmd8bnVtYmVyfGJvb2xlYW58c3RyaW5nW10pOiB7W2tleTogc3RyaW5nXTogdW5rbm93bn18dGhpcyB7XG4gICAgc3dpdGNoIChhcmd1bWVudHMubGVuZ3RoKSB7XG4gICAgICBjYXNlIDA6XG4gICAgICAgIHJldHVybiB0aGlzLiQkc2VhcmNoO1xuICAgICAgY2FzZSAxOlxuICAgICAgICBpZiAodHlwZW9mIHNlYXJjaCA9PT0gJ3N0cmluZycgfHwgdHlwZW9mIHNlYXJjaCA9PT0gJ251bWJlcicpIHtcbiAgICAgICAgICB0aGlzLiQkc2VhcmNoID0gdGhpcy51cmxDb2RlYy5kZWNvZGVTZWFyY2goc2VhcmNoLnRvU3RyaW5nKCkpO1xuICAgICAgICB9IGVsc2UgaWYgKHR5cGVvZiBzZWFyY2ggPT09ICdvYmplY3QnICYmIHNlYXJjaCAhPT0gbnVsbCkge1xuICAgICAgICAgIC8vIENvcHkgdGhlIG9iamVjdCBzbyBpdCdzIG5ldmVyIG11dGF0ZWRcbiAgICAgICAgICBzZWFyY2ggPSB7Li4uc2VhcmNofTtcbiAgICAgICAgICAvLyByZW1vdmUgb2JqZWN0IHVuZGVmaW5lZCBvciBudWxsIHByb3BlcnRpZXNcbiAgICAgICAgICBmb3IgKGNvbnN0IGtleSBpbiBzZWFyY2gpIHtcbiAgICAgICAgICAgIGlmIChzZWFyY2hba2V5XSA9PSBudWxsKSBkZWxldGUgc2VhcmNoW2tleV07XG4gICAgICAgICAgfVxuXG4gICAgICAgICAgdGhpcy4kJHNlYXJjaCA9IHNlYXJjaDtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgICAgICdMb2NhdGlvblByb3ZpZGVyLnNlYXJjaCgpOiBGaXJzdCBhcmd1bWVudCBtdXN0IGJlIGEgc3RyaW5nIG9yIGFuIG9iamVjdC4nKTtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIGlmICh0eXBlb2Ygc2VhcmNoID09PSAnc3RyaW5nJykge1xuICAgICAgICAgIGNvbnN0IGN1cnJlbnRTZWFyY2ggPSB0aGlzLnNlYXJjaCgpO1xuICAgICAgICAgIGlmICh0eXBlb2YgcGFyYW1WYWx1ZSA9PT0gJ3VuZGVmaW5lZCcgfHwgcGFyYW1WYWx1ZSA9PT0gbnVsbCkge1xuICAgICAgICAgICAgZGVsZXRlIGN1cnJlbnRTZWFyY2hbc2VhcmNoXTtcbiAgICAgICAgICAgIHJldHVybiB0aGlzLnNlYXJjaChjdXJyZW50U2VhcmNoKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgY3VycmVudFNlYXJjaFtzZWFyY2hdID0gcGFyYW1WYWx1ZTtcbiAgICAgICAgICAgIHJldHVybiB0aGlzLnNlYXJjaChjdXJyZW50U2VhcmNoKTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5jb21wb3NlVXJscygpO1xuICAgIHJldHVybiB0aGlzO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlcyB0aGUgY3VycmVudCBoYXNoIGZyYWdtZW50LCBvciBjaGFuZ2VzIHRoZSBoYXNoIGZyYWdtZW50IGFuZCByZXR1cm5zIGEgcmVmZXJlbmNlIHRvXG4gICAqIGl0cyBvd24gaW5zdGFuY2UuXG4gICAqXG4gICAqIGBgYGpzXG4gICAqIC8vIGdpdmVuIFVSTCBodHRwOi8vZXhhbXBsZS5jb20vIy9zb21lL3BhdGg/Zm9vPWJhciZiYXo9eG94byNoYXNoVmFsdWVcbiAgICogbGV0IGhhc2ggPSAkbG9jYXRpb24uaGFzaCgpO1xuICAgKiAvLyA9PiBcImhhc2hWYWx1ZVwiXG4gICAqIGBgYFxuICAgKi9cbiAgaGFzaCgpOiBzdHJpbmc7XG4gIGhhc2goaGFzaDogc3RyaW5nfG51bWJlcnxudWxsKTogdGhpcztcbiAgaGFzaChoYXNoPzogc3RyaW5nfG51bWJlcnxudWxsKTogc3RyaW5nfHRoaXMge1xuICAgIGlmICh0eXBlb2YgaGFzaCA9PT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgIHJldHVybiB0aGlzLiQkaGFzaDtcbiAgICB9XG5cbiAgICB0aGlzLiQkaGFzaCA9IGhhc2ggIT09IG51bGwgPyBoYXNoLnRvU3RyaW5nKCkgOiAnJztcblxuICAgIHRoaXMuY29tcG9zZVVybHMoKTtcbiAgICByZXR1cm4gdGhpcztcbiAgfVxuXG4gIC8qKlxuICAgKiBDaGFuZ2VzIHRvIGAkbG9jYXRpb25gIGR1cmluZyB0aGUgY3VycmVudCBgJGRpZ2VzdGAgd2lsbCByZXBsYWNlIHRoZSBjdXJyZW50XG4gICAqIGhpc3RvcnkgcmVjb3JkLCBpbnN0ZWFkIG9mIGFkZGluZyBhIG5ldyBvbmUuXG4gICAqL1xuICByZXBsYWNlKCk6IHRoaXMge1xuICAgIHRoaXMuJCRyZXBsYWNlID0gdHJ1ZTtcbiAgICByZXR1cm4gdGhpcztcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXRyaWV2ZXMgdGhlIGhpc3Rvcnkgc3RhdGUgb2JqZWN0IHdoZW4gY2FsbGVkIHdpdGhvdXQgYW55IHBhcmFtZXRlci5cbiAgICpcbiAgICogQ2hhbmdlIHRoZSBoaXN0b3J5IHN0YXRlIG9iamVjdCB3aGVuIGNhbGxlZCB3aXRoIG9uZSBwYXJhbWV0ZXIgYW5kIHJldHVybiBgJGxvY2F0aW9uYC5cbiAgICogVGhlIHN0YXRlIG9iamVjdCBpcyBsYXRlciBwYXNzZWQgdG8gYHB1c2hTdGF0ZWAgb3IgYHJlcGxhY2VTdGF0ZWAuXG4gICAqXG4gICAqIFRoaXMgbWV0aG9kIGlzIHN1cHBvcnRlZCBvbmx5IGluIEhUTUw1IG1vZGUgYW5kIG9ubHkgaW4gYnJvd3NlcnMgc3VwcG9ydGluZ1xuICAgKiB0aGUgSFRNTDUgSGlzdG9yeSBBUEkgbWV0aG9kcyBzdWNoIGFzIGBwdXNoU3RhdGVgIGFuZCBgcmVwbGFjZVN0YXRlYC4gSWYgeW91IG5lZWQgdG8gc3VwcG9ydFxuICAgKiBvbGRlciBicm93c2VycyAobGlrZSBJRTkgb3IgQW5kcm9pZCA8IDQuMCksIGRvbid0IHVzZSB0aGlzIG1ldGhvZC5cbiAgICpcbiAgICovXG4gIHN0YXRlKCk6IHVua25vd247XG4gIHN0YXRlKHN0YXRlOiB1bmtub3duKTogdGhpcztcbiAgc3RhdGUoc3RhdGU/OiB1bmtub3duKTogdW5rbm93bnx0aGlzIHtcbiAgICBpZiAodHlwZW9mIHN0YXRlID09PSAndW5kZWZpbmVkJykge1xuICAgICAgcmV0dXJuIHRoaXMuJCRzdGF0ZTtcbiAgICB9XG5cbiAgICB0aGlzLiQkc3RhdGUgPSBzdGF0ZTtcbiAgICByZXR1cm4gdGhpcztcbiAgfVxufVxuXG4vKipcbiAqIFRoZSBmYWN0b3J5IGZ1bmN0aW9uIHVzZWQgdG8gY3JlYXRlIGFuIGluc3RhbmNlIG9mIHRoZSBgJGxvY2F0aW9uU2hpbWAgaW4gQW5ndWxhcixcbiAqIGFuZCBwcm92aWRlcyBhbiBBUEktY29tcGF0aWFibGUgYCRsb2NhdGlvblByb3ZpZGVyYCBmb3IgQW5ndWxhckpTLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzICRsb2NhdGlvblNoaW1Qcm92aWRlciB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBuZ1VwZ3JhZGU6IFVwZ3JhZGVNb2R1bGUsIHByaXZhdGUgbG9jYXRpb246IExvY2F0aW9uLFxuICAgICAgcHJpdmF0ZSBwbGF0Zm9ybUxvY2F0aW9uOiBQbGF0Zm9ybUxvY2F0aW9uLCBwcml2YXRlIHVybENvZGVjOiBVcmxDb2RlYyxcbiAgICAgIHByaXZhdGUgbG9jYXRpb25TdHJhdGVneTogTG9jYXRpb25TdHJhdGVneSkge31cblxuICAvKipcbiAgICogRmFjdG9yeSBtZXRob2QgdGhhdCByZXR1cm5zIGFuIGluc3RhbmNlIG9mIHRoZSAkbG9jYXRpb25TaGltXG4gICAqL1xuICAkZ2V0KCkge1xuICAgIHJldHVybiBuZXcgJGxvY2F0aW9uU2hpbShcbiAgICAgICAgdGhpcy5uZ1VwZ3JhZGUuJGluamVjdG9yLCB0aGlzLmxvY2F0aW9uLCB0aGlzLnBsYXRmb3JtTG9jYXRpb24sIHRoaXMudXJsQ29kZWMsXG4gICAgICAgIHRoaXMubG9jYXRpb25TdHJhdGVneSk7XG4gIH1cblxuICAvKipcbiAgICogU3R1YiBtZXRob2QgdXNlZCB0byBrZWVwIEFQSSBjb21wYXRpYmxlIHdpdGggQW5ndWxhckpTLiBUaGlzIHNldHRpbmcgaXMgY29uZmlndXJlZCB0aHJvdWdoXG4gICAqIHRoZSBMb2NhdGlvblVwZ3JhZGVNb2R1bGUncyBgY29uZmlnYCBtZXRob2QgaW4geW91ciBBbmd1bGFyIGFwcC5cbiAgICovXG4gIGhhc2hQcmVmaXgocHJlZml4Pzogc3RyaW5nKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKCdDb25maWd1cmUgTG9jYXRpb25VcGdyYWRlIHRocm91Z2ggTG9jYXRpb25VcGdyYWRlTW9kdWxlLmNvbmZpZyBtZXRob2QuJyk7XG4gIH1cblxuICAvKipcbiAgICogU3R1YiBtZXRob2QgdXNlZCB0byBrZWVwIEFQSSBjb21wYXRpYmxlIHdpdGggQW5ndWxhckpTLiBUaGlzIHNldHRpbmcgaXMgY29uZmlndXJlZCB0aHJvdWdoXG4gICAqIHRoZSBMb2NhdGlvblVwZ3JhZGVNb2R1bGUncyBgY29uZmlnYCBtZXRob2QgaW4geW91ciBBbmd1bGFyIGFwcC5cbiAgICovXG4gIGh0bWw1TW9kZShtb2RlPzogYW55KSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKCdDb25maWd1cmUgTG9jYXRpb25VcGdyYWRlIHRocm91Z2ggTG9jYXRpb25VcGdyYWRlTW9kdWxlLmNvbmZpZyBtZXRob2QuJyk7XG4gIH1cbn1cbiJdfQ==