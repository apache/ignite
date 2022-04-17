/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { Location, PlatformLocation, LocationStrategy, APP_BASE_HREF, CommonModule, HashLocationStrategy, PathLocationStrategy } from '@angular/common';
import { InjectionToken, Inject, Optional, NgModule } from '@angular/core';
import { UpgradeModule } from '@angular/upgrade/static';

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
 * @param {?} val
 * @param {?} prefix
 * @return {?}
 */
function stripPrefix(val, prefix) {
    return val.startsWith(prefix) ? val.substring(prefix.length) : val;
}
/**
 * @param {?} a
 * @param {?} b
 * @return {?}
 */
function deepEqual(a, b) {
    if (a === b) {
        return true;
    }
    else if (!a || !b) {
        return false;
    }
    else {
        try {
            if ((a.prototype !== b.prototype) || (Array.isArray(a) && Array.isArray(b))) {
                return false;
            }
            return JSON.stringify(a) === JSON.stringify(b);
        }
        catch (e) {
            return false;
        }
    }
}
/**
 * @param {?} el
 * @return {?}
 */
function isAnchor(el) {
    return ((/** @type {?} */ (el))).href !== undefined;
}
/**
 * @param {?} obj
 * @return {?}
 */
function isPromise(obj) {
    // allow any Promise/A+ compliant thenable.
    // It's up to the caller to ensure that obj.then conforms to the spec
    return !!obj && typeof obj.then === 'function';
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const PATH_MATCH = /^([^?#]*)(\?([^#]*))?(#(.*))?$/;
/** @type {?} */
const DOUBLE_SLASH_REGEX = /^\s*[\\/]{2,}/;
/** @type {?} */
const IGNORE_URI_REGEXP = /^\s*(javascript|mailto):/i;
/** @type {?} */
const DEFAULT_PORTS = {
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
 * \@publicApi
 */
class $locationShim {
    /**
     * @param {?} $injector
     * @param {?} location
     * @param {?} platformLocation
     * @param {?} urlCodec
     * @param {?} locationStrategy
     */
    constructor($injector, location, platformLocation, urlCodec, locationStrategy) {
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
        /** @type {?} */
        const initialUrl = this.browserUrl();
        /** @type {?} */
        let parsedUrl = this.urlCodec.parse(initialUrl);
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
            $injector.then((/**
             * @param {?} $i
             * @return {?}
             */
            $i => this.initialize($i)));
        }
        else {
            this.initialize($injector);
        }
    }
    /**
     * @private
     * @param {?} $injector
     * @return {?}
     */
    initialize($injector) {
        /** @type {?} */
        const $rootScope = $injector.get('$rootScope');
        /** @type {?} */
        const $rootElement = $injector.get('$rootElement');
        $rootElement.on('click', (/**
         * @param {?} event
         * @return {?}
         */
        (event) => {
            if (event.ctrlKey || event.metaKey || event.shiftKey || event.which === 2 ||
                event.button === 2) {
                return;
            }
            /** @type {?} */
            let elm = event.target;
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
            /** @type {?} */
            const absHref = elm.href;
            /** @type {?} */
            const relHref = elm.getAttribute('href');
            // Ignore when url is started with javascript: or mailto:
            if (IGNORE_URI_REGEXP.test(absHref)) {
                return;
            }
            if (absHref && !elm.getAttribute('target') && !event.isDefaultPrevented()) {
                if (this.$$parseLinkUrl(absHref, relHref)) {
                    // We do a preventDefault for all urls that are part of the AngularJS application,
                    // in html5mode and also without, so that we are able to abort navigation without
                    // getting double entries in the location history.
                    event.preventDefault();
                    // update location manually
                    if (this.absUrl() !== this.browserUrl()) {
                        $rootScope.$apply();
                    }
                }
            }
        }));
        this.location.onUrlChange((/**
         * @param {?} newUrl
         * @param {?} newState
         * @return {?}
         */
        (newUrl, newState) => {
            /** @type {?} */
            let oldUrl = this.absUrl();
            /** @type {?} */
            let oldState = this.$$state;
            this.$$parse(newUrl);
            newUrl = this.absUrl();
            this.$$state = newState;
            /** @type {?} */
            const defaultPrevented = $rootScope.$broadcast('$locationChangeStart', newUrl, oldUrl, newState, oldState)
                .defaultPrevented;
            // if the location was changed by a `$locationChangeStart` handler then stop
            // processing this location change
            if (this.absUrl() !== newUrl)
                return;
            // If default was prevented, set back to old state. This is the state that was locally
            // cached in the $location service.
            if (defaultPrevented) {
                this.$$parse(oldUrl);
                this.state(oldState);
                this.setBrowserUrlWithFallback(oldUrl, false, oldState);
            }
            else {
                this.initalizing = false;
                $rootScope.$broadcast('$locationChangeSuccess', newUrl, oldUrl, newState, oldState);
                this.resetBrowserUpdate();
            }
            if (!$rootScope.$$phase) {
                $rootScope.$digest();
            }
        }));
        // update browser
        $rootScope.$watch((/**
         * @return {?}
         */
        () => {
            if (this.initalizing || this.updateBrowser) {
                this.updateBrowser = false;
                /** @type {?} */
                const oldUrl = this.browserUrl();
                /** @type {?} */
                const newUrl = this.absUrl();
                /** @type {?} */
                const oldState = this.browserState();
                /** @type {?} */
                let currentReplace = this.$$replace;
                /** @type {?} */
                const urlOrStateChanged = !this.urlCodec.areEqual(oldUrl, newUrl) || oldState !== this.$$state;
                // Fire location changes one time to on initialization. This must be done on the
                // next tick (thus inside $evalAsync()) in order for listeners to be registered
                // before the event fires. Mimicing behavior from $locationWatch:
                // https://github.com/angular/angular.js/blob/master/src/ng/location.js#L983
                if (this.initalizing || urlOrStateChanged) {
                    this.initalizing = false;
                    $rootScope.$evalAsync((/**
                     * @return {?}
                     */
                    () => {
                        // Get the new URL again since it could have changed due to async update
                        /** @type {?} */
                        const newUrl = this.absUrl();
                        /** @type {?} */
                        const defaultPrevented = $rootScope
                            .$broadcast('$locationChangeStart', newUrl, oldUrl, this.$$state, oldState)
                            .defaultPrevented;
                        // if the location was changed by a `$locationChangeStart` handler then stop
                        // processing this location change
                        if (this.absUrl() !== newUrl)
                            return;
                        if (defaultPrevented) {
                            this.$$parse(oldUrl);
                            this.$$state = oldState;
                        }
                        else {
                            // This block doesn't run when initalizing because it's going to perform the update to
                            // the URL which shouldn't be needed when initalizing.
                            if (urlOrStateChanged) {
                                this.setBrowserUrlWithFallback(newUrl, currentReplace, oldState === this.$$state ? null : this.$$state);
                                this.$$replace = false;
                            }
                            $rootScope.$broadcast('$locationChangeSuccess', newUrl, oldUrl, this.$$state, oldState);
                        }
                    }));
                }
            }
            this.$$replace = false;
        }));
    }
    /**
     * @private
     * @return {?}
     */
    resetBrowserUpdate() {
        this.$$replace = false;
        this.$$state = this.browserState();
        this.updateBrowser = false;
        this.lastBrowserUrl = this.browserUrl();
    }
    /**
     * @private
     * @param {?=} url
     * @param {?=} replace
     * @param {?=} state
     * @return {?}
     */
    browserUrl(url, replace, state) {
        // In modern browsers `history.state` is `null` by default; treating it separately
        // from `undefined` would cause `$browser.url('/foo')` to change `history.state`
        // to undefined via `pushState`. Instead, let's change `undefined` to `null` here.
        if (typeof state === 'undefined') {
            state = null;
        }
        // setter
        if (url) {
            /** @type {?} */
            let sameState = this.lastHistoryState === state;
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
    }
    /**
     * @private
     * @return {?}
     */
    cacheState() {
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
    }
    /**
     * This function emulates the $browser.state() function from AngularJS. It will cause
     * history.state to be cached unless changed with deep equality check.
     * @private
     * @return {?}
     */
    browserState() { return this.cachedState; }
    /**
     * @private
     * @param {?} base
     * @param {?} url
     * @return {?}
     */
    stripBaseUrl(base, url) {
        if (url.startsWith(base)) {
            return url.substr(base.length);
        }
        return undefined;
    }
    /**
     * @private
     * @return {?}
     */
    getServerBase() {
        const { protocol, hostname, port } = this.platformLocation;
        /** @type {?} */
        const baseHref = this.locationStrategy.getBaseHref();
        /** @type {?} */
        let url = `${protocol}//${hostname}${port ? ':' + port : ''}${baseHref || '/'}`;
        return url.endsWith('/') ? url : url + '/';
    }
    /**
     * @private
     * @param {?} url
     * @return {?}
     */
    parseAppUrl(url) {
        if (DOUBLE_SLASH_REGEX.test(url)) {
            throw new Error(`Bad Path - URL cannot start with double slashes: ${url}`);
        }
        /** @type {?} */
        let prefixed = (url.charAt(0) !== '/');
        if (prefixed) {
            url = '/' + url;
        }
        /** @type {?} */
        let match = this.urlCodec.parse(url, this.getServerBase());
        if (typeof match === 'string') {
            throw new Error(`Bad URL - Cannot parse URL: ${url}`);
        }
        /** @type {?} */
        let path = prefixed && match.pathname.charAt(0) === '/' ? match.pathname.substring(1) : match.pathname;
        this.$$path = this.urlCodec.decodePath(path);
        this.$$search = this.urlCodec.decodeSearch(match.search);
        this.$$hash = this.urlCodec.decodeHash(match.hash);
        // make sure path starts with '/';
        if (this.$$path && this.$$path.charAt(0) !== '/') {
            this.$$path = '/' + this.$$path;
        }
    }
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
     * @param {?} fn The callback function that is triggered for the listener when the URL changes.
     * @param {?=} err The callback function that is triggered when an error occurs.
     * @return {?}
     */
    onChange(fn, err = (/**
     * @param {?} e
     * @return {?}
     */
    (e) => { })) {
        this.$$changeListeners.push([fn, err]);
    }
    /**
     * \@internal
     * @param {?=} url
     * @param {?=} state
     * @param {?=} oldUrl
     * @param {?=} oldState
     * @return {?}
     */
    $$notifyChangeListeners(url = '', state, oldUrl = '', oldState) {
        this.$$changeListeners.forEach((/**
         * @param {?} __0
         * @return {?}
         */
        ([fn, err]) => {
            try {
                fn(url, state, oldUrl, oldState);
            }
            catch (e) {
                err(e);
            }
        }));
    }
    /**
     * Parses the provided URL, and sets the current URL to the parsed result.
     *
     * @param {?} url The URL string.
     * @return {?}
     */
    $$parse(url) {
        /** @type {?} */
        let pathUrl;
        if (url.startsWith('/')) {
            pathUrl = url;
        }
        else {
            // Remove protocol & hostname if URL starts with it
            pathUrl = this.stripBaseUrl(this.getServerBase(), url);
        }
        if (typeof pathUrl === 'undefined') {
            throw new Error(`Invalid url "${url}", missing path prefix "${this.getServerBase()}".`);
        }
        this.parseAppUrl(pathUrl);
        if (!this.$$path) {
            this.$$path = '/';
        }
        this.composeUrls();
    }
    /**
     * Parses the provided URL and its relative URL.
     *
     * @param {?} url The full URL string.
     * @param {?=} relHref A URL string relative to the full URL string.
     * @return {?}
     */
    $$parseLinkUrl(url, relHref) {
        // When relHref is passed, it should be a hash and is handled separately
        if (relHref && relHref[0] === '#') {
            this.hash(relHref.slice(1));
            return true;
        }
        /** @type {?} */
        let rewrittenUrl;
        /** @type {?} */
        let appUrl = this.stripBaseUrl(this.getServerBase(), url);
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
    }
    /**
     * @private
     * @param {?} url
     * @param {?} replace
     * @param {?} state
     * @return {?}
     */
    setBrowserUrlWithFallback(url, replace, state) {
        /** @type {?} */
        const oldUrl = this.url();
        /** @type {?} */
        const oldState = this.$$state;
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
    }
    /**
     * @private
     * @return {?}
     */
    composeUrls() {
        this.$$url = this.urlCodec.normalize(this.$$path, this.$$search, this.$$hash);
        this.$$absUrl = this.getServerBase() + this.$$url.substr(1); // remove '/' from front of URL
        this.updateBrowser = true;
    }
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
     * @return {?}
     */
    absUrl() { return this.$$absUrl; }
    /**
     * @param {?=} url
     * @return {?}
     */
    url(url) {
        if (typeof url === 'string') {
            if (!url.length) {
                url = '/';
            }
            /** @type {?} */
            const match = PATH_MATCH.exec(url);
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
    }
    /**
     * Retrieves the protocol of the current URL.
     *
     * ```js
     * // given URL http://example.com/#/some/path?foo=bar&baz=xoxo
     * let protocol = $location.protocol();
     * // => "http"
     * ```
     * @return {?}
     */
    protocol() { return this.$$protocol; }
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
     * // given URL http://user:password\@example.com:8080/#/some/path?foo=bar&baz=xoxo
     * host = $location.host();
     * // => "example.com"
     * host = location.host;
     * // => "example.com:8080"
     * ```
     * @return {?}
     */
    host() { return this.$$host; }
    /**
     * Retrieves the port of the current URL.
     *
     * ```js
     * // given URL http://example.com/#/some/path?foo=bar&baz=xoxo
     * let port = $location.port();
     * // => 80
     * ```
     * @return {?}
     */
    port() { return this.$$port; }
    /**
     * @param {?=} path
     * @return {?}
     */
    path(path) {
        if (typeof path === 'undefined') {
            return this.$$path;
        }
        // null path converts to empty string. Prepend with "/" if needed.
        path = path !== null ? path.toString() : '';
        path = path.charAt(0) === '/' ? path : '/' + path;
        this.$$path = path;
        this.composeUrls();
        return this;
    }
    /**
     * @param {?=} search
     * @param {?=} paramValue
     * @return {?}
     */
    search(search, paramValue) {
        switch (arguments.length) {
            case 0:
                return this.$$search;
            case 1:
                if (typeof search === 'string' || typeof search === 'number') {
                    this.$$search = this.urlCodec.decodeSearch(search.toString());
                }
                else if (typeof search === 'object' && search !== null) {
                    // Copy the object so it's never mutated
                    search = Object.assign({}, search);
                    // remove object undefined or null properties
                    for (const key in search) {
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
                    /** @type {?} */
                    const currentSearch = this.search();
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
    }
    /**
     * @param {?=} hash
     * @return {?}
     */
    hash(hash) {
        if (typeof hash === 'undefined') {
            return this.$$hash;
        }
        this.$$hash = hash !== null ? hash.toString() : '';
        this.composeUrls();
        return this;
    }
    /**
     * Changes to `$location` during the current `$digest` will replace the current
     * history record, instead of adding a new one.
     * @template THIS
     * @this {THIS}
     * @return {THIS}
     */
    replace() {
        (/** @type {?} */ (this)).$$replace = true;
        return (/** @type {?} */ (this));
    }
    /**
     * @param {?=} state
     * @return {?}
     */
    state(state) {
        if (typeof state === 'undefined') {
            return this.$$state;
        }
        this.$$state = state;
        return this;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.initalizing;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.updateBrowser;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$absUrl;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$url;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$protocol;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$host;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$port;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$replace;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$path;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$search;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$hash;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$state;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.$$changeListeners;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.cachedState;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.lastHistoryState;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.lastBrowserUrl;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.lastCachedState;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.location;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.platformLocation;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.urlCodec;
    /**
     * @type {?}
     * @private
     */
    $locationShim.prototype.locationStrategy;
}
/**
 * The factory function used to create an instance of the `$locationShim` in Angular,
 * and provides an API-compatiable `$locationProvider` for AngularJS.
 *
 * \@publicApi
 */
class $locationShimProvider {
    /**
     * @param {?} ngUpgrade
     * @param {?} location
     * @param {?} platformLocation
     * @param {?} urlCodec
     * @param {?} locationStrategy
     */
    constructor(ngUpgrade, location, platformLocation, urlCodec, locationStrategy) {
        this.ngUpgrade = ngUpgrade;
        this.location = location;
        this.platformLocation = platformLocation;
        this.urlCodec = urlCodec;
        this.locationStrategy = locationStrategy;
    }
    /**
     * Factory method that returns an instance of the $locationShim
     * @return {?}
     */
    $get() {
        return new $locationShim(this.ngUpgrade.$injector, this.location, this.platformLocation, this.urlCodec, this.locationStrategy);
    }
    /**
     * Stub method used to keep API compatible with AngularJS. This setting is configured through
     * the LocationUpgradeModule's `config` method in your Angular app.
     * @param {?=} prefix
     * @return {?}
     */
    hashPrefix(prefix) {
        throw new Error('Configure LocationUpgrade through LocationUpgradeModule.config method.');
    }
    /**
     * Stub method used to keep API compatible with AngularJS. This setting is configured through
     * the LocationUpgradeModule's `config` method in your Angular app.
     * @param {?=} mode
     * @return {?}
     */
    html5Mode(mode) {
        throw new Error('Configure LocationUpgrade through LocationUpgradeModule.config method.');
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    $locationShimProvider.prototype.ngUpgrade;
    /**
     * @type {?}
     * @private
     */
    $locationShimProvider.prototype.location;
    /**
     * @type {?}
     * @private
     */
    $locationShimProvider.prototype.platformLocation;
    /**
     * @type {?}
     * @private
     */
    $locationShimProvider.prototype.urlCodec;
    /**
     * @type {?}
     * @private
     */
    $locationShimProvider.prototype.locationStrategy;
}

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
 * A codec for encoding and decoding URL parts.
 *
 * \@publicApi
 *
 * @abstract
 */
class UrlCodec {
}
if (false) {
    /**
     * Encodes the path from the provided string
     *
     * @abstract
     * @param {?} path The path string
     * @return {?}
     */
    UrlCodec.prototype.encodePath = function (path) { };
    /**
     * Decodes the path from the provided string
     *
     * @abstract
     * @param {?} path The path string
     * @return {?}
     */
    UrlCodec.prototype.decodePath = function (path) { };
    /**
     * Encodes the search string from the provided string or object
     *
     * @abstract
     * @param {?} search
     * @return {?}
     */
    UrlCodec.prototype.encodeSearch = function (search) { };
    /**
     * Decodes the search objects from the provided string
     *
     * @abstract
     * @param {?} search
     * @return {?}
     */
    UrlCodec.prototype.decodeSearch = function (search) { };
    /**
     * Encodes the hash from the provided string
     *
     * @abstract
     * @param {?} hash
     * @return {?}
     */
    UrlCodec.prototype.encodeHash = function (hash) { };
    /**
     * Decodes the hash from the provided string
     *
     * @abstract
     * @param {?} hash
     * @return {?}
     */
    UrlCodec.prototype.decodeHash = function (hash) { };
    /**
     * Normalizes the URL from the provided string
     *
     * @abstract
     * @param {?} href
     * @return {?}
     */
    UrlCodec.prototype.normalize = function (href) { };
    /**
     * Normalizes the URL from the provided string, search, hash, and base URL parameters
     *
     * @abstract
     * @param {?} path The URL path
     * @param {?} search The search object
     * @param {?} hash The has string
     * @param {?=} baseUrl The base URL for the URL
     * @return {?}
     */
    UrlCodec.prototype.normalize = function (path, search, hash, baseUrl) { };
    /**
     * Checks whether the two strings are equal
     * @abstract
     * @param {?} valA First string for comparison
     * @param {?} valB Second string for comparison
     * @return {?}
     */
    UrlCodec.prototype.areEqual = function (valA, valB) { };
    /**
     * Parses the URL string based on the base URL
     *
     * @abstract
     * @param {?} url The full URL string
     * @param {?=} base The base for the URL
     * @return {?}
     */
    UrlCodec.prototype.parse = function (url, base) { };
}
/**
 * A `UrlCodec` that uses logic from AngularJS to serialize and parse URLs
 * and URL parameters.
 *
 * \@publicApi
 */
class AngularJSUrlCodec {
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L15
    /**
     * @param {?} path
     * @return {?}
     */
    encodePath(path) {
        /** @type {?} */
        const segments = path.split('/');
        /** @type {?} */
        let i = segments.length;
        while (i--) {
            // decode forward slashes to prevent them from being double encoded
            segments[i] = encodeUriSegment(segments[i].replace(/%2F/g, '/'));
        }
        path = segments.join('/');
        return _stripIndexHtml((path && path[0] !== '/' && '/' || '') + path);
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L42
    /**
     * @param {?} search
     * @return {?}
     */
    encodeSearch(search) {
        if (typeof search === 'string') {
            search = parseKeyValue(search);
        }
        search = toKeyValue(search);
        return search ? '?' + search : '';
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L44
    /**
     * @param {?} hash
     * @return {?}
     */
    encodeHash(hash) {
        hash = encodeUriSegment(hash);
        return hash ? '#' + hash : '';
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L27
    /**
     * @param {?} path
     * @param {?=} html5Mode
     * @return {?}
     */
    decodePath(path, html5Mode = true) {
        /** @type {?} */
        const segments = path.split('/');
        /** @type {?} */
        let i = segments.length;
        while (i--) {
            segments[i] = decodeURIComponent(segments[i]);
            if (html5Mode) {
                // encode forward slashes to prevent them from being mistaken for path separators
                segments[i] = segments[i].replace(/\//g, '%2F');
            }
        }
        return segments.join('/');
    }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L72
    /**
     * @param {?} search
     * @return {?}
     */
    decodeSearch(search) { return parseKeyValue(search); }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/location.js#L73
    /**
     * @param {?} hash
     * @return {?}
     */
    decodeHash(hash) {
        hash = decodeURIComponent(hash);
        return hash[0] === '#' ? hash.substring(1) : hash;
    }
    /**
     * @param {?} pathOrHref
     * @param {?=} search
     * @param {?=} hash
     * @param {?=} baseUrl
     * @return {?}
     */
    normalize(pathOrHref, search, hash, baseUrl) {
        if (arguments.length === 1) {
            /** @type {?} */
            const parsed = this.parse(pathOrHref, baseUrl);
            if (typeof parsed === 'string') {
                return parsed;
            }
            /** @type {?} */
            const serverUrl = `${parsed.protocol}://${parsed.hostname}${parsed.port ? ':' + parsed.port : ''}`;
            return this.normalize(this.decodePath(parsed.pathname), this.decodeSearch(parsed.search), this.decodeHash(parsed.hash), serverUrl);
        }
        else {
            /** @type {?} */
            const encPath = this.encodePath(pathOrHref);
            /** @type {?} */
            const encSearch = search && this.encodeSearch(search) || '';
            /** @type {?} */
            const encHash = hash && this.encodeHash(hash) || '';
            /** @type {?} */
            let joinedPath = (baseUrl || '') + encPath;
            if (!joinedPath.length || joinedPath[0] !== '/') {
                joinedPath = '/' + joinedPath;
            }
            return joinedPath + encSearch + encHash;
        }
    }
    /**
     * @param {?} valA
     * @param {?} valB
     * @return {?}
     */
    areEqual(valA, valB) { return this.normalize(valA) === this.normalize(valB); }
    // https://github.com/angular/angular.js/blob/864c7f0/src/ng/urlUtils.js#L60
    /**
     * @param {?} url
     * @param {?=} base
     * @return {?}
     */
    parse(url, base) {
        try {
            // Safari 12 throws an error when the URL constructor is called with an undefined base.
            /** @type {?} */
            const parsed = !base ? new URL(url) : new URL(url, base);
            return {
                href: parsed.href,
                protocol: parsed.protocol ? parsed.protocol.replace(/:$/, '') : '',
                host: parsed.host,
                search: parsed.search ? parsed.search.replace(/^\?/, '') : '',
                hash: parsed.hash ? parsed.hash.replace(/^#/, '') : '',
                hostname: parsed.hostname,
                port: parsed.port,
                pathname: (parsed.pathname.charAt(0) === '/') ? parsed.pathname : '/' + parsed.pathname
            };
        }
        catch (e) {
            throw new Error(`Invalid URL (${url}) with base (${base})`);
        }
    }
}
/**
 * @param {?} url
 * @return {?}
 */
function _stripIndexHtml(url) {
    return url.replace(/\/index.html$/, '');
}
/**
 * Tries to decode the URI component without throwing an exception.
 *
 * @param {?} value
 * @return {?}
 */
function tryDecodeURIComponent(value) {
    try {
        return decodeURIComponent(value);
    }
    catch (e) {
        // Ignore any invalid uri component.
        return undefined;
    }
}
/**
 * Parses an escaped url query string into key-value pairs. Logic taken from
 * https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1382
 * @param {?} keyValue
 * @return {?}
 */
function parseKeyValue(keyValue) {
    /** @type {?} */
    const obj = {};
    (keyValue || '').split('&').forEach((/**
     * @param {?} keyValue
     * @return {?}
     */
    (keyValue) => {
        /** @type {?} */
        let splitPoint;
        /** @type {?} */
        let key;
        /** @type {?} */
        let val;
        if (keyValue) {
            key = keyValue = keyValue.replace(/\+/g, '%20');
            splitPoint = keyValue.indexOf('=');
            if (splitPoint !== -1) {
                key = keyValue.substring(0, splitPoint);
                val = keyValue.substring(splitPoint + 1);
            }
            key = tryDecodeURIComponent(key);
            if (typeof key !== 'undefined') {
                val = typeof val !== 'undefined' ? tryDecodeURIComponent(val) : true;
                if (!obj.hasOwnProperty(key)) {
                    obj[key] = val;
                }
                else if (Array.isArray(obj[key])) {
                    ((/** @type {?} */ (obj[key]))).push(val);
                }
                else {
                    obj[key] = [obj[key], val];
                }
            }
        }
    }));
    return obj;
}
/**
 * Serializes into key-value pairs. Logic taken from
 * https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1409
 * @param {?} obj
 * @return {?}
 */
function toKeyValue(obj) {
    /** @type {?} */
    const parts = [];
    for (const key in obj) {
        /** @type {?} */
        let value = obj[key];
        if (Array.isArray(value)) {
            value.forEach((/**
             * @param {?} arrayValue
             * @return {?}
             */
            (arrayValue) => {
                parts.push(encodeUriQuery(key, true) +
                    (arrayValue === true ? '' : '=' + encodeUriQuery(arrayValue, true)));
            }));
        }
        else {
            parts.push(encodeUriQuery(key, true) +
                (value === true ? '' : '=' + encodeUriQuery((/** @type {?} */ (value)), true)));
        }
    }
    return parts.length ? parts.join('&') : '';
}
/**
 * We need our custom method because encodeURIComponent is too aggressive and doesn't follow
 * http://www.ietf.org/rfc/rfc3986.txt with regards to the character set (pchar) allowed in path
 * segments:
 *    segment       = *pchar
 *    pchar         = unreserved / pct-encoded / sub-delims / ":" / "\@"
 *    pct-encoded   = "%" HEXDIG HEXDIG
 *    unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
 *    sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
 *                     / "*" / "+" / "," / ";" / "="
 *
 * Logic from https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1437
 * @param {?} val
 * @return {?}
 */
function encodeUriSegment(val) {
    return encodeUriQuery(val, true)
        .replace(/%26/gi, '&')
        .replace(/%3D/gi, '=')
        .replace(/%2B/gi, '+');
}
/**
 * This method is intended for encoding *key* or *value* parts of query component. We need a custom
 * method because encodeURIComponent is too aggressive and encodes stuff that doesn't have to be
 * encoded per http://tools.ietf.org/html/rfc3986:
 *    query         = *( pchar / "/" / "?" )
 *    pchar         = unreserved / pct-encoded / sub-delims / ":" / "\@"
 *    unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
 *    pct-encoded   = "%" HEXDIG HEXDIG
 *    sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
 *                     / "*" / "+" / "," / ";" / "="
 *
 * Logic from https://github.com/angular/angular.js/blob/864c7f0/src/Angular.js#L1456
 * @param {?} val
 * @param {?=} pctEncodeSpaces
 * @return {?}
 */
function encodeUriQuery(val, pctEncodeSpaces = false) {
    return encodeURIComponent(val)
        .replace(/%40/gi, '@')
        .replace(/%3A/gi, ':')
        .replace(/%24/g, '$')
        .replace(/%2C/gi, ',')
        .replace(/%3B/gi, ';')
        .replace(/%20/g, (pctEncodeSpaces ? '%20' : '+'));
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Configuration options for LocationUpgrade.
 *
 * \@publicApi
 * @record
 */
function LocationUpgradeConfig() { }
if (false) {
    /**
     * Configures whether the location upgrade module should use the `HashLocationStrategy`
     * or the `PathLocationStrategy`
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.useHash;
    /**
     * Configures the hash prefix used in the URL when using the `HashLocationStrategy`
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.hashPrefix;
    /**
     * Configures the URL codec for encoding and decoding URLs. Default is the `AngularJSCodec`
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.urlCodec;
    /**
     * Configures the base href when used in server-side rendered applications
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.serverBaseHref;
    /**
     * Configures the base href when used in client-side rendered applications
     * @type {?|undefined}
     */
    LocationUpgradeConfig.prototype.appBaseHref;
}
/**
 * A provider token used to configure the location upgrade module.
 *
 * \@publicApi
 * @type {?}
 */
const LOCATION_UPGRADE_CONFIGURATION = new InjectionToken('LOCATION_UPGRADE_CONFIGURATION');
/** @type {?} */
const APP_BASE_HREF_RESOLVED = new InjectionToken('APP_BASE_HREF_RESOLVED');
/**
 * `NgModule` used for providing and configuring Angular's Unified Location Service for upgrading.
 *
 * @see [Using the Unified Angular Location Service](guide/upgrade#using-the-unified-angular-location-service)
 *
 * \@publicApi
 */
class LocationUpgradeModule {
    /**
     * @param {?=} config
     * @return {?}
     */
    static config(config) {
        return {
            ngModule: LocationUpgradeModule,
            providers: [
                Location,
                {
                    provide: $locationShim,
                    useFactory: provide$location,
                    deps: [UpgradeModule, Location, PlatformLocation, UrlCodec, LocationStrategy]
                },
                { provide: LOCATION_UPGRADE_CONFIGURATION, useValue: config ? config : {} },
                { provide: UrlCodec, useFactory: provideUrlCodec, deps: [LOCATION_UPGRADE_CONFIGURATION] },
                {
                    provide: APP_BASE_HREF_RESOLVED,
                    useFactory: provideAppBaseHref,
                    deps: [LOCATION_UPGRADE_CONFIGURATION, [new Inject(APP_BASE_HREF), new Optional()]]
                },
                {
                    provide: LocationStrategy,
                    useFactory: provideLocationStrategy,
                    deps: [
                        PlatformLocation,
                        APP_BASE_HREF_RESOLVED,
                        LOCATION_UPGRADE_CONFIGURATION,
                    ]
                },
            ],
        };
    }
}
LocationUpgradeModule.decorators = [
    { type: NgModule, args: [{ imports: [CommonModule] },] }
];
/**
 * @param {?} config
 * @param {?=} appBaseHref
 * @return {?}
 */
function provideAppBaseHref(config, appBaseHref) {
    if (config && config.appBaseHref != null) {
        return config.appBaseHref;
    }
    else if (appBaseHref != null) {
        return appBaseHref;
    }
    return '';
}
/**
 * @param {?} config
 * @return {?}
 */
function provideUrlCodec(config) {
    /** @type {?} */
    const codec = config && config.urlCodec || AngularJSUrlCodec;
    return new ((/** @type {?} */ (codec)))();
}
/**
 * @param {?} platformLocation
 * @param {?} baseHref
 * @param {?=} options
 * @return {?}
 */
function provideLocationStrategy(platformLocation, baseHref, options = {}) {
    return options.useHash ? new HashLocationStrategy(platformLocation, baseHref) :
        new PathLocationStrategy(platformLocation, baseHref);
}
/**
 * @param {?} ngUpgrade
 * @param {?} location
 * @param {?} platformLocation
 * @param {?} urlCodec
 * @param {?} locationStrategy
 * @return {?}
 */
function provide$location(ngUpgrade, location, platformLocation, urlCodec, locationStrategy) {
    /** @type {?} */
    const $locationProvider = new $locationShimProvider(ngUpgrade, location, platformLocation, urlCodec, locationStrategy);
    return $locationProvider.$get();
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */

/**
 * Generated bundle index. Do not edit.
 */

export { provide$location as ɵangular_packages_common_upgrade_upgrade_d, provideAppBaseHref as ɵangular_packages_common_upgrade_upgrade_a, provideLocationStrategy as ɵangular_packages_common_upgrade_upgrade_c, provideUrlCodec as ɵangular_packages_common_upgrade_upgrade_b, $locationShim, $locationShimProvider, LOCATION_UPGRADE_CONFIGURATION, LocationUpgradeModule, AngularJSUrlCodec, UrlCodec };
//# sourceMappingURL=upgrade.js.map
