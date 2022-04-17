/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { EventEmitter, Injectable, InjectionToken, Inject, Optional } from '@angular/core';
import { LocationStrategy } from '@angular/common';
import { Subject } from 'rxjs';

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * A spy for {\@link Location} that allows tests to fire simulated location events.
 *
 * \@publicApi
 */
class SpyLocation {
    constructor() {
        this.urlChanges = [];
        this._history = [new LocationState('', '', null)];
        this._historyIndex = 0;
        /**
         * \@internal
         */
        this._subject = new EventEmitter();
        /**
         * \@internal
         */
        this._baseHref = '';
        /**
         * \@internal
         */
        this._platformStrategy = (/** @type {?} */ (null));
        /**
         * \@internal
         */
        this._platformLocation = (/** @type {?} */ (null));
        /**
         * \@internal
         */
        this._urlChangeListeners = [];
    }
    /**
     * @param {?} url
     * @return {?}
     */
    setInitialPath(url) { this._history[this._historyIndex].path = url; }
    /**
     * @param {?} url
     * @return {?}
     */
    setBaseHref(url) { this._baseHref = url; }
    /**
     * @return {?}
     */
    path() { return this._history[this._historyIndex].path; }
    /**
     * @return {?}
     */
    getState() { return this._history[this._historyIndex].state; }
    /**
     * @param {?} path
     * @param {?=} query
     * @return {?}
     */
    isCurrentPathEqualTo(path, query = '') {
        /** @type {?} */
        const givenPath = path.endsWith('/') ? path.substring(0, path.length - 1) : path;
        /** @type {?} */
        const currPath = this.path().endsWith('/') ? this.path().substring(0, this.path().length - 1) : this.path();
        return currPath == givenPath + (query.length > 0 ? ('?' + query) : '');
    }
    /**
     * @param {?} pathname
     * @return {?}
     */
    simulateUrlPop(pathname) {
        this._subject.emit({ 'url': pathname, 'pop': true, 'type': 'popstate' });
    }
    /**
     * @param {?} pathname
     * @return {?}
     */
    simulateHashChange(pathname) {
        // Because we don't prevent the native event, the browser will independently update the path
        this.setInitialPath(pathname);
        this.urlChanges.push('hash: ' + pathname);
        this._subject.emit({ 'url': pathname, 'pop': true, 'type': 'hashchange' });
    }
    /**
     * @param {?} url
     * @return {?}
     */
    prepareExternalUrl(url) {
        if (url.length > 0 && !url.startsWith('/')) {
            url = '/' + url;
        }
        return this._baseHref + url;
    }
    /**
     * @param {?} path
     * @param {?=} query
     * @param {?=} state
     * @return {?}
     */
    go(path, query = '', state = null) {
        path = this.prepareExternalUrl(path);
        if (this._historyIndex > 0) {
            this._history.splice(this._historyIndex + 1);
        }
        this._history.push(new LocationState(path, query, state));
        this._historyIndex = this._history.length - 1;
        /** @type {?} */
        const locationState = this._history[this._historyIndex - 1];
        if (locationState.path == path && locationState.query == query) {
            return;
        }
        /** @type {?} */
        const url = path + (query.length > 0 ? ('?' + query) : '');
        this.urlChanges.push(url);
        this._subject.emit({ 'url': url, 'pop': false });
    }
    /**
     * @param {?} path
     * @param {?=} query
     * @param {?=} state
     * @return {?}
     */
    replaceState(path, query = '', state = null) {
        path = this.prepareExternalUrl(path);
        /** @type {?} */
        const history = this._history[this._historyIndex];
        if (history.path == path && history.query == query) {
            return;
        }
        history.path = path;
        history.query = query;
        history.state = state;
        /** @type {?} */
        const url = path + (query.length > 0 ? ('?' + query) : '');
        this.urlChanges.push('replace: ' + url);
    }
    /**
     * @return {?}
     */
    forward() {
        if (this._historyIndex < (this._history.length - 1)) {
            this._historyIndex++;
            this._subject.emit({ 'url': this.path(), 'state': this.getState(), 'pop': true });
        }
    }
    /**
     * @return {?}
     */
    back() {
        if (this._historyIndex > 0) {
            this._historyIndex--;
            this._subject.emit({ 'url': this.path(), 'state': this.getState(), 'pop': true });
        }
    }
    /**
     * @param {?} fn
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
     * @param {?} onNext
     * @param {?=} onThrow
     * @param {?=} onReturn
     * @return {?}
     */
    subscribe(onNext, onThrow, onReturn) {
        return this._subject.subscribe({ next: onNext, error: onThrow, complete: onReturn });
    }
    /**
     * @param {?} url
     * @return {?}
     */
    normalize(url) { return (/** @type {?} */ (null)); }
}
SpyLocation.decorators = [
    { type: Injectable }
];
if (false) {
    /** @type {?} */
    SpyLocation.prototype.urlChanges;
    /**
     * @type {?}
     * @private
     */
    SpyLocation.prototype._history;
    /**
     * @type {?}
     * @private
     */
    SpyLocation.prototype._historyIndex;
    /**
     * \@internal
     * @type {?}
     */
    SpyLocation.prototype._subject;
    /**
     * \@internal
     * @type {?}
     */
    SpyLocation.prototype._baseHref;
    /**
     * \@internal
     * @type {?}
     */
    SpyLocation.prototype._platformStrategy;
    /**
     * \@internal
     * @type {?}
     */
    SpyLocation.prototype._platformLocation;
    /**
     * \@internal
     * @type {?}
     */
    SpyLocation.prototype._urlChangeListeners;
}
class LocationState {
    /**
     * @param {?} path
     * @param {?} query
     * @param {?} state
     */
    constructor(path, query, state) {
        this.path = path;
        this.query = query;
        this.state = state;
    }
}
if (false) {
    /** @type {?} */
    LocationState.prototype.path;
    /** @type {?} */
    LocationState.prototype.query;
    /** @type {?} */
    LocationState.prototype.state;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * A mock implementation of {\@link LocationStrategy} that allows tests to fire simulated
 * location events.
 *
 * \@publicApi
 */
class MockLocationStrategy extends LocationStrategy {
    constructor() {
        super();
        this.internalBaseHref = '/';
        this.internalPath = '/';
        this.internalTitle = '';
        this.urlChanges = [];
        /**
         * \@internal
         */
        this._subject = new EventEmitter();
        this.stateChanges = [];
    }
    /**
     * @param {?} url
     * @return {?}
     */
    simulatePopState(url) {
        this.internalPath = url;
        this._subject.emit(new _MockPopStateEvent(this.path()));
    }
    /**
     * @param {?=} includeHash
     * @return {?}
     */
    path(includeHash = false) { return this.internalPath; }
    /**
     * @param {?} internal
     * @return {?}
     */
    prepareExternalUrl(internal) {
        if (internal.startsWith('/') && this.internalBaseHref.endsWith('/')) {
            return this.internalBaseHref + internal.substring(1);
        }
        return this.internalBaseHref + internal;
    }
    /**
     * @param {?} ctx
     * @param {?} title
     * @param {?} path
     * @param {?} query
     * @return {?}
     */
    pushState(ctx, title, path, query) {
        // Add state change to changes array
        this.stateChanges.push(ctx);
        this.internalTitle = title;
        /** @type {?} */
        const url = path + (query.length > 0 ? ('?' + query) : '');
        this.internalPath = url;
        /** @type {?} */
        const externalUrl = this.prepareExternalUrl(url);
        this.urlChanges.push(externalUrl);
    }
    /**
     * @param {?} ctx
     * @param {?} title
     * @param {?} path
     * @param {?} query
     * @return {?}
     */
    replaceState(ctx, title, path, query) {
        // Reset the last index of stateChanges to the ctx (state) object
        this.stateChanges[(this.stateChanges.length || 1) - 1] = ctx;
        this.internalTitle = title;
        /** @type {?} */
        const url = path + (query.length > 0 ? ('?' + query) : '');
        this.internalPath = url;
        /** @type {?} */
        const externalUrl = this.prepareExternalUrl(url);
        this.urlChanges.push('replace: ' + externalUrl);
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    onPopState(fn) { this._subject.subscribe({ next: fn }); }
    /**
     * @return {?}
     */
    getBaseHref() { return this.internalBaseHref; }
    /**
     * @return {?}
     */
    back() {
        if (this.urlChanges.length > 0) {
            this.urlChanges.pop();
            this.stateChanges.pop();
            /** @type {?} */
            const nextUrl = this.urlChanges.length > 0 ? this.urlChanges[this.urlChanges.length - 1] : '';
            this.simulatePopState(nextUrl);
        }
    }
    /**
     * @return {?}
     */
    forward() { throw 'not implemented'; }
    /**
     * @return {?}
     */
    getState() { return this.stateChanges[(this.stateChanges.length || 1) - 1]; }
}
MockLocationStrategy.decorators = [
    { type: Injectable }
];
/** @nocollapse */
MockLocationStrategy.ctorParameters = () => [];
if (false) {
    /** @type {?} */
    MockLocationStrategy.prototype.internalBaseHref;
    /** @type {?} */
    MockLocationStrategy.prototype.internalPath;
    /** @type {?} */
    MockLocationStrategy.prototype.internalTitle;
    /** @type {?} */
    MockLocationStrategy.prototype.urlChanges;
    /**
     * \@internal
     * @type {?}
     */
    MockLocationStrategy.prototype._subject;
    /**
     * @type {?}
     * @private
     */
    MockLocationStrategy.prototype.stateChanges;
}
class _MockPopStateEvent {
    /**
     * @param {?} newUrl
     */
    constructor(newUrl) {
        this.newUrl = newUrl;
        this.pop = true;
        this.type = 'popstate';
    }
}
if (false) {
    /** @type {?} */
    _MockPopStateEvent.prototype.pop;
    /** @type {?} */
    _MockPopStateEvent.prototype.type;
    /** @type {?} */
    _MockPopStateEvent.prototype.newUrl;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Parser from https://tools.ietf.org/html/rfc3986#appendix-B
 * ^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?
 *  12            3  4          5       6  7        8 9
 *
 * Example: http://www.ics.uci.edu/pub/ietf/uri/#Related
 *
 * Results in:
 *
 * $1 = http:
 * $2 = http
 * $3 = //www.ics.uci.edu
 * $4 = www.ics.uci.edu
 * $5 = /pub/ietf/uri/
 * $6 = <undefined>
 * $7 = <undefined>
 * $8 = #Related
 * $9 = Related
 * @type {?}
 */
const urlParse = /^(([^:\/?#]+):)?(\/\/([^\/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?/;
/**
 * @param {?} urlStr
 * @param {?} baseHref
 * @return {?}
 */
function parseUrl(urlStr, baseHref) {
    /** @type {?} */
    const verifyProtocol = /^((http[s]?|ftp):\/\/)/;
    /** @type {?} */
    let serverBase;
    // URL class requires full URL. If the URL string doesn't start with protocol, we need to add
    // an arbitrary base URL which can be removed afterward.
    if (!verifyProtocol.test(urlStr)) {
        serverBase = 'http://empty.com/';
    }
    /** @type {?} */
    let parsedUrl;
    try {
        parsedUrl = new URL(urlStr, serverBase);
    }
    catch (e) {
        /** @type {?} */
        const result = urlParse.exec(serverBase || '' + urlStr);
        if (!result) {
            throw new Error(`Invalid URL: ${urlStr} with base: ${baseHref}`);
        }
        /** @type {?} */
        const hostSplit = result[4].split(':');
        parsedUrl = {
            protocol: result[1],
            hostname: hostSplit[0],
            port: hostSplit[1] || '',
            pathname: result[5],
            search: result[6],
            hash: result[8],
        };
    }
    if (parsedUrl.pathname && parsedUrl.pathname.indexOf(baseHref) === 0) {
        parsedUrl.pathname = parsedUrl.pathname.substring(baseHref.length);
    }
    return {
        hostname: !serverBase && parsedUrl.hostname || '',
        protocol: !serverBase && parsedUrl.protocol || '',
        port: !serverBase && parsedUrl.port || '',
        pathname: parsedUrl.pathname || '/',
        search: parsedUrl.search || '',
        hash: parsedUrl.hash || '',
    };
}
/**
 * Mock platform location config
 *
 * \@publicApi
 * @record
 */
function MockPlatformLocationConfig() { }
if (false) {
    /** @type {?|undefined} */
    MockPlatformLocationConfig.prototype.startUrl;
    /** @type {?|undefined} */
    MockPlatformLocationConfig.prototype.appBaseHref;
}
/**
 * Provider for mock platform location config
 *
 * \@publicApi
 * @type {?}
 */
const MOCK_PLATFORM_LOCATION_CONFIG = new InjectionToken('MOCK_PLATFORM_LOCATION_CONFIG');
/**
 * Mock implementation of URL state.
 *
 * \@publicApi
 */
class MockPlatformLocation {
    /**
     * @param {?=} config
     */
    constructor(config) {
        this.baseHref = '';
        this.hashUpdate = new Subject();
        this.urlChanges = [{ hostname: '', protocol: '', port: '', pathname: '/', search: '', hash: '', state: null }];
        if (config) {
            this.baseHref = config.appBaseHref || '';
            /** @type {?} */
            const parsedChanges = this.parseChanges(null, config.startUrl || 'http://<empty>/', this.baseHref);
            this.urlChanges[0] = Object.assign({}, parsedChanges);
        }
    }
    /**
     * @return {?}
     */
    get hostname() { return this.urlChanges[0].hostname; }
    /**
     * @return {?}
     */
    get protocol() { return this.urlChanges[0].protocol; }
    /**
     * @return {?}
     */
    get port() { return this.urlChanges[0].port; }
    /**
     * @return {?}
     */
    get pathname() { return this.urlChanges[0].pathname; }
    /**
     * @return {?}
     */
    get search() { return this.urlChanges[0].search; }
    /**
     * @return {?}
     */
    get hash() { return this.urlChanges[0].hash; }
    /**
     * @return {?}
     */
    get state() { return this.urlChanges[0].state; }
    /**
     * @return {?}
     */
    getBaseHrefFromDOM() { return this.baseHref; }
    /**
     * @param {?} fn
     * @return {?}
     */
    onPopState(fn) {
        // No-op: a state stack is not implemented, so
        // no events will ever come.
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    onHashChange(fn) { this.hashUpdate.subscribe(fn); }
    /**
     * @return {?}
     */
    get href() {
        /** @type {?} */
        let url = `${this.protocol}//${this.hostname}${this.port ? ':' + this.port : ''}`;
        url += `${this.pathname === '/' ? '' : this.pathname}${this.search}${this.hash}`;
        return url;
    }
    /**
     * @return {?}
     */
    get url() { return `${this.pathname}${this.search}${this.hash}`; }
    /**
     * @private
     * @param {?} state
     * @param {?} url
     * @param {?=} baseHref
     * @return {?}
     */
    parseChanges(state, url, baseHref = '') {
        // When the `history.state` value is stored, it is always copied.
        state = JSON.parse(JSON.stringify(state));
        return Object.assign({}, parseUrl(url, baseHref), { state });
    }
    /**
     * @param {?} state
     * @param {?} title
     * @param {?} newUrl
     * @return {?}
     */
    replaceState(state, title, newUrl) {
        const { pathname, search, state: parsedState, hash } = this.parseChanges(state, newUrl);
        this.urlChanges[0] = Object.assign({}, this.urlChanges[0], { pathname, search, hash, state: parsedState });
    }
    /**
     * @param {?} state
     * @param {?} title
     * @param {?} newUrl
     * @return {?}
     */
    pushState(state, title, newUrl) {
        const { pathname, search, state: parsedState, hash } = this.parseChanges(state, newUrl);
        this.urlChanges.unshift(Object.assign({}, this.urlChanges[0], { pathname, search, hash, state: parsedState }));
    }
    /**
     * @return {?}
     */
    forward() { throw new Error('Not implemented'); }
    /**
     * @return {?}
     */
    back() {
        /** @type {?} */
        const oldUrl = this.url;
        /** @type {?} */
        const oldHash = this.hash;
        this.urlChanges.shift();
        /** @type {?} */
        const newHash = this.hash;
        if (oldHash !== newHash) {
            scheduleMicroTask((/**
             * @return {?}
             */
            () => this.hashUpdate.next((/** @type {?} */ ({
                type: 'hashchange', state: null, oldUrl, newUrl: this.url
            })))));
        }
    }
    /**
     * @return {?}
     */
    getState() { return this.state; }
}
MockPlatformLocation.decorators = [
    { type: Injectable }
];
/** @nocollapse */
MockPlatformLocation.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [MOCK_PLATFORM_LOCATION_CONFIG,] }, { type: Optional }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    MockPlatformLocation.prototype.baseHref;
    /**
     * @type {?}
     * @private
     */
    MockPlatformLocation.prototype.hashUpdate;
    /**
     * @type {?}
     * @private
     */
    MockPlatformLocation.prototype.urlChanges;
}
/**
 * @param {?} cb
 * @return {?}
 */
function scheduleMicroTask(cb) {
    Promise.resolve(null).then(cb);
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

export { SpyLocation, MockLocationStrategy, MOCK_PLATFORM_LOCATION_CONFIG, MockPlatformLocation };
//# sourceMappingURL=testing.js.map
