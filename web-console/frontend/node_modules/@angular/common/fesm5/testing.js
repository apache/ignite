/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { __decorate, __extends, __metadata, __assign, __param } from 'tslib';
import { EventEmitter, Injectable, InjectionToken, Inject, Optional } from '@angular/core';
import { LocationStrategy } from '@angular/common';
import { Subject } from 'rxjs';

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * A spy for {@link Location} that allows tests to fire simulated location events.
 *
 * @publicApi
 */
var SpyLocation = /** @class */ (function () {
    function SpyLocation() {
        this.urlChanges = [];
        this._history = [new LocationState('', '', null)];
        this._historyIndex = 0;
        /** @internal */
        this._subject = new EventEmitter();
        /** @internal */
        this._baseHref = '';
        /** @internal */
        this._platformStrategy = null;
        /** @internal */
        this._platformLocation = null;
        /** @internal */
        this._urlChangeListeners = [];
    }
    SpyLocation.prototype.setInitialPath = function (url) { this._history[this._historyIndex].path = url; };
    SpyLocation.prototype.setBaseHref = function (url) { this._baseHref = url; };
    SpyLocation.prototype.path = function () { return this._history[this._historyIndex].path; };
    SpyLocation.prototype.getState = function () { return this._history[this._historyIndex].state; };
    SpyLocation.prototype.isCurrentPathEqualTo = function (path, query) {
        if (query === void 0) { query = ''; }
        var givenPath = path.endsWith('/') ? path.substring(0, path.length - 1) : path;
        var currPath = this.path().endsWith('/') ? this.path().substring(0, this.path().length - 1) : this.path();
        return currPath == givenPath + (query.length > 0 ? ('?' + query) : '');
    };
    SpyLocation.prototype.simulateUrlPop = function (pathname) {
        this._subject.emit({ 'url': pathname, 'pop': true, 'type': 'popstate' });
    };
    SpyLocation.prototype.simulateHashChange = function (pathname) {
        // Because we don't prevent the native event, the browser will independently update the path
        this.setInitialPath(pathname);
        this.urlChanges.push('hash: ' + pathname);
        this._subject.emit({ 'url': pathname, 'pop': true, 'type': 'hashchange' });
    };
    SpyLocation.prototype.prepareExternalUrl = function (url) {
        if (url.length > 0 && !url.startsWith('/')) {
            url = '/' + url;
        }
        return this._baseHref + url;
    };
    SpyLocation.prototype.go = function (path, query, state) {
        if (query === void 0) { query = ''; }
        if (state === void 0) { state = null; }
        path = this.prepareExternalUrl(path);
        if (this._historyIndex > 0) {
            this._history.splice(this._historyIndex + 1);
        }
        this._history.push(new LocationState(path, query, state));
        this._historyIndex = this._history.length - 1;
        var locationState = this._history[this._historyIndex - 1];
        if (locationState.path == path && locationState.query == query) {
            return;
        }
        var url = path + (query.length > 0 ? ('?' + query) : '');
        this.urlChanges.push(url);
        this._subject.emit({ 'url': url, 'pop': false });
    };
    SpyLocation.prototype.replaceState = function (path, query, state) {
        if (query === void 0) { query = ''; }
        if (state === void 0) { state = null; }
        path = this.prepareExternalUrl(path);
        var history = this._history[this._historyIndex];
        if (history.path == path && history.query == query) {
            return;
        }
        history.path = path;
        history.query = query;
        history.state = state;
        var url = path + (query.length > 0 ? ('?' + query) : '');
        this.urlChanges.push('replace: ' + url);
    };
    SpyLocation.prototype.forward = function () {
        if (this._historyIndex < (this._history.length - 1)) {
            this._historyIndex++;
            this._subject.emit({ 'url': this.path(), 'state': this.getState(), 'pop': true });
        }
    };
    SpyLocation.prototype.back = function () {
        if (this._historyIndex > 0) {
            this._historyIndex--;
            this._subject.emit({ 'url': this.path(), 'state': this.getState(), 'pop': true });
        }
    };
    SpyLocation.prototype.onUrlChange = function (fn) {
        var _this = this;
        this._urlChangeListeners.push(fn);
        this.subscribe(function (v) { _this._notifyUrlChangeListeners(v.url, v.state); });
    };
    /** @internal */
    SpyLocation.prototype._notifyUrlChangeListeners = function (url, state) {
        if (url === void 0) { url = ''; }
        this._urlChangeListeners.forEach(function (fn) { return fn(url, state); });
    };
    SpyLocation.prototype.subscribe = function (onNext, onThrow, onReturn) {
        return this._subject.subscribe({ next: onNext, error: onThrow, complete: onReturn });
    };
    SpyLocation.prototype.normalize = function (url) { return null; };
    SpyLocation = __decorate([
        Injectable()
    ], SpyLocation);
    return SpyLocation;
}());
var LocationState = /** @class */ (function () {
    function LocationState(path, query, state) {
        this.path = path;
        this.query = query;
        this.state = state;
    }
    return LocationState;
}());

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * A mock implementation of {@link LocationStrategy} that allows tests to fire simulated
 * location events.
 *
 * @publicApi
 */
var MockLocationStrategy = /** @class */ (function (_super) {
    __extends(MockLocationStrategy, _super);
    function MockLocationStrategy() {
        var _this = _super.call(this) || this;
        _this.internalBaseHref = '/';
        _this.internalPath = '/';
        _this.internalTitle = '';
        _this.urlChanges = [];
        /** @internal */
        _this._subject = new EventEmitter();
        _this.stateChanges = [];
        return _this;
    }
    MockLocationStrategy.prototype.simulatePopState = function (url) {
        this.internalPath = url;
        this._subject.emit(new _MockPopStateEvent(this.path()));
    };
    MockLocationStrategy.prototype.path = function (includeHash) {
        if (includeHash === void 0) { includeHash = false; }
        return this.internalPath;
    };
    MockLocationStrategy.prototype.prepareExternalUrl = function (internal) {
        if (internal.startsWith('/') && this.internalBaseHref.endsWith('/')) {
            return this.internalBaseHref + internal.substring(1);
        }
        return this.internalBaseHref + internal;
    };
    MockLocationStrategy.prototype.pushState = function (ctx, title, path, query) {
        // Add state change to changes array
        this.stateChanges.push(ctx);
        this.internalTitle = title;
        var url = path + (query.length > 0 ? ('?' + query) : '');
        this.internalPath = url;
        var externalUrl = this.prepareExternalUrl(url);
        this.urlChanges.push(externalUrl);
    };
    MockLocationStrategy.prototype.replaceState = function (ctx, title, path, query) {
        // Reset the last index of stateChanges to the ctx (state) object
        this.stateChanges[(this.stateChanges.length || 1) - 1] = ctx;
        this.internalTitle = title;
        var url = path + (query.length > 0 ? ('?' + query) : '');
        this.internalPath = url;
        var externalUrl = this.prepareExternalUrl(url);
        this.urlChanges.push('replace: ' + externalUrl);
    };
    MockLocationStrategy.prototype.onPopState = function (fn) { this._subject.subscribe({ next: fn }); };
    MockLocationStrategy.prototype.getBaseHref = function () { return this.internalBaseHref; };
    MockLocationStrategy.prototype.back = function () {
        if (this.urlChanges.length > 0) {
            this.urlChanges.pop();
            this.stateChanges.pop();
            var nextUrl = this.urlChanges.length > 0 ? this.urlChanges[this.urlChanges.length - 1] : '';
            this.simulatePopState(nextUrl);
        }
    };
    MockLocationStrategy.prototype.forward = function () { throw 'not implemented'; };
    MockLocationStrategy.prototype.getState = function () { return this.stateChanges[(this.stateChanges.length || 1) - 1]; };
    MockLocationStrategy = __decorate([
        Injectable(),
        __metadata("design:paramtypes", [])
    ], MockLocationStrategy);
    return MockLocationStrategy;
}(LocationStrategy));
var _MockPopStateEvent = /** @class */ (function () {
    function _MockPopStateEvent(newUrl) {
        this.newUrl = newUrl;
        this.pop = true;
        this.type = 'popstate';
    }
    return _MockPopStateEvent;
}());

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
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
 */
var urlParse = /^(([^:\/?#]+):)?(\/\/([^\/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?/;
function parseUrl(urlStr, baseHref) {
    var verifyProtocol = /^((http[s]?|ftp):\/\/)/;
    var serverBase;
    // URL class requires full URL. If the URL string doesn't start with protocol, we need to add
    // an arbitrary base URL which can be removed afterward.
    if (!verifyProtocol.test(urlStr)) {
        serverBase = 'http://empty.com/';
    }
    var parsedUrl;
    try {
        parsedUrl = new URL(urlStr, serverBase);
    }
    catch (e) {
        var result = urlParse.exec(serverBase || '' + urlStr);
        if (!result) {
            throw new Error("Invalid URL: " + urlStr + " with base: " + baseHref);
        }
        var hostSplit = result[4].split(':');
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
 * Provider for mock platform location config
 *
 * @publicApi
 */
var MOCK_PLATFORM_LOCATION_CONFIG = new InjectionToken('MOCK_PLATFORM_LOCATION_CONFIG');
/**
 * Mock implementation of URL state.
 *
 * @publicApi
 */
var MockPlatformLocation = /** @class */ (function () {
    function MockPlatformLocation(config) {
        this.baseHref = '';
        this.hashUpdate = new Subject();
        this.urlChanges = [{ hostname: '', protocol: '', port: '', pathname: '/', search: '', hash: '', state: null }];
        if (config) {
            this.baseHref = config.appBaseHref || '';
            var parsedChanges = this.parseChanges(null, config.startUrl || 'http://<empty>/', this.baseHref);
            this.urlChanges[0] = __assign({}, parsedChanges);
        }
    }
    Object.defineProperty(MockPlatformLocation.prototype, "hostname", {
        get: function () { return this.urlChanges[0].hostname; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(MockPlatformLocation.prototype, "protocol", {
        get: function () { return this.urlChanges[0].protocol; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(MockPlatformLocation.prototype, "port", {
        get: function () { return this.urlChanges[0].port; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(MockPlatformLocation.prototype, "pathname", {
        get: function () { return this.urlChanges[0].pathname; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(MockPlatformLocation.prototype, "search", {
        get: function () { return this.urlChanges[0].search; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(MockPlatformLocation.prototype, "hash", {
        get: function () { return this.urlChanges[0].hash; },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(MockPlatformLocation.prototype, "state", {
        get: function () { return this.urlChanges[0].state; },
        enumerable: true,
        configurable: true
    });
    MockPlatformLocation.prototype.getBaseHrefFromDOM = function () { return this.baseHref; };
    MockPlatformLocation.prototype.onPopState = function (fn) {
        // No-op: a state stack is not implemented, so
        // no events will ever come.
    };
    MockPlatformLocation.prototype.onHashChange = function (fn) { this.hashUpdate.subscribe(fn); };
    Object.defineProperty(MockPlatformLocation.prototype, "href", {
        get: function () {
            var url = this.protocol + "//" + this.hostname + (this.port ? ':' + this.port : '');
            url += "" + (this.pathname === '/' ? '' : this.pathname) + this.search + this.hash;
            return url;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(MockPlatformLocation.prototype, "url", {
        get: function () { return "" + this.pathname + this.search + this.hash; },
        enumerable: true,
        configurable: true
    });
    MockPlatformLocation.prototype.parseChanges = function (state, url, baseHref) {
        if (baseHref === void 0) { baseHref = ''; }
        // When the `history.state` value is stored, it is always copied.
        state = JSON.parse(JSON.stringify(state));
        return __assign({}, parseUrl(url, baseHref), { state: state });
    };
    MockPlatformLocation.prototype.replaceState = function (state, title, newUrl) {
        var _a = this.parseChanges(state, newUrl), pathname = _a.pathname, search = _a.search, parsedState = _a.state, hash = _a.hash;
        this.urlChanges[0] = __assign({}, this.urlChanges[0], { pathname: pathname, search: search, hash: hash, state: parsedState });
    };
    MockPlatformLocation.prototype.pushState = function (state, title, newUrl) {
        var _a = this.parseChanges(state, newUrl), pathname = _a.pathname, search = _a.search, parsedState = _a.state, hash = _a.hash;
        this.urlChanges.unshift(__assign({}, this.urlChanges[0], { pathname: pathname, search: search, hash: hash, state: parsedState }));
    };
    MockPlatformLocation.prototype.forward = function () { throw new Error('Not implemented'); };
    MockPlatformLocation.prototype.back = function () {
        var _this = this;
        var oldUrl = this.url;
        var oldHash = this.hash;
        this.urlChanges.shift();
        var newHash = this.hash;
        if (oldHash !== newHash) {
            scheduleMicroTask(function () { return _this.hashUpdate.next({
                type: 'hashchange', state: null, oldUrl: oldUrl, newUrl: _this.url
            }); });
        }
    };
    MockPlatformLocation.prototype.getState = function () { return this.state; };
    MockPlatformLocation = __decorate([
        Injectable(),
        __param(0, Inject(MOCK_PLATFORM_LOCATION_CONFIG)), __param(0, Optional()),
        __metadata("design:paramtypes", [Object])
    ], MockPlatformLocation);
    return MockPlatformLocation;
}());
function scheduleMicroTask(cb) {
    Promise.resolve(null).then(cb);
}

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
// This file only reexports content of the `src` folder. Keep it that way.

/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */

/**
 * Generated bundle index. Do not edit.
 */

export { SpyLocation, MockLocationStrategy, MOCK_PLATFORM_LOCATION_CONFIG, MockPlatformLocation };
//# sourceMappingURL=testing.js.map
