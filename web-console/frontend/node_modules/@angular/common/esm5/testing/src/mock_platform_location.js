/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Inject, Injectable, InjectionToken, Optional } from '@angular/core';
import { Subject } from 'rxjs';
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
export var MOCK_PLATFORM_LOCATION_CONFIG = new InjectionToken('MOCK_PLATFORM_LOCATION_CONFIG');
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
            this.urlChanges[0] = tslib_1.__assign({}, parsedChanges);
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
        return tslib_1.__assign({}, parseUrl(url, baseHref), { state: state });
    };
    MockPlatformLocation.prototype.replaceState = function (state, title, newUrl) {
        var _a = this.parseChanges(state, newUrl), pathname = _a.pathname, search = _a.search, parsedState = _a.state, hash = _a.hash;
        this.urlChanges[0] = tslib_1.__assign({}, this.urlChanges[0], { pathname: pathname, search: search, hash: hash, state: parsedState });
    };
    MockPlatformLocation.prototype.pushState = function (state, title, newUrl) {
        var _a = this.parseChanges(state, newUrl), pathname = _a.pathname, search = _a.search, parsedState = _a.state, hash = _a.hash;
        this.urlChanges.unshift(tslib_1.__assign({}, this.urlChanges[0], { pathname: pathname, search: search, hash: hash, state: parsedState }));
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
    MockPlatformLocation = tslib_1.__decorate([
        Injectable(),
        tslib_1.__param(0, Inject(MOCK_PLATFORM_LOCATION_CONFIG)), tslib_1.__param(0, Optional()),
        tslib_1.__metadata("design:paramtypes", [Object])
    ], MockPlatformLocation);
    return MockPlatformLocation;
}());
export { MockPlatformLocation };
export function scheduleMicroTask(cb) {
    Promise.resolve(null).then(cb);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW9ja19wbGF0Zm9ybV9sb2NhdGlvbi5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi90ZXN0aW5nL3NyYy9tb2NrX3BsYXRmb3JtX2xvY2F0aW9uLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFHSCxPQUFPLEVBQUMsTUFBTSxFQUFFLFVBQVUsRUFBRSxjQUFjLEVBQUUsUUFBUSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQzNFLE9BQU8sRUFBQyxPQUFPLEVBQUMsTUFBTSxNQUFNLENBQUM7QUFFN0I7Ozs7Ozs7Ozs7Ozs7Ozs7OztHQWtCRztBQUNILElBQU0sUUFBUSxHQUFHLCtEQUErRCxDQUFDO0FBRWpGLFNBQVMsUUFBUSxDQUFDLE1BQWMsRUFBRSxRQUFnQjtJQUNoRCxJQUFNLGNBQWMsR0FBRyx3QkFBd0IsQ0FBQztJQUNoRCxJQUFJLFVBQTRCLENBQUM7SUFFakMsNkZBQTZGO0lBQzdGLHdEQUF3RDtJQUN4RCxJQUFJLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsRUFBRTtRQUNoQyxVQUFVLEdBQUcsbUJBQW1CLENBQUM7S0FDbEM7SUFDRCxJQUFJLFNBT0gsQ0FBQztJQUNGLElBQUk7UUFDRixTQUFTLEdBQUcsSUFBSSxHQUFHLENBQUMsTUFBTSxFQUFFLFVBQVUsQ0FBQyxDQUFDO0tBQ3pDO0lBQUMsT0FBTyxDQUFDLEVBQUU7UUFDVixJQUFNLE1BQU0sR0FBRyxRQUFRLENBQUMsSUFBSSxDQUFDLFVBQVUsSUFBSSxFQUFFLEdBQUcsTUFBTSxDQUFDLENBQUM7UUFDeEQsSUFBSSxDQUFDLE1BQU0sRUFBRTtZQUNYLE1BQU0sSUFBSSxLQUFLLENBQUMsa0JBQWdCLE1BQU0sb0JBQWUsUUFBVSxDQUFDLENBQUM7U0FDbEU7UUFDRCxJQUFNLFNBQVMsR0FBRyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMs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O1FBVkMsSUFBTSxNQUFNLEdBQUcsSUFBSSxDQUFDLEdBQUcsQ0FBQztRQUN4QixJQUFNLE9BQU8sR0FBRyxJQUFJLENBQUMsSUFBSSxDQUFDO1FBQzFCLElBQUksQ0FBQyxVQUFVLENBQUMsS0FBSyxFQUFFLENBQUM7UUFDeEIsSUFBTSxPQUFPLEdBQUcsSUFBSSxDQUFDLElBQUksQ0FBQztRQUUxQixJQUFJLE9BQU8sS0FBSyxPQUFPLEVBQUU7WUFDdkIsaUJBQWlCLENBQUMsY0FBTSxPQUFBLEtBQUksQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDO2dCQUMzQyxJQUFJLEVBQUUsWUFBWSxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsTUFBTSxRQUFBLEVBQUUsTUFBTSxFQUFFLEtBQUksQ0FBQyxHQUFHO2FBQ25DLENBQUMsRUFGRCxDQUVDLENBQUMsQ0FBQztTQUM1QjtJQUNILENBQUM7SUFFRCx1Q0FBUSxHQUFSLGNBQXNCLE9BQU8sSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUM7SUFsRi9CLG9CQUFvQjtRQURoQyxVQUFVLEVBQUU7UUFjRSxtQkFBQSxNQUFNLENBQUMsNkJBQTZCLENBQUMsQ0FBQSxFQUFFLG1CQUFBLFFBQVEsRUFBRSxDQUFBOztPQWJuRCxvQkFBb0IsQ0FtRmhDO0lBQUQsMkJBQUM7Q0FBQSxBQW5GRCxJQW1GQztTQW5GWSxvQkFBb0I7QUFxRmpDLE1BQU0sVUFBVSxpQkFBaUIsQ0FBQyxFQUFhO0lBQzdDLE9BQU8sQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0FBQ2pDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7TG9jYXRpb25DaGFuZ2VFdmVudCwgTG9jYXRpb25DaGFuZ2VMaXN0ZW5lciwgUGxhdGZvcm1Mb2NhdGlvbn0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7SW5qZWN0LCBJbmplY3RhYmxlLCBJbmplY3Rpb25Ub2tlbiwgT3B0aW9uYWx9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHtTdWJqZWN0fSBmcm9tICdyeGpzJztcblxuLyoqXG4gKiBQYXJzZXIgZnJvbSBodHRwczovL3Rvb2xzLmlldGYub3JnL2h0bWwvcmZjMzk4NiNhcHBlbmRpeC1CXG4gKiBeKChbXjovPyNdKyk6KT8oLy8oW14vPyNdKikpPyhbXj8jXSopKFxcPyhbXiNdKikpPygjKC4qKSk/XG4gKiAgMTIgICAgICAgICAgICAzICA0ICAgICAgICAgIDUgICAgICAgNiAgNyAgICAgICAgOCA5XG4gKlxuICogRXhhbXBsZTogaHR0cDovL3d3dy5pY3MudWNpLmVkdS9wdWIvaWV0Zi91cmkvI1JlbGF0ZWRcbiAqXG4gKiBSZXN1bHRzIGluOlxuICpcbiAqICQxID0gaHR0cDpcbiAqICQyID0gaHR0cFxuICogJDMgPSAvL3d3dy5pY3MudWNpLmVkdVxuICogJDQgPSB3d3cuaWNzLnVjaS5lZHVcbiAqICQ1ID0gL3B1Yi9pZXRmL3VyaS9cbiAqICQ2ID0gPHVuZGVmaW5lZD5cbiAqICQ3ID0gPHVuZGVmaW5lZD5cbiAqICQ4ID0gI1JlbGF0ZWRcbiAqICQ5ID0gUmVsYXRlZFxuICovXG5jb25zdCB1cmxQYXJzZSA9IC9eKChbXjpcXC8/I10rKTopPyhcXC9cXC8oW15cXC8/I10qKSk/KFtePyNdKikoXFw/KFteI10qKSk/KCMoLiopKT8vO1xuXG5mdW5jdGlvbiBwYXJzZVVybCh1cmxTdHI6IHN0cmluZywgYmFzZUhyZWY6IHN0cmluZykge1xuICBjb25zdCB2ZXJpZnlQcm90b2NvbCA9IC9eKChodHRwW3NdP3xmdHApOlxcL1xcLykvO1xuICBsZXQgc2VydmVyQmFzZTogc3RyaW5nfHVuZGVmaW5lZDtcblxuICAvLyBVUkwgY2xhc3MgcmVxdWlyZXMgZnVsbCBVUkwuIElmIHRoZSBVUkwgc3RyaW5nIGRvZXNuJ3Qgc3RhcnQgd2l0aCBwcm90b2NvbCwgd2UgbmVlZCB0byBhZGRcbiAgLy8gYW4gYXJiaXRyYXJ5IGJhc2UgVVJMIHdoaWNoIGNhbiBiZSByZW1vdmVkIGFmdGVyd2FyZC5cbiAgaWYgKCF2ZXJpZnlQcm90b2NvbC50ZXN0KHVybFN0cikpIHtcbiAgICBzZXJ2ZXJCYXNlID0gJ2h0dHA6Ly9lbXB0eS5jb20vJztcbiAgfVxuICBsZXQgcGFyc2VkVXJsOiB7XG4gICAgcHJvdG9jb2w6IHN0cmluZyxcbiAgICBob3N0bmFtZTogc3RyaW5nLFxuICAgIHBvcnQ6IHN0cmluZyxcbiAgICBwYXRobmFtZTogc3RyaW5nLFxuICAgIHNlYXJjaDogc3RyaW5nLFxuICAgIGhhc2g6IHN0cmluZ1xuICB9O1xuICB0cnkge1xuICAgIHBhcnNlZFVybCA9IG5ldyBVUkwodXJsU3RyLCBzZXJ2ZXJCYXNlKTtcbiAgfSBjYXRjaCAoZSkge1xuICAgIGNvbnN0IHJlc3VsdCA9IHVybFBhcnNlLmV4ZWMoc2VydmVyQmFzZSB8fCAnJyArIHVybFN0cik7XG4gICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgSW52YWxpZCBVUkw6ICR7dXJsU3RyfSB3aXRoIGJhc2U6ICR7YmFzZUhyZWZ9YCk7XG4gICAgfVxuICAgIGNvbnN0IGhvc3RTcGxpdCA9IHJlc3VsdFs0XS5zcGxpdCgnOicpO1xuICAgIHBhcnNlZFVybCA9IHtcbiAgICAgIHByb3RvY29sOiByZXN1bHRbMV0sXG4gICAgICBob3N0bmFtZTogaG9zdFNwbGl0WzBdLFxuICAgICAgcG9ydDogaG9zdFNwbGl0WzFdIHx8ICcnLFxuICAgICAgcGF0aG5hbWU6IHJlc3VsdFs1XSxcbiAgICAgIHNlYXJjaDogcmVzdWx0WzZdLFxuICAgICAgaGFzaDogcmVzdWx0WzhdLFxuICAgIH07XG4gIH1cbiAgaWYgKHBhcnNlZFVybC5wYXRobmFtZSAmJiBwYXJzZWRVcmwucGF0aG5hbWUuaW5kZXhPZihiYXNlSHJlZikgPT09IDApIHtcbiAgICBwYXJzZWRVcmwucGF0aG5hbWUgPSBwYXJzZWRVcmwucGF0aG5hbWUuc3Vic3RyaW5nKGJhc2VIcmVmLmxlbmd0aCk7XG4gIH1cbiAgcmV0dXJuIHtcbiAgICBob3N0bmFtZTogIXNlcnZlckJhc2UgJiYgcGFyc2VkVXJsLmhvc3RuYW1lIHx8ICcnLFxuICAgIHByb3RvY29sOiAhc2VydmVyQmFzZSAmJiBwYXJzZWRVcmwucHJvdG9jb2wgfHwgJycsXG4gICAgcG9ydDogIXNlcnZlckJhc2UgJiYgcGFyc2VkVXJsLnBvcnQgfHwgJycsXG4gICAgcGF0aG5hbWU6IHBhcnNlZFVybC5wYXRobmFtZSB8fCAnLycsXG4gICAgc2VhcmNoOiBwYXJzZWRVcmwuc2VhcmNoIHx8ICcnLFxuICAgIGhhc2g6IHBhcnNlZFVybC5oYXNoIHx8ICcnLFxuICB9O1xufVxuXG4vKipcbiAqIE1vY2sgcGxhdGZvcm0gbG9jYXRpb24gY29uZmlnXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgaW50ZXJmYWNlIE1vY2tQbGF0Zm9ybUxvY2F0aW9uQ29uZmlnIHtcbiAgc3RhcnRVcmw/OiBzdHJpbmc7XG4gIGFwcEJhc2VIcmVmPzogc3RyaW5nO1xufVxuXG4vKipcbiAqIFByb3ZpZGVyIGZvciBtb2NrIHBsYXRmb3JtIGxvY2F0aW9uIGNvbmZpZ1xuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNvbnN0IE1PQ0tfUExBVEZPUk1fTE9DQVRJT05fQ09ORklHID1cbiAgICBuZXcgSW5qZWN0aW9uVG9rZW48TW9ja1BsYXRmb3JtTG9jYXRpb25Db25maWc+KCdNT0NLX1BMQVRGT1JNX0xPQ0FUSU9OX0NPTkZJRycpO1xuXG4vKipcbiAqIE1vY2sgaW1wbGVtZW50YXRpb24gb2YgVVJMIHN0YXRlLlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIE1vY2tQbGF0Zm9ybUxvY2F0aW9uIGltcGxlbWVudHMgUGxhdGZvcm1Mb2NhdGlvbiB7XG4gIHByaXZhdGUgYmFzZUhyZWY6IHN0cmluZyA9ICcnO1xuICBwcml2YXRlIGhhc2hVcGRhdGUgPSBuZXcgU3ViamVjdDxMb2NhdGlvbkNoYW5nZUV2ZW50PigpO1xuICBwcml2YXRlIHVybENoYW5nZXM6IHtcbiAgICBob3N0bmFtZTogc3RyaW5nLFxuICAgIHByb3RvY29sOiBzdHJpbmcsXG4gICAgcG9ydDogc3RyaW5nLFxuICAgIHBhdGhuYW1lOiBzdHJpbmcsXG4gICAgc2VhcmNoOiBzdHJpbmcsXG4gICAgaGFzaDogc3RyaW5nLFxuICAgIHN0YXRlOiB1bmtub3duXG4gIH1bXSA9IFt7aG9zdG5hbWU6ICcnLCBwcm90b2NvbDogJycsIHBvcnQ6ICcnLCBwYXRobmFtZTogJy8nLCBzZWFyY2g6ICcnLCBoYXNoOiAnJywgc3RhdGU6IG51bGx9XTtcblxuICBjb25zdHJ1Y3RvcihASW5qZWN0KE1PQ0tfUExBVEZPUk1fTE9DQVRJT05fQ09ORklHKSBAT3B0aW9uYWwoKSBjb25maWc/OlxuICAgICAgICAgICAgICAgICAgTW9ja1BsYXRmb3JtTG9jYXRpb25Db25maWcpIHtcbiAgICBpZiAoY29uZmlnKSB7XG4gICAgICB0aGlzLmJhc2VIcmVmID0gY29uZmlnLmFwcEJhc2VIcmVmIHx8ICcnO1xuXG4gICAgICBjb25zdCBwYXJzZWRDaGFuZ2VzID1cbiAgICAgICAgICB0aGlzLnBhcnNlQ2hhbmdlcyhudWxsLCBjb25maWcuc3RhcnRVcmwgfHwgJ2h0dHA6Ly88ZW1wdHk+LycsIHRoaXMuYmFzZUhyZWYpO1xuICAgICAgdGhpcy51cmxDaGFuZ2VzWzBdID0gey4uLnBhcnNlZENoYW5nZXN9O1xuICAgIH1cbiAgfVxuXG4gIGdldCBob3N0bmFtZSgpIHsgcmV0dXJuIHRoaXMudXJsQ2hhbmdlc1swXS5ob3N0bmFtZTsgfVxuICBnZXQgcHJvdG9jb2woKSB7IHJldHVybiB0aGlzLnVybENoYW5nZXNbMF0ucHJvdG9jb2w7IH1cbiAgZ2V0IHBvcnQoKSB7IHJldHVybiB0aGlzLnVybENoYW5nZXNbMF0ucG9ydDsgfVxuICBnZXQgcGF0aG5hbWUoKSB7IHJldHVybiB0aGlzLnVybENoYW5nZXNbMF0ucGF0aG5hbWU7IH1cbiAgZ2V0IHNlYXJjaCgpIHsgcmV0dXJuIHRoaXMudXJsQ2hhbmdlc1swXS5zZWFyY2g7IH1cbiAgZ2V0IGhhc2goKSB7IHJldHVybiB0aGlzLnVybENoYW5nZXNbMF0uaGFzaDsgfVxuICBnZXQgc3RhdGUoKSB7IHJldHVybiB0aGlzLnVybENoYW5nZXNbMF0uc3RhdGU7IH1cblxuXG4gIGdldEJhc2VIcmVmRnJvbURPTSgpOiBzdHJpbmcgeyByZXR1cm4gdGhpcy5iYXNlSHJlZjsgfVxuXG4gIG9uUG9wU3RhdGUoZm46IExvY2F0aW9uQ2hhbmdlTGlzdGVuZXIpOiB2b2lkIHtcbiAgICAvLyBOby1vcDogYSBzdGF0ZSBzdGFjayBpcyBub3QgaW1wbGVtZW50ZWQsIHNvXG4gICAgLy8gbm8gZXZlbnRzIHdpbGwgZXZlciBjb21lLlxuICB9XG5cbiAgb25IYXNoQ2hhbmdlKGZuOiBMb2NhdGlvbkNoYW5nZUxpc3RlbmVyKTogdm9pZCB7IHRoaXMuaGFzaFVwZGF0ZS5zdWJzY3JpYmUoZm4pOyB9XG5cbiAgZ2V0IGhyZWYoKTogc3RyaW5nIHtcbiAgICBsZXQgdXJsID0gYCR7dGhpcy5wcm90b2NvbH0vLyR7dGhpcy5ob3N0bmFtZX0ke3RoaXMucG9ydCA/ICc6JyArIHRoaXMucG9ydCA6ICcnfWA7XG4gICAgdXJsICs9IGAke3RoaXMucGF0aG5hbWUgPT09ICcvJyA/ICcnIDogdGhpcy5wYXRobmFtZX0ke3RoaXMuc2VhcmNofSR7dGhpcy5oYXNofWA7XG4gICAgcmV0dXJuIHVybDtcbiAgfVxuXG4gIGdldCB1cmwoKTogc3RyaW5nIHsgcmV0dXJuIGAke3RoaXMucGF0aG5hbWV9JHt0aGlzLnNlYXJjaH0ke3RoaXMuaGFzaH1gOyB9XG5cbiAgcHJpdmF0ZSBwYXJzZUNoYW5nZXMoc3RhdGU6IHVua25vd24sIHVybDogc3RyaW5nLCBiYXNlSHJlZjogc3RyaW5nID0gJycpIHtcbiAgICAvLyBXaGVuIHRoZSBgaGlzdG9yeS5zdGF0ZWAgdmFsdWUgaXMgc3RvcmVkLCBpdCBpcyBhbHdheXMgY29waWVkLlxuICAgIHN0YXRlID0gSlNPTi5wYXJzZShKU09OLnN0cmluZ2lmeShzdGF0ZSkpO1xuICAgIHJldHVybiB7Li4ucGFyc2VVcmwodXJsLCBiYXNlSHJlZiksIHN0YXRlfTtcbiAgfVxuXG4gIHJlcGxhY2VTdGF0ZShzdGF0ZTogYW55LCB0aXRsZTogc3RyaW5nLCBuZXdVcmw6IHN0cmluZyk6IHZvaWQge1xuICAgIGNvbnN0IHtwYXRobmFtZSwgc2VhcmNoLCBzdGF0ZTogcGFyc2VkU3RhdGUsIGhhc2h9ID0gdGhpcy5wYXJzZUNoYW5nZXMoc3RhdGUsIG5ld1VybCk7XG5cbiAgICB0aGlzLnVybENoYW5nZXNbMF0gPSB7Li4udGhpcy51cmxDaGFuZ2VzWzBdLCBwYXRobmFtZSwgc2VhcmNoLCBoYXNoLCBzdGF0ZTogcGFyc2VkU3RhdGV9O1xuICB9XG5cbiAgcHVzaFN0YXRlKHN0YXRlOiBhbnksIHRpdGxlOiBzdHJpbmcsIG5ld1VybDogc3RyaW5nKTogdm9pZCB7XG4gICAgY29uc3Qge3BhdGhuYW1lLCBzZWFyY2gsIHN0YXRlOiBwYXJzZWRTdGF0ZSwgaGFzaH0gPSB0aGlzLnBhcnNlQ2hhbmdlcyhzdGF0ZSwgbmV3VXJsKTtcbiAgICB0aGlzLnVybENoYW5nZXMudW5zaGlmdCh7Li4udGhpcy51cmxDaGFuZ2VzWzBdLCBwYXRobmFtZSwgc2VhcmNoLCBoYXNoLCBzdGF0ZTogcGFyc2VkU3RhdGV9KTtcbiAgfVxuXG4gIGZvcndhcmQoKTogdm9pZCB7IHRocm93IG5ldyBFcnJvcignTm90IGltcGxlbWVudGVkJyk7IH1cblxuICBiYWNrKCk6IHZvaWQge1xuICAgIGNvbnN0IG9sZFVybCA9IHRoaXMudXJsO1xuICAgIGNvbnN0IG9sZEhhc2ggPSB0aGlzLmhhc2g7XG4gICAgdGhpcy51cmxDaGFuZ2VzLnNoaWZ0KCk7XG4gICAgY29uc3QgbmV3SGFzaCA9IHRoaXMuaGFzaDtcblxuICAgIGlmIChvbGRIYXNoICE9PSBuZXdIYXNoKSB7XG4gICAgICBzY2hlZHVsZU1pY3JvVGFzaygoKSA9PiB0aGlzLmhhc2hVcGRhdGUubmV4dCh7XG4gICAgICAgIHR5cGU6ICdoYXNoY2hhbmdlJywgc3RhdGU6IG51bGwsIG9sZFVybCwgbmV3VXJsOiB0aGlzLnVybFxuICAgICAgfSBhcyBMb2NhdGlvbkNoYW5nZUV2ZW50KSk7XG4gICAgfVxuICB9XG5cbiAgZ2V0U3RhdGUoKTogdW5rbm93biB7IHJldHVybiB0aGlzLnN0YXRlOyB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzY2hlZHVsZU1pY3JvVGFzayhjYjogKCkgPT4gYW55KSB7XG4gIFByb21pc2UucmVzb2x2ZShudWxsKS50aGVuKGNiKTtcbn0iXX0=