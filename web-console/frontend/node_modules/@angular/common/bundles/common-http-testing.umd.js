/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('@angular/common/http'), require('@angular/core'), require('rxjs')) :
    typeof define === 'function' && define.amd ? define('@angular/common/http/testing', ['exports', '@angular/common/http', '@angular/core', 'rxjs'], factory) :
    (global = global || self, factory((global.ng = global.ng || {}, global.ng.common = global.ng.common || {}, global.ng.common.http = global.ng.common.http || {}, global.ng.common.http.testing = {}), global.ng.common.http, global.ng.core, global.rxjs));
}(this, function (exports, http, core, rxjs) { 'use strict';

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * Controller to be injected into tests, that allows for mocking and flushing
     * of requests.
     *
     * @publicApi
     */
    var HttpTestingController = /** @class */ (function () {
        function HttpTestingController() {
        }
        return HttpTestingController;
    }());

    /*! *****************************************************************************
    Copyright (c) Microsoft Corporation. All rights reserved.
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use
    this file except in compliance with the License. You may obtain a copy of the
    License at http://www.apache.org/licenses/LICENSE-2.0

    THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
    WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
    MERCHANTABLITY OR NON-INFRINGEMENT.

    See the Apache Version 2.0 License for specific language governing permissions
    and limitations under the License.
    ***************************************************************************** */
    /* global Reflect, Promise */

    var extendStatics = function(d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };

    function __extends(d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    }

    var __assign = function() {
        __assign = Object.assign || function __assign(t) {
            for (var s, i = 1, n = arguments.length; i < n; i++) {
                s = arguments[i];
                for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p)) t[p] = s[p];
            }
            return t;
        };
        return __assign.apply(this, arguments);
    };

    function __rest(s, e) {
        var t = {};
        for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
            t[p] = s[p];
        if (s != null && typeof Object.getOwnPropertySymbols === "function")
            for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) if (e.indexOf(p[i]) < 0)
                t[p[i]] = s[p[i]];
        return t;
    }

    function __decorate(decorators, target, key, desc) {
        var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
        if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
        else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
        return c > 3 && r && Object.defineProperty(target, key, r), r;
    }

    function __param(paramIndex, decorator) {
        return function (target, key) { decorator(target, key, paramIndex); }
    }

    function __metadata(metadataKey, metadataValue) {
        if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(metadataKey, metadataValue);
    }

    function __awaiter(thisArg, _arguments, P, generator) {
        return new (P || (P = Promise))(function (resolve, reject) {
            function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
            function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
            function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
            step((generator = generator.apply(thisArg, _arguments || [])).next());
        });
    }

    function __generator(thisArg, body) {
        var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
        return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
        function verb(n) { return function (v) { return step([n, v]); }; }
        function step(op) {
            if (f) throw new TypeError("Generator is already executing.");
            while (_) try {
                if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
                if (y = 0, t) op = [op[0] & 2, t.value];
                switch (op[0]) {
                    case 0: case 1: t = op; break;
                    case 4: _.label++; return { value: op[1], done: false };
                    case 5: _.label++; y = op[1]; op = [0]; continue;
                    case 7: op = _.ops.pop(); _.trys.pop(); continue;
                    default:
                        if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                        if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                        if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                        if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                        if (t[2]) _.ops.pop();
                        _.trys.pop(); continue;
                }
                op = body.call(thisArg, _);
            } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
            if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
        }
    }

    function __exportStar(m, exports) {
        for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
    }

    function __values(o) {
        var m = typeof Symbol === "function" && o[Symbol.iterator], i = 0;
        if (m) return m.call(o);
        return {
            next: function () {
                if (o && i >= o.length) o = void 0;
                return { value: o && o[i++], done: !o };
            }
        };
    }

    function __read(o, n) {
        var m = typeof Symbol === "function" && o[Symbol.iterator];
        if (!m) return o;
        var i = m.call(o), r, ar = [], e;
        try {
            while ((n === void 0 || n-- > 0) && !(r = i.next()).done) ar.push(r.value);
        }
        catch (error) { e = { error: error }; }
        finally {
            try {
                if (r && !r.done && (m = i["return"])) m.call(i);
            }
            finally { if (e) throw e.error; }
        }
        return ar;
    }

    function __spread() {
        for (var ar = [], i = 0; i < arguments.length; i++)
            ar = ar.concat(__read(arguments[i]));
        return ar;
    }

    function __await(v) {
        return this instanceof __await ? (this.v = v, this) : new __await(v);
    }

    function __asyncGenerator(thisArg, _arguments, generator) {
        if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
        var g = generator.apply(thisArg, _arguments || []), i, q = [];
        return i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i;
        function verb(n) { if (g[n]) i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; }
        function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
        function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
        function fulfill(value) { resume("next", value); }
        function reject(value) { resume("throw", value); }
        function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
    }

    function __asyncDelegator(o) {
        var i, p;
        return i = {}, verb("next"), verb("throw", function (e) { throw e; }), verb("return"), i[Symbol.iterator] = function () { return this; }, i;
        function verb(n, f) { i[n] = o[n] ? function (v) { return (p = !p) ? { value: __await(o[n](v)), done: n === "return" } : f ? f(v) : v; } : f; }
    }

    function __asyncValues(o) {
        if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
        var m = o[Symbol.asyncIterator], i;
        return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
        function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
        function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
    }

    function __makeTemplateObject(cooked, raw) {
        if (Object.defineProperty) { Object.defineProperty(cooked, "raw", { value: raw }); } else { cooked.raw = raw; }
        return cooked;
    };

    function __importStar(mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
        result.default = mod;
        return result;
    }

    function __importDefault(mod) {
        return (mod && mod.__esModule) ? mod : { default: mod };
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * A mock requests that was received and is ready to be answered.
     *
     * This interface allows access to the underlying `HttpRequest`, and allows
     * responding with `HttpEvent`s or `HttpErrorResponse`s.
     *
     * @publicApi
     */
    var TestRequest = /** @class */ (function () {
        function TestRequest(request, observer) {
            this.request = request;
            this.observer = observer;
            /**
             * @internal set by `HttpClientTestingBackend`
             */
            this._cancelled = false;
        }
        Object.defineProperty(TestRequest.prototype, "cancelled", {
            /**
             * Whether the request was cancelled after it was sent.
             */
            get: function () { return this._cancelled; },
            enumerable: true,
            configurable: true
        });
        /**
         * Resolve the request by returning a body plus additional HTTP information (such as response
         * headers) if provided.
         * If the request specifies an expected body type, the body is converted into the requested type.
         * Otherwise, the body is converted to `JSON` by default.
         *
         * Both successful and unsuccessful responses can be delivered via `flush()`.
         */
        TestRequest.prototype.flush = function (body, opts) {
            if (opts === void 0) { opts = {}; }
            if (this.cancelled) {
                throw new Error("Cannot flush a cancelled request.");
            }
            var url = this.request.urlWithParams;
            var headers = (opts.headers instanceof http.HttpHeaders) ? opts.headers : new http.HttpHeaders(opts.headers);
            body = _maybeConvertBody(this.request.responseType, body);
            var statusText = opts.statusText;
            var status = opts.status !== undefined ? opts.status : 200;
            if (opts.status === undefined) {
                if (body === null) {
                    status = 204;
                    statusText = statusText || 'No Content';
                }
                else {
                    statusText = statusText || 'OK';
                }
            }
            if (statusText === undefined) {
                throw new Error('statusText is required when setting a custom status.');
            }
            if (status >= 200 && status < 300) {
                this.observer.next(new http.HttpResponse({ body: body, headers: headers, status: status, statusText: statusText, url: url }));
                this.observer.complete();
            }
            else {
                this.observer.error(new http.HttpErrorResponse({ error: body, headers: headers, status: status, statusText: statusText, url: url }));
            }
        };
        /**
         * Resolve the request by returning an `ErrorEvent` (e.g. simulating a network failure).
         */
        TestRequest.prototype.error = function (error, opts) {
            if (opts === void 0) { opts = {}; }
            if (this.cancelled) {
                throw new Error("Cannot return an error for a cancelled request.");
            }
            if (opts.status && opts.status >= 200 && opts.status < 300) {
                throw new Error("error() called with a successful status.");
            }
            var headers = (opts.headers instanceof http.HttpHeaders) ? opts.headers : new http.HttpHeaders(opts.headers);
            this.observer.error(new http.HttpErrorResponse({
                error: error,
                headers: headers,
                status: opts.status || 0,
                statusText: opts.statusText || '',
                url: this.request.urlWithParams,
            }));
        };
        /**
         * Deliver an arbitrary `HttpEvent` (such as a progress event) on the response stream for this
         * request.
         */
        TestRequest.prototype.event = function (event) {
            if (this.cancelled) {
                throw new Error("Cannot send events to a cancelled request.");
            }
            this.observer.next(event);
        };
        return TestRequest;
    }());
    /**
     * Helper function to convert a response body to an ArrayBuffer.
     */
    function _toArrayBufferBody(body) {
        if (typeof ArrayBuffer === 'undefined') {
            throw new Error('ArrayBuffer responses are not supported on this platform.');
        }
        if (body instanceof ArrayBuffer) {
            return body;
        }
        throw new Error('Automatic conversion to ArrayBuffer is not supported for response type.');
    }
    /**
     * Helper function to convert a response body to a Blob.
     */
    function _toBlob(body) {
        if (typeof Blob === 'undefined') {
            throw new Error('Blob responses are not supported on this platform.');
        }
        if (body instanceof Blob) {
            return body;
        }
        if (ArrayBuffer && body instanceof ArrayBuffer) {
            return new Blob([body]);
        }
        throw new Error('Automatic conversion to Blob is not supported for response type.');
    }
    /**
     * Helper function to convert a response body to JSON data.
     */
    function _toJsonBody(body, format) {
        if (format === void 0) { format = 'JSON'; }
        if (typeof ArrayBuffer !== 'undefined' && body instanceof ArrayBuffer) {
            throw new Error("Automatic conversion to " + format + " is not supported for ArrayBuffers.");
        }
        if (typeof Blob !== 'undefined' && body instanceof Blob) {
            throw new Error("Automatic conversion to " + format + " is not supported for Blobs.");
        }
        if (typeof body === 'string' || typeof body === 'number' || typeof body === 'object' ||
            Array.isArray(body)) {
            return body;
        }
        throw new Error("Automatic conversion to " + format + " is not supported for response type.");
    }
    /**
     * Helper function to convert a response body to a string.
     */
    function _toTextBody(body) {
        if (typeof body === 'string') {
            return body;
        }
        if (typeof ArrayBuffer !== 'undefined' && body instanceof ArrayBuffer) {
            throw new Error('Automatic conversion to text is not supported for ArrayBuffers.');
        }
        if (typeof Blob !== 'undefined' && body instanceof Blob) {
            throw new Error('Automatic conversion to text is not supported for Blobs.');
        }
        return JSON.stringify(_toJsonBody(body, 'text'));
    }
    /**
     * Convert a response body to the requested type.
     */
    function _maybeConvertBody(responseType, body) {
        if (body === null) {
            return null;
        }
        switch (responseType) {
            case 'arraybuffer':
                return _toArrayBufferBody(body);
            case 'blob':
                return _toBlob(body);
            case 'json':
                return _toJsonBody(body);
            case 'text':
                return _toTextBody(body);
            default:
                throw new Error("Unsupported responseType: " + responseType);
        }
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * A testing backend for `HttpClient` which both acts as an `HttpBackend`
     * and as the `HttpTestingController`.
     *
     * `HttpClientTestingBackend` works by keeping a list of all open requests.
     * As requests come in, they're added to the list. Users can assert that specific
     * requests were made and then flush them. In the end, a verify() method asserts
     * that no unexpected requests were made.
     *
     *
     */
    var HttpClientTestingBackend = /** @class */ (function () {
        function HttpClientTestingBackend() {
            /**
             * List of pending requests which have not yet been expected.
             */
            this.open = [];
        }
        /**
         * Handle an incoming request by queueing it in the list of open requests.
         */
        HttpClientTestingBackend.prototype.handle = function (req) {
            var _this = this;
            return new rxjs.Observable(function (observer) {
                var testReq = new TestRequest(req, observer);
                _this.open.push(testReq);
                observer.next({ type: http.HttpEventType.Sent });
                return function () { testReq._cancelled = true; };
            });
        };
        /**
         * Helper function to search for requests in the list of open requests.
         */
        HttpClientTestingBackend.prototype._match = function (match) {
            if (typeof match === 'string') {
                return this.open.filter(function (testReq) { return testReq.request.urlWithParams === match; });
            }
            else if (typeof match === 'function') {
                return this.open.filter(function (testReq) { return match(testReq.request); });
            }
            else {
                return this.open.filter(function (testReq) { return (!match.method || testReq.request.method === match.method.toUpperCase()) &&
                    (!match.url || testReq.request.urlWithParams === match.url); });
            }
        };
        /**
         * Search for requests in the list of open requests, and return all that match
         * without asserting anything about the number of matches.
         */
        HttpClientTestingBackend.prototype.match = function (match) {
            var _this = this;
            var results = this._match(match);
            results.forEach(function (result) {
                var index = _this.open.indexOf(result);
                if (index !== -1) {
                    _this.open.splice(index, 1);
                }
            });
            return results;
        };
        /**
         * Expect that a single outstanding request matches the given matcher, and return
         * it.
         *
         * Requests returned through this API will no longer be in the list of open requests,
         * and thus will not match twice.
         */
        HttpClientTestingBackend.prototype.expectOne = function (match, description) {
            description = description || this.descriptionFromMatcher(match);
            var matches = this.match(match);
            if (matches.length > 1) {
                throw new Error("Expected one matching request for criteria \"" + description + "\", found " + matches.length + " requests.");
            }
            if (matches.length === 0) {
                throw new Error("Expected one matching request for criteria \"" + description + "\", found none.");
            }
            return matches[0];
        };
        /**
         * Expect that no outstanding requests match the given matcher, and throw an error
         * if any do.
         */
        HttpClientTestingBackend.prototype.expectNone = function (match, description) {
            description = description || this.descriptionFromMatcher(match);
            var matches = this.match(match);
            if (matches.length > 0) {
                throw new Error("Expected zero matching requests for criteria \"" + description + "\", found " + matches.length + ".");
            }
        };
        /**
         * Validate that there are no outstanding requests.
         */
        HttpClientTestingBackend.prototype.verify = function (opts) {
            if (opts === void 0) { opts = {}; }
            var open = this.open;
            // It's possible that some requests may be cancelled, and this is expected.
            // The user can ask to ignore open requests which have been cancelled.
            if (opts.ignoreCancelled) {
                open = open.filter(function (testReq) { return !testReq.cancelled; });
            }
            if (open.length > 0) {
                // Show the methods and URLs of open requests in the error, for convenience.
                var requests = open.map(function (testReq) {
                    var url = testReq.request.urlWithParams.split('?')[0];
                    var method = testReq.request.method;
                    return method + " " + url;
                })
                    .join(', ');
                throw new Error("Expected no open requests, found " + open.length + ": " + requests);
            }
        };
        HttpClientTestingBackend.prototype.descriptionFromMatcher = function (matcher) {
            if (typeof matcher === 'string') {
                return "Match URL: " + matcher;
            }
            else if (typeof matcher === 'object') {
                var method = matcher.method || '(any)';
                var url = matcher.url || '(any)';
                return "Match method: " + method + ", URL: " + url;
            }
            else {
                return "Match by function: " + matcher.name;
            }
        };
        HttpClientTestingBackend = __decorate([
            core.Injectable()
        ], HttpClientTestingBackend);
        return HttpClientTestingBackend;
    }());

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    /**
     * Configures `HttpClientTestingBackend` as the `HttpBackend` used by `HttpClient`.
     *
     * Inject `HttpTestingController` to expect and flush requests in your tests.
     *
     * @publicApi
     */
    var HttpClientTestingModule = /** @class */ (function () {
        function HttpClientTestingModule() {
        }
        HttpClientTestingModule = __decorate([
            core.NgModule({
                imports: [
                    http.HttpClientModule,
                ],
                providers: [
                    HttpClientTestingBackend,
                    { provide: http.HttpBackend, useExisting: HttpClientTestingBackend },
                    { provide: HttpTestingController, useExisting: HttpClientTestingBackend },
                ],
            })
        ], HttpClientTestingModule);
        return HttpClientTestingModule;
    }());

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

    /**
     * Generated bundle index. Do not edit.
     */

    exports.ɵangular_packages_common_http_testing_testing_a = HttpClientTestingBackend;
    exports.HttpTestingController = HttpTestingController;
    exports.HttpClientTestingModule = HttpClientTestingModule;
    exports.TestRequest = TestRequest;

    Object.defineProperty(exports, '__esModule', { value: true });

}));
//# sourceMappingURL=common-http-testing.umd.js.map
