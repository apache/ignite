/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('@angular/core'), require('@angular/platform-browser'), require('@angular/animations'), require('@angular/animations/browser'), require('@angular/common')) :
    typeof define === 'function' && define.amd ? define('@angular/platform-browser/animations', ['exports', '@angular/core', '@angular/platform-browser', '@angular/animations', '@angular/animations/browser', '@angular/common'], factory) :
    (global = global || self, factory((global.ng = global.ng || {}, global.ng.platformBrowser = global.ng.platformBrowser || {}, global.ng.platformBrowser.animations = {}), global.ng.core, global.ng.platformBrowser, global.ng.animations, global.ng.animations.browser, global.ng.common));
}(this, function (exports, core, platformBrowser, animations, browser, common) { 'use strict';

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

    var BrowserAnimationBuilder = /** @class */ (function (_super) {
        __extends(BrowserAnimationBuilder, _super);
        function BrowserAnimationBuilder(rootRenderer, doc) {
            var _this = _super.call(this) || this;
            _this._nextAnimationId = 0;
            var typeData = {
                id: '0',
                encapsulation: core.ViewEncapsulation.None,
                styles: [],
                data: { animation: [] }
            };
            _this._renderer = rootRenderer.createRenderer(doc.body, typeData);
            return _this;
        }
        BrowserAnimationBuilder.prototype.build = function (animation) {
            var id = this._nextAnimationId.toString();
            this._nextAnimationId++;
            var entry = Array.isArray(animation) ? animations.sequence(animation) : animation;
            issueAnimationCommand(this._renderer, null, id, 'register', [entry]);
            return new BrowserAnimationFactory(id, this._renderer);
        };
        BrowserAnimationBuilder = __decorate([
            core.Injectable(),
            __param(1, core.Inject(common.DOCUMENT)),
            __metadata("design:paramtypes", [core.RendererFactory2, Object])
        ], BrowserAnimationBuilder);
        return BrowserAnimationBuilder;
    }(animations.AnimationBuilder));
    var BrowserAnimationFactory = /** @class */ (function (_super) {
        __extends(BrowserAnimationFactory, _super);
        function BrowserAnimationFactory(_id, _renderer) {
            var _this = _super.call(this) || this;
            _this._id = _id;
            _this._renderer = _renderer;
            return _this;
        }
        BrowserAnimationFactory.prototype.create = function (element, options) {
            return new RendererAnimationPlayer(this._id, element, options || {}, this._renderer);
        };
        return BrowserAnimationFactory;
    }(animations.AnimationFactory));
    var RendererAnimationPlayer = /** @class */ (function () {
        function RendererAnimationPlayer(id, element, options, _renderer) {
            this.id = id;
            this.element = element;
            this._renderer = _renderer;
            this.parentPlayer = null;
            this._started = false;
            this.totalTime = 0;
            this._command('create', options);
        }
        RendererAnimationPlayer.prototype._listen = function (eventName, callback) {
            return this._renderer.listen(this.element, "@@" + this.id + ":" + eventName, callback);
        };
        RendererAnimationPlayer.prototype._command = function (command) {
            var args = [];
            for (var _i = 1; _i < arguments.length; _i++) {
                args[_i - 1] = arguments[_i];
            }
            return issueAnimationCommand(this._renderer, this.element, this.id, command, args);
        };
        RendererAnimationPlayer.prototype.onDone = function (fn) { this._listen('done', fn); };
        RendererAnimationPlayer.prototype.onStart = function (fn) { this._listen('start', fn); };
        RendererAnimationPlayer.prototype.onDestroy = function (fn) { this._listen('destroy', fn); };
        RendererAnimationPlayer.prototype.init = function () { this._command('init'); };
        RendererAnimationPlayer.prototype.hasStarted = function () { return this._started; };
        RendererAnimationPlayer.prototype.play = function () {
            this._command('play');
            this._started = true;
        };
        RendererAnimationPlayer.prototype.pause = function () { this._command('pause'); };
        RendererAnimationPlayer.prototype.restart = function () { this._command('restart'); };
        RendererAnimationPlayer.prototype.finish = function () { this._command('finish'); };
        RendererAnimationPlayer.prototype.destroy = function () { this._command('destroy'); };
        RendererAnimationPlayer.prototype.reset = function () { this._command('reset'); };
        RendererAnimationPlayer.prototype.setPosition = function (p) { this._command('setPosition', p); };
        RendererAnimationPlayer.prototype.getPosition = function () { return 0; };
        return RendererAnimationPlayer;
    }());
    function issueAnimationCommand(renderer, element, id, command, args) {
        return renderer.setProperty(element, "@@" + id + ":" + command, args);
    }

    var ANIMATION_PREFIX = '@';
    var DISABLE_ANIMATIONS_FLAG = '@.disabled';
    var AnimationRendererFactory = /** @class */ (function () {
        function AnimationRendererFactory(delegate, engine, _zone) {
            this.delegate = delegate;
            this.engine = engine;
            this._zone = _zone;
            this._currentId = 0;
            this._microtaskId = 1;
            this._animationCallbacksBuffer = [];
            this._rendererCache = new Map();
            this._cdRecurDepth = 0;
            this.promise = Promise.resolve(0);
            engine.onRemovalComplete = function (element, delegate) {
                // Note: if an component element has a leave animation, and the component
                // a host leave animation, the view engine will call `removeChild` for the parent
                // component renderer as well as for the child component renderer.
                // Therefore, we need to check if we already removed the element.
                if (delegate && delegate.parentNode(element)) {
                    delegate.removeChild(element.parentNode, element);
                }
            };
        }
        AnimationRendererFactory.prototype.createRenderer = function (hostElement, type) {
            var _this = this;
            var EMPTY_NAMESPACE_ID = '';
            // cache the delegates to find out which cached delegate can
            // be used by which cached renderer
            var delegate = this.delegate.createRenderer(hostElement, type);
            if (!hostElement || !type || !type.data || !type.data['animation']) {
                var renderer = this._rendererCache.get(delegate);
                if (!renderer) {
                    renderer = new BaseAnimationRenderer(EMPTY_NAMESPACE_ID, delegate, this.engine);
                    // only cache this result when the base renderer is used
                    this._rendererCache.set(delegate, renderer);
                }
                return renderer;
            }
            var componentId = type.id;
            var namespaceId = type.id + '-' + this._currentId;
            this._currentId++;
            this.engine.register(namespaceId, hostElement);
            var animationTriggers = type.data['animation'];
            animationTriggers.forEach(function (trigger) { return _this.engine.registerTrigger(componentId, namespaceId, hostElement, trigger.name, trigger); });
            return new AnimationRenderer(this, namespaceId, delegate, this.engine);
        };
        AnimationRendererFactory.prototype.begin = function () {
            this._cdRecurDepth++;
            if (this.delegate.begin) {
                this.delegate.begin();
            }
        };
        AnimationRendererFactory.prototype._scheduleCountTask = function () {
            var _this = this;
            // always use promise to schedule microtask instead of use Zone
            this.promise.then(function () { _this._microtaskId++; });
        };
        /** @internal */
        AnimationRendererFactory.prototype.scheduleListenerCallback = function (count, fn, data) {
            var _this = this;
            if (count >= 0 && count < this._microtaskId) {
                this._zone.run(function () { return fn(data); });
                return;
            }
            if (this._animationCallbacksBuffer.length == 0) {
                Promise.resolve(null).then(function () {
                    _this._zone.run(function () {
                        _this._animationCallbacksBuffer.forEach(function (tuple) {
                            var _a = __read(tuple, 2), fn = _a[0], data = _a[1];
                            fn(data);
                        });
                        _this._animationCallbacksBuffer = [];
                    });
                });
            }
            this._animationCallbacksBuffer.push([fn, data]);
        };
        AnimationRendererFactory.prototype.end = function () {
            var _this = this;
            this._cdRecurDepth--;
            // this is to prevent animations from running twice when an inner
            // component does CD when a parent component instead has inserted it
            if (this._cdRecurDepth == 0) {
                this._zone.runOutsideAngular(function () {
                    _this._scheduleCountTask();
                    _this.engine.flush(_this._microtaskId);
                });
            }
            if (this.delegate.end) {
                this.delegate.end();
            }
        };
        AnimationRendererFactory.prototype.whenRenderingDone = function () { return this.engine.whenRenderingDone(); };
        AnimationRendererFactory = __decorate([
            core.Injectable(),
            __metadata("design:paramtypes", [core.RendererFactory2, browser.ɵAnimationEngine, core.NgZone])
        ], AnimationRendererFactory);
        return AnimationRendererFactory;
    }());
    var BaseAnimationRenderer = /** @class */ (function () {
        function BaseAnimationRenderer(namespaceId, delegate, engine) {
            this.namespaceId = namespaceId;
            this.delegate = delegate;
            this.engine = engine;
            this.destroyNode = this.delegate.destroyNode ? function (n) { return delegate.destroyNode(n); } : null;
        }
        Object.defineProperty(BaseAnimationRenderer.prototype, "data", {
            get: function () { return this.delegate.data; },
            enumerable: true,
            configurable: true
        });
        BaseAnimationRenderer.prototype.destroy = function () {
            this.engine.destroy(this.namespaceId, this.delegate);
            this.delegate.destroy();
        };
        BaseAnimationRenderer.prototype.createElement = function (name, namespace) {
            return this.delegate.createElement(name, namespace);
        };
        BaseAnimationRenderer.prototype.createComment = function (value) { return this.delegate.createComment(value); };
        BaseAnimationRenderer.prototype.createText = function (value) { return this.delegate.createText(value); };
        BaseAnimationRenderer.prototype.appendChild = function (parent, newChild) {
            this.delegate.appendChild(parent, newChild);
            this.engine.onInsert(this.namespaceId, newChild, parent, false);
        };
        BaseAnimationRenderer.prototype.insertBefore = function (parent, newChild, refChild) {
            this.delegate.insertBefore(parent, newChild, refChild);
            this.engine.onInsert(this.namespaceId, newChild, parent, true);
        };
        BaseAnimationRenderer.prototype.removeChild = function (parent, oldChild, isHostElement) {
            this.engine.onRemove(this.namespaceId, oldChild, this.delegate, isHostElement);
        };
        BaseAnimationRenderer.prototype.selectRootElement = function (selectorOrNode, preserveContent) {
            return this.delegate.selectRootElement(selectorOrNode, preserveContent);
        };
        BaseAnimationRenderer.prototype.parentNode = function (node) { return this.delegate.parentNode(node); };
        BaseAnimationRenderer.prototype.nextSibling = function (node) { return this.delegate.nextSibling(node); };
        BaseAnimationRenderer.prototype.setAttribute = function (el, name, value, namespace) {
            this.delegate.setAttribute(el, name, value, namespace);
        };
        BaseAnimationRenderer.prototype.removeAttribute = function (el, name, namespace) {
            this.delegate.removeAttribute(el, name, namespace);
        };
        BaseAnimationRenderer.prototype.addClass = function (el, name) { this.delegate.addClass(el, name); };
        BaseAnimationRenderer.prototype.removeClass = function (el, name) { this.delegate.removeClass(el, name); };
        BaseAnimationRenderer.prototype.setStyle = function (el, style, value, flags) {
            this.delegate.setStyle(el, style, value, flags);
        };
        BaseAnimationRenderer.prototype.removeStyle = function (el, style, flags) {
            this.delegate.removeStyle(el, style, flags);
        };
        BaseAnimationRenderer.prototype.setProperty = function (el, name, value) {
            if (name.charAt(0) == ANIMATION_PREFIX && name == DISABLE_ANIMATIONS_FLAG) {
                this.disableAnimations(el, !!value);
            }
            else {
                this.delegate.setProperty(el, name, value);
            }
        };
        BaseAnimationRenderer.prototype.setValue = function (node, value) { this.delegate.setValue(node, value); };
        BaseAnimationRenderer.prototype.listen = function (target, eventName, callback) {
            return this.delegate.listen(target, eventName, callback);
        };
        BaseAnimationRenderer.prototype.disableAnimations = function (element, value) {
            this.engine.disableAnimations(element, value);
        };
        return BaseAnimationRenderer;
    }());
    var AnimationRenderer = /** @class */ (function (_super) {
        __extends(AnimationRenderer, _super);
        function AnimationRenderer(factory, namespaceId, delegate, engine) {
            var _this = _super.call(this, namespaceId, delegate, engine) || this;
            _this.factory = factory;
            _this.namespaceId = namespaceId;
            return _this;
        }
        AnimationRenderer.prototype.setProperty = function (el, name, value) {
            if (name.charAt(0) == ANIMATION_PREFIX) {
                if (name.charAt(1) == '.' && name == DISABLE_ANIMATIONS_FLAG) {
                    value = value === undefined ? true : !!value;
                    this.disableAnimations(el, value);
                }
                else {
                    this.engine.process(this.namespaceId, el, name.substr(1), value);
                }
            }
            else {
                this.delegate.setProperty(el, name, value);
            }
        };
        AnimationRenderer.prototype.listen = function (target, eventName, callback) {
            var _a;
            var _this = this;
            if (eventName.charAt(0) == ANIMATION_PREFIX) {
                var element = resolveElementFromTarget(target);
                var name_1 = eventName.substr(1);
                var phase = '';
                // @listener.phase is for trigger animation callbacks
                // @@listener is for animation builder callbacks
                if (name_1.charAt(0) != ANIMATION_PREFIX) {
                    _a = __read(parseTriggerCallbackName(name_1), 2), name_1 = _a[0], phase = _a[1];
                }
                return this.engine.listen(this.namespaceId, element, name_1, phase, function (event) {
                    var countId = event['_data'] || -1;
                    _this.factory.scheduleListenerCallback(countId, callback, event);
                });
            }
            return this.delegate.listen(target, eventName, callback);
        };
        return AnimationRenderer;
    }(BaseAnimationRenderer));
    function resolveElementFromTarget(target) {
        switch (target) {
            case 'body':
                return document.body;
            case 'document':
                return document;
            case 'window':
                return window;
            default:
                return target;
        }
    }
    function parseTriggerCallbackName(triggerName) {
        var dotIndex = triggerName.indexOf('.');
        var trigger = triggerName.substring(0, dotIndex);
        var phase = triggerName.substr(dotIndex + 1);
        return [trigger, phase];
    }

    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var InjectableAnimationEngine = /** @class */ (function (_super) {
        __extends(InjectableAnimationEngine, _super);
        function InjectableAnimationEngine(doc, driver, normalizer) {
            return _super.call(this, doc.body, driver, normalizer) || this;
        }
        InjectableAnimationEngine = __decorate([
            core.Injectable(),
            __param(0, core.Inject(common.DOCUMENT)),
            __metadata("design:paramtypes", [Object, browser.AnimationDriver, browser.ɵAnimationStyleNormalizer])
        ], InjectableAnimationEngine);
        return InjectableAnimationEngine;
    }(browser.ɵAnimationEngine));
    function instantiateSupportedAnimationDriver() {
        return browser.ɵsupportsWebAnimations() ? new browser.ɵWebAnimationsDriver() : new browser.ɵCssKeyframesDriver();
    }
    function instantiateDefaultStyleNormalizer() {
        return new browser.ɵWebAnimationsStyleNormalizer();
    }
    function instantiateRendererFactory(renderer, engine, zone) {
        return new AnimationRendererFactory(renderer, engine, zone);
    }
    /**
     * @publicApi
     */
    var ANIMATION_MODULE_TYPE = new core.InjectionToken('AnimationModuleType');
    var SHARED_ANIMATION_PROVIDERS = [
        { provide: animations.AnimationBuilder, useClass: BrowserAnimationBuilder },
        { provide: browser.ɵAnimationStyleNormalizer, useFactory: instantiateDefaultStyleNormalizer },
        { provide: browser.ɵAnimationEngine, useClass: InjectableAnimationEngine }, {
            provide: core.RendererFactory2,
            useFactory: instantiateRendererFactory,
            deps: [platformBrowser.ɵDomRendererFactory2, browser.ɵAnimationEngine, core.NgZone]
        }
    ];
    /**
     * Separate providers from the actual module so that we can do a local modification in Google3 to
     * include them in the BrowserModule.
     */
    var BROWSER_ANIMATIONS_PROVIDERS = __spread([
        { provide: browser.AnimationDriver, useFactory: instantiateSupportedAnimationDriver },
        { provide: ANIMATION_MODULE_TYPE, useValue: 'BrowserAnimations' }
    ], SHARED_ANIMATION_PROVIDERS);
    /**
     * Separate providers from the actual module so that we can do a local modification in Google3 to
     * include them in the BrowserTestingModule.
     */
    var BROWSER_NOOP_ANIMATIONS_PROVIDERS = __spread([
        { provide: browser.AnimationDriver, useClass: browser.ɵNoopAnimationDriver },
        { provide: ANIMATION_MODULE_TYPE, useValue: 'NoopAnimations' }
    ], SHARED_ANIMATION_PROVIDERS);

    /**
     * Exports `BrowserModule` with additional [dependency-injection providers](guide/glossary#provider)
     * for use with animations. See [Animations](guide/animations).
     * @publicApi
     */
    var BrowserAnimationsModule = /** @class */ (function () {
        function BrowserAnimationsModule() {
        }
        BrowserAnimationsModule = __decorate([
            core.NgModule({
                exports: [platformBrowser.BrowserModule],
                providers: BROWSER_ANIMATIONS_PROVIDERS,
            })
        ], BrowserAnimationsModule);
        return BrowserAnimationsModule;
    }());
    /**
     * A null player that must be imported to allow disabling of animations.
     * @publicApi
     */
    var NoopAnimationsModule = /** @class */ (function () {
        function NoopAnimationsModule() {
        }
        NoopAnimationsModule = __decorate([
            core.NgModule({
                exports: [platformBrowser.BrowserModule],
                providers: BROWSER_NOOP_ANIMATIONS_PROVIDERS,
            })
        ], NoopAnimationsModule);
        return NoopAnimationsModule;
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

    exports.ɵangular_packages_platform_browser_animations_animations_f = BaseAnimationRenderer;
    exports.ɵangular_packages_platform_browser_animations_animations_d = BROWSER_ANIMATIONS_PROVIDERS;
    exports.ɵangular_packages_platform_browser_animations_animations_e = BROWSER_NOOP_ANIMATIONS_PROVIDERS;
    exports.ɵangular_packages_platform_browser_animations_animations_b = instantiateDefaultStyleNormalizer;
    exports.ɵangular_packages_platform_browser_animations_animations_c = instantiateRendererFactory;
    exports.ɵangular_packages_platform_browser_animations_animations_a = instantiateSupportedAnimationDriver;
    exports.BrowserAnimationsModule = BrowserAnimationsModule;
    exports.NoopAnimationsModule = NoopAnimationsModule;
    exports.ANIMATION_MODULE_TYPE = ANIMATION_MODULE_TYPE;
    exports.ɵBrowserAnimationBuilder = BrowserAnimationBuilder;
    exports.ɵBrowserAnimationFactory = BrowserAnimationFactory;
    exports.ɵAnimationRenderer = AnimationRenderer;
    exports.ɵAnimationRendererFactory = AnimationRendererFactory;
    exports.ɵInjectableAnimationEngine = InjectableAnimationEngine;

    Object.defineProperty(exports, '__esModule', { value: true });

}));
//# sourceMappingURL=platform-browser-animations.umd.js.map
