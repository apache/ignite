/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { __extends, __decorate, __param, __metadata, __read, __spread } from 'tslib';
import { ViewEncapsulation, Injectable, Inject, RendererFactory2, NgZone, InjectionToken, NgModule } from '@angular/core';
import { ɵDomRendererFactory2, BrowserModule } from '@angular/platform-browser';
import { sequence, AnimationBuilder, AnimationFactory } from '@angular/animations';
import { ɵAnimationEngine, AnimationDriver, ɵAnimationStyleNormalizer, ɵsupportsWebAnimations, ɵWebAnimationsDriver, ɵCssKeyframesDriver, ɵWebAnimationsStyleNormalizer, ɵNoopAnimationDriver } from '@angular/animations/browser';
import { DOCUMENT } from '@angular/common';

var BrowserAnimationBuilder = /** @class */ (function (_super) {
    __extends(BrowserAnimationBuilder, _super);
    function BrowserAnimationBuilder(rootRenderer, doc) {
        var _this = _super.call(this) || this;
        _this._nextAnimationId = 0;
        var typeData = {
            id: '0',
            encapsulation: ViewEncapsulation.None,
            styles: [],
            data: { animation: [] }
        };
        _this._renderer = rootRenderer.createRenderer(doc.body, typeData);
        return _this;
    }
    BrowserAnimationBuilder.prototype.build = function (animation) {
        var id = this._nextAnimationId.toString();
        this._nextAnimationId++;
        var entry = Array.isArray(animation) ? sequence(animation) : animation;
        issueAnimationCommand(this._renderer, null, id, 'register', [entry]);
        return new BrowserAnimationFactory(id, this._renderer);
    };
    BrowserAnimationBuilder = __decorate([
        Injectable(),
        __param(1, Inject(DOCUMENT)),
        __metadata("design:paramtypes", [RendererFactory2, Object])
    ], BrowserAnimationBuilder);
    return BrowserAnimationBuilder;
}(AnimationBuilder));
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
}(AnimationFactory));
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
        Injectable(),
        __metadata("design:paramtypes", [RendererFactory2, ɵAnimationEngine, NgZone])
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
        Injectable(),
        __param(0, Inject(DOCUMENT)),
        __metadata("design:paramtypes", [Object, AnimationDriver, ɵAnimationStyleNormalizer])
    ], InjectableAnimationEngine);
    return InjectableAnimationEngine;
}(ɵAnimationEngine));
function instantiateSupportedAnimationDriver() {
    return ɵsupportsWebAnimations() ? new ɵWebAnimationsDriver() : new ɵCssKeyframesDriver();
}
function instantiateDefaultStyleNormalizer() {
    return new ɵWebAnimationsStyleNormalizer();
}
function instantiateRendererFactory(renderer, engine, zone) {
    return new AnimationRendererFactory(renderer, engine, zone);
}
/**
 * @publicApi
 */
var ANIMATION_MODULE_TYPE = new InjectionToken('AnimationModuleType');
var SHARED_ANIMATION_PROVIDERS = [
    { provide: AnimationBuilder, useClass: BrowserAnimationBuilder },
    { provide: ɵAnimationStyleNormalizer, useFactory: instantiateDefaultStyleNormalizer },
    { provide: ɵAnimationEngine, useClass: InjectableAnimationEngine }, {
        provide: RendererFactory2,
        useFactory: instantiateRendererFactory,
        deps: [ɵDomRendererFactory2, ɵAnimationEngine, NgZone]
    }
];
/**
 * Separate providers from the actual module so that we can do a local modification in Google3 to
 * include them in the BrowserModule.
 */
var BROWSER_ANIMATIONS_PROVIDERS = __spread([
    { provide: AnimationDriver, useFactory: instantiateSupportedAnimationDriver },
    { provide: ANIMATION_MODULE_TYPE, useValue: 'BrowserAnimations' }
], SHARED_ANIMATION_PROVIDERS);
/**
 * Separate providers from the actual module so that we can do a local modification in Google3 to
 * include them in the BrowserTestingModule.
 */
var BROWSER_NOOP_ANIMATIONS_PROVIDERS = __spread([
    { provide: AnimationDriver, useClass: ɵNoopAnimationDriver },
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
        NgModule({
            exports: [BrowserModule],
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
        NgModule({
            exports: [BrowserModule],
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

export { BaseAnimationRenderer as ɵangular_packages_platform_browser_animations_animations_f, BROWSER_ANIMATIONS_PROVIDERS as ɵangular_packages_platform_browser_animations_animations_d, BROWSER_NOOP_ANIMATIONS_PROVIDERS as ɵangular_packages_platform_browser_animations_animations_e, instantiateDefaultStyleNormalizer as ɵangular_packages_platform_browser_animations_animations_b, instantiateRendererFactory as ɵangular_packages_platform_browser_animations_animations_c, instantiateSupportedAnimationDriver as ɵangular_packages_platform_browser_animations_animations_a, BrowserAnimationsModule, NoopAnimationsModule, ANIMATION_MODULE_TYPE, BrowserAnimationBuilder as ɵBrowserAnimationBuilder, BrowserAnimationFactory as ɵBrowserAnimationFactory, AnimationRenderer as ɵAnimationRenderer, AnimationRendererFactory as ɵAnimationRendererFactory, InjectableAnimationEngine as ɵInjectableAnimationEngine };
//# sourceMappingURL=animations.js.map
