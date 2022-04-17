/**
 * @license Angular v8.2.14
 * (c) 2010-2019 Google LLC. https://angular.io/
 * License: MIT
 */

import { ɵparseCookieValue, PlatformLocation, DOCUMENT, isPlatformServer, ɵPLATFORM_BROWSER_ID, CommonModule } from '@angular/common';
import { ɵglobal, Injectable, Inject, InjectionToken, ApplicationInitStatus, APP_INITIALIZER, Injector, setTestabilityGetter, ApplicationRef, NgZone, getDebugNode, NgProbeToken, Optional, ViewEncapsulation, APP_ID, RendererStyleFlags2, PLATFORM_ID, ɵConsole, SecurityContext, ɵ_sanitizeHtml, ɵ_sanitizeStyle, ɵ_sanitizeUrl, PLATFORM_INITIALIZER, Sanitizer, createPlatformFactory, platformCore, ErrorHandler, ɵAPP_ROOT, RendererFactory2, Testability, NgModule, ApplicationModule, SkipSelf, ɵɵinject, ɵɵdefineInjectable, Version } from '@angular/core';

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
/** @type {?} */
let _DOM = (/** @type {?} */ (null));
/**
 * @return {?}
 */
function getDOM() {
    return _DOM;
}
/**
 * @param {?} adapter
 * @return {?}
 */
function setDOM(adapter) {
    _DOM = adapter;
}
/**
 * @param {?} adapter
 * @return {?}
 */
function setRootDomAdapter(adapter) {
    if (!_DOM) {
        _DOM = adapter;
    }
}
/* tslint:disable:requireParameterType */
/**
 * Provides DOM operations in an environment-agnostic way.
 *
 * \@security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 * @abstract
 */
class DomAdapter {
    constructor() {
        this.resourceLoaderType = (/** @type {?} */ (null));
    }
    /**
     * Maps attribute names to their corresponding property names for cases
     * where attribute name doesn't match property name.
     * @return {?}
     */
    get attrToPropMap() { return this._attrToPropMap; }
    /**
     * @param {?} value
     * @return {?}
     */
    set attrToPropMap(value) { this._attrToPropMap = value; }
}
if (false) {
    /** @type {?} */
    DomAdapter.prototype.resourceLoaderType;
    /**
     * \@internal
     * @type {?}
     */
    DomAdapter.prototype._attrToPropMap;
    /**
     * @abstract
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    DomAdapter.prototype.hasProperty = function (element, name) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setProperty = function (el, name, value) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} name
     * @return {?}
     */
    DomAdapter.prototype.getProperty = function (el, name) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} methodName
     * @param {?} args
     * @return {?}
     */
    DomAdapter.prototype.invoke = function (el, methodName, args) { };
    /**
     * @abstract
     * @param {?} error
     * @return {?}
     */
    DomAdapter.prototype.logError = function (error) { };
    /**
     * @abstract
     * @param {?} error
     * @return {?}
     */
    DomAdapter.prototype.log = function (error) { };
    /**
     * @abstract
     * @param {?} error
     * @return {?}
     */
    DomAdapter.prototype.logGroup = function (error) { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.logGroupEnd = function () { };
    /**
     * @abstract
     * @param {?} nodeA
     * @param {?} nodeB
     * @return {?}
     */
    DomAdapter.prototype.contains = function (nodeA, nodeB) { };
    /**
     * @abstract
     * @param {?} templateHtml
     * @return {?}
     */
    DomAdapter.prototype.parse = function (templateHtml) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} selector
     * @return {?}
     */
    DomAdapter.prototype.querySelector = function (el, selector) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} selector
     * @return {?}
     */
    DomAdapter.prototype.querySelectorAll = function (el, selector) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} evt
     * @param {?} listener
     * @return {?}
     */
    DomAdapter.prototype.on = function (el, evt, listener) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} evt
     * @param {?} listener
     * @return {?}
     */
    DomAdapter.prototype.onAndCancel = function (el, evt, listener) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} evt
     * @return {?}
     */
    DomAdapter.prototype.dispatchEvent = function (el, evt) { };
    /**
     * @abstract
     * @param {?} eventType
     * @return {?}
     */
    DomAdapter.prototype.createMouseEvent = function (eventType) { };
    /**
     * @abstract
     * @param {?} eventType
     * @return {?}
     */
    DomAdapter.prototype.createEvent = function (eventType) { };
    /**
     * @abstract
     * @param {?} evt
     * @return {?}
     */
    DomAdapter.prototype.preventDefault = function (evt) { };
    /**
     * @abstract
     * @param {?} evt
     * @return {?}
     */
    DomAdapter.prototype.isPrevented = function (evt) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getInnerHTML = function (el) { };
    /**
     * Returns content if el is a <template> element, null otherwise.
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getTemplateContent = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getOuterHTML = function (el) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.nodeName = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.nodeValue = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.type = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.content = function (node) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.firstChild = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.nextSibling = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.parentElement = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.childNodes = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.childNodesAsList = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.clearNodes = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.appendChild = function (el, node) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.removeChild = function (el, node) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} newNode
     * @param {?} oldNode
     * @return {?}
     */
    DomAdapter.prototype.replaceChild = function (el, newNode, oldNode) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.remove = function (el) { };
    /**
     * @abstract
     * @param {?} parent
     * @param {?} ref
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.insertBefore = function (parent, ref, node) { };
    /**
     * @abstract
     * @param {?} parent
     * @param {?} ref
     * @param {?} nodes
     * @return {?}
     */
    DomAdapter.prototype.insertAllBefore = function (parent, ref, nodes) { };
    /**
     * @abstract
     * @param {?} parent
     * @param {?} el
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.insertAfter = function (parent, el, node) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setInnerHTML = function (el, value) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getText = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setText = function (el, value) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getValue = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setValue = function (el, value) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getChecked = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setChecked = function (el, value) { };
    /**
     * @abstract
     * @param {?} text
     * @return {?}
     */
    DomAdapter.prototype.createComment = function (text) { };
    /**
     * @abstract
     * @param {?} html
     * @return {?}
     */
    DomAdapter.prototype.createTemplate = function (html) { };
    /**
     * @abstract
     * @param {?} tagName
     * @param {?=} doc
     * @return {?}
     */
    DomAdapter.prototype.createElement = function (tagName, doc) { };
    /**
     * @abstract
     * @param {?} ns
     * @param {?} tagName
     * @param {?=} doc
     * @return {?}
     */
    DomAdapter.prototype.createElementNS = function (ns, tagName, doc) { };
    /**
     * @abstract
     * @param {?} text
     * @param {?=} doc
     * @return {?}
     */
    DomAdapter.prototype.createTextNode = function (text, doc) { };
    /**
     * @abstract
     * @param {?} attrName
     * @param {?} attrValue
     * @param {?=} doc
     * @return {?}
     */
    DomAdapter.prototype.createScriptTag = function (attrName, attrValue, doc) { };
    /**
     * @abstract
     * @param {?} css
     * @param {?=} doc
     * @return {?}
     */
    DomAdapter.prototype.createStyleElement = function (css, doc) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.createShadowRoot = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getShadowRoot = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getHost = function (el) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getDistributedNodes = function (el) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.clone = function (node) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    DomAdapter.prototype.getElementsByClassName = function (element, name) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    DomAdapter.prototype.getElementsByTagName = function (element, name) { };
    /**
     * @abstract
     * @param {?} element
     * @return {?}
     */
    DomAdapter.prototype.classList = function (element) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} className
     * @return {?}
     */
    DomAdapter.prototype.addClass = function (element, className) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} className
     * @return {?}
     */
    DomAdapter.prototype.removeClass = function (element, className) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} className
     * @return {?}
     */
    DomAdapter.prototype.hasClass = function (element, className) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} styleName
     * @param {?} styleValue
     * @return {?}
     */
    DomAdapter.prototype.setStyle = function (element, styleName, styleValue) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} styleName
     * @return {?}
     */
    DomAdapter.prototype.removeStyle = function (element, styleName) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} styleName
     * @return {?}
     */
    DomAdapter.prototype.getStyle = function (element, styleName) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} styleName
     * @param {?=} styleValue
     * @return {?}
     */
    DomAdapter.prototype.hasStyle = function (element, styleName, styleValue) { };
    /**
     * @abstract
     * @param {?} element
     * @return {?}
     */
    DomAdapter.prototype.tagName = function (element) { };
    /**
     * @abstract
     * @param {?} element
     * @return {?}
     */
    DomAdapter.prototype.attributeMap = function (element) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} attribute
     * @return {?}
     */
    DomAdapter.prototype.hasAttribute = function (element, attribute) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} ns
     * @param {?} attribute
     * @return {?}
     */
    DomAdapter.prototype.hasAttributeNS = function (element, ns, attribute) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} attribute
     * @return {?}
     */
    DomAdapter.prototype.getAttribute = function (element, attribute) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} ns
     * @param {?} attribute
     * @return {?}
     */
    DomAdapter.prototype.getAttributeNS = function (element, ns, attribute) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setAttribute = function (element, name, value) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} ns
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setAttributeNS = function (element, ns, name, value) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} attribute
     * @return {?}
     */
    DomAdapter.prototype.removeAttribute = function (element, attribute) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} ns
     * @param {?} attribute
     * @return {?}
     */
    DomAdapter.prototype.removeAttributeNS = function (element, ns, attribute) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.templateAwareRoot = function (el) { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.createHtmlDocument = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.getDefaultDocument = function () { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.getBoundingClientRect = function (el) { };
    /**
     * @abstract
     * @param {?} doc
     * @return {?}
     */
    DomAdapter.prototype.getTitle = function (doc) { };
    /**
     * @abstract
     * @param {?} doc
     * @param {?} newTitle
     * @return {?}
     */
    DomAdapter.prototype.setTitle = function (doc, newTitle) { };
    /**
     * @abstract
     * @param {?} n
     * @param {?} selector
     * @return {?}
     */
    DomAdapter.prototype.elementMatches = function (n, selector) { };
    /**
     * @abstract
     * @param {?} el
     * @return {?}
     */
    DomAdapter.prototype.isTemplateElement = function (el) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.isTextNode = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.isCommentNode = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.isElementNode = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.hasShadowRoot = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.isShadowRoot = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.importIntoDoc = function (node) { };
    /**
     * @abstract
     * @param {?} node
     * @return {?}
     */
    DomAdapter.prototype.adoptNode = function (node) { };
    /**
     * @abstract
     * @param {?} element
     * @return {?}
     */
    DomAdapter.prototype.getHref = function (element) { };
    /**
     * @abstract
     * @param {?} event
     * @return {?}
     */
    DomAdapter.prototype.getEventKey = function (event) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} baseUrl
     * @param {?} href
     * @return {?}
     */
    DomAdapter.prototype.resolveAndSetHref = function (element, baseUrl, href) { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.supportsDOMEvents = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.supportsNativeShadowDOM = function () { };
    /**
     * @abstract
     * @param {?} doc
     * @param {?} target
     * @return {?}
     */
    DomAdapter.prototype.getGlobalEventTarget = function (doc, target) { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.getHistory = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.getLocation = function () { };
    /**
     * @abstract
     * @param {?} doc
     * @return {?}
     */
    DomAdapter.prototype.getBaseHref = function (doc) { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.resetBaseElement = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.getUserAgent = function () { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setData = function (element, name, value) { };
    /**
     * @abstract
     * @param {?} element
     * @return {?}
     */
    DomAdapter.prototype.getComputedStyle = function (element) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    DomAdapter.prototype.getData = function (element, name) { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.supportsWebAnimation = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.performanceNow = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.getAnimationPrefix = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.getTransitionEnd = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.supportsAnimation = function () { };
    /**
     * @abstract
     * @return {?}
     */
    DomAdapter.prototype.supportsCookies = function () { };
    /**
     * @abstract
     * @param {?} name
     * @return {?}
     */
    DomAdapter.prototype.getCookie = function (name) { };
    /**
     * @abstract
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    DomAdapter.prototype.setCookie = function (name, value) { };
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Provides DOM operations in any browser environment.
 *
 * \@security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 * @abstract
 */
class GenericBrowserDomAdapter extends DomAdapter {
    constructor() {
        super();
        this._animationPrefix = null;
        this._transitionEnd = null;
        try {
            /** @type {?} */
            const element = this.createElement('div', document);
            if (this.getStyle(element, 'animationName') != null) {
                this._animationPrefix = '';
            }
            else {
                /** @type {?} */
                const domPrefixes = ['Webkit', 'Moz', 'O', 'ms'];
                for (let i = 0; i < domPrefixes.length; i++) {
                    if (this.getStyle(element, domPrefixes[i] + 'AnimationName') != null) {
                        this._animationPrefix = '-' + domPrefixes[i].toLowerCase() + '-';
                        break;
                    }
                }
            }
            /** @type {?} */
            const transEndEventNames = {
                WebkitTransition: 'webkitTransitionEnd',
                MozTransition: 'transitionend',
                OTransition: 'oTransitionEnd otransitionend',
                transition: 'transitionend'
            };
            Object.keys(transEndEventNames).forEach((/**
             * @param {?} key
             * @return {?}
             */
            (key) => {
                if (this.getStyle(element, key) != null) {
                    this._transitionEnd = transEndEventNames[key];
                }
            }));
        }
        catch (_a) {
            this._animationPrefix = null;
            this._transitionEnd = null;
        }
    }
    /**
     * @param {?} el
     * @return {?}
     */
    getDistributedNodes(el) { return ((/** @type {?} */ (el))).getDistributedNodes(); }
    /**
     * @param {?} el
     * @param {?} baseUrl
     * @param {?} href
     * @return {?}
     */
    resolveAndSetHref(el, baseUrl, href) {
        el.href = href == null ? baseUrl : baseUrl + '/../' + href;
    }
    /**
     * @return {?}
     */
    supportsDOMEvents() { return true; }
    /**
     * @return {?}
     */
    supportsNativeShadowDOM() {
        return typeof ((/** @type {?} */ (document.body))).createShadowRoot === 'function';
    }
    /**
     * @return {?}
     */
    getAnimationPrefix() { return this._animationPrefix ? this._animationPrefix : ''; }
    /**
     * @return {?}
     */
    getTransitionEnd() { return this._transitionEnd ? this._transitionEnd : ''; }
    /**
     * @return {?}
     */
    supportsAnimation() {
        return this._animationPrefix != null && this._transitionEnd != null;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    GenericBrowserDomAdapter.prototype._animationPrefix;
    /**
     * @type {?}
     * @private
     */
    GenericBrowserDomAdapter.prototype._transitionEnd;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const _attrToPropMap = {
    'class': 'className',
    'innerHtml': 'innerHTML',
    'readonly': 'readOnly',
    'tabindex': 'tabIndex',
};
/** @type {?} */
const DOM_KEY_LOCATION_NUMPAD = 3;
// Map to convert some key or keyIdentifier values to what will be returned by getEventKey
/** @type {?} */
const _keyMap = {
    // The following values are here for cross-browser compatibility and to match the W3C standard
    // cf http://www.w3.org/TR/DOM-Level-3-Events-key/
    '\b': 'Backspace',
    '\t': 'Tab',
    '\x7F': 'Delete',
    '\x1B': 'Escape',
    'Del': 'Delete',
    'Esc': 'Escape',
    'Left': 'ArrowLeft',
    'Right': 'ArrowRight',
    'Up': 'ArrowUp',
    'Down': 'ArrowDown',
    'Menu': 'ContextMenu',
    'Scroll': 'ScrollLock',
    'Win': 'OS'
};
// There is a bug in Chrome for numeric keypad keys:
// https://code.google.com/p/chromium/issues/detail?id=155654
// 1, 2, 3 ... are reported as A, B, C ...
/** @type {?} */
const _chromeNumKeyPadMap = {
    'A': '1',
    'B': '2',
    'C': '3',
    'D': '4',
    'E': '5',
    'F': '6',
    'G': '7',
    'H': '8',
    'I': '9',
    'J': '*',
    'K': '+',
    'M': '-',
    'N': '.',
    'O': '/',
    '\x60': '0',
    '\x90': 'NumLock'
};
const ɵ0 = /**
 * @return {?}
 */
() => {
    if (ɵglobal['Node']) {
        return ɵglobal['Node'].prototype.contains || (/**
         * @this {?}
         * @param {?} node
         * @return {?}
         */
        function (node) {
            return !!(this.compareDocumentPosition(node) & 16);
        });
    }
    return (/** @type {?} */ (undefined));
};
/** @type {?} */
const nodeContains = ((ɵ0))();
/**
 * A `DomAdapter` powered by full browser DOM APIs.
 *
 * \@security Tread carefully! Interacting with the DOM directly is dangerous and
 * can introduce XSS risks.
 */
/* tslint:disable:requireParameterType no-console */
class BrowserDomAdapter extends GenericBrowserDomAdapter {
    /**
     * @param {?} templateHtml
     * @return {?}
     */
    parse(templateHtml) { throw new Error('parse not implemented'); }
    /**
     * @return {?}
     */
    static makeCurrent() { setRootDomAdapter(new BrowserDomAdapter()); }
    /**
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    hasProperty(element, name) { return name in element; }
    /**
     * @param {?} el
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    setProperty(el, name, value) { ((/** @type {?} */ (el)))[name] = value; }
    /**
     * @param {?} el
     * @param {?} name
     * @return {?}
     */
    getProperty(el, name) { return ((/** @type {?} */ (el)))[name]; }
    /**
     * @param {?} el
     * @param {?} methodName
     * @param {?} args
     * @return {?}
     */
    invoke(el, methodName, args) { ((/** @type {?} */ (el)))[methodName](...args); }
    // TODO(tbosch): move this into a separate environment class once we have it
    /**
     * @param {?} error
     * @return {?}
     */
    logError(error) {
        if (window.console) {
            if (console.error) {
                console.error(error);
            }
            else {
                console.log(error);
            }
        }
    }
    /**
     * @param {?} error
     * @return {?}
     */
    log(error) {
        if (window.console) {
            window.console.log && window.console.log(error);
        }
    }
    /**
     * @param {?} error
     * @return {?}
     */
    logGroup(error) {
        if (window.console) {
            window.console.group && window.console.group(error);
        }
    }
    /**
     * @return {?}
     */
    logGroupEnd() {
        if (window.console) {
            window.console.groupEnd && window.console.groupEnd();
        }
    }
    /**
     * @return {?}
     */
    get attrToPropMap() { return _attrToPropMap; }
    /**
     * @param {?} nodeA
     * @param {?} nodeB
     * @return {?}
     */
    contains(nodeA, nodeB) { return nodeContains.call(nodeA, nodeB); }
    /**
     * @param {?} el
     * @param {?} selector
     * @return {?}
     */
    querySelector(el, selector) { return el.querySelector(selector); }
    /**
     * @param {?} el
     * @param {?} selector
     * @return {?}
     */
    querySelectorAll(el, selector) { return el.querySelectorAll(selector); }
    /**
     * @param {?} el
     * @param {?} evt
     * @param {?} listener
     * @return {?}
     */
    on(el, evt, listener) { el.addEventListener(evt, listener, false); }
    /**
     * @param {?} el
     * @param {?} evt
     * @param {?} listener
     * @return {?}
     */
    onAndCancel(el, evt, listener) {
        el.addEventListener(evt, listener, false);
        // Needed to follow Dart's subscription semantic, until fix of
        // https://code.google.com/p/dart/issues/detail?id=17406
        return (/**
         * @return {?}
         */
        () => { el.removeEventListener(evt, listener, false); });
    }
    /**
     * @param {?} el
     * @param {?} evt
     * @return {?}
     */
    dispatchEvent(el, evt) { el.dispatchEvent(evt); }
    /**
     * @param {?} eventType
     * @return {?}
     */
    createMouseEvent(eventType) {
        /** @type {?} */
        const evt = this.getDefaultDocument().createEvent('MouseEvent');
        evt.initEvent(eventType, true, true);
        return evt;
    }
    /**
     * @param {?} eventType
     * @return {?}
     */
    createEvent(eventType) {
        /** @type {?} */
        const evt = this.getDefaultDocument().createEvent('Event');
        evt.initEvent(eventType, true, true);
        return evt;
    }
    /**
     * @param {?} evt
     * @return {?}
     */
    preventDefault(evt) {
        evt.preventDefault();
        evt.returnValue = false;
    }
    /**
     * @param {?} evt
     * @return {?}
     */
    isPrevented(evt) {
        return evt.defaultPrevented || evt.returnValue != null && !evt.returnValue;
    }
    /**
     * @param {?} el
     * @return {?}
     */
    getInnerHTML(el) { return el.innerHTML; }
    /**
     * @param {?} el
     * @return {?}
     */
    getTemplateContent(el) {
        return 'content' in el && this.isTemplateElement(el) ? ((/** @type {?} */ (el))).content : null;
    }
    /**
     * @param {?} el
     * @return {?}
     */
    getOuterHTML(el) { return el.outerHTML; }
    /**
     * @param {?} node
     * @return {?}
     */
    nodeName(node) { return node.nodeName; }
    /**
     * @param {?} node
     * @return {?}
     */
    nodeValue(node) { return node.nodeValue; }
    /**
     * @param {?} node
     * @return {?}
     */
    type(node) { return node.type; }
    /**
     * @param {?} node
     * @return {?}
     */
    content(node) {
        if (this.hasProperty(node, 'content')) {
            return ((/** @type {?} */ (node))).content;
        }
        else {
            return node;
        }
    }
    /**
     * @param {?} el
     * @return {?}
     */
    firstChild(el) { return el.firstChild; }
    /**
     * @param {?} el
     * @return {?}
     */
    nextSibling(el) { return el.nextSibling; }
    /**
     * @param {?} el
     * @return {?}
     */
    parentElement(el) { return el.parentNode; }
    /**
     * @param {?} el
     * @return {?}
     */
    childNodes(el) { return el.childNodes; }
    /**
     * @param {?} el
     * @return {?}
     */
    childNodesAsList(el) {
        /** @type {?} */
        const childNodes = el.childNodes;
        /** @type {?} */
        const res = new Array(childNodes.length);
        for (let i = 0; i < childNodes.length; i++) {
            res[i] = childNodes[i];
        }
        return res;
    }
    /**
     * @param {?} el
     * @return {?}
     */
    clearNodes(el) {
        while (el.firstChild) {
            el.removeChild(el.firstChild);
        }
    }
    /**
     * @param {?} el
     * @param {?} node
     * @return {?}
     */
    appendChild(el, node) { el.appendChild(node); }
    /**
     * @param {?} el
     * @param {?} node
     * @return {?}
     */
    removeChild(el, node) { el.removeChild(node); }
    /**
     * @param {?} el
     * @param {?} newChild
     * @param {?} oldChild
     * @return {?}
     */
    replaceChild(el, newChild, oldChild) { el.replaceChild(newChild, oldChild); }
    /**
     * @param {?} node
     * @return {?}
     */
    remove(node) {
        if (node.parentNode) {
            node.parentNode.removeChild(node);
        }
        return node;
    }
    /**
     * @param {?} parent
     * @param {?} ref
     * @param {?} node
     * @return {?}
     */
    insertBefore(parent, ref, node) { parent.insertBefore(node, ref); }
    /**
     * @param {?} parent
     * @param {?} ref
     * @param {?} nodes
     * @return {?}
     */
    insertAllBefore(parent, ref, nodes) {
        nodes.forEach((/**
         * @param {?} n
         * @return {?}
         */
        (n) => parent.insertBefore(n, ref)));
    }
    /**
     * @param {?} parent
     * @param {?} ref
     * @param {?} node
     * @return {?}
     */
    insertAfter(parent, ref, node) { parent.insertBefore(node, ref.nextSibling); }
    /**
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    setInnerHTML(el, value) { el.innerHTML = value; }
    /**
     * @param {?} el
     * @return {?}
     */
    getText(el) { return el.textContent; }
    /**
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    setText(el, value) { el.textContent = value; }
    /**
     * @param {?} el
     * @return {?}
     */
    getValue(el) { return el.value; }
    /**
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    setValue(el, value) { el.value = value; }
    /**
     * @param {?} el
     * @return {?}
     */
    getChecked(el) { return el.checked; }
    /**
     * @param {?} el
     * @param {?} value
     * @return {?}
     */
    setChecked(el, value) { el.checked = value; }
    /**
     * @param {?} text
     * @return {?}
     */
    createComment(text) { return this.getDefaultDocument().createComment(text); }
    /**
     * @param {?} html
     * @return {?}
     */
    createTemplate(html) {
        /** @type {?} */
        const t = this.getDefaultDocument().createElement('template');
        t.innerHTML = html;
        return t;
    }
    /**
     * @param {?} tagName
     * @param {?=} doc
     * @return {?}
     */
    createElement(tagName, doc) {
        doc = doc || this.getDefaultDocument();
        return doc.createElement(tagName);
    }
    /**
     * @param {?} ns
     * @param {?} tagName
     * @param {?=} doc
     * @return {?}
     */
    createElementNS(ns, tagName, doc) {
        doc = doc || this.getDefaultDocument();
        return doc.createElementNS(ns, tagName);
    }
    /**
     * @param {?} text
     * @param {?=} doc
     * @return {?}
     */
    createTextNode(text, doc) {
        doc = doc || this.getDefaultDocument();
        return doc.createTextNode(text);
    }
    /**
     * @param {?} attrName
     * @param {?} attrValue
     * @param {?=} doc
     * @return {?}
     */
    createScriptTag(attrName, attrValue, doc) {
        doc = doc || this.getDefaultDocument();
        /** @type {?} */
        const el = (/** @type {?} */ (doc.createElement('SCRIPT')));
        el.setAttribute(attrName, attrValue);
        return el;
    }
    /**
     * @param {?} css
     * @param {?=} doc
     * @return {?}
     */
    createStyleElement(css, doc) {
        doc = doc || this.getDefaultDocument();
        /** @type {?} */
        const style = (/** @type {?} */ (doc.createElement('style')));
        this.appendChild(style, this.createTextNode(css, doc));
        return style;
    }
    /**
     * @param {?} el
     * @return {?}
     */
    createShadowRoot(el) { return ((/** @type {?} */ (el))).createShadowRoot(); }
    /**
     * @param {?} el
     * @return {?}
     */
    getShadowRoot(el) { return ((/** @type {?} */ (el))).shadowRoot; }
    /**
     * @param {?} el
     * @return {?}
     */
    getHost(el) { return ((/** @type {?} */ (el))).host; }
    /**
     * @param {?} node
     * @return {?}
     */
    clone(node) { return node.cloneNode(true); }
    /**
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    getElementsByClassName(element, name) {
        return element.getElementsByClassName(name);
    }
    /**
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    getElementsByTagName(element, name) {
        return element.getElementsByTagName(name);
    }
    /**
     * @param {?} element
     * @return {?}
     */
    classList(element) { return Array.prototype.slice.call(element.classList, 0); }
    /**
     * @param {?} element
     * @param {?} className
     * @return {?}
     */
    addClass(element, className) { element.classList.add(className); }
    /**
     * @param {?} element
     * @param {?} className
     * @return {?}
     */
    removeClass(element, className) { element.classList.remove(className); }
    /**
     * @param {?} element
     * @param {?} className
     * @return {?}
     */
    hasClass(element, className) {
        return element.classList.contains(className);
    }
    /**
     * @param {?} element
     * @param {?} styleName
     * @param {?} styleValue
     * @return {?}
     */
    setStyle(element, styleName, styleValue) {
        element.style[styleName] = styleValue;
    }
    /**
     * @param {?} element
     * @param {?} stylename
     * @return {?}
     */
    removeStyle(element, stylename) {
        // IE requires '' instead of null
        // see https://github.com/angular/angular/issues/7916
        element.style[stylename] = '';
    }
    /**
     * @param {?} element
     * @param {?} stylename
     * @return {?}
     */
    getStyle(element, stylename) { return element.style[stylename]; }
    /**
     * @param {?} element
     * @param {?} styleName
     * @param {?=} styleValue
     * @return {?}
     */
    hasStyle(element, styleName, styleValue) {
        /** @type {?} */
        const value = this.getStyle(element, styleName) || '';
        return styleValue ? value == styleValue : value.length > 0;
    }
    /**
     * @param {?} element
     * @return {?}
     */
    tagName(element) { return element.tagName; }
    /**
     * @param {?} element
     * @return {?}
     */
    attributeMap(element) {
        /** @type {?} */
        const res = new Map();
        /** @type {?} */
        const elAttrs = element.attributes;
        for (let i = 0; i < elAttrs.length; i++) {
            /** @type {?} */
            const attrib = elAttrs.item(i);
            res.set(attrib.name, attrib.value);
        }
        return res;
    }
    /**
     * @param {?} element
     * @param {?} attribute
     * @return {?}
     */
    hasAttribute(element, attribute) {
        return element.hasAttribute(attribute);
    }
    /**
     * @param {?} element
     * @param {?} ns
     * @param {?} attribute
     * @return {?}
     */
    hasAttributeNS(element, ns, attribute) {
        return element.hasAttributeNS(ns, attribute);
    }
    /**
     * @param {?} element
     * @param {?} attribute
     * @return {?}
     */
    getAttribute(element, attribute) {
        return element.getAttribute(attribute);
    }
    /**
     * @param {?} element
     * @param {?} ns
     * @param {?} name
     * @return {?}
     */
    getAttributeNS(element, ns, name) {
        return element.getAttributeNS(ns, name);
    }
    /**
     * @param {?} element
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    setAttribute(element, name, value) { element.setAttribute(name, value); }
    /**
     * @param {?} element
     * @param {?} ns
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    setAttributeNS(element, ns, name, value) {
        element.setAttributeNS(ns, name, value);
    }
    /**
     * @param {?} element
     * @param {?} attribute
     * @return {?}
     */
    removeAttribute(element, attribute) { element.removeAttribute(attribute); }
    /**
     * @param {?} element
     * @param {?} ns
     * @param {?} name
     * @return {?}
     */
    removeAttributeNS(element, ns, name) {
        element.removeAttributeNS(ns, name);
    }
    /**
     * @param {?} el
     * @return {?}
     */
    templateAwareRoot(el) { return this.isTemplateElement(el) ? this.content(el) : el; }
    /**
     * @return {?}
     */
    createHtmlDocument() {
        return document.implementation.createHTMLDocument('fakeTitle');
    }
    /**
     * @return {?}
     */
    getDefaultDocument() { return document; }
    /**
     * @param {?} el
     * @return {?}
     */
    getBoundingClientRect(el) {
        try {
            return el.getBoundingClientRect();
        }
        catch (_a) {
            return { top: 0, bottom: 0, left: 0, right: 0, width: 0, height: 0 };
        }
    }
    /**
     * @param {?} doc
     * @return {?}
     */
    getTitle(doc) { return doc.title; }
    /**
     * @param {?} doc
     * @param {?} newTitle
     * @return {?}
     */
    setTitle(doc, newTitle) { doc.title = newTitle || ''; }
    /**
     * @param {?} n
     * @param {?} selector
     * @return {?}
     */
    elementMatches(n, selector) {
        if (this.isElementNode(n)) {
            return n.matches && n.matches(selector) ||
                n.msMatchesSelector && n.msMatchesSelector(selector) ||
                n.webkitMatchesSelector && n.webkitMatchesSelector(selector);
        }
        return false;
    }
    /**
     * @param {?} el
     * @return {?}
     */
    isTemplateElement(el) {
        return this.isElementNode(el) && el.nodeName === 'TEMPLATE';
    }
    /**
     * @param {?} node
     * @return {?}
     */
    isTextNode(node) { return node.nodeType === Node.TEXT_NODE; }
    /**
     * @param {?} node
     * @return {?}
     */
    isCommentNode(node) { return node.nodeType === Node.COMMENT_NODE; }
    /**
     * @param {?} node
     * @return {?}
     */
    isElementNode(node) { return node.nodeType === Node.ELEMENT_NODE; }
    /**
     * @param {?} node
     * @return {?}
     */
    hasShadowRoot(node) {
        return node.shadowRoot != null && node instanceof HTMLElement;
    }
    /**
     * @param {?} node
     * @return {?}
     */
    isShadowRoot(node) { return node instanceof DocumentFragment; }
    /**
     * @param {?} node
     * @return {?}
     */
    importIntoDoc(node) { return document.importNode(this.templateAwareRoot(node), true); }
    /**
     * @param {?} node
     * @return {?}
     */
    adoptNode(node) { return document.adoptNode(node); }
    /**
     * @param {?} el
     * @return {?}
     */
    getHref(el) { return (/** @type {?} */ (el.getAttribute('href'))); }
    /**
     * @param {?} event
     * @return {?}
     */
    getEventKey(event) {
        /** @type {?} */
        let key = event.key;
        if (key == null) {
            key = event.keyIdentifier;
            // keyIdentifier is defined in the old draft of DOM Level 3 Events implemented by Chrome and
            // Safari cf
            // http://www.w3.org/TR/2007/WD-DOM-Level-3-Events-20071221/events.html#Events-KeyboardEvents-Interfaces
            if (key == null) {
                return 'Unidentified';
            }
            if (key.startsWith('U+')) {
                key = String.fromCharCode(parseInt(key.substring(2), 16));
                if (event.location === DOM_KEY_LOCATION_NUMPAD && _chromeNumKeyPadMap.hasOwnProperty(key)) {
                    // There is a bug in Chrome for numeric keypad keys:
                    // https://code.google.com/p/chromium/issues/detail?id=155654
                    // 1, 2, 3 ... are reported as A, B, C ...
                    key = ((/** @type {?} */ (_chromeNumKeyPadMap)))[key];
                }
            }
        }
        return _keyMap[key] || key;
    }
    /**
     * @param {?} doc
     * @param {?} target
     * @return {?}
     */
    getGlobalEventTarget(doc, target) {
        if (target === 'window') {
            return window;
        }
        if (target === 'document') {
            return doc;
        }
        if (target === 'body') {
            return doc.body;
        }
        return null;
    }
    /**
     * @return {?}
     */
    getHistory() { return window.history; }
    /**
     * @return {?}
     */
    getLocation() { return window.location; }
    /**
     * @param {?} doc
     * @return {?}
     */
    getBaseHref(doc) {
        /** @type {?} */
        const href = getBaseElementHref();
        return href == null ? null : relativePath(href);
    }
    /**
     * @return {?}
     */
    resetBaseElement() { baseElement = null; }
    /**
     * @return {?}
     */
    getUserAgent() { return window.navigator.userAgent; }
    /**
     * @param {?} element
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    setData(element, name, value) {
        this.setAttribute(element, 'data-' + name, value);
    }
    /**
     * @param {?} element
     * @param {?} name
     * @return {?}
     */
    getData(element, name) {
        return this.getAttribute(element, 'data-' + name);
    }
    /**
     * @param {?} element
     * @return {?}
     */
    getComputedStyle(element) { return getComputedStyle(element); }
    // TODO(tbosch): move this into a separate environment class once we have it
    /**
     * @return {?}
     */
    supportsWebAnimation() {
        return typeof ((/** @type {?} */ (Element))).prototype['animate'] === 'function';
    }
    /**
     * @return {?}
     */
    performanceNow() {
        // performance.now() is not available in all browsers, see
        // http://caniuse.com/#search=performance.now
        return window.performance && window.performance.now ? window.performance.now() :
            new Date().getTime();
    }
    /**
     * @return {?}
     */
    supportsCookies() { return true; }
    /**
     * @param {?} name
     * @return {?}
     */
    getCookie(name) { return ɵparseCookieValue(document.cookie, name); }
    /**
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    setCookie(name, value) {
        // document.cookie is magical, assigning into it assigns/overrides one cookie value, but does
        // not clear other cookies.
        document.cookie = encodeURIComponent(name) + '=' + encodeURIComponent(value);
    }
}
/** @type {?} */
let baseElement = null;
/**
 * @return {?}
 */
function getBaseElementHref() {
    if (!baseElement) {
        baseElement = (/** @type {?} */ (document.querySelector('base')));
        if (!baseElement) {
            return null;
        }
    }
    return baseElement.getAttribute('href');
}
// based on urlUtils.js in AngularJS 1
/** @type {?} */
let urlParsingNode;
/**
 * @param {?} url
 * @return {?}
 */
function relativePath(url) {
    if (!urlParsingNode) {
        urlParsingNode = document.createElement('a');
    }
    urlParsingNode.setAttribute('href', url);
    return (urlParsingNode.pathname.charAt(0) === '/') ? urlParsingNode.pathname :
        '/' + urlParsingNode.pathname;
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
 * @return {?}
 */
function supportsState() {
    return !!window.history.pushState;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * `PlatformLocation` encapsulates all of the direct calls to platform APIs.
 * This class should not be used directly by an application developer. Instead, use
 * {\@link Location}.
 */
class BrowserPlatformLocation extends PlatformLocation {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        super();
        this._doc = _doc;
        this._init();
    }
    // This is moved to its own method so that `MockPlatformLocationStrategy` can overwrite it
    /**
     * \@internal
     * @return {?}
     */
    _init() {
        ((/** @type {?} */ (this))).location = getDOM().getLocation();
        this._history = getDOM().getHistory();
    }
    /**
     * @return {?}
     */
    getBaseHrefFromDOM() { return (/** @type {?} */ (getDOM().getBaseHref(this._doc))); }
    /**
     * @param {?} fn
     * @return {?}
     */
    onPopState(fn) {
        getDOM().getGlobalEventTarget(this._doc, 'window').addEventListener('popstate', fn, false);
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    onHashChange(fn) {
        getDOM().getGlobalEventTarget(this._doc, 'window').addEventListener('hashchange', fn, false);
    }
    /**
     * @return {?}
     */
    get href() { return this.location.href; }
    /**
     * @return {?}
     */
    get protocol() { return this.location.protocol; }
    /**
     * @return {?}
     */
    get hostname() { return this.location.hostname; }
    /**
     * @return {?}
     */
    get port() { return this.location.port; }
    /**
     * @return {?}
     */
    get pathname() { return this.location.pathname; }
    /**
     * @return {?}
     */
    get search() { return this.location.search; }
    /**
     * @return {?}
     */
    get hash() { return this.location.hash; }
    /**
     * @param {?} newPath
     * @return {?}
     */
    set pathname(newPath) { this.location.pathname = newPath; }
    /**
     * @param {?} state
     * @param {?} title
     * @param {?} url
     * @return {?}
     */
    pushState(state, title, url) {
        if (supportsState()) {
            this._history.pushState(state, title, url);
        }
        else {
            this.location.hash = url;
        }
    }
    /**
     * @param {?} state
     * @param {?} title
     * @param {?} url
     * @return {?}
     */
    replaceState(state, title, url) {
        if (supportsState()) {
            this._history.replaceState(state, title, url);
        }
        else {
            this.location.hash = url;
        }
    }
    /**
     * @return {?}
     */
    forward() { this._history.forward(); }
    /**
     * @return {?}
     */
    back() { this._history.back(); }
    /**
     * @return {?}
     */
    getState() { return this._history.state; }
}
BrowserPlatformLocation.decorators = [
    { type: Injectable }
];
/** @nocollapse */
BrowserPlatformLocation.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
if (false) {
    /** @type {?} */
    BrowserPlatformLocation.prototype.location;
    /**
     * @type {?}
     * @private
     */
    BrowserPlatformLocation.prototype._history;
    /**
     * @type {?}
     * @private
     */
    BrowserPlatformLocation.prototype._doc;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * An id that identifies a particular application being bootstrapped, that should
 * match across the client/server boundary.
 * @type {?}
 */
const TRANSITION_ID = new InjectionToken('TRANSITION_ID');
/**
 * @param {?} transitionId
 * @param {?} document
 * @param {?} injector
 * @return {?}
 */
function appInitializerFactory(transitionId, document, injector) {
    return (/**
     * @return {?}
     */
    () => {
        // Wait for all application initializers to be completed before removing the styles set by
        // the server.
        injector.get(ApplicationInitStatus).donePromise.then((/**
         * @return {?}
         */
        () => {
            /** @type {?} */
            const dom = getDOM();
            /** @type {?} */
            const styles = Array.prototype.slice.apply(dom.querySelectorAll(document, `style[ng-transition]`));
            styles.filter((/**
             * @param {?} el
             * @return {?}
             */
            el => dom.getAttribute(el, 'ng-transition') === transitionId))
                .forEach((/**
             * @param {?} el
             * @return {?}
             */
            el => dom.remove(el)));
        }));
    });
}
/** @type {?} */
const SERVER_TRANSITION_PROVIDERS = [
    {
        provide: APP_INITIALIZER,
        useFactory: appInitializerFactory,
        deps: [TRANSITION_ID, DOCUMENT, Injector],
        multi: true
    },
];

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class BrowserGetTestability {
    /**
     * @return {?}
     */
    static init() { setTestabilityGetter(new BrowserGetTestability()); }
    /**
     * @param {?} registry
     * @return {?}
     */
    addToWindow(registry) {
        ɵglobal['getAngularTestability'] = (/**
         * @param {?} elem
         * @param {?=} findInAncestors
         * @return {?}
         */
        (elem, findInAncestors = true) => {
            /** @type {?} */
            const testability = registry.findTestabilityInTree(elem, findInAncestors);
            if (testability == null) {
                throw new Error('Could not find testability for element.');
            }
            return testability;
        });
        ɵglobal['getAllAngularTestabilities'] = (/**
         * @return {?}
         */
        () => registry.getAllTestabilities());
        ɵglobal['getAllAngularRootElements'] = (/**
         * @return {?}
         */
        () => registry.getAllRootElements());
        /** @type {?} */
        const whenAllStable = (/**
         * @param {?} callback
         * @return {?}
         */
        (callback /** TODO #9100 */) => {
            /** @type {?} */
            const testabilities = ɵglobal['getAllAngularTestabilities']();
            /** @type {?} */
            let count = testabilities.length;
            /** @type {?} */
            let didWork = false;
            /** @type {?} */
            const decrement = (/**
             * @param {?} didWork_
             * @return {?}
             */
            function (didWork_ /** TODO #9100 */) {
                didWork = didWork || didWork_;
                count--;
                if (count == 0) {
                    callback(didWork);
                }
            });
            testabilities.forEach((/**
             * @param {?} testability
             * @return {?}
             */
            function (testability /** TODO #9100 */) {
                testability.whenStable(decrement);
            }));
        });
        if (!ɵglobal['frameworkStabilizers']) {
            ɵglobal['frameworkStabilizers'] = [];
        }
        ɵglobal['frameworkStabilizers'].push(whenAllStable);
    }
    /**
     * @param {?} registry
     * @param {?} elem
     * @param {?} findInAncestors
     * @return {?}
     */
    findTestabilityInTree(registry, elem, findInAncestors) {
        if (elem == null) {
            return null;
        }
        /** @type {?} */
        const t = registry.getTestability(elem);
        if (t != null) {
            return t;
        }
        else if (!findInAncestors) {
            return null;
        }
        if (getDOM().isShadowRoot(elem)) {
            return this.findTestabilityInTree(registry, getDOM().getHost(elem), true);
        }
        return this.findTestabilityInTree(registry, getDOM().parentElement(elem), true);
    }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const CAMEL_CASE_REGEXP = /([A-Z])/g;
/** @type {?} */
const DASH_CASE_REGEXP = /-([a-z])/g;
/**
 * @param {?} input
 * @return {?}
 */
function camelCaseToDashCase(input) {
    return input.replace(CAMEL_CASE_REGEXP, (/**
     * @param {...?} m
     * @return {?}
     */
    (...m) => '-' + m[1].toLowerCase()));
}
/**
 * @param {?} input
 * @return {?}
 */
function dashCaseToCamelCase(input) {
    return input.replace(DASH_CASE_REGEXP, (/**
     * @param {...?} m
     * @return {?}
     */
    (...m) => m[1].toUpperCase()));
}
/**
 * Exports the value under a given `name` in the global property `ng`. For example `ng.probe` if
 * `name` is `'probe'`.
 * @param {?} name Name under which it will be exported. Keep in mind this will be a property of the
 * global `ng` object.
 * @param {?} value The value to export.
 * @return {?}
 */
function exportNgVar(name, value) {
    if (typeof COMPILED === 'undefined' || !COMPILED) {
        // Note: we can't export `ng` when using closure enhanced optimization as:
        // - closure declares globals itself for minified names, which sometimes clobber our `ng` global
        // - we can't declare a closure extern as the namespace `ng` is already used within Google
        //   for typings for angularJS (via `goog.provide('ng....')`).
        /** @type {?} */
        const ng = ɵglobal['ng'] = ((/** @type {?} */ (ɵglobal['ng']))) || {};
        ng[name] = value;
    }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
const ɵ0$1 = /**
 * @return {?}
 */
() => ({
    'ApplicationRef': ApplicationRef,
    'NgZone': NgZone,
});
/** @type {?} */
const CORE_TOKENS = ((ɵ0$1))();
/** @type {?} */
const INSPECT_GLOBAL_NAME = 'probe';
/** @type {?} */
const CORE_TOKENS_GLOBAL_NAME = 'coreTokens';
/**
 * Returns a {\@link DebugElement} for the given native DOM element, or
 * null if the given native element does not have an Angular view associated
 * with it.
 * @param {?} element
 * @return {?}
 */
function inspectNativeElement(element) {
    return getDebugNode(element);
}
/**
 * @param {?} coreTokens
 * @return {?}
 */
function _createNgProbe(coreTokens) {
    exportNgVar(INSPECT_GLOBAL_NAME, inspectNativeElement);
    exportNgVar(CORE_TOKENS_GLOBAL_NAME, Object.assign({}, CORE_TOKENS, _ngProbeTokensToMap(coreTokens || [])));
    return (/**
     * @return {?}
     */
    () => inspectNativeElement);
}
/**
 * @param {?} tokens
 * @return {?}
 */
function _ngProbeTokensToMap(tokens) {
    return tokens.reduce((/**
     * @param {?} prev
     * @param {?} t
     * @return {?}
     */
    (prev, t) => (prev[t.name] = t.token, prev)), {});
}
/**
 * In Ivy, we don't support NgProbe because we have our own set of testing utilities
 * with more robust functionality.
 *
 * We shouldn't bring in NgProbe because it prevents DebugNode and friends from
 * tree-shaking properly.
 * @type {?}
 */
const ELEMENT_PROBE_PROVIDERS__POST_R3__ = [];
/**
 * Providers which support debugging Angular applications (e.g. via `ng.probe`).
 * @type {?}
 */
const ELEMENT_PROBE_PROVIDERS__PRE_R3__ = [
    {
        provide: APP_INITIALIZER,
        useFactory: _createNgProbe,
        deps: [
            [NgProbeToken, new Optional()],
        ],
        multi: true,
    },
];
/** @type {?} */
const ELEMENT_PROBE_PROVIDERS = ELEMENT_PROBE_PROVIDERS__PRE_R3__;

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * The injection token for the event-manager plug-in service.
 *
 * \@publicApi
 * @type {?}
 */
const EVENT_MANAGER_PLUGINS = new InjectionToken('EventManagerPlugins');
/**
 * An injectable service that provides event management for Angular
 * through a browser plug-in.
 *
 * \@publicApi
 */
class EventManager {
    /**
     * Initializes an instance of the event-manager service.
     * @param {?} plugins
     * @param {?} _zone
     */
    constructor(plugins, _zone) {
        this._zone = _zone;
        this._eventNameToPlugin = new Map();
        plugins.forEach((/**
         * @template THIS
         * @this {THIS}
         * @param {?} p
         * @return {THIS}
         */
        p => p.manager = this));
        this._plugins = plugins.slice().reverse();
    }
    /**
     * Registers a handler for a specific element and event.
     *
     * @param {?} element The HTML element to receive event notifications.
     * @param {?} eventName The name of the event to listen for.
     * @param {?} handler A function to call when the notification occurs. Receives the
     * event object as an argument.
     * @return {?} A callback function that can be used to remove the handler.
     */
    addEventListener(element, eventName, handler) {
        /** @type {?} */
        const plugin = this._findPluginFor(eventName);
        return plugin.addEventListener(element, eventName, handler);
    }
    /**
     * Registers a global handler for an event in a target view.
     *
     * @param {?} target A target for global event notifications. One of "window", "document", or "body".
     * @param {?} eventName The name of the event to listen for.
     * @param {?} handler A function to call when the notification occurs. Receives the
     * event object as an argument.
     * @return {?} A callback function that can be used to remove the handler.
     */
    addGlobalEventListener(target, eventName, handler) {
        /** @type {?} */
        const plugin = this._findPluginFor(eventName);
        return plugin.addGlobalEventListener(target, eventName, handler);
    }
    /**
     * Retrieves the compilation zone in which event listeners are registered.
     * @return {?}
     */
    getZone() { return this._zone; }
    /**
     * \@internal
     * @param {?} eventName
     * @return {?}
     */
    _findPluginFor(eventName) {
        /** @type {?} */
        const plugin = this._eventNameToPlugin.get(eventName);
        if (plugin) {
            return plugin;
        }
        /** @type {?} */
        const plugins = this._plugins;
        for (let i = 0; i < plugins.length; i++) {
            /** @type {?} */
            const plugin = plugins[i];
            if (plugin.supports(eventName)) {
                this._eventNameToPlugin.set(eventName, plugin);
                return plugin;
            }
        }
        throw new Error(`No event manager plugin found for event ${eventName}`);
    }
}
EventManager.decorators = [
    { type: Injectable }
];
/** @nocollapse */
EventManager.ctorParameters = () => [
    { type: Array, decorators: [{ type: Inject, args: [EVENT_MANAGER_PLUGINS,] }] },
    { type: NgZone }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    EventManager.prototype._plugins;
    /**
     * @type {?}
     * @private
     */
    EventManager.prototype._eventNameToPlugin;
    /**
     * @type {?}
     * @private
     */
    EventManager.prototype._zone;
}
/**
 * @abstract
 */
class EventManagerPlugin {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        this._doc = _doc;
    }
    /**
     * @param {?} element
     * @param {?} eventName
     * @param {?} handler
     * @return {?}
     */
    addGlobalEventListener(element, eventName, handler) {
        /** @type {?} */
        const target = getDOM().getGlobalEventTarget(this._doc, element);
        if (!target) {
            throw new Error(`Unsupported event target ${target} for event ${eventName}`);
        }
        return this.addEventListener(target, eventName, handler);
    }
}
if (false) {
    /** @type {?} */
    EventManagerPlugin.prototype.manager;
    /**
     * @type {?}
     * @private
     */
    EventManagerPlugin.prototype._doc;
    /**
     * @abstract
     * @param {?} eventName
     * @return {?}
     */
    EventManagerPlugin.prototype.supports = function (eventName) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} eventName
     * @param {?} handler
     * @return {?}
     */
    EventManagerPlugin.prototype.addEventListener = function (element, eventName, handler) { };
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class SharedStylesHost {
    constructor() {
        /**
         * \@internal
         */
        this._stylesSet = new Set();
    }
    /**
     * @param {?} styles
     * @return {?}
     */
    addStyles(styles) {
        /** @type {?} */
        const additions = new Set();
        styles.forEach((/**
         * @param {?} style
         * @return {?}
         */
        style => {
            if (!this._stylesSet.has(style)) {
                this._stylesSet.add(style);
                additions.add(style);
            }
        }));
        this.onStylesAdded(additions);
    }
    /**
     * @param {?} additions
     * @return {?}
     */
    onStylesAdded(additions) { }
    /**
     * @return {?}
     */
    getAllStyles() { return Array.from(this._stylesSet); }
}
SharedStylesHost.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * \@internal
     * @type {?}
     * @protected
     */
    SharedStylesHost.prototype._stylesSet;
}
class DomSharedStylesHost extends SharedStylesHost {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        super();
        this._doc = _doc;
        this._hostNodes = new Set();
        this._styleNodes = new Set();
        this._hostNodes.add(_doc.head);
    }
    /**
     * @private
     * @param {?} styles
     * @param {?} host
     * @return {?}
     */
    _addStylesToHost(styles, host) {
        styles.forEach((/**
         * @param {?} style
         * @return {?}
         */
        (style) => {
            /** @type {?} */
            const styleEl = this._doc.createElement('style');
            styleEl.textContent = style;
            this._styleNodes.add(host.appendChild(styleEl));
        }));
    }
    /**
     * @param {?} hostNode
     * @return {?}
     */
    addHost(hostNode) {
        this._addStylesToHost(this._stylesSet, hostNode);
        this._hostNodes.add(hostNode);
    }
    /**
     * @param {?} hostNode
     * @return {?}
     */
    removeHost(hostNode) { this._hostNodes.delete(hostNode); }
    /**
     * @param {?} additions
     * @return {?}
     */
    onStylesAdded(additions) {
        this._hostNodes.forEach((/**
         * @param {?} hostNode
         * @return {?}
         */
        hostNode => this._addStylesToHost(additions, hostNode)));
    }
    /**
     * @return {?}
     */
    ngOnDestroy() { this._styleNodes.forEach((/**
     * @param {?} styleNode
     * @return {?}
     */
    styleNode => getDOM().remove(styleNode))); }
}
DomSharedStylesHost.decorators = [
    { type: Injectable }
];
/** @nocollapse */
DomSharedStylesHost.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DomSharedStylesHost.prototype._hostNodes;
    /**
     * @type {?}
     * @private
     */
    DomSharedStylesHost.prototype._styleNodes;
    /**
     * @type {?}
     * @private
     */
    DomSharedStylesHost.prototype._doc;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const NAMESPACE_URIS = {
    'svg': 'http://www.w3.org/2000/svg',
    'xhtml': 'http://www.w3.org/1999/xhtml',
    'xlink': 'http://www.w3.org/1999/xlink',
    'xml': 'http://www.w3.org/XML/1998/namespace',
    'xmlns': 'http://www.w3.org/2000/xmlns/',
};
/** @type {?} */
const COMPONENT_REGEX = /%COMP%/g;
/** @type {?} */
const COMPONENT_VARIABLE = '%COMP%';
/** @type {?} */
const HOST_ATTR = `_nghost-${COMPONENT_VARIABLE}`;
/** @type {?} */
const CONTENT_ATTR = `_ngcontent-${COMPONENT_VARIABLE}`;
/**
 * @param {?} componentShortId
 * @return {?}
 */
function shimContentAttribute(componentShortId) {
    return CONTENT_ATTR.replace(COMPONENT_REGEX, componentShortId);
}
/**
 * @param {?} componentShortId
 * @return {?}
 */
function shimHostAttribute(componentShortId) {
    return HOST_ATTR.replace(COMPONENT_REGEX, componentShortId);
}
/**
 * @param {?} compId
 * @param {?} styles
 * @param {?} target
 * @return {?}
 */
function flattenStyles(compId, styles, target) {
    for (let i = 0; i < styles.length; i++) {
        /** @type {?} */
        let style = styles[i];
        if (Array.isArray(style)) {
            flattenStyles(compId, style, target);
        }
        else {
            style = style.replace(COMPONENT_REGEX, compId);
            target.push(style);
        }
    }
    return target;
}
/**
 * @param {?} eventHandler
 * @return {?}
 */
function decoratePreventDefault(eventHandler) {
    return (/**
     * @param {?} event
     * @return {?}
     */
    (event) => {
        /** @type {?} */
        const allowDefaultBehavior = eventHandler(event);
        if (allowDefaultBehavior === false) {
            // TODO(tbosch): move preventDefault into event plugins...
            event.preventDefault();
            event.returnValue = false;
        }
    });
}
class DomRendererFactory2 {
    /**
     * @param {?} eventManager
     * @param {?} sharedStylesHost
     * @param {?} appId
     */
    constructor(eventManager, sharedStylesHost, appId) {
        this.eventManager = eventManager;
        this.sharedStylesHost = sharedStylesHost;
        this.appId = appId;
        this.rendererByCompId = new Map();
        this.defaultRenderer = new DefaultDomRenderer2(eventManager);
    }
    /**
     * @param {?} element
     * @param {?} type
     * @return {?}
     */
    createRenderer(element, type) {
        if (!element || !type) {
            return this.defaultRenderer;
        }
        switch (type.encapsulation) {
            case ViewEncapsulation.Emulated: {
                /** @type {?} */
                let renderer = this.rendererByCompId.get(type.id);
                if (!renderer) {
                    renderer = new EmulatedEncapsulationDomRenderer2(this.eventManager, this.sharedStylesHost, type, this.appId);
                    this.rendererByCompId.set(type.id, renderer);
                }
                ((/** @type {?} */ (renderer))).applyToHost(element);
                return renderer;
            }
            case ViewEncapsulation.Native:
            case ViewEncapsulation.ShadowDom:
                return new ShadowDomRenderer(this.eventManager, this.sharedStylesHost, element, type);
            default: {
                if (!this.rendererByCompId.has(type.id)) {
                    /** @type {?} */
                    const styles = flattenStyles(type.id, type.styles, []);
                    this.sharedStylesHost.addStyles(styles);
                    this.rendererByCompId.set(type.id, this.defaultRenderer);
                }
                return this.defaultRenderer;
            }
        }
    }
    /**
     * @return {?}
     */
    begin() { }
    /**
     * @return {?}
     */
    end() { }
}
DomRendererFactory2.decorators = [
    { type: Injectable }
];
/** @nocollapse */
DomRendererFactory2.ctorParameters = () => [
    { type: EventManager },
    { type: DomSharedStylesHost },
    { type: String, decorators: [{ type: Inject, args: [APP_ID,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DomRendererFactory2.prototype.rendererByCompId;
    /**
     * @type {?}
     * @private
     */
    DomRendererFactory2.prototype.defaultRenderer;
    /**
     * @type {?}
     * @private
     */
    DomRendererFactory2.prototype.eventManager;
    /**
     * @type {?}
     * @private
     */
    DomRendererFactory2.prototype.sharedStylesHost;
    /**
     * @type {?}
     * @private
     */
    DomRendererFactory2.prototype.appId;
}
class DefaultDomRenderer2 {
    /**
     * @param {?} eventManager
     */
    constructor(eventManager) {
        this.eventManager = eventManager;
        this.data = Object.create(null);
    }
    /**
     * @return {?}
     */
    destroy() { }
    /**
     * @param {?} name
     * @param {?=} namespace
     * @return {?}
     */
    createElement(name, namespace) {
        if (namespace) {
            // In cases where Ivy (not ViewEngine) is giving us the actual namespace, the look up by key
            // will result in undefined, so we just return the namespace here.
            return document.createElementNS(NAMESPACE_URIS[namespace] || namespace, name);
        }
        return document.createElement(name);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    createComment(value) { return document.createComment(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    createText(value) { return document.createTextNode(value); }
    /**
     * @param {?} parent
     * @param {?} newChild
     * @return {?}
     */
    appendChild(parent, newChild) { parent.appendChild(newChild); }
    /**
     * @param {?} parent
     * @param {?} newChild
     * @param {?} refChild
     * @return {?}
     */
    insertBefore(parent, newChild, refChild) {
        if (parent) {
            parent.insertBefore(newChild, refChild);
        }
    }
    /**
     * @param {?} parent
     * @param {?} oldChild
     * @return {?}
     */
    removeChild(parent, oldChild) {
        if (parent) {
            parent.removeChild(oldChild);
        }
    }
    /**
     * @param {?} selectorOrNode
     * @param {?=} preserveContent
     * @return {?}
     */
    selectRootElement(selectorOrNode, preserveContent) {
        /** @type {?} */
        let el = typeof selectorOrNode === 'string' ? document.querySelector(selectorOrNode) :
            selectorOrNode;
        if (!el) {
            throw new Error(`The selector "${selectorOrNode}" did not match any elements`);
        }
        if (!preserveContent) {
            el.textContent = '';
        }
        return el;
    }
    /**
     * @param {?} node
     * @return {?}
     */
    parentNode(node) { return node.parentNode; }
    /**
     * @param {?} node
     * @return {?}
     */
    nextSibling(node) { return node.nextSibling; }
    /**
     * @param {?} el
     * @param {?} name
     * @param {?} value
     * @param {?=} namespace
     * @return {?}
     */
    setAttribute(el, name, value, namespace) {
        if (namespace) {
            name = namespace + ':' + name;
            // TODO(benlesh): Ivy may cause issues here because it's passing around
            // full URIs for namespaces, therefore this lookup will fail.
            /** @type {?} */
            const namespaceUri = NAMESPACE_URIS[namespace];
            if (namespaceUri) {
                el.setAttributeNS(namespaceUri, name, value);
            }
            else {
                el.setAttribute(name, value);
            }
        }
        else {
            el.setAttribute(name, value);
        }
    }
    /**
     * @param {?} el
     * @param {?} name
     * @param {?=} namespace
     * @return {?}
     */
    removeAttribute(el, name, namespace) {
        if (namespace) {
            // TODO(benlesh): Ivy may cause issues here because it's passing around
            // full URIs for namespaces, therefore this lookup will fail.
            /** @type {?} */
            const namespaceUri = NAMESPACE_URIS[namespace];
            if (namespaceUri) {
                el.removeAttributeNS(namespaceUri, name);
            }
            else {
                // TODO(benlesh): Since ivy is passing around full URIs for namespaces
                // this could result in properties like `http://www.w3.org/2000/svg:cx="123"`,
                // which is wrong.
                el.removeAttribute(`${namespace}:${name}`);
            }
        }
        else {
            el.removeAttribute(name);
        }
    }
    /**
     * @param {?} el
     * @param {?} name
     * @return {?}
     */
    addClass(el, name) { el.classList.add(name); }
    /**
     * @param {?} el
     * @param {?} name
     * @return {?}
     */
    removeClass(el, name) { el.classList.remove(name); }
    /**
     * @param {?} el
     * @param {?} style
     * @param {?} value
     * @param {?} flags
     * @return {?}
     */
    setStyle(el, style, value, flags) {
        if (flags & RendererStyleFlags2.DashCase) {
            el.style.setProperty(style, value, !!(flags & RendererStyleFlags2.Important) ? 'important' : '');
        }
        else {
            el.style[style] = value;
        }
    }
    /**
     * @param {?} el
     * @param {?} style
     * @param {?} flags
     * @return {?}
     */
    removeStyle(el, style, flags) {
        if (flags & RendererStyleFlags2.DashCase) {
            el.style.removeProperty(style);
        }
        else {
            // IE requires '' instead of null
            // see https://github.com/angular/angular/issues/7916
            el.style[style] = '';
        }
    }
    /**
     * @param {?} el
     * @param {?} name
     * @param {?} value
     * @return {?}
     */
    setProperty(el, name, value) {
        checkNoSyntheticProp(name, 'property');
        el[name] = value;
    }
    /**
     * @param {?} node
     * @param {?} value
     * @return {?}
     */
    setValue(node, value) { node.nodeValue = value; }
    /**
     * @param {?} target
     * @param {?} event
     * @param {?} callback
     * @return {?}
     */
    listen(target, event, callback) {
        checkNoSyntheticProp(event, 'listener');
        if (typeof target === 'string') {
            return (/** @type {?} */ (this.eventManager.addGlobalEventListener(target, event, decoratePreventDefault(callback))));
        }
        return (/** @type {?} */ ((/** @type {?} */ (this.eventManager.addEventListener(target, event, decoratePreventDefault(callback))))));
    }
}
if (false) {
    /** @type {?} */
    DefaultDomRenderer2.prototype.data;
    /** @type {?} */
    DefaultDomRenderer2.prototype.destroyNode;
    /**
     * @type {?}
     * @private
     */
    DefaultDomRenderer2.prototype.eventManager;
}
const ɵ0$2 = /**
 * @return {?}
 */
() => '@'.charCodeAt(0);
/** @type {?} */
const AT_CHARCODE = ((ɵ0$2))();
/**
 * @param {?} name
 * @param {?} nameKind
 * @return {?}
 */
function checkNoSyntheticProp(name, nameKind) {
    if (name.charCodeAt(0) === AT_CHARCODE) {
        throw new Error(`Found the synthetic ${nameKind} ${name}. Please include either "BrowserAnimationsModule" or "NoopAnimationsModule" in your application.`);
    }
}
class EmulatedEncapsulationDomRenderer2 extends DefaultDomRenderer2 {
    /**
     * @param {?} eventManager
     * @param {?} sharedStylesHost
     * @param {?} component
     * @param {?} appId
     */
    constructor(eventManager, sharedStylesHost, component, appId) {
        super(eventManager);
        this.component = component;
        /** @type {?} */
        const styles = flattenStyles(appId + '-' + component.id, component.styles, []);
        sharedStylesHost.addStyles(styles);
        this.contentAttr = shimContentAttribute(appId + '-' + component.id);
        this.hostAttr = shimHostAttribute(appId + '-' + component.id);
    }
    /**
     * @param {?} element
     * @return {?}
     */
    applyToHost(element) { super.setAttribute(element, this.hostAttr, ''); }
    /**
     * @param {?} parent
     * @param {?} name
     * @return {?}
     */
    createElement(parent, name) {
        /** @type {?} */
        const el = super.createElement(parent, name);
        super.setAttribute(el, this.contentAttr, '');
        return el;
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    EmulatedEncapsulationDomRenderer2.prototype.contentAttr;
    /**
     * @type {?}
     * @private
     */
    EmulatedEncapsulationDomRenderer2.prototype.hostAttr;
    /**
     * @type {?}
     * @private
     */
    EmulatedEncapsulationDomRenderer2.prototype.component;
}
class ShadowDomRenderer extends DefaultDomRenderer2 {
    /**
     * @param {?} eventManager
     * @param {?} sharedStylesHost
     * @param {?} hostEl
     * @param {?} component
     */
    constructor(eventManager, sharedStylesHost, hostEl, component) {
        super(eventManager);
        this.sharedStylesHost = sharedStylesHost;
        this.hostEl = hostEl;
        this.component = component;
        if (component.encapsulation === ViewEncapsulation.ShadowDom) {
            this.shadowRoot = ((/** @type {?} */ (hostEl))).attachShadow({ mode: 'open' });
        }
        else {
            this.shadowRoot = ((/** @type {?} */ (hostEl))).createShadowRoot();
        }
        this.sharedStylesHost.addHost(this.shadowRoot);
        /** @type {?} */
        const styles = flattenStyles(component.id, component.styles, []);
        for (let i = 0; i < styles.length; i++) {
            /** @type {?} */
            const styleEl = document.createElement('style');
            styleEl.textContent = styles[i];
            this.shadowRoot.appendChild(styleEl);
        }
    }
    /**
     * @private
     * @param {?} node
     * @return {?}
     */
    nodeOrShadowRoot(node) { return node === this.hostEl ? this.shadowRoot : node; }
    /**
     * @return {?}
     */
    destroy() { this.sharedStylesHost.removeHost(this.shadowRoot); }
    /**
     * @param {?} parent
     * @param {?} newChild
     * @return {?}
     */
    appendChild(parent, newChild) {
        return super.appendChild(this.nodeOrShadowRoot(parent), newChild);
    }
    /**
     * @param {?} parent
     * @param {?} newChild
     * @param {?} refChild
     * @return {?}
     */
    insertBefore(parent, newChild, refChild) {
        return super.insertBefore(this.nodeOrShadowRoot(parent), newChild, refChild);
    }
    /**
     * @param {?} parent
     * @param {?} oldChild
     * @return {?}
     */
    removeChild(parent, oldChild) {
        return super.removeChild(this.nodeOrShadowRoot(parent), oldChild);
    }
    /**
     * @param {?} node
     * @return {?}
     */
    parentNode(node) {
        return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(node)));
    }
}
if (false) {
    /**
     * @type {?}
     * @private
     */
    ShadowDomRenderer.prototype.shadowRoot;
    /**
     * @type {?}
     * @private
     */
    ShadowDomRenderer.prototype.sharedStylesHost;
    /**
     * @type {?}
     * @private
     */
    ShadowDomRenderer.prototype.hostEl;
    /**
     * @type {?}
     * @private
     */
    ShadowDomRenderer.prototype.component;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
const ɵ0$3 = /**
 * @return {?}
 */
() => (typeof Zone !== 'undefined') && ((/** @type {?} */ (Zone)))['__symbol__'] ||
    (/**
     * @param {?} v
     * @return {?}
     */
    function (v) { return '__zone_symbol__' + v; });
/**
 * Detect if Zone is present. If it is then use simple zone aware 'addEventListener'
 * since Angular can do much more
 * efficient bookkeeping than Zone can, because we have additional information. This speeds up
 * addEventListener by 3x.
 * @type {?}
 */
const __symbol__ = ((ɵ0$3))();
/** @type {?} */
const ADD_EVENT_LISTENER = __symbol__('addEventListener');
/** @type {?} */
const REMOVE_EVENT_LISTENER = __symbol__('removeEventListener');
/** @type {?} */
const symbolNames = {};
/** @type {?} */
const FALSE = 'FALSE';
/** @type {?} */
const ANGULAR = 'ANGULAR';
/** @type {?} */
const NATIVE_ADD_LISTENER = 'addEventListener';
/** @type {?} */
const NATIVE_REMOVE_LISTENER = 'removeEventListener';
// use the same symbol string which is used in zone.js
/** @type {?} */
const stopSymbol = '__zone_symbol__propagationStopped';
/** @type {?} */
const stopMethodSymbol = '__zone_symbol__stopImmediatePropagation';
const ɵ1 = /**
 * @return {?}
 */
() => {
    /** @type {?} */
    const blackListedEvents = (typeof Zone !== 'undefined') && ((/** @type {?} */ (Zone)))[__symbol__('BLACK_LISTED_EVENTS')];
    if (blackListedEvents) {
        /** @type {?} */
        const res = {};
        blackListedEvents.forEach((/**
         * @param {?} eventName
         * @return {?}
         */
        eventName => { res[eventName] = eventName; }));
        return res;
    }
    return undefined;
};
/** @type {?} */
const blackListedMap = ((ɵ1))();
/** @type {?} */
const isBlackListedEvent = (/**
 * @param {?} eventName
 * @return {?}
 */
function (eventName) {
    if (!blackListedMap) {
        return false;
    }
    return blackListedMap.hasOwnProperty(eventName);
});
const ɵ2 = isBlackListedEvent;
/**
 * @record
 */
function TaskData() { }
if (false) {
    /** @type {?} */
    TaskData.prototype.zone;
    /** @type {?} */
    TaskData.prototype.handler;
}
// a global listener to handle all dom event,
// so we do not need to create a closure every time
/** @type {?} */
const globalListener = (/**
 * @this {?}
 * @param {?} event
 * @return {?}
 */
function (event) {
    /** @type {?} */
    const symbolName = symbolNames[event.type];
    if (!symbolName) {
        return;
    }
    /** @type {?} */
    const taskDatas = this[symbolName];
    if (!taskDatas) {
        return;
    }
    /** @type {?} */
    const args = [event];
    if (taskDatas.length === 1) {
        // if taskDatas only have one element, just invoke it
        /** @type {?} */
        const taskData = taskDatas[0];
        if (taskData.zone !== Zone.current) {
            // only use Zone.run when Zone.current not equals to stored zone
            return taskData.zone.run(taskData.handler, this, args);
        }
        else {
            return taskData.handler.apply(this, args);
        }
    }
    else {
        // copy tasks as a snapshot to avoid event handlers remove
        // itself or others
        /** @type {?} */
        const copiedTasks = taskDatas.slice();
        for (let i = 0; i < copiedTasks.length; i++) {
            // if other listener call event.stopImmediatePropagation
            // just break
            if (((/** @type {?} */ (event)))[stopSymbol] === true) {
                break;
            }
            /** @type {?} */
            const taskData = copiedTasks[i];
            if (taskData.zone !== Zone.current) {
                // only use Zone.run when Zone.current not equals to stored zone
                taskData.zone.run(taskData.handler, this, args);
            }
            else {
                taskData.handler.apply(this, args);
            }
        }
    }
});
const ɵ3 = globalListener;
class DomEventsPlugin extends EventManagerPlugin {
    /**
     * @param {?} doc
     * @param {?} ngZone
     * @param {?} platformId
     */
    constructor(doc, ngZone, platformId) {
        super(doc);
        this.ngZone = ngZone;
        if (!platformId || !isPlatformServer(platformId)) {
            this.patchEvent();
        }
    }
    /**
     * @private
     * @return {?}
     */
    patchEvent() {
        if (typeof Event === 'undefined' || !Event || !Event.prototype) {
            return;
        }
        if (((/** @type {?} */ (Event.prototype)))[stopMethodSymbol]) {
            // already patched by zone.js
            return;
        }
        /** @type {?} */
        const delegate = ((/** @type {?} */ (Event.prototype)))[stopMethodSymbol] =
            Event.prototype.stopImmediatePropagation;
        Event.prototype.stopImmediatePropagation = (/**
         * @this {?}
         * @return {?}
         */
        function () {
            if (this) {
                this[stopSymbol] = true;
            }
            // We should call native delegate in case in some environment part of
            // the application will not use the patched Event. Also we cast the
            // "arguments" to any since "stopImmediatePropagation" technically does not
            // accept any arguments, but we don't know what developers pass through the
            // function and we want to not break these calls.
            delegate && delegate.apply(this, (/** @type {?} */ (arguments)));
        });
    }
    // This plugin should come last in the list of plugins, because it accepts all
    // events.
    /**
     * @param {?} eventName
     * @return {?}
     */
    supports(eventName) { return true; }
    /**
     * @param {?} element
     * @param {?} eventName
     * @param {?} handler
     * @return {?}
     */
    addEventListener(element, eventName, handler) {
        /**
         * This code is about to add a listener to the DOM. If Zone.js is present, than
         * `addEventListener` has been patched. The patched code adds overhead in both
         * memory and speed (3x slower) than native. For this reason if we detect that
         * Zone.js is present we use a simple version of zone aware addEventListener instead.
         * The result is faster registration and the zone will be restored.
         * But ZoneSpec.onScheduleTask, ZoneSpec.onInvokeTask, ZoneSpec.onCancelTask
         * will not be invoked
         * We also do manual zone restoration in element.ts renderEventHandlerClosure method.
         *
         * NOTE: it is possible that the element is from different iframe, and so we
         * have to check before we execute the method.
         * @type {?}
         */
        const self = this;
        /** @type {?} */
        const zoneJsLoaded = element[ADD_EVENT_LISTENER];
        /** @type {?} */
        let callback = (/** @type {?} */ (handler));
        // if zonejs is loaded and current zone is not ngZone
        // we keep Zone.current on target for later restoration.
        if (zoneJsLoaded && (!NgZone.isInAngularZone() || isBlackListedEvent(eventName))) {
            /** @type {?} */
            let symbolName = symbolNames[eventName];
            if (!symbolName) {
                symbolName = symbolNames[eventName] = __symbol__(ANGULAR + eventName + FALSE);
            }
            /** @type {?} */
            let taskDatas = ((/** @type {?} */ (element)))[symbolName];
            /** @type {?} */
            const globalListenerRegistered = taskDatas && taskDatas.length > 0;
            if (!taskDatas) {
                taskDatas = ((/** @type {?} */ (element)))[symbolName] = [];
            }
            /** @type {?} */
            const zone = isBlackListedEvent(eventName) ? Zone.root : Zone.current;
            if (taskDatas.length === 0) {
                taskDatas.push({ zone: zone, handler: callback });
            }
            else {
                /** @type {?} */
                let callbackRegistered = false;
                for (let i = 0; i < taskDatas.length; i++) {
                    if (taskDatas[i].handler === callback) {
                        callbackRegistered = true;
                        break;
                    }
                }
                if (!callbackRegistered) {
                    taskDatas.push({ zone: zone, handler: callback });
                }
            }
            if (!globalListenerRegistered) {
                element[ADD_EVENT_LISTENER](eventName, globalListener, false);
            }
        }
        else {
            element[NATIVE_ADD_LISTENER](eventName, callback, false);
        }
        return (/**
         * @return {?}
         */
        () => this.removeEventListener(element, eventName, callback));
    }
    /**
     * @param {?} target
     * @param {?} eventName
     * @param {?} callback
     * @return {?}
     */
    removeEventListener(target, eventName, callback) {
        /** @type {?} */
        let underlyingRemove = target[REMOVE_EVENT_LISTENER];
        // zone.js not loaded, use native removeEventListener
        if (!underlyingRemove) {
            return target[NATIVE_REMOVE_LISTENER].apply(target, [eventName, callback, false]);
        }
        /** @type {?} */
        let symbolName = symbolNames[eventName];
        /** @type {?} */
        let taskDatas = symbolName && target[symbolName];
        if (!taskDatas) {
            // addEventListener not using patched version
            // just call native removeEventListener
            return target[NATIVE_REMOVE_LISTENER].apply(target, [eventName, callback, false]);
        }
        // fix issue 20532, should be able to remove
        // listener which was added inside of ngZone
        /** @type {?} */
        let found = false;
        for (let i = 0; i < taskDatas.length; i++) {
            // remove listener from taskDatas if the callback equals
            if (taskDatas[i].handler === callback) {
                found = true;
                taskDatas.splice(i, 1);
                break;
            }
        }
        if (found) {
            if (taskDatas.length === 0) {
                // all listeners are removed, we can remove the globalListener from target
                underlyingRemove.apply(target, [eventName, globalListener, false]);
            }
        }
        else {
            // not found in taskDatas, the callback may be added inside of ngZone
            // use native remove listener to remove the callback
            target[NATIVE_REMOVE_LISTENER].apply(target, [eventName, callback, false]);
        }
    }
}
DomEventsPlugin.decorators = [
    { type: Injectable }
];
/** @nocollapse */
DomEventsPlugin.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] },
    { type: NgZone },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [PLATFORM_ID,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DomEventsPlugin.prototype.ngZone;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Supported HammerJS recognizer event names.
 * @type {?}
 */
const EVENT_NAMES = {
    // pan
    'pan': true,
    'panstart': true,
    'panmove': true,
    'panend': true,
    'pancancel': true,
    'panleft': true,
    'panright': true,
    'panup': true,
    'pandown': true,
    // pinch
    'pinch': true,
    'pinchstart': true,
    'pinchmove': true,
    'pinchend': true,
    'pinchcancel': true,
    'pinchin': true,
    'pinchout': true,
    // press
    'press': true,
    'pressup': true,
    // rotate
    'rotate': true,
    'rotatestart': true,
    'rotatemove': true,
    'rotateend': true,
    'rotatecancel': true,
    // swipe
    'swipe': true,
    'swipeleft': true,
    'swiperight': true,
    'swipeup': true,
    'swipedown': true,
    // tap
    'tap': true,
};
/**
 * DI token for providing [HammerJS](http://hammerjs.github.io/) support to Angular.
 * @see `HammerGestureConfig`
 *
 * \@publicApi
 * @type {?}
 */
const HAMMER_GESTURE_CONFIG = new InjectionToken('HammerGestureConfig');
/**
 * Injection token used to provide a {\@link HammerLoader} to Angular.
 *
 * \@publicApi
 * @type {?}
 */
const HAMMER_LOADER = new InjectionToken('HammerLoader');
/**
 * @record
 */
function HammerInstance() { }
if (false) {
    /**
     * @param {?} eventName
     * @param {?=} callback
     * @return {?}
     */
    HammerInstance.prototype.on = function (eventName, callback) { };
    /**
     * @param {?} eventName
     * @param {?=} callback
     * @return {?}
     */
    HammerInstance.prototype.off = function (eventName, callback) { };
    /**
     * @return {?}
     */
    HammerInstance.prototype.destroy = function () { };
}
/**
 * An injectable [HammerJS Manager](http://hammerjs.github.io/api/#hammer.manager)
 * for gesture recognition. Configures specific event recognition.
 * \@publicApi
 */
class HammerGestureConfig {
    constructor() {
        /**
         * A set of supported event names for gestures to be used in Angular.
         * Angular supports all built-in recognizers, as listed in
         * [HammerJS documentation](http://hammerjs.github.io/).
         */
        this.events = [];
        /**
         * Maps gesture event names to a set of configuration options
         * that specify overrides to the default values for specific properties.
         *
         * The key is a supported event name to be configured,
         * and the options object contains a set of properties, with override values
         * to be applied to the named recognizer event.
         * For example, to disable recognition of the rotate event, specify
         *  `{"rotate": {"enable": false}}`.
         *
         * Properties that are not present take the HammerJS default values.
         * For information about which properties are supported for which events,
         * and their allowed and default values, see
         * [HammerJS documentation](http://hammerjs.github.io/).
         *
         */
        this.overrides = {};
    }
    /**
     * Creates a [HammerJS Manager](http://hammerjs.github.io/api/#hammer.manager)
     * and attaches it to a given HTML element.
     * @param {?} element The element that will recognize gestures.
     * @return {?} A HammerJS event-manager object.
     */
    buildHammer(element) {
        /** @type {?} */
        const mc = new (/** @type {?} */ (Hammer))(element, this.options);
        mc.get('pinch').set({ enable: true });
        mc.get('rotate').set({ enable: true });
        for (const eventName in this.overrides) {
            mc.get(eventName).set(this.overrides[eventName]);
        }
        return mc;
    }
}
HammerGestureConfig.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * A set of supported event names for gestures to be used in Angular.
     * Angular supports all built-in recognizers, as listed in
     * [HammerJS documentation](http://hammerjs.github.io/).
     * @type {?}
     */
    HammerGestureConfig.prototype.events;
    /**
     * Maps gesture event names to a set of configuration options
     * that specify overrides to the default values for specific properties.
     *
     * The key is a supported event name to be configured,
     * and the options object contains a set of properties, with override values
     * to be applied to the named recognizer event.
     * For example, to disable recognition of the rotate event, specify
     *  `{"rotate": {"enable": false}}`.
     *
     * Properties that are not present take the HammerJS default values.
     * For information about which properties are supported for which events,
     * and their allowed and default values, see
     * [HammerJS documentation](http://hammerjs.github.io/).
     *
     * @type {?}
     */
    HammerGestureConfig.prototype.overrides;
    /**
     * Properties whose default values can be overridden for a given event.
     * Different sets of properties apply to different events.
     * For information about which properties are supported for which events,
     * and their allowed and default values, see
     * [HammerJS documentation](http://hammerjs.github.io/).
     * @type {?}
     */
    HammerGestureConfig.prototype.options;
}
class HammerGesturesPlugin extends EventManagerPlugin {
    /**
     * @param {?} doc
     * @param {?} _config
     * @param {?} console
     * @param {?=} loader
     */
    constructor(doc, _config, console, loader) {
        super(doc);
        this._config = _config;
        this.console = console;
        this.loader = loader;
    }
    /**
     * @param {?} eventName
     * @return {?}
     */
    supports(eventName) {
        if (!EVENT_NAMES.hasOwnProperty(eventName.toLowerCase()) && !this.isCustomEvent(eventName)) {
            return false;
        }
        if (!((/** @type {?} */ (window))).Hammer && !this.loader) {
            this.console.warn(`The "${eventName}" event cannot be bound because Hammer.JS is not ` +
                `loaded and no custom loader has been specified.`);
            return false;
        }
        return true;
    }
    /**
     * @param {?} element
     * @param {?} eventName
     * @param {?} handler
     * @return {?}
     */
    addEventListener(element, eventName, handler) {
        /** @type {?} */
        const zone = this.manager.getZone();
        eventName = eventName.toLowerCase();
        // If Hammer is not present but a loader is specified, we defer adding the event listener
        // until Hammer is loaded.
        if (!((/** @type {?} */ (window))).Hammer && this.loader) {
            // This `addEventListener` method returns a function to remove the added listener.
            // Until Hammer is loaded, the returned function needs to *cancel* the registration rather
            // than remove anything.
            /** @type {?} */
            let cancelRegistration = false;
            /** @type {?} */
            let deregister = (/**
             * @return {?}
             */
            () => { cancelRegistration = true; });
            this.loader()
                .then((/**
             * @return {?}
             */
            () => {
                // If Hammer isn't actually loaded when the custom loader resolves, give up.
                if (!((/** @type {?} */ (window))).Hammer) {
                    this.console.warn(`The custom HAMMER_LOADER completed, but Hammer.JS is not present.`);
                    deregister = (/**
                     * @return {?}
                     */
                    () => { });
                    return;
                }
                if (!cancelRegistration) {
                    // Now that Hammer is loaded and the listener is being loaded for real,
                    // the deregistration function changes from canceling registration to removal.
                    deregister = this.addEventListener(element, eventName, handler);
                }
            }))
                .catch((/**
             * @return {?}
             */
            () => {
                this.console.warn(`The "${eventName}" event cannot be bound because the custom ` +
                    `Hammer.JS loader failed.`);
                deregister = (/**
                 * @return {?}
                 */
                () => { });
            }));
            // Return a function that *executes* `deregister` (and not `deregister` itself) so that we
            // can change the behavior of `deregister` once the listener is added. Using a closure in
            // this way allows us to avoid any additional data structures to track listener removal.
            return (/**
             * @return {?}
             */
            () => { deregister(); });
        }
        return zone.runOutsideAngular((/**
         * @return {?}
         */
        () => {
            // Creating the manager bind events, must be done outside of angular
            /** @type {?} */
            const mc = this._config.buildHammer(element);
            /** @type {?} */
            const callback = (/**
             * @param {?} eventObj
             * @return {?}
             */
            function (eventObj) {
                zone.runGuarded((/**
                 * @return {?}
                 */
                function () { handler(eventObj); }));
            });
            mc.on(eventName, callback);
            return (/**
             * @return {?}
             */
            () => {
                mc.off(eventName, callback);
                // destroy mc to prevent memory leak
                if (typeof mc.destroy === 'function') {
                    mc.destroy();
                }
            });
        }));
    }
    /**
     * @param {?} eventName
     * @return {?}
     */
    isCustomEvent(eventName) { return this._config.events.indexOf(eventName) > -1; }
}
HammerGesturesPlugin.decorators = [
    { type: Injectable }
];
/** @nocollapse */
HammerGesturesPlugin.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] },
    { type: HammerGestureConfig, decorators: [{ type: Inject, args: [HAMMER_GESTURE_CONFIG,] }] },
    { type: ɵConsole },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [HAMMER_LOADER,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    HammerGesturesPlugin.prototype._config;
    /**
     * @type {?}
     * @private
     */
    HammerGesturesPlugin.prototype.console;
    /**
     * @type {?}
     * @private
     */
    HammerGesturesPlugin.prototype.loader;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Defines supported modifiers for key events.
 * @type {?}
 */
const MODIFIER_KEYS = ['alt', 'control', 'meta', 'shift'];
const ɵ0$4 = /**
 * @param {?} event
 * @return {?}
 */
(event) => event.altKey, ɵ1$1 = /**
 * @param {?} event
 * @return {?}
 */
(event) => event.ctrlKey, ɵ2$1 = /**
 * @param {?} event
 * @return {?}
 */
(event) => event.metaKey, ɵ3$1 = /**
 * @param {?} event
 * @return {?}
 */
(event) => event.shiftKey;
/**
 * Retrieves modifiers from key-event objects.
 * @type {?}
 */
const MODIFIER_KEY_GETTERS = {
    'alt': (ɵ0$4),
    'control': (ɵ1$1),
    'meta': (ɵ2$1),
    'shift': (ɵ3$1)
};
/**
 * \@publicApi
 * A browser plug-in that provides support for handling of key events in Angular.
 */
class KeyEventsPlugin extends EventManagerPlugin {
    /**
     * Initializes an instance of the browser plug-in.
     * @param {?} doc The document in which key events will be detected.
     */
    constructor(doc) { super(doc); }
    /**
     * Reports whether a named key event is supported.
     * @param {?} eventName The event name to query.
     * @return {?} True if the named key event is supported.
     */
    supports(eventName) { return KeyEventsPlugin.parseEventName(eventName) != null; }
    /**
     * Registers a handler for a specific element and key event.
     * @param {?} element The HTML element to receive event notifications.
     * @param {?} eventName The name of the key event to listen for.
     * @param {?} handler A function to call when the notification occurs. Receives the
     * event object as an argument.
     * @return {?} The key event that was registered.
     */
    addEventListener(element, eventName, handler) {
        /** @type {?} */
        const parsedEvent = (/** @type {?} */ (KeyEventsPlugin.parseEventName(eventName)));
        /** @type {?} */
        const outsideHandler = KeyEventsPlugin.eventCallback(parsedEvent['fullKey'], handler, this.manager.getZone());
        return this.manager.getZone().runOutsideAngular((/**
         * @return {?}
         */
        () => {
            return getDOM().onAndCancel(element, parsedEvent['domEventName'], outsideHandler);
        }));
    }
    /**
     * @param {?} eventName
     * @return {?}
     */
    static parseEventName(eventName) {
        /** @type {?} */
        const parts = eventName.toLowerCase().split('.');
        /** @type {?} */
        const domEventName = parts.shift();
        if ((parts.length === 0) || !(domEventName === 'keydown' || domEventName === 'keyup')) {
            return null;
        }
        /** @type {?} */
        const key = KeyEventsPlugin._normalizeKey((/** @type {?} */ (parts.pop())));
        /** @type {?} */
        let fullKey = '';
        MODIFIER_KEYS.forEach((/**
         * @param {?} modifierName
         * @return {?}
         */
        modifierName => {
            /** @type {?} */
            const index = parts.indexOf(modifierName);
            if (index > -1) {
                parts.splice(index, 1);
                fullKey += modifierName + '.';
            }
        }));
        fullKey += key;
        if (parts.length != 0 || key.length === 0) {
            // returning null instead of throwing to let another plugin process the event
            return null;
        }
        /** @type {?} */
        const result = {};
        result['domEventName'] = domEventName;
        result['fullKey'] = fullKey;
        return result;
    }
    /**
     * @param {?} event
     * @return {?}
     */
    static getEventFullKey(event) {
        /** @type {?} */
        let fullKey = '';
        /** @type {?} */
        let key = getDOM().getEventKey(event);
        key = key.toLowerCase();
        if (key === ' ') {
            key = 'space'; // for readability
        }
        else if (key === '.') {
            key = 'dot'; // because '.' is used as a separator in event names
        }
        MODIFIER_KEYS.forEach((/**
         * @param {?} modifierName
         * @return {?}
         */
        modifierName => {
            if (modifierName != key) {
                /** @type {?} */
                const modifierGetter = MODIFIER_KEY_GETTERS[modifierName];
                if (modifierGetter(event)) {
                    fullKey += modifierName + '.';
                }
            }
        }));
        fullKey += key;
        return fullKey;
    }
    /**
     * Configures a handler callback for a key event.
     * @param {?} fullKey The event name that combines all simultaneous keystrokes.
     * @param {?} handler The function that responds to the key event.
     * @param {?} zone The zone in which the event occurred.
     * @return {?} A callback function.
     */
    static eventCallback(fullKey, handler, zone) {
        return (/**
         * @param {?} event
         * @return {?}
         */
        (event /** TODO #9100 */) => {
            if (KeyEventsPlugin.getEventFullKey(event) === fullKey) {
                zone.runGuarded((/**
                 * @return {?}
                 */
                () => handler(event)));
            }
        });
    }
    /**
     * \@internal
     * @param {?} keyName
     * @return {?}
     */
    static _normalizeKey(keyName) {
        // TODO: switch to a Map if the mapping grows too much
        switch (keyName) {
            case 'esc':
                return 'escape';
            default:
                return keyName;
        }
    }
}
KeyEventsPlugin.decorators = [
    { type: Injectable }
];
/** @nocollapse */
KeyEventsPlugin.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Marker interface for a value that's safe to use in a particular context.
 *
 * \@publicApi
 * @record
 */
function SafeValue() { }
/**
 * Marker interface for a value that's safe to use as HTML.
 *
 * \@publicApi
 * @record
 */
function SafeHtml() { }
/**
 * Marker interface for a value that's safe to use as style (CSS).
 *
 * \@publicApi
 * @record
 */
function SafeStyle() { }
/**
 * Marker interface for a value that's safe to use as JavaScript.
 *
 * \@publicApi
 * @record
 */
function SafeScript() { }
/**
 * Marker interface for a value that's safe to use as a URL linking to a document.
 *
 * \@publicApi
 * @record
 */
function SafeUrl() { }
/**
 * Marker interface for a value that's safe to use as a URL to load executable code from.
 *
 * \@publicApi
 * @record
 */
function SafeResourceUrl() { }
/**
 * DomSanitizer helps preventing Cross Site Scripting Security bugs (XSS) by sanitizing
 * values to be safe to use in the different DOM contexts.
 *
 * For example, when binding a URL in an `<a [href]="someValue">` hyperlink, `someValue` will be
 * sanitized so that an attacker cannot inject e.g. a `javascript:` URL that would execute code on
 * the website.
 *
 * In specific situations, it might be necessary to disable sanitization, for example if the
 * application genuinely needs to produce a `javascript:` style link with a dynamic value in it.
 * Users can bypass security by constructing a value with one of the `bypassSecurityTrust...`
 * methods, and then binding to that value from the template.
 *
 * These situations should be very rare, and extraordinary care must be taken to avoid creating a
 * Cross Site Scripting (XSS) security bug!
 *
 * When using `bypassSecurityTrust...`, make sure to call the method as early as possible and as
 * close as possible to the source of the value, to make it easy to verify no security bug is
 * created by its use.
 *
 * It is not required (and not recommended) to bypass security if the value is safe, e.g. a URL that
 * does not start with a suspicious protocol, or an HTML snippet that does not contain dangerous
 * code. The sanitizer leaves safe values intact.
 *
 * \@security Calling any of the `bypassSecurityTrust...` APIs disables Angular's built-in
 * sanitization for the value passed in. Carefully check and audit all values and code paths going
 * into this call. Make sure any user data is appropriately escaped for this security context.
 * For more detail, see the [Security Guide](http://g.co/ng/security).
 *
 * \@publicApi
 * @abstract
 */
class DomSanitizer {
}
if (false) {
    /**
     * Sanitizes a value for use in the given SecurityContext.
     *
     * If value is trusted for the context, this method will unwrap the contained safe value and use
     * it directly. Otherwise, value will be sanitized to be safe in the given context, for example
     * by replacing URLs that have an unsafe protocol part (such as `javascript:`). The implementation
     * is responsible to make sure that the value can definitely be safely used in the given context.
     * @abstract
     * @param {?} context
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.sanitize = function (context, value) { };
    /**
     * Bypass security and trust the given value to be safe HTML. Only use this when the bound HTML
     * is unsafe (e.g. contains `<script>` tags) and the code should be executed. The sanitizer will
     * leave safe HTML intact, so in most situations this method should not be used.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustHtml = function (value) { };
    /**
     * Bypass security and trust the given value to be safe style value (CSS).
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustStyle = function (value) { };
    /**
     * Bypass security and trust the given value to be safe JavaScript.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustScript = function (value) { };
    /**
     * Bypass security and trust the given value to be a safe style URL, i.e. a value that can be used
     * in hyperlinks or `<img src>`.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustUrl = function (value) { };
    /**
     * Bypass security and trust the given value to be a safe resource URL, i.e. a location that may
     * be used to load executable code from, like `<script src>`, or `<iframe src>`.
     *
     * **WARNING:** calling this method with untrusted user data exposes your application to XSS
     * security risks!
     * @abstract
     * @param {?} value
     * @return {?}
     */
    DomSanitizer.prototype.bypassSecurityTrustResourceUrl = function (value) { };
}
class DomSanitizerImpl extends DomSanitizer {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        super();
        this._doc = _doc;
    }
    /**
     * @param {?} ctx
     * @param {?} value
     * @return {?}
     */
    sanitize(ctx, value) {
        if (value == null)
            return null;
        switch (ctx) {
            case SecurityContext.NONE:
                return (/** @type {?} */ (value));
            case SecurityContext.HTML:
                if (value instanceof SafeHtmlImpl)
                    return value.changingThisBreaksApplicationSecurity;
                this.checkNotSafeValue(value, 'HTML');
                return ɵ_sanitizeHtml(this._doc, String(value));
            case SecurityContext.STYLE:
                if (value instanceof SafeStyleImpl)
                    return value.changingThisBreaksApplicationSecurity;
                this.checkNotSafeValue(value, 'Style');
                return ɵ_sanitizeStyle((/** @type {?} */ (value)));
            case SecurityContext.SCRIPT:
                if (value instanceof SafeScriptImpl)
                    return value.changingThisBreaksApplicationSecurity;
                this.checkNotSafeValue(value, 'Script');
                throw new Error('unsafe value used in a script context');
            case SecurityContext.URL:
                if (value instanceof SafeResourceUrlImpl || value instanceof SafeUrlImpl) {
                    // Allow resource URLs in URL contexts, they are strictly more trusted.
                    return value.changingThisBreaksApplicationSecurity;
                }
                this.checkNotSafeValue(value, 'URL');
                return ɵ_sanitizeUrl(String(value));
            case SecurityContext.RESOURCE_URL:
                if (value instanceof SafeResourceUrlImpl) {
                    return value.changingThisBreaksApplicationSecurity;
                }
                this.checkNotSafeValue(value, 'ResourceURL');
                throw new Error('unsafe value used in a resource URL context (see http://g.co/ng/security#xss)');
            default:
                throw new Error(`Unexpected SecurityContext ${ctx} (see http://g.co/ng/security#xss)`);
        }
    }
    /**
     * @private
     * @param {?} value
     * @param {?} expectedType
     * @return {?}
     */
    checkNotSafeValue(value, expectedType) {
        if (value instanceof SafeValueImpl) {
            throw new Error(`Required a safe ${expectedType}, got a ${value.getTypeName()} ` +
                `(see http://g.co/ng/security#xss)`);
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustHtml(value) { return new SafeHtmlImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustStyle(value) { return new SafeStyleImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustScript(value) { return new SafeScriptImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustUrl(value) { return new SafeUrlImpl(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    bypassSecurityTrustResourceUrl(value) {
        return new SafeResourceUrlImpl(value);
    }
}
DomSanitizerImpl.decorators = [
    { type: Injectable }
];
/** @nocollapse */
DomSanitizerImpl.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DomSanitizerImpl.prototype._doc;
}
/**
 * @abstract
 */
class SafeValueImpl {
    /**
     * @param {?} changingThisBreaksApplicationSecurity
     */
    constructor(changingThisBreaksApplicationSecurity) {
        this.changingThisBreaksApplicationSecurity = changingThisBreaksApplicationSecurity;
        // empty
    }
    /**
     * @return {?}
     */
    toString() {
        return `SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}` +
            ` (see http://g.co/ng/security#xss)`;
    }
}
if (false) {
    /** @type {?} */
    SafeValueImpl.prototype.changingThisBreaksApplicationSecurity;
    /**
     * @abstract
     * @return {?}
     */
    SafeValueImpl.prototype.getTypeName = function () { };
}
class SafeHtmlImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'HTML'; }
}
class SafeStyleImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'Style'; }
}
class SafeScriptImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'Script'; }
}
class SafeUrlImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'URL'; }
}
class SafeResourceUrlImpl extends SafeValueImpl {
    /**
     * @return {?}
     */
    getTypeName() { return 'ResourceURL'; }
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
const ɵ0$5 = ɵPLATFORM_BROWSER_ID;
/** @type {?} */
const INTERNAL_BROWSER_PLATFORM_PROVIDERS = [
    { provide: PLATFORM_ID, useValue: ɵ0$5 },
    { provide: PLATFORM_INITIALIZER, useValue: initDomAdapter, multi: true },
    { provide: PlatformLocation, useClass: BrowserPlatformLocation, deps: [DOCUMENT] },
    { provide: DOCUMENT, useFactory: _document, deps: [] },
];
/**
 * \@security Replacing built-in sanitization providers exposes the application to XSS risks.
 * Attacker-controlled data introduced by an unsanitized provider could expose your
 * application to XSS risks. For more detail, see the [Security Guide](http://g.co/ng/security).
 * \@publicApi
 * @type {?}
 */
const BROWSER_SANITIZATION_PROVIDERS = [
    { provide: Sanitizer, useExisting: DomSanitizer },
    { provide: DomSanitizer, useClass: DomSanitizerImpl, deps: [DOCUMENT] },
];
/**
 * \@publicApi
 * @type {?}
 */
const platformBrowser = createPlatformFactory(platformCore, 'browser', INTERNAL_BROWSER_PLATFORM_PROVIDERS);
/**
 * @return {?}
 */
function initDomAdapter() {
    BrowserDomAdapter.makeCurrent();
    BrowserGetTestability.init();
}
/**
 * @return {?}
 */
function errorHandler() {
    return new ErrorHandler();
}
/**
 * @return {?}
 */
function _document() {
    return document;
}
/** @type {?} */
const BROWSER_MODULE_PROVIDERS = [
    BROWSER_SANITIZATION_PROVIDERS,
    { provide: ɵAPP_ROOT, useValue: true },
    { provide: ErrorHandler, useFactory: errorHandler, deps: [] },
    {
        provide: EVENT_MANAGER_PLUGINS,
        useClass: DomEventsPlugin,
        multi: true,
        deps: [DOCUMENT, NgZone, PLATFORM_ID]
    },
    { provide: EVENT_MANAGER_PLUGINS, useClass: KeyEventsPlugin, multi: true, deps: [DOCUMENT] },
    {
        provide: EVENT_MANAGER_PLUGINS,
        useClass: HammerGesturesPlugin,
        multi: true,
        deps: [DOCUMENT, HAMMER_GESTURE_CONFIG, ɵConsole, [new Optional(), HAMMER_LOADER]]
    },
    { provide: HAMMER_GESTURE_CONFIG, useClass: HammerGestureConfig, deps: [] },
    {
        provide: DomRendererFactory2,
        useClass: DomRendererFactory2,
        deps: [EventManager, DomSharedStylesHost, APP_ID]
    },
    { provide: RendererFactory2, useExisting: DomRendererFactory2 },
    { provide: SharedStylesHost, useExisting: DomSharedStylesHost },
    { provide: DomSharedStylesHost, useClass: DomSharedStylesHost, deps: [DOCUMENT] },
    { provide: Testability, useClass: Testability, deps: [NgZone] },
    { provide: EventManager, useClass: EventManager, deps: [EVENT_MANAGER_PLUGINS, NgZone] },
    ELEMENT_PROBE_PROVIDERS,
];
/**
 * Exports required infrastructure for all Angular apps.
 * Included by default in all Angular apps created with the CLI
 * `new` command.
 * Re-exports `CommonModule` and `ApplicationModule`, making their
 * exports and providers available to all apps.
 *
 * \@publicApi
 */
class BrowserModule {
    /**
     * @param {?} parentModule
     */
    constructor(parentModule) {
        if (parentModule) {
            throw new Error(`BrowserModule has already been loaded. If you need access to common directives such as NgIf and NgFor from a lazy loaded module, import CommonModule instead.`);
        }
    }
    /**
     * Configures a browser-based app to transition from a server-rendered app, if
     * one is present on the page.
     *
     * @param {?} params An object containing an identifier for the app to transition.
     * The ID must match between the client and server versions of the app.
     * @return {?} The reconfigured `BrowserModule` to import into the app's root `AppModule`.
     */
    static withServerTransition(params) {
        return {
            ngModule: BrowserModule,
            providers: [
                { provide: APP_ID, useValue: params.appId },
                { provide: TRANSITION_ID, useExisting: APP_ID },
                SERVER_TRANSITION_PROVIDERS,
            ],
        };
    }
}
BrowserModule.decorators = [
    { type: NgModule, args: [{ providers: BROWSER_MODULE_PROVIDERS, exports: [CommonModule, ApplicationModule] },] }
];
/** @nocollapse */
BrowserModule.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Optional }, { type: SkipSelf }, { type: Inject, args: [BrowserModule,] }] }
];

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Factory to create Meta service.
 * @return {?}
 */
function createMeta() {
    return new Meta(ɵɵinject(DOCUMENT));
}
/**
 * A service that can be used to get and add meta tags.
 *
 * \@publicApi
 */
class Meta {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        this._doc = _doc;
        this._dom = getDOM();
    }
    /**
     * @param {?} tag
     * @param {?=} forceCreation
     * @return {?}
     */
    addTag(tag, forceCreation = false) {
        if (!tag)
            return null;
        return this._getOrCreateElement(tag, forceCreation);
    }
    /**
     * @param {?} tags
     * @param {?=} forceCreation
     * @return {?}
     */
    addTags(tags, forceCreation = false) {
        if (!tags)
            return [];
        return tags.reduce((/**
         * @param {?} result
         * @param {?} tag
         * @return {?}
         */
        (result, tag) => {
            if (tag) {
                result.push(this._getOrCreateElement(tag, forceCreation));
            }
            return result;
        }), []);
    }
    /**
     * @param {?} attrSelector
     * @return {?}
     */
    getTag(attrSelector) {
        if (!attrSelector)
            return null;
        return this._dom.querySelector(this._doc, `meta[${attrSelector}]`) || null;
    }
    /**
     * @param {?} attrSelector
     * @return {?}
     */
    getTags(attrSelector) {
        if (!attrSelector)
            return [];
        /** @type {?} */
        const list /*NodeList*/ = this._dom.querySelectorAll(this._doc, `meta[${attrSelector}]`);
        return list ? [].slice.call(list) : [];
    }
    /**
     * @param {?} tag
     * @param {?=} selector
     * @return {?}
     */
    updateTag(tag, selector) {
        if (!tag)
            return null;
        selector = selector || this._parseSelector(tag);
        /** @type {?} */
        const meta = (/** @type {?} */ (this.getTag(selector)));
        if (meta) {
            return this._setMetaElementAttributes(tag, meta);
        }
        return this._getOrCreateElement(tag, true);
    }
    /**
     * @param {?} attrSelector
     * @return {?}
     */
    removeTag(attrSelector) { this.removeTagElement((/** @type {?} */ (this.getTag(attrSelector)))); }
    /**
     * @param {?} meta
     * @return {?}
     */
    removeTagElement(meta) {
        if (meta) {
            this._dom.remove(meta);
        }
    }
    /**
     * @private
     * @param {?} meta
     * @param {?=} forceCreation
     * @return {?}
     */
    _getOrCreateElement(meta, forceCreation = false) {
        if (!forceCreation) {
            /** @type {?} */
            const selector = this._parseSelector(meta);
            /** @type {?} */
            const elem = (/** @type {?} */ (this.getTag(selector)));
            // It's allowed to have multiple elements with the same name so it's not enough to
            // just check that element with the same name already present on the page. We also need to
            // check if element has tag attributes
            if (elem && this._containsAttributes(meta, elem))
                return elem;
        }
        /** @type {?} */
        const element = (/** @type {?} */ (this._dom.createElement('meta')));
        this._setMetaElementAttributes(meta, element);
        /** @type {?} */
        const head = this._dom.getElementsByTagName(this._doc, 'head')[0];
        this._dom.appendChild(head, element);
        return element;
    }
    /**
     * @private
     * @param {?} tag
     * @param {?} el
     * @return {?}
     */
    _setMetaElementAttributes(tag, el) {
        Object.keys(tag).forEach((/**
         * @param {?} prop
         * @return {?}
         */
        (prop) => this._dom.setAttribute(el, prop, tag[prop])));
        return el;
    }
    /**
     * @private
     * @param {?} tag
     * @return {?}
     */
    _parseSelector(tag) {
        /** @type {?} */
        const attr = tag.name ? 'name' : 'property';
        return `${attr}="${tag[attr]}"`;
    }
    /**
     * @private
     * @param {?} tag
     * @param {?} elem
     * @return {?}
     */
    _containsAttributes(tag, elem) {
        return Object.keys(tag).every((/**
         * @param {?} key
         * @return {?}
         */
        (key) => this._dom.getAttribute(elem, key) === tag[key]));
    }
}
Meta.decorators = [
    { type: Injectable, args: [{ providedIn: 'root', useFactory: createMeta, deps: [] },] }
];
/** @nocollapse */
Meta.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
/** @nocollapse */ Meta.ngInjectableDef = ɵɵdefineInjectable({ factory: createMeta, token: Meta, providedIn: "root" });
if (false) {
    /**
     * @type {?}
     * @private
     */
    Meta.prototype._dom;
    /**
     * @type {?}
     * @private
     */
    Meta.prototype._doc;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Factory to create Title service.
 * @return {?}
 */
function createTitle() {
    return new Title(ɵɵinject(DOCUMENT));
}
/**
 * A service that can be used to get and set the title of a current HTML document.
 *
 * Since an Angular application can't be bootstrapped on the entire HTML document (`<html>` tag)
 * it is not possible to bind to the `text` property of the `HTMLTitleElement` elements
 * (representing the `<title>` tag). Instead, this service can be used to set and get the current
 * title value.
 *
 * \@publicApi
 */
class Title {
    /**
     * @param {?} _doc
     */
    constructor(_doc) {
        this._doc = _doc;
    }
    /**
     * Get the title of the current HTML document.
     * @return {?}
     */
    getTitle() { return getDOM().getTitle(this._doc); }
    /**
     * Set the title of the current HTML document.
     * @param {?} newTitle
     * @return {?}
     */
    setTitle(newTitle) { getDOM().setTitle(this._doc, newTitle); }
}
Title.decorators = [
    { type: Injectable, args: [{ providedIn: 'root', useFactory: createTitle, deps: [] },] }
];
/** @nocollapse */
Title.ctorParameters = () => [
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
/** @nocollapse */ Title.ngInjectableDef = ɵɵdefineInjectable({ factory: createTitle, token: Title, providedIn: "root" });
if (false) {
    /**
     * @type {?}
     * @private
     */
    Title.prototype._doc;
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
/** @type {?} */
const win = typeof window !== 'undefined' && window || (/** @type {?} */ ({}));

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
class ChangeDetectionPerfRecord {
    /**
     * @param {?} msPerTick
     * @param {?} numTicks
     */
    constructor(msPerTick, numTicks) {
        this.msPerTick = msPerTick;
        this.numTicks = numTicks;
    }
}
if (false) {
    /** @type {?} */
    ChangeDetectionPerfRecord.prototype.msPerTick;
    /** @type {?} */
    ChangeDetectionPerfRecord.prototype.numTicks;
}
/**
 * Entry point for all Angular profiling-related debug tools. This object
 * corresponds to the `ng.profiler` in the dev console.
 */
class AngularProfiler {
    /**
     * @param {?} ref
     */
    constructor(ref) { this.appRef = ref.injector.get(ApplicationRef); }
    // tslint:disable:no-console
    /**
     * Exercises change detection in a loop and then prints the average amount of
     * time in milliseconds how long a single round of change detection takes for
     * the current state of the UI. It runs a minimum of 5 rounds for a minimum
     * of 500 milliseconds.
     *
     * Optionally, a user may pass a `config` parameter containing a map of
     * options. Supported options are:
     *
     * `record` (boolean) - causes the profiler to record a CPU profile while
     * it exercises the change detector. Example:
     *
     * ```
     * ng.profiler.timeChangeDetection({record: true})
     * ```
     * @param {?} config
     * @return {?}
     */
    timeChangeDetection(config) {
        /** @type {?} */
        const record = config && config['record'];
        /** @type {?} */
        const profileName = 'Change Detection';
        // Profiler is not available in Android browsers, nor in IE 9 without dev tools opened
        /** @type {?} */
        const isProfilerAvailable = win.console.profile != null;
        if (record && isProfilerAvailable) {
            win.console.profile(profileName);
        }
        /** @type {?} */
        const start = getDOM().performanceNow();
        /** @type {?} */
        let numTicks = 0;
        while (numTicks < 5 || (getDOM().performanceNow() - start) < 500) {
            this.appRef.tick();
            numTicks++;
        }
        /** @type {?} */
        const end = getDOM().performanceNow();
        if (record && isProfilerAvailable) {
            win.console.profileEnd(profileName);
        }
        /** @type {?} */
        const msPerTick = (end - start) / numTicks;
        win.console.log(`ran ${numTicks} change detection cycles`);
        win.console.log(`${msPerTick.toFixed(2)} ms per check`);
        return new ChangeDetectionPerfRecord(msPerTick, numTicks);
    }
}
if (false) {
    /** @type {?} */
    AngularProfiler.prototype.appRef;
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const PROFILER_GLOBAL_NAME = 'profiler';
/**
 * Enabled Angular debug tools that are accessible via your browser's
 * developer console.
 *
 * Usage:
 *
 * 1. Open developer console (e.g. in Chrome Ctrl + Shift + j)
 * 1. Type `ng.` (usually the console will show auto-complete suggestion)
 * 1. Try the change detection profiler `ng.profiler.timeChangeDetection()`
 *    then hit Enter.
 *
 * \@publicApi
 * @template T
 * @param {?} ref
 * @return {?}
 */
function enableDebugTools(ref) {
    exportNgVar(PROFILER_GLOBAL_NAME, new AngularProfiler(ref));
    return ref;
}
/**
 * Disables Angular tools.
 *
 * \@publicApi
 * @return {?}
 */
function disableDebugTools() {
    exportNgVar(PROFILER_GLOBAL_NAME, null);
}

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} text
 * @return {?}
 */
function escapeHtml(text) {
    /** @type {?} */
    const escapedText = {
        '&': '&a;',
        '"': '&q;',
        '\'': '&s;',
        '<': '&l;',
        '>': '&g;',
    };
    return text.replace(/[&"'<>]/g, (/**
     * @param {?} s
     * @return {?}
     */
    s => escapedText[s]));
}
/**
 * @param {?} text
 * @return {?}
 */
function unescapeHtml(text) {
    /** @type {?} */
    const unescapedText = {
        '&a;': '&',
        '&q;': '"',
        '&s;': '\'',
        '&l;': '<',
        '&g;': '>',
    };
    return text.replace(/&[^;]+;/g, (/**
     * @param {?} s
     * @return {?}
     */
    s => unescapedText[s]));
}
/**
 * Create a `StateKey<T>` that can be used to store value of type T with `TransferState`.
 *
 * Example:
 *
 * ```
 * const COUNTER_KEY = makeStateKey<number>('counter');
 * let value = 10;
 *
 * transferState.set(COUNTER_KEY, value);
 * ```
 *
 * \@publicApi
 * @template T
 * @param {?} key
 * @return {?}
 */
function makeStateKey(key) {
    return (/** @type {?} */ (key));
}
/**
 * A key value store that is transferred from the application on the server side to the application
 * on the client side.
 *
 * `TransferState` will be available as an injectable token. To use it import
 * `ServerTransferStateModule` on the server and `BrowserTransferStateModule` on the client.
 *
 * The values in the store are serialized/deserialized using JSON.stringify/JSON.parse. So only
 * boolean, number, string, null and non-class objects will be serialized and deserialzied in a
 * non-lossy manner.
 *
 * \@publicApi
 */
class TransferState {
    constructor() {
        this.store = {};
        this.onSerializeCallbacks = {};
    }
    /**
     * \@internal
     * @param {?} initState
     * @return {?}
     */
    static init(initState) {
        /** @type {?} */
        const transferState = new TransferState();
        transferState.store = initState;
        return transferState;
    }
    /**
     * Get the value corresponding to a key. Return `defaultValue` if key is not found.
     * @template T
     * @param {?} key
     * @param {?} defaultValue
     * @return {?}
     */
    get(key, defaultValue) {
        return this.store[key] !== undefined ? (/** @type {?} */ (this.store[key])) : defaultValue;
    }
    /**
     * Set the value corresponding to a key.
     * @template T
     * @param {?} key
     * @param {?} value
     * @return {?}
     */
    set(key, value) { this.store[key] = value; }
    /**
     * Remove a key from the store.
     * @template T
     * @param {?} key
     * @return {?}
     */
    remove(key) { delete this.store[key]; }
    /**
     * Test whether a key exists in the store.
     * @template T
     * @param {?} key
     * @return {?}
     */
    hasKey(key) { return this.store.hasOwnProperty(key); }
    /**
     * Register a callback to provide the value for a key when `toJson` is called.
     * @template T
     * @param {?} key
     * @param {?} callback
     * @return {?}
     */
    onSerialize(key, callback) {
        this.onSerializeCallbacks[key] = callback;
    }
    /**
     * Serialize the current state of the store to JSON.
     * @return {?}
     */
    toJson() {
        // Call the onSerialize callbacks and put those values into the store.
        for (const key in this.onSerializeCallbacks) {
            if (this.onSerializeCallbacks.hasOwnProperty(key)) {
                try {
                    this.store[key] = this.onSerializeCallbacks[key]();
                }
                catch (e) {
                    console.warn('Exception in onSerialize callback: ', e);
                }
            }
        }
        return JSON.stringify(this.store);
    }
}
TransferState.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    TransferState.prototype.store;
    /**
     * @type {?}
     * @private
     */
    TransferState.prototype.onSerializeCallbacks;
}
/**
 * @param {?} doc
 * @param {?} appId
 * @return {?}
 */
function initTransferState(doc, appId) {
    // Locate the script tag with the JSON data transferred from the server.
    // The id of the script tag is set to the Angular appId + 'state'.
    /** @type {?} */
    const script = doc.getElementById(appId + '-state');
    /** @type {?} */
    let initialState = {};
    if (script && script.textContent) {
        try {
            initialState = JSON.parse(unescapeHtml(script.textContent));
        }
        catch (e) {
            console.warn('Exception while restoring TransferState for app ' + appId, e);
        }
    }
    return TransferState.init(initialState);
}
/**
 * NgModule to install on the client side while using the `TransferState` to transfer state from
 * server to client.
 *
 * \@publicApi
 */
class BrowserTransferStateModule {
}
BrowserTransferStateModule.decorators = [
    { type: NgModule, args: [{
                providers: [{ provide: TransferState, useFactory: initTransferState, deps: [DOCUMENT, APP_ID] }],
            },] }
];

/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Predicates for use with {\@link DebugElement}'s query functions.
 *
 * \@publicApi
 */
class By {
    /**
     * Match all nodes.
     *
     * \@usageNotes
     * ### Example
     *
     * {\@example platform-browser/dom/debug/ts/by/by.ts region='by_all'}
     * @return {?}
     */
    static all() { return (/**
     * @return {?}
     */
    () => true); }
    /**
     * Match elements by the given CSS selector.
     *
     * \@usageNotes
     * ### Example
     *
     * {\@example platform-browser/dom/debug/ts/by/by.ts region='by_css'}
     * @param {?} selector
     * @return {?}
     */
    static css(selector) {
        return (/**
         * @param {?} debugElement
         * @return {?}
         */
        (debugElement) => {
            return debugElement.nativeElement != null ?
                getDOM().elementMatches(debugElement.nativeElement, selector) :
                false;
        });
    }
    /**
     * Match nodes that have the given directive present.
     *
     * \@usageNotes
     * ### Example
     *
     * {\@example platform-browser/dom/debug/ts/by/by.ts region='by_directive'}
     * @param {?} type
     * @return {?}
     */
    static directive(type) {
        return (/**
         * @param {?} debugNode
         * @return {?}
         */
        (debugNode) => (/** @type {?} */ (debugNode.providerTokens)).indexOf(type) !== -1);
    }
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
 * \@publicApi
 * @type {?}
 */
const VERSION = new Version('8.2.14');

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

export { BROWSER_MODULE_PROVIDERS as ɵangular_packages_platform_browser_platform_browser_c, _document as ɵangular_packages_platform_browser_platform_browser_b, errorHandler as ɵangular_packages_platform_browser_platform_browser_a, GenericBrowserDomAdapter as ɵangular_packages_platform_browser_platform_browser_l, createMeta as ɵangular_packages_platform_browser_platform_browser_d, SERVER_TRANSITION_PROVIDERS as ɵangular_packages_platform_browser_platform_browser_i, appInitializerFactory as ɵangular_packages_platform_browser_platform_browser_h, createTitle as ɵangular_packages_platform_browser_platform_browser_e, initTransferState as ɵangular_packages_platform_browser_platform_browser_f, ELEMENT_PROBE_PROVIDERS__PRE_R3__ as ɵangular_packages_platform_browser_platform_browser_k, _createNgProbe as ɵangular_packages_platform_browser_platform_browser_j, EventManagerPlugin as ɵangular_packages_platform_browser_platform_browser_g, BrowserModule, platformBrowser, Meta, Title, disableDebugTools, enableDebugTools, BrowserTransferStateModule, TransferState, makeStateKey, By, EVENT_MANAGER_PLUGINS, EventManager, HAMMER_GESTURE_CONFIG, HAMMER_LOADER, HammerGestureConfig, DomSanitizer, VERSION, ELEMENT_PROBE_PROVIDERS__POST_R3__ as ɵELEMENT_PROBE_PROVIDERS__POST_R3__, BROWSER_SANITIZATION_PROVIDERS as ɵBROWSER_SANITIZATION_PROVIDERS, INTERNAL_BROWSER_PLATFORM_PROVIDERS as ɵINTERNAL_BROWSER_PLATFORM_PROVIDERS, initDomAdapter as ɵinitDomAdapter, BrowserDomAdapter as ɵBrowserDomAdapter, BrowserPlatformLocation as ɵBrowserPlatformLocation, TRANSITION_ID as ɵTRANSITION_ID, BrowserGetTestability as ɵBrowserGetTestability, escapeHtml as ɵescapeHtml, ELEMENT_PROBE_PROVIDERS as ɵELEMENT_PROBE_PROVIDERS, DomAdapter as ɵDomAdapter, getDOM as ɵgetDOM, setRootDomAdapter as ɵsetRootDomAdapter, DomRendererFactory2 as ɵDomRendererFactory2, NAMESPACE_URIS as ɵNAMESPACE_URIS, flattenStyles as ɵflattenStyles, shimContentAttribute as ɵshimContentAttribute, shimHostAttribute as ɵshimHostAttribute, DomEventsPlugin as ɵDomEventsPlugin, HammerGesturesPlugin as ɵHammerGesturesPlugin, KeyEventsPlugin as ɵKeyEventsPlugin, DomSharedStylesHost as ɵDomSharedStylesHost, SharedStylesHost as ɵSharedStylesHost, DomSanitizerImpl as ɵDomSanitizerImpl };
//# sourceMappingURL=platform-browser.js.map
