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
import { ɵparseCookieValue as parseCookieValue } from '@angular/common';
import { ɵglobal as global } from '@angular/core';
import { setRootDomAdapter } from '../dom/dom_adapter';
import { GenericBrowserDomAdapter } from './generic_browser_adapter';
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
    if (global['Node']) {
        return global['Node'].prototype.contains || (/**
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
export class BrowserDomAdapter extends GenericBrowserDomAdapter {
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
    getCookie(name) { return parseCookieValue(document.cookie, name); }
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
export { ɵ0 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYnJvd3Nlcl9hZGFwdGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci9zcmMvYnJvd3Nlci9icm93c2VyX2FkYXB0ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsaUJBQWlCLElBQUksZ0JBQWdCLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUN0RSxPQUFPLEVBQUMsT0FBTyxJQUFJLE1BQU0sRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUVoRCxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUVyRCxPQUFPLEVBQUMsd0JBQXdCLEVBQUMsTUFBTSwyQkFBMkIsQ0FBQzs7TUFFN0QsY0FBYyxHQUFHO0lBQ3JCLE9BQU8sRUFBRSxXQUFXO0lBQ3BCLFdBQVcsRUFBRSxXQUFXO0lBQ3hCLFVBQVUsRUFBRSxVQUFVO0lBQ3RCLFVBQVUsRUFBRSxVQUFVO0NBQ3ZCOztNQUVLLHVCQUF1QixHQUFHLENBQUM7OztNQUczQixPQUFPLEdBQTBCOzs7SUFHckMsSUFBSSxFQUFFLFdBQVc7SUFDakIsSUFBSSxFQUFFLEtBQUs7SUFDWCxNQUFNLEVBQUUsUUFBUTtJQUNoQixNQUFNLEVBQUUsUUFBUTtJQUNoQixLQUFLLEVBQUUsUUFBUTtJQUNmLEtBQUssRUFBRSxRQUFRO0lBQ2YsTUFBTSxFQUFFLFdBQVc7SUFDbkIsT0FBTyxFQUFFLFlBQVk7SUFDckIsSUFBSSxFQUFFLFNBQVM7SUFDZixNQUFNLEVBQUUsV0FBVztJQUNuQixNQUFNLEVBQUUsYUFBYTtJQUNyQixRQUFRLEVBQUUsWUFBWTtJQUN0QixLQUFLLEVBQUUsSUFBSTtDQUNaOzs7OztNQUtLLG1CQUFtQixHQUFHO0lBQzFCLEdBQUcsRUFBRSxHQUFHO0lBQ1IsR0FBRyxFQUFFLEdBQUc7SUFDUixHQUFHLEVBQUUsR0FBRztJQUNSLEdBQUcsRUFB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OlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge8m1cGFyc2VDb29raWVWYWx1ZSBhcyBwYXJzZUNvb2tpZVZhbHVlfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHvJtWdsb2JhbCBhcyBnbG9iYWx9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge3NldFJvb3REb21BZGFwdGVyfSBmcm9tICcuLi9kb20vZG9tX2FkYXB0ZXInO1xuXG5pbXBvcnQge0dlbmVyaWNCcm93c2VyRG9tQWRhcHRlcn0gZnJvbSAnLi9nZW5lcmljX2Jyb3dzZXJfYWRhcHRlcic7XG5cbmNvbnN0IF9hdHRyVG9Qcm9wTWFwID0ge1xuICAnY2xhc3MnOiAnY2xhc3NOYW1lJyxcbiAgJ2lubmVySHRtbCc6ICdpbm5lckhUTUwnLFxuICAncmVhZG9ubHknOiAncmVhZE9ubHknLFxuICAndGFiaW5kZXgnOiAndGFiSW5kZXgnLFxufTtcblxuY29uc3QgRE9NX0tFWV9MT0NBVElPTl9OVU1QQUQgPSAzO1xuXG4vLyBNYXAgdG8gY29udmVydCBzb21lIGtleSBvciBrZXlJZGVudGlmaWVyIHZhbHVlcyB0byB3aGF0IHdpbGwgYmUgcmV0dXJuZWQgYnkgZ2V0RXZlbnRLZXlcbmNvbnN0IF9rZXlNYXA6IHtbazogc3RyaW5nXTogc3RyaW5nfSA9IHtcbiAgLy8gVGhlIGZvbGxvd2luZyB2YWx1ZXMgYXJlIGhlcmUgZm9yIGNyb3NzLWJyb3dzZXIgY29tcGF0aWJpbGl0eSBhbmQgdG8gbWF0Y2ggdGhlIFczQyBzdGFuZGFyZFxuICAvLyBjZiBodHRwOi8vd3d3LnczLm9yZy9UUi9ET00tTGV2ZWwtMy1FdmVudHMta2V5L1xuICAnXFxiJzogJ0JhY2tzcGFjZScsXG4gICdcXHQnOiAnVGFiJyxcbiAgJ1xceDdGJzogJ0RlbGV0ZScsXG4gICdcXHgxQic6ICdFc2NhcGUnLFxuICAnRGVsJzogJ0RlbGV0ZScsXG4gICdFc2MnOiAnRXNjYXBlJyxcbiAgJ0xlZnQnOiAnQXJyb3dMZWZ0JyxcbiAgJ1JpZ2h0JzogJ0Fycm93UmlnaHQnLFxuICAnVXAnOiAnQXJyb3dVcCcsXG4gICdEb3duJzogJ0Fycm93RG93bicsXG4gICdNZW51JzogJ0NvbnRleHRNZW51JyxcbiAgJ1Njcm9sbCc6ICdTY3JvbGxMb2NrJyxcbiAgJ1dpbic6ICdPUydcbn07XG5cbi8vIFRoZXJlIGlzIGEgYnVnIGluIENocm9tZSBmb3IgbnVtZXJpYyBrZXlwYWQga2V5czpcbi8vIGh0dHBzOi8vY29kZS5nb29nbGUuY29tL3AvY2hyb21pdW0vaXNzdWVzL2RldGFpbD9pZD0xNTU2NTRcbi8vIDEsIDIsIDMgLi4uIGFyZSByZXBvcnRlZCBhcyBBLCBCLCBDIC4uLlxuY29uc3QgX2Nocm9tZU51bUtleVBhZE1hcCA9IHtcbiAgJ0EnOiAnMScsXG4gICdCJzogJzInLFxuICAnQyc6ICczJyxcbiAgJ0QnOiAnNCcsXG4gICdFJzogJzUnLFxuICAnRic6ICc2JyxcbiAgJ0cnOiAnNycsXG4gICdIJzogJzgnLFxuICAnSSc6ICc5JyxcbiAgJ0onOiAnKicsXG4gICdLJzogJysnLFxuICAnTSc6ICctJyxcbiAgJ04nOiAnLicsXG4gICdPJzogJy8nLFxuICAnXFx4NjAnOiAnMCcsXG4gICdcXHg5MCc6ICdOdW1Mb2NrJ1xufTtcblxuY29uc3Qgbm9kZUNvbnRhaW5zOiAodGhpczogTm9kZSwgb3RoZXI6IE5vZGUpID0+IGJvb2xlYW4gPSAoKCkgPT4ge1xuICBpZiAoZ2xvYmFsWydOb2RlJ10pIHtcbiAgICByZXR1cm4gZ2xvYmFsWydOb2RlJ10ucHJvdG90eXBlLmNvbnRhaW5zIHx8IGZ1bmN0aW9uKHRoaXM6IE5vZGUsIG5vZGU6IGFueSkge1xuICAgICAgcmV0dXJuICEhKHRoaXMuY29tcGFyZURvY3VtZW50UG9zaXRpb24obm9kZSkgJiAxNik7XG4gICAgfTtcbiAgfVxuXG4gIHJldHVybiB1bmRlZmluZWQgYXMgYW55O1xufSkoKTtcblxuLyoqXG4gKiBBIGBEb21BZGFwdGVyYCBwb3dlcmVkIGJ5IGZ1bGwgYnJvd3NlciBET00gQVBJcy5cbiAqXG4gKiBAc2VjdXJpdHkgVHJlYWQgY2FyZWZ1bGx5ISBJbnRlcmFjdGluZyB3aXRoIHRoZSBET00gZGlyZWN0bHkgaXMgZGFuZ2Vyb3VzIGFuZFxuICogY2FuIGludHJvZHVjZSBYU1Mgcmlza3MuXG4gKi9cbi8qIHRzbGludDpkaXNhYmxlOnJlcXVpcmVQYXJhbWV0ZXJUeXBlIG5vLWNvbnNvbGUgKi9cbmV4cG9ydCBjbGFzcyBCcm93c2VyRG9tQWRhcHRlciBleHRlbmRzIEdlbmVyaWNCcm93c2VyRG9tQWRhcHRlciB7XG4gIHBhcnNlKHRlbXBsYXRlSHRtbDogc3RyaW5nKSB7IHRocm93IG5ldyBFcnJvcigncGFyc2Ugbm90IGltcGxlbWVudGVkJyk7IH1cbiAgc3RhdGljIG1ha2VDdXJyZW50KCkgeyBzZXRSb290RG9tQWRhcHRlcihuZXcgQnJvd3NlckRvbUFkYXB0ZXIoKSk7IH1cbiAgaGFzUHJvcGVydHkoZWxlbWVudDogTm9kZSwgbmFtZTogc3RyaW5nKTogYm9vbGVhbiB7IHJldHVybiBuYW1lIGluIGVsZW1lbnQ7IH1cbiAgc2V0UHJvcGVydHkoZWw6IE5vZGUsIG5hbWU6IHN0cmluZywgdmFsdWU6IGFueSkgeyAoPGFueT5lbClbbmFtZV0gPSB2YWx1ZTsgfVxuICBnZXRQcm9wZXJ0eShlbDogTm9kZSwgbmFtZTogc3RyaW5nKTogYW55IHsgcmV0dXJuICg8YW55PmVsKVtuYW1lXTsgfVxuICBpbnZva2UoZWw6IE5vZGUsIG1ldGhvZE5hbWU6IHN0cmluZywgYXJnczogYW55W10pOiBhbnkgeyAoPGFueT5lbClbbWV0aG9kTmFtZV0oLi4uYXJncyk7IH1cblxuICAvLyBUT0RPKHRib3NjaCk6IG1vdmUgdGhpcyBpbnRvIGEgc2VwYXJhdGUgZW52aXJvbm1lbnQgY2xhc3Mgb25jZSB3ZSBoYXZlIGl0XG4gIGxvZ0Vycm9yKGVycm9yOiBzdHJpbmcpOiB2b2lkIHtcbiAgICBpZiAod2luZG93LmNvbnNvbGUpIHtcbiAgICAgIGlmIChjb25zb2xlLmVycm9yKSB7XG4gICAgICAgIGNvbnNvbGUuZXJyb3IoZXJyb3IpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS5sb2coZXJyb3IpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIGxvZyhlcnJvcjogc3RyaW5nKTogdm9pZCB7XG4gICAgaWYgKHdpbmRvdy5jb25zb2xlKSB7XG4gICAgICB3aW5kb3cuY29uc29sZS5sb2cgJiYgd2luZG93LmNvbnNvbGUubG9nKGVycm9yKTtcbiAgICB9XG4gIH1cblxuICBsb2dHcm91cChlcnJvcjogc3RyaW5nKTogdm9pZCB7XG4gICAgaWYgKHdpbmRvdy5jb25zb2xlKSB7XG4gICAgICB3aW5kb3cuY29uc29sZS5ncm91cCAmJiB3aW5kb3cuY29uc29sZS5ncm91cChlcnJvcik7XG4gICAgfVxuICB9XG5cbiAgbG9nR3JvdXBFbmQoKTogdm9pZCB7XG4gICAgaWYgKHdpbmRvdy5jb25zb2xlKSB7XG4gICAgICB3aW5kb3cuY29uc29sZS5ncm91cEVuZCAmJiB3aW5kb3cuY29uc29sZS5ncm91cEVuZCgpO1xuICAgIH1cbiAgfVxuXG4gIGdldCBhdHRyVG9Qcm9wTWFwKCk6IGFueSB7IHJldHVybiBfYXR0clRvUHJvcE1hcDsgfVxuXG4gIGNvbnRhaW5zKG5vZGVBOiBhbnksIG5vZGVCOiBhbnkpOiBib29sZWFuIHsgcmV0dXJuIG5vZGVDb250YWlucy5jYWxsKG5vZGVBLCBub2RlQik7IH1cbiAgcXVlcnlTZWxlY3RvcihlbDogSFRNTEVsZW1lbnQsIHNlbGVjdG9yOiBzdHJpbmcpOiBhbnkgeyByZXR1cm4gZWwucXVlcnlTZWxlY3RvcihzZWxlY3Rvcik7IH1cbiAgcXVlcnlTZWxlY3RvckFsbChlbDogYW55LCBzZWxlY3Rvcjogc3RyaW5nKTogYW55W10geyByZXR1cm4gZWwucXVlcnlTZWxlY3RvckFsbChzZWxlY3Rvcik7IH1cbiAgb24oZWw6IE5vZGUsIGV2dDogYW55LCBsaXN0ZW5lcjogYW55KSB7IGVsLmFkZEV2ZW50TGlzdGVuZXIoZXZ0LCBsaXN0ZW5lciwgZmFsc2UpOyB9XG4gIG9uQW5kQ2FuY2VsKGVsOiBOb2RlLCBldnQ6IGFueSwgbGlzdGVuZXI6IGFueSk6IEZ1bmN0aW9uIHtcbiAgICBlbC5hZGRFdmVudExpc3RlbmVyKGV2dCwgbGlzdGVuZXIsIGZhbHNlKTtcbiAgICAvLyBOZWVkZWQgdG8gZm9sbG93IERhcnQncyBzdWJzY3JpcHRpb24gc2VtYW50aWMsIHVudGlsIGZpeCBvZlxuICAgIC8vIGh0dHBzOi8vY29kZS5nb29nbGUuY29tL3AvZGFydC9pc3N1ZXMvZGV0YWlsP2lkPTE3NDA2XG4gICAgcmV0dXJuICgpID0+IHsgZWwucmVtb3ZlRXZlbnRMaXN0ZW5lcihldnQsIGxpc3RlbmVyLCBmYWxzZSk7IH07XG4gIH1cbiAgZGlzcGF0Y2hFdmVudChlbDogTm9kZSwgZXZ0OiBhbnkpIHsgZWwuZGlzcGF0Y2hFdmVudChldnQpOyB9XG4gIGNyZWF0ZU1vdXNlRXZlbnQoZXZlbnRUeXBlOiBzdHJpbmcpOiBNb3VzZUV2ZW50IHtcbiAgICBjb25zdCBldnQ6IE1vdXNlRXZlbnQgPSB0aGlzLmdldERlZmF1bHREb2N1bWVudCgpLmNyZWF0ZUV2ZW50KCdNb3VzZUV2ZW50Jyk7XG4gICAgZXZ0LmluaXRFdmVudChldmVudFR5cGUsIHRydWUsIHRydWUpO1xuICAgIHJldHVybiBldnQ7XG4gIH1cbiAgY3JlYXRlRXZlbnQoZXZlbnRUeXBlOiBhbnkpOiBFdmVudCB7XG4gICAgY29uc3QgZXZ0OiBFdmVudCA9IHRoaXMuZ2V0RGVmYXVsdERvY3VtZW50KCkuY3JlYXRlRXZlbnQoJ0V2ZW50Jyk7XG4gICAgZXZ0LmluaXRFdmVudChldmVudFR5cGUsIHRydWUsIHRydWUpO1xuICAgIHJldHVybiBldnQ7XG4gIH1cbiAgcHJldmVudERlZmF1bHQoZXZ0OiBFdmVudCkge1xuICAgIGV2dC5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIGV2dC5yZXR1cm5WYWx1ZSA9IGZhbHNlO1xuICB9XG4gIGlzUHJldmVudGVkKGV2dDogRXZlbnQpOiBib29sZWFuIHtcbiAgICByZXR1cm4gZXZ0LmRlZmF1bHRQcmV2ZW50ZWQgfHwgZXZ0LnJldHVyblZhbHVlICE9IG51bGwgJiYgIWV2dC5yZXR1cm5WYWx1ZTtcbiAgfVxuICBnZXRJbm5lckhUTUwoZWw6IEhUTUxFbGVtZW50KTogc3RyaW5nIHsgcmV0dXJuIGVsLmlubmVySFRNTDsgfVxuICBnZXRUZW1wbGF0ZUNvbnRlbnQoZWw6IE5vZGUpOiBOb2RlfG51bGwge1xuICAgIHJldHVybiAnY29udGVudCcgaW4gZWwgJiYgdGhpcy5pc1RlbXBsYXRlRWxlbWVudChlbCkgPyAoPGFueT5lbCkuY29udGVudCA6IG51bGw7XG4gIH1cbiAgZ2V0T3V0ZXJIVE1MKGVsOiBIVE1MRWxlbWVudCk6IHN0cmluZyB7IHJldHVybiBlbC5vdXRlckhUTUw7IH1cbiAgbm9kZU5hbWUobm9kZTogTm9kZSk6IHN0cmluZyB7IHJldHVybiBub2RlLm5vZGVOYW1lOyB9XG4gIG5vZGVWYWx1ZShub2RlOiBOb2RlKTogc3RyaW5nfG51bGwgeyByZXR1cm4gbm9kZS5ub2RlVmFsdWU7IH1cbiAgdHlwZShub2RlOiBIVE1MSW5wdXRFbGVtZW50KTogc3RyaW5nIHsgcmV0dXJuIG5vZGUudHlwZTsgfVxuICBjb250ZW50KG5vZGU6IE5vZGUpOiBOb2RlIHtcbiAgICBpZiAodGhpcy5oYXNQcm9wZXJ0eShub2RlLCAnY29udGVudCcpKSB7XG4gICAgICByZXR1cm4gKDxhbnk+bm9kZSkuY29udGVudDtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIG5vZGU7XG4gICAgfVxuICB9XG4gIGZpcnN0Q2hpbGQoZWw6IE5vZGUpOiBOb2RlfG51bGwgeyByZXR1cm4gZWwuZmlyc3RDaGlsZDsgfVxuICBuZXh0U2libGluZyhlbDogTm9kZSk6IE5vZGV8bnVsbCB7IHJldHVybiBlbC5uZXh0U2libGluZzsgfVxuICBwYXJlbnRFbGVtZW50KGVsOiBOb2RlKTogTm9kZXxudWxsIHsgcmV0dXJuIGVsLnBhcmVudE5vZGU7IH1cbiAgY2hpbGROb2RlcyhlbDogYW55KTogTm9kZVtdIHsgcmV0dXJuIGVsLmNoaWxkTm9kZXM7IH1cbiAgY2hpbGROb2Rlc0FzTGlzdChlbDogTm9kZSk6IGFueVtdIHtcbiAgICBjb25zdCBjaGlsZE5vZGVzID0gZWwuY2hpbGROb2RlcztcbiAgICBjb25zdCByZXMgPSBuZXcgQXJyYXkoY2hpbGROb2Rlcy5sZW5ndGgpO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY2hpbGROb2Rlcy5sZW5ndGg7IGkrKykge1xuICAgICAgcmVzW2ldID0gY2hpbGROb2Rlc1tpXTtcbiAgICB9XG4gICAgcmV0dXJuIHJlcztcbiAgfVxuICBjbGVhck5vZGVzKGVsOiBOb2RlKSB7XG4gICAgd2hpbGUgKGVsLmZpcnN0Q2hpbGQpIHtcbiAgICAgIGVsLnJlbW92ZUNoaWxkKGVsLmZpcnN0Q2hpbGQpO1xuICAgIH1cbiAgfVxuICBhcHBlbmRDaGlsZChlbDogTm9kZSwgbm9kZTogTm9kZSkgeyBlbC5hcHBlbmRDaGlsZChub2RlKTsgfVxuICByZW1vdmVDaGlsZChlbDogTm9kZSwgbm9kZTogTm9kZSkgeyBlbC5yZW1vdmVDaGlsZChub2RlKTsgfVxuICByZXBsYWNlQ2hpbGQoZWw6IE5vZGUsIG5ld0NoaWxkOiBOb2RlLCBvbGRDaGlsZDogTm9kZSkgeyBlbC5yZXBsYWNlQ2hpbGQobmV3Q2hpbGQsIG9sZENoaWxkKTsgfVxuICByZW1vdmUobm9kZTogTm9kZSk6IE5vZGUge1xuICAgIGlmIChub2RlLnBhcmVudE5vZGUpIHtcbiAgICAgIG5vZGUucGFyZW50Tm9kZS5yZW1vdmVDaGlsZChub2RlKTtcbiAgICB9XG4gICAgcmV0dXJuIG5vZGU7XG4gIH1cbiAgaW5zZXJ0QmVmb3JlKHBhcmVudDogTm9kZSwgcmVmOiBOb2RlLCBub2RlOiBOb2RlKSB7IHBhcmVudC5pbnNlcnRCZWZvcmUobm9kZSwgcmVmKTsgfVxuICBpbnNlcnRBbGxCZWZvcmUocGFyZW50OiBOb2RlLCByZWY6IE5vZGUsIG5vZGVzOiBOb2RlW10pIHtcbiAgICBub2Rlcy5mb3JFYWNoKChuOiBhbnkpID0+IHBhcmVudC5pbnNlcnRCZWZvcmUobiwgcmVmKSk7XG4gIH1cbiAgaW5zZXJ0QWZ0ZXIocGFyZW50OiBOb2RlLCByZWY6IE5vZGUsIG5vZGU6IGFueSkgeyBwYXJlbnQuaW5zZXJ0QmVmb3JlKG5vZGUsIHJlZi5uZXh0U2libGluZyk7IH1cbiAgc2V0SW5uZXJIVE1MKGVsOiBFbGVtZW50LCB2YWx1ZTogc3RyaW5nKSB7IGVsLmlubmVySFRNTCA9IHZhbHVlOyB9XG4gIGdldFRleHQoZWw6IE5vZGUpOiBzdHJpbmd8bnVsbCB7IHJldHVybiBlbC50ZXh0Q29udGVudDsgfVxuICBzZXRUZXh0KGVsOiBOb2RlLCB2YWx1ZTogc3RyaW5nKSB7IGVsLnRleHRDb250ZW50ID0gdmFsdWU7IH1cbiAgZ2V0VmFsdWUoZWw6IGFueSk6IHN0cmluZyB7IHJldHVybiBlbC52YWx1ZTsgfVxuICBzZXRWYWx1ZShlbDogYW55LCB2YWx1ZTogc3RyaW5nKSB7IGVsLnZhbHVlID0gdmFsdWU7IH1cbiAgZ2V0Q2hlY2tlZChlbDogYW55KTogYm9vbGVhbiB7IHJldHVybiBlbC5jaGVja2VkOyB9XG4gIHNldENoZWNrZWQoZWw6IGFueSwgdmFsdWU6IGJvb2xlYW4pIHsgZWwuY2hlY2tlZCA9IHZhbHVlOyB9XG4gIGNyZWF0ZUNvbW1lbnQodGV4dDogc3RyaW5nKTogQ29tbWVudCB7IHJldHVybiB0aGlzLmdldERlZmF1bHREb2N1bWVudCgpLmNyZWF0ZUNvbW1lbnQodGV4dCk7IH1cbiAgY3JlYXRlVGVtcGxhdGUoaHRtbDogYW55KTogSFRNTEVsZW1lbnQge1xuICAgIGNvbnN0IHQgPSB0aGlzLmdldERlZmF1bHREb2N1bWVudCgpLmNyZWF0ZUVsZW1lbnQoJ3RlbXBsYXRlJyk7XG4gICAgdC5pbm5lckhUTUwgPSBodG1sO1xuICAgIHJldHVybiB0O1xuICB9XG4gIGNyZWF0ZUVsZW1lbnQodGFnTmFtZTogc3RyaW5nLCBkb2M/OiBEb2N1bWVudCk6IEhUTUxFbGVtZW50IHtcbiAgICBkb2MgPSBkb2MgfHwgdGhpcy5nZXREZWZhdWx0RG9jdW1lbnQoKTtcbiAgICByZXR1cm4gZG9jLmNyZWF0ZUVsZW1lbnQodGFnTmFtZSk7XG4gIH1cbiAgY3JlYXRlRWxlbWVudE5TKG5zOiBzdHJpbmcsIHRhZ05hbWU6IHN0cmluZywgZG9jPzogRG9jdW1lbnQpOiBFbGVtZW50IHtcbiAgICBkb2MgPSBkb2MgfHwgdGhpcy5nZXREZWZhdWx0RG9jdW1lbnQoKTtcbiAgICByZXR1cm4gZG9jLmNyZWF0ZUVsZW1lbnROUyhucywgdGFnTmFtZSk7XG4gIH1cbiAgY3JlYXRlVGV4dE5vZGUodGV4dDogc3RyaW5nLCBkb2M/OiBEb2N1bWVudCk6IFRleHQge1xuICAgIGRvYyA9IGRvYyB8fCB0aGlzLmdldERlZmF1bHREb2N1bWVudCgpO1xuICAgIHJldHVybiBkb2MuY3JlYXRlVGV4dE5vZGUodGV4dCk7XG4gIH1cbiAgY3JlYXRlU2NyaXB0VGFnKGF0dHJOYW1lOiBzdHJpbmcsIGF0dHJWYWx1ZTogc3RyaW5nLCBkb2M/OiBEb2N1bWVudCk6IEhUTUxTY3JpcHRFbGVtZW50IHtcbiAgICBkb2MgPSBkb2MgfHwgdGhpcy5nZXREZWZhdWx0RG9jdW1lbnQoKTtcbiAgICBjb25zdCBlbCA9IDxIVE1MU2NyaXB0RWxlbWVudD5kb2MuY3JlYXRlRWxlbWVudCgnU0NSSVBUJyk7XG4gICAgZWwuc2V0QXR0cmlidXRlKGF0dHJOYW1lLCBhdHRyVmFsdWUpO1xuICAgIHJldHVybiBlbDtcbiAgfVxuICBjcmVhdGVTdHlsZUVsZW1lbnQoY3NzOiBzdHJpbmcsIGRvYz86IERvY3VtZW50KTogSFRNTFN0eWxlRWxlbWVudCB7XG4gICAgZG9jID0gZG9jIHx8IHRoaXMuZ2V0RGVmYXVsdERvY3VtZW50KCk7XG4gICAgY29uc3Qgc3R5bGUgPSA8SFRNTFN0eWxlRWxlbWVudD5kb2MuY3JlYXRlRWxlbWVudCgnc3R5bGUnKTtcbiAgICB0aGlzLmFwcGVuZENoaWxkKHN0eWxlLCB0aGlzLmNyZWF0ZVRleHROb2RlKGNzcywgZG9jKSk7XG4gICAgcmV0dXJuIHN0eWxlO1xuICB9XG4gIGNyZWF0ZVNoYWRvd1Jvb3QoZWw6IEhUTUxFbGVtZW50KTogRG9jdW1lbnRGcmFnbWVudCB7IHJldHVybiAoPGFueT5lbCkuY3JlYXRlU2hhZG93Um9vdCgpOyB9XG4gIGdldFNoYWRvd1Jvb3QoZWw6IEhUTUxFbGVtZW50KTogRG9jdW1lbnRGcmFnbWVudCB7IHJldHVybiAoPGFueT5lbCkuc2hhZG93Um9vdDsgfVxuICBnZXRIb3N0KGVsOiBIVE1MRWxlbWVudCk6IEhUTUxFbGVtZW50IHsgcmV0dXJuICg8YW55PmVsKS5ob3N0OyB9XG4gIGNsb25lKG5vZGU6IE5vZGUpOiBOb2RlIHsgcmV0dXJuIG5vZGUuY2xvbmVOb2RlKHRydWUpOyB9XG4gIGdldEVsZW1lbnRzQnlDbGFzc05hbWUoZWxlbWVudDogYW55LCBuYW1lOiBzdHJpbmcpOiBIVE1MRWxlbWVudFtdIHtcbiAgICByZXR1cm4gZWxlbWVudC5nZXRFbGVtZW50c0J5Q2xhc3NOYW1lKG5hbWUpO1xuICB9XG4gIGdldEVsZW1lbnRzQnlUYWdOYW1lKGVsZW1lbnQ6IGFueSwgbmFtZTogc3RyaW5nKTogSFRNTEVsZW1lbnRbXSB7XG4gICAgcmV0dXJuIGVsZW1lbnQuZ2V0RWxlbWVudHNCeVRhZ05hbWUobmFtZSk7XG4gIH1cbiAgY2xhc3NMaXN0KGVsZW1lbnQ6IGFueSk6IGFueVtdIHsgcmV0dXJuIEFycmF5LnByb3RvdHlwZS5zbGljZS5jYWxsKGVsZW1lbnQuY2xhc3NMaXN0LCAwKTsgfVxuICBhZGRDbGFzcyhlbGVtZW50OiBhbnksIGNsYXNzTmFtZTogc3RyaW5nKSB7IGVsZW1lbnQuY2xhc3NMaXN0LmFkZChjbGFzc05hbWUpOyB9XG4gIHJlbW92ZUNsYXNzKGVsZW1lbnQ6IGFueSwgY2xhc3NOYW1lOiBzdHJpbmcpIHsgZWxlbWVudC5jbGFzc0xpc3QucmVtb3ZlKGNsYXNzTmFtZSk7IH1cbiAgaGFzQ2xhc3MoZWxlbWVudDogYW55LCBjbGFzc05hbWU6IHN0cmluZyk6IGJvb2xlYW4ge1xuICAgIHJldHVybiBlbGVtZW50LmNsYXNzTGlzdC5jb250YWlucyhjbGFzc05hbWUpO1xuICB9XG4gIHNldFN0eWxlKGVsZW1lbnQ6IGFueSwgc3R5bGVOYW1lOiBzdHJpbmcsIHN0eWxlVmFsdWU6IHN0cmluZykge1xuICAgIGVsZW1lbnQuc3R5bGVbc3R5bGVOYW1lXSA9IHN0eWxlVmFsdWU7XG4gIH1cbiAgcmVtb3ZlU3R5bGUoZWxlbWVudDogYW55LCBzdHlsZW5hbWU6IHN0cmluZykge1xuICAgIC8vIElFIHJlcXVpcmVzICcnIGluc3RlYWQgb2YgbnVsbFxuICAgIC8vIHNlZSBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyL2lzc3Vlcy83OTE2XG4gICAgZWxlbWVudC5zdHlsZVtzdHlsZW5hbWVdID0gJyc7XG4gIH1cbiAgZ2V0U3R5bGUoZWxlbWVudDogYW55LCBzdHlsZW5hbWU6IHN0cmluZyk6IHN0cmluZyB7IHJldHVybiBlbGVtZW50LnN0eWxlW3N0eWxlbmFtZV07IH1cbiAgaGFzU3R5bGUoZWxlbWVudDogYW55LCBzdHlsZU5hbWU6IHN0cmluZywgc3R5bGVWYWx1ZT86IHN0cmluZ3xudWxsKTogYm9vbGVhbiB7XG4gICAgY29uc3QgdmFsdWUgPSB0aGlzLmdldFN0eWxlKGVsZW1lbnQsIHN0eWxlTmFtZSkgfHwgJyc7XG4gICAgcmV0dXJuIHN0eWxlVmFsdWUgPyB2YWx1ZSA9PSBzdHlsZVZhbHVlIDogdmFsdWUubGVuZ3RoID4gMDtcbiAgfVxuICB0YWdOYW1lKGVsZW1lbnQ6IGFueSk6IHN0cmluZyB7IHJldHVybiBlbGVtZW50LnRhZ05hbWU7IH1cbiAgYXR0cmlidXRlTWFwKGVsZW1lbnQ6IGFueSk6IE1hcDxzdHJpbmcsIHN0cmluZz4ge1xuICAgIGNvbnN0IHJlcyA9IG5ldyBNYXA8c3RyaW5nLCBzdHJpbmc+KCk7XG4gICAgY29uc3QgZWxBdHRycyA9IGVsZW1lbnQuYXR0cmlidXRlcztcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGVsQXR0cnMubGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IGF0dHJpYiA9IGVsQXR0cnMuaXRlbShpKTtcbiAgICAgIHJlcy5zZXQoYXR0cmliLm5hbWUsIGF0dHJpYi52YWx1ZSk7XG4gICAgfVxuICAgIHJldHVybiByZXM7XG4gIH1cbiAgaGFzQXR0cmlidXRlKGVsZW1lbnQ6IEVsZW1lbnQsIGF0dHJpYnV0ZTogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIGVsZW1lbnQuaGFzQXR0cmlidXRlKGF0dHJpYnV0ZSk7XG4gIH1cbiAgaGFzQXR0cmlidXRlTlMoZWxlbWVudDogRWxlbWVudCwgbnM6IHN0cmluZywgYXR0cmlidXRlOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICByZXR1cm4gZWxlbWVudC5oYXNBdHRyaWJ1dGVOUyhucywgYXR0cmlidXRlKTtcbiAgfVxuICBnZXRBdHRyaWJ1dGUoZWxlbWVudDogRWxlbWVudCwgYXR0cmlidXRlOiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgcmV0dXJuIGVsZW1lbnQuZ2V0QXR0cmlidXRlKGF0dHJpYnV0ZSk7XG4gIH1cbiAgZ2V0QXR0cmlidXRlTlMoZWxlbWVudDogRWxlbWVudCwgbnM6IHN0cmluZywgbmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIHJldHVybiBlbGVtZW50LmdldEF0dHJpYnV0ZU5TKG5zLCBuYW1lKTtcbiAgfVxuICBzZXRBdHRyaWJ1dGUoZWxlbWVudDogRWxlbWVudCwgbmFtZTogc3RyaW5nLCB2YWx1ZTogc3RyaW5nKSB7IGVsZW1lbnQuc2V0QXR0cmlidXRlKG5hbWUsIHZhbHVlKTsgfVxuICBzZXRBdHRyaWJ1dGVOUyhlbGVtZW50OiBFbGVtZW50LCBuczogc3RyaW5nLCBuYW1lOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcpIHtcbiAgICBlbGVtZW50LnNldEF0dHJpYnV0ZU5TKG5zLCBuYW1lLCB2YWx1ZSk7XG4gIH1cbiAgcmVtb3ZlQXR0cmlidXRlKGVsZW1lbnQ6IEVsZW1lbnQsIGF0dHJpYnV0ZTogc3RyaW5nKSB7IGVsZW1lbnQucmVtb3ZlQXR0cmlidXRlKGF0dHJpYnV0ZSk7IH1cbiAgcmVtb3ZlQXR0cmlidXRlTlMoZWxlbWVudDogRWxlbWVudCwgbnM6IHN0cmluZywgbmFtZTogc3RyaW5nKSB7XG4gICAgZWxlbWVudC5yZW1vdmVBdHRyaWJ1dGVOUyhucywgbmFtZSk7XG4gIH1cbiAgdGVtcGxhdGVBd2FyZVJvb3QoZWw6IE5vZGUpOiBhbnkgeyByZXR1cm4gdGhpcy5pc1RlbXBsYXRlRWxlbWVudChlbCkgPyB0aGlzLmNvbnRlbnQoZWwpIDogZWw7IH1cbiAgY3JlYXRlSHRtbERvY3VtZW50KCk6IEhUTUxEb2N1bWVudCB7XG4gICAgcmV0dXJuIGRvY3VtZW50LmltcGxlbWVudGF0aW9uLmNyZWF0ZUhUTUxEb2N1bWVudCgnZmFrZVRpdGxlJyk7XG4gIH1cbiAgZ2V0RGVmYXVsdERvY3VtZW50KCk6IERvY3VtZW50IHsgcmV0dXJuIGRvY3VtZW50OyB9XG4gIGdldEJvdW5kaW5nQ2xpZW50UmVjdChlbDogRWxlbWVudCk6IGFueSB7XG4gICAgdHJ5IHtcbiAgICAgIHJldHVybiBlbC5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKTtcbiAgICB9IGNhdGNoIHtcbiAgICAgIHJldHVybiB7dG9wOiAwLCBib3R0b206IDAsIGxlZnQ6IDAsIHJpZ2h0OiAwLCB3aWR0aDogMCwgaGVpZ2h0OiAwfTtcbiAgICB9XG4gIH1cbiAgZ2V0VGl0bGUoZG9jOiBEb2N1bWVudCk6IHN0cmluZyB7IHJldHVybiBkb2MudGl0bGU7IH1cbiAgc2V0VGl0bGUoZG9jOiBEb2N1bWVudCwgbmV3VGl0bGU6IHN0cmluZykgeyBkb2MudGl0bGUgPSBuZXdUaXRsZSB8fCAnJzsgfVxuICBlbGVtZW50TWF0Y2hlcyhuOiBhbnksIHNlbGVjdG9yOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICBpZiAodGhpcy5pc0VsZW1lbnROb2RlKG4pKSB7XG4gICAgICByZXR1cm4gbi5tYXRjaGVzICYmIG4ubWF0Y2hlcyhzZWxlY3RvcikgfHxcbiAgICAgICAgICBuLm1zTWF0Y2hlc1NlbGVjdG9yICYmIG4ubXNNYXRjaGVzU2VsZWN0b3Ioc2VsZWN0b3IpIHx8XG4gICAgICAgICAgbi53ZWJraXRNYXRjaGVzU2VsZWN0b3IgJiYgbi53ZWJraXRNYXRjaGVzU2VsZWN0b3Ioc2VsZWN0b3IpO1xuICAgIH1cblxuICAgIHJldHVybiBmYWxzZTtcbiAgfVxuICBpc1RlbXBsYXRlRWxlbWVudChlbDogTm9kZSk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLmlzRWxlbWVudE5vZGUoZWwpICYmIGVsLm5vZGVOYW1lID09PSAnVEVNUExBVEUnO1xuICB9XG4gIGlzVGV4dE5vZGUobm9kZTogTm9kZSk6IGJvb2xlYW4geyByZXR1cm4gbm9kZS5ub2RlVHlwZSA9PT0gTm9kZS5URVhUX05PREU7IH1cbiAgaXNDb21tZW50Tm9kZShub2RlOiBOb2RlKTogYm9vbGVhbiB7IHJldHVybiBub2RlLm5vZGVUeXBlID09PSBOb2RlLkNPTU1FTlRfTk9ERTsgfVxuICBpc0VsZW1lbnROb2RlKG5vZGU6IE5vZGUpOiBib29sZWFuIHsgcmV0dXJuIG5vZGUubm9kZVR5cGUgPT09IE5vZGUuRUxFTUVOVF9OT0RFOyB9XG4gIGhhc1NoYWRvd1Jvb3Qobm9kZTogYW55KTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIG5vZGUuc2hhZG93Um9vdCAhPSBudWxsICYmIG5vZGUgaW5zdGFuY2VvZiBIVE1MRWxlbWVudDtcbiAgfVxuICBpc1NoYWRvd1Jvb3Qobm9kZTogYW55KTogYm9vbGVhbiB7IHJldHVybiBub2RlIGluc3RhbmNlb2YgRG9jdW1lbnRGcmFnbWVudDsgfVxuICBpbXBvcnRJbnRvRG9jKG5vZGU6IE5vZGUpOiBhbnkgeyByZXR1cm4gZG9jdW1lbnQuaW1wb3J0Tm9kZSh0aGlzLnRlbXBsYXRlQXdhcmVSb290KG5vZGUpLCB0cnVlKTsgfVxuICBhZG9wdE5vZGUobm9kZTogTm9kZSk6IGFueSB7IHJldHVybiBkb2N1bWVudC5hZG9wdE5vZGUobm9kZSk7IH1cbiAgZ2V0SHJlZihlbDogRWxlbWVudCk6IHN0cmluZyB7IHJldHVybiBlbC5nZXRBdHRyaWJ1dGUoJ2hyZWYnKSAhOyB9XG5cbiAgZ2V0RXZlbnRLZXkoZXZlbnQ6IGFueSk6IHN0cmluZyB7XG4gICAgbGV0IGtleSA9IGV2ZW50LmtleTtcbiAgICBpZiAoa2V5ID09IG51bGwpIHtcbiAgICAgIGtleSA9IGV2ZW50LmtleUlkZW50aWZpZXI7XG4gICAgICAvLyBrZXlJZGVudGlmaWVyIGlzIGRlZmluZWQgaW4gdGhlIG9sZCBkcmFmdCBvZiBET00gTGV2ZWwgMyBFdmVudHMgaW1wbGVtZW50ZWQgYnkgQ2hyb21lIGFuZFxuICAgICAgLy8gU2FmYXJpIGNmXG4gICAgICAvLyBodHRwOi8vd3d3LnczLm9yZy9UUi8yMDA3L1dELURPTS1MZXZlbC0zLUV2ZW50cy0yMDA3MTIyMS9ldmVudHMuaHRtbCNFdmVudHMtS2V5Ym9hcmRFdmVudHMtSW50ZXJmYWNlc1xuICAgICAgaWYgKGtleSA9PSBudWxsKSB7XG4gICAgICAgIHJldHVybiAnVW5pZGVudGlmaWVkJztcbiAgICAgIH1cbiAgICAgIGlmIChrZXkuc3RhcnRzV2l0aCgnVSsnKSkge1xuICAgICAgICBrZXkgPSBTdHJpbmcuZnJvbUNoYXJDb2RlKHBhcnNlSW50KGtleS5zdWJzdHJpbmcoMiksIDE2KSk7XG4gICAgICAgIGlmIChldmVudC5sb2NhdGlvbiA9PT0gRE9NX0tFWV9MT0NBVElPTl9OVU1QQUQgJiYgX2Nocm9tZU51bUtleVBhZE1hcC5oYXNPd25Qcm9wZXJ0eShrZXkpKSB7XG4gICAgICAgICAgLy8gVGhlcmUgaXMgYSBidWcgaW4gQ2hyb21lIGZvciBudW1lcmljIGtleXBhZCBrZXlzOlxuICAgICAgICAgIC8vIGh0dHBzOi8vY29kZS5nb29nbGUuY29tL3AvY2hyb21pdW0vaXNzdWVzL2RldGFpbD9pZD0xNTU2NTRcbiAgICAgICAgICAvLyAxLCAyLCAzIC4uLiBhcmUgcmVwb3J0ZWQgYXMgQSwgQiwgQyAuLi5cbiAgICAgICAgICBrZXkgPSAoX2Nocm9tZU51bUtleVBhZE1hcCBhcyBhbnkpW2tleV07XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gX2tleU1hcFtrZXldIHx8IGtleTtcbiAgfVxuICBnZXRHbG9iYWxFdmVudFRhcmdldChkb2M6IERvY3VtZW50LCB0YXJnZXQ6IHN0cmluZyk6IEV2ZW50VGFyZ2V0fG51bGwge1xuICAgIGlmICh0YXJnZXQgPT09ICd3aW5kb3cnKSB7XG4gICAgICByZXR1cm4gd2luZG93O1xuICAgIH1cbiAgICBpZiAodGFyZ2V0ID09PSAnZG9jdW1lbnQnKSB7XG4gICAgICByZXR1cm4gZG9jO1xuICAgIH1cbiAgICBpZiAodGFyZ2V0ID09PSAnYm9keScpIHtcbiAgICAgIHJldHVybiBkb2MuYm9keTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgZ2V0SGlzdG9yeSgpOiBIaXN0b3J5IHsgcmV0dXJuIHdpbmRvdy5oaXN0b3J5OyB9XG4gIGdldExvY2F0aW9uKCk6IExvY2F0aW9uIHsgcmV0dXJuIHdpbmRvdy5sb2NhdGlvbjsgfVxuICBnZXRCYXNlSHJlZihkb2M6IERvY3VtZW50KTogc3RyaW5nfG51bGwge1xuICAgIGNvbnN0IGhyZWYgPSBnZXRCYXNlRWxlbWVudEhyZWYoKTtcbiAgICByZXR1cm4gaHJlZiA9PSBudWxsID8gbnVsbCA6IHJlbGF0aXZlUGF0aChocmVmKTtcbiAgfVxuICByZXNldEJhc2VFbGVtZW50KCk6IHZvaWQgeyBiYXNlRWxlbWVudCA9IG51bGw7IH1cbiAgZ2V0VXNlckFnZW50KCk6IHN0cmluZyB7IHJldHVybiB3aW5kb3cubmF2aWdhdG9yLnVzZXJBZ2VudDsgfVxuICBzZXREYXRhKGVsZW1lbnQ6IEVsZW1lbnQsIG5hbWU6IHN0cmluZywgdmFsdWU6IHN0cmluZykge1xuICAgIHRoaXMuc2V0QXR0cmlidXRlKGVsZW1lbnQsICdkYXRhLScgKyBuYW1lLCB2YWx1ZSk7XG4gIH1cbiAgZ2V0RGF0YShlbGVtZW50OiBFbGVtZW50LCBuYW1lOiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgcmV0dXJuIHRoaXMuZ2V0QXR0cmlidXRlKGVsZW1lbnQsICdkYXRhLScgKyBuYW1lKTtcbiAgfVxuICBnZXRDb21wdXRlZFN0eWxlKGVsZW1lbnQ6IGFueSk6IGFueSB7IHJldHVybiBnZXRDb21wdXRlZFN0eWxlKGVsZW1lbnQpOyB9XG4gIC8vIFRPRE8odGJvc2NoKTogbW92ZSB0aGlzIGludG8gYSBzZXBhcmF0ZSBlbnZpcm9ubWVudCBjbGFzcyBvbmNlIHdlIGhhdmUgaXRcbiAgc3VwcG9ydHNXZWJBbmltYXRpb24oKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHR5cGVvZig8YW55PkVsZW1lbnQpLnByb3RvdHlwZVsnYW5pbWF0ZSddID09PSAnZnVuY3Rpb24nO1xuICB9XG4gIHBlcmZvcm1hbmNlTm93KCk6IG51bWJlciB7XG4gICAgLy8gcGVyZm9ybWFuY2Uubm93KCkgaXMgbm90IGF2YWlsYWJsZSBpbiBhbGwgYnJvd3NlcnMsIHNlZVxuICAgIC8vIGh0dHA6Ly9jYW5pdXNlLmNvbS8jc2VhcmNoPXBlcmZvcm1hbmNlLm5vd1xuICAgIHJldHVybiB3aW5kb3cucGVyZm9ybWFuY2UgJiYgd2luZG93LnBlcmZvcm1hbmNlLm5vdyA/IHdpbmRvdy5wZXJmb3JtYW5jZS5ub3coKSA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgbmV3IERhdGUoKS5nZXRUaW1lKCk7XG4gIH1cblxuICBzdXBwb3J0c0Nvb2tpZXMoKTogYm9vbGVhbiB7IHJldHVybiB0cnVlOyB9XG5cbiAgZ2V0Q29va2llKG5hbWU6IHN0cmluZyk6IHN0cmluZ3xudWxsIHsgcmV0dXJuIHBhcnNlQ29va2llVmFsdWUoZG9jdW1lbnQuY29va2llLCBuYW1lKTsgfVxuXG4gIHNldENvb2tpZShuYW1lOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcpIHtcbiAgICAvLyBkb2N1bWVudC5jb29raWUgaXMgbWFnaWNhbCwgYXNzaWduaW5nIGludG8gaXQgYXNzaWducy9vdmVycmlkZXMgb25lIGNvb2tpZSB2YWx1ZSwgYnV0IGRvZXNcbiAgICAvLyBub3QgY2xlYXIgb3RoZXIgY29va2llcy5cbiAgICBkb2N1bWVudC5jb29raWUgPSBlbmNvZGVVUklDb21wb25lbnQobmFtZSkgKyAnPScgKyBlbmNvZGVVUklDb21wb25lbnQodmFsdWUpO1xuICB9XG59XG5cbmxldCBiYXNlRWxlbWVudDogSFRNTEVsZW1lbnR8bnVsbCA9IG51bGw7XG5mdW5jdGlvbiBnZXRCYXNlRWxlbWVudEhyZWYoKTogc3RyaW5nfG51bGwge1xuICBpZiAoIWJhc2VFbGVtZW50KSB7XG4gICAgYmFzZUVsZW1lbnQgPSBkb2N1bWVudC5xdWVyeVNlbGVjdG9yKCdiYXNlJykgITtcbiAgICBpZiAoIWJhc2VFbGVtZW50KSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIGJhc2VFbGVtZW50LmdldEF0dHJpYnV0ZSgnaHJlZicpO1xufVxuXG4vLyBiYXNlZCBvbiB1cmxVdGlscy5qcyBpbiBBbmd1bGFySlMgMVxubGV0IHVybFBhcnNpbmdOb2RlOiBhbnk7XG5mdW5jdGlvbiByZWxhdGl2ZVBhdGgodXJsOiBhbnkpOiBzdHJpbmcge1xuICBpZiAoIXVybFBhcnNpbmdOb2RlKSB7XG4gICAgdXJsUGFyc2luZ05vZGUgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KCdhJyk7XG4gIH1cbiAgdXJsUGFyc2luZ05vZGUuc2V0QXR0cmlidXRlKCdocmVmJywgdXJsKTtcbiAgcmV0dXJuICh1cmxQYXJzaW5nTm9kZS5wYXRobmFtZS5jaGFyQXQoMCkgPT09ICcvJykgPyB1cmxQYXJzaW5nTm9kZS5wYXRobmFtZSA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgJy8nICsgdXJsUGFyc2luZ05vZGUucGF0aG5hbWU7XG59XG4iXX0=