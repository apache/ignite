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
export function getDOM() {
    return _DOM;
}
/**
 * @param {?} adapter
 * @return {?}
 */
export function setDOM(adapter) {
    _DOM = adapter;
}
/**
 * @param {?} adapter
 * @return {?}
 */
export function setRootDomAdapter(adapter) {
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
export class DomAdapter {
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG9tX2FkYXB0ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9wbGF0Zm9ybS1icm93c2VyL3NyYy9kb20vZG9tX2FkYXB0ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7O0lBVUksSUFBSSxHQUFlLG1CQUFBLElBQUksRUFBRTs7OztBQUU3QixNQUFNLFVBQVUsTUFBTTtJQUNwQixPQUFPLElBQUksQ0FBQztBQUNkLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLE1BQU0sQ0FBQyxPQUFtQjtJQUN4QyxJQUFJLEdBQUcsT0FBTyxDQUFDO0FBQ2pCLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLGlCQUFpQixDQUFDLE9BQW1CO0lBQ25ELElBQUksQ0FBQyxJQUFJLEVBQUU7UUFDVCxJQUFJLEdBQUcsT0FBTyxDQUFDO0tBQ2hCO0FBQ0gsQ0FBQzs7Ozs7Ozs7O0FBU0QsTUFBTSxPQUFnQixVQUFVO0lBQWhDO1FBQ1MsdUJBQWtCLEdBQWMsbUJBQUEsSUFBSSxFQUFFLENBQUM7SUFrSWhELENBQUM7Ozs7OztJQW5IQyxJQUFJLGFBQWEsS0FBOEIsT0FBTyxJQUFJLENBQUMsY0FBYyxDQUFDLENBQUMsQ0FBQzs7Ozs7SUFDNUUsSUFBSSxhQUFhLENBQUMsS0FBOEIsSUFBSSxJQUFJLENBQUMsY0FBYyxHQUFHLEtBQUssQ0FBQyxDQUFDLENBQUM7Q0FrSG5GOzs7SUFsSUMsd0NBQThDOzs7OztJQW1COUMsb0NBQTBDOzs7Ozs7O0lBbEIxQyxnRUFBMEQ7Ozs7Ozs7O0lBQzFELGtFQUFpRTs7Ozs7OztJQUNqRSwyREFBcUQ7Ozs7Ozs7O0lBQ3JELGtFQUFtRTs7Ozs7O0lBRW5FLHFEQUFtQzs7Ozs7O0lBQ25DLGdEQUE4Qjs7Ozs7O0lBQzlCLHFEQUFtQzs7Ozs7SUFDbkMsbURBQTRCOzs7Ozs7O0lBWTVCLDREQUFtRDs7Ozs7O0lBQ25ELHlEQUEwQzs7Ozs7OztJQUMxQyxpRUFB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Ozs7OztJQUN6QyxzREFBa0M7Ozs7O0lBQ2xDLDBEQUFzQzs7Ozs7SUFDdEMsd0RBQW9DOzs7OztJQUNwQyx5REFBc0M7Ozs7O0lBRXRDLHVEQUFvQzs7Ozs7O0lBQ3BDLHFEQUE4Qzs7Ozs7OztJQUM5Qyw0REFBcUQiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VHlwZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmxldCBfRE9NOiBEb21BZGFwdGVyID0gbnVsbCAhO1xuXG5leHBvcnQgZnVuY3Rpb24gZ2V0RE9NKCkge1xuICByZXR1cm4gX0RPTTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNldERPTShhZGFwdGVyOiBEb21BZGFwdGVyKSB7XG4gIF9ET00gPSBhZGFwdGVyO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gc2V0Um9vdERvbUFkYXB0ZXIoYWRhcHRlcjogRG9tQWRhcHRlcikge1xuICBpZiAoIV9ET00pIHtcbiAgICBfRE9NID0gYWRhcHRlcjtcbiAgfVxufVxuXG4vKiB0c2xpbnQ6ZGlzYWJsZTpyZXF1aXJlUGFyYW1ldGVyVHlwZSAqL1xuLyoqXG4gKiBQcm92aWRlcyBET00gb3BlcmF0aW9ucyBpbiBhbiBlbnZpcm9ubWVudC1hZ25vc3RpYyB3YXkuXG4gKlxuICogQHNlY3VyaXR5IFRyZWFkIGNhcmVmdWxseSEgSW50ZXJhY3Rpbmcgd2l0aCB0aGUgRE9NIGRpcmVjdGx5IGlzIGRhbmdlcm91cyBhbmRcbiAqIGNhbiBpbnRyb2R1Y2UgWFNTIHJpc2tzLlxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgRG9tQWRhcHRlciB7XG4gIHB1YmxpYyByZXNvdXJjZUxvYWRlclR5cGU6IFR5cGU8YW55PiA9IG51bGwgITtcbiAgYWJzdHJhY3QgaGFzUHJvcGVydHkoZWxlbWVudDogYW55LCBuYW1lOiBzdHJpbmcpOiBib29sZWFuO1xuICBhYnN0cmFjdCBzZXRQcm9wZXJ0eShlbDogRWxlbWVudCwgbmFtZTogc3RyaW5nLCB2YWx1ZTogYW55KTogYW55O1xuICBhYnN0cmFjdCBnZXRQcm9wZXJ0eShlbDogRWxlbWVudCwgbmFtZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCBpbnZva2UoZWw6IEVsZW1lbnQsIG1ldGhvZE5hbWU6IHN0cmluZywgYXJnczogYW55W10pOiBhbnk7XG5cbiAgYWJzdHJhY3QgbG9nRXJyb3IoZXJyb3I6IGFueSk6IGFueTtcbiAgYWJzdHJhY3QgbG9nKGVycm9yOiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IGxvZ0dyb3VwKGVycm9yOiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IGxvZ0dyb3VwRW5kKCk6IGFueTtcblxuICAvKipcbiAgICogTWFwcyBhdHRyaWJ1dGUgbmFtZXMgdG8gdGhlaXIgY29ycmVzcG9uZGluZyBwcm9wZXJ0eSBuYW1lcyBmb3IgY2FzZXNcbiAgICogd2hlcmUgYXR0cmlidXRlIG5hbWUgZG9lc24ndCBtYXRjaCBwcm9wZXJ0eSBuYW1lLlxuICAgKi9cbiAgZ2V0IGF0dHJUb1Byb3BNYXAoKToge1trZXk6IHN0cmluZ106IHN0cmluZ30geyByZXR1cm4gdGhpcy5fYXR0clRvUHJvcE1hcDsgfVxuICBzZXQgYXR0clRvUHJvcE1hcCh2YWx1ZToge1trZXk6IHN0cmluZ106IHN0cmluZ30pIHsgdGhpcy5fYXR0clRvUHJvcE1hcCA9IHZhbHVlOyB9XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIF9hdHRyVG9Qcm9wTWFwICE6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9O1xuXG4gIGFic3RyYWN0IGNvbnRhaW5zKG5vZGVBOiBhbnksIG5vZGVCOiBhbnkpOiBib29sZWFuO1xuICBhYnN0cmFjdCBwYXJzZSh0ZW1wbGF0ZUh0bWw6IHN0cmluZyk6IGFueTtcbiAgYWJzdHJhY3QgcXVlcnlTZWxlY3RvcihlbDogYW55LCBzZWxlY3Rvcjogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCBxdWVyeVNlbGVjdG9yQWxsKGVsOiBhbnksIHNlbGVjdG9yOiBzdHJpbmcpOiBhbnlbXTtcbiAgYWJzdHJhY3Qgb24oZWw6IGFueSwgZXZ0OiBhbnksIGxpc3RlbmVyOiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IG9uQW5kQ2FuY2VsKGVsOiBhbnksIGV2dDogYW55LCBsaXN0ZW5lcjogYW55KTogRnVuY3Rpb247XG4gIGFic3RyYWN0IGRpc3BhdGNoRXZlbnQoZWw6IGFueSwgZXZ0OiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IGNyZWF0ZU1vdXNlRXZlbnQoZXZlbnRUeXBlOiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IGNyZWF0ZUV2ZW50KGV2ZW50VHlwZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCBwcmV2ZW50RGVmYXVsdChldnQ6IGFueSk6IGFueTtcbiAgYWJzdHJhY3QgaXNQcmV2ZW50ZWQoZXZ0OiBhbnkpOiBib29sZWFuO1xuICBhYnN0cmFjdCBnZXRJbm5lckhUTUwoZWw6IGFueSk6IHN0cmluZztcbiAgLyoqIFJldHVybnMgY29udGVudCBpZiBlbCBpcyBhIDx0ZW1wbGF0ZT4gZWxlbWVudCwgbnVsbCBvdGhlcndpc2UuICovXG4gIGFic3RyYWN0IGdldFRlbXBsYXRlQ29udGVudChlbDogYW55KTogYW55O1xuICBhYnN0cmFjdCBnZXRPdXRlckhUTUwoZWw6IGFueSk6IHN0cmluZztcbiAgYWJzdHJhY3Qgbm9kZU5hbWUobm9kZTogYW55KTogc3RyaW5nO1xuICBhYnN0cmFjdCBub2RlVmFsdWUobm9kZTogYW55KTogc3RyaW5nfG51bGw7XG4gIGFic3RyYWN0IHR5cGUobm9kZTogYW55KTogc3RyaW5nO1xuICBhYnN0cmFjdCBjb250ZW50KG5vZGU6IGFueSk6IGFueTtcbiAgYWJzdHJhY3QgZmlyc3RDaGlsZChlbDogYW55KTogTm9kZXxudWxsO1xuICBhYnN0cmFjdCBuZXh0U2libGluZyhlbDogYW55KTogTm9kZXxudWxsO1xuICBhYnN0cmFjdCBwYXJlbnRFbGVtZW50KGVsOiBhbnkpOiBOb2RlfG51bGw7XG4gIGFic3RyYWN0IGNoaWxkTm9kZXMoZWw6IGFueSk6IE5vZGVbXTtcbiAgYWJzdHJhY3QgY2hpbGROb2Rlc0FzTGlzdChlbDogYW55KTogTm9kZVtdO1xuICBhYnN0cmFjdCBjbGVhck5vZGVzKGVsOiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IGFwcGVuZENoaWxkKGVsOiBhbnksIG5vZGU6IGFueSk6IGFueTtcbiAgYWJzdHJhY3QgcmVtb3ZlQ2hpbGQoZWw6IGFueSwgbm9kZTogYW55KTogYW55O1xuICBhYnN0cmFjdCByZXBsYWNlQ2hpbGQoZWw6IGFueSwgbmV3Tm9kZTogYW55LCBvbGROb2RlOiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IHJlbW92ZShlbDogYW55KTogTm9kZTtcbiAgYWJzdHJhY3QgaW5zZXJ0QmVmb3JlKHBhcmVudDogYW55LCByZWY6IGFueSwgbm9kZTogYW55KTogYW55O1xuICBhYnN0cmFjdCBpbnNlcnRBbGxCZWZvcmUocGFyZW50OiBhbnksIHJlZjogYW55LCBub2RlczogYW55KTogYW55O1xuICBhYnN0cmFjdCBpbnNlcnRBZnRlcihwYXJlbnQ6IGFueSwgZWw6IGFueSwgbm9kZTogYW55KTogYW55O1xuICBhYnN0cmFjdCBzZXRJbm5lckhUTUwoZWw6IGFueSwgdmFsdWU6IGFueSk6IGFueTtcbiAgYWJzdHJhY3QgZ2V0VGV4dChlbDogYW55KTogc3RyaW5nfG51bGw7XG4gIGFic3RyYWN0IHNldFRleHQoZWw6IGFueSwgdmFsdWU6IHN0cmluZyk6IGFueTtcbiAgYWJzdHJhY3QgZ2V0VmFsdWUoZWw6IGFueSk6IHN0cmluZztcbiAgYWJzdHJhY3Qgc2V0VmFsdWUoZWw6IGFueSwgdmFsdWU6IHN0cmluZyk6IGFueTtcbiAgYWJzdHJhY3QgZ2V0Q2hlY2tlZChlbDogYW55KTogYm9vbGVhbjtcbiAgYWJzdHJhY3Qgc2V0Q2hlY2tlZChlbDogYW55LCB2YWx1ZTogYm9vbGVhbik6IGFueTtcbiAgYWJzdHJhY3QgY3JlYXRlQ29tbWVudCh0ZXh0OiBzdHJpbmcpOiBhbnk7XG4gIGFic3RyYWN0IGNyZWF0ZVRlbXBsYXRlKGh0bWw6IGFueSk6IEhUTUxFbGVtZW50O1xuICBhYnN0cmFjdCBjcmVhdGVFbGVtZW50KHRhZ05hbWU6IGFueSwgZG9jPzogYW55KTogSFRNTEVsZW1lbnQ7XG4gIGFic3RyYWN0IGNyZWF0ZUVsZW1lbnROUyhuczogc3RyaW5nLCB0YWdOYW1lOiBzdHJpbmcsIGRvYz86IGFueSk6IEVsZW1lbnQ7XG4gIGFic3RyYWN0IGNyZWF0ZVRleHROb2RlKHRleHQ6IHN0cmluZywgZG9jPzogYW55KTogVGV4dDtcbiAgYWJzdHJhY3QgY3JlYXRlU2NyaXB0VGFnKGF0dHJOYW1lOiBzdHJpbmcsIGF0dHJWYWx1ZTogc3RyaW5nLCBkb2M/OiBhbnkpOiBIVE1MRWxlbWVudDtcbiAgYWJzdHJhY3QgY3JlYXRlU3R5bGVFbGVtZW50KGNzczogc3RyaW5nLCBkb2M/OiBhbnkpOiBIVE1MU3R5bGVFbGVtZW50O1xuICBhYnN0cmFjdCBjcmVhdGVTaGFkb3dSb290KGVsOiBhbnkpOiBhbnk7XG4gIGFic3RyYWN0IGdldFNoYWRvd1Jvb3QoZWw6IGFueSk6IGFueTtcbiAgYWJzdHJhY3QgZ2V0SG9zdChlbDogYW55KTogYW55O1xuICBhYnN0cmFjdCBnZXREaXN0cmlidXRlZE5vZGVzKGVsOiBhbnkpOiBOb2RlW107XG4gIGFic3RyYWN0IGNsb25lIC8qPFQgZXh0ZW5kcyBOb2RlPiovIChub2RlOiBOb2RlIC8qVCovKTogTm9kZSAvKlQqLztcbiAgYWJzdHJhY3QgZ2V0RWxlbWVudHNCeUNsYXNzTmFtZShlbGVtZW50OiBhbnksIG5hbWU6IHN0cmluZyk6IEhUTUxFbGVtZW50W107XG4gIGFic3RyYWN0IGdldEVsZW1lbnRzQnlUYWdOYW1lKGVsZW1lbnQ6IGFueSwgbmFtZTogc3RyaW5nKTogSFRNTEVsZW1lbnRbXTtcbiAgYWJzdHJhY3QgY2xhc3NMaXN0KGVsZW1lbnQ6IGFueSk6IGFueVtdO1xuICBhYnN0cmFjdCBhZGRDbGFzcyhlbGVtZW50OiBhbnksIGNsYXNzTmFtZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCByZW1vdmVDbGFzcyhlbGVtZW50OiBhbnksIGNsYXNzTmFtZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCBoYXNDbGFzcyhlbGVtZW50OiBhbnksIGNsYXNzTmFtZTogc3RyaW5nKTogYm9vbGVhbjtcbiAgYWJzdHJhY3Qgc2V0U3R5bGUoZWxlbWVudDogYW55LCBzdHlsZU5hbWU6IHN0cmluZywgc3R5bGVWYWx1ZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCByZW1vdmVTdHlsZShlbGVtZW50OiBhbnksIHN0eWxlTmFtZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCBnZXRTdHlsZShlbGVtZW50OiBhbnksIHN0eWxlTmFtZTogc3RyaW5nKTogc3RyaW5nO1xuICBhYnN0cmFjdCBoYXNTdHlsZShlbGVtZW50OiBhbnksIHN0eWxlTmFtZTogc3RyaW5nLCBzdHlsZVZhbHVlPzogc3RyaW5nKTogYm9vbGVhbjtcbiAgYWJzdHJhY3QgdGFnTmFtZShlbGVtZW50OiBhbnkpOiBzdHJpbmc7XG4gIGFic3RyYWN0IGF0dHJpYnV0ZU1hcChlbGVtZW50OiBhbnkpOiBNYXA8c3RyaW5nLCBzdHJpbmc+O1xuICBhYnN0cmFjdCBoYXNBdHRyaWJ1dGUoZWxlbWVudDogYW55LCBhdHRyaWJ1dGU6IHN0cmluZyk6IGJvb2xlYW47XG4gIGFic3RyYWN0IGhhc0F0dHJpYnV0ZU5TKGVsZW1lbnQ6IGFueSwgbnM6IHN0cmluZywgYXR0cmlidXRlOiBzdHJpbmcpOiBib29sZWFuO1xuICBhYnN0cmFjdCBnZXRBdHRyaWJ1dGUoZWxlbWVudDogYW55LCBhdHRyaWJ1dGU6IHN0cmluZyk6IHN0cmluZ3xudWxsO1xuICBhYnN0cmFjdCBnZXRBdHRyaWJ1dGVOUyhlbGVtZW50OiBhbnksIG5zOiBzdHJpbmcsIGF0dHJpYnV0ZTogc3RyaW5nKTogc3RyaW5nfG51bGw7XG4gIGFic3RyYWN0IHNldEF0dHJpYnV0ZShlbGVtZW50OiBhbnksIG5hbWU6IHN0cmluZywgdmFsdWU6IHN0cmluZyk6IGFueTtcbiAgYWJzdHJhY3Qgc2V0QXR0cmlidXRlTlMoZWxlbWVudDogYW55LCBuczogc3RyaW5nLCBuYW1lOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcpOiBhbnk7XG4gIGFic3RyYWN0IHJlbW92ZUF0dHJpYnV0ZShlbGVtZW50OiBhbnksIGF0dHJpYnV0ZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCByZW1vdmVBdHRyaWJ1dGVOUyhlbGVtZW50OiBhbnksIG5zOiBzdHJpbmcsIGF0dHJpYnV0ZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCB0ZW1wbGF0ZUF3YXJlUm9vdChlbDogYW55KTogYW55O1xuICBhYnN0cmFjdCBjcmVhdGVIdG1sRG9jdW1lbnQoKTogSFRNTERvY3VtZW50O1xuICBhYnN0cmFjdCBnZXREZWZhdWx0RG9jdW1lbnQoKTogRG9jdW1lbnQ7XG4gIGFic3RyYWN0IGdldEJvdW5kaW5nQ2xpZW50UmVjdChlbDogYW55KTogYW55O1xuICBhYnN0cmFjdCBnZXRUaXRsZShkb2M6IERvY3VtZW50KTogc3RyaW5nO1xuICBhYnN0cmFjdCBzZXRUaXRsZShkb2M6IERvY3VtZW50LCBuZXdUaXRsZTogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCBlbGVtZW50TWF0Y2hlcyhuOiBhbnksIHNlbGVjdG9yOiBzdHJpbmcpOiBib29sZWFuO1xuICBhYnN0cmFjdCBpc1RlbXBsYXRlRWxlbWVudChlbDogYW55KTogYm9vbGVhbjtcbiAgYWJzdHJhY3QgaXNUZXh0Tm9kZShub2RlOiBhbnkpOiBib29sZWFuO1xuICBhYnN0cmFjdCBpc0NvbW1lbnROb2RlKG5vZGU6IGFueSk6IGJvb2xlYW47XG4gIGFic3RyYWN0IGlzRWxlbWVudE5vZGUobm9kZTogYW55KTogYm9vbGVhbjtcbiAgYWJzdHJhY3QgaGFzU2hhZG93Um9vdChub2RlOiBhbnkpOiBib29sZWFuO1xuICBhYnN0cmFjdCBpc1NoYWRvd1Jvb3Qobm9kZTogYW55KTogYm9vbGVhbjtcbiAgYWJzdHJhY3QgaW1wb3J0SW50b0RvYyAvKjxUIGV4dGVuZHMgTm9kZT4qLyAobm9kZTogTm9kZSAvKlQqLyk6IE5vZGUgLypUKi87XG4gIGFic3RyYWN0IGFkb3B0Tm9kZSAvKjxUIGV4dGVuZHMgTm9kZT4qLyAobm9kZTogTm9kZSAvKlQqLyk6IE5vZGUgLypUKi87XG4gIGFic3RyYWN0IGdldEhyZWYoZWxlbWVudDogYW55KTogc3RyaW5nO1xuICBhYnN0cmFjdCBnZXRFdmVudEtleShldmVudDogYW55KTogc3RyaW5nO1xuICBhYnN0cmFjdCByZXNvbHZlQW5kU2V0SHJlZihlbGVtZW50OiBhbnksIGJhc2VVcmw6IHN0cmluZywgaHJlZjogc3RyaW5nKTogYW55O1xuICBhYnN0cmFjdCBzdXBwb3J0c0RPTUV2ZW50cygpOiBib29sZWFuO1xuICBhYnN0cmFjdCBzdXBwb3J0c05hdGl2ZVNoYWRvd0RPTSgpOiBib29sZWFuO1xuICBhYnN0cmFjdCBnZXRHbG9iYWxFdmVudFRhcmdldChkb2M6IERvY3VtZW50LCB0YXJnZXQ6IHN0cmluZyk6IGFueTtcbiAgYWJzdHJhY3QgZ2V0SGlzdG9yeSgpOiBIaXN0b3J5O1xuICBhYnN0cmFjdCBnZXRMb2NhdGlvbigpOiBMb2NhdGlvbjtcbiAgYWJzdHJhY3QgZ2V0QmFzZUhyZWYoZG9jOiBEb2N1bWVudCk6IHN0cmluZ3xudWxsO1xuICBhYnN0cmFjdCByZXNldEJhc2VFbGVtZW50KCk6IHZvaWQ7XG4gIGFic3RyYWN0IGdldFVzZXJBZ2VudCgpOiBzdHJpbmc7XG4gIGFic3RyYWN0IHNldERhdGEoZWxlbWVudDogYW55LCBuYW1lOiBzdHJpbmcsIHZhbHVlOiBzdHJpbmcpOiBhbnk7XG4gIGFic3RyYWN0IGdldENvbXB1dGVkU3R5bGUoZWxlbWVudDogYW55KTogYW55O1xuICBhYnN0cmFjdCBnZXREYXRhKGVsZW1lbnQ6IGFueSwgbmFtZTogc3RyaW5nKTogc3RyaW5nfG51bGw7XG4gIGFic3RyYWN0IHN1cHBvcnRzV2ViQW5pbWF0aW9uKCk6IGJvb2xlYW47XG4gIGFic3RyYWN0IHBlcmZvcm1hbmNlTm93KCk6IG51bWJlcjtcbiAgYWJzdHJhY3QgZ2V0QW5pbWF0aW9uUHJlZml4KCk6IHN0cmluZztcbiAgYWJzdHJhY3QgZ2V0VHJhbnNpdGlvbkVuZCgpOiBzdHJpbmc7XG4gIGFic3RyYWN0IHN1cHBvcnRzQW5pbWF0aW9uKCk6IGJvb2xlYW47XG5cbiAgYWJzdHJhY3Qgc3VwcG9ydHNDb29raWVzKCk6IGJvb2xlYW47XG4gIGFic3RyYWN0IGdldENvb2tpZShuYW1lOiBzdHJpbmcpOiBzdHJpbmd8bnVsbDtcbiAgYWJzdHJhY3Qgc2V0Q29va2llKG5hbWU6IHN0cmluZywgdmFsdWU6IHN0cmluZyk6IGFueTtcbn1cbiJdfQ==