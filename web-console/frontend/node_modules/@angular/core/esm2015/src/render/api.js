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
import { InjectionToken } from '../di/injection_token';
import { injectRenderer2 as render3InjectRenderer2 } from '../render3/view_engine_compatibility';
import { noop } from '../util/noop';
/**
 * @deprecated Use `RendererType2` (and `Renderer2`) instead.
 * \@publicApi
 */
export class RenderComponentType {
    /**
     * @param {?} id
     * @param {?} templateUrl
     * @param {?} slotCount
     * @param {?} encapsulation
     * @param {?} styles
     * @param {?} animations
     */
    constructor(id, templateUrl, slotCount, encapsulation, styles, animations) {
        this.id = id;
        this.templateUrl = templateUrl;
        this.slotCount = slotCount;
        this.encapsulation = encapsulation;
        this.styles = styles;
        this.animations = animations;
    }
}
if (false) {
    /** @type {?} */
    RenderComponentType.prototype.id;
    /** @type {?} */
    RenderComponentType.prototype.templateUrl;
    /** @type {?} */
    RenderComponentType.prototype.slotCount;
    /** @type {?} */
    RenderComponentType.prototype.encapsulation;
    /** @type {?} */
    RenderComponentType.prototype.styles;
    /** @type {?} */
    RenderComponentType.prototype.animations;
}
/**
 * @deprecated Debug info is handled internally in the view engine now.
 * @abstract
 */
export class RenderDebugInfo {
}
if (false) {
    /**
     * @abstract
     * @return {?}
     */
    RenderDebugInfo.prototype.injector = function () { };
    /**
     * @abstract
     * @return {?}
     */
    RenderDebugInfo.prototype.component = function () { };
    /**
     * @abstract
     * @return {?}
     */
    RenderDebugInfo.prototype.providerTokens = function () { };
    /**
     * @abstract
     * @return {?}
     */
    RenderDebugInfo.prototype.references = function () { };
    /**
     * @abstract
     * @return {?}
     */
    RenderDebugInfo.prototype.context = function () { };
    /**
     * @abstract
     * @return {?}
     */
    RenderDebugInfo.prototype.source = function () { };
}
/**
 * @deprecated Use the `Renderer2` instead.
 * @record
 */
export function DirectRenderer() { }
if (false) {
    /**
     * @param {?} node
     * @return {?}
     */
    DirectRenderer.prototype.remove = function (node) { };
    /**
     * @param {?} node
     * @param {?} parent
     * @return {?}
     */
    DirectRenderer.prototype.appendChild = function (node, parent) { };
    /**
     * @param {?} node
     * @param {?} refNode
     * @return {?}
     */
    DirectRenderer.prototype.insertBefore = function (node, refNode) { };
    /**
     * @param {?} node
     * @return {?}
     */
    DirectRenderer.prototype.nextSibling = function (node) { };
    /**
     * @param {?} node
     * @return {?}
     */
    DirectRenderer.prototype.parentElement = function (node) { };
}
/**
 * @deprecated Use the `Renderer2` instead.
 * \@publicApi
 * @abstract
 */
export class Renderer {
}
if (false) {
    /**
     * @abstract
     * @param {?} selectorOrNode
     * @param {?=} debugInfo
     * @return {?}
     */
    Renderer.prototype.selectRootElement = function (selectorOrNode, debugInfo) { };
    /**
     * @abstract
     * @param {?} parentElement
     * @param {?} name
     * @param {?=} debugInfo
     * @return {?}
     */
    Renderer.prototype.createElement = function (parentElement, name, debugInfo) { };
    /**
     * @abstract
     * @param {?} hostElement
     * @return {?}
     */
    Renderer.prototype.createViewRoot = function (hostElement) { };
    /**
     * @abstract
     * @param {?} parentElement
     * @param {?=} debugInfo
     * @return {?}
     */
    Renderer.prototype.createTemplateAnchor = function (parentElement, debugInfo) { };
    /**
     * @abstract
     * @param {?} parentElement
     * @param {?} value
     * @param {?=} debugInfo
     * @return {?}
     */
    Renderer.prototype.createText = function (parentElement, value, debugInfo) { };
    /**
     * @abstract
     * @param {?} parentElement
     * @param {?} nodes
     * @return {?}
     */
    Renderer.prototype.projectNodes = function (parentElement, nodes) { };
    /**
     * @abstract
     * @param {?} node
     * @param {?} viewRootNodes
     * @return {?}
     */
    Renderer.prototype.attachViewAfter = function (node, viewRootNodes) { };
    /**
     * @abstract
     * @param {?} viewRootNodes
     * @return {?}
     */
    Renderer.prototype.detachView = function (viewRootNodes) { };
    /**
     * @abstract
     * @param {?} hostElement
     * @param {?} viewAllNodes
     * @return {?}
     */
    Renderer.prototype.destroyView = function (hostElement, viewAllNodes) { };
    /**
     * @abstract
     * @param {?} renderElement
     * @param {?} name
     * @param {?} callback
     * @return {?}
     */
    Renderer.prototype.listen = function (renderElement, name, callback) { };
    /**
     * @abstract
     * @param {?} target
     * @param {?} name
     * @param {?} callback
     * @return {?}
     */
    Renderer.prototype.listenGlobal = function (target, name, callback) { };
    /**
     * @abstract
     * @param {?} renderElement
     * @param {?} propertyName
     * @param {?} propertyValue
     * @return {?}
     */
    Renderer.prototype.setElementProperty = function (renderElement, propertyName, propertyValue) { };
    /**
     * @abstract
     * @param {?} renderElement
     * @param {?} attributeName
     * @param {?=} attributeValue
     * @return {?}
     */
    Renderer.prototype.setElementAttribute = function (renderElement, attributeName, attributeValue) { };
    /**
     * Used only in debug mode to serialize property changes to dom nodes as attributes.
     * @abstract
     * @param {?} renderElement
     * @param {?} propertyName
     * @param {?} propertyValue
     * @return {?}
     */
    Renderer.prototype.setBindingDebugInfo = function (renderElement, propertyName, propertyValue) { };
    /**
     * @abstract
     * @param {?} renderElement
     * @param {?} className
     * @param {?} isAdd
     * @return {?}
     */
    Renderer.prototype.setElementClass = function (renderElement, className, isAdd) { };
    /**
     * @abstract
     * @param {?} renderElement
     * @param {?} styleName
     * @param {?=} styleValue
     * @return {?}
     */
    Renderer.prototype.setElementStyle = function (renderElement, styleName, styleValue) { };
    /**
     * @abstract
     * @param {?} renderElement
     * @param {?} methodName
     * @param {?=} args
     * @return {?}
     */
    Renderer.prototype.invokeElementMethod = function (renderElement, methodName, args) { };
    /**
     * @abstract
     * @param {?} renderNode
     * @param {?} text
     * @return {?}
     */
    Renderer.prototype.setText = function (renderNode, text) { };
    /**
     * @abstract
     * @param {?} element
     * @param {?} startingStyles
     * @param {?} keyframes
     * @param {?} duration
     * @param {?} delay
     * @param {?} easing
     * @param {?=} previousPlayers
     * @return {?}
     */
    Renderer.prototype.animate = function (element, startingStyles, keyframes, duration, delay, easing, previousPlayers) { };
}
/** @type {?} */
export const Renderer2Interceptor = new InjectionToken('Renderer2Interceptor');
/**
 * Injectable service that provides a low-level interface for modifying the UI.
 *
 * Use this service to bypass Angular's templating and make custom UI changes that can't be
 * expressed declaratively. For example if you need to set a property or an attribute whose name is
 * not statically known, use {\@link Renderer#setElementProperty setElementProperty} or
 * {\@link Renderer#setElementAttribute setElementAttribute} respectively.
 *
 * If you are implementing a custom renderer, you must implement this interface.
 *
 * The default Renderer implementation is `DomRenderer`. Also available is `WebWorkerRenderer`.
 *
 * @deprecated Use `RendererFactory2` instead.
 * \@publicApi
 * @abstract
 */
export class RootRenderer {
}
if (false) {
    /**
     * @abstract
     * @param {?} componentType
     * @return {?}
     */
    RootRenderer.prototype.renderComponent = function (componentType) { };
}
/**
 * Used by `RendererFactory2` to associate custom rendering data and styles
 * with a rendering implementation.
 * \@publicApi
 * @record
 */
export function RendererType2() { }
if (false) {
    /**
     * A unique identifying string for the new renderer, used when creating
     * unique styles for encapsulation.
     * @type {?}
     */
    RendererType2.prototype.id;
    /**
     * The view encapsulation type, which determines how styles are applied to
     * DOM elements. One of
     * - `Emulated` (default): Emulate native scoping of styles.
     * - `Native`: Use the native encapsulation mechanism of the renderer.
     * - `ShadowDom`: Use modern [Shadow
     * DOM](https://w3c.github.io/webcomponents/spec/shadow/) and
     * create a ShadowRoot for component's host element.
     * - `None`: Do not provide any template or style encapsulation.
     * @type {?}
     */
    RendererType2.prototype.encapsulation;
    /**
     * Defines CSS styles to be stored on a renderer instance.
     * @type {?}
     */
    RendererType2.prototype.styles;
    /**
     * Defines arbitrary developer-defined data to be stored on a renderer instance.
     * This is useful for renderers that delegate to other renderers.
     * @type {?}
     */
    RendererType2.prototype.data;
}
/**
 * Creates and initializes a custom renderer that implements the `Renderer2` base class.
 *
 * \@publicApi
 * @abstract
 */
export class RendererFactory2 {
}
if (false) {
    /**
     * Creates and initializes a custom renderer for a host DOM element.
     * @abstract
     * @param {?} hostElement The element to render.
     * @param {?} type The base class to implement.
     * @return {?} The new custom renderer instance.
     */
    RendererFactory2.prototype.createRenderer = function (hostElement, type) { };
    /**
     * A callback invoked when rendering has begun.
     * @abstract
     * @return {?}
     */
    RendererFactory2.prototype.begin = function () { };
    /**
     * A callback invoked when rendering has completed.
     * @abstract
     * @return {?}
     */
    RendererFactory2.prototype.end = function () { };
    /**
     * Use with animations test-only mode. Notifies the test when rendering has completed.
     * @abstract
     * @return {?} The asynchronous result of the developer-defined function.
     */
    RendererFactory2.prototype.whenRenderingDone = function () { };
}
/** @enum {number} */
const RendererStyleFlags2 = {
    /**
     * Marks a style as important.
     */
    Important: 1,
    /**
     * Marks a style as using dash case naming (this-is-dash-case).
     */
    DashCase: 2,
};
export { RendererStyleFlags2 };
RendererStyleFlags2[RendererStyleFlags2.Important] = 'Important';
RendererStyleFlags2[RendererStyleFlags2.DashCase] = 'DashCase';
/**
 * Extend this base class to implement custom rendering. By default, Angular
 * renders a template into DOM. You can use custom rendering to intercept
 * rendering calls, or to render to something other than DOM.
 *
 * Create your custom renderer using `RendererFactory2`.
 *
 * Use a custom renderer to bypass Angular's templating and
 * make custom UI changes that can't be expressed declaratively.
 * For example if you need to set a property or an attribute whose name is
 * not statically known, use the `setProperty()` or
 * `setAttribute()` method.
 *
 * \@publicApi
 * @abstract
 */
export class Renderer2 {
}
/**
 * \@internal
 * @nocollapse
 */
Renderer2.__NG_ELEMENT_ID__ = (/**
 * @return {?}
 */
() => SWITCH_RENDERER2_FACTORY());
if (false) {
    /**
     * \@internal
     * @nocollapse
     * @type {?}
     */
    Renderer2.__NG_ELEMENT_ID__;
    /**
     * If null or undefined, the view engine won't call it.
     * This is used as a performance optimization for production mode.
     * @type {?}
     */
    Renderer2.prototype.destroyNode;
    /**
     * Use to store arbitrary developer-defined data on a renderer instance,
     * as an object containing key-value pairs.
     * This is useful for renderers that delegate to other renderers.
     * @abstract
     * @return {?}
     */
    Renderer2.prototype.data = function () { };
    /**
     * Implement this callback to destroy the renderer or the host element.
     * @abstract
     * @return {?}
     */
    Renderer2.prototype.destroy = function () { };
    /**
     * Implement this callback to create an instance of the host element.
     * @abstract
     * @param {?} name An identifying name for the new element, unique within the namespace.
     * @param {?=} namespace The namespace for the new element.
     * @return {?} The new element.
     */
    Renderer2.prototype.createElement = function (name, namespace) { };
    /**
     * Implement this callback to add a comment to the DOM of the host element.
     * @abstract
     * @param {?} value The comment text.
     * @return {?} The modified element.
     */
    Renderer2.prototype.createComment = function (value) { };
    /**
     * Implement this callback to add text to the DOM of the host element.
     * @abstract
     * @param {?} value The text string.
     * @return {?} The modified element.
     */
    Renderer2.prototype.createText = function (value) { };
    /**
     * Appends a child to a given parent node in the host element DOM.
     * @abstract
     * @param {?} parent The parent node.
     * @param {?} newChild The new child node.
     * @return {?}
     */
    Renderer2.prototype.appendChild = function (parent, newChild) { };
    /**
     * Implement this callback to insert a child node at a given position in a parent node
     * in the host element DOM.
     * @abstract
     * @param {?} parent The parent node.
     * @param {?} newChild The new child nodes.
     * @param {?} refChild The existing child node that should precede the new node.
     * @return {?}
     */
    Renderer2.prototype.insertBefore = function (parent, newChild, refChild) { };
    /**
     * Implement this callback to remove a child node from the host element's DOM.
     * @abstract
     * @param {?} parent The parent node.
     * @param {?} oldChild The child node to remove.
     * @param {?=} isHostElement Optionally signal to the renderer whether this element is a host element
     * or not
     * @return {?}
     */
    Renderer2.prototype.removeChild = function (parent, oldChild, isHostElement) { };
    /**
     * Implement this callback to prepare an element to be bootstrapped
     * as a root element, and return the element instance.
     * @abstract
     * @param {?} selectorOrNode The DOM element.
     * @param {?=} preserveContent Whether the contents of the root element
     * should be preserved, or cleared upon bootstrap (default behavior).
     * Use with `ViewEncapsulation.ShadowDom` to allow simple native
     * content projection via `<slot>` elements.
     * @return {?} The root element.
     */
    Renderer2.prototype.selectRootElement = function (selectorOrNode, preserveContent) { };
    /**
     * Implement this callback to get the parent of a given node
     * in the host element's DOM.
     * @abstract
     * @param {?} node The child node to query.
     * @return {?} The parent node, or null if there is no parent.
     * For WebWorkers, always returns true.
     * This is because the check is synchronous,
     * and the caller can't rely on checking for null.
     */
    Renderer2.prototype.parentNode = function (node) { };
    /**
     * Implement this callback to get the next sibling node of a given node
     * in the host element's DOM.
     * @abstract
     * @param {?} node
     * @return {?} The sibling node, or null if there is no sibling.
     * For WebWorkers, always returns a value.
     * This is because the check is synchronous,
     * and the caller can't rely on checking for null.
     */
    Renderer2.prototype.nextSibling = function (node) { };
    /**
     * Implement this callback to set an attribute value for an element in the DOM.
     * @abstract
     * @param {?} el The element.
     * @param {?} name The attribute name.
     * @param {?} value The new value.
     * @param {?=} namespace The namespace.
     * @return {?}
     */
    Renderer2.prototype.setAttribute = function (el, name, value, namespace) { };
    /**
     * Implement this callback to remove an attribute from an element in the DOM.
     * @abstract
     * @param {?} el The element.
     * @param {?} name The attribute name.
     * @param {?=} namespace The namespace.
     * @return {?}
     */
    Renderer2.prototype.removeAttribute = function (el, name, namespace) { };
    /**
     * Implement this callback to add a class to an element in the DOM.
     * @abstract
     * @param {?} el The element.
     * @param {?} name The class name.
     * @return {?}
     */
    Renderer2.prototype.addClass = function (el, name) { };
    /**
     * Implement this callback to remove a class from an element in the DOM.
     * @abstract
     * @param {?} el The element.
     * @param {?} name The class name.
     * @return {?}
     */
    Renderer2.prototype.removeClass = function (el, name) { };
    /**
     * Implement this callback to set a CSS style for an element in the DOM.
     * @abstract
     * @param {?} el The element.
     * @param {?} style The name of the style.
     * @param {?} value The new value.
     * @param {?=} flags Flags for style variations. No flags are set by default.
     * @return {?}
     */
    Renderer2.prototype.setStyle = function (el, style, value, flags) { };
    /**
     * Implement this callback to remove the value from a CSS style for an element in the DOM.
     * @abstract
     * @param {?} el The element.
     * @param {?} style The name of the style.
     * @param {?=} flags Flags for style variations to remove, if set. ???
     * @return {?}
     */
    Renderer2.prototype.removeStyle = function (el, style, flags) { };
    /**
     * Implement this callback to set the value of a property of an element in the DOM.
     * @abstract
     * @param {?} el The element.
     * @param {?} name The property name.
     * @param {?} value The new value.
     * @return {?}
     */
    Renderer2.prototype.setProperty = function (el, name, value) { };
    /**
     * Implement this callback to set the value of a node in the host element.
     * @abstract
     * @param {?} node The node.
     * @param {?} value The new value.
     * @return {?}
     */
    Renderer2.prototype.setValue = function (node, value) { };
    /**
     * Implement this callback to start an event listener.
     * @abstract
     * @param {?} target The context in which to listen for events. Can be
     * the entire window or document, the body of the document, or a specific
     * DOM element.
     * @param {?} eventName The event to listen for.
     * @param {?} callback A handler function to invoke when the event occurs.
     * @return {?} An "unlisten" function for disposing of this handler.
     */
    Renderer2.prototype.listen = function (target, eventName, callback) { };
}
/** @type {?} */
export const SWITCH_RENDERER2_FACTORY__POST_R3__ = render3InjectRenderer2;
/** @type {?} */
const SWITCH_RENDERER2_FACTORY__PRE_R3__ = noop;
/** @type {?} */
const SWITCH_RENDERER2_FACTORY = SWITCH_RENDERER2_FACTORY__PRE_R3__;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXBpLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zcmMvcmVuZGVyL2FwaS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxjQUFjLEVBQUMsTUFBTSx1QkFBdUIsQ0FBQztBQUdyRCxPQUFPLEVBQUMsZUFBZSxJQUFJLHNCQUFzQixFQUFDLE1BQU0sc0NBQXNDLENBQUM7QUFDL0YsT0FBTyxFQUFDLElBQUksRUFBQyxNQUFNLGNBQWMsQ0FBQzs7Ozs7QUFRbEMsTUFBTSxPQUFPLG1CQUFtQjs7Ozs7Ozs7O0lBQzlCLFlBQ1csRUFBVSxFQUFTLFdBQW1CLEVBQVMsU0FBaUIsRUFDaEUsYUFBZ0MsRUFBUyxNQUEyQixFQUNwRSxVQUFlO1FBRmYsT0FBRSxHQUFGLEVBQUUsQ0FBUTtRQUFTLGdCQUFXLEdBQVgsV0FBVyxDQUFRO1FBQVMsY0FBUyxHQUFULFNBQVMsQ0FBUTtRQUNoRSxrQkFBYSxHQUFiLGFBQWEsQ0FBbUI7UUFBUyxXQUFNLEdBQU4sTUFBTSxDQUFxQjtRQUNwRSxlQUFVLEdBQVYsVUFBVSxDQUFLO0lBQUcsQ0FBQztDQUMvQjs7O0lBSEssaUNBQWlCOztJQUFFLDBDQUEwQjs7SUFBRSx3Q0FBd0I7O0lBQ3ZFLDRDQUF1Qzs7SUFBRSxxQ0FBa0M7O0lBQzNFLHlDQUFzQjs7Ozs7O0FBTTVCLE1BQU0sT0FBZ0IsZUFBZTtDQU9wQzs7Ozs7O0lBTkMscURBQWtDOzs7OztJQUNsQyxzREFBOEI7Ozs7O0lBQzlCLDJEQUFxQzs7Ozs7SUFDckMsdURBQWdEOzs7OztJQUNoRCxvREFBNEI7Ozs7O0lBQzVCLG1EQUE4Qjs7Ozs7O0FBTWhDLG9DQU1DOzs7Ozs7SUFMQyxzREFBd0I7Ozs7OztJQUN4QixtRUFBMEM7Ozs7OztJQUMxQyxxRUFBNEM7Ozs7O0lBQzVDLDJEQUE0Qjs7Ozs7SUFDNUIsNkRBQThCOzs7Ozs7O0FBT2hDLE1BQU0sT0FBZ0IsUUFBUTtDQTZDN0I7Ozs7Ozs7O0lBNUNDLGdGQUF5Rjs7Ozs7Ozs7SUFFekYsaUZBQTJGOzs7Ozs7SUFFM0YsK0RBQStDOzs7Ozs7O0lBRS9DLGtGQUFvRjs7Ozs7Ozs7SUFFcEYsK0VBQXlGOzs7Ozs7O0lBRXpGLHNFQUE4RDs7Ozs7OztJQUU5RCx3RUFBZ0U7Ozs7OztJQUVoRSw2REFBZ0Q7Ozs7Ozs7SUFFaEQsMEVBQWtFOzs7Ozs7OztJQUVsRSx5RUFBZ0Y7Ozs7Ozs7O0lBRWhGLHdFQUFrRjs7Ozs7Ozs7SUFFbEYsa0dBQWdHOzs7Ozs7OztJQUVoRyxxR0FDUzs7Ozs7Ozs7O0lBS1QsbUdBQ1M7Ozs7Ozs7O0lBRVQsb0ZBQXNGOzs7Ozs7OztJQUV0Rix5RkFBMkY7Ozs7Ozs7O0lBRTNGLHdGQUF5Rjs7Ozs7OztJQUV6Riw2REFBc0Q7Ozs7Ozs7Ozs7OztJQUV0RCx5SEFFa0Q7OztBQUdwRCxNQUFNLE9BQU8sb0JBQW9CLEdBQUcsSUFBSSxjQUFjLENBQWMsc0JBQXNCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBaUIzRixNQUFNLE9BQWdCLFlBQVk7Q0FFakM7Ozs7Ozs7SUFEQyxzRUFBdUU7Ozs7Ozs7O0FBUXpFLG1DQTBCQzs7Ozs7OztJQXJCQywyQkFBVzs7Ozs7Ozs7Ozs7O0lBV1gsc0NBQWlDOzs7OztJQUlqQywrQkFBeUI7Ozs7OztJQUt6Qiw2QkFBNEI7Ozs7Ozs7O0FBUTlCLE1BQU0sT0FBZ0IsZ0JBQWdCO0NBcUJyQzs7Ozs7Ozs7O0lBZEMsNkVBQStFOzs7Ozs7SUFJL0UsbURBQXdCOzs7Ozs7SUFJeEIsaURBQXNCOzs7Ozs7SUFLdEIsK0RBQTRDOzs7O0lBUTVDOztPQUVHO0lBQ0gsWUFBa0I7SUFDbEI7O09BRUc7SUFDSCxXQUFpQjs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBa0JuQixNQUFNLE9BQWdCLFNBQVM7Ozs7OztBQXlLdEIsMkJBQWlCOzs7QUFBb0IsR0FBRyxFQUFFLENBQUMsd0JBQXdCLEVBQUUsRUFBQzs7Ozs7OztJQUE3RSw0QkFBNkU7Ozs7OztJQXBJN0UsZ0NBQTRDOzs7Ozs7OztJQS9CNUMsMkNBQTBDOzs7Ozs7SUFLMUMsOENBQXlCOzs7Ozs7OztJQU96QixtRUFBbUU7Ozs7Ozs7SUFNbkUseURBQTJDOzs7Ozs7O0lBTzNDLHNEQUF3Qzs7Ozs7Ozs7SUFZeEMsa0VBQXVEOzs7Ozs7Ozs7O0lBUXZELDZFQUF1RTs7Ozs7Ozs7OztJQVF2RSxpRkFBZ0Y7Ozs7Ozs7Ozs7OztJQVdoRix1RkFBdUY7Ozs7Ozs7Ozs7O0lBVXZGLHFEQUFvQzs7Ozs7Ozs7Ozs7SUFTcEMsc0RBQXFDOzs7Ozs7Ozs7O0lBUXJDLDZFQUEyRjs7Ozs7Ozs7O0lBUTNGLHlFQUErRTs7Ozs7Ozs7SUFNL0UsdURBQStDOzs7Ozs7OztJQU8vQywwREFBa0Q7Ozs7Ozs7Ozs7SUFTbEQsc0VBQXlGOzs7Ozs7Ozs7SUFRekYsa0VBQWdGOzs7Ozs7Ozs7SUFRaEYsaUVBQThEOzs7Ozs7OztJQU85RCwwREFBa0Q7Ozs7Ozs7Ozs7O0lBV2xELHdFQUUwRDs7O0FBVTVELE1BQU0sT0FBTyxtQ0FBbUMsR0FBRyxzQkFBc0I7O01BQ25FLGtDQUFrQyxHQUFHLElBQUk7O01BQ3pDLHdCQUF3QixHQUFrQyxrQ0FBa0MiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0aW9uVG9rZW59IGZyb20gJy4uL2RpL2luamVjdGlvbl90b2tlbic7XG5pbXBvcnQge0luamVjdG9yfSBmcm9tICcuLi9kaS9pbmplY3Rvcic7XG5pbXBvcnQge1ZpZXdFbmNhcHN1bGF0aW9ufSBmcm9tICcuLi9tZXRhZGF0YS92aWV3JztcbmltcG9ydCB7aW5qZWN0UmVuZGVyZXIyIGFzIHJlbmRlcjNJbmplY3RSZW5kZXJlcjJ9IGZyb20gJy4uL3JlbmRlcjMvdmlld19lbmdpbmVfY29tcGF0aWJpbGl0eSc7XG5pbXBvcnQge25vb3B9IGZyb20gJy4uL3V0aWwvbm9vcCc7XG5cblxuXG4vKipcbiAqIEBkZXByZWNhdGVkIFVzZSBgUmVuZGVyZXJUeXBlMmAgKGFuZCBgUmVuZGVyZXIyYCkgaW5zdGVhZC5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIFJlbmRlckNvbXBvbmVudFR5cGUge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBpZDogc3RyaW5nLCBwdWJsaWMgdGVtcGxhdGVVcmw6IHN0cmluZywgcHVibGljIHNsb3RDb3VudDogbnVtYmVyLFxuICAgICAgcHVibGljIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLCBwdWJsaWMgc3R5bGVzOiBBcnJheTxzdHJpbmd8YW55W10+LFxuICAgICAgcHVibGljIGFuaW1hdGlvbnM6IGFueSkge31cbn1cblxuLyoqXG4gKiBAZGVwcmVjYXRlZCBEZWJ1ZyBpbmZvIGlzIGhhbmRsZWQgaW50ZXJuYWxseSBpbiB0aGUgdmlldyBlbmdpbmUgbm93LlxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgUmVuZGVyRGVidWdJbmZvIHtcbiAgYWJzdHJhY3QgZ2V0IGluamVjdG9yKCk6IEluamVjdG9yO1xuICBhYnN0cmFjdCBnZXQgY29tcG9uZW50KCk6IGFueTtcbiAgYWJzdHJhY3QgZ2V0IHByb3ZpZGVyVG9rZW5zKCk6IGFueVtdO1xuICBhYnN0cmFjdCBnZXQgcmVmZXJlbmNlcygpOiB7W2tleTogc3RyaW5nXTogYW55fTtcbiAgYWJzdHJhY3QgZ2V0IGNvbnRleHQoKTogYW55O1xuICBhYnN0cmFjdCBnZXQgc291cmNlKCk6IHN0cmluZztcbn1cblxuLyoqXG4gKiBAZGVwcmVjYXRlZCBVc2UgdGhlIGBSZW5kZXJlcjJgIGluc3RlYWQuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgRGlyZWN0UmVuZGVyZXIge1xuICByZW1vdmUobm9kZTogYW55KTogdm9pZDtcbiAgYXBwZW5kQ2hpbGQobm9kZTogYW55LCBwYXJlbnQ6IGFueSk6IHZvaWQ7XG4gIGluc2VydEJlZm9yZShub2RlOiBhbnksIHJlZk5vZGU6IGFueSk6IHZvaWQ7XG4gIG5leHRTaWJsaW5nKG5vZGU6IGFueSk6IGFueTtcbiAgcGFyZW50RWxlbWVudChub2RlOiBhbnkpOiBhbnk7XG59XG5cbi8qKlxuICogQGRlcHJlY2F0ZWQgVXNlIHRoZSBgUmVuZGVyZXIyYCBpbnN0ZWFkLlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgUmVuZGVyZXIge1xuICBhYnN0cmFjdCBzZWxlY3RSb290RWxlbWVudChzZWxlY3Rvck9yTm9kZTogc3RyaW5nfGFueSwgZGVidWdJbmZvPzogUmVuZGVyRGVidWdJbmZvKTogYW55O1xuXG4gIGFic3RyYWN0IGNyZWF0ZUVsZW1lbnQocGFyZW50RWxlbWVudDogYW55LCBuYW1lOiBzdHJpbmcsIGRlYnVnSW5mbz86IFJlbmRlckRlYnVnSW5mbyk6IGFueTtcblxuICBhYnN0cmFjdCBjcmVhdGVWaWV3Um9vdChob3N0RWxlbWVudDogYW55KTogYW55O1xuXG4gIGFic3RyYWN0IGNyZWF0ZVRlbXBsYXRlQW5jaG9yKHBhcmVudEVsZW1lbnQ6IGFueSwgZGVidWdJbmZvPzogUmVuZGVyRGVidWdJbmZvKTogYW55O1xuXG4gIGFic3RyYWN0IGNyZWF0ZVRleHQocGFyZW50RWxlbWVudDogYW55LCB2YWx1ZTogc3RyaW5nLCBkZWJ1Z0luZm8/OiBSZW5kZXJEZWJ1Z0luZm8pOiBhbnk7XG5cbiAgYWJzdHJhY3QgcHJvamVjdE5vZGVzKHBhcmVudEVsZW1lbnQ6IGFueSwgbm9kZXM6IGFueVtdKTogdm9pZDtcblxuICBhYnN0cmFjdCBhdHRhY2hWaWV3QWZ0ZXIobm9kZTogYW55LCB2aWV3Um9vdE5vZGVzOiBhbnlbXSk6IHZvaWQ7XG5cbiAgYWJzdHJhY3QgZGV0YWNoVmlldyh2aWV3Um9vdE5vZGVzOiBhbnlbXSk6IHZvaWQ7XG5cbiAgYWJzdHJhY3QgZGVzdHJveVZpZXcoaG9zdEVsZW1lbnQ6IGFueSwgdmlld0FsbE5vZGVzOiBhbnlbXSk6IHZvaWQ7XG5cbiAgYWJzdHJhY3QgbGlzdGVuKHJlbmRlckVsZW1lbnQ6IGFueSwgbmFtZTogc3RyaW5nLCBjYWxsYmFjazogRnVuY3Rpb24pOiBGdW5jdGlvbjtcblxuICBhYnN0cmFjdCBsaXN0ZW5HbG9iYWwodGFyZ2V0OiBzdHJpbmcsIG5hbWU6IHN0cmluZywgY2FsbGJhY2s6IEZ1bmN0aW9uKTogRnVuY3Rpb247XG5cbiAgYWJzdHJhY3Qgc2V0RWxlbWVudFByb3BlcnR5KHJlbmRlckVsZW1lbnQ6IGFueSwgcHJvcGVydHlOYW1lOiBzdHJpbmcsIHByb3BlcnR5VmFsdWU6IGFueSk6IHZvaWQ7XG5cbiAgYWJzdHJhY3Qgc2V0RWxlbWVudEF0dHJpYnV0ZShyZW5kZXJFbGVtZW50OiBhbnksIGF0dHJpYnV0ZU5hbWU6IHN0cmluZywgYXR0cmlidXRlVmFsdWU/OiBzdHJpbmcpOlxuICAgICAgdm9pZDtcblxuICAvKipcbiAgICogVXNlZCBvbmx5IGluIGRlYnVnIG1vZGUgdG8gc2VyaWFsaXplIHByb3BlcnR5IGNoYW5nZXMgdG8gZG9tIG5vZGVzIGFzIGF0dHJpYnV0ZXMuXG4gICAqL1xuICBhYnN0cmFjdCBzZXRCaW5kaW5nRGVidWdJbmZvKHJlbmRlckVsZW1lbnQ6IGFueSwgcHJvcGVydHlOYW1lOiBzdHJpbmcsIHByb3BlcnR5VmFsdWU6IHN0cmluZyk6XG4gICAgICB2b2lkO1xuXG4gIGFic3RyYWN0IHNldEVsZW1lbnRDbGFzcyhyZW5kZXJFbGVtZW50OiBhbnksIGNsYXNzTmFtZTogc3RyaW5nLCBpc0FkZDogYm9vbGVhbik6IHZvaWQ7XG5cbiAgYWJzdHJhY3Qgc2V0RWxlbWVudFN0eWxlKHJlbmRlckVsZW1lbnQ6IGFueSwgc3R5bGVOYW1lOiBzdHJpbmcsIHN0eWxlVmFsdWU/OiBzdHJpbmcpOiB2b2lkO1xuXG4gIGFic3RyYWN0IGludm9rZUVsZW1lbnRNZXRob2QocmVuZGVyRWxlbWVudDogYW55LCBtZXRob2ROYW1lOiBzdHJpbmcsIGFyZ3M/OiBhbnlbXSk6IHZvaWQ7XG5cbiAgYWJzdHJhY3Qgc2V0VGV4dChyZW5kZXJOb2RlOiBhbnksIHRleHQ6IHN0cmluZyk6IHZvaWQ7XG5cbiAgYWJzdHJhY3QgYW5pbWF0ZShcbiAgICAgIGVsZW1lbnQ6IGFueSwgc3RhcnRpbmdTdHlsZXM6IGFueSwga2V5ZnJhbWVzOiBhbnlbXSwgZHVyYXRpb246IG51bWJlciwgZGVsYXk6IG51bWJlcixcbiAgICAgIGVhc2luZzogc3RyaW5nLCBwcmV2aW91c1BsYXllcnM/OiBhbnlbXSk6IGFueTtcbn1cblxuZXhwb3J0IGNvbnN0IFJlbmRlcmVyMkludGVyY2VwdG9yID0gbmV3IEluamVjdGlvblRva2VuPFJlbmRlcmVyMltdPignUmVuZGVyZXIySW50ZXJjZXB0b3InKTtcblxuLyoqXG4gKiBJbmplY3RhYmxlIHNlcnZpY2UgdGhhdCBwcm92aWRlcyBhIGxvdy1sZXZlbCBpbnRlcmZhY2UgZm9yIG1vZGlmeWluZyB0aGUgVUkuXG4gKlxuICogVXNlIHRoaXMgc2VydmljZSB0byBieXBhc3MgQW5ndWxhcidzIHRlbXBsYXRpbmcgYW5kIG1ha2UgY3VzdG9tIFVJIGNoYW5nZXMgdGhhdCBjYW4ndCBiZVxuICogZXhwcmVzc2VkIGRlY2xhcmF0aXZlbHkuIEZvciBleGFtcGxlIGlmIHlvdSBuZWVkIHRvIHNldCBhIHByb3BlcnR5IG9yIGFuIGF0dHJpYnV0ZSB3aG9zZSBuYW1lIGlzXG4gKiBub3Qgc3RhdGljYWxseSBrbm93biwgdXNlIHtAbGluayBSZW5kZXJlciNzZXRFbGVtZW50UHJvcGVydHkgc2V0RWxlbWVudFByb3BlcnR5fSBvclxuICoge0BsaW5rIFJlbmRlcmVyI3NldEVsZW1lbnRBdHRyaWJ1dGUgc2V0RWxlbWVudEF0dHJpYnV0ZX0gcmVzcGVjdGl2ZWx5LlxuICpcbiAqIElmIHlvdSBhcmUgaW1wbGVtZW50aW5nIGEgY3VzdG9tIHJlbmRlcmVyLCB5b3UgbXVzdCBpbXBsZW1lbnQgdGhpcyBpbnRlcmZhY2UuXG4gKlxuICogVGhlIGRlZmF1bHQgUmVuZGVyZXIgaW1wbGVtZW50YXRpb24gaXMgYERvbVJlbmRlcmVyYC4gQWxzbyBhdmFpbGFibGUgaXMgYFdlYldvcmtlclJlbmRlcmVyYC5cbiAqXG4gKiBAZGVwcmVjYXRlZCBVc2UgYFJlbmRlcmVyRmFjdG9yeTJgIGluc3RlYWQuXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBSb290UmVuZGVyZXIge1xuICBhYnN0cmFjdCByZW5kZXJDb21wb25lbnQoY29tcG9uZW50VHlwZTogUmVuZGVyQ29tcG9uZW50VHlwZSk6IFJlbmRlcmVyO1xufVxuXG4vKipcbiAqIFVzZWQgYnkgYFJlbmRlcmVyRmFjdG9yeTJgIHRvIGFzc29jaWF0ZSBjdXN0b20gcmVuZGVyaW5nIGRhdGEgYW5kIHN0eWxlc1xuICogd2l0aCBhIHJlbmRlcmluZyBpbXBsZW1lbnRhdGlvbi5cbiAqICBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgUmVuZGVyZXJUeXBlMiB7XG4gIC8qKlxuICAgKiBBIHVuaXF1ZSBpZGVudGlmeWluZyBzdHJpbmcgZm9yIHRoZSBuZXcgcmVuZGVyZXIsIHVzZWQgd2hlbiBjcmVhdGluZ1xuICAgKiB1bmlxdWUgc3R5bGVzIGZvciBlbmNhcHN1bGF0aW9uLlxuICAgKi9cbiAgaWQ6IHN0cmluZztcbiAgLyoqXG4gICAqIFRoZSB2aWV3IGVuY2Fwc3VsYXRpb24gdHlwZSwgd2hpY2ggZGV0ZXJtaW5lcyBob3cgc3R5bGVzIGFyZSBhcHBsaWVkIHRvXG4gICAqIERPTSBlbGVtZW50cy4gT25lIG9mXG4gICAqIC0gYEVtdWxhdGVkYCAoZGVmYXVsdCk6IEVtdWxhdGUgbmF0aXZlIHNjb3Bpbmcgb2Ygc3R5bGVzLlxuICAgKiAtIGBOYXRpdmVgOiBVc2UgdGhlIG5hdGl2ZSBlbmNhcHN1bGF0aW9uIG1lY2hhbmlzbSBvZiB0aGUgcmVuZGVyZXIuXG4gICAqIC0gYFNoYWRvd0RvbWA6IFVzZSBtb2Rlcm4gW1NoYWRvd1xuICAgKiBET01dKGh0dHBzOi8vdzNjLmdpdGh1Yi5pby93ZWJjb21wb25lbnRzL3NwZWMvc2hhZG93LykgYW5kXG4gICAqIGNyZWF0ZSBhIFNoYWRvd1Jvb3QgZm9yIGNvbXBvbmVudCdzIGhvc3QgZWxlbWVudC5cbiAgICogLSBgTm9uZWA6IERvIG5vdCBwcm92aWRlIGFueSB0ZW1wbGF0ZSBvciBzdHlsZSBlbmNhcHN1bGF0aW9uLlxuICAgKi9cbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb247XG4gIC8qKlxuICAgKiBEZWZpbmVzIENTUyBzdHlsZXMgdG8gYmUgc3RvcmVkIG9uIGEgcmVuZGVyZXIgaW5zdGFuY2UuXG4gICAqL1xuICBzdHlsZXM6IChzdHJpbmd8YW55W10pW107XG4gIC8qKlxuICAgKiBEZWZpbmVzIGFyYml0cmFyeSBkZXZlbG9wZXItZGVmaW5lZCBkYXRhIHRvIGJlIHN0b3JlZCBvbiBhIHJlbmRlcmVyIGluc3RhbmNlLlxuICAgKiBUaGlzIGlzIHVzZWZ1bCBmb3IgcmVuZGVyZXJzIHRoYXQgZGVsZWdhdGUgdG8gb3RoZXIgcmVuZGVyZXJzLlxuICAgKi9cbiAgZGF0YToge1traW5kOiBzdHJpbmddOiBhbnl9O1xufVxuXG4vKipcbiAqIENyZWF0ZXMgYW5kIGluaXRpYWxpemVzIGEgY3VzdG9tIHJlbmRlcmVyIHRoYXQgaW1wbGVtZW50cyB0aGUgYFJlbmRlcmVyMmAgYmFzZSBjbGFzcy5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBSZW5kZXJlckZhY3RvcnkyIHtcbiAgLyoqXG4gICAqIENyZWF0ZXMgYW5kIGluaXRpYWxpemVzIGEgY3VzdG9tIHJlbmRlcmVyIGZvciBhIGhvc3QgRE9NIGVsZW1lbnQuXG4gICAqIEBwYXJhbSBob3N0RWxlbWVudCBUaGUgZWxlbWVudCB0byByZW5kZXIuXG4gICAqIEBwYXJhbSB0eXBlIFRoZSBiYXNlIGNsYXNzIHRvIGltcGxlbWVudC5cbiAgICogQHJldHVybnMgVGhlIG5ldyBjdXN0b20gcmVuZGVyZXIgaW5zdGFuY2UuXG4gICAqL1xuICBhYnN0cmFjdCBjcmVhdGVSZW5kZXJlcihob3N0RWxlbWVudDogYW55LCB0eXBlOiBSZW5kZXJlclR5cGUyfG51bGwpOiBSZW5kZXJlcjI7XG4gIC8qKlxuICAgKiBBIGNhbGxiYWNrIGludm9rZWQgd2hlbiByZW5kZXJpbmcgaGFzIGJlZ3VuLlxuICAgKi9cbiAgYWJzdHJhY3QgYmVnaW4/KCk6IHZvaWQ7XG4gIC8qKlxuICAgKiBBIGNhbGxiYWNrIGludm9rZWQgd2hlbiByZW5kZXJpbmcgaGFzIGNvbXBsZXRlZC5cbiAgICovXG4gIGFic3RyYWN0IGVuZD8oKTogdm9pZDtcbiAgLyoqXG4gICAqIFVzZSB3aXRoIGFuaW1hdGlvbnMgdGVzdC1vbmx5IG1vZGUuIE5vdGlmaWVzIHRoZSB0ZXN0IHdoZW4gcmVuZGVyaW5nIGhhcyBjb21wbGV0ZWQuXG4gICAqIEByZXR1cm5zIFRoZSBhc3luY2hyb25vdXMgcmVzdWx0IG9mIHRoZSBkZXZlbG9wZXItZGVmaW5lZCBmdW5jdGlvbi5cbiAgICovXG4gIGFic3RyYWN0IHdoZW5SZW5kZXJpbmdEb25lPygpOiBQcm9taXNlPGFueT47XG59XG5cbi8qKlxuICogRmxhZ3MgZm9yIHJlbmRlcmVyLXNwZWNpZmljIHN0eWxlIG1vZGlmaWVycy5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGVudW0gUmVuZGVyZXJTdHlsZUZsYWdzMiB7XG4gIC8qKlxuICAgKiBNYXJrcyBhIHN0eWxlIGFzIGltcG9ydGFudC5cbiAgICovXG4gIEltcG9ydGFudCA9IDEgPDwgMCxcbiAgLyoqXG4gICAqIE1hcmtzIGEgc3R5bGUgYXMgdXNpbmcgZGFzaCBjYXNlIG5hbWluZyAodGhpcy1pcy1kYXNoLWNhc2UpLlxuICAgKi9cbiAgRGFzaENhc2UgPSAxIDw8IDFcbn1cblxuLyoqXG4gKiBFeHRlbmQgdGhpcyBiYXNlIGNsYXNzIHRvIGltcGxlbWVudCBjdXN0b20gcmVuZGVyaW5nLiBCeSBkZWZhdWx0LCBBbmd1bGFyXG4gKiByZW5kZXJzIGEgdGVtcGxhdGUgaW50byBET00uIFlvdSBjYW4gdXNlIGN1c3RvbSByZW5kZXJpbmcgdG8gaW50ZXJjZXB0XG4gKiByZW5kZXJpbmcgY2FsbHMsIG9yIHRvIHJlbmRlciB0byBzb21ldGhpbmcgb3RoZXIgdGhhbiBET00uXG4gKlxuICogQ3JlYXRlIHlvdXIgY3VzdG9tIHJlbmRlcmVyIHVzaW5nIGBSZW5kZXJlckZhY3RvcnkyYC5cbiAqXG4gKiBVc2UgYSBjdXN0b20gcmVuZGVyZXIgdG8gYnlwYXNzIEFuZ3VsYXIncyB0ZW1wbGF0aW5nIGFuZFxuICogbWFrZSBjdXN0b20gVUkgY2hhbmdlcyB0aGF0IGNhbid0IGJlIGV4cHJlc3NlZCBkZWNsYXJhdGl2ZWx5LlxuICogRm9yIGV4YW1wbGUgaWYgeW91IG5lZWQgdG8gc2V0IGEgcHJvcGVydHkgb3IgYW4gYXR0cmlidXRlIHdob3NlIG5hbWUgaXNcbiAqIG5vdCBzdGF0aWNhbGx5IGtub3duLCB1c2UgdGhlIGBzZXRQcm9wZXJ0eSgpYCBvclxuICogYHNldEF0dHJpYnV0ZSgpYCBtZXRob2QuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgUmVuZGVyZXIyIHtcbiAgLyoqXG4gICAqIFVzZSB0byBzdG9yZSBhcmJpdHJhcnkgZGV2ZWxvcGVyLWRlZmluZWQgZGF0YSBvbiBhIHJlbmRlcmVyIGluc3RhbmNlLFxuICAgKiBhcyBhbiBvYmplY3QgY29udGFpbmluZyBrZXktdmFsdWUgcGFpcnMuXG4gICAqIFRoaXMgaXMgdXNlZnVsIGZvciByZW5kZXJlcnMgdGhhdCBkZWxlZ2F0ZSB0byBvdGhlciByZW5kZXJlcnMuXG4gICAqL1xuICBhYnN0cmFjdCBnZXQgZGF0YSgpOiB7W2tleTogc3RyaW5nXTogYW55fTtcblxuICAvKipcbiAgICogSW1wbGVtZW50IHRoaXMgY2FsbGJhY2sgdG8gZGVzdHJveSB0aGUgcmVuZGVyZXIgb3IgdGhlIGhvc3QgZWxlbWVudC5cbiAgICovXG4gIGFic3RyYWN0IGRlc3Ryb3koKTogdm9pZDtcbiAgLyoqXG4gICAqIEltcGxlbWVudCB0aGlzIGNhbGxiYWNrIHRvIGNyZWF0ZSBhbiBpbnN0YW5jZSBvZiB0aGUgaG9zdCBlbGVtZW50LlxuICAgKiBAcGFyYW0gbmFtZSBBbiBpZGVudGlmeWluZyBuYW1lIGZvciB0aGUgbmV3IGVsZW1lbnQsIHVuaXF1ZSB3aXRoaW4gdGhlIG5hbWVzcGFjZS5cbiAgICogQHBhcmFtIG5hbWVzcGFjZSBUaGUgbmFtZXNwYWNlIGZvciB0aGUgbmV3IGVsZW1lbnQuXG4gICAqIEByZXR1cm5zIFRoZSBuZXcgZWxlbWVudC5cbiAgICovXG4gIGFic3RyYWN0IGNyZWF0ZUVsZW1lbnQobmFtZTogc3RyaW5nLCBuYW1lc3BhY2U/OiBzdHJpbmd8bnVsbCk6IGFueTtcbiAgLyoqXG4gICAqIEltcGxlbWVudCB0aGlzIGNhbGxiYWNrIHRvIGFkZCBhIGNvbW1lbnQgdG8gdGhlIERPTSBvZiB0aGUgaG9zdCBlbGVtZW50LlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIGNvbW1lbnQgdGV4dC5cbiAgICogQHJldHVybnMgVGhlIG1vZGlmaWVkIGVsZW1lbnQuXG4gICAqL1xuICBhYnN0cmFjdCBjcmVhdGVDb21tZW50KHZhbHVlOiBzdHJpbmcpOiBhbnk7XG5cbiAgLyoqXG4gICAqIEltcGxlbWVudCB0aGlzIGNhbGxiYWNrIHRvIGFkZCB0ZXh0IHRvIHRoZSBET00gb2YgdGhlIGhvc3QgZWxlbWVudC5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSB0ZXh0IHN0cmluZy5cbiAgICogQHJldHVybnMgVGhlIG1vZGlmaWVkIGVsZW1lbnQuXG4gICAqL1xuICBhYnN0cmFjdCBjcmVhdGVUZXh0KHZhbHVlOiBzdHJpbmcpOiBhbnk7XG4gIC8qKlxuICAgKiBJZiBudWxsIG9yIHVuZGVmaW5lZCwgdGhlIHZpZXcgZW5naW5lIHdvbid0IGNhbGwgaXQuXG4gICAqIFRoaXMgaXMgdXNlZCBhcyBhIHBlcmZvcm1hbmNlIG9wdGltaXphdGlvbiBmb3IgcHJvZHVjdGlvbiBtb2RlLlxuICAgKi9cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIGRlc3Ryb3lOb2RlICE6ICgobm9kZTogYW55KSA9PiB2b2lkKSB8IG51bGw7XG4gIC8qKlxuICAgKiBBcHBlbmRzIGEgY2hpbGQgdG8gYSBnaXZlbiBwYXJlbnQgbm9kZSBpbiB0aGUgaG9zdCBlbGVtZW50IERPTS5cbiAgICogQHBhcmFtIHBhcmVudCBUaGUgcGFyZW50IG5vZGUuXG4gICAqIEBwYXJhbSBuZXdDaGlsZCBUaGUgbmV3IGNoaWxkIG5vZGUuXG4gICAqL1xuICBhYnN0cmFjdCBhcHBlbmRDaGlsZChwYXJlbnQ6IGFueSwgbmV3Q2hpbGQ6IGFueSk6IHZvaWQ7XG4gIC8qKlxuICAgKiBJbXBsZW1lbnQgdGhpcyBjYWxsYmFjayB0byBpbnNlcnQgYSBjaGlsZCBub2RlIGF0IGEgZ2l2ZW4gcG9zaXRpb24gaW4gYSBwYXJlbnQgbm9kZVxuICAgKiBpbiB0aGUgaG9zdCBlbGVtZW50IERPTS5cbiAgICogQHBhcmFtIHBhcmVudCBUaGUgcGFyZW50IG5vZGUuXG4gICAqIEBwYXJhbSBuZXdDaGlsZCBUaGUgbmV3IGNoaWxkIG5vZGVzLlxuICAgKiBAcGFyYW0gcmVmQ2hpbGQgVGhlIGV4aXN0aW5nIGNoaWxkIG5vZGUgdGhhdCBzaG91bGQgcHJlY2VkZSB0aGUgbmV3IG5vZGUuXG4gICAqL1xuICBhYnN0cmFjdCBpbnNlcnRCZWZvcmUocGFyZW50OiBhbnksIG5ld0NoaWxkOiBhbnksIHJlZkNoaWxkOiBhbnkpOiB2b2lkO1xuICAvKipcbiAgICogSW1wbGVtZW50IHRoaXMgY2FsbGJhY2sgdG8gcmVtb3ZlIGEgY2hpbGQgbm9kZSBmcm9tIHRoZSBob3N0IGVsZW1lbnQncyBET00uXG4gICAqIEBwYXJhbSBwYXJlbnQgVGhlIHBhcmVudCBub2RlLlxuICAgKiBAcGFyYW0gb2xkQ2hpbGQgVGhlIGNoaWxkIG5vZGUgdG8gcmVtb3ZlLlxuICAgKiBAcGFyYW0gaXNIb3N0RWxlbWVudCBPcHRpb25hbGx5IHNpZ25hbCB0byB0aGUgcmVuZGVyZXIgd2hldGhlciB0aGlzIGVsZW1lbnQgaXMgYSBob3N0IGVsZW1lbnRcbiAgICogb3Igbm90XG4gICAqL1xuICBhYnN0cmFjdCByZW1vdmVDaGlsZChwYXJlbnQ6IGFueSwgb2xkQ2hpbGQ6IGFueSwgaXNIb3N0RWxlbWVudD86IGJvb2xlYW4pOiB2b2lkO1xuICAvKipcbiAgICogSW1wbGVtZW50IHRoaXMgY2FsbGJhY2sgdG8gcHJlcGFyZSBhbiBlbGVtZW50IHRvIGJlIGJvb3RzdHJhcHBlZFxuICAgKiBhcyBhIHJvb3QgZWxlbWVudCwgYW5kIHJldHVybiB0aGUgZWxlbWVudCBpbnN0YW5jZS5cbiAgICogQHBhcmFtIHNlbGVjdG9yT3JOb2RlIFRoZSBET00gZWxlbWVudC5cbiAgICogQHBhcmFtIHByZXNlcnZlQ29udGVudCBXaGV0aGVyIHRoZSBjb250ZW50cyBvZiB0aGUgcm9vdCBlbGVtZW50XG4gICAqIHNob3VsZCBiZSBwcmVzZXJ2ZWQsIG9yIGNsZWFyZWQgdXBvbiBib290c3RyYXAgKGRlZmF1bHQgYmVoYXZpb3IpLlxuICAgKiBVc2Ugd2l0aCBgVmlld0VuY2Fwc3VsYXRpb24uU2hhZG93RG9tYCB0byBhbGxvdyBzaW1wbGUgbmF0aXZlXG4gICAqIGNvbnRlbnQgcHJvamVjdGlvbiB2aWEgYDxzbG90PmAgZWxlbWVudHMuXG4gICAqIEByZXR1cm5zIFRoZSByb290IGVsZW1lbnQuXG4gICAqL1xuICBhYnN0cmFjdCBzZWxlY3RSb290RWxlbWVudChzZWxlY3Rvck9yTm9kZTogc3RyaW5nfGFueSwgcHJlc2VydmVDb250ZW50PzogYm9vbGVhbik6IGFueTtcbiAgLyoqXG4gICAqIEltcGxlbWVudCB0aGlzIGNhbGxiYWNrIHRvIGdldCB0aGUgcGFyZW50IG9mIGEgZ2l2ZW4gbm9kZVxuICAgKiBpbiB0aGUgaG9zdCBlbGVtZW50J3MgRE9NLlxuICAgKiBAcGFyYW0gbm9kZSBUaGUgY2hpbGQgbm9kZSB0byBxdWVyeS5cbiAgICogQHJldHVybnMgVGhlIHBhcmVudCBub2RlLCBvciBudWxsIGlmIHRoZXJlIGlzIG5vIHBhcmVudC5cbiAgICogRm9yIFdlYldvcmtlcnMsIGFsd2F5cyByZXR1cm5zIHRydWUuXG4gICAqIFRoaXMgaXMgYmVjYXVzZSB0aGUgY2hlY2sgaXMgc3luY2hyb25vdXMsXG4gICAqIGFuZCB0aGUgY2FsbGVyIGNhbid0IHJlbHkgb24gY2hlY2tpbmcgZm9yIG51bGwuXG4gICAqL1xuICBhYnN0cmFjdCBwYXJlbnROb2RlKG5vZGU6IGFueSk6IGFueTtcbiAgLyoqXG4gICAqIEltcGxlbWVudCB0aGlzIGNhbGxiYWNrIHRvIGdldCB0aGUgbmV4dCBzaWJsaW5nIG5vZGUgb2YgYSBnaXZlbiBub2RlXG4gICAqIGluIHRoZSBob3N0IGVsZW1lbnQncyBET00uXG4gICAqIEByZXR1cm5zIFRoZSBzaWJsaW5nIG5vZGUsIG9yIG51bGwgaWYgdGhlcmUgaXMgbm8gc2libGluZy5cbiAgICogRm9yIFdlYldvcmtlcnMsIGFsd2F5cyByZXR1cm5zIGEgdmFsdWUuXG4gICAqIFRoaXMgaXMgYmVjYXVzZSB0aGUgY2hlY2sgaXMgc3luY2hyb25vdXMsXG4gICAqIGFuZCB0aGUgY2FsbGVyIGNhbid0IHJlbHkgb24gY2hlY2tpbmcgZm9yIG51bGwuXG4gICAqL1xuICBhYnN0cmFjdCBuZXh0U2libGluZyhub2RlOiBhbnkpOiBhbnk7XG4gIC8qKlxuICAgKiBJbXBsZW1lbnQgdGhpcyBjYWxsYmFjayB0byBzZXQgYW4gYXR0cmlidXRlIHZhbHVlIGZvciBhbiBlbGVtZW50IGluIHRoZSBET00uXG4gICAqIEBwYXJhbSBlbCBUaGUgZWxlbWVudC5cbiAgICogQHBhcmFtIG5hbWUgVGhlIGF0dHJpYnV0ZSBuYW1lLlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIG5ldyB2YWx1ZS5cbiAgICogQHBhcmFtIG5hbWVzcGFjZSBUaGUgbmFtZXNwYWNlLlxuICAgKi9cbiAgYWJzdHJhY3Qgc2V0QXR0cmlidXRlKGVsOiBhbnksIG5hbWU6IHN0cmluZywgdmFsdWU6IHN0cmluZywgbmFtZXNwYWNlPzogc3RyaW5nfG51bGwpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBJbXBsZW1lbnQgdGhpcyBjYWxsYmFjayB0byByZW1vdmUgYW4gYXR0cmlidXRlIGZyb20gYW4gZWxlbWVudCBpbiB0aGUgRE9NLlxuICAgKiBAcGFyYW0gZWwgVGhlIGVsZW1lbnQuXG4gICAqIEBwYXJhbSBuYW1lIFRoZSBhdHRyaWJ1dGUgbmFtZS5cbiAgICogQHBhcmFtIG5hbWVzcGFjZSBUaGUgbmFtZXNwYWNlLlxuICAgKi9cbiAgYWJzdHJhY3QgcmVtb3ZlQXR0cmlidXRlKGVsOiBhbnksIG5hbWU6IHN0cmluZywgbmFtZXNwYWNlPzogc3RyaW5nfG51bGwpOiB2b2lkO1xuICAvKipcbiAgICogSW1wbGVtZW50IHRoaXMgY2FsbGJhY2sgdG8gYWRkIGEgY2xhc3MgdG8gYW4gZWxlbWVudCBpbiB0aGUgRE9NLlxuICAgKiBAcGFyYW0gZWwgVGhlIGVsZW1lbnQuXG4gICAqIEBwYXJhbSBuYW1lIFRoZSBjbGFzcyBuYW1lLlxuICAgKi9cbiAgYWJzdHJhY3QgYWRkQ2xhc3MoZWw6IGFueSwgbmFtZTogc3RyaW5nKTogdm9pZDtcblxuICAvKipcbiAgICogSW1wbGVtZW50IHRoaXMgY2FsbGJhY2sgdG8gcmVtb3ZlIGEgY2xhc3MgZnJvbSBhbiBlbGVtZW50IGluIHRoZSBET00uXG4gICAqIEBwYXJhbSBlbCBUaGUgZWxlbWVudC5cbiAgICogQHBhcmFtIG5hbWUgVGhlIGNsYXNzIG5hbWUuXG4gICAqL1xuICBhYnN0cmFjdCByZW1vdmVDbGFzcyhlbDogYW55LCBuYW1lOiBzdHJpbmcpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBJbXBsZW1lbnQgdGhpcyBjYWxsYmFjayB0byBzZXQgYSBDU1Mgc3R5bGUgZm9yIGFuIGVsZW1lbnQgaW4gdGhlIERPTS5cbiAgICogQHBhcmFtIGVsIFRoZSBlbGVtZW50LlxuICAgKiBAcGFyYW0gc3R5bGUgVGhlIG5hbWUgb2YgdGhlIHN0eWxlLlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIG5ldyB2YWx1ZS5cbiAgICogQHBhcmFtIGZsYWdzIEZsYWdzIGZvciBzdHlsZSB2YXJpYXRpb25zLiBObyBmbGFncyBhcmUgc2V0IGJ5IGRlZmF1bHQuXG4gICAqL1xuICBhYnN0cmFjdCBzZXRTdHlsZShlbDogYW55LCBzdHlsZTogc3RyaW5nLCB2YWx1ZTogYW55LCBmbGFncz86IFJlbmRlcmVyU3R5bGVGbGFnczIpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBJbXBsZW1lbnQgdGhpcyBjYWxsYmFjayB0byByZW1vdmUgdGhlIHZhbHVlIGZyb20gYSBDU1Mgc3R5bGUgZm9yIGFuIGVsZW1lbnQgaW4gdGhlIERPTS5cbiAgICogQHBhcmFtIGVsIFRoZSBlbGVtZW50LlxuICAgKiBAcGFyYW0gc3R5bGUgVGhlIG5hbWUgb2YgdGhlIHN0eWxlLlxuICAgKiBAcGFyYW0gZmxhZ3MgRmxhZ3MgZm9yIHN0eWxlIHZhcmlhdGlvbnMgdG8gcmVtb3ZlLCBpZiBzZXQuID8/P1xuICAgKi9cbiAgYWJzdHJhY3QgcmVtb3ZlU3R5bGUoZWw6IGFueSwgc3R5bGU6IHN0cmluZywgZmxhZ3M/OiBSZW5kZXJlclN0eWxlRmxhZ3MyKTogdm9pZDtcblxuICAvKipcbiAgICogSW1wbGVtZW50IHRoaXMgY2FsbGJhY2sgdG8gc2V0IHRoZSB2YWx1ZSBvZiBhIHByb3BlcnR5IG9mIGFuIGVsZW1lbnQgaW4gdGhlIERPTS5cbiAgICogQHBhcmFtIGVsIFRoZSBlbGVtZW50LlxuICAgKiBAcGFyYW0gbmFtZSBUaGUgcHJvcGVydHkgbmFtZS5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSBuZXcgdmFsdWUuXG4gICAqL1xuICBhYnN0cmFjdCBzZXRQcm9wZXJ0eShlbDogYW55LCBuYW1lOiBzdHJpbmcsIHZhbHVlOiBhbnkpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBJbXBsZW1lbnQgdGhpcyBjYWxsYmFjayB0byBzZXQgdGhlIHZhbHVlIG9mIGEgbm9kZSBpbiB0aGUgaG9zdCBlbGVtZW50LlxuICAgKiBAcGFyYW0gbm9kZSBUaGUgbm9kZS5cbiAgICogQHBhcmFtIHZhbHVlIFRoZSBuZXcgdmFsdWUuXG4gICAqL1xuICBhYnN0cmFjdCBzZXRWYWx1ZShub2RlOiBhbnksIHZhbHVlOiBzdHJpbmcpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBJbXBsZW1lbnQgdGhpcyBjYWxsYmFjayB0byBzdGFydCBhbiBldmVudCBsaXN0ZW5lci5cbiAgICogQHBhcmFtIHRhcmdldCBUaGUgY29udGV4dCBpbiB3aGljaCB0byBsaXN0ZW4gZm9yIGV2ZW50cy4gQ2FuIGJlXG4gICAqIHRoZSBlbnRpcmUgd2luZG93IG9yIGRvY3VtZW50LCB0aGUgYm9keSBvZiB0aGUgZG9jdW1lbnQsIG9yIGEgc3BlY2lmaWNcbiAgICogRE9NIGVsZW1lbnQuXG4gICAqIEBwYXJhbSBldmVudE5hbWUgVGhlIGV2ZW50IHRvIGxpc3RlbiBmb3IuXG4gICAqIEBwYXJhbSBjYWxsYmFjayBBIGhhbmRsZXIgZnVuY3Rpb24gdG8gaW52b2tlIHdoZW4gdGhlIGV2ZW50IG9jY3Vycy5cbiAgICogQHJldHVybnMgQW4gXCJ1bmxpc3RlblwiIGZ1bmN0aW9uIGZvciBkaXNwb3Npbmcgb2YgdGhpcyBoYW5kbGVyLlxuICAgKi9cbiAgYWJzdHJhY3QgbGlzdGVuKFxuICAgICAgdGFyZ2V0OiAnd2luZG93J3wnZG9jdW1lbnQnfCdib2R5J3xhbnksIGV2ZW50TmFtZTogc3RyaW5nLFxuICAgICAgY2FsbGJhY2s6IChldmVudDogYW55KSA9PiBib29sZWFuIHwgdm9pZCk6ICgpID0+IHZvaWQ7XG5cbiAgLyoqXG4gICAqIEBpbnRlcm5hbFxuICAgKiBAbm9jb2xsYXBzZVxuICAgKi9cbiAgc3RhdGljIF9fTkdfRUxFTUVOVF9JRF9fOiAoKSA9PiBSZW5kZXJlcjIgPSAoKSA9PiBTV0lUQ0hfUkVOREVSRVIyX0ZBQ1RPUlkoKTtcbn1cblxuXG5leHBvcnQgY29uc3QgU1dJVENIX1JFTkRFUkVSMl9GQUNUT1JZX19QT1NUX1IzX18gPSByZW5kZXIzSW5qZWN0UmVuZGVyZXIyO1xuY29uc3QgU1dJVENIX1JFTkRFUkVSMl9GQUNUT1JZX19QUkVfUjNfXyA9IG5vb3A7XG5jb25zdCBTV0lUQ0hfUkVOREVSRVIyX0ZBQ1RPUlk6IHR5cGVvZiByZW5kZXIzSW5qZWN0UmVuZGVyZXIyID0gU1dJVENIX1JFTkRFUkVSMl9GQUNUT1JZX19QUkVfUjNfXztcbiJdfQ==