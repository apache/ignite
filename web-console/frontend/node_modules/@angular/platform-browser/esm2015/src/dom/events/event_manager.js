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
import { Inject, Injectable, InjectionToken, NgZone } from '@angular/core';
import { getDOM } from '../dom_adapter';
/**
 * The injection token for the event-manager plug-in service.
 *
 * \@publicApi
 * @type {?}
 */
export const EVENT_MANAGER_PLUGINS = new InjectionToken('EventManagerPlugins');
/**
 * An injectable service that provides event management for Angular
 * through a browser plug-in.
 *
 * \@publicApi
 */
export class EventManager {
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
export class EventManagerPlugin {
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXZlbnRfbWFuYWdlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXIvc3JjL2RvbS9ldmVudHMvZXZlbnRfbWFuYWdlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxNQUFNLEVBQUUsVUFBVSxFQUFFLGNBQWMsRUFBRSxNQUFNLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFekUsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLGdCQUFnQixDQUFDOzs7Ozs7O0FBT3RDLE1BQU0sT0FBTyxxQkFBcUIsR0FDOUIsSUFBSSxjQUFjLENBQXVCLHFCQUFxQixDQUFDOzs7Ozs7O0FBU25FLE1BQU0sT0FBTyxZQUFZOzs7Ozs7SUFPdkIsWUFBMkMsT0FBNkIsRUFBVSxLQUFhO1FBQWIsVUFBSyxHQUFMLEtBQUssQ0FBUTtRQUx2Rix1QkFBa0IsR0FBRyxJQUFJLEdBQUcsRUFBOEIsQ0FBQztRQU1qRSxPQUFPLENBQUMsT0FBTzs7Ozs7O1FBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsT0FBTyxHQUFHLElBQUksRUFBQyxDQUFDO1FBQ3ZDLElBQUksQ0FBQyxRQUFRLEdBQUcsT0FBTyxDQUFDLEtBQUssRUFBRSxDQUFDLE9BQU8sRUFBRSxDQUFDO0lBQzVDLENBQUM7Ozs7Ozs7Ozs7SUFXRCxnQkFBZ0IsQ0FBQyxPQUFvQixFQUFFLFNBQWlCLEVBQUUsT0FBaUI7O2NBQ25FLE1BQU0sR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLFNBQVMsQ0FBQztRQUM3QyxPQUFPLE1BQU0sQ0FBQyxnQkFBZ0IsQ0FBQyxPQUFPLEVBQUUsU0FBUyxFQUFFLE9BQU8sQ0FBQyxDQUFDO0lBQzlELENBQUM7Ozs7Ozs7Ozs7SUFXRCxzQkFBc0IsQ0FBQyxNQUFjLEVBQUUsU0FBaUIsRUFBRSxPQUFpQjs7Y0FDbkUsTUFBTSxHQUFHLElBQUksQ0FBQyxjQUFjLENBQUMsU0FBUyxDQUFDO1FBQzdDLE9BQU8sTUFBTSxDQUFDLHNCQUFzQixDQUFDLE1BQU0sRUFBRSxTQUFTLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDbkUsQ0FBQzs7Ozs7SUFLRCxPQUFPLEtBQWEsT0FBTyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQzs7Ozs7O0lBR3hDLGNBQWMsQ0FBQyxTQUFpQjs7Y0FDeEIsTUFBTSxHQUFHLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxHQUFHLENBQUMsU0FBUyxDQUFDO1FBQ3JELElBQUksTUFBTSxFQUFFO1lBQ1YsT0FBTyxNQUFNLENBQUM7U0FDZjs7Y0FFSyxPQUFPLEdBQUcsSUFBSSxDQUFDLFFBQVE7UUFDN0IsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLE9BQU8sQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLEVBQUU7O2tCQUNqQyxNQUFNLEdBQUcsT0FBTyxDQUFDLENBQUMsQ0FBQztZQUN6QixJQUFJLE1BQU0sQ0FBQyxRQUFRLENBQUMsU0FBUyxDQUFDLEVBQUU7Z0JBQzlCLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxHQUFHLENBQUMsU0FBUyxFQUFFLE1BQU0sQ0FBQyxDQUFDO2dCQUMvQyxPQUFPLE1BQU0sQ0FBQzthQUNmO1NBQ0Y7UUFDRCxNQUFNLElBQUksS0FBSyxDQUFDLDJDQUEyQyxTQUFTLEVBQUUsQ0FBQyxDQUFDO0lBQzFFLENBQUM7OztZQTlERixVQUFVOzs7O3dDQVFJLE1BQU0sU0FBQyxxQkFBcUI7WUExQkMsTUFBTTs7Ozs7OztJQW9CaEQsZ0NBQXVDOzs7OztJQUN2QywwQ0FBbUU7Ozs7O0lBS08sNkJBQXFCOzs7OztBQXlEakcsTUFBTSxPQUFnQixrQkFBa0I7Ozs7SUFDdEMsWUFBb0IsSUFBUztRQUFULFNBQUksR0FBSixJQUFJLENBQUs7SUFBRyxDQUFDOzs7Ozs7O0lBU2pDLHNCQUFzQixDQUFDLE9BQWUsRUFBRSxTQUFpQixFQUFFLE9BQWlCOztjQUNwRSxNQUFNLEdBQWdCLE1BQU0sRUFBRSxDQUFDLG9CQUFvQixDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDO1FBQzdFLElBQUksQ0FBQyxNQUFNLEVBQUU7WUFDWCxNQUFNLElBQUksS0FBSyxDQUFDLDRCQUE0QixNQUFNLGNBQWMsU0FBUyxFQUFFLENBQUMsQ0FBQztTQUM5RTtRQUNELE9BQU8sSUFBSSxDQUFDLGdCQUFnQixDQUFDLE1BQU0sRUFBRSxTQUFTLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDM0QsQ0FBQztDQUNGOzs7SUFiQyxxQ0FBd0I7Ozs7O0lBSFosa0NBQWlCOzs7Ozs7SUFLN0IsaUVBQThDOzs7Ozs7OztJQUU5QywyRkFBZ0ciLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0LCBJbmplY3RhYmxlLCBJbmplY3Rpb25Ub2tlbiwgTmdab25lfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtnZXRET019IGZyb20gJy4uL2RvbV9hZGFwdGVyJztcblxuLyoqXG4gKiBUaGUgaW5qZWN0aW9uIHRva2VuIGZvciB0aGUgZXZlbnQtbWFuYWdlciBwbHVnLWluIHNlcnZpY2UuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY29uc3QgRVZFTlRfTUFOQUdFUl9QTFVHSU5TID1cbiAgICBuZXcgSW5qZWN0aW9uVG9rZW48RXZlbnRNYW5hZ2VyUGx1Z2luW10+KCdFdmVudE1hbmFnZXJQbHVnaW5zJyk7XG5cbi8qKlxuICogQW4gaW5qZWN0YWJsZSBzZXJ2aWNlIHRoYXQgcHJvdmlkZXMgZXZlbnQgbWFuYWdlbWVudCBmb3IgQW5ndWxhclxuICogdGhyb3VnaCBhIGJyb3dzZXIgcGx1Zy1pbi5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBFdmVudE1hbmFnZXIge1xuICBwcml2YXRlIF9wbHVnaW5zOiBFdmVudE1hbmFnZXJQbHVnaW5bXTtcbiAgcHJpdmF0ZSBfZXZlbnROYW1lVG9QbHVnaW4gPSBuZXcgTWFwPHN0cmluZywgRXZlbnRNYW5hZ2VyUGx1Z2luPigpO1xuXG4gIC8qKlxuICAgKiBJbml0aWFsaXplcyBhbiBpbnN0YW5jZSBvZiB0aGUgZXZlbnQtbWFuYWdlciBzZXJ2aWNlLlxuICAgKi9cbiAgY29uc3RydWN0b3IoQEluamVjdChFVkVOVF9NQU5BR0VSX1BMVUdJTlMpIHBsdWdpbnM6IEV2ZW50TWFuYWdlclBsdWdpbltdLCBwcml2YXRlIF96b25lOiBOZ1pvbmUpIHtcbiAgICBwbHVnaW5zLmZvckVhY2gocCA9PiBwLm1hbmFnZXIgPSB0aGlzKTtcbiAgICB0aGlzLl9wbHVnaW5zID0gcGx1Z2lucy5zbGljZSgpLnJldmVyc2UoKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgYSBoYW5kbGVyIGZvciBhIHNwZWNpZmljIGVsZW1lbnQgYW5kIGV2ZW50LlxuICAgKlxuICAgKiBAcGFyYW0gZWxlbWVudCBUaGUgSFRNTCBlbGVtZW50IHRvIHJlY2VpdmUgZXZlbnQgbm90aWZpY2F0aW9ucy5cbiAgICogQHBhcmFtIGV2ZW50TmFtZSBUaGUgbmFtZSBvZiB0aGUgZXZlbnQgdG8gbGlzdGVuIGZvci5cbiAgICogQHBhcmFtIGhhbmRsZXIgQSBmdW5jdGlvbiB0byBjYWxsIHdoZW4gdGhlIG5vdGlmaWNhdGlvbiBvY2N1cnMuIFJlY2VpdmVzIHRoZVxuICAgKiBldmVudCBvYmplY3QgYXMgYW4gYXJndW1lbnQuXG4gICAqIEByZXR1cm5zICBBIGNhbGxiYWNrIGZ1bmN0aW9uIHRoYXQgY2FuIGJlIHVzZWQgdG8gcmVtb3ZlIHRoZSBoYW5kbGVyLlxuICAgKi9cbiAgYWRkRXZlbnRMaXN0ZW5lcihlbGVtZW50OiBIVE1MRWxlbWVudCwgZXZlbnROYW1lOiBzdHJpbmcsIGhhbmRsZXI6IEZ1bmN0aW9uKTogRnVuY3Rpb24ge1xuICAgIGNvbnN0IHBsdWdpbiA9IHRoaXMuX2ZpbmRQbHVnaW5Gb3IoZXZlbnROYW1lKTtcbiAgICByZXR1cm4gcGx1Z2luLmFkZEV2ZW50TGlzdGVuZXIoZWxlbWVudCwgZXZlbnROYW1lLCBoYW5kbGVyKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgYSBnbG9iYWwgaGFuZGxlciBmb3IgYW4gZXZlbnQgaW4gYSB0YXJnZXQgdmlldy5cbiAgICpcbiAgICogQHBhcmFtIHRhcmdldCBBIHRhcmdldCBmb3IgZ2xvYmFsIGV2ZW50IG5vdGlmaWNhdGlvbnMuIE9uZSBvZiBcIndpbmRvd1wiLCBcImRvY3VtZW50XCIsIG9yIFwiYm9keVwiLlxuICAgKiBAcGFyYW0gZXZlbnROYW1lIFRoZSBuYW1lIG9mIHRoZSBldmVudCB0byBsaXN0ZW4gZm9yLlxuICAgKiBAcGFyYW0gaGFuZGxlciBBIGZ1bmN0aW9uIHRvIGNhbGwgd2hlbiB0aGUgbm90aWZpY2F0aW9uIG9jY3Vycy4gUmVjZWl2ZXMgdGhlXG4gICAqIGV2ZW50IG9iamVjdCBhcyBhbiBhcmd1bWVudC5cbiAgICogQHJldHVybnMgQSBjYWxsYmFjayBmdW5jdGlvbiB0aGF0IGNhbiBiZSB1c2VkIHRvIHJlbW92ZSB0aGUgaGFuZGxlci5cbiAgICovXG4gIGFkZEdsb2JhbEV2ZW50TGlzdGVuZXIodGFyZ2V0OiBzdHJpbmcsIGV2ZW50TmFtZTogc3RyaW5nLCBoYW5kbGVyOiBGdW5jdGlvbik6IEZ1bmN0aW9uIHtcbiAgICBjb25zdCBwbHVnaW4gPSB0aGlzLl9maW5kUGx1Z2luRm9yKGV2ZW50TmFtZSk7XG4gICAgcmV0dXJuIHBsdWdpbi5hZGRHbG9iYWxFdmVudExpc3RlbmVyKHRhcmdldCwgZXZlbnROYW1lLCBoYW5kbGVyKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXRyaWV2ZXMgdGhlIGNvbXBpbGF0aW9uIHpvbmUgaW4gd2hpY2ggZXZlbnQgbGlzdGVuZXJzIGFyZSByZWdpc3RlcmVkLlxuICAgKi9cbiAgZ2V0Wm9uZSgpOiBOZ1pvbmUgeyByZXR1cm4gdGhpcy5fem9uZTsgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2ZpbmRQbHVnaW5Gb3IoZXZlbnROYW1lOiBzdHJpbmcpOiBFdmVudE1hbmFnZXJQbHVnaW4ge1xuICAgIGNvbnN0IHBsdWdpbiA9IHRoaXMuX2V2ZW50TmFtZVRvUGx1Z2luLmdldChldmVudE5hbWUpO1xuICAgIGlmIChwbHVnaW4pIHtcbiAgICAgIHJldHVybiBwbHVnaW47XG4gICAgfVxuXG4gICAgY29uc3QgcGx1Z2lucyA9IHRoaXMuX3BsdWdpbnM7XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBwbHVnaW5zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBjb25zdCBwbHVnaW4gPSBwbHVnaW5zW2ldO1xuICAgICAgaWYgKHBsdWdpbi5zdXBwb3J0cyhldmVudE5hbWUpKSB7XG4gICAgICAgIHRoaXMuX2V2ZW50TmFtZVRvUGx1Z2luLnNldChldmVudE5hbWUsIHBsdWdpbik7XG4gICAgICAgIHJldHVybiBwbHVnaW47XG4gICAgICB9XG4gICAgfVxuICAgIHRocm93IG5ldyBFcnJvcihgTm8gZXZlbnQgbWFuYWdlciBwbHVnaW4gZm91bmQgZm9yIGV2ZW50ICR7ZXZlbnROYW1lfWApO1xuICB9XG59XG5cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBFdmVudE1hbmFnZXJQbHVnaW4ge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF9kb2M6IGFueSkge31cblxuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgbWFuYWdlciAhOiBFdmVudE1hbmFnZXI7XG5cbiAgYWJzdHJhY3Qgc3VwcG9ydHMoZXZlbnROYW1lOiBzdHJpbmcpOiBib29sZWFuO1xuXG4gIGFic3RyYWN0IGFkZEV2ZW50TGlzdGVuZXIoZWxlbWVudDogSFRNTEVsZW1lbnQsIGV2ZW50TmFtZTogc3RyaW5nLCBoYW5kbGVyOiBGdW5jdGlvbik6IEZ1bmN0aW9uO1xuXG4gIGFkZEdsb2JhbEV2ZW50TGlzdGVuZXIoZWxlbWVudDogc3RyaW5nLCBldmVudE5hbWU6IHN0cmluZywgaGFuZGxlcjogRnVuY3Rpb24pOiBGdW5jdGlvbiB7XG4gICAgY29uc3QgdGFyZ2V0OiBIVE1MRWxlbWVudCA9IGdldERPTSgpLmdldEdsb2JhbEV2ZW50VGFyZ2V0KHRoaXMuX2RvYywgZWxlbWVudCk7XG4gICAgaWYgKCF0YXJnZXQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgVW5zdXBwb3J0ZWQgZXZlbnQgdGFyZ2V0ICR7dGFyZ2V0fSBmb3IgZXZlbnQgJHtldmVudE5hbWV9YCk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLmFkZEV2ZW50TGlzdGVuZXIodGFyZ2V0LCBldmVudE5hbWUsIGhhbmRsZXIpO1xuICB9XG59XG4iXX0=