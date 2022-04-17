/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { DOCUMENT } from '@angular/common';
import { Inject, Injectable, InjectionToken, Optional, ɵConsole as Console } from '@angular/core';
import { EventManagerPlugin } from './event_manager';
/**
 * Supported HammerJS recognizer event names.
 */
var EVENT_NAMES = {
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
 * @publicApi
 */
export var HAMMER_GESTURE_CONFIG = new InjectionToken('HammerGestureConfig');
/**
 * Injection token used to provide a {@link HammerLoader} to Angular.
 *
 * @publicApi
 */
export var HAMMER_LOADER = new InjectionToken('HammerLoader');
/**
 * An injectable [HammerJS Manager](http://hammerjs.github.io/api/#hammer.manager)
 * for gesture recognition. Configures specific event recognition.
 * @publicApi
 */
var HammerGestureConfig = /** @class */ (function () {
    function HammerGestureConfig() {
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
     * @param element The element that will recognize gestures.
     * @returns A HammerJS event-manager object.
     */
    HammerGestureConfig.prototype.buildHammer = function (element) {
        var mc = new Hammer(element, this.options);
        mc.get('pinch').set({ enable: true });
        mc.get('rotate').set({ enable: true });
        for (var eventName in this.overrides) {
            mc.get(eventName).set(this.overrides[eventName]);
        }
        return mc;
    };
    HammerGestureConfig = tslib_1.__decorate([
        Injectable()
    ], HammerGestureConfig);
    return HammerGestureConfig;
}());
export { HammerGestureConfig };
var HammerGesturesPlugin = /** @class */ (function (_super) {
    tslib_1.__extends(HammerGesturesPlugin, _super);
    function HammerGesturesPlugin(doc, _config, console, loader) {
        var _this = _super.call(this, doc) || this;
        _this._config = _config;
        _this.console = console;
        _this.loader = loader;
        return _this;
    }
    HammerGesturesPlugin.prototype.supports = function (eventName) {
        if (!EVENT_NAMES.hasOwnProperty(eventName.toLowerCase()) && !this.isCustomEvent(eventName)) {
            return false;
        }
        if (!window.Hammer && !this.loader) {
            this.console.warn("The \"" + eventName + "\" event cannot be bound because Hammer.JS is not " +
                "loaded and no custom loader has been specified.");
            return false;
        }
        return true;
    };
    HammerGesturesPlugin.prototype.addEventListener = function (element, eventName, handler) {
        var _this = this;
        var zone = this.manager.getZone();
        eventName = eventName.toLowerCase();
        // If Hammer is not present but a loader is specified, we defer adding the event listener
        // until Hammer is loaded.
        if (!window.Hammer && this.loader) {
            // This `addEventListener` method returns a function to remove the added listener.
            // Until Hammer is loaded, the returned function needs to *cancel* the registration rather
            // than remove anything.
            var cancelRegistration_1 = false;
            var deregister_1 = function () { cancelRegistration_1 = true; };
            this.loader()
                .then(function () {
                // If Hammer isn't actually loaded when the custom loader resolves, give up.
                if (!window.Hammer) {
                    _this.console.warn("The custom HAMMER_LOADER completed, but Hammer.JS is not present.");
                    deregister_1 = function () { };
                    return;
                }
                if (!cancelRegistration_1) {
                    // Now that Hammer is loaded and the listener is being loaded for real,
                    // the deregistration function changes from canceling registration to removal.
                    deregister_1 = _this.addEventListener(element, eventName, handler);
                }
            })
                .catch(function () {
                _this.console.warn("The \"" + eventName + "\" event cannot be bound because the custom " +
                    "Hammer.JS loader failed.");
                deregister_1 = function () { };
            });
            // Return a function that *executes* `deregister` (and not `deregister` itself) so that we
            // can change the behavior of `deregister` once the listener is added. Using a closure in
            // this way allows us to avoid any additional data structures to track listener removal.
            return function () { deregister_1(); };
        }
        return zone.runOutsideAngular(function () {
            // Creating the manager bind events, must be done outside of angular
            var mc = _this._config.buildHammer(element);
            var callback = function (eventObj) {
                zone.runGuarded(function () { handler(eventObj); });
            };
            mc.on(eventName, callback);
            return function () {
                mc.off(eventName, callback);
                // destroy mc to prevent memory leak
                if (typeof mc.destroy === 'function') {
                    mc.destroy();
                }
            };
        });
    };
    HammerGesturesPlugin.prototype.isCustomEvent = function (eventName) { return this._config.events.indexOf(eventName) > -1; };
    HammerGesturesPlugin = tslib_1.__decorate([
        Injectable(),
        tslib_1.__param(0, Inject(DOCUMENT)),
        tslib_1.__param(1, Inject(HAMMER_GESTURE_CONFIG)),
        tslib_1.__param(3, Optional()), tslib_1.__param(3, Inject(HAMMER_LOADER)),
        tslib_1.__metadata("design:paramtypes", [Object, HammerGestureConfig, Console, Object])
    ], HammerGesturesPlugin);
    return HammerGesturesPlugin;
}(EventManagerPlugin));
export { HammerGesturesPlugin };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaGFtbWVyX2dlc3R1cmVzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci9zcmMvZG9tL2V2ZW50cy9oYW1tZXJfZ2VzdHVyZXMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQyxRQUFRLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUN6QyxPQUFPLEVBQUMsTUFBTSxFQUFFLFVBQVUsRUFBRSxjQUFjLEVBQUUsUUFBUSxFQUFFLFFBQVEsSUFBSSxPQUFPLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFaEcsT0FBTyxFQUFDLGtCQUFrQixFQUFDLE1BQU0saUJBQWlCLENBQUM7QUFFbkQ7O0dBRUc7QUFDSCxJQUFNLFdBQVcsR0FBRztJQUNsQixNQUFNO0lBQ04sS0FBSyxFQUFFLElBQUk7SUFDWCxVQUFVLEVBQUUsSUFBSTtJQUNoQixTQUFTLEVBQUUsSUFBSTtJQUNmLFFBQVEsRUFBRSxJQUFJO0lBQ2QsV0FBVyxFQUFFLElBQUk7SUFDakIsU0FBUyxFQUFFLElBQUk7SUFDZixVQUFVLEVBQUUsSUFBSTtJQUNoQixPQUFPLEVBQUUsSUFBSTtJQUNiLFNBQVMsRUFBRSxJQUFJO0lBQ2YsUUFBUTtJQUNSLE9BQU8sRUFBRSxJQUFJO0lBQ2IsWUFBWSxFQUFFLElBQUk7SUFDbEIsV0FBVyxFQUFFLElBQUk7SUFDakIsVUFBVSxFQUFFLElBQUk7SUFDaEIsYUFBYSxFQUFFLElBQUk7SUFDbkIsU0FBUyxFQUFFLElBQUk7SUFDZixVQUFVLEVBQUUsSUFBSTtJQUNoQixRQUFRO0lBQ1IsT0FBTyxFQUFFLElBQUk7SUFDYixTQUFTLEVBQUUsSUFBSTtJQUNmLFNBQVM7SUFDVCxRQUFRLEVBQUUsSUFBSTtJQUNkLGFBQWEsRUFBRSxJQUFJO0lBQ25CLFlBQVksRUFBRSxJQUFJO0lBQ2xCLFdBQVcsRUFBRSxJ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QUFRLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO1lBQ3JELENBQUMsQ0FBQztZQUNGLEVBQUUsQ0FBQyxFQUFFLENBQUMsU0FBUyxFQUFFLFFBQVEsQ0FBQyxDQUFDO1lBQzNCLE9BQU87Z0JBQ0wsRUFBRSxDQUFDLEdBQUcsQ0FBQyxTQUFTLEVBQUUsUUFBUSxDQUFDLENBQUM7Z0JBQzVCLG9DQUFvQztnQkFDcEMsSUFBSSxPQUFPLEVBQUUsQ0FBQyxPQUFPLEtBQUssVUFBVSxFQUFFO29CQUNwQyxFQUFFLENBQUMsT0FBTyxFQUFFLENBQUM7aUJBQ2Q7WUFDSCxDQUFDLENBQUM7UUFDSixDQUFDLENBQUMsQ0FBQztJQUNMLENBQUM7SUFFRCw0Q0FBYSxHQUFiLFVBQWMsU0FBaUIsSUFBYSxPQUFPLElBQUksQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLE9BQU8sQ0FBQyxTQUFTLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFsRnRGLG9CQUFvQjtRQURoQyxVQUFVLEVBQUU7UUFHTixtQkFBQSxNQUFNLENBQUMsUUFBUSxDQUFDLENBQUE7UUFDaEIsbUJBQUEsTUFBTSxDQUFDLHFCQUFxQixDQUFDLENBQUE7UUFDN0IsbUJBQUEsUUFBUSxFQUFFLENBQUEsRUFBRSxtQkFBQSxNQUFNLENBQUMsYUFBYSxDQUFDLENBQUE7eURBRGMsbUJBQW1CLEVBQW1CLE9BQU87T0FIdEYsb0JBQW9CLENBbUZoQztJQUFELDJCQUFDO0NBQUEsQUFuRkQsQ0FBMEMsa0JBQWtCLEdBbUYzRDtTQW5GWSxvQkFBb0IiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7RE9DVU1FTlR9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQge0luamVjdCwgSW5qZWN0YWJsZSwgSW5qZWN0aW9uVG9rZW4sIE9wdGlvbmFsLCDJtUNvbnNvbGUgYXMgQ29uc29sZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7RXZlbnRNYW5hZ2VyUGx1Z2lufSBmcm9tICcuL2V2ZW50X21hbmFnZXInO1xuXG4vKipcbiAqIFN1cHBvcnRlZCBIYW1tZXJKUyByZWNvZ25pemVyIGV2ZW50IG5hbWVzLlxuICovXG5jb25zdCBFVkVOVF9OQU1FUyA9IHtcbiAgLy8gcGFuXG4gICdwYW4nOiB0cnVlLFxuICAncGFuc3RhcnQnOiB0cnVlLFxuICAncGFubW92ZSc6IHRydWUsXG4gICdwYW5lbmQnOiB0cnVlLFxuICAncGFuY2FuY2VsJzogdHJ1ZSxcbiAgJ3BhbmxlZnQnOiB0cnVlLFxuICAncGFucmlnaHQnOiB0cnVlLFxuICAncGFudXAnOiB0cnVlLFxuICAncGFuZG93bic6IHRydWUsXG4gIC8vIHBpbmNoXG4gICdwaW5jaCc6IHRydWUsXG4gICdwaW5jaHN0YXJ0JzogdHJ1ZSxcbiAgJ3BpbmNobW92ZSc6IHRydWUsXG4gICdwaW5jaGVuZCc6IHRydWUsXG4gICdwaW5jaGNhbmNlbCc6IHRydWUsXG4gICdwaW5jaGluJzogdHJ1ZSxcbiAgJ3BpbmNob3V0JzogdHJ1ZSxcbiAgLy8gcHJlc3NcbiAgJ3ByZXNzJzogdHJ1ZSxcbiAgJ3ByZXNzdXAnOiB0cnVlLFxuICAvLyByb3RhdGVcbiAgJ3JvdGF0ZSc6IHRydWUsXG4gICdyb3RhdGVzdGFydCc6IHRydWUsXG4gICdyb3RhdGVtb3ZlJzogdHJ1ZSxcbiAgJ3JvdGF0ZWVuZCc6IHRydWUsXG4gICdyb3RhdGVjYW5jZWwnOiB0cnVlLFxuICAvLyBzd2lwZVxuICAnc3dpcGUnOiB0cnVlLFxuICAnc3dpcGVsZWZ0JzogdHJ1ZSxcbiAgJ3N3aXBlcmlnaHQnOiB0cnVlLFxuICAnc3dpcGV1cCc6IHRydWUsXG4gICdzd2lwZWRvd24nOiB0cnVlLFxuICAvLyB0YXBcbiAgJ3RhcCc6IHRydWUsXG59O1xuXG4vKipcbiAqIERJIHRva2VuIGZvciBwcm92aWRpbmcgW0hhbW1lckpTXShodHRwOi8vaGFtbWVyanMuZ2l0aHViLmlvLykgc3VwcG9ydCB0byBBbmd1bGFyLlxuICogQHNlZSBgSGFtbWVyR2VzdHVyZUNvbmZpZ2BcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBjb25zdCBIQU1NRVJfR0VTVFVSRV9DT05GSUcgPSBuZXcgSW5qZWN0aW9uVG9rZW48SGFtbWVyR2VzdHVyZUNvbmZpZz4oJ0hhbW1lckdlc3R1cmVDb25maWcnKTtcblxuXG4vKipcbiAqIEZ1bmN0aW9uIHRoYXQgbG9hZHMgSGFtbWVySlMsIHJldHVybmluZyBhIHByb21pc2UgdGhhdCBpcyByZXNvbHZlZCBvbmNlIEhhbW1lckpzIGlzIGxvYWRlZC5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCB0eXBlIEhhbW1lckxvYWRlciA9ICgpID0+IFByb21pc2U8dm9pZD47XG5cbi8qKlxuICogSW5qZWN0aW9uIHRva2VuIHVzZWQgdG8gcHJvdmlkZSBhIHtAbGluayBIYW1tZXJMb2FkZXJ9IHRvIEFuZ3VsYXIuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY29uc3QgSEFNTUVSX0xPQURFUiA9IG5ldyBJbmplY3Rpb25Ub2tlbjxIYW1tZXJMb2FkZXI+KCdIYW1tZXJMb2FkZXInKTtcblxuZXhwb3J0IGludGVyZmFjZSBIYW1tZXJJbnN0YW5jZSB7XG4gIG9uKGV2ZW50TmFtZTogc3RyaW5nLCBjYWxsYmFjaz86IEZ1bmN0aW9uKTogdm9pZDtcbiAgb2ZmKGV2ZW50TmFtZTogc3RyaW5nLCBjYWxsYmFjaz86IEZ1bmN0aW9uKTogdm9pZDtcbiAgZGVzdHJveT8oKTogdm9pZDtcbn1cblxuLyoqXG4gKiBBbiBpbmplY3RhYmxlIFtIYW1tZXJKUyBNYW5hZ2VyXShodHRwOi8vaGFtbWVyanMuZ2l0aHViLmlvL2FwaS8jaGFtbWVyLm1hbmFnZXIpXG4gKiBmb3IgZ2VzdHVyZSByZWNvZ25pdGlvbi4gQ29uZmlndXJlcyBzcGVjaWZpYyBldmVudCByZWNvZ25pdGlvbi5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIEhhbW1lckdlc3R1cmVDb25maWcge1xuICAvKipcbiAgICogQSBzZXQgb2Ygc3VwcG9ydGVkIGV2ZW50IG5hbWVzIGZvciBnZXN0dXJlcyB0byBiZSB1c2VkIGluIEFuZ3VsYXIuXG4gICAqIEFuZ3VsYXIgc3VwcG9ydHMgYWxsIGJ1aWx0LWluIHJlY29nbml6ZXJzLCBhcyBsaXN0ZWQgaW5cbiAgICogW0hhbW1lckpTIGRvY3VtZW50YXRpb25dKGh0dHA6Ly9oYW1tZXJqcy5naXRodWIuaW8vKS5cbiAgICovXG4gIGV2ZW50czogc3RyaW5nW10gPSBbXTtcblxuICAvKipcbiAgKiBNYXBzIGdlc3R1cmUgZXZlbnQgbmFtZXMgdG8gYSBzZXQgb2YgY29uZmlndXJhdGlvbiBvcHRpb25zXG4gICogdGhhdCBzcGVjaWZ5IG92ZXJyaWRlcyB0byB0aGUgZGVmYXVsdCB2YWx1ZXMgZm9yIHNwZWNpZmljIHByb3BlcnRpZXMuXG4gICpcbiAgKiBUaGUga2V5IGlzIGEgc3VwcG9ydGVkIGV2ZW50IG5hbWUgdG8gYmUgY29uZmlndXJlZCxcbiAgKiBhbmQgdGhlIG9wdGlvbnMgb2JqZWN0IGNvbnRhaW5zIGEgc2V0IG9mIHByb3BlcnRpZXMsIHdpdGggb3ZlcnJpZGUgdmFsdWVzXG4gICogdG8gYmUgYXBwbGllZCB0byB0aGUgbmFtZWQgcmVjb2duaXplciBldmVudC5cbiAgKiBGb3IgZXhhbXBsZSwgdG8gZGlzYWJsZSByZWNvZ25pdGlvbiBvZiB0aGUgcm90YXRlIGV2ZW50LCBzcGVjaWZ5XG4gICogIGB7XCJyb3RhdGVcIjoge1wiZW5hYmxlXCI6IGZhbHNlfX1gLlxuICAqXG4gICogUHJvcGVydGllcyB0aGF0IGFyZSBub3QgcHJlc2VudCB0YWtlIHRoZSBIYW1tZXJKUyBkZWZhdWx0IHZhbHVlcy5cbiAgKiBGb3IgaW5mb3JtYXRpb24gYWJvdXQgd2hpY2ggcHJvcGVydGllcyBhcmUgc3VwcG9ydGVkIGZvciB3aGljaCBldmVudHMsXG4gICogYW5kIHRoZWlyIGFsbG93ZWQgYW5kIGRlZmF1bHQgdmFsdWVzLCBzZWVcbiAgKiBbSGFtbWVySlMgZG9jdW1lbnRhdGlvbl0oaHR0cDovL2hhbW1lcmpzLmdpdGh1Yi5pby8pLlxuICAqXG4gICovXG4gIG92ZXJyaWRlczoge1trZXk6IHN0cmluZ106IE9iamVjdH0gPSB7fTtcblxuICAvKipcbiAgICogUHJvcGVydGllcyB3aG9zZSBkZWZhdWx0IHZhbHVlcyBjYW4gYmUgb3ZlcnJpZGRlbiBmb3IgYSBnaXZlbiBldmVudC5cbiAgICogRGlmZmVyZW50IHNldHMgb2YgcHJvcGVydGllcyBhcHBseSB0byBkaWZmZXJlbnQgZXZlbnRzLlxuICAgKiBGb3IgaW5mb3JtYXRpb24gYWJvdXQgd2hpY2ggcHJvcGVydGllcyBhcmUgc3VwcG9ydGVkIGZvciB3aGljaCBldmVudHMsXG4gICAqIGFuZCB0aGVpciBhbGxvd2VkIGFuZCBkZWZhdWx0IHZhbHVlcywgc2VlXG4gICAqIFtIYW1tZXJKUyBkb2N1bWVudGF0aW9uXShodHRwOi8vaGFtbWVyanMuZ2l0aHViLmlvLykuXG4gICAqL1xuICBvcHRpb25zPzoge1xuICAgIGNzc1Byb3BzPzogYW55OyBkb21FdmVudHM/OiBib29sZWFuOyBlbmFibGU/OiBib29sZWFuIHwgKChtYW5hZ2VyOiBhbnkpID0+IGJvb2xlYW4pO1xuICAgIHByZXNldD86IGFueVtdO1xuICAgIHRvdWNoQWN0aW9uPzogc3RyaW5nO1xuICAgIHJlY29nbml6ZXJzPzogYW55W107XG4gICAgaW5wdXRDbGFzcz86IGFueTtcbiAgICBpbnB1dFRhcmdldD86IEV2ZW50VGFyZ2V0O1xuICB9O1xuXG4gIC8qKlxuICAgKiBDcmVhdGVzIGEgW0hhbW1lckpTIE1hbmFnZXJdKGh0dHA6Ly9oYW1tZXJqcy5naXRodWIuaW8vYXBpLyNoYW1tZXIubWFuYWdlcilcbiAgICogYW5kIGF0dGFjaGVzIGl0IHRvIGEgZ2l2ZW4gSFRNTCBlbGVtZW50LlxuICAgKiBAcGFyYW0gZWxlbWVudCBUaGUgZWxlbWVudCB0aGF0IHdpbGwgcmVjb2duaXplIGdlc3R1cmVzLlxuICAgKiBAcmV0dXJucyBBIEhhbW1lckpTIGV2ZW50LW1hbmFnZXIgb2JqZWN0LlxuICAgKi9cbiAgYnVpbGRIYW1tZXIoZWxlbWVudDogSFRNTEVsZW1lbnQpOiBIYW1tZXJJbnN0YW5jZSB7XG4gICAgY29uc3QgbWMgPSBuZXcgSGFtbWVyICEoZWxlbWVudCwgdGhpcy5vcHRpb25zKTtcblxuICAgIG1jLmdldCgncGluY2gnKS5zZXQoe2VuYWJsZTogdHJ1ZX0pO1xuICAgIG1jLmdldCgncm90YXRlJykuc2V0KHtlbmFibGU6IHRydWV9KTtcblxuICAgIGZvciAoY29uc3QgZXZlbnROYW1lIGluIHRoaXMub3ZlcnJpZGVzKSB7XG4gICAgICBtYy5nZXQoZXZlbnROYW1lKS5zZXQodGhpcy5vdmVycmlkZXNbZXZlbnROYW1lXSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIG1jO1xuICB9XG59XG5cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBIYW1tZXJHZXN0dXJlc1BsdWdpbiBleHRlbmRzIEV2ZW50TWFuYWdlclBsdWdpbiB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgQEluamVjdChET0NVTUVOVCkgZG9jOiBhbnksXG4gICAgICBASW5qZWN0KEhBTU1FUl9HRVNUVVJFX0NPTkZJRykgcHJpdmF0ZSBfY29uZmlnOiBIYW1tZXJHZXN0dXJlQ29uZmlnLCBwcml2YXRlIGNvbnNvbGU6IENvbnNvbGUsXG4gICAgICBAT3B0aW9uYWwoKSBASW5qZWN0KEhBTU1FUl9MT0FERVIpIHByaXZhdGUgbG9hZGVyPzogSGFtbWVyTG9hZGVyfG51bGwpIHtcbiAgICBzdXBlcihkb2MpO1xuICB9XG5cbiAgc3VwcG9ydHMoZXZlbnROYW1lOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICBpZiAoIUVWRU5UX05BTUVTLmhhc093blByb3BlcnR5KGV2ZW50TmFtZS50b0xvd2VyQ2FzZSgpKSAmJiAhdGhpcy5pc0N1c3RvbUV2ZW50KGV2ZW50TmFtZSkpIHtcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG5cbiAgICBpZiAoISh3aW5kb3cgYXMgYW55KS5IYW1tZXIgJiYgIXRoaXMubG9hZGVyKSB7XG4gICAgICB0aGlzLmNvbnNvbGUud2FybihcbiAgICAgICAgICBgVGhlIFwiJHtldmVudE5hbWV9XCIgZXZlbnQgY2Fubm90IGJlIGJvdW5kIGJlY2F1c2UgSGFtbWVyLkpTIGlzIG5vdCBgICtcbiAgICAgICAgICBgbG9hZGVkIGFuZCBubyBjdXN0b20gbG9hZGVyIGhhcyBiZWVuIHNwZWNpZmllZC5gKTtcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgICB9XG5cbiAgICByZXR1cm4gdHJ1ZTtcbiAgfVxuXG4gIGFkZEV2ZW50TGlzdGVuZXIoZWxlbWVudDogSFRNTEVsZW1lbnQsIGV2ZW50TmFtZTogc3RyaW5nLCBoYW5kbGVyOiBGdW5jdGlvbik6IEZ1bmN0aW9uIHtcbiAgICBjb25zdCB6b25lID0gdGhpcy5tYW5hZ2VyLmdldFpvbmUoKTtcbiAgICBldmVudE5hbWUgPSBldmVudE5hbWUudG9Mb3dlckNhc2UoKTtcblxuICAgIC8vIElmIEhhbW1lciBpcyBub3QgcHJlc2VudCBidXQgYSBsb2FkZXIgaXMgc3BlY2lmaWVkLCB3ZSBkZWZlciBhZGRpbmcgdGhlIGV2ZW50IGxpc3RlbmVyXG4gICAgLy8gdW50aWwgSGFtbWVyIGlzIGxvYWRlZC5cbiAgICBpZiAoISh3aW5kb3cgYXMgYW55KS5IYW1tZXIgJiYgdGhpcy5sb2FkZXIpIHtcbiAgICAgIC8vIFRoaXMgYGFkZEV2ZW50TGlzdGVuZXJgIG1ldGhvZCByZXR1cm5zIGEgZnVuY3Rpb24gdG8gcmVtb3ZlIHRoZSBhZGRlZCBsaXN0ZW5lci5cbiAgICAgIC8vIFVudGlsIEhhbW1lciBpcyBsb2FkZWQsIHRoZSByZXR1cm5lZCBmdW5jdGlvbiBuZWVkcyB0byAqY2FuY2VsKiB0aGUgcmVnaXN0cmF0aW9uIHJhdGhlclxuICAgICAgLy8gdGhhbiByZW1vdmUgYW55dGhpbmcuXG4gICAgICBsZXQgY2FuY2VsUmVnaXN0cmF0aW9uID0gZmFsc2U7XG4gICAgICBsZXQgZGVyZWdpc3RlcjogRnVuY3Rpb24gPSAoKSA9PiB7IGNhbmNlbFJlZ2lzdHJhdGlvbiA9IHRydWU7IH07XG5cbiAgICAgIHRoaXMubG9hZGVyKClcbiAgICAgICAgICAudGhlbigoKSA9PiB7XG4gICAgICAgICAgICAvLyBJZiBIYW1tZXIgaXNuJ3QgYWN0dWFsbHkgbG9hZGVkIHdoZW4gdGhlIGN1c3RvbSBsb2FkZXIgcmVzb2x2ZXMsIGdpdmUgdXAuXG4gICAgICAgICAgICBpZiAoISh3aW5kb3cgYXMgYW55KS5IYW1tZXIpIHtcbiAgICAgICAgICAgICAgdGhpcy5jb25zb2xlLndhcm4oXG4gICAgICAgICAgICAgICAgICBgVGhlIGN1c3RvbSBIQU1NRVJfTE9BREVSIGNvbXBsZXRlZCwgYnV0IEhhbW1lci5KUyBpcyBub3QgcHJlc2VudC5gKTtcbiAgICAgICAgICAgICAgZGVyZWdpc3RlciA9ICgpID0+IHt9O1xuICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICB9XG5cbiAgICAgICAgICAgIGlmICghY2FuY2VsUmVnaXN0cmF0aW9uKSB7XG4gICAgICAgICAgICAgIC8vIE5vdyB0aGF0IEhhbW1lciBpcyBsb2FkZWQgYW5kIHRoZSBsaXN0ZW5lciBpcyBiZWluZyBsb2FkZWQgZm9yIHJlYWwsXG4gICAgICAgICAgICAgIC8vIHRoZSBkZXJlZ2lzdHJhdGlvbiBmdW5jdGlvbiBjaGFuZ2VzIGZyb20gY2FuY2VsaW5nIHJlZ2lzdHJhdGlvbiB0byByZW1vdmFsLlxuICAgICAgICAgICAgICBkZXJlZ2lzdGVyID0gdGhpcy5hZGRFdmVudExpc3RlbmVyKGVsZW1lbnQsIGV2ZW50TmFtZSwgaGFuZGxlcik7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfSlcbiAgICAgICAgICAuY2F0Y2goKCkgPT4ge1xuICAgICAgICAgICAgdGhpcy5jb25zb2xlLndhcm4oXG4gICAgICAgICAgICAgICAgYFRoZSBcIiR7ZXZlbnROYW1lfVwiIGV2ZW50IGNhbm5vdCBiZSBib3VuZCBiZWNhdXNlIHRoZSBjdXN0b20gYCArXG4gICAgICAgICAgICAgICAgYEhhbW1lci5KUyBsb2FkZXIgZmFpbGVkLmApO1xuICAgICAgICAgICAgZGVyZWdpc3RlciA9ICgpID0+IHt9O1xuICAgICAgICAgIH0pO1xuXG4gICAgICAvLyBSZXR1cm4gYSBmdW5jdGlvbiB0aGF0ICpleGVjdXRlcyogYGRlcmVnaXN0ZXJgIChhbmQgbm90IGBkZXJlZ2lzdGVyYCBpdHNlbGYpIHNvIHRoYXQgd2VcbiAgICAgIC8vIGNhbiBjaGFuZ2UgdGhlIGJlaGF2aW9yIG9mIGBkZXJlZ2lzdGVyYCBvbmNlIHRoZSBsaXN0ZW5lciBpcyBhZGRlZC4gVXNpbmcgYSBjbG9zdXJlIGluXG4gICAgICAvLyB0aGlzIHdheSBhbGxvd3MgdXMgdG8gYXZvaWQgYW55IGFkZGl0aW9uYWwgZGF0YSBzdHJ1Y3R1cmVzIHRvIHRyYWNrIGxpc3RlbmVyIHJlbW92YWwuXG4gICAgICByZXR1cm4gKCkgPT4geyBkZXJlZ2lzdGVyKCk7IH07XG4gICAgfVxuXG4gICAgcmV0dXJuIHpvbmUucnVuT3V0c2lkZUFuZ3VsYXIoKCkgPT4ge1xuICAgICAgLy8gQ3JlYXRpbmcgdGhlIG1hbmFnZXIgYmluZCBldmVudHMsIG11c3QgYmUgZG9uZSBvdXRzaWRlIG9mIGFuZ3VsYXJcbiAgICAgIGNvbnN0IG1jID0gdGhpcy5fY29uZmlnLmJ1aWxkSGFtbWVyKGVsZW1lbnQpO1xuICAgICAgY29uc3QgY2FsbGJhY2sgPSBmdW5jdGlvbihldmVudE9iajogSGFtbWVySW5wdXQpIHtcbiAgICAgICAgem9uZS5ydW5HdWFyZGVkKGZ1bmN0aW9uKCkgeyBoYW5kbGVyKGV2ZW50T2JqKTsgfSk7XG4gICAgICB9O1xuICAgICAgbWMub24oZXZlbnROYW1lLCBjYWxsYmFjayk7XG4gICAgICByZXR1cm4gKCkgPT4ge1xuICAgICAgICBtYy5vZmYoZXZlbnROYW1lLCBjYWxsYmFjayk7XG4gICAgICAgIC8vIGRlc3Ryb3kgbWMgdG8gcHJldmVudCBtZW1vcnkgbGVha1xuICAgICAgICBpZiAodHlwZW9mIG1jLmRlc3Ryb3kgPT09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgICBtYy5kZXN0cm95KCk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgfSk7XG4gIH1cblxuICBpc0N1c3RvbUV2ZW50KGV2ZW50TmFtZTogc3RyaW5nKTogYm9vbGVhbiB7IHJldHVybiB0aGlzLl9jb25maWcuZXZlbnRzLmluZGV4T2YoZXZlbnROYW1lKSA+IC0xOyB9XG59XG4iXX0=