/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { DOCUMENT } from '@angular/common';
import { Inject, Injectable } from '@angular/core';
import { getDOM } from '../dom_adapter';
import { EventManagerPlugin } from './event_manager';
/**
 * Defines supported modifiers for key events.
 */
var MODIFIER_KEYS = ['alt', 'control', 'meta', 'shift'];
var ɵ0 = function (event) { return event.altKey; }, ɵ1 = function (event) { return event.ctrlKey; }, ɵ2 = function (event) { return event.metaKey; }, ɵ3 = function (event) { return event.shiftKey; };
/**
 * Retrieves modifiers from key-event objects.
 */
var MODIFIER_KEY_GETTERS = {
    'alt': ɵ0,
    'control': ɵ1,
    'meta': ɵ2,
    'shift': ɵ3
};
/**
 * @publicApi
 * A browser plug-in that provides support for handling of key events in Angular.
 */
var KeyEventsPlugin = /** @class */ (function (_super) {
    tslib_1.__extends(KeyEventsPlugin, _super);
    /**
     * Initializes an instance of the browser plug-in.
     * @param doc The document in which key events will be detected.
     */
    function KeyEventsPlugin(doc) {
        return _super.call(this, doc) || this;
    }
    KeyEventsPlugin_1 = KeyEventsPlugin;
    /**
      * Reports whether a named key event is supported.
      * @param eventName The event name to query.
      * @return True if the named key event is supported.
     */
    KeyEventsPlugin.prototype.supports = function (eventName) { return KeyEventsPlugin_1.parseEventName(eventName) != null; };
    /**
     * Registers a handler for a specific element and key event.
     * @param element The HTML element to receive event notifications.
     * @param eventName The name of the key event to listen for.
     * @param handler A function to call when the notification occurs. Receives the
     * event object as an argument.
     * @returns The key event that was registered.
    */
    KeyEventsPlugin.prototype.addEventListener = function (element, eventName, handler) {
        var parsedEvent = KeyEventsPlugin_1.parseEventName(eventName);
        var outsideHandler = KeyEventsPlugin_1.eventCallback(parsedEvent['fullKey'], handler, this.manager.getZone());
        return this.manager.getZone().runOutsideAngular(function () {
            return getDOM().onAndCancel(element, parsedEvent['domEventName'], outsideHandler);
        });
    };
    KeyEventsPlugin.parseEventName = function (eventName) {
        var parts = eventName.toLowerCase().split('.');
        var domEventName = parts.shift();
        if ((parts.length === 0) || !(domEventName === 'keydown' || domEventName === 'keyup')) {
            return null;
        }
        var key = KeyEventsPlugin_1._normalizeKey(parts.pop());
        var fullKey = '';
        MODIFIER_KEYS.forEach(function (modifierName) {
            var index = parts.indexOf(modifierName);
            if (index > -1) {
                parts.splice(index, 1);
                fullKey += modifierName + '.';
            }
        });
        fullKey += key;
        if (parts.length != 0 || key.length === 0) {
            // returning null instead of throwing to let another plugin process the event
            return null;
        }
        var result = {};
        result['domEventName'] = domEventName;
        result['fullKey'] = fullKey;
        return result;
    };
    KeyEventsPlugin.getEventFullKey = function (event) {
        var fullKey = '';
        var key = getDOM().getEventKey(event);
        key = key.toLowerCase();
        if (key === ' ') {
            key = 'space'; // for readability
        }
        else if (key === '.') {
            key = 'dot'; // because '.' is used as a separator in event names
        }
        MODIFIER_KEYS.forEach(function (modifierName) {
            if (modifierName != key) {
                var modifierGetter = MODIFIER_KEY_GETTERS[modifierName];
                if (modifierGetter(event)) {
                    fullKey += modifierName + '.';
                }
            }
        });
        fullKey += key;
        return fullKey;
    };
    /**
     * Configures a handler callback for a key event.
     * @param fullKey The event name that combines all simultaneous keystrokes.
     * @param handler The function that responds to the key event.
     * @param zone The zone in which the event occurred.
     * @returns A callback function.
     */
    KeyEventsPlugin.eventCallback = function (fullKey, handler, zone) {
        return function (event /** TODO #9100 */) {
            if (KeyEventsPlugin_1.getEventFullKey(event) === fullKey) {
                zone.runGuarded(function () { return handler(event); });
            }
        };
    };
    /** @internal */
    KeyEventsPlugin._normalizeKey = function (keyName) {
        // TODO: switch to a Map if the mapping grows too much
        switch (keyName) {
            case 'esc':
                return 'escape';
            default:
                return keyName;
        }
    };
    var KeyEventsPlugin_1;
    KeyEventsPlugin = KeyEventsPlugin_1 = tslib_1.__decorate([
        Injectable(),
        tslib_1.__param(0, Inject(DOCUMENT)),
        tslib_1.__metadata("design:paramtypes", [Object])
    ], KeyEventsPlugin);
    return KeyEventsPlugin;
}(EventManagerPlugin));
export { KeyEventsPlugin };
export { ɵ0, ɵ1, ɵ2, ɵ3 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoia2V5X2V2ZW50cy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXIvc3JjL2RvbS9ldmVudHMva2V5X2V2ZW50cy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGlCQUFpQixDQUFDO0FBQ3pDLE9BQU8sRUFBQyxNQUFNLEVBQUUsVUFBVSxFQUFTLE1BQU0sZUFBZSxDQUFDO0FBRXpELE9BQU8sRUFBQyxNQUFNLEVBQUMsTUFBTSxnQkFBZ0IsQ0FBQztBQUV0QyxPQUFPLEVBQUMsa0JBQWtCLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUVuRDs7R0FFRztBQUNILElBQU0sYUFBYSxHQUFHLENBQUMsS0FBSyxFQUFFLFNBQVMsRUFBRSxNQUFNLEVBQUUsT0FBTyxDQUFDLENBQUM7U0FNakQsVUFBQyxLQUFvQixJQUFLLE9BQUEsS0FBSyxDQUFDLE1BQU0sRUFBWixDQUFZLE9BQ2xDLFVBQUMsS0FBb0IsSUFBSyxPQUFBLEtBQUssQ0FBQyxPQUFPLEVBQWIsQ0FBYSxPQUMxQyxVQUFDLEtBQW9CLElBQUssT0FBQSxLQUFLLENBQUMsT0FBTyxFQUFiLENBQWEsT0FDdEMsVUFBQyxLQUFvQixJQUFLLE9BQUEsS0FBSyxDQUFDLFFBQVEsRUFBZCxDQUFjO0FBUG5EOztHQUVHO0FBQ0gsSUFBTSxvQkFBb0IsR0FBdUQ7SUFDL0UsS0FBSyxJQUF3QztJQUM3QyxTQUFTLElBQXlDO0lBQ2xELE1BQU0sSUFBeUM7SUFDL0MsT0FBTyxJQUEwQztDQUNsRCxDQUFDO0FBRUY7OztHQUdHO0FBRUg7SUFBcUMsMkNBQWtCO0lBQ3JEOzs7T0FHRztJQUNILHlCQUE4QixHQUFRO2VBQUksa0JBQU0sR0FBRyxDQUFDO0lBQUUsQ0FBQzt3QkFMNUMsZUFBZTtJQU8xQjs7OztPQUlHO0lBQ0gs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LEdBQUcsb0JBQW9CLENBQUMsWUFBWSxDQUFDLENBQUM7Z0JBQzFELElBQUksY0FBYyxDQUFDLEtBQUssQ0FBQyxFQUFFO29CQUN6QixPQUFPLElBQUksWUFBWSxHQUFHLEdBQUcsQ0FBQztpQkFDL0I7YUFDRjtRQUNILENBQUMsQ0FBQyxDQUFDO1FBQ0gsT0FBTyxJQUFJLEdBQUcsQ0FBQztRQUNmLE9BQU8sT0FBTyxDQUFDO0lBQ2pCLENBQUM7SUFFRDs7Ozs7O09BTUc7SUFDSSw2QkFBYSxHQUFwQixVQUFxQixPQUFZLEVBQUUsT0FBaUIsRUFBRSxJQUFZO1FBQ2hFLE9BQU8sVUFBQyxLQUFVLENBQUMsaUJBQWlCO1lBQ2xDLElBQUksaUJBQWUsQ0FBQyxlQUFlLENBQUMsS0FBSyxDQUFDLEtBQUssT0FBTyxFQUFFO2dCQUN0RCxJQUFJLENBQUMsVUFBVSxDQUFDLGNBQU0sT0FBQSxPQUFPLENBQUMsS0FBSyxDQUFDLEVBQWQsQ0FBYyxDQUFDLENBQUM7YUFDdkM7UUFDSCxDQUFDLENBQUM7SUFDSixDQUFDO0lBRUQsZ0JBQWdCO0lBQ1QsNkJBQWEsR0FBcEIsVUFBcUIsT0FBZTtRQUNsQyxzREFBc0Q7UUFDdEQsUUFBUSxPQUFPLEVBQUU7WUFDZixLQUFLLEtBQUs7Z0JBQ1IsT0FBTyxRQUFRLENBQUM7WUFDbEI7Z0JBQ0UsT0FBTyxPQUFPLENBQUM7U0FDbEI7SUFDSCxDQUFDOztJQTdHVSxlQUFlO1FBRDNCLFVBQVUsRUFBRTtRQU1FLG1CQUFBLE1BQU0sQ0FBQyxRQUFRLENBQUMsQ0FBQTs7T0FMbEIsZUFBZSxDQThHM0I7SUFBRCxzQkFBQztDQUFBLEFBOUdELENBQXFDLGtCQUFrQixHQThHdEQ7U0E5R1ksZUFBZSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtET0NVTUVOVH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7SW5qZWN0LCBJbmplY3RhYmxlLCBOZ1pvbmV9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQge2dldERPTX0gZnJvbSAnLi4vZG9tX2FkYXB0ZXInO1xuXG5pbXBvcnQge0V2ZW50TWFuYWdlclBsdWdpbn0gZnJvbSAnLi9ldmVudF9tYW5hZ2VyJztcblxuLyoqXG4gKiBEZWZpbmVzIHN1cHBvcnRlZCBtb2RpZmllcnMgZm9yIGtleSBldmVudHMuXG4gKi9cbmNvbnN0IE1PRElGSUVSX0tFWVMgPSBbJ2FsdCcsICdjb250cm9sJywgJ21ldGEnLCAnc2hpZnQnXTtcblxuLyoqXG4gKiBSZXRyaWV2ZXMgbW9kaWZpZXJzIGZyb20ga2V5LWV2ZW50IG9iamVjdHMuXG4gKi9cbmNvbnN0IE1PRElGSUVSX0tFWV9HRVRURVJTOiB7W2tleTogc3RyaW5nXTogKGV2ZW50OiBLZXlib2FyZEV2ZW50KSA9PiBib29sZWFufSA9IHtcbiAgJ2FsdCc6IChldmVudDogS2V5Ym9hcmRFdmVudCkgPT4gZXZlbnQuYWx0S2V5LFxuICAnY29udHJvbCc6IChldmVudDogS2V5Ym9hcmRFdmVudCkgPT4gZXZlbnQuY3RybEtleSxcbiAgJ21ldGEnOiAoZXZlbnQ6IEtleWJvYXJkRXZlbnQpID0+IGV2ZW50Lm1ldGFLZXksXG4gICdzaGlmdCc6IChldmVudDogS2V5Ym9hcmRFdmVudCkgPT4gZXZlbnQuc2hpZnRLZXlcbn07XG5cbi8qKlxuICogQHB1YmxpY0FwaVxuICogQSBicm93c2VyIHBsdWctaW4gdGhhdCBwcm92aWRlcyBzdXBwb3J0IGZvciBoYW5kbGluZyBvZiBrZXkgZXZlbnRzIGluIEFuZ3VsYXIuXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBLZXlFdmVudHNQbHVnaW4gZXh0ZW5kcyBFdmVudE1hbmFnZXJQbHVnaW4ge1xuICAvKipcbiAgICogSW5pdGlhbGl6ZXMgYW4gaW5zdGFuY2Ugb2YgdGhlIGJyb3dzZXIgcGx1Zy1pbi5cbiAgICogQHBhcmFtIGRvYyBUaGUgZG9jdW1lbnQgaW4gd2hpY2gga2V5IGV2ZW50cyB3aWxsIGJlIGRldGVjdGVkLlxuICAgKi9cbiAgY29uc3RydWN0b3IoQEluamVjdChET0NVTUVOVCkgZG9jOiBhbnkpIHsgc3VwZXIoZG9jKTsgfVxuXG4gIC8qKlxuICAgICogUmVwb3J0cyB3aGV0aGVyIGEgbmFtZWQga2V5IGV2ZW50IGlzIHN1cHBvcnRlZC5cbiAgICAqIEBwYXJhbSBldmVudE5hbWUgVGhlIGV2ZW50IG5hbWUgdG8gcXVlcnkuXG4gICAgKiBAcmV0dXJuIFRydWUgaWYgdGhlIG5hbWVkIGtleSBldmVudCBpcyBzdXBwb3J0ZWQuXG4gICAqL1xuICBzdXBwb3J0cyhldmVudE5hbWU6IHN0cmluZyk6IGJvb2xlYW4geyByZXR1cm4gS2V5RXZlbnRzUGx1Z2luLnBhcnNlRXZlbnROYW1lKGV2ZW50TmFtZSkgIT0gbnVsbDsgfVxuXG4gIC8qKlxuICAgKiBSZWdpc3RlcnMgYSBoYW5kbGVyIGZvciBhIHNwZWNpZmljIGVsZW1lbnQgYW5kIGtleSBldmVudC5cbiAgICogQHBhcmFtIGVsZW1lbnQgVGhlIEhUTUwgZWxlbWVudCB0byByZWNlaXZlIGV2ZW50IG5vdGlmaWNhdGlvbnMuXG4gICAqIEBwYXJhbSBldmVudE5hbWUgVGhlIG5hbWUgb2YgdGhlIGtleSBldmVudCB0byBsaXN0ZW4gZm9yLlxuICAgKiBAcGFyYW0gaGFuZGxlciBBIGZ1bmN0aW9uIHRvIGNhbGwgd2hlbiB0aGUgbm90aWZpY2F0aW9uIG9jY3Vycy4gUmVjZWl2ZXMgdGhlXG4gICAqIGV2ZW50IG9iamVjdCBhcyBhbiBhcmd1bWVudC5cbiAgICogQHJldHVybnMgVGhlIGtleSBldmVudCB0aGF0IHdhcyByZWdpc3RlcmVkLlxuICAqL1xuICBhZGRFdmVudExpc3RlbmVyKGVsZW1lbnQ6IEhUTUxFbGVtZW50LCBldmVudE5hbWU6IHN0cmluZywgaGFuZGxlcjogRnVuY3Rpb24pOiBGdW5jdGlvbiB7XG4gICAgY29uc3QgcGFyc2VkRXZlbnQgPSBLZXlFdmVudHNQbHVnaW4ucGFyc2VFdmVudE5hbWUoZXZlbnROYW1lKSAhO1xuXG4gICAgY29uc3Qgb3V0c2lkZUhhbmRsZXIgPVxuICAgICAgICBLZXlFdmVudHNQbHVnaW4uZXZlbnRDYWxsYmFjayhwYXJzZWRFdmVudFsnZnVsbEtleSddLCBoYW5kbGVyLCB0aGlzLm1hbmFnZXIuZ2V0Wm9uZSgpKTtcblxuICAgIHJldHVybiB0aGlzLm1hbmFnZXIuZ2V0Wm9uZSgpLnJ1bk91dHNpZGVBbmd1bGFyKCgpID0+IHtcbiAgICAgIHJldHVybiBnZXRET00oKS5vbkFuZENhbmNlbChlbGVtZW50LCBwYXJzZWRFdmVudFsnZG9tRXZlbnROYW1lJ10sIG91dHNpZGVIYW5kbGVyKTtcbiAgICB9KTtcbiAgfVxuXG4gIHN0YXRpYyBwYXJzZUV2ZW50TmFtZShldmVudE5hbWU6IHN0cmluZyk6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9fG51bGwge1xuICAgIGNvbnN0IHBhcnRzOiBzdHJpbmdbXSA9IGV2ZW50TmFtZS50b0xvd2VyQ2FzZSgpLnNwbGl0KCcuJyk7XG5cbiAgICBjb25zdCBkb21FdmVudE5hbWUgPSBwYXJ0cy5zaGlmdCgpO1xuICAgIGlmICgocGFydHMubGVuZ3RoID09PSAwKSB8fCAhKGRvbUV2ZW50TmFtZSA9PT0gJ2tleWRvd24nIHx8IGRvbUV2ZW50TmFtZSA9PT0gJ2tleXVwJykpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIGNvbnN0IGtleSA9IEtleUV2ZW50c1BsdWdpbi5fbm9ybWFsaXplS2V5KHBhcnRzLnBvcCgpICEpO1xuXG4gICAgbGV0IGZ1bGxLZXkgPSAnJztcbiAgICBNT0RJRklFUl9LRVlTLmZvckVhY2gobW9kaWZpZXJOYW1lID0+IHtcbiAgICAgIGNvbnN0IGluZGV4OiBudW1iZXIgPSBwYXJ0cy5pbmRleE9mKG1vZGlmaWVyTmFtZSk7XG4gICAgICBpZiAoaW5kZXggPiAtMSkge1xuICAgICAgICBwYXJ0cy5zcGxpY2UoaW5kZXgsIDEpO1xuICAgICAgICBmdWxsS2V5ICs9IG1vZGlmaWVyTmFtZSArICcuJztcbiAgICAgIH1cbiAgICB9KTtcbiAgICBmdWxsS2V5ICs9IGtleTtcblxuICAgIGlmIChwYXJ0cy5sZW5ndGggIT0gMCB8fCBrZXkubGVuZ3RoID09PSAwKSB7XG4gICAgICAvLyByZXR1cm5pbmcgbnVsbCBpbnN0ZWFkIG9mIHRocm93aW5nIHRvIGxldCBhbm90aGVyIHBsdWdpbiBwcm9jZXNzIHRoZSBldmVudFxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgY29uc3QgcmVzdWx0OiB7W2s6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgICByZXN1bHRbJ2RvbUV2ZW50TmFtZSddID0gZG9tRXZlbnROYW1lO1xuICAgIHJlc3VsdFsnZnVsbEtleSddID0gZnVsbEtleTtcbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgc3RhdGljIGdldEV2ZW50RnVsbEtleShldmVudDogS2V5Ym9hcmRFdmVudCk6IHN0cmluZyB7XG4gICAgbGV0IGZ1bGxLZXkgPSAnJztcbiAgICBsZXQga2V5ID0gZ2V0RE9NKCkuZ2V0RXZlbnRLZXkoZXZlbnQpO1xuICAgIGtleSA9IGtleS50b0xvd2VyQ2FzZSgpO1xuICAgIGlmIChrZXkgPT09ICcgJykge1xuICAgICAga2V5ID0gJ3NwYWNlJzsgIC8vIGZvciByZWFkYWJpbGl0eVxuICAgIH0gZWxzZSBpZiAoa2V5ID09PSAnLicpIHtcbiAgICAgIGtleSA9ICdkb3QnOyAgLy8gYmVjYXVzZSAnLicgaXMgdXNlZCBhcyBhIHNlcGFyYXRvciBpbiBldmVudCBuYW1lc1xuICAgIH1cbiAgICBNT0RJRklFUl9LRVlTLmZvckVhY2gobW9kaWZpZXJOYW1lID0+IHtcbiAgICAgIGlmIChtb2RpZmllck5hbWUgIT0ga2V5KSB7XG4gICAgICAgIGNvbnN0IG1vZGlmaWVyR2V0dGVyID0gTU9ESUZJRVJfS0VZX0dFVFRFUlNbbW9kaWZpZXJOYW1lXTtcbiAgICAgICAgaWYgKG1vZGlmaWVyR2V0dGVyKGV2ZW50KSkge1xuICAgICAgICAgIGZ1bGxLZXkgKz0gbW9kaWZpZXJOYW1lICsgJy4nO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSk7XG4gICAgZnVsbEtleSArPSBrZXk7XG4gICAgcmV0dXJuIGZ1bGxLZXk7XG4gIH1cblxuICAvKipcbiAgICogQ29uZmlndXJlcyBhIGhhbmRsZXIgY2FsbGJhY2sgZm9yIGEga2V5IGV2ZW50LlxuICAgKiBAcGFyYW0gZnVsbEtleSBUaGUgZXZlbnQgbmFtZSB0aGF0IGNvbWJpbmVzIGFsbCBzaW11bHRhbmVvdXMga2V5c3Ryb2tlcy5cbiAgICogQHBhcmFtIGhhbmRsZXIgVGhlIGZ1bmN0aW9uIHRoYXQgcmVzcG9uZHMgdG8gdGhlIGtleSBldmVudC5cbiAgICogQHBhcmFtIHpvbmUgVGhlIHpvbmUgaW4gd2hpY2ggdGhlIGV2ZW50IG9jY3VycmVkLlxuICAgKiBAcmV0dXJucyBBIGNhbGxiYWNrIGZ1bmN0aW9uLlxuICAgKi9cbiAgc3RhdGljIGV2ZW50Q2FsbGJhY2soZnVsbEtleTogYW55LCBoYW5kbGVyOiBGdW5jdGlvbiwgem9uZTogTmdab25lKTogRnVuY3Rpb24ge1xuICAgIHJldHVybiAoZXZlbnQ6IGFueSAvKiogVE9ETyAjOTEwMCAqLykgPT4ge1xuICAgICAgaWYgKEtleUV2ZW50c1BsdWdpbi5nZXRFdmVudEZ1bGxLZXkoZXZlbnQpID09PSBmdWxsS2V5KSB7XG4gICAgICAgIHpvbmUucnVuR3VhcmRlZCgoKSA9PiBoYW5kbGVyKGV2ZW50KSk7XG4gICAgICB9XG4gICAgfTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgc3RhdGljIF9ub3JtYWxpemVLZXkoa2V5TmFtZTogc3RyaW5nKTogc3RyaW5nIHtcbiAgICAvLyBUT0RPOiBzd2l0Y2ggdG8gYSBNYXAgaWYgdGhlIG1hcHBpbmcgZ3Jvd3MgdG9vIG11Y2hcbiAgICBzd2l0Y2ggKGtleU5hbWUpIHtcbiAgICAgIGNhc2UgJ2VzYyc6XG4gICAgICAgIHJldHVybiAnZXNjYXBlJztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiBrZXlOYW1lO1xuICAgIH1cbiAgfVxufVxuIl19