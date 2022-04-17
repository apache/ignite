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
import { DOCUMENT, isPlatformServer } from '@angular/common';
import { Inject, Injectable, NgZone, Optional, PLATFORM_ID } from '@angular/core';
import { EventManagerPlugin } from './event_manager';
const ɵ0 = /**
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
const __symbol__ = ((ɵ0))();
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
export class DomEventsPlugin extends EventManagerPlugin {
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
export { ɵ0, ɵ1, ɵ2, ɵ3 };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG9tX2V2ZW50cy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL3BsYXRmb3JtLWJyb3dzZXIvc3JjL2RvbS9ldmVudHMvZG9tX2V2ZW50cy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxRQUFRLEVBQUUsZ0JBQWdCLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQztBQUMzRCxPQUFPLEVBQUMsTUFBTSxFQUFFLFVBQVUsRUFBRSxNQUFNLEVBQUUsUUFBUSxFQUFFLFdBQVcsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUVoRixPQUFPLEVBQUMsa0JBQWtCLEVBQUMsTUFBTSxpQkFBaUIsQ0FBQzs7OztBQVM5QyxHQUFHLEVBQUUsQ0FBQyxDQUFDLE9BQU8sSUFBSSxLQUFLLFdBQVcsQ0FBQyxJQUFJLENBQUMsbUJBQUEsSUFBSSxFQUFPLENBQUMsQ0FBQyxZQUFZLENBQUM7Ozs7O0lBQzlELFVBQVMsQ0FBUyxJQUFZLE9BQU8saUJBQWlCLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFBOzs7Ozs7OztNQUZoRSxVQUFVLEdBQ1osTUFDbUUsRUFBRTs7TUFDbkUsa0JBQWtCLEdBQXVCLFVBQVUsQ0FBQyxrQkFBa0IsQ0FBQzs7TUFDdkUscUJBQXFCLEdBQTBCLFVBQVUsQ0FBQyxxQkFBcUIsQ0FBQzs7TUFFaEYsV0FBVyxHQUE0QixFQUFFOztNQUV6QyxLQUFLLEdBQUcsT0FBTzs7TUFDZixPQUFPLEdBQUcsU0FBUzs7TUFDbkIsbUJBQW1CLEdBQUcsa0JBQWtCOztNQUN4QyxzQkFBc0IsR0FBRyxxQkFBcUI7OztNQUc5QyxVQUFVLEdBQUcsbUNBQW1DOztNQUNoRCxnQkFBZ0IsR0FBRyx5Q0FBeUM7Ozs7QUFHMUMsR0FBRyxFQUFFOztVQUNyQixpQkFBaUIsR0FDbkIsQ0FBQyxPQUFPLElBQUksS0FBSyxXQUFXLENBQUMsSUFBSSxDQUFDLG1CQUFBLElB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Qyx3REFBd0Q7WUFDeEQsSUFBSSxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsT0FBTyxLQUFLLFFBQVEsRUFBRTtnQkFDckMsS0FBSyxHQUFHLElBQUksQ0FBQztnQkFDYixTQUFTLENBQUMsTUFBTSxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQztnQkFDdkIsTUFBTTthQUNQO1NBQ0Y7UUFDRCxJQUFJLEtBQUssRUFBRTtZQUNULElBQUksU0FBUyxDQUFDLE1BQU0sS0FBSyxDQUFDLEVBQUU7Z0JBQzFCLDBFQUEwRTtnQkFDMUUsZ0JBQWdCLENBQUMsS0FBSyxDQUFDLE1BQU0sRUFBRSxDQUFDLFNBQVMsRUFBRSxjQUFjLEVBQUUsS0FBSyxDQUFDLENBQUMsQ0FBQzthQUNwRTtTQUNGO2FBQU07WUFDTCxxRUFBcUU7WUFDckUsb0RBQW9EO1lBQ3BELE1BQU0sQ0FBQyxzQkFBc0IsQ0FBQyxDQUFDLEtBQUssQ0FBQyxNQUFNLEVBQUUsQ0FBQyxTQUFTLEVBQUUsUUFBUSxFQUFFLEtBQUssQ0FBQyxDQUFDLENBQUM7U0FDNUU7SUFDSCxDQUFDOzs7WUFqSUYsVUFBVTs7Ozs0Q0FHSixNQUFNLFNBQUMsUUFBUTtZQWpHTSxNQUFNOzRDQWtHM0IsUUFBUSxZQUFJLE1BQU0sU0FBQyxXQUFXOzs7Ozs7O0lBREgsaUNBQXNCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0RPQ1VNRU5ULCBpc1BsYXRmb3JtU2VydmVyfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHtJbmplY3QsIEluamVjdGFibGUsIE5nWm9uZSwgT3B0aW9uYWwsIFBMQVRGT1JNX0lEfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtFdmVudE1hbmFnZXJQbHVnaW59IGZyb20gJy4vZXZlbnRfbWFuYWdlcic7XG5cbi8qKlxuICogRGV0ZWN0IGlmIFpvbmUgaXMgcHJlc2VudC4gSWYgaXQgaXMgdGhlbiB1c2Ugc2ltcGxlIHpvbmUgYXdhcmUgJ2FkZEV2ZW50TGlzdGVuZXInXG4gKiBzaW5jZSBBbmd1bGFyIGNhbiBkbyBtdWNoIG1vcmVcbiAqIGVmZmljaWVudCBib29ra2VlcGluZyB0aGFuIFpvbmUgY2FuLCBiZWNhdXNlIHdlIGhhdmUgYWRkaXRpb25hbCBpbmZvcm1hdGlvbi4gVGhpcyBzcGVlZHMgdXBcbiAqIGFkZEV2ZW50TGlzdGVuZXIgYnkgM3guXG4gKi9cbmNvbnN0IF9fc3ltYm9sX18gPVxuICAgICgoKSA9PiAodHlwZW9mIFpvbmUgIT09ICd1bmRlZmluZWQnKSAmJiAoWm9uZSBhcyBhbnkpWydfX3N5bWJvbF9fJ10gfHxcbiAgICAgICAgIGZ1bmN0aW9uKHY6IHN0cmluZyk6IHN0cmluZyB7IHJldHVybiAnX196b25lX3N5bWJvbF9fJyArIHY7IH0pKCk7XG5jb25zdCBBRERfRVZFTlRfTElTVEVORVI6ICdhZGRFdmVudExpc3RlbmVyJyA9IF9fc3ltYm9sX18oJ2FkZEV2ZW50TGlzdGVuZXInKTtcbmNvbnN0IFJFTU9WRV9FVkVOVF9MSVNURU5FUjogJ3JlbW92ZUV2ZW50TGlzdGVuZXInID0gX19zeW1ib2xfXygncmVtb3ZlRXZlbnRMaXN0ZW5lcicpO1xuXG5jb25zdCBzeW1ib2xOYW1lczoge1trZXk6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcblxuY29uc3QgRkFMU0UgPSAnRkFMU0UnO1xuY29uc3QgQU5HVUxBUiA9ICdBTkdVTEFSJztcbmNvbnN0IE5BVElWRV9BRERfTElTVEVORVIgPSAnYWRkRXZlbnRMaXN0ZW5lcic7XG5jb25zdCBOQVRJVkVfUkVNT1ZFX0xJU1RFTkVSID0gJ3JlbW92ZUV2ZW50TGlzdGVuZXInO1xuXG4vLyB1c2UgdGhlIHNhbWUgc3ltYm9sIHN0cmluZyB3aGljaCBpcyB1c2VkIGluIHpvbmUuanNcbmNvbnN0IHN0b3BTeW1ib2wgPSAnX196b25lX3N5bWJvbF9fcHJvcGFnYXRpb25TdG9wcGVkJztcbmNvbnN0IHN0b3BNZXRob2RTeW1ib2wgPSAnX196b25lX3N5bWJvbF9fc3RvcEltbWVkaWF0ZVByb3BhZ2F0aW9uJztcblxuXG5jb25zdCBibGFja0xpc3RlZE1hcCA9ICgoKSA9PiB7XG4gIGNvbnN0IGJsYWNrTGlzdGVkRXZlbnRzOiBzdHJpbmdbXSA9XG4gICAgICAodHlwZW9mIFpvbmUgIT09ICd1bmRlZmluZWQnKSAmJiAoWm9uZSBhcyBhbnkpW19fc3ltYm9sX18oJ0JMQUNLX0xJU1RFRF9FVkVOVFMnKV07XG4gIGlmIChibGFja0xpc3RlZEV2ZW50cykge1xuICAgIGNvbnN0IHJlczoge1tldmVudE5hbWU6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgICBibGFja0xpc3RlZEV2ZW50cy5mb3JFYWNoKGV2ZW50TmFtZSA9PiB7IHJlc1tldmVudE5hbWVdID0gZXZlbnROYW1lOyB9KTtcbiAgICByZXR1cm4gcmVzO1xuICB9XG4gIHJldHVybiB1bmRlZmluZWQ7XG59KSgpO1xuXG5cbmNvbnN0IGlzQmxhY2tMaXN0ZWRFdmVudCA9IGZ1bmN0aW9uKGV2ZW50TmFtZTogc3RyaW5nKSB7XG4gIGlmICghYmxhY2tMaXN0ZWRNYXApIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbiAgcmV0dXJuIGJsYWNrTGlzdGVkTWFwLmhhc093blByb3BlcnR5KGV2ZW50TmFtZSk7XG59O1xuXG5pbnRlcmZhY2UgVGFza0RhdGEge1xuICB6b25lOiBhbnk7XG4gIGhhbmRsZXI6IEZ1bmN0aW9uO1xufVxuXG4vLyBhIGdsb2JhbCBsaXN0ZW5lciB0byBoYW5kbGUgYWxsIGRvbSBldmVudCxcbi8vIHNvIHdlIGRvIG5vdCBuZWVkIHRvIGNyZWF0ZSBhIGNsb3N1cmUgZXZlcnkgdGltZVxuY29uc3QgZ2xvYmFsTGlzdGVuZXIgPSBmdW5jdGlvbih0aGlzOiBhbnksIGV2ZW50OiBFdmVudCkge1xuICBjb25zdCBzeW1ib2xOYW1lID0gc3ltYm9sTmFtZXNbZXZlbnQudHlwZV07XG4gIGlmICghc3ltYm9sTmFtZSkge1xuICAgIHJldHVybjtcbiAgfVxuICBjb25zdCB0YXNrRGF0YXM6IFRhc2tEYXRhW10gPSB0aGlzW3N5bWJvbE5hbWVdO1xuICBpZiAoIXRhc2tEYXRhcykge1xuICAgIHJldHVybjtcbiAgfVxuICBjb25zdCBhcmdzOiBhbnkgPSBbZXZlbnRdO1xuICBpZiAodGFza0RhdGFzLmxlbmd0aCA9PT0gMSkge1xuICAgIC8vIGlmIHRhc2tEYXRhcyBvbmx5IGhhdmUgb25lIGVsZW1lbnQsIGp1c3QgaW52b2tlIGl0XG4gICAgY29uc3QgdGFza0RhdGEgPSB0YXNrRGF0YXNbMF07XG4gICAgaWYgKHRhc2tEYXRhLnpvbmUgIT09IFpvbmUuY3VycmVudCkge1xuICAgICAgLy8gb25seSB1c2UgWm9uZS5ydW4gd2hlbiBab25lLmN1cnJlbnQgbm90IGVxdWFscyB0byBzdG9yZWQgem9uZVxuICAgICAgcmV0dXJuIHRhc2tEYXRhLnpvbmUucnVuKHRhc2tEYXRhLmhhbmRsZXIsIHRoaXMsIGFyZ3MpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdGFza0RhdGEuaGFuZGxlci5hcHBseSh0aGlzLCBhcmdzKTtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgLy8gY29weSB0YXNrcyBhcyBhIHNuYXBzaG90IHRvIGF2b2lkIGV2ZW50IGhhbmRsZXJzIHJlbW92ZVxuICAgIC8vIGl0c2VsZiBvciBvdGhlcnNcbiAgICBjb25zdCBjb3BpZWRUYXNrcyA9IHRhc2tEYXRhcy5zbGljZSgpO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgY29waWVkVGFza3MubGVuZ3RoOyBpKyspIHtcbiAgICAgIC8vIGlmIG90aGVyIGxpc3RlbmVyIGNhbGwgZXZlbnQuc3RvcEltbWVkaWF0ZVByb3BhZ2F0aW9uXG4gICAgICAvLyBqdXN0IGJyZWFrXG4gICAgICBpZiAoKGV2ZW50IGFzIGFueSlbc3RvcFN5bWJvbF0gPT09IHRydWUpIHtcbiAgICAgICAgYnJlYWs7XG4gICAgICB9XG4gICAgICBjb25zdCB0YXNrRGF0YSA9IGNvcGllZFRhc2tzW2ldO1xuICAgICAgaWYgKHRhc2tEYXRhLnpvbmUgIT09IFpvbmUuY3VycmVudCkge1xuICAgICAgICAvLyBvbmx5IHVzZSBab25lLnJ1biB3aGVuIFpvbmUuY3VycmVudCBub3QgZXF1YWxzIHRvIHN0b3JlZCB6b25lXG4gICAgICAgIHRhc2tEYXRhLnpvbmUucnVuKHRhc2tEYXRhLmhhbmRsZXIsIHRoaXMsIGFyZ3MpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGFza0RhdGEuaGFuZGxlci5hcHBseSh0aGlzLCBhcmdzKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cbn07XG5cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBEb21FdmVudHNQbHVnaW4gZXh0ZW5kcyBFdmVudE1hbmFnZXJQbHVnaW4ge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIEBJbmplY3QoRE9DVU1FTlQpIGRvYzogYW55LCBwcml2YXRlIG5nWm9uZTogTmdab25lLFxuICAgICAgQE9wdGlvbmFsKCkgQEluamVjdChQTEFURk9STV9JRCkgcGxhdGZvcm1JZDoge318bnVsbCkge1xuICAgIHN1cGVyKGRvYyk7XG5cbiAgICBpZiAoIXBsYXRmb3JtSWQgfHwgIWlzUGxhdGZvcm1TZXJ2ZXIocGxhdGZvcm1JZCkpIHtcbiAgICAgIHRoaXMucGF0Y2hFdmVudCgpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgcGF0Y2hFdmVudCgpIHtcbiAgICBpZiAodHlwZW9mIEV2ZW50ID09PSAndW5kZWZpbmVkJyB8fCAhRXZlbnQgfHwgIUV2ZW50LnByb3RvdHlwZSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBpZiAoKEV2ZW50LnByb3RvdHlwZSBhcyBhbnkpW3N0b3BNZXRob2RTeW1ib2xdKSB7XG4gICAgICAvLyBhbHJlYWR5IHBhdGNoZWQgYnkgem9uZS5qc1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCBkZWxlZ2F0ZSA9IChFdmVudC5wcm90b3R5cGUgYXMgYW55KVtzdG9wTWV0aG9kU3ltYm9sXSA9XG4gICAgICAgIEV2ZW50LnByb3RvdHlwZS5zdG9wSW1tZWRpYXRlUHJvcGFnYXRpb247XG4gICAgRXZlbnQucHJvdG90eXBlLnN0b3BJbW1lZGlhdGVQcm9wYWdhdGlvbiA9IGZ1bmN0aW9uKHRoaXM6IGFueSkge1xuICAgICAgaWYgKHRoaXMpIHtcbiAgICAgICAgdGhpc1tzdG9wU3ltYm9sXSA9IHRydWU7XG4gICAgICB9XG5cbiAgICAgIC8vIFdlIHNob3VsZCBjYWxsIG5hdGl2ZSBkZWxlZ2F0ZSBpbiBjYXNlIGluIHNvbWUgZW52aXJvbm1lbnQgcGFydCBvZlxuICAgICAgLy8gdGhlIGFwcGxpY2F0aW9uIHdpbGwgbm90IHVzZSB0aGUgcGF0Y2hlZCBFdmVudC4gQWxzbyB3ZSBjYXN0IHRoZVxuICAgICAgLy8gXCJhcmd1bWVudHNcIiB0byBhbnkgc2luY2UgXCJzdG9wSW1tZWRpYXRlUHJvcGFnYXRpb25cIiB0ZWNobmljYWxseSBkb2VzIG5vdFxuICAgICAgLy8gYWNjZXB0IGFueSBhcmd1bWVudHMsIGJ1dCB3ZSBkb24ndCBrbm93IHdoYXQgZGV2ZWxvcGVycyBwYXNzIHRocm91Z2ggdGhlXG4gICAgICAvLyBmdW5jdGlvbiBhbmQgd2Ugd2FudCB0byBub3QgYnJlYWsgdGhlc2UgY2FsbHMuXG4gICAgICBkZWxlZ2F0ZSAmJiBkZWxlZ2F0ZS5hcHBseSh0aGlzLCBhcmd1bWVudHMgYXMgYW55KTtcbiAgICB9O1xuICB9XG5cbiAgLy8gVGhpcyBwbHVnaW4gc2hvdWxkIGNvbWUgbGFzdCBpbiB0aGUgbGlzdCBvZiBwbHVnaW5zLCBiZWNhdXNlIGl0IGFjY2VwdHMgYWxsXG4gIC8vIGV2ZW50cy5cbiAgc3VwcG9ydHMoZXZlbnROYW1lOiBzdHJpbmcpOiBib29sZWFuIHsgcmV0dXJuIHRydWU7IH1cblxuICBhZGRFdmVudExpc3RlbmVyKGVsZW1lbnQ6IEhUTUxFbGVtZW50LCBldmVudE5hbWU6IHN0cmluZywgaGFuZGxlcjogRnVuY3Rpb24pOiBGdW5jdGlvbiB7XG4gICAgLyoqXG4gICAgICogVGhpcyBjb2RlIGlzIGFib3V0IHRvIGFkZCBhIGxpc3RlbmVyIHRvIHRoZSBET00uIElmIFpvbmUuanMgaXMgcHJlc2VudCwgdGhhblxuICAgICAqIGBhZGRFdmVudExpc3RlbmVyYCBoYXMgYmVlbiBwYXRjaGVkLiBUaGUgcGF0Y2hlZCBjb2RlIGFkZHMgb3ZlcmhlYWQgaW4gYm90aFxuICAgICAqIG1lbW9yeSBhbmQgc3BlZWQgKDN4IHNsb3dlcikgdGhhbiBuYXRpdmUuIEZvciB0aGlzIHJlYXNvbiBpZiB3ZSBkZXRlY3QgdGhhdFxuICAgICAqIFpvbmUuanMgaXMgcHJlc2VudCB3ZSB1c2UgYSBzaW1wbGUgdmVyc2lvbiBvZiB6b25lIGF3YXJlIGFkZEV2ZW50TGlzdGVuZXIgaW5zdGVhZC5cbiAgICAgKiBUaGUgcmVzdWx0IGlzIGZhc3RlciByZWdpc3RyYXRpb24gYW5kIHRoZSB6b25lIHdpbGwgYmUgcmVzdG9yZWQuXG4gICAgICogQnV0IFpvbmVTcGVjLm9uU2NoZWR1bGVUYXNrLCBab25lU3BlYy5vbkludm9rZVRhc2ssIFpvbmVTcGVjLm9uQ2FuY2VsVGFza1xuICAgICAqIHdpbGwgbm90IGJlIGludm9rZWRcbiAgICAgKiBXZSBhbHNvIGRvIG1hbnVhbCB6b25lIHJlc3RvcmF0aW9uIGluIGVsZW1lbnQudHMgcmVuZGVyRXZlbnRIYW5kbGVyQ2xvc3VyZSBtZXRob2QuXG4gICAgICpcbiAgICAgKiBOT1RFOiBpdCBpcyBwb3NzaWJsZSB0aGF0IHRoZSBlbGVtZW50IGlzIGZyb20gZGlmZmVyZW50IGlmcmFtZSwgYW5kIHNvIHdlXG4gICAgICogaGF2ZSB0byBjaGVjayBiZWZvcmUgd2UgZXhlY3V0ZSB0aGUgbWV0aG9kLlxuICAgICAqL1xuICAgIGNvbnN0IHNlbGYgPSB0aGlzO1xuICAgIGNvbnN0IHpvbmVKc0xvYWRlZCA9IGVsZW1lbnRbQUREX0VWRU5UX0xJU1RFTkVSXTtcbiAgICBsZXQgY2FsbGJhY2s6IEV2ZW50TGlzdGVuZXIgPSBoYW5kbGVyIGFzIEV2ZW50TGlzdGVuZXI7XG4gICAgLy8gaWYgem9uZWpzIGlzIGxvYWRlZCBhbmQgY3VycmVudCB6b25lIGlzIG5vdCBuZ1pvbmVcbiAgICAvLyB3ZSBrZWVwIFpvbmUuY3VycmVudCBvbiB0YXJnZXQgZm9yIGxhdGVyIHJlc3RvcmF0aW9uLlxuICAgIGlmICh6b25lSnNMb2FkZWQgJiYgKCFOZ1pvbmUuaXNJbkFuZ3VsYXJab25lKCkgfHwgaXNCbGFja0xpc3RlZEV2ZW50KGV2ZW50TmFtZSkpKSB7XG4gICAgICBsZXQgc3ltYm9sTmFtZSA9IHN5bWJvbE5hbWVzW2V2ZW50TmFtZV07XG4gICAgICBpZiAoIXN5bWJvbE5hbWUpIHtcbiAgICAgICAgc3ltYm9sTmFtZSA9IHN5bWJvbE5hbWVzW2V2ZW50TmFtZV0gPSBfX3N5bWJvbF9fKEFOR1VMQVIgKyBldmVudE5hbWUgKyBGQUxTRSk7XG4gICAgICB9XG4gICAgICBsZXQgdGFza0RhdGFzOiBUYXNrRGF0YVtdID0gKGVsZW1lbnQgYXMgYW55KVtzeW1ib2xOYW1lXTtcbiAgICAgIGNvbnN0IGdsb2JhbExpc3RlbmVyUmVnaXN0ZXJlZCA9IHRhc2tEYXRhcyAmJiB0YXNrRGF0YXMubGVuZ3RoID4gMDtcbiAgICAgIGlmICghdGFza0RhdGFzKSB7XG4gICAgICAgIHRhc2tEYXRhcyA9IChlbGVtZW50IGFzIGFueSlbc3ltYm9sTmFtZV0gPSBbXTtcbiAgICAgIH1cblxuICAgICAgY29uc3Qgem9uZSA9IGlzQmxhY2tMaXN0ZWRFdmVudChldmVudE5hbWUpID8gWm9uZS5yb290IDogWm9uZS5jdXJyZW50O1xuICAgICAgaWYgKHRhc2tEYXRhcy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgdGFza0RhdGFzLnB1c2goe3pvbmU6IHpvbmUsIGhhbmRsZXI6IGNhbGxiYWNrfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBsZXQgY2FsbGJhY2tSZWdpc3RlcmVkID0gZmFsc2U7XG4gICAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdGFza0RhdGFzLmxlbmd0aDsgaSsrKSB7XG4gICAgICAgICAgaWYgKHRhc2tEYXRhc1tpXS5oYW5kbGVyID09PSBjYWxsYmFjaykge1xuICAgICAgICAgICAgY2FsbGJhY2tSZWdpc3RlcmVkID0gdHJ1ZTtcbiAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBpZiAoIWNhbGxiYWNrUmVnaXN0ZXJlZCkge1xuICAgICAgICAgIHRhc2tEYXRhcy5wdXNoKHt6b25lOiB6b25lLCBoYW5kbGVyOiBjYWxsYmFja30pO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGlmICghZ2xvYmFsTGlzdGVuZXJSZWdpc3RlcmVkKSB7XG4gICAgICAgIGVsZW1lbnRbQUREX0VWRU5UX0xJU1RFTkVSXShldmVudE5hbWUsIGdsb2JhbExpc3RlbmVyLCBmYWxzZSk7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIGVsZW1lbnRbTkFUSVZFX0FERF9MSVNURU5FUl0oZXZlbnROYW1lLCBjYWxsYmFjaywgZmFsc2UpO1xuICAgIH1cbiAgICByZXR1cm4gKCkgPT4gdGhpcy5yZW1vdmVFdmVudExpc3RlbmVyKGVsZW1lbnQsIGV2ZW50TmFtZSwgY2FsbGJhY2spO1xuICB9XG5cbiAgcmVtb3ZlRXZlbnRMaXN0ZW5lcih0YXJnZXQ6IGFueSwgZXZlbnROYW1lOiBzdHJpbmcsIGNhbGxiYWNrOiBGdW5jdGlvbik6IHZvaWQge1xuICAgIGxldCB1bmRlcmx5aW5nUmVtb3ZlID0gdGFyZ2V0W1JFTU9WRV9FVkVOVF9MSVNURU5FUl07XG4gICAgLy8gem9uZS5qcyBub3QgbG9hZGVkLCB1c2UgbmF0aXZlIHJlbW92ZUV2ZW50TGlzdGVuZXJcbiAgICBpZiAoIXVuZGVybHlpbmdSZW1vdmUpIHtcbiAgICAgIHJldHVybiB0YXJnZXRbTkFUSVZFX1JFTU9WRV9MSVNURU5FUl0uYXBwbHkodGFyZ2V0LCBbZXZlbnROYW1lLCBjYWxsYmFjaywgZmFsc2VdKTtcbiAgICB9XG4gICAgbGV0IHN5bWJvbE5hbWUgPSBzeW1ib2xOYW1lc1tldmVudE5hbWVdO1xuICAgIGxldCB0YXNrRGF0YXM6IFRhc2tEYXRhW10gPSBzeW1ib2xOYW1lICYmIHRhcmdldFtzeW1ib2xOYW1lXTtcbiAgICBpZiAoIXRhc2tEYXRhcykge1xuICAgICAgLy8gYWRkRXZlbnRMaXN0ZW5lciBub3QgdXNpbmcgcGF0Y2hlZCB2ZXJzaW9uXG4gICAgICAvLyBqdXN0IGNhbGwgbmF0aXZlIHJlbW92ZUV2ZW50TGlzdGVuZXJcbiAgICAgIHJldHVybiB0YXJnZXRbTkFUSVZFX1JFTU9WRV9MSVNURU5FUl0uYXBwbHkodGFyZ2V0LCBbZXZlbnROYW1lLCBjYWxsYmFjaywgZmFsc2VdKTtcbiAgICB9XG4gICAgLy8gZml4IGlzc3VlIDIwNTMyLCBzaG91bGQgYmUgYWJsZSB0byByZW1vdmVcbiAgICAvLyBsaXN0ZW5lciB3aGljaCB3YXMgYWRkZWQgaW5zaWRlIG9mIG5nWm9uZVxuICAgIGxldCBmb3VuZCA9IGZhbHNlO1xuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgdGFza0RhdGFzLmxlbmd0aDsgaSsrKSB7XG4gICAgICAvLyByZW1vdmUgbGlzdGVuZXIgZnJvbSB0YXNrRGF0YXMgaWYgdGhlIGNhbGxiYWNrIGVxdWFsc1xuICAgICAgaWYgKHRhc2tEYXRhc1tpXS5oYW5kbGVyID09PSBjYWxsYmFjaykge1xuICAgICAgICBmb3VuZCA9IHRydWU7XG4gICAgICAgIHRhc2tEYXRhcy5zcGxpY2UoaSwgMSk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgfVxuICAgIH1cbiAgICBpZiAoZm91bmQpIHtcbiAgICAgIGlmICh0YXNrRGF0YXMubGVuZ3RoID09PSAwKSB7XG4gICAgICAgIC8vIGFsbCBsaXN0ZW5lcnMgYXJlIHJlbW92ZWQsIHdlIGNhbiByZW1vdmUgdGhlIGdsb2JhbExpc3RlbmVyIGZyb20gdGFyZ2V0XG4gICAgICAgIHVuZGVybHlpbmdSZW1vdmUuYXBwbHkodGFyZ2V0LCBbZXZlbnROYW1lLCBnbG9iYWxMaXN0ZW5lciwgZmFsc2VdKTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgLy8gbm90IGZvdW5kIGluIHRhc2tEYXRhcywgdGhlIGNhbGxiYWNrIG1heSBiZSBhZGRlZCBpbnNpZGUgb2Ygbmdab25lXG4gICAgICAvLyB1c2UgbmF0aXZlIHJlbW92ZSBsaXN0ZW5lciB0byByZW1vdmUgdGhlIGNhbGxiYWNrXG4gICAgICB0YXJnZXRbTkFUSVZFX1JFTU9WRV9MSVNURU5FUl0uYXBwbHkodGFyZ2V0LCBbZXZlbnROYW1lLCBjYWxsYmFjaywgZmFsc2VdKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==