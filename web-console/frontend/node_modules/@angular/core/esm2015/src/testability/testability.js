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
import { Injectable } from '../di';
import { scheduleMicroTask } from '../util/microtask';
import { NgZone } from '../zone/ng_zone';
/**
 * @record
 */
export function PendingMacrotask() { }
if (false) {
    /** @type {?} */
    PendingMacrotask.prototype.source;
    /** @type {?} */
    PendingMacrotask.prototype.creationLocation;
    /** @type {?|undefined} */
    PendingMacrotask.prototype.runCount;
    /** @type {?|undefined} */
    PendingMacrotask.prototype.data;
}
/**
 * @record
 */
export function TaskData() { }
if (false) {
    /** @type {?|undefined} */
    TaskData.prototype.target;
    /** @type {?|undefined} */
    TaskData.prototype.delay;
    /** @type {?|undefined} */
    TaskData.prototype.isPeriodic;
}
/**
 * @record
 */
function WaitCallback() { }
if (false) {
    /** @type {?} */
    WaitCallback.prototype.timeoutId;
    /** @type {?} */
    WaitCallback.prototype.doneCb;
    /** @type {?|undefined} */
    WaitCallback.prototype.updateCb;
}
/**
 * The Testability service provides testing hooks that can be accessed from
 * the browser and by services such as Protractor. Each bootstrapped Angular
 * application on the page will have an instance of Testability.
 * \@publicApi
 */
export class Testability {
    /**
     * @param {?} _ngZone
     */
    constructor(_ngZone) {
        this._ngZone = _ngZone;
        this._pendingCount = 0;
        this._isZoneStable = true;
        /**
         * Whether any work was done since the last 'whenStable' callback. This is
         * useful to detect if this could have potentially destabilized another
         * component while it is stabilizing.
         * \@internal
         */
        this._didWork = false;
        this._callbacks = [];
        this.taskTrackingZone = null;
        this._watchAngularEvents();
        _ngZone.run((/**
         * @return {?}
         */
        () => {
            this.taskTrackingZone =
                typeof Zone == 'undefined' ? null : Zone.current.get('TaskTrackingZone');
        }));
    }
    /**
     * @private
     * @return {?}
     */
    _watchAngularEvents() {
        this._ngZone.onUnstable.subscribe({
            next: (/**
             * @return {?}
             */
            () => {
                this._didWork = true;
                this._isZoneStable = false;
            })
        });
        this._ngZone.runOutsideAngular((/**
         * @return {?}
         */
        () => {
            this._ngZone.onStable.subscribe({
                next: (/**
                 * @return {?}
                 */
                () => {
                    NgZone.assertNotInAngularZone();
                    scheduleMicroTask((/**
                     * @return {?}
                     */
                    () => {
                        this._isZoneStable = true;
                        this._runCallbacksIfReady();
                    }));
                })
            });
        }));
    }
    /**
     * Increases the number of pending request
     * @deprecated pending requests are now tracked with zones.
     * @return {?}
     */
    increasePendingRequestCount() {
        this._pendingCount += 1;
        this._didWork = true;
        return this._pendingCount;
    }
    /**
     * Decreases the number of pending request
     * @deprecated pending requests are now tracked with zones
     * @return {?}
     */
    decreasePendingRequestCount() {
        this._pendingCount -= 1;
        if (this._pendingCount < 0) {
            throw new Error('pending async requests below zero');
        }
        this._runCallbacksIfReady();
        return this._pendingCount;
    }
    /**
     * Whether an associated application is stable
     * @return {?}
     */
    isStable() {
        return this._isZoneStable && this._pendingCount === 0 && !this._ngZone.hasPendingMacrotasks;
    }
    /**
     * @private
     * @return {?}
     */
    _runCallbacksIfReady() {
        if (this.isStable()) {
            // Schedules the call backs in a new frame so that it is always async.
            scheduleMicroTask((/**
             * @return {?}
             */
            () => {
                while (this._callbacks.length !== 0) {
                    /** @type {?} */
                    let cb = (/** @type {?} */ (this._callbacks.pop()));
                    clearTimeout(cb.timeoutId);
                    cb.doneCb(this._didWork);
                }
                this._didWork = false;
            }));
        }
        else {
            // Still not stable, send updates.
            /** @type {?} */
            let pending = this.getPendingTasks();
            this._callbacks = this._callbacks.filter((/**
             * @param {?} cb
             * @return {?}
             */
            (cb) => {
                if (cb.updateCb && cb.updateCb(pending)) {
                    clearTimeout(cb.timeoutId);
                    return false;
                }
                return true;
            }));
            this._didWork = true;
        }
    }
    /**
     * @private
     * @return {?}
     */
    getPendingTasks() {
        if (!this.taskTrackingZone) {
            return [];
        }
        // Copy the tasks data so that we don't leak tasks.
        return this.taskTrackingZone.macroTasks.map((/**
         * @param {?} t
         * @return {?}
         */
        (t) => {
            return {
                source: t.source,
                // From TaskTrackingZone:
                // https://github.com/angular/zone.js/blob/master/lib/zone-spec/task-tracking.ts#L40
                creationLocation: (/** @type {?} */ (((/** @type {?} */ (t))).creationLocation)),
                data: t.data
            };
        }));
    }
    /**
     * @private
     * @param {?} cb
     * @param {?=} timeout
     * @param {?=} updateCb
     * @return {?}
     */
    addCallback(cb, timeout, updateCb) {
        /** @type {?} */
        let timeoutId = -1;
        if (timeout && timeout > 0) {
            timeoutId = setTimeout((/**
             * @return {?}
             */
            () => {
                this._callbacks = this._callbacks.filter((/**
                 * @param {?} cb
                 * @return {?}
                 */
                (cb) => cb.timeoutId !== timeoutId));
                cb(this._didWork, this.getPendingTasks());
            }), timeout);
        }
        this._callbacks.push((/** @type {?} */ ({ doneCb: cb, timeoutId: timeoutId, updateCb: updateCb })));
    }
    /**
     * Wait for the application to be stable with a timeout. If the timeout is reached before that
     * happens, the callback receives a list of the macro tasks that were pending, otherwise null.
     *
     * @param {?} doneCb The callback to invoke when Angular is stable or the timeout expires
     *    whichever comes first.
     * @param {?=} timeout Optional. The maximum time to wait for Angular to become stable. If not
     *    specified, whenStable() will wait forever.
     * @param {?=} updateCb Optional. If specified, this callback will be invoked whenever the set of
     *    pending macrotasks changes. If this callback returns true doneCb will not be invoked
     *    and no further updates will be issued.
     * @return {?}
     */
    whenStable(doneCb, timeout, updateCb) {
        if (updateCb && !this.taskTrackingZone) {
            throw new Error('Task tracking zone is required when passing an update callback to ' +
                'whenStable(). Is "zone.js/dist/task-tracking.js" loaded?');
        }
        // These arguments are 'Function' above to keep the public API simple.
        this.addCallback((/** @type {?} */ (doneCb)), timeout, (/** @type {?} */ (updateCb)));
        this._runCallbacksIfReady();
    }
    /**
     * Get the number of pending requests
     * @deprecated pending requests are now tracked with zones
     * @return {?}
     */
    getPendingRequestCount() { return this._pendingCount; }
    /**
     * Find providers by name
     * @param {?} using The root element to search from
     * @param {?} provider The name of binding variable
     * @param {?} exactMatch Whether using exactMatch
     * @return {?}
     */
    findProviders(using, provider, exactMatch) {
        // TODO(juliemr): implement.
        return [];
    }
}
Testability.decorators = [
    { type: Injectable }
];
/** @nocollapse */
Testability.ctorParameters = () => [
    { type: NgZone }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    Testability.prototype._pendingCount;
    /**
     * @type {?}
     * @private
     */
    Testability.prototype._isZoneStable;
    /**
     * Whether any work was done since the last 'whenStable' callback. This is
     * useful to detect if this could have potentially destabilized another
     * component while it is stabilizing.
     * \@internal
     * @type {?}
     * @private
     */
    Testability.prototype._didWork;
    /**
     * @type {?}
     * @private
     */
    Testability.prototype._callbacks;
    /**
     * @type {?}
     * @private
     */
    Testability.prototype.taskTrackingZone;
    /**
     * @type {?}
     * @private
     */
    Testability.prototype._ngZone;
}
/**
 * A global registry of {\@link Testability} instances for specific elements.
 * \@publicApi
 */
export class TestabilityRegistry {
    constructor() {
        /**
         * \@internal
         */
        this._applications = new Map();
        _testabilityGetter.addToWindow(this);
    }
    /**
     * Registers an application with a testability hook so that it can be tracked
     * @param {?} token token of application, root element
     * @param {?} testability Testability hook
     * @return {?}
     */
    registerApplication(token, testability) {
        this._applications.set(token, testability);
    }
    /**
     * Unregisters an application.
     * @param {?} token token of application, root element
     * @return {?}
     */
    unregisterApplication(token) { this._applications.delete(token); }
    /**
     * Unregisters all applications
     * @return {?}
     */
    unregisterAllApplications() { this._applications.clear(); }
    /**
     * Get a testability hook associated with the application
     * @param {?} elem root element
     * @return {?}
     */
    getTestability(elem) { return this._applications.get(elem) || null; }
    /**
     * Get all registered testabilities
     * @return {?}
     */
    getAllTestabilities() { return Array.from(this._applications.values()); }
    /**
     * Get all registered applications(root elements)
     * @return {?}
     */
    getAllRootElements() { return Array.from(this._applications.keys()); }
    /**
     * Find testability of a node in the Tree
     * @param {?} elem node
     * @param {?=} findInAncestors whether finding testability in ancestors if testability was not found in
     * current node
     * @return {?}
     */
    findTestabilityInTree(elem, findInAncestors = true) {
        return _testabilityGetter.findTestabilityInTree(this, elem, findInAncestors);
    }
}
TestabilityRegistry.decorators = [
    { type: Injectable }
];
/** @nocollapse */
TestabilityRegistry.ctorParameters = () => [];
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    TestabilityRegistry.prototype._applications;
}
/**
 * Adapter interface for retrieving the `Testability` service associated for a
 * particular context.
 *
 * \@publicApi
 * @record
 */
export function GetTestability() { }
if (false) {
    /**
     * @param {?} registry
     * @return {?}
     */
    GetTestability.prototype.addToWindow = function (registry) { };
    /**
     * @param {?} registry
     * @param {?} elem
     * @param {?} findInAncestors
     * @return {?}
     */
    GetTestability.prototype.findTestabilityInTree = function (registry, elem, findInAncestors) { };
}
class _NoopGetTestability {
    /**
     * @param {?} registry
     * @return {?}
     */
    addToWindow(registry) { }
    /**
     * @param {?} registry
     * @param {?} elem
     * @param {?} findInAncestors
     * @return {?}
     */
    findTestabilityInTree(registry, elem, findInAncestors) {
        return null;
    }
}
/**
 * Set the {\@link GetTestability} implementation used by the Angular testing framework.
 * \@publicApi
 * @param {?} getter
 * @return {?}
 */
export function setTestabilityGetter(getter) {
    _testabilityGetter = getter;
}
/** @type {?} */
let _testabilityGetter = new _NoopGetTestability();
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVzdGFiaWxpdHkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy90ZXN0YWJpbGl0eS90ZXN0YWJpbGl0eS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxVQUFVLEVBQUMsTUFBTSxPQUFPLENBQUM7QUFDakMsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDcEQsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLGlCQUFpQixDQUFDOzs7O0FBZXZDLHNDQUtDOzs7SUFKQyxrQ0FBZTs7SUFDZiw0Q0FBd0I7O0lBQ3hCLG9DQUFrQjs7SUFDbEIsZ0NBQWdCOzs7OztBQUdsQiw4QkFJQzs7O0lBSEMsMEJBQXdCOztJQUN4Qix5QkFBZTs7SUFDZiw4QkFBcUI7Ozs7O0FBT3ZCLDJCQU1DOzs7SUFIQyxpQ0FBZTs7SUFDZiw4QkFBcUI7O0lBQ3JCLGdDQUEwQjs7Ozs7Ozs7QUFVNUIsTUFBTSxPQUFPLFdBQVc7Ozs7SUFjdEIsWUFBb0IsT0FBZTtRQUFmLFlBQU8sR0FBUCxPQUFPLENBQVE7UUFiM0Isa0JBQWEsR0FBVyxDQUFDLENBQUM7UUFDMUIsa0JBQWEsR0FBWSxJQUFJLENBQUM7Ozs7Ozs7UUFPOUIsYUFBUSxHQUFZLEtBQUssQ0FBQztRQUMxQixlQUFVLEdBQW1CLEVBQUUsQ0FBQztRQUVoQyxxQkFBZ0IsR0FBOEIsSUFBSSxDQUFDO1FBR3pELElBQUksQ0FBQyxtQkFBbUIsRUFBRSxDQUFDO1FBQzNCLE9BQU8sQ0FBQyxHQUFHOzs7UUFBQyxHQUFHLEVBQUU7WUFDZixJQUFJLENBQUMsZ0JBQWdCO2dCQUNqQixPQUFPLElBQUksSUFBSSxXQUFXLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsa0JBQWtCLENBQUMsQ0FBQztRQUMvRSxD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Qzs7Ozs7O0lBTTNELGNBQWMsQ0FBQyxJQUFTLElBQXNCLE9BQU8sSUFBSSxDQUFDLGFBQWEsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLElBQUksSUFBSSxDQUFDLENBQUMsQ0FBQzs7Ozs7SUFLNUYsbUJBQW1CLEtBQW9CLE9BQU8sS0FBSyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsYUFBYSxDQUFDLE1BQU0sRUFBRSxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7OztJQUt4RixrQkFBa0IsS0FBWSxPQUFPLEtBQUssQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQzs7Ozs7Ozs7SUFRN0UscUJBQXFCLENBQUMsSUFBVSxFQUFFLGtCQUEyQixJQUFJO1FBQy9ELE9BQU8sa0JBQWtCLENBQUMscUJBQXFCLENBQUMsSUFBSSxFQUFFLElBQUksRUFBRSxlQUFlLENBQUMsQ0FBQztJQUMvRSxDQUFDOzs7WUFuREYsVUFBVTs7Ozs7Ozs7O0lBR1QsNENBQTRDOzs7Ozs7Ozs7QUF5RDlDLG9DQUlDOzs7Ozs7SUFIQywrREFBaUQ7Ozs7Ozs7SUFDakQsZ0dBQ3FCOztBQUd2QixNQUFNLG1CQUFtQjs7Ozs7SUFDdkIsV0FBVyxDQUFDLFFBQTZCLElBQVMsQ0FBQzs7Ozs7OztJQUNuRCxxQkFBcUIsQ0FBQyxRQUE2QixFQUFFLElBQVMsRUFBRSxlQUF3QjtRQUV0RixPQUFPLElBQUksQ0FBQztJQUNkLENBQUM7Q0FDRjs7Ozs7OztBQU1ELE1BQU0sVUFBVSxvQkFBb0IsQ0FBQyxNQUFzQjtJQUN6RCxrQkFBa0IsR0FBRyxNQUFNLENBQUM7QUFDOUIsQ0FBQzs7SUFFRyxrQkFBa0IsR0FBbUIsSUFBSSxtQkFBbUIsRUFBRSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtJbmplY3RhYmxlfSBmcm9tICcuLi9kaSc7XG5pbXBvcnQge3NjaGVkdWxlTWljcm9UYXNrfSBmcm9tICcuLi91dGlsL21pY3JvdGFzayc7XG5pbXBvcnQge05nWm9uZX0gZnJvbSAnLi4vem9uZS9uZ196b25lJztcblxuLyoqXG4gKiBUZXN0YWJpbGl0eSBBUEkuXG4gKiBgZGVjbGFyZWAga2V5d29yZCBjYXVzZXMgdHNpY2tsZSB0byBnZW5lcmF0ZSBleHRlcm5zLCBzbyB0aGVzZSBtZXRob2RzIGFyZVxuICogbm90IHJlbmFtZWQgYnkgQ2xvc3VyZSBDb21waWxlci5cbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGRlY2xhcmUgaW50ZXJmYWNlIFB1YmxpY1Rlc3RhYmlsaXR5IHtcbiAgaXNTdGFibGUoKTogYm9vbGVhbjtcbiAgd2hlblN0YWJsZShjYWxsYmFjazogRnVuY3Rpb24sIHRpbWVvdXQ/OiBudW1iZXIsIHVwZGF0ZUNhbGxiYWNrPzogRnVuY3Rpb24pOiB2b2lkO1xuICBmaW5kUHJvdmlkZXJzKHVzaW5nOiBhbnksIHByb3ZpZGVyOiBzdHJpbmcsIGV4YWN0TWF0Y2g6IGJvb2xlYW4pOiBhbnlbXTtcbn1cblxuLy8gQW5ndWxhciBpbnRlcm5hbCwgbm90IGludGVuZGVkIGZvciBwdWJsaWMgQVBJLlxuZXhwb3J0IGludGVyZmFjZSBQZW5kaW5nTWFjcm90YXNrIHtcbiAgc291cmNlOiBzdHJpbmc7XG4gIGNyZWF0aW9uTG9jYXRpb246IEVycm9yO1xuICBydW5Db3VudD86IG51bWJlcjtcbiAgZGF0YT86IFRhc2tEYXRhO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIFRhc2tEYXRhIHtcbiAgdGFyZ2V0PzogWE1MSHR0cFJlcXVlc3Q7XG4gIGRlbGF5PzogbnVtYmVyO1xuICBpc1BlcmlvZGljPzogYm9vbGVhbjtcbn1cblxuLy8gQW5ndWxhciBpbnRlcm5hbCwgbm90IGludGVuZGVkIGZvciBwdWJsaWMgQVBJLlxuZXhwb3J0IHR5cGUgRG9uZUNhbGxiYWNrID0gKGRpZFdvcms6IGJvb2xlYW4sIHRhc2tzPzogUGVuZGluZ01hY3JvdGFza1tdKSA9PiB2b2lkO1xuZXhwb3J0IHR5cGUgVXBkYXRlQ2FsbGJhY2sgPSAodGFza3M6IFBlbmRpbmdNYWNyb3Rhc2tbXSkgPT4gYm9vbGVhbjtcblxuaW50ZXJmYWNlIFdhaXRDYWxsYmFjayB7XG4gIC8vIE5lZWRzIHRvIGJlICdhbnknIC0gc2V0VGltZW91dCByZXR1cm5zIGEgbnVtYmVyIGFjY29yZGluZyB0byBFUzYsIGJ1dFxuICAvLyBvbiBOb2RlSlMgaXQgcmV0dXJucyBhIFRpbWVyLlxuICB0aW1lb3V0SWQ6IGFueTtcbiAgZG9uZUNiOiBEb25lQ2FsbGJhY2s7XG4gIHVwZGF0ZUNiPzogVXBkYXRlQ2FsbGJhY2s7XG59XG5cbi8qKlxuICogVGhlIFRlc3RhYmlsaXR5IHNlcnZpY2UgcHJvdmlkZXMgdGVzdGluZyBob29rcyB0aGF0IGNhbiBiZSBhY2Nlc3NlZCBmcm9tXG4gKiB0aGUgYnJvd3NlciBhbmQgYnkgc2VydmljZXMgc3VjaCBhcyBQcm90cmFjdG9yLiBFYWNoIGJvb3RzdHJhcHBlZCBBbmd1bGFyXG4gKiBhcHBsaWNhdGlvbiBvbiB0aGUgcGFnZSB3aWxsIGhhdmUgYW4gaW5zdGFuY2Ugb2YgVGVzdGFiaWxpdHkuXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBUZXN0YWJpbGl0eSBpbXBsZW1lbnRzIFB1YmxpY1Rlc3RhYmlsaXR5IHtcbiAgcHJpdmF0ZSBfcGVuZGluZ0NvdW50OiBudW1iZXIgPSAwO1xuICBwcml2YXRlIF9pc1pvbmVTdGFibGU6IGJvb2xlYW4gPSB0cnVlO1xuICAvKipcbiAgICogV2hldGhlciBhbnkgd29yayB3YXMgZG9uZSBzaW5jZSB0aGUgbGFzdCAnd2hlblN0YWJsZScgY2FsbGJhY2suIFRoaXMgaXNcbiAgICogdXNlZnVsIHRvIGRldGVjdCBpZiB0aGlzIGNvdWxkIGhhdmUgcG90ZW50aWFsbHkgZGVzdGFiaWxpemVkIGFub3RoZXJcbiAgICogY29tcG9uZW50IHdoaWxlIGl0IGlzIHN0YWJpbGl6aW5nLlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIHByaXZhdGUgX2RpZFdvcms6IGJvb2xlYW4gPSBmYWxzZTtcbiAgcHJpdmF0ZSBfY2FsbGJhY2tzOiBXYWl0Q2FsbGJhY2tbXSA9IFtdO1xuXG4gIHByaXZhdGUgdGFza1RyYWNraW5nWm9uZToge21hY3JvVGFza3M6IFRhc2tbXX18bnVsbCA9IG51bGw7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfbmdab25lOiBOZ1pvbmUpIHtcbiAgICB0aGlzLl93YXRjaEFuZ3VsYXJFdmVudHMoKTtcbiAgICBfbmdab25lLnJ1bigoKSA9PiB7XG4gICAgICB0aGlzLnRhc2tUcmFja2luZ1pvbmUgPVxuICAgICAgICAgIHR5cGVvZiBab25lID09ICd1bmRlZmluZWQnID8gbnVsbCA6IFpvbmUuY3VycmVudC5nZXQoJ1Rhc2tUcmFja2luZ1pvbmUnKTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgX3dhdGNoQW5ndWxhckV2ZW50cygpOiB2b2lkIHtcbiAgICB0aGlzLl9uZ1pvbmUub25VbnN0YWJsZS5zdWJzY3JpYmUoe1xuICAgICAgbmV4dDogKCkgPT4ge1xuICAgICAgICB0aGlzLl9kaWRXb3JrID0gdHJ1ZTtcbiAgICAgICAgdGhpcy5faXNab25lU3RhYmxlID0gZmFsc2U7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICB0aGlzLl9uZ1pvbmUucnVuT3V0c2lkZUFuZ3VsYXIoKCkgPT4ge1xuICAgICAgdGhpcy5fbmdab25lLm9uU3RhYmxlLnN1YnNjcmliZSh7XG4gICAgICAgIG5leHQ6ICgpID0+IHtcbiAgICAgICAgICBOZ1pvbmUuYXNzZXJ0Tm90SW5Bbmd1bGFyWm9uZSgpO1xuICAgICAgICAgIHNjaGVkdWxlTWljcm9UYXNrKCgpID0+IHtcbiAgICAgICAgICAgIHRoaXMuX2lzWm9uZVN0YWJsZSA9IHRydWU7XG4gICAgICAgICAgICB0aGlzLl9ydW5DYWxsYmFja3NJZlJlYWR5KCk7XG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH0pO1xuICB9XG5cbiAgLyoqXG4gICAqIEluY3JlYXNlcyB0aGUgbnVtYmVyIG9mIHBlbmRpbmcgcmVxdWVzdFxuICAgKiBAZGVwcmVjYXRlZCBwZW5kaW5nIHJlcXVlc3RzIGFyZSBub3cgdHJhY2tlZCB3aXRoIHpvbmVzLlxuICAgKi9cbiAgaW5jcmVhc2VQZW5kaW5nUmVxdWVzdENvdW50KCk6IG51bWJlciB7XG4gICAgdGhpcy5fcGVuZGluZ0NvdW50ICs9IDE7XG4gICAgdGhpcy5fZGlkV29yayA9IHRydWU7XG4gICAgcmV0dXJuIHRoaXMuX3BlbmRpbmdDb3VudDtcbiAgfVxuXG4gIC8qKlxuICAgKiBEZWNyZWFzZXMgdGhlIG51bWJlciBvZiBwZW5kaW5nIHJlcXVlc3RcbiAgICogQGRlcHJlY2F0ZWQgcGVuZGluZyByZXF1ZXN0cyBhcmUgbm93IHRyYWNrZWQgd2l0aCB6b25lc1xuICAgKi9cbiAgZGVjcmVhc2VQZW5kaW5nUmVxdWVzdENvdW50KCk6IG51bWJlciB7XG4gICAgdGhpcy5fcGVuZGluZ0NvdW50IC09IDE7XG4gICAgaWYgKHRoaXMuX3BlbmRpbmdDb3VudCA8IDApIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcigncGVuZGluZyBhc3luYyByZXF1ZXN0cyBiZWxvdyB6ZXJvJyk7XG4gICAgfVxuICAgIHRoaXMuX3J1bkNhbGxiYWNrc0lmUmVhZHkoKTtcbiAgICByZXR1cm4gdGhpcy5fcGVuZGluZ0NvdW50O1xuICB9XG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgYW4gYXNzb2NpYXRlZCBhcHBsaWNhdGlvbiBpcyBzdGFibGVcbiAgICovXG4gIGlzU3RhYmxlKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9pc1pvbmVTdGFibGUgJiYgdGhpcy5fcGVuZGluZ0NvdW50ID09PSAwICYmICF0aGlzLl9uZ1pvbmUuaGFzUGVuZGluZ01hY3JvdGFza3M7XG4gIH1cblxuICBwcml2YXRlIF9ydW5DYWxsYmFja3NJZlJlYWR5KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmlzU3RhYmxlKCkpIHtcbiAgICAgIC8vIFNjaGVkdWxlcyB0aGUgY2FsbCBiYWNrcyBpbiBhIG5ldyBmcmFtZSBzbyB0aGF0IGl0IGlzIGFsd2F5cyBhc3luYy5cbiAgICAgIHNjaGVkdWxlTWljcm9UYXNrKCgpID0+IHtcbiAgICAgICAgd2hpbGUgKHRoaXMuX2NhbGxiYWNrcy5sZW5ndGggIT09IDApIHtcbiAgICAgICAgICBsZXQgY2IgPSB0aGlzLl9jYWxsYmFja3MucG9wKCkgITtcbiAgICAgICAgICBjbGVhclRpbWVvdXQoY2IudGltZW91dElkKTtcbiAgICAgICAgICBjYi5kb25lQ2IodGhpcy5fZGlkV29yayk7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy5fZGlkV29yayA9IGZhbHNlO1xuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIFN0aWxsIG5vdCBzdGFibGUsIHNlbmQgdXBkYXRlcy5cbiAgICAgIGxldCBwZW5kaW5nID0gdGhpcy5nZXRQZW5kaW5nVGFza3MoKTtcbiAgICAgIHRoaXMuX2NhbGxiYWNrcyA9IHRoaXMuX2NhbGxiYWNrcy5maWx0ZXIoKGNiKSA9PiB7XG4gICAgICAgIGlmIChjYi51cGRhdGVDYiAmJiBjYi51cGRhdGVDYihwZW5kaW5nKSkge1xuICAgICAgICAgIGNsZWFyVGltZW91dChjYi50aW1lb3V0SWQpO1xuICAgICAgICAgIHJldHVybiBmYWxzZTtcbiAgICAgICAgfVxuXG4gICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgfSk7XG5cbiAgICAgIHRoaXMuX2RpZFdvcmsgPSB0cnVlO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZ2V0UGVuZGluZ1Rhc2tzKCk6IFBlbmRpbmdNYWNyb3Rhc2tbXSB7XG4gICAgaWYgKCF0aGlzLnRhc2tUcmFja2luZ1pvbmUpIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG5cbiAgICAvLyBDb3B5IHRoZSB0YXNrcyBkYXRhIHNvIHRoYXQgd2UgZG9uJ3QgbGVhayB0YXNrcy5cbiAgICByZXR1cm4gdGhpcy50YXNrVHJhY2tpbmdab25lLm1hY3JvVGFza3MubWFwKCh0OiBUYXNrKSA9PiB7XG4gICAgICByZXR1cm4ge1xuICAgICAgICBzb3VyY2U6IHQuc291cmNlLFxuICAgICAgICAvLyBGcm9tIFRhc2tUcmFja2luZ1pvbmU6XG4gICAgICAgIC8vIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL3pvbmUuanMvYmxvYi9tYXN0ZXIvbGliL3pvbmUtc3BlYy90YXNrLXRyYWNraW5nLnRzI0w0MFxuICAgICAgICBjcmVhdGlvbkxvY2F0aW9uOiAodCBhcyBhbnkpLmNyZWF0aW9uTG9jYXRpb24gYXMgRXJyb3IsXG4gICAgICAgIGRhdGE6IHQuZGF0YVxuICAgICAgfTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgYWRkQ2FsbGJhY2soY2I6IERvbmVDYWxsYmFjaywgdGltZW91dD86IG51bWJlciwgdXBkYXRlQ2I/OiBVcGRhdGVDYWxsYmFjaykge1xuICAgIGxldCB0aW1lb3V0SWQ6IGFueSA9IC0xO1xuICAgIGlmICh0aW1lb3V0ICYmIHRpbWVvdXQgPiAwKSB7XG4gICAgICB0aW1lb3V0SWQgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgICAgdGhpcy5fY2FsbGJhY2tzID0gdGhpcy5fY2FsbGJhY2tzLmZpbHRlcigoY2IpID0+IGNiLnRpbWVvdXRJZCAhPT0gdGltZW91dElkKTtcbiAgICAgICAgY2IodGhpcy5fZGlkV29yaywgdGhpcy5nZXRQZW5kaW5nVGFza3MoKSk7XG4gICAgICB9LCB0aW1lb3V0KTtcbiAgICB9XG4gICAgdGhpcy5fY2FsbGJhY2tzLnB1c2goPFdhaXRDYWxsYmFjaz57ZG9uZUNiOiBjYiwgdGltZW91dElkOiB0aW1lb3V0SWQsIHVwZGF0ZUNiOiB1cGRhdGVDYn0pO1xuICB9XG5cbiAgLyoqXG4gICAqIFdhaXQgZm9yIHRoZSBhcHBsaWNhdGlvbiB0byBiZSBzdGFibGUgd2l0aCBhIHRpbWVvdXQuIElmIHRoZSB0aW1lb3V0IGlzIHJlYWNoZWQgYmVmb3JlIHRoYXRcbiAgICogaGFwcGVucywgdGhlIGNhbGxiYWNrIHJlY2VpdmVzIGEgbGlzdCBvZiB0aGUgbWFjcm8gdGFza3MgdGhhdCB3ZXJlIHBlbmRpbmcsIG90aGVyd2lzZSBudWxsLlxuICAgKlxuICAgKiBAcGFyYW0gZG9uZUNiIFRoZSBjYWxsYmFjayB0byBpbnZva2Ugd2hlbiBBbmd1bGFyIGlzIHN0YWJsZSBvciB0aGUgdGltZW91dCBleHBpcmVzXG4gICAqICAgIHdoaWNoZXZlciBjb21lcyBmaXJzdC5cbiAgICogQHBhcmFtIHRpbWVvdXQgT3B0aW9uYWwuIFRoZSBtYXhpbXVtIHRpbWUgdG8gd2FpdCBmb3IgQW5ndWxhciB0byBiZWNvbWUgc3RhYmxlLiBJZiBub3RcbiAgICogICAgc3BlY2lmaWVkLCB3aGVuU3RhYmxlKCkgd2lsbCB3YWl0IGZvcmV2ZXIuXG4gICAqIEBwYXJhbSB1cGRhdGVDYiBPcHRpb25hbC4gSWYgc3BlY2lmaWVkLCB0aGlzIGNhbGxiYWNrIHdpbGwgYmUgaW52b2tlZCB3aGVuZXZlciB0aGUgc2V0IG9mXG4gICAqICAgIHBlbmRpbmcgbWFjcm90YXNrcyBjaGFuZ2VzLiBJZiB0aGlzIGNhbGxiYWNrIHJldHVybnMgdHJ1ZSBkb25lQ2Igd2lsbCBub3QgYmUgaW52b2tlZFxuICAgKiAgICBhbmQgbm8gZnVydGhlciB1cGRhdGVzIHdpbGwgYmUgaXNzdWVkLlxuICAgKi9cbiAgd2hlblN0YWJsZShkb25lQ2I6IEZ1bmN0aW9uLCB0aW1lb3V0PzogbnVtYmVyLCB1cGRhdGVDYj86IEZ1bmN0aW9uKTogdm9pZCB7XG4gICAgaWYgKHVwZGF0ZUNiICYmICF0aGlzLnRhc2tUcmFja2luZ1pvbmUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICAnVGFzayB0cmFja2luZyB6b25lIGlzIHJlcXVpcmVkIHdoZW4gcGFzc2luZyBhbiB1cGRhdGUgY2FsbGJhY2sgdG8gJyArXG4gICAgICAgICAgJ3doZW5TdGFibGUoKS4gSXMgXCJ6b25lLmpzL2Rpc3QvdGFzay10cmFja2luZy5qc1wiIGxvYWRlZD8nKTtcbiAgICB9XG4gICAgLy8gVGhlc2UgYXJndW1lbnRzIGFyZSAnRnVuY3Rpb24nIGFib3ZlIHRvIGtlZXAgdGhlIHB1YmxpYyBBUEkgc2ltcGxlLlxuICAgIHRoaXMuYWRkQ2FsbGJhY2soZG9uZUNiIGFzIERvbmVDYWxsYmFjaywgdGltZW91dCwgdXBkYXRlQ2IgYXMgVXBkYXRlQ2FsbGJhY2spO1xuICAgIHRoaXMuX3J1bkNhbGxiYWNrc0lmUmVhZHkoKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBHZXQgdGhlIG51bWJlciBvZiBwZW5kaW5nIHJlcXVlc3RzXG4gICAqIEBkZXByZWNhdGVkIHBlbmRpbmcgcmVxdWVzdHMgYXJlIG5vdyB0cmFja2VkIHdpdGggem9uZXNcbiAgICovXG4gIGdldFBlbmRpbmdSZXF1ZXN0Q291bnQoKTogbnVtYmVyIHsgcmV0dXJuIHRoaXMuX3BlbmRpbmdDb3VudDsgfVxuXG4gIC8qKlxuICAgKiBGaW5kIHByb3ZpZGVycyBieSBuYW1lXG4gICAqIEBwYXJhbSB1c2luZyBUaGUgcm9vdCBlbGVtZW50IHRvIHNlYXJjaCBmcm9tXG4gICAqIEBwYXJhbSBwcm92aWRlciBUaGUgbmFtZSBvZiBiaW5kaW5nIHZhcmlhYmxlXG4gICAqIEBwYXJhbSBleGFjdE1hdGNoIFdoZXRoZXIgdXNpbmcgZXhhY3RNYXRjaFxuICAgKi9cbiAgZmluZFByb3ZpZGVycyh1c2luZzogYW55LCBwcm92aWRlcjogc3RyaW5nLCBleGFjdE1hdGNoOiBib29sZWFuKTogYW55W10ge1xuICAgIC8vIFRPRE8oanVsaWVtcik6IGltcGxlbWVudC5cbiAgICByZXR1cm4gW107XG4gIH1cbn1cblxuLyoqXG4gKiBBIGdsb2JhbCByZWdpc3RyeSBvZiB7QGxpbmsgVGVzdGFiaWxpdHl9IGluc3RhbmNlcyBmb3Igc3BlY2lmaWMgZWxlbWVudHMuXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBUZXN0YWJpbGl0eVJlZ2lzdHJ5IHtcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfYXBwbGljYXRpb25zID0gbmV3IE1hcDxhbnksIFRlc3RhYmlsaXR5PigpO1xuXG4gIGNvbnN0cnVjdG9yKCkgeyBfdGVzdGFiaWxpdHlHZXR0ZXIuYWRkVG9XaW5kb3codGhpcyk7IH1cblxuICAvKipcbiAgICogUmVnaXN0ZXJzIGFuIGFwcGxpY2F0aW9uIHdpdGggYSB0ZXN0YWJpbGl0eSBob29rIHNvIHRoYXQgaXQgY2FuIGJlIHRyYWNrZWRcbiAgICogQHBhcmFtIHRva2VuIHRva2VuIG9mIGFwcGxpY2F0aW9uLCByb290IGVsZW1lbnRcbiAgICogQHBhcmFtIHRlc3RhYmlsaXR5IFRlc3RhYmlsaXR5IGhvb2tcbiAgICovXG4gIHJlZ2lzdGVyQXBwbGljYXRpb24odG9rZW46IGFueSwgdGVzdGFiaWxpdHk6IFRlc3RhYmlsaXR5KSB7XG4gICAgdGhpcy5fYXBwbGljYXRpb25zLnNldCh0b2tlbiwgdGVzdGFiaWxpdHkpO1xuICB9XG5cbiAgLyoqXG4gICAqIFVucmVnaXN0ZXJzIGFuIGFwcGxpY2F0aW9uLlxuICAgKiBAcGFyYW0gdG9rZW4gdG9rZW4gb2YgYXBwbGljYXRpb24sIHJvb3QgZWxlbWVudFxuICAgKi9cbiAgdW5yZWdpc3RlckFwcGxpY2F0aW9uKHRva2VuOiBhbnkpIHsgdGhpcy5fYXBwbGljYXRpb25zLmRlbGV0ZSh0b2tlbik7IH1cblxuICAvKipcbiAgICogVW5yZWdpc3RlcnMgYWxsIGFwcGxpY2F0aW9uc1xuICAgKi9cbiAgdW5yZWdpc3RlckFsbEFwcGxpY2F0aW9ucygpIHsgdGhpcy5fYXBwbGljYXRpb25zLmNsZWFyKCk7IH1cblxuICAvKipcbiAgICogR2V0IGEgdGVzdGFiaWxpdHkgaG9vayBhc3NvY2lhdGVkIHdpdGggdGhlIGFwcGxpY2F0aW9uXG4gICAqIEBwYXJhbSBlbGVtIHJvb3QgZWxlbWVudFxuICAgKi9cbiAgZ2V0VGVzdGFiaWxpdHkoZWxlbTogYW55KTogVGVzdGFiaWxpdHl8bnVsbCB7IHJldHVybiB0aGlzLl9hcHBsaWNhdGlvbnMuZ2V0KGVsZW0pIHx8IG51bGw7IH1cblxuICAvKipcbiAgICogR2V0IGFsbCByZWdpc3RlcmVkIHRlc3RhYmlsaXRpZXNcbiAgICovXG4gIGdldEFsbFRlc3RhYmlsaXRpZXMoKTogVGVzdGFiaWxpdHlbXSB7IHJldHVybiBBcnJheS5mcm9tKHRoaXMuX2FwcGxpY2F0aW9ucy52YWx1ZXMoKSk7IH1cblxuICAvKipcbiAgICogR2V0IGFsbCByZWdpc3RlcmVkIGFwcGxpY2F0aW9ucyhyb290IGVsZW1lbnRzKVxuICAgKi9cbiAgZ2V0QWxsUm9vdEVsZW1lbnRzKCk6IGFueVtdIHsgcmV0dXJuIEFycmF5LmZyb20odGhpcy5fYXBwbGljYXRpb25zLmtleXMoKSk7IH1cblxuICAvKipcbiAgICogRmluZCB0ZXN0YWJpbGl0eSBvZiBhIG5vZGUgaW4gdGhlIFRyZWVcbiAgICogQHBhcmFtIGVsZW0gbm9kZVxuICAgKiBAcGFyYW0gZmluZEluQW5jZXN0b3JzIHdoZXRoZXIgZmluZGluZyB0ZXN0YWJpbGl0eSBpbiBhbmNlc3RvcnMgaWYgdGVzdGFiaWxpdHkgd2FzIG5vdCBmb3VuZCBpblxuICAgKiBjdXJyZW50IG5vZGVcbiAgICovXG4gIGZpbmRUZXN0YWJpbGl0eUluVHJlZShlbGVtOiBOb2RlLCBmaW5kSW5BbmNlc3RvcnM6IGJvb2xlYW4gPSB0cnVlKTogVGVzdGFiaWxpdHl8bnVsbCB7XG4gICAgcmV0dXJuIF90ZXN0YWJpbGl0eUdldHRlci5maW5kVGVzdGFiaWxpdHlJblRyZWUodGhpcywgZWxlbSwgZmluZEluQW5jZXN0b3JzKTtcbiAgfVxufVxuXG4vKipcbiAqIEFkYXB0ZXIgaW50ZXJmYWNlIGZvciByZXRyaWV2aW5nIHRoZSBgVGVzdGFiaWxpdHlgIHNlcnZpY2UgYXNzb2NpYXRlZCBmb3IgYVxuICogcGFydGljdWxhciBjb250ZXh0LlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGludGVyZmFjZSBHZXRUZXN0YWJpbGl0eSB7XG4gIGFkZFRvV2luZG93KHJlZ2lzdHJ5OiBUZXN0YWJpbGl0eVJlZ2lzdHJ5KTogdm9pZDtcbiAgZmluZFRlc3RhYmlsaXR5SW5UcmVlKHJlZ2lzdHJ5OiBUZXN0YWJpbGl0eVJlZ2lzdHJ5LCBlbGVtOiBhbnksIGZpbmRJbkFuY2VzdG9yczogYm9vbGVhbik6XG4gICAgICBUZXN0YWJpbGl0eXxudWxsO1xufVxuXG5jbGFzcyBfTm9vcEdldFRlc3RhYmlsaXR5IGltcGxlbWVudHMgR2V0VGVzdGFiaWxpdHkge1xuICBhZGRUb1dpbmRvdyhyZWdpc3RyeTogVGVzdGFiaWxpdHlSZWdpc3RyeSk6IHZvaWQge31cbiAgZmluZFRlc3RhYmlsaXR5SW5UcmVlKHJlZ2lzdHJ5OiBUZXN0YWJpbGl0eVJlZ2lzdHJ5LCBlbGVtOiBhbnksIGZpbmRJbkFuY2VzdG9yczogYm9vbGVhbik6XG4gICAgICBUZXN0YWJpbGl0eXxudWxsIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxufVxuXG4vKipcbiAqIFNldCB0aGUge0BsaW5rIEdldFRlc3RhYmlsaXR5fSBpbXBsZW1lbnRhdGlvbiB1c2VkIGJ5IHRoZSBBbmd1bGFyIHRlc3RpbmcgZnJhbWV3b3JrLlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gc2V0VGVzdGFiaWxpdHlHZXR0ZXIoZ2V0dGVyOiBHZXRUZXN0YWJpbGl0eSk6IHZvaWQge1xuICBfdGVzdGFiaWxpdHlHZXR0ZXIgPSBnZXR0ZXI7XG59XG5cbmxldCBfdGVzdGFiaWxpdHlHZXR0ZXI6IEdldFRlc3RhYmlsaXR5ID0gbmV3IF9Ob29wR2V0VGVzdGFiaWxpdHkoKTtcbiJdfQ==