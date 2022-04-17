/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { EventEmitter } from '../event_emitter';
/**
 * An injectable service for executing work inside or outside of the Angular zone.
 *
 * The most common use of this service is to optimize performance when starting a work consisting of
 * one or more asynchronous tasks that don't require UI updates or error handling to be handled by
 * Angular. Such tasks can be kicked off via {@link #runOutsideAngular} and if needed, these tasks
 * can reenter the Angular zone via {@link #run}.
 *
 * <!-- TODO: add/fix links to:
 *   - docs explaining zones and the use of zones in Angular and change-detection
 *   - link to runOutsideAngular/run (throughout this file!)
 *   -->
 *
 * @usageNotes
 * ### Example
 *
 * ```
 * import {Component, NgZone} from '@angular/core';
 * import {NgIf} from '@angular/common';
 *
 * @Component({
 *   selector: 'ng-zone-demo',
 *   template: `
 *     <h2>Demo: NgZone</h2>
 *
 *     <p>Progress: {{progress}}%</p>
 *     <p *ngIf="progress >= 100">Done processing {{label}} of Angular zone!</p>
 *
 *     <button (click)="processWithinAngularZone()">Process within Angular zone</button>
 *     <button (click)="processOutsideOfAngularZone()">Process outside of Angular zone</button>
 *   `,
 * })
 * export class NgZoneDemo {
 *   progress: number = 0;
 *   label: string;
 *
 *   constructor(private _ngZone: NgZone) {}
 *
 *   // Loop inside the Angular zone
 *   // so the UI DOES refresh after each setTimeout cycle
 *   processWithinAngularZone() {
 *     this.label = 'inside';
 *     this.progress = 0;
 *     this._increaseProgress(() => console.log('Inside Done!'));
 *   }
 *
 *   // Loop outside of the Angular zone
 *   // so the UI DOES NOT refresh after each setTimeout cycle
 *   processOutsideOfAngularZone() {
 *     this.label = 'outside';
 *     this.progress = 0;
 *     this._ngZone.runOutsideAngular(() => {
 *       this._increaseProgress(() => {
 *         // reenter the Angular zone and display done
 *         this._ngZone.run(() => { console.log('Outside Done!'); });
 *       });
 *     });
 *   }
 *
 *   _increaseProgress(doneCallback: () => void) {
 *     this.progress += 1;
 *     console.log(`Current progress: ${this.progress}%`);
 *
 *     if (this.progress < 100) {
 *       window.setTimeout(() => this._increaseProgress(doneCallback), 10);
 *     } else {
 *       doneCallback();
 *     }
 *   }
 * }
 * ```
 *
 * @publicApi
 */
var NgZone = /** @class */ (function () {
    function NgZone(_a) {
        var _b = _a.enableLongStackTrace, enableLongStackTrace = _b === void 0 ? false : _b;
        this.hasPendingMicrotasks = false;
        this.hasPendingMacrotasks = false;
        /**
         * Whether there are no outstanding microtasks or macrotasks.
         */
        this.isStable = true;
        /**
         * Notifies when code enters Angular Zone. This gets fired first on VM Turn.
         */
        this.onUnstable = new EventEmitter(false);
        /**
         * Notifies when there is no more microtasks enqueued in the current VM Turn.
         * This is a hint for Angular to do change detection, which may enqueue more microtasks.
         * For this reason this event can fire multiple times per VM Turn.
         */
        this.onMicrotaskEmpty = new EventEmitter(false);
        /**
         * Notifies when the last `onMicrotaskEmpty` has run and there are no more microtasks, which
         * implies we are about to relinquish VM turn.
         * This event gets called just once.
         */
        this.onStable = new EventEmitter(false);
        /**
         * Notifies that an error has been delivered.
         */
        this.onError = new EventEmitter(false);
        if (typeof Zone == 'undefined') {
            throw new Error("In this configuration Angular requires Zone.js");
        }
        Zone.assertZonePatched();
        var self = this;
        self._nesting = 0;
        self._outer = self._inner = Zone.current;
        if (Zone['wtfZoneSpec']) {
            self._inner = self._inner.fork(Zone['wtfZoneSpec']);
        }
        if (Zone['TaskTrackingZoneSpec']) {
            self._inner = self._inner.fork(new Zone['TaskTrackingZoneSpec']);
        }
        if (enableLongStackTrace && Zone['longStackTraceZoneSpec']) {
            self._inner = self._inner.fork(Zone['longStackTraceZoneSpec']);
        }
        forkInnerZoneWithAngularBehavior(self);
    }
    NgZone.isInAngularZone = function () { return Zone.current.get('isAngularZone') === true; };
    NgZone.assertInAngularZone = function () {
        if (!NgZone.isInAngularZone()) {
            throw new Error('Expected to be in Angular Zone, but it is not!');
        }
    };
    NgZone.assertNotInAngularZone = function () {
        if (NgZone.isInAngularZone()) {
            throw new Error('Expected to not be in Angular Zone, but it is!');
        }
    };
    /**
     * Executes the `fn` function synchronously within the Angular zone and returns value returned by
     * the function.
     *
     * Running functions via `run` allows you to reenter Angular zone from a task that was executed
     * outside of the Angular zone (typically started via {@link #runOutsideAngular}).
     *
     * Any future tasks or microtasks scheduled from within this function will continue executing from
     * within the Angular zone.
     *
     * If a synchronous error happens it will be rethrown and not reported via `onError`.
     */
    NgZone.prototype.run = function (fn, applyThis, applyArgs) {
        return this._inner.run(fn, applyThis, applyArgs);
    };
    /**
     * Executes the `fn` function synchronously within the Angular zone as a task and returns value
     * returned by the function.
     *
     * Running functions via `run` allows you to reenter Angular zone from a task that was executed
     * outside of the Angular zone (typically started via {@link #runOutsideAngular}).
     *
     * Any future tasks or microtasks scheduled from within this function will continue executing from
     * within the Angular zone.
     *
     * If a synchronous error happens it will be rethrown and not reported via `onError`.
     */
    NgZone.prototype.runTask = function (fn, applyThis, applyArgs, name) {
        var zone = this._inner;
        var task = zone.scheduleEventTask('NgZoneEvent: ' + name, fn, EMPTY_PAYLOAD, noop, noop);
        try {
            return zone.runTask(task, applyThis, applyArgs);
        }
        finally {
            zone.cancelTask(task);
        }
    };
    /**
     * Same as `run`, except that synchronous errors are caught and forwarded via `onError` and not
     * rethrown.
     */
    NgZone.prototype.runGuarded = function (fn, applyThis, applyArgs) {
        return this._inner.runGuarded(fn, applyThis, applyArgs);
    };
    /**
     * Executes the `fn` function synchronously in Angular's parent zone and returns value returned by
     * the function.
     *
     * Running functions via {@link #runOutsideAngular} allows you to escape Angular's zone and do
     * work that
     * doesn't trigger Angular change-detection or is subject to Angular's error handling.
     *
     * Any future tasks or microtasks scheduled from within this function will continue executing from
     * outside of the Angular zone.
     *
     * Use {@link #run} to reenter the Angular zone and do work that updates the application model.
     */
    NgZone.prototype.runOutsideAngular = function (fn) {
        return this._outer.run(fn);
    };
    return NgZone;
}());
export { NgZone };
function noop() { }
var EMPTY_PAYLOAD = {};
function checkStable(zone) {
    if (zone._nesting == 0 && !zone.hasPendingMicrotasks && !zone.isStable) {
        try {
            zone._nesting++;
            zone.onMicrotaskEmpty.emit(null);
        }
        finally {
            zone._nesting--;
            if (!zone.hasPendingMicrotasks) {
                try {
                    zone.runOutsideAngular(function () { return zone.onStable.emit(null); });
                }
                finally {
                    zone.isStable = true;
                }
            }
        }
    }
}
function forkInnerZoneWithAngularBehavior(zone) {
    zone._inner = zone._inner.fork({
        name: 'angular',
        properties: { 'isAngularZone': true },
        onInvokeTask: function (delegate, current, target, task, applyThis, applyArgs) {
            try {
                onEnter(zone);
                return delegate.invokeTask(target, task, applyThis, applyArgs);
            }
            finally {
                onLeave(zone);
            }
        },
        onInvoke: function (delegate, current, target, callback, applyThis, applyArgs, source) {
            try {
                onEnter(zone);
                return delegate.invoke(target, callback, applyThis, applyArgs, source);
            }
            finally {
                onLeave(zone);
            }
        },
        onHasTask: function (delegate, current, target, hasTaskState) {
            delegate.hasTask(target, hasTaskState);
            if (current === target) {
                // We are only interested in hasTask events which originate from our zone
                // (A child hasTask event is not interesting to us)
                if (hasTaskState.change == 'microTask') {
                    zone.hasPendingMicrotasks = hasTaskState.microTask;
                    checkStable(zone);
                }
                else if (hasTaskState.change == 'macroTask') {
                    zone.hasPendingMacrotasks = hasTaskState.macroTask;
                }
            }
        },
        onHandleError: function (delegate, current, target, error) {
            delegate.handleError(target, error);
            zone.runOutsideAngular(function () { return zone.onError.emit(error); });
            return false;
        }
    });
}
function onEnter(zone) {
    zone._nesting++;
    if (zone.isStable) {
        zone.isStable = false;
        zone.onUnstable.emit(null);
    }
}
function onLeave(zone) {
    zone._nesting--;
    checkStable(zone);
}
/**
 * Provides a noop implementation of `NgZone` which does nothing. This zone requires explicit calls
 * to framework to perform rendering.
 */
var NoopNgZone = /** @class */ (function () {
    function NoopNgZone() {
        this.hasPendingMicrotasks = false;
        this.hasPendingMacrotasks = false;
        this.isStable = true;
        this.onUnstable = new EventEmitter();
        this.onMicrotaskEmpty = new EventEmitter();
        this.onStable = new EventEmitter();
        this.onError = new EventEmitter();
    }
    NoopNgZone.prototype.run = function (fn) { return fn(); };
    NoopNgZone.prototype.runGuarded = function (fn) { return fn(); };
    NoopNgZone.prototype.runOutsideAngular = function (fn) { return fn(); };
    NoopNgZone.prototype.runTask = function (fn) { return fn(); };
    return NoopNgZone;
}());
export { NoopNgZone };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfem9uZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3pvbmUvbmdfem9uZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFFSCxPQUFPLEVBQUMsWUFBWSxFQUFDLE1BQU0sa0JBQWtCLENBQUM7QUFFOUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7R0F5RUc7QUFDSDtJQWlDRSxnQkFBWSxFQUE4QjtZQUE3Qiw0QkFBNEIsRUFBNUIsaURBQTRCO1FBaENoQyx5QkFBb0IsR0FBWSxLQUFLLENBQUM7UUFDdEMseUJBQW9CLEdBQVksS0FBSyxDQUFDO1FBRS9DOztXQUVHO1FBQ00sYUFBUSxHQUFZLElBQUksQ0FBQztRQUVsQzs7V0FFRztRQUNNLGVBQVUsR0FBc0IsSUFBSSxZQUFZLENBQUMsS0FBSyxDQUFDLENBQUM7UUFFakU7Ozs7V0FJRztRQUNNLHFCQUFnQixHQUFzQixJQUFJLFlBQVksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUV2RTs7OztXQUlHO1FBQ00sYUFBUSxHQUFzQixJQUFJLFlBQVksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUUvRDs7V0FFRztRQUNNLFlBQU8sR0FBc0IsSUFBSSxZQUFZLENBQUMsS0FBSyxDQUFDLENBQUM7UUFHNUQsSUFBSSxPQUFPLElBQUksSUFBSSxXQUFXLEVBQUU7WUFDOUIsTUFBTSxJQUFJLEtBQUssQ0FBQyxnREFBZ0QsQ0FBQyxDQUFDO1NBQ25FO1FBRUQsSUFBSSxDQUFDLGlCQUFpQixFQUFFLENBQUM7UUFDekIsSUFBTSxJQUFJLEdBQUcsSUFBNEIsQ0FBQztRQUMxQyxJQUFJLENBQUMsUUFBUSxHQUFHLENBQUMsQ0FBQztRQUVsQixJQUFJLENBQUMsTUFBTSxHQUFHLElBQUksQ0FBQyxNQUFNLEdB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O1FBQ2pCLElBQUksQ0FBQyxRQUFRLEdBQUcsS0FBSyxDQUFDO1FBQ3RCLElBQUksQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDO0tBQzVCO0FBQ0gsQ0FBQztBQUVELFNBQVMsT0FBTyxDQUFDLElBQW1CO0lBQ2xDLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQztJQUNoQixXQUFXLENBQUMsSUFBSSxDQUFDLENBQUM7QUFDcEIsQ0FBQztBQUVEOzs7R0FHRztBQUNIO0lBQUE7UUFDVyx5QkFBb0IsR0FBWSxLQUFLLENBQUM7UUFDdEMseUJBQW9CLEdBQVksS0FBSyxDQUFDO1FBQ3RDLGFBQVEsR0FBWSxJQUFJLENBQUM7UUFDekIsZUFBVSxHQUFzQixJQUFJLFlBQVksRUFBRSxDQUFDO1FBQ25ELHFCQUFnQixHQUFzQixJQUFJLFlBQVksRUFBRSxDQUFDO1FBQ3pELGFBQVEsR0FBc0IsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUNqRCxZQUFPLEdBQXNCLElBQUksWUFBWSxFQUFFLENBQUM7SUFTM0QsQ0FBQztJQVBDLHdCQUFHLEdBQUgsVUFBSSxFQUFhLElBQVMsT0FBTyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFFeEMsK0JBQVUsR0FBVixVQUFXLEVBQWEsSUFBUyxPQUFPLEVBQUUsRUFBRSxDQUFDLENBQUMsQ0FBQztJQUUvQyxzQ0FBaUIsR0FBakIsVUFBa0IsRUFBYSxJQUFTLE9BQU8sRUFBRSxFQUFFLENBQUMsQ0FBQyxDQUFDO0lBRXRELDRCQUFPLEdBQVAsVUFBVyxFQUFhLElBQVMsT0FBTyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFDakQsaUJBQUM7QUFBRCxDQUFDLEFBaEJELElBZ0JDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0V2ZW50RW1pdHRlcn0gZnJvbSAnLi4vZXZlbnRfZW1pdHRlcic7XG5cbi8qKlxuICogQW4gaW5qZWN0YWJsZSBzZXJ2aWNlIGZvciBleGVjdXRpbmcgd29yayBpbnNpZGUgb3Igb3V0c2lkZSBvZiB0aGUgQW5ndWxhciB6b25lLlxuICpcbiAqIFRoZSBtb3N0IGNvbW1vbiB1c2Ugb2YgdGhpcyBzZXJ2aWNlIGlzIHRvIG9wdGltaXplIHBlcmZvcm1hbmNlIHdoZW4gc3RhcnRpbmcgYSB3b3JrIGNvbnNpc3Rpbmcgb2ZcbiAqIG9uZSBvciBtb3JlIGFzeW5jaHJvbm91cyB0YXNrcyB0aGF0IGRvbid0IHJlcXVpcmUgVUkgdXBkYXRlcyBvciBlcnJvciBoYW5kbGluZyB0byBiZSBoYW5kbGVkIGJ5XG4gKiBBbmd1bGFyLiBTdWNoIHRhc2tzIGNhbiBiZSBraWNrZWQgb2ZmIHZpYSB7QGxpbmsgI3J1bk91dHNpZGVBbmd1bGFyfSBhbmQgaWYgbmVlZGVkLCB0aGVzZSB0YXNrc1xuICogY2FuIHJlZW50ZXIgdGhlIEFuZ3VsYXIgem9uZSB2aWEge0BsaW5rICNydW59LlxuICpcbiAqIDwhLS0gVE9ETzogYWRkL2ZpeCBsaW5rcyB0bzpcbiAqICAgLSBkb2NzIGV4cGxhaW5pbmcgem9uZXMgYW5kIHRoZSB1c2Ugb2Ygem9uZXMgaW4gQW5ndWxhciBhbmQgY2hhbmdlLWRldGVjdGlvblxuICogICAtIGxpbmsgdG8gcnVuT3V0c2lkZUFuZ3VsYXIvcnVuICh0aHJvdWdob3V0IHRoaXMgZmlsZSEpXG4gKiAgIC0tPlxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiAjIyMgRXhhbXBsZVxuICpcbiAqIGBgYFxuICogaW1wb3J0IHtDb21wb25lbnQsIE5nWm9uZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG4gKiBpbXBvcnQge05nSWZ9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG4gKlxuICogQENvbXBvbmVudCh7XG4gKiAgIHNlbGVjdG9yOiAnbmctem9uZS1kZW1vJyxcbiAqICAgdGVtcGxhdGU6IGBcbiAqICAgICA8aDI+RGVtbzogTmdab25lPC9oMj5cbiAqXG4gKiAgICAgPHA+UHJvZ3Jlc3M6IHt7cHJvZ3Jlc3N9fSU8L3A+XG4gKiAgICAgPHAgKm5nSWY9XCJwcm9ncmVzcyA+PSAxMDBcIj5Eb25lIHByb2Nlc3Npbmcge3tsYWJlbH19IG9mIEFuZ3VsYXIgem9uZSE8L3A+XG4gKlxuICogICAgIDxidXR0b24gKGNsaWNrKT1cInByb2Nlc3NXaXRoaW5Bbmd1bGFyWm9uZSgpXCI+UHJvY2VzcyB3aXRoaW4gQW5ndWxhciB6b25lPC9idXR0b24+XG4gKiAgICAgPGJ1dHRvbiAoY2xpY2spPVwicHJvY2Vzc091dHNpZGVPZkFuZ3VsYXJab25lKClcIj5Qcm9jZXNzIG91dHNpZGUgb2YgQW5ndWxhciB6b25lPC9idXR0b24+XG4gKiAgIGAsXG4gKiB9KVxuICogZXhwb3J0IGNsYXNzIE5nWm9uZURlbW8ge1xuICogICBwcm9ncmVzczogbnVtYmVyID0gMDtcbiAqICAgbGFiZWw6IHN0cmluZztcbiAqXG4gKiAgIGNvbnN0cnVjdG9yKHByaXZhdGUgX25nWm9uZTogTmdab25lKSB7fVxuICpcbiAqICAgLy8gTG9vcCBpbnNpZGUgdGhlIEFuZ3VsYXIgem9uZVxuICogICAvLyBzbyB0aGUgVUkgRE9FUyByZWZyZXNoIGFmdGVyIGVhY2ggc2V0VGltZW91dCBjeWNsZVxuICogICBwcm9jZXNzV2l0aGluQW5ndWxhclpvbmUoKSB7XG4gKiAgICAgdGhpcy5sYWJlbCA9ICdpbnNpZGUnO1xuICogICAgIHRoaXMucHJvZ3Jlc3MgPSAwO1xuICogICAgIHRoaXMuX2luY3JlYXNlUHJvZ3Jlc3MoKCkgPT4gY29uc29sZS5sb2coJ0luc2lkZSBEb25lIScpKTtcbiAqICAgfVxuICpcbiAqICAgLy8gTG9vcCBvdXRzaWRlIG9mIHRoZSBBbmd1bGFyIHpvbmVcbiAqICAgLy8gc28gdGhlIFVJIERPRVMgTk9UIHJlZnJlc2ggYWZ0ZXIgZWFjaCBzZXRUaW1lb3V0IGN5Y2xlXG4gKiAgIHByb2Nlc3NPdXRzaWRlT2ZBbmd1bGFyWm9uZSgpIHtcbiAqICAgICB0aGlzLmxhYmVsID0gJ291dHNpZGUnO1xuICogICAgIHRoaXMucHJvZ3Jlc3MgPSAwO1xuICogICAgIHRoaXMuX25nWm9uZS5ydW5PdXRzaWRlQW5ndWxhcigoKSA9PiB7XG4gKiAgICAgICB0aGlzLl9pbmNyZWFzZVByb2dyZXNzKCgpID0+IHtcbiAqICAgICAgICAgLy8gcmVlbnRlciB0aGUgQW5ndWxhciB6b25lIGFuZCBkaXNwbGF5IGRvbmVcbiAqICAgICAgICAgdGhpcy5fbmdab25lLnJ1bigoKSA9PiB7IGNvbnNvbGUubG9nKCdPdXRzaWRlIERvbmUhJyk7IH0pO1xuICogICAgICAgfSk7XG4gKiAgICAgfSk7XG4gKiAgIH1cbiAqXG4gKiAgIF9pbmNyZWFzZVByb2dyZXNzKGRvbmVDYWxsYmFjazogKCkgPT4gdm9pZCkge1xuICogICAgIHRoaXMucHJvZ3Jlc3MgKz0gMTtcbiAqICAgICBjb25zb2xlLmxvZyhgQ3VycmVudCBwcm9ncmVzczogJHt0aGlzLnByb2dyZXNzfSVgKTtcbiAqXG4gKiAgICAgaWYgKHRoaXMucHJvZ3Jlc3MgPCAxMDApIHtcbiAqICAgICAgIHdpbmRvdy5zZXRUaW1lb3V0KCgpID0+IHRoaXMuX2luY3JlYXNlUHJvZ3Jlc3MoZG9uZUNhbGxiYWNrKSwgMTApO1xuICogICAgIH0gZWxzZSB7XG4gKiAgICAgICBkb25lQ2FsbGJhY2soKTtcbiAqICAgICB9XG4gKiAgIH1cbiAqIH1cbiAqIGBgYFxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuZXhwb3J0IGNsYXNzIE5nWm9uZSB7XG4gIHJlYWRvbmx5IGhhc1BlbmRpbmdNaWNyb3Rhc2tzOiBib29sZWFuID0gZmFsc2U7XG4gIHJlYWRvbmx5IGhhc1BlbmRpbmdNYWNyb3Rhc2tzOiBib29sZWFuID0gZmFsc2U7XG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgdGhlcmUgYXJlIG5vIG91dHN0YW5kaW5nIG1pY3JvdGFza3Mgb3IgbWFjcm90YXNrcy5cbiAgICovXG4gIHJlYWRvbmx5IGlzU3RhYmxlOiBib29sZWFuID0gdHJ1ZTtcblxuICAvKipcbiAgICogTm90aWZpZXMgd2hlbiBjb2RlIGVudGVycyBBbmd1bGFyIFpvbmUuIFRoaXMgZ2V0cyBmaXJlZCBmaXJzdCBvbiBWTSBUdXJuLlxuICAgKi9cbiAgcmVhZG9ubHkgb25VbnN0YWJsZTogRXZlbnRFbWl0dGVyPGFueT4gPSBuZXcgRXZlbnRFbWl0dGVyKGZhbHNlKTtcblxuICAvKipcbiAgICogTm90aWZpZXMgd2hlbiB0aGVyZSBpcyBubyBtb3JlIG1pY3JvdGFza3MgZW5xdWV1ZWQgaW4gdGhlIGN1cnJlbnQgVk0gVHVybi5cbiAgICogVGhpcyBpcyBhIGhpbnQgZm9yIEFuZ3VsYXIgdG8gZG8gY2hhbmdlIGRldGVjdGlvbiwgd2hpY2ggbWF5IGVucXVldWUgbW9yZSBtaWNyb3Rhc2tzLlxuICAgKiBGb3IgdGhpcyByZWFzb24gdGhpcyBldmVudCBjYW4gZmlyZSBtdWx0aXBsZSB0aW1lcyBwZXIgVk0gVHVybi5cbiAgICovXG4gIHJlYWRvbmx5IG9uTWljcm90YXNrRW1wdHk6IEV2ZW50RW1pdHRlcjxhbnk+ID0gbmV3IEV2ZW50RW1pdHRlcihmYWxzZSk7XG5cbiAgLyoqXG4gICAqIE5vdGlmaWVzIHdoZW4gdGhlIGxhc3QgYG9uTWljcm90YXNrRW1wdHlgIGhhcyBydW4gYW5kIHRoZXJlIGFyZSBubyBtb3JlIG1pY3JvdGFza3MsIHdoaWNoXG4gICAqIGltcGxpZXMgd2UgYXJlIGFib3V0IHRvIHJlbGlucXVpc2ggVk0gdHVybi5cbiAgICogVGhpcyBldmVudCBnZXRzIGNhbGxlZCBqdXN0IG9uY2UuXG4gICAqL1xuICByZWFkb25seSBvblN0YWJsZTogRXZlbnRFbWl0dGVyPGFueT4gPSBuZXcgRXZlbnRFbWl0dGVyKGZhbHNlKTtcblxuICAvKipcbiAgICogTm90aWZpZXMgdGhhdCBhbiBlcnJvciBoYXMgYmVlbiBkZWxpdmVyZWQuXG4gICAqL1xuICByZWFkb25seSBvbkVycm9yOiBFdmVudEVtaXR0ZXI8YW55PiA9IG5ldyBFdmVudEVtaXR0ZXIoZmFsc2UpO1xuXG4gIGNvbnN0cnVjdG9yKHtlbmFibGVMb25nU3RhY2tUcmFjZSA9IGZhbHNlfSkge1xuICAgIGlmICh0eXBlb2YgWm9uZSA9PSAndW5kZWZpbmVkJykge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBJbiB0aGlzIGNvbmZpZ3VyYXRpb24gQW5ndWxhciByZXF1aXJlcyBab25lLmpzYCk7XG4gICAgfVxuXG4gICAgWm9uZS5hc3NlcnRab25lUGF0Y2hlZCgpO1xuICAgIGNvbnN0IHNlbGYgPSB0aGlzIGFzIGFueSBhcyBOZ1pvbmVQcml2YXRlO1xuICAgIHNlbGYuX25lc3RpbmcgPSAwO1xuXG4gICAgc2VsZi5fb3V0ZXIgPSBzZWxmLl9pbm5lciA9IFpvbmUuY3VycmVudDtcblxuICAgIGlmICgoWm9uZSBhcyBhbnkpWyd3dGZab25lU3BlYyddKSB7XG4gICAgICBzZWxmLl9pbm5lciA9IHNlbGYuX2lubmVyLmZvcmsoKFpvbmUgYXMgYW55KVsnd3RmWm9uZVNwZWMnXSk7XG4gICAgfVxuXG4gICAgaWYgKChab25lIGFzIGFueSlbJ1Rhc2tUcmFja2luZ1pvbmVTcGVjJ10pIHtcbiAgICAgIHNlbGYuX2lubmVyID0gc2VsZi5faW5uZXIuZm9yayhuZXcgKChab25lIGFzIGFueSlbJ1Rhc2tUcmFja2luZ1pvbmVTcGVjJ10gYXMgYW55KSk7XG4gICAgfVxuXG4gICAgaWYgKGVuYWJsZUxvbmdTdGFja1RyYWNlICYmIChab25lIGFzIGFueSlbJ2xvbmdTdGFja1RyYWNlWm9uZVNwZWMnXSkge1xuICAgICAgc2VsZi5faW5uZXIgPSBzZWxmLl9pbm5lci5mb3JrKChab25lIGFzIGFueSlbJ2xvbmdTdGFja1RyYWNlWm9uZVNwZWMnXSk7XG4gICAgfVxuXG4gICAgZm9ya0lubmVyWm9uZVdpdGhBbmd1bGFyQmVoYXZpb3Ioc2VsZik7XG4gIH1cblxuICBzdGF0aWMgaXNJbkFuZ3VsYXJab25lKCk6IGJvb2xlYW4geyByZXR1cm4gWm9uZS5jdXJyZW50LmdldCgnaXNBbmd1bGFyWm9uZScpID09PSB0cnVlOyB9XG5cbiAgc3RhdGljIGFzc2VydEluQW5ndWxhclpvbmUoKTogdm9pZCB7XG4gICAgaWYgKCFOZ1pvbmUuaXNJbkFuZ3VsYXJab25lKCkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignRXhwZWN0ZWQgdG8gYmUgaW4gQW5ndWxhciBab25lLCBidXQgaXQgaXMgbm90IScpO1xuICAgIH1cbiAgfVxuXG4gIHN0YXRpYyBhc3NlcnROb3RJbkFuZ3VsYXJab25lKCk6IHZvaWQge1xuICAgIGlmIChOZ1pvbmUuaXNJbkFuZ3VsYXJab25lKCkpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignRXhwZWN0ZWQgdG8gbm90IGJlIGluIEFuZ3VsYXIgWm9uZSwgYnV0IGl0IGlzIScpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBFeGVjdXRlcyB0aGUgYGZuYCBmdW5jdGlvbiBzeW5jaHJvbm91c2x5IHdpdGhpbiB0aGUgQW5ndWxhciB6b25lIGFuZCByZXR1cm5zIHZhbHVlIHJldHVybmVkIGJ5XG4gICAqIHRoZSBmdW5jdGlvbi5cbiAgICpcbiAgICogUnVubmluZyBmdW5jdGlvbnMgdmlhIGBydW5gIGFsbG93cyB5b3UgdG8gcmVlbnRlciBBbmd1bGFyIHpvbmUgZnJvbSBhIHRhc2sgdGhhdCB3YXMgZXhlY3V0ZWRcbiAgICogb3V0c2lkZSBvZiB0aGUgQW5ndWxhciB6b25lICh0eXBpY2FsbHkgc3RhcnRlZCB2aWEge0BsaW5rICNydW5PdXRzaWRlQW5ndWxhcn0pLlxuICAgKlxuICAgKiBBbnkgZnV0dXJlIHRhc2tzIG9yIG1pY3JvdGFza3Mgc2NoZWR1bGVkIGZyb20gd2l0aGluIHRoaXMgZnVuY3Rpb24gd2lsbCBjb250aW51ZSBleGVjdXRpbmcgZnJvbVxuICAgKiB3aXRoaW4gdGhlIEFuZ3VsYXIgem9uZS5cbiAgICpcbiAgICogSWYgYSBzeW5jaHJvbm91cyBlcnJvciBoYXBwZW5zIGl0IHdpbGwgYmUgcmV0aHJvd24gYW5kIG5vdCByZXBvcnRlZCB2aWEgYG9uRXJyb3JgLlxuICAgKi9cbiAgcnVuPFQ+KGZuOiAoLi4uYXJnczogYW55W10pID0+IFQsIGFwcGx5VGhpcz86IGFueSwgYXBwbHlBcmdzPzogYW55W10pOiBUIHtcbiAgICByZXR1cm4gKHRoaXMgYXMgYW55IGFzIE5nWm9uZVByaXZhdGUpLl9pbm5lci5ydW4oZm4sIGFwcGx5VGhpcywgYXBwbHlBcmdzKSBhcyBUO1xuICB9XG5cbiAgLyoqXG4gICAqIEV4ZWN1dGVzIHRoZSBgZm5gIGZ1bmN0aW9uIHN5bmNocm9ub3VzbHkgd2l0aGluIHRoZSBBbmd1bGFyIHpvbmUgYXMgYSB0YXNrIGFuZCByZXR1cm5zIHZhbHVlXG4gICAqIHJldHVybmVkIGJ5IHRoZSBmdW5jdGlvbi5cbiAgICpcbiAgICogUnVubmluZyBmdW5jdGlvbnMgdmlhIGBydW5gIGFsbG93cyB5b3UgdG8gcmVlbnRlciBBbmd1bGFyIHpvbmUgZnJvbSBhIHRhc2sgdGhhdCB3YXMgZXhlY3V0ZWRcbiAgICogb3V0c2lkZSBvZiB0aGUgQW5ndWxhciB6b25lICh0eXBpY2FsbHkgc3RhcnRlZCB2aWEge0BsaW5rICNydW5PdXRzaWRlQW5ndWxhcn0pLlxuICAgKlxuICAgKiBBbnkgZnV0dXJlIHRhc2tzIG9yIG1pY3JvdGFza3Mgc2NoZWR1bGVkIGZyb20gd2l0aGluIHRoaXMgZnVuY3Rpb24gd2lsbCBjb250aW51ZSBleGVjdXRpbmcgZnJvbVxuICAgKiB3aXRoaW4gdGhlIEFuZ3VsYXIgem9uZS5cbiAgICpcbiAgICogSWYgYSBzeW5jaHJvbm91cyBlcnJvciBoYXBwZW5zIGl0IHdpbGwgYmUgcmV0aHJvd24gYW5kIG5vdCByZXBvcnRlZCB2aWEgYG9uRXJyb3JgLlxuICAgKi9cbiAgcnVuVGFzazxUPihmbjogKC4uLmFyZ3M6IGFueVtdKSA9PiBULCBhcHBseVRoaXM/OiBhbnksIGFwcGx5QXJncz86IGFueVtdLCBuYW1lPzogc3RyaW5nKTogVCB7XG4gICAgY29uc3Qgem9uZSA9ICh0aGlzIGFzIGFueSBhcyBOZ1pvbmVQcml2YXRlKS5faW5uZXI7XG4gICAgY29uc3QgdGFzayA9IHpvbmUuc2NoZWR1bGVFdmVudFRhc2soJ05nWm9uZUV2ZW50OiAnICsgbmFtZSwgZm4sIEVNUFRZX1BBWUxPQUQsIG5vb3AsIG5vb3ApO1xuICAgIHRyeSB7XG4gICAgICByZXR1cm4gem9uZS5ydW5UYXNrKHRhc2ssIGFwcGx5VGhpcywgYXBwbHlBcmdzKSBhcyBUO1xuICAgIH0gZmluYWxseSB7XG4gICAgICB6b25lLmNhbmNlbFRhc2sodGFzayk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFNhbWUgYXMgYHJ1bmAsIGV4Y2VwdCB0aGF0IHN5bmNocm9ub3VzIGVycm9ycyBhcmUgY2F1Z2h0IGFuZCBmb3J3YXJkZWQgdmlhIGBvbkVycm9yYCBhbmQgbm90XG4gICAqIHJldGhyb3duLlxuICAgKi9cbiAgcnVuR3VhcmRlZDxUPihmbjogKC4uLmFyZ3M6IGFueVtdKSA9PiBULCBhcHBseVRoaXM/OiBhbnksIGFwcGx5QXJncz86IGFueVtdKTogVCB7XG4gICAgcmV0dXJuICh0aGlzIGFzIGFueSBhcyBOZ1pvbmVQcml2YXRlKS5faW5uZXIucnVuR3VhcmRlZChmbiwgYXBwbHlUaGlzLCBhcHBseUFyZ3MpIGFzIFQ7XG4gIH1cblxuICAvKipcbiAgICogRXhlY3V0ZXMgdGhlIGBmbmAgZnVuY3Rpb24gc3luY2hyb25vdXNseSBpbiBBbmd1bGFyJ3MgcGFyZW50IHpvbmUgYW5kIHJldHVybnMgdmFsdWUgcmV0dXJuZWQgYnlcbiAgICogdGhlIGZ1bmN0aW9uLlxuICAgKlxuICAgKiBSdW5uaW5nIGZ1bmN0aW9ucyB2aWEge0BsaW5rICNydW5PdXRzaWRlQW5ndWxhcn0gYWxsb3dzIHlvdSB0byBlc2NhcGUgQW5ndWxhcidzIHpvbmUgYW5kIGRvXG4gICAqIHdvcmsgdGhhdFxuICAgKiBkb2Vzbid0IHRyaWdnZXIgQW5ndWxhciBjaGFuZ2UtZGV0ZWN0aW9uIG9yIGlzIHN1YmplY3QgdG8gQW5ndWxhcidzIGVycm9yIGhhbmRsaW5nLlxuICAgKlxuICAgKiBBbnkgZnV0dXJlIHRhc2tzIG9yIG1pY3JvdGFza3Mgc2NoZWR1bGVkIGZyb20gd2l0aGluIHRoaXMgZnVuY3Rpb24gd2lsbCBjb250aW51ZSBleGVjdXRpbmcgZnJvbVxuICAgKiBvdXRzaWRlIG9mIHRoZSBBbmd1bGFyIHpvbmUuXG4gICAqXG4gICAqIFVzZSB7QGxpbmsgI3J1bn0gdG8gcmVlbnRlciB0aGUgQW5ndWxhciB6b25lIGFuZCBkbyB3b3JrIHRoYXQgdXBkYXRlcyB0aGUgYXBwbGljYXRpb24gbW9kZWwuXG4gICAqL1xuICBydW5PdXRzaWRlQW5ndWxhcjxUPihmbjogKC4uLmFyZ3M6IGFueVtdKSA9PiBUKTogVCB7XG4gICAgcmV0dXJuICh0aGlzIGFzIGFueSBhcyBOZ1pvbmVQcml2YXRlKS5fb3V0ZXIucnVuKGZuKSBhcyBUO1xuICB9XG59XG5cbmZ1bmN0aW9uIG5vb3AoKSB7fVxuY29uc3QgRU1QVFlfUEFZTE9BRCA9IHt9O1xuXG5cbmludGVyZmFjZSBOZ1pvbmVQcml2YXRlIGV4dGVuZHMgTmdab25lIHtcbiAgX291dGVyOiBab25lO1xuICBfaW5uZXI6IFpvbmU7XG4gIF9uZXN0aW5nOiBudW1iZXI7XG5cbiAgaGFzUGVuZGluZ01pY3JvdGFza3M6IGJvb2xlYW47XG4gIGhhc1BlbmRpbmdNYWNyb3Rhc2tzOiBib29sZWFuO1xuICBpc1N0YWJsZTogYm9vbGVhbjtcbn1cblxuZnVuY3Rpb24gY2hlY2tTdGFibGUoem9uZTogTmdab25lUHJpdmF0ZSkge1xuICBpZiAoem9uZS5fbmVzdGluZyA9PSAwICYmICF6b25lLmhhc1BlbmRpbmdNaWNyb3Rhc2tzICYmICF6b25lLmlzU3RhYmxlKSB7XG4gICAgdHJ5IHtcbiAgICAgIHpvbmUuX25lc3RpbmcrKztcbiAgICAgIHpvbmUub25NaWNyb3Rhc2tFbXB0eS5lbWl0KG51bGwpO1xuICAgIH0gZmluYWxseSB7XG4gICAgICB6b25lLl9uZXN0aW5nLS07XG4gICAgICBpZiAoIXpvbmUuaGFzUGVuZGluZ01pY3JvdGFza3MpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICB6b25lLnJ1bk91dHNpZGVBbmd1bGFyKCgpID0+IHpvbmUub25TdGFibGUuZW1pdChudWxsKSk7XG4gICAgICAgIH0gZmluYWxseSB7XG4gICAgICAgICAgem9uZS5pc1N0YWJsZSA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gZm9ya0lubmVyWm9uZVdpdGhBbmd1bGFyQmVoYXZpb3Ioem9uZTogTmdab25lUHJpdmF0ZSkge1xuICB6b25lLl9pbm5lciA9IHpvbmUuX2lubmVyLmZvcmsoe1xuICAgIG5hbWU6ICdhbmd1bGFyJyxcbiAgICBwcm9wZXJ0aWVzOiA8YW55PnsnaXNBbmd1bGFyWm9uZSc6IHRydWV9LFxuICAgIG9uSW52b2tlVGFzazogKGRlbGVnYXRlOiBab25lRGVsZWdhdGUsIGN1cnJlbnQ6IFpvbmUsIHRhcmdldDogWm9uZSwgdGFzazogVGFzaywgYXBwbHlUaGlzOiBhbnksXG4gICAgICAgICAgICAgICAgICAgYXBwbHlBcmdzOiBhbnkpOiBhbnkgPT4ge1xuICAgICAgdHJ5IHtcbiAgICAgICAgb25FbnRlcih6b25lKTtcbiAgICAgICAgcmV0dXJuIGRlbGVnYXRlLmludm9rZVRhc2sodGFyZ2V0LCB0YXNrLCBhcHBseVRoaXMsIGFwcGx5QXJncyk7XG4gICAgICB9IGZpbmFsbHkge1xuICAgICAgICBvbkxlYXZlKHpvbmUpO1xuICAgICAgfVxuICAgIH0sXG5cblxuICAgIG9uSW52b2tlOiAoZGVsZWdhdGU6IFpvbmVEZWxlZ2F0ZSwgY3VycmVudDogWm9uZSwgdGFyZ2V0OiBab25lLCBjYWxsYmFjazogRnVuY3Rpb24sXG4gICAgICAgICAgICAgICBhcHBseVRoaXM6IGFueSwgYXBwbHlBcmdzPzogYW55W10sIHNvdXJjZT86IHN0cmluZyk6IGFueSA9PiB7XG4gICAgICB0cnkge1xuICAgICAgICBvbkVudGVyKHpvbmUpO1xuICAgICAgICByZXR1cm4gZGVsZWdhdGUuaW52b2tlKHRhcmdldCwgY2FsbGJhY2ssIGFwcGx5VGhpcywgYXBwbHlBcmdzLCBzb3VyY2UpO1xuICAgICAgfSBmaW5hbGx5IHtcbiAgICAgICAgb25MZWF2ZSh6b25lKTtcbiAgICAgIH1cbiAgICB9LFxuXG4gICAgb25IYXNUYXNrOlxuICAgICAgICAoZGVsZWdhdGU6IFpvbmVEZWxlZ2F0ZSwgY3VycmVudDogWm9uZSwgdGFyZ2V0OiBab25lLCBoYXNUYXNrU3RhdGU6IEhhc1Rhc2tTdGF0ZSkgPT4ge1xuICAgICAgICAgIGRlbGVnYXRlLmhhc1Rhc2sodGFyZ2V0LCBoYXNUYXNrU3RhdGUpO1xuICAgICAgICAgIGlmIChjdXJyZW50ID09PSB0YXJnZXQpIHtcbiAgICAgICAgICAgIC8vIFdlIGFyZSBvbmx5IGludGVyZXN0ZWQgaW4gaGFzVGFzayBldmVudHMgd2hpY2ggb3JpZ2luYXRlIGZyb20gb3VyIHpvbmVcbiAgICAgICAgICAgIC8vIChBIGNoaWxkIGhhc1Rhc2sgZXZlbnQgaXMgbm90IGludGVyZXN0aW5nIHRvIHVzKVxuICAgICAgICAgICAgaWYgKGhhc1Rhc2tTdGF0ZS5jaGFuZ2UgPT0gJ21pY3JvVGFzaycpIHtcbiAgICAgICAgICAgICAgem9uZS5oYXNQZW5kaW5nTWljcm90YXNrcyA9IGhhc1Rhc2tTdGF0ZS5taWNyb1Rhc2s7XG4gICAgICAgICAgICAgIGNoZWNrU3RhYmxlKHpvbmUpO1xuICAgICAgICAgICAgfSBlbHNlIGlmIChoYXNUYXNrU3RhdGUuY2hhbmdlID09ICdtYWNyb1Rhc2snKSB7XG4gICAgICAgICAgICAgIHpvbmUuaGFzUGVuZGluZ01hY3JvdGFza3MgPSBoYXNUYXNrU3RhdGUubWFjcm9UYXNrO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfSxcblxuICAgIG9uSGFuZGxlRXJyb3I6IChkZWxlZ2F0ZTogWm9uZURlbGVnYXRlLCBjdXJyZW50OiBab25lLCB0YXJnZXQ6IFpvbmUsIGVycm9yOiBhbnkpOiBib29sZWFuID0+IHtcbiAgICAgIGRlbGVnYXRlLmhhbmRsZUVycm9yKHRhcmdldCwgZXJyb3IpO1xuICAgICAgem9uZS5ydW5PdXRzaWRlQW5ndWxhcigoKSA9PiB6b25lLm9uRXJyb3IuZW1pdChlcnJvcikpO1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgfSk7XG59XG5cbmZ1bmN0aW9uIG9uRW50ZXIoem9uZTogTmdab25lUHJpdmF0ZSkge1xuICB6b25lLl9uZXN0aW5nKys7XG4gIGlmICh6b25lLmlzU3RhYmxlKSB7XG4gICAgem9uZS5pc1N0YWJsZSA9IGZhbHNlO1xuICAgIHpvbmUub25VbnN0YWJsZS5lbWl0KG51bGwpO1xuICB9XG59XG5cbmZ1bmN0aW9uIG9uTGVhdmUoem9uZTogTmdab25lUHJpdmF0ZSkge1xuICB6b25lLl9uZXN0aW5nLS07XG4gIGNoZWNrU3RhYmxlKHpvbmUpO1xufVxuXG4vKipcbiAqIFByb3ZpZGVzIGEgbm9vcCBpbXBsZW1lbnRhdGlvbiBvZiBgTmdab25lYCB3aGljaCBkb2VzIG5vdGhpbmcuIFRoaXMgem9uZSByZXF1aXJlcyBleHBsaWNpdCBjYWxsc1xuICogdG8gZnJhbWV3b3JrIHRvIHBlcmZvcm0gcmVuZGVyaW5nLlxuICovXG5leHBvcnQgY2xhc3MgTm9vcE5nWm9uZSBpbXBsZW1lbnRzIE5nWm9uZSB7XG4gIHJlYWRvbmx5IGhhc1BlbmRpbmdNaWNyb3Rhc2tzOiBib29sZWFuID0gZmFsc2U7XG4gIHJlYWRvbmx5IGhhc1BlbmRpbmdNYWNyb3Rhc2tzOiBib29sZWFuID0gZmFsc2U7XG4gIHJlYWRvbmx5IGlzU3RhYmxlOiBib29sZWFuID0gdHJ1ZTtcbiAgcmVhZG9ubHkgb25VbnN0YWJsZTogRXZlbnRFbWl0dGVyPGFueT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIHJlYWRvbmx5IG9uTWljcm90YXNrRW1wdHk6IEV2ZW50RW1pdHRlcjxhbnk+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuICByZWFkb25seSBvblN0YWJsZTogRXZlbnRFbWl0dGVyPGFueT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIHJlYWRvbmx5IG9uRXJyb3I6IEV2ZW50RW1pdHRlcjxhbnk+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIHJ1bihmbjogKCkgPT4gYW55KTogYW55IHsgcmV0dXJuIGZuKCk7IH1cblxuICBydW5HdWFyZGVkKGZuOiAoKSA9PiBhbnkpOiBhbnkgeyByZXR1cm4gZm4oKTsgfVxuXG4gIHJ1bk91dHNpZGVBbmd1bGFyKGZuOiAoKSA9PiBhbnkpOiBhbnkgeyByZXR1cm4gZm4oKTsgfVxuXG4gIHJ1blRhc2s8VD4oZm46ICgpID0+IGFueSk6IGFueSB7IHJldHVybiBmbigpOyB9XG59XG4iXX0=