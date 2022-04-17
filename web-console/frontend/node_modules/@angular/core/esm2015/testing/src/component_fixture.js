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
import { RendererFactory2, getDebugNode } from '@angular/core';
/**
 * Fixture for debugging and testing a component.
 *
 * \@publicApi
 * @template T
 */
export class ComponentFixture {
    /**
     * @param {?} componentRef
     * @param {?} ngZone
     * @param {?} _autoDetect
     */
    constructor(componentRef, ngZone, _autoDetect) {
        this.componentRef = componentRef;
        this.ngZone = ngZone;
        this._autoDetect = _autoDetect;
        this._isStable = true;
        this._isDestroyed = false;
        this._resolve = null;
        this._promise = null;
        this._onUnstableSubscription = null;
        this._onStableSubscription = null;
        this._onMicrotaskEmptySubscription = null;
        this._onErrorSubscription = null;
        this.changeDetectorRef = componentRef.changeDetectorRef;
        this.elementRef = componentRef.location;
        this.debugElement = (/** @type {?} */ (getDebugNode(this.elementRef.nativeElement)));
        this.componentInstance = componentRef.instance;
        this.nativeElement = this.elementRef.nativeElement;
        this.componentRef = componentRef;
        this.ngZone = ngZone;
        if (ngZone) {
            // Create subscriptions outside the NgZone so that the callbacks run oustide
            // of NgZone.
            ngZone.runOutsideAngular((/**
             * @return {?}
             */
            () => {
                this._onUnstableSubscription =
                    ngZone.onUnstable.subscribe({ next: (/**
                         * @return {?}
                         */
                        () => { this._isStable = false; }) });
                this._onMicrotaskEmptySubscription = ngZone.onMicrotaskEmpty.subscribe({
                    next: (/**
                     * @return {?}
                     */
                    () => {
                        if (this._autoDetect) {
                            // Do a change detection run with checkNoChanges set to true to check
                            // there are no changes on the second run.
                            this.detectChanges(true);
                        }
                    })
                });
                this._onStableSubscription = ngZone.onStable.subscribe({
                    next: (/**
                     * @return {?}
                     */
                    () => {
                        this._isStable = true;
                        // Check whether there is a pending whenStable() completer to resolve.
                        if (this._promise !== null) {
                            // If so check whether there are no pending macrotasks before resolving.
                            // Do this check in the next tick so that ngZone gets a chance to update the state of
                            // pending macrotasks.
                            scheduleMicroTask((/**
                             * @return {?}
                             */
                            () => {
                                if (!ngZone.hasPendingMacrotasks) {
                                    if (this._promise !== null) {
                                        (/** @type {?} */ (this._resolve))(true);
                                        this._resolve = null;
                                        this._promise = null;
                                    }
                                }
                            }));
                        }
                    })
                });
                this._onErrorSubscription =
                    ngZone.onError.subscribe({ next: (/**
                         * @param {?} error
                         * @return {?}
                         */
                        (error) => { throw error; }) });
            }));
        }
    }
    /**
     * @private
     * @param {?} checkNoChanges
     * @return {?}
     */
    _tick(checkNoChanges) {
        this.changeDetectorRef.detectChanges();
        if (checkNoChanges) {
            this.checkNoChanges();
        }
    }
    /**
     * Trigger a change detection cycle for the component.
     * @param {?=} checkNoChanges
     * @return {?}
     */
    detectChanges(checkNoChanges = true) {
        if (this.ngZone != null) {
            // Run the change detection inside the NgZone so that any async tasks as part of the change
            // detection are captured by the zone and can be waited for in isStable.
            this.ngZone.run((/**
             * @return {?}
             */
            () => { this._tick(checkNoChanges); }));
        }
        else {
            // Running without zone. Just do the change detection.
            this._tick(checkNoChanges);
        }
    }
    /**
     * Do a change detection run to make sure there were no changes.
     * @return {?}
     */
    checkNoChanges() { this.changeDetectorRef.checkNoChanges(); }
    /**
     * Set whether the fixture should autodetect changes.
     *
     * Also runs detectChanges once so that any existing change is detected.
     * @param {?=} autoDetect
     * @return {?}
     */
    autoDetectChanges(autoDetect = true) {
        if (this.ngZone == null) {
            throw new Error('Cannot call autoDetectChanges when ComponentFixtureNoNgZone is set');
        }
        this._autoDetect = autoDetect;
        this.detectChanges();
    }
    /**
     * Return whether the fixture is currently stable or has async tasks that have not been completed
     * yet.
     * @return {?}
     */
    isStable() { return this._isStable && !(/** @type {?} */ (this.ngZone)).hasPendingMacrotasks; }
    /**
     * Get a promise that resolves when the fixture is stable.
     *
     * This can be used to resume testing after events have triggered asynchronous activity or
     * asynchronous change detection.
     * @return {?}
     */
    whenStable() {
        if (this.isStable()) {
            return Promise.resolve(false);
        }
        else if (this._promise !== null) {
            return this._promise;
        }
        else {
            this._promise = new Promise((/**
             * @param {?} res
             * @return {?}
             */
            res => { this._resolve = res; }));
            return this._promise;
        }
    }
    /**
     * @private
     * @return {?}
     */
    _getRenderer() {
        if (this._renderer === undefined) {
            this._renderer = this.componentRef.injector.get(RendererFactory2, null);
        }
        return (/** @type {?} */ (this._renderer));
    }
    /**
     * Get a promise that resolves when the ui state is stable following animations.
     * @return {?}
     */
    whenRenderingDone() {
        /** @type {?} */
        const renderer = this._getRenderer();
        if (renderer && renderer.whenRenderingDone) {
            return renderer.whenRenderingDone();
        }
        return this.whenStable();
    }
    /**
     * Trigger component destruction.
     * @return {?}
     */
    destroy() {
        if (!this._isDestroyed) {
            this.componentRef.destroy();
            if (this._onUnstableSubscription != null) {
                this._onUnstableSubscription.unsubscribe();
                this._onUnstableSubscription = null;
            }
            if (this._onStableSubscription != null) {
                this._onStableSubscription.unsubscribe();
                this._onStableSubscription = null;
            }
            if (this._onMicrotaskEmptySubscription != null) {
                this._onMicrotaskEmptySubscription.unsubscribe();
                this._onMicrotaskEmptySubscription = null;
            }
            if (this._onErrorSubscription != null) {
                this._onErrorSubscription.unsubscribe();
                this._onErrorSubscription = null;
            }
            this._isDestroyed = true;
        }
    }
}
if (false) {
    /**
     * The DebugElement associated with the root element of this component.
     * @type {?}
     */
    ComponentFixture.prototype.debugElement;
    /**
     * The instance of the root component class.
     * @type {?}
     */
    ComponentFixture.prototype.componentInstance;
    /**
     * The native element at the root of the component.
     * @type {?}
     */
    ComponentFixture.prototype.nativeElement;
    /**
     * The ElementRef for the element at the root of the component.
     * @type {?}
     */
    ComponentFixture.prototype.elementRef;
    /**
     * The ChangeDetectorRef for the component
     * @type {?}
     */
    ComponentFixture.prototype.changeDetectorRef;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._renderer;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._isStable;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._isDestroyed;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._resolve;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._promise;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._onUnstableSubscription;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._onStableSubscription;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._onMicrotaskEmptySubscription;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._onErrorSubscription;
    /** @type {?} */
    ComponentFixture.prototype.componentRef;
    /** @type {?} */
    ComponentFixture.prototype.ngZone;
    /**
     * @type {?}
     * @private
     */
    ComponentFixture.prototype._autoDetect;
}
/**
 * @param {?} fn
 * @return {?}
 */
function scheduleMicroTask(fn) {
    Zone.current.scheduleMicroTask('scheduleMicrotask', fn);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcG9uZW50X2ZpeHR1cmUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3Rlc3Rpbmcvc3JjL2NvbXBvbmVudF9maXh0dXJlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFvRSxnQkFBZ0IsRUFBRSxZQUFZLEVBQUMsTUFBTSxlQUFlLENBQUM7Ozs7Ozs7QUFRaEksTUFBTSxPQUFPLGdCQUFnQjs7Ozs7O0lBb0MzQixZQUNXLFlBQTZCLEVBQVMsTUFBbUIsRUFDeEQsV0FBb0I7UUFEckIsaUJBQVksR0FBWixZQUFZLENBQWlCO1FBQVMsV0FBTSxHQUFOLE1BQU0sQ0FBYTtRQUN4RCxnQkFBVyxHQUFYLFdBQVcsQ0FBUztRQVh4QixjQUFTLEdBQVksSUFBSSxDQUFDO1FBQzFCLGlCQUFZLEdBQVksS0FBSyxDQUFDO1FBQzlCLGFBQVEsR0FBaUMsSUFBSSxDQUFDO1FBQzlDLGFBQVEsR0FBc0IsSUFBSSxDQUFDO1FBQ25DLDRCQUF1QixHQUEwQixJQUFJLENBQUM7UUFDdEQsMEJBQXFCLEdBQTBCLElBQUksQ0FBQztRQUNwRCxrQ0FBNkIsR0FBMEIsSUFBSSxDQUFDO1FBQzVELHlCQUFvQixHQUEwQixJQUFJLENBQUM7UUFLekQsSUFBSSxDQUFDLGlCQUFpQixHQUFHLFlBQVksQ0FBQyxpQkFBaUIsQ0FBQztRQUN4RCxJQUFJLENBQUMsVUFBVSxHQUFHLFlBQVksQ0FBQyxRQUFRLENBQUM7UUFDeEMsSUFBSSxDQUFDLFlBQVksR0FBRyxtQkFBYyxZQUFZLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUMsRUFBQSxDQUFDO1FBQzlFLElBQUksQ0FBQyxpQkFBaUIsR0FBRyxZQUFZLENBQUMsUUFBUSxDQUFDO1FBQy9DLElBQUksQ0FBQyxhQUFhLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUM7UUFDbkQsSUFBSSxD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b0NBQXNEOzs7OztJQUN0RCxvQ0FBMkM7Ozs7O0lBQzNDLG1EQUE4RDs7Ozs7SUFDOUQsaURBQTREOzs7OztJQUM1RCx5REFBb0U7Ozs7O0lBQ3BFLGdEQUEyRDs7SUFHdkQsd0NBQW9DOztJQUFFLGtDQUEwQjs7Ozs7SUFDaEUsdUNBQTRCOzs7Ozs7QUErSmxDLFNBQVMsaUJBQWlCLENBQUMsRUFBWTtJQUNyQyxJQUFJLENBQUMsT0FBTyxDQUFDLGlCQUFpQixDQUFDLG1CQUFtQixFQUFFLEVBQUUsQ0FBQyxDQUFDO0FBQzFELENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q2hhbmdlRGV0ZWN0b3JSZWYsIENvbXBvbmVudFJlZiwgRGVidWdFbGVtZW50LCBFbGVtZW50UmVmLCBOZ1pvbmUsIFJlbmRlcmVyRmFjdG9yeTIsIGdldERlYnVnTm9kZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cblxuLyoqXG4gKiBGaXh0dXJlIGZvciBkZWJ1Z2dpbmcgYW5kIHRlc3RpbmcgYSBjb21wb25lbnQuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgY2xhc3MgQ29tcG9uZW50Rml4dHVyZTxUPiB7XG4gIC8qKlxuICAgKiBUaGUgRGVidWdFbGVtZW50IGFzc29jaWF0ZWQgd2l0aCB0aGUgcm9vdCBlbGVtZW50IG9mIHRoaXMgY29tcG9uZW50LlxuICAgKi9cbiAgZGVidWdFbGVtZW50OiBEZWJ1Z0VsZW1lbnQ7XG5cbiAgLyoqXG4gICAqIFRoZSBpbnN0YW5jZSBvZiB0aGUgcm9vdCBjb21wb25lbnQgY2xhc3MuXG4gICAqL1xuICBjb21wb25lbnRJbnN0YW5jZTogVDtcblxuICAvKipcbiAgICogVGhlIG5hdGl2ZSBlbGVtZW50IGF0IHRoZSByb290IG9mIHRoZSBjb21wb25lbnQuXG4gICAqL1xuICBuYXRpdmVFbGVtZW50OiBhbnk7XG5cbiAgLyoqXG4gICAqIFRoZSBFbGVtZW50UmVmIGZvciB0aGUgZWxlbWVudCBhdCB0aGUgcm9vdCBvZiB0aGUgY29tcG9uZW50LlxuICAgKi9cbiAgZWxlbWVudFJlZjogRWxlbWVudFJlZjtcblxuICAvKipcbiAgICogVGhlIENoYW5nZURldGVjdG9yUmVmIGZvciB0aGUgY29tcG9uZW50XG4gICAqL1xuICBjaGFuZ2VEZXRlY3RvclJlZjogQ2hhbmdlRGV0ZWN0b3JSZWY7XG5cbiAgcHJpdmF0ZSBfcmVuZGVyZXI6IFJlbmRlcmVyRmFjdG9yeTJ8bnVsbHx1bmRlZmluZWQ7XG4gIHByaXZhdGUgX2lzU3RhYmxlOiBib29sZWFuID0gdHJ1ZTtcbiAgcHJpdmF0ZSBfaXNEZXN0cm95ZWQ6IGJvb2xlYW4gPSBmYWxzZTtcbiAgcHJpdmF0ZSBfcmVzb2x2ZTogKChyZXN1bHQ6IGFueSkgPT4gdm9pZCl8bnVsbCA9IG51bGw7XG4gIHByaXZhdGUgX3Byb21pc2U6IFByb21pc2U8YW55PnxudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBfb25VbnN0YWJsZVN1YnNjcmlwdGlvbjogYW55IC8qKiBUT0RPICM5MTAwICovID0gbnVsbDtcbiAgcHJpdmF0ZSBfb25TdGFibGVTdWJzY3JpcHRpb246IGFueSAvKiogVE9ETyAjOTEwMCAqLyA9IG51bGw7XG4gIHByaXZhdGUgX29uTWljcm90YXNrRW1wdHlTdWJzY3JpcHRpb246IGFueSAvKiogVE9ETyAjOTEwMCAqLyA9IG51bGw7XG4gIHByaXZhdGUgX29uRXJyb3JTdWJzY3JpcHRpb246IGFueSAvKiogVE9ETyAjOTEwMCAqLyA9IG51bGw7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgY29tcG9uZW50UmVmOiBDb21wb25lbnRSZWY8VD4sIHB1YmxpYyBuZ1pvbmU6IE5nWm9uZXxudWxsLFxuICAgICAgcHJpdmF0ZSBfYXV0b0RldGVjdDogYm9vbGVhbikge1xuICAgIHRoaXMuY2hhbmdlRGV0ZWN0b3JSZWYgPSBjb21wb25lbnRSZWYuY2hhbmdlRGV0ZWN0b3JSZWY7XG4gICAgdGhpcy5lbGVtZW50UmVmID0gY29tcG9uZW50UmVmLmxvY2F0aW9uO1xuICAgIHRoaXMuZGVidWdFbGVtZW50ID0gPERlYnVnRWxlbWVudD5nZXREZWJ1Z05vZGUodGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQpO1xuICAgIHRoaXMuY29tcG9uZW50SW5zdGFuY2UgPSBjb21wb25lbnRSZWYuaW5zdGFuY2U7XG4gICAgdGhpcy5uYXRpdmVFbGVtZW50ID0gdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQ7XG4gICAgdGhpcy5jb21wb25lbnRSZWYgPSBjb21wb25lbnRSZWY7XG4gICAgdGhpcy5uZ1pvbmUgPSBuZ1pvbmU7XG5cbiAgICBpZiAobmdab25lKSB7XG4gICAgICAvLyBDcmVhdGUgc3Vic2NyaXB0aW9ucyBvdXRzaWRlIHRoZSBOZ1pvbmUgc28gdGhhdCB0aGUgY2FsbGJhY2tzIHJ1biBvdXN0aWRlXG4gICAgICAvLyBvZiBOZ1pvbmUuXG4gICAgICBuZ1pvbmUucnVuT3V0c2lkZUFuZ3VsYXIoKCkgPT4ge1xuICAgICAgICB0aGlzLl9vblVuc3RhYmxlU3Vic2NyaXB0aW9uID1cbiAgICAgICAgICAgIG5nWm9uZS5vblVuc3RhYmxlLnN1YnNjcmliZSh7bmV4dDogKCkgPT4geyB0aGlzLl9pc1N0YWJsZSA9IGZhbHNlOyB9fSk7XG4gICAgICAgIHRoaXMuX29uTWljcm90YXNrRW1wdHlTdWJzY3JpcHRpb24gPSBuZ1pvbmUub25NaWNyb3Rhc2tFbXB0eS5zdWJzY3JpYmUoe1xuICAgICAgICAgIG5leHQ6ICgpID0+IHtcbiAgICAgICAgICAgIGlmICh0aGlzLl9hdXRvRGV0ZWN0KSB7XG4gICAgICAgICAgICAgIC8vIERvIGEgY2hhbmdlIGRldGVjdGlvbiBydW4gd2l0aCBjaGVja05vQ2hhbmdlcyBzZXQgdG8gdHJ1ZSB0byBjaGVja1xuICAgICAgICAgICAgICAvLyB0aGVyZSBhcmUgbm8gY2hhbmdlcyBvbiB0aGUgc2Vjb25kIHJ1bi5cbiAgICAgICAgICAgICAgdGhpcy5kZXRlY3RDaGFuZ2VzKHRydWUpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIHRoaXMuX29uU3RhYmxlU3Vic2NyaXB0aW9uID0gbmdab25lLm9uU3RhYmxlLnN1YnNjcmliZSh7XG4gICAgICAgICAgbmV4dDogKCkgPT4ge1xuICAgICAgICAgICAgdGhpcy5faXNTdGFibGUgPSB0cnVlO1xuICAgICAgICAgICAgLy8gQ2hlY2sgd2hldGhlciB0aGVyZSBpcyBhIHBlbmRpbmcgd2hlblN0YWJsZSgpIGNvbXBsZXRlciB0byByZXNvbHZlLlxuICAgICAgICAgICAgaWYgKHRoaXMuX3Byb21pc2UgIT09IG51bGwpIHtcbiAgICAgICAgICAgICAgLy8gSWYgc28gY2hlY2sgd2hldGhlciB0aGVyZSBhcmUgbm8gcGVuZGluZyBtYWNyb3Rhc2tzIGJlZm9yZSByZXNvbHZpbmcuXG4gICAgICAgICAgICAgIC8vIERvIHRoaXMgY2hlY2sgaW4gdGhlIG5leHQgdGljayBzbyB0aGF0IG5nWm9uZSBnZXRzIGEgY2hhbmNlIHRvIHVwZGF0ZSB0aGUgc3RhdGUgb2ZcbiAgICAgICAgICAgICAgLy8gcGVuZGluZyBtYWNyb3Rhc2tzLlxuICAgICAgICAgICAgICBzY2hlZHVsZU1pY3JvVGFzaygoKSA9PiB7XG4gICAgICAgICAgICAgICAgaWYgKCFuZ1pvbmUuaGFzUGVuZGluZ01hY3JvdGFza3MpIHtcbiAgICAgICAgICAgICAgICAgIGlmICh0aGlzLl9wcm9taXNlICE9PSBudWxsKSB7XG4gICAgICAgICAgICAgICAgICAgIHRoaXMuX3Jlc29sdmUgISh0cnVlKTtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy5fcmVzb2x2ZSA9IG51bGw7XG4gICAgICAgICAgICAgICAgICAgIHRoaXMuX3Byb21pc2UgPSBudWxsO1xuICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgICAgICB0aGlzLl9vbkVycm9yU3Vic2NyaXB0aW9uID1cbiAgICAgICAgICAgIG5nWm9uZS5vbkVycm9yLnN1YnNjcmliZSh7bmV4dDogKGVycm9yOiBhbnkpID0+IHsgdGhyb3cgZXJyb3I7IH19KTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgX3RpY2soY2hlY2tOb0NoYW5nZXM6IGJvb2xlYW4pIHtcbiAgICB0aGlzLmNoYW5nZURldGVjdG9yUmVmLmRldGVjdENoYW5nZXMoKTtcbiAgICBpZiAoY2hlY2tOb0NoYW5nZXMpIHtcbiAgICAgIHRoaXMuY2hlY2tOb0NoYW5nZXMoKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogVHJpZ2dlciBhIGNoYW5nZSBkZXRlY3Rpb24gY3ljbGUgZm9yIHRoZSBjb21wb25lbnQuXG4gICAqL1xuICBkZXRlY3RDaGFuZ2VzKGNoZWNrTm9DaGFuZ2VzOiBib29sZWFuID0gdHJ1ZSk6IHZvaWQge1xuICAgIGlmICh0aGlzLm5nWm9uZSAhPSBudWxsKSB7XG4gICAgICAvLyBSdW4gdGhlIGNoYW5nZSBkZXRlY3Rpb24gaW5zaWRlIHRoZSBOZ1pvbmUgc28gdGhhdCBhbnkgYXN5bmMgdGFza3MgYXMgcGFydCBvZiB0aGUgY2hhbmdlXG4gICAgICAvLyBkZXRlY3Rpb24gYXJlIGNhcHR1cmVkIGJ5IHRoZSB6b25lIGFuZCBjYW4gYmUgd2FpdGVkIGZvciBpbiBpc1N0YWJsZS5cbiAgICAgIHRoaXMubmdab25lLnJ1bigoKSA9PiB7IHRoaXMuX3RpY2soY2hlY2tOb0NoYW5nZXMpOyB9KTtcbiAgICB9IGVsc2Uge1xuICAgICAgLy8gUnVubmluZyB3aXRob3V0IHpvbmUuIEp1c3QgZG8gdGhlIGNoYW5nZSBkZXRlY3Rpb24uXG4gICAgICB0aGlzLl90aWNrKGNoZWNrTm9DaGFuZ2VzKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogRG8gYSBjaGFuZ2UgZGV0ZWN0aW9uIHJ1biB0byBtYWtlIHN1cmUgdGhlcmUgd2VyZSBubyBjaGFuZ2VzLlxuICAgKi9cbiAgY2hlY2tOb0NoYW5nZXMoKTogdm9pZCB7IHRoaXMuY2hhbmdlRGV0ZWN0b3JSZWYuY2hlY2tOb0NoYW5nZXMoKTsgfVxuXG4gIC8qKlxuICAgKiBTZXQgd2hldGhlciB0aGUgZml4dHVyZSBzaG91bGQgYXV0b2RldGVjdCBjaGFuZ2VzLlxuICAgKlxuICAgKiBBbHNvIHJ1bnMgZGV0ZWN0Q2hhbmdlcyBvbmNlIHNvIHRoYXQgYW55IGV4aXN0aW5nIGNoYW5nZSBpcyBkZXRlY3RlZC5cbiAgICovXG4gIGF1dG9EZXRlY3RDaGFuZ2VzKGF1dG9EZXRlY3Q6IGJvb2xlYW4gPSB0cnVlKSB7XG4gICAgaWYgKHRoaXMubmdab25lID09IG51bGwpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignQ2Fubm90IGNhbGwgYXV0b0RldGVjdENoYW5nZXMgd2hlbiBDb21wb25lbnRGaXh0dXJlTm9OZ1pvbmUgaXMgc2V0Jyk7XG4gICAgfVxuICAgIHRoaXMuX2F1dG9EZXRlY3QgPSBhdXRvRGV0ZWN0O1xuICAgIHRoaXMuZGV0ZWN0Q2hhbmdlcygpO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybiB3aGV0aGVyIHRoZSBmaXh0dXJlIGlzIGN1cnJlbnRseSBzdGFibGUgb3IgaGFzIGFzeW5jIHRhc2tzIHRoYXQgaGF2ZSBub3QgYmVlbiBjb21wbGV0ZWRcbiAgICogeWV0LlxuICAgKi9cbiAgaXNTdGFibGUoKTogYm9vbGVhbiB7IHJldHVybiB0aGlzLl9pc1N0YWJsZSAmJiAhdGhpcy5uZ1pvbmUgIS5oYXNQZW5kaW5nTWFjcm90YXNrczsgfVxuXG4gIC8qKlxuICAgKiBHZXQgYSBwcm9taXNlIHRoYXQgcmVzb2x2ZXMgd2hlbiB0aGUgZml4dHVyZSBpcyBzdGFibGUuXG4gICAqXG4gICAqIFRoaXMgY2FuIGJlIHVzZWQgdG8gcmVzdW1lIHRlc3RpbmcgYWZ0ZXIgZXZlbnRzIGhhdmUgdHJpZ2dlcmVkIGFzeW5jaHJvbm91cyBhY3Rpdml0eSBvclxuICAgKiBhc3luY2hyb25vdXMgY2hhbmdlIGRldGVjdGlvbi5cbiAgICovXG4gIHdoZW5TdGFibGUoKTogUHJvbWlzZTxhbnk+IHtcbiAgICBpZiAodGhpcy5pc1N0YWJsZSgpKSB7XG4gICAgICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKGZhbHNlKTtcbiAgICB9IGVsc2UgaWYgKHRoaXMuX3Byb21pc2UgIT09IG51bGwpIHtcbiAgICAgIHJldHVybiB0aGlzLl9wcm9taXNlO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLl9wcm9taXNlID0gbmV3IFByb21pc2UocmVzID0+IHsgdGhpcy5fcmVzb2x2ZSA9IHJlczsgfSk7XG4gICAgICByZXR1cm4gdGhpcy5fcHJvbWlzZTtcbiAgICB9XG4gIH1cblxuXG4gIHByaXZhdGUgX2dldFJlbmRlcmVyKCkge1xuICAgIGlmICh0aGlzLl9yZW5kZXJlciA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICB0aGlzLl9yZW5kZXJlciA9IHRoaXMuY29tcG9uZW50UmVmLmluamVjdG9yLmdldChSZW5kZXJlckZhY3RvcnkyLCBudWxsKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX3JlbmRlcmVyIGFzIFJlbmRlcmVyRmFjdG9yeTIgfCBudWxsO1xuICB9XG5cbiAgLyoqXG4gICAgKiBHZXQgYSBwcm9taXNlIHRoYXQgcmVzb2x2ZXMgd2hlbiB0aGUgdWkgc3RhdGUgaXMgc3RhYmxlIGZvbGxvd2luZyBhbmltYXRpb25zLlxuICAgICovXG4gIHdoZW5SZW5kZXJpbmdEb25lKCk6IFByb21pc2U8YW55PiB7XG4gICAgY29uc3QgcmVuZGVyZXIgPSB0aGlzLl9nZXRSZW5kZXJlcigpO1xuICAgIGlmIChyZW5kZXJlciAmJiByZW5kZXJlci53aGVuUmVuZGVyaW5nRG9uZSkge1xuICAgICAgcmV0dXJuIHJlbmRlcmVyLndoZW5SZW5kZXJpbmdEb25lKCk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLndoZW5TdGFibGUoKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBUcmlnZ2VyIGNvbXBvbmVudCBkZXN0cnVjdGlvbi5cbiAgICovXG4gIGRlc3Ryb3koKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLl9pc0Rlc3Ryb3llZCkge1xuICAgICAgdGhpcy5jb21wb25lbnRSZWYuZGVzdHJveSgpO1xuICAgICAgaWYgKHRoaXMuX29uVW5zdGFibGVTdWJzY3JpcHRpb24gIT0gbnVsbCkge1xuICAgICAgICB0aGlzLl9vblVuc3RhYmxlU3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7XG4gICAgICAgIHRoaXMuX29uVW5zdGFibGVTdWJzY3JpcHRpb24gPSBudWxsO1xuICAgICAgfVxuICAgICAgaWYgKHRoaXMuX29uU3RhYmxlU3Vic2NyaXB0aW9uICE9IG51bGwpIHtcbiAgICAgICAgdGhpcy5fb25TdGFibGVTdWJzY3JpcHRpb24udW5zdWJzY3JpYmUoKTtcbiAgICAgICAgdGhpcy5fb25TdGFibGVTdWJzY3JpcHRpb24gPSBudWxsO1xuICAgICAgfVxuICAgICAgaWYgKHRoaXMuX29uTWljcm90YXNrRW1wdHlTdWJzY3JpcHRpb24gIT0gbnVsbCkge1xuICAgICAgICB0aGlzLl9vbk1pY3JvdGFza0VtcHR5U3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7XG4gICAgICAgIHRoaXMuX29uTWljcm90YXNrRW1wdHlTdWJzY3JpcHRpb24gPSBudWxsO1xuICAgICAgfVxuICAgICAgaWYgKHRoaXMuX29uRXJyb3JTdWJzY3JpcHRpb24gIT0gbnVsbCkge1xuICAgICAgICB0aGlzLl9vbkVycm9yU3Vic2NyaXB0aW9uLnVuc3Vic2NyaWJlKCk7XG4gICAgICAgIHRoaXMuX29uRXJyb3JTdWJzY3JpcHRpb24gPSBudWxsO1xuICAgICAgfVxuICAgICAgdGhpcy5faXNEZXN0cm95ZWQgPSB0cnVlO1xuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBzY2hlZHVsZU1pY3JvVGFzayhmbjogRnVuY3Rpb24pIHtcbiAgWm9uZS5jdXJyZW50LnNjaGVkdWxlTWljcm9UYXNrKCdzY2hlZHVsZU1pY3JvdGFzaycsIGZuKTtcbn1cbiJdfQ==