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
import { ApplicationRef } from '@angular/core';
import { getDOM } from '../../dom/dom_adapter';
import { window } from './browser';
export class ChangeDetectionPerfRecord {
    /**
     * @param {?} msPerTick
     * @param {?} numTicks
     */
    constructor(msPerTick, numTicks) {
        this.msPerTick = msPerTick;
        this.numTicks = numTicks;
    }
}
if (false) {
    /** @type {?} */
    ChangeDetectionPerfRecord.prototype.msPerTick;
    /** @type {?} */
    ChangeDetectionPerfRecord.prototype.numTicks;
}
/**
 * Entry point for all Angular profiling-related debug tools. This object
 * corresponds to the `ng.profiler` in the dev console.
 */
export class AngularProfiler {
    /**
     * @param {?} ref
     */
    constructor(ref) { this.appRef = ref.injector.get(ApplicationRef); }
    // tslint:disable:no-console
    /**
     * Exercises change detection in a loop and then prints the average amount of
     * time in milliseconds how long a single round of change detection takes for
     * the current state of the UI. It runs a minimum of 5 rounds for a minimum
     * of 500 milliseconds.
     *
     * Optionally, a user may pass a `config` parameter containing a map of
     * options. Supported options are:
     *
     * `record` (boolean) - causes the profiler to record a CPU profile while
     * it exercises the change detector. Example:
     *
     * ```
     * ng.profiler.timeChangeDetection({record: true})
     * ```
     * @param {?} config
     * @return {?}
     */
    timeChangeDetection(config) {
        /** @type {?} */
        const record = config && config['record'];
        /** @type {?} */
        const profileName = 'Change Detection';
        // Profiler is not available in Android browsers, nor in IE 9 without dev tools opened
        /** @type {?} */
        const isProfilerAvailable = window.console.profile != null;
        if (record && isProfilerAvailable) {
            window.console.profile(profileName);
        }
        /** @type {?} */
        const start = getDOM().performanceNow();
        /** @type {?} */
        let numTicks = 0;
        while (numTicks < 5 || (getDOM().performanceNow() - start) < 500) {
            this.appRef.tick();
            numTicks++;
        }
        /** @type {?} */
        const end = getDOM().performanceNow();
        if (record && isProfilerAvailable) {
            window.console.profileEnd(profileName);
        }
        /** @type {?} */
        const msPerTick = (end - start) / numTicks;
        window.console.log(`ran ${numTicks} change detection cycles`);
        window.console.log(`${msPerTick.toFixed(2)} ms per check`);
        return new ChangeDetectionPerfRecord(msPerTick, numTicks);
    }
}
if (false) {
    /** @type {?} */
    AngularProfiler.prototype.appRef;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tbW9uX3Rvb2xzLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvcGxhdGZvcm0tYnJvd3Nlci9zcmMvYnJvd3Nlci90b29scy9jb21tb25fdG9vbHMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsY0FBYyxFQUFlLE1BQU0sZUFBZSxDQUFDO0FBQzNELE9BQU8sRUFBQyxNQUFNLEVBQUMsTUFBTSx1QkFBdUIsQ0FBQztBQUM3QyxPQUFPLEVBQUMsTUFBTSxFQUFDLE1BQU0sV0FBVyxDQUFDO0FBRWpDLE1BQU0sT0FBTyx5QkFBeUI7Ozs7O0lBQ3BDLFlBQW1CLFNBQWlCLEVBQVMsUUFBZ0I7UUFBMUMsY0FBUyxHQUFULFNBQVMsQ0FBUTtRQUFTLGFBQVEsR0FBUixRQUFRLENBQVE7SUFBRyxDQUFDO0NBQ2xFOzs7SUFEYSw4Q0FBd0I7O0lBQUUsNkNBQXVCOzs7Ozs7QUFPL0QsTUFBTSxPQUFPLGVBQWU7Ozs7SUFHMUIsWUFBWSxHQUFzQixJQUFJLElBQUksQ0FBQyxNQUFNLEdBQUcsR0FBRyxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsY0FBYyxDQUFDLENBQUMsQ0FBQyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7OztJQW1CdkYsbUJBQW1CLENBQUMsTUFBVzs7Y0FDdkIsTUFBTSxHQUFHLE1BQU0sSUFBSSxNQUFNLENBQUMsUUFBUSxDQUFDOztjQUNuQyxXQUFXLEdBQUcsa0JBQWtCOzs7Y0FFaEMsbUJBQW1CLEdBQUcsTUFBTSxDQUFDLE9BQU8sQ0FBQyxPQUFPLElBQUksSUFBSTtRQUMxRCxJQUFJLE1BQU0sSUFBSSxtQkFBbUIsRUFBRTtZQUNqQyxNQUFNLENBQUMsT0FBTyxDQUFDLE9BQU8sQ0FBQyxXQUFXLENBQUMsQ0FBQztTQUNyQzs7Y0FDSyxLQUFLLEdBQUcsTUFBTSxFQUFFLENBQUMsY0FBYyxFQUFFOztZQUNuQyxRQUFRLEdBQUcsQ0FBQztRQUNoQixPQUFPLFFBQVEsR0FBRyxDQUFDLElBQUksQ0FBQyxNQUFNLEVBQUUsQ0FBQyxjQUFjLEVBQUUsR0FBRyxLQUFLLENBQUMsR0FBRyxHQUFHLEVBQUU7WUFDaEUsSUFBSSxDQUFDLE1BQU0sQ0FBQyxJQUFJLEVBQUUsQ0FBQztZQUNuQixRQUFRLEVBQUUsQ0FBQztTQUNaOztjQUNLLEdBQUcsR0FBRyxNQUFNLEVBQUUsQ0FBQyxjQUFjLEVBQUU7UUFDckMsSUFBSSxNQUFNLElBQUksbUJBQW1CLEVBQUU7WUFDakMsTUFBTSxDQUFDLE9BQU8sQ0FBQyxVQUFVLENBQUMsV0FBVyxDQUFDLENBQUM7U0FDeEM7O2NBQ0ssU0FBUyxHQUFHLENBQUMsR0FBRyxHQUFHLEtBQUssQ0FBQyxHQUFHLFFBQVE7UUFDMUMsTUFBTSxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsT0FBTyxRQUFRLDBCQUEwQixDQUFDLENBQUM7UUFDOUQsTUFBTSxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsR0FBRyxTQUFTLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQyxlQUFlLENBQUMsQ0FBQztRQUUzRCxPQUFPLElBQUkseUJBQXlCLENBQUMsU0FBUyxFQUFFLFFBQVEsQ0FBQyxDQUFDO0lBQzVELENBQUM7Q0FDRjs7O0lBN0NDLGlDQUF1QiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtBcHBsaWNhdGlvblJlZiwgQ29tcG9uZW50UmVmfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7Z2V0RE9NfSBmcm9tICcuLi8uLi9kb20vZG9tX2FkYXB0ZXInO1xuaW1wb3J0IHt3aW5kb3d9IGZyb20gJy4vYnJvd3Nlcic7XG5cbmV4cG9ydCBjbGFzcyBDaGFuZ2VEZXRlY3Rpb25QZXJmUmVjb3JkIHtcbiAgY29uc3RydWN0b3IocHVibGljIG1zUGVyVGljazogbnVtYmVyLCBwdWJsaWMgbnVtVGlja3M6IG51bWJlcikge31cbn1cblxuLyoqXG4gKiBFbnRyeSBwb2ludCBmb3IgYWxsIEFuZ3VsYXIgcHJvZmlsaW5nLXJlbGF0ZWQgZGVidWcgdG9vbHMuIFRoaXMgb2JqZWN0XG4gKiBjb3JyZXNwb25kcyB0byB0aGUgYG5nLnByb2ZpbGVyYCBpbiB0aGUgZGV2IGNvbnNvbGUuXG4gKi9cbmV4cG9ydCBjbGFzcyBBbmd1bGFyUHJvZmlsZXIge1xuICBhcHBSZWY6IEFwcGxpY2F0aW9uUmVmO1xuXG4gIGNvbnN0cnVjdG9yKHJlZjogQ29tcG9uZW50UmVmPGFueT4pIHsgdGhpcy5hcHBSZWYgPSByZWYuaW5qZWN0b3IuZ2V0KEFwcGxpY2F0aW9uUmVmKTsgfVxuXG4gIC8vIHRzbGludDpkaXNhYmxlOm5vLWNvbnNvbGVcbiAgLyoqXG4gICAqIEV4ZXJjaXNlcyBjaGFuZ2UgZGV0ZWN0aW9uIGluIGEgbG9vcCBhbmQgdGhlbiBwcmludHMgdGhlIGF2ZXJhZ2UgYW1vdW50IG9mXG4gICAqIHRpbWUgaW4gbWlsbGlzZWNvbmRzIGhvdyBsb25nIGEgc2luZ2xlIHJvdW5kIG9mIGNoYW5nZSBkZXRlY3Rpb24gdGFrZXMgZm9yXG4gICAqIHRoZSBjdXJyZW50IHN0YXRlIG9mIHRoZSBVSS4gSXQgcnVucyBhIG1pbmltdW0gb2YgNSByb3VuZHMgZm9yIGEgbWluaW11bVxuICAgKiBvZiA1MDAgbWlsbGlzZWNvbmRzLlxuICAgKlxuICAgKiBPcHRpb25hbGx5LCBhIHVzZXIgbWF5IHBhc3MgYSBgY29uZmlnYCBwYXJhbWV0ZXIgY29udGFpbmluZyBhIG1hcCBvZlxuICAgKiBvcHRpb25zLiBTdXBwb3J0ZWQgb3B0aW9ucyBhcmU6XG4gICAqXG4gICAqIGByZWNvcmRgIChib29sZWFuKSAtIGNhdXNlcyB0aGUgcHJvZmlsZXIgdG8gcmVjb3JkIGEgQ1BVIHByb2ZpbGUgd2hpbGVcbiAgICogaXQgZXhlcmNpc2VzIHRoZSBjaGFuZ2UgZGV0ZWN0b3IuIEV4YW1wbGU6XG4gICAqXG4gICAqIGBgYFxuICAgKiBuZy5wcm9maWxlci50aW1lQ2hhbmdlRGV0ZWN0aW9uKHtyZWNvcmQ6IHRydWV9KVxuICAgKiBgYGBcbiAgICovXG4gIHRpbWVDaGFuZ2VEZXRlY3Rpb24oY29uZmlnOiBhbnkpOiBDaGFuZ2VEZXRlY3Rpb25QZXJmUmVjb3JkIHtcbiAgICBjb25zdCByZWNvcmQgPSBjb25maWcgJiYgY29uZmlnWydyZWNvcmQnXTtcbiAgICBjb25zdCBwcm9maWxlTmFtZSA9ICdDaGFuZ2UgRGV0ZWN0aW9uJztcbiAgICAvLyBQcm9maWxlciBpcyBub3QgYXZhaWxhYmxlIGluIEFuZHJvaWQgYnJvd3NlcnMsIG5vciBpbiBJRSA5IHdpdGhvdXQgZGV2IHRvb2xzIG9wZW5lZFxuICAgIGNvbnN0IGlzUHJvZmlsZXJBdmFpbGFibGUgPSB3aW5kb3cuY29uc29sZS5wcm9maWxlICE9IG51bGw7XG4gICAgaWYgKHJlY29yZCAmJiBpc1Byb2ZpbGVyQXZhaWxhYmxlKSB7XG4gICAgICB3aW5kb3cuY29uc29sZS5wcm9maWxlKHByb2ZpbGVOYW1lKTtcbiAgICB9XG4gICAgY29uc3Qgc3RhcnQgPSBnZXRET00oKS5wZXJmb3JtYW5jZU5vdygpO1xuICAgIGxldCBudW1UaWNrcyA9IDA7XG4gICAgd2hpbGUgKG51bVRpY2tzIDwgNSB8fCAoZ2V0RE9NKCkucGVyZm9ybWFuY2VOb3coKSAtIHN0YXJ0KSA8IDUwMCkge1xuICAgICAgdGhpcy5hcHBSZWYudGljaygpO1xuICAgICAgbnVtVGlja3MrKztcbiAgICB9XG4gICAgY29uc3QgZW5kID0gZ2V0RE9NKCkucGVyZm9ybWFuY2VOb3coKTtcbiAgICBpZiAocmVjb3JkICYmIGlzUHJvZmlsZXJBdmFpbGFibGUpIHtcbiAgICAgIHdpbmRvdy5jb25zb2xlLnByb2ZpbGVFbmQocHJvZmlsZU5hbWUpO1xuICAgIH1cbiAgICBjb25zdCBtc1BlclRpY2sgPSAoZW5kIC0gc3RhcnQpIC8gbnVtVGlja3M7XG4gICAgd2luZG93LmNvbnNvbGUubG9nKGByYW4gJHtudW1UaWNrc30gY2hhbmdlIGRldGVjdGlvbiBjeWNsZXNgKTtcbiAgICB3aW5kb3cuY29uc29sZS5sb2coYCR7bXNQZXJUaWNrLnRvRml4ZWQoMil9IG1zIHBlciBjaGVja2ApO1xuXG4gICAgcmV0dXJuIG5ldyBDaGFuZ2VEZXRlY3Rpb25QZXJmUmVjb3JkKG1zUGVyVGljaywgbnVtVGlja3MpO1xuICB9XG59XG4iXX0=