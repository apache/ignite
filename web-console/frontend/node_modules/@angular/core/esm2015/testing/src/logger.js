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
import { Injectable } from '@angular/core';
export class Log {
    constructor() { this.logItems = []; }
    /**
     * @param {?} value
     * @return {?}
     */
    add(value /** TODO #9100 */) { this.logItems.push(value); }
    /**
     * @param {?} value
     * @return {?}
     */
    fn(value /** TODO #9100 */) {
        return (/**
         * @param {?=} a1
         * @param {?=} a2
         * @param {?=} a3
         * @param {?=} a4
         * @param {?=} a5
         * @return {?}
         */
        (a1 = null, a2 = null, a3 = null, a4 = null, a5 = null) => {
            this.logItems.push(value);
        });
    }
    /**
     * @return {?}
     */
    clear() { this.logItems = []; }
    /**
     * @return {?}
     */
    result() { return this.logItems.join('; '); }
}
Log.decorators = [
    { type: Injectable }
];
/** @nocollapse */
Log.ctorParameters = () => [];
if (false) {
    /** @type {?} */
    Log.prototype.logItems;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9nZ2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS90ZXN0aW5nL3NyYy9sb2dnZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsVUFBVSxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBR3pDLE1BQU0sT0FBTyxHQUFHO0lBR2QsZ0JBQWdCLElBQUksQ0FBQyxRQUFRLEdBQUcsRUFBRSxDQUFDLENBQUMsQ0FBQzs7Ozs7SUFFckMsR0FBRyxDQUFDLEtBQVUsQ0FBQyxpQkFBaUIsSUFBVSxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUM7Ozs7O0lBRXRFLEVBQUUsQ0FBQyxLQUFVLENBQUMsaUJBQWlCO1FBQzdCOzs7Ozs7OztRQUFPLENBQUMsS0FBVSxJQUFJLEVBQUUsS0FBVSxJQUFJLEVBQUUsS0FBVSxJQUFJLEVBQUUsS0FBVSxJQUFJLEVBQUUsS0FBVSxJQUFJLEVBQUUsRUFBRTtZQUN4RixJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUM1QixDQUFDLEVBQUM7SUFDSixDQUFDOzs7O0lBRUQsS0FBSyxLQUFXLElBQUksQ0FBQyxRQUFRLEdBQUcsRUFBRSxDQUFDLENBQUMsQ0FBQzs7OztJQUVyQyxNQUFNLEtBQWEsT0FBTyxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7OztZQWhCdEQsVUFBVTs7Ozs7O0lBRVQsdUJBQWdCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdGFibGV9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgTG9nIHtcbiAgbG9nSXRlbXM6IGFueVtdO1xuXG4gIGNvbnN0cnVjdG9yKCkgeyB0aGlzLmxvZ0l0ZW1zID0gW107IH1cblxuICBhZGQodmFsdWU6IGFueSAvKiogVE9ETyAjOTEwMCAqLyk6IHZvaWQgeyB0aGlzLmxvZ0l0ZW1zLnB1c2godmFsdWUpOyB9XG5cbiAgZm4odmFsdWU6IGFueSAvKiogVE9ETyAjOTEwMCAqLykge1xuICAgIHJldHVybiAoYTE6IGFueSA9IG51bGwsIGEyOiBhbnkgPSBudWxsLCBhMzogYW55ID0gbnVsbCwgYTQ6IGFueSA9IG51bGwsIGE1OiBhbnkgPSBudWxsKSA9PiB7XG4gICAgICB0aGlzLmxvZ0l0ZW1zLnB1c2godmFsdWUpO1xuICAgIH07XG4gIH1cblxuICBjbGVhcigpOiB2b2lkIHsgdGhpcy5sb2dJdGVtcyA9IFtdOyB9XG5cbiAgcmVzdWx0KCk6IHN0cmluZyB7IHJldHVybiB0aGlzLmxvZ0l0ZW1zLmpvaW4oJzsgJyk7IH1cbn1cbiJdfQ==