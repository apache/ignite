/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { EventEmitter, Injectable, NgZone } from '@angular/core';
/**
 * A mock implementation of {@link NgZone}.
 */
var MockNgZone = /** @class */ (function (_super) {
    tslib_1.__extends(MockNgZone, _super);
    function MockNgZone() {
        var _this = _super.call(this, { enableLongStackTrace: false }) || this;
        _this.onStable = new EventEmitter(false);
        return _this;
    }
    MockNgZone.prototype.run = function (fn) { return fn(); };
    MockNgZone.prototype.runOutsideAngular = function (fn) { return fn(); };
    MockNgZone.prototype.simulateZoneExit = function () { this.onStable.emit(null); };
    MockNgZone = tslib_1.__decorate([
        Injectable(),
        tslib_1.__metadata("design:paramtypes", [])
    ], MockNgZone);
    return MockNgZone;
}(NgZone));
export { MockNgZone };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfem9uZV9tb2NrLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS90ZXN0aW5nL3NyYy9uZ196b25lX21vY2sudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQyxZQUFZLEVBQUUsVUFBVSxFQUFFLE1BQU0sRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUcvRDs7R0FFRztBQUVIO0lBQWdDLHNDQUFNO0lBR3BDO1FBQUEsWUFBZ0Isa0JBQU0sRUFBQyxvQkFBb0IsRUFBRSxLQUFLLEVBQUMsQ0FBQyxTQUFHO1FBRnZELGNBQVEsR0FBc0IsSUFBSSxZQUFZLENBQUMsS0FBSyxDQUFDLENBQUM7O0lBRUEsQ0FBQztJQUV2RCx3QkFBRyxHQUFILFVBQUksRUFBWSxJQUFTLE9BQU8sRUFBRSxFQUFFLENBQUMsQ0FBQyxDQUFDO0lBRXZDLHNDQUFpQixHQUFqQixVQUFrQixFQUFZLElBQVMsT0FBTyxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFFckQscUNBQWdCLEdBQWhCLGNBQTJCLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQztJQVQzQyxVQUFVO1FBRHRCLFVBQVUsRUFBRTs7T0FDQSxVQUFVLENBVXRCO0lBQUQsaUJBQUM7Q0FBQSxBQVZELENBQWdDLE1BQU0sR0FVckM7U0FWWSxVQUFVIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0V2ZW50RW1pdHRlciwgSW5qZWN0YWJsZSwgTmdab25lfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuXG4vKipcbiAqIEEgbW9jayBpbXBsZW1lbnRhdGlvbiBvZiB7QGxpbmsgTmdab25lfS5cbiAqL1xuQEluamVjdGFibGUoKVxuZXhwb3J0IGNsYXNzIE1vY2tOZ1pvbmUgZXh0ZW5kcyBOZ1pvbmUge1xuICBvblN0YWJsZTogRXZlbnRFbWl0dGVyPGFueT4gPSBuZXcgRXZlbnRFbWl0dGVyKGZhbHNlKTtcblxuICBjb25zdHJ1Y3RvcigpIHsgc3VwZXIoe2VuYWJsZUxvbmdTdGFja1RyYWNlOiBmYWxzZX0pOyB9XG5cbiAgcnVuKGZuOiBGdW5jdGlvbik6IGFueSB7IHJldHVybiBmbigpOyB9XG5cbiAgcnVuT3V0c2lkZUFuZ3VsYXIoZm46IEZ1bmN0aW9uKTogYW55IHsgcmV0dXJuIGZuKCk7IH1cblxuICBzaW11bGF0ZVpvbmVFeGl0KCk6IHZvaWQgeyB0aGlzLm9uU3RhYmxlLmVtaXQobnVsbCk7IH1cbn1cbiJdfQ==