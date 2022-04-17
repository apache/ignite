/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Injectable } from '@angular/core';
var Log = /** @class */ (function () {
    function Log() {
        this.logItems = [];
    }
    Log.prototype.add = function (value /** TODO #9100 */) { this.logItems.push(value); };
    Log.prototype.fn = function (value /** TODO #9100 */) {
        var _this = this;
        return function (a1, a2, a3, a4, a5) {
            if (a1 === void 0) { a1 = null; }
            if (a2 === void 0) { a2 = null; }
            if (a3 === void 0) { a3 = null; }
            if (a4 === void 0) { a4 = null; }
            if (a5 === void 0) { a5 = null; }
            _this.logItems.push(value);
        };
    };
    Log.prototype.clear = function () { this.logItems = []; };
    Log.prototype.result = function () { return this.logItems.join('; '); };
    Log = tslib_1.__decorate([
        Injectable(),
        tslib_1.__metadata("design:paramtypes", [])
    ], Log);
    return Log;
}());
export { Log };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9nZ2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS90ZXN0aW5nL3NyYy9sb2dnZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQyxVQUFVLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFHekM7SUFHRTtRQUFnQixJQUFJLENBQUMsUUFBUSxHQUFHLEVBQUUsQ0FBQztJQUFDLENBQUM7SUFFckMsaUJBQUcsR0FBSCxVQUFJLEtBQVUsQ0FBQyxpQkFBaUIsSUFBVSxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFFdEUsZ0JBQUUsR0FBRixVQUFHLEtBQVUsQ0FBQyxpQkFBaUI7UUFBL0IsaUJBSUM7UUFIQyxPQUFPLFVBQUMsRUFBYyxFQUFFLEVBQWMsRUFBRSxFQUFjLEVBQUUsRUFBYyxFQUFFLEVBQWM7WUFBOUUsbUJBQUEsRUFBQSxTQUFjO1lBQUUsbUJBQUEsRUFBQSxTQUFjO1lBQUUsbUJBQUEsRUFBQSxTQUFjO1lBQUUsbUJBQUEsRUFBQSxTQUFjO1lBQUUsbUJBQUEsRUFBQSxTQUFjO1lBQ3BGLEtBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDO1FBQzVCLENBQUMsQ0FBQztJQUNKLENBQUM7SUFFRCxtQkFBSyxHQUFMLGNBQWdCLElBQUksQ0FBQyxRQUFRLEdBQUcsRUFBRSxDQUFDLENBQUMsQ0FBQztJQUVyQyxvQkFBTSxHQUFOLGNBQW1CLE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBZjFDLEdBQUc7UUFEZixVQUFVLEVBQUU7O09BQ0EsR0FBRyxDQWdCZjtJQUFELFVBQUM7Q0FBQSxBQWhCRCxJQWdCQztTQWhCWSxHQUFHIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdGFibGV9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgTG9nIHtcbiAgbG9nSXRlbXM6IGFueVtdO1xuXG4gIGNvbnN0cnVjdG9yKCkgeyB0aGlzLmxvZ0l0ZW1zID0gW107IH1cblxuICBhZGQodmFsdWU6IGFueSAvKiogVE9ETyAjOTEwMCAqLyk6IHZvaWQgeyB0aGlzLmxvZ0l0ZW1zLnB1c2godmFsdWUpOyB9XG5cbiAgZm4odmFsdWU6IGFueSAvKiogVE9ETyAjOTEwMCAqLykge1xuICAgIHJldHVybiAoYTE6IGFueSA9IG51bGwsIGEyOiBhbnkgPSBudWxsLCBhMzogYW55ID0gbnVsbCwgYTQ6IGFueSA9IG51bGwsIGE1OiBhbnkgPSBudWxsKSA9PiB7XG4gICAgICB0aGlzLmxvZ0l0ZW1zLnB1c2godmFsdWUpO1xuICAgIH07XG4gIH1cblxuICBjbGVhcigpOiB2b2lkIHsgdGhpcy5sb2dJdGVtcyA9IFtdOyB9XG5cbiAgcmVzdWx0KCk6IHN0cmluZyB7IHJldHVybiB0aGlzLmxvZ0l0ZW1zLmpvaW4oJzsgJyk7IH1cbn1cbiJdfQ==