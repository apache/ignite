/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { NgModuleResolver } from '@angular/compiler';
var MockNgModuleResolver = /** @class */ (function (_super) {
    tslib_1.__extends(MockNgModuleResolver, _super);
    function MockNgModuleResolver(reflector) {
        var _this = _super.call(this, reflector) || this;
        _this._ngModules = new Map();
        return _this;
    }
    /**
     * Overrides the {@link NgModule} for a module.
     */
    MockNgModuleResolver.prototype.setNgModule = function (type, metadata) {
        this._ngModules.set(type, metadata);
    };
    /**
     * Returns the {@link NgModule} for a module:
     * - Set the {@link NgModule} to the overridden view when it exists or fallback to the
     * default
     * `NgModuleResolver`, see `setNgModule`.
     */
    MockNgModuleResolver.prototype.resolve = function (type, throwIfNotFound) {
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        return this._ngModules.get(type) || _super.prototype.resolve.call(this, type, throwIfNotFound);
    };
    return MockNgModuleResolver;
}(NgModuleResolver));
export { MockNgModuleResolver };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbW9kdWxlX3Jlc29sdmVyX21vY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci90ZXN0aW5nL3NyYy9uZ19tb2R1bGVfcmVzb2x2ZXJfbW9jay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFtQixnQkFBZ0IsRUFBTyxNQUFNLG1CQUFtQixDQUFDO0FBRTNFO0lBQTBDLGdEQUFnQjtJQUd4RCw4QkFBWSxTQUEyQjtRQUF2QyxZQUEyQyxrQkFBTSxTQUFTLENBQUMsU0FBRztRQUZ0RCxnQkFBVSxHQUFHLElBQUksR0FBRyxFQUE0QixDQUFDOztJQUVJLENBQUM7SUFFOUQ7O09BRUc7SUFDSCwwQ0FBVyxHQUFYLFVBQVksSUFBZSxFQUFFLFFBQXVCO1FBQ2xELElBQUksQ0FBQyxVQUFVLENBQUMsR0FBRyxDQUFDLElBQUksRUFBRSxRQUFRLENBQUMsQ0FBQztJQUN0QyxDQUFDO0lBRUQ7Ozs7O09BS0c7SUFDSCxzQ0FBTyxHQUFQLFVBQVEsSUFBZSxFQUFFLGVBQXNCO1FBQXRCLGdDQUFBLEVBQUEsc0JBQXNCO1FBQzdDLE9BQU8sSUFBSSxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLElBQUksaUJBQU0sT0FBTyxZQUFDLElBQUksRUFBRSxlQUFlLENBQUcsQ0FBQztJQUM3RSxDQUFDO0lBQ0gsMkJBQUM7QUFBRCxDQUFDLEFBckJELENBQTBDLGdCQUFnQixHQXFCekQiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcGlsZVJlZmxlY3RvciwgTmdNb2R1bGVSZXNvbHZlciwgY29yZX0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuXG5leHBvcnQgY2xhc3MgTW9ja05nTW9kdWxlUmVzb2x2ZXIgZXh0ZW5kcyBOZ01vZHVsZVJlc29sdmVyIHtcbiAgcHJpdmF0ZSBfbmdNb2R1bGVzID0gbmV3IE1hcDxjb3JlLlR5cGUsIGNvcmUuTmdNb2R1bGU+KCk7XG5cbiAgY29uc3RydWN0b3IocmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7IHN1cGVyKHJlZmxlY3Rvcik7IH1cblxuICAvKipcbiAgICogT3ZlcnJpZGVzIHRoZSB7QGxpbmsgTmdNb2R1bGV9IGZvciBhIG1vZHVsZS5cbiAgICovXG4gIHNldE5nTW9kdWxlKHR5cGU6IGNvcmUuVHlwZSwgbWV0YWRhdGE6IGNvcmUuTmdNb2R1bGUpOiB2b2lkIHtcbiAgICB0aGlzLl9uZ01vZHVsZXMuc2V0KHR5cGUsIG1ldGFkYXRhKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIHRoZSB7QGxpbmsgTmdNb2R1bGV9IGZvciBhIG1vZHVsZTpcbiAgICogLSBTZXQgdGhlIHtAbGluayBOZ01vZHVsZX0gdG8gdGhlIG92ZXJyaWRkZW4gdmlldyB3aGVuIGl0IGV4aXN0cyBvciBmYWxsYmFjayB0byB0aGVcbiAgICogZGVmYXVsdFxuICAgKiBgTmdNb2R1bGVSZXNvbHZlcmAsIHNlZSBgc2V0TmdNb2R1bGVgLlxuICAgKi9cbiAgcmVzb2x2ZSh0eXBlOiBjb3JlLlR5cGUsIHRocm93SWZOb3RGb3VuZCA9IHRydWUpOiBjb3JlLk5nTW9kdWxlIHtcbiAgICByZXR1cm4gdGhpcy5fbmdNb2R1bGVzLmdldCh0eXBlKSB8fCBzdXBlci5yZXNvbHZlKHR5cGUsIHRocm93SWZOb3RGb3VuZCkgITtcbiAgfVxufVxuIl19