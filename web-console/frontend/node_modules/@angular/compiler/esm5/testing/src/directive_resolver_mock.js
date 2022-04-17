import * as tslib_1 from "tslib";
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { DirectiveResolver } from '@angular/compiler';
/**
 * An implementation of {@link DirectiveResolver} that allows overriding
 * various properties of directives.
 */
var MockDirectiveResolver = /** @class */ (function (_super) {
    tslib_1.__extends(MockDirectiveResolver, _super);
    function MockDirectiveResolver(reflector) {
        var _this = _super.call(this, reflector) || this;
        _this._directives = new Map();
        return _this;
    }
    MockDirectiveResolver.prototype.resolve = function (type, throwIfNotFound) {
        if (throwIfNotFound === void 0) { throwIfNotFound = true; }
        return this._directives.get(type) || _super.prototype.resolve.call(this, type, throwIfNotFound);
    };
    /**
     * Overrides the {@link core.Directive} for a directive.
     */
    MockDirectiveResolver.prototype.setDirective = function (type, metadata) {
        this._directives.set(type, metadata);
    };
    return MockDirectiveResolver;
}(DirectiveResolver));
export { MockDirectiveResolver };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlyZWN0aXZlX3Jlc29sdmVyX21vY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci90ZXN0aW5nL3NyYy9kaXJlY3RpdmVfcmVzb2x2ZXJfbW9jay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiO0FBQUE7Ozs7OztHQU1HO0FBQ0gsT0FBTyxFQUFtQixpQkFBaUIsRUFBTyxNQUFNLG1CQUFtQixDQUFDO0FBRTVFOzs7R0FHRztBQUNIO0lBQTJDLGlEQUFpQjtJQUcxRCwrQkFBWSxTQUEyQjtRQUF2QyxZQUEyQyxrQkFBTSxTQUFTLENBQUMsU0FBRztRQUZ0RCxpQkFBVyxHQUFHLElBQUksR0FBRyxFQUE2QixDQUFDOztJQUVFLENBQUM7SUFLOUQsdUNBQU8sR0FBUCxVQUFRLElBQWUsRUFBRSxlQUFzQjtRQUF0QixnQ0FBQSxFQUFBLHNCQUFzQjtRQUM3QyxPQUFPLElBQUksQ0FBQyxXQUFXLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxJQUFJLGlCQUFNLE9BQU8sWUFBQyxJQUFJLEVBQUUsZUFBZSxDQUFDLENBQUM7SUFDNUUsQ0FBQztJQUVEOztPQUVHO0lBQ0gsNENBQVksR0FBWixVQUFhLElBQWUsRUFBRSxRQUF3QjtRQUNwRCxJQUFJLENBQUMsV0FBVyxDQUFDLEdBQUcsQ0FBQyxJQUFJLEVBQUUsUUFBUSxDQUFDLENBQUM7SUFDdkMsQ0FBQztJQUNILDRCQUFDO0FBQUQsQ0FBQyxBQWxCRCxDQUEyQyxpQkFBaUIsR0FrQjNEIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yLCBEaXJlY3RpdmVSZXNvbHZlciwgY29yZX0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuXG4vKipcbiAqIEFuIGltcGxlbWVudGF0aW9uIG9mIHtAbGluayBEaXJlY3RpdmVSZXNvbHZlcn0gdGhhdCBhbGxvd3Mgb3ZlcnJpZGluZ1xuICogdmFyaW91cyBwcm9wZXJ0aWVzIG9mIGRpcmVjdGl2ZXMuXG4gKi9cbmV4cG9ydCBjbGFzcyBNb2NrRGlyZWN0aXZlUmVzb2x2ZXIgZXh0ZW5kcyBEaXJlY3RpdmVSZXNvbHZlciB7XG4gIHByaXZhdGUgX2RpcmVjdGl2ZXMgPSBuZXcgTWFwPGNvcmUuVHlwZSwgY29yZS5EaXJlY3RpdmU+KCk7XG5cbiAgY29uc3RydWN0b3IocmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7IHN1cGVyKHJlZmxlY3Rvcik7IH1cblxuICByZXNvbHZlKHR5cGU6IGNvcmUuVHlwZSk6IGNvcmUuRGlyZWN0aXZlO1xuICByZXNvbHZlKHR5cGU6IGNvcmUuVHlwZSwgdGhyb3dJZk5vdEZvdW5kOiB0cnVlKTogY29yZS5EaXJlY3RpdmU7XG4gIHJlc29sdmUodHlwZTogY29yZS5UeXBlLCB0aHJvd0lmTm90Rm91bmQ6IGJvb2xlYW4pOiBjb3JlLkRpcmVjdGl2ZXxudWxsO1xuICByZXNvbHZlKHR5cGU6IGNvcmUuVHlwZSwgdGhyb3dJZk5vdEZvdW5kID0gdHJ1ZSk6IGNvcmUuRGlyZWN0aXZlfG51bGwge1xuICAgIHJldHVybiB0aGlzLl9kaXJlY3RpdmVzLmdldCh0eXBlKSB8fCBzdXBlci5yZXNvbHZlKHR5cGUsIHRocm93SWZOb3RGb3VuZCk7XG4gIH1cblxuICAvKipcbiAgICogT3ZlcnJpZGVzIHRoZSB7QGxpbmsgY29yZS5EaXJlY3RpdmV9IGZvciBhIGRpcmVjdGl2ZS5cbiAgICovXG4gIHNldERpcmVjdGl2ZSh0eXBlOiBjb3JlLlR5cGUsIG1ldGFkYXRhOiBjb3JlLkRpcmVjdGl2ZSk6IHZvaWQge1xuICAgIHRoaXMuX2RpcmVjdGl2ZXMuc2V0KHR5cGUsIG1ldGFkYXRhKTtcbiAgfVxufVxuIl19