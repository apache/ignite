(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/testing/src/directive_resolver_mock", ["require", "exports", "tslib", "@angular/compiler"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    /**
     * @license
     * Copyright Google Inc. All Rights Reserved.
     *
     * Use of this source code is governed by an MIT-style license that can be
     * found in the LICENSE file at https://angular.io/license
     */
    var compiler_1 = require("@angular/compiler");
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
    }(compiler_1.DirectiveResolver));
    exports.MockDirectiveResolver = MockDirectiveResolver;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlyZWN0aXZlX3Jlc29sdmVyX21vY2suanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci90ZXN0aW5nL3NyYy9kaXJlY3RpdmVfcmVzb2x2ZXJfbW9jay50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7SUFBQTs7Ozs7O09BTUc7SUFDSCw4Q0FBNEU7SUFFNUU7OztPQUdHO0lBQ0g7UUFBMkMsaURBQWlCO1FBRzFELCtCQUFZLFNBQTJCO1lBQXZDLFlBQTJDLGtCQUFNLFNBQVMsQ0FBQyxTQUFHO1lBRnRELGlCQUFXLEdBQUcsSUFBSSxHQUFHLEVBQTZCLENBQUM7O1FBRUUsQ0FBQztRQUs5RCx1Q0FBTyxHQUFQLFVBQVEsSUFBZSxFQUFFLGVBQXNCO1lBQXRCLGdDQUFBLEVBQUEsc0JBQXNCO1lBQzdDLE9BQU8sSUFBSSxDQUFDLFdBQVcsQ0FBQyxHQUFHLENBQUMsSUFBSSxDQUFDLElBQUksaUJBQU0sT0FBTyxZQUFDLElBQUksRUFBRSxlQUFlLENBQUMsQ0FBQztRQUM1RSxDQUFDO1FBRUQ7O1dBRUc7UUFDSCw0Q0FBWSxHQUFaLFVBQWEsSUFBZSxFQUFFLFFBQXdCO1lBQ3BELElBQUksQ0FBQyxXQUFXLENBQUMsR0FBRyxDQUFDLElBQUksRUFBRSxRQUFRLENBQUMsQ0FBQztRQUN2QyxDQUFDO1FBQ0gsNEJBQUM7SUFBRCxDQUFDLEFBbEJELENBQTJDLDRCQUFpQixHQWtCM0Q7SUFsQlksc0RBQXFCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yLCBEaXJlY3RpdmVSZXNvbHZlciwgY29yZX0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuXG4vKipcbiAqIEFuIGltcGxlbWVudGF0aW9uIG9mIHtAbGluayBEaXJlY3RpdmVSZXNvbHZlcn0gdGhhdCBhbGxvd3Mgb3ZlcnJpZGluZ1xuICogdmFyaW91cyBwcm9wZXJ0aWVzIG9mIGRpcmVjdGl2ZXMuXG4gKi9cbmV4cG9ydCBjbGFzcyBNb2NrRGlyZWN0aXZlUmVzb2x2ZXIgZXh0ZW5kcyBEaXJlY3RpdmVSZXNvbHZlciB7XG4gIHByaXZhdGUgX2RpcmVjdGl2ZXMgPSBuZXcgTWFwPGNvcmUuVHlwZSwgY29yZS5EaXJlY3RpdmU+KCk7XG5cbiAgY29uc3RydWN0b3IocmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7IHN1cGVyKHJlZmxlY3Rvcik7IH1cblxuICByZXNvbHZlKHR5cGU6IGNvcmUuVHlwZSk6IGNvcmUuRGlyZWN0aXZlO1xuICByZXNvbHZlKHR5cGU6IGNvcmUuVHlwZSwgdGhyb3dJZk5vdEZvdW5kOiB0cnVlKTogY29yZS5EaXJlY3RpdmU7XG4gIHJlc29sdmUodHlwZTogY29yZS5UeXBlLCB0aHJvd0lmTm90Rm91bmQ6IGJvb2xlYW4pOiBjb3JlLkRpcmVjdGl2ZXxudWxsO1xuICByZXNvbHZlKHR5cGU6IGNvcmUuVHlwZSwgdGhyb3dJZk5vdEZvdW5kID0gdHJ1ZSk6IGNvcmUuRGlyZWN0aXZlfG51bGwge1xuICAgIHJldHVybiB0aGlzLl9kaXJlY3RpdmVzLmdldCh0eXBlKSB8fCBzdXBlci5yZXNvbHZlKHR5cGUsIHRocm93SWZOb3RGb3VuZCk7XG4gIH1cblxuICAvKipcbiAgICogT3ZlcnJpZGVzIHRoZSB7QGxpbmsgY29yZS5EaXJlY3RpdmV9IGZvciBhIGRpcmVjdGl2ZS5cbiAgICovXG4gIHNldERpcmVjdGl2ZSh0eXBlOiBjb3JlLlR5cGUsIG1ldGFkYXRhOiBjb3JlLkRpcmVjdGl2ZSk6IHZvaWQge1xuICAgIHRoaXMuX2RpcmVjdGl2ZXMuc2V0KHR5cGUsIG1ldGFkYXRhKTtcbiAgfVxufVxuIl19