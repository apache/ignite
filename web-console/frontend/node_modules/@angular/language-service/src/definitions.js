/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/language-service/src/definitions", ["require", "exports", "@angular/language-service/src/locate_symbol"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var locate_symbol_1 = require("@angular/language-service/src/locate_symbol");
    function getDefinition(info) {
        var result = locate_symbol_1.locateSymbol(info);
        return result && result.symbol.definition;
    }
    exports.getDefinition = getDefinition;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGVmaW5pdGlvbnMuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9sYW5ndWFnZS1zZXJ2aWNlL3NyYy9kZWZpbml0aW9ucy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUdILDZFQUE2QztJQUc3QyxTQUFnQixhQUFhLENBQUMsSUFBa0I7UUFDOUMsSUFBTSxNQUFNLEdBQUcsNEJBQVksQ0FBQyxJQUFJLENBQUMsQ0FBQztRQUNsQyxPQUFPLE1BQU0sSUFBSSxNQUFNLENBQUMsTUFBTSxDQUFDLFVBQVUsQ0FBQztJQUM1QyxDQUFDO0lBSEQsc0NBR0MiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VGVtcGxhdGVJbmZvfSBmcm9tICcuL2NvbW1vbic7XG5pbXBvcnQge2xvY2F0ZVN5bWJvbH0gZnJvbSAnLi9sb2NhdGVfc3ltYm9sJztcbmltcG9ydCB7RGVmaW5pdGlvbn0gZnJvbSAnLi90eXBlcyc7XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWZpbml0aW9uKGluZm86IFRlbXBsYXRlSW5mbyk6IERlZmluaXRpb24ge1xuICBjb25zdCByZXN1bHQgPSBsb2NhdGVTeW1ib2woaW5mbyk7XG4gIHJldHVybiByZXN1bHQgJiYgcmVzdWx0LnN5bWJvbC5kZWZpbml0aW9uO1xufVxuIl19