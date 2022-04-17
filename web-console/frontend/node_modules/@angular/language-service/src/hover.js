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
        define("@angular/language-service/src/hover", ["require", "exports", "@angular/language-service/src/locate_symbol"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var locate_symbol_1 = require("@angular/language-service/src/locate_symbol");
    function getHover(info) {
        var result = locate_symbol_1.locateSymbol(info);
        if (result) {
            return { text: hoverTextOf(result.symbol), span: result.span };
        }
    }
    exports.getHover = getHover;
    function hoverTextOf(symbol) {
        var result = [{ text: symbol.kind }, { text: ' ' }, { text: symbol.name, language: symbol.language }];
        var container = symbol.container;
        if (container) {
            result.push({ text: ' of ' }, { text: container.name, language: container.language });
        }
        return result;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaG92ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9sYW5ndWFnZS1zZXJ2aWNlL3NyYy9ob3Zlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUdILDZFQUE2QztJQUc3QyxTQUFnQixRQUFRLENBQUMsSUFBa0I7UUFDekMsSUFBTSxNQUFNLEdBQUcsNEJBQVksQ0FBQyxJQUFJLENBQUMsQ0FBQztRQUNsQyxJQUFJLE1BQU0sRUFBRTtZQUNWLE9BQU8sRUFBQyxJQUFJLEVBQUUsV0FBVyxDQUFDLE1BQU0sQ0FBQyxNQUFNLENBQUMsRUFBRSxJQUFJLEVBQUUsTUFBTSxDQUFDLElBQUksRUFBQyxDQUFDO1NBQzlEO0lBQ0gsQ0FBQztJQUxELDRCQUtDO0lBRUQsU0FBUyxXQUFXLENBQUMsTUFBYztRQUNqQyxJQUFNLE1BQU0sR0FDUixDQUFDLEVBQUMsSUFBSSxFQUFFLE1BQU0sQ0FBQyxJQUFJLEVBQUMsRUFBRSxFQUFDLElBQUksRUFBRSxHQUFHLEVBQUMsRUFBRSxFQUFDLElBQUksRUFBRSxNQUFNLENBQUMsSUFBSSxFQUFFLFFBQVEsRUFBRSxNQUFNLENBQUMsUUFBUSxFQUFDLENBQUMsQ0FBQztRQUN2RixJQUFNLFNBQVMsR0FBRyxNQUFNLENBQUMsU0FBUyxDQUFDO1FBQ25DLElBQUksU0FBUyxFQUFFO1lBQ2IsTUFBTSxDQUFDLElBQUksQ0FBQyxFQUFDLElBQUksRUFBRSxNQUFNLEVBQUMsRUFBRSxFQUFDLElBQUksRUFBRSxTQUFTLENBQUMsSUFBSSxFQUFFLFFBQVEsRUFBRSxTQUFTLENBQUMsUUFBUSxFQUFDLENBQUMsQ0FBQztTQUNuRjtRQUNELE9BQU8sTUFBTSxDQUFDO0lBQ2hCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VGVtcGxhdGVJbmZvfSBmcm9tICcuL2NvbW1vbic7XG5pbXBvcnQge2xvY2F0ZVN5bWJvbH0gZnJvbSAnLi9sb2NhdGVfc3ltYm9sJztcbmltcG9ydCB7SG92ZXIsIEhvdmVyVGV4dFNlY3Rpb24sIFN5bWJvbH0gZnJvbSAnLi90eXBlcyc7XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRIb3ZlcihpbmZvOiBUZW1wbGF0ZUluZm8pOiBIb3Zlcnx1bmRlZmluZWQge1xuICBjb25zdCByZXN1bHQgPSBsb2NhdGVTeW1ib2woaW5mbyk7XG4gIGlmIChyZXN1bHQpIHtcbiAgICByZXR1cm4ge3RleHQ6IGhvdmVyVGV4dE9mKHJlc3VsdC5zeW1ib2wpLCBzcGFuOiByZXN1bHQuc3Bhbn07XG4gIH1cbn1cblxuZnVuY3Rpb24gaG92ZXJUZXh0T2Yoc3ltYm9sOiBTeW1ib2wpOiBIb3ZlclRleHRTZWN0aW9uW10ge1xuICBjb25zdCByZXN1bHQ6IEhvdmVyVGV4dFNlY3Rpb25bXSA9XG4gICAgICBbe3RleHQ6IHN5bWJvbC5raW5kfSwge3RleHQ6ICcgJ30sIHt0ZXh0OiBzeW1ib2wubmFtZSwgbGFuZ3VhZ2U6IHN5bWJvbC5sYW5ndWFnZX1dO1xuICBjb25zdCBjb250YWluZXIgPSBzeW1ib2wuY29udGFpbmVyO1xuICBpZiAoY29udGFpbmVyKSB7XG4gICAgcmVzdWx0LnB1c2goe3RleHQ6ICcgb2YgJ30sIHt0ZXh0OiBjb250YWluZXIubmFtZSwgbGFuZ3VhZ2U6IGNvbnRhaW5lci5sYW5ndWFnZX0pO1xuICB9XG4gIHJldHVybiByZXN1bHQ7XG59Il19