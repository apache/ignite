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
        define("@angular/compiler/src/ml_parser/html_whitespaces", ["require", "exports", "@angular/compiler/src/ml_parser/ast", "@angular/compiler/src/ml_parser/parser", "@angular/compiler/src/ml_parser/tags"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var html = require("@angular/compiler/src/ml_parser/ast");
    var parser_1 = require("@angular/compiler/src/ml_parser/parser");
    var tags_1 = require("@angular/compiler/src/ml_parser/tags");
    exports.PRESERVE_WS_ATTR_NAME = 'ngPreserveWhitespaces';
    var SKIP_WS_TRIM_TAGS = new Set(['pre', 'template', 'textarea', 'script', 'style']);
    // Equivalent to \s with \u00a0 (non-breaking space) excluded.
    // Based on https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/RegExp
    var WS_CHARS = ' \f\n\r\t\v\u1680\u180e\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff';
    var NO_WS_REGEXP = new RegExp("[^" + WS_CHARS + "]");
    var WS_REPLACE_REGEXP = new RegExp("[" + WS_CHARS + "]{2,}", 'g');
    function hasPreserveWhitespacesAttr(attrs) {
        return attrs.some(function (attr) { return attr.name === exports.PRESERVE_WS_ATTR_NAME; });
    }
    /**
     * Angular Dart introduced &ngsp; as a placeholder for non-removable space, see:
     * https://github.com/dart-lang/angular/blob/0bb611387d29d65b5af7f9d2515ab571fd3fbee4/_tests/test/compiler/preserve_whitespace_test.dart#L25-L32
     * In Angular Dart &ngsp; is converted to the 0xE500 PUA (Private Use Areas) unicode character
     * and later on replaced by a space. We are re-implementing the same idea here.
     */
    function replaceNgsp(value) {
        // lexer is replacing the &ngsp; pseudo-entity with NGSP_UNICODE
        return value.replace(new RegExp(tags_1.NGSP_UNICODE, 'g'), ' ');
    }
    exports.replaceNgsp = replaceNgsp;
    /**
     * This visitor can walk HTML parse tree and remove / trim text nodes using the following rules:
     * - consider spaces, tabs and new lines as whitespace characters;
     * - drop text nodes consisting of whitespace characters only;
     * - for all other text nodes replace consecutive whitespace characters with one space;
     * - convert &ngsp; pseudo-entity to a single space;
     *
     * Removal and trimming of whitespaces have positive performance impact (less code to generate
     * while compiling templates, faster view creation). At the same time it can be "destructive"
     * in some cases (whitespaces can influence layout). Because of the potential of breaking layout
     * this visitor is not activated by default in Angular 5 and people need to explicitly opt-in for
     * whitespace removal. The default option for whitespace removal will be revisited in Angular 6
     * and might be changed to "on" by default.
     */
    var WhitespaceVisitor = /** @class */ (function () {
        function WhitespaceVisitor() {
        }
        WhitespaceVisitor.prototype.visitElement = function (element, context) {
            if (SKIP_WS_TRIM_TAGS.has(element.name) || hasPreserveWhitespacesAttr(element.attrs)) {
                // don't descent into elements where we need to preserve whitespaces
                // but still visit all attributes to eliminate one used as a market to preserve WS
                return new html.Element(element.name, html.visitAll(this, element.attrs), element.children, element.sourceSpan, element.startSourceSpan, element.endSourceSpan, element.i18n);
            }
            return new html.Element(element.name, element.attrs, html.visitAll(this, element.children), element.sourceSpan, element.startSourceSpan, element.endSourceSpan, element.i18n);
        };
        WhitespaceVisitor.prototype.visitAttribute = function (attribute, context) {
            return attribute.name !== exports.PRESERVE_WS_ATTR_NAME ? attribute : null;
        };
        WhitespaceVisitor.prototype.visitText = function (text, context) {
            var isNotBlank = text.value.match(NO_WS_REGEXP);
            if (isNotBlank) {
                return new html.Text(replaceNgsp(text.value).replace(WS_REPLACE_REGEXP, ' '), text.sourceSpan, text.i18n);
            }
            return null;
        };
        WhitespaceVisitor.prototype.visitComment = function (comment, context) { return comment; };
        WhitespaceVisitor.prototype.visitExpansion = function (expansion, context) { return expansion; };
        WhitespaceVisitor.prototype.visitExpansionCase = function (expansionCase, context) { return expansionCase; };
        return WhitespaceVisitor;
    }());
    exports.WhitespaceVisitor = WhitespaceVisitor;
    function removeWhitespaces(htmlAstWithErrors) {
        return new parser_1.ParseTreeResult(html.visitAll(new WhitespaceVisitor(), htmlAstWithErrors.rootNodes), htmlAstWithErrors.errors);
    }
    exports.removeWhitespaces = removeWhitespaces;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHRtbF93aGl0ZXNwYWNlcy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9tbF9wYXJzZXIvaHRtbF93aGl0ZXNwYWNlcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILDBEQUE4QjtJQUM5QixpRUFBeUM7SUFDekMsNkRBQW9DO0lBRXZCLFFBQUEscUJBQXFCLEdBQUcsdUJBQXVCLENBQUM7SUFFN0QsSUFBTSxpQkFBaUIsR0FBRyxJQUFJLEdBQUcsQ0FBQyxDQUFDLEtBQUssRUFBRSxVQUFVLEVBQUUsVUFBVSxFQUFFLFFBQVEsRUFBRSxPQUFPLENBQUMsQ0FBQyxDQUFDO0lBRXRGLDhEQUE4RDtJQUM5RCxtR0FBbUc7SUFDbkcsSUFBTSxRQUFRLEdBQUcsMEVBQTBFLENBQUM7SUFDNUYsSUFBTSxZQUFZLEdBQUcsSUFBSSxNQUFNLENBQUMsT0FBSyxRQUFRLE1BQUcsQ0FBQyxDQUFDO0lBQ2xELElBQU0saUJBQWlCLEdBQUcsSUFBSSxNQUFNLENBQUMsTUFBSSxRQUFRLFVBQU8sRUFBRSxHQUFHLENBQUMsQ0FBQztJQUUvRCxTQUFTLDBCQUEwQixDQUFDLEtBQXVCO1FBQ3pELE9BQU8sS0FBSyxDQUFDLElBQUksQ0FBQyxVQUFDLElBQW9CLElBQUssT0FBQSxJQUFJLENBQUMsSUFBSSxLQUFLLDZCQUFxQixFQUFuQyxDQUFtQyxDQUFDLENBQUM7SUFDbkYsQ0FBQztJQUVEOzs7OztPQUtHO0lBQ0gsU0FBZ0IsV0FBVyxDQUFDLEtBQWE7UUFDdkMsZ0VBQWdFO1FBQ2hFLE9BQU8sS0FBSyxDQUFDLE9BQU8sQ0FBQyxJQUFJLE1BQU0sQ0FBQyxtQkFBWSxFQUFFLEdBQUcsQ0FBQyxFQUFFLEdBQUcsQ0FBQyxDQUFDO0lBQzNELENBQUM7SUFIRCxrQ0FHQztJQUVEOzs7Ozs7Ozs7Ozs7O09BYUc7SUFDSDtRQUFBO1FBbUNBLENBQUM7UUFsQ0Msd0NBQVksR0FBWixVQUFhLE9BQXFCLEVBQUUsT0FBWTtZQUM5QyxJQUFJLGlCQUFpQixDQUFDLEdBQUcsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLElBQUksMEJBQTBCLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxFQUFFO2dCQUNwRixvRUFBb0U7Z0JBQ3BFLGtGQUFrRjtnQkFDbEYsT0FBTyxJQUFJLElBQUksQ0FBQyxPQUFPLENBQ25CLE9BQU8sQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLEtBQUssQ0FBQyxFQUFFLE9BQU8sQ0FBQyxRQUFRLEVBQUUsT0FBTyxDQUFDLFVBQVUsRUFDdEYsT0FBTyxDQUFDLGVBQWUsRUFBRSxPQUFPLENBQUMsYUFBYSxFQUFFLE9BQU8sQ0FBQyxJQUFJLENBQUMsQ0FBQzthQUNuRTtZQUVELE9BQU8sSUFBSSxJQUFJLENBQUMsT0FBTyxDQUNuQixPQUFPLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxLQUFLLEVBQUUsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLFFBQVEsQ0FBQyxFQUFFLE9BQU8sQ0FBQyxVQUFVLEVBQ3RGLE9BQU8sQ0FBQyxlQUFlLEVBQUUsT0FBTyxDQUFDLGFBQWEsRUFBRSxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDcEUsQ0FBQztRQUVELDBDQUFjLEdBQWQsVUFBZSxTQUF5QixFQUFFLE9BQVk7WUFDcEQsT0FBTyxTQUFTLENBQUMsSUFBSSxLQUFLLDZCQUFxQixDQUFDLENBQUMsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztRQUNyRSxDQUFDO1FBRUQscUNBQVMsR0FBVCxVQUFVLElBQWUsRUFBRSxPQUFZO1lBQ3JDLElBQU0sVUFBVSxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLFlBQVksQ0FBQyxDQUFDO1lBRWxELElBQUksVUFBVSxFQUFFO2dCQUNkLE9BQU8sSUFBSSxJQUFJLENBQUMsSUFBSSxDQUNoQixXQUFXLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLE9BQU8sQ0FBQyxpQkFBaUIsRUFBRSxHQUFHLENBQUMsRUFBRSxJQUFJLENBQUMsVUFBVSxFQUFFLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQzthQUMxRjtZQUVELE9BQU8sSUFBSSxDQUFDO1FBQ2QsQ0FBQztRQUVELHdDQUFZLEdBQVosVUFBYSxPQUFxQixFQUFFLE9BQVksSUFBUyxPQUFPLE9BQU8sQ0FBQyxDQUFDLENBQUM7UUFFMUUsMENBQWMsR0FBZCxVQUFlLFNBQXlCLEVBQUUsT0FBWSxJQUFTLE9BQU8sU0FBUyxDQUFDLENBQUMsQ0FBQztRQUVsRiw4Q0FBa0IsR0FBbEIsVUFBbUIsYUFBaUMsRUFBRSxPQUFZLElBQVMsT0FBTyxhQUFhLENBQUMsQ0FBQyxDQUFDO1FBQ3BHLHdCQUFDO0lBQUQsQ0FBQyxBQW5DRCxJQW1DQztJQW5DWSw4Q0FBaUI7SUFxQzlCLFNBQWdCLGlCQUFpQixDQUFDLGlCQUFrQztRQUNsRSxPQUFPLElBQUksd0JBQWUsQ0FDdEIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLGlCQUFpQixFQUFFLEVBQUUsaUJBQWlCLENBQUMsU0FBUyxDQUFDLEVBQ25FLGlCQUFpQixDQUFDLE1BQU0sQ0FBQyxDQUFDO0lBQ2hDLENBQUM7SUFKRCw4Q0FJQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgaHRtbCBmcm9tICcuL2FzdCc7XG5pbXBvcnQge1BhcnNlVHJlZVJlc3VsdH0gZnJvbSAnLi9wYXJzZXInO1xuaW1wb3J0IHtOR1NQX1VOSUNPREV9IGZyb20gJy4vdGFncyc7XG5cbmV4cG9ydCBjb25zdCBQUkVTRVJWRV9XU19BVFRSX05BTUUgPSAnbmdQcmVzZXJ2ZVdoaXRlc3BhY2VzJztcblxuY29uc3QgU0tJUF9XU19UUklNX1RBR1MgPSBuZXcgU2V0KFsncHJlJywgJ3RlbXBsYXRlJywgJ3RleHRhcmVhJywgJ3NjcmlwdCcsICdzdHlsZSddKTtcblxuLy8gRXF1aXZhbGVudCB0byBcXHMgd2l0aCBcXHUwMGEwIChub24tYnJlYWtpbmcgc3BhY2UpIGV4Y2x1ZGVkLlxuLy8gQmFzZWQgb24gaHR0cHM6Ly9kZXZlbG9wZXIubW96aWxsYS5vcmcvZW4tVVMvZG9jcy9XZWIvSmF2YVNjcmlwdC9SZWZlcmVuY2UvR2xvYmFsX09iamVjdHMvUmVnRXhwXG5jb25zdCBXU19DSEFSUyA9ICcgXFxmXFxuXFxyXFx0XFx2XFx1MTY4MFxcdTE4MGVcXHUyMDAwLVxcdTIwMGFcXHUyMDI4XFx1MjAyOVxcdTIwMmZcXHUyMDVmXFx1MzAwMFxcdWZlZmYnO1xuY29uc3QgTk9fV1NfUkVHRVhQID0gbmV3IFJlZ0V4cChgW14ke1dTX0NIQVJTfV1gKTtcbmNvbnN0IFdTX1JFUExBQ0VfUkVHRVhQID0gbmV3IFJlZ0V4cChgWyR7V1NfQ0hBUlN9XXsyLH1gLCAnZycpO1xuXG5mdW5jdGlvbiBoYXNQcmVzZXJ2ZVdoaXRlc3BhY2VzQXR0cihhdHRyczogaHRtbC5BdHRyaWJ1dGVbXSk6IGJvb2xlYW4ge1xuICByZXR1cm4gYXR0cnMuc29tZSgoYXR0cjogaHRtbC5BdHRyaWJ1dGUpID0+IGF0dHIubmFtZSA9PT0gUFJFU0VSVkVfV1NfQVRUUl9OQU1FKTtcbn1cblxuLyoqXG4gKiBBbmd1bGFyIERhcnQgaW50cm9kdWNlZCAmbmdzcDsgYXMgYSBwbGFjZWhvbGRlciBmb3Igbm9uLXJlbW92YWJsZSBzcGFjZSwgc2VlOlxuICogaHR0cHM6Ly9naXRodWIuY29tL2RhcnQtbGFuZy9hbmd1bGFyL2Jsb2IvMGJiNjExMzg3ZDI5ZDY1YjVhZjdmOWQyNTE1YWI1NzFmZDNmYmVlNC9fdGVzdHMvdGVzdC9jb21waWxlci9wcmVzZXJ2ZV93aGl0ZXNwYWNlX3Rlc3QuZGFydCNMMjUtTDMyXG4gKiBJbiBBbmd1bGFyIERhcnQgJm5nc3A7IGlzIGNvbnZlcnRlZCB0byB0aGUgMHhFNTAwIFBVQSAoUHJpdmF0ZSBVc2UgQXJlYXMpIHVuaWNvZGUgY2hhcmFjdGVyXG4gKiBhbmQgbGF0ZXIgb24gcmVwbGFjZWQgYnkgYSBzcGFjZS4gV2UgYXJlIHJlLWltcGxlbWVudGluZyB0aGUgc2FtZSBpZGVhIGhlcmUuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiByZXBsYWNlTmdzcCh2YWx1ZTogc3RyaW5nKTogc3RyaW5nIHtcbiAgLy8gbGV4ZXIgaXMgcmVwbGFjaW5nIHRoZSAmbmdzcDsgcHNldWRvLWVudGl0eSB3aXRoIE5HU1BfVU5JQ09ERVxuICByZXR1cm4gdmFsdWUucmVwbGFjZShuZXcgUmVnRXhwKE5HU1BfVU5JQ09ERSwgJ2cnKSwgJyAnKTtcbn1cblxuLyoqXG4gKiBUaGlzIHZpc2l0b3IgY2FuIHdhbGsgSFRNTCBwYXJzZSB0cmVlIGFuZCByZW1vdmUgLyB0cmltIHRleHQgbm9kZXMgdXNpbmcgdGhlIGZvbGxvd2luZyBydWxlczpcbiAqIC0gY29uc2lkZXIgc3BhY2VzLCB0YWJzIGFuZCBuZXcgbGluZXMgYXMgd2hpdGVzcGFjZSBjaGFyYWN0ZXJzO1xuICogLSBkcm9wIHRleHQgbm9kZXMgY29uc2lzdGluZyBvZiB3aGl0ZXNwYWNlIGNoYXJhY3RlcnMgb25seTtcbiAqIC0gZm9yIGFsbCBvdGhlciB0ZXh0IG5vZGVzIHJlcGxhY2UgY29uc2VjdXRpdmUgd2hpdGVzcGFjZSBjaGFyYWN0ZXJzIHdpdGggb25lIHNwYWNlO1xuICogLSBjb252ZXJ0ICZuZ3NwOyBwc2V1ZG8tZW50aXR5IHRvIGEgc2luZ2xlIHNwYWNlO1xuICpcbiAqIFJlbW92YWwgYW5kIHRyaW1taW5nIG9mIHdoaXRlc3BhY2VzIGhhdmUgcG9zaXRpdmUgcGVyZm9ybWFuY2UgaW1wYWN0IChsZXNzIGNvZGUgdG8gZ2VuZXJhdGVcbiAqIHdoaWxlIGNvbXBpbGluZyB0ZW1wbGF0ZXMsIGZhc3RlciB2aWV3IGNyZWF0aW9uKS4gQXQgdGhlIHNhbWUgdGltZSBpdCBjYW4gYmUgXCJkZXN0cnVjdGl2ZVwiXG4gKiBpbiBzb21lIGNhc2VzICh3aGl0ZXNwYWNlcyBjYW4gaW5mbHVlbmNlIGxheW91dCkuIEJlY2F1c2Ugb2YgdGhlIHBvdGVudGlhbCBvZiBicmVha2luZyBsYXlvdXRcbiAqIHRoaXMgdmlzaXRvciBpcyBub3QgYWN0aXZhdGVkIGJ5IGRlZmF1bHQgaW4gQW5ndWxhciA1IGFuZCBwZW9wbGUgbmVlZCB0byBleHBsaWNpdGx5IG9wdC1pbiBmb3JcbiAqIHdoaXRlc3BhY2UgcmVtb3ZhbC4gVGhlIGRlZmF1bHQgb3B0aW9uIGZvciB3aGl0ZXNwYWNlIHJlbW92YWwgd2lsbCBiZSByZXZpc2l0ZWQgaW4gQW5ndWxhciA2XG4gKiBhbmQgbWlnaHQgYmUgY2hhbmdlZCB0byBcIm9uXCIgYnkgZGVmYXVsdC5cbiAqL1xuZXhwb3J0IGNsYXNzIFdoaXRlc3BhY2VWaXNpdG9yIGltcGxlbWVudHMgaHRtbC5WaXNpdG9yIHtcbiAgdmlzaXRFbGVtZW50KGVsZW1lbnQ6IGh0bWwuRWxlbWVudCwgY29udGV4dDogYW55KTogYW55IHtcbiAgICBpZiAoU0tJUF9XU19UUklNX1RBR1MuaGFzKGVsZW1lbnQubmFtZSkgfHwgaGFzUHJlc2VydmVXaGl0ZXNwYWNlc0F0dHIoZWxlbWVudC5hdHRycykpIHtcbiAgICAgIC8vIGRvbid0IGRlc2NlbnQgaW50byBlbGVtZW50cyB3aGVyZSB3ZSBuZWVkIHRvIHByZXNlcnZlIHdoaXRlc3BhY2VzXG4gICAgICAvLyBidXQgc3RpbGwgdmlzaXQgYWxsIGF0dHJpYnV0ZXMgdG8gZWxpbWluYXRlIG9uZSB1c2VkIGFzIGEgbWFya2V0IHRvIHByZXNlcnZlIFdTXG4gICAgICByZXR1cm4gbmV3IGh0bWwuRWxlbWVudChcbiAgICAgICAgICBlbGVtZW50Lm5hbWUsIGh0bWwudmlzaXRBbGwodGhpcywgZWxlbWVudC5hdHRycyksIGVsZW1lbnQuY2hpbGRyZW4sIGVsZW1lbnQuc291cmNlU3BhbixcbiAgICAgICAgICBlbGVtZW50LnN0YXJ0U291cmNlU3BhbiwgZWxlbWVudC5lbmRTb3VyY2VTcGFuLCBlbGVtZW50LmkxOG4pO1xuICAgIH1cblxuICAgIHJldHVybiBuZXcgaHRtbC5FbGVtZW50KFxuICAgICAgICBlbGVtZW50Lm5hbWUsIGVsZW1lbnQuYXR0cnMsIGh0bWwudmlzaXRBbGwodGhpcywgZWxlbWVudC5jaGlsZHJlbiksIGVsZW1lbnQuc291cmNlU3BhbixcbiAgICAgICAgZWxlbWVudC5zdGFydFNvdXJjZVNwYW4sIGVsZW1lbnQuZW5kU291cmNlU3BhbiwgZWxlbWVudC5pMThuKTtcbiAgfVxuXG4gIHZpc2l0QXR0cmlidXRlKGF0dHJpYnV0ZTogaHRtbC5BdHRyaWJ1dGUsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIGF0dHJpYnV0ZS5uYW1lICE9PSBQUkVTRVJWRV9XU19BVFRSX05BTUUgPyBhdHRyaWJ1dGUgOiBudWxsO1xuICB9XG5cbiAgdmlzaXRUZXh0KHRleHQ6IGh0bWwuVGV4dCwgY29udGV4dDogYW55KTogYW55IHtcbiAgICBjb25zdCBpc05vdEJsYW5rID0gdGV4dC52YWx1ZS5tYXRjaChOT19XU19SRUdFWFApO1xuXG4gICAgaWYgKGlzTm90QmxhbmspIHtcbiAgICAgIHJldHVybiBuZXcgaHRtbC5UZXh0KFxuICAgICAgICAgIHJlcGxhY2VOZ3NwKHRleHQudmFsdWUpLnJlcGxhY2UoV1NfUkVQTEFDRV9SRUdFWFAsICcgJyksIHRleHQuc291cmNlU3BhbiwgdGV4dC5pMThuKTtcbiAgICB9XG5cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHZpc2l0Q29tbWVudChjb21tZW50OiBodG1sLkNvbW1lbnQsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiBjb21tZW50OyB9XG5cbiAgdmlzaXRFeHBhbnNpb24oZXhwYW5zaW9uOiBodG1sLkV4cGFuc2lvbiwgY29udGV4dDogYW55KTogYW55IHsgcmV0dXJuIGV4cGFuc2lvbjsgfVxuXG4gIHZpc2l0RXhwYW5zaW9uQ2FzZShleHBhbnNpb25DYXNlOiBodG1sLkV4cGFuc2lvbkNhc2UsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiBleHBhbnNpb25DYXNlOyB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVXaGl0ZXNwYWNlcyhodG1sQXN0V2l0aEVycm9yczogUGFyc2VUcmVlUmVzdWx0KTogUGFyc2VUcmVlUmVzdWx0IHtcbiAgcmV0dXJuIG5ldyBQYXJzZVRyZWVSZXN1bHQoXG4gICAgICBodG1sLnZpc2l0QWxsKG5ldyBXaGl0ZXNwYWNlVmlzaXRvcigpLCBodG1sQXN0V2l0aEVycm9ycy5yb290Tm9kZXMpLFxuICAgICAgaHRtbEFzdFdpdGhFcnJvcnMuZXJyb3JzKTtcbn1cbiJdfQ==