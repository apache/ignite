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
        define("@angular/compiler/src/ml_parser/html_parser", ["require", "exports", "tslib", "@angular/compiler/src/ml_parser/html_tags", "@angular/compiler/src/ml_parser/parser", "@angular/compiler/src/ml_parser/parser"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var html_tags_1 = require("@angular/compiler/src/ml_parser/html_tags");
    var parser_1 = require("@angular/compiler/src/ml_parser/parser");
    var parser_2 = require("@angular/compiler/src/ml_parser/parser");
    exports.ParseTreeResult = parser_2.ParseTreeResult;
    exports.TreeError = parser_2.TreeError;
    var HtmlParser = /** @class */ (function (_super) {
        tslib_1.__extends(HtmlParser, _super);
        function HtmlParser() {
            return _super.call(this, html_tags_1.getHtmlTagDefinition) || this;
        }
        HtmlParser.prototype.parse = function (source, url, options) {
            return _super.prototype.parse.call(this, source, url, options);
        };
        return HtmlParser;
    }(parser_1.Parser));
    exports.HtmlParser = HtmlParser;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHRtbF9wYXJzZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvbWxfcGFyc2VyL2h0bWxfcGFyc2VyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILHVFQUFpRDtJQUVqRCxpRUFBaUQ7SUFFakQsaUVBQW9EO0lBQTVDLG1DQUFBLGVBQWUsQ0FBQTtJQUFFLDZCQUFBLFNBQVMsQ0FBQTtJQUVsQztRQUFnQyxzQ0FBTTtRQUNwQzttQkFBZ0Isa0JBQU0sZ0NBQW9CLENBQUM7UUFBRSxDQUFDO1FBRTlDLDBCQUFLLEdBQUwsVUFBTSxNQUFjLEVBQUUsR0FBVyxFQUFFLE9BQXlCO1lBQzFELE9BQU8saUJBQU0sS0FBSyxZQUFDLE1BQU0sRUFBRSxHQUFHLEVBQUUsT0FBTyxDQUFDLENBQUM7UUFDM0MsQ0FBQztRQUNILGlCQUFDO0lBQUQsQ0FBQyxBQU5ELENBQWdDLGVBQU0sR0FNckM7SUFOWSxnQ0FBVSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtnZXRIdG1sVGFnRGVmaW5pdGlvbn0gZnJvbSAnLi9odG1sX3RhZ3MnO1xuaW1wb3J0IHtUb2tlbml6ZU9wdGlvbnN9IGZyb20gJy4vbGV4ZXInO1xuaW1wb3J0IHtQYXJzZVRyZWVSZXN1bHQsIFBhcnNlcn0gZnJvbSAnLi9wYXJzZXInO1xuXG5leHBvcnQge1BhcnNlVHJlZVJlc3VsdCwgVHJlZUVycm9yfSBmcm9tICcuL3BhcnNlcic7XG5cbmV4cG9ydCBjbGFzcyBIdG1sUGFyc2VyIGV4dGVuZHMgUGFyc2VyIHtcbiAgY29uc3RydWN0b3IoKSB7IHN1cGVyKGdldEh0bWxUYWdEZWZpbml0aW9uKTsgfVxuXG4gIHBhcnNlKHNvdXJjZTogc3RyaW5nLCB1cmw6IHN0cmluZywgb3B0aW9ucz86IFRva2VuaXplT3B0aW9ucyk6IFBhcnNlVHJlZVJlc3VsdCB7XG4gICAgcmV0dXJuIHN1cGVyLnBhcnNlKHNvdXJjZSwgdXJsLCBvcHRpb25zKTtcbiAgfVxufVxuIl19