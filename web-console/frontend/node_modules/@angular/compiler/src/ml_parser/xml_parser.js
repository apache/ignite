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
        define("@angular/compiler/src/ml_parser/xml_parser", ["require", "exports", "tslib", "@angular/compiler/src/ml_parser/parser", "@angular/compiler/src/ml_parser/xml_tags", "@angular/compiler/src/ml_parser/parser"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var parser_1 = require("@angular/compiler/src/ml_parser/parser");
    var xml_tags_1 = require("@angular/compiler/src/ml_parser/xml_tags");
    var parser_2 = require("@angular/compiler/src/ml_parser/parser");
    exports.ParseTreeResult = parser_2.ParseTreeResult;
    exports.TreeError = parser_2.TreeError;
    var XmlParser = /** @class */ (function (_super) {
        tslib_1.__extends(XmlParser, _super);
        function XmlParser() {
            return _super.call(this, xml_tags_1.getXmlTagDefinition) || this;
        }
        XmlParser.prototype.parse = function (source, url, options) {
            return _super.prototype.parse.call(this, source, url, options);
        };
        return XmlParser;
    }(parser_1.Parser));
    exports.XmlParser = XmlParser;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieG1sX3BhcnNlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9tbF9wYXJzZXIveG1sX3BhcnNlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFHSCxpRUFBaUQ7SUFDakQscUVBQStDO0lBRS9DLGlFQUFvRDtJQUE1QyxtQ0FBQSxlQUFlLENBQUE7SUFBRSw2QkFBQSxTQUFTLENBQUE7SUFFbEM7UUFBK0IscUNBQU07UUFDbkM7bUJBQWdCLGtCQUFNLDhCQUFtQixDQUFDO1FBQUUsQ0FBQztRQUU3Qyx5QkFBSyxHQUFMLFVBQU0sTUFBYyxFQUFFLEdBQVcsRUFBRSxPQUF5QjtZQUMxRCxPQUFPLGlCQUFNLEtBQUssWUFBQyxNQUFNLEVBQUUsR0FBRyxFQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQzNDLENBQUM7UUFDSCxnQkFBQztJQUFELENBQUMsQUFORCxDQUErQixlQUFNLEdBTXBDO0lBTlksOEJBQVMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7VG9rZW5pemVPcHRpb25zfSBmcm9tICcuL2xleGVyJztcbmltcG9ydCB7UGFyc2VUcmVlUmVzdWx0LCBQYXJzZXJ9IGZyb20gJy4vcGFyc2VyJztcbmltcG9ydCB7Z2V0WG1sVGFnRGVmaW5pdGlvbn0gZnJvbSAnLi94bWxfdGFncyc7XG5cbmV4cG9ydCB7UGFyc2VUcmVlUmVzdWx0LCBUcmVlRXJyb3J9IGZyb20gJy4vcGFyc2VyJztcblxuZXhwb3J0IGNsYXNzIFhtbFBhcnNlciBleHRlbmRzIFBhcnNlciB7XG4gIGNvbnN0cnVjdG9yKCkgeyBzdXBlcihnZXRYbWxUYWdEZWZpbml0aW9uKTsgfVxuXG4gIHBhcnNlKHNvdXJjZTogc3RyaW5nLCB1cmw6IHN0cmluZywgb3B0aW9ucz86IFRva2VuaXplT3B0aW9ucyk6IFBhcnNlVHJlZVJlc3VsdCB7XG4gICAgcmV0dXJuIHN1cGVyLnBhcnNlKHNvdXJjZSwgdXJsLCBvcHRpb25zKTtcbiAgfVxufVxuIl19