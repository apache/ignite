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
        define("@angular/compiler/src/i18n/index", ["require", "exports", "@angular/compiler/src/i18n/extractor", "@angular/compiler/src/i18n/i18n_html_parser", "@angular/compiler/src/i18n/message_bundle", "@angular/compiler/src/i18n/serializers/serializer", "@angular/compiler/src/i18n/serializers/xliff", "@angular/compiler/src/i18n/serializers/xliff2", "@angular/compiler/src/i18n/serializers/xmb", "@angular/compiler/src/i18n/serializers/xtb"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var extractor_1 = require("@angular/compiler/src/i18n/extractor");
    exports.Extractor = extractor_1.Extractor;
    var i18n_html_parser_1 = require("@angular/compiler/src/i18n/i18n_html_parser");
    exports.I18NHtmlParser = i18n_html_parser_1.I18NHtmlParser;
    var message_bundle_1 = require("@angular/compiler/src/i18n/message_bundle");
    exports.MessageBundle = message_bundle_1.MessageBundle;
    var serializer_1 = require("@angular/compiler/src/i18n/serializers/serializer");
    exports.Serializer = serializer_1.Serializer;
    var xliff_1 = require("@angular/compiler/src/i18n/serializers/xliff");
    exports.Xliff = xliff_1.Xliff;
    var xliff2_1 = require("@angular/compiler/src/i18n/serializers/xliff2");
    exports.Xliff2 = xliff2_1.Xliff2;
    var xmb_1 = require("@angular/compiler/src/i18n/serializers/xmb");
    exports.Xmb = xmb_1.Xmb;
    var xtb_1 = require("@angular/compiler/src/i18n/serializers/xtb");
    exports.Xtb = xtb_1.Xtb;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaTE4bi9pbmRleC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILGtFQUFxRDtJQUE3QyxnQ0FBQSxTQUFTLENBQUE7SUFDakIsZ0ZBQWtEO0lBQTFDLDRDQUFBLGNBQWMsQ0FBQTtJQUN0Qiw0RUFBK0M7SUFBdkMseUNBQUEsYUFBYSxDQUFBO0lBQ3JCLGdGQUFvRDtJQUE1QyxrQ0FBQSxVQUFVLENBQUE7SUFDbEIsc0VBQTBDO0lBQWxDLHdCQUFBLEtBQUssQ0FBQTtJQUNiLHdFQUE0QztJQUFwQywwQkFBQSxNQUFNLENBQUE7SUFDZCxrRUFBc0M7SUFBOUIsb0JBQUEsR0FBRyxDQUFBO0lBQ1gsa0VBQXNDO0lBQTlCLG9CQUFBLEdBQUcsQ0FBQSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuZXhwb3J0IHtFeHRyYWN0b3IsIEV4dHJhY3Rvckhvc3R9IGZyb20gJy4vZXh0cmFjdG9yJztcbmV4cG9ydCB7STE4Tkh0bWxQYXJzZXJ9IGZyb20gJy4vaTE4bl9odG1sX3BhcnNlcic7XG5leHBvcnQge01lc3NhZ2VCdW5kbGV9IGZyb20gJy4vbWVzc2FnZV9idW5kbGUnO1xuZXhwb3J0IHtTZXJpYWxpemVyfSBmcm9tICcuL3NlcmlhbGl6ZXJzL3NlcmlhbGl6ZXInO1xuZXhwb3J0IHtYbGlmZn0gZnJvbSAnLi9zZXJpYWxpemVycy94bGlmZic7XG5leHBvcnQge1hsaWZmMn0gZnJvbSAnLi9zZXJpYWxpemVycy94bGlmZjInO1xuZXhwb3J0IHtYbWJ9IGZyb20gJy4vc2VyaWFsaXplcnMveG1iJztcbmV4cG9ydCB7WHRifSBmcm9tICcuL3NlcmlhbGl6ZXJzL3h0Yic7XG4iXX0=