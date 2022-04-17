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
        define("@angular/compiler/src/i18n/translation_bundle", ["require", "exports", "tslib", "@angular/compiler/src/core", "@angular/compiler/src/ml_parser/html_parser", "@angular/compiler/src/i18n/parse_util", "@angular/compiler/src/i18n/serializers/xml_helper"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var core_1 = require("@angular/compiler/src/core");
    var html_parser_1 = require("@angular/compiler/src/ml_parser/html_parser");
    var parse_util_1 = require("@angular/compiler/src/i18n/parse_util");
    var xml_helper_1 = require("@angular/compiler/src/i18n/serializers/xml_helper");
    /**
     * A container for translated messages
     */
    var TranslationBundle = /** @class */ (function () {
        function TranslationBundle(_i18nNodesByMsgId, locale, digest, mapperFactory, missingTranslationStrategy, console) {
            if (_i18nNodesByMsgId === void 0) { _i18nNodesByMsgId = {}; }
            if (missingTranslationStrategy === void 0) { missingTranslationStrategy = core_1.MissingTranslationStrategy.Warning; }
            this._i18nNodesByMsgId = _i18nNodesByMsgId;
            this.digest = digest;
            this.mapperFactory = mapperFactory;
            this._i18nToHtml = new I18nToHtmlVisitor(_i18nNodesByMsgId, locale, digest, mapperFactory, missingTranslationStrategy, console);
        }
        // Creates a `TranslationBundle` by parsing the given `content` with the `serializer`.
        TranslationBundle.load = function (content, url, serializer, missingTranslationStrategy, console) {
            var _a = serializer.load(content, url), locale = _a.locale, i18nNodesByMsgId = _a.i18nNodesByMsgId;
            var digestFn = function (m) { return serializer.digest(m); };
            var mapperFactory = function (m) { return serializer.createNameMapper(m); };
            return new TranslationBundle(i18nNodesByMsgId, locale, digestFn, mapperFactory, missingTranslationStrategy, console);
        };
        // Returns the translation as HTML nodes from the given source message.
        TranslationBundle.prototype.get = function (srcMsg) {
            var html = this._i18nToHtml.convert(srcMsg);
            if (html.errors.length) {
                throw new Error(html.errors.join('\n'));
            }
            return html.nodes;
        };
        TranslationBundle.prototype.has = function (srcMsg) { return this.digest(srcMsg) in this._i18nNodesByMsgId; };
        return TranslationBundle;
    }());
    exports.TranslationBundle = TranslationBundle;
    var I18nToHtmlVisitor = /** @class */ (function () {
        function I18nToHtmlVisitor(_i18nNodesByMsgId, _locale, _digest, _mapperFactory, _missingTranslationStrategy, _console) {
            if (_i18nNodesByMsgId === void 0) { _i18nNodesByMsgId = {}; }
            this._i18nNodesByMsgId = _i18nNodesByMsgId;
            this._locale = _locale;
            this._digest = _digest;
            this._mapperFactory = _mapperFactory;
            this._missingTranslationStrategy = _missingTranslationStrategy;
            this._console = _console;
            this._contextStack = [];
            this._errors = [];
        }
        I18nToHtmlVisitor.prototype.convert = function (srcMsg) {
            this._contextStack.length = 0;
            this._errors.length = 0;
            // i18n to text
            var text = this._convertToText(srcMsg);
            // text to html
            var url = srcMsg.nodes[0].sourceSpan.start.file.url;
            var html = new html_parser_1.HtmlParser().parse(text, url, { tokenizeExpansionForms: true });
            return {
                nodes: html.rootNodes,
                errors: tslib_1.__spread(this._errors, html.errors),
            };
        };
        I18nToHtmlVisitor.prototype.visitText = function (text, context) {
            // `convert()` uses an `HtmlParser` to return `html.Node`s
            // we should then make sure that any special characters are escaped
            return xml_helper_1.escapeXml(text.value);
        };
        I18nToHtmlVisitor.prototype.visitContainer = function (container, context) {
            var _this = this;
            return container.children.map(function (n) { return n.visit(_this); }).join('');
        };
        I18nToHtmlVisitor.prototype.visitIcu = function (icu, context) {
            var _this = this;
            var cases = Object.keys(icu.cases).map(function (k) { return k + " {" + icu.cases[k].visit(_this) + "}"; });
            // TODO(vicb): Once all format switch to using expression placeholders
            // we should throw when the placeholder is not in the source message
            var exp = this._srcMsg.placeholders.hasOwnProperty(icu.expression) ?
                this._srcMsg.placeholders[icu.expression] :
                icu.expression;
            return "{" + exp + ", " + icu.type + ", " + cases.join(' ') + "}";
        };
        I18nToHtmlVisitor.prototype.visitPlaceholder = function (ph, context) {
            var phName = this._mapper(ph.name);
            if (this._srcMsg.placeholders.hasOwnProperty(phName)) {
                return this._srcMsg.placeholders[phName];
            }
            if (this._srcMsg.placeholderToMessage.hasOwnProperty(phName)) {
                return this._convertToText(this._srcMsg.placeholderToMessage[phName]);
            }
            this._addError(ph, "Unknown placeholder \"" + ph.name + "\"");
            return '';
        };
        // Loaded message contains only placeholders (vs tag and icu placeholders).
        // However when a translation can not be found, we need to serialize the source message
        // which can contain tag placeholders
        I18nToHtmlVisitor.prototype.visitTagPlaceholder = function (ph, context) {
            var _this = this;
            var tag = "" + ph.tag;
            var attrs = Object.keys(ph.attrs).map(function (name) { return name + "=\"" + ph.attrs[name] + "\""; }).join(' ');
            if (ph.isVoid) {
                return "<" + tag + " " + attrs + "/>";
            }
            var children = ph.children.map(function (c) { return c.visit(_this); }).join('');
            return "<" + tag + " " + attrs + ">" + children + "</" + tag + ">";
        };
        // Loaded message contains only placeholders (vs tag and icu placeholders).
        // However when a translation can not be found, we need to serialize the source message
        // which can contain tag placeholders
        I18nToHtmlVisitor.prototype.visitIcuPlaceholder = function (ph, context) {
            // An ICU placeholder references the source message to be serialized
            return this._convertToText(this._srcMsg.placeholderToMessage[ph.name]);
        };
        /**
         * Convert a source message to a translated text string:
         * - text nodes are replaced with their translation,
         * - placeholders are replaced with their content,
         * - ICU nodes are converted to ICU expressions.
         */
        I18nToHtmlVisitor.prototype._convertToText = function (srcMsg) {
            var _this = this;
            var id = this._digest(srcMsg);
            var mapper = this._mapperFactory ? this._mapperFactory(srcMsg) : null;
            var nodes;
            this._contextStack.push({ msg: this._srcMsg, mapper: this._mapper });
            this._srcMsg = srcMsg;
            if (this._i18nNodesByMsgId.hasOwnProperty(id)) {
                // When there is a translation use its nodes as the source
                // And create a mapper to convert serialized placeholder names to internal names
                nodes = this._i18nNodesByMsgId[id];
                this._mapper = function (name) { return mapper ? mapper.toInternalName(name) : name; };
            }
            else {
                // When no translation has been found
                // - report an error / a warning / nothing,
                // - use the nodes from the original message
                // - placeholders are already internal and need no mapper
                if (this._missingTranslationStrategy === core_1.MissingTranslationStrategy.Error) {
                    var ctx = this._locale ? " for locale \"" + this._locale + "\"" : '';
                    this._addError(srcMsg.nodes[0], "Missing translation for message \"" + id + "\"" + ctx);
                }
                else if (this._console &&
                    this._missingTranslationStrategy === core_1.MissingTranslationStrategy.Warning) {
                    var ctx = this._locale ? " for locale \"" + this._locale + "\"" : '';
                    this._console.warn("Missing translation for message \"" + id + "\"" + ctx);
                }
                nodes = srcMsg.nodes;
                this._mapper = function (name) { return name; };
            }
            var text = nodes.map(function (node) { return node.visit(_this); }).join('');
            var context = this._contextStack.pop();
            this._srcMsg = context.msg;
            this._mapper = context.mapper;
            return text;
        };
        I18nToHtmlVisitor.prototype._addError = function (el, msg) {
            this._errors.push(new parse_util_1.I18nError(el.sourceSpan, msg));
        };
        return I18nToHtmlVisitor;
    }());
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHJhbnNsYXRpb25fYnVuZGxlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2kxOG4vdHJhbnNsYXRpb25fYnVuZGxlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILG1EQUFtRDtJQUVuRCwyRUFBb0Q7SUFJcEQsb0VBQXVDO0lBRXZDLGdGQUFtRDtJQUduRDs7T0FFRztJQUNIO1FBR0UsMkJBQ1ksaUJBQXNELEVBQUUsTUFBbUIsRUFDNUUsTUFBbUMsRUFDbkMsYUFBc0QsRUFDN0QsMEJBQTJGLEVBQzNGLE9BQWlCO1lBSlQsa0NBQUEsRUFBQSxzQkFBc0Q7WUFHOUQsMkNBQUEsRUFBQSw2QkFBeUQsaUNBQTBCLENBQUMsT0FBTztZQUhuRixzQkFBaUIsR0FBakIsaUJBQWlCLENBQXFDO1lBQ3ZELFdBQU0sR0FBTixNQUFNLENBQTZCO1lBQ25DLGtCQUFhLEdBQWIsYUFBYSxDQUF5QztZQUcvRCxJQUFJLENBQUMsV0FBVyxHQUFHLElBQUksaUJBQWlCLENBQ3BDLGlCQUFpQixFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsYUFBZSxFQUFFLDBCQUEwQixFQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQy9GLENBQUM7UUFFRCxzRkFBc0Y7UUFDL0Usc0JBQUksR0FBWCxVQUNJLE9BQWUsRUFBRSxHQUFXLEVBQUUsVUFBc0IsRUFDcEQsMEJBQXNELEVBQ3RELE9BQWlCO1lBQ2IsSUFBQSxrQ0FBMEQsRUFBekQsa0JBQU0sRUFBRSxzQ0FBaUQsQ0FBQztZQUNqRSxJQUFNLFFBQVEsR0FBRyxVQUFDLENBQWUsSUFBSyxPQUFBLFVBQVUsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQXBCLENBQW9CLENBQUM7WUFDM0QsSUFBTSxhQUFhLEdBQUcsVUFBQyxDQUFlLElBQUssT0FBQSxVQUFVLENBQUMsZ0JBQWdCLENBQUMsQ0FB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Q0gsd0JBQUM7SUFBRCxDQUFDLEFBdklELElBdUlDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge01pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5fSBmcm9tICcuLi9jb3JlJztcbmltcG9ydCAqIGFzIGh0bWwgZnJvbSAnLi4vbWxfcGFyc2VyL2FzdCc7XG5pbXBvcnQge0h0bWxQYXJzZXJ9IGZyb20gJy4uL21sX3BhcnNlci9odG1sX3BhcnNlcic7XG5pbXBvcnQge0NvbnNvbGV9IGZyb20gJy4uL3V0aWwnO1xuXG5pbXBvcnQgKiBhcyBpMThuIGZyb20gJy4vaTE4bl9hc3QnO1xuaW1wb3J0IHtJMThuRXJyb3J9IGZyb20gJy4vcGFyc2VfdXRpbCc7XG5pbXBvcnQge1BsYWNlaG9sZGVyTWFwcGVyLCBTZXJpYWxpemVyfSBmcm9tICcuL3NlcmlhbGl6ZXJzL3NlcmlhbGl6ZXInO1xuaW1wb3J0IHtlc2NhcGVYbWx9IGZyb20gJy4vc2VyaWFsaXplcnMveG1sX2hlbHBlcic7XG5cblxuLyoqXG4gKiBBIGNvbnRhaW5lciBmb3IgdHJhbnNsYXRlZCBtZXNzYWdlc1xuICovXG5leHBvcnQgY2xhc3MgVHJhbnNsYXRpb25CdW5kbGUge1xuICBwcml2YXRlIF9pMThuVG9IdG1sOiBJMThuVG9IdG1sVmlzaXRvcjtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgX2kxOG5Ob2Rlc0J5TXNnSWQ6IHtbbXNnSWQ6IHN0cmluZ106IGkxOG4uTm9kZVtdfSA9IHt9LCBsb2NhbGU6IHN0cmluZ3xudWxsLFxuICAgICAgcHVibGljIGRpZ2VzdDogKG06IGkxOG4uTWVzc2FnZSkgPT4gc3RyaW5nLFxuICAgICAgcHVibGljIG1hcHBlckZhY3Rvcnk/OiAobTogaTE4bi5NZXNzYWdlKSA9PiBQbGFjZWhvbGRlck1hcHBlcixcbiAgICAgIG1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5OiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSA9IE1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5Lldhcm5pbmcsXG4gICAgICBjb25zb2xlPzogQ29uc29sZSkge1xuICAgIHRoaXMuX2kxOG5Ub0h0bWwgPSBuZXcgSTE4blRvSHRtbFZpc2l0b3IoXG4gICAgICAgIF9pMThuTm9kZXNCeU1zZ0lkLCBsb2NhbGUsIGRpZ2VzdCwgbWFwcGVyRmFjdG9yeSAhLCBtaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSwgY29uc29sZSk7XG4gIH1cblxuICAvLyBDcmVhdGVzIGEgYFRyYW5zbGF0aW9uQnVuZGxlYCBieSBwYXJzaW5nIHRoZSBnaXZlbiBgY29udGVudGAgd2l0aCB0aGUgYHNlcmlhbGl6ZXJgLlxuICBzdGF0aWMgbG9hZChcbiAgICAgIGNvbnRlbnQ6IHN0cmluZywgdXJsOiBzdHJpbmcsIHNlcmlhbGl6ZXI6IFNlcmlhbGl6ZXIsXG4gICAgICBtaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneTogTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3ksXG4gICAgICBjb25zb2xlPzogQ29uc29sZSk6IFRyYW5zbGF0aW9uQnVuZGxlIHtcbiAgICBjb25zdCB7bG9jYWxlLCBpMThuTm9kZXNCeU1zZ0lkfSA9IHNlcmlhbGl6ZXIubG9hZChjb250ZW50LCB1cmwpO1xuICAgIGNvbnN0IGRpZ2VzdEZuID0gKG06IGkxOG4uTWVzc2FnZSkgPT4gc2VyaWFsaXplci5kaWdlc3QobSk7XG4gICAgY29uc3QgbWFwcGVyRmFjdG9yeSA9IChtOiBpMThuLk1lc3NhZ2UpID0+IHNlcmlhbGl6ZXIuY3JlYXRlTmFtZU1hcHBlcihtKSAhO1xuICAgIHJldHVybiBuZXcgVHJhbnNsYXRpb25CdW5kbGUoXG4gICAgICAgIGkxOG5Ob2Rlc0J5TXNnSWQsIGxvY2FsZSwgZGlnZXN0Rm4sIG1hcHBlckZhY3RvcnksIG1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5LCBjb25zb2xlKTtcbiAgfVxuXG4gIC8vIFJldHVybnMgdGhlIHRyYW5zbGF0aW9uIGFzIEhUTUwgbm9kZXMgZnJvbSB0aGUgZ2l2ZW4gc291cmNlIG1lc3NhZ2UuXG4gIGdldChzcmNNc2c6IGkxOG4uTWVzc2FnZSk6IGh0bWwuTm9kZVtdIHtcbiAgICBjb25zdCBodG1sID0gdGhpcy5faTE4blRvSHRtbC5jb252ZXJ0KHNyY01zZyk7XG5cbiAgICBpZiAoaHRtbC5lcnJvcnMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoaHRtbC5lcnJvcnMuam9pbignXFxuJykpO1xuICAgIH1cblxuICAgIHJldHVybiBodG1sLm5vZGVzO1xuICB9XG5cbiAgaGFzKHNyY01zZzogaTE4bi5NZXNzYWdlKTogYm9vbGVhbiB7IHJldHVybiB0aGlzLmRpZ2VzdChzcmNNc2cpIGluIHRoaXMuX2kxOG5Ob2Rlc0J5TXNnSWQ7IH1cbn1cblxuY2xhc3MgSTE4blRvSHRtbFZpc2l0b3IgaW1wbGVtZW50cyBpMThuLlZpc2l0b3Ige1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfc3JjTXNnICE6IGkxOG4uTWVzc2FnZTtcbiAgcHJpdmF0ZSBfY29udGV4dFN0YWNrOiB7bXNnOiBpMThuLk1lc3NhZ2UsIG1hcHBlcjogKG5hbWU6IHN0cmluZykgPT4gc3RyaW5nfVtdID0gW107XG4gIHByaXZhdGUgX2Vycm9yczogSTE4bkVycm9yW10gPSBbXTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX21hcHBlciAhOiAobmFtZTogc3RyaW5nKSA9PiBzdHJpbmc7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9pMThuTm9kZXNCeU1zZ0lkOiB7W21zZ0lkOiBzdHJpbmddOiBpMThuLk5vZGVbXX0gPSB7fSwgcHJpdmF0ZSBfbG9jYWxlOiBzdHJpbmd8bnVsbCxcbiAgICAgIHByaXZhdGUgX2RpZ2VzdDogKG06IGkxOG4uTWVzc2FnZSkgPT4gc3RyaW5nLFxuICAgICAgcHJpdmF0ZSBfbWFwcGVyRmFjdG9yeTogKG06IGkxOG4uTWVzc2FnZSkgPT4gUGxhY2Vob2xkZXJNYXBwZXIsXG4gICAgICBwcml2YXRlIF9taXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneTogTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3ksIHByaXZhdGUgX2NvbnNvbGU/OiBDb25zb2xlKSB7XG4gIH1cblxuICBjb252ZXJ0KHNyY01zZzogaTE4bi5NZXNzYWdlKToge25vZGVzOiBodG1sLk5vZGVbXSwgZXJyb3JzOiBJMThuRXJyb3JbXX0ge1xuICAgIHRoaXMuX2NvbnRleHRTdGFjay5sZW5ndGggPSAwO1xuICAgIHRoaXMuX2Vycm9ycy5sZW5ndGggPSAwO1xuXG4gICAgLy8gaTE4biB0byB0ZXh0XG4gICAgY29uc3QgdGV4dCA9IHRoaXMuX2NvbnZlcnRUb1RleHQoc3JjTXNnKTtcblxuICAgIC8vIHRleHQgdG8gaHRtbFxuICAgIGNvbnN0IHVybCA9IHNyY01zZy5ub2Rlc1swXS5zb3VyY2VTcGFuLnN0YXJ0LmZpbGUudXJsO1xuICAgIGNvbnN0IGh0bWwgPSBuZXcgSHRtbFBhcnNlcigpLnBhcnNlKHRleHQsIHVybCwge3Rva2VuaXplRXhwYW5zaW9uRm9ybXM6IHRydWV9KTtcblxuICAgIHJldHVybiB7XG4gICAgICBub2RlczogaHRtbC5yb290Tm9kZXMsXG4gICAgICBlcnJvcnM6IFsuLi50aGlzLl9lcnJvcnMsIC4uLmh0bWwuZXJyb3JzXSxcbiAgICB9O1xuICB9XG5cbiAgdmlzaXRUZXh0KHRleHQ6IGkxOG4uVGV4dCwgY29udGV4dD86IGFueSk6IHN0cmluZyB7XG4gICAgLy8gYGNvbnZlcnQoKWAgdXNlcyBhbiBgSHRtbFBhcnNlcmAgdG8gcmV0dXJuIGBodG1sLk5vZGVgc1xuICAgIC8vIHdlIHNob3VsZCB0aGVuIG1ha2Ugc3VyZSB0aGF0IGFueSBzcGVjaWFsIGNoYXJhY3RlcnMgYXJlIGVzY2FwZWRcbiAgICByZXR1cm4gZXNjYXBlWG1sKHRleHQudmFsdWUpO1xuICB9XG5cbiAgdmlzaXRDb250YWluZXIoY29udGFpbmVyOiBpMThuLkNvbnRhaW5lciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIGNvbnRhaW5lci5jaGlsZHJlbi5tYXAobiA9PiBuLnZpc2l0KHRoaXMpKS5qb2luKCcnKTtcbiAgfVxuXG4gIHZpc2l0SWN1KGljdTogaTE4bi5JY3UsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIGNvbnN0IGNhc2VzID0gT2JqZWN0LmtleXMoaWN1LmNhc2VzKS5tYXAoayA9PiBgJHtrfSB7JHtpY3UuY2FzZXNba10udmlzaXQodGhpcyl9fWApO1xuXG4gICAgLy8gVE9ETyh2aWNiKTogT25jZSBhbGwgZm9ybWF0IHN3aXRjaCB0byB1c2luZyBleHByZXNzaW9uIHBsYWNlaG9sZGVyc1xuICAgIC8vIHdlIHNob3VsZCB0aHJvdyB3aGVuIHRoZSBwbGFjZWhvbGRlciBpcyBub3QgaW4gdGhlIHNvdXJjZSBtZXNzYWdlXG4gICAgY29uc3QgZXhwID0gdGhpcy5fc3JjTXNnLnBsYWNlaG9sZGVycy5oYXNPd25Qcm9wZXJ0eShpY3UuZXhwcmVzc2lvbikgP1xuICAgICAgICB0aGlzLl9zcmNNc2cucGxhY2Vob2xkZXJzW2ljdS5leHByZXNzaW9uXSA6XG4gICAgICAgIGljdS5leHByZXNzaW9uO1xuXG4gICAgcmV0dXJuIGB7JHtleHB9LCAke2ljdS50eXBlfSwgJHtjYXNlcy5qb2luKCcgJyl9fWA7XG4gIH1cblxuICB2aXNpdFBsYWNlaG9sZGVyKHBoOiBpMThuLlBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogc3RyaW5nIHtcbiAgICBjb25zdCBwaE5hbWUgPSB0aGlzLl9tYXBwZXIocGgubmFtZSk7XG4gICAgaWYgKHRoaXMuX3NyY01zZy5wbGFjZWhvbGRlcnMuaGFzT3duUHJvcGVydHkocGhOYW1lKSkge1xuICAgICAgcmV0dXJuIHRoaXMuX3NyY01zZy5wbGFjZWhvbGRlcnNbcGhOYW1lXTtcbiAgICB9XG5cbiAgICBpZiAodGhpcy5fc3JjTXNnLnBsYWNlaG9sZGVyVG9NZXNzYWdlLmhhc093blByb3BlcnR5KHBoTmFtZSkpIHtcbiAgICAgIHJldHVybiB0aGlzLl9jb252ZXJ0VG9UZXh0KHRoaXMuX3NyY01zZy5wbGFjZWhvbGRlclRvTWVzc2FnZVtwaE5hbWVdKTtcbiAgICB9XG5cbiAgICB0aGlzLl9hZGRFcnJvcihwaCwgYFVua25vd24gcGxhY2Vob2xkZXIgXCIke3BoLm5hbWV9XCJgKTtcbiAgICByZXR1cm4gJyc7XG4gIH1cblxuICAvLyBMb2FkZWQgbWVzc2FnZSBjb250YWlucyBvbmx5IHBsYWNlaG9sZGVycyAodnMgdGFnIGFuZCBpY3UgcGxhY2Vob2xkZXJzKS5cbiAgLy8gSG93ZXZlciB3aGVuIGEgdHJhbnNsYXRpb24gY2FuIG5vdCBiZSBmb3VuZCwgd2UgbmVlZCB0byBzZXJpYWxpemUgdGhlIHNvdXJjZSBtZXNzYWdlXG4gIC8vIHdoaWNoIGNhbiBjb250YWluIHRhZyBwbGFjZWhvbGRlcnNcbiAgdmlzaXRUYWdQbGFjZWhvbGRlcihwaDogaTE4bi5UYWdQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IHN0cmluZyB7XG4gICAgY29uc3QgdGFnID0gYCR7cGgudGFnfWA7XG4gICAgY29uc3QgYXR0cnMgPSBPYmplY3Qua2V5cyhwaC5hdHRycykubWFwKG5hbWUgPT4gYCR7bmFtZX09XCIke3BoLmF0dHJzW25hbWVdfVwiYCkuam9pbignICcpO1xuICAgIGlmIChwaC5pc1ZvaWQpIHtcbiAgICAgIHJldHVybiBgPCR7dGFnfSAke2F0dHJzfS8+YDtcbiAgICB9XG4gICAgY29uc3QgY2hpbGRyZW4gPSBwaC5jaGlsZHJlbi5tYXAoKGM6IGkxOG4uTm9kZSkgPT4gYy52aXNpdCh0aGlzKSkuam9pbignJyk7XG4gICAgcmV0dXJuIGA8JHt0YWd9ICR7YXR0cnN9PiR7Y2hpbGRyZW59PC8ke3RhZ30+YDtcbiAgfVxuXG4gIC8vIExvYWRlZCBtZXNzYWdlIGNvbnRhaW5zIG9ubHkgcGxhY2Vob2xkZXJzICh2cyB0YWcgYW5kIGljdSBwbGFjZWhvbGRlcnMpLlxuICAvLyBIb3dldmVyIHdoZW4gYSB0cmFuc2xhdGlvbiBjYW4gbm90IGJlIGZvdW5kLCB3ZSBuZWVkIHRvIHNlcmlhbGl6ZSB0aGUgc291cmNlIG1lc3NhZ2VcbiAgLy8gd2hpY2ggY2FuIGNvbnRhaW4gdGFnIHBsYWNlaG9sZGVyc1xuICB2aXNpdEljdVBsYWNlaG9sZGVyKHBoOiBpMThuLkljdVBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogc3RyaW5nIHtcbiAgICAvLyBBbiBJQ1UgcGxhY2Vob2xkZXIgcmVmZXJlbmNlcyB0aGUgc291cmNlIG1lc3NhZ2UgdG8gYmUgc2VyaWFsaXplZFxuICAgIHJldHVybiB0aGlzLl9jb252ZXJ0VG9UZXh0KHRoaXMuX3NyY01zZy5wbGFjZWhvbGRlclRvTWVzc2FnZVtwaC5uYW1lXSk7XG4gIH1cblxuICAvKipcbiAgICogQ29udmVydCBhIHNvdXJjZSBtZXNzYWdlIHRvIGEgdHJhbnNsYXRlZCB0ZXh0IHN0cmluZzpcbiAgICogLSB0ZXh0IG5vZGVzIGFyZSByZXBsYWNlZCB3aXRoIHRoZWlyIHRyYW5zbGF0aW9uLFxuICAgKiAtIHBsYWNlaG9sZGVycyBhcmUgcmVwbGFjZWQgd2l0aCB0aGVpciBjb250ZW50LFxuICAgKiAtIElDVSBub2RlcyBhcmUgY29udmVydGVkIHRvIElDVSBleHByZXNzaW9ucy5cbiAgICovXG4gIHByaXZhdGUgX2NvbnZlcnRUb1RleHQoc3JjTXNnOiBpMThuLk1lc3NhZ2UpOiBzdHJpbmcge1xuICAgIGNvbnN0IGlkID0gdGhpcy5fZGlnZXN0KHNyY01zZyk7XG4gICAgY29uc3QgbWFwcGVyID0gdGhpcy5fbWFwcGVyRmFjdG9yeSA/IHRoaXMuX21hcHBlckZhY3Rvcnkoc3JjTXNnKSA6IG51bGw7XG4gICAgbGV0IG5vZGVzOiBpMThuLk5vZGVbXTtcblxuICAgIHRoaXMuX2NvbnRleHRTdGFjay5wdXNoKHttc2c6IHRoaXMuX3NyY01zZywgbWFwcGVyOiB0aGlzLl9tYXBwZXJ9KTtcbiAgICB0aGlzLl9zcmNNc2cgPSBzcmNNc2c7XG5cbiAgICBpZiAodGhpcy5faTE4bk5vZGVzQnlNc2dJZC5oYXNPd25Qcm9wZXJ0eShpZCkpIHtcbiAgICAgIC8vIFdoZW4gdGhlcmUgaXMgYSB0cmFuc2xhdGlvbiB1c2UgaXRzIG5vZGVzIGFzIHRoZSBzb3VyY2VcbiAgICAgIC8vIEFuZCBjcmVhdGUgYSBtYXBwZXIgdG8gY29udmVydCBzZXJpYWxpemVkIHBsYWNlaG9sZGVyIG5hbWVzIHRvIGludGVybmFsIG5hbWVzXG4gICAgICBub2RlcyA9IHRoaXMuX2kxOG5Ob2Rlc0J5TXNnSWRbaWRdO1xuICAgICAgdGhpcy5fbWFwcGVyID0gKG5hbWU6IHN0cmluZykgPT4gbWFwcGVyID8gbWFwcGVyLnRvSW50ZXJuYWxOYW1lKG5hbWUpICEgOiBuYW1lO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBXaGVuIG5vIHRyYW5zbGF0aW9uIGhhcyBiZWVuIGZvdW5kXG4gICAgICAvLyAtIHJlcG9ydCBhbiBlcnJvciAvIGEgd2FybmluZyAvIG5vdGhpbmcsXG4gICAgICAvLyAtIHVzZSB0aGUgbm9kZXMgZnJvbSB0aGUgb3JpZ2luYWwgbWVzc2FnZVxuICAgICAgLy8gLSBwbGFjZWhvbGRlcnMgYXJlIGFscmVhZHkgaW50ZXJuYWwgYW5kIG5lZWQgbm8gbWFwcGVyXG4gICAgICBpZiAodGhpcy5fbWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3kgPT09IE1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5LkVycm9yKSB7XG4gICAgICAgIGNvbnN0IGN0eCA9IHRoaXMuX2xvY2FsZSA/IGAgZm9yIGxvY2FsZSBcIiR7dGhpcy5fbG9jYWxlfVwiYCA6ICcnO1xuICAgICAgICB0aGlzLl9hZGRFcnJvcihzcmNNc2cubm9kZXNbMF0sIGBNaXNzaW5nIHRyYW5zbGF0aW9uIGZvciBtZXNzYWdlIFwiJHtpZH1cIiR7Y3R4fWApO1xuICAgICAgfSBlbHNlIGlmIChcbiAgICAgICAgICB0aGlzLl9jb25zb2xlICYmXG4gICAgICAgICAgdGhpcy5fbWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3kgPT09IE1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5Lldhcm5pbmcpIHtcbiAgICAgICAgY29uc3QgY3R4ID0gdGhpcy5fbG9jYWxlID8gYCBmb3IgbG9jYWxlIFwiJHt0aGlzLl9sb2NhbGV9XCJgIDogJyc7XG4gICAgICAgIHRoaXMuX2NvbnNvbGUud2FybihgTWlzc2luZyB0cmFuc2xhdGlvbiBmb3IgbWVzc2FnZSBcIiR7aWR9XCIke2N0eH1gKTtcbiAgICAgIH1cbiAgICAgIG5vZGVzID0gc3JjTXNnLm5vZGVzO1xuICAgICAgdGhpcy5fbWFwcGVyID0gKG5hbWU6IHN0cmluZykgPT4gbmFtZTtcbiAgICB9XG4gICAgY29uc3QgdGV4dCA9IG5vZGVzLm1hcChub2RlID0+IG5vZGUudmlzaXQodGhpcykpLmpvaW4oJycpO1xuICAgIGNvbnN0IGNvbnRleHQgPSB0aGlzLl9jb250ZXh0U3RhY2sucG9wKCkgITtcbiAgICB0aGlzLl9zcmNNc2cgPSBjb250ZXh0Lm1zZztcbiAgICB0aGlzLl9tYXBwZXIgPSBjb250ZXh0Lm1hcHBlcjtcbiAgICByZXR1cm4gdGV4dDtcbiAgfVxuXG4gIHByaXZhdGUgX2FkZEVycm9yKGVsOiBpMThuLk5vZGUsIG1zZzogc3RyaW5nKSB7XG4gICAgdGhpcy5fZXJyb3JzLnB1c2gobmV3IEkxOG5FcnJvcihlbC5zb3VyY2VTcGFuLCBtc2cpKTtcbiAgfVxufVxuIl19