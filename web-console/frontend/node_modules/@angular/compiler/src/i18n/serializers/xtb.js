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
        define("@angular/compiler/src/i18n/serializers/xtb", ["require", "exports", "tslib", "@angular/compiler/src/ml_parser/ast", "@angular/compiler/src/ml_parser/xml_parser", "@angular/compiler/src/i18n/i18n_ast", "@angular/compiler/src/i18n/parse_util", "@angular/compiler/src/i18n/serializers/serializer", "@angular/compiler/src/i18n/serializers/xmb"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var ml = require("@angular/compiler/src/ml_parser/ast");
    var xml_parser_1 = require("@angular/compiler/src/ml_parser/xml_parser");
    var i18n = require("@angular/compiler/src/i18n/i18n_ast");
    var parse_util_1 = require("@angular/compiler/src/i18n/parse_util");
    var serializer_1 = require("@angular/compiler/src/i18n/serializers/serializer");
    var xmb_1 = require("@angular/compiler/src/i18n/serializers/xmb");
    var _TRANSLATIONS_TAG = 'translationbundle';
    var _TRANSLATION_TAG = 'translation';
    var _PLACEHOLDER_TAG = 'ph';
    var Xtb = /** @class */ (function (_super) {
        tslib_1.__extends(Xtb, _super);
        function Xtb() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        Xtb.prototype.write = function (messages, locale) { throw new Error('Unsupported'); };
        Xtb.prototype.load = function (content, url) {
            // xtb to xml nodes
            var xtbParser = new XtbParser();
            var _a = xtbParser.parse(content, url), locale = _a.locale, msgIdToHtml = _a.msgIdToHtml, errors = _a.errors;
            // xml nodes to i18n nodes
            var i18nNodesByMsgId = {};
            var converter = new XmlToI18n();
            // Because we should be able to load xtb files that rely on features not supported by angular,
            // we need to delay the conversion of html to i18n nodes so that non angular messages are not
            // converted
            Object.keys(msgIdToHtml).forEach(function (msgId) {
                var valueFn = function () {
                    var _a = converter.convert(msgIdToHtml[msgId], url), i18nNodes = _a.i18nNodes, errors = _a.errors;
                    if (errors.length) {
                        throw new Error("xtb parse errors:\n" + errors.join('\n'));
                    }
                    return i18nNodes;
                };
                createLazyProperty(i18nNodesByMsgId, msgId, valueFn);
            });
            if (errors.length) {
                throw new Error("xtb parse errors:\n" + errors.join('\n'));
            }
            return { locale: locale, i18nNodesByMsgId: i18nNodesByMsgId };
        };
        Xtb.prototype.digest = function (message) { return xmb_1.digest(message); };
        Xtb.prototype.createNameMapper = function (message) {
            return new serializer_1.SimplePlaceholderMapper(message, xmb_1.toPublicName);
        };
        return Xtb;
    }(serializer_1.Serializer));
    exports.Xtb = Xtb;
    function createLazyProperty(messages, id, valueFn) {
        Object.defineProperty(messages, id, {
            configurable: true,
            enumerable: true,
            get: function () {
                var value = valueFn();
                Object.defineProperty(messages, id, { enumerable: true, value: value });
                return value;
            },
            set: function (_) { throw new Error('Could not overwrite an XTB translation'); },
        });
    }
    // Extract messages as xml nodes from the xtb file
    var XtbParser = /** @class */ (function () {
        function XtbParser() {
            this._locale = null;
        }
        XtbParser.prototype.parse = function (xtb, url) {
            this._bundleDepth = 0;
            this._msgIdToHtml = {};
            // We can not parse the ICU messages at this point as some messages might not originate
            // from Angular that could not be lex'd.
            var xml = new xml_parser_1.XmlParser().parse(xtb, url);
            this._errors = xml.errors;
            ml.visitAll(this, xml.rootNodes);
            return {
                msgIdToHtml: this._msgIdToHtml,
                errors: this._errors,
                locale: this._locale,
            };
        };
        XtbParser.prototype.visitElement = function (element, context) {
            switch (element.name) {
                case _TRANSLATIONS_TAG:
                    this._bundleDepth++;
                    if (this._bundleDepth > 1) {
                        this._addError(element, "<" + _TRANSLATIONS_TAG + "> elements can not be nested");
                    }
                    var langAttr = element.attrs.find(function (attr) { return attr.name === 'lang'; });
                    if (langAttr) {
                        this._locale = langAttr.value;
                    }
                    ml.visitAll(this, element.children, null);
                    this._bundleDepth--;
                    break;
                case _TRANSLATION_TAG:
                    var idAttr = element.attrs.find(function (attr) { return attr.name === 'id'; });
                    if (!idAttr) {
                        this._addError(element, "<" + _TRANSLATION_TAG + "> misses the \"id\" attribute");
                    }
                    else {
                        var id = idAttr.value;
                        if (this._msgIdToHtml.hasOwnProperty(id)) {
                            this._addError(element, "Duplicated translations for msg " + id);
                        }
                        else {
                            var innerTextStart = element.startSourceSpan.end.offset;
                            var innerTextEnd = element.endSourceSpan.start.offset;
                            var content = element.startSourceSpan.start.file.content;
                            var innerText = content.slice(innerTextStart, innerTextEnd);
                            this._msgIdToHtml[id] = innerText;
                        }
                    }
                    break;
                default:
                    this._addError(element, 'Unexpected tag');
            }
        };
        XtbParser.prototype.visitAttribute = function (attribute, context) { };
        XtbParser.prototype.visitText = function (text, context) { };
        XtbParser.prototype.visitComment = function (comment, context) { };
        XtbParser.prototype.visitExpansion = function (expansion, context) { };
        XtbParser.prototype.visitExpansionCase = function (expansionCase, context) { };
        XtbParser.prototype._addError = function (node, message) {
            this._errors.push(new parse_util_1.I18nError(node.sourceSpan, message));
        };
        return XtbParser;
    }());
    // Convert ml nodes (xtb syntax) to i18n nodes
    var XmlToI18n = /** @class */ (function () {
        function XmlToI18n() {
        }
        XmlToI18n.prototype.convert = function (message, url) {
            var xmlIcu = new xml_parser_1.XmlParser().parse(message, url, { tokenizeExpansionForms: true });
            this._errors = xmlIcu.errors;
            var i18nNodes = this._errors.length > 0 || xmlIcu.rootNodes.length == 0 ?
                [] :
                ml.visitAll(this, xmlIcu.rootNodes);
            return {
                i18nNodes: i18nNodes,
                errors: this._errors,
            };
        };
        XmlToI18n.prototype.visitText = function (text, context) { return new i18n.Text(text.value, text.sourceSpan); };
        XmlToI18n.prototype.visitExpansion = function (icu, context) {
            var caseMap = {};
            ml.visitAll(this, icu.cases).forEach(function (c) {
                caseMap[c.value] = new i18n.Container(c.nodes, icu.sourceSpan);
            });
            return new i18n.Icu(icu.switchValue, icu.type, caseMap, icu.sourceSpan);
        };
        XmlToI18n.prototype.visitExpansionCase = function (icuCase, context) {
            return {
                value: icuCase.value,
                nodes: ml.visitAll(this, icuCase.expression),
            };
        };
        XmlToI18n.prototype.visitElement = function (el, context) {
            if (el.name === _PLACEHOLDER_TAG) {
                var nameAttr = el.attrs.find(function (attr) { return attr.name === 'name'; });
                if (nameAttr) {
                    return new i18n.Placeholder('', nameAttr.value, el.sourceSpan);
                }
                this._addError(el, "<" + _PLACEHOLDER_TAG + "> misses the \"name\" attribute");
            }
            else {
                this._addError(el, "Unexpected tag");
            }
            return null;
        };
        XmlToI18n.prototype.visitComment = function (comment, context) { };
        XmlToI18n.prototype.visitAttribute = function (attribute, context) { };
        XmlToI18n.prototype._addError = function (node, message) {
            this._errors.push(new parse_util_1.I18nError(node.sourceSpan, message));
        };
        return XmlToI18n;
    }());
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieHRiLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2kxOG4vc2VyaWFsaXplcnMveHRiLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILHdEQUEwQztJQUMxQyx5RUFBcUQ7SUFDckQsMERBQW9DO0lBQ3BDLG9FQUF3QztJQUV4QyxnRkFBb0Y7SUFDcEYsa0VBQTJDO0lBRTNDLElBQU0saUJBQWlCLEdBQUcsbUJBQW1CLENBQUM7SUFDOUMsSUFBTSxnQkFBZ0IsR0FBRyxhQUFhLENBQUM7SUFDdkMsSUFBTSxnQkFBZ0IsR0FBRyxJQUFJLENBQUM7SUFFOUI7UUFBeUIsK0JBQVU7UUFBbkM7O1FBdUNBLENBQUM7UUF0Q0MsbUJBQUssR0FBTCxVQUFNLFFBQXdCLEVBQUUsTUFBbUIsSUFBWSxNQUFNLElBQUksS0FBSyxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUVoRyxrQkFBSSxHQUFKLFVBQUssT0FBZSxFQUFFLEdBQVc7WUFFL0IsbUJBQW1CO1lBQ25CLElBQU0sU0FBUyxHQUFHLElBQUksU0FBUyxFQUFFLENBQUM7WUFDNUIsSUFBQSxrQ0FBNkQsRUFBNUQsa0JBQU0sRUFBRSw0QkFBVyxFQUFFLGtCQUF1QyxDQUFDO1lBRXBFLDBCQUEwQjtZQUMxQixJQUFNLGdCQUFnQixHQUFtQyxFQUFFLENBQUM7WUFDNUQsSUFBTSxTQUFTLEdBQUcsSUFBSSxTQUFTLEVBQUUsQ0FBQztZQUVsQyw4RkFBOEY7WUFDOUYsNkZBQTZGO1lBQzdGLFlBQVk7WUFDWixNQUFNLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxVQUFBLEtBQUs7Z0JBQ3BDLElBQU0sT0FBTyxHQUFHO29CQUNSLElBQUEsK0NBQWdFLEVBQS9ELHdCQUFTLEVBQUUsa0JBQW9ELENBQUM7b0JBQ3ZFLElBQUksTUFB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Q0FBQzthQUM3QyxDQUFDO1FBQ0osQ0FBQztRQUVELGdDQUFZLEdBQVosVUFBYSxFQUFjLEVBQUUsT0FBWTtZQUN2QyxJQUFJLEVBQUUsQ0FBQyxJQUFJLEtBQUssZ0JBQWdCLEVBQUU7Z0JBQ2hDLElBQU0sUUFBUSxHQUFHLEVBQUUsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLFVBQUMsSUFBSSxJQUFLLE9BQUEsSUFBSSxDQUFDLElBQUksS0FBSyxNQUFNLEVBQXBCLENBQW9CLENBQUMsQ0FBQztnQkFDL0QsSUFBSSxRQUFRLEVBQUU7b0JBQ1osT0FBTyxJQUFJLElBQUksQ0FBQyxXQUFXLENBQUMsRUFBRSxFQUFFLFFBQVEsQ0FBQyxLQUFLLEVBQUUsRUFBRSxDQUFDLFVBQVksQ0FBQyxDQUFDO2lCQUNsRTtnQkFFRCxJQUFJLENBQUMsU0FBUyxDQUFDLEVBQUUsRUFBRSxNQUFJLGdCQUFnQixvQ0FBK0IsQ0FBQyxDQUFDO2FBQ3pFO2lCQUFNO2dCQUNMLElBQUksQ0FBQyxTQUFTLENBQUMsRUFBRSxFQUFFLGdCQUFnQixDQUFDLENBQUM7YUFDdEM7WUFDRCxPQUFPLElBQUksQ0FBQztRQUNkLENBQUM7UUFFRCxnQ0FBWSxHQUFaLFVBQWEsT0FBbUIsRUFBRSxPQUFZLElBQUcsQ0FBQztRQUVsRCxrQ0FBYyxHQUFkLFVBQWUsU0FBdUIsRUFBRSxPQUFZLElBQUcsQ0FBQztRQUVoRCw2QkFBUyxHQUFqQixVQUFrQixJQUFhLEVBQUUsT0FBZTtZQUM5QyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxJQUFJLHNCQUFTLENBQUMsSUFBSSxDQUFDLFVBQVksRUFBRSxPQUFPLENBQUMsQ0FBQyxDQUFDO1FBQy9ELENBQUM7UUFDSCxnQkFBQztJQUFELENBQUMsQUExREQsSUEwREMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAqIGFzIG1sIGZyb20gJy4uLy4uL21sX3BhcnNlci9hc3QnO1xuaW1wb3J0IHtYbWxQYXJzZXJ9IGZyb20gJy4uLy4uL21sX3BhcnNlci94bWxfcGFyc2VyJztcbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi4vaTE4bl9hc3QnO1xuaW1wb3J0IHtJMThuRXJyb3J9IGZyb20gJy4uL3BhcnNlX3V0aWwnO1xuXG5pbXBvcnQge1BsYWNlaG9sZGVyTWFwcGVyLCBTZXJpYWxpemVyLCBTaW1wbGVQbGFjZWhvbGRlck1hcHBlcn0gZnJvbSAnLi9zZXJpYWxpemVyJztcbmltcG9ydCB7ZGlnZXN0LCB0b1B1YmxpY05hbWV9IGZyb20gJy4veG1iJztcblxuY29uc3QgX1RSQU5TTEFUSU9OU19UQUcgPSAndHJhbnNsYXRpb25idW5kbGUnO1xuY29uc3QgX1RSQU5TTEFUSU9OX1RBRyA9ICd0cmFuc2xhdGlvbic7XG5jb25zdCBfUExBQ0VIT0xERVJfVEFHID0gJ3BoJztcblxuZXhwb3J0IGNsYXNzIFh0YiBleHRlbmRzIFNlcmlhbGl6ZXIge1xuICB3cml0ZShtZXNzYWdlczogaTE4bi5NZXNzYWdlW10sIGxvY2FsZTogc3RyaW5nfG51bGwpOiBzdHJpbmcgeyB0aHJvdyBuZXcgRXJyb3IoJ1Vuc3VwcG9ydGVkJyk7IH1cblxuICBsb2FkKGNvbnRlbnQ6IHN0cmluZywgdXJsOiBzdHJpbmcpOlxuICAgICAge2xvY2FsZTogc3RyaW5nLCBpMThuTm9kZXNCeU1zZ0lkOiB7W21zZ0lkOiBzdHJpbmddOiBpMThuLk5vZGVbXX19IHtcbiAgICAvLyB4dGIgdG8geG1sIG5vZGVzXG4gICAgY29uc3QgeHRiUGFyc2VyID0gbmV3IFh0YlBhcnNlcigpO1xuICAgIGNvbnN0IHtsb2NhbGUsIG1zZ0lkVG9IdG1sLCBlcnJvcnN9ID0geHRiUGFyc2VyLnBhcnNlKGNvbnRlbnQsIHVybCk7XG5cbiAgICAvLyB4bWwgbm9kZXMgdG8gaTE4biBub2Rlc1xuICAgIGNvbnN0IGkxOG5Ob2Rlc0J5TXNnSWQ6IHtbbXNnSWQ6IHN0cmluZ106IGkxOG4uTm9kZVtdfSA9IHt9O1xuICAgIGNvbnN0IGNvbnZlcnRlciA9IG5ldyBYbWxUb0kxOG4oKTtcblxuICAgIC8vIEJlY2F1c2Ugd2Ugc2hvdWxkIGJlIGFibGUgdG8gbG9hZCB4dGIgZmlsZXMgdGhhdCByZWx5IG9uIGZlYXR1cmVzIG5vdCBzdXBwb3J0ZWQgYnkgYW5ndWxhcixcbiAgICAvLyB3ZSBuZWVkIHRvIGRlbGF5IHRoZSBjb252ZXJzaW9uIG9mIGh0bWwgdG8gaTE4biBub2RlcyBzbyB0aGF0IG5vbiBhbmd1bGFyIG1lc3NhZ2VzIGFyZSBub3RcbiAgICAvLyBjb252ZXJ0ZWRcbiAgICBPYmplY3Qua2V5cyhtc2dJZFRvSHRtbCkuZm9yRWFjaChtc2dJZCA9PiB7XG4gICAgICBjb25zdCB2YWx1ZUZuID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIGNvbnN0IHtpMThuTm9kZXMsIGVycm9yc30gPSBjb252ZXJ0ZXIuY29udmVydChtc2dJZFRvSHRtbFttc2dJZF0sIHVybCk7XG4gICAgICAgIGlmIChlcnJvcnMubGVuZ3RoKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGB4dGIgcGFyc2UgZXJyb3JzOlxcbiR7ZXJyb3JzLmpvaW4oJ1xcbicpfWApO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBpMThuTm9kZXM7XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGF6eVByb3BlcnR5KGkxOG5Ob2Rlc0J5TXNnSWQsIG1zZ0lkLCB2YWx1ZUZuKTtcbiAgICB9KTtcblxuICAgIGlmIChlcnJvcnMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYHh0YiBwYXJzZSBlcnJvcnM6XFxuJHtlcnJvcnMuam9pbignXFxuJyl9YCk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHtsb2NhbGU6IGxvY2FsZSAhLCBpMThuTm9kZXNCeU1zZ0lkfTtcbiAgfVxuXG4gIGRpZ2VzdChtZXNzYWdlOiBpMThuLk1lc3NhZ2UpOiBzdHJpbmcgeyByZXR1cm4gZGlnZXN0KG1lc3NhZ2UpOyB9XG5cbiAgY3JlYXRlTmFtZU1hcHBlcihtZXNzYWdlOiBpMThuLk1lc3NhZ2UpOiBQbGFjZWhvbGRlck1hcHBlciB7XG4gICAgcmV0dXJuIG5ldyBTaW1wbGVQbGFjZWhvbGRlck1hcHBlcihtZXNzYWdlLCB0b1B1YmxpY05hbWUpO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNyZWF0ZUxhenlQcm9wZXJ0eShtZXNzYWdlczogYW55LCBpZDogc3RyaW5nLCB2YWx1ZUZuOiAoKSA9PiBhbnkpIHtcbiAgT2JqZWN0LmRlZmluZVByb3BlcnR5KG1lc3NhZ2VzLCBpZCwge1xuICAgIGNvbmZpZ3VyYWJsZTogdHJ1ZSxcbiAgICBlbnVtZXJhYmxlOiB0cnVlLFxuICAgIGdldDogZnVuY3Rpb24oKSB7XG4gICAgICBjb25zdCB2YWx1ZSA9IHZhbHVlRm4oKTtcbiAgICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShtZXNzYWdlcywgaWQsIHtlbnVtZXJhYmxlOiB0cnVlLCB2YWx1ZX0pO1xuICAgICAgcmV0dXJuIHZhbHVlO1xuICAgIH0sXG4gICAgc2V0OiBfID0+IHsgdGhyb3cgbmV3IEVycm9yKCdDb3VsZCBub3Qgb3ZlcndyaXRlIGFuIFhUQiB0cmFuc2xhdGlvbicpOyB9LFxuICB9KTtcbn1cblxuLy8gRXh0cmFjdCBtZXNzYWdlcyBhcyB4bWwgbm9kZXMgZnJvbSB0aGUgeHRiIGZpbGVcbmNsYXNzIFh0YlBhcnNlciBpbXBsZW1lbnRzIG1sLlZpc2l0b3Ige1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfYnVuZGxlRGVwdGggITogbnVtYmVyO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfZXJyb3JzICE6IEkxOG5FcnJvcltdO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfbXNnSWRUb0h0bWwgIToge1ttc2dJZDogc3RyaW5nXTogc3RyaW5nfTtcbiAgcHJpdmF0ZSBfbG9jYWxlOiBzdHJpbmd8bnVsbCA9IG51bGw7XG5cbiAgcGFyc2UoeHRiOiBzdHJpbmcsIHVybDogc3RyaW5nKSB7XG4gICAgdGhpcy5fYnVuZGxlRGVwdGggPSAwO1xuICAgIHRoaXMuX21zZ0lkVG9IdG1sID0ge307XG5cbiAgICAvLyBXZSBjYW4gbm90IHBhcnNlIHRoZSBJQ1UgbWVzc2FnZXMgYXQgdGhpcyBwb2ludCBhcyBzb21lIG1lc3NhZ2VzIG1pZ2h0IG5vdCBvcmlnaW5hdGVcbiAgICAvLyBmcm9tIEFuZ3VsYXIgdGhhdCBjb3VsZCBub3QgYmUgbGV4J2QuXG4gICAgY29uc3QgeG1sID0gbmV3IFhtbFBhcnNlcigpLnBhcnNlKHh0YiwgdXJsKTtcblxuICAgIHRoaXMuX2Vycm9ycyA9IHhtbC5lcnJvcnM7XG4gICAgbWwudmlzaXRBbGwodGhpcywgeG1sLnJvb3ROb2Rlcyk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgbXNnSWRUb0h0bWw6IHRoaXMuX21zZ0lkVG9IdG1sLFxuICAgICAgZXJyb3JzOiB0aGlzLl9lcnJvcnMsXG4gICAgICBsb2NhbGU6IHRoaXMuX2xvY2FsZSxcbiAgICB9O1xuICB9XG5cbiAgdmlzaXRFbGVtZW50KGVsZW1lbnQ6IG1sLkVsZW1lbnQsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgc3dpdGNoIChlbGVtZW50Lm5hbWUpIHtcbiAgICAgIGNhc2UgX1RSQU5TTEFUSU9OU19UQUc6XG4gICAgICAgIHRoaXMuX2J1bmRsZURlcHRoKys7XG4gICAgICAgIGlmICh0aGlzLl9idW5kbGVEZXB0aCA+IDEpIHtcbiAgICAgICAgICB0aGlzLl9hZGRFcnJvcihlbGVtZW50LCBgPCR7X1RSQU5TTEFUSU9OU19UQUd9PiBlbGVtZW50cyBjYW4gbm90IGJlIG5lc3RlZGApO1xuICAgICAgICB9XG4gICAgICAgIGNvbnN0IGxhbmdBdHRyID0gZWxlbWVudC5hdHRycy5maW5kKChhdHRyKSA9PiBhdHRyLm5hbWUgPT09ICdsYW5nJyk7XG4gICAgICAgIGlmIChsYW5nQXR0cikge1xuICAgICAgICAgIHRoaXMuX2xvY2FsZSA9IGxhbmdBdHRyLnZhbHVlO1xuICAgICAgICB9XG4gICAgICAgIG1sLnZpc2l0QWxsKHRoaXMsIGVsZW1lbnQuY2hpbGRyZW4sIG51bGwpO1xuICAgICAgICB0aGlzLl9idW5kbGVEZXB0aC0tO1xuICAgICAgICBicmVhaztcblxuICAgICAgY2FzZSBfVFJBTlNMQVRJT05fVEFHOlxuICAgICAgICBjb25zdCBpZEF0dHIgPSBlbGVtZW50LmF0dHJzLmZpbmQoKGF0dHIpID0+IGF0dHIubmFtZSA9PT0gJ2lkJyk7XG4gICAgICAgIGlmICghaWRBdHRyKSB7XG4gICAgICAgICAgdGhpcy5fYWRkRXJyb3IoZWxlbWVudCwgYDwke19UUkFOU0xBVElPTl9UQUd9PiBtaXNzZXMgdGhlIFwiaWRcIiBhdHRyaWJ1dGVgKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zdCBpZCA9IGlkQXR0ci52YWx1ZTtcbiAgICAgICAgICBpZiAodGhpcy5fbXNnSWRUb0h0bWwuaGFzT3duUHJvcGVydHkoaWQpKSB7XG4gICAgICAgICAgICB0aGlzLl9hZGRFcnJvcihlbGVtZW50LCBgRHVwbGljYXRlZCB0cmFuc2xhdGlvbnMgZm9yIG1zZyAke2lkfWApO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBjb25zdCBpbm5lclRleHRTdGFydCA9IGVsZW1lbnQuc3RhcnRTb3VyY2VTcGFuICEuZW5kLm9mZnNldDtcbiAgICAgICAgICAgIGNvbnN0IGlubmVyVGV4dEVuZCA9IGVsZW1lbnQuZW5kU291cmNlU3BhbiAhLnN0YXJ0Lm9mZnNldDtcbiAgICAgICAgICAgIGNvbnN0IGNvbnRlbnQgPSBlbGVtZW50LnN0YXJ0U291cmNlU3BhbiAhLnN0YXJ0LmZpbGUuY29udGVudDtcbiAgICAgICAgICAgIGNvbnN0IGlubmVyVGV4dCA9IGNvbnRlbnQuc2xpY2UoaW5uZXJUZXh0U3RhcnQgISwgaW5uZXJUZXh0RW5kICEpO1xuICAgICAgICAgICAgdGhpcy5fbXNnSWRUb0h0bWxbaWRdID0gaW5uZXJUZXh0O1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBicmVhaztcblxuICAgICAgZGVmYXVsdDpcbiAgICAgICAgdGhpcy5fYWRkRXJyb3IoZWxlbWVudCwgJ1VuZXhwZWN0ZWQgdGFnJyk7XG4gICAgfVxuICB9XG5cbiAgdmlzaXRBdHRyaWJ1dGUoYXR0cmlidXRlOiBtbC5BdHRyaWJ1dGUsIGNvbnRleHQ6IGFueSk6IGFueSB7fVxuXG4gIHZpc2l0VGV4dCh0ZXh0OiBtbC5UZXh0LCBjb250ZXh0OiBhbnkpOiBhbnkge31cblxuICB2aXNpdENvbW1lbnQoY29tbWVudDogbWwuQ29tbWVudCwgY29udGV4dDogYW55KTogYW55IHt9XG5cbiAgdmlzaXRFeHBhbnNpb24oZXhwYW5zaW9uOiBtbC5FeHBhbnNpb24sIGNvbnRleHQ6IGFueSk6IGFueSB7fVxuXG4gIHZpc2l0RXhwYW5zaW9uQ2FzZShleHBhbnNpb25DYXNlOiBtbC5FeHBhbnNpb25DYXNlLCBjb250ZXh0OiBhbnkpOiBhbnkge31cblxuICBwcml2YXRlIF9hZGRFcnJvcihub2RlOiBtbC5Ob2RlLCBtZXNzYWdlOiBzdHJpbmcpOiB2b2lkIHtcbiAgICB0aGlzLl9lcnJvcnMucHVzaChuZXcgSTE4bkVycm9yKG5vZGUuc291cmNlU3BhbiAhLCBtZXNzYWdlKSk7XG4gIH1cbn1cblxuLy8gQ29udmVydCBtbCBub2RlcyAoeHRiIHN5bnRheCkgdG8gaTE4biBub2Rlc1xuY2xhc3MgWG1sVG9JMThuIGltcGxlbWVudHMgbWwuVmlzaXRvciB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9lcnJvcnMgITogSTE4bkVycm9yW107XG5cbiAgY29udmVydChtZXNzYWdlOiBzdHJpbmcsIHVybDogc3RyaW5nKSB7XG4gICAgY29uc3QgeG1sSWN1ID0gbmV3IFhtbFBhcnNlcigpLnBhcnNlKG1lc3NhZ2UsIHVybCwge3Rva2VuaXplRXhwYW5zaW9uRm9ybXM6IHRydWV9KTtcbiAgICB0aGlzLl9lcnJvcnMgPSB4bWxJY3UuZXJyb3JzO1xuXG4gICAgY29uc3QgaTE4bk5vZGVzID0gdGhpcy5fZXJyb3JzLmxlbmd0aCA+IDAgfHwgeG1sSWN1LnJvb3ROb2Rlcy5sZW5ndGggPT0gMCA/XG4gICAgICAgIFtdIDpcbiAgICAgICAgbWwudmlzaXRBbGwodGhpcywgeG1sSWN1LnJvb3ROb2Rlcyk7XG5cbiAgICByZXR1cm4ge1xuICAgICAgaTE4bk5vZGVzLFxuICAgICAgZXJyb3JzOiB0aGlzLl9lcnJvcnMsXG4gICAgfTtcbiAgfVxuXG4gIHZpc2l0VGV4dCh0ZXh0OiBtbC5UZXh0LCBjb250ZXh0OiBhbnkpIHsgcmV0dXJuIG5ldyBpMThuLlRleHQodGV4dC52YWx1ZSwgdGV4dC5zb3VyY2VTcGFuICEpOyB9XG5cbiAgdmlzaXRFeHBhbnNpb24oaWN1OiBtbC5FeHBhbnNpb24sIGNvbnRleHQ6IGFueSkge1xuICAgIGNvbnN0IGNhc2VNYXA6IHtbdmFsdWU6IHN0cmluZ106IGkxOG4uTm9kZX0gPSB7fTtcblxuICAgIG1sLnZpc2l0QWxsKHRoaXMsIGljdS5jYXNlcykuZm9yRWFjaChjID0+IHtcbiAgICAgIGNhc2VNYXBbYy52YWx1ZV0gPSBuZXcgaTE4bi5Db250YWluZXIoYy5ub2RlcywgaWN1LnNvdXJjZVNwYW4pO1xuICAgIH0pO1xuXG4gICAgcmV0dXJuIG5ldyBpMThuLkljdShpY3Uuc3dpdGNoVmFsdWUsIGljdS50eXBlLCBjYXNlTWFwLCBpY3Uuc291cmNlU3Bhbik7XG4gIH1cblxuICB2aXNpdEV4cGFuc2lvbkNhc2UoaWN1Q2FzZTogbWwuRXhwYW5zaW9uQ2FzZSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICByZXR1cm4ge1xuICAgICAgdmFsdWU6IGljdUNhc2UudmFsdWUsXG4gICAgICBub2RlczogbWwudmlzaXRBbGwodGhpcywgaWN1Q2FzZS5leHByZXNzaW9uKSxcbiAgICB9O1xuICB9XG5cbiAgdmlzaXRFbGVtZW50KGVsOiBtbC5FbGVtZW50LCBjb250ZXh0OiBhbnkpOiBpMThuLlBsYWNlaG9sZGVyfG51bGwge1xuICAgIGlmIChlbC5uYW1lID09PSBfUExBQ0VIT0xERVJfVEFHKSB7XG4gICAgICBjb25zdCBuYW1lQXR0ciA9IGVsLmF0dHJzLmZpbmQoKGF0dHIpID0+IGF0dHIubmFtZSA9PT0gJ25hbWUnKTtcbiAgICAgIGlmIChuYW1lQXR0cikge1xuICAgICAgICByZXR1cm4gbmV3IGkxOG4uUGxhY2Vob2xkZXIoJycsIG5hbWVBdHRyLnZhbHVlLCBlbC5zb3VyY2VTcGFuICEpO1xuICAgICAgfVxuXG4gICAgICB0aGlzLl9hZGRFcnJvcihlbCwgYDwke19QTEFDRUhPTERFUl9UQUd9PiBtaXNzZXMgdGhlIFwibmFtZVwiIGF0dHJpYnV0ZWApO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLl9hZGRFcnJvcihlbCwgYFVuZXhwZWN0ZWQgdGFnYCk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgdmlzaXRDb21tZW50KGNvbW1lbnQ6IG1sLkNvbW1lbnQsIGNvbnRleHQ6IGFueSkge31cblxuICB2aXNpdEF0dHJpYnV0ZShhdHRyaWJ1dGU6IG1sLkF0dHJpYnV0ZSwgY29udGV4dDogYW55KSB7fVxuXG4gIHByaXZhdGUgX2FkZEVycm9yKG5vZGU6IG1sLk5vZGUsIG1lc3NhZ2U6IHN0cmluZyk6IHZvaWQge1xuICAgIHRoaXMuX2Vycm9ycy5wdXNoKG5ldyBJMThuRXJyb3Iobm9kZS5zb3VyY2VTcGFuICEsIG1lc3NhZ2UpKTtcbiAgfVxufVxuIl19