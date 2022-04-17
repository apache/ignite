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
        define("@angular/compiler/src/i18n/serializers/xliff", ["require", "exports", "tslib", "@angular/compiler/src/ml_parser/ast", "@angular/compiler/src/ml_parser/xml_parser", "@angular/compiler/src/i18n/digest", "@angular/compiler/src/i18n/i18n_ast", "@angular/compiler/src/i18n/parse_util", "@angular/compiler/src/i18n/serializers/serializer", "@angular/compiler/src/i18n/serializers/xml_helper"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var ml = require("@angular/compiler/src/ml_parser/ast");
    var xml_parser_1 = require("@angular/compiler/src/ml_parser/xml_parser");
    var digest_1 = require("@angular/compiler/src/i18n/digest");
    var i18n = require("@angular/compiler/src/i18n/i18n_ast");
    var parse_util_1 = require("@angular/compiler/src/i18n/parse_util");
    var serializer_1 = require("@angular/compiler/src/i18n/serializers/serializer");
    var xml = require("@angular/compiler/src/i18n/serializers/xml_helper");
    var _VERSION = '1.2';
    var _XMLNS = 'urn:oasis:names:tc:xliff:document:1.2';
    // TODO(vicb): make this a param (s/_/-/)
    var _DEFAULT_SOURCE_LANG = 'en';
    var _PLACEHOLDER_TAG = 'x';
    var _MARKER_TAG = 'mrk';
    var _FILE_TAG = 'file';
    var _SOURCE_TAG = 'source';
    var _SEGMENT_SOURCE_TAG = 'seg-source';
    var _ALT_TRANS_TAG = 'alt-trans';
    var _TARGET_TAG = 'target';
    var _UNIT_TAG = 'trans-unit';
    var _CONTEXT_GROUP_TAG = 'context-group';
    var _CONTEXT_TAG = 'context';
    // http://docs.oasis-open.org/xliff/v1.2/os/xliff-core.html
    // http://docs.oasis-open.org/xliff/v1.2/xliff-profile-html/xliff-profile-html-1.2.html
    var Xliff = /** @class */ (function (_super) {
        tslib_1.__extends(Xliff, _super);
        function Xliff() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        Xliff.prototype.write = function (messages, locale) {
            var visitor = new _WriteVisitor();
            var transUnits = [];
            messages.forEach(function (message) {
                var _a;
                var contextTags = [];
                message.sources.forEach(function (source) {
                    var contextGroupTag = new xml.Tag(_CONTEXT_GROUP_TAG, { purpose: 'location' });
                    contextGroupTag.children.push(new xml.CR(10), new xml.Tag(_CONTEXT_TAG, { 'context-type': 'sourcefile' }, [new xml.Text(source.filePath)]), new xml.CR(10), new xml.Tag(_CONTEXT_TAG, { 'context-type': 'linenumber' }, [new xml.Text("" + source.startLine)]), new xml.CR(8));
                    contextTags.push(new xml.CR(8), contextGroupTag);
                });
                var transUnit = new xml.Tag(_UNIT_TAG, { id: message.id, datatype: 'html' });
                (_a = transUnit.children).push.apply(_a, tslib_1.__spread([new xml.CR(8), new xml.Tag(_SOURCE_TAG, {}, visitor.serialize(message.nodes))], contextTags));
                if (message.description) {
                    transUnit.children.push(new xml.CR(8), new xml.Tag('note', { priority: '1', from: 'description' }, [new xml.Text(message.description)]));
                }
                if (message.meaning) {
                    transUnit.children.push(new xml.CR(8), new xml.Tag('note', { priority: '1', from: 'meaning' }, [new xml.Text(message.meaning)]));
                }
                transUnit.children.push(new xml.CR(6));
                transUnits.push(new xml.CR(6), transUnit);
            });
            var body = new xml.Tag('body', {}, tslib_1.__spread(transUnits, [new xml.CR(4)]));
            var file = new xml.Tag('file', {
                'source-language': locale || _DEFAULT_SOURCE_LANG,
                datatype: 'plaintext',
                original: 'ng2.template',
            }, [new xml.CR(4), body, new xml.CR(2)]);
            var xliff = new xml.Tag('xliff', { version: _VERSION, xmlns: _XMLNS }, [new xml.CR(2), file, new xml.CR()]);
            return xml.serialize([
                new xml.Declaration({ version: '1.0', encoding: 'UTF-8' }), new xml.CR(), xliff, new xml.CR()
            ]);
        };
        Xliff.prototype.load = function (content, url) {
            // xliff to xml nodes
            var xliffParser = new XliffParser();
            var _a = xliffParser.parse(content, url), locale = _a.locale, msgIdToHtml = _a.msgIdToHtml, errors = _a.errors;
            // xml nodes to i18n nodes
            var i18nNodesByMsgId = {};
            var converter = new XmlToI18n();
            Object.keys(msgIdToHtml).forEach(function (msgId) {
                var _a = converter.convert(msgIdToHtml[msgId], url), i18nNodes = _a.i18nNodes, e = _a.errors;
                errors.push.apply(errors, tslib_1.__spread(e));
                i18nNodesByMsgId[msgId] = i18nNodes;
            });
            if (errors.length) {
                throw new Error("xliff parse errors:\n" + errors.join('\n'));
            }
            return { locale: locale, i18nNodesByMsgId: i18nNodesByMsgId };
        };
        Xliff.prototype.digest = function (message) { return digest_1.digest(message); };
        return Xliff;
    }(serializer_1.Serializer));
    exports.Xliff = Xliff;
    var _WriteVisitor = /** @class */ (function () {
        function _WriteVisitor() {
        }
        _WriteVisitor.prototype.visitText = function (text, context) { return [new xml.Text(text.value)]; };
        _WriteVisitor.prototype.visitContainer = function (container, context) {
            var _this = this;
            var nodes = [];
            container.children.forEach(function (node) { return nodes.push.apply(nodes, tslib_1.__spread(node.visit(_this))); });
            return nodes;
        };
        _WriteVisitor.prototype.visitIcu = function (icu, context) {
            var _this = this;
            var nodes = [new xml.Text("{" + icu.expressionPlaceholder + ", " + icu.type + ", ")];
            Object.keys(icu.cases).forEach(function (c) {
                nodes.push.apply(nodes, tslib_1.__spread([new xml.Text(c + " {")], icu.cases[c].visit(_this), [new xml.Text("} ")]));
            });
            nodes.push(new xml.Text("}"));
            return nodes;
        };
        _WriteVisitor.prototype.visitTagPlaceholder = function (ph, context) {
            var ctype = getCtypeForTag(ph.tag);
            if (ph.isVoid) {
                // void tags have no children nor closing tags
                return [new xml.Tag(_PLACEHOLDER_TAG, { id: ph.startName, ctype: ctype, 'equiv-text': "<" + ph.tag + "/>" })];
            }
            var startTagPh = new xml.Tag(_PLACEHOLDER_TAG, { id: ph.startName, ctype: ctype, 'equiv-text': "<" + ph.tag + ">" });
            var closeTagPh = new xml.Tag(_PLACEHOLDER_TAG, { id: ph.closeName, ctype: ctype, 'equiv-text': "</" + ph.tag + ">" });
            return tslib_1.__spread([startTagPh], this.serialize(ph.children), [closeTagPh]);
        };
        _WriteVisitor.prototype.visitPlaceholder = function (ph, context) {
            return [new xml.Tag(_PLACEHOLDER_TAG, { id: ph.name, 'equiv-text': "{{" + ph.value + "}}" })];
        };
        _WriteVisitor.prototype.visitIcuPlaceholder = function (ph, context) {
            var equivText = "{" + ph.value.expression + ", " + ph.value.type + ", " + Object.keys(ph.value.cases).map(function (value) { return value + ' {...}'; }).join(' ') + "}";
            return [new xml.Tag(_PLACEHOLDER_TAG, { id: ph.name, 'equiv-text': equivText })];
        };
        _WriteVisitor.prototype.serialize = function (nodes) {
            var _this = this;
            return [].concat.apply([], tslib_1.__spread(nodes.map(function (node) { return node.visit(_this); })));
        };
        return _WriteVisitor;
    }());
    // TODO(vicb): add error management (structure)
    // Extract messages as xml nodes from the xliff file
    var XliffParser = /** @class */ (function () {
        function XliffParser() {
            this._locale = null;
        }
        XliffParser.prototype.parse = function (xliff, url) {
            this._unitMlString = null;
            this._msgIdToHtml = {};
            var xml = new xml_parser_1.XmlParser().parse(xliff, url);
            this._errors = xml.errors;
            ml.visitAll(this, xml.rootNodes, null);
            return {
                msgIdToHtml: this._msgIdToHtml,
                errors: this._errors,
                locale: this._locale,
            };
        };
        XliffParser.prototype.visitElement = function (element, context) {
            switch (element.name) {
                case _UNIT_TAG:
                    this._unitMlString = null;
                    var idAttr = element.attrs.find(function (attr) { return attr.name === 'id'; });
                    if (!idAttr) {
                        this._addError(element, "<" + _UNIT_TAG + "> misses the \"id\" attribute");
                    }
                    else {
                        var id = idAttr.value;
                        if (this._msgIdToHtml.hasOwnProperty(id)) {
                            this._addError(element, "Duplicated translations for msg " + id);
                        }
                        else {
                            ml.visitAll(this, element.children, null);
                            if (typeof this._unitMlString === 'string') {
                                this._msgIdToHtml[id] = this._unitMlString;
                            }
                            else {
                                this._addError(element, "Message " + id + " misses a translation");
                            }
                        }
                    }
                    break;
                // ignore those tags
                case _SOURCE_TAG:
                case _SEGMENT_SOURCE_TAG:
                case _ALT_TRANS_TAG:
                    break;
                case _TARGET_TAG:
                    var innerTextStart = element.startSourceSpan.end.offset;
                    var innerTextEnd = element.endSourceSpan.start.offset;
                    var content = element.startSourceSpan.start.file.content;
                    var innerText = content.slice(innerTextStart, innerTextEnd);
                    this._unitMlString = innerText;
                    break;
                case _FILE_TAG:
                    var localeAttr = element.attrs.find(function (attr) { return attr.name === 'target-language'; });
                    if (localeAttr) {
                        this._locale = localeAttr.value;
                    }
                    ml.visitAll(this, element.children, null);
                    break;
                default:
                    // TODO(vicb): assert file structure, xliff version
                    // For now only recurse on unhandled nodes
                    ml.visitAll(this, element.children, null);
            }
        };
        XliffParser.prototype.visitAttribute = function (attribute, context) { };
        XliffParser.prototype.visitText = function (text, context) { };
        XliffParser.prototype.visitComment = function (comment, context) { };
        XliffParser.prototype.visitExpansion = function (expansion, context) { };
        XliffParser.prototype.visitExpansionCase = function (expansionCase, context) { };
        XliffParser.prototype._addError = function (node, message) {
            this._errors.push(new parse_util_1.I18nError(node.sourceSpan, message));
        };
        return XliffParser;
    }());
    // Convert ml nodes (xliff syntax) to i18n nodes
    var XmlToI18n = /** @class */ (function () {
        function XmlToI18n() {
        }
        XmlToI18n.prototype.convert = function (message, url) {
            var xmlIcu = new xml_parser_1.XmlParser().parse(message, url, { tokenizeExpansionForms: true });
            this._errors = xmlIcu.errors;
            var i18nNodes = this._errors.length > 0 || xmlIcu.rootNodes.length == 0 ?
                [] : [].concat.apply([], tslib_1.__spread(ml.visitAll(this, xmlIcu.rootNodes)));
            return {
                i18nNodes: i18nNodes,
                errors: this._errors,
            };
        };
        XmlToI18n.prototype.visitText = function (text, context) { return new i18n.Text(text.value, text.sourceSpan); };
        XmlToI18n.prototype.visitElement = function (el, context) {
            if (el.name === _PLACEHOLDER_TAG) {
                var nameAttr = el.attrs.find(function (attr) { return attr.name === 'id'; });
                if (nameAttr) {
                    return new i18n.Placeholder('', nameAttr.value, el.sourceSpan);
                }
                this._addError(el, "<" + _PLACEHOLDER_TAG + "> misses the \"id\" attribute");
                return null;
            }
            if (el.name === _MARKER_TAG) {
                return [].concat.apply([], tslib_1.__spread(ml.visitAll(this, el.children)));
            }
            this._addError(el, "Unexpected tag");
            return null;
        };
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
        XmlToI18n.prototype.visitComment = function (comment, context) { };
        XmlToI18n.prototype.visitAttribute = function (attribute, context) { };
        XmlToI18n.prototype._addError = function (node, message) {
            this._errors.push(new parse_util_1.I18nError(node.sourceSpan, message));
        };
        return XmlToI18n;
    }());
    function getCtypeForTag(tag) {
        switch (tag.toLowerCase()) {
            case 'br':
                return 'lb';
            case 'img':
                return 'image';
            default:
                return "x-" + tag;
        }
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieGxpZmYuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaTE4bi9zZXJpYWxpemVycy94bGlmZi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCx3REFBMEM7SUFDMUMseUVBQXFEO0lBQ3JELDREQUFpQztJQUNqQywwREFBb0M7SUFDcEMsb0VBQXdDO0lBRXhDLGdGQUF3QztJQUN4Qyx1RUFBb0M7SUFFcEMsSUFBTSxRQUFRLEdBQUcsS0FBSyxDQUFDO0lBQ3ZCLElBQU0sTUFBTSxHQUFHLHVDQUF1QyxDQUFDO0lBQ3ZELHlDQUF5QztJQUN6QyxJQUFNLG9CQUFvQixHQUFHLElBQUksQ0FBQztJQUNsQyxJQUFNLGdCQUFnQixHQUFHLEdBQUcsQ0FBQztJQUM3QixJQUFNLFdBQVcsR0FBRyxLQUFLLENBQUM7SUFFMUIsSUFBTSxTQUFTLEdBQUcsTUFBTSxDQUFDO0lBQ3pCLElBQU0sV0FBVyxHQUFHLFFBQVEsQ0FBQztJQUM3QixJQUFNLG1CQUFtQixHQUFHLFlBQVksQ0FBQztJQUN6QyxJQUFNLGNBQWMsR0FBRyxXQUFXLENBQUM7SUFDbkMsSUFBTSxXQUFXLEdBQUcsUUFBUSxDQUFDO0lBQzdCLElBQU0sU0FBUyxHQUFHLFlBQVksQ0FBQztJQUMvQixJQUFNLGtCQUFrQixHQUFHLGVBQWUsQ0FBQztJQUMzQyxJQUFNLFlBQVksR0FBRyxTQUFTLENBQUM7SUFFL0IsMkRBQTJEO0lBQzNELHVGQUF1RjtJQUN2RjtRQUEyQixpQ0FBVTtRQUFyQzs7UUFtRkEsQ0FBQztRQWxGQyxxQkFBSyxHQUFMLFVBQU0sUUFBd0IsRUFBRSxNQUFtQjtZQUNqRCxJQUFNLE9BQU8sR0FBRyxJQUFJLGFBQWEsRUFBRSxDQUFDO1lBQ3BDLElBQU0sVUFBVSxHQUFlLEVBQUUsQ0FBQztZQUVsQyxRQUFRLENBQUMsT0FBTyxDQUFDLFVB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LENBQUM7WUFFSCxPQUFPLElBQUksSUFBSSxDQUFDLEdBQUcsQ0FBQyxHQUFHLENBQUMsV0FBVyxFQUFFLEdBQUcsQ0FBQyxJQUFJLEVBQUUsT0FBTyxFQUFFLEdBQUcsQ0FBQyxVQUFVLENBQUMsQ0FBQztRQUMxRSxDQUFDO1FBRUQsc0NBQWtCLEdBQWxCLFVBQW1CLE9BQXlCLEVBQUUsT0FBWTtZQUN4RCxPQUFPO2dCQUNMLEtBQUssRUFBRSxPQUFPLENBQUMsS0FBSztnQkFDcEIsS0FBSyxFQUFFLEVBQUUsQ0FBQyxRQUFRLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxVQUFVLENBQUM7YUFDN0MsQ0FBQztRQUNKLENBQUM7UUFFRCxnQ0FBWSxHQUFaLFVBQWEsT0FBbUIsRUFBRSxPQUFZLElBQUcsQ0FBQztRQUVsRCxrQ0FBYyxHQUFkLFVBQWUsU0FBdUIsRUFBRSxPQUFZLElBQUcsQ0FBQztRQUVoRCw2QkFBUyxHQUFqQixVQUFrQixJQUFhLEVBQUUsT0FBZTtZQUM5QyxJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxJQUFJLHNCQUFTLENBQUMsSUFBSSxDQUFDLFVBQVksRUFBRSxPQUFPLENBQUMsQ0FBQyxDQUFDO1FBQy9ELENBQUM7UUFDSCxnQkFBQztJQUFELENBQUMsQUEvREQsSUErREM7SUFFRCxTQUFTLGNBQWMsQ0FBQyxHQUFXO1FBQ2pDLFFBQVEsR0FBRyxDQUFDLFdBQVcsRUFBRSxFQUFFO1lBQ3pCLEtBQUssSUFBSTtnQkFDUCxPQUFPLElBQUksQ0FBQztZQUNkLEtBQUssS0FBSztnQkFDUixPQUFPLE9BQU8sQ0FBQztZQUNqQjtnQkFDRSxPQUFPLE9BQUssR0FBSyxDQUFDO1NBQ3JCO0lBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgbWwgZnJvbSAnLi4vLi4vbWxfcGFyc2VyL2FzdCc7XG5pbXBvcnQge1htbFBhcnNlcn0gZnJvbSAnLi4vLi4vbWxfcGFyc2VyL3htbF9wYXJzZXInO1xuaW1wb3J0IHtkaWdlc3R9IGZyb20gJy4uL2RpZ2VzdCc7XG5pbXBvcnQgKiBhcyBpMThuIGZyb20gJy4uL2kxOG5fYXN0JztcbmltcG9ydCB7STE4bkVycm9yfSBmcm9tICcuLi9wYXJzZV91dGlsJztcblxuaW1wb3J0IHtTZXJpYWxpemVyfSBmcm9tICcuL3NlcmlhbGl6ZXInO1xuaW1wb3J0ICogYXMgeG1sIGZyb20gJy4veG1sX2hlbHBlcic7XG5cbmNvbnN0IF9WRVJTSU9OID0gJzEuMic7XG5jb25zdCBfWE1MTlMgPSAndXJuOm9hc2lzOm5hbWVzOnRjOnhsaWZmOmRvY3VtZW50OjEuMic7XG4vLyBUT0RPKHZpY2IpOiBtYWtlIHRoaXMgYSBwYXJhbSAocy9fLy0vKVxuY29uc3QgX0RFRkFVTFRfU09VUkNFX0xBTkcgPSAnZW4nO1xuY29uc3QgX1BMQUNFSE9MREVSX1RBRyA9ICd4JztcbmNvbnN0IF9NQVJLRVJfVEFHID0gJ21yayc7XG5cbmNvbnN0IF9GSUxFX1RBRyA9ICdmaWxlJztcbmNvbnN0IF9TT1VSQ0VfVEFHID0gJ3NvdXJjZSc7XG5jb25zdCBfU0VHTUVOVF9TT1VSQ0VfVEFHID0gJ3NlZy1zb3VyY2UnO1xuY29uc3QgX0FMVF9UUkFOU19UQUcgPSAnYWx0LXRyYW5zJztcbmNvbnN0IF9UQVJHRVRfVEFHID0gJ3RhcmdldCc7XG5jb25zdCBfVU5JVF9UQUcgPSAndHJhbnMtdW5pdCc7XG5jb25zdCBfQ09OVEVYVF9HUk9VUF9UQUcgPSAnY29udGV4dC1ncm91cCc7XG5jb25zdCBfQ09OVEVYVF9UQUcgPSAnY29udGV4dCc7XG5cbi8vIGh0dHA6Ly9kb2NzLm9hc2lzLW9wZW4ub3JnL3hsaWZmL3YxLjIvb3MveGxpZmYtY29yZS5odG1sXG4vLyBodHRwOi8vZG9jcy5vYXNpcy1vcGVuLm9yZy94bGlmZi92MS4yL3hsaWZmLXByb2ZpbGUtaHRtbC94bGlmZi1wcm9maWxlLWh0bWwtMS4yLmh0bWxcbmV4cG9ydCBjbGFzcyBYbGlmZiBleHRlbmRzIFNlcmlhbGl6ZXIge1xuICB3cml0ZShtZXNzYWdlczogaTE4bi5NZXNzYWdlW10sIGxvY2FsZTogc3RyaW5nfG51bGwpOiBzdHJpbmcge1xuICAgIGNvbnN0IHZpc2l0b3IgPSBuZXcgX1dyaXRlVmlzaXRvcigpO1xuICAgIGNvbnN0IHRyYW5zVW5pdHM6IHhtbC5Ob2RlW10gPSBbXTtcblxuICAgIG1lc3NhZ2VzLmZvckVhY2gobWVzc2FnZSA9PiB7XG4gICAgICBsZXQgY29udGV4dFRhZ3M6IHhtbC5Ob2RlW10gPSBbXTtcbiAgICAgIG1lc3NhZ2Uuc291cmNlcy5mb3JFYWNoKChzb3VyY2U6IGkxOG4uTWVzc2FnZVNwYW4pID0+IHtcbiAgICAgICAgbGV0IGNvbnRleHRHcm91cFRhZyA9IG5ldyB4bWwuVGFnKF9DT05URVhUX0dST1VQX1RBRywge3B1cnBvc2U6ICdsb2NhdGlvbid9KTtcbiAgICAgICAgY29udGV4dEdyb3VwVGFnLmNoaWxkcmVuLnB1c2goXG4gICAgICAgICAgICBuZXcgeG1sLkNSKDEwKSxcbiAgICAgICAgICAgIG5ldyB4bWwuVGFnKFxuICAgICAgICAgICAgICAgIF9DT05URVhUX1RBRywgeydjb250ZXh0LXR5cGUnOiAnc291cmNlZmlsZSd9LCBbbmV3IHhtbC5UZXh0KHNvdXJjZS5maWxlUGF0aCldKSxcbiAgICAgICAgICAgIG5ldyB4bWwuQ1IoMTApLCBuZXcgeG1sLlRhZyhcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgX0NPTlRFWFRfVEFHLCB7J2NvbnRleHQtdHlwZSc6ICdsaW5lbnVtYmVyJ30sXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIFtuZXcgeG1sLlRleHQoYCR7c291cmNlLnN0YXJ0TGluZX1gKV0pLFxuICAgICAgICAgICAgbmV3IHhtbC5DUig4KSk7XG4gICAgICAgIGNvbnRleHRUYWdzLnB1c2gobmV3IHhtbC5DUig4KSwgY29udGV4dEdyb3VwVGFnKTtcbiAgICAgIH0pO1xuXG4gICAgICBjb25zdCB0cmFuc1VuaXQgPSBuZXcgeG1sLlRhZyhfVU5JVF9UQUcsIHtpZDogbWVzc2FnZS5pZCwgZGF0YXR5cGU6ICdodG1sJ30pO1xuICAgICAgdHJhbnNVbml0LmNoaWxkcmVuLnB1c2goXG4gICAgICAgICAgbmV3IHhtbC5DUig4KSwgbmV3IHhtbC5UYWcoX1NPVVJDRV9UQUcsIHt9LCB2aXNpdG9yLnNlcmlhbGl6ZShtZXNzYWdlLm5vZGVzKSksXG4gICAgICAgICAgLi4uY29udGV4dFRhZ3MpO1xuXG4gICAgICBpZiAobWVzc2FnZS5kZXNjcmlwdGlvbikge1xuICAgICAgICB0cmFuc1VuaXQuY2hpbGRyZW4ucHVzaChcbiAgICAgICAgICAgIG5ldyB4bWwuQ1IoOCksXG4gICAgICAgICAgICBuZXcgeG1sLlRhZyhcbiAgICAgICAgICAgICAgICAnbm90ZScsIHtwcmlvcml0eTogJzEnLCBmcm9tOiAnZGVzY3JpcHRpb24nfSwgW25ldyB4bWwuVGV4dChtZXNzYWdlLmRlc2NyaXB0aW9uKV0pKTtcbiAgICAgIH1cblxuICAgICAgaWYgKG1lc3NhZ2UubWVhbmluZykge1xuICAgICAgICB0cmFuc1VuaXQuY2hpbGRyZW4ucHVzaChcbiAgICAgICAgICAgIG5ldyB4bWwuQ1IoOCksXG4gICAgICAgICAgICBuZXcgeG1sLlRhZygnbm90ZScsIHtwcmlvcml0eTogJzEnLCBmcm9tOiAnbWVhbmluZyd9LCBbbmV3IHhtbC5UZXh0KG1lc3NhZ2UubWVhbmluZyldKSk7XG4gICAgICB9XG5cbiAgICAgIHRyYW5zVW5pdC5jaGlsZHJlbi5wdXNoKG5ldyB4bWwuQ1IoNikpO1xuXG4gICAgICB0cmFuc1VuaXRzLnB1c2gobmV3IHhtbC5DUig2KSwgdHJhbnNVbml0KTtcbiAgICB9KTtcblxuICAgIGNvbnN0IGJvZHkgPSBuZXcgeG1sLlRhZygnYm9keScsIHt9LCBbLi4udHJhbnNVbml0cywgbmV3IHhtbC5DUig0KV0pO1xuICAgIGNvbnN0IGZpbGUgPSBuZXcgeG1sLlRhZyhcbiAgICAgICAgJ2ZpbGUnLCB7XG4gICAgICAgICAgJ3NvdXJjZS1sYW5ndWFnZSc6IGxvY2FsZSB8fCBfREVGQVVMVF9TT1VSQ0VfTEFORyxcbiAgICAgICAgICBkYXRhdHlwZTogJ3BsYWludGV4dCcsXG4gICAgICAgICAgb3JpZ2luYWw6ICduZzIudGVtcGxhdGUnLFxuICAgICAgICB9LFxuICAgICAgICBbbmV3IHhtbC5DUig0KSwgYm9keSwgbmV3IHhtbC5DUigyKV0pO1xuICAgIGNvbnN0IHhsaWZmID0gbmV3IHhtbC5UYWcoXG4gICAgICAgICd4bGlmZicsIHt2ZXJzaW9uOiBfVkVSU0lPTiwgeG1sbnM6IF9YTUxOU30sIFtuZXcgeG1sLkNSKDIpLCBmaWxlLCBuZXcgeG1sLkNSKCldKTtcblxuICAgIHJldHVybiB4bWwuc2VyaWFsaXplKFtcbiAgICAgIG5ldyB4bWwuRGVjbGFyYXRpb24oe3ZlcnNpb246ICcxLjAnLCBlbmNvZGluZzogJ1VURi04J30pLCBuZXcgeG1sLkNSKCksIHhsaWZmLCBuZXcgeG1sLkNSKClcbiAgICBdKTtcbiAgfVxuXG4gIGxvYWQoY29udGVudDogc3RyaW5nLCB1cmw6IHN0cmluZyk6XG4gICAgICB7bG9jYWxlOiBzdHJpbmcsIGkxOG5Ob2Rlc0J5TXNnSWQ6IHtbbXNnSWQ6IHN0cmluZ106IGkxOG4uTm9kZVtdfX0ge1xuICAgIC8vIHhsaWZmIHRvIHhtbCBub2Rlc1xuICAgIGNvbnN0IHhsaWZmUGFyc2VyID0gbmV3IFhsaWZmUGFyc2VyKCk7XG4gICAgY29uc3Qge2xvY2FsZSwgbXNnSWRUb0h0bWwsIGVycm9yc30gPSB4bGlmZlBhcnNlci5wYXJzZShjb250ZW50LCB1cmwpO1xuXG4gICAgLy8geG1sIG5vZGVzIHRvIGkxOG4gbm9kZXNcbiAgICBjb25zdCBpMThuTm9kZXNCeU1zZ0lkOiB7W21zZ0lkOiBzdHJpbmddOiBpMThuLk5vZGVbXX0gPSB7fTtcbiAgICBjb25zdCBjb252ZXJ0ZXIgPSBuZXcgWG1sVG9JMThuKCk7XG5cbiAgICBPYmplY3Qua2V5cyhtc2dJZFRvSHRtbCkuZm9yRWFjaChtc2dJZCA9PiB7XG4gICAgICBjb25zdCB7aTE4bk5vZGVzLCBlcnJvcnM6IGV9ID0gY29udmVydGVyLmNvbnZlcnQobXNnSWRUb0h0bWxbbXNnSWRdLCB1cmwpO1xuICAgICAgZXJyb3JzLnB1c2goLi4uZSk7XG4gICAgICBpMThuTm9kZXNCeU1zZ0lkW21zZ0lkXSA9IGkxOG5Ob2RlcztcbiAgICB9KTtcblxuICAgIGlmIChlcnJvcnMubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYHhsaWZmIHBhcnNlIGVycm9yczpcXG4ke2Vycm9ycy5qb2luKCdcXG4nKX1gKTtcbiAgICB9XG5cbiAgICByZXR1cm4ge2xvY2FsZTogbG9jYWxlICEsIGkxOG5Ob2Rlc0J5TXNnSWR9O1xuICB9XG5cbiAgZGlnZXN0KG1lc3NhZ2U6IGkxOG4uTWVzc2FnZSk6IHN0cmluZyB7IHJldHVybiBkaWdlc3QobWVzc2FnZSk7IH1cbn1cblxuY2xhc3MgX1dyaXRlVmlzaXRvciBpbXBsZW1lbnRzIGkxOG4uVmlzaXRvciB7XG4gIHZpc2l0VGV4dCh0ZXh0OiBpMThuLlRleHQsIGNvbnRleHQ/OiBhbnkpOiB4bWwuTm9kZVtdIHsgcmV0dXJuIFtuZXcgeG1sLlRleHQodGV4dC52YWx1ZSldOyB9XG5cbiAgdmlzaXRDb250YWluZXIoY29udGFpbmVyOiBpMThuLkNvbnRhaW5lciwgY29udGV4dD86IGFueSk6IHhtbC5Ob2RlW10ge1xuICAgIGNvbnN0IG5vZGVzOiB4bWwuTm9kZVtdID0gW107XG4gICAgY29udGFpbmVyLmNoaWxkcmVuLmZvckVhY2goKG5vZGU6IGkxOG4uTm9kZSkgPT4gbm9kZXMucHVzaCguLi5ub2RlLnZpc2l0KHRoaXMpKSk7XG4gICAgcmV0dXJuIG5vZGVzO1xuICB9XG5cbiAgdmlzaXRJY3UoaWN1OiBpMThuLkljdSwgY29udGV4dD86IGFueSk6IHhtbC5Ob2RlW10ge1xuICAgIGNvbnN0IG5vZGVzID0gW25ldyB4bWwuVGV4dChgeyR7aWN1LmV4cHJlc3Npb25QbGFjZWhvbGRlcn0sICR7aWN1LnR5cGV9LCBgKV07XG5cbiAgICBPYmplY3Qua2V5cyhpY3UuY2FzZXMpLmZvckVhY2goKGM6IHN0cmluZykgPT4ge1xuICAgICAgbm9kZXMucHVzaChuZXcgeG1sLlRleHQoYCR7Y30ge2ApLCAuLi5pY3UuY2FzZXNbY10udmlzaXQodGhpcyksIG5ldyB4bWwuVGV4dChgfSBgKSk7XG4gICAgfSk7XG5cbiAgICBub2Rlcy5wdXNoKG5ldyB4bWwuVGV4dChgfWApKTtcblxuICAgIHJldHVybiBub2RlcztcbiAgfVxuXG4gIHZpc2l0VGFnUGxhY2Vob2xkZXIocGg6IGkxOG4uVGFnUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiB4bWwuTm9kZVtdIHtcbiAgICBjb25zdCBjdHlwZSA9IGdldEN0eXBlRm9yVGFnKHBoLnRhZyk7XG5cbiAgICBpZiAocGguaXNWb2lkKSB7XG4gICAgICAvLyB2b2lkIHRhZ3MgaGF2ZSBubyBjaGlsZHJlbiBub3IgY2xvc2luZyB0YWdzXG4gICAgICByZXR1cm4gW25ldyB4bWwuVGFnKFxuICAgICAgICAgIF9QTEFDRUhPTERFUl9UQUcsIHtpZDogcGguc3RhcnROYW1lLCBjdHlwZSwgJ2VxdWl2LXRleHQnOiBgPCR7cGgudGFnfS8+YH0pXTtcbiAgICB9XG5cbiAgICBjb25zdCBzdGFydFRhZ1BoID1cbiAgICAgICAgbmV3IHhtbC5UYWcoX1BMQUNFSE9MREVSX1RBRywge2lkOiBwaC5zdGFydE5hbWUsIGN0eXBlLCAnZXF1aXYtdGV4dCc6IGA8JHtwaC50YWd9PmB9KTtcbiAgICBjb25zdCBjbG9zZVRhZ1BoID1cbiAgICAgICAgbmV3IHhtbC5UYWcoX1BMQUNFSE9MREVSX1RBRywge2lkOiBwaC5jbG9zZU5hbWUsIGN0eXBlLCAnZXF1aXYtdGV4dCc6IGA8LyR7cGgudGFnfT5gfSk7XG5cbiAgICByZXR1cm4gW3N0YXJ0VGFnUGgsIC4uLnRoaXMuc2VyaWFsaXplKHBoLmNoaWxkcmVuKSwgY2xvc2VUYWdQaF07XG4gIH1cblxuICB2aXNpdFBsYWNlaG9sZGVyKHBoOiBpMThuLlBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogeG1sLk5vZGVbXSB7XG4gICAgcmV0dXJuIFtuZXcgeG1sLlRhZyhfUExBQ0VIT0xERVJfVEFHLCB7aWQ6IHBoLm5hbWUsICdlcXVpdi10ZXh0JzogYHt7JHtwaC52YWx1ZX19fWB9KV07XG4gIH1cblxuICB2aXNpdEljdVBsYWNlaG9sZGVyKHBoOiBpMThuLkljdVBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogeG1sLk5vZGVbXSB7XG4gICAgY29uc3QgZXF1aXZUZXh0ID1cbiAgICAgICAgYHske3BoLnZhbHVlLmV4cHJlc3Npb259LCAke3BoLnZhbHVlLnR5cGV9LCAke09iamVjdC5rZXlzKHBoLnZhbHVlLmNhc2VzKS5tYXAoKHZhbHVlOiBzdHJpbmcpID0+IHZhbHVlICsgJyB7Li4ufScpLmpvaW4oJyAnKX19YDtcbiAgICByZXR1cm4gW25ldyB4bWwuVGFnKF9QTEFDRUhPTERFUl9UQUcsIHtpZDogcGgubmFtZSwgJ2VxdWl2LXRleHQnOiBlcXVpdlRleHR9KV07XG4gIH1cblxuICBzZXJpYWxpemUobm9kZXM6IGkxOG4uTm9kZVtdKTogeG1sLk5vZGVbXSB7XG4gICAgcmV0dXJuIFtdLmNvbmNhdCguLi5ub2Rlcy5tYXAobm9kZSA9PiBub2RlLnZpc2l0KHRoaXMpKSk7XG4gIH1cbn1cblxuLy8gVE9ETyh2aWNiKTogYWRkIGVycm9yIG1hbmFnZW1lbnQgKHN0cnVjdHVyZSlcbi8vIEV4dHJhY3QgbWVzc2FnZXMgYXMgeG1sIG5vZGVzIGZyb20gdGhlIHhsaWZmIGZpbGVcbmNsYXNzIFhsaWZmUGFyc2VyIGltcGxlbWVudHMgbWwuVmlzaXRvciB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF91bml0TWxTdHJpbmcgITogc3RyaW5nIHwgbnVsbDtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2Vycm9ycyAhOiBJMThuRXJyb3JbXTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX21zZ0lkVG9IdG1sICE6IHtbbXNnSWQ6IHN0cmluZ106IHN0cmluZ307XG4gIHByaXZhdGUgX2xvY2FsZTogc3RyaW5nfG51bGwgPSBudWxsO1xuXG4gIHBhcnNlKHhsaWZmOiBzdHJpbmcsIHVybDogc3RyaW5nKSB7XG4gICAgdGhpcy5fdW5pdE1sU3RyaW5nID0gbnVsbDtcbiAgICB0aGlzLl9tc2dJZFRvSHRtbCA9IHt9O1xuXG4gICAgY29uc3QgeG1sID0gbmV3IFhtbFBhcnNlcigpLnBhcnNlKHhsaWZmLCB1cmwpO1xuXG4gICAgdGhpcy5fZXJyb3JzID0geG1sLmVycm9ycztcbiAgICBtbC52aXNpdEFsbCh0aGlzLCB4bWwucm9vdE5vZGVzLCBudWxsKTtcblxuICAgIHJldHVybiB7XG4gICAgICBtc2dJZFRvSHRtbDogdGhpcy5fbXNnSWRUb0h0bWwsXG4gICAgICBlcnJvcnM6IHRoaXMuX2Vycm9ycyxcbiAgICAgIGxvY2FsZTogdGhpcy5fbG9jYWxlLFxuICAgIH07XG4gIH1cblxuICB2aXNpdEVsZW1lbnQoZWxlbWVudDogbWwuRWxlbWVudCwgY29udGV4dDogYW55KTogYW55IHtcbiAgICBzd2l0Y2ggKGVsZW1lbnQubmFtZSkge1xuICAgICAgY2FzZSBfVU5JVF9UQUc6XG4gICAgICAgIHRoaXMuX3VuaXRNbFN0cmluZyA9IG51bGwgITtcbiAgICAgICAgY29uc3QgaWRBdHRyID0gZWxlbWVudC5hdHRycy5maW5kKChhdHRyKSA9PiBhdHRyLm5hbWUgPT09ICdpZCcpO1xuICAgICAgICBpZiAoIWlkQXR0cikge1xuICAgICAgICAgIHRoaXMuX2FkZEVycm9yKGVsZW1lbnQsIGA8JHtfVU5JVF9UQUd9PiBtaXNzZXMgdGhlIFwiaWRcIiBhdHRyaWJ1dGVgKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb25zdCBpZCA9IGlkQXR0ci52YWx1ZTtcbiAgICAgICAgICBpZiAodGhpcy5fbXNnSWRUb0h0bWwuaGFzT3duUHJvcGVydHkoaWQpKSB7XG4gICAgICAgICAgICB0aGlzLl9hZGRFcnJvcihlbGVtZW50LCBgRHVwbGljYXRlZCB0cmFuc2xhdGlvbnMgZm9yIG1zZyAke2lkfWApO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBtbC52aXNpdEFsbCh0aGlzLCBlbGVtZW50LmNoaWxkcmVuLCBudWxsKTtcbiAgICAgICAgICAgIGlmICh0eXBlb2YgdGhpcy5fdW5pdE1sU3RyaW5nID09PSAnc3RyaW5nJykge1xuICAgICAgICAgICAgICB0aGlzLl9tc2dJZFRvSHRtbFtpZF0gPSB0aGlzLl91bml0TWxTdHJpbmc7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICB0aGlzLl9hZGRFcnJvcihlbGVtZW50LCBgTWVzc2FnZSAke2lkfSBtaXNzZXMgYSB0cmFuc2xhdGlvbmApO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICBicmVhaztcblxuICAgICAgLy8gaWdub3JlIHRob3NlIHRhZ3NcbiAgICAgIGNhc2UgX1NPVVJDRV9UQUc6XG4gICAgICBjYXNlIF9TRUdNRU5UX1NPVVJDRV9UQUc6XG4gICAgICBjYXNlIF9BTFRfVFJBTlNfVEFHOlxuICAgICAgICBicmVhaztcblxuICAgICAgY2FzZSBfVEFSR0VUX1RBRzpcbiAgICAgICAgY29uc3QgaW5uZXJUZXh0U3RhcnQgPSBlbGVtZW50LnN0YXJ0U291cmNlU3BhbiAhLmVuZC5vZmZzZXQ7XG4gICAgICAgIGNvbnN0IGlubmVyVGV4dEVuZCA9IGVsZW1lbnQuZW5kU291cmNlU3BhbiAhLnN0YXJ0Lm9mZnNldDtcbiAgICAgICAgY29uc3QgY29udGVudCA9IGVsZW1lbnQuc3RhcnRTb3VyY2VTcGFuICEuc3RhcnQuZmlsZS5jb250ZW50O1xuICAgICAgICBjb25zdCBpbm5lclRleHQgPSBjb250ZW50LnNsaWNlKGlubmVyVGV4dFN0YXJ0LCBpbm5lclRleHRFbmQpO1xuICAgICAgICB0aGlzLl91bml0TWxTdHJpbmcgPSBpbm5lclRleHQ7XG4gICAgICAgIGJyZWFrO1xuXG4gICAgICBjYXNlIF9GSUxFX1RBRzpcbiAgICAgICAgY29uc3QgbG9jYWxlQXR0ciA9IGVsZW1lbnQuYXR0cnMuZmluZCgoYXR0cikgPT4gYXR0ci5uYW1lID09PSAndGFyZ2V0LWxhbmd1YWdlJyk7XG4gICAgICAgIGlmIChsb2NhbGVBdHRyKSB7XG4gICAgICAgICAgdGhpcy5fbG9jYWxlID0gbG9jYWxlQXR0ci52YWx1ZTtcbiAgICAgICAgfVxuICAgICAgICBtbC52aXNpdEFsbCh0aGlzLCBlbGVtZW50LmNoaWxkcmVuLCBudWxsKTtcbiAgICAgICAgYnJlYWs7XG5cbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIC8vIFRPRE8odmljYik6IGFzc2VydCBmaWxlIHN0cnVjdHVyZSwgeGxpZmYgdmVyc2lvblxuICAgICAgICAvLyBGb3Igbm93IG9ubHkgcmVjdXJzZSBvbiB1bmhhbmRsZWQgbm9kZXNcbiAgICAgICAgbWwudmlzaXRBbGwodGhpcywgZWxlbWVudC5jaGlsZHJlbiwgbnVsbCk7XG4gICAgfVxuICB9XG5cbiAgdmlzaXRBdHRyaWJ1dGUoYXR0cmlidXRlOiBtbC5BdHRyaWJ1dGUsIGNvbnRleHQ6IGFueSk6IGFueSB7fVxuXG4gIHZpc2l0VGV4dCh0ZXh0OiBtbC5UZXh0LCBjb250ZXh0OiBhbnkpOiBhbnkge31cblxuICB2aXNpdENvbW1lbnQoY29tbWVudDogbWwuQ29tbWVudCwgY29udGV4dDogYW55KTogYW55IHt9XG5cbiAgdmlzaXRFeHBhbnNpb24oZXhwYW5zaW9uOiBtbC5FeHBhbnNpb24sIGNvbnRleHQ6IGFueSk6IGFueSB7fVxuXG4gIHZpc2l0RXhwYW5zaW9uQ2FzZShleHBhbnNpb25DYXNlOiBtbC5FeHBhbnNpb25DYXNlLCBjb250ZXh0OiBhbnkpOiBhbnkge31cblxuICBwcml2YXRlIF9hZGRFcnJvcihub2RlOiBtbC5Ob2RlLCBtZXNzYWdlOiBzdHJpbmcpOiB2b2lkIHtcbiAgICB0aGlzLl9lcnJvcnMucHVzaChuZXcgSTE4bkVycm9yKG5vZGUuc291cmNlU3BhbiAhLCBtZXNzYWdlKSk7XG4gIH1cbn1cblxuLy8gQ29udmVydCBtbCBub2RlcyAoeGxpZmYgc3ludGF4KSB0byBpMThuIG5vZGVzXG5jbGFzcyBYbWxUb0kxOG4gaW1wbGVtZW50cyBtbC5WaXNpdG9yIHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2Vycm9ycyAhOiBJMThuRXJyb3JbXTtcblxuICBjb252ZXJ0KG1lc3NhZ2U6IHN0cmluZywgdXJsOiBzdHJpbmcpIHtcbiAgICBjb25zdCB4bWxJY3UgPSBuZXcgWG1sUGFyc2VyKCkucGFyc2UobWVzc2FnZSwgdXJsLCB7dG9rZW5pemVFeHBhbnNpb25Gb3JtczogdHJ1ZX0pO1xuICAgIHRoaXMuX2Vycm9ycyA9IHhtbEljdS5lcnJvcnM7XG5cbiAgICBjb25zdCBpMThuTm9kZXMgPSB0aGlzLl9lcnJvcnMubGVuZ3RoID4gMCB8fCB4bWxJY3Uucm9vdE5vZGVzLmxlbmd0aCA9PSAwID9cbiAgICAgICAgW10gOlxuICAgICAgICBbXS5jb25jYXQoLi4ubWwudmlzaXRBbGwodGhpcywgeG1sSWN1LnJvb3ROb2RlcykpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIGkxOG5Ob2RlczogaTE4bk5vZGVzLFxuICAgICAgZXJyb3JzOiB0aGlzLl9lcnJvcnMsXG4gICAgfTtcbiAgfVxuXG4gIHZpc2l0VGV4dCh0ZXh0OiBtbC5UZXh0LCBjb250ZXh0OiBhbnkpIHsgcmV0dXJuIG5ldyBpMThuLlRleHQodGV4dC52YWx1ZSwgdGV4dC5zb3VyY2VTcGFuICEpOyB9XG5cbiAgdmlzaXRFbGVtZW50KGVsOiBtbC5FbGVtZW50LCBjb250ZXh0OiBhbnkpOiBpMThuLlBsYWNlaG9sZGVyfG1sLk5vZGVbXXxudWxsIHtcbiAgICBpZiAoZWwubmFtZSA9PT0gX1BMQUNFSE9MREVSX1RBRykge1xuICAgICAgY29uc3QgbmFtZUF0dHIgPSBlbC5hdHRycy5maW5kKChhdHRyKSA9PiBhdHRyLm5hbWUgPT09ICdpZCcpO1xuICAgICAgaWYgKG5hbWVBdHRyKSB7XG4gICAgICAgIHJldHVybiBuZXcgaTE4bi5QbGFjZWhvbGRlcignJywgbmFtZUF0dHIudmFsdWUsIGVsLnNvdXJjZVNwYW4gISk7XG4gICAgICB9XG5cbiAgICAgIHRoaXMuX2FkZEVycm9yKGVsLCBgPCR7X1BMQUNFSE9MREVSX1RBR30+IG1pc3NlcyB0aGUgXCJpZFwiIGF0dHJpYnV0ZWApO1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgaWYgKGVsLm5hbWUgPT09IF9NQVJLRVJfVEFHKSB7XG4gICAgICByZXR1cm4gW10uY29uY2F0KC4uLm1sLnZpc2l0QWxsKHRoaXMsIGVsLmNoaWxkcmVuKSk7XG4gICAgfVxuXG4gICAgdGhpcy5fYWRkRXJyb3IoZWwsIGBVbmV4cGVjdGVkIHRhZ2ApO1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgdmlzaXRFeHBhbnNpb24oaWN1OiBtbC5FeHBhbnNpb24sIGNvbnRleHQ6IGFueSkge1xuICAgIGNvbnN0IGNhc2VNYXA6IHtbdmFsdWU6IHN0cmluZ106IGkxOG4uTm9kZX0gPSB7fTtcblxuICAgIG1sLnZpc2l0QWxsKHRoaXMsIGljdS5jYXNlcykuZm9yRWFjaCgoYzogYW55KSA9PiB7XG4gICAgICBjYXNlTWFwW2MudmFsdWVdID0gbmV3IGkxOG4uQ29udGFpbmVyKGMubm9kZXMsIGljdS5zb3VyY2VTcGFuKTtcbiAgICB9KTtcblxuICAgIHJldHVybiBuZXcgaTE4bi5JY3UoaWN1LnN3aXRjaFZhbHVlLCBpY3UudHlwZSwgY2FzZU1hcCwgaWN1LnNvdXJjZVNwYW4pO1xuICB9XG5cbiAgdmlzaXRFeHBhbnNpb25DYXNlKGljdUNhc2U6IG1sLkV4cGFuc2lvbkNhc2UsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHtcbiAgICAgIHZhbHVlOiBpY3VDYXNlLnZhbHVlLFxuICAgICAgbm9kZXM6IG1sLnZpc2l0QWxsKHRoaXMsIGljdUNhc2UuZXhwcmVzc2lvbiksXG4gICAgfTtcbiAgfVxuXG4gIHZpc2l0Q29tbWVudChjb21tZW50OiBtbC5Db21tZW50LCBjb250ZXh0OiBhbnkpIHt9XG5cbiAgdmlzaXRBdHRyaWJ1dGUoYXR0cmlidXRlOiBtbC5BdHRyaWJ1dGUsIGNvbnRleHQ6IGFueSkge31cblxuICBwcml2YXRlIF9hZGRFcnJvcihub2RlOiBtbC5Ob2RlLCBtZXNzYWdlOiBzdHJpbmcpOiB2b2lkIHtcbiAgICB0aGlzLl9lcnJvcnMucHVzaChuZXcgSTE4bkVycm9yKG5vZGUuc291cmNlU3BhbiAhLCBtZXNzYWdlKSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0Q3R5cGVGb3JUYWcodGFnOiBzdHJpbmcpOiBzdHJpbmcge1xuICBzd2l0Y2ggKHRhZy50b0xvd2VyQ2FzZSgpKSB7XG4gICAgY2FzZSAnYnInOlxuICAgICAgcmV0dXJuICdsYic7XG4gICAgY2FzZSAnaW1nJzpcbiAgICAgIHJldHVybiAnaW1hZ2UnO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gYHgtJHt0YWd9YDtcbiAgfVxufVxuIl19