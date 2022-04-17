/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import * as html from '../ml_parser/ast';
import { ParseTreeResult } from '../ml_parser/parser';
import * as i18n from './i18n_ast';
import { createI18nMessageFactory } from './i18n_parser';
import { I18nError } from './parse_util';
var _I18N_ATTR = 'i18n';
var _I18N_ATTR_PREFIX = 'i18n-';
var _I18N_COMMENT_PREFIX_REGEXP = /^i18n:?/;
var MEANING_SEPARATOR = '|';
var ID_SEPARATOR = '@@';
var i18nCommentsWarned = false;
/**
 * Extract translatable messages from an html AST
 */
export function extractMessages(nodes, interpolationConfig, implicitTags, implicitAttrs) {
    var visitor = new _Visitor(implicitTags, implicitAttrs);
    return visitor.extract(nodes, interpolationConfig);
}
export function mergeTranslations(nodes, translations, interpolationConfig, implicitTags, implicitAttrs) {
    var visitor = new _Visitor(implicitTags, implicitAttrs);
    return visitor.merge(nodes, translations, interpolationConfig);
}
var ExtractionResult = /** @class */ (function () {
    function ExtractionResult(messages, errors) {
        this.messages = messages;
        this.errors = errors;
    }
    return ExtractionResult;
}());
export { ExtractionResult };
var _VisitorMode;
(function (_VisitorMode) {
    _VisitorMode[_VisitorMode["Extract"] = 0] = "Extract";
    _VisitorMode[_VisitorMode["Merge"] = 1] = "Merge";
})(_VisitorMode || (_VisitorMode = {}));
/**
 * This Visitor is used:
 * 1. to extract all the translatable strings from an html AST (see `extract()`),
 * 2. to replace the translatable strings with the actual translations (see `merge()`)
 *
 * @internal
 */
var _Visitor = /** @class */ (function () {
    function _Visitor(_implicitTags, _implicitAttrs) {
        this._implicitTags = _implicitTags;
        this._implicitAttrs = _implicitAttrs;
    }
    /**
     * Extracts the messages from the tree
     */
    _Visitor.prototype.extract = function (nodes, interpolationConfig) {
        var _this = this;
        this._init(_VisitorMode.Extract, interpolationConfig);
        nodes.forEach(function (node) { return node.visit(_this, null); });
        if (this._inI18nBlock) {
            this._reportError(nodes[nodes.length - 1], 'Unclosed block');
        }
        return new ExtractionResult(this._messages, this._errors);
    };
    /**
     * Returns a tree where all translatable nodes are translated
     */
    _Visitor.prototype.merge = function (nodes, translations, interpolationConfig) {
        this._init(_VisitorMode.Merge, interpolationConfig);
        this._translations = translations;
        // Construct a single fake root element
        var wrapper = new html.Element('wrapper', [], nodes, undefined, undefined, undefined);
        var translatedNode = wrapper.visit(this, null);
        if (this._inI18nBlock) {
            this._reportError(nodes[nodes.length - 1], 'Unclosed block');
        }
        return new ParseTreeResult(translatedNode.children, this._errors);
    };
    _Visitor.prototype.visitExpansionCase = function (icuCase, context) {
        // Parse cases for translatable html attributes
        var expression = html.visitAll(this, icuCase.expression, context);
        if (this._mode === _VisitorMode.Merge) {
            return new html.ExpansionCase(icuCase.value, expression, icuCase.sourceSpan, icuCase.valueSourceSpan, icuCase.expSourceSpan);
        }
    };
    _Visitor.prototype.visitExpansion = function (icu, context) {
        this._mayBeAddBlockChildren(icu);
        var wasInIcu = this._inIcu;
        if (!this._inIcu) {
            // nested ICU messages should not be extracted but top-level translated as a whole
            if (this._isInTranslatableSection) {
                this._addMessage([icu]);
            }
            this._inIcu = true;
        }
        var cases = html.visitAll(this, icu.cases, context);
        if (this._mode === _VisitorMode.Merge) {
            icu = new html.Expansion(icu.switchValue, icu.type, cases, icu.sourceSpan, icu.switchValueSourceSpan);
        }
        this._inIcu = wasInIcu;
        return icu;
    };
    _Visitor.prototype.visitComment = function (comment, context) {
        var isOpening = _isOpeningComment(comment);
        if (isOpening && this._isInTranslatableSection) {
            this._reportError(comment, 'Could not start a block inside a translatable section');
            return;
        }
        var isClosing = _isClosingComment(comment);
        if (isClosing && !this._inI18nBlock) {
            this._reportError(comment, 'Trying to close an unopened block');
            return;
        }
        if (!this._inI18nNode && !this._inIcu) {
            if (!this._inI18nBlock) {
                if (isOpening) {
                    // deprecated from v5 you should use <ng-container i18n> instead of i18n comments
                    if (!i18nCommentsWarned && console && console.warn) {
                        i18nCommentsWarned = true;
                        var details = comment.sourceSpan.details ? ", " + comment.sourceSpan.details : '';
                        // TODO(ocombe): use a log service once there is a public one available
                        console.warn("I18n comments are deprecated, use an <ng-container> element instead (" + comment.sourceSpan.start + details + ")");
                    }
                    this._inI18nBlock = true;
                    this._blockStartDepth = this._depth;
                    this._blockChildren = [];
                    this._blockMeaningAndDesc =
                        comment.value.replace(_I18N_COMMENT_PREFIX_REGEXP, '').trim();
                    this._openTranslatableSection(comment);
                }
            }
            else {
                if (isClosing) {
                    if (this._depth == this._blockStartDepth) {
                        this._closeTranslatableSection(comment, this._blockChildren);
                        this._inI18nBlock = false;
                        var message = this._addMessage(this._blockChildren, this._blockMeaningAndDesc);
                        // merge attributes in sections
                        var nodes = this._translateMessage(comment, message);
                        return html.visitAll(this, nodes);
                    }
                    else {
                        this._reportError(comment, 'I18N blocks should not cross element boundaries');
                        return;
                    }
                }
            }
        }
    };
    _Visitor.prototype.visitText = function (text, context) {
        if (this._isInTranslatableSection) {
            this._mayBeAddBlockChildren(text);
        }
        return text;
    };
    _Visitor.prototype.visitElement = function (el, context) {
        var _this = this;
        this._mayBeAddBlockChildren(el);
        this._depth++;
        var wasInI18nNode = this._inI18nNode;
        var wasInImplicitNode = this._inImplicitNode;
        var childNodes = [];
        var translatedChildNodes = undefined;
        // Extract:
        // - top level nodes with the (implicit) "i18n" attribute if not already in a section
        // - ICU messages
        var i18nAttr = _getI18nAttr(el);
        var i18nMeta = i18nAttr ? i18nAttr.value : '';
        var isImplicit = this._implicitTags.some(function (tag) { return el.name === tag; }) && !this._inIcu &&
            !this._isInTranslatableSection;
        var isTopLevelImplicit = !wasInImplicitNode && isImplicit;
        this._inImplicitNode = wasInImplicitNode || isImplicit;
        if (!this._isInTranslatableSection && !this._inIcu) {
            if (i18nAttr || isTopLevelImplicit) {
                this._inI18nNode = true;
                var message = this._addMessage(el.children, i18nMeta);
                translatedChildNodes = this._translateMessage(el, message);
            }
            if (this._mode == _VisitorMode.Extract) {
                var isTranslatable = i18nAttr || isTopLevelImplicit;
                if (isTranslatable)
                    this._openTranslatableSection(el);
                html.visitAll(this, el.children);
                if (isTranslatable)
                    this._closeTranslatableSection(el, el.children);
            }
        }
        else {
            if (i18nAttr || isTopLevelImplicit) {
                this._reportError(el, 'Could not mark an element as translatable inside a translatable section');
            }
            if (this._mode == _VisitorMode.Extract) {
                // Descend into child nodes for extraction
                html.visitAll(this, el.children);
            }
        }
        if (this._mode === _VisitorMode.Merge) {
            var visitNodes = translatedChildNodes || el.children;
            visitNodes.forEach(function (child) {
                var visited = child.visit(_this, context);
                if (visited && !_this._isInTranslatableSection) {
                    // Do not add the children from translatable sections (= i18n blocks here)
                    // They will be added later in this loop when the block closes (i.e. on `<!-- /i18n -->`)
                    childNodes = childNodes.concat(visited);
                }
            });
        }
        this._visitAttributesOf(el);
        this._depth--;
        this._inI18nNode = wasInI18nNode;
        this._inImplicitNode = wasInImplicitNode;
        if (this._mode === _VisitorMode.Merge) {
            var translatedAttrs = this._translateAttributes(el);
            return new html.Element(el.name, translatedAttrs, childNodes, el.sourceSpan, el.startSourceSpan, el.endSourceSpan);
        }
        return null;
    };
    _Visitor.prototype.visitAttribute = function (attribute, context) {
        throw new Error('unreachable code');
    };
    _Visitor.prototype._init = function (mode, interpolationConfig) {
        this._mode = mode;
        this._inI18nBlock = false;
        this._inI18nNode = false;
        this._depth = 0;
        this._inIcu = false;
        this._msgCountAtSectionStart = undefined;
        this._errors = [];
        this._messages = [];
        this._inImplicitNode = false;
        this._createI18nMessage = createI18nMessageFactory(interpolationConfig);
    };
    // looks for translatable attributes
    _Visitor.prototype._visitAttributesOf = function (el) {
        var _this = this;
        var explicitAttrNameToValue = {};
        var implicitAttrNames = this._implicitAttrs[el.name] || [];
        el.attrs.filter(function (attr) { return attr.name.startsWith(_I18N_ATTR_PREFIX); })
            .forEach(function (attr) { return explicitAttrNameToValue[attr.name.slice(_I18N_ATTR_PREFIX.length)] =
            attr.value; });
        el.attrs.forEach(function (attr) {
            if (attr.name in explicitAttrNameToValue) {
                _this._addMessage([attr], explicitAttrNameToValue[attr.name]);
            }
            else if (implicitAttrNames.some(function (name) { return attr.name === name; })) {
                _this._addMessage([attr]);
            }
        });
    };
    // add a translatable message
    _Visitor.prototype._addMessage = function (ast, msgMeta) {
        if (ast.length == 0 ||
            ast.length == 1 && ast[0] instanceof html.Attribute && !ast[0].value) {
            // Do not create empty messages
            return null;
        }
        var _a = _parseMessageMeta(msgMeta), meaning = _a.meaning, description = _a.description, id = _a.id;
        var message = this._createI18nMessage(ast, meaning, description, id);
        this._messages.push(message);
        return message;
    };
    // Translates the given message given the `TranslationBundle`
    // This is used for translating elements / blocks - see `_translateAttributes` for attributes
    // no-op when called in extraction mode (returns [])
    _Visitor.prototype._translateMessage = function (el, message) {
        if (message && this._mode === _VisitorMode.Merge) {
            var nodes = this._translations.get(message);
            if (nodes) {
                return nodes;
            }
            this._reportError(el, "Translation unavailable for message id=\"" + this._translations.digest(message) + "\"");
        }
        return [];
    };
    // translate the attributes of an element and remove i18n specific attributes
    _Visitor.prototype._translateAttributes = function (el) {
        var _this = this;
        var attributes = el.attrs;
        var i18nParsedMessageMeta = {};
        attributes.forEach(function (attr) {
            if (attr.name.startsWith(_I18N_ATTR_PREFIX)) {
                i18nParsedMessageMeta[attr.name.slice(_I18N_ATTR_PREFIX.length)] =
                    _parseMessageMeta(attr.value);
            }
        });
        var translatedAttributes = [];
        attributes.forEach(function (attr) {
            if (attr.name === _I18N_ATTR || attr.name.startsWith(_I18N_ATTR_PREFIX)) {
                // strip i18n specific attributes
                return;
            }
            if (attr.value && attr.value != '' && i18nParsedMessageMeta.hasOwnProperty(attr.name)) {
                var _a = i18nParsedMessageMeta[attr.name], meaning = _a.meaning, description = _a.description, id = _a.id;
                var message = _this._createI18nMessage([attr], meaning, description, id);
                var nodes = _this._translations.get(message);
                if (nodes) {
                    if (nodes.length == 0) {
                        translatedAttributes.push(new html.Attribute(attr.name, '', attr.sourceSpan));
                    }
                    else if (nodes[0] instanceof html.Text) {
                        var value = nodes[0].value;
                        translatedAttributes.push(new html.Attribute(attr.name, value, attr.sourceSpan));
                    }
                    else {
                        _this._reportError(el, "Unexpected translation for attribute \"" + attr.name + "\" (id=\"" + (id || _this._translations.digest(message)) + "\")");
                    }
                }
                else {
                    _this._reportError(el, "Translation unavailable for attribute \"" + attr.name + "\" (id=\"" + (id || _this._translations.digest(message)) + "\")");
                }
            }
            else {
                translatedAttributes.push(attr);
            }
        });
        return translatedAttributes;
    };
    /**
     * Add the node as a child of the block when:
     * - we are in a block,
     * - we are not inside a ICU message (those are handled separately),
     * - the node is a "direct child" of the block
     */
    _Visitor.prototype._mayBeAddBlockChildren = function (node) {
        if (this._inI18nBlock && !this._inIcu && this._depth == this._blockStartDepth) {
            this._blockChildren.push(node);
        }
    };
    /**
     * Marks the start of a section, see `_closeTranslatableSection`
     */
    _Visitor.prototype._openTranslatableSection = function (node) {
        if (this._isInTranslatableSection) {
            this._reportError(node, 'Unexpected section start');
        }
        else {
            this._msgCountAtSectionStart = this._messages.length;
        }
    };
    Object.defineProperty(_Visitor.prototype, "_isInTranslatableSection", {
        /**
         * A translatable section could be:
         * - the content of translatable element,
         * - nodes between `<!-- i18n -->` and `<!-- /i18n -->` comments
         */
        get: function () {
            return this._msgCountAtSectionStart !== void 0;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * Terminates a section.
     *
     * If a section has only one significant children (comments not significant) then we should not
     * keep the message from this children:
     *
     * `<p i18n="meaning|description">{ICU message}</p>` would produce two messages:
     * - one for the <p> content with meaning and description,
     * - another one for the ICU message.
     *
     * In this case the last message is discarded as it contains less information (the AST is
     * otherwise identical).
     *
     * Note that we should still keep messages extracted from attributes inside the section (ie in the
     * ICU message here)
     */
    _Visitor.prototype._closeTranslatableSection = function (node, directChildren) {
        if (!this._isInTranslatableSection) {
            this._reportError(node, 'Unexpected section end');
            return;
        }
        var startIndex = this._msgCountAtSectionStart;
        var significantChildren = directChildren.reduce(function (count, node) { return count + (node instanceof html.Comment ? 0 : 1); }, 0);
        if (significantChildren == 1) {
            for (var i = this._messages.length - 1; i >= startIndex; i--) {
                var ast = this._messages[i].nodes;
                if (!(ast.length == 1 && ast[0] instanceof i18n.Text)) {
                    this._messages.splice(i, 1);
                    break;
                }
            }
        }
        this._msgCountAtSectionStart = undefined;
    };
    _Visitor.prototype._reportError = function (node, msg) {
        this._errors.push(new I18nError(node.sourceSpan, msg));
    };
    return _Visitor;
}());
function _isOpeningComment(n) {
    return !!(n instanceof html.Comment && n.value && n.value.startsWith('i18n'));
}
function _isClosingComment(n) {
    return !!(n instanceof html.Comment && n.value && n.value === '/i18n');
}
function _getI18nAttr(p) {
    return p.attrs.find(function (attr) { return attr.name === _I18N_ATTR; }) || null;
}
function _parseMessageMeta(i18n) {
    if (!i18n)
        return { meaning: '', description: '', id: '' };
    var idIndex = i18n.indexOf(ID_SEPARATOR);
    var descIndex = i18n.indexOf(MEANING_SEPARATOR);
    var _a = tslib_1.__read((idIndex > -1) ? [i18n.slice(0, idIndex), i18n.slice(idIndex + 2)] : [i18n, ''], 2), meaningAndDesc = _a[0], id = _a[1];
    var _b = tslib_1.__read((descIndex > -1) ?
        [meaningAndDesc.slice(0, descIndex), meaningAndDesc.slice(descIndex + 1)] :
        ['', meaningAndDesc], 2), meaning = _b[0], description = _b[1];
    return { meaning: meaning, description: description, id: id };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXh0cmFjdG9yX21lcmdlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pMThuL2V4dHJhY3Rvcl9tZXJnZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sS0FBSyxJQUFJLE1BQU0sa0JBQWtCLENBQUM7QUFFekMsT0FBTyxFQUFDLGVBQWUsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBQ3BELE9BQU8sS0FBSyxJQUFJLE1BQU0sWUFBWSxDQUFDO0FBQ25DLE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUN2RCxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sY0FBYyxDQUFDO0FBR3ZDLElBQU0sVUFBVSxHQUFHLE1BQU0sQ0FBQztBQUMxQixJQUFNLGlCQUFpQixHQUFHLE9BQU8sQ0FBQztBQUNsQyxJQUFNLDJCQUEyQixHQUFHLFNBQVMsQ0FBQztBQUM5QyxJQUFNLGlCQUFpQixHQUFHLEdBQUcsQ0FBQztBQUM5QixJQUFNLFlBQVksR0FBRyxJQUFJLENBQUM7QUFDMUIsSUFBSSxrQkFBa0IsR0FBRyxLQUFLLENBQUM7QUFFL0I7O0dBRUc7QUFDSCxNQUFNLFVBQVUsZUFBZSxDQUMzQixLQUFrQixFQUFFLG1CQUF3QyxFQUFFLFlBQXNCLEVBQ3BGLGFBQXNDO0lBQ3hDLElBQU0sT0FBTyxHQUFHLElBQUksUUFBUSxDQUFDLFlBQVksRUFBRSxhQUFhLENBQUMsQ0FBQztJQUMxRCxPQUFPLE9BQU8sQ0FBQyxPQUFPLENBQUMsS0FBSyxFQUFFLG1CQUFtQixDQUFDLENBQUM7QUFDckQsQ0FBQztBQUVELE1BQU0sVUFBVSxpQkFBaUIsQ0FDN0IsS0FBa0IsRUFBRSxZQUErQixFQUFFLG1CQUF3QyxFQUM3RixZQUFzQixFQUFFLGFBQXNDO0lBQ2hFLElBQU0sT0FBTyxHQUFHLElBQUksUUFB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SUFDM0MsSUFBTSxTQUFTLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxpQkFBaUIsQ0FBQyxDQUFDO0lBQzVDLElBQUEsdUdBQzZFLEVBRDVFLHNCQUFjLEVBQUUsVUFDNEQsQ0FBQztJQUM5RSxJQUFBOztnQ0FFa0IsRUFGakIsZUFBTyxFQUFFLG1CQUVRLENBQUM7SUFFekIsT0FBTyxFQUFDLE9BQU8sU0FBQSxFQUFFLFdBQVcsYUFBQSxFQUFFLEVBQUUsSUFBQSxFQUFDLENBQUM7QUFDcEMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgaHRtbCBmcm9tICcuLi9tbF9wYXJzZXIvYXN0JztcbmltcG9ydCB7SW50ZXJwb2xhdGlvbkNvbmZpZ30gZnJvbSAnLi4vbWxfcGFyc2VyL2ludGVycG9sYXRpb25fY29uZmlnJztcbmltcG9ydCB7UGFyc2VUcmVlUmVzdWx0fSBmcm9tICcuLi9tbF9wYXJzZXIvcGFyc2VyJztcbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi9pMThuX2FzdCc7XG5pbXBvcnQge2NyZWF0ZUkxOG5NZXNzYWdlRmFjdG9yeX0gZnJvbSAnLi9pMThuX3BhcnNlcic7XG5pbXBvcnQge0kxOG5FcnJvcn0gZnJvbSAnLi9wYXJzZV91dGlsJztcbmltcG9ydCB7VHJhbnNsYXRpb25CdW5kbGV9IGZyb20gJy4vdHJhbnNsYXRpb25fYnVuZGxlJztcblxuY29uc3QgX0kxOE5fQVRUUiA9ICdpMThuJztcbmNvbnN0IF9JMThOX0FUVFJfUFJFRklYID0gJ2kxOG4tJztcbmNvbnN0IF9JMThOX0NPTU1FTlRfUFJFRklYX1JFR0VYUCA9IC9eaTE4bjo/LztcbmNvbnN0IE1FQU5JTkdfU0VQQVJBVE9SID0gJ3wnO1xuY29uc3QgSURfU0VQQVJBVE9SID0gJ0BAJztcbmxldCBpMThuQ29tbWVudHNXYXJuZWQgPSBmYWxzZTtcblxuLyoqXG4gKiBFeHRyYWN0IHRyYW5zbGF0YWJsZSBtZXNzYWdlcyBmcm9tIGFuIGh0bWwgQVNUXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBleHRyYWN0TWVzc2FnZXMoXG4gICAgbm9kZXM6IGh0bWwuTm9kZVtdLCBpbnRlcnBvbGF0aW9uQ29uZmlnOiBJbnRlcnBvbGF0aW9uQ29uZmlnLCBpbXBsaWNpdFRhZ3M6IHN0cmluZ1tdLFxuICAgIGltcGxpY2l0QXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nW119KTogRXh0cmFjdGlvblJlc3VsdCB7XG4gIGNvbnN0IHZpc2l0b3IgPSBuZXcgX1Zpc2l0b3IoaW1wbGljaXRUYWdzLCBpbXBsaWNpdEF0dHJzKTtcbiAgcmV0dXJuIHZpc2l0b3IuZXh0cmFjdChub2RlcywgaW50ZXJwb2xhdGlvbkNvbmZpZyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBtZXJnZVRyYW5zbGF0aW9ucyhcbiAgICBub2RlczogaHRtbC5Ob2RlW10sIHRyYW5zbGF0aW9uczogVHJhbnNsYXRpb25CdW5kbGUsIGludGVycG9sYXRpb25Db25maWc6IEludGVycG9sYXRpb25Db25maWcsXG4gICAgaW1wbGljaXRUYWdzOiBzdHJpbmdbXSwgaW1wbGljaXRBdHRyczoge1trOiBzdHJpbmddOiBzdHJpbmdbXX0pOiBQYXJzZVRyZWVSZXN1bHQge1xuICBjb25zdCB2aXNpdG9yID0gbmV3IF9WaXNpdG9yKGltcGxpY2l0VGFncywgaW1wbGljaXRBdHRycyk7XG4gIHJldHVybiB2aXNpdG9yLm1lcmdlKG5vZGVzLCB0cmFuc2xhdGlvbnMsIGludGVycG9sYXRpb25Db25maWcpO1xufVxuXG5leHBvcnQgY2xhc3MgRXh0cmFjdGlvblJlc3VsdCB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBtZXNzYWdlczogaTE4bi5NZXNzYWdlW10sIHB1YmxpYyBlcnJvcnM6IEkxOG5FcnJvcltdKSB7fVxufVxuXG5lbnVtIF9WaXNpdG9yTW9kZSB7XG4gIEV4dHJhY3QsXG4gIE1lcmdlXG59XG5cbi8qKlxuICogVGhpcyBWaXNpdG9yIGlzIHVzZWQ6XG4gKiAxLiB0byBleHRyYWN0IGFsbCB0aGUgdHJhbnNsYXRhYmxlIHN0cmluZ3MgZnJvbSBhbiBodG1sIEFTVCAoc2VlIGBleHRyYWN0KClgKSxcbiAqIDIuIHRvIHJlcGxhY2UgdGhlIHRyYW5zbGF0YWJsZSBzdHJpbmdzIHdpdGggdGhlIGFjdHVhbCB0cmFuc2xhdGlvbnMgKHNlZSBgbWVyZ2UoKWApXG4gKlxuICogQGludGVybmFsXG4gKi9cbmNsYXNzIF9WaXNpdG9yIGltcGxlbWVudHMgaHRtbC5WaXNpdG9yIHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2RlcHRoICE6IG51bWJlcjtcblxuICAvLyA8ZWwgaTE4bj4uLi48L2VsPlxuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfaW5JMThuTm9kZSAhOiBib29sZWFuO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfaW5JbXBsaWNpdE5vZGUgITogYm9vbGVhbjtcblxuICAvLyA8IS0taTE4bi0tPi4uLjwhLS0vaTE4bi0tPlxuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfaW5JMThuQmxvY2sgITogYm9vbGVhbjtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2Jsb2NrTWVhbmluZ0FuZERlc2MgITogc3RyaW5nO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfYmxvY2tDaGlsZHJlbiAhOiBodG1sLk5vZGVbXTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2Jsb2NrU3RhcnREZXB0aCAhOiBudW1iZXI7XG5cbiAgLy8gezxpY3UgbWVzc2FnZT59XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9pbkljdSAhOiBib29sZWFuO1xuXG4gIC8vIHNldCB0byB2b2lkIDAgd2hlbiBub3QgaW4gYSBzZWN0aW9uXG4gIHByaXZhdGUgX21zZ0NvdW50QXRTZWN0aW9uU3RhcnQ6IG51bWJlcnx1bmRlZmluZWQ7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9lcnJvcnMgITogSTE4bkVycm9yW107XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9tb2RlICE6IF9WaXNpdG9yTW9kZTtcblxuICAvLyBfVmlzaXRvck1vZGUuRXh0cmFjdCBvbmx5XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9tZXNzYWdlcyAhOiBpMThuLk1lc3NhZ2VbXTtcblxuICAvLyBfVmlzaXRvck1vZGUuTWVyZ2Ugb25seVxuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfdHJhbnNsYXRpb25zICE6IFRyYW5zbGF0aW9uQnVuZGxlO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfY3JlYXRlSTE4bk1lc3NhZ2UgITogKFxuICAgICAgbXNnOiBodG1sLk5vZGVbXSwgbWVhbmluZzogc3RyaW5nLCBkZXNjcmlwdGlvbjogc3RyaW5nLCBpZDogc3RyaW5nKSA9PiBpMThuLk1lc3NhZ2U7XG5cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF9pbXBsaWNpdFRhZ3M6IHN0cmluZ1tdLCBwcml2YXRlIF9pbXBsaWNpdEF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ1tdfSkge31cblxuICAvKipcbiAgICogRXh0cmFjdHMgdGhlIG1lc3NhZ2VzIGZyb20gdGhlIHRyZWVcbiAgICovXG4gIGV4dHJhY3Qobm9kZXM6IGh0bWwuTm9kZVtdLCBpbnRlcnBvbGF0aW9uQ29uZmlnOiBJbnRlcnBvbGF0aW9uQ29uZmlnKTogRXh0cmFjdGlvblJlc3VsdCB7XG4gICAgdGhpcy5faW5pdChfVmlzaXRvck1vZGUuRXh0cmFjdCwgaW50ZXJwb2xhdGlvbkNvbmZpZyk7XG5cbiAgICBub2Rlcy5mb3JFYWNoKG5vZGUgPT4gbm9kZS52aXNpdCh0aGlzLCBudWxsKSk7XG5cbiAgICBpZiAodGhpcy5faW5JMThuQmxvY2spIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKG5vZGVzW25vZGVzLmxlbmd0aCAtIDFdLCAnVW5jbG9zZWQgYmxvY2snKTtcbiAgICB9XG5cbiAgICByZXR1cm4gbmV3IEV4dHJhY3Rpb25SZXN1bHQodGhpcy5fbWVzc2FnZXMsIHRoaXMuX2Vycm9ycyk7XG4gIH1cblxuICAvKipcbiAgICogUmV0dXJucyBhIHRyZWUgd2hlcmUgYWxsIHRyYW5zbGF0YWJsZSBub2RlcyBhcmUgdHJhbnNsYXRlZFxuICAgKi9cbiAgbWVyZ2UoXG4gICAgICBub2RlczogaHRtbC5Ob2RlW10sIHRyYW5zbGF0aW9uczogVHJhbnNsYXRpb25CdW5kbGUsXG4gICAgICBpbnRlcnBvbGF0aW9uQ29uZmlnOiBJbnRlcnBvbGF0aW9uQ29uZmlnKTogUGFyc2VUcmVlUmVzdWx0IHtcbiAgICB0aGlzLl9pbml0KF9WaXNpdG9yTW9kZS5NZXJnZSwgaW50ZXJwb2xhdGlvbkNvbmZpZyk7XG4gICAgdGhpcy5fdHJhbnNsYXRpb25zID0gdHJhbnNsYXRpb25zO1xuXG4gICAgLy8gQ29uc3RydWN0IGEgc2luZ2xlIGZha2Ugcm9vdCBlbGVtZW50XG4gICAgY29uc3Qgd3JhcHBlciA9IG5ldyBodG1sLkVsZW1lbnQoJ3dyYXBwZXInLCBbXSwgbm9kZXMsIHVuZGVmaW5lZCAhLCB1bmRlZmluZWQsIHVuZGVmaW5lZCk7XG5cbiAgICBjb25zdCB0cmFuc2xhdGVkTm9kZSA9IHdyYXBwZXIudmlzaXQodGhpcywgbnVsbCk7XG5cbiAgICBpZiAodGhpcy5faW5JMThuQmxvY2spIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKG5vZGVzW25vZGVzLmxlbmd0aCAtIDFdLCAnVW5jbG9zZWQgYmxvY2snKTtcbiAgICB9XG5cbiAgICByZXR1cm4gbmV3IFBhcnNlVHJlZVJlc3VsdCh0cmFuc2xhdGVkTm9kZS5jaGlsZHJlbiwgdGhpcy5fZXJyb3JzKTtcbiAgfVxuXG4gIHZpc2l0RXhwYW5zaW9uQ2FzZShpY3VDYXNlOiBodG1sLkV4cGFuc2lvbkNhc2UsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgLy8gUGFyc2UgY2FzZXMgZm9yIHRyYW5zbGF0YWJsZSBodG1sIGF0dHJpYnV0ZXNcbiAgICBjb25zdCBleHByZXNzaW9uID0gaHRtbC52aXNpdEFsbCh0aGlzLCBpY3VDYXNlLmV4cHJlc3Npb24sIGNvbnRleHQpO1xuXG4gICAgaWYgKHRoaXMuX21vZGUgPT09IF9WaXNpdG9yTW9kZS5NZXJnZSkge1xuICAgICAgcmV0dXJuIG5ldyBodG1sLkV4cGFuc2lvbkNhc2UoXG4gICAgICAgICAgaWN1Q2FzZS52YWx1ZSwgZXhwcmVzc2lvbiwgaWN1Q2FzZS5zb3VyY2VTcGFuLCBpY3VDYXNlLnZhbHVlU291cmNlU3BhbixcbiAgICAgICAgICBpY3VDYXNlLmV4cFNvdXJjZVNwYW4pO1xuICAgIH1cbiAgfVxuXG4gIHZpc2l0RXhwYW5zaW9uKGljdTogaHRtbC5FeHBhbnNpb24sIGNvbnRleHQ6IGFueSk6IGh0bWwuRXhwYW5zaW9uIHtcbiAgICB0aGlzLl9tYXlCZUFkZEJsb2NrQ2hpbGRyZW4oaWN1KTtcblxuICAgIGNvbnN0IHdhc0luSWN1ID0gdGhpcy5faW5JY3U7XG5cbiAgICBpZiAoIXRoaXMuX2luSWN1KSB7XG4gICAgICAvLyBuZXN0ZWQgSUNVIG1lc3NhZ2VzIHNob3VsZCBub3QgYmUgZXh0cmFjdGVkIGJ1dCB0b3AtbGV2ZWwgdHJhbnNsYXRlZCBhcyBhIHdob2xlXG4gICAgICBpZiAodGhpcy5faXNJblRyYW5zbGF0YWJsZVNlY3Rpb24pIHtcbiAgICAgICAgdGhpcy5fYWRkTWVzc2FnZShbaWN1XSk7XG4gICAgICB9XG4gICAgICB0aGlzLl9pbkljdSA9IHRydWU7XG4gICAgfVxuXG4gICAgY29uc3QgY2FzZXMgPSBodG1sLnZpc2l0QWxsKHRoaXMsIGljdS5jYXNlcywgY29udGV4dCk7XG5cbiAgICBpZiAodGhpcy5fbW9kZSA9PT0gX1Zpc2l0b3JNb2RlLk1lcmdlKSB7XG4gICAgICBpY3UgPSBuZXcgaHRtbC5FeHBhbnNpb24oXG4gICAgICAgICAgaWN1LnN3aXRjaFZhbHVlLCBpY3UudHlwZSwgY2FzZXMsIGljdS5zb3VyY2VTcGFuLCBpY3Uuc3dpdGNoVmFsdWVTb3VyY2VTcGFuKTtcbiAgICB9XG5cbiAgICB0aGlzLl9pbkljdSA9IHdhc0luSWN1O1xuXG4gICAgcmV0dXJuIGljdTtcbiAgfVxuXG4gIHZpc2l0Q29tbWVudChjb21tZW50OiBodG1sLkNvbW1lbnQsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgY29uc3QgaXNPcGVuaW5nID0gX2lzT3BlbmluZ0NvbW1lbnQoY29tbWVudCk7XG5cbiAgICBpZiAoaXNPcGVuaW5nICYmIHRoaXMuX2lzSW5UcmFuc2xhdGFibGVTZWN0aW9uKSB7XG4gICAgICB0aGlzLl9yZXBvcnRFcnJvcihjb21tZW50LCAnQ291bGQgbm90IHN0YXJ0IGEgYmxvY2sgaW5zaWRlIGEgdHJhbnNsYXRhYmxlIHNlY3Rpb24nKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBpc0Nsb3NpbmcgPSBfaXNDbG9zaW5nQ29tbWVudChjb21tZW50KTtcblxuICAgIGlmIChpc0Nsb3NpbmcgJiYgIXRoaXMuX2luSTE4bkJsb2NrKSB7XG4gICAgICB0aGlzLl9yZXBvcnRFcnJvcihjb21tZW50LCAnVHJ5aW5nIHRvIGNsb3NlIGFuIHVub3BlbmVkIGJsb2NrJyk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgaWYgKCF0aGlzLl9pbkkxOG5Ob2RlICYmICF0aGlzLl9pbkljdSkge1xuICAgICAgaWYgKCF0aGlzLl9pbkkxOG5CbG9jaykge1xuICAgICAgICBpZiAoaXNPcGVuaW5nKSB7XG4gICAgICAgICAgLy8gZGVwcmVjYXRlZCBmcm9tIHY1IHlvdSBzaG91bGQgdXNlIDxuZy1jb250YWluZXIgaTE4bj4gaW5zdGVhZCBvZiBpMThuIGNvbW1lbnRzXG4gICAgICAgICAgaWYgKCFpMThuQ29tbWVudHNXYXJuZWQgJiYgPGFueT5jb25zb2xlICYmIDxhbnk+Y29uc29sZS53YXJuKSB7XG4gICAgICAgICAgICBpMThuQ29tbWVudHNXYXJuZWQgPSB0cnVlO1xuICAgICAgICAgICAgY29uc3QgZGV0YWlscyA9IGNvbW1lbnQuc291cmNlU3Bhbi5kZXRhaWxzID8gYCwgJHtjb21tZW50LnNvdXJjZVNwYW4uZGV0YWlsc31gIDogJyc7XG4gICAgICAgICAgICAvLyBUT0RPKG9jb21iZSk6IHVzZSBhIGxvZyBzZXJ2aWNlIG9uY2UgdGhlcmUgaXMgYSBwdWJsaWMgb25lIGF2YWlsYWJsZVxuICAgICAgICAgICAgY29uc29sZS53YXJuKFxuICAgICAgICAgICAgICAgIGBJMThuIGNvbW1lbnRzIGFyZSBkZXByZWNhdGVkLCB1c2UgYW4gPG5nLWNvbnRhaW5lcj4gZWxlbWVudCBpbnN0ZWFkICgke2NvbW1lbnQuc291cmNlU3Bhbi5zdGFydH0ke2RldGFpbHN9KWApO1xuICAgICAgICAgIH1cbiAgICAgICAgICB0aGlzLl9pbkkxOG5CbG9jayA9IHRydWU7XG4gICAgICAgICAgdGhpcy5fYmxvY2tTdGFydERlcHRoID0gdGhpcy5fZGVwdGg7XG4gICAgICAgICAgdGhpcy5fYmxvY2tDaGlsZHJlbiA9IFtdO1xuICAgICAgICAgIHRoaXMuX2Jsb2NrTWVhbmluZ0FuZERlc2MgPVxuICAgICAgICAgICAgICBjb21tZW50LnZhbHVlICEucmVwbGFjZShfSTE4Tl9DT01NRU5UX1BSRUZJWF9SRUdFWFAsICcnKS50cmltKCk7XG4gICAgICAgICAgdGhpcy5fb3BlblRyYW5zbGF0YWJsZVNlY3Rpb24oY29tbWVudCk7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGlmIChpc0Nsb3NpbmcpIHtcbiAgICAgICAgICBpZiAodGhpcy5fZGVwdGggPT0gdGhpcy5fYmxvY2tTdGFydERlcHRoKSB7XG4gICAgICAgICAgICB0aGlzLl9jbG9zZVRyYW5zbGF0YWJsZVNlY3Rpb24oY29tbWVudCwgdGhpcy5fYmxvY2tDaGlsZHJlbik7XG4gICAgICAgICAgICB0aGlzLl9pbkkxOG5CbG9jayA9IGZhbHNlO1xuICAgICAgICAgICAgY29uc3QgbWVzc2FnZSA9IHRoaXMuX2FkZE1lc3NhZ2UodGhpcy5fYmxvY2tDaGlsZHJlbiwgdGhpcy5fYmxvY2tNZWFuaW5nQW5kRGVzYykgITtcbiAgICAgICAgICAgIC8vIG1lcmdlIGF0dHJpYnV0ZXMgaW4gc2VjdGlvbnNcbiAgICAgICAgICAgIGNvbnN0IG5vZGVzID0gdGhpcy5fdHJhbnNsYXRlTWVzc2FnZShjb21tZW50LCBtZXNzYWdlKTtcbiAgICAgICAgICAgIHJldHVybiBodG1sLnZpc2l0QWxsKHRoaXMsIG5vZGVzKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgdGhpcy5fcmVwb3J0RXJyb3IoY29tbWVudCwgJ0kxOE4gYmxvY2tzIHNob3VsZCBub3QgY3Jvc3MgZWxlbWVudCBib3VuZGFyaWVzJyk7XG4gICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgdmlzaXRUZXh0KHRleHQ6IGh0bWwuVGV4dCwgY29udGV4dDogYW55KTogaHRtbC5UZXh0IHtcbiAgICBpZiAodGhpcy5faXNJblRyYW5zbGF0YWJsZVNlY3Rpb24pIHtcbiAgICAgIHRoaXMuX21heUJlQWRkQmxvY2tDaGlsZHJlbih0ZXh0KTtcbiAgICB9XG4gICAgcmV0dXJuIHRleHQ7XG4gIH1cblxuICB2aXNpdEVsZW1lbnQoZWw6IGh0bWwuRWxlbWVudCwgY29udGV4dDogYW55KTogaHRtbC5FbGVtZW50fG51bGwge1xuICAgIHRoaXMuX21heUJlQWRkQmxvY2tDaGlsZHJlbihlbCk7XG4gICAgdGhpcy5fZGVwdGgrKztcbiAgICBjb25zdCB3YXNJbkkxOG5Ob2RlID0gdGhpcy5faW5JMThuTm9kZTtcbiAgICBjb25zdCB3YXNJbkltcGxpY2l0Tm9kZSA9IHRoaXMuX2luSW1wbGljaXROb2RlO1xuICAgIGxldCBjaGlsZE5vZGVzOiBodG1sLk5vZGVbXSA9IFtdO1xuICAgIGxldCB0cmFuc2xhdGVkQ2hpbGROb2RlczogaHRtbC5Ob2RlW10gPSB1bmRlZmluZWQgITtcblxuICAgIC8vIEV4dHJhY3Q6XG4gICAgLy8gLSB0b3AgbGV2ZWwgbm9kZXMgd2l0aCB0aGUgKGltcGxpY2l0KSBcImkxOG5cIiBhdHRyaWJ1dGUgaWYgbm90IGFscmVhZHkgaW4gYSBzZWN0aW9uXG4gICAgLy8gLSBJQ1UgbWVzc2FnZXNcbiAgICBjb25zdCBpMThuQXR0ciA9IF9nZXRJMThuQXR0cihlbCk7XG4gICAgY29uc3QgaTE4bk1ldGEgPSBpMThuQXR0ciA/IGkxOG5BdHRyLnZhbHVlIDogJyc7XG4gICAgY29uc3QgaXNJbXBsaWNpdCA9IHRoaXMuX2ltcGxpY2l0VGFncy5zb21lKHRhZyA9PiBlbC5uYW1lID09PSB0YWcpICYmICF0aGlzLl9pbkljdSAmJlxuICAgICAgICAhdGhpcy5faXNJblRyYW5zbGF0YWJsZVNlY3Rpb247XG4gICAgY29uc3QgaXNUb3BMZXZlbEltcGxpY2l0ID0gIXdhc0luSW1wbGljaXROb2RlICYmIGlzSW1wbGljaXQ7XG4gICAgdGhpcy5faW5JbXBsaWNpdE5vZGUgPSB3YXNJbkltcGxpY2l0Tm9kZSB8fCBpc0ltcGxpY2l0O1xuXG4gICAgaWYgKCF0aGlzLl9pc0luVHJhbnNsYXRhYmxlU2VjdGlvbiAmJiAhdGhpcy5faW5JY3UpIHtcbiAgICAgIGlmIChpMThuQXR0ciB8fCBpc1RvcExldmVsSW1wbGljaXQpIHtcbiAgICAgICAgdGhpcy5faW5JMThuTm9kZSA9IHRydWU7XG4gICAgICAgIGNvbnN0IG1lc3NhZ2UgPSB0aGlzLl9hZGRNZXNzYWdlKGVsLmNoaWxkcmVuLCBpMThuTWV0YSkgITtcbiAgICAgICAgdHJhbnNsYXRlZENoaWxkTm9kZXMgPSB0aGlzLl90cmFuc2xhdGVNZXNzYWdlKGVsLCBtZXNzYWdlKTtcbiAgICAgIH1cblxuICAgICAgaWYgKHRoaXMuX21vZGUgPT0gX1Zpc2l0b3JNb2RlLkV4dHJhY3QpIHtcbiAgICAgICAgY29uc3QgaXNUcmFuc2xhdGFibGUgPSBpMThuQXR0ciB8fCBpc1RvcExldmVsSW1wbGljaXQ7XG4gICAgICAgIGlmIChpc1RyYW5zbGF0YWJsZSkgdGhpcy5fb3BlblRyYW5zbGF0YWJsZVNlY3Rpb24oZWwpO1xuICAgICAgICBodG1sLnZpc2l0QWxsKHRoaXMsIGVsLmNoaWxkcmVuKTtcbiAgICAgICAgaWYgKGlzVHJhbnNsYXRhYmxlKSB0aGlzLl9jbG9zZVRyYW5zbGF0YWJsZVNlY3Rpb24oZWwsIGVsLmNoaWxkcmVuKTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKGkxOG5BdHRyIHx8IGlzVG9wTGV2ZWxJbXBsaWNpdCkge1xuICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICAgIGVsLCAnQ291bGQgbm90IG1hcmsgYW4gZWxlbWVudCBhcyB0cmFuc2xhdGFibGUgaW5zaWRlIGEgdHJhbnNsYXRhYmxlIHNlY3Rpb24nKTtcbiAgICAgIH1cblxuICAgICAgaWYgKHRoaXMuX21vZGUgPT0gX1Zpc2l0b3JNb2RlLkV4dHJhY3QpIHtcbiAgICAgICAgLy8gRGVzY2VuZCBpbnRvIGNoaWxkIG5vZGVzIGZvciBleHRyYWN0aW9uXG4gICAgICAgIGh0bWwudmlzaXRBbGwodGhpcywgZWwuY2hpbGRyZW4pO1xuICAgICAgfVxuICAgIH1cblxuICAgIGlmICh0aGlzLl9tb2RlID09PSBfVmlzaXRvck1vZGUuTWVyZ2UpIHtcbiAgICAgIGNvbnN0IHZpc2l0Tm9kZXMgPSB0cmFuc2xhdGVkQ2hpbGROb2RlcyB8fCBlbC5jaGlsZHJlbjtcbiAgICAgIHZpc2l0Tm9kZXMuZm9yRWFjaChjaGlsZCA9PiB7XG4gICAgICAgIGNvbnN0IHZpc2l0ZWQgPSBjaGlsZC52aXNpdCh0aGlzLCBjb250ZXh0KTtcbiAgICAgICAgaWYgKHZpc2l0ZWQgJiYgIXRoaXMuX2lzSW5UcmFuc2xhdGFibGVTZWN0aW9uKSB7XG4gICAgICAgICAgLy8gRG8gbm90IGFkZCB0aGUgY2hpbGRyZW4gZnJvbSB0cmFuc2xhdGFibGUgc2VjdGlvbnMgKD0gaTE4biBibG9ja3MgaGVyZSlcbiAgICAgICAgICAvLyBUaGV5IHdpbGwgYmUgYWRkZWQgbGF0ZXIgaW4gdGhpcyBsb29wIHdoZW4gdGhlIGJsb2NrIGNsb3NlcyAoaS5lLiBvbiBgPCEtLSAvaTE4biAtLT5gKVxuICAgICAgICAgIGNoaWxkTm9kZXMgPSBjaGlsZE5vZGVzLmNvbmNhdCh2aXNpdGVkKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuXG4gICAgdGhpcy5fdmlzaXRBdHRyaWJ1dGVzT2YoZWwpO1xuXG4gICAgdGhpcy5fZGVwdGgtLTtcbiAgICB0aGlzLl9pbkkxOG5Ob2RlID0gd2FzSW5JMThuTm9kZTtcbiAgICB0aGlzLl9pbkltcGxpY2l0Tm9kZSA9IHdhc0luSW1wbGljaXROb2RlO1xuXG4gICAgaWYgKHRoaXMuX21vZGUgPT09IF9WaXNpdG9yTW9kZS5NZXJnZSkge1xuICAgICAgY29uc3QgdHJhbnNsYXRlZEF0dHJzID0gdGhpcy5fdHJhbnNsYXRlQXR0cmlidXRlcyhlbCk7XG4gICAgICByZXR1cm4gbmV3IGh0bWwuRWxlbWVudChcbiAgICAgICAgICBlbC5uYW1lLCB0cmFuc2xhdGVkQXR0cnMsIGNoaWxkTm9kZXMsIGVsLnNvdXJjZVNwYW4sIGVsLnN0YXJ0U291cmNlU3BhbixcbiAgICAgICAgICBlbC5lbmRTb3VyY2VTcGFuKTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICB2aXNpdEF0dHJpYnV0ZShhdHRyaWJ1dGU6IGh0bWwuQXR0cmlidXRlLCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIHRocm93IG5ldyBFcnJvcigndW5yZWFjaGFibGUgY29kZScpO1xuICB9XG5cbiAgcHJpdmF0ZSBfaW5pdChtb2RlOiBfVmlzaXRvck1vZGUsIGludGVycG9sYXRpb25Db25maWc6IEludGVycG9sYXRpb25Db25maWcpOiB2b2lkIHtcbiAgICB0aGlzLl9tb2RlID0gbW9kZTtcbiAgICB0aGlzLl9pbkkxOG5CbG9jayA9IGZhbHNlO1xuICAgIHRoaXMuX2luSTE4bk5vZGUgPSBmYWxzZTtcbiAgICB0aGlzLl9kZXB0aCA9IDA7XG4gICAgdGhpcy5faW5JY3UgPSBmYWxzZTtcbiAgICB0aGlzLl9tc2dDb3VudEF0U2VjdGlvblN0YXJ0ID0gdW5kZWZpbmVkO1xuICAgIHRoaXMuX2Vycm9ycyA9IFtdO1xuICAgIHRoaXMuX21lc3NhZ2VzID0gW107XG4gICAgdGhpcy5faW5JbXBsaWNpdE5vZGUgPSBmYWxzZTtcbiAgICB0aGlzLl9jcmVhdGVJMThuTWVzc2FnZSA9IGNyZWF0ZUkxOG5NZXNzYWdlRmFjdG9yeShpbnRlcnBvbGF0aW9uQ29uZmlnKTtcbiAgfVxuXG4gIC8vIGxvb2tzIGZvciB0cmFuc2xhdGFibGUgYXR0cmlidXRlc1xuICBwcml2YXRlIF92aXNpdEF0dHJpYnV0ZXNPZihlbDogaHRtbC5FbGVtZW50KTogdm9pZCB7XG4gICAgY29uc3QgZXhwbGljaXRBdHRyTmFtZVRvVmFsdWU6IHtbazogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICAgIGNvbnN0IGltcGxpY2l0QXR0ck5hbWVzOiBzdHJpbmdbXSA9IHRoaXMuX2ltcGxpY2l0QXR0cnNbZWwubmFtZV0gfHwgW107XG5cbiAgICBlbC5hdHRycy5maWx0ZXIoYXR0ciA9PiBhdHRyLm5hbWUuc3RhcnRzV2l0aChfSTE4Tl9BVFRSX1BSRUZJWCkpXG4gICAgICAgIC5mb3JFYWNoKFxuICAgICAgICAgICAgYXR0ciA9PiBleHBsaWNpdEF0dHJOYW1lVG9WYWx1ZVthdHRyLm5hbWUuc2xpY2UoX0kxOE5fQVRUUl9QUkVGSVgubGVuZ3RoKV0gPVxuICAgICAgICAgICAgICAgIGF0dHIudmFsdWUpO1xuXG4gICAgZWwuYXR0cnMuZm9yRWFjaChhdHRyID0+IHtcbiAgICAgIGlmIChhdHRyLm5hbWUgaW4gZXhwbGljaXRBdHRyTmFtZVRvVmFsdWUpIHtcbiAgICAgICAgdGhpcy5fYWRkTWVzc2FnZShbYXR0cl0sIGV4cGxpY2l0QXR0ck5hbWVUb1ZhbHVlW2F0dHIubmFtZV0pO1xuICAgICAgfSBlbHNlIGlmIChpbXBsaWNpdEF0dHJOYW1lcy5zb21lKG5hbWUgPT4gYXR0ci5uYW1lID09PSBuYW1lKSkge1xuICAgICAgICB0aGlzLl9hZGRNZXNzYWdlKFthdHRyXSk7XG4gICAgICB9XG4gICAgfSk7XG4gIH1cblxuICAvLyBhZGQgYSB0cmFuc2xhdGFibGUgbWVzc2FnZVxuICBwcml2YXRlIF9hZGRNZXNzYWdlKGFzdDogaHRtbC5Ob2RlW10sIG1zZ01ldGE/OiBzdHJpbmcpOiBpMThuLk1lc3NhZ2V8bnVsbCB7XG4gICAgaWYgKGFzdC5sZW5ndGggPT0gMCB8fFxuICAgICAgICBhc3QubGVuZ3RoID09IDEgJiYgYXN0WzBdIGluc3RhbmNlb2YgaHRtbC5BdHRyaWJ1dGUgJiYgISg8aHRtbC5BdHRyaWJ1dGU+YXN0WzBdKS52YWx1ZSkge1xuICAgICAgLy8gRG8gbm90IGNyZWF0ZSBlbXB0eSBtZXNzYWdlc1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgY29uc3Qge21lYW5pbmcsIGRlc2NyaXB0aW9uLCBpZH0gPSBfcGFyc2VNZXNzYWdlTWV0YShtc2dNZXRhKTtcbiAgICBjb25zdCBtZXNzYWdlID0gdGhpcy5fY3JlYXRlSTE4bk1lc3NhZ2UoYXN0LCBtZWFuaW5nLCBkZXNjcmlwdGlvbiwgaWQpO1xuICAgIHRoaXMuX21lc3NhZ2VzLnB1c2gobWVzc2FnZSk7XG4gICAgcmV0dXJuIG1lc3NhZ2U7XG4gIH1cblxuICAvLyBUcmFuc2xhdGVzIHRoZSBnaXZlbiBtZXNzYWdlIGdpdmVuIHRoZSBgVHJhbnNsYXRpb25CdW5kbGVgXG4gIC8vIFRoaXMgaXMgdXNlZCBmb3IgdHJhbnNsYXRpbmcgZWxlbWVudHMgLyBibG9ja3MgLSBzZWUgYF90cmFuc2xhdGVBdHRyaWJ1dGVzYCBmb3IgYXR0cmlidXRlc1xuICAvLyBuby1vcCB3aGVuIGNhbGxlZCBpbiBleHRyYWN0aW9uIG1vZGUgKHJldHVybnMgW10pXG4gIHByaXZhdGUgX3RyYW5zbGF0ZU1lc3NhZ2UoZWw6IGh0bWwuTm9kZSwgbWVzc2FnZTogaTE4bi5NZXNzYWdlKTogaHRtbC5Ob2RlW10ge1xuICAgIGlmIChtZXNzYWdlICYmIHRoaXMuX21vZGUgPT09IF9WaXNpdG9yTW9kZS5NZXJnZSkge1xuICAgICAgY29uc3Qgbm9kZXMgPSB0aGlzLl90cmFuc2xhdGlvbnMuZ2V0KG1lc3NhZ2UpO1xuXG4gICAgICBpZiAobm9kZXMpIHtcbiAgICAgICAgcmV0dXJuIG5vZGVzO1xuICAgICAgfVxuXG4gICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICBlbCwgYFRyYW5zbGF0aW9uIHVuYXZhaWxhYmxlIGZvciBtZXNzYWdlIGlkPVwiJHt0aGlzLl90cmFuc2xhdGlvbnMuZGlnZXN0KG1lc3NhZ2UpfVwiYCk7XG4gICAgfVxuXG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgLy8gdHJhbnNsYXRlIHRoZSBhdHRyaWJ1dGVzIG9mIGFuIGVsZW1lbnQgYW5kIHJlbW92ZSBpMThuIHNwZWNpZmljIGF0dHJpYnV0ZXNcbiAgcHJpdmF0ZSBfdHJhbnNsYXRlQXR0cmlidXRlcyhlbDogaHRtbC5FbGVtZW50KTogaHRtbC5BdHRyaWJ1dGVbXSB7XG4gICAgY29uc3QgYXR0cmlidXRlcyA9IGVsLmF0dHJzO1xuICAgIGNvbnN0IGkxOG5QYXJzZWRNZXNzYWdlTWV0YTpcbiAgICAgICAge1tuYW1lOiBzdHJpbmddOiB7bWVhbmluZzogc3RyaW5nLCBkZXNjcmlwdGlvbjogc3RyaW5nLCBpZDogc3RyaW5nfX0gPSB7fTtcblxuICAgIGF0dHJpYnV0ZXMuZm9yRWFjaChhdHRyID0+IHtcbiAgICAgIGlmIChhdHRyLm5hbWUuc3RhcnRzV2l0aChfSTE4Tl9BVFRSX1BSRUZJWCkpIHtcbiAgICAgICAgaTE4blBhcnNlZE1lc3NhZ2VNZXRhW2F0dHIubmFtZS5zbGljZShfSTE4Tl9BVFRSX1BSRUZJWC5sZW5ndGgpXSA9XG4gICAgICAgICAgICBfcGFyc2VNZXNzYWdlTWV0YShhdHRyLnZhbHVlKTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIGNvbnN0IHRyYW5zbGF0ZWRBdHRyaWJ1dGVzOiBodG1sLkF0dHJpYnV0ZVtdID0gW107XG5cbiAgICBhdHRyaWJ1dGVzLmZvckVhY2goKGF0dHIpID0+IHtcbiAgICAgIGlmIChhdHRyLm5hbWUgPT09IF9JMThOX0FUVFIgfHwgYXR0ci5uYW1lLnN0YXJ0c1dpdGgoX0kxOE5fQVRUUl9QUkVGSVgpKSB7XG4gICAgICAgIC8vIHN0cmlwIGkxOG4gc3BlY2lmaWMgYXR0cmlidXRlc1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG5cbiAgICAgIGlmIChhdHRyLnZhbHVlICYmIGF0dHIudmFsdWUgIT0gJycgJiYgaTE4blBhcnNlZE1lc3NhZ2VNZXRhLmhhc093blByb3BlcnR5KGF0dHIubmFtZSkpIHtcbiAgICAgICAgY29uc3Qge21lYW5pbmcsIGRlc2NyaXB0aW9uLCBpZH0gPSBpMThuUGFyc2VkTWVzc2FnZU1ldGFbYXR0ci5uYW1lXTtcbiAgICAgICAgY29uc3QgbWVzc2FnZTogaTE4bi5NZXNzYWdlID0gdGhpcy5fY3JlYXRlSTE4bk1lc3NhZ2UoW2F0dHJdLCBtZWFuaW5nLCBkZXNjcmlwdGlvbiwgaWQpO1xuICAgICAgICBjb25zdCBub2RlcyA9IHRoaXMuX3RyYW5zbGF0aW9ucy5nZXQobWVzc2FnZSk7XG4gICAgICAgIGlmIChub2Rlcykge1xuICAgICAgICAgIGlmIChub2Rlcy5sZW5ndGggPT0gMCkge1xuICAgICAgICAgICAgdHJhbnNsYXRlZEF0dHJpYnV0ZXMucHVzaChuZXcgaHRtbC5BdHRyaWJ1dGUoYXR0ci5uYW1lLCAnJywgYXR0ci5zb3VyY2VTcGFuKSk7XG4gICAgICAgICAgfSBlbHNlIGlmIChub2Rlc1swXSBpbnN0YW5jZW9mIGh0bWwuVGV4dCkge1xuICAgICAgICAgICAgY29uc3QgdmFsdWUgPSAobm9kZXNbMF0gYXMgaHRtbC5UZXh0KS52YWx1ZTtcbiAgICAgICAgICAgIHRyYW5zbGF0ZWRBdHRyaWJ1dGVzLnB1c2gobmV3IGh0bWwuQXR0cmlidXRlKGF0dHIubmFtZSwgdmFsdWUsIGF0dHIuc291cmNlU3BhbikpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICAgICAgICBlbCxcbiAgICAgICAgICAgICAgICBgVW5leHBlY3RlZCB0cmFuc2xhdGlvbiBmb3IgYXR0cmlidXRlIFwiJHthdHRyLm5hbWV9XCIgKGlkPVwiJHtpZCB8fCB0aGlzLl90cmFuc2xhdGlvbnMuZGlnZXN0KG1lc3NhZ2UpfVwiKWApO1xuICAgICAgICAgIH1cbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihcbiAgICAgICAgICAgICAgZWwsXG4gICAgICAgICAgICAgIGBUcmFuc2xhdGlvbiB1bmF2YWlsYWJsZSBmb3IgYXR0cmlidXRlIFwiJHthdHRyLm5hbWV9XCIgKGlkPVwiJHtpZCB8fCB0aGlzLl90cmFuc2xhdGlvbnMuZGlnZXN0KG1lc3NhZ2UpfVwiKWApO1xuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0cmFuc2xhdGVkQXR0cmlidXRlcy5wdXNoKGF0dHIpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgcmV0dXJuIHRyYW5zbGF0ZWRBdHRyaWJ1dGVzO1xuICB9XG5cblxuICAvKipcbiAgICogQWRkIHRoZSBub2RlIGFzIGEgY2hpbGQgb2YgdGhlIGJsb2NrIHdoZW46XG4gICAqIC0gd2UgYXJlIGluIGEgYmxvY2ssXG4gICAqIC0gd2UgYXJlIG5vdCBpbnNpZGUgYSBJQ1UgbWVzc2FnZSAodGhvc2UgYXJlIGhhbmRsZWQgc2VwYXJhdGVseSksXG4gICAqIC0gdGhlIG5vZGUgaXMgYSBcImRpcmVjdCBjaGlsZFwiIG9mIHRoZSBibG9ja1xuICAgKi9cbiAgcHJpdmF0ZSBfbWF5QmVBZGRCbG9ja0NoaWxkcmVuKG5vZGU6IGh0bWwuTm9kZSk6IHZvaWQge1xuICAgIGlmICh0aGlzLl9pbkkxOG5CbG9jayAmJiAhdGhpcy5faW5JY3UgJiYgdGhpcy5fZGVwdGggPT0gdGhpcy5fYmxvY2tTdGFydERlcHRoKSB7XG4gICAgICB0aGlzLl9ibG9ja0NoaWxkcmVuLnB1c2gobm9kZSk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIE1hcmtzIHRoZSBzdGFydCBvZiBhIHNlY3Rpb24sIHNlZSBgX2Nsb3NlVHJhbnNsYXRhYmxlU2VjdGlvbmBcbiAgICovXG4gIHByaXZhdGUgX29wZW5UcmFuc2xhdGFibGVTZWN0aW9uKG5vZGU6IGh0bWwuTm9kZSk6IHZvaWQge1xuICAgIGlmICh0aGlzLl9pc0luVHJhbnNsYXRhYmxlU2VjdGlvbikge1xuICAgICAgdGhpcy5fcmVwb3J0RXJyb3Iobm9kZSwgJ1VuZXhwZWN0ZWQgc2VjdGlvbiBzdGFydCcpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLl9tc2dDb3VudEF0U2VjdGlvblN0YXJ0ID0gdGhpcy5fbWVzc2FnZXMubGVuZ3RoO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBBIHRyYW5zbGF0YWJsZSBzZWN0aW9uIGNvdWxkIGJlOlxuICAgKiAtIHRoZSBjb250ZW50IG9mIHRyYW5zbGF0YWJsZSBlbGVtZW50LFxuICAgKiAtIG5vZGVzIGJldHdlZW4gYDwhLS0gaTE4biAtLT5gIGFuZCBgPCEtLSAvaTE4biAtLT5gIGNvbW1lbnRzXG4gICAqL1xuICBwcml2YXRlIGdldCBfaXNJblRyYW5zbGF0YWJsZVNlY3Rpb24oKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX21zZ0NvdW50QXRTZWN0aW9uU3RhcnQgIT09IHZvaWQgMDtcbiAgfVxuXG4gIC8qKlxuICAgKiBUZXJtaW5hdGVzIGEgc2VjdGlvbi5cbiAgICpcbiAgICogSWYgYSBzZWN0aW9uIGhhcyBvbmx5IG9uZSBzaWduaWZpY2FudCBjaGlsZHJlbiAoY29tbWVudHMgbm90IHNpZ25pZmljYW50KSB0aGVuIHdlIHNob3VsZCBub3RcbiAgICoga2VlcCB0aGUgbWVzc2FnZSBmcm9tIHRoaXMgY2hpbGRyZW46XG4gICAqXG4gICAqIGA8cCBpMThuPVwibWVhbmluZ3xkZXNjcmlwdGlvblwiPntJQ1UgbWVzc2FnZX08L3A+YCB3b3VsZCBwcm9kdWNlIHR3byBtZXNzYWdlczpcbiAgICogLSBvbmUgZm9yIHRoZSA8cD4gY29udGVudCB3aXRoIG1lYW5pbmcgYW5kIGRlc2NyaXB0aW9uLFxuICAgKiAtIGFub3RoZXIgb25lIGZvciB0aGUgSUNVIG1lc3NhZ2UuXG4gICAqXG4gICAqIEluIHRoaXMgY2FzZSB0aGUgbGFzdCBtZXNzYWdlIGlzIGRpc2NhcmRlZCBhcyBpdCBjb250YWlucyBsZXNzIGluZm9ybWF0aW9uICh0aGUgQVNUIGlzXG4gICAqIG90aGVyd2lzZSBpZGVudGljYWwpLlxuICAgKlxuICAgKiBOb3RlIHRoYXQgd2Ugc2hvdWxkIHN0aWxsIGtlZXAgbWVzc2FnZXMgZXh0cmFjdGVkIGZyb20gYXR0cmlidXRlcyBpbnNpZGUgdGhlIHNlY3Rpb24gKGllIGluIHRoZVxuICAgKiBJQ1UgbWVzc2FnZSBoZXJlKVxuICAgKi9cbiAgcHJpdmF0ZSBfY2xvc2VUcmFuc2xhdGFibGVTZWN0aW9uKG5vZGU6IGh0bWwuTm9kZSwgZGlyZWN0Q2hpbGRyZW46IGh0bWwuTm9kZVtdKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLl9pc0luVHJhbnNsYXRhYmxlU2VjdGlvbikge1xuICAgICAgdGhpcy5fcmVwb3J0RXJyb3Iobm9kZSwgJ1VuZXhwZWN0ZWQgc2VjdGlvbiBlbmQnKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBzdGFydEluZGV4ID0gdGhpcy5fbXNnQ291bnRBdFNlY3Rpb25TdGFydDtcbiAgICBjb25zdCBzaWduaWZpY2FudENoaWxkcmVuOiBudW1iZXIgPSBkaXJlY3RDaGlsZHJlbi5yZWR1Y2UoXG4gICAgICAgIChjb3VudDogbnVtYmVyLCBub2RlOiBodG1sLk5vZGUpOiBudW1iZXIgPT4gY291bnQgKyAobm9kZSBpbnN0YW5jZW9mIGh0bWwuQ29tbWVudCA/IDAgOiAxKSxcbiAgICAgICAgMCk7XG5cbiAgICBpZiAoc2lnbmlmaWNhbnRDaGlsZHJlbiA9PSAxKSB7XG4gICAgICBmb3IgKGxldCBpID0gdGhpcy5fbWVzc2FnZXMubGVuZ3RoIC0gMTsgaSA+PSBzdGFydEluZGV4ICE7IGktLSkge1xuICAgICAgICBjb25zdCBhc3QgPSB0aGlzLl9tZXNzYWdlc1tpXS5ub2RlcztcbiAgICAgICAgaWYgKCEoYXN0Lmxlbmd0aCA9PSAxICYmIGFzdFswXSBpbnN0YW5jZW9mIGkxOG4uVGV4dCkpIHtcbiAgICAgICAgICB0aGlzLl9tZXNzYWdlcy5zcGxpY2UoaSwgMSk7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICB0aGlzLl9tc2dDb3VudEF0U2VjdGlvblN0YXJ0ID0gdW5kZWZpbmVkO1xuICB9XG5cbiAgcHJpdmF0ZSBfcmVwb3J0RXJyb3Iobm9kZTogaHRtbC5Ob2RlLCBtc2c6IHN0cmluZyk6IHZvaWQge1xuICAgIHRoaXMuX2Vycm9ycy5wdXNoKG5ldyBJMThuRXJyb3Iobm9kZS5zb3VyY2VTcGFuICEsIG1zZykpO1xuICB9XG59XG5cbmZ1bmN0aW9uIF9pc09wZW5pbmdDb21tZW50KG46IGh0bWwuTm9kZSk6IGJvb2xlYW4ge1xuICByZXR1cm4gISEobiBpbnN0YW5jZW9mIGh0bWwuQ29tbWVudCAmJiBuLnZhbHVlICYmIG4udmFsdWUuc3RhcnRzV2l0aCgnaTE4bicpKTtcbn1cblxuZnVuY3Rpb24gX2lzQ2xvc2luZ0NvbW1lbnQobjogaHRtbC5Ob2RlKTogYm9vbGVhbiB7XG4gIHJldHVybiAhIShuIGluc3RhbmNlb2YgaHRtbC5Db21tZW50ICYmIG4udmFsdWUgJiYgbi52YWx1ZSA9PT0gJy9pMThuJyk7XG59XG5cbmZ1bmN0aW9uIF9nZXRJMThuQXR0cihwOiBodG1sLkVsZW1lbnQpOiBodG1sLkF0dHJpYnV0ZXxudWxsIHtcbiAgcmV0dXJuIHAuYXR0cnMuZmluZChhdHRyID0+IGF0dHIubmFtZSA9PT0gX0kxOE5fQVRUUikgfHwgbnVsbDtcbn1cblxuZnVuY3Rpb24gX3BhcnNlTWVzc2FnZU1ldGEoaTE4bj86IHN0cmluZyk6IHttZWFuaW5nOiBzdHJpbmcsIGRlc2NyaXB0aW9uOiBzdHJpbmcsIGlkOiBzdHJpbmd9IHtcbiAgaWYgKCFpMThuKSByZXR1cm4ge21lYW5pbmc6ICcnLCBkZXNjcmlwdGlvbjogJycsIGlkOiAnJ307XG5cbiAgY29uc3QgaWRJbmRleCA9IGkxOG4uaW5kZXhPZihJRF9TRVBBUkFUT1IpO1xuICBjb25zdCBkZXNjSW5kZXggPSBpMThuLmluZGV4T2YoTUVBTklOR19TRVBBUkFUT1IpO1xuICBjb25zdCBbbWVhbmluZ0FuZERlc2MsIGlkXSA9XG4gICAgICAoaWRJbmRleCA+IC0xKSA/IFtpMThuLnNsaWNlKDAsIGlkSW5kZXgpLCBpMThuLnNsaWNlKGlkSW5kZXggKyAyKV0gOiBbaTE4biwgJyddO1xuICBjb25zdCBbbWVhbmluZywgZGVzY3JpcHRpb25dID0gKGRlc2NJbmRleCA+IC0xKSA/XG4gICAgICBbbWVhbmluZ0FuZERlc2Muc2xpY2UoMCwgZGVzY0luZGV4KSwgbWVhbmluZ0FuZERlc2Muc2xpY2UoZGVzY0luZGV4ICsgMSldIDpcbiAgICAgIFsnJywgbWVhbmluZ0FuZERlc2NdO1xuXG4gIHJldHVybiB7bWVhbmluZywgZGVzY3JpcHRpb24sIGlkfTtcbn1cbiJdfQ==