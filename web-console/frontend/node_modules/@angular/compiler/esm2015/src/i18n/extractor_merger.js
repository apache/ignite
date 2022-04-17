/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as html from '../ml_parser/ast';
import { ParseTreeResult } from '../ml_parser/parser';
import * as i18n from './i18n_ast';
import { createI18nMessageFactory } from './i18n_parser';
import { I18nError } from './parse_util';
const _I18N_ATTR = 'i18n';
const _I18N_ATTR_PREFIX = 'i18n-';
const _I18N_COMMENT_PREFIX_REGEXP = /^i18n:?/;
const MEANING_SEPARATOR = '|';
const ID_SEPARATOR = '@@';
let i18nCommentsWarned = false;
/**
 * Extract translatable messages from an html AST
 */
export function extractMessages(nodes, interpolationConfig, implicitTags, implicitAttrs) {
    const visitor = new _Visitor(implicitTags, implicitAttrs);
    return visitor.extract(nodes, interpolationConfig);
}
export function mergeTranslations(nodes, translations, interpolationConfig, implicitTags, implicitAttrs) {
    const visitor = new _Visitor(implicitTags, implicitAttrs);
    return visitor.merge(nodes, translations, interpolationConfig);
}
export class ExtractionResult {
    constructor(messages, errors) {
        this.messages = messages;
        this.errors = errors;
    }
}
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
class _Visitor {
    constructor(_implicitTags, _implicitAttrs) {
        this._implicitTags = _implicitTags;
        this._implicitAttrs = _implicitAttrs;
    }
    /**
     * Extracts the messages from the tree
     */
    extract(nodes, interpolationConfig) {
        this._init(_VisitorMode.Extract, interpolationConfig);
        nodes.forEach(node => node.visit(this, null));
        if (this._inI18nBlock) {
            this._reportError(nodes[nodes.length - 1], 'Unclosed block');
        }
        return new ExtractionResult(this._messages, this._errors);
    }
    /**
     * Returns a tree where all translatable nodes are translated
     */
    merge(nodes, translations, interpolationConfig) {
        this._init(_VisitorMode.Merge, interpolationConfig);
        this._translations = translations;
        // Construct a single fake root element
        const wrapper = new html.Element('wrapper', [], nodes, undefined, undefined, undefined);
        const translatedNode = wrapper.visit(this, null);
        if (this._inI18nBlock) {
            this._reportError(nodes[nodes.length - 1], 'Unclosed block');
        }
        return new ParseTreeResult(translatedNode.children, this._errors);
    }
    visitExpansionCase(icuCase, context) {
        // Parse cases for translatable html attributes
        const expression = html.visitAll(this, icuCase.expression, context);
        if (this._mode === _VisitorMode.Merge) {
            return new html.ExpansionCase(icuCase.value, expression, icuCase.sourceSpan, icuCase.valueSourceSpan, icuCase.expSourceSpan);
        }
    }
    visitExpansion(icu, context) {
        this._mayBeAddBlockChildren(icu);
        const wasInIcu = this._inIcu;
        if (!this._inIcu) {
            // nested ICU messages should not be extracted but top-level translated as a whole
            if (this._isInTranslatableSection) {
                this._addMessage([icu]);
            }
            this._inIcu = true;
        }
        const cases = html.visitAll(this, icu.cases, context);
        if (this._mode === _VisitorMode.Merge) {
            icu = new html.Expansion(icu.switchValue, icu.type, cases, icu.sourceSpan, icu.switchValueSourceSpan);
        }
        this._inIcu = wasInIcu;
        return icu;
    }
    visitComment(comment, context) {
        const isOpening = _isOpeningComment(comment);
        if (isOpening && this._isInTranslatableSection) {
            this._reportError(comment, 'Could not start a block inside a translatable section');
            return;
        }
        const isClosing = _isClosingComment(comment);
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
                        const details = comment.sourceSpan.details ? `, ${comment.sourceSpan.details}` : '';
                        // TODO(ocombe): use a log service once there is a public one available
                        console.warn(`I18n comments are deprecated, use an <ng-container> element instead (${comment.sourceSpan.start}${details})`);
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
                        const message = this._addMessage(this._blockChildren, this._blockMeaningAndDesc);
                        // merge attributes in sections
                        const nodes = this._translateMessage(comment, message);
                        return html.visitAll(this, nodes);
                    }
                    else {
                        this._reportError(comment, 'I18N blocks should not cross element boundaries');
                        return;
                    }
                }
            }
        }
    }
    visitText(text, context) {
        if (this._isInTranslatableSection) {
            this._mayBeAddBlockChildren(text);
        }
        return text;
    }
    visitElement(el, context) {
        this._mayBeAddBlockChildren(el);
        this._depth++;
        const wasInI18nNode = this._inI18nNode;
        const wasInImplicitNode = this._inImplicitNode;
        let childNodes = [];
        let translatedChildNodes = undefined;
        // Extract:
        // - top level nodes with the (implicit) "i18n" attribute if not already in a section
        // - ICU messages
        const i18nAttr = _getI18nAttr(el);
        const i18nMeta = i18nAttr ? i18nAttr.value : '';
        const isImplicit = this._implicitTags.some(tag => el.name === tag) && !this._inIcu &&
            !this._isInTranslatableSection;
        const isTopLevelImplicit = !wasInImplicitNode && isImplicit;
        this._inImplicitNode = wasInImplicitNode || isImplicit;
        if (!this._isInTranslatableSection && !this._inIcu) {
            if (i18nAttr || isTopLevelImplicit) {
                this._inI18nNode = true;
                const message = this._addMessage(el.children, i18nMeta);
                translatedChildNodes = this._translateMessage(el, message);
            }
            if (this._mode == _VisitorMode.Extract) {
                const isTranslatable = i18nAttr || isTopLevelImplicit;
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
            const visitNodes = translatedChildNodes || el.children;
            visitNodes.forEach(child => {
                const visited = child.visit(this, context);
                if (visited && !this._isInTranslatableSection) {
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
            const translatedAttrs = this._translateAttributes(el);
            return new html.Element(el.name, translatedAttrs, childNodes, el.sourceSpan, el.startSourceSpan, el.endSourceSpan);
        }
        return null;
    }
    visitAttribute(attribute, context) {
        throw new Error('unreachable code');
    }
    _init(mode, interpolationConfig) {
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
    }
    // looks for translatable attributes
    _visitAttributesOf(el) {
        const explicitAttrNameToValue = {};
        const implicitAttrNames = this._implicitAttrs[el.name] || [];
        el.attrs.filter(attr => attr.name.startsWith(_I18N_ATTR_PREFIX))
            .forEach(attr => explicitAttrNameToValue[attr.name.slice(_I18N_ATTR_PREFIX.length)] =
            attr.value);
        el.attrs.forEach(attr => {
            if (attr.name in explicitAttrNameToValue) {
                this._addMessage([attr], explicitAttrNameToValue[attr.name]);
            }
            else if (implicitAttrNames.some(name => attr.name === name)) {
                this._addMessage([attr]);
            }
        });
    }
    // add a translatable message
    _addMessage(ast, msgMeta) {
        if (ast.length == 0 ||
            ast.length == 1 && ast[0] instanceof html.Attribute && !ast[0].value) {
            // Do not create empty messages
            return null;
        }
        const { meaning, description, id } = _parseMessageMeta(msgMeta);
        const message = this._createI18nMessage(ast, meaning, description, id);
        this._messages.push(message);
        return message;
    }
    // Translates the given message given the `TranslationBundle`
    // This is used for translating elements / blocks - see `_translateAttributes` for attributes
    // no-op when called in extraction mode (returns [])
    _translateMessage(el, message) {
        if (message && this._mode === _VisitorMode.Merge) {
            const nodes = this._translations.get(message);
            if (nodes) {
                return nodes;
            }
            this._reportError(el, `Translation unavailable for message id="${this._translations.digest(message)}"`);
        }
        return [];
    }
    // translate the attributes of an element and remove i18n specific attributes
    _translateAttributes(el) {
        const attributes = el.attrs;
        const i18nParsedMessageMeta = {};
        attributes.forEach(attr => {
            if (attr.name.startsWith(_I18N_ATTR_PREFIX)) {
                i18nParsedMessageMeta[attr.name.slice(_I18N_ATTR_PREFIX.length)] =
                    _parseMessageMeta(attr.value);
            }
        });
        const translatedAttributes = [];
        attributes.forEach((attr) => {
            if (attr.name === _I18N_ATTR || attr.name.startsWith(_I18N_ATTR_PREFIX)) {
                // strip i18n specific attributes
                return;
            }
            if (attr.value && attr.value != '' && i18nParsedMessageMeta.hasOwnProperty(attr.name)) {
                const { meaning, description, id } = i18nParsedMessageMeta[attr.name];
                const message = this._createI18nMessage([attr], meaning, description, id);
                const nodes = this._translations.get(message);
                if (nodes) {
                    if (nodes.length == 0) {
                        translatedAttributes.push(new html.Attribute(attr.name, '', attr.sourceSpan));
                    }
                    else if (nodes[0] instanceof html.Text) {
                        const value = nodes[0].value;
                        translatedAttributes.push(new html.Attribute(attr.name, value, attr.sourceSpan));
                    }
                    else {
                        this._reportError(el, `Unexpected translation for attribute "${attr.name}" (id="${id || this._translations.digest(message)}")`);
                    }
                }
                else {
                    this._reportError(el, `Translation unavailable for attribute "${attr.name}" (id="${id || this._translations.digest(message)}")`);
                }
            }
            else {
                translatedAttributes.push(attr);
            }
        });
        return translatedAttributes;
    }
    /**
     * Add the node as a child of the block when:
     * - we are in a block,
     * - we are not inside a ICU message (those are handled separately),
     * - the node is a "direct child" of the block
     */
    _mayBeAddBlockChildren(node) {
        if (this._inI18nBlock && !this._inIcu && this._depth == this._blockStartDepth) {
            this._blockChildren.push(node);
        }
    }
    /**
     * Marks the start of a section, see `_closeTranslatableSection`
     */
    _openTranslatableSection(node) {
        if (this._isInTranslatableSection) {
            this._reportError(node, 'Unexpected section start');
        }
        else {
            this._msgCountAtSectionStart = this._messages.length;
        }
    }
    /**
     * A translatable section could be:
     * - the content of translatable element,
     * - nodes between `<!-- i18n -->` and `<!-- /i18n -->` comments
     */
    get _isInTranslatableSection() {
        return this._msgCountAtSectionStart !== void 0;
    }
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
    _closeTranslatableSection(node, directChildren) {
        if (!this._isInTranslatableSection) {
            this._reportError(node, 'Unexpected section end');
            return;
        }
        const startIndex = this._msgCountAtSectionStart;
        const significantChildren = directChildren.reduce((count, node) => count + (node instanceof html.Comment ? 0 : 1), 0);
        if (significantChildren == 1) {
            for (let i = this._messages.length - 1; i >= startIndex; i--) {
                const ast = this._messages[i].nodes;
                if (!(ast.length == 1 && ast[0] instanceof i18n.Text)) {
                    this._messages.splice(i, 1);
                    break;
                }
            }
        }
        this._msgCountAtSectionStart = undefined;
    }
    _reportError(node, msg) {
        this._errors.push(new I18nError(node.sourceSpan, msg));
    }
}
function _isOpeningComment(n) {
    return !!(n instanceof html.Comment && n.value && n.value.startsWith('i18n'));
}
function _isClosingComment(n) {
    return !!(n instanceof html.Comment && n.value && n.value === '/i18n');
}
function _getI18nAttr(p) {
    return p.attrs.find(attr => attr.name === _I18N_ATTR) || null;
}
function _parseMessageMeta(i18n) {
    if (!i18n)
        return { meaning: '', description: '', id: '' };
    const idIndex = i18n.indexOf(ID_SEPARATOR);
    const descIndex = i18n.indexOf(MEANING_SEPARATOR);
    const [meaningAndDesc, id] = (idIndex > -1) ? [i18n.slice(0, idIndex), i18n.slice(idIndex + 2)] : [i18n, ''];
    const [meaning, description] = (descIndex > -1) ?
        [meaningAndDesc.slice(0, descIndex), meaningAndDesc.slice(descIndex + 1)] :
        ['', meaningAndDesc];
    return { meaning, description, id };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXh0cmFjdG9yX21lcmdlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pMThuL2V4dHJhY3Rvcl9tZXJnZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxLQUFLLElBQUksTUFBTSxrQkFBa0IsQ0FBQztBQUV6QyxPQUFPLEVBQUMsZUFBZSxFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFDcEQsT0FBTyxLQUFLLElBQUksTUFBTSxZQUFZLENBQUM7QUFDbkMsT0FBTyxFQUFDLHdCQUF3QixFQUFDLE1BQU0sZUFBZSxDQUFDO0FBQ3ZELE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFHdkMsTUFBTSxVQUFVLEdBQUcsTUFBTSxDQUFDO0FBQzFCLE1BQU0saUJBQWlCLEdBQUcsT0FBTyxDQUFDO0FBQ2xDLE1BQU0sMkJBQTJCLEdBQUcsU0FBUyxDQUFDO0FBQzlDLE1BQU0saUJBQWlCLEdBQUcsR0FBRyxDQUFDO0FBQzlCLE1BQU0sWUFBWSxHQUFHLElBQUksQ0FBQztBQUMxQixJQUFJLGtCQUFrQixHQUFHLEtBQUssQ0FBQztBQUUvQjs7R0FFRztBQUNILE1BQU0sVUFBVSxlQUFlLENBQzNCLEtBQWtCLEVBQUUsbUJBQXdDLEVBQUUsWUFBc0IsRUFDcEYsYUFBc0M7SUFDeEMsTUFBTSxPQUFPLEdBQUcsSUFBSSxRQUFRLENBQUMsWUFBWSxFQUFFLGFBQWEsQ0FBQyxDQUFDO0lBQzFELE9BQU8sT0FBTyxDQUFDLE9BQU8sQ0FBQyxLQUFLLEVBQUUsbUJBQW1CLENBQUMsQ0FBQztBQUNyRCxDQUFDO0FBRUQsTUFBTSxVQUFVLGlCQUFpQixDQUM3QixLQUFrQixFQUFFLFlBQStCLEVBQUUsbUJBQXdDLEVBQzdGLFlBQXNCLEVBQUUsYUFBc0M7SUFDaEUsTUFBTSxPQUFPLEdBQUcsSUFBSSxRQUFRLENBQUMsWUFB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SyxDQUFDLE9BQU8sR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksRUFBRSxFQUFFLENBQUMsQ0FBQztJQUNwRixNQUFNLENBQUMsT0FBTyxFQUFFLFdBQVcsQ0FBQyxHQUFHLENBQUMsU0FBUyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUM3QyxDQUFDLGNBQWMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxFQUFFLFNBQVMsQ0FBQyxFQUFFLGNBQWMsQ0FBQyxLQUFLLENBQUMsU0FBUyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUMzRSxDQUFDLEVBQUUsRUFBRSxjQUFjLENBQUMsQ0FBQztJQUV6QixPQUFPLEVBQUMsT0FBTyxFQUFFLFdBQVcsRUFBRSxFQUFFLEVBQUMsQ0FBQztBQUNwQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQgKiBhcyBodG1sIGZyb20gJy4uL21sX3BhcnNlci9hc3QnO1xuaW1wb3J0IHtJbnRlcnBvbGF0aW9uQ29uZmlnfSBmcm9tICcuLi9tbF9wYXJzZXIvaW50ZXJwb2xhdGlvbl9jb25maWcnO1xuaW1wb3J0IHtQYXJzZVRyZWVSZXN1bHR9IGZyb20gJy4uL21sX3BhcnNlci9wYXJzZXInO1xuaW1wb3J0ICogYXMgaTE4biBmcm9tICcuL2kxOG5fYXN0JztcbmltcG9ydCB7Y3JlYXRlSTE4bk1lc3NhZ2VGYWN0b3J5fSBmcm9tICcuL2kxOG5fcGFyc2VyJztcbmltcG9ydCB7STE4bkVycm9yfSBmcm9tICcuL3BhcnNlX3V0aWwnO1xuaW1wb3J0IHtUcmFuc2xhdGlvbkJ1bmRsZX0gZnJvbSAnLi90cmFuc2xhdGlvbl9idW5kbGUnO1xuXG5jb25zdCBfSTE4Tl9BVFRSID0gJ2kxOG4nO1xuY29uc3QgX0kxOE5fQVRUUl9QUkVGSVggPSAnaTE4bi0nO1xuY29uc3QgX0kxOE5fQ09NTUVOVF9QUkVGSVhfUkVHRVhQID0gL15pMThuOj8vO1xuY29uc3QgTUVBTklOR19TRVBBUkFUT1IgPSAnfCc7XG5jb25zdCBJRF9TRVBBUkFUT1IgPSAnQEAnO1xubGV0IGkxOG5Db21tZW50c1dhcm5lZCA9IGZhbHNlO1xuXG4vKipcbiAqIEV4dHJhY3QgdHJhbnNsYXRhYmxlIG1lc3NhZ2VzIGZyb20gYW4gaHRtbCBBU1RcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RNZXNzYWdlcyhcbiAgICBub2RlczogaHRtbC5Ob2RlW10sIGludGVycG9sYXRpb25Db25maWc6IEludGVycG9sYXRpb25Db25maWcsIGltcGxpY2l0VGFnczogc3RyaW5nW10sXG4gICAgaW1wbGljaXRBdHRyczoge1trOiBzdHJpbmddOiBzdHJpbmdbXX0pOiBFeHRyYWN0aW9uUmVzdWx0IHtcbiAgY29uc3QgdmlzaXRvciA9IG5ldyBfVmlzaXRvcihpbXBsaWNpdFRhZ3MsIGltcGxpY2l0QXR0cnMpO1xuICByZXR1cm4gdmlzaXRvci5leHRyYWN0KG5vZGVzLCBpbnRlcnBvbGF0aW9uQ29uZmlnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG1lcmdlVHJhbnNsYXRpb25zKFxuICAgIG5vZGVzOiBodG1sLk5vZGVbXSwgdHJhbnNsYXRpb25zOiBUcmFuc2xhdGlvbkJ1bmRsZSwgaW50ZXJwb2xhdGlvbkNvbmZpZzogSW50ZXJwb2xhdGlvbkNvbmZpZyxcbiAgICBpbXBsaWNpdFRhZ3M6IHN0cmluZ1tdLCBpbXBsaWNpdEF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ1tdfSk6IFBhcnNlVHJlZVJlc3VsdCB7XG4gIGNvbnN0IHZpc2l0b3IgPSBuZXcgX1Zpc2l0b3IoaW1wbGljaXRUYWdzLCBpbXBsaWNpdEF0dHJzKTtcbiAgcmV0dXJuIHZpc2l0b3IubWVyZ2Uobm9kZXMsIHRyYW5zbGF0aW9ucywgaW50ZXJwb2xhdGlvbkNvbmZpZyk7XG59XG5cbmV4cG9ydCBjbGFzcyBFeHRyYWN0aW9uUmVzdWx0IHtcbiAgY29uc3RydWN0b3IocHVibGljIG1lc3NhZ2VzOiBpMThuLk1lc3NhZ2VbXSwgcHVibGljIGVycm9yczogSTE4bkVycm9yW10pIHt9XG59XG5cbmVudW0gX1Zpc2l0b3JNb2RlIHtcbiAgRXh0cmFjdCxcbiAgTWVyZ2Vcbn1cblxuLyoqXG4gKiBUaGlzIFZpc2l0b3IgaXMgdXNlZDpcbiAqIDEuIHRvIGV4dHJhY3QgYWxsIHRoZSB0cmFuc2xhdGFibGUgc3RyaW5ncyBmcm9tIGFuIGh0bWwgQVNUIChzZWUgYGV4dHJhY3QoKWApLFxuICogMi4gdG8gcmVwbGFjZSB0aGUgdHJhbnNsYXRhYmxlIHN0cmluZ3Mgd2l0aCB0aGUgYWN0dWFsIHRyYW5zbGF0aW9ucyAoc2VlIGBtZXJnZSgpYClcbiAqXG4gKiBAaW50ZXJuYWxcbiAqL1xuY2xhc3MgX1Zpc2l0b3IgaW1wbGVtZW50cyBodG1sLlZpc2l0b3Ige1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfZGVwdGggITogbnVtYmVyO1xuXG4gIC8vIDxlbCBpMThuPi4uLjwvZWw+XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9pbkkxOG5Ob2RlICE6IGJvb2xlYW47XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9pbkltcGxpY2l0Tm9kZSAhOiBib29sZWFuO1xuXG4gIC8vIDwhLS1pMThuLS0+Li4uPCEtLS9pMThuLS0+XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9pbkkxOG5CbG9jayAhOiBib29sZWFuO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfYmxvY2tNZWFuaW5nQW5kRGVzYyAhOiBzdHJpbmc7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9ibG9ja0NoaWxkcmVuICE6IGh0bWwuTm9kZVtdO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfYmxvY2tTdGFydERlcHRoICE6IG51bWJlcjtcblxuICAvLyB7PGljdSBtZXNzYWdlPn1cbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2luSWN1ICE6IGJvb2xlYW47XG5cbiAgLy8gc2V0IHRvIHZvaWQgMCB3aGVuIG5vdCBpbiBhIHNlY3Rpb25cbiAgcHJpdmF0ZSBfbXNnQ291bnRBdFNlY3Rpb25TdGFydDogbnVtYmVyfHVuZGVmaW5lZDtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2Vycm9ycyAhOiBJMThuRXJyb3JbXTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX21vZGUgITogX1Zpc2l0b3JNb2RlO1xuXG4gIC8vIF9WaXNpdG9yTW9kZS5FeHRyYWN0IG9ubHlcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX21lc3NhZ2VzICE6IGkxOG4uTWVzc2FnZVtdO1xuXG4gIC8vIF9WaXNpdG9yTW9kZS5NZXJnZSBvbmx5XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF90cmFuc2xhdGlvbnMgITogVHJhbnNsYXRpb25CdW5kbGU7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9jcmVhdGVJMThuTWVzc2FnZSAhOiAoXG4gICAgICBtc2c6IGh0bWwuTm9kZVtdLCBtZWFuaW5nOiBzdHJpbmcsIGRlc2NyaXB0aW9uOiBzdHJpbmcsIGlkOiBzdHJpbmcpID0+IGkxOG4uTWVzc2FnZTtcblxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX2ltcGxpY2l0VGFnczogc3RyaW5nW10sIHByaXZhdGUgX2ltcGxpY2l0QXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nW119KSB7fVxuXG4gIC8qKlxuICAgKiBFeHRyYWN0cyB0aGUgbWVzc2FnZXMgZnJvbSB0aGUgdHJlZVxuICAgKi9cbiAgZXh0cmFjdChub2RlczogaHRtbC5Ob2RlW10sIGludGVycG9sYXRpb25Db25maWc6IEludGVycG9sYXRpb25Db25maWcpOiBFeHRyYWN0aW9uUmVzdWx0IHtcbiAgICB0aGlzLl9pbml0KF9WaXNpdG9yTW9kZS5FeHRyYWN0LCBpbnRlcnBvbGF0aW9uQ29uZmlnKTtcblxuICAgIG5vZGVzLmZvckVhY2gobm9kZSA9PiBub2RlLnZpc2l0KHRoaXMsIG51bGwpKTtcblxuICAgIGlmICh0aGlzLl9pbkkxOG5CbG9jaykge1xuICAgICAgdGhpcy5fcmVwb3J0RXJyb3Iobm9kZXNbbm9kZXMubGVuZ3RoIC0gMV0sICdVbmNsb3NlZCBibG9jaycpO1xuICAgIH1cblxuICAgIHJldHVybiBuZXcgRXh0cmFjdGlvblJlc3VsdCh0aGlzLl9tZXNzYWdlcywgdGhpcy5fZXJyb3JzKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIGEgdHJlZSB3aGVyZSBhbGwgdHJhbnNsYXRhYmxlIG5vZGVzIGFyZSB0cmFuc2xhdGVkXG4gICAqL1xuICBtZXJnZShcbiAgICAgIG5vZGVzOiBodG1sLk5vZGVbXSwgdHJhbnNsYXRpb25zOiBUcmFuc2xhdGlvbkJ1bmRsZSxcbiAgICAgIGludGVycG9sYXRpb25Db25maWc6IEludGVycG9sYXRpb25Db25maWcpOiBQYXJzZVRyZWVSZXN1bHQge1xuICAgIHRoaXMuX2luaXQoX1Zpc2l0b3JNb2RlLk1lcmdlLCBpbnRlcnBvbGF0aW9uQ29uZmlnKTtcbiAgICB0aGlzLl90cmFuc2xhdGlvbnMgPSB0cmFuc2xhdGlvbnM7XG5cbiAgICAvLyBDb25zdHJ1Y3QgYSBzaW5nbGUgZmFrZSByb290IGVsZW1lbnRcbiAgICBjb25zdCB3cmFwcGVyID0gbmV3IGh0bWwuRWxlbWVudCgnd3JhcHBlcicsIFtdLCBub2RlcywgdW5kZWZpbmVkICEsIHVuZGVmaW5lZCwgdW5kZWZpbmVkKTtcblxuICAgIGNvbnN0IHRyYW5zbGF0ZWROb2RlID0gd3JhcHBlci52aXNpdCh0aGlzLCBudWxsKTtcblxuICAgIGlmICh0aGlzLl9pbkkxOG5CbG9jaykge1xuICAgICAgdGhpcy5fcmVwb3J0RXJyb3Iobm9kZXNbbm9kZXMubGVuZ3RoIC0gMV0sICdVbmNsb3NlZCBibG9jaycpO1xuICAgIH1cblxuICAgIHJldHVybiBuZXcgUGFyc2VUcmVlUmVzdWx0KHRyYW5zbGF0ZWROb2RlLmNoaWxkcmVuLCB0aGlzLl9lcnJvcnMpO1xuICB9XG5cbiAgdmlzaXRFeHBhbnNpb25DYXNlKGljdUNhc2U6IGh0bWwuRXhwYW5zaW9uQ2FzZSwgY29udGV4dDogYW55KTogYW55IHtcbiAgICAvLyBQYXJzZSBjYXNlcyBmb3IgdHJhbnNsYXRhYmxlIGh0bWwgYXR0cmlidXRlc1xuICAgIGNvbnN0IGV4cHJlc3Npb24gPSBodG1sLnZpc2l0QWxsKHRoaXMsIGljdUNhc2UuZXhwcmVzc2lvbiwgY29udGV4dCk7XG5cbiAgICBpZiAodGhpcy5fbW9kZSA9PT0gX1Zpc2l0b3JNb2RlLk1lcmdlKSB7XG4gICAgICByZXR1cm4gbmV3IGh0bWwuRXhwYW5zaW9uQ2FzZShcbiAgICAgICAgICBpY3VDYXNlLnZhbHVlLCBleHByZXNzaW9uLCBpY3VDYXNlLnNvdXJjZVNwYW4sIGljdUNhc2UudmFsdWVTb3VyY2VTcGFuLFxuICAgICAgICAgIGljdUNhc2UuZXhwU291cmNlU3Bhbik7XG4gICAgfVxuICB9XG5cbiAgdmlzaXRFeHBhbnNpb24oaWN1OiBodG1sLkV4cGFuc2lvbiwgY29udGV4dDogYW55KTogaHRtbC5FeHBhbnNpb24ge1xuICAgIHRoaXMuX21heUJlQWRkQmxvY2tDaGlsZHJlbihpY3UpO1xuXG4gICAgY29uc3Qgd2FzSW5JY3UgPSB0aGlzLl9pbkljdTtcblxuICAgIGlmICghdGhpcy5faW5JY3UpIHtcbiAgICAgIC8vIG5lc3RlZCBJQ1UgbWVzc2FnZXMgc2hvdWxkIG5vdCBiZSBleHRyYWN0ZWQgYnV0IHRvcC1sZXZlbCB0cmFuc2xhdGVkIGFzIGEgd2hvbGVcbiAgICAgIGlmICh0aGlzLl9pc0luVHJhbnNsYXRhYmxlU2VjdGlvbikge1xuICAgICAgICB0aGlzLl9hZGRNZXNzYWdlKFtpY3VdKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuX2luSWN1ID0gdHJ1ZTtcbiAgICB9XG5cbiAgICBjb25zdCBjYXNlcyA9IGh0bWwudmlzaXRBbGwodGhpcywgaWN1LmNhc2VzLCBjb250ZXh0KTtcblxuICAgIGlmICh0aGlzLl9tb2RlID09PSBfVmlzaXRvck1vZGUuTWVyZ2UpIHtcbiAgICAgIGljdSA9IG5ldyBodG1sLkV4cGFuc2lvbihcbiAgICAgICAgICBpY3Uuc3dpdGNoVmFsdWUsIGljdS50eXBlLCBjYXNlcywgaWN1LnNvdXJjZVNwYW4sIGljdS5zd2l0Y2hWYWx1ZVNvdXJjZVNwYW4pO1xuICAgIH1cblxuICAgIHRoaXMuX2luSWN1ID0gd2FzSW5JY3U7XG5cbiAgICByZXR1cm4gaWN1O1xuICB9XG5cbiAgdmlzaXRDb21tZW50KGNvbW1lbnQ6IGh0bWwuQ29tbWVudCwgY29udGV4dDogYW55KTogYW55IHtcbiAgICBjb25zdCBpc09wZW5pbmcgPSBfaXNPcGVuaW5nQ29tbWVudChjb21tZW50KTtcblxuICAgIGlmIChpc09wZW5pbmcgJiYgdGhpcy5faXNJblRyYW5zbGF0YWJsZVNlY3Rpb24pIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKGNvbW1lbnQsICdDb3VsZCBub3Qgc3RhcnQgYSBibG9jayBpbnNpZGUgYSB0cmFuc2xhdGFibGUgc2VjdGlvbicpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IGlzQ2xvc2luZyA9IF9pc0Nsb3NpbmdDb21tZW50KGNvbW1lbnQpO1xuXG4gICAgaWYgKGlzQ2xvc2luZyAmJiAhdGhpcy5faW5JMThuQmxvY2spIHtcbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKGNvbW1lbnQsICdUcnlpbmcgdG8gY2xvc2UgYW4gdW5vcGVuZWQgYmxvY2snKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBpZiAoIXRoaXMuX2luSTE4bk5vZGUgJiYgIXRoaXMuX2luSWN1KSB7XG4gICAgICBpZiAoIXRoaXMuX2luSTE4bkJsb2NrKSB7XG4gICAgICAgIGlmIChpc09wZW5pbmcpIHtcbiAgICAgICAgICAvLyBkZXByZWNhdGVkIGZyb20gdjUgeW91IHNob3VsZCB1c2UgPG5nLWNvbnRhaW5lciBpMThuPiBpbnN0ZWFkIG9mIGkxOG4gY29tbWVudHNcbiAgICAgICAgICBpZiAoIWkxOG5Db21tZW50c1dhcm5lZCAmJiA8YW55PmNvbnNvbGUgJiYgPGFueT5jb25zb2xlLndhcm4pIHtcbiAgICAgICAgICAgIGkxOG5Db21tZW50c1dhcm5lZCA9IHRydWU7XG4gICAgICAgICAgICBjb25zdCBkZXRhaWxzID0gY29tbWVudC5zb3VyY2VTcGFuLmRldGFpbHMgPyBgLCAke2NvbW1lbnQuc291cmNlU3Bhbi5kZXRhaWxzfWAgOiAnJztcbiAgICAgICAgICAgIC8vIFRPRE8ob2NvbWJlKTogdXNlIGEgbG9nIHNlcnZpY2Ugb25jZSB0aGVyZSBpcyBhIHB1YmxpYyBvbmUgYXZhaWxhYmxlXG4gICAgICAgICAgICBjb25zb2xlLndhcm4oXG4gICAgICAgICAgICAgICAgYEkxOG4gY29tbWVudHMgYXJlIGRlcHJlY2F0ZWQsIHVzZSBhbiA8bmctY29udGFpbmVyPiBlbGVtZW50IGluc3RlYWQgKCR7Y29tbWVudC5zb3VyY2VTcGFuLnN0YXJ0fSR7ZGV0YWlsc30pYCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHRoaXMuX2luSTE4bkJsb2NrID0gdHJ1ZTtcbiAgICAgICAgICB0aGlzLl9ibG9ja1N0YXJ0RGVwdGggPSB0aGlzLl9kZXB0aDtcbiAgICAgICAgICB0aGlzLl9ibG9ja0NoaWxkcmVuID0gW107XG4gICAgICAgICAgdGhpcy5fYmxvY2tNZWFuaW5nQW5kRGVzYyA9XG4gICAgICAgICAgICAgIGNvbW1lbnQudmFsdWUgIS5yZXBsYWNlKF9JMThOX0NPTU1FTlRfUFJFRklYX1JFR0VYUCwgJycpLnRyaW0oKTtcbiAgICAgICAgICB0aGlzLl9vcGVuVHJhbnNsYXRhYmxlU2VjdGlvbihjb21tZW50KTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgaWYgKGlzQ2xvc2luZykge1xuICAgICAgICAgIGlmICh0aGlzLl9kZXB0aCA9PSB0aGlzLl9ibG9ja1N0YXJ0RGVwdGgpIHtcbiAgICAgICAgICAgIHRoaXMuX2Nsb3NlVHJhbnNsYXRhYmxlU2VjdGlvbihjb21tZW50LCB0aGlzLl9ibG9ja0NoaWxkcmVuKTtcbiAgICAgICAgICAgIHRoaXMuX2luSTE4bkJsb2NrID0gZmFsc2U7XG4gICAgICAgICAgICBjb25zdCBtZXNzYWdlID0gdGhpcy5fYWRkTWVzc2FnZSh0aGlzLl9ibG9ja0NoaWxkcmVuLCB0aGlzLl9ibG9ja01lYW5pbmdBbmREZXNjKSAhO1xuICAgICAgICAgICAgLy8gbWVyZ2UgYXR0cmlidXRlcyBpbiBzZWN0aW9uc1xuICAgICAgICAgICAgY29uc3Qgbm9kZXMgPSB0aGlzLl90cmFuc2xhdGVNZXNzYWdlKGNvbW1lbnQsIG1lc3NhZ2UpO1xuICAgICAgICAgICAgcmV0dXJuIGh0bWwudmlzaXRBbGwodGhpcywgbm9kZXMpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICB0aGlzLl9yZXBvcnRFcnJvcihjb21tZW50LCAnSTE4TiBibG9ja3Mgc2hvdWxkIG5vdCBjcm9zcyBlbGVtZW50IGJvdW5kYXJpZXMnKTtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICB2aXNpdFRleHQodGV4dDogaHRtbC5UZXh0LCBjb250ZXh0OiBhbnkpOiBodG1sLlRleHQge1xuICAgIGlmICh0aGlzLl9pc0luVHJhbnNsYXRhYmxlU2VjdGlvbikge1xuICAgICAgdGhpcy5fbWF5QmVBZGRCbG9ja0NoaWxkcmVuKHRleHQpO1xuICAgIH1cbiAgICByZXR1cm4gdGV4dDtcbiAgfVxuXG4gIHZpc2l0RWxlbWVudChlbDogaHRtbC5FbGVtZW50LCBjb250ZXh0OiBhbnkpOiBodG1sLkVsZW1lbnR8bnVsbCB7XG4gICAgdGhpcy5fbWF5QmVBZGRCbG9ja0NoaWxkcmVuKGVsKTtcbiAgICB0aGlzLl9kZXB0aCsrO1xuICAgIGNvbnN0IHdhc0luSTE4bk5vZGUgPSB0aGlzLl9pbkkxOG5Ob2RlO1xuICAgIGNvbnN0IHdhc0luSW1wbGljaXROb2RlID0gdGhpcy5faW5JbXBsaWNpdE5vZGU7XG4gICAgbGV0IGNoaWxkTm9kZXM6IGh0bWwuTm9kZVtdID0gW107XG4gICAgbGV0IHRyYW5zbGF0ZWRDaGlsZE5vZGVzOiBodG1sLk5vZGVbXSA9IHVuZGVmaW5lZCAhO1xuXG4gICAgLy8gRXh0cmFjdDpcbiAgICAvLyAtIHRvcCBsZXZlbCBub2RlcyB3aXRoIHRoZSAoaW1wbGljaXQpIFwiaTE4blwiIGF0dHJpYnV0ZSBpZiBub3QgYWxyZWFkeSBpbiBhIHNlY3Rpb25cbiAgICAvLyAtIElDVSBtZXNzYWdlc1xuICAgIGNvbnN0IGkxOG5BdHRyID0gX2dldEkxOG5BdHRyKGVsKTtcbiAgICBjb25zdCBpMThuTWV0YSA9IGkxOG5BdHRyID8gaTE4bkF0dHIudmFsdWUgOiAnJztcbiAgICBjb25zdCBpc0ltcGxpY2l0ID0gdGhpcy5faW1wbGljaXRUYWdzLnNvbWUodGFnID0+IGVsLm5hbWUgPT09IHRhZykgJiYgIXRoaXMuX2luSWN1ICYmXG4gICAgICAgICF0aGlzLl9pc0luVHJhbnNsYXRhYmxlU2VjdGlvbjtcbiAgICBjb25zdCBpc1RvcExldmVsSW1wbGljaXQgPSAhd2FzSW5JbXBsaWNpdE5vZGUgJiYgaXNJbXBsaWNpdDtcbiAgICB0aGlzLl9pbkltcGxpY2l0Tm9kZSA9IHdhc0luSW1wbGljaXROb2RlIHx8IGlzSW1wbGljaXQ7XG5cbiAgICBpZiAoIXRoaXMuX2lzSW5UcmFuc2xhdGFibGVTZWN0aW9uICYmICF0aGlzLl9pbkljdSkge1xuICAgICAgaWYgKGkxOG5BdHRyIHx8IGlzVG9wTGV2ZWxJbXBsaWNpdCkge1xuICAgICAgICB0aGlzLl9pbkkxOG5Ob2RlID0gdHJ1ZTtcbiAgICAgICAgY29uc3QgbWVzc2FnZSA9IHRoaXMuX2FkZE1lc3NhZ2UoZWwuY2hpbGRyZW4sIGkxOG5NZXRhKSAhO1xuICAgICAgICB0cmFuc2xhdGVkQ2hpbGROb2RlcyA9IHRoaXMuX3RyYW5zbGF0ZU1lc3NhZ2UoZWwsIG1lc3NhZ2UpO1xuICAgICAgfVxuXG4gICAgICBpZiAodGhpcy5fbW9kZSA9PSBfVmlzaXRvck1vZGUuRXh0cmFjdCkge1xuICAgICAgICBjb25zdCBpc1RyYW5zbGF0YWJsZSA9IGkxOG5BdHRyIHx8IGlzVG9wTGV2ZWxJbXBsaWNpdDtcbiAgICAgICAgaWYgKGlzVHJhbnNsYXRhYmxlKSB0aGlzLl9vcGVuVHJhbnNsYXRhYmxlU2VjdGlvbihlbCk7XG4gICAgICAgIGh0bWwudmlzaXRBbGwodGhpcywgZWwuY2hpbGRyZW4pO1xuICAgICAgICBpZiAoaXNUcmFuc2xhdGFibGUpIHRoaXMuX2Nsb3NlVHJhbnNsYXRhYmxlU2VjdGlvbihlbCwgZWwuY2hpbGRyZW4pO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoaTE4bkF0dHIgfHwgaXNUb3BMZXZlbEltcGxpY2l0KSB7XG4gICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgICAgZWwsICdDb3VsZCBub3QgbWFyayBhbiBlbGVtZW50IGFzIHRyYW5zbGF0YWJsZSBpbnNpZGUgYSB0cmFuc2xhdGFibGUgc2VjdGlvbicpO1xuICAgICAgfVxuXG4gICAgICBpZiAodGhpcy5fbW9kZSA9PSBfVmlzaXRvck1vZGUuRXh0cmFjdCkge1xuICAgICAgICAvLyBEZXNjZW5kIGludG8gY2hpbGQgbm9kZXMgZm9yIGV4dHJhY3Rpb25cbiAgICAgICAgaHRtbC52aXNpdEFsbCh0aGlzLCBlbC5jaGlsZHJlbik7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKHRoaXMuX21vZGUgPT09IF9WaXNpdG9yTW9kZS5NZXJnZSkge1xuICAgICAgY29uc3QgdmlzaXROb2RlcyA9IHRyYW5zbGF0ZWRDaGlsZE5vZGVzIHx8IGVsLmNoaWxkcmVuO1xuICAgICAgdmlzaXROb2Rlcy5mb3JFYWNoKGNoaWxkID0+IHtcbiAgICAgICAgY29uc3QgdmlzaXRlZCA9IGNoaWxkLnZpc2l0KHRoaXMsIGNvbnRleHQpO1xuICAgICAgICBpZiAodmlzaXRlZCAmJiAhdGhpcy5faXNJblRyYW5zbGF0YWJsZVNlY3Rpb24pIHtcbiAgICAgICAgICAvLyBEbyBub3QgYWRkIHRoZSBjaGlsZHJlbiBmcm9tIHRyYW5zbGF0YWJsZSBzZWN0aW9ucyAoPSBpMThuIGJsb2NrcyBoZXJlKVxuICAgICAgICAgIC8vIFRoZXkgd2lsbCBiZSBhZGRlZCBsYXRlciBpbiB0aGlzIGxvb3Agd2hlbiB0aGUgYmxvY2sgY2xvc2VzIChpLmUuIG9uIGA8IS0tIC9pMThuIC0tPmApXG4gICAgICAgICAgY2hpbGROb2RlcyA9IGNoaWxkTm9kZXMuY29uY2F0KHZpc2l0ZWQpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG5cbiAgICB0aGlzLl92aXNpdEF0dHJpYnV0ZXNPZihlbCk7XG5cbiAgICB0aGlzLl9kZXB0aC0tO1xuICAgIHRoaXMuX2luSTE4bk5vZGUgPSB3YXNJbkkxOG5Ob2RlO1xuICAgIHRoaXMuX2luSW1wbGljaXROb2RlID0gd2FzSW5JbXBsaWNpdE5vZGU7XG5cbiAgICBpZiAodGhpcy5fbW9kZSA9PT0gX1Zpc2l0b3JNb2RlLk1lcmdlKSB7XG4gICAgICBjb25zdCB0cmFuc2xhdGVkQXR0cnMgPSB0aGlzLl90cmFuc2xhdGVBdHRyaWJ1dGVzKGVsKTtcbiAgICAgIHJldHVybiBuZXcgaHRtbC5FbGVtZW50KFxuICAgICAgICAgIGVsLm5hbWUsIHRyYW5zbGF0ZWRBdHRycywgY2hpbGROb2RlcywgZWwuc291cmNlU3BhbiwgZWwuc3RhcnRTb3VyY2VTcGFuLFxuICAgICAgICAgIGVsLmVuZFNvdXJjZVNwYW4pO1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHZpc2l0QXR0cmlidXRlKGF0dHJpYnV0ZTogaHRtbC5BdHRyaWJ1dGUsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKCd1bnJlYWNoYWJsZSBjb2RlJyk7XG4gIH1cblxuICBwcml2YXRlIF9pbml0KG1vZGU6IF9WaXNpdG9yTW9kZSwgaW50ZXJwb2xhdGlvbkNvbmZpZzogSW50ZXJwb2xhdGlvbkNvbmZpZyk6IHZvaWQge1xuICAgIHRoaXMuX21vZGUgPSBtb2RlO1xuICAgIHRoaXMuX2luSTE4bkJsb2NrID0gZmFsc2U7XG4gICAgdGhpcy5faW5JMThuTm9kZSA9IGZhbHNlO1xuICAgIHRoaXMuX2RlcHRoID0gMDtcbiAgICB0aGlzLl9pbkljdSA9IGZhbHNlO1xuICAgIHRoaXMuX21zZ0NvdW50QXRTZWN0aW9uU3RhcnQgPSB1bmRlZmluZWQ7XG4gICAgdGhpcy5fZXJyb3JzID0gW107XG4gICAgdGhpcy5fbWVzc2FnZXMgPSBbXTtcbiAgICB0aGlzLl9pbkltcGxpY2l0Tm9kZSA9IGZhbHNlO1xuICAgIHRoaXMuX2NyZWF0ZUkxOG5NZXNzYWdlID0gY3JlYXRlSTE4bk1lc3NhZ2VGYWN0b3J5KGludGVycG9sYXRpb25Db25maWcpO1xuICB9XG5cbiAgLy8gbG9va3MgZm9yIHRyYW5zbGF0YWJsZSBhdHRyaWJ1dGVzXG4gIHByaXZhdGUgX3Zpc2l0QXR0cmlidXRlc09mKGVsOiBodG1sLkVsZW1lbnQpOiB2b2lkIHtcbiAgICBjb25zdCBleHBsaWNpdEF0dHJOYW1lVG9WYWx1ZToge1trOiBzdHJpbmddOiBzdHJpbmd9ID0ge307XG4gICAgY29uc3QgaW1wbGljaXRBdHRyTmFtZXM6IHN0cmluZ1tdID0gdGhpcy5faW1wbGljaXRBdHRyc1tlbC5uYW1lXSB8fCBbXTtcblxuICAgIGVsLmF0dHJzLmZpbHRlcihhdHRyID0+IGF0dHIubmFtZS5zdGFydHNXaXRoKF9JMThOX0FUVFJfUFJFRklYKSlcbiAgICAgICAgLmZvckVhY2goXG4gICAgICAgICAgICBhdHRyID0+IGV4cGxpY2l0QXR0ck5hbWVUb1ZhbHVlW2F0dHIubmFtZS5zbGljZShfSTE4Tl9BVFRSX1BSRUZJWC5sZW5ndGgpXSA9XG4gICAgICAgICAgICAgICAgYXR0ci52YWx1ZSk7XG5cbiAgICBlbC5hdHRycy5mb3JFYWNoKGF0dHIgPT4ge1xuICAgICAgaWYgKGF0dHIubmFtZSBpbiBleHBsaWNpdEF0dHJOYW1lVG9WYWx1ZSkge1xuICAgICAgICB0aGlzLl9hZGRNZXNzYWdlKFthdHRyXSwgZXhwbGljaXRBdHRyTmFtZVRvVmFsdWVbYXR0ci5uYW1lXSk7XG4gICAgICB9IGVsc2UgaWYgKGltcGxpY2l0QXR0ck5hbWVzLnNvbWUobmFtZSA9PiBhdHRyLm5hbWUgPT09IG5hbWUpKSB7XG4gICAgICAgIHRoaXMuX2FkZE1lc3NhZ2UoW2F0dHJdKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIC8vIGFkZCBhIHRyYW5zbGF0YWJsZSBtZXNzYWdlXG4gIHByaXZhdGUgX2FkZE1lc3NhZ2UoYXN0OiBodG1sLk5vZGVbXSwgbXNnTWV0YT86IHN0cmluZyk6IGkxOG4uTWVzc2FnZXxudWxsIHtcbiAgICBpZiAoYXN0Lmxlbmd0aCA9PSAwIHx8XG4gICAgICAgIGFzdC5sZW5ndGggPT0gMSAmJiBhc3RbMF0gaW5zdGFuY2VvZiBodG1sLkF0dHJpYnV0ZSAmJiAhKDxodG1sLkF0dHJpYnV0ZT5hc3RbMF0pLnZhbHVlKSB7XG4gICAgICAvLyBEbyBub3QgY3JlYXRlIGVtcHR5IG1lc3NhZ2VzXG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG5cbiAgICBjb25zdCB7bWVhbmluZywgZGVzY3JpcHRpb24sIGlkfSA9IF9wYXJzZU1lc3NhZ2VNZXRhKG1zZ01ldGEpO1xuICAgIGNvbnN0IG1lc3NhZ2UgPSB0aGlzLl9jcmVhdGVJMThuTWVzc2FnZShhc3QsIG1lYW5pbmcsIGRlc2NyaXB0aW9uLCBpZCk7XG4gICAgdGhpcy5fbWVzc2FnZXMucHVzaChtZXNzYWdlKTtcbiAgICByZXR1cm4gbWVzc2FnZTtcbiAgfVxuXG4gIC8vIFRyYW5zbGF0ZXMgdGhlIGdpdmVuIG1lc3NhZ2UgZ2l2ZW4gdGhlIGBUcmFuc2xhdGlvbkJ1bmRsZWBcbiAgLy8gVGhpcyBpcyB1c2VkIGZvciB0cmFuc2xhdGluZyBlbGVtZW50cyAvIGJsb2NrcyAtIHNlZSBgX3RyYW5zbGF0ZUF0dHJpYnV0ZXNgIGZvciBhdHRyaWJ1dGVzXG4gIC8vIG5vLW9wIHdoZW4gY2FsbGVkIGluIGV4dHJhY3Rpb24gbW9kZSAocmV0dXJucyBbXSlcbiAgcHJpdmF0ZSBfdHJhbnNsYXRlTWVzc2FnZShlbDogaHRtbC5Ob2RlLCBtZXNzYWdlOiBpMThuLk1lc3NhZ2UpOiBodG1sLk5vZGVbXSB7XG4gICAgaWYgKG1lc3NhZ2UgJiYgdGhpcy5fbW9kZSA9PT0gX1Zpc2l0b3JNb2RlLk1lcmdlKSB7XG4gICAgICBjb25zdCBub2RlcyA9IHRoaXMuX3RyYW5zbGF0aW9ucy5nZXQobWVzc2FnZSk7XG5cbiAgICAgIGlmIChub2Rlcykge1xuICAgICAgICByZXR1cm4gbm9kZXM7XG4gICAgICB9XG5cbiAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgIGVsLCBgVHJhbnNsYXRpb24gdW5hdmFpbGFibGUgZm9yIG1lc3NhZ2UgaWQ9XCIke3RoaXMuX3RyYW5zbGF0aW9ucy5kaWdlc3QobWVzc2FnZSl9XCJgKTtcbiAgICB9XG5cbiAgICByZXR1cm4gW107XG4gIH1cblxuICAvLyB0cmFuc2xhdGUgdGhlIGF0dHJpYnV0ZXMgb2YgYW4gZWxlbWVudCBhbmQgcmVtb3ZlIGkxOG4gc3BlY2lmaWMgYXR0cmlidXRlc1xuICBwcml2YXRlIF90cmFuc2xhdGVBdHRyaWJ1dGVzKGVsOiBodG1sLkVsZW1lbnQpOiBodG1sLkF0dHJpYnV0ZVtdIHtcbiAgICBjb25zdCBhdHRyaWJ1dGVzID0gZWwuYXR0cnM7XG4gICAgY29uc3QgaTE4blBhcnNlZE1lc3NhZ2VNZXRhOlxuICAgICAgICB7W25hbWU6IHN0cmluZ106IHttZWFuaW5nOiBzdHJpbmcsIGRlc2NyaXB0aW9uOiBzdHJpbmcsIGlkOiBzdHJpbmd9fSA9IHt9O1xuXG4gICAgYXR0cmlidXRlcy5mb3JFYWNoKGF0dHIgPT4ge1xuICAgICAgaWYgKGF0dHIubmFtZS5zdGFydHNXaXRoKF9JMThOX0FUVFJfUFJFRklYKSkge1xuICAgICAgICBpMThuUGFyc2VkTWVzc2FnZU1ldGFbYXR0ci5uYW1lLnNsaWNlKF9JMThOX0FUVFJfUFJFRklYLmxlbmd0aCldID1cbiAgICAgICAgICAgIF9wYXJzZU1lc3NhZ2VNZXRhKGF0dHIudmFsdWUpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgY29uc3QgdHJhbnNsYXRlZEF0dHJpYnV0ZXM6IGh0bWwuQXR0cmlidXRlW10gPSBbXTtcblxuICAgIGF0dHJpYnV0ZXMuZm9yRWFjaCgoYXR0cikgPT4ge1xuICAgICAgaWYgKGF0dHIubmFtZSA9PT0gX0kxOE5fQVRUUiB8fCBhdHRyLm5hbWUuc3RhcnRzV2l0aChfSTE4Tl9BVFRSX1BSRUZJWCkpIHtcbiAgICAgICAgLy8gc3RyaXAgaTE4biBzcGVjaWZpYyBhdHRyaWJ1dGVzXG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cblxuICAgICAgaWYgKGF0dHIudmFsdWUgJiYgYXR0ci52YWx1ZSAhPSAnJyAmJiBpMThuUGFyc2VkTWVzc2FnZU1ldGEuaGFzT3duUHJvcGVydHkoYXR0ci5uYW1lKSkge1xuICAgICAgICBjb25zdCB7bWVhbmluZywgZGVzY3JpcHRpb24sIGlkfSA9IGkxOG5QYXJzZWRNZXNzYWdlTWV0YVthdHRyLm5hbWVdO1xuICAgICAgICBjb25zdCBtZXNzYWdlOiBpMThuLk1lc3NhZ2UgPSB0aGlzLl9jcmVhdGVJMThuTWVzc2FnZShbYXR0cl0sIG1lYW5pbmcsIGRlc2NyaXB0aW9uLCBpZCk7XG4gICAgICAgIGNvbnN0IG5vZGVzID0gdGhpcy5fdHJhbnNsYXRpb25zLmdldChtZXNzYWdlKTtcbiAgICAgICAgaWYgKG5vZGVzKSB7XG4gICAgICAgICAgaWYgKG5vZGVzLmxlbmd0aCA9PSAwKSB7XG4gICAgICAgICAgICB0cmFuc2xhdGVkQXR0cmlidXRlcy5wdXNoKG5ldyBodG1sLkF0dHJpYnV0ZShhdHRyLm5hbWUsICcnLCBhdHRyLnNvdXJjZVNwYW4pKTtcbiAgICAgICAgICB9IGVsc2UgaWYgKG5vZGVzWzBdIGluc3RhbmNlb2YgaHRtbC5UZXh0KSB7XG4gICAgICAgICAgICBjb25zdCB2YWx1ZSA9IChub2Rlc1swXSBhcyBodG1sLlRleHQpLnZhbHVlO1xuICAgICAgICAgICAgdHJhbnNsYXRlZEF0dHJpYnV0ZXMucHVzaChuZXcgaHRtbC5BdHRyaWJ1dGUoYXR0ci5uYW1lLCB2YWx1ZSwgYXR0ci5zb3VyY2VTcGFuKSk7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgICAgICAgIGVsLFxuICAgICAgICAgICAgICAgIGBVbmV4cGVjdGVkIHRyYW5zbGF0aW9uIGZvciBhdHRyaWJ1dGUgXCIke2F0dHIubmFtZX1cIiAoaWQ9XCIke2lkIHx8IHRoaXMuX3RyYW5zbGF0aW9ucy5kaWdlc3QobWVzc2FnZSl9XCIpYCk7XG4gICAgICAgICAgfVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRoaXMuX3JlcG9ydEVycm9yKFxuICAgICAgICAgICAgICBlbCxcbiAgICAgICAgICAgICAgYFRyYW5zbGF0aW9uIHVuYXZhaWxhYmxlIGZvciBhdHRyaWJ1dGUgXCIke2F0dHIubmFtZX1cIiAoaWQ9XCIke2lkIHx8IHRoaXMuX3RyYW5zbGF0aW9ucy5kaWdlc3QobWVzc2FnZSl9XCIpYCk7XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRyYW5zbGF0ZWRBdHRyaWJ1dGVzLnB1c2goYXR0cik7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICByZXR1cm4gdHJhbnNsYXRlZEF0dHJpYnV0ZXM7XG4gIH1cblxuXG4gIC8qKlxuICAgKiBBZGQgdGhlIG5vZGUgYXMgYSBjaGlsZCBvZiB0aGUgYmxvY2sgd2hlbjpcbiAgICogLSB3ZSBhcmUgaW4gYSBibG9jayxcbiAgICogLSB3ZSBhcmUgbm90IGluc2lkZSBhIElDVSBtZXNzYWdlICh0aG9zZSBhcmUgaGFuZGxlZCBzZXBhcmF0ZWx5KSxcbiAgICogLSB0aGUgbm9kZSBpcyBhIFwiZGlyZWN0IGNoaWxkXCIgb2YgdGhlIGJsb2NrXG4gICAqL1xuICBwcml2YXRlIF9tYXlCZUFkZEJsb2NrQ2hpbGRyZW4obm9kZTogaHRtbC5Ob2RlKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX2luSTE4bkJsb2NrICYmICF0aGlzLl9pbkljdSAmJiB0aGlzLl9kZXB0aCA9PSB0aGlzLl9ibG9ja1N0YXJ0RGVwdGgpIHtcbiAgICAgIHRoaXMuX2Jsb2NrQ2hpbGRyZW4ucHVzaChub2RlKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogTWFya3MgdGhlIHN0YXJ0IG9mIGEgc2VjdGlvbiwgc2VlIGBfY2xvc2VUcmFuc2xhdGFibGVTZWN0aW9uYFxuICAgKi9cbiAgcHJpdmF0ZSBfb3BlblRyYW5zbGF0YWJsZVNlY3Rpb24obm9kZTogaHRtbC5Ob2RlKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuX2lzSW5UcmFuc2xhdGFibGVTZWN0aW9uKSB7XG4gICAgICB0aGlzLl9yZXBvcnRFcnJvcihub2RlLCAnVW5leHBlY3RlZCBzZWN0aW9uIHN0YXJ0Jyk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX21zZ0NvdW50QXRTZWN0aW9uU3RhcnQgPSB0aGlzLl9tZXNzYWdlcy5sZW5ndGg7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEEgdHJhbnNsYXRhYmxlIHNlY3Rpb24gY291bGQgYmU6XG4gICAqIC0gdGhlIGNvbnRlbnQgb2YgdHJhbnNsYXRhYmxlIGVsZW1lbnQsXG4gICAqIC0gbm9kZXMgYmV0d2VlbiBgPCEtLSBpMThuIC0tPmAgYW5kIGA8IS0tIC9pMThuIC0tPmAgY29tbWVudHNcbiAgICovXG4gIHByaXZhdGUgZ2V0IF9pc0luVHJhbnNsYXRhYmxlU2VjdGlvbigpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5fbXNnQ291bnRBdFNlY3Rpb25TdGFydCAhPT0gdm9pZCAwO1xuICB9XG5cbiAgLyoqXG4gICAqIFRlcm1pbmF0ZXMgYSBzZWN0aW9uLlxuICAgKlxuICAgKiBJZiBhIHNlY3Rpb24gaGFzIG9ubHkgb25lIHNpZ25pZmljYW50IGNoaWxkcmVuIChjb21tZW50cyBub3Qgc2lnbmlmaWNhbnQpIHRoZW4gd2Ugc2hvdWxkIG5vdFxuICAgKiBrZWVwIHRoZSBtZXNzYWdlIGZyb20gdGhpcyBjaGlsZHJlbjpcbiAgICpcbiAgICogYDxwIGkxOG49XCJtZWFuaW5nfGRlc2NyaXB0aW9uXCI+e0lDVSBtZXNzYWdlfTwvcD5gIHdvdWxkIHByb2R1Y2UgdHdvIG1lc3NhZ2VzOlxuICAgKiAtIG9uZSBmb3IgdGhlIDxwPiBjb250ZW50IHdpdGggbWVhbmluZyBhbmQgZGVzY3JpcHRpb24sXG4gICAqIC0gYW5vdGhlciBvbmUgZm9yIHRoZSBJQ1UgbWVzc2FnZS5cbiAgICpcbiAgICogSW4gdGhpcyBjYXNlIHRoZSBsYXN0IG1lc3NhZ2UgaXMgZGlzY2FyZGVkIGFzIGl0IGNvbnRhaW5zIGxlc3MgaW5mb3JtYXRpb24gKHRoZSBBU1QgaXNcbiAgICogb3RoZXJ3aXNlIGlkZW50aWNhbCkuXG4gICAqXG4gICAqIE5vdGUgdGhhdCB3ZSBzaG91bGQgc3RpbGwga2VlcCBtZXNzYWdlcyBleHRyYWN0ZWQgZnJvbSBhdHRyaWJ1dGVzIGluc2lkZSB0aGUgc2VjdGlvbiAoaWUgaW4gdGhlXG4gICAqIElDVSBtZXNzYWdlIGhlcmUpXG4gICAqL1xuICBwcml2YXRlIF9jbG9zZVRyYW5zbGF0YWJsZVNlY3Rpb24obm9kZTogaHRtbC5Ob2RlLCBkaXJlY3RDaGlsZHJlbjogaHRtbC5Ob2RlW10pOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMuX2lzSW5UcmFuc2xhdGFibGVTZWN0aW9uKSB7XG4gICAgICB0aGlzLl9yZXBvcnRFcnJvcihub2RlLCAnVW5leHBlY3RlZCBzZWN0aW9uIGVuZCcpO1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IHN0YXJ0SW5kZXggPSB0aGlzLl9tc2dDb3VudEF0U2VjdGlvblN0YXJ0O1xuICAgIGNvbnN0IHNpZ25pZmljYW50Q2hpbGRyZW46IG51bWJlciA9IGRpcmVjdENoaWxkcmVuLnJlZHVjZShcbiAgICAgICAgKGNvdW50OiBudW1iZXIsIG5vZGU6IGh0bWwuTm9kZSk6IG51bWJlciA9PiBjb3VudCArIChub2RlIGluc3RhbmNlb2YgaHRtbC5Db21tZW50ID8gMCA6IDEpLFxuICAgICAgICAwKTtcblxuICAgIGlmIChzaWduaWZpY2FudENoaWxkcmVuID09IDEpIHtcbiAgICAgIGZvciAobGV0IGkgPSB0aGlzLl9tZXNzYWdlcy5sZW5ndGggLSAxOyBpID49IHN0YXJ0SW5kZXggITsgaS0tKSB7XG4gICAgICAgIGNvbnN0IGFzdCA9IHRoaXMuX21lc3NhZ2VzW2ldLm5vZGVzO1xuICAgICAgICBpZiAoIShhc3QubGVuZ3RoID09IDEgJiYgYXN0WzBdIGluc3RhbmNlb2YgaTE4bi5UZXh0KSkge1xuICAgICAgICAgIHRoaXMuX21lc3NhZ2VzLnNwbGljZShpLCAxKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHRoaXMuX21zZ0NvdW50QXRTZWN0aW9uU3RhcnQgPSB1bmRlZmluZWQ7XG4gIH1cblxuICBwcml2YXRlIF9yZXBvcnRFcnJvcihub2RlOiBodG1sLk5vZGUsIG1zZzogc3RyaW5nKTogdm9pZCB7XG4gICAgdGhpcy5fZXJyb3JzLnB1c2gobmV3IEkxOG5FcnJvcihub2RlLnNvdXJjZVNwYW4gISwgbXNnKSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gX2lzT3BlbmluZ0NvbW1lbnQobjogaHRtbC5Ob2RlKTogYm9vbGVhbiB7XG4gIHJldHVybiAhIShuIGluc3RhbmNlb2YgaHRtbC5Db21tZW50ICYmIG4udmFsdWUgJiYgbi52YWx1ZS5zdGFydHNXaXRoKCdpMThuJykpO1xufVxuXG5mdW5jdGlvbiBfaXNDbG9zaW5nQ29tbWVudChuOiBodG1sLk5vZGUpOiBib29sZWFuIHtcbiAgcmV0dXJuICEhKG4gaW5zdGFuY2VvZiBodG1sLkNvbW1lbnQgJiYgbi52YWx1ZSAmJiBuLnZhbHVlID09PSAnL2kxOG4nKTtcbn1cblxuZnVuY3Rpb24gX2dldEkxOG5BdHRyKHA6IGh0bWwuRWxlbWVudCk6IGh0bWwuQXR0cmlidXRlfG51bGwge1xuICByZXR1cm4gcC5hdHRycy5maW5kKGF0dHIgPT4gYXR0ci5uYW1lID09PSBfSTE4Tl9BVFRSKSB8fCBudWxsO1xufVxuXG5mdW5jdGlvbiBfcGFyc2VNZXNzYWdlTWV0YShpMThuPzogc3RyaW5nKToge21lYW5pbmc6IHN0cmluZywgZGVzY3JpcHRpb246IHN0cmluZywgaWQ6IHN0cmluZ30ge1xuICBpZiAoIWkxOG4pIHJldHVybiB7bWVhbmluZzogJycsIGRlc2NyaXB0aW9uOiAnJywgaWQ6ICcnfTtcblxuICBjb25zdCBpZEluZGV4ID0gaTE4bi5pbmRleE9mKElEX1NFUEFSQVRPUik7XG4gIGNvbnN0IGRlc2NJbmRleCA9IGkxOG4uaW5kZXhPZihNRUFOSU5HX1NFUEFSQVRPUik7XG4gIGNvbnN0IFttZWFuaW5nQW5kRGVzYywgaWRdID1cbiAgICAgIChpZEluZGV4ID4gLTEpID8gW2kxOG4uc2xpY2UoMCwgaWRJbmRleCksIGkxOG4uc2xpY2UoaWRJbmRleCArIDIpXSA6IFtpMThuLCAnJ107XG4gIGNvbnN0IFttZWFuaW5nLCBkZXNjcmlwdGlvbl0gPSAoZGVzY0luZGV4ID4gLTEpID9cbiAgICAgIFttZWFuaW5nQW5kRGVzYy5zbGljZSgwLCBkZXNjSW5kZXgpLCBtZWFuaW5nQW5kRGVzYy5zbGljZShkZXNjSW5kZXggKyAxKV0gOlxuICAgICAgWycnLCBtZWFuaW5nQW5kRGVzY107XG5cbiAgcmV0dXJuIHttZWFuaW5nLCBkZXNjcmlwdGlvbiwgaWR9O1xufVxuIl19