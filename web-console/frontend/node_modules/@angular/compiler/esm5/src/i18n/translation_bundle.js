/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { MissingTranslationStrategy } from '../core';
import { HtmlParser } from '../ml_parser/html_parser';
import { I18nError } from './parse_util';
import { escapeXml } from './serializers/xml_helper';
/**
 * A container for translated messages
 */
var TranslationBundle = /** @class */ (function () {
    function TranslationBundle(_i18nNodesByMsgId, locale, digest, mapperFactory, missingTranslationStrategy, console) {
        if (_i18nNodesByMsgId === void 0) { _i18nNodesByMsgId = {}; }
        if (missingTranslationStrategy === void 0) { missingTranslationStrategy = MissingTranslationStrategy.Warning; }
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
export { TranslationBundle };
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
        var html = new HtmlParser().parse(text, url, { tokenizeExpansionForms: true });
        return {
            nodes: html.rootNodes,
            errors: tslib_1.__spread(this._errors, html.errors),
        };
    };
    I18nToHtmlVisitor.prototype.visitText = function (text, context) {
        // `convert()` uses an `HtmlParser` to return `html.Node`s
        // we should then make sure that any special characters are escaped
        return escapeXml(text.value);
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
            if (this._missingTranslationStrategy === MissingTranslationStrategy.Error) {
                var ctx = this._locale ? " for locale \"" + this._locale + "\"" : '';
                this._addError(srcMsg.nodes[0], "Missing translation for message \"" + id + "\"" + ctx);
            }
            else if (this._console &&
                this._missingTranslationStrategy === MissingTranslationStrategy.Warning) {
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
        this._errors.push(new I18nError(el.sourceSpan, msg));
    };
    return I18nToHtmlVisitor;
}());
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidHJhbnNsYXRpb25fYnVuZGxlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2kxOG4vdHJhbnNsYXRpb25fYnVuZGxlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsMEJBQTBCLEVBQUMsTUFBTSxTQUFTLENBQUM7QUFFbkQsT0FBTyxFQUFDLFVBQVUsRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBSXBELE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFFdkMsT0FBTyxFQUFDLFNBQVMsRUFBQyxNQUFNLDBCQUEwQixDQUFDO0FBR25EOztHQUVHO0FBQ0g7SUFHRSwyQkFDWSxpQkFBc0QsRUFBRSxNQUFtQixFQUM1RSxNQUFtQyxFQUNuQyxhQUFzRCxFQUM3RCwwQkFBMkYsRUFDM0YsT0FBaUI7UUFKVCxrQ0FBQSxFQUFBLHNCQUFzRDtRQUc5RCwyQ0FBQSxFQUFBLDZCQUF5RCwwQkFBMEIsQ0FBQyxPQUFPO1FBSG5GLHNCQUFpQixHQUFqQixpQkFBaUIsQ0FBcUM7UUFDdkQsV0FBTSxHQUFOLE1BQU0sQ0FBNkI7UUFDbkMsa0JBQWEsR0FBYixhQUFhLENBQXlDO1FBRy9ELElBQUksQ0FBQyxXQUFXLEdBQUcsSUFBSSxpQkFBaUIsQ0FDcEMsaUJBQWlCLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxhQUFlLEVBQUUsMEJBQTBCLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDL0YsQ0FBQztJQUVELHNGQUFzRjtJQUMvRSxzQkFBSSxHQUFYLFVBQ0ksT0FBZSxFQUFFLEdBQVcsRUFBRSxVQUFzQixFQUNwRCwwQkFBc0QsRUFDdEQsT0FBaUI7UUFDYixJQUFBLGtDQUEwRCxFQUF6RCxrQkFBTSxFQUFFLHNDQUFpRCxDQUFDO1FBQ2pFLElBQU0sUUFBUSxHQUFHLFVBQUMsQ0FBZSxJQUFLLE9B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QUFJLENBQUMsSUFBSSxTQUFTLENBQUMsRUFBRSxDQUFDLFVBQVUsRUFBRSxHQUFHLENBQUMsQ0FBQyxDQUFDO0lBQ3ZELENBQUM7SUFDSCx3QkFBQztBQUFELENBQUMsQUF2SUQsSUF1SUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7TWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3l9IGZyb20gJy4uL2NvcmUnO1xuaW1wb3J0ICogYXMgaHRtbCBmcm9tICcuLi9tbF9wYXJzZXIvYXN0JztcbmltcG9ydCB7SHRtbFBhcnNlcn0gZnJvbSAnLi4vbWxfcGFyc2VyL2h0bWxfcGFyc2VyJztcbmltcG9ydCB7Q29uc29sZX0gZnJvbSAnLi4vdXRpbCc7XG5cbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi9pMThuX2FzdCc7XG5pbXBvcnQge0kxOG5FcnJvcn0gZnJvbSAnLi9wYXJzZV91dGlsJztcbmltcG9ydCB7UGxhY2Vob2xkZXJNYXBwZXIsIFNlcmlhbGl6ZXJ9IGZyb20gJy4vc2VyaWFsaXplcnMvc2VyaWFsaXplcic7XG5pbXBvcnQge2VzY2FwZVhtbH0gZnJvbSAnLi9zZXJpYWxpemVycy94bWxfaGVscGVyJztcblxuXG4vKipcbiAqIEEgY29udGFpbmVyIGZvciB0cmFuc2xhdGVkIG1lc3NhZ2VzXG4gKi9cbmV4cG9ydCBjbGFzcyBUcmFuc2xhdGlvbkJ1bmRsZSB7XG4gIHByaXZhdGUgX2kxOG5Ub0h0bWw6IEkxOG5Ub0h0bWxWaXNpdG9yO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBfaTE4bk5vZGVzQnlNc2dJZDoge1ttc2dJZDogc3RyaW5nXTogaTE4bi5Ob2RlW119ID0ge30sIGxvY2FsZTogc3RyaW5nfG51bGwsXG4gICAgICBwdWJsaWMgZGlnZXN0OiAobTogaTE4bi5NZXNzYWdlKSA9PiBzdHJpbmcsXG4gICAgICBwdWJsaWMgbWFwcGVyRmFjdG9yeT86IChtOiBpMThuLk1lc3NhZ2UpID0+IFBsYWNlaG9sZGVyTWFwcGVyLFxuICAgICAgbWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3k6IE1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5ID0gTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3kuV2FybmluZyxcbiAgICAgIGNvbnNvbGU/OiBDb25zb2xlKSB7XG4gICAgdGhpcy5faTE4blRvSHRtbCA9IG5ldyBJMThuVG9IdG1sVmlzaXRvcihcbiAgICAgICAgX2kxOG5Ob2Rlc0J5TXNnSWQsIGxvY2FsZSwgZGlnZXN0LCBtYXBwZXJGYWN0b3J5ICEsIG1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5LCBjb25zb2xlKTtcbiAgfVxuXG4gIC8vIENyZWF0ZXMgYSBgVHJhbnNsYXRpb25CdW5kbGVgIGJ5IHBhcnNpbmcgdGhlIGdpdmVuIGBjb250ZW50YCB3aXRoIHRoZSBgc2VyaWFsaXplcmAuXG4gIHN0YXRpYyBsb2FkKFxuICAgICAgY29udGVudDogc3RyaW5nLCB1cmw6IHN0cmluZywgc2VyaWFsaXplcjogU2VyaWFsaXplcixcbiAgICAgIG1pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5OiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSxcbiAgICAgIGNvbnNvbGU/OiBDb25zb2xlKTogVHJhbnNsYXRpb25CdW5kbGUge1xuICAgIGNvbnN0IHtsb2NhbGUsIGkxOG5Ob2Rlc0J5TXNnSWR9ID0gc2VyaWFsaXplci5sb2FkKGNvbnRlbnQsIHVybCk7XG4gICAgY29uc3QgZGlnZXN0Rm4gPSAobTogaTE4bi5NZXNzYWdlKSA9PiBzZXJpYWxpemVyLmRpZ2VzdChtKTtcbiAgICBjb25zdCBtYXBwZXJGYWN0b3J5ID0gKG06IGkxOG4uTWVzc2FnZSkgPT4gc2VyaWFsaXplci5jcmVhdGVOYW1lTWFwcGVyKG0pICE7XG4gICAgcmV0dXJuIG5ldyBUcmFuc2xhdGlvbkJ1bmRsZShcbiAgICAgICAgaTE4bk5vZGVzQnlNc2dJZCwgbG9jYWxlLCBkaWdlc3RGbiwgbWFwcGVyRmFjdG9yeSwgbWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3ksIGNvbnNvbGUpO1xuICB9XG5cbiAgLy8gUmV0dXJucyB0aGUgdHJhbnNsYXRpb24gYXMgSFRNTCBub2RlcyBmcm9tIHRoZSBnaXZlbiBzb3VyY2UgbWVzc2FnZS5cbiAgZ2V0KHNyY01zZzogaTE4bi5NZXNzYWdlKTogaHRtbC5Ob2RlW10ge1xuICAgIGNvbnN0IGh0bWwgPSB0aGlzLl9pMThuVG9IdG1sLmNvbnZlcnQoc3JjTXNnKTtcblxuICAgIGlmIChodG1sLmVycm9ycy5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihodG1sLmVycm9ycy5qb2luKCdcXG4nKSk7XG4gICAgfVxuXG4gICAgcmV0dXJuIGh0bWwubm9kZXM7XG4gIH1cblxuICBoYXMoc3JjTXNnOiBpMThuLk1lc3NhZ2UpOiBib29sZWFuIHsgcmV0dXJuIHRoaXMuZGlnZXN0KHNyY01zZykgaW4gdGhpcy5faTE4bk5vZGVzQnlNc2dJZDsgfVxufVxuXG5jbGFzcyBJMThuVG9IdG1sVmlzaXRvciBpbXBsZW1lbnRzIGkxOG4uVmlzaXRvciB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9zcmNNc2cgITogaTE4bi5NZXNzYWdlO1xuICBwcml2YXRlIF9jb250ZXh0U3RhY2s6IHttc2c6IGkxOG4uTWVzc2FnZSwgbWFwcGVyOiAobmFtZTogc3RyaW5nKSA9PiBzdHJpbmd9W10gPSBbXTtcbiAgcHJpdmF0ZSBfZXJyb3JzOiBJMThuRXJyb3JbXSA9IFtdO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfbWFwcGVyICE6IChuYW1lOiBzdHJpbmcpID0+IHN0cmluZztcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgX2kxOG5Ob2Rlc0J5TXNnSWQ6IHtbbXNnSWQ6IHN0cmluZ106IGkxOG4uTm9kZVtdfSA9IHt9LCBwcml2YXRlIF9sb2NhbGU6IHN0cmluZ3xudWxsLFxuICAgICAgcHJpdmF0ZSBfZGlnZXN0OiAobTogaTE4bi5NZXNzYWdlKSA9PiBzdHJpbmcsXG4gICAgICBwcml2YXRlIF9tYXBwZXJGYWN0b3J5OiAobTogaTE4bi5NZXNzYWdlKSA9PiBQbGFjZWhvbGRlck1hcHBlcixcbiAgICAgIHByaXZhdGUgX21pc3NpbmdUcmFuc2xhdGlvblN0cmF0ZWd5OiBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSwgcHJpdmF0ZSBfY29uc29sZT86IENvbnNvbGUpIHtcbiAgfVxuXG4gIGNvbnZlcnQoc3JjTXNnOiBpMThuLk1lc3NhZ2UpOiB7bm9kZXM6IGh0bWwuTm9kZVtdLCBlcnJvcnM6IEkxOG5FcnJvcltdfSB7XG4gICAgdGhpcy5fY29udGV4dFN0YWNrLmxlbmd0aCA9IDA7XG4gICAgdGhpcy5fZXJyb3JzLmxlbmd0aCA9IDA7XG5cbiAgICAvLyBpMThuIHRvIHRleHRcbiAgICBjb25zdCB0ZXh0ID0gdGhpcy5fY29udmVydFRvVGV4dChzcmNNc2cpO1xuXG4gICAgLy8gdGV4dCB0byBodG1sXG4gICAgY29uc3QgdXJsID0gc3JjTXNnLm5vZGVzWzBdLnNvdXJjZVNwYW4uc3RhcnQuZmlsZS51cmw7XG4gICAgY29uc3QgaHRtbCA9IG5ldyBIdG1sUGFyc2VyKCkucGFyc2UodGV4dCwgdXJsLCB7dG9rZW5pemVFeHBhbnNpb25Gb3JtczogdHJ1ZX0pO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIG5vZGVzOiBodG1sLnJvb3ROb2RlcyxcbiAgICAgIGVycm9yczogWy4uLnRoaXMuX2Vycm9ycywgLi4uaHRtbC5lcnJvcnNdLFxuICAgIH07XG4gIH1cblxuICB2aXNpdFRleHQodGV4dDogaTE4bi5UZXh0LCBjb250ZXh0PzogYW55KTogc3RyaW5nIHtcbiAgICAvLyBgY29udmVydCgpYCB1c2VzIGFuIGBIdG1sUGFyc2VyYCB0byByZXR1cm4gYGh0bWwuTm9kZWBzXG4gICAgLy8gd2Ugc2hvdWxkIHRoZW4gbWFrZSBzdXJlIHRoYXQgYW55IHNwZWNpYWwgY2hhcmFjdGVycyBhcmUgZXNjYXBlZFxuICAgIHJldHVybiBlc2NhcGVYbWwodGV4dC52YWx1ZSk7XG4gIH1cblxuICB2aXNpdENvbnRhaW5lcihjb250YWluZXI6IGkxOG4uQ29udGFpbmVyLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gY29udGFpbmVyLmNoaWxkcmVuLm1hcChuID0+IG4udmlzaXQodGhpcykpLmpvaW4oJycpO1xuICB9XG5cbiAgdmlzaXRJY3UoaWN1OiBpMThuLkljdSwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgY29uc3QgY2FzZXMgPSBPYmplY3Qua2V5cyhpY3UuY2FzZXMpLm1hcChrID0+IGAke2t9IHske2ljdS5jYXNlc1trXS52aXNpdCh0aGlzKX19YCk7XG5cbiAgICAvLyBUT0RPKHZpY2IpOiBPbmNlIGFsbCBmb3JtYXQgc3dpdGNoIHRvIHVzaW5nIGV4cHJlc3Npb24gcGxhY2Vob2xkZXJzXG4gICAgLy8gd2Ugc2hvdWxkIHRocm93IHdoZW4gdGhlIHBsYWNlaG9sZGVyIGlzIG5vdCBpbiB0aGUgc291cmNlIG1lc3NhZ2VcbiAgICBjb25zdCBleHAgPSB0aGlzLl9zcmNNc2cucGxhY2Vob2xkZXJzLmhhc093blByb3BlcnR5KGljdS5leHByZXNzaW9uKSA/XG4gICAgICAgIHRoaXMuX3NyY01zZy5wbGFjZWhvbGRlcnNbaWN1LmV4cHJlc3Npb25dIDpcbiAgICAgICAgaWN1LmV4cHJlc3Npb247XG5cbiAgICByZXR1cm4gYHske2V4cH0sICR7aWN1LnR5cGV9LCAke2Nhc2VzLmpvaW4oJyAnKX19YDtcbiAgfVxuXG4gIHZpc2l0UGxhY2Vob2xkZXIocGg6IGkxOG4uUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBzdHJpbmcge1xuICAgIGNvbnN0IHBoTmFtZSA9IHRoaXMuX21hcHBlcihwaC5uYW1lKTtcbiAgICBpZiAodGhpcy5fc3JjTXNnLnBsYWNlaG9sZGVycy5oYXNPd25Qcm9wZXJ0eShwaE5hbWUpKSB7XG4gICAgICByZXR1cm4gdGhpcy5fc3JjTXNnLnBsYWNlaG9sZGVyc1twaE5hbWVdO1xuICAgIH1cblxuICAgIGlmICh0aGlzLl9zcmNNc2cucGxhY2Vob2xkZXJUb01lc3NhZ2UuaGFzT3duUHJvcGVydHkocGhOYW1lKSkge1xuICAgICAgcmV0dXJuIHRoaXMuX2NvbnZlcnRUb1RleHQodGhpcy5fc3JjTXNnLnBsYWNlaG9sZGVyVG9NZXNzYWdlW3BoTmFtZV0pO1xuICAgIH1cblxuICAgIHRoaXMuX2FkZEVycm9yKHBoLCBgVW5rbm93biBwbGFjZWhvbGRlciBcIiR7cGgubmFtZX1cImApO1xuICAgIHJldHVybiAnJztcbiAgfVxuXG4gIC8vIExvYWRlZCBtZXNzYWdlIGNvbnRhaW5zIG9ubHkgcGxhY2Vob2xkZXJzICh2cyB0YWcgYW5kIGljdSBwbGFjZWhvbGRlcnMpLlxuICAvLyBIb3dldmVyIHdoZW4gYSB0cmFuc2xhdGlvbiBjYW4gbm90IGJlIGZvdW5kLCB3ZSBuZWVkIHRvIHNlcmlhbGl6ZSB0aGUgc291cmNlIG1lc3NhZ2VcbiAgLy8gd2hpY2ggY2FuIGNvbnRhaW4gdGFnIHBsYWNlaG9sZGVyc1xuICB2aXNpdFRhZ1BsYWNlaG9sZGVyKHBoOiBpMThuLlRhZ1BsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogc3RyaW5nIHtcbiAgICBjb25zdCB0YWcgPSBgJHtwaC50YWd9YDtcbiAgICBjb25zdCBhdHRycyA9IE9iamVjdC5rZXlzKHBoLmF0dHJzKS5tYXAobmFtZSA9PiBgJHtuYW1lfT1cIiR7cGguYXR0cnNbbmFtZV19XCJgKS5qb2luKCcgJyk7XG4gICAgaWYgKHBoLmlzVm9pZCkge1xuICAgICAgcmV0dXJuIGA8JHt0YWd9ICR7YXR0cnN9Lz5gO1xuICAgIH1cbiAgICBjb25zdCBjaGlsZHJlbiA9IHBoLmNoaWxkcmVuLm1hcCgoYzogaTE4bi5Ob2RlKSA9PiBjLnZpc2l0KHRoaXMpKS5qb2luKCcnKTtcbiAgICByZXR1cm4gYDwke3RhZ30gJHthdHRyc30+JHtjaGlsZHJlbn08LyR7dGFnfT5gO1xuICB9XG5cbiAgLy8gTG9hZGVkIG1lc3NhZ2UgY29udGFpbnMgb25seSBwbGFjZWhvbGRlcnMgKHZzIHRhZyBhbmQgaWN1IHBsYWNlaG9sZGVycykuXG4gIC8vIEhvd2V2ZXIgd2hlbiBhIHRyYW5zbGF0aW9uIGNhbiBub3QgYmUgZm91bmQsIHdlIG5lZWQgdG8gc2VyaWFsaXplIHRoZSBzb3VyY2UgbWVzc2FnZVxuICAvLyB3aGljaCBjYW4gY29udGFpbiB0YWcgcGxhY2Vob2xkZXJzXG4gIHZpc2l0SWN1UGxhY2Vob2xkZXIocGg6IGkxOG4uSWN1UGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiBzdHJpbmcge1xuICAgIC8vIEFuIElDVSBwbGFjZWhvbGRlciByZWZlcmVuY2VzIHRoZSBzb3VyY2UgbWVzc2FnZSB0byBiZSBzZXJpYWxpemVkXG4gICAgcmV0dXJuIHRoaXMuX2NvbnZlcnRUb1RleHQodGhpcy5fc3JjTXNnLnBsYWNlaG9sZGVyVG9NZXNzYWdlW3BoLm5hbWVdKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBDb252ZXJ0IGEgc291cmNlIG1lc3NhZ2UgdG8gYSB0cmFuc2xhdGVkIHRleHQgc3RyaW5nOlxuICAgKiAtIHRleHQgbm9kZXMgYXJlIHJlcGxhY2VkIHdpdGggdGhlaXIgdHJhbnNsYXRpb24sXG4gICAqIC0gcGxhY2Vob2xkZXJzIGFyZSByZXBsYWNlZCB3aXRoIHRoZWlyIGNvbnRlbnQsXG4gICAqIC0gSUNVIG5vZGVzIGFyZSBjb252ZXJ0ZWQgdG8gSUNVIGV4cHJlc3Npb25zLlxuICAgKi9cbiAgcHJpdmF0ZSBfY29udmVydFRvVGV4dChzcmNNc2c6IGkxOG4uTWVzc2FnZSk6IHN0cmluZyB7XG4gICAgY29uc3QgaWQgPSB0aGlzLl9kaWdlc3Qoc3JjTXNnKTtcbiAgICBjb25zdCBtYXBwZXIgPSB0aGlzLl9tYXBwZXJGYWN0b3J5ID8gdGhpcy5fbWFwcGVyRmFjdG9yeShzcmNNc2cpIDogbnVsbDtcbiAgICBsZXQgbm9kZXM6IGkxOG4uTm9kZVtdO1xuXG4gICAgdGhpcy5fY29udGV4dFN0YWNrLnB1c2goe21zZzogdGhpcy5fc3JjTXNnLCBtYXBwZXI6IHRoaXMuX21hcHBlcn0pO1xuICAgIHRoaXMuX3NyY01zZyA9IHNyY01zZztcblxuICAgIGlmICh0aGlzLl9pMThuTm9kZXNCeU1zZ0lkLmhhc093blByb3BlcnR5KGlkKSkge1xuICAgICAgLy8gV2hlbiB0aGVyZSBpcyBhIHRyYW5zbGF0aW9uIHVzZSBpdHMgbm9kZXMgYXMgdGhlIHNvdXJjZVxuICAgICAgLy8gQW5kIGNyZWF0ZSBhIG1hcHBlciB0byBjb252ZXJ0IHNlcmlhbGl6ZWQgcGxhY2Vob2xkZXIgbmFtZXMgdG8gaW50ZXJuYWwgbmFtZXNcbiAgICAgIG5vZGVzID0gdGhpcy5faTE4bk5vZGVzQnlNc2dJZFtpZF07XG4gICAgICB0aGlzLl9tYXBwZXIgPSAobmFtZTogc3RyaW5nKSA9PiBtYXBwZXIgPyBtYXBwZXIudG9JbnRlcm5hbE5hbWUobmFtZSkgISA6IG5hbWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIFdoZW4gbm8gdHJhbnNsYXRpb24gaGFzIGJlZW4gZm91bmRcbiAgICAgIC8vIC0gcmVwb3J0IGFuIGVycm9yIC8gYSB3YXJuaW5nIC8gbm90aGluZyxcbiAgICAgIC8vIC0gdXNlIHRoZSBub2RlcyBmcm9tIHRoZSBvcmlnaW5hbCBtZXNzYWdlXG4gICAgICAvLyAtIHBsYWNlaG9sZGVycyBhcmUgYWxyZWFkeSBpbnRlcm5hbCBhbmQgbmVlZCBubyBtYXBwZXJcbiAgICAgIGlmICh0aGlzLl9taXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSA9PT0gTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3kuRXJyb3IpIHtcbiAgICAgICAgY29uc3QgY3R4ID0gdGhpcy5fbG9jYWxlID8gYCBmb3IgbG9jYWxlIFwiJHt0aGlzLl9sb2NhbGV9XCJgIDogJyc7XG4gICAgICAgIHRoaXMuX2FkZEVycm9yKHNyY01zZy5ub2Rlc1swXSwgYE1pc3NpbmcgdHJhbnNsYXRpb24gZm9yIG1lc3NhZ2UgXCIke2lkfVwiJHtjdHh9YCk7XG4gICAgICB9IGVsc2UgaWYgKFxuICAgICAgICAgIHRoaXMuX2NvbnNvbGUgJiZcbiAgICAgICAgICB0aGlzLl9taXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneSA9PT0gTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3kuV2FybmluZykge1xuICAgICAgICBjb25zdCBjdHggPSB0aGlzLl9sb2NhbGUgPyBgIGZvciBsb2NhbGUgXCIke3RoaXMuX2xvY2FsZX1cImAgOiAnJztcbiAgICAgICAgdGhpcy5fY29uc29sZS53YXJuKGBNaXNzaW5nIHRyYW5zbGF0aW9uIGZvciBtZXNzYWdlIFwiJHtpZH1cIiR7Y3R4fWApO1xuICAgICAgfVxuICAgICAgbm9kZXMgPSBzcmNNc2cubm9kZXM7XG4gICAgICB0aGlzLl9tYXBwZXIgPSAobmFtZTogc3RyaW5nKSA9PiBuYW1lO1xuICAgIH1cbiAgICBjb25zdCB0ZXh0ID0gbm9kZXMubWFwKG5vZGUgPT4gbm9kZS52aXNpdCh0aGlzKSkuam9pbignJyk7XG4gICAgY29uc3QgY29udGV4dCA9IHRoaXMuX2NvbnRleHRTdGFjay5wb3AoKSAhO1xuICAgIHRoaXMuX3NyY01zZyA9IGNvbnRleHQubXNnO1xuICAgIHRoaXMuX21hcHBlciA9IGNvbnRleHQubWFwcGVyO1xuICAgIHJldHVybiB0ZXh0O1xuICB9XG5cbiAgcHJpdmF0ZSBfYWRkRXJyb3IoZWw6IGkxOG4uTm9kZSwgbXNnOiBzdHJpbmcpIHtcbiAgICB0aGlzLl9lcnJvcnMucHVzaChuZXcgSTE4bkVycm9yKGVsLnNvdXJjZVNwYW4sIG1zZykpO1xuICB9XG59XG4iXX0=