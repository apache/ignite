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
        define("@angular/compiler/src/i18n/i18n_parser", ["require", "exports", "@angular/compiler/src/expression_parser/lexer", "@angular/compiler/src/expression_parser/parser", "@angular/compiler/src/ml_parser/ast", "@angular/compiler/src/ml_parser/html_tags", "@angular/compiler/src/i18n/i18n_ast", "@angular/compiler/src/i18n/serializers/placeholder"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var lexer_1 = require("@angular/compiler/src/expression_parser/lexer");
    var parser_1 = require("@angular/compiler/src/expression_parser/parser");
    var html = require("@angular/compiler/src/ml_parser/ast");
    var html_tags_1 = require("@angular/compiler/src/ml_parser/html_tags");
    var i18n = require("@angular/compiler/src/i18n/i18n_ast");
    var placeholder_1 = require("@angular/compiler/src/i18n/serializers/placeholder");
    var _expParser = new parser_1.Parser(new lexer_1.Lexer());
    /**
     * Returns a function converting html nodes to an i18n Message given an interpolationConfig
     */
    function createI18nMessageFactory(interpolationConfig) {
        var visitor = new _I18nVisitor(_expParser, interpolationConfig);
        return function (nodes, meaning, description, id, visitNodeFn) {
            return visitor.toI18nMessage(nodes, meaning, description, id, visitNodeFn);
        };
    }
    exports.createI18nMessageFactory = createI18nMessageFactory;
    var _I18nVisitor = /** @class */ (function () {
        function _I18nVisitor(_expressionParser, _interpolationConfig) {
            this._expressionParser = _expressionParser;
            this._interpolationConfig = _interpolationConfig;
        }
        _I18nVisitor.prototype.toI18nMessage = function (nodes, meaning, description, id, visitNodeFn) {
            this._isIcu = nodes.length == 1 && nodes[0] instanceof html.Expansion;
            this._icuDepth = 0;
            this._placeholderRegistry = new placeholder_1.PlaceholderRegistry();
            this._placeholderToContent = {};
            this._placeholderToMessage = {};
            this._visitNodeFn = visitNodeFn;
            var i18nodes = html.visitAll(this, nodes, {});
            return new i18n.Message(i18nodes, this._placeholderToContent, this._placeholderToMessage, meaning, description, id);
        };
        _I18nVisitor.prototype._visitNode = function (html, i18n) {
            if (this._visitNodeFn) {
                this._visitNodeFn(html, i18n);
            }
            return i18n;
        };
        _I18nVisitor.prototype.visitElement = function (el, context) {
            var children = html.visitAll(this, el.children);
            var attrs = {};
            el.attrs.forEach(function (attr) {
                // Do not visit the attributes, translatable ones are top-level ASTs
                attrs[attr.name] = attr.value;
            });
            var isVoid = html_tags_1.getHtmlTagDefinition(el.name).isVoid;
            var startPhName = this._placeholderRegistry.getStartTagPlaceholderName(el.name, attrs, isVoid);
            this._placeholderToContent[startPhName] = el.sourceSpan.toString();
            var closePhName = '';
            if (!isVoid) {
                closePhName = this._placeholderRegistry.getCloseTagPlaceholderName(el.name);
                this._placeholderToContent[closePhName] = "</" + el.name + ">";
            }
            var node = new i18n.TagPlaceholder(el.name, attrs, startPhName, closePhName, children, isVoid, el.sourceSpan);
            return this._visitNode(el, node);
        };
        _I18nVisitor.prototype.visitAttribute = function (attribute, context) {
            var node = this._visitTextWithInterpolation(attribute.value, attribute.sourceSpan);
            return this._visitNode(attribute, node);
        };
        _I18nVisitor.prototype.visitText = function (text, context) {
            var node = this._visitTextWithInterpolation(text.value, text.sourceSpan);
            return this._visitNode(text, node);
        };
        _I18nVisitor.prototype.visitComment = function (comment, context) { return null; };
        _I18nVisitor.prototype.visitExpansion = function (icu, context) {
            var _this = this;
            this._icuDepth++;
            var i18nIcuCases = {};
            var i18nIcu = new i18n.Icu(icu.switchValue, icu.type, i18nIcuCases, icu.sourceSpan);
            icu.cases.forEach(function (caze) {
                i18nIcuCases[caze.value] = new i18n.Container(caze.expression.map(function (node) { return node.visit(_this, {}); }), caze.expSourceSpan);
            });
            this._icuDepth--;
            if (this._isIcu || this._icuDepth > 0) {
                // Returns an ICU node when:
                // - the message (vs a part of the message) is an ICU message, or
                // - the ICU message is nested.
                var expPh = this._placeholderRegistry.getUniquePlaceholder("VAR_" + icu.type);
                i18nIcu.expressionPlaceholder = expPh;
                this._placeholderToContent[expPh] = icu.switchValue;
                return this._visitNode(icu, i18nIcu);
            }
            // Else returns a placeholder
            // ICU placeholders should not be replaced with their original content but with the their
            // translations. We need to create a new visitor (they are not re-entrant) to compute the
            // message id.
            // TODO(vicb): add a html.Node -> i18n.Message cache to avoid having to re-create the msg
            var phName = this._placeholderRegistry.getPlaceholderName('ICU', icu.sourceSpan.toString());
            var visitor = new _I18nVisitor(this._expressionParser, this._interpolationConfig);
            this._placeholderToMessage[phName] = visitor.toI18nMessage([icu], '', '', '');
            var node = new i18n.IcuPlaceholder(i18nIcu, phName, icu.sourceSpan);
            return this._visitNode(icu, node);
        };
        _I18nVisitor.prototype.visitExpansionCase = function (icuCase, context) {
            throw new Error('Unreachable code');
        };
        _I18nVisitor.prototype._visitTextWithInterpolation = function (text, sourceSpan) {
            var splitInterpolation = this._expressionParser.splitInterpolation(text, sourceSpan.start.toString(), this._interpolationConfig);
            if (!splitInterpolation) {
                // No expression, return a single text
                return new i18n.Text(text, sourceSpan);
            }
            // Return a group of text + expressions
            var nodes = [];
            var container = new i18n.Container(nodes, sourceSpan);
            var _a = this._interpolationConfig, sDelimiter = _a.start, eDelimiter = _a.end;
            for (var i = 0; i < splitInterpolation.strings.length - 1; i++) {
                var expression = splitInterpolation.expressions[i];
                var baseName = _extractPlaceholderName(expression) || 'INTERPOLATION';
                var phName = this._placeholderRegistry.getPlaceholderName(baseName, expression);
                if (splitInterpolation.strings[i].length) {
                    // No need to add empty strings
                    nodes.push(new i18n.Text(splitInterpolation.strings[i], sourceSpan));
                }
                nodes.push(new i18n.Placeholder(expression, phName, sourceSpan));
                this._placeholderToContent[phName] = sDelimiter + expression + eDelimiter;
            }
            // The last index contains no expression
            var lastStringIdx = splitInterpolation.strings.length - 1;
            if (splitInterpolation.strings[lastStringIdx].length) {
                nodes.push(new i18n.Text(splitInterpolation.strings[lastStringIdx], sourceSpan));
            }
            return container;
        };
        return _I18nVisitor;
    }());
    var _CUSTOM_PH_EXP = /\/\/[\s\S]*i18n[\s\S]*\([\s\S]*ph[\s\S]*=[\s\S]*("|')([\s\S]*?)\1[\s\S]*\)/g;
    function _extractPlaceholderName(input) {
        return input.split(_CUSTOM_PH_EXP)[2];
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaTE4bl9wYXJzZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaTE4bi9pMThuX3BhcnNlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHVFQUFvRTtJQUNwRSx5RUFBdUU7SUFDdkUsMERBQXlDO0lBQ3pDLHVFQUE0RDtJQUk1RCwwREFBbUM7SUFDbkMsa0ZBQThEO0lBRTlELElBQU0sVUFBVSxHQUFHLElBQUksZUFBZ0IsQ0FBQyxJQUFJLGFBQWUsRUFBRSxDQUFDLENBQUM7SUFJL0Q7O09BRUc7SUFDSCxTQUFnQix3QkFBd0IsQ0FBQyxtQkFBd0M7UUFHL0UsSUFBTSxPQUFPLEdBQUcsSUFBSSxZQUFZLENBQUMsVUFBVSxFQUFFLG1CQUFtQixDQUFDLENBQUM7UUFFbEUsT0FBTyxVQUFDLEtBQWtCLEVBQUUsT0FBZSxFQUFFLFdBQW1CLEVBQUUsRUFBVSxFQUNwRSxXQUF5QjtZQUN0QixPQUFBLE9BQU8sQ0FBQyxhQUFhLENBQUMsS0FBSyxFQUFFLE9BQU8sRUFBRSxXQUFXLEVBQUUsRUFBRSxFQUFFLFdBQVcsQ0FBQztRQUFuRSxDQUFtRSxDQUFDO0lBQ2pGLENBQUM7SUFSRCw0REFRQztJQUVEO1FBYUUsc0JBQ1ksaUJBQW1DLEVBQ25DLG9CQUF5QztZQUR6QyxzQkFBaUIsR0FBakIsaUJBQWlCLENBQWtCO1lBQ25DLHlCQUFvQixHQUFwQixvQkFBb0IsQ0FBcUI7UUFBRyxDQUFDO1FBRWxELG9DQUFhLEdBQXBCLFVBQ0ksS0FBa0IsRUFBRSxPQUFlLEVBQUUsV0FBbUIsRUFBRSxFQUFVLEVBQ3BFLFdBQXlCO1lBQzNCLElBQUksQ0FBQyxNQUFNLEdBQUcsS0FBSyxDQUFDLE1BQU0sSUFBSSxDQUFDLElBQUksS0FBSyxDQUFDLENBQUMsQ0FBQyxZQUFZLElBQUksQ0FBQyxTQUFTLENBQUM7WUFDdEUsSUFB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QztnQkFDakUsSUFBSSxDQUFDLHFCQUFxQixDQUFDLE1BQU0sQ0FBQyxHQUFHLFVBQVUsR0FBRyxVQUFVLEdBQUcsVUFBVSxDQUFDO2FBQzNFO1lBRUQsd0NBQXdDO1lBQ3hDLElBQU0sYUFBYSxHQUFHLGtCQUFrQixDQUFDLE9BQU8sQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDO1lBQzVELElBQUksa0JBQWtCLENBQUMsT0FBTyxDQUFDLGFBQWEsQ0FBQyxDQUFDLE1BQU0sRUFBRTtnQkFDcEQsS0FBSyxDQUFDLElBQUksQ0FBQyxJQUFJLElBQUksQ0FBQyxJQUFJLENBQUMsa0JBQWtCLENBQUMsT0FBTyxDQUFDLGFBQWEsQ0FBQyxFQUFFLFVBQVUsQ0FBQyxDQUFDLENBQUM7YUFDbEY7WUFDRCxPQUFPLFNBQVMsQ0FBQztRQUNuQixDQUFDO1FBQ0gsbUJBQUM7SUFBRCxDQUFDLEFBcEpELElBb0pDO0lBRUQsSUFBTSxjQUFjLEdBQ2hCLDZFQUE2RSxDQUFDO0lBRWxGLFNBQVMsdUJBQXVCLENBQUMsS0FBYTtRQUM1QyxPQUFPLEtBQUssQ0FBQyxLQUFLLENBQUMsY0FBYyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDeEMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtMZXhlciBhcyBFeHByZXNzaW9uTGV4ZXJ9IGZyb20gJy4uL2V4cHJlc3Npb25fcGFyc2VyL2xleGVyJztcbmltcG9ydCB7UGFyc2VyIGFzIEV4cHJlc3Npb25QYXJzZXJ9IGZyb20gJy4uL2V4cHJlc3Npb25fcGFyc2VyL3BhcnNlcic7XG5pbXBvcnQgKiBhcyBodG1sIGZyb20gJy4uL21sX3BhcnNlci9hc3QnO1xuaW1wb3J0IHtnZXRIdG1sVGFnRGVmaW5pdGlvbn0gZnJvbSAnLi4vbWxfcGFyc2VyL2h0bWxfdGFncyc7XG5pbXBvcnQge0ludGVycG9sYXRpb25Db25maWd9IGZyb20gJy4uL21sX3BhcnNlci9pbnRlcnBvbGF0aW9uX2NvbmZpZyc7XG5pbXBvcnQge1BhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5cbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi9pMThuX2FzdCc7XG5pbXBvcnQge1BsYWNlaG9sZGVyUmVnaXN0cnl9IGZyb20gJy4vc2VyaWFsaXplcnMvcGxhY2Vob2xkZXInO1xuXG5jb25zdCBfZXhwUGFyc2VyID0gbmV3IEV4cHJlc3Npb25QYXJzZXIobmV3IEV4cHJlc3Npb25MZXhlcigpKTtcblxudHlwZSBWaXNpdE5vZGVGbiA9IChodG1sOiBodG1sLk5vZGUsIGkxOG46IGkxOG4uTm9kZSkgPT4gdm9pZDtcblxuLyoqXG4gKiBSZXR1cm5zIGEgZnVuY3Rpb24gY29udmVydGluZyBodG1sIG5vZGVzIHRvIGFuIGkxOG4gTWVzc2FnZSBnaXZlbiBhbiBpbnRlcnBvbGF0aW9uQ29uZmlnXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVJMThuTWVzc2FnZUZhY3RvcnkoaW50ZXJwb2xhdGlvbkNvbmZpZzogSW50ZXJwb2xhdGlvbkNvbmZpZyk6IChcbiAgICBub2RlczogaHRtbC5Ob2RlW10sIG1lYW5pbmc6IHN0cmluZywgZGVzY3JpcHRpb246IHN0cmluZywgaWQ6IHN0cmluZyxcbiAgICB2aXNpdE5vZGVGbj86IFZpc2l0Tm9kZUZuKSA9PiBpMThuLk1lc3NhZ2Uge1xuICBjb25zdCB2aXNpdG9yID0gbmV3IF9JMThuVmlzaXRvcihfZXhwUGFyc2VyLCBpbnRlcnBvbGF0aW9uQ29uZmlnKTtcblxuICByZXR1cm4gKG5vZGVzOiBodG1sLk5vZGVbXSwgbWVhbmluZzogc3RyaW5nLCBkZXNjcmlwdGlvbjogc3RyaW5nLCBpZDogc3RyaW5nLFxuICAgICAgICAgIHZpc2l0Tm9kZUZuPzogVmlzaXROb2RlRm4pID0+XG4gICAgICAgICAgICAgdmlzaXRvci50b0kxOG5NZXNzYWdlKG5vZGVzLCBtZWFuaW5nLCBkZXNjcmlwdGlvbiwgaWQsIHZpc2l0Tm9kZUZuKTtcbn1cblxuY2xhc3MgX0kxOG5WaXNpdG9yIGltcGxlbWVudHMgaHRtbC5WaXNpdG9yIHtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2lzSWN1ICE6IGJvb2xlYW47XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9pY3VEZXB0aCAhOiBudW1iZXI7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9wbGFjZWhvbGRlclJlZ2lzdHJ5ICE6IFBsYWNlaG9sZGVyUmVnaXN0cnk7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9wbGFjZWhvbGRlclRvQ29udGVudCAhOiB7W3BoTmFtZTogc3RyaW5nXTogc3RyaW5nfTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX3BsYWNlaG9sZGVyVG9NZXNzYWdlICE6IHtbcGhOYW1lOiBzdHJpbmddOiBpMThuLk1lc3NhZ2V9O1xuICBwcml2YXRlIF92aXNpdE5vZGVGbjogVmlzaXROb2RlRm58dW5kZWZpbmVkO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBfZXhwcmVzc2lvblBhcnNlcjogRXhwcmVzc2lvblBhcnNlcixcbiAgICAgIHByaXZhdGUgX2ludGVycG9sYXRpb25Db25maWc6IEludGVycG9sYXRpb25Db25maWcpIHt9XG5cbiAgcHVibGljIHRvSTE4bk1lc3NhZ2UoXG4gICAgICBub2RlczogaHRtbC5Ob2RlW10sIG1lYW5pbmc6IHN0cmluZywgZGVzY3JpcHRpb246IHN0cmluZywgaWQ6IHN0cmluZyxcbiAgICAgIHZpc2l0Tm9kZUZuPzogVmlzaXROb2RlRm4pOiBpMThuLk1lc3NhZ2Uge1xuICAgIHRoaXMuX2lzSWN1ID0gbm9kZXMubGVuZ3RoID09IDEgJiYgbm9kZXNbMF0gaW5zdGFuY2VvZiBodG1sLkV4cGFuc2lvbjtcbiAgICB0aGlzLl9pY3VEZXB0aCA9IDA7XG4gICAgdGhpcy5fcGxhY2Vob2xkZXJSZWdpc3RyeSA9IG5ldyBQbGFjZWhvbGRlclJlZ2lzdHJ5KCk7XG4gICAgdGhpcy5fcGxhY2Vob2xkZXJUb0NvbnRlbnQgPSB7fTtcbiAgICB0aGlzLl9wbGFjZWhvbGRlclRvTWVzc2FnZSA9IHt9O1xuICAgIHRoaXMuX3Zpc2l0Tm9kZUZuID0gdmlzaXROb2RlRm47XG5cbiAgICBjb25zdCBpMThub2RlczogaTE4bi5Ob2RlW10gPSBodG1sLnZpc2l0QWxsKHRoaXMsIG5vZGVzLCB7fSk7XG5cbiAgICByZXR1cm4gbmV3IGkxOG4uTWVzc2FnZShcbiAgICAgICAgaTE4bm9kZXMsIHRoaXMuX3BsYWNlaG9sZGVyVG9Db250ZW50LCB0aGlzLl9wbGFjZWhvbGRlclRvTWVzc2FnZSwgbWVhbmluZywgZGVzY3JpcHRpb24sIGlkKTtcbiAgfVxuXG4gIHByaXZhdGUgX3Zpc2l0Tm9kZShodG1sOiBodG1sLk5vZGUsIGkxOG46IGkxOG4uTm9kZSk6IGkxOG4uTm9kZSB7XG4gICAgaWYgKHRoaXMuX3Zpc2l0Tm9kZUZuKSB7XG4gICAgICB0aGlzLl92aXNpdE5vZGVGbihodG1sLCBpMThuKTtcbiAgICB9XG4gICAgcmV0dXJuIGkxOG47XG4gIH1cblxuICB2aXNpdEVsZW1lbnQoZWw6IGh0bWwuRWxlbWVudCwgY29udGV4dDogYW55KTogaTE4bi5Ob2RlIHtcbiAgICBjb25zdCBjaGlsZHJlbiA9IGh0bWwudmlzaXRBbGwodGhpcywgZWwuY2hpbGRyZW4pO1xuICAgIGNvbnN0IGF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcbiAgICBlbC5hdHRycy5mb3JFYWNoKGF0dHIgPT4ge1xuICAgICAgLy8gRG8gbm90IHZpc2l0IHRoZSBhdHRyaWJ1dGVzLCB0cmFuc2xhdGFibGUgb25lcyBhcmUgdG9wLWxldmVsIEFTVHNcbiAgICAgIGF0dHJzW2F0dHIubmFtZV0gPSBhdHRyLnZhbHVlO1xuICAgIH0pO1xuXG4gICAgY29uc3QgaXNWb2lkOiBib29sZWFuID0gZ2V0SHRtbFRhZ0RlZmluaXRpb24oZWwubmFtZSkuaXNWb2lkO1xuICAgIGNvbnN0IHN0YXJ0UGhOYW1lID1cbiAgICAgICAgdGhpcy5fcGxhY2Vob2xkZXJSZWdpc3RyeS5nZXRTdGFydFRhZ1BsYWNlaG9sZGVyTmFtZShlbC5uYW1lLCBhdHRycywgaXNWb2lkKTtcbiAgICB0aGlzLl9wbGFjZWhvbGRlclRvQ29udGVudFtzdGFydFBoTmFtZV0gPSBlbC5zb3VyY2VTcGFuICEudG9TdHJpbmcoKTtcblxuICAgIGxldCBjbG9zZVBoTmFtZSA9ICcnO1xuXG4gICAgaWYgKCFpc1ZvaWQpIHtcbiAgICAgIGNsb3NlUGhOYW1lID0gdGhpcy5fcGxhY2Vob2xkZXJSZWdpc3RyeS5nZXRDbG9zZVRhZ1BsYWNlaG9sZGVyTmFtZShlbC5uYW1lKTtcbiAgICAgIHRoaXMuX3BsYWNlaG9sZGVyVG9Db250ZW50W2Nsb3NlUGhOYW1lXSA9IGA8LyR7ZWwubmFtZX0+YDtcbiAgICB9XG5cbiAgICBjb25zdCBub2RlID0gbmV3IGkxOG4uVGFnUGxhY2Vob2xkZXIoXG4gICAgICAgIGVsLm5hbWUsIGF0dHJzLCBzdGFydFBoTmFtZSwgY2xvc2VQaE5hbWUsIGNoaWxkcmVuLCBpc1ZvaWQsIGVsLnNvdXJjZVNwYW4gISk7XG4gICAgcmV0dXJuIHRoaXMuX3Zpc2l0Tm9kZShlbCwgbm9kZSk7XG4gIH1cblxuICB2aXNpdEF0dHJpYnV0ZShhdHRyaWJ1dGU6IGh0bWwuQXR0cmlidXRlLCBjb250ZXh0OiBhbnkpOiBpMThuLk5vZGUge1xuICAgIGNvbnN0IG5vZGUgPSB0aGlzLl92aXNpdFRleHRXaXRoSW50ZXJwb2xhdGlvbihhdHRyaWJ1dGUudmFsdWUsIGF0dHJpYnV0ZS5zb3VyY2VTcGFuKTtcbiAgICByZXR1cm4gdGhpcy5fdmlzaXROb2RlKGF0dHJpYnV0ZSwgbm9kZSk7XG4gIH1cblxuICB2aXNpdFRleHQodGV4dDogaHRtbC5UZXh0LCBjb250ZXh0OiBhbnkpOiBpMThuLk5vZGUge1xuICAgIGNvbnN0IG5vZGUgPSB0aGlzLl92aXNpdFRleHRXaXRoSW50ZXJwb2xhdGlvbih0ZXh0LnZhbHVlLCB0ZXh0LnNvdXJjZVNwYW4gISk7XG4gICAgcmV0dXJuIHRoaXMuX3Zpc2l0Tm9kZSh0ZXh0LCBub2RlKTtcbiAgfVxuXG4gIHZpc2l0Q29tbWVudChjb21tZW50OiBodG1sLkNvbW1lbnQsIGNvbnRleHQ6IGFueSk6IGkxOG4uTm9kZXxudWxsIHsgcmV0dXJuIG51bGw7IH1cblxuICB2aXNpdEV4cGFuc2lvbihpY3U6IGh0bWwuRXhwYW5zaW9uLCBjb250ZXh0OiBhbnkpOiBpMThuLk5vZGUge1xuICAgIHRoaXMuX2ljdURlcHRoKys7XG4gICAgY29uc3QgaTE4bkljdUNhc2VzOiB7W2s6IHN0cmluZ106IGkxOG4uTm9kZX0gPSB7fTtcbiAgICBjb25zdCBpMThuSWN1ID0gbmV3IGkxOG4uSWN1KGljdS5zd2l0Y2hWYWx1ZSwgaWN1LnR5cGUsIGkxOG5JY3VDYXNlcywgaWN1LnNvdXJjZVNwYW4pO1xuICAgIGljdS5jYXNlcy5mb3JFYWNoKChjYXplKTogdm9pZCA9PiB7XG4gICAgICBpMThuSWN1Q2FzZXNbY2F6ZS52YWx1ZV0gPSBuZXcgaTE4bi5Db250YWluZXIoXG4gICAgICAgICAgY2F6ZS5leHByZXNzaW9uLm1hcCgobm9kZSkgPT4gbm9kZS52aXNpdCh0aGlzLCB7fSkpLCBjYXplLmV4cFNvdXJjZVNwYW4pO1xuICAgIH0pO1xuICAgIHRoaXMuX2ljdURlcHRoLS07XG5cbiAgICBpZiAodGhpcy5faXNJY3UgfHwgdGhpcy5faWN1RGVwdGggPiAwKSB7XG4gICAgICAvLyBSZXR1cm5zIGFuIElDVSBub2RlIHdoZW46XG4gICAgICAvLyAtIHRoZSBtZXNzYWdlICh2cyBhIHBhcnQgb2YgdGhlIG1lc3NhZ2UpIGlzIGFuIElDVSBtZXNzYWdlLCBvclxuICAgICAgLy8gLSB0aGUgSUNVIG1lc3NhZ2UgaXMgbmVzdGVkLlxuICAgICAgY29uc3QgZXhwUGggPSB0aGlzLl9wbGFjZWhvbGRlclJlZ2lzdHJ5LmdldFVuaXF1ZVBsYWNlaG9sZGVyKGBWQVJfJHtpY3UudHlwZX1gKTtcbiAgICAgIGkxOG5JY3UuZXhwcmVzc2lvblBsYWNlaG9sZGVyID0gZXhwUGg7XG4gICAgICB0aGlzLl9wbGFjZWhvbGRlclRvQ29udGVudFtleHBQaF0gPSBpY3Uuc3dpdGNoVmFsdWU7XG4gICAgICByZXR1cm4gdGhpcy5fdmlzaXROb2RlKGljdSwgaTE4bkljdSk7XG4gICAgfVxuXG4gICAgLy8gRWxzZSByZXR1cm5zIGEgcGxhY2Vob2xkZXJcbiAgICAvLyBJQ1UgcGxhY2Vob2xkZXJzIHNob3VsZCBub3QgYmUgcmVwbGFjZWQgd2l0aCB0aGVpciBvcmlnaW5hbCBjb250ZW50IGJ1dCB3aXRoIHRoZSB0aGVpclxuICAgIC8vIHRyYW5zbGF0aW9ucy4gV2UgbmVlZCB0byBjcmVhdGUgYSBuZXcgdmlzaXRvciAodGhleSBhcmUgbm90IHJlLWVudHJhbnQpIHRvIGNvbXB1dGUgdGhlXG4gICAgLy8gbWVzc2FnZSBpZC5cbiAgICAvLyBUT0RPKHZpY2IpOiBhZGQgYSBodG1sLk5vZGUgLT4gaTE4bi5NZXNzYWdlIGNhY2hlIHRvIGF2b2lkIGhhdmluZyB0byByZS1jcmVhdGUgdGhlIG1zZ1xuICAgIGNvbnN0IHBoTmFtZSA9IHRoaXMuX3BsYWNlaG9sZGVyUmVnaXN0cnkuZ2V0UGxhY2Vob2xkZXJOYW1lKCdJQ1UnLCBpY3Uuc291cmNlU3Bhbi50b1N0cmluZygpKTtcbiAgICBjb25zdCB2aXNpdG9yID0gbmV3IF9JMThuVmlzaXRvcih0aGlzLl9leHByZXNzaW9uUGFyc2VyLCB0aGlzLl9pbnRlcnBvbGF0aW9uQ29uZmlnKTtcbiAgICB0aGlzLl9wbGFjZWhvbGRlclRvTWVzc2FnZVtwaE5hbWVdID0gdmlzaXRvci50b0kxOG5NZXNzYWdlKFtpY3VdLCAnJywgJycsICcnKTtcbiAgICBjb25zdCBub2RlID0gbmV3IGkxOG4uSWN1UGxhY2Vob2xkZXIoaTE4bkljdSwgcGhOYW1lLCBpY3Uuc291cmNlU3Bhbik7XG4gICAgcmV0dXJuIHRoaXMuX3Zpc2l0Tm9kZShpY3UsIG5vZGUpO1xuICB9XG5cbiAgdmlzaXRFeHBhbnNpb25DYXNlKGljdUNhc2U6IGh0bWwuRXhwYW5zaW9uQ2FzZSwgY29udGV4dDogYW55KTogaTE4bi5Ob2RlIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ1VucmVhY2hhYmxlIGNvZGUnKTtcbiAgfVxuXG4gIHByaXZhdGUgX3Zpc2l0VGV4dFdpdGhJbnRlcnBvbGF0aW9uKHRleHQ6IHN0cmluZywgc291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuKTogaTE4bi5Ob2RlIHtcbiAgICBjb25zdCBzcGxpdEludGVycG9sYXRpb24gPSB0aGlzLl9leHByZXNzaW9uUGFyc2VyLnNwbGl0SW50ZXJwb2xhdGlvbihcbiAgICAgICAgdGV4dCwgc291cmNlU3Bhbi5zdGFydC50b1N0cmluZygpLCB0aGlzLl9pbnRlcnBvbGF0aW9uQ29uZmlnKTtcblxuICAgIGlmICghc3BsaXRJbnRlcnBvbGF0aW9uKSB7XG4gICAgICAvLyBObyBleHByZXNzaW9uLCByZXR1cm4gYSBzaW5nbGUgdGV4dFxuICAgICAgcmV0dXJuIG5ldyBpMThuLlRleHQodGV4dCwgc291cmNlU3Bhbik7XG4gICAgfVxuXG4gICAgLy8gUmV0dXJuIGEgZ3JvdXAgb2YgdGV4dCArIGV4cHJlc3Npb25zXG4gICAgY29uc3Qgbm9kZXM6IGkxOG4uTm9kZVtdID0gW107XG4gICAgY29uc3QgY29udGFpbmVyID0gbmV3IGkxOG4uQ29udGFpbmVyKG5vZGVzLCBzb3VyY2VTcGFuKTtcbiAgICBjb25zdCB7c3RhcnQ6IHNEZWxpbWl0ZXIsIGVuZDogZURlbGltaXRlcn0gPSB0aGlzLl9pbnRlcnBvbGF0aW9uQ29uZmlnO1xuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBzcGxpdEludGVycG9sYXRpb24uc3RyaW5ncy5sZW5ndGggLSAxOyBpKyspIHtcbiAgICAgIGNvbnN0IGV4cHJlc3Npb24gPSBzcGxpdEludGVycG9sYXRpb24uZXhwcmVzc2lvbnNbaV07XG4gICAgICBjb25zdCBiYXNlTmFtZSA9IF9leHRyYWN0UGxhY2Vob2xkZXJOYW1lKGV4cHJlc3Npb24pIHx8ICdJTlRFUlBPTEFUSU9OJztcbiAgICAgIGNvbnN0IHBoTmFtZSA9IHRoaXMuX3BsYWNlaG9sZGVyUmVnaXN0cnkuZ2V0UGxhY2Vob2xkZXJOYW1lKGJhc2VOYW1lLCBleHByZXNzaW9uKTtcblxuICAgICAgaWYgKHNwbGl0SW50ZXJwb2xhdGlvbi5zdHJpbmdzW2ldLmxlbmd0aCkge1xuICAgICAgICAvLyBObyBuZWVkIHRvIGFkZCBlbXB0eSBzdHJpbmdzXG4gICAgICAgIG5vZGVzLnB1c2gobmV3IGkxOG4uVGV4dChzcGxpdEludGVycG9sYXRpb24uc3RyaW5nc1tpXSwgc291cmNlU3BhbikpO1xuICAgICAgfVxuXG4gICAgICBub2Rlcy5wdXNoKG5ldyBpMThuLlBsYWNlaG9sZGVyKGV4cHJlc3Npb24sIHBoTmFtZSwgc291cmNlU3BhbikpO1xuICAgICAgdGhpcy5fcGxhY2Vob2xkZXJUb0NvbnRlbnRbcGhOYW1lXSA9IHNEZWxpbWl0ZXIgKyBleHByZXNzaW9uICsgZURlbGltaXRlcjtcbiAgICB9XG5cbiAgICAvLyBUaGUgbGFzdCBpbmRleCBjb250YWlucyBubyBleHByZXNzaW9uXG4gICAgY29uc3QgbGFzdFN0cmluZ0lkeCA9IHNwbGl0SW50ZXJwb2xhdGlvbi5zdHJpbmdzLmxlbmd0aCAtIDE7XG4gICAgaWYgKHNwbGl0SW50ZXJwb2xhdGlvbi5zdHJpbmdzW2xhc3RTdHJpbmdJZHhdLmxlbmd0aCkge1xuICAgICAgbm9kZXMucHVzaChuZXcgaTE4bi5UZXh0KHNwbGl0SW50ZXJwb2xhdGlvbi5zdHJpbmdzW2xhc3RTdHJpbmdJZHhdLCBzb3VyY2VTcGFuKSk7XG4gICAgfVxuICAgIHJldHVybiBjb250YWluZXI7XG4gIH1cbn1cblxuY29uc3QgX0NVU1RPTV9QSF9FWFAgPVxuICAgIC9cXC9cXC9bXFxzXFxTXSppMThuW1xcc1xcU10qXFwoW1xcc1xcU10qcGhbXFxzXFxTXSo9W1xcc1xcU10qKFwifCcpKFtcXHNcXFNdKj8pXFwxW1xcc1xcU10qXFwpL2c7XG5cbmZ1bmN0aW9uIF9leHRyYWN0UGxhY2Vob2xkZXJOYW1lKGlucHV0OiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gaW5wdXQuc3BsaXQoX0NVU1RPTV9QSF9FWFApWzJdO1xufVxuIl19