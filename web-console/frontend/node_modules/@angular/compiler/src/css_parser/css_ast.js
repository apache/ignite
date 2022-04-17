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
        define("@angular/compiler/src/css_parser/css_ast", ["require", "exports", "tslib", "@angular/compiler/src/css_parser/css_lexer"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var css_lexer_1 = require("@angular/compiler/src/css_parser/css_lexer");
    var BlockType;
    (function (BlockType) {
        BlockType[BlockType["Import"] = 0] = "Import";
        BlockType[BlockType["Charset"] = 1] = "Charset";
        BlockType[BlockType["Namespace"] = 2] = "Namespace";
        BlockType[BlockType["Supports"] = 3] = "Supports";
        BlockType[BlockType["Keyframes"] = 4] = "Keyframes";
        BlockType[BlockType["MediaQuery"] = 5] = "MediaQuery";
        BlockType[BlockType["Selector"] = 6] = "Selector";
        BlockType[BlockType["FontFace"] = 7] = "FontFace";
        BlockType[BlockType["Page"] = 8] = "Page";
        BlockType[BlockType["Document"] = 9] = "Document";
        BlockType[BlockType["Viewport"] = 10] = "Viewport";
        BlockType[BlockType["Unsupported"] = 11] = "Unsupported";
    })(BlockType = exports.BlockType || (exports.BlockType = {}));
    var CssAst = /** @class */ (function () {
        function CssAst(location) {
            this.location = location;
        }
        Object.defineProperty(CssAst.prototype, "start", {
            get: function () { return this.location.start; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(CssAst.prototype, "end", {
            get: function () { return this.location.end; },
            enumerable: true,
            configurable: true
        });
        return CssAst;
    }());
    exports.CssAst = CssAst;
    var CssStyleValueAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssStyleValueAst, _super);
        function CssStyleValueAst(location, tokens, strValue) {
            var _this = _super.call(this, location) || this;
            _this.tokens = tokens;
            _this.strValue = strValue;
            return _this;
        }
        CssStyleValueAst.prototype.visit = function (visitor, context) { return visitor.visitCssValue(this); };
        return CssStyleValueAst;
    }(CssAst));
    exports.CssStyleValueAst = CssStyleValueAst;
    var CssRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssRuleAst, _super);
        function CssRuleAst(location) {
            return _super.call(this, location) || this;
        }
        return CssRuleAst;
    }(CssAst));
    exports.CssRuleAst = CssRuleAst;
    var CssBlockRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssBlockRuleAst, _super);
        function CssBlockRuleAst(location, type, block, name) {
            if (name === void 0) { name = null; }
            var _this = _super.call(this, location) || this;
            _this.location = location;
            _this.type = type;
            _this.block = block;
            _this.name = name;
            return _this;
        }
        CssBlockRuleAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssBlock(this.block, context);
        };
        return CssBlockRuleAst;
    }(CssRuleAst));
    exports.CssBlockRuleAst = CssBlockRuleAst;
    var CssKeyframeRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssKeyframeRuleAst, _super);
        function CssKeyframeRuleAst(location, name, block) {
            return _super.call(this, location, BlockType.Keyframes, block, name) || this;
        }
        CssKeyframeRuleAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssKeyframeRule(this, context);
        };
        return CssKeyframeRuleAst;
    }(CssBlockRuleAst));
    exports.CssKeyframeRuleAst = CssKeyframeRuleAst;
    var CssKeyframeDefinitionAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssKeyframeDefinitionAst, _super);
        function CssKeyframeDefinitionAst(location, steps, block) {
            var _this = _super.call(this, location, BlockType.Keyframes, block, mergeTokens(steps, ',')) || this;
            _this.steps = steps;
            return _this;
        }
        CssKeyframeDefinitionAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssKeyframeDefinition(this, context);
        };
        return CssKeyframeDefinitionAst;
    }(CssBlockRuleAst));
    exports.CssKeyframeDefinitionAst = CssKeyframeDefinitionAst;
    var CssBlockDefinitionRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssBlockDefinitionRuleAst, _super);
        function CssBlockDefinitionRuleAst(location, strValue, type, query, block) {
            var _this = _super.call(this, location, type, block) || this;
            _this.strValue = strValue;
            _this.query = query;
            var firstCssToken = query.tokens[0];
            _this.name = new css_lexer_1.CssToken(firstCssToken.index, firstCssToken.column, firstCssToken.line, css_lexer_1.CssTokenType.Identifier, _this.strValue);
            return _this;
        }
        CssBlockDefinitionRuleAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssBlock(this.block, context);
        };
        return CssBlockDefinitionRuleAst;
    }(CssBlockRuleAst));
    exports.CssBlockDefinitionRuleAst = CssBlockDefinitionRuleAst;
    var CssMediaQueryRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssMediaQueryRuleAst, _super);
        function CssMediaQueryRuleAst(location, strValue, query, block) {
            return _super.call(this, location, strValue, BlockType.MediaQuery, query, block) || this;
        }
        CssMediaQueryRuleAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssMediaQueryRule(this, context);
        };
        return CssMediaQueryRuleAst;
    }(CssBlockDefinitionRuleAst));
    exports.CssMediaQueryRuleAst = CssMediaQueryRuleAst;
    var CssAtRulePredicateAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssAtRulePredicateAst, _super);
        function CssAtRulePredicateAst(location, strValue, tokens) {
            var _this = _super.call(this, location) || this;
            _this.strValue = strValue;
            _this.tokens = tokens;
            return _this;
        }
        CssAtRulePredicateAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssAtRulePredicate(this, context);
        };
        return CssAtRulePredicateAst;
    }(CssAst));
    exports.CssAtRulePredicateAst = CssAtRulePredicateAst;
    var CssInlineRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssInlineRuleAst, _super);
        function CssInlineRuleAst(location, type, value) {
            var _this = _super.call(this, location) || this;
            _this.type = type;
            _this.value = value;
            return _this;
        }
        CssInlineRuleAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssInlineRule(this, context);
        };
        return CssInlineRuleAst;
    }(CssRuleAst));
    exports.CssInlineRuleAst = CssInlineRuleAst;
    var CssSelectorRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssSelectorRuleAst, _super);
        function CssSelectorRuleAst(location, selectors, block) {
            var _this = _super.call(this, location, BlockType.Selector, block) || this;
            _this.selectors = selectors;
            _this.strValue = selectors.map(function (selector) { return selector.strValue; }).join(',');
            return _this;
        }
        CssSelectorRuleAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssSelectorRule(this, context);
        };
        return CssSelectorRuleAst;
    }(CssBlockRuleAst));
    exports.CssSelectorRuleAst = CssSelectorRuleAst;
    var CssDefinitionAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssDefinitionAst, _super);
        function CssDefinitionAst(location, property, value) {
            var _this = _super.call(this, location) || this;
            _this.property = property;
            _this.value = value;
            return _this;
        }
        CssDefinitionAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssDefinition(this, context);
        };
        return CssDefinitionAst;
    }(CssAst));
    exports.CssDefinitionAst = CssDefinitionAst;
    var CssSelectorPartAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssSelectorPartAst, _super);
        function CssSelectorPartAst(location) {
            return _super.call(this, location) || this;
        }
        return CssSelectorPartAst;
    }(CssAst));
    exports.CssSelectorPartAst = CssSelectorPartAst;
    var CssSelectorAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssSelectorAst, _super);
        function CssSelectorAst(location, selectorParts) {
            var _this = _super.call(this, location) || this;
            _this.selectorParts = selectorParts;
            _this.strValue = selectorParts.map(function (part) { return part.strValue; }).join('');
            return _this;
        }
        CssSelectorAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssSelector(this, context);
        };
        return CssSelectorAst;
    }(CssSelectorPartAst));
    exports.CssSelectorAst = CssSelectorAst;
    var CssSimpleSelectorAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssSimpleSelectorAst, _super);
        function CssSimpleSelectorAst(location, tokens, strValue, pseudoSelectors, operator) {
            var _this = _super.call(this, location) || this;
            _this.tokens = tokens;
            _this.strValue = strValue;
            _this.pseudoSelectors = pseudoSelectors;
            _this.operator = operator;
            return _this;
        }
        CssSimpleSelectorAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssSimpleSelector(this, context);
        };
        return CssSimpleSelectorAst;
    }(CssSelectorPartAst));
    exports.CssSimpleSelectorAst = CssSimpleSelectorAst;
    var CssPseudoSelectorAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssPseudoSelectorAst, _super);
        function CssPseudoSelectorAst(location, strValue, name, tokens, innerSelectors) {
            var _this = _super.call(this, location) || this;
            _this.strValue = strValue;
            _this.name = name;
            _this.tokens = tokens;
            _this.innerSelectors = innerSelectors;
            return _this;
        }
        CssPseudoSelectorAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssPseudoSelector(this, context);
        };
        return CssPseudoSelectorAst;
    }(CssSelectorPartAst));
    exports.CssPseudoSelectorAst = CssPseudoSelectorAst;
    var CssBlockAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssBlockAst, _super);
        function CssBlockAst(location, entries) {
            var _this = _super.call(this, location) || this;
            _this.entries = entries;
            return _this;
        }
        CssBlockAst.prototype.visit = function (visitor, context) { return visitor.visitCssBlock(this, context); };
        return CssBlockAst;
    }(CssAst));
    exports.CssBlockAst = CssBlockAst;
    /*
     a style block is different from a standard block because it contains
     css prop:value definitions. A regular block can contain a list of Ast entries.
     */
    var CssStylesBlockAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssStylesBlockAst, _super);
        function CssStylesBlockAst(location, definitions) {
            var _this = _super.call(this, location, definitions) || this;
            _this.definitions = definitions;
            return _this;
        }
        CssStylesBlockAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssStylesBlock(this, context);
        };
        return CssStylesBlockAst;
    }(CssBlockAst));
    exports.CssStylesBlockAst = CssStylesBlockAst;
    var CssStyleSheetAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssStyleSheetAst, _super);
        function CssStyleSheetAst(location, rules) {
            var _this = _super.call(this, location) || this;
            _this.rules = rules;
            return _this;
        }
        CssStyleSheetAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssStyleSheet(this, context);
        };
        return CssStyleSheetAst;
    }(CssAst));
    exports.CssStyleSheetAst = CssStyleSheetAst;
    var CssUnknownRuleAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssUnknownRuleAst, _super);
        function CssUnknownRuleAst(location, ruleName, tokens) {
            var _this = _super.call(this, location) || this;
            _this.ruleName = ruleName;
            _this.tokens = tokens;
            return _this;
        }
        CssUnknownRuleAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssUnknownRule(this, context);
        };
        return CssUnknownRuleAst;
    }(CssRuleAst));
    exports.CssUnknownRuleAst = CssUnknownRuleAst;
    var CssUnknownTokenListAst = /** @class */ (function (_super) {
        tslib_1.__extends(CssUnknownTokenListAst, _super);
        function CssUnknownTokenListAst(location, name, tokens) {
            var _this = _super.call(this, location) || this;
            _this.name = name;
            _this.tokens = tokens;
            return _this;
        }
        CssUnknownTokenListAst.prototype.visit = function (visitor, context) {
            return visitor.visitCssUnknownTokenList(this, context);
        };
        return CssUnknownTokenListAst;
    }(CssRuleAst));
    exports.CssUnknownTokenListAst = CssUnknownTokenListAst;
    function mergeTokens(tokens, separator) {
        if (separator === void 0) { separator = ''; }
        var mainToken = tokens[0];
        var str = mainToken.strValue;
        for (var i = 1; i < tokens.length; i++) {
            str += separator + tokens[i].strValue;
        }
        return new css_lexer_1.CssToken(mainToken.index, mainToken.column, mainToken.line, mainToken.type, str);
    }
    exports.mergeTokens = mergeTokens;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NzX2FzdC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9jc3NfcGFyc2VyL2Nzc19hc3QudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBSUgsd0VBQW1EO0lBRW5ELElBQVksU0FhWDtJQWJELFdBQVksU0FBUztRQUNuQiw2Q0FBTSxDQUFBO1FBQ04sK0NBQU8sQ0FBQTtRQUNQLG1EQUFTLENBQUE7UUFDVCxpREFBUSxDQUFBO1FBQ1IsbURBQVMsQ0FBQTtRQUNULHFEQUFVLENBQUE7UUFDVixpREFBUSxDQUFBO1FBQ1IsaURBQVEsQ0FBQTtRQUNSLHlDQUFJLENBQUE7UUFDSixpREFBUSxDQUFBO1FBQ1Isa0RBQVEsQ0FBQTtRQUNSLHdEQUFXLENBQUE7SUFDYixDQUFDLEVBYlcsU0FBUyxHQUFULGlCQUFTLEtBQVQsaUJBQVMsUUFhcEI7SUFxQkQ7UUFDRSxnQkFBbUIsUUFBeUI7WUFBekIsYUFBUSxHQUFSLFFBQVEsQ0FBaUI7UUFBRyxDQUFDO1FBQ2hELHNCQUFJLHlCQUFLO2lCQUFULGNBQTZCLE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDOzs7V0FBQTtRQUMxRCxzQkFBSSx1QkFBRztpQkFBUCxjQUEyQixPQUFPLElBQUksQ0FBQyxRQUFRLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQzs7O1dBQUE7UUFFeEQsYUFBQztJQUFELENBQUMsQUFMRCxJQUtDO0lBTHFCLHdCQUFNO0lBTzVCO1FBQXNDLDRDQUFNO1FBQzFDLDBCQUFZLFFBQXlCLEVBQVMsTUFBa0IsRUFBUyxRQUFnQjtZQUF6RixZQUNFLGtCQUFNLFFBQVEsQ0FBQyxTQUNoQjtZQUY2QyxZQUFNLEdBQU4sTUFBTSxDQUFZO1lBQVMsY0FBUSxHQUFSLFFBQVEsQ0FBUTs7UUFFekYsQ0FBQztRQUNELGdDQUFLLEdBQUws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Qyx3QkFBd0IsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDLENBQUM7UUFDekQsQ0FBQztRQUNILDZCQUFDO0lBQUQsQ0FBQyxBQVBELENBQTRDLFVBQVUsR0FPckQ7SUFQWSx3REFBc0I7SUFTbkMsU0FBZ0IsV0FBVyxDQUFDLE1BQWtCLEVBQUUsU0FBc0I7UUFBdEIsMEJBQUEsRUFBQSxjQUFzQjtRQUNwRSxJQUFNLFNBQVMsR0FBRyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDNUIsSUFBSSxHQUFHLEdBQUcsU0FBUyxDQUFDLFFBQVEsQ0FBQztRQUM3QixLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsTUFBTSxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTtZQUN0QyxHQUFHLElBQUksU0FBUyxHQUFHLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUM7U0FDdkM7UUFFRCxPQUFPLElBQUksb0JBQVEsQ0FBQyxTQUFTLENBQUMsS0FBSyxFQUFFLFNBQVMsQ0FBQyxNQUFNLEVBQUUsU0FBUyxDQUFDLElBQUksRUFBRSxTQUFTLENBQUMsSUFBSSxFQUFFLEdBQUcsQ0FBQyxDQUFDO0lBQzlGLENBQUM7SUFSRCxrQ0FRQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtQYXJzZUxvY2F0aW9uLCBQYXJzZVNvdXJjZVNwYW59IGZyb20gJy4uL3BhcnNlX3V0aWwnO1xuXG5pbXBvcnQge0Nzc1Rva2VuLCBDc3NUb2tlblR5cGV9IGZyb20gJy4vY3NzX2xleGVyJztcblxuZXhwb3J0IGVudW0gQmxvY2tUeXBlIHtcbiAgSW1wb3J0LFxuICBDaGFyc2V0LFxuICBOYW1lc3BhY2UsXG4gIFN1cHBvcnRzLFxuICBLZXlmcmFtZXMsXG4gIE1lZGlhUXVlcnksXG4gIFNlbGVjdG9yLFxuICBGb250RmFjZSxcbiAgUGFnZSxcbiAgRG9jdW1lbnQsXG4gIFZpZXdwb3J0LFxuICBVbnN1cHBvcnRlZFxufVxuXG5leHBvcnQgaW50ZXJmYWNlIENzc0FzdFZpc2l0b3Ige1xuICB2aXNpdENzc1ZhbHVlKGFzdDogQ3NzU3R5bGVWYWx1ZUFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NJbmxpbmVSdWxlKGFzdDogQ3NzSW5saW5lUnVsZUFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NBdFJ1bGVQcmVkaWNhdGUoYXN0OiBDc3NBdFJ1bGVQcmVkaWNhdGVBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzS2V5ZnJhbWVSdWxlKGFzdDogQ3NzS2V5ZnJhbWVSdWxlQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc0tleWZyYW1lRGVmaW5pdGlvbihhc3Q6IENzc0tleWZyYW1lRGVmaW5pdGlvbkFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NNZWRpYVF1ZXJ5UnVsZShhc3Q6IENzc01lZGlhUXVlcnlSdWxlQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc1NlbGVjdG9yUnVsZShhc3Q6IENzc1NlbGVjdG9yUnVsZUFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NTZWxlY3Rvcihhc3Q6IENzc1NlbGVjdG9yQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc1NpbXBsZVNlbGVjdG9yKGFzdDogQ3NzU2ltcGxlU2VsZWN0b3JBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzUHNldWRvU2VsZWN0b3IoYXN0OiBDc3NQc2V1ZG9TZWxlY3RvckFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NEZWZpbml0aW9uKGFzdDogQ3NzRGVmaW5pdGlvbkFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NCbG9jayhhc3Q6IENzc0Jsb2NrQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc1N0eWxlc0Jsb2NrKGFzdDogQ3NzU3R5bGVzQmxvY2tBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzU3R5bGVTaGVldChhc3Q6IENzc1N0eWxlU2hlZXRBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzVW5rbm93blJ1bGUoYXN0OiBDc3NVbmtub3duUnVsZUFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NVbmtub3duVG9rZW5MaXN0KGFzdDogQ3NzVW5rbm93blRva2VuTGlzdEFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbn1cblxuZXhwb3J0IGFic3RyYWN0IGNsYXNzIENzc0FzdCB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuKSB7fVxuICBnZXQgc3RhcnQoKTogUGFyc2VMb2NhdGlvbiB7IHJldHVybiB0aGlzLmxvY2F0aW9uLnN0YXJ0OyB9XG4gIGdldCBlbmQoKTogUGFyc2VMb2NhdGlvbiB7IHJldHVybiB0aGlzLmxvY2F0aW9uLmVuZDsgfVxuICBhYnN0cmFjdCB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55O1xufVxuXG5leHBvcnQgY2xhc3MgQ3NzU3R5bGVWYWx1ZUFzdCBleHRlbmRzIENzc0FzdCB7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyB0b2tlbnM6IENzc1Rva2VuW10sIHB1YmxpYyBzdHJWYWx1ZTogc3RyaW5nKSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdENzc1ZhbHVlKHRoaXMpOyB9XG59XG5cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBDc3NSdWxlQXN0IGV4dGVuZHMgQ3NzQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbikgeyBzdXBlcihsb2NhdGlvbik7IH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc0Jsb2NrUnVsZUFzdCBleHRlbmRzIENzc1J1bGVBc3Qge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgdHlwZTogQmxvY2tUeXBlLCBwdWJsaWMgYmxvY2s6IENzc0Jsb2NrQXN0LFxuICAgICAgcHVibGljIG5hbWU6IENzc1Rva2VufG51bGwgPSBudWxsKSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzQmxvY2sodGhpcy5ibG9jaywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc0tleWZyYW1lUnVsZUFzdCBleHRlbmRzIENzc0Jsb2NrUnVsZUFzdCB7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIG5hbWU6IENzc1Rva2VuLCBibG9jazogQ3NzQmxvY2tBc3QpIHtcbiAgICBzdXBlcihsb2NhdGlvbiwgQmxvY2tUeXBlLktleWZyYW1lcywgYmxvY2ssIG5hbWUpO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzS2V5ZnJhbWVSdWxlKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NLZXlmcmFtZURlZmluaXRpb25Bc3QgZXh0ZW5kcyBDc3NCbG9ja1J1bGVBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgc3RlcHM6IENzc1Rva2VuW10sIGJsb2NrOiBDc3NCbG9ja0FzdCkge1xuICAgIHN1cGVyKGxvY2F0aW9uLCBCbG9ja1R5cGUuS2V5ZnJhbWVzLCBibG9jaywgbWVyZ2VUb2tlbnMoc3RlcHMsICcsJykpO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzS2V5ZnJhbWVEZWZpbml0aW9uKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NCbG9ja0RlZmluaXRpb25SdWxlQXN0IGV4dGVuZHMgQ3NzQmxvY2tSdWxlQXN0IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgc3RyVmFsdWU6IHN0cmluZywgdHlwZTogQmxvY2tUeXBlLFxuICAgICAgcHVibGljIHF1ZXJ5OiBDc3NBdFJ1bGVQcmVkaWNhdGVBc3QsIGJsb2NrOiBDc3NCbG9ja0FzdCkge1xuICAgIHN1cGVyKGxvY2F0aW9uLCB0eXBlLCBibG9jayk7XG4gICAgY29uc3QgZmlyc3RDc3NUb2tlbjogQ3NzVG9rZW4gPSBxdWVyeS50b2tlbnNbMF07XG4gICAgdGhpcy5uYW1lID0gbmV3IENzc1Rva2VuKFxuICAgICAgICBmaXJzdENzc1Rva2VuLmluZGV4LCBmaXJzdENzc1Rva2VuLmNvbHVtbiwgZmlyc3RDc3NUb2tlbi5saW5lLCBDc3NUb2tlblR5cGUuSWRlbnRpZmllcixcbiAgICAgICAgdGhpcy5zdHJWYWx1ZSk7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NCbG9jayh0aGlzLmJsb2NrLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzTWVkaWFRdWVyeVJ1bGVBc3QgZXh0ZW5kcyBDc3NCbG9ja0RlZmluaXRpb25SdWxlQXN0IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBzdHJWYWx1ZTogc3RyaW5nLCBxdWVyeTogQ3NzQXRSdWxlUHJlZGljYXRlQXN0LFxuICAgICAgYmxvY2s6IENzc0Jsb2NrQXN0KSB7XG4gICAgc3VwZXIobG9jYXRpb24sIHN0clZhbHVlLCBCbG9ja1R5cGUuTWVkaWFRdWVyeSwgcXVlcnksIGJsb2NrKTtcbiAgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc01lZGlhUXVlcnlSdWxlKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NBdFJ1bGVQcmVkaWNhdGVBc3QgZXh0ZW5kcyBDc3NBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgc3RyVmFsdWU6IHN0cmluZywgcHVibGljIHRva2VuczogQ3NzVG9rZW5bXSkge1xuICAgIHN1cGVyKGxvY2F0aW9uKTtcbiAgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc0F0UnVsZVByZWRpY2F0ZSh0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzSW5saW5lUnVsZUFzdCBleHRlbmRzIENzc1J1bGVBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgdHlwZTogQmxvY2tUeXBlLCBwdWJsaWMgdmFsdWU6IENzc1N0eWxlVmFsdWVBc3QpIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NJbmxpbmVSdWxlKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NTZWxlY3RvclJ1bGVBc3QgZXh0ZW5kcyBDc3NCbG9ja1J1bGVBc3Qge1xuICBwdWJsaWMgc3RyVmFsdWU6IHN0cmluZztcblxuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgc2VsZWN0b3JzOiBDc3NTZWxlY3RvckFzdFtdLCBibG9jazogQ3NzQmxvY2tBc3QpIHtcbiAgICBzdXBlcihsb2NhdGlvbiwgQmxvY2tUeXBlLlNlbGVjdG9yLCBibG9jayk7XG4gICAgdGhpcy5zdHJWYWx1ZSA9IHNlbGVjdG9ycy5tYXAoc2VsZWN0b3IgPT4gc2VsZWN0b3Iuc3RyVmFsdWUpLmpvaW4oJywnKTtcbiAgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc1NlbGVjdG9yUnVsZSh0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzRGVmaW5pdGlvbkFzdCBleHRlbmRzIENzc0FzdCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgbG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIHByb3BlcnR5OiBDc3NUb2tlbiwgcHVibGljIHZhbHVlOiBDc3NTdHlsZVZhbHVlQXN0KSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzRGVmaW5pdGlvbih0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgQ3NzU2VsZWN0b3JQYXJ0QXN0IGV4dGVuZHMgQ3NzQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbikgeyBzdXBlcihsb2NhdGlvbik7IH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1NlbGVjdG9yQXN0IGV4dGVuZHMgQ3NzU2VsZWN0b3JQYXJ0QXN0IHtcbiAgcHVibGljIHN0clZhbHVlOiBzdHJpbmc7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBzZWxlY3RvclBhcnRzOiBDc3NTaW1wbGVTZWxlY3RvckFzdFtdKSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICAgIHRoaXMuc3RyVmFsdWUgPSBzZWxlY3RvclBhcnRzLm1hcChwYXJ0ID0+IHBhcnQuc3RyVmFsdWUpLmpvaW4oJycpO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzU2VsZWN0b3IodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1NpbXBsZVNlbGVjdG9yQXN0IGV4dGVuZHMgQ3NzU2VsZWN0b3JQYXJ0QXN0IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgdG9rZW5zOiBDc3NUb2tlbltdLCBwdWJsaWMgc3RyVmFsdWU6IHN0cmluZyxcbiAgICAgIHB1YmxpYyBwc2V1ZG9TZWxlY3RvcnM6IENzc1BzZXVkb1NlbGVjdG9yQXN0W10sIHB1YmxpYyBvcGVyYXRvcjogQ3NzVG9rZW4pIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NTaW1wbGVTZWxlY3Rvcih0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzUHNldWRvU2VsZWN0b3JBc3QgZXh0ZW5kcyBDc3NTZWxlY3RvclBhcnRBc3Qge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBzdHJWYWx1ZTogc3RyaW5nLCBwdWJsaWMgbmFtZTogc3RyaW5nLFxuICAgICAgcHVibGljIHRva2VuczogQ3NzVG9rZW5bXSwgcHVibGljIGlubmVyU2VsZWN0b3JzOiBDc3NTZWxlY3RvckFzdFtdKSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzUHNldWRvU2VsZWN0b3IodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc0Jsb2NrQXN0IGV4dGVuZHMgQ3NzQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIGVudHJpZXM6IENzc0FzdFtdKSB7IHN1cGVyKGxvY2F0aW9uKTsgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NCbG9jayh0aGlzLCBjb250ZXh0KTsgfVxufVxuXG4vKlxuIGEgc3R5bGUgYmxvY2sgaXMgZGlmZmVyZW50IGZyb20gYSBzdGFuZGFyZCBibG9jayBiZWNhdXNlIGl0IGNvbnRhaW5zXG4gY3NzIHByb3A6dmFsdWUgZGVmaW5pdGlvbnMuIEEgcmVndWxhciBibG9jayBjYW4gY29udGFpbiBhIGxpc3Qgb2YgQXN0IGVudHJpZXMuXG4gKi9cbmV4cG9ydCBjbGFzcyBDc3NTdHlsZXNCbG9ja0FzdCBleHRlbmRzIENzc0Jsb2NrQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIGRlZmluaXRpb25zOiBDc3NEZWZpbml0aW9uQXN0W10pIHtcbiAgICBzdXBlcihsb2NhdGlvbiwgZGVmaW5pdGlvbnMpO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzU3R5bGVzQmxvY2sodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1N0eWxlU2hlZXRBc3QgZXh0ZW5kcyBDc3NBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgcnVsZXM6IENzc0FzdFtdKSB7IHN1cGVyKGxvY2F0aW9uKTsgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc1N0eWxlU2hlZXQodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1Vua25vd25SdWxlQXN0IGV4dGVuZHMgQ3NzUnVsZUFzdCB7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBydWxlTmFtZTogc3RyaW5nLCBwdWJsaWMgdG9rZW5zOiBDc3NUb2tlbltdKSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzVW5rbm93blJ1bGUodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1Vua25vd25Ub2tlbkxpc3RBc3QgZXh0ZW5kcyBDc3NSdWxlQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIG5hbWU6IHN0cmluZywgcHVibGljIHRva2VuczogQ3NzVG9rZW5bXSkge1xuICAgIHN1cGVyKGxvY2F0aW9uKTtcbiAgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc1Vua25vd25Ub2tlbkxpc3QodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIG1lcmdlVG9rZW5zKHRva2VuczogQ3NzVG9rZW5bXSwgc2VwYXJhdG9yOiBzdHJpbmcgPSAnJyk6IENzc1Rva2VuIHtcbiAgY29uc3QgbWFpblRva2VuID0gdG9rZW5zWzBdO1xuICBsZXQgc3RyID0gbWFpblRva2VuLnN0clZhbHVlO1xuICBmb3IgKGxldCBpID0gMTsgaSA8IHRva2Vucy5sZW5ndGg7IGkrKykge1xuICAgIHN0ciArPSBzZXBhcmF0b3IgKyB0b2tlbnNbaV0uc3RyVmFsdWU7XG4gIH1cblxuICByZXR1cm4gbmV3IENzc1Rva2VuKG1haW5Ub2tlbi5pbmRleCwgbWFpblRva2VuLmNvbHVtbiwgbWFpblRva2VuLmxpbmUsIG1haW5Ub2tlbi50eXBlLCBzdHIpO1xufVxuIl19