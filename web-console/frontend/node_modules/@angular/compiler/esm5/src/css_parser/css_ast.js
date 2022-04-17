/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { CssToken, CssTokenType } from './css_lexer';
export var BlockType;
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
})(BlockType || (BlockType = {}));
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
export { CssAst };
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
export { CssStyleValueAst };
var CssRuleAst = /** @class */ (function (_super) {
    tslib_1.__extends(CssRuleAst, _super);
    function CssRuleAst(location) {
        return _super.call(this, location) || this;
    }
    return CssRuleAst;
}(CssAst));
export { CssRuleAst };
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
export { CssBlockRuleAst };
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
export { CssKeyframeRuleAst };
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
export { CssKeyframeDefinitionAst };
var CssBlockDefinitionRuleAst = /** @class */ (function (_super) {
    tslib_1.__extends(CssBlockDefinitionRuleAst, _super);
    function CssBlockDefinitionRuleAst(location, strValue, type, query, block) {
        var _this = _super.call(this, location, type, block) || this;
        _this.strValue = strValue;
        _this.query = query;
        var firstCssToken = query.tokens[0];
        _this.name = new CssToken(firstCssToken.index, firstCssToken.column, firstCssToken.line, CssTokenType.Identifier, _this.strValue);
        return _this;
    }
    CssBlockDefinitionRuleAst.prototype.visit = function (visitor, context) {
        return visitor.visitCssBlock(this.block, context);
    };
    return CssBlockDefinitionRuleAst;
}(CssBlockRuleAst));
export { CssBlockDefinitionRuleAst };
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
export { CssMediaQueryRuleAst };
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
export { CssAtRulePredicateAst };
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
export { CssInlineRuleAst };
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
export { CssSelectorRuleAst };
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
export { CssDefinitionAst };
var CssSelectorPartAst = /** @class */ (function (_super) {
    tslib_1.__extends(CssSelectorPartAst, _super);
    function CssSelectorPartAst(location) {
        return _super.call(this, location) || this;
    }
    return CssSelectorPartAst;
}(CssAst));
export { CssSelectorPartAst };
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
export { CssSelectorAst };
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
export { CssSimpleSelectorAst };
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
export { CssPseudoSelectorAst };
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
export { CssBlockAst };
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
export { CssStylesBlockAst };
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
export { CssStyleSheetAst };
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
export { CssUnknownRuleAst };
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
export { CssUnknownTokenListAst };
export function mergeTokens(tokens, separator) {
    if (separator === void 0) { separator = ''; }
    var mainToken = tokens[0];
    var str = mainToken.strValue;
    for (var i = 1; i < tokens.length; i++) {
        str += separator + tokens[i].strValue;
    }
    return new CssToken(mainToken.index, mainToken.column, mainToken.line, mainToken.type, str);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NzX2FzdC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9jc3NfcGFyc2VyL2Nzc19hc3QudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUlILE9BQU8sRUFBQyxRQUFRLEVBQUUsWUFBWSxFQUFDLE1BQU0sYUFBYSxDQUFDO0FBRW5ELE1BQU0sQ0FBTixJQUFZLFNBYVg7QUFiRCxXQUFZLFNBQVM7SUFDbkIsNkNBQU0sQ0FBQTtJQUNOLCtDQUFPLENBQUE7SUFDUCxtREFBUyxDQUFBO0lBQ1QsaURBQVEsQ0FBQTtJQUNSLG1EQUFTLENBQUE7SUFDVCxxREFBVSxDQUFBO0lBQ1YsaURBQVEsQ0FBQTtJQUNSLGlEQUFRLENBQUE7SUFDUix5Q0FBSSxDQUFBO0lBQ0osaURBQVEsQ0FBQTtJQUNSLGtEQUFRLENBQUE7SUFDUix3REFBVyxDQUFBO0FBQ2IsQ0FBQyxFQWJXLFNBQVMsS0FBVCxTQUFTLFFBYXBCO0FBcUJEO0lBQ0UsZ0JBQW1CLFFBQXlCO1FBQXpCLGFBQVEsR0FBUixRQUFRLENBQWlCO0lBQUcsQ0FBQztJQUNoRCxzQkFBSSx5QkFBSzthQUFULGNBQTZCLE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDOzs7T0FBQTtJQUMxRCxzQkFBSSx1QkFBRzthQUFQLGNBQTJCLE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDOzs7T0FBQTtJQUV4RCxhQUFDO0FBQUQsQ0FBQyxBQUxELElBS0M7O0FBRUQ7SUFBc0MsNENBQU07SUFDMUMsMEJBQVksUUFBeUIsRUFBUyxNQUFrQixFQUFTLFFBQWdCO1FBQXpGLFlBQ0Usa0JBQU0sUUFBUSxDQUFDLFNBQ2hCO1FBRjZDLFlBQU0sR0FBTixNQUFNLENBQVk7UUFBUyxjQUFRLEdBQVIsUUFBUSxDQUFROztJQUV6RixD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QUFJLEdBQUcsR0FBRyxTQUFTLENBQUMsUUFBUSxDQUFDO0lBQzdCLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxNQUFNLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO1FBQ3RDLEdBQUcsSUFBSSxTQUFTLEdBQUcsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDLFFBQVEsQ0FBQztLQUN2QztJQUVELE9BQU8sSUFBSSxRQUFRLENBQUMsU0FBUyxDQUFDLEtBQUssRUFBRSxTQUFTLENBQUMsTUFBTSxFQUFFLFNBQVMsQ0FBQyxJQUFJLEVBQUUsU0FBUyxDQUFDLElBQUksRUFBRSxHQUFHLENBQUMsQ0FBQztBQUM5RixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1BhcnNlTG9jYXRpb24sIFBhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5cbmltcG9ydCB7Q3NzVG9rZW4sIENzc1Rva2VuVHlwZX0gZnJvbSAnLi9jc3NfbGV4ZXInO1xuXG5leHBvcnQgZW51bSBCbG9ja1R5cGUge1xuICBJbXBvcnQsXG4gIENoYXJzZXQsXG4gIE5hbWVzcGFjZSxcbiAgU3VwcG9ydHMsXG4gIEtleWZyYW1lcyxcbiAgTWVkaWFRdWVyeSxcbiAgU2VsZWN0b3IsXG4gIEZvbnRGYWNlLFxuICBQYWdlLFxuICBEb2N1bWVudCxcbiAgVmlld3BvcnQsXG4gIFVuc3VwcG9ydGVkXG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ3NzQXN0VmlzaXRvciB7XG4gIHZpc2l0Q3NzVmFsdWUoYXN0OiBDc3NTdHlsZVZhbHVlQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc0lubGluZVJ1bGUoYXN0OiBDc3NJbmxpbmVSdWxlQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc0F0UnVsZVByZWRpY2F0ZShhc3Q6IENzc0F0UnVsZVByZWRpY2F0ZUFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NLZXlmcmFtZVJ1bGUoYXN0OiBDc3NLZXlmcmFtZVJ1bGVBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzS2V5ZnJhbWVEZWZpbml0aW9uKGFzdDogQ3NzS2V5ZnJhbWVEZWZpbml0aW9uQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc01lZGlhUXVlcnlSdWxlKGFzdDogQ3NzTWVkaWFRdWVyeVJ1bGVBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzU2VsZWN0b3JSdWxlKGFzdDogQ3NzU2VsZWN0b3JSdWxlQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc1NlbGVjdG9yKGFzdDogQ3NzU2VsZWN0b3JBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzU2ltcGxlU2VsZWN0b3IoYXN0OiBDc3NTaW1wbGVTZWxlY3RvckFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NQc2V1ZG9TZWxlY3Rvcihhc3Q6IENzc1BzZXVkb1NlbGVjdG9yQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc0RlZmluaXRpb24oYXN0OiBDc3NEZWZpbml0aW9uQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc0Jsb2NrKGFzdDogQ3NzQmxvY2tBc3QsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG4gIHZpc2l0Q3NzU3R5bGVzQmxvY2soYXN0OiBDc3NTdHlsZXNCbG9ja0FzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NTdHlsZVNoZWV0KGFzdDogQ3NzU3R5bGVTaGVldEFzdCwgY29udGV4dD86IGFueSk6IGFueTtcbiAgdmlzaXRDc3NVbmtub3duUnVsZShhc3Q6IENzc1Vua25vd25SdWxlQXN0LCBjb250ZXh0PzogYW55KTogYW55O1xuICB2aXNpdENzc1Vua25vd25Ub2tlbkxpc3QoYXN0OiBDc3NVbmtub3duVG9rZW5MaXN0QXN0LCBjb250ZXh0PzogYW55KTogYW55O1xufVxuXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgQ3NzQXN0IHtcbiAgY29uc3RydWN0b3IocHVibGljIGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4pIHt9XG4gIGdldCBzdGFydCgpOiBQYXJzZUxvY2F0aW9uIHsgcmV0dXJuIHRoaXMubG9jYXRpb24uc3RhcnQ7IH1cbiAgZ2V0IGVuZCgpOiBQYXJzZUxvY2F0aW9uIHsgcmV0dXJuIHRoaXMubG9jYXRpb24uZW5kOyB9XG4gIGFic3RyYWN0IHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnk7XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NTdHlsZVZhbHVlQXN0IGV4dGVuZHMgQ3NzQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIHRva2VuczogQ3NzVG9rZW5bXSwgcHVibGljIHN0clZhbHVlOiBzdHJpbmcpIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7IHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzVmFsdWUodGhpcyk7IH1cbn1cblxuZXhwb3J0IGFic3RyYWN0IGNsYXNzIENzc1J1bGVBc3QgZXh0ZW5kcyBDc3NBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuKSB7IHN1cGVyKGxvY2F0aW9uKTsgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzQmxvY2tSdWxlQXN0IGV4dGVuZHMgQ3NzUnVsZUFzdCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyB0eXBlOiBCbG9ja1R5cGUsIHB1YmxpYyBibG9jazogQ3NzQmxvY2tBc3QsXG4gICAgICBwdWJsaWMgbmFtZTogQ3NzVG9rZW58bnVsbCA9IG51bGwpIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NCbG9jayh0aGlzLmJsb2NrLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzS2V5ZnJhbWVSdWxlQXN0IGV4dGVuZHMgQ3NzQmxvY2tSdWxlQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgbmFtZTogQ3NzVG9rZW4sIGJsb2NrOiBDc3NCbG9ja0FzdCkge1xuICAgIHN1cGVyKGxvY2F0aW9uLCBCbG9ja1R5cGUuS2V5ZnJhbWVzLCBibG9jaywgbmFtZSk7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NLZXlmcmFtZVJ1bGUodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc0tleWZyYW1lRGVmaW5pdGlvbkFzdCBleHRlbmRzIENzc0Jsb2NrUnVsZUFzdCB7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBzdGVwczogQ3NzVG9rZW5bXSwgYmxvY2s6IENzc0Jsb2NrQXN0KSB7XG4gICAgc3VwZXIobG9jYXRpb24sIEJsb2NrVHlwZS5LZXlmcmFtZXMsIGJsb2NrLCBtZXJnZVRva2VucyhzdGVwcywgJywnKSk7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NLZXlmcmFtZURlZmluaXRpb24odGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc0Jsb2NrRGVmaW5pdGlvblJ1bGVBc3QgZXh0ZW5kcyBDc3NCbG9ja1J1bGVBc3Qge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBzdHJWYWx1ZTogc3RyaW5nLCB0eXBlOiBCbG9ja1R5cGUsXG4gICAgICBwdWJsaWMgcXVlcnk6IENzc0F0UnVsZVByZWRpY2F0ZUFzdCwgYmxvY2s6IENzc0Jsb2NrQXN0KSB7XG4gICAgc3VwZXIobG9jYXRpb24sIHR5cGUsIGJsb2NrKTtcbiAgICBjb25zdCBmaXJzdENzc1Rva2VuOiBDc3NUb2tlbiA9IHF1ZXJ5LnRva2Vuc1swXTtcbiAgICB0aGlzLm5hbWUgPSBuZXcgQ3NzVG9rZW4oXG4gICAgICAgIGZpcnN0Q3NzVG9rZW4uaW5kZXgsIGZpcnN0Q3NzVG9rZW4uY29sdW1uLCBmaXJzdENzc1Rva2VuLmxpbmUsIENzc1Rva2VuVHlwZS5JZGVudGlmaWVyLFxuICAgICAgICB0aGlzLnN0clZhbHVlKTtcbiAgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc0Jsb2NrKHRoaXMuYmxvY2ssIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NNZWRpYVF1ZXJ5UnVsZUFzdCBleHRlbmRzIENzc0Jsb2NrRGVmaW5pdGlvblJ1bGVBc3Qge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHN0clZhbHVlOiBzdHJpbmcsIHF1ZXJ5OiBDc3NBdFJ1bGVQcmVkaWNhdGVBc3QsXG4gICAgICBibG9jazogQ3NzQmxvY2tBc3QpIHtcbiAgICBzdXBlcihsb2NhdGlvbiwgc3RyVmFsdWUsIEJsb2NrVHlwZS5NZWRpYVF1ZXJ5LCBxdWVyeSwgYmxvY2spO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzTWVkaWFRdWVyeVJ1bGUodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc0F0UnVsZVByZWRpY2F0ZUFzdCBleHRlbmRzIENzc0FzdCB7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBzdHJWYWx1ZTogc3RyaW5nLCBwdWJsaWMgdG9rZW5zOiBDc3NUb2tlbltdKSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzQXRSdWxlUHJlZGljYXRlKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NJbmxpbmVSdWxlQXN0IGV4dGVuZHMgQ3NzUnVsZUFzdCB7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyB0eXBlOiBCbG9ja1R5cGUsIHB1YmxpYyB2YWx1ZTogQ3NzU3R5bGVWYWx1ZUFzdCkge1xuICAgIHN1cGVyKGxvY2F0aW9uKTtcbiAgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc0lubGluZVJ1bGUodGhpcywgY29udGV4dCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1NlbGVjdG9yUnVsZUFzdCBleHRlbmRzIENzc0Jsb2NrUnVsZUFzdCB7XG4gIHB1YmxpYyBzdHJWYWx1ZTogc3RyaW5nO1xuXG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBzZWxlY3RvcnM6IENzc1NlbGVjdG9yQXN0W10sIGJsb2NrOiBDc3NCbG9ja0FzdCkge1xuICAgIHN1cGVyKGxvY2F0aW9uLCBCbG9ja1R5cGUuU2VsZWN0b3IsIGJsb2NrKTtcbiAgICB0aGlzLnN0clZhbHVlID0gc2VsZWN0b3JzLm1hcChzZWxlY3RvciA9PiBzZWxlY3Rvci5zdHJWYWx1ZSkuam9pbignLCcpO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzU2VsZWN0b3JSdWxlKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NEZWZpbml0aW9uQXN0IGV4dGVuZHMgQ3NzQXN0IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgcHJvcGVydHk6IENzc1Rva2VuLCBwdWJsaWMgdmFsdWU6IENzc1N0eWxlVmFsdWVBc3QpIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NEZWZpbml0aW9uKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBDc3NTZWxlY3RvclBhcnRBc3QgZXh0ZW5kcyBDc3NBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuKSB7IHN1cGVyKGxvY2F0aW9uKTsgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzU2VsZWN0b3JBc3QgZXh0ZW5kcyBDc3NTZWxlY3RvclBhcnRBc3Qge1xuICBwdWJsaWMgc3RyVmFsdWU6IHN0cmluZztcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIHNlbGVjdG9yUGFydHM6IENzc1NpbXBsZVNlbGVjdG9yQXN0W10pIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gICAgdGhpcy5zdHJWYWx1ZSA9IHNlbGVjdG9yUGFydHMubWFwKHBhcnQgPT4gcGFydC5zdHJWYWx1ZSkuam9pbignJyk7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NTZWxlY3Rvcih0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzU2ltcGxlU2VsZWN0b3JBc3QgZXh0ZW5kcyBDc3NTZWxlY3RvclBhcnRBc3Qge1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyB0b2tlbnM6IENzc1Rva2VuW10sIHB1YmxpYyBzdHJWYWx1ZTogc3RyaW5nLFxuICAgICAgcHVibGljIHBzZXVkb1NlbGVjdG9yczogQ3NzUHNldWRvU2VsZWN0b3JBc3RbXSwgcHVibGljIG9wZXJhdG9yOiBDc3NUb2tlbikge1xuICAgIHN1cGVyKGxvY2F0aW9uKTtcbiAgfVxuICB2aXNpdCh2aXNpdG9yOiBDc3NBc3RWaXNpdG9yLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdmlzaXRvci52aXNpdENzc1NpbXBsZVNlbGVjdG9yKHRoaXMsIGNvbnRleHQpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NQc2V1ZG9TZWxlY3RvckFzdCBleHRlbmRzIENzc1NlbGVjdG9yUGFydEFzdCB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgbG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIHN0clZhbHVlOiBzdHJpbmcsIHB1YmxpYyBuYW1lOiBzdHJpbmcsXG4gICAgICBwdWJsaWMgdG9rZW5zOiBDc3NUb2tlbltdLCBwdWJsaWMgaW5uZXJTZWxlY3RvcnM6IENzc1NlbGVjdG9yQXN0W10pIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NQc2V1ZG9TZWxlY3Rvcih0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzQmxvY2tBc3QgZXh0ZW5kcyBDc3NBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgZW50cmllczogQ3NzQXN0W10pIHsgc3VwZXIobG9jYXRpb24pOyB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdENzc0Jsb2NrKHRoaXMsIGNvbnRleHQpOyB9XG59XG5cbi8qXG4gYSBzdHlsZSBibG9jayBpcyBkaWZmZXJlbnQgZnJvbSBhIHN0YW5kYXJkIGJsb2NrIGJlY2F1c2UgaXQgY29udGFpbnNcbiBjc3MgcHJvcDp2YWx1ZSBkZWZpbml0aW9ucy4gQSByZWd1bGFyIGJsb2NrIGNhbiBjb250YWluIGEgbGlzdCBvZiBBc3QgZW50cmllcy5cbiAqL1xuZXhwb3J0IGNsYXNzIENzc1N0eWxlc0Jsb2NrQXN0IGV4dGVuZHMgQ3NzQmxvY2tBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgZGVmaW5pdGlvbnM6IENzc0RlZmluaXRpb25Bc3RbXSkge1xuICAgIHN1cGVyKGxvY2F0aW9uLCBkZWZpbml0aW9ucyk7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NTdHlsZXNCbG9jayh0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzU3R5bGVTaGVldEFzdCBleHRlbmRzIENzc0FzdCB7XG4gIGNvbnN0cnVjdG9yKGxvY2F0aW9uOiBQYXJzZVNvdXJjZVNwYW4sIHB1YmxpYyBydWxlczogQ3NzQXN0W10pIHsgc3VwZXIobG9jYXRpb24pOyB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzU3R5bGVTaGVldCh0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzVW5rbm93blJ1bGVBc3QgZXh0ZW5kcyBDc3NSdWxlQXN0IHtcbiAgY29uc3RydWN0b3IobG9jYXRpb246IFBhcnNlU291cmNlU3BhbiwgcHVibGljIHJ1bGVOYW1lOiBzdHJpbmcsIHB1YmxpYyB0b2tlbnM6IENzc1Rva2VuW10pIHtcbiAgICBzdXBlcihsb2NhdGlvbik7XG4gIH1cbiAgdmlzaXQodmlzaXRvcjogQ3NzQXN0VmlzaXRvciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHZpc2l0b3IudmlzaXRDc3NVbmtub3duUnVsZSh0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzVW5rbm93blRva2VuTGlzdEFzdCBleHRlbmRzIENzc1J1bGVBc3Qge1xuICBjb25zdHJ1Y3Rvcihsb2NhdGlvbjogUGFyc2VTb3VyY2VTcGFuLCBwdWJsaWMgbmFtZTogc3RyaW5nLCBwdWJsaWMgdG9rZW5zOiBDc3NUb2tlbltdKSB7XG4gICAgc3VwZXIobG9jYXRpb24pO1xuICB9XG4gIHZpc2l0KHZpc2l0b3I6IENzc0FzdFZpc2l0b3IsIGNvbnRleHQ/OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiB2aXNpdG9yLnZpc2l0Q3NzVW5rbm93blRva2VuTGlzdCh0aGlzLCBjb250ZXh0KTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gbWVyZ2VUb2tlbnModG9rZW5zOiBDc3NUb2tlbltdLCBzZXBhcmF0b3I6IHN0cmluZyA9ICcnKTogQ3NzVG9rZW4ge1xuICBjb25zdCBtYWluVG9rZW4gPSB0b2tlbnNbMF07XG4gIGxldCBzdHIgPSBtYWluVG9rZW4uc3RyVmFsdWU7XG4gIGZvciAobGV0IGkgPSAxOyBpIDwgdG9rZW5zLmxlbmd0aDsgaSsrKSB7XG4gICAgc3RyICs9IHNlcGFyYXRvciArIHRva2Vuc1tpXS5zdHJWYWx1ZTtcbiAgfVxuXG4gIHJldHVybiBuZXcgQ3NzVG9rZW4obWFpblRva2VuLmluZGV4LCBtYWluVG9rZW4uY29sdW1uLCBtYWluVG9rZW4ubGluZSwgbWFpblRva2VuLnR5cGUsIHN0cik7XG59XG4iXX0=