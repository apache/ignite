/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ParseLocation, ParseSourceSpan } from '../parse_util';
import { CssToken } from './css_lexer';
export declare enum BlockType {
    Import = 0,
    Charset = 1,
    Namespace = 2,
    Supports = 3,
    Keyframes = 4,
    MediaQuery = 5,
    Selector = 6,
    FontFace = 7,
    Page = 8,
    Document = 9,
    Viewport = 10,
    Unsupported = 11
}
export interface CssAstVisitor {
    visitCssValue(ast: CssStyleValueAst, context?: any): any;
    visitCssInlineRule(ast: CssInlineRuleAst, context?: any): any;
    visitCssAtRulePredicate(ast: CssAtRulePredicateAst, context?: any): any;
    visitCssKeyframeRule(ast: CssKeyframeRuleAst, context?: any): any;
    visitCssKeyframeDefinition(ast: CssKeyframeDefinitionAst, context?: any): any;
    visitCssMediaQueryRule(ast: CssMediaQueryRuleAst, context?: any): any;
    visitCssSelectorRule(ast: CssSelectorRuleAst, context?: any): any;
    visitCssSelector(ast: CssSelectorAst, context?: any): any;
    visitCssSimpleSelector(ast: CssSimpleSelectorAst, context?: any): any;
    visitCssPseudoSelector(ast: CssPseudoSelectorAst, context?: any): any;
    visitCssDefinition(ast: CssDefinitionAst, context?: any): any;
    visitCssBlock(ast: CssBlockAst, context?: any): any;
    visitCssStylesBlock(ast: CssStylesBlockAst, context?: any): any;
    visitCssStyleSheet(ast: CssStyleSheetAst, context?: any): any;
    visitCssUnknownRule(ast: CssUnknownRuleAst, context?: any): any;
    visitCssUnknownTokenList(ast: CssUnknownTokenListAst, context?: any): any;
}
export declare abstract class CssAst {
    location: ParseSourceSpan;
    constructor(location: ParseSourceSpan);
    readonly start: ParseLocation;
    readonly end: ParseLocation;
    abstract visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssStyleValueAst extends CssAst {
    tokens: CssToken[];
    strValue: string;
    constructor(location: ParseSourceSpan, tokens: CssToken[], strValue: string);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare abstract class CssRuleAst extends CssAst {
    constructor(location: ParseSourceSpan);
}
export declare class CssBlockRuleAst extends CssRuleAst {
    location: ParseSourceSpan;
    type: BlockType;
    block: CssBlockAst;
    name: CssToken | null;
    constructor(location: ParseSourceSpan, type: BlockType, block: CssBlockAst, name?: CssToken | null);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssKeyframeRuleAst extends CssBlockRuleAst {
    constructor(location: ParseSourceSpan, name: CssToken, block: CssBlockAst);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssKeyframeDefinitionAst extends CssBlockRuleAst {
    steps: CssToken[];
    constructor(location: ParseSourceSpan, steps: CssToken[], block: CssBlockAst);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssBlockDefinitionRuleAst extends CssBlockRuleAst {
    strValue: string;
    query: CssAtRulePredicateAst;
    constructor(location: ParseSourceSpan, strValue: string, type: BlockType, query: CssAtRulePredicateAst, block: CssBlockAst);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssMediaQueryRuleAst extends CssBlockDefinitionRuleAst {
    constructor(location: ParseSourceSpan, strValue: string, query: CssAtRulePredicateAst, block: CssBlockAst);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssAtRulePredicateAst extends CssAst {
    strValue: string;
    tokens: CssToken[];
    constructor(location: ParseSourceSpan, strValue: string, tokens: CssToken[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssInlineRuleAst extends CssRuleAst {
    type: BlockType;
    value: CssStyleValueAst;
    constructor(location: ParseSourceSpan, type: BlockType, value: CssStyleValueAst);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssSelectorRuleAst extends CssBlockRuleAst {
    selectors: CssSelectorAst[];
    strValue: string;
    constructor(location: ParseSourceSpan, selectors: CssSelectorAst[], block: CssBlockAst);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssDefinitionAst extends CssAst {
    property: CssToken;
    value: CssStyleValueAst;
    constructor(location: ParseSourceSpan, property: CssToken, value: CssStyleValueAst);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare abstract class CssSelectorPartAst extends CssAst {
    constructor(location: ParseSourceSpan);
}
export declare class CssSelectorAst extends CssSelectorPartAst {
    selectorParts: CssSimpleSelectorAst[];
    strValue: string;
    constructor(location: ParseSourceSpan, selectorParts: CssSimpleSelectorAst[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssSimpleSelectorAst extends CssSelectorPartAst {
    tokens: CssToken[];
    strValue: string;
    pseudoSelectors: CssPseudoSelectorAst[];
    operator: CssToken;
    constructor(location: ParseSourceSpan, tokens: CssToken[], strValue: string, pseudoSelectors: CssPseudoSelectorAst[], operator: CssToken);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssPseudoSelectorAst extends CssSelectorPartAst {
    strValue: string;
    name: string;
    tokens: CssToken[];
    innerSelectors: CssSelectorAst[];
    constructor(location: ParseSourceSpan, strValue: string, name: string, tokens: CssToken[], innerSelectors: CssSelectorAst[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssBlockAst extends CssAst {
    entries: CssAst[];
    constructor(location: ParseSourceSpan, entries: CssAst[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssStylesBlockAst extends CssBlockAst {
    definitions: CssDefinitionAst[];
    constructor(location: ParseSourceSpan, definitions: CssDefinitionAst[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssStyleSheetAst extends CssAst {
    rules: CssAst[];
    constructor(location: ParseSourceSpan, rules: CssAst[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssUnknownRuleAst extends CssRuleAst {
    ruleName: string;
    tokens: CssToken[];
    constructor(location: ParseSourceSpan, ruleName: string, tokens: CssToken[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare class CssUnknownTokenListAst extends CssRuleAst {
    name: string;
    tokens: CssToken[];
    constructor(location: ParseSourceSpan, name: string, tokens: CssToken[]);
    visit(visitor: CssAstVisitor, context?: any): any;
}
export declare function mergeTokens(tokens: CssToken[], separator?: string): CssToken;
