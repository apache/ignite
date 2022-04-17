/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as chars from '../chars';
import { ParseError, ParseLocation, ParseSourceFile, ParseSourceSpan } from '../parse_util';
import { BlockType, CssAst, CssAtRulePredicateAst, CssBlockAst, CssBlockDefinitionRuleAst, CssBlockRuleAst, CssDefinitionAst, CssInlineRuleAst, CssKeyframeDefinitionAst, CssKeyframeRuleAst, CssMediaQueryRuleAst, CssPseudoSelectorAst, CssSelectorAst, CssSelectorRuleAst, CssSimpleSelectorAst, CssStyleSheetAst, CssStyleValueAst, CssStylesBlockAst, CssUnknownRuleAst, CssUnknownTokenListAst, mergeTokens } from './css_ast';
import { CssLexer, CssLexerMode, CssToken, CssTokenType, generateErrorMessage, getRawMessage, isNewline } from './css_lexer';
const SPACE_OPERATOR = ' ';
export { CssToken } from './css_lexer';
export { BlockType } from './css_ast';
const SLASH_CHARACTER = '/';
const GT_CHARACTER = '>';
const TRIPLE_GT_OPERATOR_STR = '>>>';
const DEEP_OPERATOR_STR = '/deep/';
const EOF_DELIM_FLAG = 1;
const RBRACE_DELIM_FLAG = 2;
const LBRACE_DELIM_FLAG = 4;
const COMMA_DELIM_FLAG = 8;
const COLON_DELIM_FLAG = 16;
const SEMICOLON_DELIM_FLAG = 32;
const NEWLINE_DELIM_FLAG = 64;
const RPAREN_DELIM_FLAG = 128;
const LPAREN_DELIM_FLAG = 256;
const SPACE_DELIM_FLAG = 512;
function _pseudoSelectorSupportsInnerSelectors(name) {
    return ['not', 'host', 'host-context'].indexOf(name) >= 0;
}
function isSelectorOperatorCharacter(code) {
    switch (code) {
        case chars.$SLASH:
        case chars.$TILDA:
        case chars.$PLUS:
        case chars.$GT:
            return true;
        default:
            return chars.isWhitespace(code);
    }
}
function getDelimFromCharacter(code) {
    switch (code) {
        case chars.$EOF:
            return EOF_DELIM_FLAG;
        case chars.$COMMA:
            return COMMA_DELIM_FLAG;
        case chars.$COLON:
            return COLON_DELIM_FLAG;
        case chars.$SEMICOLON:
            return SEMICOLON_DELIM_FLAG;
        case chars.$RBRACE:
            return RBRACE_DELIM_FLAG;
        case chars.$LBRACE:
            return LBRACE_DELIM_FLAG;
        case chars.$RPAREN:
            return RPAREN_DELIM_FLAG;
        case chars.$SPACE:
        case chars.$TAB:
            return SPACE_DELIM_FLAG;
        default:
            return isNewline(code) ? NEWLINE_DELIM_FLAG : 0;
    }
}
function characterContainsDelimiter(code, delimiters) {
    return (getDelimFromCharacter(code) & delimiters) > 0;
}
export class ParsedCssResult {
    constructor(errors, ast) {
        this.errors = errors;
        this.ast = ast;
    }
}
export class CssParser {
    constructor() {
        this._errors = [];
    }
    /**
     * @param css the CSS code that will be parsed
     * @param url the name of the CSS file containing the CSS source code
     */
    parse(css, url) {
        const lexer = new CssLexer();
        this._file = new ParseSourceFile(css, url);
        this._scanner = lexer.scan(css, false);
        const ast = this._parseStyleSheet(EOF_DELIM_FLAG);
        const errors = this._errors;
        this._errors = [];
        const result = new ParsedCssResult(errors, ast);
        this._file = null;
        this._scanner = null;
        return result;
    }
    /** @internal */
    _parseStyleSheet(delimiters) {
        const results = [];
        this._scanner.consumeEmptyStatements();
        while (this._scanner.peek != chars.$EOF) {
            this._scanner.setMode(CssLexerMode.BLOCK);
            results.push(this._parseRule(delimiters));
        }
        let span = null;
        if (results.length > 0) {
            const firstRule = results[0];
            // we collect the last token like so incase there was an
            // EOF token that was emitted sometime during the lexing
            span = this._generateSourceSpan(firstRule, this._lastToken);
        }
        return new CssStyleSheetAst(span, results);
    }
    /** @internal */
    _getSourceContent() { return this._scanner != null ? this._scanner.input : ''; }
    /** @internal */
    _extractSourceContent(start, end) {
        return this._getSourceContent().substring(start, end + 1);
    }
    /** @internal */
    _generateSourceSpan(start, end = null) {
        let startLoc;
        if (start instanceof CssAst) {
            startLoc = start.location.start;
        }
        else {
            let token = start;
            if (token == null) {
                // the data here is invalid, however, if and when this does
                // occur, any other errors associated with this will be collected
                token = this._lastToken;
            }
            startLoc = new ParseLocation(this._file, token.index, token.line, token.column);
        }
        if (end == null) {
            end = this._lastToken;
        }
        let endLine = -1;
        let endColumn = -1;
        let endIndex = -1;
        if (end instanceof CssAst) {
            endLine = end.location.end.line;
            endColumn = end.location.end.col;
            endIndex = end.location.end.offset;
        }
        else if (end instanceof CssToken) {
            endLine = end.line;
            endColumn = end.column;
            endIndex = end.index;
        }
        const endLoc = new ParseLocation(this._file, endIndex, endLine, endColumn);
        return new ParseSourceSpan(startLoc, endLoc);
    }
    /** @internal */
    _resolveBlockType(token) {
        switch (token.strValue) {
            case '@-o-keyframes':
            case '@-moz-keyframes':
            case '@-webkit-keyframes':
            case '@keyframes':
                return BlockType.Keyframes;
            case '@charset':
                return BlockType.Charset;
            case '@import':
                return BlockType.Import;
            case '@namespace':
                return BlockType.Namespace;
            case '@page':
                return BlockType.Page;
            case '@document':
                return BlockType.Document;
            case '@media':
                return BlockType.MediaQuery;
            case '@font-face':
                return BlockType.FontFace;
            case '@viewport':
                return BlockType.Viewport;
            case '@supports':
                return BlockType.Supports;
            default:
                return BlockType.Unsupported;
        }
    }
    /** @internal */
    _parseRule(delimiters) {
        if (this._scanner.peek == chars.$AT) {
            return this._parseAtRule(delimiters);
        }
        return this._parseSelectorRule(delimiters);
    }
    /** @internal */
    _parseAtRule(delimiters) {
        const start = this._getScannerIndex();
        this._scanner.setMode(CssLexerMode.BLOCK);
        const token = this._scan();
        const startToken = token;
        this._assertCondition(token.type == CssTokenType.AtKeyword, `The CSS Rule ${token.strValue} is not a valid [@] rule.`, token);
        let block;
        const type = this._resolveBlockType(token);
        let span;
        let tokens;
        let endToken;
        let end;
        let strValue;
        let query;
        switch (type) {
            case BlockType.Charset:
            case BlockType.Namespace:
            case BlockType.Import:
                let value = this._parseValue(delimiters);
                this._scanner.setMode(CssLexerMode.BLOCK);
                this._scanner.consumeEmptyStatements();
                span = this._generateSourceSpan(startToken, value);
                return new CssInlineRuleAst(span, type, value);
            case BlockType.Viewport:
            case BlockType.FontFace:
                block = this._parseStyleBlock(delimiters);
                span = this._generateSourceSpan(startToken, block);
                return new CssBlockRuleAst(span, type, block);
            case BlockType.Keyframes:
                tokens = this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG);
                // keyframes only have one identifier name
                let name = tokens[0];
                block = this._parseKeyframeBlock(delimiters);
                span = this._generateSourceSpan(startToken, block);
                return new CssKeyframeRuleAst(span, name, block);
            case BlockType.MediaQuery:
                this._scanner.setMode(CssLexerMode.MEDIA_QUERY);
                tokens = this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG);
                endToken = tokens[tokens.length - 1];
                // we do not track the whitespace after the mediaQuery predicate ends
                // so we have to calculate the end string value on our own
                end = endToken.index + endToken.strValue.length - 1;
                strValue = this._extractSourceContent(start, end);
                span = this._generateSourceSpan(startToken, endToken);
                query = new CssAtRulePredicateAst(span, strValue, tokens);
                block = this._parseBlock(delimiters);
                strValue = this._extractSourceContent(start, this._getScannerIndex() - 1);
                span = this._generateSourceSpan(startToken, block);
                return new CssMediaQueryRuleAst(span, strValue, query, block);
            case BlockType.Document:
            case BlockType.Supports:
            case BlockType.Page:
                this._scanner.setMode(CssLexerMode.AT_RULE_QUERY);
                tokens = this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG);
                endToken = tokens[tokens.length - 1];
                // we do not track the whitespace after this block rule predicate ends
                // so we have to calculate the end string value on our own
                end = endToken.index + endToken.strValue.length - 1;
                strValue = this._extractSourceContent(start, end);
                span = this._generateSourceSpan(startToken, tokens[tokens.length - 1]);
                query = new CssAtRulePredicateAst(span, strValue, tokens);
                block = this._parseBlock(delimiters);
                strValue = this._extractSourceContent(start, block.end.offset);
                span = this._generateSourceSpan(startToken, block);
                return new CssBlockDefinitionRuleAst(span, strValue, type, query, block);
            // if a custom @rule { ... } is used it should still tokenize the insides
            default:
                let listOfTokens = [];
                let tokenName = token.strValue;
                this._scanner.setMode(CssLexerMode.ALL);
                this._error(generateErrorMessage(this._getSourceContent(), `The CSS "at" rule "${tokenName}" is not allowed to used here`, token.strValue, token.index, token.line, token.column), token);
                this._collectUntilDelim(delimiters | LBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG)
                    .forEach((token) => { listOfTokens.push(token); });
                if (this._scanner.peek == chars.$LBRACE) {
                    listOfTokens.push(this._consume(CssTokenType.Character, '{'));
                    this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG)
                        .forEach((token) => { listOfTokens.push(token); });
                    listOfTokens.push(this._consume(CssTokenType.Character, '}'));
                }
                endToken = listOfTokens[listOfTokens.length - 1];
                span = this._generateSourceSpan(startToken, endToken);
                return new CssUnknownRuleAst(span, tokenName, listOfTokens);
        }
    }
    /** @internal */
    _parseSelectorRule(delimiters) {
        const start = this._getScannerIndex();
        const selectors = this._parseSelectors(delimiters);
        const block = this._parseStyleBlock(delimiters);
        let ruleAst;
        let span;
        const startSelector = selectors[0];
        if (block != null) {
            span = this._generateSourceSpan(startSelector, block);
            ruleAst = new CssSelectorRuleAst(span, selectors, block);
        }
        else {
            const name = this._extractSourceContent(start, this._getScannerIndex() - 1);
            const innerTokens = [];
            selectors.forEach((selector) => {
                selector.selectorParts.forEach((part) => {
                    part.tokens.forEach((token) => { innerTokens.push(token); });
                });
            });
            const endToken = innerTokens[innerTokens.length - 1];
            span = this._generateSourceSpan(startSelector, endToken);
            ruleAst = new CssUnknownTokenListAst(span, name, innerTokens);
        }
        this._scanner.setMode(CssLexerMode.BLOCK);
        this._scanner.consumeEmptyStatements();
        return ruleAst;
    }
    /** @internal */
    _parseSelectors(delimiters) {
        delimiters |= LBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG;
        const selectors = [];
        let isParsingSelectors = true;
        while (isParsingSelectors) {
            selectors.push(this._parseSelector(delimiters));
            isParsingSelectors = !characterContainsDelimiter(this._scanner.peek, delimiters);
            if (isParsingSelectors) {
                this._consume(CssTokenType.Character, ',');
                isParsingSelectors = !characterContainsDelimiter(this._scanner.peek, delimiters);
                if (isParsingSelectors) {
                    this._scanner.consumeWhitespace();
                }
            }
        }
        return selectors;
    }
    /** @internal */
    _scan() {
        const output = this._scanner.scan();
        const token = output.token;
        const error = output.error;
        if (error != null) {
            this._error(getRawMessage(error), token);
        }
        this._lastToken = token;
        return token;
    }
    /** @internal */
    _getScannerIndex() { return this._scanner.index; }
    /** @internal */
    _consume(type, value = null) {
        const output = this._scanner.consume(type, value);
        const token = output.token;
        const error = output.error;
        if (error != null) {
            this._error(getRawMessage(error), token);
        }
        this._lastToken = token;
        return token;
    }
    /** @internal */
    _parseKeyframeBlock(delimiters) {
        delimiters |= RBRACE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.KEYFRAME_BLOCK);
        const startToken = this._consume(CssTokenType.Character, '{');
        const definitions = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            definitions.push(this._parseKeyframeDefinition(delimiters));
        }
        const endToken = this._consume(CssTokenType.Character, '}');
        const span = this._generateSourceSpan(startToken, endToken);
        return new CssBlockAst(span, definitions);
    }
    /** @internal */
    _parseKeyframeDefinition(delimiters) {
        const start = this._getScannerIndex();
        const stepTokens = [];
        delimiters |= LBRACE_DELIM_FLAG;
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            stepTokens.push(this._parseKeyframeLabel(delimiters | COMMA_DELIM_FLAG));
            if (this._scanner.peek != chars.$LBRACE) {
                this._consume(CssTokenType.Character, ',');
            }
        }
        const stylesBlock = this._parseStyleBlock(delimiters | RBRACE_DELIM_FLAG);
        const span = this._generateSourceSpan(stepTokens[0], stylesBlock);
        const ast = new CssKeyframeDefinitionAst(span, stepTokens, stylesBlock);
        this._scanner.setMode(CssLexerMode.BLOCK);
        return ast;
    }
    /** @internal */
    _parseKeyframeLabel(delimiters) {
        this._scanner.setMode(CssLexerMode.KEYFRAME_BLOCK);
        return mergeTokens(this._collectUntilDelim(delimiters));
    }
    /** @internal */
    _parsePseudoSelector(delimiters) {
        const start = this._getScannerIndex();
        delimiters &= ~COMMA_DELIM_FLAG;
        // we keep the original value since we may use it to recurse when :not, :host are used
        const startingDelims = delimiters;
        const startToken = this._consume(CssTokenType.Character, ':');
        const tokens = [startToken];
        if (this._scanner.peek == chars.$COLON) { // ::something
            tokens.push(this._consume(CssTokenType.Character, ':'));
        }
        const innerSelectors = [];
        this._scanner.setMode(CssLexerMode.PSEUDO_SELECTOR);
        // host, host-context, lang, not, nth-child are all identifiers
        const pseudoSelectorToken = this._consume(CssTokenType.Identifier);
        const pseudoSelectorName = pseudoSelectorToken.strValue;
        tokens.push(pseudoSelectorToken);
        // host(), lang(), nth-child(), etc...
        if (this._scanner.peek == chars.$LPAREN) {
            this._scanner.setMode(CssLexerMode.PSEUDO_SELECTOR_WITH_ARGUMENTS);
            const openParenToken = this._consume(CssTokenType.Character, '(');
            tokens.push(openParenToken);
            // :host(innerSelector(s)), :not(selector), etc...
            if (_pseudoSelectorSupportsInnerSelectors(pseudoSelectorName)) {
                let innerDelims = startingDelims | LPAREN_DELIM_FLAG | RPAREN_DELIM_FLAG;
                if (pseudoSelectorName == 'not') {
                    // the inner selector inside of :not(...) can only be one
                    // CSS selector (no commas allowed) ... This is according
                    // to the CSS specification
                    innerDelims |= COMMA_DELIM_FLAG;
                }
                // :host(a, b, c) {
                this._parseSelectors(innerDelims).forEach((selector, index) => {
                    innerSelectors.push(selector);
                });
            }
            else {
                // this branch is for things like "en-us, 2k + 1, etc..."
                // which all end up in pseudoSelectors like :lang, :nth-child, etc..
                const innerValueDelims = delimiters | LBRACE_DELIM_FLAG | COLON_DELIM_FLAG |
                    RPAREN_DELIM_FLAG | LPAREN_DELIM_FLAG;
                while (!characterContainsDelimiter(this._scanner.peek, innerValueDelims)) {
                    const token = this._scan();
                    tokens.push(token);
                }
            }
            const closeParenToken = this._consume(CssTokenType.Character, ')');
            tokens.push(closeParenToken);
        }
        const end = this._getScannerIndex() - 1;
        const strValue = this._extractSourceContent(start, end);
        const endToken = tokens[tokens.length - 1];
        const span = this._generateSourceSpan(startToken, endToken);
        return new CssPseudoSelectorAst(span, strValue, pseudoSelectorName, tokens, innerSelectors);
    }
    /** @internal */
    _parseSimpleSelector(delimiters) {
        const start = this._getScannerIndex();
        delimiters |= COMMA_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.SELECTOR);
        const selectorCssTokens = [];
        const pseudoSelectors = [];
        let previousToken = undefined;
        const selectorPartDelimiters = delimiters | SPACE_DELIM_FLAG;
        let loopOverSelector = !characterContainsDelimiter(this._scanner.peek, selectorPartDelimiters);
        let hasAttributeError = false;
        while (loopOverSelector) {
            const peek = this._scanner.peek;
            switch (peek) {
                case chars.$COLON:
                    let innerPseudo = this._parsePseudoSelector(delimiters);
                    pseudoSelectors.push(innerPseudo);
                    this._scanner.setMode(CssLexerMode.SELECTOR);
                    break;
                case chars.$LBRACKET:
                    // we set the mode after the scan because attribute mode does not
                    // allow attribute [] values. And this also will catch any errors
                    // if an extra "[" is used inside.
                    selectorCssTokens.push(this._scan());
                    this._scanner.setMode(CssLexerMode.ATTRIBUTE_SELECTOR);
                    break;
                case chars.$RBRACKET:
                    if (this._scanner.getMode() != CssLexerMode.ATTRIBUTE_SELECTOR) {
                        hasAttributeError = true;
                    }
                    // we set the mode early because attribute mode does not
                    // allow attribute [] values
                    this._scanner.setMode(CssLexerMode.SELECTOR);
                    selectorCssTokens.push(this._scan());
                    break;
                default:
                    if (isSelectorOperatorCharacter(peek)) {
                        loopOverSelector = false;
                        continue;
                    }
                    let token = this._scan();
                    previousToken = token;
                    selectorCssTokens.push(token);
                    break;
            }
            loopOverSelector = !characterContainsDelimiter(this._scanner.peek, selectorPartDelimiters);
        }
        hasAttributeError =
            hasAttributeError || this._scanner.getMode() == CssLexerMode.ATTRIBUTE_SELECTOR;
        if (hasAttributeError) {
            this._error(`Unbalanced CSS attribute selector at column ${previousToken.line}:${previousToken.column}`, previousToken);
        }
        let end = this._getScannerIndex() - 1;
        // this happens if the selector is not directly followed by
        // a comma or curly brace without a space in between
        let operator = null;
        let operatorScanCount = 0;
        let lastOperatorToken = null;
        if (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            while (operator == null && !characterContainsDelimiter(this._scanner.peek, delimiters) &&
                isSelectorOperatorCharacter(this._scanner.peek)) {
                let token = this._scan();
                const tokenOperator = token.strValue;
                operatorScanCount++;
                lastOperatorToken = token;
                if (tokenOperator != SPACE_OPERATOR) {
                    switch (tokenOperator) {
                        case SLASH_CHARACTER:
                            // /deep/ operator
                            let deepToken = this._consume(CssTokenType.Identifier);
                            let deepSlash = this._consume(CssTokenType.Character);
                            let index = lastOperatorToken.index;
                            let line = lastOperatorToken.line;
                            let column = lastOperatorToken.column;
                            if (deepToken != null && deepToken.strValue.toLowerCase() == 'deep' &&
                                deepSlash.strValue == SLASH_CHARACTER) {
                                token = new CssToken(lastOperatorToken.index, lastOperatorToken.column, lastOperatorToken.line, CssTokenType.Identifier, DEEP_OPERATOR_STR);
                            }
                            else {
                                const text = SLASH_CHARACTER + deepToken.strValue + deepSlash.strValue;
                                this._error(generateErrorMessage(this._getSourceContent(), `${text} is an invalid CSS operator`, text, index, line, column), lastOperatorToken);
                                token = new CssToken(index, column, line, CssTokenType.Invalid, text);
                            }
                            break;
                        case GT_CHARACTER:
                            // >>> operator
                            if (this._scanner.peek == chars.$GT && this._scanner.peekPeek == chars.$GT) {
                                this._consume(CssTokenType.Character, GT_CHARACTER);
                                this._consume(CssTokenType.Character, GT_CHARACTER);
                                token = new CssToken(lastOperatorToken.index, lastOperatorToken.column, lastOperatorToken.line, CssTokenType.Identifier, TRIPLE_GT_OPERATOR_STR);
                            }
                            break;
                    }
                    operator = token;
                }
            }
            // so long as there is an operator then we can have an
            // ending value that is beyond the selector value ...
            // otherwise it's just a bunch of trailing whitespace
            if (operator != null) {
                end = operator.index;
            }
        }
        this._scanner.consumeWhitespace();
        const strValue = this._extractSourceContent(start, end);
        // if we do come across one or more spaces inside of
        // the operators loop then an empty space is still a
        // valid operator to use if something else was not found
        if (operator == null && operatorScanCount > 0 && this._scanner.peek != chars.$LBRACE) {
            operator = lastOperatorToken;
        }
        // please note that `endToken` is reassigned multiple times below
        // so please do not optimize the if statements into if/elseif
        let startTokenOrAst = null;
        let endTokenOrAst = null;
        if (selectorCssTokens.length > 0) {
            startTokenOrAst = startTokenOrAst || selectorCssTokens[0];
            endTokenOrAst = selectorCssTokens[selectorCssTokens.length - 1];
        }
        if (pseudoSelectors.length > 0) {
            startTokenOrAst = startTokenOrAst || pseudoSelectors[0];
            endTokenOrAst = pseudoSelectors[pseudoSelectors.length - 1];
        }
        if (operator != null) {
            startTokenOrAst = startTokenOrAst || operator;
            endTokenOrAst = operator;
        }
        const span = this._generateSourceSpan(startTokenOrAst, endTokenOrAst);
        return new CssSimpleSelectorAst(span, selectorCssTokens, strValue, pseudoSelectors, operator);
    }
    /** @internal */
    _parseSelector(delimiters) {
        delimiters |= COMMA_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.SELECTOR);
        const simpleSelectors = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            simpleSelectors.push(this._parseSimpleSelector(delimiters));
            this._scanner.consumeWhitespace();
        }
        const firstSelector = simpleSelectors[0];
        const lastSelector = simpleSelectors[simpleSelectors.length - 1];
        const span = this._generateSourceSpan(firstSelector, lastSelector);
        return new CssSelectorAst(span, simpleSelectors);
    }
    /** @internal */
    _parseValue(delimiters) {
        delimiters |= RBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG | NEWLINE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.STYLE_VALUE);
        const start = this._getScannerIndex();
        const tokens = [];
        let wsStr = '';
        let previous = undefined;
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            let token;
            if (previous != null && previous.type == CssTokenType.Identifier &&
                this._scanner.peek == chars.$LPAREN) {
                token = this._consume(CssTokenType.Character, '(');
                tokens.push(token);
                this._scanner.setMode(CssLexerMode.STYLE_VALUE_FUNCTION);
                token = this._scan();
                tokens.push(token);
                this._scanner.setMode(CssLexerMode.STYLE_VALUE);
                token = this._consume(CssTokenType.Character, ')');
                tokens.push(token);
            }
            else {
                token = this._scan();
                if (token.type == CssTokenType.Whitespace) {
                    wsStr += token.strValue;
                }
                else {
                    wsStr = '';
                    tokens.push(token);
                }
            }
            previous = token;
        }
        const end = this._getScannerIndex() - 1;
        this._scanner.consumeWhitespace();
        const code = this._scanner.peek;
        if (code == chars.$SEMICOLON) {
            this._consume(CssTokenType.Character, ';');
        }
        else if (code != chars.$RBRACE) {
            this._error(generateErrorMessage(this._getSourceContent(), `The CSS key/value definition did not end with a semicolon`, previous.strValue, previous.index, previous.line, previous.column), previous);
        }
        const strValue = this._extractSourceContent(start, end);
        const startToken = tokens[0];
        const endToken = tokens[tokens.length - 1];
        const span = this._generateSourceSpan(startToken, endToken);
        return new CssStyleValueAst(span, tokens, strValue);
    }
    /** @internal */
    _collectUntilDelim(delimiters, assertType = null) {
        const tokens = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            const val = assertType != null ? this._consume(assertType) : this._scan();
            tokens.push(val);
        }
        return tokens;
    }
    /** @internal */
    _parseBlock(delimiters) {
        delimiters |= RBRACE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.BLOCK);
        const startToken = this._consume(CssTokenType.Character, '{');
        this._scanner.consumeEmptyStatements();
        const results = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            results.push(this._parseRule(delimiters));
        }
        const endToken = this._consume(CssTokenType.Character, '}');
        this._scanner.setMode(CssLexerMode.BLOCK);
        this._scanner.consumeEmptyStatements();
        const span = this._generateSourceSpan(startToken, endToken);
        return new CssBlockAst(span, results);
    }
    /** @internal */
    _parseStyleBlock(delimiters) {
        delimiters |= RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.STYLE_BLOCK);
        const startToken = this._consume(CssTokenType.Character, '{');
        if (startToken.numValue != chars.$LBRACE) {
            return null;
        }
        const definitions = [];
        this._scanner.consumeEmptyStatements();
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            definitions.push(this._parseDefinition(delimiters));
            this._scanner.consumeEmptyStatements();
        }
        const endToken = this._consume(CssTokenType.Character, '}');
        this._scanner.setMode(CssLexerMode.STYLE_BLOCK);
        this._scanner.consumeEmptyStatements();
        const span = this._generateSourceSpan(startToken, endToken);
        return new CssStylesBlockAst(span, definitions);
    }
    /** @internal */
    _parseDefinition(delimiters) {
        this._scanner.setMode(CssLexerMode.STYLE_BLOCK);
        let prop = this._consume(CssTokenType.Identifier);
        let parseValue = false;
        let value = null;
        let endToken = prop;
        // the colon value separates the prop from the style.
        // there are a few cases as to what could happen if it
        // is missing
        switch (this._scanner.peek) {
            case chars.$SEMICOLON:
            case chars.$RBRACE:
            case chars.$EOF:
                parseValue = false;
                break;
            default:
                let propStr = [prop.strValue];
                if (this._scanner.peek != chars.$COLON) {
                    // this will throw the error
                    const nextValue = this._consume(CssTokenType.Character, ':');
                    propStr.push(nextValue.strValue);
                    const remainingTokens = this._collectUntilDelim(delimiters | COLON_DELIM_FLAG | SEMICOLON_DELIM_FLAG, CssTokenType.Identifier);
                    if (remainingTokens.length > 0) {
                        remainingTokens.forEach((token) => { propStr.push(token.strValue); });
                    }
                    endToken = prop =
                        new CssToken(prop.index, prop.column, prop.line, prop.type, propStr.join(' '));
                }
                // this means we've reached the end of the definition and/or block
                if (this._scanner.peek == chars.$COLON) {
                    this._consume(CssTokenType.Character, ':');
                    parseValue = true;
                }
                break;
        }
        if (parseValue) {
            value = this._parseValue(delimiters);
            endToken = value;
        }
        else {
            this._error(generateErrorMessage(this._getSourceContent(), `The CSS property was not paired with a style value`, prop.strValue, prop.index, prop.line, prop.column), prop);
        }
        const span = this._generateSourceSpan(prop, endToken);
        return new CssDefinitionAst(span, prop, value);
    }
    /** @internal */
    _assertCondition(status, errorMessage, problemToken) {
        if (!status) {
            this._error(errorMessage, problemToken);
            return true;
        }
        return false;
    }
    /** @internal */
    _error(message, problemToken) {
        const length = problemToken.strValue.length;
        const error = CssParseError.create(this._file, 0, problemToken.line, problemToken.column, length, message);
        this._errors.push(error);
    }
}
export class CssParseError extends ParseError {
    static create(file, offset, line, col, length, errMsg) {
        const start = new ParseLocation(file, offset, line, col);
        const end = new ParseLocation(file, offset, line, col + length);
        const span = new ParseSourceSpan(start, end);
        return new CssParseError(span, 'CSS Parse Error: ' + errMsg);
    }
    constructor(span, message) { super(span, message); }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NzX3BhcnNlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9jc3NfcGFyc2VyL2Nzc19wYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBRUgsT0FBTyxLQUFLLEtBQUssTUFBTSxVQUFVLENBQUM7QUFDbEMsT0FBTyxFQUFDLFVBQVUsRUFBRSxhQUFhLEVBQUUsZUFBZSxFQUFFLGVBQWUsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUUxRixPQUFPLEVBQUMsU0FBUyxFQUFFLE1BQU0sRUFBRSxxQkFBcUIsRUFBRSxXQUFXLEVBQUUseUJBQXlCLEVBQUUsZUFBZSxFQUFFLGdCQUFnQixFQUFFLGdCQUFnQixFQUFFLHdCQUF3QixFQUFFLGtCQUFrQixFQUFFLG9CQUFvQixFQUFFLG9CQUFvQixFQUFjLGNBQWMsRUFBRSxrQkFBa0IsRUFBRSxvQkFBb0IsRUFBRSxnQkFBZ0IsRUFBRSxnQkFBZ0IsRUFBRSxpQkFBaUIsRUFBRSxpQkFBaUIsRUFBRSxzQkFBc0IsRUFBRSxXQUFXLEVBQUMsTUFBTSxXQUFXLENBQUM7QUFDL2EsT0FBTyxFQUFDLFFBQVEsRUFBRSxZQUFZLEVBQWMsUUFBUSxFQUFFLFlBQVksRUFBRSxvQkFBb0IsRUFBRSxhQUFhLEVBQUUsU0FBUyxFQUFDLE1BQU0sYUFBYSxDQUFDO0FBRXZJLE1BQU0sY0FBYyxHQUFHLEdBQUcsQ0FBQztBQUUzQixPQUFPLEVBQUMsUUFBUSxFQUFDLE1BQU0sYUFBYSxDQUFDO0FBQ3JDLE9BQU8sRUFBQyxTQUFTLEVBQUMsTUFBTSxXQUFXLENBQUM7QUFFcEMsTUFBTSxlQUFlLEdBQUcsR0FBRyxDQUFDO0FBQzVCLE1BQU0sWUFBWSxHQUFHLEdBQUcsQ0FBQztBQUN6QixNQUFNLHNCQUFzQixHQUFHLEtBQUssQ0FBQztBQUNyQyxNQUFNLGlCQUFpQixHQUFHLFFBQVEsQ0FBQztBQUVuQyxN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QUMsS0FBSyxDQUFDLENBQUM7SUFDM0IsQ0FBQztDQUNGO0FBRUQsTUFBTSxPQUFPLGFBQWMsU0FBUSxVQUFVO0lBQzNDLE1BQU0sQ0FBQyxNQUFNLENBQ1QsSUFBcUIsRUFBRSxNQUFjLEVBQUUsSUFBWSxFQUFFLEdBQVcsRUFBRSxNQUFjLEVBQ2hGLE1BQWM7UUFDaEIsTUFBTSxLQUFLLEdBQUcsSUFBSSxhQUFhLENBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxJQUFJLEVBQUUsR0FBRyxDQUFDLENBQUM7UUFDekQsTUFBTSxHQUFHLEdBQUcsSUFBSSxhQUFhLENBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxJQUFJLEVBQUUsR0FBRyxHQUFHLE1BQU0sQ0FBQyxDQUFDO1FBQ2hFLE1BQU0sSUFBSSxHQUFHLElBQUksZUFBZSxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsQ0FBQztRQUM3QyxPQUFPLElBQUksYUFBYSxDQUFDLElBQUksRUFBRSxtQkFBbUIsR0FBRyxNQUFNLENBQUMsQ0FBQztJQUMvRCxDQUFDO0lBRUQsWUFBWSxJQUFxQixFQUFFLE9BQWUsSUFBSSxLQUFLLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQyxDQUFDLENBQUMsQ0FBQztDQUM5RSIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0ICogYXMgY2hhcnMgZnJvbSAnLi4vY2hhcnMnO1xuaW1wb3J0IHtQYXJzZUVycm9yLCBQYXJzZUxvY2F0aW9uLCBQYXJzZVNvdXJjZUZpbGUsIFBhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5cbmltcG9ydCB7QmxvY2tUeXBlLCBDc3NBc3QsIENzc0F0UnVsZVByZWRpY2F0ZUFzdCwgQ3NzQmxvY2tBc3QsIENzc0Jsb2NrRGVmaW5pdGlvblJ1bGVBc3QsIENzc0Jsb2NrUnVsZUFzdCwgQ3NzRGVmaW5pdGlvbkFzdCwgQ3NzSW5saW5lUnVsZUFzdCwgQ3NzS2V5ZnJhbWVEZWZpbml0aW9uQXN0LCBDc3NLZXlmcmFtZVJ1bGVBc3QsIENzc01lZGlhUXVlcnlSdWxlQXN0LCBDc3NQc2V1ZG9TZWxlY3RvckFzdCwgQ3NzUnVsZUFzdCwgQ3NzU2VsZWN0b3JBc3QsIENzc1NlbGVjdG9yUnVsZUFzdCwgQ3NzU2ltcGxlU2VsZWN0b3JBc3QsIENzc1N0eWxlU2hlZXRBc3QsIENzc1N0eWxlVmFsdWVBc3QsIENzc1N0eWxlc0Jsb2NrQXN0LCBDc3NVbmtub3duUnVsZUFzdCwgQ3NzVW5rbm93blRva2VuTGlzdEFzdCwgbWVyZ2VUb2tlbnN9IGZyb20gJy4vY3NzX2FzdCc7XG5pbXBvcnQge0Nzc0xleGVyLCBDc3NMZXhlck1vZGUsIENzc1NjYW5uZXIsIENzc1Rva2VuLCBDc3NUb2tlblR5cGUsIGdlbmVyYXRlRXJyb3JNZXNzYWdlLCBnZXRSYXdNZXNzYWdlLCBpc05ld2xpbmV9IGZyb20gJy4vY3NzX2xleGVyJztcblxuY29uc3QgU1BBQ0VfT1BFUkFUT1IgPSAnICc7XG5cbmV4cG9ydCB7Q3NzVG9rZW59IGZyb20gJy4vY3NzX2xleGVyJztcbmV4cG9ydCB7QmxvY2tUeXBlfSBmcm9tICcuL2Nzc19hc3QnO1xuXG5jb25zdCBTTEFTSF9DSEFSQUNURVIgPSAnLyc7XG5jb25zdCBHVF9DSEFSQUNURVIgPSAnPic7XG5jb25zdCBUUklQTEVfR1RfT1BFUkFUT1JfU1RSID0gJz4+Pic7XG5jb25zdCBERUVQX09QRVJBVE9SX1NUUiA9ICcvZGVlcC8nO1xuXG5jb25zdCBFT0ZfREVMSU1fRkxBRyA9IDE7XG5jb25zdCBSQlJBQ0VfREVMSU1fRkxBRyA9IDI7XG5jb25zdCBMQlJBQ0VfREVMSU1fRkxBRyA9IDQ7XG5jb25zdCBDT01NQV9ERUxJTV9GTEFHID0gODtcbmNvbnN0IENPTE9OX0RFTElNX0ZMQUcgPSAxNjtcbmNvbnN0IFNFTUlDT0xPTl9ERUxJTV9GTEFHID0gMzI7XG5jb25zdCBORVdMSU5FX0RFTElNX0ZMQUcgPSA2NDtcbmNvbnN0IFJQQVJFTl9ERUxJTV9GTEFHID0gMTI4O1xuY29uc3QgTFBBUkVOX0RFTElNX0ZMQUcgPSAyNTY7XG5jb25zdCBTUEFDRV9ERUxJTV9GTEFHID0gNTEyO1xuXG5mdW5jdGlvbiBfcHNldWRvU2VsZWN0b3JTdXBwb3J0c0lubmVyU2VsZWN0b3JzKG5hbWU6IHN0cmluZyk6IGJvb2xlYW4ge1xuICByZXR1cm4gWydub3QnLCAnaG9zdCcsICdob3N0LWNvbnRleHQnXS5pbmRleE9mKG5hbWUpID49IDA7XG59XG5cbmZ1bmN0aW9uIGlzU2VsZWN0b3JPcGVyYXRvckNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kU0xBU0g6XG4gICAgY2FzZSBjaGFycy4kVElMREE6XG4gICAgY2FzZSBjaGFycy4kUExVUzpcbiAgICBjYXNlIGNoYXJzLiRHVDpcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gY2hhcnMuaXNXaGl0ZXNwYWNlKGNvZGUpO1xuICB9XG59XG5cbmZ1bmN0aW9uIGdldERlbGltRnJvbUNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBudW1iZXIge1xuICBzd2l0Y2ggKGNvZGUpIHtcbiAgICBjYXNlIGNoYXJzLiRFT0Y6XG4gICAgICByZXR1cm4gRU9GX0RFTElNX0ZMQUc7XG4gICAgY2FzZSBjaGFycy4kQ09NTUE6XG4gICAgICByZXR1cm4gQ09NTUFfREVMSU1fRkxBRztcbiAgICBjYXNlIGNoYXJzLiRDT0xPTjpcbiAgICAgIHJldHVybiBDT0xPTl9ERUxJTV9GTEFHO1xuICAgIGNhc2UgY2hhcnMuJFNFTUlDT0xPTjpcbiAgICAgIHJldHVybiBTRU1JQ09MT05fREVMSU1fRkxBRztcbiAgICBjYXNlIGNoYXJzLiRSQlJBQ0U6XG4gICAgICByZXR1cm4gUkJSQUNFX0RFTElNX0ZMQUc7XG4gICAgY2FzZSBjaGFycy4kTEJSQUNFOlxuICAgICAgcmV0dXJuIExCUkFDRV9ERUxJTV9GTEFHO1xuICAgIGNhc2UgY2hhcnMuJFJQQVJFTjpcbiAgICAgIHJldHVybiBSUEFSRU5fREVMSU1fRkxBRztcbiAgICBjYXNlIGNoYXJzLiRTUEFDRTpcbiAgICBjYXNlIGNoYXJzLiRUQUI6XG4gICAgICByZXR1cm4gU1BBQ0VfREVMSU1fRkxBRztcbiAgICBkZWZhdWx0OlxuICAgICAgcmV0dXJuIGlzTmV3bGluZShjb2RlKSA/IE5FV0xJTkVfREVMSU1fRkxBRyA6IDA7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIoY29kZTogbnVtYmVyLCBkZWxpbWl0ZXJzOiBudW1iZXIpOiBib29sZWFuIHtcbiAgcmV0dXJuIChnZXREZWxpbUZyb21DaGFyYWN0ZXIoY29kZSkgJiBkZWxpbWl0ZXJzKSA+IDA7XG59XG5cbmV4cG9ydCBjbGFzcyBQYXJzZWRDc3NSZXN1bHQge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgZXJyb3JzOiBDc3NQYXJzZUVycm9yW10sIHB1YmxpYyBhc3Q6IENzc1N0eWxlU2hlZXRBc3QpIHt9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NQYXJzZXIge1xuICBwcml2YXRlIF9lcnJvcnM6IENzc1BhcnNlRXJyb3JbXSA9IFtdO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfZmlsZSAhOiBQYXJzZVNvdXJjZUZpbGU7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9zY2FubmVyICE6IENzc1NjYW5uZXI7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9sYXN0VG9rZW4gITogQ3NzVG9rZW47XG5cbiAgLyoqXG4gICAqIEBwYXJhbSBjc3MgdGhlIENTUyBjb2RlIHRoYXQgd2lsbCBiZSBwYXJzZWRcbiAgICogQHBhcmFtIHVybCB0aGUgbmFtZSBvZiB0aGUgQ1NTIGZpbGUgY29udGFpbmluZyB0aGUgQ1NTIHNvdXJjZSBjb2RlXG4gICAqL1xuICBwYXJzZShjc3M6IHN0cmluZywgdXJsOiBzdHJpbmcpOiBQYXJzZWRDc3NSZXN1bHQge1xuICAgIGNvbnN0IGxleGVyID0gbmV3IENzc0xleGVyKCk7XG4gICAgdGhpcy5fZmlsZSA9IG5ldyBQYXJzZVNvdXJjZUZpbGUoY3NzLCB1cmwpO1xuICAgIHRoaXMuX3NjYW5uZXIgPSBsZXhlci5zY2FuKGNzcywgZmFsc2UpO1xuXG4gICAgY29uc3QgYXN0ID0gdGhpcy5fcGFyc2VTdHlsZVNoZWV0KEVPRl9ERUxJTV9GTEFHKTtcblxuICAgIGNvbnN0IGVycm9ycyA9IHRoaXMuX2Vycm9ycztcbiAgICB0aGlzLl9lcnJvcnMgPSBbXTtcblxuICAgIGNvbnN0IHJlc3VsdCA9IG5ldyBQYXJzZWRDc3NSZXN1bHQoZXJyb3JzLCBhc3QpO1xuICAgIHRoaXMuX2ZpbGUgPSBudWxsIGFzIGFueTtcbiAgICB0aGlzLl9zY2FubmVyID0gbnVsbCBhcyBhbnk7XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlU3R5bGVTaGVldChkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NTdHlsZVNoZWV0QXN0IHtcbiAgICBjb25zdCByZXN1bHRzOiBDc3NSdWxlQXN0W10gPSBbXTtcbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcbiAgICB3aGlsZSAodGhpcy5fc2Nhbm5lci5wZWVrICE9IGNoYXJzLiRFT0YpIHtcbiAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQkxPQ0spO1xuICAgICAgcmVzdWx0cy5wdXNoKHRoaXMuX3BhcnNlUnVsZShkZWxpbWl0ZXJzKSk7XG4gICAgfVxuICAgIGxldCBzcGFuOiBQYXJzZVNvdXJjZVNwYW58bnVsbCA9IG51bGw7XG4gICAgaWYgKHJlc3VsdHMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgZmlyc3RSdWxlID0gcmVzdWx0c1swXTtcbiAgICAgIC8vIHdlIGNvbGxlY3QgdGhlIGxhc3QgdG9rZW4gbGlrZSBzbyBpbmNhc2UgdGhlcmUgd2FzIGFuXG4gICAgICAvLyBFT0YgdG9rZW4gdGhhdCB3YXMgZW1pdHRlZCBzb21ldGltZSBkdXJpbmcgdGhlIGxleGluZ1xuICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihmaXJzdFJ1bGUsIHRoaXMuX2xhc3RUb2tlbik7XG4gICAgfVxuICAgIHJldHVybiBuZXcgQ3NzU3R5bGVTaGVldEFzdChzcGFuICEsIHJlc3VsdHMpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfZ2V0U291cmNlQ29udGVudCgpOiBzdHJpbmcgeyByZXR1cm4gdGhpcy5fc2Nhbm5lciAhPSBudWxsID8gdGhpcy5fc2Nhbm5lci5pbnB1dCA6ICcnOyB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQ6IG51bWJlciwgZW5kOiBudW1iZXIpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLl9nZXRTb3VyY2VDb250ZW50KCkuc3Vic3RyaW5nKHN0YXJ0LCBlbmQgKyAxKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2dlbmVyYXRlU291cmNlU3BhbihzdGFydDogQ3NzVG9rZW58Q3NzQXN0LCBlbmQ6IENzc1Rva2VufENzc0FzdHxudWxsID0gbnVsbCk6IFBhcnNlU291cmNlU3BhbiB7XG4gICAgbGV0IHN0YXJ0TG9jOiBQYXJzZUxvY2F0aW9uO1xuICAgIGlmIChzdGFydCBpbnN0YW5jZW9mIENzc0FzdCkge1xuICAgICAgc3RhcnRMb2MgPSBzdGFydC5sb2NhdGlvbi5zdGFydDtcbiAgICB9IGVsc2Uge1xuICAgICAgbGV0IHRva2VuID0gc3RhcnQ7XG4gICAgICBpZiAodG9rZW4gPT0gbnVsbCkge1xuICAgICAgICAvLyB0aGUgZGF0YSBoZXJlIGlzIGludmFsaWQsIGhvd2V2ZXIsIGlmIGFuZCB3aGVuIHRoaXMgZG9lc1xuICAgICAgICAvLyBvY2N1ciwgYW55IG90aGVyIGVycm9ycyBhc3NvY2lhdGVkIHdpdGggdGhpcyB3aWxsIGJlIGNvbGxlY3RlZFxuICAgICAgICB0b2tlbiA9IHRoaXMuX2xhc3RUb2tlbjtcbiAgICAgIH1cbiAgICAgIHN0YXJ0TG9jID0gbmV3IFBhcnNlTG9jYXRpb24odGhpcy5fZmlsZSwgdG9rZW4uaW5kZXgsIHRva2VuLmxpbmUsIHRva2VuLmNvbHVtbik7XG4gICAgfVxuXG4gICAgaWYgKGVuZCA9PSBudWxsKSB7XG4gICAgICBlbmQgPSB0aGlzLl9sYXN0VG9rZW47XG4gICAgfVxuXG4gICAgbGV0IGVuZExpbmU6IG51bWJlciA9IC0xO1xuICAgIGxldCBlbmRDb2x1bW46IG51bWJlciA9IC0xO1xuICAgIGxldCBlbmRJbmRleDogbnVtYmVyID0gLTE7XG4gICAgaWYgKGVuZCBpbnN0YW5jZW9mIENzc0FzdCkge1xuICAgICAgZW5kTGluZSA9IGVuZC5sb2NhdGlvbi5lbmQubGluZSAhO1xuICAgICAgZW5kQ29sdW1uID0gZW5kLmxvY2F0aW9uLmVuZC5jb2wgITtcbiAgICAgIGVuZEluZGV4ID0gZW5kLmxvY2F0aW9uLmVuZC5vZmZzZXQgITtcbiAgICB9IGVsc2UgaWYgKGVuZCBpbnN0YW5jZW9mIENzc1Rva2VuKSB7XG4gICAgICBlbmRMaW5lID0gZW5kLmxpbmU7XG4gICAgICBlbmRDb2x1bW4gPSBlbmQuY29sdW1uO1xuICAgICAgZW5kSW5kZXggPSBlbmQuaW5kZXg7XG4gICAgfVxuXG4gICAgY29uc3QgZW5kTG9jID0gbmV3IFBhcnNlTG9jYXRpb24odGhpcy5fZmlsZSwgZW5kSW5kZXgsIGVuZExpbmUsIGVuZENvbHVtbik7XG4gICAgcmV0dXJuIG5ldyBQYXJzZVNvdXJjZVNwYW4oc3RhcnRMb2MsIGVuZExvYyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9yZXNvbHZlQmxvY2tUeXBlKHRva2VuOiBDc3NUb2tlbik6IEJsb2NrVHlwZSB7XG4gICAgc3dpdGNoICh0b2tlbi5zdHJWYWx1ZSkge1xuICAgICAgY2FzZSAnQC1vLWtleWZyYW1lcyc6XG4gICAgICBjYXNlICdALW1vei1rZXlmcmFtZXMnOlxuICAgICAgY2FzZSAnQC13ZWJraXQta2V5ZnJhbWVzJzpcbiAgICAgIGNhc2UgJ0BrZXlmcmFtZXMnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLktleWZyYW1lcztcblxuICAgICAgY2FzZSAnQGNoYXJzZXQnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLkNoYXJzZXQ7XG5cbiAgICAgIGNhc2UgJ0BpbXBvcnQnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLkltcG9ydDtcblxuICAgICAgY2FzZSAnQG5hbWVzcGFjZSc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuTmFtZXNwYWNlO1xuXG4gICAgICBjYXNlICdAcGFnZSc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuUGFnZTtcblxuICAgICAgY2FzZSAnQGRvY3VtZW50JzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5Eb2N1bWVudDtcblxuICAgICAgY2FzZSAnQG1lZGlhJzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5NZWRpYVF1ZXJ5O1xuXG4gICAgICBjYXNlICdAZm9udC1mYWNlJzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5Gb250RmFjZTtcblxuICAgICAgY2FzZSAnQHZpZXdwb3J0JzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5WaWV3cG9ydDtcblxuICAgICAgY2FzZSAnQHN1cHBvcnRzJzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5TdXBwb3J0cztcblxuICAgICAgZGVmYXVsdDpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5VbnN1cHBvcnRlZDtcbiAgICB9XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVJ1bGUoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzUnVsZUFzdCB7XG4gICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayA9PSBjaGFycy4kQVQpIHtcbiAgICAgIHJldHVybiB0aGlzLl9wYXJzZUF0UnVsZShkZWxpbWl0ZXJzKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX3BhcnNlU2VsZWN0b3JSdWxlKGRlbGltaXRlcnMpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VBdFJ1bGUoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzUnVsZUFzdCB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKTtcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQkxPQ0spO1xuICAgIGNvbnN0IHRva2VuID0gdGhpcy5fc2NhbigpO1xuICAgIGNvbnN0IHN0YXJ0VG9rZW4gPSB0b2tlbjtcblxuICAgIHRoaXMuX2Fzc2VydENvbmRpdGlvbihcbiAgICAgICAgdG9rZW4udHlwZSA9PSBDc3NUb2tlblR5cGUuQXRLZXl3b3JkLFxuICAgICAgICBgVGhlIENTUyBSdWxlICR7dG9rZW4uc3RyVmFsdWV9IGlzIG5vdCBhIHZhbGlkIFtAXSBydWxlLmAsIHRva2VuKTtcblxuICAgIGxldCBibG9jazogQ3NzQmxvY2tBc3Q7XG4gICAgY29uc3QgdHlwZSA9IHRoaXMuX3Jlc29sdmVCbG9ja1R5cGUodG9rZW4pO1xuICAgIGxldCBzcGFuOiBQYXJzZVNvdXJjZVNwYW47XG4gICAgbGV0IHRva2VuczogQ3NzVG9rZW5bXTtcbiAgICBsZXQgZW5kVG9rZW46IENzc1Rva2VuO1xuICAgIGxldCBlbmQ6IG51bWJlcjtcbiAgICBsZXQgc3RyVmFsdWU6IHN0cmluZztcbiAgICBsZXQgcXVlcnk6IENzc0F0UnVsZVByZWRpY2F0ZUFzdDtcbiAgICBzd2l0Y2ggKHR5cGUpIHtcbiAgICAgIGNhc2UgQmxvY2tUeXBlLkNoYXJzZXQ6XG4gICAgICBjYXNlIEJsb2NrVHlwZS5OYW1lc3BhY2U6XG4gICAgICBjYXNlIEJsb2NrVHlwZS5JbXBvcnQ6XG4gICAgICAgIGxldCB2YWx1ZSA9IHRoaXMuX3BhcnNlVmFsdWUoZGVsaW1pdGVycyk7XG4gICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQkxPQ0spO1xuICAgICAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCB2YWx1ZSk7XG4gICAgICAgIHJldHVybiBuZXcgQ3NzSW5saW5lUnVsZUFzdChzcGFuLCB0eXBlLCB2YWx1ZSk7XG5cbiAgICAgIGNhc2UgQmxvY2tUeXBlLlZpZXdwb3J0OlxuICAgICAgY2FzZSBCbG9ja1R5cGUuRm9udEZhY2U6XG4gICAgICAgIGJsb2NrID0gdGhpcy5fcGFyc2VTdHlsZUJsb2NrKGRlbGltaXRlcnMpICE7XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgYmxvY2spO1xuICAgICAgICByZXR1cm4gbmV3IENzc0Jsb2NrUnVsZUFzdChzcGFuLCB0eXBlLCBibG9jayk7XG5cbiAgICAgIGNhc2UgQmxvY2tUeXBlLktleWZyYW1lczpcbiAgICAgICAgdG9rZW5zID0gdGhpcy5fY29sbGVjdFVudGlsRGVsaW0oZGVsaW1pdGVycyB8IFJCUkFDRV9ERUxJTV9GTEFHIHwgTEJSQUNFX0RFTElNX0ZMQUcpO1xuICAgICAgICAvLyBrZXlmcmFtZXMgb25seSBoYXZlIG9uZSBpZGVudGlmaWVyIG5hbWVcbiAgICAgICAgbGV0IG5hbWUgPSB0b2tlbnNbMF07XG4gICAgICAgIGJsb2NrID0gdGhpcy5fcGFyc2VLZXlmcmFtZUJsb2NrKGRlbGltaXRlcnMpO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGJsb2NrKTtcbiAgICAgICAgcmV0dXJuIG5ldyBDc3NLZXlmcmFtZVJ1bGVBc3Qoc3BhbiwgbmFtZSwgYmxvY2spO1xuXG4gICAgICBjYXNlIEJsb2NrVHlwZS5NZWRpYVF1ZXJ5OlxuICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLk1FRElBX1FVRVJZKTtcbiAgICAgICAgdG9rZW5zID0gdGhpcy5fY29sbGVjdFVudGlsRGVsaW0oZGVsaW1pdGVycyB8IFJCUkFDRV9ERUxJTV9GTEFHIHwgTEJSQUNFX0RFTElNX0ZMQUcpO1xuICAgICAgICBlbmRUb2tlbiA9IHRva2Vuc1t0b2tlbnMubGVuZ3RoIC0gMV07XG4gICAgICAgIC8vIHdlIGRvIG5vdCB0cmFjayB0aGUgd2hpdGVzcGFjZSBhZnRlciB0aGUgbWVkaWFRdWVyeSBwcmVkaWNhdGUgZW5kc1xuICAgICAgICAvLyBzbyB3ZSBoYXZlIHRvIGNhbGN1bGF0ZSB0aGUgZW5kIHN0cmluZyB2YWx1ZSBvbiBvdXIgb3duXG4gICAgICAgIGVuZCA9IGVuZFRva2VuLmluZGV4ICsgZW5kVG9rZW4uc3RyVmFsdWUubGVuZ3RoIC0gMTtcbiAgICAgICAgc3RyVmFsdWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgZW5kKTtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBlbmRUb2tlbik7XG4gICAgICAgIHF1ZXJ5ID0gbmV3IENzc0F0UnVsZVByZWRpY2F0ZUFzdChzcGFuLCBzdHJWYWx1ZSwgdG9rZW5zKTtcbiAgICAgICAgYmxvY2sgPSB0aGlzLl9wYXJzZUJsb2NrKGRlbGltaXRlcnMpO1xuICAgICAgICBzdHJWYWx1ZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCB0aGlzLl9nZXRTY2FubmVySW5kZXgoKSAtIDEpO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGJsb2NrKTtcbiAgICAgICAgcmV0dXJuIG5ldyBDc3NNZWRpYVF1ZXJ5UnVsZUFzdChzcGFuLCBzdHJWYWx1ZSwgcXVlcnksIGJsb2NrKTtcblxuICAgICAgY2FzZSBCbG9ja1R5cGUuRG9jdW1lbnQ6XG4gICAgICBjYXNlIEJsb2NrVHlwZS5TdXBwb3J0czpcbiAgICAgIGNhc2UgQmxvY2tUeXBlLlBhZ2U6XG4gICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQVRfUlVMRV9RVUVSWSk7XG4gICAgICAgIHRva2VucyA9IHRoaXMuX2NvbGxlY3RVbnRpbERlbGltKGRlbGltaXRlcnMgfCBSQlJBQ0VfREVMSU1fRkxBRyB8IExCUkFDRV9ERUxJTV9GTEFHKTtcbiAgICAgICAgZW5kVG9rZW4gPSB0b2tlbnNbdG9rZW5zLmxlbmd0aCAtIDFdO1xuICAgICAgICAvLyB3ZSBkbyBub3QgdHJhY2sgdGhlIHdoaXRlc3BhY2UgYWZ0ZXIgdGhpcyBibG9jayBydWxlIHByZWRpY2F0ZSBlbmRzXG4gICAgICAgIC8vIHNvIHdlIGhhdmUgdG8gY2FsY3VsYXRlIHRoZSBlbmQgc3RyaW5nIHZhbHVlIG9uIG91ciBvd25cbiAgICAgICAgZW5kID0gZW5kVG9rZW4uaW5kZXggKyBlbmRUb2tlbi5zdHJWYWx1ZS5sZW5ndGggLSAxO1xuICAgICAgICBzdHJWYWx1ZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCBlbmQpO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIHRva2Vuc1t0b2tlbnMubGVuZ3RoIC0gMV0pO1xuICAgICAgICBxdWVyeSA9IG5ldyBDc3NBdFJ1bGVQcmVkaWNhdGVBc3Qoc3Bhbiwgc3RyVmFsdWUsIHRva2Vucyk7XG4gICAgICAgIGJsb2NrID0gdGhpcy5fcGFyc2VCbG9jayhkZWxpbWl0ZXJzKTtcbiAgICAgICAgc3RyVmFsdWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgYmxvY2suZW5kLm9mZnNldCAhKTtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBibG9jayk7XG4gICAgICAgIHJldHVybiBuZXcgQ3NzQmxvY2tEZWZpbml0aW9uUnVsZUFzdChzcGFuLCBzdHJWYWx1ZSwgdHlwZSwgcXVlcnksIGJsb2NrKTtcblxuICAgICAgLy8gaWYgYSBjdXN0b20gQHJ1bGUgeyAuLi4gfSBpcyB1c2VkIGl0IHNob3VsZCBzdGlsbCB0b2tlbml6ZSB0aGUgaW5zaWRlc1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgbGV0IGxpc3RPZlRva2VuczogQ3NzVG9rZW5bXSA9IFtdO1xuICAgICAgICBsZXQgdG9rZW5OYW1lID0gdG9rZW4uc3RyVmFsdWU7XG4gICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQUxMKTtcbiAgICAgICAgdGhpcy5fZXJyb3IoXG4gICAgICAgICAgICBnZW5lcmF0ZUVycm9yTWVzc2FnZShcbiAgICAgICAgICAgICAgICB0aGlzLl9nZXRTb3VyY2VDb250ZW50KCksXG4gICAgICAgICAgICAgICAgYFRoZSBDU1MgXCJhdFwiIHJ1bGUgXCIke3Rva2VuTmFtZX1cIiBpcyBub3QgYWxsb3dlZCB0byB1c2VkIGhlcmVgLCB0b2tlbi5zdHJWYWx1ZSxcbiAgICAgICAgICAgICAgICB0b2tlbi5pbmRleCwgdG9rZW4ubGluZSwgdG9rZW4uY29sdW1uKSxcbiAgICAgICAgICAgIHRva2VuKTtcblxuICAgICAgICB0aGlzLl9jb2xsZWN0VW50aWxEZWxpbShkZWxpbWl0ZXJzIHwgTEJSQUNFX0RFTElNX0ZMQUcgfCBTRU1JQ09MT05fREVMSU1fRkxBRylcbiAgICAgICAgICAgIC5mb3JFYWNoKCh0b2tlbikgPT4geyBsaXN0T2ZUb2tlbnMucHVzaCh0b2tlbik7IH0pO1xuICAgICAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrID09IGNoYXJzLiRMQlJBQ0UpIHtcbiAgICAgICAgICBsaXN0T2ZUb2tlbnMucHVzaCh0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd7JykpO1xuICAgICAgICAgIHRoaXMuX2NvbGxlY3RVbnRpbERlbGltKGRlbGltaXRlcnMgfCBSQlJBQ0VfREVMSU1fRkxBRyB8IExCUkFDRV9ERUxJTV9GTEFHKVxuICAgICAgICAgICAgICAuZm9yRWFjaCgodG9rZW4pID0+IHsgbGlzdE9mVG9rZW5zLnB1c2godG9rZW4pOyB9KTtcbiAgICAgICAgICBsaXN0T2ZUb2tlbnMucHVzaCh0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd9JykpO1xuICAgICAgICB9XG4gICAgICAgIGVuZFRva2VuID0gbGlzdE9mVG9rZW5zW2xpc3RPZlRva2Vucy5sZW5ndGggLSAxXTtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBlbmRUb2tlbik7XG4gICAgICAgIHJldHVybiBuZXcgQ3NzVW5rbm93blJ1bGVBc3Qoc3BhbiwgdG9rZW5OYW1lLCBsaXN0T2ZUb2tlbnMpO1xuICAgIH1cbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlU2VsZWN0b3JSdWxlKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1J1bGVBc3Qge1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCk7XG4gICAgY29uc3Qgc2VsZWN0b3JzID0gdGhpcy5fcGFyc2VTZWxlY3RvcnMoZGVsaW1pdGVycyk7XG4gICAgY29uc3QgYmxvY2sgPSB0aGlzLl9wYXJzZVN0eWxlQmxvY2soZGVsaW1pdGVycyk7XG4gICAgbGV0IHJ1bGVBc3Q6IENzc1J1bGVBc3Q7XG4gICAgbGV0IHNwYW46IFBhcnNlU291cmNlU3BhbjtcbiAgICBjb25zdCBzdGFydFNlbGVjdG9yID0gc2VsZWN0b3JzWzBdO1xuICAgIGlmIChibG9jayAhPSBudWxsKSB7XG4gICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0U2VsZWN0b3IsIGJsb2NrKTtcbiAgICAgIHJ1bGVBc3QgPSBuZXcgQ3NzU2VsZWN0b3JSdWxlQXN0KHNwYW4sIHNlbGVjdG9ycywgYmxvY2spO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBuYW1lID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIHRoaXMuX2dldFNjYW5uZXJJbmRleCgpIC0gMSk7XG4gICAgICBjb25zdCBpbm5lclRva2VuczogQ3NzVG9rZW5bXSA9IFtdO1xuICAgICAgc2VsZWN0b3JzLmZvckVhY2goKHNlbGVjdG9yOiBDc3NTZWxlY3RvckFzdCkgPT4ge1xuICAgICAgICBzZWxlY3Rvci5zZWxlY3RvclBhcnRzLmZvckVhY2goKHBhcnQ6IENzc1NpbXBsZVNlbGVjdG9yQXN0KSA9PiB7XG4gICAgICAgICAgcGFydC50b2tlbnMuZm9yRWFjaCgodG9rZW46IENzc1Rva2VuKSA9PiB7IGlubmVyVG9rZW5zLnB1c2godG9rZW4pOyB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgICAgIGNvbnN0IGVuZFRva2VuID0gaW5uZXJUb2tlbnNbaW5uZXJUb2tlbnMubGVuZ3RoIC0gMV07XG4gICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0U2VsZWN0b3IsIGVuZFRva2VuKTtcbiAgICAgIHJ1bGVBc3QgPSBuZXcgQ3NzVW5rbm93blRva2VuTGlzdEFzdChzcGFuLCBuYW1lLCBpbm5lclRva2Vucyk7XG4gICAgfVxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQkxPQ0spO1xuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuICAgIHJldHVybiBydWxlQXN0O1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VTZWxlY3RvcnMoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzU2VsZWN0b3JBc3RbXSB7XG4gICAgZGVsaW1pdGVycyB8PSBMQlJBQ0VfREVMSU1fRkxBRyB8IFNFTUlDT0xPTl9ERUxJTV9GTEFHO1xuXG4gICAgY29uc3Qgc2VsZWN0b3JzOiBDc3NTZWxlY3RvckFzdFtdID0gW107XG4gICAgbGV0IGlzUGFyc2luZ1NlbGVjdG9ycyA9IHRydWU7XG4gICAgd2hpbGUgKGlzUGFyc2luZ1NlbGVjdG9ycykge1xuICAgICAgc2VsZWN0b3JzLnB1c2godGhpcy5fcGFyc2VTZWxlY3RvcihkZWxpbWl0ZXJzKSk7XG5cbiAgICAgIGlzUGFyc2luZ1NlbGVjdG9ycyA9ICFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpO1xuXG4gICAgICBpZiAoaXNQYXJzaW5nU2VsZWN0b3JzKSB7XG4gICAgICAgIHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJywnKTtcbiAgICAgICAgaXNQYXJzaW5nU2VsZWN0b3JzID0gIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycyk7XG4gICAgICAgIGlmIChpc1BhcnNpbmdTZWxlY3RvcnMpIHtcbiAgICAgICAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gc2VsZWN0b3JzO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfc2NhbigpOiBDc3NUb2tlbiB7XG4gICAgY29uc3Qgb3V0cHV0ID0gdGhpcy5fc2Nhbm5lci5zY2FuKCkgITtcbiAgICBjb25zdCB0b2tlbiA9IG91dHB1dC50b2tlbjtcbiAgICBjb25zdCBlcnJvciA9IG91dHB1dC5lcnJvcjtcbiAgICBpZiAoZXJyb3IgIT0gbnVsbCkge1xuICAgICAgdGhpcy5fZXJyb3IoZ2V0UmF3TWVzc2FnZShlcnJvciksIHRva2VuKTtcbiAgICB9XG4gICAgdGhpcy5fbGFzdFRva2VuID0gdG9rZW47XG4gICAgcmV0dXJuIHRva2VuO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfZ2V0U2Nhbm5lckluZGV4KCk6IG51bWJlciB7IHJldHVybiB0aGlzLl9zY2FubmVyLmluZGV4OyB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfY29uc3VtZSh0eXBlOiBDc3NUb2tlblR5cGUsIHZhbHVlOiBzdHJpbmd8bnVsbCA9IG51bGwpOiBDc3NUb2tlbiB7XG4gICAgY29uc3Qgb3V0cHV0ID0gdGhpcy5fc2Nhbm5lci5jb25zdW1lKHR5cGUsIHZhbHVlKTtcbiAgICBjb25zdCB0b2tlbiA9IG91dHB1dC50b2tlbjtcbiAgICBjb25zdCBlcnJvciA9IG91dHB1dC5lcnJvcjtcbiAgICBpZiAoZXJyb3IgIT0gbnVsbCkge1xuICAgICAgdGhpcy5fZXJyb3IoZ2V0UmF3TWVzc2FnZShlcnJvciksIHRva2VuKTtcbiAgICB9XG4gICAgdGhpcy5fbGFzdFRva2VuID0gdG9rZW47XG4gICAgcmV0dXJuIHRva2VuO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VLZXlmcmFtZUJsb2NrKGRlbGltaXRlcnM6IG51bWJlcik6IENzc0Jsb2NrQXN0IHtcbiAgICBkZWxpbWl0ZXJzIHw9IFJCUkFDRV9ERUxJTV9GTEFHO1xuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuS0VZRlJBTUVfQkxPQ0spO1xuXG4gICAgY29uc3Qgc3RhcnRUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ3snKTtcblxuICAgIGNvbnN0IGRlZmluaXRpb25zOiBDc3NLZXlmcmFtZURlZmluaXRpb25Bc3RbXSA9IFtdO1xuICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgZGVmaW5pdGlvbnMucHVzaCh0aGlzLl9wYXJzZUtleWZyYW1lRGVmaW5pdGlvbihkZWxpbWl0ZXJzKSk7XG4gICAgfVxuXG4gICAgY29uc3QgZW5kVG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd9Jyk7XG5cbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGVuZFRva2VuKTtcbiAgICByZXR1cm4gbmV3IENzc0Jsb2NrQXN0KHNwYW4sIGRlZmluaXRpb25zKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlS2V5ZnJhbWVEZWZpbml0aW9uKGRlbGltaXRlcnM6IG51bWJlcik6IENzc0tleWZyYW1lRGVmaW5pdGlvbkFzdCB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKTtcbiAgICBjb25zdCBzdGVwVG9rZW5zOiBDc3NUb2tlbltdID0gW107XG4gICAgZGVsaW1pdGVycyB8PSBMQlJBQ0VfREVMSU1fRkxBRztcbiAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIHN0ZXBUb2tlbnMucHVzaCh0aGlzLl9wYXJzZUtleWZyYW1lTGFiZWwoZGVsaW1pdGVycyB8IENPTU1BX0RFTElNX0ZMQUcpKTtcbiAgICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgIT0gY2hhcnMuJExCUkFDRSkge1xuICAgICAgICB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICcsJyk7XG4gICAgICB9XG4gICAgfVxuICAgIGNvbnN0IHN0eWxlc0Jsb2NrID0gdGhpcy5fcGFyc2VTdHlsZUJsb2NrKGRlbGltaXRlcnMgfCBSQlJBQ0VfREVMSU1fRkxBRyk7XG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGVwVG9rZW5zWzBdLCBzdHlsZXNCbG9jayk7XG4gICAgY29uc3QgYXN0ID0gbmV3IENzc0tleWZyYW1lRGVmaW5pdGlvbkFzdChzcGFuLCBzdGVwVG9rZW5zLCBzdHlsZXNCbG9jayAhKTtcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQkxPQ0spO1xuICAgIHJldHVybiBhc3Q7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZUtleWZyYW1lTGFiZWwoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzVG9rZW4ge1xuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuS0VZRlJBTUVfQkxPQ0spO1xuICAgIHJldHVybiBtZXJnZVRva2Vucyh0aGlzLl9jb2xsZWN0VW50aWxEZWxpbShkZWxpbWl0ZXJzKSk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVBzZXVkb1NlbGVjdG9yKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1BzZXVkb1NlbGVjdG9yQXN0IHtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpO1xuXG4gICAgZGVsaW1pdGVycyAmPSB+Q09NTUFfREVMSU1fRkxBRztcblxuICAgIC8vIHdlIGtlZXAgdGhlIG9yaWdpbmFsIHZhbHVlIHNpbmNlIHdlIG1heSB1c2UgaXQgdG8gcmVjdXJzZSB3aGVuIDpub3QsIDpob3N0IGFyZSB1c2VkXG4gICAgY29uc3Qgc3RhcnRpbmdEZWxpbXMgPSBkZWxpbWl0ZXJzO1xuXG4gICAgY29uc3Qgc3RhcnRUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJzonKTtcbiAgICBjb25zdCB0b2tlbnMgPSBbc3RhcnRUb2tlbl07XG5cbiAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrID09IGNoYXJzLiRDT0xPTikgeyAgLy8gOjpzb21ldGhpbmdcbiAgICAgIHRva2Vucy5wdXNoKHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJzonKSk7XG4gICAgfVxuXG4gICAgY29uc3QgaW5uZXJTZWxlY3RvcnM6IENzc1NlbGVjdG9yQXN0W10gPSBbXTtcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuUFNFVURPX1NFTEVDVE9SKTtcblxuICAgIC8vIGhvc3QsIGhvc3QtY29udGV4dCwgbGFuZywgbm90LCBudGgtY2hpbGQgYXJlIGFsbCBpZGVudGlmaWVyc1xuICAgIGNvbnN0IHBzZXVkb1NlbGVjdG9yVG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5JZGVudGlmaWVyKTtcbiAgICBjb25zdCBwc2V1ZG9TZWxlY3Rvck5hbWUgPSBwc2V1ZG9TZWxlY3RvclRva2VuLnN0clZhbHVlO1xuICAgIHRva2Vucy5wdXNoKHBzZXVkb1NlbGVjdG9yVG9rZW4pO1xuXG4gICAgLy8gaG9zdCgpLCBsYW5nKCksIG50aC1jaGlsZCgpLCBldGMuLi5cbiAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrID09IGNoYXJzLiRMUEFSRU4pIHtcbiAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuUFNFVURPX1NFTEVDVE9SX1dJVEhfQVJHVU1FTlRTKTtcblxuICAgICAgY29uc3Qgb3BlblBhcmVuVG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICcoJyk7XG4gICAgICB0b2tlbnMucHVzaChvcGVuUGFyZW5Ub2tlbik7XG5cbiAgICAgIC8vIDpob3N0KGlubmVyU2VsZWN0b3IocykpLCA6bm90KHNlbGVjdG9yKSwgZXRjLi4uXG4gICAgICBpZiAoX3BzZXVkb1NlbGVjdG9yU3VwcG9ydHNJbm5lclNlbGVjdG9ycyhwc2V1ZG9TZWxlY3Rvck5hbWUpKSB7XG4gICAgICAgIGxldCBpbm5lckRlbGltcyA9IHN0YXJ0aW5nRGVsaW1zIHwgTFBBUkVOX0RFTElNX0ZMQUcgfCBSUEFSRU5fREVMSU1fRkxBRztcbiAgICAgICAgaWYgKHBzZXVkb1NlbGVjdG9yTmFtZSA9PSAnbm90Jykge1xuICAgICAgICAgIC8vIHRoZSBpbm5lciBzZWxlY3RvciBpbnNpZGUgb2YgOm5vdCguLi4pIGNhbiBvbmx5IGJlIG9uZVxuICAgICAgICAgIC8vIENTUyBzZWxlY3RvciAobm8gY29tbWFzIGFsbG93ZWQpIC4uLiBUaGlzIGlzIGFjY29yZGluZ1xuICAgICAgICAgIC8vIHRvIHRoZSBDU1Mgc3BlY2lmaWNhdGlvblxuICAgICAgICAgIGlubmVyRGVsaW1zIHw9IENPTU1BX0RFTElNX0ZMQUc7XG4gICAgICAgIH1cblxuICAgICAgICAvLyA6aG9zdChhLCBiLCBjKSB7XG4gICAgICAgIHRoaXMuX3BhcnNlU2VsZWN0b3JzKGlubmVyRGVsaW1zKS5mb3JFYWNoKChzZWxlY3RvciwgaW5kZXgpID0+IHtcbiAgICAgICAgICBpbm5lclNlbGVjdG9ycy5wdXNoKHNlbGVjdG9yKTtcbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICAvLyB0aGlzIGJyYW5jaCBpcyBmb3IgdGhpbmdzIGxpa2UgXCJlbi11cywgMmsgKyAxLCBldGMuLi5cIlxuICAgICAgICAvLyB3aGljaCBhbGwgZW5kIHVwIGluIHBzZXVkb1NlbGVjdG9ycyBsaWtlIDpsYW5nLCA6bnRoLWNoaWxkLCBldGMuLlxuICAgICAgICBjb25zdCBpbm5lclZhbHVlRGVsaW1zID0gZGVsaW1pdGVycyB8IExCUkFDRV9ERUxJTV9GTEFHIHwgQ09MT05fREVMSU1fRkxBRyB8XG4gICAgICAgICAgICBSUEFSRU5fREVMSU1fRkxBRyB8IExQQVJFTl9ERUxJTV9GTEFHO1xuICAgICAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgaW5uZXJWYWx1ZURlbGltcykpIHtcbiAgICAgICAgICBjb25zdCB0b2tlbiA9IHRoaXMuX3NjYW4oKTtcbiAgICAgICAgICB0b2tlbnMucHVzaCh0b2tlbik7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgY29uc3QgY2xvc2VQYXJlblRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnKScpO1xuICAgICAgdG9rZW5zLnB1c2goY2xvc2VQYXJlblRva2VuKTtcbiAgICB9XG5cbiAgICBjb25zdCBlbmQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKSAtIDE7XG4gICAgY29uc3Qgc3RyVmFsdWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgZW5kKTtcblxuICAgIGNvbnN0IGVuZFRva2VuID0gdG9rZW5zW3Rva2Vucy5sZW5ndGggLSAxXTtcbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGVuZFRva2VuKTtcbiAgICByZXR1cm4gbmV3IENzc1BzZXVkb1NlbGVjdG9yQXN0KHNwYW4sIHN0clZhbHVlLCBwc2V1ZG9TZWxlY3Rvck5hbWUsIHRva2VucywgaW5uZXJTZWxlY3RvcnMpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VTaW1wbGVTZWxlY3RvcihkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NTaW1wbGVTZWxlY3RvckFzdCB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKTtcblxuICAgIGRlbGltaXRlcnMgfD0gQ09NTUFfREVMSU1fRkxBRztcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU0VMRUNUT1IpO1xuICAgIGNvbnN0IHNlbGVjdG9yQ3NzVG9rZW5zOiBDc3NUb2tlbltdID0gW107XG4gICAgY29uc3QgcHNldWRvU2VsZWN0b3JzOiBDc3NQc2V1ZG9TZWxlY3RvckFzdFtdID0gW107XG5cbiAgICBsZXQgcHJldmlvdXNUb2tlbjogQ3NzVG9rZW4gPSB1bmRlZmluZWQgITtcblxuICAgIGNvbnN0IHNlbGVjdG9yUGFydERlbGltaXRlcnMgPSBkZWxpbWl0ZXJzIHwgU1BBQ0VfREVMSU1fRkxBRztcbiAgICBsZXQgbG9vcE92ZXJTZWxlY3RvciA9ICFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIHNlbGVjdG9yUGFydERlbGltaXRlcnMpO1xuXG4gICAgbGV0IGhhc0F0dHJpYnV0ZUVycm9yID0gZmFsc2U7XG4gICAgd2hpbGUgKGxvb3BPdmVyU2VsZWN0b3IpIHtcbiAgICAgIGNvbnN0IHBlZWsgPSB0aGlzLl9zY2FubmVyLnBlZWs7XG5cbiAgICAgIHN3aXRjaCAocGVlaykge1xuICAgICAgICBjYXNlIGNoYXJzLiRDT0xPTjpcbiAgICAgICAgICBsZXQgaW5uZXJQc2V1ZG8gPSB0aGlzLl9wYXJzZVBzZXVkb1NlbGVjdG9yKGRlbGltaXRlcnMpO1xuICAgICAgICAgIHBzZXVkb1NlbGVjdG9ycy5wdXNoKGlubmVyUHNldWRvKTtcbiAgICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNFTEVDVE9SKTtcbiAgICAgICAgICBicmVhaztcblxuICAgICAgICBjYXNlIGNoYXJzLiRMQlJBQ0tFVDpcbiAgICAgICAgICAvLyB3ZSBzZXQgdGhlIG1vZGUgYWZ0ZXIgdGhlIHNjYW4gYmVjYXVzZSBhdHRyaWJ1dGUgbW9kZSBkb2VzIG5vdFxuICAgICAgICAgIC8vIGFsbG93IGF0dHJpYnV0ZSBbXSB2YWx1ZXMuIEFuZCB0aGlzIGFsc28gd2lsbCBjYXRjaCBhbnkgZXJyb3JzXG4gICAgICAgICAgLy8gaWYgYW4gZXh0cmEgXCJbXCIgaXMgdXNlZCBpbnNpZGUuXG4gICAgICAgICAgc2VsZWN0b3JDc3NUb2tlbnMucHVzaCh0aGlzLl9zY2FuKCkpO1xuICAgICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQVRUUklCVVRFX1NFTEVDVE9SKTtcbiAgICAgICAgICBicmVhaztcblxuICAgICAgICBjYXNlIGNoYXJzLiRSQlJBQ0tFVDpcbiAgICAgICAgICBpZiAodGhpcy5fc2Nhbm5lci5nZXRNb2RlKCkgIT0gQ3NzTGV4ZXJNb2RlLkFUVFJJQlVURV9TRUxFQ1RPUikge1xuICAgICAgICAgICAgaGFzQXR0cmlidXRlRXJyb3IgPSB0cnVlO1xuICAgICAgICAgIH1cbiAgICAgICAgICAvLyB3ZSBzZXQgdGhlIG1vZGUgZWFybHkgYmVjYXVzZSBhdHRyaWJ1dGUgbW9kZSBkb2VzIG5vdFxuICAgICAgICAgIC8vIGFsbG93IGF0dHJpYnV0ZSBbXSB2YWx1ZXNcbiAgICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNFTEVDVE9SKTtcbiAgICAgICAgICBzZWxlY3RvckNzc1Rva2Vucy5wdXNoKHRoaXMuX3NjYW4oKSk7XG4gICAgICAgICAgYnJlYWs7XG5cbiAgICAgICAgZGVmYXVsdDpcbiAgICAgICAgICBpZiAoaXNTZWxlY3Rvck9wZXJhdG9yQ2hhcmFjdGVyKHBlZWspKSB7XG4gICAgICAgICAgICBsb29wT3ZlclNlbGVjdG9yID0gZmFsc2U7XG4gICAgICAgICAgICBjb250aW51ZTtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBsZXQgdG9rZW4gPSB0aGlzLl9zY2FuKCk7XG4gICAgICAgICAgcHJldmlvdXNUb2tlbiA9IHRva2VuO1xuICAgICAgICAgIHNlbGVjdG9yQ3NzVG9rZW5zLnB1c2godG9rZW4pO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgfVxuXG4gICAgICBsb29wT3ZlclNlbGVjdG9yID0gIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgc2VsZWN0b3JQYXJ0RGVsaW1pdGVycyk7XG4gICAgfVxuXG4gICAgaGFzQXR0cmlidXRlRXJyb3IgPVxuICAgICAgICBoYXNBdHRyaWJ1dGVFcnJvciB8fCB0aGlzLl9zY2FubmVyLmdldE1vZGUoKSA9PSBDc3NMZXhlck1vZGUuQVRUUklCVVRFX1NFTEVDVE9SO1xuICAgIGlmIChoYXNBdHRyaWJ1dGVFcnJvcikge1xuICAgICAgdGhpcy5fZXJyb3IoXG4gICAgICAgICAgYFVuYmFsYW5jZWQgQ1NTIGF0dHJpYnV0ZSBzZWxlY3RvciBhdCBjb2x1bW4gJHtwcmV2aW91c1Rva2VuLmxpbmV9OiR7cHJldmlvdXNUb2tlbi5jb2x1bW59YCxcbiAgICAgICAgICBwcmV2aW91c1Rva2VuKTtcbiAgICB9XG5cbiAgICBsZXQgZW5kID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCkgLSAxO1xuXG4gICAgLy8gdGhpcyBoYXBwZW5zIGlmIHRoZSBzZWxlY3RvciBpcyBub3QgZGlyZWN0bHkgZm9sbG93ZWQgYnlcbiAgICAvLyBhIGNvbW1hIG9yIGN1cmx5IGJyYWNlIHdpdGhvdXQgYSBzcGFjZSBpbiBiZXR3ZWVuXG4gICAgbGV0IG9wZXJhdG9yOiBDc3NUb2tlbnxudWxsID0gbnVsbDtcbiAgICBsZXQgb3BlcmF0b3JTY2FuQ291bnQgPSAwO1xuICAgIGxldCBsYXN0T3BlcmF0b3JUb2tlbjogQ3NzVG9rZW58bnVsbCA9IG51bGw7XG4gICAgaWYgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICB3aGlsZSAob3BlcmF0b3IgPT0gbnVsbCAmJiAhY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSAmJlxuICAgICAgICAgICAgIGlzU2VsZWN0b3JPcGVyYXRvckNoYXJhY3Rlcih0aGlzLl9zY2FubmVyLnBlZWspKSB7XG4gICAgICAgIGxldCB0b2tlbiA9IHRoaXMuX3NjYW4oKTtcbiAgICAgICAgY29uc3QgdG9rZW5PcGVyYXRvciA9IHRva2VuLnN0clZhbHVlO1xuICAgICAgICBvcGVyYXRvclNjYW5Db3VudCsrO1xuICAgICAgICBsYXN0T3BlcmF0b3JUb2tlbiA9IHRva2VuO1xuICAgICAgICBpZiAodG9rZW5PcGVyYXRvciAhPSBTUEFDRV9PUEVSQVRPUikge1xuICAgICAgICAgIHN3aXRjaCAodG9rZW5PcGVyYXRvcikge1xuICAgICAgICAgICAgY2FzZSBTTEFTSF9DSEFSQUNURVI6XG4gICAgICAgICAgICAgIC8vIC9kZWVwLyBvcGVyYXRvclxuICAgICAgICAgICAgICBsZXQgZGVlcFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuSWRlbnRpZmllcik7XG4gICAgICAgICAgICAgIGxldCBkZWVwU2xhc2ggPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIpO1xuICAgICAgICAgICAgICBsZXQgaW5kZXggPSBsYXN0T3BlcmF0b3JUb2tlbi5pbmRleDtcbiAgICAgICAgICAgICAgbGV0IGxpbmUgPSBsYXN0T3BlcmF0b3JUb2tlbi5saW5lO1xuICAgICAgICAgICAgICBsZXQgY29sdW1uID0gbGFzdE9wZXJhdG9yVG9rZW4uY29sdW1uO1xuICAgICAgICAgICAgICBpZiAoZGVlcFRva2VuICE9IG51bGwgJiYgZGVlcFRva2VuLnN0clZhbHVlLnRvTG93ZXJDYXNlKCkgPT0gJ2RlZXAnICYmXG4gICAgICAgICAgICAgICAgICBkZWVwU2xhc2guc3RyVmFsdWUgPT0gU0xBU0hfQ0hBUkFDVEVSKSB7XG4gICAgICAgICAgICAgICAgdG9rZW4gPSBuZXcgQ3NzVG9rZW4oXG4gICAgICAgICAgICAgICAgICAgIGxhc3RPcGVyYXRvclRva2VuLmluZGV4LCBsYXN0T3BlcmF0b3JUb2tlbi5jb2x1bW4sIGxhc3RPcGVyYXRvclRva2VuLmxpbmUsXG4gICAgICAgICAgICAgICAgICAgIENzc1Rva2VuVHlwZS5JZGVudGlmaWVyLCBERUVQX09QRVJBVE9SX1NUUik7XG4gICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgY29uc3QgdGV4dCA9IFNMQVNIX0NIQVJBQ1RFUiArIGRlZXBUb2tlbi5zdHJWYWx1ZSArIGRlZXBTbGFzaC5zdHJWYWx1ZTtcbiAgICAgICAgICAgICAgICB0aGlzLl9lcnJvcihcbiAgICAgICAgICAgICAgICAgICAgZ2VuZXJhdGVFcnJvck1lc3NhZ2UoXG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLl9nZXRTb3VyY2VDb250ZW50KCksIGAke3RleHR9IGlzIGFuIGludmFsaWQgQ1NTIG9wZXJhdG9yYCwgdGV4dCwgaW5kZXgsXG4gICAgICAgICAgICAgICAgICAgICAgICBsaW5lLCBjb2x1bW4pLFxuICAgICAgICAgICAgICAgICAgICBsYXN0T3BlcmF0b3JUb2tlbik7XG4gICAgICAgICAgICAgICAgdG9rZW4gPSBuZXcgQ3NzVG9rZW4oaW5kZXgsIGNvbHVtbiwgbGluZSwgQ3NzVG9rZW5UeXBlLkludmFsaWQsIHRleHQpO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIGJyZWFrO1xuXG4gICAgICAgICAgICBjYXNlIEdUX0NIQVJBQ1RFUjpcbiAgICAgICAgICAgICAgLy8gPj4+IG9wZXJhdG9yXG4gICAgICAgICAgICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgPT0gY2hhcnMuJEdUICYmIHRoaXMuX3NjYW5uZXIucGVla1BlZWsgPT0gY2hhcnMuJEdUKSB7XG4gICAgICAgICAgICAgICAgdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCBHVF9DSEFSQUNURVIpO1xuICAgICAgICAgICAgICAgIHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgR1RfQ0hBUkFDVEVSKTtcbiAgICAgICAgICAgICAgICB0b2tlbiA9IG5ldyBDc3NUb2tlbihcbiAgICAgICAgICAgICAgICAgICAgbGFzdE9wZXJhdG9yVG9rZW4uaW5kZXgsIGxhc3RPcGVyYXRvclRva2VuLmNvbHVtbiwgbGFzdE9wZXJhdG9yVG9rZW4ubGluZSxcbiAgICAgICAgICAgICAgICAgICAgQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIsIFRSSVBMRV9HVF9PUEVSQVRPUl9TVFIpO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIG9wZXJhdG9yID0gdG9rZW47XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgLy8gc28gbG9uZyBhcyB0aGVyZSBpcyBhbiBvcGVyYXRvciB0aGVuIHdlIGNhbiBoYXZlIGFuXG4gICAgICAvLyBlbmRpbmcgdmFsdWUgdGhhdCBpcyBiZXlvbmQgdGhlIHNlbGVjdG9yIHZhbHVlIC4uLlxuICAgICAgLy8gb3RoZXJ3aXNlIGl0J3MganVzdCBhIGJ1bmNoIG9mIHRyYWlsaW5nIHdoaXRlc3BhY2VcbiAgICAgIGlmIChvcGVyYXRvciAhPSBudWxsKSB7XG4gICAgICAgIGVuZCA9IG9wZXJhdG9yLmluZGV4O1xuICAgICAgfVxuICAgIH1cblxuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZVdoaXRlc3BhY2UoKTtcblxuICAgIGNvbnN0IHN0clZhbHVlID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIGVuZCk7XG5cbiAgICAvLyBpZiB3ZSBkbyBjb21lIGFjcm9zcyBvbmUgb3IgbW9yZSBzcGFjZXMgaW5zaWRlIG9mXG4gICAgLy8gdGhlIG9wZXJhdG9ycyBsb29wIHRoZW4gYW4gZW1wdHkgc3BhY2UgaXMgc3RpbGwgYVxuICAgIC8vIHZhbGlkIG9wZXJhdG9yIHRvIHVzZSBpZiBzb21ldGhpbmcgZWxzZSB3YXMgbm90IGZvdW5kXG4gICAgaWYgKG9wZXJhdG9yID09IG51bGwgJiYgb3BlcmF0b3JTY2FuQ291bnQgPiAwICYmIHRoaXMuX3NjYW5uZXIucGVlayAhPSBjaGFycy4kTEJSQUNFKSB7XG4gICAgICBvcGVyYXRvciA9IGxhc3RPcGVyYXRvclRva2VuO1xuICAgIH1cblxuICAgIC8vIHBsZWFzZSBub3RlIHRoYXQgYGVuZFRva2VuYCBpcyByZWFzc2lnbmVkIG11bHRpcGxlIHRpbWVzIGJlbG93XG4gICAgLy8gc28gcGxlYXNlIGRvIG5vdCBvcHRpbWl6ZSB0aGUgaWYgc3RhdGVtZW50cyBpbnRvIGlmL2Vsc2VpZlxuICAgIGxldCBzdGFydFRva2VuT3JBc3Q6IENzc1Rva2VufENzc0FzdHxudWxsID0gbnVsbDtcbiAgICBsZXQgZW5kVG9rZW5PckFzdDogQ3NzVG9rZW58Q3NzQXN0fG51bGwgPSBudWxsO1xuICAgIGlmIChzZWxlY3RvckNzc1Rva2Vucy5sZW5ndGggPiAwKSB7XG4gICAgICBzdGFydFRva2VuT3JBc3QgPSBzdGFydFRva2VuT3JBc3QgfHwgc2VsZWN0b3JDc3NUb2tlbnNbMF07XG4gICAgICBlbmRUb2tlbk9yQXN0ID0gc2VsZWN0b3JDc3NUb2tlbnNbc2VsZWN0b3JDc3NUb2tlbnMubGVuZ3RoIC0gMV07XG4gICAgfVxuICAgIGlmIChwc2V1ZG9TZWxlY3RvcnMubGVuZ3RoID4gMCkge1xuICAgICAgc3RhcnRUb2tlbk9yQXN0ID0gc3RhcnRUb2tlbk9yQXN0IHx8IHBzZXVkb1NlbGVjdG9yc1swXTtcbiAgICAgIGVuZFRva2VuT3JBc3QgPSBwc2V1ZG9TZWxlY3RvcnNbcHNldWRvU2VsZWN0b3JzLmxlbmd0aCAtIDFdO1xuICAgIH1cbiAgICBpZiAob3BlcmF0b3IgIT0gbnVsbCkge1xuICAgICAgc3RhcnRUb2tlbk9yQXN0ID0gc3RhcnRUb2tlbk9yQXN0IHx8IG9wZXJhdG9yO1xuICAgICAgZW5kVG9rZW5PckFzdCA9IG9wZXJhdG9yO1xuICAgIH1cblxuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbk9yQXN0ICEsIGVuZFRva2VuT3JBc3QpO1xuICAgIHJldHVybiBuZXcgQ3NzU2ltcGxlU2VsZWN0b3JBc3Qoc3Bhbiwgc2VsZWN0b3JDc3NUb2tlbnMsIHN0clZhbHVlLCBwc2V1ZG9TZWxlY3RvcnMsIG9wZXJhdG9yICEpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VTZWxlY3RvcihkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NTZWxlY3RvckFzdCB7XG4gICAgZGVsaW1pdGVycyB8PSBDT01NQV9ERUxJTV9GTEFHO1xuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU0VMRUNUT1IpO1xuXG4gICAgY29uc3Qgc2ltcGxlU2VsZWN0b3JzOiBDc3NTaW1wbGVTZWxlY3RvckFzdFtdID0gW107XG4gICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICBzaW1wbGVTZWxlY3RvcnMucHVzaCh0aGlzLl9wYXJzZVNpbXBsZVNlbGVjdG9yKGRlbGltaXRlcnMpKTtcbiAgICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZVdoaXRlc3BhY2UoKTtcbiAgICB9XG5cbiAgICBjb25zdCBmaXJzdFNlbGVjdG9yID0gc2ltcGxlU2VsZWN0b3JzWzBdO1xuICAgIGNvbnN0IGxhc3RTZWxlY3RvciA9IHNpbXBsZVNlbGVjdG9yc1tzaW1wbGVTZWxlY3RvcnMubGVuZ3RoIC0gMV07XG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihmaXJzdFNlbGVjdG9yLCBsYXN0U2VsZWN0b3IpO1xuICAgIHJldHVybiBuZXcgQ3NzU2VsZWN0b3JBc3Qoc3Bhbiwgc2ltcGxlU2VsZWN0b3JzKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlVmFsdWUoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzU3R5bGVWYWx1ZUFzdCB7XG4gICAgZGVsaW1pdGVycyB8PSBSQlJBQ0VfREVMSU1fRkxBRyB8IFNFTUlDT0xPTl9ERUxJTV9GTEFHIHwgTkVXTElORV9ERUxJTV9GTEFHO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TVFlMRV9WQUxVRSk7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKTtcblxuICAgIGNvbnN0IHRva2VuczogQ3NzVG9rZW5bXSA9IFtdO1xuICAgIGxldCB3c1N0ciA9ICcnO1xuICAgIGxldCBwcmV2aW91czogQ3NzVG9rZW4gPSB1bmRlZmluZWQgITtcbiAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIGxldCB0b2tlbjogQ3NzVG9rZW47XG4gICAgICBpZiAocHJldmlvdXMgIT0gbnVsbCAmJiBwcmV2aW91cy50eXBlID09IENzc1Rva2VuVHlwZS5JZGVudGlmaWVyICYmXG4gICAgICAgICAgdGhpcy5fc2Nhbm5lci5wZWVrID09IGNoYXJzLiRMUEFSRU4pIHtcbiAgICAgICAgdG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICcoJyk7XG4gICAgICAgIHRva2Vucy5wdXNoKHRva2VuKTtcblxuICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNUWUxFX1ZBTFVFX0ZVTkNUSU9OKTtcblxuICAgICAgICB0b2tlbiA9IHRoaXMuX3NjYW4oKTtcbiAgICAgICAgdG9rZW5zLnB1c2godG9rZW4pO1xuXG4gICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU1RZTEVfVkFMVUUpO1xuXG4gICAgICAgIHRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnKScpO1xuICAgICAgICB0b2tlbnMucHVzaCh0b2tlbik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0b2tlbiA9IHRoaXMuX3NjYW4oKTtcbiAgICAgICAgaWYgKHRva2VuLnR5cGUgPT0gQ3NzVG9rZW5UeXBlLldoaXRlc3BhY2UpIHtcbiAgICAgICAgICB3c1N0ciArPSB0b2tlbi5zdHJWYWx1ZTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB3c1N0ciA9ICcnO1xuICAgICAgICAgIHRva2Vucy5wdXNoKHRva2VuKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcHJldmlvdXMgPSB0b2tlbjtcbiAgICB9XG5cbiAgICBjb25zdCBlbmQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKSAtIDE7XG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lV2hpdGVzcGFjZSgpO1xuXG4gICAgY29uc3QgY29kZSA9IHRoaXMuX3NjYW5uZXIucGVlaztcbiAgICBpZiAoY29kZSA9PSBjaGFycy4kU0VNSUNPTE9OKSB7XG4gICAgICB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICc7Jyk7XG4gICAgfSBlbHNlIGlmIChjb2RlICE9IGNoYXJzLiRSQlJBQ0UpIHtcbiAgICAgIHRoaXMuX2Vycm9yKFxuICAgICAgICAgIGdlbmVyYXRlRXJyb3JNZXNzYWdlKFxuICAgICAgICAgICAgICB0aGlzLl9nZXRTb3VyY2VDb250ZW50KCksIGBUaGUgQ1NTIGtleS92YWx1ZSBkZWZpbml0aW9uIGRpZCBub3QgZW5kIHdpdGggYSBzZW1pY29sb25gLFxuICAgICAgICAgICAgICBwcmV2aW91cy5zdHJWYWx1ZSwgcHJldmlvdXMuaW5kZXgsIHByZXZpb3VzLmxpbmUsIHByZXZpb3VzLmNvbHVtbiksXG4gICAgICAgICAgcHJldmlvdXMpO1xuICAgIH1cblxuICAgIGNvbnN0IHN0clZhbHVlID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIGVuZCk7XG4gICAgY29uc3Qgc3RhcnRUb2tlbiA9IHRva2Vuc1swXTtcbiAgICBjb25zdCBlbmRUb2tlbiA9IHRva2Vuc1t0b2tlbnMubGVuZ3RoIC0gMV07XG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBlbmRUb2tlbik7XG4gICAgcmV0dXJuIG5ldyBDc3NTdHlsZVZhbHVlQXN0KHNwYW4sIHRva2Vucywgc3RyVmFsdWUpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfY29sbGVjdFVudGlsRGVsaW0oZGVsaW1pdGVyczogbnVtYmVyLCBhc3NlcnRUeXBlOiBDc3NUb2tlblR5cGV8bnVsbCA9IG51bGwpOiBDc3NUb2tlbltdIHtcbiAgICBjb25zdCB0b2tlbnM6IENzc1Rva2VuW10gPSBbXTtcbiAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIGNvbnN0IHZhbCA9IGFzc2VydFR5cGUgIT0gbnVsbCA/IHRoaXMuX2NvbnN1bWUoYXNzZXJ0VHlwZSkgOiB0aGlzLl9zY2FuKCk7XG4gICAgICB0b2tlbnMucHVzaCh2YWwpO1xuICAgIH1cbiAgICByZXR1cm4gdG9rZW5zO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VCbG9jayhkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NCbG9ja0FzdCB7XG4gICAgZGVsaW1pdGVycyB8PSBSQlJBQ0VfREVMSU1fRkxBRztcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQkxPQ0spO1xuXG4gICAgY29uc3Qgc3RhcnRUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ3snKTtcbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcblxuICAgIGNvbnN0IHJlc3VsdHM6IENzc1J1bGVBc3RbXSA9IFtdO1xuICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgcmVzdWx0cy5wdXNoKHRoaXMuX3BhcnNlUnVsZShkZWxpbWl0ZXJzKSk7XG4gICAgfVxuXG4gICAgY29uc3QgZW5kVG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd9Jyk7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkJMT0NLKTtcbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcblxuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgZW5kVG9rZW4pO1xuICAgIHJldHVybiBuZXcgQ3NzQmxvY2tBc3Qoc3BhbiwgcmVzdWx0cyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVN0eWxlQmxvY2soZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzU3R5bGVzQmxvY2tBc3R8bnVsbCB7XG4gICAgZGVsaW1pdGVycyB8PSBSQlJBQ0VfREVMSU1fRkxBRyB8IExCUkFDRV9ERUxJTV9GTEFHO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TVFlMRV9CTE9DSyk7XG5cbiAgICBjb25zdCBzdGFydFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAneycpO1xuICAgIGlmIChzdGFydFRva2VuLm51bVZhbHVlICE9IGNoYXJzLiRMQlJBQ0UpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIGNvbnN0IGRlZmluaXRpb25zOiBDc3NEZWZpbml0aW9uQXN0W10gPSBbXTtcbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcblxuICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgZGVmaW5pdGlvbnMucHVzaCh0aGlzLl9wYXJzZURlZmluaXRpb24oZGVsaW1pdGVycykpO1xuICAgICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG4gICAgfVxuXG4gICAgY29uc3QgZW5kVG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd9Jyk7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNUWUxFX0JMT0NLKTtcbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcblxuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgZW5kVG9rZW4pO1xuICAgIHJldHVybiBuZXcgQ3NzU3R5bGVzQmxvY2tBc3Qoc3BhbiwgZGVmaW5pdGlvbnMpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VEZWZpbml0aW9uKGRlbGltaXRlcnM6IG51bWJlcik6IENzc0RlZmluaXRpb25Bc3Qge1xuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU1RZTEVfQkxPQ0spO1xuXG4gICAgbGV0IHByb3AgPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5JZGVudGlmaWVyKTtcbiAgICBsZXQgcGFyc2VWYWx1ZTogYm9vbGVhbiA9IGZhbHNlO1xuICAgIGxldCB2YWx1ZTogQ3NzU3R5bGVWYWx1ZUFzdHxudWxsID0gbnVsbDtcbiAgICBsZXQgZW5kVG9rZW46IENzc1Rva2VufENzc1N0eWxlVmFsdWVBc3QgPSBwcm9wO1xuXG4gICAgLy8gdGhlIGNvbG9uIHZhbHVlIHNlcGFyYXRlcyB0aGUgcHJvcCBmcm9tIHRoZSBzdHlsZS5cbiAgICAvLyB0aGVyZSBhcmUgYSBmZXcgY2FzZXMgYXMgdG8gd2hhdCBjb3VsZCBoYXBwZW4gaWYgaXRcbiAgICAvLyBpcyBtaXNzaW5nXG4gICAgc3dpdGNoICh0aGlzLl9zY2FubmVyLnBlZWspIHtcbiAgICAgIGNhc2UgY2hhcnMuJFNFTUlDT0xPTjpcbiAgICAgIGNhc2UgY2hhcnMuJFJCUkFDRTpcbiAgICAgIGNhc2UgY2hhcnMuJEVPRjpcbiAgICAgICAgcGFyc2VWYWx1ZSA9IGZhbHNlO1xuICAgICAgICBicmVhaztcblxuICAgICAgZGVmYXVsdDpcbiAgICAgICAgbGV0IHByb3BTdHIgPSBbcHJvcC5zdHJWYWx1ZV07XG4gICAgICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgIT0gY2hhcnMuJENPTE9OKSB7XG4gICAgICAgICAgLy8gdGhpcyB3aWxsIHRocm93IHRoZSBlcnJvclxuICAgICAgICAgIGNvbnN0IG5leHRWYWx1ZSA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJzonKTtcbiAgICAgICAgICBwcm9wU3RyLnB1c2gobmV4dFZhbHVlLnN0clZhbHVlKTtcblxuICAgICAgICAgIGNvbnN0IHJlbWFpbmluZ1Rva2VucyA9IHRoaXMuX2NvbGxlY3RVbnRpbERlbGltKFxuICAgICAgICAgICAgICBkZWxpbWl0ZXJzIHwgQ09MT05fREVMSU1fRkxBRyB8IFNFTUlDT0xPTl9ERUxJTV9GTEFHLCBDc3NUb2tlblR5cGUuSWRlbnRpZmllcik7XG4gICAgICAgICAgaWYgKHJlbWFpbmluZ1Rva2Vucy5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgICByZW1haW5pbmdUb2tlbnMuZm9yRWFjaCgodG9rZW4pID0+IHsgcHJvcFN0ci5wdXNoKHRva2VuLnN0clZhbHVlKTsgfSk7XG4gICAgICAgICAgfVxuXG4gICAgICAgICAgZW5kVG9rZW4gPSBwcm9wID1cbiAgICAgICAgICAgICAgbmV3IENzc1Rva2VuKHByb3AuaW5kZXgsIHByb3AuY29sdW1uLCBwcm9wLmxpbmUsIHByb3AudHlwZSwgcHJvcFN0ci5qb2luKCcgJykpO1xuICAgICAgICB9XG5cbiAgICAgICAgLy8gdGhpcyBtZWFucyB3ZSd2ZSByZWFjaGVkIHRoZSBlbmQgb2YgdGhlIGRlZmluaXRpb24gYW5kL29yIGJsb2NrXG4gICAgICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgPT0gY2hhcnMuJENPTE9OKSB7XG4gICAgICAgICAgdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnOicpO1xuICAgICAgICAgIHBhcnNlVmFsdWUgPSB0cnVlO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgIH1cblxuICAgIGlmIChwYXJzZVZhbHVlKSB7XG4gICAgICB2YWx1ZSA9IHRoaXMuX3BhcnNlVmFsdWUoZGVsaW1pdGVycyk7XG4gICAgICBlbmRUb2tlbiA9IHZhbHVlO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLl9lcnJvcihcbiAgICAgICAgICBnZW5lcmF0ZUVycm9yTWVzc2FnZShcbiAgICAgICAgICAgICAgdGhpcy5fZ2V0U291cmNlQ29udGVudCgpLCBgVGhlIENTUyBwcm9wZXJ0eSB3YXMgbm90IHBhaXJlZCB3aXRoIGEgc3R5bGUgdmFsdWVgLFxuICAgICAgICAgICAgICBwcm9wLnN0clZhbHVlLCBwcm9wLmluZGV4LCBwcm9wLmxpbmUsIHByb3AuY29sdW1uKSxcbiAgICAgICAgICBwcm9wKTtcbiAgICB9XG5cbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHByb3AsIGVuZFRva2VuKTtcbiAgICByZXR1cm4gbmV3IENzc0RlZmluaXRpb25Bc3Qoc3BhbiwgcHJvcCwgdmFsdWUgISk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9hc3NlcnRDb25kaXRpb24oc3RhdHVzOiBib29sZWFuLCBlcnJvck1lc3NhZ2U6IHN0cmluZywgcHJvYmxlbVRva2VuOiBDc3NUb2tlbik6IGJvb2xlYW4ge1xuICAgIGlmICghc3RhdHVzKSB7XG4gICAgICB0aGlzLl9lcnJvcihlcnJvck1lc3NhZ2UsIHByb2JsZW1Ub2tlbik7XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICB9XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfZXJyb3IobWVzc2FnZTogc3RyaW5nLCBwcm9ibGVtVG9rZW46IENzc1Rva2VuKSB7XG4gICAgY29uc3QgbGVuZ3RoID0gcHJvYmxlbVRva2VuLnN0clZhbHVlLmxlbmd0aDtcbiAgICBjb25zdCBlcnJvciA9IENzc1BhcnNlRXJyb3IuY3JlYXRlKFxuICAgICAgICB0aGlzLl9maWxlLCAwLCBwcm9ibGVtVG9rZW4ubGluZSwgcHJvYmxlbVRva2VuLmNvbHVtbiwgbGVuZ3RoLCBtZXNzYWdlKTtcbiAgICB0aGlzLl9lcnJvcnMucHVzaChlcnJvcik7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1BhcnNlRXJyb3IgZXh0ZW5kcyBQYXJzZUVycm9yIHtcbiAgc3RhdGljIGNyZWF0ZShcbiAgICAgIGZpbGU6IFBhcnNlU291cmNlRmlsZSwgb2Zmc2V0OiBudW1iZXIsIGxpbmU6IG51bWJlciwgY29sOiBudW1iZXIsIGxlbmd0aDogbnVtYmVyLFxuICAgICAgZXJyTXNnOiBzdHJpbmcpOiBDc3NQYXJzZUVycm9yIHtcbiAgICBjb25zdCBzdGFydCA9IG5ldyBQYXJzZUxvY2F0aW9uKGZpbGUsIG9mZnNldCwgbGluZSwgY29sKTtcbiAgICBjb25zdCBlbmQgPSBuZXcgUGFyc2VMb2NhdGlvbihmaWxlLCBvZmZzZXQsIGxpbmUsIGNvbCArIGxlbmd0aCk7XG4gICAgY29uc3Qgc3BhbiA9IG5ldyBQYXJzZVNvdXJjZVNwYW4oc3RhcnQsIGVuZCk7XG4gICAgcmV0dXJuIG5ldyBDc3NQYXJzZUVycm9yKHNwYW4sICdDU1MgUGFyc2UgRXJyb3I6ICcgKyBlcnJNc2cpO1xuICB9XG5cbiAgY29uc3RydWN0b3Ioc3BhbjogUGFyc2VTb3VyY2VTcGFuLCBtZXNzYWdlOiBzdHJpbmcpIHsgc3VwZXIoc3BhbiwgbWVzc2FnZSk7IH1cbn1cbiJdfQ==