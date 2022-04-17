/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import * as chars from '../chars';
import { ParseError, ParseLocation, ParseSourceFile, ParseSourceSpan } from '../parse_util';
import { BlockType, CssAst, CssAtRulePredicateAst, CssBlockAst, CssBlockDefinitionRuleAst, CssBlockRuleAst, CssDefinitionAst, CssInlineRuleAst, CssKeyframeDefinitionAst, CssKeyframeRuleAst, CssMediaQueryRuleAst, CssPseudoSelectorAst, CssSelectorAst, CssSelectorRuleAst, CssSimpleSelectorAst, CssStyleSheetAst, CssStyleValueAst, CssStylesBlockAst, CssUnknownRuleAst, CssUnknownTokenListAst, mergeTokens } from './css_ast';
import { CssLexer, CssLexerMode, CssToken, CssTokenType, generateErrorMessage, getRawMessage, isNewline } from './css_lexer';
var SPACE_OPERATOR = ' ';
export { CssToken } from './css_lexer';
export { BlockType } from './css_ast';
var SLASH_CHARACTER = '/';
var GT_CHARACTER = '>';
var TRIPLE_GT_OPERATOR_STR = '>>>';
var DEEP_OPERATOR_STR = '/deep/';
var EOF_DELIM_FLAG = 1;
var RBRACE_DELIM_FLAG = 2;
var LBRACE_DELIM_FLAG = 4;
var COMMA_DELIM_FLAG = 8;
var COLON_DELIM_FLAG = 16;
var SEMICOLON_DELIM_FLAG = 32;
var NEWLINE_DELIM_FLAG = 64;
var RPAREN_DELIM_FLAG = 128;
var LPAREN_DELIM_FLAG = 256;
var SPACE_DELIM_FLAG = 512;
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
var ParsedCssResult = /** @class */ (function () {
    function ParsedCssResult(errors, ast) {
        this.errors = errors;
        this.ast = ast;
    }
    return ParsedCssResult;
}());
export { ParsedCssResult };
var CssParser = /** @class */ (function () {
    function CssParser() {
        this._errors = [];
    }
    /**
     * @param css the CSS code that will be parsed
     * @param url the name of the CSS file containing the CSS source code
     */
    CssParser.prototype.parse = function (css, url) {
        var lexer = new CssLexer();
        this._file = new ParseSourceFile(css, url);
        this._scanner = lexer.scan(css, false);
        var ast = this._parseStyleSheet(EOF_DELIM_FLAG);
        var errors = this._errors;
        this._errors = [];
        var result = new ParsedCssResult(errors, ast);
        this._file = null;
        this._scanner = null;
        return result;
    };
    /** @internal */
    CssParser.prototype._parseStyleSheet = function (delimiters) {
        var results = [];
        this._scanner.consumeEmptyStatements();
        while (this._scanner.peek != chars.$EOF) {
            this._scanner.setMode(CssLexerMode.BLOCK);
            results.push(this._parseRule(delimiters));
        }
        var span = null;
        if (results.length > 0) {
            var firstRule = results[0];
            // we collect the last token like so incase there was an
            // EOF token that was emitted sometime during the lexing
            span = this._generateSourceSpan(firstRule, this._lastToken);
        }
        return new CssStyleSheetAst(span, results);
    };
    /** @internal */
    CssParser.prototype._getSourceContent = function () { return this._scanner != null ? this._scanner.input : ''; };
    /** @internal */
    CssParser.prototype._extractSourceContent = function (start, end) {
        return this._getSourceContent().substring(start, end + 1);
    };
    /** @internal */
    CssParser.prototype._generateSourceSpan = function (start, end) {
        if (end === void 0) { end = null; }
        var startLoc;
        if (start instanceof CssAst) {
            startLoc = start.location.start;
        }
        else {
            var token = start;
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
        var endLine = -1;
        var endColumn = -1;
        var endIndex = -1;
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
        var endLoc = new ParseLocation(this._file, endIndex, endLine, endColumn);
        return new ParseSourceSpan(startLoc, endLoc);
    };
    /** @internal */
    CssParser.prototype._resolveBlockType = function (token) {
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
    };
    /** @internal */
    CssParser.prototype._parseRule = function (delimiters) {
        if (this._scanner.peek == chars.$AT) {
            return this._parseAtRule(delimiters);
        }
        return this._parseSelectorRule(delimiters);
    };
    /** @internal */
    CssParser.prototype._parseAtRule = function (delimiters) {
        var start = this._getScannerIndex();
        this._scanner.setMode(CssLexerMode.BLOCK);
        var token = this._scan();
        var startToken = token;
        this._assertCondition(token.type == CssTokenType.AtKeyword, "The CSS Rule " + token.strValue + " is not a valid [@] rule.", token);
        var block;
        var type = this._resolveBlockType(token);
        var span;
        var tokens;
        var endToken;
        var end;
        var strValue;
        var query;
        switch (type) {
            case BlockType.Charset:
            case BlockType.Namespace:
            case BlockType.Import:
                var value = this._parseValue(delimiters);
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
                var name_1 = tokens[0];
                block = this._parseKeyframeBlock(delimiters);
                span = this._generateSourceSpan(startToken, block);
                return new CssKeyframeRuleAst(span, name_1, block);
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
                var listOfTokens_1 = [];
                var tokenName = token.strValue;
                this._scanner.setMode(CssLexerMode.ALL);
                this._error(generateErrorMessage(this._getSourceContent(), "The CSS \"at\" rule \"" + tokenName + "\" is not allowed to used here", token.strValue, token.index, token.line, token.column), token);
                this._collectUntilDelim(delimiters | LBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG)
                    .forEach(function (token) { listOfTokens_1.push(token); });
                if (this._scanner.peek == chars.$LBRACE) {
                    listOfTokens_1.push(this._consume(CssTokenType.Character, '{'));
                    this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG)
                        .forEach(function (token) { listOfTokens_1.push(token); });
                    listOfTokens_1.push(this._consume(CssTokenType.Character, '}'));
                }
                endToken = listOfTokens_1[listOfTokens_1.length - 1];
                span = this._generateSourceSpan(startToken, endToken);
                return new CssUnknownRuleAst(span, tokenName, listOfTokens_1);
        }
    };
    /** @internal */
    CssParser.prototype._parseSelectorRule = function (delimiters) {
        var start = this._getScannerIndex();
        var selectors = this._parseSelectors(delimiters);
        var block = this._parseStyleBlock(delimiters);
        var ruleAst;
        var span;
        var startSelector = selectors[0];
        if (block != null) {
            span = this._generateSourceSpan(startSelector, block);
            ruleAst = new CssSelectorRuleAst(span, selectors, block);
        }
        else {
            var name_2 = this._extractSourceContent(start, this._getScannerIndex() - 1);
            var innerTokens_1 = [];
            selectors.forEach(function (selector) {
                selector.selectorParts.forEach(function (part) {
                    part.tokens.forEach(function (token) { innerTokens_1.push(token); });
                });
            });
            var endToken = innerTokens_1[innerTokens_1.length - 1];
            span = this._generateSourceSpan(startSelector, endToken);
            ruleAst = new CssUnknownTokenListAst(span, name_2, innerTokens_1);
        }
        this._scanner.setMode(CssLexerMode.BLOCK);
        this._scanner.consumeEmptyStatements();
        return ruleAst;
    };
    /** @internal */
    CssParser.prototype._parseSelectors = function (delimiters) {
        delimiters |= LBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG;
        var selectors = [];
        var isParsingSelectors = true;
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
    };
    /** @internal */
    CssParser.prototype._scan = function () {
        var output = this._scanner.scan();
        var token = output.token;
        var error = output.error;
        if (error != null) {
            this._error(getRawMessage(error), token);
        }
        this._lastToken = token;
        return token;
    };
    /** @internal */
    CssParser.prototype._getScannerIndex = function () { return this._scanner.index; };
    /** @internal */
    CssParser.prototype._consume = function (type, value) {
        if (value === void 0) { value = null; }
        var output = this._scanner.consume(type, value);
        var token = output.token;
        var error = output.error;
        if (error != null) {
            this._error(getRawMessage(error), token);
        }
        this._lastToken = token;
        return token;
    };
    /** @internal */
    CssParser.prototype._parseKeyframeBlock = function (delimiters) {
        delimiters |= RBRACE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.KEYFRAME_BLOCK);
        var startToken = this._consume(CssTokenType.Character, '{');
        var definitions = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            definitions.push(this._parseKeyframeDefinition(delimiters));
        }
        var endToken = this._consume(CssTokenType.Character, '}');
        var span = this._generateSourceSpan(startToken, endToken);
        return new CssBlockAst(span, definitions);
    };
    /** @internal */
    CssParser.prototype._parseKeyframeDefinition = function (delimiters) {
        var start = this._getScannerIndex();
        var stepTokens = [];
        delimiters |= LBRACE_DELIM_FLAG;
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            stepTokens.push(this._parseKeyframeLabel(delimiters | COMMA_DELIM_FLAG));
            if (this._scanner.peek != chars.$LBRACE) {
                this._consume(CssTokenType.Character, ',');
            }
        }
        var stylesBlock = this._parseStyleBlock(delimiters | RBRACE_DELIM_FLAG);
        var span = this._generateSourceSpan(stepTokens[0], stylesBlock);
        var ast = new CssKeyframeDefinitionAst(span, stepTokens, stylesBlock);
        this._scanner.setMode(CssLexerMode.BLOCK);
        return ast;
    };
    /** @internal */
    CssParser.prototype._parseKeyframeLabel = function (delimiters) {
        this._scanner.setMode(CssLexerMode.KEYFRAME_BLOCK);
        return mergeTokens(this._collectUntilDelim(delimiters));
    };
    /** @internal */
    CssParser.prototype._parsePseudoSelector = function (delimiters) {
        var start = this._getScannerIndex();
        delimiters &= ~COMMA_DELIM_FLAG;
        // we keep the original value since we may use it to recurse when :not, :host are used
        var startingDelims = delimiters;
        var startToken = this._consume(CssTokenType.Character, ':');
        var tokens = [startToken];
        if (this._scanner.peek == chars.$COLON) { // ::something
            tokens.push(this._consume(CssTokenType.Character, ':'));
        }
        var innerSelectors = [];
        this._scanner.setMode(CssLexerMode.PSEUDO_SELECTOR);
        // host, host-context, lang, not, nth-child are all identifiers
        var pseudoSelectorToken = this._consume(CssTokenType.Identifier);
        var pseudoSelectorName = pseudoSelectorToken.strValue;
        tokens.push(pseudoSelectorToken);
        // host(), lang(), nth-child(), etc...
        if (this._scanner.peek == chars.$LPAREN) {
            this._scanner.setMode(CssLexerMode.PSEUDO_SELECTOR_WITH_ARGUMENTS);
            var openParenToken = this._consume(CssTokenType.Character, '(');
            tokens.push(openParenToken);
            // :host(innerSelector(s)), :not(selector), etc...
            if (_pseudoSelectorSupportsInnerSelectors(pseudoSelectorName)) {
                var innerDelims = startingDelims | LPAREN_DELIM_FLAG | RPAREN_DELIM_FLAG;
                if (pseudoSelectorName == 'not') {
                    // the inner selector inside of :not(...) can only be one
                    // CSS selector (no commas allowed) ... This is according
                    // to the CSS specification
                    innerDelims |= COMMA_DELIM_FLAG;
                }
                // :host(a, b, c) {
                this._parseSelectors(innerDelims).forEach(function (selector, index) {
                    innerSelectors.push(selector);
                });
            }
            else {
                // this branch is for things like "en-us, 2k + 1, etc..."
                // which all end up in pseudoSelectors like :lang, :nth-child, etc..
                var innerValueDelims = delimiters | LBRACE_DELIM_FLAG | COLON_DELIM_FLAG |
                    RPAREN_DELIM_FLAG | LPAREN_DELIM_FLAG;
                while (!characterContainsDelimiter(this._scanner.peek, innerValueDelims)) {
                    var token = this._scan();
                    tokens.push(token);
                }
            }
            var closeParenToken = this._consume(CssTokenType.Character, ')');
            tokens.push(closeParenToken);
        }
        var end = this._getScannerIndex() - 1;
        var strValue = this._extractSourceContent(start, end);
        var endToken = tokens[tokens.length - 1];
        var span = this._generateSourceSpan(startToken, endToken);
        return new CssPseudoSelectorAst(span, strValue, pseudoSelectorName, tokens, innerSelectors);
    };
    /** @internal */
    CssParser.prototype._parseSimpleSelector = function (delimiters) {
        var start = this._getScannerIndex();
        delimiters |= COMMA_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.SELECTOR);
        var selectorCssTokens = [];
        var pseudoSelectors = [];
        var previousToken = undefined;
        var selectorPartDelimiters = delimiters | SPACE_DELIM_FLAG;
        var loopOverSelector = !characterContainsDelimiter(this._scanner.peek, selectorPartDelimiters);
        var hasAttributeError = false;
        while (loopOverSelector) {
            var peek = this._scanner.peek;
            switch (peek) {
                case chars.$COLON:
                    var innerPseudo = this._parsePseudoSelector(delimiters);
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
                    var token = this._scan();
                    previousToken = token;
                    selectorCssTokens.push(token);
                    break;
            }
            loopOverSelector = !characterContainsDelimiter(this._scanner.peek, selectorPartDelimiters);
        }
        hasAttributeError =
            hasAttributeError || this._scanner.getMode() == CssLexerMode.ATTRIBUTE_SELECTOR;
        if (hasAttributeError) {
            this._error("Unbalanced CSS attribute selector at column " + previousToken.line + ":" + previousToken.column, previousToken);
        }
        var end = this._getScannerIndex() - 1;
        // this happens if the selector is not directly followed by
        // a comma or curly brace without a space in between
        var operator = null;
        var operatorScanCount = 0;
        var lastOperatorToken = null;
        if (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            while (operator == null && !characterContainsDelimiter(this._scanner.peek, delimiters) &&
                isSelectorOperatorCharacter(this._scanner.peek)) {
                var token = this._scan();
                var tokenOperator = token.strValue;
                operatorScanCount++;
                lastOperatorToken = token;
                if (tokenOperator != SPACE_OPERATOR) {
                    switch (tokenOperator) {
                        case SLASH_CHARACTER:
                            // /deep/ operator
                            var deepToken = this._consume(CssTokenType.Identifier);
                            var deepSlash = this._consume(CssTokenType.Character);
                            var index = lastOperatorToken.index;
                            var line = lastOperatorToken.line;
                            var column = lastOperatorToken.column;
                            if (deepToken != null && deepToken.strValue.toLowerCase() == 'deep' &&
                                deepSlash.strValue == SLASH_CHARACTER) {
                                token = new CssToken(lastOperatorToken.index, lastOperatorToken.column, lastOperatorToken.line, CssTokenType.Identifier, DEEP_OPERATOR_STR);
                            }
                            else {
                                var text = SLASH_CHARACTER + deepToken.strValue + deepSlash.strValue;
                                this._error(generateErrorMessage(this._getSourceContent(), text + " is an invalid CSS operator", text, index, line, column), lastOperatorToken);
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
        var strValue = this._extractSourceContent(start, end);
        // if we do come across one or more spaces inside of
        // the operators loop then an empty space is still a
        // valid operator to use if something else was not found
        if (operator == null && operatorScanCount > 0 && this._scanner.peek != chars.$LBRACE) {
            operator = lastOperatorToken;
        }
        // please note that `endToken` is reassigned multiple times below
        // so please do not optimize the if statements into if/elseif
        var startTokenOrAst = null;
        var endTokenOrAst = null;
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
        var span = this._generateSourceSpan(startTokenOrAst, endTokenOrAst);
        return new CssSimpleSelectorAst(span, selectorCssTokens, strValue, pseudoSelectors, operator);
    };
    /** @internal */
    CssParser.prototype._parseSelector = function (delimiters) {
        delimiters |= COMMA_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.SELECTOR);
        var simpleSelectors = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            simpleSelectors.push(this._parseSimpleSelector(delimiters));
            this._scanner.consumeWhitespace();
        }
        var firstSelector = simpleSelectors[0];
        var lastSelector = simpleSelectors[simpleSelectors.length - 1];
        var span = this._generateSourceSpan(firstSelector, lastSelector);
        return new CssSelectorAst(span, simpleSelectors);
    };
    /** @internal */
    CssParser.prototype._parseValue = function (delimiters) {
        delimiters |= RBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG | NEWLINE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.STYLE_VALUE);
        var start = this._getScannerIndex();
        var tokens = [];
        var wsStr = '';
        var previous = undefined;
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            var token = void 0;
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
        var end = this._getScannerIndex() - 1;
        this._scanner.consumeWhitespace();
        var code = this._scanner.peek;
        if (code == chars.$SEMICOLON) {
            this._consume(CssTokenType.Character, ';');
        }
        else if (code != chars.$RBRACE) {
            this._error(generateErrorMessage(this._getSourceContent(), "The CSS key/value definition did not end with a semicolon", previous.strValue, previous.index, previous.line, previous.column), previous);
        }
        var strValue = this._extractSourceContent(start, end);
        var startToken = tokens[0];
        var endToken = tokens[tokens.length - 1];
        var span = this._generateSourceSpan(startToken, endToken);
        return new CssStyleValueAst(span, tokens, strValue);
    };
    /** @internal */
    CssParser.prototype._collectUntilDelim = function (delimiters, assertType) {
        if (assertType === void 0) { assertType = null; }
        var tokens = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            var val = assertType != null ? this._consume(assertType) : this._scan();
            tokens.push(val);
        }
        return tokens;
    };
    /** @internal */
    CssParser.prototype._parseBlock = function (delimiters) {
        delimiters |= RBRACE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.BLOCK);
        var startToken = this._consume(CssTokenType.Character, '{');
        this._scanner.consumeEmptyStatements();
        var results = [];
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            results.push(this._parseRule(delimiters));
        }
        var endToken = this._consume(CssTokenType.Character, '}');
        this._scanner.setMode(CssLexerMode.BLOCK);
        this._scanner.consumeEmptyStatements();
        var span = this._generateSourceSpan(startToken, endToken);
        return new CssBlockAst(span, results);
    };
    /** @internal */
    CssParser.prototype._parseStyleBlock = function (delimiters) {
        delimiters |= RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG;
        this._scanner.setMode(CssLexerMode.STYLE_BLOCK);
        var startToken = this._consume(CssTokenType.Character, '{');
        if (startToken.numValue != chars.$LBRACE) {
            return null;
        }
        var definitions = [];
        this._scanner.consumeEmptyStatements();
        while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
            definitions.push(this._parseDefinition(delimiters));
            this._scanner.consumeEmptyStatements();
        }
        var endToken = this._consume(CssTokenType.Character, '}');
        this._scanner.setMode(CssLexerMode.STYLE_BLOCK);
        this._scanner.consumeEmptyStatements();
        var span = this._generateSourceSpan(startToken, endToken);
        return new CssStylesBlockAst(span, definitions);
    };
    /** @internal */
    CssParser.prototype._parseDefinition = function (delimiters) {
        this._scanner.setMode(CssLexerMode.STYLE_BLOCK);
        var prop = this._consume(CssTokenType.Identifier);
        var parseValue = false;
        var value = null;
        var endToken = prop;
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
                var propStr_1 = [prop.strValue];
                if (this._scanner.peek != chars.$COLON) {
                    // this will throw the error
                    var nextValue = this._consume(CssTokenType.Character, ':');
                    propStr_1.push(nextValue.strValue);
                    var remainingTokens = this._collectUntilDelim(delimiters | COLON_DELIM_FLAG | SEMICOLON_DELIM_FLAG, CssTokenType.Identifier);
                    if (remainingTokens.length > 0) {
                        remainingTokens.forEach(function (token) { propStr_1.push(token.strValue); });
                    }
                    endToken = prop =
                        new CssToken(prop.index, prop.column, prop.line, prop.type, propStr_1.join(' '));
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
            this._error(generateErrorMessage(this._getSourceContent(), "The CSS property was not paired with a style value", prop.strValue, prop.index, prop.line, prop.column), prop);
        }
        var span = this._generateSourceSpan(prop, endToken);
        return new CssDefinitionAst(span, prop, value);
    };
    /** @internal */
    CssParser.prototype._assertCondition = function (status, errorMessage, problemToken) {
        if (!status) {
            this._error(errorMessage, problemToken);
            return true;
        }
        return false;
    };
    /** @internal */
    CssParser.prototype._error = function (message, problemToken) {
        var length = problemToken.strValue.length;
        var error = CssParseError.create(this._file, 0, problemToken.line, problemToken.column, length, message);
        this._errors.push(error);
    };
    return CssParser;
}());
export { CssParser };
var CssParseError = /** @class */ (function (_super) {
    tslib_1.__extends(CssParseError, _super);
    function CssParseError(span, message) {
        return _super.call(this, span, message) || this;
    }
    CssParseError.create = function (file, offset, line, col, length, errMsg) {
        var start = new ParseLocation(file, offset, line, col);
        var end = new ParseLocation(file, offset, line, col + length);
        var span = new ParseSourceSpan(start, end);
        return new CssParseError(span, 'CSS Parse Error: ' + errMsg);
    };
    return CssParseError;
}(ParseError));
export { CssParseError };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NzX3BhcnNlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9jc3NfcGFyc2VyL2Nzc19wYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sS0FBSyxLQUFLLE1BQU0sVUFBVSxDQUFDO0FBQ2xDLE9BQU8sRUFBQyxVQUFVLEVBQUUsYUFBYSxFQUFFLGVBQWUsRUFBRSxlQUFlLEVBQUMsTUFBTSxlQUFlLENBQUM7QUFFMUYsT0FBTyxFQUFDLFNBQVMsRUFBRSxNQUFNLEVBQUUscUJBQXFCLEVBQUUsV0FBVyxFQUFFLHlCQUF5QixFQUFFLGVBQWUsRUFBRSxnQkFBZ0IsRUFBRSxnQkFBZ0IsRUFBRSx3QkFBd0IsRUFBRSxrQkFBa0IsRUFBRSxvQkFBb0IsRUFBRSxvQkFBb0IsRUFBYyxjQUFjLEVBQUUsa0JBQWtCLEVBQUUsb0JBQW9CLEVBQUUsZ0JBQWdCLEVBQUUsZ0JBQWdCLEVBQUUsaUJBQWlCLEVBQUUsaUJBQWlCLEVBQUUsc0JBQXNCLEVBQUUsV0FBVyxFQUFDLE1BQU0sV0FBVyxDQUFDO0FBQy9hLE9BQU8sRUFBQyxRQUFRLEVBQUUsWUFBWSxFQUFjLFFBQVEsRUFBRSxZQUFZLEVBQUUsb0JBQW9CLEVBQUUsYUFBYSxFQUFFLFNBQVMsRUFBQyxNQUFNLGFBQWEsQ0FBQztBQUV2SSxJQUFNLGNBQWMsR0FBRyxHQUFHLENBQUM7QUFFM0IsT0FBTyxFQUFDLFFBQVEsRUFBQyxNQUFNLGFBQWEsQ0FBQztBQUNyQyxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sV0FBVyxDQUFDO0FBRXBDLElBQU0sZUFBZSxHQUFHLEdBQUcsQ0FBQztBQUM1QixJQUFNLFlBQVksR0FBRyxHQUFHLENBQUM7QUFDekIsSUFBTSxzQkFBc0IsR0FBRyxLQUFLLENBQUM7QUFDckMsSUFBTSxpQkFBaUIsR0FBRyxRQUFRLENB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S0FBSyxHQUFHLGFBQWEsQ0FBQyxNQUFNLENBQzlCLElBQUksQ0FBQyxLQUFLLEVBQUUsQ0FBQyxFQUFFLFlBQVksQ0FBQyxJQUFJLEVBQUUsWUFBWSxDQUFDLE1BQU0sRUFBRSxNQUFNLEVBQUUsT0FBTyxDQUFDLENBQUM7UUFDNUUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDM0IsQ0FBQztJQUNILGdCQUFDO0FBQUQsQ0FBQyxBQXR5QkQsSUFzeUJDOztBQUVEO0lBQW1DLHlDQUFVO0lBVTNDLHVCQUFZLElBQXFCLEVBQUUsT0FBZTtlQUFJLGtCQUFNLElBQUksRUFBRSxPQUFPLENBQUM7SUFBRSxDQUFDO0lBVHRFLG9CQUFNLEdBQWIsVUFDSSxJQUFxQixFQUFFLE1BQWMsRUFBRSxJQUFZLEVBQUUsR0FBVyxFQUFFLE1BQWMsRUFDaEYsTUFBYztRQUNoQixJQUFNLEtBQUssR0FBRyxJQUFJLGFBQWEsQ0FBQyxJQUFJLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxHQUFHLENBQUMsQ0FBQztRQUN6RCxJQUFNLEdBQUcsR0FBRyxJQUFJLGFBQWEsQ0FBQyxJQUFJLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxHQUFHLEdBQUcsTUFBTSxDQUFDLENBQUM7UUFDaEUsSUFBTSxJQUFJLEdBQUcsSUFBSSxlQUFlLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxDQUFDO1FBQzdDLE9BQU8sSUFBSSxhQUFhLENBQUMsSUFBSSxFQUFFLG1CQUFtQixHQUFHLE1BQU0sQ0FBQyxDQUFDO0lBQy9ELENBQUM7SUFHSCxvQkFBQztBQUFELENBQUMsQUFYRCxDQUFtQyxVQUFVLEdBVzVDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQgKiBhcyBjaGFycyBmcm9tICcuLi9jaGFycyc7XG5pbXBvcnQge1BhcnNlRXJyb3IsIFBhcnNlTG9jYXRpb24sIFBhcnNlU291cmNlRmlsZSwgUGFyc2VTb3VyY2VTcGFufSBmcm9tICcuLi9wYXJzZV91dGlsJztcblxuaW1wb3J0IHtCbG9ja1R5cGUsIENzc0FzdCwgQ3NzQXRSdWxlUHJlZGljYXRlQXN0LCBDc3NCbG9ja0FzdCwgQ3NzQmxvY2tEZWZpbml0aW9uUnVsZUFzdCwgQ3NzQmxvY2tSdWxlQXN0LCBDc3NEZWZpbml0aW9uQXN0LCBDc3NJbmxpbmVSdWxlQXN0LCBDc3NLZXlmcmFtZURlZmluaXRpb25Bc3QsIENzc0tleWZyYW1lUnVsZUFzdCwgQ3NzTWVkaWFRdWVyeVJ1bGVBc3QsIENzc1BzZXVkb1NlbGVjdG9yQXN0LCBDc3NSdWxlQXN0LCBDc3NTZWxlY3RvckFzdCwgQ3NzU2VsZWN0b3JSdWxlQXN0LCBDc3NTaW1wbGVTZWxlY3RvckFzdCwgQ3NzU3R5bGVTaGVldEFzdCwgQ3NzU3R5bGVWYWx1ZUFzdCwgQ3NzU3R5bGVzQmxvY2tBc3QsIENzc1Vua25vd25SdWxlQXN0LCBDc3NVbmtub3duVG9rZW5MaXN0QXN0LCBtZXJnZVRva2Vuc30gZnJvbSAnLi9jc3NfYXN0JztcbmltcG9ydCB7Q3NzTGV4ZXIsIENzc0xleGVyTW9kZSwgQ3NzU2Nhbm5lciwgQ3NzVG9rZW4sIENzc1Rva2VuVHlwZSwgZ2VuZXJhdGVFcnJvck1lc3NhZ2UsIGdldFJhd01lc3NhZ2UsIGlzTmV3bGluZX0gZnJvbSAnLi9jc3NfbGV4ZXInO1xuXG5jb25zdCBTUEFDRV9PUEVSQVRPUiA9ICcgJztcblxuZXhwb3J0IHtDc3NUb2tlbn0gZnJvbSAnLi9jc3NfbGV4ZXInO1xuZXhwb3J0IHtCbG9ja1R5cGV9IGZyb20gJy4vY3NzX2FzdCc7XG5cbmNvbnN0IFNMQVNIX0NIQVJBQ1RFUiA9ICcvJztcbmNvbnN0IEdUX0NIQVJBQ1RFUiA9ICc+JztcbmNvbnN0IFRSSVBMRV9HVF9PUEVSQVRPUl9TVFIgPSAnPj4+JztcbmNvbnN0IERFRVBfT1BFUkFUT1JfU1RSID0gJy9kZWVwLyc7XG5cbmNvbnN0IEVPRl9ERUxJTV9GTEFHID0gMTtcbmNvbnN0IFJCUkFDRV9ERUxJTV9GTEFHID0gMjtcbmNvbnN0IExCUkFDRV9ERUxJTV9GTEFHID0gNDtcbmNvbnN0IENPTU1BX0RFTElNX0ZMQUcgPSA4O1xuY29uc3QgQ09MT05fREVMSU1fRkxBRyA9IDE2O1xuY29uc3QgU0VNSUNPTE9OX0RFTElNX0ZMQUcgPSAzMjtcbmNvbnN0IE5FV0xJTkVfREVMSU1fRkxBRyA9IDY0O1xuY29uc3QgUlBBUkVOX0RFTElNX0ZMQUcgPSAxMjg7XG5jb25zdCBMUEFSRU5fREVMSU1fRkxBRyA9IDI1NjtcbmNvbnN0IFNQQUNFX0RFTElNX0ZMQUcgPSA1MTI7XG5cbmZ1bmN0aW9uIF9wc2V1ZG9TZWxlY3RvclN1cHBvcnRzSW5uZXJTZWxlY3RvcnMobmFtZTogc3RyaW5nKTogYm9vbGVhbiB7XG4gIHJldHVybiBbJ25vdCcsICdob3N0JywgJ2hvc3QtY29udGV4dCddLmluZGV4T2YobmFtZSkgPj0gMDtcbn1cblxuZnVuY3Rpb24gaXNTZWxlY3Rvck9wZXJhdG9yQ2hhcmFjdGVyKGNvZGU6IG51bWJlcik6IGJvb2xlYW4ge1xuICBzd2l0Y2ggKGNvZGUpIHtcbiAgICBjYXNlIGNoYXJzLiRTTEFTSDpcbiAgICBjYXNlIGNoYXJzLiRUSUxEQTpcbiAgICBjYXNlIGNoYXJzLiRQTFVTOlxuICAgIGNhc2UgY2hhcnMuJEdUOlxuICAgICAgcmV0dXJuIHRydWU7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBjaGFycy5pc1doaXRlc3BhY2UoY29kZSk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0RGVsaW1Gcm9tQ2hhcmFjdGVyKGNvZGU6IG51bWJlcik6IG51bWJlciB7XG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJEVPRjpcbiAgICAgIHJldHVybiBFT0ZfREVMSU1fRkxBRztcbiAgICBjYXNlIGNoYXJzLiRDT01NQTpcbiAgICAgIHJldHVybiBDT01NQV9ERUxJTV9GTEFHO1xuICAgIGNhc2UgY2hhcnMuJENPTE9OOlxuICAgICAgcmV0dXJuIENPTE9OX0RFTElNX0ZMQUc7XG4gICAgY2FzZSBjaGFycy4kU0VNSUNPTE9OOlxuICAgICAgcmV0dXJuIFNFTUlDT0xPTl9ERUxJTV9GTEFHO1xuICAgIGNhc2UgY2hhcnMuJFJCUkFDRTpcbiAgICAgIHJldHVybiBSQlJBQ0VfREVMSU1fRkxBRztcbiAgICBjYXNlIGNoYXJzLiRMQlJBQ0U6XG4gICAgICByZXR1cm4gTEJSQUNFX0RFTElNX0ZMQUc7XG4gICAgY2FzZSBjaGFycy4kUlBBUkVOOlxuICAgICAgcmV0dXJuIFJQQVJFTl9ERUxJTV9GTEFHO1xuICAgIGNhc2UgY2hhcnMuJFNQQUNFOlxuICAgIGNhc2UgY2hhcnMuJFRBQjpcbiAgICAgIHJldHVybiBTUEFDRV9ERUxJTV9GTEFHO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gaXNOZXdsaW5lKGNvZGUpID8gTkVXTElORV9ERUxJTV9GTEFHIDogMDtcbiAgfVxufVxuXG5mdW5jdGlvbiBjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcihjb2RlOiBudW1iZXIsIGRlbGltaXRlcnM6IG51bWJlcik6IGJvb2xlYW4ge1xuICByZXR1cm4gKGdldERlbGltRnJvbUNoYXJhY3Rlcihjb2RlKSAmIGRlbGltaXRlcnMpID4gMDtcbn1cblxuZXhwb3J0IGNsYXNzIFBhcnNlZENzc1Jlc3VsdCB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBlcnJvcnM6IENzc1BhcnNlRXJyb3JbXSwgcHVibGljIGFzdDogQ3NzU3R5bGVTaGVldEFzdCkge31cbn1cblxuZXhwb3J0IGNsYXNzIENzc1BhcnNlciB7XG4gIHByaXZhdGUgX2Vycm9yczogQ3NzUGFyc2VFcnJvcltdID0gW107XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwcml2YXRlIF9maWxlICE6IFBhcnNlU291cmNlRmlsZTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX3NjYW5uZXIgITogQ3NzU2Nhbm5lcjtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2xhc3RUb2tlbiAhOiBDc3NUb2tlbjtcblxuICAvKipcbiAgICogQHBhcmFtIGNzcyB0aGUgQ1NTIGNvZGUgdGhhdCB3aWxsIGJlIHBhcnNlZFxuICAgKiBAcGFyYW0gdXJsIHRoZSBuYW1lIG9mIHRoZSBDU1MgZmlsZSBjb250YWluaW5nIHRoZSBDU1Mgc291cmNlIGNvZGVcbiAgICovXG4gIHBhcnNlKGNzczogc3RyaW5nLCB1cmw6IHN0cmluZyk6IFBhcnNlZENzc1Jlc3VsdCB7XG4gICAgY29uc3QgbGV4ZXIgPSBuZXcgQ3NzTGV4ZXIoKTtcbiAgICB0aGlzLl9maWxlID0gbmV3IFBhcnNlU291cmNlRmlsZShjc3MsIHVybCk7XG4gICAgdGhpcy5fc2Nhbm5lciA9IGxleGVyLnNjYW4oY3NzLCBmYWxzZSk7XG5cbiAgICBjb25zdCBhc3QgPSB0aGlzLl9wYXJzZVN0eWxlU2hlZXQoRU9GX0RFTElNX0ZMQUcpO1xuXG4gICAgY29uc3QgZXJyb3JzID0gdGhpcy5fZXJyb3JzO1xuICAgIHRoaXMuX2Vycm9ycyA9IFtdO1xuXG4gICAgY29uc3QgcmVzdWx0ID0gbmV3IFBhcnNlZENzc1Jlc3VsdChlcnJvcnMsIGFzdCk7XG4gICAgdGhpcy5fZmlsZSA9IG51bGwgYXMgYW55O1xuICAgIHRoaXMuX3NjYW5uZXIgPSBudWxsIGFzIGFueTtcbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VTdHlsZVNoZWV0KGRlbGltaXRlcnM6IG51bWJlcik6IENzc1N0eWxlU2hlZXRBc3Qge1xuICAgIGNvbnN0IHJlc3VsdHM6IENzc1J1bGVBc3RbXSA9IFtdO1xuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuICAgIHdoaWxlICh0aGlzLl9zY2FubmVyLnBlZWsgIT0gY2hhcnMuJEVPRikge1xuICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5CTE9DSyk7XG4gICAgICByZXN1bHRzLnB1c2godGhpcy5fcGFyc2VSdWxlKGRlbGltaXRlcnMpKTtcbiAgICB9XG4gICAgbGV0IHNwYW46IFBhcnNlU291cmNlU3BhbnxudWxsID0gbnVsbDtcbiAgICBpZiAocmVzdWx0cy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zdCBmaXJzdFJ1bGUgPSByZXN1bHRzWzBdO1xuICAgICAgLy8gd2UgY29sbGVjdCB0aGUgbGFzdCB0b2tlbiBsaWtlIHNvIGluY2FzZSB0aGVyZSB3YXMgYW5cbiAgICAgIC8vIEVPRiB0b2tlbiB0aGF0IHdhcyBlbWl0dGVkIHNvbWV0aW1lIGR1cmluZyB0aGUgbGV4aW5nXG4gICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKGZpcnN0UnVsZSwgdGhpcy5fbGFzdFRva2VuKTtcbiAgICB9XG4gICAgcmV0dXJuIG5ldyBDc3NTdHlsZVNoZWV0QXN0KHNwYW4gISwgcmVzdWx0cyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9nZXRTb3VyY2VDb250ZW50KCk6IHN0cmluZyB7IHJldHVybiB0aGlzLl9zY2FubmVyICE9IG51bGwgPyB0aGlzLl9zY2FubmVyLmlucHV0IDogJyc7IH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9leHRyYWN0U291cmNlQ29udGVudChzdGFydDogbnVtYmVyLCBlbmQ6IG51bWJlcik6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuX2dldFNvdXJjZUNvbnRlbnQoKS5zdWJzdHJpbmcoc3RhcnQsIGVuZCArIDEpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0OiBDc3NUb2tlbnxDc3NBc3QsIGVuZDogQ3NzVG9rZW58Q3NzQXN0fG51bGwgPSBudWxsKTogUGFyc2VTb3VyY2VTcGFuIHtcbiAgICBsZXQgc3RhcnRMb2M6IFBhcnNlTG9jYXRpb247XG4gICAgaWYgKHN0YXJ0IGluc3RhbmNlb2YgQ3NzQXN0KSB7XG4gICAgICBzdGFydExvYyA9IHN0YXJ0LmxvY2F0aW9uLnN0YXJ0O1xuICAgIH0gZWxzZSB7XG4gICAgICBsZXQgdG9rZW4gPSBzdGFydDtcbiAgICAgIGlmICh0b2tlbiA9PSBudWxsKSB7XG4gICAgICAgIC8vIHRoZSBkYXRhIGhlcmUgaXMgaW52YWxpZCwgaG93ZXZlciwgaWYgYW5kIHdoZW4gdGhpcyBkb2VzXG4gICAgICAgIC8vIG9jY3VyLCBhbnkgb3RoZXIgZXJyb3JzIGFzc29jaWF0ZWQgd2l0aCB0aGlzIHdpbGwgYmUgY29sbGVjdGVkXG4gICAgICAgIHRva2VuID0gdGhpcy5fbGFzdFRva2VuO1xuICAgICAgfVxuICAgICAgc3RhcnRMb2MgPSBuZXcgUGFyc2VMb2NhdGlvbih0aGlzLl9maWxlLCB0b2tlbi5pbmRleCwgdG9rZW4ubGluZSwgdG9rZW4uY29sdW1uKTtcbiAgICB9XG5cbiAgICBpZiAoZW5kID09IG51bGwpIHtcbiAgICAgIGVuZCA9IHRoaXMuX2xhc3RUb2tlbjtcbiAgICB9XG5cbiAgICBsZXQgZW5kTGluZTogbnVtYmVyID0gLTE7XG4gICAgbGV0IGVuZENvbHVtbjogbnVtYmVyID0gLTE7XG4gICAgbGV0IGVuZEluZGV4OiBudW1iZXIgPSAtMTtcbiAgICBpZiAoZW5kIGluc3RhbmNlb2YgQ3NzQXN0KSB7XG4gICAgICBlbmRMaW5lID0gZW5kLmxvY2F0aW9uLmVuZC5saW5lICE7XG4gICAgICBlbmRDb2x1bW4gPSBlbmQubG9jYXRpb24uZW5kLmNvbCAhO1xuICAgICAgZW5kSW5kZXggPSBlbmQubG9jYXRpb24uZW5kLm9mZnNldCAhO1xuICAgIH0gZWxzZSBpZiAoZW5kIGluc3RhbmNlb2YgQ3NzVG9rZW4pIHtcbiAgICAgIGVuZExpbmUgPSBlbmQubGluZTtcbiAgICAgIGVuZENvbHVtbiA9IGVuZC5jb2x1bW47XG4gICAgICBlbmRJbmRleCA9IGVuZC5pbmRleDtcbiAgICB9XG5cbiAgICBjb25zdCBlbmRMb2MgPSBuZXcgUGFyc2VMb2NhdGlvbih0aGlzLl9maWxlLCBlbmRJbmRleCwgZW5kTGluZSwgZW5kQ29sdW1uKTtcbiAgICByZXR1cm4gbmV3IFBhcnNlU291cmNlU3BhbihzdGFydExvYywgZW5kTG9jKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3Jlc29sdmVCbG9ja1R5cGUodG9rZW46IENzc1Rva2VuKTogQmxvY2tUeXBlIHtcbiAgICBzd2l0Y2ggKHRva2VuLnN0clZhbHVlKSB7XG4gICAgICBjYXNlICdALW8ta2V5ZnJhbWVzJzpcbiAgICAgIGNhc2UgJ0AtbW96LWtleWZyYW1lcyc6XG4gICAgICBjYXNlICdALXdlYmtpdC1rZXlmcmFtZXMnOlxuICAgICAgY2FzZSAnQGtleWZyYW1lcyc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuS2V5ZnJhbWVzO1xuXG4gICAgICBjYXNlICdAY2hhcnNldCc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuQ2hhcnNldDtcblxuICAgICAgY2FzZSAnQGltcG9ydCc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuSW1wb3J0O1xuXG4gICAgICBjYXNlICdAbmFtZXNwYWNlJzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5OYW1lc3BhY2U7XG5cbiAgICAgIGNhc2UgJ0BwYWdlJzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5QYWdlO1xuXG4gICAgICBjYXNlICdAZG9jdW1lbnQnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLkRvY3VtZW50O1xuXG4gICAgICBjYXNlICdAbWVkaWEnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLk1lZGlhUXVlcnk7XG5cbiAgICAgIGNhc2UgJ0Bmb250LWZhY2UnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLkZvbnRGYWNlO1xuXG4gICAgICBjYXNlICdAdmlld3BvcnQnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLlZpZXdwb3J0O1xuXG4gICAgICBjYXNlICdAc3VwcG9ydHMnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLlN1cHBvcnRzO1xuXG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLlVuc3VwcG9ydGVkO1xuICAgIH1cbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlUnVsZShkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NSdWxlQXN0IHtcbiAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrID09IGNoYXJzLiRBVCkge1xuICAgICAgcmV0dXJuIHRoaXMuX3BhcnNlQXRSdWxlKGRlbGltaXRlcnMpO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy5fcGFyc2VTZWxlY3RvclJ1bGUoZGVsaW1pdGVycyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZUF0UnVsZShkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NSdWxlQXN0IHtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5CTE9DSyk7XG4gICAgY29uc3QgdG9rZW4gPSB0aGlzLl9zY2FuKCk7XG4gICAgY29uc3Qgc3RhcnRUb2tlbiA9IHRva2VuO1xuXG4gICAgdGhpcy5fYXNzZXJ0Q29uZGl0aW9uKFxuICAgICAgICB0b2tlbi50eXBlID09IENzc1Rva2VuVHlwZS5BdEtleXdvcmQsXG4gICAgICAgIGBUaGUgQ1NTIFJ1bGUgJHt0b2tlbi5zdHJWYWx1ZX0gaXMgbm90IGEgdmFsaWQgW0BdIHJ1bGUuYCwgdG9rZW4pO1xuXG4gICAgbGV0IGJsb2NrOiBDc3NCbG9ja0FzdDtcbiAgICBjb25zdCB0eXBlID0gdGhpcy5fcmVzb2x2ZUJsb2NrVHlwZSh0b2tlbik7XG4gICAgbGV0IHNwYW46IFBhcnNlU291cmNlU3BhbjtcbiAgICBsZXQgdG9rZW5zOiBDc3NUb2tlbltdO1xuICAgIGxldCBlbmRUb2tlbjogQ3NzVG9rZW47XG4gICAgbGV0IGVuZDogbnVtYmVyO1xuICAgIGxldCBzdHJWYWx1ZTogc3RyaW5nO1xuICAgIGxldCBxdWVyeTogQ3NzQXRSdWxlUHJlZGljYXRlQXN0O1xuICAgIHN3aXRjaCAodHlwZSkge1xuICAgICAgY2FzZSBCbG9ja1R5cGUuQ2hhcnNldDpcbiAgICAgIGNhc2UgQmxvY2tUeXBlLk5hbWVzcGFjZTpcbiAgICAgIGNhc2UgQmxvY2tUeXBlLkltcG9ydDpcbiAgICAgICAgbGV0IHZhbHVlID0gdGhpcy5fcGFyc2VWYWx1ZShkZWxpbWl0ZXJzKTtcbiAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5CTE9DSyk7XG4gICAgICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIHZhbHVlKTtcbiAgICAgICAgcmV0dXJuIG5ldyBDc3NJbmxpbmVSdWxlQXN0KHNwYW4sIHR5cGUsIHZhbHVlKTtcblxuICAgICAgY2FzZSBCbG9ja1R5cGUuVmlld3BvcnQ6XG4gICAgICBjYXNlIEJsb2NrVHlwZS5Gb250RmFjZTpcbiAgICAgICAgYmxvY2sgPSB0aGlzLl9wYXJzZVN0eWxlQmxvY2soZGVsaW1pdGVycykgITtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBibG9jayk7XG4gICAgICAgIHJldHVybiBuZXcgQ3NzQmxvY2tSdWxlQXN0KHNwYW4sIHR5cGUsIGJsb2NrKTtcblxuICAgICAgY2FzZSBCbG9ja1R5cGUuS2V5ZnJhbWVzOlxuICAgICAgICB0b2tlbnMgPSB0aGlzLl9jb2xsZWN0VW50aWxEZWxpbShkZWxpbWl0ZXJzIHwgUkJSQUNFX0RFTElNX0ZMQUcgfCBMQlJBQ0VfREVMSU1fRkxBRyk7XG4gICAgICAgIC8vIGtleWZyYW1lcyBvbmx5IGhhdmUgb25lIGlkZW50aWZpZXIgbmFtZVxuICAgICAgICBsZXQgbmFtZSA9IHRva2Vuc1swXTtcbiAgICAgICAgYmxvY2sgPSB0aGlzLl9wYXJzZUtleWZyYW1lQmxvY2soZGVsaW1pdGVycyk7XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgYmxvY2spO1xuICAgICAgICByZXR1cm4gbmV3IENzc0tleWZyYW1lUnVsZUFzdChzcGFuLCBuYW1lLCBibG9jayk7XG5cbiAgICAgIGNhc2UgQmxvY2tUeXBlLk1lZGlhUXVlcnk6XG4gICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuTUVESUFfUVVFUlkpO1xuICAgICAgICB0b2tlbnMgPSB0aGlzLl9jb2xsZWN0VW50aWxEZWxpbShkZWxpbWl0ZXJzIHwgUkJSQUNFX0RFTElNX0ZMQUcgfCBMQlJBQ0VfREVMSU1fRkxBRyk7XG4gICAgICAgIGVuZFRva2VuID0gdG9rZW5zW3Rva2Vucy5sZW5ndGggLSAxXTtcbiAgICAgICAgLy8gd2UgZG8gbm90IHRyYWNrIHRoZSB3aGl0ZXNwYWNlIGFmdGVyIHRoZSBtZWRpYVF1ZXJ5IHByZWRpY2F0ZSBlbmRzXG4gICAgICAgIC8vIHNvIHdlIGhhdmUgdG8gY2FsY3VsYXRlIHRoZSBlbmQgc3RyaW5nIHZhbHVlIG9uIG91ciBvd25cbiAgICAgICAgZW5kID0gZW5kVG9rZW4uaW5kZXggKyBlbmRUb2tlbi5zdHJWYWx1ZS5sZW5ndGggLSAxO1xuICAgICAgICBzdHJWYWx1ZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCBlbmQpO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGVuZFRva2VuKTtcbiAgICAgICAgcXVlcnkgPSBuZXcgQ3NzQXRSdWxlUHJlZGljYXRlQXN0KHNwYW4sIHN0clZhbHVlLCB0b2tlbnMpO1xuICAgICAgICBibG9jayA9IHRoaXMuX3BhcnNlQmxvY2soZGVsaW1pdGVycyk7XG4gICAgICAgIHN0clZhbHVlID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIHRoaXMuX2dldFNjYW5uZXJJbmRleCgpIC0gMSk7XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgYmxvY2spO1xuICAgICAgICByZXR1cm4gbmV3IENzc01lZGlhUXVlcnlSdWxlQXN0KHNwYW4sIHN0clZhbHVlLCBxdWVyeSwgYmxvY2spO1xuXG4gICAgICBjYXNlIEJsb2NrVHlwZS5Eb2N1bWVudDpcbiAgICAgIGNhc2UgQmxvY2tUeXBlLlN1cHBvcnRzOlxuICAgICAgY2FzZSBCbG9ja1R5cGUuUGFnZTpcbiAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5BVF9SVUxFX1FVRVJZKTtcbiAgICAgICAgdG9rZW5zID0gdGhpcy5fY29sbGVjdFVudGlsRGVsaW0oZGVsaW1pdGVycyB8IFJCUkFDRV9ERUxJTV9GTEFHIHwgTEJSQUNFX0RFTElNX0ZMQUcpO1xuICAgICAgICBlbmRUb2tlbiA9IHRva2Vuc1t0b2tlbnMubGVuZ3RoIC0gMV07XG4gICAgICAgIC8vIHdlIGRvIG5vdCB0cmFjayB0aGUgd2hpdGVzcGFjZSBhZnRlciB0aGlzIGJsb2NrIHJ1bGUgcHJlZGljYXRlIGVuZHNcbiAgICAgICAgLy8gc28gd2UgaGF2ZSB0byBjYWxjdWxhdGUgdGhlIGVuZCBzdHJpbmcgdmFsdWUgb24gb3VyIG93blxuICAgICAgICBlbmQgPSBlbmRUb2tlbi5pbmRleCArIGVuZFRva2VuLnN0clZhbHVlLmxlbmd0aCAtIDE7XG4gICAgICAgIHN0clZhbHVlID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIGVuZCk7XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgdG9rZW5zW3Rva2Vucy5sZW5ndGggLSAxXSk7XG4gICAgICAgIHF1ZXJ5ID0gbmV3IENzc0F0UnVsZVByZWRpY2F0ZUFzdChzcGFuLCBzdHJWYWx1ZSwgdG9rZW5zKTtcbiAgICAgICAgYmxvY2sgPSB0aGlzLl9wYXJzZUJsb2NrKGRlbGltaXRlcnMpO1xuICAgICAgICBzdHJWYWx1ZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCBibG9jay5lbmQub2Zmc2V0ICEpO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGJsb2NrKTtcbiAgICAgICAgcmV0dXJuIG5ldyBDc3NCbG9ja0RlZmluaXRpb25SdWxlQXN0KHNwYW4sIHN0clZhbHVlLCB0eXBlLCBxdWVyeSwgYmxvY2spO1xuXG4gICAgICAvLyBpZiBhIGN1c3RvbSBAcnVsZSB7IC4uLiB9IGlzIHVzZWQgaXQgc2hvdWxkIHN0aWxsIHRva2VuaXplIHRoZSBpbnNpZGVzXG4gICAgICBkZWZhdWx0OlxuICAgICAgICBsZXQgbGlzdE9mVG9rZW5zOiBDc3NUb2tlbltdID0gW107XG4gICAgICAgIGxldCB0b2tlbk5hbWUgPSB0b2tlbi5zdHJWYWx1ZTtcbiAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5BTEwpO1xuICAgICAgICB0aGlzLl9lcnJvcihcbiAgICAgICAgICAgIGdlbmVyYXRlRXJyb3JNZXNzYWdlKFxuICAgICAgICAgICAgICAgIHRoaXMuX2dldFNvdXJjZUNvbnRlbnQoKSxcbiAgICAgICAgICAgICAgICBgVGhlIENTUyBcImF0XCIgcnVsZSBcIiR7dG9rZW5OYW1lfVwiIGlzIG5vdCBhbGxvd2VkIHRvIHVzZWQgaGVyZWAsIHRva2VuLnN0clZhbHVlLFxuICAgICAgICAgICAgICAgIHRva2VuLmluZGV4LCB0b2tlbi5saW5lLCB0b2tlbi5jb2x1bW4pLFxuICAgICAgICAgICAgdG9rZW4pO1xuXG4gICAgICAgIHRoaXMuX2NvbGxlY3RVbnRpbERlbGltKGRlbGltaXRlcnMgfCBMQlJBQ0VfREVMSU1fRkxBRyB8IFNFTUlDT0xPTl9ERUxJTV9GTEFHKVxuICAgICAgICAgICAgLmZvckVhY2goKHRva2VuKSA9PiB7IGxpc3RPZlRva2Vucy5wdXNoKHRva2VuKTsgfSk7XG4gICAgICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgPT0gY2hhcnMuJExCUkFDRSkge1xuICAgICAgICAgIGxpc3RPZlRva2Vucy5wdXNoKHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ3snKSk7XG4gICAgICAgICAgdGhpcy5fY29sbGVjdFVudGlsRGVsaW0oZGVsaW1pdGVycyB8IFJCUkFDRV9ERUxJTV9GTEFHIHwgTEJSQUNFX0RFTElNX0ZMQUcpXG4gICAgICAgICAgICAgIC5mb3JFYWNoKCh0b2tlbikgPT4geyBsaXN0T2ZUb2tlbnMucHVzaCh0b2tlbik7IH0pO1xuICAgICAgICAgIGxpc3RPZlRva2Vucy5wdXNoKHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ30nKSk7XG4gICAgICAgIH1cbiAgICAgICAgZW5kVG9rZW4gPSBsaXN0T2ZUb2tlbnNbbGlzdE9mVG9rZW5zLmxlbmd0aCAtIDFdO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGVuZFRva2VuKTtcbiAgICAgICAgcmV0dXJuIG5ldyBDc3NVbmtub3duUnVsZUFzdChzcGFuLCB0b2tlbk5hbWUsIGxpc3RPZlRva2Vucyk7XG4gICAgfVxuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VTZWxlY3RvclJ1bGUoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzUnVsZUFzdCB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKTtcbiAgICBjb25zdCBzZWxlY3RvcnMgPSB0aGlzLl9wYXJzZVNlbGVjdG9ycyhkZWxpbWl0ZXJzKTtcbiAgICBjb25zdCBibG9jayA9IHRoaXMuX3BhcnNlU3R5bGVCbG9jayhkZWxpbWl0ZXJzKTtcbiAgICBsZXQgcnVsZUFzdDogQ3NzUnVsZUFzdDtcbiAgICBsZXQgc3BhbjogUGFyc2VTb3VyY2VTcGFuO1xuICAgIGNvbnN0IHN0YXJ0U2VsZWN0b3IgPSBzZWxlY3RvcnNbMF07XG4gICAgaWYgKGJsb2NrICE9IG51bGwpIHtcbiAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRTZWxlY3RvciwgYmxvY2spO1xuICAgICAgcnVsZUFzdCA9IG5ldyBDc3NTZWxlY3RvclJ1bGVBc3Qoc3Bhbiwgc2VsZWN0b3JzLCBibG9jayk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNvbnN0IG5hbWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCkgLSAxKTtcbiAgICAgIGNvbnN0IGlubmVyVG9rZW5zOiBDc3NUb2tlbltdID0gW107XG4gICAgICBzZWxlY3RvcnMuZm9yRWFjaCgoc2VsZWN0b3I6IENzc1NlbGVjdG9yQXN0KSA9PiB7XG4gICAgICAgIHNlbGVjdG9yLnNlbGVjdG9yUGFydHMuZm9yRWFjaCgocGFydDogQ3NzU2ltcGxlU2VsZWN0b3JBc3QpID0+IHtcbiAgICAgICAgICBwYXJ0LnRva2Vucy5mb3JFYWNoKCh0b2tlbjogQ3NzVG9rZW4pID0+IHsgaW5uZXJUb2tlbnMucHVzaCh0b2tlbik7IH0pO1xuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgICAgY29uc3QgZW5kVG9rZW4gPSBpbm5lclRva2Vuc1tpbm5lclRva2Vucy5sZW5ndGggLSAxXTtcbiAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRTZWxlY3RvciwgZW5kVG9rZW4pO1xuICAgICAgcnVsZUFzdCA9IG5ldyBDc3NVbmtub3duVG9rZW5MaXN0QXN0KHNwYW4sIG5hbWUsIGlubmVyVG9rZW5zKTtcbiAgICB9XG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5CTE9DSyk7XG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG4gICAgcmV0dXJuIHJ1bGVBc3Q7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVNlbGVjdG9ycyhkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NTZWxlY3RvckFzdFtdIHtcbiAgICBkZWxpbWl0ZXJzIHw9IExCUkFDRV9ERUxJTV9GTEFHIHwgU0VNSUNPTE9OX0RFTElNX0ZMQUc7XG5cbiAgICBjb25zdCBzZWxlY3RvcnM6IENzc1NlbGVjdG9yQXN0W10gPSBbXTtcbiAgICBsZXQgaXNQYXJzaW5nU2VsZWN0b3JzID0gdHJ1ZTtcbiAgICB3aGlsZSAoaXNQYXJzaW5nU2VsZWN0b3JzKSB7XG4gICAgICBzZWxlY3RvcnMucHVzaCh0aGlzLl9wYXJzZVNlbGVjdG9yKGRlbGltaXRlcnMpKTtcblxuICAgICAgaXNQYXJzaW5nU2VsZWN0b3JzID0gIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycyk7XG5cbiAgICAgIGlmIChpc1BhcnNpbmdTZWxlY3RvcnMpIHtcbiAgICAgICAgdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnLCcpO1xuICAgICAgICBpc1BhcnNpbmdTZWxlY3RvcnMgPSAhY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKTtcbiAgICAgICAgaWYgKGlzUGFyc2luZ1NlbGVjdG9ycykge1xuICAgICAgICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZVdoaXRlc3BhY2UoKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiBzZWxlY3RvcnM7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9zY2FuKCk6IENzc1Rva2VuIHtcbiAgICBjb25zdCBvdXRwdXQgPSB0aGlzLl9zY2FubmVyLnNjYW4oKSAhO1xuICAgIGNvbnN0IHRva2VuID0gb3V0cHV0LnRva2VuO1xuICAgIGNvbnN0IGVycm9yID0gb3V0cHV0LmVycm9yO1xuICAgIGlmIChlcnJvciAhPSBudWxsKSB7XG4gICAgICB0aGlzLl9lcnJvcihnZXRSYXdNZXNzYWdlKGVycm9yKSwgdG9rZW4pO1xuICAgIH1cbiAgICB0aGlzLl9sYXN0VG9rZW4gPSB0b2tlbjtcbiAgICByZXR1cm4gdG9rZW47XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9nZXRTY2FubmVySW5kZXgoKTogbnVtYmVyIHsgcmV0dXJuIHRoaXMuX3NjYW5uZXIuaW5kZXg7IH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9jb25zdW1lKHR5cGU6IENzc1Rva2VuVHlwZSwgdmFsdWU6IHN0cmluZ3xudWxsID0gbnVsbCk6IENzc1Rva2VuIHtcbiAgICBjb25zdCBvdXRwdXQgPSB0aGlzLl9zY2FubmVyLmNvbnN1bWUodHlwZSwgdmFsdWUpO1xuICAgIGNvbnN0IHRva2VuID0gb3V0cHV0LnRva2VuO1xuICAgIGNvbnN0IGVycm9yID0gb3V0cHV0LmVycm9yO1xuICAgIGlmIChlcnJvciAhPSBudWxsKSB7XG4gICAgICB0aGlzLl9lcnJvcihnZXRSYXdNZXNzYWdlKGVycm9yKSwgdG9rZW4pO1xuICAgIH1cbiAgICB0aGlzLl9sYXN0VG9rZW4gPSB0b2tlbjtcbiAgICByZXR1cm4gdG9rZW47XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZUtleWZyYW1lQmxvY2soZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzQmxvY2tBc3Qge1xuICAgIGRlbGltaXRlcnMgfD0gUkJSQUNFX0RFTElNX0ZMQUc7XG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5LRVlGUkFNRV9CTE9DSyk7XG5cbiAgICBjb25zdCBzdGFydFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAneycpO1xuXG4gICAgY29uc3QgZGVmaW5pdGlvbnM6IENzc0tleWZyYW1lRGVmaW5pdGlvbkFzdFtdID0gW107XG4gICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICBkZWZpbml0aW9ucy5wdXNoKHRoaXMuX3BhcnNlS2V5ZnJhbWVEZWZpbml0aW9uKGRlbGltaXRlcnMpKTtcbiAgICB9XG5cbiAgICBjb25zdCBlbmRUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ30nKTtcblxuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgZW5kVG9rZW4pO1xuICAgIHJldHVybiBuZXcgQ3NzQmxvY2tBc3Qoc3BhbiwgZGVmaW5pdGlvbnMpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VLZXlmcmFtZURlZmluaXRpb24oZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzS2V5ZnJhbWVEZWZpbml0aW9uQXN0IHtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpO1xuICAgIGNvbnN0IHN0ZXBUb2tlbnM6IENzc1Rva2VuW10gPSBbXTtcbiAgICBkZWxpbWl0ZXJzIHw9IExCUkFDRV9ERUxJTV9GTEFHO1xuICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgc3RlcFRva2Vucy5wdXNoKHRoaXMuX3BhcnNlS2V5ZnJhbWVMYWJlbChkZWxpbWl0ZXJzIHwgQ09NTUFfREVMSU1fRkxBRykpO1xuICAgICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayAhPSBjaGFycy4kTEJSQUNFKSB7XG4gICAgICAgIHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJywnKTtcbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3Qgc3R5bGVzQmxvY2sgPSB0aGlzLl9wYXJzZVN0eWxlQmxvY2soZGVsaW1pdGVycyB8IFJCUkFDRV9ERUxJTV9GTEFHKTtcbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0ZXBUb2tlbnNbMF0sIHN0eWxlc0Jsb2NrKTtcbiAgICBjb25zdCBhc3QgPSBuZXcgQ3NzS2V5ZnJhbWVEZWZpbml0aW9uQXN0KHNwYW4sIHN0ZXBUb2tlbnMsIHN0eWxlc0Jsb2NrICEpO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5CTE9DSyk7XG4gICAgcmV0dXJuIGFzdDtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlS2V5ZnJhbWVMYWJlbChkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NUb2tlbiB7XG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5LRVlGUkFNRV9CTE9DSyk7XG4gICAgcmV0dXJuIG1lcmdlVG9rZW5zKHRoaXMuX2NvbGxlY3RVbnRpbERlbGltKGRlbGltaXRlcnMpKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlUHNldWRvU2VsZWN0b3IoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzUHNldWRvU2VsZWN0b3JBc3Qge1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCk7XG5cbiAgICBkZWxpbWl0ZXJzICY9IH5DT01NQV9ERUxJTV9GTEFHO1xuXG4gICAgLy8gd2Uga2VlcCB0aGUgb3JpZ2luYWwgdmFsdWUgc2luY2Ugd2UgbWF5IHVzZSBpdCB0byByZWN1cnNlIHdoZW4gOm5vdCwgOmhvc3QgYXJlIHVzZWRcbiAgICBjb25zdCBzdGFydGluZ0RlbGltcyA9IGRlbGltaXRlcnM7XG5cbiAgICBjb25zdCBzdGFydFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnOicpO1xuICAgIGNvbnN0IHRva2VucyA9IFtzdGFydFRva2VuXTtcblxuICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgPT0gY2hhcnMuJENPTE9OKSB7ICAvLyA6OnNvbWV0aGluZ1xuICAgICAgdG9rZW5zLnB1c2godGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnOicpKTtcbiAgICB9XG5cbiAgICBjb25zdCBpbm5lclNlbGVjdG9yczogQ3NzU2VsZWN0b3JBc3RbXSA9IFtdO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5QU0VVRE9fU0VMRUNUT1IpO1xuXG4gICAgLy8gaG9zdCwgaG9zdC1jb250ZXh0LCBsYW5nLCBub3QsIG50aC1jaGlsZCBhcmUgYWxsIGlkZW50aWZpZXJzXG4gICAgY29uc3QgcHNldWRvU2VsZWN0b3JUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIpO1xuICAgIGNvbnN0IHBzZXVkb1NlbGVjdG9yTmFtZSA9IHBzZXVkb1NlbGVjdG9yVG9rZW4uc3RyVmFsdWU7XG4gICAgdG9rZW5zLnB1c2gocHNldWRvU2VsZWN0b3JUb2tlbik7XG5cbiAgICAvLyBob3N0KCksIGxhbmcoKSwgbnRoLWNoaWxkKCksIGV0Yy4uLlxuICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgPT0gY2hhcnMuJExQQVJFTikge1xuICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5QU0VVRE9fU0VMRUNUT1JfV0lUSF9BUkdVTUVOVFMpO1xuXG4gICAgICBjb25zdCBvcGVuUGFyZW5Ub2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJygnKTtcbiAgICAgIHRva2Vucy5wdXNoKG9wZW5QYXJlblRva2VuKTtcblxuICAgICAgLy8gOmhvc3QoaW5uZXJTZWxlY3RvcihzKSksIDpub3Qoc2VsZWN0b3IpLCBldGMuLi5cbiAgICAgIGlmIChfcHNldWRvU2VsZWN0b3JTdXBwb3J0c0lubmVyU2VsZWN0b3JzKHBzZXVkb1NlbGVjdG9yTmFtZSkpIHtcbiAgICAgICAgbGV0IGlubmVyRGVsaW1zID0gc3RhcnRpbmdEZWxpbXMgfCBMUEFSRU5fREVMSU1fRkxBRyB8IFJQQVJFTl9ERUxJTV9GTEFHO1xuICAgICAgICBpZiAocHNldWRvU2VsZWN0b3JOYW1lID09ICdub3QnKSB7XG4gICAgICAgICAgLy8gdGhlIGlubmVyIHNlbGVjdG9yIGluc2lkZSBvZiA6bm90KC4uLikgY2FuIG9ubHkgYmUgb25lXG4gICAgICAgICAgLy8gQ1NTIHNlbGVjdG9yIChubyBjb21tYXMgYWxsb3dlZCkgLi4uIFRoaXMgaXMgYWNjb3JkaW5nXG4gICAgICAgICAgLy8gdG8gdGhlIENTUyBzcGVjaWZpY2F0aW9uXG4gICAgICAgICAgaW5uZXJEZWxpbXMgfD0gQ09NTUFfREVMSU1fRkxBRztcbiAgICAgICAgfVxuXG4gICAgICAgIC8vIDpob3N0KGEsIGIsIGMpIHtcbiAgICAgICAgdGhpcy5fcGFyc2VTZWxlY3RvcnMoaW5uZXJEZWxpbXMpLmZvckVhY2goKHNlbGVjdG9yLCBpbmRleCkgPT4ge1xuICAgICAgICAgIGlubmVyU2VsZWN0b3JzLnB1c2goc2VsZWN0b3IpO1xuICAgICAgICB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIC8vIHRoaXMgYnJhbmNoIGlzIGZvciB0aGluZ3MgbGlrZSBcImVuLXVzLCAyayArIDEsIGV0Yy4uLlwiXG4gICAgICAgIC8vIHdoaWNoIGFsbCBlbmQgdXAgaW4gcHNldWRvU2VsZWN0b3JzIGxpa2UgOmxhbmcsIDpudGgtY2hpbGQsIGV0Yy4uXG4gICAgICAgIGNvbnN0IGlubmVyVmFsdWVEZWxpbXMgPSBkZWxpbWl0ZXJzIHwgTEJSQUNFX0RFTElNX0ZMQUcgfCBDT0xPTl9ERUxJTV9GTEFHIHxcbiAgICAgICAgICAgIFJQQVJFTl9ERUxJTV9GTEFHIHwgTFBBUkVOX0RFTElNX0ZMQUc7XG4gICAgICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBpbm5lclZhbHVlRGVsaW1zKSkge1xuICAgICAgICAgIGNvbnN0IHRva2VuID0gdGhpcy5fc2NhbigpO1xuICAgICAgICAgIHRva2Vucy5wdXNoKHRva2VuKTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjb25zdCBjbG9zZVBhcmVuVG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICcpJyk7XG4gICAgICB0b2tlbnMucHVzaChjbG9zZVBhcmVuVG9rZW4pO1xuICAgIH1cblxuICAgIGNvbnN0IGVuZCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpIC0gMTtcbiAgICBjb25zdCBzdHJWYWx1ZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCBlbmQpO1xuXG4gICAgY29uc3QgZW5kVG9rZW4gPSB0b2tlbnNbdG9rZW5zLmxlbmd0aCAtIDFdO1xuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgZW5kVG9rZW4pO1xuICAgIHJldHVybiBuZXcgQ3NzUHNldWRvU2VsZWN0b3JBc3Qoc3Bhbiwgc3RyVmFsdWUsIHBzZXVkb1NlbGVjdG9yTmFtZSwgdG9rZW5zLCBpbm5lclNlbGVjdG9ycyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVNpbXBsZVNlbGVjdG9yKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1NpbXBsZVNlbGVjdG9yQXN0IHtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpO1xuXG4gICAgZGVsaW1pdGVycyB8PSBDT01NQV9ERUxJTV9GTEFHO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TRUxFQ1RPUik7XG4gICAgY29uc3Qgc2VsZWN0b3JDc3NUb2tlbnM6IENzc1Rva2VuW10gPSBbXTtcbiAgICBjb25zdCBwc2V1ZG9TZWxlY3RvcnM6IENzc1BzZXVkb1NlbGVjdG9yQXN0W10gPSBbXTtcblxuICAgIGxldCBwcmV2aW91c1Rva2VuOiBDc3NUb2tlbiA9IHVuZGVmaW5lZCAhO1xuXG4gICAgY29uc3Qgc2VsZWN0b3JQYXJ0RGVsaW1pdGVycyA9IGRlbGltaXRlcnMgfCBTUEFDRV9ERUxJTV9GTEFHO1xuICAgIGxldCBsb29wT3ZlclNlbGVjdG9yID0gIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgc2VsZWN0b3JQYXJ0RGVsaW1pdGVycyk7XG5cbiAgICBsZXQgaGFzQXR0cmlidXRlRXJyb3IgPSBmYWxzZTtcbiAgICB3aGlsZSAobG9vcE92ZXJTZWxlY3Rvcikge1xuICAgICAgY29uc3QgcGVlayA9IHRoaXMuX3NjYW5uZXIucGVlaztcblxuICAgICAgc3dpdGNoIChwZWVrKSB7XG4gICAgICAgIGNhc2UgY2hhcnMuJENPTE9OOlxuICAgICAgICAgIGxldCBpbm5lclBzZXVkbyA9IHRoaXMuX3BhcnNlUHNldWRvU2VsZWN0b3IoZGVsaW1pdGVycyk7XG4gICAgICAgICAgcHNldWRvU2VsZWN0b3JzLnB1c2goaW5uZXJQc2V1ZG8pO1xuICAgICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU0VMRUNUT1IpO1xuICAgICAgICAgIGJyZWFrO1xuXG4gICAgICAgIGNhc2UgY2hhcnMuJExCUkFDS0VUOlxuICAgICAgICAgIC8vIHdlIHNldCB0aGUgbW9kZSBhZnRlciB0aGUgc2NhbiBiZWNhdXNlIGF0dHJpYnV0ZSBtb2RlIGRvZXMgbm90XG4gICAgICAgICAgLy8gYWxsb3cgYXR0cmlidXRlIFtdIHZhbHVlcy4gQW5kIHRoaXMgYWxzbyB3aWxsIGNhdGNoIGFueSBlcnJvcnNcbiAgICAgICAgICAvLyBpZiBhbiBleHRyYSBcIltcIiBpcyB1c2VkIGluc2lkZS5cbiAgICAgICAgICBzZWxlY3RvckNzc1Rva2Vucy5wdXNoKHRoaXMuX3NjYW4oKSk7XG4gICAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5BVFRSSUJVVEVfU0VMRUNUT1IpO1xuICAgICAgICAgIGJyZWFrO1xuXG4gICAgICAgIGNhc2UgY2hhcnMuJFJCUkFDS0VUOlxuICAgICAgICAgIGlmICh0aGlzLl9zY2FubmVyLmdldE1vZGUoKSAhPSBDc3NMZXhlck1vZGUuQVRUUklCVVRFX1NFTEVDVE9SKSB7XG4gICAgICAgICAgICBoYXNBdHRyaWJ1dGVFcnJvciA9IHRydWU7XG4gICAgICAgICAgfVxuICAgICAgICAgIC8vIHdlIHNldCB0aGUgbW9kZSBlYXJseSBiZWNhdXNlIGF0dHJpYnV0ZSBtb2RlIGRvZXMgbm90XG4gICAgICAgICAgLy8gYWxsb3cgYXR0cmlidXRlIFtdIHZhbHVlc1xuICAgICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU0VMRUNUT1IpO1xuICAgICAgICAgIHNlbGVjdG9yQ3NzVG9rZW5zLnB1c2godGhpcy5fc2NhbigpKTtcbiAgICAgICAgICBicmVhaztcblxuICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgIGlmIChpc1NlbGVjdG9yT3BlcmF0b3JDaGFyYWN0ZXIocGVlaykpIHtcbiAgICAgICAgICAgIGxvb3BPdmVyU2VsZWN0b3IgPSBmYWxzZTtcbiAgICAgICAgICAgIGNvbnRpbnVlO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIGxldCB0b2tlbiA9IHRoaXMuX3NjYW4oKTtcbiAgICAgICAgICBwcmV2aW91c1Rva2VuID0gdG9rZW47XG4gICAgICAgICAgc2VsZWN0b3JDc3NUb2tlbnMucHVzaCh0b2tlbik7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICB9XG5cbiAgICAgIGxvb3BPdmVyU2VsZWN0b3IgPSAhY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBzZWxlY3RvclBhcnREZWxpbWl0ZXJzKTtcbiAgICB9XG5cbiAgICBoYXNBdHRyaWJ1dGVFcnJvciA9XG4gICAgICAgIGhhc0F0dHJpYnV0ZUVycm9yIHx8IHRoaXMuX3NjYW5uZXIuZ2V0TW9kZSgpID09IENzc0xleGVyTW9kZS5BVFRSSUJVVEVfU0VMRUNUT1I7XG4gICAgaWYgKGhhc0F0dHJpYnV0ZUVycm9yKSB7XG4gICAgICB0aGlzLl9lcnJvcihcbiAgICAgICAgICBgVW5iYWxhbmNlZCBDU1MgYXR0cmlidXRlIHNlbGVjdG9yIGF0IGNvbHVtbiAke3ByZXZpb3VzVG9rZW4ubGluZX06JHtwcmV2aW91c1Rva2VuLmNvbHVtbn1gLFxuICAgICAgICAgIHByZXZpb3VzVG9rZW4pO1xuICAgIH1cblxuICAgIGxldCBlbmQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKSAtIDE7XG5cbiAgICAvLyB0aGlzIGhhcHBlbnMgaWYgdGhlIHNlbGVjdG9yIGlzIG5vdCBkaXJlY3RseSBmb2xsb3dlZCBieVxuICAgIC8vIGEgY29tbWEgb3IgY3VybHkgYnJhY2Ugd2l0aG91dCBhIHNwYWNlIGluIGJldHdlZW5cbiAgICBsZXQgb3BlcmF0b3I6IENzc1Rva2VufG51bGwgPSBudWxsO1xuICAgIGxldCBvcGVyYXRvclNjYW5Db3VudCA9IDA7XG4gICAgbGV0IGxhc3RPcGVyYXRvclRva2VuOiBDc3NUb2tlbnxudWxsID0gbnVsbDtcbiAgICBpZiAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIHdoaWxlIChvcGVyYXRvciA9PSBudWxsICYmICFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpICYmXG4gICAgICAgICAgICAgaXNTZWxlY3Rvck9wZXJhdG9yQ2hhcmFjdGVyKHRoaXMuX3NjYW5uZXIucGVlaykpIHtcbiAgICAgICAgbGV0IHRva2VuID0gdGhpcy5fc2NhbigpO1xuICAgICAgICBjb25zdCB0b2tlbk9wZXJhdG9yID0gdG9rZW4uc3RyVmFsdWU7XG4gICAgICAgIG9wZXJhdG9yU2NhbkNvdW50Kys7XG4gICAgICAgIGxhc3RPcGVyYXRvclRva2VuID0gdG9rZW47XG4gICAgICAgIGlmICh0b2tlbk9wZXJhdG9yICE9IFNQQUNFX09QRVJBVE9SKSB7XG4gICAgICAgICAgc3dpdGNoICh0b2tlbk9wZXJhdG9yKSB7XG4gICAgICAgICAgICBjYXNlIFNMQVNIX0NIQVJBQ1RFUjpcbiAgICAgICAgICAgICAgLy8gL2RlZXAvIG9wZXJhdG9yXG4gICAgICAgICAgICAgIGxldCBkZWVwVG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5JZGVudGlmaWVyKTtcbiAgICAgICAgICAgICAgbGV0IGRlZXBTbGFzaCA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3Rlcik7XG4gICAgICAgICAgICAgIGxldCBpbmRleCA9IGxhc3RPcGVyYXRvclRva2VuLmluZGV4O1xuICAgICAgICAgICAgICBsZXQgbGluZSA9IGxhc3RPcGVyYXRvclRva2VuLmxpbmU7XG4gICAgICAgICAgICAgIGxldCBjb2x1bW4gPSBsYXN0T3BlcmF0b3JUb2tlbi5jb2x1bW47XG4gICAgICAgICAgICAgIGlmIChkZWVwVG9rZW4gIT0gbnVsbCAmJiBkZWVwVG9rZW4uc3RyVmFsdWUudG9Mb3dlckNhc2UoKSA9PSAnZGVlcCcgJiZcbiAgICAgICAgICAgICAgICAgIGRlZXBTbGFzaC5zdHJWYWx1ZSA9PSBTTEFTSF9DSEFSQUNURVIpIHtcbiAgICAgICAgICAgICAgICB0b2tlbiA9IG5ldyBDc3NUb2tlbihcbiAgICAgICAgICAgICAgICAgICAgbGFzdE9wZXJhdG9yVG9rZW4uaW5kZXgsIGxhc3RPcGVyYXRvclRva2VuLmNvbHVtbiwgbGFzdE9wZXJhdG9yVG9rZW4ubGluZSxcbiAgICAgICAgICAgICAgICAgICAgQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIsIERFRVBfT1BFUkFUT1JfU1RSKTtcbiAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICBjb25zdCB0ZXh0ID0gU0xBU0hfQ0hBUkFDVEVSICsgZGVlcFRva2VuLnN0clZhbHVlICsgZGVlcFNsYXNoLnN0clZhbHVlO1xuICAgICAgICAgICAgICAgIHRoaXMuX2Vycm9yKFxuICAgICAgICAgICAgICAgICAgICBnZW5lcmF0ZUVycm9yTWVzc2FnZShcbiAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMuX2dldFNvdXJjZUNvbnRlbnQoKSwgYCR7dGV4dH0gaXMgYW4gaW52YWxpZCBDU1Mgb3BlcmF0b3JgLCB0ZXh0LCBpbmRleCxcbiAgICAgICAgICAgICAgICAgICAgICAgIGxpbmUsIGNvbHVtbiksXG4gICAgICAgICAgICAgICAgICAgIGxhc3RPcGVyYXRvclRva2VuKTtcbiAgICAgICAgICAgICAgICB0b2tlbiA9IG5ldyBDc3NUb2tlbihpbmRleCwgY29sdW1uLCBsaW5lLCBDc3NUb2tlblR5cGUuSW52YWxpZCwgdGV4dCk7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgYnJlYWs7XG5cbiAgICAgICAgICAgIGNhc2UgR1RfQ0hBUkFDVEVSOlxuICAgICAgICAgICAgICAvLyA+Pj4gb3BlcmF0b3JcbiAgICAgICAgICAgICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayA9PSBjaGFycy4kR1QgJiYgdGhpcy5fc2Nhbm5lci5wZWVrUGVlayA9PSBjaGFycy4kR1QpIHtcbiAgICAgICAgICAgICAgICB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsIEdUX0NIQVJBQ1RFUik7XG4gICAgICAgICAgICAgICAgdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCBHVF9DSEFSQUNURVIpO1xuICAgICAgICAgICAgICAgIHRva2VuID0gbmV3IENzc1Rva2VuKFxuICAgICAgICAgICAgICAgICAgICBsYXN0T3BlcmF0b3JUb2tlbi5pbmRleCwgbGFzdE9wZXJhdG9yVG9rZW4uY29sdW1uLCBsYXN0T3BlcmF0b3JUb2tlbi5saW5lLFxuICAgICAgICAgICAgICAgICAgICBDc3NUb2tlblR5cGUuSWRlbnRpZmllciwgVFJJUExFX0dUX09QRVJBVE9SX1NUUik7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgfVxuXG4gICAgICAgICAgb3BlcmF0b3IgPSB0b2tlbjtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICAvLyBzbyBsb25nIGFzIHRoZXJlIGlzIGFuIG9wZXJhdG9yIHRoZW4gd2UgY2FuIGhhdmUgYW5cbiAgICAgIC8vIGVuZGluZyB2YWx1ZSB0aGF0IGlzIGJleW9uZCB0aGUgc2VsZWN0b3IgdmFsdWUgLi4uXG4gICAgICAvLyBvdGhlcndpc2UgaXQncyBqdXN0IGEgYnVuY2ggb2YgdHJhaWxpbmcgd2hpdGVzcGFjZVxuICAgICAgaWYgKG9wZXJhdG9yICE9IG51bGwpIHtcbiAgICAgICAgZW5kID0gb3BlcmF0b3IuaW5kZXg7XG4gICAgICB9XG4gICAgfVxuXG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lV2hpdGVzcGFjZSgpO1xuXG4gICAgY29uc3Qgc3RyVmFsdWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgZW5kKTtcblxuICAgIC8vIGlmIHdlIGRvIGNvbWUgYWNyb3NzIG9uZSBvciBtb3JlIHNwYWNlcyBpbnNpZGUgb2ZcbiAgICAvLyB0aGUgb3BlcmF0b3JzIGxvb3AgdGhlbiBhbiBlbXB0eSBzcGFjZSBpcyBzdGlsbCBhXG4gICAgLy8gdmFsaWQgb3BlcmF0b3IgdG8gdXNlIGlmIHNvbWV0aGluZyBlbHNlIHdhcyBub3QgZm91bmRcbiAgICBpZiAob3BlcmF0b3IgPT0gbnVsbCAmJiBvcGVyYXRvclNjYW5Db3VudCA+IDAgJiYgdGhpcy5fc2Nhbm5lci5wZWVrICE9IGNoYXJzLiRMQlJBQ0UpIHtcbiAgICAgIG9wZXJhdG9yID0gbGFzdE9wZXJhdG9yVG9rZW47XG4gICAgfVxuXG4gICAgLy8gcGxlYXNlIG5vdGUgdGhhdCBgZW5kVG9rZW5gIGlzIHJlYXNzaWduZWQgbXVsdGlwbGUgdGltZXMgYmVsb3dcbiAgICAvLyBzbyBwbGVhc2UgZG8gbm90IG9wdGltaXplIHRoZSBpZiBzdGF0ZW1lbnRzIGludG8gaWYvZWxzZWlmXG4gICAgbGV0IHN0YXJ0VG9rZW5PckFzdDogQ3NzVG9rZW58Q3NzQXN0fG51bGwgPSBudWxsO1xuICAgIGxldCBlbmRUb2tlbk9yQXN0OiBDc3NUb2tlbnxDc3NBc3R8bnVsbCA9IG51bGw7XG4gICAgaWYgKHNlbGVjdG9yQ3NzVG9rZW5zLmxlbmd0aCA+IDApIHtcbiAgICAgIHN0YXJ0VG9rZW5PckFzdCA9IHN0YXJ0VG9rZW5PckFzdCB8fCBzZWxlY3RvckNzc1Rva2Vuc1swXTtcbiAgICAgIGVuZFRva2VuT3JBc3QgPSBzZWxlY3RvckNzc1Rva2Vuc1tzZWxlY3RvckNzc1Rva2Vucy5sZW5ndGggLSAxXTtcbiAgICB9XG4gICAgaWYgKHBzZXVkb1NlbGVjdG9ycy5sZW5ndGggPiAwKSB7XG4gICAgICBzdGFydFRva2VuT3JBc3QgPSBzdGFydFRva2VuT3JBc3QgfHwgcHNldWRvU2VsZWN0b3JzWzBdO1xuICAgICAgZW5kVG9rZW5PckFzdCA9IHBzZXVkb1NlbGVjdG9yc1twc2V1ZG9TZWxlY3RvcnMubGVuZ3RoIC0gMV07XG4gICAgfVxuICAgIGlmIChvcGVyYXRvciAhPSBudWxsKSB7XG4gICAgICBzdGFydFRva2VuT3JBc3QgPSBzdGFydFRva2VuT3JBc3QgfHwgb3BlcmF0b3I7XG4gICAgICBlbmRUb2tlbk9yQXN0ID0gb3BlcmF0b3I7XG4gICAgfVxuXG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuT3JBc3QgISwgZW5kVG9rZW5PckFzdCk7XG4gICAgcmV0dXJuIG5ldyBDc3NTaW1wbGVTZWxlY3RvckFzdChzcGFuLCBzZWxlY3RvckNzc1Rva2Vucywgc3RyVmFsdWUsIHBzZXVkb1NlbGVjdG9ycywgb3BlcmF0b3IgISk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVNlbGVjdG9yKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1NlbGVjdG9yQXN0IHtcbiAgICBkZWxpbWl0ZXJzIHw9IENPTU1BX0RFTElNX0ZMQUc7XG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TRUxFQ1RPUik7XG5cbiAgICBjb25zdCBzaW1wbGVTZWxlY3RvcnM6IENzc1NpbXBsZVNlbGVjdG9yQXN0W10gPSBbXTtcbiAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIHNpbXBsZVNlbGVjdG9ycy5wdXNoKHRoaXMuX3BhcnNlU2ltcGxlU2VsZWN0b3IoZGVsaW1pdGVycykpO1xuICAgICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lV2hpdGVzcGFjZSgpO1xuICAgIH1cblxuICAgIGNvbnN0IGZpcnN0U2VsZWN0b3IgPSBzaW1wbGVTZWxlY3RvcnNbMF07XG4gICAgY29uc3QgbGFzdFNlbGVjdG9yID0gc2ltcGxlU2VsZWN0b3JzW3NpbXBsZVNlbGVjdG9ycy5sZW5ndGggLSAxXTtcbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKGZpcnN0U2VsZWN0b3IsIGxhc3RTZWxlY3Rvcik7XG4gICAgcmV0dXJuIG5ldyBDc3NTZWxlY3RvckFzdChzcGFuLCBzaW1wbGVTZWxlY3RvcnMpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VWYWx1ZShkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NTdHlsZVZhbHVlQXN0IHtcbiAgICBkZWxpbWl0ZXJzIHw9IFJCUkFDRV9ERUxJTV9GTEFHIHwgU0VNSUNPTE9OX0RFTElNX0ZMQUcgfCBORVdMSU5FX0RFTElNX0ZMQUc7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNUWUxFX1ZBTFVFKTtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpO1xuXG4gICAgY29uc3QgdG9rZW5zOiBDc3NUb2tlbltdID0gW107XG4gICAgbGV0IHdzU3RyID0gJyc7XG4gICAgbGV0IHByZXZpb3VzOiBDc3NUb2tlbiA9IHVuZGVmaW5lZCAhO1xuICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgbGV0IHRva2VuOiBDc3NUb2tlbjtcbiAgICAgIGlmIChwcmV2aW91cyAhPSBudWxsICYmIHByZXZpb3VzLnR5cGUgPT0gQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIgJiZcbiAgICAgICAgICB0aGlzLl9zY2FubmVyLnBlZWsgPT0gY2hhcnMuJExQQVJFTikge1xuICAgICAgICB0b2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJygnKTtcbiAgICAgICAgdG9rZW5zLnB1c2godG9rZW4pO1xuXG4gICAgICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU1RZTEVfVkFMVUVfRlVOQ1RJT04pO1xuXG4gICAgICAgIHRva2VuID0gdGhpcy5fc2NhbigpO1xuICAgICAgICB0b2tlbnMucHVzaCh0b2tlbik7XG5cbiAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TVFlMRV9WQUxVRSk7XG5cbiAgICAgICAgdG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICcpJyk7XG4gICAgICAgIHRva2Vucy5wdXNoKHRva2VuKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRva2VuID0gdGhpcy5fc2NhbigpO1xuICAgICAgICBpZiAodG9rZW4udHlwZSA9PSBDc3NUb2tlblR5cGUuV2hpdGVzcGFjZSkge1xuICAgICAgICAgIHdzU3RyICs9IHRva2VuLnN0clZhbHVlO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHdzU3RyID0gJyc7XG4gICAgICAgICAgdG9rZW5zLnB1c2godG9rZW4pO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICBwcmV2aW91cyA9IHRva2VuO1xuICAgIH1cblxuICAgIGNvbnN0IGVuZCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpIC0gMTtcbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG5cbiAgICBjb25zdCBjb2RlID0gdGhpcy5fc2Nhbm5lci5wZWVrO1xuICAgIGlmIChjb2RlID09IGNoYXJzLiRTRU1JQ09MT04pIHtcbiAgICAgIHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJzsnKTtcbiAgICB9IGVsc2UgaWYgKGNvZGUgIT0gY2hhcnMuJFJCUkFDRSkge1xuICAgICAgdGhpcy5fZXJyb3IoXG4gICAgICAgICAgZ2VuZXJhdGVFcnJvck1lc3NhZ2UoXG4gICAgICAgICAgICAgIHRoaXMuX2dldFNvdXJjZUNvbnRlbnQoKSwgYFRoZSBDU1Mga2V5L3ZhbHVlIGRlZmluaXRpb24gZGlkIG5vdCBlbmQgd2l0aCBhIHNlbWljb2xvbmAsXG4gICAgICAgICAgICAgIHByZXZpb3VzLnN0clZhbHVlLCBwcmV2aW91cy5pbmRleCwgcHJldmlvdXMubGluZSwgcHJldmlvdXMuY29sdW1uKSxcbiAgICAgICAgICBwcmV2aW91cyk7XG4gICAgfVxuXG4gICAgY29uc3Qgc3RyVmFsdWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgZW5kKTtcbiAgICBjb25zdCBzdGFydFRva2VuID0gdG9rZW5zWzBdO1xuICAgIGNvbnN0IGVuZFRva2VuID0gdG9rZW5zW3Rva2Vucy5sZW5ndGggLSAxXTtcbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGVuZFRva2VuKTtcbiAgICByZXR1cm4gbmV3IENzc1N0eWxlVmFsdWVBc3Qoc3BhbiwgdG9rZW5zLCBzdHJWYWx1ZSk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9jb2xsZWN0VW50aWxEZWxpbShkZWxpbWl0ZXJzOiBudW1iZXIsIGFzc2VydFR5cGU6IENzc1Rva2VuVHlwZXxudWxsID0gbnVsbCk6IENzc1Rva2VuW10ge1xuICAgIGNvbnN0IHRva2VuczogQ3NzVG9rZW5bXSA9IFtdO1xuICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgY29uc3QgdmFsID0gYXNzZXJ0VHlwZSAhPSBudWxsID8gdGhpcy5fY29uc3VtZShhc3NlcnRUeXBlKSA6IHRoaXMuX3NjYW4oKTtcbiAgICAgIHRva2Vucy5wdXNoKHZhbCk7XG4gICAgfVxuICAgIHJldHVybiB0b2tlbnM7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZUJsb2NrKGRlbGltaXRlcnM6IG51bWJlcik6IENzc0Jsb2NrQXN0IHtcbiAgICBkZWxpbWl0ZXJzIHw9IFJCUkFDRV9ERUxJTV9GTEFHO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5CTE9DSyk7XG5cbiAgICBjb25zdCBzdGFydFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAneycpO1xuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuXG4gICAgY29uc3QgcmVzdWx0czogQ3NzUnVsZUFzdFtdID0gW107XG4gICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICByZXN1bHRzLnB1c2godGhpcy5fcGFyc2VSdWxlKGRlbGltaXRlcnMpKTtcbiAgICB9XG5cbiAgICBjb25zdCBlbmRUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ30nKTtcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuQkxPQ0spO1xuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuXG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBlbmRUb2tlbik7XG4gICAgcmV0dXJuIG5ldyBDc3NCbG9ja0FzdChzcGFuLCByZXN1bHRzKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlU3R5bGVCbG9jayhkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NTdHlsZXNCbG9ja0FzdHxudWxsIHtcbiAgICBkZWxpbWl0ZXJzIHw9IFJCUkFDRV9ERUxJTV9GTEFHIHwgTEJSQUNFX0RFTElNX0ZMQUc7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNUWUxFX0JMT0NLKTtcblxuICAgIGNvbnN0IHN0YXJ0VG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd7Jyk7XG4gICAgaWYgKHN0YXJ0VG9rZW4ubnVtVmFsdWUgIT0gY2hhcnMuJExCUkFDRSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgY29uc3QgZGVmaW5pdGlvbnM6IENzc0RlZmluaXRpb25Bc3RbXSA9IFtdO1xuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuXG4gICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICBkZWZpbml0aW9ucy5wdXNoKHRoaXMuX3BhcnNlRGVmaW5pdGlvbihkZWxpbWl0ZXJzKSk7XG4gICAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcbiAgICB9XG5cbiAgICBjb25zdCBlbmRUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ30nKTtcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU1RZTEVfQkxPQ0spO1xuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuXG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBlbmRUb2tlbik7XG4gICAgcmV0dXJuIG5ldyBDc3NTdHlsZXNCbG9ja0FzdChzcGFuLCBkZWZpbml0aW9ucyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZURlZmluaXRpb24oZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzRGVmaW5pdGlvbkFzdCB7XG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TVFlMRV9CTE9DSyk7XG5cbiAgICBsZXQgcHJvcCA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIpO1xuICAgIGxldCBwYXJzZVZhbHVlOiBib29sZWFuID0gZmFsc2U7XG4gICAgbGV0IHZhbHVlOiBDc3NTdHlsZVZhbHVlQXN0fG51bGwgPSBudWxsO1xuICAgIGxldCBlbmRUb2tlbjogQ3NzVG9rZW58Q3NzU3R5bGVWYWx1ZUFzdCA9IHByb3A7XG5cbiAgICAvLyB0aGUgY29sb24gdmFsdWUgc2VwYXJhdGVzIHRoZSBwcm9wIGZyb20gdGhlIHN0eWxlLlxuICAgIC8vIHRoZXJlIGFyZSBhIGZldyBjYXNlcyBhcyB0byB3aGF0IGNvdWxkIGhhcHBlbiBpZiBpdFxuICAgIC8vIGlzIG1pc3NpbmdcbiAgICBzd2l0Y2ggKHRoaXMuX3NjYW5uZXIucGVlaykge1xuICAgICAgY2FzZSBjaGFycy4kU0VNSUNPTE9OOlxuICAgICAgY2FzZSBjaGFycy4kUkJSQUNFOlxuICAgICAgY2FzZSBjaGFycy4kRU9GOlxuICAgICAgICBwYXJzZVZhbHVlID0gZmFsc2U7XG4gICAgICAgIGJyZWFrO1xuXG4gICAgICBkZWZhdWx0OlxuICAgICAgICBsZXQgcHJvcFN0ciA9IFtwcm9wLnN0clZhbHVlXTtcbiAgICAgICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayAhPSBjaGFycy4kQ09MT04pIHtcbiAgICAgICAgICAvLyB0aGlzIHdpbGwgdGhyb3cgdGhlIGVycm9yXG4gICAgICAgICAgY29uc3QgbmV4dFZhbHVlID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnOicpO1xuICAgICAgICAgIHByb3BTdHIucHVzaChuZXh0VmFsdWUuc3RyVmFsdWUpO1xuXG4gICAgICAgICAgY29uc3QgcmVtYWluaW5nVG9rZW5zID0gdGhpcy5fY29sbGVjdFVudGlsRGVsaW0oXG4gICAgICAgICAgICAgIGRlbGltaXRlcnMgfCBDT0xPTl9ERUxJTV9GTEFHIHwgU0VNSUNPTE9OX0RFTElNX0ZMQUcsIENzc1Rva2VuVHlwZS5JZGVudGlmaWVyKTtcbiAgICAgICAgICBpZiAocmVtYWluaW5nVG9rZW5zLmxlbmd0aCA+IDApIHtcbiAgICAgICAgICAgIHJlbWFpbmluZ1Rva2Vucy5mb3JFYWNoKCh0b2tlbikgPT4geyBwcm9wU3RyLnB1c2godG9rZW4uc3RyVmFsdWUpOyB9KTtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBlbmRUb2tlbiA9IHByb3AgPVxuICAgICAgICAgICAgICBuZXcgQ3NzVG9rZW4ocHJvcC5pbmRleCwgcHJvcC5jb2x1bW4sIHByb3AubGluZSwgcHJvcC50eXBlLCBwcm9wU3RyLmpvaW4oJyAnKSk7XG4gICAgICAgIH1cblxuICAgICAgICAvLyB0aGlzIG1lYW5zIHdlJ3ZlIHJlYWNoZWQgdGhlIGVuZCBvZiB0aGUgZGVmaW5pdGlvbiBhbmQvb3IgYmxvY2tcbiAgICAgICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayA9PSBjaGFycy4kQ09MT04pIHtcbiAgICAgICAgICB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICc6Jyk7XG4gICAgICAgICAgcGFyc2VWYWx1ZSA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgfVxuXG4gICAgaWYgKHBhcnNlVmFsdWUpIHtcbiAgICAgIHZhbHVlID0gdGhpcy5fcGFyc2VWYWx1ZShkZWxpbWl0ZXJzKTtcbiAgICAgIGVuZFRva2VuID0gdmFsdWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX2Vycm9yKFxuICAgICAgICAgIGdlbmVyYXRlRXJyb3JNZXNzYWdlKFxuICAgICAgICAgICAgICB0aGlzLl9nZXRTb3VyY2VDb250ZW50KCksIGBUaGUgQ1NTIHByb3BlcnR5IHdhcyBub3QgcGFpcmVkIHdpdGggYSBzdHlsZSB2YWx1ZWAsXG4gICAgICAgICAgICAgIHByb3Auc3RyVmFsdWUsIHByb3AuaW5kZXgsIHByb3AubGluZSwgcHJvcC5jb2x1bW4pLFxuICAgICAgICAgIHByb3ApO1xuICAgIH1cblxuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4ocHJvcCwgZW5kVG9rZW4pO1xuICAgIHJldHVybiBuZXcgQ3NzRGVmaW5pdGlvbkFzdChzcGFuLCBwcm9wLCB2YWx1ZSAhKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2Fzc2VydENvbmRpdGlvbihzdGF0dXM6IGJvb2xlYW4sIGVycm9yTWVzc2FnZTogc3RyaW5nLCBwcm9ibGVtVG9rZW46IENzc1Rva2VuKTogYm9vbGVhbiB7XG4gICAgaWYgKCFzdGF0dXMpIHtcbiAgICAgIHRoaXMuX2Vycm9yKGVycm9yTWVzc2FnZSwgcHJvYmxlbVRva2VuKTtcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIH1cbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9lcnJvcihtZXNzYWdlOiBzdHJpbmcsIHByb2JsZW1Ub2tlbjogQ3NzVG9rZW4pIHtcbiAgICBjb25zdCBsZW5ndGggPSBwcm9ibGVtVG9rZW4uc3RyVmFsdWUubGVuZ3RoO1xuICAgIGNvbnN0IGVycm9yID0gQ3NzUGFyc2VFcnJvci5jcmVhdGUoXG4gICAgICAgIHRoaXMuX2ZpbGUsIDAsIHByb2JsZW1Ub2tlbi5saW5lLCBwcm9ibGVtVG9rZW4uY29sdW1uLCBsZW5ndGgsIG1lc3NhZ2UpO1xuICAgIHRoaXMuX2Vycm9ycy5wdXNoKGVycm9yKTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzUGFyc2VFcnJvciBleHRlbmRzIFBhcnNlRXJyb3Ige1xuICBzdGF0aWMgY3JlYXRlKFxuICAgICAgZmlsZTogUGFyc2VTb3VyY2VGaWxlLCBvZmZzZXQ6IG51bWJlciwgbGluZTogbnVtYmVyLCBjb2w6IG51bWJlciwgbGVuZ3RoOiBudW1iZXIsXG4gICAgICBlcnJNc2c6IHN0cmluZyk6IENzc1BhcnNlRXJyb3Ige1xuICAgIGNvbnN0IHN0YXJ0ID0gbmV3IFBhcnNlTG9jYXRpb24oZmlsZSwgb2Zmc2V0LCBsaW5lLCBjb2wpO1xuICAgIGNvbnN0IGVuZCA9IG5ldyBQYXJzZUxvY2F0aW9uKGZpbGUsIG9mZnNldCwgbGluZSwgY29sICsgbGVuZ3RoKTtcbiAgICBjb25zdCBzcGFuID0gbmV3IFBhcnNlU291cmNlU3BhbihzdGFydCwgZW5kKTtcbiAgICByZXR1cm4gbmV3IENzc1BhcnNlRXJyb3Ioc3BhbiwgJ0NTUyBQYXJzZSBFcnJvcjogJyArIGVyck1zZyk7XG4gIH1cblxuICBjb25zdHJ1Y3RvcihzcGFuOiBQYXJzZVNvdXJjZVNwYW4sIG1lc3NhZ2U6IHN0cmluZykgeyBzdXBlcihzcGFuLCBtZXNzYWdlKTsgfVxufVxuIl19