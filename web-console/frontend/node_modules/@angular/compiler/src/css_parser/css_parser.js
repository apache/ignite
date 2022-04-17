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
        define("@angular/compiler/src/css_parser/css_parser", ["require", "exports", "tslib", "@angular/compiler/src/chars", "@angular/compiler/src/parse_util", "@angular/compiler/src/css_parser/css_ast", "@angular/compiler/src/css_parser/css_lexer", "@angular/compiler/src/css_parser/css_lexer", "@angular/compiler/src/css_parser/css_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var chars = require("@angular/compiler/src/chars");
    var parse_util_1 = require("@angular/compiler/src/parse_util");
    var css_ast_1 = require("@angular/compiler/src/css_parser/css_ast");
    var css_lexer_1 = require("@angular/compiler/src/css_parser/css_lexer");
    var SPACE_OPERATOR = ' ';
    var css_lexer_2 = require("@angular/compiler/src/css_parser/css_lexer");
    exports.CssToken = css_lexer_2.CssToken;
    var css_ast_2 = require("@angular/compiler/src/css_parser/css_ast");
    exports.BlockType = css_ast_2.BlockType;
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
                return css_lexer_1.isNewline(code) ? NEWLINE_DELIM_FLAG : 0;
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
    exports.ParsedCssResult = ParsedCssResult;
    var CssParser = /** @class */ (function () {
        function CssParser() {
            this._errors = [];
        }
        /**
         * @param css the CSS code that will be parsed
         * @param url the name of the CSS file containing the CSS source code
         */
        CssParser.prototype.parse = function (css, url) {
            var lexer = new css_lexer_1.CssLexer();
            this._file = new parse_util_1.ParseSourceFile(css, url);
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
                this._scanner.setMode(css_lexer_1.CssLexerMode.BLOCK);
                results.push(this._parseRule(delimiters));
            }
            var span = null;
            if (results.length > 0) {
                var firstRule = results[0];
                // we collect the last token like so incase there was an
                // EOF token that was emitted sometime during the lexing
                span = this._generateSourceSpan(firstRule, this._lastToken);
            }
            return new css_ast_1.CssStyleSheetAst(span, results);
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
            if (start instanceof css_ast_1.CssAst) {
                startLoc = start.location.start;
            }
            else {
                var token = start;
                if (token == null) {
                    // the data here is invalid, however, if and when this does
                    // occur, any other errors associated with this will be collected
                    token = this._lastToken;
                }
                startLoc = new parse_util_1.ParseLocation(this._file, token.index, token.line, token.column);
            }
            if (end == null) {
                end = this._lastToken;
            }
            var endLine = -1;
            var endColumn = -1;
            var endIndex = -1;
            if (end instanceof css_ast_1.CssAst) {
                endLine = end.location.end.line;
                endColumn = end.location.end.col;
                endIndex = end.location.end.offset;
            }
            else if (end instanceof css_lexer_1.CssToken) {
                endLine = end.line;
                endColumn = end.column;
                endIndex = end.index;
            }
            var endLoc = new parse_util_1.ParseLocation(this._file, endIndex, endLine, endColumn);
            return new parse_util_1.ParseSourceSpan(startLoc, endLoc);
        };
        /** @internal */
        CssParser.prototype._resolveBlockType = function (token) {
            switch (token.strValue) {
                case '@-o-keyframes':
                case '@-moz-keyframes':
                case '@-webkit-keyframes':
                case '@keyframes':
                    return css_ast_1.BlockType.Keyframes;
                case '@charset':
                    return css_ast_1.BlockType.Charset;
                case '@import':
                    return css_ast_1.BlockType.Import;
                case '@namespace':
                    return css_ast_1.BlockType.Namespace;
                case '@page':
                    return css_ast_1.BlockType.Page;
                case '@document':
                    return css_ast_1.BlockType.Document;
                case '@media':
                    return css_ast_1.BlockType.MediaQuery;
                case '@font-face':
                    return css_ast_1.BlockType.FontFace;
                case '@viewport':
                    return css_ast_1.BlockType.Viewport;
                case '@supports':
                    return css_ast_1.BlockType.Supports;
                default:
                    return css_ast_1.BlockType.Unsupported;
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
            this._scanner.setMode(css_lexer_1.CssLexerMode.BLOCK);
            var token = this._scan();
            var startToken = token;
            this._assertCondition(token.type == css_lexer_1.CssTokenType.AtKeyword, "The CSS Rule " + token.strValue + " is not a valid [@] rule.", token);
            var block;
            var type = this._resolveBlockType(token);
            var span;
            var tokens;
            var endToken;
            var end;
            var strValue;
            var query;
            switch (type) {
                case css_ast_1.BlockType.Charset:
                case css_ast_1.BlockType.Namespace:
                case css_ast_1.BlockType.Import:
                    var value = this._parseValue(delimiters);
                    this._scanner.setMode(css_lexer_1.CssLexerMode.BLOCK);
                    this._scanner.consumeEmptyStatements();
                    span = this._generateSourceSpan(startToken, value);
                    return new css_ast_1.CssInlineRuleAst(span, type, value);
                case css_ast_1.BlockType.Viewport:
                case css_ast_1.BlockType.FontFace:
                    block = this._parseStyleBlock(delimiters);
                    span = this._generateSourceSpan(startToken, block);
                    return new css_ast_1.CssBlockRuleAst(span, type, block);
                case css_ast_1.BlockType.Keyframes:
                    tokens = this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG);
                    // keyframes only have one identifier name
                    var name_1 = tokens[0];
                    block = this._parseKeyframeBlock(delimiters);
                    span = this._generateSourceSpan(startToken, block);
                    return new css_ast_1.CssKeyframeRuleAst(span, name_1, block);
                case css_ast_1.BlockType.MediaQuery:
                    this._scanner.setMode(css_lexer_1.CssLexerMode.MEDIA_QUERY);
                    tokens = this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG);
                    endToken = tokens[tokens.length - 1];
                    // we do not track the whitespace after the mediaQuery predicate ends
                    // so we have to calculate the end string value on our own
                    end = endToken.index + endToken.strValue.length - 1;
                    strValue = this._extractSourceContent(start, end);
                    span = this._generateSourceSpan(startToken, endToken);
                    query = new css_ast_1.CssAtRulePredicateAst(span, strValue, tokens);
                    block = this._parseBlock(delimiters);
                    strValue = this._extractSourceContent(start, this._getScannerIndex() - 1);
                    span = this._generateSourceSpan(startToken, block);
                    return new css_ast_1.CssMediaQueryRuleAst(span, strValue, query, block);
                case css_ast_1.BlockType.Document:
                case css_ast_1.BlockType.Supports:
                case css_ast_1.BlockType.Page:
                    this._scanner.setMode(css_lexer_1.CssLexerMode.AT_RULE_QUERY);
                    tokens = this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG);
                    endToken = tokens[tokens.length - 1];
                    // we do not track the whitespace after this block rule predicate ends
                    // so we have to calculate the end string value on our own
                    end = endToken.index + endToken.strValue.length - 1;
                    strValue = this._extractSourceContent(start, end);
                    span = this._generateSourceSpan(startToken, tokens[tokens.length - 1]);
                    query = new css_ast_1.CssAtRulePredicateAst(span, strValue, tokens);
                    block = this._parseBlock(delimiters);
                    strValue = this._extractSourceContent(start, block.end.offset);
                    span = this._generateSourceSpan(startToken, block);
                    return new css_ast_1.CssBlockDefinitionRuleAst(span, strValue, type, query, block);
                // if a custom @rule { ... } is used it should still tokenize the insides
                default:
                    var listOfTokens_1 = [];
                    var tokenName = token.strValue;
                    this._scanner.setMode(css_lexer_1.CssLexerMode.ALL);
                    this._error(css_lexer_1.generateErrorMessage(this._getSourceContent(), "The CSS \"at\" rule \"" + tokenName + "\" is not allowed to used here", token.strValue, token.index, token.line, token.column), token);
                    this._collectUntilDelim(delimiters | LBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG)
                        .forEach(function (token) { listOfTokens_1.push(token); });
                    if (this._scanner.peek == chars.$LBRACE) {
                        listOfTokens_1.push(this._consume(css_lexer_1.CssTokenType.Character, '{'));
                        this._collectUntilDelim(delimiters | RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG)
                            .forEach(function (token) { listOfTokens_1.push(token); });
                        listOfTokens_1.push(this._consume(css_lexer_1.CssTokenType.Character, '}'));
                    }
                    endToken = listOfTokens_1[listOfTokens_1.length - 1];
                    span = this._generateSourceSpan(startToken, endToken);
                    return new css_ast_1.CssUnknownRuleAst(span, tokenName, listOfTokens_1);
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
                ruleAst = new css_ast_1.CssSelectorRuleAst(span, selectors, block);
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
                ruleAst = new css_ast_1.CssUnknownTokenListAst(span, name_2, innerTokens_1);
            }
            this._scanner.setMode(css_lexer_1.CssLexerMode.BLOCK);
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
                    this._consume(css_lexer_1.CssTokenType.Character, ',');
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
                this._error(css_lexer_1.getRawMessage(error), token);
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
                this._error(css_lexer_1.getRawMessage(error), token);
            }
            this._lastToken = token;
            return token;
        };
        /** @internal */
        CssParser.prototype._parseKeyframeBlock = function (delimiters) {
            delimiters |= RBRACE_DELIM_FLAG;
            this._scanner.setMode(css_lexer_1.CssLexerMode.KEYFRAME_BLOCK);
            var startToken = this._consume(css_lexer_1.CssTokenType.Character, '{');
            var definitions = [];
            while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
                definitions.push(this._parseKeyframeDefinition(delimiters));
            }
            var endToken = this._consume(css_lexer_1.CssTokenType.Character, '}');
            var span = this._generateSourceSpan(startToken, endToken);
            return new css_ast_1.CssBlockAst(span, definitions);
        };
        /** @internal */
        CssParser.prototype._parseKeyframeDefinition = function (delimiters) {
            var start = this._getScannerIndex();
            var stepTokens = [];
            delimiters |= LBRACE_DELIM_FLAG;
            while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
                stepTokens.push(this._parseKeyframeLabel(delimiters | COMMA_DELIM_FLAG));
                if (this._scanner.peek != chars.$LBRACE) {
                    this._consume(css_lexer_1.CssTokenType.Character, ',');
                }
            }
            var stylesBlock = this._parseStyleBlock(delimiters | RBRACE_DELIM_FLAG);
            var span = this._generateSourceSpan(stepTokens[0], stylesBlock);
            var ast = new css_ast_1.CssKeyframeDefinitionAst(span, stepTokens, stylesBlock);
            this._scanner.setMode(css_lexer_1.CssLexerMode.BLOCK);
            return ast;
        };
        /** @internal */
        CssParser.prototype._parseKeyframeLabel = function (delimiters) {
            this._scanner.setMode(css_lexer_1.CssLexerMode.KEYFRAME_BLOCK);
            return css_ast_1.mergeTokens(this._collectUntilDelim(delimiters));
        };
        /** @internal */
        CssParser.prototype._parsePseudoSelector = function (delimiters) {
            var start = this._getScannerIndex();
            delimiters &= ~COMMA_DELIM_FLAG;
            // we keep the original value since we may use it to recurse when :not, :host are used
            var startingDelims = delimiters;
            var startToken = this._consume(css_lexer_1.CssTokenType.Character, ':');
            var tokens = [startToken];
            if (this._scanner.peek == chars.$COLON) { // ::something
                tokens.push(this._consume(css_lexer_1.CssTokenType.Character, ':'));
            }
            var innerSelectors = [];
            this._scanner.setMode(css_lexer_1.CssLexerMode.PSEUDO_SELECTOR);
            // host, host-context, lang, not, nth-child are all identifiers
            var pseudoSelectorToken = this._consume(css_lexer_1.CssTokenType.Identifier);
            var pseudoSelectorName = pseudoSelectorToken.strValue;
            tokens.push(pseudoSelectorToken);
            // host(), lang(), nth-child(), etc...
            if (this._scanner.peek == chars.$LPAREN) {
                this._scanner.setMode(css_lexer_1.CssLexerMode.PSEUDO_SELECTOR_WITH_ARGUMENTS);
                var openParenToken = this._consume(css_lexer_1.CssTokenType.Character, '(');
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
                var closeParenToken = this._consume(css_lexer_1.CssTokenType.Character, ')');
                tokens.push(closeParenToken);
            }
            var end = this._getScannerIndex() - 1;
            var strValue = this._extractSourceContent(start, end);
            var endToken = tokens[tokens.length - 1];
            var span = this._generateSourceSpan(startToken, endToken);
            return new css_ast_1.CssPseudoSelectorAst(span, strValue, pseudoSelectorName, tokens, innerSelectors);
        };
        /** @internal */
        CssParser.prototype._parseSimpleSelector = function (delimiters) {
            var start = this._getScannerIndex();
            delimiters |= COMMA_DELIM_FLAG;
            this._scanner.setMode(css_lexer_1.CssLexerMode.SELECTOR);
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
                        this._scanner.setMode(css_lexer_1.CssLexerMode.SELECTOR);
                        break;
                    case chars.$LBRACKET:
                        // we set the mode after the scan because attribute mode does not
                        // allow attribute [] values. And this also will catch any errors
                        // if an extra "[" is used inside.
                        selectorCssTokens.push(this._scan());
                        this._scanner.setMode(css_lexer_1.CssLexerMode.ATTRIBUTE_SELECTOR);
                        break;
                    case chars.$RBRACKET:
                        if (this._scanner.getMode() != css_lexer_1.CssLexerMode.ATTRIBUTE_SELECTOR) {
                            hasAttributeError = true;
                        }
                        // we set the mode early because attribute mode does not
                        // allow attribute [] values
                        this._scanner.setMode(css_lexer_1.CssLexerMode.SELECTOR);
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
                hasAttributeError || this._scanner.getMode() == css_lexer_1.CssLexerMode.ATTRIBUTE_SELECTOR;
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
                                var deepToken = this._consume(css_lexer_1.CssTokenType.Identifier);
                                var deepSlash = this._consume(css_lexer_1.CssTokenType.Character);
                                var index = lastOperatorToken.index;
                                var line = lastOperatorToken.line;
                                var column = lastOperatorToken.column;
                                if (deepToken != null && deepToken.strValue.toLowerCase() == 'deep' &&
                                    deepSlash.strValue == SLASH_CHARACTER) {
                                    token = new css_lexer_1.CssToken(lastOperatorToken.index, lastOperatorToken.column, lastOperatorToken.line, css_lexer_1.CssTokenType.Identifier, DEEP_OPERATOR_STR);
                                }
                                else {
                                    var text = SLASH_CHARACTER + deepToken.strValue + deepSlash.strValue;
                                    this._error(css_lexer_1.generateErrorMessage(this._getSourceContent(), text + " is an invalid CSS operator", text, index, line, column), lastOperatorToken);
                                    token = new css_lexer_1.CssToken(index, column, line, css_lexer_1.CssTokenType.Invalid, text);
                                }
                                break;
                            case GT_CHARACTER:
                                // >>> operator
                                if (this._scanner.peek == chars.$GT && this._scanner.peekPeek == chars.$GT) {
                                    this._consume(css_lexer_1.CssTokenType.Character, GT_CHARACTER);
                                    this._consume(css_lexer_1.CssTokenType.Character, GT_CHARACTER);
                                    token = new css_lexer_1.CssToken(lastOperatorToken.index, lastOperatorToken.column, lastOperatorToken.line, css_lexer_1.CssTokenType.Identifier, TRIPLE_GT_OPERATOR_STR);
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
            return new css_ast_1.CssSimpleSelectorAst(span, selectorCssTokens, strValue, pseudoSelectors, operator);
        };
        /** @internal */
        CssParser.prototype._parseSelector = function (delimiters) {
            delimiters |= COMMA_DELIM_FLAG;
            this._scanner.setMode(css_lexer_1.CssLexerMode.SELECTOR);
            var simpleSelectors = [];
            while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
                simpleSelectors.push(this._parseSimpleSelector(delimiters));
                this._scanner.consumeWhitespace();
            }
            var firstSelector = simpleSelectors[0];
            var lastSelector = simpleSelectors[simpleSelectors.length - 1];
            var span = this._generateSourceSpan(firstSelector, lastSelector);
            return new css_ast_1.CssSelectorAst(span, simpleSelectors);
        };
        /** @internal */
        CssParser.prototype._parseValue = function (delimiters) {
            delimiters |= RBRACE_DELIM_FLAG | SEMICOLON_DELIM_FLAG | NEWLINE_DELIM_FLAG;
            this._scanner.setMode(css_lexer_1.CssLexerMode.STYLE_VALUE);
            var start = this._getScannerIndex();
            var tokens = [];
            var wsStr = '';
            var previous = undefined;
            while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
                var token = void 0;
                if (previous != null && previous.type == css_lexer_1.CssTokenType.Identifier &&
                    this._scanner.peek == chars.$LPAREN) {
                    token = this._consume(css_lexer_1.CssTokenType.Character, '(');
                    tokens.push(token);
                    this._scanner.setMode(css_lexer_1.CssLexerMode.STYLE_VALUE_FUNCTION);
                    token = this._scan();
                    tokens.push(token);
                    this._scanner.setMode(css_lexer_1.CssLexerMode.STYLE_VALUE);
                    token = this._consume(css_lexer_1.CssTokenType.Character, ')');
                    tokens.push(token);
                }
                else {
                    token = this._scan();
                    if (token.type == css_lexer_1.CssTokenType.Whitespace) {
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
                this._consume(css_lexer_1.CssTokenType.Character, ';');
            }
            else if (code != chars.$RBRACE) {
                this._error(css_lexer_1.generateErrorMessage(this._getSourceContent(), "The CSS key/value definition did not end with a semicolon", previous.strValue, previous.index, previous.line, previous.column), previous);
            }
            var strValue = this._extractSourceContent(start, end);
            var startToken = tokens[0];
            var endToken = tokens[tokens.length - 1];
            var span = this._generateSourceSpan(startToken, endToken);
            return new css_ast_1.CssStyleValueAst(span, tokens, strValue);
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
            this._scanner.setMode(css_lexer_1.CssLexerMode.BLOCK);
            var startToken = this._consume(css_lexer_1.CssTokenType.Character, '{');
            this._scanner.consumeEmptyStatements();
            var results = [];
            while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
                results.push(this._parseRule(delimiters));
            }
            var endToken = this._consume(css_lexer_1.CssTokenType.Character, '}');
            this._scanner.setMode(css_lexer_1.CssLexerMode.BLOCK);
            this._scanner.consumeEmptyStatements();
            var span = this._generateSourceSpan(startToken, endToken);
            return new css_ast_1.CssBlockAst(span, results);
        };
        /** @internal */
        CssParser.prototype._parseStyleBlock = function (delimiters) {
            delimiters |= RBRACE_DELIM_FLAG | LBRACE_DELIM_FLAG;
            this._scanner.setMode(css_lexer_1.CssLexerMode.STYLE_BLOCK);
            var startToken = this._consume(css_lexer_1.CssTokenType.Character, '{');
            if (startToken.numValue != chars.$LBRACE) {
                return null;
            }
            var definitions = [];
            this._scanner.consumeEmptyStatements();
            while (!characterContainsDelimiter(this._scanner.peek, delimiters)) {
                definitions.push(this._parseDefinition(delimiters));
                this._scanner.consumeEmptyStatements();
            }
            var endToken = this._consume(css_lexer_1.CssTokenType.Character, '}');
            this._scanner.setMode(css_lexer_1.CssLexerMode.STYLE_BLOCK);
            this._scanner.consumeEmptyStatements();
            var span = this._generateSourceSpan(startToken, endToken);
            return new css_ast_1.CssStylesBlockAst(span, definitions);
        };
        /** @internal */
        CssParser.prototype._parseDefinition = function (delimiters) {
            this._scanner.setMode(css_lexer_1.CssLexerMode.STYLE_BLOCK);
            var prop = this._consume(css_lexer_1.CssTokenType.Identifier);
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
                        var nextValue = this._consume(css_lexer_1.CssTokenType.Character, ':');
                        propStr_1.push(nextValue.strValue);
                        var remainingTokens = this._collectUntilDelim(delimiters | COLON_DELIM_FLAG | SEMICOLON_DELIM_FLAG, css_lexer_1.CssTokenType.Identifier);
                        if (remainingTokens.length > 0) {
                            remainingTokens.forEach(function (token) { propStr_1.push(token.strValue); });
                        }
                        endToken = prop =
                            new css_lexer_1.CssToken(prop.index, prop.column, prop.line, prop.type, propStr_1.join(' '));
                    }
                    // this means we've reached the end of the definition and/or block
                    if (this._scanner.peek == chars.$COLON) {
                        this._consume(css_lexer_1.CssTokenType.Character, ':');
                        parseValue = true;
                    }
                    break;
            }
            if (parseValue) {
                value = this._parseValue(delimiters);
                endToken = value;
            }
            else {
                this._error(css_lexer_1.generateErrorMessage(this._getSourceContent(), "The CSS property was not paired with a style value", prop.strValue, prop.index, prop.line, prop.column), prop);
            }
            var span = this._generateSourceSpan(prop, endToken);
            return new css_ast_1.CssDefinitionAst(span, prop, value);
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
    exports.CssParser = CssParser;
    var CssParseError = /** @class */ (function (_super) {
        tslib_1.__extends(CssParseError, _super);
        function CssParseError(span, message) {
            return _super.call(this, span, message) || this;
        }
        CssParseError.create = function (file, offset, line, col, length, errMsg) {
            var start = new parse_util_1.ParseLocation(file, offset, line, col);
            var end = new parse_util_1.ParseLocation(file, offset, line, col + length);
            var span = new parse_util_1.ParseSourceSpan(start, end);
            return new CssParseError(span, 'CSS Parse Error: ' + errMsg);
        };
        return CssParseError;
    }(parse_util_1.ParseError));
    exports.CssParseError = CssParseError;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NzX3BhcnNlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9jc3NfcGFyc2VyL2Nzc19wYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBRUgsbURBQWtDO0lBQ2xDLCtEQUEwRjtJQUUxRixvRUFBK2E7SUFDL2Esd0VBQXVJO0lBRXZJLElBQU0sY0FBYyxHQUFHLEdBQUcsQ0FBQztJQUUzQix3RUFBcUM7SUFBN0IsK0JBQUEsUUFBUSxDQUFBO0lBQ2hCLG9FQUFvQztJQUE1Qiw4QkFBQSxTQUFTLENBQUE7SUFFakIsSUFBTSxlQUFlLEdBQUcsR0FBRyxDQUFDO0lBQzVCLElBQU0sWUFBWSxHQUFHLEdBQUcsQ0FBQztJQUN6QixJQUFNLHNCQUFzQixHQUFHLEtBQUssQ0FBQztJQUNyQyxJQUFNLGlCQUFpQixHQUFHLFFBQVEsQ0FBQztJQUVuQyxJQUFNLGNBQWMsR0FBRyxDQUFDLENBQUM7SUFDekIsSUFBTSxpQkFBaUIsR0FBRyxDQUFDLENBQUM7SUFDNUIsSUFBTSxpQkFBaUIsR0FBRyxDQUFDLENBQUM7SUFDNUIsSUFBTSxnQkFBZ0IsR0FBRyxDQUFDLENBQUM7SUFDM0IsSUFBTSxnQkFBZ0IsR0FBRyxFQUFFLENBQUM7SUFDNUIsSUFBTSxvQkFBb0IsR0FBRyxFQUFFLENBQUM7SUFDaEMsSUFBTSxrQkFBa0IsR0FBRyxFQUFFLENBQUM7SUFDOUIsSUFBTSxpQkFBaUIsR0FBRyxHQUFHLENBQUM7SUFDOUIsSUFBTSxpQkFBaUIsR0FBRyxHQUFHLENBQUM7SUFDOUIsSUFBTSxnQkFBZ0IsR0FBRyxHQUFHLENBQUM7SUFFN0IsU0FBUyxxQ0FBcUMsQ0FBQyxJQUFZO1FBQ3pELE9BQU8sQ0FBQyxLQUFLLEVBQUUsTUFBTSxFQUFFLGNBQWMsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFD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QkFBQztJQUFELENBQUMsQUF0eUJELElBc3lCQztJQXR5QlksOEJBQVM7SUF3eUJ0QjtRQUFtQyx5Q0FBVTtRQVUzQyx1QkFBWSxJQUFxQixFQUFFLE9BQWU7bUJBQUksa0JBQU0sSUFBSSxFQUFFLE9BQU8sQ0FBQztRQUFFLENBQUM7UUFUdEUsb0JBQU0sR0FBYixVQUNJLElBQXFCLEVBQUUsTUFBYyxFQUFFLElBQVksRUFBRSxHQUFXLEVBQUUsTUFBYyxFQUNoRixNQUFjO1lBQ2hCLElBQU0sS0FBSyxHQUFHLElBQUksMEJBQWEsQ0FBQyxJQUFJLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxHQUFHLENBQUMsQ0FBQztZQUN6RCxJQUFNLEdBQUcsR0FBRyxJQUFJLDBCQUFhLENBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxJQUFJLEVBQUUsR0FBRyxHQUFHLE1BQU0sQ0FBQyxDQUFDO1lBQ2hFLElBQU0sSUFBSSxHQUFHLElBQUksNEJBQWUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQUM7WUFDN0MsT0FBTyxJQUFJLGFBQWEsQ0FBQyxJQUFJLEVBQUUsbUJBQW1CLEdBQUcsTUFBTSxDQUFDLENBQUM7UUFDL0QsQ0FBQztRQUdILG9CQUFDO0lBQUQsQ0FBQyxBQVhELENBQW1DLHVCQUFVLEdBVzVDO0lBWFksc0NBQWEiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAqIGFzIGNoYXJzIGZyb20gJy4uL2NoYXJzJztcbmltcG9ydCB7UGFyc2VFcnJvciwgUGFyc2VMb2NhdGlvbiwgUGFyc2VTb3VyY2VGaWxlLCBQYXJzZVNvdXJjZVNwYW59IGZyb20gJy4uL3BhcnNlX3V0aWwnO1xuXG5pbXBvcnQge0Jsb2NrVHlwZSwgQ3NzQXN0LCBDc3NBdFJ1bGVQcmVkaWNhdGVBc3QsIENzc0Jsb2NrQXN0LCBDc3NCbG9ja0RlZmluaXRpb25SdWxlQXN0LCBDc3NCbG9ja1J1bGVBc3QsIENzc0RlZmluaXRpb25Bc3QsIENzc0lubGluZVJ1bGVBc3QsIENzc0tleWZyYW1lRGVmaW5pdGlvbkFzdCwgQ3NzS2V5ZnJhbWVSdWxlQXN0LCBDc3NNZWRpYVF1ZXJ5UnVsZUFzdCwgQ3NzUHNldWRvU2VsZWN0b3JBc3QsIENzc1J1bGVBc3QsIENzc1NlbGVjdG9yQXN0LCBDc3NTZWxlY3RvclJ1bGVBc3QsIENzc1NpbXBsZVNlbGVjdG9yQXN0LCBDc3NTdHlsZVNoZWV0QXN0LCBDc3NTdHlsZVZhbHVlQXN0LCBDc3NTdHlsZXNCbG9ja0FzdCwgQ3NzVW5rbm93blJ1bGVBc3QsIENzc1Vua25vd25Ub2tlbkxpc3RBc3QsIG1lcmdlVG9rZW5zfSBmcm9tICcuL2Nzc19hc3QnO1xuaW1wb3J0IHtDc3NMZXhlciwgQ3NzTGV4ZXJNb2RlLCBDc3NTY2FubmVyLCBDc3NUb2tlbiwgQ3NzVG9rZW5UeXBlLCBnZW5lcmF0ZUVycm9yTWVzc2FnZSwgZ2V0UmF3TWVzc2FnZSwgaXNOZXdsaW5lfSBmcm9tICcuL2Nzc19sZXhlcic7XG5cbmNvbnN0IFNQQUNFX09QRVJBVE9SID0gJyAnO1xuXG5leHBvcnQge0Nzc1Rva2VufSBmcm9tICcuL2Nzc19sZXhlcic7XG5leHBvcnQge0Jsb2NrVHlwZX0gZnJvbSAnLi9jc3NfYXN0JztcblxuY29uc3QgU0xBU0hfQ0hBUkFDVEVSID0gJy8nO1xuY29uc3QgR1RfQ0hBUkFDVEVSID0gJz4nO1xuY29uc3QgVFJJUExFX0dUX09QRVJBVE9SX1NUUiA9ICc+Pj4nO1xuY29uc3QgREVFUF9PUEVSQVRPUl9TVFIgPSAnL2RlZXAvJztcblxuY29uc3QgRU9GX0RFTElNX0ZMQUcgPSAxO1xuY29uc3QgUkJSQUNFX0RFTElNX0ZMQUcgPSAyO1xuY29uc3QgTEJSQUNFX0RFTElNX0ZMQUcgPSA0O1xuY29uc3QgQ09NTUFfREVMSU1fRkxBRyA9IDg7XG5jb25zdCBDT0xPTl9ERUxJTV9GTEFHID0gMTY7XG5jb25zdCBTRU1JQ09MT05fREVMSU1fRkxBRyA9IDMyO1xuY29uc3QgTkVXTElORV9ERUxJTV9GTEFHID0gNjQ7XG5jb25zdCBSUEFSRU5fREVMSU1fRkxBRyA9IDEyODtcbmNvbnN0IExQQVJFTl9ERUxJTV9GTEFHID0gMjU2O1xuY29uc3QgU1BBQ0VfREVMSU1fRkxBRyA9IDUxMjtcblxuZnVuY3Rpb24gX3BzZXVkb1NlbGVjdG9yU3VwcG9ydHNJbm5lclNlbGVjdG9ycyhuYW1lOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgcmV0dXJuIFsnbm90JywgJ2hvc3QnLCAnaG9zdC1jb250ZXh0J10uaW5kZXhPZihuYW1lKSA+PSAwO1xufVxuXG5mdW5jdGlvbiBpc1NlbGVjdG9yT3BlcmF0b3JDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJFNMQVNIOlxuICAgIGNhc2UgY2hhcnMuJFRJTERBOlxuICAgIGNhc2UgY2hhcnMuJFBMVVM6XG4gICAgY2FzZSBjaGFycy4kR1Q6XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICBkZWZhdWx0OlxuICAgICAgcmV0dXJuIGNoYXJzLmlzV2hpdGVzcGFjZShjb2RlKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBnZXREZWxpbUZyb21DaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogbnVtYmVyIHtcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kRU9GOlxuICAgICAgcmV0dXJuIEVPRl9ERUxJTV9GTEFHO1xuICAgIGNhc2UgY2hhcnMuJENPTU1BOlxuICAgICAgcmV0dXJuIENPTU1BX0RFTElNX0ZMQUc7XG4gICAgY2FzZSBjaGFycy4kQ09MT046XG4gICAgICByZXR1cm4gQ09MT05fREVMSU1fRkxBRztcbiAgICBjYXNlIGNoYXJzLiRTRU1JQ09MT046XG4gICAgICByZXR1cm4gU0VNSUNPTE9OX0RFTElNX0ZMQUc7XG4gICAgY2FzZSBjaGFycy4kUkJSQUNFOlxuICAgICAgcmV0dXJuIFJCUkFDRV9ERUxJTV9GTEFHO1xuICAgIGNhc2UgY2hhcnMuJExCUkFDRTpcbiAgICAgIHJldHVybiBMQlJBQ0VfREVMSU1fRkxBRztcbiAgICBjYXNlIGNoYXJzLiRSUEFSRU46XG4gICAgICByZXR1cm4gUlBBUkVOX0RFTElNX0ZMQUc7XG4gICAgY2FzZSBjaGFycy4kU1BBQ0U6XG4gICAgY2FzZSBjaGFycy4kVEFCOlxuICAgICAgcmV0dXJuIFNQQUNFX0RFTElNX0ZMQUc7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBpc05ld2xpbmUoY29kZSkgPyBORVdMSU5FX0RFTElNX0ZMQUcgOiAwO1xuICB9XG59XG5cbmZ1bmN0aW9uIGNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKGNvZGU6IG51bWJlciwgZGVsaW1pdGVyczogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHJldHVybiAoZ2V0RGVsaW1Gcm9tQ2hhcmFjdGVyKGNvZGUpICYgZGVsaW1pdGVycykgPiAwO1xufVxuXG5leHBvcnQgY2xhc3MgUGFyc2VkQ3NzUmVzdWx0IHtcbiAgY29uc3RydWN0b3IocHVibGljIGVycm9yczogQ3NzUGFyc2VFcnJvcltdLCBwdWJsaWMgYXN0OiBDc3NTdHlsZVNoZWV0QXN0KSB7fVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzUGFyc2VyIHtcbiAgcHJpdmF0ZSBfZXJyb3JzOiBDc3NQYXJzZUVycm9yW10gPSBbXTtcbiAgLy8gVE9ETyhpc3N1ZS8yNDU3MSk6IHJlbW92ZSAnIScuXG4gIHByaXZhdGUgX2ZpbGUgITogUGFyc2VTb3VyY2VGaWxlO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfc2Nhbm5lciAhOiBDc3NTY2FubmVyO1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcHJpdmF0ZSBfbGFzdFRva2VuICE6IENzc1Rva2VuO1xuXG4gIC8qKlxuICAgKiBAcGFyYW0gY3NzIHRoZSBDU1MgY29kZSB0aGF0IHdpbGwgYmUgcGFyc2VkXG4gICAqIEBwYXJhbSB1cmwgdGhlIG5hbWUgb2YgdGhlIENTUyBmaWxlIGNvbnRhaW5pbmcgdGhlIENTUyBzb3VyY2UgY29kZVxuICAgKi9cbiAgcGFyc2UoY3NzOiBzdHJpbmcsIHVybDogc3RyaW5nKTogUGFyc2VkQ3NzUmVzdWx0IHtcbiAgICBjb25zdCBsZXhlciA9IG5ldyBDc3NMZXhlcigpO1xuICAgIHRoaXMuX2ZpbGUgPSBuZXcgUGFyc2VTb3VyY2VGaWxlKGNzcywgdXJsKTtcbiAgICB0aGlzLl9zY2FubmVyID0gbGV4ZXIuc2Nhbihjc3MsIGZhbHNlKTtcblxuICAgIGNvbnN0IGFzdCA9IHRoaXMuX3BhcnNlU3R5bGVTaGVldChFT0ZfREVMSU1fRkxBRyk7XG5cbiAgICBjb25zdCBlcnJvcnMgPSB0aGlzLl9lcnJvcnM7XG4gICAgdGhpcy5fZXJyb3JzID0gW107XG5cbiAgICBjb25zdCByZXN1bHQgPSBuZXcgUGFyc2VkQ3NzUmVzdWx0KGVycm9ycywgYXN0KTtcbiAgICB0aGlzLl9maWxlID0gbnVsbCBhcyBhbnk7XG4gICAgdGhpcy5fc2Nhbm5lciA9IG51bGwgYXMgYW55O1xuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVN0eWxlU2hlZXQoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzU3R5bGVTaGVldEFzdCB7XG4gICAgY29uc3QgcmVzdWx0czogQ3NzUnVsZUFzdFtdID0gW107XG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG4gICAgd2hpbGUgKHRoaXMuX3NjYW5uZXIucGVlayAhPSBjaGFycy4kRU9GKSB7XG4gICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkJMT0NLKTtcbiAgICAgIHJlc3VsdHMucHVzaCh0aGlzLl9wYXJzZVJ1bGUoZGVsaW1pdGVycykpO1xuICAgIH1cbiAgICBsZXQgc3BhbjogUGFyc2VTb3VyY2VTcGFufG51bGwgPSBudWxsO1xuICAgIGlmIChyZXN1bHRzLmxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnN0IGZpcnN0UnVsZSA9IHJlc3VsdHNbMF07XG4gICAgICAvLyB3ZSBjb2xsZWN0IHRoZSBsYXN0IHRva2VuIGxpa2Ugc28gaW5jYXNlIHRoZXJlIHdhcyBhblxuICAgICAgLy8gRU9GIHRva2VuIHRoYXQgd2FzIGVtaXR0ZWQgc29tZXRpbWUgZHVyaW5nIHRoZSBsZXhpbmdcbiAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oZmlyc3RSdWxlLCB0aGlzLl9sYXN0VG9rZW4pO1xuICAgIH1cbiAgICByZXR1cm4gbmV3IENzc1N0eWxlU2hlZXRBc3Qoc3BhbiAhLCByZXN1bHRzKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2dldFNvdXJjZUNvbnRlbnQoKTogc3RyaW5nIHsgcmV0dXJuIHRoaXMuX3NjYW5uZXIgIT0gbnVsbCA/IHRoaXMuX3NjYW5uZXIuaW5wdXQgOiAnJzsgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0OiBudW1iZXIsIGVuZDogbnVtYmVyKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5fZ2V0U291cmNlQ29udGVudCgpLnN1YnN0cmluZyhzdGFydCwgZW5kICsgMSk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnQ6IENzc1Rva2VufENzc0FzdCwgZW5kOiBDc3NUb2tlbnxDc3NBc3R8bnVsbCA9IG51bGwpOiBQYXJzZVNvdXJjZVNwYW4ge1xuICAgIGxldCBzdGFydExvYzogUGFyc2VMb2NhdGlvbjtcbiAgICBpZiAoc3RhcnQgaW5zdGFuY2VvZiBDc3NBc3QpIHtcbiAgICAgIHN0YXJ0TG9jID0gc3RhcnQubG9jYXRpb24uc3RhcnQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIGxldCB0b2tlbiA9IHN0YXJ0O1xuICAgICAgaWYgKHRva2VuID09IG51bGwpIHtcbiAgICAgICAgLy8gdGhlIGRhdGEgaGVyZSBpcyBpbnZhbGlkLCBob3dldmVyLCBpZiBhbmQgd2hlbiB0aGlzIGRvZXNcbiAgICAgICAgLy8gb2NjdXIsIGFueSBvdGhlciBlcnJvcnMgYXNzb2NpYXRlZCB3aXRoIHRoaXMgd2lsbCBiZSBjb2xsZWN0ZWRcbiAgICAgICAgdG9rZW4gPSB0aGlzLl9sYXN0VG9rZW47XG4gICAgICB9XG4gICAgICBzdGFydExvYyA9IG5ldyBQYXJzZUxvY2F0aW9uKHRoaXMuX2ZpbGUsIHRva2VuLmluZGV4LCB0b2tlbi5saW5lLCB0b2tlbi5jb2x1bW4pO1xuICAgIH1cblxuICAgIGlmIChlbmQgPT0gbnVsbCkge1xuICAgICAgZW5kID0gdGhpcy5fbGFzdFRva2VuO1xuICAgIH1cblxuICAgIGxldCBlbmRMaW5lOiBudW1iZXIgPSAtMTtcbiAgICBsZXQgZW5kQ29sdW1uOiBudW1iZXIgPSAtMTtcbiAgICBsZXQgZW5kSW5kZXg6IG51bWJlciA9IC0xO1xuICAgIGlmIChlbmQgaW5zdGFuY2VvZiBDc3NBc3QpIHtcbiAgICAgIGVuZExpbmUgPSBlbmQubG9jYXRpb24uZW5kLmxpbmUgITtcbiAgICAgIGVuZENvbHVtbiA9IGVuZC5sb2NhdGlvbi5lbmQuY29sICE7XG4gICAgICBlbmRJbmRleCA9IGVuZC5sb2NhdGlvbi5lbmQub2Zmc2V0ICE7XG4gICAgfSBlbHNlIGlmIChlbmQgaW5zdGFuY2VvZiBDc3NUb2tlbikge1xuICAgICAgZW5kTGluZSA9IGVuZC5saW5lO1xuICAgICAgZW5kQ29sdW1uID0gZW5kLmNvbHVtbjtcbiAgICAgIGVuZEluZGV4ID0gZW5kLmluZGV4O1xuICAgIH1cblxuICAgIGNvbnN0IGVuZExvYyA9IG5ldyBQYXJzZUxvY2F0aW9uKHRoaXMuX2ZpbGUsIGVuZEluZGV4LCBlbmRMaW5lLCBlbmRDb2x1bW4pO1xuICAgIHJldHVybiBuZXcgUGFyc2VTb3VyY2VTcGFuKHN0YXJ0TG9jLCBlbmRMb2MpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcmVzb2x2ZUJsb2NrVHlwZSh0b2tlbjogQ3NzVG9rZW4pOiBCbG9ja1R5cGUge1xuICAgIHN3aXRjaCAodG9rZW4uc3RyVmFsdWUpIHtcbiAgICAgIGNhc2UgJ0Atby1rZXlmcmFtZXMnOlxuICAgICAgY2FzZSAnQC1tb3ota2V5ZnJhbWVzJzpcbiAgICAgIGNhc2UgJ0Atd2Via2l0LWtleWZyYW1lcyc6XG4gICAgICBjYXNlICdAa2V5ZnJhbWVzJzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5LZXlmcmFtZXM7XG5cbiAgICAgIGNhc2UgJ0BjaGFyc2V0JzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5DaGFyc2V0O1xuXG4gICAgICBjYXNlICdAaW1wb3J0JzpcbiAgICAgICAgcmV0dXJuIEJsb2NrVHlwZS5JbXBvcnQ7XG5cbiAgICAgIGNhc2UgJ0BuYW1lc3BhY2UnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLk5hbWVzcGFjZTtcblxuICAgICAgY2FzZSAnQHBhZ2UnOlxuICAgICAgICByZXR1cm4gQmxvY2tUeXBlLlBhZ2U7XG5cbiAgICAgIGNhc2UgJ0Bkb2N1bWVudCc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuRG9jdW1lbnQ7XG5cbiAgICAgIGNhc2UgJ0BtZWRpYSc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuTWVkaWFRdWVyeTtcblxuICAgICAgY2FzZSAnQGZvbnQtZmFjZSc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuRm9udEZhY2U7XG5cbiAgICAgIGNhc2UgJ0B2aWV3cG9ydCc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuVmlld3BvcnQ7XG5cbiAgICAgIGNhc2UgJ0BzdXBwb3J0cyc6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuU3VwcG9ydHM7XG5cbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHJldHVybiBCbG9ja1R5cGUuVW5zdXBwb3J0ZWQ7XG4gICAgfVxuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VSdWxlKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1J1bGVBc3Qge1xuICAgIGlmICh0aGlzLl9zY2FubmVyLnBlZWsgPT0gY2hhcnMuJEFUKSB7XG4gICAgICByZXR1cm4gdGhpcy5fcGFyc2VBdFJ1bGUoZGVsaW1pdGVycyk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLl9wYXJzZVNlbGVjdG9yUnVsZShkZWxpbWl0ZXJzKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlQXRSdWxlKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1J1bGVBc3Qge1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCk7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkJMT0NLKTtcbiAgICBjb25zdCB0b2tlbiA9IHRoaXMuX3NjYW4oKTtcbiAgICBjb25zdCBzdGFydFRva2VuID0gdG9rZW47XG5cbiAgICB0aGlzLl9hc3NlcnRDb25kaXRpb24oXG4gICAgICAgIHRva2VuLnR5cGUgPT0gQ3NzVG9rZW5UeXBlLkF0S2V5d29yZCxcbiAgICAgICAgYFRoZSBDU1MgUnVsZSAke3Rva2VuLnN0clZhbHVlfSBpcyBub3QgYSB2YWxpZCBbQF0gcnVsZS5gLCB0b2tlbik7XG5cbiAgICBsZXQgYmxvY2s6IENzc0Jsb2NrQXN0O1xuICAgIGNvbnN0IHR5cGUgPSB0aGlzLl9yZXNvbHZlQmxvY2tUeXBlKHRva2VuKTtcbiAgICBsZXQgc3BhbjogUGFyc2VTb3VyY2VTcGFuO1xuICAgIGxldCB0b2tlbnM6IENzc1Rva2VuW107XG4gICAgbGV0IGVuZFRva2VuOiBDc3NUb2tlbjtcbiAgICBsZXQgZW5kOiBudW1iZXI7XG4gICAgbGV0IHN0clZhbHVlOiBzdHJpbmc7XG4gICAgbGV0IHF1ZXJ5OiBDc3NBdFJ1bGVQcmVkaWNhdGVBc3Q7XG4gICAgc3dpdGNoICh0eXBlKSB7XG4gICAgICBjYXNlIEJsb2NrVHlwZS5DaGFyc2V0OlxuICAgICAgY2FzZSBCbG9ja1R5cGUuTmFtZXNwYWNlOlxuICAgICAgY2FzZSBCbG9ja1R5cGUuSW1wb3J0OlxuICAgICAgICBsZXQgdmFsdWUgPSB0aGlzLl9wYXJzZVZhbHVlKGRlbGltaXRlcnMpO1xuICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkJMT0NLKTtcbiAgICAgICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgdmFsdWUpO1xuICAgICAgICByZXR1cm4gbmV3IENzc0lubGluZVJ1bGVBc3Qoc3BhbiwgdHlwZSwgdmFsdWUpO1xuXG4gICAgICBjYXNlIEJsb2NrVHlwZS5WaWV3cG9ydDpcbiAgICAgIGNhc2UgQmxvY2tUeXBlLkZvbnRGYWNlOlxuICAgICAgICBibG9jayA9IHRoaXMuX3BhcnNlU3R5bGVCbG9jayhkZWxpbWl0ZXJzKSAhO1xuICAgICAgICBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGJsb2NrKTtcbiAgICAgICAgcmV0dXJuIG5ldyBDc3NCbG9ja1J1bGVBc3Qoc3BhbiwgdHlwZSwgYmxvY2spO1xuXG4gICAgICBjYXNlIEJsb2NrVHlwZS5LZXlmcmFtZXM6XG4gICAgICAgIHRva2VucyA9IHRoaXMuX2NvbGxlY3RVbnRpbERlbGltKGRlbGltaXRlcnMgfCBSQlJBQ0VfREVMSU1fRkxBRyB8IExCUkFDRV9ERUxJTV9GTEFHKTtcbiAgICAgICAgLy8ga2V5ZnJhbWVzIG9ubHkgaGF2ZSBvbmUgaWRlbnRpZmllciBuYW1lXG4gICAgICAgIGxldCBuYW1lID0gdG9rZW5zWzBdO1xuICAgICAgICBibG9jayA9IHRoaXMuX3BhcnNlS2V5ZnJhbWVCbG9jayhkZWxpbWl0ZXJzKTtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBibG9jayk7XG4gICAgICAgIHJldHVybiBuZXcgQ3NzS2V5ZnJhbWVSdWxlQXN0KHNwYW4sIG5hbWUsIGJsb2NrKTtcblxuICAgICAgY2FzZSBCbG9ja1R5cGUuTWVkaWFRdWVyeTpcbiAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5NRURJQV9RVUVSWSk7XG4gICAgICAgIHRva2VucyA9IHRoaXMuX2NvbGxlY3RVbnRpbERlbGltKGRlbGltaXRlcnMgfCBSQlJBQ0VfREVMSU1fRkxBRyB8IExCUkFDRV9ERUxJTV9GTEFHKTtcbiAgICAgICAgZW5kVG9rZW4gPSB0b2tlbnNbdG9rZW5zLmxlbmd0aCAtIDFdO1xuICAgICAgICAvLyB3ZSBkbyBub3QgdHJhY2sgdGhlIHdoaXRlc3BhY2UgYWZ0ZXIgdGhlIG1lZGlhUXVlcnkgcHJlZGljYXRlIGVuZHNcbiAgICAgICAgLy8gc28gd2UgaGF2ZSB0byBjYWxjdWxhdGUgdGhlIGVuZCBzdHJpbmcgdmFsdWUgb24gb3VyIG93blxuICAgICAgICBlbmQgPSBlbmRUb2tlbi5pbmRleCArIGVuZFRva2VuLnN0clZhbHVlLmxlbmd0aCAtIDE7XG4gICAgICAgIHN0clZhbHVlID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIGVuZCk7XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgZW5kVG9rZW4pO1xuICAgICAgICBxdWVyeSA9IG5ldyBDc3NBdFJ1bGVQcmVkaWNhdGVBc3Qoc3Bhbiwgc3RyVmFsdWUsIHRva2Vucyk7XG4gICAgICAgIGJsb2NrID0gdGhpcy5fcGFyc2VCbG9jayhkZWxpbWl0ZXJzKTtcbiAgICAgICAgc3RyVmFsdWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCkgLSAxKTtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBibG9jayk7XG4gICAgICAgIHJldHVybiBuZXcgQ3NzTWVkaWFRdWVyeVJ1bGVBc3Qoc3Bhbiwgc3RyVmFsdWUsIHF1ZXJ5LCBibG9jayk7XG5cbiAgICAgIGNhc2UgQmxvY2tUeXBlLkRvY3VtZW50OlxuICAgICAgY2FzZSBCbG9ja1R5cGUuU3VwcG9ydHM6XG4gICAgICBjYXNlIEJsb2NrVHlwZS5QYWdlOlxuICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkFUX1JVTEVfUVVFUlkpO1xuICAgICAgICB0b2tlbnMgPSB0aGlzLl9jb2xsZWN0VW50aWxEZWxpbShkZWxpbWl0ZXJzIHwgUkJSQUNFX0RFTElNX0ZMQUcgfCBMQlJBQ0VfREVMSU1fRkxBRyk7XG4gICAgICAgIGVuZFRva2VuID0gdG9rZW5zW3Rva2Vucy5sZW5ndGggLSAxXTtcbiAgICAgICAgLy8gd2UgZG8gbm90IHRyYWNrIHRoZSB3aGl0ZXNwYWNlIGFmdGVyIHRoaXMgYmxvY2sgcnVsZSBwcmVkaWNhdGUgZW5kc1xuICAgICAgICAvLyBzbyB3ZSBoYXZlIHRvIGNhbGN1bGF0ZSB0aGUgZW5kIHN0cmluZyB2YWx1ZSBvbiBvdXIgb3duXG4gICAgICAgIGVuZCA9IGVuZFRva2VuLmluZGV4ICsgZW5kVG9rZW4uc3RyVmFsdWUubGVuZ3RoIC0gMTtcbiAgICAgICAgc3RyVmFsdWUgPSB0aGlzLl9leHRyYWN0U291cmNlQ29udGVudChzdGFydCwgZW5kKTtcbiAgICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCB0b2tlbnNbdG9rZW5zLmxlbmd0aCAtIDFdKTtcbiAgICAgICAgcXVlcnkgPSBuZXcgQ3NzQXRSdWxlUHJlZGljYXRlQXN0KHNwYW4sIHN0clZhbHVlLCB0b2tlbnMpO1xuICAgICAgICBibG9jayA9IHRoaXMuX3BhcnNlQmxvY2soZGVsaW1pdGVycyk7XG4gICAgICAgIHN0clZhbHVlID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIGJsb2NrLmVuZC5vZmZzZXQgISk7XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgYmxvY2spO1xuICAgICAgICByZXR1cm4gbmV3IENzc0Jsb2NrRGVmaW5pdGlvblJ1bGVBc3Qoc3Bhbiwgc3RyVmFsdWUsIHR5cGUsIHF1ZXJ5LCBibG9jayk7XG5cbiAgICAgIC8vIGlmIGEgY3VzdG9tIEBydWxlIHsgLi4uIH0gaXMgdXNlZCBpdCBzaG91bGQgc3RpbGwgdG9rZW5pemUgdGhlIGluc2lkZXNcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIGxldCBsaXN0T2ZUb2tlbnM6IENzc1Rva2VuW10gPSBbXTtcbiAgICAgICAgbGV0IHRva2VuTmFtZSA9IHRva2VuLnN0clZhbHVlO1xuICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkFMTCk7XG4gICAgICAgIHRoaXMuX2Vycm9yKFxuICAgICAgICAgICAgZ2VuZXJhdGVFcnJvck1lc3NhZ2UoXG4gICAgICAgICAgICAgICAgdGhpcy5fZ2V0U291cmNlQ29udGVudCgpLFxuICAgICAgICAgICAgICAgIGBUaGUgQ1NTIFwiYXRcIiBydWxlIFwiJHt0b2tlbk5hbWV9XCIgaXMgbm90IGFsbG93ZWQgdG8gdXNlZCBoZXJlYCwgdG9rZW4uc3RyVmFsdWUsXG4gICAgICAgICAgICAgICAgdG9rZW4uaW5kZXgsIHRva2VuLmxpbmUsIHRva2VuLmNvbHVtbiksXG4gICAgICAgICAgICB0b2tlbik7XG5cbiAgICAgICAgdGhpcy5fY29sbGVjdFVudGlsRGVsaW0oZGVsaW1pdGVycyB8IExCUkFDRV9ERUxJTV9GTEFHIHwgU0VNSUNPTE9OX0RFTElNX0ZMQUcpXG4gICAgICAgICAgICAuZm9yRWFjaCgodG9rZW4pID0+IHsgbGlzdE9mVG9rZW5zLnB1c2godG9rZW4pOyB9KTtcbiAgICAgICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayA9PSBjaGFycy4kTEJSQUNFKSB7XG4gICAgICAgICAgbGlzdE9mVG9rZW5zLnB1c2godGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAneycpKTtcbiAgICAgICAgICB0aGlzLl9jb2xsZWN0VW50aWxEZWxpbShkZWxpbWl0ZXJzIHwgUkJSQUNFX0RFTElNX0ZMQUcgfCBMQlJBQ0VfREVMSU1fRkxBRylcbiAgICAgICAgICAgICAgLmZvckVhY2goKHRva2VuKSA9PiB7IGxpc3RPZlRva2Vucy5wdXNoKHRva2VuKTsgfSk7XG4gICAgICAgICAgbGlzdE9mVG9rZW5zLnB1c2godGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnfScpKTtcbiAgICAgICAgfVxuICAgICAgICBlbmRUb2tlbiA9IGxpc3RPZlRva2Vuc1tsaXN0T2ZUb2tlbnMubGVuZ3RoIC0gMV07XG4gICAgICAgIHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgZW5kVG9rZW4pO1xuICAgICAgICByZXR1cm4gbmV3IENzc1Vua25vd25SdWxlQXN0KHNwYW4sIHRva2VuTmFtZSwgbGlzdE9mVG9rZW5zKTtcbiAgICB9XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVNlbGVjdG9yUnVsZShkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NSdWxlQXN0IHtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpO1xuICAgIGNvbnN0IHNlbGVjdG9ycyA9IHRoaXMuX3BhcnNlU2VsZWN0b3JzKGRlbGltaXRlcnMpO1xuICAgIGNvbnN0IGJsb2NrID0gdGhpcy5fcGFyc2VTdHlsZUJsb2NrKGRlbGltaXRlcnMpO1xuICAgIGxldCBydWxlQXN0OiBDc3NSdWxlQXN0O1xuICAgIGxldCBzcGFuOiBQYXJzZVNvdXJjZVNwYW47XG4gICAgY29uc3Qgc3RhcnRTZWxlY3RvciA9IHNlbGVjdG9yc1swXTtcbiAgICBpZiAoYmxvY2sgIT0gbnVsbCkge1xuICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFNlbGVjdG9yLCBibG9jayk7XG4gICAgICBydWxlQXN0ID0gbmV3IENzc1NlbGVjdG9yUnVsZUFzdChzcGFuLCBzZWxlY3RvcnMsIGJsb2NrKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY29uc3QgbmFtZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCB0aGlzLl9nZXRTY2FubmVySW5kZXgoKSAtIDEpO1xuICAgICAgY29uc3QgaW5uZXJUb2tlbnM6IENzc1Rva2VuW10gPSBbXTtcbiAgICAgIHNlbGVjdG9ycy5mb3JFYWNoKChzZWxlY3RvcjogQ3NzU2VsZWN0b3JBc3QpID0+IHtcbiAgICAgICAgc2VsZWN0b3Iuc2VsZWN0b3JQYXJ0cy5mb3JFYWNoKChwYXJ0OiBDc3NTaW1wbGVTZWxlY3RvckFzdCkgPT4ge1xuICAgICAgICAgIHBhcnQudG9rZW5zLmZvckVhY2goKHRva2VuOiBDc3NUb2tlbikgPT4geyBpbm5lclRva2Vucy5wdXNoKHRva2VuKTsgfSk7XG4gICAgICAgIH0pO1xuICAgICAgfSk7XG4gICAgICBjb25zdCBlbmRUb2tlbiA9IGlubmVyVG9rZW5zW2lubmVyVG9rZW5zLmxlbmd0aCAtIDFdO1xuICAgICAgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFNlbGVjdG9yLCBlbmRUb2tlbik7XG4gICAgICBydWxlQXN0ID0gbmV3IENzc1Vua25vd25Ub2tlbkxpc3RBc3Qoc3BhbiwgbmFtZSwgaW5uZXJUb2tlbnMpO1xuICAgIH1cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkJMT0NLKTtcbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVFbXB0eVN0YXRlbWVudHMoKTtcbiAgICByZXR1cm4gcnVsZUFzdDtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlU2VsZWN0b3JzKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1NlbGVjdG9yQXN0W10ge1xuICAgIGRlbGltaXRlcnMgfD0gTEJSQUNFX0RFTElNX0ZMQUcgfCBTRU1JQ09MT05fREVMSU1fRkxBRztcblxuICAgIGNvbnN0IHNlbGVjdG9yczogQ3NzU2VsZWN0b3JBc3RbXSA9IFtdO1xuICAgIGxldCBpc1BhcnNpbmdTZWxlY3RvcnMgPSB0cnVlO1xuICAgIHdoaWxlIChpc1BhcnNpbmdTZWxlY3RvcnMpIHtcbiAgICAgIHNlbGVjdG9ycy5wdXNoKHRoaXMuX3BhcnNlU2VsZWN0b3IoZGVsaW1pdGVycykpO1xuXG4gICAgICBpc1BhcnNpbmdTZWxlY3RvcnMgPSAhY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKTtcblxuICAgICAgaWYgKGlzUGFyc2luZ1NlbGVjdG9ycykge1xuICAgICAgICB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICcsJyk7XG4gICAgICAgIGlzUGFyc2luZ1NlbGVjdG9ycyA9ICFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpO1xuICAgICAgICBpZiAoaXNQYXJzaW5nU2VsZWN0b3JzKSB7XG4gICAgICAgICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lV2hpdGVzcGFjZSgpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgcmV0dXJuIHNlbGVjdG9ycztcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3NjYW4oKTogQ3NzVG9rZW4ge1xuICAgIGNvbnN0IG91dHB1dCA9IHRoaXMuX3NjYW5uZXIuc2NhbigpICE7XG4gICAgY29uc3QgdG9rZW4gPSBvdXRwdXQudG9rZW47XG4gICAgY29uc3QgZXJyb3IgPSBvdXRwdXQuZXJyb3I7XG4gICAgaWYgKGVycm9yICE9IG51bGwpIHtcbiAgICAgIHRoaXMuX2Vycm9yKGdldFJhd01lc3NhZ2UoZXJyb3IpLCB0b2tlbik7XG4gICAgfVxuICAgIHRoaXMuX2xhc3RUb2tlbiA9IHRva2VuO1xuICAgIHJldHVybiB0b2tlbjtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2dldFNjYW5uZXJJbmRleCgpOiBudW1iZXIgeyByZXR1cm4gdGhpcy5fc2Nhbm5lci5pbmRleDsgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2NvbnN1bWUodHlwZTogQ3NzVG9rZW5UeXBlLCB2YWx1ZTogc3RyaW5nfG51bGwgPSBudWxsKTogQ3NzVG9rZW4ge1xuICAgIGNvbnN0IG91dHB1dCA9IHRoaXMuX3NjYW5uZXIuY29uc3VtZSh0eXBlLCB2YWx1ZSk7XG4gICAgY29uc3QgdG9rZW4gPSBvdXRwdXQudG9rZW47XG4gICAgY29uc3QgZXJyb3IgPSBvdXRwdXQuZXJyb3I7XG4gICAgaWYgKGVycm9yICE9IG51bGwpIHtcbiAgICAgIHRoaXMuX2Vycm9yKGdldFJhd01lc3NhZ2UoZXJyb3IpLCB0b2tlbik7XG4gICAgfVxuICAgIHRoaXMuX2xhc3RUb2tlbiA9IHRva2VuO1xuICAgIHJldHVybiB0b2tlbjtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlS2V5ZnJhbWVCbG9jayhkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NCbG9ja0FzdCB7XG4gICAgZGVsaW1pdGVycyB8PSBSQlJBQ0VfREVMSU1fRkxBRztcbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLktFWUZSQU1FX0JMT0NLKTtcblxuICAgIGNvbnN0IHN0YXJ0VG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd7Jyk7XG5cbiAgICBjb25zdCBkZWZpbml0aW9uczogQ3NzS2V5ZnJhbWVEZWZpbml0aW9uQXN0W10gPSBbXTtcbiAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIGRlZmluaXRpb25zLnB1c2godGhpcy5fcGFyc2VLZXlmcmFtZURlZmluaXRpb24oZGVsaW1pdGVycykpO1xuICAgIH1cblxuICAgIGNvbnN0IGVuZFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnfScpO1xuXG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBlbmRUb2tlbik7XG4gICAgcmV0dXJuIG5ldyBDc3NCbG9ja0FzdChzcGFuLCBkZWZpbml0aW9ucyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZUtleWZyYW1lRGVmaW5pdGlvbihkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NLZXlmcmFtZURlZmluaXRpb25Bc3Qge1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCk7XG4gICAgY29uc3Qgc3RlcFRva2VuczogQ3NzVG9rZW5bXSA9IFtdO1xuICAgIGRlbGltaXRlcnMgfD0gTEJSQUNFX0RFTElNX0ZMQUc7XG4gICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICBzdGVwVG9rZW5zLnB1c2godGhpcy5fcGFyc2VLZXlmcmFtZUxhYmVsKGRlbGltaXRlcnMgfCBDT01NQV9ERUxJTV9GTEFHKSk7XG4gICAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrICE9IGNoYXJzLiRMQlJBQ0UpIHtcbiAgICAgICAgdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnLCcpO1xuICAgICAgfVxuICAgIH1cbiAgICBjb25zdCBzdHlsZXNCbG9jayA9IHRoaXMuX3BhcnNlU3R5bGVCbG9jayhkZWxpbWl0ZXJzIHwgUkJSQUNFX0RFTElNX0ZMQUcpO1xuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RlcFRva2Vuc1swXSwgc3R5bGVzQmxvY2spO1xuICAgIGNvbnN0IGFzdCA9IG5ldyBDc3NLZXlmcmFtZURlZmluaXRpb25Bc3Qoc3Bhbiwgc3RlcFRva2Vucywgc3R5bGVzQmxvY2sgISk7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkJMT0NLKTtcbiAgICByZXR1cm4gYXN0O1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VLZXlmcmFtZUxhYmVsKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1Rva2VuIHtcbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLktFWUZSQU1FX0JMT0NLKTtcbiAgICByZXR1cm4gbWVyZ2VUb2tlbnModGhpcy5fY29sbGVjdFVudGlsRGVsaW0oZGVsaW1pdGVycykpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VQc2V1ZG9TZWxlY3RvcihkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NQc2V1ZG9TZWxlY3RvckFzdCB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLl9nZXRTY2FubmVySW5kZXgoKTtcblxuICAgIGRlbGltaXRlcnMgJj0gfkNPTU1BX0RFTElNX0ZMQUc7XG5cbiAgICAvLyB3ZSBrZWVwIHRoZSBvcmlnaW5hbCB2YWx1ZSBzaW5jZSB3ZSBtYXkgdXNlIGl0IHRvIHJlY3Vyc2Ugd2hlbiA6bm90LCA6aG9zdCBhcmUgdXNlZFxuICAgIGNvbnN0IHN0YXJ0aW5nRGVsaW1zID0gZGVsaW1pdGVycztcblxuICAgIGNvbnN0IHN0YXJ0VG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICc6Jyk7XG4gICAgY29uc3QgdG9rZW5zID0gW3N0YXJ0VG9rZW5dO1xuXG4gICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayA9PSBjaGFycy4kQ09MT04pIHsgIC8vIDo6c29tZXRoaW5nXG4gICAgICB0b2tlbnMucHVzaCh0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICc6JykpO1xuICAgIH1cblxuICAgIGNvbnN0IGlubmVyU2VsZWN0b3JzOiBDc3NTZWxlY3RvckFzdFtdID0gW107XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlBTRVVET19TRUxFQ1RPUik7XG5cbiAgICAvLyBob3N0LCBob3N0LWNvbnRleHQsIGxhbmcsIG5vdCwgbnRoLWNoaWxkIGFyZSBhbGwgaWRlbnRpZmllcnNcbiAgICBjb25zdCBwc2V1ZG9TZWxlY3RvclRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuSWRlbnRpZmllcik7XG4gICAgY29uc3QgcHNldWRvU2VsZWN0b3JOYW1lID0gcHNldWRvU2VsZWN0b3JUb2tlbi5zdHJWYWx1ZTtcbiAgICB0b2tlbnMucHVzaChwc2V1ZG9TZWxlY3RvclRva2VuKTtcblxuICAgIC8vIGhvc3QoKSwgbGFuZygpLCBudGgtY2hpbGQoKSwgZXRjLi4uXG4gICAgaWYgKHRoaXMuX3NjYW5uZXIucGVlayA9PSBjaGFycy4kTFBBUkVOKSB7XG4gICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlBTRVVET19TRUxFQ1RPUl9XSVRIX0FSR1VNRU5UUyk7XG5cbiAgICAgIGNvbnN0IG9wZW5QYXJlblRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnKCcpO1xuICAgICAgdG9rZW5zLnB1c2gob3BlblBhcmVuVG9rZW4pO1xuXG4gICAgICAvLyA6aG9zdChpbm5lclNlbGVjdG9yKHMpKSwgOm5vdChzZWxlY3RvciksIGV0Yy4uLlxuICAgICAgaWYgKF9wc2V1ZG9TZWxlY3RvclN1cHBvcnRzSW5uZXJTZWxlY3RvcnMocHNldWRvU2VsZWN0b3JOYW1lKSkge1xuICAgICAgICBsZXQgaW5uZXJEZWxpbXMgPSBzdGFydGluZ0RlbGltcyB8IExQQVJFTl9ERUxJTV9GTEFHIHwgUlBBUkVOX0RFTElNX0ZMQUc7XG4gICAgICAgIGlmIChwc2V1ZG9TZWxlY3Rvck5hbWUgPT0gJ25vdCcpIHtcbiAgICAgICAgICAvLyB0aGUgaW5uZXIgc2VsZWN0b3IgaW5zaWRlIG9mIDpub3QoLi4uKSBjYW4gb25seSBiZSBvbmVcbiAgICAgICAgICAvLyBDU1Mgc2VsZWN0b3IgKG5vIGNvbW1hcyBhbGxvd2VkKSAuLi4gVGhpcyBpcyBhY2NvcmRpbmdcbiAgICAgICAgICAvLyB0byB0aGUgQ1NTIHNwZWNpZmljYXRpb25cbiAgICAgICAgICBpbm5lckRlbGltcyB8PSBDT01NQV9ERUxJTV9GTEFHO1xuICAgICAgICB9XG5cbiAgICAgICAgLy8gOmhvc3QoYSwgYiwgYykge1xuICAgICAgICB0aGlzLl9wYXJzZVNlbGVjdG9ycyhpbm5lckRlbGltcykuZm9yRWFjaCgoc2VsZWN0b3IsIGluZGV4KSA9PiB7XG4gICAgICAgICAgaW5uZXJTZWxlY3RvcnMucHVzaChzZWxlY3Rvcik7XG4gICAgICAgIH0pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgLy8gdGhpcyBicmFuY2ggaXMgZm9yIHRoaW5ncyBsaWtlIFwiZW4tdXMsIDJrICsgMSwgZXRjLi4uXCJcbiAgICAgICAgLy8gd2hpY2ggYWxsIGVuZCB1cCBpbiBwc2V1ZG9TZWxlY3RvcnMgbGlrZSA6bGFuZywgOm50aC1jaGlsZCwgZXRjLi5cbiAgICAgICAgY29uc3QgaW5uZXJWYWx1ZURlbGltcyA9IGRlbGltaXRlcnMgfCBMQlJBQ0VfREVMSU1fRkxBRyB8IENPTE9OX0RFTElNX0ZMQUcgfFxuICAgICAgICAgICAgUlBBUkVOX0RFTElNX0ZMQUcgfCBMUEFSRU5fREVMSU1fRkxBRztcbiAgICAgICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGlubmVyVmFsdWVEZWxpbXMpKSB7XG4gICAgICAgICAgY29uc3QgdG9rZW4gPSB0aGlzLl9zY2FuKCk7XG4gICAgICAgICAgdG9rZW5zLnB1c2godG9rZW4pO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGNsb3NlUGFyZW5Ub2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJyknKTtcbiAgICAgIHRva2Vucy5wdXNoKGNsb3NlUGFyZW5Ub2tlbik7XG4gICAgfVxuXG4gICAgY29uc3QgZW5kID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCkgLSAxO1xuICAgIGNvbnN0IHN0clZhbHVlID0gdGhpcy5fZXh0cmFjdFNvdXJjZUNvbnRlbnQoc3RhcnQsIGVuZCk7XG5cbiAgICBjb25zdCBlbmRUb2tlbiA9IHRva2Vuc1t0b2tlbnMubGVuZ3RoIC0gMV07XG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3BhbihzdGFydFRva2VuLCBlbmRUb2tlbik7XG4gICAgcmV0dXJuIG5ldyBDc3NQc2V1ZG9TZWxlY3RvckFzdChzcGFuLCBzdHJWYWx1ZSwgcHNldWRvU2VsZWN0b3JOYW1lLCB0b2tlbnMsIGlubmVyU2VsZWN0b3JzKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlU2ltcGxlU2VsZWN0b3IoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzU2ltcGxlU2VsZWN0b3JBc3Qge1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCk7XG5cbiAgICBkZWxpbWl0ZXJzIHw9IENPTU1BX0RFTElNX0ZMQUc7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNFTEVDVE9SKTtcbiAgICBjb25zdCBzZWxlY3RvckNzc1Rva2VuczogQ3NzVG9rZW5bXSA9IFtdO1xuICAgIGNvbnN0IHBzZXVkb1NlbGVjdG9yczogQ3NzUHNldWRvU2VsZWN0b3JBc3RbXSA9IFtdO1xuXG4gICAgbGV0IHByZXZpb3VzVG9rZW46IENzc1Rva2VuID0gdW5kZWZpbmVkICE7XG5cbiAgICBjb25zdCBzZWxlY3RvclBhcnREZWxpbWl0ZXJzID0gZGVsaW1pdGVycyB8IFNQQUNFX0RFTElNX0ZMQUc7XG4gICAgbGV0IGxvb3BPdmVyU2VsZWN0b3IgPSAhY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBzZWxlY3RvclBhcnREZWxpbWl0ZXJzKTtcblxuICAgIGxldCBoYXNBdHRyaWJ1dGVFcnJvciA9IGZhbHNlO1xuICAgIHdoaWxlIChsb29wT3ZlclNlbGVjdG9yKSB7XG4gICAgICBjb25zdCBwZWVrID0gdGhpcy5fc2Nhbm5lci5wZWVrO1xuXG4gICAgICBzd2l0Y2ggKHBlZWspIHtcbiAgICAgICAgY2FzZSBjaGFycy4kQ09MT046XG4gICAgICAgICAgbGV0IGlubmVyUHNldWRvID0gdGhpcy5fcGFyc2VQc2V1ZG9TZWxlY3RvcihkZWxpbWl0ZXJzKTtcbiAgICAgICAgICBwc2V1ZG9TZWxlY3RvcnMucHVzaChpbm5lclBzZXVkbyk7XG4gICAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TRUxFQ1RPUik7XG4gICAgICAgICAgYnJlYWs7XG5cbiAgICAgICAgY2FzZSBjaGFycy4kTEJSQUNLRVQ6XG4gICAgICAgICAgLy8gd2Ugc2V0IHRoZSBtb2RlIGFmdGVyIHRoZSBzY2FuIGJlY2F1c2UgYXR0cmlidXRlIG1vZGUgZG9lcyBub3RcbiAgICAgICAgICAvLyBhbGxvdyBhdHRyaWJ1dGUgW10gdmFsdWVzLiBBbmQgdGhpcyBhbHNvIHdpbGwgY2F0Y2ggYW55IGVycm9yc1xuICAgICAgICAgIC8vIGlmIGFuIGV4dHJhIFwiW1wiIGlzIHVzZWQgaW5zaWRlLlxuICAgICAgICAgIHNlbGVjdG9yQ3NzVG9rZW5zLnB1c2godGhpcy5fc2NhbigpKTtcbiAgICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkFUVFJJQlVURV9TRUxFQ1RPUik7XG4gICAgICAgICAgYnJlYWs7XG5cbiAgICAgICAgY2FzZSBjaGFycy4kUkJSQUNLRVQ6XG4gICAgICAgICAgaWYgKHRoaXMuX3NjYW5uZXIuZ2V0TW9kZSgpICE9IENzc0xleGVyTW9kZS5BVFRSSUJVVEVfU0VMRUNUT1IpIHtcbiAgICAgICAgICAgIGhhc0F0dHJpYnV0ZUVycm9yID0gdHJ1ZTtcbiAgICAgICAgICB9XG4gICAgICAgICAgLy8gd2Ugc2V0IHRoZSBtb2RlIGVhcmx5IGJlY2F1c2UgYXR0cmlidXRlIG1vZGUgZG9lcyBub3RcbiAgICAgICAgICAvLyBhbGxvdyBhdHRyaWJ1dGUgW10gdmFsdWVzXG4gICAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TRUxFQ1RPUik7XG4gICAgICAgICAgc2VsZWN0b3JDc3NUb2tlbnMucHVzaCh0aGlzLl9zY2FuKCkpO1xuICAgICAgICAgIGJyZWFrO1xuXG4gICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgaWYgKGlzU2VsZWN0b3JPcGVyYXRvckNoYXJhY3RlcihwZWVrKSkge1xuICAgICAgICAgICAgbG9vcE92ZXJTZWxlY3RvciA9IGZhbHNlO1xuICAgICAgICAgICAgY29udGludWU7XG4gICAgICAgICAgfVxuXG4gICAgICAgICAgbGV0IHRva2VuID0gdGhpcy5fc2NhbigpO1xuICAgICAgICAgIHByZXZpb3VzVG9rZW4gPSB0b2tlbjtcbiAgICAgICAgICBzZWxlY3RvckNzc1Rva2Vucy5wdXNoKHRva2VuKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgIH1cblxuICAgICAgbG9vcE92ZXJTZWxlY3RvciA9ICFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIHNlbGVjdG9yUGFydERlbGltaXRlcnMpO1xuICAgIH1cblxuICAgIGhhc0F0dHJpYnV0ZUVycm9yID1cbiAgICAgICAgaGFzQXR0cmlidXRlRXJyb3IgfHwgdGhpcy5fc2Nhbm5lci5nZXRNb2RlKCkgPT0gQ3NzTGV4ZXJNb2RlLkFUVFJJQlVURV9TRUxFQ1RPUjtcbiAgICBpZiAoaGFzQXR0cmlidXRlRXJyb3IpIHtcbiAgICAgIHRoaXMuX2Vycm9yKFxuICAgICAgICAgIGBVbmJhbGFuY2VkIENTUyBhdHRyaWJ1dGUgc2VsZWN0b3IgYXQgY29sdW1uICR7cHJldmlvdXNUb2tlbi5saW5lfToke3ByZXZpb3VzVG9rZW4uY29sdW1ufWAsXG4gICAgICAgICAgcHJldmlvdXNUb2tlbik7XG4gICAgfVxuXG4gICAgbGV0IGVuZCA9IHRoaXMuX2dldFNjYW5uZXJJbmRleCgpIC0gMTtcblxuICAgIC8vIHRoaXMgaGFwcGVucyBpZiB0aGUgc2VsZWN0b3IgaXMgbm90IGRpcmVjdGx5IGZvbGxvd2VkIGJ5XG4gICAgLy8gYSBjb21tYSBvciBjdXJseSBicmFjZSB3aXRob3V0IGEgc3BhY2UgaW4gYmV0d2VlblxuICAgIGxldCBvcGVyYXRvcjogQ3NzVG9rZW58bnVsbCA9IG51bGw7XG4gICAgbGV0IG9wZXJhdG9yU2NhbkNvdW50ID0gMDtcbiAgICBsZXQgbGFzdE9wZXJhdG9yVG9rZW46IENzc1Rva2VufG51bGwgPSBudWxsO1xuICAgIGlmICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgd2hpbGUgKG9wZXJhdG9yID09IG51bGwgJiYgIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykgJiZcbiAgICAgICAgICAgICBpc1NlbGVjdG9yT3BlcmF0b3JDaGFyYWN0ZXIodGhpcy5fc2Nhbm5lci5wZWVrKSkge1xuICAgICAgICBsZXQgdG9rZW4gPSB0aGlzLl9zY2FuKCk7XG4gICAgICAgIGNvbnN0IHRva2VuT3BlcmF0b3IgPSB0b2tlbi5zdHJWYWx1ZTtcbiAgICAgICAgb3BlcmF0b3JTY2FuQ291bnQrKztcbiAgICAgICAgbGFzdE9wZXJhdG9yVG9rZW4gPSB0b2tlbjtcbiAgICAgICAgaWYgKHRva2VuT3BlcmF0b3IgIT0gU1BBQ0VfT1BFUkFUT1IpIHtcbiAgICAgICAgICBzd2l0Y2ggKHRva2VuT3BlcmF0b3IpIHtcbiAgICAgICAgICAgIGNhc2UgU0xBU0hfQ0hBUkFDVEVSOlxuICAgICAgICAgICAgICAvLyAvZGVlcC8gb3BlcmF0b3JcbiAgICAgICAgICAgICAgbGV0IGRlZXBUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIpO1xuICAgICAgICAgICAgICBsZXQgZGVlcFNsYXNoID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyKTtcbiAgICAgICAgICAgICAgbGV0IGluZGV4ID0gbGFzdE9wZXJhdG9yVG9rZW4uaW5kZXg7XG4gICAgICAgICAgICAgIGxldCBsaW5lID0gbGFzdE9wZXJhdG9yVG9rZW4ubGluZTtcbiAgICAgICAgICAgICAgbGV0IGNvbHVtbiA9IGxhc3RPcGVyYXRvclRva2VuLmNvbHVtbjtcbiAgICAgICAgICAgICAgaWYgKGRlZXBUb2tlbiAhPSBudWxsICYmIGRlZXBUb2tlbi5zdHJWYWx1ZS50b0xvd2VyQ2FzZSgpID09ICdkZWVwJyAmJlxuICAgICAgICAgICAgICAgICAgZGVlcFNsYXNoLnN0clZhbHVlID09IFNMQVNIX0NIQVJBQ1RFUikge1xuICAgICAgICAgICAgICAgIHRva2VuID0gbmV3IENzc1Rva2VuKFxuICAgICAgICAgICAgICAgICAgICBsYXN0T3BlcmF0b3JUb2tlbi5pbmRleCwgbGFzdE9wZXJhdG9yVG9rZW4uY29sdW1uLCBsYXN0T3BlcmF0b3JUb2tlbi5saW5lLFxuICAgICAgICAgICAgICAgICAgICBDc3NUb2tlblR5cGUuSWRlbnRpZmllciwgREVFUF9PUEVSQVRPUl9TVFIpO1xuICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgIGNvbnN0IHRleHQgPSBTTEFTSF9DSEFSQUNURVIgKyBkZWVwVG9rZW4uc3RyVmFsdWUgKyBkZWVwU2xhc2guc3RyVmFsdWU7XG4gICAgICAgICAgICAgICAgdGhpcy5fZXJyb3IoXG4gICAgICAgICAgICAgICAgICAgIGdlbmVyYXRlRXJyb3JNZXNzYWdlKFxuICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5fZ2V0U291cmNlQ29udGVudCgpLCBgJHt0ZXh0fSBpcyBhbiBpbnZhbGlkIENTUyBvcGVyYXRvcmAsIHRleHQsIGluZGV4LFxuICAgICAgICAgICAgICAgICAgICAgICAgbGluZSwgY29sdW1uKSxcbiAgICAgICAgICAgICAgICAgICAgbGFzdE9wZXJhdG9yVG9rZW4pO1xuICAgICAgICAgICAgICAgIHRva2VuID0gbmV3IENzc1Rva2VuKGluZGV4LCBjb2x1bW4sIGxpbmUsIENzc1Rva2VuVHlwZS5JbnZhbGlkLCB0ZXh0KTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICBicmVhaztcblxuICAgICAgICAgICAgY2FzZSBHVF9DSEFSQUNURVI6XG4gICAgICAgICAgICAgIC8vID4+PiBvcGVyYXRvclxuICAgICAgICAgICAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrID09IGNoYXJzLiRHVCAmJiB0aGlzLl9zY2FubmVyLnBlZWtQZWVrID09IGNoYXJzLiRHVCkge1xuICAgICAgICAgICAgICAgIHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgR1RfQ0hBUkFDVEVSKTtcbiAgICAgICAgICAgICAgICB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsIEdUX0NIQVJBQ1RFUik7XG4gICAgICAgICAgICAgICAgdG9rZW4gPSBuZXcgQ3NzVG9rZW4oXG4gICAgICAgICAgICAgICAgICAgIGxhc3RPcGVyYXRvclRva2VuLmluZGV4LCBsYXN0T3BlcmF0b3JUb2tlbi5jb2x1bW4sIGxhc3RPcGVyYXRvclRva2VuLmxpbmUsXG4gICAgICAgICAgICAgICAgICAgIENzc1Rva2VuVHlwZS5JZGVudGlmaWVyLCBUUklQTEVfR1RfT1BFUkFUT1JfU1RSKTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBvcGVyYXRvciA9IHRva2VuO1xuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIC8vIHNvIGxvbmcgYXMgdGhlcmUgaXMgYW4gb3BlcmF0b3IgdGhlbiB3ZSBjYW4gaGF2ZSBhblxuICAgICAgLy8gZW5kaW5nIHZhbHVlIHRoYXQgaXMgYmV5b25kIHRoZSBzZWxlY3RvciB2YWx1ZSAuLi5cbiAgICAgIC8vIG90aGVyd2lzZSBpdCdzIGp1c3QgYSBidW5jaCBvZiB0cmFpbGluZyB3aGl0ZXNwYWNlXG4gICAgICBpZiAob3BlcmF0b3IgIT0gbnVsbCkge1xuICAgICAgICBlbmQgPSBvcGVyYXRvci5pbmRleDtcbiAgICAgIH1cbiAgICB9XG5cbiAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG5cbiAgICBjb25zdCBzdHJWYWx1ZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCBlbmQpO1xuXG4gICAgLy8gaWYgd2UgZG8gY29tZSBhY3Jvc3Mgb25lIG9yIG1vcmUgc3BhY2VzIGluc2lkZSBvZlxuICAgIC8vIHRoZSBvcGVyYXRvcnMgbG9vcCB0aGVuIGFuIGVtcHR5IHNwYWNlIGlzIHN0aWxsIGFcbiAgICAvLyB2YWxpZCBvcGVyYXRvciB0byB1c2UgaWYgc29tZXRoaW5nIGVsc2Ugd2FzIG5vdCBmb3VuZFxuICAgIGlmIChvcGVyYXRvciA9PSBudWxsICYmIG9wZXJhdG9yU2NhbkNvdW50ID4gMCAmJiB0aGlzLl9zY2FubmVyLnBlZWsgIT0gY2hhcnMuJExCUkFDRSkge1xuICAgICAgb3BlcmF0b3IgPSBsYXN0T3BlcmF0b3JUb2tlbjtcbiAgICB9XG5cbiAgICAvLyBwbGVhc2Ugbm90ZSB0aGF0IGBlbmRUb2tlbmAgaXMgcmVhc3NpZ25lZCBtdWx0aXBsZSB0aW1lcyBiZWxvd1xuICAgIC8vIHNvIHBsZWFzZSBkbyBub3Qgb3B0aW1pemUgdGhlIGlmIHN0YXRlbWVudHMgaW50byBpZi9lbHNlaWZcbiAgICBsZXQgc3RhcnRUb2tlbk9yQXN0OiBDc3NUb2tlbnxDc3NBc3R8bnVsbCA9IG51bGw7XG4gICAgbGV0IGVuZFRva2VuT3JBc3Q6IENzc1Rva2VufENzc0FzdHxudWxsID0gbnVsbDtcbiAgICBpZiAoc2VsZWN0b3JDc3NUb2tlbnMubGVuZ3RoID4gMCkge1xuICAgICAgc3RhcnRUb2tlbk9yQXN0ID0gc3RhcnRUb2tlbk9yQXN0IHx8IHNlbGVjdG9yQ3NzVG9rZW5zWzBdO1xuICAgICAgZW5kVG9rZW5PckFzdCA9IHNlbGVjdG9yQ3NzVG9rZW5zW3NlbGVjdG9yQ3NzVG9rZW5zLmxlbmd0aCAtIDFdO1xuICAgIH1cbiAgICBpZiAocHNldWRvU2VsZWN0b3JzLmxlbmd0aCA+IDApIHtcbiAgICAgIHN0YXJ0VG9rZW5PckFzdCA9IHN0YXJ0VG9rZW5PckFzdCB8fCBwc2V1ZG9TZWxlY3RvcnNbMF07XG4gICAgICBlbmRUb2tlbk9yQXN0ID0gcHNldWRvU2VsZWN0b3JzW3BzZXVkb1NlbGVjdG9ycy5sZW5ndGggLSAxXTtcbiAgICB9XG4gICAgaWYgKG9wZXJhdG9yICE9IG51bGwpIHtcbiAgICAgIHN0YXJ0VG9rZW5PckFzdCA9IHN0YXJ0VG9rZW5PckFzdCB8fCBvcGVyYXRvcjtcbiAgICAgIGVuZFRva2VuT3JBc3QgPSBvcGVyYXRvcjtcbiAgICB9XG5cbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW5PckFzdCAhLCBlbmRUb2tlbk9yQXN0KTtcbiAgICByZXR1cm4gbmV3IENzc1NpbXBsZVNlbGVjdG9yQXN0KHNwYW4sIHNlbGVjdG9yQ3NzVG9rZW5zLCBzdHJWYWx1ZSwgcHNldWRvU2VsZWN0b3JzLCBvcGVyYXRvciAhKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlU2VsZWN0b3IoZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzU2VsZWN0b3JBc3Qge1xuICAgIGRlbGltaXRlcnMgfD0gQ09NTUFfREVMSU1fRkxBRztcbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNFTEVDVE9SKTtcblxuICAgIGNvbnN0IHNpbXBsZVNlbGVjdG9yczogQ3NzU2ltcGxlU2VsZWN0b3JBc3RbXSA9IFtdO1xuICAgIHdoaWxlICghY2hhcmFjdGVyQ29udGFpbnNEZWxpbWl0ZXIodGhpcy5fc2Nhbm5lci5wZWVrLCBkZWxpbWl0ZXJzKSkge1xuICAgICAgc2ltcGxlU2VsZWN0b3JzLnB1c2godGhpcy5fcGFyc2VTaW1wbGVTZWxlY3RvcihkZWxpbWl0ZXJzKSk7XG4gICAgICB0aGlzLl9zY2FubmVyLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG4gICAgfVxuXG4gICAgY29uc3QgZmlyc3RTZWxlY3RvciA9IHNpbXBsZVNlbGVjdG9yc1swXTtcbiAgICBjb25zdCBsYXN0U2VsZWN0b3IgPSBzaW1wbGVTZWxlY3RvcnNbc2ltcGxlU2VsZWN0b3JzLmxlbmd0aCAtIDFdO1xuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oZmlyc3RTZWxlY3RvciwgbGFzdFNlbGVjdG9yKTtcbiAgICByZXR1cm4gbmV3IENzc1NlbGVjdG9yQXN0KHNwYW4sIHNpbXBsZVNlbGVjdG9ycyk7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9wYXJzZVZhbHVlKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1N0eWxlVmFsdWVBc3Qge1xuICAgIGRlbGltaXRlcnMgfD0gUkJSQUNFX0RFTElNX0ZMQUcgfCBTRU1JQ09MT05fREVMSU1fRkxBRyB8IE5FV0xJTkVfREVMSU1fRkxBRztcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU1RZTEVfVkFMVUUpO1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCk7XG5cbiAgICBjb25zdCB0b2tlbnM6IENzc1Rva2VuW10gPSBbXTtcbiAgICBsZXQgd3NTdHIgPSAnJztcbiAgICBsZXQgcHJldmlvdXM6IENzc1Rva2VuID0gdW5kZWZpbmVkICE7XG4gICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICBsZXQgdG9rZW46IENzc1Rva2VuO1xuICAgICAgaWYgKHByZXZpb3VzICE9IG51bGwgJiYgcHJldmlvdXMudHlwZSA9PSBDc3NUb2tlblR5cGUuSWRlbnRpZmllciAmJlxuICAgICAgICAgIHRoaXMuX3NjYW5uZXIucGVlayA9PSBjaGFycy4kTFBBUkVOKSB7XG4gICAgICAgIHRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnKCcpO1xuICAgICAgICB0b2tlbnMucHVzaCh0b2tlbik7XG5cbiAgICAgICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TVFlMRV9WQUxVRV9GVU5DVElPTik7XG5cbiAgICAgICAgdG9rZW4gPSB0aGlzLl9zY2FuKCk7XG4gICAgICAgIHRva2Vucy5wdXNoKHRva2VuKTtcblxuICAgICAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNUWUxFX1ZBTFVFKTtcblxuICAgICAgICB0b2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJyknKTtcbiAgICAgICAgdG9rZW5zLnB1c2godG9rZW4pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdG9rZW4gPSB0aGlzLl9zY2FuKCk7XG4gICAgICAgIGlmICh0b2tlbi50eXBlID09IENzc1Rva2VuVHlwZS5XaGl0ZXNwYWNlKSB7XG4gICAgICAgICAgd3NTdHIgKz0gdG9rZW4uc3RyVmFsdWU7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgd3NTdHIgPSAnJztcbiAgICAgICAgICB0b2tlbnMucHVzaCh0b2tlbik7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHByZXZpb3VzID0gdG9rZW47XG4gICAgfVxuXG4gICAgY29uc3QgZW5kID0gdGhpcy5fZ2V0U2Nhbm5lckluZGV4KCkgLSAxO1xuICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZVdoaXRlc3BhY2UoKTtcblxuICAgIGNvbnN0IGNvZGUgPSB0aGlzLl9zY2FubmVyLnBlZWs7XG4gICAgaWYgKGNvZGUgPT0gY2hhcnMuJFNFTUlDT0xPTikge1xuICAgICAgdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnOycpO1xuICAgIH0gZWxzZSBpZiAoY29kZSAhPSBjaGFycy4kUkJSQUNFKSB7XG4gICAgICB0aGlzLl9lcnJvcihcbiAgICAgICAgICBnZW5lcmF0ZUVycm9yTWVzc2FnZShcbiAgICAgICAgICAgICAgdGhpcy5fZ2V0U291cmNlQ29udGVudCgpLCBgVGhlIENTUyBrZXkvdmFsdWUgZGVmaW5pdGlvbiBkaWQgbm90IGVuZCB3aXRoIGEgc2VtaWNvbG9uYCxcbiAgICAgICAgICAgICAgcHJldmlvdXMuc3RyVmFsdWUsIHByZXZpb3VzLmluZGV4LCBwcmV2aW91cy5saW5lLCBwcmV2aW91cy5jb2x1bW4pLFxuICAgICAgICAgIHByZXZpb3VzKTtcbiAgICB9XG5cbiAgICBjb25zdCBzdHJWYWx1ZSA9IHRoaXMuX2V4dHJhY3RTb3VyY2VDb250ZW50KHN0YXJ0LCBlbmQpO1xuICAgIGNvbnN0IHN0YXJ0VG9rZW4gPSB0b2tlbnNbMF07XG4gICAgY29uc3QgZW5kVG9rZW4gPSB0b2tlbnNbdG9rZW5zLmxlbmd0aCAtIDFdO1xuICAgIGNvbnN0IHNwYW4gPSB0aGlzLl9nZW5lcmF0ZVNvdXJjZVNwYW4oc3RhcnRUb2tlbiwgZW5kVG9rZW4pO1xuICAgIHJldHVybiBuZXcgQ3NzU3R5bGVWYWx1ZUFzdChzcGFuLCB0b2tlbnMsIHN0clZhbHVlKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2NvbGxlY3RVbnRpbERlbGltKGRlbGltaXRlcnM6IG51bWJlciwgYXNzZXJ0VHlwZTogQ3NzVG9rZW5UeXBlfG51bGwgPSBudWxsKTogQ3NzVG9rZW5bXSB7XG4gICAgY29uc3QgdG9rZW5zOiBDc3NUb2tlbltdID0gW107XG4gICAgd2hpbGUgKCFjaGFyYWN0ZXJDb250YWluc0RlbGltaXRlcih0aGlzLl9zY2FubmVyLnBlZWssIGRlbGltaXRlcnMpKSB7XG4gICAgICBjb25zdCB2YWwgPSBhc3NlcnRUeXBlICE9IG51bGwgPyB0aGlzLl9jb25zdW1lKGFzc2VydFR5cGUpIDogdGhpcy5fc2NhbigpO1xuICAgICAgdG9rZW5zLnB1c2godmFsKTtcbiAgICB9XG4gICAgcmV0dXJuIHRva2VucztcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlQmxvY2soZGVsaW1pdGVyczogbnVtYmVyKTogQ3NzQmxvY2tBc3Qge1xuICAgIGRlbGltaXRlcnMgfD0gUkJSQUNFX0RFTElNX0ZMQUc7XG5cbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLkJMT0NLKTtcblxuICAgIGNvbnN0IHN0YXJ0VG9rZW4gPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICd7Jyk7XG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG5cbiAgICBjb25zdCByZXN1bHRzOiBDc3NSdWxlQXN0W10gPSBbXTtcbiAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIHJlc3VsdHMucHVzaCh0aGlzLl9wYXJzZVJ1bGUoZGVsaW1pdGVycykpO1xuICAgIH1cblxuICAgIGNvbnN0IGVuZFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnfScpO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5CTE9DSyk7XG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG5cbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGVuZFRva2VuKTtcbiAgICByZXR1cm4gbmV3IENzc0Jsb2NrQXN0KHNwYW4sIHJlc3VsdHMpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfcGFyc2VTdHlsZUJsb2NrKGRlbGltaXRlcnM6IG51bWJlcik6IENzc1N0eWxlc0Jsb2NrQXN0fG51bGwge1xuICAgIGRlbGltaXRlcnMgfD0gUkJSQUNFX0RFTElNX0ZMQUcgfCBMQlJBQ0VfREVMSU1fRkxBRztcblxuICAgIHRoaXMuX3NjYW5uZXIuc2V0TW9kZShDc3NMZXhlck1vZGUuU1RZTEVfQkxPQ0spO1xuXG4gICAgY29uc3Qgc3RhcnRUb2tlbiA9IHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJ3snKTtcbiAgICBpZiAoc3RhcnRUb2tlbi5udW1WYWx1ZSAhPSBjaGFycy4kTEJSQUNFKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG5cbiAgICBjb25zdCBkZWZpbml0aW9uczogQ3NzRGVmaW5pdGlvbkFzdFtdID0gW107XG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG5cbiAgICB3aGlsZSAoIWNoYXJhY3RlckNvbnRhaW5zRGVsaW1pdGVyKHRoaXMuX3NjYW5uZXIucGVlaywgZGVsaW1pdGVycykpIHtcbiAgICAgIGRlZmluaXRpb25zLnB1c2godGhpcy5fcGFyc2VEZWZpbml0aW9uKGRlbGltaXRlcnMpKTtcbiAgICAgIHRoaXMuX3NjYW5uZXIuY29uc3VtZUVtcHR5U3RhdGVtZW50cygpO1xuICAgIH1cblxuICAgIGNvbnN0IGVuZFRva2VuID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCAnfScpO1xuXG4gICAgdGhpcy5fc2Nhbm5lci5zZXRNb2RlKENzc0xleGVyTW9kZS5TVFlMRV9CTE9DSyk7XG4gICAgdGhpcy5fc2Nhbm5lci5jb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk7XG5cbiAgICBjb25zdCBzcGFuID0gdGhpcy5fZ2VuZXJhdGVTb3VyY2VTcGFuKHN0YXJ0VG9rZW4sIGVuZFRva2VuKTtcbiAgICByZXR1cm4gbmV3IENzc1N0eWxlc0Jsb2NrQXN0KHNwYW4sIGRlZmluaXRpb25zKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3BhcnNlRGVmaW5pdGlvbihkZWxpbWl0ZXJzOiBudW1iZXIpOiBDc3NEZWZpbml0aW9uQXN0IHtcbiAgICB0aGlzLl9zY2FubmVyLnNldE1vZGUoQ3NzTGV4ZXJNb2RlLlNUWUxFX0JMT0NLKTtcblxuICAgIGxldCBwcm9wID0gdGhpcy5fY29uc3VtZShDc3NUb2tlblR5cGUuSWRlbnRpZmllcik7XG4gICAgbGV0IHBhcnNlVmFsdWU6IGJvb2xlYW4gPSBmYWxzZTtcbiAgICBsZXQgdmFsdWU6IENzc1N0eWxlVmFsdWVBc3R8bnVsbCA9IG51bGw7XG4gICAgbGV0IGVuZFRva2VuOiBDc3NUb2tlbnxDc3NTdHlsZVZhbHVlQXN0ID0gcHJvcDtcblxuICAgIC8vIHRoZSBjb2xvbiB2YWx1ZSBzZXBhcmF0ZXMgdGhlIHByb3AgZnJvbSB0aGUgc3R5bGUuXG4gICAgLy8gdGhlcmUgYXJlIGEgZmV3IGNhc2VzIGFzIHRvIHdoYXQgY291bGQgaGFwcGVuIGlmIGl0XG4gICAgLy8gaXMgbWlzc2luZ1xuICAgIHN3aXRjaCAodGhpcy5fc2Nhbm5lci5wZWVrKSB7XG4gICAgICBjYXNlIGNoYXJzLiRTRU1JQ09MT046XG4gICAgICBjYXNlIGNoYXJzLiRSQlJBQ0U6XG4gICAgICBjYXNlIGNoYXJzLiRFT0Y6XG4gICAgICAgIHBhcnNlVmFsdWUgPSBmYWxzZTtcbiAgICAgICAgYnJlYWs7XG5cbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIGxldCBwcm9wU3RyID0gW3Byb3Auc3RyVmFsdWVdO1xuICAgICAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrICE9IGNoYXJzLiRDT0xPTikge1xuICAgICAgICAgIC8vIHRoaXMgd2lsbCB0aHJvdyB0aGUgZXJyb3JcbiAgICAgICAgICBjb25zdCBuZXh0VmFsdWUgPSB0aGlzLl9jb25zdW1lKENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsICc6Jyk7XG4gICAgICAgICAgcHJvcFN0ci5wdXNoKG5leHRWYWx1ZS5zdHJWYWx1ZSk7XG5cbiAgICAgICAgICBjb25zdCByZW1haW5pbmdUb2tlbnMgPSB0aGlzLl9jb2xsZWN0VW50aWxEZWxpbShcbiAgICAgICAgICAgICAgZGVsaW1pdGVycyB8IENPTE9OX0RFTElNX0ZMQUcgfCBTRU1JQ09MT05fREVMSU1fRkxBRywgQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIpO1xuICAgICAgICAgIGlmIChyZW1haW5pbmdUb2tlbnMubGVuZ3RoID4gMCkge1xuICAgICAgICAgICAgcmVtYWluaW5nVG9rZW5zLmZvckVhY2goKHRva2VuKSA9PiB7IHByb3BTdHIucHVzaCh0b2tlbi5zdHJWYWx1ZSk7IH0pO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIGVuZFRva2VuID0gcHJvcCA9XG4gICAgICAgICAgICAgIG5ldyBDc3NUb2tlbihwcm9wLmluZGV4LCBwcm9wLmNvbHVtbiwgcHJvcC5saW5lLCBwcm9wLnR5cGUsIHByb3BTdHIuam9pbignICcpKTtcbiAgICAgICAgfVxuXG4gICAgICAgIC8vIHRoaXMgbWVhbnMgd2UndmUgcmVhY2hlZCB0aGUgZW5kIG9mIHRoZSBkZWZpbml0aW9uIGFuZC9vciBibG9ja1xuICAgICAgICBpZiAodGhpcy5fc2Nhbm5lci5wZWVrID09IGNoYXJzLiRDT0xPTikge1xuICAgICAgICAgIHRoaXMuX2NvbnN1bWUoQ3NzVG9rZW5UeXBlLkNoYXJhY3RlciwgJzonKTtcbiAgICAgICAgICBwYXJzZVZhbHVlID0gdHJ1ZTtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICB9XG5cbiAgICBpZiAocGFyc2VWYWx1ZSkge1xuICAgICAgdmFsdWUgPSB0aGlzLl9wYXJzZVZhbHVlKGRlbGltaXRlcnMpO1xuICAgICAgZW5kVG9rZW4gPSB2YWx1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5fZXJyb3IoXG4gICAgICAgICAgZ2VuZXJhdGVFcnJvck1lc3NhZ2UoXG4gICAgICAgICAgICAgIHRoaXMuX2dldFNvdXJjZUNvbnRlbnQoKSwgYFRoZSBDU1MgcHJvcGVydHkgd2FzIG5vdCBwYWlyZWQgd2l0aCBhIHN0eWxlIHZhbHVlYCxcbiAgICAgICAgICAgICAgcHJvcC5zdHJWYWx1ZSwgcHJvcC5pbmRleCwgcHJvcC5saW5lLCBwcm9wLmNvbHVtbiksXG4gICAgICAgICAgcHJvcCk7XG4gICAgfVxuXG4gICAgY29uc3Qgc3BhbiA9IHRoaXMuX2dlbmVyYXRlU291cmNlU3Bhbihwcm9wLCBlbmRUb2tlbik7XG4gICAgcmV0dXJuIG5ldyBDc3NEZWZpbml0aW9uQXN0KHNwYW4sIHByb3AsIHZhbHVlICEpO1xuICB9XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfYXNzZXJ0Q29uZGl0aW9uKHN0YXR1czogYm9vbGVhbiwgZXJyb3JNZXNzYWdlOiBzdHJpbmcsIHByb2JsZW1Ub2tlbjogQ3NzVG9rZW4pOiBib29sZWFuIHtcbiAgICBpZiAoIXN0YXR1cykge1xuICAgICAgdGhpcy5fZXJyb3IoZXJyb3JNZXNzYWdlLCBwcm9ibGVtVG9rZW4pO1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfVxuICAgIHJldHVybiBmYWxzZTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2Vycm9yKG1lc3NhZ2U6IHN0cmluZywgcHJvYmxlbVRva2VuOiBDc3NUb2tlbikge1xuICAgIGNvbnN0IGxlbmd0aCA9IHByb2JsZW1Ub2tlbi5zdHJWYWx1ZS5sZW5ndGg7XG4gICAgY29uc3QgZXJyb3IgPSBDc3NQYXJzZUVycm9yLmNyZWF0ZShcbiAgICAgICAgdGhpcy5fZmlsZSwgMCwgcHJvYmxlbVRva2VuLmxpbmUsIHByb2JsZW1Ub2tlbi5jb2x1bW4sIGxlbmd0aCwgbWVzc2FnZSk7XG4gICAgdGhpcy5fZXJyb3JzLnB1c2goZXJyb3IpO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NQYXJzZUVycm9yIGV4dGVuZHMgUGFyc2VFcnJvciB7XG4gIHN0YXRpYyBjcmVhdGUoXG4gICAgICBmaWxlOiBQYXJzZVNvdXJjZUZpbGUsIG9mZnNldDogbnVtYmVyLCBsaW5lOiBudW1iZXIsIGNvbDogbnVtYmVyLCBsZW5ndGg6IG51bWJlcixcbiAgICAgIGVyck1zZzogc3RyaW5nKTogQ3NzUGFyc2VFcnJvciB7XG4gICAgY29uc3Qgc3RhcnQgPSBuZXcgUGFyc2VMb2NhdGlvbihmaWxlLCBvZmZzZXQsIGxpbmUsIGNvbCk7XG4gICAgY29uc3QgZW5kID0gbmV3IFBhcnNlTG9jYXRpb24oZmlsZSwgb2Zmc2V0LCBsaW5lLCBjb2wgKyBsZW5ndGgpO1xuICAgIGNvbnN0IHNwYW4gPSBuZXcgUGFyc2VTb3VyY2VTcGFuKHN0YXJ0LCBlbmQpO1xuICAgIHJldHVybiBuZXcgQ3NzUGFyc2VFcnJvcihzcGFuLCAnQ1NTIFBhcnNlIEVycm9yOiAnICsgZXJyTXNnKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHNwYW46IFBhcnNlU291cmNlU3BhbiwgbWVzc2FnZTogc3RyaW5nKSB7IHN1cGVyKHNwYW4sIG1lc3NhZ2UpOyB9XG59XG4iXX0=