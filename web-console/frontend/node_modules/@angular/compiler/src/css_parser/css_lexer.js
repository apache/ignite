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
        define("@angular/compiler/src/css_parser/css_lexer", ["require", "exports", "@angular/compiler/src/chars"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var chars = require("@angular/compiler/src/chars");
    var CssTokenType;
    (function (CssTokenType) {
        CssTokenType[CssTokenType["EOF"] = 0] = "EOF";
        CssTokenType[CssTokenType["String"] = 1] = "String";
        CssTokenType[CssTokenType["Comment"] = 2] = "Comment";
        CssTokenType[CssTokenType["Identifier"] = 3] = "Identifier";
        CssTokenType[CssTokenType["Number"] = 4] = "Number";
        CssTokenType[CssTokenType["IdentifierOrNumber"] = 5] = "IdentifierOrNumber";
        CssTokenType[CssTokenType["AtKeyword"] = 6] = "AtKeyword";
        CssTokenType[CssTokenType["Character"] = 7] = "Character";
        CssTokenType[CssTokenType["Whitespace"] = 8] = "Whitespace";
        CssTokenType[CssTokenType["Invalid"] = 9] = "Invalid";
    })(CssTokenType = exports.CssTokenType || (exports.CssTokenType = {}));
    var CssLexerMode;
    (function (CssLexerMode) {
        CssLexerMode[CssLexerMode["ALL"] = 0] = "ALL";
        CssLexerMode[CssLexerMode["ALL_TRACK_WS"] = 1] = "ALL_TRACK_WS";
        CssLexerMode[CssLexerMode["SELECTOR"] = 2] = "SELECTOR";
        CssLexerMode[CssLexerMode["PSEUDO_SELECTOR"] = 3] = "PSEUDO_SELECTOR";
        CssLexerMode[CssLexerMode["PSEUDO_SELECTOR_WITH_ARGUMENTS"] = 4] = "PSEUDO_SELECTOR_WITH_ARGUMENTS";
        CssLexerMode[CssLexerMode["ATTRIBUTE_SELECTOR"] = 5] = "ATTRIBUTE_SELECTOR";
        CssLexerMode[CssLexerMode["AT_RULE_QUERY"] = 6] = "AT_RULE_QUERY";
        CssLexerMode[CssLexerMode["MEDIA_QUERY"] = 7] = "MEDIA_QUERY";
        CssLexerMode[CssLexerMode["BLOCK"] = 8] = "BLOCK";
        CssLexerMode[CssLexerMode["KEYFRAME_BLOCK"] = 9] = "KEYFRAME_BLOCK";
        CssLexerMode[CssLexerMode["STYLE_BLOCK"] = 10] = "STYLE_BLOCK";
        CssLexerMode[CssLexerMode["STYLE_VALUE"] = 11] = "STYLE_VALUE";
        CssLexerMode[CssLexerMode["STYLE_VALUE_FUNCTION"] = 12] = "STYLE_VALUE_FUNCTION";
        CssLexerMode[CssLexerMode["STYLE_CALC_FUNCTION"] = 13] = "STYLE_CALC_FUNCTION";
    })(CssLexerMode = exports.CssLexerMode || (exports.CssLexerMode = {}));
    var LexedCssResult = /** @class */ (function () {
        function LexedCssResult(error, token) {
            this.error = error;
            this.token = token;
        }
        return LexedCssResult;
    }());
    exports.LexedCssResult = LexedCssResult;
    function generateErrorMessage(input, message, errorValue, index, row, column) {
        return message + " at column " + row + ":" + column + " in expression [" +
            findProblemCode(input, errorValue, index, column) + ']';
    }
    exports.generateErrorMessage = generateErrorMessage;
    function findProblemCode(input, errorValue, index, column) {
        var endOfProblemLine = index;
        var current = charCode(input, index);
        while (current > 0 && !isNewline(current)) {
            current = charCode(input, ++endOfProblemLine);
        }
        var choppedString = input.substring(0, endOfProblemLine);
        var pointerPadding = '';
        for (var i = 0; i < column; i++) {
            pointerPadding += ' ';
        }
        var pointerString = '';
        for (var i = 0; i < errorValue.length; i++) {
            pointerString += '^';
        }
        return choppedString + '\n' + pointerPadding + pointerString + '\n';
    }
    exports.findProblemCode = findProblemCode;
    var CssToken = /** @class */ (function () {
        function CssToken(index, column, line, type, strValue) {
            this.index = index;
            this.column = column;
            this.line = line;
            this.type = type;
            this.strValue = strValue;
            this.numValue = charCode(strValue, 0);
        }
        return CssToken;
    }());
    exports.CssToken = CssToken;
    var CssLexer = /** @class */ (function () {
        function CssLexer() {
        }
        CssLexer.prototype.scan = function (text, trackComments) {
            if (trackComments === void 0) { trackComments = false; }
            return new CssScanner(text, trackComments);
        };
        return CssLexer;
    }());
    exports.CssLexer = CssLexer;
    function cssScannerError(token, message) {
        var error = Error('CssParseError: ' + message);
        error[ERROR_RAW_MESSAGE] = message;
        error[ERROR_TOKEN] = token;
        return error;
    }
    exports.cssScannerError = cssScannerError;
    var ERROR_TOKEN = 'ngToken';
    var ERROR_RAW_MESSAGE = 'ngRawMessage';
    function getRawMessage(error) {
        return error[ERROR_RAW_MESSAGE];
    }
    exports.getRawMessage = getRawMessage;
    function getToken(error) {
        return error[ERROR_TOKEN];
    }
    exports.getToken = getToken;
    function _trackWhitespace(mode) {
        switch (mode) {
            case CssLexerMode.SELECTOR:
            case CssLexerMode.PSEUDO_SELECTOR:
            case CssLexerMode.ALL_TRACK_WS:
            case CssLexerMode.STYLE_VALUE:
                return true;
            default:
                return false;
        }
    }
    var CssScanner = /** @class */ (function () {
        function CssScanner(input, _trackComments) {
            if (_trackComments === void 0) { _trackComments = false; }
            this.input = input;
            this._trackComments = _trackComments;
            this.length = 0;
            this.index = -1;
            this.column = -1;
            this.line = 0;
            /** @internal */
            this._currentMode = CssLexerMode.BLOCK;
            /** @internal */
            this._currentError = null;
            this.length = this.input.length;
            this.peekPeek = this.peekAt(0);
            this.advance();
        }
        CssScanner.prototype.getMode = function () { return this._currentMode; };
        CssScanner.prototype.setMode = function (mode) {
            if (this._currentMode != mode) {
                if (_trackWhitespace(this._currentMode) && !_trackWhitespace(mode)) {
                    this.consumeWhitespace();
                }
                this._currentMode = mode;
            }
        };
        CssScanner.prototype.advance = function () {
            if (isNewline(this.peek)) {
                this.column = 0;
                this.line++;
            }
            else {
                this.column++;
            }
            this.index++;
            this.peek = this.peekPeek;
            this.peekPeek = this.peekAt(this.index + 1);
        };
        CssScanner.prototype.peekAt = function (index) {
            return index >= this.length ? chars.$EOF : this.input.charCodeAt(index);
        };
        CssScanner.prototype.consumeEmptyStatements = function () {
            this.consumeWhitespace();
            while (this.peek == chars.$SEMICOLON) {
                this.advance();
                this.consumeWhitespace();
            }
        };
        CssScanner.prototype.consumeWhitespace = function () {
            while (chars.isWhitespace(this.peek) || isNewline(this.peek)) {
                this.advance();
                if (!this._trackComments && isCommentStart(this.peek, this.peekPeek)) {
                    this.advance(); // /
                    this.advance(); // *
                    while (!isCommentEnd(this.peek, this.peekPeek)) {
                        if (this.peek == chars.$EOF) {
                            this.error('Unterminated comment');
                        }
                        this.advance();
                    }
                    this.advance(); // *
                    this.advance(); // /
                }
            }
        };
        CssScanner.prototype.consume = function (type, value) {
            if (value === void 0) { value = null; }
            var mode = this._currentMode;
            this.setMode(_trackWhitespace(mode) ? CssLexerMode.ALL_TRACK_WS : CssLexerMode.ALL);
            var previousIndex = this.index;
            var previousLine = this.line;
            var previousColumn = this.column;
            var next = undefined;
            var output = this.scan();
            if (output != null) {
                // just incase the inner scan method returned an error
                if (output.error != null) {
                    this.setMode(mode);
                    return output;
                }
                next = output.token;
            }
            if (next == null) {
                next = new CssToken(this.index, this.column, this.line, CssTokenType.EOF, 'end of file');
            }
            var isMatchingType = false;
            if (type == CssTokenType.IdentifierOrNumber) {
                // TODO (matsko): implement array traversal for lookup here
                isMatchingType = next.type == CssTokenType.Number || next.type == CssTokenType.Identifier;
            }
            else {
                isMatchingType = next.type == type;
            }
            // before throwing the error we need to bring back the former
            // mode so that the parser can recover...
            this.setMode(mode);
            var error = null;
            if (!isMatchingType || (value != null && value != next.strValue)) {
                var errorMessage = CssTokenType[next.type] + ' does not match expected ' + CssTokenType[type] + ' value';
                if (value != null) {
                    errorMessage += ' ("' + next.strValue + '" should match "' + value + '")';
                }
                error = cssScannerError(next, generateErrorMessage(this.input, errorMessage, next.strValue, previousIndex, previousLine, previousColumn));
            }
            return new LexedCssResult(error, next);
        };
        CssScanner.prototype.scan = function () {
            var trackWS = _trackWhitespace(this._currentMode);
            if (this.index == 0 && !trackWS) { // first scan
                this.consumeWhitespace();
            }
            var token = this._scan();
            if (token == null)
                return null;
            var error = this._currentError;
            this._currentError = null;
            if (!trackWS) {
                this.consumeWhitespace();
            }
            return new LexedCssResult(error, token);
        };
        /** @internal */
        CssScanner.prototype._scan = function () {
            var peek = this.peek;
            var peekPeek = this.peekPeek;
            if (peek == chars.$EOF)
                return null;
            if (isCommentStart(peek, peekPeek)) {
                // even if comments are not tracked we still lex the
                // comment so we can move the pointer forward
                var commentToken = this.scanComment();
                if (this._trackComments) {
                    return commentToken;
                }
            }
            if (_trackWhitespace(this._currentMode) && (chars.isWhitespace(peek) || isNewline(peek))) {
                return this.scanWhitespace();
            }
            peek = this.peek;
            peekPeek = this.peekPeek;
            if (peek == chars.$EOF)
                return null;
            if (isStringStart(peek, peekPeek)) {
                return this.scanString();
            }
            // something like url(cool)
            if (this._currentMode == CssLexerMode.STYLE_VALUE_FUNCTION) {
                return this.scanCssValueFunction();
            }
            var isModifier = peek == chars.$PLUS || peek == chars.$MINUS;
            var digitA = isModifier ? false : chars.isDigit(peek);
            var digitB = chars.isDigit(peekPeek);
            if (digitA || (isModifier && (peekPeek == chars.$PERIOD || digitB)) ||
                (peek == chars.$PERIOD && digitB)) {
                return this.scanNumber();
            }
            if (peek == chars.$AT) {
                return this.scanAtExpression();
            }
            if (isIdentifierStart(peek, peekPeek)) {
                return this.scanIdentifier();
            }
            if (isValidCssCharacter(peek, this._currentMode)) {
                return this.scanCharacter();
            }
            return this.error("Unexpected character [" + String.fromCharCode(peek) + "]");
        };
        CssScanner.prototype.scanComment = function () {
            if (this.assertCondition(isCommentStart(this.peek, this.peekPeek), 'Expected comment start value')) {
                return null;
            }
            var start = this.index;
            var startingColumn = this.column;
            var startingLine = this.line;
            this.advance(); // /
            this.advance(); // *
            while (!isCommentEnd(this.peek, this.peekPeek)) {
                if (this.peek == chars.$EOF) {
                    this.error('Unterminated comment');
                }
                this.advance();
            }
            this.advance(); // *
            this.advance(); // /
            var str = this.input.substring(start, this.index);
            return new CssToken(start, startingColumn, startingLine, CssTokenType.Comment, str);
        };
        CssScanner.prototype.scanWhitespace = function () {
            var start = this.index;
            var startingColumn = this.column;
            var startingLine = this.line;
            while (chars.isWhitespace(this.peek) && this.peek != chars.$EOF) {
                this.advance();
            }
            var str = this.input.substring(start, this.index);
            return new CssToken(start, startingColumn, startingLine, CssTokenType.Whitespace, str);
        };
        CssScanner.prototype.scanString = function () {
            if (this.assertCondition(isStringStart(this.peek, this.peekPeek), 'Unexpected non-string starting value')) {
                return null;
            }
            var target = this.peek;
            var start = this.index;
            var startingColumn = this.column;
            var startingLine = this.line;
            var previous = target;
            this.advance();
            while (!isCharMatch(target, previous, this.peek)) {
                if (this.peek == chars.$EOF || isNewline(this.peek)) {
                    this.error('Unterminated quote');
                }
                previous = this.peek;
                this.advance();
            }
            if (this.assertCondition(this.peek == target, 'Unterminated quote')) {
                return null;
            }
            this.advance();
            var str = this.input.substring(start, this.index);
            return new CssToken(start, startingColumn, startingLine, CssTokenType.String, str);
        };
        CssScanner.prototype.scanNumber = function () {
            var start = this.index;
            var startingColumn = this.column;
            if (this.peek == chars.$PLUS || this.peek == chars.$MINUS) {
                this.advance();
            }
            var periodUsed = false;
            while (chars.isDigit(this.peek) || this.peek == chars.$PERIOD) {
                if (this.peek == chars.$PERIOD) {
                    if (periodUsed) {
                        this.error('Unexpected use of a second period value');
                    }
                    periodUsed = true;
                }
                this.advance();
            }
            var strValue = this.input.substring(start, this.index);
            return new CssToken(start, startingColumn, this.line, CssTokenType.Number, strValue);
        };
        CssScanner.prototype.scanIdentifier = function () {
            if (this.assertCondition(isIdentifierStart(this.peek, this.peekPeek), 'Expected identifier starting value')) {
                return null;
            }
            var start = this.index;
            var startingColumn = this.column;
            while (isIdentifierPart(this.peek)) {
                this.advance();
            }
            var strValue = this.input.substring(start, this.index);
            return new CssToken(start, startingColumn, this.line, CssTokenType.Identifier, strValue);
        };
        CssScanner.prototype.scanCssValueFunction = function () {
            var start = this.index;
            var startingColumn = this.column;
            var parenBalance = 1;
            while (this.peek != chars.$EOF && parenBalance > 0) {
                this.advance();
                if (this.peek == chars.$LPAREN) {
                    parenBalance++;
                }
                else if (this.peek == chars.$RPAREN) {
                    parenBalance--;
                }
            }
            var strValue = this.input.substring(start, this.index);
            return new CssToken(start, startingColumn, this.line, CssTokenType.Identifier, strValue);
        };
        CssScanner.prototype.scanCharacter = function () {
            var start = this.index;
            var startingColumn = this.column;
            if (this.assertCondition(isValidCssCharacter(this.peek, this._currentMode), charStr(this.peek) + ' is not a valid CSS character')) {
                return null;
            }
            var c = this.input.substring(start, start + 1);
            this.advance();
            return new CssToken(start, startingColumn, this.line, CssTokenType.Character, c);
        };
        CssScanner.prototype.scanAtExpression = function () {
            if (this.assertCondition(this.peek == chars.$AT, 'Expected @ value')) {
                return null;
            }
            var start = this.index;
            var startingColumn = this.column;
            this.advance();
            if (isIdentifierStart(this.peek, this.peekPeek)) {
                var ident = this.scanIdentifier();
                var strValue = '@' + ident.strValue;
                return new CssToken(start, startingColumn, this.line, CssTokenType.AtKeyword, strValue);
            }
            else {
                return this.scanCharacter();
            }
        };
        CssScanner.prototype.assertCondition = function (status, errorMessage) {
            if (!status) {
                this.error(errorMessage);
                return true;
            }
            return false;
        };
        CssScanner.prototype.error = function (message, errorTokenValue, doNotAdvance) {
            if (errorTokenValue === void 0) { errorTokenValue = null; }
            if (doNotAdvance === void 0) { doNotAdvance = false; }
            var index = this.index;
            var column = this.column;
            var line = this.line;
            errorTokenValue = errorTokenValue || String.fromCharCode(this.peek);
            var invalidToken = new CssToken(index, column, line, CssTokenType.Invalid, errorTokenValue);
            var errorMessage = generateErrorMessage(this.input, message, errorTokenValue, index, line, column);
            if (!doNotAdvance) {
                this.advance();
            }
            this._currentError = cssScannerError(invalidToken, errorMessage);
            return invalidToken;
        };
        return CssScanner;
    }());
    exports.CssScanner = CssScanner;
    function isCharMatch(target, previous, code) {
        return code == target && previous != chars.$BACKSLASH;
    }
    function isCommentStart(code, next) {
        return code == chars.$SLASH && next == chars.$STAR;
    }
    function isCommentEnd(code, next) {
        return code == chars.$STAR && next == chars.$SLASH;
    }
    function isStringStart(code, next) {
        var target = code;
        if (target == chars.$BACKSLASH) {
            target = next;
        }
        return target == chars.$DQ || target == chars.$SQ;
    }
    function isIdentifierStart(code, next) {
        var target = code;
        if (target == chars.$MINUS) {
            target = next;
        }
        return chars.isAsciiLetter(target) || target == chars.$BACKSLASH || target == chars.$MINUS ||
            target == chars.$_;
    }
    function isIdentifierPart(target) {
        return chars.isAsciiLetter(target) || target == chars.$BACKSLASH || target == chars.$MINUS ||
            target == chars.$_ || chars.isDigit(target);
    }
    function isValidPseudoSelectorCharacter(code) {
        switch (code) {
            case chars.$LPAREN:
            case chars.$RPAREN:
                return true;
            default:
                return false;
        }
    }
    function isValidKeyframeBlockCharacter(code) {
        return code == chars.$PERCENT;
    }
    function isValidAttributeSelectorCharacter(code) {
        // value^*|$~=something
        switch (code) {
            case chars.$$:
            case chars.$PIPE:
            case chars.$CARET:
            case chars.$TILDA:
            case chars.$STAR:
            case chars.$EQ:
                return true;
            default:
                return false;
        }
    }
    function isValidSelectorCharacter(code) {
        // selector [ key   = value ]
        // IDENT    C IDENT C IDENT C
        // #id, .class, *+~>
        // tag:PSEUDO
        switch (code) {
            case chars.$HASH:
            case chars.$PERIOD:
            case chars.$TILDA:
            case chars.$STAR:
            case chars.$PLUS:
            case chars.$GT:
            case chars.$COLON:
            case chars.$PIPE:
            case chars.$COMMA:
            case chars.$LBRACKET:
            case chars.$RBRACKET:
                return true;
            default:
                return false;
        }
    }
    function isValidStyleBlockCharacter(code) {
        // key:value;
        // key:calc(something ... )
        switch (code) {
            case chars.$HASH:
            case chars.$SEMICOLON:
            case chars.$COLON:
            case chars.$PERCENT:
            case chars.$SLASH:
            case chars.$BACKSLASH:
            case chars.$BANG:
            case chars.$PERIOD:
            case chars.$LPAREN:
            case chars.$RPAREN:
                return true;
            default:
                return false;
        }
    }
    function isValidMediaQueryRuleCharacter(code) {
        // (min-width: 7.5em) and (orientation: landscape)
        switch (code) {
            case chars.$LPAREN:
            case chars.$RPAREN:
            case chars.$COLON:
            case chars.$PERCENT:
            case chars.$PERIOD:
                return true;
            default:
                return false;
        }
    }
    function isValidAtRuleCharacter(code) {
        // @document url(http://www.w3.org/page?something=on#hash),
        switch (code) {
            case chars.$LPAREN:
            case chars.$RPAREN:
            case chars.$COLON:
            case chars.$PERCENT:
            case chars.$PERIOD:
            case chars.$SLASH:
            case chars.$BACKSLASH:
            case chars.$HASH:
            case chars.$EQ:
            case chars.$QUESTION:
            case chars.$AMPERSAND:
            case chars.$STAR:
            case chars.$COMMA:
            case chars.$MINUS:
            case chars.$PLUS:
                return true;
            default:
                return false;
        }
    }
    function isValidStyleFunctionCharacter(code) {
        switch (code) {
            case chars.$PERIOD:
            case chars.$MINUS:
            case chars.$PLUS:
            case chars.$STAR:
            case chars.$SLASH:
            case chars.$LPAREN:
            case chars.$RPAREN:
            case chars.$COMMA:
                return true;
            default:
                return false;
        }
    }
    function isValidBlockCharacter(code) {
        // @something { }
        // IDENT
        return code == chars.$AT;
    }
    function isValidCssCharacter(code, mode) {
        switch (mode) {
            case CssLexerMode.ALL:
            case CssLexerMode.ALL_TRACK_WS:
                return true;
            case CssLexerMode.SELECTOR:
                return isValidSelectorCharacter(code);
            case CssLexerMode.PSEUDO_SELECTOR_WITH_ARGUMENTS:
                return isValidPseudoSelectorCharacter(code);
            case CssLexerMode.ATTRIBUTE_SELECTOR:
                return isValidAttributeSelectorCharacter(code);
            case CssLexerMode.MEDIA_QUERY:
                return isValidMediaQueryRuleCharacter(code);
            case CssLexerMode.AT_RULE_QUERY:
                return isValidAtRuleCharacter(code);
            case CssLexerMode.KEYFRAME_BLOCK:
                return isValidKeyframeBlockCharacter(code);
            case CssLexerMode.STYLE_BLOCK:
            case CssLexerMode.STYLE_VALUE:
                return isValidStyleBlockCharacter(code);
            case CssLexerMode.STYLE_CALC_FUNCTION:
                return isValidStyleFunctionCharacter(code);
            case CssLexerMode.BLOCK:
                return isValidBlockCharacter(code);
            default:
                return false;
        }
    }
    function charCode(input, index) {
        return index >= input.length ? chars.$EOF : input.charCodeAt(index);
    }
    function charStr(code) {
        return String.fromCharCode(code);
    }
    function isNewline(code) {
        switch (code) {
            case chars.$FF:
            case chars.$CR:
            case chars.$LF:
            case chars.$VTAB:
                return true;
            default:
                return false;
        }
    }
    exports.isNewline = isNewline;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NzX2xleGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2Nzc19wYXJzZXIvY3NzX2xleGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBR0gsbURBQWtDO0lBRWxDLElBQVksWUFXWDtJQVhELFdBQVksWUFBWTtRQUN0Qiw2Q0FBRyxDQUFBO1FBQ0gsbURBQU0sQ0FBQTtRQUNOLHFEQUFPLENBQUE7UUFDUCwyREFBVSxDQUFBO1FBQ1YsbURBQU0sQ0FBQTtRQUNOLDJFQUFrQixDQUFBO1FBQ2xCLHlEQUFTLENBQUE7UUFDVCx5REFBUyxDQUFBO1FBQ1QsMkRBQVUsQ0FBQTtRQUNWLHFEQUFPLENBQUE7SUFDVCxDQUFDLEVBWFcsWUFBWSxHQUFaLG9CQUFZLEtBQVosb0JBQVksUUFXdkI7SUFFRCxJQUFZLFlBZVg7SUFmRCxXQUFZLFlBQVk7UUFDdEIsNkNBQUcsQ0FBQTtRQUNILCtEQUFZLENBQUE7UUFDWix1REFBUSxDQUFBO1FBQ1IscUVBQWUsQ0FBQTtRQUNmLG1HQUE4QixDQUFBO1FBQzlCLDJFQUFrQixDQUFBO1FBQ2xCLGlFQUFhLENBQUE7UUFDYiw2REFBVyxDQUFBO1FBQ1gsaURBQUssQ0FBQTtRQUNMLG1FQUFjLENBQUE7UUFDZCw4REFBVyxDQUFBO1FBQ1gsOERBQVcsQ0FBQTtRQUNYLGdGQUFvQixDQUFBO1FBQ3BCLDhFQUFtQixDQUFBO0lBQ3JCLENBQUMsRUFmVyxZQUFZLEdBQVosb0JBQVksS0FBWixvQkFBWSxRQWV2QjtJQUVEO1FBQ0Usd0JBQW1CLEtBQWlCLEVBQVMsS0FBZTtZQUF6QyxVQUFLLEdBQUwsS0FBSyxDQUFZO1lBQVMsVUFBSyxHQUFMLEtBQUssQ0FBVTtRQUFHLENBQUM7UUFDbEUscUJBQUM7SUFBRCxDQUFDLEFBRkQsSUFFQztJQUZZLHdDQUFjO0lBSTNCLFNBQWdCLG9CQUFv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QyxRQUFRLElBQUksRUFBRTtZQUNaLEtBQUssS0FBSyxDQUFDLEdBQUcsQ0FBQztZQUNmLEtBQUssS0FBSyxDQUFDLEdBQUcsQ0FBQztZQUNmLEtBQUssS0FBSyxDQUFDLEdBQUcsQ0FBQztZQUNmLEtBQUssS0FBSyxDQUFDLEtBQUs7Z0JBQ2QsT0FBTyxJQUFJLENBQUM7WUFFZDtnQkFDRSxPQUFPLEtBQUssQ0FBQztTQUNoQjtJQUNILENBQUM7SUFYRCw4QkFXQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG5pbXBvcnQgKiBhcyBjaGFycyBmcm9tICcuLi9jaGFycyc7XG5cbmV4cG9ydCBlbnVtIENzc1Rva2VuVHlwZSB7XG4gIEVPRixcbiAgU3RyaW5nLFxuICBDb21tZW50LFxuICBJZGVudGlmaWVyLFxuICBOdW1iZXIsXG4gIElkZW50aWZpZXJPck51bWJlcixcbiAgQXRLZXl3b3JkLFxuICBDaGFyYWN0ZXIsXG4gIFdoaXRlc3BhY2UsXG4gIEludmFsaWRcbn1cblxuZXhwb3J0IGVudW0gQ3NzTGV4ZXJNb2RlIHtcbiAgQUxMLFxuICBBTExfVFJBQ0tfV1MsXG4gIFNFTEVDVE9SLFxuICBQU0VVRE9fU0VMRUNUT1IsXG4gIFBTRVVET19TRUxFQ1RPUl9XSVRIX0FSR1VNRU5UUyxcbiAgQVRUUklCVVRFX1NFTEVDVE9SLFxuICBBVF9SVUxFX1FVRVJZLFxuICBNRURJQV9RVUVSWSxcbiAgQkxPQ0ssXG4gIEtFWUZSQU1FX0JMT0NLLFxuICBTVFlMRV9CTE9DSyxcbiAgU1RZTEVfVkFMVUUsXG4gIFNUWUxFX1ZBTFVFX0ZVTkNUSU9OLFxuICBTVFlMRV9DQUxDX0ZVTkNUSU9OXG59XG5cbmV4cG9ydCBjbGFzcyBMZXhlZENzc1Jlc3VsdCB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBlcnJvcjogRXJyb3J8bnVsbCwgcHVibGljIHRva2VuOiBDc3NUb2tlbikge31cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdlbmVyYXRlRXJyb3JNZXNzYWdlKFxuICAgIGlucHV0OiBzdHJpbmcsIG1lc3NhZ2U6IHN0cmluZywgZXJyb3JWYWx1ZTogc3RyaW5nLCBpbmRleDogbnVtYmVyLCByb3c6IG51bWJlcixcbiAgICBjb2x1bW46IG51bWJlcik6IHN0cmluZyB7XG4gIHJldHVybiBgJHttZXNzYWdlfSBhdCBjb2x1bW4gJHtyb3d9OiR7Y29sdW1ufSBpbiBleHByZXNzaW9uIFtgICtcbiAgICAgIGZpbmRQcm9ibGVtQ29kZShpbnB1dCwgZXJyb3JWYWx1ZSwgaW5kZXgsIGNvbHVtbikgKyAnXSc7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmaW5kUHJvYmxlbUNvZGUoXG4gICAgaW5wdXQ6IHN0cmluZywgZXJyb3JWYWx1ZTogc3RyaW5nLCBpbmRleDogbnVtYmVyLCBjb2x1bW46IG51bWJlcik6IHN0cmluZyB7XG4gIGxldCBlbmRPZlByb2JsZW1MaW5lID0gaW5kZXg7XG4gIGxldCBjdXJyZW50ID0gY2hhckNvZGUoaW5wdXQsIGluZGV4KTtcbiAgd2hpbGUgKGN1cnJlbnQgPiAwICYmICFpc05ld2xpbmUoY3VycmVudCkpIHtcbiAgICBjdXJyZW50ID0gY2hhckNvZGUoaW5wdXQsICsrZW5kT2ZQcm9ibGVtTGluZSk7XG4gIH1cbiAgY29uc3QgY2hvcHBlZFN0cmluZyA9IGlucHV0LnN1YnN0cmluZygwLCBlbmRPZlByb2JsZW1MaW5lKTtcbiAgbGV0IHBvaW50ZXJQYWRkaW5nID0gJyc7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgY29sdW1uOyBpKyspIHtcbiAgICBwb2ludGVyUGFkZGluZyArPSAnICc7XG4gIH1cbiAgbGV0IHBvaW50ZXJTdHJpbmcgPSAnJztcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCBlcnJvclZhbHVlLmxlbmd0aDsgaSsrKSB7XG4gICAgcG9pbnRlclN0cmluZyArPSAnXic7XG4gIH1cbiAgcmV0dXJuIGNob3BwZWRTdHJpbmcgKyAnXFxuJyArIHBvaW50ZXJQYWRkaW5nICsgcG9pbnRlclN0cmluZyArICdcXG4nO1xufVxuXG5leHBvcnQgY2xhc3MgQ3NzVG9rZW4ge1xuICBudW1WYWx1ZTogbnVtYmVyO1xuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBpbmRleDogbnVtYmVyLCBwdWJsaWMgY29sdW1uOiBudW1iZXIsIHB1YmxpYyBsaW5lOiBudW1iZXIsIHB1YmxpYyB0eXBlOiBDc3NUb2tlblR5cGUsXG4gICAgICBwdWJsaWMgc3RyVmFsdWU6IHN0cmluZykge1xuICAgIHRoaXMubnVtVmFsdWUgPSBjaGFyQ29kZShzdHJWYWx1ZSwgMCk7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc0xleGVyIHtcbiAgc2Nhbih0ZXh0OiBzdHJpbmcsIHRyYWNrQ29tbWVudHM6IGJvb2xlYW4gPSBmYWxzZSk6IENzc1NjYW5uZXIge1xuICAgIHJldHVybiBuZXcgQ3NzU2Nhbm5lcih0ZXh0LCB0cmFja0NvbW1lbnRzKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gY3NzU2Nhbm5lckVycm9yKHRva2VuOiBDc3NUb2tlbiwgbWVzc2FnZTogc3RyaW5nKTogRXJyb3Ige1xuICBjb25zdCBlcnJvciA9IEVycm9yKCdDc3NQYXJzZUVycm9yOiAnICsgbWVzc2FnZSk7XG4gIChlcnJvciBhcyBhbnkpW0VSUk9SX1JBV19NRVNTQUdFXSA9IG1lc3NhZ2U7XG4gIChlcnJvciBhcyBhbnkpW0VSUk9SX1RPS0VOXSA9IHRva2VuO1xuICByZXR1cm4gZXJyb3I7XG59XG5cbmNvbnN0IEVSUk9SX1RPS0VOID0gJ25nVG9rZW4nO1xuY29uc3QgRVJST1JfUkFXX01FU1NBR0UgPSAnbmdSYXdNZXNzYWdlJztcblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJhd01lc3NhZ2UoZXJyb3I6IEVycm9yKTogc3RyaW5nIHtcbiAgcmV0dXJuIChlcnJvciBhcyBhbnkpW0VSUk9SX1JBV19NRVNTQUdFXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFRva2VuKGVycm9yOiBFcnJvcik6IENzc1Rva2VuIHtcbiAgcmV0dXJuIChlcnJvciBhcyBhbnkpW0VSUk9SX1RPS0VOXTtcbn1cblxuZnVuY3Rpb24gX3RyYWNrV2hpdGVzcGFjZShtb2RlOiBDc3NMZXhlck1vZGUpIHtcbiAgc3dpdGNoIChtb2RlKSB7XG4gICAgY2FzZSBDc3NMZXhlck1vZGUuU0VMRUNUT1I6XG4gICAgY2FzZSBDc3NMZXhlck1vZGUuUFNFVURPX1NFTEVDVE9SOlxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLkFMTF9UUkFDS19XUzpcbiAgICBjYXNlIENzc0xleGVyTW9kZS5TVFlMRV9WQUxVRTpcbiAgICAgIHJldHVybiB0cnVlO1xuXG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5leHBvcnQgY2xhc3MgQ3NzU2Nhbm5lciB7XG4gIC8vIFRPRE8oaXNzdWUvMjQ1NzEpOiByZW1vdmUgJyEnLlxuICBwZWVrICE6IG51bWJlcjtcbiAgcGVla1BlZWs6IG51bWJlcjtcbiAgbGVuZ3RoOiBudW1iZXIgPSAwO1xuICBpbmRleDogbnVtYmVyID0gLTE7XG4gIGNvbHVtbjogbnVtYmVyID0gLTE7XG4gIGxpbmU6IG51bWJlciA9IDA7XG5cbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfY3VycmVudE1vZGU6IENzc0xleGVyTW9kZSA9IENzc0xleGVyTW9kZS5CTE9DSztcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBfY3VycmVudEVycm9yOiBFcnJvcnxudWxsID0gbnVsbDtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgaW5wdXQ6IHN0cmluZywgcHJpdmF0ZSBfdHJhY2tDb21tZW50czogYm9vbGVhbiA9IGZhbHNlKSB7XG4gICAgdGhpcy5sZW5ndGggPSB0aGlzLmlucHV0Lmxlbmd0aDtcbiAgICB0aGlzLnBlZWtQZWVrID0gdGhpcy5wZWVrQXQoMCk7XG4gICAgdGhpcy5hZHZhbmNlKCk7XG4gIH1cblxuICBnZXRNb2RlKCk6IENzc0xleGVyTW9kZSB7IHJldHVybiB0aGlzLl9jdXJyZW50TW9kZTsgfVxuXG4gIHNldE1vZGUobW9kZTogQ3NzTGV4ZXJNb2RlKSB7XG4gICAgaWYgKHRoaXMuX2N1cnJlbnRNb2RlICE9IG1vZGUpIHtcbiAgICAgIGlmIChfdHJhY2tXaGl0ZXNwYWNlKHRoaXMuX2N1cnJlbnRNb2RlKSAmJiAhX3RyYWNrV2hpdGVzcGFjZShtb2RlKSkge1xuICAgICAgICB0aGlzLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG4gICAgICB9XG4gICAgICB0aGlzLl9jdXJyZW50TW9kZSA9IG1vZGU7XG4gICAgfVxuICB9XG5cbiAgYWR2YW5jZSgpOiB2b2lkIHtcbiAgICBpZiAoaXNOZXdsaW5lKHRoaXMucGVlaykpIHtcbiAgICAgIHRoaXMuY29sdW1uID0gMDtcbiAgICAgIHRoaXMubGluZSsrO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmNvbHVtbisrO1xuICAgIH1cblxuICAgIHRoaXMuaW5kZXgrKztcbiAgICB0aGlzLnBlZWsgPSB0aGlzLnBlZWtQZWVrO1xuICAgIHRoaXMucGVla1BlZWsgPSB0aGlzLnBlZWtBdCh0aGlzLmluZGV4ICsgMSk7XG4gIH1cblxuICBwZWVrQXQoaW5kZXg6IG51bWJlcik6IG51bWJlciB7XG4gICAgcmV0dXJuIGluZGV4ID49IHRoaXMubGVuZ3RoID8gY2hhcnMuJEVPRiA6IHRoaXMuaW5wdXQuY2hhckNvZGVBdChpbmRleCk7XG4gIH1cblxuICBjb25zdW1lRW1wdHlTdGF0ZW1lbnRzKCk6IHZvaWQge1xuICAgIHRoaXMuY29uc3VtZVdoaXRlc3BhY2UoKTtcbiAgICB3aGlsZSAodGhpcy5wZWVrID09IGNoYXJzLiRTRU1JQ09MT04pIHtcbiAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgICAgdGhpcy5jb25zdW1lV2hpdGVzcGFjZSgpO1xuICAgIH1cbiAgfVxuXG4gIGNvbnN1bWVXaGl0ZXNwYWNlKCk6IHZvaWQge1xuICAgIHdoaWxlIChjaGFycy5pc1doaXRlc3BhY2UodGhpcy5wZWVrKSB8fCBpc05ld2xpbmUodGhpcy5wZWVrKSkge1xuICAgICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgICBpZiAoIXRoaXMuX3RyYWNrQ29tbWVudHMgJiYgaXNDb21tZW50U3RhcnQodGhpcy5wZWVrLCB0aGlzLnBlZWtQZWVrKSkge1xuICAgICAgICB0aGlzLmFkdmFuY2UoKTsgIC8vIC9cbiAgICAgICAgdGhpcy5hZHZhbmNlKCk7ICAvLyAqXG4gICAgICAgIHdoaWxlICghaXNDb21tZW50RW5kKHRoaXMucGVlaywgdGhpcy5wZWVrUGVlaykpIHtcbiAgICAgICAgICBpZiAodGhpcy5wZWVrID09IGNoYXJzLiRFT0YpIHtcbiAgICAgICAgICAgIHRoaXMuZXJyb3IoJ1VudGVybWluYXRlZCBjb21tZW50Jyk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgICAgICB9XG4gICAgICAgIHRoaXMuYWR2YW5jZSgpOyAgLy8gKlxuICAgICAgICB0aGlzLmFkdmFuY2UoKTsgIC8vIC9cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBjb25zdW1lKHR5cGU6IENzc1Rva2VuVHlwZSwgdmFsdWU6IHN0cmluZ3xudWxsID0gbnVsbCk6IExleGVkQ3NzUmVzdWx0IHtcbiAgICBjb25zdCBtb2RlID0gdGhpcy5fY3VycmVudE1vZGU7XG5cbiAgICB0aGlzLnNldE1vZGUoX3RyYWNrV2hpdGVzcGFjZShtb2RlKSA/IENzc0xleGVyTW9kZS5BTExfVFJBQ0tfV1MgOiBDc3NMZXhlck1vZGUuQUxMKTtcblxuICAgIGNvbnN0IHByZXZpb3VzSW5kZXggPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHByZXZpb3VzTGluZSA9IHRoaXMubGluZTtcbiAgICBjb25zdCBwcmV2aW91c0NvbHVtbiA9IHRoaXMuY29sdW1uO1xuXG4gICAgbGV0IG5leHQ6IENzc1Rva2VuID0gdW5kZWZpbmVkICE7XG4gICAgY29uc3Qgb3V0cHV0ID0gdGhpcy5zY2FuKCk7XG4gICAgaWYgKG91dHB1dCAhPSBudWxsKSB7XG4gICAgICAvLyBqdXN0IGluY2FzZSB0aGUgaW5uZXIgc2NhbiBtZXRob2QgcmV0dXJuZWQgYW4gZXJyb3JcbiAgICAgIGlmIChvdXRwdXQuZXJyb3IgIT0gbnVsbCkge1xuICAgICAgICB0aGlzLnNldE1vZGUobW9kZSk7XG4gICAgICAgIHJldHVybiBvdXRwdXQ7XG4gICAgICB9XG5cbiAgICAgIG5leHQgPSBvdXRwdXQudG9rZW47XG4gICAgfVxuXG4gICAgaWYgKG5leHQgPT0gbnVsbCkge1xuICAgICAgbmV4dCA9IG5ldyBDc3NUb2tlbih0aGlzLmluZGV4LCB0aGlzLmNvbHVtbiwgdGhpcy5saW5lLCBDc3NUb2tlblR5cGUuRU9GLCAnZW5kIG9mIGZpbGUnKTtcbiAgICB9XG5cbiAgICBsZXQgaXNNYXRjaGluZ1R5cGU6IGJvb2xlYW4gPSBmYWxzZTtcbiAgICBpZiAodHlwZSA9PSBDc3NUb2tlblR5cGUuSWRlbnRpZmllck9yTnVtYmVyKSB7XG4gICAgICAvLyBUT0RPIChtYXRza28pOiBpbXBsZW1lbnQgYXJyYXkgdHJhdmVyc2FsIGZvciBsb29rdXAgaGVyZVxuICAgICAgaXNNYXRjaGluZ1R5cGUgPSBuZXh0LnR5cGUgPT0gQ3NzVG9rZW5UeXBlLk51bWJlciB8fCBuZXh0LnR5cGUgPT0gQ3NzVG9rZW5UeXBlLklkZW50aWZpZXI7XG4gICAgfSBlbHNlIHtcbiAgICAgIGlzTWF0Y2hpbmdUeXBlID0gbmV4dC50eXBlID09IHR5cGU7XG4gICAgfVxuXG4gICAgLy8gYmVmb3JlIHRocm93aW5nIHRoZSBlcnJvciB3ZSBuZWVkIHRvIGJyaW5nIGJhY2sgdGhlIGZvcm1lclxuICAgIC8vIG1vZGUgc28gdGhhdCB0aGUgcGFyc2VyIGNhbiByZWNvdmVyLi4uXG4gICAgdGhpcy5zZXRNb2RlKG1vZGUpO1xuXG4gICAgbGV0IGVycm9yOiBFcnJvcnxudWxsID0gbnVsbDtcbiAgICBpZiAoIWlzTWF0Y2hpbmdUeXBlIHx8ICh2YWx1ZSAhPSBudWxsICYmIHZhbHVlICE9IG5leHQuc3RyVmFsdWUpKSB7XG4gICAgICBsZXQgZXJyb3JNZXNzYWdlID1cbiAgICAgICAgICBDc3NUb2tlblR5cGVbbmV4dC50eXBlXSArICcgZG9lcyBub3QgbWF0Y2ggZXhwZWN0ZWQgJyArIENzc1Rva2VuVHlwZVt0eXBlXSArICcgdmFsdWUnO1xuXG4gICAgICBpZiAodmFsdWUgIT0gbnVsbCkge1xuICAgICAgICBlcnJvck1lc3NhZ2UgKz0gJyAoXCInICsgbmV4dC5zdHJWYWx1ZSArICdcIiBzaG91bGQgbWF0Y2ggXCInICsgdmFsdWUgKyAnXCIpJztcbiAgICAgIH1cblxuICAgICAgZXJyb3IgPSBjc3NTY2FubmVyRXJyb3IoXG4gICAgICAgICAgbmV4dCwgZ2VuZXJhdGVFcnJvck1lc3NhZ2UoXG4gICAgICAgICAgICAgICAgICAgIHRoaXMuaW5wdXQsIGVycm9yTWVzc2FnZSwgbmV4dC5zdHJWYWx1ZSwgcHJldmlvdXNJbmRleCwgcHJldmlvdXNMaW5lLFxuICAgICAgICAgICAgICAgICAgICBwcmV2aW91c0NvbHVtbikpO1xuICAgIH1cblxuICAgIHJldHVybiBuZXcgTGV4ZWRDc3NSZXN1bHQoZXJyb3IsIG5leHQpO1xuICB9XG5cblxuICBzY2FuKCk6IExleGVkQ3NzUmVzdWx0fG51bGwge1xuICAgIGNvbnN0IHRyYWNrV1MgPSBfdHJhY2tXaGl0ZXNwYWNlKHRoaXMuX2N1cnJlbnRNb2RlKTtcbiAgICBpZiAodGhpcy5pbmRleCA9PSAwICYmICF0cmFja1dTKSB7ICAvLyBmaXJzdCBzY2FuXG4gICAgICB0aGlzLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG4gICAgfVxuXG4gICAgY29uc3QgdG9rZW4gPSB0aGlzLl9zY2FuKCk7XG4gICAgaWYgKHRva2VuID09IG51bGwpIHJldHVybiBudWxsO1xuXG4gICAgY29uc3QgZXJyb3IgPSB0aGlzLl9jdXJyZW50RXJyb3IgITtcbiAgICB0aGlzLl9jdXJyZW50RXJyb3IgPSBudWxsO1xuXG4gICAgaWYgKCF0cmFja1dTKSB7XG4gICAgICB0aGlzLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG4gICAgfVxuICAgIHJldHVybiBuZXcgTGV4ZWRDc3NSZXN1bHQoZXJyb3IsIHRva2VuKTtcbiAgfVxuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX3NjYW4oKTogQ3NzVG9rZW58bnVsbCB7XG4gICAgbGV0IHBlZWsgPSB0aGlzLnBlZWs7XG4gICAgbGV0IHBlZWtQZWVrID0gdGhpcy5wZWVrUGVlaztcbiAgICBpZiAocGVlayA9PSBjaGFycy4kRU9GKSByZXR1cm4gbnVsbDtcblxuICAgIGlmIChpc0NvbW1lbnRTdGFydChwZWVrLCBwZWVrUGVlaykpIHtcbiAgICAgIC8vIGV2ZW4gaWYgY29tbWVudHMgYXJlIG5vdCB0cmFja2VkIHdlIHN0aWxsIGxleCB0aGVcbiAgICAgIC8vIGNvbW1lbnQgc28gd2UgY2FuIG1vdmUgdGhlIHBvaW50ZXIgZm9yd2FyZFxuICAgICAgY29uc3QgY29tbWVudFRva2VuID0gdGhpcy5zY2FuQ29tbWVudCgpO1xuICAgICAgaWYgKHRoaXMuX3RyYWNrQ29tbWVudHMpIHtcbiAgICAgICAgcmV0dXJuIGNvbW1lbnRUb2tlbjtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoX3RyYWNrV2hpdGVzcGFjZSh0aGlzLl9jdXJyZW50TW9kZSkgJiYgKGNoYXJzLmlzV2hpdGVzcGFjZShwZWVrKSB8fCBpc05ld2xpbmUocGVlaykpKSB7XG4gICAgICByZXR1cm4gdGhpcy5zY2FuV2hpdGVzcGFjZSgpO1xuICAgIH1cblxuICAgIHBlZWsgPSB0aGlzLnBlZWs7XG4gICAgcGVla1BlZWsgPSB0aGlzLnBlZWtQZWVrO1xuICAgIGlmIChwZWVrID09IGNoYXJzLiRFT0YpIHJldHVybiBudWxsO1xuXG4gICAgaWYgKGlzU3RyaW5nU3RhcnQocGVlaywgcGVla1BlZWspKSB7XG4gICAgICByZXR1cm4gdGhpcy5zY2FuU3RyaW5nKCk7XG4gICAgfVxuXG4gICAgLy8gc29tZXRoaW5nIGxpa2UgdXJsKGNvb2wpXG4gICAgaWYgKHRoaXMuX2N1cnJlbnRNb2RlID09IENzc0xleGVyTW9kZS5TVFlMRV9WQUxVRV9GVU5DVElPTikge1xuICAgICAgcmV0dXJuIHRoaXMuc2NhbkNzc1ZhbHVlRnVuY3Rpb24oKTtcbiAgICB9XG5cbiAgICBjb25zdCBpc01vZGlmaWVyID0gcGVlayA9PSBjaGFycy4kUExVUyB8fCBwZWVrID09IGNoYXJzLiRNSU5VUztcbiAgICBjb25zdCBkaWdpdEEgPSBpc01vZGlmaWVyID8gZmFsc2UgOiBjaGFycy5pc0RpZ2l0KHBlZWspO1xuICAgIGNvbnN0IGRpZ2l0QiA9IGNoYXJzLmlzRGlnaXQocGVla1BlZWspO1xuICAgIGlmIChkaWdpdEEgfHwgKGlzTW9kaWZpZXIgJiYgKHBlZWtQZWVrID09IGNoYXJzLiRQRVJJT0QgfHwgZGlnaXRCKSkgfHxcbiAgICAgICAgKHBlZWsgPT0gY2hhcnMuJFBFUklPRCAmJiBkaWdpdEIpKSB7XG4gICAgICByZXR1cm4gdGhpcy5zY2FuTnVtYmVyKCk7XG4gICAgfVxuXG4gICAgaWYgKHBlZWsgPT0gY2hhcnMuJEFUKSB7XG4gICAgICByZXR1cm4gdGhpcy5zY2FuQXRFeHByZXNzaW9uKCk7XG4gICAgfVxuXG4gICAgaWYgKGlzSWRlbnRpZmllclN0YXJ0KHBlZWssIHBlZWtQZWVrKSkge1xuICAgICAgcmV0dXJuIHRoaXMuc2NhbklkZW50aWZpZXIoKTtcbiAgICB9XG5cbiAgICBpZiAoaXNWYWxpZENzc0NoYXJhY3RlcihwZWVrLCB0aGlzLl9jdXJyZW50TW9kZSkpIHtcbiAgICAgIHJldHVybiB0aGlzLnNjYW5DaGFyYWN0ZXIoKTtcbiAgICB9XG5cbiAgICByZXR1cm4gdGhpcy5lcnJvcihgVW5leHBlY3RlZCBjaGFyYWN0ZXIgWyR7U3RyaW5nLmZyb21DaGFyQ29kZShwZWVrKX1dYCk7XG4gIH1cblxuICBzY2FuQ29tbWVudCgpOiBDc3NUb2tlbnxudWxsIHtcbiAgICBpZiAodGhpcy5hc3NlcnRDb25kaXRpb24oXG4gICAgICAgICAgICBpc0NvbW1lbnRTdGFydCh0aGlzLnBlZWssIHRoaXMucGVla1BlZWspLCAnRXhwZWN0ZWQgY29tbWVudCBzdGFydCB2YWx1ZScpKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG5cbiAgICBjb25zdCBzdGFydCA9IHRoaXMuaW5kZXg7XG4gICAgY29uc3Qgc3RhcnRpbmdDb2x1bW4gPSB0aGlzLmNvbHVtbjtcbiAgICBjb25zdCBzdGFydGluZ0xpbmUgPSB0aGlzLmxpbmU7XG5cbiAgICB0aGlzLmFkdmFuY2UoKTsgIC8vIC9cbiAgICB0aGlzLmFkdmFuY2UoKTsgIC8vICpcblxuICAgIHdoaWxlICghaXNDb21tZW50RW5kKHRoaXMucGVlaywgdGhpcy5wZWVrUGVlaykpIHtcbiAgICAgIGlmICh0aGlzLnBlZWsgPT0gY2hhcnMuJEVPRikge1xuICAgICAgICB0aGlzLmVycm9yKCdVbnRlcm1pbmF0ZWQgY29tbWVudCcpO1xuICAgICAgfVxuICAgICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgfVxuXG4gICAgdGhpcy5hZHZhbmNlKCk7ICAvLyAqXG4gICAgdGhpcy5hZHZhbmNlKCk7ICAvLyAvXG5cbiAgICBjb25zdCBzdHIgPSB0aGlzLmlucHV0LnN1YnN0cmluZyhzdGFydCwgdGhpcy5pbmRleCk7XG4gICAgcmV0dXJuIG5ldyBDc3NUb2tlbihzdGFydCwgc3RhcnRpbmdDb2x1bW4sIHN0YXJ0aW5nTGluZSwgQ3NzVG9rZW5UeXBlLkNvbW1lbnQsIHN0cik7XG4gIH1cblxuICBzY2FuV2hpdGVzcGFjZSgpOiBDc3NUb2tlbiB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgY29uc3Qgc3RhcnRpbmdMaW5lID0gdGhpcy5saW5lO1xuICAgIHdoaWxlIChjaGFycy5pc1doaXRlc3BhY2UodGhpcy5wZWVrKSAmJiB0aGlzLnBlZWsgIT0gY2hhcnMuJEVPRikge1xuICAgICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgfVxuICAgIGNvbnN0IHN0ciA9IHRoaXMuaW5wdXQuc3Vic3RyaW5nKHN0YXJ0LCB0aGlzLmluZGV4KTtcbiAgICByZXR1cm4gbmV3IENzc1Rva2VuKHN0YXJ0LCBzdGFydGluZ0NvbHVtbiwgc3RhcnRpbmdMaW5lLCBDc3NUb2tlblR5cGUuV2hpdGVzcGFjZSwgc3RyKTtcbiAgfVxuXG4gIHNjYW5TdHJpbmcoKTogQ3NzVG9rZW58bnVsbCB7XG4gICAgaWYgKHRoaXMuYXNzZXJ0Q29uZGl0aW9uKFxuICAgICAgICAgICAgaXNTdHJpbmdTdGFydCh0aGlzLnBlZWssIHRoaXMucGVla1BlZWspLCAnVW5leHBlY3RlZCBub24tc3RyaW5nIHN0YXJ0aW5nIHZhbHVlJykpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIGNvbnN0IHRhcmdldCA9IHRoaXMucGVlaztcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuaW5kZXg7XG4gICAgY29uc3Qgc3RhcnRpbmdDb2x1bW4gPSB0aGlzLmNvbHVtbjtcbiAgICBjb25zdCBzdGFydGluZ0xpbmUgPSB0aGlzLmxpbmU7XG4gICAgbGV0IHByZXZpb3VzID0gdGFyZ2V0O1xuICAgIHRoaXMuYWR2YW5jZSgpO1xuXG4gICAgd2hpbGUgKCFpc0NoYXJNYXRjaCh0YXJnZXQsIHByZXZpb3VzLCB0aGlzLnBlZWspKSB7XG4gICAgICBpZiAodGhpcy5wZWVrID09IGNoYXJzLiRFT0YgfHwgaXNOZXdsaW5lKHRoaXMucGVlaykpIHtcbiAgICAgICAgdGhpcy5lcnJvcignVW50ZXJtaW5hdGVkIHF1b3RlJyk7XG4gICAgICB9XG4gICAgICBwcmV2aW91cyA9IHRoaXMucGVlaztcbiAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgIH1cblxuICAgIGlmICh0aGlzLmFzc2VydENvbmRpdGlvbih0aGlzLnBlZWsgPT0gdGFyZ2V0LCAnVW50ZXJtaW5hdGVkIHF1b3RlJykpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICB0aGlzLmFkdmFuY2UoKTtcblxuICAgIGNvbnN0IHN0ciA9IHRoaXMuaW5wdXQuc3Vic3RyaW5nKHN0YXJ0LCB0aGlzLmluZGV4KTtcbiAgICByZXR1cm4gbmV3IENzc1Rva2VuKHN0YXJ0LCBzdGFydGluZ0NvbHVtbiwgc3RhcnRpbmdMaW5lLCBDc3NUb2tlblR5cGUuU3RyaW5nLCBzdHIpO1xuICB9XG5cbiAgc2Nhbk51bWJlcigpOiBDc3NUb2tlbiB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgaWYgKHRoaXMucGVlayA9PSBjaGFycy4kUExVUyB8fCB0aGlzLnBlZWsgPT0gY2hhcnMuJE1JTlVTKSB7XG4gICAgICB0aGlzLmFkdmFuY2UoKTtcbiAgICB9XG4gICAgbGV0IHBlcmlvZFVzZWQgPSBmYWxzZTtcbiAgICB3aGlsZSAoY2hhcnMuaXNEaWdpdCh0aGlzLnBlZWspIHx8IHRoaXMucGVlayA9PSBjaGFycy4kUEVSSU9EKSB7XG4gICAgICBpZiAodGhpcy5wZWVrID09IGNoYXJzLiRQRVJJT0QpIHtcbiAgICAgICAgaWYgKHBlcmlvZFVzZWQpIHtcbiAgICAgICAgICB0aGlzLmVycm9yKCdVbmV4cGVjdGVkIHVzZSBvZiBhIHNlY29uZCBwZXJpb2QgdmFsdWUnKTtcbiAgICAgICAgfVxuICAgICAgICBwZXJpb2RVc2VkID0gdHJ1ZTtcbiAgICAgIH1cbiAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgIH1cbiAgICBjb25zdCBzdHJWYWx1ZSA9IHRoaXMuaW5wdXQuc3Vic3RyaW5nKHN0YXJ0LCB0aGlzLmluZGV4KTtcbiAgICByZXR1cm4gbmV3IENzc1Rva2VuKHN0YXJ0LCBzdGFydGluZ0NvbHVtbiwgdGhpcy5saW5lLCBDc3NUb2tlblR5cGUuTnVtYmVyLCBzdHJWYWx1ZSk7XG4gIH1cblxuICBzY2FuSWRlbnRpZmllcigpOiBDc3NUb2tlbnxudWxsIHtcbiAgICBpZiAodGhpcy5hc3NlcnRDb25kaXRpb24oXG4gICAgICAgICAgICBpc0lkZW50aWZpZXJTdGFydCh0aGlzLnBlZWssIHRoaXMucGVla1BlZWspLCAnRXhwZWN0ZWQgaWRlbnRpZmllciBzdGFydGluZyB2YWx1ZScpKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG5cbiAgICBjb25zdCBzdGFydCA9IHRoaXMuaW5kZXg7XG4gICAgY29uc3Qgc3RhcnRpbmdDb2x1bW4gPSB0aGlzLmNvbHVtbjtcbiAgICB3aGlsZSAoaXNJZGVudGlmaWVyUGFydCh0aGlzLnBlZWspKSB7XG4gICAgICB0aGlzLmFkdmFuY2UoKTtcbiAgICB9XG4gICAgY29uc3Qgc3RyVmFsdWUgPSB0aGlzLmlucHV0LnN1YnN0cmluZyhzdGFydCwgdGhpcy5pbmRleCk7XG4gICAgcmV0dXJuIG5ldyBDc3NUb2tlbihzdGFydCwgc3RhcnRpbmdDb2x1bW4sIHRoaXMubGluZSwgQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIsIHN0clZhbHVlKTtcbiAgfVxuXG4gIHNjYW5Dc3NWYWx1ZUZ1bmN0aW9uKCk6IENzc1Rva2VuIHtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuaW5kZXg7XG4gICAgY29uc3Qgc3RhcnRpbmdDb2x1bW4gPSB0aGlzLmNvbHVtbjtcbiAgICBsZXQgcGFyZW5CYWxhbmNlID0gMTtcbiAgICB3aGlsZSAodGhpcy5wZWVrICE9IGNoYXJzLiRFT0YgJiYgcGFyZW5CYWxhbmNlID4gMCkge1xuICAgICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgICBpZiAodGhpcy5wZWVrID09IGNoYXJzLiRMUEFSRU4pIHtcbiAgICAgICAgcGFyZW5CYWxhbmNlKys7XG4gICAgICB9IGVsc2UgaWYgKHRoaXMucGVlayA9PSBjaGFycy4kUlBBUkVOKSB7XG4gICAgICAgIHBhcmVuQmFsYW5jZS0tO1xuICAgICAgfVxuICAgIH1cbiAgICBjb25zdCBzdHJWYWx1ZSA9IHRoaXMuaW5wdXQuc3Vic3RyaW5nKHN0YXJ0LCB0aGlzLmluZGV4KTtcbiAgICByZXR1cm4gbmV3IENzc1Rva2VuKHN0YXJ0LCBzdGFydGluZ0NvbHVtbiwgdGhpcy5saW5lLCBDc3NUb2tlblR5cGUuSWRlbnRpZmllciwgc3RyVmFsdWUpO1xuICB9XG5cbiAgc2NhbkNoYXJhY3RlcigpOiBDc3NUb2tlbnxudWxsIHtcbiAgICBjb25zdCBzdGFydCA9IHRoaXMuaW5kZXg7XG4gICAgY29uc3Qgc3RhcnRpbmdDb2x1bW4gPSB0aGlzLmNvbHVtbjtcbiAgICBpZiAodGhpcy5hc3NlcnRDb25kaXRpb24oXG4gICAgICAgICAgICBpc1ZhbGlkQ3NzQ2hhcmFjdGVyKHRoaXMucGVlaywgdGhpcy5fY3VycmVudE1vZGUpLFxuICAgICAgICAgICAgY2hhclN0cih0aGlzLnBlZWspICsgJyBpcyBub3QgYSB2YWxpZCBDU1MgY2hhcmFjdGVyJykpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIGNvbnN0IGMgPSB0aGlzLmlucHV0LnN1YnN0cmluZyhzdGFydCwgc3RhcnQgKyAxKTtcbiAgICB0aGlzLmFkdmFuY2UoKTtcblxuICAgIHJldHVybiBuZXcgQ3NzVG9rZW4oc3RhcnQsIHN0YXJ0aW5nQ29sdW1uLCB0aGlzLmxpbmUsIENzc1Rva2VuVHlwZS5DaGFyYWN0ZXIsIGMpO1xuICB9XG5cbiAgc2NhbkF0RXhwcmVzc2lvbigpOiBDc3NUb2tlbnxudWxsIHtcbiAgICBpZiAodGhpcy5hc3NlcnRDb25kaXRpb24odGhpcy5wZWVrID09IGNoYXJzLiRBVCwgJ0V4cGVjdGVkIEAgdmFsdWUnKSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgaWYgKGlzSWRlbnRpZmllclN0YXJ0KHRoaXMucGVlaywgdGhpcy5wZWVrUGVlaykpIHtcbiAgICAgIGNvbnN0IGlkZW50ID0gdGhpcy5zY2FuSWRlbnRpZmllcigpICE7XG4gICAgICBjb25zdCBzdHJWYWx1ZSA9ICdAJyArIGlkZW50LnN0clZhbHVlO1xuICAgICAgcmV0dXJuIG5ldyBDc3NUb2tlbihzdGFydCwgc3RhcnRpbmdDb2x1bW4sIHRoaXMubGluZSwgQ3NzVG9rZW5UeXBlLkF0S2V5d29yZCwgc3RyVmFsdWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdGhpcy5zY2FuQ2hhcmFjdGVyKCk7XG4gICAgfVxuICB9XG5cbiAgYXNzZXJ0Q29uZGl0aW9uKHN0YXR1czogYm9vbGVhbiwgZXJyb3JNZXNzYWdlOiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICBpZiAoIXN0YXR1cykge1xuICAgICAgdGhpcy5lcnJvcihlcnJvck1lc3NhZ2UpO1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfVxuICAgIHJldHVybiBmYWxzZTtcbiAgfVxuXG4gIGVycm9yKG1lc3NhZ2U6IHN0cmluZywgZXJyb3JUb2tlblZhbHVlOiBzdHJpbmd8bnVsbCA9IG51bGwsIGRvTm90QWR2YW5jZTogYm9vbGVhbiA9IGZhbHNlKTpcbiAgICAgIENzc1Rva2VuIHtcbiAgICBjb25zdCBpbmRleDogbnVtYmVyID0gdGhpcy5pbmRleDtcbiAgICBjb25zdCBjb2x1bW46IG51bWJlciA9IHRoaXMuY29sdW1uO1xuICAgIGNvbnN0IGxpbmU6IG51bWJlciA9IHRoaXMubGluZTtcbiAgICBlcnJvclRva2VuVmFsdWUgPSBlcnJvclRva2VuVmFsdWUgfHwgU3RyaW5nLmZyb21DaGFyQ29kZSh0aGlzLnBlZWspO1xuICAgIGNvbnN0IGludmFsaWRUb2tlbiA9IG5ldyBDc3NUb2tlbihpbmRleCwgY29sdW1uLCBsaW5lLCBDc3NUb2tlblR5cGUuSW52YWxpZCwgZXJyb3JUb2tlblZhbHVlKTtcbiAgICBjb25zdCBlcnJvck1lc3NhZ2UgPVxuICAgICAgICBnZW5lcmF0ZUVycm9yTWVzc2FnZSh0aGlzLmlucHV0LCBtZXNzYWdlLCBlcnJvclRva2VuVmFsdWUsIGluZGV4LCBsaW5lLCBjb2x1bW4pO1xuICAgIGlmICghZG9Ob3RBZHZhbmNlKSB7XG4gICAgICB0aGlzLmFkdmFuY2UoKTtcbiAgICB9XG4gICAgdGhpcy5fY3VycmVudEVycm9yID0gY3NzU2Nhbm5lckVycm9yKGludmFsaWRUb2tlbiwgZXJyb3JNZXNzYWdlKTtcbiAgICByZXR1cm4gaW52YWxpZFRva2VuO1xuICB9XG59XG5cbmZ1bmN0aW9uIGlzQ2hhck1hdGNoKHRhcmdldDogbnVtYmVyLCBwcmV2aW91czogbnVtYmVyLCBjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgcmV0dXJuIGNvZGUgPT0gdGFyZ2V0ICYmIHByZXZpb3VzICE9IGNoYXJzLiRCQUNLU0xBU0g7XG59XG5cbmZ1bmN0aW9uIGlzQ29tbWVudFN0YXJ0KGNvZGU6IG51bWJlciwgbmV4dDogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHJldHVybiBjb2RlID09IGNoYXJzLiRTTEFTSCAmJiBuZXh0ID09IGNoYXJzLiRTVEFSO1xufVxuXG5mdW5jdGlvbiBpc0NvbW1lbnRFbmQoY29kZTogbnVtYmVyLCBuZXh0OiBudW1iZXIpOiBib29sZWFuIHtcbiAgcmV0dXJuIGNvZGUgPT0gY2hhcnMuJFNUQVIgJiYgbmV4dCA9PSBjaGFycy4kU0xBU0g7XG59XG5cbmZ1bmN0aW9uIGlzU3RyaW5nU3RhcnQoY29kZTogbnVtYmVyLCBuZXh0OiBudW1iZXIpOiBib29sZWFuIHtcbiAgbGV0IHRhcmdldCA9IGNvZGU7XG4gIGlmICh0YXJnZXQgPT0gY2hhcnMuJEJBQ0tTTEFTSCkge1xuICAgIHRhcmdldCA9IG5leHQ7XG4gIH1cbiAgcmV0dXJuIHRhcmdldCA9PSBjaGFycy4kRFEgfHwgdGFyZ2V0ID09IGNoYXJzLiRTUTtcbn1cblxuZnVuY3Rpb24gaXNJZGVudGlmaWVyU3RhcnQoY29kZTogbnVtYmVyLCBuZXh0OiBudW1iZXIpOiBib29sZWFuIHtcbiAgbGV0IHRhcmdldCA9IGNvZGU7XG4gIGlmICh0YXJnZXQgPT0gY2hhcnMuJE1JTlVTKSB7XG4gICAgdGFyZ2V0ID0gbmV4dDtcbiAgfVxuXG4gIHJldHVybiBjaGFycy5pc0FzY2lpTGV0dGVyKHRhcmdldCkgfHwgdGFyZ2V0ID09IGNoYXJzLiRCQUNLU0xBU0ggfHwgdGFyZ2V0ID09IGNoYXJzLiRNSU5VUyB8fFxuICAgICAgdGFyZ2V0ID09IGNoYXJzLiRfO1xufVxuXG5mdW5jdGlvbiBpc0lkZW50aWZpZXJQYXJ0KHRhcmdldDogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHJldHVybiBjaGFycy5pc0FzY2lpTGV0dGVyKHRhcmdldCkgfHwgdGFyZ2V0ID09IGNoYXJzLiRCQUNLU0xBU0ggfHwgdGFyZ2V0ID09IGNoYXJzLiRNSU5VUyB8fFxuICAgICAgdGFyZ2V0ID09IGNoYXJzLiRfIHx8IGNoYXJzLmlzRGlnaXQodGFyZ2V0KTtcbn1cblxuZnVuY3Rpb24gaXNWYWxpZFBzZXVkb1NlbGVjdG9yQ2hhcmFjdGVyKGNvZGU6IG51bWJlcik6IGJvb2xlYW4ge1xuICBzd2l0Y2ggKGNvZGUpIHtcbiAgICBjYXNlIGNoYXJzLiRMUEFSRU46XG4gICAgY2FzZSBjaGFycy4kUlBBUkVOOlxuICAgICAgcmV0dXJuIHRydWU7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5mdW5jdGlvbiBpc1ZhbGlkS2V5ZnJhbWVCbG9ja0NoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgcmV0dXJuIGNvZGUgPT0gY2hhcnMuJFBFUkNFTlQ7XG59XG5cbmZ1bmN0aW9uIGlzVmFsaWRBdHRyaWJ1dGVTZWxlY3RvckNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgLy8gdmFsdWVeKnwkfj1zb21ldGhpbmdcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kJDpcbiAgICBjYXNlIGNoYXJzLiRQSVBFOlxuICAgIGNhc2UgY2hhcnMuJENBUkVUOlxuICAgIGNhc2UgY2hhcnMuJFRJTERBOlxuICAgIGNhc2UgY2hhcnMuJFNUQVI6XG4gICAgY2FzZSBjaGFycy4kRVE6XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICBkZWZhdWx0OlxuICAgICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbmZ1bmN0aW9uIGlzVmFsaWRTZWxlY3RvckNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgLy8gc2VsZWN0b3IgWyBrZXkgICA9IHZhbHVlIF1cbiAgLy8gSURFTlQgICAgQyBJREVOVCBDIElERU5UIENcbiAgLy8gI2lkLCAuY2xhc3MsICorfj5cbiAgLy8gdGFnOlBTRVVET1xuICBzd2l0Y2ggKGNvZGUpIHtcbiAgICBjYXNlIGNoYXJzLiRIQVNIOlxuICAgIGNhc2UgY2hhcnMuJFBFUklPRDpcbiAgICBjYXNlIGNoYXJzLiRUSUxEQTpcbiAgICBjYXNlIGNoYXJzLiRTVEFSOlxuICAgIGNhc2UgY2hhcnMuJFBMVVM6XG4gICAgY2FzZSBjaGFycy4kR1Q6XG4gICAgY2FzZSBjaGFycy4kQ09MT046XG4gICAgY2FzZSBjaGFycy4kUElQRTpcbiAgICBjYXNlIGNoYXJzLiRDT01NQTpcbiAgICBjYXNlIGNoYXJzLiRMQlJBQ0tFVDpcbiAgICBjYXNlIGNoYXJzLiRSQlJBQ0tFVDpcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNWYWxpZFN0eWxlQmxvY2tDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIC8vIGtleTp2YWx1ZTtcbiAgLy8ga2V5OmNhbGMoc29tZXRoaW5nIC4uLiApXG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJEhBU0g6XG4gICAgY2FzZSBjaGFycy4kU0VNSUNPTE9OOlxuICAgIGNhc2UgY2hhcnMuJENPTE9OOlxuICAgIGNhc2UgY2hhcnMuJFBFUkNFTlQ6XG4gICAgY2FzZSBjaGFycy4kU0xBU0g6XG4gICAgY2FzZSBjaGFycy4kQkFDS1NMQVNIOlxuICAgIGNhc2UgY2hhcnMuJEJBTkc6XG4gICAgY2FzZSBjaGFycy4kUEVSSU9EOlxuICAgIGNhc2UgY2hhcnMuJExQQVJFTjpcbiAgICBjYXNlIGNoYXJzLiRSUEFSRU46XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICBkZWZhdWx0OlxuICAgICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbmZ1bmN0aW9uIGlzVmFsaWRNZWRpYVF1ZXJ5UnVsZUNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgLy8gKG1pbi13aWR0aDogNy41ZW0pIGFuZCAob3JpZW50YXRpb246IGxhbmRzY2FwZSlcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kTFBBUkVOOlxuICAgIGNhc2UgY2hhcnMuJFJQQVJFTjpcbiAgICBjYXNlIGNoYXJzLiRDT0xPTjpcbiAgICBjYXNlIGNoYXJzLiRQRVJDRU5UOlxuICAgIGNhc2UgY2hhcnMuJFBFUklPRDpcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNWYWxpZEF0UnVsZUNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgLy8gQGRvY3VtZW50IHVybChodHRwOi8vd3d3LnczLm9yZy9wYWdlP3NvbWV0aGluZz1vbiNoYXNoKSxcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kTFBBUkVOOlxuICAgIGNhc2UgY2hhcnMuJFJQQVJFTjpcbiAgICBjYXNlIGNoYXJzLiRDT0xPTjpcbiAgICBjYXNlIGNoYXJzLiRQRVJDRU5UOlxuICAgIGNhc2UgY2hhcnMuJFBFUklPRDpcbiAgICBjYXNlIGNoYXJzLiRTTEFTSDpcbiAgICBjYXNlIGNoYXJzLiRCQUNLU0xBU0g6XG4gICAgY2FzZSBjaGFycy4kSEFTSDpcbiAgICBjYXNlIGNoYXJzLiRFUTpcbiAgICBjYXNlIGNoYXJzLiRRVUVTVElPTjpcbiAgICBjYXNlIGNoYXJzLiRBTVBFUlNBTkQ6XG4gICAgY2FzZSBjaGFycy4kU1RBUjpcbiAgICBjYXNlIGNoYXJzLiRDT01NQTpcbiAgICBjYXNlIGNoYXJzLiRNSU5VUzpcbiAgICBjYXNlIGNoYXJzLiRQTFVTOlxuICAgICAgcmV0dXJuIHRydWU7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5mdW5jdGlvbiBpc1ZhbGlkU3R5bGVGdW5jdGlvbkNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kUEVSSU9EOlxuICAgIGNhc2UgY2hhcnMuJE1JTlVTOlxuICAgIGNhc2UgY2hhcnMuJFBMVVM6XG4gICAgY2FzZSBjaGFycy4kU1RBUjpcbiAgICBjYXNlIGNoYXJzLiRTTEFTSDpcbiAgICBjYXNlIGNoYXJzLiRMUEFSRU46XG4gICAgY2FzZSBjaGFycy4kUlBBUkVOOlxuICAgIGNhc2UgY2hhcnMuJENPTU1BOlxuICAgICAgcmV0dXJuIHRydWU7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5mdW5jdGlvbiBpc1ZhbGlkQmxvY2tDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIC8vIEBzb21ldGhpbmcgeyB9XG4gIC8vIElERU5UXG4gIHJldHVybiBjb2RlID09IGNoYXJzLiRBVDtcbn1cblxuZnVuY3Rpb24gaXNWYWxpZENzc0NoYXJhY3Rlcihjb2RlOiBudW1iZXIsIG1vZGU6IENzc0xleGVyTW9kZSk6IGJvb2xlYW4ge1xuICBzd2l0Y2ggKG1vZGUpIHtcbiAgICBjYXNlIENzc0xleGVyTW9kZS5BTEw6XG4gICAgY2FzZSBDc3NMZXhlck1vZGUuQUxMX1RSQUNLX1dTOlxuICAgICAgcmV0dXJuIHRydWU7XG5cbiAgICBjYXNlIENzc0xleGVyTW9kZS5TRUxFQ1RPUjpcbiAgICAgIHJldHVybiBpc1ZhbGlkU2VsZWN0b3JDaGFyYWN0ZXIoY29kZSk7XG5cbiAgICBjYXNlIENzc0xleGVyTW9kZS5QU0VVRE9fU0VMRUNUT1JfV0lUSF9BUkdVTUVOVFM6XG4gICAgICByZXR1cm4gaXNWYWxpZFBzZXVkb1NlbGVjdG9yQ2hhcmFjdGVyKGNvZGUpO1xuXG4gICAgY2FzZSBDc3NMZXhlck1vZGUuQVRUUklCVVRFX1NFTEVDVE9SOlxuICAgICAgcmV0dXJuIGlzVmFsaWRBdHRyaWJ1dGVTZWxlY3RvckNoYXJhY3Rlcihjb2RlKTtcblxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLk1FRElBX1FVRVJZOlxuICAgICAgcmV0dXJuIGlzVmFsaWRNZWRpYVF1ZXJ5UnVsZUNoYXJhY3Rlcihjb2RlKTtcblxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLkFUX1JVTEVfUVVFUlk6XG4gICAgICByZXR1cm4gaXNWYWxpZEF0UnVsZUNoYXJhY3Rlcihjb2RlKTtcblxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLktFWUZSQU1FX0JMT0NLOlxuICAgICAgcmV0dXJuIGlzVmFsaWRLZXlmcmFtZUJsb2NrQ2hhcmFjdGVyKGNvZGUpO1xuXG4gICAgY2FzZSBDc3NMZXhlck1vZGUuU1RZTEVfQkxPQ0s6XG4gICAgY2FzZSBDc3NMZXhlck1vZGUuU1RZTEVfVkFMVUU6XG4gICAgICByZXR1cm4gaXNWYWxpZFN0eWxlQmxvY2tDaGFyYWN0ZXIoY29kZSk7XG5cbiAgICBjYXNlIENzc0xleGVyTW9kZS5TVFlMRV9DQUxDX0ZVTkNUSU9OOlxuICAgICAgcmV0dXJuIGlzVmFsaWRTdHlsZUZ1bmN0aW9uQ2hhcmFjdGVyKGNvZGUpO1xuXG4gICAgY2FzZSBDc3NMZXhlck1vZGUuQkxPQ0s6XG4gICAgICByZXR1cm4gaXNWYWxpZEJsb2NrQ2hhcmFjdGVyKGNvZGUpO1xuXG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5mdW5jdGlvbiBjaGFyQ29kZShpbnB1dDogc3RyaW5nLCBpbmRleDogbnVtYmVyKTogbnVtYmVyIHtcbiAgcmV0dXJuIGluZGV4ID49IGlucHV0Lmxlbmd0aCA/IGNoYXJzLiRFT0YgOiBpbnB1dC5jaGFyQ29kZUF0KGluZGV4KTtcbn1cblxuZnVuY3Rpb24gY2hhclN0cihjb2RlOiBudW1iZXIpOiBzdHJpbmcge1xuICByZXR1cm4gU3RyaW5nLmZyb21DaGFyQ29kZShjb2RlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzTmV3bGluZShjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kRkY6XG4gICAgY2FzZSBjaGFycy4kQ1I6XG4gICAgY2FzZSBjaGFycy4kTEY6XG4gICAgY2FzZSBjaGFycy4kVlRBQjpcbiAgICAgIHJldHVybiB0cnVlO1xuXG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuIl19