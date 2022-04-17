/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as chars from '../chars';
export var CssTokenType;
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
})(CssTokenType || (CssTokenType = {}));
export var CssLexerMode;
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
})(CssLexerMode || (CssLexerMode = {}));
var LexedCssResult = /** @class */ (function () {
    function LexedCssResult(error, token) {
        this.error = error;
        this.token = token;
    }
    return LexedCssResult;
}());
export { LexedCssResult };
export function generateErrorMessage(input, message, errorValue, index, row, column) {
    return message + " at column " + row + ":" + column + " in expression [" +
        findProblemCode(input, errorValue, index, column) + ']';
}
export function findProblemCode(input, errorValue, index, column) {
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
export { CssToken };
var CssLexer = /** @class */ (function () {
    function CssLexer() {
    }
    CssLexer.prototype.scan = function (text, trackComments) {
        if (trackComments === void 0) { trackComments = false; }
        return new CssScanner(text, trackComments);
    };
    return CssLexer;
}());
export { CssLexer };
export function cssScannerError(token, message) {
    var error = Error('CssParseError: ' + message);
    error[ERROR_RAW_MESSAGE] = message;
    error[ERROR_TOKEN] = token;
    return error;
}
var ERROR_TOKEN = 'ngToken';
var ERROR_RAW_MESSAGE = 'ngRawMessage';
export function getRawMessage(error) {
    return error[ERROR_RAW_MESSAGE];
}
export function getToken(error) {
    return error[ERROR_TOKEN];
}
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
export { CssScanner };
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
export function isNewline(code) {
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY3NzX2xleGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2Nzc19wYXJzZXIvY3NzX2xleGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUdILE9BQU8sS0FBSyxLQUFLLE1BQU0sVUFBVSxDQUFDO0FBRWxDLE1BQU0sQ0FBTixJQUFZLFlBV1g7QUFYRCxXQUFZLFlBQVk7SUFDdEIsNkNBQUcsQ0FBQTtJQUNILG1EQUFNLENBQUE7SUFDTixxREFBTyxDQUFBO0lBQ1AsMkRBQVUsQ0FBQTtJQUNWLG1EQUFNLENBQUE7SUFDTiwyRUFBa0IsQ0FBQTtJQUNsQix5REFBUyxDQUFBO0lBQ1QseURBQVMsQ0FBQTtJQUNULDJEQUFVLENBQUE7SUFDVixxREFBTyxDQUFBO0FBQ1QsQ0FBQyxFQVhXLFlBQVksS0FBWixZQUFZLFFBV3ZCO0FBRUQsTUFBTSxDQUFOLElBQVksWUFlWDtBQWZELFdBQVksWUFBWTtJQUN0Qiw2Q0FBRyxDQUFBO0lBQ0gsK0RBQVksQ0FBQTtJQUNaLHVEQUFRLENBQUE7SUFDUixxRUFBZSxDQUFBO0lBQ2YsbUdBQThCLENBQUE7SUFDOUIsMkVBQWtCLENBQUE7SUFDbEIsaUVBQWEsQ0FBQTtJQUNiLDZEQUFXLENBQUE7SUFDWCxpREFBSyxDQUFBO0lBQ0wsbUVBQWMsQ0FBQTtJQUNkLDhEQUFXLENBQUE7SUFDWCw4REFBVyxDQUFBO0lBQ1gsZ0ZBQW9CLENBQUE7SUFDcEIsOEVBQW1CLENBQUE7QUFDckIsQ0FBQyxFQWZXLFlBQVksS0FBWixZQUFZLFFBZXZCO0FBRUQ7SUFDRSx3QkFBbUIsS0FBaUIsRUFBUyxLQUFlO1FBQXpDLFVBQUssR0FBTCxLQUFLLENBQVk7UUFBUyxVQUFLLEdBQUwsS0FBSyxDQUFVO0lBQUcsQ0FBQztJQUNsRSxxQkFBQztBQUFELENBQUMsQUFGRCxJQUVDOztBQUVELE1BQU0s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QUFPLEtBQUssQ0FBQztLQUNoQjtBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cblxuaW1wb3J0ICogYXMgY2hhcnMgZnJvbSAnLi4vY2hhcnMnO1xuXG5leHBvcnQgZW51bSBDc3NUb2tlblR5cGUge1xuICBFT0YsXG4gIFN0cmluZyxcbiAgQ29tbWVudCxcbiAgSWRlbnRpZmllcixcbiAgTnVtYmVyLFxuICBJZGVudGlmaWVyT3JOdW1iZXIsXG4gIEF0S2V5d29yZCxcbiAgQ2hhcmFjdGVyLFxuICBXaGl0ZXNwYWNlLFxuICBJbnZhbGlkXG59XG5cbmV4cG9ydCBlbnVtIENzc0xleGVyTW9kZSB7XG4gIEFMTCxcbiAgQUxMX1RSQUNLX1dTLFxuICBTRUxFQ1RPUixcbiAgUFNFVURPX1NFTEVDVE9SLFxuICBQU0VVRE9fU0VMRUNUT1JfV0lUSF9BUkdVTUVOVFMsXG4gIEFUVFJJQlVURV9TRUxFQ1RPUixcbiAgQVRfUlVMRV9RVUVSWSxcbiAgTUVESUFfUVVFUlksXG4gIEJMT0NLLFxuICBLRVlGUkFNRV9CTE9DSyxcbiAgU1RZTEVfQkxPQ0ssXG4gIFNUWUxFX1ZBTFVFLFxuICBTVFlMRV9WQUxVRV9GVU5DVElPTixcbiAgU1RZTEVfQ0FMQ19GVU5DVElPTlxufVxuXG5leHBvcnQgY2xhc3MgTGV4ZWRDc3NSZXN1bHQge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgZXJyb3I6IEVycm9yfG51bGwsIHB1YmxpYyB0b2tlbjogQ3NzVG9rZW4pIHt9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZW5lcmF0ZUVycm9yTWVzc2FnZShcbiAgICBpbnB1dDogc3RyaW5nLCBtZXNzYWdlOiBzdHJpbmcsIGVycm9yVmFsdWU6IHN0cmluZywgaW5kZXg6IG51bWJlciwgcm93OiBudW1iZXIsXG4gICAgY29sdW1uOiBudW1iZXIpOiBzdHJpbmcge1xuICByZXR1cm4gYCR7bWVzc2FnZX0gYXQgY29sdW1uICR7cm93fToke2NvbHVtbn0gaW4gZXhwcmVzc2lvbiBbYCArXG4gICAgICBmaW5kUHJvYmxlbUNvZGUoaW5wdXQsIGVycm9yVmFsdWUsIGluZGV4LCBjb2x1bW4pICsgJ10nO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZmluZFByb2JsZW1Db2RlKFxuICAgIGlucHV0OiBzdHJpbmcsIGVycm9yVmFsdWU6IHN0cmluZywgaW5kZXg6IG51bWJlciwgY29sdW1uOiBudW1iZXIpOiBzdHJpbmcge1xuICBsZXQgZW5kT2ZQcm9ibGVtTGluZSA9IGluZGV4O1xuICBsZXQgY3VycmVudCA9IGNoYXJDb2RlKGlucHV0LCBpbmRleCk7XG4gIHdoaWxlIChjdXJyZW50ID4gMCAmJiAhaXNOZXdsaW5lKGN1cnJlbnQpKSB7XG4gICAgY3VycmVudCA9IGNoYXJDb2RlKGlucHV0LCArK2VuZE9mUHJvYmxlbUxpbmUpO1xuICB9XG4gIGNvbnN0IGNob3BwZWRTdHJpbmcgPSBpbnB1dC5zdWJzdHJpbmcoMCwgZW5kT2ZQcm9ibGVtTGluZSk7XG4gIGxldCBwb2ludGVyUGFkZGluZyA9ICcnO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNvbHVtbjsgaSsrKSB7XG4gICAgcG9pbnRlclBhZGRpbmcgKz0gJyAnO1xuICB9XG4gIGxldCBwb2ludGVyU3RyaW5nID0gJyc7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgZXJyb3JWYWx1ZS5sZW5ndGg7IGkrKykge1xuICAgIHBvaW50ZXJTdHJpbmcgKz0gJ14nO1xuICB9XG4gIHJldHVybiBjaG9wcGVkU3RyaW5nICsgJ1xcbicgKyBwb2ludGVyUGFkZGluZyArIHBvaW50ZXJTdHJpbmcgKyAnXFxuJztcbn1cblxuZXhwb3J0IGNsYXNzIENzc1Rva2VuIHtcbiAgbnVtVmFsdWU6IG51bWJlcjtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgaW5kZXg6IG51bWJlciwgcHVibGljIGNvbHVtbjogbnVtYmVyLCBwdWJsaWMgbGluZTogbnVtYmVyLCBwdWJsaWMgdHlwZTogQ3NzVG9rZW5UeXBlLFxuICAgICAgcHVibGljIHN0clZhbHVlOiBzdHJpbmcpIHtcbiAgICB0aGlzLm51bVZhbHVlID0gY2hhckNvZGUoc3RyVmFsdWUsIDApO1xuICB9XG59XG5cbmV4cG9ydCBjbGFzcyBDc3NMZXhlciB7XG4gIHNjYW4odGV4dDogc3RyaW5nLCB0cmFja0NvbW1lbnRzOiBib29sZWFuID0gZmFsc2UpOiBDc3NTY2FubmVyIHtcbiAgICByZXR1cm4gbmV3IENzc1NjYW5uZXIodGV4dCwgdHJhY2tDb21tZW50cyk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNzc1NjYW5uZXJFcnJvcih0b2tlbjogQ3NzVG9rZW4sIG1lc3NhZ2U6IHN0cmluZyk6IEVycm9yIHtcbiAgY29uc3QgZXJyb3IgPSBFcnJvcignQ3NzUGFyc2VFcnJvcjogJyArIG1lc3NhZ2UpO1xuICAoZXJyb3IgYXMgYW55KVtFUlJPUl9SQVdfTUVTU0FHRV0gPSBtZXNzYWdlO1xuICAoZXJyb3IgYXMgYW55KVtFUlJPUl9UT0tFTl0gPSB0b2tlbjtcbiAgcmV0dXJuIGVycm9yO1xufVxuXG5jb25zdCBFUlJPUl9UT0tFTiA9ICduZ1Rva2VuJztcbmNvbnN0IEVSUk9SX1JBV19NRVNTQUdFID0gJ25nUmF3TWVzc2FnZSc7XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSYXdNZXNzYWdlKGVycm9yOiBFcnJvcik6IHN0cmluZyB7XG4gIHJldHVybiAoZXJyb3IgYXMgYW55KVtFUlJPUl9SQVdfTUVTU0FHRV07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRUb2tlbihlcnJvcjogRXJyb3IpOiBDc3NUb2tlbiB7XG4gIHJldHVybiAoZXJyb3IgYXMgYW55KVtFUlJPUl9UT0tFTl07XG59XG5cbmZ1bmN0aW9uIF90cmFja1doaXRlc3BhY2UobW9kZTogQ3NzTGV4ZXJNb2RlKSB7XG4gIHN3aXRjaCAobW9kZSkge1xuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLlNFTEVDVE9SOlxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLlBTRVVET19TRUxFQ1RPUjpcbiAgICBjYXNlIENzc0xleGVyTW9kZS5BTExfVFJBQ0tfV1M6XG4gICAgY2FzZSBDc3NMZXhlck1vZGUuU1RZTEVfVkFMVUU6XG4gICAgICByZXR1cm4gdHJ1ZTtcblxuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZXhwb3J0IGNsYXNzIENzc1NjYW5uZXIge1xuICAvLyBUT0RPKGlzc3VlLzI0NTcxKTogcmVtb3ZlICchJy5cbiAgcGVlayAhOiBudW1iZXI7XG4gIHBlZWtQZWVrOiBudW1iZXI7XG4gIGxlbmd0aDogbnVtYmVyID0gMDtcbiAgaW5kZXg6IG51bWJlciA9IC0xO1xuICBjb2x1bW46IG51bWJlciA9IC0xO1xuICBsaW5lOiBudW1iZXIgPSAwO1xuXG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2N1cnJlbnRNb2RlOiBDc3NMZXhlck1vZGUgPSBDc3NMZXhlck1vZGUuQkxPQ0s7XG4gIC8qKiBAaW50ZXJuYWwgKi9cbiAgX2N1cnJlbnRFcnJvcjogRXJyb3J8bnVsbCA9IG51bGw7XG5cbiAgY29uc3RydWN0b3IocHVibGljIGlucHV0OiBzdHJpbmcsIHByaXZhdGUgX3RyYWNrQ29tbWVudHM6IGJvb2xlYW4gPSBmYWxzZSkge1xuICAgIHRoaXMubGVuZ3RoID0gdGhpcy5pbnB1dC5sZW5ndGg7XG4gICAgdGhpcy5wZWVrUGVlayA9IHRoaXMucGVla0F0KDApO1xuICAgIHRoaXMuYWR2YW5jZSgpO1xuICB9XG5cbiAgZ2V0TW9kZSgpOiBDc3NMZXhlck1vZGUgeyByZXR1cm4gdGhpcy5fY3VycmVudE1vZGU7IH1cblxuICBzZXRNb2RlKG1vZGU6IENzc0xleGVyTW9kZSkge1xuICAgIGlmICh0aGlzLl9jdXJyZW50TW9kZSAhPSBtb2RlKSB7XG4gICAgICBpZiAoX3RyYWNrV2hpdGVzcGFjZSh0aGlzLl9jdXJyZW50TW9kZSkgJiYgIV90cmFja1doaXRlc3BhY2UobW9kZSkpIHtcbiAgICAgICAgdGhpcy5jb25zdW1lV2hpdGVzcGFjZSgpO1xuICAgICAgfVxuICAgICAgdGhpcy5fY3VycmVudE1vZGUgPSBtb2RlO1xuICAgIH1cbiAgfVxuXG4gIGFkdmFuY2UoKTogdm9pZCB7XG4gICAgaWYgKGlzTmV3bGluZSh0aGlzLnBlZWspKSB7XG4gICAgICB0aGlzLmNvbHVtbiA9IDA7XG4gICAgICB0aGlzLmxpbmUrKztcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5jb2x1bW4rKztcbiAgICB9XG5cbiAgICB0aGlzLmluZGV4Kys7XG4gICAgdGhpcy5wZWVrID0gdGhpcy5wZWVrUGVlaztcbiAgICB0aGlzLnBlZWtQZWVrID0gdGhpcy5wZWVrQXQodGhpcy5pbmRleCArIDEpO1xuICB9XG5cbiAgcGVla0F0KGluZGV4OiBudW1iZXIpOiBudW1iZXIge1xuICAgIHJldHVybiBpbmRleCA+PSB0aGlzLmxlbmd0aCA/IGNoYXJzLiRFT0YgOiB0aGlzLmlucHV0LmNoYXJDb2RlQXQoaW5kZXgpO1xuICB9XG5cbiAgY29uc3VtZUVtcHR5U3RhdGVtZW50cygpOiB2b2lkIHtcbiAgICB0aGlzLmNvbnN1bWVXaGl0ZXNwYWNlKCk7XG4gICAgd2hpbGUgKHRoaXMucGVlayA9PSBjaGFycy4kU0VNSUNPTE9OKSB7XG4gICAgICB0aGlzLmFkdmFuY2UoKTtcbiAgICAgIHRoaXMuY29uc3VtZVdoaXRlc3BhY2UoKTtcbiAgICB9XG4gIH1cblxuICBjb25zdW1lV2hpdGVzcGFjZSgpOiB2b2lkIHtcbiAgICB3aGlsZSAoY2hhcnMuaXNXaGl0ZXNwYWNlKHRoaXMucGVlaykgfHwgaXNOZXdsaW5lKHRoaXMucGVlaykpIHtcbiAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgICAgaWYgKCF0aGlzLl90cmFja0NvbW1lbnRzICYmIGlzQ29tbWVudFN0YXJ0KHRoaXMucGVlaywgdGhpcy5wZWVrUGVlaykpIHtcbiAgICAgICAgdGhpcy5hZHZhbmNlKCk7ICAvLyAvXG4gICAgICAgIHRoaXMuYWR2YW5jZSgpOyAgLy8gKlxuICAgICAgICB3aGlsZSAoIWlzQ29tbWVudEVuZCh0aGlzLnBlZWssIHRoaXMucGVla1BlZWspKSB7XG4gICAgICAgICAgaWYgKHRoaXMucGVlayA9PSBjaGFycy4kRU9GKSB7XG4gICAgICAgICAgICB0aGlzLmVycm9yKCdVbnRlcm1pbmF0ZWQgY29tbWVudCcpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB0aGlzLmFkdmFuY2UoKTtcbiAgICAgICAgfVxuICAgICAgICB0aGlzLmFkdmFuY2UoKTsgIC8vICpcbiAgICAgICAgdGhpcy5hZHZhbmNlKCk7ICAvLyAvXG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgY29uc3VtZSh0eXBlOiBDc3NUb2tlblR5cGUsIHZhbHVlOiBzdHJpbmd8bnVsbCA9IG51bGwpOiBMZXhlZENzc1Jlc3VsdCB7XG4gICAgY29uc3QgbW9kZSA9IHRoaXMuX2N1cnJlbnRNb2RlO1xuXG4gICAgdGhpcy5zZXRNb2RlKF90cmFja1doaXRlc3BhY2UobW9kZSkgPyBDc3NMZXhlck1vZGUuQUxMX1RSQUNLX1dTIDogQ3NzTGV4ZXJNb2RlLkFMTCk7XG5cbiAgICBjb25zdCBwcmV2aW91c0luZGV4ID0gdGhpcy5pbmRleDtcbiAgICBjb25zdCBwcmV2aW91c0xpbmUgPSB0aGlzLmxpbmU7XG4gICAgY29uc3QgcHJldmlvdXNDb2x1bW4gPSB0aGlzLmNvbHVtbjtcblxuICAgIGxldCBuZXh0OiBDc3NUb2tlbiA9IHVuZGVmaW5lZCAhO1xuICAgIGNvbnN0IG91dHB1dCA9IHRoaXMuc2NhbigpO1xuICAgIGlmIChvdXRwdXQgIT0gbnVsbCkge1xuICAgICAgLy8ganVzdCBpbmNhc2UgdGhlIGlubmVyIHNjYW4gbWV0aG9kIHJldHVybmVkIGFuIGVycm9yXG4gICAgICBpZiAob3V0cHV0LmVycm9yICE9IG51bGwpIHtcbiAgICAgICAgdGhpcy5zZXRNb2RlKG1vZGUpO1xuICAgICAgICByZXR1cm4gb3V0cHV0O1xuICAgICAgfVxuXG4gICAgICBuZXh0ID0gb3V0cHV0LnRva2VuO1xuICAgIH1cblxuICAgIGlmIChuZXh0ID09IG51bGwpIHtcbiAgICAgIG5leHQgPSBuZXcgQ3NzVG9rZW4odGhpcy5pbmRleCwgdGhpcy5jb2x1bW4sIHRoaXMubGluZSwgQ3NzVG9rZW5UeXBlLkVPRiwgJ2VuZCBvZiBmaWxlJyk7XG4gICAgfVxuXG4gICAgbGV0IGlzTWF0Y2hpbmdUeXBlOiBib29sZWFuID0gZmFsc2U7XG4gICAgaWYgKHR5cGUgPT0gQ3NzVG9rZW5UeXBlLklkZW50aWZpZXJPck51bWJlcikge1xuICAgICAgLy8gVE9ETyAobWF0c2tvKTogaW1wbGVtZW50IGFycmF5IHRyYXZlcnNhbCBmb3IgbG9va3VwIGhlcmVcbiAgICAgIGlzTWF0Y2hpbmdUeXBlID0gbmV4dC50eXBlID09IENzc1Rva2VuVHlwZS5OdW1iZXIgfHwgbmV4dC50eXBlID09IENzc1Rva2VuVHlwZS5JZGVudGlmaWVyO1xuICAgIH0gZWxzZSB7XG4gICAgICBpc01hdGNoaW5nVHlwZSA9IG5leHQudHlwZSA9PSB0eXBlO1xuICAgIH1cblxuICAgIC8vIGJlZm9yZSB0aHJvd2luZyB0aGUgZXJyb3Igd2UgbmVlZCB0byBicmluZyBiYWNrIHRoZSBmb3JtZXJcbiAgICAvLyBtb2RlIHNvIHRoYXQgdGhlIHBhcnNlciBjYW4gcmVjb3Zlci4uLlxuICAgIHRoaXMuc2V0TW9kZShtb2RlKTtcblxuICAgIGxldCBlcnJvcjogRXJyb3J8bnVsbCA9IG51bGw7XG4gICAgaWYgKCFpc01hdGNoaW5nVHlwZSB8fCAodmFsdWUgIT0gbnVsbCAmJiB2YWx1ZSAhPSBuZXh0LnN0clZhbHVlKSkge1xuICAgICAgbGV0IGVycm9yTWVzc2FnZSA9XG4gICAgICAgICAgQ3NzVG9rZW5UeXBlW25leHQudHlwZV0gKyAnIGRvZXMgbm90IG1hdGNoIGV4cGVjdGVkICcgKyBDc3NUb2tlblR5cGVbdHlwZV0gKyAnIHZhbHVlJztcblxuICAgICAgaWYgKHZhbHVlICE9IG51bGwpIHtcbiAgICAgICAgZXJyb3JNZXNzYWdlICs9ICcgKFwiJyArIG5leHQuc3RyVmFsdWUgKyAnXCIgc2hvdWxkIG1hdGNoIFwiJyArIHZhbHVlICsgJ1wiKSc7XG4gICAgICB9XG5cbiAgICAgIGVycm9yID0gY3NzU2Nhbm5lckVycm9yKFxuICAgICAgICAgIG5leHQsIGdlbmVyYXRlRXJyb3JNZXNzYWdlKFxuICAgICAgICAgICAgICAgICAgICB0aGlzLmlucHV0LCBlcnJvck1lc3NhZ2UsIG5leHQuc3RyVmFsdWUsIHByZXZpb3VzSW5kZXgsIHByZXZpb3VzTGluZSxcbiAgICAgICAgICAgICAgICAgICAgcHJldmlvdXNDb2x1bW4pKTtcbiAgICB9XG5cbiAgICByZXR1cm4gbmV3IExleGVkQ3NzUmVzdWx0KGVycm9yLCBuZXh0KTtcbiAgfVxuXG5cbiAgc2NhbigpOiBMZXhlZENzc1Jlc3VsdHxudWxsIHtcbiAgICBjb25zdCB0cmFja1dTID0gX3RyYWNrV2hpdGVzcGFjZSh0aGlzLl9jdXJyZW50TW9kZSk7XG4gICAgaWYgKHRoaXMuaW5kZXggPT0gMCAmJiAhdHJhY2tXUykgeyAgLy8gZmlyc3Qgc2NhblxuICAgICAgdGhpcy5jb25zdW1lV2hpdGVzcGFjZSgpO1xuICAgIH1cblxuICAgIGNvbnN0IHRva2VuID0gdGhpcy5fc2NhbigpO1xuICAgIGlmICh0b2tlbiA9PSBudWxsKSByZXR1cm4gbnVsbDtcblxuICAgIGNvbnN0IGVycm9yID0gdGhpcy5fY3VycmVudEVycm9yICE7XG4gICAgdGhpcy5fY3VycmVudEVycm9yID0gbnVsbDtcblxuICAgIGlmICghdHJhY2tXUykge1xuICAgICAgdGhpcy5jb25zdW1lV2hpdGVzcGFjZSgpO1xuICAgIH1cbiAgICByZXR1cm4gbmV3IExleGVkQ3NzUmVzdWx0KGVycm9yLCB0b2tlbik7XG4gIH1cblxuICAvKiogQGludGVybmFsICovXG4gIF9zY2FuKCk6IENzc1Rva2VufG51bGwge1xuICAgIGxldCBwZWVrID0gdGhpcy5wZWVrO1xuICAgIGxldCBwZWVrUGVlayA9IHRoaXMucGVla1BlZWs7XG4gICAgaWYgKHBlZWsgPT0gY2hhcnMuJEVPRikgcmV0dXJuIG51bGw7XG5cbiAgICBpZiAoaXNDb21tZW50U3RhcnQocGVlaywgcGVla1BlZWspKSB7XG4gICAgICAvLyBldmVuIGlmIGNvbW1lbnRzIGFyZSBub3QgdHJhY2tlZCB3ZSBzdGlsbCBsZXggdGhlXG4gICAgICAvLyBjb21tZW50IHNvIHdlIGNhbiBtb3ZlIHRoZSBwb2ludGVyIGZvcndhcmRcbiAgICAgIGNvbnN0IGNvbW1lbnRUb2tlbiA9IHRoaXMuc2NhbkNvbW1lbnQoKTtcbiAgICAgIGlmICh0aGlzLl90cmFja0NvbW1lbnRzKSB7XG4gICAgICAgIHJldHVybiBjb21tZW50VG9rZW47XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKF90cmFja1doaXRlc3BhY2UodGhpcy5fY3VycmVudE1vZGUpICYmIChjaGFycy5pc1doaXRlc3BhY2UocGVlaykgfHwgaXNOZXdsaW5lKHBlZWspKSkge1xuICAgICAgcmV0dXJuIHRoaXMuc2NhbldoaXRlc3BhY2UoKTtcbiAgICB9XG5cbiAgICBwZWVrID0gdGhpcy5wZWVrO1xuICAgIHBlZWtQZWVrID0gdGhpcy5wZWVrUGVlaztcbiAgICBpZiAocGVlayA9PSBjaGFycy4kRU9GKSByZXR1cm4gbnVsbDtcblxuICAgIGlmIChpc1N0cmluZ1N0YXJ0KHBlZWssIHBlZWtQZWVrKSkge1xuICAgICAgcmV0dXJuIHRoaXMuc2NhblN0cmluZygpO1xuICAgIH1cblxuICAgIC8vIHNvbWV0aGluZyBsaWtlIHVybChjb29sKVxuICAgIGlmICh0aGlzLl9jdXJyZW50TW9kZSA9PSBDc3NMZXhlck1vZGUuU1RZTEVfVkFMVUVfRlVOQ1RJT04pIHtcbiAgICAgIHJldHVybiB0aGlzLnNjYW5Dc3NWYWx1ZUZ1bmN0aW9uKCk7XG4gICAgfVxuXG4gICAgY29uc3QgaXNNb2RpZmllciA9IHBlZWsgPT0gY2hhcnMuJFBMVVMgfHwgcGVlayA9PSBjaGFycy4kTUlOVVM7XG4gICAgY29uc3QgZGlnaXRBID0gaXNNb2RpZmllciA/IGZhbHNlIDogY2hhcnMuaXNEaWdpdChwZWVrKTtcbiAgICBjb25zdCBkaWdpdEIgPSBjaGFycy5pc0RpZ2l0KHBlZWtQZWVrKTtcbiAgICBpZiAoZGlnaXRBIHx8IChpc01vZGlmaWVyICYmIChwZWVrUGVlayA9PSBjaGFycy4kUEVSSU9EIHx8IGRpZ2l0QikpIHx8XG4gICAgICAgIChwZWVrID09IGNoYXJzLiRQRVJJT0QgJiYgZGlnaXRCKSkge1xuICAgICAgcmV0dXJuIHRoaXMuc2Nhbk51bWJlcigpO1xuICAgIH1cblxuICAgIGlmIChwZWVrID09IGNoYXJzLiRBVCkge1xuICAgICAgcmV0dXJuIHRoaXMuc2NhbkF0RXhwcmVzc2lvbigpO1xuICAgIH1cblxuICAgIGlmIChpc0lkZW50aWZpZXJTdGFydChwZWVrLCBwZWVrUGVlaykpIHtcbiAgICAgIHJldHVybiB0aGlzLnNjYW5JZGVudGlmaWVyKCk7XG4gICAgfVxuXG4gICAgaWYgKGlzVmFsaWRDc3NDaGFyYWN0ZXIocGVlaywgdGhpcy5fY3VycmVudE1vZGUpKSB7XG4gICAgICByZXR1cm4gdGhpcy5zY2FuQ2hhcmFjdGVyKCk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMuZXJyb3IoYFVuZXhwZWN0ZWQgY2hhcmFjdGVyIFske1N0cmluZy5mcm9tQ2hhckNvZGUocGVlayl9XWApO1xuICB9XG5cbiAgc2NhbkNvbW1lbnQoKTogQ3NzVG9rZW58bnVsbCB7XG4gICAgaWYgKHRoaXMuYXNzZXJ0Q29uZGl0aW9uKFxuICAgICAgICAgICAgaXNDb21tZW50U3RhcnQodGhpcy5wZWVrLCB0aGlzLnBlZWtQZWVrKSwgJ0V4cGVjdGVkIGNvbW1lbnQgc3RhcnQgdmFsdWUnKSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgY29uc3Qgc3RhcnRpbmdMaW5lID0gdGhpcy5saW5lO1xuXG4gICAgdGhpcy5hZHZhbmNlKCk7ICAvLyAvXG4gICAgdGhpcy5hZHZhbmNlKCk7ICAvLyAqXG5cbiAgICB3aGlsZSAoIWlzQ29tbWVudEVuZCh0aGlzLnBlZWssIHRoaXMucGVla1BlZWspKSB7XG4gICAgICBpZiAodGhpcy5wZWVrID09IGNoYXJzLiRFT0YpIHtcbiAgICAgICAgdGhpcy5lcnJvcignVW50ZXJtaW5hdGVkIGNvbW1lbnQnKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgIH1cblxuICAgIHRoaXMuYWR2YW5jZSgpOyAgLy8gKlxuICAgIHRoaXMuYWR2YW5jZSgpOyAgLy8gL1xuXG4gICAgY29uc3Qgc3RyID0gdGhpcy5pbnB1dC5zdWJzdHJpbmcoc3RhcnQsIHRoaXMuaW5kZXgpO1xuICAgIHJldHVybiBuZXcgQ3NzVG9rZW4oc3RhcnQsIHN0YXJ0aW5nQ29sdW1uLCBzdGFydGluZ0xpbmUsIENzc1Rva2VuVHlwZS5Db21tZW50LCBzdHIpO1xuICB9XG5cbiAgc2NhbldoaXRlc3BhY2UoKTogQ3NzVG9rZW4ge1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5pbmRleDtcbiAgICBjb25zdCBzdGFydGluZ0NvbHVtbiA9IHRoaXMuY29sdW1uO1xuICAgIGNvbnN0IHN0YXJ0aW5nTGluZSA9IHRoaXMubGluZTtcbiAgICB3aGlsZSAoY2hhcnMuaXNXaGl0ZXNwYWNlKHRoaXMucGVlaykgJiYgdGhpcy5wZWVrICE9IGNoYXJzLiRFT0YpIHtcbiAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgIH1cbiAgICBjb25zdCBzdHIgPSB0aGlzLmlucHV0LnN1YnN0cmluZyhzdGFydCwgdGhpcy5pbmRleCk7XG4gICAgcmV0dXJuIG5ldyBDc3NUb2tlbihzdGFydCwgc3RhcnRpbmdDb2x1bW4sIHN0YXJ0aW5nTGluZSwgQ3NzVG9rZW5UeXBlLldoaXRlc3BhY2UsIHN0cik7XG4gIH1cblxuICBzY2FuU3RyaW5nKCk6IENzc1Rva2VufG51bGwge1xuICAgIGlmICh0aGlzLmFzc2VydENvbmRpdGlvbihcbiAgICAgICAgICAgIGlzU3RyaW5nU3RhcnQodGhpcy5wZWVrLCB0aGlzLnBlZWtQZWVrKSwgJ1VuZXhwZWN0ZWQgbm9uLXN0cmluZyBzdGFydGluZyB2YWx1ZScpKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG5cbiAgICBjb25zdCB0YXJnZXQgPSB0aGlzLnBlZWs7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgY29uc3Qgc3RhcnRpbmdMaW5lID0gdGhpcy5saW5lO1xuICAgIGxldCBwcmV2aW91cyA9IHRhcmdldDtcbiAgICB0aGlzLmFkdmFuY2UoKTtcblxuICAgIHdoaWxlICghaXNDaGFyTWF0Y2godGFyZ2V0LCBwcmV2aW91cywgdGhpcy5wZWVrKSkge1xuICAgICAgaWYgKHRoaXMucGVlayA9PSBjaGFycy4kRU9GIHx8IGlzTmV3bGluZSh0aGlzLnBlZWspKSB7XG4gICAgICAgIHRoaXMuZXJyb3IoJ1VudGVybWluYXRlZCBxdW90ZScpO1xuICAgICAgfVxuICAgICAgcHJldmlvdXMgPSB0aGlzLnBlZWs7XG4gICAgICB0aGlzLmFkdmFuY2UoKTtcbiAgICB9XG5cbiAgICBpZiAodGhpcy5hc3NlcnRDb25kaXRpb24odGhpcy5wZWVrID09IHRhcmdldCwgJ1VudGVybWluYXRlZCBxdW90ZScpKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgdGhpcy5hZHZhbmNlKCk7XG5cbiAgICBjb25zdCBzdHIgPSB0aGlzLmlucHV0LnN1YnN0cmluZyhzdGFydCwgdGhpcy5pbmRleCk7XG4gICAgcmV0dXJuIG5ldyBDc3NUb2tlbihzdGFydCwgc3RhcnRpbmdDb2x1bW4sIHN0YXJ0aW5nTGluZSwgQ3NzVG9rZW5UeXBlLlN0cmluZywgc3RyKTtcbiAgfVxuXG4gIHNjYW5OdW1iZXIoKTogQ3NzVG9rZW4ge1xuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5pbmRleDtcbiAgICBjb25zdCBzdGFydGluZ0NvbHVtbiA9IHRoaXMuY29sdW1uO1xuICAgIGlmICh0aGlzLnBlZWsgPT0gY2hhcnMuJFBMVVMgfHwgdGhpcy5wZWVrID09IGNoYXJzLiRNSU5VUykge1xuICAgICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgfVxuICAgIGxldCBwZXJpb2RVc2VkID0gZmFsc2U7XG4gICAgd2hpbGUgKGNoYXJzLmlzRGlnaXQodGhpcy5wZWVrKSB8fCB0aGlzLnBlZWsgPT0gY2hhcnMuJFBFUklPRCkge1xuICAgICAgaWYgKHRoaXMucGVlayA9PSBjaGFycy4kUEVSSU9EKSB7XG4gICAgICAgIGlmIChwZXJpb2RVc2VkKSB7XG4gICAgICAgICAgdGhpcy5lcnJvcignVW5leHBlY3RlZCB1c2Ugb2YgYSBzZWNvbmQgcGVyaW9kIHZhbHVlJyk7XG4gICAgICAgIH1cbiAgICAgICAgcGVyaW9kVXNlZCA9IHRydWU7XG4gICAgICB9XG4gICAgICB0aGlzLmFkdmFuY2UoKTtcbiAgICB9XG4gICAgY29uc3Qgc3RyVmFsdWUgPSB0aGlzLmlucHV0LnN1YnN0cmluZyhzdGFydCwgdGhpcy5pbmRleCk7XG4gICAgcmV0dXJuIG5ldyBDc3NUb2tlbihzdGFydCwgc3RhcnRpbmdDb2x1bW4sIHRoaXMubGluZSwgQ3NzVG9rZW5UeXBlLk51bWJlciwgc3RyVmFsdWUpO1xuICB9XG5cbiAgc2NhbklkZW50aWZpZXIoKTogQ3NzVG9rZW58bnVsbCB7XG4gICAgaWYgKHRoaXMuYXNzZXJ0Q29uZGl0aW9uKFxuICAgICAgICAgICAgaXNJZGVudGlmaWVyU3RhcnQodGhpcy5wZWVrLCB0aGlzLnBlZWtQZWVrKSwgJ0V4cGVjdGVkIGlkZW50aWZpZXIgc3RhcnRpbmcgdmFsdWUnKSkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgd2hpbGUgKGlzSWRlbnRpZmllclBhcnQodGhpcy5wZWVrKSkge1xuICAgICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgfVxuICAgIGNvbnN0IHN0clZhbHVlID0gdGhpcy5pbnB1dC5zdWJzdHJpbmcoc3RhcnQsIHRoaXMuaW5kZXgpO1xuICAgIHJldHVybiBuZXcgQ3NzVG9rZW4oc3RhcnQsIHN0YXJ0aW5nQ29sdW1uLCB0aGlzLmxpbmUsIENzc1Rva2VuVHlwZS5JZGVudGlmaWVyLCBzdHJWYWx1ZSk7XG4gIH1cblxuICBzY2FuQ3NzVmFsdWVGdW5jdGlvbigpOiBDc3NUb2tlbiB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgbGV0IHBhcmVuQmFsYW5jZSA9IDE7XG4gICAgd2hpbGUgKHRoaXMucGVlayAhPSBjaGFycy4kRU9GICYmIHBhcmVuQmFsYW5jZSA+IDApIHtcbiAgICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgICAgaWYgKHRoaXMucGVlayA9PSBjaGFycy4kTFBBUkVOKSB7XG4gICAgICAgIHBhcmVuQmFsYW5jZSsrO1xuICAgICAgfSBlbHNlIGlmICh0aGlzLnBlZWsgPT0gY2hhcnMuJFJQQVJFTikge1xuICAgICAgICBwYXJlbkJhbGFuY2UtLTtcbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3Qgc3RyVmFsdWUgPSB0aGlzLmlucHV0LnN1YnN0cmluZyhzdGFydCwgdGhpcy5pbmRleCk7XG4gICAgcmV0dXJuIG5ldyBDc3NUb2tlbihzdGFydCwgc3RhcnRpbmdDb2x1bW4sIHRoaXMubGluZSwgQ3NzVG9rZW5UeXBlLklkZW50aWZpZXIsIHN0clZhbHVlKTtcbiAgfVxuXG4gIHNjYW5DaGFyYWN0ZXIoKTogQ3NzVG9rZW58bnVsbCB7XG4gICAgY29uc3Qgc3RhcnQgPSB0aGlzLmluZGV4O1xuICAgIGNvbnN0IHN0YXJ0aW5nQ29sdW1uID0gdGhpcy5jb2x1bW47XG4gICAgaWYgKHRoaXMuYXNzZXJ0Q29uZGl0aW9uKFxuICAgICAgICAgICAgaXNWYWxpZENzc0NoYXJhY3Rlcih0aGlzLnBlZWssIHRoaXMuX2N1cnJlbnRNb2RlKSxcbiAgICAgICAgICAgIGNoYXJTdHIodGhpcy5wZWVrKSArICcgaXMgbm90IGEgdmFsaWQgQ1NTIGNoYXJhY3RlcicpKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG5cbiAgICBjb25zdCBjID0gdGhpcy5pbnB1dC5zdWJzdHJpbmcoc3RhcnQsIHN0YXJ0ICsgMSk7XG4gICAgdGhpcy5hZHZhbmNlKCk7XG5cbiAgICByZXR1cm4gbmV3IENzc1Rva2VuKHN0YXJ0LCBzdGFydGluZ0NvbHVtbiwgdGhpcy5saW5lLCBDc3NUb2tlblR5cGUuQ2hhcmFjdGVyLCBjKTtcbiAgfVxuXG4gIHNjYW5BdEV4cHJlc3Npb24oKTogQ3NzVG9rZW58bnVsbCB7XG4gICAgaWYgKHRoaXMuYXNzZXJ0Q29uZGl0aW9uKHRoaXMucGVlayA9PSBjaGFycy4kQVQsICdFeHBlY3RlZCBAIHZhbHVlJykpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cblxuICAgIGNvbnN0IHN0YXJ0ID0gdGhpcy5pbmRleDtcbiAgICBjb25zdCBzdGFydGluZ0NvbHVtbiA9IHRoaXMuY29sdW1uO1xuICAgIHRoaXMuYWR2YW5jZSgpO1xuICAgIGlmIChpc0lkZW50aWZpZXJTdGFydCh0aGlzLnBlZWssIHRoaXMucGVla1BlZWspKSB7XG4gICAgICBjb25zdCBpZGVudCA9IHRoaXMuc2NhbklkZW50aWZpZXIoKSAhO1xuICAgICAgY29uc3Qgc3RyVmFsdWUgPSAnQCcgKyBpZGVudC5zdHJWYWx1ZTtcbiAgICAgIHJldHVybiBuZXcgQ3NzVG9rZW4oc3RhcnQsIHN0YXJ0aW5nQ29sdW1uLCB0aGlzLmxpbmUsIENzc1Rva2VuVHlwZS5BdEtleXdvcmQsIHN0clZhbHVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMuc2NhbkNoYXJhY3RlcigpO1xuICAgIH1cbiAgfVxuXG4gIGFzc2VydENvbmRpdGlvbihzdGF0dXM6IGJvb2xlYW4sIGVycm9yTWVzc2FnZTogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgaWYgKCFzdGF0dXMpIHtcbiAgICAgIHRoaXMuZXJyb3IoZXJyb3JNZXNzYWdlKTtcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIH1cbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBlcnJvcihtZXNzYWdlOiBzdHJpbmcsIGVycm9yVG9rZW5WYWx1ZTogc3RyaW5nfG51bGwgPSBudWxsLCBkb05vdEFkdmFuY2U6IGJvb2xlYW4gPSBmYWxzZSk6XG4gICAgICBDc3NUb2tlbiB7XG4gICAgY29uc3QgaW5kZXg6IG51bWJlciA9IHRoaXMuaW5kZXg7XG4gICAgY29uc3QgY29sdW1uOiBudW1iZXIgPSB0aGlzLmNvbHVtbjtcbiAgICBjb25zdCBsaW5lOiBudW1iZXIgPSB0aGlzLmxpbmU7XG4gICAgZXJyb3JUb2tlblZhbHVlID0gZXJyb3JUb2tlblZhbHVlIHx8IFN0cmluZy5mcm9tQ2hhckNvZGUodGhpcy5wZWVrKTtcbiAgICBjb25zdCBpbnZhbGlkVG9rZW4gPSBuZXcgQ3NzVG9rZW4oaW5kZXgsIGNvbHVtbiwgbGluZSwgQ3NzVG9rZW5UeXBlLkludmFsaWQsIGVycm9yVG9rZW5WYWx1ZSk7XG4gICAgY29uc3QgZXJyb3JNZXNzYWdlID1cbiAgICAgICAgZ2VuZXJhdGVFcnJvck1lc3NhZ2UodGhpcy5pbnB1dCwgbWVzc2FnZSwgZXJyb3JUb2tlblZhbHVlLCBpbmRleCwgbGluZSwgY29sdW1uKTtcbiAgICBpZiAoIWRvTm90QWR2YW5jZSkge1xuICAgICAgdGhpcy5hZHZhbmNlKCk7XG4gICAgfVxuICAgIHRoaXMuX2N1cnJlbnRFcnJvciA9IGNzc1NjYW5uZXJFcnJvcihpbnZhbGlkVG9rZW4sIGVycm9yTWVzc2FnZSk7XG4gICAgcmV0dXJuIGludmFsaWRUb2tlbjtcbiAgfVxufVxuXG5mdW5jdGlvbiBpc0NoYXJNYXRjaCh0YXJnZXQ6IG51bWJlciwgcHJldmlvdXM6IG51bWJlciwgY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHJldHVybiBjb2RlID09IHRhcmdldCAmJiBwcmV2aW91cyAhPSBjaGFycy4kQkFDS1NMQVNIO1xufVxuXG5mdW5jdGlvbiBpc0NvbW1lbnRTdGFydChjb2RlOiBudW1iZXIsIG5leHQ6IG51bWJlcik6IGJvb2xlYW4ge1xuICByZXR1cm4gY29kZSA9PSBjaGFycy4kU0xBU0ggJiYgbmV4dCA9PSBjaGFycy4kU1RBUjtcbn1cblxuZnVuY3Rpb24gaXNDb21tZW50RW5kKGNvZGU6IG51bWJlciwgbmV4dDogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHJldHVybiBjb2RlID09IGNoYXJzLiRTVEFSICYmIG5leHQgPT0gY2hhcnMuJFNMQVNIO1xufVxuXG5mdW5jdGlvbiBpc1N0cmluZ1N0YXJ0KGNvZGU6IG51bWJlciwgbmV4dDogbnVtYmVyKTogYm9vbGVhbiB7XG4gIGxldCB0YXJnZXQgPSBjb2RlO1xuICBpZiAodGFyZ2V0ID09IGNoYXJzLiRCQUNLU0xBU0gpIHtcbiAgICB0YXJnZXQgPSBuZXh0O1xuICB9XG4gIHJldHVybiB0YXJnZXQgPT0gY2hhcnMuJERRIHx8IHRhcmdldCA9PSBjaGFycy4kU1E7XG59XG5cbmZ1bmN0aW9uIGlzSWRlbnRpZmllclN0YXJ0KGNvZGU6IG51bWJlciwgbmV4dDogbnVtYmVyKTogYm9vbGVhbiB7XG4gIGxldCB0YXJnZXQgPSBjb2RlO1xuICBpZiAodGFyZ2V0ID09IGNoYXJzLiRNSU5VUykge1xuICAgIHRhcmdldCA9IG5leHQ7XG4gIH1cblxuICByZXR1cm4gY2hhcnMuaXNBc2NpaUxldHRlcih0YXJnZXQpIHx8IHRhcmdldCA9PSBjaGFycy4kQkFDS1NMQVNIIHx8IHRhcmdldCA9PSBjaGFycy4kTUlOVVMgfHxcbiAgICAgIHRhcmdldCA9PSBjaGFycy4kXztcbn1cblxuZnVuY3Rpb24gaXNJZGVudGlmaWVyUGFydCh0YXJnZXQ6IG51bWJlcik6IGJvb2xlYW4ge1xuICByZXR1cm4gY2hhcnMuaXNBc2NpaUxldHRlcih0YXJnZXQpIHx8IHRhcmdldCA9PSBjaGFycy4kQkFDS1NMQVNIIHx8IHRhcmdldCA9PSBjaGFycy4kTUlOVVMgfHxcbiAgICAgIHRhcmdldCA9PSBjaGFycy4kXyB8fCBjaGFycy5pc0RpZ2l0KHRhcmdldCk7XG59XG5cbmZ1bmN0aW9uIGlzVmFsaWRQc2V1ZG9TZWxlY3RvckNoYXJhY3Rlcihjb2RlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kTFBBUkVOOlxuICAgIGNhc2UgY2hhcnMuJFJQQVJFTjpcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNWYWxpZEtleWZyYW1lQmxvY2tDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHJldHVybiBjb2RlID09IGNoYXJzLiRQRVJDRU5UO1xufVxuXG5mdW5jdGlvbiBpc1ZhbGlkQXR0cmlidXRlU2VsZWN0b3JDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIC8vIHZhbHVlXip8JH49c29tZXRoaW5nXG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJCQ6XG4gICAgY2FzZSBjaGFycy4kUElQRTpcbiAgICBjYXNlIGNoYXJzLiRDQVJFVDpcbiAgICBjYXNlIGNoYXJzLiRUSUxEQTpcbiAgICBjYXNlIGNoYXJzLiRTVEFSOlxuICAgIGNhc2UgY2hhcnMuJEVROlxuICAgICAgcmV0dXJuIHRydWU7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5mdW5jdGlvbiBpc1ZhbGlkU2VsZWN0b3JDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIC8vIHNlbGVjdG9yIFsga2V5ICAgPSB2YWx1ZSBdXG4gIC8vIElERU5UICAgIEMgSURFTlQgQyBJREVOVCBDXG4gIC8vICNpZCwgLmNsYXNzLCAqK34+XG4gIC8vIHRhZzpQU0VVRE9cbiAgc3dpdGNoIChjb2RlKSB7XG4gICAgY2FzZSBjaGFycy4kSEFTSDpcbiAgICBjYXNlIGNoYXJzLiRQRVJJT0Q6XG4gICAgY2FzZSBjaGFycy4kVElMREE6XG4gICAgY2FzZSBjaGFycy4kU1RBUjpcbiAgICBjYXNlIGNoYXJzLiRQTFVTOlxuICAgIGNhc2UgY2hhcnMuJEdUOlxuICAgIGNhc2UgY2hhcnMuJENPTE9OOlxuICAgIGNhc2UgY2hhcnMuJFBJUEU6XG4gICAgY2FzZSBjaGFycy4kQ09NTUE6XG4gICAgY2FzZSBjaGFycy4kTEJSQUNLRVQ6XG4gICAgY2FzZSBjaGFycy4kUkJSQUNLRVQ6XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICBkZWZhdWx0OlxuICAgICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbmZ1bmN0aW9uIGlzVmFsaWRTdHlsZUJsb2NrQ2hhcmFjdGVyKGNvZGU6IG51bWJlcik6IGJvb2xlYW4ge1xuICAvLyBrZXk6dmFsdWU7XG4gIC8vIGtleTpjYWxjKHNvbWV0aGluZyAuLi4gKVxuICBzd2l0Y2ggKGNvZGUpIHtcbiAgICBjYXNlIGNoYXJzLiRIQVNIOlxuICAgIGNhc2UgY2hhcnMuJFNFTUlDT0xPTjpcbiAgICBjYXNlIGNoYXJzLiRDT0xPTjpcbiAgICBjYXNlIGNoYXJzLiRQRVJDRU5UOlxuICAgIGNhc2UgY2hhcnMuJFNMQVNIOlxuICAgIGNhc2UgY2hhcnMuJEJBQ0tTTEFTSDpcbiAgICBjYXNlIGNoYXJzLiRCQU5HOlxuICAgIGNhc2UgY2hhcnMuJFBFUklPRDpcbiAgICBjYXNlIGNoYXJzLiRMUEFSRU46XG4gICAgY2FzZSBjaGFycy4kUlBBUkVOOlxuICAgICAgcmV0dXJuIHRydWU7XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5mdW5jdGlvbiBpc1ZhbGlkTWVkaWFRdWVyeVJ1bGVDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIC8vIChtaW4td2lkdGg6IDcuNWVtKSBhbmQgKG9yaWVudGF0aW9uOiBsYW5kc2NhcGUpXG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJExQQVJFTjpcbiAgICBjYXNlIGNoYXJzLiRSUEFSRU46XG4gICAgY2FzZSBjaGFycy4kQ09MT046XG4gICAgY2FzZSBjaGFycy4kUEVSQ0VOVDpcbiAgICBjYXNlIGNoYXJzLiRQRVJJT0Q6XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICBkZWZhdWx0OlxuICAgICAgcmV0dXJuIGZhbHNlO1xuICB9XG59XG5cbmZ1bmN0aW9uIGlzVmFsaWRBdFJ1bGVDaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIC8vIEBkb2N1bWVudCB1cmwoaHR0cDovL3d3dy53My5vcmcvcGFnZT9zb21ldGhpbmc9b24jaGFzaCksXG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJExQQVJFTjpcbiAgICBjYXNlIGNoYXJzLiRSUEFSRU46XG4gICAgY2FzZSBjaGFycy4kQ09MT046XG4gICAgY2FzZSBjaGFycy4kUEVSQ0VOVDpcbiAgICBjYXNlIGNoYXJzLiRQRVJJT0Q6XG4gICAgY2FzZSBjaGFycy4kU0xBU0g6XG4gICAgY2FzZSBjaGFycy4kQkFDS1NMQVNIOlxuICAgIGNhc2UgY2hhcnMuJEhBU0g6XG4gICAgY2FzZSBjaGFycy4kRVE6XG4gICAgY2FzZSBjaGFycy4kUVVFU1RJT046XG4gICAgY2FzZSBjaGFycy4kQU1QRVJTQU5EOlxuICAgIGNhc2UgY2hhcnMuJFNUQVI6XG4gICAgY2FzZSBjaGFycy4kQ09NTUE6XG4gICAgY2FzZSBjaGFycy4kTUlOVVM6XG4gICAgY2FzZSBjaGFycy4kUExVUzpcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNWYWxpZFN0eWxlRnVuY3Rpb25DaGFyYWN0ZXIoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJFBFUklPRDpcbiAgICBjYXNlIGNoYXJzLiRNSU5VUzpcbiAgICBjYXNlIGNoYXJzLiRQTFVTOlxuICAgIGNhc2UgY2hhcnMuJFNUQVI6XG4gICAgY2FzZSBjaGFycy4kU0xBU0g6XG4gICAgY2FzZSBjaGFycy4kTFBBUkVOOlxuICAgIGNhc2UgY2hhcnMuJFJQQVJFTjpcbiAgICBjYXNlIGNoYXJzLiRDT01NQTpcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNWYWxpZEJsb2NrQ2hhcmFjdGVyKGNvZGU6IG51bWJlcik6IGJvb2xlYW4ge1xuICAvLyBAc29tZXRoaW5nIHsgfVxuICAvLyBJREVOVFxuICByZXR1cm4gY29kZSA9PSBjaGFycy4kQVQ7XG59XG5cbmZ1bmN0aW9uIGlzVmFsaWRDc3NDaGFyYWN0ZXIoY29kZTogbnVtYmVyLCBtb2RlOiBDc3NMZXhlck1vZGUpOiBib29sZWFuIHtcbiAgc3dpdGNoIChtb2RlKSB7XG4gICAgY2FzZSBDc3NMZXhlck1vZGUuQUxMOlxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLkFMTF9UUkFDS19XUzpcbiAgICAgIHJldHVybiB0cnVlO1xuXG4gICAgY2FzZSBDc3NMZXhlck1vZGUuU0VMRUNUT1I6XG4gICAgICByZXR1cm4gaXNWYWxpZFNlbGVjdG9yQ2hhcmFjdGVyKGNvZGUpO1xuXG4gICAgY2FzZSBDc3NMZXhlck1vZGUuUFNFVURPX1NFTEVDVE9SX1dJVEhfQVJHVU1FTlRTOlxuICAgICAgcmV0dXJuIGlzVmFsaWRQc2V1ZG9TZWxlY3RvckNoYXJhY3Rlcihjb2RlKTtcblxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLkFUVFJJQlVURV9TRUxFQ1RPUjpcbiAgICAgIHJldHVybiBpc1ZhbGlkQXR0cmlidXRlU2VsZWN0b3JDaGFyYWN0ZXIoY29kZSk7XG5cbiAgICBjYXNlIENzc0xleGVyTW9kZS5NRURJQV9RVUVSWTpcbiAgICAgIHJldHVybiBpc1ZhbGlkTWVkaWFRdWVyeVJ1bGVDaGFyYWN0ZXIoY29kZSk7XG5cbiAgICBjYXNlIENzc0xleGVyTW9kZS5BVF9SVUxFX1FVRVJZOlxuICAgICAgcmV0dXJuIGlzVmFsaWRBdFJ1bGVDaGFyYWN0ZXIoY29kZSk7XG5cbiAgICBjYXNlIENzc0xleGVyTW9kZS5LRVlGUkFNRV9CTE9DSzpcbiAgICAgIHJldHVybiBpc1ZhbGlkS2V5ZnJhbWVCbG9ja0NoYXJhY3Rlcihjb2RlKTtcblxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLlNUWUxFX0JMT0NLOlxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLlNUWUxFX1ZBTFVFOlxuICAgICAgcmV0dXJuIGlzVmFsaWRTdHlsZUJsb2NrQ2hhcmFjdGVyKGNvZGUpO1xuXG4gICAgY2FzZSBDc3NMZXhlck1vZGUuU1RZTEVfQ0FMQ19GVU5DVElPTjpcbiAgICAgIHJldHVybiBpc1ZhbGlkU3R5bGVGdW5jdGlvbkNoYXJhY3Rlcihjb2RlKTtcblxuICAgIGNhc2UgQ3NzTGV4ZXJNb2RlLkJMT0NLOlxuICAgICAgcmV0dXJuIGlzVmFsaWRCbG9ja0NoYXJhY3Rlcihjb2RlKTtcblxuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cblxuZnVuY3Rpb24gY2hhckNvZGUoaW5wdXQ6IHN0cmluZywgaW5kZXg6IG51bWJlcik6IG51bWJlciB7XG4gIHJldHVybiBpbmRleCA+PSBpbnB1dC5sZW5ndGggPyBjaGFycy4kRU9GIDogaW5wdXQuY2hhckNvZGVBdChpbmRleCk7XG59XG5cbmZ1bmN0aW9uIGNoYXJTdHIoY29kZTogbnVtYmVyKTogc3RyaW5nIHtcbiAgcmV0dXJuIFN0cmluZy5mcm9tQ2hhckNvZGUoY29kZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc05ld2xpbmUoY29kZTogbnVtYmVyKTogYm9vbGVhbiB7XG4gIHN3aXRjaCAoY29kZSkge1xuICAgIGNhc2UgY2hhcnMuJEZGOlxuICAgIGNhc2UgY2hhcnMuJENSOlxuICAgIGNhc2UgY2hhcnMuJExGOlxuICAgIGNhc2UgY2hhcnMuJFZUQUI6XG4gICAgICByZXR1cm4gdHJ1ZTtcblxuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cbiJdfQ==