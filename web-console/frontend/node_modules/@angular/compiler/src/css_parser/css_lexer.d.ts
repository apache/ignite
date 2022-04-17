/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
export declare enum CssTokenType {
    EOF = 0,
    String = 1,
    Comment = 2,
    Identifier = 3,
    Number = 4,
    IdentifierOrNumber = 5,
    AtKeyword = 6,
    Character = 7,
    Whitespace = 8,
    Invalid = 9
}
export declare enum CssLexerMode {
    ALL = 0,
    ALL_TRACK_WS = 1,
    SELECTOR = 2,
    PSEUDO_SELECTOR = 3,
    PSEUDO_SELECTOR_WITH_ARGUMENTS = 4,
    ATTRIBUTE_SELECTOR = 5,
    AT_RULE_QUERY = 6,
    MEDIA_QUERY = 7,
    BLOCK = 8,
    KEYFRAME_BLOCK = 9,
    STYLE_BLOCK = 10,
    STYLE_VALUE = 11,
    STYLE_VALUE_FUNCTION = 12,
    STYLE_CALC_FUNCTION = 13
}
export declare class LexedCssResult {
    error: Error | null;
    token: CssToken;
    constructor(error: Error | null, token: CssToken);
}
export declare function generateErrorMessage(input: string, message: string, errorValue: string, index: number, row: number, column: number): string;
export declare function findProblemCode(input: string, errorValue: string, index: number, column: number): string;
export declare class CssToken {
    index: number;
    column: number;
    line: number;
    type: CssTokenType;
    strValue: string;
    numValue: number;
    constructor(index: number, column: number, line: number, type: CssTokenType, strValue: string);
}
export declare class CssLexer {
    scan(text: string, trackComments?: boolean): CssScanner;
}
export declare function cssScannerError(token: CssToken, message: string): Error;
export declare function getRawMessage(error: Error): string;
export declare function getToken(error: Error): CssToken;
export declare class CssScanner {
    input: string;
    private _trackComments;
    peek: number;
    peekPeek: number;
    length: number;
    index: number;
    column: number;
    line: number;
    constructor(input: string, _trackComments?: boolean);
    getMode(): CssLexerMode;
    setMode(mode: CssLexerMode): void;
    advance(): void;
    peekAt(index: number): number;
    consumeEmptyStatements(): void;
    consumeWhitespace(): void;
    consume(type: CssTokenType, value?: string | null): LexedCssResult;
    scan(): LexedCssResult | null;
    scanComment(): CssToken | null;
    scanWhitespace(): CssToken;
    scanString(): CssToken | null;
    scanNumber(): CssToken;
    scanIdentifier(): CssToken | null;
    scanCssValueFunction(): CssToken;
    scanCharacter(): CssToken | null;
    scanAtExpression(): CssToken | null;
    assertCondition(status: boolean, errorMessage: string): boolean;
    error(message: string, errorTokenValue?: string | null, doNotAdvance?: boolean): CssToken;
}
export declare function isNewline(code: number): boolean;
