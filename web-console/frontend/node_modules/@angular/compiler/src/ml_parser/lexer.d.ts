/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ParseError, ParseSourceSpan } from '../parse_util';
import { InterpolationConfig } from './interpolation_config';
import { TagDefinition } from './tags';
export declare enum TokenType {
    TAG_OPEN_START = 0,
    TAG_OPEN_END = 1,
    TAG_OPEN_END_VOID = 2,
    TAG_CLOSE = 3,
    TEXT = 4,
    ESCAPABLE_RAW_TEXT = 5,
    RAW_TEXT = 6,
    COMMENT_START = 7,
    COMMENT_END = 8,
    CDATA_START = 9,
    CDATA_END = 10,
    ATTR_NAME = 11,
    ATTR_QUOTE = 12,
    ATTR_VALUE = 13,
    DOC_TYPE = 14,
    EXPANSION_FORM_START = 15,
    EXPANSION_CASE_VALUE = 16,
    EXPANSION_CASE_EXP_START = 17,
    EXPANSION_CASE_EXP_END = 18,
    EXPANSION_FORM_END = 19,
    EOF = 20
}
export declare class Token {
    type: TokenType | null;
    parts: string[];
    sourceSpan: ParseSourceSpan;
    constructor(type: TokenType | null, parts: string[], sourceSpan: ParseSourceSpan);
}
export declare class TokenError extends ParseError {
    tokenType: TokenType | null;
    constructor(errorMsg: string, tokenType: TokenType | null, span: ParseSourceSpan);
}
export declare class TokenizeResult {
    tokens: Token[];
    errors: TokenError[];
    constructor(tokens: Token[], errors: TokenError[]);
}
export interface LexerRange {
    startPos: number;
    startLine: number;
    startCol: number;
    endPos: number;
}
/**
 * Options that modify how the text is tokenized.
 */
export interface TokenizeOptions {
    /** Whether to tokenize ICU messages (considered as text nodes when false). */
    tokenizeExpansionForms?: boolean;
    /** How to tokenize interpolation markers. */
    interpolationConfig?: InterpolationConfig;
    /**
     * The start and end point of the text to parse within the `source` string.
     * The entire `source` string is parsed if this is not provided.
     * */
    range?: LexerRange;
    /**
     * If this text is stored in a JavaScript string, then we have to deal with escape sequences.
     *
     * **Example 1:**
     *
     * ```
     * "abc\"def\nghi"
     * ```
     *
     * - The `\"` must be converted to `"`.
     * - The `\n` must be converted to a new line character in a token,
     *   but it should not increment the current line for source mapping.
     *
     * **Example 2:**
     *
     * ```
     * "abc\
     *  def"
     * ```
     *
     * The line continuation (`\` followed by a newline) should be removed from a token
     * but the new line should increment the current line for source mapping.
     */
    escapedString?: boolean;
    /**
     * An array of characters that should be considered as leading trivia.
     * Leading trivia are characters that are not important to the developer, and so should not be
     * included in source-map segments.  A common example is whitespace.
     */
    leadingTriviaChars?: string[];
}
export declare function tokenize(source: string, url: string, getTagDefinition: (tagName: string) => TagDefinition, options?: TokenizeOptions): TokenizeResult;
/**
 * The _Tokenizer uses objects of this type to move through the input text,
 * extracting "parsed characters". These could be more than one actual character
 * if the text contains escape sequences.
 */
interface CharacterCursor {
    /** Initialize the cursor. */
    init(): void;
    /** The parsed character at the current cursor position. */
    peek(): number;
    /** Advance the cursor by one parsed character. */
    advance(): void;
    /** Get a span from the marked start point to the current point. */
    getSpan(start?: this, leadingTriviaCodePoints?: number[]): ParseSourceSpan;
    /** Get the parsed characters from the marked start point to the current point. */
    getChars(start: this): string;
    /** The number of characters left before the end of the cursor. */
    charsLeft(): number;
    /** The number of characters between `this` cursor and `other` cursor. */
    diff(other: this): number;
    /** Make a copy of this cursor */
    clone(): CharacterCursor;
}
export declare class CursorError {
    msg: string;
    cursor: CharacterCursor;
    constructor(msg: string, cursor: CharacterCursor);
}
export {};
