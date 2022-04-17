/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ParseError, ParseSourceFile, ParseSourceSpan } from '../parse_util';
import { CssStyleSheetAst } from './css_ast';
export { CssToken } from './css_lexer';
export { BlockType } from './css_ast';
export declare class ParsedCssResult {
    errors: CssParseError[];
    ast: CssStyleSheetAst;
    constructor(errors: CssParseError[], ast: CssStyleSheetAst);
}
export declare class CssParser {
    private _errors;
    private _file;
    private _scanner;
    private _lastToken;
    /**
     * @param css the CSS code that will be parsed
     * @param url the name of the CSS file containing the CSS source code
     */
    parse(css: string, url: string): ParsedCssResult;
}
export declare class CssParseError extends ParseError {
    static create(file: ParseSourceFile, offset: number, line: number, col: number, length: number, errMsg: string): CssParseError;
    constructor(span: ParseSourceSpan, message: string);
}
