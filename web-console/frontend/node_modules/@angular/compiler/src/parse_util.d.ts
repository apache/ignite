import { CompileIdentifierMetadata } from './compile_metadata';
export declare class ParseLocation {
    file: ParseSourceFile;
    offset: number;
    line: number;
    col: number;
    constructor(file: ParseSourceFile, offset: number, line: number, col: number);
    toString(): string;
    moveBy(delta: number): ParseLocation;
    getContext(maxChars: number, maxLines: number): {
        before: string;
        after: string;
    } | null;
}
export declare class ParseSourceFile {
    content: string;
    url: string;
    constructor(content: string, url: string);
}
export declare class ParseSourceSpan {
    start: ParseLocation;
    end: ParseLocation;
    details: string | null;
    constructor(start: ParseLocation, end: ParseLocation, details?: string | null);
    toString(): string;
}
export declare const EMPTY_PARSE_LOCATION: ParseLocation;
export declare const EMPTY_SOURCE_SPAN: ParseSourceSpan;
export declare enum ParseErrorLevel {
    WARNING = 0,
    ERROR = 1
}
export declare class ParseError {
    span: ParseSourceSpan;
    msg: string;
    level: ParseErrorLevel;
    constructor(span: ParseSourceSpan, msg: string, level?: ParseErrorLevel);
    contextualMessage(): string;
    toString(): string;
}
export declare function typeSourceSpan(kind: string, type: CompileIdentifierMetadata): ParseSourceSpan;
/**
 * Generates Source Span object for a given R3 Type for JIT mode.
 *
 * @param kind Component or Directive.
 * @param typeName name of the Component or Directive.
 * @param sourceUrl reference to Component or Directive source.
 * @returns instance of ParseSourceSpan that represent a given Component or Directive.
 */
export declare function r3JitTypeSourceSpan(kind: string, typeName: string, sourceUrl: string): ParseSourceSpan;
