/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/utils" />
import { CompileTypeMetadata, ParseSourceSpan, TemplateAst, TemplateAstPath } from '@angular/compiler';
import { DiagnosticTemplateInfo } from '@angular/compiler-cli/src/language_services';
import { SelectorInfo, TemplateInfo } from './common';
import { Span } from './types';
export interface SpanHolder {
    sourceSpan: ParseSourceSpan;
    endSourceSpan?: ParseSourceSpan | null;
    children?: SpanHolder[];
}
export declare function isParseSourceSpan(value: any): value is ParseSourceSpan;
export declare function spanOf(span: SpanHolder): Span;
export declare function spanOf(span: ParseSourceSpan): Span;
export declare function spanOf(span: SpanHolder | ParseSourceSpan | undefined): Span | undefined;
export declare function inSpan(position: number, span?: Span, exclusive?: boolean): boolean;
export declare function offsetSpan(span: Span, amount: number): Span;
export declare function isNarrower(spanA: Span, spanB: Span): boolean;
export declare function hasTemplateReference(type: CompileTypeMetadata): boolean;
export declare function getSelectors(info: TemplateInfo): SelectorInfo;
export declare function flatten<T>(a: T[][]): T[];
export declare function removeSuffix(value: string, suffix: string): string;
export declare function uniqueByName<T extends {
    name: string;
}>(elements: T[] | undefined): T[] | undefined;
export declare function isTypescriptVersion(low: string, high?: string): boolean;
export declare function diagnosticInfoFromTemplateInfo(info: TemplateInfo): DiagnosticTemplateInfo;
export declare function findTemplateAstAt(ast: TemplateAst[], position: number, allowWidening?: boolean): TemplateAstPath;
