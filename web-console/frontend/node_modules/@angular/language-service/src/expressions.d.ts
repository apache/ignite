/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/expressions" />
import { AST } from '@angular/compiler';
import { Span, Symbol, SymbolQuery, SymbolTable } from './types';
export declare function getExpressionCompletions(scope: SymbolTable, ast: AST, position: number, query: SymbolQuery): Symbol[] | undefined;
export declare function getExpressionSymbol(scope: SymbolTable, ast: AST, position: number, query: SymbolQuery): {
    symbol: Symbol;
    span: Span;
} | undefined;
