/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/locate_symbol" />
import { TemplateInfo } from './common';
import { Span, Symbol } from './types';
export interface SymbolInfo {
    symbol: Symbol;
    span: Span;
}
export declare function locateSymbol(info: TemplateInfo): SymbolInfo | undefined;
