/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/diagnostics" />
import { NgAnalyzedModules } from '@angular/compiler';
import { AstResult } from './common';
import { Declarations, Diagnostics, TemplateSource } from './types';
export interface AstProvider {
    getTemplateAst(template: TemplateSource, fileName: string): AstResult;
}
export declare function getDeclarationDiagnostics(declarations: Declarations, modules: NgAnalyzedModules): Diagnostics;
