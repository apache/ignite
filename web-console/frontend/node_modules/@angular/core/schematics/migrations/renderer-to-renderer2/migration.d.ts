/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/core/schematics/migrations/renderer-to-renderer2/migration" />
import * as ts from 'typescript';
import { HelperFunction } from './helpers';
/** Replaces an import inside an import statement with a different one. */
export declare function replaceImport(node: ts.NamedImports, oldImport: string, newImport: string): ts.NamedImports;
/**
 * Migrates a function call expression from `Renderer` to `Renderer2`.
 * Returns null if the expression should be dropped.
 */
export declare function migrateExpression(node: ts.CallExpression, typeChecker: ts.TypeChecker): {
    node: ts.Node | null;
    requiredHelpers?: HelperFunction[];
};
