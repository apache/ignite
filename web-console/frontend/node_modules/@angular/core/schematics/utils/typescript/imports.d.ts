/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/core/schematics/utils/typescript/imports" />
import * as ts from 'typescript';
export declare type Import = {
    name: string;
    importModule: string;
    node: ts.ImportDeclaration;
};
/** Gets import information about the specified identifier by using the Type checker. */
export declare function getImportOfIdentifier(typeChecker: ts.TypeChecker, node: ts.Identifier): Import | null;
