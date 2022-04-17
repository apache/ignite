/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/core/schematics/migrations/renderer-to-renderer2/util" />
import * as ts from 'typescript';
/**
 * Finds typed nodes (e.g. function parameters or class properties) that are referencing the old
 * `Renderer`, as well as calls to the `Renderer` methods.
 */
export declare function findRendererReferences(sourceFile: ts.SourceFile, typeChecker: ts.TypeChecker, rendererImport: ts.NamedImports): {
    typedNodes: Set<ts.ParameterDeclaration | ts.PropertyDeclaration | ts.AsExpression>;
    methodCalls: Set<ts.CallExpression>;
    forwardRefs: Set<ts.Identifier>;
};
/** Finds the import from @angular/core that has a symbol with a particular name. */
export declare function findCoreImport(sourceFile: ts.SourceFile, symbolName: string): ts.NamedImports | null;
/** Finds an import specifier with a particular name, accounting for aliases. */
export declare function findImportSpecifier(elements: ts.NodeArray<ts.ImportSpecifier>, importName: string): ts.ImportSpecifier | null;
