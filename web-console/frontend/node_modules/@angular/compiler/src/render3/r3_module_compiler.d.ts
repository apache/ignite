/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { CompileShallowModuleMetadata } from '../compile_metadata';
import { InjectableCompiler } from '../injectable_compiler';
import * as o from '../output/output_ast';
import { OutputContext } from '../util';
import { R3DependencyMetadata } from './r3_factory';
import { R3Reference } from './util';
export interface R3NgModuleDef {
    expression: o.Expression;
    type: o.Type;
    additionalStatements: o.Statement[];
}
/**
 * Metadata required by the module compiler to generate a `ngModuleDef` for a type.
 */
export interface R3NgModuleMetadata {
    /**
     * An expression representing the module type being compiled.
     */
    type: o.Expression;
    /**
     * An array of expressions representing the bootstrap components specified by the module.
     */
    bootstrap: R3Reference[];
    /**
     * An array of expressions representing the directives and pipes declared by the module.
     */
    declarations: R3Reference[];
    /**
     * An array of expressions representing the imports of the module.
     */
    imports: R3Reference[];
    /**
     * An array of expressions representing the exports of the module.
     */
    exports: R3Reference[];
    /**
     * Whether to emit the selector scope values (declarations, imports, exports) inline into the
     * module definition, or to generate additional statements which patch them on. Inline emission
     * does not allow components to be tree-shaken, but is useful for JIT mode.
     */
    emitInline: boolean;
    /**
     * Whether to generate closure wrappers for bootstrap, declarations, imports, and exports.
     */
    containsForwardDecls: boolean;
    /**
     * The set of schemas that declare elements to be allowed in the NgModule.
     */
    schemas: R3Reference[] | null;
    /** Unique ID or expression representing the unique ID of an NgModule. */
    id: o.Expression | null;
}
/**
 * Construct an `R3NgModuleDef` for the given `R3NgModuleMetadata`.
 */
export declare function compileNgModule(meta: R3NgModuleMetadata): R3NgModuleDef;
export interface R3InjectorDef {
    expression: o.Expression;
    type: o.Type;
    statements: o.Statement[];
}
export interface R3InjectorMetadata {
    name: string;
    type: o.Expression;
    deps: R3DependencyMetadata[] | null;
    providers: o.Expression | null;
    imports: o.Expression[];
}
export declare function compileInjector(meta: R3InjectorMetadata): R3InjectorDef;
export declare function compileNgModuleFromRender2(ctx: OutputContext, ngModule: CompileShallowModuleMetadata, injectableCompiler: InjectableCompiler): void;
