/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { CompileNgModuleMetadata, CompileTypeMetadata } from '../compile_metadata';
import { CompileMetadataResolver } from '../metadata_resolver';
import * as o from '../output/output_ast';
import { OutputContext } from '../util';
/**
 * Write a Renderer2 compatibility module factory to the output context.
 */
export declare function compileModuleFactory(outputCtx: OutputContext, module: CompileNgModuleMetadata, backPatchReferenceOf: (module: CompileTypeMetadata) => o.Expression, resolver: CompileMetadataResolver): void;
