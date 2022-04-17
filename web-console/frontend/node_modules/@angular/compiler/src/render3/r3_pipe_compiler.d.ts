/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { CompilePipeMetadata } from '../compile_metadata';
import { CompileReflector } from '../compile_reflector';
import * as o from '../output/output_ast';
import { OutputContext } from '../util';
import { R3DependencyMetadata } from './r3_factory';
export interface R3PipeMetadata {
    /**
     * Name of the pipe type.
     */
    name: string;
    /**
     * An expression representing a reference to the pipe itself.
     */
    type: o.Expression;
    /**
     * Number of generic type parameters of the type itself.
     */
    typeArgumentCount: number;
    /**
     * Name of the pipe.
     */
    pipeName: string;
    /**
     * Dependencies of the pipe's constructor.
     */
    deps: R3DependencyMetadata[] | null;
    /**
     * Whether the pipe is marked as pure.
     */
    pure: boolean;
}
export declare function compilePipeFromMetadata(metadata: R3PipeMetadata): {
    expression: o.InvokeFunctionExpr;
    type: o.ExpressionType;
    statements: o.Statement[];
};
/**
 * Write a pipe definition to the output context.
 */
export declare function compilePipeFromRender2(outputCtx: OutputContext, pipe: CompilePipeMetadata, reflector: CompileReflector): undefined;
