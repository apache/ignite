/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { CompileDirectiveMetadata } from '../../compile_metadata';
import { CompileReflector } from '../../compile_reflector';
import { ConstantPool } from '../../constant_pool';
import * as o from '../../output/output_ast';
import { ParseError, ParseSourceSpan } from '../../parse_util';
import { BindingParser } from '../../template_parser/binding_parser';
import { OutputContext } from '../../util';
import { Render3ParseResult } from '../r3_template_transform';
import { R3ComponentDef, R3ComponentMetadata, R3DirectiveDef, R3DirectiveMetadata, R3HostMetadata, R3QueryMetadata } from './api';
/**
 * Compile a directive for the render3 runtime as defined by the `R3DirectiveMetadata`.
 */
export declare function compileDirectiveFromMetadata(meta: R3DirectiveMetadata, constantPool: ConstantPool, bindingParser: BindingParser): R3DirectiveDef;
export interface R3BaseRefMetaData {
    name: string;
    type: o.Expression;
    typeSourceSpan: ParseSourceSpan;
    inputs?: {
        [key: string]: string | [string, string];
    };
    outputs?: {
        [key: string]: string;
    };
    viewQueries?: R3QueryMetadata[];
    queries?: R3QueryMetadata[];
    host?: R3HostMetadata;
}
/**
 * Compile a base definition for the render3 runtime as defined by {@link R3BaseRefMetadata}
 * @param meta the metadata used for compilation.
 */
export declare function compileBaseDefFromMetadata(meta: R3BaseRefMetaData, constantPool: ConstantPool, bindingParser: BindingParser): {
    expression: o.InvokeFunctionExpr;
    type: o.ExpressionType;
};
/**
 * Compile a component for the render3 runtime as defined by the `R3ComponentMetadata`.
 */
export declare function compileComponentFromMetadata(meta: R3ComponentMetadata, constantPool: ConstantPool, bindingParser: BindingParser): R3ComponentDef;
/**
 * A wrapper around `compileDirective` which depends on render2 global analysis data as its input
 * instead of the `R3DirectiveMetadata`.
 *
 * `R3DirectiveMetadata` is computed from `CompileDirectiveMetadata` and other statically reflected
 * information.
 */
export declare function compileDirectiveFromRender2(outputCtx: OutputContext, directive: CompileDirectiveMetadata, reflector: CompileReflector, bindingParser: BindingParser): void;
/**
 * A wrapper around `compileComponent` which depends on render2 global analysis data as its input
 * instead of the `R3DirectiveMetadata`.
 *
 * `R3ComponentMetadata` is computed from `CompileDirectiveMetadata` and other statically reflected
 * information.
 */
export declare function compileComponentFromRender2(outputCtx: OutputContext, component: CompileDirectiveMetadata, render3Ast: Render3ParseResult, reflector: CompileReflector, bindingParser: BindingParser, directiveTypeBySel: Map<string, any>, pipeTypeByName: Map<string, any>): void;
export interface ParsedHostBindings {
    attributes: {
        [key: string]: o.Expression;
    };
    listeners: {
        [key: string]: string;
    };
    properties: {
        [key: string]: string;
    };
    specialAttributes: {
        styleAttr?: string;
        classAttr?: string;
    };
}
export declare function parseHostBindings(host: {
    [key: string]: string | o.Expression;
}): ParsedHostBindings;
/**
 * Verifies host bindings and returns the list of errors (if any). Empty array indicates that a
 * given set of host bindings has no errors.
 *
 * @param bindings set of host bindings to verify.
 * @param sourceSpan source span where host bindings were defined.
 * @returns array of errors associated with a given set of host bindings.
 */
export declare function verifyHostBindings(bindings: ParsedHostBindings, sourceSpan: ParseSourceSpan): ParseError[];
