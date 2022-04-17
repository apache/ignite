/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { CompileDirectiveMetadata, CompilePipeSummary } from '../compile_metadata';
import { CompileReflector } from '../compile_reflector';
import * as o from '../output/output_ast';
import { TemplateAst } from '../template_parser/template_ast';
import { OutputContext } from '../util';
export declare class ViewCompileResult {
    viewClassVar: string;
    rendererTypeVar: string;
    constructor(viewClassVar: string, rendererTypeVar: string);
}
export declare class ViewCompiler {
    private _reflector;
    constructor(_reflector: CompileReflector);
    compileComponent(outputCtx: OutputContext, component: CompileDirectiveMetadata, template: TemplateAst[], styles: o.Expression, usedPipes: CompilePipeSummary[]): ViewCompileResult;
}
interface StaticAndDynamicQueryIds {
    staticQueryIds: Set<number>;
    dynamicQueryIds: Set<number>;
}
export declare function findStaticQueryIds(nodes: TemplateAst[], result?: Map<TemplateAst, StaticAndDynamicQueryIds>): Map<TemplateAst, StaticAndDynamicQueryIds>;
export declare function staticViewQueryIds(nodeStaticQueryIds: Map<TemplateAst, StaticAndDynamicQueryIds>): StaticAndDynamicQueryIds;
export declare function elementEventFullName(target: string | null, name: string): string;
export {};
