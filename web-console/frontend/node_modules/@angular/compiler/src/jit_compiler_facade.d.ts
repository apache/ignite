/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { CompilerFacade, CoreEnvironment, R3BaseMetadataFacade, R3ComponentMetadataFacade, R3DirectiveMetadataFacade, R3InjectableMetadataFacade, R3InjectorMetadataFacade, R3NgModuleMetadataFacade, R3PipeMetadataFacade } from './compiler_facade_interface';
import { JitEvaluator } from './output/output_jit';
import { ParseSourceSpan } from './parse_util';
import { ResourceLoader } from './resource_loader';
export declare class CompilerFacadeImpl implements CompilerFacade {
    private jitEvaluator;
    R3ResolvedDependencyType: any;
    ResourceLoader: typeof ResourceLoader;
    private elementSchemaRegistry;
    constructor(jitEvaluator?: JitEvaluator);
    compilePipe(angularCoreEnv: CoreEnvironment, sourceMapUrl: string, facade: R3PipeMetadataFacade): any;
    compileInjectable(angularCoreEnv: CoreEnvironment, sourceMapUrl: string, facade: R3InjectableMetadataFacade): any;
    compileInjector(angularCoreEnv: CoreEnvironment, sourceMapUrl: string, facade: R3InjectorMetadataFacade): any;
    compileNgModule(angularCoreEnv: CoreEnvironment, sourceMapUrl: string, facade: R3NgModuleMetadataFacade): any;
    compileDirective(angularCoreEnv: CoreEnvironment, sourceMapUrl: string, facade: R3DirectiveMetadataFacade): any;
    compileComponent(angularCoreEnv: CoreEnvironment, sourceMapUrl: string, facade: R3ComponentMetadataFacade): any;
    compileBase(angularCoreEnv: CoreEnvironment, sourceMapUrl: string, facade: R3BaseMetadataFacade): any;
    createParseSourceSpan(kind: string, typeName: string, sourceUrl: string): ParseSourceSpan;
    /**
     * JIT compiles an expression and returns the result of executing that expression.
     *
     * @param def the definition which will be compiled and executed to get the value to patch
     * @param context an object map of @angular/core symbol names to symbols which will be available
     * in the context of the compiled expression
     * @param sourceUrl a URL to use for the source map of the compiled expression
     * @param preStatements a collection of statements that should be evaluated before the expression.
     */
    private jitExpression;
}
export declare function publishFacade(global: any): void;
