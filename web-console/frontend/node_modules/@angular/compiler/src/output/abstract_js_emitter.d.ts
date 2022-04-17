/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { AbstractEmitterVisitor, EmitterVisitorContext } from './abstract_emitter';
import * as o from './output_ast';
export declare abstract class AbstractJsEmitterVisitor extends AbstractEmitterVisitor {
    constructor();
    visitDeclareClassStmt(stmt: o.ClassStmt, ctx: EmitterVisitorContext): any;
    private _visitClassConstructor;
    private _visitClassGetter;
    private _visitClassMethod;
    visitWrappedNodeExpr(ast: o.WrappedNodeExpr<any>, ctx: EmitterVisitorContext): any;
    visitReadVarExpr(ast: o.ReadVarExpr, ctx: EmitterVisitorContext): string | null;
    visitDeclareVarStmt(stmt: o.DeclareVarStmt, ctx: EmitterVisitorContext): any;
    visitCastExpr(ast: o.CastExpr, ctx: EmitterVisitorContext): any;
    visitInvokeFunctionExpr(expr: o.InvokeFunctionExpr, ctx: EmitterVisitorContext): string | null;
    visitFunctionExpr(ast: o.FunctionExpr, ctx: EmitterVisitorContext): any;
    visitDeclareFunctionStmt(stmt: o.DeclareFunctionStmt, ctx: EmitterVisitorContext): any;
    visitTryCatchStmt(stmt: o.TryCatchStmt, ctx: EmitterVisitorContext): any;
    private _visitParams;
    getBuiltinMethodName(method: o.BuiltinMethod): string;
}
