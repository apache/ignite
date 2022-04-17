/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ParseSourceSpan } from '../parse_util';
export declare enum TypeModifier {
    Const = 0
}
export declare abstract class Type {
    modifiers: TypeModifier[] | null;
    constructor(modifiers?: TypeModifier[] | null);
    abstract visitType(visitor: TypeVisitor, context: any): any;
    hasModifier(modifier: TypeModifier): boolean;
}
export declare enum BuiltinTypeName {
    Dynamic = 0,
    Bool = 1,
    String = 2,
    Int = 3,
    Number = 4,
    Function = 5,
    Inferred = 6,
    None = 7
}
export declare class BuiltinType extends Type {
    name: BuiltinTypeName;
    constructor(name: BuiltinTypeName, modifiers?: TypeModifier[] | null);
    visitType(visitor: TypeVisitor, context: any): any;
}
export declare class ExpressionType extends Type {
    value: Expression;
    typeParams: Type[] | null;
    constructor(value: Expression, modifiers?: TypeModifier[] | null, typeParams?: Type[] | null);
    visitType(visitor: TypeVisitor, context: any): any;
}
export declare class ArrayType extends Type {
    of: Type;
    constructor(of: Type, modifiers?: TypeModifier[] | null);
    visitType(visitor: TypeVisitor, context: any): any;
}
export declare class MapType extends Type {
    valueType: Type | null;
    constructor(valueType: Type | null | undefined, modifiers?: TypeModifier[] | null);
    visitType(visitor: TypeVisitor, context: any): any;
}
export declare const DYNAMIC_TYPE: BuiltinType;
export declare const INFERRED_TYPE: BuiltinType;
export declare const BOOL_TYPE: BuiltinType;
export declare const INT_TYPE: BuiltinType;
export declare const NUMBER_TYPE: BuiltinType;
export declare const STRING_TYPE: BuiltinType;
export declare const FUNCTION_TYPE: BuiltinType;
export declare const NONE_TYPE: BuiltinType;
export interface TypeVisitor {
    visitBuiltinType(type: BuiltinType, context: any): any;
    visitExpressionType(type: ExpressionType, context: any): any;
    visitArrayType(type: ArrayType, context: any): any;
    visitMapType(type: MapType, context: any): any;
}
export declare enum BinaryOperator {
    Equals = 0,
    NotEquals = 1,
    Identical = 2,
    NotIdentical = 3,
    Minus = 4,
    Plus = 5,
    Divide = 6,
    Multiply = 7,
    Modulo = 8,
    And = 9,
    Or = 10,
    BitwiseAnd = 11,
    Lower = 12,
    LowerEquals = 13,
    Bigger = 14,
    BiggerEquals = 15
}
export declare function nullSafeIsEquivalent<T extends {
    isEquivalent(other: T): boolean;
}>(base: T | null, other: T | null): boolean;
export declare function areAllEquivalent<T extends {
    isEquivalent(other: T): boolean;
}>(base: T[], other: T[]): boolean;
export declare abstract class Expression {
    type: Type | null;
    sourceSpan: ParseSourceSpan | null;
    constructor(type: Type | null | undefined, sourceSpan?: ParseSourceSpan | null);
    abstract visitExpression(visitor: ExpressionVisitor, context: any): any;
    /**
     * Calculates whether this expression produces the same value as the given expression.
     * Note: We don't check Types nor ParseSourceSpans nor function arguments.
     */
    abstract isEquivalent(e: Expression): boolean;
    /**
     * Return true if the expression is constant.
     */
    abstract isConstant(): boolean;
    prop(name: string, sourceSpan?: ParseSourceSpan | null): ReadPropExpr;
    key(index: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null): ReadKeyExpr;
    callMethod(name: string | BuiltinMethod, params: Expression[], sourceSpan?: ParseSourceSpan | null): InvokeMethodExpr;
    callFn(params: Expression[], sourceSpan?: ParseSourceSpan | null): InvokeFunctionExpr;
    instantiate(params: Expression[], type?: Type | null, sourceSpan?: ParseSourceSpan | null): InstantiateExpr;
    conditional(trueCase: Expression, falseCase?: Expression | null, sourceSpan?: ParseSourceSpan | null): ConditionalExpr;
    equals(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    notEquals(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    identical(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    notIdentical(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    minus(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    plus(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    divide(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    multiply(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    modulo(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    and(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    bitwiseAnd(rhs: Expression, sourceSpan?: ParseSourceSpan | null, parens?: boolean): BinaryOperatorExpr;
    or(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    lower(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    lowerEquals(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    bigger(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    biggerEquals(rhs: Expression, sourceSpan?: ParseSourceSpan | null): BinaryOperatorExpr;
    isBlank(sourceSpan?: ParseSourceSpan | null): Expression;
    cast(type: Type, sourceSpan?: ParseSourceSpan | null): Expression;
    toStmt(): Statement;
}
export declare enum BuiltinVar {
    This = 0,
    Super = 1,
    CatchError = 2,
    CatchStack = 3
}
export declare class ReadVarExpr extends Expression {
    name: string | null;
    builtin: BuiltinVar | null;
    constructor(name: string | BuiltinVar, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
    set(value: Expression): WriteVarExpr;
}
export declare class TypeofExpr extends Expression {
    expr: Expression;
    constructor(expr: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    visitExpression(visitor: ExpressionVisitor, context: any): any;
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
}
export declare class WrappedNodeExpr<T> extends Expression {
    node: T;
    constructor(node: T, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class WriteVarExpr extends Expression {
    name: string;
    value: Expression;
    constructor(name: string, value: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
    toDeclStmt(type?: Type | null, modifiers?: StmtModifier[] | null): DeclareVarStmt;
    toConstDecl(): DeclareVarStmt;
}
export declare class WriteKeyExpr extends Expression {
    receiver: Expression;
    index: Expression;
    value: Expression;
    constructor(receiver: Expression, index: Expression, value: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class WritePropExpr extends Expression {
    receiver: Expression;
    name: string;
    value: Expression;
    constructor(receiver: Expression, name: string, value: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare enum BuiltinMethod {
    ConcatArray = 0,
    SubscribeObservable = 1,
    Bind = 2
}
export declare class InvokeMethodExpr extends Expression {
    receiver: Expression;
    args: Expression[];
    name: string | null;
    builtin: BuiltinMethod | null;
    constructor(receiver: Expression, method: string | BuiltinMethod, args: Expression[], type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class InvokeFunctionExpr extends Expression {
    fn: Expression;
    args: Expression[];
    pure: boolean;
    constructor(fn: Expression, args: Expression[], type?: Type | null, sourceSpan?: ParseSourceSpan | null, pure?: boolean);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class InstantiateExpr extends Expression {
    classExpr: Expression;
    args: Expression[];
    constructor(classExpr: Expression, args: Expression[], type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class LiteralExpr extends Expression {
    value: number | string | boolean | null | undefined;
    constructor(value: number | string | boolean | null | undefined, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class ExternalExpr extends Expression {
    value: ExternalReference;
    typeParams: Type[] | null;
    constructor(value: ExternalReference, type?: Type | null, typeParams?: Type[] | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class ExternalReference {
    moduleName: string | null;
    name: string | null;
    runtime?: any;
    constructor(moduleName: string | null, name: string | null, runtime?: any);
}
export declare class ConditionalExpr extends Expression {
    condition: Expression;
    falseCase: Expression | null;
    trueCase: Expression;
    constructor(condition: Expression, trueCase: Expression, falseCase?: Expression | null, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class NotExpr extends Expression {
    condition: Expression;
    constructor(condition: Expression, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class AssertNotNull extends Expression {
    condition: Expression;
    constructor(condition: Expression, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class CastExpr extends Expression {
    value: Expression;
    constructor(value: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class FnParam {
    name: string;
    type: Type | null;
    constructor(name: string, type?: Type | null);
    isEquivalent(param: FnParam): boolean;
}
export declare class FunctionExpr extends Expression {
    params: FnParam[];
    statements: Statement[];
    name?: string | null | undefined;
    constructor(params: FnParam[], statements: Statement[], type?: Type | null, sourceSpan?: ParseSourceSpan | null, name?: string | null | undefined);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
    toDeclStmt(name: string, modifiers?: StmtModifier[] | null): DeclareFunctionStmt;
}
export declare class BinaryOperatorExpr extends Expression {
    operator: BinaryOperator;
    rhs: Expression;
    parens: boolean;
    lhs: Expression;
    constructor(operator: BinaryOperator, lhs: Expression, rhs: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null, parens?: boolean);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class ReadPropExpr extends Expression {
    receiver: Expression;
    name: string;
    constructor(receiver: Expression, name: string, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
    set(value: Expression): WritePropExpr;
}
export declare class ReadKeyExpr extends Expression {
    receiver: Expression;
    index: Expression;
    constructor(receiver: Expression, index: Expression, type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
    set(value: Expression): WriteKeyExpr;
}
export declare class LiteralArrayExpr extends Expression {
    entries: Expression[];
    constructor(entries: Expression[], type?: Type | null, sourceSpan?: ParseSourceSpan | null);
    isConstant(): boolean;
    isEquivalent(e: Expression): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class LiteralMapEntry {
    key: string;
    value: Expression;
    quoted: boolean;
    constructor(key: string, value: Expression, quoted: boolean);
    isEquivalent(e: LiteralMapEntry): boolean;
}
export declare class LiteralMapExpr extends Expression {
    entries: LiteralMapEntry[];
    valueType: Type | null;
    constructor(entries: LiteralMapEntry[], type?: MapType | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export declare class CommaExpr extends Expression {
    parts: Expression[];
    constructor(parts: Expression[], sourceSpan?: ParseSourceSpan | null);
    isEquivalent(e: Expression): boolean;
    isConstant(): boolean;
    visitExpression(visitor: ExpressionVisitor, context: any): any;
}
export interface ExpressionVisitor {
    visitReadVarExpr(ast: ReadVarExpr, context: any): any;
    visitWriteVarExpr(expr: WriteVarExpr, context: any): any;
    visitWriteKeyExpr(expr: WriteKeyExpr, context: any): any;
    visitWritePropExpr(expr: WritePropExpr, context: any): any;
    visitInvokeMethodExpr(ast: InvokeMethodExpr, context: any): any;
    visitInvokeFunctionExpr(ast: InvokeFunctionExpr, context: any): any;
    visitInstantiateExpr(ast: InstantiateExpr, context: any): any;
    visitLiteralExpr(ast: LiteralExpr, context: any): any;
    visitExternalExpr(ast: ExternalExpr, context: any): any;
    visitConditionalExpr(ast: ConditionalExpr, context: any): any;
    visitNotExpr(ast: NotExpr, context: any): any;
    visitAssertNotNullExpr(ast: AssertNotNull, context: any): any;
    visitCastExpr(ast: CastExpr, context: any): any;
    visitFunctionExpr(ast: FunctionExpr, context: any): any;
    visitBinaryOperatorExpr(ast: BinaryOperatorExpr, context: any): any;
    visitReadPropExpr(ast: ReadPropExpr, context: any): any;
    visitReadKeyExpr(ast: ReadKeyExpr, context: any): any;
    visitLiteralArrayExpr(ast: LiteralArrayExpr, context: any): any;
    visitLiteralMapExpr(ast: LiteralMapExpr, context: any): any;
    visitCommaExpr(ast: CommaExpr, context: any): any;
    visitWrappedNodeExpr(ast: WrappedNodeExpr<any>, context: any): any;
    visitTypeofExpr(ast: TypeofExpr, context: any): any;
}
export declare const THIS_EXPR: ReadVarExpr;
export declare const SUPER_EXPR: ReadVarExpr;
export declare const CATCH_ERROR_VAR: ReadVarExpr;
export declare const CATCH_STACK_VAR: ReadVarExpr;
export declare const NULL_EXPR: LiteralExpr;
export declare const TYPED_NULL_EXPR: LiteralExpr;
export declare enum StmtModifier {
    Final = 0,
    Private = 1,
    Exported = 2,
    Static = 3
}
export declare abstract class Statement {
    modifiers: StmtModifier[];
    sourceSpan: ParseSourceSpan | null;
    constructor(modifiers?: StmtModifier[] | null, sourceSpan?: ParseSourceSpan | null);
    /**
     * Calculates whether this statement produces the same value as the given statement.
     * Note: We don't check Types nor ParseSourceSpans nor function arguments.
     */
    abstract isEquivalent(stmt: Statement): boolean;
    abstract visitStatement(visitor: StatementVisitor, context: any): any;
    hasModifier(modifier: StmtModifier): boolean;
}
export declare class DeclareVarStmt extends Statement {
    name: string;
    value?: Expression | undefined;
    type: Type | null;
    constructor(name: string, value?: Expression | undefined, type?: Type | null, modifiers?: StmtModifier[] | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class DeclareFunctionStmt extends Statement {
    name: string;
    params: FnParam[];
    statements: Statement[];
    type: Type | null;
    constructor(name: string, params: FnParam[], statements: Statement[], type?: Type | null, modifiers?: StmtModifier[] | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class ExpressionStatement extends Statement {
    expr: Expression;
    constructor(expr: Expression, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class ReturnStatement extends Statement {
    value: Expression;
    constructor(value: Expression, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class AbstractClassPart {
    modifiers: StmtModifier[] | null;
    type: Type | null;
    constructor(type: Type | null | undefined, modifiers: StmtModifier[] | null);
    hasModifier(modifier: StmtModifier): boolean;
}
export declare class ClassField extends AbstractClassPart {
    name: string;
    initializer?: Expression | undefined;
    constructor(name: string, type?: Type | null, modifiers?: StmtModifier[] | null, initializer?: Expression | undefined);
    isEquivalent(f: ClassField): boolean;
}
export declare class ClassMethod extends AbstractClassPart {
    name: string | null;
    params: FnParam[];
    body: Statement[];
    constructor(name: string | null, params: FnParam[], body: Statement[], type?: Type | null, modifiers?: StmtModifier[] | null);
    isEquivalent(m: ClassMethod): boolean;
}
export declare class ClassGetter extends AbstractClassPart {
    name: string;
    body: Statement[];
    constructor(name: string, body: Statement[], type?: Type | null, modifiers?: StmtModifier[] | null);
    isEquivalent(m: ClassGetter): boolean;
}
export declare class ClassStmt extends Statement {
    name: string;
    parent: Expression | null;
    fields: ClassField[];
    getters: ClassGetter[];
    constructorMethod: ClassMethod;
    methods: ClassMethod[];
    constructor(name: string, parent: Expression | null, fields: ClassField[], getters: ClassGetter[], constructorMethod: ClassMethod, methods: ClassMethod[], modifiers?: StmtModifier[] | null, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class IfStmt extends Statement {
    condition: Expression;
    trueCase: Statement[];
    falseCase: Statement[];
    constructor(condition: Expression, trueCase: Statement[], falseCase?: Statement[], sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class CommentStmt extends Statement {
    comment: string;
    multiline: boolean;
    constructor(comment: string, multiline?: boolean, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class JSDocCommentStmt extends Statement {
    tags: JSDocTag[];
    constructor(tags?: JSDocTag[], sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
    toString(): string;
}
export declare class TryCatchStmt extends Statement {
    bodyStmts: Statement[];
    catchStmts: Statement[];
    constructor(bodyStmts: Statement[], catchStmts: Statement[], sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: Statement): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export declare class ThrowStmt extends Statement {
    error: Expression;
    constructor(error: Expression, sourceSpan?: ParseSourceSpan | null);
    isEquivalent(stmt: ThrowStmt): boolean;
    visitStatement(visitor: StatementVisitor, context: any): any;
}
export interface StatementVisitor {
    visitDeclareVarStmt(stmt: DeclareVarStmt, context: any): any;
    visitDeclareFunctionStmt(stmt: DeclareFunctionStmt, context: any): any;
    visitExpressionStmt(stmt: ExpressionStatement, context: any): any;
    visitReturnStmt(stmt: ReturnStatement, context: any): any;
    visitDeclareClassStmt(stmt: ClassStmt, context: any): any;
    visitIfStmt(stmt: IfStmt, context: any): any;
    visitTryCatchStmt(stmt: TryCatchStmt, context: any): any;
    visitThrowStmt(stmt: ThrowStmt, context: any): any;
    visitCommentStmt(stmt: CommentStmt, context: any): any;
    visitJSDocCommentStmt(stmt: JSDocCommentStmt, context: any): any;
}
export declare class AstTransformer implements StatementVisitor, ExpressionVisitor {
    transformExpr(expr: Expression, context: any): Expression;
    transformStmt(stmt: Statement, context: any): Statement;
    visitReadVarExpr(ast: ReadVarExpr, context: any): any;
    visitWrappedNodeExpr(ast: WrappedNodeExpr<any>, context: any): any;
    visitTypeofExpr(expr: TypeofExpr, context: any): any;
    visitWriteVarExpr(expr: WriteVarExpr, context: any): any;
    visitWriteKeyExpr(expr: WriteKeyExpr, context: any): any;
    visitWritePropExpr(expr: WritePropExpr, context: any): any;
    visitInvokeMethodExpr(ast: InvokeMethodExpr, context: any): any;
    visitInvokeFunctionExpr(ast: InvokeFunctionExpr, context: any): any;
    visitInstantiateExpr(ast: InstantiateExpr, context: any): any;
    visitLiteralExpr(ast: LiteralExpr, context: any): any;
    visitExternalExpr(ast: ExternalExpr, context: any): any;
    visitConditionalExpr(ast: ConditionalExpr, context: any): any;
    visitNotExpr(ast: NotExpr, context: any): any;
    visitAssertNotNullExpr(ast: AssertNotNull, context: any): any;
    visitCastExpr(ast: CastExpr, context: any): any;
    visitFunctionExpr(ast: FunctionExpr, context: any): any;
    visitBinaryOperatorExpr(ast: BinaryOperatorExpr, context: any): any;
    visitReadPropExpr(ast: ReadPropExpr, context: any): any;
    visitReadKeyExpr(ast: ReadKeyExpr, context: any): any;
    visitLiteralArrayExpr(ast: LiteralArrayExpr, context: any): any;
    visitLiteralMapExpr(ast: LiteralMapExpr, context: any): any;
    visitCommaExpr(ast: CommaExpr, context: any): any;
    visitAllExpressions(exprs: Expression[], context: any): Expression[];
    visitDeclareVarStmt(stmt: DeclareVarStmt, context: any): any;
    visitDeclareFunctionStmt(stmt: DeclareFunctionStmt, context: any): any;
    visitExpressionStmt(stmt: ExpressionStatement, context: any): any;
    visitReturnStmt(stmt: ReturnStatement, context: any): any;
    visitDeclareClassStmt(stmt: ClassStmt, context: any): any;
    visitIfStmt(stmt: IfStmt, context: any): any;
    visitTryCatchStmt(stmt: TryCatchStmt, context: any): any;
    visitThrowStmt(stmt: ThrowStmt, context: any): any;
    visitCommentStmt(stmt: CommentStmt, context: any): any;
    visitJSDocCommentStmt(stmt: JSDocCommentStmt, context: any): any;
    visitAllStatements(stmts: Statement[], context: any): Statement[];
}
export declare class RecursiveAstVisitor implements StatementVisitor, ExpressionVisitor {
    visitType(ast: Type, context: any): any;
    visitExpression(ast: Expression, context: any): any;
    visitBuiltinType(type: BuiltinType, context: any): any;
    visitExpressionType(type: ExpressionType, context: any): any;
    visitArrayType(type: ArrayType, context: any): any;
    visitMapType(type: MapType, context: any): any;
    visitWrappedNodeExpr(ast: WrappedNodeExpr<any>, context: any): any;
    visitTypeofExpr(ast: TypeofExpr, context: any): any;
    visitReadVarExpr(ast: ReadVarExpr, context: any): any;
    visitWriteVarExpr(ast: WriteVarExpr, context: any): any;
    visitWriteKeyExpr(ast: WriteKeyExpr, context: any): any;
    visitWritePropExpr(ast: WritePropExpr, context: any): any;
    visitInvokeMethodExpr(ast: InvokeMethodExpr, context: any): any;
    visitInvokeFunctionExpr(ast: InvokeFunctionExpr, context: any): any;
    visitInstantiateExpr(ast: InstantiateExpr, context: any): any;
    visitLiteralExpr(ast: LiteralExpr, context: any): any;
    visitExternalExpr(ast: ExternalExpr, context: any): any;
    visitConditionalExpr(ast: ConditionalExpr, context: any): any;
    visitNotExpr(ast: NotExpr, context: any): any;
    visitAssertNotNullExpr(ast: AssertNotNull, context: any): any;
    visitCastExpr(ast: CastExpr, context: any): any;
    visitFunctionExpr(ast: FunctionExpr, context: any): any;
    visitBinaryOperatorExpr(ast: BinaryOperatorExpr, context: any): any;
    visitReadPropExpr(ast: ReadPropExpr, context: any): any;
    visitReadKeyExpr(ast: ReadKeyExpr, context: any): any;
    visitLiteralArrayExpr(ast: LiteralArrayExpr, context: any): any;
    visitLiteralMapExpr(ast: LiteralMapExpr, context: any): any;
    visitCommaExpr(ast: CommaExpr, context: any): any;
    visitAllExpressions(exprs: Expression[], context: any): void;
    visitDeclareVarStmt(stmt: DeclareVarStmt, context: any): any;
    visitDeclareFunctionStmt(stmt: DeclareFunctionStmt, context: any): any;
    visitExpressionStmt(stmt: ExpressionStatement, context: any): any;
    visitReturnStmt(stmt: ReturnStatement, context: any): any;
    visitDeclareClassStmt(stmt: ClassStmt, context: any): any;
    visitIfStmt(stmt: IfStmt, context: any): any;
    visitTryCatchStmt(stmt: TryCatchStmt, context: any): any;
    visitThrowStmt(stmt: ThrowStmt, context: any): any;
    visitCommentStmt(stmt: CommentStmt, context: any): any;
    visitJSDocCommentStmt(stmt: JSDocCommentStmt, context: any): any;
    visitAllStatements(stmts: Statement[], context: any): void;
}
export declare function findReadVarNames(stmts: Statement[]): Set<string>;
export declare function collectExternalReferences(stmts: Statement[]): ExternalReference[];
export declare function applySourceSpanToStatementIfNeeded(stmt: Statement, sourceSpan: ParseSourceSpan | null): Statement;
export declare function applySourceSpanToExpressionIfNeeded(expr: Expression, sourceSpan: ParseSourceSpan | null): Expression;
export declare function variable(name: string, type?: Type | null, sourceSpan?: ParseSourceSpan | null): ReadVarExpr;
export declare function importExpr(id: ExternalReference, typeParams?: Type[] | null, sourceSpan?: ParseSourceSpan | null): ExternalExpr;
export declare function importType(id: ExternalReference, typeParams?: Type[] | null, typeModifiers?: TypeModifier[] | null): ExpressionType | null;
export declare function expressionType(expr: Expression, typeModifiers?: TypeModifier[] | null, typeParams?: Type[] | null): ExpressionType;
export declare function typeofExpr(expr: Expression): TypeofExpr;
export declare function literalArr(values: Expression[], type?: Type | null, sourceSpan?: ParseSourceSpan | null): LiteralArrayExpr;
export declare function literalMap(values: {
    key: string;
    quoted: boolean;
    value: Expression;
}[], type?: MapType | null): LiteralMapExpr;
export declare function not(expr: Expression, sourceSpan?: ParseSourceSpan | null): NotExpr;
export declare function assertNotNull(expr: Expression, sourceSpan?: ParseSourceSpan | null): AssertNotNull;
export declare function fn(params: FnParam[], body: Statement[], type?: Type | null, sourceSpan?: ParseSourceSpan | null, name?: string | null): FunctionExpr;
export declare function ifStmt(condition: Expression, thenClause: Statement[], elseClause?: Statement[]): IfStmt;
export declare function literal(value: any, type?: Type | null, sourceSpan?: ParseSourceSpan | null): LiteralExpr;
export declare function isNull(exp: Expression): boolean;
export declare const enum JSDocTagName {
    Desc = "desc",
    Id = "id",
    Meaning = "meaning"
}
export declare type JSDocTag = {
    tagName: JSDocTagName | string;
    text?: string;
} | {
    tagName?: undefined;
    text: string;
};
