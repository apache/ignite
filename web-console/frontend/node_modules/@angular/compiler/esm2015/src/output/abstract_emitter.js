/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as o from './output_ast';
import { SourceMapGenerator } from './source_map';
const _SINGLE_QUOTE_ESCAPE_STRING_RE = /'|\\|\n|\r|\$/g;
const _LEGAL_IDENTIFIER_RE = /^[$A-Z_][0-9A-Z_$]*$/i;
const _INDENT_WITH = '  ';
export const CATCH_ERROR_VAR = o.variable('error', null, null);
export const CATCH_STACK_VAR = o.variable('stack', null, null);
class _EmittedLine {
    constructor(indent) {
        this.indent = indent;
        this.partsLength = 0;
        this.parts = [];
        this.srcSpans = [];
    }
}
export class EmitterVisitorContext {
    constructor(_indent) {
        this._indent = _indent;
        this._classes = [];
        this._preambleLineCount = 0;
        this._lines = [new _EmittedLine(_indent)];
    }
    static createRoot() { return new EmitterVisitorContext(0); }
    get _currentLine() { return this._lines[this._lines.length - 1]; }
    println(from, lastPart = '') {
        this.print(from || null, lastPart, true);
    }
    lineIsEmpty() { return this._currentLine.parts.length === 0; }
    lineLength() {
        return this._currentLine.indent * _INDENT_WITH.length + this._currentLine.partsLength;
    }
    print(from, part, newLine = false) {
        if (part.length > 0) {
            this._currentLine.parts.push(part);
            this._currentLine.partsLength += part.length;
            this._currentLine.srcSpans.push(from && from.sourceSpan || null);
        }
        if (newLine) {
            this._lines.push(new _EmittedLine(this._indent));
        }
    }
    removeEmptyLastLine() {
        if (this.lineIsEmpty()) {
            this._lines.pop();
        }
    }
    incIndent() {
        this._indent++;
        if (this.lineIsEmpty()) {
            this._currentLine.indent = this._indent;
        }
    }
    decIndent() {
        this._indent--;
        if (this.lineIsEmpty()) {
            this._currentLine.indent = this._indent;
        }
    }
    pushClass(clazz) { this._classes.push(clazz); }
    popClass() { return this._classes.pop(); }
    get currentClass() {
        return this._classes.length > 0 ? this._classes[this._classes.length - 1] : null;
    }
    toSource() {
        return this.sourceLines
            .map(l => l.parts.length > 0 ? _createIndent(l.indent) + l.parts.join('') : '')
            .join('\n');
    }
    toSourceMapGenerator(genFilePath, startsAtLine = 0) {
        const map = new SourceMapGenerator(genFilePath);
        let firstOffsetMapped = false;
        const mapFirstOffsetIfNeeded = () => {
            if (!firstOffsetMapped) {
                // Add a single space so that tools won't try to load the file from disk.
                // Note: We are using virtual urls like `ng:///`, so we have to
                // provide a content here.
                map.addSource(genFilePath, ' ').addMapping(0, genFilePath, 0, 0);
                firstOffsetMapped = true;
            }
        };
        for (let i = 0; i < startsAtLine; i++) {
            map.addLine();
            mapFirstOffsetIfNeeded();
        }
        this.sourceLines.forEach((line, lineIdx) => {
            map.addLine();
            const spans = line.srcSpans;
            const parts = line.parts;
            let col0 = line.indent * _INDENT_WITH.length;
            let spanIdx = 0;
            // skip leading parts without source spans
            while (spanIdx < spans.length && !spans[spanIdx]) {
                col0 += parts[spanIdx].length;
                spanIdx++;
            }
            if (spanIdx < spans.length && lineIdx === 0 && col0 === 0) {
                firstOffsetMapped = true;
            }
            else {
                mapFirstOffsetIfNeeded();
            }
            while (spanIdx < spans.length) {
                const span = spans[spanIdx];
                const source = span.start.file;
                const sourceLine = span.start.line;
                const sourceCol = span.start.col;
                map.addSource(source.url, source.content)
                    .addMapping(col0, source.url, sourceLine, sourceCol);
                col0 += parts[spanIdx].length;
                spanIdx++;
                // assign parts without span or the same span to the previous segment
                while (spanIdx < spans.length && (span === spans[spanIdx] || !spans[spanIdx])) {
                    col0 += parts[spanIdx].length;
                    spanIdx++;
                }
            }
        });
        return map;
    }
    setPreambleLineCount(count) { return this._preambleLineCount = count; }
    spanOf(line, column) {
        const emittedLine = this._lines[line - this._preambleLineCount];
        if (emittedLine) {
            let columnsLeft = column - _createIndent(emittedLine.indent).length;
            for (let partIndex = 0; partIndex < emittedLine.parts.length; partIndex++) {
                const part = emittedLine.parts[partIndex];
                if (part.length > columnsLeft) {
                    return emittedLine.srcSpans[partIndex];
                }
                columnsLeft -= part.length;
            }
        }
        return null;
    }
    get sourceLines() {
        if (this._lines.length && this._lines[this._lines.length - 1].parts.length === 0) {
            return this._lines.slice(0, -1);
        }
        return this._lines;
    }
}
export class AbstractEmitterVisitor {
    constructor(_escapeDollarInStrings) {
        this._escapeDollarInStrings = _escapeDollarInStrings;
    }
    visitExpressionStmt(stmt, ctx) {
        stmt.expr.visitExpression(this, ctx);
        ctx.println(stmt, ';');
        return null;
    }
    visitReturnStmt(stmt, ctx) {
        ctx.print(stmt, `return `);
        stmt.value.visitExpression(this, ctx);
        ctx.println(stmt, ';');
        return null;
    }
    visitIfStmt(stmt, ctx) {
        ctx.print(stmt, `if (`);
        stmt.condition.visitExpression(this, ctx);
        ctx.print(stmt, `) {`);
        const hasElseCase = stmt.falseCase != null && stmt.falseCase.length > 0;
        if (stmt.trueCase.length <= 1 && !hasElseCase) {
            ctx.print(stmt, ` `);
            this.visitAllStatements(stmt.trueCase, ctx);
            ctx.removeEmptyLastLine();
            ctx.print(stmt, ` `);
        }
        else {
            ctx.println();
            ctx.incIndent();
            this.visitAllStatements(stmt.trueCase, ctx);
            ctx.decIndent();
            if (hasElseCase) {
                ctx.println(stmt, `} else {`);
                ctx.incIndent();
                this.visitAllStatements(stmt.falseCase, ctx);
                ctx.decIndent();
            }
        }
        ctx.println(stmt, `}`);
        return null;
    }
    visitThrowStmt(stmt, ctx) {
        ctx.print(stmt, `throw `);
        stmt.error.visitExpression(this, ctx);
        ctx.println(stmt, `;`);
        return null;
    }
    visitCommentStmt(stmt, ctx) {
        if (stmt.multiline) {
            ctx.println(stmt, `/* ${stmt.comment} */`);
        }
        else {
            stmt.comment.split('\n').forEach((line) => { ctx.println(stmt, `// ${line}`); });
        }
        return null;
    }
    visitJSDocCommentStmt(stmt, ctx) {
        ctx.println(stmt, `/*${stmt.toString()}*/`);
        return null;
    }
    visitWriteVarExpr(expr, ctx) {
        const lineWasEmpty = ctx.lineIsEmpty();
        if (!lineWasEmpty) {
            ctx.print(expr, '(');
        }
        ctx.print(expr, `${expr.name} = `);
        expr.value.visitExpression(this, ctx);
        if (!lineWasEmpty) {
            ctx.print(expr, ')');
        }
        return null;
    }
    visitWriteKeyExpr(expr, ctx) {
        const lineWasEmpty = ctx.lineIsEmpty();
        if (!lineWasEmpty) {
            ctx.print(expr, '(');
        }
        expr.receiver.visitExpression(this, ctx);
        ctx.print(expr, `[`);
        expr.index.visitExpression(this, ctx);
        ctx.print(expr, `] = `);
        expr.value.visitExpression(this, ctx);
        if (!lineWasEmpty) {
            ctx.print(expr, ')');
        }
        return null;
    }
    visitWritePropExpr(expr, ctx) {
        const lineWasEmpty = ctx.lineIsEmpty();
        if (!lineWasEmpty) {
            ctx.print(expr, '(');
        }
        expr.receiver.visitExpression(this, ctx);
        ctx.print(expr, `.${expr.name} = `);
        expr.value.visitExpression(this, ctx);
        if (!lineWasEmpty) {
            ctx.print(expr, ')');
        }
        return null;
    }
    visitInvokeMethodExpr(expr, ctx) {
        expr.receiver.visitExpression(this, ctx);
        let name = expr.name;
        if (expr.builtin != null) {
            name = this.getBuiltinMethodName(expr.builtin);
            if (name == null) {
                // some builtins just mean to skip the call.
                return null;
            }
        }
        ctx.print(expr, `.${name}(`);
        this.visitAllExpressions(expr.args, ctx, `,`);
        ctx.print(expr, `)`);
        return null;
    }
    visitInvokeFunctionExpr(expr, ctx) {
        expr.fn.visitExpression(this, ctx);
        ctx.print(expr, `(`);
        this.visitAllExpressions(expr.args, ctx, ',');
        ctx.print(expr, `)`);
        return null;
    }
    visitWrappedNodeExpr(ast, ctx) {
        throw new Error('Abstract emitter cannot visit WrappedNodeExpr.');
    }
    visitTypeofExpr(expr, ctx) {
        ctx.print(expr, 'typeof ');
        expr.expr.visitExpression(this, ctx);
    }
    visitReadVarExpr(ast, ctx) {
        let varName = ast.name;
        if (ast.builtin != null) {
            switch (ast.builtin) {
                case o.BuiltinVar.Super:
                    varName = 'super';
                    break;
                case o.BuiltinVar.This:
                    varName = 'this';
                    break;
                case o.BuiltinVar.CatchError:
                    varName = CATCH_ERROR_VAR.name;
                    break;
                case o.BuiltinVar.CatchStack:
                    varName = CATCH_STACK_VAR.name;
                    break;
                default:
                    throw new Error(`Unknown builtin variable ${ast.builtin}`);
            }
        }
        ctx.print(ast, varName);
        return null;
    }
    visitInstantiateExpr(ast, ctx) {
        ctx.print(ast, `new `);
        ast.classExpr.visitExpression(this, ctx);
        ctx.print(ast, `(`);
        this.visitAllExpressions(ast.args, ctx, ',');
        ctx.print(ast, `)`);
        return null;
    }
    visitLiteralExpr(ast, ctx) {
        const value = ast.value;
        if (typeof value === 'string') {
            ctx.print(ast, escapeIdentifier(value, this._escapeDollarInStrings));
        }
        else {
            ctx.print(ast, `${value}`);
        }
        return null;
    }
    visitConditionalExpr(ast, ctx) {
        ctx.print(ast, `(`);
        ast.condition.visitExpression(this, ctx);
        ctx.print(ast, '? ');
        ast.trueCase.visitExpression(this, ctx);
        ctx.print(ast, ': ');
        ast.falseCase.visitExpression(this, ctx);
        ctx.print(ast, `)`);
        return null;
    }
    visitNotExpr(ast, ctx) {
        ctx.print(ast, '!');
        ast.condition.visitExpression(this, ctx);
        return null;
    }
    visitAssertNotNullExpr(ast, ctx) {
        ast.condition.visitExpression(this, ctx);
        return null;
    }
    visitBinaryOperatorExpr(ast, ctx) {
        let opStr;
        switch (ast.operator) {
            case o.BinaryOperator.Equals:
                opStr = '==';
                break;
            case o.BinaryOperator.Identical:
                opStr = '===';
                break;
            case o.BinaryOperator.NotEquals:
                opStr = '!=';
                break;
            case o.BinaryOperator.NotIdentical:
                opStr = '!==';
                break;
            case o.BinaryOperator.And:
                opStr = '&&';
                break;
            case o.BinaryOperator.BitwiseAnd:
                opStr = '&';
                break;
            case o.BinaryOperator.Or:
                opStr = '||';
                break;
            case o.BinaryOperator.Plus:
                opStr = '+';
                break;
            case o.BinaryOperator.Minus:
                opStr = '-';
                break;
            case o.BinaryOperator.Divide:
                opStr = '/';
                break;
            case o.BinaryOperator.Multiply:
                opStr = '*';
                break;
            case o.BinaryOperator.Modulo:
                opStr = '%';
                break;
            case o.BinaryOperator.Lower:
                opStr = '<';
                break;
            case o.BinaryOperator.LowerEquals:
                opStr = '<=';
                break;
            case o.BinaryOperator.Bigger:
                opStr = '>';
                break;
            case o.BinaryOperator.BiggerEquals:
                opStr = '>=';
                break;
            default:
                throw new Error(`Unknown operator ${ast.operator}`);
        }
        if (ast.parens)
            ctx.print(ast, `(`);
        ast.lhs.visitExpression(this, ctx);
        ctx.print(ast, ` ${opStr} `);
        ast.rhs.visitExpression(this, ctx);
        if (ast.parens)
            ctx.print(ast, `)`);
        return null;
    }
    visitReadPropExpr(ast, ctx) {
        ast.receiver.visitExpression(this, ctx);
        ctx.print(ast, `.`);
        ctx.print(ast, ast.name);
        return null;
    }
    visitReadKeyExpr(ast, ctx) {
        ast.receiver.visitExpression(this, ctx);
        ctx.print(ast, `[`);
        ast.index.visitExpression(this, ctx);
        ctx.print(ast, `]`);
        return null;
    }
    visitLiteralArrayExpr(ast, ctx) {
        ctx.print(ast, `[`);
        this.visitAllExpressions(ast.entries, ctx, ',');
        ctx.print(ast, `]`);
        return null;
    }
    visitLiteralMapExpr(ast, ctx) {
        ctx.print(ast, `{`);
        this.visitAllObjects(entry => {
            ctx.print(ast, `${escapeIdentifier(entry.key, this._escapeDollarInStrings, entry.quoted)}:`);
            entry.value.visitExpression(this, ctx);
        }, ast.entries, ctx, ',');
        ctx.print(ast, `}`);
        return null;
    }
    visitCommaExpr(ast, ctx) {
        ctx.print(ast, '(');
        this.visitAllExpressions(ast.parts, ctx, ',');
        ctx.print(ast, ')');
        return null;
    }
    visitAllExpressions(expressions, ctx, separator) {
        this.visitAllObjects(expr => expr.visitExpression(this, ctx), expressions, ctx, separator);
    }
    visitAllObjects(handler, expressions, ctx, separator) {
        let incrementedIndent = false;
        for (let i = 0; i < expressions.length; i++) {
            if (i > 0) {
                if (ctx.lineLength() > 80) {
                    ctx.print(null, separator, true);
                    if (!incrementedIndent) {
                        // continuation are marked with double indent.
                        ctx.incIndent();
                        ctx.incIndent();
                        incrementedIndent = true;
                    }
                }
                else {
                    ctx.print(null, separator, false);
                }
            }
            handler(expressions[i]);
        }
        if (incrementedIndent) {
            // continuation are marked with double indent.
            ctx.decIndent();
            ctx.decIndent();
        }
    }
    visitAllStatements(statements, ctx) {
        statements.forEach((stmt) => stmt.visitStatement(this, ctx));
    }
}
export function escapeIdentifier(input, escapeDollar, alwaysQuote = true) {
    if (input == null) {
        return null;
    }
    const body = input.replace(_SINGLE_QUOTE_ESCAPE_STRING_RE, (...match) => {
        if (match[0] == '$') {
            return escapeDollar ? '\\$' : '$';
        }
        else if (match[0] == '\n') {
            return '\\n';
        }
        else if (match[0] == '\r') {
            return '\\r';
        }
        else {
            return `\\${match[0]}`;
        }
    });
    const requiresQuotes = alwaysQuote || !_LEGAL_IDENTIFIER_RE.test(body);
    return requiresQuotes ? `'${body}'` : body;
}
function _createIndent(count) {
    let res = '';
    for (let i = 0; i < count; i++) {
        res += _INDENT_WITH;
    }
    return res;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYWJzdHJhY3RfZW1pdHRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9vdXRwdXQvYWJzdHJhY3RfZW1pdHRlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFJSCxPQUFPLEtBQUssQ0FBQyxNQUFNLGNBQWMsQ0FBQztBQUNsQyxPQUFPLEVBQUMsa0JBQWtCLEVBQUMsTUFBTSxjQUFjLENBQUM7QUFFaEQsTUFBTSw4QkFBOEIsR0FBRyxnQkFBZ0IsQ0FBQztBQUN4RCxNQUFNLG9CQUFvQixHQUFHLHVCQUF1QixDQUFDO0FBQ3JELE1BQU0sWUFBWSxHQUFHLElBQUksQ0FBQztBQUMxQixNQUFNLENBQUMsTUFBTSxlQUFlLEdBQUcsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxPQUFPLEVBQUUsSUFBSSxFQUFFLElBQUksQ0FBQyxDQUFDO0FBQy9ELE1BQU0sQ0FBQyxNQUFNLGVBQWUsR0FBRyxDQUFDLENBQUMsUUFBUSxDQUFDLE9BQU8sRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDLENBQUM7QUFNL0QsTUFBTSxZQUFZO0lBSWhCLFlBQW1CLE1BQWM7UUFBZCxXQUFNLEdBQU4sTUFBTSxDQUFRO1FBSGpDLGdCQUFXLEdBQUcsQ0FBQyxDQUFDO1FBQ2hCLFVBQUssR0FBYSxFQUFFLENBQUM7UUFDckIsYUFBUSxHQUE2QixFQUFFLENBQUM7SUFDSixDQUFDO0NBQ3RDO0FBRUQsTUFBTSxPQUFPLHFCQUFxQjtJQU9oQyxZQUFvQixPQUFlO1FBQWYsWUFBTyxHQUFQLE9BQU8sQ0FBUTtRQUgzQixhQUFRLEdBQWtCLEVBQUUsQ0FBQztRQUM3Qix1QkFBa0IsR0FBRyxDQUFDLENBQUM7UUFFUSxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsSUFBSSxZQUFZLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQztJQUFDLENBQUM7SUFObkYsTUFBTSxDQUFDLFVBQVUs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QUFDLEtBQWE7SUFDbEMsSUFBSSxHQUFHLEdBQUcsRUFBRSxDQUFDO0lBQ2IsS0FBSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxHQUFHLEtBQUssRUFBRSxDQUFDLEVBQUUsRUFBRTtRQUM5QixHQUFHLElBQUksWUFBWSxDQUFDO0tBQ3JCO0lBQ0QsT0FBTyxHQUFHLENBQUM7QUFDYixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1BhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5cbmltcG9ydCAqIGFzIG8gZnJvbSAnLi9vdXRwdXRfYXN0JztcbmltcG9ydCB7U291cmNlTWFwR2VuZXJhdG9yfSBmcm9tICcuL3NvdXJjZV9tYXAnO1xuXG5jb25zdCBfU0lOR0xFX1FVT1RFX0VTQ0FQRV9TVFJJTkdfUkUgPSAvJ3xcXFxcfFxcbnxcXHJ8XFwkL2c7XG5jb25zdCBfTEVHQUxfSURFTlRJRklFUl9SRSA9IC9eWyRBLVpfXVswLTlBLVpfJF0qJC9pO1xuY29uc3QgX0lOREVOVF9XSVRIID0gJyAgJztcbmV4cG9ydCBjb25zdCBDQVRDSF9FUlJPUl9WQVIgPSBvLnZhcmlhYmxlKCdlcnJvcicsIG51bGwsIG51bGwpO1xuZXhwb3J0IGNvbnN0IENBVENIX1NUQUNLX1ZBUiA9IG8udmFyaWFibGUoJ3N0YWNrJywgbnVsbCwgbnVsbCk7XG5cbmV4cG9ydCBpbnRlcmZhY2UgT3V0cHV0RW1pdHRlciB7XG4gIGVtaXRTdGF0ZW1lbnRzKGdlbkZpbGVQYXRoOiBzdHJpbmcsIHN0bXRzOiBvLlN0YXRlbWVudFtdLCBwcmVhbWJsZT86IHN0cmluZ3xudWxsKTogc3RyaW5nO1xufVxuXG5jbGFzcyBfRW1pdHRlZExpbmUge1xuICBwYXJ0c0xlbmd0aCA9IDA7XG4gIHBhcnRzOiBzdHJpbmdbXSA9IFtdO1xuICBzcmNTcGFuczogKFBhcnNlU291cmNlU3BhbnxudWxsKVtdID0gW107XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBpbmRlbnQ6IG51bWJlcikge31cbn1cblxuZXhwb3J0IGNsYXNzIEVtaXR0ZXJWaXNpdG9yQ29udGV4dCB7XG4gIHN0YXRpYyBjcmVhdGVSb290KCk6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCB7IHJldHVybiBuZXcgRW1pdHRlclZpc2l0b3JDb250ZXh0KDApOyB9XG5cbiAgcHJpdmF0ZSBfbGluZXM6IF9FbWl0dGVkTGluZVtdO1xuICBwcml2YXRlIF9jbGFzc2VzOiBvLkNsYXNzU3RtdFtdID0gW107XG4gIHByaXZhdGUgX3ByZWFtYmxlTGluZUNvdW50ID0gMDtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIF9pbmRlbnQ6IG51bWJlcikgeyB0aGlzLl9saW5lcyA9IFtuZXcgX0VtaXR0ZWRMaW5lKF9pbmRlbnQpXTsgfVxuXG4gIHByaXZhdGUgZ2V0IF9jdXJyZW50TGluZSgpOiBfRW1pdHRlZExpbmUgeyByZXR1cm4gdGhpcy5fbGluZXNbdGhpcy5fbGluZXMubGVuZ3RoIC0gMV07IH1cblxuICBwcmludGxuKGZyb20/OiB7c291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuIHwgbnVsbH18bnVsbCwgbGFzdFBhcnQ6IHN0cmluZyA9ICcnKTogdm9pZCB7XG4gICAgdGhpcy5wcmludChmcm9tIHx8IG51bGwsIGxhc3RQYXJ0LCB0cnVlKTtcbiAgfVxuXG4gIGxpbmVJc0VtcHR5KCk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5fY3VycmVudExpbmUucGFydHMubGVuZ3RoID09PSAwOyB9XG5cbiAgbGluZUxlbmd0aCgpOiBudW1iZXIge1xuICAgIHJldHVybiB0aGlzLl9jdXJyZW50TGluZS5pbmRlbnQgKiBfSU5ERU5UX1dJVEgubGVuZ3RoICsgdGhpcy5fY3VycmVudExpbmUucGFydHNMZW5ndGg7XG4gIH1cblxuICBwcmludChmcm9tOiB7c291cmNlU3BhbjogUGFyc2VTb3VyY2VTcGFuIHwgbnVsbH18bnVsbCwgcGFydDogc3RyaW5nLCBuZXdMaW5lOiBib29sZWFuID0gZmFsc2UpIHtcbiAgICBpZiAocGFydC5sZW5ndGggPiAwKSB7XG4gICAgICB0aGlzLl9jdXJyZW50TGluZS5wYXJ0cy5wdXNoKHBhcnQpO1xuICAgICAgdGhpcy5fY3VycmVudExpbmUucGFydHNMZW5ndGggKz0gcGFydC5sZW5ndGg7XG4gICAgICB0aGlzLl9jdXJyZW50TGluZS5zcmNTcGFucy5wdXNoKGZyb20gJiYgZnJvbS5zb3VyY2VTcGFuIHx8IG51bGwpO1xuICAgIH1cbiAgICBpZiAobmV3TGluZSkge1xuICAgICAgdGhpcy5fbGluZXMucHVzaChuZXcgX0VtaXR0ZWRMaW5lKHRoaXMuX2luZGVudCkpO1xuICAgIH1cbiAgfVxuXG4gIHJlbW92ZUVtcHR5TGFzdExpbmUoKSB7XG4gICAgaWYgKHRoaXMubGluZUlzRW1wdHkoKSkge1xuICAgICAgdGhpcy5fbGluZXMucG9wKCk7XG4gICAgfVxuICB9XG5cbiAgaW5jSW5kZW50KCkge1xuICAgIHRoaXMuX2luZGVudCsrO1xuICAgIGlmICh0aGlzLmxpbmVJc0VtcHR5KCkpIHtcbiAgICAgIHRoaXMuX2N1cnJlbnRMaW5lLmluZGVudCA9IHRoaXMuX2luZGVudDtcbiAgICB9XG4gIH1cblxuICBkZWNJbmRlbnQoKSB7XG4gICAgdGhpcy5faW5kZW50LS07XG4gICAgaWYgKHRoaXMubGluZUlzRW1wdHkoKSkge1xuICAgICAgdGhpcy5fY3VycmVudExpbmUuaW5kZW50ID0gdGhpcy5faW5kZW50O1xuICAgIH1cbiAgfVxuXG4gIHB1c2hDbGFzcyhjbGF6ejogby5DbGFzc1N0bXQpIHsgdGhpcy5fY2xhc3Nlcy5wdXNoKGNsYXp6KTsgfVxuXG4gIHBvcENsYXNzKCk6IG8uQ2xhc3NTdG10IHsgcmV0dXJuIHRoaXMuX2NsYXNzZXMucG9wKCkgITsgfVxuXG4gIGdldCBjdXJyZW50Q2xhc3MoKTogby5DbGFzc1N0bXR8bnVsbCB7XG4gICAgcmV0dXJuIHRoaXMuX2NsYXNzZXMubGVuZ3RoID4gMCA/IHRoaXMuX2NsYXNzZXNbdGhpcy5fY2xhc3Nlcy5sZW5ndGggLSAxXSA6IG51bGw7XG4gIH1cblxuICB0b1NvdXJjZSgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLnNvdXJjZUxpbmVzXG4gICAgICAgIC5tYXAobCA9PiBsLnBhcnRzLmxlbmd0aCA+IDAgPyBfY3JlYXRlSW5kZW50KGwuaW5kZW50KSArIGwucGFydHMuam9pbignJykgOiAnJylcbiAgICAgICAgLmpvaW4oJ1xcbicpO1xuICB9XG5cbiAgdG9Tb3VyY2VNYXBHZW5lcmF0b3IoZ2VuRmlsZVBhdGg6IHN0cmluZywgc3RhcnRzQXRMaW5lOiBudW1iZXIgPSAwKTogU291cmNlTWFwR2VuZXJhdG9yIHtcbiAgICBjb25zdCBtYXAgPSBuZXcgU291cmNlTWFwR2VuZXJhdG9yKGdlbkZpbGVQYXRoKTtcblxuICAgIGxldCBmaXJzdE9mZnNldE1hcHBlZCA9IGZhbHNlO1xuICAgIGNvbnN0IG1hcEZpcnN0T2Zmc2V0SWZOZWVkZWQgPSAoKSA9PiB7XG4gICAgICBpZiAoIWZpcnN0T2Zmc2V0TWFwcGVkKSB7XG4gICAgICAgIC8vIEFkZCBhIHNpbmdsZSBzcGFjZSBzbyB0aGF0IHRvb2xzIHdvbid0IHRyeSB0byBsb2FkIHRoZSBmaWxlIGZyb20gZGlzay5cbiAgICAgICAgLy8gTm90ZTogV2UgYXJlIHVzaW5nIHZpcnR1YWwgdXJscyBsaWtlIGBuZzovLy9gLCBzbyB3ZSBoYXZlIHRvXG4gICAgICAgIC8vIHByb3ZpZGUgYSBjb250ZW50IGhlcmUuXG4gICAgICAgIG1hcC5hZGRTb3VyY2UoZ2VuRmlsZVBhdGgsICcgJykuYWRkTWFwcGluZygwLCBnZW5GaWxlUGF0aCwgMCwgMCk7XG4gICAgICAgIGZpcnN0T2Zmc2V0TWFwcGVkID0gdHJ1ZTtcbiAgICAgIH1cbiAgICB9O1xuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBzdGFydHNBdExpbmU7IGkrKykge1xuICAgICAgbWFwLmFkZExpbmUoKTtcbiAgICAgIG1hcEZpcnN0T2Zmc2V0SWZOZWVkZWQoKTtcbiAgICB9XG5cbiAgICB0aGlzLnNvdXJjZUxpbmVzLmZvckVhY2goKGxpbmUsIGxpbmVJZHgpID0+IHtcbiAgICAgIG1hcC5hZGRMaW5lKCk7XG5cbiAgICAgIGNvbnN0IHNwYW5zID0gbGluZS5zcmNTcGFucztcbiAgICAgIGNvbnN0IHBhcnRzID0gbGluZS5wYXJ0cztcbiAgICAgIGxldCBjb2wwID0gbGluZS5pbmRlbnQgKiBfSU5ERU5UX1dJVEgubGVuZ3RoO1xuICAgICAgbGV0IHNwYW5JZHggPSAwO1xuICAgICAgLy8gc2tpcCBsZWFkaW5nIHBhcnRzIHdpdGhvdXQgc291cmNlIHNwYW5zXG4gICAgICB3aGlsZSAoc3BhbklkeCA8IHNwYW5zLmxlbmd0aCAmJiAhc3BhbnNbc3BhbklkeF0pIHtcbiAgICAgICAgY29sMCArPSBwYXJ0c1tzcGFuSWR4XS5sZW5ndGg7XG4gICAgICAgIHNwYW5JZHgrKztcbiAgICAgIH1cbiAgICAgIGlmIChzcGFuSWR4IDwgc3BhbnMubGVuZ3RoICYmIGxpbmVJZHggPT09IDAgJiYgY29sMCA9PT0gMCkge1xuICAgICAgICBmaXJzdE9mZnNldE1hcHBlZCA9IHRydWU7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBtYXBGaXJzdE9mZnNldElmTmVlZGVkKCk7XG4gICAgICB9XG5cbiAgICAgIHdoaWxlIChzcGFuSWR4IDwgc3BhbnMubGVuZ3RoKSB7XG4gICAgICAgIGNvbnN0IHNwYW4gPSBzcGFuc1tzcGFuSWR4XSAhO1xuICAgICAgICBjb25zdCBzb3VyY2UgPSBzcGFuLnN0YXJ0LmZpbGU7XG4gICAgICAgIGNvbnN0IHNvdXJjZUxpbmUgPSBzcGFuLnN0YXJ0LmxpbmU7XG4gICAgICAgIGNvbnN0IHNvdXJjZUNvbCA9IHNwYW4uc3RhcnQuY29sO1xuICAgICAgICBtYXAuYWRkU291cmNlKHNvdXJjZS51cmwsIHNvdXJjZS5jb250ZW50KVxuICAgICAgICAgICAgLmFkZE1hcHBpbmcoY29sMCwgc291cmNlLnVybCwgc291cmNlTGluZSwgc291cmNlQ29sKTtcblxuICAgICAgICBjb2wwICs9IHBhcnRzW3NwYW5JZHhdLmxlbmd0aDtcbiAgICAgICAgc3BhbklkeCsrO1xuXG4gICAgICAgIC8vIGFzc2lnbiBwYXJ0cyB3aXRob3V0IHNwYW4gb3IgdGhlIHNhbWUgc3BhbiB0byB0aGUgcHJldmlvdXMgc2VnbWVudFxuICAgICAgICB3aGlsZSAoc3BhbklkeCA8IHNwYW5zLmxlbmd0aCAmJiAoc3BhbiA9PT0gc3BhbnNbc3BhbklkeF0gfHwgIXNwYW5zW3NwYW5JZHhdKSkge1xuICAgICAgICAgIGNvbDAgKz0gcGFydHNbc3BhbklkeF0ubGVuZ3RoO1xuICAgICAgICAgIHNwYW5JZHgrKztcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0pO1xuXG4gICAgcmV0dXJuIG1hcDtcbiAgfVxuXG4gIHNldFByZWFtYmxlTGluZUNvdW50KGNvdW50OiBudW1iZXIpIHsgcmV0dXJuIHRoaXMuX3ByZWFtYmxlTGluZUNvdW50ID0gY291bnQ7IH1cblxuICBzcGFuT2YobGluZTogbnVtYmVyLCBjb2x1bW46IG51bWJlcik6IFBhcnNlU291cmNlU3BhbnxudWxsIHtcbiAgICBjb25zdCBlbWl0dGVkTGluZSA9IHRoaXMuX2xpbmVzW2xpbmUgLSB0aGlzLl9wcmVhbWJsZUxpbmVDb3VudF07XG4gICAgaWYgKGVtaXR0ZWRMaW5lKSB7XG4gICAgICBsZXQgY29sdW1uc0xlZnQgPSBjb2x1bW4gLSBfY3JlYXRlSW5kZW50KGVtaXR0ZWRMaW5lLmluZGVudCkubGVuZ3RoO1xuICAgICAgZm9yIChsZXQgcGFydEluZGV4ID0gMDsgcGFydEluZGV4IDwgZW1pdHRlZExpbmUucGFydHMubGVuZ3RoOyBwYXJ0SW5kZXgrKykge1xuICAgICAgICBjb25zdCBwYXJ0ID0gZW1pdHRlZExpbmUucGFydHNbcGFydEluZGV4XTtcbiAgICAgICAgaWYgKHBhcnQubGVuZ3RoID4gY29sdW1uc0xlZnQpIHtcbiAgICAgICAgICByZXR1cm4gZW1pdHRlZExpbmUuc3JjU3BhbnNbcGFydEluZGV4XTtcbiAgICAgICAgfVxuICAgICAgICBjb2x1bW5zTGVmdCAtPSBwYXJ0Lmxlbmd0aDtcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICBwcml2YXRlIGdldCBzb3VyY2VMaW5lcygpOiBfRW1pdHRlZExpbmVbXSB7XG4gICAgaWYgKHRoaXMuX2xpbmVzLmxlbmd0aCAmJiB0aGlzLl9saW5lc1t0aGlzLl9saW5lcy5sZW5ndGggLSAxXS5wYXJ0cy5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiB0aGlzLl9saW5lcy5zbGljZSgwLCAtMSk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLl9saW5lcztcbiAgfVxufVxuXG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgQWJzdHJhY3RFbWl0dGVyVmlzaXRvciBpbXBsZW1lbnRzIG8uU3RhdGVtZW50VmlzaXRvciwgby5FeHByZXNzaW9uVmlzaXRvciB7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgX2VzY2FwZURvbGxhckluU3RyaW5nczogYm9vbGVhbikge31cblxuICB2aXNpdEV4cHJlc3Npb25TdG10KHN0bXQ6IG8uRXhwcmVzc2lvblN0YXRlbWVudCwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIHN0bXQuZXhwci52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBjdHgucHJpbnRsbihzdG10LCAnOycpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgdmlzaXRSZXR1cm5TdG10KHN0bXQ6IG8uUmV0dXJuU3RhdGVtZW50LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgY3R4LnByaW50KHN0bXQsIGByZXR1cm4gYCk7XG4gICAgc3RtdC52YWx1ZS52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBjdHgucHJpbnRsbihzdG10LCAnOycpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgYWJzdHJhY3QgdmlzaXRDYXN0RXhwcihhc3Q6IG8uQ2FzdEV4cHIsIGNvbnRleHQ6IGFueSk6IGFueTtcblxuICBhYnN0cmFjdCB2aXNpdERlY2xhcmVDbGFzc1N0bXQoc3RtdDogby5DbGFzc1N0bXQsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55O1xuXG4gIHZpc2l0SWZTdG10KHN0bXQ6IG8uSWZTdG10LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgY3R4LnByaW50KHN0bXQsIGBpZiAoYCk7XG4gICAgc3RtdC5jb25kaXRpb24udmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgY3R4LnByaW50KHN0bXQsIGApIHtgKTtcbiAgICBjb25zdCBoYXNFbHNlQ2FzZSA9IHN0bXQuZmFsc2VDYXNlICE9IG51bGwgJiYgc3RtdC5mYWxzZUNhc2UubGVuZ3RoID4gMDtcbiAgICBpZiAoc3RtdC50cnVlQ2FzZS5sZW5ndGggPD0gMSAmJiAhaGFzRWxzZUNhc2UpIHtcbiAgICAgIGN0eC5wcmludChzdG10LCBgIGApO1xuICAgICAgdGhpcy52aXNpdEFsbFN0YXRlbWVudHMoc3RtdC50cnVlQ2FzZSwgY3R4KTtcbiAgICAgIGN0eC5yZW1vdmVFbXB0eUxhc3RMaW5lKCk7XG4gICAgICBjdHgucHJpbnQoc3RtdCwgYCBgKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY3R4LnByaW50bG4oKTtcbiAgICAgIGN0eC5pbmNJbmRlbnQoKTtcbiAgICAgIHRoaXMudmlzaXRBbGxTdGF0ZW1lbnRzKHN0bXQudHJ1ZUNhc2UsIGN0eCk7XG4gICAgICBjdHguZGVjSW5kZW50KCk7XG4gICAgICBpZiAoaGFzRWxzZUNhc2UpIHtcbiAgICAgICAgY3R4LnByaW50bG4oc3RtdCwgYH0gZWxzZSB7YCk7XG4gICAgICAgIGN0eC5pbmNJbmRlbnQoKTtcbiAgICAgICAgdGhpcy52aXNpdEFsbFN0YXRlbWVudHMoc3RtdC5mYWxzZUNhc2UsIGN0eCk7XG4gICAgICAgIGN0eC5kZWNJbmRlbnQoKTtcbiAgICAgIH1cbiAgICB9XG4gICAgY3R4LnByaW50bG4oc3RtdCwgYH1gKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGFic3RyYWN0IHZpc2l0VHJ5Q2F0Y2hTdG10KHN0bXQ6IG8uVHJ5Q2F0Y2hTdG10LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueTtcblxuICB2aXNpdFRocm93U3RtdChzdG10OiBvLlRocm93U3RtdCwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIGN0eC5wcmludChzdG10LCBgdGhyb3cgYCk7XG4gICAgc3RtdC5lcnJvci52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBjdHgucHJpbnRsbihzdG10LCBgO2ApO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0Q29tbWVudFN0bXQoc3RtdDogby5Db21tZW50U3RtdCwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIGlmIChzdG10Lm11bHRpbGluZSkge1xuICAgICAgY3R4LnByaW50bG4oc3RtdCwgYC8qICR7c3RtdC5jb21tZW50fSAqL2ApO1xuICAgIH0gZWxzZSB7XG4gICAgICBzdG10LmNvbW1lbnQuc3BsaXQoJ1xcbicpLmZvckVhY2goKGxpbmUpID0+IHsgY3R4LnByaW50bG4oc3RtdCwgYC8vICR7bGluZX1gKTsgfSk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0SlNEb2NDb21tZW50U3RtdChzdG10OiBvLkpTRG9jQ29tbWVudFN0bXQsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KSB7XG4gICAgY3R4LnByaW50bG4oc3RtdCwgYC8qJHtzdG10LnRvU3RyaW5nKCl9Ki9gKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGFic3RyYWN0IHZpc2l0RGVjbGFyZVZhclN0bXQoc3RtdDogby5EZWNsYXJlVmFyU3RtdCwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnk7XG5cbiAgdmlzaXRXcml0ZVZhckV4cHIoZXhwcjogby5Xcml0ZVZhckV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBjb25zdCBsaW5lV2FzRW1wdHkgPSBjdHgubGluZUlzRW1wdHkoKTtcbiAgICBpZiAoIWxpbmVXYXNFbXB0eSkge1xuICAgICAgY3R4LnByaW50KGV4cHIsICcoJyk7XG4gICAgfVxuICAgIGN0eC5wcmludChleHByLCBgJHtleHByLm5hbWV9ID0gYCk7XG4gICAgZXhwci52YWx1ZS52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBpZiAoIWxpbmVXYXNFbXB0eSkge1xuICAgICAgY3R4LnByaW50KGV4cHIsICcpJyk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0V3JpdGVLZXlFeHByKGV4cHI6IG8uV3JpdGVLZXlFeHByLCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgY29uc3QgbGluZVdhc0VtcHR5ID0gY3R4LmxpbmVJc0VtcHR5KCk7XG4gICAgaWYgKCFsaW5lV2FzRW1wdHkpIHtcbiAgICAgIGN0eC5wcmludChleHByLCAnKCcpO1xuICAgIH1cbiAgICBleHByLnJlY2VpdmVyLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICAgIGN0eC5wcmludChleHByLCBgW2ApO1xuICAgIGV4cHIuaW5kZXgudmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgY3R4LnByaW50KGV4cHIsIGBdID0gYCk7XG4gICAgZXhwci52YWx1ZS52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBpZiAoIWxpbmVXYXNFbXB0eSkge1xuICAgICAgY3R4LnByaW50KGV4cHIsICcpJyk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0V3JpdGVQcm9wRXhwcihleHByOiBvLldyaXRlUHJvcEV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBjb25zdCBsaW5lV2FzRW1wdHkgPSBjdHgubGluZUlzRW1wdHkoKTtcbiAgICBpZiAoIWxpbmVXYXNFbXB0eSkge1xuICAgICAgY3R4LnByaW50KGV4cHIsICcoJyk7XG4gICAgfVxuICAgIGV4cHIucmVjZWl2ZXIudmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgY3R4LnByaW50KGV4cHIsIGAuJHtleHByLm5hbWV9ID0gYCk7XG4gICAgZXhwci52YWx1ZS52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBpZiAoIWxpbmVXYXNFbXB0eSkge1xuICAgICAgY3R4LnByaW50KGV4cHIsICcpJyk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0SW52b2tlTWV0aG9kRXhwcihleHByOiBvLkludm9rZU1ldGhvZEV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBleHByLnJlY2VpdmVyLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICAgIGxldCBuYW1lID0gZXhwci5uYW1lO1xuICAgIGlmIChleHByLmJ1aWx0aW4gIT0gbnVsbCkge1xuICAgICAgbmFtZSA9IHRoaXMuZ2V0QnVpbHRpbk1ldGhvZE5hbWUoZXhwci5idWlsdGluKTtcbiAgICAgIGlmIChuYW1lID09IG51bGwpIHtcbiAgICAgICAgLy8gc29tZSBidWlsdGlucyBqdXN0IG1lYW4gdG8gc2tpcCB0aGUgY2FsbC5cbiAgICAgICAgcmV0dXJuIG51bGw7XG4gICAgICB9XG4gICAgfVxuICAgIGN0eC5wcmludChleHByLCBgLiR7bmFtZX0oYCk7XG4gICAgdGhpcy52aXNpdEFsbEV4cHJlc3Npb25zKGV4cHIuYXJncywgY3R4LCBgLGApO1xuICAgIGN0eC5wcmludChleHByLCBgKWApO1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgYWJzdHJhY3QgZ2V0QnVpbHRpbk1ldGhvZE5hbWUobWV0aG9kOiBvLkJ1aWx0aW5NZXRob2QpOiBzdHJpbmc7XG5cbiAgdmlzaXRJbnZva2VGdW5jdGlvbkV4cHIoZXhwcjogby5JbnZva2VGdW5jdGlvbkV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBleHByLmZuLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICAgIGN0eC5wcmludChleHByLCBgKGApO1xuICAgIHRoaXMudmlzaXRBbGxFeHByZXNzaW9ucyhleHByLmFyZ3MsIGN0eCwgJywnKTtcbiAgICBjdHgucHJpbnQoZXhwciwgYClgKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICB2aXNpdFdyYXBwZWROb2RlRXhwcihhc3Q6IG8uV3JhcHBlZE5vZGVFeHByPGFueT4sIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ0Fic3RyYWN0IGVtaXR0ZXIgY2Fubm90IHZpc2l0IFdyYXBwZWROb2RlRXhwci4nKTtcbiAgfVxuICB2aXNpdFR5cGVvZkV4cHIoZXhwcjogby5UeXBlb2ZFeHByLCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgY3R4LnByaW50KGV4cHIsICd0eXBlb2YgJyk7XG4gICAgZXhwci5leHByLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICB9XG4gIHZpc2l0UmVhZFZhckV4cHIoYXN0OiBvLlJlYWRWYXJFeHByLCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgbGV0IHZhck5hbWUgPSBhc3QubmFtZSAhO1xuICAgIGlmIChhc3QuYnVpbHRpbiAhPSBudWxsKSB7XG4gICAgICBzd2l0Y2ggKGFzdC5idWlsdGluKSB7XG4gICAgICAgIGNhc2Ugby5CdWlsdGluVmFyLlN1cGVyOlxuICAgICAgICAgIHZhck5hbWUgPSAnc3VwZXInO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICBjYXNlIG8uQnVpbHRpblZhci5UaGlzOlxuICAgICAgICAgIHZhck5hbWUgPSAndGhpcyc7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIGNhc2Ugby5CdWlsdGluVmFyLkNhdGNoRXJyb3I6XG4gICAgICAgICAgdmFyTmFtZSA9IENBVENIX0VSUk9SX1ZBUi5uYW1lICE7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIGNhc2Ugby5CdWlsdGluVmFyLkNhdGNoU3RhY2s6XG4gICAgICAgICAgdmFyTmFtZSA9IENBVENIX1NUQUNLX1ZBUi5uYW1lICE7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIGRlZmF1bHQ6XG4gICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBVbmtub3duIGJ1aWx0aW4gdmFyaWFibGUgJHthc3QuYnVpbHRpbn1gKTtcbiAgICAgIH1cbiAgICB9XG4gICAgY3R4LnByaW50KGFzdCwgdmFyTmFtZSk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgdmlzaXRJbnN0YW50aWF0ZUV4cHIoYXN0OiBvLkluc3RhbnRpYXRlRXhwciwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIGN0eC5wcmludChhc3QsIGBuZXcgYCk7XG4gICAgYXN0LmNsYXNzRXhwci52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBjdHgucHJpbnQoYXN0LCBgKGApO1xuICAgIHRoaXMudmlzaXRBbGxFeHByZXNzaW9ucyhhc3QuYXJncywgY3R4LCAnLCcpO1xuICAgIGN0eC5wcmludChhc3QsIGApYCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICB2aXNpdExpdGVyYWxFeHByKGFzdDogby5MaXRlcmFsRXhwciwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIGNvbnN0IHZhbHVlID0gYXN0LnZhbHVlO1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09ICdzdHJpbmcnKSB7XG4gICAgICBjdHgucHJpbnQoYXN0LCBlc2NhcGVJZGVudGlmaWVyKHZhbHVlLCB0aGlzLl9lc2NhcGVEb2xsYXJJblN0cmluZ3MpKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY3R4LnByaW50KGFzdCwgYCR7dmFsdWV9YCk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgYWJzdHJhY3QgdmlzaXRFeHRlcm5hbEV4cHIoYXN0OiBvLkV4dGVybmFsRXhwciwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnk7XG5cbiAgdmlzaXRDb25kaXRpb25hbEV4cHIoYXN0OiBvLkNvbmRpdGlvbmFsRXhwciwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIGN0eC5wcmludChhc3QsIGAoYCk7XG4gICAgYXN0LmNvbmRpdGlvbi52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBjdHgucHJpbnQoYXN0LCAnPyAnKTtcbiAgICBhc3QudHJ1ZUNhc2UudmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgY3R4LnByaW50KGFzdCwgJzogJyk7XG4gICAgYXN0LmZhbHNlQ2FzZSAhLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICAgIGN0eC5wcmludChhc3QsIGApYCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgdmlzaXROb3RFeHByKGFzdDogby5Ob3RFeHByLCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgY3R4LnByaW50KGFzdCwgJyEnKTtcbiAgICBhc3QuY29uZGl0aW9uLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0QXNzZXJ0Tm90TnVsbEV4cHIoYXN0OiBvLkFzc2VydE5vdE51bGwsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBhc3QuY29uZGl0aW9uLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIGFic3RyYWN0IHZpc2l0RnVuY3Rpb25FeHByKGFzdDogby5GdW5jdGlvbkV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55O1xuICBhYnN0cmFjdCB2aXNpdERlY2xhcmVGdW5jdGlvblN0bXQoc3RtdDogby5EZWNsYXJlRnVuY3Rpb25TdG10LCBjb250ZXh0OiBhbnkpOiBhbnk7XG5cbiAgdmlzaXRCaW5hcnlPcGVyYXRvckV4cHIoYXN0OiBvLkJpbmFyeU9wZXJhdG9yRXhwciwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIGxldCBvcFN0cjogc3RyaW5nO1xuICAgIHN3aXRjaCAoYXN0Lm9wZXJhdG9yKSB7XG4gICAgICBjYXNlIG8uQmluYXJ5T3BlcmF0b3IuRXF1YWxzOlxuICAgICAgICBvcFN0ciA9ICc9PSc7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBvLkJpbmFyeU9wZXJhdG9yLklkZW50aWNhbDpcbiAgICAgICAgb3BTdHIgPSAnPT09JztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIG8uQmluYXJ5T3BlcmF0b3IuTm90RXF1YWxzOlxuICAgICAgICBvcFN0ciA9ICchPSc7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBvLkJpbmFyeU9wZXJhdG9yLk5vdElkZW50aWNhbDpcbiAgICAgICAgb3BTdHIgPSAnIT09JztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIG8uQmluYXJ5T3BlcmF0b3IuQW5kOlxuICAgICAgICBvcFN0ciA9ICcmJic7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBvLkJpbmFyeU9wZXJhdG9yLkJpdHdpc2VBbmQ6XG4gICAgICAgIG9wU3RyID0gJyYnO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2Ugby5CaW5hcnlPcGVyYXRvci5PcjpcbiAgICAgICAgb3BTdHIgPSAnfHwnO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2Ugby5CaW5hcnlPcGVyYXRvci5QbHVzOlxuICAgICAgICBvcFN0ciA9ICcrJztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIG8uQmluYXJ5T3BlcmF0b3IuTWludXM6XG4gICAgICAgIG9wU3RyID0gJy0nO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2Ugby5CaW5hcnlPcGVyYXRvci5EaXZpZGU6XG4gICAgICAgIG9wU3RyID0gJy8nO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2Ugby5CaW5hcnlPcGVyYXRvci5NdWx0aXBseTpcbiAgICAgICAgb3BTdHIgPSAnKic7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBvLkJpbmFyeU9wZXJhdG9yLk1vZHVsbzpcbiAgICAgICAgb3BTdHIgPSAnJSc7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBvLkJpbmFyeU9wZXJhdG9yLkxvd2VyOlxuICAgICAgICBvcFN0ciA9ICc8JztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIG8uQmluYXJ5T3BlcmF0b3IuTG93ZXJFcXVhbHM6XG4gICAgICAgIG9wU3RyID0gJzw9JztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIG8uQmluYXJ5T3BlcmF0b3IuQmlnZ2VyOlxuICAgICAgICBvcFN0ciA9ICc+JztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIG8uQmluYXJ5T3BlcmF0b3IuQmlnZ2VyRXF1YWxzOlxuICAgICAgICBvcFN0ciA9ICc+PSc7XG4gICAgICAgIGJyZWFrO1xuICAgICAgZGVmYXVsdDpcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBVbmtub3duIG9wZXJhdG9yICR7YXN0Lm9wZXJhdG9yfWApO1xuICAgIH1cbiAgICBpZiAoYXN0LnBhcmVucykgY3R4LnByaW50KGFzdCwgYChgKTtcbiAgICBhc3QubGhzLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpO1xuICAgIGN0eC5wcmludChhc3QsIGAgJHtvcFN0cn0gYCk7XG4gICAgYXN0LnJocy52aXNpdEV4cHJlc3Npb24odGhpcywgY3R4KTtcbiAgICBpZiAoYXN0LnBhcmVucykgY3R4LnByaW50KGFzdCwgYClgKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHZpc2l0UmVhZFByb3BFeHByKGFzdDogby5SZWFkUHJvcEV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBhc3QucmVjZWl2ZXIudmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgY3R4LnByaW50KGFzdCwgYC5gKTtcbiAgICBjdHgucHJpbnQoYXN0LCBhc3QubmFtZSk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgdmlzaXRSZWFkS2V5RXhwcihhc3Q6IG8uUmVhZEtleUV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBhc3QucmVjZWl2ZXIudmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgY3R4LnByaW50KGFzdCwgYFtgKTtcbiAgICBhc3QuaW5kZXgudmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgY3R4LnByaW50KGFzdCwgYF1gKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICB2aXNpdExpdGVyYWxBcnJheUV4cHIoYXN0OiBvLkxpdGVyYWxBcnJheUV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBjdHgucHJpbnQoYXN0LCBgW2ApO1xuICAgIHRoaXMudmlzaXRBbGxFeHByZXNzaW9ucyhhc3QuZW50cmllcywgY3R4LCAnLCcpO1xuICAgIGN0eC5wcmludChhc3QsIGBdYCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgdmlzaXRMaXRlcmFsTWFwRXhwcihhc3Q6IG8uTGl0ZXJhbE1hcEV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBjdHgucHJpbnQoYXN0LCBge2ApO1xuICAgIHRoaXMudmlzaXRBbGxPYmplY3RzKGVudHJ5ID0+IHtcbiAgICAgIGN0eC5wcmludChhc3QsIGAke2VzY2FwZUlkZW50aWZpZXIoZW50cnkua2V5LCB0aGlzLl9lc2NhcGVEb2xsYXJJblN0cmluZ3MsIGVudHJ5LnF1b3RlZCl9OmApO1xuICAgICAgZW50cnkudmFsdWUudmlzaXRFeHByZXNzaW9uKHRoaXMsIGN0eCk7XG4gICAgfSwgYXN0LmVudHJpZXMsIGN0eCwgJywnKTtcbiAgICBjdHgucHJpbnQoYXN0LCBgfWApO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0Q29tbWFFeHByKGFzdDogby5Db21tYUV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBjdHgucHJpbnQoYXN0LCAnKCcpO1xuICAgIHRoaXMudmlzaXRBbGxFeHByZXNzaW9ucyhhc3QucGFydHMsIGN0eCwgJywnKTtcbiAgICBjdHgucHJpbnQoYXN0LCAnKScpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0QWxsRXhwcmVzc2lvbnMoZXhwcmVzc2lvbnM6IG8uRXhwcmVzc2lvbltdLCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCwgc2VwYXJhdG9yOiBzdHJpbmcpOlxuICAgICAgdm9pZCB7XG4gICAgdGhpcy52aXNpdEFsbE9iamVjdHMoZXhwciA9PiBleHByLnZpc2l0RXhwcmVzc2lvbih0aGlzLCBjdHgpLCBleHByZXNzaW9ucywgY3R4LCBzZXBhcmF0b3IpO1xuICB9XG5cbiAgdmlzaXRBbGxPYmplY3RzPFQ+KFxuICAgICAgaGFuZGxlcjogKHQ6IFQpID0+IHZvaWQsIGV4cHJlc3Npb25zOiBUW10sIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0LFxuICAgICAgc2VwYXJhdG9yOiBzdHJpbmcpOiB2b2lkIHtcbiAgICBsZXQgaW5jcmVtZW50ZWRJbmRlbnQgPSBmYWxzZTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IGV4cHJlc3Npb25zLmxlbmd0aDsgaSsrKSB7XG4gICAgICBpZiAoaSA+IDApIHtcbiAgICAgICAgaWYgKGN0eC5saW5lTGVuZ3RoKCkgPiA4MCkge1xuICAgICAgICAgIGN0eC5wcmludChudWxsLCBzZXBhcmF0b3IsIHRydWUpO1xuICAgICAgICAgIGlmICghaW5jcmVtZW50ZWRJbmRlbnQpIHtcbiAgICAgICAgICAgIC8vIGNvbnRpbnVhdGlvbiBhcmUgbWFya2VkIHdpdGggZG91YmxlIGluZGVudC5cbiAgICAgICAgICAgIGN0eC5pbmNJbmRlbnQoKTtcbiAgICAgICAgICAgIGN0eC5pbmNJbmRlbnQoKTtcbiAgICAgICAgICAgIGluY3JlbWVudGVkSW5kZW50ID0gdHJ1ZTtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY3R4LnByaW50KG51bGwsIHNlcGFyYXRvciwgZmFsc2UpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICBoYW5kbGVyKGV4cHJlc3Npb25zW2ldKTtcbiAgICB9XG4gICAgaWYgKGluY3JlbWVudGVkSW5kZW50KSB7XG4gICAgICAvLyBjb250aW51YXRpb24gYXJlIG1hcmtlZCB3aXRoIGRvdWJsZSBpbmRlbnQuXG4gICAgICBjdHguZGVjSW5kZW50KCk7XG4gICAgICBjdHguZGVjSW5kZW50KCk7XG4gICAgfVxuICB9XG5cbiAgdmlzaXRBbGxTdGF0ZW1lbnRzKHN0YXRlbWVudHM6IG8uU3RhdGVtZW50W10sIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogdm9pZCB7XG4gICAgc3RhdGVtZW50cy5mb3JFYWNoKChzdG10KSA9PiBzdG10LnZpc2l0U3RhdGVtZW50KHRoaXMsIGN0eCkpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBlc2NhcGVJZGVudGlmaWVyKFxuICAgIGlucHV0OiBzdHJpbmcsIGVzY2FwZURvbGxhcjogYm9vbGVhbiwgYWx3YXlzUXVvdGU6IGJvb2xlYW4gPSB0cnVlKTogYW55IHtcbiAgaWYgKGlucHV0ID09IG51bGwpIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICBjb25zdCBib2R5ID0gaW5wdXQucmVwbGFjZShfU0lOR0xFX1FVT1RFX0VTQ0FQRV9TVFJJTkdfUkUsICguLi5tYXRjaDogc3RyaW5nW10pID0+IHtcbiAgICBpZiAobWF0Y2hbMF0gPT0gJyQnKSB7XG4gICAgICByZXR1cm4gZXNjYXBlRG9sbGFyID8gJ1xcXFwkJyA6ICckJztcbiAgICB9IGVsc2UgaWYgKG1hdGNoWzBdID09ICdcXG4nKSB7XG4gICAgICByZXR1cm4gJ1xcXFxuJztcbiAgICB9IGVsc2UgaWYgKG1hdGNoWzBdID09ICdcXHInKSB7XG4gICAgICByZXR1cm4gJ1xcXFxyJztcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGBcXFxcJHttYXRjaFswXX1gO1xuICAgIH1cbiAgfSk7XG4gIGNvbnN0IHJlcXVpcmVzUXVvdGVzID0gYWx3YXlzUXVvdGUgfHwgIV9MRUdBTF9JREVOVElGSUVSX1JFLnRlc3QoYm9keSk7XG4gIHJldHVybiByZXF1aXJlc1F1b3RlcyA/IGAnJHtib2R5fSdgIDogYm9keTtcbn1cblxuZnVuY3Rpb24gX2NyZWF0ZUluZGVudChjb3VudDogbnVtYmVyKTogc3RyaW5nIHtcbiAgbGV0IHJlcyA9ICcnO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNvdW50OyBpKyspIHtcbiAgICByZXMgKz0gX0lOREVOVF9XSVRIO1xuICB9XG4gIHJldHVybiByZXM7XG59XG4iXX0=