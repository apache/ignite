/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { identifierName } from '../compile_metadata';
import { EmitterVisitorContext } from './abstract_emitter';
import { AbstractJsEmitterVisitor } from './abstract_js_emitter';
import * as o from './output_ast';
/**
 * A helper class to manage the evaluation of JIT generated code.
 */
var JitEvaluator = /** @class */ (function () {
    function JitEvaluator() {
    }
    /**
     *
     * @param sourceUrl The URL of the generated code.
     * @param statements An array of Angular statement AST nodes to be evaluated.
     * @param reflector A helper used when converting the statements to executable code.
     * @param createSourceMaps If true then create a source-map for the generated code and include it
     * inline as a source-map comment.
     * @returns A map of all the variables in the generated code.
     */
    JitEvaluator.prototype.evaluateStatements = function (sourceUrl, statements, reflector, createSourceMaps) {
        var converter = new JitEmitterVisitor(reflector);
        var ctx = EmitterVisitorContext.createRoot();
        // Ensure generated code is in strict mode
        if (statements.length > 0 && !isUseStrictStatement(statements[0])) {
            statements = tslib_1.__spread([
                o.literal('use strict').toStmt()
            ], statements);
        }
        converter.visitAllStatements(statements, ctx);
        converter.createReturnStmt(ctx);
        return this.evaluateCode(sourceUrl, ctx, converter.getArgs(), createSourceMaps);
    };
    /**
     * Evaluate a piece of JIT generated code.
     * @param sourceUrl The URL of this generated code.
     * @param ctx A context object that contains an AST of the code to be evaluated.
     * @param vars A map containing the names and values of variables that the evaluated code might
     * reference.
     * @param createSourceMap If true then create a source-map for the generated code and include it
     * inline as a source-map comment.
     * @returns The result of evaluating the code.
     */
    JitEvaluator.prototype.evaluateCode = function (sourceUrl, ctx, vars, createSourceMap) {
        var fnBody = "\"use strict\";" + ctx.toSource() + "\n//# sourceURL=" + sourceUrl;
        var fnArgNames = [];
        var fnArgValues = [];
        for (var argName in vars) {
            fnArgValues.push(vars[argName]);
            fnArgNames.push(argName);
        }
        if (createSourceMap) {
            // using `new Function(...)` generates a header, 1 line of no arguments, 2 lines otherwise
            // E.g. ```
            // function anonymous(a,b,c
            // /**/) { ... }```
            // We don't want to hard code this fact, so we auto detect it via an empty function first.
            var emptyFn = new (Function.bind.apply(Function, tslib_1.__spread([void 0], fnArgNames.concat('return null;'))))().toString();
            var headerLines = emptyFn.slice(0, emptyFn.indexOf('return null;')).split('\n').length - 1;
            fnBody += "\n" + ctx.toSourceMapGenerator(sourceUrl, headerLines).toJsComment();
        }
        var fn = new (Function.bind.apply(Function, tslib_1.__spread([void 0], fnArgNames.concat(fnBody))))();
        return this.executeFunction(fn, fnArgValues);
    };
    /**
     * Execute a JIT generated function by calling it.
     *
     * This method can be overridden in tests to capture the functions that are generated
     * by this `JitEvaluator` class.
     *
     * @param fn A function to execute.
     * @param args The arguments to pass to the function being executed.
     * @returns The return value of the executed function.
     */
    JitEvaluator.prototype.executeFunction = function (fn, args) { return fn.apply(void 0, tslib_1.__spread(args)); };
    return JitEvaluator;
}());
export { JitEvaluator };
/**
 * An Angular AST visitor that converts AST nodes into executable JavaScript code.
 */
var JitEmitterVisitor = /** @class */ (function (_super) {
    tslib_1.__extends(JitEmitterVisitor, _super);
    function JitEmitterVisitor(reflector) {
        var _this = _super.call(this) || this;
        _this.reflector = reflector;
        _this._evalArgNames = [];
        _this._evalArgValues = [];
        _this._evalExportedVars = [];
        return _this;
    }
    JitEmitterVisitor.prototype.createReturnStmt = function (ctx) {
        var stmt = new o.ReturnStatement(new o.LiteralMapExpr(this._evalExportedVars.map(function (resultVar) { return new o.LiteralMapEntry(resultVar, o.variable(resultVar), false); })));
        stmt.visitStatement(this, ctx);
    };
    JitEmitterVisitor.prototype.getArgs = function () {
        var result = {};
        for (var i = 0; i < this._evalArgNames.length; i++) {
            result[this._evalArgNames[i]] = this._evalArgValues[i];
        }
        return result;
    };
    JitEmitterVisitor.prototype.visitExternalExpr = function (ast, ctx) {
        this._emitReferenceToExternal(ast, this.reflector.resolveExternalReference(ast.value), ctx);
        return null;
    };
    JitEmitterVisitor.prototype.visitWrappedNodeExpr = function (ast, ctx) {
        this._emitReferenceToExternal(ast, ast.node, ctx);
        return null;
    };
    JitEmitterVisitor.prototype.visitDeclareVarStmt = function (stmt, ctx) {
        if (stmt.hasModifier(o.StmtModifier.Exported)) {
            this._evalExportedVars.push(stmt.name);
        }
        return _super.prototype.visitDeclareVarStmt.call(this, stmt, ctx);
    };
    JitEmitterVisitor.prototype.visitDeclareFunctionStmt = function (stmt, ctx) {
        if (stmt.hasModifier(o.StmtModifier.Exported)) {
            this._evalExportedVars.push(stmt.name);
        }
        return _super.prototype.visitDeclareFunctionStmt.call(this, stmt, ctx);
    };
    JitEmitterVisitor.prototype.visitDeclareClassStmt = function (stmt, ctx) {
        if (stmt.hasModifier(o.StmtModifier.Exported)) {
            this._evalExportedVars.push(stmt.name);
        }
        return _super.prototype.visitDeclareClassStmt.call(this, stmt, ctx);
    };
    JitEmitterVisitor.prototype._emitReferenceToExternal = function (ast, value, ctx) {
        var id = this._evalArgValues.indexOf(value);
        if (id === -1) {
            id = this._evalArgValues.length;
            this._evalArgValues.push(value);
            var name_1 = identifierName({ reference: value }) || 'val';
            this._evalArgNames.push("jit_" + name_1 + "_" + id);
        }
        ctx.print(ast, this._evalArgNames[id]);
    };
    return JitEmitterVisitor;
}(AbstractJsEmitterVisitor));
export { JitEmitterVisitor };
function isUseStrictStatement(statement) {
    return statement.isEquivalent(o.literal('use strict').toStmt());
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoib3V0cHV0X2ppdC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9vdXRwdXQvb3V0cHV0X2ppdC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsT0FBTyxFQUFDLGNBQWMsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBR25ELE9BQU8sRUFBQyxxQkFBcUIsRUFBQyxNQUFNLG9CQUFvQixDQUFDO0FBQ3pELE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBQy9ELE9BQU8sS0FBSyxDQUFDLE1BQU0sY0FBYyxDQUFDO0FBRWxDOztHQUVHO0FBQ0g7SUFBQTtJQXdFQSxDQUFDO0lBdkVDOzs7Ozs7OztPQVFHO0lBQ0gseUNBQWtCLEdBQWxCLFVBQ0ksU0FBaUIsRUFBRSxVQUF5QixFQUFFLFNBQTJCLEVBQ3pFLGdCQUF5QjtRQUMzQixJQUFNLFNBQVMsR0FBRyxJQUFJLGlCQUFpQixDQUFDLFNBQVMsQ0FBQyxDQUFDO1FBQ25ELElBQU0sR0FBRyxHQUFHLHFCQUFxQixDQUFDLFVBQVUsRUFBRSxDQUFDO1FBQy9DLDBDQUEwQztRQUMxQyxJQUFJLFVBQVUsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxJQUFJLENBQUMsb0JBQW9CLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUU7WUFDakUsVUFBVTtnQkFDUixDQUFDLENBQUMsT0FBTyxDQUFDLFlBQVksQ0FBQyxDQUFDLE1BQU0sRUFBRTtlQUM3QixVQUFVLENBQ2QsQ0FBQztTQUNIO1FBQ0QsU0FBUyxDQUFDLGtCQUFrQixDQUFDLFVBQVUsRUFBRSxHQUFHLENBQUMsQ0FBQztRQUM5QyxTQUFTLENBQUMsZ0JBQWdCLENBQUMsR0FBRyxDQUFDLENBQUM7UUFDaEMsT0FBTyxJQUFJLENBQUMsWUFBWSxDQUFDLFNBQVMsRUFBRSxHQUFHLEVBQUUsU0FB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ZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2lkZW50aWZpZXJOYW1lfSBmcm9tICcuLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCB7Q29tcGlsZVJlZmxlY3Rvcn0gZnJvbSAnLi4vY29tcGlsZV9yZWZsZWN0b3InO1xuXG5pbXBvcnQge0VtaXR0ZXJWaXNpdG9yQ29udGV4dH0gZnJvbSAnLi9hYnN0cmFjdF9lbWl0dGVyJztcbmltcG9ydCB7QWJzdHJhY3RKc0VtaXR0ZXJWaXNpdG9yfSBmcm9tICcuL2Fic3RyYWN0X2pzX2VtaXR0ZXInO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuL291dHB1dF9hc3QnO1xuXG4vKipcbiAqIEEgaGVscGVyIGNsYXNzIHRvIG1hbmFnZSB0aGUgZXZhbHVhdGlvbiBvZiBKSVQgZ2VuZXJhdGVkIGNvZGUuXG4gKi9cbmV4cG9ydCBjbGFzcyBKaXRFdmFsdWF0b3Ige1xuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHNvdXJjZVVybCBUaGUgVVJMIG9mIHRoZSBnZW5lcmF0ZWQgY29kZS5cbiAgICogQHBhcmFtIHN0YXRlbWVudHMgQW4gYXJyYXkgb2YgQW5ndWxhciBzdGF0ZW1lbnQgQVNUIG5vZGVzIHRvIGJlIGV2YWx1YXRlZC5cbiAgICogQHBhcmFtIHJlZmxlY3RvciBBIGhlbHBlciB1c2VkIHdoZW4gY29udmVydGluZyB0aGUgc3RhdGVtZW50cyB0byBleGVjdXRhYmxlIGNvZGUuXG4gICAqIEBwYXJhbSBjcmVhdGVTb3VyY2VNYXBzIElmIHRydWUgdGhlbiBjcmVhdGUgYSBzb3VyY2UtbWFwIGZvciB0aGUgZ2VuZXJhdGVkIGNvZGUgYW5kIGluY2x1ZGUgaXRcbiAgICogaW5saW5lIGFzIGEgc291cmNlLW1hcCBjb21tZW50LlxuICAgKiBAcmV0dXJucyBBIG1hcCBvZiBhbGwgdGhlIHZhcmlhYmxlcyBpbiB0aGUgZ2VuZXJhdGVkIGNvZGUuXG4gICAqL1xuICBldmFsdWF0ZVN0YXRlbWVudHMoXG4gICAgICBzb3VyY2VVcmw6IHN0cmluZywgc3RhdGVtZW50czogby5TdGF0ZW1lbnRbXSwgcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yLFxuICAgICAgY3JlYXRlU291cmNlTWFwczogYm9vbGVhbik6IHtba2V5OiBzdHJpbmddOiBhbnl9IHtcbiAgICBjb25zdCBjb252ZXJ0ZXIgPSBuZXcgSml0RW1pdHRlclZpc2l0b3IocmVmbGVjdG9yKTtcbiAgICBjb25zdCBjdHggPSBFbWl0dGVyVmlzaXRvckNvbnRleHQuY3JlYXRlUm9vdCgpO1xuICAgIC8vIEVuc3VyZSBnZW5lcmF0ZWQgY29kZSBpcyBpbiBzdHJpY3QgbW9kZVxuICAgIGlmIChzdGF0ZW1lbnRzLmxlbmd0aCA+IDAgJiYgIWlzVXNlU3RyaWN0U3RhdGVtZW50KHN0YXRlbWVudHNbMF0pKSB7XG4gICAgICBzdGF0ZW1lbnRzID0gW1xuICAgICAgICBvLmxpdGVyYWwoJ3VzZSBzdHJpY3QnKS50b1N0bXQoKSxcbiAgICAgICAgLi4uc3RhdGVtZW50cyxcbiAgICAgIF07XG4gICAgfVxuICAgIGNvbnZlcnRlci52aXNpdEFsbFN0YXRlbWVudHMoc3RhdGVtZW50cywgY3R4KTtcbiAgICBjb252ZXJ0ZXIuY3JlYXRlUmV0dXJuU3RtdChjdHgpO1xuICAgIHJldHVybiB0aGlzLmV2YWx1YXRlQ29kZShzb3VyY2VVcmwsIGN0eCwgY29udmVydGVyLmdldEFyZ3MoKSwgY3JlYXRlU291cmNlTWFwcyk7XG4gIH1cblxuICAvKipcbiAgICogRXZhbHVhdGUgYSBwaWVjZSBvZiBKSVQgZ2VuZXJhdGVkIGNvZGUuXG4gICAqIEBwYXJhbSBzb3VyY2VVcmwgVGhlIFVSTCBvZiB0aGlzIGdlbmVyYXRlZCBjb2RlLlxuICAgKiBAcGFyYW0gY3R4IEEgY29udGV4dCBvYmplY3QgdGhhdCBjb250YWlucyBhbiBBU1Qgb2YgdGhlIGNvZGUgdG8gYmUgZXZhbHVhdGVkLlxuICAgKiBAcGFyYW0gdmFycyBBIG1hcCBjb250YWluaW5nIHRoZSBuYW1lcyBhbmQgdmFsdWVzIG9mIHZhcmlhYmxlcyB0aGF0IHRoZSBldmFsdWF0ZWQgY29kZSBtaWdodFxuICAgKiByZWZlcmVuY2UuXG4gICAqIEBwYXJhbSBjcmVhdGVTb3VyY2VNYXAgSWYgdHJ1ZSB0aGVuIGNyZWF0ZSBhIHNvdXJjZS1tYXAgZm9yIHRoZSBnZW5lcmF0ZWQgY29kZSBhbmQgaW5jbHVkZSBpdFxuICAgKiBpbmxpbmUgYXMgYSBzb3VyY2UtbWFwIGNvbW1lbnQuXG4gICAqIEByZXR1cm5zIFRoZSByZXN1bHQgb2YgZXZhbHVhdGluZyB0aGUgY29kZS5cbiAgICovXG4gIGV2YWx1YXRlQ29kZShcbiAgICAgIHNvdXJjZVVybDogc3RyaW5nLCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCwgdmFyczoge1trZXk6IHN0cmluZ106IGFueX0sXG4gICAgICBjcmVhdGVTb3VyY2VNYXA6IGJvb2xlYW4pOiBhbnkge1xuICAgIGxldCBmbkJvZHkgPSBgXCJ1c2Ugc3RyaWN0XCI7JHtjdHgudG9Tb3VyY2UoKX1cXG4vLyMgc291cmNlVVJMPSR7c291cmNlVXJsfWA7XG4gICAgY29uc3QgZm5BcmdOYW1lczogc3RyaW5nW10gPSBbXTtcbiAgICBjb25zdCBmbkFyZ1ZhbHVlczogYW55W10gPSBbXTtcbiAgICBmb3IgKGNvbnN0IGFyZ05hbWUgaW4gdmFycykge1xuICAgICAgZm5BcmdWYWx1ZXMucHVzaCh2YXJzW2FyZ05hbWVdKTtcbiAgICAgIGZuQXJnTmFtZXMucHVzaChhcmdOYW1lKTtcbiAgICB9XG4gICAgaWYgKGNyZWF0ZVNvdXJjZU1hcCkge1xuICAgICAgLy8gdXNpbmcgYG5ldyBGdW5jdGlvbiguLi4pYCBnZW5lcmF0ZXMgYSBoZWFkZXIsIDEgbGluZSBvZiBubyBhcmd1bWVudHMsIDIgbGluZXMgb3RoZXJ3aXNlXG4gICAgICAvLyBFLmcuIGBgYFxuICAgICAgLy8gZnVuY3Rpb24gYW5vbnltb3VzKGEsYixjXG4gICAgICAvLyAvKiovKSB7IC4uLiB9YGBgXG4gICAgICAvLyBXZSBkb24ndCB3YW50IHRvIGhhcmQgY29kZSB0aGlzIGZhY3QsIHNvIHdlIGF1dG8gZGV0ZWN0IGl0IHZpYSBhbiBlbXB0eSBmdW5jdGlvbiBmaXJzdC5cbiAgICAgIGNvbnN0IGVtcHR5Rm4gPSBuZXcgRnVuY3Rpb24oLi4uZm5BcmdOYW1lcy5jb25jYXQoJ3JldHVybiBudWxsOycpKS50b1N0cmluZygpO1xuICAgICAgY29uc3QgaGVhZGVyTGluZXMgPSBlbXB0eUZuLnNsaWNlKDAsIGVtcHR5Rm4uaW5kZXhPZigncmV0dXJuIG51bGw7JykpLnNwbGl0KCdcXG4nKS5sZW5ndGggLSAxO1xuICAgICAgZm5Cb2R5ICs9IGBcXG4ke2N0eC50b1NvdXJjZU1hcEdlbmVyYXRvcihzb3VyY2VVcmwsIGhlYWRlckxpbmVzKS50b0pzQ29tbWVudCgpfWA7XG4gICAgfVxuICAgIGNvbnN0IGZuID0gbmV3IEZ1bmN0aW9uKC4uLmZuQXJnTmFtZXMuY29uY2F0KGZuQm9keSkpO1xuICAgIHJldHVybiB0aGlzLmV4ZWN1dGVGdW5jdGlvbihmbiwgZm5BcmdWYWx1ZXMpO1xuICB9XG5cbiAgLyoqXG4gICAqIEV4ZWN1dGUgYSBKSVQgZ2VuZXJhdGVkIGZ1bmN0aW9uIGJ5IGNhbGxpbmcgaXQuXG4gICAqXG4gICAqIFRoaXMgbWV0aG9kIGNhbiBiZSBvdmVycmlkZGVuIGluIHRlc3RzIHRvIGNhcHR1cmUgdGhlIGZ1bmN0aW9ucyB0aGF0IGFyZSBnZW5lcmF0ZWRcbiAgICogYnkgdGhpcyBgSml0RXZhbHVhdG9yYCBjbGFzcy5cbiAgICpcbiAgICogQHBhcmFtIGZuIEEgZnVuY3Rpb24gdG8gZXhlY3V0ZS5cbiAgICogQHBhcmFtIGFyZ3MgVGhlIGFyZ3VtZW50cyB0byBwYXNzIHRvIHRoZSBmdW5jdGlvbiBiZWluZyBleGVjdXRlZC5cbiAgICogQHJldHVybnMgVGhlIHJldHVybiB2YWx1ZSBvZiB0aGUgZXhlY3V0ZWQgZnVuY3Rpb24uXG4gICAqL1xuICBleGVjdXRlRnVuY3Rpb24oZm46IEZ1bmN0aW9uLCBhcmdzOiBhbnlbXSkgeyByZXR1cm4gZm4oLi4uYXJncyk7IH1cbn1cblxuLyoqXG4gKiBBbiBBbmd1bGFyIEFTVCB2aXNpdG9yIHRoYXQgY29udmVydHMgQVNUIG5vZGVzIGludG8gZXhlY3V0YWJsZSBKYXZhU2NyaXB0IGNvZGUuXG4gKi9cbmV4cG9ydCBjbGFzcyBKaXRFbWl0dGVyVmlzaXRvciBleHRlbmRzIEFic3RyYWN0SnNFbWl0dGVyVmlzaXRvciB7XG4gIHByaXZhdGUgX2V2YWxBcmdOYW1lczogc3RyaW5nW10gPSBbXTtcbiAgcHJpdmF0ZSBfZXZhbEFyZ1ZhbHVlczogYW55W10gPSBbXTtcbiAgcHJpdmF0ZSBfZXZhbEV4cG9ydGVkVmFyczogc3RyaW5nW10gPSBbXTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHJlZmxlY3RvcjogQ29tcGlsZVJlZmxlY3RvcikgeyBzdXBlcigpOyB9XG5cbiAgY3JlYXRlUmV0dXJuU3RtdChjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCkge1xuICAgIGNvbnN0IHN0bXQgPSBuZXcgby5SZXR1cm5TdGF0ZW1lbnQobmV3IG8uTGl0ZXJhbE1hcEV4cHIodGhpcy5fZXZhbEV4cG9ydGVkVmFycy5tYXAoXG4gICAgICAgIHJlc3VsdFZhciA9PiBuZXcgby5MaXRlcmFsTWFwRW50cnkocmVzdWx0VmFyLCBvLnZhcmlhYmxlKHJlc3VsdFZhciksIGZhbHNlKSkpKTtcbiAgICBzdG10LnZpc2l0U3RhdGVtZW50KHRoaXMsIGN0eCk7XG4gIH1cblxuICBnZXRBcmdzKCk6IHtba2V5OiBzdHJpbmddOiBhbnl9IHtcbiAgICBjb25zdCByZXN1bHQ6IHtba2V5OiBzdHJpbmddOiBhbnl9ID0ge307XG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCB0aGlzLl9ldmFsQXJnTmFtZXMubGVuZ3RoOyBpKyspIHtcbiAgICAgIHJlc3VsdFt0aGlzLl9ldmFsQXJnTmFtZXNbaV1dID0gdGhpcy5fZXZhbEFyZ1ZhbHVlc1tpXTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIHZpc2l0RXh0ZXJuYWxFeHByKGFzdDogby5FeHRlcm5hbEV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICB0aGlzLl9lbWl0UmVmZXJlbmNlVG9FeHRlcm5hbChhc3QsIHRoaXMucmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShhc3QudmFsdWUpLCBjdHgpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgdmlzaXRXcmFwcGVkTm9kZUV4cHIoYXN0OiBvLldyYXBwZWROb2RlRXhwcjxhbnk+LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgdGhpcy5fZW1pdFJlZmVyZW5jZVRvRXh0ZXJuYWwoYXN0LCBhc3Qubm9kZSwgY3R4KTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIHZpc2l0RGVjbGFyZVZhclN0bXQoc3RtdDogby5EZWNsYXJlVmFyU3RtdCwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIGlmIChzdG10Lmhhc01vZGlmaWVyKG8uU3RtdE1vZGlmaWVyLkV4cG9ydGVkKSkge1xuICAgICAgdGhpcy5fZXZhbEV4cG9ydGVkVmFycy5wdXNoKHN0bXQubmFtZSk7XG4gICAgfVxuICAgIHJldHVybiBzdXBlci52aXNpdERlY2xhcmVWYXJTdG10KHN0bXQsIGN0eCk7XG4gIH1cblxuICB2aXNpdERlY2xhcmVGdW5jdGlvblN0bXQoc3RtdDogby5EZWNsYXJlRnVuY3Rpb25TdG10LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgaWYgKHN0bXQuaGFzTW9kaWZpZXIoby5TdG10TW9kaWZpZXIuRXhwb3J0ZWQpKSB7XG4gICAgICB0aGlzLl9ldmFsRXhwb3J0ZWRWYXJzLnB1c2goc3RtdC5uYW1lKTtcbiAgICB9XG4gICAgcmV0dXJuIHN1cGVyLnZpc2l0RGVjbGFyZUZ1bmN0aW9uU3RtdChzdG10LCBjdHgpO1xuICB9XG5cbiAgdmlzaXREZWNsYXJlQ2xhc3NTdG10KHN0bXQ6IG8uQ2xhc3NTdG10LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgaWYgKHN0bXQuaGFzTW9kaWZpZXIoby5TdG10TW9kaWZpZXIuRXhwb3J0ZWQpKSB7XG4gICAgICB0aGlzLl9ldmFsRXhwb3J0ZWRWYXJzLnB1c2goc3RtdC5uYW1lKTtcbiAgICB9XG4gICAgcmV0dXJuIHN1cGVyLnZpc2l0RGVjbGFyZUNsYXNzU3RtdChzdG10LCBjdHgpO1xuICB9XG5cbiAgcHJpdmF0ZSBfZW1pdFJlZmVyZW5jZVRvRXh0ZXJuYWwoYXN0OiBvLkV4cHJlc3Npb24sIHZhbHVlOiBhbnksIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTpcbiAgICAgIHZvaWQge1xuICAgIGxldCBpZCA9IHRoaXMuX2V2YWxBcmdWYWx1ZXMuaW5kZXhPZih2YWx1ZSk7XG4gICAgaWYgKGlkID09PSAtMSkge1xuICAgICAgaWQgPSB0aGlzLl9ldmFsQXJnVmFsdWVzLmxlbmd0aDtcbiAgICAgIHRoaXMuX2V2YWxBcmdWYWx1ZXMucHVzaCh2YWx1ZSk7XG4gICAgICBjb25zdCBuYW1lID0gaWRlbnRpZmllck5hbWUoe3JlZmVyZW5jZTogdmFsdWV9KSB8fCAndmFsJztcbiAgICAgIHRoaXMuX2V2YWxBcmdOYW1lcy5wdXNoKGBqaXRfJHtuYW1lfV8ke2lkfWApO1xuICAgIH1cbiAgICBjdHgucHJpbnQoYXN0LCB0aGlzLl9ldmFsQXJnTmFtZXNbaWRdKTtcbiAgfVxufVxuXG5cbmZ1bmN0aW9uIGlzVXNlU3RyaWN0U3RhdGVtZW50KHN0YXRlbWVudDogby5TdGF0ZW1lbnQpOiBib29sZWFuIHtcbiAgcmV0dXJuIHN0YXRlbWVudC5pc0VxdWl2YWxlbnQoby5saXRlcmFsKCd1c2Ugc3RyaWN0JykudG9TdG10KCkpO1xufVxuIl19