/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/output/output_jit", ["require", "exports", "tslib", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/output/abstract_emitter", "@angular/compiler/src/output/abstract_js_emitter", "@angular/compiler/src/output/output_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var abstract_emitter_1 = require("@angular/compiler/src/output/abstract_emitter");
    var abstract_js_emitter_1 = require("@angular/compiler/src/output/abstract_js_emitter");
    var o = require("@angular/compiler/src/output/output_ast");
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
            var ctx = abstract_emitter_1.EmitterVisitorContext.createRoot();
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
    exports.JitEvaluator = JitEvaluator;
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
                var name_1 = compile_metadata_1.identifierName({ reference: value }) || 'val';
                this._evalArgNames.push("jit_" + name_1 + "_" + id);
            }
            ctx.print(ast, this._evalArgNames[id]);
        };
        return JitEmitterVisitor;
    }(abstract_js_emitter_1.AbstractJsEmitterVisitor));
    exports.JitEmitterVisitor = JitEmitterVisitor;
    function isUseStrictStatement(statement) {
        return statement.isEquivalent(o.literal('use strict').toStmt());
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoib3V0cHV0X2ppdC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9vdXRwdXQvb3V0cHV0X2ppdC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCwyRUFBbUQ7SUFHbkQsa0ZBQXlEO0lBQ3pELHdGQUErRDtJQUMvRCwyREFBa0M7SUFFbEM7O09BRUc7SUFDSDtRQUFBO1FBd0VBLENBQUM7UUF2RUM7Ozs7Ozs7O1dBUUc7UUFDSCx5Q0FBa0IsR0FBbEIsVUFDSSxTQUFpQixFQUFFLFVBQXlCLEVBQUUsU0FBMkIsRUFDekUsZ0JBQXlCO1lBQzNCLElBQU0sU0FBUyxHQUFHLElBQUksaUJBQWlCLENBQUMsU0FBUyxDQUFDLENBQUM7WUFDbkQsSUFBTSxHQUFHLEdBQUcsd0NBQXFCLENBQUMsVUFBVSxFQUFFLENBQUM7WUFDL0MsMENBQTBDO1lBQzFDLElBQUksVUFBVSxDQUFDLE1BQU0sR0FBRyxDQUFDLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRTtnQkFDakUsVUFBVTtvQkFDUixDQUFDLENBQUMsT0FBTyxDQUFDLFlBQVksQ0FBQyxDQUFDLE1BQU0sRUFBRTttQkFDN0IsVUFBVSxDQUNkLENBQUM7YUFDSDtZQUNELFNBQVMsQ0FBQyxrQkFBa0IsQ0FBQyxVQUFVLEVBQUUsR0FBRyxDQUFDLENBQUM7WUFDOUMsU0FBUyxDQUFDLGdCQUFnQixDQUFDLEdBQUcsQ0FBQyxDQUFDO1lBQ2hDLE9BQU8sSUFBSSxDQUFDLFlBQVksQ0FBQyxTQUFTLEVBQUUsR0FBRyxFQUFFLFNBQVMsQ0FBQyxPQUFPLEVBQUUsRUFBRSxnQkFBZ0IsQ0FBQyxDQUFDO1FBQ2xGLENBQUM7UUFFRDs7Ozs7Ozs7O1dBU0c7UUFDSCxtQ0FBWSxHQUFaLFVBQ0ksU0FBaUIsRUFBRSxHQUEwQixFQUFFLElB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QUFJLENBQUMsaUJBQWlCLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQzthQUN4QztZQUNELE9BQU8saUJBQU0scUJBQXFCLFlBQUMsSUFBSSxFQUFFLEdBQUcsQ0FBQyxDQUFDO1FBQ2hELENBQUM7UUFFTyxvREFBd0IsR0FBaEMsVUFBaUMsR0FBaUIsRUFBRSxLQUFVLEVBQUUsR0FBMEI7WUFFeEYsSUFBSSxFQUFFLEdBQUcsSUFBSSxDQUFDLGNBQWMsQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLENBQUM7WUFDNUMsSUFBSSxFQUFFLEtBQUssQ0FBQyxDQUFDLEVBQUU7Z0JBQ2IsRUFBRSxHQUFHLElBQUksQ0FBQyxjQUFjLENBQUMsTUFBTSxDQUFDO2dCQUNoQyxJQUFJLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztnQkFDaEMsSUFBTSxNQUFJLEdBQUcsaUNBQWMsQ0FBQyxFQUFDLFNBQVMsRUFBRSxLQUFLLEVBQUMsQ0FBQyxJQUFJLEtBQUssQ0FBQztnQkFDekQsSUFBSSxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsU0FBTyxNQUFJLFNBQUksRUFBSSxDQUFDLENBQUM7YUFDOUM7WUFDRCxHQUFHLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRSxJQUFJLENBQUMsYUFBYSxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7UUFDekMsQ0FBQztRQUNILHdCQUFDO0lBQUQsQ0FBQyxBQS9ERCxDQUF1Qyw4Q0FBd0IsR0ErRDlEO0lBL0RZLDhDQUFpQjtJQWtFOUIsU0FBUyxvQkFBb0IsQ0FBQyxTQUFzQjtRQUNsRCxPQUFPLFNBQVMsQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLE9BQU8sQ0FBQyxZQUFZLENBQUMsQ0FBQyxNQUFNLEVBQUUsQ0FBQyxDQUFDO0lBQ2xFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7aWRlbnRpZmllck5hbWV9IGZyb20gJy4uL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuLi9jb21waWxlX3JlZmxlY3Rvcic7XG5cbmltcG9ydCB7RW1pdHRlclZpc2l0b3JDb250ZXh0fSBmcm9tICcuL2Fic3RyYWN0X2VtaXR0ZXInO1xuaW1wb3J0IHtBYnN0cmFjdEpzRW1pdHRlclZpc2l0b3J9IGZyb20gJy4vYWJzdHJhY3RfanNfZW1pdHRlcic7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4vb3V0cHV0X2FzdCc7XG5cbi8qKlxuICogQSBoZWxwZXIgY2xhc3MgdG8gbWFuYWdlIHRoZSBldmFsdWF0aW9uIG9mIEpJVCBnZW5lcmF0ZWQgY29kZS5cbiAqL1xuZXhwb3J0IGNsYXNzIEppdEV2YWx1YXRvciB7XG4gIC8qKlxuICAgKlxuICAgKiBAcGFyYW0gc291cmNlVXJsIFRoZSBVUkwgb2YgdGhlIGdlbmVyYXRlZCBjb2RlLlxuICAgKiBAcGFyYW0gc3RhdGVtZW50cyBBbiBhcnJheSBvZiBBbmd1bGFyIHN0YXRlbWVudCBBU1Qgbm9kZXMgdG8gYmUgZXZhbHVhdGVkLlxuICAgKiBAcGFyYW0gcmVmbGVjdG9yIEEgaGVscGVyIHVzZWQgd2hlbiBjb252ZXJ0aW5nIHRoZSBzdGF0ZW1lbnRzIHRvIGV4ZWN1dGFibGUgY29kZS5cbiAgICogQHBhcmFtIGNyZWF0ZVNvdXJjZU1hcHMgSWYgdHJ1ZSB0aGVuIGNyZWF0ZSBhIHNvdXJjZS1tYXAgZm9yIHRoZSBnZW5lcmF0ZWQgY29kZSBhbmQgaW5jbHVkZSBpdFxuICAgKiBpbmxpbmUgYXMgYSBzb3VyY2UtbWFwIGNvbW1lbnQuXG4gICAqIEByZXR1cm5zIEEgbWFwIG9mIGFsbCB0aGUgdmFyaWFibGVzIGluIHRoZSBnZW5lcmF0ZWQgY29kZS5cbiAgICovXG4gIGV2YWx1YXRlU3RhdGVtZW50cyhcbiAgICAgIHNvdXJjZVVybDogc3RyaW5nLCBzdGF0ZW1lbnRzOiBvLlN0YXRlbWVudFtdLCByZWZsZWN0b3I6IENvbXBpbGVSZWZsZWN0b3IsXG4gICAgICBjcmVhdGVTb3VyY2VNYXBzOiBib29sZWFuKToge1trZXk6IHN0cmluZ106IGFueX0ge1xuICAgIGNvbnN0IGNvbnZlcnRlciA9IG5ldyBKaXRFbWl0dGVyVmlzaXRvcihyZWZsZWN0b3IpO1xuICAgIGNvbnN0IGN0eCA9IEVtaXR0ZXJWaXNpdG9yQ29udGV4dC5jcmVhdGVSb290KCk7XG4gICAgLy8gRW5zdXJlIGdlbmVyYXRlZCBjb2RlIGlzIGluIHN0cmljdCBtb2RlXG4gICAgaWYgKHN0YXRlbWVudHMubGVuZ3RoID4gMCAmJiAhaXNVc2VTdHJpY3RTdGF0ZW1lbnQoc3RhdGVtZW50c1swXSkpIHtcbiAgICAgIHN0YXRlbWVudHMgPSBbXG4gICAgICAgIG8ubGl0ZXJhbCgndXNlIHN0cmljdCcpLnRvU3RtdCgpLFxuICAgICAgICAuLi5zdGF0ZW1lbnRzLFxuICAgICAgXTtcbiAgICB9XG4gICAgY29udmVydGVyLnZpc2l0QWxsU3RhdGVtZW50cyhzdGF0ZW1lbnRzLCBjdHgpO1xuICAgIGNvbnZlcnRlci5jcmVhdGVSZXR1cm5TdG10KGN0eCk7XG4gICAgcmV0dXJuIHRoaXMuZXZhbHVhdGVDb2RlKHNvdXJjZVVybCwgY3R4LCBjb252ZXJ0ZXIuZ2V0QXJncygpLCBjcmVhdGVTb3VyY2VNYXBzKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBFdmFsdWF0ZSBhIHBpZWNlIG9mIEpJVCBnZW5lcmF0ZWQgY29kZS5cbiAgICogQHBhcmFtIHNvdXJjZVVybCBUaGUgVVJMIG9mIHRoaXMgZ2VuZXJhdGVkIGNvZGUuXG4gICAqIEBwYXJhbSBjdHggQSBjb250ZXh0IG9iamVjdCB0aGF0IGNvbnRhaW5zIGFuIEFTVCBvZiB0aGUgY29kZSB0byBiZSBldmFsdWF0ZWQuXG4gICAqIEBwYXJhbSB2YXJzIEEgbWFwIGNvbnRhaW5pbmcgdGhlIG5hbWVzIGFuZCB2YWx1ZXMgb2YgdmFyaWFibGVzIHRoYXQgdGhlIGV2YWx1YXRlZCBjb2RlIG1pZ2h0XG4gICAqIHJlZmVyZW5jZS5cbiAgICogQHBhcmFtIGNyZWF0ZVNvdXJjZU1hcCBJZiB0cnVlIHRoZW4gY3JlYXRlIGEgc291cmNlLW1hcCBmb3IgdGhlIGdlbmVyYXRlZCBjb2RlIGFuZCBpbmNsdWRlIGl0XG4gICAqIGlubGluZSBhcyBhIHNvdXJjZS1tYXAgY29tbWVudC5cbiAgICogQHJldHVybnMgVGhlIHJlc3VsdCBvZiBldmFsdWF0aW5nIHRoZSBjb2RlLlxuICAgKi9cbiAgZXZhbHVhdGVDb2RlKFxuICAgICAgc291cmNlVXJsOiBzdHJpbmcsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0LCB2YXJzOiB7W2tleTogc3RyaW5nXTogYW55fSxcbiAgICAgIGNyZWF0ZVNvdXJjZU1hcDogYm9vbGVhbik6IGFueSB7XG4gICAgbGV0IGZuQm9keSA9IGBcInVzZSBzdHJpY3RcIjske2N0eC50b1NvdXJjZSgpfVxcbi8vIyBzb3VyY2VVUkw9JHtzb3VyY2VVcmx9YDtcbiAgICBjb25zdCBmbkFyZ05hbWVzOiBzdHJpbmdbXSA9IFtdO1xuICAgIGNvbnN0IGZuQXJnVmFsdWVzOiBhbnlbXSA9IFtdO1xuICAgIGZvciAoY29uc3QgYXJnTmFtZSBpbiB2YXJzKSB7XG4gICAgICBmbkFyZ1ZhbHVlcy5wdXNoKHZhcnNbYXJnTmFtZV0pO1xuICAgICAgZm5BcmdOYW1lcy5wdXNoKGFyZ05hbWUpO1xuICAgIH1cbiAgICBpZiAoY3JlYXRlU291cmNlTWFwKSB7XG4gICAgICAvLyB1c2luZyBgbmV3IEZ1bmN0aW9uKC4uLilgIGdlbmVyYXRlcyBhIGhlYWRlciwgMSBsaW5lIG9mIG5vIGFyZ3VtZW50cywgMiBsaW5lcyBvdGhlcndpc2VcbiAgICAgIC8vIEUuZy4gYGBgXG4gICAgICAvLyBmdW5jdGlvbiBhbm9ueW1vdXMoYSxiLGNcbiAgICAgIC8vIC8qKi8pIHsgLi4uIH1gYGBcbiAgICAgIC8vIFdlIGRvbid0IHdhbnQgdG8gaGFyZCBjb2RlIHRoaXMgZmFjdCwgc28gd2UgYXV0byBkZXRlY3QgaXQgdmlhIGFuIGVtcHR5IGZ1bmN0aW9uIGZpcnN0LlxuICAgICAgY29uc3QgZW1wdHlGbiA9IG5ldyBGdW5jdGlvbiguLi5mbkFyZ05hbWVzLmNvbmNhdCgncmV0dXJuIG51bGw7JykpLnRvU3RyaW5nKCk7XG4gICAgICBjb25zdCBoZWFkZXJMaW5lcyA9IGVtcHR5Rm4uc2xpY2UoMCwgZW1wdHlGbi5pbmRleE9mKCdyZXR1cm4gbnVsbDsnKSkuc3BsaXQoJ1xcbicpLmxlbmd0aCAtIDE7XG4gICAgICBmbkJvZHkgKz0gYFxcbiR7Y3R4LnRvU291cmNlTWFwR2VuZXJhdG9yKHNvdXJjZVVybCwgaGVhZGVyTGluZXMpLnRvSnNDb21tZW50KCl9YDtcbiAgICB9XG4gICAgY29uc3QgZm4gPSBuZXcgRnVuY3Rpb24oLi4uZm5BcmdOYW1lcy5jb25jYXQoZm5Cb2R5KSk7XG4gICAgcmV0dXJuIHRoaXMuZXhlY3V0ZUZ1bmN0aW9uKGZuLCBmbkFyZ1ZhbHVlcyk7XG4gIH1cblxuICAvKipcbiAgICogRXhlY3V0ZSBhIEpJVCBnZW5lcmF0ZWQgZnVuY3Rpb24gYnkgY2FsbGluZyBpdC5cbiAgICpcbiAgICogVGhpcyBtZXRob2QgY2FuIGJlIG92ZXJyaWRkZW4gaW4gdGVzdHMgdG8gY2FwdHVyZSB0aGUgZnVuY3Rpb25zIHRoYXQgYXJlIGdlbmVyYXRlZFxuICAgKiBieSB0aGlzIGBKaXRFdmFsdWF0b3JgIGNsYXNzLlxuICAgKlxuICAgKiBAcGFyYW0gZm4gQSBmdW5jdGlvbiB0byBleGVjdXRlLlxuICAgKiBAcGFyYW0gYXJncyBUaGUgYXJndW1lbnRzIHRvIHBhc3MgdG8gdGhlIGZ1bmN0aW9uIGJlaW5nIGV4ZWN1dGVkLlxuICAgKiBAcmV0dXJucyBUaGUgcmV0dXJuIHZhbHVlIG9mIHRoZSBleGVjdXRlZCBmdW5jdGlvbi5cbiAgICovXG4gIGV4ZWN1dGVGdW5jdGlvbihmbjogRnVuY3Rpb24sIGFyZ3M6IGFueVtdKSB7IHJldHVybiBmbiguLi5hcmdzKTsgfVxufVxuXG4vKipcbiAqIEFuIEFuZ3VsYXIgQVNUIHZpc2l0b3IgdGhhdCBjb252ZXJ0cyBBU1Qgbm9kZXMgaW50byBleGVjdXRhYmxlIEphdmFTY3JpcHQgY29kZS5cbiAqL1xuZXhwb3J0IGNsYXNzIEppdEVtaXR0ZXJWaXNpdG9yIGV4dGVuZHMgQWJzdHJhY3RKc0VtaXR0ZXJWaXNpdG9yIHtcbiAgcHJpdmF0ZSBfZXZhbEFyZ05hbWVzOiBzdHJpbmdbXSA9IFtdO1xuICBwcml2YXRlIF9ldmFsQXJnVmFsdWVzOiBhbnlbXSA9IFtdO1xuICBwcml2YXRlIF9ldmFsRXhwb3J0ZWRWYXJzOiBzdHJpbmdbXSA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKSB7IHN1cGVyKCk7IH1cblxuICBjcmVhdGVSZXR1cm5TdG10KGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KSB7XG4gICAgY29uc3Qgc3RtdCA9IG5ldyBvLlJldHVyblN0YXRlbWVudChuZXcgby5MaXRlcmFsTWFwRXhwcih0aGlzLl9ldmFsRXhwb3J0ZWRWYXJzLm1hcChcbiAgICAgICAgcmVzdWx0VmFyID0+IG5ldyBvLkxpdGVyYWxNYXBFbnRyeShyZXN1bHRWYXIsIG8udmFyaWFibGUocmVzdWx0VmFyKSwgZmFsc2UpKSkpO1xuICAgIHN0bXQudmlzaXRTdGF0ZW1lbnQodGhpcywgY3R4KTtcbiAgfVxuXG4gIGdldEFyZ3MoKToge1trZXk6IHN0cmluZ106IGFueX0ge1xuICAgIGNvbnN0IHJlc3VsdDoge1trZXk6IHN0cmluZ106IGFueX0gPSB7fTtcbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHRoaXMuX2V2YWxBcmdOYW1lcy5sZW5ndGg7IGkrKykge1xuICAgICAgcmVzdWx0W3RoaXMuX2V2YWxBcmdOYW1lc1tpXV0gPSB0aGlzLl9ldmFsQXJnVmFsdWVzW2ldO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgdmlzaXRFeHRlcm5hbEV4cHIoYXN0OiBvLkV4dGVybmFsRXhwciwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIHRoaXMuX2VtaXRSZWZlcmVuY2VUb0V4dGVybmFsKGFzdCwgdGhpcy5yZWZsZWN0b3IucmVzb2x2ZUV4dGVybmFsUmVmZXJlbmNlKGFzdC52YWx1ZSksIGN0eCk7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICB2aXNpdFdyYXBwZWROb2RlRXhwcihhc3Q6IG8uV3JhcHBlZE5vZGVFeHByPGFueT4sIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICB0aGlzLl9lbWl0UmVmZXJlbmNlVG9FeHRlcm5hbChhc3QsIGFzdC5ub2RlLCBjdHgpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgdmlzaXREZWNsYXJlVmFyU3RtdChzdG10OiBvLkRlY2xhcmVWYXJTdG10LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgaWYgKHN0bXQuaGFzTW9kaWZpZXIoby5TdG10TW9kaWZpZXIuRXhwb3J0ZWQpKSB7XG4gICAgICB0aGlzLl9ldmFsRXhwb3J0ZWRWYXJzLnB1c2goc3RtdC5uYW1lKTtcbiAgICB9XG4gICAgcmV0dXJuIHN1cGVyLnZpc2l0RGVjbGFyZVZhclN0bXQoc3RtdCwgY3R4KTtcbiAgfVxuXG4gIHZpc2l0RGVjbGFyZUZ1bmN0aW9uU3RtdChzdG10OiBvLkRlY2xhcmVGdW5jdGlvblN0bXQsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBpZiAoc3RtdC5oYXNNb2RpZmllcihvLlN0bXRNb2RpZmllci5FeHBvcnRlZCkpIHtcbiAgICAgIHRoaXMuX2V2YWxFeHBvcnRlZFZhcnMucHVzaChzdG10Lm5hbWUpO1xuICAgIH1cbiAgICByZXR1cm4gc3VwZXIudmlzaXREZWNsYXJlRnVuY3Rpb25TdG10KHN0bXQsIGN0eCk7XG4gIH1cblxuICB2aXNpdERlY2xhcmVDbGFzc1N0bXQoc3RtdDogby5DbGFzc1N0bXQsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBpZiAoc3RtdC5oYXNNb2RpZmllcihvLlN0bXRNb2RpZmllci5FeHBvcnRlZCkpIHtcbiAgICAgIHRoaXMuX2V2YWxFeHBvcnRlZFZhcnMucHVzaChzdG10Lm5hbWUpO1xuICAgIH1cbiAgICByZXR1cm4gc3VwZXIudmlzaXREZWNsYXJlQ2xhc3NTdG10KHN0bXQsIGN0eCk7XG4gIH1cblxuICBwcml2YXRlIF9lbWl0UmVmZXJlbmNlVG9FeHRlcm5hbChhc3Q6IG8uRXhwcmVzc2lvbiwgdmFsdWU6IGFueSwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOlxuICAgICAgdm9pZCB7XG4gICAgbGV0IGlkID0gdGhpcy5fZXZhbEFyZ1ZhbHVlcy5pbmRleE9mKHZhbHVlKTtcbiAgICBpZiAoaWQgPT09IC0xKSB7XG4gICAgICBpZCA9IHRoaXMuX2V2YWxBcmdWYWx1ZXMubGVuZ3RoO1xuICAgICAgdGhpcy5fZXZhbEFyZ1ZhbHVlcy5wdXNoKHZhbHVlKTtcbiAgICAgIGNvbnN0IG5hbWUgPSBpZGVudGlmaWVyTmFtZSh7cmVmZXJlbmNlOiB2YWx1ZX0pIHx8ICd2YWwnO1xuICAgICAgdGhpcy5fZXZhbEFyZ05hbWVzLnB1c2goYGppdF8ke25hbWV9XyR7aWR9YCk7XG4gICAgfVxuICAgIGN0eC5wcmludChhc3QsIHRoaXMuX2V2YWxBcmdOYW1lc1tpZF0pO1xuICB9XG59XG5cblxuZnVuY3Rpb24gaXNVc2VTdHJpY3RTdGF0ZW1lbnQoc3RhdGVtZW50OiBvLlN0YXRlbWVudCk6IGJvb2xlYW4ge1xuICByZXR1cm4gc3RhdGVtZW50LmlzRXF1aXZhbGVudChvLmxpdGVyYWwoJ3VzZSBzdHJpY3QnKS50b1N0bXQoKSk7XG59XG4iXX0=