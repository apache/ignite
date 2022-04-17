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
        define("@angular/compiler/src/output/js_emitter", ["require", "exports", "tslib", "@angular/compiler/src/output/abstract_emitter", "@angular/compiler/src/output/abstract_js_emitter", "@angular/compiler/src/output/output_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var abstract_emitter_1 = require("@angular/compiler/src/output/abstract_emitter");
    var abstract_js_emitter_1 = require("@angular/compiler/src/output/abstract_js_emitter");
    var o = require("@angular/compiler/src/output/output_ast");
    var JavaScriptEmitter = /** @class */ (function () {
        function JavaScriptEmitter() {
        }
        JavaScriptEmitter.prototype.emitStatements = function (genFilePath, stmts, preamble) {
            if (preamble === void 0) { preamble = ''; }
            var converter = new JsEmitterVisitor();
            var ctx = abstract_emitter_1.EmitterVisitorContext.createRoot();
            converter.visitAllStatements(stmts, ctx);
            var preambleLines = preamble ? preamble.split('\n') : [];
            converter.importsWithPrefixes.forEach(function (prefix, importedModuleName) {
                // Note: can't write the real word for import as it screws up system.js auto detection...
                preambleLines.push("var " + prefix + " = req" +
                    ("uire('" + importedModuleName + "');"));
            });
            var sm = ctx.toSourceMapGenerator(genFilePath, preambleLines.length).toJsComment();
            var lines = tslib_1.__spread(preambleLines, [ctx.toSource(), sm]);
            if (sm) {
                // always add a newline at the end, as some tools have bugs without it.
                lines.push('');
            }
            return lines.join('\n');
        };
        return JavaScriptEmitter;
    }());
    exports.JavaScriptEmitter = JavaScriptEmitter;
    var JsEmitterVisitor = /** @class */ (function (_super) {
        tslib_1.__extends(JsEmitterVisitor, _super);
        function JsEmitterVisitor() {
            var _this = _super !== null && _super.apply(this, arguments) || this;
            _this.importsWithPrefixes = new Map();
            return _this;
        }
        JsEmitterVisitor.prototype.visitExternalExpr = function (ast, ctx) {
            var _a = ast.value, name = _a.name, moduleName = _a.moduleName;
            if (moduleName) {
                var prefix = this.importsWithPrefixes.get(moduleName);
                if (prefix == null) {
                    prefix = "i" + this.importsWithPrefixes.size;
                    this.importsWithPrefixes.set(moduleName, prefix);
                }
                ctx.print(ast, prefix + ".");
            }
            ctx.print(ast, name);
            return null;
        };
        JsEmitterVisitor.prototype.visitDeclareVarStmt = function (stmt, ctx) {
            _super.prototype.visitDeclareVarStmt.call(this, stmt, ctx);
            if (stmt.hasModifier(o.StmtModifier.Exported)) {
                ctx.println(stmt, exportVar(stmt.name));
            }
            return null;
        };
        JsEmitterVisitor.prototype.visitDeclareFunctionStmt = function (stmt, ctx) {
            _super.prototype.visitDeclareFunctionStmt.call(this, stmt, ctx);
            if (stmt.hasModifier(o.StmtModifier.Exported)) {
                ctx.println(stmt, exportVar(stmt.name));
            }
            return null;
        };
        JsEmitterVisitor.prototype.visitDeclareClassStmt = function (stmt, ctx) {
            _super.prototype.visitDeclareClassStmt.call(this, stmt, ctx);
            if (stmt.hasModifier(o.StmtModifier.Exported)) {
                ctx.println(stmt, exportVar(stmt.name));
            }
            return null;
        };
        return JsEmitterVisitor;
    }(abstract_js_emitter_1.AbstractJsEmitterVisitor));
    function exportVar(varName) {
        return "Object.defineProperty(exports, '" + varName + "', { get: function() { return " + varName + "; }});";
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoianNfZW1pdHRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9vdXRwdXQvanNfZW1pdHRlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFNSCxrRkFBd0U7SUFDeEUsd0ZBQStEO0lBQy9ELDJEQUFrQztJQUVsQztRQUFBO1FBc0JBLENBQUM7UUFyQkMsMENBQWMsR0FBZCxVQUFlLFdBQW1CLEVBQUUsS0FBb0IsRUFBRSxRQUFxQjtZQUFyQix5QkFBQSxFQUFBLGFBQXFCO1lBQzdFLElBQU0sU0FBUyxHQUFHLElBQUksZ0JBQWdCLEVBQUUsQ0FBQztZQUN6QyxJQUFNLEdBQUcsR0FBRyx3Q0FBcUIsQ0FBQyxVQUFVLEVBQUUsQ0FBQztZQUMvQyxTQUFTLENBQUMsa0JBQWtCLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxDQUFDO1lBRXpDLElBQU0sYUFBYSxHQUFHLFFBQVEsQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO1lBQzNELFNBQVMsQ0FBQyxtQkFBbUIsQ0FBQyxPQUFPLENBQUMsVUFBQyxNQUFNLEVBQUUsa0JBQWtCO2dCQUMvRCx5RkFBeUY7Z0JBQ3pGLGFBQWEsQ0FBQyxJQUFJLENBQ2QsU0FBTyxNQUFNLFdBQVE7cUJBQ3JCLFdBQVMsa0JBQWtCLFFBQUssQ0FBQSxDQUFDLENBQUM7WUFDeEMsQ0FBQyxDQUFDLENBQUM7WUFFSCxJQUFNLEVBQUUsR0FBRyxHQUFHLENBQUMsb0JBQW9CLENBQUMsV0FBVyxFQUFFLGFBQWEsQ0FBQyxNQUFNLENBQUMsQ0FBQyxXQUFXLEVBQUUsQ0FBQztZQUNyRixJQUFNLEtBQUssb0JBQU8sYUFBYSxHQUFFLEdBQUcsQ0FBQyxRQUFRLEVBQUUsRUFBRSxFQUFFLEVBQUMsQ0FBQztZQUNyRCxJQUFJLEVBQUUsRUFBRTtnQkFDTix1RUFBdUU7Z0JBQ3ZFLEtBQUssQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7YUFDaEI7WUFDRCxPQUFPLEtBQUssQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDMUIsQ0FBQztRQUNILHdCQUFDO0lBQUQsQ0FBQyxBQXRCRCxJQXNCQztJQXRCWSw4Q0FBaUI7SUF3QjlCO1FBQStCLDRDQUF3QjtRQUF2RDtZQUFBLHFFQXFDQztZQXBDQyx5QkFBbUIsR0FBRyxJQUFJLEdBQUcsRUFBa0IsQ0FBQzs7UUFvQ2xELENBQUM7UUFsQ0MsNENBQWlCLEdBQWpCLFVBQWtCLEdBQW1CLEVBQUUsR0FBMEI7WUFDekQsSUFBQSxjQUE4QixFQUE3QixjQUFJLEVBQUUsMEJBQXVCLENBQUM7WUFDckMsSUFBSSxVQUFVLEVBQUU7Z0JBQ2QsSUFBSSxNQUFNLEdBQUcsSUFBSSxDQUFDLG1CQUFtQixDQUFDLEdBQUcsQ0FBQyxVQUFVLENBQUMsQ0FBQztnQkFDdEQsSUFBSSxNQUFNLElBQUksSUFBSSxFQUFFO29CQUNsQixNQUFNLEdBQUcsTUFBSSxJQUFJLENBQUMsbUJBQW1CLENBQUMsSUFBTSxDQUFDO29CQUM3QyxJQUFJLENBQUMsbUJBQW1CLENBQUMsR0FBRyxDQUFDLFVBQVUsRUFBRSxNQUFNLENBQUMsQ0FBQztpQkFDbEQ7Z0JBQ0QsR0FBRyxDQUFDLEtBQUssQ0FBQyxHQUFHLEVBQUssTUFBTSxNQUFHLENBQUMsQ0FBQzthQUM5QjtZQUNELEdBQUcsQ0FBQyxLQUFLLENBQUMsR0FBRyxFQUFFLElBQU0sQ0FBQyxDQUFDO1lBQ3ZCLE9BQU8sSUFBSSxDQUFDO1FBQ2QsQ0FBQztRQUNELDhDQUFtQixHQUFuQixVQUFvQixJQUFzQixFQUFFLEdBQTBCO1lBQ3BFLGlCQUFNLG1CQUFtQixZQUFDLElBQUksRUFBRSxHQUFHLENBQUMsQ0FBQztZQUNyQyxJQUFJLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQyxDQUFDLFlBQVksQ0FBQyxRQUFRLENBQUMsRUFBRTtnQkFDN0MsR0FBRyxDQUFDLE9BQU8sQ0FBQyxJQUFJLEVBQUUsU0FBUyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO2FBQ3pDO1lBQ0QsT0FBTyxJQUFJLENBQUM7UUFDZCxDQUFDO1FBQ0QsbURBQXdCLEdBQXhCLFVBQXlCLElBQTJCLEVBQUUsR0FBMEI7WUFDOUUsaUJBQU0sd0JBQXdCLFlBQUMsSUFBSSxFQUFFLEdBQUcsQ0FBQyxDQUFDO1lBQzFDLElBQUksSUFBSSxDQUFDLFdBQVcsQ0FBQyxDQUFDLENBQUMsWUFBWSxDQUFDLFFBQVEsQ0FBQyxFQUFFO2dCQUM3QyxHQUFHLENBQUMsT0FBTyxDQUFDLElBQUksRUFBRSxTQUFTLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUM7YUFDekM7WUFDRCxPQUFPLElBQUksQ0FBQztRQUNkLENBQUM7UUFDRCxnREFBcUIsR0FBckIsVUFBc0IsSUFBaUIsRUFBRSxHQUEwQjtZQUNqRSxpQkFBTSxxQkFBcUIsWUFBQyxJQUFJLEVBQUUsR0FBRyxDQUFDLENBQUM7WUFDdkMsSUFBSSxJQUFJLENBQUMsV0FBVyxDQUFDLENBQUMsQ0FBQyxZQUFZLENBQUMsUUFBUSxDQUFDLEVBQUU7Z0JBQzdDLEdBQUcsQ0FBQyxPQUFPLENBQUMsSUFBSSxFQUFFLFNBQVMsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQzthQUN6QztZQUNELE9BQU8sSUFBSSxDQUFDO1FBQ2QsQ0FBQztRQUNILHVCQUFDO0lBQUQsQ0FBQyxBQXJDRCxDQUErQiw4Q0FBd0IsR0FxQ3REO0lBRUQsU0FBUyxTQUFTLENBQUMsT0FBZTtRQUNoQyxPQUFPLHFDQUFtQyxPQUFPLHNDQUFpQyxPQUFPLFdBQVEsQ0FBQztJQUNwRyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5cbmltcG9ydCB7U3RhdGljU3ltYm9sfSBmcm9tICcuLi9hb3Qvc3RhdGljX3N5bWJvbCc7XG5pbXBvcnQge0NvbXBpbGVJZGVudGlmaWVyTWV0YWRhdGF9IGZyb20gJy4uL2NvbXBpbGVfbWV0YWRhdGEnO1xuXG5pbXBvcnQge0VtaXR0ZXJWaXNpdG9yQ29udGV4dCwgT3V0cHV0RW1pdHRlcn0gZnJvbSAnLi9hYnN0cmFjdF9lbWl0dGVyJztcbmltcG9ydCB7QWJzdHJhY3RKc0VtaXR0ZXJWaXNpdG9yfSBmcm9tICcuL2Fic3RyYWN0X2pzX2VtaXR0ZXInO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuL291dHB1dF9hc3QnO1xuXG5leHBvcnQgY2xhc3MgSmF2YVNjcmlwdEVtaXR0ZXIgaW1wbGVtZW50cyBPdXRwdXRFbWl0dGVyIHtcbiAgZW1pdFN0YXRlbWVudHMoZ2VuRmlsZVBhdGg6IHN0cmluZywgc3RtdHM6IG8uU3RhdGVtZW50W10sIHByZWFtYmxlOiBzdHJpbmcgPSAnJyk6IHN0cmluZyB7XG4gICAgY29uc3QgY29udmVydGVyID0gbmV3IEpzRW1pdHRlclZpc2l0b3IoKTtcbiAgICBjb25zdCBjdHggPSBFbWl0dGVyVmlzaXRvckNvbnRleHQuY3JlYXRlUm9vdCgpO1xuICAgIGNvbnZlcnRlci52aXNpdEFsbFN0YXRlbWVudHMoc3RtdHMsIGN0eCk7XG5cbiAgICBjb25zdCBwcmVhbWJsZUxpbmVzID0gcHJlYW1ibGUgPyBwcmVhbWJsZS5zcGxpdCgnXFxuJykgOiBbXTtcbiAgICBjb252ZXJ0ZXIuaW1wb3J0c1dpdGhQcmVmaXhlcy5mb3JFYWNoKChwcmVmaXgsIGltcG9ydGVkTW9kdWxlTmFtZSkgPT4ge1xuICAgICAgLy8gTm90ZTogY2FuJ3Qgd3JpdGUgdGhlIHJlYWwgd29yZCBmb3IgaW1wb3J0IGFzIGl0IHNjcmV3cyB1cCBzeXN0ZW0uanMgYXV0byBkZXRlY3Rpb24uLi5cbiAgICAgIHByZWFtYmxlTGluZXMucHVzaChcbiAgICAgICAgICBgdmFyICR7cHJlZml4fSA9IHJlcWAgK1xuICAgICAgICAgIGB1aXJlKCcke2ltcG9ydGVkTW9kdWxlTmFtZX0nKTtgKTtcbiAgICB9KTtcblxuICAgIGNvbnN0IHNtID0gY3R4LnRvU291cmNlTWFwR2VuZXJhdG9yKGdlbkZpbGVQYXRoLCBwcmVhbWJsZUxpbmVzLmxlbmd0aCkudG9Kc0NvbW1lbnQoKTtcbiAgICBjb25zdCBsaW5lcyA9IFsuLi5wcmVhbWJsZUxpbmVzLCBjdHgudG9Tb3VyY2UoKSwgc21dO1xuICAgIGlmIChzbSkge1xuICAgICAgLy8gYWx3YXlzIGFkZCBhIG5ld2xpbmUgYXQgdGhlIGVuZCwgYXMgc29tZSB0b29scyBoYXZlIGJ1Z3Mgd2l0aG91dCBpdC5cbiAgICAgIGxpbmVzLnB1c2goJycpO1xuICAgIH1cbiAgICByZXR1cm4gbGluZXMuam9pbignXFxuJyk7XG4gIH1cbn1cblxuY2xhc3MgSnNFbWl0dGVyVmlzaXRvciBleHRlbmRzIEFic3RyYWN0SnNFbWl0dGVyVmlzaXRvciB7XG4gIGltcG9ydHNXaXRoUHJlZml4ZXMgPSBuZXcgTWFwPHN0cmluZywgc3RyaW5nPigpO1xuXG4gIHZpc2l0RXh0ZXJuYWxFeHByKGFzdDogby5FeHRlcm5hbEV4cHIsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBjb25zdCB7bmFtZSwgbW9kdWxlTmFtZX0gPSBhc3QudmFsdWU7XG4gICAgaWYgKG1vZHVsZU5hbWUpIHtcbiAgICAgIGxldCBwcmVmaXggPSB0aGlzLmltcG9ydHNXaXRoUHJlZml4ZXMuZ2V0KG1vZHVsZU5hbWUpO1xuICAgICAgaWYgKHByZWZpeCA9PSBudWxsKSB7XG4gICAgICAgIHByZWZpeCA9IGBpJHt0aGlzLmltcG9ydHNXaXRoUHJlZml4ZXMuc2l6ZX1gO1xuICAgICAgICB0aGlzLmltcG9ydHNXaXRoUHJlZml4ZXMuc2V0KG1vZHVsZU5hbWUsIHByZWZpeCk7XG4gICAgICB9XG4gICAgICBjdHgucHJpbnQoYXN0LCBgJHtwcmVmaXh9LmApO1xuICAgIH1cbiAgICBjdHgucHJpbnQoYXN0LCBuYW1lICEpO1xuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0RGVjbGFyZVZhclN0bXQoc3RtdDogby5EZWNsYXJlVmFyU3RtdCwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIHN1cGVyLnZpc2l0RGVjbGFyZVZhclN0bXQoc3RtdCwgY3R4KTtcbiAgICBpZiAoc3RtdC5oYXNNb2RpZmllcihvLlN0bXRNb2RpZmllci5FeHBvcnRlZCkpIHtcbiAgICAgIGN0eC5wcmludGxuKHN0bXQsIGV4cG9ydFZhcihzdG10Lm5hbWUpKTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgdmlzaXREZWNsYXJlRnVuY3Rpb25TdG10KHN0bXQ6IG8uRGVjbGFyZUZ1bmN0aW9uU3RtdCwgY3R4OiBFbWl0dGVyVmlzaXRvckNvbnRleHQpOiBhbnkge1xuICAgIHN1cGVyLnZpc2l0RGVjbGFyZUZ1bmN0aW9uU3RtdChzdG10LCBjdHgpO1xuICAgIGlmIChzdG10Lmhhc01vZGlmaWVyKG8uU3RtdE1vZGlmaWVyLkV4cG9ydGVkKSkge1xuICAgICAgY3R4LnByaW50bG4oc3RtdCwgZXhwb3J0VmFyKHN0bXQubmFtZSkpO1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICB2aXNpdERlY2xhcmVDbGFzc1N0bXQoc3RtdDogby5DbGFzc1N0bXQsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBzdXBlci52aXNpdERlY2xhcmVDbGFzc1N0bXQoc3RtdCwgY3R4KTtcbiAgICBpZiAoc3RtdC5oYXNNb2RpZmllcihvLlN0bXRNb2RpZmllci5FeHBvcnRlZCkpIHtcbiAgICAgIGN0eC5wcmludGxuKHN0bXQsIGV4cG9ydFZhcihzdG10Lm5hbWUpKTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbn1cblxuZnVuY3Rpb24gZXhwb3J0VmFyKHZhck5hbWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBgT2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsICcke3Zhck5hbWV9JywgeyBnZXQ6IGZ1bmN0aW9uKCkgeyByZXR1cm4gJHt2YXJOYW1lfTsgfX0pO2A7XG59XG4iXX0=