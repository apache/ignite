/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { EmitterVisitorContext } from './abstract_emitter';
import { AbstractJsEmitterVisitor } from './abstract_js_emitter';
import * as o from './output_ast';
var JavaScriptEmitter = /** @class */ (function () {
    function JavaScriptEmitter() {
    }
    JavaScriptEmitter.prototype.emitStatements = function (genFilePath, stmts, preamble) {
        if (preamble === void 0) { preamble = ''; }
        var converter = new JsEmitterVisitor();
        var ctx = EmitterVisitorContext.createRoot();
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
export { JavaScriptEmitter };
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
}(AbstractJsEmitterVisitor));
function exportVar(varName) {
    return "Object.defineProperty(exports, '" + varName + "', { get: function() { return " + varName + "; }});";
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoianNfZW1pdHRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9vdXRwdXQvanNfZW1pdHRlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBTUgsT0FBTyxFQUFDLHFCQUFxQixFQUFnQixNQUFNLG9CQUFvQixDQUFDO0FBQ3hFLE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLHVCQUF1QixDQUFDO0FBQy9ELE9BQU8sS0FBSyxDQUFDLE1BQU0sY0FBYyxDQUFDO0FBRWxDO0lBQUE7SUFzQkEsQ0FBQztJQXJCQywwQ0FBYyxHQUFkLFVBQWUsV0FBbUIsRUFBRSxLQUFvQixFQUFFLFFBQXFCO1FBQXJCLHlCQUFBLEVBQUEsYUFBcUI7UUFDN0UsSUFBTSxTQUFTLEdBQUcsSUFBSSxnQkFBZ0IsRUFBRSxDQUFDO1FBQ3pDLElBQU0sR0FBRyxHQUFHLHFCQUFxQixDQUFDLFVBQVUsRUFBRSxDQUFDO1FBQy9DLFNBQVMsQ0FBQyxrQkFBa0IsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQUM7UUFFekMsSUFBTSxhQUFhLEdBQUcsUUFBUSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxFQUFFLENBQUM7UUFDM0QsU0FBUyxDQUFDLG1CQUFtQixDQUFDLE9BQU8sQ0FBQyxVQUFDLE1BQU0sRUFBRSxrQkFBa0I7WUFDL0QseUZBQXlGO1lBQ3pGLGFBQWEsQ0FBQyxJQUFJLENBQ2QsU0FBTyxNQUFNLFdBQVE7aUJBQ3JCLFdBQVMsa0JBQWtCLFFBQUssQ0FBQSxDQUFDLENBQUM7UUFDeEMsQ0FBQyxDQUFDLENBQUM7UUFFSCxJQUFNLEVBQUUsR0FBRyxHQUFHLENBQUMsb0JBQW9CLENBQUMsV0FBVyxFQUFFLGFBQWEsQ0FBQyxNQUFNLENBQUMsQ0FBQyxXQUFXLEVBQUUs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LiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG5pbXBvcnQge1N0YXRpY1N5bWJvbH0gZnJvbSAnLi4vYW90L3N0YXRpY19zeW1ib2wnO1xuaW1wb3J0IHtDb21waWxlSWRlbnRpZmllck1ldGFkYXRhfSBmcm9tICcuLi9jb21waWxlX21ldGFkYXRhJztcblxuaW1wb3J0IHtFbWl0dGVyVmlzaXRvckNvbnRleHQsIE91dHB1dEVtaXR0ZXJ9IGZyb20gJy4vYWJzdHJhY3RfZW1pdHRlcic7XG5pbXBvcnQge0Fic3RyYWN0SnNFbWl0dGVyVmlzaXRvcn0gZnJvbSAnLi9hYnN0cmFjdF9qc19lbWl0dGVyJztcbmltcG9ydCAqIGFzIG8gZnJvbSAnLi9vdXRwdXRfYXN0JztcblxuZXhwb3J0IGNsYXNzIEphdmFTY3JpcHRFbWl0dGVyIGltcGxlbWVudHMgT3V0cHV0RW1pdHRlciB7XG4gIGVtaXRTdGF0ZW1lbnRzKGdlbkZpbGVQYXRoOiBzdHJpbmcsIHN0bXRzOiBvLlN0YXRlbWVudFtdLCBwcmVhbWJsZTogc3RyaW5nID0gJycpOiBzdHJpbmcge1xuICAgIGNvbnN0IGNvbnZlcnRlciA9IG5ldyBKc0VtaXR0ZXJWaXNpdG9yKCk7XG4gICAgY29uc3QgY3R4ID0gRW1pdHRlclZpc2l0b3JDb250ZXh0LmNyZWF0ZVJvb3QoKTtcbiAgICBjb252ZXJ0ZXIudmlzaXRBbGxTdGF0ZW1lbnRzKHN0bXRzLCBjdHgpO1xuXG4gICAgY29uc3QgcHJlYW1ibGVMaW5lcyA9IHByZWFtYmxlID8gcHJlYW1ibGUuc3BsaXQoJ1xcbicpIDogW107XG4gICAgY29udmVydGVyLmltcG9ydHNXaXRoUHJlZml4ZXMuZm9yRWFjaCgocHJlZml4LCBpbXBvcnRlZE1vZHVsZU5hbWUpID0+IHtcbiAgICAgIC8vIE5vdGU6IGNhbid0IHdyaXRlIHRoZSByZWFsIHdvcmQgZm9yIGltcG9ydCBhcyBpdCBzY3Jld3MgdXAgc3lzdGVtLmpzIGF1dG8gZGV0ZWN0aW9uLi4uXG4gICAgICBwcmVhbWJsZUxpbmVzLnB1c2goXG4gICAgICAgICAgYHZhciAke3ByZWZpeH0gPSByZXFgICtcbiAgICAgICAgICBgdWlyZSgnJHtpbXBvcnRlZE1vZHVsZU5hbWV9Jyk7YCk7XG4gICAgfSk7XG5cbiAgICBjb25zdCBzbSA9IGN0eC50b1NvdXJjZU1hcEdlbmVyYXRvcihnZW5GaWxlUGF0aCwgcHJlYW1ibGVMaW5lcy5sZW5ndGgpLnRvSnNDb21tZW50KCk7XG4gICAgY29uc3QgbGluZXMgPSBbLi4ucHJlYW1ibGVMaW5lcywgY3R4LnRvU291cmNlKCksIHNtXTtcbiAgICBpZiAoc20pIHtcbiAgICAgIC8vIGFsd2F5cyBhZGQgYSBuZXdsaW5lIGF0IHRoZSBlbmQsIGFzIHNvbWUgdG9vbHMgaGF2ZSBidWdzIHdpdGhvdXQgaXQuXG4gICAgICBsaW5lcy5wdXNoKCcnKTtcbiAgICB9XG4gICAgcmV0dXJuIGxpbmVzLmpvaW4oJ1xcbicpO1xuICB9XG59XG5cbmNsYXNzIEpzRW1pdHRlclZpc2l0b3IgZXh0ZW5kcyBBYnN0cmFjdEpzRW1pdHRlclZpc2l0b3Ige1xuICBpbXBvcnRzV2l0aFByZWZpeGVzID0gbmV3IE1hcDxzdHJpbmcsIHN0cmluZz4oKTtcblxuICB2aXNpdEV4dGVybmFsRXhwcihhc3Q6IG8uRXh0ZXJuYWxFeHByLCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgY29uc3Qge25hbWUsIG1vZHVsZU5hbWV9ID0gYXN0LnZhbHVlO1xuICAgIGlmIChtb2R1bGVOYW1lKSB7XG4gICAgICBsZXQgcHJlZml4ID0gdGhpcy5pbXBvcnRzV2l0aFByZWZpeGVzLmdldChtb2R1bGVOYW1lKTtcbiAgICAgIGlmIChwcmVmaXggPT0gbnVsbCkge1xuICAgICAgICBwcmVmaXggPSBgaSR7dGhpcy5pbXBvcnRzV2l0aFByZWZpeGVzLnNpemV9YDtcbiAgICAgICAgdGhpcy5pbXBvcnRzV2l0aFByZWZpeGVzLnNldChtb2R1bGVOYW1lLCBwcmVmaXgpO1xuICAgICAgfVxuICAgICAgY3R4LnByaW50KGFzdCwgYCR7cHJlZml4fS5gKTtcbiAgICB9XG4gICAgY3R4LnByaW50KGFzdCwgbmFtZSAhKTtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICB2aXNpdERlY2xhcmVWYXJTdG10KHN0bXQ6IG8uRGVjbGFyZVZhclN0bXQsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBzdXBlci52aXNpdERlY2xhcmVWYXJTdG10KHN0bXQsIGN0eCk7XG4gICAgaWYgKHN0bXQuaGFzTW9kaWZpZXIoby5TdG10TW9kaWZpZXIuRXhwb3J0ZWQpKSB7XG4gICAgICBjdHgucHJpbnRsbihzdG10LCBleHBvcnRWYXIoc3RtdC5uYW1lKSk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG4gIHZpc2l0RGVjbGFyZUZ1bmN0aW9uU3RtdChzdG10OiBvLkRlY2xhcmVGdW5jdGlvblN0bXQsIGN0eDogRW1pdHRlclZpc2l0b3JDb250ZXh0KTogYW55IHtcbiAgICBzdXBlci52aXNpdERlY2xhcmVGdW5jdGlvblN0bXQoc3RtdCwgY3R4KTtcbiAgICBpZiAoc3RtdC5oYXNNb2RpZmllcihvLlN0bXRNb2RpZmllci5FeHBvcnRlZCkpIHtcbiAgICAgIGN0eC5wcmludGxuKHN0bXQsIGV4cG9ydFZhcihzdG10Lm5hbWUpKTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgdmlzaXREZWNsYXJlQ2xhc3NTdG10KHN0bXQ6IG8uQ2xhc3NTdG10LCBjdHg6IEVtaXR0ZXJWaXNpdG9yQ29udGV4dCk6IGFueSB7XG4gICAgc3VwZXIudmlzaXREZWNsYXJlQ2xhc3NTdG10KHN0bXQsIGN0eCk7XG4gICAgaWYgKHN0bXQuaGFzTW9kaWZpZXIoby5TdG10TW9kaWZpZXIuRXhwb3J0ZWQpKSB7XG4gICAgICBjdHgucHJpbnRsbihzdG10LCBleHBvcnRWYXIoc3RtdC5uYW1lKSk7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbmZ1bmN0aW9uIGV4cG9ydFZhcih2YXJOYW1lOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gYE9iamVjdC5kZWZpbmVQcm9wZXJ0eShleHBvcnRzLCAnJHt2YXJOYW1lfScsIHsgZ2V0OiBmdW5jdGlvbigpIHsgcmV0dXJuICR7dmFyTmFtZX07IH19KTtgO1xufVxuIl19