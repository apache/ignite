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
        define("@angular/compiler/src/render3/r3_module_factory_compiler", ["require", "exports", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/render3/r3_identifiers"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var o = require("@angular/compiler/src/output/output_ast");
    var r3_identifiers_1 = require("@angular/compiler/src/render3/r3_identifiers");
    /**
     * Write a Renderer2 compatibility module factory to the output context.
     */
    function compileModuleFactory(outputCtx, module, backPatchReferenceOf, resolver) {
        var ngModuleFactoryVar = compile_metadata_1.identifierName(module.type) + "NgFactory";
        var parentInjector = 'parentInjector';
        var createFunction = o.fn([new o.FnParam(parentInjector, o.DYNAMIC_TYPE)], [new o.IfStmt(o.THIS_EXPR.prop(r3_identifiers_1.Identifiers.PATCH_DEPS).notIdentical(o.literal(true, o.INFERRED_TYPE)), [
                o.THIS_EXPR.prop(r3_identifiers_1.Identifiers.PATCH_DEPS).set(o.literal(true, o.INFERRED_TYPE)).toStmt(),
                backPatchReferenceOf(module.type).callFn([]).toStmt()
            ])], o.INFERRED_TYPE, null, ngModuleFactoryVar + "_Create");
        var moduleFactoryLiteral = o.literalMap([
            { key: 'moduleType', value: outputCtx.importExpr(module.type.reference), quoted: false },
            { key: 'create', value: createFunction, quoted: false }
        ]);
        outputCtx.statements.push(o.variable(ngModuleFactoryVar).set(moduleFactoryLiteral).toDeclStmt(o.DYNAMIC_TYPE, [
            o.StmtModifier.Exported, o.StmtModifier.Final
        ]));
    }
    exports.compileModuleFactory = compileModuleFactory;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicjNfbW9kdWxlX2ZhY3RvcnlfY29tcGlsZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvcmVuZGVyMy9yM19tb2R1bGVfZmFjdG9yeV9jb21waWxlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILDJFQUFpRztJQUVqRywyREFBMEM7SUFHMUMsK0VBQW1EO0lBRW5EOztPQUVHO0lBQ0gsU0FBZ0Isb0JBQW9CLENBQ2hDLFNBQXdCLEVBQUUsTUFBK0IsRUFDekQsb0JBQW1FLEVBQ25FLFFBQWlDO1FBQ25DLElBQU0sa0JBQWtCLEdBQU0saUNBQWMsQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLGNBQVcsQ0FBQztRQUVyRSxJQUFNLGNBQWMsR0FBRyxnQkFBZ0IsQ0FBQztRQUN4QyxJQUFNLGNBQWMsR0FBRyxDQUFDLENBQUMsRUFBRSxDQUN2QixDQUFDLElBQUksQ0FBQyxDQUFDLE9BQU8sQ0FBQyxjQUFjLEVBQUUsQ0FBQyxDQUFDLFlBQVksQ0FBQyxDQUFDLEVBQy9DLENBQUMsSUFBSSxDQUFDLENBQUMsTUFBTSxDQUNULENBQUMsQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLDRCQUFFLENBQUMsVUFBVSxDQUFDLENBQUMsWUFBWSxDQUFDLENBQUMsQ0FBQyxPQUFPLENBQUMsSUFBSSxFQUFFLENBQUMsQ0FBQyxhQUFhLENBQUMsQ0FBQyxFQUM5RTtnQkFDRSxDQUFDLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyw0QkFBRSxDQUFDLFVBQVUsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsT0FBTyxDQUFDLElBQUksRUFBRSxDQUFDLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxNQUFNLEVBQUU7Z0JBQzlFLG9CQUFvQixDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsQ0FBQyxNQUFNLENBQUMsRUFBRSxDQUFDLENBQUMsTUFBTSxFQUFFO2FBQ3RELENBQUMsQ0FBQyxFQUNQLENBQUMsQ0FBQyxhQUFhLEVBQUUsSUFBSSxFQUFLLGtCQUFrQixZQUFTLENBQUMsQ0FBQztRQUUzRCxJQUFNLG9CQUFvQixHQUFHLENBQUMsQ0FBQyxVQUFVLENBQUM7WUFDeEMsRUFBQyxHQUFHLEVBQUUsWUFBWSxFQUFFLEtBQUssRUFBRSxTQUFTLENBQUMsVUFBVSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLEVBQUUsTUFBTSxFQUFFLEtBQUssRUFBQztZQUN0RixFQUFDLEdBQUcsRUFBRSxRQUFRLEVBQUUsS0FBSyxFQUFFLGNBQWMsRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFDO1NBQ3RELENBQUMsQ0FBQztRQUVILFNBQVMsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUNyQixDQUFDLENBQUMsUUFBUSxDQUFDLGtCQUFrQixDQUFDLENBQUMsR0FBRyxDQUFDLG9CQUFvQixDQUFDLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxZQUFZLEVBQUU7WUFDbEYsQ0FBQyxDQUFDLFlBQVksQ0FBQyxRQUFRLEVBQUUsQ0FBQyxDQUFDLFlBQVksQ0FBQyxLQUFLO1NBQzlDLENBQUMsQ0FBQyxDQUFDO0lBQ1YsQ0FBQztJQTFCRCxvREEwQkMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Q29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsIENvbXBpbGVUeXBlTWV0YWRhdGEsIGlkZW50aWZpZXJOYW1lfSBmcm9tICcuLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCB7Q29tcGlsZU1ldGFkYXRhUmVzb2x2ZXJ9IGZyb20gJy4uL21ldGFkYXRhX3Jlc29sdmVyJztcbmltcG9ydCAqIGFzIG8gZnJvbSAnLi4vb3V0cHV0L291dHB1dF9hc3QnO1xuaW1wb3J0IHtPdXRwdXRDb250ZXh0fSBmcm9tICcuLi91dGlsJztcblxuaW1wb3J0IHtJZGVudGlmaWVycyBhcyBSM30gZnJvbSAnLi9yM19pZGVudGlmaWVycyc7XG5cbi8qKlxuICogV3JpdGUgYSBSZW5kZXJlcjIgY29tcGF0aWJpbGl0eSBtb2R1bGUgZmFjdG9yeSB0byB0aGUgb3V0cHV0IGNvbnRleHQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjb21waWxlTW9kdWxlRmFjdG9yeShcbiAgICBvdXRwdXRDdHg6IE91dHB1dENvbnRleHQsIG1vZHVsZTogQ29tcGlsZU5nTW9kdWxlTWV0YWRhdGEsXG4gICAgYmFja1BhdGNoUmVmZXJlbmNlT2Y6IChtb2R1bGU6IENvbXBpbGVUeXBlTWV0YWRhdGEpID0+IG8uRXhwcmVzc2lvbixcbiAgICByZXNvbHZlcjogQ29tcGlsZU1ldGFkYXRhUmVzb2x2ZXIpIHtcbiAgY29uc3QgbmdNb2R1bGVGYWN0b3J5VmFyID0gYCR7aWRlbnRpZmllck5hbWUobW9kdWxlLnR5cGUpfU5nRmFjdG9yeWA7XG5cbiAgY29uc3QgcGFyZW50SW5qZWN0b3IgPSAncGFyZW50SW5qZWN0b3InO1xuICBjb25zdCBjcmVhdGVGdW5jdGlvbiA9IG8uZm4oXG4gICAgICBbbmV3IG8uRm5QYXJhbShwYXJlbnRJbmplY3Rvciwgby5EWU5BTUlDX1RZUEUpXSxcbiAgICAgIFtuZXcgby5JZlN0bXQoXG4gICAgICAgICAgby5USElTX0VYUFIucHJvcChSMy5QQVRDSF9ERVBTKS5ub3RJZGVudGljYWwoby5saXRlcmFsKHRydWUsIG8uSU5GRVJSRURfVFlQRSkpLFxuICAgICAgICAgIFtcbiAgICAgICAgICAgIG8uVEhJU19FWFBSLnByb3AoUjMuUEFUQ0hfREVQUykuc2V0KG8ubGl0ZXJhbCh0cnVlLCBvLklORkVSUkVEX1RZUEUpKS50b1N0bXQoKSxcbiAgICAgICAgICAgIGJhY2tQYXRjaFJlZmVyZW5jZU9mKG1vZHVsZS50eXBlKS5jYWxsRm4oW10pLnRvU3RtdCgpXG4gICAgICAgICAgXSldLFxuICAgICAgby5JTkZFUlJFRF9UWVBFLCBudWxsLCBgJHtuZ01vZHVsZUZhY3RvcnlWYXJ9X0NyZWF0ZWApO1xuXG4gIGNvbnN0IG1vZHVsZUZhY3RvcnlMaXRlcmFsID0gby5saXRlcmFsTWFwKFtcbiAgICB7a2V5OiAnbW9kdWxlVHlwZScsIHZhbHVlOiBvdXRwdXRDdHguaW1wb3J0RXhwcihtb2R1bGUudHlwZS5yZWZlcmVuY2UpLCBxdW90ZWQ6IGZhbHNlfSxcbiAgICB7a2V5OiAnY3JlYXRlJywgdmFsdWU6IGNyZWF0ZUZ1bmN0aW9uLCBxdW90ZWQ6IGZhbHNlfVxuICBdKTtcblxuICBvdXRwdXRDdHguc3RhdGVtZW50cy5wdXNoKFxuICAgICAgby52YXJpYWJsZShuZ01vZHVsZUZhY3RvcnlWYXIpLnNldChtb2R1bGVGYWN0b3J5TGl0ZXJhbCkudG9EZWNsU3RtdChvLkRZTkFNSUNfVFlQRSwgW1xuICAgICAgICBvLlN0bXRNb2RpZmllci5FeHBvcnRlZCwgby5TdG10TW9kaWZpZXIuRmluYWxcbiAgICAgIF0pKTtcbn1cbiJdfQ==