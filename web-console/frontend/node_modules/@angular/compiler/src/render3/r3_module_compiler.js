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
        define("@angular/compiler/src/render3/r3_module_compiler", ["require", "exports", "tslib", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/output/map_util", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/render3/r3_factory", "@angular/compiler/src/render3/r3_identifiers", "@angular/compiler/src/render3/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var map_util_1 = require("@angular/compiler/src/output/map_util");
    var o = require("@angular/compiler/src/output/output_ast");
    var r3_factory_1 = require("@angular/compiler/src/render3/r3_factory");
    var r3_identifiers_1 = require("@angular/compiler/src/render3/r3_identifiers");
    var util_1 = require("@angular/compiler/src/render3/util");
    /**
     * Construct an `R3NgModuleDef` for the given `R3NgModuleMetadata`.
     */
    function compileNgModule(meta) {
        var moduleType = meta.type, bootstrap = meta.bootstrap, declarations = meta.declarations, imports = meta.imports, exports = meta.exports, schemas = meta.schemas, containsForwardDecls = meta.containsForwardDecls, emitInline = meta.emitInline, id = meta.id;
        var additionalStatements = [];
        var definitionMap = {
            type: moduleType
        };
        // Only generate the keys in the metadata if the arrays have values.
        if (bootstrap.length) {
            definitionMap.bootstrap = refsToArray(bootstrap, containsForwardDecls);
        }
        // If requested to emit scope information inline, pass the declarations, imports and exports to
        // the `ɵɵdefineNgModule` call. The JIT compilation uses this.
        if (emitInline) {
            if (declarations.length) {
                definitionMap.declarations = refsToArray(declarations, containsForwardDecls);
            }
            if (imports.length) {
                definitionMap.imports = refsToArray(imports, containsForwardDecls);
            }
            if (exports.length) {
                definitionMap.exports = refsToArray(exports, containsForwardDecls);
            }
        }
        // If not emitting inline, the scope information is not passed into `ɵɵdefineNgModule` as it would
        // prevent tree-shaking of the declarations, imports and exports references.
        else {
            var setNgModuleScopeCall = generateSetNgModuleScopeCall(meta);
            if (setNgModuleScopeCall !== null) {
                additionalStatements.push(setNgModuleScopeCall);
            }
        }
        if (schemas && schemas.length) {
            definitionMap.schemas = o.literalArr(schemas.map(function (ref) { return ref.value; }));
        }
        if (id) {
            definitionMap.id = id;
        }
        var expression = o.importExpr(r3_identifiers_1.Identifiers.defineNgModule).callFn([util_1.mapToMapExpression(definitionMap)]);
        var type = new o.ExpressionType(o.importExpr(r3_identifiers_1.Identifiers.NgModuleDefWithMeta, [
            new o.ExpressionType(moduleType), tupleTypeOf(declarations), tupleTypeOf(imports),
            tupleTypeOf(exports)
        ]));
        return { expression: expression, type: type, additionalStatements: additionalStatements };
    }
    exports.compileNgModule = compileNgModule;
    /**
     * Generates a function call to `ɵɵsetNgModuleScope` with all necessary information so that the
     * transitive module scope can be computed during runtime in JIT mode. This call is marked pure
     * such that the references to declarations, imports and exports may be elided causing these
     * symbols to become tree-shakeable.
     */
    function generateSetNgModuleScopeCall(meta) {
        var moduleType = meta.type, declarations = meta.declarations, imports = meta.imports, exports = meta.exports, containsForwardDecls = meta.containsForwardDecls;
        var scopeMap = {};
        if (declarations.length) {
            scopeMap.declarations = refsToArray(declarations, containsForwardDecls);
        }
        if (imports.length) {
            scopeMap.imports = refsToArray(imports, containsForwardDecls);
        }
        if (exports.length) {
            scopeMap.exports = refsToArray(exports, containsForwardDecls);
        }
        if (Object.keys(scopeMap).length === 0) {
            return null;
        }
        var fnCall = new o.InvokeFunctionExpr(
        /* fn */ o.importExpr(r3_identifiers_1.Identifiers.setNgModuleScope), 
        /* args */ [moduleType, util_1.mapToMapExpression(scopeMap)], 
        /* type */ undefined, 
        /* sourceSpan */ undefined, 
        /* pure */ true);
        return fnCall.toStmt();
    }
    function compileInjector(meta) {
        var result = r3_factory_1.compileFactoryFunction({
            name: meta.name,
            type: meta.type,
            deps: meta.deps,
            injectFn: r3_identifiers_1.Identifiers.inject,
        });
        var definitionMap = {
            factory: result.factory,
        };
        if (meta.providers !== null) {
            definitionMap.providers = meta.providers;
        }
        if (meta.imports.length > 0) {
            definitionMap.imports = o.literalArr(meta.imports);
        }
        var expression = o.importExpr(r3_identifiers_1.Identifiers.defineInjector).callFn([util_1.mapToMapExpression(definitionMap)]);
        var type = new o.ExpressionType(o.importExpr(r3_identifiers_1.Identifiers.InjectorDef, [new o.ExpressionType(meta.type)]));
        return { expression: expression, type: type, statements: result.statements };
    }
    exports.compileInjector = compileInjector;
    // TODO(alxhub): integrate this with `compileNgModule`. Currently the two are separate operations.
    function compileNgModuleFromRender2(ctx, ngModule, injectableCompiler) {
        var className = compile_metadata_1.identifierName(ngModule.type);
        var rawImports = ngModule.rawImports ? [ngModule.rawImports] : [];
        var rawExports = ngModule.rawExports ? [ngModule.rawExports] : [];
        var injectorDefArg = map_util_1.mapLiteral({
            'factory': injectableCompiler.factoryFor({ type: ngModule.type, symbol: ngModule.type.reference }, ctx),
            'providers': util_1.convertMetaToOutput(ngModule.rawProviders, ctx),
            'imports': util_1.convertMetaToOutput(tslib_1.__spread(rawImports, rawExports), ctx),
        });
        var injectorDef = o.importExpr(r3_identifiers_1.Identifiers.defineInjector).callFn([injectorDefArg]);
        ctx.statements.push(new o.ClassStmt(
        /* name */ className, 
        /* parent */ null, 
        /* fields */ [new o.ClassField(
            /* name */ 'ngInjectorDef', 
            /* type */ o.INFERRED_TYPE, 
            /* modifiers */ [o.StmtModifier.Static], 
            /* initializer */ injectorDef)], 
        /* getters */ [], 
        /* constructorMethod */ new o.ClassMethod(null, [], []), 
        /* methods */ []));
    }
    exports.compileNgModuleFromRender2 = compileNgModuleFromRender2;
    function accessExportScope(module) {
        var selectorScope = new o.ReadPropExpr(module, 'ngModuleDef');
        return new o.ReadPropExpr(selectorScope, 'exported');
    }
    function tupleTypeOf(exp) {
        var types = exp.map(function (ref) { return o.typeofExpr(ref.type); });
        return exp.length > 0 ? o.expressionType(o.literalArr(types)) : o.NONE_TYPE;
    }
    function refsToArray(refs, shouldForwardDeclare) {
        var values = o.literalArr(refs.map(function (ref) { return ref.value; }));
        return shouldForwardDeclare ? o.fn([], [new o.ReturnStatement(values)]) : values;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicjNfbW9kdWxlX2NvbXBpbGVyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3JlbmRlcjMvcjNfbW9kdWxlX2NvbXBpbGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILDJFQUFpRjtJQUVqRixrRUFBOEM7SUFDOUMsMkRBQTBDO0lBRzFDLHVFQUEwRTtJQUMxRSwrRUFBbUQ7SUFDbkQsMkRBQTRFO0lBMEQ1RTs7T0FFRztJQUNILFNBQWdCLGVBQWUsQ0FBQyxJQUF3QjtRQUVwRCxJQUFBLHNCQUFnQixFQUNoQiwwQkFBUyxFQUNULGdDQUFZLEVBQ1osc0JBQU8sRUFDUCxzQkFBTyxFQUNQLHNCQUFPLEVBQ1AsZ0RBQW9CLEVBQ3BCLDRCQUFVLEVBQ1YsWUFBRSxDQUNLO1FBRVQsSUFBTSxvQkFBb0IsR0FBa0IsRUFBRSxDQUFDO1FBQy9DLElBQU0sYUFBYSxHQUFHO1lBQ3BCLElBQUksRUFBRSxVQUFVO1NBU2pCLENBQUM7UUFFRixvRUFBb0U7UUFDcEUsSUFBSSxTQUFTLENBQUMsTUFBTSxFQUFFO1lBQ3BCLGFBQWEsQ0FBQyxTQUFTLEdBQUcsV0FBVyxDQUFDLFNBQVMsRUFBRSxvQkFBb0IsQ0FBQyxDQUFDO1NBQ3hFO1FBRUQsK0ZBQStGO1FBQy9GLDhEQUE4RDtRQUM5RCxJQUFJLFVBQVUsRUFBRTtZQUNkLElBQUksWUFBWSxDQUFDLE1BQU0sRUFBRTtnQkFDdkIsYUFBYSxDQUFDLFlBQVksR0FBRyxXQUFXLENBQUMsWUFBWSxFQUFFLG9CQUFvQixDQUFDLENBQUM7YUFDOUU7WUFFRCxJQUFJLE9BQU8sQ0FBQyxNQUFNLEVBQUU7Z0JBQ2xCLGFBQWEsQ0FBQyxPQUFPLEdBQUcsV0FBVyxDQUFDLE9BQU8sRUFBRSxvQkFBb0IsQ0FBQyxDQUFDO2FBQ3BFO1lBRUQsSUFBSSxPQUFPLENBQUMsTUFBTSxFQUFFO2dCQUNsQixhQUFhLENBQUMsT0FBTyxHQUFHLFdBQVcsQ0FBQyxPQUFPLEVBQUUsb0JBQW9CLENBQUMsQ0FBQzthQUNwRTtTQUNGO1FBRUQsa0dBQWtHO1FBQ2xHLDRFQUE0RTthQUN2RTtZQUNILElBQU0sb0JBQW9CLEdBQUcsNEJBQTRCLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDaEUsSUFBSSxvQkFBb0IsS0FBSyxJQUFJLEVBQUU7Z0JBQ2pDLG9CQUFvQixDQUFDLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxDQUFDO2FBQ2pEO1NBQ0Y7UUFFRCxJQUFJLE9BQU8sSUFBSSxPQUFPLENBQUMsTUFBTSxFQUFFO1lBQzdCLGFBQWEsQ0FBQyxPQUFPLEdBQUcsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDLFVBQUEsR0FBRyxJQUFJLE9BQUEsR0FBRyxDQUFDLEtBQUssRUFBVCxDQUFTLENBQUMsQ0FBQyxDQUFDO1NBQ3JFO1FBRUQsSUFBSSxFQUFFLEVBQUU7WUFDTixhQUFhLENBQUMsRUFBRSxHQUFHLEVBQUUsQ0FBQztTQUN2QjtRQUVELElBQU0sVUFBVSxHQUFHLENBQUMsQ0FBQyxVQUFVLENBQUMsNEJBQUUsQ0FBQyxjQUFjLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyx5QkFBa0IsQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDL0YsSUFBTSxJQUFJLEdBQUcsSUFBSSxDQUFDLENBQUMsY0FBYyxDQUFDLENBQUMsQ0FBQyxVQUFVLENBQUMsNEJBQUUsQ0FBQyxtQkFBbUIsRUFBRTtZQUNyRSxJQUFJLENBQUMsQ0FBQyxjQUFjLENBQUMsVUFBVSxDQUFDLEVBQUUsV0FBVyxDQUFDLFlBQVksQ0FBQyxFQUFFLFdBQVcsQ0FBQyxPQUFPLENBQUM7WUFDakYsV0FBVyxDQUFDLE9BQU8sQ0FBQztTQUNyQixDQUFDLENBQUMsQ0FBQztRQUdKLE9BQU8sRUFBQyxVQUFVLFlBQUEsRUFBRSxJQUFJLE1BQUEsRUFBRSxvQkFBb0Isc0JBQUEsRUFBQyxDQUFDO0lBQ2xELENBQUM7SUF4RUQsMENBd0VDO0lBRUQ7Ozs7O09BS0c7SUFDSCxTQUFTLDRCQUE0QixDQUFDLElBQXdCO1FBQ3JELElBQUEsc0JBQWdCLEVBQUUsZ0NBQVksRUFBRSxzQkFBTyxFQUFFLHNCQUFPLEVBQUUsZ0RBQW9CLENBQVM7UUFFdEYsSUFBTSxRQUFRLEdBQUcsRUFJaEIsQ0FBQztRQUVGLElBQUksWUFBWSxDQUFDLE1BQU0sRUFBRTtZQUN2QixRQUFRLENBQUMsWUFBWSxHQUFHLFdBQVcsQ0FBQyxZQUFZLEVBQUUsb0JBQW9CLENBQUMsQ0FBQztTQUN6RTtRQUVELElBQUksT0FBTyxDQUFDLE1BQU0sRUFBRTtZQUNsQixRQUFRLENBQUMsT0FBTyxHQUFHLFdBQVcsQ0FBQyxPQUFPLEVBQUUsb0JBQW9CLENBQUMsQ0FBQztTQUMvRDtRQUVELElBQUksT0FBTyxDQUFDLE1BQU0sRUFBRTtZQUNsQixRQUFRLENBQUMsT0FBTyxHQUFHLFdBQVcsQ0FBQyxPQUFPLEVBQUUsb0JBQW9CLENBQUMsQ0FBQztTQUMvRDtRQUVELElBQUksTUFBTSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQyxNQUFNLEtBQUssQ0FBQyxFQUFFO1lBQ3RDLE9BQU8sSUFBSSxDQUFDO1NBQ2I7UUFFRCxJQUFNLE1BQU0sR0FBRyxJQUFJLENBQUMsQ0FBQyxrQkFBa0I7UUFDbkMsUUFBUSxDQUFDLENBQUMsQ0FBQyxVQUFVLENBQUMsNEJBQUUsQ0FBQyxnQkFBZ0IsQ0FBQztRQUMxQyxVQUFVLENBQUEsQ0FBQyxVQUFVLEVBQUUseUJBQWtCLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDcEQsVUFBVSxDQUFDLFNBQVM7UUFDcEIsZ0JBQWdCLENBQUMsU0FBUztRQUMxQixVQUFVLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDckIsT0FBTyxNQUFNLENBQUMsTUFBTSxFQUFFLENBQUM7SUFDekIsQ0FBQztJQWdCRCxTQUFnQixlQUFlLENBQUMsSUFBd0I7UUFDdEQsSUFBTSxNQUFNLEdBQUcsbUNBQXNCLENBQUM7WUFDcEMsSUFBSSxFQUFFLElBQUksQ0FBQyxJQUFJO1lBQ2YsSUFBSSxFQUFFLElBQUksQ0FBQyxJQUFJO1lBQ2YsSUFBSSxFQUFFLElBQUksQ0FBQyxJQUFJO1lBQ2YsUUFBUSxFQUFFLDRCQUFFLENBQUMsTUFBTTtTQUNwQixDQUFDLENBQUM7UUFDSCxJQUFNLGFBQWEsR0FBRztZQUNwQixPQUFPLEVBQUUsTUFBTSxDQUFDLE9BQU87U0FDa0QsQ0FBQztRQUU1RSxJQUFJLElBQUksQ0FBQyxTQUFTLEtBQUssSUFBSSxFQUFFO1lBQzNCLGFBQWEsQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQztTQUMxQztRQUVELElBQUksSUFBSSxDQUFDLE9BQU8sQ0FBQyxNQUFNLEdBQUcsQ0FBQyxFQUFFO1lBQzNCLGFBQWEsQ0FBQyxPQUFPLEdBQUcsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7U0FDcEQ7UUFFRCxJQUFNLFVBQVUsR0FBRyxDQUFDLENBQUMsVUFBVSxDQUFDLDRCQUFFLENBQUMsY0FBYyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMseUJBQWtCLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQy9GLElBQU0sSUFBSSxHQUNOLElBQUksQ0FBQyxDQUFDLGNBQWMsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLDRCQUFFLENBQUMsV0FBVyxFQUFFLENBQUMsSUFBSSxDQUFDLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUMxRixPQUFPLEVBQUMsVUFBVSxZQUFBLEVBQUUsSUFBSSxNQUFBLEVBQUUsVUFBVSxFQUFFLE1BQU0sQ0FBQyxVQUFVLEVBQUMsQ0FBQztJQUMzRCxDQUFDO0lBdkJELDBDQXVCQztJQUVELGtHQUFrRztJQUNsRyxTQUFnQiwwQkFBMEIsQ0FDdEMsR0FBa0IsRUFBRSxRQUFzQyxFQUMxRCxrQkFBc0M7UUFDeEMsSUFBTSxTQUFTLEdBQUcsaUNBQWMsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFHLENBQUM7UUFFbEQsSUFBTSxVQUFVLEdBQUcsUUFBUSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQztRQUNwRSxJQUFNLFVBQVUsR0FBRyxRQUFRLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDO1FBRXBFLElBQU0sY0FBYyxHQUFHLHFCQUFVLENBQUM7WUFDaEMsU0FBUyxFQUNMLGtCQUFrQixDQUFDLFVBQVUsQ0FBQyxFQUFDLElBQUksRUFBRSxRQUFRLENBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxRQUFRLENBQUMsSUFBSSxDQUFDLFNBQVMsRUFBQyxFQUFFLEdBQUcsQ0FBQztZQUM5RixXQUFXLEVBQUUsMEJBQW1CLENBQUMsUUFBUSxDQUFDLFlBQVksRUFBRSxHQUFHLENBQUM7WUFDNUQsU0FBUyxFQUFFLDBCQUFtQixrQkFBSyxVQUFVLEVBQUssVUFBVSxHQUFHLEdBQUcsQ0FBQztTQUNwRSxDQUFDLENBQUM7UUFFSCxJQUFNLFdBQVcsR0FBRyxDQUFDLENBQUMsVUFBVSxDQUFDLDRCQUFFLENBQUMsY0FBYyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsY0FBYyxDQUFDLENBQUMsQ0FBQztRQUU3RSxHQUFHLENBQUMsVUFBVSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQyxTQUFTO1FBQy9CLFVBQVUsQ0FBQyxTQUFTO1FBQ3BCLFlBQVksQ0FBQyxJQUFJO1FBQ2pCLFlBQVksQ0FBQSxDQUFDLElBQUksQ0FBQyxDQUFDLFVBQVU7WUFDekIsVUFBVSxDQUFDLGVBQWU7WUFDMUIsVUFBVSxDQUFDLENBQUMsQ0FBQyxhQUFhO1lBQzFCLGVBQWUsQ0FBQSxDQUFDLENBQUMsQ0FBQyxZQUFZLENBQUMsTUFBTSxDQUFDO1lBQ3RDLGlCQUFpQixDQUFDLFdBQVcsQ0FBRyxDQUFDO1FBQ3JDLGFBQWEsQ0FBQSxFQUFFO1FBQ2YsdUJBQXVCLENBQUMsSUFBSSxDQUFDLENBQUMsV0FBVyxDQUFDLElBQUksRUFBRSxFQUFFLEVBQUUsRUFBRSxDQUFDO1FBQ3ZELGFBQWEsQ0FBQSxFQUFFLENBQUMsQ0FBQyxDQUFDO0lBQ3hCLENBQUM7SUE1QkQsZ0VBNEJDO0lBRUQsU0FBUyxpQkFBaUIsQ0FBQyxNQUFvQjtRQUM3QyxJQUFNLGFBQWEsR0FBRyxJQUFJLENBQUMsQ0FBQyxZQUFZLENBQUMsTUFBTSxFQUFFLGFBQWEsQ0FBQyxDQUFDO1FBQ2hFLE9BQU8sSUFBSSxDQUFDLENBQUMsWUFBWSxDQUFDLGFBQWEsRUFBRSxVQUFVLENBQUMsQ0FBQztJQUN2RCxDQUFDO0lBRUQsU0FBUyxXQUFXLENBQUMsR0FBa0I7UUFDckMsSUFBTSxLQUFLLEdBQUcsR0FBRyxDQUFDLEdBQUcsQ0FBQyxVQUFBLEdBQUcsSUFBSSxPQUFBLENBQUMsQ0FBQyxVQUFVLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxFQUF0QixDQUFzQixDQUFDLENBQUM7UUFDckQsT0FBTyxHQUFHLENBQUMsTUFBTSxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLGNBQWMsQ0FBQyxDQUFDLENBQUMsVUFBVSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxTQUFTLENBQUM7SUFDOUUsQ0FBQztJQUVELFNBQVMsV0FBVyxDQUFDLElBQW1CLEVBQUUsb0JBQTZCO1FBQ3JFLElBQU0sTUFBTSxHQUFHLENBQUMsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxVQUFBLEdBQUcsSUFBSSxPQUFBLEdBQUcsQ0FBQyxLQUFLLEVBQVQsQ0FBUyxDQUFDLENBQUMsQ0FBQztRQUN4RCxPQUFPLG9CQUFvQixDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRSxDQUFDLElBQUksQ0FBQyxDQUFDLGVBQWUsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQztJQUNuRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVTaGFsbG93TW9kdWxlTWV0YWRhdGEsIGlkZW50aWZpZXJOYW1lfSBmcm9tICcuLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCB7SW5qZWN0YWJsZUNvbXBpbGVyfSBmcm9tICcuLi9pbmplY3RhYmxlX2NvbXBpbGVyJztcbmltcG9ydCB7bWFwTGl0ZXJhbH0gZnJvbSAnLi4vb3V0cHV0L21hcF91dGlsJztcbmltcG9ydCAqIGFzIG8gZnJvbSAnLi4vb3V0cHV0L291dHB1dF9hc3QnO1xuaW1wb3J0IHtPdXRwdXRDb250ZXh0fSBmcm9tICcuLi91dGlsJztcblxuaW1wb3J0IHtSM0RlcGVuZGVuY3lNZXRhZGF0YSwgY29tcGlsZUZhY3RvcnlGdW5jdGlvbn0gZnJvbSAnLi9yM19mYWN0b3J5JztcbmltcG9ydCB7SWRlbnRpZmllcnMgYXMgUjN9IGZyb20gJy4vcjNfaWRlbnRpZmllcnMnO1xuaW1wb3J0IHtSM1JlZmVyZW5jZSwgY29udmVydE1ldGFUb091dHB1dCwgbWFwVG9NYXBFeHByZXNzaW9ufSBmcm9tICcuL3V0aWwnO1xuXG5leHBvcnQgaW50ZXJmYWNlIFIzTmdNb2R1bGVEZWYge1xuICBleHByZXNzaW9uOiBvLkV4cHJlc3Npb247XG4gIHR5cGU6IG8uVHlwZTtcbiAgYWRkaXRpb25hbFN0YXRlbWVudHM6IG8uU3RhdGVtZW50W107XG59XG5cbi8qKlxuICogTWV0YWRhdGEgcmVxdWlyZWQgYnkgdGhlIG1vZHVsZSBjb21waWxlciB0byBnZW5lcmF0ZSBhIGBuZ01vZHVsZURlZmAgZm9yIGEgdHlwZS5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBSM05nTW9kdWxlTWV0YWRhdGEge1xuICAvKipcbiAgICogQW4gZXhwcmVzc2lvbiByZXByZXNlbnRpbmcgdGhlIG1vZHVsZSB0eXBlIGJlaW5nIGNvbXBpbGVkLlxuICAgKi9cbiAgdHlwZTogby5FeHByZXNzaW9uO1xuXG4gIC8qKlxuICAgKiBBbiBhcnJheSBvZiBleHByZXNzaW9ucyByZXByZXNlbnRpbmcgdGhlIGJvb3RzdHJhcCBjb21wb25lbnRzIHNwZWNpZmllZCBieSB0aGUgbW9kdWxlLlxuICAgKi9cbiAgYm9vdHN0cmFwOiBSM1JlZmVyZW5jZVtdO1xuXG4gIC8qKlxuICAgKiBBbiBhcnJheSBvZiBleHByZXNzaW9ucyByZXByZXNlbnRpbmcgdGhlIGRpcmVjdGl2ZXMgYW5kIHBpcGVzIGRlY2xhcmVkIGJ5IHRoZSBtb2R1bGUuXG4gICAqL1xuICBkZWNsYXJhdGlvbnM6IFIzUmVmZXJlbmNlW107XG5cbiAgLyoqXG4gICAqIEFuIGFycmF5IG9mIGV4cHJlc3Npb25zIHJlcHJlc2VudGluZyB0aGUgaW1wb3J0cyBvZiB0aGUgbW9kdWxlLlxuICAgKi9cbiAgaW1wb3J0czogUjNSZWZlcmVuY2VbXTtcblxuICAvKipcbiAgICogQW4gYXJyYXkgb2YgZXhwcmVzc2lvbnMgcmVwcmVzZW50aW5nIHRoZSBleHBvcnRzIG9mIHRoZSBtb2R1bGUuXG4gICAqL1xuICBleHBvcnRzOiBSM1JlZmVyZW5jZVtdO1xuXG4gIC8qKlxuICAgKiBXaGV0aGVyIHRvIGVtaXQgdGhlIHNlbGVjdG9yIHNjb3BlIHZhbHVlcyAoZGVjbGFyYXRpb25zLCBpbXBvcnRzLCBleHBvcnRzKSBpbmxpbmUgaW50byB0aGVcbiAgICogbW9kdWxlIGRlZmluaXRpb24sIG9yIHRvIGdlbmVyYXRlIGFkZGl0aW9uYWwgc3RhdGVtZW50cyB3aGljaCBwYXRjaCB0aGVtIG9uLiBJbmxpbmUgZW1pc3Npb25cbiAgICogZG9lcyBub3QgYWxsb3cgY29tcG9uZW50cyB0byBiZSB0cmVlLXNoYWtlbiwgYnV0IGlzIHVzZWZ1bCBmb3IgSklUIG1vZGUuXG4gICAqL1xuICBlbWl0SW5saW5lOiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBXaGV0aGVyIHRvIGdlbmVyYXRlIGNsb3N1cmUgd3JhcHBlcnMgZm9yIGJvb3RzdHJhcCwgZGVjbGFyYXRpb25zLCBpbXBvcnRzLCBhbmQgZXhwb3J0cy5cbiAgICovXG4gIGNvbnRhaW5zRm9yd2FyZERlY2xzOiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBUaGUgc2V0IG9mIHNjaGVtYXMgdGhhdCBkZWNsYXJlIGVsZW1lbnRzIHRvIGJlIGFsbG93ZWQgaW4gdGhlIE5nTW9kdWxlLlxuICAgKi9cbiAgc2NoZW1hczogUjNSZWZlcmVuY2VbXXxudWxsO1xuXG4gIC8qKiBVbmlxdWUgSUQgb3IgZXhwcmVzc2lvbiByZXByZXNlbnRpbmcgdGhlIHVuaXF1ZSBJRCBvZiBhbiBOZ01vZHVsZS4gKi9cbiAgaWQ6IG8uRXhwcmVzc2lvbnxudWxsO1xufVxuXG4vKipcbiAqIENvbnN0cnVjdCBhbiBgUjNOZ01vZHVsZURlZmAgZm9yIHRoZSBnaXZlbiBgUjNOZ01vZHVsZU1ldGFkYXRhYC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGNvbXBpbGVOZ01vZHVsZShtZXRhOiBSM05nTW9kdWxlTWV0YWRhdGEpOiBSM05nTW9kdWxlRGVmIHtcbiAgY29uc3Qge1xuICAgIHR5cGU6IG1vZHVsZVR5cGUsXG4gICAgYm9vdHN0cmFwLFxuICAgIGRlY2xhcmF0aW9ucyxcbiAgICBpbXBvcnRzLFxuICAgIGV4cG9ydHMsXG4gICAgc2NoZW1hcyxcbiAgICBjb250YWluc0ZvcndhcmREZWNscyxcbiAgICBlbWl0SW5saW5lLFxuICAgIGlkXG4gIH0gPSBtZXRhO1xuXG4gIGNvbnN0IGFkZGl0aW9uYWxTdGF0ZW1lbnRzOiBvLlN0YXRlbWVudFtdID0gW107XG4gIGNvbnN0IGRlZmluaXRpb25NYXAgPSB7XG4gICAgdHlwZTogbW9kdWxlVHlwZVxuICB9IGFze1xuICAgIHR5cGU6IG8uRXhwcmVzc2lvbixcbiAgICBib290c3RyYXA6IG8uRXhwcmVzc2lvbixcbiAgICBkZWNsYXJhdGlvbnM6IG8uRXhwcmVzc2lvbixcbiAgICBpbXBvcnRzOiBvLkV4cHJlc3Npb24sXG4gICAgZXhwb3J0czogby5FeHByZXNzaW9uLFxuICAgIHNjaGVtYXM6IG8uTGl0ZXJhbEFycmF5RXhwcixcbiAgICBpZDogby5FeHByZXNzaW9uXG4gIH07XG5cbiAgLy8gT25seSBnZW5lcmF0ZSB0aGUga2V5cyBpbiB0aGUgbWV0YWRhdGEgaWYgdGhlIGFycmF5cyBoYXZlIHZhbHVlcy5cbiAgaWYgKGJvb3RzdHJhcC5sZW5ndGgpIHtcbiAgICBkZWZpbml0aW9uTWFwLmJvb3RzdHJhcCA9IHJlZnNUb0FycmF5KGJvb3RzdHJhcCwgY29udGFpbnNGb3J3YXJkRGVjbHMpO1xuICB9XG5cbiAgLy8gSWYgcmVxdWVzdGVkIHRvIGVtaXQgc2NvcGUgaW5mb3JtYXRpb24gaW5saW5lLCBwYXNzIHRoZSBkZWNsYXJhdGlvbnMsIGltcG9ydHMgYW5kIGV4cG9ydHMgdG9cbiAgLy8gdGhlIGDJtcm1ZGVmaW5lTmdNb2R1bGVgIGNhbGwuIFRoZSBKSVQgY29tcGlsYXRpb24gdXNlcyB0aGlzLlxuICBpZiAoZW1pdElubGluZSkge1xuICAgIGlmIChkZWNsYXJhdGlvbnMubGVuZ3RoKSB7XG4gICAgICBkZWZpbml0aW9uTWFwLmRlY2xhcmF0aW9ucyA9IHJlZnNUb0FycmF5KGRlY2xhcmF0aW9ucywgY29udGFpbnNGb3J3YXJkRGVjbHMpO1xuICAgIH1cblxuICAgIGlmIChpbXBvcnRzLmxlbmd0aCkge1xuICAgICAgZGVmaW5pdGlvbk1hcC5pbXBvcnRzID0gcmVmc1RvQXJyYXkoaW1wb3J0cywgY29udGFpbnNGb3J3YXJkRGVjbHMpO1xuICAgIH1cblxuICAgIGlmIChleHBvcnRzLmxlbmd0aCkge1xuICAgICAgZGVmaW5pdGlvbk1hcC5leHBvcnRzID0gcmVmc1RvQXJyYXkoZXhwb3J0cywgY29udGFpbnNGb3J3YXJkRGVjbHMpO1xuICAgIH1cbiAgfVxuXG4gIC8vIElmIG5vdCBlbWl0dGluZyBpbmxpbmUsIHRoZSBzY29wZSBpbmZvcm1hdGlvbiBpcyBub3QgcGFzc2VkIGludG8gYMm1ybVkZWZpbmVOZ01vZHVsZWAgYXMgaXQgd291bGRcbiAgLy8gcHJldmVudCB0cmVlLXNoYWtpbmcgb2YgdGhlIGRlY2xhcmF0aW9ucywgaW1wb3J0cyBhbmQgZXhwb3J0cyByZWZlcmVuY2VzLlxuICBlbHNlIHtcbiAgICBjb25zdCBzZXROZ01vZHVsZVNjb3BlQ2FsbCA9IGdlbmVyYXRlU2V0TmdNb2R1bGVTY29wZUNhbGwobWV0YSk7XG4gICAgaWYgKHNldE5nTW9kdWxlU2NvcGVDYWxsICE9PSBudWxsKSB7XG4gICAgICBhZGRpdGlvbmFsU3RhdGVtZW50cy5wdXNoKHNldE5nTW9kdWxlU2NvcGVDYWxsKTtcbiAgICB9XG4gIH1cblxuICBpZiAoc2NoZW1hcyAmJiBzY2hlbWFzLmxlbmd0aCkge1xuICAgIGRlZmluaXRpb25NYXAuc2NoZW1hcyA9IG8ubGl0ZXJhbEFycihzY2hlbWFzLm1hcChyZWYgPT4gcmVmLnZhbHVlKSk7XG4gIH1cblxuICBpZiAoaWQpIHtcbiAgICBkZWZpbml0aW9uTWFwLmlkID0gaWQ7XG4gIH1cblxuICBjb25zdCBleHByZXNzaW9uID0gby5pbXBvcnRFeHByKFIzLmRlZmluZU5nTW9kdWxlKS5jYWxsRm4oW21hcFRvTWFwRXhwcmVzc2lvbihkZWZpbml0aW9uTWFwKV0pO1xuICBjb25zdCB0eXBlID0gbmV3IG8uRXhwcmVzc2lvblR5cGUoby5pbXBvcnRFeHByKFIzLk5nTW9kdWxlRGVmV2l0aE1ldGEsIFtcbiAgICBuZXcgby5FeHByZXNzaW9uVHlwZShtb2R1bGVUeXBlKSwgdHVwbGVUeXBlT2YoZGVjbGFyYXRpb25zKSwgdHVwbGVUeXBlT2YoaW1wb3J0cyksXG4gICAgdHVwbGVUeXBlT2YoZXhwb3J0cylcbiAgXSkpO1xuXG5cbiAgcmV0dXJuIHtleHByZXNzaW9uLCB0eXBlLCBhZGRpdGlvbmFsU3RhdGVtZW50c307XG59XG5cbi8qKlxuICogR2VuZXJhdGVzIGEgZnVuY3Rpb24gY2FsbCB0byBgybXJtXNldE5nTW9kdWxlU2NvcGVgIHdpdGggYWxsIG5lY2Vzc2FyeSBpbmZvcm1hdGlvbiBzbyB0aGF0IHRoZVxuICogdHJhbnNpdGl2ZSBtb2R1bGUgc2NvcGUgY2FuIGJlIGNvbXB1dGVkIGR1cmluZyBydW50aW1lIGluIEpJVCBtb2RlLiBUaGlzIGNhbGwgaXMgbWFya2VkIHB1cmVcbiAqIHN1Y2ggdGhhdCB0aGUgcmVmZXJlbmNlcyB0byBkZWNsYXJhdGlvbnMsIGltcG9ydHMgYW5kIGV4cG9ydHMgbWF5IGJlIGVsaWRlZCBjYXVzaW5nIHRoZXNlXG4gKiBzeW1ib2xzIHRvIGJlY29tZSB0cmVlLXNoYWtlYWJsZS5cbiAqL1xuZnVuY3Rpb24gZ2VuZXJhdGVTZXROZ01vZHVsZVNjb3BlQ2FsbChtZXRhOiBSM05nTW9kdWxlTWV0YWRhdGEpOiBvLlN0YXRlbWVudHxudWxsIHtcbiAgY29uc3Qge3R5cGU6IG1vZHVsZVR5cGUsIGRlY2xhcmF0aW9ucywgaW1wb3J0cywgZXhwb3J0cywgY29udGFpbnNGb3J3YXJkRGVjbHN9ID0gbWV0YTtcblxuICBjb25zdCBzY29wZU1hcCA9IHt9IGFze1xuICAgIGRlY2xhcmF0aW9uczogby5FeHByZXNzaW9uLFxuICAgIGltcG9ydHM6IG8uRXhwcmVzc2lvbixcbiAgICBleHBvcnRzOiBvLkV4cHJlc3Npb24sXG4gIH07XG5cbiAgaWYgKGRlY2xhcmF0aW9ucy5sZW5ndGgpIHtcbiAgICBzY29wZU1hcC5kZWNsYXJhdGlvbnMgPSByZWZzVG9BcnJheShkZWNsYXJhdGlvbnMsIGNvbnRhaW5zRm9yd2FyZERlY2xzKTtcbiAgfVxuXG4gIGlmIChpbXBvcnRzLmxlbmd0aCkge1xuICAgIHNjb3BlTWFwLmltcG9ydHMgPSByZWZzVG9BcnJheShpbXBvcnRzLCBjb250YWluc0ZvcndhcmREZWNscyk7XG4gIH1cblxuICBpZiAoZXhwb3J0cy5sZW5ndGgpIHtcbiAgICBzY29wZU1hcC5leHBvcnRzID0gcmVmc1RvQXJyYXkoZXhwb3J0cywgY29udGFpbnNGb3J3YXJkRGVjbHMpO1xuICB9XG5cbiAgaWYgKE9iamVjdC5rZXlzKHNjb3BlTWFwKS5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuXG4gIGNvbnN0IGZuQ2FsbCA9IG5ldyBvLkludm9rZUZ1bmN0aW9uRXhwcihcbiAgICAgIC8qIGZuICovIG8uaW1wb3J0RXhwcihSMy5zZXROZ01vZHVsZVNjb3BlKSxcbiAgICAgIC8qIGFyZ3MgKi9bbW9kdWxlVHlwZSwgbWFwVG9NYXBFeHByZXNzaW9uKHNjb3BlTWFwKV0sXG4gICAgICAvKiB0eXBlICovIHVuZGVmaW5lZCxcbiAgICAgIC8qIHNvdXJjZVNwYW4gKi8gdW5kZWZpbmVkLFxuICAgICAgLyogcHVyZSAqLyB0cnVlKTtcbiAgcmV0dXJuIGZuQ2FsbC50b1N0bXQoKTtcbn1cblxuZXhwb3J0IGludGVyZmFjZSBSM0luamVjdG9yRGVmIHtcbiAgZXhwcmVzc2lvbjogby5FeHByZXNzaW9uO1xuICB0eXBlOiBvLlR5cGU7XG4gIHN0YXRlbWVudHM6IG8uU3RhdGVtZW50W107XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgUjNJbmplY3Rvck1ldGFkYXRhIHtcbiAgbmFtZTogc3RyaW5nO1xuICB0eXBlOiBvLkV4cHJlc3Npb247XG4gIGRlcHM6IFIzRGVwZW5kZW5jeU1ldGFkYXRhW118bnVsbDtcbiAgcHJvdmlkZXJzOiBvLkV4cHJlc3Npb258bnVsbDtcbiAgaW1wb3J0czogby5FeHByZXNzaW9uW107XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjb21waWxlSW5qZWN0b3IobWV0YTogUjNJbmplY3Rvck1ldGFkYXRhKTogUjNJbmplY3RvckRlZiB7XG4gIGNvbnN0IHJlc3VsdCA9IGNvbXBpbGVGYWN0b3J5RnVuY3Rpb24oe1xuICAgIG5hbWU6IG1ldGEubmFtZSxcbiAgICB0eXBlOiBtZXRhLnR5cGUsXG4gICAgZGVwczogbWV0YS5kZXBzLFxuICAgIGluamVjdEZuOiBSMy5pbmplY3QsXG4gIH0pO1xuICBjb25zdCBkZWZpbml0aW9uTWFwID0ge1xuICAgIGZhY3Rvcnk6IHJlc3VsdC5mYWN0b3J5LFxuICB9IGFze2ZhY3Rvcnk6IG8uRXhwcmVzc2lvbiwgcHJvdmlkZXJzOiBvLkV4cHJlc3Npb24sIGltcG9ydHM6IG8uRXhwcmVzc2lvbn07XG5cbiAgaWYgKG1ldGEucHJvdmlkZXJzICE9PSBudWxsKSB7XG4gICAgZGVmaW5pdGlvbk1hcC5wcm92aWRlcnMgPSBtZXRhLnByb3ZpZGVycztcbiAgfVxuXG4gIGlmIChtZXRhLmltcG9ydHMubGVuZ3RoID4gMCkge1xuICAgIGRlZmluaXRpb25NYXAuaW1wb3J0cyA9IG8ubGl0ZXJhbEFycihtZXRhLmltcG9ydHMpO1xuICB9XG5cbiAgY29uc3QgZXhwcmVzc2lvbiA9IG8uaW1wb3J0RXhwcihSMy5kZWZpbmVJbmplY3RvcikuY2FsbEZuKFttYXBUb01hcEV4cHJlc3Npb24oZGVmaW5pdGlvbk1hcCldKTtcbiAgY29uc3QgdHlwZSA9XG4gICAgICBuZXcgby5FeHByZXNzaW9uVHlwZShvLmltcG9ydEV4cHIoUjMuSW5qZWN0b3JEZWYsIFtuZXcgby5FeHByZXNzaW9uVHlwZShtZXRhLnR5cGUpXSkpO1xuICByZXR1cm4ge2V4cHJlc3Npb24sIHR5cGUsIHN0YXRlbWVudHM6IHJlc3VsdC5zdGF0ZW1lbnRzfTtcbn1cblxuLy8gVE9ETyhhbHhodWIpOiBpbnRlZ3JhdGUgdGhpcyB3aXRoIGBjb21waWxlTmdNb2R1bGVgLiBDdXJyZW50bHkgdGhlIHR3byBhcmUgc2VwYXJhdGUgb3BlcmF0aW9ucy5cbmV4cG9ydCBmdW5jdGlvbiBjb21waWxlTmdNb2R1bGVGcm9tUmVuZGVyMihcbiAgICBjdHg6IE91dHB1dENvbnRleHQsIG5nTW9kdWxlOiBDb21waWxlU2hhbGxvd01vZHVsZU1ldGFkYXRhLFxuICAgIGluamVjdGFibGVDb21waWxlcjogSW5qZWN0YWJsZUNvbXBpbGVyKTogdm9pZCB7XG4gIGNvbnN0IGNsYXNzTmFtZSA9IGlkZW50aWZpZXJOYW1lKG5nTW9kdWxlLnR5cGUpICE7XG5cbiAgY29uc3QgcmF3SW1wb3J0cyA9IG5nTW9kdWxlLnJhd0ltcG9ydHMgPyBbbmdNb2R1bGUucmF3SW1wb3J0c10gOiBbXTtcbiAgY29uc3QgcmF3RXhwb3J0cyA9IG5nTW9kdWxlLnJhd0V4cG9ydHMgPyBbbmdNb2R1bGUucmF3RXhwb3J0c10gOiBbXTtcblxuICBjb25zdCBpbmplY3RvckRlZkFyZyA9IG1hcExpdGVyYWwoe1xuICAgICdmYWN0b3J5JzpcbiAgICAgICAgaW5qZWN0YWJsZUNvbXBpbGVyLmZhY3RvcnlGb3Ioe3R5cGU6IG5nTW9kdWxlLnR5cGUsIHN5bWJvbDogbmdNb2R1bGUudHlwZS5yZWZlcmVuY2V9LCBjdHgpLFxuICAgICdwcm92aWRlcnMnOiBjb252ZXJ0TWV0YVRvT3V0cHV0KG5nTW9kdWxlLnJhd1Byb3ZpZGVycywgY3R4KSxcbiAgICAnaW1wb3J0cyc6IGNvbnZlcnRNZXRhVG9PdXRwdXQoWy4uLnJhd0ltcG9ydHMsIC4uLnJhd0V4cG9ydHNdLCBjdHgpLFxuICB9KTtcblxuICBjb25zdCBpbmplY3RvckRlZiA9IG8uaW1wb3J0RXhwcihSMy5kZWZpbmVJbmplY3RvcikuY2FsbEZuKFtpbmplY3RvckRlZkFyZ10pO1xuXG4gIGN0eC5zdGF0ZW1lbnRzLnB1c2gobmV3IG8uQ2xhc3NTdG10KFxuICAgICAgLyogbmFtZSAqLyBjbGFzc05hbWUsXG4gICAgICAvKiBwYXJlbnQgKi8gbnVsbCxcbiAgICAgIC8qIGZpZWxkcyAqL1tuZXcgby5DbGFzc0ZpZWxkKFxuICAgICAgICAgIC8qIG5hbWUgKi8gJ25nSW5qZWN0b3JEZWYnLFxuICAgICAgICAgIC8qIHR5cGUgKi8gby5JTkZFUlJFRF9UWVBFLFxuICAgICAgICAgIC8qIG1vZGlmaWVycyAqL1tvLlN0bXRNb2RpZmllci5TdGF0aWNdLFxuICAgICAgICAgIC8qIGluaXRpYWxpemVyICovIGluamVjdG9yRGVmLCApXSxcbiAgICAgIC8qIGdldHRlcnMgKi9bXSxcbiAgICAgIC8qIGNvbnN0cnVjdG9yTWV0aG9kICovIG5ldyBvLkNsYXNzTWV0aG9kKG51bGwsIFtdLCBbXSksXG4gICAgICAvKiBtZXRob2RzICovW10pKTtcbn1cblxuZnVuY3Rpb24gYWNjZXNzRXhwb3J0U2NvcGUobW9kdWxlOiBvLkV4cHJlc3Npb24pOiBvLkV4cHJlc3Npb24ge1xuICBjb25zdCBzZWxlY3RvclNjb3BlID0gbmV3IG8uUmVhZFByb3BFeHByKG1vZHVsZSwgJ25nTW9kdWxlRGVmJyk7XG4gIHJldHVybiBuZXcgby5SZWFkUHJvcEV4cHIoc2VsZWN0b3JTY29wZSwgJ2V4cG9ydGVkJyk7XG59XG5cbmZ1bmN0aW9uIHR1cGxlVHlwZU9mKGV4cDogUjNSZWZlcmVuY2VbXSk6IG8uVHlwZSB7XG4gIGNvbnN0IHR5cGVzID0gZXhwLm1hcChyZWYgPT4gby50eXBlb2ZFeHByKHJlZi50eXBlKSk7XG4gIHJldHVybiBleHAubGVuZ3RoID4gMCA/IG8uZXhwcmVzc2lvblR5cGUoby5saXRlcmFsQXJyKHR5cGVzKSkgOiBvLk5PTkVfVFlQRTtcbn1cblxuZnVuY3Rpb24gcmVmc1RvQXJyYXkocmVmczogUjNSZWZlcmVuY2VbXSwgc2hvdWxkRm9yd2FyZERlY2xhcmU6IGJvb2xlYW4pOiBvLkV4cHJlc3Npb24ge1xuICBjb25zdCB2YWx1ZXMgPSBvLmxpdGVyYWxBcnIocmVmcy5tYXAocmVmID0+IHJlZi52YWx1ZSkpO1xuICByZXR1cm4gc2hvdWxkRm9yd2FyZERlY2xhcmUgPyBvLmZuKFtdLCBbbmV3IG8uUmV0dXJuU3RhdGVtZW50KHZhbHVlcyldKSA6IHZhbHVlcztcbn1cbiJdfQ==