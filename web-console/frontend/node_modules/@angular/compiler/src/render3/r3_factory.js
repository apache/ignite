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
        define("@angular/compiler/src/render3/r3_factory", ["require", "exports", "tslib", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/identifiers", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/render3/r3_identifiers", "@angular/compiler/src/render3/view/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var identifiers_1 = require("@angular/compiler/src/identifiers");
    var o = require("@angular/compiler/src/output/output_ast");
    var r3_identifiers_1 = require("@angular/compiler/src/render3/r3_identifiers");
    var util_1 = require("@angular/compiler/src/render3/view/util");
    var R3FactoryDelegateType;
    (function (R3FactoryDelegateType) {
        R3FactoryDelegateType[R3FactoryDelegateType["Class"] = 0] = "Class";
        R3FactoryDelegateType[R3FactoryDelegateType["Function"] = 1] = "Function";
        R3FactoryDelegateType[R3FactoryDelegateType["Factory"] = 2] = "Factory";
    })(R3FactoryDelegateType = exports.R3FactoryDelegateType || (exports.R3FactoryDelegateType = {}));
    /**
     * Resolved type of a dependency.
     *
     * Occasionally, dependencies will have special significance which is known statically. In that
     * case the `R3ResolvedDependencyType` informs the factory generator that a particular dependency
     * should be generated specially (usually by calling a special injection function instead of the
     * standard one).
     */
    var R3ResolvedDependencyType;
    (function (R3ResolvedDependencyType) {
        /**
         * A normal token dependency.
         */
        R3ResolvedDependencyType[R3ResolvedDependencyType["Token"] = 0] = "Token";
        /**
         * The dependency is for an attribute.
         *
         * The token expression is a string representing the attribute name.
         */
        R3ResolvedDependencyType[R3ResolvedDependencyType["Attribute"] = 1] = "Attribute";
        /**
         * Injecting the `ChangeDetectorRef` token. Needs special handling when injected into a pipe.
         */
        R3ResolvedDependencyType[R3ResolvedDependencyType["ChangeDetectorRef"] = 2] = "ChangeDetectorRef";
    })(R3ResolvedDependencyType = exports.R3ResolvedDependencyType || (exports.R3ResolvedDependencyType = {}));
    /**
     * Construct a factory function expression for the given `R3FactoryMetadata`.
     */
    function compileFactoryFunction(meta, isPipe) {
        if (isPipe === void 0) { isPipe = false; }
        var t = o.variable('t');
        var statements = [];
        // The type to instantiate via constructor invocation. If there is no delegated factory, meaning
        // this type is always created by constructor invocation, then this is the type-to-create
        // parameter provided by the user (t) if specified, or the current type if not. If there is a
        // delegated factory (which is used to create the current type) then this is only the type-to-
        // create parameter (t).
        var typeForCtor = !isDelegatedMetadata(meta) ? new o.BinaryOperatorExpr(o.BinaryOperator.Or, t, meta.type) : t;
        var ctorExpr = null;
        if (meta.deps !== null) {
            // There is a constructor (either explicitly or implicitly defined).
            if (meta.deps !== 'invalid') {
                ctorExpr =
                    new o.InstantiateExpr(typeForCtor, injectDependencies(meta.deps, meta.injectFn, isPipe));
            }
        }
        else {
            var baseFactory = o.variable("\u0275" + meta.name + "_BaseFactory");
            var getInheritedFactory = o.importExpr(r3_identifiers_1.Identifiers.getInheritedFactory);
            var baseFactoryStmt = baseFactory.set(getInheritedFactory.callFn([meta.type])).toDeclStmt(o.INFERRED_TYPE, [
                o.StmtModifier.Exported, o.StmtModifier.Final
            ]);
            statements.push(baseFactoryStmt);
            // There is no constructor, use the base class' factory to construct typeForCtor.
            ctorExpr = baseFactory.callFn([typeForCtor]);
        }
        var ctorExprFinal = ctorExpr;
        var body = [];
        var retExpr = null;
        function makeConditionalFactory(nonCtorExpr) {
            var r = o.variable('r');
            body.push(r.set(o.NULL_EXPR).toDeclStmt());
            var ctorStmt = null;
            if (ctorExprFinal !== null) {
                ctorStmt = r.set(ctorExprFinal).toStmt();
            }
            else {
                ctorStmt = makeErrorStmt(meta.name);
            }
            body.push(o.ifStmt(t, [ctorStmt], [r.set(nonCtorExpr).toStmt()]));
            return r;
        }
        if (isDelegatedMetadata(meta) && meta.delegateType === R3FactoryDelegateType.Factory) {
            var delegateFactory = o.variable("\u0275" + meta.name + "_BaseFactory");
            var getFactoryOf = o.importExpr(r3_identifiers_1.Identifiers.getFactoryOf);
            if (meta.delegate.isEquivalent(meta.type)) {
                throw new Error("Illegal state: compiling factory that delegates to itself");
            }
            var delegateFactoryStmt = delegateFactory.set(getFactoryOf.callFn([meta.delegate])).toDeclStmt(o.INFERRED_TYPE, [
                o.StmtModifier.Exported, o.StmtModifier.Final
            ]);
            statements.push(delegateFactoryStmt);
            retExpr = makeConditionalFactory(delegateFactory.callFn([]));
        }
        else if (isDelegatedMetadata(meta)) {
            // This type is created with a delegated factory. If a type parameter is not specified, call
            // the factory instead.
            var delegateArgs = injectDependencies(meta.delegateDeps, meta.injectFn, isPipe);
            // Either call `new delegate(...)` or `delegate(...)` depending on meta.useNewForDelegate.
            var factoryExpr = new (meta.delegateType === R3FactoryDelegateType.Class ?
                o.InstantiateExpr :
                o.InvokeFunctionExpr)(meta.delegate, delegateArgs);
            retExpr = makeConditionalFactory(factoryExpr);
        }
        else if (isExpressionFactoryMetadata(meta)) {
            // TODO(alxhub): decide whether to lower the value here or in the caller
            retExpr = makeConditionalFactory(meta.expression);
        }
        else {
            retExpr = ctorExpr;
        }
        if (retExpr !== null) {
            body.push(new o.ReturnStatement(retExpr));
        }
        else {
            body.push(makeErrorStmt(meta.name));
        }
        return {
            factory: o.fn([new o.FnParam('t', o.DYNAMIC_TYPE)], body, o.INFERRED_TYPE, undefined, meta.name + "_Factory"),
            statements: statements,
        };
    }
    exports.compileFactoryFunction = compileFactoryFunction;
    function injectDependencies(deps, injectFn, isPipe) {
        return deps.map(function (dep) { return compileInjectDependency(dep, injectFn, isPipe); });
    }
    function compileInjectDependency(dep, injectFn, isPipe) {
        // Interpret the dependency according to its resolved type.
        switch (dep.resolved) {
            case R3ResolvedDependencyType.Token:
            case R3ResolvedDependencyType.ChangeDetectorRef:
                // Build up the injection flags according to the metadata.
                var flags = 0 /* Default */ | (dep.self ? 2 /* Self */ : 0) |
                    (dep.skipSelf ? 4 /* SkipSelf */ : 0) | (dep.host ? 1 /* Host */ : 0) |
                    (dep.optional ? 8 /* Optional */ : 0);
                // If this dependency is optional or otherwise has non-default flags, then additional
                // parameters describing how to inject the dependency must be passed to the inject function
                // that's being used.
                var flagsParam = (flags !== 0 /* Default */ || dep.optional) ? o.literal(flags) : null;
                // We have a separate instruction for injecting ChangeDetectorRef into a pipe.
                if (isPipe && dep.resolved === R3ResolvedDependencyType.ChangeDetectorRef) {
                    return o.importExpr(r3_identifiers_1.Identifiers.injectPipeChangeDetectorRef).callFn(flagsParam ? [flagsParam] : []);
                }
                // Build up the arguments to the injectFn call.
                var injectArgs = [dep.token];
                if (flagsParam) {
                    injectArgs.push(flagsParam);
                }
                return o.importExpr(injectFn).callFn(injectArgs);
            case R3ResolvedDependencyType.Attribute:
                // In the case of attributes, the attribute name in question is given as the token.
                return o.importExpr(r3_identifiers_1.Identifiers.injectAttribute).callFn([dep.token]);
            default:
                return util_1.unsupported("Unknown R3ResolvedDependencyType: " + R3ResolvedDependencyType[dep.resolved]);
        }
    }
    /**
     * A helper function useful for extracting `R3DependencyMetadata` from a Render2
     * `CompileTypeMetadata` instance.
     */
    function dependenciesFromGlobalMetadata(type, outputCtx, reflector) {
        var e_1, _a;
        // Use the `CompileReflector` to look up references to some well-known Angular types. These will
        // be compared with the token to statically determine whether the token has significance to
        // Angular, and set the correct `R3ResolvedDependencyType` as a result.
        var injectorRef = reflector.resolveExternalReference(identifiers_1.Identifiers.Injector);
        // Iterate through the type's DI dependencies and produce `R3DependencyMetadata` for each of them.
        var deps = [];
        try {
            for (var _b = tslib_1.__values(type.diDeps), _c = _b.next(); !_c.done; _c = _b.next()) {
                var dependency = _c.value;
                if (dependency.token) {
                    var tokenRef = compile_metadata_1.tokenReference(dependency.token);
                    var resolved = dependency.isAttribute ?
                        R3ResolvedDependencyType.Attribute :
                        R3ResolvedDependencyType.Token;
                    // In the case of most dependencies, the token will be a reference to a type. Sometimes,
                    // however, it can be a string, in the case of older Angular code or @Attribute injection.
                    var token = tokenRef instanceof static_symbol_1.StaticSymbol ? outputCtx.importExpr(tokenRef) : o.literal(tokenRef);
                    // Construct the dependency.
                    deps.push({
                        token: token,
                        resolved: resolved,
                        host: !!dependency.isHost,
                        optional: !!dependency.isOptional,
                        self: !!dependency.isSelf,
                        skipSelf: !!dependency.isSkipSelf,
                    });
                }
                else {
                    util_1.unsupported('dependency without a token');
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return deps;
    }
    exports.dependenciesFromGlobalMetadata = dependenciesFromGlobalMetadata;
    function makeErrorStmt(name) {
        return new o.ThrowStmt(new o.InstantiateExpr(new o.ReadVarExpr('Error'), [
            o.literal(name + " has a constructor which is not compatible with Dependency Injection. It should probably not be @Injectable().")
        ]));
    }
    function isDelegatedMetadata(meta) {
        return meta.delegateType !== undefined;
    }
    function isExpressionFactoryMetadata(meta) {
        return meta.expression !== undefined;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicjNfZmFjdG9yeS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3IzX2ZhY3RvcnkudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBRUgseUVBQWtEO0lBQ2xELDJFQUF3RTtJQUd4RSxpRUFBMkM7SUFDM0MsMkRBQTBDO0lBQzFDLCtFQUE0RDtJQUc1RCxnRUFBd0M7SUFzQ3hDLElBQVkscUJBSVg7SUFKRCxXQUFZLHFCQUFxQjtRQUMvQixtRUFBSyxDQUFBO1FBQ0wseUVBQVEsQ0FBQTtRQUNSLHVFQUFPLENBQUE7SUFDVCxDQUFDLEVBSlcscUJBQXFCLEdBQXJCLDZCQUFxQixLQUFyQiw2QkFBcUIsUUFJaEM7SUFvQkQ7Ozs7Ozs7T0FPRztJQUNILElBQVksd0JBaUJYO0lBakJELFdBQVksd0JBQXdCO1FBQ2xDOztXQUVHO1FBQ0gseUVBQVMsQ0FBQTtRQUVUOzs7O1dBSUc7UUFDSCxpRkFBYSxDQUFBO1FBRWI7O1dBRUc7UUFDSCxpR0FBcUIsQ0FBQTtJQUN2QixDQUFDLEVBakJXLHdCQUF3QixHQUF4QixnQ0FBd0IsS0FBeEIsZ0NBQXdCLFFBaUJuQztJQXNDRDs7T0FFRztJQUNILFNBQWdCLHNCQUFzQixDQUNsQyxJQUF1QixFQUFFLE1BQWM7UUFBZCx1QkFBQSxFQUFBLGNBQWM7UUFDekMsSUFBTSxDQUFDLEdBQUcsQ0FBQyxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsQ0FBQztRQUMxQixJQUFNLFVBQVUsR0FBa0IsRUFBRSxDQUFDO1FBRXJDLGdHQUFnRztRQUNoRyx5RkFBeUY7UUFDekYsNkZBQTZGO1FBQzdGLDhGQUE4RjtRQUM5Rix3QkFBd0I7UUFDeEIsSUFBTSxXQUFXLEdBQ2IsQ0FBQyxtQkFBbUIsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLENBQUMsa0JBQWtC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RUFBRSxDQUFDLENBQUMsVUFBVSxDQUFDLE1BQU07d0JBQ3pCLFFBQVEsRUFBRSxDQUFDLENBQUMsVUFBVSxDQUFDLFVBQVU7cUJBQ2xDLENBQUMsQ0FBQztpQkFDSjtxQkFBTTtvQkFDTCxrQkFBVyxDQUFDLDRCQUE0QixDQUFDLENBQUM7aUJBQzNDO2FBQ0Y7Ozs7Ozs7OztRQUVELE9BQU8sSUFBSSxDQUFDO0lBQ2QsQ0FBQztJQXJDRCx3RUFxQ0M7SUFFRCxTQUFTLGFBQWEsQ0FBQyxJQUFZO1FBQ2pDLE9BQU8sSUFBSSxDQUFDLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsQ0FBQyxXQUFXLENBQUMsT0FBTyxDQUFDLEVBQUU7WUFDdkUsQ0FBQyxDQUFDLE9BQU8sQ0FDRixJQUFJLG1IQUFnSCxDQUFDO1NBQzdILENBQUMsQ0FBQyxDQUFDO0lBQ04sQ0FBQztJQUVELFNBQVMsbUJBQW1CLENBQUMsSUFBdUI7UUFFbEQsT0FBUSxJQUFZLENBQUMsWUFBWSxLQUFLLFNBQVMsQ0FBQztJQUNsRCxDQUFDO0lBRUQsU0FBUywyQkFBMkIsQ0FBQyxJQUF1QjtRQUMxRCxPQUFRLElBQVksQ0FBQyxVQUFVLEtBQUssU0FBUyxDQUFDO0lBQ2hELENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7U3RhdGljU3ltYm9sfSBmcm9tICcuLi9hb3Qvc3RhdGljX3N5bWJvbCc7XG5pbXBvcnQge0NvbXBpbGVUeXBlTWV0YWRhdGEsIHRva2VuUmVmZXJlbmNlfSBmcm9tICcuLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCB7Q29tcGlsZVJlZmxlY3Rvcn0gZnJvbSAnLi4vY29tcGlsZV9yZWZsZWN0b3InO1xuaW1wb3J0IHtJbmplY3RGbGFnc30gZnJvbSAnLi4vY29yZSc7XG5pbXBvcnQge0lkZW50aWZpZXJzfSBmcm9tICcuLi9pZGVudGlmaWVycyc7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4uL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7SWRlbnRpZmllcnMgYXMgUjN9IGZyb20gJy4uL3JlbmRlcjMvcjNfaWRlbnRpZmllcnMnO1xuaW1wb3J0IHtPdXRwdXRDb250ZXh0fSBmcm9tICcuLi91dGlsJztcblxuaW1wb3J0IHt1bnN1cHBvcnRlZH0gZnJvbSAnLi92aWV3L3V0aWwnO1xuXG5cbi8qKlxuICogTWV0YWRhdGEgcmVxdWlyZWQgYnkgdGhlIGZhY3RvcnkgZ2VuZXJhdG9yIHRvIGdlbmVyYXRlIGEgYGZhY3RvcnlgIGZ1bmN0aW9uIGZvciBhIHR5cGUuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgUjNDb25zdHJ1Y3RvckZhY3RvcnlNZXRhZGF0YSB7XG4gIC8qKlxuICAgKiBTdHJpbmcgbmFtZSBvZiB0aGUgdHlwZSBiZWluZyBnZW5lcmF0ZWQgKHVzZWQgdG8gbmFtZSB0aGUgZmFjdG9yeSBmdW5jdGlvbikuXG4gICAqL1xuICBuYW1lOiBzdHJpbmc7XG5cbiAgLyoqXG4gICAqIEFuIGV4cHJlc3Npb24gcmVwcmVzZW50aW5nIHRoZSBmdW5jdGlvbiAob3IgY29uc3RydWN0b3IpIHdoaWNoIHdpbGwgaW5zdGFudGlhdGUgdGhlIHJlcXVlc3RlZFxuICAgKiB0eXBlLlxuICAgKlxuICAgKiBUaGlzIGNvdWxkIGJlIGEgcmVmZXJlbmNlIHRvIGEgY29uc3RydWN0b3IgdHlwZSwgb3IgdG8gYSB1c2VyLWRlZmluZWQgZmFjdG9yeSBmdW5jdGlvbi4gVGhlXG4gICAqIGB1c2VOZXdgIHByb3BlcnR5IGRldGVybWluZXMgd2hldGhlciBpdCB3aWxsIGJlIGNhbGxlZCBhcyBhIGNvbnN0cnVjdG9yIG9yIG5vdC5cbiAgICovXG4gIHR5cGU6IG8uRXhwcmVzc2lvbjtcblxuICAvKipcbiAgICogUmVnYXJkbGVzcyBvZiB3aGV0aGVyIGBmbk9yQ2xhc3NgIGlzIGEgY29uc3RydWN0b3IgZnVuY3Rpb24gb3IgYSB1c2VyLWRlZmluZWQgZmFjdG9yeSwgaXRcbiAgICogbWF5IGhhdmUgMCBvciBtb3JlIHBhcmFtZXRlcnMsIHdoaWNoIHdpbGwgYmUgaW5qZWN0ZWQgYWNjb3JkaW5nIHRvIHRoZSBgUjNEZXBlbmRlbmN5TWV0YWRhdGFgXG4gICAqIGZvciB0aG9zZSBwYXJhbWV0ZXJzLiBJZiB0aGlzIGlzIGBudWxsYCwgdGhlbiB0aGUgdHlwZSdzIGNvbnN0cnVjdG9yIGlzIG5vbmV4aXN0ZW50IGFuZCB3aWxsXG4gICAqIGJlIGluaGVyaXRlZCBmcm9tIGBmbk9yQ2xhc3NgIHdoaWNoIGlzIGludGVycHJldGVkIGFzIHRoZSBjdXJyZW50IHR5cGUuIElmIHRoaXMgaXMgYCdpbnZhbGlkJ2AsXG4gICAqIHRoZW4gb25lIG9yIG1vcmUgb2YgdGhlIHBhcmFtZXRlcnMgd2Fzbid0IHJlc29sdmFibGUgYW5kIGFueSBhdHRlbXB0IHRvIHVzZSB0aGVzZSBkZXBzIHdpbGxcbiAgICogcmVzdWx0IGluIGEgcnVudGltZSBlcnJvci5cbiAgICovXG4gIGRlcHM6IFIzRGVwZW5kZW5jeU1ldGFkYXRhW118J2ludmFsaWQnfG51bGw7XG5cbiAgLyoqXG4gICAqIEFuIGV4cHJlc3Npb24gZm9yIHRoZSBmdW5jdGlvbiB3aGljaCB3aWxsIGJlIHVzZWQgdG8gaW5qZWN0IGRlcGVuZGVuY2llcy4gVGhlIEFQSSBvZiB0aGlzXG4gICAqIGZ1bmN0aW9uIGNvdWxkIGJlIGRpZmZlcmVudCwgYW5kIG90aGVyIG9wdGlvbnMgY29udHJvbCBob3cgaXQgd2lsbCBiZSBpbnZva2VkLlxuICAgKi9cbiAgaW5qZWN0Rm46IG8uRXh0ZXJuYWxSZWZlcmVuY2U7XG59XG5cbmV4cG9ydCBlbnVtIFIzRmFjdG9yeURlbGVnYXRlVHlwZSB7XG4gIENsYXNzLFxuICBGdW5jdGlvbixcbiAgRmFjdG9yeSxcbn1cblxuZXhwb3J0IGludGVyZmFjZSBSM0RlbGVnYXRlZEZhY3RvcnlNZXRhZGF0YSBleHRlbmRzIFIzQ29uc3RydWN0b3JGYWN0b3J5TWV0YWRhdGEge1xuICBkZWxlZ2F0ZTogby5FeHByZXNzaW9uO1xuICBkZWxlZ2F0ZVR5cGU6IFIzRmFjdG9yeURlbGVnYXRlVHlwZS5GYWN0b3J5O1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIFIzRGVsZWdhdGVkRm5PckNsYXNzTWV0YWRhdGEgZXh0ZW5kcyBSM0NvbnN0cnVjdG9yRmFjdG9yeU1ldGFkYXRhIHtcbiAgZGVsZWdhdGU6IG8uRXhwcmVzc2lvbjtcbiAgZGVsZWdhdGVUeXBlOiBSM0ZhY3RvcnlEZWxlZ2F0ZVR5cGUuQ2xhc3N8UjNGYWN0b3J5RGVsZWdhdGVUeXBlLkZ1bmN0aW9uO1xuICBkZWxlZ2F0ZURlcHM6IFIzRGVwZW5kZW5jeU1ldGFkYXRhW107XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgUjNFeHByZXNzaW9uRmFjdG9yeU1ldGFkYXRhIGV4dGVuZHMgUjNDb25zdHJ1Y3RvckZhY3RvcnlNZXRhZGF0YSB7XG4gIGV4cHJlc3Npb246IG8uRXhwcmVzc2lvbjtcbn1cblxuZXhwb3J0IHR5cGUgUjNGYWN0b3J5TWV0YWRhdGEgPSBSM0NvbnN0cnVjdG9yRmFjdG9yeU1ldGFkYXRhIHwgUjNEZWxlZ2F0ZWRGYWN0b3J5TWV0YWRhdGEgfFxuICAgIFIzRGVsZWdhdGVkRm5PckNsYXNzTWV0YWRhdGEgfCBSM0V4cHJlc3Npb25GYWN0b3J5TWV0YWRhdGE7XG5cbi8qKlxuICogUmVzb2x2ZWQgdHlwZSBvZiBhIGRlcGVuZGVuY3kuXG4gKlxuICogT2NjYXNpb25hbGx5LCBkZXBlbmRlbmNpZXMgd2lsbCBoYXZlIHNwZWNpYWwgc2lnbmlmaWNhbmNlIHdoaWNoIGlzIGtub3duIHN0YXRpY2FsbHkuIEluIHRoYXRcbiAqIGNhc2UgdGhlIGBSM1Jlc29sdmVkRGVwZW5kZW5jeVR5cGVgIGluZm9ybXMgdGhlIGZhY3RvcnkgZ2VuZXJhdG9yIHRoYXQgYSBwYXJ0aWN1bGFyIGRlcGVuZGVuY3lcbiAqIHNob3VsZCBiZSBnZW5lcmF0ZWQgc3BlY2lhbGx5ICh1c3VhbGx5IGJ5IGNhbGxpbmcgYSBzcGVjaWFsIGluamVjdGlvbiBmdW5jdGlvbiBpbnN0ZWFkIG9mIHRoZVxuICogc3RhbmRhcmQgb25lKS5cbiAqL1xuZXhwb3J0IGVudW0gUjNSZXNvbHZlZERlcGVuZGVuY3lUeXBlIHtcbiAgLyoqXG4gICAqIEEgbm9ybWFsIHRva2VuIGRlcGVuZGVuY3kuXG4gICAqL1xuICBUb2tlbiA9IDAsXG5cbiAgLyoqXG4gICAqIFRoZSBkZXBlbmRlbmN5IGlzIGZvciBhbiBhdHRyaWJ1dGUuXG4gICAqXG4gICAqIFRoZSB0b2tlbiBleHByZXNzaW9uIGlzIGEgc3RyaW5nIHJlcHJlc2VudGluZyB0aGUgYXR0cmlidXRlIG5hbWUuXG4gICAqL1xuICBBdHRyaWJ1dGUgPSAxLFxuXG4gIC8qKlxuICAgKiBJbmplY3RpbmcgdGhlIGBDaGFuZ2VEZXRlY3RvclJlZmAgdG9rZW4uIE5lZWRzIHNwZWNpYWwgaGFuZGxpbmcgd2hlbiBpbmplY3RlZCBpbnRvIGEgcGlwZS5cbiAgICovXG4gIENoYW5nZURldGVjdG9yUmVmID0gMixcbn1cblxuLyoqXG4gKiBNZXRhZGF0YSByZXByZXNlbnRpbmcgYSBzaW5nbGUgZGVwZW5kZW5jeSB0byBiZSBpbmplY3RlZCBpbnRvIGEgY29uc3RydWN0b3Igb3IgZnVuY3Rpb24gY2FsbC5cbiAqL1xuZXhwb3J0IGludGVyZmFjZSBSM0RlcGVuZGVuY3lNZXRhZGF0YSB7XG4gIC8qKlxuICAgKiBBbiBleHByZXNzaW9uIHJlcHJlc2VudGluZyB0aGUgdG9rZW4gb3IgdmFsdWUgdG8gYmUgaW5qZWN0ZWQuXG4gICAqL1xuICB0b2tlbjogby5FeHByZXNzaW9uO1xuXG4gIC8qKlxuICAgKiBBbiBlbnVtIGluZGljYXRpbmcgd2hldGhlciB0aGlzIGRlcGVuZGVuY3kgaGFzIHNwZWNpYWwgbWVhbmluZyB0byBBbmd1bGFyIGFuZCBuZWVkcyB0byBiZVxuICAgKiBpbmplY3RlZCBzcGVjaWFsbHkuXG4gICAqL1xuICByZXNvbHZlZDogUjNSZXNvbHZlZERlcGVuZGVuY3lUeXBlO1xuXG4gIC8qKlxuICAgKiBXaGV0aGVyIHRoZSBkZXBlbmRlbmN5IGhhcyBhbiBASG9zdCBxdWFsaWZpZXIuXG4gICAqL1xuICBob3N0OiBib29sZWFuO1xuXG4gIC8qKlxuICAgKiBXaGV0aGVyIHRoZSBkZXBlbmRlbmN5IGhhcyBhbiBAT3B0aW9uYWwgcXVhbGlmaWVyLlxuICAgKi9cbiAgb3B0aW9uYWw6IGJvb2xlYW47XG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgdGhlIGRlcGVuZGVuY3kgaGFzIGFuIEBTZWxmIHF1YWxpZmllci5cbiAgICovXG4gIHNlbGY6IGJvb2xlYW47XG5cbiAgLyoqXG4gICAqIFdoZXRoZXIgdGhlIGRlcGVuZGVuY3kgaGFzIGFuIEBTa2lwU2VsZiBxdWFsaWZpZXIuXG4gICAqL1xuICBza2lwU2VsZjogYm9vbGVhbjtcbn1cblxuLyoqXG4gKiBDb25zdHJ1Y3QgYSBmYWN0b3J5IGZ1bmN0aW9uIGV4cHJlc3Npb24gZm9yIHRoZSBnaXZlbiBgUjNGYWN0b3J5TWV0YWRhdGFgLlxuICovXG5leHBvcnQgZnVuY3Rpb24gY29tcGlsZUZhY3RvcnlGdW5jdGlvbihcbiAgICBtZXRhOiBSM0ZhY3RvcnlNZXRhZGF0YSwgaXNQaXBlID0gZmFsc2UpOiB7ZmFjdG9yeTogby5FeHByZXNzaW9uLCBzdGF0ZW1lbnRzOiBvLlN0YXRlbWVudFtdfSB7XG4gIGNvbnN0IHQgPSBvLnZhcmlhYmxlKCd0Jyk7XG4gIGNvbnN0IHN0YXRlbWVudHM6IG8uU3RhdGVtZW50W10gPSBbXTtcblxuICAvLyBUaGUgdHlwZSB0byBpbnN0YW50aWF0ZSB2aWEgY29uc3RydWN0b3IgaW52b2NhdGlvbi4gSWYgdGhlcmUgaXMgbm8gZGVsZWdhdGVkIGZhY3RvcnksIG1lYW5pbmdcbiAgLy8gdGhpcyB0eXBlIGlzIGFsd2F5cyBjcmVhdGVkIGJ5IGNvbnN0cnVjdG9yIGludm9jYXRpb24sIHRoZW4gdGhpcyBpcyB0aGUgdHlwZS10by1jcmVhdGVcbiAgLy8gcGFyYW1ldGVyIHByb3ZpZGVkIGJ5IHRoZSB1c2VyICh0KSBpZiBzcGVjaWZpZWQsIG9yIHRoZSBjdXJyZW50IHR5cGUgaWYgbm90LiBJZiB0aGVyZSBpcyBhXG4gIC8vIGRlbGVnYXRlZCBmYWN0b3J5ICh3aGljaCBpcyB1c2VkIHRvIGNyZWF0ZSB0aGUgY3VycmVudCB0eXBlKSB0aGVuIHRoaXMgaXMgb25seSB0aGUgdHlwZS10by1cbiAgLy8gY3JlYXRlIHBhcmFtZXRlciAodCkuXG4gIGNvbnN0IHR5cGVGb3JDdG9yID1cbiAgICAgICFpc0RlbGVnYXRlZE1ldGFkYXRhKG1ldGEpID8gbmV3IG8uQmluYXJ5T3BlcmF0b3JFeHByKG8uQmluYXJ5T3BlcmF0b3IuT3IsIHQsIG1ldGEudHlwZSkgOiB0O1xuXG4gIGxldCBjdG9yRXhwcjogby5FeHByZXNzaW9ufG51bGwgPSBudWxsO1xuICBpZiAobWV0YS5kZXBzICE9PSBudWxsKSB7XG4gICAgLy8gVGhlcmUgaXMgYSBjb25zdHJ1Y3RvciAoZWl0aGVyIGV4cGxpY2l0bHkgb3IgaW1wbGljaXRseSBkZWZpbmVkKS5cbiAgICBpZiAobWV0YS5kZXBzICE9PSAnaW52YWxpZCcpIHtcbiAgICAgIGN0b3JFeHByID1cbiAgICAgICAgICBuZXcgby5JbnN0YW50aWF0ZUV4cHIodHlwZUZvckN0b3IsIGluamVjdERlcGVuZGVuY2llcyhtZXRhLmRlcHMsIG1ldGEuaW5qZWN0Rm4sIGlzUGlwZSkpO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICBjb25zdCBiYXNlRmFjdG9yeSA9IG8udmFyaWFibGUoYMm1JHttZXRhLm5hbWV9X0Jhc2VGYWN0b3J5YCk7XG4gICAgY29uc3QgZ2V0SW5oZXJpdGVkRmFjdG9yeSA9IG8uaW1wb3J0RXhwcihSMy5nZXRJbmhlcml0ZWRGYWN0b3J5KTtcbiAgICBjb25zdCBiYXNlRmFjdG9yeVN0bXQgPVxuICAgICAgICBiYXNlRmFjdG9yeS5zZXQoZ2V0SW5oZXJpdGVkRmFjdG9yeS5jYWxsRm4oW21ldGEudHlwZV0pKS50b0RlY2xTdG10KG8uSU5GRVJSRURfVFlQRSwgW1xuICAgICAgICAgIG8uU3RtdE1vZGlmaWVyLkV4cG9ydGVkLCBvLlN0bXRNb2RpZmllci5GaW5hbFxuICAgICAgICBdKTtcbiAgICBzdGF0ZW1lbnRzLnB1c2goYmFzZUZhY3RvcnlTdG10KTtcblxuICAgIC8vIFRoZXJlIGlzIG5vIGNvbnN0cnVjdG9yLCB1c2UgdGhlIGJhc2UgY2xhc3MnIGZhY3RvcnkgdG8gY29uc3RydWN0IHR5cGVGb3JDdG9yLlxuICAgIGN0b3JFeHByID0gYmFzZUZhY3RvcnkuY2FsbEZuKFt0eXBlRm9yQ3Rvcl0pO1xuICB9XG4gIGNvbnN0IGN0b3JFeHByRmluYWwgPSBjdG9yRXhwcjtcblxuICBjb25zdCBib2R5OiBvLlN0YXRlbWVudFtdID0gW107XG4gIGxldCByZXRFeHByOiBvLkV4cHJlc3Npb258bnVsbCA9IG51bGw7XG5cbiAgZnVuY3Rpb24gbWFrZUNvbmRpdGlvbmFsRmFjdG9yeShub25DdG9yRXhwcjogby5FeHByZXNzaW9uKTogby5SZWFkVmFyRXhwciB7XG4gICAgY29uc3QgciA9IG8udmFyaWFibGUoJ3InKTtcbiAgICBib2R5LnB1c2goci5zZXQoby5OVUxMX0VYUFIpLnRvRGVjbFN0bXQoKSk7XG4gICAgbGV0IGN0b3JTdG10OiBvLlN0YXRlbWVudHxudWxsID0gbnVsbDtcbiAgICBpZiAoY3RvckV4cHJGaW5hbCAhPT0gbnVsbCkge1xuICAgICAgY3RvclN0bXQgPSByLnNldChjdG9yRXhwckZpbmFsKS50b1N0bXQoKTtcbiAgICB9IGVsc2Uge1xuICAgICAgY3RvclN0bXQgPSBtYWtlRXJyb3JTdG10KG1ldGEubmFtZSk7XG4gICAgfVxuICAgIGJvZHkucHVzaChvLmlmU3RtdCh0LCBbY3RvclN0bXRdLCBbci5zZXQobm9uQ3RvckV4cHIpLnRvU3RtdCgpXSkpO1xuICAgIHJldHVybiByO1xuICB9XG5cbiAgaWYgKGlzRGVsZWdhdGVkTWV0YWRhdGEobWV0YSkgJiYgbWV0YS5kZWxlZ2F0ZVR5cGUgPT09IFIzRmFjdG9yeURlbGVnYXRlVHlwZS5GYWN0b3J5KSB7XG4gICAgY29uc3QgZGVsZWdhdGVGYWN0b3J5ID0gby52YXJpYWJsZShgybUke21ldGEubmFtZX1fQmFzZUZhY3RvcnlgKTtcbiAgICBjb25zdCBnZXRGYWN0b3J5T2YgPSBvLmltcG9ydEV4cHIoUjMuZ2V0RmFjdG9yeU9mKTtcbiAgICBpZiAobWV0YS5kZWxlZ2F0ZS5pc0VxdWl2YWxlbnQobWV0YS50eXBlKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBJbGxlZ2FsIHN0YXRlOiBjb21waWxpbmcgZmFjdG9yeSB0aGF0IGRlbGVnYXRlcyB0byBpdHNlbGZgKTtcbiAgICB9XG4gICAgY29uc3QgZGVsZWdhdGVGYWN0b3J5U3RtdCA9XG4gICAgICAgIGRlbGVnYXRlRmFjdG9yeS5zZXQoZ2V0RmFjdG9yeU9mLmNhbGxGbihbbWV0YS5kZWxlZ2F0ZV0pKS50b0RlY2xTdG10KG8uSU5GRVJSRURfVFlQRSwgW1xuICAgICAgICAgIG8uU3RtdE1vZGlmaWVyLkV4cG9ydGVkLCBvLlN0bXRNb2RpZmllci5GaW5hbFxuICAgICAgICBdKTtcblxuICAgIHN0YXRlbWVudHMucHVzaChkZWxlZ2F0ZUZhY3RvcnlTdG10KTtcbiAgICByZXRFeHByID0gbWFrZUNvbmRpdGlvbmFsRmFjdG9yeShkZWxlZ2F0ZUZhY3RvcnkuY2FsbEZuKFtdKSk7XG4gIH0gZWxzZSBpZiAoaXNEZWxlZ2F0ZWRNZXRhZGF0YShtZXRhKSkge1xuICAgIC8vIFRoaXMgdHlwZSBpcyBjcmVhdGVkIHdpdGggYSBkZWxlZ2F0ZWQgZmFjdG9yeS4gSWYgYSB0eXBlIHBhcmFtZXRlciBpcyBub3Qgc3BlY2lmaWVkLCBjYWxsXG4gICAgLy8gdGhlIGZhY3RvcnkgaW5zdGVhZC5cbiAgICBjb25zdCBkZWxlZ2F0ZUFyZ3MgPSBpbmplY3REZXBlbmRlbmNpZXMobWV0YS5kZWxlZ2F0ZURlcHMsIG1ldGEuaW5qZWN0Rm4sIGlzUGlwZSk7XG4gICAgLy8gRWl0aGVyIGNhbGwgYG5ldyBkZWxlZ2F0ZSguLi4pYCBvciBgZGVsZWdhdGUoLi4uKWAgZGVwZW5kaW5nIG9uIG1ldGEudXNlTmV3Rm9yRGVsZWdhdGUuXG4gICAgY29uc3QgZmFjdG9yeUV4cHIgPSBuZXcgKFxuICAgICAgICBtZXRhLmRlbGVnYXRlVHlwZSA9PT0gUjNGYWN0b3J5RGVsZWdhdGVUeXBlLkNsYXNzID9cbiAgICAgICAgICAgIG8uSW5zdGFudGlhdGVFeHByIDpcbiAgICAgICAgICAgIG8uSW52b2tlRnVuY3Rpb25FeHByKShtZXRhLmRlbGVnYXRlLCBkZWxlZ2F0ZUFyZ3MpO1xuICAgIHJldEV4cHIgPSBtYWtlQ29uZGl0aW9uYWxGYWN0b3J5KGZhY3RvcnlFeHByKTtcbiAgfSBlbHNlIGlmIChpc0V4cHJlc3Npb25GYWN0b3J5TWV0YWRhdGEobWV0YSkpIHtcbiAgICAvLyBUT0RPKGFseGh1Yik6IGRlY2lkZSB3aGV0aGVyIHRvIGxvd2VyIHRoZSB2YWx1ZSBoZXJlIG9yIGluIHRoZSBjYWxsZXJcbiAgICByZXRFeHByID0gbWFrZUNvbmRpdGlvbmFsRmFjdG9yeShtZXRhLmV4cHJlc3Npb24pO1xuICB9IGVsc2Uge1xuICAgIHJldEV4cHIgPSBjdG9yRXhwcjtcbiAgfVxuXG4gIGlmIChyZXRFeHByICE9PSBudWxsKSB7XG4gICAgYm9keS5wdXNoKG5ldyBvLlJldHVyblN0YXRlbWVudChyZXRFeHByKSk7XG4gIH0gZWxzZSB7XG4gICAgYm9keS5wdXNoKG1ha2VFcnJvclN0bXQobWV0YS5uYW1lKSk7XG4gIH1cblxuICByZXR1cm4ge1xuICAgIGZhY3Rvcnk6IG8uZm4oXG4gICAgICAgIFtuZXcgby5GblBhcmFtKCd0Jywgby5EWU5BTUlDX1RZUEUpXSwgYm9keSwgby5JTkZFUlJFRF9UWVBFLCB1bmRlZmluZWQsXG4gICAgICAgIGAke21ldGEubmFtZX1fRmFjdG9yeWApLFxuICAgIHN0YXRlbWVudHMsXG4gIH07XG59XG5cbmZ1bmN0aW9uIGluamVjdERlcGVuZGVuY2llcyhcbiAgICBkZXBzOiBSM0RlcGVuZGVuY3lNZXRhZGF0YVtdLCBpbmplY3RGbjogby5FeHRlcm5hbFJlZmVyZW5jZSwgaXNQaXBlOiBib29sZWFuKTogby5FeHByZXNzaW9uW10ge1xuICByZXR1cm4gZGVwcy5tYXAoZGVwID0+IGNvbXBpbGVJbmplY3REZXBlbmRlbmN5KGRlcCwgaW5qZWN0Rm4sIGlzUGlwZSkpO1xufVxuXG5mdW5jdGlvbiBjb21waWxlSW5qZWN0RGVwZW5kZW5jeShcbiAgICBkZXA6IFIzRGVwZW5kZW5jeU1ldGFkYXRhLCBpbmplY3RGbjogby5FeHRlcm5hbFJlZmVyZW5jZSwgaXNQaXBlOiBib29sZWFuKTogby5FeHByZXNzaW9uIHtcbiAgLy8gSW50ZXJwcmV0IHRoZSBkZXBlbmRlbmN5IGFjY29yZGluZyB0byBpdHMgcmVzb2x2ZWQgdHlwZS5cbiAgc3dpdGNoIChkZXAucmVzb2x2ZWQpIHtcbiAgICBjYXNlIFIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZS5Ub2tlbjpcbiAgICBjYXNlIFIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZS5DaGFuZ2VEZXRlY3RvclJlZjpcbiAgICAgIC8vIEJ1aWxkIHVwIHRoZSBpbmplY3Rpb24gZmxhZ3MgYWNjb3JkaW5nIHRvIHRoZSBtZXRhZGF0YS5cbiAgICAgIGNvbnN0IGZsYWdzID0gSW5qZWN0RmxhZ3MuRGVmYXVsdCB8IChkZXAuc2VsZiA/IEluamVjdEZsYWdzLlNlbGYgOiAwKSB8XG4gICAgICAgICAgKGRlcC5za2lwU2VsZiA/IEluamVjdEZsYWdzLlNraXBTZWxmIDogMCkgfCAoZGVwLmhvc3QgPyBJbmplY3RGbGFncy5Ib3N0IDogMCkgfFxuICAgICAgICAgIChkZXAub3B0aW9uYWwgPyBJbmplY3RGbGFncy5PcHRpb25hbCA6IDApO1xuXG4gICAgICAvLyBJZiB0aGlzIGRlcGVuZGVuY3kgaXMgb3B0aW9uYWwgb3Igb3RoZXJ3aXNlIGhhcyBub24tZGVmYXVsdCBmbGFncywgdGhlbiBhZGRpdGlvbmFsXG4gICAgICAvLyBwYXJhbWV0ZXJzIGRlc2NyaWJpbmcgaG93IHRvIGluamVjdCB0aGUgZGVwZW5kZW5jeSBtdXN0IGJlIHBhc3NlZCB0byB0aGUgaW5qZWN0IGZ1bmN0aW9uXG4gICAgICAvLyB0aGF0J3MgYmVpbmcgdXNlZC5cbiAgICAgIGxldCBmbGFnc1BhcmFtOiBvLkxpdGVyYWxFeHByfG51bGwgPVxuICAgICAgICAgIChmbGFncyAhPT0gSW5qZWN0RmxhZ3MuRGVmYXVsdCB8fCBkZXAub3B0aW9uYWwpID8gby5saXRlcmFsKGZsYWdzKSA6IG51bGw7XG5cbiAgICAgIC8vIFdlIGhhdmUgYSBzZXBhcmF0ZSBpbnN0cnVjdGlvbiBmb3IgaW5qZWN0aW5nIENoYW5nZURldGVjdG9yUmVmIGludG8gYSBwaXBlLlxuICAgICAgaWYgKGlzUGlwZSAmJiBkZXAucmVzb2x2ZWQgPT09IFIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZS5DaGFuZ2VEZXRlY3RvclJlZikge1xuICAgICAgICByZXR1cm4gby5pbXBvcnRFeHByKFIzLmluamVjdFBpcGVDaGFuZ2VEZXRlY3RvclJlZikuY2FsbEZuKGZsYWdzUGFyYW0gPyBbZmxhZ3NQYXJhbV0gOiBbXSk7XG4gICAgICB9XG5cbiAgICAgIC8vIEJ1aWxkIHVwIHRoZSBhcmd1bWVudHMgdG8gdGhlIGluamVjdEZuIGNhbGwuXG4gICAgICBjb25zdCBpbmplY3RBcmdzID0gW2RlcC50b2tlbl07XG4gICAgICBpZiAoZmxhZ3NQYXJhbSkge1xuICAgICAgICBpbmplY3RBcmdzLnB1c2goZmxhZ3NQYXJhbSk7XG4gICAgICB9XG4gICAgICByZXR1cm4gby5pbXBvcnRFeHByKGluamVjdEZuKS5jYWxsRm4oaW5qZWN0QXJncyk7XG4gICAgY2FzZSBSM1Jlc29sdmVkRGVwZW5kZW5jeVR5cGUuQXR0cmlidXRlOlxuICAgICAgLy8gSW4gdGhlIGNhc2Ugb2YgYXR0cmlidXRlcywgdGhlIGF0dHJpYnV0ZSBuYW1lIGluIHF1ZXN0aW9uIGlzIGdpdmVuIGFzIHRoZSB0b2tlbi5cbiAgICAgIHJldHVybiBvLmltcG9ydEV4cHIoUjMuaW5qZWN0QXR0cmlidXRlKS5jYWxsRm4oW2RlcC50b2tlbl0pO1xuICAgIGRlZmF1bHQ6XG4gICAgICByZXR1cm4gdW5zdXBwb3J0ZWQoXG4gICAgICAgICAgYFVua25vd24gUjNSZXNvbHZlZERlcGVuZGVuY3lUeXBlOiAke1IzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZVtkZXAucmVzb2x2ZWRdfWApO1xuICB9XG59XG5cbi8qKlxuICogQSBoZWxwZXIgZnVuY3Rpb24gdXNlZnVsIGZvciBleHRyYWN0aW5nIGBSM0RlcGVuZGVuY3lNZXRhZGF0YWAgZnJvbSBhIFJlbmRlcjJcbiAqIGBDb21waWxlVHlwZU1ldGFkYXRhYCBpbnN0YW5jZS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGRlcGVuZGVuY2llc0Zyb21HbG9iYWxNZXRhZGF0YShcbiAgICB0eXBlOiBDb21waWxlVHlwZU1ldGFkYXRhLCBvdXRwdXRDdHg6IE91dHB1dENvbnRleHQsXG4gICAgcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yKTogUjNEZXBlbmRlbmN5TWV0YWRhdGFbXSB7XG4gIC8vIFVzZSB0aGUgYENvbXBpbGVSZWZsZWN0b3JgIHRvIGxvb2sgdXAgcmVmZXJlbmNlcyB0byBzb21lIHdlbGwta25vd24gQW5ndWxhciB0eXBlcy4gVGhlc2Ugd2lsbFxuICAvLyBiZSBjb21wYXJlZCB3aXRoIHRoZSB0b2tlbiB0byBzdGF0aWNhbGx5IGRldGVybWluZSB3aGV0aGVyIHRoZSB0b2tlbiBoYXMgc2lnbmlmaWNhbmNlIHRvXG4gIC8vIEFuZ3VsYXIsIGFuZCBzZXQgdGhlIGNvcnJlY3QgYFIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZWAgYXMgYSByZXN1bHQuXG4gIGNvbnN0IGluamVjdG9yUmVmID0gcmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5JbmplY3Rvcik7XG5cbiAgLy8gSXRlcmF0ZSB0aHJvdWdoIHRoZSB0eXBlJ3MgREkgZGVwZW5kZW5jaWVzIGFuZCBwcm9kdWNlIGBSM0RlcGVuZGVuY3lNZXRhZGF0YWAgZm9yIGVhY2ggb2YgdGhlbS5cbiAgY29uc3QgZGVwczogUjNEZXBlbmRlbmN5TWV0YWRhdGFbXSA9IFtdO1xuICBmb3IgKGxldCBkZXBlbmRlbmN5IG9mIHR5cGUuZGlEZXBzKSB7XG4gICAgaWYgKGRlcGVuZGVuY3kudG9rZW4pIHtcbiAgICAgIGNvbnN0IHRva2VuUmVmID0gdG9rZW5SZWZlcmVuY2UoZGVwZW5kZW5jeS50b2tlbik7XG4gICAgICBsZXQgcmVzb2x2ZWQ6IFIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZSA9IGRlcGVuZGVuY3kuaXNBdHRyaWJ1dGUgP1xuICAgICAgICAgIFIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZS5BdHRyaWJ1dGUgOlxuICAgICAgICAgIFIzUmVzb2x2ZWREZXBlbmRlbmN5VHlwZS5Ub2tlbjtcblxuICAgICAgLy8gSW4gdGhlIGNhc2Ugb2YgbW9zdCBkZXBlbmRlbmNpZXMsIHRoZSB0b2tlbiB3aWxsIGJlIGEgcmVmZXJlbmNlIHRvIGEgdHlwZS4gU29tZXRpbWVzLFxuICAgICAgLy8gaG93ZXZlciwgaXQgY2FuIGJlIGEgc3RyaW5nLCBpbiB0aGUgY2FzZSBvZiBvbGRlciBBbmd1bGFyIGNvZGUgb3IgQEF0dHJpYnV0ZSBpbmplY3Rpb24uXG4gICAgICBjb25zdCB0b2tlbiA9XG4gICAgICAgICAgdG9rZW5SZWYgaW5zdGFuY2VvZiBTdGF0aWNTeW1ib2wgPyBvdXRwdXRDdHguaW1wb3J0RXhwcih0b2tlblJlZikgOiBvLmxpdGVyYWwodG9rZW5SZWYpO1xuXG4gICAgICAvLyBDb25zdHJ1Y3QgdGhlIGRlcGVuZGVuY3kuXG4gICAgICBkZXBzLnB1c2goe1xuICAgICAgICB0b2tlbixcbiAgICAgICAgcmVzb2x2ZWQsXG4gICAgICAgIGhvc3Q6ICEhZGVwZW5kZW5jeS5pc0hvc3QsXG4gICAgICAgIG9wdGlvbmFsOiAhIWRlcGVuZGVuY3kuaXNPcHRpb25hbCxcbiAgICAgICAgc2VsZjogISFkZXBlbmRlbmN5LmlzU2VsZixcbiAgICAgICAgc2tpcFNlbGY6ICEhZGVwZW5kZW5jeS5pc1NraXBTZWxmLFxuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHVuc3VwcG9ydGVkKCdkZXBlbmRlbmN5IHdpdGhvdXQgYSB0b2tlbicpO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBkZXBzO1xufVxuXG5mdW5jdGlvbiBtYWtlRXJyb3JTdG10KG5hbWU6IHN0cmluZyk6IG8uU3RhdGVtZW50IHtcbiAgcmV0dXJuIG5ldyBvLlRocm93U3RtdChuZXcgby5JbnN0YW50aWF0ZUV4cHIobmV3IG8uUmVhZFZhckV4cHIoJ0Vycm9yJyksIFtcbiAgICBvLmxpdGVyYWwoXG4gICAgICAgIGAke25hbWV9IGhhcyBhIGNvbnN0cnVjdG9yIHdoaWNoIGlzIG5vdCBjb21wYXRpYmxlIHdpdGggRGVwZW5kZW5jeSBJbmplY3Rpb24uIEl0IHNob3VsZCBwcm9iYWJseSBub3QgYmUgQEluamVjdGFibGUoKS5gKVxuICBdKSk7XG59XG5cbmZ1bmN0aW9uIGlzRGVsZWdhdGVkTWV0YWRhdGEobWV0YTogUjNGYWN0b3J5TWV0YWRhdGEpOiBtZXRhIGlzIFIzRGVsZWdhdGVkRmFjdG9yeU1ldGFkYXRhfFxuICAgIFIzRGVsZWdhdGVkRm5PckNsYXNzTWV0YWRhdGEge1xuICByZXR1cm4gKG1ldGEgYXMgYW55KS5kZWxlZ2F0ZVR5cGUgIT09IHVuZGVmaW5lZDtcbn1cblxuZnVuY3Rpb24gaXNFeHByZXNzaW9uRmFjdG9yeU1ldGFkYXRhKG1ldGE6IFIzRmFjdG9yeU1ldGFkYXRhKTogbWV0YSBpcyBSM0V4cHJlc3Npb25GYWN0b3J5TWV0YWRhdGEge1xuICByZXR1cm4gKG1ldGEgYXMgYW55KS5leHByZXNzaW9uICE9PSB1bmRlZmluZWQ7XG59XG4iXX0=