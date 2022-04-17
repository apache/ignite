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
        define("@angular/compiler/src/injectable_compiler", ["require", "exports", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/identifiers", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/output/value_util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var identifiers_1 = require("@angular/compiler/src/identifiers");
    var o = require("@angular/compiler/src/output/output_ast");
    var value_util_1 = require("@angular/compiler/src/output/value_util");
    function mapEntry(key, value) {
        return { key: key, value: value, quoted: false };
    }
    var InjectableCompiler = /** @class */ (function () {
        function InjectableCompiler(reflector, alwaysGenerateDef) {
            this.reflector = reflector;
            this.alwaysGenerateDef = alwaysGenerateDef;
            this.tokenInjector = reflector.resolveExternalReference(identifiers_1.Identifiers.Injector);
        }
        InjectableCompiler.prototype.depsArray = function (deps, ctx) {
            var _this = this;
            return deps.map(function (dep) {
                var token = dep;
                var args = [token];
                var flags = 0 /* Default */;
                if (Array.isArray(dep)) {
                    for (var i = 0; i < dep.length; i++) {
                        var v = dep[i];
                        if (v) {
                            if (v.ngMetadataName === 'Optional') {
                                flags |= 8 /* Optional */;
                            }
                            else if (v.ngMetadataName === 'SkipSelf') {
                                flags |= 4 /* SkipSelf */;
                            }
                            else if (v.ngMetadataName === 'Self') {
                                flags |= 2 /* Self */;
                            }
                            else if (v.ngMetadataName === 'Inject') {
                                token = v.token;
                            }
                            else {
                                token = v;
                            }
                        }
                    }
                }
                var tokenExpr;
                if (typeof token === 'string') {
                    tokenExpr = o.literal(token);
                }
                else if (token === _this.tokenInjector) {
                    tokenExpr = o.importExpr(identifiers_1.Identifiers.INJECTOR);
                }
                else {
                    tokenExpr = ctx.importExpr(token);
                }
                if (flags !== 0 /* Default */) {
                    args = [tokenExpr, o.literal(flags)];
                }
                else {
                    args = [tokenExpr];
                }
                return o.importExpr(identifiers_1.Identifiers.inject).callFn(args);
            });
        };
        InjectableCompiler.prototype.factoryFor = function (injectable, ctx) {
            var retValue;
            if (injectable.useExisting) {
                retValue = o.importExpr(identifiers_1.Identifiers.inject).callFn([ctx.importExpr(injectable.useExisting)]);
            }
            else if (injectable.useFactory) {
                var deps = injectable.deps || [];
                if (deps.length > 0) {
                    retValue = ctx.importExpr(injectable.useFactory).callFn(this.depsArray(deps, ctx));
                }
                else {
                    return ctx.importExpr(injectable.useFactory);
                }
            }
            else if (injectable.useValue) {
                retValue = value_util_1.convertValueToOutputAst(ctx, injectable.useValue);
            }
            else {
                var clazz = injectable.useClass || injectable.symbol;
                var depArgs = this.depsArray(this.reflector.parameters(clazz), ctx);
                retValue = new o.InstantiateExpr(ctx.importExpr(clazz), depArgs);
            }
            return o.fn([], [new o.ReturnStatement(retValue)], undefined, undefined, injectable.symbol.name + '_Factory');
        };
        InjectableCompiler.prototype.injectableDef = function (injectable, ctx) {
            var providedIn = o.NULL_EXPR;
            if (injectable.providedIn !== undefined) {
                if (injectable.providedIn === null) {
                    providedIn = o.NULL_EXPR;
                }
                else if (typeof injectable.providedIn === 'string') {
                    providedIn = o.literal(injectable.providedIn);
                }
                else {
                    providedIn = ctx.importExpr(injectable.providedIn);
                }
            }
            var def = [
                mapEntry('factory', this.factoryFor(injectable, ctx)),
                mapEntry('token', ctx.importExpr(injectable.type.reference)),
                mapEntry('providedIn', providedIn),
            ];
            return o.importExpr(identifiers_1.Identifiers.ɵɵdefineInjectable).callFn([o.literalMap(def)]);
        };
        InjectableCompiler.prototype.compile = function (injectable, ctx) {
            if (this.alwaysGenerateDef || injectable.providedIn !== undefined) {
                var className = compile_metadata_1.identifierName(injectable.type);
                var clazz = new o.ClassStmt(className, null, [
                    new o.ClassField('ngInjectableDef', o.INFERRED_TYPE, [o.StmtModifier.Static], this.injectableDef(injectable, ctx)),
                ], [], new o.ClassMethod(null, [], []), []);
                ctx.statements.push(clazz);
            }
        };
        return InjectableCompiler;
    }());
    exports.InjectableCompiler = InjectableCompiler;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5qZWN0YWJsZV9jb21waWxlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pbmplY3RhYmxlX2NvbXBpbGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBR0gsMkVBQStIO0lBRy9ILGlFQUEwQztJQUMxQywyREFBeUM7SUFDekMsc0VBQTREO0lBYTVELFNBQVMsUUFBUSxDQUFDLEdBQVcsRUFBRSxLQUFtQjtRQUNoRCxPQUFPLEVBQUMsR0FBRyxLQUFBLEVBQUUsS0FBSyxPQUFBLEVBQUUsTUFBTSxFQUFFLEtBQUssRUFBQyxDQUFDO0lBQ3JDLENBQUM7SUFFRDtRQUVFLDRCQUFvQixTQUEyQixFQUFVLGlCQUEwQjtZQUEvRCxjQUFTLEdBQVQsU0FBUyxDQUFrQjtZQUFVLHNCQUFpQixHQUFqQixpQkFBaUIsQ0FBUztZQUNqRixJQUFJLENBQUMsYUFBYSxHQUFHLFNBQVMsQ0FBQyx3QkFBd0IsQ0FBQyx5QkFBVyxDQUFDLFFBQVEsQ0FBQyxDQUFDO1FBQ2hGLENBQUM7UUFFTyxzQ0FBUyxHQUFqQixVQUFrQixJQUFXLEVBQUUsR0FBa0I7WUFBakQsaUJBd0NDO1lBdkNDLE9BQU8sSUFBSSxDQUFDLEdBQUcsQ0FBQyxVQUFBLEdBQUc7Z0JBQ2pCLElBQUksS0FBSyxHQUFHLEdBQUcsQ0FBQztnQkFDaEIsSUFBSSxJQUFJLEdBQUcsQ0FBQyxLQUFLLENBQUMsQ0FBQztnQkFDbkIsSUFBSSxLQUFLLGtCQUFtQyxDQUFDO2dCQUM3QyxJQUFJLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDLEVBQUU7b0JBQ3RCLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxHQUFHLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO3dCQUNuQyxJQUFNLENBQUMsR0FBRyxHQUFHLENBQUMsQ0FBQyxDQUFD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QUFDLGtCQUFrQixDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDbEYsQ0FBQztRQUVELG9DQUFPLEdBQVAsVUFBUSxVQUFxQyxFQUFFLEdBQWtCO1lBQy9ELElBQUksSUFBSSxDQUFDLGlCQUFpQixJQUFJLFVBQVUsQ0FBQyxVQUFVLEtBQUssU0FBUyxFQUFFO2dCQUNqRSxJQUFNLFNBQVMsR0FBRyxpQ0FBYyxDQUFDLFVBQVUsQ0FBQyxJQUFJLENBQUcsQ0FBQztnQkFDcEQsSUFBTSxLQUFLLEdBQUcsSUFBSSxDQUFDLENBQUMsU0FBUyxDQUN6QixTQUFTLEVBQUUsSUFBSSxFQUNmO29CQUNFLElBQUksQ0FBQyxDQUFDLFVBQVUsQ0FDWixpQkFBaUIsRUFBRSxDQUFDLENBQUMsYUFBYSxFQUFFLENBQUMsQ0FBQyxDQUFDLFlBQVksQ0FBQyxNQUFNLENBQUMsRUFDM0QsSUFBSSxDQUFDLGFBQWEsQ0FBQyxVQUFVLEVBQUUsR0FBRyxDQUFDLENBQUM7aUJBQ3pDLEVBQ0QsRUFBRSxFQUFFLElBQUksQ0FBQyxDQUFDLFdBQVcsQ0FBQyxJQUFJLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDO2dCQUM3QyxHQUFHLENBQUMsVUFBVSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQzthQUM1QjtRQUNILENBQUM7UUFDSCx5QkFBQztJQUFELENBQUMsQUF4R0QsSUF3R0M7SUF4R1ksZ0RBQWtCIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1N0YXRpY1N5bWJvbH0gZnJvbSAnLi9hb3Qvc3RhdGljX3N5bWJvbCc7XG5pbXBvcnQge0NvbXBpbGVJbmplY3RhYmxlTWV0YWRhdGEsIENvbXBpbGVOZ01vZHVsZU1ldGFkYXRhLCBDb21waWxlUHJvdmlkZXJNZXRhZGF0YSwgaWRlbnRpZmllck5hbWV9IGZyb20gJy4vY29tcGlsZV9tZXRhZGF0YSc7XG5pbXBvcnQge0NvbXBpbGVSZWZsZWN0b3J9IGZyb20gJy4vY29tcGlsZV9yZWZsZWN0b3InO1xuaW1wb3J0IHtJbmplY3RGbGFncywgTm9kZUZsYWdzfSBmcm9tICcuL2NvcmUnO1xuaW1wb3J0IHtJZGVudGlmaWVyc30gZnJvbSAnLi9pZGVudGlmaWVycyc7XG5pbXBvcnQgKiBhcyBvIGZyb20gJy4vb3V0cHV0L291dHB1dF9hc3QnO1xuaW1wb3J0IHtjb252ZXJ0VmFsdWVUb091dHB1dEFzdH0gZnJvbSAnLi9vdXRwdXQvdmFsdWVfdXRpbCc7XG5pbXBvcnQge3R5cGVTb3VyY2VTcGFufSBmcm9tICcuL3BhcnNlX3V0aWwnO1xuaW1wb3J0IHtOZ01vZHVsZVByb3ZpZGVyQW5hbHl6ZXJ9IGZyb20gJy4vcHJvdmlkZXJfYW5hbHl6ZXInO1xuaW1wb3J0IHtPdXRwdXRDb250ZXh0fSBmcm9tICcuL3V0aWwnO1xuaW1wb3J0IHtjb21wb25lbnRGYWN0b3J5UmVzb2x2ZXJQcm92aWRlckRlZiwgZGVwRGVmLCBwcm92aWRlckRlZn0gZnJvbSAnLi92aWV3X2NvbXBpbGVyL3Byb3ZpZGVyX2NvbXBpbGVyJztcblxudHlwZSBNYXBFbnRyeSA9IHtcbiAga2V5OiBzdHJpbmcsXG4gIHF1b3RlZDogYm9vbGVhbixcbiAgdmFsdWU6IG8uRXhwcmVzc2lvblxufTtcbnR5cGUgTWFwTGl0ZXJhbCA9IE1hcEVudHJ5W107XG5cbmZ1bmN0aW9uIG1hcEVudHJ5KGtleTogc3RyaW5nLCB2YWx1ZTogby5FeHByZXNzaW9uKTogTWFwRW50cnkge1xuICByZXR1cm4ge2tleSwgdmFsdWUsIHF1b3RlZDogZmFsc2V9O1xufVxuXG5leHBvcnQgY2xhc3MgSW5qZWN0YWJsZUNvbXBpbGVyIHtcbiAgcHJpdmF0ZSB0b2tlbkluamVjdG9yOiBTdGF0aWNTeW1ib2w7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgcmVmbGVjdG9yOiBDb21waWxlUmVmbGVjdG9yLCBwcml2YXRlIGFsd2F5c0dlbmVyYXRlRGVmOiBib29sZWFuKSB7XG4gICAgdGhpcy50b2tlbkluamVjdG9yID0gcmVmbGVjdG9yLnJlc29sdmVFeHRlcm5hbFJlZmVyZW5jZShJZGVudGlmaWVycy5JbmplY3Rvcik7XG4gIH1cblxuICBwcml2YXRlIGRlcHNBcnJheShkZXBzOiBhbnlbXSwgY3R4OiBPdXRwdXRDb250ZXh0KTogby5FeHByZXNzaW9uW10ge1xuICAgIHJldHVybiBkZXBzLm1hcChkZXAgPT4ge1xuICAgICAgbGV0IHRva2VuID0gZGVwO1xuICAgICAgbGV0IGFyZ3MgPSBbdG9rZW5dO1xuICAgICAgbGV0IGZsYWdzOiBJbmplY3RGbGFncyA9IEluamVjdEZsYWdzLkRlZmF1bHQ7XG4gICAgICBpZiAoQXJyYXkuaXNBcnJheShkZXApKSB7XG4gICAgICAgIGZvciAobGV0IGkgPSAwOyBpIDwgZGVwLmxlbmd0aDsgaSsrKSB7XG4gICAgICAgICAgY29uc3QgdiA9IGRlcFtpXTtcbiAgICAgICAgICBpZiAodikge1xuICAgICAgICAgICAgaWYgKHYubmdNZXRhZGF0YU5hbWUgPT09ICdPcHRpb25hbCcpIHtcbiAgICAgICAgICAgICAgZmxhZ3MgfD0gSW5qZWN0RmxhZ3MuT3B0aW9uYWw7XG4gICAgICAgICAgICB9IGVsc2UgaWYgKHYubmdNZXRhZGF0YU5hbWUgPT09ICdTa2lwU2VsZicpIHtcbiAgICAgICAgICAgICAgZmxhZ3MgfD0gSW5qZWN0RmxhZ3MuU2tpcFNlbGY7XG4gICAgICAgICAgICB9IGVsc2UgaWYgKHYubmdNZXRhZGF0YU5hbWUgPT09ICdTZWxmJykge1xuICAgICAgICAgICAgICBmbGFncyB8PSBJbmplY3RGbGFncy5TZWxmO1xuICAgICAgICAgICAgfSBlbHNlIGlmICh2Lm5nTWV0YWRhdGFOYW1lID09PSAnSW5qZWN0Jykge1xuICAgICAgICAgICAgICB0b2tlbiA9IHYudG9rZW47XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICB0b2tlbiA9IHY7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGxldCB0b2tlbkV4cHI6IG8uRXhwcmVzc2lvbjtcbiAgICAgIGlmICh0eXBlb2YgdG9rZW4gPT09ICdzdHJpbmcnKSB7XG4gICAgICAgIHRva2VuRXhwciA9IG8ubGl0ZXJhbCh0b2tlbik7XG4gICAgICB9IGVsc2UgaWYgKHRva2VuID09PSB0aGlzLnRva2VuSW5qZWN0b3IpIHtcbiAgICAgICAgdG9rZW5FeHByID0gby5pbXBvcnRFeHByKElkZW50aWZpZXJzLklOSkVDVE9SKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRva2VuRXhwciA9IGN0eC5pbXBvcnRFeHByKHRva2VuKTtcbiAgICAgIH1cblxuICAgICAgaWYgKGZsYWdzICE9PSBJbmplY3RGbGFncy5EZWZhdWx0KSB7XG4gICAgICAgIGFyZ3MgPSBbdG9rZW5FeHByLCBvLmxpdGVyYWwoZmxhZ3MpXTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGFyZ3MgPSBbdG9rZW5FeHByXTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMuaW5qZWN0KS5jYWxsRm4oYXJncyk7XG4gICAgfSk7XG4gIH1cblxuICBmYWN0b3J5Rm9yKGluamVjdGFibGU6IENvbXBpbGVJbmplY3RhYmxlTWV0YWRhdGEsIGN0eDogT3V0cHV0Q29udGV4dCk6IG8uRXhwcmVzc2lvbiB7XG4gICAgbGV0IHJldFZhbHVlOiBvLkV4cHJlc3Npb247XG4gICAgaWYgKGluamVjdGFibGUudXNlRXhpc3RpbmcpIHtcbiAgICAgIHJldFZhbHVlID0gby5pbXBvcnRFeHByKElkZW50aWZpZXJzLmluamVjdCkuY2FsbEZuKFtjdHguaW1wb3J0RXhwcihpbmplY3RhYmxlLnVzZUV4aXN0aW5nKV0pO1xuICAgIH0gZWxzZSBpZiAoaW5qZWN0YWJsZS51c2VGYWN0b3J5KSB7XG4gICAgICBjb25zdCBkZXBzID0gaW5qZWN0YWJsZS5kZXBzIHx8IFtdO1xuICAgICAgaWYgKGRlcHMubGVuZ3RoID4gMCkge1xuICAgICAgICByZXRWYWx1ZSA9IGN0eC5pbXBvcnRFeHByKGluamVjdGFibGUudXNlRmFjdG9yeSkuY2FsbEZuKHRoaXMuZGVwc0FycmF5KGRlcHMsIGN0eCkpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIGN0eC5pbXBvcnRFeHByKGluamVjdGFibGUudXNlRmFjdG9yeSk7XG4gICAgICB9XG4gICAgfSBlbHNlIGlmIChpbmplY3RhYmxlLnVzZVZhbHVlKSB7XG4gICAgICByZXRWYWx1ZSA9IGNvbnZlcnRWYWx1ZVRvT3V0cHV0QXN0KGN0eCwgaW5qZWN0YWJsZS51c2VWYWx1ZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNvbnN0IGNsYXp6ID0gaW5qZWN0YWJsZS51c2VDbGFzcyB8fCBpbmplY3RhYmxlLnN5bWJvbDtcbiAgICAgIGNvbnN0IGRlcEFyZ3MgPSB0aGlzLmRlcHNBcnJheSh0aGlzLnJlZmxlY3Rvci5wYXJhbWV0ZXJzKGNsYXp6KSwgY3R4KTtcbiAgICAgIHJldFZhbHVlID0gbmV3IG8uSW5zdGFudGlhdGVFeHByKGN0eC5pbXBvcnRFeHByKGNsYXp6KSwgZGVwQXJncyk7XG4gICAgfVxuICAgIHJldHVybiBvLmZuKFxuICAgICAgICBbXSwgW25ldyBvLlJldHVyblN0YXRlbWVudChyZXRWYWx1ZSldLCB1bmRlZmluZWQsIHVuZGVmaW5lZCxcbiAgICAgICAgaW5qZWN0YWJsZS5zeW1ib2wubmFtZSArICdfRmFjdG9yeScpO1xuICB9XG5cbiAgaW5qZWN0YWJsZURlZihpbmplY3RhYmxlOiBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhLCBjdHg6IE91dHB1dENvbnRleHQpOiBvLkV4cHJlc3Npb24ge1xuICAgIGxldCBwcm92aWRlZEluOiBvLkV4cHJlc3Npb24gPSBvLk5VTExfRVhQUjtcbiAgICBpZiAoaW5qZWN0YWJsZS5wcm92aWRlZEluICE9PSB1bmRlZmluZWQpIHtcbiAgICAgIGlmIChpbmplY3RhYmxlLnByb3ZpZGVkSW4gPT09IG51bGwpIHtcbiAgICAgICAgcHJvdmlkZWRJbiA9IG8uTlVMTF9FWFBSO1xuICAgICAgfSBlbHNlIGlmICh0eXBlb2YgaW5qZWN0YWJsZS5wcm92aWRlZEluID09PSAnc3RyaW5nJykge1xuICAgICAgICBwcm92aWRlZEluID0gby5saXRlcmFsKGluamVjdGFibGUucHJvdmlkZWRJbik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBwcm92aWRlZEluID0gY3R4LmltcG9ydEV4cHIoaW5qZWN0YWJsZS5wcm92aWRlZEluKTtcbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3QgZGVmOiBNYXBMaXRlcmFsID0gW1xuICAgICAgbWFwRW50cnkoJ2ZhY3RvcnknLCB0aGlzLmZhY3RvcnlGb3IoaW5qZWN0YWJsZSwgY3R4KSksXG4gICAgICBtYXBFbnRyeSgndG9rZW4nLCBjdHguaW1wb3J0RXhwcihpbmplY3RhYmxlLnR5cGUucmVmZXJlbmNlKSksXG4gICAgICBtYXBFbnRyeSgncHJvdmlkZWRJbicsIHByb3ZpZGVkSW4pLFxuICAgIF07XG4gICAgcmV0dXJuIG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy7Jtcm1ZGVmaW5lSW5qZWN0YWJsZSkuY2FsbEZuKFtvLmxpdGVyYWxNYXAoZGVmKV0pO1xuICB9XG5cbiAgY29tcGlsZShpbmplY3RhYmxlOiBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhLCBjdHg6IE91dHB1dENvbnRleHQpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5hbHdheXNHZW5lcmF0ZURlZiB8fCBpbmplY3RhYmxlLnByb3ZpZGVkSW4gIT09IHVuZGVmaW5lZCkge1xuICAgICAgY29uc3QgY2xhc3NOYW1lID0gaWRlbnRpZmllck5hbWUoaW5qZWN0YWJsZS50eXBlKSAhO1xuICAgICAgY29uc3QgY2xhenogPSBuZXcgby5DbGFzc1N0bXQoXG4gICAgICAgICAgY2xhc3NOYW1lLCBudWxsLFxuICAgICAgICAgIFtcbiAgICAgICAgICAgIG5ldyBvLkNsYXNzRmllbGQoXG4gICAgICAgICAgICAgICAgJ25nSW5qZWN0YWJsZURlZicsIG8uSU5GRVJSRURfVFlQRSwgW28uU3RtdE1vZGlmaWVyLlN0YXRpY10sXG4gICAgICAgICAgICAgICAgdGhpcy5pbmplY3RhYmxlRGVmKGluamVjdGFibGUsIGN0eCkpLFxuICAgICAgICAgIF0sXG4gICAgICAgICAgW10sIG5ldyBvLkNsYXNzTWV0aG9kKG51bGwsIFtdLCBbXSksIFtdKTtcbiAgICAgIGN0eC5zdGF0ZW1lbnRzLnB1c2goY2xhenopO1xuICAgIH1cbiAgfVxufVxuIl19