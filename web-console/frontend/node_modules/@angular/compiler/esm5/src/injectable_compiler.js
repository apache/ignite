/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { identifierName } from './compile_metadata';
import { Identifiers } from './identifiers';
import * as o from './output/output_ast';
import { convertValueToOutputAst } from './output/value_util';
function mapEntry(key, value) {
    return { key: key, value: value, quoted: false };
}
var InjectableCompiler = /** @class */ (function () {
    function InjectableCompiler(reflector, alwaysGenerateDef) {
        this.reflector = reflector;
        this.alwaysGenerateDef = alwaysGenerateDef;
        this.tokenInjector = reflector.resolveExternalReference(Identifiers.Injector);
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
                tokenExpr = o.importExpr(Identifiers.INJECTOR);
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
            return o.importExpr(Identifiers.inject).callFn(args);
        });
    };
    InjectableCompiler.prototype.factoryFor = function (injectable, ctx) {
        var retValue;
        if (injectable.useExisting) {
            retValue = o.importExpr(Identifiers.inject).callFn([ctx.importExpr(injectable.useExisting)]);
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
            retValue = convertValueToOutputAst(ctx, injectable.useValue);
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
        return o.importExpr(Identifiers.ɵɵdefineInjectable).callFn([o.literalMap(def)]);
    };
    InjectableCompiler.prototype.compile = function (injectable, ctx) {
        if (this.alwaysGenerateDef || injectable.providedIn !== undefined) {
            var className = identifierName(injectable.type);
            var clazz = new o.ClassStmt(className, null, [
                new o.ClassField('ngInjectableDef', o.INFERRED_TYPE, [o.StmtModifier.Static], this.injectableDef(injectable, ctx)),
            ], [], new o.ClassMethod(null, [], []), []);
            ctx.statements.push(clazz);
        }
    };
    return InjectableCompiler;
}());
export { InjectableCompiler };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5qZWN0YWJsZV9jb21waWxlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pbmplY3RhYmxlX2NvbXBpbGVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUdILE9BQU8sRUFBOEUsY0FBYyxFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFHL0gsT0FBTyxFQUFDLFdBQVcsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUMxQyxPQUFPLEtBQUssQ0FBQyxNQUFNLHFCQUFxQixDQUFDO0FBQ3pDLE9BQU8sRUFBQyx1QkFBdUIsRUFBQyxNQUFNLHFCQUFxQixDQUFDO0FBYTVELFNBQVMsUUFBUSxDQUFDLEdBQVcsRUFBRSxLQUFtQjtJQUNoRCxPQUFPLEVBQUMsR0FBRyxLQUFBLEVBQUUsS0FBSyxPQUFBLEVBQUUsTUFBTSxFQUFFLEtBQUssRUFBQyxDQUFDO0FBQ3JDLENBQUM7QUFFRDtJQUVFLDRCQUFvQixTQUEyQixFQUFVLGlCQUEwQjtRQUEvRCxjQUFTLEdBQVQsU0FBUyxDQUFrQjtRQUFVLHNCQUFpQixHQUFqQixpQkFBaUIsQ0FBUztRQUNqRixJQUFJLENBQUMsYUFBYSxHQUFHLFNBQVMsQ0FBQyx3QkFBd0IsQ0FBQyxXQUFXLENBQUMsUUFBUSxDQUFDLENBQUM7SUFDaEYsQ0FBQztJQUVPLHNDQUFTLEdBQWpCLFVBQWtCLElBQVcsRUFBRSxHQUFrQjtRQUFqRCxpQkF3Q0M7UUF2Q0MsT0FBTyxJQUFJLENBQUMsR0FBRyxDQUFDLFVBQUEsR0FBRztZQUNqQixJQUFJLEtBQUssR0FBRyxHQUFHLENBQUM7WUFDaEIsSUFBSSxJQUFJLEdBQUcsQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUNuQixJQUFJLEtBQUssa0JBQW1DLENBQUM7WUFDN0MsSUFBSSxLQUFLLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxFQUFFO2dCQUN0QixLQUFLLElB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QUFRLENBQUMsWUFBWSxFQUFFLFVBQVUsQ0FBQztTQUNuQyxDQUFDO1FBQ0YsT0FBTyxDQUFDLENBQUMsVUFBVSxDQUFDLFdBQVcsQ0FBQyxrQkFBa0IsQ0FBQyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQyxVQUFVLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQ2xGLENBQUM7SUFFRCxvQ0FBTyxHQUFQLFVBQVEsVUFBcUMsRUFBRSxHQUFrQjtRQUMvRCxJQUFJLElBQUksQ0FBQyxpQkFBaUIsSUFBSSxVQUFVLENBQUMsVUFBVSxLQUFLLFNBQVMsRUFBRTtZQUNqRSxJQUFNLFNBQVMsR0FBRyxjQUFjLENBQUMsVUFBVSxDQUFDLElBQUksQ0FBRyxDQUFDO1lBQ3BELElBQU0sS0FBSyxHQUFHLElBQUksQ0FBQyxDQUFDLFNBQVMsQ0FDekIsU0FBUyxFQUFFLElBQUksRUFDZjtnQkFDRSxJQUFJLENBQUMsQ0FBQyxVQUFVLENBQ1osaUJBQWlCLEVBQUUsQ0FBQyxDQUFDLGFBQWEsRUFBRSxDQUFDLENBQUMsQ0FBQyxZQUFZLENBQUMsTUFBTSxDQUFDLEVBQzNELElBQUksQ0FBQyxhQUFhLENBQUMsVUFBVSxFQUFFLEdBQUcsQ0FBQyxDQUFDO2FBQ3pDLEVBQ0QsRUFBRSxFQUFFLElBQUksQ0FBQyxDQUFDLFdBQVcsQ0FBQyxJQUFJLEVBQUUsRUFBRSxFQUFFLEVBQUUsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxDQUFDO1lBQzdDLEdBQUcsQ0FBQyxVQUFVLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDO1NBQzVCO0lBQ0gsQ0FBQztJQUNILHlCQUFDO0FBQUQsQ0FBQyxBQXhHRCxJQXdHQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtTdGF0aWNTeW1ib2x9IGZyb20gJy4vYW90L3N0YXRpY19zeW1ib2wnO1xuaW1wb3J0IHtDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhLCBDb21waWxlTmdNb2R1bGVNZXRhZGF0YSwgQ29tcGlsZVByb3ZpZGVyTWV0YWRhdGEsIGlkZW50aWZpZXJOYW1lfSBmcm9tICcuL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtDb21waWxlUmVmbGVjdG9yfSBmcm9tICcuL2NvbXBpbGVfcmVmbGVjdG9yJztcbmltcG9ydCB7SW5qZWN0RmxhZ3MsIE5vZGVGbGFnc30gZnJvbSAnLi9jb3JlJztcbmltcG9ydCB7SWRlbnRpZmllcnN9IGZyb20gJy4vaWRlbnRpZmllcnMnO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7Y29udmVydFZhbHVlVG9PdXRwdXRBc3R9IGZyb20gJy4vb3V0cHV0L3ZhbHVlX3V0aWwnO1xuaW1wb3J0IHt0eXBlU291cmNlU3Bhbn0gZnJvbSAnLi9wYXJzZV91dGlsJztcbmltcG9ydCB7TmdNb2R1bGVQcm92aWRlckFuYWx5emVyfSBmcm9tICcuL3Byb3ZpZGVyX2FuYWx5emVyJztcbmltcG9ydCB7T3V0cHV0Q29udGV4dH0gZnJvbSAnLi91dGlsJztcbmltcG9ydCB7Y29tcG9uZW50RmFjdG9yeVJlc29sdmVyUHJvdmlkZXJEZWYsIGRlcERlZiwgcHJvdmlkZXJEZWZ9IGZyb20gJy4vdmlld19jb21waWxlci9wcm92aWRlcl9jb21waWxlcic7XG5cbnR5cGUgTWFwRW50cnkgPSB7XG4gIGtleTogc3RyaW5nLFxuICBxdW90ZWQ6IGJvb2xlYW4sXG4gIHZhbHVlOiBvLkV4cHJlc3Npb25cbn07XG50eXBlIE1hcExpdGVyYWwgPSBNYXBFbnRyeVtdO1xuXG5mdW5jdGlvbiBtYXBFbnRyeShrZXk6IHN0cmluZywgdmFsdWU6IG8uRXhwcmVzc2lvbik6IE1hcEVudHJ5IHtcbiAgcmV0dXJuIHtrZXksIHZhbHVlLCBxdW90ZWQ6IGZhbHNlfTtcbn1cblxuZXhwb3J0IGNsYXNzIEluamVjdGFibGVDb21waWxlciB7XG4gIHByaXZhdGUgdG9rZW5JbmplY3RvcjogU3RhdGljU3ltYm9sO1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIHJlZmxlY3RvcjogQ29tcGlsZVJlZmxlY3RvciwgcHJpdmF0ZSBhbHdheXNHZW5lcmF0ZURlZjogYm9vbGVhbikge1xuICAgIHRoaXMudG9rZW5JbmplY3RvciA9IHJlZmxlY3Rvci5yZXNvbHZlRXh0ZXJuYWxSZWZlcmVuY2UoSWRlbnRpZmllcnMuSW5qZWN0b3IpO1xuICB9XG5cbiAgcHJpdmF0ZSBkZXBzQXJyYXkoZGVwczogYW55W10sIGN0eDogT3V0cHV0Q29udGV4dCk6IG8uRXhwcmVzc2lvbltdIHtcbiAgICByZXR1cm4gZGVwcy5tYXAoZGVwID0+IHtcbiAgICAgIGxldCB0b2tlbiA9IGRlcDtcbiAgICAgIGxldCBhcmdzID0gW3Rva2VuXTtcbiAgICAgIGxldCBmbGFnczogSW5qZWN0RmxhZ3MgPSBJbmplY3RGbGFncy5EZWZhdWx0O1xuICAgICAgaWYgKEFycmF5LmlzQXJyYXkoZGVwKSkge1xuICAgICAgICBmb3IgKGxldCBpID0gMDsgaSA8IGRlcC5sZW5ndGg7IGkrKykge1xuICAgICAgICAgIGNvbnN0IHYgPSBkZXBbaV07XG4gICAgICAgICAgaWYgKHYpIHtcbiAgICAgICAgICAgIGlmICh2Lm5nTWV0YWRhdGFOYW1lID09PSAnT3B0aW9uYWwnKSB7XG4gICAgICAgICAgICAgIGZsYWdzIHw9IEluamVjdEZsYWdzLk9wdGlvbmFsO1xuICAgICAgICAgICAgfSBlbHNlIGlmICh2Lm5nTWV0YWRhdGFOYW1lID09PSAnU2tpcFNlbGYnKSB7XG4gICAgICAgICAgICAgIGZsYWdzIHw9IEluamVjdEZsYWdzLlNraXBTZWxmO1xuICAgICAgICAgICAgfSBlbHNlIGlmICh2Lm5nTWV0YWRhdGFOYW1lID09PSAnU2VsZicpIHtcbiAgICAgICAgICAgICAgZmxhZ3MgfD0gSW5qZWN0RmxhZ3MuU2VsZjtcbiAgICAgICAgICAgIH0gZWxzZSBpZiAodi5uZ01ldGFkYXRhTmFtZSA9PT0gJ0luamVjdCcpIHtcbiAgICAgICAgICAgICAgdG9rZW4gPSB2LnRva2VuO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgdG9rZW4gPSB2O1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBsZXQgdG9rZW5FeHByOiBvLkV4cHJlc3Npb247XG4gICAgICBpZiAodHlwZW9mIHRva2VuID09PSAnc3RyaW5nJykge1xuICAgICAgICB0b2tlbkV4cHIgPSBvLmxpdGVyYWwodG9rZW4pO1xuICAgICAgfSBlbHNlIGlmICh0b2tlbiA9PT0gdGhpcy50b2tlbkluamVjdG9yKSB7XG4gICAgICAgIHRva2VuRXhwciA9IG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5JTkpFQ1RPUik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0b2tlbkV4cHIgPSBjdHguaW1wb3J0RXhwcih0b2tlbik7XG4gICAgICB9XG5cbiAgICAgIGlmIChmbGFncyAhPT0gSW5qZWN0RmxhZ3MuRGVmYXVsdCkge1xuICAgICAgICBhcmdzID0gW3Rva2VuRXhwciwgby5saXRlcmFsKGZsYWdzKV07XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBhcmdzID0gW3Rva2VuRXhwcl07XG4gICAgICB9XG4gICAgICByZXR1cm4gby5pbXBvcnRFeHByKElkZW50aWZpZXJzLmluamVjdCkuY2FsbEZuKGFyZ3MpO1xuICAgIH0pO1xuICB9XG5cbiAgZmFjdG9yeUZvcihpbmplY3RhYmxlOiBDb21waWxlSW5qZWN0YWJsZU1ldGFkYXRhLCBjdHg6IE91dHB1dENvbnRleHQpOiBvLkV4cHJlc3Npb24ge1xuICAgIGxldCByZXRWYWx1ZTogby5FeHByZXNzaW9uO1xuICAgIGlmIChpbmplY3RhYmxlLnVzZUV4aXN0aW5nKSB7XG4gICAgICByZXRWYWx1ZSA9IG8uaW1wb3J0RXhwcihJZGVudGlmaWVycy5pbmplY3QpLmNhbGxGbihbY3R4LmltcG9ydEV4cHIoaW5qZWN0YWJsZS51c2VFeGlzdGluZyldKTtcbiAgICB9IGVsc2UgaWYgKGluamVjdGFibGUudXNlRmFjdG9yeSkge1xuICAgICAgY29uc3QgZGVwcyA9IGluamVjdGFibGUuZGVwcyB8fCBbXTtcbiAgICAgIGlmIChkZXBzLmxlbmd0aCA+IDApIHtcbiAgICAgICAgcmV0VmFsdWUgPSBjdHguaW1wb3J0RXhwcihpbmplY3RhYmxlLnVzZUZhY3RvcnkpLmNhbGxGbih0aGlzLmRlcHNBcnJheShkZXBzLCBjdHgpKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiBjdHguaW1wb3J0RXhwcihpbmplY3RhYmxlLnVzZUZhY3RvcnkpO1xuICAgICAgfVxuICAgIH0gZWxzZSBpZiAoaW5qZWN0YWJsZS51c2VWYWx1ZSkge1xuICAgICAgcmV0VmFsdWUgPSBjb252ZXJ0VmFsdWVUb091dHB1dEFzdChjdHgsIGluamVjdGFibGUudXNlVmFsdWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjb25zdCBjbGF6eiA9IGluamVjdGFibGUudXNlQ2xhc3MgfHwgaW5qZWN0YWJsZS5zeW1ib2w7XG4gICAgICBjb25zdCBkZXBBcmdzID0gdGhpcy5kZXBzQXJyYXkodGhpcy5yZWZsZWN0b3IucGFyYW1ldGVycyhjbGF6eiksIGN0eCk7XG4gICAgICByZXRWYWx1ZSA9IG5ldyBvLkluc3RhbnRpYXRlRXhwcihjdHguaW1wb3J0RXhwcihjbGF6eiksIGRlcEFyZ3MpO1xuICAgIH1cbiAgICByZXR1cm4gby5mbihcbiAgICAgICAgW10sIFtuZXcgby5SZXR1cm5TdGF0ZW1lbnQocmV0VmFsdWUpXSwgdW5kZWZpbmVkLCB1bmRlZmluZWQsXG4gICAgICAgIGluamVjdGFibGUuc3ltYm9sLm5hbWUgKyAnX0ZhY3RvcnknKTtcbiAgfVxuXG4gIGluamVjdGFibGVEZWYoaW5qZWN0YWJsZTogQ29tcGlsZUluamVjdGFibGVNZXRhZGF0YSwgY3R4OiBPdXRwdXRDb250ZXh0KTogby5FeHByZXNzaW9uIHtcbiAgICBsZXQgcHJvdmlkZWRJbjogby5FeHByZXNzaW9uID0gby5OVUxMX0VYUFI7XG4gICAgaWYgKGluamVjdGFibGUucHJvdmlkZWRJbiAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICBpZiAoaW5qZWN0YWJsZS5wcm92aWRlZEluID09PSBudWxsKSB7XG4gICAgICAgIHByb3ZpZGVkSW4gPSBvLk5VTExfRVhQUjtcbiAgICAgIH0gZWxzZSBpZiAodHlwZW9mIGluamVjdGFibGUucHJvdmlkZWRJbiA9PT0gJ3N0cmluZycpIHtcbiAgICAgICAgcHJvdmlkZWRJbiA9IG8ubGl0ZXJhbChpbmplY3RhYmxlLnByb3ZpZGVkSW4pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcHJvdmlkZWRJbiA9IGN0eC5pbXBvcnRFeHByKGluamVjdGFibGUucHJvdmlkZWRJbik7XG4gICAgICB9XG4gICAgfVxuICAgIGNvbnN0IGRlZjogTWFwTGl0ZXJhbCA9IFtcbiAgICAgIG1hcEVudHJ5KCdmYWN0b3J5JywgdGhpcy5mYWN0b3J5Rm9yKGluamVjdGFibGUsIGN0eCkpLFxuICAgICAgbWFwRW50cnkoJ3Rva2VuJywgY3R4LmltcG9ydEV4cHIoaW5qZWN0YWJsZS50eXBlLnJlZmVyZW5jZSkpLFxuICAgICAgbWFwRW50cnkoJ3Byb3ZpZGVkSW4nLCBwcm92aWRlZEluKSxcbiAgICBdO1xuICAgIHJldHVybiBvLmltcG9ydEV4cHIoSWRlbnRpZmllcnMuybXJtWRlZmluZUluamVjdGFibGUpLmNhbGxGbihbby5saXRlcmFsTWFwKGRlZildKTtcbiAgfVxuXG4gIGNvbXBpbGUoaW5qZWN0YWJsZTogQ29tcGlsZUluamVjdGFibGVNZXRhZGF0YSwgY3R4OiBPdXRwdXRDb250ZXh0KTogdm9pZCB7XG4gICAgaWYgKHRoaXMuYWx3YXlzR2VuZXJhdGVEZWYgfHwgaW5qZWN0YWJsZS5wcm92aWRlZEluICE9PSB1bmRlZmluZWQpIHtcbiAgICAgIGNvbnN0IGNsYXNzTmFtZSA9IGlkZW50aWZpZXJOYW1lKGluamVjdGFibGUudHlwZSkgITtcbiAgICAgIGNvbnN0IGNsYXp6ID0gbmV3IG8uQ2xhc3NTdG10KFxuICAgICAgICAgIGNsYXNzTmFtZSwgbnVsbCxcbiAgICAgICAgICBbXG4gICAgICAgICAgICBuZXcgby5DbGFzc0ZpZWxkKFxuICAgICAgICAgICAgICAgICduZ0luamVjdGFibGVEZWYnLCBvLklORkVSUkVEX1RZUEUsIFtvLlN0bXRNb2RpZmllci5TdGF0aWNdLFxuICAgICAgICAgICAgICAgIHRoaXMuaW5qZWN0YWJsZURlZihpbmplY3RhYmxlLCBjdHgpKSxcbiAgICAgICAgICBdLFxuICAgICAgICAgIFtdLCBuZXcgby5DbGFzc01ldGhvZChudWxsLCBbXSwgW10pLCBbXSk7XG4gICAgICBjdHguc3RhdGVtZW50cy5wdXNoKGNsYXp6KTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==