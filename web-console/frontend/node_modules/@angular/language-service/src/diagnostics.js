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
        define("@angular/language-service/src/diagnostics", ["require", "exports", "tslib", "@angular/language-service/src/types"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var types_1 = require("@angular/language-service/src/types");
    function getDeclarationDiagnostics(declarations, modules) {
        var e_1, _a;
        var results = [];
        var directives = undefined;
        var _loop_1 = function (declaration) {
            var e_2, _a;
            var report = function (message, span) {
                results.push({
                    kind: types_1.DiagnosticKind.Error,
                    span: span || declaration.declarationSpan, message: message
                });
            };
            try {
                for (var _b = (e_2 = void 0, tslib_1.__values(declaration.errors)), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var error = _c.value;
                    report(error.message, error.span);
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                }
                finally { if (e_2) throw e_2.error; }
            }
            if (declaration.metadata) {
                if (declaration.metadata.isComponent) {
                    if (!modules.ngModuleByPipeOrDirective.has(declaration.type)) {
                        report("Component '" + declaration.type.name + "' is not included in a module and will not be available inside a template. Consider adding it to a NgModule declaration");
                    }
                    var _d = declaration.metadata.template, template = _d.template, templateUrl = _d.templateUrl;
                    if (template === null && !templateUrl) {
                        report("Component '" + declaration.type.name + "' must have a template or templateUrl");
                    }
                    else if (template && templateUrl) {
                        report("Component '" + declaration.type.name + "' must not have both template and templateUrl");
                    }
                }
                else {
                    if (!directives) {
                        directives = new Set();
                        modules.ngModules.forEach(function (module) {
                            module.declaredDirectives.forEach(function (directive) { directives.add(directive.reference); });
                        });
                    }
                    if (!directives.has(declaration.type)) {
                        report("Directive '" + declaration.type.name + "' is not included in a module and will not be available inside a template. Consider adding it to a NgModule declaration");
                    }
                }
            }
        };
        try {
            for (var declarations_1 = tslib_1.__values(declarations), declarations_1_1 = declarations_1.next(); !declarations_1_1.done; declarations_1_1 = declarations_1.next()) {
                var declaration = declarations_1_1.value;
                _loop_1(declaration);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (declarations_1_1 && !declarations_1_1.done && (_a = declarations_1.return)) _a.call(declarations_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return results;
    }
    exports.getDeclarationDiagnostics = getDeclarationDiagnostics;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlhZ25vc3RpY3MuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9sYW5ndWFnZS1zZXJ2aWNlL3NyYy9kaWFnbm9zdGljcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFJSCw2REFBNEg7SUFNNUgsU0FBZ0IseUJBQXlCLENBQ3JDLFlBQTBCLEVBQUUsT0FBMEI7O1FBQ3hELElBQU0sT0FBTyxHQUFnQixFQUFFLENBQUM7UUFFaEMsSUFBSSxVQUFVLEdBQWdDLFNBQVMsQ0FBQztnQ0FDN0MsV0FBVzs7WUFDcEIsSUFBTSxNQUFNLEdBQUcsVUFBQyxPQUF3QyxFQUFFLElBQVc7Z0JBQ25FLE9BQU8sQ0FBQyxJQUFJLENBQWE7b0JBQ3ZCLElBQUksRUFBRSxzQkFBYyxDQUFDLEtBQUs7b0JBQzFCLElBQUksRUFBRSxJQUFJLElBQUksV0FBVyxDQUFDLGVBQWUsRUFBRSxPQUFPLFNBQUE7aUJBQ25ELENBQUMsQ0FBQztZQUNMLENBQUMsQ0FBQzs7Z0JBQ0YsS0FBb0IsSUFBQSxvQkFBQSxpQkFBQSxXQUFXLENBQUMsTUFBTSxDQUFBLENBQUEsZ0JBQUEsNEJBQUU7b0JBQW5DLElBQU0sS0FBSyxXQUFBO29CQUNkLE1BQU0sQ0FBQyxLQUFLLENBQUMsT0FBTyxFQUFFLEtBQUssQ0FBQyxJQUFJLENBQUMsQ0FBQztpQkFDbkM7Ozs7Ozs7OztZQUNELElBQUksV0FBVyxDQUFDLFFBQVEsRUFBRTtnQkFDeEIsSUFBSSxXQUFXLENBQUMsUUFBUSxDQUFDLFdBQVcsRUFBRTtvQkFDcEMsSUFBSSxDQUFDLE9BQU8sQ0FBQyx5QkFBeUIsQ0FBQyxHQUFHLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxFQUFFO3dCQUM1RCxNQUFNLENBQ0YsZ0JBQWMsV0FBVyxDQUFDLElBQUksQ0FBQyxJQUFJLDRIQUF5SCxDQUFDLENBQUM7cUJBQ25LO29CQUNLLElBQUEsa0NBQXlELEVBQXhELHNCQUFRLEVBQUUsNEJBQThDLENBQUM7b0JBQ2hFLElBQUksUUFBUSxLQUFLLElBQUksSUFBSSxDQUFDLFdBQVcsRUFBRTt3QkFDckMsTUFBTSxDQUFDLGdCQUFjLFdBQVcsQ0FBQyxJQUFJLENBQUMsSUFBSSwwQ0FBdUMsQ0FBQyxDQUFDO3FCQUNwRjt5QkFBTSxJQUFJLFFBQVEsSUFBSSxXQUFXLEVBQUU7d0JBQ2xDLE1BQU0sQ0FDRixnQkFBYyxXQUFXLENBQUMsSUFBSSxDQUFDLElBQUksa0RBQStDLENBQUMsQ0FBQztxQkFDekY7aUJBQ0Y7cUJBQU07b0JBQ0wsSUFBSSxDQUFDLFVBQVUsRUFBRTt3QkFDZixVQUFVLEdBQUcsSUFBSSxHQUFHLEVBQUUsQ0FBQzt3QkFDdkIsT0FBTyxDQUFDLFNBQVMsQ0FBQyxPQUFPLENBQUMsVUFBQSxNQUFNOzRCQUM5QixNQUFNLENBQUMsa0JBQWtCLENBQUMsT0FBTyxDQUM3QixVQUFBLFNBQVMsSUFBTSxVQUFZLENBQUMsR0FBRyxDQUFDLFNBQVMsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO3dCQUMvRCxDQUFDLENBQUMsQ0FBQztxQkFDSjtvQkFDRCxJQUFJLENBQUMsVUFBVSxDQUFDLEdBQUcsQ0FBQyxXQUFXLENBQUMsSUFBSSxDQUFDLEVBQUU7d0JBQ3JDLE1BQU0sQ0FDRixnQkFBYyxXQUFXLENBQUMsSUFBSSxDQUFDLElBQUksNEhBQXlILENBQUMsQ0FBQztxQkFDbks7aUJBQ0Y7YUFDRjs7O1lBcENILEtBQTBCLElBQUEsaUJBQUEsaUJBQUEsWUFBWSxDQUFBLDBDQUFBO2dCQUFqQyxJQUFNLFdBQVcseUJBQUE7d0JBQVgsV0FBVzthQXFDckI7Ozs7Ozs7OztRQUVELE9BQU8sT0FBTyxDQUFDO0lBQ2pCLENBQUM7SUE3Q0QsOERBNkNDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge05nQW5hbHl6ZWRNb2R1bGVzLCBTdGF0aWNTeW1ib2x9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyJztcbmltcG9ydCB7QXN0UmVzdWx0fSBmcm9tICcuL2NvbW1vbic7XG5pbXBvcnQge0RlY2xhcmF0aW9ucywgRGlhZ25vc3RpYywgRGlhZ25vc3RpY0tpbmQsIERpYWdub3N0aWNNZXNzYWdlQ2hhaW4sIERpYWdub3N0aWNzLCBTcGFuLCBUZW1wbGF0ZVNvdXJjZX0gZnJvbSAnLi90eXBlcyc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgQXN0UHJvdmlkZXIge1xuICBnZXRUZW1wbGF0ZUFzdCh0ZW1wbGF0ZTogVGVtcGxhdGVTb3VyY2UsIGZpbGVOYW1lOiBzdHJpbmcpOiBBc3RSZXN1bHQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXREZWNsYXJhdGlvbkRpYWdub3N0aWNzKFxuICAgIGRlY2xhcmF0aW9uczogRGVjbGFyYXRpb25zLCBtb2R1bGVzOiBOZ0FuYWx5emVkTW9kdWxlcyk6IERpYWdub3N0aWNzIHtcbiAgY29uc3QgcmVzdWx0czogRGlhZ25vc3RpY3MgPSBbXTtcblxuICBsZXQgZGlyZWN0aXZlczogU2V0PFN0YXRpY1N5bWJvbD58dW5kZWZpbmVkID0gdW5kZWZpbmVkO1xuICBmb3IgKGNvbnN0IGRlY2xhcmF0aW9uIG9mIGRlY2xhcmF0aW9ucykge1xuICAgIGNvbnN0IHJlcG9ydCA9IChtZXNzYWdlOiBzdHJpbmcgfCBEaWFnbm9zdGljTWVzc2FnZUNoYWluLCBzcGFuPzogU3BhbikgPT4ge1xuICAgICAgcmVzdWx0cy5wdXNoKDxEaWFnbm9zdGljPntcbiAgICAgICAga2luZDogRGlhZ25vc3RpY0tpbmQuRXJyb3IsXG4gICAgICAgIHNwYW46IHNwYW4gfHwgZGVjbGFyYXRpb24uZGVjbGFyYXRpb25TcGFuLCBtZXNzYWdlXG4gICAgICB9KTtcbiAgICB9O1xuICAgIGZvciAoY29uc3QgZXJyb3Igb2YgZGVjbGFyYXRpb24uZXJyb3JzKSB7XG4gICAgICByZXBvcnQoZXJyb3IubWVzc2FnZSwgZXJyb3Iuc3Bhbik7XG4gICAgfVxuICAgIGlmIChkZWNsYXJhdGlvbi5tZXRhZGF0YSkge1xuICAgICAgaWYgKGRlY2xhcmF0aW9uLm1ldGFkYXRhLmlzQ29tcG9uZW50KSB7XG4gICAgICAgIGlmICghbW9kdWxlcy5uZ01vZHVsZUJ5UGlwZU9yRGlyZWN0aXZlLmhhcyhkZWNsYXJhdGlvbi50eXBlKSkge1xuICAgICAgICAgIHJlcG9ydChcbiAgICAgICAgICAgICAgYENvbXBvbmVudCAnJHtkZWNsYXJhdGlvbi50eXBlLm5hbWV9JyBpcyBub3QgaW5jbHVkZWQgaW4gYSBtb2R1bGUgYW5kIHdpbGwgbm90IGJlIGF2YWlsYWJsZSBpbnNpZGUgYSB0ZW1wbGF0ZS4gQ29uc2lkZXIgYWRkaW5nIGl0IHRvIGEgTmdNb2R1bGUgZGVjbGFyYXRpb25gKTtcbiAgICAgICAgfVxuICAgICAgICBjb25zdCB7dGVtcGxhdGUsIHRlbXBsYXRlVXJsfSA9IGRlY2xhcmF0aW9uLm1ldGFkYXRhLnRlbXBsYXRlICE7XG4gICAgICAgIGlmICh0ZW1wbGF0ZSA9PT0gbnVsbCAmJiAhdGVtcGxhdGVVcmwpIHtcbiAgICAgICAgICByZXBvcnQoYENvbXBvbmVudCAnJHtkZWNsYXJhdGlvbi50eXBlLm5hbWV9JyBtdXN0IGhhdmUgYSB0ZW1wbGF0ZSBvciB0ZW1wbGF0ZVVybGApO1xuICAgICAgICB9IGVsc2UgaWYgKHRlbXBsYXRlICYmIHRlbXBsYXRlVXJsKSB7XG4gICAgICAgICAgcmVwb3J0KFxuICAgICAgICAgICAgICBgQ29tcG9uZW50ICcke2RlY2xhcmF0aW9uLnR5cGUubmFtZX0nIG11c3Qgbm90IGhhdmUgYm90aCB0ZW1wbGF0ZSBhbmQgdGVtcGxhdGVVcmxgKTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgaWYgKCFkaXJlY3RpdmVzKSB7XG4gICAgICAgICAgZGlyZWN0aXZlcyA9IG5ldyBTZXQoKTtcbiAgICAgICAgICBtb2R1bGVzLm5nTW9kdWxlcy5mb3JFYWNoKG1vZHVsZSA9PiB7XG4gICAgICAgICAgICBtb2R1bGUuZGVjbGFyZWREaXJlY3RpdmVzLmZvckVhY2goXG4gICAgICAgICAgICAgICAgZGlyZWN0aXZlID0+IHsgZGlyZWN0aXZlcyAhLmFkZChkaXJlY3RpdmUucmVmZXJlbmNlKTsgfSk7XG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKCFkaXJlY3RpdmVzLmhhcyhkZWNsYXJhdGlvbi50eXBlKSkge1xuICAgICAgICAgIHJlcG9ydChcbiAgICAgICAgICAgICAgYERpcmVjdGl2ZSAnJHtkZWNsYXJhdGlvbi50eXBlLm5hbWV9JyBpcyBub3QgaW5jbHVkZWQgaW4gYSBtb2R1bGUgYW5kIHdpbGwgbm90IGJlIGF2YWlsYWJsZSBpbnNpZGUgYSB0ZW1wbGF0ZS4gQ29uc2lkZXIgYWRkaW5nIGl0IHRvIGEgTmdNb2R1bGUgZGVjbGFyYXRpb25gKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiByZXN1bHRzO1xufVxuIl19