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
        define("@angular/language-service/src/locate_symbol", ["require", "exports", "tslib", "@angular/compiler", "@angular/compiler-cli/src/language_services", "@angular/language-service/src/expressions", "@angular/language-service/src/utils"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compiler_1 = require("@angular/compiler");
    var language_services_1 = require("@angular/compiler-cli/src/language_services");
    var expressions_1 = require("@angular/language-service/src/expressions");
    var utils_1 = require("@angular/language-service/src/utils");
    function locateSymbol(info) {
        if (!info.position)
            return undefined;
        var templatePosition = info.position - info.template.span.start;
        var path = utils_1.findTemplateAstAt(info.templateAst, templatePosition);
        if (path.tail) {
            var symbol_1 = undefined;
            var span_1 = undefined;
            var attributeValueSymbol_1 = function (ast, inEvent) {
                if (inEvent === void 0) { inEvent = false; }
                var attribute = findAttribute(info);
                if (attribute) {
                    if (utils_1.inSpan(templatePosition, utils_1.spanOf(attribute.valueSpan))) {
                        var dinfo = utils_1.diagnosticInfoFromTemplateInfo(info);
                        var scope = language_services_1.getExpressionScope(dinfo, path, inEvent);
                        if (attribute.valueSpan) {
                            var expressionOffset = attribute.valueSpan.start.offset;
                            var result = expressions_1.getExpressionSymbol(scope, ast, templatePosition - expressionOffset, info.template.query);
                            if (result) {
                                symbol_1 = result.symbol;
                                span_1 = utils_1.offsetSpan(result.span, expressionOffset);
                            }
                        }
                        return true;
                    }
                }
                return false;
            };
            path.tail.visit({
                visitNgContent: function (ast) { },
                visitEmbeddedTemplate: function (ast) { },
                visitElement: function (ast) {
                    var component = ast.directives.find(function (d) { return d.directive.isComponent; });
                    if (component) {
                        symbol_1 = info.template.query.getTypeSymbol(component.directive.type.reference);
                        symbol_1 = symbol_1 && new OverrideKindSymbol(symbol_1, 'component');
                        span_1 = utils_1.spanOf(ast);
                    }
                    else {
                        // Find a directive that matches the element name
                        var directive = ast.directives.find(function (d) { return d.directive.selector != null && d.directive.selector.indexOf(ast.name) >= 0; });
                        if (directive) {
                            symbol_1 = info.template.query.getTypeSymbol(directive.directive.type.reference);
                            symbol_1 = symbol_1 && new OverrideKindSymbol(symbol_1, 'directive');
                            span_1 = utils_1.spanOf(ast);
                        }
                    }
                },
                visitReference: function (ast) {
                    symbol_1 = ast.value && info.template.query.getTypeSymbol(compiler_1.tokenReference(ast.value));
                    span_1 = utils_1.spanOf(ast);
                },
                visitVariable: function (ast) { },
                visitEvent: function (ast) {
                    if (!attributeValueSymbol_1(ast.handler, /* inEvent */ true)) {
                        symbol_1 = findOutputBinding(info, path, ast);
                        symbol_1 = symbol_1 && new OverrideKindSymbol(symbol_1, 'event');
                        span_1 = utils_1.spanOf(ast);
                    }
                },
                visitElementProperty: function (ast) { attributeValueSymbol_1(ast.value); },
                visitAttr: function (ast) { },
                visitBoundText: function (ast) {
                    var expressionPosition = templatePosition - ast.sourceSpan.start.offset;
                    if (utils_1.inSpan(expressionPosition, ast.value.span)) {
                        var dinfo = utils_1.diagnosticInfoFromTemplateInfo(info);
                        var scope = language_services_1.getExpressionScope(dinfo, path, /* includeEvent */ false);
                        var result = expressions_1.getExpressionSymbol(scope, ast.value, expressionPosition, info.template.query);
                        if (result) {
                            symbol_1 = result.symbol;
                            span_1 = utils_1.offsetSpan(result.span, ast.sourceSpan.start.offset);
                        }
                    }
                },
                visitText: function (ast) { },
                visitDirective: function (ast) {
                    symbol_1 = info.template.query.getTypeSymbol(ast.directive.type.reference);
                    span_1 = utils_1.spanOf(ast);
                },
                visitDirectiveProperty: function (ast) {
                    if (!attributeValueSymbol_1(ast.value)) {
                        symbol_1 = findInputBinding(info, path, ast);
                        span_1 = utils_1.spanOf(ast);
                    }
                }
            }, null);
            if (symbol_1 && span_1) {
                return { symbol: symbol_1, span: utils_1.offsetSpan(span_1, info.template.span.start) };
            }
        }
    }
    exports.locateSymbol = locateSymbol;
    function findAttribute(info) {
        if (info.position) {
            var templatePosition = info.position - info.template.span.start;
            var path = compiler_1.findNode(info.htmlAst, templatePosition);
            return path.first(compiler_1.Attribute);
        }
    }
    function findInputBinding(info, path, binding) {
        var e_1, _a;
        var element = path.first(compiler_1.ElementAst);
        if (element) {
            try {
                for (var _b = tslib_1.__values(element.directives), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var directive = _c.value;
                    var invertedInput = invertMap(directive.directive.inputs);
                    var fieldName = invertedInput[binding.templateName];
                    if (fieldName) {
                        var classSymbol = info.template.query.getTypeSymbol(directive.directive.type.reference);
                        if (classSymbol) {
                            return classSymbol.members().get(fieldName);
                        }
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
        }
    }
    function findOutputBinding(info, path, binding) {
        var e_2, _a;
        var element = path.first(compiler_1.ElementAst);
        if (element) {
            try {
                for (var _b = tslib_1.__values(element.directives), _c = _b.next(); !_c.done; _c = _b.next()) {
                    var directive = _c.value;
                    var invertedOutputs = invertMap(directive.directive.outputs);
                    var fieldName = invertedOutputs[binding.name];
                    if (fieldName) {
                        var classSymbol = info.template.query.getTypeSymbol(directive.directive.type.reference);
                        if (classSymbol) {
                            return classSymbol.members().get(fieldName);
                        }
                    }
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                }
                finally { if (e_2) throw e_2.error; }
            }
        }
    }
    function invertMap(obj) {
        var e_3, _a;
        var result = {};
        try {
            for (var _b = tslib_1.__values(Object.keys(obj)), _c = _b.next(); !_c.done; _c = _b.next()) {
                var name_1 = _c.value;
                var v = obj[name_1];
                result[v] = name_1;
            }
        }
        catch (e_3_1) { e_3 = { error: e_3_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_3) throw e_3.error; }
        }
        return result;
    }
    /**
     * Wrap a symbol and change its kind to component.
     */
    var OverrideKindSymbol = /** @class */ (function () {
        function OverrideKindSymbol(sym, kindOverride) {
            this.sym = sym;
            this.kind = kindOverride;
        }
        Object.defineProperty(OverrideKindSymbol.prototype, "name", {
            get: function () { return this.sym.name; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(OverrideKindSymbol.prototype, "language", {
            get: function () { return this.sym.language; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(OverrideKindSymbol.prototype, "type", {
            get: function () { return this.sym.type; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(OverrideKindSymbol.prototype, "container", {
            get: function () { return this.sym.container; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(OverrideKindSymbol.prototype, "public", {
            get: function () { return this.sym.public; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(OverrideKindSymbol.prototype, "callable", {
            get: function () { return this.sym.callable; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(OverrideKindSymbol.prototype, "nullable", {
            get: function () { return this.sym.nullable; },
            enumerable: true,
            configurable: true
        });
        Object.defineProperty(OverrideKindSymbol.prototype, "definition", {
            get: function () { return this.sym.definition; },
            enumerable: true,
            configurable: true
        });
        OverrideKindSymbol.prototype.members = function () { return this.sym.members(); };
        OverrideKindSymbol.prototype.signatures = function () { return this.sym.signatures(); };
        OverrideKindSymbol.prototype.selectSignature = function (types) { return this.sym.selectSignature(types); };
        OverrideKindSymbol.prototype.indexed = function (argument) { return this.sym.indexed(argument); };
        return OverrideKindSymbol;
    }());
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9jYXRlX3N5bWJvbC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2xhbmd1YWdlLXNlcnZpY2Uvc3JjL2xvY2F0ZV9zeW1ib2wudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBRUgsOENBQWtKO0lBQ2xKLGlGQUErRTtJQUcvRSx5RUFBa0Q7SUFFbEQsNkRBQXNHO0lBT3RHLFNBQWdCLFlBQVksQ0FBQyxJQUFrQjtRQUM3QyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVE7WUFBRSxPQUFPLFNBQVMsQ0FBQztRQUNyQyxJQUFNLGdCQUFnQixHQUFHLElBQUksQ0FBQyxRQUFRLEdBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDO1FBQ2xFLElBQU0sSUFBSSxHQUFHLHlCQUFpQixDQUFDLElBQUksQ0FBQyxXQUFXLEVBQUUsZ0JBQWdCLENBQUMsQ0FBQztRQUNuRSxJQUFJLElBQUksQ0FBQyxJQUFJLEVBQUU7WUFDYixJQUFJLFFBQU0sR0FBcUIsU0FBUyxDQUFDO1lBQ3pDLElBQUksTUFBSSxHQUFtQixTQUFTLENBQUM7WUFDckMsSUFBTSxzQkFBb0IsR0FBRyxVQUFDLEdBQVEsRUFBRSxPQUF3QjtnQkFBeEIsd0JBQUEsRUFBQSxlQUF3QjtnQkFDOUQsSUFBTSxTQUFTLEdBQUcsYUFBYSxDQUFDLElBQUksQ0FBQyxDQUFDO2dCQUN0QyxJQUFJLFNBQVMsRUFBRTtvQkFDYixJQUFJLGNBQU0sQ0FBQyxnQkFBZ0IsRUFBRSxjQUFNLENBQUMsU0FBUyxDQUFDLFNBQVMsQ0FBQyxDQUFDLEVBQUU7d0JBQ3pELElBQU0sS0FBSyxHQUFHLHNDQUE4QixDQUFDLElBQUksQ0FBQyxDQUFDO3dCQUNuRCxJQUFNLEtBQUssR0FBRyxzQ0FBa0IsQ0FBQyxLQUFLLEVBQUUsSUFBSSxFQUFFLE9BQU8sQ0FBQyxDQUFDO3dCQUN2RCxJQUFJ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TyxJQUFJLENBQUMsR0FBRyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUM7OztXQUFBO1FBRWpELHNCQUFJLHdDQUFRO2lCQUFaLGNBQTBCLE9BQU8sSUFBSSxDQUFDLEdBQUcsQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDOzs7V0FBQTtRQUVyRCxzQkFBSSx3Q0FBUTtpQkFBWixjQUEwQixPQUFPLElBQUksQ0FBQyxHQUFHLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQzs7O1dBQUE7UUFFckQsc0JBQUksMENBQVU7aUJBQWQsY0FBK0IsT0FBTyxJQUFJLENBQUMsR0FBRyxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUM7OztXQUFBO1FBRTVELG9DQUFPLEdBQVAsY0FBWSxPQUFPLElBQUksQ0FBQyxHQUFHLENBQUMsT0FBTyxFQUFFLENBQUMsQ0FBQyxDQUFDO1FBRXhDLHVDQUFVLEdBQVYsY0FBZSxPQUFPLElBQUksQ0FBQyxHQUFHLENBQUMsVUFBVSxFQUFFLENBQUMsQ0FBQyxDQUFDO1FBRTlDLDRDQUFlLEdBQWYsVUFBZ0IsS0FBZSxJQUFJLE9BQU8sSUFBSSxDQUFDLEdBQUcsQ0FBQyxlQUFlLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBRTVFLG9DQUFPLEdBQVAsVUFBUSxRQUFnQixJQUFJLE9BQU8sSUFBSSxDQUFDLEdBQUcsQ0FBQyxPQUFPLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ2xFLHlCQUFDO0lBQUQsQ0FBQyxBQTNCRCxJQTJCQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtBU1QsIEF0dHJpYnV0ZSwgQm91bmREaXJlY3RpdmVQcm9wZXJ0eUFzdCwgQm91bmRFdmVudEFzdCwgRWxlbWVudEFzdCwgVGVtcGxhdGVBc3RQYXRoLCBmaW5kTm9kZSwgdG9rZW5SZWZlcmVuY2V9IGZyb20gJ0Bhbmd1bGFyL2NvbXBpbGVyJztcbmltcG9ydCB7Z2V0RXhwcmVzc2lvblNjb3BlfSBmcm9tICdAYW5ndWxhci9jb21waWxlci1jbGkvc3JjL2xhbmd1YWdlX3NlcnZpY2VzJztcblxuaW1wb3J0IHtUZW1wbGF0ZUluZm99IGZyb20gJy4vY29tbW9uJztcbmltcG9ydCB7Z2V0RXhwcmVzc2lvblN5bWJvbH0gZnJvbSAnLi9leHByZXNzaW9ucyc7XG5pbXBvcnQge0RlZmluaXRpb24sIFNwYW4sIFN5bWJvbH0gZnJvbSAnLi90eXBlcyc7XG5pbXBvcnQge2RpYWdub3N0aWNJbmZvRnJvbVRlbXBsYXRlSW5mbywgZmluZFRlbXBsYXRlQXN0QXQsIGluU3Bhbiwgb2Zmc2V0U3Bhbiwgc3Bhbk9mfSBmcm9tICcuL3V0aWxzJztcblxuZXhwb3J0IGludGVyZmFjZSBTeW1ib2xJbmZvIHtcbiAgc3ltYm9sOiBTeW1ib2w7XG4gIHNwYW46IFNwYW47XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBsb2NhdGVTeW1ib2woaW5mbzogVGVtcGxhdGVJbmZvKTogU3ltYm9sSW5mb3x1bmRlZmluZWQge1xuICBpZiAoIWluZm8ucG9zaXRpb24pIHJldHVybiB1bmRlZmluZWQ7XG4gIGNvbnN0IHRlbXBsYXRlUG9zaXRpb24gPSBpbmZvLnBvc2l0aW9uIC0gaW5mby50ZW1wbGF0ZS5zcGFuLnN0YXJ0O1xuICBjb25zdCBwYXRoID0gZmluZFRlbXBsYXRlQXN0QXQoaW5mby50ZW1wbGF0ZUFzdCwgdGVtcGxhdGVQb3NpdGlvbik7XG4gIGlmIChwYXRoLnRhaWwpIHtcbiAgICBsZXQgc3ltYm9sOiBTeW1ib2x8dW5kZWZpbmVkID0gdW5kZWZpbmVkO1xuICAgIGxldCBzcGFuOiBTcGFufHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgICBjb25zdCBhdHRyaWJ1dGVWYWx1ZVN5bWJvbCA9IChhc3Q6IEFTVCwgaW5FdmVudDogYm9vbGVhbiA9IGZhbHNlKTogYm9vbGVhbiA9PiB7XG4gICAgICBjb25zdCBhdHRyaWJ1dGUgPSBmaW5kQXR0cmlidXRlKGluZm8pO1xuICAgICAgaWYgKGF0dHJpYnV0ZSkge1xuICAgICAgICBpZiAoaW5TcGFuKHRlbXBsYXRlUG9zaXRpb24sIHNwYW5PZihhdHRyaWJ1dGUudmFsdWVTcGFuKSkpIHtcbiAgICAgICAgICBjb25zdCBkaW5mbyA9IGRpYWdub3N0aWNJbmZvRnJvbVRlbXBsYXRlSW5mbyhpbmZvKTtcbiAgICAgICAgICBjb25zdCBzY29wZSA9IGdldEV4cHJlc3Npb25TY29wZShkaW5mbywgcGF0aCwgaW5FdmVudCk7XG4gICAgICAgICAgaWYgKGF0dHJpYnV0ZS52YWx1ZVNwYW4pIHtcbiAgICAgICAgICAgIGNvbnN0IGV4cHJlc3Npb25PZmZzZXQgPSBhdHRyaWJ1dGUudmFsdWVTcGFuLnN0YXJ0Lm9mZnNldDtcbiAgICAgICAgICAgIGNvbnN0IHJlc3VsdCA9IGdldEV4cHJlc3Npb25TeW1ib2woXG4gICAgICAgICAgICAgICAgc2NvcGUsIGFzdCwgdGVtcGxhdGVQb3NpdGlvbiAtIGV4cHJlc3Npb25PZmZzZXQsIGluZm8udGVtcGxhdGUucXVlcnkpO1xuICAgICAgICAgICAgaWYgKHJlc3VsdCkge1xuICAgICAgICAgICAgICBzeW1ib2wgPSByZXN1bHQuc3ltYm9sO1xuICAgICAgICAgICAgICBzcGFuID0gb2Zmc2V0U3BhbihyZXN1bHQuc3BhbiwgZXhwcmVzc2lvbk9mZnNldCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfTtcbiAgICBwYXRoLnRhaWwudmlzaXQoXG4gICAgICAgIHtcbiAgICAgICAgICB2aXNpdE5nQ29udGVudChhc3QpIHt9LFxuICAgICAgICAgIHZpc2l0RW1iZWRkZWRUZW1wbGF0ZShhc3QpIHt9LFxuICAgICAgICAgIHZpc2l0RWxlbWVudChhc3QpIHtcbiAgICAgICAgICAgIGNvbnN0IGNvbXBvbmVudCA9IGFzdC5kaXJlY3RpdmVzLmZpbmQoZCA9PiBkLmRpcmVjdGl2ZS5pc0NvbXBvbmVudCk7XG4gICAgICAgICAgICBpZiAoY29tcG9uZW50KSB7XG4gICAgICAgICAgICAgIHN5bWJvbCA9IGluZm8udGVtcGxhdGUucXVlcnkuZ2V0VHlwZVN5bWJvbChjb21wb25lbnQuZGlyZWN0aXZlLnR5cGUucmVmZXJlbmNlKTtcbiAgICAgICAgICAgICAgc3ltYm9sID0gc3ltYm9sICYmIG5ldyBPdmVycmlkZUtpbmRTeW1ib2woc3ltYm9sLCAnY29tcG9uZW50Jyk7XG4gICAgICAgICAgICAgIHNwYW4gPSBzcGFuT2YoYXN0KTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgIC8vIEZpbmQgYSBkaXJlY3RpdmUgdGhhdCBtYXRjaGVzIHRoZSBlbGVtZW50IG5hbWVcbiAgICAgICAgICAgICAgY29uc3QgZGlyZWN0aXZlID0gYXN0LmRpcmVjdGl2ZXMuZmluZChcbiAgICAgICAgICAgICAgICAgIGQgPT4gZC5kaXJlY3RpdmUuc2VsZWN0b3IgIT0gbnVsbCAmJiBkLmRpcmVjdGl2ZS5zZWxlY3Rvci5pbmRleE9mKGFzdC5uYW1lKSA+PSAwKTtcbiAgICAgICAgICAgICAgaWYgKGRpcmVjdGl2ZSkge1xuICAgICAgICAgICAgICAgIHN5bWJvbCA9IGluZm8udGVtcGxhdGUucXVlcnkuZ2V0VHlwZVN5bWJvbChkaXJlY3RpdmUuZGlyZWN0aXZlLnR5cGUucmVmZXJlbmNlKTtcbiAgICAgICAgICAgICAgICBzeW1ib2wgPSBzeW1ib2wgJiYgbmV3IE92ZXJyaWRlS2luZFN5bWJvbChzeW1ib2wsICdkaXJlY3RpdmUnKTtcbiAgICAgICAgICAgICAgICBzcGFuID0gc3Bhbk9mKGFzdCk7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9LFxuICAgICAgICAgIHZpc2l0UmVmZXJlbmNlKGFzdCkge1xuICAgICAgICAgICAgc3ltYm9sID0gYXN0LnZhbHVlICYmIGluZm8udGVtcGxhdGUucXVlcnkuZ2V0VHlwZVN5bWJvbCh0b2tlblJlZmVyZW5jZShhc3QudmFsdWUpKTtcbiAgICAgICAgICAgIHNwYW4gPSBzcGFuT2YoYXN0KTtcbiAgICAgICAgICB9LFxuICAgICAgICAgIHZpc2l0VmFyaWFibGUoYXN0KSB7fSxcbiAgICAgICAgICB2aXNpdEV2ZW50KGFzdCkge1xuICAgICAgICAgICAgaWYgKCFhdHRyaWJ1dGVWYWx1ZVN5bWJvbChhc3QuaGFuZGxlciwgLyogaW5FdmVudCAqLyB0cnVlKSkge1xuICAgICAgICAgICAgICBzeW1ib2wgPSBmaW5kT3V0cHV0QmluZGluZyhpbmZvLCBwYXRoLCBhc3QpO1xuICAgICAgICAgICAgICBzeW1ib2wgPSBzeW1ib2wgJiYgbmV3IE92ZXJyaWRlS2luZFN5bWJvbChzeW1ib2wsICdldmVudCcpO1xuICAgICAgICAgICAgICBzcGFuID0gc3Bhbk9mKGFzdCk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfSxcbiAgICAgICAgICB2aXNpdEVsZW1lbnRQcm9wZXJ0eShhc3QpIHsgYXR0cmlidXRlVmFsdWVTeW1ib2woYXN0LnZhbHVlKTsgfSxcbiAgICAgICAgICB2aXNpdEF0dHIoYXN0KSB7fSxcbiAgICAgICAgICB2aXNpdEJvdW5kVGV4dChhc3QpIHtcbiAgICAgICAgICAgIGNvbnN0IGV4cHJlc3Npb25Qb3NpdGlvbiA9IHRlbXBsYXRlUG9zaXRpb24gLSBhc3Quc291cmNlU3Bhbi5zdGFydC5vZmZzZXQ7XG4gICAgICAgICAgICBpZiAoaW5TcGFuKGV4cHJlc3Npb25Qb3NpdGlvbiwgYXN0LnZhbHVlLnNwYW4pKSB7XG4gICAgICAgICAgICAgIGNvbnN0IGRpbmZvID0gZGlhZ25vc3RpY0luZm9Gcm9tVGVtcGxhdGVJbmZvKGluZm8pO1xuICAgICAgICAgICAgICBjb25zdCBzY29wZSA9IGdldEV4cHJlc3Npb25TY29wZShkaW5mbywgcGF0aCwgLyogaW5jbHVkZUV2ZW50ICovIGZhbHNlKTtcbiAgICAgICAgICAgICAgY29uc3QgcmVzdWx0ID1cbiAgICAgICAgICAgICAgICAgIGdldEV4cHJlc3Npb25TeW1ib2woc2NvcGUsIGFzdC52YWx1ZSwgZXhwcmVzc2lvblBvc2l0aW9uLCBpbmZvLnRlbXBsYXRlLnF1ZXJ5KTtcbiAgICAgICAgICAgICAgaWYgKHJlc3VsdCkge1xuICAgICAgICAgICAgICAgIHN5bWJvbCA9IHJlc3VsdC5zeW1ib2w7XG4gICAgICAgICAgICAgICAgc3BhbiA9IG9mZnNldFNwYW4ocmVzdWx0LnNwYW4sIGFzdC5zb3VyY2VTcGFuLnN0YXJ0Lm9mZnNldCk7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9LFxuICAgICAgICAgIHZpc2l0VGV4dChhc3QpIHt9LFxuICAgICAgICAgIHZpc2l0RGlyZWN0aXZlKGFzdCkge1xuICAgICAgICAgICAgc3ltYm9sID0gaW5mby50ZW1wbGF0ZS5xdWVyeS5nZXRUeXBlU3ltYm9sKGFzdC5kaXJlY3RpdmUudHlwZS5yZWZlcmVuY2UpO1xuICAgICAgICAgICAgc3BhbiA9IHNwYW5PZihhc3QpO1xuICAgICAgICAgIH0sXG4gICAgICAgICAgdmlzaXREaXJlY3RpdmVQcm9wZXJ0eShhc3QpIHtcbiAgICAgICAgICAgIGlmICghYXR0cmlidXRlVmFsdWVTeW1ib2woYXN0LnZhbHVlKSkge1xuICAgICAgICAgICAgICBzeW1ib2wgPSBmaW5kSW5wdXRCaW5kaW5nKGluZm8sIHBhdGgsIGFzdCk7XG4gICAgICAgICAgICAgIHNwYW4gPSBzcGFuT2YoYXN0KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH0sXG4gICAgICAgIG51bGwpO1xuICAgIGlmIChzeW1ib2wgJiYgc3Bhbikge1xuICAgICAgcmV0dXJuIHtzeW1ib2wsIHNwYW46IG9mZnNldFNwYW4oc3BhbiwgaW5mby50ZW1wbGF0ZS5zcGFuLnN0YXJ0KX07XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIGZpbmRBdHRyaWJ1dGUoaW5mbzogVGVtcGxhdGVJbmZvKTogQXR0cmlidXRlfHVuZGVmaW5lZCB7XG4gIGlmIChpbmZvLnBvc2l0aW9uKSB7XG4gICAgY29uc3QgdGVtcGxhdGVQb3NpdGlvbiA9IGluZm8ucG9zaXRpb24gLSBpbmZvLnRlbXBsYXRlLnNwYW4uc3RhcnQ7XG4gICAgY29uc3QgcGF0aCA9IGZpbmROb2RlKGluZm8uaHRtbEFzdCwgdGVtcGxhdGVQb3NpdGlvbik7XG4gICAgcmV0dXJuIHBhdGguZmlyc3QoQXR0cmlidXRlKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBmaW5kSW5wdXRCaW5kaW5nKFxuICAgIGluZm86IFRlbXBsYXRlSW5mbywgcGF0aDogVGVtcGxhdGVBc3RQYXRoLCBiaW5kaW5nOiBCb3VuZERpcmVjdGl2ZVByb3BlcnR5QXN0KTogU3ltYm9sfFxuICAgIHVuZGVmaW5lZCB7XG4gIGNvbnN0IGVsZW1lbnQgPSBwYXRoLmZpcnN0KEVsZW1lbnRBc3QpO1xuICBpZiAoZWxlbWVudCkge1xuICAgIGZvciAoY29uc3QgZGlyZWN0aXZlIG9mIGVsZW1lbnQuZGlyZWN0aXZlcykge1xuICAgICAgY29uc3QgaW52ZXJ0ZWRJbnB1dCA9IGludmVydE1hcChkaXJlY3RpdmUuZGlyZWN0aXZlLmlucHV0cyk7XG4gICAgICBjb25zdCBmaWVsZE5hbWUgPSBpbnZlcnRlZElucHV0W2JpbmRpbmcudGVtcGxhdGVOYW1lXTtcbiAgICAgIGlmIChmaWVsZE5hbWUpIHtcbiAgICAgICAgY29uc3QgY2xhc3NTeW1ib2wgPSBpbmZvLnRlbXBsYXRlLnF1ZXJ5LmdldFR5cGVTeW1ib2woZGlyZWN0aXZlLmRpcmVjdGl2ZS50eXBlLnJlZmVyZW5jZSk7XG4gICAgICAgIGlmIChjbGFzc1N5bWJvbCkge1xuICAgICAgICAgIHJldHVybiBjbGFzc1N5bWJvbC5tZW1iZXJzKCkuZ2V0KGZpZWxkTmFtZSk7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gZmluZE91dHB1dEJpbmRpbmcoXG4gICAgaW5mbzogVGVtcGxhdGVJbmZvLCBwYXRoOiBUZW1wbGF0ZUFzdFBhdGgsIGJpbmRpbmc6IEJvdW5kRXZlbnRBc3QpOiBTeW1ib2x8dW5kZWZpbmVkIHtcbiAgY29uc3QgZWxlbWVudCA9IHBhdGguZmlyc3QoRWxlbWVudEFzdCk7XG4gIGlmIChlbGVtZW50KSB7XG4gICAgZm9yIChjb25zdCBkaXJlY3RpdmUgb2YgZWxlbWVudC5kaXJlY3RpdmVzKSB7XG4gICAgICBjb25zdCBpbnZlcnRlZE91dHB1dHMgPSBpbnZlcnRNYXAoZGlyZWN0aXZlLmRpcmVjdGl2ZS5vdXRwdXRzKTtcbiAgICAgIGNvbnN0IGZpZWxkTmFtZSA9IGludmVydGVkT3V0cHV0c1tiaW5kaW5nLm5hbWVdO1xuICAgICAgaWYgKGZpZWxkTmFtZSkge1xuICAgICAgICBjb25zdCBjbGFzc1N5bWJvbCA9IGluZm8udGVtcGxhdGUucXVlcnkuZ2V0VHlwZVN5bWJvbChkaXJlY3RpdmUuZGlyZWN0aXZlLnR5cGUucmVmZXJlbmNlKTtcbiAgICAgICAgaWYgKGNsYXNzU3ltYm9sKSB7XG4gICAgICAgICAgcmV0dXJuIGNsYXNzU3ltYm9sLm1lbWJlcnMoKS5nZXQoZmllbGROYW1lKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG5mdW5jdGlvbiBpbnZlcnRNYXAob2JqOiB7W25hbWU6IHN0cmluZ106IHN0cmluZ30pOiB7W25hbWU6IHN0cmluZ106IHN0cmluZ30ge1xuICBjb25zdCByZXN1bHQ6IHtbbmFtZTogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuICBmb3IgKGNvbnN0IG5hbWUgb2YgT2JqZWN0LmtleXMob2JqKSkge1xuICAgIGNvbnN0IHYgPSBvYmpbbmFtZV07XG4gICAgcmVzdWx0W3ZdID0gbmFtZTtcbiAgfVxuICByZXR1cm4gcmVzdWx0O1xufVxuXG4vKipcbiAqIFdyYXAgYSBzeW1ib2wgYW5kIGNoYW5nZSBpdHMga2luZCB0byBjb21wb25lbnQuXG4gKi9cbmNsYXNzIE92ZXJyaWRlS2luZFN5bWJvbCBpbXBsZW1lbnRzIFN5bWJvbCB7XG4gIHB1YmxpYyByZWFkb25seSBraW5kOiBzdHJpbmc7XG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgc3ltOiBTeW1ib2wsIGtpbmRPdmVycmlkZTogc3RyaW5nKSB7IHRoaXMua2luZCA9IGtpbmRPdmVycmlkZTsgfVxuXG4gIGdldCBuYW1lKCk6IHN0cmluZyB7IHJldHVybiB0aGlzLnN5bS5uYW1lOyB9XG5cbiAgZ2V0IGxhbmd1YWdlKCk6IHN0cmluZyB7IHJldHVybiB0aGlzLnN5bS5sYW5ndWFnZTsgfVxuXG4gIGdldCB0eXBlKCk6IFN5bWJvbHx1bmRlZmluZWQgeyByZXR1cm4gdGhpcy5zeW0udHlwZTsgfVxuXG4gIGdldCBjb250YWluZXIoKTogU3ltYm9sfHVuZGVmaW5lZCB7IHJldHVybiB0aGlzLnN5bS5jb250YWluZXI7IH1cblxuICBnZXQgcHVibGljKCk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5zeW0ucHVibGljOyB9XG5cbiAgZ2V0IGNhbGxhYmxlKCk6IGJvb2xlYW4geyByZXR1cm4gdGhpcy5zeW0uY2FsbGFibGU7IH1cblxuICBnZXQgbnVsbGFibGUoKTogYm9vbGVhbiB7IHJldHVybiB0aGlzLnN5bS5udWxsYWJsZTsgfVxuXG4gIGdldCBkZWZpbml0aW9uKCk6IERlZmluaXRpb24geyByZXR1cm4gdGhpcy5zeW0uZGVmaW5pdGlvbjsgfVxuXG4gIG1lbWJlcnMoKSB7IHJldHVybiB0aGlzLnN5bS5tZW1iZXJzKCk7IH1cblxuICBzaWduYXR1cmVzKCkgeyByZXR1cm4gdGhpcy5zeW0uc2lnbmF0dXJlcygpOyB9XG5cbiAgc2VsZWN0U2lnbmF0dXJlKHR5cGVzOiBTeW1ib2xbXSkgeyByZXR1cm4gdGhpcy5zeW0uc2VsZWN0U2lnbmF0dXJlKHR5cGVzKTsgfVxuXG4gIGluZGV4ZWQoYXJndW1lbnQ6IFN5bWJvbCkgeyByZXR1cm4gdGhpcy5zeW0uaW5kZXhlZChhcmd1bWVudCk7IH1cbn1cbiJdfQ==