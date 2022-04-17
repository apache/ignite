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
        define("@angular/language-service/src/language_service", ["require", "exports", "tslib", "@angular/compiler-cli/src/language_services", "@angular/language-service/src/completions", "@angular/language-service/src/definitions", "@angular/language-service/src/diagnostics", "@angular/language-service/src/hover", "@angular/language-service/src/types", "@angular/language-service/src/utils"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var language_services_1 = require("@angular/compiler-cli/src/language_services");
    var completions_1 = require("@angular/language-service/src/completions");
    var definitions_1 = require("@angular/language-service/src/definitions");
    var diagnostics_1 = require("@angular/language-service/src/diagnostics");
    var hover_1 = require("@angular/language-service/src/hover");
    var types_1 = require("@angular/language-service/src/types");
    var utils_1 = require("@angular/language-service/src/utils");
    /**
     * Create an instance of an Angular `LanguageService`.
     *
     * @publicApi
     */
    function createLanguageService(host) {
        return new LanguageServiceImpl(host);
    }
    exports.createLanguageService = createLanguageService;
    var LanguageServiceImpl = /** @class */ (function () {
        function LanguageServiceImpl(host) {
            this.host = host;
        }
        Object.defineProperty(LanguageServiceImpl.prototype, "metadataResolver", {
            get: function () { return this.host.resolver; },
            enumerable: true,
            configurable: true
        });
        LanguageServiceImpl.prototype.getTemplateReferences = function () { return this.host.getTemplateReferences(); };
        LanguageServiceImpl.prototype.getDiagnostics = function (fileName) {
            var results = [];
            var templates = this.host.getTemplates(fileName);
            if (templates && templates.length) {
                results.push.apply(results, tslib_1.__spread(this.getTemplateDiagnostics(fileName, templates)));
            }
            var declarations = this.host.getDeclarations(fileName);
            if (declarations && declarations.length) {
                var summary = this.host.getAnalyzedModules();
                results.push.apply(results, tslib_1.__spread(diagnostics_1.getDeclarationDiagnostics(declarations, summary)));
            }
            return uniqueBySpan(results);
        };
        LanguageServiceImpl.prototype.getPipesAt = function (fileName, position) {
            var templateInfo = this.host.getTemplateAstAtPosition(fileName, position);
            if (templateInfo) {
                return templateInfo.pipes;
            }
            return [];
        };
        LanguageServiceImpl.prototype.getCompletionsAt = function (fileName, position) {
            var templateInfo = this.host.getTemplateAstAtPosition(fileName, position);
            if (templateInfo) {
                return completions_1.getTemplateCompletions(templateInfo);
            }
        };
        LanguageServiceImpl.prototype.getDefinitionAt = function (fileName, position) {
            var templateInfo = this.host.getTemplateAstAtPosition(fileName, position);
            if (templateInfo) {
                return definitions_1.getDefinition(templateInfo);
            }
        };
        LanguageServiceImpl.prototype.getHoverAt = function (fileName, position) {
            var templateInfo = this.host.getTemplateAstAtPosition(fileName, position);
            if (templateInfo) {
                return hover_1.getHover(templateInfo);
            }
        };
        LanguageServiceImpl.prototype.getTemplateDiagnostics = function (fileName, templates) {
            var e_1, _a;
            var results = [];
            var _loop_1 = function (template) {
                var ast = this_1.host.getTemplateAst(template, fileName);
                if (ast) {
                    if (ast.parseErrors && ast.parseErrors.length) {
                        results.push.apply(results, tslib_1.__spread(ast.parseErrors.map(function (e) { return ({
                            kind: types_1.DiagnosticKind.Error,
                            span: utils_1.offsetSpan(utils_1.spanOf(e.span), template.span.start),
                            message: e.msg
                        }); })));
                    }
                    else if (ast.templateAst && ast.htmlAst) {
                        var info = {
                            templateAst: ast.templateAst,
                            htmlAst: ast.htmlAst,
                            offset: template.span.start,
                            query: template.query,
                            members: template.members
                        };
                        var expressionDiagnostics = language_services_1.getTemplateExpressionDiagnostics(info);
                        results.push.apply(results, tslib_1.__spread(expressionDiagnostics));
                    }
                    if (ast.errors) {
                        results.push.apply(results, tslib_1.__spread(ast.errors.map(function (e) { return ({ kind: e.kind, span: e.span || template.span, message: e.message }); })));
                    }
                }
            };
            var this_1 = this;
            try {
                for (var templates_1 = tslib_1.__values(templates), templates_1_1 = templates_1.next(); !templates_1_1.done; templates_1_1 = templates_1.next()) {
                    var template = templates_1_1.value;
                    _loop_1(template);
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (templates_1_1 && !templates_1_1.done && (_a = templates_1.return)) _a.call(templates_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
            return results;
        };
        return LanguageServiceImpl;
    }());
    function uniqueBySpan(elements) {
        var e_2, _a;
        if (elements) {
            var result = [];
            var map = new Map();
            try {
                for (var elements_1 = tslib_1.__values(elements), elements_1_1 = elements_1.next(); !elements_1_1.done; elements_1_1 = elements_1.next()) {
                    var element = elements_1_1.value;
                    var span = element.span;
                    var set = map.get(span.start);
                    if (!set) {
                        set = new Set();
                        map.set(span.start, set);
                    }
                    if (!set.has(span.end)) {
                        set.add(span.end);
                        result.push(element);
                    }
                }
            }
            catch (e_2_1) { e_2 = { error: e_2_1 }; }
            finally {
                try {
                    if (elements_1_1 && !elements_1_1.done && (_a = elements_1.return)) _a.call(elements_1);
                }
                finally { if (e_2) throw e_2.error; }
            }
            return result;
        }
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGFuZ3VhZ2Vfc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2xhbmd1YWdlLXNlcnZpY2Uvc3JjL2xhbmd1YWdlX3NlcnZpY2UudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBR0gsaUZBQXFIO0lBRXJILHlFQUFxRDtJQUNyRCx5RUFBNEM7SUFDNUMseUVBQXdEO0lBQ3hELDZEQUFpQztJQUNqQyw2REFBNEo7SUFDNUosNkRBQTJDO0lBSTNDOzs7O09BSUc7SUFDSCxTQUFnQixxQkFBcUIsQ0FBQyxJQUF5QjtRQUM3RCxPQUFPLElBQUksbUJBQW1CLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDdkMsQ0FBQztJQUZELHNEQUVDO0lBRUQ7UUFDRSw2QkFBb0IsSUFBeUI7WUFBekIsU0FBSSxHQUFKLElBQUksQ0FBcUI7UUFBRyxDQUFDO1FBRWpELHNCQUFZLGlEQUFnQjtpQkFBNUIsY0FBMEQsT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDLENBQUM7OztXQUFBO1FBRXRGLG1EQUFxQixHQUFyQixjQUFvQyxPQUFPLElBQUksQ0FBQyxJQUFJLENBQUMscUJBQXFCLEVBQUUsQ0FBQyxDQUFDLENBQUM7UUFFL0UsNENBQWMsR0FBZCxVQUFlLFFBQWdCO1lBQzdCLElBQUksT0FBTyxHQUFnQixFQUFFLENBQUM7WUFDOUIsSUFBSSxTQUFTLEdBQUcsSUFBSSxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsUUFBUSxDQUFDLENBQUM7WUFDakQsSUFBSSxTQUFTLElBQUksU0FBUyxDQUFDLE1BQU0sRUFBRTtnQkFDakMsT0FBTyxDQUFDLElBQUksT0FBWixPQUFPLG1CQUFTLElBQUksQ0FBQyxzQkFBc0IsQ0FBQyxRQUFRLEVBQUUsU0FBUyxDQUFDLEdBQUU7YUFDbkU7WUFFRCxJQUFJLFlBQVksR0FBRyxJQUFJLENBQUMsSUFB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RyxFQUFFO3dCQUNSLEdBQUcsR0FBRyxJQUFJLEdBQUcsRUFBRSxDQUFDO3dCQUNoQixHQUFHLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQUM7cUJBQzFCO29CQUNELElBQUksQ0FBQyxHQUFHLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRTt3QkFDdEIsR0FBRyxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUM7d0JBQ2xCLE1BQU0sQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7cUJBQ3RCO2lCQUNGOzs7Ozs7Ozs7WUFDRCxPQUFPLE1BQU0sQ0FBQztTQUNmO0lBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlTWV0YWRhdGFSZXNvbHZlciwgQ29tcGlsZVBpcGVTdW1tYXJ5fSBmcm9tICdAYW5ndWxhci9jb21waWxlcic7XG5pbXBvcnQge0RpYWdub3N0aWNUZW1wbGF0ZUluZm8sIGdldFRlbXBsYXRlRXhwcmVzc2lvbkRpYWdub3N0aWNzfSBmcm9tICdAYW5ndWxhci9jb21waWxlci1jbGkvc3JjL2xhbmd1YWdlX3NlcnZpY2VzJztcblxuaW1wb3J0IHtnZXRUZW1wbGF0ZUNvbXBsZXRpb25zfSBmcm9tICcuL2NvbXBsZXRpb25zJztcbmltcG9ydCB7Z2V0RGVmaW5pdGlvbn0gZnJvbSAnLi9kZWZpbml0aW9ucyc7XG5pbXBvcnQge2dldERlY2xhcmF0aW9uRGlhZ25vc3RpY3N9IGZyb20gJy4vZGlhZ25vc3RpY3MnO1xuaW1wb3J0IHtnZXRIb3Zlcn0gZnJvbSAnLi9ob3Zlcic7XG5pbXBvcnQge0NvbXBsZXRpb25zLCBEZWZpbml0aW9uLCBEaWFnbm9zdGljLCBEaWFnbm9zdGljS2luZCwgRGlhZ25vc3RpY3MsIEhvdmVyLCBMYW5ndWFnZVNlcnZpY2UsIExhbmd1YWdlU2VydmljZUhvc3QsIFNwYW4sIFRlbXBsYXRlU291cmNlfSBmcm9tICcuL3R5cGVzJztcbmltcG9ydCB7b2Zmc2V0U3Bhbiwgc3Bhbk9mfSBmcm9tICcuL3V0aWxzJztcblxuXG5cbi8qKlxuICogQ3JlYXRlIGFuIGluc3RhbmNlIG9mIGFuIEFuZ3VsYXIgYExhbmd1YWdlU2VydmljZWAuXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlTGFuZ3VhZ2VTZXJ2aWNlKGhvc3Q6IExhbmd1YWdlU2VydmljZUhvc3QpOiBMYW5ndWFnZVNlcnZpY2Uge1xuICByZXR1cm4gbmV3IExhbmd1YWdlU2VydmljZUltcGwoaG9zdCk7XG59XG5cbmNsYXNzIExhbmd1YWdlU2VydmljZUltcGwgaW1wbGVtZW50cyBMYW5ndWFnZVNlcnZpY2Uge1xuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGhvc3Q6IExhbmd1YWdlU2VydmljZUhvc3QpIHt9XG5cbiAgcHJpdmF0ZSBnZXQgbWV0YWRhdGFSZXNvbHZlcigpOiBDb21waWxlTWV0YWRhdGFSZXNvbHZlciB7IHJldHVybiB0aGlzLmhvc3QucmVzb2x2ZXI7IH1cblxuICBnZXRUZW1wbGF0ZVJlZmVyZW5jZXMoKTogc3RyaW5nW10geyByZXR1cm4gdGhpcy5ob3N0LmdldFRlbXBsYXRlUmVmZXJlbmNlcygpOyB9XG5cbiAgZ2V0RGlhZ25vc3RpY3MoZmlsZU5hbWU6IHN0cmluZyk6IERpYWdub3N0aWNzfHVuZGVmaW5lZCB7XG4gICAgbGV0IHJlc3VsdHM6IERpYWdub3N0aWNzID0gW107XG4gICAgbGV0IHRlbXBsYXRlcyA9IHRoaXMuaG9zdC5nZXRUZW1wbGF0ZXMoZmlsZU5hbWUpO1xuICAgIGlmICh0ZW1wbGF0ZXMgJiYgdGVtcGxhdGVzLmxlbmd0aCkge1xuICAgICAgcmVzdWx0cy5wdXNoKC4uLnRoaXMuZ2V0VGVtcGxhdGVEaWFnbm9zdGljcyhmaWxlTmFtZSwgdGVtcGxhdGVzKSk7XG4gICAgfVxuXG4gICAgbGV0IGRlY2xhcmF0aW9ucyA9IHRoaXMuaG9zdC5nZXREZWNsYXJhdGlvbnMoZmlsZU5hbWUpO1xuICAgIGlmIChkZWNsYXJhdGlvbnMgJiYgZGVjbGFyYXRpb25zLmxlbmd0aCkge1xuICAgICAgY29uc3Qgc3VtbWFyeSA9IHRoaXMuaG9zdC5nZXRBbmFseXplZE1vZHVsZXMoKTtcbiAgICAgIHJlc3VsdHMucHVzaCguLi5nZXREZWNsYXJhdGlvbkRpYWdub3N0aWNzKGRlY2xhcmF0aW9ucywgc3VtbWFyeSkpO1xuICAgIH1cblxuICAgIHJldHVybiB1bmlxdWVCeVNwYW4ocmVzdWx0cyk7XG4gIH1cblxuICBnZXRQaXBlc0F0KGZpbGVOYW1lOiBzdHJpbmcsIHBvc2l0aW9uOiBudW1iZXIpOiBDb21waWxlUGlwZVN1bW1hcnlbXSB7XG4gICAgbGV0IHRlbXBsYXRlSW5mbyA9IHRoaXMuaG9zdC5nZXRUZW1wbGF0ZUFzdEF0UG9zaXRpb24oZmlsZU5hbWUsIHBvc2l0aW9uKTtcbiAgICBpZiAodGVtcGxhdGVJbmZvKSB7XG4gICAgICByZXR1cm4gdGVtcGxhdGVJbmZvLnBpcGVzO1xuICAgIH1cbiAgICByZXR1cm4gW107XG4gIH1cblxuICBnZXRDb21wbGV0aW9uc0F0KGZpbGVOYW1lOiBzdHJpbmcsIHBvc2l0aW9uOiBudW1iZXIpOiBDb21wbGV0aW9ucyB7XG4gICAgbGV0IHRlbXBsYXRlSW5mbyA9IHRoaXMuaG9zdC5nZXRUZW1wbGF0ZUFzdEF0UG9zaXRpb24oZmlsZU5hbWUsIHBvc2l0aW9uKTtcbiAgICBpZiAodGVtcGxhdGVJbmZvKSB7XG4gICAgICByZXR1cm4gZ2V0VGVtcGxhdGVDb21wbGV0aW9ucyh0ZW1wbGF0ZUluZm8pO1xuICAgIH1cbiAgfVxuXG4gIGdldERlZmluaXRpb25BdChmaWxlTmFtZTogc3RyaW5nLCBwb3NpdGlvbjogbnVtYmVyKTogRGVmaW5pdGlvbiB7XG4gICAgbGV0IHRlbXBsYXRlSW5mbyA9IHRoaXMuaG9zdC5nZXRUZW1wbGF0ZUFzdEF0UG9zaXRpb24oZmlsZU5hbWUsIHBvc2l0aW9uKTtcbiAgICBpZiAodGVtcGxhdGVJbmZvKSB7XG4gICAgICByZXR1cm4gZ2V0RGVmaW5pdGlvbih0ZW1wbGF0ZUluZm8pO1xuICAgIH1cbiAgfVxuXG4gIGdldEhvdmVyQXQoZmlsZU5hbWU6IHN0cmluZywgcG9zaXRpb246IG51bWJlcik6IEhvdmVyfHVuZGVmaW5lZCB7XG4gICAgbGV0IHRlbXBsYXRlSW5mbyA9IHRoaXMuaG9zdC5nZXRUZW1wbGF0ZUFzdEF0UG9zaXRpb24oZmlsZU5hbWUsIHBvc2l0aW9uKTtcbiAgICBpZiAodGVtcGxhdGVJbmZvKSB7XG4gICAgICByZXR1cm4gZ2V0SG92ZXIodGVtcGxhdGVJbmZvKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGdldFRlbXBsYXRlRGlhZ25vc3RpY3MoZmlsZU5hbWU6IHN0cmluZywgdGVtcGxhdGVzOiBUZW1wbGF0ZVNvdXJjZVtdKTogRGlhZ25vc3RpY3Mge1xuICAgIGNvbnN0IHJlc3VsdHM6IERpYWdub3N0aWNzID0gW107XG4gICAgZm9yIChjb25zdCB0ZW1wbGF0ZSBvZiB0ZW1wbGF0ZXMpIHtcbiAgICAgIGNvbnN0IGFzdCA9IHRoaXMuaG9zdC5nZXRUZW1wbGF0ZUFzdCh0ZW1wbGF0ZSwgZmlsZU5hbWUpO1xuICAgICAgaWYgKGFzdCkge1xuICAgICAgICBpZiAoYXN0LnBhcnNlRXJyb3JzICYmIGFzdC5wYXJzZUVycm9ycy5sZW5ndGgpIHtcbiAgICAgICAgICByZXN1bHRzLnB1c2goLi4uYXN0LnBhcnNlRXJyb3JzLm1hcDxEaWFnbm9zdGljPihcbiAgICAgICAgICAgICAgZSA9PiAoe1xuICAgICAgICAgICAgICAgIGtpbmQ6IERpYWdub3N0aWNLaW5kLkVycm9yLFxuICAgICAgICAgICAgICAgIHNwYW46IG9mZnNldFNwYW4oc3Bhbk9mKGUuc3BhbiksIHRlbXBsYXRlLnNwYW4uc3RhcnQpLFxuICAgICAgICAgICAgICAgIG1lc3NhZ2U6IGUubXNnXG4gICAgICAgICAgICAgIH0pKSk7XG4gICAgICAgIH0gZWxzZSBpZiAoYXN0LnRlbXBsYXRlQXN0ICYmIGFzdC5odG1sQXN0KSB7XG4gICAgICAgICAgY29uc3QgaW5mbzogRGlhZ25vc3RpY1RlbXBsYXRlSW5mbyA9IHtcbiAgICAgICAgICAgIHRlbXBsYXRlQXN0OiBhc3QudGVtcGxhdGVBc3QsXG4gICAgICAgICAgICBodG1sQXN0OiBhc3QuaHRtbEFzdCxcbiAgICAgICAgICAgIG9mZnNldDogdGVtcGxhdGUuc3Bhbi5zdGFydCxcbiAgICAgICAgICAgIHF1ZXJ5OiB0ZW1wbGF0ZS5xdWVyeSxcbiAgICAgICAgICAgIG1lbWJlcnM6IHRlbXBsYXRlLm1lbWJlcnNcbiAgICAgICAgICB9O1xuICAgICAgICAgIGNvbnN0IGV4cHJlc3Npb25EaWFnbm9zdGljcyA9IGdldFRlbXBsYXRlRXhwcmVzc2lvbkRpYWdub3N0aWNzKGluZm8pO1xuICAgICAgICAgIHJlc3VsdHMucHVzaCguLi5leHByZXNzaW9uRGlhZ25vc3RpY3MpO1xuICAgICAgICB9XG4gICAgICAgIGlmIChhc3QuZXJyb3JzKSB7XG4gICAgICAgICAgcmVzdWx0cy5wdXNoKC4uLmFzdC5lcnJvcnMubWFwPERpYWdub3N0aWM+KFxuICAgICAgICAgICAgICBlID0+ICh7a2luZDogZS5raW5kLCBzcGFuOiBlLnNwYW4gfHwgdGVtcGxhdGUuc3BhbiwgbWVzc2FnZTogZS5tZXNzYWdlfSkpKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0cztcbiAgfVxufVxuXG5mdW5jdGlvbiB1bmlxdWVCeVNwYW4gPCBUIGV4dGVuZHMge1xuICBzcGFuOiBTcGFuO1xufVxuPiAoZWxlbWVudHM6IFRbXSB8IHVuZGVmaW5lZCk6IFRbXXx1bmRlZmluZWQge1xuICBpZiAoZWxlbWVudHMpIHtcbiAgICBjb25zdCByZXN1bHQ6IFRbXSA9IFtdO1xuICAgIGNvbnN0IG1hcCA9IG5ldyBNYXA8bnVtYmVyLCBTZXQ8bnVtYmVyPj4oKTtcbiAgICBmb3IgKGNvbnN0IGVsZW1lbnQgb2YgZWxlbWVudHMpIHtcbiAgICAgIGxldCBzcGFuID0gZWxlbWVudC5zcGFuO1xuICAgICAgbGV0IHNldCA9IG1hcC5nZXQoc3Bhbi5zdGFydCk7XG4gICAgICBpZiAoIXNldCkge1xuICAgICAgICBzZXQgPSBuZXcgU2V0KCk7XG4gICAgICAgIG1hcC5zZXQoc3Bhbi5zdGFydCwgc2V0KTtcbiAgICAgIH1cbiAgICAgIGlmICghc2V0LmhhcyhzcGFuLmVuZCkpIHtcbiAgICAgICAgc2V0LmFkZChzcGFuLmVuZCk7XG4gICAgICAgIHJlc3VsdC5wdXNoKGVsZW1lbnQpO1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG59XG4iXX0=