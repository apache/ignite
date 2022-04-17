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
        define("@angular/compiler/src/ml_parser/icu_ast_expander", ["require", "exports", "tslib", "@angular/compiler/src/parse_util", "@angular/compiler/src/ml_parser/ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var parse_util_1 = require("@angular/compiler/src/parse_util");
    var html = require("@angular/compiler/src/ml_parser/ast");
    // http://cldr.unicode.org/index/cldr-spec/plural-rules
    var PLURAL_CASES = ['zero', 'one', 'two', 'few', 'many', 'other'];
    /**
     * Expands special forms into elements.
     *
     * For example,
     *
     * ```
     * { messages.length, plural,
     *   =0 {zero}
     *   =1 {one}
     *   other {more than one}
     * }
     * ```
     *
     * will be expanded into
     *
     * ```
     * <ng-container [ngPlural]="messages.length">
     *   <ng-template ngPluralCase="=0">zero</ng-template>
     *   <ng-template ngPluralCase="=1">one</ng-template>
     *   <ng-template ngPluralCase="other">more than one</ng-template>
     * </ng-container>
     * ```
     */
    function expandNodes(nodes) {
        var expander = new _Expander();
        return new ExpansionResult(html.visitAll(expander, nodes), expander.isExpanded, expander.errors);
    }
    exports.expandNodes = expandNodes;
    var ExpansionResult = /** @class */ (function () {
        function ExpansionResult(nodes, expanded, errors) {
            this.nodes = nodes;
            this.expanded = expanded;
            this.errors = errors;
        }
        return ExpansionResult;
    }());
    exports.ExpansionResult = ExpansionResult;
    var ExpansionError = /** @class */ (function (_super) {
        tslib_1.__extends(ExpansionError, _super);
        function ExpansionError(span, errorMsg) {
            return _super.call(this, span, errorMsg) || this;
        }
        return ExpansionError;
    }(parse_util_1.ParseError));
    exports.ExpansionError = ExpansionError;
    /**
     * Expand expansion forms (plural, select) to directives
     *
     * @internal
     */
    var _Expander = /** @class */ (function () {
        function _Expander() {
            this.isExpanded = false;
            this.errors = [];
        }
        _Expander.prototype.visitElement = function (element, context) {
            return new html.Element(element.name, element.attrs, html.visitAll(this, element.children), element.sourceSpan, element.startSourceSpan, element.endSourceSpan);
        };
        _Expander.prototype.visitAttribute = function (attribute, context) { return attribute; };
        _Expander.prototype.visitText = function (text, context) { return text; };
        _Expander.prototype.visitComment = function (comment, context) { return comment; };
        _Expander.prototype.visitExpansion = function (icu, context) {
            this.isExpanded = true;
            return icu.type == 'plural' ? _expandPluralForm(icu, this.errors) :
                _expandDefaultForm(icu, this.errors);
        };
        _Expander.prototype.visitExpansionCase = function (icuCase, context) {
            throw new Error('Should not be reached');
        };
        return _Expander;
    }());
    // Plural forms are expanded to `NgPlural` and `NgPluralCase`s
    function _expandPluralForm(ast, errors) {
        var children = ast.cases.map(function (c) {
            if (PLURAL_CASES.indexOf(c.value) == -1 && !c.value.match(/^=\d+$/)) {
                errors.push(new ExpansionError(c.valueSourceSpan, "Plural cases should be \"=<number>\" or one of " + PLURAL_CASES.join(", ")));
            }
            var expansionResult = expandNodes(c.expression);
            errors.push.apply(errors, tslib_1.__spread(expansionResult.errors));
            return new html.Element("ng-template", [new html.Attribute('ngPluralCase', "" + c.value, c.valueSourceSpan)], expansionResult.nodes, c.sourceSpan, c.sourceSpan, c.sourceSpan);
        });
        var switchAttr = new html.Attribute('[ngPlural]', ast.switchValue, ast.switchValueSourceSpan);
        return new html.Element('ng-container', [switchAttr], children, ast.sourceSpan, ast.sourceSpan, ast.sourceSpan);
    }
    // ICU messages (excluding plural form) are expanded to `NgSwitch`  and `NgSwitchCase`s
    function _expandDefaultForm(ast, errors) {
        var children = ast.cases.map(function (c) {
            var expansionResult = expandNodes(c.expression);
            errors.push.apply(errors, tslib_1.__spread(expansionResult.errors));
            if (c.value === 'other') {
                // other is the default case when no values match
                return new html.Element("ng-template", [new html.Attribute('ngSwitchDefault', '', c.valueSourceSpan)], expansionResult.nodes, c.sourceSpan, c.sourceSpan, c.sourceSpan);
            }
            return new html.Element("ng-template", [new html.Attribute('ngSwitchCase', "" + c.value, c.valueSourceSpan)], expansionResult.nodes, c.sourceSpan, c.sourceSpan, c.sourceSpan);
        });
        var switchAttr = new html.Attribute('[ngSwitch]', ast.switchValue, ast.switchValueSourceSpan);
        return new html.Element('ng-container', [switchAttr], children, ast.sourceSpan, ast.sourceSpan, ast.sourceSpan);
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaWN1X2FzdF9leHBhbmRlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9tbF9wYXJzZXIvaWN1X2FzdF9leHBhbmRlci50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFFSCwrREFBMEQ7SUFFMUQsMERBQThCO0lBRTlCLHVEQUF1RDtJQUN2RCxJQUFNLFlBQVksR0FBYSxDQUFDLE1BQU0sRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFFOUU7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7T0FzQkc7SUFDSCxTQUFnQixXQUFXLENBQUMsS0FBa0I7UUFDNUMsSUFBTSxRQUFRLEdBQUcsSUFBSSxTQUFTLEVBQUUsQ0FBQztRQUNqQyxPQUFPLElBQUksZUFBZSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxFQUFFLEtBQUssQ0FBQyxFQUFFLFFBQVEsQ0FBQyxVQUFVLEVBQUUsUUFBUSxDQUFDLE1BQU0sQ0FBQyxDQUFDO0lBQ25HLENBQUM7SUFIRCxrQ0FHQztJQUVEO1FBQ0UseUJBQW1CLEtBQWtCLEVBQVMsUUFBaUIsRUFBUyxNQUFvQjtZQUF6RSxVQUFLLEdBQUwsS0FBSyxDQUFhO1lBQVMsYUFBUSxHQUFSLFFBQVEsQ0FBUztZQUFTLFdBQU0sR0FBTixNQUFNLENBQWM7UUFBRyxDQUFDO1FBQ2xHLHNCQUFDO0lBQUQsQ0FBQyxBQUZELElBRUM7SUFGWSwwQ0FBZTtJQUk1QjtRQUFvQywwQ0FBVTtRQUM1Qyx3QkFBWSxJQUFxQixFQUFFLFFBQWdCO21CQUFJLGtCQUFNLElBQUksRUFBRSxRQUFRLENBQUM7UUFBRSxDQUFDO1FBQ2pGLHFCQUFDO0lBQUQsQ0FBQyxBQUZELENBQW9DLHVCQUFVLEdBRTdDO0lBRlksd0NBQWM7SUFJM0I7Ozs7T0FJRztJQUNI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cUIsQ0FBQyxDQUFDO1FBQ2hHLE9BQU8sSUFBSSxJQUFJLENBQUMsT0FBTyxDQUNuQixjQUFjLEVBQUUsQ0FBQyxVQUFVLENBQUMsRUFBRSxRQUFRLEVBQUUsR0FBRyxDQUFDLFVBQVUsRUFBRSxHQUFHLENBQUMsVUFBVSxFQUFFLEdBQUcsQ0FBQyxVQUFVLENBQUMsQ0FBQztJQUM5RixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1BhcnNlRXJyb3IsIFBhcnNlU291cmNlU3Bhbn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5cbmltcG9ydCAqIGFzIGh0bWwgZnJvbSAnLi9hc3QnO1xuXG4vLyBodHRwOi8vY2xkci51bmljb2RlLm9yZy9pbmRleC9jbGRyLXNwZWMvcGx1cmFsLXJ1bGVzXG5jb25zdCBQTFVSQUxfQ0FTRVM6IHN0cmluZ1tdID0gWyd6ZXJvJywgJ29uZScsICd0d28nLCAnZmV3JywgJ21hbnknLCAnb3RoZXInXTtcblxuLyoqXG4gKiBFeHBhbmRzIHNwZWNpYWwgZm9ybXMgaW50byBlbGVtZW50cy5cbiAqXG4gKiBGb3IgZXhhbXBsZSxcbiAqXG4gKiBgYGBcbiAqIHsgbWVzc2FnZXMubGVuZ3RoLCBwbHVyYWwsXG4gKiAgID0wIHt6ZXJvfVxuICogICA9MSB7b25lfVxuICogICBvdGhlciB7bW9yZSB0aGFuIG9uZX1cbiAqIH1cbiAqIGBgYFxuICpcbiAqIHdpbGwgYmUgZXhwYW5kZWQgaW50b1xuICpcbiAqIGBgYFxuICogPG5nLWNvbnRhaW5lciBbbmdQbHVyYWxdPVwibWVzc2FnZXMubGVuZ3RoXCI+XG4gKiAgIDxuZy10ZW1wbGF0ZSBuZ1BsdXJhbENhc2U9XCI9MFwiPnplcm88L25nLXRlbXBsYXRlPlxuICogICA8bmctdGVtcGxhdGUgbmdQbHVyYWxDYXNlPVwiPTFcIj5vbmU8L25nLXRlbXBsYXRlPlxuICogICA8bmctdGVtcGxhdGUgbmdQbHVyYWxDYXNlPVwib3RoZXJcIj5tb3JlIHRoYW4gb25lPC9uZy10ZW1wbGF0ZT5cbiAqIDwvbmctY29udGFpbmVyPlxuICogYGBgXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBleHBhbmROb2Rlcyhub2RlczogaHRtbC5Ob2RlW10pOiBFeHBhbnNpb25SZXN1bHQge1xuICBjb25zdCBleHBhbmRlciA9IG5ldyBfRXhwYW5kZXIoKTtcbiAgcmV0dXJuIG5ldyBFeHBhbnNpb25SZXN1bHQoaHRtbC52aXNpdEFsbChleHBhbmRlciwgbm9kZXMpLCBleHBhbmRlci5pc0V4cGFuZGVkLCBleHBhbmRlci5lcnJvcnMpO1xufVxuXG5leHBvcnQgY2xhc3MgRXhwYW5zaW9uUmVzdWx0IHtcbiAgY29uc3RydWN0b3IocHVibGljIG5vZGVzOiBodG1sLk5vZGVbXSwgcHVibGljIGV4cGFuZGVkOiBib29sZWFuLCBwdWJsaWMgZXJyb3JzOiBQYXJzZUVycm9yW10pIHt9XG59XG5cbmV4cG9ydCBjbGFzcyBFeHBhbnNpb25FcnJvciBleHRlbmRzIFBhcnNlRXJyb3Ige1xuICBjb25zdHJ1Y3RvcihzcGFuOiBQYXJzZVNvdXJjZVNwYW4sIGVycm9yTXNnOiBzdHJpbmcpIHsgc3VwZXIoc3BhbiwgZXJyb3JNc2cpOyB9XG59XG5cbi8qKlxuICogRXhwYW5kIGV4cGFuc2lvbiBmb3JtcyAocGx1cmFsLCBzZWxlY3QpIHRvIGRpcmVjdGl2ZXNcbiAqXG4gKiBAaW50ZXJuYWxcbiAqL1xuY2xhc3MgX0V4cGFuZGVyIGltcGxlbWVudHMgaHRtbC5WaXNpdG9yIHtcbiAgaXNFeHBhbmRlZDogYm9vbGVhbiA9IGZhbHNlO1xuICBlcnJvcnM6IFBhcnNlRXJyb3JbXSA9IFtdO1xuXG4gIHZpc2l0RWxlbWVudChlbGVtZW50OiBodG1sLkVsZW1lbnQsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIG5ldyBodG1sLkVsZW1lbnQoXG4gICAgICAgIGVsZW1lbnQubmFtZSwgZWxlbWVudC5hdHRycywgaHRtbC52aXNpdEFsbCh0aGlzLCBlbGVtZW50LmNoaWxkcmVuKSwgZWxlbWVudC5zb3VyY2VTcGFuLFxuICAgICAgICBlbGVtZW50LnN0YXJ0U291cmNlU3BhbiwgZWxlbWVudC5lbmRTb3VyY2VTcGFuKTtcbiAgfVxuXG4gIHZpc2l0QXR0cmlidXRlKGF0dHJpYnV0ZTogaHRtbC5BdHRyaWJ1dGUsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiBhdHRyaWJ1dGU7IH1cblxuICB2aXNpdFRleHQodGV4dDogaHRtbC5UZXh0LCBjb250ZXh0OiBhbnkpOiBhbnkgeyByZXR1cm4gdGV4dDsgfVxuXG4gIHZpc2l0Q29tbWVudChjb21tZW50OiBodG1sLkNvbW1lbnQsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiBjb21tZW50OyB9XG5cbiAgdmlzaXRFeHBhbnNpb24oaWN1OiBodG1sLkV4cGFuc2lvbiwgY29udGV4dDogYW55KTogYW55IHtcbiAgICB0aGlzLmlzRXhwYW5kZWQgPSB0cnVlO1xuICAgIHJldHVybiBpY3UudHlwZSA9PSAncGx1cmFsJyA/IF9leHBhbmRQbHVyYWxGb3JtKGljdSwgdGhpcy5lcnJvcnMpIDpcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBfZXhwYW5kRGVmYXVsdEZvcm0oaWN1LCB0aGlzLmVycm9ycyk7XG4gIH1cblxuICB2aXNpdEV4cGFuc2lvbkNhc2UoaWN1Q2FzZTogaHRtbC5FeHBhbnNpb25DYXNlLCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIHRocm93IG5ldyBFcnJvcignU2hvdWxkIG5vdCBiZSByZWFjaGVkJyk7XG4gIH1cbn1cblxuLy8gUGx1cmFsIGZvcm1zIGFyZSBleHBhbmRlZCB0byBgTmdQbHVyYWxgIGFuZCBgTmdQbHVyYWxDYXNlYHNcbmZ1bmN0aW9uIF9leHBhbmRQbHVyYWxGb3JtKGFzdDogaHRtbC5FeHBhbnNpb24sIGVycm9yczogUGFyc2VFcnJvcltdKTogaHRtbC5FbGVtZW50IHtcbiAgY29uc3QgY2hpbGRyZW4gPSBhc3QuY2FzZXMubWFwKGMgPT4ge1xuICAgIGlmIChQTFVSQUxfQ0FTRVMuaW5kZXhPZihjLnZhbHVlKSA9PSAtMSAmJiAhYy52YWx1ZS5tYXRjaCgvXj1cXGQrJC8pKSB7XG4gICAgICBlcnJvcnMucHVzaChuZXcgRXhwYW5zaW9uRXJyb3IoXG4gICAgICAgICAgYy52YWx1ZVNvdXJjZVNwYW4sXG4gICAgICAgICAgYFBsdXJhbCBjYXNlcyBzaG91bGQgYmUgXCI9PG51bWJlcj5cIiBvciBvbmUgb2YgJHtQTFVSQUxfQ0FTRVMuam9pbihcIiwgXCIpfWApKTtcbiAgICB9XG5cbiAgICBjb25zdCBleHBhbnNpb25SZXN1bHQgPSBleHBhbmROb2RlcyhjLmV4cHJlc3Npb24pO1xuICAgIGVycm9ycy5wdXNoKC4uLmV4cGFuc2lvblJlc3VsdC5lcnJvcnMpO1xuXG4gICAgcmV0dXJuIG5ldyBodG1sLkVsZW1lbnQoXG4gICAgICAgIGBuZy10ZW1wbGF0ZWAsIFtuZXcgaHRtbC5BdHRyaWJ1dGUoJ25nUGx1cmFsQ2FzZScsIGAke2MudmFsdWV9YCwgYy52YWx1ZVNvdXJjZVNwYW4pXSxcbiAgICAgICAgZXhwYW5zaW9uUmVzdWx0Lm5vZGVzLCBjLnNvdXJjZVNwYW4sIGMuc291cmNlU3BhbiwgYy5zb3VyY2VTcGFuKTtcbiAgfSk7XG4gIGNvbnN0IHN3aXRjaEF0dHIgPSBuZXcgaHRtbC5BdHRyaWJ1dGUoJ1tuZ1BsdXJhbF0nLCBhc3Quc3dpdGNoVmFsdWUsIGFzdC5zd2l0Y2hWYWx1ZVNvdXJjZVNwYW4pO1xuICByZXR1cm4gbmV3IGh0bWwuRWxlbWVudChcbiAgICAgICduZy1jb250YWluZXInLCBbc3dpdGNoQXR0cl0sIGNoaWxkcmVuLCBhc3Quc291cmNlU3BhbiwgYXN0LnNvdXJjZVNwYW4sIGFzdC5zb3VyY2VTcGFuKTtcbn1cblxuLy8gSUNVIG1lc3NhZ2VzIChleGNsdWRpbmcgcGx1cmFsIGZvcm0pIGFyZSBleHBhbmRlZCB0byBgTmdTd2l0Y2hgICBhbmQgYE5nU3dpdGNoQ2FzZWBzXG5mdW5jdGlvbiBfZXhwYW5kRGVmYXVsdEZvcm0oYXN0OiBodG1sLkV4cGFuc2lvbiwgZXJyb3JzOiBQYXJzZUVycm9yW10pOiBodG1sLkVsZW1lbnQge1xuICBjb25zdCBjaGlsZHJlbiA9IGFzdC5jYXNlcy5tYXAoYyA9PiB7XG4gICAgY29uc3QgZXhwYW5zaW9uUmVzdWx0ID0gZXhwYW5kTm9kZXMoYy5leHByZXNzaW9uKTtcbiAgICBlcnJvcnMucHVzaCguLi5leHBhbnNpb25SZXN1bHQuZXJyb3JzKTtcblxuICAgIGlmIChjLnZhbHVlID09PSAnb3RoZXInKSB7XG4gICAgICAvLyBvdGhlciBpcyB0aGUgZGVmYXVsdCBjYXNlIHdoZW4gbm8gdmFsdWVzIG1hdGNoXG4gICAgICByZXR1cm4gbmV3IGh0bWwuRWxlbWVudChcbiAgICAgICAgICBgbmctdGVtcGxhdGVgLCBbbmV3IGh0bWwuQXR0cmlidXRlKCduZ1N3aXRjaERlZmF1bHQnLCAnJywgYy52YWx1ZVNvdXJjZVNwYW4pXSxcbiAgICAgICAgICBleHBhbnNpb25SZXN1bHQubm9kZXMsIGMuc291cmNlU3BhbiwgYy5zb3VyY2VTcGFuLCBjLnNvdXJjZVNwYW4pO1xuICAgIH1cblxuICAgIHJldHVybiBuZXcgaHRtbC5FbGVtZW50KFxuICAgICAgICBgbmctdGVtcGxhdGVgLCBbbmV3IGh0bWwuQXR0cmlidXRlKCduZ1N3aXRjaENhc2UnLCBgJHtjLnZhbHVlfWAsIGMudmFsdWVTb3VyY2VTcGFuKV0sXG4gICAgICAgIGV4cGFuc2lvblJlc3VsdC5ub2RlcywgYy5zb3VyY2VTcGFuLCBjLnNvdXJjZVNwYW4sIGMuc291cmNlU3Bhbik7XG4gIH0pO1xuICBjb25zdCBzd2l0Y2hBdHRyID0gbmV3IGh0bWwuQXR0cmlidXRlKCdbbmdTd2l0Y2hdJywgYXN0LnN3aXRjaFZhbHVlLCBhc3Quc3dpdGNoVmFsdWVTb3VyY2VTcGFuKTtcbiAgcmV0dXJuIG5ldyBodG1sLkVsZW1lbnQoXG4gICAgICAnbmctY29udGFpbmVyJywgW3N3aXRjaEF0dHJdLCBjaGlsZHJlbiwgYXN0LnNvdXJjZVNwYW4sIGFzdC5zb3VyY2VTcGFuLCBhc3Quc291cmNlU3Bhbik7XG59XG4iXX0=