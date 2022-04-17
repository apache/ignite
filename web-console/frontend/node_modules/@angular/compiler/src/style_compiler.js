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
        define("@angular/compiler/src/style_compiler", ["require", "exports", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/core", "@angular/compiler/src/output/output_ast", "@angular/compiler/src/shadow_css"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var core_1 = require("@angular/compiler/src/core");
    var o = require("@angular/compiler/src/output/output_ast");
    var shadow_css_1 = require("@angular/compiler/src/shadow_css");
    var COMPONENT_VARIABLE = '%COMP%';
    exports.HOST_ATTR = "_nghost-" + COMPONENT_VARIABLE;
    exports.CONTENT_ATTR = "_ngcontent-" + COMPONENT_VARIABLE;
    var StylesCompileDependency = /** @class */ (function () {
        function StylesCompileDependency(name, moduleUrl, setValue) {
            this.name = name;
            this.moduleUrl = moduleUrl;
            this.setValue = setValue;
        }
        return StylesCompileDependency;
    }());
    exports.StylesCompileDependency = StylesCompileDependency;
    var CompiledStylesheet = /** @class */ (function () {
        function CompiledStylesheet(outputCtx, stylesVar, dependencies, isShimmed, meta) {
            this.outputCtx = outputCtx;
            this.stylesVar = stylesVar;
            this.dependencies = dependencies;
            this.isShimmed = isShimmed;
            this.meta = meta;
        }
        return CompiledStylesheet;
    }());
    exports.CompiledStylesheet = CompiledStylesheet;
    var StyleCompiler = /** @class */ (function () {
        function StyleCompiler(_urlResolver) {
            this._urlResolver = _urlResolver;
            this._shadowCss = new shadow_css_1.ShadowCss();
        }
        StyleCompiler.prototype.compileComponent = function (outputCtx, comp) {
            var template = comp.template;
            return this._compileStyles(outputCtx, comp, new compile_metadata_1.CompileStylesheetMetadata({
                styles: template.styles,
                styleUrls: template.styleUrls,
                moduleUrl: compile_metadata_1.identifierModuleUrl(comp.type)
            }), this.needsStyleShim(comp), true);
        };
        StyleCompiler.prototype.compileStyles = function (outputCtx, comp, stylesheet, shim) {
            if (shim === void 0) { shim = this.needsStyleShim(comp); }
            return this._compileStyles(outputCtx, comp, stylesheet, shim, false);
        };
        StyleCompiler.prototype.needsStyleShim = function (comp) {
            return comp.template.encapsulation === core_1.ViewEncapsulation.Emulated;
        };
        StyleCompiler.prototype._compileStyles = function (outputCtx, comp, stylesheet, shim, isComponentStylesheet) {
            var _this = this;
            var styleExpressions = stylesheet.styles.map(function (plainStyle) { return o.literal(_this._shimIfNeeded(plainStyle, shim)); });
            var dependencies = [];
            stylesheet.styleUrls.forEach(function (styleUrl) {
                var exprIndex = styleExpressions.length;
                // Note: This placeholder will be filled later.
                styleExpressions.push(null);
                dependencies.push(new StylesCompileDependency(getStylesVarName(null), styleUrl, function (value) { return styleExpressions[exprIndex] = outputCtx.importExpr(value); }));
            });
            // styles variable contains plain strings and arrays of other styles arrays (recursive),
            // so we set its type to dynamic.
            var stylesVar = getStylesVarName(isComponentStylesheet ? comp : null);
            var stmt = o.variable(stylesVar)
                .set(o.literalArr(styleExpressions, new o.ArrayType(o.DYNAMIC_TYPE, [o.TypeModifier.Const])))
                .toDeclStmt(null, isComponentStylesheet ? [o.StmtModifier.Final] : [
                o.StmtModifier.Final, o.StmtModifier.Exported
            ]);
            outputCtx.statements.push(stmt);
            return new CompiledStylesheet(outputCtx, stylesVar, dependencies, shim, stylesheet);
        };
        StyleCompiler.prototype._shimIfNeeded = function (style, shim) {
            return shim ? this._shadowCss.shimCssText(style, exports.CONTENT_ATTR, exports.HOST_ATTR) : style;
        };
        return StyleCompiler;
    }());
    exports.StyleCompiler = StyleCompiler;
    function getStylesVarName(component) {
        var result = "styles";
        if (component) {
            result += "_" + compile_metadata_1.identifierName(component.type);
        }
        return result;
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGVfY29tcGlsZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvc3R5bGVfY29tcGlsZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7SUFFSCwyRUFBdUo7SUFDdkosbURBQXlDO0lBQ3pDLDJEQUF5QztJQUN6QywrREFBdUM7SUFJdkMsSUFBTSxrQkFBa0IsR0FBRyxRQUFRLENBQUM7SUFDdkIsUUFBQSxTQUFTLEdBQUcsYUFBVyxrQkFBb0IsQ0FBQztJQUM1QyxRQUFBLFlBQVksR0FBRyxnQkFBYyxrQkFBb0IsQ0FBQztJQUUvRDtRQUNFLGlDQUNXLElBQVksRUFBUyxTQUFpQixFQUFTLFFBQThCO1lBQTdFLFNBQUksR0FBSixJQUFJLENBQVE7WUFBUyxjQUFTLEdBQVQsU0FBUyxDQUFRO1lBQVMsYUFBUSxHQUFSLFFBQVEsQ0FBc0I7UUFBRyxDQUFDO1FBQzlGLDhCQUFDO0lBQUQsQ0FBQyxBQUhELElBR0M7SUFIWSwwREFBdUI7SUFLcEM7UUFDRSw0QkFDVyxTQUF3QixFQUFTLFNBQWlCLEVBQ2xELFlBQXVDLEVBQVMsU0FBa0IsRUFDbEUsSUFBK0I7WUFGL0IsY0FBUyxHQUFULFNBQVMsQ0FBZTtZQUFTLGNBQVMsR0FBVCxTQUFTLENBQVE7WUFDbEQsaUJBQVksR0FBWixZQUFZLENBQTJCO1lBQVMsY0FBUyxHQUFULFNBQVMsQ0FBUztZQUNsRSxTQUFJLEdBQUosSUFBSSxDQUEyQjtRQUFHLENBQUM7UUFDaEQseUJBQUM7SUFBRCxDQUFDLEFBTEQsSUFLQztJQUxZLGdEQUFrQjtJQU8vQjtRQUdFLHVCQUFvQixZQUF5QjtZQUF6QixpQkFBWSxHQUFaLFlBQVksQ0FBYTtZQUZyQyxlQUFVLEdBQWMsSUFBSSxzQkFBUyxFQUFFLENBQUM7UUFFQSxDQUFDO1FBRWpELHdDQUFnQixHQUFoQixVQUFpQixTQUF3QixFQUFF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T0FBTyxJQUFJLGtCQUFrQixDQUFDLFNBQVMsRUFBRSxTQUFTLEVBQUUsWUFBWSxFQUFFLElBQUksRUFBRSxVQUFVLENBQUMsQ0FBQztRQUN0RixDQUFDO1FBRU8scUNBQWEsR0FBckIsVUFBc0IsS0FBYSxFQUFFLElBQWE7WUFDaEQsT0FBTyxJQUFJLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsV0FBVyxDQUFDLEtBQUssRUFBRSxvQkFBWSxFQUFFLGlCQUFTLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDO1FBQ3BGLENBQUM7UUFDSCxvQkFBQztJQUFELENBQUMsQUExREQsSUEwREM7SUExRFksc0NBQWE7SUE0RDFCLFNBQVMsZ0JBQWdCLENBQUMsU0FBMEM7UUFDbEUsSUFBSSxNQUFNLEdBQUcsUUFBUSxDQUFDO1FBQ3RCLElBQUksU0FBUyxFQUFFO1lBQ2IsTUFBTSxJQUFJLE1BQUksaUNBQWMsQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFHLENBQUM7U0FDaEQ7UUFDRCxPQUFPLE1BQU0sQ0FBQztJQUNoQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgQ29tcGlsZUlkZW50aWZpZXJNZXRhZGF0YSwgQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YSwgaWRlbnRpZmllck1vZHVsZVVybCwgaWRlbnRpZmllck5hbWV9IGZyb20gJy4vY29tcGlsZV9tZXRhZGF0YSc7XG5pbXBvcnQge1ZpZXdFbmNhcHN1bGF0aW9ufSBmcm9tICcuL2NvcmUnO1xuaW1wb3J0ICogYXMgbyBmcm9tICcuL291dHB1dC9vdXRwdXRfYXN0JztcbmltcG9ydCB7U2hhZG93Q3NzfSBmcm9tICcuL3NoYWRvd19jc3MnO1xuaW1wb3J0IHtVcmxSZXNvbHZlcn0gZnJvbSAnLi91cmxfcmVzb2x2ZXInO1xuaW1wb3J0IHtPdXRwdXRDb250ZXh0fSBmcm9tICcuL3V0aWwnO1xuXG5jb25zdCBDT01QT05FTlRfVkFSSUFCTEUgPSAnJUNPTVAlJztcbmV4cG9ydCBjb25zdCBIT1NUX0FUVFIgPSBgX25naG9zdC0ke0NPTVBPTkVOVF9WQVJJQUJMRX1gO1xuZXhwb3J0IGNvbnN0IENPTlRFTlRfQVRUUiA9IGBfbmdjb250ZW50LSR7Q09NUE9ORU5UX1ZBUklBQkxFfWA7XG5cbmV4cG9ydCBjbGFzcyBTdHlsZXNDb21waWxlRGVwZW5kZW5jeSB7XG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIG5hbWU6IHN0cmluZywgcHVibGljIG1vZHVsZVVybDogc3RyaW5nLCBwdWJsaWMgc2V0VmFsdWU6ICh2YWx1ZTogYW55KSA9PiB2b2lkKSB7fVxufVxuXG5leHBvcnQgY2xhc3MgQ29tcGlsZWRTdHlsZXNoZWV0IHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgb3V0cHV0Q3R4OiBPdXRwdXRDb250ZXh0LCBwdWJsaWMgc3R5bGVzVmFyOiBzdHJpbmcsXG4gICAgICBwdWJsaWMgZGVwZW5kZW5jaWVzOiBTdHlsZXNDb21waWxlRGVwZW5kZW5jeVtdLCBwdWJsaWMgaXNTaGltbWVkOiBib29sZWFuLFxuICAgICAgcHVibGljIG1ldGE6IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEpIHt9XG59XG5cbmV4cG9ydCBjbGFzcyBTdHlsZUNvbXBpbGVyIHtcbiAgcHJpdmF0ZSBfc2hhZG93Q3NzOiBTaGFkb3dDc3MgPSBuZXcgU2hhZG93Q3NzKCk7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBfdXJsUmVzb2x2ZXI6IFVybFJlc29sdmVyKSB7fVxuXG4gIGNvbXBpbGVDb21wb25lbnQob3V0cHV0Q3R4OiBPdXRwdXRDb250ZXh0LCBjb21wOiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEpOiBDb21waWxlZFN0eWxlc2hlZXQge1xuICAgIGNvbnN0IHRlbXBsYXRlID0gY29tcC50ZW1wbGF0ZSAhO1xuICAgIHJldHVybiB0aGlzLl9jb21waWxlU3R5bGVzKFxuICAgICAgICBvdXRwdXRDdHgsIGNvbXAsIG5ldyBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhKHtcbiAgICAgICAgICBzdHlsZXM6IHRlbXBsYXRlLnN0eWxlcyxcbiAgICAgICAgICBzdHlsZVVybHM6IHRlbXBsYXRlLnN0eWxlVXJscyxcbiAgICAgICAgICBtb2R1bGVVcmw6IGlkZW50aWZpZXJNb2R1bGVVcmwoY29tcC50eXBlKVxuICAgICAgICB9KSxcbiAgICAgICAgdGhpcy5uZWVkc1N0eWxlU2hpbShjb21wKSwgdHJ1ZSk7XG4gIH1cblxuICBjb21waWxlU3R5bGVzKFxuICAgICAgb3V0cHV0Q3R4OiBPdXRwdXRDb250ZXh0LCBjb21wOiBDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsXG4gICAgICBzdHlsZXNoZWV0OiBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhLFxuICAgICAgc2hpbTogYm9vbGVhbiA9IHRoaXMubmVlZHNTdHlsZVNoaW0oY29tcCkpOiBDb21waWxlZFN0eWxlc2hlZXQge1xuICAgIHJldHVybiB0aGlzLl9jb21waWxlU3R5bGVzKG91dHB1dEN0eCwgY29tcCwgc3R5bGVzaGVldCwgc2hpbSwgZmFsc2UpO1xuICB9XG5cbiAgbmVlZHNTdHlsZVNoaW0oY29tcDogQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIGNvbXAudGVtcGxhdGUgIS5lbmNhcHN1bGF0aW9uID09PSBWaWV3RW5jYXBzdWxhdGlvbi5FbXVsYXRlZDtcbiAgfVxuXG4gIHByaXZhdGUgX2NvbXBpbGVTdHlsZXMoXG4gICAgICBvdXRwdXRDdHg6IE91dHB1dENvbnRleHQsIGNvbXA6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSxcbiAgICAgIHN0eWxlc2hlZXQ6IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEsIHNoaW06IGJvb2xlYW4sXG4gICAgICBpc0NvbXBvbmVudFN0eWxlc2hlZXQ6IGJvb2xlYW4pOiBDb21waWxlZFN0eWxlc2hlZXQge1xuICAgIGNvbnN0IHN0eWxlRXhwcmVzc2lvbnM6IG8uRXhwcmVzc2lvbltdID1cbiAgICAgICAgc3R5bGVzaGVldC5zdHlsZXMubWFwKHBsYWluU3R5bGUgPT4gby5saXRlcmFsKHRoaXMuX3NoaW1JZk5lZWRlZChwbGFpblN0eWxlLCBzaGltKSkpO1xuICAgIGNvbnN0IGRlcGVuZGVuY2llczogU3R5bGVzQ29tcGlsZURlcGVuZGVuY3lbXSA9IFtdO1xuICAgIHN0eWxlc2hlZXQuc3R5bGVVcmxzLmZvckVhY2goKHN0eWxlVXJsKSA9PiB7XG4gICAgICBjb25zdCBleHBySW5kZXggPSBzdHlsZUV4cHJlc3Npb25zLmxlbmd0aDtcbiAgICAgIC8vIE5vdGU6IFRoaXMgcGxhY2Vob2xkZXIgd2lsbCBiZSBmaWxsZWQgbGF0ZXIuXG4gICAgICBzdHlsZUV4cHJlc3Npb25zLnB1c2gobnVsbCAhKTtcbiAgICAgIGRlcGVuZGVuY2llcy5wdXNoKG5ldyBTdHlsZXNDb21waWxlRGVwZW5kZW5jeShcbiAgICAgICAgICBnZXRTdHlsZXNWYXJOYW1lKG51bGwpLCBzdHlsZVVybCxcbiAgICAgICAgICAodmFsdWUpID0+IHN0eWxlRXhwcmVzc2lvbnNbZXhwckluZGV4XSA9IG91dHB1dEN0eC5pbXBvcnRFeHByKHZhbHVlKSkpO1xuICAgIH0pO1xuICAgIC8vIHN0eWxlcyB2YXJpYWJsZSBjb250YWlucyBwbGFpbiBzdHJpbmdzIGFuZCBhcnJheXMgb2Ygb3RoZXIgc3R5bGVzIGFycmF5cyAocmVjdXJzaXZlKSxcbiAgICAvLyBzbyB3ZSBzZXQgaXRzIHR5cGUgdG8gZHluYW1pYy5cbiAgICBjb25zdCBzdHlsZXNWYXIgPSBnZXRTdHlsZXNWYXJOYW1lKGlzQ29tcG9uZW50U3R5bGVzaGVldCA/IGNvbXAgOiBudWxsKTtcbiAgICBjb25zdCBzdG10ID0gby52YXJpYWJsZShzdHlsZXNWYXIpXG4gICAgICAgICAgICAgICAgICAgICAuc2V0KG8ubGl0ZXJhbEFycihcbiAgICAgICAgICAgICAgICAgICAgICAgICBzdHlsZUV4cHJlc3Npb25zLCBuZXcgby5BcnJheVR5cGUoby5EWU5BTUlDX1RZUEUsIFtvLlR5cGVNb2RpZmllci5Db25zdF0pKSlcbiAgICAgICAgICAgICAgICAgICAgIC50b0RlY2xTdG10KG51bGwsIGlzQ29tcG9uZW50U3R5bGVzaGVldCA/IFtvLlN0bXRNb2RpZmllci5GaW5hbF0gOiBbXG4gICAgICAgICAgICAgICAgICAgICAgIG8uU3RtdE1vZGlmaWVyLkZpbmFsLCBvLlN0bXRNb2RpZmllci5FeHBvcnRlZFxuICAgICAgICAgICAgICAgICAgICAgXSk7XG4gICAgb3V0cHV0Q3R4LnN0YXRlbWVudHMucHVzaChzdG10KTtcbiAgICByZXR1cm4gbmV3IENvbXBpbGVkU3R5bGVzaGVldChvdXRwdXRDdHgsIHN0eWxlc1ZhciwgZGVwZW5kZW5jaWVzLCBzaGltLCBzdHlsZXNoZWV0KTtcbiAgfVxuXG4gIHByaXZhdGUgX3NoaW1JZk5lZWRlZChzdHlsZTogc3RyaW5nLCBzaGltOiBib29sZWFuKTogc3RyaW5nIHtcbiAgICByZXR1cm4gc2hpbSA/IHRoaXMuX3NoYWRvd0Nzcy5zaGltQ3NzVGV4dChzdHlsZSwgQ09OVEVOVF9BVFRSLCBIT1NUX0FUVFIpIDogc3R5bGU7XG4gIH1cbn1cblxuZnVuY3Rpb24gZ2V0U3R5bGVzVmFyTmFtZShjb21wb25lbnQ6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSB8IG51bGwpOiBzdHJpbmcge1xuICBsZXQgcmVzdWx0ID0gYHN0eWxlc2A7XG4gIGlmIChjb21wb25lbnQpIHtcbiAgICByZXN1bHQgKz0gYF8ke2lkZW50aWZpZXJOYW1lKGNvbXBvbmVudC50eXBlKX1gO1xuICB9XG4gIHJldHVybiByZXN1bHQ7XG59XG4iXX0=