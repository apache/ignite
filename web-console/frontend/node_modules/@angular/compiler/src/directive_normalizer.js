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
        define("@angular/compiler/src/directive_normalizer", ["require", "exports", "tslib", "@angular/compiler/src/compile_metadata", "@angular/compiler/src/config", "@angular/compiler/src/core", "@angular/compiler/src/ml_parser/ast", "@angular/compiler/src/ml_parser/interpolation_config", "@angular/compiler/src/style_url_resolver", "@angular/compiler/src/template_parser/template_preparser", "@angular/compiler/src/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var compile_metadata_1 = require("@angular/compiler/src/compile_metadata");
    var config_1 = require("@angular/compiler/src/config");
    var core_1 = require("@angular/compiler/src/core");
    var html = require("@angular/compiler/src/ml_parser/ast");
    var interpolation_config_1 = require("@angular/compiler/src/ml_parser/interpolation_config");
    var style_url_resolver_1 = require("@angular/compiler/src/style_url_resolver");
    var template_preparser_1 = require("@angular/compiler/src/template_parser/template_preparser");
    var util_1 = require("@angular/compiler/src/util");
    var DirectiveNormalizer = /** @class */ (function () {
        function DirectiveNormalizer(_resourceLoader, _urlResolver, _htmlParser, _config) {
            this._resourceLoader = _resourceLoader;
            this._urlResolver = _urlResolver;
            this._htmlParser = _htmlParser;
            this._config = _config;
            this._resourceLoaderCache = new Map();
        }
        DirectiveNormalizer.prototype.clearCache = function () { this._resourceLoaderCache.clear(); };
        DirectiveNormalizer.prototype.clearCacheFor = function (normalizedDirective) {
            var _this = this;
            if (!normalizedDirective.isComponent) {
                return;
            }
            var template = normalizedDirective.template;
            this._resourceLoaderCache.delete(template.templateUrl);
            template.externalStylesheets.forEach(function (stylesheet) { _this._resourceLoaderCache.delete(stylesheet.moduleUrl); });
        };
        DirectiveNormalizer.prototype._fetch = function (url) {
            var result = this._resourceLoaderCache.get(url);
            if (!result) {
                result = this._resourceLoader.get(url);
                this._resourceLoaderCache.set(url, result);
            }
            return result;
        };
        DirectiveNormalizer.prototype.normalizeTemplate = function (prenormData) {
            var _this = this;
            if (util_1.isDefined(prenormData.template)) {
                if (util_1.isDefined(prenormData.templateUrl)) {
                    throw util_1.syntaxError("'" + util_1.stringify(prenormData.componentType) + "' component cannot define both template and templateUrl");
                }
                if (typeof prenormData.template !== 'string') {
                    throw util_1.syntaxError("The template specified for component " + util_1.stringify(prenormData.componentType) + " is not a string");
                }
            }
            else if (util_1.isDefined(prenormData.templateUrl)) {
                if (typeof prenormData.templateUrl !== 'string') {
                    throw util_1.syntaxError("The templateUrl specified for component " + util_1.stringify(prenormData.componentType) + " is not a string");
                }
            }
            else {
                throw util_1.syntaxError("No template specified for component " + util_1.stringify(prenormData.componentType));
            }
            if (util_1.isDefined(prenormData.preserveWhitespaces) &&
                typeof prenormData.preserveWhitespaces !== 'boolean') {
                throw util_1.syntaxError("The preserveWhitespaces option for component " + util_1.stringify(prenormData.componentType) + " must be a boolean");
            }
            return util_1.SyncAsync.then(this._preParseTemplate(prenormData), function (preparsedTemplate) { return _this._normalizeTemplateMetadata(prenormData, preparsedTemplate); });
        };
        DirectiveNormalizer.prototype._preParseTemplate = function (prenomData) {
            var _this = this;
            var template;
            var templateUrl;
            if (prenomData.template != null) {
                template = prenomData.template;
                templateUrl = prenomData.moduleUrl;
            }
            else {
                templateUrl = this._urlResolver.resolve(prenomData.moduleUrl, prenomData.templateUrl);
                template = this._fetch(templateUrl);
            }
            return util_1.SyncAsync.then(template, function (template) { return _this._preparseLoadedTemplate(prenomData, template, templateUrl); });
        };
        DirectiveNormalizer.prototype._preparseLoadedTemplate = function (prenormData, template, templateAbsUrl) {
            var isInline = !!prenormData.template;
            var interpolationConfig = interpolation_config_1.InterpolationConfig.fromArray(prenormData.interpolation);
            var templateUrl = compile_metadata_1.templateSourceUrl({ reference: prenormData.ngModuleType }, { type: { reference: prenormData.componentType } }, { isInline: isInline, templateUrl: templateAbsUrl });
            var rootNodesAndErrors = this._htmlParser.parse(template, templateUrl, { tokenizeExpansionForms: true, interpolationConfig: interpolationConfig });
            if (rootNodesAndErrors.errors.length > 0) {
                var errorString = rootNodesAndErrors.errors.join('\n');
                throw util_1.syntaxError("Template parse errors:\n" + errorString);
            }
            var templateMetadataStyles = this._normalizeStylesheet(new compile_metadata_1.CompileStylesheetMetadata({ styles: prenormData.styles, moduleUrl: prenormData.moduleUrl }));
            var visitor = new TemplatePreparseVisitor();
            html.visitAll(visitor, rootNodesAndErrors.rootNodes);
            var templateStyles = this._normalizeStylesheet(new compile_metadata_1.CompileStylesheetMetadata({ styles: visitor.styles, styleUrls: visitor.styleUrls, moduleUrl: templateAbsUrl }));
            var styles = templateMetadataStyles.styles.concat(templateStyles.styles);
            var inlineStyleUrls = templateMetadataStyles.styleUrls.concat(templateStyles.styleUrls);
            var styleUrls = this
                ._normalizeStylesheet(new compile_metadata_1.CompileStylesheetMetadata({ styleUrls: prenormData.styleUrls, moduleUrl: prenormData.moduleUrl }))
                .styleUrls;
            return {
                template: template,
                templateUrl: templateAbsUrl, isInline: isInline,
                htmlAst: rootNodesAndErrors, styles: styles, inlineStyleUrls: inlineStyleUrls, styleUrls: styleUrls,
                ngContentSelectors: visitor.ngContentSelectors,
            };
        };
        DirectiveNormalizer.prototype._normalizeTemplateMetadata = function (prenormData, preparsedTemplate) {
            var _this = this;
            return util_1.SyncAsync.then(this._loadMissingExternalStylesheets(preparsedTemplate.styleUrls.concat(preparsedTemplate.inlineStyleUrls)), function (externalStylesheets) { return _this._normalizeLoadedTemplateMetadata(prenormData, preparsedTemplate, externalStylesheets); });
        };
        DirectiveNormalizer.prototype._normalizeLoadedTemplateMetadata = function (prenormData, preparsedTemplate, stylesheets) {
            // Algorithm:
            // - produce exactly 1 entry per original styleUrl in
            // CompileTemplateMetadata.externalStylesheets with all styles inlined
            // - inline all styles that are referenced by the template into CompileTemplateMetadata.styles.
            // Reason: be able to determine how many stylesheets there are even without loading
            // the template nor the stylesheets, so we can create a stub for TypeScript always synchronously
            // (as resource loading may be async)
            var _this = this;
            var styles = tslib_1.__spread(preparsedTemplate.styles);
            this._inlineStyles(preparsedTemplate.inlineStyleUrls, stylesheets, styles);
            var styleUrls = preparsedTemplate.styleUrls;
            var externalStylesheets = styleUrls.map(function (styleUrl) {
                var stylesheet = stylesheets.get(styleUrl);
                var styles = tslib_1.__spread(stylesheet.styles);
                _this._inlineStyles(stylesheet.styleUrls, stylesheets, styles);
                return new compile_metadata_1.CompileStylesheetMetadata({ moduleUrl: styleUrl, styles: styles });
            });
            var encapsulation = prenormData.encapsulation;
            if (encapsulation == null) {
                encapsulation = this._config.defaultEncapsulation;
            }
            if (encapsulation === core_1.ViewEncapsulation.Emulated && styles.length === 0 &&
                styleUrls.length === 0) {
                encapsulation = core_1.ViewEncapsulation.None;
            }
            return new compile_metadata_1.CompileTemplateMetadata({
                encapsulation: encapsulation,
                template: preparsedTemplate.template,
                templateUrl: preparsedTemplate.templateUrl,
                htmlAst: preparsedTemplate.htmlAst, styles: styles, styleUrls: styleUrls,
                ngContentSelectors: preparsedTemplate.ngContentSelectors,
                animations: prenormData.animations,
                interpolation: prenormData.interpolation,
                isInline: preparsedTemplate.isInline, externalStylesheets: externalStylesheets,
                preserveWhitespaces: config_1.preserveWhitespacesDefault(prenormData.preserveWhitespaces, this._config.preserveWhitespaces),
            });
        };
        DirectiveNormalizer.prototype._inlineStyles = function (styleUrls, stylesheets, targetStyles) {
            var _this = this;
            styleUrls.forEach(function (styleUrl) {
                var stylesheet = stylesheets.get(styleUrl);
                stylesheet.styles.forEach(function (style) { return targetStyles.push(style); });
                _this._inlineStyles(stylesheet.styleUrls, stylesheets, targetStyles);
            });
        };
        DirectiveNormalizer.prototype._loadMissingExternalStylesheets = function (styleUrls, loadedStylesheets) {
            var _this = this;
            if (loadedStylesheets === void 0) { loadedStylesheets = new Map(); }
            return util_1.SyncAsync.then(util_1.SyncAsync.all(styleUrls.filter(function (styleUrl) { return !loadedStylesheets.has(styleUrl); })
                .map(function (styleUrl) { return util_1.SyncAsync.then(_this._fetch(styleUrl), function (loadedStyle) {
                var stylesheet = _this._normalizeStylesheet(new compile_metadata_1.CompileStylesheetMetadata({ styles: [loadedStyle], moduleUrl: styleUrl }));
                loadedStylesheets.set(styleUrl, stylesheet);
                return _this._loadMissingExternalStylesheets(stylesheet.styleUrls, loadedStylesheets);
            }); })), function (_) { return loadedStylesheets; });
        };
        DirectiveNormalizer.prototype._normalizeStylesheet = function (stylesheet) {
            var _this = this;
            var moduleUrl = stylesheet.moduleUrl;
            var allStyleUrls = stylesheet.styleUrls.filter(style_url_resolver_1.isStyleUrlResolvable)
                .map(function (url) { return _this._urlResolver.resolve(moduleUrl, url); });
            var allStyles = stylesheet.styles.map(function (style) {
                var styleWithImports = style_url_resolver_1.extractStyleUrls(_this._urlResolver, moduleUrl, style);
                allStyleUrls.push.apply(allStyleUrls, tslib_1.__spread(styleWithImports.styleUrls));
                return styleWithImports.style;
            });
            return new compile_metadata_1.CompileStylesheetMetadata({ styles: allStyles, styleUrls: allStyleUrls, moduleUrl: moduleUrl });
        };
        return DirectiveNormalizer;
    }());
    exports.DirectiveNormalizer = DirectiveNormalizer;
    var TemplatePreparseVisitor = /** @class */ (function () {
        function TemplatePreparseVisitor() {
            this.ngContentSelectors = [];
            this.styles = [];
            this.styleUrls = [];
            this.ngNonBindableStackCount = 0;
        }
        TemplatePreparseVisitor.prototype.visitElement = function (ast, context) {
            var preparsedElement = template_preparser_1.preparseElement(ast);
            switch (preparsedElement.type) {
                case template_preparser_1.PreparsedElementType.NG_CONTENT:
                    if (this.ngNonBindableStackCount === 0) {
                        this.ngContentSelectors.push(preparsedElement.selectAttr);
                    }
                    break;
                case template_preparser_1.PreparsedElementType.STYLE:
                    var textContent_1 = '';
                    ast.children.forEach(function (child) {
                        if (child instanceof html.Text) {
                            textContent_1 += child.value;
                        }
                    });
                    this.styles.push(textContent_1);
                    break;
                case template_preparser_1.PreparsedElementType.STYLESHEET:
                    this.styleUrls.push(preparsedElement.hrefAttr);
                    break;
                default:
                    break;
            }
            if (preparsedElement.nonBindable) {
                this.ngNonBindableStackCount++;
            }
            html.visitAll(this, ast.children);
            if (preparsedElement.nonBindable) {
                this.ngNonBindableStackCount--;
            }
            return null;
        };
        TemplatePreparseVisitor.prototype.visitExpansion = function (ast, context) { html.visitAll(this, ast.cases); };
        TemplatePreparseVisitor.prototype.visitExpansionCase = function (ast, context) {
            html.visitAll(this, ast.expression);
        };
        TemplatePreparseVisitor.prototype.visitComment = function (ast, context) { return null; };
        TemplatePreparseVisitor.prototype.visitAttribute = function (ast, context) { return null; };
        TemplatePreparseVisitor.prototype.visitText = function (ast, context) { return null; };
        return TemplatePreparseVisitor;
    }());
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlyZWN0aXZlX25vcm1hbGl6ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvZGlyZWN0aXZlX25vcm1hbGl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBRUgsMkVBQW1JO0lBQ25JLHVEQUFvRTtJQUNwRSxtREFBeUM7SUFDekMsMERBQXdDO0lBRXhDLDZGQUFxRTtJQUdyRSwrRUFBNEU7SUFDNUUsK0ZBQTJGO0lBRTNGLG1EQUFvRTtJQWdCcEU7UUFHRSw2QkFDWSxlQUErQixFQUFVLFlBQXlCLEVBQ2xFLFdBQXVCLEVBQVUsT0FBdUI7WUFEeEQsb0JBQWUsR0FBZixlQUFlLENBQWdCO1lBQVUsaUJBQVksR0FBWixZQUFZLENBQWE7WUFDbEUsZ0JBQVcsR0FBWCxXQUFXLENBQVk7WUFBVSxZQUFPLEdBQVAsT0FBTyxDQUFnQjtZQUo1RCx5QkFBb0IsR0FBRyxJQUFJLEdBQUcsRUFBNkIsQ0FBQztRQUlHLENBQUM7UUFFeEUsd0NBQVUsR0FBVixjQUFxQixJQUFJLENBQUMsb0JBQW9CLENBQUMsS0FBSyxFQUFFLENBQUMsQ0FBQyxDQUFDO1FBRXpELDJDQUFhLEdBQWIsVUFBYyxtQkFBNkM7WUFBM0QsaUJBUUM7WUFQQyxJQUFJLENBQUMsbUJBQW1CLENBQUMsV0FBVyxFQUFFO2dCQUNwQyxPQUFPO2FBQ1I7WUFDRCxJQUFNLFFBQVEsR0FBRyxtQkFBbUIsQ0FBQyxRQUFVLENBQUM7WUFDaEQsSUFBSSxDQUFDLG9CQUFvQixDQUFDLE1BQU0sQ0FBQyxRQUFRLENBQUMsV0FBYSxDQUFDLENBQUM7WUFDekQsUUFBUSxDQUFDLG1CQUFtQixDQUFDLE9BQU8sQ0FDaEMsVUFBQyxVQUFVLElBQU8sS0FBSSxDQUFDLG9CQUFvQixDQUFDLE1BQU0sQ0FBQyxVQUFVLENBQUMsU0FBVyxDQUFDLENBQUMsQ0FBQyxDQUFDLENB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QUFFLE9BQVksSUFBUyxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7UUFDdkUsMkNBQVMsR0FBVCxVQUFVLEdBQWMsRUFBRSxPQUFZLElBQVMsT0FBTyxJQUFJLENBQUMsQ0FBQyxDQUFDO1FBQy9ELDhCQUFDO0lBQUQsQ0FBQyxBQWhERCxJQWdEQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlRGlyZWN0aXZlTWV0YWRhdGEsIENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEsIENvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhLCB0ZW1wbGF0ZVNvdXJjZVVybH0gZnJvbSAnLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCB7Q29tcGlsZXJDb25maWcsIHByZXNlcnZlV2hpdGVzcGFjZXNEZWZhdWx0fSBmcm9tICcuL2NvbmZpZyc7XG5pbXBvcnQge1ZpZXdFbmNhcHN1bGF0aW9ufSBmcm9tICcuL2NvcmUnO1xuaW1wb3J0ICogYXMgaHRtbCBmcm9tICcuL21sX3BhcnNlci9hc3QnO1xuaW1wb3J0IHtIdG1sUGFyc2VyfSBmcm9tICcuL21sX3BhcnNlci9odG1sX3BhcnNlcic7XG5pbXBvcnQge0ludGVycG9sYXRpb25Db25maWd9IGZyb20gJy4vbWxfcGFyc2VyL2ludGVycG9sYXRpb25fY29uZmlnJztcbmltcG9ydCB7UGFyc2VUcmVlUmVzdWx0IGFzIEh0bWxQYXJzZVRyZWVSZXN1bHR9IGZyb20gJy4vbWxfcGFyc2VyL3BhcnNlcic7XG5pbXBvcnQge1Jlc291cmNlTG9hZGVyfSBmcm9tICcuL3Jlc291cmNlX2xvYWRlcic7XG5pbXBvcnQge2V4dHJhY3RTdHlsZVVybHMsIGlzU3R5bGVVcmxSZXNvbHZhYmxlfSBmcm9tICcuL3N0eWxlX3VybF9yZXNvbHZlcic7XG5pbXBvcnQge1ByZXBhcnNlZEVsZW1lbnRUeXBlLCBwcmVwYXJzZUVsZW1lbnR9IGZyb20gJy4vdGVtcGxhdGVfcGFyc2VyL3RlbXBsYXRlX3ByZXBhcnNlcic7XG5pbXBvcnQge1VybFJlc29sdmVyfSBmcm9tICcuL3VybF9yZXNvbHZlcic7XG5pbXBvcnQge1N5bmNBc3luYywgaXNEZWZpbmVkLCBzdHJpbmdpZnksIHN5bnRheEVycm9yfSBmcm9tICcuL3V0aWwnO1xuXG5leHBvcnQgaW50ZXJmYWNlIFByZW5vcm1hbGl6ZWRUZW1wbGF0ZU1ldGFkYXRhIHtcbiAgbmdNb2R1bGVUeXBlOiBhbnk7XG4gIGNvbXBvbmVudFR5cGU6IGFueTtcbiAgbW9kdWxlVXJsOiBzdHJpbmc7XG4gIHRlbXBsYXRlOiBzdHJpbmd8bnVsbDtcbiAgdGVtcGxhdGVVcmw6IHN0cmluZ3xudWxsO1xuICBzdHlsZXM6IHN0cmluZ1tdO1xuICBzdHlsZVVybHM6IHN0cmluZ1tdO1xuICBpbnRlcnBvbGF0aW9uOiBbc3RyaW5nLCBzdHJpbmddfG51bGw7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9ufG51bGw7XG4gIGFuaW1hdGlvbnM6IGFueVtdO1xuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBib29sZWFufG51bGw7XG59XG5cbmV4cG9ydCBjbGFzcyBEaXJlY3RpdmVOb3JtYWxpemVyIHtcbiAgcHJpdmF0ZSBfcmVzb3VyY2VMb2FkZXJDYWNoZSA9IG5ldyBNYXA8c3RyaW5nLCBTeW5jQXN5bmM8c3RyaW5nPj4oKTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHByaXZhdGUgX3Jlc291cmNlTG9hZGVyOiBSZXNvdXJjZUxvYWRlciwgcHJpdmF0ZSBfdXJsUmVzb2x2ZXI6IFVybFJlc29sdmVyLFxuICAgICAgcHJpdmF0ZSBfaHRtbFBhcnNlcjogSHRtbFBhcnNlciwgcHJpdmF0ZSBfY29uZmlnOiBDb21waWxlckNvbmZpZykge31cblxuICBjbGVhckNhY2hlKCk6IHZvaWQgeyB0aGlzLl9yZXNvdXJjZUxvYWRlckNhY2hlLmNsZWFyKCk7IH1cblxuICBjbGVhckNhY2hlRm9yKG5vcm1hbGl6ZWREaXJlY3RpdmU6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSk6IHZvaWQge1xuICAgIGlmICghbm9ybWFsaXplZERpcmVjdGl2ZS5pc0NvbXBvbmVudCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCB0ZW1wbGF0ZSA9IG5vcm1hbGl6ZWREaXJlY3RpdmUudGVtcGxhdGUgITtcbiAgICB0aGlzLl9yZXNvdXJjZUxvYWRlckNhY2hlLmRlbGV0ZSh0ZW1wbGF0ZS50ZW1wbGF0ZVVybCAhKTtcbiAgICB0ZW1wbGF0ZS5leHRlcm5hbFN0eWxlc2hlZXRzLmZvckVhY2goXG4gICAgICAgIChzdHlsZXNoZWV0KSA9PiB7IHRoaXMuX3Jlc291cmNlTG9hZGVyQ2FjaGUuZGVsZXRlKHN0eWxlc2hlZXQubW9kdWxlVXJsICEpOyB9KTtcbiAgfVxuXG4gIHByaXZhdGUgX2ZldGNoKHVybDogc3RyaW5nKTogU3luY0FzeW5jPHN0cmluZz4ge1xuICAgIGxldCByZXN1bHQgPSB0aGlzLl9yZXNvdXJjZUxvYWRlckNhY2hlLmdldCh1cmwpO1xuICAgIGlmICghcmVzdWx0KSB7XG4gICAgICByZXN1bHQgPSB0aGlzLl9yZXNvdXJjZUxvYWRlci5nZXQodXJsKTtcbiAgICAgIHRoaXMuX3Jlc291cmNlTG9hZGVyQ2FjaGUuc2V0KHVybCwgcmVzdWx0KTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIG5vcm1hbGl6ZVRlbXBsYXRlKHByZW5vcm1EYXRhOiBQcmVub3JtYWxpemVkVGVtcGxhdGVNZXRhZGF0YSk6XG4gICAgICBTeW5jQXN5bmM8Q29tcGlsZVRlbXBsYXRlTWV0YWRhdGE+IHtcbiAgICBpZiAoaXNEZWZpbmVkKHByZW5vcm1EYXRhLnRlbXBsYXRlKSkge1xuICAgICAgaWYgKGlzRGVmaW5lZChwcmVub3JtRGF0YS50ZW1wbGF0ZVVybCkpIHtcbiAgICAgICAgdGhyb3cgc3ludGF4RXJyb3IoXG4gICAgICAgICAgICBgJyR7c3RyaW5naWZ5KHByZW5vcm1EYXRhLmNvbXBvbmVudFR5cGUpfScgY29tcG9uZW50IGNhbm5vdCBkZWZpbmUgYm90aCB0ZW1wbGF0ZSBhbmQgdGVtcGxhdGVVcmxgKTtcbiAgICAgIH1cbiAgICAgIGlmICh0eXBlb2YgcHJlbm9ybURhdGEudGVtcGxhdGUgIT09ICdzdHJpbmcnKSB7XG4gICAgICAgIHRocm93IHN5bnRheEVycm9yKFxuICAgICAgICAgICAgYFRoZSB0ZW1wbGF0ZSBzcGVjaWZpZWQgZm9yIGNvbXBvbmVudCAke3N0cmluZ2lmeShwcmVub3JtRGF0YS5jb21wb25lbnRUeXBlKX0gaXMgbm90IGEgc3RyaW5nYCk7XG4gICAgICB9XG4gICAgfSBlbHNlIGlmIChpc0RlZmluZWQocHJlbm9ybURhdGEudGVtcGxhdGVVcmwpKSB7XG4gICAgICBpZiAodHlwZW9mIHByZW5vcm1EYXRhLnRlbXBsYXRlVXJsICE9PSAnc3RyaW5nJykge1xuICAgICAgICB0aHJvdyBzeW50YXhFcnJvcihcbiAgICAgICAgICAgIGBUaGUgdGVtcGxhdGVVcmwgc3BlY2lmaWVkIGZvciBjb21wb25lbnQgJHtzdHJpbmdpZnkocHJlbm9ybURhdGEuY29tcG9uZW50VHlwZSl9IGlzIG5vdCBhIHN0cmluZ2ApO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICB0aHJvdyBzeW50YXhFcnJvcihcbiAgICAgICAgICBgTm8gdGVtcGxhdGUgc3BlY2lmaWVkIGZvciBjb21wb25lbnQgJHtzdHJpbmdpZnkocHJlbm9ybURhdGEuY29tcG9uZW50VHlwZSl9YCk7XG4gICAgfVxuXG4gICAgaWYgKGlzRGVmaW5lZChwcmVub3JtRGF0YS5wcmVzZXJ2ZVdoaXRlc3BhY2VzKSAmJlxuICAgICAgICB0eXBlb2YgcHJlbm9ybURhdGEucHJlc2VydmVXaGl0ZXNwYWNlcyAhPT0gJ2Jvb2xlYW4nKSB7XG4gICAgICB0aHJvdyBzeW50YXhFcnJvcihcbiAgICAgICAgICBgVGhlIHByZXNlcnZlV2hpdGVzcGFjZXMgb3B0aW9uIGZvciBjb21wb25lbnQgJHtzdHJpbmdpZnkocHJlbm9ybURhdGEuY29tcG9uZW50VHlwZSl9IG11c3QgYmUgYSBib29sZWFuYCk7XG4gICAgfVxuXG4gICAgcmV0dXJuIFN5bmNBc3luYy50aGVuKFxuICAgICAgICB0aGlzLl9wcmVQYXJzZVRlbXBsYXRlKHByZW5vcm1EYXRhKSxcbiAgICAgICAgKHByZXBhcnNlZFRlbXBsYXRlKSA9PiB0aGlzLl9ub3JtYWxpemVUZW1wbGF0ZU1ldGFkYXRhKHByZW5vcm1EYXRhLCBwcmVwYXJzZWRUZW1wbGF0ZSkpO1xuICB9XG5cbiAgcHJpdmF0ZSBfcHJlUGFyc2VUZW1wbGF0ZShwcmVub21EYXRhOiBQcmVub3JtYWxpemVkVGVtcGxhdGVNZXRhZGF0YSk6XG4gICAgICBTeW5jQXN5bmM8UHJlcGFyc2VkVGVtcGxhdGU+IHtcbiAgICBsZXQgdGVtcGxhdGU6IFN5bmNBc3luYzxzdHJpbmc+O1xuICAgIGxldCB0ZW1wbGF0ZVVybDogc3RyaW5nO1xuICAgIGlmIChwcmVub21EYXRhLnRlbXBsYXRlICE9IG51bGwpIHtcbiAgICAgIHRlbXBsYXRlID0gcHJlbm9tRGF0YS50ZW1wbGF0ZTtcbiAgICAgIHRlbXBsYXRlVXJsID0gcHJlbm9tRGF0YS5tb2R1bGVVcmw7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRlbXBsYXRlVXJsID0gdGhpcy5fdXJsUmVzb2x2ZXIucmVzb2x2ZShwcmVub21EYXRhLm1vZHVsZVVybCwgcHJlbm9tRGF0YS50ZW1wbGF0ZVVybCAhKTtcbiAgICAgIHRlbXBsYXRlID0gdGhpcy5fZmV0Y2godGVtcGxhdGVVcmwpO1xuICAgIH1cbiAgICByZXR1cm4gU3luY0FzeW5jLnRoZW4oXG4gICAgICAgIHRlbXBsYXRlLCAodGVtcGxhdGUpID0+IHRoaXMuX3ByZXBhcnNlTG9hZGVkVGVtcGxhdGUocHJlbm9tRGF0YSwgdGVtcGxhdGUsIHRlbXBsYXRlVXJsKSk7XG4gIH1cblxuICBwcml2YXRlIF9wcmVwYXJzZUxvYWRlZFRlbXBsYXRlKFxuICAgICAgcHJlbm9ybURhdGE6IFByZW5vcm1hbGl6ZWRUZW1wbGF0ZU1ldGFkYXRhLCB0ZW1wbGF0ZTogc3RyaW5nLFxuICAgICAgdGVtcGxhdGVBYnNVcmw6IHN0cmluZyk6IFByZXBhcnNlZFRlbXBsYXRlIHtcbiAgICBjb25zdCBpc0lubGluZSA9ICEhcHJlbm9ybURhdGEudGVtcGxhdGU7XG4gICAgY29uc3QgaW50ZXJwb2xhdGlvbkNvbmZpZyA9IEludGVycG9sYXRpb25Db25maWcuZnJvbUFycmF5KHByZW5vcm1EYXRhLmludGVycG9sYXRpb24gISk7XG4gICAgY29uc3QgdGVtcGxhdGVVcmwgPSB0ZW1wbGF0ZVNvdXJjZVVybChcbiAgICAgICAge3JlZmVyZW5jZTogcHJlbm9ybURhdGEubmdNb2R1bGVUeXBlfSwge3R5cGU6IHtyZWZlcmVuY2U6IHByZW5vcm1EYXRhLmNvbXBvbmVudFR5cGV9fSxcbiAgICAgICAge2lzSW5saW5lLCB0ZW1wbGF0ZVVybDogdGVtcGxhdGVBYnNVcmx9KTtcbiAgICBjb25zdCByb290Tm9kZXNBbmRFcnJvcnMgPSB0aGlzLl9odG1sUGFyc2VyLnBhcnNlKFxuICAgICAgICB0ZW1wbGF0ZSwgdGVtcGxhdGVVcmwsIHt0b2tlbml6ZUV4cGFuc2lvbkZvcm1zOiB0cnVlLCBpbnRlcnBvbGF0aW9uQ29uZmlnfSk7XG4gICAgaWYgKHJvb3ROb2Rlc0FuZEVycm9ycy5lcnJvcnMubGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgZXJyb3JTdHJpbmcgPSByb290Tm9kZXNBbmRFcnJvcnMuZXJyb3JzLmpvaW4oJ1xcbicpO1xuICAgICAgdGhyb3cgc3ludGF4RXJyb3IoYFRlbXBsYXRlIHBhcnNlIGVycm9yczpcXG4ke2Vycm9yU3RyaW5nfWApO1xuICAgIH1cblxuICAgIGNvbnN0IHRlbXBsYXRlTWV0YWRhdGFTdHlsZXMgPSB0aGlzLl9ub3JtYWxpemVTdHlsZXNoZWV0KG5ldyBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhKFxuICAgICAgICB7c3R5bGVzOiBwcmVub3JtRGF0YS5zdHlsZXMsIG1vZHVsZVVybDogcHJlbm9ybURhdGEubW9kdWxlVXJsfSkpO1xuXG4gICAgY29uc3QgdmlzaXRvciA9IG5ldyBUZW1wbGF0ZVByZXBhcnNlVmlzaXRvcigpO1xuICAgIGh0bWwudmlzaXRBbGwodmlzaXRvciwgcm9vdE5vZGVzQW5kRXJyb3JzLnJvb3ROb2Rlcyk7XG4gICAgY29uc3QgdGVtcGxhdGVTdHlsZXMgPSB0aGlzLl9ub3JtYWxpemVTdHlsZXNoZWV0KG5ldyBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhKFxuICAgICAgICB7c3R5bGVzOiB2aXNpdG9yLnN0eWxlcywgc3R5bGVVcmxzOiB2aXNpdG9yLnN0eWxlVXJscywgbW9kdWxlVXJsOiB0ZW1wbGF0ZUFic1VybH0pKTtcblxuICAgIGNvbnN0IHN0eWxlcyA9IHRlbXBsYXRlTWV0YWRhdGFTdHlsZXMuc3R5bGVzLmNvbmNhdCh0ZW1wbGF0ZVN0eWxlcy5zdHlsZXMpO1xuXG4gICAgY29uc3QgaW5saW5lU3R5bGVVcmxzID0gdGVtcGxhdGVNZXRhZGF0YVN0eWxlcy5zdHlsZVVybHMuY29uY2F0KHRlbXBsYXRlU3R5bGVzLnN0eWxlVXJscyk7XG4gICAgY29uc3Qgc3R5bGVVcmxzID0gdGhpc1xuICAgICAgICAgICAgICAgICAgICAgICAgICAuX25vcm1hbGl6ZVN0eWxlc2hlZXQobmV3IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEoXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICB7c3R5bGVVcmxzOiBwcmVub3JtRGF0YS5zdHlsZVVybHMsIG1vZHVsZVVybDogcHJlbm9ybURhdGEubW9kdWxlVXJsfSkpXG4gICAgICAgICAgICAgICAgICAgICAgICAgIC5zdHlsZVVybHM7XG4gICAgcmV0dXJuIHtcbiAgICAgIHRlbXBsYXRlLFxuICAgICAgdGVtcGxhdGVVcmw6IHRlbXBsYXRlQWJzVXJsLCBpc0lubGluZSxcbiAgICAgIGh0bWxBc3Q6IHJvb3ROb2Rlc0FuZEVycm9ycywgc3R5bGVzLCBpbmxpbmVTdHlsZVVybHMsIHN0eWxlVXJscyxcbiAgICAgIG5nQ29udGVudFNlbGVjdG9yczogdmlzaXRvci5uZ0NvbnRlbnRTZWxlY3RvcnMsXG4gICAgfTtcbiAgfVxuXG4gIHByaXZhdGUgX25vcm1hbGl6ZVRlbXBsYXRlTWV0YWRhdGEoXG4gICAgICBwcmVub3JtRGF0YTogUHJlbm9ybWFsaXplZFRlbXBsYXRlTWV0YWRhdGEsXG4gICAgICBwcmVwYXJzZWRUZW1wbGF0ZTogUHJlcGFyc2VkVGVtcGxhdGUpOiBTeW5jQXN5bmM8Q29tcGlsZVRlbXBsYXRlTWV0YWRhdGE+IHtcbiAgICByZXR1cm4gU3luY0FzeW5jLnRoZW4oXG4gICAgICAgIHRoaXMuX2xvYWRNaXNzaW5nRXh0ZXJuYWxTdHlsZXNoZWV0cyhcbiAgICAgICAgICAgIHByZXBhcnNlZFRlbXBsYXRlLnN0eWxlVXJscy5jb25jYXQocHJlcGFyc2VkVGVtcGxhdGUuaW5saW5lU3R5bGVVcmxzKSksXG4gICAgICAgIChleHRlcm5hbFN0eWxlc2hlZXRzKSA9PiB0aGlzLl9ub3JtYWxpemVMb2FkZWRUZW1wbGF0ZU1ldGFkYXRhKFxuICAgICAgICAgICAgcHJlbm9ybURhdGEsIHByZXBhcnNlZFRlbXBsYXRlLCBleHRlcm5hbFN0eWxlc2hlZXRzKSk7XG4gIH1cblxuICBwcml2YXRlIF9ub3JtYWxpemVMb2FkZWRUZW1wbGF0ZU1ldGFkYXRhKFxuICAgICAgcHJlbm9ybURhdGE6IFByZW5vcm1hbGl6ZWRUZW1wbGF0ZU1ldGFkYXRhLCBwcmVwYXJzZWRUZW1wbGF0ZTogUHJlcGFyc2VkVGVtcGxhdGUsXG4gICAgICBzdHlsZXNoZWV0czogTWFwPHN0cmluZywgQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YT4pOiBDb21waWxlVGVtcGxhdGVNZXRhZGF0YSB7XG4gICAgLy8gQWxnb3JpdGhtOlxuICAgIC8vIC0gcHJvZHVjZSBleGFjdGx5IDEgZW50cnkgcGVyIG9yaWdpbmFsIHN0eWxlVXJsIGluXG4gICAgLy8gQ29tcGlsZVRlbXBsYXRlTWV0YWRhdGEuZXh0ZXJuYWxTdHlsZXNoZWV0cyB3aXRoIGFsbCBzdHlsZXMgaW5saW5lZFxuICAgIC8vIC0gaW5saW5lIGFsbCBzdHlsZXMgdGhhdCBhcmUgcmVmZXJlbmNlZCBieSB0aGUgdGVtcGxhdGUgaW50byBDb21waWxlVGVtcGxhdGVNZXRhZGF0YS5zdHlsZXMuXG4gICAgLy8gUmVhc29uOiBiZSBhYmxlIHRvIGRldGVybWluZSBob3cgbWFueSBzdHlsZXNoZWV0cyB0aGVyZSBhcmUgZXZlbiB3aXRob3V0IGxvYWRpbmdcbiAgICAvLyB0aGUgdGVtcGxhdGUgbm9yIHRoZSBzdHlsZXNoZWV0cywgc28gd2UgY2FuIGNyZWF0ZSBhIHN0dWIgZm9yIFR5cGVTY3JpcHQgYWx3YXlzIHN5bmNocm9ub3VzbHlcbiAgICAvLyAoYXMgcmVzb3VyY2UgbG9hZGluZyBtYXkgYmUgYXN5bmMpXG5cbiAgICBjb25zdCBzdHlsZXMgPSBbLi4ucHJlcGFyc2VkVGVtcGxhdGUuc3R5bGVzXTtcbiAgICB0aGlzLl9pbmxpbmVTdHlsZXMocHJlcGFyc2VkVGVtcGxhdGUuaW5saW5lU3R5bGVVcmxzLCBzdHlsZXNoZWV0cywgc3R5bGVzKTtcbiAgICBjb25zdCBzdHlsZVVybHMgPSBwcmVwYXJzZWRUZW1wbGF0ZS5zdHlsZVVybHM7XG5cbiAgICBjb25zdCBleHRlcm5hbFN0eWxlc2hlZXRzID0gc3R5bGVVcmxzLm1hcChzdHlsZVVybCA9PiB7XG4gICAgICBjb25zdCBzdHlsZXNoZWV0ID0gc3R5bGVzaGVldHMuZ2V0KHN0eWxlVXJsKSAhO1xuICAgICAgY29uc3Qgc3R5bGVzID0gWy4uLnN0eWxlc2hlZXQuc3R5bGVzXTtcbiAgICAgIHRoaXMuX2lubGluZVN0eWxlcyhzdHlsZXNoZWV0LnN0eWxlVXJscywgc3R5bGVzaGVldHMsIHN0eWxlcyk7XG4gICAgICByZXR1cm4gbmV3IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEoe21vZHVsZVVybDogc3R5bGVVcmwsIHN0eWxlczogc3R5bGVzfSk7XG4gICAgfSk7XG5cbiAgICBsZXQgZW5jYXBzdWxhdGlvbiA9IHByZW5vcm1EYXRhLmVuY2Fwc3VsYXRpb247XG4gICAgaWYgKGVuY2Fwc3VsYXRpb24gPT0gbnVsbCkge1xuICAgICAgZW5jYXBzdWxhdGlvbiA9IHRoaXMuX2NvbmZpZy5kZWZhdWx0RW5jYXBzdWxhdGlvbjtcbiAgICB9XG4gICAgaWYgKGVuY2Fwc3VsYXRpb24gPT09IFZpZXdFbmNhcHN1bGF0aW9uLkVtdWxhdGVkICYmIHN0eWxlcy5sZW5ndGggPT09IDAgJiZcbiAgICAgICAgc3R5bGVVcmxzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgZW5jYXBzdWxhdGlvbiA9IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmU7XG4gICAgfVxuICAgIHJldHVybiBuZXcgQ29tcGlsZVRlbXBsYXRlTWV0YWRhdGEoe1xuICAgICAgZW5jYXBzdWxhdGlvbixcbiAgICAgIHRlbXBsYXRlOiBwcmVwYXJzZWRUZW1wbGF0ZS50ZW1wbGF0ZSxcbiAgICAgIHRlbXBsYXRlVXJsOiBwcmVwYXJzZWRUZW1wbGF0ZS50ZW1wbGF0ZVVybCxcbiAgICAgIGh0bWxBc3Q6IHByZXBhcnNlZFRlbXBsYXRlLmh0bWxBc3QsIHN0eWxlcywgc3R5bGVVcmxzLFxuICAgICAgbmdDb250ZW50U2VsZWN0b3JzOiBwcmVwYXJzZWRUZW1wbGF0ZS5uZ0NvbnRlbnRTZWxlY3RvcnMsXG4gICAgICBhbmltYXRpb25zOiBwcmVub3JtRGF0YS5hbmltYXRpb25zLFxuICAgICAgaW50ZXJwb2xhdGlvbjogcHJlbm9ybURhdGEuaW50ZXJwb2xhdGlvbixcbiAgICAgIGlzSW5saW5lOiBwcmVwYXJzZWRUZW1wbGF0ZS5pc0lubGluZSwgZXh0ZXJuYWxTdHlsZXNoZWV0cyxcbiAgICAgIHByZXNlcnZlV2hpdGVzcGFjZXM6IHByZXNlcnZlV2hpdGVzcGFjZXNEZWZhdWx0KFxuICAgICAgICAgIHByZW5vcm1EYXRhLnByZXNlcnZlV2hpdGVzcGFjZXMsIHRoaXMuX2NvbmZpZy5wcmVzZXJ2ZVdoaXRlc3BhY2VzKSxcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgX2lubGluZVN0eWxlcyhcbiAgICAgIHN0eWxlVXJsczogc3RyaW5nW10sIHN0eWxlc2hlZXRzOiBNYXA8c3RyaW5nLCBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhPixcbiAgICAgIHRhcmdldFN0eWxlczogc3RyaW5nW10pIHtcbiAgICBzdHlsZVVybHMuZm9yRWFjaChzdHlsZVVybCA9PiB7XG4gICAgICBjb25zdCBzdHlsZXNoZWV0ID0gc3R5bGVzaGVldHMuZ2V0KHN0eWxlVXJsKSAhO1xuICAgICAgc3R5bGVzaGVldC5zdHlsZXMuZm9yRWFjaChzdHlsZSA9PiB0YXJnZXRTdHlsZXMucHVzaChzdHlsZSkpO1xuICAgICAgdGhpcy5faW5saW5lU3R5bGVzKHN0eWxlc2hlZXQuc3R5bGVVcmxzLCBzdHlsZXNoZWV0cywgdGFyZ2V0U3R5bGVzKTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgX2xvYWRNaXNzaW5nRXh0ZXJuYWxTdHlsZXNoZWV0cyhcbiAgICAgIHN0eWxlVXJsczogc3RyaW5nW10sXG4gICAgICBsb2FkZWRTdHlsZXNoZWV0czpcbiAgICAgICAgICBNYXA8c3RyaW5nLCBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhPiA9IG5ldyBNYXA8c3RyaW5nLCBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhPigpKTpcbiAgICAgIFN5bmNBc3luYzxNYXA8c3RyaW5nLCBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhPj4ge1xuICAgIHJldHVybiBTeW5jQXN5bmMudGhlbihcbiAgICAgICAgU3luY0FzeW5jLmFsbChzdHlsZVVybHMuZmlsdGVyKChzdHlsZVVybCkgPT4gIWxvYWRlZFN0eWxlc2hlZXRzLmhhcyhzdHlsZVVybCkpXG4gICAgICAgICAgICAgICAgICAgICAgICAgIC5tYXAoXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICBzdHlsZVVybCA9PiBTeW5jQXN5bmMudGhlbihcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLl9mZXRjaChzdHlsZVVybCksXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgKGxvYWRlZFN0eWxlKSA9PiB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjb25zdCBzdHlsZXNoZWV0ID1cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLl9ub3JtYWxpemVTdHlsZXNoZWV0KG5ldyBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhKFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB7c3R5bGVzOiBbbG9hZGVkU3R5bGVdLCBtb2R1bGVVcmw6IHN0eWxlVXJsfSkpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgbG9hZGVkU3R5bGVzaGVldHMuc2V0KHN0eWxlVXJsLCBzdHlsZXNoZWV0KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiB0aGlzLl9sb2FkTWlzc2luZ0V4dGVybmFsU3R5bGVzaGVldHMoXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgc3R5bGVzaGVldC5zdHlsZVVybHMsIGxvYWRlZFN0eWxlc2hlZXRzKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KSkpLFxuICAgICAgICAoXykgPT4gbG9hZGVkU3R5bGVzaGVldHMpO1xuICB9XG5cbiAgcHJpdmF0ZSBfbm9ybWFsaXplU3R5bGVzaGVldChzdHlsZXNoZWV0OiBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhKTogQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YSB7XG4gICAgY29uc3QgbW9kdWxlVXJsID0gc3R5bGVzaGVldC5tb2R1bGVVcmwgITtcbiAgICBjb25zdCBhbGxTdHlsZVVybHMgPSBzdHlsZXNoZWV0LnN0eWxlVXJscy5maWx0ZXIoaXNTdHlsZVVybFJlc29sdmFibGUpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgIC5tYXAodXJsID0+IHRoaXMuX3VybFJlc29sdmVyLnJlc29sdmUobW9kdWxlVXJsLCB1cmwpKTtcblxuICAgIGNvbnN0IGFsbFN0eWxlcyA9IHN0eWxlc2hlZXQuc3R5bGVzLm1hcChzdHlsZSA9PiB7XG4gICAgICBjb25zdCBzdHlsZVdpdGhJbXBvcnRzID0gZXh0cmFjdFN0eWxlVXJscyh0aGlzLl91cmxSZXNvbHZlciwgbW9kdWxlVXJsLCBzdHlsZSk7XG4gICAgICBhbGxTdHlsZVVybHMucHVzaCguLi5zdHlsZVdpdGhJbXBvcnRzLnN0eWxlVXJscyk7XG4gICAgICByZXR1cm4gc3R5bGVXaXRoSW1wb3J0cy5zdHlsZTtcbiAgICB9KTtcblxuICAgIHJldHVybiBuZXcgQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YShcbiAgICAgICAge3N0eWxlczogYWxsU3R5bGVzLCBzdHlsZVVybHM6IGFsbFN0eWxlVXJscywgbW9kdWxlVXJsOiBtb2R1bGVVcmx9KTtcbiAgfVxufVxuXG5pbnRlcmZhY2UgUHJlcGFyc2VkVGVtcGxhdGUge1xuICB0ZW1wbGF0ZTogc3RyaW5nO1xuICB0ZW1wbGF0ZVVybDogc3RyaW5nO1xuICBpc0lubGluZTogYm9vbGVhbjtcbiAgaHRtbEFzdDogSHRtbFBhcnNlVHJlZVJlc3VsdDtcbiAgc3R5bGVzOiBzdHJpbmdbXTtcbiAgaW5saW5lU3R5bGVVcmxzOiBzdHJpbmdbXTtcbiAgc3R5bGVVcmxzOiBzdHJpbmdbXTtcbiAgbmdDb250ZW50U2VsZWN0b3JzOiBzdHJpbmdbXTtcbn1cblxuY2xhc3MgVGVtcGxhdGVQcmVwYXJzZVZpc2l0b3IgaW1wbGVtZW50cyBodG1sLlZpc2l0b3Ige1xuICBuZ0NvbnRlbnRTZWxlY3RvcnM6IHN0cmluZ1tdID0gW107XG4gIHN0eWxlczogc3RyaW5nW10gPSBbXTtcbiAgc3R5bGVVcmxzOiBzdHJpbmdbXSA9IFtdO1xuICBuZ05vbkJpbmRhYmxlU3RhY2tDb3VudDogbnVtYmVyID0gMDtcblxuICB2aXNpdEVsZW1lbnQoYXN0OiBodG1sLkVsZW1lbnQsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgY29uc3QgcHJlcGFyc2VkRWxlbWVudCA9IHByZXBhcnNlRWxlbWVudChhc3QpO1xuICAgIHN3aXRjaCAocHJlcGFyc2VkRWxlbWVudC50eXBlKSB7XG4gICAgICBjYXNlIFByZXBhcnNlZEVsZW1lbnRUeXBlLk5HX0NPTlRFTlQ6XG4gICAgICAgIGlmICh0aGlzLm5nTm9uQmluZGFibGVTdGFja0NvdW50ID09PSAwKSB7XG4gICAgICAgICAgdGhpcy5uZ0NvbnRlbnRTZWxlY3RvcnMucHVzaChwcmVwYXJzZWRFbGVtZW50LnNlbGVjdEF0dHIpO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBQcmVwYXJzZWRFbGVtZW50VHlwZS5TVFlMRTpcbiAgICAgICAgbGV0IHRleHRDb250ZW50ID0gJyc7XG4gICAgICAgIGFzdC5jaGlsZHJlbi5mb3JFYWNoKGNoaWxkID0+IHtcbiAgICAgICAgICBpZiAoY2hpbGQgaW5zdGFuY2VvZiBodG1sLlRleHQpIHtcbiAgICAgICAgICAgIHRleHRDb250ZW50ICs9IGNoaWxkLnZhbHVlO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICAgIHRoaXMuc3R5bGVzLnB1c2godGV4dENvbnRlbnQpO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgUHJlcGFyc2VkRWxlbWVudFR5cGUuU1RZTEVTSEVFVDpcbiAgICAgICAgdGhpcy5zdHlsZVVybHMucHVzaChwcmVwYXJzZWRFbGVtZW50LmhyZWZBdHRyKTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICBicmVhaztcbiAgICB9XG4gICAgaWYgKHByZXBhcnNlZEVsZW1lbnQubm9uQmluZGFibGUpIHtcbiAgICAgIHRoaXMubmdOb25CaW5kYWJsZVN0YWNrQ291bnQrKztcbiAgICB9XG4gICAgaHRtbC52aXNpdEFsbCh0aGlzLCBhc3QuY2hpbGRyZW4pO1xuICAgIGlmIChwcmVwYXJzZWRFbGVtZW50Lm5vbkJpbmRhYmxlKSB7XG4gICAgICB0aGlzLm5nTm9uQmluZGFibGVTdGFja0NvdW50LS07XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgdmlzaXRFeHBhbnNpb24oYXN0OiBodG1sLkV4cGFuc2lvbiwgY29udGV4dDogYW55KTogYW55IHsgaHRtbC52aXNpdEFsbCh0aGlzLCBhc3QuY2FzZXMpOyB9XG5cbiAgdmlzaXRFeHBhbnNpb25DYXNlKGFzdDogaHRtbC5FeHBhbnNpb25DYXNlLCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIGh0bWwudmlzaXRBbGwodGhpcywgYXN0LmV4cHJlc3Npb24pO1xuICB9XG5cbiAgdmlzaXRDb21tZW50KGFzdDogaHRtbC5Db21tZW50LCBjb250ZXh0OiBhbnkpOiBhbnkgeyByZXR1cm4gbnVsbDsgfVxuICB2aXNpdEF0dHJpYnV0ZShhc3Q6IGh0bWwuQXR0cmlidXRlLCBjb250ZXh0OiBhbnkpOiBhbnkgeyByZXR1cm4gbnVsbDsgfVxuICB2aXNpdFRleHQoYXN0OiBodG1sLlRleHQsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiBudWxsOyB9XG59XG4iXX0=