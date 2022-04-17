/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { CompileStylesheetMetadata, CompileTemplateMetadata, templateSourceUrl } from './compile_metadata';
import { preserveWhitespacesDefault } from './config';
import { ViewEncapsulation } from './core';
import * as html from './ml_parser/ast';
import { InterpolationConfig } from './ml_parser/interpolation_config';
import { extractStyleUrls, isStyleUrlResolvable } from './style_url_resolver';
import { PreparsedElementType, preparseElement } from './template_parser/template_preparser';
import { SyncAsync, isDefined, stringify, syntaxError } from './util';
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
        if (isDefined(prenormData.template)) {
            if (isDefined(prenormData.templateUrl)) {
                throw syntaxError("'" + stringify(prenormData.componentType) + "' component cannot define both template and templateUrl");
            }
            if (typeof prenormData.template !== 'string') {
                throw syntaxError("The template specified for component " + stringify(prenormData.componentType) + " is not a string");
            }
        }
        else if (isDefined(prenormData.templateUrl)) {
            if (typeof prenormData.templateUrl !== 'string') {
                throw syntaxError("The templateUrl specified for component " + stringify(prenormData.componentType) + " is not a string");
            }
        }
        else {
            throw syntaxError("No template specified for component " + stringify(prenormData.componentType));
        }
        if (isDefined(prenormData.preserveWhitespaces) &&
            typeof prenormData.preserveWhitespaces !== 'boolean') {
            throw syntaxError("The preserveWhitespaces option for component " + stringify(prenormData.componentType) + " must be a boolean");
        }
        return SyncAsync.then(this._preParseTemplate(prenormData), function (preparsedTemplate) { return _this._normalizeTemplateMetadata(prenormData, preparsedTemplate); });
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
        return SyncAsync.then(template, function (template) { return _this._preparseLoadedTemplate(prenomData, template, templateUrl); });
    };
    DirectiveNormalizer.prototype._preparseLoadedTemplate = function (prenormData, template, templateAbsUrl) {
        var isInline = !!prenormData.template;
        var interpolationConfig = InterpolationConfig.fromArray(prenormData.interpolation);
        var templateUrl = templateSourceUrl({ reference: prenormData.ngModuleType }, { type: { reference: prenormData.componentType } }, { isInline: isInline, templateUrl: templateAbsUrl });
        var rootNodesAndErrors = this._htmlParser.parse(template, templateUrl, { tokenizeExpansionForms: true, interpolationConfig: interpolationConfig });
        if (rootNodesAndErrors.errors.length > 0) {
            var errorString = rootNodesAndErrors.errors.join('\n');
            throw syntaxError("Template parse errors:\n" + errorString);
        }
        var templateMetadataStyles = this._normalizeStylesheet(new CompileStylesheetMetadata({ styles: prenormData.styles, moduleUrl: prenormData.moduleUrl }));
        var visitor = new TemplatePreparseVisitor();
        html.visitAll(visitor, rootNodesAndErrors.rootNodes);
        var templateStyles = this._normalizeStylesheet(new CompileStylesheetMetadata({ styles: visitor.styles, styleUrls: visitor.styleUrls, moduleUrl: templateAbsUrl }));
        var styles = templateMetadataStyles.styles.concat(templateStyles.styles);
        var inlineStyleUrls = templateMetadataStyles.styleUrls.concat(templateStyles.styleUrls);
        var styleUrls = this
            ._normalizeStylesheet(new CompileStylesheetMetadata({ styleUrls: prenormData.styleUrls, moduleUrl: prenormData.moduleUrl }))
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
        return SyncAsync.then(this._loadMissingExternalStylesheets(preparsedTemplate.styleUrls.concat(preparsedTemplate.inlineStyleUrls)), function (externalStylesheets) { return _this._normalizeLoadedTemplateMetadata(prenormData, preparsedTemplate, externalStylesheets); });
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
            return new CompileStylesheetMetadata({ moduleUrl: styleUrl, styles: styles });
        });
        var encapsulation = prenormData.encapsulation;
        if (encapsulation == null) {
            encapsulation = this._config.defaultEncapsulation;
        }
        if (encapsulation === ViewEncapsulation.Emulated && styles.length === 0 &&
            styleUrls.length === 0) {
            encapsulation = ViewEncapsulation.None;
        }
        return new CompileTemplateMetadata({
            encapsulation: encapsulation,
            template: preparsedTemplate.template,
            templateUrl: preparsedTemplate.templateUrl,
            htmlAst: preparsedTemplate.htmlAst, styles: styles, styleUrls: styleUrls,
            ngContentSelectors: preparsedTemplate.ngContentSelectors,
            animations: prenormData.animations,
            interpolation: prenormData.interpolation,
            isInline: preparsedTemplate.isInline, externalStylesheets: externalStylesheets,
            preserveWhitespaces: preserveWhitespacesDefault(prenormData.preserveWhitespaces, this._config.preserveWhitespaces),
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
        return SyncAsync.then(SyncAsync.all(styleUrls.filter(function (styleUrl) { return !loadedStylesheets.has(styleUrl); })
            .map(function (styleUrl) { return SyncAsync.then(_this._fetch(styleUrl), function (loadedStyle) {
            var stylesheet = _this._normalizeStylesheet(new CompileStylesheetMetadata({ styles: [loadedStyle], moduleUrl: styleUrl }));
            loadedStylesheets.set(styleUrl, stylesheet);
            return _this._loadMissingExternalStylesheets(stylesheet.styleUrls, loadedStylesheets);
        }); })), function (_) { return loadedStylesheets; });
    };
    DirectiveNormalizer.prototype._normalizeStylesheet = function (stylesheet) {
        var _this = this;
        var moduleUrl = stylesheet.moduleUrl;
        var allStyleUrls = stylesheet.styleUrls.filter(isStyleUrlResolvable)
            .map(function (url) { return _this._urlResolver.resolve(moduleUrl, url); });
        var allStyles = stylesheet.styles.map(function (style) {
            var styleWithImports = extractStyleUrls(_this._urlResolver, moduleUrl, style);
            allStyleUrls.push.apply(allStyleUrls, tslib_1.__spread(styleWithImports.styleUrls));
            return styleWithImports.style;
        });
        return new CompileStylesheetMetadata({ styles: allStyles, styleUrls: allStyleUrls, moduleUrl: moduleUrl });
    };
    return DirectiveNormalizer;
}());
export { DirectiveNormalizer };
var TemplatePreparseVisitor = /** @class */ (function () {
    function TemplatePreparseVisitor() {
        this.ngContentSelectors = [];
        this.styles = [];
        this.styleUrls = [];
        this.ngNonBindableStackCount = 0;
    }
    TemplatePreparseVisitor.prototype.visitElement = function (ast, context) {
        var preparsedElement = preparseElement(ast);
        switch (preparsedElement.type) {
            case PreparsedElementType.NG_CONTENT:
                if (this.ngNonBindableStackCount === 0) {
                    this.ngContentSelectors.push(preparsedElement.selectAttr);
                }
                break;
            case PreparsedElementType.STYLE:
                var textContent_1 = '';
                ast.children.forEach(function (child) {
                    if (child instanceof html.Text) {
                        textContent_1 += child.value;
                    }
                });
                this.styles.push(textContent_1);
                break;
            case PreparsedElementType.STYLESHEET:
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGlyZWN0aXZlX25vcm1hbGl6ZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvZGlyZWN0aXZlX25vcm1hbGl6ZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBMkIseUJBQXlCLEVBQUUsdUJBQXVCLEVBQUUsaUJBQWlCLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUNuSSxPQUFPLEVBQWlCLDBCQUEwQixFQUFDLE1BQU0sVUFBVSxDQUFDO0FBQ3BFLE9BQU8sRUFBQyxpQkFBaUIsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUN6QyxPQUFPLEtBQUssSUFBSSxNQUFNLGlCQUFpQixDQUFDO0FBRXhDLE9BQU8sRUFBQyxtQkFBbUIsRUFBQyxNQUFNLGtDQUFrQyxDQUFDO0FBR3JFLE9BQU8sRUFBQyxnQkFBZ0IsRUFBRSxvQkFBb0IsRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQzVFLE9BQU8sRUFBQyxvQkFBb0IsRUFBRSxlQUFlLEVBQUMsTUFBTSxzQ0FBc0MsQ0FBQztBQUUzRixPQUFPLEVBQUMsU0FBUyxFQUFFLFNBQVMsRUFBRSxTQUFTLEVBQUUsV0FBVyxFQUFDLE1BQU0sUUFBUSxDQUFDO0FBZ0JwRTtJQUdFLDZCQUNZLGVBQStCLEVBQVUsWUFBeUIsRUFDbEUsV0FBdUIsRUFBVSxPQUF1QjtRQUR4RCxvQkFBZSxHQUFmLGVBQWUsQ0FBZ0I7UUFBVSxpQkFBWSxHQUFaLFlBQVksQ0FBYTtRQUNsRSxnQkFBVyxHQUFYLFdBQVcsQ0FBWTtRQUFVLFlBQU8sR0FBUCxPQUFPLENBQWdCO1FBSjVELHlCQUFvQixHQUFHLElBQUksR0FBRyxFQUE2QixDQUFDO0lBSUcsQ0FBQztJQUV4RSx3Q0FBVSxHQUFWLGNBQXFCLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxLQUFLLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFFekQs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QyxDQUFDLENBQUM7SUFFMUYsb0RBQWtCLEdBQWxCLFVBQW1CLEdBQXVCLEVBQUUsT0FBWTtRQUN0RCxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxHQUFHLENBQUMsVUFBVSxDQUFDLENBQUM7SUFDdEMsQ0FBQztJQUVELDhDQUFZLEdBQVosVUFBYSxHQUFpQixFQUFFLE9BQVksSUFBUyxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7SUFDbkUsZ0RBQWMsR0FBZCxVQUFlLEdBQW1CLEVBQUUsT0FBWSxJQUFTLE9BQU8sSUFBSSxDQUFDLENBQUMsQ0FBQztJQUN2RSwyQ0FBUyxHQUFULFVBQVUsR0FBYyxFQUFFLE9BQVksSUFBUyxPQUFPLElBQUksQ0FBQyxDQUFDLENBQUM7SUFDL0QsOEJBQUM7QUFBRCxDQUFDLEFBaERELElBZ0RDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0NvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YSwgQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YSwgQ29tcGlsZVRlbXBsYXRlTWV0YWRhdGEsIHRlbXBsYXRlU291cmNlVXJsfSBmcm9tICcuL2NvbXBpbGVfbWV0YWRhdGEnO1xuaW1wb3J0IHtDb21waWxlckNvbmZpZywgcHJlc2VydmVXaGl0ZXNwYWNlc0RlZmF1bHR9IGZyb20gJy4vY29uZmlnJztcbmltcG9ydCB7Vmlld0VuY2Fwc3VsYXRpb259IGZyb20gJy4vY29yZSc7XG5pbXBvcnQgKiBhcyBodG1sIGZyb20gJy4vbWxfcGFyc2VyL2FzdCc7XG5pbXBvcnQge0h0bWxQYXJzZXJ9IGZyb20gJy4vbWxfcGFyc2VyL2h0bWxfcGFyc2VyJztcbmltcG9ydCB7SW50ZXJwb2xhdGlvbkNvbmZpZ30gZnJvbSAnLi9tbF9wYXJzZXIvaW50ZXJwb2xhdGlvbl9jb25maWcnO1xuaW1wb3J0IHtQYXJzZVRyZWVSZXN1bHQgYXMgSHRtbFBhcnNlVHJlZVJlc3VsdH0gZnJvbSAnLi9tbF9wYXJzZXIvcGFyc2VyJztcbmltcG9ydCB7UmVzb3VyY2VMb2FkZXJ9IGZyb20gJy4vcmVzb3VyY2VfbG9hZGVyJztcbmltcG9ydCB7ZXh0cmFjdFN0eWxlVXJscywgaXNTdHlsZVVybFJlc29sdmFibGV9IGZyb20gJy4vc3R5bGVfdXJsX3Jlc29sdmVyJztcbmltcG9ydCB7UHJlcGFyc2VkRWxlbWVudFR5cGUsIHByZXBhcnNlRWxlbWVudH0gZnJvbSAnLi90ZW1wbGF0ZV9wYXJzZXIvdGVtcGxhdGVfcHJlcGFyc2VyJztcbmltcG9ydCB7VXJsUmVzb2x2ZXJ9IGZyb20gJy4vdXJsX3Jlc29sdmVyJztcbmltcG9ydCB7U3luY0FzeW5jLCBpc0RlZmluZWQsIHN0cmluZ2lmeSwgc3ludGF4RXJyb3J9IGZyb20gJy4vdXRpbCc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgUHJlbm9ybWFsaXplZFRlbXBsYXRlTWV0YWRhdGEge1xuICBuZ01vZHVsZVR5cGU6IGFueTtcbiAgY29tcG9uZW50VHlwZTogYW55O1xuICBtb2R1bGVVcmw6IHN0cmluZztcbiAgdGVtcGxhdGU6IHN0cmluZ3xudWxsO1xuICB0ZW1wbGF0ZVVybDogc3RyaW5nfG51bGw7XG4gIHN0eWxlczogc3RyaW5nW107XG4gIHN0eWxlVXJsczogc3RyaW5nW107XG4gIGludGVycG9sYXRpb246IFtzdHJpbmcsIHN0cmluZ118bnVsbDtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb258bnVsbDtcbiAgYW5pbWF0aW9uczogYW55W107XG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGJvb2xlYW58bnVsbDtcbn1cblxuZXhwb3J0IGNsYXNzIERpcmVjdGl2ZU5vcm1hbGl6ZXIge1xuICBwcml2YXRlIF9yZXNvdXJjZUxvYWRlckNhY2hlID0gbmV3IE1hcDxzdHJpbmcsIFN5bmNBc3luYzxzdHJpbmc+PigpO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBfcmVzb3VyY2VMb2FkZXI6IFJlc291cmNlTG9hZGVyLCBwcml2YXRlIF91cmxSZXNvbHZlcjogVXJsUmVzb2x2ZXIsXG4gICAgICBwcml2YXRlIF9odG1sUGFyc2VyOiBIdG1sUGFyc2VyLCBwcml2YXRlIF9jb25maWc6IENvbXBpbGVyQ29uZmlnKSB7fVxuXG4gIGNsZWFyQ2FjaGUoKTogdm9pZCB7IHRoaXMuX3Jlc291cmNlTG9hZGVyQ2FjaGUuY2xlYXIoKTsgfVxuXG4gIGNsZWFyQ2FjaGVGb3Iobm9ybWFsaXplZERpcmVjdGl2ZTogQ29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhKTogdm9pZCB7XG4gICAgaWYgKCFub3JtYWxpemVkRGlyZWN0aXZlLmlzQ29tcG9uZW50KSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGNvbnN0IHRlbXBsYXRlID0gbm9ybWFsaXplZERpcmVjdGl2ZS50ZW1wbGF0ZSAhO1xuICAgIHRoaXMuX3Jlc291cmNlTG9hZGVyQ2FjaGUuZGVsZXRlKHRlbXBsYXRlLnRlbXBsYXRlVXJsICEpO1xuICAgIHRlbXBsYXRlLmV4dGVybmFsU3R5bGVzaGVldHMuZm9yRWFjaChcbiAgICAgICAgKHN0eWxlc2hlZXQpID0+IHsgdGhpcy5fcmVzb3VyY2VMb2FkZXJDYWNoZS5kZWxldGUoc3R5bGVzaGVldC5tb2R1bGVVcmwgISk7IH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfZmV0Y2godXJsOiBzdHJpbmcpOiBTeW5jQXN5bmM8c3RyaW5nPiB7XG4gICAgbGV0IHJlc3VsdCA9IHRoaXMuX3Jlc291cmNlTG9hZGVyQ2FjaGUuZ2V0KHVybCk7XG4gICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgIHJlc3VsdCA9IHRoaXMuX3Jlc291cmNlTG9hZGVyLmdldCh1cmwpO1xuICAgICAgdGhpcy5fcmVzb3VyY2VMb2FkZXJDYWNoZS5zZXQodXJsLCByZXN1bHQpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgbm9ybWFsaXplVGVtcGxhdGUocHJlbm9ybURhdGE6IFByZW5vcm1hbGl6ZWRUZW1wbGF0ZU1ldGFkYXRhKTpcbiAgICAgIFN5bmNBc3luYzxDb21waWxlVGVtcGxhdGVNZXRhZGF0YT4ge1xuICAgIGlmIChpc0RlZmluZWQocHJlbm9ybURhdGEudGVtcGxhdGUpKSB7XG4gICAgICBpZiAoaXNEZWZpbmVkKHByZW5vcm1EYXRhLnRlbXBsYXRlVXJsKSkge1xuICAgICAgICB0aHJvdyBzeW50YXhFcnJvcihcbiAgICAgICAgICAgIGAnJHtzdHJpbmdpZnkocHJlbm9ybURhdGEuY29tcG9uZW50VHlwZSl9JyBjb21wb25lbnQgY2Fubm90IGRlZmluZSBib3RoIHRlbXBsYXRlIGFuZCB0ZW1wbGF0ZVVybGApO1xuICAgICAgfVxuICAgICAgaWYgKHR5cGVvZiBwcmVub3JtRGF0YS50ZW1wbGF0ZSAhPT0gJ3N0cmluZycpIHtcbiAgICAgICAgdGhyb3cgc3ludGF4RXJyb3IoXG4gICAgICAgICAgICBgVGhlIHRlbXBsYXRlIHNwZWNpZmllZCBmb3IgY29tcG9uZW50ICR7c3RyaW5naWZ5KHByZW5vcm1EYXRhLmNvbXBvbmVudFR5cGUpfSBpcyBub3QgYSBzdHJpbmdgKTtcbiAgICAgIH1cbiAgICB9IGVsc2UgaWYgKGlzRGVmaW5lZChwcmVub3JtRGF0YS50ZW1wbGF0ZVVybCkpIHtcbiAgICAgIGlmICh0eXBlb2YgcHJlbm9ybURhdGEudGVtcGxhdGVVcmwgIT09ICdzdHJpbmcnKSB7XG4gICAgICAgIHRocm93IHN5bnRheEVycm9yKFxuICAgICAgICAgICAgYFRoZSB0ZW1wbGF0ZVVybCBzcGVjaWZpZWQgZm9yIGNvbXBvbmVudCAke3N0cmluZ2lmeShwcmVub3JtRGF0YS5jb21wb25lbnRUeXBlKX0gaXMgbm90IGEgc3RyaW5nYCk7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIHRocm93IHN5bnRheEVycm9yKFxuICAgICAgICAgIGBObyB0ZW1wbGF0ZSBzcGVjaWZpZWQgZm9yIGNvbXBvbmVudCAke3N0cmluZ2lmeShwcmVub3JtRGF0YS5jb21wb25lbnRUeXBlKX1gKTtcbiAgICB9XG5cbiAgICBpZiAoaXNEZWZpbmVkKHByZW5vcm1EYXRhLnByZXNlcnZlV2hpdGVzcGFjZXMpICYmXG4gICAgICAgIHR5cGVvZiBwcmVub3JtRGF0YS5wcmVzZXJ2ZVdoaXRlc3BhY2VzICE9PSAnYm9vbGVhbicpIHtcbiAgICAgIHRocm93IHN5bnRheEVycm9yKFxuICAgICAgICAgIGBUaGUgcHJlc2VydmVXaGl0ZXNwYWNlcyBvcHRpb24gZm9yIGNvbXBvbmVudCAke3N0cmluZ2lmeShwcmVub3JtRGF0YS5jb21wb25lbnRUeXBlKX0gbXVzdCBiZSBhIGJvb2xlYW5gKTtcbiAgICB9XG5cbiAgICByZXR1cm4gU3luY0FzeW5jLnRoZW4oXG4gICAgICAgIHRoaXMuX3ByZVBhcnNlVGVtcGxhdGUocHJlbm9ybURhdGEpLFxuICAgICAgICAocHJlcGFyc2VkVGVtcGxhdGUpID0+IHRoaXMuX25vcm1hbGl6ZVRlbXBsYXRlTWV0YWRhdGEocHJlbm9ybURhdGEsIHByZXBhcnNlZFRlbXBsYXRlKSk7XG4gIH1cblxuICBwcml2YXRlIF9wcmVQYXJzZVRlbXBsYXRlKHByZW5vbURhdGE6IFByZW5vcm1hbGl6ZWRUZW1wbGF0ZU1ldGFkYXRhKTpcbiAgICAgIFN5bmNBc3luYzxQcmVwYXJzZWRUZW1wbGF0ZT4ge1xuICAgIGxldCB0ZW1wbGF0ZTogU3luY0FzeW5jPHN0cmluZz47XG4gICAgbGV0IHRlbXBsYXRlVXJsOiBzdHJpbmc7XG4gICAgaWYgKHByZW5vbURhdGEudGVtcGxhdGUgIT0gbnVsbCkge1xuICAgICAgdGVtcGxhdGUgPSBwcmVub21EYXRhLnRlbXBsYXRlO1xuICAgICAgdGVtcGxhdGVVcmwgPSBwcmVub21EYXRhLm1vZHVsZVVybDtcbiAgICB9IGVsc2Uge1xuICAgICAgdGVtcGxhdGVVcmwgPSB0aGlzLl91cmxSZXNvbHZlci5yZXNvbHZlKHByZW5vbURhdGEubW9kdWxlVXJsLCBwcmVub21EYXRhLnRlbXBsYXRlVXJsICEpO1xuICAgICAgdGVtcGxhdGUgPSB0aGlzLl9mZXRjaCh0ZW1wbGF0ZVVybCk7XG4gICAgfVxuICAgIHJldHVybiBTeW5jQXN5bmMudGhlbihcbiAgICAgICAgdGVtcGxhdGUsICh0ZW1wbGF0ZSkgPT4gdGhpcy5fcHJlcGFyc2VMb2FkZWRUZW1wbGF0ZShwcmVub21EYXRhLCB0ZW1wbGF0ZSwgdGVtcGxhdGVVcmwpKTtcbiAgfVxuXG4gIHByaXZhdGUgX3ByZXBhcnNlTG9hZGVkVGVtcGxhdGUoXG4gICAgICBwcmVub3JtRGF0YTogUHJlbm9ybWFsaXplZFRlbXBsYXRlTWV0YWRhdGEsIHRlbXBsYXRlOiBzdHJpbmcsXG4gICAgICB0ZW1wbGF0ZUFic1VybDogc3RyaW5nKTogUHJlcGFyc2VkVGVtcGxhdGUge1xuICAgIGNvbnN0IGlzSW5saW5lID0gISFwcmVub3JtRGF0YS50ZW1wbGF0ZTtcbiAgICBjb25zdCBpbnRlcnBvbGF0aW9uQ29uZmlnID0gSW50ZXJwb2xhdGlvbkNvbmZpZy5mcm9tQXJyYXkocHJlbm9ybURhdGEuaW50ZXJwb2xhdGlvbiAhKTtcbiAgICBjb25zdCB0ZW1wbGF0ZVVybCA9IHRlbXBsYXRlU291cmNlVXJsKFxuICAgICAgICB7cmVmZXJlbmNlOiBwcmVub3JtRGF0YS5uZ01vZHVsZVR5cGV9LCB7dHlwZToge3JlZmVyZW5jZTogcHJlbm9ybURhdGEuY29tcG9uZW50VHlwZX19LFxuICAgICAgICB7aXNJbmxpbmUsIHRlbXBsYXRlVXJsOiB0ZW1wbGF0ZUFic1VybH0pO1xuICAgIGNvbnN0IHJvb3ROb2Rlc0FuZEVycm9ycyA9IHRoaXMuX2h0bWxQYXJzZXIucGFyc2UoXG4gICAgICAgIHRlbXBsYXRlLCB0ZW1wbGF0ZVVybCwge3Rva2VuaXplRXhwYW5zaW9uRm9ybXM6IHRydWUsIGludGVycG9sYXRpb25Db25maWd9KTtcbiAgICBpZiAocm9vdE5vZGVzQW5kRXJyb3JzLmVycm9ycy5sZW5ndGggPiAwKSB7XG4gICAgICBjb25zdCBlcnJvclN0cmluZyA9IHJvb3ROb2Rlc0FuZEVycm9ycy5lcnJvcnMuam9pbignXFxuJyk7XG4gICAgICB0aHJvdyBzeW50YXhFcnJvcihgVGVtcGxhdGUgcGFyc2UgZXJyb3JzOlxcbiR7ZXJyb3JTdHJpbmd9YCk7XG4gICAgfVxuXG4gICAgY29uc3QgdGVtcGxhdGVNZXRhZGF0YVN0eWxlcyA9IHRoaXMuX25vcm1hbGl6ZVN0eWxlc2hlZXQobmV3IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEoXG4gICAgICAgIHtzdHlsZXM6IHByZW5vcm1EYXRhLnN0eWxlcywgbW9kdWxlVXJsOiBwcmVub3JtRGF0YS5tb2R1bGVVcmx9KSk7XG5cbiAgICBjb25zdCB2aXNpdG9yID0gbmV3IFRlbXBsYXRlUHJlcGFyc2VWaXNpdG9yKCk7XG4gICAgaHRtbC52aXNpdEFsbCh2aXNpdG9yLCByb290Tm9kZXNBbmRFcnJvcnMucm9vdE5vZGVzKTtcbiAgICBjb25zdCB0ZW1wbGF0ZVN0eWxlcyA9IHRoaXMuX25vcm1hbGl6ZVN0eWxlc2hlZXQobmV3IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEoXG4gICAgICAgIHtzdHlsZXM6IHZpc2l0b3Iuc3R5bGVzLCBzdHlsZVVybHM6IHZpc2l0b3Iuc3R5bGVVcmxzLCBtb2R1bGVVcmw6IHRlbXBsYXRlQWJzVXJsfSkpO1xuXG4gICAgY29uc3Qgc3R5bGVzID0gdGVtcGxhdGVNZXRhZGF0YVN0eWxlcy5zdHlsZXMuY29uY2F0KHRlbXBsYXRlU3R5bGVzLnN0eWxlcyk7XG5cbiAgICBjb25zdCBpbmxpbmVTdHlsZVVybHMgPSB0ZW1wbGF0ZU1ldGFkYXRhU3R5bGVzLnN0eWxlVXJscy5jb25jYXQodGVtcGxhdGVTdHlsZXMuc3R5bGVVcmxzKTtcbiAgICBjb25zdCBzdHlsZVVybHMgPSB0aGlzXG4gICAgICAgICAgICAgICAgICAgICAgICAgIC5fbm9ybWFsaXplU3R5bGVzaGVldChuZXcgQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YShcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHtzdHlsZVVybHM6IHByZW5vcm1EYXRhLnN0eWxlVXJscywgbW9kdWxlVXJsOiBwcmVub3JtRGF0YS5tb2R1bGVVcmx9KSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgLnN0eWxlVXJscztcbiAgICByZXR1cm4ge1xuICAgICAgdGVtcGxhdGUsXG4gICAgICB0ZW1wbGF0ZVVybDogdGVtcGxhdGVBYnNVcmwsIGlzSW5saW5lLFxuICAgICAgaHRtbEFzdDogcm9vdE5vZGVzQW5kRXJyb3JzLCBzdHlsZXMsIGlubGluZVN0eWxlVXJscywgc3R5bGVVcmxzLFxuICAgICAgbmdDb250ZW50U2VsZWN0b3JzOiB2aXNpdG9yLm5nQ29udGVudFNlbGVjdG9ycyxcbiAgICB9O1xuICB9XG5cbiAgcHJpdmF0ZSBfbm9ybWFsaXplVGVtcGxhdGVNZXRhZGF0YShcbiAgICAgIHByZW5vcm1EYXRhOiBQcmVub3JtYWxpemVkVGVtcGxhdGVNZXRhZGF0YSxcbiAgICAgIHByZXBhcnNlZFRlbXBsYXRlOiBQcmVwYXJzZWRUZW1wbGF0ZSk6IFN5bmNBc3luYzxDb21waWxlVGVtcGxhdGVNZXRhZGF0YT4ge1xuICAgIHJldHVybiBTeW5jQXN5bmMudGhlbihcbiAgICAgICAgdGhpcy5fbG9hZE1pc3NpbmdFeHRlcm5hbFN0eWxlc2hlZXRzKFxuICAgICAgICAgICAgcHJlcGFyc2VkVGVtcGxhdGUuc3R5bGVVcmxzLmNvbmNhdChwcmVwYXJzZWRUZW1wbGF0ZS5pbmxpbmVTdHlsZVVybHMpKSxcbiAgICAgICAgKGV4dGVybmFsU3R5bGVzaGVldHMpID0+IHRoaXMuX25vcm1hbGl6ZUxvYWRlZFRlbXBsYXRlTWV0YWRhdGEoXG4gICAgICAgICAgICBwcmVub3JtRGF0YSwgcHJlcGFyc2VkVGVtcGxhdGUsIGV4dGVybmFsU3R5bGVzaGVldHMpKTtcbiAgfVxuXG4gIHByaXZhdGUgX25vcm1hbGl6ZUxvYWRlZFRlbXBsYXRlTWV0YWRhdGEoXG4gICAgICBwcmVub3JtRGF0YTogUHJlbm9ybWFsaXplZFRlbXBsYXRlTWV0YWRhdGEsIHByZXBhcnNlZFRlbXBsYXRlOiBQcmVwYXJzZWRUZW1wbGF0ZSxcbiAgICAgIHN0eWxlc2hlZXRzOiBNYXA8c3RyaW5nLCBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhPik6IENvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhIHtcbiAgICAvLyBBbGdvcml0aG06XG4gICAgLy8gLSBwcm9kdWNlIGV4YWN0bHkgMSBlbnRyeSBwZXIgb3JpZ2luYWwgc3R5bGVVcmwgaW5cbiAgICAvLyBDb21waWxlVGVtcGxhdGVNZXRhZGF0YS5leHRlcm5hbFN0eWxlc2hlZXRzIHdpdGggYWxsIHN0eWxlcyBpbmxpbmVkXG4gICAgLy8gLSBpbmxpbmUgYWxsIHN0eWxlcyB0aGF0IGFyZSByZWZlcmVuY2VkIGJ5IHRoZSB0ZW1wbGF0ZSBpbnRvIENvbXBpbGVUZW1wbGF0ZU1ldGFkYXRhLnN0eWxlcy5cbiAgICAvLyBSZWFzb246IGJlIGFibGUgdG8gZGV0ZXJtaW5lIGhvdyBtYW55IHN0eWxlc2hlZXRzIHRoZXJlIGFyZSBldmVuIHdpdGhvdXQgbG9hZGluZ1xuICAgIC8vIHRoZSB0ZW1wbGF0ZSBub3IgdGhlIHN0eWxlc2hlZXRzLCBzbyB3ZSBjYW4gY3JlYXRlIGEgc3R1YiBmb3IgVHlwZVNjcmlwdCBhbHdheXMgc3luY2hyb25vdXNseVxuICAgIC8vIChhcyByZXNvdXJjZSBsb2FkaW5nIG1heSBiZSBhc3luYylcblxuICAgIGNvbnN0IHN0eWxlcyA9IFsuLi5wcmVwYXJzZWRUZW1wbGF0ZS5zdHlsZXNdO1xuICAgIHRoaXMuX2lubGluZVN0eWxlcyhwcmVwYXJzZWRUZW1wbGF0ZS5pbmxpbmVTdHlsZVVybHMsIHN0eWxlc2hlZXRzLCBzdHlsZXMpO1xuICAgIGNvbnN0IHN0eWxlVXJscyA9IHByZXBhcnNlZFRlbXBsYXRlLnN0eWxlVXJscztcblxuICAgIGNvbnN0IGV4dGVybmFsU3R5bGVzaGVldHMgPSBzdHlsZVVybHMubWFwKHN0eWxlVXJsID0+IHtcbiAgICAgIGNvbnN0IHN0eWxlc2hlZXQgPSBzdHlsZXNoZWV0cy5nZXQoc3R5bGVVcmwpICE7XG4gICAgICBjb25zdCBzdHlsZXMgPSBbLi4uc3R5bGVzaGVldC5zdHlsZXNdO1xuICAgICAgdGhpcy5faW5saW5lU3R5bGVzKHN0eWxlc2hlZXQuc3R5bGVVcmxzLCBzdHlsZXNoZWV0cywgc3R5bGVzKTtcbiAgICAgIHJldHVybiBuZXcgQ29tcGlsZVN0eWxlc2hlZXRNZXRhZGF0YSh7bW9kdWxlVXJsOiBzdHlsZVVybCwgc3R5bGVzOiBzdHlsZXN9KTtcbiAgICB9KTtcblxuICAgIGxldCBlbmNhcHN1bGF0aW9uID0gcHJlbm9ybURhdGEuZW5jYXBzdWxhdGlvbjtcbiAgICBpZiAoZW5jYXBzdWxhdGlvbiA9PSBudWxsKSB7XG4gICAgICBlbmNhcHN1bGF0aW9uID0gdGhpcy5fY29uZmlnLmRlZmF1bHRFbmNhcHN1bGF0aW9uO1xuICAgIH1cbiAgICBpZiAoZW5jYXBzdWxhdGlvbiA9PT0gVmlld0VuY2Fwc3VsYXRpb24uRW11bGF0ZWQgJiYgc3R5bGVzLmxlbmd0aCA9PT0gMCAmJlxuICAgICAgICBzdHlsZVVybHMubGVuZ3RoID09PSAwKSB7XG4gICAgICBlbmNhcHN1bGF0aW9uID0gVmlld0VuY2Fwc3VsYXRpb24uTm9uZTtcbiAgICB9XG4gICAgcmV0dXJuIG5ldyBDb21waWxlVGVtcGxhdGVNZXRhZGF0YSh7XG4gICAgICBlbmNhcHN1bGF0aW9uLFxuICAgICAgdGVtcGxhdGU6IHByZXBhcnNlZFRlbXBsYXRlLnRlbXBsYXRlLFxuICAgICAgdGVtcGxhdGVVcmw6IHByZXBhcnNlZFRlbXBsYXRlLnRlbXBsYXRlVXJsLFxuICAgICAgaHRtbEFzdDogcHJlcGFyc2VkVGVtcGxhdGUuaHRtbEFzdCwgc3R5bGVzLCBzdHlsZVVybHMsXG4gICAgICBuZ0NvbnRlbnRTZWxlY3RvcnM6IHByZXBhcnNlZFRlbXBsYXRlLm5nQ29udGVudFNlbGVjdG9ycyxcbiAgICAgIGFuaW1hdGlvbnM6IHByZW5vcm1EYXRhLmFuaW1hdGlvbnMsXG4gICAgICBpbnRlcnBvbGF0aW9uOiBwcmVub3JtRGF0YS5pbnRlcnBvbGF0aW9uLFxuICAgICAgaXNJbmxpbmU6IHByZXBhcnNlZFRlbXBsYXRlLmlzSW5saW5lLCBleHRlcm5hbFN0eWxlc2hlZXRzLFxuICAgICAgcHJlc2VydmVXaGl0ZXNwYWNlczogcHJlc2VydmVXaGl0ZXNwYWNlc0RlZmF1bHQoXG4gICAgICAgICAgcHJlbm9ybURhdGEucHJlc2VydmVXaGl0ZXNwYWNlcywgdGhpcy5fY29uZmlnLnByZXNlcnZlV2hpdGVzcGFjZXMpLFxuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfaW5saW5lU3R5bGVzKFxuICAgICAgc3R5bGVVcmxzOiBzdHJpbmdbXSwgc3R5bGVzaGVldHM6IE1hcDxzdHJpbmcsIENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGE+LFxuICAgICAgdGFyZ2V0U3R5bGVzOiBzdHJpbmdbXSkge1xuICAgIHN0eWxlVXJscy5mb3JFYWNoKHN0eWxlVXJsID0+IHtcbiAgICAgIGNvbnN0IHN0eWxlc2hlZXQgPSBzdHlsZXNoZWV0cy5nZXQoc3R5bGVVcmwpICE7XG4gICAgICBzdHlsZXNoZWV0LnN0eWxlcy5mb3JFYWNoKHN0eWxlID0+IHRhcmdldFN0eWxlcy5wdXNoKHN0eWxlKSk7XG4gICAgICB0aGlzLl9pbmxpbmVTdHlsZXMoc3R5bGVzaGVldC5zdHlsZVVybHMsIHN0eWxlc2hlZXRzLCB0YXJnZXRTdHlsZXMpO1xuICAgIH0pO1xuICB9XG5cbiAgcHJpdmF0ZSBfbG9hZE1pc3NpbmdFeHRlcm5hbFN0eWxlc2hlZXRzKFxuICAgICAgc3R5bGVVcmxzOiBzdHJpbmdbXSxcbiAgICAgIGxvYWRlZFN0eWxlc2hlZXRzOlxuICAgICAgICAgIE1hcDxzdHJpbmcsIENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGE+ID0gbmV3IE1hcDxzdHJpbmcsIENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGE+KCkpOlxuICAgICAgU3luY0FzeW5jPE1hcDxzdHJpbmcsIENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGE+PiB7XG4gICAgcmV0dXJuIFN5bmNBc3luYy50aGVuKFxuICAgICAgICBTeW5jQXN5bmMuYWxsKHN0eWxlVXJscy5maWx0ZXIoKHN0eWxlVXJsKSA9PiAhbG9hZGVkU3R5bGVzaGVldHMuaGFzKHN0eWxlVXJsKSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgLm1hcChcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHN0eWxlVXJsID0+IFN5bmNBc3luYy50aGVuKFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMuX2ZldGNoKHN0eWxlVXJsKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAobG9hZGVkU3R5bGUpID0+IHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnN0IHN0eWxlc2hlZXQgPVxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMuX25vcm1hbGl6ZVN0eWxlc2hlZXQobmV3IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEoXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHtzdHlsZXM6IFtsb2FkZWRTdHlsZV0sIG1vZHVsZVVybDogc3R5bGVVcmx9KSk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBsb2FkZWRTdHlsZXNoZWV0cy5zZXQoc3R5bGVVcmwsIHN0eWxlc2hlZXQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIHRoaXMuX2xvYWRNaXNzaW5nRXh0ZXJuYWxTdHlsZXNoZWV0cyhcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBzdHlsZXNoZWV0LnN0eWxlVXJscywgbG9hZGVkU3R5bGVzaGVldHMpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0pKSksXG4gICAgICAgIChfKSA9PiBsb2FkZWRTdHlsZXNoZWV0cyk7XG4gIH1cblxuICBwcml2YXRlIF9ub3JtYWxpemVTdHlsZXNoZWV0KHN0eWxlc2hlZXQ6IENvbXBpbGVTdHlsZXNoZWV0TWV0YWRhdGEpOiBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhIHtcbiAgICBjb25zdCBtb2R1bGVVcmwgPSBzdHlsZXNoZWV0Lm1vZHVsZVVybCAhO1xuICAgIGNvbnN0IGFsbFN0eWxlVXJscyA9IHN0eWxlc2hlZXQuc3R5bGVVcmxzLmZpbHRlcihpc1N0eWxlVXJsUmVzb2x2YWJsZSlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLm1hcCh1cmwgPT4gdGhpcy5fdXJsUmVzb2x2ZXIucmVzb2x2ZShtb2R1bGVVcmwsIHVybCkpO1xuXG4gICAgY29uc3QgYWxsU3R5bGVzID0gc3R5bGVzaGVldC5zdHlsZXMubWFwKHN0eWxlID0+IHtcbiAgICAgIGNvbnN0IHN0eWxlV2l0aEltcG9ydHMgPSBleHRyYWN0U3R5bGVVcmxzKHRoaXMuX3VybFJlc29sdmVyLCBtb2R1bGVVcmwsIHN0eWxlKTtcbiAgICAgIGFsbFN0eWxlVXJscy5wdXNoKC4uLnN0eWxlV2l0aEltcG9ydHMuc3R5bGVVcmxzKTtcbiAgICAgIHJldHVybiBzdHlsZVdpdGhJbXBvcnRzLnN0eWxlO1xuICAgIH0pO1xuXG4gICAgcmV0dXJuIG5ldyBDb21waWxlU3R5bGVzaGVldE1ldGFkYXRhKFxuICAgICAgICB7c3R5bGVzOiBhbGxTdHlsZXMsIHN0eWxlVXJsczogYWxsU3R5bGVVcmxzLCBtb2R1bGVVcmw6IG1vZHVsZVVybH0pO1xuICB9XG59XG5cbmludGVyZmFjZSBQcmVwYXJzZWRUZW1wbGF0ZSB7XG4gIHRlbXBsYXRlOiBzdHJpbmc7XG4gIHRlbXBsYXRlVXJsOiBzdHJpbmc7XG4gIGlzSW5saW5lOiBib29sZWFuO1xuICBodG1sQXN0OiBIdG1sUGFyc2VUcmVlUmVzdWx0O1xuICBzdHlsZXM6IHN0cmluZ1tdO1xuICBpbmxpbmVTdHlsZVVybHM6IHN0cmluZ1tdO1xuICBzdHlsZVVybHM6IHN0cmluZ1tdO1xuICBuZ0NvbnRlbnRTZWxlY3RvcnM6IHN0cmluZ1tdO1xufVxuXG5jbGFzcyBUZW1wbGF0ZVByZXBhcnNlVmlzaXRvciBpbXBsZW1lbnRzIGh0bWwuVmlzaXRvciB7XG4gIG5nQ29udGVudFNlbGVjdG9yczogc3RyaW5nW10gPSBbXTtcbiAgc3R5bGVzOiBzdHJpbmdbXSA9IFtdO1xuICBzdHlsZVVybHM6IHN0cmluZ1tdID0gW107XG4gIG5nTm9uQmluZGFibGVTdGFja0NvdW50OiBudW1iZXIgPSAwO1xuXG4gIHZpc2l0RWxlbWVudChhc3Q6IGh0bWwuRWxlbWVudCwgY29udGV4dDogYW55KTogYW55IHtcbiAgICBjb25zdCBwcmVwYXJzZWRFbGVtZW50ID0gcHJlcGFyc2VFbGVtZW50KGFzdCk7XG4gICAgc3dpdGNoIChwcmVwYXJzZWRFbGVtZW50LnR5cGUpIHtcbiAgICAgIGNhc2UgUHJlcGFyc2VkRWxlbWVudFR5cGUuTkdfQ09OVEVOVDpcbiAgICAgICAgaWYgKHRoaXMubmdOb25CaW5kYWJsZVN0YWNrQ291bnQgPT09IDApIHtcbiAgICAgICAgICB0aGlzLm5nQ29udGVudFNlbGVjdG9ycy5wdXNoKHByZXBhcnNlZEVsZW1lbnQuc2VsZWN0QXR0cik7XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIFByZXBhcnNlZEVsZW1lbnRUeXBlLlNUWUxFOlxuICAgICAgICBsZXQgdGV4dENvbnRlbnQgPSAnJztcbiAgICAgICAgYXN0LmNoaWxkcmVuLmZvckVhY2goY2hpbGQgPT4ge1xuICAgICAgICAgIGlmIChjaGlsZCBpbnN0YW5jZW9mIGh0bWwuVGV4dCkge1xuICAgICAgICAgICAgdGV4dENvbnRlbnQgKz0gY2hpbGQudmFsdWU7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgICAgdGhpcy5zdHlsZXMucHVzaCh0ZXh0Q29udGVudCk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBQcmVwYXJzZWRFbGVtZW50VHlwZS5TVFlMRVNIRUVUOlxuICAgICAgICB0aGlzLnN0eWxlVXJscy5wdXNoKHByZXBhcnNlZEVsZW1lbnQuaHJlZkF0dHIpO1xuICAgICAgICBicmVhaztcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIGJyZWFrO1xuICAgIH1cbiAgICBpZiAocHJlcGFyc2VkRWxlbWVudC5ub25CaW5kYWJsZSkge1xuICAgICAgdGhpcy5uZ05vbkJpbmRhYmxlU3RhY2tDb3VudCsrO1xuICAgIH1cbiAgICBodG1sLnZpc2l0QWxsKHRoaXMsIGFzdC5jaGlsZHJlbik7XG4gICAgaWYgKHByZXBhcnNlZEVsZW1lbnQubm9uQmluZGFibGUpIHtcbiAgICAgIHRoaXMubmdOb25CaW5kYWJsZVN0YWNrQ291bnQtLTtcbiAgICB9XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cblxuICB2aXNpdEV4cGFuc2lvbihhc3Q6IGh0bWwuRXhwYW5zaW9uLCBjb250ZXh0OiBhbnkpOiBhbnkgeyBodG1sLnZpc2l0QWxsKHRoaXMsIGFzdC5jYXNlcyk7IH1cblxuICB2aXNpdEV4cGFuc2lvbkNhc2UoYXN0OiBodG1sLkV4cGFuc2lvbkNhc2UsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgaHRtbC52aXNpdEFsbCh0aGlzLCBhc3QuZXhwcmVzc2lvbik7XG4gIH1cblxuICB2aXNpdENvbW1lbnQoYXN0OiBodG1sLkNvbW1lbnQsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiBudWxsOyB9XG4gIHZpc2l0QXR0cmlidXRlKGFzdDogaHRtbC5BdHRyaWJ1dGUsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiBudWxsOyB9XG4gIHZpc2l0VGV4dChhc3Q6IGh0bWwuVGV4dCwgY29udGV4dDogYW55KTogYW55IHsgcmV0dXJuIG51bGw7IH1cbn1cbiJdfQ==