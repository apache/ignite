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
        define("@angular/compiler/src/aot/compiler_factory", ["require", "exports", "@angular/compiler/src/config", "@angular/compiler/src/core", "@angular/compiler/src/directive_normalizer", "@angular/compiler/src/directive_resolver", "@angular/compiler/src/expression_parser/lexer", "@angular/compiler/src/expression_parser/parser", "@angular/compiler/src/i18n/i18n_html_parser", "@angular/compiler/src/injectable_compiler", "@angular/compiler/src/metadata_resolver", "@angular/compiler/src/ml_parser/html_parser", "@angular/compiler/src/ng_module_compiler", "@angular/compiler/src/ng_module_resolver", "@angular/compiler/src/output/ts_emitter", "@angular/compiler/src/pipe_resolver", "@angular/compiler/src/schema/dom_element_schema_registry", "@angular/compiler/src/style_compiler", "@angular/compiler/src/template_parser/template_parser", "@angular/compiler/src/util", "@angular/compiler/src/view_compiler/type_check_compiler", "@angular/compiler/src/view_compiler/view_compiler", "@angular/compiler/src/aot/compiler", "@angular/compiler/src/aot/static_reflector", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/aot/static_symbol_resolver", "@angular/compiler/src/aot/summary_resolver"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var config_1 = require("@angular/compiler/src/config");
    var core_1 = require("@angular/compiler/src/core");
    var directive_normalizer_1 = require("@angular/compiler/src/directive_normalizer");
    var directive_resolver_1 = require("@angular/compiler/src/directive_resolver");
    var lexer_1 = require("@angular/compiler/src/expression_parser/lexer");
    var parser_1 = require("@angular/compiler/src/expression_parser/parser");
    var i18n_html_parser_1 = require("@angular/compiler/src/i18n/i18n_html_parser");
    var injectable_compiler_1 = require("@angular/compiler/src/injectable_compiler");
    var metadata_resolver_1 = require("@angular/compiler/src/metadata_resolver");
    var html_parser_1 = require("@angular/compiler/src/ml_parser/html_parser");
    var ng_module_compiler_1 = require("@angular/compiler/src/ng_module_compiler");
    var ng_module_resolver_1 = require("@angular/compiler/src/ng_module_resolver");
    var ts_emitter_1 = require("@angular/compiler/src/output/ts_emitter");
    var pipe_resolver_1 = require("@angular/compiler/src/pipe_resolver");
    var dom_element_schema_registry_1 = require("@angular/compiler/src/schema/dom_element_schema_registry");
    var style_compiler_1 = require("@angular/compiler/src/style_compiler");
    var template_parser_1 = require("@angular/compiler/src/template_parser/template_parser");
    var util_1 = require("@angular/compiler/src/util");
    var type_check_compiler_1 = require("@angular/compiler/src/view_compiler/type_check_compiler");
    var view_compiler_1 = require("@angular/compiler/src/view_compiler/view_compiler");
    var compiler_1 = require("@angular/compiler/src/aot/compiler");
    var static_reflector_1 = require("@angular/compiler/src/aot/static_reflector");
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var static_symbol_resolver_1 = require("@angular/compiler/src/aot/static_symbol_resolver");
    var summary_resolver_1 = require("@angular/compiler/src/aot/summary_resolver");
    function createAotUrlResolver(host) {
        return {
            resolve: function (basePath, url) {
                var filePath = host.resourceNameToFileName(url, basePath);
                if (!filePath) {
                    throw util_1.syntaxError("Couldn't resolve resource " + url + " from " + basePath);
                }
                return filePath;
            }
        };
    }
    exports.createAotUrlResolver = createAotUrlResolver;
    /**
     * Creates a new AotCompiler based on options and a host.
     */
    function createAotCompiler(compilerHost, options, errorCollector) {
        var translations = options.translations || '';
        var urlResolver = createAotUrlResolver(compilerHost);
        var symbolCache = new static_symbol_1.StaticSymbolCache();
        var summaryResolver = new summary_resolver_1.AotSummaryResolver(compilerHost, symbolCache);
        var symbolResolver = new static_symbol_resolver_1.StaticSymbolResolver(compilerHost, symbolCache, summaryResolver);
        var staticReflector = new static_reflector_1.StaticReflector(summaryResolver, symbolResolver, [], [], errorCollector);
        var htmlParser;
        if (!!options.enableIvy) {
            // Ivy handles i18n at the compiler level so we must use a regular parser
            htmlParser = new html_parser_1.HtmlParser();
        }
        else {
            htmlParser = new i18n_html_parser_1.I18NHtmlParser(new html_parser_1.HtmlParser(), translations, options.i18nFormat, options.missingTranslation, console);
        }
        var config = new config_1.CompilerConfig({
            defaultEncapsulation: core_1.ViewEncapsulation.Emulated,
            useJit: false,
            missingTranslation: options.missingTranslation,
            preserveWhitespaces: options.preserveWhitespaces,
            strictInjectionParameters: options.strictInjectionParameters,
        });
        var normalizer = new directive_normalizer_1.DirectiveNormalizer({ get: function (url) { return compilerHost.loadResource(url); } }, urlResolver, htmlParser, config);
        var expressionParser = new parser_1.Parser(new lexer_1.Lexer());
        var elementSchemaRegistry = new dom_element_schema_registry_1.DomElementSchemaRegistry();
        var tmplParser = new template_parser_1.TemplateParser(config, staticReflector, expressionParser, elementSchemaRegistry, htmlParser, console, []);
        var resolver = new metadata_resolver_1.CompileMetadataResolver(config, htmlParser, new ng_module_resolver_1.NgModuleResolver(staticReflector), new directive_resolver_1.DirectiveResolver(staticReflector), new pipe_resolver_1.PipeResolver(staticReflector), summaryResolver, elementSchemaRegistry, normalizer, console, symbolCache, staticReflector, errorCollector);
        // TODO(vicb): do not pass options.i18nFormat here
        var viewCompiler = new view_compiler_1.ViewCompiler(staticReflector);
        var typeCheckCompiler = new type_check_compiler_1.TypeCheckCompiler(options, staticReflector);
        var compiler = new compiler_1.AotCompiler(config, options, compilerHost, staticReflector, resolver, tmplParser, new style_compiler_1.StyleCompiler(urlResolver), viewCompiler, typeCheckCompiler, new ng_module_compiler_1.NgModuleCompiler(staticReflector), new injectable_compiler_1.InjectableCompiler(staticReflector, !!options.enableIvy), new ts_emitter_1.TypeScriptEmitter(), summaryResolver, symbolResolver);
        return { compiler: compiler, reflector: staticReflector };
    }
    exports.createAotCompiler = createAotCompiler;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29tcGlsZXJfZmFjdG9yeS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9hb3QvY29tcGlsZXJfZmFjdG9yeS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHVEQUF5QztJQUN6QyxtREFBc0U7SUFDdEUsbUZBQTREO0lBQzVELCtFQUF3RDtJQUN4RCx1RUFBaUQ7SUFDakQseUVBQW1EO0lBQ25ELGdGQUF3RDtJQUN4RCxpRkFBMEQ7SUFDMUQsNkVBQTZEO0lBQzdELDJFQUFvRDtJQUNwRCwrRUFBdUQ7SUFDdkQsK0VBQXVEO0lBQ3ZELHNFQUF1RDtJQUN2RCxxRUFBOEM7SUFDOUMsd0dBQStFO0lBQy9FLHVFQUFnRDtJQUNoRCx5RkFBa0U7SUFFbEUsbURBQW9DO0lBQ3BDLCtGQUF1RTtJQUN2RSxtRkFBNEQ7SUFFNUQsK0RBQXVDO0lBR3ZDLCtFQUFtRDtJQUNuRCx5RUFBZ0U7SUFDaEUsMkZBQThEO0lBQzlELCtFQUFzRDtJQUV0RCxTQUFnQixvQkFBb0IsQ0FBQyxJQUVwQztRQUNDLE9BQU87WUFDTCxPQUFPLEVBQUUsVUFBQyxRQUFnQixFQUFFLEdBQVc7Z0JBQ3JDLElBQU0sUUFBUSxHQUFHLElBQUksQ0FBQyxzQkFBc0IsQ0FBQyxHQUFHLEVBQUUsUUFBUSxDQUFDLENBQUM7Z0JBQzVELElBQUksQ0FBQyxRQUFRLEVBQUU7b0JBQ2IsTUFBTSxrQkFBVyxDQUFDLCtCQUE2QixHQUFHLGNBQVMsUUFBVSxDQUFDLENBQUM7aUJBQ3hFO2dCQUNELE9BQU8sUUFBUSxDQUFDO1lBQ2xCLENBQUM7U0FDRixDQUFDO0lBQ0osQ0FBQztJQVpELG9EQVlDO0lBRUQ7O09BRUc7SUFDSCxTQUFnQixpQkFBaUIsQ0FDN0IsWUFBNkIsRUFBRSxPQUEyQixFQUMxRCxjQUNRO1FBQ1YsSUFBSSxZQUFZLEdBQVcsT0FBTyxDQUFD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SUFBSSw4QkFBaUIsRUFBRSxFQUNyRixlQUFlLEVBQUUsY0FBYyxDQUFDLENBQUM7UUFDckMsT0FBTyxFQUFDLFFBQVEsVUFBQSxFQUFFLFNBQVMsRUFBRSxlQUFlLEVBQUMsQ0FBQztJQUNoRCxDQUFDO0lBL0NELDhDQStDQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtDb21waWxlckNvbmZpZ30gZnJvbSAnLi4vY29uZmlnJztcbmltcG9ydCB7TWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3ksIFZpZXdFbmNhcHN1bGF0aW9ufSBmcm9tICcuLi9jb3JlJztcbmltcG9ydCB7RGlyZWN0aXZlTm9ybWFsaXplcn0gZnJvbSAnLi4vZGlyZWN0aXZlX25vcm1hbGl6ZXInO1xuaW1wb3J0IHtEaXJlY3RpdmVSZXNvbHZlcn0gZnJvbSAnLi4vZGlyZWN0aXZlX3Jlc29sdmVyJztcbmltcG9ydCB7TGV4ZXJ9IGZyb20gJy4uL2V4cHJlc3Npb25fcGFyc2VyL2xleGVyJztcbmltcG9ydCB7UGFyc2VyfSBmcm9tICcuLi9leHByZXNzaW9uX3BhcnNlci9wYXJzZXInO1xuaW1wb3J0IHtJMThOSHRtbFBhcnNlcn0gZnJvbSAnLi4vaTE4bi9pMThuX2h0bWxfcGFyc2VyJztcbmltcG9ydCB7SW5qZWN0YWJsZUNvbXBpbGVyfSBmcm9tICcuLi9pbmplY3RhYmxlX2NvbXBpbGVyJztcbmltcG9ydCB7Q29tcGlsZU1ldGFkYXRhUmVzb2x2ZXJ9IGZyb20gJy4uL21ldGFkYXRhX3Jlc29sdmVyJztcbmltcG9ydCB7SHRtbFBhcnNlcn0gZnJvbSAnLi4vbWxfcGFyc2VyL2h0bWxfcGFyc2VyJztcbmltcG9ydCB7TmdNb2R1bGVDb21waWxlcn0gZnJvbSAnLi4vbmdfbW9kdWxlX2NvbXBpbGVyJztcbmltcG9ydCB7TmdNb2R1bGVSZXNvbHZlcn0gZnJvbSAnLi4vbmdfbW9kdWxlX3Jlc29sdmVyJztcbmltcG9ydCB7VHlwZVNjcmlwdEVtaXR0ZXJ9IGZyb20gJy4uL291dHB1dC90c19lbWl0dGVyJztcbmltcG9ydCB7UGlwZVJlc29sdmVyfSBmcm9tICcuLi9waXBlX3Jlc29sdmVyJztcbmltcG9ydCB7RG9tRWxlbWVudFNjaGVtYVJlZ2lzdHJ5fSBmcm9tICcuLi9zY2hlbWEvZG9tX2VsZW1lbnRfc2NoZW1hX3JlZ2lzdHJ5JztcbmltcG9ydCB7U3R5bGVDb21waWxlcn0gZnJvbSAnLi4vc3R5bGVfY29tcGlsZXInO1xuaW1wb3J0IHtUZW1wbGF0ZVBhcnNlcn0gZnJvbSAnLi4vdGVtcGxhdGVfcGFyc2VyL3RlbXBsYXRlX3BhcnNlcic7XG5pbXBvcnQge1VybFJlc29sdmVyfSBmcm9tICcuLi91cmxfcmVzb2x2ZXInO1xuaW1wb3J0IHtzeW50YXhFcnJvcn0gZnJvbSAnLi4vdXRpbCc7XG5pbXBvcnQge1R5cGVDaGVja0NvbXBpbGVyfSBmcm9tICcuLi92aWV3X2NvbXBpbGVyL3R5cGVfY2hlY2tfY29tcGlsZXInO1xuaW1wb3J0IHtWaWV3Q29tcGlsZXJ9IGZyb20gJy4uL3ZpZXdfY29tcGlsZXIvdmlld19jb21waWxlcic7XG5cbmltcG9ydCB7QW90Q29tcGlsZXJ9IGZyb20gJy4vY29tcGlsZXInO1xuaW1wb3J0IHtBb3RDb21waWxlckhvc3R9IGZyb20gJy4vY29tcGlsZXJfaG9zdCc7XG5pbXBvcnQge0FvdENvbXBpbGVyT3B0aW9uc30gZnJvbSAnLi9jb21waWxlcl9vcHRpb25zJztcbmltcG9ydCB7U3RhdGljUmVmbGVjdG9yfSBmcm9tICcuL3N0YXRpY19yZWZsZWN0b3InO1xuaW1wb3J0IHtTdGF0aWNTeW1ib2wsIFN0YXRpY1N5bWJvbENhY2hlfSBmcm9tICcuL3N0YXRpY19zeW1ib2wnO1xuaW1wb3J0IHtTdGF0aWNTeW1ib2xSZXNvbHZlcn0gZnJvbSAnLi9zdGF0aWNfc3ltYm9sX3Jlc29sdmVyJztcbmltcG9ydCB7QW90U3VtbWFyeVJlc29sdmVyfSBmcm9tICcuL3N1bW1hcnlfcmVzb2x2ZXInO1xuXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlQW90VXJsUmVzb2x2ZXIoaG9zdDoge1xuICByZXNvdXJjZU5hbWVUb0ZpbGVOYW1lKHJlc291cmNlTmFtZTogc3RyaW5nLCBjb250YWluaW5nRmlsZU5hbWU6IHN0cmluZyk6IHN0cmluZyB8IG51bGw7XG59KTogVXJsUmVzb2x2ZXIge1xuICByZXR1cm4ge1xuICAgIHJlc29sdmU6IChiYXNlUGF0aDogc3RyaW5nLCB1cmw6IHN0cmluZykgPT4ge1xuICAgICAgY29uc3QgZmlsZVBhdGggPSBob3N0LnJlc291cmNlTmFtZVRvRmlsZU5hbWUodXJsLCBiYXNlUGF0aCk7XG4gICAgICBpZiAoIWZpbGVQYXRoKSB7XG4gICAgICAgIHRocm93IHN5bnRheEVycm9yKGBDb3VsZG4ndCByZXNvbHZlIHJlc291cmNlICR7dXJsfSBmcm9tICR7YmFzZVBhdGh9YCk7XG4gICAgICB9XG4gICAgICByZXR1cm4gZmlsZVBhdGg7XG4gICAgfVxuICB9O1xufVxuXG4vKipcbiAqIENyZWF0ZXMgYSBuZXcgQW90Q29tcGlsZXIgYmFzZWQgb24gb3B0aW9ucyBhbmQgYSBob3N0LlxuICovXG5leHBvcnQgZnVuY3Rpb24gY3JlYXRlQW90Q29tcGlsZXIoXG4gICAgY29tcGlsZXJIb3N0OiBBb3RDb21waWxlckhvc3QsIG9wdGlvbnM6IEFvdENvbXBpbGVyT3B0aW9ucyxcbiAgICBlcnJvckNvbGxlY3Rvcj86IChlcnJvcjogYW55LCB0eXBlPzogYW55KSA9PlxuICAgICAgICB2b2lkKToge2NvbXBpbGVyOiBBb3RDb21waWxlciwgcmVmbGVjdG9yOiBTdGF0aWNSZWZsZWN0b3J9IHtcbiAgbGV0IHRyYW5zbGF0aW9uczogc3RyaW5nID0gb3B0aW9ucy50cmFuc2xhdGlvbnMgfHwgJyc7XG5cbiAgY29uc3QgdXJsUmVzb2x2ZXIgPSBjcmVhdGVBb3RVcmxSZXNvbHZlcihjb21waWxlckhvc3QpO1xuICBjb25zdCBzeW1ib2xDYWNoZSA9IG5ldyBTdGF0aWNTeW1ib2xDYWNoZSgpO1xuICBjb25zdCBzdW1tYXJ5UmVzb2x2ZXIgPSBuZXcgQW90U3VtbWFyeVJlc29sdmVyKGNvbXBpbGVySG9zdCwgc3ltYm9sQ2FjaGUpO1xuICBjb25zdCBzeW1ib2xSZXNvbHZlciA9IG5ldyBTdGF0aWNTeW1ib2xSZXNvbHZlcihjb21waWxlckhvc3QsIHN5bWJvbENhY2hlLCBzdW1tYXJ5UmVzb2x2ZXIpO1xuICBjb25zdCBzdGF0aWNSZWZsZWN0b3IgPVxuICAgICAgbmV3IFN0YXRpY1JlZmxlY3RvcihzdW1tYXJ5UmVzb2x2ZXIsIHN5bWJvbFJlc29sdmVyLCBbXSwgW10sIGVycm9yQ29sbGVjdG9yKTtcbiAgbGV0IGh0bWxQYXJzZXI6IEkxOE5IdG1sUGFyc2VyO1xuICBpZiAoISFvcHRpb25zLmVuYWJsZUl2eSkge1xuICAgIC8vIEl2eSBoYW5kbGVzIGkxOG4gYXQgdGhlIGNvbXBpbGVyIGxldmVsIHNvIHdlIG11c3QgdXNlIGEgcmVndWxhciBwYXJzZXJcbiAgICBodG1sUGFyc2VyID0gbmV3IEh0bWxQYXJzZXIoKSBhcyBJMThOSHRtbFBhcnNlcjtcbiAgfSBlbHNlIHtcbiAgICBodG1sUGFyc2VyID0gbmV3IEkxOE5IdG1sUGFyc2VyKFxuICAgICAgICBuZXcgSHRtbFBhcnNlcigpLCB0cmFuc2xhdGlvbnMsIG9wdGlvbnMuaTE4bkZvcm1hdCwgb3B0aW9ucy5taXNzaW5nVHJhbnNsYXRpb24sIGNvbnNvbGUpO1xuICB9XG4gIGNvbnN0IGNvbmZpZyA9IG5ldyBDb21waWxlckNvbmZpZyh7XG4gICAgZGVmYXVsdEVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLkVtdWxhdGVkLFxuICAgIHVzZUppdDogZmFsc2UsXG4gICAgbWlzc2luZ1RyYW5zbGF0aW9uOiBvcHRpb25zLm1pc3NpbmdUcmFuc2xhdGlvbixcbiAgICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBvcHRpb25zLnByZXNlcnZlV2hpdGVzcGFjZXMsXG4gICAgc3RyaWN0SW5qZWN0aW9uUGFyYW1ldGVyczogb3B0aW9ucy5zdHJpY3RJbmplY3Rpb25QYXJhbWV0ZXJzLFxuICB9KTtcbiAgY29uc3Qgbm9ybWFsaXplciA9IG5ldyBEaXJlY3RpdmVOb3JtYWxpemVyKFxuICAgICAge2dldDogKHVybDogc3RyaW5nKSA9PiBjb21waWxlckhvc3QubG9hZFJlc291cmNlKHVybCl9LCB1cmxSZXNvbHZlciwgaHRtbFBhcnNlciwgY29uZmlnKTtcbiAgY29uc3QgZXhwcmVzc2lvblBhcnNlciA9IG5ldyBQYXJzZXIobmV3IExleGVyKCkpO1xuICBjb25zdCBlbGVtZW50U2NoZW1hUmVnaXN0cnkgPSBuZXcgRG9tRWxlbWVudFNjaGVtYVJlZ2lzdHJ5KCk7XG4gIGNvbnN0IHRtcGxQYXJzZXIgPSBuZXcgVGVtcGxhdGVQYXJzZXIoXG4gICAgICBjb25maWcsIHN0YXRpY1JlZmxlY3RvciwgZXhwcmVzc2lvblBhcnNlciwgZWxlbWVudFNjaGVtYVJlZ2lzdHJ5LCBodG1sUGFyc2VyLCBjb25zb2xlLCBbXSk7XG4gIGNvbnN0IHJlc29sdmVyID0gbmV3IENvbXBpbGVNZXRhZGF0YVJlc29sdmVyKFxuICAgICAgY29uZmlnLCBodG1sUGFyc2VyLCBuZXcgTmdNb2R1bGVSZXNvbHZlcihzdGF0aWNSZWZsZWN0b3IpLFxuICAgICAgbmV3IERpcmVjdGl2ZVJlc29sdmVyKHN0YXRpY1JlZmxlY3RvciksIG5ldyBQaXBlUmVzb2x2ZXIoc3RhdGljUmVmbGVjdG9yKSwgc3VtbWFyeVJlc29sdmVyLFxuICAgICAgZWxlbWVudFNjaGVtYVJlZ2lzdHJ5LCBub3JtYWxpemVyLCBjb25zb2xlLCBzeW1ib2xDYWNoZSwgc3RhdGljUmVmbGVjdG9yLCBlcnJvckNvbGxlY3Rvcik7XG4gIC8vIFRPRE8odmljYik6IGRvIG5vdCBwYXNzIG9wdGlvbnMuaTE4bkZvcm1hdCBoZXJlXG4gIGNvbnN0IHZpZXdDb21waWxlciA9IG5ldyBWaWV3Q29tcGlsZXIoc3RhdGljUmVmbGVjdG9yKTtcbiAgY29uc3QgdHlwZUNoZWNrQ29tcGlsZXIgPSBuZXcgVHlwZUNoZWNrQ29tcGlsZXIob3B0aW9ucywgc3RhdGljUmVmbGVjdG9yKTtcbiAgY29uc3QgY29tcGlsZXIgPSBuZXcgQW90Q29tcGlsZXIoXG4gICAgICBjb25maWcsIG9wdGlvbnMsIGNvbXBpbGVySG9zdCwgc3RhdGljUmVmbGVjdG9yLCByZXNvbHZlciwgdG1wbFBhcnNlcixcbiAgICAgIG5ldyBTdHlsZUNvbXBpbGVyKHVybFJlc29sdmVyKSwgdmlld0NvbXBpbGVyLCB0eXBlQ2hlY2tDb21waWxlcixcbiAgICAgIG5ldyBOZ01vZHVsZUNvbXBpbGVyKHN0YXRpY1JlZmxlY3RvciksXG4gICAgICBuZXcgSW5qZWN0YWJsZUNvbXBpbGVyKHN0YXRpY1JlZmxlY3RvciwgISFvcHRpb25zLmVuYWJsZUl2eSksIG5ldyBUeXBlU2NyaXB0RW1pdHRlcigpLFxuICAgICAgc3VtbWFyeVJlc29sdmVyLCBzeW1ib2xSZXNvbHZlcik7XG4gIHJldHVybiB7Y29tcGlsZXIsIHJlZmxlY3Rvcjogc3RhdGljUmVmbGVjdG9yfTtcbn1cbiJdfQ==