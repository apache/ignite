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
        define("@angular/compiler/src/i18n/extractor", ["require", "exports", "tslib", "@angular/compiler/src/aot/compiler", "@angular/compiler/src/aot/compiler_factory", "@angular/compiler/src/aot/static_reflector", "@angular/compiler/src/aot/static_symbol", "@angular/compiler/src/aot/static_symbol_resolver", "@angular/compiler/src/aot/summary_resolver", "@angular/compiler/src/config", "@angular/compiler/src/core", "@angular/compiler/src/directive_normalizer", "@angular/compiler/src/directive_resolver", "@angular/compiler/src/metadata_resolver", "@angular/compiler/src/ml_parser/html_parser", "@angular/compiler/src/ml_parser/interpolation_config", "@angular/compiler/src/ng_module_resolver", "@angular/compiler/src/pipe_resolver", "@angular/compiler/src/schema/dom_element_schema_registry", "@angular/compiler/src/i18n/message_bundle"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    /**
     * Extract i18n messages from source code
     */
    var compiler_1 = require("@angular/compiler/src/aot/compiler");
    var compiler_factory_1 = require("@angular/compiler/src/aot/compiler_factory");
    var static_reflector_1 = require("@angular/compiler/src/aot/static_reflector");
    var static_symbol_1 = require("@angular/compiler/src/aot/static_symbol");
    var static_symbol_resolver_1 = require("@angular/compiler/src/aot/static_symbol_resolver");
    var summary_resolver_1 = require("@angular/compiler/src/aot/summary_resolver");
    var config_1 = require("@angular/compiler/src/config");
    var core_1 = require("@angular/compiler/src/core");
    var directive_normalizer_1 = require("@angular/compiler/src/directive_normalizer");
    var directive_resolver_1 = require("@angular/compiler/src/directive_resolver");
    var metadata_resolver_1 = require("@angular/compiler/src/metadata_resolver");
    var html_parser_1 = require("@angular/compiler/src/ml_parser/html_parser");
    var interpolation_config_1 = require("@angular/compiler/src/ml_parser/interpolation_config");
    var ng_module_resolver_1 = require("@angular/compiler/src/ng_module_resolver");
    var pipe_resolver_1 = require("@angular/compiler/src/pipe_resolver");
    var dom_element_schema_registry_1 = require("@angular/compiler/src/schema/dom_element_schema_registry");
    var message_bundle_1 = require("@angular/compiler/src/i18n/message_bundle");
    var Extractor = /** @class */ (function () {
        function Extractor(host, staticSymbolResolver, messageBundle, metadataResolver) {
            this.host = host;
            this.staticSymbolResolver = staticSymbolResolver;
            this.messageBundle = messageBundle;
            this.metadataResolver = metadataResolver;
        }
        Extractor.prototype.extract = function (rootFiles) {
            var _this = this;
            var _a = compiler_1.analyzeAndValidateNgModules(rootFiles, this.host, this.staticSymbolResolver, this.metadataResolver), files = _a.files, ngModules = _a.ngModules;
            return Promise
                .all(ngModules.map(function (ngModule) { return _this.metadataResolver.loadNgModuleDirectiveAndPipeMetadata(ngModule.type.reference, false); }))
                .then(function () {
                var errors = [];
                files.forEach(function (file) {
                    var compMetas = [];
                    file.directives.forEach(function (directiveType) {
                        var dirMeta = _this.metadataResolver.getDirectiveMetadata(directiveType);
                        if (dirMeta && dirMeta.isComponent) {
                            compMetas.push(dirMeta);
                        }
                    });
                    compMetas.forEach(function (compMeta) {
                        var html = compMeta.template.template;
                        // Template URL points to either an HTML or TS file depending on
                        // whether the file is used with `templateUrl:` or `template:`,
                        // respectively.
                        var templateUrl = compMeta.template.templateUrl;
                        var interpolationConfig = interpolation_config_1.InterpolationConfig.fromArray(compMeta.template.interpolation);
                        errors.push.apply(errors, tslib_1.__spread(_this.messageBundle.updateFromTemplate(html, templateUrl, interpolationConfig)));
                    });
                });
                if (errors.length) {
                    throw new Error(errors.map(function (e) { return e.toString(); }).join('\n'));
                }
                return _this.messageBundle;
            });
        };
        Extractor.create = function (host, locale) {
            var htmlParser = new html_parser_1.HtmlParser();
            var urlResolver = compiler_factory_1.createAotUrlResolver(host);
            var symbolCache = new static_symbol_1.StaticSymbolCache();
            var summaryResolver = new summary_resolver_1.AotSummaryResolver(host, symbolCache);
            var staticSymbolResolver = new static_symbol_resolver_1.StaticSymbolResolver(host, symbolCache, summaryResolver);
            var staticReflector = new static_reflector_1.StaticReflector(summaryResolver, staticSymbolResolver);
            var config = new config_1.CompilerConfig({ defaultEncapsulation: core_1.ViewEncapsulation.Emulated, useJit: false });
            var normalizer = new directive_normalizer_1.DirectiveNormalizer({ get: function (url) { return host.loadResource(url); } }, urlResolver, htmlParser, config);
            var elementSchemaRegistry = new dom_element_schema_registry_1.DomElementSchemaRegistry();
            var resolver = new metadata_resolver_1.CompileMetadataResolver(config, htmlParser, new ng_module_resolver_1.NgModuleResolver(staticReflector), new directive_resolver_1.DirectiveResolver(staticReflector), new pipe_resolver_1.PipeResolver(staticReflector), summaryResolver, elementSchemaRegistry, normalizer, console, symbolCache, staticReflector);
            // TODO(vicb): implicit tags & attributes
            var messageBundle = new message_bundle_1.MessageBundle(htmlParser, [], {}, locale);
            var extractor = new Extractor(host, staticSymbolResolver, messageBundle, resolver);
            return { extractor: extractor, staticReflector: staticReflector };
        };
        return Extractor;
    }());
    exports.Extractor = Extractor;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXh0cmFjdG9yLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2kxOG4vZXh0cmFjdG9yLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUdIOztPQUVHO0lBQ0gsK0RBQTREO0lBQzVELCtFQUE2RDtJQUM3RCwrRUFBd0Q7SUFDeEQseUVBQXVEO0lBQ3ZELDJGQUE2RjtJQUM3RiwrRUFBbUY7SUFFbkYsdURBQXlDO0lBQ3pDLG1EQUEwQztJQUMxQyxtRkFBNEQ7SUFDNUQsK0VBQXdEO0lBQ3hELDZFQUE2RDtJQUM3RCwyRUFBb0Q7SUFDcEQsNkZBQXNFO0lBQ3RFLCtFQUF1RDtJQUV2RCxxRUFBOEM7SUFDOUMsd0dBQStFO0lBRy9FLDRFQUErQztJQW9CL0M7UUFDRSxtQkFDVyxJQUFtQixFQUFVLG9CQUEwQyxFQUN0RSxhQUE0QixFQUFVLGdCQUF5QztZQURoRixTQUFJLEdBQUosSUFBSSxDQUFlO1lBQVUseUJBQW9CLEdBQXBCLG9CQUFvQixDQUFzQjtZQUN0RSxrQkFBYSxHQUFiLGFBQWEsQ0FBZTtZQUFVLHFCQUFnQixHQUFoQixnQkFBZ0IsQ0FBeUI7UUFBRyxDQUFDO1FBRS9GLDJCQUFPLEdBQVAsVUFBUSxTQUFtQjtZQUEzQixpQkFxQ0M7WUFwQ08sSUFBQSxtSEFDcUUsRUFEcEUsZ0JBQUssRUFBRSx3QkFDNkQsQ0FBQztZQUM1RSxPQUFPLE9BQU87aUJBQ1QsR0FBRyxDQUFDLFNBQVMsQ0FBQyxHQUFHLENBQ2QsVUFBQSxRQUFRLElBQUksT0FBQSxLQUFJLENBQUMsZ0JBQWdCLENBQUMsb0NBQW9DLENBQ2xFLFFBQVEsQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFLEtBQUssQ0FBQyxFQUR2QixDQUN1QixDQUFDLENBQUM7aUJBQ3hDLElBQUksQ0FBQztnQkFDSixJQUFNLE1BQU0sR0FBaUIsRUFBRSxDQUFDO2dC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LHFCQUFxQixFQUFFLFVBQVUsRUFBRSxPQUFPLEVBQUUsV0FBVyxFQUFFLGVBQWUsQ0FBQyxDQUFDO1lBRTlFLHlDQUF5QztZQUN6QyxJQUFNLGFBQWEsR0FBRyxJQUFJLDhCQUFhLENBQUMsVUFBVSxFQUFFLEVBQUUsRUFBRSxFQUFFLEVBQUUsTUFBTSxDQUFDLENBQUM7WUFFcEUsSUFBTSxTQUFTLEdBQUcsSUFBSSxTQUFTLENBQUMsSUFBSSxFQUFFLG9CQUFvQixFQUFFLGFBQWEsRUFBRSxRQUFRLENBQUMsQ0FBQztZQUNyRixPQUFPLEVBQUMsU0FBUyxXQUFBLEVBQUUsZUFBZSxpQkFBQSxFQUFDLENBQUM7UUFDdEMsQ0FBQztRQUNILGdCQUFDO0lBQUQsQ0FBQyxBQXZFRCxJQXVFQztJQXZFWSw4QkFBUyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuXG4vKipcbiAqIEV4dHJhY3QgaTE4biBtZXNzYWdlcyBmcm9tIHNvdXJjZSBjb2RlXG4gKi9cbmltcG9ydCB7YW5hbHl6ZUFuZFZhbGlkYXRlTmdNb2R1bGVzfSBmcm9tICcuLi9hb3QvY29tcGlsZXInO1xuaW1wb3J0IHtjcmVhdGVBb3RVcmxSZXNvbHZlcn0gZnJvbSAnLi4vYW90L2NvbXBpbGVyX2ZhY3RvcnknO1xuaW1wb3J0IHtTdGF0aWNSZWZsZWN0b3J9IGZyb20gJy4uL2FvdC9zdGF0aWNfcmVmbGVjdG9yJztcbmltcG9ydCB7U3RhdGljU3ltYm9sQ2FjaGV9IGZyb20gJy4uL2FvdC9zdGF0aWNfc3ltYm9sJztcbmltcG9ydCB7U3RhdGljU3ltYm9sUmVzb2x2ZXIsIFN0YXRpY1N5bWJvbFJlc29sdmVySG9zdH0gZnJvbSAnLi4vYW90L3N0YXRpY19zeW1ib2xfcmVzb2x2ZXInO1xuaW1wb3J0IHtBb3RTdW1tYXJ5UmVzb2x2ZXIsIEFvdFN1bW1hcnlSZXNvbHZlckhvc3R9IGZyb20gJy4uL2FvdC9zdW1tYXJ5X3Jlc29sdmVyJztcbmltcG9ydCB7Q29tcGlsZURpcmVjdGl2ZU1ldGFkYXRhfSBmcm9tICcuLi9jb21waWxlX21ldGFkYXRhJztcbmltcG9ydCB7Q29tcGlsZXJDb25maWd9IGZyb20gJy4uL2NvbmZpZyc7XG5pbXBvcnQge1ZpZXdFbmNhcHN1bGF0aW9ufSBmcm9tICcuLi9jb3JlJztcbmltcG9ydCB7RGlyZWN0aXZlTm9ybWFsaXplcn0gZnJvbSAnLi4vZGlyZWN0aXZlX25vcm1hbGl6ZXInO1xuaW1wb3J0IHtEaXJlY3RpdmVSZXNvbHZlcn0gZnJvbSAnLi4vZGlyZWN0aXZlX3Jlc29sdmVyJztcbmltcG9ydCB7Q29tcGlsZU1ldGFkYXRhUmVzb2x2ZXJ9IGZyb20gJy4uL21ldGFkYXRhX3Jlc29sdmVyJztcbmltcG9ydCB7SHRtbFBhcnNlcn0gZnJvbSAnLi4vbWxfcGFyc2VyL2h0bWxfcGFyc2VyJztcbmltcG9ydCB7SW50ZXJwb2xhdGlvbkNvbmZpZ30gZnJvbSAnLi4vbWxfcGFyc2VyL2ludGVycG9sYXRpb25fY29uZmlnJztcbmltcG9ydCB7TmdNb2R1bGVSZXNvbHZlcn0gZnJvbSAnLi4vbmdfbW9kdWxlX3Jlc29sdmVyJztcbmltcG9ydCB7UGFyc2VFcnJvcn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5pbXBvcnQge1BpcGVSZXNvbHZlcn0gZnJvbSAnLi4vcGlwZV9yZXNvbHZlcic7XG5pbXBvcnQge0RvbUVsZW1lbnRTY2hlbWFSZWdpc3RyeX0gZnJvbSAnLi4vc2NoZW1hL2RvbV9lbGVtZW50X3NjaGVtYV9yZWdpc3RyeSc7XG5pbXBvcnQge3N5bnRheEVycm9yfSBmcm9tICcuLi91dGlsJztcblxuaW1wb3J0IHtNZXNzYWdlQnVuZGxlfSBmcm9tICcuL21lc3NhZ2VfYnVuZGxlJztcblxuXG5cbi8qKlxuICogVGhlIGhvc3Qgb2YgdGhlIEV4dHJhY3RvciBkaXNjb25uZWN0cyB0aGUgaW1wbGVtZW50YXRpb24gZnJvbSBUeXBlU2NyaXB0IC8gb3RoZXIgbGFuZ3VhZ2VcbiAqIHNlcnZpY2VzIGFuZCBmcm9tIHVuZGVybHlpbmcgZmlsZSBzeXN0ZW1zLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIEV4dHJhY3Rvckhvc3QgZXh0ZW5kcyBTdGF0aWNTeW1ib2xSZXNvbHZlckhvc3QsIEFvdFN1bW1hcnlSZXNvbHZlckhvc3Qge1xuICAvKipcbiAgICogQ29udmVydHMgYSBwYXRoIHRoYXQgcmVmZXJzIHRvIGEgcmVzb3VyY2UgaW50byBhbiBhYnNvbHV0ZSBmaWxlUGF0aFxuICAgKiB0aGF0IGNhbiBiZSBsYXRlcm9uIHVzZWQgZm9yIGxvYWRpbmcgdGhlIHJlc291cmNlIHZpYSBgbG9hZFJlc291cmNlLlxuICAgKi9cbiAgcmVzb3VyY2VOYW1lVG9GaWxlTmFtZShwYXRoOiBzdHJpbmcsIGNvbnRhaW5pbmdGaWxlOiBzdHJpbmcpOiBzdHJpbmd8bnVsbDtcbiAgLyoqXG4gICAqIExvYWRzIGEgcmVzb3VyY2UgKGUuZy4gaHRtbCAvIGNzcylcbiAgICovXG4gIGxvYWRSZXNvdXJjZShwYXRoOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZz58c3RyaW5nO1xufVxuXG5leHBvcnQgY2xhc3MgRXh0cmFjdG9yIHtcbiAgY29uc3RydWN0b3IoXG4gICAgICBwdWJsaWMgaG9zdDogRXh0cmFjdG9ySG9zdCwgcHJpdmF0ZSBzdGF0aWNTeW1ib2xSZXNvbHZlcjogU3RhdGljU3ltYm9sUmVzb2x2ZXIsXG4gICAgICBwcml2YXRlIG1lc3NhZ2VCdW5kbGU6IE1lc3NhZ2VCdW5kbGUsIHByaXZhdGUgbWV0YWRhdGFSZXNvbHZlcjogQ29tcGlsZU1ldGFkYXRhUmVzb2x2ZXIpIHt9XG5cbiAgZXh0cmFjdChyb290RmlsZXM6IHN0cmluZ1tdKTogUHJvbWlzZTxNZXNzYWdlQnVuZGxlPiB7XG4gICAgY29uc3Qge2ZpbGVzLCBuZ01vZHVsZXN9ID0gYW5hbHl6ZUFuZFZhbGlkYXRlTmdNb2R1bGVzKFxuICAgICAgICByb290RmlsZXMsIHRoaXMuaG9zdCwgdGhpcy5zdGF0aWNTeW1ib2xSZXNvbHZlciwgdGhpcy5tZXRhZGF0YVJlc29sdmVyKTtcbiAgICByZXR1cm4gUHJvbWlzZVxuICAgICAgICAuYWxsKG5nTW9kdWxlcy5tYXAoXG4gICAgICAgICAgICBuZ01vZHVsZSA9PiB0aGlzLm1ldGFkYXRhUmVzb2x2ZXIubG9hZE5nTW9kdWxlRGlyZWN0aXZlQW5kUGlwZU1ldGFkYXRhKFxuICAgICAgICAgICAgICAgIG5nTW9kdWxlLnR5cGUucmVmZXJlbmNlLCBmYWxzZSkpKVxuICAgICAgICAudGhlbigoKSA9PiB7XG4gICAgICAgICAgY29uc3QgZXJyb3JzOiBQYXJzZUVycm9yW10gPSBbXTtcblxuICAgICAgICAgIGZpbGVzLmZvckVhY2goZmlsZSA9PiB7XG4gICAgICAgICAgICBjb25zdCBjb21wTWV0YXM6IENvbXBpbGVEaXJlY3RpdmVNZXRhZGF0YVtdID0gW107XG4gICAgICAgICAgICBmaWxlLmRpcmVjdGl2ZXMuZm9yRWFjaChkaXJlY3RpdmVUeXBlID0+IHtcbiAgICAgICAgICAgICAgY29uc3QgZGlyTWV0YSA9IHRoaXMubWV0YWRhdGFSZXNvbHZlci5nZXREaXJlY3RpdmVNZXRhZGF0YShkaXJlY3RpdmVUeXBlKTtcbiAgICAgICAgICAgICAgaWYgKGRpck1ldGEgJiYgZGlyTWV0YS5pc0NvbXBvbmVudCkge1xuICAgICAgICAgICAgICAgIGNvbXBNZXRhcy5wdXNoKGRpck1ldGEpO1xuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIGNvbXBNZXRhcy5mb3JFYWNoKGNvbXBNZXRhID0+IHtcbiAgICAgICAgICAgICAgY29uc3QgaHRtbCA9IGNvbXBNZXRhLnRlbXBsYXRlICEudGVtcGxhdGUgITtcbiAgICAgICAgICAgICAgLy8gVGVtcGxhdGUgVVJMIHBvaW50cyB0byBlaXRoZXIgYW4gSFRNTCBvciBUUyBmaWxlIGRlcGVuZGluZyBvblxuICAgICAgICAgICAgICAvLyB3aGV0aGVyIHRoZSBmaWxlIGlzIHVzZWQgd2l0aCBgdGVtcGxhdGVVcmw6YCBvciBgdGVtcGxhdGU6YCxcbiAgICAgICAgICAgICAgLy8gcmVzcGVjdGl2ZWx5LlxuICAgICAgICAgICAgICBjb25zdCB0ZW1wbGF0ZVVybCA9IGNvbXBNZXRhLnRlbXBsYXRlICEudGVtcGxhdGVVcmwgITtcbiAgICAgICAgICAgICAgY29uc3QgaW50ZXJwb2xhdGlvbkNvbmZpZyA9XG4gICAgICAgICAgICAgICAgICBJbnRlcnBvbGF0aW9uQ29uZmlnLmZyb21BcnJheShjb21wTWV0YS50ZW1wbGF0ZSAhLmludGVycG9sYXRpb24pO1xuICAgICAgICAgICAgICBlcnJvcnMucHVzaCguLi50aGlzLm1lc3NhZ2VCdW5kbGUudXBkYXRlRnJvbVRlbXBsYXRlKFxuICAgICAgICAgICAgICAgICAgaHRtbCwgdGVtcGxhdGVVcmwsIGludGVycG9sYXRpb25Db25maWcpICEpO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfSk7XG5cbiAgICAgICAgICBpZiAoZXJyb3JzLmxlbmd0aCkge1xuICAgICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGVycm9ycy5tYXAoZSA9PiBlLnRvU3RyaW5nKCkpLmpvaW4oJ1xcbicpKTtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICByZXR1cm4gdGhpcy5tZXNzYWdlQnVuZGxlO1xuICAgICAgICB9KTtcbiAgfVxuXG4gIHN0YXRpYyBjcmVhdGUoaG9zdDogRXh0cmFjdG9ySG9zdCwgbG9jYWxlOiBzdHJpbmd8bnVsbCk6XG4gICAgICB7ZXh0cmFjdG9yOiBFeHRyYWN0b3IsIHN0YXRpY1JlZmxlY3RvcjogU3RhdGljUmVmbGVjdG9yfSB7XG4gICAgY29uc3QgaHRtbFBhcnNlciA9IG5ldyBIdG1sUGFyc2VyKCk7XG5cbiAgICBjb25zdCB1cmxSZXNvbHZlciA9IGNyZWF0ZUFvdFVybFJlc29sdmVyKGhvc3QpO1xuICAgIGNvbnN0IHN5bWJvbENhY2hlID0gbmV3IFN0YXRpY1N5bWJvbENhY2hlKCk7XG4gICAgY29uc3Qgc3VtbWFyeVJlc29sdmVyID0gbmV3IEFvdFN1bW1hcnlSZXNvbHZlcihob3N0LCBzeW1ib2xDYWNoZSk7XG4gICAgY29uc3Qgc3RhdGljU3ltYm9sUmVzb2x2ZXIgPSBuZXcgU3RhdGljU3ltYm9sUmVzb2x2ZXIoaG9zdCwgc3ltYm9sQ2FjaGUsIHN1bW1hcnlSZXNvbHZlcik7XG4gICAgY29uc3Qgc3RhdGljUmVmbGVjdG9yID0gbmV3IFN0YXRpY1JlZmxlY3RvcihzdW1tYXJ5UmVzb2x2ZXIsIHN0YXRpY1N5bWJvbFJlc29sdmVyKTtcblxuICAgIGNvbnN0IGNvbmZpZyA9XG4gICAgICAgIG5ldyBDb21waWxlckNvbmZpZyh7ZGVmYXVsdEVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLkVtdWxhdGVkLCB1c2VKaXQ6IGZhbHNlfSk7XG5cbiAgICBjb25zdCBub3JtYWxpemVyID0gbmV3IERpcmVjdGl2ZU5vcm1hbGl6ZXIoXG4gICAgICAgIHtnZXQ6ICh1cmw6IHN0cmluZykgPT4gaG9zdC5sb2FkUmVzb3VyY2UodXJsKX0sIHVybFJlc29sdmVyLCBodG1sUGFyc2VyLCBjb25maWcpO1xuICAgIGNvbnN0IGVsZW1lbnRTY2hlbWFSZWdpc3RyeSA9IG5ldyBEb21FbGVtZW50U2NoZW1hUmVnaXN0cnkoKTtcbiAgICBjb25zdCByZXNvbHZlciA9IG5ldyBDb21waWxlTWV0YWRhdGFSZXNvbHZlcihcbiAgICAgICAgY29uZmlnLCBodG1sUGFyc2VyLCBuZXcgTmdNb2R1bGVSZXNvbHZlcihzdGF0aWNSZWZsZWN0b3IpLFxuICAgICAgICBuZXcgRGlyZWN0aXZlUmVzb2x2ZXIoc3RhdGljUmVmbGVjdG9yKSwgbmV3IFBpcGVSZXNvbHZlcihzdGF0aWNSZWZsZWN0b3IpLCBzdW1tYXJ5UmVzb2x2ZXIsXG4gICAgICAgIGVsZW1lbnRTY2hlbWFSZWdpc3RyeSwgbm9ybWFsaXplciwgY29uc29sZSwgc3ltYm9sQ2FjaGUsIHN0YXRpY1JlZmxlY3Rvcik7XG5cbiAgICAvLyBUT0RPKHZpY2IpOiBpbXBsaWNpdCB0YWdzICYgYXR0cmlidXRlc1xuICAgIGNvbnN0IG1lc3NhZ2VCdW5kbGUgPSBuZXcgTWVzc2FnZUJ1bmRsZShodG1sUGFyc2VyLCBbXSwge30sIGxvY2FsZSk7XG5cbiAgICBjb25zdCBleHRyYWN0b3IgPSBuZXcgRXh0cmFjdG9yKGhvc3QsIHN0YXRpY1N5bWJvbFJlc29sdmVyLCBtZXNzYWdlQnVuZGxlLCByZXNvbHZlcik7XG4gICAgcmV0dXJuIHtleHRyYWN0b3IsIHN0YXRpY1JlZmxlY3Rvcn07XG4gIH1cbn1cbiJdfQ==