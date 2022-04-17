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
        define("@angular/compiler/src/i18n/message_bundle", ["require", "exports", "tslib", "@angular/compiler/src/i18n/extractor_merger", "@angular/compiler/src/i18n/i18n_ast"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var extractor_merger_1 = require("@angular/compiler/src/i18n/extractor_merger");
    var i18n = require("@angular/compiler/src/i18n/i18n_ast");
    /**
     * A container for message extracted from the templates.
     */
    var MessageBundle = /** @class */ (function () {
        function MessageBundle(_htmlParser, _implicitTags, _implicitAttrs, _locale) {
            if (_locale === void 0) { _locale = null; }
            this._htmlParser = _htmlParser;
            this._implicitTags = _implicitTags;
            this._implicitAttrs = _implicitAttrs;
            this._locale = _locale;
            this._messages = [];
        }
        MessageBundle.prototype.updateFromTemplate = function (html, url, interpolationConfig) {
            var _a;
            var htmlParserResult = this._htmlParser.parse(html, url, { tokenizeExpansionForms: true, interpolationConfig: interpolationConfig });
            if (htmlParserResult.errors.length) {
                return htmlParserResult.errors;
            }
            var i18nParserResult = extractor_merger_1.extractMessages(htmlParserResult.rootNodes, interpolationConfig, this._implicitTags, this._implicitAttrs);
            if (i18nParserResult.errors.length) {
                return i18nParserResult.errors;
            }
            (_a = this._messages).push.apply(_a, tslib_1.__spread(i18nParserResult.messages));
            return [];
        };
        // Return the message in the internal format
        // The public (serialized) format might be different, see the `write` method.
        MessageBundle.prototype.getMessages = function () { return this._messages; };
        MessageBundle.prototype.write = function (serializer, filterSources) {
            var messages = {};
            var mapperVisitor = new MapPlaceholderNames();
            // Deduplicate messages based on their ID
            this._messages.forEach(function (message) {
                var _a;
                var id = serializer.digest(message);
                if (!messages.hasOwnProperty(id)) {
                    messages[id] = message;
                }
                else {
                    (_a = messages[id].sources).push.apply(_a, tslib_1.__spread(message.sources));
                }
            });
            // Transform placeholder names using the serializer mapping
            var msgList = Object.keys(messages).map(function (id) {
                var mapper = serializer.createNameMapper(messages[id]);
                var src = messages[id];
                var nodes = mapper ? mapperVisitor.convert(src.nodes, mapper) : src.nodes;
                var transformedMessage = new i18n.Message(nodes, {}, {}, src.meaning, src.description, id);
                transformedMessage.sources = src.sources;
                if (filterSources) {
                    transformedMessage.sources.forEach(function (source) { return source.filePath = filterSources(source.filePath); });
                }
                return transformedMessage;
            });
            return serializer.write(msgList, this._locale);
        };
        return MessageBundle;
    }());
    exports.MessageBundle = MessageBundle;
    // Transform an i18n AST by renaming the placeholder nodes with the given mapper
    var MapPlaceholderNames = /** @class */ (function (_super) {
        tslib_1.__extends(MapPlaceholderNames, _super);
        function MapPlaceholderNames() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        MapPlaceholderNames.prototype.convert = function (nodes, mapper) {
            var _this = this;
            return mapper ? nodes.map(function (n) { return n.visit(_this, mapper); }) : nodes;
        };
        MapPlaceholderNames.prototype.visitTagPlaceholder = function (ph, mapper) {
            var _this = this;
            var startName = mapper.toPublicName(ph.startName);
            var closeName = ph.closeName ? mapper.toPublicName(ph.closeName) : ph.closeName;
            var children = ph.children.map(function (n) { return n.visit(_this, mapper); });
            return new i18n.TagPlaceholder(ph.tag, ph.attrs, startName, closeName, children, ph.isVoid, ph.sourceSpan);
        };
        MapPlaceholderNames.prototype.visitPlaceholder = function (ph, mapper) {
            return new i18n.Placeholder(ph.value, mapper.toPublicName(ph.name), ph.sourceSpan);
        };
        MapPlaceholderNames.prototype.visitIcuPlaceholder = function (ph, mapper) {
            return new i18n.IcuPlaceholder(ph.value, mapper.toPublicName(ph.name), ph.sourceSpan);
        };
        return MapPlaceholderNames;
    }(i18n.CloneVisitor));
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWVzc2FnZV9idW5kbGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaTE4bi9tZXNzYWdlX2J1bmRsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7Ozs7SUFNSCxnRkFBbUQ7SUFDbkQsMERBQW1DO0lBSW5DOztPQUVHO0lBQ0g7UUFHRSx1QkFDWSxXQUF1QixFQUFVLGFBQXVCLEVBQ3hELGNBQXVDLEVBQVUsT0FBMkI7WUFBM0Isd0JBQUEsRUFBQSxjQUEyQjtZQUQ1RSxnQkFBVyxHQUFYLFdBQVcsQ0FBWTtZQUFVLGtCQUFhLEdBQWIsYUFBYSxDQUFVO1lBQ3hELG1CQUFjLEdBQWQsY0FBYyxDQUF5QjtZQUFVLFlBQU8sR0FBUCxPQUFPLENBQW9CO1lBSmhGLGNBQVMsR0FBbUIsRUFBRSxDQUFDO1FBSW9ELENBQUM7UUFFNUYsMENBQWtCLEdBQWxCLFVBQW1CLElBQVksRUFBRSxHQUFXLEVBQUUsbUJBQXdDOztZQUVwRixJQUFNLGdCQUFnQixHQUNsQixJQUFJLENBQUMsV0FBVyxDQUFDLEtBQUssQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLEVBQUMsc0JBQXNCLEVBQUUsSUFBSSxFQUFFLG1CQUFtQixxQkFBQSxFQUFDLENBQUMsQ0FBQztZQUUzRixJQUFJLGdCQUFnQixDQUFDLE1BQU0sQ0FBQyxNQUFNLEVBQUU7Z0JBQ2xDLE9BQU8sZ0JBQWdCLENBQUMsTUFBTSxDQUFDO2FBQ2hDO1lBRUQsSUFBTSxnQkFBZ0IsR0FBRyxrQ0FBZSxDQUNwQyxnQkFBZ0IsQ0FBQyxTQUFTLEVBQUUsbUJBQW1CLEVBQUUsSUFBSSxDQUFDLGFBQWEsRUFBRSxJQUFJLENBQUMsY0FBYyxDQUFDLENBQUM7WUFFOUYsSUFBSSxnQkFBZ0IsQ0FBQyxNQUFNLENBQUMsTUFBTSxFQUFFO2dCQUNsQyxPQUFPLGdCQUFnQixDQUFDLE1BQU0sQ0FB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WSxHQW9CbEQiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SHRtbFBhcnNlcn0gZnJvbSAnLi4vbWxfcGFyc2VyL2h0bWxfcGFyc2VyJztcbmltcG9ydCB7SW50ZXJwb2xhdGlvbkNvbmZpZ30gZnJvbSAnLi4vbWxfcGFyc2VyL2ludGVycG9sYXRpb25fY29uZmlnJztcbmltcG9ydCB7UGFyc2VFcnJvcn0gZnJvbSAnLi4vcGFyc2VfdXRpbCc7XG5cbmltcG9ydCB7ZXh0cmFjdE1lc3NhZ2VzfSBmcm9tICcuL2V4dHJhY3Rvcl9tZXJnZXInO1xuaW1wb3J0ICogYXMgaTE4biBmcm9tICcuL2kxOG5fYXN0JztcbmltcG9ydCB7UGxhY2Vob2xkZXJNYXBwZXIsIFNlcmlhbGl6ZXJ9IGZyb20gJy4vc2VyaWFsaXplcnMvc2VyaWFsaXplcic7XG5cblxuLyoqXG4gKiBBIGNvbnRhaW5lciBmb3IgbWVzc2FnZSBleHRyYWN0ZWQgZnJvbSB0aGUgdGVtcGxhdGVzLlxuICovXG5leHBvcnQgY2xhc3MgTWVzc2FnZUJ1bmRsZSB7XG4gIHByaXZhdGUgX21lc3NhZ2VzOiBpMThuLk1lc3NhZ2VbXSA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHJpdmF0ZSBfaHRtbFBhcnNlcjogSHRtbFBhcnNlciwgcHJpdmF0ZSBfaW1wbGljaXRUYWdzOiBzdHJpbmdbXSxcbiAgICAgIHByaXZhdGUgX2ltcGxpY2l0QXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nW119LCBwcml2YXRlIF9sb2NhbGU6IHN0cmluZ3xudWxsID0gbnVsbCkge31cblxuICB1cGRhdGVGcm9tVGVtcGxhdGUoaHRtbDogc3RyaW5nLCB1cmw6IHN0cmluZywgaW50ZXJwb2xhdGlvbkNvbmZpZzogSW50ZXJwb2xhdGlvbkNvbmZpZyk6XG4gICAgICBQYXJzZUVycm9yW10ge1xuICAgIGNvbnN0IGh0bWxQYXJzZXJSZXN1bHQgPVxuICAgICAgICB0aGlzLl9odG1sUGFyc2VyLnBhcnNlKGh0bWwsIHVybCwge3Rva2VuaXplRXhwYW5zaW9uRm9ybXM6IHRydWUsIGludGVycG9sYXRpb25Db25maWd9KTtcblxuICAgIGlmIChodG1sUGFyc2VyUmVzdWx0LmVycm9ycy5sZW5ndGgpIHtcbiAgICAgIHJldHVybiBodG1sUGFyc2VyUmVzdWx0LmVycm9ycztcbiAgICB9XG5cbiAgICBjb25zdCBpMThuUGFyc2VyUmVzdWx0ID0gZXh0cmFjdE1lc3NhZ2VzKFxuICAgICAgICBodG1sUGFyc2VyUmVzdWx0LnJvb3ROb2RlcywgaW50ZXJwb2xhdGlvbkNvbmZpZywgdGhpcy5faW1wbGljaXRUYWdzLCB0aGlzLl9pbXBsaWNpdEF0dHJzKTtcblxuICAgIGlmIChpMThuUGFyc2VyUmVzdWx0LmVycm9ycy5sZW5ndGgpIHtcbiAgICAgIHJldHVybiBpMThuUGFyc2VyUmVzdWx0LmVycm9ycztcbiAgICB9XG5cbiAgICB0aGlzLl9tZXNzYWdlcy5wdXNoKC4uLmkxOG5QYXJzZXJSZXN1bHQubWVzc2FnZXMpO1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIC8vIFJldHVybiB0aGUgbWVzc2FnZSBpbiB0aGUgaW50ZXJuYWwgZm9ybWF0XG4gIC8vIFRoZSBwdWJsaWMgKHNlcmlhbGl6ZWQpIGZvcm1hdCBtaWdodCBiZSBkaWZmZXJlbnQsIHNlZSB0aGUgYHdyaXRlYCBtZXRob2QuXG4gIGdldE1lc3NhZ2VzKCk6IGkxOG4uTWVzc2FnZVtdIHsgcmV0dXJuIHRoaXMuX21lc3NhZ2VzOyB9XG5cbiAgd3JpdGUoc2VyaWFsaXplcjogU2VyaWFsaXplciwgZmlsdGVyU291cmNlcz86IChwYXRoOiBzdHJpbmcpID0+IHN0cmluZyk6IHN0cmluZyB7XG4gICAgY29uc3QgbWVzc2FnZXM6IHtbaWQ6IHN0cmluZ106IGkxOG4uTWVzc2FnZX0gPSB7fTtcbiAgICBjb25zdCBtYXBwZXJWaXNpdG9yID0gbmV3IE1hcFBsYWNlaG9sZGVyTmFtZXMoKTtcblxuICAgIC8vIERlZHVwbGljYXRlIG1lc3NhZ2VzIGJhc2VkIG9uIHRoZWlyIElEXG4gICAgdGhpcy5fbWVzc2FnZXMuZm9yRWFjaChtZXNzYWdlID0+IHtcbiAgICAgIGNvbnN0IGlkID0gc2VyaWFsaXplci5kaWdlc3QobWVzc2FnZSk7XG4gICAgICBpZiAoIW1lc3NhZ2VzLmhhc093blByb3BlcnR5KGlkKSkge1xuICAgICAgICBtZXNzYWdlc1tpZF0gPSBtZXNzYWdlO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbWVzc2FnZXNbaWRdLnNvdXJjZXMucHVzaCguLi5tZXNzYWdlLnNvdXJjZXMpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gICAgLy8gVHJhbnNmb3JtIHBsYWNlaG9sZGVyIG5hbWVzIHVzaW5nIHRoZSBzZXJpYWxpemVyIG1hcHBpbmdcbiAgICBjb25zdCBtc2dMaXN0ID0gT2JqZWN0LmtleXMobWVzc2FnZXMpLm1hcChpZCA9PiB7XG4gICAgICBjb25zdCBtYXBwZXIgPSBzZXJpYWxpemVyLmNyZWF0ZU5hbWVNYXBwZXIobWVzc2FnZXNbaWRdKTtcbiAgICAgIGNvbnN0IHNyYyA9IG1lc3NhZ2VzW2lkXTtcbiAgICAgIGNvbnN0IG5vZGVzID0gbWFwcGVyID8gbWFwcGVyVmlzaXRvci5jb252ZXJ0KHNyYy5ub2RlcywgbWFwcGVyKSA6IHNyYy5ub2RlcztcbiAgICAgIGxldCB0cmFuc2Zvcm1lZE1lc3NhZ2UgPSBuZXcgaTE4bi5NZXNzYWdlKG5vZGVzLCB7fSwge30sIHNyYy5tZWFuaW5nLCBzcmMuZGVzY3JpcHRpb24sIGlkKTtcbiAgICAgIHRyYW5zZm9ybWVkTWVzc2FnZS5zb3VyY2VzID0gc3JjLnNvdXJjZXM7XG4gICAgICBpZiAoZmlsdGVyU291cmNlcykge1xuICAgICAgICB0cmFuc2Zvcm1lZE1lc3NhZ2Uuc291cmNlcy5mb3JFYWNoKFxuICAgICAgICAgICAgKHNvdXJjZTogaTE4bi5NZXNzYWdlU3BhbikgPT4gc291cmNlLmZpbGVQYXRoID0gZmlsdGVyU291cmNlcyhzb3VyY2UuZmlsZVBhdGgpKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiB0cmFuc2Zvcm1lZE1lc3NhZ2U7XG4gICAgfSk7XG5cbiAgICByZXR1cm4gc2VyaWFsaXplci53cml0ZShtc2dMaXN0LCB0aGlzLl9sb2NhbGUpO1xuICB9XG59XG5cbi8vIFRyYW5zZm9ybSBhbiBpMThuIEFTVCBieSByZW5hbWluZyB0aGUgcGxhY2Vob2xkZXIgbm9kZXMgd2l0aCB0aGUgZ2l2ZW4gbWFwcGVyXG5jbGFzcyBNYXBQbGFjZWhvbGRlck5hbWVzIGV4dGVuZHMgaTE4bi5DbG9uZVZpc2l0b3Ige1xuICBjb252ZXJ0KG5vZGVzOiBpMThuLk5vZGVbXSwgbWFwcGVyOiBQbGFjZWhvbGRlck1hcHBlcik6IGkxOG4uTm9kZVtdIHtcbiAgICByZXR1cm4gbWFwcGVyID8gbm9kZXMubWFwKG4gPT4gbi52aXNpdCh0aGlzLCBtYXBwZXIpKSA6IG5vZGVzO1xuICB9XG5cbiAgdmlzaXRUYWdQbGFjZWhvbGRlcihwaDogaTE4bi5UYWdQbGFjZWhvbGRlciwgbWFwcGVyOiBQbGFjZWhvbGRlck1hcHBlcik6IGkxOG4uVGFnUGxhY2Vob2xkZXIge1xuICAgIGNvbnN0IHN0YXJ0TmFtZSA9IG1hcHBlci50b1B1YmxpY05hbWUocGguc3RhcnROYW1lKSAhO1xuICAgIGNvbnN0IGNsb3NlTmFtZSA9IHBoLmNsb3NlTmFtZSA/IG1hcHBlci50b1B1YmxpY05hbWUocGguY2xvc2VOYW1lKSAhIDogcGguY2xvc2VOYW1lO1xuICAgIGNvbnN0IGNoaWxkcmVuID0gcGguY2hpbGRyZW4ubWFwKG4gPT4gbi52aXNpdCh0aGlzLCBtYXBwZXIpKTtcbiAgICByZXR1cm4gbmV3IGkxOG4uVGFnUGxhY2Vob2xkZXIoXG4gICAgICAgIHBoLnRhZywgcGguYXR0cnMsIHN0YXJ0TmFtZSwgY2xvc2VOYW1lLCBjaGlsZHJlbiwgcGguaXNWb2lkLCBwaC5zb3VyY2VTcGFuKTtcbiAgfVxuXG4gIHZpc2l0UGxhY2Vob2xkZXIocGg6IGkxOG4uUGxhY2Vob2xkZXIsIG1hcHBlcjogUGxhY2Vob2xkZXJNYXBwZXIpOiBpMThuLlBsYWNlaG9sZGVyIHtcbiAgICByZXR1cm4gbmV3IGkxOG4uUGxhY2Vob2xkZXIocGgudmFsdWUsIG1hcHBlci50b1B1YmxpY05hbWUocGgubmFtZSkgISwgcGguc291cmNlU3Bhbik7XG4gIH1cblxuICB2aXNpdEljdVBsYWNlaG9sZGVyKHBoOiBpMThuLkljdVBsYWNlaG9sZGVyLCBtYXBwZXI6IFBsYWNlaG9sZGVyTWFwcGVyKTogaTE4bi5JY3VQbGFjZWhvbGRlciB7XG4gICAgcmV0dXJuIG5ldyBpMThuLkljdVBsYWNlaG9sZGVyKHBoLnZhbHVlLCBtYXBwZXIudG9QdWJsaWNOYW1lKHBoLm5hbWUpICEsIHBoLnNvdXJjZVNwYW4pO1xuICB9XG59XG4iXX0=