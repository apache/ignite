/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { extractMessages } from './extractor_merger';
import * as i18n from './i18n_ast';
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
        var i18nParserResult = extractMessages(htmlParserResult.rootNodes, interpolationConfig, this._implicitTags, this._implicitAttrs);
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
export { MessageBundle };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWVzc2FnZV9idW5kbGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvaTE4bi9tZXNzYWdlX2J1bmRsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBTUgsT0FBTyxFQUFDLGVBQWUsRUFBQyxNQUFNLG9CQUFvQixDQUFDO0FBQ25ELE9BQU8sS0FBSyxJQUFJLE1BQU0sWUFBWSxDQUFDO0FBSW5DOztHQUVHO0FBQ0g7SUFHRSx1QkFDWSxXQUF1QixFQUFVLGFBQXVCLEVBQ3hELGNBQXVDLEVBQVUsT0FBMkI7UUFBM0Isd0JBQUEsRUFBQSxjQUEyQjtRQUQ1RSxnQkFBVyxHQUFYLFdBQVcsQ0FBWTtRQUFVLGtCQUFhLEdBQWIsYUFBYSxDQUFVO1FBQ3hELG1CQUFjLEdBQWQsY0FBYyxDQUF5QjtRQUFVLFlBQU8sR0FBUCxPQUFPLENBQW9CO1FBSmhGLGNBQVMsR0FBbUIsRUFBRSxDQUFDO0lBSW9ELENBQUM7SUFFNUYsMENBQWtCLEdBQWxCLFVBQW1CLElBQVksRUFBRSxHQUFXLEVBQUUsbUJBQXdDOztRQUVwRixJQUFNLGdCQUFnQixHQUNsQixJQUFJLENBQUMsV0FBVyxDQUFDLEtBQUssQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLEVBQUMsc0JBQXNCLEVBQUUsSUFBSSxFQUFFLG1CQUFtQixxQkFBQSxFQUFDLENBQUMsQ0FBQztRQUUzRixJQUFJLGdCQUFnQixDQUFDLE1BQU0sQ0FBQyxNQUFNLEVBQUU7WUFDbEMsT0FBTyxnQkFBZ0IsQ0FBQyxNQUFNLENBQUM7U0FDaEM7UUFFRCxJQUFNLGdCQUFnQixHQUFHLGVBQWUsQ0FDcEMsZ0JBQWdCLENBQUMsU0FBUyxFQUFFLG1CQUFtQixFQUFFLElBQUksQ0FBQyxhQUFhLEVBQUUsSUFBSSxDQUFDLGNBQWMsQ0FBQyxDQUFDO1FBRTlGLElBQUksZ0JBQWdCLENBQUMsTUFB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cEJELENBQWtDLElBQUksQ0FBQyxZQUFZLEdBb0JsRCIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtIdG1sUGFyc2VyfSBmcm9tICcuLi9tbF9wYXJzZXIvaHRtbF9wYXJzZXInO1xuaW1wb3J0IHtJbnRlcnBvbGF0aW9uQ29uZmlnfSBmcm9tICcuLi9tbF9wYXJzZXIvaW50ZXJwb2xhdGlvbl9jb25maWcnO1xuaW1wb3J0IHtQYXJzZUVycm9yfSBmcm9tICcuLi9wYXJzZV91dGlsJztcblxuaW1wb3J0IHtleHRyYWN0TWVzc2FnZXN9IGZyb20gJy4vZXh0cmFjdG9yX21lcmdlcic7XG5pbXBvcnQgKiBhcyBpMThuIGZyb20gJy4vaTE4bl9hc3QnO1xuaW1wb3J0IHtQbGFjZWhvbGRlck1hcHBlciwgU2VyaWFsaXplcn0gZnJvbSAnLi9zZXJpYWxpemVycy9zZXJpYWxpemVyJztcblxuXG4vKipcbiAqIEEgY29udGFpbmVyIGZvciBtZXNzYWdlIGV4dHJhY3RlZCBmcm9tIHRoZSB0ZW1wbGF0ZXMuXG4gKi9cbmV4cG9ydCBjbGFzcyBNZXNzYWdlQnVuZGxlIHtcbiAgcHJpdmF0ZSBfbWVzc2FnZXM6IGkxOG4uTWVzc2FnZVtdID0gW107XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9odG1sUGFyc2VyOiBIdG1sUGFyc2VyLCBwcml2YXRlIF9pbXBsaWNpdFRhZ3M6IHN0cmluZ1tdLFxuICAgICAgcHJpdmF0ZSBfaW1wbGljaXRBdHRyczoge1trOiBzdHJpbmddOiBzdHJpbmdbXX0sIHByaXZhdGUgX2xvY2FsZTogc3RyaW5nfG51bGwgPSBudWxsKSB7fVxuXG4gIHVwZGF0ZUZyb21UZW1wbGF0ZShodG1sOiBzdHJpbmcsIHVybDogc3RyaW5nLCBpbnRlcnBvbGF0aW9uQ29uZmlnOiBJbnRlcnBvbGF0aW9uQ29uZmlnKTpcbiAgICAgIFBhcnNlRXJyb3JbXSB7XG4gICAgY29uc3QgaHRtbFBhcnNlclJlc3VsdCA9XG4gICAgICAgIHRoaXMuX2h0bWxQYXJzZXIucGFyc2UoaHRtbCwgdXJsLCB7dG9rZW5pemVFeHBhbnNpb25Gb3JtczogdHJ1ZSwgaW50ZXJwb2xhdGlvbkNvbmZpZ30pO1xuXG4gICAgaWYgKGh0bWxQYXJzZXJSZXN1bHQuZXJyb3JzLmxlbmd0aCkge1xuICAgICAgcmV0dXJuIGh0bWxQYXJzZXJSZXN1bHQuZXJyb3JzO1xuICAgIH1cblxuICAgIGNvbnN0IGkxOG5QYXJzZXJSZXN1bHQgPSBleHRyYWN0TWVzc2FnZXMoXG4gICAgICAgIGh0bWxQYXJzZXJSZXN1bHQucm9vdE5vZGVzLCBpbnRlcnBvbGF0aW9uQ29uZmlnLCB0aGlzLl9pbXBsaWNpdFRhZ3MsIHRoaXMuX2ltcGxpY2l0QXR0cnMpO1xuXG4gICAgaWYgKGkxOG5QYXJzZXJSZXN1bHQuZXJyb3JzLmxlbmd0aCkge1xuICAgICAgcmV0dXJuIGkxOG5QYXJzZXJSZXN1bHQuZXJyb3JzO1xuICAgIH1cblxuICAgIHRoaXMuX21lc3NhZ2VzLnB1c2goLi4uaTE4blBhcnNlclJlc3VsdC5tZXNzYWdlcyk7XG4gICAgcmV0dXJuIFtdO1xuICB9XG5cbiAgLy8gUmV0dXJuIHRoZSBtZXNzYWdlIGluIHRoZSBpbnRlcm5hbCBmb3JtYXRcbiAgLy8gVGhlIHB1YmxpYyAoc2VyaWFsaXplZCkgZm9ybWF0IG1pZ2h0IGJlIGRpZmZlcmVudCwgc2VlIHRoZSBgd3JpdGVgIG1ldGhvZC5cbiAgZ2V0TWVzc2FnZXMoKTogaTE4bi5NZXNzYWdlW10geyByZXR1cm4gdGhpcy5fbWVzc2FnZXM7IH1cblxuICB3cml0ZShzZXJpYWxpemVyOiBTZXJpYWxpemVyLCBmaWx0ZXJTb3VyY2VzPzogKHBhdGg6IHN0cmluZykgPT4gc3RyaW5nKTogc3RyaW5nIHtcbiAgICBjb25zdCBtZXNzYWdlczoge1tpZDogc3RyaW5nXTogaTE4bi5NZXNzYWdlfSA9IHt9O1xuICAgIGNvbnN0IG1hcHBlclZpc2l0b3IgPSBuZXcgTWFwUGxhY2Vob2xkZXJOYW1lcygpO1xuXG4gICAgLy8gRGVkdXBsaWNhdGUgbWVzc2FnZXMgYmFzZWQgb24gdGhlaXIgSURcbiAgICB0aGlzLl9tZXNzYWdlcy5mb3JFYWNoKG1lc3NhZ2UgPT4ge1xuICAgICAgY29uc3QgaWQgPSBzZXJpYWxpemVyLmRpZ2VzdChtZXNzYWdlKTtcbiAgICAgIGlmICghbWVzc2FnZXMuaGFzT3duUHJvcGVydHkoaWQpKSB7XG4gICAgICAgIG1lc3NhZ2VzW2lkXSA9IG1lc3NhZ2U7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBtZXNzYWdlc1tpZF0uc291cmNlcy5wdXNoKC4uLm1lc3NhZ2Uuc291cmNlcyk7XG4gICAgICB9XG4gICAgfSk7XG5cbiAgICAvLyBUcmFuc2Zvcm0gcGxhY2Vob2xkZXIgbmFtZXMgdXNpbmcgdGhlIHNlcmlhbGl6ZXIgbWFwcGluZ1xuICAgIGNvbnN0IG1zZ0xpc3QgPSBPYmplY3Qua2V5cyhtZXNzYWdlcykubWFwKGlkID0+IHtcbiAgICAgIGNvbnN0IG1hcHBlciA9IHNlcmlhbGl6ZXIuY3JlYXRlTmFtZU1hcHBlcihtZXNzYWdlc1tpZF0pO1xuICAgICAgY29uc3Qgc3JjID0gbWVzc2FnZXNbaWRdO1xuICAgICAgY29uc3Qgbm9kZXMgPSBtYXBwZXIgPyBtYXBwZXJWaXNpdG9yLmNvbnZlcnQoc3JjLm5vZGVzLCBtYXBwZXIpIDogc3JjLm5vZGVzO1xuICAgICAgbGV0IHRyYW5zZm9ybWVkTWVzc2FnZSA9IG5ldyBpMThuLk1lc3NhZ2Uobm9kZXMsIHt9LCB7fSwgc3JjLm1lYW5pbmcsIHNyYy5kZXNjcmlwdGlvbiwgaWQpO1xuICAgICAgdHJhbnNmb3JtZWRNZXNzYWdlLnNvdXJjZXMgPSBzcmMuc291cmNlcztcbiAgICAgIGlmIChmaWx0ZXJTb3VyY2VzKSB7XG4gICAgICAgIHRyYW5zZm9ybWVkTWVzc2FnZS5zb3VyY2VzLmZvckVhY2goXG4gICAgICAgICAgICAoc291cmNlOiBpMThuLk1lc3NhZ2VTcGFuKSA9PiBzb3VyY2UuZmlsZVBhdGggPSBmaWx0ZXJTb3VyY2VzKHNvdXJjZS5maWxlUGF0aCkpO1xuICAgICAgfVxuICAgICAgcmV0dXJuIHRyYW5zZm9ybWVkTWVzc2FnZTtcbiAgICB9KTtcblxuICAgIHJldHVybiBzZXJpYWxpemVyLndyaXRlKG1zZ0xpc3QsIHRoaXMuX2xvY2FsZSk7XG4gIH1cbn1cblxuLy8gVHJhbnNmb3JtIGFuIGkxOG4gQVNUIGJ5IHJlbmFtaW5nIHRoZSBwbGFjZWhvbGRlciBub2RlcyB3aXRoIHRoZSBnaXZlbiBtYXBwZXJcbmNsYXNzIE1hcFBsYWNlaG9sZGVyTmFtZXMgZXh0ZW5kcyBpMThuLkNsb25lVmlzaXRvciB7XG4gIGNvbnZlcnQobm9kZXM6IGkxOG4uTm9kZVtdLCBtYXBwZXI6IFBsYWNlaG9sZGVyTWFwcGVyKTogaTE4bi5Ob2RlW10ge1xuICAgIHJldHVybiBtYXBwZXIgPyBub2Rlcy5tYXAobiA9PiBuLnZpc2l0KHRoaXMsIG1hcHBlcikpIDogbm9kZXM7XG4gIH1cblxuICB2aXNpdFRhZ1BsYWNlaG9sZGVyKHBoOiBpMThuLlRhZ1BsYWNlaG9sZGVyLCBtYXBwZXI6IFBsYWNlaG9sZGVyTWFwcGVyKTogaTE4bi5UYWdQbGFjZWhvbGRlciB7XG4gICAgY29uc3Qgc3RhcnROYW1lID0gbWFwcGVyLnRvUHVibGljTmFtZShwaC5zdGFydE5hbWUpICE7XG4gICAgY29uc3QgY2xvc2VOYW1lID0gcGguY2xvc2VOYW1lID8gbWFwcGVyLnRvUHVibGljTmFtZShwaC5jbG9zZU5hbWUpICEgOiBwaC5jbG9zZU5hbWU7XG4gICAgY29uc3QgY2hpbGRyZW4gPSBwaC5jaGlsZHJlbi5tYXAobiA9PiBuLnZpc2l0KHRoaXMsIG1hcHBlcikpO1xuICAgIHJldHVybiBuZXcgaTE4bi5UYWdQbGFjZWhvbGRlcihcbiAgICAgICAgcGgudGFnLCBwaC5hdHRycywgc3RhcnROYW1lLCBjbG9zZU5hbWUsIGNoaWxkcmVuLCBwaC5pc1ZvaWQsIHBoLnNvdXJjZVNwYW4pO1xuICB9XG5cbiAgdmlzaXRQbGFjZWhvbGRlcihwaDogaTE4bi5QbGFjZWhvbGRlciwgbWFwcGVyOiBQbGFjZWhvbGRlck1hcHBlcik6IGkxOG4uUGxhY2Vob2xkZXIge1xuICAgIHJldHVybiBuZXcgaTE4bi5QbGFjZWhvbGRlcihwaC52YWx1ZSwgbWFwcGVyLnRvUHVibGljTmFtZShwaC5uYW1lKSAhLCBwaC5zb3VyY2VTcGFuKTtcbiAgfVxuXG4gIHZpc2l0SWN1UGxhY2Vob2xkZXIocGg6IGkxOG4uSWN1UGxhY2Vob2xkZXIsIG1hcHBlcjogUGxhY2Vob2xkZXJNYXBwZXIpOiBpMThuLkljdVBsYWNlaG9sZGVyIHtcbiAgICByZXR1cm4gbmV3IGkxOG4uSWN1UGxhY2Vob2xkZXIocGgudmFsdWUsIG1hcHBlci50b1B1YmxpY05hbWUocGgubmFtZSkgISwgcGguc291cmNlU3Bhbik7XG4gIH1cbn1cbiJdfQ==