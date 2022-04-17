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
        define("@angular/compiler/src/i18n/serializers/xmb", ["require", "exports", "tslib", "@angular/compiler/src/i18n/digest", "@angular/compiler/src/i18n/serializers/serializer", "@angular/compiler/src/i18n/serializers/xml_helper"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var digest_1 = require("@angular/compiler/src/i18n/digest");
    var serializer_1 = require("@angular/compiler/src/i18n/serializers/serializer");
    var xml = require("@angular/compiler/src/i18n/serializers/xml_helper");
    var _MESSAGES_TAG = 'messagebundle';
    var _MESSAGE_TAG = 'msg';
    var _PLACEHOLDER_TAG = 'ph';
    var _EXAMPLE_TAG = 'ex';
    var _SOURCE_TAG = 'source';
    var _DOCTYPE = "<!ELEMENT messagebundle (msg)*>\n<!ATTLIST messagebundle class CDATA #IMPLIED>\n\n<!ELEMENT msg (#PCDATA|ph|source)*>\n<!ATTLIST msg id CDATA #IMPLIED>\n<!ATTLIST msg seq CDATA #IMPLIED>\n<!ATTLIST msg name CDATA #IMPLIED>\n<!ATTLIST msg desc CDATA #IMPLIED>\n<!ATTLIST msg meaning CDATA #IMPLIED>\n<!ATTLIST msg obsolete (obsolete) #IMPLIED>\n<!ATTLIST msg xml:space (default|preserve) \"default\">\n<!ATTLIST msg is_hidden CDATA #IMPLIED>\n\n<!ELEMENT source (#PCDATA)>\n\n<!ELEMENT ph (#PCDATA|ex)*>\n<!ATTLIST ph name CDATA #REQUIRED>\n\n<!ELEMENT ex (#PCDATA)>";
    var Xmb = /** @class */ (function (_super) {
        tslib_1.__extends(Xmb, _super);
        function Xmb() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        Xmb.prototype.write = function (messages, locale) {
            var exampleVisitor = new ExampleVisitor();
            var visitor = new _Visitor();
            var rootNode = new xml.Tag(_MESSAGES_TAG);
            messages.forEach(function (message) {
                var attrs = { id: message.id };
                if (message.description) {
                    attrs['desc'] = message.description;
                }
                if (message.meaning) {
                    attrs['meaning'] = message.meaning;
                }
                var sourceTags = [];
                message.sources.forEach(function (source) {
                    sourceTags.push(new xml.Tag(_SOURCE_TAG, {}, [
                        new xml.Text(source.filePath + ":" + source.startLine + (source.endLine !== source.startLine ? ',' + source.endLine : ''))
                    ]));
                });
                rootNode.children.push(new xml.CR(2), new xml.Tag(_MESSAGE_TAG, attrs, tslib_1.__spread(sourceTags, visitor.serialize(message.nodes))));
            });
            rootNode.children.push(new xml.CR());
            return xml.serialize([
                new xml.Declaration({ version: '1.0', encoding: 'UTF-8' }),
                new xml.CR(),
                new xml.Doctype(_MESSAGES_TAG, _DOCTYPE),
                new xml.CR(),
                exampleVisitor.addDefaultExamples(rootNode),
                new xml.CR(),
            ]);
        };
        Xmb.prototype.load = function (content, url) {
            throw new Error('Unsupported');
        };
        Xmb.prototype.digest = function (message) { return digest(message); };
        Xmb.prototype.createNameMapper = function (message) {
            return new serializer_1.SimplePlaceholderMapper(message, toPublicName);
        };
        return Xmb;
    }(serializer_1.Serializer));
    exports.Xmb = Xmb;
    var _Visitor = /** @class */ (function () {
        function _Visitor() {
        }
        _Visitor.prototype.visitText = function (text, context) { return [new xml.Text(text.value)]; };
        _Visitor.prototype.visitContainer = function (container, context) {
            var _this = this;
            var nodes = [];
            container.children.forEach(function (node) { return nodes.push.apply(nodes, tslib_1.__spread(node.visit(_this))); });
            return nodes;
        };
        _Visitor.prototype.visitIcu = function (icu, context) {
            var _this = this;
            var nodes = [new xml.Text("{" + icu.expressionPlaceholder + ", " + icu.type + ", ")];
            Object.keys(icu.cases).forEach(function (c) {
                nodes.push.apply(nodes, tslib_1.__spread([new xml.Text(c + " {")], icu.cases[c].visit(_this), [new xml.Text("} ")]));
            });
            nodes.push(new xml.Text("}"));
            return nodes;
        };
        _Visitor.prototype.visitTagPlaceholder = function (ph, context) {
            var startTagAsText = new xml.Text("<" + ph.tag + ">");
            var startEx = new xml.Tag(_EXAMPLE_TAG, {}, [startTagAsText]);
            // TC requires PH to have a non empty EX, and uses the text node to show the "original" value.
            var startTagPh = new xml.Tag(_PLACEHOLDER_TAG, { name: ph.startName }, [startEx, startTagAsText]);
            if (ph.isVoid) {
                // void tags have no children nor closing tags
                return [startTagPh];
            }
            var closeTagAsText = new xml.Text("</" + ph.tag + ">");
            var closeEx = new xml.Tag(_EXAMPLE_TAG, {}, [closeTagAsText]);
            // TC requires PH to have a non empty EX, and uses the text node to show the "original" value.
            var closeTagPh = new xml.Tag(_PLACEHOLDER_TAG, { name: ph.closeName }, [closeEx, closeTagAsText]);
            return tslib_1.__spread([startTagPh], this.serialize(ph.children), [closeTagPh]);
        };
        _Visitor.prototype.visitPlaceholder = function (ph, context) {
            var interpolationAsText = new xml.Text("{{" + ph.value + "}}");
            // Example tag needs to be not-empty for TC.
            var exTag = new xml.Tag(_EXAMPLE_TAG, {}, [interpolationAsText]);
            return [
                // TC requires PH to have a non empty EX, and uses the text node to show the "original" value.
                new xml.Tag(_PLACEHOLDER_TAG, { name: ph.name }, [exTag, interpolationAsText])
            ];
        };
        _Visitor.prototype.visitIcuPlaceholder = function (ph, context) {
            var icuExpression = ph.value.expression;
            var icuType = ph.value.type;
            var icuCases = Object.keys(ph.value.cases).map(function (value) { return value + ' {...}'; }).join(' ');
            var icuAsText = new xml.Text("{" + icuExpression + ", " + icuType + ", " + icuCases + "}");
            var exTag = new xml.Tag(_EXAMPLE_TAG, {}, [icuAsText]);
            return [
                // TC requires PH to have a non empty EX, and uses the text node to show the "original" value.
                new xml.Tag(_PLACEHOLDER_TAG, { name: ph.name }, [exTag, icuAsText])
            ];
        };
        _Visitor.prototype.serialize = function (nodes) {
            var _this = this;
            return [].concat.apply([], tslib_1.__spread(nodes.map(function (node) { return node.visit(_this); })));
        };
        return _Visitor;
    }());
    function digest(message) {
        return digest_1.decimalDigest(message);
    }
    exports.digest = digest;
    // TC requires at least one non-empty example on placeholders
    var ExampleVisitor = /** @class */ (function () {
        function ExampleVisitor() {
        }
        ExampleVisitor.prototype.addDefaultExamples = function (node) {
            node.visit(this);
            return node;
        };
        ExampleVisitor.prototype.visitTag = function (tag) {
            var _this = this;
            if (tag.name === _PLACEHOLDER_TAG) {
                if (!tag.children || tag.children.length == 0) {
                    var exText = new xml.Text(tag.attrs['name'] || '...');
                    tag.children = [new xml.Tag(_EXAMPLE_TAG, {}, [exText])];
                }
            }
            else if (tag.children) {
                tag.children.forEach(function (node) { return node.visit(_this); });
            }
        };
        ExampleVisitor.prototype.visitText = function (text) { };
        ExampleVisitor.prototype.visitDeclaration = function (decl) { };
        ExampleVisitor.prototype.visitDoctype = function (doctype) { };
        return ExampleVisitor;
    }());
    // XMB/XTB placeholders can only contain A-Z, 0-9 and _
    function toPublicName(internalName) {
        return internalName.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    }
    exports.toPublicName = toPublicName;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieG1iLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL2kxOG4vc2VyaWFsaXplcnMveG1iLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7OztJQUVILDREQUF3QztJQUd4QyxnRkFBb0Y7SUFDcEYsdUVBQW9DO0lBRXBDLElBQU0sYUFBYSxHQUFHLGVBQWUsQ0FBQztJQUN0QyxJQUFNLFlBQVksR0FBRyxLQUFLLENBQUM7SUFDM0IsSUFBTSxnQkFBZ0IsR0FBRyxJQUFJLENBQUM7SUFDOUIsSUFBTSxZQUFZLEdBQUcsSUFBSSxDQUFDO0lBQzFCLElBQU0sV0FBVyxHQUFHLFFBQVEsQ0FBQztJQUU3QixJQUFNLFFBQVEsR0FBRyx1akJBa0JPLENBQUM7SUFFekI7UUFBeUIsK0JBQVU7UUFBbkM7O1FBcURBLENBQUM7UUFwREMsbUJBQUssR0FBTCxVQUFNLFFBQXdCLEVBQUUsTUFBbUI7WUFDakQsSUFBTSxjQUFjLEdBQUcsSUFBSSxjQUFjLEVBQUUsQ0FBQztZQUM1QyxJQUFNLE9BQU8sR0FBRyxJQUFJLFFBQVEsRUFBRSxDQUFDO1lBQy9CLElBQUksUUFBUSxHQUFHLElBQUksR0FBRyxDQUFDLEdBQUcsQ0FBQyxhQUFhLENBQUMsQ0FBQztZQUUxQyxRQUFRLENBQUMsT0FBTyxDQUFDLFVBQUEsT0FBTztnQkFDdEIsSUFBTSxLQUFLLEdBQTBCLEVBQUMsRUFBRSxFQUFFLE9BQU8sQ0FBQyxFQUFFLEVBQUMsQ0FBQztnQkFFdEQsSUFBSSxPQUFPLENBQUMsV0FBVyxFQUFFO29CQUN2QixLQUFLLENBQUMsTUFBTSxDQUFDLEdBQUcsT0FBTyxDQUFDLFdBQVcsQ0FBQztpQkFDckM7Z0JBRUQsSUFBSSxPQUFPLENBQUMsT0FBTyxFQUFFO29CQUNuQixLQUFLLENBQUMsU0FBUyxDQUFDLEdBQUcsT0FBTyxDQUFDLE9BQU8sQ0FBQztp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aHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7ZGVjaW1hbERpZ2VzdH0gZnJvbSAnLi4vZGlnZXN0JztcbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi4vaTE4bl9hc3QnO1xuXG5pbXBvcnQge1BsYWNlaG9sZGVyTWFwcGVyLCBTZXJpYWxpemVyLCBTaW1wbGVQbGFjZWhvbGRlck1hcHBlcn0gZnJvbSAnLi9zZXJpYWxpemVyJztcbmltcG9ydCAqIGFzIHhtbCBmcm9tICcuL3htbF9oZWxwZXInO1xuXG5jb25zdCBfTUVTU0FHRVNfVEFHID0gJ21lc3NhZ2VidW5kbGUnO1xuY29uc3QgX01FU1NBR0VfVEFHID0gJ21zZyc7XG5jb25zdCBfUExBQ0VIT0xERVJfVEFHID0gJ3BoJztcbmNvbnN0IF9FWEFNUExFX1RBRyA9ICdleCc7XG5jb25zdCBfU09VUkNFX1RBRyA9ICdzb3VyY2UnO1xuXG5jb25zdCBfRE9DVFlQRSA9IGA8IUVMRU1FTlQgbWVzc2FnZWJ1bmRsZSAobXNnKSo+XG48IUFUVExJU1QgbWVzc2FnZWJ1bmRsZSBjbGFzcyBDREFUQSAjSU1QTElFRD5cblxuPCFFTEVNRU5UIG1zZyAoI1BDREFUQXxwaHxzb3VyY2UpKj5cbjwhQVRUTElTVCBtc2cgaWQgQ0RBVEEgI0lNUExJRUQ+XG48IUFUVExJU1QgbXNnIHNlcSBDREFUQSAjSU1QTElFRD5cbjwhQVRUTElTVCBtc2cgbmFtZSBDREFUQSAjSU1QTElFRD5cbjwhQVRUTElTVCBtc2cgZGVzYyBDREFUQSAjSU1QTElFRD5cbjwhQVRUTElTVCBtc2cgbWVhbmluZyBDREFUQSAjSU1QTElFRD5cbjwhQVRUTElTVCBtc2cgb2Jzb2xldGUgKG9ic29sZXRlKSAjSU1QTElFRD5cbjwhQVRUTElTVCBtc2cgeG1sOnNwYWNlIChkZWZhdWx0fHByZXNlcnZlKSBcImRlZmF1bHRcIj5cbjwhQVRUTElTVCBtc2cgaXNfaGlkZGVuIENEQVRBICNJTVBMSUVEPlxuXG48IUVMRU1FTlQgc291cmNlICgjUENEQVRBKT5cblxuPCFFTEVNRU5UIHBoICgjUENEQVRBfGV4KSo+XG48IUFUVExJU1QgcGggbmFtZSBDREFUQSAjUkVRVUlSRUQ+XG5cbjwhRUxFTUVOVCBleCAoI1BDREFUQSk+YDtcblxuZXhwb3J0IGNsYXNzIFhtYiBleHRlbmRzIFNlcmlhbGl6ZXIge1xuICB3cml0ZShtZXNzYWdlczogaTE4bi5NZXNzYWdlW10sIGxvY2FsZTogc3RyaW5nfG51bGwpOiBzdHJpbmcge1xuICAgIGNvbnN0IGV4YW1wbGVWaXNpdG9yID0gbmV3IEV4YW1wbGVWaXNpdG9yKCk7XG4gICAgY29uc3QgdmlzaXRvciA9IG5ldyBfVmlzaXRvcigpO1xuICAgIGxldCByb290Tm9kZSA9IG5ldyB4bWwuVGFnKF9NRVNTQUdFU19UQUcpO1xuXG4gICAgbWVzc2FnZXMuZm9yRWFjaChtZXNzYWdlID0+IHtcbiAgICAgIGNvbnN0IGF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30gPSB7aWQ6IG1lc3NhZ2UuaWR9O1xuXG4gICAgICBpZiAobWVzc2FnZS5kZXNjcmlwdGlvbikge1xuICAgICAgICBhdHRyc1snZGVzYyddID0gbWVzc2FnZS5kZXNjcmlwdGlvbjtcbiAgICAgIH1cblxuICAgICAgaWYgKG1lc3NhZ2UubWVhbmluZykge1xuICAgICAgICBhdHRyc1snbWVhbmluZyddID0gbWVzc2FnZS5tZWFuaW5nO1xuICAgICAgfVxuXG4gICAgICBsZXQgc291cmNlVGFnczogeG1sLlRhZ1tdID0gW107XG4gICAgICBtZXNzYWdlLnNvdXJjZXMuZm9yRWFjaCgoc291cmNlOiBpMThuLk1lc3NhZ2VTcGFuKSA9PiB7XG4gICAgICAgIHNvdXJjZVRhZ3MucHVzaChuZXcgeG1sLlRhZyhfU09VUkNFX1RBRywge30sIFtcbiAgICAgICAgICBuZXcgeG1sLlRleHQoXG4gICAgICAgICAgICAgIGAke3NvdXJjZS5maWxlUGF0aH06JHtzb3VyY2Uuc3RhcnRMaW5lfSR7c291cmNlLmVuZExpbmUgIT09IHNvdXJjZS5zdGFydExpbmUgPyAnLCcgKyBzb3VyY2UuZW5kTGluZSA6ICcnfWApXG4gICAgICAgIF0pKTtcbiAgICAgIH0pO1xuXG4gICAgICByb290Tm9kZS5jaGlsZHJlbi5wdXNoKFxuICAgICAgICAgIG5ldyB4bWwuQ1IoMiksXG4gICAgICAgICAgbmV3IHhtbC5UYWcoX01FU1NBR0VfVEFHLCBhdHRycywgWy4uLnNvdXJjZVRhZ3MsIC4uLnZpc2l0b3Iuc2VyaWFsaXplKG1lc3NhZ2Uubm9kZXMpXSkpO1xuICAgIH0pO1xuXG4gICAgcm9vdE5vZGUuY2hpbGRyZW4ucHVzaChuZXcgeG1sLkNSKCkpO1xuXG4gICAgcmV0dXJuIHhtbC5zZXJpYWxpemUoW1xuICAgICAgbmV3IHhtbC5EZWNsYXJhdGlvbih7dmVyc2lvbjogJzEuMCcsIGVuY29kaW5nOiAnVVRGLTgnfSksXG4gICAgICBuZXcgeG1sLkNSKCksXG4gICAgICBuZXcgeG1sLkRvY3R5cGUoX01FU1NBR0VTX1RBRywgX0RPQ1RZUEUpLFxuICAgICAgbmV3IHhtbC5DUigpLFxuICAgICAgZXhhbXBsZVZpc2l0b3IuYWRkRGVmYXVsdEV4YW1wbGVzKHJvb3ROb2RlKSxcbiAgICAgIG5ldyB4bWwuQ1IoKSxcbiAgICBdKTtcbiAgfVxuXG4gIGxvYWQoY29udGVudDogc3RyaW5nLCB1cmw6IHN0cmluZyk6XG4gICAgICB7bG9jYWxlOiBzdHJpbmcsIGkxOG5Ob2Rlc0J5TXNnSWQ6IHtbbXNnSWQ6IHN0cmluZ106IGkxOG4uTm9kZVtdfX0ge1xuICAgIHRocm93IG5ldyBFcnJvcignVW5zdXBwb3J0ZWQnKTtcbiAgfVxuXG4gIGRpZ2VzdChtZXNzYWdlOiBpMThuLk1lc3NhZ2UpOiBzdHJpbmcgeyByZXR1cm4gZGlnZXN0KG1lc3NhZ2UpOyB9XG5cblxuICBjcmVhdGVOYW1lTWFwcGVyKG1lc3NhZ2U6IGkxOG4uTWVzc2FnZSk6IFBsYWNlaG9sZGVyTWFwcGVyIHtcbiAgICByZXR1cm4gbmV3IFNpbXBsZVBsYWNlaG9sZGVyTWFwcGVyKG1lc3NhZ2UsIHRvUHVibGljTmFtZSk7XG4gIH1cbn1cblxuY2xhc3MgX1Zpc2l0b3IgaW1wbGVtZW50cyBpMThuLlZpc2l0b3Ige1xuICB2aXNpdFRleHQodGV4dDogaTE4bi5UZXh0LCBjb250ZXh0PzogYW55KTogeG1sLk5vZGVbXSB7IHJldHVybiBbbmV3IHhtbC5UZXh0KHRleHQudmFsdWUpXTsgfVxuXG4gIHZpc2l0Q29udGFpbmVyKGNvbnRhaW5lcjogaTE4bi5Db250YWluZXIsIGNvbnRleHQ6IGFueSk6IHhtbC5Ob2RlW10ge1xuICAgIGNvbnN0IG5vZGVzOiB4bWwuTm9kZVtdID0gW107XG4gICAgY29udGFpbmVyLmNoaWxkcmVuLmZvckVhY2goKG5vZGU6IGkxOG4uTm9kZSkgPT4gbm9kZXMucHVzaCguLi5ub2RlLnZpc2l0KHRoaXMpKSk7XG4gICAgcmV0dXJuIG5vZGVzO1xuICB9XG5cbiAgdmlzaXRJY3UoaWN1OiBpMThuLkljdSwgY29udGV4dD86IGFueSk6IHhtbC5Ob2RlW10ge1xuICAgIGNvbnN0IG5vZGVzID0gW25ldyB4bWwuVGV4dChgeyR7aWN1LmV4cHJlc3Npb25QbGFjZWhvbGRlcn0sICR7aWN1LnR5cGV9LCBgKV07XG5cbiAgICBPYmplY3Qua2V5cyhpY3UuY2FzZXMpLmZvckVhY2goKGM6IHN0cmluZykgPT4ge1xuICAgICAgbm9kZXMucHVzaChuZXcgeG1sLlRleHQoYCR7Y30ge2ApLCAuLi5pY3UuY2FzZXNbY10udmlzaXQodGhpcyksIG5ldyB4bWwuVGV4dChgfSBgKSk7XG4gICAgfSk7XG5cbiAgICBub2Rlcy5wdXNoKG5ldyB4bWwuVGV4dChgfWApKTtcblxuICAgIHJldHVybiBub2RlcztcbiAgfVxuXG4gIHZpc2l0VGFnUGxhY2Vob2xkZXIocGg6IGkxOG4uVGFnUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiB4bWwuTm9kZVtdIHtcbiAgICBjb25zdCBzdGFydFRhZ0FzVGV4dCA9IG5ldyB4bWwuVGV4dChgPCR7cGgudGFnfT5gKTtcbiAgICBjb25zdCBzdGFydEV4ID0gbmV3IHhtbC5UYWcoX0VYQU1QTEVfVEFHLCB7fSwgW3N0YXJ0VGFnQXNUZXh0XSk7XG4gICAgLy8gVEMgcmVxdWlyZXMgUEggdG8gaGF2ZSBhIG5vbiBlbXB0eSBFWCwgYW5kIHVzZXMgdGhlIHRleHQgbm9kZSB0byBzaG93IHRoZSBcIm9yaWdpbmFsXCIgdmFsdWUuXG4gICAgY29uc3Qgc3RhcnRUYWdQaCA9XG4gICAgICAgIG5ldyB4bWwuVGFnKF9QTEFDRUhPTERFUl9UQUcsIHtuYW1lOiBwaC5zdGFydE5hbWV9LCBbc3RhcnRFeCwgc3RhcnRUYWdBc1RleHRdKTtcbiAgICBpZiAocGguaXNWb2lkKSB7XG4gICAgICAvLyB2b2lkIHRhZ3MgaGF2ZSBubyBjaGlsZHJlbiBub3IgY2xvc2luZyB0YWdzXG4gICAgICByZXR1cm4gW3N0YXJ0VGFnUGhdO1xuICAgIH1cblxuICAgIGNvbnN0IGNsb3NlVGFnQXNUZXh0ID0gbmV3IHhtbC5UZXh0KGA8LyR7cGgudGFnfT5gKTtcbiAgICBjb25zdCBjbG9zZUV4ID0gbmV3IHhtbC5UYWcoX0VYQU1QTEVfVEFHLCB7fSwgW2Nsb3NlVGFnQXNUZXh0XSk7XG4gICAgLy8gVEMgcmVxdWlyZXMgUEggdG8gaGF2ZSBhIG5vbiBlbXB0eSBFWCwgYW5kIHVzZXMgdGhlIHRleHQgbm9kZSB0byBzaG93IHRoZSBcIm9yaWdpbmFsXCIgdmFsdWUuXG4gICAgY29uc3QgY2xvc2VUYWdQaCA9XG4gICAgICAgIG5ldyB4bWwuVGFnKF9QTEFDRUhPTERFUl9UQUcsIHtuYW1lOiBwaC5jbG9zZU5hbWV9LCBbY2xvc2VFeCwgY2xvc2VUYWdBc1RleHRdKTtcblxuICAgIHJldHVybiBbc3RhcnRUYWdQaCwgLi4udGhpcy5zZXJpYWxpemUocGguY2hpbGRyZW4pLCBjbG9zZVRhZ1BoXTtcbiAgfVxuXG4gIHZpc2l0UGxhY2Vob2xkZXIocGg6IGkxOG4uUGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiB4bWwuTm9kZVtdIHtcbiAgICBjb25zdCBpbnRlcnBvbGF0aW9uQXNUZXh0ID0gbmV3IHhtbC5UZXh0KGB7eyR7cGgudmFsdWV9fX1gKTtcbiAgICAvLyBFeGFtcGxlIHRhZyBuZWVkcyB0byBiZSBub3QtZW1wdHkgZm9yIFRDLlxuICAgIGNvbnN0IGV4VGFnID0gbmV3IHhtbC5UYWcoX0VYQU1QTEVfVEFHLCB7fSwgW2ludGVycG9sYXRpb25Bc1RleHRdKTtcbiAgICByZXR1cm4gW1xuICAgICAgLy8gVEMgcmVxdWlyZXMgUEggdG8gaGF2ZSBhIG5vbiBlbXB0eSBFWCwgYW5kIHVzZXMgdGhlIHRleHQgbm9kZSB0byBzaG93IHRoZSBcIm9yaWdpbmFsXCIgdmFsdWUuXG4gICAgICBuZXcgeG1sLlRhZyhfUExBQ0VIT0xERVJfVEFHLCB7bmFtZTogcGgubmFtZX0sIFtleFRhZywgaW50ZXJwb2xhdGlvbkFzVGV4dF0pXG4gICAgXTtcbiAgfVxuXG4gIHZpc2l0SWN1UGxhY2Vob2xkZXIocGg6IGkxOG4uSWN1UGxhY2Vob2xkZXIsIGNvbnRleHQ/OiBhbnkpOiB4bWwuTm9kZVtdIHtcbiAgICBjb25zdCBpY3VFeHByZXNzaW9uID0gcGgudmFsdWUuZXhwcmVzc2lvbjtcbiAgICBjb25zdCBpY3VUeXBlID0gcGgudmFsdWUudHlwZTtcbiAgICBjb25zdCBpY3VDYXNlcyA9IE9iamVjdC5rZXlzKHBoLnZhbHVlLmNhc2VzKS5tYXAoKHZhbHVlOiBzdHJpbmcpID0+IHZhbHVlICsgJyB7Li4ufScpLmpvaW4oJyAnKTtcbiAgICBjb25zdCBpY3VBc1RleHQgPSBuZXcgeG1sLlRleHQoYHske2ljdUV4cHJlc3Npb259LCAke2ljdVR5cGV9LCAke2ljdUNhc2VzfX1gKTtcbiAgICBjb25zdCBleFRhZyA9IG5ldyB4bWwuVGFnKF9FWEFNUExFX1RBRywge30sIFtpY3VBc1RleHRdKTtcbiAgICByZXR1cm4gW1xuICAgICAgLy8gVEMgcmVxdWlyZXMgUEggdG8gaGF2ZSBhIG5vbiBlbXB0eSBFWCwgYW5kIHVzZXMgdGhlIHRleHQgbm9kZSB0byBzaG93IHRoZSBcIm9yaWdpbmFsXCIgdmFsdWUuXG4gICAgICBuZXcgeG1sLlRhZyhfUExBQ0VIT0xERVJfVEFHLCB7bmFtZTogcGgubmFtZX0sIFtleFRhZywgaWN1QXNUZXh0XSlcbiAgICBdO1xuICB9XG5cbiAgc2VyaWFsaXplKG5vZGVzOiBpMThuLk5vZGVbXSk6IHhtbC5Ob2RlW10ge1xuICAgIHJldHVybiBbXS5jb25jYXQoLi4ubm9kZXMubWFwKG5vZGUgPT4gbm9kZS52aXNpdCh0aGlzKSkpO1xuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkaWdlc3QobWVzc2FnZTogaTE4bi5NZXNzYWdlKTogc3RyaW5nIHtcbiAgcmV0dXJuIGRlY2ltYWxEaWdlc3QobWVzc2FnZSk7XG59XG5cbi8vIFRDIHJlcXVpcmVzIGF0IGxlYXN0IG9uZSBub24tZW1wdHkgZXhhbXBsZSBvbiBwbGFjZWhvbGRlcnNcbmNsYXNzIEV4YW1wbGVWaXNpdG9yIGltcGxlbWVudHMgeG1sLklWaXNpdG9yIHtcbiAgYWRkRGVmYXVsdEV4YW1wbGVzKG5vZGU6IHhtbC5Ob2RlKTogeG1sLk5vZGUge1xuICAgIG5vZGUudmlzaXQodGhpcyk7XG4gICAgcmV0dXJuIG5vZGU7XG4gIH1cblxuICB2aXNpdFRhZyh0YWc6IHhtbC5UYWcpOiB2b2lkIHtcbiAgICBpZiAodGFnLm5hbWUgPT09IF9QTEFDRUhPTERFUl9UQUcpIHtcbiAgICAgIGlmICghdGFnLmNoaWxkcmVuIHx8IHRhZy5jaGlsZHJlbi5sZW5ndGggPT0gMCkge1xuICAgICAgICBjb25zdCBleFRleHQgPSBuZXcgeG1sLlRleHQodGFnLmF0dHJzWyduYW1lJ10gfHwgJy4uLicpO1xuICAgICAgICB0YWcuY2hpbGRyZW4gPSBbbmV3IHhtbC5UYWcoX0VYQU1QTEVfVEFHLCB7fSwgW2V4VGV4dF0pXTtcbiAgICAgIH1cbiAgICB9IGVsc2UgaWYgKHRhZy5jaGlsZHJlbikge1xuICAgICAgdGFnLmNoaWxkcmVuLmZvckVhY2gobm9kZSA9PiBub2RlLnZpc2l0KHRoaXMpKTtcbiAgICB9XG4gIH1cblxuICB2aXNpdFRleHQodGV4dDogeG1sLlRleHQpOiB2b2lkIHt9XG4gIHZpc2l0RGVjbGFyYXRpb24oZGVjbDogeG1sLkRlY2xhcmF0aW9uKTogdm9pZCB7fVxuICB2aXNpdERvY3R5cGUoZG9jdHlwZTogeG1sLkRvY3R5cGUpOiB2b2lkIHt9XG59XG5cbi8vIFhNQi9YVEIgcGxhY2Vob2xkZXJzIGNhbiBvbmx5IGNvbnRhaW4gQS1aLCAwLTkgYW5kIF9cbmV4cG9ydCBmdW5jdGlvbiB0b1B1YmxpY05hbWUoaW50ZXJuYWxOYW1lOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gaW50ZXJuYWxOYW1lLnRvVXBwZXJDYXNlKCkucmVwbGFjZSgvW15BLVowLTlfXS9nLCAnXycpO1xufVxuIl19