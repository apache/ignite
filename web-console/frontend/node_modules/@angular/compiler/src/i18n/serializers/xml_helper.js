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
        define("@angular/compiler/src/i18n/serializers/xml_helper", ["require", "exports", "tslib"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var tslib_1 = require("tslib");
    var _Visitor = /** @class */ (function () {
        function _Visitor() {
        }
        _Visitor.prototype.visitTag = function (tag) {
            var _this = this;
            var strAttrs = this._serializeAttributes(tag.attrs);
            if (tag.children.length == 0) {
                return "<" + tag.name + strAttrs + "/>";
            }
            var strChildren = tag.children.map(function (node) { return node.visit(_this); });
            return "<" + tag.name + strAttrs + ">" + strChildren.join('') + "</" + tag.name + ">";
        };
        _Visitor.prototype.visitText = function (text) { return text.value; };
        _Visitor.prototype.visitDeclaration = function (decl) {
            return "<?xml" + this._serializeAttributes(decl.attrs) + " ?>";
        };
        _Visitor.prototype._serializeAttributes = function (attrs) {
            var strAttrs = Object.keys(attrs).map(function (name) { return name + "=\"" + attrs[name] + "\""; }).join(' ');
            return strAttrs.length > 0 ? ' ' + strAttrs : '';
        };
        _Visitor.prototype.visitDoctype = function (doctype) {
            return "<!DOCTYPE " + doctype.rootTag + " [\n" + doctype.dtd + "\n]>";
        };
        return _Visitor;
    }());
    var _visitor = new _Visitor();
    function serialize(nodes) {
        return nodes.map(function (node) { return node.visit(_visitor); }).join('');
    }
    exports.serialize = serialize;
    var Declaration = /** @class */ (function () {
        function Declaration(unescapedAttrs) {
            var _this = this;
            this.attrs = {};
            Object.keys(unescapedAttrs).forEach(function (k) {
                _this.attrs[k] = escapeXml(unescapedAttrs[k]);
            });
        }
        Declaration.prototype.visit = function (visitor) { return visitor.visitDeclaration(this); };
        return Declaration;
    }());
    exports.Declaration = Declaration;
    var Doctype = /** @class */ (function () {
        function Doctype(rootTag, dtd) {
            this.rootTag = rootTag;
            this.dtd = dtd;
        }
        Doctype.prototype.visit = function (visitor) { return visitor.visitDoctype(this); };
        return Doctype;
    }());
    exports.Doctype = Doctype;
    var Tag = /** @class */ (function () {
        function Tag(name, unescapedAttrs, children) {
            var _this = this;
            if (unescapedAttrs === void 0) { unescapedAttrs = {}; }
            if (children === void 0) { children = []; }
            this.name = name;
            this.children = children;
            this.attrs = {};
            Object.keys(unescapedAttrs).forEach(function (k) {
                _this.attrs[k] = escapeXml(unescapedAttrs[k]);
            });
        }
        Tag.prototype.visit = function (visitor) { return visitor.visitTag(this); };
        return Tag;
    }());
    exports.Tag = Tag;
    var Text = /** @class */ (function () {
        function Text(unescapedValue) {
            this.value = escapeXml(unescapedValue);
        }
        Text.prototype.visit = function (visitor) { return visitor.visitText(this); };
        return Text;
    }());
    exports.Text = Text;
    var CR = /** @class */ (function (_super) {
        tslib_1.__extends(CR, _super);
        function CR(ws) {
            if (ws === void 0) { ws = 0; }
            return _super.call(this, "\n" + new Array(ws + 1).join(' ')) || this;
        }
        return CR;
    }(Text));
    exports.CR = CR;
    var _ESCAPED_CHARS = [
        [/&/g, '&amp;'],
        [/"/g, '&quot;'],
        [/'/g, '&apos;'],
        [/</g, '&lt;'],
        [/>/g, '&gt;'],
    ];
    // Escape `_ESCAPED_CHARS` characters in the given text with encoded entities
    function escapeXml(text) {
        return _ESCAPED_CHARS.reduce(function (text, entry) { return text.replace(entry[0], entry[1]); }, text);
    }
    exports.escapeXml = escapeXml;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieG1sX2hlbHBlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pMThuL3NlcmlhbGl6ZXJzL3htbF9oZWxwZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7O0lBU0g7UUFBQTtRQTBCQSxDQUFDO1FBekJDLDJCQUFRLEdBQVIsVUFBUyxHQUFRO1lBQWpCLGlCQVNDO1lBUkMsSUFBTSxRQUFRLEdBQUcsSUFBSSxDQUFDLG9CQUFvQixDQUFDLEdBQUcsQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUV0RCxJQUFJLEdBQUcsQ0FBQyxRQUFRLENBQUMsTUFBTSxJQUFJLENBQUMsRUFBRTtnQkFDNUIsT0FBTyxNQUFJLEdBQUcsQ0FBQyxJQUFJLEdBQUcsUUFBUSxPQUFJLENBQUM7YUFDcEM7WUFFRCxJQUFNLFdBQVcsR0FBRyxHQUFHLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxVQUFBLElBQUksSUFBSSxPQUFBLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSSxDQUFDLEVBQWhCLENBQWdCLENBQUMsQ0FBQztZQUMvRCxPQUFPLE1BQUksR0FBRyxDQUFDLElBQUksR0FBRyxRQUFRLFNBQUksV0FBVyxDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsVUFBSyxHQUFHLENBQUMsSUFBSSxNQUFHLENBQUM7UUFDekUsQ0FBQztRQUVELDRCQUFTLEdBQVQsVUFBVSxJQUFVLElBQVksT0FBTyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztRQUVwRCxtQ0FBZ0IsR0FBaEIsVUFBaUIsSUFBaUI7WUFDaEMsT0FBTyxVQUFRLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLFFBQUssQ0FBQztRQUM1RCxDQUFDO1FBRU8sdUNBQW9CLEdBQTVCLFVBQTZCLEtBQTRCO1lBQ3ZELElBQU0sUUFBUSxHQUFHLE1BQU0s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LENBQUMsQ0FBQyxDQUFDO1FBQ25FLFdBQUM7SUFBRCxDQUFDLEFBTEQsSUFLQztJQUxZLG9CQUFJO0lBT2pCO1FBQXdCLDhCQUFJO1FBQzFCLFlBQVksRUFBYztZQUFkLG1CQUFBLEVBQUEsTUFBYzttQkFBSSxrQkFBTSxPQUFLLElBQUksS0FBSyxDQUFDLEVBQUUsR0FBRyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFHLENBQUM7UUFBRSxDQUFDO1FBQzVFLFNBQUM7SUFBRCxDQUFDLEFBRkQsQ0FBd0IsSUFBSSxHQUUzQjtJQUZZLGdCQUFFO0lBSWYsSUFBTSxjQUFjLEdBQXVCO1FBQ3pDLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQztRQUNmLENBQUMsSUFBSSxFQUFFLFFBQVEsQ0FBQztRQUNoQixDQUFDLElBQUksRUFBRSxRQUFRLENBQUM7UUFDaEIsQ0FBQyxJQUFJLEVBQUUsTUFBTSxDQUFDO1FBQ2QsQ0FBQyxJQUFJLEVBQUUsTUFBTSxDQUFDO0tBQ2YsQ0FBQztJQUVGLDZFQUE2RTtJQUM3RSxTQUFnQixTQUFTLENBQUMsSUFBWTtRQUNwQyxPQUFPLGNBQWMsQ0FBQyxNQUFNLENBQ3hCLFVBQUMsSUFBWSxFQUFFLEtBQXVCLElBQUssT0FBQSxJQUFJLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBaEMsQ0FBZ0MsRUFBRSxJQUFJLENBQUMsQ0FBQztJQUN6RixDQUFDO0lBSEQsOEJBR0MiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmV4cG9ydCBpbnRlcmZhY2UgSVZpc2l0b3Ige1xuICB2aXNpdFRhZyh0YWc6IFRhZyk6IGFueTtcbiAgdmlzaXRUZXh0KHRleHQ6IFRleHQpOiBhbnk7XG4gIHZpc2l0RGVjbGFyYXRpb24oZGVjbDogRGVjbGFyYXRpb24pOiBhbnk7XG4gIHZpc2l0RG9jdHlwZShkb2N0eXBlOiBEb2N0eXBlKTogYW55O1xufVxuXG5jbGFzcyBfVmlzaXRvciBpbXBsZW1lbnRzIElWaXNpdG9yIHtcbiAgdmlzaXRUYWcodGFnOiBUYWcpOiBzdHJpbmcge1xuICAgIGNvbnN0IHN0ckF0dHJzID0gdGhpcy5fc2VyaWFsaXplQXR0cmlidXRlcyh0YWcuYXR0cnMpO1xuXG4gICAgaWYgKHRhZy5jaGlsZHJlbi5sZW5ndGggPT0gMCkge1xuICAgICAgcmV0dXJuIGA8JHt0YWcubmFtZX0ke3N0ckF0dHJzfS8+YDtcbiAgICB9XG5cbiAgICBjb25zdCBzdHJDaGlsZHJlbiA9IHRhZy5jaGlsZHJlbi5tYXAobm9kZSA9PiBub2RlLnZpc2l0KHRoaXMpKTtcbiAgICByZXR1cm4gYDwke3RhZy5uYW1lfSR7c3RyQXR0cnN9PiR7c3RyQ2hpbGRyZW4uam9pbignJyl9PC8ke3RhZy5uYW1lfT5gO1xuICB9XG5cbiAgdmlzaXRUZXh0KHRleHQ6IFRleHQpOiBzdHJpbmcgeyByZXR1cm4gdGV4dC52YWx1ZTsgfVxuXG4gIHZpc2l0RGVjbGFyYXRpb24oZGVjbDogRGVjbGFyYXRpb24pOiBzdHJpbmcge1xuICAgIHJldHVybiBgPD94bWwke3RoaXMuX3NlcmlhbGl6ZUF0dHJpYnV0ZXMoZGVjbC5hdHRycyl9ID8+YDtcbiAgfVxuXG4gIHByaXZhdGUgX3NlcmlhbGl6ZUF0dHJpYnV0ZXMoYXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nfSkge1xuICAgIGNvbnN0IHN0ckF0dHJzID0gT2JqZWN0LmtleXMoYXR0cnMpLm1hcCgobmFtZTogc3RyaW5nKSA9PiBgJHtuYW1lfT1cIiR7YXR0cnNbbmFtZV19XCJgKS5qb2luKCcgJyk7XG4gICAgcmV0dXJuIHN0ckF0dHJzLmxlbmd0aCA+IDAgPyAnICcgKyBzdHJBdHRycyA6ICcnO1xuICB9XG5cbiAgdmlzaXREb2N0eXBlKGRvY3R5cGU6IERvY3R5cGUpOiBhbnkge1xuICAgIHJldHVybiBgPCFET0NUWVBFICR7ZG9jdHlwZS5yb290VGFnfSBbXFxuJHtkb2N0eXBlLmR0ZH1cXG5dPmA7XG4gIH1cbn1cblxuY29uc3QgX3Zpc2l0b3IgPSBuZXcgX1Zpc2l0b3IoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIHNlcmlhbGl6ZShub2RlczogTm9kZVtdKTogc3RyaW5nIHtcbiAgcmV0dXJuIG5vZGVzLm1hcCgobm9kZTogTm9kZSk6IHN0cmluZyA9PiBub2RlLnZpc2l0KF92aXNpdG9yKSkuam9pbignJyk7XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgTm9kZSB7IHZpc2l0KHZpc2l0b3I6IElWaXNpdG9yKTogYW55OyB9XG5cbmV4cG9ydCBjbGFzcyBEZWNsYXJhdGlvbiBpbXBsZW1lbnRzIE5vZGUge1xuICBwdWJsaWMgYXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuXG4gIGNvbnN0cnVjdG9yKHVuZXNjYXBlZEF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30pIHtcbiAgICBPYmplY3Qua2V5cyh1bmVzY2FwZWRBdHRycykuZm9yRWFjaCgoazogc3RyaW5nKSA9PiB7XG4gICAgICB0aGlzLmF0dHJzW2tdID0gZXNjYXBlWG1sKHVuZXNjYXBlZEF0dHJzW2tdKTtcbiAgICB9KTtcbiAgfVxuXG4gIHZpc2l0KHZpc2l0b3I6IElWaXNpdG9yKTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXREZWNsYXJhdGlvbih0aGlzKTsgfVxufVxuXG5leHBvcnQgY2xhc3MgRG9jdHlwZSBpbXBsZW1lbnRzIE5vZGUge1xuICBjb25zdHJ1Y3RvcihwdWJsaWMgcm9vdFRhZzogc3RyaW5nLCBwdWJsaWMgZHRkOiBzdHJpbmcpIHt9XG5cbiAgdmlzaXQodmlzaXRvcjogSVZpc2l0b3IpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdERvY3R5cGUodGhpcyk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIFRhZyBpbXBsZW1lbnRzIE5vZGUge1xuICBwdWJsaWMgYXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nfSA9IHt9O1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgICAgcHVibGljIG5hbWU6IHN0cmluZywgdW5lc2NhcGVkQXR0cnM6IHtbazogc3RyaW5nXTogc3RyaW5nfSA9IHt9LFxuICAgICAgcHVibGljIGNoaWxkcmVuOiBOb2RlW10gPSBbXSkge1xuICAgIE9iamVjdC5rZXlzKHVuZXNjYXBlZEF0dHJzKS5mb3JFYWNoKChrOiBzdHJpbmcpID0+IHtcbiAgICAgIHRoaXMuYXR0cnNba10gPSBlc2NhcGVYbWwodW5lc2NhcGVkQXR0cnNba10pO1xuICAgIH0pO1xuICB9XG5cbiAgdmlzaXQodmlzaXRvcjogSVZpc2l0b3IpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdFRhZyh0aGlzKTsgfVxufVxuXG5leHBvcnQgY2xhc3MgVGV4dCBpbXBsZW1lbnRzIE5vZGUge1xuICB2YWx1ZTogc3RyaW5nO1xuICBjb25zdHJ1Y3Rvcih1bmVzY2FwZWRWYWx1ZTogc3RyaW5nKSB7IHRoaXMudmFsdWUgPSBlc2NhcGVYbWwodW5lc2NhcGVkVmFsdWUpOyB9XG5cbiAgdmlzaXQodmlzaXRvcjogSVZpc2l0b3IpOiBhbnkgeyByZXR1cm4gdmlzaXRvci52aXNpdFRleHQodGhpcyk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIENSIGV4dGVuZHMgVGV4dCB7XG4gIGNvbnN0cnVjdG9yKHdzOiBudW1iZXIgPSAwKSB7IHN1cGVyKGBcXG4ke25ldyBBcnJheSh3cyArIDEpLmpvaW4oJyAnKX1gKTsgfVxufVxuXG5jb25zdCBfRVNDQVBFRF9DSEFSUzogW1JlZ0V4cCwgc3RyaW5nXVtdID0gW1xuICBbLyYvZywgJyZhbXA7J10sXG4gIFsvXCIvZywgJyZxdW90OyddLFxuICBbLycvZywgJyZhcG9zOyddLFxuICBbLzwvZywgJyZsdDsnXSxcbiAgWy8+L2csICcmZ3Q7J10sXG5dO1xuXG4vLyBFc2NhcGUgYF9FU0NBUEVEX0NIQVJTYCBjaGFyYWN0ZXJzIGluIHRoZSBnaXZlbiB0ZXh0IHdpdGggZW5jb2RlZCBlbnRpdGllc1xuZXhwb3J0IGZ1bmN0aW9uIGVzY2FwZVhtbCh0ZXh0OiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gX0VTQ0FQRURfQ0hBUlMucmVkdWNlKFxuICAgICAgKHRleHQ6IHN0cmluZywgZW50cnk6IFtSZWdFeHAsIHN0cmluZ10pID0+IHRleHQucmVwbGFjZShlbnRyeVswXSwgZW50cnlbMV0pLCB0ZXh0KTtcbn1cbiJdfQ==