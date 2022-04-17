/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
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
export function serialize(nodes) {
    return nodes.map(function (node) { return node.visit(_visitor); }).join('');
}
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
export { Declaration };
var Doctype = /** @class */ (function () {
    function Doctype(rootTag, dtd) {
        this.rootTag = rootTag;
        this.dtd = dtd;
    }
    Doctype.prototype.visit = function (visitor) { return visitor.visitDoctype(this); };
    return Doctype;
}());
export { Doctype };
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
export { Tag };
var Text = /** @class */ (function () {
    function Text(unescapedValue) {
        this.value = escapeXml(unescapedValue);
    }
    Text.prototype.visit = function (visitor) { return visitor.visitText(this); };
    return Text;
}());
export { Text };
var CR = /** @class */ (function (_super) {
    tslib_1.__extends(CR, _super);
    function CR(ws) {
        if (ws === void 0) { ws = 0; }
        return _super.call(this, "\n" + new Array(ws + 1).join(' ')) || this;
    }
    return CR;
}(Text));
export { CR };
var _ESCAPED_CHARS = [
    [/&/g, '&amp;'],
    [/"/g, '&quot;'],
    [/'/g, '&apos;'],
    [/</g, '&lt;'],
    [/>/g, '&gt;'],
];
// Escape `_ESCAPED_CHARS` characters in the given text with encoded entities
export function escapeXml(text) {
    return _ESCAPED_CHARS.reduce(function (text, entry) { return text.replace(entry[0], entry[1]); }, text);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieG1sX2hlbHBlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pMThuL3NlcmlhbGl6ZXJzL3htbF9oZWxwZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQVNIO0lBQUE7SUEwQkEsQ0FBQztJQXpCQywyQkFBUSxHQUFSLFVBQVMsR0FBUTtRQUFqQixpQkFTQztRQVJDLElBQU0sUUFBUSxHQUFHLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLENBQUM7UUFFdEQsSUFBSSxHQUFHLENBQUMsUUFBUSxDQUFDLE1BQU0sSUFBSSxDQUFDLEVBQUU7WUFDNUIsT0FBTyxNQUFJLEdBQUcsQ0FBQyxJQUFJLEdBQUcsUUFBUSxPQUFJLENBQUM7U0FDcEM7UUFFRCxJQUFNLFdBQVcsR0FBRyxHQUFHLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxVQUFBLElBQUksSUFBSSxPQUFBLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSSxDQUFDLEVBQWhCLENBQWdCLENBQUMsQ0FBQztRQUMvRCxPQUFPLE1BQUksR0FBRyxDQUFDLElBQUksR0FBRyxRQUFRLFNBQUksV0FBVyxDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsVUFBSyxHQUFHLENBQUMsSUFBSSxNQUFHLENBQUM7SUFDekUsQ0FBQztJQUVELDRCQUFTLEdBQVQsVUFBVSxJQUFVLElBQVksT0FBTyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztJQUVwRCxtQ0FBZ0IsR0FBaEIsVUFBaUIsSUFBaUI7UUFDaEMsT0FBTyxVQUFRLElBQUksQ0FBQyxvQkFBb0IsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLFFBQUssQ0FBQztJQUM1RCxDQUFDO0lBRU8sdUNBQW9CLEdBQTVCLFVBQTZCLEtBQTRCO1FBQ3ZELElBQU0sUUFBUSxHQUFHLE1BQU0sQ0FB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Qzs7QUFFRDtJQUF3Qiw4QkFBSTtJQUMxQixZQUFZLEVBQWM7UUFBZCxtQkFBQSxFQUFBLE1BQWM7ZUFBSSxrQkFBTSxPQUFLLElBQUksS0FBSyxDQUFDLEVBQUUsR0FBRyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFHLENBQUM7SUFBRSxDQUFDO0lBQzVFLFNBQUM7QUFBRCxDQUFDLEFBRkQsQ0FBd0IsSUFBSSxHQUUzQjs7QUFFRCxJQUFNLGNBQWMsR0FBdUI7SUFDekMsQ0FBQyxJQUFJLEVBQUUsT0FBTyxDQUFDO0lBQ2YsQ0FBQyxJQUFJLEVBQUUsUUFBUSxDQUFDO0lBQ2hCLENBQUMsSUFBSSxFQUFFLFFBQVEsQ0FBQztJQUNoQixDQUFDLElBQUksRUFBRSxNQUFNLENBQUM7SUFDZCxDQUFDLElBQUksRUFBRSxNQUFNLENBQUM7Q0FDZixDQUFDO0FBRUYsNkVBQTZFO0FBQzdFLE1BQU0sVUFBVSxTQUFTLENBQUMsSUFBWTtJQUNwQyxPQUFPLGNBQWMsQ0FBQyxNQUFNLENBQ3hCLFVBQUMsSUFBWSxFQUFFLEtBQXVCLElBQUssT0FBQSxJQUFJLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsRUFBRSxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBaEMsQ0FBZ0MsRUFBRSxJQUFJLENBQUMsQ0FBQztBQUN6RixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5leHBvcnQgaW50ZXJmYWNlIElWaXNpdG9yIHtcbiAgdmlzaXRUYWcodGFnOiBUYWcpOiBhbnk7XG4gIHZpc2l0VGV4dCh0ZXh0OiBUZXh0KTogYW55O1xuICB2aXNpdERlY2xhcmF0aW9uKGRlY2w6IERlY2xhcmF0aW9uKTogYW55O1xuICB2aXNpdERvY3R5cGUoZG9jdHlwZTogRG9jdHlwZSk6IGFueTtcbn1cblxuY2xhc3MgX1Zpc2l0b3IgaW1wbGVtZW50cyBJVmlzaXRvciB7XG4gIHZpc2l0VGFnKHRhZzogVGFnKTogc3RyaW5nIHtcbiAgICBjb25zdCBzdHJBdHRycyA9IHRoaXMuX3NlcmlhbGl6ZUF0dHJpYnV0ZXModGFnLmF0dHJzKTtcblxuICAgIGlmICh0YWcuY2hpbGRyZW4ubGVuZ3RoID09IDApIHtcbiAgICAgIHJldHVybiBgPCR7dGFnLm5hbWV9JHtzdHJBdHRyc30vPmA7XG4gICAgfVxuXG4gICAgY29uc3Qgc3RyQ2hpbGRyZW4gPSB0YWcuY2hpbGRyZW4ubWFwKG5vZGUgPT4gbm9kZS52aXNpdCh0aGlzKSk7XG4gICAgcmV0dXJuIGA8JHt0YWcubmFtZX0ke3N0ckF0dHJzfT4ke3N0ckNoaWxkcmVuLmpvaW4oJycpfTwvJHt0YWcubmFtZX0+YDtcbiAgfVxuXG4gIHZpc2l0VGV4dCh0ZXh0OiBUZXh0KTogc3RyaW5nIHsgcmV0dXJuIHRleHQudmFsdWU7IH1cblxuICB2aXNpdERlY2xhcmF0aW9uKGRlY2w6IERlY2xhcmF0aW9uKTogc3RyaW5nIHtcbiAgICByZXR1cm4gYDw/eG1sJHt0aGlzLl9zZXJpYWxpemVBdHRyaWJ1dGVzKGRlY2wuYXR0cnMpfSA/PmA7XG4gIH1cblxuICBwcml2YXRlIF9zZXJpYWxpemVBdHRyaWJ1dGVzKGF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30pIHtcbiAgICBjb25zdCBzdHJBdHRycyA9IE9iamVjdC5rZXlzKGF0dHJzKS5tYXAoKG5hbWU6IHN0cmluZykgPT4gYCR7bmFtZX09XCIke2F0dHJzW25hbWVdfVwiYCkuam9pbignICcpO1xuICAgIHJldHVybiBzdHJBdHRycy5sZW5ndGggPiAwID8gJyAnICsgc3RyQXR0cnMgOiAnJztcbiAgfVxuXG4gIHZpc2l0RG9jdHlwZShkb2N0eXBlOiBEb2N0eXBlKTogYW55IHtcbiAgICByZXR1cm4gYDwhRE9DVFlQRSAke2RvY3R5cGUucm9vdFRhZ30gW1xcbiR7ZG9jdHlwZS5kdGR9XFxuXT5gO1xuICB9XG59XG5cbmNvbnN0IF92aXNpdG9yID0gbmV3IF9WaXNpdG9yKCk7XG5cbmV4cG9ydCBmdW5jdGlvbiBzZXJpYWxpemUobm9kZXM6IE5vZGVbXSk6IHN0cmluZyB7XG4gIHJldHVybiBub2Rlcy5tYXAoKG5vZGU6IE5vZGUpOiBzdHJpbmcgPT4gbm9kZS52aXNpdChfdmlzaXRvcikpLmpvaW4oJycpO1xufVxuXG5leHBvcnQgaW50ZXJmYWNlIE5vZGUgeyB2aXNpdCh2aXNpdG9yOiBJVmlzaXRvcik6IGFueTsgfVxuXG5leHBvcnQgY2xhc3MgRGVjbGFyYXRpb24gaW1wbGVtZW50cyBOb2RlIHtcbiAgcHVibGljIGF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcblxuICBjb25zdHJ1Y3Rvcih1bmVzY2FwZWRBdHRyczoge1trOiBzdHJpbmddOiBzdHJpbmd9KSB7XG4gICAgT2JqZWN0LmtleXModW5lc2NhcGVkQXR0cnMpLmZvckVhY2goKGs6IHN0cmluZykgPT4ge1xuICAgICAgdGhpcy5hdHRyc1trXSA9IGVzY2FwZVhtbCh1bmVzY2FwZWRBdHRyc1trXSk7XG4gICAgfSk7XG4gIH1cblxuICB2aXNpdCh2aXNpdG9yOiBJVmlzaXRvcik6IGFueSB7IHJldHVybiB2aXNpdG9yLnZpc2l0RGVjbGFyYXRpb24odGhpcyk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIERvY3R5cGUgaW1wbGVtZW50cyBOb2RlIHtcbiAgY29uc3RydWN0b3IocHVibGljIHJvb3RUYWc6IHN0cmluZywgcHVibGljIGR0ZDogc3RyaW5nKSB7fVxuXG4gIHZpc2l0KHZpc2l0b3I6IElWaXNpdG9yKTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXREb2N0eXBlKHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBUYWcgaW1wbGVtZW50cyBOb2RlIHtcbiAgcHVibGljIGF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30gPSB7fTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICAgIHB1YmxpYyBuYW1lOiBzdHJpbmcsIHVuZXNjYXBlZEF0dHJzOiB7W2s6IHN0cmluZ106IHN0cmluZ30gPSB7fSxcbiAgICAgIHB1YmxpYyBjaGlsZHJlbjogTm9kZVtdID0gW10pIHtcbiAgICBPYmplY3Qua2V5cyh1bmVzY2FwZWRBdHRycykuZm9yRWFjaCgoazogc3RyaW5nKSA9PiB7XG4gICAgICB0aGlzLmF0dHJzW2tdID0gZXNjYXBlWG1sKHVuZXNjYXBlZEF0dHJzW2tdKTtcbiAgICB9KTtcbiAgfVxuXG4gIHZpc2l0KHZpc2l0b3I6IElWaXNpdG9yKTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRUYWcodGhpcyk7IH1cbn1cblxuZXhwb3J0IGNsYXNzIFRleHQgaW1wbGVtZW50cyBOb2RlIHtcbiAgdmFsdWU6IHN0cmluZztcbiAgY29uc3RydWN0b3IodW5lc2NhcGVkVmFsdWU6IHN0cmluZykgeyB0aGlzLnZhbHVlID0gZXNjYXBlWG1sKHVuZXNjYXBlZFZhbHVlKTsgfVxuXG4gIHZpc2l0KHZpc2l0b3I6IElWaXNpdG9yKTogYW55IHsgcmV0dXJuIHZpc2l0b3IudmlzaXRUZXh0KHRoaXMpOyB9XG59XG5cbmV4cG9ydCBjbGFzcyBDUiBleHRlbmRzIFRleHQge1xuICBjb25zdHJ1Y3Rvcih3czogbnVtYmVyID0gMCkgeyBzdXBlcihgXFxuJHtuZXcgQXJyYXkod3MgKyAxKS5qb2luKCcgJyl9YCk7IH1cbn1cblxuY29uc3QgX0VTQ0FQRURfQ0hBUlM6IFtSZWdFeHAsIHN0cmluZ11bXSA9IFtcbiAgWy8mL2csICcmYW1wOyddLFxuICBbL1wiL2csICcmcXVvdDsnXSxcbiAgWy8nL2csICcmYXBvczsnXSxcbiAgWy88L2csICcmbHQ7J10sXG4gIFsvPi9nLCAnJmd0OyddLFxuXTtcblxuLy8gRXNjYXBlIGBfRVNDQVBFRF9DSEFSU2AgY2hhcmFjdGVycyBpbiB0aGUgZ2l2ZW4gdGV4dCB3aXRoIGVuY29kZWQgZW50aXRpZXNcbmV4cG9ydCBmdW5jdGlvbiBlc2NhcGVYbWwodGV4dDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIF9FU0NBUEVEX0NIQVJTLnJlZHVjZShcbiAgICAgICh0ZXh0OiBzdHJpbmcsIGVudHJ5OiBbUmVnRXhwLCBzdHJpbmddKSA9PiB0ZXh0LnJlcGxhY2UoZW50cnlbMF0sIGVudHJ5WzFdKSwgdGV4dCk7XG59XG4iXX0=