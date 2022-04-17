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
        define("@angular/compiler/src/render3/view/i18n/serializer", ["require", "exports", "@angular/compiler/src/render3/view/i18n/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var util_1 = require("@angular/compiler/src/render3/view/i18n/util");
    /**
     * This visitor walks over i18n tree and generates its string representation, including ICUs and
     * placeholders in `{$placeholder}` (for plain messages) or `{PLACEHOLDER}` (inside ICUs) format.
     */
    var SerializerVisitor = /** @class */ (function () {
        function SerializerVisitor() {
            /**
             * Keeps track of ICU nesting level, allowing to detect that we are processing elements of an ICU.
             *
             * This is needed due to the fact that placeholders in ICUs and in other messages are represented
             * differently in Closure:
             * - {$placeholder} in non-ICU case
             * - {PLACEHOLDER} inside ICU
             */
            this.icuNestingLevel = 0;
        }
        SerializerVisitor.prototype.formatPh = function (value) {
            var isInsideIcu = this.icuNestingLevel > 0;
            var formatted = util_1.formatI18nPlaceholderName(value, /* useCamelCase */ !isInsideIcu);
            return isInsideIcu ? "{" + formatted + "}" : "{$" + formatted + "}";
        };
        SerializerVisitor.prototype.visitText = function (text, context) { return text.value; };
        SerializerVisitor.prototype.visitContainer = function (container, context) {
            var _this = this;
            return container.children.map(function (child) { return child.visit(_this); }).join('');
        };
        SerializerVisitor.prototype.visitIcu = function (icu, context) {
            var _this = this;
            this.icuNestingLevel++;
            var strCases = Object.keys(icu.cases).map(function (k) { return k + " {" + icu.cases[k].visit(_this) + "}"; });
            var result = "{" + icu.expressionPlaceholder + ", " + icu.type + ", " + strCases.join(' ') + "}";
            this.icuNestingLevel--;
            return result;
        };
        SerializerVisitor.prototype.visitTagPlaceholder = function (ph, context) {
            var _this = this;
            return ph.isVoid ?
                this.formatPh(ph.startName) :
                "" + this.formatPh(ph.startName) + ph.children.map(function (child) { return child.visit(_this); }).join('') + this.formatPh(ph.closeName);
        };
        SerializerVisitor.prototype.visitPlaceholder = function (ph, context) { return this.formatPh(ph.name); };
        SerializerVisitor.prototype.visitIcuPlaceholder = function (ph, context) {
            return this.formatPh(ph.name);
        };
        return SerializerVisitor;
    }());
    var serializerVisitor = new SerializerVisitor();
    function getSerializedI18nContent(message) {
        return message.nodes.map(function (node) { return node.visit(serializerVisitor, null); }).join('');
    }
    exports.getSerializedI18nContent = getSerializedI18nContent;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VyaWFsaXplci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3ZpZXcvaTE4bi9zZXJpYWxpemVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBSUgscUVBQWlEO0lBRWpEOzs7T0FHRztJQUNIO1FBQUE7WUFDRTs7Ozs7OztlQU9HO1lBQ0ssb0JBQWUsR0FBRyxDQUFDLENBQUM7UUFrQzlCLENBQUM7UUFoQ1Msb0NBQVEsR0FBaEIsVUFBaUIsS0FBYTtZQUM1QixJQUFNLFdBQVcsR0FBRyxJQUFJLENBQUMsZUFBZSxHQUFHLENBQUMsQ0FBQztZQUM3QyxJQUFNLFNBQVMsR0FBRyxnQ0FBeUIsQ0FBQyxLQUFLLEVBQUUsa0JBQWtCLENBQUMsQ0FBQyxXQUFXLENBQUMsQ0FBQztZQUNwRixPQUFPLFdBQVcsQ0FBQyxDQUFDLENBQUMsTUFBSSxTQUFTLE1BQUcsQ0FBQyxDQUFDLENBQUMsT0FBSyxTQUFTLE1BQUcsQ0FBQztRQUM1RCxDQUFDO1FBRUQscUNBQVMsR0FBVCxVQUFVLElBQWUsRUFBRSxPQUFZLElBQVMsT0FBTyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztRQUVwRSwwQ0FBYyxHQUFkLFVBQWUsU0FBeUIsRUFBRSxPQUFZO1lBQXRELGlCQUVDO1lBREMsT0FBTyxTQUFTLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxVQUFBLEtBQUssSUFBSSxPQUFBLEtBQUssQ0FBQyxLQUFLLENBQUMsS0FBSSxDQUFDLEVBQWpCLENBQWlCLENBQUMsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDckUsQ0FBQztRQUVELG9DQUFRLEdBQVIsVUFBUyxHQUFhLEVBQUUsT0FBWTtZQUFwQyxpQkFPQztZQU5DLElBQUksQ0FBQyxlQUFlLEVBQUUsQ0FBQztZQUN2QixJQUFNLFFBQVEsR0FDVixNQUFNLENBQUMsSUFBSSxDQUFDLEdBQUcsQ0FBQyxLQUFLLENBQUMsQ0FBQyxHQUFHLENBQUMsVUFBQyxDQUFTLElBQUssT0FBRyxDQUFDLFVBQUssR0FBRyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsS0FBSSxDQUFDLE1BQUcsRUFBcEMsQ0FBb0MsQ0FBQyxDQUFDO1lBQ3BGLElBQU0sTUFBTSxHQUFHLE1BQUksR0FBRyxDQUFDLHFCQUFxQixVQUFLLEdBQUcsQ0FBQyxJQUFJLFVBQUssUUFBUSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsTUFBRyxDQUFDO1lBQ3BGLElBQUksQ0FBQyxlQUFlLEVBQUUsQ0FBQztZQUN2QixPQUFPLE1BQU0sQ0FBQztRQUNoQixDQUFDO1FBRUQsK0NBQW1CLEdBQW5CLFVBQW9CLEVBQXVCLEVBQUUsT0FBWTtZQUF6RCxpQkFJQztZQUhDLE9BQU8sRUFBRSxDQUFDLE1BQU0sQ0FBQyxDQUFDO2dCQUNkLElBQUksQ0FBQyxRQUFRLENBQUMsRUFBRSxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUM7Z0JBQzdCLEtBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxFQUFFLENBQUMsU0FBUyxDQUFDLEdBQUcsRUFBRSxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxLQUFLLENBQUMsS0FBSyxDQUFDLEtBQUksQ0FBQyxFQUFqQixDQUFpQixDQUFDLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsRUFBRSxDQUFDLFNBQVMsQ0FBRyxDQUFDO1FBQzVILENBQUM7UUFFRCw0Q0FBZ0IsR0FBaEIsVUFBaUIsRUFBb0IsRUFBRSxPQUFZLElBQVMsT0FBTyxJQUFJLENBQUMsUUFBUSxDQUFDLEVBQUUsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFFNUYsK0NBQW1CLEdBQW5CLFVBQW9CLEVBQXVCLEVBQUUsT0FBYTtZQUN4RCxPQUFPLElBQUksQ0FBQyxRQUFRLENBQUMsRUFBRSxDQUFDLElBQUksQ0FBQyxDQUFDO1FBQ2hDLENBQUM7UUFDSCx3QkFBQztJQUFELENBQUMsQUEzQ0QsSUEyQ0M7SUFFRCxJQUFNLGlCQUFpQixHQUFHLElBQUksaUJBQWlCLEVBQUUsQ0FBQztJQUVsRCxTQUFnQix3QkFBd0IsQ0FBQyxPQUFxQjtRQUM1RCxPQUFPLE9BQU8sQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLFVBQUEsSUFBSSxJQUFJLE9BQUEsSUFBSSxDQUFDLEtBQUssQ0FBQyxpQkFBaUIsRUFBRSxJQUFJLENBQUMsRUFBbkMsQ0FBbUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsQ0FBQztJQUNqRixDQUFDO0lBRkQsNERBRUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAqIGFzIGkxOG4gZnJvbSAnLi4vLi4vLi4vaTE4bi9pMThuX2FzdCc7XG5cbmltcG9ydCB7Zm9ybWF0STE4blBsYWNlaG9sZGVyTmFtZX0gZnJvbSAnLi91dGlsJztcblxuLyoqXG4gKiBUaGlzIHZpc2l0b3Igd2Fsa3Mgb3ZlciBpMThuIHRyZWUgYW5kIGdlbmVyYXRlcyBpdHMgc3RyaW5nIHJlcHJlc2VudGF0aW9uLCBpbmNsdWRpbmcgSUNVcyBhbmRcbiAqIHBsYWNlaG9sZGVycyBpbiBgeyRwbGFjZWhvbGRlcn1gIChmb3IgcGxhaW4gbWVzc2FnZXMpIG9yIGB7UExBQ0VIT0xERVJ9YCAoaW5zaWRlIElDVXMpIGZvcm1hdC5cbiAqL1xuY2xhc3MgU2VyaWFsaXplclZpc2l0b3IgaW1wbGVtZW50cyBpMThuLlZpc2l0b3Ige1xuICAvKipcbiAgICogS2VlcHMgdHJhY2sgb2YgSUNVIG5lc3RpbmcgbGV2ZWwsIGFsbG93aW5nIHRvIGRldGVjdCB0aGF0IHdlIGFyZSBwcm9jZXNzaW5nIGVsZW1lbnRzIG9mIGFuIElDVS5cbiAgICpcbiAgICogVGhpcyBpcyBuZWVkZWQgZHVlIHRvIHRoZSBmYWN0IHRoYXQgcGxhY2Vob2xkZXJzIGluIElDVXMgYW5kIGluIG90aGVyIG1lc3NhZ2VzIGFyZSByZXByZXNlbnRlZFxuICAgKiBkaWZmZXJlbnRseSBpbiBDbG9zdXJlOlxuICAgKiAtIHskcGxhY2Vob2xkZXJ9IGluIG5vbi1JQ1UgY2FzZVxuICAgKiAtIHtQTEFDRUhPTERFUn0gaW5zaWRlIElDVVxuICAgKi9cbiAgcHJpdmF0ZSBpY3VOZXN0aW5nTGV2ZWwgPSAwO1xuXG4gIHByaXZhdGUgZm9ybWF0UGgodmFsdWU6IHN0cmluZyk6IHN0cmluZyB7XG4gICAgY29uc3QgaXNJbnNpZGVJY3UgPSB0aGlzLmljdU5lc3RpbmdMZXZlbCA+IDA7XG4gICAgY29uc3QgZm9ybWF0dGVkID0gZm9ybWF0STE4blBsYWNlaG9sZGVyTmFtZSh2YWx1ZSwgLyogdXNlQ2FtZWxDYXNlICovICFpc0luc2lkZUljdSk7XG4gICAgcmV0dXJuIGlzSW5zaWRlSWN1ID8gYHske2Zvcm1hdHRlZH19YCA6IGB7JCR7Zm9ybWF0dGVkfX1gO1xuICB9XG5cbiAgdmlzaXRUZXh0KHRleHQ6IGkxOG4uVGV4dCwgY29udGV4dDogYW55KTogYW55IHsgcmV0dXJuIHRleHQudmFsdWU7IH1cblxuICB2aXNpdENvbnRhaW5lcihjb250YWluZXI6IGkxOG4uQ29udGFpbmVyLCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIHJldHVybiBjb250YWluZXIuY2hpbGRyZW4ubWFwKGNoaWxkID0+IGNoaWxkLnZpc2l0KHRoaXMpKS5qb2luKCcnKTtcbiAgfVxuXG4gIHZpc2l0SWN1KGljdTogaTE4bi5JY3UsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgdGhpcy5pY3VOZXN0aW5nTGV2ZWwrKztcbiAgICBjb25zdCBzdHJDYXNlcyA9XG4gICAgICAgIE9iamVjdC5rZXlzKGljdS5jYXNlcykubWFwKChrOiBzdHJpbmcpID0+IGAke2t9IHske2ljdS5jYXNlc1trXS52aXNpdCh0aGlzKX19YCk7XG4gICAgY29uc3QgcmVzdWx0ID0gYHske2ljdS5leHByZXNzaW9uUGxhY2Vob2xkZXJ9LCAke2ljdS50eXBlfSwgJHtzdHJDYXNlcy5qb2luKCcgJyl9fWA7XG4gICAgdGhpcy5pY3VOZXN0aW5nTGV2ZWwtLTtcbiAgICByZXR1cm4gcmVzdWx0O1xuICB9XG5cbiAgdmlzaXRUYWdQbGFjZWhvbGRlcihwaDogaTE4bi5UYWdQbGFjZWhvbGRlciwgY29udGV4dDogYW55KTogYW55IHtcbiAgICByZXR1cm4gcGguaXNWb2lkID9cbiAgICAgICAgdGhpcy5mb3JtYXRQaChwaC5zdGFydE5hbWUpIDpcbiAgICAgICAgYCR7dGhpcy5mb3JtYXRQaChwaC5zdGFydE5hbWUpfSR7cGguY2hpbGRyZW4ubWFwKGNoaWxkID0+IGNoaWxkLnZpc2l0KHRoaXMpKS5qb2luKCcnKX0ke3RoaXMuZm9ybWF0UGgocGguY2xvc2VOYW1lKX1gO1xuICB9XG5cbiAgdmlzaXRQbGFjZWhvbGRlcihwaDogaTE4bi5QbGFjZWhvbGRlciwgY29udGV4dDogYW55KTogYW55IHsgcmV0dXJuIHRoaXMuZm9ybWF0UGgocGgubmFtZSk7IH1cblxuICB2aXNpdEljdVBsYWNlaG9sZGVyKHBoOiBpMThuLkljdVBsYWNlaG9sZGVyLCBjb250ZXh0PzogYW55KTogYW55IHtcbiAgICByZXR1cm4gdGhpcy5mb3JtYXRQaChwaC5uYW1lKTtcbiAgfVxufVxuXG5jb25zdCBzZXJpYWxpemVyVmlzaXRvciA9IG5ldyBTZXJpYWxpemVyVmlzaXRvcigpO1xuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2VyaWFsaXplZEkxOG5Db250ZW50KG1lc3NhZ2U6IGkxOG4uTWVzc2FnZSk6IHN0cmluZyB7XG4gIHJldHVybiBtZXNzYWdlLm5vZGVzLm1hcChub2RlID0+IG5vZGUudmlzaXQoc2VyaWFsaXplclZpc2l0b3IsIG51bGwpKS5qb2luKCcnKTtcbn0iXX0=