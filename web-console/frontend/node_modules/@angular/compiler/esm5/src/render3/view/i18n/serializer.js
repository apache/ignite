/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { formatI18nPlaceholderName } from './util';
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
        var formatted = formatI18nPlaceholderName(value, /* useCamelCase */ !isInsideIcu);
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
export function getSerializedI18nContent(message) {
    return message.nodes.map(function (node) { return node.visit(serializerVisitor, null); }).join('');
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2VyaWFsaXplci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9yZW5kZXIzL3ZpZXcvaTE4bi9zZXJpYWxpemVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUlILE9BQU8sRUFBQyx5QkFBeUIsRUFBQyxNQUFNLFFBQVEsQ0FBQztBQUVqRDs7O0dBR0c7QUFDSDtJQUFBO1FBQ0U7Ozs7Ozs7V0FPRztRQUNLLG9CQUFlLEdBQUcsQ0FBQyxDQUFDO0lBa0M5QixDQUFDO0lBaENTLG9DQUFRLEdBQWhCLFVBQWlCLEtBQWE7UUFDNUIsSUFBTSxXQUFXLEdBQUcsSUFBSSxDQUFDLGVBQWUsR0FBRyxDQUFDLENBQUM7UUFDN0MsSUFBTSxTQUFTLEdBQUcseUJBQXlCLENBQUMsS0FBSyxFQUFFLGtCQUFrQixDQUFDLENBQUMsV0FBVyxDQUFDLENBQUM7UUFDcEYsT0FBTyxXQUFXLENBQUMsQ0FBQyxDQUFDLE1BQUksU0FBUyxNQUFHLENBQUMsQ0FBQyxDQUFDLE9BQUssU0FBUyxNQUFHLENBQUM7SUFDNUQsQ0FBQztJQUVELHFDQUFTLEdBQVQsVUFBVSxJQUFlLEVBQUUsT0FBWSxJQUFTLE9BQU8sSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUM7SUFFcEUsMENBQWMsR0FBZCxVQUFlLFNBQXlCLEVBQUUsT0FBWTtRQUF0RCxpQkFFQztRQURDLE9BQU8sU0FBUyxDQUFDLFFBQVEsQ0FBQyxHQUFHLENBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxLQUFLLENBQUMsS0FBSyxDQUFDLEtBQUksQ0FBQyxFQUFqQixDQUFpQixDQUFDLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0lBQ3JFLENBQUM7SUFFRCxvQ0FBUSxHQUFSLFVBQVMsR0FBYSxFQUFFLE9BQVk7UUFBcEMsaUJBT0M7UUFOQyxJQUFJLENBQUMsZUFBZSxFQUFFLENBQUM7UUFDdkIsSUFBTSxRQUFRLEdBQ1YsTUFBTSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsS0FBSyxDQUFDLENBQUMsR0FBRyxDQUFDLFVBQUMsQ0FBUyxJQUFLLE9BQUcsQ0FBQyxVQUFLLEdBQUcsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLEtBQUksQ0FBQyxNQUFHLEVBQXBDLENBQW9DLENBQUMsQ0FBQztRQUNwRixJQUFNLE1BQU0sR0FBRyxNQUFJLEdBQUcsQ0FBQyxxQkFBcUIsVUFBSyxHQUFHLENBQUMsSUFBSSxVQUFLLFFBQVEsQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLE1BQUcsQ0FBQztRQUNwRixJQUFJLENBQUMsZUFBZSxFQUFFLENBQUM7UUFDdkIsT0FBTyxNQUFNLENBQUM7SUFDaEIsQ0FBQztJQUVELCtDQUFtQixHQUFuQixVQUFvQixFQUF1QixFQUFFLE9BQVk7UUFBekQsaUJBSUM7UUFIQyxPQUFPLEVBQUUsQ0FBQyxNQUFNLENBQUMsQ0FBQztZQUNkLElBQUksQ0FBQyxRQUFRLENBQUMsRUFBRSxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUM7WUFDN0IsS0FBRyxJQUFJLENBQUMsUUFBUSxDQUFDLEVBQUUsQ0FBQyxTQUFTLENBQUMsR0FBRyxFQUFFLENBQUMsUUFBUSxDQUFDLEdBQUcsQ0FBQyxVQUFBLEtBQUssSUFBSSxPQUFBLEtBQUssQ0FBQyxLQUFLLENBQUMsS0FBSSxDQUFDLEVBQWpCLENBQWlCLENBQUMsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLEdBQUcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxFQUFFLENBQUMsU0FBUyxDQUFHLENBQUM7SUFDNUgsQ0FBQztJQUVELDRDQUFnQixHQUFoQixVQUFpQixFQUFvQixFQUFFLE9BQVksSUFBUyxPQUFPLElBQUksQ0FBQyxRQUFRLENBQUMsRUFBRSxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUU1RiwrQ0FBbUIsR0FBbkIsVUFBb0IsRUFBdUIsRUFBRSxPQUFhO1FBQ3hELE9BQU8sSUFBSSxDQUFDLFFBQVEsQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLENBQUM7SUFDaEMsQ0FBQztJQUNILHdCQUFDO0FBQUQsQ0FBQyxBQTNDRCxJQTJDQztBQUVELElBQU0saUJBQWlCLEdBQUcsSUFBSSxpQkFBaUIsRUFBRSxDQUFDO0FBRWxELE1BQU0sVUFBVSx3QkFBd0IsQ0FBQyxPQUFxQjtJQUM1RCxPQUFPLE9BQU8sQ0FBQyxLQUFLLENBQUMsR0FBRyxDQUFDLFVBQUEsSUFBSSxJQUFJLE9BQUEsSUFBSSxDQUFDLEtBQUssQ0FBQyxpQkFBaUIsRUFBRSxJQUFJLENBQUMsRUFBbkMsQ0FBbUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsQ0FBQztBQUNqRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQgKiBhcyBpMThuIGZyb20gJy4uLy4uLy4uL2kxOG4vaTE4bl9hc3QnO1xuXG5pbXBvcnQge2Zvcm1hdEkxOG5QbGFjZWhvbGRlck5hbWV9IGZyb20gJy4vdXRpbCc7XG5cbi8qKlxuICogVGhpcyB2aXNpdG9yIHdhbGtzIG92ZXIgaTE4biB0cmVlIGFuZCBnZW5lcmF0ZXMgaXRzIHN0cmluZyByZXByZXNlbnRhdGlvbiwgaW5jbHVkaW5nIElDVXMgYW5kXG4gKiBwbGFjZWhvbGRlcnMgaW4gYHskcGxhY2Vob2xkZXJ9YCAoZm9yIHBsYWluIG1lc3NhZ2VzKSBvciBge1BMQUNFSE9MREVSfWAgKGluc2lkZSBJQ1VzKSBmb3JtYXQuXG4gKi9cbmNsYXNzIFNlcmlhbGl6ZXJWaXNpdG9yIGltcGxlbWVudHMgaTE4bi5WaXNpdG9yIHtcbiAgLyoqXG4gICAqIEtlZXBzIHRyYWNrIG9mIElDVSBuZXN0aW5nIGxldmVsLCBhbGxvd2luZyB0byBkZXRlY3QgdGhhdCB3ZSBhcmUgcHJvY2Vzc2luZyBlbGVtZW50cyBvZiBhbiBJQ1UuXG4gICAqXG4gICAqIFRoaXMgaXMgbmVlZGVkIGR1ZSB0byB0aGUgZmFjdCB0aGF0IHBsYWNlaG9sZGVycyBpbiBJQ1VzIGFuZCBpbiBvdGhlciBtZXNzYWdlcyBhcmUgcmVwcmVzZW50ZWRcbiAgICogZGlmZmVyZW50bHkgaW4gQ2xvc3VyZTpcbiAgICogLSB7JHBsYWNlaG9sZGVyfSBpbiBub24tSUNVIGNhc2VcbiAgICogLSB7UExBQ0VIT0xERVJ9IGluc2lkZSBJQ1VcbiAgICovXG4gIHByaXZhdGUgaWN1TmVzdGluZ0xldmVsID0gMDtcblxuICBwcml2YXRlIGZvcm1hdFBoKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIGNvbnN0IGlzSW5zaWRlSWN1ID0gdGhpcy5pY3VOZXN0aW5nTGV2ZWwgPiAwO1xuICAgIGNvbnN0IGZvcm1hdHRlZCA9IGZvcm1hdEkxOG5QbGFjZWhvbGRlck5hbWUodmFsdWUsIC8qIHVzZUNhbWVsQ2FzZSAqLyAhaXNJbnNpZGVJY3UpO1xuICAgIHJldHVybiBpc0luc2lkZUljdSA/IGB7JHtmb3JtYXR0ZWR9fWAgOiBgeyQke2Zvcm1hdHRlZH19YDtcbiAgfVxuXG4gIHZpc2l0VGV4dCh0ZXh0OiBpMThuLlRleHQsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiB0ZXh0LnZhbHVlOyB9XG5cbiAgdmlzaXRDb250YWluZXIoY29udGFpbmVyOiBpMThuLkNvbnRhaW5lciwgY29udGV4dDogYW55KTogYW55IHtcbiAgICByZXR1cm4gY29udGFpbmVyLmNoaWxkcmVuLm1hcChjaGlsZCA9PiBjaGlsZC52aXNpdCh0aGlzKSkuam9pbignJyk7XG4gIH1cblxuICB2aXNpdEljdShpY3U6IGkxOG4uSWN1LCBjb250ZXh0OiBhbnkpOiBhbnkge1xuICAgIHRoaXMuaWN1TmVzdGluZ0xldmVsKys7XG4gICAgY29uc3Qgc3RyQ2FzZXMgPVxuICAgICAgICBPYmplY3Qua2V5cyhpY3UuY2FzZXMpLm1hcCgoazogc3RyaW5nKSA9PiBgJHtrfSB7JHtpY3UuY2FzZXNba10udmlzaXQodGhpcyl9fWApO1xuICAgIGNvbnN0IHJlc3VsdCA9IGB7JHtpY3UuZXhwcmVzc2lvblBsYWNlaG9sZGVyfSwgJHtpY3UudHlwZX0sICR7c3RyQ2FzZXMuam9pbignICcpfX1gO1xuICAgIHRoaXMuaWN1TmVzdGluZ0xldmVsLS07XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuXG4gIHZpc2l0VGFnUGxhY2Vob2xkZXIocGg6IGkxOG4uVGFnUGxhY2Vob2xkZXIsIGNvbnRleHQ6IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHBoLmlzVm9pZCA/XG4gICAgICAgIHRoaXMuZm9ybWF0UGgocGguc3RhcnROYW1lKSA6XG4gICAgICAgIGAke3RoaXMuZm9ybWF0UGgocGguc3RhcnROYW1lKX0ke3BoLmNoaWxkcmVuLm1hcChjaGlsZCA9PiBjaGlsZC52aXNpdCh0aGlzKSkuam9pbignJyl9JHt0aGlzLmZvcm1hdFBoKHBoLmNsb3NlTmFtZSl9YDtcbiAgfVxuXG4gIHZpc2l0UGxhY2Vob2xkZXIocGg6IGkxOG4uUGxhY2Vob2xkZXIsIGNvbnRleHQ6IGFueSk6IGFueSB7IHJldHVybiB0aGlzLmZvcm1hdFBoKHBoLm5hbWUpOyB9XG5cbiAgdmlzaXRJY3VQbGFjZWhvbGRlcihwaDogaTE4bi5JY3VQbGFjZWhvbGRlciwgY29udGV4dD86IGFueSk6IGFueSB7XG4gICAgcmV0dXJuIHRoaXMuZm9ybWF0UGgocGgubmFtZSk7XG4gIH1cbn1cblxuY29uc3Qgc2VyaWFsaXplclZpc2l0b3IgPSBuZXcgU2VyaWFsaXplclZpc2l0b3IoKTtcblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlcmlhbGl6ZWRJMThuQ29udGVudChtZXNzYWdlOiBpMThuLk1lc3NhZ2UpOiBzdHJpbmcge1xuICByZXR1cm4gbWVzc2FnZS5ub2Rlcy5tYXAobm9kZSA9PiBub2RlLnZpc2l0KHNlcmlhbGl6ZXJWaXNpdG9yLCBudWxsKSkuam9pbignJyk7XG59Il19