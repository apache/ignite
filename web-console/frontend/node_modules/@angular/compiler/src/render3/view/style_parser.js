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
        define("@angular/compiler/src/render3/view/style_parser", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    /**
     * Parses string representation of a style and converts it into object literal.
     *
     * @param value string representation of style as used in the `style` attribute in HTML.
     *   Example: `color: red; height: auto`.
     * @returns An array of style property name and value pairs, e.g. `['color', 'red', 'height',
     * 'auto']`
     */
    function parse(value) {
        // we use a string array here instead of a string map
        // because a string-map is not guaranteed to retain the
        // order of the entries whereas a string array can be
        // construted in a [key, value, key, value] format.
        var styles = [];
        var i = 0;
        var parenDepth = 0;
        var quote = 0 /* QuoteNone */;
        var valueStart = 0;
        var propStart = 0;
        var currentProp = null;
        var valueHasQuotes = false;
        while (i < value.length) {
            var token = value.charCodeAt(i++);
            switch (token) {
                case 40 /* OpenParen */:
                    parenDepth++;
                    break;
                case 41 /* CloseParen */:
                    parenDepth--;
                    break;
                case 39 /* QuoteSingle */:
                    // valueStart needs to be there since prop values don't
                    // have quotes in CSS
                    valueHasQuotes = valueHasQuotes || valueStart > 0;
                    if (quote === 0 /* QuoteNone */) {
                        quote = 39 /* QuoteSingle */;
                    }
                    else if (quote === 39 /* QuoteSingle */ && value.charCodeAt(i - 1) !== 92 /* BackSlash */) {
                        quote = 0 /* QuoteNone */;
                    }
                    break;
                case 34 /* QuoteDouble */:
                    // same logic as above
                    valueHasQuotes = valueHasQuotes || valueStart > 0;
                    if (quote === 0 /* QuoteNone */) {
                        quote = 34 /* QuoteDouble */;
                    }
                    else if (quote === 34 /* QuoteDouble */ && value.charCodeAt(i - 1) !== 92 /* BackSlash */) {
                        quote = 0 /* QuoteNone */;
                    }
                    break;
                case 58 /* Colon */:
                    if (!currentProp && parenDepth === 0 && quote === 0 /* QuoteNone */) {
                        currentProp = hyphenate(value.substring(propStart, i - 1).trim());
                        valueStart = i;
                    }
                    break;
                case 59 /* Semicolon */:
                    if (currentProp && valueStart > 0 && parenDepth === 0 && quote === 0 /* QuoteNone */) {
                        var styleVal = value.substring(valueStart, i - 1).trim();
                        styles.push(currentProp, valueHasQuotes ? stripUnnecessaryQuotes(styleVal) : styleVal);
                        propStart = i;
                        valueStart = 0;
                        currentProp = null;
                        valueHasQuotes = false;
                    }
                    break;
            }
        }
        if (currentProp && valueStart) {
            var styleVal = value.substr(valueStart).trim();
            styles.push(currentProp, valueHasQuotes ? stripUnnecessaryQuotes(styleVal) : styleVal);
        }
        return styles;
    }
    exports.parse = parse;
    function stripUnnecessaryQuotes(value) {
        var qS = value.charCodeAt(0);
        var qE = value.charCodeAt(value.length - 1);
        if (qS == qE && (qS == 39 /* QuoteSingle */ || qS == 34 /* QuoteDouble */)) {
            var tempValue = value.substring(1, value.length - 1);
            // special case to avoid using a multi-quoted string that was just chomped
            // (e.g. `font-family: "Verdana", "sans-serif"`)
            if (tempValue.indexOf('\'') == -1 && tempValue.indexOf('"') == -1) {
                value = tempValue;
            }
        }
        return value;
    }
    exports.stripUnnecessaryQuotes = stripUnnecessaryQuotes;
    function hyphenate(value) {
        return value.replace(/[a-z][A-Z]/g, function (v) {
            return v.charAt(0) + '-' + v.charAt(1);
        }).toLowerCase();
    }
    exports.hyphenate = hyphenate;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGVfcGFyc2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3JlbmRlcjMvdmlldy9zdHlsZV9wYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOzs7Ozs7Ozs7Ozs7SUFjSDs7Ozs7OztPQU9HO0lBQ0gsU0FBZ0IsS0FBSyxDQUFDLEtBQWE7UUFDakMscURBQXFEO1FBQ3JELHVEQUF1RDtRQUN2RCxxREFBcUQ7UUFDckQsbURBQW1EO1FBQ25ELElBQU0sTUFBTSxHQUFhLEVBQUUsQ0FBQztRQUU1QixJQUFJLENBQUMsR0FBRyxDQUFDLENBQUM7UUFDVixJQUFJLFVBQVUsR0FBRyxDQUFDLENBQUM7UUFDbkIsSUFBSSxLQUFLLG9CQUF1QixDQUFDO1FBQ2pDLElBQUksVUFBVSxHQUFHLENBQUMsQ0FBQztRQUNuQixJQUFJLFNBQVMsR0FBRyxDQUFDLENBQUM7UUFDbEIsSUFBSSxXQUFXLEdBQWdCLElBQUksQ0FBQztRQUNwQyxJQUFJLGNBQWMsR0FBRyxLQUFLLENBQUM7UUFDM0IsT0FBTyxDQUFDLEdBQUcsS0FBSyxDQUFDLE1BQU0sRUFBRTtZQUN2QixJQUFNLEtBQUssR0FBRyxLQUFLLENBQUMsVUFBVSxDQUFDLENBQUMsRUFBRSxDQUFTLENBQUM7WUFDNUMsUUFBUSxLQUFLLEVBQUU7Z0JBQ2I7b0JBQ0UsVUFBVSxFQUFFLENBQUM7b0JBQ2IsTUFBTTtnQkFDUjtvQkFDRSxVQUFVLEVBQUUsQ0FBQztvQkFDYixNQUFNO2dCQUNSO29CQUNFLHVEQUF1RDtvQkFDdkQscUJBQXFCO29CQUNyQixjQUFjLEdBQUcsY0FBYyxJQUFJLFVBQVUsR0FBRyxDQUFDLENBQUM7b0JBQ2xELElBQUksS0FBSyxzQkFBbUIsRUFBRTt3QkFDNUIsS0FBSyx1QkFBbUIsQ0FBQztxQkFDMUI7eUJBQU0sSUFBSSxLQUFLLHlCQUFxQixJQUFJLEtB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ckMsT0FBTyxLQUFLLENBQUMsT0FBTyxDQUFDLGFBQWEsRUFBRSxVQUFBLENBQUM7WUFDdkIsT0FBTyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxHQUFHLEdBQUcsR0FBRyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ3pDLENBQUMsQ0FBQyxDQUFDLFdBQVcsRUFBRSxDQUFDO0lBQy9CLENBQUM7SUFKRCw4QkFJQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuY29uc3QgZW51bSBDaGFyIHtcbiAgT3BlblBhcmVuID0gNDAsXG4gIENsb3NlUGFyZW4gPSA0MSxcbiAgQ29sb24gPSA1OCxcbiAgU2VtaWNvbG9uID0gNTksXG4gIEJhY2tTbGFzaCA9IDkyLFxuICBRdW90ZU5vbmUgPSAwLCAgLy8gaW5kaWNhdGluZyB3ZSBhcmUgbm90IGluc2lkZSBhIHF1b3RlXG4gIFF1b3RlRG91YmxlID0gMzQsXG4gIFF1b3RlU2luZ2xlID0gMzksXG59XG5cblxuLyoqXG4gKiBQYXJzZXMgc3RyaW5nIHJlcHJlc2VudGF0aW9uIG9mIGEgc3R5bGUgYW5kIGNvbnZlcnRzIGl0IGludG8gb2JqZWN0IGxpdGVyYWwuXG4gKlxuICogQHBhcmFtIHZhbHVlIHN0cmluZyByZXByZXNlbnRhdGlvbiBvZiBzdHlsZSBhcyB1c2VkIGluIHRoZSBgc3R5bGVgIGF0dHJpYnV0ZSBpbiBIVE1MLlxuICogICBFeGFtcGxlOiBgY29sb3I6IHJlZDsgaGVpZ2h0OiBhdXRvYC5cbiAqIEByZXR1cm5zIEFuIGFycmF5IG9mIHN0eWxlIHByb3BlcnR5IG5hbWUgYW5kIHZhbHVlIHBhaXJzLCBlLmcuIGBbJ2NvbG9yJywgJ3JlZCcsICdoZWlnaHQnLFxuICogJ2F1dG8nXWBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmdbXSB7XG4gIC8vIHdlIHVzZSBhIHN0cmluZyBhcnJheSBoZXJlIGluc3RlYWQgb2YgYSBzdHJpbmcgbWFwXG4gIC8vIGJlY2F1c2UgYSBzdHJpbmctbWFwIGlzIG5vdCBndWFyYW50ZWVkIHRvIHJldGFpbiB0aGVcbiAgLy8gb3JkZXIgb2YgdGhlIGVudHJpZXMgd2hlcmVhcyBhIHN0cmluZyBhcnJheSBjYW4gYmVcbiAgLy8gY29uc3RydXRlZCBpbiBhIFtrZXksIHZhbHVlLCBrZXksIHZhbHVlXSBmb3JtYXQuXG4gIGNvbnN0IHN0eWxlczogc3RyaW5nW10gPSBbXTtcblxuICBsZXQgaSA9IDA7XG4gIGxldCBwYXJlbkRlcHRoID0gMDtcbiAgbGV0IHF1b3RlOiBDaGFyID0gQ2hhci5RdW90ZU5vbmU7XG4gIGxldCB2YWx1ZVN0YXJ0ID0gMDtcbiAgbGV0IHByb3BTdGFydCA9IDA7XG4gIGxldCBjdXJyZW50UHJvcDogc3RyaW5nfG51bGwgPSBudWxsO1xuICBsZXQgdmFsdWVIYXNRdW90ZXMgPSBmYWxzZTtcbiAgd2hpbGUgKGkgPCB2YWx1ZS5sZW5ndGgpIHtcbiAgICBjb25zdCB0b2tlbiA9IHZhbHVlLmNoYXJDb2RlQXQoaSsrKSBhcyBDaGFyO1xuICAgIHN3aXRjaCAodG9rZW4pIHtcbiAgICAgIGNhc2UgQ2hhci5PcGVuUGFyZW46XG4gICAgICAgIHBhcmVuRGVwdGgrKztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIENoYXIuQ2xvc2VQYXJlbjpcbiAgICAgICAgcGFyZW5EZXB0aC0tO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgQ2hhci5RdW90ZVNpbmdsZTpcbiAgICAgICAgLy8gdmFsdWVTdGFydCBuZWVkcyB0byBiZSB0aGVyZSBzaW5jZSBwcm9wIHZhbHVlcyBkb24ndFxuICAgICAgICAvLyBoYXZlIHF1b3RlcyBpbiBDU1NcbiAgICAgICAgdmFsdWVIYXNRdW90ZXMgPSB2YWx1ZUhhc1F1b3RlcyB8fCB2YWx1ZVN0YXJ0ID4gMDtcbiAgICAgICAgaWYgKHF1b3RlID09PSBDaGFyLlF1b3RlTm9uZSkge1xuICAgICAgICAgIHF1b3RlID0gQ2hhci5RdW90ZVNpbmdsZTtcbiAgICAgICAgfSBlbHNlIGlmIChxdW90ZSA9PT0gQ2hhci5RdW90ZVNpbmdsZSAmJiB2YWx1ZS5jaGFyQ29kZUF0KGkgLSAxKSAhPT0gQ2hhci5CYWNrU2xhc2gpIHtcbiAgICAgICAgICBxdW90ZSA9IENoYXIuUXVvdGVOb25lO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBDaGFyLlF1b3RlRG91YmxlOlxuICAgICAgICAvLyBzYW1lIGxvZ2ljIGFzIGFib3ZlXG4gICAgICAgIHZhbHVlSGFzUXVvdGVzID0gdmFsdWVIYXNRdW90ZXMgfHwgdmFsdWVTdGFydCA+IDA7XG4gICAgICAgIGlmIChxdW90ZSA9PT0gQ2hhci5RdW90ZU5vbmUpIHtcbiAgICAgICAgICBxdW90ZSA9IENoYXIuUXVvdGVEb3VibGU7XG4gICAgICAgIH0gZWxzZSBpZiAocXVvdGUgPT09IENoYXIuUXVvdGVEb3VibGUgJiYgdmFsdWUuY2hhckNvZGVBdChpIC0gMSkgIT09IENoYXIuQmFja1NsYXNoKSB7XG4gICAgICAgICAgcXVvdGUgPSBDaGFyLlF1b3RlTm9uZTtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgQ2hhci5Db2xvbjpcbiAgICAgICAgaWYgKCFjdXJyZW50UHJvcCAmJiBwYXJlbkRlcHRoID09PSAwICYmIHF1b3RlID09PSBDaGFyLlF1b3RlTm9uZSkge1xuICAgICAgICAgIGN1cnJlbnRQcm9wID0gaHlwaGVuYXRlKHZhbHVlLnN1YnN0cmluZyhwcm9wU3RhcnQsIGkgLSAxKS50cmltKCkpO1xuICAgICAgICAgIHZhbHVlU3RhcnQgPSBpO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBDaGFyLlNlbWljb2xvbjpcbiAgICAgICAgaWYgKGN1cnJlbnRQcm9wICYmIHZhbHVlU3RhcnQgPiAwICYmIHBhcmVuRGVwdGggPT09IDAgJiYgcXVvdGUgPT09IENoYXIuUXVvdGVOb25lKSB7XG4gICAgICAgICAgY29uc3Qgc3R5bGVWYWwgPSB2YWx1ZS5zdWJzdHJpbmcodmFsdWVTdGFydCwgaSAtIDEpLnRyaW0oKTtcbiAgICAgICAgICBzdHlsZXMucHVzaChjdXJyZW50UHJvcCwgdmFsdWVIYXNRdW90ZXMgPyBzdHJpcFVubmVjZXNzYXJ5UXVvdGVzKHN0eWxlVmFsKSA6IHN0eWxlVmFsKTtcbiAgICAgICAgICBwcm9wU3RhcnQgPSBpO1xuICAgICAgICAgIHZhbHVlU3RhcnQgPSAwO1xuICAgICAgICAgIGN1cnJlbnRQcm9wID0gbnVsbDtcbiAgICAgICAgICB2YWx1ZUhhc1F1b3RlcyA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuXG4gIGlmIChjdXJyZW50UHJvcCAmJiB2YWx1ZVN0YXJ0KSB7XG4gICAgY29uc3Qgc3R5bGVWYWwgPSB2YWx1ZS5zdWJzdHIodmFsdWVTdGFydCkudHJpbSgpO1xuICAgIHN0eWxlcy5wdXNoKGN1cnJlbnRQcm9wLCB2YWx1ZUhhc1F1b3RlcyA/IHN0cmlwVW5uZWNlc3NhcnlRdW90ZXMoc3R5bGVWYWwpIDogc3R5bGVWYWwpO1xuICB9XG5cbiAgcmV0dXJuIHN0eWxlcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN0cmlwVW5uZWNlc3NhcnlRdW90ZXModmFsdWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IHFTID0gdmFsdWUuY2hhckNvZGVBdCgwKTtcbiAgY29uc3QgcUUgPSB2YWx1ZS5jaGFyQ29kZUF0KHZhbHVlLmxlbmd0aCAtIDEpO1xuICBpZiAocVMgPT0gcUUgJiYgKHFTID09IENoYXIuUXVvdGVTaW5nbGUgfHwgcVMgPT0gQ2hhci5RdW90ZURvdWJsZSkpIHtcbiAgICBjb25zdCB0ZW1wVmFsdWUgPSB2YWx1ZS5zdWJzdHJpbmcoMSwgdmFsdWUubGVuZ3RoIC0gMSk7XG4gICAgLy8gc3BlY2lhbCBjYXNlIHRvIGF2b2lkIHVzaW5nIGEgbXVsdGktcXVvdGVkIHN0cmluZyB0aGF0IHdhcyBqdXN0IGNob21wZWRcbiAgICAvLyAoZS5nLiBgZm9udC1mYW1pbHk6IFwiVmVyZGFuYVwiLCBcInNhbnMtc2VyaWZcImApXG4gICAgaWYgKHRlbXBWYWx1ZS5pbmRleE9mKCdcXCcnKSA9PSAtMSAmJiB0ZW1wVmFsdWUuaW5kZXhPZignXCInKSA9PSAtMSkge1xuICAgICAgdmFsdWUgPSB0ZW1wVmFsdWU7XG4gICAgfVxuICB9XG4gIHJldHVybiB2YWx1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGh5cGhlbmF0ZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIHZhbHVlLnJlcGxhY2UoL1thLXpdW0EtWl0vZywgdiA9PiB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIHYuY2hhckF0KDApICsgJy0nICsgdi5jaGFyQXQoMSk7XG4gICAgICAgICAgICAgIH0pLnRvTG93ZXJDYXNlKCk7XG59XG4iXX0=