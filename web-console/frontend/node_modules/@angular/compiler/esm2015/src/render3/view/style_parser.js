/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * Parses string representation of a style and converts it into object literal.
 *
 * @param value string representation of style as used in the `style` attribute in HTML.
 *   Example: `color: red; height: auto`.
 * @returns An array of style property name and value pairs, e.g. `['color', 'red', 'height',
 * 'auto']`
 */
export function parse(value) {
    // we use a string array here instead of a string map
    // because a string-map is not guaranteed to retain the
    // order of the entries whereas a string array can be
    // construted in a [key, value, key, value] format.
    const styles = [];
    let i = 0;
    let parenDepth = 0;
    let quote = 0 /* QuoteNone */;
    let valueStart = 0;
    let propStart = 0;
    let currentProp = null;
    let valueHasQuotes = false;
    while (i < value.length) {
        const token = value.charCodeAt(i++);
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
                    const styleVal = value.substring(valueStart, i - 1).trim();
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
        const styleVal = value.substr(valueStart).trim();
        styles.push(currentProp, valueHasQuotes ? stripUnnecessaryQuotes(styleVal) : styleVal);
    }
    return styles;
}
export function stripUnnecessaryQuotes(value) {
    const qS = value.charCodeAt(0);
    const qE = value.charCodeAt(value.length - 1);
    if (qS == qE && (qS == 39 /* QuoteSingle */ || qS == 34 /* QuoteDouble */)) {
        const tempValue = value.substring(1, value.length - 1);
        // special case to avoid using a multi-quoted string that was just chomped
        // (e.g. `font-family: "Verdana", "sans-serif"`)
        if (tempValue.indexOf('\'') == -1 && tempValue.indexOf('"') == -1) {
            value = tempValue;
        }
    }
    return value;
}
export function hyphenate(value) {
    return value.replace(/[a-z][A-Z]/g, v => {
        return v.charAt(0) + '-' + v.charAt(1);
    }).toLowerCase();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGVfcGFyc2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3JlbmRlcjMvdmlldy9zdHlsZV9wYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBY0g7Ozs7Ozs7R0FPRztBQUNILE1BQU0sVUFBVSxLQUFLLENBQUMsS0FBYTtJQUNqQyxxREFBcUQ7SUFDckQsdURBQXVEO0lBQ3ZELHFEQUFxRDtJQUNyRCxtREFBbUQ7SUFDbkQsTUFBTSxNQUFNLEdBQWEsRUFBRSxDQUFDO0lBRTVCLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQztJQUNWLElBQUksVUFBVSxHQUFHLENBQUMsQ0FBQztJQUNuQixJQUFJLEtBQUssb0JBQXVCLENBQUM7SUFDakMsSUFBSSxVQUFVLEdBQUcsQ0FBQyxDQUFDO0lBQ25CLElBQUksU0FBUyxHQUFHLENBQUMsQ0FBQztJQUNsQixJQUFJLFdBQVcsR0FBZ0IsSUFBSSxDQUFDO0lBQ3BDLElBQUksY0FBYyxHQUFHLEtBQUssQ0FBQztJQUMzQixPQUFPLENBQUMsR0FBRyxLQUFLLENBQUMsTUFBTSxFQUFFO1FBQ3ZCLE1BQU0sS0FBSyxHQUFHLEtBQUssQ0FBQyxVQUFVLENBQUMsQ0FBQyxFQUFFLENBQVMsQ0FBQztRQUM1QyxRQUFRLEtBQUssRUFBRTtZQUNiO2dCQUNFLFVBQVUsRUFBRSxDQUFDO2dCQUNiLE1BQU07WUFDUjtnQkFDRSxVQUFVLEVBQUUsQ0FBQztnQkFDYixNQUFNO1lBQ1I7Z0JBQ0UsdURBQXVEO2dCQUN2RCxxQkFBcUI7Z0JBQ3JCLGNBQWMsR0FBRyxjQUFjLElBQUksVUFBVSxHQUFHLENBQUMsQ0FBQztnQkFDbEQsSUFBSSxLQUFLLHNCQUFtQixFQUFFO29CQUM1QixLQUFLLHVCQUFtQixDQUFDO2lCQUMxQjtxQkFBTSxJQUFJLEtBQUsseUJBQXFCLElBQUksS0FBSyxDQUFDLFVB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QWEsRUFBRSxDQUFDLENBQUMsRUFBRTtRQUMxQixPQUFPLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEdBQUcsR0FBRyxHQUFHLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDekMsQ0FBQyxDQUFDLENBQUMsV0FBVyxFQUFFLENBQUM7QUFDL0IsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuY29uc3QgZW51bSBDaGFyIHtcbiAgT3BlblBhcmVuID0gNDAsXG4gIENsb3NlUGFyZW4gPSA0MSxcbiAgQ29sb24gPSA1OCxcbiAgU2VtaWNvbG9uID0gNTksXG4gIEJhY2tTbGFzaCA9IDkyLFxuICBRdW90ZU5vbmUgPSAwLCAgLy8gaW5kaWNhdGluZyB3ZSBhcmUgbm90IGluc2lkZSBhIHF1b3RlXG4gIFF1b3RlRG91YmxlID0gMzQsXG4gIFF1b3RlU2luZ2xlID0gMzksXG59XG5cblxuLyoqXG4gKiBQYXJzZXMgc3RyaW5nIHJlcHJlc2VudGF0aW9uIG9mIGEgc3R5bGUgYW5kIGNvbnZlcnRzIGl0IGludG8gb2JqZWN0IGxpdGVyYWwuXG4gKlxuICogQHBhcmFtIHZhbHVlIHN0cmluZyByZXByZXNlbnRhdGlvbiBvZiBzdHlsZSBhcyB1c2VkIGluIHRoZSBgc3R5bGVgIGF0dHJpYnV0ZSBpbiBIVE1MLlxuICogICBFeGFtcGxlOiBgY29sb3I6IHJlZDsgaGVpZ2h0OiBhdXRvYC5cbiAqIEByZXR1cm5zIEFuIGFycmF5IG9mIHN0eWxlIHByb3BlcnR5IG5hbWUgYW5kIHZhbHVlIHBhaXJzLCBlLmcuIGBbJ2NvbG9yJywgJ3JlZCcsICdoZWlnaHQnLFxuICogJ2F1dG8nXWBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmdbXSB7XG4gIC8vIHdlIHVzZSBhIHN0cmluZyBhcnJheSBoZXJlIGluc3RlYWQgb2YgYSBzdHJpbmcgbWFwXG4gIC8vIGJlY2F1c2UgYSBzdHJpbmctbWFwIGlzIG5vdCBndWFyYW50ZWVkIHRvIHJldGFpbiB0aGVcbiAgLy8gb3JkZXIgb2YgdGhlIGVudHJpZXMgd2hlcmVhcyBhIHN0cmluZyBhcnJheSBjYW4gYmVcbiAgLy8gY29uc3RydXRlZCBpbiBhIFtrZXksIHZhbHVlLCBrZXksIHZhbHVlXSBmb3JtYXQuXG4gIGNvbnN0IHN0eWxlczogc3RyaW5nW10gPSBbXTtcblxuICBsZXQgaSA9IDA7XG4gIGxldCBwYXJlbkRlcHRoID0gMDtcbiAgbGV0IHF1b3RlOiBDaGFyID0gQ2hhci5RdW90ZU5vbmU7XG4gIGxldCB2YWx1ZVN0YXJ0ID0gMDtcbiAgbGV0IHByb3BTdGFydCA9IDA7XG4gIGxldCBjdXJyZW50UHJvcDogc3RyaW5nfG51bGwgPSBudWxsO1xuICBsZXQgdmFsdWVIYXNRdW90ZXMgPSBmYWxzZTtcbiAgd2hpbGUgKGkgPCB2YWx1ZS5sZW5ndGgpIHtcbiAgICBjb25zdCB0b2tlbiA9IHZhbHVlLmNoYXJDb2RlQXQoaSsrKSBhcyBDaGFyO1xuICAgIHN3aXRjaCAodG9rZW4pIHtcbiAgICAgIGNhc2UgQ2hhci5PcGVuUGFyZW46XG4gICAgICAgIHBhcmVuRGVwdGgrKztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIENoYXIuQ2xvc2VQYXJlbjpcbiAgICAgICAgcGFyZW5EZXB0aC0tO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgQ2hhci5RdW90ZVNpbmdsZTpcbiAgICAgICAgLy8gdmFsdWVTdGFydCBuZWVkcyB0byBiZSB0aGVyZSBzaW5jZSBwcm9wIHZhbHVlcyBkb24ndFxuICAgICAgICAvLyBoYXZlIHF1b3RlcyBpbiBDU1NcbiAgICAgICAgdmFsdWVIYXNRdW90ZXMgPSB2YWx1ZUhhc1F1b3RlcyB8fCB2YWx1ZVN0YXJ0ID4gMDtcbiAgICAgICAgaWYgKHF1b3RlID09PSBDaGFyLlF1b3RlTm9uZSkge1xuICAgICAgICAgIHF1b3RlID0gQ2hhci5RdW90ZVNpbmdsZTtcbiAgICAgICAgfSBlbHNlIGlmIChxdW90ZSA9PT0gQ2hhci5RdW90ZVNpbmdsZSAmJiB2YWx1ZS5jaGFyQ29kZUF0KGkgLSAxKSAhPT0gQ2hhci5CYWNrU2xhc2gpIHtcbiAgICAgICAgICBxdW90ZSA9IENoYXIuUXVvdGVOb25lO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBDaGFyLlF1b3RlRG91YmxlOlxuICAgICAgICAvLyBzYW1lIGxvZ2ljIGFzIGFib3ZlXG4gICAgICAgIHZhbHVlSGFzUXVvdGVzID0gdmFsdWVIYXNRdW90ZXMgfHwgdmFsdWVTdGFydCA+IDA7XG4gICAgICAgIGlmIChxdW90ZSA9PT0gQ2hhci5RdW90ZU5vbmUpIHtcbiAgICAgICAgICBxdW90ZSA9IENoYXIuUXVvdGVEb3VibGU7XG4gICAgICAgIH0gZWxzZSBpZiAocXVvdGUgPT09IENoYXIuUXVvdGVEb3VibGUgJiYgdmFsdWUuY2hhckNvZGVBdChpIC0gMSkgIT09IENoYXIuQmFja1NsYXNoKSB7XG4gICAgICAgICAgcXVvdGUgPSBDaGFyLlF1b3RlTm9uZTtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgQ2hhci5Db2xvbjpcbiAgICAgICAgaWYgKCFjdXJyZW50UHJvcCAmJiBwYXJlbkRlcHRoID09PSAwICYmIHF1b3RlID09PSBDaGFyLlF1b3RlTm9uZSkge1xuICAgICAgICAgIGN1cnJlbnRQcm9wID0gaHlwaGVuYXRlKHZhbHVlLnN1YnN0cmluZyhwcm9wU3RhcnQsIGkgLSAxKS50cmltKCkpO1xuICAgICAgICAgIHZhbHVlU3RhcnQgPSBpO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBDaGFyLlNlbWljb2xvbjpcbiAgICAgICAgaWYgKGN1cnJlbnRQcm9wICYmIHZhbHVlU3RhcnQgPiAwICYmIHBhcmVuRGVwdGggPT09IDAgJiYgcXVvdGUgPT09IENoYXIuUXVvdGVOb25lKSB7XG4gICAgICAgICAgY29uc3Qgc3R5bGVWYWwgPSB2YWx1ZS5zdWJzdHJpbmcodmFsdWVTdGFydCwgaSAtIDEpLnRyaW0oKTtcbiAgICAgICAgICBzdHlsZXMucHVzaChjdXJyZW50UHJvcCwgdmFsdWVIYXNRdW90ZXMgPyBzdHJpcFVubmVjZXNzYXJ5UXVvdGVzKHN0eWxlVmFsKSA6IHN0eWxlVmFsKTtcbiAgICAgICAgICBwcm9wU3RhcnQgPSBpO1xuICAgICAgICAgIHZhbHVlU3RhcnQgPSAwO1xuICAgICAgICAgIGN1cnJlbnRQcm9wID0gbnVsbDtcbiAgICAgICAgICB2YWx1ZUhhc1F1b3RlcyA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgIH1cbiAgfVxuXG4gIGlmIChjdXJyZW50UHJvcCAmJiB2YWx1ZVN0YXJ0KSB7XG4gICAgY29uc3Qgc3R5bGVWYWwgPSB2YWx1ZS5zdWJzdHIodmFsdWVTdGFydCkudHJpbSgpO1xuICAgIHN0eWxlcy5wdXNoKGN1cnJlbnRQcm9wLCB2YWx1ZUhhc1F1b3RlcyA/IHN0cmlwVW5uZWNlc3NhcnlRdW90ZXMoc3R5bGVWYWwpIDogc3R5bGVWYWwpO1xuICB9XG5cbiAgcmV0dXJuIHN0eWxlcztcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHN0cmlwVW5uZWNlc3NhcnlRdW90ZXModmFsdWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IHFTID0gdmFsdWUuY2hhckNvZGVBdCgwKTtcbiAgY29uc3QgcUUgPSB2YWx1ZS5jaGFyQ29kZUF0KHZhbHVlLmxlbmd0aCAtIDEpO1xuICBpZiAocVMgPT0gcUUgJiYgKHFTID09IENoYXIuUXVvdGVTaW5nbGUgfHwgcVMgPT0gQ2hhci5RdW90ZURvdWJsZSkpIHtcbiAgICBjb25zdCB0ZW1wVmFsdWUgPSB2YWx1ZS5zdWJzdHJpbmcoMSwgdmFsdWUubGVuZ3RoIC0gMSk7XG4gICAgLy8gc3BlY2lhbCBjYXNlIHRvIGF2b2lkIHVzaW5nIGEgbXVsdGktcXVvdGVkIHN0cmluZyB0aGF0IHdhcyBqdXN0IGNob21wZWRcbiAgICAvLyAoZS5nLiBgZm9udC1mYW1pbHk6IFwiVmVyZGFuYVwiLCBcInNhbnMtc2VyaWZcImApXG4gICAgaWYgKHRlbXBWYWx1ZS5pbmRleE9mKCdcXCcnKSA9PSAtMSAmJiB0ZW1wVmFsdWUuaW5kZXhPZignXCInKSA9PSAtMSkge1xuICAgICAgdmFsdWUgPSB0ZW1wVmFsdWU7XG4gICAgfVxuICB9XG4gIHJldHVybiB2YWx1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGh5cGhlbmF0ZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIHZhbHVlLnJlcGxhY2UoL1thLXpdW0EtWl0vZywgdiA9PiB7XG4gICAgICAgICAgICAgICAgcmV0dXJuIHYuY2hhckF0KDApICsgJy0nICsgdi5jaGFyQXQoMSk7XG4gICAgICAgICAgICAgIH0pLnRvTG93ZXJDYXNlKCk7XG59XG4iXX0=