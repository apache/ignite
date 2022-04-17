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
export function stripUnnecessaryQuotes(value) {
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
export function hyphenate(value) {
    return value.replace(/[a-z][A-Z]/g, function (v) {
        return v.charAt(0) + '-' + v.charAt(1);
    }).toLowerCase();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3R5bGVfcGFyc2VyLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvc3JjL3JlbmRlcjMvdmlldy9zdHlsZV9wYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HO0FBY0g7Ozs7Ozs7R0FPRztBQUNILE1BQU0sVUFBVSxLQUFLLENBQUMsS0FBYTtJQUNqQyxxREFBcUQ7SUFDckQsdURBQXVEO0lBQ3ZELHFEQUFxRDtJQUNyRCxtREFBbUQ7SUFDbkQsSUFBTSxNQUFNLEdBQWEsRUFBRSxDQUFDO0lBRTVCLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQztJQUNWLElBQUksVUFBVSxHQUFHLENBQUMsQ0FBQztJQUNuQixJQUFJLEtBQUssb0JBQXVCLENBQUM7SUFDakMsSUFBSSxVQUFVLEdBQUcsQ0FBQyxDQUFDO0lBQ25CLElBQUksU0FBUyxHQUFHLENBQUMsQ0FBQztJQUNsQixJQUFJLFdBQVcsR0FBZ0IsSUFBSSxDQUFDO0lBQ3BDLElBQUksY0FBYyxHQUFHLEtBQUssQ0FBQztJQUMzQixPQUFPLENBQUMsR0FBRyxLQUFLLENBQUMsTUFBTSxFQUFFO1FBQ3ZCLElBQU0sS0FBSyxHQUFHLEtBQUssQ0FBQyxVQUFVLENBQUMsQ0FBQyxFQUFFLENBQVMsQ0FBQztRQUM1QyxRQUFRLEtBQUssRUFBRTtZQUNiO2dCQUNFLFVBQVUsRUFBRSxDQUFDO2dCQUNiLE1BQU07WUFDUjtnQkFDRSxVQUFVLEVBQUUsQ0FBQztnQkFDYixNQUFNO1lBQ1I7Z0JBQ0UsdURBQXVEO2dCQUN2RCxxQkFBcUI7Z0JBQ3JCLGNBQWMsR0FBRyxjQUFjLElBQUksVUFBVSxHQUFHLENBQUMsQ0FBQztnQkFDbEQsSUFBSSxLQUFLLHNCQUFtQixFQUFFO29CQUM1QixLQUFLLHVCQUFtQixDQUFDO2lCQUMxQjtxQkFBTSxJQUFJLEtBQUsseUJBQXFCLElBQUksS0FB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TyxDQUFDLGFBQWEsRUFBRSxVQUFBLENBQUM7UUFDdkIsT0FBTyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxHQUFHLEdBQUcsR0FBRyxDQUFDLENBQUMsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQ3pDLENBQUMsQ0FBQyxDQUFDLFdBQVcsRUFBRSxDQUFDO0FBQy9CLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmNvbnN0IGVudW0gQ2hhciB7XG4gIE9wZW5QYXJlbiA9IDQwLFxuICBDbG9zZVBhcmVuID0gNDEsXG4gIENvbG9uID0gNTgsXG4gIFNlbWljb2xvbiA9IDU5LFxuICBCYWNrU2xhc2ggPSA5MixcbiAgUXVvdGVOb25lID0gMCwgIC8vIGluZGljYXRpbmcgd2UgYXJlIG5vdCBpbnNpZGUgYSBxdW90ZVxuICBRdW90ZURvdWJsZSA9IDM0LFxuICBRdW90ZVNpbmdsZSA9IDM5LFxufVxuXG5cbi8qKlxuICogUGFyc2VzIHN0cmluZyByZXByZXNlbnRhdGlvbiBvZiBhIHN0eWxlIGFuZCBjb252ZXJ0cyBpdCBpbnRvIG9iamVjdCBsaXRlcmFsLlxuICpcbiAqIEBwYXJhbSB2YWx1ZSBzdHJpbmcgcmVwcmVzZW50YXRpb24gb2Ygc3R5bGUgYXMgdXNlZCBpbiB0aGUgYHN0eWxlYCBhdHRyaWJ1dGUgaW4gSFRNTC5cbiAqICAgRXhhbXBsZTogYGNvbG9yOiByZWQ7IGhlaWdodDogYXV0b2AuXG4gKiBAcmV0dXJucyBBbiBhcnJheSBvZiBzdHlsZSBwcm9wZXJ0eSBuYW1lIGFuZCB2YWx1ZSBwYWlycywgZS5nLiBgWydjb2xvcicsICdyZWQnLCAnaGVpZ2h0JyxcbiAqICdhdXRvJ11gXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZSh2YWx1ZTogc3RyaW5nKTogc3RyaW5nW10ge1xuICAvLyB3ZSB1c2UgYSBzdHJpbmcgYXJyYXkgaGVyZSBpbnN0ZWFkIG9mIGEgc3RyaW5nIG1hcFxuICAvLyBiZWNhdXNlIGEgc3RyaW5nLW1hcCBpcyBub3QgZ3VhcmFudGVlZCB0byByZXRhaW4gdGhlXG4gIC8vIG9yZGVyIG9mIHRoZSBlbnRyaWVzIHdoZXJlYXMgYSBzdHJpbmcgYXJyYXkgY2FuIGJlXG4gIC8vIGNvbnN0cnV0ZWQgaW4gYSBba2V5LCB2YWx1ZSwga2V5LCB2YWx1ZV0gZm9ybWF0LlxuICBjb25zdCBzdHlsZXM6IHN0cmluZ1tdID0gW107XG5cbiAgbGV0IGkgPSAwO1xuICBsZXQgcGFyZW5EZXB0aCA9IDA7XG4gIGxldCBxdW90ZTogQ2hhciA9IENoYXIuUXVvdGVOb25lO1xuICBsZXQgdmFsdWVTdGFydCA9IDA7XG4gIGxldCBwcm9wU3RhcnQgPSAwO1xuICBsZXQgY3VycmVudFByb3A6IHN0cmluZ3xudWxsID0gbnVsbDtcbiAgbGV0IHZhbHVlSGFzUXVvdGVzID0gZmFsc2U7XG4gIHdoaWxlIChpIDwgdmFsdWUubGVuZ3RoKSB7XG4gICAgY29uc3QgdG9rZW4gPSB2YWx1ZS5jaGFyQ29kZUF0KGkrKykgYXMgQ2hhcjtcbiAgICBzd2l0Y2ggKHRva2VuKSB7XG4gICAgICBjYXNlIENoYXIuT3BlblBhcmVuOlxuICAgICAgICBwYXJlbkRlcHRoKys7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSBDaGFyLkNsb3NlUGFyZW46XG4gICAgICAgIHBhcmVuRGVwdGgtLTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIENoYXIuUXVvdGVTaW5nbGU6XG4gICAgICAgIC8vIHZhbHVlU3RhcnQgbmVlZHMgdG8gYmUgdGhlcmUgc2luY2UgcHJvcCB2YWx1ZXMgZG9uJ3RcbiAgICAgICAgLy8gaGF2ZSBxdW90ZXMgaW4gQ1NTXG4gICAgICAgIHZhbHVlSGFzUXVvdGVzID0gdmFsdWVIYXNRdW90ZXMgfHwgdmFsdWVTdGFydCA+IDA7XG4gICAgICAgIGlmIChxdW90ZSA9PT0gQ2hhci5RdW90ZU5vbmUpIHtcbiAgICAgICAgICBxdW90ZSA9IENoYXIuUXVvdGVTaW5nbGU7XG4gICAgICAgIH0gZWxzZSBpZiAocXVvdGUgPT09IENoYXIuUXVvdGVTaW5nbGUgJiYgdmFsdWUuY2hhckNvZGVBdChpIC0gMSkgIT09IENoYXIuQmFja1NsYXNoKSB7XG4gICAgICAgICAgcXVvdGUgPSBDaGFyLlF1b3RlTm9uZTtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgQ2hhci5RdW90ZURvdWJsZTpcbiAgICAgICAgLy8gc2FtZSBsb2dpYyBhcyBhYm92ZVxuICAgICAgICB2YWx1ZUhhc1F1b3RlcyA9IHZhbHVlSGFzUXVvdGVzIHx8IHZhbHVlU3RhcnQgPiAwO1xuICAgICAgICBpZiAocXVvdGUgPT09IENoYXIuUXVvdGVOb25lKSB7XG4gICAgICAgICAgcXVvdGUgPSBDaGFyLlF1b3RlRG91YmxlO1xuICAgICAgICB9IGVsc2UgaWYgKHF1b3RlID09PSBDaGFyLlF1b3RlRG91YmxlICYmIHZhbHVlLmNoYXJDb2RlQXQoaSAtIDEpICE9PSBDaGFyLkJhY2tTbGFzaCkge1xuICAgICAgICAgIHF1b3RlID0gQ2hhci5RdW90ZU5vbmU7XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIENoYXIuQ29sb246XG4gICAgICAgIGlmICghY3VycmVudFByb3AgJiYgcGFyZW5EZXB0aCA9PT0gMCAmJiBxdW90ZSA9PT0gQ2hhci5RdW90ZU5vbmUpIHtcbiAgICAgICAgICBjdXJyZW50UHJvcCA9IGh5cGhlbmF0ZSh2YWx1ZS5zdWJzdHJpbmcocHJvcFN0YXJ0LCBpIC0gMSkudHJpbSgpKTtcbiAgICAgICAgICB2YWx1ZVN0YXJ0ID0gaTtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgQ2hhci5TZW1pY29sb246XG4gICAgICAgIGlmIChjdXJyZW50UHJvcCAmJiB2YWx1ZVN0YXJ0ID4gMCAmJiBwYXJlbkRlcHRoID09PSAwICYmIHF1b3RlID09PSBDaGFyLlF1b3RlTm9uZSkge1xuICAgICAgICAgIGNvbnN0IHN0eWxlVmFsID0gdmFsdWUuc3Vic3RyaW5nKHZhbHVlU3RhcnQsIGkgLSAxKS50cmltKCk7XG4gICAgICAgICAgc3R5bGVzLnB1c2goY3VycmVudFByb3AsIHZhbHVlSGFzUXVvdGVzID8gc3RyaXBVbm5lY2Vzc2FyeVF1b3RlcyhzdHlsZVZhbCkgOiBzdHlsZVZhbCk7XG4gICAgICAgICAgcHJvcFN0YXJ0ID0gaTtcbiAgICAgICAgICB2YWx1ZVN0YXJ0ID0gMDtcbiAgICAgICAgICBjdXJyZW50UHJvcCA9IG51bGw7XG4gICAgICAgICAgdmFsdWVIYXNRdW90ZXMgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICB9XG4gIH1cblxuICBpZiAoY3VycmVudFByb3AgJiYgdmFsdWVTdGFydCkge1xuICAgIGNvbnN0IHN0eWxlVmFsID0gdmFsdWUuc3Vic3RyKHZhbHVlU3RhcnQpLnRyaW0oKTtcbiAgICBzdHlsZXMucHVzaChjdXJyZW50UHJvcCwgdmFsdWVIYXNRdW90ZXMgPyBzdHJpcFVubmVjZXNzYXJ5UXVvdGVzKHN0eWxlVmFsKSA6IHN0eWxlVmFsKTtcbiAgfVxuXG4gIHJldHVybiBzdHlsZXM7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzdHJpcFVubmVjZXNzYXJ5UXVvdGVzKHZhbHVlOiBzdHJpbmcpOiBzdHJpbmcge1xuICBjb25zdCBxUyA9IHZhbHVlLmNoYXJDb2RlQXQoMCk7XG4gIGNvbnN0IHFFID0gdmFsdWUuY2hhckNvZGVBdCh2YWx1ZS5sZW5ndGggLSAxKTtcbiAgaWYgKHFTID09IHFFICYmIChxUyA9PSBDaGFyLlF1b3RlU2luZ2xlIHx8IHFTID09IENoYXIuUXVvdGVEb3VibGUpKSB7XG4gICAgY29uc3QgdGVtcFZhbHVlID0gdmFsdWUuc3Vic3RyaW5nKDEsIHZhbHVlLmxlbmd0aCAtIDEpO1xuICAgIC8vIHNwZWNpYWwgY2FzZSB0byBhdm9pZCB1c2luZyBhIG11bHRpLXF1b3RlZCBzdHJpbmcgdGhhdCB3YXMganVzdCBjaG9tcGVkXG4gICAgLy8gKGUuZy4gYGZvbnQtZmFtaWx5OiBcIlZlcmRhbmFcIiwgXCJzYW5zLXNlcmlmXCJgKVxuICAgIGlmICh0ZW1wVmFsdWUuaW5kZXhPZignXFwnJykgPT0gLTEgJiYgdGVtcFZhbHVlLmluZGV4T2YoJ1wiJykgPT0gLTEpIHtcbiAgICAgIHZhbHVlID0gdGVtcFZhbHVlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gdmFsdWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBoeXBoZW5hdGUodmFsdWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiB2YWx1ZS5yZXBsYWNlKC9bYS16XVtBLVpdL2csIHYgPT4ge1xuICAgICAgICAgICAgICAgIHJldHVybiB2LmNoYXJBdCgwKSArICctJyArIHYuY2hhckF0KDEpO1xuICAgICAgICAgICAgICB9KS50b0xvd2VyQ2FzZSgpO1xufVxuIl19