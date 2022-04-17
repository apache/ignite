/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Inject, Injectable, LOCALE_ID, Pipe } from '@angular/core';
import { formatCurrency, formatNumber, formatPercent } from '../i18n/format_number';
import { getCurrencySymbol } from '../i18n/locale_data_api';
import { invalidPipeArgumentError } from './invalid_pipe_argument_error';
/**
 * @ngModule CommonModule
 * @description
 *
 * Transforms a number into a string,
 * formatted according to locale rules that determine group sizing and
 * separator, decimal-point character, and other locale-specific
 * configurations.
 *
 * If no parameters are specified, the function rounds off to the nearest value using this
 * [rounding method](https://en.wikibooks.org/wiki/Arithmetic/Rounding).
 * The behavior differs from that of the JavaScript ```Math.round()``` function.
 * In the following case for example, the pipe rounds down where
 * ```Math.round()``` rounds up:
 *
 * ```html
 * -2.5 | number:'1.0-0'
 * > -3
 * Math.round(-2.5)
 * > -2
 * ```
 *
 * @see `formatNumber()`
 *
 * @usageNotes
 * The following code shows how the pipe transforms numbers
 * into text strings, according to various format specifications,
 * where the caller's default locale is `en-US`.
 *
 * ### Example
 *
 * <code-example path="common/pipes/ts/number_pipe.ts" region='NumberPipe'></code-example>
 *
 * @publicApi
 */
var DecimalPipe = /** @class */ (function () {
    function DecimalPipe(_locale) {
        this._locale = _locale;
    }
    DecimalPipe_1 = DecimalPipe;
    /**
     * @param value The number to be formatted.
     * @param digitsInfo Decimal representation options, specified by a string
     * in the following format:<br>
     * <code>{minIntegerDigits}.{minFractionDigits}-{maxFractionDigits}</code>.
     *   - `minIntegerDigits`: The minimum number of integer digits before the decimal point.
     * Default is `1`.
     *   - `minFractionDigits`: The minimum number of digits after the decimal point.
     * Default is `0`.
     *   - `maxFractionDigits`: The maximum number of digits after the decimal point.
     * Default is `3`.
     * @param locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     */
    DecimalPipe.prototype.transform = function (value, digitsInfo, locale) {
        if (isEmpty(value))
            return null;
        locale = locale || this._locale;
        try {
            var num = strToNumber(value);
            return formatNumber(num, locale, digitsInfo);
        }
        catch (error) {
            throw invalidPipeArgumentError(DecimalPipe_1, error.message);
        }
    };
    var DecimalPipe_1;
    DecimalPipe = DecimalPipe_1 = tslib_1.__decorate([
        Injectable(),
        Pipe({ name: 'number' }),
        tslib_1.__param(0, Inject(LOCALE_ID)),
        tslib_1.__metadata("design:paramtypes", [String])
    ], DecimalPipe);
    return DecimalPipe;
}());
export { DecimalPipe };
/**
 * @ngModule CommonModule
 * @description
 *
 * Transforms a number to a percentage
 * string, formatted according to locale rules that determine group sizing and
 * separator, decimal-point character, and other locale-specific
 * configurations.
 *
 * @see `formatPercent()`
 *
 * @usageNotes
 * The following code shows how the pipe transforms numbers
 * into text strings, according to various format specifications,
 * where the caller's default locale is `en-US`.
 *
 * <code-example path="common/pipes/ts/percent_pipe.ts" region='PercentPipe'></code-example>
 *
 * @publicApi
 */
var PercentPipe = /** @class */ (function () {
    function PercentPipe(_locale) {
        this._locale = _locale;
    }
    PercentPipe_1 = PercentPipe;
    /**
     *
     * @param value The number to be formatted as a percentage.
     * @param digitsInfo Decimal representation options, specified by a string
     * in the following format:<br>
     * <code>{minIntegerDigits}.{minFractionDigits}-{maxFractionDigits}</code>.
     *   - `minIntegerDigits`: The minimum number of integer digits before the decimal point.
     * Default is `1`.
     *   - `minFractionDigits`: The minimum number of digits after the decimal point.
     * Default is `0`.
     *   - `maxFractionDigits`: The maximum number of digits after the decimal point.
     * Default is `0`.
     * @param locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     */
    PercentPipe.prototype.transform = function (value, digitsInfo, locale) {
        if (isEmpty(value))
            return null;
        locale = locale || this._locale;
        try {
            var num = strToNumber(value);
            return formatPercent(num, locale, digitsInfo);
        }
        catch (error) {
            throw invalidPipeArgumentError(PercentPipe_1, error.message);
        }
    };
    var PercentPipe_1;
    PercentPipe = PercentPipe_1 = tslib_1.__decorate([
        Injectable(),
        Pipe({ name: 'percent' }),
        tslib_1.__param(0, Inject(LOCALE_ID)),
        tslib_1.__metadata("design:paramtypes", [String])
    ], PercentPipe);
    return PercentPipe;
}());
export { PercentPipe };
/**
 * @ngModule CommonModule
 * @description
 *
 * Transforms a number to a currency string, formatted according to locale rules
 * that determine group sizing and separator, decimal-point character,
 * and other locale-specific configurations.
 *
 * @see `getCurrencySymbol()`
 * @see `formatCurrency()`
 *
 * @usageNotes
 * The following code shows how the pipe transforms numbers
 * into text strings, according to various format specifications,
 * where the caller's default locale is `en-US`.
 *
 * <code-example path="common/pipes/ts/currency_pipe.ts" region='CurrencyPipe'></code-example>
 *
 * @publicApi
 */
var CurrencyPipe = /** @class */ (function () {
    function CurrencyPipe(_locale) {
        this._locale = _locale;
    }
    CurrencyPipe_1 = CurrencyPipe;
    /**
     *
     * @param value The number to be formatted as currency.
     * @param currencyCode The [ISO 4217](https://en.wikipedia.org/wiki/ISO_4217) currency code,
     * such as `USD` for the US dollar and `EUR` for the euro.
     * @param display The format for the currency indicator. One of the following:
     *   - `code`: Show the code (such as `USD`).
     *   - `symbol`(default): Show the symbol (such as `$`).
     *   - `symbol-narrow`: Use the narrow symbol for locales that have two symbols for their
     * currency.
     * For example, the Canadian dollar CAD has the symbol `CA$` and the symbol-narrow `$`. If the
     * locale has no narrow symbol, uses the standard symbol for the locale.
     *   - String: Use the given string value instead of a code or a symbol.
     * For example, an empty string will suppress the currency & symbol.
     *   - Boolean (marked deprecated in v5): `true` for symbol and false for `code`.
     *
     * @param digitsInfo Decimal representation options, specified by a string
     * in the following format:<br>
     * <code>{minIntegerDigits}.{minFractionDigits}-{maxFractionDigits}</code>.
     *   - `minIntegerDigits`: The minimum number of integer digits before the decimal point.
     * Default is `1`.
     *   - `minFractionDigits`: The minimum number of digits after the decimal point.
     * Default is `2`.
     *   - `maxFractionDigits`: The maximum number of digits after the decimal point.
     * Default is `2`.
     * If not provided, the number will be formatted with the proper amount of digits,
     * depending on what the [ISO 4217](https://en.wikipedia.org/wiki/ISO_4217) specifies.
     * For example, the Canadian dollar has 2 digits, whereas the Chilean peso has none.
     * @param locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     */
    CurrencyPipe.prototype.transform = function (value, currencyCode, display, digitsInfo, locale) {
        if (display === void 0) { display = 'symbol'; }
        if (isEmpty(value))
            return null;
        locale = locale || this._locale;
        if (typeof display === 'boolean') {
            if (console && console.warn) {
                console.warn("Warning: the currency pipe has been changed in Angular v5. The symbolDisplay option (third parameter) is now a string instead of a boolean. The accepted values are \"code\", \"symbol\" or \"symbol-narrow\".");
            }
            display = display ? 'symbol' : 'code';
        }
        var currency = currencyCode || 'USD';
        if (display !== 'code') {
            if (display === 'symbol' || display === 'symbol-narrow') {
                currency = getCurrencySymbol(currency, display === 'symbol' ? 'wide' : 'narrow', locale);
            }
            else {
                currency = display;
            }
        }
        try {
            var num = strToNumber(value);
            return formatCurrency(num, locale, currency, currencyCode, digitsInfo);
        }
        catch (error) {
            throw invalidPipeArgumentError(CurrencyPipe_1, error.message);
        }
    };
    var CurrencyPipe_1;
    CurrencyPipe = CurrencyPipe_1 = tslib_1.__decorate([
        Injectable(),
        Pipe({ name: 'currency' }),
        tslib_1.__param(0, Inject(LOCALE_ID)),
        tslib_1.__metadata("design:paramtypes", [String])
    ], CurrencyPipe);
    return CurrencyPipe;
}());
export { CurrencyPipe };
function isEmpty(value) {
    return value == null || value === '' || value !== value;
}
/**
 * Transforms a string into a number (if needed).
 */
function strToNumber(value) {
    // Convert strings to numbers
    if (typeof value === 'string' && !isNaN(Number(value) - parseFloat(value))) {
        return Number(value);
    }
    if (typeof value !== 'number') {
        throw new Error(value + " is not a number");
    }
    return value;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnVtYmVyX3BpcGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vc3JjL3BpcGVzL251bWJlcl9waXBlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsTUFBTSxFQUFFLFVBQVUsRUFBRSxTQUFTLEVBQUUsSUFBSSxFQUFnQixNQUFNLGVBQWUsQ0FBQztBQUNqRixPQUFPLEVBQUMsY0FBYyxFQUFFLFlBQVksRUFBRSxhQUFhLEVBQUMsTUFBTSx1QkFBdUIsQ0FBQztBQUNsRixPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSx5QkFBeUIsQ0FBQztBQUMxRCxPQUFPLEVBQUMsd0JBQXdCLEVBQUMsTUFBTSwrQkFBK0IsQ0FBQztBQUV2RTs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztHQWtDRztBQUdIO0lBQ0UscUJBQXVDLE9BQWU7UUFBZixZQUFPLEdBQVAsT0FBTyxDQUFRO0lBQUcsQ0FBQztvQkFEL0MsV0FBVztJQUd0Qjs7Ozs7Ozs7Ozs7Ozs7T0FjRztJQUNILCtCQUFTLEdBQVQsVUFBVSxLQUFVLEVBQUUsVUFBbUIsRUFBRSxNQUFlO1FBQ3hELElBQUksT0FBTyxDQUFDLEtBQUssQ0FBQztZQUFFLE9BQU8sSUFBSSxDQUFDO1FBRWhDLE1BQU0sR0FBRyxNQUFNLElBQUksSUFBSSxDQUFDLE9BQU8sQ0FBQztRQUVoQyxJQUFJO1lBQ0YsSUFBTSxHQUFHLEdBQUcsV0FBVyxDQUFDLEtBQUssQ0FBQyxDQUFDO1lBQy9CLE9BQU8sWUFBWSxDQUFDLEdBQUcsRUFBRSxNQUFNLEVBQUUsVUFBVSxDQUFDLENBQUM7U0FDOUM7UUFBQyxPQUFPLEtBQUssRUFBRTtZQUNkLE1BQU0sd0JBQXdCLENBQUMsYUFBVyxFQUFFLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUM1RDtJQUNILENBQUM7O0lBN0JV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QUMsY0FBWSxFQUFFLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUM3RDtJQUNILENBQUM7O0lBbEVVLFlBQVk7UUFGeEIsVUFBVSxFQUFFO1FBQ1osSUFBSSxDQUFDLEVBQUMsSUFBSSxFQUFFLFVBQVUsRUFBQyxDQUFDO1FBRVYsbUJBQUEsTUFBTSxDQUFDLFNBQVMsQ0FBQyxDQUFBOztPQURuQixZQUFZLENBbUV4QjtJQUFELG1CQUFDO0NBQUEsQUFuRUQsSUFtRUM7U0FuRVksWUFBWTtBQXFFekIsU0FBUyxPQUFPLENBQUMsS0FBVTtJQUN6QixPQUFPLEtBQUssSUFBSSxJQUFJLElBQUksS0FBSyxLQUFLLEVBQUUsSUFBSSxLQUFLLEtBQUssS0FBSyxDQUFDO0FBQzFELENBQUM7QUFFRDs7R0FFRztBQUNILFNBQVMsV0FBVyxDQUFDLEtBQXNCO0lBQ3pDLDZCQUE2QjtJQUM3QixJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsSUFBSSxDQUFDLEtBQUssQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLEdBQUcsVUFBVSxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUU7UUFDMUUsT0FBTyxNQUFNLENBQUMsS0FBSyxDQUFDLENBQUM7S0FDdEI7SUFDRCxJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsRUFBRTtRQUM3QixNQUFNLElBQUksS0FBSyxDQUFJLEtBQUsscUJBQWtCLENBQUMsQ0FBQztLQUM3QztJQUNELE9BQU8sS0FBSyxDQUFDO0FBQ2YsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuaW1wb3J0IHtJbmplY3QsIEluamVjdGFibGUsIExPQ0FMRV9JRCwgUGlwZSwgUGlwZVRyYW5zZm9ybX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge2Zvcm1hdEN1cnJlbmN5LCBmb3JtYXROdW1iZXIsIGZvcm1hdFBlcmNlbnR9IGZyb20gJy4uL2kxOG4vZm9ybWF0X251bWJlcic7XG5pbXBvcnQge2dldEN1cnJlbmN5U3ltYm9sfSBmcm9tICcuLi9pMThuL2xvY2FsZV9kYXRhX2FwaSc7XG5pbXBvcnQge2ludmFsaWRQaXBlQXJndW1lbnRFcnJvcn0gZnJvbSAnLi9pbnZhbGlkX3BpcGVfYXJndW1lbnRfZXJyb3InO1xuXG4vKipcbiAqIEBuZ01vZHVsZSBDb21tb25Nb2R1bGVcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIFRyYW5zZm9ybXMgYSBudW1iZXIgaW50byBhIHN0cmluZyxcbiAqIGZvcm1hdHRlZCBhY2NvcmRpbmcgdG8gbG9jYWxlIHJ1bGVzIHRoYXQgZGV0ZXJtaW5lIGdyb3VwIHNpemluZyBhbmRcbiAqIHNlcGFyYXRvciwgZGVjaW1hbC1wb2ludCBjaGFyYWN0ZXIsIGFuZCBvdGhlciBsb2NhbGUtc3BlY2lmaWNcbiAqIGNvbmZpZ3VyYXRpb25zLlxuICpcbiAqIElmIG5vIHBhcmFtZXRlcnMgYXJlIHNwZWNpZmllZCwgdGhlIGZ1bmN0aW9uIHJvdW5kcyBvZmYgdG8gdGhlIG5lYXJlc3QgdmFsdWUgdXNpbmcgdGhpc1xuICogW3JvdW5kaW5nIG1ldGhvZF0oaHR0cHM6Ly9lbi53aWtpYm9va3Mub3JnL3dpa2kvQXJpdGhtZXRpYy9Sb3VuZGluZykuXG4gKiBUaGUgYmVoYXZpb3IgZGlmZmVycyBmcm9tIHRoYXQgb2YgdGhlIEphdmFTY3JpcHQgYGBgTWF0aC5yb3VuZCgpYGBgIGZ1bmN0aW9uLlxuICogSW4gdGhlIGZvbGxvd2luZyBjYXNlIGZvciBleGFtcGxlLCB0aGUgcGlwZSByb3VuZHMgZG93biB3aGVyZVxuICogYGBgTWF0aC5yb3VuZCgpYGBgIHJvdW5kcyB1cDpcbiAqXG4gKiBgYGBodG1sXG4gKiAtMi41IHwgbnVtYmVyOicxLjAtMCdcbiAqID4gLTNcbiAqIE1hdGgucm91bmQoLTIuNSlcbiAqID4gLTJcbiAqIGBgYFxuICpcbiAqIEBzZWUgYGZvcm1hdE51bWJlcigpYFxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiBUaGUgZm9sbG93aW5nIGNvZGUgc2hvd3MgaG93IHRoZSBwaXBlIHRyYW5zZm9ybXMgbnVtYmVyc1xuICogaW50byB0ZXh0IHN0cmluZ3MsIGFjY29yZGluZyB0byB2YXJpb3VzIGZvcm1hdCBzcGVjaWZpY2F0aW9ucyxcbiAqIHdoZXJlIHRoZSBjYWxsZXIncyBkZWZhdWx0IGxvY2FsZSBpcyBgZW4tVVNgLlxuICpcbiAqICMjIyBFeGFtcGxlXG4gKlxuICogPGNvZGUtZXhhbXBsZSBwYXRoPVwiY29tbW9uL3BpcGVzL3RzL251bWJlcl9waXBlLnRzXCIgcmVnaW9uPSdOdW1iZXJQaXBlJz48L2NvZGUtZXhhbXBsZT5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbkBQaXBlKHtuYW1lOiAnbnVtYmVyJ30pXG5leHBvcnQgY2xhc3MgRGVjaW1hbFBpcGUgaW1wbGVtZW50cyBQaXBlVHJhbnNmb3JtIHtcbiAgY29uc3RydWN0b3IoQEluamVjdChMT0NBTEVfSUQpIHByaXZhdGUgX2xvY2FsZTogc3RyaW5nKSB7fVxuXG4gIC8qKlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIG51bWJlciB0byBiZSBmb3JtYXR0ZWQuXG4gICAqIEBwYXJhbSBkaWdpdHNJbmZvIERlY2ltYWwgcmVwcmVzZW50YXRpb24gb3B0aW9ucywgc3BlY2lmaWVkIGJ5IGEgc3RyaW5nXG4gICAqIGluIHRoZSBmb2xsb3dpbmcgZm9ybWF0Ojxicj5cbiAgICogPGNvZGU+e21pbkludGVnZXJEaWdpdHN9LnttaW5GcmFjdGlvbkRpZ2l0c30te21heEZyYWN0aW9uRGlnaXRzfTwvY29kZT4uXG4gICAqICAgLSBgbWluSW50ZWdlckRpZ2l0c2A6IFRoZSBtaW5pbXVtIG51bWJlciBvZiBpbnRlZ2VyIGRpZ2l0cyBiZWZvcmUgdGhlIGRlY2ltYWwgcG9pbnQuXG4gICAqIERlZmF1bHQgaXMgYDFgLlxuICAgKiAgIC0gYG1pbkZyYWN0aW9uRGlnaXRzYDogVGhlIG1pbmltdW0gbnVtYmVyIG9mIGRpZ2l0cyBhZnRlciB0aGUgZGVjaW1hbCBwb2ludC5cbiAgICogRGVmYXVsdCBpcyBgMGAuXG4gICAqICAgLSBgbWF4RnJhY3Rpb25EaWdpdHNgOiBUaGUgbWF4aW11bSBudW1iZXIgb2YgZGlnaXRzIGFmdGVyIHRoZSBkZWNpbWFsIHBvaW50LlxuICAgKiBEZWZhdWx0IGlzIGAzYC5cbiAgICogQHBhcmFtIGxvY2FsZSBBIGxvY2FsZSBjb2RlIGZvciB0aGUgbG9jYWxlIGZvcm1hdCBydWxlcyB0byB1c2UuXG4gICAqIFdoZW4gbm90IHN1cHBsaWVkLCB1c2VzIHRoZSB2YWx1ZSBvZiBgTE9DQUxFX0lEYCwgd2hpY2ggaXMgYGVuLVVTYCBieSBkZWZhdWx0LlxuICAgKiBTZWUgW1NldHRpbmcgeW91ciBhcHAgbG9jYWxlXShndWlkZS9pMThuI3NldHRpbmctdXAtdGhlLWxvY2FsZS1vZi15b3VyLWFwcCkuXG4gICAqL1xuICB0cmFuc2Zvcm0odmFsdWU6IGFueSwgZGlnaXRzSW5mbz86IHN0cmluZywgbG9jYWxlPzogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIGlmIChpc0VtcHR5KHZhbHVlKSkgcmV0dXJuIG51bGw7XG5cbiAgICBsb2NhbGUgPSBsb2NhbGUgfHwgdGhpcy5fbG9jYWxlO1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IG51bSA9IHN0clRvTnVtYmVyKHZhbHVlKTtcbiAgICAgIHJldHVybiBmb3JtYXROdW1iZXIobnVtLCBsb2NhbGUsIGRpZ2l0c0luZm8pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICB0aHJvdyBpbnZhbGlkUGlwZUFyZ3VtZW50RXJyb3IoRGVjaW1hbFBpcGUsIGVycm9yLm1lc3NhZ2UpO1xuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIEBuZ01vZHVsZSBDb21tb25Nb2R1bGVcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIFRyYW5zZm9ybXMgYSBudW1iZXIgdG8gYSBwZXJjZW50YWdlXG4gKiBzdHJpbmcsIGZvcm1hdHRlZCBhY2NvcmRpbmcgdG8gbG9jYWxlIHJ1bGVzIHRoYXQgZGV0ZXJtaW5lIGdyb3VwIHNpemluZyBhbmRcbiAqIHNlcGFyYXRvciwgZGVjaW1hbC1wb2ludCBjaGFyYWN0ZXIsIGFuZCBvdGhlciBsb2NhbGUtc3BlY2lmaWNcbiAqIGNvbmZpZ3VyYXRpb25zLlxuICpcbiAqIEBzZWUgYGZvcm1hdFBlcmNlbnQoKWBcbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogVGhlIGZvbGxvd2luZyBjb2RlIHNob3dzIGhvdyB0aGUgcGlwZSB0cmFuc2Zvcm1zIG51bWJlcnNcbiAqIGludG8gdGV4dCBzdHJpbmdzLCBhY2NvcmRpbmcgdG8gdmFyaW91cyBmb3JtYXQgc3BlY2lmaWNhdGlvbnMsXG4gKiB3aGVyZSB0aGUgY2FsbGVyJ3MgZGVmYXVsdCBsb2NhbGUgaXMgYGVuLVVTYC5cbiAqXG4gKiA8Y29kZS1leGFtcGxlIHBhdGg9XCJjb21tb24vcGlwZXMvdHMvcGVyY2VudF9waXBlLnRzXCIgcmVnaW9uPSdQZXJjZW50UGlwZSc+PC9jb2RlLWV4YW1wbGU+XG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5ASW5qZWN0YWJsZSgpXG5AUGlwZSh7bmFtZTogJ3BlcmNlbnQnfSlcbmV4cG9ydCBjbGFzcyBQZXJjZW50UGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICBjb25zdHJ1Y3RvcihASW5qZWN0KExPQ0FMRV9JRCkgcHJpdmF0ZSBfbG9jYWxlOiBzdHJpbmcpIHt9XG5cbiAgLyoqXG4gICAqXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgbnVtYmVyIHRvIGJlIGZvcm1hdHRlZCBhcyBhIHBlcmNlbnRhZ2UuXG4gICAqIEBwYXJhbSBkaWdpdHNJbmZvIERlY2ltYWwgcmVwcmVzZW50YXRpb24gb3B0aW9ucywgc3BlY2lmaWVkIGJ5IGEgc3RyaW5nXG4gICAqIGluIHRoZSBmb2xsb3dpbmcgZm9ybWF0Ojxicj5cbiAgICogPGNvZGU+e21pbkludGVnZXJEaWdpdHN9LnttaW5GcmFjdGlvbkRpZ2l0c30te21heEZyYWN0aW9uRGlnaXRzfTwvY29kZT4uXG4gICAqICAgLSBgbWluSW50ZWdlckRpZ2l0c2A6IFRoZSBtaW5pbXVtIG51bWJlciBvZiBpbnRlZ2VyIGRpZ2l0cyBiZWZvcmUgdGhlIGRlY2ltYWwgcG9pbnQuXG4gICAqIERlZmF1bHQgaXMgYDFgLlxuICAgKiAgIC0gYG1pbkZyYWN0aW9uRGlnaXRzYDogVGhlIG1pbmltdW0gbnVtYmVyIG9mIGRpZ2l0cyBhZnRlciB0aGUgZGVjaW1hbCBwb2ludC5cbiAgICogRGVmYXVsdCBpcyBgMGAuXG4gICAqICAgLSBgbWF4RnJhY3Rpb25EaWdpdHNgOiBUaGUgbWF4aW11bSBudW1iZXIgb2YgZGlnaXRzIGFmdGVyIHRoZSBkZWNpbWFsIHBvaW50LlxuICAgKiBEZWZhdWx0IGlzIGAwYC5cbiAgICogQHBhcmFtIGxvY2FsZSBBIGxvY2FsZSBjb2RlIGZvciB0aGUgbG9jYWxlIGZvcm1hdCBydWxlcyB0byB1c2UuXG4gICAqIFdoZW4gbm90IHN1cHBsaWVkLCB1c2VzIHRoZSB2YWx1ZSBvZiBgTE9DQUxFX0lEYCwgd2hpY2ggaXMgYGVuLVVTYCBieSBkZWZhdWx0LlxuICAgKiBTZWUgW1NldHRpbmcgeW91ciBhcHAgbG9jYWxlXShndWlkZS9pMThuI3NldHRpbmctdXAtdGhlLWxvY2FsZS1vZi15b3VyLWFwcCkuXG4gICAqL1xuICB0cmFuc2Zvcm0odmFsdWU6IGFueSwgZGlnaXRzSW5mbz86IHN0cmluZywgbG9jYWxlPzogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIGlmIChpc0VtcHR5KHZhbHVlKSkgcmV0dXJuIG51bGw7XG5cbiAgICBsb2NhbGUgPSBsb2NhbGUgfHwgdGhpcy5fbG9jYWxlO1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IG51bSA9IHN0clRvTnVtYmVyKHZhbHVlKTtcbiAgICAgIHJldHVybiBmb3JtYXRQZXJjZW50KG51bSwgbG9jYWxlLCBkaWdpdHNJbmZvKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgdGhyb3cgaW52YWxpZFBpcGVBcmd1bWVudEVycm9yKFBlcmNlbnRQaXBlLCBlcnJvci5tZXNzYWdlKTtcbiAgICB9XG4gIH1cbn1cblxuLyoqXG4gKiBAbmdNb2R1bGUgQ29tbW9uTW9kdWxlXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBUcmFuc2Zvcm1zIGEgbnVtYmVyIHRvIGEgY3VycmVuY3kgc3RyaW5nLCBmb3JtYXR0ZWQgYWNjb3JkaW5nIHRvIGxvY2FsZSBydWxlc1xuICogdGhhdCBkZXRlcm1pbmUgZ3JvdXAgc2l6aW5nIGFuZCBzZXBhcmF0b3IsIGRlY2ltYWwtcG9pbnQgY2hhcmFjdGVyLFxuICogYW5kIG90aGVyIGxvY2FsZS1zcGVjaWZpYyBjb25maWd1cmF0aW9ucy5cbiAqXG4gKiBAc2VlIGBnZXRDdXJyZW5jeVN5bWJvbCgpYFxuICogQHNlZSBgZm9ybWF0Q3VycmVuY3koKWBcbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICogVGhlIGZvbGxvd2luZyBjb2RlIHNob3dzIGhvdyB0aGUgcGlwZSB0cmFuc2Zvcm1zIG51bWJlcnNcbiAqIGludG8gdGV4dCBzdHJpbmdzLCBhY2NvcmRpbmcgdG8gdmFyaW91cyBmb3JtYXQgc3BlY2lmaWNhdGlvbnMsXG4gKiB3aGVyZSB0aGUgY2FsbGVyJ3MgZGVmYXVsdCBsb2NhbGUgaXMgYGVuLVVTYC5cbiAqXG4gKiA8Y29kZS1leGFtcGxlIHBhdGg9XCJjb21tb24vcGlwZXMvdHMvY3VycmVuY3lfcGlwZS50c1wiIHJlZ2lvbj0nQ3VycmVuY3lQaXBlJz48L2NvZGUtZXhhbXBsZT5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbkBQaXBlKHtuYW1lOiAnY3VycmVuY3knfSlcbmV4cG9ydCBjbGFzcyBDdXJyZW5jeVBpcGUgaW1wbGVtZW50cyBQaXBlVHJhbnNmb3JtIHtcbiAgY29uc3RydWN0b3IoQEluamVjdChMT0NBTEVfSUQpIHByaXZhdGUgX2xvY2FsZTogc3RyaW5nKSB7fVxuXG4gIC8qKlxuICAgKlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIG51bWJlciB0byBiZSBmb3JtYXR0ZWQgYXMgY3VycmVuY3kuXG4gICAqIEBwYXJhbSBjdXJyZW5jeUNvZGUgVGhlIFtJU08gNDIxN10oaHR0cHM6Ly9lbi53aWtpcGVkaWEub3JnL3dpa2kvSVNPXzQyMTcpIGN1cnJlbmN5IGNvZGUsXG4gICAqIHN1Y2ggYXMgYFVTRGAgZm9yIHRoZSBVUyBkb2xsYXIgYW5kIGBFVVJgIGZvciB0aGUgZXVyby5cbiAgICogQHBhcmFtIGRpc3BsYXkgVGhlIGZvcm1hdCBmb3IgdGhlIGN1cnJlbmN5IGluZGljYXRvci4gT25lIG9mIHRoZSBmb2xsb3dpbmc6XG4gICAqICAgLSBgY29kZWA6IFNob3cgdGhlIGNvZGUgKHN1Y2ggYXMgYFVTRGApLlxuICAgKiAgIC0gYHN5bWJvbGAoZGVmYXVsdCk6IFNob3cgdGhlIHN5bWJvbCAoc3VjaCBhcyBgJGApLlxuICAgKiAgIC0gYHN5bWJvbC1uYXJyb3dgOiBVc2UgdGhlIG5hcnJvdyBzeW1ib2wgZm9yIGxvY2FsZXMgdGhhdCBoYXZlIHR3byBzeW1ib2xzIGZvciB0aGVpclxuICAgKiBjdXJyZW5jeS5cbiAgICogRm9yIGV4YW1wbGUsIHRoZSBDYW5hZGlhbiBkb2xsYXIgQ0FEIGhhcyB0aGUgc3ltYm9sIGBDQSRgIGFuZCB0aGUgc3ltYm9sLW5hcnJvdyBgJGAuIElmIHRoZVxuICAgKiBsb2NhbGUgaGFzIG5vIG5hcnJvdyBzeW1ib2wsIHVzZXMgdGhlIHN0YW5kYXJkIHN5bWJvbCBmb3IgdGhlIGxvY2FsZS5cbiAgICogICAtIFN0cmluZzogVXNlIHRoZSBnaXZlbiBzdHJpbmcgdmFsdWUgaW5zdGVhZCBvZiBhIGNvZGUgb3IgYSBzeW1ib2wuXG4gICAqIEZvciBleGFtcGxlLCBhbiBlbXB0eSBzdHJpbmcgd2lsbCBzdXBwcmVzcyB0aGUgY3VycmVuY3kgJiBzeW1ib2wuXG4gICAqICAgLSBCb29sZWFuIChtYXJrZWQgZGVwcmVjYXRlZCBpbiB2NSk6IGB0cnVlYCBmb3Igc3ltYm9sIGFuZCBmYWxzZSBmb3IgYGNvZGVgLlxuICAgKlxuICAgKiBAcGFyYW0gZGlnaXRzSW5mbyBEZWNpbWFsIHJlcHJlc2VudGF0aW9uIG9wdGlvbnMsIHNwZWNpZmllZCBieSBhIHN0cmluZ1xuICAgKiBpbiB0aGUgZm9sbG93aW5nIGZvcm1hdDo8YnI+XG4gICAqIDxjb2RlPnttaW5JbnRlZ2VyRGlnaXRzfS57bWluRnJhY3Rpb25EaWdpdHN9LXttYXhGcmFjdGlvbkRpZ2l0c308L2NvZGU+LlxuICAgKiAgIC0gYG1pbkludGVnZXJEaWdpdHNgOiBUaGUgbWluaW11bSBudW1iZXIgb2YgaW50ZWdlciBkaWdpdHMgYmVmb3JlIHRoZSBkZWNpbWFsIHBvaW50LlxuICAgKiBEZWZhdWx0IGlzIGAxYC5cbiAgICogICAtIGBtaW5GcmFjdGlvbkRpZ2l0c2A6IFRoZSBtaW5pbXVtIG51bWJlciBvZiBkaWdpdHMgYWZ0ZXIgdGhlIGRlY2ltYWwgcG9pbnQuXG4gICAqIERlZmF1bHQgaXMgYDJgLlxuICAgKiAgIC0gYG1heEZyYWN0aW9uRGlnaXRzYDogVGhlIG1heGltdW0gbnVtYmVyIG9mIGRpZ2l0cyBhZnRlciB0aGUgZGVjaW1hbCBwb2ludC5cbiAgICogRGVmYXVsdCBpcyBgMmAuXG4gICAqIElmIG5vdCBwcm92aWRlZCwgdGhlIG51bWJlciB3aWxsIGJlIGZvcm1hdHRlZCB3aXRoIHRoZSBwcm9wZXIgYW1vdW50IG9mIGRpZ2l0cyxcbiAgICogZGVwZW5kaW5nIG9uIHdoYXQgdGhlIFtJU08gNDIxN10oaHR0cHM6Ly9lbi53aWtpcGVkaWEub3JnL3dpa2kvSVNPXzQyMTcpIHNwZWNpZmllcy5cbiAgICogRm9yIGV4YW1wbGUsIHRoZSBDYW5hZGlhbiBkb2xsYXIgaGFzIDIgZGlnaXRzLCB3aGVyZWFzIHRoZSBDaGlsZWFuIHBlc28gaGFzIG5vbmUuXG4gICAqIEBwYXJhbSBsb2NhbGUgQSBsb2NhbGUgY29kZSBmb3IgdGhlIGxvY2FsZSBmb3JtYXQgcnVsZXMgdG8gdXNlLlxuICAgKiBXaGVuIG5vdCBzdXBwbGllZCwgdXNlcyB0aGUgdmFsdWUgb2YgYExPQ0FMRV9JRGAsIHdoaWNoIGlzIGBlbi1VU2AgYnkgZGVmYXVsdC5cbiAgICogU2VlIFtTZXR0aW5nIHlvdXIgYXBwIGxvY2FsZV0oZ3VpZGUvaTE4biNzZXR0aW5nLXVwLXRoZS1sb2NhbGUtb2YteW91ci1hcHApLlxuICAgKi9cbiAgdHJhbnNmb3JtKFxuICAgICAgdmFsdWU6IGFueSwgY3VycmVuY3lDb2RlPzogc3RyaW5nLFxuICAgICAgZGlzcGxheTogJ2NvZGUnfCdzeW1ib2wnfCdzeW1ib2wtbmFycm93J3xzdHJpbmd8Ym9vbGVhbiA9ICdzeW1ib2wnLCBkaWdpdHNJbmZvPzogc3RyaW5nLFxuICAgICAgbG9jYWxlPzogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIGlmIChpc0VtcHR5KHZhbHVlKSkgcmV0dXJuIG51bGw7XG5cbiAgICBsb2NhbGUgPSBsb2NhbGUgfHwgdGhpcy5fbG9jYWxlO1xuXG4gICAgaWYgKHR5cGVvZiBkaXNwbGF5ID09PSAnYm9vbGVhbicpIHtcbiAgICAgIGlmICg8YW55PmNvbnNvbGUgJiYgPGFueT5jb25zb2xlLndhcm4pIHtcbiAgICAgICAgY29uc29sZS53YXJuKFxuICAgICAgICAgICAgYFdhcm5pbmc6IHRoZSBjdXJyZW5jeSBwaXBlIGhhcyBiZWVuIGNoYW5nZWQgaW4gQW5ndWxhciB2NS4gVGhlIHN5bWJvbERpc3BsYXkgb3B0aW9uICh0aGlyZCBwYXJhbWV0ZXIpIGlzIG5vdyBhIHN0cmluZyBpbnN0ZWFkIG9mIGEgYm9vbGVhbi4gVGhlIGFjY2VwdGVkIHZhbHVlcyBhcmUgXCJjb2RlXCIsIFwic3ltYm9sXCIgb3IgXCJzeW1ib2wtbmFycm93XCIuYCk7XG4gICAgICB9XG4gICAgICBkaXNwbGF5ID0gZGlzcGxheSA/ICdzeW1ib2wnIDogJ2NvZGUnO1xuICAgIH1cblxuICAgIGxldCBjdXJyZW5jeTogc3RyaW5nID0gY3VycmVuY3lDb2RlIHx8ICdVU0QnO1xuICAgIGlmIChkaXNwbGF5ICE9PSAnY29kZScpIHtcbiAgICAgIGlmIChkaXNwbGF5ID09PSAnc3ltYm9sJyB8fCBkaXNwbGF5ID09PSAnc3ltYm9sLW5hcnJvdycpIHtcbiAgICAgICAgY3VycmVuY3kgPSBnZXRDdXJyZW5jeVN5bWJvbChjdXJyZW5jeSwgZGlzcGxheSA9PT0gJ3N5bWJvbCcgPyAnd2lkZScgOiAnbmFycm93JywgbG9jYWxlKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGN1cnJlbmN5ID0gZGlzcGxheTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgbnVtID0gc3RyVG9OdW1iZXIodmFsdWUpO1xuICAgICAgcmV0dXJuIGZvcm1hdEN1cnJlbmN5KG51bSwgbG9jYWxlLCBjdXJyZW5jeSwgY3VycmVuY3lDb2RlLCBkaWdpdHNJbmZvKTtcbiAgICB9IGNhdGNoIChlcnJvcikge1xuICAgICAgdGhyb3cgaW52YWxpZFBpcGVBcmd1bWVudEVycm9yKEN1cnJlbmN5UGlwZSwgZXJyb3IubWVzc2FnZSk7XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIGlzRW1wdHkodmFsdWU6IGFueSk6IGJvb2xlYW4ge1xuICByZXR1cm4gdmFsdWUgPT0gbnVsbCB8fCB2YWx1ZSA9PT0gJycgfHwgdmFsdWUgIT09IHZhbHVlO1xufVxuXG4vKipcbiAqIFRyYW5zZm9ybXMgYSBzdHJpbmcgaW50byBhIG51bWJlciAoaWYgbmVlZGVkKS5cbiAqL1xuZnVuY3Rpb24gc3RyVG9OdW1iZXIodmFsdWU6IG51bWJlciB8IHN0cmluZyk6IG51bWJlciB7XG4gIC8vIENvbnZlcnQgc3RyaW5ncyB0byBudW1iZXJzXG4gIGlmICh0eXBlb2YgdmFsdWUgPT09ICdzdHJpbmcnICYmICFpc05hTihOdW1iZXIodmFsdWUpIC0gcGFyc2VGbG9hdCh2YWx1ZSkpKSB7XG4gICAgcmV0dXJuIE51bWJlcih2YWx1ZSk7XG4gIH1cbiAgaWYgKHR5cGVvZiB2YWx1ZSAhPT0gJ251bWJlcicpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoYCR7dmFsdWV9IGlzIG5vdCBhIG51bWJlcmApO1xuICB9XG4gIHJldHVybiB2YWx1ZTtcbn1cbiJdfQ==