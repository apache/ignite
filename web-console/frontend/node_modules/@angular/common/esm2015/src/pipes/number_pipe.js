/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { Inject, Injectable, LOCALE_ID, Pipe } from '@angular/core';
import { formatCurrency, formatNumber, formatPercent } from '../i18n/format_number';
import { getCurrencySymbol } from '../i18n/locale_data_api';
import { invalidPipeArgumentError } from './invalid_pipe_argument_error';
/**
 * \@ngModule CommonModule
 * \@description
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
 * \@usageNotes
 * The following code shows how the pipe transforms numbers
 * into text strings, according to various format specifications,
 * where the caller's default locale is `en-US`.
 *
 * ### Example
 *
 * <code-example path="common/pipes/ts/number_pipe.ts" region='NumberPipe'></code-example>
 *
 * \@publicApi
 */
export class DecimalPipe {
    /**
     * @param {?} _locale
     */
    constructor(_locale) {
        this._locale = _locale;
    }
    /**
     * @param {?} value The number to be formatted.
     * @param {?=} digitsInfo Decimal representation options, specified by a string
     * in the following format:<br>
     * <code>{minIntegerDigits}.{minFractionDigits}-{maxFractionDigits}</code>.
     *   - `minIntegerDigits`: The minimum number of integer digits before the decimal point.
     * Default is `1`.
     *   - `minFractionDigits`: The minimum number of digits after the decimal point.
     * Default is `0`.
     *   - `maxFractionDigits`: The maximum number of digits after the decimal point.
     * Default is `3`.
     * @param {?=} locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     * @return {?}
     */
    transform(value, digitsInfo, locale) {
        if (isEmpty(value))
            return null;
        locale = locale || this._locale;
        try {
            /** @type {?} */
            const num = strToNumber(value);
            return formatNumber(num, locale, digitsInfo);
        }
        catch (error) {
            throw invalidPipeArgumentError(DecimalPipe, error.message);
        }
    }
}
DecimalPipe.decorators = [
    { type: Injectable },
    { type: Pipe, args: [{ name: 'number' },] }
];
/** @nocollapse */
DecimalPipe.ctorParameters = () => [
    { type: String, decorators: [{ type: Inject, args: [LOCALE_ID,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DecimalPipe.prototype._locale;
}
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Transforms a number to a percentage
 * string, formatted according to locale rules that determine group sizing and
 * separator, decimal-point character, and other locale-specific
 * configurations.
 *
 * @see `formatPercent()`
 *
 * \@usageNotes
 * The following code shows how the pipe transforms numbers
 * into text strings, according to various format specifications,
 * where the caller's default locale is `en-US`.
 *
 * <code-example path="common/pipes/ts/percent_pipe.ts" region='PercentPipe'></code-example>
 *
 * \@publicApi
 */
export class PercentPipe {
    /**
     * @param {?} _locale
     */
    constructor(_locale) {
        this._locale = _locale;
    }
    /**
     *
     * @param {?} value The number to be formatted as a percentage.
     * @param {?=} digitsInfo Decimal representation options, specified by a string
     * in the following format:<br>
     * <code>{minIntegerDigits}.{minFractionDigits}-{maxFractionDigits}</code>.
     *   - `minIntegerDigits`: The minimum number of integer digits before the decimal point.
     * Default is `1`.
     *   - `minFractionDigits`: The minimum number of digits after the decimal point.
     * Default is `0`.
     *   - `maxFractionDigits`: The maximum number of digits after the decimal point.
     * Default is `0`.
     * @param {?=} locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     * @return {?}
     */
    transform(value, digitsInfo, locale) {
        if (isEmpty(value))
            return null;
        locale = locale || this._locale;
        try {
            /** @type {?} */
            const num = strToNumber(value);
            return formatPercent(num, locale, digitsInfo);
        }
        catch (error) {
            throw invalidPipeArgumentError(PercentPipe, error.message);
        }
    }
}
PercentPipe.decorators = [
    { type: Injectable },
    { type: Pipe, args: [{ name: 'percent' },] }
];
/** @nocollapse */
PercentPipe.ctorParameters = () => [
    { type: String, decorators: [{ type: Inject, args: [LOCALE_ID,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    PercentPipe.prototype._locale;
}
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Transforms a number to a currency string, formatted according to locale rules
 * that determine group sizing and separator, decimal-point character,
 * and other locale-specific configurations.
 *
 * @see `getCurrencySymbol()`
 * @see `formatCurrency()`
 *
 * \@usageNotes
 * The following code shows how the pipe transforms numbers
 * into text strings, according to various format specifications,
 * where the caller's default locale is `en-US`.
 *
 * <code-example path="common/pipes/ts/currency_pipe.ts" region='CurrencyPipe'></code-example>
 *
 * \@publicApi
 */
export class CurrencyPipe {
    /**
     * @param {?} _locale
     */
    constructor(_locale) {
        this._locale = _locale;
    }
    /**
     *
     * @param {?} value The number to be formatted as currency.
     * @param {?=} currencyCode The [ISO 4217](https://en.wikipedia.org/wiki/ISO_4217) currency code,
     * such as `USD` for the US dollar and `EUR` for the euro.
     * @param {?=} display The format for the currency indicator. One of the following:
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
     * @param {?=} digitsInfo Decimal representation options, specified by a string
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
     * @param {?=} locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     * @return {?}
     */
    transform(value, currencyCode, display = 'symbol', digitsInfo, locale) {
        if (isEmpty(value))
            return null;
        locale = locale || this._locale;
        if (typeof display === 'boolean') {
            if ((/** @type {?} */ (console)) && (/** @type {?} */ (console.warn))) {
                console.warn(`Warning: the currency pipe has been changed in Angular v5. The symbolDisplay option (third parameter) is now a string instead of a boolean. The accepted values are "code", "symbol" or "symbol-narrow".`);
            }
            display = display ? 'symbol' : 'code';
        }
        /** @type {?} */
        let currency = currencyCode || 'USD';
        if (display !== 'code') {
            if (display === 'symbol' || display === 'symbol-narrow') {
                currency = getCurrencySymbol(currency, display === 'symbol' ? 'wide' : 'narrow', locale);
            }
            else {
                currency = display;
            }
        }
        try {
            /** @type {?} */
            const num = strToNumber(value);
            return formatCurrency(num, locale, currency, currencyCode, digitsInfo);
        }
        catch (error) {
            throw invalidPipeArgumentError(CurrencyPipe, error.message);
        }
    }
}
CurrencyPipe.decorators = [
    { type: Injectable },
    { type: Pipe, args: [{ name: 'currency' },] }
];
/** @nocollapse */
CurrencyPipe.ctorParameters = () => [
    { type: String, decorators: [{ type: Inject, args: [LOCALE_ID,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    CurrencyPipe.prototype._locale;
}
/**
 * @param {?} value
 * @return {?}
 */
function isEmpty(value) {
    return value == null || value === '' || value !== value;
}
/**
 * Transforms a string into a number (if needed).
 * @param {?} value
 * @return {?}
 */
function strToNumber(value) {
    // Convert strings to numbers
    if (typeof value === 'string' && !isNaN(Number(value) - parseFloat(value))) {
        return Number(value);
    }
    if (typeof value !== 'number') {
        throw new Error(`${value} is not a number`);
    }
    return value;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnVtYmVyX3BpcGUuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vc3JjL3BpcGVzL251bWJlcl9waXBlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLE1BQU0sRUFBRSxVQUFVLEVBQUUsU0FBUyxFQUFFLElBQUksRUFBZ0IsTUFBTSxlQUFlLENBQUM7QUFDakYsT0FBTyxFQUFDLGNBQWMsRUFBRSxZQUFZLEVBQUUsYUFBYSxFQUFDLE1BQU0sdUJBQXVCLENBQUM7QUFDbEYsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0seUJBQXlCLENBQUM7QUFDMUQsT0FBTyxFQUFDLHdCQUF3QixFQUFDLE1BQU0sK0JBQStCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQXVDdkUsTUFBTSxPQUFPLFdBQVc7Ozs7SUFDdEIsWUFBdUMsT0FBZTtRQUFmLFlBQU8sR0FBUCxPQUFPLENBQVE7SUFBRyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7OztJQWlCMUQsU0FBUyxDQUFDLEtBQVUsRUFBRSxVQUFtQixFQUFFLE1BQWU7UUFDeEQsSUFBSSxPQUFPLENBQUMsS0FBSyxDQUFDO1lBQUUsT0FBTyxJQUFJLENBQUM7UUFFaEMsTUFBTSxHQUFHLE1BQU0sSUFBSSxJQUFJLENBQUMsT0FBTyxDQUFDO1FBRWhDLElBQUk7O2tCQUNJLEdBQUcsR0FBRyxXQUFXLENBQUMsS0FBSyxDQUFDO1lBQzlCLE9BQU8sWUFBWSxDQUFDLEdBQUcsRUFBRSxNQUFNLEVBQUUsVUFBVSxDQUFDLENBQUM7U0FDOUM7UUFBQyxPQUFPLEtBQUssRUFBRTtZQUNkLE1BQU0sd0JBQXdCLENBQUMsV0FBVyxFQUFFLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUM1RDtJQUNILENBQUM7OztZQS9CRixVQUFVO1lBQ1YsSUFBSSxTQUFDLEVBQUMsSUFBSSxFQUFFLFFBQVEsRUFBQzs7Ozt5Q0FFUCxNQUFNLFNBQUMsU0FBUzs7Ozs7OztJQUFqQiw4QkFBMEM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFxRHhELE1BQU0sT0FBTyxXQUFXOzs7O0lBQ3RCLFlBQXVDLE9BQWU7UUFBZixZQUFPLEdBQVAsT0FBTyxDQUFRO0lBQUcsQ0FBQzs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBa0IxRCxTQUFTLENBQUMsS0FBVSxFQUFFLFVBQW1CLEVBQUUsTUFBZTtRQUN4RCxJQUFJLE9BQU8sQ0FBQyxLQUFLLENBQUM7WUFBRSxPQUFPLElBQUksQ0FBQztRQUVoQyxNQUFNLEdBQUcsTUFBTSxJQUFJLElBQUksQ0FBQyxPQUFPLENBQUM7UUFFaEMsSUFBSTs7a0JBQ0ksR0FBRyxHQUFHLFdBQVcsQ0FBQyxLQUFLLENBQUM7WUFDOUIsT0FBTyxhQUFhLENBQUMsR0FBRyxFQUFFLE1BQU0sRUFBRSxVQUFVLENBQUMsQ0FBQztTQUMvQztRQUFDLE9BQU8sS0FBSyxFQUFFO1lBQ2QsTUFBTSx3QkFBd0IsQ0FBQyxXQUFXLEVBQUUsS0FBSyxDQUFDLE9BQU8sQ0FBQyxDQUFDO1NBQzVEO0lBQ0gsQ0FBQzs7O1lBaENGLFVBQVU7WUFDVixJQUFJLFNBQUMsRUFBQyxJQUFJLEVBQUUsU0FBUyxFQUFDOzs7O3lDQUVSLE1BQU0sU0FBQyxTQUFTOzs7Ozs7O0lBQWpCLDhCQUEwQzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQXNEeEQsTUFBTSxPQUFPLFlBQVk7Ozs7SUFDdkIsWUFBdUMsT0FBZTtRQUFmLFlBQU8sR0FBUCxPQUFPLENBQVE7SUFBRyxDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0lBa0MxRCxTQUFTLENBQ0wsS0FBVSxFQUFFLFlBQXFCLEVBQ2pDLFVBQTBELFFBQVEsRUFBRSxVQUFtQixFQUN2RixNQUFlO1FBQ2pCLElBQUksT0FBTyxDQUFDLEtBQUssQ0FBQztZQUFFLE9BQU8sSUFBSSxDQUFDO1FBRWhDLE1BQU0sR0FBRyxNQUFNLElBQUksSUFBSSxDQUFDLE9BQU8sQ0FBQztRQUVoQyxJQUFJLE9BQU8sT0FBTyxLQUFLLFNBQVMsRUFBRTtZQUNoQyxJQUFJLG1CQUFLLE9BQU8sRUFBQSxJQUFJLG1CQUFLLE9BQU8sQ0FBQyxJQUFJLEVBQUEsRUFBRTtnQkFDckMsT0FBTyxDQUFDLElBQUksQ0FDUiwwTUFBME0sQ0FBQyxDQUFDO2FBQ2pOO1lBQ0QsT0FBTyxHQUFHLE9BQU8sQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUM7U0FDdkM7O1lBRUcsUUFBUSxHQUFXLFlBQVksSUFBSSxLQUFLO1FBQzVDLElBQUksT0FBTyxLQUFLLE1BQU0sRUFBRTtZQUN0QixJQUFJLE9BQU8sS0FBSyxRQUFRLElBQUksT0FBTyxLQUFLLGVBQWUsRUFBRTtnQkFDdkQsUUFBUSxHQUFHLGlCQUFpQixDQUFDLFFBQVEsRUFBRSxPQUFPLEtBQUssUUFBUSxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLFFBQVEsRUFBRSxNQUFNLENBQUMsQ0FBQzthQUMxRjtpQkFBTTtnQkFDTCxRQUFRLEdBQUcsT0FBTyxDQUFDO2FBQ3BCO1NBQ0Y7UUFFRCxJQUFJOztrQkFDSSxHQUFHLEdBQUcsV0FBVyxDQUFDLEtBQUssQ0FBQztZQUM5QixPQUFPLGNBQWMsQ0FBQyxHQUFHLEVBQUUsTUFBTSxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsVUFBVSxDQUFDLENBQUM7U0FDeEU7UUFBQyxPQUFPLEtBQUssRUFBRTtZQUNkLE1BQU0sd0JBQXdCLENBQUMsWUFBWSxFQUFFLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUM3RDtJQUNILENBQUM7OztZQXBFRixVQUFVO1lBQ1YsSUFBSSxTQUFDLEVBQUMsSUFBSSxFQUFFLFVBQVUsRUFBQzs7Ozt5Q0FFVCxNQUFNLFNBQUMsU0FBUzs7Ozs7OztJQUFqQiwrQkFBMEM7Ozs7OztBQW9FeEQsU0FBUyxPQUFPLENBQUMsS0FBVTtJQUN6QixPQUFPLEtBQUssSUFBSSxJQUFJLElBQUksS0FBSyxLQUFLLEVBQUUsSUFBSSxLQUFLLEtBQUssS0FBSyxDQUFDO0FBQzFELENBQUM7Ozs7OztBQUtELFNBQVMsV0FBVyxDQUFDLEtBQXNCO0lBQ3pDLDZCQUE2QjtJQUM3QixJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsSUFBSSxDQUFDLEtBQUssQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLEdBQUcsVUFBVSxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUU7UUFDMUUsT0FBTyxNQUFNLENBQUMsS0FBSyxDQUFDLENBQUM7S0FDdEI7SUFDRCxJQUFJLE9BQU8sS0FBSyxLQUFLLFFBQVEsRUFBRTtRQUM3QixNQUFNLElBQUksS0FBSyxDQUFDLEdBQUcsS0FBSyxrQkFBa0IsQ0FBQyxDQUFDO0tBQzdDO0lBQ0QsT0FBTyxLQUFLLENBQUM7QUFDZixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdCwgSW5qZWN0YWJsZSwgTE9DQUxFX0lELCBQaXBlLCBQaXBlVHJhbnNmb3JtfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7Zm9ybWF0Q3VycmVuY3ksIGZvcm1hdE51bWJlciwgZm9ybWF0UGVyY2VudH0gZnJvbSAnLi4vaTE4bi9mb3JtYXRfbnVtYmVyJztcbmltcG9ydCB7Z2V0Q3VycmVuY3lTeW1ib2x9IGZyb20gJy4uL2kxOG4vbG9jYWxlX2RhdGFfYXBpJztcbmltcG9ydCB7aW52YWxpZFBpcGVBcmd1bWVudEVycm9yfSBmcm9tICcuL2ludmFsaWRfcGlwZV9hcmd1bWVudF9lcnJvcic7XG5cbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogVHJhbnNmb3JtcyBhIG51bWJlciBpbnRvIGEgc3RyaW5nLFxuICogZm9ybWF0dGVkIGFjY29yZGluZyB0byBsb2NhbGUgcnVsZXMgdGhhdCBkZXRlcm1pbmUgZ3JvdXAgc2l6aW5nIGFuZFxuICogc2VwYXJhdG9yLCBkZWNpbWFsLXBvaW50IGNoYXJhY3RlciwgYW5kIG90aGVyIGxvY2FsZS1zcGVjaWZpY1xuICogY29uZmlndXJhdGlvbnMuXG4gKlxuICogSWYgbm8gcGFyYW1ldGVycyBhcmUgc3BlY2lmaWVkLCB0aGUgZnVuY3Rpb24gcm91bmRzIG9mZiB0byB0aGUgbmVhcmVzdCB2YWx1ZSB1c2luZyB0aGlzXG4gKiBbcm91bmRpbmcgbWV0aG9kXShodHRwczovL2VuLndpa2lib29rcy5vcmcvd2lraS9Bcml0aG1ldGljL1JvdW5kaW5nKS5cbiAqIFRoZSBiZWhhdmlvciBkaWZmZXJzIGZyb20gdGhhdCBvZiB0aGUgSmF2YVNjcmlwdCBgYGBNYXRoLnJvdW5kKClgYGAgZnVuY3Rpb24uXG4gKiBJbiB0aGUgZm9sbG93aW5nIGNhc2UgZm9yIGV4YW1wbGUsIHRoZSBwaXBlIHJvdW5kcyBkb3duIHdoZXJlXG4gKiBgYGBNYXRoLnJvdW5kKClgYGAgcm91bmRzIHVwOlxuICpcbiAqIGBgYGh0bWxcbiAqIC0yLjUgfCBudW1iZXI6JzEuMC0wJ1xuICogPiAtM1xuICogTWF0aC5yb3VuZCgtMi41KVxuICogPiAtMlxuICogYGBgXG4gKlxuICogQHNlZSBgZm9ybWF0TnVtYmVyKClgXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqIFRoZSBmb2xsb3dpbmcgY29kZSBzaG93cyBob3cgdGhlIHBpcGUgdHJhbnNmb3JtcyBudW1iZXJzXG4gKiBpbnRvIHRleHQgc3RyaW5ncywgYWNjb3JkaW5nIHRvIHZhcmlvdXMgZm9ybWF0IHNwZWNpZmljYXRpb25zLFxuICogd2hlcmUgdGhlIGNhbGxlcidzIGRlZmF1bHQgbG9jYWxlIGlzIGBlbi1VU2AuXG4gKlxuICogIyMjIEV4YW1wbGVcbiAqXG4gKiA8Y29kZS1leGFtcGxlIHBhdGg9XCJjb21tb24vcGlwZXMvdHMvbnVtYmVyX3BpcGUudHNcIiByZWdpb249J051bWJlclBpcGUnPjwvY29kZS1leGFtcGxlPlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQEluamVjdGFibGUoKVxuQFBpcGUoe25hbWU6ICdudW1iZXInfSlcbmV4cG9ydCBjbGFzcyBEZWNpbWFsUGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICBjb25zdHJ1Y3RvcihASW5qZWN0KExPQ0FMRV9JRCkgcHJpdmF0ZSBfbG9jYWxlOiBzdHJpbmcpIHt9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgbnVtYmVyIHRvIGJlIGZvcm1hdHRlZC5cbiAgICogQHBhcmFtIGRpZ2l0c0luZm8gRGVjaW1hbCByZXByZXNlbnRhdGlvbiBvcHRpb25zLCBzcGVjaWZpZWQgYnkgYSBzdHJpbmdcbiAgICogaW4gdGhlIGZvbGxvd2luZyBmb3JtYXQ6PGJyPlxuICAgKiA8Y29kZT57bWluSW50ZWdlckRpZ2l0c30ue21pbkZyYWN0aW9uRGlnaXRzfS17bWF4RnJhY3Rpb25EaWdpdHN9PC9jb2RlPi5cbiAgICogICAtIGBtaW5JbnRlZ2VyRGlnaXRzYDogVGhlIG1pbmltdW0gbnVtYmVyIG9mIGludGVnZXIgZGlnaXRzIGJlZm9yZSB0aGUgZGVjaW1hbCBwb2ludC5cbiAgICogRGVmYXVsdCBpcyBgMWAuXG4gICAqICAgLSBgbWluRnJhY3Rpb25EaWdpdHNgOiBUaGUgbWluaW11bSBudW1iZXIgb2YgZGlnaXRzIGFmdGVyIHRoZSBkZWNpbWFsIHBvaW50LlxuICAgKiBEZWZhdWx0IGlzIGAwYC5cbiAgICogICAtIGBtYXhGcmFjdGlvbkRpZ2l0c2A6IFRoZSBtYXhpbXVtIG51bWJlciBvZiBkaWdpdHMgYWZ0ZXIgdGhlIGRlY2ltYWwgcG9pbnQuXG4gICAqIERlZmF1bHQgaXMgYDNgLlxuICAgKiBAcGFyYW0gbG9jYWxlIEEgbG9jYWxlIGNvZGUgZm9yIHRoZSBsb2NhbGUgZm9ybWF0IHJ1bGVzIHRvIHVzZS5cbiAgICogV2hlbiBub3Qgc3VwcGxpZWQsIHVzZXMgdGhlIHZhbHVlIG9mIGBMT0NBTEVfSURgLCB3aGljaCBpcyBgZW4tVVNgIGJ5IGRlZmF1bHQuXG4gICAqIFNlZSBbU2V0dGluZyB5b3VyIGFwcCBsb2NhbGVdKGd1aWRlL2kxOG4jc2V0dGluZy11cC10aGUtbG9jYWxlLW9mLXlvdXItYXBwKS5cbiAgICovXG4gIHRyYW5zZm9ybSh2YWx1ZTogYW55LCBkaWdpdHNJbmZvPzogc3RyaW5nLCBsb2NhbGU/OiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgaWYgKGlzRW1wdHkodmFsdWUpKSByZXR1cm4gbnVsbDtcblxuICAgIGxvY2FsZSA9IGxvY2FsZSB8fCB0aGlzLl9sb2NhbGU7XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgbnVtID0gc3RyVG9OdW1iZXIodmFsdWUpO1xuICAgICAgcmV0dXJuIGZvcm1hdE51bWJlcihudW0sIGxvY2FsZSwgZGlnaXRzSW5mbyk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHRocm93IGludmFsaWRQaXBlQXJndW1lbnRFcnJvcihEZWNpbWFsUGlwZSwgZXJyb3IubWVzc2FnZSk7XG4gICAgfVxuICB9XG59XG5cbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogVHJhbnNmb3JtcyBhIG51bWJlciB0byBhIHBlcmNlbnRhZ2VcbiAqIHN0cmluZywgZm9ybWF0dGVkIGFjY29yZGluZyB0byBsb2NhbGUgcnVsZXMgdGhhdCBkZXRlcm1pbmUgZ3JvdXAgc2l6aW5nIGFuZFxuICogc2VwYXJhdG9yLCBkZWNpbWFsLXBvaW50IGNoYXJhY3RlciwgYW5kIG90aGVyIGxvY2FsZS1zcGVjaWZpY1xuICogY29uZmlndXJhdGlvbnMuXG4gKlxuICogQHNlZSBgZm9ybWF0UGVyY2VudCgpYFxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiBUaGUgZm9sbG93aW5nIGNvZGUgc2hvd3MgaG93IHRoZSBwaXBlIHRyYW5zZm9ybXMgbnVtYmVyc1xuICogaW50byB0ZXh0IHN0cmluZ3MsIGFjY29yZGluZyB0byB2YXJpb3VzIGZvcm1hdCBzcGVjaWZpY2F0aW9ucyxcbiAqIHdoZXJlIHRoZSBjYWxsZXIncyBkZWZhdWx0IGxvY2FsZSBpcyBgZW4tVVNgLlxuICpcbiAqIDxjb2RlLWV4YW1wbGUgcGF0aD1cImNvbW1vbi9waXBlcy90cy9wZXJjZW50X3BpcGUudHNcIiByZWdpb249J1BlcmNlbnRQaXBlJz48L2NvZGUtZXhhbXBsZT5cbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbkBJbmplY3RhYmxlKClcbkBQaXBlKHtuYW1lOiAncGVyY2VudCd9KVxuZXhwb3J0IGNsYXNzIFBlcmNlbnRQaXBlIGltcGxlbWVudHMgUGlwZVRyYW5zZm9ybSB7XG4gIGNvbnN0cnVjdG9yKEBJbmplY3QoTE9DQUxFX0lEKSBwcml2YXRlIF9sb2NhbGU6IHN0cmluZykge31cblxuICAvKipcbiAgICpcbiAgICogQHBhcmFtIHZhbHVlIFRoZSBudW1iZXIgdG8gYmUgZm9ybWF0dGVkIGFzIGEgcGVyY2VudGFnZS5cbiAgICogQHBhcmFtIGRpZ2l0c0luZm8gRGVjaW1hbCByZXByZXNlbnRhdGlvbiBvcHRpb25zLCBzcGVjaWZpZWQgYnkgYSBzdHJpbmdcbiAgICogaW4gdGhlIGZvbGxvd2luZyBmb3JtYXQ6PGJyPlxuICAgKiA8Y29kZT57bWluSW50ZWdlckRpZ2l0c30ue21pbkZyYWN0aW9uRGlnaXRzfS17bWF4RnJhY3Rpb25EaWdpdHN9PC9jb2RlPi5cbiAgICogICAtIGBtaW5JbnRlZ2VyRGlnaXRzYDogVGhlIG1pbmltdW0gbnVtYmVyIG9mIGludGVnZXIgZGlnaXRzIGJlZm9yZSB0aGUgZGVjaW1hbCBwb2ludC5cbiAgICogRGVmYXVsdCBpcyBgMWAuXG4gICAqICAgLSBgbWluRnJhY3Rpb25EaWdpdHNgOiBUaGUgbWluaW11bSBudW1iZXIgb2YgZGlnaXRzIGFmdGVyIHRoZSBkZWNpbWFsIHBvaW50LlxuICAgKiBEZWZhdWx0IGlzIGAwYC5cbiAgICogICAtIGBtYXhGcmFjdGlvbkRpZ2l0c2A6IFRoZSBtYXhpbXVtIG51bWJlciBvZiBkaWdpdHMgYWZ0ZXIgdGhlIGRlY2ltYWwgcG9pbnQuXG4gICAqIERlZmF1bHQgaXMgYDBgLlxuICAgKiBAcGFyYW0gbG9jYWxlIEEgbG9jYWxlIGNvZGUgZm9yIHRoZSBsb2NhbGUgZm9ybWF0IHJ1bGVzIHRvIHVzZS5cbiAgICogV2hlbiBub3Qgc3VwcGxpZWQsIHVzZXMgdGhlIHZhbHVlIG9mIGBMT0NBTEVfSURgLCB3aGljaCBpcyBgZW4tVVNgIGJ5IGRlZmF1bHQuXG4gICAqIFNlZSBbU2V0dGluZyB5b3VyIGFwcCBsb2NhbGVdKGd1aWRlL2kxOG4jc2V0dGluZy11cC10aGUtbG9jYWxlLW9mLXlvdXItYXBwKS5cbiAgICovXG4gIHRyYW5zZm9ybSh2YWx1ZTogYW55LCBkaWdpdHNJbmZvPzogc3RyaW5nLCBsb2NhbGU/OiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgaWYgKGlzRW1wdHkodmFsdWUpKSByZXR1cm4gbnVsbDtcblxuICAgIGxvY2FsZSA9IGxvY2FsZSB8fCB0aGlzLl9sb2NhbGU7XG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgbnVtID0gc3RyVG9OdW1iZXIodmFsdWUpO1xuICAgICAgcmV0dXJuIGZvcm1hdFBlcmNlbnQobnVtLCBsb2NhbGUsIGRpZ2l0c0luZm8pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICB0aHJvdyBpbnZhbGlkUGlwZUFyZ3VtZW50RXJyb3IoUGVyY2VudFBpcGUsIGVycm9yLm1lc3NhZ2UpO1xuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIEBuZ01vZHVsZSBDb21tb25Nb2R1bGVcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIFRyYW5zZm9ybXMgYSBudW1iZXIgdG8gYSBjdXJyZW5jeSBzdHJpbmcsIGZvcm1hdHRlZCBhY2NvcmRpbmcgdG8gbG9jYWxlIHJ1bGVzXG4gKiB0aGF0IGRldGVybWluZSBncm91cCBzaXppbmcgYW5kIHNlcGFyYXRvciwgZGVjaW1hbC1wb2ludCBjaGFyYWN0ZXIsXG4gKiBhbmQgb3RoZXIgbG9jYWxlLXNwZWNpZmljIGNvbmZpZ3VyYXRpb25zLlxuICpcbiAqIEBzZWUgYGdldEN1cnJlbmN5U3ltYm9sKClgXG4gKiBAc2VlIGBmb3JtYXRDdXJyZW5jeSgpYFxuICpcbiAqIEB1c2FnZU5vdGVzXG4gKiBUaGUgZm9sbG93aW5nIGNvZGUgc2hvd3MgaG93IHRoZSBwaXBlIHRyYW5zZm9ybXMgbnVtYmVyc1xuICogaW50byB0ZXh0IHN0cmluZ3MsIGFjY29yZGluZyB0byB2YXJpb3VzIGZvcm1hdCBzcGVjaWZpY2F0aW9ucyxcbiAqIHdoZXJlIHRoZSBjYWxsZXIncyBkZWZhdWx0IGxvY2FsZSBpcyBgZW4tVVNgLlxuICpcbiAqIDxjb2RlLWV4YW1wbGUgcGF0aD1cImNvbW1vbi9waXBlcy90cy9jdXJyZW5jeV9waXBlLnRzXCIgcmVnaW9uPSdDdXJyZW5jeVBpcGUnPjwvY29kZS1leGFtcGxlPlxuICpcbiAqIEBwdWJsaWNBcGlcbiAqL1xuQEluamVjdGFibGUoKVxuQFBpcGUoe25hbWU6ICdjdXJyZW5jeSd9KVxuZXhwb3J0IGNsYXNzIEN1cnJlbmN5UGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICBjb25zdHJ1Y3RvcihASW5qZWN0KExPQ0FMRV9JRCkgcHJpdmF0ZSBfbG9jYWxlOiBzdHJpbmcpIHt9XG5cbiAgLyoqXG4gICAqXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgbnVtYmVyIHRvIGJlIGZvcm1hdHRlZCBhcyBjdXJyZW5jeS5cbiAgICogQHBhcmFtIGN1cnJlbmN5Q29kZSBUaGUgW0lTTyA0MjE3XShodHRwczovL2VuLndpa2lwZWRpYS5vcmcvd2lraS9JU09fNDIxNykgY3VycmVuY3kgY29kZSxcbiAgICogc3VjaCBhcyBgVVNEYCBmb3IgdGhlIFVTIGRvbGxhciBhbmQgYEVVUmAgZm9yIHRoZSBldXJvLlxuICAgKiBAcGFyYW0gZGlzcGxheSBUaGUgZm9ybWF0IGZvciB0aGUgY3VycmVuY3kgaW5kaWNhdG9yLiBPbmUgb2YgdGhlIGZvbGxvd2luZzpcbiAgICogICAtIGBjb2RlYDogU2hvdyB0aGUgY29kZSAoc3VjaCBhcyBgVVNEYCkuXG4gICAqICAgLSBgc3ltYm9sYChkZWZhdWx0KTogU2hvdyB0aGUgc3ltYm9sIChzdWNoIGFzIGAkYCkuXG4gICAqICAgLSBgc3ltYm9sLW5hcnJvd2A6IFVzZSB0aGUgbmFycm93IHN5bWJvbCBmb3IgbG9jYWxlcyB0aGF0IGhhdmUgdHdvIHN5bWJvbHMgZm9yIHRoZWlyXG4gICAqIGN1cnJlbmN5LlxuICAgKiBGb3IgZXhhbXBsZSwgdGhlIENhbmFkaWFuIGRvbGxhciBDQUQgaGFzIHRoZSBzeW1ib2wgYENBJGAgYW5kIHRoZSBzeW1ib2wtbmFycm93IGAkYC4gSWYgdGhlXG4gICAqIGxvY2FsZSBoYXMgbm8gbmFycm93IHN5bWJvbCwgdXNlcyB0aGUgc3RhbmRhcmQgc3ltYm9sIGZvciB0aGUgbG9jYWxlLlxuICAgKiAgIC0gU3RyaW5nOiBVc2UgdGhlIGdpdmVuIHN0cmluZyB2YWx1ZSBpbnN0ZWFkIG9mIGEgY29kZSBvciBhIHN5bWJvbC5cbiAgICogRm9yIGV4YW1wbGUsIGFuIGVtcHR5IHN0cmluZyB3aWxsIHN1cHByZXNzIHRoZSBjdXJyZW5jeSAmIHN5bWJvbC5cbiAgICogICAtIEJvb2xlYW4gKG1hcmtlZCBkZXByZWNhdGVkIGluIHY1KTogYHRydWVgIGZvciBzeW1ib2wgYW5kIGZhbHNlIGZvciBgY29kZWAuXG4gICAqXG4gICAqIEBwYXJhbSBkaWdpdHNJbmZvIERlY2ltYWwgcmVwcmVzZW50YXRpb24gb3B0aW9ucywgc3BlY2lmaWVkIGJ5IGEgc3RyaW5nXG4gICAqIGluIHRoZSBmb2xsb3dpbmcgZm9ybWF0Ojxicj5cbiAgICogPGNvZGU+e21pbkludGVnZXJEaWdpdHN9LnttaW5GcmFjdGlvbkRpZ2l0c30te21heEZyYWN0aW9uRGlnaXRzfTwvY29kZT4uXG4gICAqICAgLSBgbWluSW50ZWdlckRpZ2l0c2A6IFRoZSBtaW5pbXVtIG51bWJlciBvZiBpbnRlZ2VyIGRpZ2l0cyBiZWZvcmUgdGhlIGRlY2ltYWwgcG9pbnQuXG4gICAqIERlZmF1bHQgaXMgYDFgLlxuICAgKiAgIC0gYG1pbkZyYWN0aW9uRGlnaXRzYDogVGhlIG1pbmltdW0gbnVtYmVyIG9mIGRpZ2l0cyBhZnRlciB0aGUgZGVjaW1hbCBwb2ludC5cbiAgICogRGVmYXVsdCBpcyBgMmAuXG4gICAqICAgLSBgbWF4RnJhY3Rpb25EaWdpdHNgOiBUaGUgbWF4aW11bSBudW1iZXIgb2YgZGlnaXRzIGFmdGVyIHRoZSBkZWNpbWFsIHBvaW50LlxuICAgKiBEZWZhdWx0IGlzIGAyYC5cbiAgICogSWYgbm90IHByb3ZpZGVkLCB0aGUgbnVtYmVyIHdpbGwgYmUgZm9ybWF0dGVkIHdpdGggdGhlIHByb3BlciBhbW91bnQgb2YgZGlnaXRzLFxuICAgKiBkZXBlbmRpbmcgb24gd2hhdCB0aGUgW0lTTyA0MjE3XShodHRwczovL2VuLndpa2lwZWRpYS5vcmcvd2lraS9JU09fNDIxNykgc3BlY2lmaWVzLlxuICAgKiBGb3IgZXhhbXBsZSwgdGhlIENhbmFkaWFuIGRvbGxhciBoYXMgMiBkaWdpdHMsIHdoZXJlYXMgdGhlIENoaWxlYW4gcGVzbyBoYXMgbm9uZS5cbiAgICogQHBhcmFtIGxvY2FsZSBBIGxvY2FsZSBjb2RlIGZvciB0aGUgbG9jYWxlIGZvcm1hdCBydWxlcyB0byB1c2UuXG4gICAqIFdoZW4gbm90IHN1cHBsaWVkLCB1c2VzIHRoZSB2YWx1ZSBvZiBgTE9DQUxFX0lEYCwgd2hpY2ggaXMgYGVuLVVTYCBieSBkZWZhdWx0LlxuICAgKiBTZWUgW1NldHRpbmcgeW91ciBhcHAgbG9jYWxlXShndWlkZS9pMThuI3NldHRpbmctdXAtdGhlLWxvY2FsZS1vZi15b3VyLWFwcCkuXG4gICAqL1xuICB0cmFuc2Zvcm0oXG4gICAgICB2YWx1ZTogYW55LCBjdXJyZW5jeUNvZGU/OiBzdHJpbmcsXG4gICAgICBkaXNwbGF5OiAnY29kZSd8J3N5bWJvbCd8J3N5bWJvbC1uYXJyb3cnfHN0cmluZ3xib29sZWFuID0gJ3N5bWJvbCcsIGRpZ2l0c0luZm8/OiBzdHJpbmcsXG4gICAgICBsb2NhbGU/OiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgaWYgKGlzRW1wdHkodmFsdWUpKSByZXR1cm4gbnVsbDtcblxuICAgIGxvY2FsZSA9IGxvY2FsZSB8fCB0aGlzLl9sb2NhbGU7XG5cbiAgICBpZiAodHlwZW9mIGRpc3BsYXkgPT09ICdib29sZWFuJykge1xuICAgICAgaWYgKDxhbnk+Y29uc29sZSAmJiA8YW55PmNvbnNvbGUud2Fybikge1xuICAgICAgICBjb25zb2xlLndhcm4oXG4gICAgICAgICAgICBgV2FybmluZzogdGhlIGN1cnJlbmN5IHBpcGUgaGFzIGJlZW4gY2hhbmdlZCBpbiBBbmd1bGFyIHY1LiBUaGUgc3ltYm9sRGlzcGxheSBvcHRpb24gKHRoaXJkIHBhcmFtZXRlcikgaXMgbm93IGEgc3RyaW5nIGluc3RlYWQgb2YgYSBib29sZWFuLiBUaGUgYWNjZXB0ZWQgdmFsdWVzIGFyZSBcImNvZGVcIiwgXCJzeW1ib2xcIiBvciBcInN5bWJvbC1uYXJyb3dcIi5gKTtcbiAgICAgIH1cbiAgICAgIGRpc3BsYXkgPSBkaXNwbGF5ID8gJ3N5bWJvbCcgOiAnY29kZSc7XG4gICAgfVxuXG4gICAgbGV0IGN1cnJlbmN5OiBzdHJpbmcgPSBjdXJyZW5jeUNvZGUgfHwgJ1VTRCc7XG4gICAgaWYgKGRpc3BsYXkgIT09ICdjb2RlJykge1xuICAgICAgaWYgKGRpc3BsYXkgPT09ICdzeW1ib2wnIHx8IGRpc3BsYXkgPT09ICdzeW1ib2wtbmFycm93Jykge1xuICAgICAgICBjdXJyZW5jeSA9IGdldEN1cnJlbmN5U3ltYm9sKGN1cnJlbmN5LCBkaXNwbGF5ID09PSAnc3ltYm9sJyA/ICd3aWRlJyA6ICduYXJyb3cnLCBsb2NhbGUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY3VycmVuY3kgPSBkaXNwbGF5O1xuICAgICAgfVxuICAgIH1cblxuICAgIHRyeSB7XG4gICAgICBjb25zdCBudW0gPSBzdHJUb051bWJlcih2YWx1ZSk7XG4gICAgICByZXR1cm4gZm9ybWF0Q3VycmVuY3kobnVtLCBsb2NhbGUsIGN1cnJlbmN5LCBjdXJyZW5jeUNvZGUsIGRpZ2l0c0luZm8pO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICB0aHJvdyBpbnZhbGlkUGlwZUFyZ3VtZW50RXJyb3IoQ3VycmVuY3lQaXBlLCBlcnJvci5tZXNzYWdlKTtcbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNFbXB0eSh2YWx1ZTogYW55KTogYm9vbGVhbiB7XG4gIHJldHVybiB2YWx1ZSA9PSBudWxsIHx8IHZhbHVlID09PSAnJyB8fCB2YWx1ZSAhPT0gdmFsdWU7XG59XG5cbi8qKlxuICogVHJhbnNmb3JtcyBhIHN0cmluZyBpbnRvIGEgbnVtYmVyIChpZiBuZWVkZWQpLlxuICovXG5mdW5jdGlvbiBzdHJUb051bWJlcih2YWx1ZTogbnVtYmVyIHwgc3RyaW5nKTogbnVtYmVyIHtcbiAgLy8gQ29udmVydCBzdHJpbmdzIHRvIG51bWJlcnNcbiAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gJ3N0cmluZycgJiYgIWlzTmFOKE51bWJlcih2YWx1ZSkgLSBwYXJzZUZsb2F0KHZhbHVlKSkpIHtcbiAgICByZXR1cm4gTnVtYmVyKHZhbHVlKTtcbiAgfVxuICBpZiAodHlwZW9mIHZhbHVlICE9PSAnbnVtYmVyJykge1xuICAgIHRocm93IG5ldyBFcnJvcihgJHt2YWx1ZX0gaXMgbm90IGEgbnVtYmVyYCk7XG4gIH1cbiAgcmV0dXJuIHZhbHVlO1xufVxuIl19