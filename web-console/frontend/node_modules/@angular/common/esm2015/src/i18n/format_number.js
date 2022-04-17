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
import { NumberFormatStyle, NumberSymbol, getLocaleNumberFormat, getLocaleNumberSymbol, getNumberOfCurrencyDigits } from './locale_data_api';
/** @type {?} */
export const NUMBER_FORMAT_REGEXP = /^(\d+)?\.((\d+)(-(\d+))?)?$/;
/** @type {?} */
const MAX_DIGITS = 22;
/** @type {?} */
const DECIMAL_SEP = '.';
/** @type {?} */
const ZERO_CHAR = '0';
/** @type {?} */
const PATTERN_SEP = ';';
/** @type {?} */
const GROUP_SEP = ',';
/** @type {?} */
const DIGIT_CHAR = '#';
/** @type {?} */
const CURRENCY_CHAR = '¤';
/** @type {?} */
const PERCENT_CHAR = '%';
/**
 * Transforms a number to a locale string based on a style and a format.
 * @param {?} value
 * @param {?} pattern
 * @param {?} locale
 * @param {?} groupSymbol
 * @param {?} decimalSymbol
 * @param {?=} digitsInfo
 * @param {?=} isPercent
 * @return {?}
 */
function formatNumberToLocaleString(value, pattern, locale, groupSymbol, decimalSymbol, digitsInfo, isPercent = false) {
    /** @type {?} */
    let formattedText = '';
    /** @type {?} */
    let isZero = false;
    if (!isFinite(value)) {
        formattedText = getLocaleNumberSymbol(locale, NumberSymbol.Infinity);
    }
    else {
        /** @type {?} */
        let parsedNumber = parseNumber(value);
        if (isPercent) {
            parsedNumber = toPercent(parsedNumber);
        }
        /** @type {?} */
        let minInt = pattern.minInt;
        /** @type {?} */
        let minFraction = pattern.minFrac;
        /** @type {?} */
        let maxFraction = pattern.maxFrac;
        if (digitsInfo) {
            /** @type {?} */
            const parts = digitsInfo.match(NUMBER_FORMAT_REGEXP);
            if (parts === null) {
                throw new Error(`${digitsInfo} is not a valid digit info`);
            }
            /** @type {?} */
            const minIntPart = parts[1];
            /** @type {?} */
            const minFractionPart = parts[3];
            /** @type {?} */
            const maxFractionPart = parts[5];
            if (minIntPart != null) {
                minInt = parseIntAutoRadix(minIntPart);
            }
            if (minFractionPart != null) {
                minFraction = parseIntAutoRadix(minFractionPart);
            }
            if (maxFractionPart != null) {
                maxFraction = parseIntAutoRadix(maxFractionPart);
            }
            else if (minFractionPart != null && minFraction > maxFraction) {
                maxFraction = minFraction;
            }
        }
        roundNumber(parsedNumber, minFraction, maxFraction);
        /** @type {?} */
        let digits = parsedNumber.digits;
        /** @type {?} */
        let integerLen = parsedNumber.integerLen;
        /** @type {?} */
        const exponent = parsedNumber.exponent;
        /** @type {?} */
        let decimals = [];
        isZero = digits.every((/**
         * @param {?} d
         * @return {?}
         */
        d => !d));
        // pad zeros for small numbers
        for (; integerLen < minInt; integerLen++) {
            digits.unshift(0);
        }
        // pad zeros for small numbers
        for (; integerLen < 0; integerLen++) {
            digits.unshift(0);
        }
        // extract decimals digits
        if (integerLen > 0) {
            decimals = digits.splice(integerLen, digits.length);
        }
        else {
            decimals = digits;
            digits = [0];
        }
        // format the integer digits with grouping separators
        /** @type {?} */
        const groups = [];
        if (digits.length >= pattern.lgSize) {
            groups.unshift(digits.splice(-pattern.lgSize, digits.length).join(''));
        }
        while (digits.length > pattern.gSize) {
            groups.unshift(digits.splice(-pattern.gSize, digits.length).join(''));
        }
        if (digits.length) {
            groups.unshift(digits.join(''));
        }
        formattedText = groups.join(getLocaleNumberSymbol(locale, groupSymbol));
        // append the decimal digits
        if (decimals.length) {
            formattedText += getLocaleNumberSymbol(locale, decimalSymbol) + decimals.join('');
        }
        if (exponent) {
            formattedText += getLocaleNumberSymbol(locale, NumberSymbol.Exponential) + '+' + exponent;
        }
    }
    if (value < 0 && !isZero) {
        formattedText = pattern.negPre + formattedText + pattern.negSuf;
    }
    else {
        formattedText = pattern.posPre + formattedText + pattern.posSuf;
    }
    return formattedText;
}
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Formats a number as currency using locale rules.
 *
 * @see `formatNumber()` / `DecimalPipe` / [Internationalization (i18n) Guide](https://angular.io/guide/i18n)
 *
 * \@publicApi
 * @param {?} value The number to format.
 * @param {?} locale A locale code for the locale format rules to use.
 * @param {?} currency A string containing the currency symbol or its name,
 * such as "$" or "Canadian Dollar". Used in output string, but does not affect the operation
 * of the function.
 * @param {?=} currencyCode The [ISO 4217](https://en.wikipedia.org/wiki/ISO_4217)
 * currency code, such as `USD` for the US dollar and `EUR` for the euro.
 * Used to determine the number of digits in the decimal part.
 * @param {?=} digitsInfo
 * @return {?} The formatted currency value.
 *
 */
export function formatCurrency(value, locale, currency, currencyCode, digitsInfo) {
    /** @type {?} */
    const format = getLocaleNumberFormat(locale, NumberFormatStyle.Currency);
    /** @type {?} */
    const pattern = parseNumberFormat(format, getLocaleNumberSymbol(locale, NumberSymbol.MinusSign));
    pattern.minFrac = getNumberOfCurrencyDigits((/** @type {?} */ (currencyCode)));
    pattern.maxFrac = pattern.minFrac;
    /** @type {?} */
    const res = formatNumberToLocaleString(value, pattern, locale, NumberSymbol.CurrencyGroup, NumberSymbol.CurrencyDecimal, digitsInfo);
    return res
        .replace(CURRENCY_CHAR, currency)
        // if we have 2 time the currency character, the second one is ignored
        .replace(CURRENCY_CHAR, '');
}
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Formats a number as a percentage according to locale rules.
 *
 * @see `formatNumber()` / `DecimalPipe` / [Internationalization (i18n) Guide](https://angular.io/guide/i18n)
 * \@publicApi
 *
 * @param {?} value The number to format.
 * @param {?} locale A locale code for the locale format rules to use.
 * @param {?=} digitsInfo
 * @return {?} The formatted percentage value.
 *
 */
export function formatPercent(value, locale, digitsInfo) {
    /** @type {?} */
    const format = getLocaleNumberFormat(locale, NumberFormatStyle.Percent);
    /** @type {?} */
    const pattern = parseNumberFormat(format, getLocaleNumberSymbol(locale, NumberSymbol.MinusSign));
    /** @type {?} */
    const res = formatNumberToLocaleString(value, pattern, locale, NumberSymbol.Group, NumberSymbol.Decimal, digitsInfo, true);
    return res.replace(new RegExp(PERCENT_CHAR, 'g'), getLocaleNumberSymbol(locale, NumberSymbol.PercentSign));
}
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Formats a number as text, with group sizing, separator, and other
 * parameters based on the locale.
 *
 * @see [Internationalization (i18n) Guide](https://angular.io/guide/i18n)
 *
 * \@publicApi
 * @param {?} value The number to format.
 * @param {?} locale A locale code for the locale format rules to use.
 * @param {?=} digitsInfo
 * @return {?} The formatted text string.
 */
export function formatNumber(value, locale, digitsInfo) {
    /** @type {?} */
    const format = getLocaleNumberFormat(locale, NumberFormatStyle.Decimal);
    /** @type {?} */
    const pattern = parseNumberFormat(format, getLocaleNumberSymbol(locale, NumberSymbol.MinusSign));
    return formatNumberToLocaleString(value, pattern, locale, NumberSymbol.Group, NumberSymbol.Decimal, digitsInfo);
}
/**
 * @record
 */
function ParsedNumberFormat() { }
if (false) {
    /** @type {?} */
    ParsedNumberFormat.prototype.minInt;
    /** @type {?} */
    ParsedNumberFormat.prototype.minFrac;
    /** @type {?} */
    ParsedNumberFormat.prototype.maxFrac;
    /** @type {?} */
    ParsedNumberFormat.prototype.posPre;
    /** @type {?} */
    ParsedNumberFormat.prototype.posSuf;
    /** @type {?} */
    ParsedNumberFormat.prototype.negPre;
    /** @type {?} */
    ParsedNumberFormat.prototype.negSuf;
    /** @type {?} */
    ParsedNumberFormat.prototype.gSize;
    /** @type {?} */
    ParsedNumberFormat.prototype.lgSize;
}
/**
 * @param {?} format
 * @param {?=} minusSign
 * @return {?}
 */
function parseNumberFormat(format, minusSign = '-') {
    /** @type {?} */
    const p = {
        minInt: 1,
        minFrac: 0,
        maxFrac: 0,
        posPre: '',
        posSuf: '',
        negPre: '',
        negSuf: '',
        gSize: 0,
        lgSize: 0
    };
    /** @type {?} */
    const patternParts = format.split(PATTERN_SEP);
    /** @type {?} */
    const positive = patternParts[0];
    /** @type {?} */
    const negative = patternParts[1];
    /** @type {?} */
    const positiveParts = positive.indexOf(DECIMAL_SEP) !== -1 ?
        positive.split(DECIMAL_SEP) :
        [
            positive.substring(0, positive.lastIndexOf(ZERO_CHAR) + 1),
            positive.substring(positive.lastIndexOf(ZERO_CHAR) + 1)
        ];
    /** @type {?} */
    const integer = positiveParts[0];
    /** @type {?} */
    const fraction = positiveParts[1] || '';
    p.posPre = integer.substr(0, integer.indexOf(DIGIT_CHAR));
    for (let i = 0; i < fraction.length; i++) {
        /** @type {?} */
        const ch = fraction.charAt(i);
        if (ch === ZERO_CHAR) {
            p.minFrac = p.maxFrac = i + 1;
        }
        else if (ch === DIGIT_CHAR) {
            p.maxFrac = i + 1;
        }
        else {
            p.posSuf += ch;
        }
    }
    /** @type {?} */
    const groups = integer.split(GROUP_SEP);
    p.gSize = groups[1] ? groups[1].length : 0;
    p.lgSize = (groups[2] || groups[1]) ? (groups[2] || groups[1]).length : 0;
    if (negative) {
        /** @type {?} */
        const trunkLen = positive.length - p.posPre.length - p.posSuf.length;
        /** @type {?} */
        const pos = negative.indexOf(DIGIT_CHAR);
        p.negPre = negative.substr(0, pos).replace(/'/g, '');
        p.negSuf = negative.substr(pos + trunkLen).replace(/'/g, '');
    }
    else {
        p.negPre = minusSign + p.posPre;
        p.negSuf = p.posSuf;
    }
    return p;
}
/**
 * @record
 */
function ParsedNumber() { }
if (false) {
    /** @type {?} */
    ParsedNumber.prototype.digits;
    /** @type {?} */
    ParsedNumber.prototype.exponent;
    /** @type {?} */
    ParsedNumber.prototype.integerLen;
}
// Transforms a parsed number into a percentage by multiplying it by 100
/**
 * @param {?} parsedNumber
 * @return {?}
 */
function toPercent(parsedNumber) {
    // if the number is 0, don't do anything
    if (parsedNumber.digits[0] === 0) {
        return parsedNumber;
    }
    // Getting the current number of decimals
    /** @type {?} */
    const fractionLen = parsedNumber.digits.length - parsedNumber.integerLen;
    if (parsedNumber.exponent) {
        parsedNumber.exponent += 2;
    }
    else {
        if (fractionLen === 0) {
            parsedNumber.digits.push(0, 0);
        }
        else if (fractionLen === 1) {
            parsedNumber.digits.push(0);
        }
        parsedNumber.integerLen += 2;
    }
    return parsedNumber;
}
/**
 * Parses a number.
 * Significant bits of this parse algorithm came from https://github.com/MikeMcl/big.js/
 * @param {?} num
 * @return {?}
 */
function parseNumber(num) {
    /** @type {?} */
    let numStr = Math.abs(num) + '';
    /** @type {?} */
    let exponent = 0;
    /** @type {?} */
    let digits;
    /** @type {?} */
    let integerLen;
    /** @type {?} */
    let i;
    /** @type {?} */
    let j;
    /** @type {?} */
    let zeros;
    // Decimal point?
    if ((integerLen = numStr.indexOf(DECIMAL_SEP)) > -1) {
        numStr = numStr.replace(DECIMAL_SEP, '');
    }
    // Exponential form?
    if ((i = numStr.search(/e/i)) > 0) {
        // Work out the exponent.
        if (integerLen < 0)
            integerLen = i;
        integerLen += +numStr.slice(i + 1);
        numStr = numStr.substring(0, i);
    }
    else if (integerLen < 0) {
        // There was no decimal point or exponent so it is an integer.
        integerLen = numStr.length;
    }
    // Count the number of leading zeros.
    for (i = 0; numStr.charAt(i) === ZERO_CHAR; i++) { /* empty */
    }
    if (i === (zeros = numStr.length)) {
        // The digits are all zero.
        digits = [0];
        integerLen = 1;
    }
    else {
        // Count the number of trailing zeros
        zeros--;
        while (numStr.charAt(zeros) === ZERO_CHAR)
            zeros--;
        // Trailing zeros are insignificant so ignore them
        integerLen -= i;
        digits = [];
        // Convert string to array of digits without leading/trailing zeros.
        for (j = 0; i <= zeros; i++, j++) {
            digits[j] = Number(numStr.charAt(i));
        }
    }
    // If the number overflows the maximum allowed digits then use an exponent.
    if (integerLen > MAX_DIGITS) {
        digits = digits.splice(0, MAX_DIGITS - 1);
        exponent = integerLen - 1;
        integerLen = 1;
    }
    return { digits, exponent, integerLen };
}
/**
 * Round the parsed number to the specified number of decimal places
 * This function changes the parsedNumber in-place
 * @param {?} parsedNumber
 * @param {?} minFrac
 * @param {?} maxFrac
 * @return {?}
 */
function roundNumber(parsedNumber, minFrac, maxFrac) {
    if (minFrac > maxFrac) {
        throw new Error(`The minimum number of digits after fraction (${minFrac}) is higher than the maximum (${maxFrac}).`);
    }
    /** @type {?} */
    let digits = parsedNumber.digits;
    /** @type {?} */
    let fractionLen = digits.length - parsedNumber.integerLen;
    /** @type {?} */
    const fractionSize = Math.min(Math.max(minFrac, fractionLen), maxFrac);
    // The index of the digit to where rounding is to occur
    /** @type {?} */
    let roundAt = fractionSize + parsedNumber.integerLen;
    /** @type {?} */
    let digit = digits[roundAt];
    if (roundAt > 0) {
        // Drop fractional digits beyond `roundAt`
        digits.splice(Math.max(parsedNumber.integerLen, roundAt));
        // Set non-fractional digits beyond `roundAt` to 0
        for (let j = roundAt; j < digits.length; j++) {
            digits[j] = 0;
        }
    }
    else {
        // We rounded to zero so reset the parsedNumber
        fractionLen = Math.max(0, fractionLen);
        parsedNumber.integerLen = 1;
        digits.length = Math.max(1, roundAt = fractionSize + 1);
        digits[0] = 0;
        for (let i = 1; i < roundAt; i++)
            digits[i] = 0;
    }
    if (digit >= 5) {
        if (roundAt - 1 < 0) {
            for (let k = 0; k > roundAt; k--) {
                digits.unshift(0);
                parsedNumber.integerLen++;
            }
            digits.unshift(1);
            parsedNumber.integerLen++;
        }
        else {
            digits[roundAt - 1]++;
        }
    }
    // Pad out with zeros to get the required fraction length
    for (; fractionLen < Math.max(0, fractionSize); fractionLen++)
        digits.push(0);
    /** @type {?} */
    let dropTrailingZeros = fractionSize !== 0;
    // Minimal length = nb of decimals required + current nb of integers
    // Any number besides that is optional and can be removed if it's a trailing 0
    /** @type {?} */
    const minLen = minFrac + parsedNumber.integerLen;
    // Do any carrying, e.g. a digit was rounded up to 10
    /** @type {?} */
    const carry = digits.reduceRight((/**
     * @param {?} carry
     * @param {?} d
     * @param {?} i
     * @param {?} digits
     * @return {?}
     */
    function (carry, d, i, digits) {
        d = d + carry;
        digits[i] = d < 10 ? d : d - 10; // d % 10
        if (dropTrailingZeros) {
            // Do not keep meaningless fractional trailing zeros (e.g. 15.52000 --> 15.52)
            if (digits[i] === 0 && i >= minLen) {
                digits.pop();
            }
            else {
                dropTrailingZeros = false;
            }
        }
        return d >= 10 ? 1 : 0; // Math.floor(d / 10);
    }), 0);
    if (carry) {
        digits.unshift(carry);
        parsedNumber.integerLen++;
    }
}
/**
 * @param {?} text
 * @return {?}
 */
export function parseIntAutoRadix(text) {
    /** @type {?} */
    const result = parseInt(text);
    if (isNaN(result)) {
        throw new Error('Invalid integer literal when parsing ' + text);
    }
    return result;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZm9ybWF0X251bWJlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbW1vbi9zcmMvaTE4bi9mb3JtYXRfbnVtYmVyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7O0FBUUEsT0FBTyxFQUFDLGlCQUFpQixFQUFFLFlBQVksRUFBRSxxQkFBcUIsRUFBRSxxQkFBcUIsRUFBRSx5QkFBeUIsRUFBQyxNQUFNLG1CQUFtQixDQUFDOztBQUUzSSxNQUFNLE9BQU8sb0JBQW9CLEdBQUcsNkJBQTZCOztNQUMzRCxVQUFVLEdBQUcsRUFBRTs7TUFDZixXQUFXLEdBQUcsR0FBRzs7TUFDakIsU0FBUyxHQUFHLEdBQUc7O01BQ2YsV0FBVyxHQUFHLEdBQUc7O01BQ2pCLFNBQVMsR0FBRyxHQUFHOztNQUNmLFVBQVUsR0FBRyxHQUFHOztNQUNoQixhQUFhLEdBQUcsR0FBRzs7TUFDbkIsWUFBWSxHQUFHLEdBQUc7Ozs7Ozs7Ozs7OztBQUt4QixTQUFTLDBCQUEwQixDQUMvQixLQUFhLEVBQUUsT0FBMkIsRUFBRSxNQUFjLEVBQUUsV0FBeUIsRUFDckYsYUFBMkIsRUFBRSxVQUFtQixFQUFFLFNBQVMsR0FBRyxLQUFLOztRQUNqRSxhQUFhLEdBQUcsRUFBRTs7UUFDbEIsTUFBTSxHQUFHLEtBQUs7SUFFbEIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxLQUFLLENBQUMsRUFBRTtRQUNwQixhQUFhLEdBQUcscUJBQXFCLENBQUMsTUFBTSxFQUFFLFlBQVksQ0FBQyxRQUFRLENBQUMsQ0FBQztLQUN0RTtTQUFNOztZQUNELFlBQVksR0FBRyxXQUFXLENBQUMsS0FBSyxDQUFDO1FBRXJDLElBQUksU0FBUyxFQUFFO1lBQ2IsWUFBWSxHQUFHLFNBQVMsQ0FBQyxZQUFZLENBQUMsQ0FBQztTQUN4Qzs7WUFFRyxNQUFNLEdBQUcsT0FBTyxDQUFDLE1BQU07O1lBQ3ZCLFdBQVcsR0FBRyxPQUFPLENBQUMs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QUFFO1lBQ3JCLDhFQUE4RTtZQUM5RSxJQUFJLE1BQU0sQ0FBQyxDQUFDLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxJQUFJLE1BQU0sRUFBRTtnQkFDbEMsTUFBTSxDQUFDLEdBQUcsRUFBRSxDQUFDO2FBQ2Q7aUJBQU07Z0JBQ0wsaUJBQWlCLEdBQUcsS0FBSyxDQUFDO2FBQzNCO1NBQ0Y7UUFDRCxPQUFPLENBQUMsSUFBSSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUUsc0JBQXNCO0lBQ2pELENBQUMsR0FBRSxDQUFDLENBQUM7SUFDTCxJQUFJLEtBQUssRUFBRTtRQUNULE1BQU0sQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLENBQUM7UUFDdEIsWUFBWSxDQUFDLFVBQVUsRUFBRSxDQUFDO0tBQzNCO0FBQ0gsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsaUJBQWlCLENBQUMsSUFBWTs7VUFDdEMsTUFBTSxHQUFXLFFBQVEsQ0FBQyxJQUFJLENBQUM7SUFDckMsSUFBSSxLQUFLLENBQUMsTUFBTSxDQUFDLEVBQUU7UUFDakIsTUFBTSxJQUFJLEtBQUssQ0FBQyx1Q0FBdUMsR0FBRyxJQUFJLENBQUMsQ0FBQztLQUNqRTtJQUNELE9BQU8sTUFBTSxDQUFDO0FBQ2hCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7TnVtYmVyRm9ybWF0U3R5bGUsIE51bWJlclN5bWJvbCwgZ2V0TG9jYWxlTnVtYmVyRm9ybWF0LCBnZXRMb2NhbGVOdW1iZXJTeW1ib2wsIGdldE51bWJlck9mQ3VycmVuY3lEaWdpdHN9IGZyb20gJy4vbG9jYWxlX2RhdGFfYXBpJztcblxuZXhwb3J0IGNvbnN0IE5VTUJFUl9GT1JNQVRfUkVHRVhQID0gL14oXFxkKyk/XFwuKChcXGQrKSgtKFxcZCspKT8pPyQvO1xuY29uc3QgTUFYX0RJR0lUUyA9IDIyO1xuY29uc3QgREVDSU1BTF9TRVAgPSAnLic7XG5jb25zdCBaRVJPX0NIQVIgPSAnMCc7XG5jb25zdCBQQVRURVJOX1NFUCA9ICc7JztcbmNvbnN0IEdST1VQX1NFUCA9ICcsJztcbmNvbnN0IERJR0lUX0NIQVIgPSAnIyc7XG5jb25zdCBDVVJSRU5DWV9DSEFSID0gJ8KkJztcbmNvbnN0IFBFUkNFTlRfQ0hBUiA9ICclJztcblxuLyoqXG4gKiBUcmFuc2Zvcm1zIGEgbnVtYmVyIHRvIGEgbG9jYWxlIHN0cmluZyBiYXNlZCBvbiBhIHN0eWxlIGFuZCBhIGZvcm1hdC5cbiAqL1xuZnVuY3Rpb24gZm9ybWF0TnVtYmVyVG9Mb2NhbGVTdHJpbmcoXG4gICAgdmFsdWU6IG51bWJlciwgcGF0dGVybjogUGFyc2VkTnVtYmVyRm9ybWF0LCBsb2NhbGU6IHN0cmluZywgZ3JvdXBTeW1ib2w6IE51bWJlclN5bWJvbCxcbiAgICBkZWNpbWFsU3ltYm9sOiBOdW1iZXJTeW1ib2wsIGRpZ2l0c0luZm8/OiBzdHJpbmcsIGlzUGVyY2VudCA9IGZhbHNlKTogc3RyaW5nIHtcbiAgbGV0IGZvcm1hdHRlZFRleHQgPSAnJztcbiAgbGV0IGlzWmVybyA9IGZhbHNlO1xuXG4gIGlmICghaXNGaW5pdGUodmFsdWUpKSB7XG4gICAgZm9ybWF0dGVkVGV4dCA9IGdldExvY2FsZU51bWJlclN5bWJvbChsb2NhbGUsIE51bWJlclN5bWJvbC5JbmZpbml0eSk7XG4gIH0gZWxzZSB7XG4gICAgbGV0IHBhcnNlZE51bWJlciA9IHBhcnNlTnVtYmVyKHZhbHVlKTtcblxuICAgIGlmIChpc1BlcmNlbnQpIHtcbiAgICAgIHBhcnNlZE51bWJlciA9IHRvUGVyY2VudChwYXJzZWROdW1iZXIpO1xuICAgIH1cblxuICAgIGxldCBtaW5JbnQgPSBwYXR0ZXJuLm1pbkludDtcbiAgICBsZXQgbWluRnJhY3Rpb24gPSBwYXR0ZXJuLm1pbkZyYWM7XG4gICAgbGV0IG1heEZyYWN0aW9uID0gcGF0dGVybi5tYXhGcmFjO1xuXG4gICAgaWYgKGRpZ2l0c0luZm8pIHtcbiAgICAgIGNvbnN0IHBhcnRzID0gZGlnaXRzSW5mby5tYXRjaChOVU1CRVJfRk9STUFUX1JFR0VYUCk7XG4gICAgICBpZiAocGFydHMgPT09IG51bGwpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGAke2RpZ2l0c0luZm99IGlzIG5vdCBhIHZhbGlkIGRpZ2l0IGluZm9gKTtcbiAgICAgIH1cbiAgICAgIGNvbnN0IG1pbkludFBhcnQgPSBwYXJ0c1sxXTtcbiAgICAgIGNvbnN0IG1pbkZyYWN0aW9uUGFydCA9IHBhcnRzWzNdO1xuICAgICAgY29uc3QgbWF4RnJhY3Rpb25QYXJ0ID0gcGFydHNbNV07XG4gICAgICBpZiAobWluSW50UGFydCAhPSBudWxsKSB7XG4gICAgICAgIG1pbkludCA9IHBhcnNlSW50QXV0b1JhZGl4KG1pbkludFBhcnQpO1xuICAgICAgfVxuICAgICAgaWYgKG1pbkZyYWN0aW9uUGFydCAhPSBudWxsKSB7XG4gICAgICAgIG1pbkZyYWN0aW9uID0gcGFyc2VJbnRBdXRvUmFkaXgobWluRnJhY3Rpb25QYXJ0KTtcbiAgICAgIH1cbiAgICAgIGlmIChtYXhGcmFjdGlvblBhcnQgIT0gbnVsbCkge1xuICAgICAgICBtYXhGcmFjdGlvbiA9IHBhcnNlSW50QXV0b1JhZGl4KG1heEZyYWN0aW9uUGFydCk7XG4gICAgICB9IGVsc2UgaWYgKG1pbkZyYWN0aW9uUGFydCAhPSBudWxsICYmIG1pbkZyYWN0aW9uID4gbWF4RnJhY3Rpb24pIHtcbiAgICAgICAgbWF4RnJhY3Rpb24gPSBtaW5GcmFjdGlvbjtcbiAgICAgIH1cbiAgICB9XG5cbiAgICByb3VuZE51bWJlcihwYXJzZWROdW1iZXIsIG1pbkZyYWN0aW9uLCBtYXhGcmFjdGlvbik7XG5cbiAgICBsZXQgZGlnaXRzID0gcGFyc2VkTnVtYmVyLmRpZ2l0cztcbiAgICBsZXQgaW50ZWdlckxlbiA9IHBhcnNlZE51bWJlci5pbnRlZ2VyTGVuO1xuICAgIGNvbnN0IGV4cG9uZW50ID0gcGFyc2VkTnVtYmVyLmV4cG9uZW50O1xuICAgIGxldCBkZWNpbWFscyA9IFtdO1xuICAgIGlzWmVybyA9IGRpZ2l0cy5ldmVyeShkID0+ICFkKTtcblxuICAgIC8vIHBhZCB6ZXJvcyBmb3Igc21hbGwgbnVtYmVyc1xuICAgIGZvciAoOyBpbnRlZ2VyTGVuIDwgbWluSW50OyBpbnRlZ2VyTGVuKyspIHtcbiAgICAgIGRpZ2l0cy51bnNoaWZ0KDApO1xuICAgIH1cblxuICAgIC8vIHBhZCB6ZXJvcyBmb3Igc21hbGwgbnVtYmVyc1xuICAgIGZvciAoOyBpbnRlZ2VyTGVuIDwgMDsgaW50ZWdlckxlbisrKSB7XG4gICAgICBkaWdpdHMudW5zaGlmdCgwKTtcbiAgICB9XG5cbiAgICAvLyBleHRyYWN0IGRlY2ltYWxzIGRpZ2l0c1xuICAgIGlmIChpbnRlZ2VyTGVuID4gMCkge1xuICAgICAgZGVjaW1hbHMgPSBkaWdpdHMuc3BsaWNlKGludGVnZXJMZW4sIGRpZ2l0cy5sZW5ndGgpO1xuICAgIH0gZWxzZSB7XG4gICAgICBkZWNpbWFscyA9IGRpZ2l0cztcbiAgICAgIGRpZ2l0cyA9IFswXTtcbiAgICB9XG5cbiAgICAvLyBmb3JtYXQgdGhlIGludGVnZXIgZGlnaXRzIHdpdGggZ3JvdXBpbmcgc2VwYXJhdG9yc1xuICAgIGNvbnN0IGdyb3VwcyA9IFtdO1xuICAgIGlmIChkaWdpdHMubGVuZ3RoID49IHBhdHRlcm4ubGdTaXplKSB7XG4gICAgICBncm91cHMudW5zaGlmdChkaWdpdHMuc3BsaWNlKC1wYXR0ZXJuLmxnU2l6ZSwgZGlnaXRzLmxlbmd0aCkuam9pbignJykpO1xuICAgIH1cblxuICAgIHdoaWxlIChkaWdpdHMubGVuZ3RoID4gcGF0dGVybi5nU2l6ZSkge1xuICAgICAgZ3JvdXBzLnVuc2hpZnQoZGlnaXRzLnNwbGljZSgtcGF0dGVybi5nU2l6ZSwgZGlnaXRzLmxlbmd0aCkuam9pbignJykpO1xuICAgIH1cblxuICAgIGlmIChkaWdpdHMubGVuZ3RoKSB7XG4gICAgICBncm91cHMudW5zaGlmdChkaWdpdHMuam9pbignJykpO1xuICAgIH1cblxuICAgIGZvcm1hdHRlZFRleHQgPSBncm91cHMuam9pbihnZXRMb2NhbGVOdW1iZXJTeW1ib2wobG9jYWxlLCBncm91cFN5bWJvbCkpO1xuXG4gICAgLy8gYXBwZW5kIHRoZSBkZWNpbWFsIGRpZ2l0c1xuICAgIGlmIChkZWNpbWFscy5sZW5ndGgpIHtcbiAgICAgIGZvcm1hdHRlZFRleHQgKz0gZ2V0TG9jYWxlTnVtYmVyU3ltYm9sKGxvY2FsZSwgZGVjaW1hbFN5bWJvbCkgKyBkZWNpbWFscy5qb2luKCcnKTtcbiAgICB9XG5cbiAgICBpZiAoZXhwb25lbnQpIHtcbiAgICAgIGZvcm1hdHRlZFRleHQgKz0gZ2V0TG9jYWxlTnVtYmVyU3ltYm9sKGxvY2FsZSwgTnVtYmVyU3ltYm9sLkV4cG9uZW50aWFsKSArICcrJyArIGV4cG9uZW50O1xuICAgIH1cbiAgfVxuXG4gIGlmICh2YWx1ZSA8IDAgJiYgIWlzWmVybykge1xuICAgIGZvcm1hdHRlZFRleHQgPSBwYXR0ZXJuLm5lZ1ByZSArIGZvcm1hdHRlZFRleHQgKyBwYXR0ZXJuLm5lZ1N1ZjtcbiAgfSBlbHNlIHtcbiAgICBmb3JtYXR0ZWRUZXh0ID0gcGF0dGVybi5wb3NQcmUgKyBmb3JtYXR0ZWRUZXh0ICsgcGF0dGVybi5wb3NTdWY7XG4gIH1cblxuICByZXR1cm4gZm9ybWF0dGVkVGV4dDtcbn1cblxuLyoqXG4gKiBAbmdNb2R1bGUgQ29tbW9uTW9kdWxlXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBGb3JtYXRzIGEgbnVtYmVyIGFzIGN1cnJlbmN5IHVzaW5nIGxvY2FsZSBydWxlcy5cbiAqXG4gKiBAcGFyYW0gdmFsdWUgVGhlIG51bWJlciB0byBmb3JtYXQuXG4gKiBAcGFyYW0gbG9jYWxlIEEgbG9jYWxlIGNvZGUgZm9yIHRoZSBsb2NhbGUgZm9ybWF0IHJ1bGVzIHRvIHVzZS5cbiAqIEBwYXJhbSBjdXJyZW5jeSBBIHN0cmluZyBjb250YWluaW5nIHRoZSBjdXJyZW5jeSBzeW1ib2wgb3IgaXRzIG5hbWUsXG4gKiBzdWNoIGFzIFwiJFwiIG9yIFwiQ2FuYWRpYW4gRG9sbGFyXCIuIFVzZWQgaW4gb3V0cHV0IHN0cmluZywgYnV0IGRvZXMgbm90IGFmZmVjdCB0aGUgb3BlcmF0aW9uXG4gKiBvZiB0aGUgZnVuY3Rpb24uXG4gKiBAcGFyYW0gY3VycmVuY3lDb2RlIFRoZSBbSVNPIDQyMTddKGh0dHBzOi8vZW4ud2lraXBlZGlhLm9yZy93aWtpL0lTT180MjE3KVxuICogY3VycmVuY3kgY29kZSwgc3VjaCBhcyBgVVNEYCBmb3IgdGhlIFVTIGRvbGxhciBhbmQgYEVVUmAgZm9yIHRoZSBldXJvLlxuICogVXNlZCB0byBkZXRlcm1pbmUgdGhlIG51bWJlciBvZiBkaWdpdHMgaW4gdGhlIGRlY2ltYWwgcGFydC5cbiAqIEBwYXJhbSBkaWdpdEluZm8gRGVjaW1hbCByZXByZXNlbnRhdGlvbiBvcHRpb25zLCBzcGVjaWZpZWQgYnkgYSBzdHJpbmcgaW4gdGhlIGZvbGxvd2luZyBmb3JtYXQ6XG4gKiBge21pbkludGVnZXJEaWdpdHN9LnttaW5GcmFjdGlvbkRpZ2l0c30te21heEZyYWN0aW9uRGlnaXRzfWAuIFNlZSBgRGVjaW1hbFBpcGVgIGZvciBtb3JlIGRldGFpbHMuXG4gKlxuICogQHJldHVybnMgVGhlIGZvcm1hdHRlZCBjdXJyZW5jeSB2YWx1ZS5cbiAqXG4gKiBAc2VlIGBmb3JtYXROdW1iZXIoKWBcbiAqIEBzZWUgYERlY2ltYWxQaXBlYFxuICogQHNlZSBbSW50ZXJuYXRpb25hbGl6YXRpb24gKGkxOG4pIEd1aWRlXShodHRwczovL2FuZ3VsYXIuaW8vZ3VpZGUvaTE4bilcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXRDdXJyZW5jeShcbiAgICB2YWx1ZTogbnVtYmVyLCBsb2NhbGU6IHN0cmluZywgY3VycmVuY3k6IHN0cmluZywgY3VycmVuY3lDb2RlPzogc3RyaW5nLFxuICAgIGRpZ2l0c0luZm8/OiBzdHJpbmcpOiBzdHJpbmcge1xuICBjb25zdCBmb3JtYXQgPSBnZXRMb2NhbGVOdW1iZXJGb3JtYXQobG9jYWxlLCBOdW1iZXJGb3JtYXRTdHlsZS5DdXJyZW5jeSk7XG4gIGNvbnN0IHBhdHRlcm4gPSBwYXJzZU51bWJlckZvcm1hdChmb3JtYXQsIGdldExvY2FsZU51bWJlclN5bWJvbChsb2NhbGUsIE51bWJlclN5bWJvbC5NaW51c1NpZ24pKTtcblxuICBwYXR0ZXJuLm1pbkZyYWMgPSBnZXROdW1iZXJPZkN1cnJlbmN5RGlnaXRzKGN1cnJlbmN5Q29kZSAhKTtcbiAgcGF0dGVybi5tYXhGcmFjID0gcGF0dGVybi5taW5GcmFjO1xuXG4gIGNvbnN0IHJlcyA9IGZvcm1hdE51bWJlclRvTG9jYWxlU3RyaW5nKFxuICAgICAgdmFsdWUsIHBhdHRlcm4sIGxvY2FsZSwgTnVtYmVyU3ltYm9sLkN1cnJlbmN5R3JvdXAsIE51bWJlclN5bWJvbC5DdXJyZW5jeURlY2ltYWwsIGRpZ2l0c0luZm8pO1xuICByZXR1cm4gcmVzXG4gICAgICAucmVwbGFjZShDVVJSRU5DWV9DSEFSLCBjdXJyZW5jeSlcbiAgICAgIC8vIGlmIHdlIGhhdmUgMiB0aW1lIHRoZSBjdXJyZW5jeSBjaGFyYWN0ZXIsIHRoZSBzZWNvbmQgb25lIGlzIGlnbm9yZWRcbiAgICAgIC5yZXBsYWNlKENVUlJFTkNZX0NIQVIsICcnKTtcbn1cblxuLyoqXG4gKiBAbmdNb2R1bGUgQ29tbW9uTW9kdWxlXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBGb3JtYXRzIGEgbnVtYmVyIGFzIGEgcGVyY2VudGFnZSBhY2NvcmRpbmcgdG8gbG9jYWxlIHJ1bGVzLlxuICpcbiAqIEBwYXJhbSB2YWx1ZSBUaGUgbnVtYmVyIHRvIGZvcm1hdC5cbiAqIEBwYXJhbSBsb2NhbGUgQSBsb2NhbGUgY29kZSBmb3IgdGhlIGxvY2FsZSBmb3JtYXQgcnVsZXMgdG8gdXNlLlxuICogQHBhcmFtIGRpZ2l0SW5mbyBEZWNpbWFsIHJlcHJlc2VudGF0aW9uIG9wdGlvbnMsIHNwZWNpZmllZCBieSBhIHN0cmluZyBpbiB0aGUgZm9sbG93aW5nIGZvcm1hdDpcbiAqIGB7bWluSW50ZWdlckRpZ2l0c30ue21pbkZyYWN0aW9uRGlnaXRzfS17bWF4RnJhY3Rpb25EaWdpdHN9YC4gU2VlIGBEZWNpbWFsUGlwZWAgZm9yIG1vcmUgZGV0YWlscy5cbiAqXG4gKiBAcmV0dXJucyBUaGUgZm9ybWF0dGVkIHBlcmNlbnRhZ2UgdmFsdWUuXG4gKlxuICogQHNlZSBgZm9ybWF0TnVtYmVyKClgXG4gKiBAc2VlIGBEZWNpbWFsUGlwZWBcbiAqIEBzZWUgW0ludGVybmF0aW9uYWxpemF0aW9uIChpMThuKSBHdWlkZV0oaHR0cHM6Ly9hbmd1bGFyLmlvL2d1aWRlL2kxOG4pXG4gKiBAcHVibGljQXBpXG4gKlxuICovXG5leHBvcnQgZnVuY3Rpb24gZm9ybWF0UGVyY2VudCh2YWx1ZTogbnVtYmVyLCBsb2NhbGU6IHN0cmluZywgZGlnaXRzSW5mbz86IHN0cmluZyk6IHN0cmluZyB7XG4gIGNvbnN0IGZvcm1hdCA9IGdldExvY2FsZU51bWJlckZvcm1hdChsb2NhbGUsIE51bWJlckZvcm1hdFN0eWxlLlBlcmNlbnQpO1xuICBjb25zdCBwYXR0ZXJuID0gcGFyc2VOdW1iZXJGb3JtYXQoZm9ybWF0LCBnZXRMb2NhbGVOdW1iZXJTeW1ib2wobG9jYWxlLCBOdW1iZXJTeW1ib2wuTWludXNTaWduKSk7XG4gIGNvbnN0IHJlcyA9IGZvcm1hdE51bWJlclRvTG9jYWxlU3RyaW5nKFxuICAgICAgdmFsdWUsIHBhdHRlcm4sIGxvY2FsZSwgTnVtYmVyU3ltYm9sLkdyb3VwLCBOdW1iZXJTeW1ib2wuRGVjaW1hbCwgZGlnaXRzSW5mbywgdHJ1ZSk7XG4gIHJldHVybiByZXMucmVwbGFjZShcbiAgICAgIG5ldyBSZWdFeHAoUEVSQ0VOVF9DSEFSLCAnZycpLCBnZXRMb2NhbGVOdW1iZXJTeW1ib2wobG9jYWxlLCBOdW1iZXJTeW1ib2wuUGVyY2VudFNpZ24pKTtcbn1cblxuLyoqXG4gKiBAbmdNb2R1bGUgQ29tbW9uTW9kdWxlXG4gKiBAZGVzY3JpcHRpb25cbiAqXG4gKiBGb3JtYXRzIGEgbnVtYmVyIGFzIHRleHQsIHdpdGggZ3JvdXAgc2l6aW5nLCBzZXBhcmF0b3IsIGFuZCBvdGhlclxuICogcGFyYW1ldGVycyBiYXNlZCBvbiB0aGUgbG9jYWxlLlxuICpcbiAqIEBwYXJhbSB2YWx1ZSBUaGUgbnVtYmVyIHRvIGZvcm1hdC5cbiAqIEBwYXJhbSBsb2NhbGUgQSBsb2NhbGUgY29kZSBmb3IgdGhlIGxvY2FsZSBmb3JtYXQgcnVsZXMgdG8gdXNlLlxuICogQHBhcmFtIGRpZ2l0SW5mbyBEZWNpbWFsIHJlcHJlc2VudGF0aW9uIG9wdGlvbnMsIHNwZWNpZmllZCBieSBhIHN0cmluZyBpbiB0aGUgZm9sbG93aW5nIGZvcm1hdDpcbiAqIGB7bWluSW50ZWdlckRpZ2l0c30ue21pbkZyYWN0aW9uRGlnaXRzfS17bWF4RnJhY3Rpb25EaWdpdHN9YC4gU2VlIGBEZWNpbWFsUGlwZWAgZm9yIG1vcmUgZGV0YWlscy5cbiAqXG4gKiBAcmV0dXJucyBUaGUgZm9ybWF0dGVkIHRleHQgc3RyaW5nLlxuICogQHNlZSBbSW50ZXJuYXRpb25hbGl6YXRpb24gKGkxOG4pIEd1aWRlXShodHRwczovL2FuZ3VsYXIuaW8vZ3VpZGUvaTE4bilcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBmb3JtYXROdW1iZXIodmFsdWU6IG51bWJlciwgbG9jYWxlOiBzdHJpbmcsIGRpZ2l0c0luZm8/OiBzdHJpbmcpOiBzdHJpbmcge1xuICBjb25zdCBmb3JtYXQgPSBnZXRMb2NhbGVOdW1iZXJGb3JtYXQobG9jYWxlLCBOdW1iZXJGb3JtYXRTdHlsZS5EZWNpbWFsKTtcbiAgY29uc3QgcGF0dGVybiA9IHBhcnNlTnVtYmVyRm9ybWF0KGZvcm1hdCwgZ2V0TG9jYWxlTnVtYmVyU3ltYm9sKGxvY2FsZSwgTnVtYmVyU3ltYm9sLk1pbnVzU2lnbikpO1xuICByZXR1cm4gZm9ybWF0TnVtYmVyVG9Mb2NhbGVTdHJpbmcoXG4gICAgICB2YWx1ZSwgcGF0dGVybiwgbG9jYWxlLCBOdW1iZXJTeW1ib2wuR3JvdXAsIE51bWJlclN5bWJvbC5EZWNpbWFsLCBkaWdpdHNJbmZvKTtcbn1cblxuaW50ZXJmYWNlIFBhcnNlZE51bWJlckZvcm1hdCB7XG4gIG1pbkludDogbnVtYmVyO1xuICAvLyB0aGUgbWluaW11bSBudW1iZXIgb2YgZGlnaXRzIHJlcXVpcmVkIGluIHRoZSBmcmFjdGlvbiBwYXJ0IG9mIHRoZSBudW1iZXJcbiAgbWluRnJhYzogbnVtYmVyO1xuICAvLyB0aGUgbWF4aW11bSBudW1iZXIgb2YgZGlnaXRzIHJlcXVpcmVkIGluIHRoZSBmcmFjdGlvbiBwYXJ0IG9mIHRoZSBudW1iZXJcbiAgbWF4RnJhYzogbnVtYmVyO1xuICAvLyB0aGUgcHJlZml4IGZvciBhIHBvc2l0aXZlIG51bWJlclxuICBwb3NQcmU6IHN0cmluZztcbiAgLy8gdGhlIHN1ZmZpeCBmb3IgYSBwb3NpdGl2ZSBudW1iZXJcbiAgcG9zU3VmOiBzdHJpbmc7XG4gIC8vIHRoZSBwcmVmaXggZm9yIGEgbmVnYXRpdmUgbnVtYmVyIChlLmcuIGAtYCBvciBgKGApKVxuICBuZWdQcmU6IHN0cmluZztcbiAgLy8gdGhlIHN1ZmZpeCBmb3IgYSBuZWdhdGl2ZSBudW1iZXIgKGUuZy4gYClgKVxuICBuZWdTdWY6IHN0cmluZztcbiAgLy8gbnVtYmVyIG9mIGRpZ2l0cyBpbiBlYWNoIGdyb3VwIG9mIHNlcGFyYXRlZCBkaWdpdHNcbiAgZ1NpemU6IG51bWJlcjtcbiAgLy8gbnVtYmVyIG9mIGRpZ2l0cyBpbiB0aGUgbGFzdCBncm91cCBvZiBkaWdpdHMgYmVmb3JlIHRoZSBkZWNpbWFsIHNlcGFyYXRvclxuICBsZ1NpemU6IG51bWJlcjtcbn1cblxuZnVuY3Rpb24gcGFyc2VOdW1iZXJGb3JtYXQoZm9ybWF0OiBzdHJpbmcsIG1pbnVzU2lnbiA9ICctJyk6IFBhcnNlZE51bWJlckZvcm1hdCB7XG4gIGNvbnN0IHAgPSB7XG4gICAgbWluSW50OiAxLFxuICAgIG1pbkZyYWM6IDAsXG4gICAgbWF4RnJhYzogMCxcbiAgICBwb3NQcmU6ICcnLFxuICAgIHBvc1N1ZjogJycsXG4gICAgbmVnUHJlOiAnJyxcbiAgICBuZWdTdWY6ICcnLFxuICAgIGdTaXplOiAwLFxuICAgIGxnU2l6ZTogMFxuICB9O1xuXG4gIGNvbnN0IHBhdHRlcm5QYXJ0cyA9IGZvcm1hdC5zcGxpdChQQVRURVJOX1NFUCk7XG4gIGNvbnN0IHBvc2l0aXZlID0gcGF0dGVyblBhcnRzWzBdO1xuICBjb25zdCBuZWdhdGl2ZSA9IHBhdHRlcm5QYXJ0c1sxXTtcblxuICBjb25zdCBwb3NpdGl2ZVBhcnRzID0gcG9zaXRpdmUuaW5kZXhPZihERUNJTUFMX1NFUCkgIT09IC0xID9cbiAgICAgIHBvc2l0aXZlLnNwbGl0KERFQ0lNQUxfU0VQKSA6XG4gICAgICBbXG4gICAgICAgIHBvc2l0aXZlLnN1YnN0cmluZygwLCBwb3NpdGl2ZS5sYXN0SW5kZXhPZihaRVJPX0NIQVIpICsgMSksXG4gICAgICAgIHBvc2l0aXZlLnN1YnN0cmluZyhwb3NpdGl2ZS5sYXN0SW5kZXhPZihaRVJPX0NIQVIpICsgMSlcbiAgICAgIF0sXG4gICAgICAgIGludGVnZXIgPSBwb3NpdGl2ZVBhcnRzWzBdLCBmcmFjdGlvbiA9IHBvc2l0aXZlUGFydHNbMV0gfHwgJyc7XG5cbiAgcC5wb3NQcmUgPSBpbnRlZ2VyLnN1YnN0cigwLCBpbnRlZ2VyLmluZGV4T2YoRElHSVRfQ0hBUikpO1xuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgZnJhY3Rpb24ubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBjaCA9IGZyYWN0aW9uLmNoYXJBdChpKTtcbiAgICBpZiAoY2ggPT09IFpFUk9fQ0hBUikge1xuICAgICAgcC5taW5GcmFjID0gcC5tYXhGcmFjID0gaSArIDE7XG4gICAgfSBlbHNlIGlmIChjaCA9PT0gRElHSVRfQ0hBUikge1xuICAgICAgcC5tYXhGcmFjID0gaSArIDE7XG4gICAgfSBlbHNlIHtcbiAgICAgIHAucG9zU3VmICs9IGNoO1xuICAgIH1cbiAgfVxuXG4gIGNvbnN0IGdyb3VwcyA9IGludGVnZXIuc3BsaXQoR1JPVVBfU0VQKTtcbiAgcC5nU2l6ZSA9IGdyb3Vwc1sxXSA/IGdyb3Vwc1sxXS5sZW5ndGggOiAwO1xuICBwLmxnU2l6ZSA9IChncm91cHNbMl0gfHwgZ3JvdXBzWzFdKSA/IChncm91cHNbMl0gfHwgZ3JvdXBzWzFdKS5sZW5ndGggOiAwO1xuXG4gIGlmIChuZWdhdGl2ZSkge1xuICAgIGNvbnN0IHRydW5rTGVuID0gcG9zaXRpdmUubGVuZ3RoIC0gcC5wb3NQcmUubGVuZ3RoIC0gcC5wb3NTdWYubGVuZ3RoLFxuICAgICAgICAgIHBvcyA9IG5lZ2F0aXZlLmluZGV4T2YoRElHSVRfQ0hBUik7XG5cbiAgICBwLm5lZ1ByZSA9IG5lZ2F0aXZlLnN1YnN0cigwLCBwb3MpLnJlcGxhY2UoLycvZywgJycpO1xuICAgIHAubmVnU3VmID0gbmVnYXRpdmUuc3Vic3RyKHBvcyArIHRydW5rTGVuKS5yZXBsYWNlKC8nL2csICcnKTtcbiAgfSBlbHNlIHtcbiAgICBwLm5lZ1ByZSA9IG1pbnVzU2lnbiArIHAucG9zUHJlO1xuICAgIHAubmVnU3VmID0gcC5wb3NTdWY7XG4gIH1cblxuICByZXR1cm4gcDtcbn1cblxuaW50ZXJmYWNlIFBhcnNlZE51bWJlciB7XG4gIC8vIGFuIGFycmF5IG9mIGRpZ2l0cyBjb250YWluaW5nIGxlYWRpbmcgemVyb3MgYXMgbmVjZXNzYXJ5XG4gIGRpZ2l0czogbnVtYmVyW107XG4gIC8vIHRoZSBleHBvbmVudCBmb3IgbnVtYmVycyB0aGF0IHdvdWxkIG5lZWQgbW9yZSB0aGFuIGBNQVhfRElHSVRTYCBkaWdpdHMgaW4gYGRgXG4gIGV4cG9uZW50OiBudW1iZXI7XG4gIC8vIHRoZSBudW1iZXIgb2YgdGhlIGRpZ2l0cyBpbiBgZGAgdGhhdCBhcmUgdG8gdGhlIGxlZnQgb2YgdGhlIGRlY2ltYWwgcG9pbnRcbiAgaW50ZWdlckxlbjogbnVtYmVyO1xufVxuXG4vLyBUcmFuc2Zvcm1zIGEgcGFyc2VkIG51bWJlciBpbnRvIGEgcGVyY2VudGFnZSBieSBtdWx0aXBseWluZyBpdCBieSAxMDBcbmZ1bmN0aW9uIHRvUGVyY2VudChwYXJzZWROdW1iZXI6IFBhcnNlZE51bWJlcik6IFBhcnNlZE51bWJlciB7XG4gIC8vIGlmIHRoZSBudW1iZXIgaXMgMCwgZG9uJ3QgZG8gYW55dGhpbmdcbiAgaWYgKHBhcnNlZE51bWJlci5kaWdpdHNbMF0gPT09IDApIHtcbiAgICByZXR1cm4gcGFyc2VkTnVtYmVyO1xuICB9XG5cbiAgLy8gR2V0dGluZyB0aGUgY3VycmVudCBudW1iZXIgb2YgZGVjaW1hbHNcbiAgY29uc3QgZnJhY3Rpb25MZW4gPSBwYXJzZWROdW1iZXIuZGlnaXRzLmxlbmd0aCAtIHBhcnNlZE51bWJlci5pbnRlZ2VyTGVuO1xuICBpZiAocGFyc2VkTnVtYmVyLmV4cG9uZW50KSB7XG4gICAgcGFyc2VkTnVtYmVyLmV4cG9uZW50ICs9IDI7XG4gIH0gZWxzZSB7XG4gICAgaWYgKGZyYWN0aW9uTGVuID09PSAwKSB7XG4gICAgICBwYXJzZWROdW1iZXIuZGlnaXRzLnB1c2goMCwgMCk7XG4gICAgfSBlbHNlIGlmIChmcmFjdGlvbkxlbiA9PT0gMSkge1xuICAgICAgcGFyc2VkTnVtYmVyLmRpZ2l0cy5wdXNoKDApO1xuICAgIH1cbiAgICBwYXJzZWROdW1iZXIuaW50ZWdlckxlbiArPSAyO1xuICB9XG5cbiAgcmV0dXJuIHBhcnNlZE51bWJlcjtcbn1cblxuLyoqXG4gKiBQYXJzZXMgYSBudW1iZXIuXG4gKiBTaWduaWZpY2FudCBiaXRzIG9mIHRoaXMgcGFyc2UgYWxnb3JpdGhtIGNhbWUgZnJvbSBodHRwczovL2dpdGh1Yi5jb20vTWlrZU1jbC9iaWcuanMvXG4gKi9cbmZ1bmN0aW9uIHBhcnNlTnVtYmVyKG51bTogbnVtYmVyKTogUGFyc2VkTnVtYmVyIHtcbiAgbGV0IG51bVN0ciA9IE1hdGguYWJzKG51bSkgKyAnJztcbiAgbGV0IGV4cG9uZW50ID0gMCwgZGlnaXRzLCBpbnRlZ2VyTGVuO1xuICBsZXQgaSwgaiwgemVyb3M7XG5cbiAgLy8gRGVjaW1hbCBwb2ludD9cbiAgaWYgKChpbnRlZ2VyTGVuID0gbnVtU3RyLmluZGV4T2YoREVDSU1BTF9TRVApKSA+IC0xKSB7XG4gICAgbnVtU3RyID0gbnVtU3RyLnJlcGxhY2UoREVDSU1BTF9TRVAsICcnKTtcbiAgfVxuXG4gIC8vIEV4cG9uZW50aWFsIGZvcm0/XG4gIGlmICgoaSA9IG51bVN0ci5zZWFyY2goL2UvaSkpID4gMCkge1xuICAgIC8vIFdvcmsgb3V0IHRoZSBleHBvbmVudC5cbiAgICBpZiAoaW50ZWdlckxlbiA8IDApIGludGVnZXJMZW4gPSBpO1xuICAgIGludGVnZXJMZW4gKz0gK251bVN0ci5zbGljZShpICsgMSk7XG4gICAgbnVtU3RyID0gbnVtU3RyLnN1YnN0cmluZygwLCBpKTtcbiAgfSBlbHNlIGlmIChpbnRlZ2VyTGVuIDwgMCkge1xuICAgIC8vIFRoZXJlIHdhcyBubyBkZWNpbWFsIHBvaW50IG9yIGV4cG9uZW50IHNvIGl0IGlzIGFuIGludGVnZXIuXG4gICAgaW50ZWdlckxlbiA9IG51bVN0ci5sZW5ndGg7XG4gIH1cblxuICAvLyBDb3VudCB0aGUgbnVtYmVyIG9mIGxlYWRpbmcgemVyb3MuXG4gIGZvciAoaSA9IDA7IG51bVN0ci5jaGFyQXQoaSkgPT09IFpFUk9fQ0hBUjsgaSsrKSB7IC8qIGVtcHR5ICovXG4gIH1cblxuICBpZiAoaSA9PT0gKHplcm9zID0gbnVtU3RyLmxlbmd0aCkpIHtcbiAgICAvLyBUaGUgZGlnaXRzIGFyZSBhbGwgemVyby5cbiAgICBkaWdpdHMgPSBbMF07XG4gICAgaW50ZWdlckxlbiA9IDE7XG4gIH0gZWxzZSB7XG4gICAgLy8gQ291bnQgdGhlIG51bWJlciBvZiB0cmFpbGluZyB6ZXJvc1xuICAgIHplcm9zLS07XG4gICAgd2hpbGUgKG51bVN0ci5jaGFyQXQoemVyb3MpID09PSBaRVJPX0NIQVIpIHplcm9zLS07XG5cbiAgICAvLyBUcmFpbGluZyB6ZXJvcyBhcmUgaW5zaWduaWZpY2FudCBzbyBpZ25vcmUgdGhlbVxuICAgIGludGVnZXJMZW4gLT0gaTtcbiAgICBkaWdpdHMgPSBbXTtcbiAgICAvLyBDb252ZXJ0IHN0cmluZyB0byBhcnJheSBvZiBkaWdpdHMgd2l0aG91dCBsZWFkaW5nL3RyYWlsaW5nIHplcm9zLlxuICAgIGZvciAoaiA9IDA7IGkgPD0gemVyb3M7IGkrKywgaisrKSB7XG4gICAgICBkaWdpdHNbal0gPSBOdW1iZXIobnVtU3RyLmNoYXJBdChpKSk7XG4gICAgfVxuICB9XG5cbiAgLy8gSWYgdGhlIG51bWJlciBvdmVyZmxvd3MgdGhlIG1heGltdW0gYWxsb3dlZCBkaWdpdHMgdGhlbiB1c2UgYW4gZXhwb25lbnQuXG4gIGlmIChpbnRlZ2VyTGVuID4gTUFYX0RJR0lUUykge1xuICAgIGRpZ2l0cyA9IGRpZ2l0cy5zcGxpY2UoMCwgTUFYX0RJR0lUUyAtIDEpO1xuICAgIGV4cG9uZW50ID0gaW50ZWdlckxlbiAtIDE7XG4gICAgaW50ZWdlckxlbiA9IDE7XG4gIH1cblxuICByZXR1cm4ge2RpZ2l0cywgZXhwb25lbnQsIGludGVnZXJMZW59O1xufVxuXG4vKipcbiAqIFJvdW5kIHRoZSBwYXJzZWQgbnVtYmVyIHRvIHRoZSBzcGVjaWZpZWQgbnVtYmVyIG9mIGRlY2ltYWwgcGxhY2VzXG4gKiBUaGlzIGZ1bmN0aW9uIGNoYW5nZXMgdGhlIHBhcnNlZE51bWJlciBpbi1wbGFjZVxuICovXG5mdW5jdGlvbiByb3VuZE51bWJlcihwYXJzZWROdW1iZXI6IFBhcnNlZE51bWJlciwgbWluRnJhYzogbnVtYmVyLCBtYXhGcmFjOiBudW1iZXIpIHtcbiAgaWYgKG1pbkZyYWMgPiBtYXhGcmFjKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKFxuICAgICAgICBgVGhlIG1pbmltdW0gbnVtYmVyIG9mIGRpZ2l0cyBhZnRlciBmcmFjdGlvbiAoJHttaW5GcmFjfSkgaXMgaGlnaGVyIHRoYW4gdGhlIG1heGltdW0gKCR7bWF4RnJhY30pLmApO1xuICB9XG5cbiAgbGV0IGRpZ2l0cyA9IHBhcnNlZE51bWJlci5kaWdpdHM7XG4gIGxldCBmcmFjdGlvbkxlbiA9IGRpZ2l0cy5sZW5ndGggLSBwYXJzZWROdW1iZXIuaW50ZWdlckxlbjtcbiAgY29uc3QgZnJhY3Rpb25TaXplID0gTWF0aC5taW4oTWF0aC5tYXgobWluRnJhYywgZnJhY3Rpb25MZW4pLCBtYXhGcmFjKTtcblxuICAvLyBUaGUgaW5kZXggb2YgdGhlIGRpZ2l0IHRvIHdoZXJlIHJvdW5kaW5nIGlzIHRvIG9jY3VyXG4gIGxldCByb3VuZEF0ID0gZnJhY3Rpb25TaXplICsgcGFyc2VkTnVtYmVyLmludGVnZXJMZW47XG4gIGxldCBkaWdpdCA9IGRpZ2l0c1tyb3VuZEF0XTtcblxuICBpZiAocm91bmRBdCA+IDApIHtcbiAgICAvLyBEcm9wIGZyYWN0aW9uYWwgZGlnaXRzIGJleW9uZCBgcm91bmRBdGBcbiAgICBkaWdpdHMuc3BsaWNlKE1hdGgubWF4KHBhcnNlZE51bWJlci5pbnRlZ2VyTGVuLCByb3VuZEF0KSk7XG5cbiAgICAvLyBTZXQgbm9uLWZyYWN0aW9uYWwgZGlnaXRzIGJleW9uZCBgcm91bmRBdGAgdG8gMFxuICAgIGZvciAobGV0IGogPSByb3VuZEF0OyBqIDwgZGlnaXRzLmxlbmd0aDsgaisrKSB7XG4gICAgICBkaWdpdHNbal0gPSAwO1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICAvLyBXZSByb3VuZGVkIHRvIHplcm8gc28gcmVzZXQgdGhlIHBhcnNlZE51bWJlclxuICAgIGZyYWN0aW9uTGVuID0gTWF0aC5tYXgoMCwgZnJhY3Rpb25MZW4pO1xuICAgIHBhcnNlZE51bWJlci5pbnRlZ2VyTGVuID0gMTtcbiAgICBkaWdpdHMubGVuZ3RoID0gTWF0aC5tYXgoMSwgcm91bmRBdCA9IGZyYWN0aW9uU2l6ZSArIDEpO1xuICAgIGRpZ2l0c1swXSA9IDA7XG4gICAgZm9yIChsZXQgaSA9IDE7IGkgPCByb3VuZEF0OyBpKyspIGRpZ2l0c1tpXSA9IDA7XG4gIH1cblxuICBpZiAoZGlnaXQgPj0gNSkge1xuICAgIGlmIChyb3VuZEF0IC0gMSA8IDApIHtcbiAgICAgIGZvciAobGV0IGsgPSAwOyBrID4gcm91bmRBdDsgay0tKSB7XG4gICAgICAgIGRpZ2l0cy51bnNoaWZ0KDApO1xuICAgICAgICBwYXJzZWROdW1iZXIuaW50ZWdlckxlbisrO1xuICAgICAgfVxuICAgICAgZGlnaXRzLnVuc2hpZnQoMSk7XG4gICAgICBwYXJzZWROdW1iZXIuaW50ZWdlckxlbisrO1xuICAgIH0gZWxzZSB7XG4gICAgICBkaWdpdHNbcm91bmRBdCAtIDFdKys7XG4gICAgfVxuICB9XG5cbiAgLy8gUGFkIG91dCB3aXRoIHplcm9zIHRvIGdldCB0aGUgcmVxdWlyZWQgZnJhY3Rpb24gbGVuZ3RoXG4gIGZvciAoOyBmcmFjdGlvbkxlbiA8IE1hdGgubWF4KDAsIGZyYWN0aW9uU2l6ZSk7IGZyYWN0aW9uTGVuKyspIGRpZ2l0cy5wdXNoKDApO1xuXG4gIGxldCBkcm9wVHJhaWxpbmdaZXJvcyA9IGZyYWN0aW9uU2l6ZSAhPT0gMDtcbiAgLy8gTWluaW1hbCBsZW5ndGggPSBuYiBvZiBkZWNpbWFscyByZXF1aXJlZCArIGN1cnJlbnQgbmIgb2YgaW50ZWdlcnNcbiAgLy8gQW55IG51bWJlciBiZXNpZGVzIHRoYXQgaXMgb3B0aW9uYWwgYW5kIGNhbiBiZSByZW1vdmVkIGlmIGl0J3MgYSB0cmFpbGluZyAwXG4gIGNvbnN0IG1pbkxlbiA9IG1pbkZyYWMgKyBwYXJzZWROdW1iZXIuaW50ZWdlckxlbjtcbiAgLy8gRG8gYW55IGNhcnJ5aW5nLCBlLmcuIGEgZGlnaXQgd2FzIHJvdW5kZWQgdXAgdG8gMTBcbiAgY29uc3QgY2FycnkgPSBkaWdpdHMucmVkdWNlUmlnaHQoZnVuY3Rpb24oY2FycnksIGQsIGksIGRpZ2l0cykge1xuICAgIGQgPSBkICsgY2Fycnk7XG4gICAgZGlnaXRzW2ldID0gZCA8IDEwID8gZCA6IGQgLSAxMDsgIC8vIGQgJSAxMFxuICAgIGlmIChkcm9wVHJhaWxpbmdaZXJvcykge1xuICAgICAgLy8gRG8gbm90IGtlZXAgbWVhbmluZ2xlc3MgZnJhY3Rpb25hbCB0cmFpbGluZyB6ZXJvcyAoZS5nLiAxNS41MjAwMCAtLT4gMTUuNTIpXG4gICAgICBpZiAoZGlnaXRzW2ldID09PSAwICYmIGkgPj0gbWluTGVuKSB7XG4gICAgICAgIGRpZ2l0cy5wb3AoKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGRyb3BUcmFpbGluZ1plcm9zID0gZmFsc2U7XG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiBkID49IDEwID8gMSA6IDA7ICAvLyBNYXRoLmZsb29yKGQgLyAxMCk7XG4gIH0sIDApO1xuICBpZiAoY2FycnkpIHtcbiAgICBkaWdpdHMudW5zaGlmdChjYXJyeSk7XG4gICAgcGFyc2VkTnVtYmVyLmludGVnZXJMZW4rKztcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VJbnRBdXRvUmFkaXgodGV4dDogc3RyaW5nKTogbnVtYmVyIHtcbiAgY29uc3QgcmVzdWx0OiBudW1iZXIgPSBwYXJzZUludCh0ZXh0KTtcbiAgaWYgKGlzTmFOKHJlc3VsdCkpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ0ludmFsaWQgaW50ZWdlciBsaXRlcmFsIHdoZW4gcGFyc2luZyAnICsgdGV4dCk7XG4gIH1cbiAgcmV0dXJuIHJlc3VsdDtcbn1cbiJdfQ==