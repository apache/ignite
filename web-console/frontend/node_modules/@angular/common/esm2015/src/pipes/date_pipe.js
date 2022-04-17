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
import { formatDate } from '../i18n/format_date';
import { invalidPipeArgumentError } from './invalid_pipe_argument_error';
// clang-format off
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Formats a date value according to locale rules.
 *
 * Only the `en-US` locale data comes with Angular. To localize dates
 * in another language, you must import the corresponding locale data.
 * See the [I18n guide](guide/i18n#i18n-pipes) for more information.
 *
 * @see `formatDate()`
 *
 *
 * \@usageNotes
 *
 * The result of this pipe is not reevaluated when the input is mutated. To avoid the need to
 * reformat the date on every change-detection cycle, treat the date as an immutable object
 * and change the reference when the pipe needs to run again.
 *
 * ### Pre-defined format options
 *
 * Examples are given in `en-US` locale.
 *
 * - `'short'`: equivalent to `'M/d/yy, h:mm a'` (`6/15/15, 9:03 AM`).
 * - `'medium'`: equivalent to `'MMM d, y, h:mm:ss a'` (`Jun 15, 2015, 9:03:01 AM`).
 * - `'long'`: equivalent to `'MMMM d, y, h:mm:ss a z'` (`June 15, 2015 at 9:03:01 AM
 * GMT+1`).
 * - `'full'`: equivalent to `'EEEE, MMMM d, y, h:mm:ss a zzzz'` (`Monday, June 15, 2015 at
 * 9:03:01 AM GMT+01:00`).
 * - `'shortDate'`: equivalent to `'M/d/yy'` (`6/15/15`).
 * - `'mediumDate'`: equivalent to `'MMM d, y'` (`Jun 15, 2015`).
 * - `'longDate'`: equivalent to `'MMMM d, y'` (`June 15, 2015`).
 * - `'fullDate'`: equivalent to `'EEEE, MMMM d, y'` (`Monday, June 15, 2015`).
 * - `'shortTime'`: equivalent to `'h:mm a'` (`9:03 AM`).
 * - `'mediumTime'`: equivalent to `'h:mm:ss a'` (`9:03:01 AM`).
 * - `'longTime'`: equivalent to `'h:mm:ss a z'` (`9:03:01 AM GMT+1`).
 * - `'fullTime'`: equivalent to `'h:mm:ss a zzzz'` (`9:03:01 AM GMT+01:00`).
 *
 * ### Custom format options
 *
 * You can construct a format string using symbols to specify the components
 * of a date-time value, as described in the following table.
 * Format details depend on the locale.
 * Fields marked with (*) are only available in the extra data set for the given locale.
 *
 *  | Field type         | Format      | Description                                                   | Example Value                                              |
 *  |--------------------|-------------|---------------------------------------------------------------|------------------------------------------------------------|
 *  | Era                | G, GG & GGG | Abbreviated                                                   | AD                                                         |
 *  |                    | GGGG        | Wide                                                          | Anno Domini                                                |
 *  |                    | GGGGG       | Narrow                                                        | A                                                          |
 *  | Year               | y           | Numeric: minimum digits                                       | 2, 20, 201, 2017, 20173                                    |
 *  |                    | yy          | Numeric: 2 digits + zero padded                               | 02, 20, 01, 17, 73                                         |
 *  |                    | yyy         | Numeric: 3 digits + zero padded                               | 002, 020, 201, 2017, 20173                                 |
 *  |                    | yyyy        | Numeric: 4 digits or more + zero padded                       | 0002, 0020, 0201, 2017, 20173                              |
 *  | Month              | M           | Numeric: 1 digit                                              | 9, 12                                                      |
 *  |                    | MM          | Numeric: 2 digits + zero padded                               | 09, 12                                                     |
 *  |                    | MMM         | Abbreviated                                                   | Sep                                                        |
 *  |                    | MMMM        | Wide                                                          | September                                                  |
 *  |                    | MMMMM       | Narrow                                                        | S                                                          |
 *  | Month standalone   | L           | Numeric: 1 digit                                              | 9, 12                                                      |
 *  |                    | LL          | Numeric: 2 digits + zero padded                               | 09, 12                                                     |
 *  |                    | LLL         | Abbreviated                                                   | Sep                                                        |
 *  |                    | LLLL        | Wide                                                          | September                                                  |
 *  |                    | LLLLL       | Narrow                                                        | S                                                          |
 *  | Week of year       | w           | Numeric: minimum digits                                       | 1... 53                                                    |
 *  |                    | ww          | Numeric: 2 digits + zero padded                               | 01... 53                                                   |
 *  | Week of month      | W           | Numeric: 1 digit                                              | 1... 5                                                     |
 *  | Day of month       | d           | Numeric: minimum digits                                       | 1                                                          |
 *  |                    | dd          | Numeric: 2 digits + zero padded                               | 01                                                          |
 *  | Week day           | E, EE & EEE | Abbreviated                                                   | Tue                                                        |
 *  |                    | EEEE        | Wide                                                          | Tuesday                                                    |
 *  |                    | EEEEE       | Narrow                                                        | T                                                          |
 *  |                    | EEEEEE      | Short                                                         | Tu                                                         |
 *  | Period             | a, aa & aaa | Abbreviated                                                   | am/pm or AM/PM                                             |
 *  |                    | aaaa        | Wide (fallback to `a` when missing)                           | ante meridiem/post meridiem                                |
 *  |                    | aaaaa       | Narrow                                                        | a/p                                                        |
 *  | Period*            | B, BB & BBB | Abbreviated                                                   | mid.                                                       |
 *  |                    | BBBB        | Wide                                                          | am, pm, midnight, noon, morning, afternoon, evening, night |
 *  |                    | BBBBB       | Narrow                                                        | md                                                         |
 *  | Period standalone* | b, bb & bbb | Abbreviated                                                   | mid.                                                       |
 *  |                    | bbbb        | Wide                                                          | am, pm, midnight, noon, morning, afternoon, evening, night |
 *  |                    | bbbbb       | Narrow                                                        | md                                                         |
 *  | Hour 1-12          | h           | Numeric: minimum digits                                       | 1, 12                                                      |
 *  |                    | hh          | Numeric: 2 digits + zero padded                               | 01, 12                                                     |
 *  | Hour 0-23          | H           | Numeric: minimum digits                                       | 0, 23                                                      |
 *  |                    | HH          | Numeric: 2 digits + zero padded                               | 00, 23                                                     |
 *  | Minute             | m           | Numeric: minimum digits                                       | 8, 59                                                      |
 *  |                    | mm          | Numeric: 2 digits + zero padded                               | 08, 59                                                     |
 *  | Second             | s           | Numeric: minimum digits                                       | 0... 59                                                    |
 *  |                    | ss          | Numeric: 2 digits + zero padded                               | 00... 59                                                   |
 *  | Fractional seconds | S           | Numeric: 1 digit                                              | 0... 9                                                     |
 *  |                    | SS          | Numeric: 2 digits + zero padded                               | 00... 99                                                   |
 *  |                    | SSS         | Numeric: 3 digits + zero padded (= milliseconds)              | 000... 999                                                 |
 *  | Zone               | z, zz & zzz | Short specific non location format (fallback to O)            | GMT-8                                                      |
 *  |                    | zzzz        | Long specific non location format (fallback to OOOO)          | GMT-08:00                                                  |
 *  |                    | Z, ZZ & ZZZ | ISO8601 basic format                                          | -0800                                                      |
 *  |                    | ZZZZ        | Long localized GMT format                                     | GMT-8:00                                                   |
 *  |                    | ZZZZZ       | ISO8601 extended format + Z indicator for offset 0 (= XXXXX)  | -08:00                                                     |
 *  |                    | O, OO & OOO | Short localized GMT format                                    | GMT-8                                                      |
 *  |                    | OOOO        | Long localized GMT format                                     | GMT-08:00                                                  |
 *
 * Note that timezone correction is not applied to an ISO string that has no time component, such as "2016-09-19"
 *
 * ### Format examples
 *
 * These examples transform a date into various formats,
 * assuming that `dateObj` is a JavaScript `Date` object for
 * year: 2015, month: 6, day: 15, hour: 21, minute: 43, second: 11,
 * given in the local time for the `en-US` locale.
 *
 * ```
 * {{ dateObj | date }}               // output is 'Jun 15, 2015'
 * {{ dateObj | date:'medium' }}      // output is 'Jun 15, 2015, 9:43:11 PM'
 * {{ dateObj | date:'shortTime' }}   // output is '9:43 PM'
 * {{ dateObj | date:'mm:ss' }}       // output is '43:11'
 * ```
 *
 * ### Usage example
 *
 * The following component uses a date pipe to display the current date in different formats.
 *
 * ```
 * \@Component({
 *  selector: 'date-pipe',
 *  template: `<div>
 *    <p>Today is {{today | date}}</p>
 *    <p>Or if you prefer, {{today | date:'fullDate'}}</p>
 *    <p>The time is {{today | date:'h:mm a z'}}</p>
 *  </div>`
 * })
 * // Get the current date and time as a date-time value.
 * export class DatePipeComponent {
 *   today: number = Date.now();
 * }
 * ```
 *
 * \@publicApi
 */
// clang-format on
export class DatePipe {
    /**
     * @param {?} locale
     */
    constructor(locale) {
        this.locale = locale;
    }
    /**
     * @param {?} value The date expression: a `Date` object,  a number
     * (milliseconds since UTC epoch), or an ISO string (https://www.w3.org/TR/NOTE-datetime).
     * @param {?=} format The date/time components to include, using predefined options or a
     * custom format string.
     * @param {?=} timezone A timezone offset (such as `'+0430'`), or a standard
     * UTC/GMT or continental US timezone abbreviation.
     * When not supplied, uses the end-user's local system timezone.
     * @param {?=} locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     * @return {?} A date string in the desired format.
     */
    transform(value, format = 'mediumDate', timezone, locale) {
        if (value == null || value === '' || value !== value)
            return null;
        try {
            return formatDate(value, format, locale || this.locale, timezone);
        }
        catch (error) {
            throw invalidPipeArgumentError(DatePipe, error.message);
        }
    }
}
DatePipe.decorators = [
    { type: Injectable },
    { type: Pipe, args: [{ name: 'date', pure: true },] }
];
/** @nocollapse */
DatePipe.ctorParameters = () => [
    { type: String, decorators: [{ type: Inject, args: [LOCALE_ID,] }] }
];
if (false) {
    /**
     * @type {?}
     * @private
     */
    DatePipe.prototype.locale;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGF0ZV9waXBlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9waXBlcy9kYXRlX3BpcGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFRQSxPQUFPLEVBQUMsTUFBTSxFQUFFLFVBQVUsRUFBRSxTQUFTLEVBQUUsSUFBSSxFQUFnQixNQUFNLGVBQWUsQ0FBQztBQUNqRixPQUFPLEVBQUMsVUFBVSxFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFDL0MsT0FBTyxFQUFDLHdCQUF3QixFQUFDLE1BQU0sK0JBQStCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQWdKdkUsTUFBTSxPQUFPLFFBQVE7Ozs7SUFDbkIsWUFBdUMsTUFBYztRQUFkLFdBQU0sR0FBTixNQUFNLENBQVE7SUFBRyxDQUFDOzs7Ozs7Ozs7Ozs7OztJQWV6RCxTQUFTLENBQUMsS0FBVSxFQUFFLE1BQU0sR0FBRyxZQUFZLEVBQUUsUUFBaUIsRUFBRSxNQUFlO1FBQzdFLElBQUksS0FBSyxJQUFJLElBQUksSUFBSSxLQUFLLEtBQUssRUFBRSxJQUFJLEtBQUssS0FBSyxLQUFLO1lBQUUsT0FBTyxJQUFJLENBQUM7UUFFbEUsSUFBSTtZQUNGLE9BQU8sVUFBVSxDQUFDLEtBQUssRUFBRSxNQUFNLEVBQUUsTUFBTSxJQUFJLElBQUksQ0FBQyxNQUFNLEVBQUUsUUFBUSxDQUFDLENBQUM7U0FDbkU7UUFBQyxPQUFPLEtBQUssRUFBRTtZQUNkLE1BQU0sd0JBQXdCLENBQUMsUUFBUSxFQUFFLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUN6RDtJQUNILENBQUM7OztZQTFCRixVQUFVO1lBQ1YsSUFBSSxTQUFDLEVBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFDOzs7O3lDQUVqQixNQUFNLFNBQUMsU0FBUzs7Ozs7OztJQUFqQiwwQkFBeUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7SW5qZWN0LCBJbmplY3RhYmxlLCBMT0NBTEVfSUQsIFBpcGUsIFBpcGVUcmFuc2Zvcm19IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHtmb3JtYXREYXRlfSBmcm9tICcuLi9pMThuL2Zvcm1hdF9kYXRlJztcbmltcG9ydCB7aW52YWxpZFBpcGVBcmd1bWVudEVycm9yfSBmcm9tICcuL2ludmFsaWRfcGlwZV9hcmd1bWVudF9lcnJvcic7XG5cbi8vIGNsYW5nLWZvcm1hdCBvZmZcbi8qKlxuICogQG5nTW9kdWxlIENvbW1vbk1vZHVsZVxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogRm9ybWF0cyBhIGRhdGUgdmFsdWUgYWNjb3JkaW5nIHRvIGxvY2FsZSBydWxlcy5cbiAqXG4gKiBPbmx5IHRoZSBgZW4tVVNgIGxvY2FsZSBkYXRhIGNvbWVzIHdpdGggQW5ndWxhci4gVG8gbG9jYWxpemUgZGF0ZXNcbiAqIGluIGFub3RoZXIgbGFuZ3VhZ2UsIHlvdSBtdXN0IGltcG9ydCB0aGUgY29ycmVzcG9uZGluZyBsb2NhbGUgZGF0YS5cbiAqIFNlZSB0aGUgW0kxOG4gZ3VpZGVdKGd1aWRlL2kxOG4jaTE4bi1waXBlcykgZm9yIG1vcmUgaW5mb3JtYXRpb24uXG4gKlxuICogQHNlZSBgZm9ybWF0RGF0ZSgpYFxuICpcbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICpcbiAqIFRoZSByZXN1bHQgb2YgdGhpcyBwaXBlIGlzIG5vdCByZWV2YWx1YXRlZCB3aGVuIHRoZSBpbnB1dCBpcyBtdXRhdGVkLiBUbyBhdm9pZCB0aGUgbmVlZCB0b1xuICogcmVmb3JtYXQgdGhlIGRhdGUgb24gZXZlcnkgY2hhbmdlLWRldGVjdGlvbiBjeWNsZSwgdHJlYXQgdGhlIGRhdGUgYXMgYW4gaW1tdXRhYmxlIG9iamVjdFxuICogYW5kIGNoYW5nZSB0aGUgcmVmZXJlbmNlIHdoZW4gdGhlIHBpcGUgbmVlZHMgdG8gcnVuIGFnYWluLlxuICpcbiAqICMjIyBQcmUtZGVmaW5lZCBmb3JtYXQgb3B0aW9uc1xuICpcbiAqIEV4YW1wbGVzIGFyZSBnaXZlbiBpbiBgZW4tVVNgIGxvY2FsZS5cbiAqXG4gKiAtIGAnc2hvcnQnYDogZXF1aXZhbGVudCB0byBgJ00vZC95eSwgaDptbSBhJ2AgKGA2LzE1LzE1LCA5OjAzIEFNYCkuXG4gKiAtIGAnbWVkaXVtJ2A6IGVxdWl2YWxlbnQgdG8gYCdNTU0gZCwgeSwgaDptbTpzcyBhJ2AgKGBKdW4gMTUsIDIwMTUsIDk6MDM6MDEgQU1gKS5cbiAqIC0gYCdsb25nJ2A6IGVxdWl2YWxlbnQgdG8gYCdNTU1NIGQsIHksIGg6bW06c3MgYSB6J2AgKGBKdW5lIDE1LCAyMDE1IGF0IDk6MDM6MDEgQU1cbiAqIEdNVCsxYCkuXG4gKiAtIGAnZnVsbCdgOiBlcXVpdmFsZW50IHRvIGAnRUVFRSwgTU1NTSBkLCB5LCBoOm1tOnNzIGEgenp6eidgIChgTW9uZGF5LCBKdW5lIDE1LCAyMDE1IGF0XG4gKiA5OjAzOjAxIEFNIEdNVCswMTowMGApLlxuICogLSBgJ3Nob3J0RGF0ZSdgOiBlcXVpdmFsZW50IHRvIGAnTS9kL3l5J2AgKGA2LzE1LzE1YCkuXG4gKiAtIGAnbWVkaXVtRGF0ZSdgOiBlcXVpdmFsZW50IHRvIGAnTU1NIGQsIHknYCAoYEp1biAxNSwgMjAxNWApLlxuICogLSBgJ2xvbmdEYXRlJ2A6IGVxdWl2YWxlbnQgdG8gYCdNTU1NIGQsIHknYCAoYEp1bmUgMTUsIDIwMTVgKS5cbiAqIC0gYCdmdWxsRGF0ZSdgOiBlcXVpdmFsZW50IHRvIGAnRUVFRSwgTU1NTSBkLCB5J2AgKGBNb25kYXksIEp1bmUgMTUsIDIwMTVgKS5cbiAqIC0gYCdzaG9ydFRpbWUnYDogZXF1aXZhbGVudCB0byBgJ2g6bW0gYSdgIChgOTowMyBBTWApLlxuICogLSBgJ21lZGl1bVRpbWUnYDogZXF1aXZhbGVudCB0byBgJ2g6bW06c3MgYSdgIChgOTowMzowMSBBTWApLlxuICogLSBgJ2xvbmdUaW1lJ2A6IGVxdWl2YWxlbnQgdG8gYCdoOm1tOnNzIGEgeidgIChgOTowMzowMSBBTSBHTVQrMWApLlxuICogLSBgJ2Z1bGxUaW1lJ2A6IGVxdWl2YWxlbnQgdG8gYCdoOm1tOnNzIGEgenp6eidgIChgOTowMzowMSBBTSBHTVQrMDE6MDBgKS5cbiAqXG4gKiAjIyMgQ3VzdG9tIGZvcm1hdCBvcHRpb25zXG4gKlxuICogWW91IGNhbiBjb25zdHJ1Y3QgYSBmb3JtYXQgc3RyaW5nIHVzaW5nIHN5bWJvbHMgdG8gc3BlY2lmeSB0aGUgY29tcG9uZW50c1xuICogb2YgYSBkYXRlLXRpbWUgdmFsdWUsIGFzIGRlc2NyaWJlZCBpbiB0aGUgZm9sbG93aW5nIHRhYmxlLlxuICogRm9ybWF0IGRldGFpbHMgZGVwZW5kIG9uIHRoZSBsb2NhbGUuXG4gKiBGaWVsZHMgbWFya2VkIHdpdGggKCopIGFyZSBvbmx5IGF2YWlsYWJsZSBpbiB0aGUgZXh0cmEgZGF0YSBzZXQgZm9yIHRoZSBnaXZlbiBsb2NhbGUuXG4gKlxuICogIHwgRmllbGQgdHlwZSAgICAgICAgIHwgRm9ybWF0ICAgICAgfCBEZXNjcmlwdGlvbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgRXhhbXBsZSBWYWx1ZSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfC0tLS0tLS0tLS0tLS0tLS0tLS0tfC0tLS0tLS0tLS0tLS18LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tfC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLXxcbiAqICB8IEVyYSAgICAgICAgICAgICAgICB8IEcsIEdHICYgR0dHIHwgQWJicmV2aWF0ZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IEFEICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgR0dHRyAgICAgICAgfCBXaWRlICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgQW5ubyBEb21pbmkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBHR0dHRyAgICAgICB8IE5hcnJvdyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBBICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFllYXIgICAgICAgICAgICAgICB8IHkgICAgICAgICAgIHwgTnVtZXJpYzogbWluaW11bSBkaWdpdHMgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDIsIDIwLCAyMDEsIDIwMTcsIDIwMTczICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgeXkgICAgICAgICAgfCBOdW1lcmljOiAyIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDIsIDIwLCAwMSwgMTcsIDczICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCB5eXkgICAgICAgICB8IE51bWVyaWM6IDMgZGlnaXRzICsgemVybyBwYWRkZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwMDIsIDAyMCwgMjAxLCAyMDE3LCAyMDE3MyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IHl5eXkgICAgICAgIHwgTnVtZXJpYzogNCBkaWdpdHMgb3IgbW9yZSArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICB8IDAwMDIsIDAwMjAsIDAyMDEsIDIwMTcsIDIwMTczICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgTW9udGggICAgICAgICAgICAgIHwgTSAgICAgICAgICAgfCBOdW1lcmljOiAxIGRpZ2l0ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgOSwgMTIgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBNTSAgICAgICAgICB8IE51bWVyaWM6IDIgZGlnaXRzICsgemVybyBwYWRkZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwOSwgMTIgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IE1NTSAgICAgICAgIHwgQWJicmV2aWF0ZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IFNlcCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgTU1NTSAgICAgICAgfCBXaWRlICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgU2VwdGVtYmVyICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBNTU1NTSAgICAgICB8IE5hcnJvdyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBTICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IE1vbnRoIHN0YW5kYWxvbmUgICB8IEwgICAgICAgICAgIHwgTnVtZXJpYzogMSBkaWdpdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDksIDEyICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgTEwgICAgICAgICAgfCBOdW1lcmljOiAyIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDksIDEyICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBMTEwgICAgICAgICB8IEFiYnJldmlhdGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBTZXAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IExMTEwgICAgICAgIHwgV2lkZSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IFNlcHRlbWJlciAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgTExMTEwgICAgICAgfCBOYXJyb3cgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgUyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBXZWVrIG9mIHllYXIgICAgICAgfCB3ICAgICAgICAgICB8IE51bWVyaWM6IG1pbmltdW0gZGlnaXRzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAxLi4uIDUzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IHd3ICAgICAgICAgIHwgTnVtZXJpYzogMiBkaWdpdHMgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAxLi4uIDUzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgV2VlayBvZiBtb250aCAgICAgIHwgVyAgICAgICAgICAgfCBOdW1lcmljOiAxIGRpZ2l0ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMS4uLiA1ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBEYXkgb2YgbW9udGggICAgICAgfCBkICAgICAgICAgICB8IE51bWVyaWM6IG1pbmltdW0gZGlnaXRzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAxICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IGRkICAgICAgICAgIHwgTnVtZXJpYzogMiBkaWdpdHMgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAxICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFdlZWsgZGF5ICAgICAgICAgICB8IEUsIEVFICYgRUVFIHwgQWJicmV2aWF0ZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IFR1ZSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgRUVFRSAgICAgICAgfCBXaWRlICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgVHVlc2RheSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBFRUVFRSAgICAgICB8IE5hcnJvdyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBUICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IEVFRUVFRSAgICAgIHwgU2hvcnQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IFR1ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgUGVyaW9kICAgICAgICAgICAgIHwgYSwgYWEgJiBhYWEgfCBBYmJyZXZpYXRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgYW0vcG0gb3IgQU0vUE0gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBhYWFhICAgICAgICB8IFdpZGUgKGZhbGxiYWNrIHRvIGBhYCB3aGVuIG1pc3NpbmcpICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBhbnRlIG1lcmlkaWVtL3Bvc3QgbWVyaWRpZW0gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IGFhYWFhICAgICAgIHwgTmFycm93ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IGEvcCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgUGVyaW9kKiAgICAgICAgICAgIHwgQiwgQkIgJiBCQkIgfCBBYmJyZXZpYXRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgbWlkLiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBCQkJCICAgICAgICB8IFdpZGUgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBhbSwgcG0sIG1pZG5pZ2h0LCBub29uLCBtb3JuaW5nLCBhZnRlcm5vb24sIGV2ZW5pbmcsIG5pZ2h0IHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IEJCQkJCICAgICAgIHwgTmFycm93ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IG1kICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgUGVyaW9kIHN0YW5kYWxvbmUqIHwgYiwgYmIgJiBiYmIgfCBBYmJyZXZpYXRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgbWlkLiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBiYmJiICAgICAgICB8IFdpZGUgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBhbSwgcG0sIG1pZG5pZ2h0LCBub29uLCBtb3JuaW5nLCBhZnRlcm5vb24sIGV2ZW5pbmcsIG5pZ2h0IHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IGJiYmJiICAgICAgIHwgTmFycm93ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IG1kICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgSG91ciAxLTEyICAgICAgICAgIHwgaCAgICAgICAgICAgfCBOdW1lcmljOiBtaW5pbXVtIGRpZ2l0cyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMSwgMTIgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBoaCAgICAgICAgICB8IE51bWVyaWM6IDIgZGlnaXRzICsgemVybyBwYWRkZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwMSwgMTIgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IEhvdXIgMC0yMyAgICAgICAgICB8IEggICAgICAgICAgIHwgTnVtZXJpYzogbWluaW11bSBkaWdpdHMgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAsIDIzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgSEggICAgICAgICAgfCBOdW1lcmljOiAyIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDAsIDIzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBNaW51dGUgICAgICAgICAgICAgfCBtICAgICAgICAgICB8IE51bWVyaWM6IG1pbmltdW0gZGlnaXRzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCA4LCA1OSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IG1tICAgICAgICAgIHwgTnVtZXJpYzogMiBkaWdpdHMgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDA4LCA1OSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgU2Vjb25kICAgICAgICAgICAgIHwgcyAgICAgICAgICAgfCBOdW1lcmljOiBtaW5pbXVtIGRpZ2l0cyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMC4uLiA1OSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBzcyAgICAgICAgICB8IE51bWVyaWM6IDIgZGlnaXRzICsgemVybyBwYWRkZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwMC4uLiA1OSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IEZyYWN0aW9uYWwgc2Vjb25kcyB8IFMgICAgICAgICAgIHwgTnVtZXJpYzogMSBkaWdpdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAuLi4gOSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgU1MgICAgICAgICAgfCBOdW1lcmljOiAyIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDAuLi4gOTkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBTU1MgICAgICAgICB8IE51bWVyaWM6IDMgZGlnaXRzICsgemVybyBwYWRkZWQgKD0gbWlsbGlzZWNvbmRzKSAgICAgICAgICAgICAgfCAwMDAuLi4gOTk5ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFpvbmUgICAgICAgICAgICAgICB8IHosIHp6ICYgenp6IHwgU2hvcnQgc3BlY2lmaWMgbm9uIGxvY2F0aW9uIGZvcm1hdCAoZmFsbGJhY2sgdG8gTykgICAgICAgICAgICB8IEdNVC04ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgenp6eiAgICAgICAgfCBMb25nIHNwZWNpZmljIG5vbiBsb2NhdGlvbiBmb3JtYXQgKGZhbGxiYWNrIHRvIE9PT08pICAgICAgICAgIHwgR01ULTA4OjAwICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBaLCBaWiAmIFpaWiB8IElTTzg2MDEgYmFzaWMgZm9ybWF0ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAtMDgwMCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IFpaWlogICAgICAgIHwgTG9uZyBsb2NhbGl6ZWQgR01UIGZvcm1hdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IEdNVC04OjAwICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgWlpaWlogICAgICAgfCBJU084NjAxIGV4dGVuZGVkIGZvcm1hdCArIFogaW5kaWNhdG9yIGZvciBvZmZzZXQgMCAoPSBYWFhYWCkgIHwgLTA4OjAwICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBPLCBPTyAmIE9PTyB8IFNob3J0IGxvY2FsaXplZCBHTVQgZm9ybWF0ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBHTVQtOCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IE9PT08gICAgICAgIHwgTG9uZyBsb2NhbGl6ZWQgR01UIGZvcm1hdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IEdNVC0wODowMCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICpcbiAqIE5vdGUgdGhhdCB0aW1lem9uZSBjb3JyZWN0aW9uIGlzIG5vdCBhcHBsaWVkIHRvIGFuIElTTyBzdHJpbmcgdGhhdCBoYXMgbm8gdGltZSBjb21wb25lbnQsIHN1Y2ggYXMgXCIyMDE2LTA5LTE5XCJcbiAqXG4gKiAjIyMgRm9ybWF0IGV4YW1wbGVzXG4gKlxuICogVGhlc2UgZXhhbXBsZXMgdHJhbnNmb3JtIGEgZGF0ZSBpbnRvIHZhcmlvdXMgZm9ybWF0cyxcbiAqIGFzc3VtaW5nIHRoYXQgYGRhdGVPYmpgIGlzIGEgSmF2YVNjcmlwdCBgRGF0ZWAgb2JqZWN0IGZvclxuICogeWVhcjogMjAxNSwgbW9udGg6IDYsIGRheTogMTUsIGhvdXI6IDIxLCBtaW51dGU6IDQzLCBzZWNvbmQ6IDExLFxuICogZ2l2ZW4gaW4gdGhlIGxvY2FsIHRpbWUgZm9yIHRoZSBgZW4tVVNgIGxvY2FsZS5cbiAqXG4gKiBgYGBcbiAqIHt7IGRhdGVPYmogfCBkYXRlIH19ICAgICAgICAgICAgICAgLy8gb3V0cHV0IGlzICdKdW4gMTUsIDIwMTUnXG4gKiB7eyBkYXRlT2JqIHwgZGF0ZTonbWVkaXVtJyB9fSAgICAgIC8vIG91dHB1dCBpcyAnSnVuIDE1LCAyMDE1LCA5OjQzOjExIFBNJ1xuICoge3sgZGF0ZU9iaiB8IGRhdGU6J3Nob3J0VGltZScgfX0gICAvLyBvdXRwdXQgaXMgJzk6NDMgUE0nXG4gKiB7eyBkYXRlT2JqIHwgZGF0ZTonbW06c3MnIH19ICAgICAgIC8vIG91dHB1dCBpcyAnNDM6MTEnXG4gKiBgYGBcbiAqXG4gKiAjIyMgVXNhZ2UgZXhhbXBsZVxuICpcbiAqIFRoZSBmb2xsb3dpbmcgY29tcG9uZW50IHVzZXMgYSBkYXRlIHBpcGUgdG8gZGlzcGxheSB0aGUgY3VycmVudCBkYXRlIGluIGRpZmZlcmVudCBmb3JtYXRzLlxuICpcbiAqIGBgYFxuICogQENvbXBvbmVudCh7XG4gKiAgc2VsZWN0b3I6ICdkYXRlLXBpcGUnLFxuICogIHRlbXBsYXRlOiBgPGRpdj5cbiAqICAgIDxwPlRvZGF5IGlzIHt7dG9kYXkgfCBkYXRlfX08L3A+XG4gKiAgICA8cD5PciBpZiB5b3UgcHJlZmVyLCB7e3RvZGF5IHwgZGF0ZTonZnVsbERhdGUnfX08L3A+XG4gKiAgICA8cD5UaGUgdGltZSBpcyB7e3RvZGF5IHwgZGF0ZTonaDptbSBhIHonfX08L3A+XG4gKiAgPC9kaXY+YFxuICogfSlcbiAqIC8vIEdldCB0aGUgY3VycmVudCBkYXRlIGFuZCB0aW1lIGFzIGEgZGF0ZS10aW1lIHZhbHVlLlxuICogZXhwb3J0IGNsYXNzIERhdGVQaXBlQ29tcG9uZW50IHtcbiAqICAgdG9kYXk6IG51bWJlciA9IERhdGUubm93KCk7XG4gKiB9XG4gKiBgYGBcbiAqXG4gKiBAcHVibGljQXBpXG4gKi9cbi8vIGNsYW5nLWZvcm1hdCBvblxuQEluamVjdGFibGUoKVxuQFBpcGUoe25hbWU6ICdkYXRlJywgcHVyZTogdHJ1ZX0pXG5leHBvcnQgY2xhc3MgRGF0ZVBpcGUgaW1wbGVtZW50cyBQaXBlVHJhbnNmb3JtIHtcbiAgY29uc3RydWN0b3IoQEluamVjdChMT0NBTEVfSUQpIHByaXZhdGUgbG9jYWxlOiBzdHJpbmcpIHt9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB2YWx1ZSBUaGUgZGF0ZSBleHByZXNzaW9uOiBhIGBEYXRlYCBvYmplY3QsICBhIG51bWJlclxuICAgKiAobWlsbGlzZWNvbmRzIHNpbmNlIFVUQyBlcG9jaCksIG9yIGFuIElTTyBzdHJpbmcgKGh0dHBzOi8vd3d3LnczLm9yZy9UUi9OT1RFLWRhdGV0aW1lKS5cbiAgICogQHBhcmFtIGZvcm1hdCBUaGUgZGF0ZS90aW1lIGNvbXBvbmVudHMgdG8gaW5jbHVkZSwgdXNpbmcgcHJlZGVmaW5lZCBvcHRpb25zIG9yIGFcbiAgICogY3VzdG9tIGZvcm1hdCBzdHJpbmcuXG4gICAqIEBwYXJhbSB0aW1lem9uZSBBIHRpbWV6b25lIG9mZnNldCAoc3VjaCBhcyBgJyswNDMwJ2ApLCBvciBhIHN0YW5kYXJkXG4gICAqIFVUQy9HTVQgb3IgY29udGluZW50YWwgVVMgdGltZXpvbmUgYWJicmV2aWF0aW9uLlxuICAgKiBXaGVuIG5vdCBzdXBwbGllZCwgdXNlcyB0aGUgZW5kLXVzZXIncyBsb2NhbCBzeXN0ZW0gdGltZXpvbmUuXG4gICAqIEBwYXJhbSBsb2NhbGUgQSBsb2NhbGUgY29kZSBmb3IgdGhlIGxvY2FsZSBmb3JtYXQgcnVsZXMgdG8gdXNlLlxuICAgKiBXaGVuIG5vdCBzdXBwbGllZCwgdXNlcyB0aGUgdmFsdWUgb2YgYExPQ0FMRV9JRGAsIHdoaWNoIGlzIGBlbi1VU2AgYnkgZGVmYXVsdC5cbiAgICogU2VlIFtTZXR0aW5nIHlvdXIgYXBwIGxvY2FsZV0oZ3VpZGUvaTE4biNzZXR0aW5nLXVwLXRoZS1sb2NhbGUtb2YteW91ci1hcHApLlxuICAgKiBAcmV0dXJucyBBIGRhdGUgc3RyaW5nIGluIHRoZSBkZXNpcmVkIGZvcm1hdC5cbiAgICovXG4gIHRyYW5zZm9ybSh2YWx1ZTogYW55LCBmb3JtYXQgPSAnbWVkaXVtRGF0ZScsIHRpbWV6b25lPzogc3RyaW5nLCBsb2NhbGU/OiBzdHJpbmcpOiBzdHJpbmd8bnVsbCB7XG4gICAgaWYgKHZhbHVlID09IG51bGwgfHwgdmFsdWUgPT09ICcnIHx8IHZhbHVlICE9PSB2YWx1ZSkgcmV0dXJuIG51bGw7XG5cbiAgICB0cnkge1xuICAgICAgcmV0dXJuIGZvcm1hdERhdGUodmFsdWUsIGZvcm1hdCwgbG9jYWxlIHx8IHRoaXMubG9jYWxlLCB0aW1lem9uZSk7XG4gICAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAgIHRocm93IGludmFsaWRQaXBlQXJndW1lbnRFcnJvcihEYXRlUGlwZSwgZXJyb3IubWVzc2FnZSk7XG4gICAgfVxuICB9XG59XG4iXX0=