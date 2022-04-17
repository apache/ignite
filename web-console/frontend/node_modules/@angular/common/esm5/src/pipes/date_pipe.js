/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Inject, Injectable, LOCALE_ID, Pipe } from '@angular/core';
import { formatDate } from '../i18n/format_date';
import { invalidPipeArgumentError } from './invalid_pipe_argument_error';
// clang-format off
/**
 * @ngModule CommonModule
 * @description
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
 * @usageNotes
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
 * @Component({
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
 * @publicApi
 */
// clang-format on
var DatePipe = /** @class */ (function () {
    function DatePipe(locale) {
        this.locale = locale;
    }
    DatePipe_1 = DatePipe;
    /**
     * @param value The date expression: a `Date` object,  a number
     * (milliseconds since UTC epoch), or an ISO string (https://www.w3.org/TR/NOTE-datetime).
     * @param format The date/time components to include, using predefined options or a
     * custom format string.
     * @param timezone A timezone offset (such as `'+0430'`), or a standard
     * UTC/GMT or continental US timezone abbreviation.
     * When not supplied, uses the end-user's local system timezone.
     * @param locale A locale code for the locale format rules to use.
     * When not supplied, uses the value of `LOCALE_ID`, which is `en-US` by default.
     * See [Setting your app locale](guide/i18n#setting-up-the-locale-of-your-app).
     * @returns A date string in the desired format.
     */
    DatePipe.prototype.transform = function (value, format, timezone, locale) {
        if (format === void 0) { format = 'mediumDate'; }
        if (value == null || value === '' || value !== value)
            return null;
        try {
            return formatDate(value, format, locale || this.locale, timezone);
        }
        catch (error) {
            throw invalidPipeArgumentError(DatePipe_1, error.message);
        }
    };
    var DatePipe_1;
    DatePipe = DatePipe_1 = tslib_1.__decorate([
        Injectable(),
        Pipe({ name: 'date', pure: true }),
        tslib_1.__param(0, Inject(LOCALE_ID)),
        tslib_1.__metadata("design:paramtypes", [String])
    ], DatePipe);
    return DatePipe;
}());
export { DatePipe };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGF0ZV9waXBlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9waXBlcy9kYXRlX3BpcGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQyxNQUFNLEVBQUUsVUFBVSxFQUFFLFNBQVMsRUFBRSxJQUFJLEVBQWdCLE1BQU0sZUFBZSxDQUFDO0FBQ2pGLE9BQU8sRUFBQyxVQUFVLEVBQUMsTUFBTSxxQkFBcUIsQ0FBQztBQUMvQyxPQUFPLEVBQUMsd0JBQXdCLEVBQUMsTUFBTSwrQkFBK0IsQ0FBQztBQUV2RSxtQkFBbUI7QUFDbkI7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0dBeUlHO0FBQ0gsa0JBQWtCO0FBR2xCO0lBQ0Usa0JBQXVDLE1BQWM7UUFBZCxXQUFNLEdBQU4sTUFBTSxDQUFRO0lBQUcsQ0FBQztpQkFEOUMsUUFBUTtJQUduQjs7Ozs7Ozs7Ozs7O09BWUc7SUFDSCw0QkFBUyxHQUFULFVBQVUsS0FBVSxFQUFFLE1BQXFCLEVBQUUsUUFBaUIsRUFBRSxNQUFlO1FBQXpELHVCQUFBLEVBQUEscUJBQXFCO1FBQ3pDLElBQUksS0FBSyxJQUFJLElBQUksSUFBSSxLQUFLLEtBQUssRUFBRSxJQUFJLEtBQUssS0FBSyxLQUFLO1lBQUUsT0FBTyxJQUFJLENBQUM7UUFFbEUsSUFBSTtZQUNGLE9BQU8sVUFBVSxDQUFDLEtBQUssRUFBRSxNQUFNLEVBQUUsTUFBTSxJQUFJLElBQUksQ0FBQyxNQUFNLEVBQUUsUUFBUSxDQUFDLENBQUM7U0FDbkU7UUFBQyxPQUFPLEtBQUssRUFBRTtZQUNkLE1BQU0sd0JBQXdCLENBQUMsVUFBUSxFQUFFLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQztTQUN6RDtJQUNILENBQUM7O0lBeEJVLFFBQVE7UUFGcEIsVUFBVSxFQUFFO1FBQ1osSUFBSSxDQUFDLEVBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFDLENBQUM7UUFFbEIsbUJBQUEsTUFBTSxDQUFDLFNBQVMsQ0FBQyxDQUFBOztPQURuQixRQUFRLENBeUJwQjtJQUFELGVBQUM7Q0FBQSxBQXpCRCxJQXlCQztTQXpCWSxRQUFRIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0luamVjdCwgSW5qZWN0YWJsZSwgTE9DQUxFX0lELCBQaXBlLCBQaXBlVHJhbnNmb3JtfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7Zm9ybWF0RGF0ZX0gZnJvbSAnLi4vaTE4bi9mb3JtYXRfZGF0ZSc7XG5pbXBvcnQge2ludmFsaWRQaXBlQXJndW1lbnRFcnJvcn0gZnJvbSAnLi9pbnZhbGlkX3BpcGVfYXJndW1lbnRfZXJyb3InO1xuXG4vLyBjbGFuZy1mb3JtYXQgb2ZmXG4vKipcbiAqIEBuZ01vZHVsZSBDb21tb25Nb2R1bGVcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIEZvcm1hdHMgYSBkYXRlIHZhbHVlIGFjY29yZGluZyB0byBsb2NhbGUgcnVsZXMuXG4gKlxuICogT25seSB0aGUgYGVuLVVTYCBsb2NhbGUgZGF0YSBjb21lcyB3aXRoIEFuZ3VsYXIuIFRvIGxvY2FsaXplIGRhdGVzXG4gKiBpbiBhbm90aGVyIGxhbmd1YWdlLCB5b3UgbXVzdCBpbXBvcnQgdGhlIGNvcnJlc3BvbmRpbmcgbG9jYWxlIGRhdGEuXG4gKiBTZWUgdGhlIFtJMThuIGd1aWRlXShndWlkZS9pMThuI2kxOG4tcGlwZXMpIGZvciBtb3JlIGluZm9ybWF0aW9uLlxuICpcbiAqIEBzZWUgYGZvcm1hdERhdGUoKWBcbiAqXG4gKlxuICogQHVzYWdlTm90ZXNcbiAqXG4gKiBUaGUgcmVzdWx0IG9mIHRoaXMgcGlwZSBpcyBub3QgcmVldmFsdWF0ZWQgd2hlbiB0aGUgaW5wdXQgaXMgbXV0YXRlZC4gVG8gYXZvaWQgdGhlIG5lZWQgdG9cbiAqIHJlZm9ybWF0IHRoZSBkYXRlIG9uIGV2ZXJ5IGNoYW5nZS1kZXRlY3Rpb24gY3ljbGUsIHRyZWF0IHRoZSBkYXRlIGFzIGFuIGltbXV0YWJsZSBvYmplY3RcbiAqIGFuZCBjaGFuZ2UgdGhlIHJlZmVyZW5jZSB3aGVuIHRoZSBwaXBlIG5lZWRzIHRvIHJ1biBhZ2Fpbi5cbiAqXG4gKiAjIyMgUHJlLWRlZmluZWQgZm9ybWF0IG9wdGlvbnNcbiAqXG4gKiBFeGFtcGxlcyBhcmUgZ2l2ZW4gaW4gYGVuLVVTYCBsb2NhbGUuXG4gKlxuICogLSBgJ3Nob3J0J2A6IGVxdWl2YWxlbnQgdG8gYCdNL2QveXksIGg6bW0gYSdgIChgNi8xNS8xNSwgOTowMyBBTWApLlxuICogLSBgJ21lZGl1bSdgOiBlcXVpdmFsZW50IHRvIGAnTU1NIGQsIHksIGg6bW06c3MgYSdgIChgSnVuIDE1LCAyMDE1LCA5OjAzOjAxIEFNYCkuXG4gKiAtIGAnbG9uZydgOiBlcXVpdmFsZW50IHRvIGAnTU1NTSBkLCB5LCBoOm1tOnNzIGEgeidgIChgSnVuZSAxNSwgMjAxNSBhdCA5OjAzOjAxIEFNXG4gKiBHTVQrMWApLlxuICogLSBgJ2Z1bGwnYDogZXF1aXZhbGVudCB0byBgJ0VFRUUsIE1NTU0gZCwgeSwgaDptbTpzcyBhIHp6enonYCAoYE1vbmRheSwgSnVuZSAxNSwgMjAxNSBhdFxuICogOTowMzowMSBBTSBHTVQrMDE6MDBgKS5cbiAqIC0gYCdzaG9ydERhdGUnYDogZXF1aXZhbGVudCB0byBgJ00vZC95eSdgIChgNi8xNS8xNWApLlxuICogLSBgJ21lZGl1bURhdGUnYDogZXF1aXZhbGVudCB0byBgJ01NTSBkLCB5J2AgKGBKdW4gMTUsIDIwMTVgKS5cbiAqIC0gYCdsb25nRGF0ZSdgOiBlcXVpdmFsZW50IHRvIGAnTU1NTSBkLCB5J2AgKGBKdW5lIDE1LCAyMDE1YCkuXG4gKiAtIGAnZnVsbERhdGUnYDogZXF1aXZhbGVudCB0byBgJ0VFRUUsIE1NTU0gZCwgeSdgIChgTW9uZGF5LCBKdW5lIDE1LCAyMDE1YCkuXG4gKiAtIGAnc2hvcnRUaW1lJ2A6IGVxdWl2YWxlbnQgdG8gYCdoOm1tIGEnYCAoYDk6MDMgQU1gKS5cbiAqIC0gYCdtZWRpdW1UaW1lJ2A6IGVxdWl2YWxlbnQgdG8gYCdoOm1tOnNzIGEnYCAoYDk6MDM6MDEgQU1gKS5cbiAqIC0gYCdsb25nVGltZSdgOiBlcXVpdmFsZW50IHRvIGAnaDptbTpzcyBhIHonYCAoYDk6MDM6MDEgQU0gR01UKzFgKS5cbiAqIC0gYCdmdWxsVGltZSdgOiBlcXVpdmFsZW50IHRvIGAnaDptbTpzcyBhIHp6enonYCAoYDk6MDM6MDEgQU0gR01UKzAxOjAwYCkuXG4gKlxuICogIyMjIEN1c3RvbSBmb3JtYXQgb3B0aW9uc1xuICpcbiAqIFlvdSBjYW4gY29uc3RydWN0IGEgZm9ybWF0IHN0cmluZyB1c2luZyBzeW1ib2xzIHRvIHNwZWNpZnkgdGhlIGNvbXBvbmVudHNcbiAqIG9mIGEgZGF0ZS10aW1lIHZhbHVlLCBhcyBkZXNjcmliZWQgaW4gdGhlIGZvbGxvd2luZyB0YWJsZS5cbiAqIEZvcm1hdCBkZXRhaWxzIGRlcGVuZCBvbiB0aGUgbG9jYWxlLlxuICogRmllbGRzIG1hcmtlZCB3aXRoICgqKSBhcmUgb25seSBhdmFpbGFibGUgaW4gdGhlIGV4dHJhIGRhdGEgc2V0IGZvciB0aGUgZ2l2ZW4gbG9jYWxlLlxuICpcbiAqICB8IEZpZWxkIHR5cGUgICAgICAgICB8IEZvcm1hdCAgICAgIHwgRGVzY3JpcHRpb24gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IEV4YW1wbGUgVmFsdWUgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwtLS0tLS0tLS0tLS0tLS0tLS0tLXwtLS0tLS0tLS0tLS0tfC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLXwtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS18XG4gKiAgfCBFcmEgICAgICAgICAgICAgICAgfCBHLCBHRyAmIEdHRyB8IEFiYnJldmlhdGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBBRCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IEdHR0cgICAgICAgIHwgV2lkZSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IEFubm8gRG9taW5pICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgR0dHR0cgICAgICAgfCBOYXJyb3cgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgQSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBZZWFyICAgICAgICAgICAgICAgfCB5ICAgICAgICAgICB8IE51bWVyaWM6IG1pbmltdW0gZGlnaXRzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAyLCAyMCwgMjAxLCAyMDE3LCAyMDE3MyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IHl5ICAgICAgICAgIHwgTnVtZXJpYzogMiBkaWdpdHMgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAyLCAyMCwgMDEsIDE3LCA3MyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgeXl5ICAgICAgICAgfCBOdW1lcmljOiAzIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDAyLCAwMjAsIDIwMSwgMjAxNywgMjAxNzMgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCB5eXl5ICAgICAgICB8IE51bWVyaWM6IDQgZGlnaXRzIG9yIG1vcmUgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgfCAwMDAyLCAwMDIwLCAwMjAxLCAyMDE3LCAyMDE3MyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IE1vbnRoICAgICAgICAgICAgICB8IE0gICAgICAgICAgIHwgTnVtZXJpYzogMSBkaWdpdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDksIDEyICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgTU0gICAgICAgICAgfCBOdW1lcmljOiAyIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDksIDEyICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBNTU0gICAgICAgICB8IEFiYnJldmlhdGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBTZXAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IE1NTU0gICAgICAgIHwgV2lkZSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IFNlcHRlbWJlciAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgTU1NTU0gICAgICAgfCBOYXJyb3cgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgUyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBNb250aCBzdGFuZGFsb25lICAgfCBMICAgICAgICAgICB8IE51bWVyaWM6IDEgZGlnaXQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCA5LCAxMiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IExMICAgICAgICAgIHwgTnVtZXJpYzogMiBkaWdpdHMgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDA5LCAxMiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgTExMICAgICAgICAgfCBBYmJyZXZpYXRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgU2VwICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBMTExMICAgICAgICB8IFdpZGUgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBTZXB0ZW1iZXIgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IExMTExMICAgICAgIHwgTmFycm93ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IFMgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgV2VlayBvZiB5ZWFyICAgICAgIHwgdyAgICAgICAgICAgfCBOdW1lcmljOiBtaW5pbXVtIGRpZ2l0cyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMS4uLiA1MyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCB3dyAgICAgICAgICB8IE51bWVyaWM6IDIgZGlnaXRzICsgemVybyBwYWRkZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwMS4uLiA1MyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFdlZWsgb2YgbW9udGggICAgICB8IFcgICAgICAgICAgIHwgTnVtZXJpYzogMSBkaWdpdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDEuLi4gNSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgRGF5IG9mIG1vbnRoICAgICAgIHwgZCAgICAgICAgICAgfCBOdW1lcmljOiBtaW5pbXVtIGRpZ2l0cyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBkZCAgICAgICAgICB8IE51bWVyaWM6IDIgZGlnaXRzICsgemVybyBwYWRkZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwMSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBXZWVrIGRheSAgICAgICAgICAgfCBFLCBFRSAmIEVFRSB8IEFiYnJldmlhdGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBUdWUgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IEVFRUUgICAgICAgIHwgV2lkZSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IFR1ZXNkYXkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgRUVFRUUgICAgICAgfCBOYXJyb3cgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgVCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBFRUVFRUUgICAgICB8IFNob3J0ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBUdSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFBlcmlvZCAgICAgICAgICAgICB8IGEsIGFhICYgYWFhIHwgQWJicmV2aWF0ZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IGFtL3BtIG9yIEFNL1BNICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgYWFhYSAgICAgICAgfCBXaWRlIChmYWxsYmFjayB0byBgYWAgd2hlbiBtaXNzaW5nKSAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgYW50ZSBtZXJpZGllbS9wb3N0IG1lcmlkaWVtICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBhYWFhYSAgICAgICB8IE5hcnJvdyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBhL3AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFBlcmlvZCogICAgICAgICAgICB8IEIsIEJCICYgQkJCIHwgQWJicmV2aWF0ZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IG1pZC4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgQkJCQiAgICAgICAgfCBXaWRlICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgYW0sIHBtLCBtaWRuaWdodCwgbm9vbiwgbW9ybmluZywgYWZ0ZXJub29uLCBldmVuaW5nLCBuaWdodCB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBCQkJCQiAgICAgICB8IE5hcnJvdyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBtZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFBlcmlvZCBzdGFuZGFsb25lKiB8IGIsIGJiICYgYmJiIHwgQWJicmV2aWF0ZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IG1pZC4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgYmJiYiAgICAgICAgfCBXaWRlICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgYW0sIHBtLCBtaWRuaWdodCwgbm9vbiwgbW9ybmluZywgYWZ0ZXJub29uLCBldmVuaW5nLCBuaWdodCB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBiYmJiYiAgICAgICB8IE5hcnJvdyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBtZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IEhvdXIgMS0xMiAgICAgICAgICB8IGggICAgICAgICAgIHwgTnVtZXJpYzogbWluaW11bSBkaWdpdHMgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDEsIDEyICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgaGggICAgICAgICAgfCBOdW1lcmljOiAyIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDEsIDEyICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBIb3VyIDAtMjMgICAgICAgICAgfCBIICAgICAgICAgICB8IE51bWVyaWM6IG1pbmltdW0gZGlnaXRzICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwLCAyMyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IEhIICAgICAgICAgIHwgTnVtZXJpYzogMiBkaWdpdHMgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAwLCAyMyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgTWludXRlICAgICAgICAgICAgIHwgbSAgICAgICAgICAgfCBOdW1lcmljOiBtaW5pbXVtIGRpZ2l0cyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgOCwgNTkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBtbSAgICAgICAgICB8IE51bWVyaWM6IDIgZGlnaXRzICsgemVybyBwYWRkZWQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwOCwgNTkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8IFNlY29uZCAgICAgICAgICAgICB8IHMgICAgICAgICAgIHwgTnVtZXJpYzogbWluaW11bSBkaWdpdHMgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAuLi4gNTkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgc3MgICAgICAgICAgfCBOdW1lcmljOiAyIGRpZ2l0cyArIHplcm8gcGFkZGVkICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgMDAuLi4gNTkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBGcmFjdGlvbmFsIHNlY29uZHMgfCBTICAgICAgICAgICB8IE51bWVyaWM6IDEgZGlnaXQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCAwLi4uIDkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IFNTICAgICAgICAgIHwgTnVtZXJpYzogMiBkaWdpdHMgKyB6ZXJvIHBhZGRlZCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8IDAwLi4uIDk5ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgU1NTICAgICAgICAgfCBOdW1lcmljOiAzIGRpZ2l0cyArIHplcm8gcGFkZGVkICg9IG1pbGxpc2Vjb25kcykgICAgICAgICAgICAgIHwgMDAwLi4uIDk5OSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCBab25lICAgICAgICAgICAgICAgfCB6LCB6eiAmIHp6eiB8IFNob3J0IHNwZWNpZmljIG5vbiBsb2NhdGlvbiBmb3JtYXQgKGZhbGxiYWNrIHRvIE8pICAgICAgICAgICAgfCBHTVQtOCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IHp6enogICAgICAgIHwgTG9uZyBzcGVjaWZpYyBub24gbG9jYXRpb24gZm9ybWF0IChmYWxsYmFjayB0byBPT09PKSAgICAgICAgICB8IEdNVC0wODowMCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgWiwgWlogJiBaWlogfCBJU084NjAxIGJhc2ljIGZvcm1hdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgLTA4MDAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBaWlpaICAgICAgICB8IExvbmcgbG9jYWxpemVkIEdNVCBmb3JtYXQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBHTVQtODowMCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqICB8ICAgICAgICAgICAgICAgICAgICB8IFpaWlpaICAgICAgIHwgSVNPODYwMSBleHRlbmRlZCBmb3JtYXQgKyBaIGluZGljYXRvciBmb3Igb2Zmc2V0IDAgKD0gWFhYWFgpICB8IC0wODowMCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfFxuICogIHwgICAgICAgICAgICAgICAgICAgIHwgTywgT08gJiBPT08gfCBTaG9ydCBsb2NhbGl6ZWQgR01UIGZvcm1hdCAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHwgR01ULTggICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB8XG4gKiAgfCAgICAgICAgICAgICAgICAgICAgfCBPT09PICAgICAgICB8IExvbmcgbG9jYWxpemVkIEdNVCBmb3JtYXQgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfCBHTVQtMDg6MDAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHxcbiAqXG4gKiBOb3RlIHRoYXQgdGltZXpvbmUgY29ycmVjdGlvbiBpcyBub3QgYXBwbGllZCB0byBhbiBJU08gc3RyaW5nIHRoYXQgaGFzIG5vIHRpbWUgY29tcG9uZW50LCBzdWNoIGFzIFwiMjAxNi0wOS0xOVwiXG4gKlxuICogIyMjIEZvcm1hdCBleGFtcGxlc1xuICpcbiAqIFRoZXNlIGV4YW1wbGVzIHRyYW5zZm9ybSBhIGRhdGUgaW50byB2YXJpb3VzIGZvcm1hdHMsXG4gKiBhc3N1bWluZyB0aGF0IGBkYXRlT2JqYCBpcyBhIEphdmFTY3JpcHQgYERhdGVgIG9iamVjdCBmb3JcbiAqIHllYXI6IDIwMTUsIG1vbnRoOiA2LCBkYXk6IDE1LCBob3VyOiAyMSwgbWludXRlOiA0Mywgc2Vjb25kOiAxMSxcbiAqIGdpdmVuIGluIHRoZSBsb2NhbCB0aW1lIGZvciB0aGUgYGVuLVVTYCBsb2NhbGUuXG4gKlxuICogYGBgXG4gKiB7eyBkYXRlT2JqIHwgZGF0ZSB9fSAgICAgICAgICAgICAgIC8vIG91dHB1dCBpcyAnSnVuIDE1LCAyMDE1J1xuICoge3sgZGF0ZU9iaiB8IGRhdGU6J21lZGl1bScgfX0gICAgICAvLyBvdXRwdXQgaXMgJ0p1biAxNSwgMjAxNSwgOTo0MzoxMSBQTSdcbiAqIHt7IGRhdGVPYmogfCBkYXRlOidzaG9ydFRpbWUnIH19ICAgLy8gb3V0cHV0IGlzICc5OjQzIFBNJ1xuICoge3sgZGF0ZU9iaiB8IGRhdGU6J21tOnNzJyB9fSAgICAgICAvLyBvdXRwdXQgaXMgJzQzOjExJ1xuICogYGBgXG4gKlxuICogIyMjIFVzYWdlIGV4YW1wbGVcbiAqXG4gKiBUaGUgZm9sbG93aW5nIGNvbXBvbmVudCB1c2VzIGEgZGF0ZSBwaXBlIHRvIGRpc3BsYXkgdGhlIGN1cnJlbnQgZGF0ZSBpbiBkaWZmZXJlbnQgZm9ybWF0cy5cbiAqXG4gKiBgYGBcbiAqIEBDb21wb25lbnQoe1xuICogIHNlbGVjdG9yOiAnZGF0ZS1waXBlJyxcbiAqICB0ZW1wbGF0ZTogYDxkaXY+XG4gKiAgICA8cD5Ub2RheSBpcyB7e3RvZGF5IHwgZGF0ZX19PC9wPlxuICogICAgPHA+T3IgaWYgeW91IHByZWZlciwge3t0b2RheSB8IGRhdGU6J2Z1bGxEYXRlJ319PC9wPlxuICogICAgPHA+VGhlIHRpbWUgaXMge3t0b2RheSB8IGRhdGU6J2g6bW0gYSB6J319PC9wPlxuICogIDwvZGl2PmBcbiAqIH0pXG4gKiAvLyBHZXQgdGhlIGN1cnJlbnQgZGF0ZSBhbmQgdGltZSBhcyBhIGRhdGUtdGltZSB2YWx1ZS5cbiAqIGV4cG9ydCBjbGFzcyBEYXRlUGlwZUNvbXBvbmVudCB7XG4gKiAgIHRvZGF5OiBudW1iZXIgPSBEYXRlLm5vdygpO1xuICogfVxuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICovXG4vLyBjbGFuZy1mb3JtYXQgb25cbkBJbmplY3RhYmxlKClcbkBQaXBlKHtuYW1lOiAnZGF0ZScsIHB1cmU6IHRydWV9KVxuZXhwb3J0IGNsYXNzIERhdGVQaXBlIGltcGxlbWVudHMgUGlwZVRyYW5zZm9ybSB7XG4gIGNvbnN0cnVjdG9yKEBJbmplY3QoTE9DQUxFX0lEKSBwcml2YXRlIGxvY2FsZTogc3RyaW5nKSB7fVxuXG4gIC8qKlxuICAgKiBAcGFyYW0gdmFsdWUgVGhlIGRhdGUgZXhwcmVzc2lvbjogYSBgRGF0ZWAgb2JqZWN0LCAgYSBudW1iZXJcbiAgICogKG1pbGxpc2Vjb25kcyBzaW5jZSBVVEMgZXBvY2gpLCBvciBhbiBJU08gc3RyaW5nIChodHRwczovL3d3dy53My5vcmcvVFIvTk9URS1kYXRldGltZSkuXG4gICAqIEBwYXJhbSBmb3JtYXQgVGhlIGRhdGUvdGltZSBjb21wb25lbnRzIHRvIGluY2x1ZGUsIHVzaW5nIHByZWRlZmluZWQgb3B0aW9ucyBvciBhXG4gICAqIGN1c3RvbSBmb3JtYXQgc3RyaW5nLlxuICAgKiBAcGFyYW0gdGltZXpvbmUgQSB0aW1lem9uZSBvZmZzZXQgKHN1Y2ggYXMgYCcrMDQzMCdgKSwgb3IgYSBzdGFuZGFyZFxuICAgKiBVVEMvR01UIG9yIGNvbnRpbmVudGFsIFVTIHRpbWV6b25lIGFiYnJldmlhdGlvbi5cbiAgICogV2hlbiBub3Qgc3VwcGxpZWQsIHVzZXMgdGhlIGVuZC11c2VyJ3MgbG9jYWwgc3lzdGVtIHRpbWV6b25lLlxuICAgKiBAcGFyYW0gbG9jYWxlIEEgbG9jYWxlIGNvZGUgZm9yIHRoZSBsb2NhbGUgZm9ybWF0IHJ1bGVzIHRvIHVzZS5cbiAgICogV2hlbiBub3Qgc3VwcGxpZWQsIHVzZXMgdGhlIHZhbHVlIG9mIGBMT0NBTEVfSURgLCB3aGljaCBpcyBgZW4tVVNgIGJ5IGRlZmF1bHQuXG4gICAqIFNlZSBbU2V0dGluZyB5b3VyIGFwcCBsb2NhbGVdKGd1aWRlL2kxOG4jc2V0dGluZy11cC10aGUtbG9jYWxlLW9mLXlvdXItYXBwKS5cbiAgICogQHJldHVybnMgQSBkYXRlIHN0cmluZyBpbiB0aGUgZGVzaXJlZCBmb3JtYXQuXG4gICAqL1xuICB0cmFuc2Zvcm0odmFsdWU6IGFueSwgZm9ybWF0ID0gJ21lZGl1bURhdGUnLCB0aW1lem9uZT86IHN0cmluZywgbG9jYWxlPzogc3RyaW5nKTogc3RyaW5nfG51bGwge1xuICAgIGlmICh2YWx1ZSA9PSBudWxsIHx8IHZhbHVlID09PSAnJyB8fCB2YWx1ZSAhPT0gdmFsdWUpIHJldHVybiBudWxsO1xuXG4gICAgdHJ5IHtcbiAgICAgIHJldHVybiBmb3JtYXREYXRlKHZhbHVlLCBmb3JtYXQsIGxvY2FsZSB8fCB0aGlzLmxvY2FsZSwgdGltZXpvbmUpO1xuICAgIH0gY2F0Y2ggKGVycm9yKSB7XG4gICAgICB0aHJvdyBpbnZhbGlkUGlwZUFyZ3VtZW50RXJyb3IoRGF0ZVBpcGUsIGVycm9yLm1lc3NhZ2UpO1xuICAgIH1cbiAgfVxufVxuIl19