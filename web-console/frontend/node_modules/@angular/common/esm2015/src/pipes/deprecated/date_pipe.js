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
import { Inject, LOCALE_ID, Pipe } from '@angular/core';
import { ISO8601_DATE_REGEX, isoStringToDate } from '../../i18n/format_date';
import { invalidPipeArgumentError } from '../invalid_pipe_argument_error';
import { DateFormatter } from './intl';
/**
 * \@ngModule CommonModule
 * \@description
 *
 * Formats a date according to locale rules.
 *
 * Where:
 * - `expression` is a date object or a number (milliseconds since UTC epoch) or an ISO string
 * (https://www.w3.org/TR/NOTE-datetime).
 * - `format` indicates which date/time components to include. The format can be predefined as
 *   shown below or custom as shown in the table.
 *   - `'medium'`: equivalent to `'yMMMdjms'` (e.g. `Sep 3, 2010, 12:05:08 PM` for `en-US`)
 *   - `'short'`: equivalent to `'yMdjm'` (e.g. `9/3/2010, 12:05 PM` for `en-US`)
 *   - `'fullDate'`: equivalent to `'yMMMMEEEEd'` (e.g. `Friday, September 3, 2010` for `en-US`)
 *   - `'longDate'`: equivalent to `'yMMMMd'` (e.g. `September 3, 2010` for `en-US`)
 *   - `'mediumDate'`: equivalent to `'yMMMd'` (e.g. `Sep 3, 2010` for `en-US`)
 *   - `'shortDate'`: equivalent to `'yMd'` (e.g. `9/3/2010` for `en-US`)
 *   - `'mediumTime'`: equivalent to `'jms'` (e.g. `12:05:08 PM` for `en-US`)
 *   - `'shortTime'`: equivalent to `'jm'` (e.g. `12:05 PM` for `en-US`)
 *
 *
 *  | Component | Symbol | Narrow | Short Form   | Long Form         | Numeric   | 2-digit   |
 *  |-----------|:------:|--------|--------------|-------------------|-----------|-----------|
 *  | era       |   G    | G (A)  | GGG (AD)     | GGGG (Anno Domini)| -         | -         |
 *  | year      |   y    | -      | -            | -                 | y (2015)  | yy (15)   |
 *  | month     |   M    | L (S)  | MMM (Sep)    | MMMM (September)  | M (9)     | MM (09)   |
 *  | day       |   d    | -      | -            | -                 | d (3)     | dd (03)   |
 *  | weekday   |   E    | E (S)  | EEE (Sun)    | EEEE (Sunday)     | -         | -         |
 *  | hour      |   j    | -      | -            | -                 | j (13)    | jj (13)   |
 *  | hour12    |   h    | -      | -            | -                 | h (1 PM)  | hh (01 PM)|
 *  | hour24    |   H    | -      | -            | -                 | H (13)    | HH (13)   |
 *  | minute    |   m    | -      | -            | -                 | m (5)     | mm (05)   |
 *  | second    |   s    | -      | -            | -                 | s (9)     | ss (09)   |
 *  | timezone  |   z    | -      | -            | z (Pacific Standard Time)| -  | -         |
 *  | timezone  |   Z    | -      | Z (GMT-8:00) | -                 | -         | -         |
 *  | timezone  |   a    | -      | a (PM)       | -                 | -         | -         |
 *
 * In javascript, only the components specified will be respected (not the ordering,
 * punctuations, ...) and details of the formatting will be dependent on the locale.
 *
 * Timezone of the formatted text will be the local system timezone of the end-user's machine.
 *
 * When the expression is a ISO string without time (e.g. 2016-09-19) the time zone offset is not
 * applied and the formatted text will have the same day, month and year of the expression.
 *
 * WARNINGS:
 * - this pipe is marked as pure hence it will not be re-evaluated when the input is mutated.
 *   Instead users should treat the date as an immutable object and change the reference when the
 *   pipe needs to re-run (this is to avoid reformatting the date on every change detection run
 *   which would be an expensive operation).
 * - this pipe uses the Internationalization API. Therefore it is only reliable in Chrome and Opera
 *   browsers.
 *
 * \@usageNotes
 *
 * ### Examples
 *
 * Assuming `dateObj` is (year: 2010, month: 9, day: 3, hour: 12 PM, minute: 05, second: 08)
 * in the _local_ time and locale is 'en-US':
 *
 * {\@example common/pipes/ts/date_pipe.ts region='DeprecatedDatePipe'}
 *
 * \@publicApi
 */
export class DeprecatedDatePipe {
    /**
     * @param {?} _locale
     */
    constructor(_locale) {
        this._locale = _locale;
    }
    /**
     * @param {?} value
     * @param {?=} pattern
     * @return {?}
     */
    transform(value, pattern = 'mediumDate') {
        if (value == null || value === '' || value !== value)
            return null;
        /** @type {?} */
        let date;
        if (typeof value === 'string') {
            value = value.trim();
        }
        if (isDate(value)) {
            date = value;
        }
        else if (!isNaN(value - parseFloat(value))) {
            date = new Date(parseFloat(value));
        }
        else if (typeof value === 'string' && /^(\d{4}-\d{1,2}-\d{1,2})$/.test(value)) {
            /**
             * For ISO Strings without time the day, month and year must be extracted from the ISO String
             * before Date creation to avoid time offset and errors in the new Date.
             * If we only replace '-' with ',' in the ISO String ("2015,01,01"), and try to create a new
             * date, some browsers (e.g. IE 9) will throw an invalid Date error
             * If we leave the '-' ("2015-01-01") and try to create a new Date("2015-01-01") the
             * timeoffset
             * is applied
             * Note: ISO months are 0 for January, 1 for February, ...
             */
            const [y, m, d] = value.split('-').map((/**
             * @param {?} val
             * @return {?}
             */
            (val) => parseInt(val, 10)));
            date = new Date(y, m - 1, d);
        }
        else {
            date = new Date(value);
        }
        if (!isDate(date)) {
            /** @type {?} */
            let match;
            if ((typeof value === 'string') && (match = value.match(ISO8601_DATE_REGEX))) {
                date = isoStringToDate(match);
            }
            else {
                throw invalidPipeArgumentError(DeprecatedDatePipe, value);
            }
        }
        return DateFormatter.format(date, this._locale, DeprecatedDatePipe._ALIASES[pattern] || pattern);
    }
}
/**
 * \@internal
 */
DeprecatedDatePipe._ALIASES = {
    'medium': 'yMMMdjms',
    'short': 'yMdjm',
    'fullDate': 'yMMMMEEEEd',
    'longDate': 'yMMMMd',
    'mediumDate': 'yMMMd',
    'shortDate': 'yMd',
    'mediumTime': 'jms',
    'shortTime': 'jm'
};
DeprecatedDatePipe.decorators = [
    { type: Pipe, args: [{ name: 'date', pure: true },] }
];
/** @nocollapse */
DeprecatedDatePipe.ctorParameters = () => [
    { type: String, decorators: [{ type: Inject, args: [LOCALE_ID,] }] }
];
if (false) {
    /**
     * \@internal
     * @type {?}
     */
    DeprecatedDatePipe._ALIASES;
    /**
     * @type {?}
     * @private
     */
    DeprecatedDatePipe.prototype._locale;
}
/**
 * @param {?} value
 * @return {?}
 */
function isDate(value) {
    return value instanceof Date && !isNaN(value.valueOf());
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGF0ZV9waXBlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL3NyYy9waXBlcy9kZXByZWNhdGVkL2RhdGVfcGlwZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVFBLE9BQU8sRUFBQyxNQUFNLEVBQUUsU0FBUyxFQUFFLElBQUksRUFBZ0IsTUFBTSxlQUFlLENBQUM7QUFDckUsT0FBTyxFQUFDLGtCQUFrQixFQUFFLGVBQWUsRUFBQyxNQUFNLHdCQUF3QixDQUFDO0FBQzNFLE9BQU8sRUFBQyx3QkFBd0IsRUFBQyxNQUFNLGdDQUFnQyxDQUFDO0FBQ3hFLE9BQU8sRUFBQyxhQUFhLEVBQUMsTUFBTSxRQUFRLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBbUVyQyxNQUFNLE9BQU8sa0JBQWtCOzs7O0lBYTdCLFlBQXVDLE9BQWU7UUFBZixZQUFPLEdBQVAsT0FBTyxDQUFRO0lBQUcsQ0FBQzs7Ozs7O0lBRTFELFNBQVMsQ0FBQyxLQUFVLEVBQUUsVUFBa0IsWUFBWTtRQUNsRCxJQUFJLEtBQUssSUFBSSxJQUFJLElBQUksS0FBSyxLQUFLLEVBQUUsSUFBSSxLQUFLLEtBQUssS0FBSztZQUFFLE9BQU8sSUFBSSxDQUFDOztZQUU5RCxJQUFVO1FBRWQsSUFBSSxPQUFPLEtBQUssS0FBSyxRQUFRLEVBQUU7WUFDN0IsS0FBSyxHQUFHLEtBQUssQ0FBQyxJQUFJLEVBQUUsQ0FBQztTQUN0QjtRQUVELElBQUksTUFBTSxDQUFDLEtBQUssQ0FBQyxFQUFFO1lBQ2pCLElBQUksR0FBRyxLQUFLLENBQUM7U0FDZDthQUFNLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSyxHQUFHLFVBQVUsQ0FBQyxLQUFLLENBQUMsQ0FBQyxFQUFFO1lBQzVDLElBQUksR0FBRyxJQUFJLElBQUksQ0FBQyxVQUFVLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQztTQUNwQzthQUFNLElBQUksT0FBTyxLQUFLLEtBQUssUUFBUSxJQUFJLDJCQUEyQixDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsRUFBRTs7Ozs7Ozs7Ozs7a0JBV3pFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLENBQUMsR0FBRyxLQUFLLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDLEdBQUc7Ozs7WUFBQyxDQUFDLEdBQVcsRUFBRSxFQUFFLENBQUMsUUFBUSxDQUFDLEdBQUcsRUFBRSxFQUFFLENBQUMsRUFBQztZQUMxRSxJQUFJLEdBQUcsSUFBSSxJQUFJLENBQUMsQ0FBQyxFQUFFLENBQUMsR0FBRyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7U0FDOUI7YUFBTTtZQUNMLElBQUksR0FBRyxJQUFJLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztTQUN4QjtRQUVELElBQUksQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLEVBQUU7O2dCQUNiLEtBQTRCO1lBQ2hDLElBQUksQ0FBQyxPQUFPLEtBQUssS0FBSyxRQUFRLENBQUMsSUFBSSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUMsS0FBSyxDQUFDLGtCQUFrQixDQUFDLENBQUMsRUFBRTtnQkFDNUUsSUFBSSxHQUFHLGVBQWUsQ0FBQyxLQUFLLENBQUMsQ0FBQzthQUMvQjtpQkFBTTtnQkFDTCxNQUFNLHdCQUF3QixDQUFDLGtCQUFrQixFQUFFLEtBQUssQ0FBQyxDQUFDO2FBQzNEO1NBQ0Y7UUFFRCxPQUFPLGFBQWEsQ0FBQyxNQUFNLENBQ3ZCLElBQUksRUFBRSxJQUFJLENBQUMsT0FBTyxFQUFFLGtCQUFrQixDQUFDLFFBQVEsQ0FBQyxPQUFPLENBQUMsSUFBSSxPQUFPLENBQUMsQ0FBQztJQUMzRSxDQUFDOzs7OztBQXRETSwyQkFBUSxHQUE0QjtJQUN6QyxRQUFRLEVBQUUsVUFBVTtJQUNwQixPQUFPLEVBQUUsT0FBTztJQUNoQixVQUFVLEVBQUUsWUFBWTtJQUN4QixVQUFVLEVBQUUsUUFBUTtJQUNwQixZQUFZLEVBQUUsT0FBTztJQUNyQixXQUFXLEVBQUUsS0FBSztJQUNsQixZQUFZLEVBQUUsS0FBSztJQUNuQixXQUFXLEVBQUUsSUFBSTtDQUNsQixDQUFDOztZQVpILElBQUksU0FBQyxFQUFDLElBQUksRUFBRSxNQUFNLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBQzs7Ozt5Q0FjakIsTUFBTSxTQUFDLFNBQVM7Ozs7Ozs7SUFYN0IsNEJBU0U7Ozs7O0lBRVUscUNBQTBDOzs7Ozs7QUE4Q3hELFNBQVMsTUFBTSxDQUFDLEtBQVU7SUFDeEIsT0FBTyxLQUFLLFlBQVksSUFBSSxJQUFJLENBQUMsS0FBSyxDQUFDLEtBQUssQ0FBQyxPQUFPLEVBQUUsQ0FBQyxDQUFDO0FBQzFELENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiogQGxpY2Vuc2VcbiogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4qXG4qIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4qIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAgKi9cblxuaW1wb3J0IHtJbmplY3QsIExPQ0FMRV9JRCwgUGlwZSwgUGlwZVRyYW5zZm9ybX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQge0lTTzg2MDFfREFURV9SRUdFWCwgaXNvU3RyaW5nVG9EYXRlfSBmcm9tICcuLi8uLi9pMThuL2Zvcm1hdF9kYXRlJztcbmltcG9ydCB7aW52YWxpZFBpcGVBcmd1bWVudEVycm9yfSBmcm9tICcuLi9pbnZhbGlkX3BpcGVfYXJndW1lbnRfZXJyb3InO1xuaW1wb3J0IHtEYXRlRm9ybWF0dGVyfSBmcm9tICcuL2ludGwnO1xuXG4vKipcbiAqIEBuZ01vZHVsZSBDb21tb25Nb2R1bGVcbiAqIEBkZXNjcmlwdGlvblxuICpcbiAqIEZvcm1hdHMgYSBkYXRlIGFjY29yZGluZyB0byBsb2NhbGUgcnVsZXMuXG4gKlxuICogV2hlcmU6XG4gKiAtIGBleHByZXNzaW9uYCBpcyBhIGRhdGUgb2JqZWN0IG9yIGEgbnVtYmVyIChtaWxsaXNlY29uZHMgc2luY2UgVVRDIGVwb2NoKSBvciBhbiBJU08gc3RyaW5nXG4gKiAoaHR0cHM6Ly93d3cudzMub3JnL1RSL05PVEUtZGF0ZXRpbWUpLlxuICogLSBgZm9ybWF0YCBpbmRpY2F0ZXMgd2hpY2ggZGF0ZS90aW1lIGNvbXBvbmVudHMgdG8gaW5jbHVkZS4gVGhlIGZvcm1hdCBjYW4gYmUgcHJlZGVmaW5lZCBhc1xuICogICBzaG93biBiZWxvdyBvciBjdXN0b20gYXMgc2hvd24gaW4gdGhlIHRhYmxlLlxuICogICAtIGAnbWVkaXVtJ2A6IGVxdWl2YWxlbnQgdG8gYCd5TU1NZGptcydgIChlLmcuIGBTZXAgMywgMjAxMCwgMTI6MDU6MDggUE1gIGZvciBgZW4tVVNgKVxuICogICAtIGAnc2hvcnQnYDogZXF1aXZhbGVudCB0byBgJ3lNZGptJ2AgKGUuZy4gYDkvMy8yMDEwLCAxMjowNSBQTWAgZm9yIGBlbi1VU2ApXG4gKiAgIC0gYCdmdWxsRGF0ZSdgOiBlcXVpdmFsZW50IHRvIGAneU1NTU1FRUVFZCdgIChlLmcuIGBGcmlkYXksIFNlcHRlbWJlciAzLCAyMDEwYCBmb3IgYGVuLVVTYClcbiAqICAgLSBgJ2xvbmdEYXRlJ2A6IGVxdWl2YWxlbnQgdG8gYCd5TU1NTWQnYCAoZS5nLiBgU2VwdGVtYmVyIDMsIDIwMTBgIGZvciBgZW4tVVNgKVxuICogICAtIGAnbWVkaXVtRGF0ZSdgOiBlcXVpdmFsZW50IHRvIGAneU1NTWQnYCAoZS5nLiBgU2VwIDMsIDIwMTBgIGZvciBgZW4tVVNgKVxuICogICAtIGAnc2hvcnREYXRlJ2A6IGVxdWl2YWxlbnQgdG8gYCd5TWQnYCAoZS5nLiBgOS8zLzIwMTBgIGZvciBgZW4tVVNgKVxuICogICAtIGAnbWVkaXVtVGltZSdgOiBlcXVpdmFsZW50IHRvIGAnam1zJ2AgKGUuZy4gYDEyOjA1OjA4IFBNYCBmb3IgYGVuLVVTYClcbiAqICAgLSBgJ3Nob3J0VGltZSdgOiBlcXVpdmFsZW50IHRvIGAnam0nYCAoZS5nLiBgMTI6MDUgUE1gIGZvciBgZW4tVVNgKVxuICpcbiAqXG4gKiAgfCBDb21wb25lbnQgfCBTeW1ib2wgfCBOYXJyb3cgfCBTaG9ydCBGb3JtICAgfCBMb25nIEZvcm0gICAgICAgICB8IE51bWVyaWMgICB8IDItZGlnaXQgICB8XG4gKiAgfC0tLS0tLS0tLS0tfDotLS0tLS06fC0tLS0tLS0tfC0tLS0tLS0tLS0tLS0tfC0tLS0tLS0tLS0tLS0tLS0tLS18LS0tLS0tLS0tLS18LS0tLS0tLS0tLS18XG4gKiAgfCBlcmEgICAgICAgfCAgIEcgICAgfCBHIChBKSAgfCBHR0cgKEFEKSAgICAgfCBHR0dHIChBbm5vIERvbWluaSl8IC0gICAgICAgICB8IC0gICAgICAgICB8XG4gKiAgfCB5ZWFyICAgICAgfCAgIHkgICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IHkgKDIwMTUpICB8IHl5ICgxNSkgICB8XG4gKiAgfCBtb250aCAgICAgfCAgIE0gICAgfCBMIChTKSAgfCBNTU0gKFNlcCkgICAgfCBNTU1NIChTZXB0ZW1iZXIpICB8IE0gKDkpICAgICB8IE1NICgwOSkgICB8XG4gKiAgfCBkYXkgICAgICAgfCAgIGQgICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IGQgKDMpICAgICB8IGRkICgwMykgICB8XG4gKiAgfCB3ZWVrZGF5ICAgfCAgIEUgICAgfCBFIChTKSAgfCBFRUUgKFN1bikgICAgfCBFRUVFIChTdW5kYXkpICAgICB8IC0gICAgICAgICB8IC0gICAgICAgICB8XG4gKiAgfCBob3VyICAgICAgfCAgIGogICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IGogKDEzKSAgICB8IGpqICgxMykgICB8XG4gKiAgfCBob3VyMTIgICAgfCAgIGggICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IGggKDEgUE0pICB8IGhoICgwMSBQTSl8XG4gKiAgfCBob3VyMjQgICAgfCAgIEggICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IEggKDEzKSAgICB8IEhIICgxMykgICB8XG4gKiAgfCBtaW51dGUgICAgfCAgIG0gICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IG0gKDUpICAgICB8IG1tICgwNSkgICB8XG4gKiAgfCBzZWNvbmQgICAgfCAgIHMgICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IHMgKDkpICAgICB8IHNzICgwOSkgICB8XG4gKiAgfCB0aW1lem9uZSAgfCAgIHogICAgfCAtICAgICAgfCAtICAgICAgICAgICAgfCB6IChQYWNpZmljIFN0YW5kYXJkIFRpbWUpfCAtICB8IC0gICAgICAgICB8XG4gKiAgfCB0aW1lem9uZSAgfCAgIFogICAgfCAtICAgICAgfCBaIChHTVQtODowMCkgfCAtICAgICAgICAgICAgICAgICB8IC0gICAgICAgICB8IC0gICAgICAgICB8XG4gKiAgfCB0aW1lem9uZSAgfCAgIGEgICAgfCAtICAgICAgfCBhIChQTSkgICAgICAgfCAtICAgICAgICAgICAgICAgICB8IC0gICAgICAgICB8IC0gICAgICAgICB8XG4gKlxuICogSW4gamF2YXNjcmlwdCwgb25seSB0aGUgY29tcG9uZW50cyBzcGVjaWZpZWQgd2lsbCBiZSByZXNwZWN0ZWQgKG5vdCB0aGUgb3JkZXJpbmcsXG4gKiBwdW5jdHVhdGlvbnMsIC4uLikgYW5kIGRldGFpbHMgb2YgdGhlIGZvcm1hdHRpbmcgd2lsbCBiZSBkZXBlbmRlbnQgb24gdGhlIGxvY2FsZS5cbiAqXG4gKiBUaW1lem9uZSBvZiB0aGUgZm9ybWF0dGVkIHRleHQgd2lsbCBiZSB0aGUgbG9jYWwgc3lzdGVtIHRpbWV6b25lIG9mIHRoZSBlbmQtdXNlcidzIG1hY2hpbmUuXG4gKlxuICogV2hlbiB0aGUgZXhwcmVzc2lvbiBpcyBhIElTTyBzdHJpbmcgd2l0aG91dCB0aW1lIChlLmcuIDIwMTYtMDktMTkpIHRoZSB0aW1lIHpvbmUgb2Zmc2V0IGlzIG5vdFxuICogYXBwbGllZCBhbmQgdGhlIGZvcm1hdHRlZCB0ZXh0IHdpbGwgaGF2ZSB0aGUgc2FtZSBkYXksIG1vbnRoIGFuZCB5ZWFyIG9mIHRoZSBleHByZXNzaW9uLlxuICpcbiAqIFdBUk5JTkdTOlxuICogLSB0aGlzIHBpcGUgaXMgbWFya2VkIGFzIHB1cmUgaGVuY2UgaXQgd2lsbCBub3QgYmUgcmUtZXZhbHVhdGVkIHdoZW4gdGhlIGlucHV0IGlzIG11dGF0ZWQuXG4gKiAgIEluc3RlYWQgdXNlcnMgc2hvdWxkIHRyZWF0IHRoZSBkYXRlIGFzIGFuIGltbXV0YWJsZSBvYmplY3QgYW5kIGNoYW5nZSB0aGUgcmVmZXJlbmNlIHdoZW4gdGhlXG4gKiAgIHBpcGUgbmVlZHMgdG8gcmUtcnVuICh0aGlzIGlzIHRvIGF2b2lkIHJlZm9ybWF0dGluZyB0aGUgZGF0ZSBvbiBldmVyeSBjaGFuZ2UgZGV0ZWN0aW9uIHJ1blxuICogICB3aGljaCB3b3VsZCBiZSBhbiBleHBlbnNpdmUgb3BlcmF0aW9uKS5cbiAqIC0gdGhpcyBwaXBlIHVzZXMgdGhlIEludGVybmF0aW9uYWxpemF0aW9uIEFQSS4gVGhlcmVmb3JlIGl0IGlzIG9ubHkgcmVsaWFibGUgaW4gQ2hyb21lIGFuZCBPcGVyYVxuICogICBicm93c2Vycy5cbiAqXG4gKiBAdXNhZ2VOb3Rlc1xuICpcbiAqICMjIyBFeGFtcGxlc1xuICpcbiAqIEFzc3VtaW5nIGBkYXRlT2JqYCBpcyAoeWVhcjogMjAxMCwgbW9udGg6IDksIGRheTogMywgaG91cjogMTIgUE0sIG1pbnV0ZTogMDUsIHNlY29uZDogMDgpXG4gKiBpbiB0aGUgX2xvY2FsXyB0aW1lIGFuZCBsb2NhbGUgaXMgJ2VuLVVTJzpcbiAqXG4gKiB7QGV4YW1wbGUgY29tbW9uL3BpcGVzL3RzL2RhdGVfcGlwZS50cyByZWdpb249J0RlcHJlY2F0ZWREYXRlUGlwZSd9XG4gKlxuICogQHB1YmxpY0FwaVxuICovXG5AUGlwZSh7bmFtZTogJ2RhdGUnLCBwdXJlOiB0cnVlfSlcbmV4cG9ydCBjbGFzcyBEZXByZWNhdGVkRGF0ZVBpcGUgaW1wbGVtZW50cyBQaXBlVHJhbnNmb3JtIHtcbiAgLyoqIEBpbnRlcm5hbCAqL1xuICBzdGF0aWMgX0FMSUFTRVM6IHtba2V5OiBzdHJpbmddOiBzdHJpbmd9ID0ge1xuICAgICdtZWRpdW0nOiAneU1NTWRqbXMnLFxuICAgICdzaG9ydCc6ICd5TWRqbScsXG4gICAgJ2Z1bGxEYXRlJzogJ3lNTU1NRUVFRWQnLFxuICAgICdsb25nRGF0ZSc6ICd5TU1NTWQnLFxuICAgICdtZWRpdW1EYXRlJzogJ3lNTU1kJyxcbiAgICAnc2hvcnREYXRlJzogJ3lNZCcsXG4gICAgJ21lZGl1bVRpbWUnOiAnam1zJyxcbiAgICAnc2hvcnRUaW1lJzogJ2ptJ1xuICB9O1xuXG4gIGNvbnN0cnVjdG9yKEBJbmplY3QoTE9DQUxFX0lEKSBwcml2YXRlIF9sb2NhbGU6IHN0cmluZykge31cblxuICB0cmFuc2Zvcm0odmFsdWU6IGFueSwgcGF0dGVybjogc3RyaW5nID0gJ21lZGl1bURhdGUnKTogc3RyaW5nfG51bGwge1xuICAgIGlmICh2YWx1ZSA9PSBudWxsIHx8IHZhbHVlID09PSAnJyB8fCB2YWx1ZSAhPT0gdmFsdWUpIHJldHVybiBudWxsO1xuXG4gICAgbGV0IGRhdGU6IERhdGU7XG5cbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSAnc3RyaW5nJykge1xuICAgICAgdmFsdWUgPSB2YWx1ZS50cmltKCk7XG4gICAgfVxuXG4gICAgaWYgKGlzRGF0ZSh2YWx1ZSkpIHtcbiAgICAgIGRhdGUgPSB2YWx1ZTtcbiAgICB9IGVsc2UgaWYgKCFpc05hTih2YWx1ZSAtIHBhcnNlRmxvYXQodmFsdWUpKSkge1xuICAgICAgZGF0ZSA9IG5ldyBEYXRlKHBhcnNlRmxvYXQodmFsdWUpKTtcbiAgICB9IGVsc2UgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gJ3N0cmluZycgJiYgL14oXFxkezR9LVxcZHsxLDJ9LVxcZHsxLDJ9KSQvLnRlc3QodmFsdWUpKSB7XG4gICAgICAvKipcbiAgICAgICAqIEZvciBJU08gU3RyaW5ncyB3aXRob3V0IHRpbWUgdGhlIGRheSwgbW9udGggYW5kIHllYXIgbXVzdCBiZSBleHRyYWN0ZWQgZnJvbSB0aGUgSVNPIFN0cmluZ1xuICAgICAgICogYmVmb3JlIERhdGUgY3JlYXRpb24gdG8gYXZvaWQgdGltZSBvZmZzZXQgYW5kIGVycm9ycyBpbiB0aGUgbmV3IERhdGUuXG4gICAgICAgKiBJZiB3ZSBvbmx5IHJlcGxhY2UgJy0nIHdpdGggJywnIGluIHRoZSBJU08gU3RyaW5nIChcIjIwMTUsMDEsMDFcIiksIGFuZCB0cnkgdG8gY3JlYXRlIGEgbmV3XG4gICAgICAgKiBkYXRlLCBzb21lIGJyb3dzZXJzIChlLmcuIElFIDkpIHdpbGwgdGhyb3cgYW4gaW52YWxpZCBEYXRlIGVycm9yXG4gICAgICAgKiBJZiB3ZSBsZWF2ZSB0aGUgJy0nIChcIjIwMTUtMDEtMDFcIikgYW5kIHRyeSB0byBjcmVhdGUgYSBuZXcgRGF0ZShcIjIwMTUtMDEtMDFcIikgdGhlXG4gICAgICAgKiB0aW1lb2Zmc2V0XG4gICAgICAgKiBpcyBhcHBsaWVkXG4gICAgICAgKiBOb3RlOiBJU08gbW9udGhzIGFyZSAwIGZvciBKYW51YXJ5LCAxIGZvciBGZWJydWFyeSwgLi4uXG4gICAgICAgKi9cbiAgICAgIGNvbnN0IFt5LCBtLCBkXSA9IHZhbHVlLnNwbGl0KCctJykubWFwKCh2YWw6IHN0cmluZykgPT4gcGFyc2VJbnQodmFsLCAxMCkpO1xuICAgICAgZGF0ZSA9IG5ldyBEYXRlKHksIG0gLSAxLCBkKTtcbiAgICB9IGVsc2Uge1xuICAgICAgZGF0ZSA9IG5ldyBEYXRlKHZhbHVlKTtcbiAgICB9XG5cbiAgICBpZiAoIWlzRGF0ZShkYXRlKSkge1xuICAgICAgbGV0IG1hdGNoOiBSZWdFeHBNYXRjaEFycmF5fG51bGw7XG4gICAgICBpZiAoKHR5cGVvZiB2YWx1ZSA9PT0gJ3N0cmluZycpICYmIChtYXRjaCA9IHZhbHVlLm1hdGNoKElTTzg2MDFfREFURV9SRUdFWCkpKSB7XG4gICAgICAgIGRhdGUgPSBpc29TdHJpbmdUb0RhdGUobWF0Y2gpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhyb3cgaW52YWxpZFBpcGVBcmd1bWVudEVycm9yKERlcHJlY2F0ZWREYXRlUGlwZSwgdmFsdWUpO1xuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiBEYXRlRm9ybWF0dGVyLmZvcm1hdChcbiAgICAgICAgZGF0ZSwgdGhpcy5fbG9jYWxlLCBEZXByZWNhdGVkRGF0ZVBpcGUuX0FMSUFTRVNbcGF0dGVybl0gfHwgcGF0dGVybik7XG4gIH1cbn1cblxuZnVuY3Rpb24gaXNEYXRlKHZhbHVlOiBhbnkpOiB2YWx1ZSBpcyBEYXRlIHtcbiAgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgRGF0ZSAmJiAhaXNOYU4odmFsdWUudmFsdWVPZigpKTtcbn1cbiJdfQ==