/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/**
 * This const is used to store the locale data registered with `registerLocaleData`
 */
export var LOCALE_DATA = {};
/**
 * Index of each type of locale data from the locale data array
 */
export var LocaleDataIndex;
(function (LocaleDataIndex) {
    LocaleDataIndex[LocaleDataIndex["LocaleId"] = 0] = "LocaleId";
    LocaleDataIndex[LocaleDataIndex["DayPeriodsFormat"] = 1] = "DayPeriodsFormat";
    LocaleDataIndex[LocaleDataIndex["DayPeriodsStandalone"] = 2] = "DayPeriodsStandalone";
    LocaleDataIndex[LocaleDataIndex["DaysFormat"] = 3] = "DaysFormat";
    LocaleDataIndex[LocaleDataIndex["DaysStandalone"] = 4] = "DaysStandalone";
    LocaleDataIndex[LocaleDataIndex["MonthsFormat"] = 5] = "MonthsFormat";
    LocaleDataIndex[LocaleDataIndex["MonthsStandalone"] = 6] = "MonthsStandalone";
    LocaleDataIndex[LocaleDataIndex["Eras"] = 7] = "Eras";
    LocaleDataIndex[LocaleDataIndex["FirstDayOfWeek"] = 8] = "FirstDayOfWeek";
    LocaleDataIndex[LocaleDataIndex["WeekendRange"] = 9] = "WeekendRange";
    LocaleDataIndex[LocaleDataIndex["DateFormat"] = 10] = "DateFormat";
    LocaleDataIndex[LocaleDataIndex["TimeFormat"] = 11] = "TimeFormat";
    LocaleDataIndex[LocaleDataIndex["DateTimeFormat"] = 12] = "DateTimeFormat";
    LocaleDataIndex[LocaleDataIndex["NumberSymbols"] = 13] = "NumberSymbols";
    LocaleDataIndex[LocaleDataIndex["NumberFormats"] = 14] = "NumberFormats";
    LocaleDataIndex[LocaleDataIndex["CurrencySymbol"] = 15] = "CurrencySymbol";
    LocaleDataIndex[LocaleDataIndex["CurrencyName"] = 16] = "CurrencyName";
    LocaleDataIndex[LocaleDataIndex["Currencies"] = 17] = "Currencies";
    LocaleDataIndex[LocaleDataIndex["PluralCase"] = 18] = "PluralCase";
    LocaleDataIndex[LocaleDataIndex["ExtraData"] = 19] = "ExtraData";
})(LocaleDataIndex || (LocaleDataIndex = {}));
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibG9jYWxlX2RhdGEuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb3JlL3NyYy9pMThuL2xvY2FsZV9kYXRhLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVIOztHQUVHO0FBQ0gsTUFBTSxDQUFDLElBQU0sV0FBVyxHQUE4QixFQUFFLENBQUM7QUFFekQ7O0dBRUc7QUFDSCxNQUFNLENBQU4sSUFBWSxlQXFCWDtBQXJCRCxXQUFZLGVBQWU7SUFDekIsNkRBQVksQ0FBQTtJQUNaLDZFQUFnQixDQUFBO0lBQ2hCLHFGQUFvQixDQUFBO0lBQ3BCLGlFQUFVLENBQUE7SUFDVix5RUFBYyxDQUFBO0lBQ2QscUVBQVksQ0FBQTtJQUNaLDZFQUFnQixDQUFBO0lBQ2hCLHFEQUFJLENBQUE7SUFDSix5RUFBYyxDQUFBO0lBQ2QscUVBQVksQ0FBQTtJQUNaLGtFQUFVLENBQUE7SUFDVixrRUFBVSxDQUFBO0lBQ1YsMEVBQWMsQ0FBQTtJQUNkLHdFQUFhLENBQUE7SUFDYix3RUFBYSxDQUFBO0lBQ2IsMEVBQWMsQ0FBQTtJQUNkLHNFQUFZLENBQUE7SUFDWixrRUFBVSxDQUFBO0lBQ1Ysa0VBQVUsQ0FBQTtJQUNWLGdFQUFTLENBQUE7QUFDWCxDQUFDLEVBckJXLGVBQWUsS0FBZixlQUFlLFFBcUIxQiIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLyoqXG4gKiBUaGlzIGNvbnN0IGlzIHVzZWQgdG8gc3RvcmUgdGhlIGxvY2FsZSBkYXRhIHJlZ2lzdGVyZWQgd2l0aCBgcmVnaXN0ZXJMb2NhbGVEYXRhYFxuICovXG5leHBvcnQgY29uc3QgTE9DQUxFX0RBVEE6IHtbbG9jYWxlSWQ6IHN0cmluZ106IGFueX0gPSB7fTtcblxuLyoqXG4gKiBJbmRleCBvZiBlYWNoIHR5cGUgb2YgbG9jYWxlIGRhdGEgZnJvbSB0aGUgbG9jYWxlIGRhdGEgYXJyYXlcbiAqL1xuZXhwb3J0IGVudW0gTG9jYWxlRGF0YUluZGV4IHtcbiAgTG9jYWxlSWQgPSAwLFxuICBEYXlQZXJpb2RzRm9ybWF0LFxuICBEYXlQZXJpb2RzU3RhbmRhbG9uZSxcbiAgRGF5c0Zvcm1hdCxcbiAgRGF5c1N0YW5kYWxvbmUsXG4gIE1vbnRoc0Zvcm1hdCxcbiAgTW9udGhzU3RhbmRhbG9uZSxcbiAgRXJhcyxcbiAgRmlyc3REYXlPZldlZWssXG4gIFdlZWtlbmRSYW5nZSxcbiAgRGF0ZUZvcm1hdCxcbiAgVGltZUZvcm1hdCxcbiAgRGF0ZVRpbWVGb3JtYXQsXG4gIE51bWJlclN5bWJvbHMsXG4gIE51bWJlckZvcm1hdHMsXG4gIEN1cnJlbmN5U3ltYm9sLFxuICBDdXJyZW5jeU5hbWUsXG4gIEN1cnJlbmNpZXMsXG4gIFBsdXJhbENhc2UsXG4gIEV4dHJhRGF0YVxufVxuIl19