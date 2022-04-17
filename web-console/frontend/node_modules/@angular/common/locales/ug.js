/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(null, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/common/locales/ug", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    // THIS CODE IS GENERATED - DO NOT MODIFY
    // See angular/tools/gulp-tasks/cldr/extract.js
    var u = undefined;
    function plural(n) {
        if (n === 1)
            return 1;
        return 5;
    }
    exports.default = [
        'ug', [['ب', 'ك'], ['چ.ب', 'چ.ك'], ['چۈشتىن بۇرۇن', 'چۈشتىن كېيىن']],
        [['چ.ب', 'چ.ك'], u, u],
        [
            ['ي', 'د', 'س', 'چ', 'پ', 'ج', 'ش'],
            ['يە', 'دۈ', 'سە', 'چا', 'پە', 'جۈ', 'شە'],
            [
                'يەكشەنبە', 'دۈشەنبە', 'سەيشەنبە', 'چارشەنبە',
                'پەيشەنبە', 'جۈمە', 'شەنبە'
            ],
            ['ي', 'د', 'س', 'چ', 'پ', 'ج', 'ش']
        ],
        u,
        [
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
            [
                'يانۋار', 'فېۋرال', 'مارت', 'ئاپرېل', 'ماي', 'ئىيۇن',
                'ئىيۇل', 'ئاۋغۇست', 'سېنتەبىر', 'ئۆكتەبىر', 'نويابىر',
                'دېكابىر'
            ],
            u
        ],
        u, [['BCE', 'مىلادىيە'], u, ['مىلادىيەدىن بۇرۇن', 'مىلادىيە']], 0,
        [6, 0], ['y-MM-dd', 'd-MMM، y', 'd-MMMM، y', 'y d-MMMM، EEEE'],
        ['h:mm a', 'h:mm:ss a', 'h:mm:ss a z', 'h:mm:ss a zzzz'], ['{1}، {0}', u, '{1} {0}', u],
        ['.', ',', ';', '%', '+', '-', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##0.###', '#,##0%', '¤#,##0.00', '#E0'], '￥', 'جۇڭگو يۈەنى',
        { 'CNY': ['￥', '¥'], 'JPY': ['JP¥', '¥'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidWcuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy91Zy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsSUFBSSxDQUFDLEtBQUssQ0FBQztZQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQ3RCLE9BQU8sQ0FBQyxDQUFDO0lBQ1gsQ0FBQztJQUVELGtCQUFlO1FBQ2IsSUFBSSxFQUFFLENBQUMsQ0FBQyxHQUFHLEVBQUUsR0FBRyxDQUFDLEVBQUUsQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDLEVBQUUsQ0FBQyxjQUFjLEVBQUUsY0FBYyxDQUFDLENBQUM7UUFDcEUsQ0FBQyxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQ3RCO1lBQ0UsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLENBQUM7WUFDbkMsQ0FBQyxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLENBQUM7WUFDMUM7Z0JBQ0UsVUFBVSxFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsVUFBVTtnQkFDN0MsVUFBVSxFQUFFLE1BQU0sRUFBRSxPQUFPO2FBQzVCO1lBQ0QsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLENBQUM7U0FDcEM7UUFDRCxDQUFDO1FBQ0Q7WUFDRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDO1lBQy9EO2dCQUNFLFFBQVEsRUFBRSxRQUFRLEVBQUUsTUFBTSxFQUFFLFFBQVEsRUFBRSxLQUFLLEVBQUUsT0FBTztnQkFDcEQsT0FBTyxFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsVUFBVSxFQUFFLFNBQVM7Z0JBQ3JELFNBQVM7YUFDVjtZQUNELENBQUM7U0FDRjtRQUNELENBQUMsRUFBRSxDQUFDLENBQUMsS0FBSyxFQUFFLFVBQVUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLG1CQUFtQixFQUFFLFVBQVUsQ0FBQyxDQUFDLEVBQUUsQ0FBQztRQUNqRSxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLFNBQVMsRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLGdCQUFnQixDQUFDO1FBQzlELENBQUMsUUFBUSxFQUFFLFdBQVcsRUFBRSxhQUFhLEVBQUUsZ0JBQWdCLENBQUMsRUFBRSxDQUFDLFVBQVUsRUFBRSxDQUFDLEVBQUUsU0FBUyxFQUFFLENBQUMsQ0FBQztRQUN2RixDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxLQUFLLEVBQUUsR0FBRyxDQUFDO1FBQzlELENBQUMsV0FBVyxFQUFFLFFBQVEsRUFBRSxXQUFXLEVBQUUsS0FBSyxDQUFDLEVBQUUsR0FBRyxFQUFFLGFBQWE7UUFDL0QsRUFBQyxLQUFLLEVBQUUsQ0FBQyxHQUFHLEVBQUUsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxFQUFDLEVBQUUsTUFBTTtLQUNqRCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vLyBUSElTIENPREUgSVMgR0VORVJBVEVEIC0gRE8gTk9UIE1PRElGWVxuLy8gU2VlIGFuZ3VsYXIvdG9vbHMvZ3VscC10YXNrcy9jbGRyL2V4dHJhY3QuanNcblxuY29uc3QgdSA9IHVuZGVmaW5lZDtcblxuZnVuY3Rpb24gcGx1cmFsKG46IG51bWJlcik6IG51bWJlciB7XG4gIGlmIChuID09PSAxKSByZXR1cm4gMTtcbiAgcmV0dXJuIDU7XG59XG5cbmV4cG9ydCBkZWZhdWx0IFtcbiAgJ3VnJywgW1sn2KgnLCAn2YMnXSwgWyfahi7YqCcsICfahi7ZgyddLCBbJ9qG24jYtNiq2YnZhiDYqNuH2LHbh9mGJywgJ9qG24jYtNiq2YnZhiDZg9uQ2YrZidmGJ11dLFxuICBbWyfahi7YqCcsICfahi7ZgyddLCB1LCB1XSxcbiAgW1xuICAgIFsn2YonLCAn2K8nLCAn2LMnLCAn2oYnLCAn2b4nLCAn2KwnLCAn2LQnXSxcbiAgICBbJ9mK25UnLCAn2K/biCcsICfYs9uVJywgJ9qG2KcnLCAn2b7blScsICfYrNuIJywgJ9i025UnXSxcbiAgICBbXG4gICAgICAn2YrbldmD2LTbldmG2KjblScsICfYr9uI2LTbldmG2KjblScsICfYs9uV2YrYtNuV2YbYqNuVJywgJ9qG2KfYsdi025XZhtio25UnLFxuICAgICAgJ9m+25XZiti025XZhtio25UnLCAn2KzbiNmF25UnLCAn2LTbldmG2KjblSdcbiAgICBdLFxuICAgIFsn2YonLCAn2K8nLCAn2LMnLCAn2oYnLCAn2b4nLCAn2KwnLCAn2LQnXVxuICBdLFxuICB1LFxuICBbXG4gICAgWycxJywgJzInLCAnMycsICc0JywgJzUnLCAnNicsICc3JywgJzgnLCAnOScsICcxMCcsICcxMScsICcxMiddLFxuICAgIFtcbiAgICAgICfZitin2Ybbi9in2LEnLCAn2YHbkNuL2LHYp9mEJywgJ9mF2KfYsdiqJywgJ9im2KfZvtix25DZhCcsICfZhdin2YonLCAn2KbZidmK24fZhicsXG4gICAgICAn2KbZidmK24fZhCcsICfYptin24vYutuH2LPYqicsICfYs9uQ2YbYqtuV2KjZidixJywgJ9im24bZg9iq25XYqNmJ2LEnLCAn2YbZiNmK2KfYqNmJ2LEnLFxuICAgICAgJ9iv25DZg9in2KjZidixJ1xuICAgIF0sXG4gICAgdVxuICBdLFxuICB1LCBbWydCQ0UnLCAn2YXZidmE2KfYr9mJ2YrblSddLCB1LCBbJ9mF2YnZhNin2K/ZidmK25XYr9mJ2YYg2Kjbh9ix24fZhicsICfZhdmJ2YTYp9iv2YnZituVJ11dLCAwLFxuICBbNiwgMF0sIFsneS1NTS1kZCcsICdkLU1NTdiMIHknLCAnZC1NTU1N2IwgeScsICd5IGQtTU1NTdiMIEVFRUUnXSxcbiAgWydoOm1tIGEnLCAnaDptbTpzcyBhJywgJ2g6bW06c3MgYSB6JywgJ2g6bW06c3MgYSB6enp6J10sIFsnezF92IwgezB9JywgdSwgJ3sxfSB7MH0nLCB1XSxcbiAgWycuJywgJywnLCAnOycsICclJywgJysnLCAnLScsICdFJywgJ8OXJywgJ+KAsCcsICfiiJ4nLCAnTmFOJywgJzonXSxcbiAgWycjLCMjMC4jIyMnLCAnIywjIzAlJywgJ8KkIywjIzAuMDAnLCAnI0UwJ10sICfvv6UnLCAn2Kzbh9qt2q/ZiCDZituI25XZhtmJJyxcbiAgeydDTlknOiBbJ++/pScsICfCpSddLCAnSlBZJzogWydKUMKlJywgJ8KlJ119LCBwbHVyYWxcbl07XG4iXX0=