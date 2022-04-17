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
        define("@angular/common/locales/rm", ["require", "exports"], factory);
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
        'rm', [['AM', 'PM'], u, u], u,
        [
            ['D', 'G', 'M', 'M', 'G', 'V', 'S'], ['du', 'gli', 'ma', 'me', 'gie', 've', 'so'],
            ['dumengia', 'glindesdi', 'mardi', 'mesemna', 'gievgia', 'venderdi', 'sonda'],
            ['du', 'gli', 'ma', 'me', 'gie', 've', 'so']
        ],
        u,
        [
            ['S', 'F', 'M', 'A', 'M', 'Z', 'F', 'A', 'S', 'O', 'N', 'D'],
            [
                'schan.', 'favr.', 'mars', 'avr.', 'matg', 'zercl.', 'fan.', 'avust', 'sett.', 'oct.', 'nov.',
                'dec.'
            ],
            [
                'schaner', 'favrer', 'mars', 'avrigl', 'matg', 'zercladur', 'fanadur', 'avust', 'settember',
                'october', 'november', 'december'
            ]
        ],
        u, [['av. Cr.', 's. Cr.'], u, ['avant Cristus', 'suenter Cristus']], 1, [6, 0],
        ['dd-MM-yy', 'dd-MM-y', 'd \'da\' MMMM y', 'EEEE, \'ils\' d \'da\' MMMM y'],
        ['HH:mm', 'HH:mm:ss', 'HH:mm:ss z', 'HH:mm:ss zzzz'], ['{1} {0}', u, u, u],
        ['.', '’', ';', '%', '+', '−', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##0.###', '#,##0 %', '#,##0.00 ¤', '#E0'], 'CHF', 'franc svizzer',
        { 'JPY': ['JP¥', '¥'], 'USD': ['US$', '$'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicm0uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy9ybS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsSUFBSSxDQUFDLEtBQUssQ0FBQztZQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQ3RCLE9BQU8sQ0FBQyxDQUFDO0lBQ1gsQ0FBQztJQUVELGtCQUFlO1FBQ2IsSUFBSSxFQUFFLENBQUMsQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUM7UUFDN0I7WUFDRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsQ0FBQyxFQUFFLENBQUMsSUFBSSxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDO1lBQ2pGLENBQUMsVUFBVSxFQUFFLFdBQVcsRUFBRSxPQUFPLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsT0FBTyxDQUFDO1lBQzdFLENBQUMsSUFBSSxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDO1NBQzdDO1FBQ0QsQ0FBQztRQUNEO1lBQ0UsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsQ0FBQztZQUM1RDtnQkFDRSxRQUFRLEVBQUUsT0FBTyxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLFFBQVEsRUFBRSxNQUFNLEVBQUUsT0FBTyxFQUFFLE9BQU8sRUFBRSxNQUFNLEVBQUUsTUFBTTtnQkFDN0YsTUFBTTthQUNQO1lBQ0Q7Z0JBQ0UsU0FBUyxFQUFFLFFBQVEsRUFBRSxNQUFNLEVBQUUsUUFBUSxFQUFFLE1BQU0sRUFBRSxXQUFXLEVBQUUsU0FBUyxFQUFFLE9BQU8sRUFBRSxXQUFXO2dCQUMzRixTQUFTLEVBQUUsVUFBVSxFQUFFLFVBQVU7YUFDbEM7U0FDRjtRQUNELENBQUMsRUFBRSxDQUFDLENBQUMsU0FBUyxFQUFFLFFBQVEsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLGVBQWUsRUFBRSxpQkFBaUIsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQztRQUM5RSxDQUFDLFVBQVUsRUFBRSxTQUFTLEVBQUUsaUJBQWlCLEVBQUUsK0JBQStCLENBQUM7UUFDM0UsQ0FBQyxPQUFPLEVBQUUsVUFBVSxFQUFFLFlBQVksRUFBRSxlQUFlLENBQUMsRUFBRSxDQUFDLFNBQVMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQztRQUMxRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxLQUFLLEVBQUUsR0FBRyxDQUFDO1FBQzlELENBQUMsV0FBVyxFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsS0FBSyxDQUFDLEVBQUUsS0FBSyxFQUFFLGVBQWU7UUFDckUsRUFBQyxLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxFQUFDLEVBQUUsTUFBTTtLQUNuRCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vLyBUSElTIENPREUgSVMgR0VORVJBVEVEIC0gRE8gTk9UIE1PRElGWVxuLy8gU2VlIGFuZ3VsYXIvdG9vbHMvZ3VscC10YXNrcy9jbGRyL2V4dHJhY3QuanNcblxuY29uc3QgdSA9IHVuZGVmaW5lZDtcblxuZnVuY3Rpb24gcGx1cmFsKG46IG51bWJlcik6IG51bWJlciB7XG4gIGlmIChuID09PSAxKSByZXR1cm4gMTtcbiAgcmV0dXJuIDU7XG59XG5cbmV4cG9ydCBkZWZhdWx0IFtcbiAgJ3JtJywgW1snQU0nLCAnUE0nXSwgdSwgdV0sIHUsXG4gIFtcbiAgICBbJ0QnLCAnRycsICdNJywgJ00nLCAnRycsICdWJywgJ1MnXSwgWydkdScsICdnbGknLCAnbWEnLCAnbWUnLCAnZ2llJywgJ3ZlJywgJ3NvJ10sXG4gICAgWydkdW1lbmdpYScsICdnbGluZGVzZGknLCAnbWFyZGknLCAnbWVzZW1uYScsICdnaWV2Z2lhJywgJ3ZlbmRlcmRpJywgJ3NvbmRhJ10sXG4gICAgWydkdScsICdnbGknLCAnbWEnLCAnbWUnLCAnZ2llJywgJ3ZlJywgJ3NvJ11cbiAgXSxcbiAgdSxcbiAgW1xuICAgIFsnUycsICdGJywgJ00nLCAnQScsICdNJywgJ1onLCAnRicsICdBJywgJ1MnLCAnTycsICdOJywgJ0QnXSxcbiAgICBbXG4gICAgICAnc2NoYW4uJywgJ2ZhdnIuJywgJ21hcnMnLCAnYXZyLicsICdtYXRnJywgJ3plcmNsLicsICdmYW4uJywgJ2F2dXN0JywgJ3NldHQuJywgJ29jdC4nLCAnbm92LicsXG4gICAgICAnZGVjLidcbiAgICBdLFxuICAgIFtcbiAgICAgICdzY2hhbmVyJywgJ2ZhdnJlcicsICdtYXJzJywgJ2F2cmlnbCcsICdtYXRnJywgJ3plcmNsYWR1cicsICdmYW5hZHVyJywgJ2F2dXN0JywgJ3NldHRlbWJlcicsXG4gICAgICAnb2N0b2JlcicsICdub3ZlbWJlcicsICdkZWNlbWJlcidcbiAgICBdXG4gIF0sXG4gIHUsIFtbJ2F2LiBDci4nLCAncy4gQ3IuJ10sIHUsIFsnYXZhbnQgQ3Jpc3R1cycsICdzdWVudGVyIENyaXN0dXMnXV0sIDEsIFs2LCAwXSxcbiAgWydkZC1NTS15eScsICdkZC1NTS15JywgJ2QgXFwnZGFcXCcgTU1NTSB5JywgJ0VFRUUsIFxcJ2lsc1xcJyBkIFxcJ2RhXFwnIE1NTU0geSddLFxuICBbJ0hIOm1tJywgJ0hIOm1tOnNzJywgJ0hIOm1tOnNzIHonLCAnSEg6bW06c3Mgenp6eiddLCBbJ3sxfSB7MH0nLCB1LCB1LCB1XSxcbiAgWycuJywgJ+KAmScsICc7JywgJyUnLCAnKycsICfiiJInLCAnRScsICfDlycsICfigLAnLCAn4oieJywgJ05hTicsICc6J10sXG4gIFsnIywjIzAuIyMjJywgJyMsIyMwwqAlJywgJyMsIyMwLjAwwqDCpCcsICcjRTAnXSwgJ0NIRicsICdmcmFuYyBzdml6emVyJyxcbiAgeydKUFknOiBbJ0pQwqUnLCAnwqUnXSwgJ1VTRCc6IFsnVVMkJywgJyQnXX0sIHBsdXJhbFxuXTtcbiJdfQ==