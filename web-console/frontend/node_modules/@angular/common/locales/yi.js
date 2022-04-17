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
        define("@angular/common/locales/yi", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    // THIS CODE IS GENERATED - DO NOT MODIFY
    // See angular/tools/gulp-tasks/cldr/extract.js
    var u = undefined;
    function plural(n) {
        var i = Math.floor(Math.abs(n)), v = n.toString().replace(/^[^.]*\.?/, '').length;
        if (i === 1 && v === 0)
            return 1;
        return 5;
    }
    exports.default = [
        'yi', [['פֿאַרמיטאָג', 'נאָכמיטאָג'], u, u], u,
        [
            ['S', 'M', 'T', 'W', 'T', 'F', 'S'],
            [
                'זונטיק', 'מאָנטיק', 'דינסטיק', 'מיטוואך', 'דאנערשטיק',
                'פֿרײַטיק', 'שבת'
            ],
            u, u
        ],
        u,
        [
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
            [
                'יאַנואַר', 'פֿעברואַר', 'מערץ', 'אַפּריל', 'מיי', 'יוני',
                'יולי', 'אויגוסט', 'סעפּטעמבער', 'אקטאבער', 'נאוועמבער',
                'דעצעמבער'
            ],
            u
        ],
        [
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
            [
                'יאַנ', 'פֿעב', 'מערץ', 'אַפּר', 'מיי', 'יוני', 'יולי',
                'אויג', 'סעפּ', 'אקט', 'נאוו', 'דעצ'
            ],
            [
                'יאַנואַר', 'פֿעברואַר', 'מערץ', 'אַפּריל', 'מיי', 'יוני',
                'יולי', 'אויגוסט', 'סעפּטעמבער', 'אקטאבער', 'נאוועמבער',
                'דעצעמבער'
            ]
        ],
        [['BCE', 'CE'], u, u], 1, [6, 0],
        ['dd/MM/yy', 'dטן MMM y', 'dטן MMMM y', 'EEEE, dטן MMMM y'],
        ['HH:mm', 'HH:mm:ss', 'HH:mm:ss z', 'HH:mm:ss zzzz'], ['{1} {0}', '{1}, {0}', '{1} {0}', u],
        ['.', ',', ';', '%', '+', '-', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##0.###', '#,##0%', '¤ #,##0.00', '#E0'], u, u,
        { 'JPY': ['JP¥', '¥'], 'USD': ['US$', '$'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieWkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy95aS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsSUFBSSxDQUFDLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxHQUFHLENBQUMsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxPQUFPLENBQUMsV0FBVyxFQUFFLEVBQUUsQ0FBQyxDQUFDLE1BQU0sQ0FBQztRQUNsRixJQUFJLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUM7WUFBRSxPQUFPLENBQUMsQ0FBQztRQUNqQyxPQUFPLENBQUMsQ0FBQztJQUNYLENBQUM7SUFFRCxrQkFBZTtRQUNiLElBQUksRUFBRSxDQUFDLENBQUMsYUFBYSxFQUFFLFlBQVksQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDO1FBQzlDO1lBQ0UsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLENBQUM7WUFDbkM7Z0JBQ0UsUUFBUSxFQUFFLFNBQVMsRUFBRSxTQUFTLEVBQUUsU0FBUyxFQUFFLFdBQVc7Z0JBQ3RELFVBQVUsRUFBRSxLQUFLO2FBQ2xCO1lBQ0QsQ0FBQyxFQUFFLENBQUM7U0FDTDtRQUNELENBQUM7UUFDRDtZQUNFLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLENBQUM7WUFDL0Q7Z0JBQ0UsVUFBVSxFQUFFLFdBQVcsRUFBRSxNQUFNLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBRSxNQUFNO2dCQUN6RCxNQUFNLEVBQUUsU0FBUyxFQUFFLFlBQVksRUFBRSxTQUFTLEVBQUUsV0FBVztnQkFDdkQsVUFBVTthQUNYO1lBQ0QsQ0FBQztTQUNGO1FBQ0Q7WUFDRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDO1lBQy9EO2dCQUNFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE9BQU8sRUFBRSxLQUFLLEVBQUUsTUFBTSxFQUFFLE1BQU07Z0JBQ3RELE1BQU0sRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxLQUFLO2FBQ3JDO1lBQ0Q7Z0JBQ0UsVUFBVSxFQUFFLFdBQVcsRUFBRSxNQUFNLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBRSxNQUFNO2dCQUN6RCxNQUFNLEVBQUUsU0FBUyxFQUFFLFlBQVksRUFBRSxTQUFTLEVBQUUsV0FBVztnQkFDdkQsVUFBVTthQUNYO1NBQ0Y7UUFDRCxDQUFDLENBQUMsS0FBSyxFQUFFLElBQUksQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQ2hDLENBQUMsVUFBVSxFQUFFLFdBQVcsRUFBRSxZQUFZLEVBQUUsa0JBQWtCLENBQUM7UUFDM0QsQ0FBQyxPQUFPLEVBQUUsVUFBVSxFQUFFLFlBQVksRUFBRSxlQUFlLENBQUMsRUFBRSxDQUFDLFNBQVMsRUFBRSxVQUFVLEVBQUUsU0FBUyxFQUFFLENBQUMsQ0FBQztRQUMzRixDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxLQUFLLEVBQUUsR0FBRyxDQUFDO1FBQzlELENBQUMsV0FBVyxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsS0FBSyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUM7UUFDbEQsRUFBQyxLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxFQUFDLEVBQUUsTUFBTTtLQUNuRCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vLyBUSElTIENPREUgSVMgR0VORVJBVEVEIC0gRE8gTk9UIE1PRElGWVxuLy8gU2VlIGFuZ3VsYXIvdG9vbHMvZ3VscC10YXNrcy9jbGRyL2V4dHJhY3QuanNcblxuY29uc3QgdSA9IHVuZGVmaW5lZDtcblxuZnVuY3Rpb24gcGx1cmFsKG46IG51bWJlcik6IG51bWJlciB7XG4gIGxldCBpID0gTWF0aC5mbG9vcihNYXRoLmFicyhuKSksIHYgPSBuLnRvU3RyaW5nKCkucmVwbGFjZSgvXlteLl0qXFwuPy8sICcnKS5sZW5ndGg7XG4gIGlmIChpID09PSAxICYmIHYgPT09IDApIHJldHVybiAxO1xuICByZXR1cm4gNTtcbn1cblxuZXhwb3J0IGRlZmF1bHQgW1xuICAneWknLCBbWyfXpNa/15DWt9eo157XmdeY15DWuNeSJywgJ9eg15DWuNeb157XmdeY15DWuNeSJ10sIHUsIHVdLCB1LFxuICBbXG4gICAgWydTJywgJ00nLCAnVCcsICdXJywgJ1QnLCAnRicsICdTJ10sXG4gICAgW1xuICAgICAgJ9eW15XXoNeY15nXpycsICfXnteQ1rjXoNeY15nXpycsICfXk9eZ16DXodeY15nXpycsICfXnteZ15jXldeV15DXmicsICfXk9eQ16DXoteo16nXmNeZ16cnLFxuICAgICAgJ9ek1r/XqNey1rfXmNeZ16cnLCAn16nXkdeqJ1xuICAgIF0sXG4gICAgdSwgdVxuICBdLFxuICB1LFxuICBbXG4gICAgWycxJywgJzInLCAnMycsICc0JywgJzUnLCAnNicsICc3JywgJzgnLCAnOScsICcxMCcsICcxMScsICcxMiddLFxuICAgIFtcbiAgICAgICfXmdeQ1rfXoNeV15DWt9eoJywgJ9ek1r/XoteR16jXldeQ1rfXqCcsICfXntei16jXpScsICfXkNa316TWvNeo15nXnCcsICfXnteZ15knLCAn15nXldeg15knLFxuICAgICAgJ9eZ15XXnNeZJywgJ9eQ15XXmdeS15XXodeYJywgJ9eh16LXpNa815jXotee15HXoteoJywgJ9eQ16fXmNeQ15HXoteoJywgJ9eg15DXldeV16LXnteR16LXqCcsXG4gICAgICAn15PXotem16LXnteR16LXqCdcbiAgICBdLFxuICAgIHVcbiAgXSxcbiAgW1xuICAgIFsnMScsICcyJywgJzMnLCAnNCcsICc1JywgJzYnLCAnNycsICc4JywgJzknLCAnMTAnLCAnMTEnLCAnMTInXSxcbiAgICBbXG4gICAgICAn15nXkNa316AnLCAn16TWv9ei15EnLCAn157Xoteo16UnLCAn15DWt9ek1rzXqCcsICfXnteZ15knLCAn15nXldeg15knLCAn15nXldec15knLFxuICAgICAgJ9eQ15XXmdeSJywgJ9eh16LXpNa8JywgJ9eQ16fXmCcsICfXoNeQ15XXlScsICfXk9ei16YnXG4gICAgXSxcbiAgICBbXG4gICAgICAn15nXkNa316DXldeQ1rfXqCcsICfXpNa/16LXkdeo15XXkNa316gnLCAn157Xoteo16UnLCAn15DWt9ek1rzXqNeZ15wnLCAn157XmdeZJywgJ9eZ15XXoNeZJyxcbiAgICAgICfXmdeV15zXmScsICfXkNeV15nXkteV16HXmCcsICfXodei16TWvNeY16LXnteR16LXqCcsICfXkNen15jXkNeR16LXqCcsICfXoNeQ15XXldei157Xkdei16gnLFxuICAgICAgJ9eT16LXptei157Xkdei16gnXG4gICAgXVxuICBdLFxuICBbWydCQ0UnLCAnQ0UnXSwgdSwgdV0sIDEsIFs2LCAwXSxcbiAgWydkZC9NTS95eScsICdk15jXnyBNTU0geScsICdk15jXnyBNTU1NIHknLCAnRUVFRSwgZNeY158gTU1NTSB5J10sXG4gIFsnSEg6bW0nLCAnSEg6bW06c3MnLCAnSEg6bW06c3MgeicsICdISDptbTpzcyB6enp6J10sIFsnezF9IHswfScsICd7MX0sIHswfScsICd7MX0gezB9JywgdV0sXG4gIFsnLicsICcsJywgJzsnLCAnJScsICcrJywgJy0nLCAnRScsICfDlycsICfigLAnLCAn4oieJywgJ05hTicsICc6J10sXG4gIFsnIywjIzAuIyMjJywgJyMsIyMwJScsICfCpMKgIywjIzAuMDAnLCAnI0UwJ10sIHUsIHUsXG4gIHsnSlBZJzogWydKUMKlJywgJ8KlJ10sICdVU0QnOiBbJ1VTJCcsICckJ119LCBwbHVyYWxcbl07XG4iXX0=