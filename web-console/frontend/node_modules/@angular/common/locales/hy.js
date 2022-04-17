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
        define("@angular/common/locales/hy", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    // THIS CODE IS GENERATED - DO NOT MODIFY
    // See angular/tools/gulp-tasks/cldr/extract.js
    var u = undefined;
    function plural(n) {
        var i = Math.floor(Math.abs(n));
        if (i === 0 || i === 1)
            return 1;
        return 5;
    }
    exports.default = [
        'hy', [['ա', 'հ'], ['ԿԱ', 'ԿՀ'], u], [['ԿԱ', 'ԿՀ'], u, u],
        [
            ['Կ', 'Ե', 'Ե', 'Չ', 'Հ', 'Ո', 'Շ'],
            ['կիր', 'երկ', 'երք', 'չրք', 'հնգ', 'ուր', 'շբթ'],
            [
                'կիրակի', 'երկուշաբթի', 'երեքշաբթի', 'չորեքշաբթի',
                'հինգշաբթի', 'ուրբաթ', 'շաբաթ'
            ],
            ['կր', 'եկ', 'եք', 'չք', 'հգ', 'ու', 'շբ']
        ],
        u,
        [
            ['Հ', 'Փ', 'Մ', 'Ա', 'Մ', 'Հ', 'Հ', 'Օ', 'Ս', 'Հ', 'Ն', 'Դ'],
            [
                'հնվ', 'փտվ', 'մրտ', 'ապր', 'մյս', 'հնս', 'հլս', 'օգս', 'սեպ',
                'հոկ', 'նոյ', 'դեկ'
            ],
            [
                'հունվարի', 'փետրվարի', 'մարտի', 'ապրիլի', 'մայիսի',
                'հունիսի', 'հուլիսի', 'օգոստոսի', 'սեպտեմբերի',
                'հոկտեմբերի', 'նոյեմբերի', 'դեկտեմբերի'
            ]
        ],
        [
            ['Հ', 'Փ', 'Մ', 'Ա', 'Մ', 'Հ', 'Հ', 'Օ', 'Ս', 'Հ', 'Ն', 'Դ'],
            [
                'հնվ', 'փտվ', 'մրտ', 'ապր', 'մյս', 'հնս', 'հլս', 'օգս', 'սեպ',
                'հոկ', 'նոյ', 'դեկ'
            ],
            [
                'հունվար', 'փետրվար', 'մարտ', 'ապրիլ', 'մայիս', 'հունիս',
                'հուլիս', 'օգոստոս', 'սեպտեմբեր', 'հոկտեմբեր',
                'նոյեմբեր', 'դեկտեմբեր'
            ]
        ],
        [['մ.թ.ա.', 'մ.թ.'], u, ['Քրիստոսից առաջ', 'Քրիստոսից հետո']], 1,
        [6, 0], ['dd.MM.yy', 'dd MMM, y թ.', 'dd MMMM, y թ.', 'y թ. MMMM d, EEEE'],
        ['HH:mm', 'HH:mm:ss', 'HH:mm:ss z', 'HH:mm:ss zzzz'], ['{1}, {0}', u, u, u],
        [',', ' ', ';', '%', '+', '-', 'E', '×', '‰', '∞', 'ՈչԹ', ':'],
        ['#,##0.###', '#,##0%', '#,##0.00 ¤', '#E0'], '֏', 'հայկական դրամ',
        { 'AMD': ['֏'], 'JPY': ['JP¥', '¥'], 'THB': ['฿'], 'TWD': ['NT$'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy9oeS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsSUFBSSxDQUFDLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDaEMsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDO1lBQUUsT0FBTyxDQUFDLENBQUM7UUFDakMsT0FBTyxDQUFDLENBQUM7SUFDWCxDQUFDO0lBRUQsa0JBQWU7UUFDYixJQUFJLEVBQUUsQ0FBQyxDQUFDLEdBQUcsRUFBRSxHQUFHLENBQUMsRUFBRSxDQUFDLElBQUksRUFBRSxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsSUFBSSxFQUFFLElBQUksQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDekQ7WUFDRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsQ0FBQztZQUNuQyxDQUFDLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssQ0FBQztZQUNqRDtnQkFDRSxRQUFRLEVBQUUsWUFBWSxFQUFFLFdBQVcsRUFBRSxZQUFZO2dCQUNqRCxXQUFXLEVBQUUsUUFBUSxFQUFFLE9BQU87YUFDL0I7WUFDRCxDQUFDLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksQ0FBQztTQUMzQztRQUNELENBQUM7UUFDRDtZQUNFLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLENBQUM7WUFDNUQ7Z0JBQ0UsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLO2dCQUM3RCxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUs7YUFDcEI7WUFDRDtnQkFDRSxVQUFVLEVBQUUsVUFBVSxFQUFFLE9BQU8sRUFBRSxRQUFRLEVBQUUsUUFBUTtnQkFDbkQsU0FBUyxFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsWUFBWTtnQkFDOUMsWUFBWSxFQUFFLFdBQVcsRUFBRSxZQUFZO2FBQ3hDO1NBQ0Y7UUFDRDtZQUNFLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLENBQUM7WUFDNUQ7Z0JBQ0UsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLO2dCQUM3RCxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUs7YUFDcEI7WUFDRDtnQkFDRSxTQUFTLEVBQUUsU0FBUyxFQUFFLE1BQU0sRUFBRSxPQUFPLEVBQUUsT0FBTyxFQUFFLFFBQVE7Z0JBQ3hELFFBQVEsRUFBRSxTQUFTLEVBQUUsV0FBVyxFQUFFLFdBQVc7Z0JBQzdDLFVBQVUsRUFBRSxXQUFXO2FBQ3hCO1NBQ0Y7UUFDRCxDQUFDLENBQUMsUUFBUSxFQUFFLE1BQU0sQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLGdCQUFnQixFQUFFLGdCQUFnQixDQUFDLENBQUMsRUFBRSxDQUFDO1FBQ2hFLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsVUFBVSxFQUFFLGNBQWMsRUFBRSxlQUFlLEVBQUUsbUJBQW1CLENBQUM7UUFDMUUsQ0FBQyxPQUFPLEVBQUUsVUFBVSxFQUFFLFlBQVksRUFBRSxlQUFlLENBQUMsRUFBRSxDQUFDLFVBQVUsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQztRQUMzRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxLQUFLLEVBQUUsR0FBRyxDQUFDO1FBQzlELENBQUMsV0FBVyxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsS0FBSyxDQUFDLEVBQUUsR0FBRyxFQUFFLGVBQWU7UUFDbEUsRUFBQyxLQUFLLEVBQUUsQ0FBQyxHQUFHLENBQUMsRUFBRSxLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxDQUFDLEVBQUMsRUFBRSxNQUFNO0tBQzFFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8vIFRISVMgQ09ERSBJUyBHRU5FUkFURUQgLSBETyBOT1QgTU9ESUZZXG4vLyBTZWUgYW5ndWxhci90b29scy9ndWxwLXRhc2tzL2NsZHIvZXh0cmFjdC5qc1xuXG5jb25zdCB1ID0gdW5kZWZpbmVkO1xuXG5mdW5jdGlvbiBwbHVyYWwobjogbnVtYmVyKTogbnVtYmVyIHtcbiAgbGV0IGkgPSBNYXRoLmZsb29yKE1hdGguYWJzKG4pKTtcbiAgaWYgKGkgPT09IDAgfHwgaSA9PT0gMSkgcmV0dXJuIDE7XG4gIHJldHVybiA1O1xufVxuXG5leHBvcnQgZGVmYXVsdCBbXG4gICdoeScsIFtbJ9WhJywgJ9WwJ10sIFsn1L/UsScsICfUv9WAJ10sIHVdLCBbWyfUv9SxJywgJ9S/1YAnXSwgdSwgdV0sXG4gIFtcbiAgICBbJ9S/JywgJ9S1JywgJ9S1JywgJ9WJJywgJ9WAJywgJ9WIJywgJ9WHJ10sXG4gICAgWyfVr9Wr1oAnLCAn1aXWgNWvJywgJ9Wl1oDWhCcsICfVudaA1oQnLCAn1bDVttWjJywgJ9W41oLWgCcsICfVt9Wi1aknXSxcbiAgICBbXG4gICAgICAn1a/Vq9aA1aHVr9WrJywgJ9Wl1oDVr9W41oLVt9Wh1aLVqdWrJywgJ9Wl1oDVpdaE1bfVodWi1anVqycsICfVudW41oDVpdaE1bfVodWi1anVqycsXG4gICAgICAn1bDVq9W21aPVt9Wh1aLVqdWrJywgJ9W41oLWgNWi1aHVqScsICfVt9Wh1aLVodWpJ1xuICAgIF0sXG4gICAgWyfVr9aAJywgJ9Wl1a8nLCAn1aXWhCcsICfVudaEJywgJ9Ww1aMnLCAn1bjWgicsICfVt9WiJ11cbiAgXSxcbiAgdSxcbiAgW1xuICAgIFsn1YAnLCAn1ZMnLCAn1YQnLCAn1LEnLCAn1YQnLCAn1YAnLCAn1YAnLCAn1ZUnLCAn1Y0nLCAn1YAnLCAn1YYnLCAn1LQnXSxcbiAgICBbXG4gICAgICAn1bDVttW+JywgJ9aD1b/VvicsICfVtNaA1b8nLCAn1aHVutaAJywgJ9W01bXVvScsICfVsNW21b0nLCAn1bDVrNW9JywgJ9aF1aPVvScsICfVvdWl1bonLFxuICAgICAgJ9Ww1bjVrycsICfVttW41bUnLCAn1aTVpdWvJ1xuICAgIF0sXG4gICAgW1xuICAgICAgJ9Ww1bjWgtW21b7VodaA1asnLCAn1oPVpdW/1oDVvtWh1oDVqycsICfVtNWh1oDVv9WrJywgJ9Wh1brWgNWr1azVqycsICfVtNWh1bXVq9W91asnLFxuICAgICAgJ9Ww1bjWgtW21avVvdWrJywgJ9Ww1bjWgtWs1avVvdWrJywgJ9aF1aPVuNW91b/VuNW91asnLCAn1b3VpdW61b/VpdW01aLVpdaA1asnLFxuICAgICAgJ9Ww1bjVr9W/1aXVtNWi1aXWgNWrJywgJ9W21bjVtdWl1bTVotWl1oDVqycsICfVpNWl1a/Vv9Wl1bTVotWl1oDVqydcbiAgICBdXG4gIF0sXG4gIFtcbiAgICBbJ9WAJywgJ9WTJywgJ9WEJywgJ9SxJywgJ9WEJywgJ9WAJywgJ9WAJywgJ9WVJywgJ9WNJywgJ9WAJywgJ9WGJywgJ9S0J10sXG4gICAgW1xuICAgICAgJ9Ww1bbVvicsICfWg9W/1b4nLCAn1bTWgNW/JywgJ9Wh1brWgCcsICfVtNW11b0nLCAn1bDVttW9JywgJ9Ww1azVvScsICfWhdWj1b0nLCAn1b3VpdW6JyxcbiAgICAgICfVsNW41a8nLCAn1bbVuNW1JywgJ9Wk1aXVrydcbiAgICBdLFxuICAgIFtcbiAgICAgICfVsNW41oLVttW+1aHWgCcsICfWg9Wl1b/WgNW+1aHWgCcsICfVtNWh1oDVvycsICfVodW61oDVq9WsJywgJ9W01aHVtdWr1b0nLCAn1bDVuNaC1bbVq9W9JyxcbiAgICAgICfVsNW41oLVrNWr1b0nLCAn1oXVo9W41b3Vv9W41b0nLCAn1b3VpdW61b/VpdW01aLVpdaAJywgJ9Ww1bjVr9W/1aXVtNWi1aXWgCcsXG4gICAgICAn1bbVuNW11aXVtNWi1aXWgCcsICfVpNWl1a/Vv9Wl1bTVotWl1oAnXG4gICAgXVxuICBdLFxuICBbWyfVtC7VqS7VoS4nLCAn1bQu1akuJ10sIHUsIFsn1ZTWgNWr1b3Vv9W41b3Vq9aBINWh1bzVodW7JywgJ9WU1oDVq9W91b/VuNW91avWgSDVsNWl1b/VuCddXSwgMSxcbiAgWzYsIDBdLCBbJ2RkLk1NLnl5JywgJ2RkIE1NTSwgeSDVqS4nLCAnZGQgTU1NTSwgeSDVqS4nLCAneSDVqS4gTU1NTSBkLCBFRUVFJ10sXG4gIFsnSEg6bW0nLCAnSEg6bW06c3MnLCAnSEg6bW06c3MgeicsICdISDptbTpzcyB6enp6J10sIFsnezF9LCB7MH0nLCB1LCB1LCB1XSxcbiAgWycsJywgJ8KgJywgJzsnLCAnJScsICcrJywgJy0nLCAnRScsICfDlycsICfigLAnLCAn4oieJywgJ9WI1bnUuScsICc6J10sXG4gIFsnIywjIzAuIyMjJywgJyMsIyMwJScsICcjLCMjMC4wMMKgwqQnLCAnI0UwJ10sICfWjycsICfVsNWh1bXVr9Wh1a/VodW2INWk1oDVodW0JyxcbiAgeydBTUQnOiBbJ9aPJ10sICdKUFknOiBbJ0pQwqUnLCAnwqUnXSwgJ1RIQic6IFsn4Li/J10sICdUV0QnOiBbJ05UJCddfSwgcGx1cmFsXG5dO1xuIl19