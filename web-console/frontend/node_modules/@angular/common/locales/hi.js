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
        define("@angular/common/locales/hi", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    // THIS CODE IS GENERATED - DO NOT MODIFY
    // See angular/tools/gulp-tasks/cldr/extract.js
    var u = undefined;
    function plural(n) {
        var i = Math.floor(Math.abs(n));
        if (i === 0 || n === 1)
            return 1;
        return 5;
    }
    exports.default = [
        'hi', [['पू', 'अ'], ['पूर्वाह्न', 'अपराह्न'], u], u,
        [
            ['र', 'सो', 'मं', 'बु', 'गु', 'शु', 'श'],
            [
                'रवि', 'सोम', 'मंगल', 'बुध', 'गुरु', 'शुक्र',
                'शनि'
            ],
            [
                'रविवार', 'सोमवार', 'मंगलवार', 'बुधवार',
                'गुरुवार', 'शुक्रवार', 'शनिवार'
            ],
            ['र', 'सो', 'मं', 'बु', 'गु', 'शु', 'श']
        ],
        u,
        [
            [
                'ज', 'फ़', 'मा', 'अ', 'म', 'जू', 'जु', 'अ', 'सि', 'अ', 'न',
                'दि'
            ],
            [
                'जन॰', 'फ़र॰', 'मार्च', 'अप्रैल', 'मई', 'जून',
                'जुल॰', 'अग॰', 'सित॰', 'अक्तू॰', 'नव॰', 'दिस॰'
            ],
            [
                'जनवरी', 'फ़रवरी', 'मार्च', 'अप्रैल', 'मई',
                'जून', 'जुलाई', 'अगस्त', 'सितंबर',
                'अक्तूबर', 'नवंबर', 'दिसंबर'
            ]
        ],
        u,
        [
            ['ईसा-पूर्व', 'ईस्वी'], u,
            ['ईसा-पूर्व', 'ईसवी सन']
        ],
        0, [0, 0], ['d/M/yy', 'd MMM y', 'd MMMM y', 'EEEE, d MMMM y'],
        ['h:mm a', 'h:mm:ss a', 'h:mm:ss a z', 'h:mm:ss a zzzz'], ['{1}, {0}', u, '{1} को {0}', u],
        ['.', ',', ';', '%', '+', '-', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##,##0.###', '#,##,##0%', '¤#,##,##0.00', '[#E0]'], '₹',
        'भारतीय रुपया',
        { 'JPY': ['JP¥', '¥'], 'RON': [u, 'लेई'], 'THB': ['฿'], 'TWD': ['NT$'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaGkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy9oaS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsSUFBSSxDQUFDLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDaEMsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDO1lBQUUsT0FBTyxDQUFDLENBQUM7UUFDakMsT0FBTyxDQUFDLENBQUM7SUFDWCxDQUFDO0lBRUQsa0JBQWU7UUFDYixJQUFJLEVBQUUsQ0FBQyxDQUFDLElBQUksRUFBRSxHQUFHLENBQUMsRUFBRSxDQUFDLFdBQVcsRUFBRSxTQUFTLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDO1FBQ25EO1lBQ0UsQ0FBQyxHQUFHLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxHQUFHLENBQUM7WUFDeEM7Z0JBQ0UsS0FBSyxFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxPQUFPO2dCQUM1QyxLQUFLO2FBQ047WUFDRDtnQkFDRSxRQUFRLEVBQUUsUUFBUSxFQUFFLFNBQVMsRUFBRSxRQUFRO2dCQUN2QyxTQUFTLEVBQUUsVUFBVSxFQUFFLFFBQVE7YUFDaEM7WUFDRCxDQUFDLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLEdBQUcsQ0FBQztTQUN6QztRQUNELENBQUM7UUFDRDtZQUNFO2dCQUNFLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxHQUFHLEVBQUUsSUFBSSxFQUFFLEdBQUcsRUFBRSxHQUFHO2dCQUMxRCxJQUFJO2FBQ0w7WUFDRDtnQkFDRSxLQUFLLEVBQUUsTUFBTSxFQUFFLE9BQU8sRUFBRSxRQUFRLEVBQUUsSUFBSSxFQUFFLEtBQUs7Z0JBQzdDLE1BQU0sRUFBRSxLQUFLLEVBQUUsTUFBTSxFQUFFLFFBQVEsRUFBRSxLQUFLLEVBQUUsTUFBTTthQUMvQztZQUNEO2dCQUNFLE9BQU8sRUFBRSxRQUFRLEVBQUUsT0FBTyxFQUFFLFFBQVEsRUFBRSxJQUFJO2dCQUMxQyxLQUFLLEVBQUUsT0FBTyxFQUFFLE9BQU8sRUFBRSxRQUFRO2dCQUNqQyxTQUFTLEVBQUUsT0FBTyxFQUFFLFFBQVE7YUFDN0I7U0FDRjtRQUNELENBQUM7UUFDRDtZQUNFLENBQUMsV0FBVyxFQUFFLE9BQU8sQ0FBQyxFQUFFLENBQUM7WUFDekIsQ0FBQyxXQUFXLEVBQUUsU0FBUyxDQUFDO1NBQ3pCO1FBQ0QsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsUUFBUSxFQUFFLFNBQVMsRUFBRSxVQUFVLEVBQUUsZ0JBQWdCLENBQUM7UUFDOUQsQ0FBQyxRQUFRLEVBQUUsV0FBVyxFQUFFLGFBQWEsRUFBRSxnQkFBZ0IsQ0FBQyxFQUFFLENBQUMsVUFBVSxFQUFFLENBQUMsRUFBRSxZQUFZLEVBQUUsQ0FBQyxDQUFDO1FBQzFGLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEtBQUssRUFBRSxHQUFHLENBQUM7UUFDOUQsQ0FBQyxjQUFjLEVBQUUsV0FBVyxFQUFFLGNBQWMsRUFBRSxPQUFPLENBQUMsRUFBRSxHQUFHO1FBQzNELGNBQWM7UUFDZCxFQUFDLEtBQUssRUFBRSxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsRUFBRSxLQUFLLEVBQUUsQ0FBQyxDQUFDLEVBQUUsS0FBSyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxDQUFDLEVBQUMsRUFBRSxNQUFNO0tBQy9FLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8vIFRISVMgQ09ERSBJUyBHRU5FUkFURUQgLSBETyBOT1QgTU9ESUZZXG4vLyBTZWUgYW5ndWxhci90b29scy9ndWxwLXRhc2tzL2NsZHIvZXh0cmFjdC5qc1xuXG5jb25zdCB1ID0gdW5kZWZpbmVkO1xuXG5mdW5jdGlvbiBwbHVyYWwobjogbnVtYmVyKTogbnVtYmVyIHtcbiAgbGV0IGkgPSBNYXRoLmZsb29yKE1hdGguYWJzKG4pKTtcbiAgaWYgKGkgPT09IDAgfHwgbiA9PT0gMSkgcmV0dXJuIDE7XG4gIHJldHVybiA1O1xufVxuXG5leHBvcnQgZGVmYXVsdCBbXG4gICdoaScsIFtbJ+CkquClgicsICfgpIUnXSwgWyfgpKrgpYLgpLDgpY3gpLXgpL7gpLngpY3gpKgnLCAn4KSF4KSq4KSw4KS+4KS54KWN4KSoJ10sIHVdLCB1LFxuICBbXG4gICAgWyfgpLAnLCAn4KS44KWLJywgJ+CkruCkgicsICfgpKzgpYEnLCAn4KSX4KWBJywgJ+CktuClgScsICfgpLYnXSxcbiAgICBbXG4gICAgICAn4KSw4KS14KS/JywgJ+CkuOCli+CkricsICfgpK7gpILgpJfgpLInLCAn4KSs4KWB4KSnJywgJ+Ckl+ClgeCksOClgScsICfgpLbgpYHgpJXgpY3gpLAnLFxuICAgICAgJ+CktuCkqOCkvydcbiAgICBdLFxuICAgIFtcbiAgICAgICfgpLDgpLXgpL/gpLXgpL7gpLAnLCAn4KS44KWL4KSu4KS14KS+4KSwJywgJ+CkruCkguCkl+CksuCkteCkvuCksCcsICfgpKzgpYHgpKfgpLXgpL7gpLAnLFxuICAgICAgJ+Ckl+ClgeCksOClgeCkteCkvuCksCcsICfgpLbgpYHgpJXgpY3gpLDgpLXgpL7gpLAnLCAn4KS24KSo4KS/4KS14KS+4KSwJ1xuICAgIF0sXG4gICAgWyfgpLAnLCAn4KS44KWLJywgJ+CkruCkgicsICfgpKzgpYEnLCAn4KSX4KWBJywgJ+CktuClgScsICfgpLYnXVxuICBdLFxuICB1LFxuICBbXG4gICAgW1xuICAgICAgJ+CknCcsICfgpKvgpLwnLCAn4KSu4KS+JywgJ+CkhScsICfgpK4nLCAn4KSc4KWCJywgJ+CknOClgScsICfgpIUnLCAn4KS44KS/JywgJ+CkhScsICfgpKgnLFxuICAgICAgJ+CkpuCkvydcbiAgICBdLFxuICAgIFtcbiAgICAgICfgpJzgpKjgpbAnLCAn4KSr4KS84KSw4KWwJywgJ+CkruCkvuCksOCljeCkmicsICfgpIXgpKrgpY3gpLDgpYjgpLInLCAn4KSu4KSIJywgJ+CknOClguCkqCcsXG4gICAgICAn4KSc4KWB4KSy4KWwJywgJ+CkheCkl+ClsCcsICfgpLjgpL/gpKTgpbAnLCAn4KSF4KSV4KWN4KSk4KWC4KWwJywgJ+CkqOCkteClsCcsICfgpKbgpL/gpLjgpbAnXG4gICAgXSxcbiAgICBbXG4gICAgICAn4KSc4KSo4KS14KSw4KWAJywgJ+Ckq+CkvOCksOCkteCksOClgCcsICfgpK7gpL7gpLDgpY3gpJonLCAn4KSF4KSq4KWN4KSw4KWI4KSyJywgJ+CkruCkiCcsXG4gICAgICAn4KSc4KWC4KSoJywgJ+CknOClgeCksuCkvuCkiCcsICfgpIXgpJfgpLjgpY3gpKQnLCAn4KS44KS/4KSk4KSC4KSs4KSwJyxcbiAgICAgICfgpIXgpJXgpY3gpKTgpYLgpKzgpLAnLCAn4KSo4KS14KSC4KSs4KSwJywgJ+CkpuCkv+CkuOCkguCkrOCksCdcbiAgICBdXG4gIF0sXG4gIHUsXG4gIFtcbiAgICBbJ+CkiOCkuOCkvi3gpKrgpYLgpLDgpY3gpLUnLCAn4KSI4KS44KWN4KS14KWAJ10sIHUsXG4gICAgWyfgpIjgpLjgpL4t4KSq4KWC4KSw4KWN4KS1JywgJ+CkiOCkuOCkteClgCDgpLjgpKgnXVxuICBdLFxuICAwLCBbMCwgMF0sIFsnZC9NL3l5JywgJ2QgTU1NIHknLCAnZCBNTU1NIHknLCAnRUVFRSwgZCBNTU1NIHknXSxcbiAgWydoOm1tIGEnLCAnaDptbTpzcyBhJywgJ2g6bW06c3MgYSB6JywgJ2g6bW06c3MgYSB6enp6J10sIFsnezF9LCB7MH0nLCB1LCAnezF9IOCkleCliyB7MH0nLCB1XSxcbiAgWycuJywgJywnLCAnOycsICclJywgJysnLCAnLScsICdFJywgJ8OXJywgJ+KAsCcsICfiiJ4nLCAnTmFOJywgJzonXSxcbiAgWycjLCMjLCMjMC4jIyMnLCAnIywjIywjIzAlJywgJ8KkIywjIywjIzAuMDAnLCAnWyNFMF0nXSwgJ+KCuScsXG4gICfgpK3gpL7gpLDgpKTgpYDgpK8g4KSw4KWB4KSq4KSv4KS+JyxcbiAgeydKUFknOiBbJ0pQwqUnLCAnwqUnXSwgJ1JPTic6IFt1LCAn4KSy4KWH4KSIJ10sICdUSEInOiBbJ+C4vyddLCAnVFdEJzogWydOVCQnXX0sIHBsdXJhbFxuXTtcbiJdfQ==