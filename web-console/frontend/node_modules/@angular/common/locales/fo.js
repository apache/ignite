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
        define("@angular/common/locales/fo", ["require", "exports"], factory);
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
        'fo', [['AM', 'PM'], u, u], u,
        [
            ['S', 'M', 'T', 'M', 'H', 'F', 'L'],
            ['sun.', 'mán.', 'týs.', 'mik.', 'hós.', 'frí.', 'ley.'],
            [
                'sunnudagur', 'mánadagur', 'týsdagur', 'mikudagur', 'hósdagur', 'fríggjadagur',
                'leygardagur'
            ],
            ['su.', 'má.', 'tý.', 'mi.', 'hó.', 'fr.', 'le.']
        ],
        [
            ['S', 'M', 'T', 'M', 'H', 'F', 'L'], ['sun', 'mán', 'týs', 'mik', 'hós', 'frí', 'ley'],
            [
                'sunnudagur', 'mánadagur', 'týsdagur', 'mikudagur', 'hósdagur', 'fríggjadagur',
                'leygardagur'
            ],
            ['su', 'má', 'tý', 'mi', 'hó', 'fr', 'le']
        ],
        [
            ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'],
            ['jan.', 'feb.', 'mar.', 'apr.', 'mai', 'jun.', 'jul.', 'aug.', 'sep.', 'okt.', 'nov.', 'des.'],
            [
                'januar', 'februar', 'mars', 'apríl', 'mai', 'juni', 'juli', 'august', 'september',
                'oktober', 'november', 'desember'
            ]
        ],
        [
            ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'],
            ['jan', 'feb', 'mar', 'apr', 'mai', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'des'],
            [
                'januar', 'februar', 'mars', 'apríl', 'mai', 'juni', 'juli', 'august', 'september',
                'oktober', 'november', 'desember'
            ]
        ],
        [['fKr', 'eKr'], ['f.Kr.', 'e.Kr.'], ['fyri Krist', 'eftir Krist']], 1, [6, 0],
        ['dd.MM.yy', 'dd.MM.y', 'd. MMMM y', 'EEEE, d. MMMM y'],
        ['HH:mm', 'HH:mm:ss', 'HH:mm:ss z', 'HH:mm:ss zzzz'], ['{1}, {0}', u, '{1} \'kl\'. {0}', u],
        [',', '.', ';', '%', '+', '−', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##0.###', '#,##0 %', '#,##0.00 ¤', '#E0'], 'kr', 'donsk króna',
        { 'DKK': ['kr'], 'JPY': ['JP¥', '¥'], 'USD': ['US$', '$'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZm8uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy9mby50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsSUFBSSxDQUFDLEtBQUssQ0FBQztZQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQ3RCLE9BQU8sQ0FBQyxDQUFDO0lBQ1gsQ0FBQztJQUVELGtCQUFlO1FBQ2IsSUFBSSxFQUFFLENBQUMsQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUM7UUFDN0I7WUFDRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsQ0FBQztZQUNuQyxDQUFDLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU0sQ0FBQztZQUN4RDtnQkFDRSxZQUFZLEVBQUUsV0FBVyxFQUFFLFVBQVUsRUFBRSxXQUFXLEVBQUUsVUFBVSxFQUFFLGNBQWM7Z0JBQzlFLGFBQWE7YUFDZDtZQUNELENBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxDQUFDO1NBQ2xEO1FBQ0Q7WUFDRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsQ0FBQyxFQUFFLENBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxDQUFDO1lBQ3RGO2dCQUNFLFlBQVksRUFBRSxXQUFXLEVBQUUsVUFBVSxFQUFFLFdBQVcsRUFBRSxVQUFVLEVBQUUsY0FBYztnQkFDOUUsYUFBYTthQUNkO1lBQ0QsQ0FBQyxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLENBQUM7U0FDM0M7UUFDRDtZQUNFLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLENBQUM7WUFDNUQsQ0FBQyxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU0sQ0FBQztZQUMvRjtnQkFDRSxRQUFRLEVBQUUsU0FBUyxFQUFFLE1BQU0sRUFBRSxPQUFPLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsUUFBUSxFQUFFLFdBQVc7Z0JBQ2xGLFNBQVMsRUFBRSxVQUFVLEVBQUUsVUFBVTthQUNsQztTQUNGO1FBQ0Q7WUFDRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxDQUFDO1lBQzVELENBQUMsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxLQUFLLENBQUM7WUFDcEY7Z0JBQ0UsUUFBUSxFQUFFLFNBQVMsRUFBRSxNQUFNLEVBQUUsT0FBTyxFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLFFBQVEsRUFBRSxXQUFXO2dCQUNsRixTQUFTLEVBQUUsVUFBVSxFQUFFLFVBQVU7YUFDbEM7U0FDRjtRQUNELENBQUMsQ0FBQyxLQUFLLEVBQUUsS0FBSyxDQUFDLEVBQUUsQ0FBQyxPQUFPLEVBQUUsT0FBTyxDQUFDLEVBQUUsQ0FBQyxZQUFZLEVBQUUsYUFBYSxDQUFDLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQzlFLENBQUMsVUFBVSxFQUFFLFNBQVMsRUFBRSxXQUFXLEVBQUUsaUJBQWlCLENBQUM7UUFDdkQsQ0FBQyxPQUFPLEVBQUUsVUFBVSxFQUFFLFlBQVksRUFBRSxlQUFlLENBQUMsRUFBRSxDQUFDLFVBQVUsRUFBRSxDQUFDLEVBQUUsaUJBQWlCLEVBQUUsQ0FBQyxDQUFDO1FBQzNGLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEtBQUssRUFBRSxHQUFHLENBQUM7UUFDOUQsQ0FBQyxXQUFXLEVBQUUsU0FBUyxFQUFFLFlBQVksRUFBRSxLQUFLLENBQUMsRUFBRSxJQUFJLEVBQUUsYUFBYTtRQUNsRSxFQUFDLEtBQUssRUFBRSxDQUFDLElBQUksQ0FBQyxFQUFFLEtBQUssRUFBRSxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsRUFBRSxLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLEVBQUMsRUFBRSxNQUFNO0tBQ2xFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8vIFRISVMgQ09ERSBJUyBHRU5FUkFURUQgLSBETyBOT1QgTU9ESUZZXG4vLyBTZWUgYW5ndWxhci90b29scy9ndWxwLXRhc2tzL2NsZHIvZXh0cmFjdC5qc1xuXG5jb25zdCB1ID0gdW5kZWZpbmVkO1xuXG5mdW5jdGlvbiBwbHVyYWwobjogbnVtYmVyKTogbnVtYmVyIHtcbiAgaWYgKG4gPT09IDEpIHJldHVybiAxO1xuICByZXR1cm4gNTtcbn1cblxuZXhwb3J0IGRlZmF1bHQgW1xuICAnZm8nLCBbWydBTScsICdQTSddLCB1LCB1XSwgdSxcbiAgW1xuICAgIFsnUycsICdNJywgJ1QnLCAnTScsICdIJywgJ0YnLCAnTCddLFxuICAgIFsnc3VuLicsICdtw6FuLicsICd0w71zLicsICdtaWsuJywgJ2jDs3MuJywgJ2Zyw60uJywgJ2xleS4nXSxcbiAgICBbXG4gICAgICAnc3VubnVkYWd1cicsICdtw6FuYWRhZ3VyJywgJ3TDvXNkYWd1cicsICdtaWt1ZGFndXInLCAnaMOzc2RhZ3VyJywgJ2Zyw61nZ2phZGFndXInLFxuICAgICAgJ2xleWdhcmRhZ3VyJ1xuICAgIF0sXG4gICAgWydzdS4nLCAnbcOhLicsICd0w70uJywgJ21pLicsICdow7MuJywgJ2ZyLicsICdsZS4nXVxuICBdLFxuICBbXG4gICAgWydTJywgJ00nLCAnVCcsICdNJywgJ0gnLCAnRicsICdMJ10sIFsnc3VuJywgJ23DoW4nLCAndMO9cycsICdtaWsnLCAnaMOzcycsICdmcsOtJywgJ2xleSddLFxuICAgIFtcbiAgICAgICdzdW5udWRhZ3VyJywgJ23DoW5hZGFndXInLCAndMO9c2RhZ3VyJywgJ21pa3VkYWd1cicsICdow7NzZGFndXInLCAnZnLDrWdnamFkYWd1cicsXG4gICAgICAnbGV5Z2FyZGFndXInXG4gICAgXSxcbiAgICBbJ3N1JywgJ23DoScsICd0w70nLCAnbWknLCAnaMOzJywgJ2ZyJywgJ2xlJ11cbiAgXSxcbiAgW1xuICAgIFsnSicsICdGJywgJ00nLCAnQScsICdNJywgJ0onLCAnSicsICdBJywgJ1MnLCAnTycsICdOJywgJ0QnXSxcbiAgICBbJ2phbi4nLCAnZmViLicsICdtYXIuJywgJ2Fwci4nLCAnbWFpJywgJ2p1bi4nLCAnanVsLicsICdhdWcuJywgJ3NlcC4nLCAnb2t0LicsICdub3YuJywgJ2Rlcy4nXSxcbiAgICBbXG4gICAgICAnamFudWFyJywgJ2ZlYnJ1YXInLCAnbWFycycsICdhcHLDrWwnLCAnbWFpJywgJ2p1bmknLCAnanVsaScsICdhdWd1c3QnLCAnc2VwdGVtYmVyJyxcbiAgICAgICdva3RvYmVyJywgJ25vdmVtYmVyJywgJ2Rlc2VtYmVyJ1xuICAgIF1cbiAgXSxcbiAgW1xuICAgIFsnSicsICdGJywgJ00nLCAnQScsICdNJywgJ0onLCAnSicsICdBJywgJ1MnLCAnTycsICdOJywgJ0QnXSxcbiAgICBbJ2phbicsICdmZWInLCAnbWFyJywgJ2FwcicsICdtYWknLCAnanVuJywgJ2p1bCcsICdhdWcnLCAnc2VwJywgJ29rdCcsICdub3YnLCAnZGVzJ10sXG4gICAgW1xuICAgICAgJ2phbnVhcicsICdmZWJydWFyJywgJ21hcnMnLCAnYXByw61sJywgJ21haScsICdqdW5pJywgJ2p1bGknLCAnYXVndXN0JywgJ3NlcHRlbWJlcicsXG4gICAgICAnb2t0b2JlcicsICdub3ZlbWJlcicsICdkZXNlbWJlcidcbiAgICBdXG4gIF0sXG4gIFtbJ2ZLcicsICdlS3InXSwgWydmLktyLicsICdlLktyLiddLCBbJ2Z5cmkgS3Jpc3QnLCAnZWZ0aXIgS3Jpc3QnXV0sIDEsIFs2LCAwXSxcbiAgWydkZC5NTS55eScsICdkZC5NTS55JywgJ2QuIE1NTU0geScsICdFRUVFLCBkLiBNTU1NIHknXSxcbiAgWydISDptbScsICdISDptbTpzcycsICdISDptbTpzcyB6JywgJ0hIOm1tOnNzIHp6enonXSwgWyd7MX0sIHswfScsIHUsICd7MX0gXFwna2xcXCcuIHswfScsIHVdLFxuICBbJywnLCAnLicsICc7JywgJyUnLCAnKycsICfiiJInLCAnRScsICfDlycsICfigLAnLCAn4oieJywgJ05hTicsICc6J10sXG4gIFsnIywjIzAuIyMjJywgJyMsIyMwwqAlJywgJyMsIyMwLjAwwqDCpCcsICcjRTAnXSwgJ2tyJywgJ2RvbnNrIGtyw7NuYScsXG4gIHsnREtLJzogWydrciddLCAnSlBZJzogWydKUMKlJywgJ8KlJ10sICdVU0QnOiBbJ1VTJCcsICckJ119LCBwbHVyYWxcbl07XG4iXX0=