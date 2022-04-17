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
        define("@angular/common/locales/kn", ["require", "exports"], factory);
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
        'kn', [['ಪೂ', 'ಅ'], ['ಪೂರ್ವಾಹ್ನ', 'ಅಪರಾಹ್ನ'], u],
        [['ಪೂರ್ವಾಹ್ನ', 'ಅಪರಾಹ್ನ'], u, u],
        [
            ['ಭಾ', 'ಸೋ', 'ಮಂ', 'ಬು', 'ಗು', 'ಶು', 'ಶ'],
            [
                'ಭಾನು', 'ಸೋಮ', 'ಮಂಗಳ', 'ಬುಧ', 'ಗುರು', 'ಶುಕ್ರ',
                'ಶನಿ'
            ],
            [
                'ಭಾನುವಾರ', 'ಸೋಮವಾರ', 'ಮಂಗಳವಾರ', 'ಬುಧವಾರ',
                'ಗುರುವಾರ', 'ಶುಕ್ರವಾರ', 'ಶನಿವಾರ'
            ],
            [
                'ಭಾನು', 'ಸೋಮ', 'ಮಂಗಳ', 'ಬುಧ', 'ಗುರು', 'ಶುಕ್ರ',
                'ಶನಿ'
            ]
        ],
        u,
        [
            [
                'ಜ', 'ಫೆ', 'ಮಾ', 'ಏ', 'ಮೇ', 'ಜೂ', 'ಜು', 'ಆ', 'ಸೆ', 'ಅ', 'ನ',
                'ಡಿ'
            ],
            [
                'ಜನವರಿ', 'ಫೆಬ್ರವರಿ', 'ಮಾರ್ಚ್', 'ಏಪ್ರಿ',
                'ಮೇ', 'ಜೂನ್', 'ಜುಲೈ', 'ಆಗ', 'ಸೆಪ್ಟೆಂ',
                'ಅಕ್ಟೋ', 'ನವೆಂ', 'ಡಿಸೆಂ'
            ],
            [
                'ಜನವರಿ', 'ಫೆಬ್ರವರಿ', 'ಮಾರ್ಚ್', 'ಏಪ್ರಿಲ್',
                'ಮೇ', 'ಜೂನ್', 'ಜುಲೈ', 'ಆಗಸ್ಟ್',
                'ಸೆಪ್ಟೆಂಬರ್', 'ಅಕ್ಟೋಬರ್', 'ನವೆಂಬರ್',
                'ಡಿಸೆಂಬರ್'
            ]
        ],
        [
            [
                'ಜ', 'ಫೆ', 'ಮಾ', 'ಏ', 'ಮೇ', 'ಜೂ', 'ಜು', 'ಆ', 'ಸೆ', 'ಅ', 'ನ',
                'ಡಿ'
            ],
            [
                'ಜನ', 'ಫೆಬ್ರ', 'ಮಾರ್ಚ್', 'ಏಪ್ರಿ', 'ಮೇ',
                'ಜೂನ್', 'ಜುಲೈ', 'ಆಗ', 'ಸೆಪ್ಟೆಂ', 'ಅಕ್ಟೋ',
                'ನವೆಂ', 'ಡಿಸೆಂ'
            ],
            [
                'ಜನವರಿ', 'ಫೆಬ್ರವರಿ', 'ಮಾರ್ಚ್', 'ಏಪ್ರಿಲ್',
                'ಮೇ', 'ಜೂನ್', 'ಜುಲೈ', 'ಆಗಸ್ಟ್',
                'ಸೆಪ್ಟೆಂಬರ್', 'ಅಕ್ಟೋಬರ್', 'ನವೆಂಬರ್',
                'ಡಿಸೆಂಬರ್'
            ]
        ],
        [
            ['ಕ್ರಿ.ಪೂ', 'ಕ್ರಿ.ಶ'], u,
            ['ಕ್ರಿಸ್ತ ಪೂರ್ವ', 'ಕ್ರಿಸ್ತ ಶಕ']
        ],
        0, [0, 0], ['d/M/yy', 'MMM d, y', 'MMMM d, y', 'EEEE, MMMM d, y'],
        ['hh:mm a', 'hh:mm:ss a', 'hh:mm:ss a z', 'hh:mm:ss a zzzz'], ['{1} {0}', u, u, u],
        ['.', ',', ';', '%', '+', '-', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##0.###', '#,##0%', '¤#,##0.00', '#E0'], '₹', 'ಭಾರತೀಯ ರೂಪಾಯಿ',
        { 'JPY': ['JP¥', '¥'], 'RON': [u, 'ಲೀ'], 'THB': ['฿'], 'TWD': ['NT$'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoia24uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy9rbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsSUFBSSxDQUFDLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDaEMsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDO1lBQUUsT0FBTyxDQUFDLENBQUM7UUFDakMsT0FBTyxDQUFDLENBQUM7SUFDWCxDQUFDO0lBRUQsa0JBQWU7UUFDYixJQUFJLEVBQUUsQ0FBQyxDQUFDLElBQUksRUFBRSxHQUFHLENBQUMsRUFBRSxDQUFDLFdBQVcsRUFBRSxTQUFTLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDaEQsQ0FBQyxDQUFDLFdBQVcsRUFBRSxTQUFTLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQ2hDO1lBQ0UsQ0FBQyxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxHQUFHLENBQUM7WUFDekM7Z0JBQ0UsTUFBTSxFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxPQUFPO2dCQUM3QyxLQUFLO2FBQ047WUFDRDtnQkFDRSxTQUFTLEVBQUUsUUFBUSxFQUFFLFNBQVMsRUFBRSxRQUFRO2dCQUN4QyxTQUFTLEVBQUUsVUFBVSxFQUFFLFFBQVE7YUFDaEM7WUFDRDtnQkFDRSxNQUFNLEVBQUUsS0FBSyxFQUFFLE1BQU0sRUFBRSxLQUFLLEVBQUUsTUFBTSxFQUFFLE9BQU87Z0JBQzdDLEtBQUs7YUFDTjtTQUNGO1FBQ0QsQ0FBQztRQUNEO1lBQ0U7Z0JBQ0UsR0FBRyxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsR0FBRyxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLEdBQUcsRUFBRSxJQUFJLEVBQUUsR0FBRyxFQUFFLEdBQUc7Z0JBQzNELElBQUk7YUFDTDtZQUNEO2dCQUNFLE9BQU8sRUFBRSxVQUFVLEVBQUUsUUFBUSxFQUFFLE9BQU87Z0JBQ3RDLElBQUksRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLElBQUksRUFBRSxTQUFTO2dCQUNyQyxPQUFPLEVBQUUsTUFBTSxFQUFFLE9BQU87YUFDekI7WUFDRDtnQkFDRSxPQUFPLEVBQUUsVUFBVSxFQUFFLFFBQVEsRUFBRSxTQUFTO2dCQUN4QyxJQUFJLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxRQUFRO2dCQUM5QixZQUFZLEVBQUUsVUFBVSxFQUFFLFNBQVM7Z0JBQ25DLFVBQVU7YUFDWDtTQUNGO1FBQ0Q7WUFDRTtnQkFDRSxHQUFHLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxHQUFHLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLEVBQUUsR0FBRyxFQUFFLElBQUksRUFBRSxHQUFHLEVBQUUsR0FBRztnQkFDM0QsSUFBSTthQUNMO1lBQ0Q7Z0JBQ0UsSUFBSSxFQUFFLE9BQU8sRUFBRSxRQUFRLEVBQUUsT0FBTyxFQUFFLElBQUk7Z0JBQ3RDLE1BQU0sRUFBRSxNQUFNLEVBQUUsSUFBSSxFQUFFLFNBQVMsRUFBRSxPQUFPO2dCQUN4QyxNQUFNLEVBQUUsT0FBTzthQUNoQjtZQUNEO2dCQUNFLE9BQU8sRUFBRSxVQUFVLEVBQUUsUUFBUSxFQUFFLFNBQVM7Z0JBQ3hDLElBQUksRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLFFBQVE7Z0JBQzlCLFlBQVksRUFBRSxVQUFVLEVBQUUsU0FBUztnQkFDbkMsVUFBVTthQUNYO1NBQ0Y7UUFDRDtZQUNFLENBQUMsU0FBUyxFQUFFLFFBQVEsQ0FBQyxFQUFFLENBQUM7WUFDeEIsQ0FBQyxlQUFlLEVBQUUsWUFBWSxDQUFDO1NBQ2hDO1FBQ0QsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsUUFBUSxFQUFFLFVBQVUsRUFBRSxXQUFXLEVBQUUsaUJBQWlCLENBQUM7UUFDakUsQ0FBQyxTQUFTLEVBQUUsWUFBWSxFQUFFLGNBQWMsRUFBRSxpQkFBaUIsQ0FBQyxFQUFFLENBQUMsU0FBUyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDO1FBQ2xGLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEtBQUssRUFBRSxHQUFHLENBQUM7UUFDOUQsQ0FBQyxXQUFXLEVBQUUsUUFBUSxFQUFFLFdBQVcsRUFBRSxLQUFLLENBQUMsRUFBRSxHQUFHLEVBQUUsZUFBZTtRQUNqRSxFQUFDLEtBQUssRUFBRSxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsRUFBRSxLQUFLLEVBQUUsQ0FBQyxDQUFDLEVBQUUsSUFBSSxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxDQUFDLEVBQUMsRUFBRSxNQUFNO0tBQzlFLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbi8vIFRISVMgQ09ERSBJUyBHRU5FUkFURUQgLSBETyBOT1QgTU9ESUZZXG4vLyBTZWUgYW5ndWxhci90b29scy9ndWxwLXRhc2tzL2NsZHIvZXh0cmFjdC5qc1xuXG5jb25zdCB1ID0gdW5kZWZpbmVkO1xuXG5mdW5jdGlvbiBwbHVyYWwobjogbnVtYmVyKTogbnVtYmVyIHtcbiAgbGV0IGkgPSBNYXRoLmZsb29yKE1hdGguYWJzKG4pKTtcbiAgaWYgKGkgPT09IDAgfHwgbiA9PT0gMSkgcmV0dXJuIDE7XG4gIHJldHVybiA1O1xufVxuXG5leHBvcnQgZGVmYXVsdCBbXG4gICdrbicsIFtbJ+CyquCzgicsICfgsoUnXSwgWyfgsqrgs4LgsrDgs43gsrXgsr7gsrngs43gsqgnLCAn4LKF4LKq4LKw4LK+4LK54LON4LKoJ10sIHVdLFxuICBbWyfgsqrgs4LgsrDgs43gsrXgsr7gsrngs43gsqgnLCAn4LKF4LKq4LKw4LK+4LK54LON4LKoJ10sIHUsIHVdLFxuICBbXG4gICAgWyfgsq3gsr4nLCAn4LK44LOLJywgJ+CyruCygicsICfgsqzgs4EnLCAn4LKX4LOBJywgJ+CytuCzgScsICfgsrYnXSxcbiAgICBbXG4gICAgICAn4LKt4LK+4LKo4LOBJywgJ+CyuOCzi+CyricsICfgsq7gsoLgspfgsrMnLCAn4LKs4LOB4LKnJywgJ+Cyl+CzgeCysOCzgScsICfgsrbgs4HgspXgs43gsrAnLFxuICAgICAgJ+CytuCyqOCyvydcbiAgICBdLFxuICAgIFtcbiAgICAgICfgsq3gsr7gsqjgs4HgsrXgsr7gsrAnLCAn4LK44LOL4LKu4LK14LK+4LKwJywgJ+CyruCyguCyl+Cys+CyteCyvuCysCcsICfgsqzgs4HgsqfgsrXgsr7gsrAnLFxuICAgICAgJ+Cyl+CzgeCysOCzgeCyteCyvuCysCcsICfgsrbgs4HgspXgs43gsrDgsrXgsr7gsrAnLCAn4LK24LKo4LK/4LK14LK+4LKwJ1xuICAgIF0sXG4gICAgW1xuICAgICAgJ+CyreCyvuCyqOCzgScsICfgsrjgs4vgsq4nLCAn4LKu4LKC4LKX4LKzJywgJ+CyrOCzgeCypycsICfgspfgs4HgsrDgs4EnLCAn4LK24LOB4LKV4LON4LKwJyxcbiAgICAgICfgsrbgsqjgsr8nXG4gICAgXVxuICBdLFxuICB1LFxuICBbXG4gICAgW1xuICAgICAgJ+CynCcsICfgsqvgs4YnLCAn4LKu4LK+JywgJ+CyjycsICfgsq7gs4cnLCAn4LKc4LOCJywgJ+CynOCzgScsICfgsoYnLCAn4LK44LOGJywgJ+CyhScsICfgsqgnLFxuICAgICAgJ+CyoeCyvydcbiAgICBdLFxuICAgIFtcbiAgICAgICfgspzgsqjgsrXgsrDgsr8nLCAn4LKr4LOG4LKs4LON4LKw4LK14LKw4LK/JywgJ+CyruCyvuCysOCzjeCymuCzjScsICfgso/gsqrgs43gsrDgsr8nLFxuICAgICAgJ+CyruCzhycsICfgspzgs4Lgsqjgs40nLCAn4LKc4LOB4LKy4LOIJywgJ+CyhuCylycsICfgsrjgs4bgsqrgs43gsp/gs4bgsoInLFxuICAgICAgJ+CyheCyleCzjeCyn+CziycsICfgsqjgsrXgs4bgsoInLCAn4LKh4LK/4LK44LOG4LKCJ1xuICAgIF0sXG4gICAgW1xuICAgICAgJ+CynOCyqOCyteCysOCyvycsICfgsqvgs4bgsqzgs43gsrDgsrXgsrDgsr8nLCAn4LKu4LK+4LKw4LON4LKa4LONJywgJ+Cyj+CyquCzjeCysOCyv+CysuCzjScsXG4gICAgICAn4LKu4LOHJywgJ+CynOCzguCyqOCzjScsICfgspzgs4HgsrLgs4gnLCAn4LKG4LKX4LK44LON4LKf4LONJyxcbiAgICAgICfgsrjgs4bgsqrgs43gsp/gs4bgsoLgsqzgsrDgs40nLCAn4LKF4LKV4LON4LKf4LOL4LKs4LKw4LONJywgJ+CyqOCyteCzhuCyguCyrOCysOCzjScsXG4gICAgICAn4LKh4LK/4LK44LOG4LKC4LKs4LKw4LONJ1xuICAgIF1cbiAgXSxcbiAgW1xuICAgIFtcbiAgICAgICfgspwnLCAn4LKr4LOGJywgJ+CyruCyvicsICfgso8nLCAn4LKu4LOHJywgJ+CynOCzgicsICfgspzgs4EnLCAn4LKGJywgJ+CyuOCzhicsICfgsoUnLCAn4LKoJyxcbiAgICAgICfgsqHgsr8nXG4gICAgXSxcbiAgICBbXG4gICAgICAn4LKc4LKoJywgJ+Cyq+CzhuCyrOCzjeCysCcsICfgsq7gsr7gsrDgs43gsprgs40nLCAn4LKP4LKq4LON4LKw4LK/JywgJ+CyruCzhycsXG4gICAgICAn4LKc4LOC4LKo4LONJywgJ+CynOCzgeCysuCziCcsICfgsobgspcnLCAn4LK44LOG4LKq4LON4LKf4LOG4LKCJywgJ+CyheCyleCzjeCyn+CziycsXG4gICAgICAn4LKo4LK14LOG4LKCJywgJ+CyoeCyv+CyuOCzhuCygidcbiAgICBdLFxuICAgIFtcbiAgICAgICfgspzgsqjgsrXgsrDgsr8nLCAn4LKr4LOG4LKs4LON4LKw4LK14LKw4LK/JywgJ+CyruCyvuCysOCzjeCymuCzjScsICfgso/gsqrgs43gsrDgsr/gsrLgs40nLFxuICAgICAgJ+CyruCzhycsICfgspzgs4Lgsqjgs40nLCAn4LKc4LOB4LKy4LOIJywgJ+CyhuCyl+CyuOCzjeCyn+CzjScsXG4gICAgICAn4LK44LOG4LKq4LON4LKf4LOG4LKC4LKs4LKw4LONJywgJ+CyheCyleCzjeCyn+Czi+CyrOCysOCzjScsICfgsqjgsrXgs4bgsoLgsqzgsrDgs40nLFxuICAgICAgJ+CyoeCyv+CyuOCzhuCyguCyrOCysOCzjSdcbiAgICBdXG4gIF0sXG4gIFtcbiAgICBbJ+CyleCzjeCysOCyvy7gsqrgs4InLCAn4LKV4LON4LKw4LK/LuCytiddLCB1LFxuICAgIFsn4LKV4LON4LKw4LK/4LK44LON4LKkIOCyquCzguCysOCzjeCytScsICfgspXgs43gsrDgsr/gsrjgs43gsqQg4LK24LKVJ11cbiAgXSxcbiAgMCwgWzAsIDBdLCBbJ2QvTS95eScsICdNTU0gZCwgeScsICdNTU1NIGQsIHknLCAnRUVFRSwgTU1NTSBkLCB5J10sXG4gIFsnaGg6bW0gYScsICdoaDptbTpzcyBhJywgJ2hoOm1tOnNzIGEgeicsICdoaDptbTpzcyBhIHp6enonXSwgWyd7MX0gezB9JywgdSwgdSwgdV0sXG4gIFsnLicsICcsJywgJzsnLCAnJScsICcrJywgJy0nLCAnRScsICfDlycsICfigLAnLCAn4oieJywgJ05hTicsICc6J10sXG4gIFsnIywjIzAuIyMjJywgJyMsIyMwJScsICfCpCMsIyMwLjAwJywgJyNFMCddLCAn4oK5JywgJ+CyreCyvuCysOCypOCzgOCyryDgsrDgs4Lgsqrgsr7gsq/gsr8nLFxuICB7J0pQWSc6IFsnSlDCpScsICfCpSddLCAnUk9OJzogW3UsICfgsrLgs4AnXSwgJ1RIQic6IFsn4Li/J10sICdUV0QnOiBbJ05UJCddfSwgcGx1cmFsXG5dO1xuIl19