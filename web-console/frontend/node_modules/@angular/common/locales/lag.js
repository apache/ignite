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
        define("@angular/common/locales/lag", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    // THIS CODE IS GENERATED - DO NOT MODIFY
    // See angular/tools/gulp-tasks/cldr/extract.js
    var u = undefined;
    function plural(n) {
        var i = Math.floor(Math.abs(n));
        if (n === 0)
            return 0;
        if ((i === 0 || i === 1) && !(n === 0))
            return 1;
        return 5;
    }
    exports.default = [
        'lag', [['TOO', 'MUU'], u, u], u,
        [
            ['P', 'T', 'E', 'O', 'A', 'I', 'M'],
            ['Píili', 'Táatu', 'Íne', 'Táano', 'Alh', 'Ijm', 'Móosi'],
            ['Jumapíiri', 'Jumatátu', 'Jumaíne', 'Jumatáano', 'Alamíisi', 'Ijumáa', 'Jumamóosi'],
            ['Píili', 'Táatu', 'Íne', 'Táano', 'Alh', 'Ijm', 'Móosi']
        ],
        u,
        [
            ['F', 'N', 'K', 'I', 'I', 'I', 'M', 'V', 'S', 'I', 'S', 'S'],
            [
                'Fúngatɨ', 'Naanɨ', 'Keenda', 'Ikúmi', 'Inyambala', 'Idwaata', 'Mʉʉnchɨ', 'Vɨɨrɨ',
                'Saatʉ', 'Inyi', 'Saano', 'Sasatʉ'
            ],
            [
                'Kʉfúngatɨ', 'Kʉnaanɨ', 'Kʉkeenda', 'Kwiikumi', 'Kwiinyambála', 'Kwiidwaata',
                'Kʉmʉʉnchɨ', 'Kʉvɨɨrɨ', 'Kʉsaatʉ', 'Kwiinyi', 'Kʉsaano', 'Kʉsasatʉ'
            ]
        ],
        u, [['KSA', 'KA'], u, ['Kɨrɨsitʉ sɨ anavyaal', 'Kɨrɨsitʉ akavyaalwe']], 1, [6, 0],
        ['dd/MM/y', 'd MMM y', 'd MMMM y', 'EEEE, d MMMM y'],
        ['HH:mm', 'HH:mm:ss', 'HH:mm:ss z', 'HH:mm:ss zzzz'], ['{1} {0}', u, u, u],
        ['.', ',', ';', '%', '+', '-', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##0.###', '#,##0%', '¤ #,##0.00', '#E0'], 'TSh', 'Shilíingi ya Taansanía',
        { 'JPY': ['JP¥', '¥'], 'TZS': ['TSh'], 'USD': ['US$', '$'] }, plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGFnLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tbW9uL2xvY2FsZXMvbGFnLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7Ozs7Ozs7Ozs7O0lBRUgseUNBQXlDO0lBQ3pDLCtDQUErQztJQUUvQyxJQUFNLENBQUMsR0FBRyxTQUFTLENBQUM7SUFFcEIsU0FBUyxNQUFNLENBQUMsQ0FBUztRQUN2QixJQUFJLENBQUMsR0FBRyxJQUFJLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNoQyxJQUFJLENBQUMsS0FBSyxDQUFDO1lBQUUsT0FBTyxDQUFDLENBQUM7UUFDdEIsSUFBSSxDQUFDLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDO1lBQUUsT0FBTyxDQUFDLENBQUM7UUFDakQsT0FBTyxDQUFDLENBQUM7SUFDWCxDQUFDO0lBRUQsa0JBQWU7UUFDYixLQUFLLEVBQUUsQ0FBQyxDQUFDLEtBQUssRUFBRSxLQUFLLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQztRQUNoQztZQUNFLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxDQUFDO1lBQ25DLENBQUMsT0FBTyxFQUFFLE9BQU8sRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsT0FBTyxDQUFDO1lBQ3pELENBQUMsV0FBVyxFQUFFLFVBQVUsRUFBRSxTQUFTLEVBQUUsV0FBVyxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQUUsV0FBVyxDQUFDO1lBQ3BGLENBQUMsT0FBTyxFQUFFLE9BQU8sRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsT0FBTyxDQUFDO1NBQzFEO1FBQ0QsQ0FBQztRQUNEO1lBQ0UsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsQ0FBQztZQUM1RDtnQkFDRSxTQUFTLEVBQUUsT0FBTyxFQUFFLFFBQVEsRUFBRSxPQUFPLEVBQUUsV0FBVyxFQUFFLFNBQVMsRUFBRSxTQUFTLEVBQUUsT0FBTztnQkFDakYsT0FBTyxFQUFFLE1BQU0sRUFBRSxPQUFPLEVBQUUsUUFBUTthQUNuQztZQUNEO2dCQUNFLFdBQVcsRUFBRSxTQUFTLEVBQUUsVUFBVSxFQUFFLFVBQVUsRUFBRSxjQUFjLEVBQUUsWUFBWTtnQkFDNUUsV0FBVyxFQUFFLFNBQVMsRUFBRSxTQUFTLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxVQUFVO2FBQ3BFO1NBQ0Y7UUFDRCxDQUFDLEVBQUUsQ0FBQyxDQUFDLEtBQUssRUFBRSxJQUFJLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxzQkFBc0IsRUFBRSxxQkFBcUIsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsQ0FBQztRQUNqRixDQUFDLFNBQVMsRUFBRSxTQUFTLEVBQUUsVUFBVSxFQUFFLGdCQUFnQixDQUFDO1FBQ3BELENBQUMsT0FBTyxFQUFFLFVBQVUsRUFBRSxZQUFZLEVBQUUsZUFBZSxDQUFDLEVBQUUsQ0FBQyxTQUFTLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDMUUsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsS0FBSyxFQUFFLEdBQUcsQ0FBQztRQUM5RCxDQUFDLFdBQVcsRUFBRSxRQUFRLEVBQUUsWUFBWSxFQUFFLEtBQUssQ0FBQyxFQUFFLEtBQUssRUFBRSx3QkFBd0I7UUFDN0UsRUFBQyxLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxDQUFDLEVBQUUsS0FBSyxFQUFFLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQyxFQUFDLEVBQUUsTUFBTTtLQUNuRSxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG4vLyBUSElTIENPREUgSVMgR0VORVJBVEVEIC0gRE8gTk9UIE1PRElGWVxuLy8gU2VlIGFuZ3VsYXIvdG9vbHMvZ3VscC10YXNrcy9jbGRyL2V4dHJhY3QuanNcblxuY29uc3QgdSA9IHVuZGVmaW5lZDtcblxuZnVuY3Rpb24gcGx1cmFsKG46IG51bWJlcik6IG51bWJlciB7XG4gIGxldCBpID0gTWF0aC5mbG9vcihNYXRoLmFicyhuKSk7XG4gIGlmIChuID09PSAwKSByZXR1cm4gMDtcbiAgaWYgKChpID09PSAwIHx8IGkgPT09IDEpICYmICEobiA9PT0gMCkpIHJldHVybiAxO1xuICByZXR1cm4gNTtcbn1cblxuZXhwb3J0IGRlZmF1bHQgW1xuICAnbGFnJywgW1snVE9PJywgJ01VVSddLCB1LCB1XSwgdSxcbiAgW1xuICAgIFsnUCcsICdUJywgJ0UnLCAnTycsICdBJywgJ0knLCAnTSddLFxuICAgIFsnUMOtaWxpJywgJ1TDoWF0dScsICfDjW5lJywgJ1TDoWFubycsICdBbGgnLCAnSWptJywgJ03Ds29zaSddLFxuICAgIFsnSnVtYXDDrWlyaScsICdKdW1hdMOhdHUnLCAnSnVtYcOtbmUnLCAnSnVtYXTDoWFubycsICdBbGFtw61pc2knLCAnSWp1bcOhYScsICdKdW1hbcOzb3NpJ10sXG4gICAgWydQw61pbGknLCAnVMOhYXR1JywgJ8ONbmUnLCAnVMOhYW5vJywgJ0FsaCcsICdJam0nLCAnTcOzb3NpJ11cbiAgXSxcbiAgdSxcbiAgW1xuICAgIFsnRicsICdOJywgJ0snLCAnSScsICdJJywgJ0knLCAnTScsICdWJywgJ1MnLCAnSScsICdTJywgJ1MnXSxcbiAgICBbXG4gICAgICAnRsO6bmdhdMmoJywgJ05hYW7JqCcsICdLZWVuZGEnLCAnSWvDum1pJywgJ0lueWFtYmFsYScsICdJZHdhYXRhJywgJ03KicqJbmNoyagnLCAnVsmoyahyyagnLFxuICAgICAgJ1NhYXTKiScsICdJbnlpJywgJ1NhYW5vJywgJ1Nhc2F0yoknXG4gICAgXSxcbiAgICBbXG4gICAgICAnS8qJZsO6bmdhdMmoJywgJ0vKiW5hYW7JqCcsICdLyolrZWVuZGEnLCAnS3dpaWt1bWknLCAnS3dpaW55YW1iw6FsYScsICdLd2lpZHdhYXRhJyxcbiAgICAgICdLyoltyonKiW5jaMmoJywgJ0vKiXbJqMmocsmoJywgJ0vKiXNhYXTKiScsICdLd2lpbnlpJywgJ0vKiXNhYW5vJywgJ0vKiXNhc2F0yoknXG4gICAgXVxuICBdLFxuICB1LCBbWydLU0EnLCAnS0EnXSwgdSwgWydLyahyyahzaXTKiSBzyaggYW5hdnlhYWwnLCAnS8mocsmoc2l0yokgYWthdnlhYWx3ZSddXSwgMSwgWzYsIDBdLFxuICBbJ2RkL01NL3knLCAnZCBNTU0geScsICdkIE1NTU0geScsICdFRUVFLCBkIE1NTU0geSddLFxuICBbJ0hIOm1tJywgJ0hIOm1tOnNzJywgJ0hIOm1tOnNzIHonLCAnSEg6bW06c3Mgenp6eiddLCBbJ3sxfSB7MH0nLCB1LCB1LCB1XSxcbiAgWycuJywgJywnLCAnOycsICclJywgJysnLCAnLScsICdFJywgJ8OXJywgJ+KAsCcsICfiiJ4nLCAnTmFOJywgJzonXSxcbiAgWycjLCMjMC4jIyMnLCAnIywjIzAlJywgJ8KkwqAjLCMjMC4wMCcsICcjRTAnXSwgJ1RTaCcsICdTaGlsw61pbmdpIHlhIFRhYW5zYW7DrWEnLFxuICB7J0pQWSc6IFsnSlDCpScsICfCpSddLCAnVFpTJzogWydUU2gnXSwgJ1VTRCc6IFsnVVMkJywgJyQnXX0sIHBsdXJhbFxuXTtcbiJdfQ==