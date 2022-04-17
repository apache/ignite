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
        define("@angular/common/locales/dz", ["require", "exports"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    // THIS CODE IS GENERATED - DO NOT MODIFY
    // See angular/tools/gulp-tasks/cldr/extract.js
    var u = undefined;
    function plural(n) {
        return 5;
    }
    exports.default = [
        'dz', [['སྔ་ཆ་', 'ཕྱི་ཆ་'], u, u], u,
        [
            ['ཟླ', 'མིར', 'ལྷག', 'ཕུར', 'སངྶ', 'སྤེན', 'ཉི'],
            [
                'ཟླ་', 'མིར་', 'ལྷག་', 'ཕུར་', 'སངས་',
                'སྤེན་', 'ཉི་'
            ],
            [
                'གཟའ་ཟླ་བ་', 'གཟའ་མིག་དམར་',
                'གཟའ་ལྷག་པ་', 'གཟའ་ཕུར་བུ་',
                'གཟའ་པ་སངས་', 'གཟའ་སྤེན་པ་',
                'གཟའ་ཉི་མ་'
            ],
            [
                'ཟླ་', 'མིར་', 'ལྷག་', 'ཕུར་', 'སངས་',
                'སྤེན་', 'ཉི་'
            ]
        ],
        u,
        [
            ['༡', '༢', '༣', '4', '༥', '༦', '༧', '༨', '9', '༡༠', '༡༡', '༡༢'],
            ['༡', '༢', '༣', '༤', '༥', '༦', '༧', '༨', '༩', '༡༠', '༡༡', '12'],
            [
                'ཟླ་དངཔ་', 'ཟླ་གཉིས་པ་', 'ཟླ་གསུམ་པ་',
                'ཟླ་བཞི་པ་', 'ཟླ་ལྔ་པ་', 'ཟླ་དྲུག་པ',
                'ཟླ་བདུན་པ་', 'ཟླ་བརྒྱད་པ་',
                'ཟླ་དགུ་པ་', 'ཟླ་བཅུ་པ་',
                'ཟླ་བཅུ་གཅིག་པ་', 'ཟླ་བཅུ་གཉིས་པ་'
            ]
        ],
        [
            ['༡', '༢', '༣', '༤', '༥', '༦', '༧', '༨', '༩', '༡༠', '༡༡', '༡༢'],
            [
                'ཟླ་༡', 'ཟླ་༢', 'ཟླ་༣', 'ཟླ་༤', 'ཟླ་༥',
                'ཟླ་༦', 'ཟླ་༧', 'ཟླ་༨', 'ཟླ་༩', 'ཟླ་༡༠',
                'ཟླ་༡༡', 'ཟླ་༡༢'
            ],
            [
                'སྤྱི་ཟླ་དངཔ་', 'སྤྱི་ཟླ་གཉིས་པ་',
                'སྤྱི་ཟླ་གསུམ་པ་', 'སྤྱི་ཟླ་བཞི་པ',
                'སྤྱི་ཟླ་ལྔ་པ་', 'སྤྱི་ཟླ་དྲུག་པ',
                'སྤྱི་ཟླ་བདུན་པ་',
                'སྤྱི་ཟླ་བརྒྱད་པ་',
                'སྤྱི་ཟླ་དགུ་པ་', 'སྤྱི་ཟླ་བཅུ་པ་',
                'སྤྱི་ཟླ་བཅུ་གཅིག་པ་',
                'སྤྱི་ཟླ་བཅུ་གཉིས་པ་'
            ]
        ],
        [['BCE', 'CE'], u, u], 0, [6, 0],
        [
            'y-MM-dd', 'སྤྱི་ལོ་y ཟླ་MMM ཚེས་dd',
            'སྤྱི་ལོ་y MMMM ཚེས་ dd',
            'EEEE, སྤྱི་ལོ་y MMMM ཚེས་dd'
        ],
        [
            'ཆུ་ཚོད་ h སྐར་མ་ mm a', 'ཆུ་ཚོད་h:mm:ss a',
            'ཆུ་ཚོད་ h སྐར་མ་ mm:ss a z',
            'ཆུ་ཚོད་ h སྐར་མ་ mm:ss a zzzz'
        ],
        ['{1} {0}', u, u, u], ['.', ',', ';', '%', '+', '-', 'E', '×', '‰', '∞', 'NaN', ':'],
        ['#,##,##0.###', '#,##,##0 %', '¤#,##,##0.00', '#E0'], '₹',
        'རྒྱ་གར་གྱི་དངུལ་ རུ་པི', {
            'AUD': ['AU$', '$'],
            'BTN': ['Nu.'],
            'ILS': [u, '₪'],
            'JPY': ['JP¥', '¥'],
            'KRW': ['KR₩', '₩'],
            'THB': ['TH฿', '฿'],
            'USD': ['US$', '$'],
            'XAF': []
        },
        plural
    ];
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZHouanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21tb24vbG9jYWxlcy9kei50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILHlDQUF5QztJQUN6QywrQ0FBK0M7SUFFL0MsSUFBTSxDQUFDLEdBQUcsU0FBUyxDQUFDO0lBRXBCLFNBQVMsTUFBTSxDQUFDLENBQVM7UUFDdkIsT0FBTyxDQUFDLENBQUM7SUFDWCxDQUFDO0lBRUQsa0JBQWU7UUFDYixJQUFJLEVBQUUsQ0FBQyxDQUFDLE9BQU8sRUFBRSxRQUFRLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQztRQUNwQztZQUNFLENBQUMsSUFBSSxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFFLEtBQUssRUFBRSxNQUFNLEVBQUUsSUFBSSxDQUFDO1lBQ2hEO2dCQUNFLEtBQUssRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxNQUFNO2dCQUNyQyxPQUFPLEVBQUUsS0FBSzthQUNmO1lBQ0Q7Z0JBQ0UsV0FBVyxFQUFFLGNBQWM7Z0JBQzNCLFlBQVksRUFBRSxhQUFhO2dCQUMzQixZQUFZLEVBQUUsYUFBYTtnQkFDM0IsV0FBVzthQUNaO1lBQ0Q7Z0JBQ0UsS0FBSyxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU07Z0JBQ3JDLE9BQU8sRUFBRSxLQUFLO2FBQ2Y7U0FDRjtRQUNELENBQUM7UUFDRDtZQUNFLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsSUFBSSxFQUFFLElBQUksRUFBRSxJQUFJLENBQUM7WUFDL0QsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksQ0FBQztZQUMvRDtnQkFDRSxTQUFTLEVBQUUsWUFBWSxFQUFFLFlBQVk7Z0JBQ3JDLFdBQVcsRUFBRSxVQUFVLEVBQUUsV0FBVztnQkFDcEMsWUFBWSxFQUFFLGFBQWE7Z0JBQzNCLFdBQVcsRUFBRSxXQUFXO2dCQUN4QixnQkFBZ0IsRUFBRSxnQkFBZ0I7YUFDbkM7U0FDRjtRQUNEO1lBQ0UsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxFQUFFLElBQUksQ0FBQztZQUMvRDtnQkFDRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTTtnQkFDdEMsTUFBTSxFQUFFLE1BQU0sRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFFLE9BQU87Z0JBQ3ZDLE9BQU8sRUFBRSxPQUFPO2FBQ2pCO1lBQ0Q7Z0JBQ0UsY0FBYyxFQUFFLGlCQUFpQjtnQkFDakMsaUJBQWlCLEVBQUUsZUFBZTtnQkFDbEMsZUFBZSxFQUFFLGdCQUFnQjtnQkFDakMsaUJBQWlCO2dCQUNqQixrQkFBa0I7Z0JBQ2xCLGdCQUFnQixFQUFFLGdCQUFnQjtnQkFDbEMscUJBQXFCO2dCQUNyQixxQkFBcUI7YUFDdEI7U0FDRjtRQUNELENBQUMsQ0FBQyxLQUFLLEVBQUUsSUFBSSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDaEM7WUFDRSxTQUFTLEVBQUUseUJBQXlCO1lBQ3BDLHdCQUF3QjtZQUN4Qiw2QkFBNkI7U0FDOUI7UUFDRDtZQUNFLHVCQUF1QixFQUFFLGtCQUFrQjtZQUMzQyw0QkFBNEI7WUFDNUIsK0JBQStCO1NBQ2hDO1FBQ0QsQ0FBQyxTQUFTLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsRUFBRSxLQUFLLEVBQUUsR0FBRyxDQUFDO1FBQ3BGLENBQUMsY0FBYyxFQUFFLFlBQVksRUFBRSxjQUFjLEVBQUUsS0FBSyxDQUFDLEVBQUUsR0FBRztRQUMxRCx3QkFBd0IsRUFBRTtZQUN4QixLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDO1lBQ25CLEtBQUssRUFBRSxDQUFDLEtBQUssQ0FBQztZQUNkLEtBQUssRUFBRSxDQUFDLENBQUMsRUFBRSxHQUFHLENBQUM7WUFDZixLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDO1lBQ25CLEtBQUssRUFBRSxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUM7WUFDbkIsS0FBSyxFQUFFLENBQUMsS0FBSyxFQUFFLEdBQUcsQ0FBQztZQUNuQixLQUFLLEVBQUUsQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDO1lBQ25CLEtBQUssRUFBRSxFQUFFO1NBQ1Y7UUFDRCxNQUFNO0tBQ1AsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGxpY2Vuc2VcbiAqIENvcHlyaWdodCBHb29nbGUgSW5jLiBBbGwgUmlnaHRzIFJlc2VydmVkLlxuICpcbiAqIFVzZSBvZiB0aGlzIHNvdXJjZSBjb2RlIGlzIGdvdmVybmVkIGJ5IGFuIE1JVC1zdHlsZSBsaWNlbnNlIHRoYXQgY2FuIGJlXG4gKiBmb3VuZCBpbiB0aGUgTElDRU5TRSBmaWxlIGF0IGh0dHBzOi8vYW5ndWxhci5pby9saWNlbnNlXG4gKi9cblxuLy8gVEhJUyBDT0RFIElTIEdFTkVSQVRFRCAtIERPIE5PVCBNT0RJRllcbi8vIFNlZSBhbmd1bGFyL3Rvb2xzL2d1bHAtdGFza3MvY2xkci9leHRyYWN0LmpzXG5cbmNvbnN0IHUgPSB1bmRlZmluZWQ7XG5cbmZ1bmN0aW9uIHBsdXJhbChuOiBudW1iZXIpOiBudW1iZXIge1xuICByZXR1cm4gNTtcbn1cblxuZXhwb3J0IGRlZmF1bHQgW1xuICAnZHonLCBbWyfgvabgvpTgvIvgvYbgvIsnLCAn4L2V4L6x4L2y4LyL4L2G4LyLJ10sIHUsIHVdLCB1LFxuICBbXG4gICAgWyfgvZ/gvrMnLCAn4L2Y4L2y4L2iJywgJ+C9o+C+t+C9gicsICfgvZXgvbTgvaInLCAn4L2m4L2E4L62JywgJ+C9puC+pOC9uuC9kycsICfgvYngvbInXSxcbiAgICBbXG4gICAgICAn4L2f4L6z4LyLJywgJ+C9mOC9suC9ouC8iycsICfgvaPgvrfgvYLgvIsnLCAn4L2V4L204L2i4LyLJywgJ+C9puC9hOC9puC8iycsXG4gICAgICAn4L2m4L6k4L264L2T4LyLJywgJ+C9ieC9suC8iydcbiAgICBdLFxuICAgIFtcbiAgICAgICfgvYLgvZ/gvaDgvIvgvZ/gvrPgvIvgvZbgvIsnLCAn4L2C4L2f4L2g4LyL4L2Y4L2y4L2C4LyL4L2R4L2Y4L2i4LyLJyxcbiAgICAgICfgvYLgvZ/gvaDgvIvgvaPgvrfgvYLgvIvgvZTgvIsnLCAn4L2C4L2f4L2g4LyL4L2V4L204L2i4LyL4L2W4L204LyLJyxcbiAgICAgICfgvYLgvZ/gvaDgvIvgvZTgvIvgvabgvYTgvabgvIsnLCAn4L2C4L2f4L2g4LyL4L2m4L6k4L264L2T4LyL4L2U4LyLJyxcbiAgICAgICfgvYLgvZ/gvaDgvIvgvYngvbLgvIvgvZjgvIsnXG4gICAgXSxcbiAgICBbXG4gICAgICAn4L2f4L6z4LyLJywgJ+C9mOC9suC9ouC8iycsICfgvaPgvrfgvYLgvIsnLCAn4L2V4L204L2i4LyLJywgJ+C9puC9hOC9puC8iycsXG4gICAgICAn4L2m4L6k4L264L2T4LyLJywgJ+C9ieC9suC8iydcbiAgICBdXG4gIF0sXG4gIHUsXG4gIFtcbiAgICBbJ+C8oScsICfgvKInLCAn4LyjJywgJzQnLCAn4LylJywgJ+C8picsICfgvKcnLCAn4LyoJywgJzknLCAn4Lyh4LygJywgJ+C8oeC8oScsICfgvKHgvKInXSxcbiAgICBbJ+C8oScsICfgvKInLCAn4LyjJywgJ+C8pCcsICfgvKUnLCAn4LymJywgJ+C8pycsICfgvKgnLCAn4LypJywgJ+C8oeC8oCcsICfgvKHgvKEnLCAnMTInXSxcbiAgICBbXG4gICAgICAn4L2f4L6z4LyL4L2R4L2E4L2U4LyLJywgJ+C9n+C+s+C8i+C9guC9ieC9suC9puC8i+C9lOC8iycsICfgvZ/gvrPgvIvgvYLgvabgvbTgvZjgvIvgvZTgvIsnLFxuICAgICAgJ+C9n+C+s+C8i+C9luC9nuC9suC8i+C9lOC8iycsICfgvZ/gvrPgvIvgvaPgvpTgvIvgvZTgvIsnLCAn4L2f4L6z4LyL4L2R4L6y4L204L2C4LyL4L2UJyxcbiAgICAgICfgvZ/gvrPgvIvgvZbgvZHgvbTgvZPgvIvgvZTgvIsnLCAn4L2f4L6z4LyL4L2W4L2i4L6S4L6x4L2R4LyL4L2U4LyLJyxcbiAgICAgICfgvZ/gvrPgvIvgvZHgvYLgvbTgvIvgvZTgvIsnLCAn4L2f4L6z4LyL4L2W4L2F4L204LyL4L2U4LyLJyxcbiAgICAgICfgvZ/gvrPgvIvgvZbgvYXgvbTgvIvgvYLgvYXgvbLgvYLgvIvgvZTgvIsnLCAn4L2f4L6z4LyL4L2W4L2F4L204LyL4L2C4L2J4L2y4L2m4LyL4L2U4LyLJ1xuICAgIF1cbiAgXSxcbiAgW1xuICAgIFsn4LyhJywgJ+C8oicsICfgvKMnLCAn4LykJywgJ+C8pScsICfgvKYnLCAn4LynJywgJ+C8qCcsICfgvKknLCAn4Lyh4LygJywgJ+C8oeC8oScsICfgvKHgvKInXSxcbiAgICBbXG4gICAgICAn4L2f4L6z4LyL4LyhJywgJ+C9n+C+s+C8i+C8oicsICfgvZ/gvrPgvIvgvKMnLCAn4L2f4L6z4LyL4LykJywgJ+C9n+C+s+C8i+C8pScsXG4gICAgICAn4L2f4L6z4LyL4LymJywgJ+C9n+C+s+C8i+C8pycsICfgvZ/gvrPgvIvgvKgnLCAn4L2f4L6z4LyL4LypJywgJ+C9n+C+s+C8i+C8oeC8oCcsXG4gICAgICAn4L2f4L6z4LyL4Lyh4LyhJywgJ+C9n+C+s+C8i+C8oeC8oidcbiAgICBdLFxuICAgIFtcbiAgICAgICfgvabgvqTgvrHgvbLgvIvgvZ/gvrPgvIvgvZHgvYTgvZTgvIsnLCAn4L2m4L6k4L6x4L2y4LyL4L2f4L6z4LyL4L2C4L2J4L2y4L2m4LyL4L2U4LyLJyxcbiAgICAgICfgvabgvqTgvrHgvbLgvIvgvZ/gvrPgvIvgvYLgvabgvbTgvZjgvIvgvZTgvIsnLCAn4L2m4L6k4L6x4L2y4LyL4L2f4L6z4LyL4L2W4L2e4L2y4LyL4L2UJyxcbiAgICAgICfgvabgvqTgvrHgvbLgvIvgvZ/gvrPgvIvgvaPgvpTgvIvgvZTgvIsnLCAn4L2m4L6k4L6x4L2y4LyL4L2f4L6z4LyL4L2R4L6y4L204L2C4LyL4L2UJyxcbiAgICAgICfgvabgvqTgvrHgvbLgvIvgvZ/gvrPgvIvgvZbgvZHgvbTgvZPgvIvgvZTgvIsnLFxuICAgICAgJ+C9puC+pOC+seC9suC8i+C9n+C+s+C8i+C9luC9ouC+kuC+seC9keC8i+C9lOC8iycsXG4gICAgICAn4L2m4L6k4L6x4L2y4LyL4L2f4L6z4LyL4L2R4L2C4L204LyL4L2U4LyLJywgJ+C9puC+pOC+seC9suC8i+C9n+C+s+C8i+C9luC9heC9tOC8i+C9lOC8iycsXG4gICAgICAn4L2m4L6k4L6x4L2y4LyL4L2f4L6z4LyL4L2W4L2F4L204LyL4L2C4L2F4L2y4L2C4LyL4L2U4LyLJyxcbiAgICAgICfgvabgvqTgvrHgvbLgvIvgvZ/gvrPgvIvgvZbgvYXgvbTgvIvgvYLgvYngvbLgvabgvIvgvZTgvIsnXG4gICAgXVxuICBdLFxuICBbWydCQ0UnLCAnQ0UnXSwgdSwgdV0sIDAsIFs2LCAwXSxcbiAgW1xuICAgICd5LU1NLWRkJywgJ+C9puC+pOC+seC9suC8i+C9o+C9vOC8i3kg4L2f4L6z4LyLTU1NIOC9muC9uuC9puC8i2RkJyxcbiAgICAn4L2m4L6k4L6x4L2y4LyL4L2j4L284LyLeSBNTU1NIOC9muC9uuC9puC8iyBkZCcsXG4gICAgJ0VFRUUsIOC9puC+pOC+seC9suC8i+C9o+C9vOC8i3kgTU1NTSDgvZrgvbrgvabgvItkZCdcbiAgXSxcbiAgW1xuICAgICfgvYbgvbTgvIvgvZrgvbzgvZHgvIsgaCDgvabgvpDgvaLgvIvgvZjgvIsgbW0gYScsICfgvYbgvbTgvIvgvZrgvbzgvZHgvItoOm1tOnNzIGEnLFxuICAgICfgvYbgvbTgvIvgvZrgvbzgvZHgvIsgaCDgvabgvpDgvaLgvIvgvZjgvIsgbW06c3MgYSB6JyxcbiAgICAn4L2G4L204LyL4L2a4L284L2R4LyLIGgg4L2m4L6Q4L2i4LyL4L2Y4LyLIG1tOnNzIGEgenp6eidcbiAgXSxcbiAgWyd7MX0gezB9JywgdSwgdSwgdV0sIFsnLicsICcsJywgJzsnLCAnJScsICcrJywgJy0nLCAnRScsICfDlycsICfigLAnLCAn4oieJywgJ05hTicsICc6J10sXG4gIFsnIywjIywjIzAuIyMjJywgJyMsIyMsIyMwwqAlJywgJ8KkIywjIywjIzAuMDAnLCAnI0UwJ10sICfigrknLFxuICAn4L2i4L6S4L6x4LyL4L2C4L2i4LyL4L2C4L6x4L2y4LyL4L2R4L2E4L204L2j4LyLIOC9ouC9tOC8i+C9lOC9sicsIHtcbiAgICAnQVVEJzogWydBVSQnLCAnJCddLFxuICAgICdCVE4nOiBbJ051LiddLFxuICAgICdJTFMnOiBbdSwgJ+KCqiddLFxuICAgICdKUFknOiBbJ0pQwqUnLCAnwqUnXSxcbiAgICAnS1JXJzogWydLUuKCqScsICfigqknXSxcbiAgICAnVEhCJzogWydUSOC4vycsICfguL8nXSxcbiAgICAnVVNEJzogWydVUyQnLCAnJCddLFxuICAgICdYQUYnOiBbXVxuICB9LFxuICBwbHVyYWxcbl07XG4iXX0=