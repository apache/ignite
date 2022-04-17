/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { setTestabilityGetter, ɵglobal as global } from '@angular/core';
import { getDOM } from '../dom/dom_adapter';
var BrowserGetTestability = /** @class */ (function () {
    function BrowserGetTestability() {
    }
    BrowserGetTestability.init = function () { setTestabilityGetter(new BrowserGetTestability()); };
    BrowserGetTestability.prototype.addToWindow = function (registry) {
        global['getAngularTestability'] = function (elem, findInAncestors) {
            if (findInAncestors === void 0) { findInAncestors = true; }
            var testability = registry.findTestabilityInTree(elem, findInAncestors);
            if (testability == null) {
                throw new Error('Could not find testability for element.');
            }
            return testability;
        };
        global['getAllAngularTestabilities'] = function () { return registry.getAllTestabilities(); };
        global['getAllAngularRootElements'] = function () { return registry.getAllRootElements(); };
        var whenAllStable = function (callback /** TODO #9100 */) {
            var testabilities = global['getAllAngularTestabilities']();
            var count = testabilities.length;
            var didWork = false;
            var decrement = function (didWork_ /** TODO #9100 */) {
                didWork = didWork || didWork_;
                count--;
                if (count == 0) {
                    callback(didWork);
                }
            };
            testabilities.forEach(function (testability /** TODO #9100 */) {
                testability.whenStable(decrement);
            });
        };
        if (!global['frameworkStabilizers']) {
            global['frameworkStabilizers'] = [];
        }
        global['frameworkStabilizers'].push(whenAllStable);
    };
    BrowserGetTestability.prototype.findTestabilityInTree = function (registry, elem, findInAncestors) {
        if (elem == null) {
            return null;
        }
        var t = registry.getTestability(elem);
        if (t != null) {
            return t;
        }
        else if (!findInAncestors) {
            return null;
        }
        if (getDOM().isShadowRoot(elem)) {
            return this.findTestabilityInTree(registry, getDOM().getHost(elem), true);
        }
        return this.findTestabilityInTree(registry, getDOM().parentElement(elem), true);
    };
    return BrowserGetTestability;
}());
export { BrowserGetTestability };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGVzdGFiaWxpdHkuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9wbGF0Zm9ybS1icm93c2VyL3NyYy9icm93c2VyL3Rlc3RhYmlsaXR5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILE9BQU8sRUFBbUQsb0JBQW9CLEVBQUUsT0FBTyxJQUFJLE1BQU0sRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUV4SCxPQUFPLEVBQUMsTUFBTSxFQUFDLE1BQU0sb0JBQW9CLENBQUM7QUFFMUM7SUFBQTtJQXNEQSxDQUFDO0lBckRRLDBCQUFJLEdBQVgsY0FBZ0Isb0JBQW9CLENBQUMsSUFBSSxxQkFBcUIsRUFBRSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBRXBFLDJDQUFXLEdBQVgsVUFBWSxRQUE2QjtRQUN2QyxNQUFNLENBQUMsdUJBQXVCLENBQUMsR0FBRyxVQUFDLElBQVMsRUFBRSxlQUErQjtZQUEvQixnQ0FBQSxFQUFBLHNCQUErQjtZQUMzRSxJQUFNLFdBQVcsR0FBRyxRQUFRLENBQUMscUJBQXFCLENBQUMsSUFBSSxFQUFFLGVBQWUsQ0FBQyxDQUFDO1lBQzFFLElBQUksV0FBVyxJQUFJLElBQUksRUFBRTtnQkFDdkIsTUFBTSxJQUFJLEtBQUssQ0FBQyx5Q0FBeUMsQ0FBQyxDQUFDO2FBQzVEO1lBQ0QsT0FBTyxXQUFXLENBQUM7UUFDckIsQ0FBQyxDQUFDO1FBRUYsTUFBTSxDQUFDLDRCQUE0QixDQUFDLEdBQUcsY0FBTSxPQUFBLFFBQVEsQ0FBQyxtQkFBbUIsRUFBRSxFQUE5QixDQUE4QixDQUFDO1FBRTVFLE1BQU0sQ0FBQywyQkFBMkIsQ0FBQyxHQUFHLGNBQU0sT0FBQSxRQUFRLENBQUMsa0JBQWtCLEVBQUUsRUFBN0IsQ0FBNkIsQ0FBQztRQUUxRSxJQUFNLGFBQWEsR0FBRyxVQUFDLFFBQWEsQ0FBQyxpQkFBaUI7WUFDcEQsSUFBTSxhQUFhLEdBQUcsTUFBTSxDQUFDLDRCQUE0QixDQUFDLEVBQUUsQ0FBQztZQUM3RCxJQUFJLEtBQUssR0FBRyxhQUFhLENBQUMsTUFBTSxDQUFDO1lBQ2pDLElBQUksT0FBTyxHQUFHLEtBQUssQ0FBQztZQUNwQixJQUFNLFNBQVMsR0FBRyxVQUFTLFFBQWEsQ0FBQyxpQkFBaUI7Z0JBQ3hELE9BQU8sR0FBRyxPQUFPLElBQUksUUFBUSxDQUFDO2dCQUM5QixLQUFLLEVBQUUsQ0FBQztnQkFDUixJQUFJLEtBQUssSUFBSSxDQUFDLEVBQUU7b0JBQ2QsUUFBUSxDQUFDLE9BQU8sQ0FBQyxDQUFDO2lCQUNuQjtZQUNILENBQUMsQ0FBQztZQUNGLGFBQWEsQ0FBQyxPQUFPLENBQUMsVUFBUyxXQUFnQixDQUFDLGlCQUFpQjtnQkFDL0QsV0FBVyxDQUFDLFVBQVUsQ0FBQyxTQUFTLENBQUMsQ0FBQztZQUNwQyxDQUFDLENBQUMsQ0FBQztRQUNMLENBQUMsQ0FBQztRQUVGLElBQUksQ0FBQyxNQUFNLENBQUMsc0JBQXNCLENBQUMsRUFBRTtZQUNuQyxNQUFNLENBQUMsc0JBQXNCLENBQUMsR0FBRyxFQUFFLENBQUM7U0FDckM7UUFDRCxNQUFNLENBQUMsc0JBQXNCLENBQUMsQ0FBQyxJQUFJLENBQUMsYUFBYSxDQUFDLENBQUM7SUFDckQsQ0FBQztJQUVELHFEQUFxQixHQUFyQixVQUFzQixRQUE2QixFQUFFLElBQVMsRUFBRSxlQUF3QjtRQUV0RixJQUFJLElBQUksSUFBSSxJQUFJLEVBQUU7WUFDaEIsT0FBTyxJQUFJLENBQUM7U0FDYjtRQUNELElBQU0sQ0FBQyxHQUFHLFFBQVEsQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLENBQUM7UUFDeEMsSUFBSSxDQUFDLElBQUksSUFBSSxFQUFFO1lBQ2IsT0FBTyxDQUFDLENBQUM7U0FDVjthQUFNLElBQUksQ0FBQyxlQUFlLEVBQUU7WUFDM0IsT0FBTyxJQUFJLENBQUM7U0FDYjtRQUNELElBQUksTUFBTSxFQUFFLENBQUMsWUFBWSxDQUFDLElBQUksQ0FBQyxFQUFFO1lBQy9CLE9BQU8sSUFBSSxDQUFDLHFCQUFxQixDQUFDLFFBQVEsRUFBRSxNQUFNLEVBQUUsQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLEVBQUUsSUFBSSxDQUFDLENBQUM7U0FDM0U7UUFDRCxPQUFPLElBQUksQ0FBQyxxQkFBcUIsQ0FBQyxRQUFRLEVBQUUsTUFBTSxFQUFFLENBQUMsYUFBYSxDQUFDLElBQUksQ0FBQyxFQUFFLElBQUksQ0FBQyxDQUFDO0lBQ2xGLENBQUM7SUFDSCw0QkFBQztBQUFELENBQUMsQUF0REQsSUFzREMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7R2V0VGVzdGFiaWxpdHksIFRlc3RhYmlsaXR5LCBUZXN0YWJpbGl0eVJlZ2lzdHJ5LCBzZXRUZXN0YWJpbGl0eUdldHRlciwgybVnbG9iYWwgYXMgZ2xvYmFsfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHtnZXRET019IGZyb20gJy4uL2RvbS9kb21fYWRhcHRlcic7XG5cbmV4cG9ydCBjbGFzcyBCcm93c2VyR2V0VGVzdGFiaWxpdHkgaW1wbGVtZW50cyBHZXRUZXN0YWJpbGl0eSB7XG4gIHN0YXRpYyBpbml0KCkgeyBzZXRUZXN0YWJpbGl0eUdldHRlcihuZXcgQnJvd3NlckdldFRlc3RhYmlsaXR5KCkpOyB9XG5cbiAgYWRkVG9XaW5kb3cocmVnaXN0cnk6IFRlc3RhYmlsaXR5UmVnaXN0cnkpOiB2b2lkIHtcbiAgICBnbG9iYWxbJ2dldEFuZ3VsYXJUZXN0YWJpbGl0eSddID0gKGVsZW06IGFueSwgZmluZEluQW5jZXN0b3JzOiBib29sZWFuID0gdHJ1ZSkgPT4ge1xuICAgICAgY29uc3QgdGVzdGFiaWxpdHkgPSByZWdpc3RyeS5maW5kVGVzdGFiaWxpdHlJblRyZWUoZWxlbSwgZmluZEluQW5jZXN0b3JzKTtcbiAgICAgIGlmICh0ZXN0YWJpbGl0eSA9PSBudWxsKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcignQ291bGQgbm90IGZpbmQgdGVzdGFiaWxpdHkgZm9yIGVsZW1lbnQuJyk7XG4gICAgICB9XG4gICAgICByZXR1cm4gdGVzdGFiaWxpdHk7XG4gICAgfTtcblxuICAgIGdsb2JhbFsnZ2V0QWxsQW5ndWxhclRlc3RhYmlsaXRpZXMnXSA9ICgpID0+IHJlZ2lzdHJ5LmdldEFsbFRlc3RhYmlsaXRpZXMoKTtcblxuICAgIGdsb2JhbFsnZ2V0QWxsQW5ndWxhclJvb3RFbGVtZW50cyddID0gKCkgPT4gcmVnaXN0cnkuZ2V0QWxsUm9vdEVsZW1lbnRzKCk7XG5cbiAgICBjb25zdCB3aGVuQWxsU3RhYmxlID0gKGNhbGxiYWNrOiBhbnkgLyoqIFRPRE8gIzkxMDAgKi8pID0+IHtcbiAgICAgIGNvbnN0IHRlc3RhYmlsaXRpZXMgPSBnbG9iYWxbJ2dldEFsbEFuZ3VsYXJUZXN0YWJpbGl0aWVzJ10oKTtcbiAgICAgIGxldCBjb3VudCA9IHRlc3RhYmlsaXRpZXMubGVuZ3RoO1xuICAgICAgbGV0IGRpZFdvcmsgPSBmYWxzZTtcbiAgICAgIGNvbnN0IGRlY3JlbWVudCA9IGZ1bmN0aW9uKGRpZFdvcmtfOiBhbnkgLyoqIFRPRE8gIzkxMDAgKi8pIHtcbiAgICAgICAgZGlkV29yayA9IGRpZFdvcmsgfHwgZGlkV29ya187XG4gICAgICAgIGNvdW50LS07XG4gICAgICAgIGlmIChjb3VudCA9PSAwKSB7XG4gICAgICAgICAgY2FsbGJhY2soZGlkV29yayk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICB0ZXN0YWJpbGl0aWVzLmZvckVhY2goZnVuY3Rpb24odGVzdGFiaWxpdHk6IGFueSAvKiogVE9ETyAjOTEwMCAqLykge1xuICAgICAgICB0ZXN0YWJpbGl0eS53aGVuU3RhYmxlKGRlY3JlbWVudCk7XG4gICAgICB9KTtcbiAgICB9O1xuXG4gICAgaWYgKCFnbG9iYWxbJ2ZyYW1ld29ya1N0YWJpbGl6ZXJzJ10pIHtcbiAgICAgIGdsb2JhbFsnZnJhbWV3b3JrU3RhYmlsaXplcnMnXSA9IFtdO1xuICAgIH1cbiAgICBnbG9iYWxbJ2ZyYW1ld29ya1N0YWJpbGl6ZXJzJ10ucHVzaCh3aGVuQWxsU3RhYmxlKTtcbiAgfVxuXG4gIGZpbmRUZXN0YWJpbGl0eUluVHJlZShyZWdpc3RyeTogVGVzdGFiaWxpdHlSZWdpc3RyeSwgZWxlbTogYW55LCBmaW5kSW5BbmNlc3RvcnM6IGJvb2xlYW4pOlxuICAgICAgVGVzdGFiaWxpdHl8bnVsbCB7XG4gICAgaWYgKGVsZW0gPT0gbnVsbCkge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGNvbnN0IHQgPSByZWdpc3RyeS5nZXRUZXN0YWJpbGl0eShlbGVtKTtcbiAgICBpZiAodCAhPSBudWxsKSB7XG4gICAgICByZXR1cm4gdDtcbiAgICB9IGVsc2UgaWYgKCFmaW5kSW5BbmNlc3RvcnMpIHtcbiAgICAgIHJldHVybiBudWxsO1xuICAgIH1cbiAgICBpZiAoZ2V0RE9NKCkuaXNTaGFkb3dSb290KGVsZW0pKSB7XG4gICAgICByZXR1cm4gdGhpcy5maW5kVGVzdGFiaWxpdHlJblRyZWUocmVnaXN0cnksIGdldERPTSgpLmdldEhvc3QoZWxlbSksIHRydWUpO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy5maW5kVGVzdGFiaWxpdHlJblRyZWUocmVnaXN0cnksIGdldERPTSgpLnBhcmVudEVsZW1lbnQoZWxlbSksIHRydWUpO1xuICB9XG59XG4iXX0=