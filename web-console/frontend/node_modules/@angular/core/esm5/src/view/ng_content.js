/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { getParentRenderElement, visitProjectedRenderNodes } from './util';
export function ngContentDef(ngContentIndex, index) {
    return {
        // will bet set by the view definition
        nodeIndex: -1,
        parent: null,
        renderParent: null,
        bindingIndex: -1,
        outputIndex: -1,
        // regular values
        checkIndex: -1,
        flags: 8 /* TypeNgContent */,
        childFlags: 0,
        directChildFlags: 0,
        childMatchedQueries: 0,
        matchedQueries: {},
        matchedQueryIds: 0,
        references: {}, ngContentIndex: ngContentIndex,
        childCount: 0,
        bindings: [],
        bindingFlags: 0,
        outputs: [],
        element: null,
        provider: null,
        text: null,
        query: null,
        ngContent: { index: index }
    };
}
export function appendNgContent(view, renderHost, def) {
    var parentEl = getParentRenderElement(view, renderHost, def);
    if (!parentEl) {
        // Nothing to do if there is no parent element.
        return;
    }
    var ngContentIndex = def.ngContent.index;
    visitProjectedRenderNodes(view, ngContentIndex, 1 /* AppendChild */, parentEl, null, undefined);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY29udGVudC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3ZpZXcvbmdfY29udGVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7QUFHSCxPQUFPLEVBQW1CLHNCQUFzQixFQUFFLHlCQUF5QixFQUFDLE1BQU0sUUFBUSxDQUFDO0FBRTNGLE1BQU0sVUFBVSxZQUFZLENBQUMsY0FBNkIsRUFBRSxLQUFhO0lBQ3ZFLE9BQU87UUFDTCxzQ0FBc0M7UUFDdEMsU0FBUyxFQUFFLENBQUMsQ0FBQztRQUNiLE1BQU0sRUFBRSxJQUFJO1FBQ1osWUFBWSxFQUFFLElBQUk7UUFDbEIsWUFBWSxFQUFFLENBQUMsQ0FBQztRQUNoQixXQUFXLEVBQUUsQ0FBQyxDQUFDO1FBQ2YsaUJBQWlCO1FBQ2pCLFVBQVUsRUFBRSxDQUFDLENBQUM7UUFDZCxLQUFLLHVCQUF5QjtRQUM5QixVQUFVLEVBQUUsQ0FBQztRQUNiLGdCQUFnQixFQUFFLENBQUM7UUFDbkIsbUJBQW1CLEVBQUUsQ0FBQztRQUN0QixjQUFjLEVBQUUsRUFBRTtRQUNsQixlQUFlLEVBQUUsQ0FBQztRQUNsQixVQUFVLEVBQUUsRUFBRSxFQUFFLGNBQWMsZ0JBQUE7UUFDOUIsVUFBVSxFQUFFLENBQUM7UUFDYixRQUFRLEVBQUUsRUFBRTtRQUNaLFlBQVksRUFBRSxDQUFDO1FBQ2YsT0FBTyxFQUFFLEVBQUU7UUFDWCxPQUFPLEVBQUUsSUFBSTtRQUNiLFFBQVEsRUFBRSxJQUFJO1FBQ2QsSUFBSSxFQUFFLElBQUk7UUFDVixLQUFLLEVBQUUsSUFBSTtRQUNYLFNBQVMsRUFBRSxFQUFDLEtBQUssT0FBQSxFQUFDO0tBQ25CLENBQUM7QUFDSixDQUFDO0FBRUQsTUFBTSxVQUFVLGVBQWUsQ0FBQyxJQUFjLEVBQUUsVUFBZSxFQUFFLEdBQVk7SUFDM0UsSUFBTSxRQUFRLEdBQUcsc0JBQXNCLENBQUMsSUFBSSxFQUFFLFVBQVUsRUFBRSxHQUFHLENBQUMsQ0FBQztJQUMvRCxJQUFJLENBQUMsUUFBUSxFQUFFO1FBQ2IsK0NBQStDO1FBQy9DLE9BQU87S0FDUjtJQUNELElBQU0sY0FBYyxHQUFHLEdBQUcsQ0FBQyxTQUFXLENBQUMsS0FBSyxDQUFDO0lBQzdDLHlCQUF5QixDQUNyQixJQUFJLEVBQUUsY0FBYyx1QkFBZ0MsUUFBUSxFQUFFLElBQUksRUFBRSxTQUFTLENBQUMsQ0FBQztBQUNyRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge05vZGVEZWYsIE5vZGVGbGFncywgVmlld0RhdGF9IGZyb20gJy4vdHlwZXMnO1xuaW1wb3J0IHtSZW5kZXJOb2RlQWN0aW9uLCBnZXRQYXJlbnRSZW5kZXJFbGVtZW50LCB2aXNpdFByb2plY3RlZFJlbmRlck5vZGVzfSBmcm9tICcuL3V0aWwnO1xuXG5leHBvcnQgZnVuY3Rpb24gbmdDb250ZW50RGVmKG5nQ29udGVudEluZGV4OiBudWxsIHwgbnVtYmVyLCBpbmRleDogbnVtYmVyKTogTm9kZURlZiB7XG4gIHJldHVybiB7XG4gICAgLy8gd2lsbCBiZXQgc2V0IGJ5IHRoZSB2aWV3IGRlZmluaXRpb25cbiAgICBub2RlSW5kZXg6IC0xLFxuICAgIHBhcmVudDogbnVsbCxcbiAgICByZW5kZXJQYXJlbnQ6IG51bGwsXG4gICAgYmluZGluZ0luZGV4OiAtMSxcbiAgICBvdXRwdXRJbmRleDogLTEsXG4gICAgLy8gcmVndWxhciB2YWx1ZXNcbiAgICBjaGVja0luZGV4OiAtMSxcbiAgICBmbGFnczogTm9kZUZsYWdzLlR5cGVOZ0NvbnRlbnQsXG4gICAgY2hpbGRGbGFnczogMCxcbiAgICBkaXJlY3RDaGlsZEZsYWdzOiAwLFxuICAgIGNoaWxkTWF0Y2hlZFF1ZXJpZXM6IDAsXG4gICAgbWF0Y2hlZFF1ZXJpZXM6IHt9LFxuICAgIG1hdGNoZWRRdWVyeUlkczogMCxcbiAgICByZWZlcmVuY2VzOiB7fSwgbmdDb250ZW50SW5kZXgsXG4gICAgY2hpbGRDb3VudDogMCxcbiAgICBiaW5kaW5nczogW10sXG4gICAgYmluZGluZ0ZsYWdzOiAwLFxuICAgIG91dHB1dHM6IFtdLFxuICAgIGVsZW1lbnQ6IG51bGwsXG4gICAgcHJvdmlkZXI6IG51bGwsXG4gICAgdGV4dDogbnVsbCxcbiAgICBxdWVyeTogbnVsbCxcbiAgICBuZ0NvbnRlbnQ6IHtpbmRleH1cbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFwcGVuZE5nQ29udGVudCh2aWV3OiBWaWV3RGF0YSwgcmVuZGVySG9zdDogYW55LCBkZWY6IE5vZGVEZWYpIHtcbiAgY29uc3QgcGFyZW50RWwgPSBnZXRQYXJlbnRSZW5kZXJFbGVtZW50KHZpZXcsIHJlbmRlckhvc3QsIGRlZik7XG4gIGlmICghcGFyZW50RWwpIHtcbiAgICAvLyBOb3RoaW5nIHRvIGRvIGlmIHRoZXJlIGlzIG5vIHBhcmVudCBlbGVtZW50LlxuICAgIHJldHVybjtcbiAgfVxuICBjb25zdCBuZ0NvbnRlbnRJbmRleCA9IGRlZi5uZ0NvbnRlbnQgIS5pbmRleDtcbiAgdmlzaXRQcm9qZWN0ZWRSZW5kZXJOb2RlcyhcbiAgICAgIHZpZXcsIG5nQ29udGVudEluZGV4LCBSZW5kZXJOb2RlQWN0aW9uLkFwcGVuZENoaWxkLCBwYXJlbnRFbCwgbnVsbCwgdW5kZWZpbmVkKTtcbn1cbiJdfQ==