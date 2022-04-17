/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { Directive } from '@angular/core';
/**
 * @description
 *
 * Adds `novalidate` attribute to all forms by default.
 *
 * `novalidate` is used to disable browser's native form validation.
 *
 * If you want to use native validation with Angular forms, just add `ngNativeValidate` attribute:
 *
 * ```
 * <form ngNativeValidate></form>
 * ```
 *
 * @publicApi
 * @ngModule ReactiveFormsModule
 * @ngModule FormsModule
 */
var ɵNgNoValidate = /** @class */ (function () {
    function ɵNgNoValidate() {
    }
    ɵNgNoValidate = tslib_1.__decorate([
        Directive({
            selector: 'form:not([ngNoForm]):not([ngNativeValidate])',
            host: { 'novalidate': '' },
        })
    ], ɵNgNoValidate);
    return ɵNgNoValidate;
}());
export { ɵNgNoValidate };
export { ɵNgNoValidate as NgNoValidate };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfbm9fdmFsaWRhdGVfZGlyZWN0aXZlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvZm9ybXMvc3JjL2RpcmVjdGl2ZXMvbmdfbm9fdmFsaWRhdGVfZGlyZWN0aXZlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsU0FBUyxFQUFDLE1BQU0sZUFBZSxDQUFDO0FBRXhDOzs7Ozs7Ozs7Ozs7Ozs7O0dBZ0JHO0FBS0g7SUFBQTtJQUNBLENBQUM7SUFEWSxhQUFhO1FBSnpCLFNBQVMsQ0FBQztZQUNULFFBQVEsRUFBRSw4Q0FBOEM7WUFDeEQsSUFBSSxFQUFFLEVBQUMsWUFBWSxFQUFFLEVBQUUsRUFBQztTQUN6QixDQUFDO09BQ1csYUFBYSxDQUN6QjtJQUFELG9CQUFDO0NBQUEsQUFERCxJQUNDO1NBRFksYUFBYTtBQUcxQixPQUFPLEVBQUMsYUFBYSxJQUFJLFlBQVksRUFBQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge0RpcmVjdGl2ZX0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbi8qKlxuICogQGRlc2NyaXB0aW9uXG4gKlxuICogQWRkcyBgbm92YWxpZGF0ZWAgYXR0cmlidXRlIHRvIGFsbCBmb3JtcyBieSBkZWZhdWx0LlxuICpcbiAqIGBub3ZhbGlkYXRlYCBpcyB1c2VkIHRvIGRpc2FibGUgYnJvd3NlcidzIG5hdGl2ZSBmb3JtIHZhbGlkYXRpb24uXG4gKlxuICogSWYgeW91IHdhbnQgdG8gdXNlIG5hdGl2ZSB2YWxpZGF0aW9uIHdpdGggQW5ndWxhciBmb3JtcywganVzdCBhZGQgYG5nTmF0aXZlVmFsaWRhdGVgIGF0dHJpYnV0ZTpcbiAqXG4gKiBgYGBcbiAqIDxmb3JtIG5nTmF0aXZlVmFsaWRhdGU+PC9mb3JtPlxuICogYGBgXG4gKlxuICogQHB1YmxpY0FwaVxuICogQG5nTW9kdWxlIFJlYWN0aXZlRm9ybXNNb2R1bGVcbiAqIEBuZ01vZHVsZSBGb3Jtc01vZHVsZVxuICovXG5ARGlyZWN0aXZlKHtcbiAgc2VsZWN0b3I6ICdmb3JtOm5vdChbbmdOb0Zvcm1dKTpub3QoW25nTmF0aXZlVmFsaWRhdGVdKScsXG4gIGhvc3Q6IHsnbm92YWxpZGF0ZSc6ICcnfSxcbn0pXG5leHBvcnQgY2xhc3MgybVOZ05vVmFsaWRhdGUge1xufVxuXG5leHBvcnQge8m1TmdOb1ZhbGlkYXRlIGFzIE5nTm9WYWxpZGF0ZX07XG4iXX0=