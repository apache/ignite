/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
export function getTypeOf(instance /** TODO #9100 */) {
    return instance.constructor;
}
export function instantiateType(type, params) {
    var _a;
    if (params === void 0) { params = []; }
    return new ((_a = type).bind.apply(_a, tslib_1.__spread([void 0], params)))();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGFuZ191dGlscy5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvdGVzdGluZy9zcmMvbGFuZ191dGlscy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7O0FBRUgsTUFBTSxVQUFVLFNBQVMsQ0FBQyxRQUFhLENBQUMsaUJBQWlCO0lBQ3ZELE9BQU8sUUFBUSxDQUFDLFdBQVcsQ0FBQztBQUM5QixDQUFDO0FBRUQsTUFBTSxVQUFVLGVBQWUsQ0FBQyxJQUFjLEVBQUUsTUFBa0I7O0lBQWxCLHVCQUFBLEVBQUEsV0FBa0I7SUFDaEUsWUFBVyxDQUFBLEtBQU0sSUFBSyxDQUFBLDJDQUFJLE1BQU0sTUFBRTtBQUNwQyxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5leHBvcnQgZnVuY3Rpb24gZ2V0VHlwZU9mKGluc3RhbmNlOiBhbnkgLyoqIFRPRE8gIzkxMDAgKi8pIHtcbiAgcmV0dXJuIGluc3RhbmNlLmNvbnN0cnVjdG9yO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaW5zdGFudGlhdGVUeXBlKHR5cGU6IEZ1bmN0aW9uLCBwYXJhbXM6IGFueVtdID0gW10pIHtcbiAgcmV0dXJuIG5ldyAoPGFueT50eXBlKSguLi5wYXJhbXMpO1xufVxuIl19