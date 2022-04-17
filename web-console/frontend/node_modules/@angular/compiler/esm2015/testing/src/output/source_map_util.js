/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,constantProperty,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/** @type {?} */
const b64 = require('base64-js');
/** @type {?} */
const SourceMapConsumer = require('source-map').SourceMapConsumer;
/**
 * @record
 */
export function SourceLocation() { }
if (false) {
    /** @type {?} */
    SourceLocation.prototype.line;
    /** @type {?} */
    SourceLocation.prototype.column;
    /** @type {?} */
    SourceLocation.prototype.source;
}
/**
 * @param {?} sourceMap
 * @param {?} genPosition
 * @return {?}
 */
export function originalPositionFor(sourceMap, genPosition) {
    /** @type {?} */
    const smc = new SourceMapConsumer(sourceMap);
    // Note: We don't return the original object as it also contains a `name` property
    // which is always null and we don't want to include that in our assertions...
    const { line, column, source } = smc.originalPositionFor(genPosition);
    return { line, column, source };
}
/**
 * @param {?} source
 * @return {?}
 */
export function extractSourceMap(source) {
    /** @type {?} */
    let idx = source.lastIndexOf('\n//#');
    if (idx == -1)
        return null;
    /** @type {?} */
    const smComment = source.slice(idx).split('\n', 2)[1].trim();
    /** @type {?} */
    const smB64 = smComment.split('sourceMappingURL=data:application/json;base64,')[1];
    return smB64 ? JSON.parse(decodeB64String(smB64)) : null;
}
/**
 * @param {?} s
 * @return {?}
 */
function decodeB64String(s) {
    return b64.toByteArray(s).reduce((/**
     * @param {?} s
     * @param {?} c
     * @return {?}
     */
    (s, c) => s + String.fromCharCode(c)), '');
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic291cmNlX21hcF91dGlsLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29tcGlsZXIvdGVzdGluZy9zcmMvb3V0cHV0L3NvdXJjZV9tYXBfdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7TUFTTSxHQUFHLEdBQUcsT0FBTyxDQUFDLFdBQVcsQ0FBQzs7TUFDMUIsaUJBQWlCLEdBQUcsT0FBTyxDQUFDLFlBQVksQ0FBQyxDQUFDLGlCQUFpQjs7OztBQUVqRSxvQ0FJQzs7O0lBSEMsOEJBQWE7O0lBQ2IsZ0NBQWU7O0lBQ2YsZ0NBQWU7Ozs7Ozs7QUFHakIsTUFBTSxVQUFVLG1CQUFtQixDQUMvQixTQUFvQixFQUNwQixXQUF5RDs7VUFDckQsR0FBRyxHQUFHLElBQUksaUJBQWlCLENBQUMsU0FBUyxDQUFDOzs7VUFHdEMsRUFBQyxJQUFJLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBQyxHQUFHLEdBQUcsQ0FBQyxtQkFBbUIsQ0FBQyxXQUFXLENBQUM7SUFDbkUsT0FBTyxFQUFDLElBQUksRUFBRSxNQUFNLEVBQUUsTUFBTSxFQUFDLENBQUM7QUFDaEMsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsZ0JBQWdCLENBQUMsTUFBYzs7UUFDekMsR0FBRyxHQUFHLE1BQU0sQ0FBQyxXQUFXLENBQUMsT0FBTyxDQUFDO0lBQ3JDLElBQUksR0FBRyxJQUFJLENBQUMsQ0FBQztRQUFFLE9BQU8sSUFBSSxDQUFDOztVQUNyQixTQUFTLEdBQUcsTUFBTSxDQUFDLEtBQUssQ0FBQyxHQUFHLENBQUMsQ0FBQyxLQUFLLENBQUMsSUFBSSxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksRUFBRTs7VUFDdEQsS0FBSyxHQUFHLFNBQVMsQ0FBQyxLQUFLLENBQUMsZ0RBQWdELENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDbEYsT0FBTyxLQUFLLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsZUFBZSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztBQUMzRCxDQUFDOzs7OztBQUVELFNBQVMsZUFBZSxDQUFDLENBQVM7SUFDaEMsT0FBTyxHQUFHLENBQUMsV0FBVyxDQUFDLENBQUMsQ0FBQyxDQUFDLE1BQU07Ozs7O0lBQUMsQ0FBQyxDQUFTLEVBQUUsQ0FBUyxFQUFFLEVBQUUsQ0FBQyxDQUFDLEdBQUcsTUFBTSxDQUFDLFlBQVksQ0FBQyxDQUFDLENBQUMsR0FBRSxFQUFFLENBQUMsQ0FBQztBQUM3RixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge1NvdXJjZU1hcH0gZnJvbSAnQGFuZ3VsYXIvY29tcGlsZXInO1xuY29uc3QgYjY0ID0gcmVxdWlyZSgnYmFzZTY0LWpzJyk7XG5jb25zdCBTb3VyY2VNYXBDb25zdW1lciA9IHJlcXVpcmUoJ3NvdXJjZS1tYXAnKS5Tb3VyY2VNYXBDb25zdW1lcjtcblxuZXhwb3J0IGludGVyZmFjZSBTb3VyY2VMb2NhdGlvbiB7XG4gIGxpbmU6IG51bWJlcjtcbiAgY29sdW1uOiBudW1iZXI7XG4gIHNvdXJjZTogc3RyaW5nO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gb3JpZ2luYWxQb3NpdGlvbkZvcihcbiAgICBzb3VyY2VNYXA6IFNvdXJjZU1hcCxcbiAgICBnZW5Qb3NpdGlvbjoge2xpbmU6IG51bWJlciB8IG51bGwsIGNvbHVtbjogbnVtYmVyIHwgbnVsbH0pOiBTb3VyY2VMb2NhdGlvbiB7XG4gIGNvbnN0IHNtYyA9IG5ldyBTb3VyY2VNYXBDb25zdW1lcihzb3VyY2VNYXApO1xuICAvLyBOb3RlOiBXZSBkb24ndCByZXR1cm4gdGhlIG9yaWdpbmFsIG9iamVjdCBhcyBpdCBhbHNvIGNvbnRhaW5zIGEgYG5hbWVgIHByb3BlcnR5XG4gIC8vIHdoaWNoIGlzIGFsd2F5cyBudWxsIGFuZCB3ZSBkb24ndCB3YW50IHRvIGluY2x1ZGUgdGhhdCBpbiBvdXIgYXNzZXJ0aW9ucy4uLlxuICBjb25zdCB7bGluZSwgY29sdW1uLCBzb3VyY2V9ID0gc21jLm9yaWdpbmFsUG9zaXRpb25Gb3IoZ2VuUG9zaXRpb24pO1xuICByZXR1cm4ge2xpbmUsIGNvbHVtbiwgc291cmNlfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RTb3VyY2VNYXAoc291cmNlOiBzdHJpbmcpOiBTb3VyY2VNYXB8bnVsbCB7XG4gIGxldCBpZHggPSBzb3VyY2UubGFzdEluZGV4T2YoJ1xcbi8vIycpO1xuICBpZiAoaWR4ID09IC0xKSByZXR1cm4gbnVsbDtcbiAgY29uc3Qgc21Db21tZW50ID0gc291cmNlLnNsaWNlKGlkeCkuc3BsaXQoJ1xcbicsIDIpWzFdLnRyaW0oKTtcbiAgY29uc3Qgc21CNjQgPSBzbUNvbW1lbnQuc3BsaXQoJ3NvdXJjZU1hcHBpbmdVUkw9ZGF0YTphcHBsaWNhdGlvbi9qc29uO2Jhc2U2NCwnKVsxXTtcbiAgcmV0dXJuIHNtQjY0ID8gSlNPTi5wYXJzZShkZWNvZGVCNjRTdHJpbmcoc21CNjQpKSA6IG51bGw7XG59XG5cbmZ1bmN0aW9uIGRlY29kZUI2NFN0cmluZyhzOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gYjY0LnRvQnl0ZUFycmF5KHMpLnJlZHVjZSgoczogc3RyaW5nLCBjOiBudW1iZXIpID0+IHMgKyBTdHJpbmcuZnJvbUNoYXJDb2RlKGMpLCAnJyk7XG59XG4iXX0=