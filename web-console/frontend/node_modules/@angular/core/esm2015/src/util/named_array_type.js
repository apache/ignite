/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import './ng_dev_mode';
/**
 * THIS FILE CONTAINS CODE WHICH SHOULD BE TREE SHAKEN AND NEVER CALLED FROM PRODUCTION CODE!!!
 */
/**
 * Creates an `Array` construction with a given name. This is useful when
 * looking for memory consumption to see what time of array it is.
 *
 *
 * @param name Name to give to the constructor
 * @returns A subclass of `Array` if possible. This can only be done in
 *          environments which support `class` construct.
 */
export function createNamedArrayType(name) {
    // This should never be called in prod mode, so let's verify that is the case.
    if (ngDevMode) {
        try {
            // We need to do it this way so that TypeScript does not down-level the below code.
            const FunctionConstructor = createNamedArrayType.constructor;
            return (new FunctionConstructor('Array', `return class ABC extends Array{}`))(Array);
        }
        catch (e) {
            // If it does not work just give up and fall back to regular Array.
            return Array;
        }
    }
    else {
        throw new Error('Looks like we are in \'prod mode\', but we are creating a named Array type, which is wrong! Check your code');
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmFtZWRfYXJyYXlfdHlwZS5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvcmUvc3JjL3V0aWwvbmFtZWRfYXJyYXlfdHlwZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFDQTs7Ozs7O0dBTUc7QUFFSCxPQUFPLGVBQWUsQ0FBQztBQUd2Qjs7R0FFRztBQUdIOzs7Ozs7OztHQVFHO0FBQ0gsTUFBTSxVQUFVLG9CQUFvQixDQUFDLElBQVk7SUFDL0MsOEVBQThFO0lBQzlFLElBQUksU0FBUyxFQUFFO1FBQ2IsSUFBSTtZQUNGLG1GQUFtRjtZQUNuRixNQUFNLG1CQUFtQixHQUFRLG9CQUFvQixDQUFDLFdBQVcsQ0FBQztZQUNsRSxPQUFPLENBQUMsSUFBSSxtQkFBbUIsQ0FBQyxPQUFPLEVBQUUsa0NBQWtDLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDO1NBQ3RGO1FBQUMsT0FBTyxDQUFDLEVBQUU7WUFDVixtRUFBbUU7WUFDbkUsT0FBTyxLQUFLLENBQUM7U0FDZDtLQUNGO1NBQU07UUFDTCxNQUFNLElBQUksS0FBSyxDQUNYLDZHQUE2RyxDQUFDLENBQUM7S0FDcEg7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiXG4vKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCAnLi9uZ19kZXZfbW9kZSc7XG5pbXBvcnQge2dsb2JhbH0gZnJvbSAnLi9nbG9iYWwnO1xuXG4vKipcbiAqIFRISVMgRklMRSBDT05UQUlOUyBDT0RFIFdISUNIIFNIT1VMRCBCRSBUUkVFIFNIQUtFTiBBTkQgTkVWRVIgQ0FMTEVEIEZST00gUFJPRFVDVElPTiBDT0RFISEhXG4gKi9cblxuXG4vKipcbiAqIENyZWF0ZXMgYW4gYEFycmF5YCBjb25zdHJ1Y3Rpb24gd2l0aCBhIGdpdmVuIG5hbWUuIFRoaXMgaXMgdXNlZnVsIHdoZW5cbiAqIGxvb2tpbmcgZm9yIG1lbW9yeSBjb25zdW1wdGlvbiB0byBzZWUgd2hhdCB0aW1lIG9mIGFycmF5IGl0IGlzLlxuICpcbiAqXG4gKiBAcGFyYW0gbmFtZSBOYW1lIHRvIGdpdmUgdG8gdGhlIGNvbnN0cnVjdG9yXG4gKiBAcmV0dXJucyBBIHN1YmNsYXNzIG9mIGBBcnJheWAgaWYgcG9zc2libGUuIFRoaXMgY2FuIG9ubHkgYmUgZG9uZSBpblxuICogICAgICAgICAgZW52aXJvbm1lbnRzIHdoaWNoIHN1cHBvcnQgYGNsYXNzYCBjb25zdHJ1Y3QuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBjcmVhdGVOYW1lZEFycmF5VHlwZShuYW1lOiBzdHJpbmcpOiB0eXBlb2YgQXJyYXkge1xuICAvLyBUaGlzIHNob3VsZCBuZXZlciBiZSBjYWxsZWQgaW4gcHJvZCBtb2RlLCBzbyBsZXQncyB2ZXJpZnkgdGhhdCBpcyB0aGUgY2FzZS5cbiAgaWYgKG5nRGV2TW9kZSkge1xuICAgIHRyeSB7XG4gICAgICAvLyBXZSBuZWVkIHRvIGRvIGl0IHRoaXMgd2F5IHNvIHRoYXQgVHlwZVNjcmlwdCBkb2VzIG5vdCBkb3duLWxldmVsIHRoZSBiZWxvdyBjb2RlLlxuICAgICAgY29uc3QgRnVuY3Rpb25Db25zdHJ1Y3RvcjogYW55ID0gY3JlYXRlTmFtZWRBcnJheVR5cGUuY29uc3RydWN0b3I7XG4gICAgICByZXR1cm4gKG5ldyBGdW5jdGlvbkNvbnN0cnVjdG9yKCdBcnJheScsIGByZXR1cm4gY2xhc3MgQUJDIGV4dGVuZHMgQXJyYXl7fWApKShBcnJheSk7XG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgLy8gSWYgaXQgZG9lcyBub3Qgd29yayBqdXN0IGdpdmUgdXAgYW5kIGZhbGwgYmFjayB0byByZWd1bGFyIEFycmF5LlxuICAgICAgcmV0dXJuIEFycmF5O1xuICAgIH1cbiAgfSBlbHNlIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICdMb29rcyBsaWtlIHdlIGFyZSBpbiBcXCdwcm9kIG1vZGVcXCcsIGJ1dCB3ZSBhcmUgY3JlYXRpbmcgYSBuYW1lZCBBcnJheSB0eXBlLCB3aGljaCBpcyB3cm9uZyEgQ2hlY2sgeW91ciBjb2RlJyk7XG4gIH1cbn0iXX0=