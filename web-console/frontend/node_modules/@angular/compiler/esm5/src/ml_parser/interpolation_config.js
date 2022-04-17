/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { assertInterpolationSymbols } from '../assertions';
var InterpolationConfig = /** @class */ (function () {
    function InterpolationConfig(start, end) {
        this.start = start;
        this.end = end;
    }
    InterpolationConfig.fromArray = function (markers) {
        if (!markers) {
            return DEFAULT_INTERPOLATION_CONFIG;
        }
        assertInterpolationSymbols('interpolation', markers);
        return new InterpolationConfig(markers[0], markers[1]);
    };
    return InterpolationConfig;
}());
export { InterpolationConfig };
export var DEFAULT_INTERPOLATION_CONFIG = new InterpolationConfig('{{', '}}');
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW50ZXJwb2xhdGlvbl9jb25maWcuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvbWxfcGFyc2VyL2ludGVycG9sYXRpb25fY29uZmlnLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRztBQUVILE9BQU8sRUFBQywwQkFBMEIsRUFBQyxNQUFNLGVBQWUsQ0FBQztBQUV6RDtJQVVFLDZCQUFtQixLQUFhLEVBQVMsR0FBVztRQUFqQyxVQUFLLEdBQUwsS0FBSyxDQUFRO1FBQVMsUUFBRyxHQUFILEdBQUcsQ0FBUTtJQUFHLENBQUM7SUFUakQsNkJBQVMsR0FBaEIsVUFBaUIsT0FBOEI7UUFDN0MsSUFBSSxDQUFDLE9BQU8sRUFBRTtZQUNaLE9BQU8sNEJBQTRCLENBQUM7U0FDckM7UUFFRCwwQkFBMEIsQ0FBQyxlQUFlLEVBQUUsT0FBTyxDQUFDLENBQUM7UUFDckQsT0FBTyxJQUFJLG1CQUFtQixDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsRUFBRSxPQUFPLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUN6RCxDQUFDO0lBR0gsMEJBQUM7QUFBRCxDQUFDLEFBWEQsSUFXQzs7QUFFRCxNQUFNLENBQUMsSUFBTSw0QkFBNEIsR0FDckMsSUFBSSxtQkFBbUIsQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7YXNzZXJ0SW50ZXJwb2xhdGlvblN5bWJvbHN9IGZyb20gJy4uL2Fzc2VydGlvbnMnO1xuXG5leHBvcnQgY2xhc3MgSW50ZXJwb2xhdGlvbkNvbmZpZyB7XG4gIHN0YXRpYyBmcm9tQXJyYXkobWFya2VyczogW3N0cmluZywgc3RyaW5nXXxudWxsKTogSW50ZXJwb2xhdGlvbkNvbmZpZyB7XG4gICAgaWYgKCFtYXJrZXJzKSB7XG4gICAgICByZXR1cm4gREVGQVVMVF9JTlRFUlBPTEFUSU9OX0NPTkZJRztcbiAgICB9XG5cbiAgICBhc3NlcnRJbnRlcnBvbGF0aW9uU3ltYm9scygnaW50ZXJwb2xhdGlvbicsIG1hcmtlcnMpO1xuICAgIHJldHVybiBuZXcgSW50ZXJwb2xhdGlvbkNvbmZpZyhtYXJrZXJzWzBdLCBtYXJrZXJzWzFdKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBzdGFydDogc3RyaW5nLCBwdWJsaWMgZW5kOiBzdHJpbmcpIHt9XG59XG5cbmV4cG9ydCBjb25zdCBERUZBVUxUX0lOVEVSUE9MQVRJT05fQ09ORklHOiBJbnRlcnBvbGF0aW9uQ29uZmlnID1cbiAgICBuZXcgSW50ZXJwb2xhdGlvbkNvbmZpZygne3snLCAnfX0nKTtcbiJdfQ==