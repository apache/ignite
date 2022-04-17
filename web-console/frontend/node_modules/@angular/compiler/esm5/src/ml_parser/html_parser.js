/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { getHtmlTagDefinition } from './html_tags';
import { Parser } from './parser';
export { ParseTreeResult, TreeError } from './parser';
var HtmlParser = /** @class */ (function (_super) {
    tslib_1.__extends(HtmlParser, _super);
    function HtmlParser() {
        return _super.call(this, getHtmlTagDefinition) || this;
    }
    HtmlParser.prototype.parse = function (source, url, options) {
        return _super.prototype.parse.call(this, source, url, options);
    };
    return HtmlParser;
}(Parser));
export { HtmlParser };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaHRtbF9wYXJzZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi8uLi9wYWNrYWdlcy9jb21waWxlci9zcmMvbWxfcGFyc2VyL2h0bWxfcGFyc2VyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBOzs7Ozs7R0FNRzs7QUFFSCxPQUFPLEVBQUMsb0JBQW9CLEVBQUMsTUFBTSxhQUFhLENBQUM7QUFFakQsT0FBTyxFQUFrQixNQUFNLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFFakQsT0FBTyxFQUFDLGVBQWUsRUFBRSxTQUFTLEVBQUMsTUFBTSxVQUFVLENBQUM7QUFFcEQ7SUFBZ0Msc0NBQU07SUFDcEM7ZUFBZ0Isa0JBQU0sb0JBQW9CLENBQUM7SUFBRSxDQUFDO0lBRTlDLDBCQUFLLEdBQUwsVUFBTSxNQUFjLEVBQUUsR0FBVyxFQUFFLE9BQXlCO1FBQzFELE9BQU8saUJBQU0sS0FBSyxZQUFDLE1BQU0sRUFBRSxHQUFHLEVBQUUsT0FBTyxDQUFDLENBQUM7SUFDM0MsQ0FBQztJQUNILGlCQUFDO0FBQUQsQ0FBQyxBQU5ELENBQWdDLE1BQU0sR0FNckMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7Z2V0SHRtbFRhZ0RlZmluaXRpb259IGZyb20gJy4vaHRtbF90YWdzJztcbmltcG9ydCB7VG9rZW5pemVPcHRpb25zfSBmcm9tICcuL2xleGVyJztcbmltcG9ydCB7UGFyc2VUcmVlUmVzdWx0LCBQYXJzZXJ9IGZyb20gJy4vcGFyc2VyJztcblxuZXhwb3J0IHtQYXJzZVRyZWVSZXN1bHQsIFRyZWVFcnJvcn0gZnJvbSAnLi9wYXJzZXInO1xuXG5leHBvcnQgY2xhc3MgSHRtbFBhcnNlciBleHRlbmRzIFBhcnNlciB7XG4gIGNvbnN0cnVjdG9yKCkgeyBzdXBlcihnZXRIdG1sVGFnRGVmaW5pdGlvbik7IH1cblxuICBwYXJzZShzb3VyY2U6IHN0cmluZywgdXJsOiBzdHJpbmcsIG9wdGlvbnM/OiBUb2tlbml6ZU9wdGlvbnMpOiBQYXJzZVRyZWVSZXN1bHQge1xuICAgIHJldHVybiBzdXBlci5wYXJzZShzb3VyY2UsIHVybCwgb3B0aW9ucyk7XG4gIH1cbn1cbiJdfQ==