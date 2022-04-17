/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/compiler/src/output/source_map", ["require", "exports", "@angular/compiler/src/util"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    var util_1 = require("@angular/compiler/src/util");
    // https://docs.google.com/document/d/1U1RGAehQwRypUTovF1KRlpiOFze0b-_2gc6fAH0KY0k/edit
    var VERSION = 3;
    var JS_B64_PREFIX = '# sourceMappingURL=data:application/json;base64,';
    var SourceMapGenerator = /** @class */ (function () {
        function SourceMapGenerator(file) {
            if (file === void 0) { file = null; }
            this.file = file;
            this.sourcesContent = new Map();
            this.lines = [];
            this.lastCol0 = 0;
            this.hasMappings = false;
        }
        // The content is `null` when the content is expected to be loaded using the URL
        SourceMapGenerator.prototype.addSource = function (url, content) {
            if (content === void 0) { content = null; }
            if (!this.sourcesContent.has(url)) {
                this.sourcesContent.set(url, content);
            }
            return this;
        };
        SourceMapGenerator.prototype.addLine = function () {
            this.lines.push([]);
            this.lastCol0 = 0;
            return this;
        };
        SourceMapGenerator.prototype.addMapping = function (col0, sourceUrl, sourceLine0, sourceCol0) {
            if (!this.currentLine) {
                throw new Error("A line must be added before mappings can be added");
            }
            if (sourceUrl != null && !this.sourcesContent.has(sourceUrl)) {
                throw new Error("Unknown source file \"" + sourceUrl + "\"");
            }
            if (col0 == null) {
                throw new Error("The column in the generated code must be provided");
            }
            if (col0 < this.lastCol0) {
                throw new Error("Mapping should be added in output order");
            }
            if (sourceUrl && (sourceLine0 == null || sourceCol0 == null)) {
                throw new Error("The source location must be provided when a source url is provided");
            }
            this.hasMappings = true;
            this.lastCol0 = col0;
            this.currentLine.push({ col0: col0, sourceUrl: sourceUrl, sourceLine0: sourceLine0, sourceCol0: sourceCol0 });
            return this;
        };
        Object.defineProperty(SourceMapGenerator.prototype, "currentLine", {
            get: function () { return this.lines.slice(-1)[0]; },
            enumerable: true,
            configurable: true
        });
        SourceMapGenerator.prototype.toJSON = function () {
            var _this = this;
            if (!this.hasMappings) {
                return null;
            }
            var sourcesIndex = new Map();
            var sources = [];
            var sourcesContent = [];
            Array.from(this.sourcesContent.keys()).forEach(function (url, i) {
                sourcesIndex.set(url, i);
                sources.push(url);
                sourcesContent.push(_this.sourcesContent.get(url) || null);
            });
            var mappings = '';
            var lastCol0 = 0;
            var lastSourceIndex = 0;
            var lastSourceLine0 = 0;
            var lastSourceCol0 = 0;
            this.lines.forEach(function (segments) {
                lastCol0 = 0;
                mappings += segments
                    .map(function (segment) {
                    // zero-based starting column of the line in the generated code
                    var segAsStr = toBase64VLQ(segment.col0 - lastCol0);
                    lastCol0 = segment.col0;
                    if (segment.sourceUrl != null) {
                        // zero-based index into the “sources” list
                        segAsStr +=
                            toBase64VLQ(sourcesIndex.get(segment.sourceUrl) - lastSourceIndex);
                        lastSourceIndex = sourcesIndex.get(segment.sourceUrl);
                        // the zero-based starting line in the original source
                        segAsStr += toBase64VLQ(segment.sourceLine0 - lastSourceLine0);
                        lastSourceLine0 = segment.sourceLine0;
                        // the zero-based starting column in the original source
                        segAsStr += toBase64VLQ(segment.sourceCol0 - lastSourceCol0);
                        lastSourceCol0 = segment.sourceCol0;
                    }
                    return segAsStr;
                })
                    .join(',');
                mappings += ';';
            });
            mappings = mappings.slice(0, -1);
            return {
                'file': this.file || '',
                'version': VERSION,
                'sourceRoot': '',
                'sources': sources,
                'sourcesContent': sourcesContent,
                'mappings': mappings,
            };
        };
        SourceMapGenerator.prototype.toJsComment = function () {
            return this.hasMappings ? '//' + JS_B64_PREFIX + toBase64String(JSON.stringify(this, null, 0)) :
                '';
        };
        return SourceMapGenerator;
    }());
    exports.SourceMapGenerator = SourceMapGenerator;
    function toBase64String(value) {
        var b64 = '';
        value = util_1.utf8Encode(value);
        for (var i = 0; i < value.length;) {
            var i1 = value.charCodeAt(i++);
            var i2 = value.charCodeAt(i++);
            var i3 = value.charCodeAt(i++);
            b64 += toBase64Digit(i1 >> 2);
            b64 += toBase64Digit(((i1 & 3) << 4) | (isNaN(i2) ? 0 : i2 >> 4));
            b64 += isNaN(i2) ? '=' : toBase64Digit(((i2 & 15) << 2) | (i3 >> 6));
            b64 += isNaN(i2) || isNaN(i3) ? '=' : toBase64Digit(i3 & 63);
        }
        return b64;
    }
    exports.toBase64String = toBase64String;
    function toBase64VLQ(value) {
        value = value < 0 ? ((-value) << 1) + 1 : value << 1;
        var out = '';
        do {
            var digit = value & 31;
            value = value >> 5;
            if (value > 0) {
                digit = digit | 32;
            }
            out += toBase64Digit(digit);
        } while (value > 0);
        return out;
    }
    var B64_DIGITS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    function toBase64Digit(value) {
        if (value < 0 || value >= 64) {
            throw new Error("Can only encode value in the range [0, 63]");
        }
        return B64_DIGITS[value];
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic291cmNlX21hcC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9vdXRwdXQvc291cmNlX21hcC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILG1EQUFtQztJQUVuQyx1RkFBdUY7SUFDdkYsSUFBTSxPQUFPLEdBQUcsQ0FBQyxDQUFDO0lBRWxCLElBQU0sYUFBYSxHQUFHLGtEQUFrRCxDQUFDO0lBa0J6RTtRQU1FLDRCQUFvQixJQUF3QjtZQUF4QixxQkFBQSxFQUFBLFdBQXdCO1lBQXhCLFNBQUksR0FBSixJQUFJLENBQW9CO1lBTHBDLG1CQUFjLEdBQTZCLElBQUksR0FBRyxFQUFFLENBQUM7WUFDckQsVUFBSyxHQUFnQixFQUFFLENBQUM7WUFDeEIsYUFBUSxHQUFXLENBQUMsQ0FBQztZQUNyQixnQkFBVyxHQUFHLEtBQUssQ0FBQztRQUVtQixDQUFDO1FBRWhELGdGQUFnRjtRQUNoRixzQ0FBUyxHQUFULFVBQVUsR0FBVyxFQUFFLE9BQTJCO1lBQTNCLHdCQUFBLEVBQUEsY0FBMkI7WUFDaEQsSUFBSSxDQUFDLElBQUksQ0FBQyxjQUFjLENBQUMsR0FBRyxDQUFDLEdBQUcsQ0FBQyxFQUFFO2dCQUNqQyxJQUFJLENBQUMsY0FBYyxDQUFDLEdBQUcsQ0FBQyxHQUFHLEVBQUUsT0FBTyxDQUFDLENBQUM7YUFDdkM7WUFDRCxPQUFPLElBQUksQ0FBQztRQUNkLENBQUM7UUFFRCxvQ0FBTyxHQUFQO1lBQ0UsSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7WUFDcEIsSUFBSSxDQUFDLFFBQVEsR0FBRyxDQUFDLENBQUM7WUFDbEIsT0FBTyxJQUFJLENBQUM7UUFDZCxDQUFDO1FBRUQsdUNBQVUsR0FBVixVQUFXLElBQVksRUFBRSxTQUFrQixFQUFFLFdBQW9CLEVBQUUsVUFBbUI7WUFD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RSxHQUFHLEVBQUUsQ0FBQyxDQUFDO1NBQzlEO1FBRUQsT0FBTyxHQUFHLENBQUM7SUFDYixDQUFDO0lBZEQsd0NBY0M7SUFFRCxTQUFTLFdBQVcsQ0FBQyxLQUFhO1FBQ2hDLEtBQUssR0FBRyxLQUFLLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxLQUFLLENBQUMsSUFBSSxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLEtBQUssSUFBSSxDQUFDLENBQUM7UUFFckQsSUFBSSxHQUFHLEdBQUcsRUFBRSxDQUFDO1FBQ2IsR0FBRztZQUNELElBQUksS0FBSyxHQUFHLEtBQUssR0FBRyxFQUFFLENBQUM7WUFDdkIsS0FBSyxHQUFHLEtBQUssSUFBSSxDQUFDLENBQUM7WUFDbkIsSUFBSSxLQUFLLEdBQUcsQ0FBQyxFQUFFO2dCQUNiLEtBQUssR0FBRyxLQUFLLEdBQUcsRUFBRSxDQUFDO2FBQ3BCO1lBQ0QsR0FBRyxJQUFJLGFBQWEsQ0FBQyxLQUFLLENBQUMsQ0FBQztTQUM3QixRQUFRLEtBQUssR0FBRyxDQUFDLEVBQUU7UUFFcEIsT0FBTyxHQUFHLENBQUM7SUFDYixDQUFDO0lBRUQsSUFBTSxVQUFVLEdBQUcsa0VBQWtFLENBQUM7SUFFdEYsU0FBUyxhQUFhLENBQUMsS0FBYTtRQUNsQyxJQUFJLEtBQUssR0FBRyxDQUFDLElBQUksS0FBSyxJQUFJLEVBQUUsRUFBRTtZQUM1QixNQUFNLElBQUksS0FBSyxDQUFDLDRDQUE0QyxDQUFDLENBQUM7U0FDL0Q7UUFFRCxPQUFPLFVBQVUsQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUMzQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge3V0ZjhFbmNvZGV9IGZyb20gJy4uL3V0aWwnO1xuXG4vLyBodHRwczovL2RvY3MuZ29vZ2xlLmNvbS9kb2N1bWVudC9kLzFVMVJHQWVoUXdSeXBVVG92RjFLUmxwaU9GemUwYi1fMmdjNmZBSDBLWTBrL2VkaXRcbmNvbnN0IFZFUlNJT04gPSAzO1xuXG5jb25zdCBKU19CNjRfUFJFRklYID0gJyMgc291cmNlTWFwcGluZ1VSTD1kYXRhOmFwcGxpY2F0aW9uL2pzb247YmFzZTY0LCc7XG5cbnR5cGUgU2VnbWVudCA9IHtcbiAgY29sMDogbnVtYmVyLFxuICBzb3VyY2VVcmw/OiBzdHJpbmcsXG4gIHNvdXJjZUxpbmUwPzogbnVtYmVyLFxuICBzb3VyY2VDb2wwPzogbnVtYmVyLFxufTtcblxuZXhwb3J0IHR5cGUgU291cmNlTWFwID0ge1xuICB2ZXJzaW9uOiBudW1iZXIsXG4gIGZpbGU/OiBzdHJpbmcsXG4gIHNvdXJjZVJvb3Q6IHN0cmluZyxcbiAgc291cmNlczogc3RyaW5nW10sXG4gIHNvdXJjZXNDb250ZW50OiAoc3RyaW5nIHwgbnVsbClbXSxcbiAgbWFwcGluZ3M6IHN0cmluZyxcbn07XG5cbmV4cG9ydCBjbGFzcyBTb3VyY2VNYXBHZW5lcmF0b3Ige1xuICBwcml2YXRlIHNvdXJjZXNDb250ZW50OiBNYXA8c3RyaW5nLCBzdHJpbmd8bnVsbD4gPSBuZXcgTWFwKCk7XG4gIHByaXZhdGUgbGluZXM6IFNlZ21lbnRbXVtdID0gW107XG4gIHByaXZhdGUgbGFzdENvbDA6IG51bWJlciA9IDA7XG4gIHByaXZhdGUgaGFzTWFwcGluZ3MgPSBmYWxzZTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGZpbGU6IHN0cmluZ3xudWxsID0gbnVsbCkge31cblxuICAvLyBUaGUgY29udGVudCBpcyBgbnVsbGAgd2hlbiB0aGUgY29udGVudCBpcyBleHBlY3RlZCB0byBiZSBsb2FkZWQgdXNpbmcgdGhlIFVSTFxuICBhZGRTb3VyY2UodXJsOiBzdHJpbmcsIGNvbnRlbnQ6IHN0cmluZ3xudWxsID0gbnVsbCk6IHRoaXMge1xuICAgIGlmICghdGhpcy5zb3VyY2VzQ29udGVudC5oYXModXJsKSkge1xuICAgICAgdGhpcy5zb3VyY2VzQ29udGVudC5zZXQodXJsLCBjb250ZW50KTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXM7XG4gIH1cblxuICBhZGRMaW5lKCk6IHRoaXMge1xuICAgIHRoaXMubGluZXMucHVzaChbXSk7XG4gICAgdGhpcy5sYXN0Q29sMCA9IDA7XG4gICAgcmV0dXJuIHRoaXM7XG4gIH1cblxuICBhZGRNYXBwaW5nKGNvbDA6IG51bWJlciwgc291cmNlVXJsPzogc3RyaW5nLCBzb3VyY2VMaW5lMD86IG51bWJlciwgc291cmNlQ29sMD86IG51bWJlcik6IHRoaXMge1xuICAgIGlmICghdGhpcy5jdXJyZW50TGluZSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBBIGxpbmUgbXVzdCBiZSBhZGRlZCBiZWZvcmUgbWFwcGluZ3MgY2FuIGJlIGFkZGVkYCk7XG4gICAgfVxuICAgIGlmIChzb3VyY2VVcmwgIT0gbnVsbCAmJiAhdGhpcy5zb3VyY2VzQ29udGVudC5oYXMoc291cmNlVXJsKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBVbmtub3duIHNvdXJjZSBmaWxlIFwiJHtzb3VyY2VVcmx9XCJgKTtcbiAgICB9XG4gICAgaWYgKGNvbDAgPT0gbnVsbCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBUaGUgY29sdW1uIGluIHRoZSBnZW5lcmF0ZWQgY29kZSBtdXN0IGJlIHByb3ZpZGVkYCk7XG4gICAgfVxuICAgIGlmIChjb2wwIDwgdGhpcy5sYXN0Q29sMCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGBNYXBwaW5nIHNob3VsZCBiZSBhZGRlZCBpbiBvdXRwdXQgb3JkZXJgKTtcbiAgICB9XG4gICAgaWYgKHNvdXJjZVVybCAmJiAoc291cmNlTGluZTAgPT0gbnVsbCB8fCBzb3VyY2VDb2wwID09IG51bGwpKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoYFRoZSBzb3VyY2UgbG9jYXRpb24gbXVzdCBiZSBwcm92aWRlZCB3aGVuIGEgc291cmNlIHVybCBpcyBwcm92aWRlZGApO1xuICAgIH1cblxuICAgIHRoaXMuaGFzTWFwcGluZ3MgPSB0cnVlO1xuICAgIHRoaXMubGFzdENvbDAgPSBjb2wwO1xuICAgIHRoaXMuY3VycmVudExpbmUucHVzaCh7Y29sMCwgc291cmNlVXJsLCBzb3VyY2VMaW5lMCwgc291cmNlQ29sMH0pO1xuICAgIHJldHVybiB0aGlzO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXQgY3VycmVudExpbmUoKTogU2VnbWVudFtdfG51bGwgeyByZXR1cm4gdGhpcy5saW5lcy5zbGljZSgtMSlbMF07IH1cblxuICB0b0pTT04oKTogU291cmNlTWFwfG51bGwge1xuICAgIGlmICghdGhpcy5oYXNNYXBwaW5ncykge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuXG4gICAgY29uc3Qgc291cmNlc0luZGV4ID0gbmV3IE1hcDxzdHJpbmcsIG51bWJlcj4oKTtcbiAgICBjb25zdCBzb3VyY2VzOiBzdHJpbmdbXSA9IFtdO1xuICAgIGNvbnN0IHNvdXJjZXNDb250ZW50OiAoc3RyaW5nIHwgbnVsbClbXSA9IFtdO1xuXG4gICAgQXJyYXkuZnJvbSh0aGlzLnNvdXJjZXNDb250ZW50LmtleXMoKSkuZm9yRWFjaCgodXJsOiBzdHJpbmcsIGk6IG51bWJlcikgPT4ge1xuICAgICAgc291cmNlc0luZGV4LnNldCh1cmwsIGkpO1xuICAgICAgc291cmNlcy5wdXNoKHVybCk7XG4gICAgICBzb3VyY2VzQ29udGVudC5wdXNoKHRoaXMuc291cmNlc0NvbnRlbnQuZ2V0KHVybCkgfHwgbnVsbCk7XG4gICAgfSk7XG5cbiAgICBsZXQgbWFwcGluZ3M6IHN0cmluZyA9ICcnO1xuICAgIGxldCBsYXN0Q29sMDogbnVtYmVyID0gMDtcbiAgICBsZXQgbGFzdFNvdXJjZUluZGV4OiBudW1iZXIgPSAwO1xuICAgIGxldCBsYXN0U291cmNlTGluZTA6IG51bWJlciA9IDA7XG4gICAgbGV0IGxhc3RTb3VyY2VDb2wwOiBudW1iZXIgPSAwO1xuXG4gICAgdGhpcy5saW5lcy5mb3JFYWNoKHNlZ21lbnRzID0+IHtcbiAgICAgIGxhc3RDb2wwID0gMDtcblxuICAgICAgbWFwcGluZ3MgKz0gc2VnbWVudHNcbiAgICAgICAgICAgICAgICAgICAgICAubWFwKHNlZ21lbnQgPT4ge1xuICAgICAgICAgICAgICAgICAgICAgICAgLy8gemVyby1iYXNlZCBzdGFydGluZyBjb2x1bW4gb2YgdGhlIGxpbmUgaW4gdGhlIGdlbmVyYXRlZCBjb2RlXG4gICAgICAgICAgICAgICAgICAgICAgICBsZXQgc2VnQXNTdHIgPSB0b0Jhc2U2NFZMUShzZWdtZW50LmNvbDAgLSBsYXN0Q29sMCk7XG4gICAgICAgICAgICAgICAgICAgICAgICBsYXN0Q29sMCA9IHNlZ21lbnQuY29sMDtcblxuICAgICAgICAgICAgICAgICAgICAgICAgaWYgKHNlZ21lbnQuc291cmNlVXJsICE9IG51bGwpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgLy8gemVyby1iYXNlZCBpbmRleCBpbnRvIHRoZSDigJxzb3VyY2Vz4oCdIGxpc3RcbiAgICAgICAgICAgICAgICAgICAgICAgICAgc2VnQXNTdHIgKz1cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRvQmFzZTY0VkxRKHNvdXJjZXNJbmRleC5nZXQoc2VnbWVudC5zb3VyY2VVcmwpICEgLSBsYXN0U291cmNlSW5kZXgpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICBsYXN0U291cmNlSW5kZXggPSBzb3VyY2VzSW5kZXguZ2V0KHNlZ21lbnQuc291cmNlVXJsKSAhO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAvLyB0aGUgemVyby1iYXNlZCBzdGFydGluZyBsaW5lIGluIHRoZSBvcmlnaW5hbCBzb3VyY2VcbiAgICAgICAgICAgICAgICAgICAgICAgICAgc2VnQXNTdHIgKz0gdG9CYXNlNjRWTFEoc2VnbWVudC5zb3VyY2VMaW5lMCAhIC0gbGFzdFNvdXJjZUxpbmUwKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgbGFzdFNvdXJjZUxpbmUwID0gc2VnbWVudC5zb3VyY2VMaW5lMCAhO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAvLyB0aGUgemVyby1iYXNlZCBzdGFydGluZyBjb2x1bW4gaW4gdGhlIG9yaWdpbmFsIHNvdXJjZVxuICAgICAgICAgICAgICAgICAgICAgICAgICBzZWdBc1N0ciArPSB0b0Jhc2U2NFZMUShzZWdtZW50LnNvdXJjZUNvbDAgISAtIGxhc3RTb3VyY2VDb2wwKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgbGFzdFNvdXJjZUNvbDAgPSBzZWdtZW50LnNvdXJjZUNvbDAgITtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuIHNlZ0FzU3RyO1xuICAgICAgICAgICAgICAgICAgICAgIH0pXG4gICAgICAgICAgICAgICAgICAgICAgLmpvaW4oJywnKTtcbiAgICAgIG1hcHBpbmdzICs9ICc7JztcbiAgICB9KTtcblxuICAgIG1hcHBpbmdzID0gbWFwcGluZ3Muc2xpY2UoMCwgLTEpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgICdmaWxlJzogdGhpcy5maWxlIHx8ICcnLFxuICAgICAgJ3ZlcnNpb24nOiBWRVJTSU9OLFxuICAgICAgJ3NvdXJjZVJvb3QnOiAnJyxcbiAgICAgICdzb3VyY2VzJzogc291cmNlcyxcbiAgICAgICdzb3VyY2VzQ29udGVudCc6IHNvdXJjZXNDb250ZW50LFxuICAgICAgJ21hcHBpbmdzJzogbWFwcGluZ3MsXG4gICAgfTtcbiAgfVxuXG4gIHRvSnNDb21tZW50KCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuaGFzTWFwcGluZ3MgPyAnLy8nICsgSlNfQjY0X1BSRUZJWCArIHRvQmFzZTY0U3RyaW5nKEpTT04uc3RyaW5naWZ5KHRoaXMsIG51bGwsIDApKSA6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAnJztcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gdG9CYXNlNjRTdHJpbmcodmFsdWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIGxldCBiNjQgPSAnJztcbiAgdmFsdWUgPSB1dGY4RW5jb2RlKHZhbHVlKTtcbiAgZm9yIChsZXQgaSA9IDA7IGkgPCB2YWx1ZS5sZW5ndGg7KSB7XG4gICAgY29uc3QgaTEgPSB2YWx1ZS5jaGFyQ29kZUF0KGkrKyk7XG4gICAgY29uc3QgaTIgPSB2YWx1ZS5jaGFyQ29kZUF0KGkrKyk7XG4gICAgY29uc3QgaTMgPSB2YWx1ZS5jaGFyQ29kZUF0KGkrKyk7XG4gICAgYjY0ICs9IHRvQmFzZTY0RGlnaXQoaTEgPj4gMik7XG4gICAgYjY0ICs9IHRvQmFzZTY0RGlnaXQoKChpMSAmIDMpIDw8IDQpIHwgKGlzTmFOKGkyKSA/IDAgOiBpMiA+PiA0KSk7XG4gICAgYjY0ICs9IGlzTmFOKGkyKSA/ICc9JyA6IHRvQmFzZTY0RGlnaXQoKChpMiAmIDE1KSA8PCAyKSB8IChpMyA+PiA2KSk7XG4gICAgYjY0ICs9IGlzTmFOKGkyKSB8fCBpc05hTihpMykgPyAnPScgOiB0b0Jhc2U2NERpZ2l0KGkzICYgNjMpO1xuICB9XG5cbiAgcmV0dXJuIGI2NDtcbn1cblxuZnVuY3Rpb24gdG9CYXNlNjRWTFEodmFsdWU6IG51bWJlcik6IHN0cmluZyB7XG4gIHZhbHVlID0gdmFsdWUgPCAwID8gKCgtdmFsdWUpIDw8IDEpICsgMSA6IHZhbHVlIDw8IDE7XG5cbiAgbGV0IG91dCA9ICcnO1xuICBkbyB7XG4gICAgbGV0IGRpZ2l0ID0gdmFsdWUgJiAzMTtcbiAgICB2YWx1ZSA9IHZhbHVlID4+IDU7XG4gICAgaWYgKHZhbHVlID4gMCkge1xuICAgICAgZGlnaXQgPSBkaWdpdCB8IDMyO1xuICAgIH1cbiAgICBvdXQgKz0gdG9CYXNlNjREaWdpdChkaWdpdCk7XG4gIH0gd2hpbGUgKHZhbHVlID4gMCk7XG5cbiAgcmV0dXJuIG91dDtcbn1cblxuY29uc3QgQjY0X0RJR0lUUyA9ICdBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6MDEyMzQ1Njc4OSsvJztcblxuZnVuY3Rpb24gdG9CYXNlNjREaWdpdCh2YWx1ZTogbnVtYmVyKTogc3RyaW5nIHtcbiAgaWYgKHZhbHVlIDwgMCB8fCB2YWx1ZSA+PSA2NCkge1xuICAgIHRocm93IG5ldyBFcnJvcihgQ2FuIG9ubHkgZW5jb2RlIHZhbHVlIGluIHRoZSByYW5nZSBbMCwgNjNdYCk7XG4gIH1cblxuICByZXR1cm4gQjY0X0RJR0lUU1t2YWx1ZV07XG59XG4iXX0=