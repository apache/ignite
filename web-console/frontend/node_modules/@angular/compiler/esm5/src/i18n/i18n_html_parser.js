/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as tslib_1 from "tslib";
import { MissingTranslationStrategy } from '../core';
import { DEFAULT_INTERPOLATION_CONFIG } from '../ml_parser/interpolation_config';
import { ParseTreeResult } from '../ml_parser/parser';
import { digest } from './digest';
import { mergeTranslations } from './extractor_merger';
import { Xliff } from './serializers/xliff';
import { Xliff2 } from './serializers/xliff2';
import { Xmb } from './serializers/xmb';
import { Xtb } from './serializers/xtb';
import { TranslationBundle } from './translation_bundle';
var I18NHtmlParser = /** @class */ (function () {
    function I18NHtmlParser(_htmlParser, translations, translationsFormat, missingTranslation, console) {
        if (missingTranslation === void 0) { missingTranslation = MissingTranslationStrategy.Warning; }
        this._htmlParser = _htmlParser;
        if (translations) {
            var serializer = createSerializer(translationsFormat);
            this._translationBundle =
                TranslationBundle.load(translations, 'i18n', serializer, missingTranslation, console);
        }
        else {
            this._translationBundle =
                new TranslationBundle({}, null, digest, undefined, missingTranslation, console);
        }
    }
    I18NHtmlParser.prototype.parse = function (source, url, options) {
        if (options === void 0) { options = {}; }
        var interpolationConfig = options.interpolationConfig || DEFAULT_INTERPOLATION_CONFIG;
        var parseResult = this._htmlParser.parse(source, url, tslib_1.__assign({ interpolationConfig: interpolationConfig }, options));
        if (parseResult.errors.length) {
            return new ParseTreeResult(parseResult.rootNodes, parseResult.errors);
        }
        return mergeTranslations(parseResult.rootNodes, this._translationBundle, interpolationConfig, [], {});
    };
    return I18NHtmlParser;
}());
export { I18NHtmlParser };
function createSerializer(format) {
    format = (format || 'xlf').toLowerCase();
    switch (format) {
        case 'xmb':
            return new Xmb();
        case 'xtb':
            return new Xtb();
        case 'xliff2':
        case 'xlf2':
            return new Xliff2();
        case 'xliff':
        case 'xlf':
        default:
            return new Xliff();
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaTE4bl9odG1sX3BhcnNlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uLy4uL3BhY2thZ2VzL2NvbXBpbGVyL3NyYy9pMThuL2kxOG5faHRtbF9wYXJzZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7Ozs7OztHQU1HOztBQUVILE9BQU8sRUFBQywwQkFBMEIsRUFBQyxNQUFNLFNBQVMsQ0FBQztBQUVuRCxPQUFPLEVBQUMsNEJBQTRCLEVBQUMsTUFBTSxtQ0FBbUMsQ0FBQztBQUUvRSxPQUFPLEVBQUMsZUFBZSxFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFHcEQsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLFVBQVUsQ0FBQztBQUNoQyxPQUFPLEVBQUMsaUJBQWlCLEVBQUMsTUFBTSxvQkFBb0IsQ0FBQztBQUVyRCxPQUFPLEVBQUMsS0FBSyxFQUFDLE1BQU0scUJBQXFCLENBQUM7QUFDMUMsT0FBTyxFQUFDLE1BQU0sRUFBQyxNQUFNLHNCQUFzQixDQUFDO0FBQzVDLE9BQU8sRUFBQyxHQUFHLEVBQUMsTUFBTSxtQkFBbUIsQ0FBQztBQUN0QyxPQUFPLEVBQUMsR0FBRyxFQUFDLE1BQU0sbUJBQW1CLENBQUM7QUFDdEMsT0FBTyxFQUFDLGlCQUFpQixFQUFDLE1BQU0sc0JBQXNCLENBQUM7QUFFdkQ7SUFNRSx3QkFDWSxXQUF1QixFQUFFLFlBQXFCLEVBQUUsa0JBQTJCLEVBQ25GLGtCQUFtRixFQUNuRixPQUFpQjtRQURqQixtQ0FBQSxFQUFBLHFCQUFpRCwwQkFBMEIsQ0FBQyxPQUFPO1FBRDNFLGdCQUFXLEdBQVgsV0FBVyxDQUFZO1FBR2pDLElBQUksWUFBWSxFQUFFO1lBQ2hCLElBQU0sVUFBVSxHQUFHLGdCQUFnQixDQUFDLGtCQUFrQixDQUFDLENBQUM7WUFDeEQsSUFBSSxDQUFDLGtCQUFrQjtnQkFDbkIsaUJBQWlCLENBQUMsSUFBSSxDQUFDLFlBQVksRUFBRSxNQUFNLEVBQUUsVUFBVSxFQUFFLGtCQUFrQixFQUFFLE9BQU8sQ0FBQyxDQUFDO1NBQzNGO2FBQU07WUFDTCxJQUFJLENBQUMsa0JBQWtCO2dCQUNuQixJQUFJLGlCQUFpQixDQUFDLEVBQUUsRUFBRSxJQUFJLEVBQUUsTUFBTSxFQUFFLFNBQVMsRUFBRSxrQkFBa0IsRUFBRSxPQUFPLENBQUMsQ0FBQztTQUNyRjtJQUNILENBQUM7SUFFRCw4QkFBSyxHQUFMLFVBQU0sTUFBYyxFQUFFLEdBQVcsRUFBRSxPQUE2QjtRQUE3Qix3QkFBQSxFQUFBLFlBQTZCO1FBQzlELElBQU0sbUJBQW1CLEdBQUcsT0FBTyxDQUFDLG1CQUFtQixJQUFJLDRCQUE0QixDQUFDO1FBQ3hGLElBQU0sV0FBVyxHQUFHLElBQUksQ0FBQyxXQUFXLENBQUMsS0FBSyxDQUFDLE1BQU0sRUFBRSxHQUFHLHFCQUFHLG1CQUFtQixxQkFBQSxJQUFLLE9BQU8sRUFBRSxDQUFDO1FBRTNGLElBQUksV0FBVyxDQUFDLE1BQU0sQ0FBQyxNQUFNLEVBQUU7WUFDN0IsT0FBTyxJQUFJLGVBQWUsQ0FBQyxXQUFXLENBQUMsU0FBUyxFQUFFLFdBQVcsQ0FBQyxNQUFNLENBQUMsQ0FBQztTQUN2RTtRQUVELE9BQU8saUJBQWlCLENBQ3BCLFdBQVcsQ0FBQyxTQUFTLEVBQUUsSUFBSSxDQUFDLGtCQUFrQixFQUFFLG1CQUFtQixFQUFFLEVBQUUsRUFBRSxFQUFFLENBQUMsQ0FBQztJQUNuRixDQUFDO0lBQ0gscUJBQUM7QUFBRCxDQUFDLEFBL0JELElBK0JDOztBQUVELFNBQVMsZ0JBQWdCLENBQUMsTUFBZTtJQUN2QyxNQUFNLEdBQUcsQ0FBQyxNQUFNLElBQUksS0FBSyxDQUFDLENBQUMsV0FBVyxFQUFFLENBQUM7SUFFekMsUUFBUSxNQUFNLEVBQUU7UUFDZCxLQUFLLEtBQUs7WUFDUixPQUFPLElBQUksR0FBRyxFQUFFLENBQUM7UUFDbkIsS0FBSyxLQUFLO1lBQ1IsT0FBTyxJQUFJLEdBQUcsRUFBRSxDQUFDO1FBQ25CLEtBQUssUUFBUSxDQUFDO1FBQ2QsS0FBSyxNQUFNO1lBQ1QsT0FBTyxJQUFJLE1BQU0sRUFBRSxDQUFDO1FBQ3RCLEtBQUssT0FBTyxDQUFDO1FBQ2IsS0FBSyxLQUFLLENBQUM7UUFDWDtZQUNFLE9BQU8sSUFBSSxLQUFLLEVBQUUsQ0FBQztLQUN0QjtBQUNILENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIEBsaWNlbnNlXG4gKiBDb3B5cmlnaHQgR29vZ2xlIEluYy4gQWxsIFJpZ2h0cyBSZXNlcnZlZC5cbiAqXG4gKiBVc2Ugb2YgdGhpcyBzb3VyY2UgY29kZSBpcyBnb3Zlcm5lZCBieSBhbiBNSVQtc3R5bGUgbGljZW5zZSB0aGF0IGNhbiBiZVxuICogZm91bmQgaW4gdGhlIExJQ0VOU0UgZmlsZSBhdCBodHRwczovL2FuZ3VsYXIuaW8vbGljZW5zZVxuICovXG5cbmltcG9ydCB7TWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3l9IGZyb20gJy4uL2NvcmUnO1xuaW1wb3J0IHtIdG1sUGFyc2VyfSBmcm9tICcuLi9tbF9wYXJzZXIvaHRtbF9wYXJzZXInO1xuaW1wb3J0IHtERUZBVUxUX0lOVEVSUE9MQVRJT05fQ09ORklHfSBmcm9tICcuLi9tbF9wYXJzZXIvaW50ZXJwb2xhdGlvbl9jb25maWcnO1xuaW1wb3J0IHtUb2tlbml6ZU9wdGlvbnN9IGZyb20gJy4uL21sX3BhcnNlci9sZXhlcic7XG5pbXBvcnQge1BhcnNlVHJlZVJlc3VsdH0gZnJvbSAnLi4vbWxfcGFyc2VyL3BhcnNlcic7XG5pbXBvcnQge0NvbnNvbGV9IGZyb20gJy4uL3V0aWwnO1xuXG5pbXBvcnQge2RpZ2VzdH0gZnJvbSAnLi9kaWdlc3QnO1xuaW1wb3J0IHttZXJnZVRyYW5zbGF0aW9uc30gZnJvbSAnLi9leHRyYWN0b3JfbWVyZ2VyJztcbmltcG9ydCB7U2VyaWFsaXplcn0gZnJvbSAnLi9zZXJpYWxpemVycy9zZXJpYWxpemVyJztcbmltcG9ydCB7WGxpZmZ9IGZyb20gJy4vc2VyaWFsaXplcnMveGxpZmYnO1xuaW1wb3J0IHtYbGlmZjJ9IGZyb20gJy4vc2VyaWFsaXplcnMveGxpZmYyJztcbmltcG9ydCB7WG1ifSBmcm9tICcuL3NlcmlhbGl6ZXJzL3htYic7XG5pbXBvcnQge1h0Yn0gZnJvbSAnLi9zZXJpYWxpemVycy94dGInO1xuaW1wb3J0IHtUcmFuc2xhdGlvbkJ1bmRsZX0gZnJvbSAnLi90cmFuc2xhdGlvbl9idW5kbGUnO1xuXG5leHBvcnQgY2xhc3MgSTE4Tkh0bWxQYXJzZXIgaW1wbGVtZW50cyBIdG1sUGFyc2VyIHtcbiAgLy8gQG92ZXJyaWRlXG4gIGdldFRhZ0RlZmluaXRpb246IGFueTtcblxuICBwcml2YXRlIF90cmFuc2xhdGlvbkJ1bmRsZTogVHJhbnNsYXRpb25CdW5kbGU7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgICBwcml2YXRlIF9odG1sUGFyc2VyOiBIdG1sUGFyc2VyLCB0cmFuc2xhdGlvbnM/OiBzdHJpbmcsIHRyYW5zbGF0aW9uc0Zvcm1hdD86IHN0cmluZyxcbiAgICAgIG1pc3NpbmdUcmFuc2xhdGlvbjogTWlzc2luZ1RyYW5zbGF0aW9uU3RyYXRlZ3kgPSBNaXNzaW5nVHJhbnNsYXRpb25TdHJhdGVneS5XYXJuaW5nLFxuICAgICAgY29uc29sZT86IENvbnNvbGUpIHtcbiAgICBpZiAodHJhbnNsYXRpb25zKSB7XG4gICAgICBjb25zdCBzZXJpYWxpemVyID0gY3JlYXRlU2VyaWFsaXplcih0cmFuc2xhdGlvbnNGb3JtYXQpO1xuICAgICAgdGhpcy5fdHJhbnNsYXRpb25CdW5kbGUgPVxuICAgICAgICAgIFRyYW5zbGF0aW9uQnVuZGxlLmxvYWQodHJhbnNsYXRpb25zLCAnaTE4bicsIHNlcmlhbGl6ZXIsIG1pc3NpbmdUcmFuc2xhdGlvbiwgY29uc29sZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX3RyYW5zbGF0aW9uQnVuZGxlID1cbiAgICAgICAgICBuZXcgVHJhbnNsYXRpb25CdW5kbGUoe30sIG51bGwsIGRpZ2VzdCwgdW5kZWZpbmVkLCBtaXNzaW5nVHJhbnNsYXRpb24sIGNvbnNvbGUpO1xuICAgIH1cbiAgfVxuXG4gIHBhcnNlKHNvdXJjZTogc3RyaW5nLCB1cmw6IHN0cmluZywgb3B0aW9uczogVG9rZW5pemVPcHRpb25zID0ge30pOiBQYXJzZVRyZWVSZXN1bHQge1xuICAgIGNvbnN0IGludGVycG9sYXRpb25Db25maWcgPSBvcHRpb25zLmludGVycG9sYXRpb25Db25maWcgfHwgREVGQVVMVF9JTlRFUlBPTEFUSU9OX0NPTkZJRztcbiAgICBjb25zdCBwYXJzZVJlc3VsdCA9IHRoaXMuX2h0bWxQYXJzZXIucGFyc2Uoc291cmNlLCB1cmwsIHtpbnRlcnBvbGF0aW9uQ29uZmlnLCAuLi5vcHRpb25zfSk7XG5cbiAgICBpZiAocGFyc2VSZXN1bHQuZXJyb3JzLmxlbmd0aCkge1xuICAgICAgcmV0dXJuIG5ldyBQYXJzZVRyZWVSZXN1bHQocGFyc2VSZXN1bHQucm9vdE5vZGVzLCBwYXJzZVJlc3VsdC5lcnJvcnMpO1xuICAgIH1cblxuICAgIHJldHVybiBtZXJnZVRyYW5zbGF0aW9ucyhcbiAgICAgICAgcGFyc2VSZXN1bHQucm9vdE5vZGVzLCB0aGlzLl90cmFuc2xhdGlvbkJ1bmRsZSwgaW50ZXJwb2xhdGlvbkNvbmZpZywgW10sIHt9KTtcbiAgfVxufVxuXG5mdW5jdGlvbiBjcmVhdGVTZXJpYWxpemVyKGZvcm1hdD86IHN0cmluZyk6IFNlcmlhbGl6ZXIge1xuICBmb3JtYXQgPSAoZm9ybWF0IHx8ICd4bGYnKS50b0xvd2VyQ2FzZSgpO1xuXG4gIHN3aXRjaCAoZm9ybWF0KSB7XG4gICAgY2FzZSAneG1iJzpcbiAgICAgIHJldHVybiBuZXcgWG1iKCk7XG4gICAgY2FzZSAneHRiJzpcbiAgICAgIHJldHVybiBuZXcgWHRiKCk7XG4gICAgY2FzZSAneGxpZmYyJzpcbiAgICBjYXNlICd4bGYyJzpcbiAgICAgIHJldHVybiBuZXcgWGxpZmYyKCk7XG4gICAgY2FzZSAneGxpZmYnOlxuICAgIGNhc2UgJ3hsZic6XG4gICAgZGVmYXVsdDpcbiAgICAgIHJldHVybiBuZXcgWGxpZmYoKTtcbiAgfVxufVxuIl19