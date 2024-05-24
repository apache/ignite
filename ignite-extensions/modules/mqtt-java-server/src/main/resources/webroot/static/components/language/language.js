// global locale messages
var locale_messages = {};

function getLanguage() {
    var language = null;

    if (navigator.language) {
        language = navigator.language;
    } else if (navigator.browserLanguage) {
        language = navigator.browserLanguage;
    } else if (navigator.userLanguage) {
        language = navigator.userLanguage;
    } else if (navigator.systemLanguage) {
        language = navigator.systemLanguage;
    }

    if (language) {
        // to lower case
        language = language.toLowerCase();

        if (language.startsWith('zh')) {
            language = 'zh';
        } else {
            language = 'en';
        }
    } else {
        language = 'en';
    }

    return language;
}

function loadLanguage(language, callback) {
    var lang = language.toLowerCase();
    var file = '/static/i18n/messages.' + lang + '.js';

    $.getScript(file, function() {
        if (callback && callback instanceof Function) {
            callback();
        }
    });
}

function changeLanguage() {
    $("[data-locale]").each(function() {
        var _this = $(this);

        if (_this.is('input')) {
            _this.attr('placeholder', locale_messages[_this.attr('data-locale')]);
        } else {
            _this.text(locale_messages[_this.attr('data-locale')]);
        }
    });
}

function loadAndChangeLanguage(language) {
    var lang = language.toLowerCase();
    var file = '/static/i18n/messages.' + lang + '.js';

    $.getScript(file, function() {
        $("[data-locale]").each(function() {
            var _this = $(this);

            if (_this.is('input')) {
                _this.attr('placeholder', locale_messages[_this.attr('data-locale')]);
            } else {
                _this.text(locale_messages[_this.attr('data-locale')]);
            }
        });
    });
}