var gulp = require('gulp');
var inject = require('gulp-inject');

var common_options = {
    read: false
};

var html_targets = [
    './build/*.html',
    './build/**/*.html'
];

var js_targets = [
    './build/common-module.js'
];

var js_sources = [
    './build/plugins/**/*.js'
];

gulp.task('inject:plugins:html', function() {
    gulp.src(html_targets)
        .pipe(inject(gulp.src(js_sources, common_options), {
            starttag: '<!-- ignite:plugins-->',
            endtag: '<!-- endignite-->',
            transform: function (filePath, file, i, length) {
                // return file contents as string
                return '<script src="' + filePath.replace(/\/build(.*)/mgi, '$1') + '"></script>';
            }
        }))
        .pipe(gulp.dest('./build'));
});

gulp.task('inject:plugins:js', function() {
    gulp.src(js_targets)
        .pipe(inject(gulp.src(js_sources, common_options), {
            starttag: '/* ignite:plugins */',
            endtag: ' /* endignite */',
                transform: function (filePath, file, i, length) {
                // return file contents as string
                return ", '" + filePath.replace(/.*plugins\/([^\/]+).*/mgi, '$1') + "'";
            }
        }))
        .pipe(gulp.dest('./build'));
});

gulp.task('inject:plugins', ['inject:plugins:html', 'inject:plugins:js']);
