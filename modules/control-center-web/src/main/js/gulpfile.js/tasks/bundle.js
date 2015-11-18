var gulp = require('gulp');
var jspm = require('jspm');


var paths = [
    './app/**/*.js'
];

gulp.task('bundle', function() {
	return jspm.bundleSFX('app/index', 'build/app.min.js', {})
});

gulp.task('bundle:watch', function() {
	gulp.watch(paths, ['bundle'])
})