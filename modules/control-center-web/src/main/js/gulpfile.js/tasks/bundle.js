var gulp = require('gulp');
var jspm = require('jspm');

gulp.task('bundle', function() {
	return jspm.bundleSFX('app/index', 'build/app.min.js', {})
});

