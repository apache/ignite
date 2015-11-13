var gulp = require('gulp');
var jspm = require('jspm');

gulp.task('bundle', function() {
	jspm.bundleSFX('app/index', 'build/app.min.js', {
		
	})
})

