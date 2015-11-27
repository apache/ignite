var gulp = require('gulp');
var sequence = require('gulp-sequence');
var environments = require('gulp-environments');

var production = environments.production;

gulp.task('set-prod', production.task)

gulp.task('production', function(cb) {
	sequence('set-prod', 'build', cb)
});