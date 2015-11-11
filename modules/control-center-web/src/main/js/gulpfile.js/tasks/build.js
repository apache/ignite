var gulp = require('gulp');
var gulpSequence = require('gulp-sequence');

gulp.task('build', function(cb) {
    gulpSequence('clean', ['copy', 'jade', 'sass'], 'inject:plugins', cb)
});
