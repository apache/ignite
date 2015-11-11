var gulp     = require('gulp');
var jade     = require('gulp-jade');
var sequence = require('gulp-sequence');

var paths = [
    '!./views/error.jade',
    './views/*.jade',
    './views/**/*.jade'
];

var options = {
};

gulp.task('jade', function() {
    return gulp.src(paths)
        .pipe(jade(options))
        .pipe(gulp.dest('./build'))
});

gulp.task('jade:watch', function () {
    return gulp.watch(paths, function(e) {
        sequence('jade', 'inject:plugins:html')()
    });
});
