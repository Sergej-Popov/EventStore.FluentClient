fromCategory('LinkToProjectionOriginalStream')
.when({
    $init: function(s, e) {
        return {};
    },
    'SampleEvent': function (s, e) {
        linkTo('ProjectedStream', e);
    }
});