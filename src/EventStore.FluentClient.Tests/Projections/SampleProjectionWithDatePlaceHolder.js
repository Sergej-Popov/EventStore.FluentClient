fromStream('QueryTestStreamForPlaceholderProjection')
.when({
    $init: function (s, e) {
        return {
            count: 0
        };
    },
    $any: function (s, e) {
        if (new Date(e.body) > new Date("$0$"))
            s.count += 1;
    }
});