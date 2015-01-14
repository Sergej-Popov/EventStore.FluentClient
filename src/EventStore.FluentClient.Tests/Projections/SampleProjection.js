fromStream('QueryTestStream')
.when({
    $init: function (s, e) {
        return {
            count: 0
        };
    },
    $any: function (s, e) {
        s.count += 1;
    }
});