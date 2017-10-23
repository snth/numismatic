import attr


@attr.s
class Collector:

    source_stream = attr.ib()
    types = attr.ib(default=attr.Factory(list))
    filters = attr.ib(default=attr.Factory(list))

    # FIXME: Add a factory method

    def __attrs_post_init__(self):
        if self.types:
            self.source_stream = self.source_stream.filter(
                lambda ev: ev.__class__.__name__ in set(self.types))
        for _filter in self.filters:
            self.source_stream = self.source_stream.filter(
                lambda x: eval(_filter, attr.asdict(x)))
