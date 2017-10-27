import attr


@attr.s
class Collector:

    event_stream = attr.ib()
    types = attr.ib(default=attr.Factory(list))
    filters = attr.ib(default=attr.Factory(list))

    def __attrs_post_init__(self):
        if self.types:
            self.event_stream = self.event_stream.filter(
                lambda ev: ev.__class__.__name__ in set(self.types))
        for _filter in self.filters:
            self.event_stream = self.event_stream.filter(
                lambda x: eval(_filter, attr.asdict(x)))
