import logging


class NotionEtlLogAdapter(logging.LoggerAdapter):
    """Logging adapter to add ETL job name to all logs."""

    def process(self, msg: str, kwargs):
        return "[%s] %s" % (self.extra["name"], msg), kwargs
