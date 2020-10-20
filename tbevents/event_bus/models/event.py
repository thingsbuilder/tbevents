import datetime
from dateutil import parser

from tbevents.event_bus.utils.logger import logger


class Event:

    def __init__(self, **kwargs):
        try:
            self.event_name = kwargs.get("event_name", "")
            self.aggregate_id = kwargs.get("aggregate_id", "")
            self.aggregate_type = kwargs.get("aggregate_type", "")
            self.sender = kwargs.get("sender", "")
            date_ = kwargs.get("date_time")
            if isinstance(date_, datetime.date):
                self.date_time = date_
            else:
                self.date_time = parser.parse(kwargs.get("date_time"))
            self.payload = kwargs.get("payload", "")
            self.version = kwargs.get("version", "")
            if self.event_name == "" or self.aggregate_id == "" or self.aggregate_type == "" or self.sender == "" or \
                    self.payload == "" or self.version == "":
                raise
        except:
            raise ValueError(f"{kwargs} is not a valid Event") from None
