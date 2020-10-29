# -*- coding: utf-8 -*-
from datetime import datetime
from pydantic import BaseModel
from tbevents.event_bus.models.event import Event
from tbevents.tb_worker import TbWorker

class Sample(BaseModel):
    SampleStringField: str
    SampleDateField: datetime


def callback_function(e: Event, s:Sample) -> bool:
    print(f'Todo payload: {e.payload}')
    print(f'Campo string: {s.SampleStringField}')
    print(f'Campo data: {s.SampleDateField}')
    return True


w = TbWorker("example_worker_2")

w.append_listening_event("my_fancy_event", callback_function)

w.start_listening()
