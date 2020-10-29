# -*- coding: utf-8 -*-
from tbevents.event_bus.models.event import Event
from tbevents.tb_worker import TbWorker


def callback_function(e: Event) -> bool:
    print(e.payload)
    return True


w = TbWorker("example_worker_2")

w.append_listening_event("my_fancy_event", callback_function)

w.start_listening()
