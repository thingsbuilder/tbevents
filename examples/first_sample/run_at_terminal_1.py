# -*- coding: utf-8 -*-
import uuid


from tbevents.tb_worker import TbWorker

sample1 = {
    "sample_payload": {
        "field_1": 0,
        "field_2": ""
    }
}


def publisher():
    w = TbWorker("example_worker_1")
    w.append_publish_event(event_name="my_fancy_event")

    for i in range(0, 10):
        sample1['sample_payload']['field_1'] = i
        sample1['sample_payload']['field_2'] = str(uuid.uuid4())
        print("send payload %s", sample1)
        w.send_event("my_fancy_event", f"{i}", "fancy-aggregator", sample1)


if __name__ == "__main__":
    publisher()