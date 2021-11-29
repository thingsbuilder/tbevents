# tbevents

tbevents is a library that makes it possible to create an event-drive system in Python.

A messaging service such as Rabbit is used to transit events.

A [series of articles on Medium](https://medium.com/@eskelsen/uma-arquitectura-simples-e-eficiente-para-sistemas-event-driven-em-python-parte-i-5eb59336d858) describes its use

#### Changelog:

#####28/11/2021
- Now, by default, new topic aren't announced in new_event_topic_created/event_store.
To announce, you must specify announce_new_topic=True on TbWorker declaration, like that: `w = TbWorker("example_worker_1", announce_new_topic=True)`
- Now you can declare a prefix to Rabbit Exchange names creating an environment variable: `export RBMQ_PREFIX="my_prefix/"`  