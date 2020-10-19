# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
import os


class BrokerSettings(ABC):
    @abstractmethod
    def __init__(self, host=None, user=None, password=None, port=None):
        pass

    @abstractmethod
    def get_host(self):
        pass

    @abstractmethod
    def get_user(self):
        pass

    @abstractmethod
    def get_password(self):
        pass

    @abstractmethod
    def get_port(self):
        pass

    @abstractmethod
    def get_virtual_host(self):
        pass

