from unittest import TestCase

from tbevents.event_bus.broker.rabbit.rabbit_settings import RabbitSettings


class TestRabbitSettings(TestCase):

    def test_valid_settings(self):
        RabbitSettings("localhost", "user_xyz", "password_xyz", port=5432)

    def test_settings_get(self):
        rs = RabbitSettings("localhost", "user_xyz", "password_xyz", port=5432, virtual_host="/test")
        assert rs.get_host() == "localhost"
        assert rs.get_port() == 5432
        assert rs.get_user() == "user_xyz"
        assert rs.get_password() == "password_xyz"
        assert rs.get_virtual_host() == "/test"

