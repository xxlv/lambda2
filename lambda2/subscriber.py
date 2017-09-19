# -*- coding: utf-8 -*-

from lambda2 import driver


class Subscriber():
    def __init__(self):
        self.driver = driver.Driver.get_driver()

    def accept(self):
        pass

    def publish(self, event, data):
        return self.driver.publish(event, data)
