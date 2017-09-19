#!/usr/local/bin/python
from lambda2 import subscriber



class Discover():
    @staticmethod
    def get_id():
        return "LAMBAD2_DISCOVER_EVENT"

    @staticmethod
    def mount(server):
        subscriber.Subscriber().publish(Discover.get_id(), server)
