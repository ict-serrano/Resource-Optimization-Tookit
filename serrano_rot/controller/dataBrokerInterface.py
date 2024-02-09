import pika
import json
import logging

import serrano_rot.utils.constants as constants


logger = logging.getLogger("SERRANO.ROT.DataBrokerInterface")


def ExecutionResponseHander(config, message):

    connection_parameters = pika.ConnectionParameters(host=config["address"],
                                                      virtual_host=config["virtual_host"],
                                                      credentials=pika.PlainCredentials(config["username"],
                                                                                        config["password"]),
                                                      blocked_connection_timeout=5,
                                                      socket_timeout=None,
                                                      heartbeat=15)

    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange=constants.DISPATCHER_RESULTS_EXCHANGE, exchange_type='direct')

    logger.debug("Forward execution response '%s' to client '%s'" % (message["uuid"], message["client_uuid"]))
    channel.basic_publish(exchange=constants.DISPATCHER_RESULTS_EXCHANGE,
                          routing_key=message["client_uuid"],
                          body=json.dumps(message))

    connection.close()

