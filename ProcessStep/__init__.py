import json
import logging

import azure.functions as func
from datetime import datetime
from opentelemetry.propagate import extract

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace


configure_azure_monitor()
tracer = trace.get_tracer(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

@tracer.start_as_current_span("ProcessStepFunction")
def main(
    event: func.EventHubEvent,
    outputServiceBusMessage: func.Out[str],
    context: func.Context,
):
        logging.info(f"Python EventHub trigger processed event")
        with tracer.start_as_current_span("readEvent"):
            logging.info("in readEvent span")
            content = event.get_body().decode("utf-8")
            dict_content = json.loads(content)

        with tracer.start_as_current_span("processEvent"):
            logging.info("in processEvent span")
            try:
                logging.info("Processing the received event")
                dict_content.pop("user_id")
                date_to_process = dict_content["date"]
                new_date = datetime.strptime(date_to_process, "%Y-%m-%d %H:%M:%S")
                dict_content["date"] = new_date.strftime("%Y-%m-%d")
                content = json.dumps(dict_content)
            except Exception as e:
                logging.exception(e)
                raise e

        with tracer.start_as_current_span("sendMessages"):
            logging.info("in sendMessages span")
            logging.info("Publishing message to Service Bus Queue")
            outputServiceBusMessage.set(content)
