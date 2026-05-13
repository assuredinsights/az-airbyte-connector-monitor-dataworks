import logging
import azure.functions as func
from airbyte_connectors_monitor import run_clients_by_schedule_group

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 8 * * 1-5", arg_name="myTimer", run_on_startup=False,
              use_monitor=True) 
def airbyte_monitor_daily8am(myTimer: func.TimerRequest) -> None:
    logging.info("Running Airbyte daily 8 am schedule group")
    results = run_clients_by_schedule_group("daily_8am")
    logging.info("Completed Airbyte schedule group daily8am. Results: %s", results)

@app.timer_trigger(schedule="0 0 9 * * 1", arg_name="myTimer", run_on_startup=False,
              use_monitor=True) 
def airbyte_monitor_monday_8am(myTimer: func.TimerRequest) -> None:
    logging.info("Running Airbyte schedule group: monday_8am")
    results = run_clients_by_schedule_group("monday_8am")
    logging.info("Completed Airbyte schedule group monday_8am. Results: %s", results)


# @app.timer_trigger(schedule="0 0 */6 * * *", arg_name="myTimer", run_on_startup=True,
#               use_monitor=False) 
# def airbyte_monitor_every_6_hours(myTimer: func.TimerRequest) -> None:
#     logging.info("Running Airbyte every 6 hours schedule group")
#     run_clients_by_schedule_group("every_6_hours")
#     logging.info('Python timer trigger function airbyte_monitor_every_6_hours executed.')