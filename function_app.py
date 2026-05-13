import logging
import azure.functions as func
from airbyte_connectors_monitor import run_clients_by_schedule_group

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 10 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def airbyte_monitor_daily10am(myTimer: func.TimerRequest) -> None:
    logging.info("Running Airbyte daily 10 am schedule group")
    run_clients_by_schedule_group("daily_10am")
    logging.info('Python timer trigger function airbyte_monitor_daily10am executed.')


@app.timer_trigger(schedule="0 0 */6 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def airbyte_monitor_every_6_hours(myTimer: func.TimerRequest) -> None:
    logging.info("Running Airbyte every 6 hours schedule group")
    run_clients_by_schedule_group("every_6_hours")
    logging.info('Python timer trigger function airbyte_monitor_every_6_hours executed.')