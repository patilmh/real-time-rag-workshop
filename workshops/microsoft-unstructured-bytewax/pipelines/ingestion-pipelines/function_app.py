###################################################################
# Azure function with timer trigger - run every 3 hours

from datetime import datetime, timezone
import logging
import azure.functions as func

# from local_dataflow import test_flow
from run_batch import build_flow
from bytewax.testing import run_main

app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.timer_trigger(schedule="0 0 0/3 * * *", 
                    arg_name="mytimer",
                    run_on_startup=False)
def alpaca_timer_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.now(timezone.utc)
    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    ## Workaround for executing bytewax because 
    ## 'python -m bytewax.run' does not work inside a Azure function
    # flow = test_flow(debug=False)
    flow = build_flow(latest_n_days=3/24, debug=False)
    run_main(flow)

    logging.info(f"Python timer trigger function ending")

    if mytimer.past_due:
        logging.info('The timer is past due!')


###################################################################
# Azure function with HTTP trigger

# import azure.functions as func
# import logging

# from run_batch import build_flow
# from bytewax.dataflow import Dataflow
# from bytewax.testing import run_main

# app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# @app.route(route="MyHttpTrigger")
# def MyHttpTrigger(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')

#     name = req.params.get('name')
#     if not name:
#         try:
#             req_body = req.get_json()
#         except ValueError:
#             pass
#         else:
#             name = req_body.get('name')

#     if name:
#         # Workaround for executing bytewax because 
#         # 'python -m bytewax.run' does not work inside a Azure function
#         flow = build_flow(latest_n_days=1, debug=True)
#         run_main(flow)
#         return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
#     else:
#         return func.HttpResponse(
#              "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
#              status_code=200
#         )

###################################################################
# Azure durable function with HTTP trigger

# import azure.functions as func
# import azure.durable_functions as df

# myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# # An HTTP-triggered function with a Durable Functions client binding
# @myApp.route(route="orchestrators/{functionName}")
# @myApp.durable_client_input(client_name="client")
# async def http_start(req: func.HttpRequest, client):
#     function_name = req.route_params.get('functionName')
#     instance_id = await client.start_new(function_name)
#     response = client.create_check_status_response(req, instance_id)
#     return response

# # Orchestrator
# @myApp.orchestration_trigger(context_name="context")
# def hello_orchestrator(context):
#     result1 = yield context.call_activity("hello", "Seattle")
#     result2 = yield context.call_activity("hello", "Tokyo")
#     result3 = yield context.call_activity("hello", "London")

#     return [result1, result2, result3]

# # Activity
# @myApp.activity_trigger(input_name="city")
# def hello(city: str):
#     return f"Hello {city}"

###################################################################
# Azure durable function with eternal orchestration
# DOES NOT WORK - Timer does not stop once started

# import azure.functions as func
# import azure.durable_functions as df
# from datetime import datetime, timedelta
# import logging

# myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# @myApp.route(route="orchestrators/{functionName}")
# @myApp.durable_client_input(client_name="client")
# async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
#     import string
#     import random

#     # using random.choices() generating random strings
#     instance_id = ''.join(random.choices(string.ascii_letters,
#                                 k=16)) # initializing size of string
#     function_name = req.route_params.get('functionName')
#     await client.start_new(function_name, instance_id, None)

#     logging.info(f"Started orchestration with ID = '{instance_id}'")
#     response = client.create_check_status_response(req, instance_id)
#     return response

# @myApp.orchestration_trigger(context_name="context")
# def orchestrator_function(context: df.DurableOrchestrationContext):
#     time_str = context.current_utc_datetime.strftime("%m/%d/%Y, %H:%M:%S")
#     result = yield context.call_activity("hello", time_str)
#     logging.info(f"result={result}")

#     # sleep for one min between calls
#     next_call = context.current_utc_datetime + timedelta(minutes = 1)
#     yield context.create_timer(next_call)

#     context.continue_as_new(None)
#     return result

# @myApp.activity_trigger(input_name="time")
# def hello(time: str):
#     return f"Hello {time}"


    
# Python command
# func host start