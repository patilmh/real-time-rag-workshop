import azure.functions as func
import logging

from run_batch import build_flow
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="MyHttpTrigger")
def MyHttpTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        # Workaround for executing bytewax because 
        # 'python -m bytewax.run' does not work inside a Azure function
        flow = build_flow(latest_n_days=1, debug=True)
        run_main(flow)
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    
# Python command
# func host start