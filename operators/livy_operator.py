from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.utils import apply_defaults
import logging
import textwrap
import time
import json


class LivySparkOperator(BaseOperator):
    """
   Operator to facilitate interacting with the Livy Server which executes Apache Spark code via a REST API.

   :param spark_script: Scala, Python or R code to submit to the Livy Server (templated)
   :type spark_script: string
   :param session_kind: Type of session to setup with Livy. This will determine which type of code will be accepted. Possible values include "spark" (executes Scala code), "pyspark" (executes Python code) or "sparkr" (executes R code).
   :type session_kind: string
   :param http_conn_id: The http connection to run the operator against
   :type http_conn_id: string
   :param poll_interval: The polling interval to use when checking if the code in spark_script has finished executing. In seconds. (default: 30 seconds)
   :type poll_interval: integer
   """

    template_fields = ['spark_script']  # todo : make sure this works
    template_ext = ['.py', '.R', '.r']
    ui_color = '#34a8dd'  # Clouderas Main Color: Blue

    acceptable_response_codes = [200, 201]
    statement_non_terminated_status_list = ['waiting', 'running']

    @apply_defaults
    def __init__(
            self,
            spark_script,
            session_kind="spark",  # spark, pyspark, or sparkr
            http_conn_id='http_default',
            poll_interval=30,
            *args, **kwargs):
        super(LivySparkOperator, self).__init__(*args, **kwargs)

        self.spark_script = spark_script
        self.session_kind = session_kind
        self.http_conn_id = http_conn_id
        self.poll_interval = poll_interval

        self.http = HttpHook("GET", http_conn_id=self.http_conn_id)

    def execute(self, context):
        logging.info("Executing LivySparkOperator.execute(context)")

        logging.info("Validating arguments...")
        self._validate_arguments()
        logging.info("Finished validating arguments")

        logging.info("Creating a Livy Session...")
        session_id = self._create_session()
        logging.info("Finished creating a Livy Session. (session_id: " + str(session_id) + ")")

        logging.info("Submitting spark script...")
        statement_id, overall_statements_state = self._submit_spark_script(session_id=session_id)
        logging.info("Finished submitting spark script. (statement_id: " + str(statement_id) + ", overall_statements_state: " + str(overall_statements_state) + ")")

        poll_for_completion = (overall_statements_state in self.statement_non_terminated_status_list)

        if poll_for_completion:
            logging.info("Spark job did not complete immediately. Starting to Poll for completion...")

        while overall_statements_state in self.statement_non_terminated_status_list:  # todo: test execution_timeout
            logging.info("Sleeping for " + str(self.poll_interval) + " seconds...")
            time.sleep(self.poll_interval)
            logging.info("Finished sleeping. Checking if Spark job has completed...")
            statements = self._get_session_statements(session_id=session_id)

            is_all_complete = True
            for statement in statements:
                if statement["state"] in self.statement_non_terminated_status_list:
                    is_all_complete = False

                # In case one of the statements finished with errors throw exception
                elif statement["state"] != 'available' or statement["output"]["status"] == 'error':
                    logging.error("Statement failed. (state: " + str(statement["state"]) + ". Output:\n" +
                                  str(statement["output"]))
                    response = self._close_session(session_id=session_id)
                    logging.error("Closed session. (response: " + str(response) + ")")
                    raise Exception("Statement failed. (state: " + str(statement["state"]) + ". Output:\n" +
                                    str(statement["output"]))

            if is_all_complete:
                overall_statements_state = "available"

            logging.info("Finished checking if Spark job has completed. (overall_statements_state: " + str(overall_statements_state) + ")")

        if poll_for_completion:
            logging.info("Finished Polling for completion.")

        logging.info("Session Logs:\n" + str(self._get_session_logs(session_id=session_id)))

        for statement in self._get_session_statements(session_id):
            logging.info("Statement '" + str(statement["id"]) + "' Output:\n" + str(statement["output"]))

        logging.info("Closing session...")
        response = self._close_session(session_id=session_id)
        logging.info("Finished closing session. (response: " + str(response) + ")")

        logging.info("Finished executing LivySparkOperator.execute(context)")

    def _validate_arguments(self):
        if self.session_kind is None or self.session_kind == "":
            raise Exception(
                "session_kind argument is invalid. It is empty or None. (value: '" + str(self.session_kind) + "')")
        elif self.session_kind not in ["spark", "pyspark", "sparkr"]:
            raise Exception(
                "session_kind argument is invalid. It should be set to 'spark', 'pyspark', or 'sparkr'. (value: '" + str(
                    self.session_kind) + "')")

    def _get_sessions(self):
        method = "GET"
        endpoint = "sessions"
        response = self._http_rest_call(method=method, endpoint=endpoint)

        if response.status_code in self.acceptable_response_codes:
            return response.json()["sessions"]
        else:
            raise Exception("Call to get sessions didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _get_session(self, session_id):
        sessions = self._get_sessions()
        for session in sessions:
            if session["id"] == session_id:
                return session

    def _get_session_logs(self, session_id):
        method = "GET"
        endpoint = "sessions/" + str(session_id) + "/log"
        response = self._http_rest_call(method=method, endpoint=endpoint)
        return response.json()

    def _create_session(self):
        method = "POST"
        endpoint = "sessions"

        data = {
            "kind": self.session_kind
        }

        response = self._http_rest_call(method=method, endpoint=endpoint, data=data)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            session_id = response_json["id"]
            session_state = response_json["state"]

            if session_state == "starting":
                logging.info("Session is starting. Polling to see if it is ready...")

            session_state_polling_interval = 10
            while session_state == "starting":
                logging.info("Sleeping for " + str(session_state_polling_interval) + " seconds")
                time.sleep(session_state_polling_interval)
                session_state_check_response = self._get_session(session_id=session_id)
                session_state = session_state_check_response["state"]
                logging.info("Got latest session state as '" + session_state + "'")

            return session_id
        else:
            raise Exception("Call to create a new session didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _submit_spark_script(self, session_id):
        method = "POST"
        endpoint = "sessions/" + str(session_id) + "/statements"

        logging.info("Executing Spark Script: \n" + str(self.spark_script))

        data = {
            'code': textwrap.dedent(self.spark_script)
        }

        response = self._http_rest_call(method=method, endpoint=endpoint, data=data)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            return response_json["id"], response_json["state"]
        else:
            raise Exception("Call to create a new statement didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _get_session_statements(self, session_id):
        method = "GET"
        endpoint = "sessions/" + str(session_id) + "/statements"
        response = self._http_rest_call(method=method, endpoint=endpoint)

        if response.status_code in self.acceptable_response_codes:
            response_json = response.json()
            statements = response_json["statements"]
            return statements
        else:
            raise Exception("Call to get the session statement response didn't return " + str(self.acceptable_response_codes) + ". Returned '" + str(response.status_code) + "'.")

    def _close_session(self, session_id):
        method = "DELETE"
        endpoint = "sessions/" + str(session_id)
        return self._http_rest_call(method=method, endpoint=endpoint)

    def _http_rest_call(self, method, endpoint, data=None, headers=None, extra_options=None):
        if not extra_options:
            extra_options = {}
        logging.debug("Performing HTTP REST call... (method: " + str(method) + ", endpoint: " + str(endpoint) + ", data: " + str(data) + ", headers: " + str(headers) + ")")
        self.http.method = method
        response = self.http.run(endpoint, json.dumps(data), headers, extra_options=extra_options)

        logging.debug("status_code: " + str(response.status_code))
        logging.debug("response_as_json: " + str(response.json()))

        return response