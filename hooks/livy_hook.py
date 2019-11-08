import logging
from airflow.hooks.http_hook import HttpHook


class LivySessionError(Exception):
    pass


class LivyHook(HttpHook):
    _ok_response_codes = [
        200,
        201,
    ]
    _session_kinds [
        "spark",
        "pyspark",
        "sparkr",
    ]

    def __init__(
        self,
        livy_conn_id='http_default'
    ):
        self.livy_conn_id = livy_conn_id
        self._retry_obj = None
        self.base_url = None
        self.method = "GET"
        self.headers = {'Content-Type': 'application/json'}


    def get_conn(self, headers=None):
        """
        Returns http session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        session = requests.Session()
        if self.livy_conn_id:
            conn = self.get_connection(self.livy_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            # Port defaults to 8998
            port = conn.port or 8998
            self.base_url = self.base_url + ":" + str(port)

            # @@TODO: Livy supports SPNEGO authentication.  Implement later if possible.
            #if conn.login:
                #session.auth = (conn.login, conn.password)

            if conn.extra:
                try:
                    session.headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning('Connection to %s has invalid extra field.', conn.host)
        if headers:
            session.headers.update(headers)

        return session


        def create_session(
            self,
            session_kind=None,
            polling_interval=10
        ):
            data = {}

            # Legacy Support: 'kind' is not required for Livy 0.5.0-incubating and later.
            if session_kind not in self._session_kinds:
                logging.warn("Livy Session Kind not specified.  Are you running >=0.5.0?"
            if session_kind:
                data["kind"] = session_kind

            self.method = "POST"
            endpoint = "sessions"

            logging.debug("Creating Livy Session")
            response = self.run("sessions", json.dumps(data))
            if response.status_code in self._ok_response_codes:
                response_json = response.json()
                session_id = response_json["id"]
                session_state = response_json["state"]
                if session_state in [
                    "shutting_down",
                    "killed",
                    "dead",
                    "error",
                ]:
                    raise LivySessionError(f"Session failed to start with status '{session_state}'")

                elif session_state == "starting":
                    logging.info("Session is starting. Polling to see if it is ready...")
                    while session_state == "starting":
                        logging.info(f"Sleeping for {polling_interval} seconds")
                        time.sleep(polling_interval)
                        session_state_check_response = self._get_session(session_id=session_id)
                        session_state = session_state_check_response["state"]
                        logging.info("Got latest session state as '" + session_state + "'")
                logging.debug(f"Livy Session Created: ID = {session_id}")
                return session_id
            else:
                raise LivySessionError(f"Call to create new session returned {response.status_code}")