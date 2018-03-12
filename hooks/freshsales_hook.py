from airflow.hooks.http_hook import HttpHook


class FreshSalesHook(HttpHook):

    def __init__(self, freshsales_conn_id):
        self.freshsales_token = None
        conn_id = self.get_connection(freshsales_conn_id)
        if conn_id.extra_dejson.get('token'):
            self.freshsales_token = conn_id.extra_dejson.get('token')
        super().__init__(method='GET', http_conn_id=freshsales_conn_id)

    def get_conn(self, headers):
        """
        Accepts Token Authentication.
        If a token exists in the "Extras" section
        with key "token", it is passed in the header.
        """
        if self.freshsales_token:
            headers = {'Authorization': 'Token token={0}'.format(self.freshsales_token),
                       'Content-Type': 'application/json'}
            session = super().get_conn(headers)
            session.auth = None
            return session
        return super().get_conn(headers)
