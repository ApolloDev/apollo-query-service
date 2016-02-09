#!/usr/bin/env python
import web
import requests

urls = (
    '/query/(.*)', 'run_query'
)

app = web.application(urls, globals())

class run_query:
    def POST(self, run_id):
        data = web.input()
        username = data.username
        password = data.password

        # first need to get scos message
        # contact run manager service
        r = requests.get('http://localhost:8080/run-manager-service-rest-frontend-4.0-SNAPSHOT/ws/run/' \
                         + run_id + '/files?username=' + username + '&password=' + password)


	return run_id

if __name__ == "__main__":
    app.run()