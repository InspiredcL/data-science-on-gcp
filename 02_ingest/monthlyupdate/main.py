#!/usr/bin/env python


'''
_summary_

Returns:
    _description_
'''
# Copyright 2016-2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
from flask import Flask
from flask import request
from markupsafe import escape
from ingest_flights import ingest, next_month

app = Flask(__name__)


@app.route("/", methods=['POST'])
def ingest_flights():
    '''Wrapper de la función ingest_flights
    '''

    # noinspection PyBroadException
    try:
        logging.basicConfig(format='%(levelname)s: %(message)s',
                            level=logging.INFO)
        # https://stackoverflow.com/questions/53216177/http-triggering-cloud-function-with-cloud-scheduler/60615210#60615210
        json = request.get_json(force=True)

        year = escape(json['year']) if 'year' in json else None
        month = escape(json['month']) if 'month' in json else None
        bucket = escape(json['bucket'])  # required

        if year is None or month is None or len(year) == 0 or len(month) == 0:
            year, month = next_month(bucket)
        logging.debug('Ingesting year=%s month=%s', year, month)
        tableref, numrows = ingest(year, month, bucket)
        ok = f'Success ... ingested {numrows} rows to {tableref}'
        logging.info(ok)
        return ok
    except Exception as e:
        # Ver como incluir el mensaje
        logging.exception("Failed to ingest ... try again later?")
        raise ValueError(f"Error: {str(e)}") from e


if __name__ == "__main__":
    app.run(
        debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080))
    )
# host en 0 es acceso para todos.
