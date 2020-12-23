import base64
import os

from flask import Flask, request

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():
    req = request.get_json()
    resp = {'status': 'Done', 'errMsg': ''}

    if not req:
        msg = 'No Pub/Sub Message Received'
        print(msg)
        resp['status'] = 'Bad Request'
        resp['errMsg'] = msg
        return (resp, 400)

    if not isinstance(req, dict) or 'message' not in req:
        msg = 'invalid Pub/Sub message format'
        print(f'{msg}, req:\n{req}')
        resp['status'] = 'Bad Request'
        resp['errMsg'] = msg
        return (resp, 400)

    message = req['message']
    print(f'message type: {type(message)}, body:\n{message}')
    data = base64.b64decode(message["data"]).decode("utf-8").strip()
    print(f'data type: {type(data)}, value:\n{data}')
    print(f"attributes type: {type(message['attributes'])}, value:\n{message['attributes']}")

    return (resp, 200)

if __name__ == '__main__':
    port = int(os.getenv('PORT')) if os.getenv('PORT') else 8080
    app.run(host='0.0.0.0', port=port)
