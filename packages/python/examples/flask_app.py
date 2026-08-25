"""Mounting into a Flask app you already have, rather than letting the
client start its own server. Useful when the process is already a web
service and you want deliveries on the same port.

    python examples/flask_app.py
"""
import os
import threading

from flask import Flask
from werkzeug.serving import make_server

from sukkal import Client, Receiver

app = Flask(__name__)


@app.get("/")
def index():
    return "an ordinary app\n"


# Passing `app` means the Receiver mounts its delivery route and leaves
# listening to you.
receiver = Receiver(app=app, mount_path="/hooks/sukkal")

PORT = int(os.environ.get("PORT", 3000))
server = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=server.serve_forever, daemon=True).start()
print(f"app on :{PORT}", flush=True)

receiver.port = PORT

client = Client(
    os.environ.get("SUKKAL_URL", "http://127.0.0.1:8080"),
    receiver=receiver,
    # What to put in the callback URL, when the broker reaches this
    # service by a name rather than by the address we happen to bind.
    advertise=os.environ.get("ADVERTISE_HOST"),
)

def show(msg):
    print(f"{msg.subject} #{msg.index} {msg.value!r}", flush=True)


client.subscribe("orders.>", show, consumer="orders-service")
print("subscribed; deliveries arrive on /hooks/sukkal/orders-service", flush=True)

threading.Event().wait()
