# CST8916 – Week 10 Lab: Clickstream Analytics with Azure Event Hubs
#
# This Flask app has two roles:
#   1. PRODUCER  – receives click events from the browser and sends them to Azure Event Hubs
#   2. CONSUMER  – reads the last N events from Event Hubs and serves a live dashboard
#
# Routes:
#   GET  /              → serves the demo e-commerce store (client.html)
#   POST /track         → receives a click event and publishes it to Event Hubs
#   GET  /dashboard     → serves the live analytics dashboard (dashboard.html)
#   GET  /api/events    → returns recent events as JSON (polled by the dashboard)

import json
import os
import threading
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Azure Event Hubs SDK
# EventHubProducerClient  – sends events to Event Hubs
# EventHubConsumerClient  – reads events from Event Hubs
# EventData               – wraps a single event payload
# ---------------------------------------------------------------------------
from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient
from azure.storage.blob import BlobServiceClient, ContainerClient
from flask import Flask, abort, g, jsonify, request, send_from_directory
from flask_cors import CORS
from user_agents import parse

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – read from environment variables so secrets never live in code
#
# Set these before running locally:
#   export EVENT_HUB_CONNECTION_STR="Endpoint=sb://..."
#   export EVENT_HUB_NAME="clickstream"
#
# On Azure App Service, set them as Application Settings in the portal.
# ---------------------------------------------------------------------------
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "clickstream")
STORAGE_CONN_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
STORAGE_ACCOUNT = "cst8916week10storageacc"

# Container names match the ASA output aliases exactly
CONTAINER_Q1 = "bloboutputq1"  # device breakdown output
CONTAINER_Q2 = "bloboutputq2"  # spike alerts output

# In-memory buffer: stores the last 50 events received by the consumer thread.
# In a production system you would query a database or Azure Stream Analytics output.
_event_buffer = []
_buffer_lock = threading.Lock()
MAX_BUFFER = 50


# ---------------------------------------------------------------------------
# Enrichment – use user-agents library to parse the User-Agent header and extract our device info.
#
#   This runs before every request so the enriched data is available in the /track route when
#   we publish to Event Hubs, and also in the /api/events route when we serve the dashboard
#   (since we buffer events locally as well).
# ---------------------------------------------------------------------------
@app.before_request
def enrich_event_context():
    """Parse the User-Agent header and store device info in Flask's g object for this request.
    g is a special Flask object that lets us store data for the duration of a request.
    before_request runs before every request, so this enrichment happens for both
    /track and /api/events routes."""
    ua_string = request.headers.get("User_Agent")
    if ua_string:
        user_agent = parse(ua_string)
        g.device_type = (
            "mobile"
            if user_agent.is_mobile
            else "tablet"
            if user_agent.is_tablet
            else "pc"
        )
        g.browser = user_agent.browser.family
        g.os = user_agent.os.family
    else:
        # fallback for requests without UA (e.g., some API clients)
        g.device_type = "unknown"
        g.browser = "unknown"
        g.os = "unknown"


# ---------------------------------------------------------------------------
# Azure Blob Storage – read latest ASA output from separate containers
# ---------------------------------------------------------------------------


def get_latest_blob_from_container(container_name: str) -> list:
    """
    Return the parsed JSON records from the most recently modified blob
    inside the given container.

    Works with both:
      - connection string auth  (AZURE_STORAGE_CONNECTION_STRING is set)
      - anonymous / public access (connection string is empty)

    ASA writes one JSON record per line (JSONL format), so we split on
    newlines and parse each line individually.
    """
    try:
        if STORAGE_CONN_STR:
            # Authenticated access via connection string
            container_client = ContainerClient.from_connection_string(
                conn_str=STORAGE_CONN_STR,
                container_name=container_name,
            )
        else:
            # Anonymous / public access – build the URL from the account name
            account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
            container_client = ContainerClient(
                account_url=account_url,
                container_name=container_name,
                credential=None,  # no credentials = anonymous
            )

        blobs = list(container_client.list_blobs())
        if not blobs:
            return []

        # Pick the most recently modified blob (latest ASA window output)
        blobs.sort(
            key=lambda b: b.last_modified, reverse=True
        )  # order from most recent changed to oldest - b (a blob in the list)
        # last_modified is a timestamp that tracks when a blob was last changed
        latest_blob = blobs[0]

        blob_client = container_client.get_blob_client(latest_blob.name)
        raw = blob_client.download_blob().readall().decode("utf-8").strip()

        if not raw:
            return []

        # ASA writes JSONL – one JSON object per line
        records = []
        for line in raw.split("\n"):
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass  # skip malformed lines

        return records

    except Exception as exc:
        app.logger.error(f"Blob read error ({container_name}): {exc}")
        return []


@app.route("/api/device-breakdown", methods=["GET"])
def device_breakdown():
    """
    Return the latest device-breakdown records from bloboutputq1.

    Each record has the shape:
        { "device_type": "pc", "event_count": 5, "window_end": "2026-03-29T..." }
    """
    data = get_latest_blob_from_container(CONTAINER_Q1)
    return jsonify(data), 200


@app.route("/api/spike-alerts", methods=["GET"])
def spike_alerts():
    """
    Return the latest spike-alert records from bloboutputq2.

    Each record has the shape:
        { "window_end": "2026-03-29T...", "event_count": 7 }

    Only windows where event_count > 10 are written here (enforced by HAVING element in ASA query).
    """
    data = get_latest_blob_from_container(CONTAINER_Q2)
    return jsonify(data), 200


# ---------------------------------------------------------------------------
# Helper – send a single event dict to Azure Event Hubs
# ---------------------------------------------------------------------------
def send_to_event_hubs(event_dict: dict):
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    if not CONNECTION_STR:
        # Gracefully skip if the connection string is not configured yet
        app.logger.warning(
            "EVENT_HUB_CONNECTION_STR is not set – skipping Event Hubs publish"
        )
        return

    # EventHubProducerClient is created fresh per request here for simplicity.
    # In a high-throughput production app you would keep a shared client instance.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    )
    with producer:
        # create_batch() lets the SDK manage event size limits automatically
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(event_batch)


# ---------------------------------------------------------------------------
# Background consumer thread – reads events from Event Hubs and buffers them
# ---------------------------------------------------------------------------
def _on_event(partition_context, event):
    """Callback invoked by the consumer client for each incoming event."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _buffer_lock:
        _event_buffer.append(data)
        # Keep the buffer at MAX_BUFFER entries (drop the oldest)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    # Acknowledge the event so Event Hubs advances the consumer offset
    partition_context.update_checkpoint(event)


def start_consumer():
    """Start the Event Hubs consumer in a background daemon thread.

    The consumer must run on a separate thread because consumer.receive() blocks
    forever waiting for events. Running it on the main thread would freeze Flask
    and make the web server unable to handle any HTTP requests.
    """
    if not CONNECTION_STR:
        app.logger.warning(
            "EVENT_HUB_CONNECTION_STR is not set – consumer thread not started"
        )
        return

    # $Default is the built-in consumer group every Event Hub has.
    # A consumer group is a logical "view" of the stream — multiple groups can
    # read the same events independently (e.g. one for analytics, one for alerts).
    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
    )

    def run():
        with consumer:
            # receive() blocks forever, calling _on_event for each new event.
            # starting_position="-1" means "start from the beginning of the stream",
            # not just events that arrive after this consumer connects.
            consumer.receive(
                on_event=_on_event,
                starting_position="-1",
            )

    # daemon=True means this thread is automatically killed when the main program
    # exits. Without it, Flask would hang on shutdown waiting for receive() to
    # return — which it never would.
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    app.logger.info("Event Hubs consumer thread started")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    """Serve the demo e-commerce store."""
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    """Serve the live analytics dashboard."""
    return send_from_directory("templates", "dashboard.html")


@app.route("/health", methods=["GET"])
def health():
    """Health check – used by Azure App Service to verify the app is running."""
    return jsonify({"status": "healthy"}), 200


@app.route("/track", methods=["POST"])
def track():
    """
    Receive a click event from the browser and publish it to Event Hubs.

    Expected JSON body:
    {
        "event_type": "page_view" | "product_click" | "add_to_cart" | "purchase",
        "page":       "/products/shoes",
        "product_id": "p_shoe_42",       (optional)
        "user_id":    "u_1234"
    }
    """
    if not request.json:
        abort(400)

    # Enrich the event with a server-side timestamp
    event = {
        "event_type": request.json.get("event_type", "unknown"),
        "page": request.json.get("page", "/"),
        "product_id": request.json.get("product_id"),
        "user_id": request.json.get("user_id", "anonymous"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        # Our new user-agent context
        "device_type": g.get("device_type", "unknown"),
        "browser": g.get("browser", "unknown"),
        "os": g.get("os", "unknown"),
    }

    send_to_event_hubs(event)

    # Also buffer locally so the dashboard works even without a consumer thread
    with _buffer_lock:
        _event_buffer.append(event)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    return jsonify({"status": "ok", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """
    Return the buffered events as JSON.
    The dashboard polls this endpoint every 2 seconds.

    Optional query param:  ?limit=20  (default 20, max 50)
    """
    try:
        # request.args.get("limit", 20) reads ?limit=N from the URL, defaulting to 20.
        # int() converts it from a string (all URL params are strings) to an integer.
        # min(..., MAX_BUFFER) clamps the value so callers can never request more
        # events than the buffer holds — e.g. ?limit=999 silently becomes 50.
        limit = min(int(request.args.get("limit", 20)), MAX_BUFFER)
    except ValueError:
        # int() raises ValueError if the param is non-numeric (e.g. ?limit=abc)
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])

    # Build a simple summary for the dashboard
    summary = {}
    for e in recent:
        et = e.get("event_type", "unknown")
        summary[et] = summary.get(et, 0) + 1

    return jsonify({"events": recent, "summary": summary, "total": len(recent)}), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start the background consumer so the dashboard receives live events
    start_consumer()
    # Run on 0.0.0.0 so it is reachable both locally and inside Azure App Service
    app.run(debug=False, host="0.0.0.0", port=8000)
