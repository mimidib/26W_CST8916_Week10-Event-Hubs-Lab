# CST8916 - Assignment 2: Event Hubs Real-Time User-Agent Enrichment & Data Analysis with Azure Stream Analytics

- Mireille Dib
- 040829779
- March 28th, 2026

## Gap 1. Where To Add Enrichment? To Answer, Find Where We Currently Enrich In The Flow
To find out my Gap 1., enriching the data, I had to search in our repository for where we currently gather for data to send to the dashboard.
- Client.html to capture from the user browser then sends to app.py payload to send to eventhub!  

In `client.html`:

```html
<script>   
  // ── Core tracking function ─────────────────────────────────────
  // Sends a POST request to /track with the event details.
  // The Flask backend publishes this to Azure Event Hubs.
  function trackEvent(eventType, page, productId) {
    const payload = {
      event_type: eventType,
      page:       page,
      product_id: productId,
      user_id:    USER_ID,
      session_id: SESSION_ID,
    };

    // Send the event to the Flask /track endpoint
    //...
  }
</script>    
```
In `app.py`:

```python
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
# ---------------------------------------------------------------------------
# Helper – send a single event dict to Azure Event Hubs
# ---------------------------------------------------------------------------
def send_to_event_hubs(event_dict: dict):
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    # Gracefully skip if the connection string is not configured yet
    # ...
    
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
    # if the incoming request doesnt contain JSON return 400
    # ...

    # Enrich the event with a server-side timestamp
    event = {
        "event_type": request.json.get("event_type", "unknown"),
        "page":       request.json.get("page", "/"),
        "product_id": request.json.get("product_id"),
        "user_id":    request.json.get("user_id", "anonymous"),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }

    send_to_event_hubs(event)

    # Also buffer locally so the dashboard works even without a consumer thread
    # ...
    return jsonify({"status": "ok", "event": event}), 201
```

We enrich in `client.html` and send it to `app.py` to send to Azure Event Hubs
dashboard POLLS using GET /api/events to get the data

I gotta add it in client.html, then in app.py to see it in Event Hubs

I found the following user-agent library that can help me retrieve this info using python:
```python
# First, install the necessary libraries:
# pip install pyyaml ua-parser user-agents

from flask import request
from user_agents import parse

@app.route('/details')
def user_agent_details():
    user_agent_string = request.headers.get('User-Agent')
    user_agent = parse(user_agent_string)

    return f"""
    Browser: {user_agent.browser.family} {user_agent.browser.version_string}<br>
    OS: {user_agent.os.family} {user_agent.os.version_string}<br>
    Device: {user_agent.device.family} (Mobile: {user_agent.is_mobile})
    """

```
Then I looked at how I can efficiently add this to a global context so I can call it once before the request for efficiency (rahter than multiple times for each event like a click) and then use that to add on to each event using the global context.  
This is the result of my research:  

> 1. Page Views: Use @app.before_request 
> For page views, a request is already hitting your server. You can use the before_request hook to parse the user-agent once and store it in Flask’s global g object, making it available to your tracking logic without re-parsing it in every route. 

> ```python
> from flask import g, request
> from user_agents import parse # Recommended for better device/OS detection
>
> @app.before_request
> def enrich_event_context():
>    ua_string = request.headers.get('User-Agent')
>    # Parse once per request
>    user_agent = parse(ua_string) 
>    
>    g.event_context = {
>        "device": "mobile" if user_agent.is_mobile else "tablet" if user_agent.is_tablet else "pc",
>        "browser": user_agent.browser.family,
>        "os": user_agent.os.family
>    }
>
>@app.route('/product/<id>')
>def product_page(id):
>    # Log the page view with enriched data from 'g'
>    log_event("page_view", product_id=id, context=g.event_context)
>    return render_template('product.html')
>```
>2. Clicks & Actions: Use an API Pattern
>Actions like "Add to Cart" or "Product Clicks" are usually sent via AJAX/Fetch from the browser. In these cases, your server-side before_request hook will still run for that specific API call. 
>
>The Optimal Way: Create a single /track-event endpoint. When a user clicks a button, the front-end sends the event type (e."add_to_cart") to this endpoint. The endpoint then uses the same g.event_context (populated by before_request) to enrich the data before saving it to your database. 
>
>When to avoid before_request
>Static Files: If you are serving images or CSS through Flask, before_request runs for those too. You may want to skip tracking if request.endpoint is for static files.
>Performance: Parsing a user-agent string is relatively fast, but if your site has massive traffic, doing it on every single request might add overhead. In that case, consider parsing it only on the first request and storing it in a session. 
>Summary Recommendation
>Centralize the parsing in @app.before_request.
>Store the results in g for use during that request.
>Use a library like user-agents for more reliable OS and device detection than Flask's basic built-in parser

Since we are not using static files this solution is sufficient. I plan to look into Sessions as a stretch goal of this assignment if time allows. I will leave my research/thoughts below.

## Stretch Goal: Sessions
then I explored sessions using Flask, to see if I can save it in a session easily
https://testdriven.io/courses/learn-flask/sessions/
session data would be stored in-memory of the app service (using RAM) and be terminated once the app goes down whicih is fine 
It uses a `session` object to store the data in cookies that are cryptographically-signed (not encrypted so no sensitive info) 
we don't need to store in a db we just need to send to Event Hub so this could be a stretch goal. 


## My approach

### 1. Install user-agent parsing library 
User-agents library wraps `ua-parser` and enriches the info for a `ua` for `is_mobile` `is_tablet`, `browser.family`, `os.family`, etc.
```bash
pip install user-agents
```
Then I added this library to `requirements.txt`: `user-agents==2.2.0`

### 2. Add a `before_request` hook
ina `app.py` I define a function that runs before every request which will parse the `User-Agent` header and store the enriched context data we want in Flask's `g` object for later reference. 

```python
from flask import g, request
from user_agents import parse

@app.before_request
def enrich_event_context():
    ua_string = request.headers.get('User_Agent')
    if ua_string:
        user_agent = parse(ua_string)
        g.device_type = (
            "mobile" if user_agent.is_mobile else
            "tablet" if user_agent.is_tablet else
            "pc"
        )
        g.browser = user_agent.browser.family
        g.os = user_agent.os.family
    else:
        # fallback for requests without UA (e.g., some API clients)
        g.device_type = "unknown"
        g.browser = "unknown"
        g.os = "unknown"
```
This `before_request` hook runs before each request made to our application, including
- Initial GET `/` and `/track` (as well as even `/dashbaord` since its a part of this `app.py` routes although we don't care for it)
- The AJAX POST request to `/track`

### 3. Enrich Event for Event Hubs Payload (Within `/track` Route)

Inside our existing `/track` route, lets add this user agent's enriched data we saved to Flask's `g` object and send it to Event Hubs to see in the Azure Portal.  

```python
@app.route("/track", methods=["POST"])
def track():
    if not request.json:
        abort(400)

    event = {
        "event_type": request.json.get("event_type", "unknown"),
        "page":       request.json.get("page", "/"),
        "product_id": request.json.get("product_id"),
        "user_id":    request.json.get("user_id", "anonymous"),
        "session_id": request.json.get("session_id", ""),  # already sent from JS
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        # Our new user-agent context
        "device_type": g.get("device_type", "unknown"),
        "browser": g.get("browser", "unknown"),
        "os": g.get("os", "unknown"),
    }

    send_to_event_hubs(event)

    # Also buffer event locally
    # ...
     
    return jsonify({"status": "ok", "event": event}), 201
```

### 4. See our User-Agent Enrichment Data in dashboard.html
We need to modify the dashboard to render the new fields. 
As long as our user-agent data is stored in `_event_buffer`- which it is, our API `/api/events` returns them, but right now our dashboard is ignoring these fields. 

To change this, the JS function `renderFeed(events)` currently builds a row using only `ts`, `type`, `page` and `user_id` so we will modify it to refer to our new fields `device_type`, `browser`, and `os`.