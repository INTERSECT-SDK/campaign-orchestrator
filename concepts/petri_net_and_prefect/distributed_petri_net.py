import yaml, time, threading
from snakes.nets import PetriNet, Place, Transition, Value
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
import pika
import paho.mqtt.client as mqtt

# ============================================================
# FACTORY INTERFACES
# ============================================================
class BaseMessagingClient:
    def publish(self, topic, payload):
        raise NotImplementedError
    def subscribe(self, topic, callback):
        raise NotImplementedError
    def loop_start(self):
        pass

class MqttClient(BaseMessagingClient):
    def __init__(self, broker="localhost", port=1883, username=None, password=None):
        self.client = mqtt.Client()
        if username and password:
            self.client.username_pw_set(username, password)
        self.client.connect(broker, port)
    def publish(self, topic, payload):
        print(f"📡 [MQTT] Publishing → {topic}: {payload}")
        self.client.publish(topic, payload)
    def subscribe(self, topic, callback):
        print(f"🔔 [MQTT] Subscribing → {topic}")
        self.client.subscribe(topic)
        self.client.message_callback_add(topic, lambda c,u,m: callback(m.topic, m.payload.decode()))
    def loop_start(self):
        threading.Thread(target=self.client.loop_forever, daemon=True).start()

class AmqpClient(BaseMessagingClient):
    def __init__(self, broker="localhost", port=5672, username=None, password=None):
        credentials = pika.PlainCredentials(username, password) if username and password else None
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=broker, port=port, credentials=credentials))
        self.channel = self.conn.channel()
    def publish(self, topic, payload):
        print(f"📡 [AMQP] Publishing → {topic}: {payload}")
        self.channel.queue_declare(queue=topic)
        self.channel.basic_publish(exchange='', routing_key=topic, body=payload)
    def subscribe(self, topic, callback):
        print(f"🔔 [AMQP] Subscribing → {topic}")
        self.channel.queue_declare(queue=topic)
        def _cb(ch, method, props, body):
            callback(topic, body.decode())
        self.channel.basic_consume(queue=topic, on_message_callback=_cb, auto_ack=True)
        threading.Thread(target=self.channel.start_consuming, daemon=True).start()

class MessagingFactory:
    @staticmethod
    def create(config):
        backend = config.get("backend", "mqtt").lower()
        broker_config = config.get(backend, {})
        broker = broker_config.get("broker", "localhost")
        port = broker_config.get("port", 1883 if backend == "mqtt" else 5672)
        username = broker_config.get("username")
        password = broker_config.get("password")
        if backend == "mqtt":
            return MqttClient(broker, port, username, password)
        elif backend == "amqp":
            return AmqpClient(broker, port, username, password)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

# ============================================================
# LOAD PETRI NET FROM YAML
# ============================================================
def load_petri_from_yaml(yaml_path):
    data = yaml.safe_load(open(yaml_path))
    net = PetriNet(data["net_name"])
    for place in data["places"]:
        net.add_place(Place(place))
    net.place("Ready").add(1)
    for t in data["transitions"]:
        name = t["name"]
        net.add_transition(Transition(name))
        for i in t.get("inputs", []):
            net.add_input(i, name, Value(1))
        for o in t.get("outputs", []):
            net.add_output(o, name, Value(1))
    return net, data["transitions"]

# ============================================================
# PREFECT TASKS
# ============================================================
@task(cache_policy=NO_CACHE)
def send_message(client, topic, payload):
    client.publish(topic, payload)

# ============================================================
# MAIN FLOW
# ============================================================
@flow
def petri_distributed_workflow(config_path="config.yaml", net_path="petri-net.yaml"):
    # Load configs
    config = yaml.safe_load(open(config_path))
    client = MessagingFactory.create(config["messaging"])
    client.loop_start()

    net, transitions = load_petri_from_yaml(net_path)

    # Keep topic mapping
    topic_map = {t["name"]: t for t in transitions}

    # Subscribe handlers
    def on_message(topic, payload):
        print(f"✅ Received external message: {topic} | {payload}")
        for tname, tdata in topic_map.items():
            if tdata.get("subscribe_topic") == topic:
                print(f"🔥 External trigger firing transition: {tname}")
                net.transition(tname).fire(net)

    # Register subscriptions
    for t in transitions:
        sub_topic = t.get("subscribe_topic")
        if sub_topic:
            client.subscribe(sub_topic, on_message)

    # Execution loop
    while True:
        enabled = [t.name for t in net.transition() if t.enabled(net)]
        print(f"\n🔁 Enabled transitions: {enabled}")
        if not enabled:
            time.sleep(2)
            continue

        for tname in enabled:
            tdata = topic_map[tname]
            pub_topic = tdata.get("publish_topic")
            send_message.submit(client, pub_topic, f"fire:{tname}")
            net.transition(tname).fire(net)

        print("📊 Current marking:", {p.name: list(p.tokens) for p in net.place()})
        time.sleep(3)

if __name__ == "__main__":
    petri_distributed_workflow()
