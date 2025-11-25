from ..config import AppConfig

def make_history_client(cfg: AppConfig):
    """
    MQTT-only mode: no HTTP/CSV history client.
    The REST endpoints should treat a None client as "not configured".
    """
    return None
