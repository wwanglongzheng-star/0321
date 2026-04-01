import logging
import os
from datetime import datetime

def get_logger():
    if not os.path.exists("logs"):
        os.makedirs("logs")
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join("logs", f"trading_{today}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("trading_bot")

log = get_logger()
