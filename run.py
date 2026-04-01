import time
import subprocess
import sys
from datetime import datetime

def is_trading_time():
    n = datetime.now()
    if n.weekday() >= 5:
        return False
    h = n.hour
    m = n.minute
    return (h == 9 and m >= 20) or (9 < h < 15) or (h == 15 and m <= 15)

def main():
    print("守护进程已启动")
    proc = None
    while True:
        trading = is_trading_time()
        if trading:
            if proc is None or proc.poll() is not None:
                proc = subprocess.Popen([sys.executable, "main.py"])
        else:
            if proc and proc.poll() is None:
                proc.terminate()
                proc = None
        time.sleep(30)

if __name__ == "__main__":
    main()
