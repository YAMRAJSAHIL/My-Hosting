from flask import Flask
from threading import Thread
import subprocess
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "🔹 HostBot is running!"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def run_bot():
    print("✅ Starting Telegram bot...")
    subprocess.call(["python3", "main.py"])

if __name__ == '__main__':
    Thread(target=run_flask).start()
    run_bot()
