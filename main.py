import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import sqlite3
import json 
import logging
import signal 
import threading
import re 
import sys
import atexit
import requests 

# --- Flask Keep Alive ---
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "𝗜'𝗮𝗺 𝗬𝗮𝗺𝗿𝗮𝗷 𝗙𝗶𝗹𝗲 𝗛𝗼𝘀𝘁"

def run_flask():
  # Make sure to run on port provided by environment or default to 8080
  port = int(os.environ.get("PORT", 8080))
  app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True # Allows program to exit even if this thread is running
    t.start()
    print("𝗙𝗹𝗮𝘀𝗸 𝗞𝗲𝗲𝗽-𝗔𝗹𝗶𝘃𝗲 𝘀𝗲𝗿𝘃𝗲𝗿 𝘀𝘁𝗮𝗿𝘁𝗲𝗱.")
# --- End Flask Keep Alive ---

# --- Configuration ---
TOKEN = '8167873077:AAH_s1IoEgwJ0ngSty3ZTbZKTSB4ing_W7U' # Replace with your actual token
OWNER_ID = 5191787565 # Replace with your Owner ID
ADMIN_ID = 5191787565 # Replace with your Admin ID (can be same as Owner)
YOUR_USERNAME = '@YAMRAJSAHIL2' # Replace with your Telegram username (without the @)
UPDATE_CHANNEL = 't.me/YamrajUpdates' # Replace with your update channel link

# Folder setup - using absolute paths
BASE_DIR = os.path.abspath(os.path.dirname(__file__)) # Get script's directory
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf') # Assuming this name is intentional
DATABASE_PATH = os.path.join(IROTECH_DIR, 'bot_data.db')

# File upload limits
FREE_USER_LIMIT = 3
SUBSCRIBED_USER_LIMIT = 15 
ADMIN_LIMIT = 999       
OWNER_LIMIT = float('inf') 

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {} # Stores info about running scripts {script_key: info_dict}
user_subscriptions = {} # {user_id: {'expiry': datetime_object}}
user_files = {} # {user_id: [(file_name, file_type), ...]}
pending_files = {} # {f"{user_id}_{file_name}": {'file_content': bytes, 'file_ext': str, 'user_id': int}}
active_users = set() # Set of all user IDs that have interacted with the bot
admin_ids = {ADMIN_ID, OWNER_ID} # Set of admin IDs
banned_users = {} # {user_id: {'reason': str, 'banned_by': int, 'ban_date': str}}
bot_locked = False
# free_mode = False # Removed free_mode

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Command Button Layouts (ReplyKeyboardMarkup) ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📤 Upload File", "📂 Check Files"],
    ["📢 Updates Channel", "📞 Contact Owner"],
    ["⚡ Bot Speed"]
]
ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📤 Upload File", "📂 Check Files"],
    ["📢 Updates Channel", "📞 Contact Owner"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["👑 Admin Panel"]
]

# --- Database Setup ---
def init_db():
    """Initialize the database with required tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False) # Allow access from multiple threads
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''') # Added admins table
        c.execute('''CREATE TABLE IF NOT EXISTS banned_users
                     (user_id INTEGER PRIMARY KEY, reason TEXT, banned_by INTEGER, ban_date TEXT)''') # Added banned users table
        # Ensure owner and initial admin are in admins table
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
             c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}", exc_info=True)

def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        # Load subscriptions
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"⚠️ Invalid expiry date format for user {user_id}: {expiry}. Skipping.")

        # Load user files
        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))

        # Load active users
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        # Load admins
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall()) # Load admins into the set

        # Load banned users
        c.execute('SELECT user_id, reason, banned_by, ban_date FROM banned_users')
        for user_id, reason, banned_by, ban_date in c.fetchall():
            banned_users[user_id] = {'reason': reason, 'banned_by': banned_by, 'ban_date': ban_date}

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, {len(admin_ids)} admins, {len(banned_users)} banned users.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

# Initialize DB and Load Data at startup
init_db()
load_data()
# --- End Database Setup ---

# --- Helper Functions ---
def check_channel_membership(user_id):
    """Check if user is a member of the update channel"""
    try:
        # Extract channel username from UPDATE_CHANNEL URL
        channel_username = UPDATE_CHANNEL.split('/')[-1]  # Gets 'YamrajUpdates' from 't.me/YamrajUpdates'
        
        # Get chat member info
        member = bot.get_chat_member(f"@{channel_username}", user_id)
        
        # Check if user is member (not left, kicked, or restricted)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking channel membership for user {user_id}: {e}")
        return False

def create_channel_join_message():
    """Create the channel join message with inline buttons"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton('📢 Join Updates Channel', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('✅ Joined', callback_data='verify_channel_join')
    )
    
    message_text = (
        "🔒 <b>Channel Verification Required!</b>\n\n"
        "📢 <b>Please join our Updates Channel first to use this bot.</b>\n\n"
        "👇 <b>Click the button below to join, then click 'Joined' to verify.</b>"
    )
    
    return message_text, markup

def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    # if free_mode: return FREE_MODE_LIMIT # Removed free_mode check
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name): # Parameter renamed for clarity
    """Check if a bot script is currently running for a specific user"""
    script_key = f"{script_owner_id}_{file_name}" # Key uses script_owner_id
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                logger.warning(f"Process {script_info['process'].pid} for {script_key} found in memory but not running/zombie. Cleaning up.")
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during zombie cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            logger.warning(f"Process for {script_key} not found (NoSuchProcess). Cleaning up.")
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                     script_info['log_file'].close()
                except Exception as log_e:
                     logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                 del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}", exc_info=True)
            return False
    return False


def kill_process_tree(process_info):
    """Kill a process and all its children, ensuring log file is closed."""
    pid = None
    log_file_closed = False
    script_key = process_info.get('script_key', 'N/A') 

    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
                log_file_closed = True
                logger.info(f"Closed log file for {script_key} (PID: {process_info.get('process', {}).get('pid', 'N/A')})")
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
           pid = process.pid
           if pid: 
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    logger.info(f"Attempting to kill process tree for {script_key} (PID: {pid}, Children: {[c.pid for c in children]})")

                    for child in children:
                        try:
                            child.terminate()
                            logger.info(f"Terminated child process {child.pid} for {script_key}")
                        except psutil.NoSuchProcess:
                            logger.warning(f"Child process {child.pid} for {script_key} already gone.")
                        except Exception as e:
                            logger.error(f"Error terminating child {child.pid} for {script_key}: {e}. Trying kill...")
                            try: child.kill(); logger.info(f"Killed child process {child.pid} for {script_key}")
                            except Exception as e2: logger.error(f"Failed to kill child {child.pid} for {script_key}: {e2}")

                    gone, alive = psutil.wait_procs(children, timeout=1)
                    for p in alive:
                        logger.warning(f"Child process {p.pid} for {script_key} still alive. Killing.")
                        try: p.kill()
                        except Exception as e: logger.error(f"Failed to kill child {p.pid} for {script_key} after wait: {e}")

                    try:
                        parent.terminate()
                        logger.info(f"Terminated parent process {pid} for {script_key}")
                        try: parent.wait(timeout=1)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Parent process {pid} for {script_key} did not terminate. Killing.")
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")
                    except psutil.NoSuchProcess:
                        logger.warning(f"Parent process {pid} for {script_key} already gone.")
                    except Exception as e:
                        logger.error(f"Error terminating parent {pid} for {script_key}: {e}. Trying kill...")
                        try: parent.kill(); logger.info(f"Killed parent process {pid} for {script_key}")
                        except Exception as e2: logger.error(f"Failed to kill parent {pid} for {script_key}: {e2}")

                except psutil.NoSuchProcess:
                    logger.warning(f"Process {pid or 'N/A'} for {script_key} not found during kill. Already terminated?")
           else: logger.error(f"Process PID is None for {script_key}.")
        elif log_file_closed: logger.warning(f"Process object missing for {script_key}, but log file closed.")
        else: logger.error(f"Process object missing for {script_key}, and no log file. Cannot kill.")
    except Exception as e:
        logger.error(f"❌ Unexpected error killing process tree for PID {pid or 'N/A'} ({script_key}): {e}", exc_info=True)

# --- Automatic Package Installation & Script Running ---

def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: 
        logger.info(f"<b>Module <code>{module_name}</code> is core. Skipping pip install.</b>")
        return False 
    try:
        bot.reply_to(message, f"<b>🐍 Module <code>{module_name}</code> found. Installing <code>{package_name}</code>...</b>", parse_mode='HTML')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"<b>✅ Package </code>{package_name}</code> (for </code>{module_name}</code>) installed.</b>", parse_mode='HTML')
            return True
        else:
            error_msg = f"<b>❌ Failed to install <code>{package_name}</code> for <code>{module_name}</code>.</b>\nLog:\n\n{result.stderr or result.stdout}\n"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='HTML')
            return False
    except Exception as e:
        error_msg = f"❌ Error installing `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"<b>🟠 Node package <code>{module_name}</code> not found. Installing locally...</b>", parse_mode='HTML')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {module_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"<b>✅ Node package <code>{module_name}</code> installed locally.</b>", parse_mode='HTML')
            return True
        else:
            error_msg = f"<b>❌ Failed to install Node package <code>{module_name}</code>.</b>\nLog:\n\n{result.stderr or result.stdout}\n"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='HTML')
            return False
    except FileNotFoundError:
         error_msg = "<b>❌ Error: npm not found. Ensure Node.js/npm are installed and in PATH.</b>"
         logger.error(error_msg)
         bot.reply_to(message, error_msg)
         return False
    except Exception as e:
        error_msg = f"<b>❌ Error installing Node package <code>{module_name}</code>: {str(e)}</b>"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run Python script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2 
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"<b>❌ Failed to run <code>{file_name}</code> after {max_attempts} attempts. Check logs.</b>")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"<b>Attempt {attempt} to run Python script: {script_path} (Key: {script_key}) for user {script_owner_id}</b>")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"<b>❌ Error: Script <code>{file_name}</code> not found at <code>{script_path}</code>!</b>")
             logger.error(f"<b>Script not found: {script_path} for user {script_owner_id}</b>")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"Running Python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"Python Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"Detected missing Python module: {module_name}")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"Install OK for {module_name}. Retrying run_script...")
                            bot.reply_to(message_obj_for_reply, f"🔄 Install successful. Retrying '{file_name}'...")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ Install failed. Cannot run '{file_name}'.")
                            return
                    else:
                         error_summary = stderr[:500]
                         bot.reply_to(message_obj_for_reply, f"<b>❌ Error in script pre-check for '{file_name}':\n\n<pre>{error_summary}</pre>\n\nFix the script.</b>", parse_mode='HTML')
                         return
            except subprocess.TimeoutExpired:
                logger.info("Python Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("Python Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 logger.error(f"Python interpreter not found: {sys.executable}")
                 bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
                 return
            except Exception as e:
                 logger.error(f"Error in Python pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in script pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"Python Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running Python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"Failed to open log file '{log_file_path}' for {script_key}: {e}", exc_info=True)
             bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"<b>Started Python process {process.pid} for {script_key}</b>")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies from script, defaults to admin/triggering user
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"<b>✅ Python script <code>{file_name}</code> started! (PID: {process.pid}) (For User: {script_owner_id})</b>")
        except FileNotFoundError:
             logger.error(f"Python interpreter {sys.executable} not found for long run {script_key}")
             bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
             if log_file and not log_file.closed: log_file.close()
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting Python script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started Python process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running Python script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run JS script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run JS script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"JS Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = ['node', script_path]
            logger.info(f"Running JS pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"JS Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"Detected missing Node module: {module_name}")
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"NPM Install OK for {module_name}. Retrying run_js_script...")
                                 bot.reply_to(message_obj_for_reply, f"🔄 NPM Install successful. Retrying '{file_name}'...")
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
                             else:
                                 bot.reply_to(message_obj_for_reply, f"❌ NPM Install failed. Cannot run '{file_name}'.")
                                 return
                        else: logger.info(f"Skipping npm install for relative/core: {module_name}")
                    error_summary = stderr[:500]
                    bot.reply_to(message_obj_for_reply, f"❌ Error in JS script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix script or install manually.", parse_mode='Markdown')
                    return
            except subprocess.TimeoutExpired:
                logger.info("JS Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("JS Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 error_msg = "❌ Error: 'node' not found. Ensure Node.js is installed for JS files."
                 logger.error(error_msg)
                 bot.reply_to(message_obj_for_reply, error_msg)
                 return
            except Exception as e:
                 logger.error(f"Error in JS pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in JS pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"JS Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running JS process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to open log file '{log_file_path}' for JS script {script_key}: {e}", exc_info=True)
            bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
            return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started JS process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"<b>✅ JS script <code>{file_name}</code> started! (PID: {process.pid}) (For User: {script_owner_id})</b>")
        except FileNotFoundError:
             error_msg = "❌ Error: node not found for long run. Ensure Node.js is installed."
             logger.error(error_msg)
             if log_file and not log_file.closed: log_file.close()
             bot.reply_to(message_obj_for_reply, error_msg)
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting JS script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started JS process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running JS script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_js_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

# Security scanning system removed - only approval/decline system remains

# --- Map Telegram import names to actual PyPI package names ---
TELEGRAM_MODULES = {
    # Main Bot Frameworks
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon', # Handle specific imports
    'from telethon.sync import telegramclient': 'telethon', # Example

    # Additional Libraries (add more specific mappings if import name differs)
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',

    # MTProto & Low-Level
    'mtproto': 'telegram-mtproto', # Example, check actual package name
    'tl': 'telethon',  # Part of Telethon, install 'telethon'

    # Utilities & Helpers (examples, verify package names)
    'telegram_utils': 'telegram-utils',
    'telegram_logger': 'telegram-logger',
    'telegram_handlers': 'python-telegram-handlers',

    # Database Integrations (examples)
    'telegram_redis': 'telegram-redis',
    'telegram_sqlalchemy': 'telegram-sqlalchemy',

    # Payment & E-commerce (examples)
    'telegram_payment': 'telegram-payment',
    'telegram_shop': 'telegram-shop-sdk',

    # Testing & Debugging (examples)
    'pytest_telegram': 'pytest-telegram',
    'telegram_debug': 'telegram-debug',

    # Scraping & Analytics (examples)
    'telegram_scraper': 'telegram-scraper',
    'telegram_analytics': 'telegram-analytics',

    # NLP & AI (examples)
    'telegram_nlp': 'telegram-nlp-toolkit',
    'telegram_ai': 'telegram-ai', # Assuming this exists

    # Web & API Integration (examples)
    'telegram_api': 'telegram-api-client',
    'telegram_web': 'telegram-web-integration',

    # Gaming & Interactive (examples)
    'telegram_games': 'telegram-games',
    'telegram_quiz': 'telegram-quiz-bot',

    # File & Media Handling (examples)
    'telegram_ffmpeg': 'telegram-ffmpeg',
    'telegram_media': 'telegram-media-utils',

    # Security & Encryption (examples)
    'telegram_2fa': 'telegram-twofa',
    'telegram_crypto': 'telegram-crypto-bot',

    # Localization & i18n (examples)
    'telegram_i18n': 'telegram-i18n',
    'telegram_translate': 'telegram-translate',

    # Common non-telegram examples
    'bs4': 'beautifulsoup4',
    'requests': 'requests',
    'pillow': 'Pillow', # Note the capitalization difference
    'cv2': 'opencv-python', # Common import name for OpenCV
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'asyncio': None, # Core module, should not be installed
    'json': None,    # Core module
    'datetime': None,# Core module
    'os': None,      # Core module
    'sys': None,     # Core module
    're': None,      # Core module
    'time': None,    # Core module
    'math': None,    # Core module
    'random': None,  # Core module
    'logging': None, # Core module
    'threading': None,# Core module
    'subprocess':None,# Core module
    'zipfile':None,  # Core module
    'tempfile':None, # Core module
    'shutil':None,   # Core module
    'sqlite3':None,  # Core module
    'psutil': 'psutil',
    'atexit': None   # Core module

}
# --- End Automatic Package Installation & Script Running ---


# --- Database Operations ---
DB_LOCK = threading.Lock() 

def save_user_file(user_id, file_name, file_type='py'):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)',
                      (user_id, file_name, file_type))
            conn.commit()
            if user_id not in user_files: user_files[user_id] = []
            user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
            user_files[user_id].append((file_name, file_type))
            logger.info(f"Saved file '{file_name}' ({file_type}) for user {user_id}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving file for user {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def remove_user_file_db(user_id, file_name):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            conn.commit()
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
                if not user_files[user_id]: del user_files[user_id]
            logger.info(f"Removed file '{file_name}' for user {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing file for {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def add_active_user(user_id):
    active_users.add(user_id) 
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO active_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logger.info(f"Added/Confirmed active user {user_id} in DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding active user {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding active user {user_id}: {e}", exc_info=True)
        finally: conn.close()

def save_subscription(user_id, expiry):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            expiry_str = expiry.isoformat()
            c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)', (user_id, expiry_str))
            conn.commit()
            user_subscriptions[user_id] = {'expiry': expiry}
            logger.info(f"Saved subscription for {user_id}, expiry {expiry_str}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_subscription_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM subscriptions WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in user_subscriptions: del user_subscriptions[user_id]
            logger.info(f"Removed subscription for {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def add_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (admin_id,))
            conn.commit()
            admin_ids.add(admin_id) 
            logger.info(f"Added admin {admin_id} to DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding admin {admin_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding admin {admin_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_admin_db(admin_id):
    if admin_id == OWNER_ID:
        logger.warning("Attempted to remove OWNER_ID from admins.")
        return False 
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        removed = False
        try:
            c.execute('SELECT 1 FROM admins WHERE user_id = ?', (admin_id,))
            if c.fetchone():
                c.execute('DELETE FROM admins WHERE user_id = ?', (admin_id,))
                conn.commit()
                removed = c.rowcount > 0 
                if removed: admin_ids.discard(admin_id); logger.info(f"Removed admin {admin_id} from DB")
                else: logger.warning(f"Admin {admin_id} found but delete affected 0 rows.")
            else:
                logger.warning(f"Admin {admin_id} not found in DB.")
                admin_ids.discard(admin_id)
            return removed
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing admin {admin_id}: {e}"); return False
        except Exception as e: logger.error(f"❌ Unexpected error removing admin {admin_id}: {e}", exc_info=True); return False
        finally: conn.close()

def ban_user_db(user_id, reason, banned_by):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            ban_date = datetime.now().isoformat()
            c.execute('INSERT OR REPLACE INTO banned_users (user_id, reason, banned_by, ban_date) VALUES (?, ?, ?, ?)',
                      (user_id, reason, banned_by, ban_date))
            conn.commit()
            banned_users[user_id] = {'reason': reason, 'banned_by': banned_by, 'ban_date': ban_date}
            logger.info(f"Banned user {user_id} with reason: {reason}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error banning user {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error banning user {user_id}: {e}", exc_info=True)
        finally: conn.close()

def unban_user_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM banned_users WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in banned_users: del banned_users[user_id]
            logger.info(f"Unbanned user {user_id}")
            return True
        except sqlite3.Error as e: logger.error(f"❌ SQLite error unbanning user {user_id}: {e}"); return False
        except Exception as e: logger.error(f"❌ Unexpected error unbanning user {user_id}: {e}", exc_info=True); return False
        finally: conn.close()
def is_user_banned(user_id):
    """Check if a user is banned"""
    return user_id in banned_users

# --- End Database Operations ---

# --- Menu creation (Inline and ReplyKeyboards) ---
def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('📤 Upload File', callback_data='upload'),
        types.InlineKeyboardButton('📂 Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
        types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}')
    ]

    if user_id in admin_ids:
        admin_buttons = [
            types.InlineKeyboardButton('📊 Statistics', callback_data='stats'), #0
            types.InlineKeyboardButton('👑 Admin Panel', callback_data='admin_panel') #1
        ]
        markup.add(buttons[0]) # Updates
        markup.add(buttons[1], buttons[2]) # Upload, Check Files
        markup.add(buttons[3], admin_buttons[0]) # Speed, Statistics
        markup.add(admin_buttons[1]) # Admin Panel
        markup.add(buttons[4]) # Contact
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        markup.add(buttons[4])
    return markup

def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True): # Parameter renamed
    markup = types.InlineKeyboardMarkup(row_width=2)
    # Callbacks use script_owner_id
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot',
                                 callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
        types.InlineKeyboardButton('🟢 Run All Scripts', callback_data='run_all_scripts')
    )
    markup.row(
        types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'),
        types.InlineKeyboardButton('💳 Subscriptions', callback_data='subscription')
    )
    markup.row(
        types.InlineKeyboardButton('🚫 Ban/Unban', callback_data='ban_unban_menu'),
        types.InlineKeyboardButton('👥 Admin Management', callback_data='admin_management_menu')
    )
    markup.row(
        types.InlineKeyboardButton('💬 Direct Message', callback_data='direct_message'),
        types.InlineKeyboardButton('📁 List All Files', callback_data='list_all_files')
    )
    markup.row(
        types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main')
    )
    return markup

def create_ban_unban_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('🚫 Ban User', callback_data='ban_user'),
        types.InlineKeyboardButton('✅ Unban User', callback_data='unban_user')
    )
    markup.row(types.InlineKeyboardButton('📋 List Banned Users', callback_data='list_banned_users'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Admin Panel', callback_data='admin_panel'))
    return markup

def create_admin_management_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Admin', callback_data='add_admin'),
        types.InlineKeyboardButton('➖ Remove Admin', callback_data='remove_admin')
    )
    markup.row(types.InlineKeyboardButton('📋 List Admins', callback_data='list_admins'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Admin Panel', callback_data='admin_panel'))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Subscription', callback_data='add_subscription'),
        types.InlineKeyboardButton('➖ Remove Subscription', callback_data='remove_subscription')
    )
    markup.row(types.InlineKeyboardButton('🔍 Check Subscription', callback_data='check_subscription'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Admin Panel', callback_data='admin_panel'))
    return markup
# --- End Menu Creation ---

# --- File Handling ---
def handle_zip_file(downloaded_file_content, file_name_zip, message):
    user_id = message.from_user.id
    # chat_id = message.chat.id # script_owner_id (user_id here) will be used for script key context
    user_folder = get_user_folder(user_id)
    temp_dir = None 
    try:
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
        logger.info(f"Temp dir for zip: {temp_dir}")
        zip_path = os.path.join(temp_dir, file_name_zip)
        with open(zip_path, 'wb') as new_file: new_file.write(downloaded_file_content)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.infolist():
                member_path = os.path.abspath(os.path.join(temp_dir, member.filename))
                if not member_path.startswith(os.path.abspath(temp_dir)):
                    raise zipfile.BadZipFile(f"Zip has unsafe path: {member.filename}")
            zip_ref.extractall(temp_dir)
            logger.info(f"Extracted zip to {temp_dir}")

        extracted_items = os.listdir(temp_dir)
        py_files = [f for f in extracted_items if f.endswith('.py')]
        js_files = [f for f in extracted_items if f.endswith('.js')]
        req_file = 'requirements.txt' if 'requirements.txt' in extracted_items else None
        pkg_json = 'package.json' if 'package.json' in extracted_items else None

        if req_file:
            req_path = os.path.join(temp_dir, req_file)
            logger.info(f"requirements.txt found, installing: {req_path}")
            bot.reply_to(message, f"🔄 Installing Python deps from `{req_file}`...")
            try:
                command = [sys.executable, '-m', 'pip', 'install', '-r', req_path]
                result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8', errors='ignore')
                logger.info(f"pip install from requirements.txt OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Python deps from `{req_file}` installed.")
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Python deps from `{req_file}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                 error_msg = f"❌ Unexpected error installing Python deps: {e}"
                 logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return

        if pkg_json:
            logger.info(f"package.json found, npm install in: {temp_dir}")
            bot.reply_to(message, f"🔄 Installing Node deps from `{pkg_json}`...")
            try:
                command = ['npm', 'install']
                result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=temp_dir, encoding='utf-8', errors='ignore')
                logger.info(f"npm install OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Node deps from `{pkg_json}` installed.")
            except FileNotFoundError:
                bot.reply_to(message, "❌ 'npm' not found. Cannot install Node deps."); return 
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Node deps from `{pkg_json}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown'); return
            except Exception as e:
                 error_msg = f"❌ Unexpected error installing Node deps: {e}"
                 logger.error(error_msg, exc_info=True); bot.reply_to(message, error_msg); return

        # ZIP files go directly to approval process

        main_script_name = None; file_type = None
        preferred_py = ['main.py', 'bot.py', 'app.py']; preferred_js = ['index.js', 'main.js', 'bot.js', 'app.js']
        for p in preferred_py:
            if p in py_files: main_script_name = p; file_type = 'py'; break
        if not main_script_name:
             for p in preferred_js:
                 if p in js_files: main_script_name = p; file_type = 'js'; break
        if not main_script_name:
            if py_files: main_script_name = py_files[0]; file_type = 'py'
            elif js_files: main_script_name = js_files[0]; file_type = 'js'
        if not main_script_name:
            bot.reply_to(message, "<b>❌ No .py or .js script found in archive!</b>", parse_mode='HTML'); return

        logger.info(f"Moving extracted files from {temp_dir} to {user_folder}")
        moved_count = 0
        for item_name in os.listdir(temp_dir):
            src_path = os.path.join(temp_dir, item_name)
            dest_path = os.path.join(user_folder, item_name)
            if os.path.isdir(dest_path): shutil.rmtree(dest_path)
            elif os.path.exists(dest_path): os.remove(dest_path)
            shutil.move(src_path, dest_path); moved_count +=1
        logger.info(f"Moved {moved_count} items to {user_folder}")

        save_user_file(user_id, main_script_name, file_type)
        logger.info(f"Saved main script '{main_script_name}' ({file_type}) for {user_id} from zip.")
        main_script_path = os.path.join(user_folder, main_script_name)
        bot.reply_to(message, f"<b>✅ Files extracted. Starting main script:</b> <code>{main_script_name}</code>...", parse_mode='HTML')

        # Use user_id as script_owner_id for script key context
        if file_type == 'py':
             threading.Thread(target=run_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()
        elif file_type == 'js':
             threading.Thread(target=run_js_script, args=(main_script_path, user_id, user_folder, main_script_name, message)).start()

    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file from {user_id}: {e}")
        bot.reply_to(message, f"<b>❌ Error: Invalid/corrupted ZIP. {e}</b>", parse_mode='HTML')
    except Exception as e:
        logger.error(f"❌ Error processing zip for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"<b>❌ Error processing zip: {str(e)}</b>", parse_mode='HTML')
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try: shutil.rmtree(temp_dir); logger.info(f"Cleaned temp dir: {temp_dir}")
            except Exception as e: logger.error(f"Failed to clean temp dir {temp_dir}: {e}", exc_info=True)

def handle_js_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'js')
        threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
        
    except Exception as e:
        logger.error(f"❌ Error processing JS file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"<b>❌ Error processing JS file:</b> {str(e)}", parse_mode='HTML')

def handle_py_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'py')
        threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
        
    except Exception as e:
        logger.error(f"❌ Error processing Python file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"<b>❌ Error processing Python file:</b> {str(e)}", parse_mode='HTML')
# --- End File Handling ---


# --- Logic Functions (called by commands and text handlers) ---
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    user_username = message.from_user.username

    logger.info(f"Welcome request from user_id: {user_id}, username: @{user_username}")

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Bot locked by admin. Try later.</b>", parse_mode='HTML')
        return

    user_bio = "Could not fetch bio"; photo_file_id = None
    try: user_bio = bot.get_chat(user_id).bio or "No bio"
    except Exception: pass
    try:
        user_profile_photos = bot.get_user_profile_photos(user_id, limit=1)
        if user_profile_photos.photos: photo_file_id = user_profile_photos.photos[0][-1].file_id
    except Exception: pass

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            # Get user status and file info for new user notification
            if user_id == OWNER_ID:
                user_status_new = "👑 Owner"
            elif user_id in admin_ids:
                user_status_new = "🛡️ Admin"
            elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now():
                user_status_new = "⭐ Premium"
            else:
                user_status_new = "🆓 Free User"
            
            file_limit_new = get_user_file_limit(user_id)
            current_files_new = get_user_file_count(user_id)
            limit_str_new = str(file_limit_new) if file_limit_new != float('inf') else "∞"
            
            profile_url = f"tg://user?id={user_id}"
            owner_notification = (
                f"🎉 <b>New user!</b>\n\n"
                f"👤 <b>NAME: {user_name}</b>\n"
                f"✳️ <b>USERNAME: @{user_username or 'N/A'}</b>\n"
                f"🔗 <b>PROFILE LINK:</b> <a href='{profile_url}'>CLICK HERE</a>\n"
                f"🆔 <b>USER ID:</b> <code>{user_id}</code>\n"
                f"🔰 <b>STATUS: {user_status_new}</b>\n"
                f"📊 <b>FILES UPLOADED: {current_files_new}/{limit_str_new}</b>"
            )
            
            # Try to get new user's profile photo (not bot's photo)
            try:
                new_user_photos = bot.get_user_profile_photos(user_id, limit=1)
                if new_user_photos.photos:
                    new_user_photo_id = new_user_photos.photos[0][-1].file_id
                    # Send user's photo with notification text as caption
                    bot.send_photo(OWNER_ID, new_user_photo_id, caption=owner_notification, parse_mode='HTML')
                else:
                    # No profile photo available, send text message
                    bot.send_message(OWNER_ID, owner_notification, parse_mode='HTML')
            except Exception as photo_e:
                logger.warning(f"Could not get profile photo for new user {user_id}: {photo_e}")
                # Fallback: send text message if photo fetch fails
                bot.send_message(OWNER_ID, owner_notification, parse_mode='HTML')
        except Exception as e: logger.error(f"⚠️ Failed to notify owner about new user {user_id}: {e}")

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 Owner"
    elif user_id in admin_ids: user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: user_status = "🆓 Free User (Expired Sub)"; remove_subscription_db(user_id) # Clean up expired
    else: user_status = "🆓 Free User"

    welcome_msg_text = f"""〽️ 𝗪𝗲𝗹𝗰𝗼𝗺𝗲,{user_name}!

🆔 𝗬𝗼𝘂𝗿 𝗨𝘀𝗲𝗿 𝗜𝗗: {user_id}
✳️ 𝗨𝘀𝗲𝗿𝗻𝗮𝗺𝗲:@{user_username or 'Not set'}
🔰 𝗬𝗼𝘂𝗿 𝗦𝘁𝗮𝘁𝘂𝘀: {user_status}{expiry_info}
📁 𝗙𝗶𝗹𝗲𝘀 𝗨𝗽𝗹𝗼𝗮𝗱𝗲𝗱: {current_files} / {limit_str}

🤖 𝗛𝗼𝘀𝘁 & 𝗿𝘂𝗻 𝗣𝘆𝘁𝗵𝗼𝗻 (.𝗽𝘆) 𝗼𝗿 𝗝𝗦 (.𝗷𝘀) 𝘀𝗰𝗿𝗶𝗽𝘁𝘀.
  𝗨𝗽𝗹𝗼𝗮𝗱 𝘀𝗶𝗻𝗴𝗹𝗲 𝘀𝗰𝗿𝗶𝗽𝘁𝘀 𝗼𝗿 .𝘇𝗶𝗽 𝗮𝗿𝗰𝗵𝗶𝘃𝗲𝘀.

👇 𝗨𝘀𝗲 𝗯𝘂𝘁𝘁𝗼𝗻𝘀 𝗼𝗿 𝘁𝘆𝗽𝗲 𝗰𝗼𝗺𝗺𝗮𝗻𝗱𝘀."""
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        if photo_file_id:
            # Send photo with welcome text as caption
            bot.send_photo(chat_id, photo_file_id, caption=welcome_msg_text, reply_markup=main_reply_markup, reply_to_message_id=message.message_id)
        else:
            # Fallback: send text message if no photo
            bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, reply_to_message_id=message.message_id)
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}", exc_info=True)
        try: bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup) # Fallback without photo
        except Exception as fallback_e: logger.error(f"Fallback send_message failed for {user_id}: {fallback_e}")

def _logic_updates_channel(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL))
    bot.reply_to(message, "<b>Visit our Updates Channel:</b>", reply_markup=markup, parse_mode='HTML')

def _logic_upload_file(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Bot locked by admin, cannot accept files.</b>", parse_mode='HTML')
        return

    # Removed free_mode check, relies on get_user_file_limit and FREE_USER_LIMIT
    # Users need to be admin or subscribed to upload if FREE_USER_LIMIT is 0
    # For now, FREE_USER_LIMIT > 0, so free users can upload up to that limit.
    # If we want to restrict free users entirely, set FREE_USER_LIMIT to 0.
    # For this implementation, free users get FREE_USER_LIMIT.

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"<b>⚠️ File limit ({current_files}/{limit_str}) reached. Delete files first.</b>", parse_mode='HTML')
        return
    bot.reply_to(message, "<b>📤 Send your Python (.py), JS (.js), or ZIP (.zip) file.</b>", parse_mode='HTML')

def _logic_check_files(message):
    user_id = message.from_user.id
    # chat_id = message.chat.id # user_id will be used as script_owner_id for buttons
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.reply_to(message, "<b>📂 Your files:</b>\n\n<b>(No files uploaded yet)</b>", parse_mode='HTML')
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name) # Use user_id for checking status
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        # Callback data includes user_id as script_owner_id
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    bot.reply_to(message, "<b>📂 Your files:</b>\n<b>Click to manage.</b>", reply_markup=markup, parse_mode='HTML')

def _logic_bot_speed(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    start_time_ping = time.time()
    wait_msg = bot.reply_to(message, "🏃 𝗧𝗲𝘀𝘁𝗶𝗻𝗴 𝘀𝗽𝗲𝗲𝗱...")
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        # mode = "💰 Free Mode: ON" if free_mode else "💸 Free Mode: OFF" # Removed free_mode
        if user_id == OWNER_ID: user_level = "👑 Owner"
        elif user_id in admin_ids: user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ Premium"
        else: user_level = "🆓 Free User"
        speed_msg = (f"<b>⚡ Bot Speed & Status:</b>\n\n<b>⏱️ API Response Time: {response_time} ms</b>\n"
                     f"<b>🚦 Bot Status: {status}</b>\n"
                     # f"模式 Mode: {mode}\n" # Removed
                     f"<b>👤 Your Level: {user_level}</b>")
        bot.edit_message_text(speed_msg, chat_id, wait_msg.message_id, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error during speed test (cmd): {e}", exc_info=True)
        bot.edit_message_text("<b>❌ Error during speed test.</b>", chat_id, wait_msg.message_id, parse_mode='HTML')

def _logic_contact_owner(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'))
    bot.reply_to(message, "<b>Click to contact Owner:</b>", reply_markup=markup, parse_mode='HTML')

# --- Admin Logic Functions ---
def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Admin permissions required.</b>", parse_mode='HTML')
        return
    bot.reply_to(message, "<b>💳 Subscription Management</b>\n<b>Use inline buttons from /start or admin command menu.</b>", reply_markup=create_subscription_menu(), parse_mode='HTML')

def _logic_statistics(message):
    user_id = message.from_user.id
    if user_id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Admin permissions required.</b>", parse_mode='HTML')
        return
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1) # Extract owner_id from key
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots +=1

    stats_msg_base = (f"<b>📊 Bot Statistics:</b>\n\n"
                      f"<b>👥 Total Users: {total_users}</b>\n"
                      f"<b>📂 Total File Records: {total_files_records}</b>\n"
                      f"<b>🟢 Total Active Bots:{running_bots_count}</b>\n")

    if user_id in admin_ids:
        stats_msg_admin = (f"<b>🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}</b>\n"
                           # f"💰 Free Mode: {'✅ ON' if free_mode else '❌ OFF'}\n" # Removed
                           f"<b>🤖 Your Running Bots: {user_running_bots}</b>")
        stats_msg = stats_msg_base + stats_msg_admin
    else:
        stats_msg = stats_msg_base + f"<b>🤖 Your Running Bots: {user_running_bots}</b>"

    bot.reply_to(message, stats_msg, parse_mode='HTML')


def _logic_broadcast_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Admin permissions required.</b>", parse_mode='HTML')
        return
    msg = bot.reply_to(message, "<b>📢 Send message to broadcast to all active users.</b>\n<b>/cancel to abort.</b>", parse_mode='HTML')
    bot.register_next_step_handler(msg, process_broadcast_message)

def _logic_toggle_lock_bot(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Admin permissions required.</b>", parse_mode='HTML')
        return
    global bot_locked
    bot_locked = not bot_locked
    status = "locked" if bot_locked else "unlocked"
    status_icon = "🔒" if bot_locked else "🔓"
    admin_name = message.from_user.first_name or "Admin"
    admin_username = message.from_user.username or "N/A"
    
    logger.warning(f"Bot {status} by Admin {message.from_user.id} via command/button.")
    bot.reply_to(message, f"<b>{status_icon} Bot has been {status}.</b>", parse_mode='HTML')
    
    # Notify owner if admin is not owner
    if message.from_user.id != OWNER_ID:
        try:
            owner_notification = (
                f"<b>🔔 Bot Status Changed</b>\n\n"
                f"<b>{status_icon} Bot has been {status}</b>\n\n"
                f"<b>👤 Changed by:</b> {admin_name}\n"
                f"<b>✳️ Username:</b> @{admin_username}\n"
                f"<b>🆔 Admin ID:</b> <code>{message.from_user.id}</code>"
            )
            bot.send_message(OWNER_ID, owner_notification, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Failed to notify owner about bot lock/unlock: {e}")

# def _logic_toggle_free_mode(message): # Removed
#     pass

def _logic_admin_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Admin permissions required.</b>", parse_mode='HTML')
        return
    bot.reply_to(message, "<b>👑 Admin Panel</b>\n<b>Manage admins. Use inline buttons from /start or admin menu.</b>",
                 reply_markup=create_admin_panel(), parse_mode='HTML')

def _logic_run_all_scripts(message_or_call):
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.chat.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message 
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("<b>⚠️ Admin permissions required.</b>", parse_mode='HTML')
        return

    # Send initial message and store its ID for later deletion
    initial_msg = bot.send_message(admin_chat_id, "<b>⏳ Starting process to run all user scripts. This may take a while...</b>", parse_mode='HTML')
    logger.info(f"Admin {admin_user_id} initiated run all scripts from chat {admin_chat_id}.")

    started_count = 0; attempted_users = 0; skipped_files = 0; error_files_details = []

    # Use a copy of user_files keys and values to avoid modification issues during iteration
    all_user_files_snapshot = dict(user_files)

    for target_user_id, files_for_user in all_user_files_snapshot.items():
        if not files_for_user: continue
        attempted_users += 1
        logger.info(f"Processing scripts for user {target_user_id}...")
        user_folder = get_user_folder(target_user_id)

        for file_name, file_type in files_for_user:
            # script_owner_id for key context is target_user_id
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                if os.path.exists(file_path):
                    logger.info(f"Admin {admin_user_id} attempting to start '{file_name}' ({file_type}) for user {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            logger.warning(f"Unknown file type '{file_type}' for {file_name} (user {target_user_id}). Skipping.")
                            error_files_details.append(f"`{file_name}` (User {target_user_id}) - Unknown type")
                            skipped_files += 1
                        time.sleep(0.7) # Increased delay slightly
                    except Exception as e:
                        logger.error(f"Error queueing start for '{file_name}' (user {target_user_id}): {e}")
                        error_files_details.append(f"`{file_name}` (User {target_user_id}) - Start error")
                        skipped_files += 1
                else:
                    logger.warning(f"File '{file_name}' for user {target_user_id} not found at '{file_path}'. Skipping.")
                    error_files_details.append(f"`{file_name}` (User {target_user_id}) - File not found")
                    skipped_files += 1
            # else: logger.info(f"Script '{file_name}' for user {target_user_id} already running.")

    summary_msg = (f"<b>✅ All Users' Scripts - Processing Complete:</b>\n\n"
                   f"<b>▶️ Attempted to start: {started_count} scripts.</b>\n"
                   f"<b>👥 Users processed: {attempted_users}.</b>\n")
    if skipped_files > 0:
        summary_msg += f"<b>⚠️ Skipped/Error files: {skipped_files}</b>\n"
        if error_files_details:
             summary_msg += "<b>Details (first 5):</b>\n" + "\n".join([f"  - {err}" for err in error_files_details[:5]])
             if len(error_files_details) > 5: summary_msg += "\n  <b>... and more (check logs).</b>"

    # Delete the initial "starting process" message
    try:
        bot.delete_message(admin_chat_id, initial_msg.message_id)
        logger.info(f"Deleted initial 'run all scripts' message {initial_msg.message_id}")
    except Exception as delete_e:
        logger.error(f"Could not delete initial message {initial_msg.message_id}: {delete_e}")
    
    # Send final summary
    bot.send_message(admin_chat_id, summary_msg, parse_mode='HTML')
    logger.info(f"Run all scripts finished. Admin: {admin_user_id}. Started: {started_count}. Skipped/Errors: {skipped_files}")


# --- Command Handlers & Text Handlers for ReplyKeyboard ---
def ban_check_wrapper(func):
    """Decorator to check if user is banned before executing command"""
    def wrapper(message):
        if is_user_banned(message.from_user.id):
            bot.reply_to(message, "<b>🚫 You are banned from using this bot.</b>\n"
                                  f"<b>Reason:</b> {banned_users[message.from_user.id]['reason']}", parse_mode='HTML')
            return
        return func(message)
    return wrapper

def channel_verification_wrapper(func):
    """Decorator to check if user has joined the update channel"""
    def wrapper(message):
        user_id = message.from_user.id
        
        # Skip verification for owner and admins
        if user_id in admin_ids:
            return func(message)
        
        # Check if user has joined the channel
        if not check_channel_membership(user_id):
            message_text, markup = create_channel_join_message()
            bot.reply_to(message, message_text, reply_markup=markup, parse_mode='HTML')
            return
        
        return func(message)
    return wrapper

def full_verification_wrapper(func):
    """Combined wrapper for ban check and channel verification"""
    def wrapper(message):
        # First check if banned
        if is_user_banned(message.from_user.id):
            bot.reply_to(message, "<b>🚫 You are banned from using this bot.</b>\n"
                                  f"<b>Reason:</b> {banned_users[message.from_user.id]['reason']}", parse_mode='HTML')
            return
        
        user_id = message.from_user.id
        
        # Skip channel verification for owner and admins
        if user_id not in admin_ids:
            # Check if user has joined the channel
            if not check_channel_membership(user_id):
                message_text, markup = create_channel_join_message()
                bot.reply_to(message, message_text, reply_markup=markup, parse_mode='HTML')
                return
        
        return func(message)
    return wrapper

@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message): 
    if is_user_banned(message.from_user.id):
        bot.reply_to(message, "<b>🚫 You are banned from using this bot.</b>\n"
                              f"<b>Reason:</b> {banned_users[message.from_user.id]['reason']}", parse_mode='HTML')
        return
    
    user_id = message.from_user.id
    
    # Skip channel verification for owner and admins
    if user_id not in admin_ids:
        # Check if user has joined the channel
        if not check_channel_membership(user_id):
            message_text, markup = create_channel_join_message()
            bot.reply_to(message, message_text, reply_markup=markup, parse_mode='HTML')
            return
    
    _logic_send_welcome(message)

@bot.message_handler(commands=['status']) # Kept for direct command
@full_verification_wrapper
def command_show_status(message): _logic_statistics(message) # Changed to call _logic_statistics


BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": _logic_updates_channel,
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📞 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics, 
    "👑 Admin Panel": _logic_admin_panel,
}

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    if is_user_banned(message.from_user.id):
        bot.reply_to(message, "<b>🚫 You are banned from using this bot.</b>\n"
                              f"<b>Reason:</b> {banned_users[message.from_user.id]['reason']}", parse_mode='HTML')
        return
    
    user_id = message.from_user.id
    
    # Skip channel verification for owner and admins
    if user_id not in admin_ids:
        # Check if user has joined the channel
        if not check_channel_membership(user_id):
            message_text, markup = create_channel_join_message()
            bot.reply_to(message, message_text, reply_markup=markup, parse_mode='HTML')
            return
    
    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func: logic_func(message)
    else: logger.warning(f"Button text '{message.text}' matched but no logic func.")

@bot.message_handler(commands=['updateschannel'])
@full_verification_wrapper
def command_updates_channel(message): _logic_updates_channel(message)
@bot.message_handler(commands=['uploadfile'])
@full_verification_wrapper
def command_upload_file(message): _logic_upload_file(message)
@bot.message_handler(commands=['checkfiles'])
@full_verification_wrapper
def command_check_files(message): _logic_check_files(message)
@bot.message_handler(commands=['botspeed'])
@full_verification_wrapper
def command_bot_speed(message): _logic_bot_speed(message)
@bot.message_handler(commands=['contactowner'])
@full_verification_wrapper
def command_contact_owner(message): _logic_contact_owner(message)
@bot.message_handler(commands=['subscriptions'])
@full_verification_wrapper
def command_subscriptions(message): _logic_subscriptions_panel(message)
@bot.message_handler(commands=['statistics']) # Alias for /status
@full_verification_wrapper
def command_statistics(message): _logic_statistics(message)
@bot.message_handler(commands=['broadcast'])
@full_verification_wrapper
def command_broadcast(message): _logic_broadcast_init(message)
@bot.message_handler(commands=['lockbot']) 
@full_verification_wrapper
def command_lock_bot(message): _logic_toggle_lock_bot(message)
# @bot.message_handler(commands=['freemode']) # Removed
# def command_free_mode(message): _logic_toggle_free_mode(message)
@bot.message_handler(commands=['adminpanel'])
@full_verification_wrapper
def command_admin_panel(message): _logic_admin_panel(message)
@bot.message_handler(commands=['runningallcode']) # Added
@full_verification_wrapper
def command_run_all_code(message): _logic_run_all_scripts(message)

# Security log commands removed


@bot.message_handler(commands=['ping'])
@full_verification_wrapper
def ping(message):
    start_ping_time = time.time() 
    msg = bot.reply_to(message, "<b>Pong!</b>", parse_mode='HTML')
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"<b>Pong! Latency:{latency} ms</b>", message.chat.id, msg.message_id, parse_mode='HTML')


# --- Document (File) Handler ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message): # Renamed
    user_id = message.from_user.id
    chat_id = message.chat.id # Used for replies, script context uses user_id
    doc = message.document
    logger.info(f"Doc from {user_id}: {doc.file_name} ({doc.mime_type}), Size: {doc.file_size}")

    if is_user_banned(user_id):
        bot.reply_to(message, "<b>🚫 You are banned from using this bot.</b>\n"
                              f"<b>Reason:</b> {banned_users[user_id]['reason']}", parse_mode='HTML')
        return

    # Skip channel verification for owner and admins
    if user_id not in admin_ids:
        # Check if user has joined the channel
        if not check_channel_membership(user_id):
            message_text, markup = create_channel_join_message()
            bot.reply_to(message, message_text, reply_markup=markup, parse_mode='HTML')
            return

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "<b>⚠️ Bot locked, cannot accept files.</b>", parse_mode='HTML')
        return

    # File limit check (relies on FREE_USER_LIMIT being > 0 for free users)
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"<b>⚠️ File limit ({current_files}/{limit_str}) reached. Delete files via /checkfiles.</b>", parse_mode='HTML')
        return

    file_name = doc.file_name
    if not file_name: bot.reply_to(message, "<b>⚠️ No file name. Ensure file has a name.</b>", parse_mode='HTML'); return
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip']:
        bot.reply_to(message, "<b>⚠️ Unsupported type! Only .py, .js, .zip allowed.</b>", parse_mode='HTML')
        return
    max_file_size = 20 * 1024 * 1024 # 20 MB
    if doc.file_size > max_file_size:
        bot.reply_to(message, f"<b>⚠️ File too large (Max: {max_file_size // 1024 // 1024} MB).</b>", parse_mode='HTML'); return

    try:
        download_wait_msg = bot.reply_to(message, f"<b>⏳ Downloading</b> <code>{file_name}</code> <b>for security check...</b>", parse_mode='HTML')
        file_info_tg_doc = bot.get_file(doc.file_id)
        downloaded_file_content = bot.download_file(file_info_tg_doc.file_path)
        
        # Store file for approval (no security scan)
        bot.edit_message_text(f"<b>📁 Processing</b> <code>{file_name}</code> <b>for approval...</b>", chat_id, download_wait_msg.message_id, parse_mode='HTML')
        pending_key = f"{user_id}_{file_name}"
        pending_files[pending_key] = {
            'file_content': downloaded_file_content,
            'file_ext': file_ext,
            'user_id': user_id,
            'message': message,
            'upload_message_id': download_wait_msg.message_id,
            'chat_id': chat_id
        }
        
        try:
            # Get user info
            user_info = message.from_user
            user_name = user_info.first_name or "Unknown"
            user_username = user_info.username or "Not set"
            user_status = ""
            
            # Determine user status
            if user_id == OWNER_ID:
                user_status = "👑 Owner"
            elif user_id in admin_ids:
                user_status = "🛡️ Admin"
            elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now():
                user_status = "⭐ Premium"
            else:
                user_status = "🆓 Free User"
            
            # Get file upload limits for display
            file_limit = get_user_file_limit(user_id)
            current_files = get_user_file_count(user_id)
            limit_str = str(file_limit) if file_limit != float('inf') else "∞"
            
            # Create detailed notification caption with profile link and file limits
            import html
            profile_url = f"tg://user?id={user_id}"
            
            # Escape HTML entities to prevent parsing errors
            safe_user_name = html.escape(user_name)
            safe_user_username = html.escape(user_username)
            safe_file_name = html.escape(file_name)
            
            notification_caption = (
                f"<b>📁 𝗙𝗜𝗟𝗘 𝗨𝗣𝗟𝗢𝗔𝗗𝗘𝗗</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>👤 𝗡𝗔𝗠𝗘:</b> {safe_user_name}\n"
                f"<b>✳️ 𝗨𝗦𝗘𝗥𝗡𝗔𝗠𝗘:</b> @{safe_user_username}\n"
                f"<b>🔗 𝗣𝗥𝗢𝗙𝗜𝗟𝗘 𝗟𝗜𝗡𝗞:</b> <a href='{profile_url}'>𝗖𝗟𝗜𝗖𝗞 𝗛𝗘𝗥𝗘</a>\n"
                f"<b>🆔 𝗨𝗦𝗘𝗥 𝗜𝗗:</b> <code>{user_id}</code>\n"
                f"<b>🔰 𝗦𝗧𝗔𝗧𝗨𝗦:</b> {user_status}\n"
                f"<b>📄 𝗙𝗜𝗟𝗘 𝗨𝗣𝗟𝗢𝗔𝗗𝗘𝗗:</b> <code>{safe_file_name}</code>\n"
                f"<b>📊 𝗙𝗜𝗟𝗘𝗦 𝗨𝗣𝗟𝗢𝗔𝗗𝗘𝗗:</b> {current_files}/{limit_str}"
            )
            
            # Create approval buttons for owner
            approval_markup = types.InlineKeyboardMarkup()
            approval_markup.row(
                types.InlineKeyboardButton("✅ Approve", callback_data=f'approve_{user_id}_{file_name}'),
                types.InlineKeyboardButton("❌ Decline", callback_data=f'decline_{user_id}_{file_name}')
            )
            
            # Send file to owner with caption and approval buttons
            bot.send_document(OWNER_ID, doc.file_id, caption=notification_caption, reply_markup=approval_markup, parse_mode='HTML')
            
        except Exception as e: 
            logger.error(f"Failed to forward uploaded file to OWNER_ID {OWNER_ID}: {e}")
        
        # Notify user that file is pending approval
        bot.edit_message_text(
            f"<b>✅ File</b> <code>{file_name}</code> <b>uploaded successfully!</b>\n"
            f"<b>⏳ Waiting for owner approval...</b>",
            chat_id, download_wait_msg.message_id, parse_mode='HTML'
        )
    except telebot.apihelper.ApiTelegramException as e:
         logger.error(f"Telegram API Error handling file for {user_id}: {e}", exc_info=True)
         if "file is too big" in str(e).lower():
              bot.reply_to(message, f"<b>❌ Telegram API Error: File too large to download (~20MB limit).</b>", parse_mode='HTML')
         else: bot.reply_to(message, f"<b>❌ Telegram API Error:</b> {str(e)}. <b>Try later.</b>", parse_mode='HTML')
    except Exception as e:
        logger.error(f"❌ General error handling file for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"<b>❌ Unexpected error:</b> {str(e)}", parse_mode='HTML')
# --- End Document Handler ---


# --- Callback Query Handlers (for Inline Buttons) ---
@bot.callback_query_handler(func=lambda call: True) 
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    logger.info(f"Callback: User={user_id}, Data='{data}'")

    if is_user_banned(user_id):
        bot.answer_callback_query(call.id, f"🚫 You are banned: {banned_users[user_id]['reason']}", show_alert=True)
        return

    # Skip channel verification for owner/admins and verification callback itself
    if user_id not in admin_ids and data != 'verify_channel_join':
        if not check_channel_membership(user_id):
            message_text, markup = create_channel_join_message()
            try:
                bot.edit_message_text(message_text, call.message.chat.id, call.message.message_id, 
                                    reply_markup=markup, parse_mode='HTML')
            except:
                bot.send_message(call.message.chat.id, message_text, reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id, "Please join the channel first!", show_alert=True)
            return

    if bot_locked and user_id not in admin_ids and data not in ['back_to_main', 'speed', 'stats', 'verify_channel_join']: # Allow stats
        bot.answer_callback_query(call.id, "⚠️ Bot locked by admin.", show_alert=True)
        return
    try:
        if data == 'upload': upload_callback(call)
        elif data == 'check_files': check_files_callback(call)
        elif data.startswith('file_'): file_control_callback(call)
        elif data.startswith('start_'): start_bot_callback(call)
        elif data.startswith('stop_'): stop_bot_callback(call)
        elif data.startswith('restart_'): restart_bot_callback(call)
        elif data.startswith('delete_'): delete_bot_callback(call)
        elif data.startswith('logs_'): logs_bot_callback(call)
        elif data.startswith('approve_'): approve_file_callback(call)
        elif data.startswith('decline_'): decline_file_callback(call)
        elif data == 'verify_channel_join': verify_channel_join_callback(call)
        elif data == 'speed': speed_callback(call)
        elif data == 'back_to_main': back_to_main_callback(call)
        elif data.startswith('confirm_broadcast_'): handle_confirm_broadcast(call)
        elif data == 'cancel_broadcast': handle_cancel_broadcast(call)
        # --- Admin Callbacks ---
        elif data == 'subscription': admin_required_callback(call, subscription_management_callback)
        elif data == 'stats': admin_required_callback(call, stats_callback)
        elif data == 'lock_bot': admin_required_callback(call, lock_bot_callback)
        elif data == 'unlock_bot': admin_required_callback(call, unlock_bot_callback)
        # elif data == 'free_mode': admin_required_callback(call, toggle_free_mode_callback) # Removed
        elif data == 'run_all_scripts': admin_required_callback(call, run_all_scripts_callback) # Added
        elif data == 'broadcast': admin_required_callback(call, broadcast_init_callback) 
        elif data == 'admin_panel': admin_required_callback(call, admin_panel_callback)
        elif data == 'add_admin': owner_required_callback(call, add_admin_init_callback) 
        elif data == 'remove_admin': owner_required_callback(call, remove_admin_init_callback) 
        elif data == 'list_admins': admin_required_callback(call, list_admins_callback)
        elif data == 'add_subscription': admin_required_callback(call, add_subscription_init_callback) 
        elif data == 'remove_subscription': admin_required_callback(call, remove_subscription_init_callback) 
        elif data == 'check_subscription': admin_required_callback(call, check_subscription_init_callback)
        elif data == 'list_all_files': admin_required_callback(call, list_all_files_callback)
        elif data == 'ban_user': admin_required_callback(call, ban_user_init_callback)
        elif data == 'unban_user': admin_required_callback(call, unban_user_init_callback)
        elif data == 'list_banned_users': admin_required_callback(call, list_banned_users_callback)
        elif data == 'ban_unban_menu': admin_required_callback(call, ban_unban_menu_callback)
        elif data == 'admin_management_menu': owner_required_callback(call, admin_management_menu_callback)
        elif data == 'direct_message': admin_required_callback(call, direct_message_init_callback)
        else:
            bot.answer_callback_query(call.id, "Unknown action.")
            logger.warning(f"Unhandled callback data: {data} from user {user_id}")
    except Exception as e:
        logger.error(f"Error handling callback '{data}' for {user_id}: {e}", exc_info=True)
        try: bot.answer_callback_query(call.id, "Error processing request.", show_alert=True)
        except Exception as e_ans: logger.error(f"Failed to answer callback after error: {e_ans}")

def admin_required_callback(call, func_to_run):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ 𝗔𝗱𝗺𝗶𝗻 𝗽𝗲𝗿𝗺𝗶𝘀𝘀𝗶𝗼𝗻𝘀 𝗿𝗲𝗾𝘂𝗶𝗿𝗲𝗱.", show_alert=True)
        return
    func_to_run(call) 

def owner_required_callback(call, func_to_run):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ 𝗢𝘄𝗻𝗲𝗿 𝗽𝗲𝗿𝗺𝗶𝘀𝘀𝗶𝗼𝗻𝘀 𝗿𝗲𝗾𝘂𝗶𝗿𝗲𝗱.", show_alert=True)
        return
    func_to_run(call)

def upload_callback(call):
    user_id = call.from_user.id
    # Removed free_mode check
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.answer_callback_query(call.id, f"⚠️ File limit ({current_files}/{limit_str}) reached.", show_alert=True)
        return
    bot.answer_callback_query(call.id) 
    bot.reply_to(call.message, "<b>📤 Send your Python (.py), JS (.js), or ZIP (.zip) file.</b>", parse_mode='HTML')

def check_files_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id 
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ No files uploaded.", show_alert=True)
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
            bot.edit_message_text("<b>📂 Your files:</b>\n\n<b>(No files uploaded)</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e: logger.error(f"Error editing msg for empty file list: {e}")
        return
    bot.answer_callback_query(call.id) 
    markup = types.InlineKeyboardMarkup(row_width=1) 
    for file_name, file_type in sorted(user_files_list): 
        is_running = is_bot_running(user_id, file_name) # Use user_id for status check
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        # Callback includes user_id as script_owner_id
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    try:
        bot.edit_message_text("<b>📂 Your files:\nClick to manage.</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): logger.warning("Msg not modified (files).")
         else: logger.error(f"Error editing msg for file list: {e}")
    except Exception as e: logger.error(f"Unexpected error editing msg for file list: {e}", exc_info=True)

def file_control_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        # Allow owner/admin to control any file, or user to control their own
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            logger.warning(f"User {requesting_user_id} tried to access file '{file_name}' of user {script_owner_id} without permission.")
            bot.answer_callback_query(call.id, "⚠️ You can only manage your own files.", show_alert=True)
            check_files_callback(call) # Show their own files
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            logger.warning(f"File '{file_name}' not found for user {script_owner_id} during control.")
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            # If admin was viewing, this might be confusing. For now, just show their own.
            check_files_callback(call) 
            return

        bot.answer_callback_query(call.id) 
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_running else '🔴 Stopped'
        file_type = next((f[1] for f in user_files_list if f[0] == file_name), '?') 
        try:
            bot.edit_message_text(
                f"<b>⚙️ Controls for: <code>{file_name}</code> ({file_type}) of User <code>{script_owner_id}</code>\nStatus: {status_text}</b>",
                call.message.chat.id, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_running),
                parse_mode='HTML'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified (controls for {file_name})")
             else: raise 
    except (ValueError, IndexError) as ve:
        logger.error(f"Error parsing file control callback: {ve}. Data: '{call.data}'")
        bot.answer_callback_query(call.id, "Error: Invalid action data.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in file_control_callback for data '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)

def start_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id # Where the admin/user gets the reply

        logger.info(f"Start request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied to start this script.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name); check_files_callback(call); return

        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already running.", show_alert=True)
            try: bot.edit_message_reply_markup(chat_id_for_reply, call.message.message_id, reply_markup=create_control_buttons(script_owner_id, file_name, True))
            except Exception as e: logger.error(f"Error updating buttons (already running): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ Attempting to start {file_name} for user {script_owner_id}...")

        # Pass call.message as message_obj_for_reply so feedback goes to the person who clicked
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ Error: Unknown file type '{file_type}' for '{file_name}'."); return 

        time.sleep(1.5) # Give script time to actually start or fail early
        is_now_running = is_bot_running(script_owner_id, file_name) 
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed, check logs/replies)'
        try:
            bot.edit_message_text(
                f"<b>⚙️ Controls for: {file_name} ({file_type}) of User {script_owner_id}\nStatus: {status_text}</b>",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), parse_mode='HTML'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified after starting {file_name}")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing start callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid start command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in start_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error starting script.", show_alert=True)
        try: # Attempt to reset buttons to 'stopped' state on error
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: logger.error(f"Failed to update buttons after start error: {e_btn}")

def stop_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Stop request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1] 
        script_key = f"{script_owner_id}_{file_name}"

        if not is_bot_running(script_owner_id, file_name): 
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already stopped.", show_alert=True)
            try:
                 bot.edit_message_text(
                     f"<b>⚙️ Controls for: {file_name} ({file_type}) of User {script_owner_id}\nStatus: 🔴 Stopped</b>",
                     chat_id_for_reply, call.message.message_id,
                     reply_markup=create_control_buttons(script_owner_id, file_name, False), parse_mode='HTML')
            except Exception as e: logger.error(f"Error updating buttons (already stopped): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ Stopping {file_name} for user {script_owner_id}...")
        process_info = bot_scripts.get(script_key)
        if process_info:
            kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]; logger.info(f"Removed {script_key} from running after stop.")
        else: logger.warning(f"Script {script_key} running by psutil but not in bot_scripts dict.")

        try:
            bot.edit_message_text(
                f"<b>⚙️ Controls for: {file_name} ({file_type}) of User {script_owner_id}\nStatus: 🔴 Stopped</b>",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, False), parse_mode='HTML'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified after stopping {file_name}")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing stop callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid stop command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in stop_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error stopping script.", show_alert=True)

def restart_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Restart: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        file_type = file_info[1]; user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name); script_key = f"{script_owner_id}_{file_name}"

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name)
            if script_key in bot_scripts: del bot_scripts[script_key]
            check_files_callback(call); return

        bot.answer_callback_query(call.id, f"⏳ Restarting {file_name} for user {script_owner_id}...")
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Restart: Stopping existing {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(1.5) 

        logger.info(f"Restart: Starting script {script_key}...")
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ Unknown type '{file_type}' for '{file_name}'."); return

        time.sleep(1.5) 
        is_now_running = is_bot_running(script_owner_id, file_name) 
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed)'
        try:
            bot.edit_message_text(
                f"<b>⚙️ Controls for: {file_name} ({file_type}) of User {script_owner_id}\nStatus: {status_text}</b>",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), parse_mode='HTML'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): logger.warning(f"Msg not modified (restart {file_name})")
             else: raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing restart callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid restart command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in restart_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error restarting.", show_alert=True)
        try:
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: logger.error(f"Failed to update buttons after restart error: {e_btn}")


def delete_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Delete: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        bot.answer_callback_query(call.id, f"🗑️ Deleting {file_name} for user {script_owner_id}...")
        script_key = f"{script_owner_id}_{file_name}"
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Delete: Stopping {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: kill_process_tree(process_info)
            if script_key in bot_scripts: del bot_scripts[script_key]
            time.sleep(0.5) 

        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        deleted_disk = []
        if os.path.exists(file_path):
            try: os.remove(file_path); deleted_disk.append(file_name); logger.info(f"Deleted file: {file_path}")
            except OSError as e: logger.error(f"Error deleting {file_path}: {e}")
        if os.path.exists(log_path):
            try: os.remove(log_path); deleted_disk.append(os.path.basename(log_path)); logger.info(f"Deleted log: {log_path}")
            except OSError as e: logger.error(f"Error deleting log {log_path}: {e}")

        remove_user_file_db(script_owner_id, file_name)
        deleted_str = ", ".join(f"`{f}`" for f in deleted_disk) if deleted_disk else "associated files"
        try:
            bot.edit_message_text(
                f"<b>🗑️ Record {file_name} (User {script_owner_id}) and {deleted_str} deleted!</b>",
                chat_id_for_reply, call.message.message_id, reply_markup=None, parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Error editing msg after delete: {e}")
            bot.send_message(chat_id_for_reply, f"🗑️ Record `{file_name}` deleted.", parse_mode='Markdown')
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing delete callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid delete command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in delete_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error deleting.", show_alert=True)

def logs_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Logs: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True); return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True); check_files_callback(call); return

        user_folder = get_user_folder(script_owner_id)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        if not os.path.exists(log_path):
            bot.answer_callback_query(call.id, f"⚠️ No logs for '{file_name}'.", show_alert=True); return

        bot.answer_callback_query(call.id) 
        try:
            log_content = ""; file_size = os.path.getsize(log_path)
            max_log_kb = 100; max_tg_msg = 4096
            if file_size == 0: log_content = "(Log empty)"
            elif file_size > max_log_kb * 1024:
                 with open(log_path, 'rb') as f: f.seek(-max_log_kb * 1024, os.SEEK_END); log_bytes = f.read()
                 log_content = log_bytes.decode('utf-8', errors='ignore')
                 log_content = f"(Last {max_log_kb} KB)\n...\n" + log_content
            else:
                 with open(log_path, 'r', encoding='utf-8', errors='ignore') as f: log_content = f.read()

            if len(log_content) > max_tg_msg:
                log_content = log_content[-max_tg_msg:]
                first_nl = log_content.find('\n')
                if first_nl != -1: log_content = "...\n" + log_content[first_nl+1:]
                else: log_content = "...\n" + log_content 
            if not log_content.strip(): log_content = "(No visible content)"

            bot.send_message(chat_id_for_reply, f"📜 Logs for `{file_name}` (User `{script_owner_id}`):\n```\n{log_content}\n```", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error reading/sending log {log_path}: {e}", exc_info=True)
            bot.send_message(chat_id_for_reply, f"❌ Error reading log for `{file_name}`.")
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing logs callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid logs command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in logs_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error fetching logs.", show_alert=True)

def speed_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    start_cb_ping_time = time.time() 
    try:
        bot.edit_message_text("🏃 Testing speed...", chat_id, call.message.message_id)
        bot.send_chat_action(chat_id, 'typing') 
        response_time = round((time.time() - start_cb_ping_time) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        # mode = "💰 Free Mode: ON" if free_mode else "💸 Free Mode: OFF" # Removed
        if user_id == OWNER_ID: user_level = "👑 Owner"
        elif user_id in admin_ids: user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ Premium"
        else: user_level = "🆓 Free User"
        speed_msg = (f"<b>⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     # f"模式 Mode: {mode}\n" # Removed
                     f"👤 Your Level: {user_level}</b>")
        bot.answer_callback_query(call.id) 
        bot.edit_message_text(speed_msg, chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id), parse_mode='HTML')
    except Exception as e:
         logger.error(f"Error during speed test (cb): {e}", exc_info=True)
         bot.answer_callback_query(call.id, "Error in speed test.", show_alert=True)
         try: bot.edit_message_text("〽️ Main Menu", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
         except Exception: pass

def approve_file_callback(call):
    """Handle file approval by owner"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Only owner can approve files.", show_alert=True)
        return
    
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        target_user_id = int(user_id_str)
        pending_key = f"{target_user_id}_{file_name}"
        
        if pending_key not in pending_files:
            bot.answer_callback_query(call.id, "⚠️ File not found in pending list.", show_alert=True)
            return
        
        bot.answer_callback_query(call.id, f"✅ Approving {file_name}...")
        
        # Get pending file data
        pending_data = pending_files[pending_key]
        file_content = pending_data['file_content']
        file_ext = pending_data['file_ext']
        original_message = pending_data['message']
        
        # Process the approved file
        user_folder = get_user_folder(target_user_id)
        
        if file_ext == '.zip':
            handle_zip_file(file_content, file_name, original_message)
        else:
            file_path = os.path.join(user_folder, file_name)
            with open(file_path, 'wb') as f:
                f.write(file_content)
            logger.info(f"Saved approved file to {file_path}")
            
            if file_ext == '.js':
                handle_js_file(file_path, target_user_id, user_folder, file_name, original_message)
            elif file_ext == '.py':
                handle_py_file(file_path, target_user_id, user_folder, file_name, original_message)
        
        # Remove from pending
        del pending_files[pending_key]
        
        # Delete the upload message and notify user about approval
        try:
            upload_msg_id = pending_data.get('upload_message_id')
            user_chat_id = pending_data.get('chat_id')
            if upload_msg_id and user_chat_id:
                try:
                    bot.delete_message(user_chat_id, upload_msg_id)
                    logger.info(f"Deleted upload message {upload_msg_id} for approved file {file_name}")
                except Exception as delete_e:
                    logger.error(f"Could not delete upload message {upload_msg_id}: {delete_e}")
            
            bot.send_message(
                target_user_id,
                f"<b>✅ File Approved!</b>\n\n"
                f"<b>📄 Your file</b> <code>{file_name}</code> <b>has been approved by the owner and is now being processed!</b>\n"
                f"<b>🚀 The script will start running shortly.</b>",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about approval: {e}")
        
    except Exception as e:
        logger.error(f"Error in approve_file_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error processing approval.", show_alert=True)

def decline_file_callback(call):
    """Handle file decline by owner"""
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Only owner can decline files.", show_alert=True)
        return
    
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        target_user_id = int(user_id_str)
        pending_key = f"{target_user_id}_{file_name}"
        
        if pending_key not in pending_files:
            bot.answer_callback_query(call.id, "⚠️ File not found in pending list.", show_alert=True)
            return
        
        bot.answer_callback_query(call.id, f"❌ Declining {file_name}...")
        
        # Get pending file data before removing it
        pending_data = pending_files[pending_key]
        
        # Remove from pending
        del pending_files[pending_key]
        
        # Delete the upload message and notify user about decline
        try:
            upload_msg_id = pending_data.get('upload_message_id')
            user_chat_id = pending_data.get('chat_id')
            if upload_msg_id and user_chat_id:
                try:
                    bot.delete_message(user_chat_id, upload_msg_id)
                    logger.info(f"Deleted upload message {upload_msg_id} for declined file {file_name}")
                except Exception as delete_e:
                    logger.error(f"Could not delete upload message {upload_msg_id}: {delete_e}")
            
            bot.send_message(
                target_user_id,
                f"<b>❌ File Declined</b>\n\n"
                f"<b>📄 Your file</b> <code>{file_name}</code> <b>was declined by the owner.</b>\n"
                f"<b>💡 Please check if your file follows the guidelines and try uploading again.</b>",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id} about decline: {e}")
        
    except Exception as e:
        logger.error(f"Error in decline_file_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Error processing decline.", show_alert=True)

def verify_channel_join_callback(call):
    """Handle channel join verification"""
    user_id = call.from_user.id
    
    # Create a fake message object with correct user info for welcome function
    class FakeMessage:
        def __init__(self, from_user, chat):
            self.from_user = from_user
            self.chat = chat
    
    class FakeChat:
        def __init__(self, chat_id):
            self.id = chat_id
    
    # Create fake message with actual user's info instead of bot's info
    fake_message = FakeMessage(call.from_user, FakeChat(call.message.chat.id))
    
    # Skip verification for owner and admins
    if user_id in admin_ids:
        bot.answer_callback_query(call.id, "✅ Admin/Owner access granted!", show_alert=False)
        
        # Delete the verification message
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception as e:
            logger.error(f"Could not delete verification message: {e}")
        
        # Send thank you message
        bot.send_message(call.message.chat.id, 
                        "🎉 <b>Thank you for joining our channel!</b>\n\n"
                        "✅ <b>You now have full access to the bot.</b>", 
                        parse_mode='HTML')
        
        _logic_send_welcome(fake_message)
        return
    
    # Check if user has actually joined the channel
    if check_channel_membership(user_id):
        bot.answer_callback_query(call.id, "✅ Welcome! Channel verification successful!", show_alert=False)
        
        # Delete the verification message
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception as e:
            logger.error(f"Could not delete verification message: {e}")
        
        # Send thank you message
        bot.send_message(call.message.chat.id, 
                        "🎉 <b>Thank you for joining our channel!</b>\n\n"
                        "✅ <b>You now have full access to the bot.</b>", 
                        parse_mode='HTML')
        
        # Send welcome message with correct user info
        _logic_send_welcome(fake_message)
    else:
        bot.answer_callback_query(call.id, "❌ Please join the channel first, then click 'Joined' again.", show_alert=True)

def back_to_main_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 Owner"
    elif user_id in admin_ids: user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: user_status = "🆓 Free User (Expired Sub)" # Will be cleaned up by welcome if not already
    else: user_status = "🆓 Free User"
    main_menu_text = (f"<b>〽️ Welcome back, {call.from_user.first_name}!\n\n🆔 ID: <code>{user_id}</code>\n"
                      f"🔰 Status: {user_status}{expiry_info}\n📁 Files: {current_files} / {limit_str}\n\n"
                      f"👇 Use buttons or type commands.</b>")
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text(main_menu_text, chat_id, call.message.message_id,
                              reply_markup=create_main_menu_inline(user_id), parse_mode='HTML')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): logger.warning("Msg not modified (back_to_main).")
         else: logger.error(f"API error on back_to_main: {e}")
    except Exception as e: logger.error(f"Error handling back_to_main: {e}", exc_info=True)

# --- Admin Callback Implementations (for Inline Buttons) ---
def subscription_management_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("💳 Subscription Management\nSelect action:",
                              call.message.chat.id, call.message.message_id, reply_markup=create_subscription_menu())
    except Exception as e: logger.error(f"Error showing sub menu: {e}")

def stats_callback(call): # Called by admin only now
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    # Send stats as a separate message instead of editing the admin panel
    user_id = call.from_user.id
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1) 
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots +=1

    stats_msg_base = (f"<b>📊 Bot Statistics:\n\n"
                      f"👥 Total Users: {total_users}\n"
                      f"📂 Total File Records: {total_files_records}\n"
                      f"🟢 Total Active Bots: {running_bots_count}\n")

    stats_msg_admin = (f"🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}\n"
                       f"🤖 Your Running Bots: {user_running_bots}</b>")
    stats_msg = stats_msg_base + stats_msg_admin

    bot.send_message(call.message.chat.id, stats_msg, parse_mode='HTML')


def lock_bot_callback(call):
    global bot_locked; bot_locked = True
    admin_name = call.from_user.first_name or "Admin"
    admin_username = call.from_user.username or "N/A"
    
    logger.warning(f"Bot locked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔒 Bot locked.")
    
    # Notify owner if admin is not owner
    if call.from_user.id != OWNER_ID:
        try:
            owner_notification = (
                f"<b>🔔 Bot Status Changed</b>\n\n"
                f"<b>🔒 Bot has been locked</b>\n\n"
                f"<b>👤 Changed by: {admin_name}</b>\n"
                f"<b>✳️ Username: @{admin_username}</b>\n"
                f"<b>🆔 Admin ID:<code>{call.from_user.id}</code></b>"
            )
            bot.send_message(OWNER_ID, owner_notification, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Failed to notify owner about bot lock: {e}")
    
    try: 
        bot.edit_message_text("<b>👑 Admin Panel</b>\n<b>Manage admins (Owner actions may be restricted).</b>",
                              call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel(), parse_mode='HTML')
    except Exception as e: logger.error(f"Error updating admin panel (lock): {e}")

def unlock_bot_callback(call):
    global bot_locked; bot_locked = False
    admin_name = call.from_user.first_name or "Admin"
    admin_username = call.from_user.username or "N/A"
    
    logger.warning(f"Bot unlocked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔓 Bot unlocked.")
    
    # Notify owner if admin is not owner
    if call.from_user.id != OWNER_ID:
        try:
            owner_notification = (
                f"<b>🔔 Bot Status Changed</b>\n\n"
                f"<b>🔓 Bot has been unlocked</b>\n\n"
                f"<b>👤 Changed by: {admin_name}</b>\n"
                f"<b>✳️ Username: @{admin_username}</b>\n"
                f"<b>🆔 Admin ID: <code>{call.from_user.id}</code></b>"
            )
            bot.send_message(OWNER_ID, owner_notification, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Failed to notify owner about bot unlock: {e}")
    
    try: 
        bot.edit_message_text("<b>👑 Admin Panel</b>\n<b>Manage admins (Owner actions may be restricted).</b>",
                              call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel(), parse_mode='HTML')
    except Exception as e: logger.error(f"Error updating admin panel (unlock): {e}")

# def toggle_free_mode_callback(call): # Removed
#     pass

def run_all_scripts_callback(call): # Added
    _logic_run_all_scripts(call) # Pass the call object
    # Stay in admin panel after running all scripts
    try:
        bot.edit_message_text("👑 Admin Panel\nManage admins (Owner actions may be restricted).",
                              call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
    except Exception as e: logger.error(f"Error updating admin panel after run all scripts: {e}")


def broadcast_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "<b>📢 Send message to broadcast.</b>\n<b>/cancel to abort.</b>", parse_mode='HTML')
    bot.register_next_step_handler(msg, process_broadcast_message)

def process_broadcast_message(message):
    user_id = message.from_user.id
    if user_id not in admin_ids: 
        bot.reply_to(message, "<b>⚠️ Not authorized.</b>", parse_mode='HTML')
        return
    if message.text and message.text.lower() == '/cancel': 
        bot.reply_to(message, "<b>Broadcast cancelled.</b>", parse_mode='HTML')
        return

    broadcast_content = message.text # Can also handle photos, videos etc. if message.content_type is checked
    if not broadcast_content and not (message.photo or message.video or message.document or message.sticker or message.voice or message.audio): # If no text and no other media
         bot.reply_to(message, "<b>⚠️ Cannot broadcast empty message. Send text or media, or /cancel.</b>", parse_mode='HTML')
         msg = bot.send_message(message.chat.id, "<b>📢 Send broadcast message or /cancel.</b>", parse_mode='HTML', reply_to_message_id=message.message_id)
         bot.register_next_step_handler(msg, process_broadcast_message)
         return

    target_count = len(active_users)
    markup = types.InlineKeyboardMarkup()
    markup.row(types.InlineKeyboardButton("✅ Confirm & Send", callback_data=f"confirm_broadcast_{message.message_id}"),
               types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast"))

    preview_text = broadcast_content[:1000].strip() if broadcast_content else "(Media message)"
    bot.reply_to(message, f"<b>⚠️ Confirm Broadcast:</b>\n\n<code>{preview_text}</code>\n\n" 
                          f"<b>To {target_count} users. Sure?</b>", reply_markup=markup, parse_mode='HTML')

def handle_confirm_broadcast(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    if user_id not in admin_ids: bot.answer_callback_query(call.id, "⚠️ Admin only.", show_alert=True); return
    try:
        original_message = call.message.reply_to_message
        if not original_message: raise ValueError("Could not retrieve original message.")

        # Check content type and get content
        broadcast_text = None
        broadcast_photo_id = None
        broadcast_video_id = None
        # Add other types as needed: document, sticker, voice, audio

        if original_message.text:
            broadcast_text = original_message.text
        elif original_message.photo:
            broadcast_photo_id = original_message.photo[-1].file_id # Get highest quality
        elif original_message.video:
            broadcast_video_id = original_message.video.file_id
        # Add more elif for other content types
        else:
            raise ValueError("Message has no text or supported media for broadcast.")

        bot.answer_callback_query(call.id, "🚀 Starting broadcast...")
        progress_msg = bot.edit_message_text(f"📢 Broadcasting to {len(active_users)} users...",
                              chat_id, call.message.message_id, reply_markup=None)
        # Pass all potential content types to execute_broadcast
        thread = threading.Thread(target=execute_broadcast, args=(
            broadcast_text, broadcast_photo_id, broadcast_video_id, 
            original_message.caption if (broadcast_photo_id or broadcast_video_id) else None, # Pass caption
            chat_id, call.message.message_id, original_message.message_id))
        thread.start()
    except ValueError as ve: 
        logger.error(f"Error retrieving msg for broadcast confirm: {ve}")
        bot.edit_message_text(f"❌ Error starting broadcast: {ve}", chat_id, call.message.message_id, reply_markup=None)
    except Exception as e:
        logger.error(f"Error in handle_confirm_broadcast: {e}", exc_info=True)
        bot.edit_message_text("❌ Unexpected error during broadcast confirm.", chat_id, call.message.message_id, reply_markup=None)

def handle_cancel_broadcast(call):
    bot.answer_callback_query(call.id, "Broadcast cancelled.")
    bot.delete_message(call.message.chat.id, call.message.message_id)
    # Optionally delete the original message too if call.message.reply_to_message exists
    if call.message.reply_to_message:
        try: bot.delete_message(call.message.chat.id, call.message.reply_to_message.message_id)
        except: pass


def execute_broadcast(broadcast_text, photo_id, video_id, caption, admin_chat_id, progress_message_id=None, original_message_id=None):
    sent_count = 0; failed_count = 0; blocked_count = 0
    start_exec_time = time.time() 
    users_to_broadcast = list(active_users); total_users = len(users_to_broadcast)
    logger.info(f"Executing broadcast to {total_users} users.")
    batch_size = 25; delay_batches = 1.5

    for i, user_id_bc in enumerate(users_to_broadcast): # Renamed
        try:
            if broadcast_text:
                bot.send_message(user_id_bc, broadcast_text, parse_mode='HTML')
            elif photo_id:
                bot.send_photo(user_id_bc, photo_id, caption=caption, parse_mode='HTML' if caption else None)
            elif video_id:
                bot.send_video(user_id_bc, video_id, caption=caption, parse_mode='HTML' if caption else None)
            # Add other send methods for other types
            sent_count += 1
        except telebot.apihelper.ApiTelegramException as e:
            err_desc = str(e).lower()
            if any(s in err_desc for s in ["bot was blocked", "user is deactivated", "chat not found", "kicked from", "restricted"]): 
                logger.warning(f"Broadcast failed to {user_id_bc}: User blocked/inactive.")
                blocked_count += 1
            elif "flood control" in err_desc or "too many requests" in err_desc:
                retry_after = 5; match = re.search(r"retry after (\d+)", err_desc)
                if match: retry_after = int(match.group(1)) + 1 
                logger.warning(f"Flood control. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                try: # Retry once
                    if broadcast_text: bot.send_message(user_id_bc, broadcast_text, parse_mode='HTML')
                    elif photo_id: bot.send_photo(user_id_bc, photo_id, caption=caption, parse_mode='HTML' if caption else None)
                    elif video_id: bot.send_video(user_id_bc, video_id, caption=caption, parse_mode='HTML' if caption else None)
                    sent_count += 1
                except Exception as e_retry: logger.error(f"Broadcast retry failed to {user_id_bc}: {e_retry}"); failed_count +=1
            else: logger.error(f"Broadcast failed to {user_id_bc}: {e}"); failed_count += 1
        except Exception as e: logger.error(f"Unexpected error broadcasting to {user_id_bc}: {e}"); failed_count += 1

        if (i + 1) % batch_size == 0 and i < total_users - 1:
            logger.info(f"Broadcast batch {i//batch_size + 1} sent. Sleeping {delay_batches}s...")
            time.sleep(delay_batches)
        elif i % 5 == 0: time.sleep(0.2) 

    duration = round(time.time() - start_exec_time, 2)
    result_msg = (f"<b>📢 Broadcast Complete!\n\n✅ Sent: {sent_count}\n❌ Failed: {failed_count}\n"
                  f"🚫 Blocked/Inactive: {blocked_count}\n👥 Targets: {total_users}\n⏱️ Duration: {duration}s</b>")
    logger.info(result_msg)
    
    try:
        # Delete the progress message
        if progress_message_id:
            bot.delete_message(admin_chat_id, progress_message_id)
            logger.info(f"Deleted broadcast progress message {progress_message_id}")
        
        # Reply to original message with summary
        if original_message_id:
            bot.send_message(admin_chat_id, result_msg, reply_to_message_id=original_message_id)
        else:
            bot.send_message(admin_chat_id, result_msg)
    except Exception as e: 
        logger.error(f"Failed to send broadcast result to admin {admin_chat_id}: {e}")
        # Fallback: send without reply if original message reply fails
        try: bot.send_message(admin_chat_id, result_msg)
        except Exception as e2: logger.error(f"Fallback broadcast result send failed: {e2}")

def admin_panel_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("👑 Admin Panel\nManage admins (Owner actions may be restricted).",
                              call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
    except Exception as e: logger.error(f"Error showing admin panel: {e}")

def add_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID to promote to Admin.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_admin_id)

def process_add_admin_id(message):
    owner_id_check = message.from_user.id 
    if owner_id_check != OWNER_ID: bot.reply_to(message, "<b>⚠️ Owner only.</b>", parse_mode='HTML'); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "<b>Admin promotion cancelled.</b>", parse_mode='HTML'); return
    try:
        new_admin_id = int(message.text.strip())
        if new_admin_id <= 0: raise ValueError("ID must be positive")
        if new_admin_id == OWNER_ID: bot.reply_to(message, "<b>⚠️ Owner is already Owner.</b>", parse_mode='HTML'); return
        if new_admin_id in admin_ids: bot.reply_to(message, f"<b>⚠️ User</b> <code>{new_admin_id}</code> <b>already Admin.</b>", parse_mode='HTML'); return
        add_admin_db(new_admin_id) 
        logger.warning(f"Admin {new_admin_id} added by Owner {owner_id_check}.")
        bot.reply_to(message, f"<b>✅ User</b> <code>{new_admin_id}</code> <b>promoted to Admin.</b>", parse_mode='HTML')
        try: bot.send_message(new_admin_id, "<b>🎉 Congrats! You are now an Admin.</b>", parse_mode='HTML')
        except Exception as e: logger.error(f"Failed to notify new admin {new_admin_id}: {e}")
    except ValueError:
        bot.reply_to(message, "<b>⚠️ Invalid ID. Send numerical ID or /cancel.</b>", parse_mode='HTML')
        msg = bot.reply_to(message, "<b>👑 Enter User ID to promote or /cancel.</b>", parse_mode='HTML')
        bot.register_next_step_handler(msg, process_add_admin_id)
    except Exception as e: logger.error(f"Error processing add admin: {e}", exc_info=True); bot.reply_to(message, "<b>Error.</b>", parse_mode='HTML')

def remove_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID of Admin to remove.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_admin_id)

def process_remove_admin_id(message):
    owner_id_check = message.from_user.id
    if owner_id_check != OWNER_ID: bot.reply_to(message, "⚠️ Owner only."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Admin removal cancelled."); return
    try:
        admin_id_remove = int(message.text.strip()) # Renamed
        if admin_id_remove <= 0: raise ValueError("ID must be positive")
        if admin_id_remove == OWNER_ID: bot.reply_to(message, "⚠️ Owner cannot remove self."); return
        if admin_id_remove not in admin_ids: bot.reply_to(message, f"⚠️ User `{admin_id_remove}` not Admin."); return
        if remove_admin_db(admin_id_remove): 
            logger.warning(f"Admin {admin_id_remove} removed by Owner {owner_id_check}.")
            bot.reply_to(message, f"✅ Admin `{admin_id_remove}` removed.")
            try: bot.send_message(admin_id_remove, "ℹ️ You are no longer an Admin.")
            except Exception as e: logger.error(f"Failed to notify removed admin {admin_id_remove}: {e}")
        else: bot.reply_to(message, f"❌ Failed to remove admin `{admin_id_remove}`. Check logs.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "👑 Enter Admin ID to remove or /cancel.")
        bot.register_next_step_handler(msg, process_remove_admin_id)
    except Exception as e: logger.error(f"Error processing remove admin: {e}", exc_info=True); bot.reply_to(message, "Error.")

def list_admins_callback(call):
    bot.answer_callback_query(call.id)
    try:
        admin_list_str = "\n".join(f"- `{aid}` {'(Owner)' if aid == OWNER_ID else ''}" for aid in sorted(list(admin_ids)))
        if not admin_list_str: admin_list_str = "(No Owner/Admins configured!)"
        bot.edit_message_text(f"👑 Current Admins:\n\n{admin_list_str}", call.message.chat.id,
                              call.message.message_id, reply_markup=create_admin_management_menu(), parse_mode='Markdown')
    except Exception as e: logger.error(f"Error listing admins: {e}")

def add_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID & days (e.g., `12345678 30`).\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_subscription_details)

def process_add_subscription_details(message):
    admin_id_check = message.from_user.id 
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Sub add cancelled."); return
    try:
        parts = message.text.split();
        if len(parts) != 2: raise ValueError("Incorrect format")
        sub_user_id = int(parts[0].strip()); days = int(parts[1].strip())
        if sub_user_id <= 0 or days <= 0: raise ValueError("User ID/days must be positive")

        current_expiry = user_subscriptions.get(sub_user_id, {}).get('expiry')
        start_date_new_sub = datetime.now() # Renamed
        if current_expiry and current_expiry > start_date_new_sub: start_date_new_sub = current_expiry
        new_expiry = start_date_new_sub + timedelta(days=days)
        save_subscription(sub_user_id, new_expiry)

        logger.info(f"Sub for {sub_user_id} by admin {admin_id_check}. Expiry: {new_expiry:%Y-%m-%d}")
        bot.reply_to(message, f"✅ Sub for `{sub_user_id}` by {days} days.\nNew expiry: {new_expiry:%Y-%m-%d}")
        try: bot.send_message(sub_user_id, f"🎉 Sub activated/extended by {days} days! Expires: {new_expiry:%Y-%m-%d}.")
        except Exception as e: logger.error(f"Failed to notify {sub_user_id} of new sub: {e}")
    except ValueError as e:
        bot.reply_to(message, f"⚠️ Invalid: {e}. Format: `ID days` or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID & days, or /cancel.")
        bot.register_next_step_handler(msg, process_add_subscription_details)
    except Exception as e: logger.error(f"Error processing add sub: {e}", exc_info=True); bot.reply_to(message, "Error.")

def remove_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to remove sub.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_subscription_id)

def process_remove_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Sub removal cancelled."); return
    try:
        sub_user_id_remove = int(message.text.strip()) # Renamed
        if sub_user_id_remove <= 0: raise ValueError("ID must be positive")
        if sub_user_id_remove not in user_subscriptions:
            bot.reply_to(message, f"⚠️ User `{sub_user_id_remove}` no active sub in memory."); return
        remove_subscription_db(sub_user_id_remove) 
        logger.warning(f"Sub removed for {sub_user_id_remove} by admin {admin_id_check}.")
        bot.reply_to(message, f"✅ Sub for `{sub_user_id_remove}` removed.")
        try: bot.send_message(sub_user_id_remove, "ℹ️ Your subscription removed by admin.")
        except Exception as e: logger.error(f"Failed to notify {sub_user_id_remove} of sub removal: {e}")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to remove sub from, or /cancel.")
        bot.register_next_step_handler(msg, process_remove_subscription_id)
    except Exception as e: logger.error(f"Error processing remove sub: {e}", exc_info=True); bot.reply_to(message, "Error.")

def check_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to check sub.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_check_subscription_id)

def list_all_files_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "📁 Enter User ID to show their files.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_list_user_files)



def process_check_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "Sub check cancelled."); return
    try:
        sub_user_id_check = int(message.text.strip()) # Renamed
        if sub_user_id_check <= 0: raise ValueError("ID must be positive")
        if sub_user_id_check in user_subscriptions:
            expiry_dt = user_subscriptions[sub_user_id_check].get('expiry')
            if expiry_dt:
                if expiry_dt > datetime.now():
                    days_left = (expiry_dt - datetime.now()).days
                    bot.reply_to(message, f"✅ User `{sub_user_id_check}` active sub.\nExpires: {expiry_dt:%Y-%m-%d %H:%M:%S} ({days_left} days left).")
                else:
                    bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` expired sub (On: {expiry_dt:%Y-%m-%d %H:%M:%S}).")
                    remove_subscription_db(sub_user_id_check) # Clean up
            else: bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` in sub list, but expiry missing. Re-add if needed.")
        else: bot.reply_to(message, f"ℹ️ User `{sub_user_id_check}` no active sub record.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to check, or /cancel.")
        bot.register_next_step_handler(msg, process_check_subscription_id)
    except Exception as e: logger.error(f"Error processing check sub: {e}", exc_info=True); bot.reply_to(message, "Error.")

def process_list_user_files(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: bot.reply_to(message, "⚠️ Not authorized."); return
    if message.text.lower() == '/cancel': bot.reply_to(message, "List files cancelled."); return
    try:
        target_user_id = int(message.text.strip())
        if target_user_id <= 0: raise ValueError("ID must be positive")
        
        # Check if user exists in our system (has uploaded files or is in active users)
        if target_user_id not in active_users and target_user_id not in user_files:
            bot.reply_to(message, f"⚠️ User `{target_user_id}` not found in bot database.\nUser must have interacted with bot first.")
            return
        
        # Get user's files
        user_files_list = user_files.get(target_user_id, [])
        
        if not user_files_list:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Back to Admin Panel", callback_data='admin_panel'))
            bot.reply_to(message, f"📁 Files for User `{target_user_id}`:\n\n(No files uploaded yet)", 
                        reply_markup=markup, parse_mode='Markdown')
            return
        
        # Create buttons for each file with full controls
        markup = types.InlineKeyboardMarkup(row_width=1)
        
        for file_name, file_type in sorted(user_files_list):
            is_running = is_bot_running(target_user_id, file_name)
            status_icon = "🟢 Running" if is_running else "🔴 Stopped"
            btn_text = f"{file_name} ({file_type}) - {status_icon}"
            # Use the same callback pattern as user's own file management
            markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{target_user_id}_{file_name}'))
        
        markup.add(types.InlineKeyboardButton("🔙 Back to Admin Panel", callback_data='admin_panel'))
        
        # Get user status for display
        user_status = ""
        if target_user_id == OWNER_ID:
            user_status = "👑 Owner"
        elif target_user_id in admin_ids:
            user_status = "🛡️ Admin"
        elif target_user_id in user_subscriptions and user_subscriptions[target_user_id].get('expiry', datetime.min) > datetime.now():
            user_status = "⭐ Premium"
        else:
            user_status = "🆓 Free User"
        
        file_count = len(user_files_list)
        bot.reply_to(message, 
                    f"📁 Files for User `{target_user_id}` ({user_status}):\n\n"
                    f"📊 Total Files: {file_count}\n\n"
                    f"Click any file to manage it with full controls (Start/Stop/Delete/Logs):",
                    reply_markup=markup, parse_mode='Markdown')
        
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid User ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "📁 Enter User ID to show their files, or /cancel.")
        bot.register_next_step_handler(msg, process_list_user_files)
    except Exception as e: 
        logger.error(f"Error processing list user files: {e}", exc_info=True)
        bot.reply_to(message, "❌ Error retrieving user files.")

def ban_user_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "<b>🚫 Send ban command:</b>\n<code>/ban {user_id} {reason}</code>\n<b>/cancel to abort.</b>", parse_mode='HTML')
    bot.register_next_step_handler(msg, process_ban_user)

def process_ban_user(message):
    admin_id = message.from_user.id
    if admin_id not in admin_ids: 
        bot.reply_to(message, "<b>⚠️ Not authorized.</b>", parse_mode='HTML')
        return
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "<b>Ban cancelled.</b>", parse_mode='HTML')
        return
    
    try:
        if not message.text.startswith('/ban '):
            raise ValueError("Invalid format")
        
        parts = message.text[5:].split(' ', 1)  # Remove '/ban ' and split into max 2 parts
        if len(parts) < 2:
            raise ValueError("Missing reason")
        
        user_id_to_ban = int(parts[0].strip())
        reason = parts[1].strip()
        
        if user_id_to_ban <= 0:
            raise ValueError("Invalid user ID")
        
        if user_id_to_ban == OWNER_ID:
            bot.reply_to(message, "<b>⚠️ Cannot ban the owner!</b>", parse_mode='HTML')
            return
        
        if user_id_to_ban in admin_ids:
            bot.reply_to(message, "<b>⚠️ Cannot ban an admin!</b>", parse_mode='HTML')
            return
        
        if is_user_banned(user_id_to_ban):
            bot.reply_to(message, f"<b>⚠️ User <code>{user_id_to_ban}</code> is already banned!</b>", parse_mode='HTML')
            return
        
        ban_user_db(user_id_to_ban, reason, admin_id)
        logger.warning(f"User {user_id_to_ban} banned by admin {admin_id} with reason: {reason}")
        
        bot.reply_to(message, f"<b>✅ User <code>{user_id_to_ban}</code> has been banned!</b>\n<b>Reason:</b> {reason}", parse_mode='HTML')
        
        # Try to notify the banned user
        try:
            bot.send_message(user_id_to_ban, f"<b>🚫 You have been banned from using this bot.</b>\n<b>Reason:</b> {reason}", parse_mode='HTML')
        except Exception as e:
            logger.error(f"Failed to notify banned user {user_id_to_ban}: {e}")
    
    except ValueError as e:
        bot.reply_to(message, f"<b>⚠️ Invalid format!</b>\nUse: <code>/ban {{user_id}} {{reason}}</code>\n<b>/cancel to abort.</b>", parse_mode='HTML')
        msg = bot.send_message(message.chat.id, "<b>🚫 Send ban command or /cancel:</b>", parse_mode='HTML')
        bot.register_next_step_handler(msg, process_ban_user)
    except Exception as e:
        logger.error(f"Error processing ban: {e}", exc_info=True)
        bot.reply_to(message, "<b>❌ Error processing ban.</b>", parse_mode='HTML')

def unban_user_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "<b>✅ Enter User ID to unban:</b>\n<b>/cancel to abort.</b>", parse_mode='HTML')
    bot.register_next_step_handler(msg, process_unban_user)

def process_unban_user(message):
    admin_id = message.from_user.id
    if admin_id not in admin_ids: 
        bot.reply_to(message, "<b>⚠️ Not authorized.</b>", parse_mode='HTML')
        return
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "<b>Unban cancelled.</b>", parse_mode='HTML')
        return
    
    try:
        user_id_to_unban = int(message.text.strip())
        if user_id_to_unban <= 0:
            raise ValueError("Invalid user ID")
        
        if not is_user_banned(user_id_to_unban):
            bot.reply_to(message, f"<b>⚠️ User <code>{user_id_to_unban}</code> is not banned!</b>", parse_mode='HTML')
            return
        
        if unban_user_db(user_id_to_unban):
            logger.warning(f"User {user_id_to_unban} unbanned by admin {admin_id}")
            bot.reply_to(message, f"<b>✅ User <code>{user_id_to_unban}</code> has been unbanned!</b>", parse_mode='HTML')
            
            # Try to notify the unbanned user
            try:
                bot.send_message(user_id_to_unban, "<b>✅ You have been unbanned! You can now use the bot again.</b>", parse_mode='HTML')
            except Exception as e:
                logger.error(f"Failed to notify unbanned user {user_id_to_unban}: {e}")
        else:
            bot.reply_to(message, f"<b>❌ Failed to unban user <code>{user_id_to_unban}</code>. Check logs.</b>", parse_mode='HTML')
    
    except ValueError:
        bot.reply_to(message, "<b>⚠️ Invalid User ID. Send numerical ID or /cancel.</b>", parse_mode='HTML')
        msg = bot.send_message(message.chat.id, "<b>✅ Enter User ID to unban or /cancel:</b>", parse_mode='HTML')
        bot.register_next_step_handler(msg, process_unban_user)
    except Exception as e:
        logger.error(f"Error processing unban: {e}", exc_info=True)
        bot.reply_to(message, "<b>❌ Error processing unban.</b>", parse_mode='HTML')

def ban_unban_menu_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("<b>🚫 Ban/Unban Management</b>\n<b>Select action:</b>",
                              call.message.chat.id, call.message.message_id, 
                              reply_markup=create_ban_unban_menu(), parse_mode='HTML')
    except Exception as e: 
        logger.error(f"Error showing ban/unban menu: {e}")

def list_banned_users_callback(call):
    bot.answer_callback_query(call.id)
    try:
        if not banned_users:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Back to Ban/Unban Menu", callback_data='ban_unban_menu'))
            bot.edit_message_text("📋 Banned Users:\n\n(No users are currently banned)", 
                                call.message.chat.id, call.message.message_id, 
                                reply_markup=markup)
            return
        
        banned_list = []
        for user_id, ban_info in banned_users.items():
            ban_date = ban_info['ban_date'][:10]  # Get only date part
            banned_list.append(f"• <code>{user_id}</code> - {ban_info['reason']}\n  <i>Banned on: {ban_date}</i>")
        
        banned_text = "\n\n".join(banned_list)
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back to Ban/Unban Menu", callback_data='ban_unban_menu'))
        
        bot.edit_message_text(f"<b>📋 Banned Users ({len(banned_users)}):</b>\n\n{banned_text}", 
                            call.message.chat.id, call.message.message_id, 
                            reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error listing banned users: {e}", exc_info=True)

def admin_management_menu_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("<b>👥 Admin Management</b>\n<b>Select action:</b>",
                              call.message.chat.id, call.message.message_id, 
                              reply_markup=create_admin_management_menu(), parse_mode='HTML')
    except Exception as e: 
        logger.error(f"Error showing admin management menu: {e}")

def direct_message_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, 
                          "<b>💬 Send Direct Message:</b>\n"
                          "<b>Format:</b> <code>{user_id} {message}</code>\n"
                          "<b>Example:</b> <code>123456789 Hello from owner!</code>\n"
                          "<b>/cancel to abort.</b>", parse_mode='HTML')
    bot.register_next_step_handler(msg, process_direct_message)

def process_direct_message(message):
    admin_id = message.from_user.id
    if admin_id not in admin_ids: 
        bot.reply_to(message, "<b>⚠️ Not authorized.</b>", parse_mode='HTML')
        return
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "<b>Direct message cancelled.</b>", parse_mode='HTML')
        return
    
    try:
        parts = message.text.split(' ', 1)  # Split into max 2 parts
        if len(parts) < 2:
            raise ValueError("Missing message content")
        
        target_user_id = int(parts[0].strip())
        msg_content = parts[1].strip()
        
        if target_user_id <= 0:
            raise ValueError("Invalid user ID")
        
        if not msg_content:
            raise ValueError("Message cannot be empty")
        
        # Get admin info for the message
        admin_name = message.from_user.first_name or "Admin"
        admin_username = message.from_user.username or "N/A"
        sender_title = "Owner" if admin_id == OWNER_ID else "Admin"
        
        # Escape HTML entities in the message content to prevent parsing errors
        import html
        escaped_msg_content = html.escape(msg_content)
        escaped_admin_name = html.escape(admin_name)
        escaped_admin_username = html.escape(admin_username)
        
        # Format the message with sender info
        formatted_message = (
            f"<b>💬 Message from {sender_title}:</b>\n\n"
            f"{escaped_msg_content}\n\n"
        )
        
        # Send the message
        try:
            bot.send_message(target_user_id, formatted_message, parse_mode='HTML')
            bot.reply_to(message, 
                        f"<b>✅ Message sent successfully!</b>\n"
                        f"<b>To:</b> <code>{target_user_id}</code>\n"
                        f"<b>Message:</b> {msg_content[:100]}{'...' if len(msg_content) > 100 else ''}", 
                        parse_mode='HTML')
            logger.info(f"Direct message sent by {sender_title} {admin_id} to user {target_user_id}: {msg_content[:50]}...")
        except telebot.apihelper.ApiTelegramException as e:
            if "bot was blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
                bot.reply_to(message, 
                            f"<b>⚠️ Failed to send message!</b>\n"
                            f"<b>User <code>{target_user_id}</code> has blocked the bot or account is deactivated.</b>", 
                            parse_mode='HTML')
            elif "chat not found" in str(e).lower():
                bot.reply_to(message, 
                            f"<b>⚠️ Failed to send message!</b>\n"
                            f"<b>User <code>{target_user_id}</code> not found.</b>", 
                            parse_mode='HTML')
            else:
                bot.reply_to(message, 
                            f"<b>❌ Failed to send message!</b>\n"
                            f"<b>Error:</b> {str(e)}", 
                            parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error sending direct message to {target_user_id}: {e}", exc_info=True)
            bot.reply_to(message, f"<b>❌ Error sending message: {str(e)}</b>", parse_mode='HTML')
    
    except ValueError as e:
        bot.reply_to(message, 
                    f"<b>⚠️ Invalid format!</b>\n"
                    f"<b>Use:</b> <code>{{user_id}} {{message}}</code>\n"
                    f"<b>Error:</b> {str(e)}\n"
                    f"<b>/cancel to abort.</b>", parse_mode='HTML')
        msg = bot.send_message(message.chat.id, "<b>💬 Send direct message or /cancel:</b>", parse_mode='HTML')
        bot.register_next_step_handler(msg, process_direct_message)
    except Exception as e:
        logger.error(f"Error processing direct message: {e}", exc_info=True)
        bot.reply_to(message, "<b>❌ Error processing direct message.</b>", parse_mode='HTML')

# --- End Callback Query Handlers ---

# --- Cleanup Function ---
def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys()) 
    if not script_keys_to_stop: logger.info("No scripts running. Exiting."); return
    logger.info(f"Stopping {len(script_keys_to_stop)} scripts...")
    for key in script_keys_to_stop:
        if key in bot_scripts: logger.info(f"Stopping: {key}"); kill_process_tree(bot_scripts[key])
        else: logger.info(f"Script {key} already removed.")
    logger.warning("Cleanup finished.")
atexit.register(cleanup)

# --- Main Execution ---
if __name__ == '__main__':
    logger.info("="*40 + "\n🤖 Bot Starting Up...\n" + f"🐍 Python: {sys.version.split()[0]}\n" +
                f"🔧 Base Dir: {BASE_DIR}\n📁 Upload Dir: {UPLOAD_BOTS_DIR}\n" +
                f"📊 Data Dir: {IROTECH_DIR}\n🔑 Owner ID: {OWNER_ID}\n🛡️ Admins: {admin_ids}\n" + "="*40)
    keep_alive()
    logger.info("🚀 Starting polling...")
    while True:
        try:
            bot.infinity_polling(logger_level=logging.INFO, timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout: logger.warning("Polling ReadTimeout. Restarting in 5s..."); time.sleep(5)
        except requests.exceptions.ConnectionError as ce: logger.error(f"Polling ConnectionError: {ce}. Retrying in 15s..."); time.sleep(15)
        except Exception as e:
            logger.critical(f"💥 Unrecoverable polling error: {e}", exc_info=True)
      
