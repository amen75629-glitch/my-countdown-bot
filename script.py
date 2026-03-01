import requests
import time
from datetime import datetime
import pytz
import threading

# ================== الإعدادات ==================
TOKEN = "5226566895:AAEDL56hfHYJRrf2_0wHOlLyUG2XL_rCM5U"
CHANNEL = "@ZERO7097"
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
utc = pytz.utc

user_states = {}

# ================== دالة عامة لطلب API ==================

def telegram_request(method, data=None, params=None):
    url = f"{BASE_URL}/{method}"
    try:
        response = requests.post(url, data=data, params=params, timeout=15)
        return response.json()
    except requests.RequestException as e:
        print(f"Telegram API error: {e}")
        return None

# ================== دوال المراسلة ==================

def send_message(chat_id, text):
    res = telegram_request("sendMessage", data={
        "chat_id": chat_id,
        "text": text
    })
    if res and res.get("ok"):
        return res["result"]["message_id"]
    return None


def edit_message(chat_id, message_id, text):
    telegram_request("editMessageText", data={
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text
    })


def check_membership(user_id):
    res = telegram_request("getChatMember", params={
        "chat_id": CHANNEL,
        "user_id": user_id
    })

    if res and res.get("ok"):
        status = res["result"]["status"]
        return status in ["member", "administrator", "creator"]
    return False


# ================== التحقق من التاريخ ==================

def parse_datetime(text):
    try:
        dt = datetime.strptime(text, "%Y-%m-%d %H:%M")
        if 2023 <= dt.year <= 2100:
            return utc.localize(dt)
    except ValueError:
        pass
    return None


# ================== العد التنازلي ==================

def countdown_worker(chat_id, end_time):
    message_id = send_message(chat_id, "⏳ بدأ العد التنازلي...")
    if not message_id:
        return

    while True:
        now = datetime.now(utc)
        remaining = end_time - now

        if remaining.total_seconds() <= 0:
            edit_message(chat_id, message_id, "🔔 انتهى الوقت المحدد!")
            break

        days = remaining.days
        hours, remainder = divmod(remaining.seconds, 3600)
        minutes, _ = divmod(remainder, 60)

        text = f"⏳ المتبقي:\n{days} يوم\n{hours:02}:{minutes:02}"
        edit_message(chat_id, message_id, text)

        time.sleep(60)


# ================== معالجة الرسائل ==================

def handle_message(msg):
    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    text = msg.get("text", "").strip()

    if not text:
        return

    if not check_membership(user_id):
        send_message(chat_id, f"⚠️ يجب الاشتراك في {CHANNEL} أولاً.")
        return

    state = user_states.get(chat_id, {}).get("step")

    if text in ("/start", "/count"):
        user_states[chat_id] = {"step": "waiting_start"}
        send_message(chat_id, "📅 أرسل تاريخ البداية (YYYY-MM-DD HH:MM)")
        return

    if state == "waiting_start":
        start_dt = parse_datetime(text)
        if not start_dt:
            send_message(chat_id, "❌ تنسيق غير صحيح.")
            return

        user_states[chat_id] = {
            "step": "waiting_end",
            "start_time": start_dt
        }
        send_message(chat_id, "✅ الآن أرسل تاريخ النهاية بنفس الصيغة.")
        return

    if state == "waiting_end":
        end_dt = parse_datetime(text)
        if not end_dt:
            send_message(chat_id, "❌ تنسيق غير صحيح.")
            return

        start_dt = user_states[chat_id]["start_time"]

        if end_dt <= start_dt:
            send_message(chat_id, "⚠️ النهاية يجب أن تكون بعد البداية.")
            return

        user_states.pop(chat_id, None)

        threading.Thread(
            target=countdown_worker,
            args=(chat_id, end_dt),
            daemon=True
        ).start()

        # ================== تشغيل البوت ==================

        def start_bot():
            print("🤖 البوت يعمل...")
            offset = None

            while True:
                try:
                    params = {"timeout": 20}
                    if offset is not None:
                        params["offset"] = offset

                    res = requests.get(f"{BASE_URL}/getUpdates", params=params, timeout=30).json()

                    if not res.get("ok"):
                        continue

                    for update in res.get("result", []):
                        offset = update["update_id"] + 1

                        message = update.get("message")
                        if message:
                            handle_message(message)

                except Exception as e:
                    print("Network error:", e)
                    time.sleep(5)

        if name == "main":
            start_bot()