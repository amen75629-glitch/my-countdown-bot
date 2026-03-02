import sqlite3
import requests
import time
from datetime import datetime
import pytz
import threading

# ================== 1. الإعدادات المستقرة ==================
TOKEN = "8496382800:AAH6051l8WnJgNfJfUmOlzpDw1sROXKTAvs" 
CHANNEL_ID = "@ZERO7097"
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
utc = pytz.utc

user_states = {}
COLORS = ["⚫", "🔵", "🟣", "🟤"]

# ================== 2. قاموس اللغات ==================
TEXTS = {
    "ar": {
        "sub_req": "⚠️ يرجى الاشتراك أولاً في القناة: {}",
        "welcome": "<b>مرحباً بك!</b>\nهذا البوت يساعدك على إنشاء عدادات دقيقة.\nاستخدم القائمة أدناه:",
        "btn_create": "➕ إنشاء وقت جديد",
        "btn_view": "📊 أوقاتي",
        "btn_lang": "🌐 اللغة / Language",
        "btn_pin": "📌 تثبيت العداد",
        "send_end": "🗓️ أرسل تاريخ النهاية (مثال: <code>2026-12-30 18:00</code>):",
        "pin_msg": "📌 لتثبيت العداد، اضغط مطولاً على رسالته واختر 'تثبيت' (Pin).",
        "format_err": "❌ التنسيق خطأ! أعد المحاولة: <code>2026-05-01 14:00</code>",
        "saved_end": "✅ وقت النهاية محفوظ.\n📝 الآن أرسل <b>اسماً</b> لهذا العداد (مثال: عطلتي):",
        "starting": "🚀 جاري التشغيل...",
        "time_up": "<b>🔔 انتهى الوقت:</b> {}",
        "active_timer": "{} <b>العداد النشط: {}</b>\n━━━━━━━━━━━━━━\n📅 المستهدف: <code>{}</code>\n━━━━━━━━━━━━━━\n⏳ المتبقي:\n┕ {} يوم و {} ساعة و {} دقيقة\n━━━━━━━━━━━━━━\n<i>✨ يتحدث تلقائياً..</i>",
        "no_timers": "📭 ليس لديك أي عدادات نشطة حالياً.",
        "your_timers": "📊 <b>عداداتك النشطة:</b>\n\n{}",
        "lang_updated": "✅ تم تغيير اللغة بنجاح!\nأرسل /start لتحديث القائمة."
    },
    "en": {
        "sub_req": "⚠️ Please subscribe to the channel first: {}",
        "welcome": "<b>Welcome!</b>\nThis bot helps you create precise countdown timers.\nUse the menu below:",
        "btn_create": "➕ Create New Timer",
        "btn_view": "📊 My Timers",
        "btn_lang": "🌐 Language / اللغة",
        "btn_pin": "📌 Pin Timer",
        "send_end": "🗓️ Send the end date (e.g., <code>2026-12-30 18:00</code>):",
        "pin_msg": "📌 To pin the timer, long-press the message and select 'Pin'.",
        "format_err": "❌ Invalid format! Try again: <code>2026-05-01 14:00</code>",
        "saved_end": "✅ End time saved.\n📝 Now send a <b>name</b> for this timer (e.g., My Vacation 🌸):",
        "starting": "🚀 Starting...",
        "time_up": "<b>🔔 Time is up:</b> {}",
        "active_timer": "{} <b>Active Timer: {}</b>\n━━━━━━━━━━━━━━\n📅 Target: <code>{}</code>\n━━━━━━━━━━━━━━\n⏳ Remaining:\n┕ {} Days, {} Hours, and {} Minutes\n━━━━━━━━━━━━━━\n<i>✨ Auto-updating..</i>",
        "no_timers": "📭 You don't have any active timers currently.",
        "your_timers": "📊 <b>Your Active Timers:</b>\n\n{}",
        "lang_updated": "✅ Language changed successfully!\nSend /start to update the menu."
    }
}

# ================== 3. قاعدة بيانات محسنة ==================
def init_db():
    with sqlite3.connect("bot_database.db") as conn:
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, lang TEXT DEFAULT "ar")')
        cursor.execute('CREATE TABLE IF NOT EXISTS timers (chat_id INTEGER, title TEXT, end_time TEXT, msg_id INTEGER PRIMARY KEY)')
        
        # تحديث قاعدة البيانات القديمة إن وجدت لإضافة عمود اللغة
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN lang TEXT DEFAULT "ar"')
        except sqlite3.OperationalError:
            pass # العمود موجود مسبقاً
        conn.commit()

def db_action(query, params=()):
    try:
        with sqlite3.connect("bot_database.db", check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            res = cursor.fetchall()
            conn.commit()
            return res
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return []

def get_user_lang(user_id):
    res = db_action("SELECT lang FROM users WHERE user_id = ?", (user_id,))
    return res[0][0] if res else "ar"

# ================== 4. وظائف الـ API ==================
def bot_api(method, data=None):
    try:
        res = requests.post(f"{BASE_URL}/{method}", json=data, timeout=15).json()
        if not res.get("ok") and "message is not modified" not in res.get('description', ''):
            print(f"⚠️ Telegram API Warning: {res.get('description')}")
        return res
    except Exception as e:
        return None

def check_sub(user_id):
    res = bot_api("getChatMember", {"chat_id": CHANNEL_ID, "user_id": user_id})
    return res["result"]["status"] in ["member", "administrator", "creator"] if res and res.get("ok") else False

# ================== 5. المحرك الذكي للعداد ==================
def countdown_worker(chat_id, end_time_str, title, mid):
    try:
        end_dt = utc.localize(datetime.strptime(end_time_str, "%Y-%m-%d %H:%M"))
    except: return

    color_idx = 0
    while True:
        lang = get_user_lang(chat_id) # جلب لغة المستخدم باستمرار
        now = datetime.now(utc)
        diff = end_dt - now
        
        if diff.total_seconds() <= 0:
            msg = TEXTS[lang]["time_up"].format(title)
            bot_api("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": msg, "parse_mode": "HTML"})
            db_action("DELETE FROM timers WHERE msg_id = ?", (mid,))
            break
        
        days = diff.days
        hours, rem = divmod(diff.seconds, 3600)
        minutes, _ = divmod(rem, 60)
        
        current_color = COLORS[color_idx]
        color_idx = (color_idx + 1) % len(COLORS)
        target_v = end_dt.strftime("%Y-%m-%d %H:%M")

        text = TEXTS[lang]["active_timer"].format(current_color, title, target_v, days, hours, minutes)
        
        bot_api("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": text, "parse_mode": "HTML"})
        time.sleep(60)

# ================== 6. نظام الاستعادة ==================
def restore_timers():
    timers = db_action("SELECT chat_id, end_time, title, msg_id FROM timers")
    for t in timers:
        threading.Thread(target=countdown_worker, args=(t[0], t[1], t[2], t[3]), daemon=True).start()

# ================== 7. المعالج الرئيسي ==================
def handle_update(upd):
    if "message" in upd:
        m = upd["message"]
        cid = m["chat"]["id"]
        uid = m["from"]["id"]
        text = m.get("text", "").strip()
        
        db_action("INSERT OR IGNORE INTO users (user_id, lang) VALUES (?, 'ar')", (uid,))
        lang = get_user_lang(uid)

        if not check_sub(uid):
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["sub_req"].format(CHANNEL_ID)})
            return

        if text == "/start":
            kb = {"inline_keyboard": [
                [{"text": TEXTS[lang]["btn_create"], "callback_data": "create"}],
                [{"text": TEXTS[lang]["btn_view"], "callback_data": "view"}, {"text": TEXTS[lang]["btn_lang"], "callback_data": "lang"}],
                [{"text": TEXTS[lang]["btn_pin"], "callback_data": "pin"}]
            ]}
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["welcome"], "reply_markup": kb, "parse_mode": "HTML"})
            return

        state = user_states.get(cid)
        
        if state and state["step"] == "wait_end":
            try:
                datetime.strptime(text, "%Y-%m-%d %H:%M")
                user_states[cid] = {"end": text, "step": "wait_name"}
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["saved_end"], "parse_mode": "HTML"})
            except ValueError:
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["format_err"], "parse_mode": "HTML"})
        
        elif state and state["step"] == "wait_name":
            title = text
            end_t = state["end"]
            res = bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["starting"]})
            if res and res.get("ok"):
                mid = res["result"]["message_id"]
                db_action("INSERT INTO timers VALUES (?, ?, ?, ?)", (cid, title, end_t, mid))
                user_states.pop(cid, None)
                threading.Thread(target=countdown_worker, args=(cid, end_t, title, mid), daemon=True).start()

    elif "callback_query" in upd:
        cq = upd["callback_query"]
        data = cq["data"]
        cid = cq["message"]["chat"]["id"]
        uid = cq["from"]["id"]
        lang = get_user_lang(uid)
        
        bot_api("answerCallbackQuery", {"callback_query_id": cq["id"]})

        if data == "create":
            user_states[cid] = {"step": "wait_end"}
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["send_end"], "parse_mode": "HTML"})
            
        elif data == "pin":
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["pin_msg"]})
            
        elif data == "view":
            user_timers = db_action("SELECT title, end_time FROM timers WHERE chat_id = ?", (cid,))
            if not user_timers:
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["no_timers"], "parse_mode": "HTML"})
            else:
                msg_content = ""
                for i, t in enumerate(user_timers, 1):
                    msg_content += f"{i}. <b>{t[0]}</b> ⏳ <code>{t[1]}</code>\n"
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["your_timers"].format(msg_content), "parse_mode": "HTML"})
                
        elif data == "lang":
            kb = {"inline_keyboard": [
                [{"text": "العربية 🇮🇶", "callback_data": "set_ar"}, {"text": "English 🇬🇧", "callback_data": "set_en"}]
            ]}
            bot_api("sendMessage", {"chat_id": cid, "text": "اختر لغتك / Choose your language:", "reply_markup": kb})
            
        elif data.startswith("set_"):
            new_lang = data.split("_")[1]
            db_action("UPDATE users SET lang = ? WHERE user_id = ?", (new_lang, uid))
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[new_lang]["lang_updated"]})

# ================== 8. نقطة الانطلاق ==================
if __name__ == "__main__":
    init_db()
    restore_timers()
    print("✅ البوت انطلق بنجاح مع دعم اللغات وزر 'أوقاتي'...")
    offset = None
    while True:
        try:
            upds = bot_api("getUpdates", {"offset": offset, "timeout": 20})
            if upds and upds.get("ok"):
                for u in upds["result"]:
                    offset = u["update_id"] + 1
                    handle_update(u)
        except Exception as e:
            time.sleep(5)

