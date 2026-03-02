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

# ================== 2. قاموس اللغات (منقح) ==================
TEXTS = {
    "ar": {
        "sub_req": "⚠️ يرجى الاشتراك أولاً في القناة: {}",
        "welcome": "<b>مرحباً بك!</b>\nهذا البوت يساعدك على إنشاء عدادات دقيقة.\nاستخدم القائمة أدناه:",
        "btn_create": "➕ إنشاء وقت جديد",
        "btn_view": "📊 أوقاتي",
        "btn_lang": "🌐 اللغة / Language",
        "btn_pin": "📌 تثبيت العداد",
        "send_start": "🗓️ أرسل <b>تاريخ البداية</b>\nمثال: <code>2024-01-01 00:00</code>",
        "send_end": "🗓️ أرسل <b>تاريخ النهاية</b>\nمثال: <code>2026-12-30 18:00</code>",
        "saved_start": "✅ تم حفظ تاريخ البداية.",
        "saved_end": "✅ تم حفظ تاريخ النهاية.\n📝 أرسل الآن <b>اسماً</b> للعداد:",
        "format_err": "❌ التنسيق خطأ! حاول مجدداً بنفس المثال.",
        "starting": "🚀 جاري تشغيل العداد...",
        "time_up": "<b>🔔 انتهى الوقت:</b> {}",
        "no_timers": "📭 لا توجد عدادات نشطة.",
        "your_timers": "📊 <b>عداداتك النشطة:</b>\n\n{}",
        "lang_updated": "✅ تم تغيير اللغة!"
    },
    "en": {
        "sub_req": "⚠️ Please subscribe first: {}",
        "welcome": "<b>Welcome!</b>\nCreate precise countdowns easily.",
        "btn_create": "➕ New Timer",
        "btn_view": "📊 My Timers",
        "btn_lang": "🌐 Language",
        "btn_pin": "📌 Pin",
        "send_start": "🗓️ Send <b>Start Date</b>\nEx: <code>2024-01-01 00:00</code>",
        "send_end": "🗓️ Send <b>End Date</b>\nEx: <code>2026-12-30 18:00</code>",
        "saved_start": "✅ Start time saved.",
        "saved_end": "✅ End time saved.\n📝 Now send a <b>name</b>:",
        "format_err": "❌ Invalid format! Try again.",
        "starting": "🚀 Starting...",
        "time_up": "<b>🔔 Time is up:</b> {}",
        "no_timers": "📭 No active timers.",
        "your_timers": "📊 <b>Your Timers:</b>\n\n{}",
        "lang_updated": "✅ Language updated!"
    }
}


# ================== 3. قاعدة البيانات ==================
def init_db():
    with sqlite3.connect("bot_database.db") as conn:
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, lang TEXT DEFAULT "ar")')
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS timers (chat_id INTEGER, title TEXT, end_time TEXT, msg_id INTEGER PRIMARY KEY)')
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
        print(f"❌ DB Error: {e}")
        return []


def get_user_lang(user_id):
    res = db_action("SELECT lang FROM users WHERE user_id = ?", (user_id,))
    return res[0][0] if res else "ar"


# ================== 4. وظائف الـ API ==================
def bot_api(method, data=None):
    try:
        return requests.post(f"{BASE_URL}/{method}", json=data, timeout=15).json()
    except:
        return None


def check_sub(user_id):
    res = bot_api("getChatMember", {"chat_id": CHANNEL_ID, "user_id": user_id})
    return res["result"]["status"] in ["member", "administrator", "creator"] if res and res.get("ok") else False


# ================== 5. المحرك مع شريط التقدم ==================
def countdown_worker(chat_id, end_time_str, title, mid, start_time_str=None):
    try:
        end_dt = utc.localize(datetime.strptime(end_time_str, "%Y-%m-%d %H:%M"))
        start_dt = utc.localize(
            datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")) if start_time_str else datetime.now(utc)
    except:
        return

    color_idx = 0
    while True:
        lang = get_user_lang(chat_id)
        now = datetime.now(utc)
        diff = end_dt - now

        if diff.total_seconds() <= 0:
            bot_api("editMessageText",
                    {"chat_id": chat_id, "message_id": mid, "text": TEXTS[lang]["time_up"].format(title),
                     "parse_mode": "HTML"})
            db_action("DELETE FROM timers WHERE msg_id = ?", (mid,))
            break

        # حساب شريط التقدم
        # حساب إجمالي المدة بالثواني
        total = (end_dt - start_dt).total_seconds()

        # صمام الأمان: التحقق من أن total ليس صفراً لمنع ZeroDivisionError
        if total <= 0:
            percent = 100  # إذا كان الوقت منتهياً أو التواريخ متطابقة
        else:
            done = (now - start_dt).total_seconds()
            # الآن القسمة آمنة لأننا تأكدنا أن total > 0
            percent = max(0, min(100, (done / total >=0) * 100))

        # رسم الشريط بناءً على النسبة المحسوبة بأمان
        blocks = int(percent / 10)
        bar = "▓" * blocks + "░" * (10 - blocks)

        # التوقيت المتبقي
        days = diff.days
        hours, rem = divmod(diff.seconds, 3600)
        minutes, _ = divmod(rem, 60)

        c = COLORS[color_idx]
        color_idx = (color_idx + 1) % len(COLORS)

        msg_text = (
            f"{c} <b>{title}</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🎯 الهدف: <code>{end_time_str}</code>\n"
            f"📊 التقدم: <code>{bar}</code> {percent:.1f}%\n"
            f"━━━━━━━━━━━━━━\n"
            f"⏳ المتبقي:\n"
            f"┕ {days} يوم و {hours} ساعة و {minutes} دقيقة\n"
            f"━━━━━━━━━━━━━━\n"
            f"<i>✨ يتحدث تلقائياً..</i>"
        )

        bot_api("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": msg_text, "parse_mode": "HTML"})
        time.sleep(60)


# ================== 6. المعالج الرئيسي ==================
def handle_update(upd):
    if "message" in upd:
        m = upd["message"]
        cid, uid = m["chat"]["id"], m["from"]["id"]
        text = m.get("text", "").strip()

        db_action("INSERT OR IGNORE INTO users (user_id, lang) VALUES (?, 'ar')", (uid,))
        lang = get_user_lang(uid)

        if text == "/start":
            kb = {"inline_keyboard": [
                [{"text": TEXTS[lang]["btn_create"], "callback_data": "create"}],
                [{"text": TEXTS[lang]["btn_view"], "callback_data": "view"},
                 {"text": TEXTS[lang]["btn_lang"], "callback_data": "lang"}]
            ]}
            bot_api("sendMessage",
                    {"chat_id": cid, "text": TEXTS[lang]["welcome"], "reply_markup": kb, "parse_mode": "HTML"})
            return

        state = user_states.get(cid)
        if not state: return

        if state["step"] == "wait_start":
            try:
                datetime.strptime(text, "%Y-%m-%d %H:%M")
                user_states[cid] = {"start_t": text, "step": "wait_end"}
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["send_end"], "parse_mode": "HTML"})
            except:
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["format_err"]})

        elif state["step"] == "wait_end":
            try:
                datetime.strptime(text, "%Y-%m-%d %H:%M")
                user_states[cid]["end_t"] = text
                user_states[cid]["step"] = "wait_name"
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["saved_end"], "parse_mode": "HTML"})
            except:
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["format_err"]})

        elif state["step"] == "wait_name":
            title, start_t, end_t = text, state["start_t"], state["end_t"]
            res = bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["starting"]})
            if res and res.get("ok"):
                mid = res["result"]["message_id"]
                db_action("INSERT INTO timers VALUES (?, ?, ?, ?)", (cid, title, end_t, mid))
                user_states.pop(cid, None)
                threading.Thread(target=countdown_worker, args=(cid, end_t, title, mid, start_t), daemon=True).start()

    elif "callback_query" in upd:
        cq = upd["callback_query"]
        data, cid, uid = cq["data"], cq["message"]["chat"]["id"], cq["from"]["id"]
        lang = get_user_lang(uid)
        bot_api("answerCallbackQuery", {"callback_query_id": cq["id"]})

        if data == "create":
            user_states[cid] = {"step": "wait_start"}
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["send_start"], "parse_mode": "HTML"})
        elif data == "lang":
            kb = {"inline_keyboard": [
                [{"text": "العربية", "callback_data": "set_ar"}, {"text": "English", "callback_data": "set_en"}]]}
            bot_api("sendMessage", {"chat_id": cid, "text": "Select Language:", "reply_markup": kb})
        elif data.startswith("set_"):
            new_l = data.split("_")[1]
            db_action("UPDATE users SET lang = ? WHERE user_id = ?", (new_l, uid))
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[new_l]["lang_updated"]})


if __name__ == "__main__":
    init_db()
    print("🚀 Bot Started...")
    offset = None
    while True:
        try:
            upds = bot_api("getUpdates", {"offset": offset, "timeout": 20})
            if upds and upds.get("ok"):
                for u in upds["result"]:
                    offset = u["update_id"] + 1
                    handle_update(u)
        except:
            time.sleep(5)

