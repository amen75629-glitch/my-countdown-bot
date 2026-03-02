import sqlite3
import requests
import time
from datetime import datetime
import pytz
import threading

# ================== 1. الإعدادات الأساسية ==================
TOKEN = "8496382800:AAH6051l8WnJgNfJfUmOlzpDw1sROXKTAvs"
CHANNEL_ID = "@ZERO7097"  # ضع معرف قناتك هنا (مثال: @my_channel)
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
utc = pytz.utc

user_states = {}
# أيقونات متحركة لإعطاء جمالية للعداد
MOTION_ICONS = ["⏳", "⌛️", "🌀", "✨", "🔔"]
BAR_FILLED = "▉"
BAR_EMPTY = "░"

# ================== 2. القاموس اللغوي ==================
TEXTS = {
    "ar": {
        "sub_req": "⚠️ <b>عذراً! يجب عليك الاشتراك في القناة أولاً:</b>\n{}\n\nبعد الاشتراك، أرسل /start مجدداً.",
        "btn_sub": "📢 انضم للقناة",
        "welcome": "<b>مرحباً بك في بوت العدادات الذكي!</b> 🚀\nيمكنك إنشاء عدادات تنازلية احترافية لمهامك.\n\nاستخدم القائمة أدناه للتحكم:",
        "btn_create": "➕ إنشاء وقت جديد",
        "btn_view": "📊 أوقاتي",
        "btn_lang": "🌐 اللغة / Language",
        "send_start": "🗓 <b>الخطوة 1:</b> أرسل تاريخ البداية\nمثال: <code>2024-01-01 00:00</code>",
        "send_end": "🗓 <b>الخطوة 2:</b> أرسل تاريخ النهاية\nمثال: <code>2026-12-30 18:00</code>",
        "saved_end": "✅ تم حفظ التواريخ.\n📝 <b>الخطوة الأخيرة:</b> أرسل اسماً للعداد:",
        "format_err": "❌ تنسيق التاريخ غير صحيح! يرجى اتباع المثال بدقة.",
        "starting": "⚙️ جاري تشغيل المحرك الزمني...",
        "time_up": "<b>🔔 انتهى الوقت المخصص لـ:</b>\n✨ <code>{}</code> ✨",
        "no_timers": "📭 لا توجد لديك عدادات نشطة حالياً.",
        "your_timers": "📊 <b>قائمة عداداتك النشطة:</b>\nاختر العداد الذي ترغب في حذفه ❌:",
        "deleted": "🗑 تم حذف العداد وإيقاف المحرك بنجاح."
    },
    "en": {
        "sub_req": "⚠️ <b>Join our channel first:</b>\n{}\n\nThen send /start again.",
        "btn_sub": "📢 Join Channel",
        "welcome": "<b>Welcome to Smart Timer Bot!</b> 🚀\nCreate professional countdowns easily.",
        "btn_create": "➕ New Timer",
        "btn_view": "📊 My Timers",
        "btn_lang": "🌐 Language",
        "send_start": "🗓 <b>Step 1:</b> Send Start Date\nEx: <code>2024-01-01 00:00</code>",
        "send_end": "🗓 <b>Step 2:</b> Send End Date\nEx: <code>2026-12-30 18:00</code>",
        "saved_end": "✅ Dates saved.\n📝 <b>Final Step:</b> Send a name:",
        "format_err": "❌ Invalid format! Please follow the example.",
        "starting": "⚙️ Starting time engine...",
        "time_up": "<b>🔔 Time is up for:</b>\n✨ <code>{}</code> ✨",
        "no_timers": "📭 No active timers found.",
        "your_timers": "📊 <b>Your Active Timers:</b>\nClick ❌ to delete:",
        "deleted": "🗑 Timer deleted successfully."
    }
}


# ================== 3. قاعدة البيانات والاشتراك ==================
def init_db():
    with sqlite3.connect("bot_database.db") as conn:
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, lang TEXT DEFAULT "ar")')
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS timers (chat_id INTEGER, title TEXT, end_time TEXT, msg_id INTEGER PRIMARY KEY, start_time TEXT)')
        conn.commit()


def db_action(query, params=()):
    with sqlite3.connect("bot_database.db", check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        res = cursor.fetchall()
        conn.commit()
        return res


def check_sub(user_id):
    """التحقق من اشتراك المستخدم في القناة"""
    res = bot_api("getChatMember", {"chat_id": CHANNEL_ID, "user_id": user_id})
    if res and res.get("ok"):
        status = res["result"]["status"]
        return status in ["member", "administrator", "creator"]
    return False


def bot_api(method, data=None):
    try:
        return requests.post(f"{BASE_URL}/{method}", json=data, timeout=10).json()
    except:
        return None


# ================== 4. محرك العداد (التأثيرات البصرية) ==================
def countdown_worker(chat_id, end_time_str, title, mid, start_time_str):
    try:
        end_dt = utc.localize(datetime.strptime(end_time_str, "%Y-%m-%d %H:%M"))
        start_dt = utc.localize(datetime.strptime(start_time_str, "%Y-%m-%d %H:%M"))
    except:
        return

    icon_idx = 0
    while True:
        if not db_action("SELECT 1 FROM timers WHERE msg_id = ?", (mid,)): break

        lang = db_action("SELECT lang FROM users WHERE user_id = ?", (chat_id,))[0][0]
        now = datetime.now(utc)
        diff = end_dt - now

        if diff.total_seconds() <= 0:
            bot_api("editMessageText",
                    {"chat_id": chat_id, "message_id": mid, "text": TEXTS[lang]["time_up"].format(title),
                     "parse_mode": "HTML"})
            db_action("DELETE FROM timers WHERE msg_id = ?", (mid,))
            break

        # حساب الشريط بنسبة 10 مربعات
        total = (end_dt - start_dt).total_seconds()
        done = (now - start_dt).total_seconds()
        percent = max(0.0, min(100.0, (done / total) * 100)) if total > 0 else 100.0

        filled = int(percent / 10)
        bar = BAR_FILLED * filled + BAR_EMPTY * (10 - filled)

        icon = MOTION_ICONS[icon_idx]
        icon_idx = (icon_idx + 1) % len(MOTION_ICONS)

        msg_text = (
            f"{icon} <b>| العداد: {title}</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"📊 <b>التقدم:</b> <code>{bar}</code> {percent:.1f}%\n"
            f"🎯 <b>الهدف:</b> <code>{end_time_str}</code>\n"
            f"━━━━━━━━━━━━━━\n"
            f"⏳ <b>المتبقي:</b>\n"
            f"┕ <code>{diff.days}</code> يوم | <code>{diff.seconds // 3600}</code> ساعة | <code>{(diff.seconds // 60) % 60}</code> دقيقة\n"
            f"━━━━━━━━━━━━━━\n"
            f"<i>💡 يتحدث تلقائياً كل دقيقة..</i>"
        )

        res = bot_api("editMessageText",
                      {"chat_id": chat_id, "message_id": mid, "text": msg_text, "parse_mode": "HTML"})
        if res and not res.get("ok"): break
        time.sleep(60)


# ================== 5. المعالج الرئيسي ==================
def handle_update(upd):
    user_id = None
    if "message" in upd:
        user_id = upd["message"]["from"]["id"]
    elif "callback_query" in upd:
        user_id = upd["callback_query"]["from"]["id"]

    if not user_id: return

    # التحقق من الاشتراك قبل أي حركة
    if not check_sub(user_id):
        lang = "ar"  # افتراضي للجديد
        kb = {"inline_keyboard": [
            [{"text": TEXTS[lang]["btn_sub"], "url": f"https://t.me/{CHANNEL_ID.replace('@', '')}"}]]}
        bot_api("sendMessage",
                {"chat_id": user_id, "text": TEXTS[lang]["sub_req"].format(CHANNEL_ID), "reply_markup": kb,
                 "parse_mode": "HTML"})
        return

    if "message" in upd:
        m = upd["message"]
        cid, text = m["chat"]["id"], m.get("text", "").strip()
        lang = db_action("SELECT lang FROM users WHERE user_id = ?", (user_id,))
        lang = lang[0][0] if lang else "ar"
        if not db_action("SELECT 1 FROM users WHERE user_id = ?", (user_id,)):
            db_action("INSERT INTO users (user_id, lang) VALUES (?, ?)", (user_id, "ar"))

        if cid in user_states: user_states[cid]["msgs"].append(m["message_id"])

        if text == "/start":
            user_states.pop(cid, None)
            kb = {"inline_keyboard": [[{"text": TEXTS[lang]["btn_create"], "callback_data": "create"}],
                                      [{"text": TEXTS[lang]["btn_view"], "callback_data": "view"},
                                       {"text": TEXTS[lang]["btn_lang"], "callback_data": "lang"}]]}
            bot_api("sendMessage",
                    {"chat_id": cid, "text": TEXTS[lang]["welcome"], "reply_markup": kb, "parse_mode": "HTML"})
            return

        state = user_states.get(cid)
        if not state: return

        if state["step"] == "wait_start":
            try:
                datetime.strptime(text, "%Y-%m-%d %H:%M")
                state.update({"start_t": text, "step": "wait_end"})
                res = bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["send_end"], "parse_mode": "HTML"})
                state["msgs"].append(res["result"]["message_id"])
            except:
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["format_err"]})

        elif state["step"] == "wait_end":
            try:
                datetime.strptime(text, "%Y-%m-%d %H:%M")
                state.update({"end_t": text, "step": "wait_name"})
                res = bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["saved_end"], "parse_mode": "HTML"})
                state["msgs"].append(res["result"]["message_id"])
            except:
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["format_err"]})

        elif state["step"] == "wait_name":
            for mid in state["msgs"]: bot_api("deleteMessage", {"chat_id": cid, "message_id": mid})
            start_t, end_t, title = state["start_t"], state["end_t"], text
            res = bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["starting"]})
            if res and res.get("ok"):
                mid = res["result"]["message_id"]
                db_action("INSERT INTO timers VALUES (?, ?, ?, ?, ?)", (cid, title, end_t, mid, start_t))
                user_states.pop(cid)
                threading.Thread(target=countdown_worker, args=(cid, end_t, title, mid, start_t), daemon=True).start()

    elif "callback_query" in upd:
        cq = upd["callback_query"]
        data = cq["data"]
        cid = cq["message"]["chat"]["id"]
        lang = db_action("SELECT lang FROM users WHERE user_id = ?", (user_id,))[0][0]
        bot_api("answerCallbackQuery", {"callback_query_id": cq["id"]})

        if data == "create":
            user_states[cid] = {"step": "wait_start", "msgs": [cq["message"]["message_id"]]}
            res = bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["send_start"], "parse_mode": "HTML"})
            user_states[cid]["msgs"].append(res["result"]["message_id"])

        elif data == "view":
            timers = db_action("SELECT title, msg_id FROM timers WHERE chat_id = ?", (cid,))
            if not timers:
                bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["no_timers"]})
            else:
                kb = {"inline_keyboard": [[{"text": f"❌ {t[0]}", "callback_data": f"del_{t[1]}"}] for t in timers]}
                bot_api("sendMessage",
                        {"chat_id": cid, "text": TEXTS[lang]["your_timers"], "reply_markup": kb, "parse_mode": "HTML"})

        elif data.startswith("del_"):
            m_id = data.split("_")[1]
            db_action("DELETE FROM timers WHERE msg_id = ?", (m_id,))
            bot_api("deleteMessage", {"chat_id": cid, "message_id": int(m_id)})
            bot_api("sendMessage", {"chat_id": cid, "text": TEXTS[lang]["deleted"]})

        elif data == "lang":
            kb = {"inline_keyboard": [
                [{"text": "العربية 🇮🇶", "callback_data": "set_ar"}, {"text": "English 🇬🇧", "callback_data": "set_en"}]]}
            bot_api("sendMessage", {"chat_id": cid, "text": "Choose Language:", "reply_markup": kb})

        elif data.startswith("set_"):
            db_action("UPDATE users SET lang = ? WHERE user_id = ?", (data.split("_")[1], user_id))
            bot_api("sendMessage", {"chat_id": cid, "text": "✅"})


if __name__ == "__main__":
    init_db()
    # استعادة العدادات عند التشغيل
    active = db_action("SELECT chat_id, end_time, title, msg_id, start_time FROM timers")
    for t in active: threading.Thread(target=countdown_worker, args=(t[0], t[1], t[2], t[3], t[4]), daemon=True).start()

    print("🚀 البوت الاحترافي يعمل الآن مع شرط الاشتراك القناة...")
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

