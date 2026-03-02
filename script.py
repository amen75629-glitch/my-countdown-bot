import sqlite3
import requests
import time
from datetime import datetime
import pytz
import threading

# ================== 1. الإعدادات الأساسية ==================
TOKEN = "8496382800:AAH6051l8WnJgNfJfUmOlzpDw1sROXKTAvs"
CHANNEL_ID = "@ZERO7097"  # معرف قناتك (مثال: @tech_iq)
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
utc = pytz.utc

# مخازن مؤقتة للسرعة ومنع الكلجات
user_states = {}
active_threads = set()
ANIM_ICONS = ["⏳", "⌛", "⏲️", "✨", "🚀"]


# ================== 2. قاعدة البيانات (نسخة مستقرة) ==================
def db_action(query, params=()):
    try:
        with sqlite3.connect("bot_database.db", check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            res = cursor.fetchall()
            conn.commit()
            return res
    except Exception as error:
        print(f"⚠️ خطأ في قاعدة البيانات: {error}")
        return []


def init_db():
    db_action('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, lang TEXT DEFAULT "ar")')
    db_action(
        'CREATE TABLE IF NOT EXISTS timers (chat_id INTEGER, title TEXT, end_time TEXT, msg_id INTEGER PRIMARY KEY, start_time TEXT)')


# ================== 3. دوال المعالجة السريعة والتنظيف ==================
def bot_api(method, data=None):
    try:
        r = requests.post(f"{BASE_URL}/{method}", json=data, timeout=7)
        return r.json()
    except:
        return None


def quick_delete(chat_id, message_id, delay=0):
    """حذف ذكي في مسار منفصل لمنع تأخير البوت"""

    def run():
        if delay > 0: time.sleep(delay)
        bot_api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

    threading.Thread(target=run, daemon=True).start()


def check_sub(uid):
    """فحص الاشتراك الإجباري"""
    res = bot_api("getChatMember", {"chat_id": CHANNEL_ID, "user_id": uid})
    if res and res.get("ok"):
        return res["result"]["status"] in ["member", "administrator", "creator"]
    return False


# ================== 4. محرك العدادات (أداء عالي + تأثيرات) ==================
def countdown_worker(chat_id, end_time_str, title, mid, start_time_str):
    if mid in active_threads: return
    active_threads.add(mid)

    try:
        end_dt = utc.localize(datetime.strptime(end_time_str, "%Y-%m-%d %H:%M"))
        start_dt = utc.localize(datetime.strptime(start_time_str, "%Y-%m-%d %H:%M"))
    except:
        active_threads.remove(mid)
        return

    idx = 0
    while True:
        # فحص وجود العداد في القاعدة (للسماح بالحذف الفوري)
        if not db_action("SELECT 1 FROM timers WHERE msg_id = ?", (mid,)): break

        now = datetime.now(utc)
        diff = end_dt - now

        if diff.total_seconds() <= 0:
            bot_api("editMessageText", {"chat_id": chat_id, "message_id": mid,
                                        "text": f"🔔 <b>انتهى الوقت تماماً:</b>\n✨ <code>{title}</code> ✨",
                                        "parse_mode": "HTML"})
            db_action("DELETE FROM timers WHERE msg_id = ?", (mid,))
            break

        # حساب شريط التقدم ▉▉▉░░
        total_sec = (end_dt - start_dt).total_seconds()
        passed_sec = (now - start_dt).total_seconds()
        percent = max(0.0, min(100.0, (passed_sec / total_sec) * 100)) if total_sec > 0 else 100.0

        bar_size = 10
        filled = int((percent / 100) * bar_size)
        bar = "▉" * filled + "░" * (bar_size - filled)

        icon = ANIM_ICONS[idx % len(ANIM_ICONS)]
        idx += 1

        msg = (f"{icon} <b>العداد: {title}</b>\n"
               f"━━━━━━━━━━━━━━\n"
               f"📊 <b>التقدم:</b> <code>{bar}</code> {percent:.1f}%\n"
               f"⏳ <b>المتبقي:</b> <code>{diff.days}</code> يوم و <code>{diff.seconds // 3600}</code> ساعة\n"
               f"━━━━━━━━━━━━━━\n"
               f"<i>🔄 تحديث تلقائي كل دقيقة</i>")

        res = bot_api("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": msg, "parse_mode": "HTML"})

        # إذا تم حذف الرسالة يدوياً من قبل المستخدم، أوقف الـ Thread
        if res and not res.get("ok"):
            db_action("DELETE FROM timers WHERE msg_id = ?", (mid,))
            break

        time.sleep(60)  # تحديث كل دقيقة لتقليل الضغط على السيرفر

    if mid in active_threads: active_threads.remove(mid)


# ================== 5. معالج الأحداث الذكي (المراقب) ==================
def handle_update(upd):
    uid = None
    if "message" in upd:
        uid = upd["message"]["from"]["id"]
    elif "callback_query" in upd:
        uid = upd["callback_query"]["from"]["id"]
    if not uid: return

    # فحص الاشتراك الإجباري أولاً
    if not check_sub(uid):
        kb = {"inline_keyboard": [[{"text": "📢 انضم للقناة الآن", "url": f"https://t.me/{CHANNEL_ID[1:]}"}]]}
        bot_api("sendMessage", {"chat_id": uid, "text": "⚠️ <b>عذراً!</b> يجب عليك الاشتراك في القناة لاستخدام البوت.",
                                "reply_markup": kb, "parse_mode": "HTML"})
        return

    if "message" in upd:
        m = upd["message"]
        cid = m["chat"]["id"]
        text = m.get("text", "")

        if text == "/start":
            user_states.pop(cid, None)  # تصفير الحالة عند البدء من جديد
            kb = {"inline_keyboard": [[{"text": "➕ إنشاء عداد جديد", "callback_data": "create"}],
                                      [{"text": "📊 أوقاتي النشطة", "callback_data": "view"},
                                       {"text": "🌐 اللغة", "callback_data": "lang"}]]}
            bot_api("sendMessage", {"chat_id": cid,
                                    "text": "<b>مرحباً بك في بوت العدادات الاحترافي!</b> 🚀\n\nالبوت يعمل الآن بأعلى كفاءة وتنظيف تلقائي.",
                                    "reply_markup": kb, "parse_mode": "HTML"})
            return

        # مراقب الرسائل العشوائية
        if cid not in user_states:
            quick_delete(cid, m["message_id"])
            return

        state = user_states[cid]
        state["msgs"].append(m["message_id"])

        # معالجة خطوات الإنشاء مع فحص الأخطاء
        if state["step"] in ["wait_start", "wait_end"]:
            try:
                datetime.strptime(text, "%Y-%m-%d %H:%M")
                if state["step"] == "wait_start":
                    state.update({"start_t": text, "step": "wait_end"})
                    res = bot_api("sendMessage", {"chat_id": cid,
                                                  "text": "🗓️ الآن أرسل <b>تاريخ ووقت النهاية</b>\nمثال: <code>2025-12-30 15:30</code>",
                                                  "parse_mode": "HTML"})
                else:
                    state.update({"end_t": text, "step": "wait_name"})
                    res = bot_api("sendMessage",
                                  {"chat_id": cid, "text": "✅ ممتاز! أخيراً أرسل <b>اسماً لهذا العداد</b>:",
                                   "parse_mode": "HTML"})
                state["msgs"].append(res["result"]["message_id"])
            except:
                quick_delete(cid, m["message_id"])
                err = bot_api("sendMessage", {"chat_id": cid,
                                              "text": "❌ <b>تنسيق خطأ!</b> يرجى الالتزام بالمثال: <code>2024-01-01 10:00</code>",
                                              "parse_mode": "HTML"})
                quick_delete(cid, err["result"]["message_id"], delay=4)

        elif state["step"] == "wait_name":
            # تنظيف المحادثة قبل إظهار العداد النهائي
            for mid in state["msgs"]: quick_delete(cid, mid)

            res = bot_api("sendMessage", {"chat_id": cid, "text": "⚙️ جاري معالجة البيانات وتشغيل المحرك..."})
            mid = res["result"]["message_id"]
            db_action("INSERT INTO timers VALUES (?, ?, ?, ?, ?)", (cid, text, state["end_t"], mid, state["start_t"]))
            threading.Thread(target=countdown_worker, args=(cid, state["end_t"], text, mid, state["start_t"]),
                             daemon=True).start()
            user_states.pop(cid)

    elif "callback_query" in upd:
        cq = upd["callback_query"]
        data = cq["data"]
        cid = cq["message"]["chat"]["id"]
        bot_api("answerCallbackQuery", {"callback_query_id": cq["id"]})  # استجابة فورية للزر

        if data == "create":
            user_states[cid] = {"step": "wait_start", "msgs": [cq["message"]["message_id"]]}
            res = bot_api("sendMessage", {"chat_id": cid,
                                          "text": "🗓️ أرسل <b>تاريخ ووقت البداية</b>\nمثال: <code>2024-01-01 00:00</code>",
                                          "parse_mode": "HTML"})
            user_states[cid]["msgs"].append(res["result"]["message_id"])

        elif data == "view":
            timers = db_action("SELECT title, msg_id FROM timers WHERE chat_id = ?", (cid,))
            if not timers:
                bot_api("sendMessage", {"chat_id": cid, "text": "📭 لا توجد لديك عدادات نشطة حالياً."})
            else:
                kb = {"inline_keyboard": [[{"text": f"❌ {tx[0]}", "callback_data": f"del_{tx[1]}"}] for tx in timers]}
                bot_api("sendMessage",
                        {"chat_id": cid, "text": "📊 <b>عداداتك النشطة:</b>\n(اضغط على الاسم لحذف العداد وإيقافه)",
                         "reply_markup": kb, "parse_mode": "HTML"})

        elif data.startswith("del_"):
            m_id = int(data.split("_")[1])
            db_action("DELETE FROM timers WHERE msg_id = ?", (m_id,))
            quick_delete(cid, m_id)  # حذف رسالة العداد فوراً


# ================== 6. التشغيل المستقر (الرسترة الذكية) ==================
if __name__ == "__main__":
    init_db()
    # استعادة العدادات بعد الرسترة
    active_timers = db_action("SELECT chat_id, end_time, title, msg_id, start_time FROM timers")
    for t in active_timers:
        threading.Thread(target=countdown_worker, args=(t[0], t[1], t[2], t[3], t[4]), daemon=True).start()

    print("✅ البوت فحص بنجاح ويعمل الآن بأقصى سرعة...")
    offset = -1  # تجاهل الرسائل القديمة أثناء التوقف
    while True:
        try:
            upds = bot_api("getUpdates", {"offset": offset, "timeout": 15})
            if upds and upds.get("ok"):
                for u in upds["result"]:
                    offset = u["update_id"] + 1
                    handle_update(u)
        except Exception as e:
            time.sleep(1)  # منع استهلاك المعالج عند حدوث خطأ في الاتصال
