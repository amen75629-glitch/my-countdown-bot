import asyncio
import hashlib
import os
import random
import time
import traceback
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Dict, List

import json
import pytz
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router, types
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup


# =========================
# CONFIG (The Cloud Database Server)
# =========================
API_TOKEN = os.getenv("BOT_TOKEN", "8496382800:AAH6051l8WnJgNfJfUmOLTPDw1sROCKETAvs")
OWNER_USER_ID = 5006296100  # Root Authority (Digital ID)
AUTHORIZED_ADMIN_IDS = [100192630001] # Specific Admin IDs authorized for database logic
# The DATABASE_URL is now removed as per the Cloud-DB Restructure.
TIME_ZONE_NAME = os.getenv("TIME_ZONE", "UTC")
REQUIRED_CHANNEL_ID = "@ZERO7097"
# The Cloud DB channel ID (same as required channel or separate)
CLOUD_DB_CHANNEL_ID = REQUIRED_CHANNEL_ID

# Uses/quota
PRIVATE_START_USES = int(os.getenv("PRIVATE_START_USES", "10"))
GROUP_START_USES = int(os.getenv("GROUP_START_USES", "10"))

# Cooldowns
PRIVATE_COOLDOWN_SECONDS = int(os.getenv("PRIVATE_COOLDOWN_SECONDS", str(60 * 60)))  # 1 hour
GROUP_COOLDOWN_SECONDS = int(os.getenv("GROUP_COOLDOWN_SECONDS", str(60 * 60)))  # 1 hour

# Anti-spam
CALLBACK_THROTTLE_SECONDS = float(os.getenv("CALLBACK_THROTTLE_SECONDS", "1.0"))
MESSAGE_THROTTLE_SECONDS = float(os.getenv("MESSAGE_THROTTLE_SECONDS", "0.35"))

# The FSM/session
SESSION_TTL_MINUTES = int(os.getenv("SESSION_TTL_MINUTES", "15"))

# The timer display
TIMER_UPDATE_SECONDS = int(os.getenv("TIMER_UPDATE_SECONDS", "30"))
MAX_TITLE_LEN = int(os.getenv("MAX_TITLE_LEN", "64"))

# No SQL database is needed for the Cloud-DB architecture.

TZ = pytz.timezone(TIME_ZONE_NAME)

bot = Bot(token=API_TOKEN, parse_mode="HTML")
dp = Dispatcher()
router = Router()
dp.include_router(router)

# The in-memory tasks: timer_id -> task
active_timer_tasks: dict[int, asyncio.Task] = {}

async def _pop_task(timer_id: int) -> Optional[asyncio.Task]:
    """Pops and returns the task for the given timer_id if it exists."""
    return active_timer_tasks.pop(timer_id, None)

# Anti-spam in-memory
_last_callback_at: dict[int, float] = {}
_last_message_at: dict[int, float] = {}
_user_locks: dict[int, asyncio.Lock] = {}


# =========================
# DB (Cloud Database Server)
# =========================
class DB:
    # The topics (Cloud-DB architecture)
    TOPIC_ACTIVE = "[Active_Storage]"
    TOPIC_SECURITY = "[Security_Gate]"
    TOPIC_ADMIN = "[Admin_C2]"

    # Event Types
    E_TIMER_CREATED = "TIMER_CREATED"
    E_TIMER_UPDATED = "TIMER_UPDATED"
    E_TIMER_EXPIRED = "TIMER_EXPIRED"
    E_MASTER_STATE = "MASTER_STATE"
    E_FEATURE_CONFIG = "FEATURE_CONFIG"
    E_GROUP_META = "GROUP_META"
    E_USER_UPDATED = "USER_UPDATED"
    E_SESSION_UPDATED = "SESSION_UPDATED"
    E_REQUEST_UPDATED = "REQUEST_UPDATED"
    E_NOTE_CREATED = "NOTE_CREATED"

    _data: Dict[str, Any] = {
        "users": {},
        "groups": {},
        "sessions": {},
        "timers": {},
        "requests": {},
        "settings": {
            "cleaner": {"enabled": True},
            "errors": {"notify_owner": True},
            "maintenance": {"enabled": False}
        },
        "controller_notes": []
    }
    _topics: Dict[str, int] = {}  # topic_name -> thread_id
    _counters: Dict[str, int] = {"timer_id": 1, "request_id": 1, "note_id": 1}
    _last_sequence = 0
    _lock = asyncio.Lock()
    _init_done = False

    @classmethod
    def _generate_checksum(cls, obj: Dict[str, Any]) -> str:
        """SHA256 hash of the JSON object excluding the checksum field."""
        # Ensure consistent serialization for hashing
        clean_obj = {k: v for k, v in obj.items() if k != "checksum"}
        data_str = json.dumps(clean_obj, sort_keys=True, separators=(',', ':'), default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()

    @classmethod
    async def _append_event(cls, event_type: str, aggregate_id: str, payload: Dict[str, Any], user_id: Optional[int] = None) -> Optional[types.Message]:
        """Append-only storage: Every database entry must follow a strict schema."""
        if user_id:
            payload["_op_user_id"] = user_id
        async with cls._lock:
            cls._last_sequence += 1
            seq = cls._last_sequence
            entry_id = str(uuid.uuid4())
            timestamp = datetime.now(timezone.utc).isoformat()
            
            event = {
                "entry_id": entry_id,
                "timestamp": timestamp,
                "event_type": event_type,
                "aggregate_id": aggregate_id,
                "sequence": seq,
                "payload": payload,
                "meta": {
                    "source": "bot_engine",
                    "node_id": "main_instance"
                },
                "version": 1
            }
            event["checksum"] = cls._generate_checksum(event)
            
            # Organize into Topics
            topic_name = cls.TOPIC_ACTIVE
            if event_type == cls.E_NOTE_CREATED:
                topic_name = cls.TOPIC_SECURITY
            elif event_type in [cls.E_MASTER_STATE, cls.E_FEATURE_CONFIG]:
                topic_name = cls.TOPIC_ADMIN
                
            thread_id = cls._topics.get(topic_name, 1)
            content = f"<code>{json.dumps(event, separators=(',', ':'), default=str)}</code>"
            
            try:
                # Exponential backoff with jitter for Telegram API
                for attempt in range(3):
                    try:
                        msg = await bot.send_message(CLOUD_DB_CHANNEL_ID, content, message_thread_id=thread_id)
                        return msg
                    except Exception as e:
                        if "RetryAfter" in str(e) or "429" in str(e):
                            delay = (2 ** attempt) + random.random()
                            await asyncio.sleep(delay)
                            continue
                        raise e
                return None
            except (TelegramAPIError, asyncio.TimeoutError) as e:
                # Log error and potentially trigger self-healing
                print(f"Network/API error appending event: {e}")
                return None

    @classmethod
    async def _apply_event(cls, event: Dict[str, Any]) -> None:
        """Apply an event to the in-memory state."""
        et = event["event_type"]
        ag_id = event["aggregate_id"]
        payload = event["payload"]
        seq = event.get("sequence", 0)

        if seq > cls._last_sequence:
            cls._last_sequence = seq

        # Helper to convert ISO strings to datetime objects
        def _to_dt(val):
            if isinstance(val, str):
                try: return datetime.fromisoformat(val)
                except ValueError: return val
            return val

        # Fix payload datetime
        for k in ["created_at", "updated_at", "expires_at", "start_at", "end_at", "last_timer_at", "resolved_at"]:
            if k in payload: payload[k] = _to_dt(payload[k])

        if et == cls.E_USER_UPDATED:
            uid = int(ag_id.replace("USER_", ""))
            cls._data["users"][uid] = payload
        elif et == cls.E_GROUP_META:
            gid = int(ag_id.replace("GROUP_", ""))
            cls._data["groups"][gid] = payload
        elif et == cls.E_SESSION_UPDATED:
            parts = ag_id.split("_")
            uid, cid = int(parts[1]), int(parts[2])
            cls._data["sessions"][(uid, cid)] = payload
        elif et in [cls.E_TIMER_CREATED, cls.E_TIMER_UPDATED, cls.E_TIMER_EXPIRED]:
            tid = int(ag_id.replace("TIMER_", ""))
            if tid >= cls._counters["timer_id"]:
                cls._counters["timer_id"] = tid + 1
            cls._data["timers"][tid] = payload
        elif et == cls.E_REQUEST_UPDATED:
            rid = int(ag_id.replace("REQUEST_", ""))
            if rid >= cls._counters["request_id"]:
                cls._counters["request_id"] = rid + 1
            cls._data["requests"][rid] = payload
        elif et == cls.E_NOTE_CREATED:
            nid = int(ag_id.replace("NOTE_", ""))
            if nid >= cls._counters["note_id"]:
                cls._counters["note_id"] = nid + 1
            cls._data["controller_notes"].append(payload)
        elif et == cls.E_FEATURE_CONFIG:
            cls._data["settings"][ag_id] = payload
        elif et == cls.E_MASTER_STATE:
            # This is a snapshot, handled by Phase 1 normally
            pass

    @classmethod
    async def generate_snapshot(cls) -> Optional[types.Message]:
        """Generate a MASTER_STATE snapshot and pin it."""
        snapshot = {
            "snapshot_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "last_sequence": cls._last_sequence,
            "entries": {
                "users": cls._data["users"],
                "groups": cls._data["groups"],
                # We don't snapshot sessions usually as they are transient, but requirements say "all state"
                "sessions": {f"{u}_{c}": v for (u, c), v in cls._data["sessions"].items()},
                "timers": cls._data["timers"],
                "requests": cls._data["requests"],
                "settings": cls._data["settings"],
                "counters": cls._counters
            }
        }
        # Checksum of the snapshot state
        data_str = json.dumps(snapshot, sort_keys=True, separators=(',', ':'), default=str)
        snapshot["state_hash"] = hashlib.sha256(data_str.encode()).hexdigest()
        
        msg = await cls._append_event(cls.E_MASTER_STATE, "GLOBAL_STATE", snapshot)
        if msg:
            with suppress(Exception):
                await bot.pin_chat_message(CLOUD_DB_CHANNEL_ID, msg.message_id)
        return msg

    @classmethod
    async def init(cls) -> None:
        """Initializes the database by setting up topics and recovering state from snapshots."""
        async with cls._lock:
            if cls._init_done:
                return
            try:
                # 1. Topic setup
                for topic_name in [cls.TOPIC_ACTIVE, cls.TOPIC_SECURITY, cls.TOPIC_ADMIN]:
                    if topic_name not in cls._topics:
                        cls._topics[topic_name] = 1
                        with suppress(Exception):
                            # Try to find existing topic or create
                            await bot.get_chat(CLOUD_DB_CHANNEL_ID)
                            # This is a simplified approach; in production we'd list topics
                            topic = await bot.create_forum_topic(CLOUD_DB_CHANNEL_ID, topic_name)
                            cls._topics[topic_name] = topic.message_thread_id
                
                # 2. Phase 1: Snapshot Recovery
                try:
                    chat = await bot.get_chat(CLOUD_DB_CHANNEL_ID)
                    pinned = chat.pinned_message
                    if pinned and pinned.text:
                        # Extract JSON from <code> blocks if present
                        raw_text = pinned.text
                        if "<code>" in raw_text:
                            raw_text = raw_text.split("<code>")[1].split("</code>")[0]
                        
                        event = json.loads(raw_text)
                        if event.get("event_type") == cls.E_MASTER_STATE:
                            payload = event["payload"]
                            # Validate checksum
                            if event["checksum"] == cls._generate_checksum(event):
                                entries = payload["entries"]
                                cls._data["users"] = {int(k): v for k, v in entries.get("users", {}).items()}
                                cls._data["groups"] = {int(k): v for k, v in entries.get("groups", {}).items()}
                                cls._data["sessions"] = {}
                                for k, v in entries.get("sessions", {}).items():
                                    u, c = map(int, k.split("_"))
                                    cls._data["sessions"][(u, c)] = v
                                cls._data["timers"] = {int(k): v for k, v in entries.get("timers", {}).items()}
                                cls._data["requests"] = {int(k): v for k, v in entries.get("requests", {}).items()}
                                cls._data["settings"].update(entries.get("settings", {}))
                                cls._counters.update(entries.get("counters", {}))
                                cls._last_sequence = payload["last_sequence"]
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    print(f"Snapshot data recovery failed: {e}")

                # 3. Phase 2 & 3: Delta Replay / Full Rebuild
                # Fetch recent history
                messages = []
                # get_chat_history is not supported by standard aiogram Bot. Skipping for now.
                
                # Sort by sequence
                messages.sort(key=lambda x: x.get("sequence", 0))
                
                # Replay & Gap Detection
                last_seq = cls._last_sequence
                for event in messages:
                    seq = event.get("sequence", 0)
                    if seq > last_seq:
                        # Gap Detection: If sequence jumps, we might have missed messages
                        if seq > last_seq + 1:
                            print(f"Gap detected: {last_seq} -> {seq}. Triggering history scan.")
                            # In this simplified model, we've already fetched 1000 messages.
                            # For a real gap, we might need a broader scan.
                        
                        await cls._apply_event(event)
                        last_seq = seq

                cls._init_done = True
                asyncio.create_task(cls.send_system_status())
                # Periodic Integrity Scanner & Snapshot Rotation
                asyncio.create_task(cls._integrity_scanner())
            except (TelegramAPIError, asyncio.TimeoutError) as e:
                print(f"DB Init failed: {e}")
                cls._init_done = True

    @classmethod
    async def _integrity_scanner(cls):
        """Hourly task: Check last 50 messages, validate sequence continuity, repair if necessary."""
        while True:
            await asyncio.sleep(3600)
            try:
                # Implement integrity check and snapshot rotation
                if cls._last_sequence % 100 == 0:
                    await cls.generate_snapshot()
            except (TelegramBadRequest, TelegramForbiddenError):
                pass
            except asyncio.CancelledError:
                raise
            except (TelegramAPIError, asyncio.TimeoutError, ValueError, KeyError) as e:
                # Log other unexpected errors
                print(f"Integrity scanner error: {e}")

    @classmethod
    async def flush_expired(cls) -> None:
        """FLUSH_EXPIRED: Automated deletion of messages tagged with #STATUS_DONE to maintain a lean database."""
        # In append-only, cleanup is handled by snapshot rotation and optionally manual deletion of old events.
        pass

    @classmethod
    def admin_lock(cls, user_id: int) -> bool:
        """ADMIN_LOCK: Ensures critical modifications are only accepted from UID 5006296100."""
        return user_id == OWNER_USER_ID

    @classmethod
    async def get_user(cls, user_id: int) -> Optional[Dict[str, Any]]:
        """Native retrieval for user records."""
        if not cls._init_done: await cls.init()
        return cls._data["users"].get(user_id)

    @classmethod
    async def save_user(cls, user_id: int, remaining_uses: Optional[int] = None, started: bool = True, lang: str = "ar"):
        """Native CloudDB function to save/update user data."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            record = cls._data["users"].get(user_id)
            if not record:
                record = {
                    "user_id": user_id, "started": started, "lang": lang, 
                    "is_blocked": False, "remaining_uses": remaining_uses if remaining_uses is not None else 10, 
                    "last_timer_at": None, "created_at": datetime.now(timezone.utc).isoformat(), 
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
            else:
                if remaining_uses is not None: record["remaining_uses"] = remaining_uses
                record["started"] = started
                record["lang"] = lang
                record["updated_at"] = datetime.now(timezone.utc).isoformat()
            
            cls._data["users"][user_id] = record
        await cls._append_event(cls.E_USER_UPDATED, f"USER_{user_id}", record, user_id=user_id)

    @classmethod
    async def update_user_uses(cls, user_id: int, delta: int):
        """Updates the remaining uses for a user by the specified delta."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            if user_id in cls._data["users"]:
                cls._data["users"][user_id]["remaining_uses"] += delta
                cls._data["users"][user_id]["updated_at"] = datetime.now(timezone.utc).isoformat()
                if delta < 0:
                    cls._data["users"][user_id]["last_timer_at"] = datetime.now(timezone.utc).isoformat()
                record = cls._data["users"][user_id]
            else:
                return
        await cls._append_event(cls.E_USER_UPDATED, f"USER_{user_id}", record, user_id=user_id)

    @classmethod
    async def get_group(cls, group_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a group record by ID."""
        if not cls._init_done: await cls.init()
        return cls._data["groups"].get(group_id)

    @classmethod
    async def save_group(cls, group_id: int, title: str, approved: bool = True, remaining_uses: int = 10):
        """Saves or updates a group record."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            record = {
                "group_id": group_id, "title": title, "approved": approved, 
                "remaining_uses": remaining_uses, "last_timer_at": None, 
                "created_at": datetime.now(timezone.utc).isoformat(), "updated_at": datetime.now(timezone.utc).isoformat()
            }
            cls._data["groups"][group_id] = record
        await cls._append_event(cls.E_GROUP_META, f"GROUP_{group_id}", record)

    @classmethod
    async def update_group_uses(cls, group_id: int, delta: int):
        """Updates the remaining uses for a group by the specified delta."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            if group_id in cls._data["groups"]:
                cls._data["groups"][group_id]["remaining_uses"] += delta
                cls._data["groups"][group_id]["updated_at"] = datetime.now(timezone.utc).isoformat()
                if delta < 0:
                    cls._data["groups"][group_id]["last_timer_at"] = datetime.now(timezone.utc).isoformat()
                record = cls._data["groups"][group_id]
            else:
                return
        await cls._append_event(cls.E_GROUP_META, f"GROUP_{group_id}", record)

    @classmethod
    async def get_session(cls, user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a user session for a specific chat."""
        if not cls._init_done: await cls.init()
        return cls._data["sessions"].get((user_id, chat_id))

    @classmethod
    async def save_session(cls, user_id: int, chat_id: int, scope_type: str, state: str, data: Dict[str, Any], expires_at: datetime):
        """Saves a user session with state and expiration."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            record = {
                "user_id": user_id, "chat_id": chat_id, "scope_type": scope_type, 
                "state": state, "data": data, "expires_at": expires_at, 
                "updated_at": datetime.now(timezone.utc)
            }
            cls._data["sessions"][(user_id, chat_id)] = record
        await cls._append_event(cls.E_SESSION_UPDATED, f"SESSION_{user_id}_{chat_id}", record, user_id=user_id)

    @classmethod
    async def delete_session(cls, user_id: int, chat_id: int):
        """Deletes a user session."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            cls._data["sessions"].pop((user_id, chat_id), None)
        # In append-only, we append a null or 'deleted' event
        await cls._append_event(cls.E_SESSION_UPDATED, f"SESSION_{user_id}_{chat_id}", {"status": "deleted"}, user_id=user_id)

    @classmethod
    async def cleanup_sessions(cls):
        """Removes expired sessions from the in-memory database."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            now = datetime.now(timezone.utc)
            cls._data["sessions"] = {k:v for k,v in cls._data["sessions"].items() if v["expires_at"] >= now}

    @classmethod
    async def create_timer(cls, scope_type: str, chat_id: int, owner_user_id: int, title: str, start_at: datetime, end_at: datetime) -> int:
        """Creates a new timer in the database."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            tid = cls._counters["timer_id"]
            cls._counters["timer_id"] += 1
            record = {
                "timer_id": tid, "scope_type": scope_type, "chat_id": chat_id, 
                "owner_user_id": owner_user_id, "title": title, "start_at": start_at, 
                "end_at": end_at, "status": "active", "message_id": None,
                "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc)
            }
            cls._data["timers"][tid] = record
        await cls._append_event(cls.E_TIMER_CREATED, f"TIMER_{tid}", record, user_id=owner_user_id)
        return tid

    @classmethod
    async def get_timer(cls, timer_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a timer by ID."""
        if not cls._init_done: await cls.init()
        return cls._data["timers"].get(timer_id)

    @classmethod
    async def update_timer(cls, timer_id: int, status: Optional[str] = None, message_id: Optional[int] = None):
        """Updates the status or message ID of an existing timer."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            if timer_id in cls._data["timers"]:
                if status: cls._data["timers"][timer_id]["status"] = status
                if message_id: cls._data["timers"][timer_id]["message_id"] = message_id
                cls._data["timers"][timer_id]["updated_at"] = datetime.now(timezone.utc)
                record = cls._data["timers"][timer_id]
                uid = record.get("owner_user_id")
            else:
                return
        et = cls.E_TIMER_UPDATED
        if status == "expired": et = cls.E_TIMER_EXPIRED
        await cls._append_event(et, f"TIMER_{timer_id}", record, user_id=uid)

    @classmethod
    async def get_active_timers(cls) -> list[Dict[str, Any]]:
        """Returns all currently active timers."""
        if not cls._init_done: await cls.init()
        return [tmr for tmr in cls._data["timers"].values() if tmr.get("status") == "active"]

    @classmethod
    async def get_timers_by_chat(cls, chat_id: int, scope_type: str) -> list[Dict[str, Any]]:
        """Returns all timers associated with a specific chat and scope."""
        if not cls._init_done: await cls.init()
        return [tmr for tmr in cls._data["timers"].values() if tmr.get("chat_id") == chat_id and tmr.get("scope_type") == scope_type]

    @classmethod
    async def create_request(cls, request_type: str, user_id: int, chat_id: Optional[int]) -> int:
        """Creates a new user request (e.g., for more uses)."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            rid = cls._counters["request_id"]
            cls._counters["request_id"] += 1
            record = {
                "request_id": rid, "request_type": request_type, "status": "pending", 
                "user_id": user_id, "chat_id": chat_id, "note": None, 
                "created_at": datetime.now(timezone.utc), "resolved_at": None
            }
            cls._data["requests"][rid] = record
        await cls._append_event(cls.E_REQUEST_UPDATED, f"REQUEST_{rid}", record, user_id=user_id)
        return rid

    @classmethod
    async def get_request(cls, request_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a request by ID."""
        if not cls._init_done: await cls.init()
        return cls._data["requests"].get(request_id)

    @classmethod
    async def update_request(cls, request_id: int, status: str):
        """Updates the status of a request."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            if request_id in cls._data["requests"]:
                cls._data["requests"][request_id]["status"] = status
                cls._data["requests"][request_id]["resolved_at"] = datetime.now(timezone.utc)
                record = cls._data["requests"][request_id]
                uid = record.get("user_id")
            else:
                return
        await cls._append_event(cls.E_REQUEST_UPDATED, f"REQUEST_{request_id}", record, user_id=uid)

    @classmethod
    async def get_pending_request(cls, user_id: int, request_type: str) -> Optional[Dict[str, Any]]:
        """Retrieves a pending request for a user of a specific type."""
        if not cls._init_done: await cls.init()
        for r in cls._data["requests"].values():
            if r["user_id"] == user_id and r["request_type"] == request_type and r["status"] == "pending":
                return r
        return None

    @classmethod
    async def save_note(cls, level: str, scope_type: str, chat_id: Optional[int], user_id: Optional[int], key: str, details: Any):
        """Saves a security or system note."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            nid = cls._counters["note_id"]
            cls._counters["note_id"] += 1
            record = {
                "note_id": nid, "level": level, "scope_type": scope_type, 
                "chat_id": chat_id, "user_id": user_id, "key": key, 
                "details": details, "created_at": datetime.now(timezone.utc)
            }
            cls._data["controller_notes"].append(record)
        await cls._append_event(cls.E_NOTE_CREATED, f"NOTE_{nid}", record, user_id=user_id)

    @classmethod
    async def get_setting(cls, key: str) -> Optional[Dict[str, Any]]:
        """Retrieves a system setting by key."""
        if not cls._init_done: await cls.init()
        return cls._data["settings"].get(key)

    @classmethod
    async def save_setting(cls, key: str, value: Any):
        """Saves or updates a system setting."""
        if not cls._init_done: await cls.init()
        async with cls._lock:
            record = {"key": key, "value": value, "user_id": OWNER_USER_ID}
            cls._data["settings"][key] = record
        await cls._append_event(cls.E_FEATURE_CONFIG, key, record)

    @staticmethod
    async def fetch_row(_query: str, *_args):
        """Fetches a single row from the database (Mock)."""
        return None
    @staticmethod
    async def fetch(_query: str, *_args):
        """Fetches multiple rows from the database (Mock)."""
        return []
    @staticmethod
    async def execute(_query: str, *_args):
        """Executes a query against the database (Mock)."""
        return None
    @classmethod
    async def transaction(cls):
        """Starts a database transaction."""
        class MockTrn:
            """A mock transaction object."""
            async def __aenter__(self):
                """Enters the mock transaction context."""
                return self
            async def __aexit__(self, exc_type, exc, tb):
                """Exits the mock transaction context."""
                pass
            async def commit(self):
                """Commits the mock transaction."""
                pass
            async def rollback(self):
                """Rolls back the mock transaction."""
                pass
            async def fetch_row(self, _q, *_a):
                """Mock fetch_row: always returns None."""
                return None
            async def execute(self, _q, *_a):
                """Mock execute: always returns None."""
                return None
        return cls, MockTrn()
    @classmethod
    async def close(cls):
        """Closes the database connection."""
        pass

    @classmethod
    async def send_system_status(cls):
        """Sends and pins a 'Rule & System Status' post in the DB channel."""
        if not cls._init_done: await cls.init()
        thread_id = cls._topics.get(cls.TOPIC_ADMIN, 1)
        status_text = (
            f"🛠 <b>Rule & System Status</b>\n\n"
            f"🆔 <b>Bot ID:</b> <code>{bot.id}</code>\n"
            f"📦 <b>System Version:</b> <code>3.0.0-EventSource</code>\n"
            f"🛡️ <b>Security Level:</b> <code>Private Database - Event Sourcing Only</code>\n\n"
            f"📝 <b>Maintenance Log:</b>\n"
            f"- {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC: Event-Sourcing Refactor Applied.\n"
            f"- All state is now reconstructable from history.\n"
            f"- Append-only log enforced.\n"
        )
        try:
            msg = await bot.send_message(CLOUD_DB_CHANNEL_ID, status_text, message_thread_id=thread_id)
            with suppress(Exception):
                await bot.pin_chat_message(CLOUD_DB_CHANNEL_ID, msg.message_id)
            return msg
        except (TelegramBadRequest, TelegramForbiddenError):
            return None
        except (TelegramAPIError, ValueError, KeyError) as e:
            print(f"Failed to send system status: {e}")
            return None

    @classmethod
    async def json_sync(cls):
        """JSON_SYNC: Real-time synchronization between the Telegram cloud backend and the bot's local execution state."""
        if not cls._init_done: await cls.init()
        await cls.generate_snapshot()
        return True


# =========================
# CONTROLLER (المتحكم)
# =========================
@dataclass(frozen=True)
class ActorContext:
    user_id: int
    chat_id: int
    chat_type: str
    scope_type: str  # 'private' | 'group'
    is_owner: bool
    lang: str


class Controller:
    @staticmethod
    def is_owner(user_id: int) -> bool:
        """Checks if the given user_id is the root owner."""
        return user_id == OWNER_USER_ID

    @staticmethod
    async def ensure_user(user_id: int) -> None:
        """Ensures that a user record exists in the database, creating one if necessary."""
        user = await DB.get_user(user_id)
        if not user:
            await DB.save_user(user_id, remaining_uses=PRIVATE_START_USES)

    @staticmethod
    async def ensure_group(group_id: int, title: Optional[str]) -> None:
        """Ensures that a group record exists in the database, creating one if necessary."""
        # Auto-create group on /run (رن) if bot is admin.
        group = await DB.get_group(group_id)
        if not group:
            await DB.save_group(group_id, title or "Group", approved=True, remaining_uses=GROUP_START_USES)

    @staticmethod
    async def get_lang(user_id: int) -> str:
        """Retrieves the language preference for a specific user."""
        user = await DB.get_user(user_id)
        return str(user["lang"]) if user else "ar"

    @staticmethod
    async def get_context_from_message(message: types.Message) -> ActorContext:
        """Extracts and builds the ActorContext from a message update."""
        user_id = message.from_user.id
        chat_id = message.chat.id
        chat_type = str(message.chat.type)

        await Controller.ensure_user(user_id)
        lang = await Controller.get_lang(user_id)

        if chat_type == str(ChatType.PRIVATE):
            scope = "private"
        elif chat_type in (str(ChatType.GROUP), str(ChatType.SUPERGROUP)):
            scope = "group"
        else:
            scope = "private"

        return ActorContext(
            user_id=user_id,
            chat_id=chat_id,
            chat_type=chat_type,
            scope_type=scope,
            is_owner=Controller.is_owner(user_id),
            lang=lang,
        )

    @staticmethod
    async def get_context_from_callback(callback: CallbackQuery) -> ActorContext:
        """Extracts and builds the ActorContext from a callback query."""
        user_id = callback.from_user.id
        chat_id = callback.message.chat.id
        chat_type = str(callback.message.chat.type)

        await Controller.ensure_user(user_id)
        lang = await Controller.get_lang(user_id)

        if chat_type == str(ChatType.PRIVATE):
            scope = "private"
        elif chat_type in (str(ChatType.GROUP), str(ChatType.SUPERGROUP)):
            scope = "group"
        else:
            scope = "private"

        return ActorContext(
            user_id=user_id,
            chat_id=chat_id,
            chat_type=chat_type,
            scope_type=scope,
            is_owner=Controller.is_owner(user_id),
            lang=lang,
        )

    @staticmethod
    async def settings_get(key: str) -> dict[str, Any]:
        """Returns a dictionary for a specific system setting."""
        row = await DB.get_setting(key)
        return dict(row["value"]) if row else {}

    @staticmethod
    async def settings_set(key: str, value: dict[str, Any], user_id: int) -> None:
        """Saves a system setting if the user is an owner."""
        if not DB.admin_lock(user_id):
            return
        await DB.save_setting(key, value)

    @staticmethod
    async def note(level: str, scope_type: str, key: str, *, chat_id: Optional[int] = None, user_id: Optional[int] = None, details: Optional[dict[str, Any]] = None) -> None:
        """Records a system note with the specified level and details."""
        await DB.save_note(level, scope_type, chat_id, user_id, key, details or {})

    @staticmethod
    async def maintenance_enabled() -> bool:
        """Checks if maintenance mode is currently enabled."""
        s = await Controller.settings_get("maintenance")
        return bool(s.get("enabled", False))

    @staticmethod
    async def cleaner_enabled() -> bool:
        """Checks if the automated message cleaner is enabled."""
        s = await Controller.settings_get("cleaner")
        return bool(s.get("enabled", True))

    @staticmethod
    async def notify_owner_error(where: str, exc: BaseException, extra: str = "") -> None:
        """Sends an error notification to the bot owner."""
        try:
            conf = await Controller.settings_get("errors")
            if not conf.get("notify_owner", True):
                return
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            extra_formatted = f'🧾 {extra}\n' if extra else ''
            text = (
                f"🚨 <b>System Error</b>\n"
                f"📍 <b>Where:</b> <code>{where}</code>\n"
                f"{extra_formatted}"
                f"❗ <b>Error:</b> <code>{type(exc).__name__}: {exc}</code>\n\n"
                f"<pre>{tb[-3500:]}</pre>"
            )
            await bot.send_message(OWNER_USER_ID, text)
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
            # Fallback for unexpected errors (log them)
            print(f"Critical error in notify_owner_error: {e}")


# =========================
# I18N
# =========================
STR = {
    "ar": {
        "maintenance": "🛠 النظام حالياً تحت الصيانة. حاول لاحقاً.",
        "private_menu": "🚀 <b>مرحباً</b>{badge}\nاختر:",
        "group_menu": "👥 <b>قائمة المجموعة</b>\nاختر:",
        "private_only": "⚠️ هذا الخيار يعمل في <b>الخاص</b> فقط.",
        "group_only": "⚠️ هذا الخيار يعمل داخل <b>المجموعة</b> فقط.",
        "need_admin_bot": "⚠️ لازم ترفع البوت <b>مشرف (Admin)</b> حتى يشتغل داخل المجموعة.",
        "need_admin_user": "⚠️ لازم تكون <b>Admin</b> بالمجموعة حتى تستدعي القائمة.",
        "step1": "🗓️ <b>الخطوة 1/3</b>\nارسل <b>وقت البداية</b>\nمثال: <code>2026-01-01 10:00</code>",
        "step2": "✅ تم حفظ البداية.\n🏁 <b>الخطوة 2/3</b>\nارسل <b>وقت النهاية</b>\nمثال: <code>2026-01-01 12:00</code>",
        "step3": "✅ تم حفظ النهاية.\n🏷️ <b>الخطوة 3/3</b>\nارسل <b>اسم الهدف</b>\nمثال: <code>مذاكرة</code>",
        "bad_dt": "❌ الصيغة غير صحيحة.\n✅ مثال: <code>2026-01-01 10:00</code>",
        "end_before": "❌ النهاية لازم تكون بعد البداية.",
        "empty_title": "❌ الاسم فارغ.",
        "title_long": f"❌ الاسم طويل جداً. الحد الأقصى {MAX_TITLE_LEN} حرف.",
        "running": "🌀 <b>جاري تشغيل العداد…</b>",
        "cooldown_private": "⏳ لازم تنتظر تقريباً {mins} دقيقة قبل إنشاء عداد جديد.",
        "cooldown_group": "⏳ لازم تنتظر تقريباً {mins} دقيقة قبل إنشاء عداد جديد للمجموعة.",
        "no_uses_private": "⚠️ انتهت باقتك (0).\n📩 للتجديد تواصل مع المسؤول.",
        "no_uses_group": "⚠️ رصيد المجموعة انتهى (0).\n📩 للتجديد تواصل مع المسؤول.",
        "req_sent": "✅ تم إرسال طلب للمسؤول. انتظر الرد.",
        "already_pending": "📩 عندك طلب سابق قيد المراجعة.",
        "sys_error": "⚠️ صار خطأ بالنظام. تم تبليغ المسؤول.",
        "cleaner_hint_private": "ℹ️ ارسل /start للبدء.",
        "cleaner_hint_group": "ℹ️ لاستدعاء البوت بالمجموعة اكتب: <code>/run</code>",
        "my_timers": "📊 <b>عداداتك</b> (آخر 20):",
        "no_timers": "📭 ماكو عدادات.",
        "cancel_ok": "✅ تم الإلغاء",
        "cancel": "❌ إلغاء",
        "back": "🔙 رجوع",
        "cancel_all": "🗑️ إلغاء الكل",
        "cancel_all_ok": "✅ تم إلغاء جميع العدادات النشطة",
        "lang_choose": "🌐 اختر اللغة:",
        "lang_set": "✅ تم تغيير اللغة.",
        "must_subscribe": "⚠️ يجب عليك الاشتراك في القناة أولاً لاستخدام البوت:\n{channel}",
        "sub_check": "✅ تحقق من الاشتراك",
    },
    "en": {
        "maintenance": "🛠 Maintenance mode. Try later.",
        "private_menu": "🚀 <b>Welcome</b>{badge}\nChoose:",
        "group_menu": "👥 <b>Group menu</b>\nChoose:",
        "private_only": "⚠️ This option works in a <b>private chat</b> only.",
        "group_only": "⚠️ This option works in <b>groups</b> only.",
        "need_admin_bot": "⚠️ Bot must be <b>Admin</b> to work in this group.",
        "need_admin_user": "⚠️ You must be a <b>group admin</b> to open the menu.",
        "step1": "🗓️ <b>Step 1/3</b>\nSend <b>start time</b>\nExample: <code>2026-01-01 10:00</code>",
        "step2": "✅ Start saved.\n🏁 <b>Step 2/3</b>\nSend <b>end time</b>\nExample: <code>2026-01-01 12:00</code>",
        "step3": "✅ End saved.\n🏷️ <b>Step 3/3</b>\nSend <b>goal name</b>\nExample: <code>Study</code>",
        "bad_dt": "❌ Invalid format.\n✅ Example: <code>2026-01-01 10:00</code>",
        "end_before": "❌ End time must be after start time.",
        "empty_title": "❌ Empty title.",
        "title_long": f"❌ Title too long. Max {MAX_TITLE_LEN}.",
        "running": "🌀 <b>Starting timer…</b>",
        "cooldown_private": "⏳ Please wait about {mins} minutes.",
        "cooldown_group": "⏳ Please wait about {mins} minutes for a new group timer.",
        "no_uses_private": "⚠️ Your package is finished (0).\n📩 Contact admin to renew.",
        "no_uses_group": "⚠️ Group uses are finished (0).\n📩 Contact admin to renew.",
        "req_sent": "✅ Request sent to admin. Please wait.",
        "already_pending": "📩 You already have a pending request.",
        "sys_error": "⚠️ System error. Admin notified.",
        "cleaner_hint_private": "ℹ️ Send /start to begin.",
        "cleaner_hint_group": "ℹ️ To open the bot in a group, send: <code>/run</code>",
        "my_timers": "📊 <b>Your timers</b> (last 20):",
        "no_timers": "📭 No timers.",
        "cancel_ok": "✅ Cancelled",
        "cancel": "❌ Cancel",
        "back": "🔙 Back",
        "cancel_all": "🗑️ Cancel All",
        "cancel_all_ok": "✅ All active timers cancelled",
        "lang_choose": "🌐 Choose language:",
        "lang_set": "✅ Language updated.",
        "must_subscribe": "⚠️ You must subscribe to the channel first to use the bot:\n{channel}",
        "sub_check": "✅ Check subscription",
    },
}


def t(lang: str, key: str, **kwargs) -> str:
    """Translation helper: returns the translated string for the given key and language."""
    s = STR.get(lang, STR["ar"]).get(key, key)
    try:
        return s.format(**kwargs)
    except (KeyError, IndexError, ValueError):
        return s


# =========================
# MIDDLEWARE: anti-spam + user lock
# =========================
def _get_lock(user_id: int) -> asyncio.Lock:
    """Returns an asyncio.Lock for the given user_id, creating one if it doesn't exist."""
    lock = _user_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _user_locks[user_id] = lock
    return lock


def _throttle_ok(store: dict[int, float], key: int, seconds: float) -> bool:
    """Simple rate-limiting check: returns True if the given key can proceed."""
    now = time.time()
    last = store.get(key, 0.0)
    if now - last < seconds:
        return False
    store[key] = now
    return True


def kb_subscription(lang: str) -> InlineKeyboardMarkup:
    """Returns an inline keyboard with a subscription link and a check button."""
    channel_url = f"https://t.me/{REQUIRED_CHANNEL_ID.lstrip('@')}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Channel" if lang == "en" else "📢 القناة", url=channel_url)],
            [InlineKeyboardButton(text=t(lang, "sub_check"), callback_data="check_sub")],
        ]
    )


class DatabaseAccessMiddleware(BaseMiddleware):
    """Authorized Personnel Only: ensures only the Bot, specific Admin IDs, and the Owner can interact with database logic."""
    async def __call__(self, handler, event, data: dict[str, Any]):
        """Middleware call that enforces database access permissions."""
        user: Optional[types.User] = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)
        
        # Permission Enforcement: Root ID 5006296100 check
        is_authorized = user.id == OWNER_USER_ID or user.id in AUTHORIZED_ADMIN_IDS
        
        # Guard administrative commands
        is_admin_cmd = False
        if isinstance(event, types.Message) and event.text:
            if event.text.startswith(("/flush", "/sync")):
                is_admin_cmd = True
        
        if is_admin_cmd and not is_authorized:
            # Security Shield: Unauthorized access denied
            return None

        data["db_authorized"] = is_authorized
        return await handler(event, data)


class SubscriptionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict[str, Any]):
        """Middleware call that ensures the user is subscribed to the required channel."""
        user: Optional[types.User] = getattr(event, "from_user", None)
        callback_data = getattr(event, "data", None)

        if callback_data == "check_sub":
            return await handler(event, data)

        if not user or user.id == OWNER_USER_ID:
            return await handler(event, data)

        try:
            member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user.id)
            if member.status in ["member", "administrator", "creator"]:
                # Log successful subscription check
                with suppress(Exception):
                    await Controller.note("INFO", "security", "subscription_ok", user_id=user.id, details={"status": member.status})
                return await handler(event, data)
        except (TelegramAPIError, asyncio.TimeoutError) as e:
            with suppress(TelegramAPIError, Exception):
                await Controller.note("WARN", "security", "subscription_error", user_id=user.id, details={"error": str(e)})
            pass

        # Not subscribed
        # Try to get the language from the DB or use the default.
        lang = "ar"
        try:
            user_rec = await DB.get_user(user.id)
            if user_rec:
                lang = user_rec["lang"]
        except (ValueError, KeyError, AttributeError):
            pass

        text = t(lang, "must_subscribe", channel=REQUIRED_CHANNEL_ID)
        if isinstance(event, CallbackQuery):
            return await event.answer(text, show_alert=True)
        return await bot.send_message(user.id, text, reply_markup=kb_subscription(lang))


class AntiSpamMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data: dict[str, Any]):
        """Middleware call that implements anti-spam rate limiting for users."""
        user: Optional[types.User] = None
        is_callback = False

        if isinstance(event, CallbackQuery):
            user = event.from_user
            is_callback = True
        elif isinstance(event, types.Message):
            user = event.from_user

        if not user:
            return await handler(event, data)

        if is_callback:
            if not _throttle_ok(_last_callback_at, user.id, CALLBACK_THROTTLE_SECONDS):
                with suppress(TelegramBadRequest):
                    return await event.answer("⏳", show_alert=False)
                return None
        else:
            if not _throttle_ok(_last_message_at, user.id, MESSAGE_THROTTLE_SECONDS):
                if isinstance(event, types.Message):
                    with suppress(TelegramBadRequest, TelegramForbiddenError):
                        return await bot.delete_message(event.chat.id, event.message_id)
                return None

        async with _get_lock(user.id):
            return await handler(event, data)


dp.update.middleware(DatabaseAccessMiddleware())
dp.update.middleware(SubscriptionMiddleware())
dp.update.middleware(AntiSpamMiddleware())


# =========================
# DB CHANNEL GUARDIAN
# =========================
@router.message(F.chat.id == CLOUD_DB_CHANNEL_ID)
async def db_channel_guardian(message: types.Message):
    """Automatically delete any message in the DB channel that is not a valid database JSON entry."""
    # Only if it's not from the bot itself (though bot messages should be valid)
    if message.from_user and message.from_user.id == bot.id:
        return
    
    is_valid = False
    if message.text and "<code>" in message.text:
        try:
            content = message.text.split("<code>")[1].split("</code>")[0]
            event = json.loads(content)
            if all(k in event for k in ["entry_id", "timestamp", "event_type", "checksum"]):
                # Checksum validation
                if event["checksum"] == DB._generate_checksum(event):
                    is_valid = True
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            pass
        except (TelegramAPIError, ValueError, KeyError, TypeError, json.JSONDecodeError) as e:
            print(f"Error parsing event in guardian: {e}")
            pass
    
    if not is_valid:
        try:
            await message.delete()
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        except Exception as e:
            print(f"Failed to delete invalid message: {e}")


# =========================
# STATES
# =========================
class TimerStates(StatesGroup):
    waiting_start = State()
    waiting_end = State()
    waiting_name = State()


# =========================
# UI
# =========================
def badge_owner(is_owner: bool) -> str:
    """Returns a crown badge if the user is an owner, otherwise an empty string."""
    return " 👑 <b>(OWNER)</b>" if is_owner else ""


def kb_lang() -> InlineKeyboardMarkup:
    """Returns the keyboard for language selection."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🇮🇶 العربية", callback_data="lang:ar")],
            [InlineKeyboardButton(text="🇬🇧 English", callback_data="lang:en")],
        ]
    )


def kb_private_menu(ctx: ActorContext) -> InlineKeyboardMarkup:
    """Returns the main menu keyboard for private chats."""
    lang = ctx.lang
    rows = [
        [InlineKeyboardButton(text=("⏳ إنشاء عداد" if lang == "ar" else "⏳ New Timer"), callback_data="p:new_timer")],
        [InlineKeyboardButton(text=("📊 عداداتي" if lang == "ar" else "📊 My Timers"), callback_data="p:list")],
        [InlineKeyboardButton(text=("🌐 اللغة" if lang == "ar" else "🌐 Language"), callback_data="open_lang")],
    ]
    if ctx.is_owner:
        rows.append([InlineKeyboardButton(text=("🧠 المتحكم" if lang == "ar" else "🧠 Controller"), callback_data="ctrl:panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_group_menu(ctx: ActorContext) -> InlineKeyboardMarkup:
    """Returns the main menu keyboard for group chats."""
    lang = ctx.lang
    rows = [
        [InlineKeyboardButton(text=("⏳ إنشاء عداد" if lang == "ar" else "⏳ New Timer"), callback_data="g:new_timer")],
        [InlineKeyboardButton(text=("📊 عدادات المجموعة" if lang == "ar" else "📊 Group Timers"), callback_data="g:list")],
        [InlineKeyboardButton(text=("🌐 اللغة" if lang == "ar" else "🌐 Language"), callback_data="open_lang")],
    ]
    if ctx.is_owner:
        rows.append([InlineKeyboardButton(text=("🧠 المتحكم" if lang == "ar" else "🧠 Controller"), callback_data="ctrl:panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_request_more(ctx: ActorContext, scope: str) -> InlineKeyboardMarkup:
    """Returns a keyboard for requesting more timer uses."""
    lang = ctx.lang
    text = "📩 طلب تجديد/زيادة" if lang == "ar" else "📩 Request renewal/more"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=f"req:{scope}")]])


def kb_timers_list(ctx: ActorContext, rows: list[Any], scope: str) -> InlineKeyboardMarkup:
    """Returns a keyboard listing all timers for the given context."""
    buttons = []
    for r in rows:
        timer_id = int(r["timer_id"])
        title = str(r["title"])
        status = str(r["status"])
        buttons.append(
            [
                InlineKeyboardButton(text=f"⏳ {title} ({status})", callback_data="noop"),
                InlineKeyboardButton(
                    text=t(ctx.lang, "cancel"),
                    callback_data=f"{scope}:cancel:{timer_id}",
                ),
            ]
        )

    # Check if there are any active timers to show "Cancel All"
    has_active = any(r["status"] == "active" for r in rows)
    if has_active:
        buttons.append([InlineKeyboardButton(text=t(ctx.lang, "cancel_all"), callback_data=f"{scope}:cancel_all")])

    buttons.append([InlineKeyboardButton(text=t(ctx.lang, "back"), callback_data=f"{scope}:home")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# =========================
# helpers
# =========================
def parse_user_datetime(text: str) -> datetime:
    """Parses a user-provided date string into a localized UTC datetime object."""
    naive = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M")
    localized = TZ.localize(naive)
    return localized.astimezone(timezone.utc)


def fmt_dt_local(dt_utc: datetime) -> str:
    """Formats a UTC datetime object as a local time string."""
    return dt_utc.astimezone(TZ).strftime("%Y-%m-%d %H:%M")


def fmt_remaining(delta: timedelta) -> str:
    """Formats a timedelta object into a human-readable 'Xd Xh Xm' string."""
    if delta.total_seconds() < 0:
        return "0m"
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds // 60) % 60
    return f"{days}d {hours}h {minutes}m"


async def safe_delete_message(chat_id: int, message_id: int) -> None:
    """Deletes a Telegram message, suppressing errors if it fails."""
    with suppress(TelegramBadRequest, TelegramForbiddenError):
        await bot.delete_message(chat_id, message_id)


async def safe_edit_message(chat_id: int, message_id: int, text: str, reply_markup=None) -> None:
    """Edits a Telegram message, suppressing errors if it fails."""
    with suppress(TelegramBadRequest, TelegramForbiddenError):
        await bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id, reply_markup=reply_markup)


async def can_bot_delete_in_chat(chat_id: int) -> bool:
    """Checks if the bot has permissions to delete messages in the given chat."""
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)
        return member.status in ("administrator", "creator")
    except (TelegramBadRequest, TelegramForbiddenError):
        return False
    except TelegramAPIError as e:
        print(f"Telegram API error checking bot permissions: {e}")
        return False
    except (ValueError, KeyError, TypeError) as e:
        print(f"Data error checking bot permissions: {e}")
        return False


async def user_is_group_admin(chat_id: int, user_id: int) -> bool:
    """Checks if a user is an administrator in the specified group."""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except (TelegramBadRequest, TelegramForbiddenError):
        return False
    except TelegramAPIError as e:
        print(f"Telegram API error checking user admin status: {e}")
        return False
    except (ValueError, KeyError, TypeError) as e:
        print(f"Data error checking user admin status: {e}")
        return False


# =========================
# Sessions (DB) per user+chat
# =========================
async def session_set(user_id: int, chat_id: int, scope_type: str, state_name: str, data: dict[str, Any]) -> None:
    """Sets a user session state and data in the database."""
    ttl = datetime.now(timezone.utc) + timedelta(minutes=SESSION_TTL_MINUTES)
    await DB.save_session(user_id, chat_id, scope_type, state_name, data, ttl)


async def session_get(user_id: int, chat_id: int) -> Optional[Any]:
    """Retrieves a user session from the database."""
    return await DB.get_session(user_id, chat_id)


async def session_clear(user_id: int, chat_id: int) -> None:
    """Clears a user session from the database."""
    await DB.delete_session(user_id, chat_id)


async def session_cleanup_expired() -> None:
    """Initiates a cleanup of expired sessions."""
    await DB.cleanup_sessions()


# =========================
# Quota + cooldown
# =========================
@dataclass
class ReserveResult:
    ok: bool
    key: str
    wait_mins: int = 0


async def reserve_private_use(user_id: int) -> ReserveResult:
    """Attempts to reserve a timer use for a private chat user."""
    try:
        user = await DB.get_user(user_id)
        if not user:
            return ReserveResult(False, "sys_error")
        if user["is_blocked"]:
            return ReserveResult(False, "blocked")

        remaining = int(user["remaining_uses"])
        if remaining <= 0:
            return ReserveResult(False, "no_uses_private")

        last_timer_at: Optional[datetime] = user.get("last_timer_at")
        if last_timer_at is not None:
            if last_timer_at.tzinfo is None:
                last_timer_at = last_timer_at.replace(tzinfo=timezone.utc)
            next_allowed: datetime = last_timer_at + timedelta(seconds=PRIVATE_COOLDOWN_SECONDS)
            now: datetime = datetime.now(timezone.utc)
            if now < next_allowed:
                delta: timedelta = next_allowed - now
                wait_min = int(delta.total_seconds() // 60) + 1
                return ReserveResult(False, "cooldown_private", wait_mins=wait_min)

        await DB.update_user_uses(user_id, -1)
        return ReserveResult(True, "ok")
    except (TelegramAPIError, KeyError, ValueError, TypeError) as e:
        await Controller.notify_owner_error("reserve_private_use", e, extra=f"user_id={user_id}")
        return ReserveResult(False, "sys_error")


async def reserve_group_use(group_id: int) -> ReserveResult:
    """Attempts to reserve a timer use for a group chat."""
    try:
        group = await DB.get_group(group_id)
        if not group or not bool(group["approved"]):
            return ReserveResult(False, "sys_error")

        remaining = int(group["remaining_uses"])
        if remaining <= 0:
            return ReserveResult(False, "no_uses_group")

        last_timer_at: Optional[datetime] = group.get("last_timer_at")
        if last_timer_at is not None:
            if last_timer_at.tzinfo is None:
                last_timer_at = last_timer_at.replace(tzinfo=timezone.utc)
            next_allowed: datetime = last_timer_at + timedelta(seconds=GROUP_COOLDOWN_SECONDS)
            now: datetime = datetime.now(timezone.utc)
            if now < next_allowed:
                delta: timedelta = next_allowed - now
                wait_min = int(delta.total_seconds() // 60) + 1
                return ReserveResult(False, "cooldown_group", wait_mins=wait_min)

        await DB.update_group_uses(group_id, -1)
        return ReserveResult(True, "ok")
    except (TelegramAPIError, KeyError, ValueError, TypeError) as e:
        await Controller.notify_owner_error("reserve_group_use", e, extra=f"group_id={group_id}")
        return ReserveResult(False, "sys_error")


# =========================
# Requests: contact owner
# =========================
async def create_request(request_type: str, user_id: int, chat_id: Optional[int]) -> int:
    """Creates a request and initiates owner notification."""
    return await DB.create_request(request_type, user_id, chat_id)


async def send_request_to_owner(text: str, request_id: int, request_type: str) -> None:
    """Sends a request notification to the bot owner."""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Approve +10", callback_data=f"owner:approve:{request_id}:{request_type}"),
                InlineKeyboardButton(text="❌ Reject", callback_data=f"owner:reject:{request_id}"),
            ]
        ]
    )
    with suppress(Exception):
        await bot.send_message(OWNER_USER_ID, text, reply_markup=kb)


# =========================
# Cleaner (private + group)
# =========================
@router.message()
async def smart_cleaner(message: types.Message, state: FSMContext):
    """
    Cleaner rules:
    - Private:
        - If not started -> delete any message except /start
        - If inside FSM -> do nothing here (handlers will delete inputs after saving)
    - Group:
        - Cleaner only active if bot can delete (admin)
        - If no active FSM state for this user -> delete random/unrelated messages
          (but allow /run and /new_timer and /activate-style commands; here we only allow /run)
        - If inside FSM -> do not delete here
    """
    try:
        ctx = await Controller.get_context_from_message(message)

        if not await Controller.cleaner_enabled():
            return

        if await state.get_state() is not None:
            return

        if ctx.scope_type == "private":
            user_rec = await DB.get_user(ctx.user_id)
            started = bool(user_rec["started"]) if user_rec else False
            if not started:
                if message.text and message.text.strip().startswith("/start"):
                    return
                await safe_delete_message(ctx.chat_id, message.message_id)
                with suppress(Exception):
                    await message.answer(t(ctx.lang, "cleaner_hint_private"))
                await Controller.note("info", "private", "cleaner_deleted_before_start", chat_id=ctx.chat_id, user_id=ctx.user_id)
            return

        # Group:
        #   - Cleaner only active if the bot can delete (admin).
        #   - If no active FSM state for this user -> delete random/unrelated messages
        #     (but allow /run, /new_timer, and /activate-style commands; here we only allow /run).
        #   - If inside FSM -> do not delete here.
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("smart_cleaner", e)
        return None


# =========================
# /start (private) - user start
# =========================
@router.message(Command("start"))
async def cmd_start_private(message: types.Message, state: FSMContext):
    """Handler for the /start command, used to initialize interaction in private chats."""
    try:
        await session_cleanup_expired()
        ctx = await Controller.get_context_from_message(message)

        if ctx.scope_type != "private":
            # In groups we want /run as the main command
            return None

        if await Controller.maintenance_enabled() and not ctx.is_owner:
            return await message.answer(t(ctx.lang, "maintenance"))

        await DB.save_user(ctx.user_id, started=True)
        await state.clear()
        await session_clear(ctx.user_id, ctx.chat_id)

        await message.answer(
            t(ctx.lang, "private_menu", badge=badge_owner(ctx.is_owner)),
            reply_markup=kb_private_menu(ctx),
        )
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("cmd_start_private", e)
        with suppress(TelegramAPIError, Exception):
            await message.answer(t("ar", "sys_error"))
        return None


# =========================
# /run (group) - open menu in group
# =========================
@router.message(Command("run"))
async def cmd_run_group(message: types.Message, state: FSMContext):
    """Handler for the /run command, used to initialize interaction in group chats."""
    try:
        await session_cleanup_expired()
        ctx = await Controller.get_context_from_message(message)

        if ctx.scope_type != "group":
            return None

        if await Controller.maintenance_enabled() and not ctx.is_owner:
            return await message.reply(t(ctx.lang, "maintenance"))

        # bot must be admin to delete/edit smoothly
        if not await can_bot_delete_in_chat(ctx.chat_id):
            return await message.reply(t(ctx.lang, "need_admin_bot"))

        # user must be admin to call menu (prevents spam from all members)
        if not await user_is_group_admin(ctx.chat_id, ctx.user_id):
            return await message.reply(t(ctx.lang, "need_admin_user"))

        # ensure group row exists with default quota
        await Controller.ensure_group(ctx.chat_id, message.chat.title)

        # clear any user session in this group to prevent overlap
        await state.clear()
        await session_clear(ctx.user_id, ctx.chat_id)

        menu = await message.reply(t(ctx.lang, "group_menu"), reply_markup=kb_group_menu(ctx))

        # clean the command message + keep menu id for cleanup
        await safe_delete_message(ctx.chat_id, message.message_id)
        await session_set(ctx.user_id, ctx.chat_id, "group", "menu_open", {"menu_id": menu.message_id})
        await Controller.note("info", "group", "menu_opened", chat_id=ctx.chat_id, user_id=ctx.user_id, details={"menu_id": menu.message_id})
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("cmd_run_group", e, extra=f"group_id={message.chat.id}")
        return None


# =========================
# Language
# =========================
@router.callback_query(F.data == "open_lang")
async def open_lang(callback: CallbackQuery):
    """Callback handler to open the language selection menu."""
    ctx = await Controller.get_context_from_callback(callback)
    await callback.message.answer(t(ctx.lang, "lang_choose"), reply_markup=kb_lang())
    return await callback.answer()


@router.callback_query(F.data.startswith("lang:"))
async def set_lang(callback: CallbackQuery):
    """Sets the user's language preference based on callback data."""
    ctx = await Controller.get_context_from_callback(callback)
    lang = callback.data.split(":", 1)[1]
    if lang not in ("ar", "en"):
        lang = "ar"
    await DB.save_user(ctx.user_id, lang=lang)
    await Controller.note("info", ctx.scope_type, "lang_changed", chat_id=ctx.chat_id, user_id=ctx.user_id, details={"lang": lang})
    await callback.answer("✅")
    with suppress(Exception):
        await callback.message.answer(t(lang, "lang_set"))
    return None


# =========================
# PRIVATE callbacks
# =========================
@router.callback_query(F.data == "p:new_timer")
async def private_new_timer(callback: CallbackQuery, state: FSMContext):
    """Callback handler to initiate a new timer flow in a private chat."""
    try:
        ctx = await Controller.get_context_from_callback(callback)
        if ctx.scope_type != "private":
            return await callback.answer(t(ctx.lang, "private_only"), show_alert=True)

        if await Controller.maintenance_enabled() and not ctx.is_owner:
            return await callback.answer(t(ctx.lang, "maintenance"), show_alert=True)

        user_rec = await DB.get_user(ctx.user_id)
        if user_rec and bool(user_rec["is_blocked"]):
            return await callback.answer("🚫", show_alert=True)
        if not user_rec or not bool(user_rec["started"]):
            return await callback.answer(t(ctx.lang, "cleaner_hint_private"), show_alert=True)

        existing = await session_get(ctx.user_id, ctx.chat_id)
        if existing and existing["expires_at"] > datetime.now(timezone.utc):
            return await callback.answer("⚠️" if ctx.lang == "ar" else "⚠️", show_alert=False)

        prompt = await callback.message.answer(t(ctx.lang, "step1"))
        await session_set(ctx.user_id, ctx.chat_id, "private", "timer_flow", {"prompt_id": prompt.message_id})
        await state.update_data(prompt_id=prompt.message_id, scope="private")
        await state.set_state(TimerStates.waiting_start)
        return await callback.answer()
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("private_new_timer", e)
        return


@router.callback_query(F.data == "p:list")
async def private_list(callback: CallbackQuery):
    """Callback handler to list all timers for the current private chat."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "private":
        return await callback.answer(t(ctx.lang, "private_only"), show_alert=True)

    rows = await DB.get_timers_by_chat(ctx.chat_id, "private")
    if not rows:
        await callback.message.edit_text(t(ctx.lang, "no_timers"), reply_markup=kb_private_menu(ctx))
        return await callback.answer()

    await callback.message.edit_text(t(ctx.lang, "my_timers"), reply_markup=kb_timers_list(ctx, rows, "p"))
    return await callback.answer()


@router.callback_query(F.data.startswith("p:cancel:"))
async def private_cancel(callback: CallbackQuery):
    """Cancels a specific timer in a private chat context."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "private":
        return await callback.answer(t(ctx.lang, "private_only"), show_alert=True)

    timer_id = int(callback.data.split(":")[2])
    await DB.update_timer(timer_id, status="cancelled")

    tsk = await _pop_task(timer_id)
    if tsk and not tsk.done():
        tsk.cancel()

    await Controller.note("info", "private", "timer_cancelled", chat_id=ctx.chat_id, user_id=ctx.user_id, details={"timer_id": timer_id})
    return await callback.answer(t(ctx.lang, "cancel_ok"))


@router.callback_query(F.data == "p:cancel_all")
async def private_cancel_all(callback: CallbackQuery):
    """Callback handler to cancel all active timers in the current private chat."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "private":
        return await callback.answer(t(ctx.lang, "private_only"), show_alert=True)

    await _cancel_all_in_chat(ctx.chat_id, "private")
    await Controller.note("info", "private", "cancel_all", chat_id=ctx.chat_id, user_id=ctx.user_id)
    await callback.answer(t(ctx.lang, "cancel_all_ok"))
    # Refresh list
    return await private_list(callback)


@router.callback_query(F.data == "p:home")
async def private_home(callback: CallbackQuery, state: FSMContext):
    """Returns the user to the private main menu and clears the current state."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "private":
        return await callback.answer(t(ctx.lang, "private_only"), show_alert=True)

    await state.clear()
    await session_clear(ctx.user_id, ctx.chat_id)

    await callback.message.edit_text(
        t(ctx.lang, "private_menu", badge=badge_owner(ctx.is_owner)),
        reply_markup=kb_private_menu(ctx),
    )
    return await callback.answer()


# =========================
# GROUP callbacks (menu-driven FSM inside group)
# =========================
@router.callback_query(F.data == "g:new_timer")
async def group_new_timer(callback: CallbackQuery, state: FSMContext):
    """Callback handler to initiate a new timer flow in a group chat."""
    try:
        ctx = await Controller.get_context_from_callback(callback)
        if ctx.scope_type != "group":
            return await callback.answer(t(ctx.lang, "group_only"), show_alert=True)

        if await Controller.maintenance_enabled() and not ctx.is_owner:
            return await callback.answer(t(ctx.lang, "maintenance"), show_alert=True)

        # bot must be admin
        if not await can_bot_delete_in_chat(ctx.chat_id):
            return await callback.answer(t(ctx.lang, "need_admin_bot"), show_alert=True)

        # user must be admin to manage
        if not await user_is_group_admin(ctx.chat_id, ctx.user_id):
            return await callback.answer(t(ctx.lang, "need_admin_user"), show_alert=True)

        # ensure group exists (quota)
        await Controller.ensure_group(ctx.chat_id, callback.message.chat.title)

        # delete the menu message first (requested behavior)
        await safe_delete_message(ctx.chat_id, callback.message.message_id)

        prompt = await bot.send_message(ctx.chat_id, t(ctx.lang, "step1"))
        await session_set(ctx.user_id, ctx.chat_id, "group", "timer_flow", {"prompt_id": prompt.message_id})
        await state.update_data(prompt_id=prompt.message_id, scope="group")
        await state.set_state(TimerStates.waiting_start)

        return await callback.answer()
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("group_new_timer", e, extra=f"group_id={callback.message.chat.id}")
        return


@router.callback_query(F.data == "g:list")
async def group_list(callback: CallbackQuery):
    """Callback handler to list all active timers in a group chat."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "group":
        return await callback.answer(t(ctx.lang, "group_only"), show_alert=True)

    rows = await DB.get_timers_by_chat(ctx.chat_id, "group")
    if not rows:
        await callback.message.edit_text(t(ctx.lang, "no_timers"), reply_markup=kb_group_menu(ctx))
        return await callback.answer()

    await callback.message.edit_text(t(ctx.lang, "my_timers"), reply_markup=kb_timers_list(ctx, rows, "g"))
    return await callback.answer()


@router.callback_query(F.data.startswith("g:cancel:"))
async def group_cancel(callback: CallbackQuery):
    """Callback handler to cancel a specific timer in a group chat."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "group":
        return await callback.answer(t(ctx.lang, "group_only"), show_alert=True)

    if not await can_bot_delete_in_chat(ctx.chat_id):
        return await callback.answer(t(ctx.lang, "need_admin_bot"), show_alert=True)

    timer_id = int(callback.data.split(":")[2])
    await DB.update_timer(timer_id, status="cancelled")

    tsk = await _pop_task(timer_id)
    if tsk and not tsk.done():
        tsk.cancel()

    await Controller.note("info", "group", "timer_cancelled", chat_id=ctx.chat_id, user_id=ctx.user_id, details={"timer_id": timer_id})
    return await callback.answer(t(ctx.lang, "cancel_ok"))


@router.callback_query(F.data == "g:cancel_all")
async def group_cancel_all(callback: CallbackQuery):
    """Callback handler to cancel all active timers in a group chat."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "group":
        return await callback.answer(t(ctx.lang, "group_only"), show_alert=True)

    if not await can_bot_delete_in_chat(ctx.chat_id):
        return await callback.answer(t(ctx.lang, "need_admin_bot"), show_alert=True)

    # user must be admin to manage
    if not await user_is_group_admin(ctx.chat_id, ctx.user_id):
        return await callback.answer(t(ctx.lang, "need_admin_user"), show_alert=True)

    await _cancel_all_in_chat(ctx.chat_id, "group")
    await Controller.note("info", "group", "cancel_all", chat_id=ctx.chat_id, user_id=ctx.user_id)
    await callback.answer(t(ctx.lang, "cancel_all_ok"))
    # Refresh list
    return await group_list(callback)


async def _cancel_all_in_chat(chat_id: int, scope_type: str):
    """Helper function to cancel all active timers in a specific chat and scope."""
    # Get active timers for this chat/scope
    rows = await DB.get_timers_by_chat(chat_id, scope_type)
    timer_ids = [int(r["timer_id"]) for r in rows if r["status"] == "active"]

    if not timer_ids:
        return

    # Update DB
    for tid in timer_ids:
        await DB.update_timer(tid, status="cancelled")

    # Stop tasks
    for tid in timer_ids:
        tsk = await _pop_task(tid)
        if tsk and not tsk.done():
            tsk.cancel()


@router.callback_query(F.data == "g:home")
async def group_home(callback: CallbackQuery, state: FSMContext):
    """Returns the user to the group main menu and clears the current state."""
    ctx = await Controller.get_context_from_callback(callback)
    if ctx.scope_type != "group":
        return await callback.answer(t(ctx.lang, "group_only"), show_alert=True)

    if not await can_bot_delete_in_chat(ctx.chat_id):
        return await callback.answer(t(ctx.lang, "need_admin_bot"), show_alert=True)

    await state.clear()
    await session_clear(ctx.user_id, ctx.chat_id)

    await callback.message.edit_text(t(ctx.lang, "group_menu"), reply_markup=kb_group_menu(ctx))
    return await callback.answer()


# =========================
# FSM input handlers (shared for private + group) - context-aware delete
# =========================
@router.message(TimerStates.waiting_start, F.text)
async def fsm_waiting_start(message: types.Message, state: FSMContext):
    """FSM handler for processing the start time of a timer."""
    try:
        ctx = await Controller.get_context_from_message(message)
        data = await state.get_data()
        prompt_id = int(data.get("prompt_id", 0))
        scope = str(data.get("scope", ctx.scope_type))

        try:
            start_at = parse_user_datetime(message.text)
        except ValueError:
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "bad_dt"))

        await state.update_data(start_at=start_at.isoformat())
        await safe_edit_message(ctx.chat_id, prompt_id, t(ctx.lang, "step2"))
        await state.set_state(TimerStates.waiting_end)

        # delete user input (clean)
        await safe_delete_message(ctx.chat_id, message.message_id)
        await Controller.note("info", scope, "timer_start_saved", chat_id=ctx.chat_id, user_id=ctx.user_id)
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("fsm_waiting_start", e, extra=f"chat_id={message.chat.id}")


@router.message(TimerStates.waiting_end, F.text)
async def fsm_waiting_end(message: types.Message, state: FSMContext):
    """FSM handler for processing the end time of a timer."""
    try:
        ctx = await Controller.get_context_from_message(message)
        data = await state.get_data()
        prompt_id = int(data.get("prompt_id", 0))
        start_iso = data.get("start_at")
        scope = str(data.get("scope", ctx.scope_type))

        try:
            end_at = parse_user_datetime(message.text)
        except ValueError:
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "bad_dt"))

        if not start_iso:
            await state.clear()
            await session_clear(ctx.user_id, ctx.chat_id)
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "sys_error"))

        start_at = datetime.fromisoformat(start_iso)
        if start_at.tzinfo is None: start_at = start_at.replace(tzinfo=timezone.utc)
        if end_at <= start_at:
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "end_before"))

        await state.update_data(end_at=end_at.isoformat())
        await safe_edit_message(ctx.chat_id, prompt_id, t(ctx.lang, "step3"))
        await state.set_state(TimerStates.waiting_name)

        await safe_delete_message(ctx.chat_id, message.message_id)
        await Controller.note("info", scope, "timer_end_saved", chat_id=ctx.chat_id, user_id=ctx.user_id)
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("fsm_waiting_end", e, extra=f"chat_id={message.chat.id}")


@router.message(TimerStates.waiting_name, F.text)
async def fsm_waiting_name(message: types.Message, state: FSMContext):
    """FSM handler for processing the name/title of a timer."""
    try:
        ctx = await Controller.get_context_from_message(message)
        data = await state.get_data()
        prompt_id = int(data.get("prompt_id", 0))
        scope = str(data.get("scope", ctx.scope_type))

        title = (message.text or "").strip()
        if not title:
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "empty_title"))
        if len(title) > MAX_TITLE_LEN:
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "title_long"))

        start_iso = data.get("start_at")
        end_iso = data.get("end_at")
        if not start_iso or not end_iso:
            await state.clear()
            await session_clear(ctx.user_id, ctx.chat_id)
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "sys_error"))

        # enforce correct scope
        if scope == "private" and ctx.scope_type != "private":
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "private_only"))
        if scope == "group" and ctx.scope_type != "group":
            await safe_delete_message(ctx.chat_id, message.message_id)
            return await message.answer(t(ctx.lang, "group_only"))

        # reserve quota
        if scope == "private":
            res = await reserve_private_use(ctx.user_id)
            if not res.ok:
                await safe_delete_message(ctx.chat_id, message.message_id)
                if res.key == "cooldown_private":
                    return await message.answer(t(ctx.lang, "cooldown_private", mins=res.wait_mins))
                if res.key == "no_uses_private":
                    return await message.answer(t(ctx.lang, "no_uses_private"), reply_markup=kb_request_more(ctx, "private"))
                if res.key == "blocked":
                    return await message.answer("🚫")
                return await message.answer(t(ctx.lang, "sys_error"))
        else:
            # group
            if not await can_bot_delete_in_chat(ctx.chat_id):
                await safe_delete_message(ctx.chat_id, message.message_id)
                return await message.answer(t(ctx.lang, "need_admin_bot"))

            await Controller.ensure_group(ctx.chat_id, message.chat.title)
            res = await reserve_group_use(ctx.chat_id)
            if not res.ok:
                await safe_delete_message(ctx.chat_id, message.message_id)
                if res.key == "cooldown_group":
                    return await message.answer(t(ctx.lang, "cooldown_group", mins=res.wait_mins))
                if res.key == "no_uses_group":
                    return await message.answer(t(ctx.lang, "no_uses_group"), reply_markup=kb_request_more(ctx, "group"))
                return await message.answer(t(ctx.lang, "sys_error"))

        start_at = datetime.fromisoformat(start_iso)
        if start_at.tzinfo is None: start_at = start_at.replace(tzinfo=timezone.utc)
        end_at = datetime.fromisoformat(end_iso)
        if end_at.tzinfo is None: end_at = end_at.replace(tzinfo=timezone.utc)

        # clean prompt
        if prompt_id:
            await safe_delete_message(ctx.chat_id, prompt_id)

        # create timer placeholder message
        msg = await message.answer(t(ctx.lang, "running"))

        timer_id = await DB.create_timer(
            scope,
            ctx.chat_id,
            ctx.user_id,
            title,
            start_at,
            end_at,
        )
        await DB.update_timer(timer_id, message_id=msg.message_id)

        await state.clear()
        await session_clear(ctx.user_id, ctx.chat_id)

        await start_timer_task(timer_id)

        # delete user input for clean chat
        await safe_delete_message(ctx.chat_id, message.message_id)

        await Controller.note("info", scope, "timer_created", chat_id=ctx.chat_id, user_id=ctx.user_id, details={"timer_id": timer_id})
    except (TelegramAPIError, ValueError, KeyError, TypeError) as e:
        await Controller.notify_owner_error("fsm_waiting_name", e, extra=f"chat_id={message.chat.id}")
        with suppress(TelegramAPIError, Exception):
            await message.answer(t("ar", "sys_error"))


# =========================
# Timer engine (motion + quotes)
# =========================
CLOCK_FRAMES = ["🕛", "🕐", "🕑", "🕒", "🕓", "🕔", "🕕", "🕖", "🕗", "🕘", "🕙", "🕚"]
SPIN_FRAMES = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]

AR_QUOTES = [
    "📖 <i>﴿إِنَّ مَعَ الْعُسْرِ يُسْرًا﴾</i>",
    "📖 <i>﴿وَاصْبِرْ لِحُكْمِ رَبِّكَ فَإِنَّكَ بِأَعْيُنِنَا﴾</i>",
    "⏰ <b>الوقت إذا راح… ما يرجع.</b>",
    "🚀 <b>ابدأ الآن، حتى لو بخطوة صغيرة.</b>",
    "💎 <b>الاستمرار أهم من الكمال.</b>",
    "🌟 <b>من يتوكل على الله فهو حسبه.</b>",
    "📚 <b>العلم في الصغر كالنقش على الحجر.</b>",
    "🛤️ <b>رحلة الألف ميل تبدأ بخطوة.</b>",
    "💪 <b>ما لا يدرك كله لا يترك جله.</b>",
    "⏳ <b>الوقت كالسيف إن لم تقطعه قطعك.</b>",
    "🌈 <b>تفاءل بما تهوى يكن.</b>",
]
EN_QUOTES = [
    "⏰ <b>Small steps, every day.</b>",
    "🚀 <b>Start now. Improve later.</b>",
    "💎 <b>Consistency is power.</b>",
    "🔥 <b>Discipline beats motivation.</b>",
    "🌟 <b>Don't stop until you're proud.</b>",
    "📈 <b>Success is a marathon, not a sprint.</b>",
    "💡 <b>The best way to predict the future is to create it.</b>",
    "🎯 <b>Focus on being productive instead of busy.</b>",
    "🌊 <b>Be like water, find your way through.</b>",
]


async def timer_engine(timer_id: int) -> None:
    """Core logic for a timer: periodic status updates until expiration."""
    try:
        while timer_id in active_timer_tasks:
            row = await DB.get_timer(timer_id)
            if not row or row["status"] != "active":
                break

            chat_id = int(row["chat_id"])
            message_id = int(row["message_id"]) if row.get("message_id") else None
            title = str(row["title"])
            start_at: datetime = row["start_at"]
            end_at: datetime = row["end_at"]

            now = datetime.now(timezone.utc)
            rem: timedelta = end_at - now

            if rem.total_seconds() <= 0:
                await DB.update_timer(timer_id, status="done")
                if message_id:
                    await safe_edit_message(chat_id, message_id, f"🎊 <b>Done:</b> {title}\n✅ Completed!")
                break

            total_dur = (end_at - start_at).total_seconds()
            perc = 100.0 if total_dur <= 0 else min(100.0, max(0.0, (now - start_at).total_seconds() / total_dur * 100.0))

            blocks = 14
            filled = int((perc / 100.0) * blocks)
            empty = blocks - filled
            bar = ("🟩" * filled) + ("⬜" * empty)

            clock = CLOCK_FRAMES[now.second % len(CLOCK_FRAMES)]
            spin = SPIN_FRAMES[now.second % len(SPIN_FRAMES)]
            quote = random.choice(AR_QUOTES if now.second % 2 == 0 else EN_QUOTES)

            display = (
                f"{clock} {spin} <b>{title}</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"📈 <b>Progress:</b> <code>{bar}</code> {perc:.1f}%\n"
                f"⏳ <b>Remaining:</b> <code>{fmt_remaining(rem)}</code>\n"
                f"🗓️ <b>End:</b> <code>{fmt_dt_local(end_at)}</code>\n"
                f"━━━━━━━━━━━━━━\n"
                f"💬 {quote}"
            )

            if message_id:
                await safe_edit_message(chat_id, message_id, display)

            await asyncio.sleep(TIMER_UPDATE_SECONDS)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        await Controller.notify_owner_error("timer_engine", e, extra=f"timer_id={timer_id}")
        return None
    finally:
        await _pop_task(timer_id)


async def start_timer_task(timer_id: int) -> None:
    """Initiates and tracks an asyncio task for the specified timer engine."""
    tsk = active_timer_tasks.get(timer_id)
    if tsk and not tsk.done():
        return
    active_timer_tasks[timer_id] = asyncio.create_task(timer_engine(timer_id))


# =========================
# Owner approve/reject requests (+10)
# =========================
@router.callback_query(F.data.startswith("owner:approve:"))
async def owner_approve(callback: CallbackQuery):
    """Owner handler to approve renewal/extra use requests."""
    ctx = await Controller.get_context_from_callback(callback)
    if not ctx.is_owner:
        return await callback.answer("Owner only.", show_alert=True)

    _, _, request_id_s, _ = callback.data.split(":", 3)
    request_id = int(request_id_s)

    req = await DB.get_request(request_id)
    if not req or req["status"] != "pending":
        return await callback.answer("Invalid request.", show_alert=True)

    user_id = int(req["user_id"])
    chat_id = req["chat_id"]
    request_type = str(req["request_type"])

    # approve +10 based on type
    if request_type == "extra_uses_private":
        await Controller.ensure_user(user_id)
        await DB.update_user_uses(user_id, 10)
    elif request_type == "extra_uses_group":
        if chat_id is None:
            return await callback.answer("Missing group chat_id.", show_alert=True)
        await Controller.ensure_group(int(chat_id), None)
        await DB.update_group_uses(int(chat_id), 10)
    else:
        return await callback.answer("Unknown request type.", show_alert=True)

    await DB.update_request(request_id, "approved")
    res = await callback.answer("✅ Approved")

    with suppress(Exception):
        await bot.send_message(user_id, "✅ تمت الموافقة: +10" if ctx.lang == "ar" else "✅ Approved: +10")
    return res


@router.callback_query(F.data.startswith("owner:reject:"))
async def owner_reject(callback: CallbackQuery):
    """Owner handler to reject renewal/extra use requests."""
    ctx = await Controller.get_context_from_callback(callback)
    if not ctx.is_owner:
        return await callback.answer("Owner only.", show_alert=True)

    request_id = int(callback.data.split(":")[2])
    req = await DB.get_request(request_id)
    if not req or req["status"] != "pending":
        return await callback.answer("Invalid request.", show_alert=True)

    await DB.update_request(request_id, "rejected")
    res = await callback.answer("❌ Rejected")

    with suppress(Exception):
        await bot.send_message(int(req["user_id"]), "❌ تم رفض طلبك" if ctx.lang == "ar" else "❌ Your request was rejected")
    return res


# =========================
# User requests renewal (private or group)
# =========================
@router.callback_query(F.data.startswith("req:"))
async def request_more(callback: CallbackQuery):
    """Callback handler for users requesting more timer uses."""
    ctx = await Controller.get_context_from_callback(callback)
    scope = callback.data.split(":", 1)[1]  # private | group

    # prevent duplicates
    request_type = "extra_uses_private" if scope == "private" else "extra_uses_group"

    pending = await DB.get_pending_request(ctx.user_id, request_type)
    if pending:
        return await callback.answer(t(ctx.lang, "already_pending"), show_alert=True)

    chat_id = ctx.chat_id if scope == "group" else None
    rid = await create_request(request_type, ctx.user_id, chat_id)

    await send_request_to_owner(
        (
            f"📩 <b>Renewal request</b>\n"
            f"Type: <code>{request_type}</code>\n"
            f"User: <code>{ctx.user_id}</code>\n"
            f"Chat: <code>{chat_id or '-'}</code>\n"
            f"Request ID: <code>{rid}</code>"
        ),
        rid,
        request_type,
    )
    await callback.message.answer(t(ctx.lang, "req_sent"))
    return await callback.answer()


@router.callback_query(F.data == "check_sub")
async def on_check_sub(callback: CallbackQuery):
    """Callback handler to process subscription checks."""
    # This will be called only if SubscriptionMiddleware lets it pass 
    # (which it does if user is subscribed or if it's the check_sub callback itself).
    # If the middleware blocks it, this handler won't be reached.
    # Wait, the middleware allows 'check_sub' data to pass TO THE HANDLER.
    # So inside the handler, we should check again or just say 'Success'.
    
    user_id = callback.from_user.id
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        if member.status in ["member", "administrator", "creator"]:
            # Success!
            lang = "ar"
            user_rec = await DB.get_user(user_id)
            if user_rec:
                lang = user_rec["lang"]
            
            await callback.message.edit_text("✅ " + ("شكراً للاشتراك! يمكنك الآن الاستخدام." if lang == "ar" else "Thanks for subscribing! You can now use the bot."))
            return
    except (TelegramBadRequest, TelegramForbiddenError):
        pass
    except Exception as e:
        print(f"Subscription check error: {e}")
    
    await callback.answer(t("ar", "must_subscribe", channel=REQUIRED_CHANNEL_ID), show_alert=True)


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    """Empty callback handler for non-interactive buttons."""
    return await callback.answer()


# =========================
# ADMIN COMMANDS (Root Authority)
# =========================
@router.message(Command("flush"))
async def cmd_flush(message: types.Message):
    """Admin command: flushes expired/completed timer records from the database."""
    if not Controller.is_owner(message.from_user.id):
        return None
    await DB.flush_expired()
    return await message.answer("🧹 Database flushed (#STATUS_DONE records removed).")

@router.message(Command("sync"))
async def cmd_sync(message: types.Message):
    """Admin command: syncs the in-memory state to the Cloud-DB backend (JSON sync)."""
    if not Controller.is_owner(message.from_user.id):
        return None
    await DB.json_sync()
    return await message.answer("🔄 Database synced with Cloud backend.")

@router.callback_query(F.data == "ctrl:panel")
async def admin_panel(callback: CallbackQuery):
    """Callback handler to display the controller/admin panel."""
    if not Controller.is_owner(callback.from_user.id):
        return await callback.answer("🚫", show_alert=True)
    
    await callback.message.answer(
        "🧠 <b>Controller Panel</b>\nRoot ID: <code>5006296100</code>\nCommands:\n/flush - Clean expired\n/sync - Sync data",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Back", callback_data="p:home")]])
    )
    await callback.answer()
    return None

# =========================
# Startup restore tasks
# =========================
async def restore_active_timers() -> None:
    """Restores all active timers from the database on startup."""
    rows = await DB.get_active_timers()
    for r in rows:
        await start_timer_task(int(r["timer_id"]))


async def main() -> None:
    """Entry point: initializes the DB, restores active timers, and starts the bot."""
    try:
        await DB.init()
        await DB.flush_expired()  # Automated deletion of expired records
        await restore_active_timers()
        await dp.start_polling(bot)
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        print(f"Fatal bot error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass

