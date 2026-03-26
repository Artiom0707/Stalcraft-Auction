"""
Stalcraft Artefact Monitor — EU  v3.0
=======================================
pip install requests matplotlib pillow

Запуск:
  python stalcraft_monitor.py           — старт
  python stalcraft_monitor.py --update  — перекачать базу предметов
  python stalcraft_monitor.py --debug-lot ITEM_ID
"""

import sys, io, json, time, zipfile, logging, threading, argparse
from datetime import datetime, timezone, timedelta
from statistics import mean
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

import requests

from dotenv import load_dotenv
import os

load_dotenv()

try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.ticker as mticker
    MPL = True
except ImportError:
    MPL = False

# ═══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════════

STALCRAFT_TOKEN = os.getenv("STALCRAFT_TOKEN")
REGION             = "EU"
REALM              = "global"          # EU → global  |  RU → ru
BASE_URL           = f"https://eapi.stalcraft.net/{REGION}"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")               # токен от @BotFather
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")               # твой chat_id (алерты сюда)

DEFAULT_DISCOUNT_PCT   = 10
DEFAULT_MIN_PRICE_DIFF = 5_000
DEFAULT_MIN_SALES      = 3

POLL_INTERVAL  = 90
REQUEST_DELAY  = 0.7
WEB_PORT       = 8765

GITHUB_ZIP    = "https://github.com/EXBO-Studio/stalcraft-database/archive/refs/heads/main.zip"
GITHUB_COMMIT = "https://api.github.com/repos/EXBO-Studio/stalcraft-database/commits/main"
CACHE_FILE    = "artefact_cache.json"
DB_FILE       = "stalcraft_db.json"

# ── Качество ────────────────────────────────────────────────────
QUALITY = {
    0: ("Обычный",        "#939393"),
    1: ("Необычный",      "#4ad94b"),
    2: ("Особый",         "#5555ff"),
    3: ("Редкий",         "#940394"),
    4: ("Исключительный", "#d14849"),
    5: ("Легендарный",    "#ffaa00"),
}
QUALITY_CIRCLE = {0: "⚪", 1: "🟢", 2: "🔵", 3: "🟣", 4: "🔴", 5: "🟡"}
COLOR_TO_QLT = {
    "ARTEFACT_JUNK": 0, "ARTEFACT_COMMON": 1, "ARTEFACT_UNCOMMON": 2,
    "ARTEFACT_RARE": 3, "ARTEFACT_EPIC": 4, "ARTEFACT_LEGENDARY": 5,
    "artefact_junk": 0, "artefact_common": 1, "artefact_uncommon": 2,
    "artefact_rare": 3, "artefact_epic": 4, "artefact_legendary": 5,
}

# ── Группы заточки: 0 | 1-4 | 5-9 | 10-14 | 15 ─────────────────
PTN_GROUPS = ["0", "1-4", "5-9", "10-14", "15"]
PTN_LABELS = {
    "0":     "без заточки",
    "1-4":   "⚡ +1-4",
    "5-9":   "⚡ +5-9",
    "10-14": "⚡ +10-14",
    "15":    "⚡ +15",
}

def ptn_group(ptn) -> str:
    if ptn is None or ptn == 0: return "0"
    if ptn <= 4:  return "1-4"
    if ptn <= 9:  return "5-9"
    if ptn <= 14: return "10-14"
    return "15"

# ═══════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("stalcraft_monitor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════

class DB:
    def __init__(self):
        self._lock        = threading.Lock()
        self.active_lots  : dict = {}
        self.sales        : list = []
        self.watch_art    : set  = set()
        self.custom_items : dict = {}
        self.sent_alerts  : set  = set()
        self.settings     : dict = {
            "discount_pct":   DEFAULT_DISCOUNT_PCT,
            "min_price_diff": DEFAULT_MIN_PRICE_DIFF,
            "min_sales":      DEFAULT_MIN_SALES,
        }
        self._load()

    def _load(self):
        p = Path(DB_FILE)
        if not p.exists(): return
        try:
            raw = json.loads(p.read_text("utf-8"))
            self.active_lots  = raw.get("active_lots", {})
            self.sales        = raw.get("sales", [])
            self.watch_art    = set(raw.get("watch_art", []))
            self.custom_items = raw.get("custom_items", {})
            self.sent_alerts  = set(raw.get("sent_alerts", []))
            saved_s = raw.get("settings", {})
            self.settings.update({k: saved_s[k] for k in self.settings if k in saved_s})
            log.info(f"БД: {len(self.sales)} продаж, {len(self.active_lots)} лотов")
        except Exception as e:
            log.error(f"Загрузка БД: {e}")

    def save(self):
        with self._lock:
            payload = {
                "active_lots":  self.active_lots,
                "sales":        self.sales[-15_000:],
                "watch_art":    list(self.watch_art),
                "custom_items": self.custom_items,
                "sent_alerts":  list(self.sent_alerts)[-2000:],
                "settings":     self.settings,
                "saved_at":     _now(),
            }
        try:
            Path(DB_FILE).write_text(json.dumps(payload, ensure_ascii=False), "utf-8")
        except Exception as e:
            log.error(f"Сохранение БД: {e}")

    def get_setting(self, key):
        return self.settings.get(key)

    def set_setting(self, key, value):
        with self._lock:
            self.settings[key] = value
        self.save()

    def lot_key(self, lot: dict) -> str:
        raw = lot.get("id")
        if raw: return str(raw)
        spawn = lot.get("additional", {}).get("spawn_time", "")
        start = lot.get("startTime", "")
        return f"{start}_{spawn}" if (start or spawn) else ""

    def upsert(self, item_id: str, lot: dict, qlt: int, ptn):
        lk = self.lot_key(lot)
        if not lk: return
        key = f"{item_id}:{lk}"
        now = _now()
        with self._lock:
            prev = self.active_lots.get(key, {})
            self.active_lots[key] = {
                "item_id":    item_id,
                "lot_key":    lk,
                "qlt":        qlt,
                "ptn":        ptn,
                "ptn_grp":    ptn_group(ptn),
                "start":      lot.get("startPrice"),
                "buyout":     lot.get("buyoutPrice"),
                "amount":     max(int(lot.get("amount") or 1), 1),
                "end_time":   lot.get("endTime", ""),
                "first_seen": prev.get("first_seen", now),
                "last_seen":  now,
            }

    def detect_buyouts(self, item_id: str, cur: set) -> list:
        now_dt = datetime.now(timezone.utc)
        bought = []
        with self._lock:
            gone = [k for k, v in self.active_lots.items()
                    if v["item_id"] == item_id and v["lot_key"] not in cur]
            for key in gone:
                snap = self.active_lots.pop(key)
                if not _is_buyout(snap.get("end_time", ""), now_dt):
                    continue
                price = snap.get("buyout") or snap.get("start") or 0
                amt   = max(snap.get("amount", 1) or 1, 1)
                if price <= 0: continue
                sale = {
                    "item_id":    item_id,
                    "lot_key":    snap["lot_key"],
                    "qlt":        snap["qlt"],
                    "ptn":        snap["ptn"],
                    "ptn_grp":    snap["ptn_grp"],
                    "price":      price,
                    "unit_price": price / amt,
                    "amount":     amt,
                    "sold_at":    _now(),
                }
                self.sales.append(sale)
                bought.append(sale)
                ps = f" ⚡+{snap['ptn']}" if snap.get("ptn") else ""
                log.info(f"  ПРОДАЖА: {item_id} q{snap['qlt']}{ps} {price:,} руб.")
        return bought

    def avg_price(self, item_id: str, qlt: int, pg: str) -> Optional[tuple]:
        with self._lock:
            ps = [s["unit_price"] for s in self.sales
                  if s["item_id"] == item_id
                  and s["qlt"] == qlt
                  and s.get("ptn_grp", ptn_group(s.get("ptn"))) == pg]
        if not ps: return None
        return mean(ps[-30:]), len(ps)

    def avg_price_custom(self, item_id: str) -> Optional[tuple]:
        with self._lock:
            ps = [s["unit_price"] for s in self.sales if s["item_id"] == item_id]
        if not ps: return None
        return mean(ps[-30:]), len(ps)

    def sales_for(self, item_id: str) -> list:
        with self._lock:
            return [s for s in self.sales if s["item_id"] == item_id]

    def lots_for(self, item_id: str) -> list:
        with self._lock:
            return [v for v in self.active_lots.values() if v["item_id"] == item_id]

    def all_sales(self) -> list:
        with self._lock: return list(self.sales)

    def stats(self) -> dict:
        with self._lock:
            return {
                "total_sales":     len(self.sales),
                "active_lots":     len(self.active_lots),
                "items_with_data": len({s["item_id"] for s in self.sales}),
                "items_tracked":   len(self.watch_art) + len(self.custom_items),
                "oldest":          min((s["sold_at"] for s in self.sales), default=None),
            }

    def watch_add(self, iid):
        with self._lock: self.watch_art.add(iid)
        self.save()

    def watch_del(self, iid):
        with self._lock: self.watch_art.discard(iid)
        self.save()

    def custom_add(self, iid, name):
        with self._lock: self.custom_items[iid] = name
        self.save()

    def custom_del(self, iid):
        with self._lock: self.custom_items.pop(iid, None)
        self.save()

    def alert_sent(self, key: str) -> bool:
        return key in self.sent_alerts

    def mark_sent(self, key: str):
        with self._lock: self.sent_alerts.add(key)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _is_buyout(end_str: str, now_dt: datetime) -> bool:
    if not end_str: return False
    for parse in (
        lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")),
        lambda s: datetime.fromtimestamp(float(s), tz=timezone.utc),
    ):
        try:
            return now_dt < parse(end_str) + timedelta(seconds=60)
        except Exception: pass
    return False


db = DB()

# ═══════════════════════════════════════════════════════════════
#  БАЗА ПРЕДМЕТОВ
# ═══════════════════════════════════════════════════════════════

art_db  : dict = {}   # item_id -> {name, qlt, subcat}
item_db : dict = {}   # item_id -> {name, category}

_QUALITY_KEY = {"junk":0,"common":1,"uncommon":2,"rare":3,"epic":4,"legendary":5}

def _extract_qlt_json(data: dict) -> int:
    for block in data.get("infoBlocks", []):
        for elem in block.get("elements", []):
            if elem.get("type") != "key-value": continue
            ko = elem.get("key", {})
            if not isinstance(ko, dict): continue
            kid = ko.get("key", "")
            if kid.startswith("core.quality."):
                suffix = kid.split(".")[-1].lower()
                if suffix in _QUALITY_KEY:
                    return _QUALITY_KEY[suffix]
    return 0

def _latest_sha() -> Optional[str]:
    try:
        r = requests.get(GITHUB_COMMIT, headers={"Accept":"application/vnd.github+json"}, timeout=10)
        return r.json().get("sha") if r.ok else None
    except Exception: return None

def _load_cache() -> Optional[dict]:
    p = Path(CACHE_FILE)
    if p.exists():
        try: return json.loads(p.read_text("utf-8"))
        except Exception: pass
    return None

def load_art_db(force=False):
    global art_db, item_db
    cache = _load_cache()
    if cache and not force:
        sha = _latest_sha()
        if sha and sha == cache.get("sha"):
            art_db  = cache.get("art_db", {})
            item_db = cache.get("item_db", {})
            log.info(f"Кэш: {len(art_db)} артефактов, {len(item_db)} предметов всего")
            return
        log.info("Репозиторий обновился, скачиваем...")

    log.info("Скачиваем ZIP с GitHub...")
    try:
        r = requests.get(GITHUB_ZIP, timeout=90)
        r.raise_for_status()
        log.info(f"Скачано {len(r.content)//1024} КБ")
    except Exception as e:
        log.error(f"Скачивание: {e}")
        if cache:
            art_db  = cache.get("art_db", {})
            item_db = cache.get("item_db", {})
        return

    arts: dict  = {}
    items: dict = {}

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        for name in zf.namelist():
            if not name.endswith(".json"): continue
            parts = name.split("/")
            if len(parts) < 4: continue
            if parts[1] != REALM or parts[2] != "items": continue
            category = parts[3]
            item_id  = parts[-1].replace(".json", "")
            try:
                with zf.open(name) as jf:
                    data = json.load(jf)
            except Exception: continue

            lines   = data.get("name", {}).get("lines", {})
            display = lines.get("ru") or lines.get("en") or item_id
            items[item_id] = {"name": display, "category": category}

            if category == "artefact":
                qlt    = _extract_qlt_json(data)
                subcat = parts[-2] if parts[-2] != "artefact" else ""
                arts[item_id] = {"name": display, "qlt": qlt, "subcat": subcat}

    log.info(f"Артефактов: {len(arts)} | Предметов: {len(items)}")
    sha = _latest_sha() or "unknown"
    Path(CACHE_FILE).write_text(json.dumps(
        {"sha": sha, "updated_at": _now(), "art_db": arts, "item_db": items},
        ensure_ascii=False, indent=2), "utf-8")
    art_db  = arts
    item_db = items

def art_name(iid: str) -> str:
    return art_db.get(iid, {}).get("name", item_db.get(iid, {}).get("name", iid))

def item_name(iid: str) -> str:
    return item_db.get(iid, {}).get("name", art_db.get(iid, {}).get("name", iid))

def art_base_qlt(iid: str) -> int:
    return art_db.get(iid, {}).get("qlt", 0)

def search_arts(q: str) -> list:
    q = q.lower()
    return sorted([(iid, i["name"]) for iid, i in art_db.items()
                   if q in i["name"].lower()], key=lambda x: x[1])

def search_items(q: str) -> list:
    q = q.lower()
    return sorted([(iid, i["name"]) for iid, i in item_db.items()
                   if q in i["name"].lower()], key=lambda x: x[1])[:30]

# ═══════════════════════════════════════════════════════════════
#  API
# ═══════════════════════════════════════════════════════════════

def api_get(path: str, params=None) -> Optional[dict]:
    try:
        r = requests.get(f"{BASE_URL}{path}",
            headers={"Authorization": f"Bearer {STALCRAFT_TOKEN}"},
            params=params or {}, timeout=15)
        if r.status_code == 429:
            log.warning("Rate limit — 35 сек...")
            time.sleep(35)
            return api_get(path, params)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e: log.error(f"HTTP {e.response.status_code} {path}")
    except Exception as e: log.error(f"{path}: {e}")
    return None

def get_lots(item_id: str) -> list:
    d = api_get(f"/auction/{item_id}/lots", {"additional": "true", "limit": 200})
    return d.get("lots", []) if d else []

def extract_qlt_ptn(lot: dict, item_id: str) -> tuple:
    add = lot.get("additional") or {}
    qlt = add.get("qlt")
    ptn = add.get("ptn")
    if qlt is None: qlt = art_base_qlt(item_id)
    return (int(qlt) if qlt is not None else 0,
            int(ptn) if ptn is not None else None)

# ═══════════════════════════════════════════════════════════════
#  АЛЕРТЫ
# ═══════════════════════════════════════════════════════════════

def _fmt(v) -> str:
    if isinstance(v, (int, float)):
        return f"{int(v):,}".replace(",", "\u202f")
    return str(v)

def _build_alert(item_id, lot, qlt, ptn, pg, unit, avg, disc, n, is_art) -> str:
    circle = QUALITY_CIRCLE.get(qlt, "")
    qname  = QUALITY.get(qlt, ("?",""))[0]
    name   = art_name(item_id) if is_art else item_name(item_id)
    plabel = PTN_LABELS.get(pg, pg)
    header = "ВЫГОДНЫЙ ЛОТ — EU" if disc >= 20 else "СКИДКА НА АУКЦИОНЕ — EU"

    lines = [f"<b>{header}</b>", "━━━━━━━━━━━━━━━━━━━━━"]
    if is_art:
        lines += [
            f"{circle} <b>{name}</b>",
            f"⭐ Качество:  <b>{qname}</b>",
            f"⚡ Заточка:   <b>{plabel}</b>",
        ]
    else:
        lines.append(f"<b>{name}</b>")

    avg_label = f"Средняя {plabel}" if is_art else "Средняя"
    lines += [
        f"",
        f"💰 Цена:      <b>{_fmt(unit)}</b> руб./шт.",
        f"📊 {avg_label}: {_fmt(avg)} руб. ({n} прод.)",
        f"📉 Скидка:    <b>{disc:.1f}%</b>",
        f"",
        f"🕐 {datetime.now().strftime('%H:%M:%S')}",
        "━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)

def check_alert(item_id: str, lot: dict, qlt: int, ptn, is_art: bool = True):
    lk  = db.lot_key(lot)
    pg  = ptn_group(ptn)
    key = f"{item_id}:{lk}:q{qlt}:{pg}"
    if db.alert_sent(key): return

    s           = db.settings
    disc_pct    = s["discount_pct"]
    min_diff    = s["min_price_diff"]
    min_sales   = s["min_sales"]

    if is_art:
        res = db.avg_price(item_id, qlt, pg)
    else:
        res = db.avg_price_custom(item_id)
    if not res or res[1] < min_sales: return
    avg, n = res

    amt  = max(int(lot.get("amount") or 1), 1)
    raw  = lot.get("buyoutPrice") or lot.get("startPrice")
    if not raw: return
    unit = float(raw) / amt

    if not ((avg - unit) >= min_diff and unit <= avg * (1 - disc_pct / 100)):
        return

    disc = (1 - unit / avg) * 100
    msg  = _build_alert(item_id, lot, qlt, ptn, pg, unit, avg, disc, n, is_art)
    tg_send(msg, TELEGRAM_CHAT_ID)
    db.mark_sent(key)
    log.info(f"ALERT: {art_name(item_id)} q{qlt} {pg} -{disc:.1f}%")

# ═══════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════

def _tg(method: str, **kw) -> Optional[dict]:
    if not TELEGRAM_TOKEN: return None
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
                          json=kw, timeout=15)
        return r.json() if r.ok else None
    except Exception as e:
        log.error(f"TG {method}: {e}")
        return None

def tg_send(text, chat_id, markup=None):
    kw = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup: kw["reply_markup"] = markup
    return _tg("sendMessage", **kw)

def tg_edit(chat_id, msg_id, text, markup=None):
    kw = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": "HTML"}
    if markup: kw["reply_markup"] = markup
    return _tg("editMessageText", **kw)

def tg_answer(cid):
    _tg("answerCallbackQuery", callback_id=cid)

def tg_photo(chat_id, data, caption=""):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"photo": ("chart.png", data, "image/png")}, timeout=30)
    except Exception as e: log.error(f"TG photo: {e}")

def kb(*rows) -> dict:
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row]
                                 for row in rows]}

_tg_offset  = 0
_user_state : dict = {}

def _state(chat_id): return _user_state.get(chat_id, "")
def _set(chat_id, s): _user_state[chat_id] = s

# ── Меню ──────────────────────────────────────────────────────

def _main_kb():
    return kb(
        [("🔍 Артефакт",    "search_art"),
         ("📦 Любой предмет","search_item")],
        [("📋 Мой список",  "watchlist"),
         ("⚙️ Настройки",  "settings")],
        [("📊 Статус",      "status")],
    )

def _send_main(chat_id, msg_id=None):
    txt = "⚗️ <b>Stalcraft Monitor EU</b>\n\nВыбери действие:"
    if msg_id: tg_edit(chat_id, msg_id, txt, _main_kb())
    else:       tg_send(txt, chat_id, _main_kb())

# ── Артефакт ──────────────────────────────────────────────────

def _send_art(chat_id, msg_id, item_id):
    name  = art_name(item_id)
    in_wl = item_id in db.watch_art
    lines = [f"💎 <b>{name}</b>", f"<code>{item_id}</code>", ""]
    has   = False
    for qlt in range(6):
        circle = QUALITY_CIRCLE.get(qlt, "")
        qname  = QUALITY[qlt][0]
        rows_q = []
        for pg in PTN_GROUPS:
            res = db.avg_price(item_id, qlt, pg)
            if res:
                avg, n = res
                rows_q.append(f"   {PTN_LABELS[pg]}: {_fmt(avg)} руб. ({n})")
        if rows_q:
            has = True
            lines.append(f"{circle} <b>{qname}</b>")
            lines.extend(rows_q)
    if not has:
        lines.append("  Данных пока нет")
    markup = kb(
        [("🔕 Убрать" if in_wl else "🔔 Следить",
          f"{'unwatch' if in_wl else 'watch'}:{item_id}")],
        [("📈 График",          f"chart:{item_id}"),
         ("📋 Лоты",            f"lots:{item_id}")],
        [("🔥 Выгодные сейчас", f"deals:{item_id}"),
         ("⬅️ Назад",           "main")],
    )
    txt = "\n".join(lines)
    if msg_id: tg_edit(chat_id, msg_id, txt, markup)
    else:       tg_send(txt, chat_id, markup)

# ── Кастомный предмет ─────────────────────────────────────────

def _send_custom(chat_id, msg_id, item_id):
    name  = item_name(item_id)
    in_wl = item_id in db.custom_items
    lines = [f"📦 <b>{name}</b>", f"<code>{item_id}</code>", ""]
    res   = db.avg_price_custom(item_id)
    if res:
        avg, n = res
        lines.append(f"💰 Средняя: {_fmt(avg)} руб. ({n} прод.)")
    else:
        lines.append("  Данных пока нет")
    markup = kb(
        [("🔕 Убрать" if in_wl else "🔔 Следить",
          f"{'cust_del' if in_wl else 'cust_add'}:{item_id}")],
        [("📋 Лоты", f"lots:{item_id}"),
         ("⬅️ Назад", "main")],
    )
    txt = "\n".join(lines)
    if msg_id: tg_edit(chat_id, msg_id, txt, markup)
    else:       tg_send(txt, chat_id, markup)

# ── Лоты ──────────────────────────────────────────────────────

def _send_lots(chat_id, msg_id, item_id):
    lots   = db.lots_for(item_id)
    name   = art_name(item_id)
    is_art = item_id in art_db
    MAX_PER_GROUP = 3

    if not lots:
        back = f"art:{item_id}" if is_art else f"cust:{item_id}"
        tg_edit(chat_id, msg_id,
                f"Нет активных лотов для <b>{name}</b>.",
                kb([("⬅️", back)]))
        return

    lines = [f"📋 <b>{name}</b>\n"]

    if is_art:
        groups: dict = {}
        for lot in lots:
            q  = lot.get("qlt", 0)
            pg = lot.get("ptn_grp", "0")
            groups.setdefault(q, {}).setdefault(pg, []).append(lot)

        for qlt in sorted(groups):
            circle = QUALITY_CIRCLE.get(qlt, "")
            qname  = QUALITY.get(qlt, ("?",""))[0]
            lines.append(f"{circle} <b>{qname}</b>")
            for pg in PTN_GROUPS:
                pg_lots = groups[qlt].get(pg, [])
                if not pg_lots: continue
                sorted_pg = sorted(pg_lots, key=lambda x: x.get("buyout") or x.get("start") or 0)
                label = PTN_LABELS[pg]
                lines.append(f"  {label}:")
                for lot in sorted_pg[:MAX_PER_GROUP]:
                    amt = max(lot.get("amount", 1) or 1, 1)
                    pr  = lot.get("buyout") or lot.get("start") or 0
                    lines.append(f"    • {_fmt(pr // amt)} руб./шт.")
                if len(sorted_pg) > MAX_PER_GROUP:
                    lines.append(f"    <i>+ещё {len(sorted_pg)-MAX_PER_GROUP}</i>")
    else:
        sorted_lots = sorted(lots, key=lambda x: x.get("buyout") or x.get("start") or 0)
        for lot in sorted_lots[:20]:
            amt = max(lot.get("amount", 1) or 1, 1)
            pr  = lot.get("buyout") or lot.get("start") or 0
            lines.append(f"• {_fmt(pr // amt)} руб./шт.")
        if len(sorted_lots) > 20:
            lines.append(f"<i>+ещё {len(sorted_lots)-20}</i>")

    back = f"art:{item_id}" if is_art else f"cust:{item_id}"
    tg_edit(chat_id, msg_id, "\n".join(lines), kb([("⬅️", back)]))

# ── Выгодные сейчас ───────────────────────────────────────────

def _send_deals(chat_id, msg_id, item_id):
    lots    = db.lots_for(item_id)
    name    = art_name(item_id)
    is_art  = item_id in art_db
    s       = db.settings
    disc_pct  = s["discount_pct"]
    min_diff  = s["min_price_diff"]
    min_sales = s["min_sales"]

    deals = []
    for lot in lots:
        amt = max(lot.get("amount", 1) or 1, 1)
        raw = lot.get("buyout") or lot.get("start")
        if not raw: continue
        unit = float(raw) / amt
        qlt  = lot.get("qlt", 0)
        pg   = lot.get("ptn_grp", "0")
        res  = db.avg_price(item_id, qlt, pg) if is_art else db.avg_price_custom(item_id)
        if not res or res[1] < min_sales: continue
        avg, n = res
        if (avg - unit) >= min_diff and unit <= avg * (1 - disc_pct / 100):
            deals.append((round((1 - unit/avg)*100, 1), unit, avg, n, lot))

    if not deals:
        tg_edit(chat_id, msg_id,
                f"Выгодных предложений для <b>{name}</b> нет.\n"
                f"<i>Порог: -{disc_pct}% и -{_fmt(min_diff)} руб.</i>",
                kb([("⬅️", f"art:{item_id}")]))
        return

    deals.sort(key=lambda x: -x[0])
    lines = [f"🔥 <b>Выгодные лоты: {name}</b>\n"]
    for disc, unit, avg, n, lot in deals[:10]:
        circle = QUALITY_CIRCLE.get(lot.get("qlt", 0), "")
        qname  = QUALITY.get(lot.get("qlt", 0), ("?",""))[0]
        plabel = PTN_LABELS.get(lot.get("ptn_grp", "0"), "")
        lines.append(
            f"{circle} {qname} {plabel}\n"
            f"  {_fmt(unit)} руб. | скидка <b>{disc}%</b> | ср. {_fmt(avg)}"
        )
    tg_edit(chat_id, msg_id, "\n".join(lines), kb([("⬅️", f"art:{item_id}")]))

# ── Настройки ─────────────────────────────────────────────────

def _send_settings(chat_id, msg_id):
    s = db.settings
    txt = (
        f"⚙️ <b>Настройки алертов</b>\n\n"
        f"📉 Порог скидки:      <b>{s['discount_pct']}%</b>\n"
        f"💰 Мин. разница:      <b>{_fmt(s['min_price_diff'])} руб.</b>\n"
        f"📊 Мин. продаж:       <b>{s['min_sales']}</b>\n\n"
        f"<i>Алерт = оба условия выполнены одновременно.</i>"
    )
    markup = kb(
        [("📉 Изменить порог %",    "set:discount_pct"),
         ("💰 Изменить мин. разницу","set:min_price_diff")],
        [("📊 Изменить мин. продаж", "set:min_sales"),
         ("⬅️ Назад",               "main")],
    )
    tg_edit(chat_id, msg_id, txt, markup)

def _send_set_prompt(chat_id, msg_id, key):
    meta = {
        "discount_pct":   ("📉 Порог скидки (%)", "Число от 1 до 50", db.settings["discount_pct"]),
        "min_price_diff": ("💰 Мин. разница (руб.)", "Например: 5000", db.settings["min_price_diff"]),
        "min_sales":      ("📊 Мин. продаж", "Число от 1 до 20", db.settings["min_sales"]),
    }
    label, hint, cur = meta[key]
    tg_edit(chat_id, msg_id,
            f"<b>{label}</b>\nСейчас: <b>{cur}</b>\n\n{hint}:",
            kb([("❌ Отмена", "settings")]))
    _set(chat_id, f"set:{key}")

def _send_status(chat_id, msg_id=None):
    st = db.stats()
    s  = db.settings
    txt = (
        f"📊 <b>Статус</b>\n\n"
        f"Продаж:         <b>{st['total_sales']}</b>\n"
        f"Активных лотов: <b>{st['active_lots']}</b>\n"
        f"Предм. с данными: <b>{st['items_with_data']}</b>\n"
        f"Отслеживается:  <b>{st['items_tracked']}</b>\n"
        f"С: {(st['oldest'] or '—')[:19]}\n\n"
        f"⚙️ Порог: {s['discount_pct']}% + {_fmt(s['min_price_diff'])} руб.\n"
        f"🌐 Дашборд: http://localhost:{WEB_PORT}"
    )
    markup = kb([("⬅️ Назад", "main")])
    if msg_id: tg_edit(chat_id, msg_id, txt, markup)
    else:       tg_send(txt, chat_id, markup)

def _send_watchlist(chat_id, msg_id):
    arts  = list(db.watch_art)
    custs = list(db.custom_items.items())
    if not arts and not custs:
        tg_edit(chat_id, msg_id, "Список пуст.",
                kb([("🔍 Артефакт","search_art"),
                    ("📦 Предмет","search_item"),
                    ("⬅️","main")]))
        return
    rows = []
    for iid in arts:
        rows.append([(f"⚗️ {art_name(iid)}", f"art:{iid}")])
    for iid, nm in custs:
        rows.append([(f"📦 {nm}", f"cust:{iid}")])
    rows.append([("⬅️ Назад","main")])
    tg_edit(chat_id, msg_id, "📋 <b>Мой список:</b>", kb(*rows))

# ── Поиск ──────────────────────────────────────────────────────

def _handle_search_art(chat_id, query, msg_id=None):
    results = search_arts(query)
    if not results:
        _send_r(chat_id, msg_id, f"Не найдено: «{query}»", kb([("⬅️","main")]))
        return
    if len(results) == 1:
        _send_art(chat_id, msg_id, results[0][0]); return
    rows = [[(n, f"art:{iid}")] for iid, n in results[:15]]
    rows.append([("⬅️","main")])
    _send_r(chat_id, msg_id, f"🔍 Найдено {len(results)} артефактов:", kb(*rows))

def _handle_search_item(chat_id, query, msg_id=None):
    results = search_items(query)
    if not results:
        _send_r(chat_id, msg_id, f"Не найдено: «{query}»", kb([("⬅️","main")]))
        return
    if len(results) == 1:
        _send_custom(chat_id, msg_id, results[0][0]); return
    rows = [[(n, f"cust:{iid}")] for iid, n in results[:15]]
    rows.append([("⬅️","main")])
    _send_r(chat_id, msg_id, f"🔍 Найдено {len(results)} предметов:", kb(*rows))

def _send_r(chat_id, msg_id, txt, markup):
    if msg_id: tg_edit(chat_id, msg_id, txt, markup)
    else:       tg_send(txt, chat_id, markup)

# ── Обработка входящих ────────────────────────────────────────

def _handle_text(chat_id, text):
    state = _state(chat_id)
    _set(chat_id, "")

    if state == "search_art":
        _handle_search_art(chat_id, text)
    elif state == "search_item":
        _handle_search_item(chat_id, text)
    elif state.startswith("set:"):
        key = state[4:]
        try:
            val = float(text.replace(",",".").replace(" ",""))
            if key == "discount_pct":    val = max(1, min(50, int(val)))
            elif key == "min_price_diff": val = max(0, int(val))
            elif key == "min_sales":      val = max(1, min(20, int(val)))
            db.set_setting(key, val)
            tg_send(f"✅ Сохранено: <b>{val}</b>", chat_id,
                    kb([("⚙️ Настройки","settings_fresh")]))
        except Exception:
            tg_send("❌ Неверный формат. Введи число.", chat_id,
                    kb([("⚙️ Назад","settings_fresh")]))
    else:
        _send_main(chat_id)

def _handle_callback(chat_id, msg_id, data):
    if data == "main":            _send_main(chat_id, msg_id)
    elif data == "status":        _send_status(chat_id, msg_id)
    elif data == "watchlist":     _send_watchlist(chat_id, msg_id)
    elif data == "settings":      _send_settings(chat_id, msg_id)
    elif data == "settings_fresh":_send_settings(chat_id, msg_id)
    elif data == "search_art":
        _set(chat_id, "search_art")
        tg_edit(chat_id, msg_id, "🔍 Введи название артефакта:", kb([("⬅️","main")]))
    elif data == "search_item":
        _set(chat_id, "search_item")
        tg_edit(chat_id, msg_id, "📦 Введи название предмета:", kb([("⬅️","main")]))
    elif data.startswith("set:"):     _send_set_prompt(chat_id, msg_id, data[4:])
    elif data.startswith("art:"):     _send_art(chat_id, msg_id, data[4:])
    elif data.startswith("cust:"):    _send_custom(chat_id, msg_id, data[5:])
    elif data.startswith("lots:"):    _send_lots(chat_id, msg_id, data[5:])
    elif data.startswith("deals:"):   _send_deals(chat_id, msg_id, data[6:])
    elif data.startswith("watch:"):
        iid = data[6:]; db.watch_add(iid)
        tg_edit(chat_id, msg_id,
                f"✅ <b>{art_name(iid)}</b> добавлен.",
                kb([("📋 Список","watchlist"),("⬅️","main")]))
    elif data.startswith("unwatch:"):
        iid = data[8:]; db.watch_del(iid)
        tg_edit(chat_id, msg_id, f"🔕 Убран.",
                kb([("📋 Список","watchlist"),("⬅️","main")]))
    elif data.startswith("cust_add:"):
        iid = data[9:]; name = item_name(iid); db.custom_add(iid, name)
        tg_edit(chat_id, msg_id,
                f"✅ <b>{name}</b> добавлен.",
                kb([("📋 Список","watchlist"),("⬅️","main")]))
    elif data.startswith("cust_del:"):
        iid = data[9:]; db.custom_del(iid)
        tg_edit(chat_id, msg_id, f"🔕 Убран.",
                kb([("📋 Список","watchlist"),("⬅️","main")]))
    elif data.startswith("chart:"):
        iid = data[6:]
        tg_edit(chat_id, msg_id, f"⏳ Генерирую график {art_name(iid)}…",
                kb([("⬅️", f"art:{iid}")]))
        png = make_chart(iid)
        if png: tg_photo(chat_id, png, f"📈 {art_name(iid)}")
        else:   tg_send("Данных пока нет.", chat_id)
        _send_art(chat_id, None, iid)

def _handle_update(upd):
    global _tg_offset
    _tg_offset = upd["update_id"] + 1
    if "callback_query" in upd:
        cq      = upd["callback_query"]
        chat_id = str(cq["message"]["chat"]["id"])
        msg_id  = cq["message"]["message_id"]
        tg_answer(cq["id"])
        try: _handle_callback(chat_id, msg_id, cq["data"])
        except Exception as e: log.error(f"CB: {e}")
        return
    if "message" not in upd: return
    msg     = upd["message"]
    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text","").strip()
    if text.startswith("/start") or text.startswith("/menu"):
        _set(chat_id, ""); _send_main(chat_id)
    elif text.startswith("/status"):
        _send_status(chat_id)
    else:
        _handle_text(chat_id, text)

def tg_polling_loop():
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN не задан")
        return
    global _tg_offset
    log.info("🤖 Telegram бот запущен")
    sess = requests.Session()
    while True:
        try:
            r = sess.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                json={"offset": _tg_offset, "timeout": 25},
                timeout=30)
            if r.ok:
                for upd in r.json().get("result",[]):
                    try: _handle_update(upd)
                    except Exception as e: log.error(f"TG: {e}")
        except requests.exceptions.ReadTimeout: pass
        except Exception as e:
            log.error(f"TG polling: {e}"); time.sleep(5)

# ═══════════════════════════════════════════════════════════════
#  ГРАФИКИ
# ═══════════════════════════════════════════════════════════════

def make_chart(item_id: str) -> Optional[bytes]:
    if not MPL: return None
    sales = db.sales_for(item_id)
    if not sales: return None

    by_qlt: dict = {}
    for s in sales:
        by_qlt.setdefault(s["qlt"], []).append(s)

    active = sorted(by_qlt.keys())
    if not active: return None

    n = len(active)
    fig, axes = plt.subplots(n, 1, figsize=(12, 3.5 * n), squeeze=False)
    fig.patch.set_facecolor("#0a0c10")
    fig.subplots_adjust(hspace=0.5)

    for idx, qlt in enumerate(active):
        ax = axes[idx][0]
        ax.set_facecolor("#12161e")
        qname, qcolor = QUALITY[qlt]
        sales_q = by_qlt[qlt]

        by_pg: dict = {}
        for s in sales_q:
            pg = s.get("ptn_grp", ptn_group(s.get("ptn")))
            by_pg.setdefault(pg, []).append(s)

        alphas = {"0":0.4,"1-4":0.55,"5-9":0.7,"10-14":0.85,"15":1.0}
        for pg in PTN_GROUPS:
            if pg not in by_pg: continue
            pts = sorted(
                [(datetime.fromisoformat(s["sold_at"]), s["unit_price"])
                 for s in by_pg[pg]], key=lambda x: x[0])
            xs, ys = zip(*pts)
            ax.plot(xs, ys, "o-", color=qcolor,
                    label=PTN_LABELS[pg],
                    linewidth=1.5, markersize=4, alpha=alphas.get(pg, 0.7))

        ax.set_title(qname, color=qcolor, fontsize=10, pad=6, loc="left")
        ax.set_ylabel("руб./шт.", color="#4a5568", fontsize=8)
        ax.tick_params(colors="#4a5568", labelsize=7)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
        # Логарифмическая шкала если разброс цен > 10x
        vals = [s["unit_price"] for s in sales_q if s["unit_price"] > 0]
        if vals and max(vals) / min(vals) > 10:
            ax.set_yscale("log")
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _: f"{int(v):,}".replace(",", " ")))
        for sp in ax.spines.values(): sp.set_edgecolor("#1e2535")
        ax.grid(color="#1e2535", linewidth=0.5, alpha=0.6)
        ax.legend(facecolor="#1a2030", edgecolor="#1e2535",
                  labelcolor="#c8d8e8", fontsize=7, loc="upper left")
        fig.autofmt_xdate(rotation=25)

    title = art_name(item_id) if item_id in art_db else item_name(item_id)
    fig.suptitle(title, color="#c8d8e8", fontsize=12)
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=120, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()

# ═══════════════════════════════════════════════════════════════
#  МОНИТОРИНГ
# ═══════════════════════════════════════════════════════════════

def process_item(item_id: str, is_art: bool):
    lots = get_lots(item_id)
    cur  = set()
    for lot in lots:
        lk = db.lot_key(lot)
        if not lk: continue
        cur.add(lk)
        if is_art:
            qlt, ptn = extract_qlt_ptn(lot, item_id)
        else:
            qlt, ptn = 0, None
        db.upsert(item_id, lot, qlt, ptn)
    db.detect_buyouts(item_id, cur)
    for lot in lots:
        if is_art:
            qlt, ptn = extract_qlt_ptn(lot, item_id)
            check_alert(item_id, lot, qlt, ptn, True)
        else:
            check_alert(item_id, lot, 0, None, False)

def monitor_loop():
    log.info("=" * 58)
    log.info("  Stalcraft Monitor — EU  v3.0")
    log.info(f"  Порог: {db.settings['discount_pct']}% + {_fmt(db.settings['min_price_diff'])} руб.")
    log.info("=" * 58)
    cycle = 0
    while True:
        cycle += 1
        art_ids    = list(art_db.keys())
        custom_ids = list(db.custom_items.keys())
        total      = len(art_ids) + len(custom_ids)
        st         = db.stats()
        log.info(f"── #{cycle} | {total} предм. | продаж:{st['total_sales']} лотов:{st['active_lots']} ──")

        for i, iid in enumerate(art_ids, 1):
            log.info(f"  [{i}/{total}] {art_name(iid)} ({iid})")
            process_item(iid, True)
            time.sleep(REQUEST_DELAY)

        for j, iid in enumerate(custom_ids, len(art_ids)+1):
            log.info(f"  [{j}/{total}] {item_name(iid)} ({iid}) [custom]")
            process_item(iid, False)
            time.sleep(REQUEST_DELAY)

        if cycle % 5 == 0:
            db.save()
        log.info(f"Цикл #{cycle} готов. Следующий через {POLL_INTERVAL} сек.")
        time.sleep(POLL_INTERVAL)

# ═══════════════════════════════════════════════════════════════
#  ВЕБ-ДАШБОРД
# ═══════════════════════════════════════════════════════════════

_DASH = Path(__file__).parent / "dashboard.html"

class WebH(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        if self.path in ("/","/index.html"):
            if _DASH.exists():
                self._ok("text/html; charset=utf-8", _DASH.read_bytes())
            else:
                self._ok("text/plain", b"Put dashboard.html next to the script")
        elif self.path == "/api/data":
            sales = db.all_sales()
            arts  = sorted(
                [{"id":iid,"name":i["name"],"qlt":i["qlt"],
                  "sales_count":sum(1 for s in sales if s["item_id"]==iid)}
                 for iid,i in art_db.items()],
                key=lambda x:(-x["sales_count"],x["name"]))
            body = json.dumps({
                "stats":      db.stats(),
                "settings":   db.settings,
                "arts":       arts,
                "sales":      sales[-3000:],
                "lots":       list(db.active_lots.values()),
                "quality":    {str(k):{"name":v[0],"color":v[1]} for k,v in QUALITY.items()},
                "ptn_groups": PTN_LABELS,
            }, ensure_ascii=False, default=str).encode("utf-8")
            self._ok("application/json; charset=utf-8", body,
                     [("Access-Control-Allow-Origin","*")])
        elif self.path.startswith("/api/chart/"):
            png = make_chart(self.path.split("/")[-1])
            if png: self._ok("image/png", png)
            else:   self.send_response(404); self.end_headers()
        else:
            self.send_response(404); self.end_headers()

    def _ok(self, ct, body, extra=None):
        self.send_response(200)
        self.send_header("Content-Type", ct)
        for k, v in (extra or []): self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

def start_web():
    HTTPServer(("0.0.0.0", WEB_PORT), WebH).serve_forever()

# ═══════════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--update",    action="store_true")
    ap.add_argument("--debug-lot", metavar="ITEM_ID")
    args = ap.parse_args()

    if args.update:
        Path(CACHE_FILE).unlink(missing_ok=True)
        load_art_db(force=True)
        sys.exit(0)

    if args.debug_lot:
        if STALCRAFT_TOKEN == "ВАШ_ТОКЕН_ЗДЕСЬ":
            print("Задай STALCRAFT_TOKEN"); sys.exit(1)
        load_art_db()
        d = api_get(f"/auction/{args.debug_lot}/lots", {"additional":"true","limit":2})
        print(json.dumps(d, ensure_ascii=False, indent=2))
        sys.exit(0)

    if STALCRAFT_TOKEN == "ВАШ_ТОКЕН_ЗДЕСЬ":
        print("Задай STALCRAFT_TOKEN в настройках файла"); sys.exit(1)

    load_art_db()
    if not art_db:
        log.error("База пуста — запусти: python stalcraft_monitor.py --update")
        sys.exit(1)

    log.info(f"Артефактов: {len(art_db)} | Предметов: {len(item_db)}")
    threading.Thread(target=start_web,       daemon=True).start()
    threading.Thread(target=tg_polling_loop, daemon=True).start()
    log.info(f"Дашборд: http://localhost:{WEB_PORT}")

    try:
        monitor_loop()
    except KeyboardInterrupt:
        log.info("Остановлено.")
        db.save()