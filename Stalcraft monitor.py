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
from statistics import mean, median, stdev
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

DEFAULT_DISCOUNT_PCT   = 12
DEFAULT_MIN_PRICE_DIFF = 100_000
DEFAULT_MIN_SALES      = 3

POLL_INTERVAL  = 90
REQUEST_DELAY  = 0.7
WEB_PORT       = 8765
# Для WebApp в Telegram нужен HTTPS URL. Пропиши в .env или замени здесь:
WEB_APP_URL    = os.getenv('WEB_APP_URL', f'http://127.0.0.1:8765')

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
        self.user_settings : dict = {}  # chat_id -> {discount_pct, ...}
        self.excluded_lots : set  = set()  # lot_keys помеченные как "купил"
        self.vip_users     : set  = set()  # получают алерты без задержки
        self.subscribers   : set  = set()  # все подписавшиеся на алерты
        self.pending_alerts: list = []     # [(send_at, chat_id, msg, markup)]
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
            self.user_settings  = raw.get("user_settings", {})
            self.excluded_lots  = set(raw.get("excluded_lots", []))
            self.vip_users      = set(raw.get("vip_users", []))
            self.subscribers    = set(raw.get("subscribers", []))
            log.info(f"БД: {len(self.sales)} продаж, {len(self.active_lots)} лотов")
        except Exception as e:
            log.error(f"Загрузка БД: {e}")

    def save(self):
        with self._lock:
            payload = {
                "active_lots":   self.active_lots,
                "sales":         self.sales[-15_000:],
                "watch_art":     list(self.watch_art),
                "custom_items":  self.custom_items,
                "sent_alerts":   list(self.sent_alerts)[-2000:],
                "settings":      self.settings,
                "user_settings": self.user_settings,
                "excluded_lots": list(self.excluded_lots)[-5000:],
                "vip_users":     list(self.vip_users),
                "subscribers":   list(self.subscribers),
                "saved_at":      _now(),
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

    def get_user_settings(self, chat_id: str) -> dict:
        """Настройки конкретного пользователя (фолбэк на глобальные)."""
        base = dict(self.settings)
        base.update(self.user_settings.get(chat_id, {}))
        return base

    def set_user_setting(self, chat_id: str, key: str, value):
        with self._lock:
            self.user_settings.setdefault(chat_id, {})[key] = value
        self.save()

    def exclude_lot(self, lot_key: str):
        """Пометить лот как 'купил' — исключить из истории цен."""
        with self._lock:
            self.excluded_lots.add(lot_key)
        self.save()

    def vip_add(self, chat_id: str):
        with self._lock: self.vip_users.add(str(chat_id))
        self.save()

    def vip_del(self, chat_id: str):
        with self._lock: self.vip_users.discard(str(chat_id))
        self.save()

    def sub_add(self, chat_id: str):
        with self._lock: self.subscribers.add(str(chat_id))
        self.save()

    def sub_del(self, chat_id: str):
        with self._lock: self.subscribers.discard(str(chat_id))
        self.save()

    def is_vip(self, chat_id: str) -> bool:
        return str(chat_id) in self.vip_users

    def is_excluded(self, sale: dict) -> bool:
        return sale.get("lot_key","") in self.excluded_lots

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
                price = snap.get("buyout") or 0
                amt   = max(snap.get("amount", 1) or 1, 1)
                if price <= 0: continue  # без buyout = не считаем продажей
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
                  and s.get("ptn_grp", ptn_group(s.get("ptn"))) == pg
                  and s.get("lot_key","") not in self.excluded_lots]
        if not ps: return None
        recent = ps[-40:]  # чуть больше истории

        filtered = _filtered_prices(recent)

        # мало нормальных данных — игнор
        if len(filtered) < 3:
            return None

        # анти-манипуляция (резкий разброс)
        if len(filtered) >= 5:
            if max(filtered) > min(filtered) * 5:
                return None

        # "реальная цена" (не средняя, а ближе к покупкам)
        base_price = _percentile(filtered, 0.3)

        # сглаживание тренда
        trend_price = _ema(filtered, 0.25)

        # комбинируем
        final_price = (base_price * 0.7 + trend_price * 0.3)

        if max(filtered) > min(filtered) * 5:
            log.warning(f"Слишком большой разброс цен для {item_id}")
            return None

        return final_price, len(filtered)

    def avg_price_custom(self, item_id: str) -> Optional[tuple]:
        with self._lock:
            ps = [s["unit_price"] for s in self.sales if s["item_id"] == item_id]
        if not ps: return None
        recent = ps[-40:]
        filtered = _filtered_prices(recent)

        if len(filtered) < 3:
            return None

        if len(filtered) >= 5:
            if max(filtered) > min(filtered) * 5:
                return None

        base_price = _percentile(filtered, 0.3)
        trend_price = _ema(filtered, 0.25)

        final_price = (base_price * 0.7 + trend_price * 0.3)

        if max(filtered) > min(filtered) * 5:
            log.warning(f"Слишком большой разброс цен для {item_id}")
            return None

        return final_price, len(filtered)

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
def _filtered_prices(prices: list) -> list:
    if len(prices) < 5:
        return prices

    s = sorted(prices)
    q1 = s[len(s)//4]
    q3 = s[len(s)*3//4]
    iqr = q3 - q1

    low  = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr

    return [p for p in prices if low <= p <= high]


def _percentile(data: list, p: float):
    if not data:
        return None
    s = sorted(data)
    k = int(len(s) * p)
    return s[min(k, len(s)-1)]


def _ema(prices: list, alpha=0.3):
    """Экспоненциальное среднее (последние значения важнее)"""
    if not prices:
        return None
    ema = prices[0]
    for p in prices[1:]:
        ema = alpha * p + (1 - alpha) * ema
    return ema


# ═══════════════════════════════════════════════════════════════
#  MARKET ANALYSIS
# ═══════════════════════════════════════════════════════════════

def _market_analysis(prices_with_times: list) -> dict:
    """
    Биржевой анализ цен.
    prices_with_times: список (timestamp, price) отсортированный по времени.
    Возвращает dict с сигналами.
    """
    result = {
        "panic_sale":  None,  # str описание или None
        "reversal":    None,
        "bot_pattern": None,
        "forecast":    None,
        "trend":       None,  # "up"/"down"/"flat"
    }
    if len(prices_with_times) < 4:
        return result

    times  = [t for t, p in prices_with_times]
    prices = [p for t, p in prices_with_times]
    n      = len(prices)

    # Фильтрация выбросов перед анализом
    filtered = _filtered_prices(prices)
    if not filtered:
        return result

    avg_p = sum(filtered) / len(filtered)

    # ── 📉 ПАНИК-СЕЙЛ ─────────────────────────────────────────
    # Последние 3 продажи значительно ниже средней
    if n >= 5:
        recent3 = prices[-3:]
        recent_avg = sum(recent3) / len(recent3)
        if recent_avg < avg_p * 0.75:
            drop_pct = (1 - recent_avg / avg_p) * 100
            result["panic_sale"] = f"📉 Паник-сейл: цена упала на {drop_pct:.0f}% ниже нормы"
        # Быстрое падение (последние 3 точки убывают)
        elif prices[-1] < prices[-2] < prices[-3] and prices[-1] < avg_p * 0.85:
            result["panic_sale"] = f"📉 Нарастающее давление: 3 продажи подряд вниз"

    # ── 📈 РАЗВОРОТ РЫНКА ─────────────────────────────────────
    # После длительного снижения — рост последних продаж
    if n >= 6:
        first_half  = prices[:n//2]
        second_half = prices[n//2:]
        avg_first   = sum(first_half) / len(first_half)
        avg_second  = sum(second_half) / len(second_half)
        if avg_second > avg_first * 1.12:
            growth = (avg_second / avg_first - 1) * 100
            result["reversal"] = f"📈 Разворот: цена выросла на {growth:.0f}% в последние сделки"
        elif avg_second < avg_first * 0.88:
            drop = (1 - avg_second / avg_first) * 100
            result["trend"] = "down"

        if result["trend"] is None:
            if avg_second > avg_first * 1.03:
                result["trend"] = "up"
            elif abs(avg_second - avg_first) / avg_first < 0.03:
                result["trend"] = "flat"
            else:
                result["trend"] = "down"

    # ── 🤖 ДЕТЕКТ БОТОВ/МАНИПУЛЯЦИЙ ──────────────────────────
    # Признаки: много одинаковых цен, или продажи через ровные промежутки
    if n >= 5:
        rounded = sum(1 for p in prices if p % 1000 == 0 or p % 500 == 0)
        if rounded / n > 0.7:
            result["bot_pattern"] = f"🤖 Возможна манипуляция: {rounded}/{n} продаж по круглым ценам"

        # Дублирующиеся цены
        from collections import Counter
        price_counts = Counter(int(p) for p in prices)
        max_dup = price_counts.most_common(1)[0][1] if price_counts else 0
        if max_dup >= 3:
            result["bot_pattern"] = (
                f"🤖 Ботовая активность: одна цена повторяется {max_dup}x"
            )

    # ── 💰 ПРОГНОЗ ЦЕНЫ (линейный тренд по времени) ──────────
    if n >= 5:
        try:
            t0 = times[0]
            xs = [(t - t0) / 3600 for t in times]  # в часах
            # Метод наименьших квадратов вручную
            n_pts = len(xs)
            sx  = sum(xs);    sy  = sum(prices)
            sxy = sum(x*y for x,y in zip(xs, prices))
            sx2 = sum(x**2 for x in xs)
            denom = n_pts * sx2 - sx * sx
            if abs(denom) > 1e-9:
                slope = (n_pts * sxy - sx * sy) / denom
                # Прогноз на +24 часа
                x_future = xs[-1] + 24
                forecast = (sy/n_pts) + slope * (x_future - sx/n_pts)
                forecast = max(forecast, 0)
                direction = "▲" if slope > 0 else "▼"
                if abs(slope) / (avg_p / 100) > 0.5:  # значимый тренд
                    result["forecast"] = (
                        f"💰 Прогноз +24ч: {direction} {int(forecast):,}".replace(",", " ") + " руб."
                    )
        except Exception:
            pass

    return result


def _get_price_history_timed(item_id: str, qlt: int, pg: str) -> list:
    """Список (timestamp, price) для анализа."""
    with db._lock:
        sales = [s for s in db.sales
                 if s["item_id"] == item_id
                 and s["qlt"] == qlt
                 and s.get("ptn_grp", ptn_group(s.get("ptn"))) == pg
                 and not db.is_excluded(s)]
    result = []
    for s in sales:
        try:
            ts = datetime.fromisoformat(s["sold_at"].replace("Z","+00:00")).timestamp()
            result.append((ts, s["unit_price"]))
        except Exception:
            pass
    return sorted(result)


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

# Глобальный счётчик ошибок API — для предохранителя аномалий
_api_consecutive_errors = 0
_api_last_success_ts    = 0.0

def api_get(path: str, params=None, _retry=0) -> Optional[dict]:
    global _api_consecutive_errors, _api_last_success_ts
    try:
        r = requests.get(f"{BASE_URL}{path}",
            headers={"Authorization": f"Bearer {STALCRAFT_TOKEN}"},
            params=params or {},
            timeout=20,          # чуть больше чем раньше
            )
        if r.status_code == 429:
            wait = 40 + _retry * 20
            log.warning(f"Rate limit — ждём {wait} сек...")
            time.sleep(wait)
            return api_get(path, params, _retry + 1)
        if r.status_code == 401:
            log.error("❌ ТОКЕН ИСТЁК (HTTP 401) — обнови STALCRAFT_TOKEN в .env файле")
            log.error("   detect_buyouts заблокирован до исправления токена")
            _api_consecutive_errors += 1
            return None   # None = не вызывать detect_buyouts
        if r.status_code in (502, 503, 504):
            log.warning(f"Сервер временно недоступен ({r.status_code}) — {path}")
            _api_consecutive_errors += 1
            return None
        r.raise_for_status()
        _api_consecutive_errors = 0
        _api_last_success_ts    = time.time()
        return r.json()
    except (requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError) as e:
        _api_consecutive_errors += 1
        log.warning(f"Таймаут/нет соединения ({_api_consecutive_errors} подряд): {path}")
        return None
    except requests.HTTPError as e:
        log.error(f"HTTP {e.response.status_code} {path}")
    except Exception as e:
        log.error(f"{path}: {e}")
    return None


def get_lots(item_id: str) -> Optional[list]:
    """
    Возвращает список лотов ИЛИ None если запрос не удался.
    None отличается от [] (нет лотов на аукционе).
    """
    d = api_get(f"/auction/{item_id}/lots", {"additional": "true", "limit": 200})
    if d is None:
        return None            # API недоступен — НЕ делать detect_buyouts
    return d.get("lots", [])  # [] = нет лотов, это нормально

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

def _build_alert(item_id, lot, qlt, ptn, pg, unit, avg, disc, n, is_art,
                 analysis: dict = None) -> str:
    circle = QUALITY_CIRCLE.get(qlt, "")
    qname  = QUALITY.get(qlt, ("?",""))[0]
    name   = art_name(item_id) if is_art else item_name(item_id)
    plabel = PTN_LABELS.get(pg, pg)
    header = "🔥 ВЫГОДНЫЙ ЛОТ — EU" if disc >= 20 else "📉 СКИДКА — EU"

    lines = [f"<b>{header}</b>", "━━━━━━━━━━━━━━━━━━━━━"]
    if is_art:
        lines += [
            f"{circle} <b>{name}</b>",
            f"⭐ Качество:  <b>{qname}</b>",
            f"⚡ Заточка:   <b>{plabel}</b>",
        ]
    else:
        lines.append(f"<b>{name}</b>")

    avg_label = f"Ориентир {plabel}" if is_art else "Ориентир"
    lines += [
        "",
        f"💰 Цена:      <b>{_fmt(unit)}</b> руб./шт.",
        f"📊 {avg_label}: {_fmt(avg)} руб. ({n} прод.)",
        f"📉 Скидка:    <b>{disc:.1f}%</b>",
    ]

    # Рыночные сигналы
    if analysis:
        signals = []
        if analysis.get("panic_sale"):  signals.append(analysis["panic_sale"])
        if analysis.get("reversal"):    signals.append(analysis["reversal"])
        if analysis.get("bot_pattern"): signals.append(analysis["bot_pattern"])
        if analysis.get("forecast"):    signals.append(analysis["forecast"])
        if analysis.get("trend") == "up":
            signals.append("📈 Тренд: цена растёт")
        elif analysis.get("trend") == "down":
            signals.append("📉 Тренд: цена снижается")
        if signals:
            lines.append("")
            lines.extend(signals)

    lines += [
        "",
        f"🕐 {datetime.now().strftime('%H:%M:%S')}",
        "━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)


def _bought_markup(item_id: str, lk: str) -> dict:
    """Inline-кнопка 'Купил' под алертом."""
    return {"inline_keyboard": [[
        {"text": "✅ Купил — убрать из истории", "callback_data": f"bought:{item_id}:{lk}"}
    ]]}


def check_alert(item_id: str, lot: dict, qlt: int, ptn, is_art: bool = True):
    lk  = db.lot_key(lot)
    pg  = ptn_group(ptn)
    key = f"{item_id}:{lk}:q{qlt}:{pg}"
    if db.alert_sent(key): return

    s         = db.settings
    disc_pct  = s["discount_pct"]
    min_diff  = s["min_price_diff"]
    min_sales = s["min_sales"]

    if is_art:
        res = db.avg_price(item_id, qlt, pg)
    else:
        res = db.avg_price_custom(item_id)
    if not res or res[1] < min_sales: return
    avg, n = res

    amt  = max(int(lot.get("amount") or 1), 1)
    raw  = lot.get("buyoutPrice")   # только лоты с ценой выкупа
    if not raw: return              # без buyoutPrice — игнорируем
    unit = float(raw) / amt

    if not ((avg - unit) >= min_diff and unit <= avg * (1 - disc_pct / 100)):
        return

    disc = (1 - unit / avg) * 100

    # Рыночный анализ
    analysis = None
    if is_art:
        history = _get_price_history_timed(item_id, qlt, pg)
        analysis = _market_analysis(history) if len(history) >= 4 else None

    msg    = _build_alert(item_id, lot, qlt, ptn, pg, unit, avg, disc, n, is_art, analysis)
    markup = _bought_markup(item_id, lk)

    # Владелец и VIP — сразу
    tg_send(msg, TELEGRAM_CHAT_ID, markup)
    for vip in list(db.vip_users):
        if vip != str(TELEGRAM_CHAT_ID):
            tg_send(msg, vip, markup)

    # Обычные подписчики — через 5 минут
    DELAY_SECS = 300
    send_at = time.time() + DELAY_SECS
    for sub in list(db.subscribers):
        if sub != str(TELEGRAM_CHAT_ID) and sub not in db.vip_users:
            db.pending_alerts.append((send_at, sub, msg, markup))

    db.mark_sent(key)
    log.info(f"ALERT: {art_name(item_id)} q{qlt} {pg} -{disc:.1f}%")

# ═══════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════

def _tg(method: str, _attempt: int = 0, **kw) -> Optional[dict]:
    if not TELEGRAM_TOKEN: return None
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
                          json=kw, timeout=20)
        if r.ok:
            return r.json()
        # Telegram rate limit
        if r.status_code == 429:
            retry_after = r.json().get("parameters", {}).get("retry_after", 5)
            log.warning(f"TG rate limit, ждём {retry_after}s")
            time.sleep(retry_after)
            return _tg(method, _attempt, **kw)
        log.debug(f"TG {method} → {r.status_code}: {r.text[:100]}")
        return None
    except (requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout) as e:
        if _attempt < 2:
            time.sleep(2 ** _attempt)  # экспоненциальная пауза: 1s, 2s
            return _tg(method, _attempt + 1, **kw)
        log.warning(f"TG {method} failed after retries: {type(e).__name__}")
        return None
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

def tg_answer_text(cid, text):
    _tg("answerCallbackQuery", callback_id=cid, text=text, show_alert=False)

def tg_photo(chat_id, data, caption="", _attempt=0):
    if not TELEGRAM_TOKEN: return
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"photo": ("chart.png", io.BytesIO(data), "image/png")},
            timeout=45)
        if not r.ok:
            log.warning(f"TG photo failed: {r.status_code}")
    except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
        if _attempt < 2:
            time.sleep(2 ** _attempt)
            tg_photo(chat_id, data, caption, _attempt + 1)
    except Exception as e:
        log.error(f"TG photo: {e}")

def kb(*rows) -> dict:
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row]
                                 for row in rows]}

_tg_offset  = 0
_user_state : dict = {}

def _state(chat_id): return _user_state.get(chat_id, "")
def _set(chat_id, s): _user_state[chat_id] = s

# ── Меню ──────────────────────────────────────────────────────

def _is_owner(chat_id: str) -> bool:
    return str(chat_id) == str(TELEGRAM_CHAT_ID)


def _main_kb(chat_id: str = "") -> dict:
    web_row = (
        [{"text": "🌐 Дашборд (WebApp)", "web_app": {"url": WEB_APP_URL}}]
        if WEB_APP_URL.startswith("https://")
        else [{"text": "🌐 Открыть дашборд", "url": WEB_APP_URL}]
    )
    rows = [
        [{"text":"🔍 Артефакт","callback_data":"search_art"},
         {"text":"📦 Любой предмет","callback_data":"search_item"}],
        [{"text":"📋 Мой список","callback_data":"watchlist"},
         {"text":"📊 Статус","callback_data":"status"}],
    ]
    if _is_owner(chat_id):
        rows.append([{"text":"⚙️ Настройки","callback_data":"settings"}])
    rows.append(web_row)
    return {"inline_keyboard": rows}


def _send_main(chat_id, msg_id=None):
    txt = "⚗️ <b>Stalcraft Monitor EU</b>\n\nВыбери действие:"
    markup = _main_kb(chat_id)
    if msg_id: tg_edit(chat_id, msg_id, txt, markup)
    else:       tg_send(txt, chat_id, markup)

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
        [("📈 График",  f"chart:{item_id}"),
         ("📋 Лоты",    f"lots:{item_id}")],
        [("⬅️ Назад", "main")],
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
                sorted_pg = sorted([l for l in pg_lots if l.get("buyout")],
                                   key=lambda x: x.get("buyout") or 0)
                label = PTN_LABELS[pg]
                lines.append(f"  {label}:")
                for lot in sorted_pg[:MAX_PER_GROUP]:
                    if not lot.get("buyout"): continue  # без buyoutPrice не показываем
                    amt = max(lot.get("amount", 1) or 1, 1)
                    pr  = lot.get("buyout")
                    lines.append(f"    • {_fmt(pr // amt)} руб./шт.")
                if len(sorted_pg) > MAX_PER_GROUP:
                    lines.append(f"    <i>+ещё {len(sorted_pg)-MAX_PER_GROUP}</i>")
    else:
        sorted_lots = [l for l in lots if l.get("buyout")]
        sorted_lots.sort(key=lambda x: x.get("buyout") or 0)
        for lot in sorted_lots[:20]:
            amt = max(lot.get("amount", 1) or 1, 1)
            pr  = lot.get("buyout")
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
        raw = lot.get("buyout")
        if not raw: continue  # только лоты с ценой выкупа
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
        [("📊 Изменить мин. продаж", "set:min_sales")],
        [("👑 Управление VIP",       "admin_vip"),
         ("⬅️ Назад",               "main")],
    )
    tg_edit(chat_id, msg_id, txt, markup)


def _send_admin_vip(chat_id, msg_id):
    vips  = list(db.vip_users)
    subs  = list(db.subscribers)
    lines = ["👑 <b>VIP пользователи</b> (алерты без задержки)\n"]
    if vips:
        for v in vips:
            tag = " (владелец)" if v == str(TELEGRAM_CHAT_ID) else ""
            lines.append(f"• <code>{v}</code>{tag}")
    else:
        lines.append("  Список пуст")
    lines.append(f"\n📢 Всего подписчиков: <b>{len(subs)}</b>")
    lines.append("<i>Подписчики получают алерты с задержкой 5 мин.</i>")
    markup = kb(
        [("➕ Добавить VIP",  "vip_add_prompt"),
         ("➖ Убрать VIP",   "vip_del_prompt")],
        [("⬅️ Назад",        "settings")],
    )
    tg_edit(chat_id, msg_id, "\n".join(lines), markup)

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
    elif state == "vip_add":
        if _is_owner(chat_id):
            new_vip = text.strip()
            if new_vip.lstrip("-").isdigit():
                db.vip_add(new_vip)
                tg_send(f"✅ <code>{new_vip}</code> добавлен в VIP.\n"
                        "Теперь он будет получать алерты без задержки.", chat_id,
                        kb([("👑 Управление VIP", "admin_vip_fresh")]))
            else:
                tg_send("❌ Неверный формат. chat_id — это число (может быть отрицательным для групп).",
                        chat_id, kb([("👑 VIP", "admin_vip_fresh")]))
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
        # Любой текст без активного состояния → автопоиск артефакта
        results = search_arts(text)
        if results:
            _handle_search_art(chat_id, text)
        else:
            results2 = search_items(text)
            if results2:
                _handle_search_item(chat_id, text)
            else:
                _send_main(chat_id)

def _handle_callback(chat_id, msg_id, data):
    if data == "main":            _send_main(chat_id, msg_id)
    elif data.startswith("bought:"):
        # Кнопка "Купил" из алерта
        parts = data.split(":", 2)
        if len(parts) == 3:
            _, b_item, b_lk = parts
            if _is_owner(chat_id):
                db.exclude_lot(b_lk)
                tg_edit(chat_id, msg_id,
                        "✅ Лот убран из истории цен.\n<i>Он больше не влияет на расчёт ориентира.</i>",
                        kb([("⬅️ Назад", f"art:{b_item}")]))
            else:
                tg_answer_text(msg_id, "Только владелец может помечать покупки.")
    elif data == "status":        _send_status(chat_id, msg_id)
    elif data == "watchlist":     _send_watchlist(chat_id, msg_id)
    elif data == "settings":
        if _is_owner(chat_id): _send_settings(chat_id, msg_id)
        else: tg_edit(chat_id, msg_id, "⛔ Только владелец может менять настройки.", kb([("⬅️","main")]))
    elif data == "settings_fresh":
        if _is_owner(chat_id): _send_settings(chat_id, msg_id)
        else: _send_main(chat_id, msg_id)
    elif data == "admin_vip_fresh":
        if _is_owner(chat_id): _send_admin_vip(chat_id, msg_id)
    elif data == "search_art":
        # Показываем топ-20 артефактов по кол-ву продаж + строку ввода
        top = sorted(art_db.items(),
                     key=lambda x: sum(1 for s in db.sales if s["item_id"]==x[0]),
                     reverse=True)[:18]
        rows = [[{"text": i["name"][:28], "callback_data": f"art:{iid}"}]
                for iid, i in top]
        rows.append([{"text":"⌨️ Поиск по названию →","callback_data":"type_art"}])
        rows.append([{"text":"⬅️ Назад","callback_data":"main"}])
        tg_edit(chat_id, msg_id,
                "⚗️ <b>Выбери артефакт</b>\n<i>Топ по активности или поиск:</i>",
                {"inline_keyboard": rows})
    elif data == "type_art":
        _set(chat_id, "search_art")
        tg_edit(chat_id, msg_id, "🔍 Введи название артефакта (или часть):",
                kb([("⬅️","main")]))
    elif data == "search_item":
        top_c = list(db.custom_items.items())[:18]
        rows  = [[{"text": nm[:28], "callback_data": f"cust:{iid}"}]
                 for iid, nm in top_c]
        rows.append([{"text":"⌨️ Поиск по названию →","callback_data":"type_item"}])
        rows.append([{"text":"⬅️ Назад","callback_data":"main"}])
        tg_edit(chat_id, msg_id,
                "📦 <b>Мои предметы</b>\n<i>Выбери или найди новый:</i>",
                {"inline_keyboard": rows})
    elif data == "type_item":
        _set(chat_id, "search_item")
        tg_edit(chat_id, msg_id, "📦 Введи название предмета:",
                kb([("⬅️","main")]))
    elif data.startswith("set:"):     _send_set_prompt(chat_id, msg_id, data[4:])
    elif data == "admin_vip":
        if _is_owner(chat_id): _send_admin_vip(chat_id, msg_id)
    elif data == "vip_add_prompt":
        if _is_owner(chat_id):
            _set(chat_id, "vip_add")
            tg_edit(chat_id, msg_id,
                    "👑 Введи <b>chat_id</b> пользователя которого хочешь добавить в VIP:\n"
                    "<i>(пользователь должен написать /start боту чтобы узнать свой ID)</i>",
                    kb([("❌ Отмена", "admin_vip")]))
    elif data == "vip_del_prompt":
        if _is_owner(chat_id):
            vips = [v for v in db.vip_users if v != str(TELEGRAM_CHAT_ID)]
            if not vips:
                tg_edit(chat_id, msg_id, "Список VIP пуст.", kb([("⬅️", "admin_vip")])); return
            rows = [[(f"❌ {v}", f"vip_del:{v}")] for v in vips]
            rows.append([("⬅️ Назад", "admin_vip")])
            tg_edit(chat_id, msg_id, "Выбери кого убрать из VIP:", kb(*rows))
    elif data.startswith("vip_del:"):
        if _is_owner(chat_id):
            v = data[8:]; db.vip_del(v)
            tg_edit(chat_id, msg_id, f"✅ <code>{v}</code> убран из VIP.", kb([("⬅️", "admin_vip")]))
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
        iid    = data[6:]
        is_art = iid in art_db
        nm     = art_name(iid) if is_art else item_name(iid)
        back   = f"art:{iid}" if is_art else f"cust:{iid}"
        tg_edit(chat_id, msg_id, f"⏳ Генерирую график {nm}…", kb([("⬅️", back)]))
        png = make_chart(iid) if is_art else make_chart_custom(iid)
        if png:
            tg_photo(chat_id, png, f"📈 {nm}")
        else:
            tg_send("Данных пока нет.", chat_id)
        if is_art: _send_art(chat_id, None, iid)
        else:      _send_custom(chat_id, None, iid)

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
        _set(chat_id, "")
        # Показываем chat_id чтобы пользователь мог передать владельцу для VIP
        if not _is_owner(chat_id):
            tg_send(f"👋 Твой chat_id: <code>{chat_id}</code>\n"
                    "Передай его владельцу если хочешь VIP-алерты без задержки.\n"
                    "Или /subscribe чтобы получать алерты с задержкой 5 мин.", chat_id)
        _send_main(chat_id)
    elif text.startswith("/subscribe"):
        db.sub_add(chat_id)
        tg_send("✅ Подписан на алерты! Буду присылать выгодные лоты с задержкой 5 мин.", chat_id,
                kb([("📋 Меню", "main")]))
    elif text.startswith("/unsubscribe"):
        db.sub_del(chat_id)
        tg_send("🔕 Отписан от алертов.", chat_id)
    elif text.startswith("/status"):
        _send_status(chat_id)
    elif text.startswith("/cleanalerts"):
        if not _is_owner(chat_id):
            tg_send("⛔ Только владелец.", chat_id); return
        with db._lock:
            old_cnt = len(db.sent_alerts)
            db.sent_alerts.clear()
        db.save()
        tg_send(f"✅ Сброшено {old_cnt} алертов.", chat_id)
    elif text.startswith("/cleandb"):
        if not _is_owner(chat_id):
            tg_send("⛔ Только владелец.", chat_id); return
        from collections import Counter
        with db._lock:
            def bkt(s):
                from datetime import datetime
                ts = datetime.fromisoformat(s['sold_at'].replace('Z','+00:00')).timestamp()
                return int(ts // 90)
            by_bkt = Counter(bkt(s) for s in db.sales)
            bad    = {b for b, c in by_bkt.items() if c > 25}
            expanded = set()
            for b in bad: expanded |= {b-1, b, b+1}
            old_cnt  = len(db.sales)
            db.sales = [s for s in db.sales if bkt(s) not in expanded]
            removed  = old_cnt - len(db.sales)
        db.save()
        tg_send(f"🧹 Удалено {removed} из {old_cnt}. Осталось {len(db.sales)}.", chat_id)
    else:
        _handle_text(chat_id, text)

def _flush_pending_alerts():
    """Отправляет отложенные алерты подписчикам когда наступает время."""
    while True:
        time.sleep(10)
        now = time.time()
        still_pending = []
        for item in list(db.pending_alerts):
            send_at, chat_id, msg, markup = item
            if now >= send_at:
                tg_send(msg, chat_id, markup)
            else:
                still_pending.append(item)
        db.pending_alerts = still_pending


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
    """
    Улучшенный scatter-график:
    - Каждое качество = отдельный subplot (свой масштаб оси Y)
    - Scatter с jitter по X (точки не сливаются)
    - Размер точки = relative price within group
    - Линия тренда (полиномиальная)
    - Медиана как горизонталь
    - Логарифм при разбросе >8x
    - Без почасовых агрегатов — каждая продажа = точка
    """
    if not MPL: return None
    sales = db.sales_for(item_id)
    if not sales: return None

    try:
        import numpy as np
    except ImportError:
        log.warning("numpy не установлен, графики ограничены")
        np = None

    from statistics import median as _med

    # ── Palette (яркие, контрастные на тёмном фоне) ────────────────
    PG_COLORS = {
        "0":     "#94a3b8",   # серый-синеватый
        "1-4":   "#4ade80",   # зелёный
        "5-9":   "#38bdf8",   # голубой
        "10-14": "#c084fc",   # фиолетовый
        "15":    "#fbbf24",   # золотой
    }
    PG_ALPHA  = {"0":.5, "1-4":.65, "5-9":.8, "10-14":.9, "15":1.0}

    by_qlt: dict = {}
    for s in sales:
        by_qlt.setdefault(s["qlt"], []).append(s)
    active = sorted(by_qlt.keys())
    if not active: return None

    n_plots = len(active)
    fig, axes = plt.subplots(n_plots, 1, figsize=(13, 4.5 * n_plots), squeeze=False)
    fig.patch.set_facecolor("#0b0f1a")
    fig.subplots_adjust(hspace=0.55)

    for idx, qlt in enumerate(active):
        ax = axes[idx][0]
        ax.set_facecolor("#111827")

        qname, _qcolor = QUALITY[qlt]
        # Используем яркий цвет качества только для заголовка,
        # сами точки окрашены по группе заточки
        QL_TITLE_COLORS = {0:"#94a3b8",1:"#4ade80",2:"#818cf8",3:"#e879f9",4:"#fb7185",5:"#fcd34d"}
        title_color = QL_TITLE_COLORS.get(qlt, "#e5e7eb")

        sales_q = by_qlt[qlt]
        all_vals = [s["unit_price"] for s in sales_q if s["unit_price"] > 0]
        if not all_vals: continue

        use_log = len(all_vals) > 2 and max(all_vals) / max(min(all_vals), 1) > 8

        by_pg: dict = {}
        for s in sales_q:
            pg = s.get("ptn_grp") or ptn_group(s.get("ptn"))
            by_pg.setdefault(pg, []).append(s)

        # Временной диапазон для jitter
        all_ts = [datetime.fromisoformat(s["sold_at"]).timestamp() for s in sales_q]
        time_span = max(all_ts) - min(all_ts) if len(all_ts) > 1 else 3600
        jitter_scale = max(time_span * 0.012, 300)  # минимум 5 мин

        for pg in PTN_GROUPS:
            if pg not in by_pg: continue
            pts = sorted(
                [(datetime.fromisoformat(s["sold_at"]), s["unit_price"])
                 for s in by_pg[pg]], key=lambda x: x[0])
            xs_dt = [x for x, y in pts]
            ys    = [y for x, y in pts]
            color = PG_COLORS.get(pg, "#e5e7eb")
            alpha = PG_ALPHA.get(pg, 0.7)

            # Jitter
            if np is not None:
                xs_num = np.array([x.timestamp() for x in xs_dt])
                jitter  = np.random.uniform(-jitter_scale, jitter_scale, len(xs_num))
                xs_plot = [datetime.fromtimestamp(t + j, tz=timezone.utc)
                           for t, j in zip(xs_num, jitter)]
            else:
                xs_plot = xs_dt

            # Размер точки — относительный внутри группы
            ymax = max(all_vals)
            sizes = [max(25, min(180, (y / ymax) * 160 + 20)) for y in ys]

            ax.scatter(xs_plot, ys, s=sizes, c=color, alpha=alpha,
                       label=PTN_LABELS[pg], edgecolors="#0b0f1a",
                       linewidths=0.4, zorder=3)

            # Линия тренда (если >=3 точек)
            if len(ys) >= 3 and np is not None:
                xs_num = np.array([x.timestamp() for x in xs_dt])
                xs_norm = (xs_num - xs_num[0]) / 3600  # в часах
                try:
                    deg = min(2, len(ys) - 1)
                    coeffs = np.polyfit(xs_norm, ys, deg)
                    xs_line = np.linspace(xs_norm[0], xs_norm[-1], 100)
                    ys_line = np.polyval(coeffs, xs_line)
                    xs_line_dt = [datetime.fromtimestamp(xs_num[0] + x * 3600, tz=timezone.utc)
                                  for x in xs_line]
                    ax.plot(xs_line_dt, ys_line, color=color, linewidth=1.2,
                            alpha=0.35, linestyle="--", zorder=2)
                except Exception: pass

            # Медиана — пунктирная горизонталь
            med_val = _med(ys)
            ax.axhline(med_val, color=color, linewidth=0.9, alpha=0.45,
                       linestyle=":", zorder=1)

        # ── Оформление subplot ─────────────────────────────────────
        ax.set_title(qname, color=title_color, fontsize=11, pad=8,
                     loc="left", fontweight="bold")
        ax.set_ylabel("руб./шт.", color="#4b5563", fontsize=8)
        ax.tick_params(colors="#4b5563", labelsize=7.5, which="both")

        if use_log:
            ax.set_yscale("log")
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _: (
                f"{int(v/1_000_000)}M" if v >= 1_000_000
                else f"{int(v/1_000)}K" if v >= 10_000
                else f"{int(v):,}".replace(",", " ")
            )))

        # X-axis: дата без часов
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=10))
        fig.autofmt_xdate(rotation=20, ha="right")

        for sp in ax.spines.values():
            sp.set_edgecolor("#1f2937")
        ax.grid(color="#1f2937", linewidth=0.5, alpha=0.4, which="both")
        ax.set_axisbelow(True)

        legend = ax.legend(
            facecolor="#0d1321", edgecolor="#1f2937",
            labelcolor="#d1d5db", fontsize=7.5, loc="upper left",
            markerscale=0.8, framealpha=0.92,
        )
        legend.get_frame().set_linewidth(0.8)

    # ── Общий заголовок ──────────────────────────────────────────
    title = art_name(item_id) if item_id in art_db else item_name(item_id)
    fig.suptitle(title, color="#f1f5f9", fontsize=13, y=1.01, fontweight="bold")

    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=150, facecolor=fig.get_facecolor(),
                bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def make_chart_custom(item_id: str) -> Optional[bytes]:
    """График для кастомного предмета (без деления по качеству)."""
    if not MPL: return None
    sales = db.sales_for(item_id)
    if not sales: return None

    try:
        import numpy as np
        HAS_NP = True
    except ImportError:
        HAS_NP = False

    from statistics import median as _med

    pts = sorted([(datetime.fromisoformat(s["sold_at"]), s["unit_price"])
                  for s in sales], key=lambda x: x[0])
    xs_dt = [x for x, y in pts]
    ys    = [y for x, y in pts]

    fig, ax = plt.subplots(figsize=(13, 4.5))
    fig.patch.set_facecolor("#0b0f1a")
    ax.set_facecolor("#111827")

    color = "#38bdf8"
    ymax  = max(ys) if ys else 1
    sizes = [max(25, min(150, (y/ymax)*130 + 20)) for y in ys]

    if HAS_NP:
        xs_num  = np.array([x.timestamp() for x in xs_dt])
        span    = max(xs_num.max() - xs_num.min(), 3600)
        jitter  = np.random.uniform(-span*0.012, span*0.012, len(xs_num))
        xs_plot = [datetime.fromtimestamp(t+j, tz=timezone.utc) for t, j in zip(xs_num, jitter)]
    else:
        xs_plot = xs_dt

    ax.scatter(xs_plot, ys, s=sizes, c=color, alpha=0.85,
               edgecolors="#0b0f1a", linewidths=0.4, zorder=3, label="Продажа")

    if len(ys) >= 3 and HAS_NP:
        xs_norm = (xs_num - xs_num[0]) / 3600
        try:
            coeffs  = np.polyfit(xs_norm, ys, min(2, len(ys)-1))
            xs_line = np.linspace(xs_norm[0], xs_norm[-1], 100)
            ys_line = np.polyval(coeffs, xs_line)
            xs_l_dt = [datetime.fromtimestamp(xs_num[0]+x*3600, tz=timezone.utc) for x in xs_line]
            ax.plot(xs_l_dt, ys_line, color=color, linewidth=1.5, alpha=0.4, linestyle="--")
        except Exception: pass

    med_val = _med(ys)
    ax.axhline(med_val, color="#fbbf24", linewidth=1.2, alpha=0.6,
               linestyle=":", label=f"Медиана: {_fmt(med_val)} ₽")

    use_log = len(ys) > 2 and max(ys) / max(min(ys), 1) > 8
    if use_log: ax.set_yscale("log")

    ax.set_title(item_name(item_id), color="#f1f5f9", fontsize=12, pad=8, fontweight="bold")
    ax.set_ylabel("руб./шт.", color="#4b5563", fontsize=8)
    ax.tick_params(colors="#4b5563", labelsize=7.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=10))
    fig.autofmt_xdate(rotation=20)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _: (
            f"{int(v/1_000_000)}M" if v >= 1_000_000
            else f"{int(v/1_000)}K" if v >= 10_000
            else f"{int(v):,}".replace(",", " ")
        )))
    for sp in ax.spines.values(): sp.set_edgecolor("#1f2937")
    ax.grid(color="#1f2937", linewidth=0.5, alpha=0.4)
    ax.set_axisbelow(True)
    ax.legend(facecolor="#0d1321", edgecolor="#1f2937", labelcolor="#d1d5db", fontsize=8)

    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=150, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()

# ═══════════════════════════════════════════════════════════════
#  МОНИТОРИНГ
# ═══════════════════════════════════════════════════════════════

# Предохранитель аномалий: максимум допустимых выкупов за один цикл
# (если за одну итерацию по всем предметам выкупов больше порога — пропускаем)
MAX_BUYOUTS_PER_CYCLE = 100  # если за цикл >20 выкупов — признак аномалии

_cycle_buyouts = 0  # сбрасывается в начале каждого цикла


def process_item(item_id: str, is_art: bool):
    global _cycle_buyouts
    lots = get_lots(item_id)

    # None = API недоступен — НЕ детектируем выкупы (ложных срабатываний не будет)
    if lots is None:
        log.debug(f"  API недоступен для {item_id} — пропускаем detect_buyouts")
        return

    cur = set()
    for lot in lots:
        lk = db.lot_key(lot)
        if not lk: continue
        cur.add(lk)
        qlt, ptn = extract_qlt_ptn(lot, item_id) if is_art else (0, None)
        db.upsert(item_id, lot, qlt, ptn)

    # Предохранитель: если за цикл уже много выкупов — пропускаем
    if _cycle_buyouts < MAX_BUYOUTS_PER_CYCLE:
        bought = db.detect_buyouts(item_id, cur)
        _cycle_buyouts += len(bought)
        if _cycle_buyouts >= MAX_BUYOUTS_PER_CYCLE:
            log.warning(f"Предохранитель: {_cycle_buyouts} выкупов за цикл — подозрительно, "
                        f"остановка detect_buyouts до следующего цикла")

    for lot in lots:
        qlt, ptn = extract_qlt_ptn(lot, item_id) if is_art else (0, None)
        check_alert(item_id, lot, qlt, ptn, is_art)

def monitor_loop():
    log.info("=" * 58)
    log.info("  Stalcraft Monitor — EU  v3.0")
    log.info(f"  Порог: {db.settings['discount_pct']}% + {_fmt(db.settings['min_price_diff'])} руб.")
    log.info("=" * 58)
    cycle = 0
    while True:
        global _cycle_buyouts
        cycle = cycle + 1
        _cycle_buyouts = 0   # сбрасываем счётчик предохранителя
        art_ids    = list(art_db.keys())
        custom_ids = list(db.custom_items.keys())
        total      = len(art_ids) + len(custom_ids)
        st         = db.stats()

        # Предупреждение если API долго недоступен
        if _api_consecutive_errors >= 5:
            log.error(f"⚠ API недоступен уже {_api_consecutive_errors} запросов подряд — возможен даунтайм сервера")
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
            # Build custom_items dict with their sales included
            custom_items_data = {}
            for iid, nm in db.custom_items.items():
                name = nm if isinstance(nm, str) else nm.get("name", iid)
                csales = [s for s in sales if s["item_id"] == iid]
                custom_items_data[iid] = {
                    "name":  name,
                    "sales": csales,
                }
            body = json.dumps({
                "stats":        db.stats(),
                "settings":     db.settings,
                "arts":         arts,
                "sales":        sales[-3000:],
                "lots":         list(db.active_lots.values()),
                "quality":      {str(k):{"name":v[0],"color":v[1]} for k,v in QUALITY.items()},
                "ptn_groups":   PTN_LABELS,
                "custom_items": custom_items_data,
            }, ensure_ascii=False, default=str).encode("utf-8")
            self._ok("application/json; charset=utf-8", body,
                     [("Access-Control-Allow-Origin","*")])
        elif self.path.startswith("/api/chart/custom/"):
            iid = self.path.split("/")[-1]
            png = make_chart_custom(iid)
            if png: self._ok("image/png", png)
            else:   self.send_response(404); self.end_headers()
        elif self.path.startswith("/api/chart/"):
            iid = self.path.split("/")[-1]
            # Route to correct chart function
            png = make_chart(iid) if iid in art_db else make_chart_custom(iid)
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
    threading.Thread(target=start_web,            daemon=True).start()
    threading.Thread(target=tg_polling_loop,      daemon=True).start()
    threading.Thread(target=_flush_pending_alerts, daemon=True).start()
    log.info(f"Дашборд: http://localhost:{WEB_PORT}")

    try:
        monitor_loop()
    except KeyboardInterrupt:
        log.info("Остановлено.")
        db.save()