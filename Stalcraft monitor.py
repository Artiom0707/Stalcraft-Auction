"""
Stalcraft Artefact Monitor — EU
=================================
Только артефакты. Качество (qlt 0-5), заточка (ptn 0-15).
Детект выкупов через исчезновение лота до таймера.
Telegram бот — чистый HTTP, никаких библиотек.

pip install requests matplotlib pillow

Запуск:
  python stalcraft_monitor.py              — старт
  python stalcraft_monitor.py --update     — переобновить базу артефактов
  python stalcraft_monitor.py --debug-lot ITEM_ID
"""

import sys, io, json, time, zipfile, logging, threading, argparse
from datetime import datetime, timezone, timedelta
from statistics import mean
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

import requests

try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.ticker as mticker
    MPL = True
except ImportError:
    MPL = False
    print("⚠  pip install matplotlib pillow  (графики недоступны)")

# ═══════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════════════

STALCRAFT_TOKEN = os.getenv("STALCRAFT_TOKEN")
REGION             = "EU"
REALM              = "global"          # EU → global  |  RU → ru
BASE_URL           = f"https://eapi.stalcraft.net/{REGION}"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")               # токен от @BotFather
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")               # твой chat_id (алерты сюда)

DISCOUNT_THRESHOLD = 10               # % скидки для алерта
MIN_SALES_ALERT    = 3                # мин. продаж (item+quality) до алертов
POLL_INTERVAL      = 90               # сек между циклами
REQUEST_DELAY      = 0.7              # сек между запросами к API
WEB_PORT           = 8765

GITHUB_ZIP    = "https://github.com/EXBO-Studio/stalcraft-database/archive/refs/heads/main.zip"
GITHUB_COMMIT = "https://api.github.com/repos/EXBO-Studio/stalcraft-database/commits/main"
CACHE_FILE    = "artefact_cache.json"
DB_FILE       = "stalcraft_db.json"

# ── Качество ────────────────────────────────────────────────────────
QUALITY = {
    0: ("Обычный",     "#939393"),
    1: ("Необычный",    "#4ad94b"),
    2: ("Особый",   "#5555ff"),
    3: ("Редкий",   "#940394"),
    4: ("Исключительный",  "#d14849"),
    5: ("Легендарный", "#ffaa00"),
}
# Реальные цвета из JSON базы EXBO → качество
# (уточняется после первого --update)
COLOR_TO_QLT = {
    "ARTEFACT_JUNK":      0,
    "ARTEFACT_COMMON":    1,
    "ARTEFACT_UNCOMMON":  2,
    "ARTEFACT_RARE":      3,
    "ARTEFACT_EPIC":      4,
    "ARTEFACT_LEGENDARY": 5,
    # Альтернативные варианты написания (на всякий случай)
    "artefact_junk":      0,
    "artefact_common":    1,
    "artefact_uncommon":  2,
    "artefact_rare":      3,
    "artefact_epic":      4,
    "artefact_legendary": 5,
}

# ═══════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("stalcraft_monitor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
#  DATABASE (в памяти + JSON персистентность)
# ═══════════════════════════════════════════════════════════════════

class DB:
    def __init__(self):
        self._lock       = threading.Lock()
        self.active_lots : dict = {}   # "item:lot_id" -> snapshot
        self.sales       : list = []   # история продаж
        self.watch_set   : set  = set()
        self._load()

    def _load(self):
        p = Path(DB_FILE)
        if not p.exists():
            return
        try:
            raw = json.loads(p.read_text("utf-8"))
            self.active_lots = raw.get("active_lots", {})
            self.sales       = raw.get("sales", [])
            self.watch_set   = set(raw.get("watch_set", []))
            log.info(f"БД: {len(self.sales)} продаж, {len(self.active_lots)} лотов")
        except Exception as e:
            log.error(f"Загрузка БД: {e}")

    def save(self):
        with self._lock:
            payload = {
                "active_lots": self.active_lots,
                "sales":       self.sales[-10_000:],
                "watch_set":   list(self.watch_set),
                "saved_at":    _now(),
            }
        try:
            Path(DB_FILE).write_text(json.dumps(payload, ensure_ascii=False), "utf-8")
        except Exception as e:
            log.error(f"Сохранение БД: {e}")

    # ── лоты ─────────────────────────────────────────────────────────

    def upsert(self, item_id: str, lot: dict, qlt: int, ptn):
        # API не возвращает поле "id" — строим уникальный ключ из доступных полей
        raw_id = lot.get("id")
        if raw_id:
            lid = str(raw_id)
        else:
            spawn = lot.get("additional", {}).get("spawn_time", "")
            start = lot.get("startTime", "")
            lid   = f"{start}_{spawn}" if (start or spawn) else ""
        if not lid:
            return
        key = f"{item_id}:{lid}"
        now = _now()
        with self._lock:
            prev = self.active_lots.get(key, {})
            self.active_lots[key] = {
                "item_id":    item_id, "lot_id": lid,
                "qlt": qlt,  "ptn": ptn,
                "start":   lot.get("startPrice"),
                "buyout":  lot.get("buyoutPrice"),
                "amount":  max(int(lot.get("amount") or 1), 1),
                "seller":  lot.get("sellerName", ""),
                "end_time":lot.get("endTime", ""),
                "first_seen": prev.get("first_seen", now),
                "last_seen":  now,
            }

    def detect_buyouts(self, item_id: str, cur: set) -> list:
        now_dt = datetime.now(timezone.utc)
        bought = []
        with self._lock:
            gone = [k for k, v in self.active_lots.items()
                    if v["item_id"] == item_id and v["lot_id"] not in cur]
            for key in gone:
                snap = self.active_lots.pop(key)
                end_str = snap.get("end_time", "")
                is_buy  = _is_buyout(end_str, now_dt)

                log.debug(f"    LOT GONE: {snap['lot_id']} end={end_str[:19] if end_str else '-'} buyout={is_buy}")

                if not is_buy:
                    continue
                price = snap.get("buyout") or snap.get("start") or 0
                amt   = max(snap.get("amount", 1) or 1, 1)
                if price <= 0:
                    log.warning(f"    LOT GONE но price=0, пропускаем")
                    continue
                sale = {
                    "item_id": item_id, "lot_id": snap["lot_id"],
                    "qlt": snap["qlt"], "ptn": snap["ptn"],
                    "price": price, "unit_price": price / amt,
                    "amount": amt, "seller": snap.get("seller", ""),
                    "sold_at": _now(),
                }
                self.sales.append(sale)
                bought.append(sale)
                ps = f" p{snap['ptn']}" if snap.get("ptn") else ""
                log.info(f"  💰 ПРОДАЖА: {item_id} q{snap['qlt']}{ps} {price:,} руб.")
        return bought

    # ── аналитика ─────────────────────────────────────────────────────

    def avg_price(self, item_id: str, qlt: int, max_ptn: bool = False) -> Optional[tuple]:
        """
        Средняя цена за единицу для (item_id, qlt).
        max_ptn=True  → только лоты с ptn==15 (максимальная заточка)
        max_ptn=False → все лоты БЕЗ максимальной заточки (ptn < 15 или None)
        """
        with self._lock:
            if max_ptn:
                ps = [s["unit_price"] for s in self.sales
                      if s["item_id"] == item_id and s["qlt"] == qlt
                      and s.get("ptn") == 15]
            else:
                ps = [s["unit_price"] for s in self.sales
                      if s["item_id"] == item_id and s["qlt"] == qlt
                      and (s.get("ptn") or 0) < 15]
        if not ps:
            return None
        return mean(ps[-30:]), len(ps)

    def sales_for(self, item_id: str) -> list:
        with self._lock:
            return [s for s in self.sales if s["item_id"] == item_id]

    def lots_for(self, item_id: str) -> list:
        with self._lock:
            return [v for v in self.active_lots.values() if v["item_id"] == item_id]

    def all_sales(self) -> list:
        with self._lock:
            return list(self.sales)

    def stats(self) -> dict:
        with self._lock:
            return {
                "total_sales":     len(self.sales),
                "active_lots":     len(self.active_lots),
                "items_with_data": len({s["item_id"] for s in self.sales}),
                "items_tracked":   len(self.watch_set),
                "oldest":          min((s["sold_at"] for s in self.sales), default=None),
            }

    # ── watch list ────────────────────────────────────────────────────

    def watch_add(self, iid: str):
        with self._lock: self.watch_set.add(iid)
        self.save()

    def watch_del(self, iid: str):
        with self._lock: self.watch_set.discard(iid)
        self.save()

    def watched(self) -> list:
        with self._lock: return list(self.watch_set)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_buyout(end_str: str, now_dt: datetime) -> bool:
    if not end_str:
        log.debug("      _is_buyout: end_time empty")
        return False
    for parse in (
        lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")),
        lambda s: datetime.fromtimestamp(float(s), tz=timezone.utc),
    ):
        try:
            end_dt = parse(end_str)
            diff   = (end_dt - now_dt).total_seconds()
            result = now_dt < end_dt + timedelta(seconds=60)
            log.debug(f"      _is_buyout: diff={diff:.0f}s result={result}")
            return result
        except Exception as e:
            log.debug(f"      _is_buyout parse err: {e}")
    log.warning(f"      _is_buyout: cannot parse '{end_str}'")
    return False


db = DB()

# ═══════════════════════════════════════════════════════════════════
#  БАЗА АРТЕФАКТОВ
# ═══════════════════════════════════════════════════════════════════

art_db: dict = {}   # item_id -> {name, qlt, subcat}


# Маппинг суффикса core.quality.XXX → int (используется в _extract_quality_from_json)
_QUALITY_SUFFIX = {
    "junk": 0, "common": 1, "uncommon": 2,
    "rare": 3, "epic": 4, "legendary": 5,
}


def _extract_quality_from_json(data: dict) -> Optional[int]:
    """
    Качество артефакта в базе EXBO хранится в infoBlocks как key-value элемент
    где key.key начинается с "core.quality."
    Примеры: core.quality.common, core.quality.rare, core.quality.legendary
    """
    # Маппинг суффикса ключа → качество
    KEY_TO_QLT = {
        "junk":       0,
        "common":     1,
        "uncommon":   2,
        "rare":       3,
        "epic":       4,
        "legendary":  5,
    }

    for block in data.get("infoBlocks", []):
        for elem in block.get("elements", []):
            if elem.get("type") != "key-value":
                continue
            key_obj = elem.get("key", {})
            if not isinstance(key_obj, dict):
                continue
            key_id = key_obj.get("key", "")
            # Ищем ключи вида "core.quality.XXX"
            if key_id.startswith("core.quality."):
                suffix = key_id.split(".")[-1].lower()
                if suffix in KEY_TO_QLT:
                    return KEY_TO_QLT[suffix]
                # Если суффикс неизвестен — пробуем по тексту value
                val_obj = elem.get("value", {})
                if isinstance(val_obj, dict):
                    val_text = (val_obj.get("lines", {}).get("en") or "").lower()
                    if val_text in KEY_TO_QLT:
                        return KEY_TO_QLT[val_text]

    # Фолбэк: включаем артефакт с качеством 0 чтобы не терять предмет
    return 0


def _get_text(obj) -> str:
    """Извлечь текст из объекта name/key/value."""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        lines = obj.get("lines", {})
        return lines.get("ru") or lines.get("en") or obj.get("text", "") or obj.get("key", "")
    return ""


def load_art_db(force=False):
    global art_db
    cache = _load_cache()

    if cache and not force:
        sha = _latest_sha()
        if sha and sha == cache.get("sha"):
            art_db = cache["items"]
            log.info(f"✅ Кэш актуален: {len(art_db)} артефактов")
            return
        log.info("🔄 Репозиторий обновился, перекачиваем...")

    log.info("Скачиваем ZIP с GitHub...")
    try:
        r = requests.get(GITHUB_ZIP, timeout=90)
        r.raise_for_status()
        log.info(f"Скачано {len(r.content)//1024} КБ")
    except Exception as e:
        log.error(f"Скачивание: {e}")
        if cache:
            art_db = cache["items"]
        return

    items: dict = {}
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        all_names = zf.namelist()
        log.info(f"Файлов в ZIP: {len(all_names)}")

        # Показываем папки для диагностики
        top4 = sorted({"/".join(n.split("/")[:4]) for n in all_names
                       if n.count("/") >= 3 and "items" in n})
        log.info(f"Папки: {top4[:30]}")

        # Все JSON из global/items/artefact/** (включая подпапки)
        art_files = [n for n in all_names
                     if n.endswith(".json")
                     and f"/{REALM}/items/artefact/" in n]
        log.info(f"Файлов artefact: {len(art_files)}")

        # Парсим все файлы — качество ищем в infoBlocks (core.quality.XXX)
        color_counts: dict = {}
        qlt_counts:   dict = {}
        for name in art_files:
            iid   = name.split("/")[-1].replace(".json", "")
            parts = name.split("/")
            # subcat = папка внутри artefact (biochemical, radiation, ...)
            # Путь: .../artefact/SUBCAT/file.json → parts[-2] = SUBCAT
            subcat = parts[-2] if len(parts) >= 2 and parts[-2] != "artefact" else ""
            try:
                with zf.open(name) as jf:
                    data = json.load(jf)
            except Exception:
                continue

            c = data.get("color", "NONE")
            color_counts[c] = color_counts.get(c, 0) + 1

            # Пробуем получить качество из разных мест
            qlt = _extract_quality_from_json(data)
            if qlt is None:
                continue
            qlt_counts[qlt] = qlt_counts.get(qlt, 0) + 1

            lines   = data.get("name", {}).get("lines", {})
            display = lines.get("ru") or lines.get("en") or iid
            items[iid] = {"name": display, "qlt": qlt, "subcat": subcat}

        log.info(f"Цвета: {color_counts}")
        log.info(f"Качества: { {QUALITY.get(k,('?',))[0]+f'(q{k})': v for k,v in sorted(qlt_counts.items())} }")

    log.info(f"Загружено артефактов: {len(items)}")
    if items:
        sha = _latest_sha() or "unknown"
        _save_cache(sha, items)
        art_db = items
    elif cache:
        log.warning("Парсинг дал 0 — используем старый кэш")
        art_db = cache["items"]


def _load_cache() -> Optional[dict]:
    p = Path(CACHE_FILE)
    if p.exists():
        try: return json.loads(p.read_text("utf-8"))
        except Exception: pass
    return None


def _save_cache(sha: str, items: dict):
    Path(CACHE_FILE).write_text(
        json.dumps({"sha": sha, "updated_at": _now(), "items": items},
                   ensure_ascii=False, indent=2), "utf-8"
    )
    log.info(f"Кэш сохранён → {CACHE_FILE}")


def _latest_sha() -> Optional[str]:
    try:
        r = requests.get(GITHUB_COMMIT, headers={"Accept":"application/vnd.github+json"}, timeout=10)
        return r.json().get("sha") if r.ok else None
    except Exception:
        return None


def art_name(iid: str) -> str:
    return art_db.get(iid, {}).get("name", iid)


def art_base_qlt(iid: str) -> int:
    return art_db.get(iid, {}).get("qlt", 0)


def search_arts(q: str) -> list:
    q = q.lower()
    return sorted([(iid, info["name"]) for iid, info in art_db.items()
                   if q in info["name"].lower()], key=lambda x: x[1])

# ═══════════════════════════════════════════════════════════════════
#  STALCRAFT API
# ═══════════════════════════════════════════════════════════════════

def api_get(path: str, params=None) -> Optional[dict]:
    try:
        r = requests.get(
            f"{BASE_URL}{path}",
            headers={"Authorization": f"Bearer {STALCRAFT_TOKEN}"},
            params=params or {}, timeout=15,
        )
        if r.status_code == 429:
            log.warning("Rate limit — 35 сек...")
            time.sleep(35)
            return api_get(path, params)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        log.error(f"HTTP {e.response.status_code} {path}")
    except Exception as e:
        log.error(f"{path}: {e}")
    return None


def get_lots(item_id: str) -> list:
    d = api_get(f"/auction/{item_id}/lots", {"additional": "true", "limit": 200})
    return d.get("lots", []) if d else []


def extract_qlt_ptn(lot: dict, item_id: str) -> tuple:
    """
    Из реального API ответа:
    lot.additional.qlt = качество (0-5)
    lot.additional.ptn = заточка (0-15, может отсутствовать)
    """
    additional = lot.get("additional") or {}
    qlt = additional.get("qlt")
    ptn = additional.get("ptn")   # None если не заточен

    # Фолбэк: прямо в лоте (на случай будущих изменений API)
    if qlt is None:
        qlt = lot.get("qlt") or lot.get("quality")
    if ptn is None:
        ptn = lot.get("ptn") or lot.get("potency")

    # Финальный фолбэк — базовое качество из базы предметов
    if qlt is None:
        qlt = art_base_qlt(item_id)

    return (int(qlt) if qlt is not None else 0,
            int(ptn) if ptn is not None else None)

# ═══════════════════════════════════════════════════════════════════
#  АЛЕРТЫ
# ═══════════════════════════════════════════════════════════════════

_sent: set = set()


def _fmt(v) -> str:
    if isinstance(v, (int, float)):
        return f"{int(v):,}".replace(",", "\u202f")
    return str(v)


# Эмодзи-кружки качества для Telegram (Unicode filled circles)
QUALITY_CIRCLE = {
    0: "⚪",   # Обычный
    1: "🟢",   # Необычный
    2: "🔵",   # Особый
    3: "🟣",   # Редкий
    4: "🔴",   # Исключительный
    5: "🟡",   # Легендарный
}


def _lot_id(lot: dict, item_id: str) -> str:
    raw_id = lot.get("id")
    if raw_id:
        return str(raw_id)
    spawn = lot.get("additional", {}).get("spawn_time", "")
    start = lot.get("startTime", "")
    return f"{start}_{spawn}" if (start or spawn) else ""


def _build_alert_msg(item_id: str, lot: dict, qlt: int, ptn,
                     unit: float, avg: float, disc: float, n: int,
                     is_max_ptn: bool) -> str:
    """Формирует красивое сообщение алерта для Telegram."""
    qname  = QUALITY.get(qlt, ("?", "#fff"))[0]
    circle = QUALITY_CIRCLE.get(qlt, "⚪")
    name   = art_name(item_id)
    seller = lot.get("sellerName", "?")
    lid    = _lot_id(lot, item_id)[:20]   # укорачиваем длинный spawn-key

    ptn_line = ""
    if ptn and ptn > 0:
        ptn_line = f"⚡ Заточка:   <b>+{ptn}</b>\n"

    bonus_props = lot.get("additional", {}).get("bonus_properties", [])
    props_line  = ""
    if bonus_props:
        props_line = f"✨ Бонусы:    {', '.join(bonus_props)}\n"

    avg_label = "Средняя +15" if is_max_ptn else "Средняя"

    header = "🔥 ВЫГОДНЫЙ ЛОТ — EU 🔥" if disc >= 20 else "📉 СКИДКА НА АУКЦИОНЕ — EU"
    tag    = " [МАКС. ЗАТОЧКА]" if is_max_ptn else ""

    return (
        f"<b>{header}</b>{tag}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{circle} <b>{name}</b>\n"
        f"⭐ Качество:  <b>{qname}</b>\n"
        f"{ptn_line}"
        f"{props_line}"
        f"💰 Цена:      <b>{_fmt(unit)}</b> руб./шт.\n"
        f"📊 {avg_label}: {_fmt(avg)} руб. ({n} прод.)\n"
        f"📉 Скидка:    <b>{disc:.1f}%</b>\n"
        f"👤 Продавец:  {seller}\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━"
    )

def check_alert(item_id: str, lot: dict, qlt: int, ptn):
    lid        = _lot_id(lot, item_id)
    is_max_ptn = (ptn == 15)
    # Раздельный ключ для алертов: лоты с +15 и остальные не пересекаются
    key = f"{item_id}:{lid}:q{qlt}:{'max' if is_max_ptn else 'reg'}"
    if key in _sent:
        return

    res = db.avg_price(item_id, qlt, max_ptn=is_max_ptn)
    if not res or res[1] < MIN_SALES_ALERT:
        return
    avg, n = res

    amt = max(int(lot.get("amount") or 1), 1)
    raw = lot.get("buyoutPrice") or lot.get("startPrice")
    if not raw:
        return
    unit = float(raw) / amt

    if unit >= avg * (1 - DISCOUNT_THRESHOLD / 100):
        return

    disc = (1 - unit / avg) * 100
    msg  = _build_alert_msg(item_id, lot, qlt, ptn, unit, avg, disc, n, is_max_ptn)
    tg_send(msg, TELEGRAM_CHAT_ID)
    _sent.add(key)
    pstr = f" ⚡+{ptn}" if ptn else ""
    log.info(f"⚡ АЛЕРТ: {art_name(item_id)} q{qlt}{pstr} -{disc:.1f}%")

# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM — чистый HTTP, без библиотек
# ═══════════════════════════════════════════════════════════════════

def _tg(method: str, **kwargs) -> Optional[dict]:
    if not TELEGRAM_TOKEN:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
            json=kwargs, timeout=15,
        )
        return r.json() if r.ok else None
    except Exception as e:
        log.error(f"TG {method}: {e}")
        return None


def tg_send(text: str, chat_id: str, reply_markup=None):
    kw = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        kw["reply_markup"] = reply_markup
    return _tg("sendMessage", **kw)


def tg_edit(chat_id: str, msg_id: int, text: str, reply_markup=None):
    kw = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        kw["reply_markup"] = reply_markup
    return _tg("editMessageText", **kw)


def tg_answer(callback_id: str):
    _tg("answerCallbackQuery", callback_id=callback_id)


def tg_send_photo(chat_id: str, photo_bytes: bytes, caption: str = ""):
    if not TELEGRAM_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"photo": ("chart.png", photo_bytes, "image/png")},
            timeout=30,
        )
    except Exception as e:
        log.error(f"TG sendPhoto: {e}")


def kb(*rows) -> dict:
    """Создать InlineKeyboard из строк кнопок [(text, callback_data), ...]."""
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row]
                                 for row in rows]}


# ── Состояние бота ────────────────────────────────────────────────────
_tg_offset    = 0
_user_state   : dict = {}   # chat_id -> "search" | None


def _handle_update(upd: dict):
    global _tg_offset
    _tg_offset = upd["update_id"] + 1

    # Callback query (кнопки)
    if "callback_query" in upd:
        cq      = upd["callback_query"]
        chat_id = str(cq["message"]["chat"]["id"])
        msg_id  = cq["message"]["message_id"]
        data    = cq["data"]
        tg_answer(cq["id"])
        _handle_callback(chat_id, msg_id, data)
        return

    # Обычное сообщение
    if "message" not in upd:
        return
    msg     = upd["message"]
    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text", "").strip()

    if text.startswith("/start") or text.startswith("/menu"):
        _user_state[chat_id] = None
        _send_main(chat_id)
    elif text.startswith("/status"):
        _send_status(chat_id)
    elif _user_state.get(chat_id) == "search" or not text.startswith("/"):
        _user_state[chat_id] = None
        _handle_search(chat_id, text)


def _send_main(chat_id: str):
    tg_send(
        "⚗️ <b>Stalcraft Artefact Monitor</b>\n\nВыбери действие:",
        chat_id,
        kb([("🔍 Найти артефакт", "search"),
            ("📋 Мой список",     "watchlist")],
           [("📊 Статус",          "status")])
    )


def _send_status(chat_id: str, msg_id: int = None):
    st  = db.stats()
    txt = (
        f"📊 <b>Статус</b>\n\n"
        f"Продаж: <b>{st['total_sales']}</b>\n"
        f"Активных лотов: <b>{st['active_lots']}</b>\n"
        f"Арт. с данными: <b>{st['items_with_data']}</b>\n"
        f"В отслеживании: <b>{st['items_tracked']}</b>\n"
        f"С: {(st['oldest'] or '—')[:19]}\n\n"
        f"🌐 http://localhost:{WEB_PORT}"
    )
    markup = kb([("⬅️ Главная", "main")])
    if msg_id:
        tg_edit(chat_id, msg_id, txt, markup)
    else:
        tg_send(txt, chat_id, markup)


def _handle_search(chat_id: str, query: str):
    results = search_arts(query)
    if not results:
        tg_send(f"Не найдено: «{query}»", chat_id,
                kb([("⬅️ Главная", "main")]))
        return
    if len(results) == 1:
        _send_item(chat_id, None, results[0][0])
        return
    rows = [[( n, f"item:{iid}")] for iid, n in results[:15]]
    rows.append([("⬅️ Главная", "main")])
    tg_send(f"🔍 Найдено {len(results)}:", chat_id, kb(*rows))


def _send_item(chat_id: str, msg_id: Optional[int], item_id: str):
    name  = art_name(item_id)
    in_wl = item_id in db.watch_set
    lines = [f"💎 <b>{name}</b>", f"<code>{item_id}</code>", ""]

    has_data = False
    for qlt in range(6):
        circle  = QUALITY_CIRCLE.get(qlt, "⚪")
        qname   = QUALITY[qlt][0]
        res_reg = db.avg_price(item_id, qlt, max_ptn=False)
        res_max = db.avg_price(item_id, qlt, max_ptn=True)
        if res_reg or res_max:
            has_data = True
            lines.append(f"{circle} <b>{qname}</b>")
            if res_reg:
                avg, n = res_reg
                lines.append(f"   без +15: {_fmt(avg)} руб. ({n} прод.)")
            if res_max:
                avg, n = res_max
                lines.append(f"   ⚡+15:   {_fmt(avg)} руб. ({n} прод.)")

    if not has_data:
        lines.append("  Данных пока нет")
    txt    = "\n".join(lines)
    markup = kb(
        [("🔕 Убрать" if in_wl else "🔔 Следить",
          f"{'unwatch' if in_wl else 'watch'}:{item_id}")],
        [("📈 График",  f"chart:{item_id}"),
         ("📋 Лоты",    f"lots:{item_id}")],
        [("⬅️ Главная", "main")],
    )
    if msg_id:
        tg_edit(chat_id, msg_id, txt, markup)
    else:
        tg_send(txt, chat_id, markup)


def _handle_callback(chat_id: str, msg_id: int, data: str):
    if data == "main":
        tg_edit(chat_id, msg_id,
                "⚗️ <b>Stalcraft Artefact Monitor</b>\n\nВыбери действие:",
                kb([("🔍 Найти артефакт", "search"),
                    ("📋 Мой список",     "watchlist")],
                   [("📊 Статус",         "status")]))

    elif data == "search":
        _user_state[chat_id] = "search"
        tg_edit(chat_id, msg_id,
                "🔍 Напиши название артефакта (или часть):",
                kb([("⬅️ Главная", "main")]))

    elif data == "status":
        _send_status(chat_id, msg_id)

    elif data == "watchlist":
        wl = db.watched()
        if not wl:
            tg_edit(chat_id, msg_id, "Список пуст.",
                    kb([("🔍 Поиск","search"), ("⬅️","main")]))
            return
        rows = [[( f"❌ {art_name(i)}", f"unwatch:{i}")] for i in wl]
        rows.append([("⬅️ Главная", "main")])
        tg_edit(chat_id, msg_id, "📋 Твои артефакты:", kb(*rows))

    elif data.startswith("item:"):
        _send_item(chat_id, msg_id, data[5:])

    elif data.startswith("watch:"):
        iid = data[6:]; db.watch_add(iid)
        tg_edit(chat_id, msg_id,
                f"✅ <b>{art_name(iid)}</b> добавлен.",
                kb([("📋 Список","watchlist"), ("⬅️","main")]))

    elif data.startswith("unwatch:"):
        iid = data[8:]; db.watch_del(iid)
        tg_edit(chat_id, msg_id,
                f"🔕 <b>{art_name(iid)}</b> убран.",
                kb([("📋 Список","watchlist"), ("⬅️","main")]))

    elif data.startswith("chart:"):
        iid = data[6:]
        tg_edit(chat_id, msg_id, f"⏳ Генерирую график {art_name(iid)}…",
                kb([("⬅️", f"item:{iid}")]))
        png = make_chart(iid)
        if png:
            tg_send_photo(chat_id, png, f"📈 {art_name(iid)}")
        else:
            tg_send("Данных пока нет.", chat_id)
        _send_item(chat_id, None, iid)

    elif data.startswith("lots:"):
        iid  = data[5:]
        lots = db.lots_for(iid)
        if not lots:
            tg_edit(chat_id, msg_id,
                    f"Нет активных лотов для <b>{art_name(iid)}</b>.",
                    kb([(f"⬅️", f"item:{iid}")]))
            return
        # Группируем по качеству, внутри сортируем по цене
        lines = [f"📋 <b>{art_name(iid)}</b>\n"]
        by_qlt = {}
        for lot in lots:
            q = lot.get("qlt", 0)
            by_qlt.setdefault(q, []).append(lot)

        shown = 0
        for qlt in sorted(by_qlt.keys()):
            circle = QUALITY_CIRCLE.get(qlt, "⚪")
            qname  = QUALITY.get(qlt, ("?",))[0]
            lines.append(f"{circle} <b>{qname}</b>")
            qsorted = sorted(by_qlt[qlt],
                             key=lambda x: x.get("buyout") or x.get("start") or 0)
            for lot in qsorted[:8]:   # до 8 лотов на качество
                amt = max(lot.get("amount", 1) or 1, 1)
                pr  = lot.get("buyout") or lot.get("start") or 0
                ps  = f" ⚡+{lot['ptn']}" if lot.get("ptn") else ""
                lines.append(f"  • {_fmt(pr // amt)} руб./шт.{ps}")
                shown += 1
            if len(qsorted) > 8:
                lines.append(f"  <i>...ещё {len(qsorted)-8}</i>")
        tg_edit(chat_id, msg_id, "\n".join(lines),
                kb([(f"⬅️", f"item:{iid}")]))


def tg_polling_loop():
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN не задан — бот отключён")
        return
    global _tg_offset
    log.info("🤖 Telegram бот запущен (long polling)")
    # Отдельная сессия для бота с увеличенным таймаутом
    tg_session = requests.Session()
    while True:
        try:
            r = tg_session.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                json={"offset": _tg_offset, "timeout": 25},
                timeout=30,   # чуть больше чем timeout внутри запроса
            )
            if r.ok:
                resp = r.json()
                for upd in resp.get("result", []):
                    try:
                        _handle_update(upd)
                    except Exception as e:
                        log.error(f"TG update: {e}")
        except requests.exceptions.ReadTimeout:
            pass   # нормально для long polling — просто повторяем
        except Exception as e:
            log.error(f"TG polling: {e}")
            time.sleep(5)

# ═══════════════════════════════════════════════════════════════════
#  ГРАФИКИ
# ═══════════════════════════════════════════════════════════════════

def make_chart(item_id: str) -> Optional[bytes]:
    if not MPL:
        return None
    sales = db.sales_for(item_id)
    if not sales:
        return None
    fig, ax = plt.subplots(figsize=(11, 5))
    fig.patch.set_facecolor("#0a0c10")
    ax.set_facecolor("#12161e")
    plotted = False
    for qlt in range(6):
        pts = sorted(
            [(datetime.fromisoformat(s["sold_at"]), s["unit_price"], s.get("ptn"))
             for s in sales if s["qlt"] == qlt],
            key=lambda x: x[0]
        )
        if not pts: continue
        xs, ys, ptns = zip(*pts)
        qname, color = QUALITY[qlt]
        ax.plot(xs, ys, "o-", color=color, label=qname,
                linewidth=1.8, markersize=5, alpha=0.9)
        for x, y, ptn in zip(xs, ys, ptns):
            if ptn:
                ax.annotate(f"+{ptn}", (x, y), textcoords="offset points",
                            xytext=(0, 7), fontsize=6.5, color=color,
                            ha="center", fontweight="bold")
        plotted = True
    if not plotted:
        plt.close(fig); return None
    ax.set_title(art_name(item_id), color="#c8d8e8", fontsize=13, pad=12)
    ax.set_xlabel("Дата", color="#4a5568", fontsize=9)
    ax.set_ylabel("Цена / шт. (руб.)", color="#4a5568", fontsize=9)
    ax.tick_params(colors="#4a5568", labelsize=8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
    fig.autofmt_xdate(rotation=28)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _: f"{int(v):,}".replace(",", " "))
    )
    for sp in ax.spines.values(): sp.set_edgecolor("#1e2535")
    ax.grid(color="#1e2535", linewidth=0.5, alpha=0.7)
    ax.legend(facecolor="#1a2030", edgecolor="#1e2535",
              labelcolor="#c8d8e8", fontsize=8)
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=130, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()

# ═══════════════════════════════════════════════════════════════════
#  МОНИТОРИНГ
# ═══════════════════════════════════════════════════════════════════

def process_item(item_id: str):
    lots   = get_lots(item_id)
    cur    = set()

    if lots:
        log.debug(f"    {len(lots)} лотов для {item_id}")

    for lot in lots:
        raw_id = lot.get("id")
        if raw_id:
            lid = str(raw_id)
        else:
            spawn = lot.get("additional", {}).get("spawn_time", "")
            start = lot.get("startTime", "")
            lid   = f"{start}_{spawn}" if (start or spawn) else ""
        if not lid: continue
        cur.add(lid)
        qlt, ptn = extract_qlt_ptn(lot, item_id)
        db.upsert(item_id, lot, qlt, ptn)

    prev_active = len([k for k in db.active_lots if k.startswith(item_id + ":")])
    bought = db.detect_buyouts(item_id, cur)
    after_active = len([k for k in db.active_lots if k.startswith(item_id + ":")])

    if bought:
        log.info(f"    ✅ {len(bought)} выкупов задетектировано")
    if prev_active != after_active:
        log.info(f"    Лотов в БД: {prev_active} → {after_active}")

    for lot in lots:
        qlt, ptn = extract_qlt_ptn(lot, item_id)
        check_alert(item_id, lot, qlt, ptn)


def monitor_loop():
    log.info("=" * 58)
    log.info("  Stalcraft Artefact Monitor — EU")
    log.info(f"  Порог: {DISCOUNT_THRESHOLD}% | Мин.продаж: {MIN_SALES_ALERT} | Опрос: {POLL_INTERVAL}с")
    log.info("=" * 58)
    cycle = 0
    while True:
        cycle += 1
        ids = list(art_db.keys())
        st  = db.stats()
        log.info(f"── #{cycle} | {len(ids)} арт. | продаж:{st['total_sales']} лотов:{st['active_lots']} ──")
        for i, iid in enumerate(ids, 1):
            log.info(f"  [{i}/{len(ids)}] {art_name(iid)} ({iid})")
            process_item(iid)
            time.sleep(REQUEST_DELAY)
        if cycle % 5 == 0:
            db.save()
        log.info(f"Цикл #{cycle} готов. Следующий через {POLL_INTERVAL} сек.")
        time.sleep(POLL_INTERVAL)

# ═══════════════════════════════════════════════════════════════════
#  ВЕБ-ДАШБОРД
# ═══════════════════════════════════════════════════════════════════

_DASH = Path(__file__).parent / "dashboard.html"


class WebH(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            if _DASH.exists():
                self._ok("text/html; charset=utf-8", _DASH.read_bytes())
            else:
                self._ok("text/plain", b"Put dashboard.html next to stalcraft_monitor.py")
        elif self.path == "/api/data":
            sales = db.all_sales()
            arts  = sorted(
                [{"id": iid, "name": info["name"], "qlt": info["qlt"],
                  "subcat": info.get("subcat",""),
                  "sales_count": sum(1 for s in sales if s["item_id"] == iid)}
                 for iid, info in art_db.items()],
                key=lambda x: (-x["sales_count"], x["name"])
            )
            body = json.dumps({
                "stats":   db.stats(),
                "arts":    arts,
                "sales":   sales[-3000:],
                "lots":    list(db.active_lots.values()),
                "quality": {str(k): {"name": v[0], "color": v[1]}
                            for k, v in QUALITY.items()},
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
        for k, v in (extra or []):
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)


def start_web():
    HTTPServer(("0.0.0.0", WEB_PORT), WebH).serve_forever()

# ═══════════════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--update",    action="store_true", help="Перекачать базу артефактов")
    ap.add_argument("--debug-lot", metavar="ITEM_ID",   help="Сырой JSON лотов для артефакта")
    args = ap.parse_args()

    if args.update:
        # Удаляем старый кэш чтобы точно перекачать
        Path(CACHE_FILE).unlink(missing_ok=True)
        load_art_db(force=True)
        sys.exit(0)

    if args.debug_lot:
        if STALCRAFT_TOKEN == "ВАШ_ТОКЕН_ЗДЕСЬ":
            print("Задай STALCRAFT_TOKEN"); sys.exit(1)
        load_art_db()
        data = api_get(f"/auction/{args.debug_lot}/lots",
                       {"additional": "true", "limit": 2})
        print(json.dumps(data, ensure_ascii=False, indent=2))
        sys.exit(0)

    if STALCRAFT_TOKEN == "ВАШ_ТОКЕН_ЗДЕСЬ":
        print("❌ Задай STALCRAFT_TOKEN в настройках"); sys.exit(1)

    load_art_db()
    if not art_db:
        log.error("База пуста — запусти: python stalcraft_monitor.py --update")
        sys.exit(1)

    log.info(f"Артефактов: {len(art_db)}")
    threading.Thread(target=start_web,         daemon=True).start()
    threading.Thread(target=tg_polling_loop,   daemon=True).start()
    log.info(f"🌐 Дашборд: http://localhost:{WEB_PORT}")

    try:
        monitor_loop()
    except KeyboardInterrupt:
        log.info("Остановлено.")
        db.save()