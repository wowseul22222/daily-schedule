import os
import re
import sys
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)
except Exception:
    pass

KST = ZoneInfo("Asia/Seoul")

SITE_NO = "0013"
SITE_NAME = "CGV 용산아이파크몰"
CO_CD = "A420"
RTCTL_SCOP_CD = "08"
GV_CODE = "0023"

DAYS = 43  # 기존 workflow 검증 호환용. 실제 감시 범위는 +4~+21일만 사용.
SCAN_START_OFFSET = 4
SCAN_END_OFFSET = 21
SCAN_CYCLE_SECONDS = 8.0
PREPARING_INTERVAL = 8.0
MIN_REQUEST_GAP = 0.35
RATE_LIMIT_COOLDOWN = 60.0
SUMMARY_SECONDS = 600.0

FAST_SCAN_MINUTES = {0, 30}
FAST_SCAN_START_OFFSET = 4
FAST_SCAN_END_OFFSET = 21
FAST_SCAN_WORKERS = 2
FAST_SCAN_ENABLED = False  # 8초 상시감시와 중복되는 00/30 burst 비활성화

# GitHub Actions workflow가 실행 구간을 RUN_SECONDS로 주입한다.
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "86400"))

BOOKING_PAGE = "https://cgv.co.kr/cnm/movieBook"
API_URL = "https://cgv.co.kr/api/v1/booking/searchMovScnInfo"

# CGV 영상부가체험 직접 필터. 일반 극장/날짜 API보다 GV 영화/날짜가 먼저 뜨는 경우를 잡는다.
DIRECT_MOVIE_LIST_URL = "https://cgv.co.kr/api/v1/booking/searchAtktTopPostrList"
DIRECT_DATE_LIST_URL = "https://cgv.co.kr/api/v1/booking/searchSiteScnscYmdListByMov"
DIRECT_FILTER_CODE = GV_CODE
DIRECT_SCAN_INTERVAL = 8.0
DIRECT_SCAN_TIMEOUT = 12

# 이번 GV ONLY 개편용 새 상태 파일. 이전 통합 감시 상태와 섞지 않는다.
STATE_FILE = "seen_cgv_yongsan_gv_v2.json"
BASELINE_FILE = "baseline_cgv_yongsan_gv_v2.done"
BOOKING_STATE_FILE = "cgv_yongsan_gv_booking_state_v2.json"
BOOKING_STATE_SCHEMA = "CGV_YONGSAN_GV_ONLY_V2_20260912"

# GitHub Secrets
DISCORD_WEBHOOK = os.environ.get("CY_WEBHOOK", "").strip()
DISCORD_MENTION_ID = os.environ.get("DISCORD_MENTION_ID", "").strip()

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": BOOKING_PAGE,
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/152.0.0.0 Safari/537.36"
    ),
}

BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}


class BrowserResponse:
    def __init__(self, status_code, text, url, content_type=""):
        self.status_code = int(status_code or 0)
        self.text = text or ""
        self.url = url or ""
        self.headers = {"content-type": content_type or ""}

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"{self.status_code} Client Error: Browser fetch for url: {self.url}"
            )


class BrowserSession:
    """CGV 페이지를 실제 Chromium으로 연 뒤, 그 페이지 컨텍스트 안에서 GET 요청한다."""

    def __init__(self, label="CGV"):
        self.label = label
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage"],
        )
        self._context = self._browser.new_context(
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            viewport={"width": 1365, "height": 768},
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(20_000)
        self._blocked_until = 0.0
        self.booking_page_status = 0
        self._warmup()

    def _warmup(self):
        try:
            response = self._page.goto(
                BOOKING_PAGE,
                wait_until="domcontentloaded",
                timeout=30_000,
            )
            self.booking_page_status = response.status if response else 0
            self._page.wait_for_timeout(1500)
            print(
                f"🌐 {self.label} PLAYWRIGHT PAGE STATUS: "
                f"{self.booking_page_status} | {self._page.url}"
            )
        except Exception as error:
            self.booking_page_status = 0
            print(f"⚠️ {self.label} PLAYWRIGHT PAGE WARNING: {repr(error)}")

    @staticmethod
    def _build_url(url, params):
        if not params:
            return url
        query = urlencode(params, doseq=True)
        return url + ("&" if "?" in url else "?") + query

    @staticmethod
    def _page_target(url):
        prefix = "https://cgv.co.kr"
        if url.startswith(prefix):
            target = url[len(prefix):]
            return target or "/"
        return url

    def _fetch_once(self, final_url, timeout):
        target = self._page_target(final_url)
        timeout_ms = max(1000, int(float(timeout or 20) * 1000))
        result = self._page.evaluate(
            """
            async ({target, timeoutMs}) => {
              const controller = new AbortController();
              const timer = setTimeout(() => controller.abort(), timeoutMs);
              try {
                const response = await fetch(target, {
                  method: 'GET',
                  credentials: 'include',
                  cache: 'no-store',
                  headers: {
                    'Accept': 'application/json, text/plain, */*'
                  },
                  signal: controller.signal
                });
                const text = await response.text();
                return {
                  status: response.status,
                  text,
                  url: response.url,
                  contentType: response.headers.get('content-type') || ''
                };
              } catch (error) {
                return {error: String(error)};
              } finally {
                clearTimeout(timer);
              }
            }
            """,
            {"target": target, "timeoutMs": timeout_ms},
        )
        if not isinstance(result, dict):
            raise requests.RequestException(f"unexpected browser result: {result!r}")
        if result.get("error"):
            raise requests.RequestException(result["error"])
        return BrowserResponse(
            result.get("status", 0),
            result.get("text", ""),
            result.get("url", final_url),
            result.get("contentType", ""),
        )

    def get(self, url, params=None, headers=None, timeout=20):
        # headers 인자는 기존 함수 인터페이스 호환용이다. 브라우저가 실제 Referer/쿠키를 관리한다.
        remaining = self._blocked_until - time.monotonic()
        if remaining > 0:
            print(f"🛡️ CGV 403 보호 대기 {remaining:.0f}초")
            time.sleep(remaining)
            self._warmup()

        final_url = self._build_url(url, params or {})
        response = self._fetch_once(final_url, timeout)

        # 일시적인 세션/페이지 상태 문제면 정상 페이지를 한 번 다시 연 뒤 단 1회 재시도한다.
        if response.status_code == 403:
            print("⚠️ 브라우저 컨텍스트 403 → 예매페이지 1회 재가열 후 재시도")
            self._warmup()
            time.sleep(2.0)
            response = self._fetch_once(final_url, timeout)
            if response.status_code == 403:
                self._blocked_until = time.monotonic() + 60.0

        return response

    def close(self):
        for obj in (self._page, self._context, self._browser):
            try:
                obj.close()
            except Exception:
                pass
        try:
            self._pw.stop()
        except Exception:
            pass


def now_kst():
    return datetime.now(KST)


def clean(value):
    return " ".join(str(value or "").split())


def normalize_code(value):
    text = clean(value)
    return text.zfill(4) if text.isdigit() else text


def all_row_text(value):
    parts = []

    def walk(item):
        if isinstance(item, dict):
            for key, val in item.items():
                if val is not None and not isinstance(val, (dict, list, tuple, set)):
                    key_text = clean(key)
                    val_text = clean(val)
                    if key_text and val_text:
                        parts.append(f"{key_text}={val_text}")
                walk(val)
        elif isinstance(item, (list, tuple, set)):
            for val in item:
                walk(val)
        elif item is not None:
            text = clean(item)
            if text:
                parts.append(text)

    walk(value)
    return " | ".join(parts)


def make_dates():
    """GV 신규 오픈 가능성이 높은 +4~+21일(18일)만 감시한다."""
    today = now_kst().date()
    return [
        (today + timedelta(days=offset)).strftime("%Y%m%d")
        for offset in range(SCAN_START_OFFSET, SCAN_END_OFFSET + 1)
    ]


def pretty_date(date):
    dt = datetime.strptime(date, "%Y%m%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    return f"{dt:%Y.%m.%d}({weekdays[dt.weekday()]})"


def pretty_time(value):
    text = clean(value).replace(":", "")
    if len(text) == 4 and text.isdigit():
        return f"{text[:2]}:{text[2:]}"
    return clean(value)


def parse_int(value):
    if value is None:
        return None
    match = re.search(r"-?\d+", str(value))
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def send_discord(message):
    payload = {
        "content": message,
        "flags": 4,
        "allowed_mentions": {
            "parse": [],
            "users": [DISCORD_MENTION_ID],
        },
    }
    try:
        response = requests.post(
            DISCORD_WEBHOOK,
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        print("DISCORD SENT:", response.status_code)
        return True
    except Exception as error:
        print("❌ DISCORD ERROR:", repr(error))
        return False


def load_seen():
    if not os.path.exists(STATE_FILE):
        return set()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data) if isinstance(data, list) else set()
    except Exception as error:
        print("⚠️ SEEN STATE LOAD ERROR:", repr(error))
        return set()


def save_seen(seen):
    try:
        temp = STATE_FILE + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(sorted(seen), f, ensure_ascii=False, indent=2)
        os.replace(temp, STATE_FILE)
    except Exception as error:
        print("⚠️ SEEN STATE SAVE ERROR:", repr(error))


def baseline_done():
    return os.path.exists(BASELINE_FILE)


def mark_baseline_done():
    with open(BASELINE_FILE, "w", encoding="utf-8") as f:
        f.write(now_kst().isoformat())
    print("BASELINE MARKER CREATED")


def load_booking_state():
    if not os.path.exists(BOOKING_STATE_FILE):
        return {}, False
    try:
        with open(BOOKING_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or data.get("schema") != BOOKING_STATE_SCHEMA:
            return {}, False
        shows = data.get("shows")
        return (shows, True) if isinstance(shows, dict) else ({}, False)
    except Exception as error:
        print("⚠️ BOOKING STATE LOAD ERROR:", repr(error))
        return {}, False


def save_booking_state(show_state):
    payload = {
        "schema": BOOKING_STATE_SCHEMA,
        "updated_at_kst": now_kst().isoformat(),
        "shows": show_state,
    }
    try:
        temp = BOOKING_STATE_FILE + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(temp, BOOKING_STATE_FILE)
    except Exception as error:
        print("⚠️ BOOKING STATE SAVE ERROR:", repr(error))


def is_gv_row(row):
    # CGV 실제 GV 코드 0023을 최우선으로 사용한다.
    if normalize_code(row.get("videoAddexpCd")) == GV_CODE:
        return True

    # 코드가 비어 있는 예외 응답만 텍스트로 보조 판정한다.
    event_fields = [
        "videoAddexpCdNm", "videoAddexpNm", "videoAddexpCont",
        "eventNm", "eventName", "specialEventNm", "specialEventName",
        "addexpNm", "addexpName", "expoProdNm", "movNm", "movName",
    ]
    event_text = " | ".join(clean(row.get(k)) for k in event_fields if clean(row.get(k)))
    compact = re.sub(r"\s+", "", event_text)
    if "관객과의대화" in compact:
        return True
    return bool(re.search(r"(?<![A-Z0-9])GV(?![A-Z0-9])", event_text.upper()))


def classify_booking_state(row):
    full_text = all_row_text(row)
    compact = re.sub(r"\s+", "", full_text)
    upper = full_text.upper()

    if "예매준비중" in compact:
        return "PREPARING", "text:예매준비중"
    if "매진" in compact or "SOLD OUT" in upper or "SOLDOUT" in upper:
        return "SOLD_OUT", "text:매진"

    cntl = clean(row.get("cntlYn")).upper()
    if cntl == "Y":
        return "PREPARING", "cntlYn=Y"

    seat_count = None
    seat_source = ""
    for field in ("frSeatCnt", "restSeatCnt", "remainSeatCnt", "remainSeats", "seatCnt"):
        if field in row and row.get(field) is not None:
            value = parse_int(row.get(field))
            if value is not None:
                seat_count = value
                seat_source = field
                break

    if seat_count is not None and seat_count > 0:
        return "OPEN", f"{seat_source}={seat_count}"

    book_flag = clean(row.get("bookYn") or row.get("bookingYn") or row.get("rsvYn")).upper()
    if seat_count == 0 and (cntl == "N" or book_flag == "Y"):
        return "SOLD_OUT", f"{seat_source}=0"

    return "UNKNOWN", "no-explicit-status"


def event_key(date, row):
    return "|".join([
        SITE_NO,
        date,
        clean(row.get("movNo")),
        clean(row.get("prodNo")),
        clean(row.get("scnsNo")),
        clean(row.get("scnSseq")),
        clean(row.get("scnsrtTm")),
        "GV",
    ])


def make_booking_link(date, row):
    params = {
        "movNo": clean(row.get("movNo")),
        "scnYmd": date,
        "siteNo": SITE_NO,
        "siteNm": SITE_NAME,
        "scnsNo": clean(row.get("scnsNo")),
        "scnSseq": clean(row.get("scnSseq")),
    }
    return "https://cgv.co.kr/cnm/movieBook/movie?" + urlencode(params)


def normalize_event(date, row):
    status, source = classify_booking_state(row)
    return {
        "date": date,
        "type": "GV",
        "movie": clean(row.get("movNm") or row.get("movName") or row.get("expoProdNm")),
        "mov_no": clean(row.get("movNo")),
        "prod_no": clean(row.get("prodNo")),
        "screen": clean(
            row.get("expoScnsNm")
            or row.get("siteScnsNm")
            or row.get("scnsNm")
            or row.get("scnsName")
            or row.get("screenNm")
            or row.get("screenName")
        ),
        "time": clean(row.get("scnsrtTm")),
        "end_time": clean(row.get("scnendTm") or row.get("scnEndTm") or row.get("endTime")),
        "status": status,
        "status_source": source,
        "link": make_booking_link(date, row),
        "row": row,
    }


def state_record(event, status=None):
    return {
        "status": status or event.get("status", "UNKNOWN"),
        "date": event.get("date", ""),
        "type": "GV",
        "movie": event.get("movie", ""),
        "mov_no": event.get("mov_no", ""),
        "prod_no": event.get("prod_no", ""),
        "screen": event.get("screen", ""),
        "time": event.get("time", ""),
        "end_time": event.get("end_time", ""),
        "status_source": event.get("status_source", ""),
        "updated_at_kst": now_kst().isoformat(),
    }


def iter_dicts(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dicts(child)


def extract_direct_movies(data):
    movies = {}
    for row in iter_dicts(data):
        mov_no = clean(row.get("movNo"))
        if not mov_no:
            continue
        movie = clean(row.get("movNm") or row.get("expoProdNm") or row.get("iMovNm") or row.get("movName"))
        movies[mov_no] = movie
    return movies


def extract_direct_dates(data):
    dates = set()
    for row in iter_dicts(data):
        ymd = clean(row.get("scnYmd"))
        if re.fullmatch(r"\d{8}", ymd):
            dates.add(ymd)
    return dates


def direct_filter_link(date, mov_no):
    return "https://cgv.co.kr/cnm/movieBook/movie?" + urlencode({
        "movNo": clean(mov_no),
        "scnYmd": clean(date),
        "siteNo": SITE_NO,
        "siteNm": SITE_NAME,
        "div": "VIDEO_ADDEXP_CD",
        "attrCd": DIRECT_FILTER_CODE,
    })


def direct_event_key(date, mov_no):
    return "|".join([SITE_NO, clean(date), clean(mov_no), "GV_DIRECT_0023"])


def make_direct_event(date, mov_no, movie):
    return {
        "date": clean(date),
        "type": "GV",
        "movie": clean(movie) or f"영화번호 {clean(mov_no)}",
        "mov_no": clean(mov_no),
        "prod_no": "",
        "screen": "",
        "time": "",
        "end_time": "",
        "status": "UNKNOWN",
        "status_source": "VIDEO_ADDEXP_CD=0023 direct filter",
        "link": direct_filter_link(date, mov_no),
        "row": {
            "movNo": clean(mov_no),
            "movNm": clean(movie),
            "videoAddexpCd": DIRECT_FILTER_CODE,
            "videoAddexpCdNm": "GV",
            "_directSynthetic": True,
        },
    }


def scan_direct_filter(session):
    """0023 전용 영화목록 -> 용산 날짜목록. 상세 회차가 숨겨져 있어도 영화/날짜 신호를 먼저 잡는다."""
    today = now_kst().date()
    valid_dates = {
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS)
    }
    response = session.get(
        DIRECT_MOVIE_LIST_URL,
        params={
            "coCd": CO_CD,
            "movNm": "",
            "div": "VIDEO_ADDEXP_CD",
            "attrCd": DIRECT_FILTER_CODE,
        },
        headers={**HEADERS, "Referer": "https://cgv.co.kr/cnm/movieBook/movie"},
        timeout=DIRECT_SCAN_TIMEOUT,
    )
    response.raise_for_status()
    movies = extract_direct_movies(response.json())

    signals = {}
    errors = 0
    for mov_no, movie in movies.items():
        try:
            dr = session.get(
                DIRECT_DATE_LIST_URL,
                params={
                    "coCd": CO_CD,
                    "siteNo": SITE_NO,
                    "movNo": mov_no,
                    "div": "VIDEO_ADDEXP_CD",
                    "attrCd": DIRECT_FILTER_CODE,
                },
                headers={**HEADERS, "Referer": "https://cgv.co.kr/cnm/movieBook/movie"},
                timeout=DIRECT_SCAN_TIMEOUT,
            )
            dr.raise_for_status()
            dates = extract_direct_dates(dr.json()) & valid_dates
        except Exception as error:
            errors += 1
            print(f"⚠️ GV 직접필터 날짜조회 오류 | MOV={mov_no} | {repr(error)}")
            continue

        for date in dates:
            key = direct_event_key(date, mov_no)
            signals[key] = make_direct_event(date, mov_no, movie)

    return signals, errors

def extract_rows(data):
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return [row for row in data["data"] if isinstance(row, dict)]

    rows = []
    def walk(item):
        if isinstance(item, dict):
            if (item.get("movNo") or item.get("movNm")) and (item.get("scnsrtTm") or item.get("scnSseq")):
                rows.append(item)
            for value in item.values():
                walk(value)
        elif isinstance(item, list):
            for value in item:
                walk(value)
    walk(data)
    return rows


def make_headers(date):
    params = urlencode({"siteNo": SITE_NO, "siteNm": SITE_NAME, "scnYmd": date})
    headers = dict(HEADERS)
    headers["Referer"] = f"https://cgv.co.kr/cnm/movieBook/cinema?{params}"
    return headers


def check_one_date(session, date):
    try:
        started = time.monotonic()
        response = session.get(
            API_URL,
            params={
                "coCd": CO_CD,
                "siteNo": SITE_NO,
                "scnYmd": date,
                "scnsNo": "",
                "scnSseq": "",
                "rtctlScopCd": RTCTL_SCOP_CD,
                "custNo": "",
            },
            headers=make_headers(date),
            timeout=20,
        )
        elapsed = time.monotonic() - started

        if response.status_code in BLOCK_STATUSES or response.status_code != 200:
            return None, f"HTTP {response.status_code} | DATE={date} | {elapsed:.2f}s"

        try:
            data = response.json()
        except Exception as error:
            return None, f"JSON ERROR | DATE={date} | {repr(error)}"

        events = {}
        for row in extract_rows(data):
            if not is_gv_row(row):
                continue
            key = event_key(date, row)
            events[key] = normalize_event(date, row)
        return events, None

    except Exception as error:
        return None, f"REQUEST ERROR | DATE={date} | {repr(error)}"


def movie_group_key(event):
    return clean(event.get("mov_no")) or clean(event.get("movie")).casefold()


def alert_time_range(event):
    start = pretty_time(event.get("time", ""))
    end = pretty_time(event.get("end_time", ""))
    if start and end:
        return f"{start}–{end}"
    return start or end or "시간 정보 없음"


def alert_line(event):
    movie = event.get("movie") or "영화명 확인 필요"
    screen = event.get("screen") or "상영관 정보 없음"
    link = event.get("link") or BOOKING_PAGE
    # 롯데/메가박스 최종 형식과 동일하게 영화 제목에만 예매 링크를 건다.
    return f"**🎟️ {alert_time_range(event)} · [{movie}]({link}) · {screen}**"


def alert_title(status):
    if status == "DETECTED":
        return "🔎 GV가 감지됐습니다"
    if status == "PREPARING":
        return "⏳ GV 상영준비중이 감지됐습니다"
    if status == "OPEN":
        return "🚨 GV 예매가 오픈됐습니다"
    return "🔎 GV 상태가 변경됐습니다"


def display_group(events, trigger, status):
    date = trigger.get("date", "")
    movie_key = movie_group_key(trigger)
    result = []
    for event in events.values():
        if event.get("date") != date or movie_group_key(event) != movie_key:
            continue
        current = event.get("status", "UNKNOWN")
        if status == "PREPARING" and current != "PREPARING":
            continue
        if status == "OPEN" and current != "OPEN":
            continue
        if status == "DETECTED" and current == "SOLD_OUT":
            continue
        result.append(event)
    return result or [trigger]


def send_alert_group(events, status):
    if not events:
        return 0

    events = sorted(events, key=lambda e: (clean(e.get("time")), clean(e.get("screen"))))
    first = events[0]
    header = [
        f"<@{DISCORD_MENTION_ID}>",
        f"**{alert_title(status)}**",
        f"**🎬 {SITE_NAME} · GV**",
        f"**📅 {pretty_date(first.get('date', ''))}**",
    ]

    messages = 0
    current = list(header)
    for event in events:
        line = alert_line(event)
        if len("\n".join(current + [line])) > 1900 and len(current) > len(header):
            if send_discord("\n".join(current)):
                messages += 1
            current = list(header)
        current.append(line)

    if len(current) > len(header):
        if send_discord("\n".join(current)):
            messages += 1
    return messages


def same_movie_date(record, event):
    if not isinstance(record, dict):
        return False
    if clean(record.get("date")) != clean(event.get("date")):
        return False
    record_mov = clean(record.get("mov_no"))
    event_mov = clean(event.get("mov_no"))
    if record_mov and event_mov:
        return record_mov == event_mov
    a = re.sub(r"\s+", "", clean(record.get("movie"))).casefold()
    b = re.sub(r"\s+", "", clean(event.get("movie"))).casefold()
    return bool(a and b and (a == b or a in b or b in a))


def has_equivalent_state(show_state, event, direct_only=False):
    for record in show_state.values():
        if direct_only and "direct filter" not in clean(record.get("status_source")).casefold():
            continue
        if same_movie_date(record, event):
            return True
    return False


def process_direct_signals(signals, seen, show_state):
    alerts = 0
    new_count = 0
    for key, event in sorted(signals.items()):
        if key in seen:
            continue

        # 이미 실제 회차로 알고 있는 GV라면 직접필터 키만 조용히 동기화한다.
        if has_equivalent_state(show_state, event):
            seen.add(key)
            show_state[key] = state_record(event, "DETECTED")
            continue

        message_count = send_alert_group([event], "DETECTED")
        if message_count <= 0:
            continue

        alerts += message_count
        new_count += 1
        seen.add(key)
        show_state[key] = state_record(event, "DETECTED")

    if new_count:
        save_seen(seen)
        save_booking_state(show_state)
    return alerts, new_count


def run_direct_filter_scan(session, seen, show_state):
    started = time.monotonic()
    try:
        signals, inner_errors = scan_direct_filter(session)
        alerts, new_count = process_direct_signals(signals, seen, show_state)
        elapsed = time.monotonic() - started
        print(
            f"🎯 GV 0023 직접필터 완료 | 신호 {len(signals)} | 신규 {new_count} | "
            f"Discord 알림 {alerts} | 오류 {inner_errors} | {elapsed:.2f}초"
        )
        return {"signals": len(signals), "alerts": alerts, "errors": inner_errors}
    except Exception as error:
        elapsed = time.monotonic() - started
        print(f"❌ GV 0023 직접필터 오류 | {repr(error)} | {elapsed:.2f}초")
        return {"signals": 0, "alerts": 0, "errors": 1}

def process_new_events(events, seen, show_state):
    # 새 회차가 처음부터 매진이면 사용자 알림 없이 내부 상태만 등록한다.
    new_items = []
    for key, event in events.items():
        if key in seen:
            continue
        current = event.get("status", "UNKNOWN")
        if current == "SOLD_OUT":
            seen.add(key)
            show_state[key] = state_record(event, "SOLD_OUT")
            continue

        # 0023/0025 직접필터에서 영화+날짜를 먼저 알린 경우,
        # 실제 회차의 DETECTED 알림은 중복시키지 않고 OPEN/PREPARING 전이만 이어서 알린다.
        if has_equivalent_state(show_state, event, direct_only=True):
            seen.add(key)
            show_state[key] = state_record(event, "DETECTED")
            continue

        new_items.append((key, event))

    groups = {}
    for key, event in new_items:
        group_key = (event.get("date", ""), movie_group_key(event))
        groups.setdefault(group_key, []).append((key, event))

    sent = 0
    for group_key in sorted(groups):
        members = groups[group_key]
        first = members[0][1]
        display = display_group(events, first, "DETECTED")
        message_count = send_alert_group(display, "DETECTED")
        if message_count <= 0:
            continue
        sent += message_count
        for key, event in members:
            seen.add(key)
            # 최초 감지와 동시에 OPEN/PREPARING이라도 같은 사이클 중복 알림은 막는다.
            show_state[key] = state_record(event, event.get("status", "UNKNOWN"))
    return sent


def process_state_transitions(events, seen, show_state):
    candidates = []
    for key, event in events.items():
        if key not in seen:
            continue
        current = event.get("status", "UNKNOWN")
        previous_record = show_state.get(key) or {}
        previous = previous_record.get("status")

        if current == "SOLD_OUT":
            show_state[key] = state_record(event, "SOLD_OUT")
            continue
        if current == "UNKNOWN":
            show_state[key] = state_record(event, previous or "UNKNOWN")
            continue

        alert_status = None
        if current == "PREPARING":
            if previous in {None, "UNKNOWN", "DETECTED"}:
                alert_status = "PREPARING"
            elif previous in {"OPEN", "SOLD_OUT"}:
                show_state[key] = state_record(event, previous)
                continue
        elif current == "OPEN":
            if previous in {None, "UNKNOWN", "DETECTED", "PREPARING"}:
                alert_status = "OPEN"
            elif previous == "SOLD_OUT":
                # 매진 -> OPEN은 취소표/재오픈이므로 사용자 알림 없음.
                show_state[key] = state_record(event, "OPEN")
                continue

        if alert_status:
            candidates.append((key, event, alert_status))
        else:
            show_state[key] = state_record(event, previous or current)

    groups = {}
    for key, event, status in candidates:
        group_key = (event.get("date", ""), movie_group_key(event), status)
        groups.setdefault(group_key, []).append((key, event, status))

    sent = 0
    for group_key in sorted(groups):
        members = groups[group_key]
        first = members[0][1]
        status = members[0][2]
        display = display_group(events, first, status)
        message_count = send_alert_group(display, status)
        if message_count <= 0:
            continue
        sent += message_count
        for key, event, _ in members:
            show_state[key] = state_record(event, status)
    return sent


def count_gv(events):
    return sum(1 for e in events.values() if e.get("type") == "GV")


def merged_cache(cache):
    result = {}
    for events in cache.values():
        if isinstance(events, dict):
            result.update(events)
    return result


def full_baseline(session):
    all_events = {}
    errors = 0
    last_request = 0.0
    dates = make_dates()
    for index, date in enumerate(dates, start=1):
        wait = MIN_REQUEST_GAP - (time.monotonic() - last_request)
        if wait > 0:
            time.sleep(wait)
        last_request = time.monotonic()
        events, error = check_one_date(session, date)
        if error or events is None:
            errors += 1
            print("❌ BASELINE API ERROR |", error)
            continue
        all_events.update(events)
        if index % 10 == 0 or index == len(dates):
            print(f"⏳ GV baseline {index}/{len(dates)} 날짜 완료")
    return all_events, errors


def initialize_state(session, seen, show_state, state_ready):
    need_seen = not baseline_done()
    need_state = not state_ready
    if not need_seen and not need_state:
        return seen, show_state, True

    print("=" * 72)
    print("INITIAL CGV YONGSAN GV-ONLY BASELINE")
    print("=" * 72)
    events, errors = full_baseline(session)
    if errors:
        print(f"❌ BASELINE FAILED | 오류 {errors}일 | 불완전 baseline은 저장하지 않습니다.")
        return seen, show_state, False

    if need_seen:
        seen = set(events.keys())
        save_seen(seen)
        mark_baseline_done()
    if need_state:
        show_state = {key: state_record(event) for key, event in events.items()}
        save_booking_state(show_state)

    print("BASELINE GV COUNT:", count_gv(events))
    print("BASELINE COMPLETE | 기존 회차 Discord 알림 없음")
    return seen, show_state, True


def interval_for_offset(offset):
    # 활성 감시 날짜(+4~+21)는 모두 동일하게 약 8초마다 재확인한다.
    return SCAN_CYCLE_SECONDS


def has_preparing(date, show_state):
    return any(
        isinstance(record, dict)
        and record.get("date") == date
        and record.get("status") == "PREPARING"
        for record in show_state.values()
    )


def effective_interval(date, show_state):
    today = now_kst().date()
    target = datetime.strptime(date, "%Y%m%d").date()
    offset = max(0, (target - today).days)
    base = interval_for_offset(offset)
    return min(base, PREPARING_INTERVAL) if has_preparing(date, show_state) else base


def build_schedule(show_state, start_at=None):
    if start_at is None:
        start_at = time.monotonic()
    groups = {}
    for date in make_dates():
        interval = effective_interval(date, show_state)
        groups.setdefault(interval, []).append(date)
    next_due = {}
    for interval, dates in groups.items():
        spacing = interval / max(1, len(dates))
        for index, date in enumerate(dates):
            next_due[date] = start_at + index * spacing
    return next_due


def fast_scan_dates():
    today = now_kst().date()
    return [
        (today + timedelta(days=offset)).strftime("%Y%m%d")
        for offset in range(FAST_SCAN_START_OFFSET, FAST_SCAN_END_OFFSET + 1)
    ]


def run_fast_scan(seen, show_state, cache):
    dates = fast_scan_dates()
    if not dates:
        return {"requests": 0, "success": 0, "errors": 0, "alerts": 0}

    # Playwright sync 객체는 스레드 간 공유하지 않는다. 2 worker마다 Chromium 세션 1개씩 유지한다.
    chunks = [dates[i::FAST_SCAN_WORKERS] for i in range(FAST_SCAN_WORKERS)]

    def worker(worker_index, worker_dates):
        results = []
        if not worker_dates:
            return results
        session = BrowserSession(label=f"GV FAST-{worker_index + 1}")
        try:
            for date in worker_dates:
                results.append((date, *check_one_date(session, date)))
                time.sleep(MIN_REQUEST_GAP)
        finally:
            session.close()
        return results

    started = time.monotonic()
    results = []
    with ThreadPoolExecutor(max_workers=FAST_SCAN_WORKERS) as executor:
        futures = [
            executor.submit(worker, index, chunk)
            for index, chunk in enumerate(chunks)
            if chunk
        ]
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as error:
                results.append(("", None, f"WORKER ERROR | {repr(error)}"))

    success = errors = alerts = 0
    for date, events, error in sorted(results, key=lambda item: item[0]):
        if error or events is None:
            errors += 1
            print("❌ CGV 00/30 ERROR |", error)
            continue
        success += 1
        cache[date] = events
        alerts += process_new_events(events, seen, show_state)
        alerts += process_state_transitions(events, seen, show_state)

    save_seen(seen)
    save_booking_state(show_state)
    elapsed = time.monotonic() - started
    icon = "⚡" if errors == 0 else "⚠️"
    print(
        f"{icon} {now_kst():%H:%M} 00/30 동시스캔 완료 | +4~+21일 | "
        f"성공 {success}/{len(dates)} | {elapsed:.2f}초 | Discord 알림 {alerts} | 오류 {errors}"
    )
    return {"requests": len(dates), "success": success, "errors": errors, "alerts": alerts}

def run_monitor(session, seen, show_state, started_at):
    cache = {}
    last_request = 0.0
    last_fast_slot = None
    report_started = time.monotonic()
    window_requests = window_success = window_errors = window_alerts = 0
    total_requests = 0

    # 시작하자마자 0023 직접필터부터 확인한다. 일반 회차 API에 아직 안 보이는 GV도 여기서 잡는다.
    direct = run_direct_filter_scan(session, seen, show_state)
    window_alerts += direct["alerts"]
    window_errors += direct["errors"]
    next_direct_scan = time.monotonic() + DIRECT_SCAN_INTERVAL

    next_due = build_schedule(show_state)

    print(
"📡 GV 집중 감시 | +4~+21일 18개 날짜 | 각 날짜 약 8초마다 재확인"
    )
    print("🎯 GV 0023 직접필터 | 영화목록+용산 날짜목록 | 8초 주기")
    print("🛡️ 00/30 추가 burst 스캔 비활성화 | 8초 상시감시로 대체")
    print("🎯 GV 판정: videoAddexpCd=0023 + 관객과의대화/GV 텍스트 fallback")

    while time.monotonic() - started_at < RUN_SECONDS and 6 <= now_kst().hour <= 23:
        mono = time.monotonic()
        remaining = RUN_SECONDS - (mono - started_at)
        if remaining <= 0:
            break

        # 일반 극장/날짜 API와 별도로 0023 필터를 계속 확인한다.
        if mono >= next_direct_scan:
            direct = run_direct_filter_scan(session, seen, show_state)
            window_alerts += direct["alerts"]
            window_errors += direct["errors"]
            next_direct_scan = time.monotonic() + DIRECT_SCAN_INTERVAL
            continue

        wall = now_kst()
        if FAST_SCAN_ENABLED and wall.minute in FAST_SCAN_MINUTES:
            slot = wall.strftime("%Y%m%d%H%M")
            if slot != last_fast_slot:
                last_fast_slot = slot
                success, errors, alerts = run_fast_scan(seen, show_state, cache)
                count = len(fast_scan_dates())
                total_requests += count
                window_requests += count
                window_success += success
                window_errors += errors
                window_alerts += alerts
                base = time.monotonic()
                for date in fast_scan_dates():
                    if date in next_due:
                        next_due[date] = base + effective_interval(date, show_state)
                continue

        if mono - report_started >= SUMMARY_SECONDS:
            count = count_gv(merged_cache(cache))
            icon = "💚" if window_errors == 0 else "⚠️"
            label = "정상 감시중" if window_errors == 0 else "감시중(API 오류 있음)"
            print(
                f"{icon} {label} | 최근 10분 날짜조회 {window_requests}회 / 성공 {window_success}회 | "
                f"누적 조회 {total_requests}회 | GV {count} | Discord 알림 {window_alerts} | 오류 {window_errors}"
            )
            report_started = mono
            window_requests = window_success = window_errors = window_alerts = 0
            continue

        due_date = min(next_due, key=next_due.get)
        due_at = next_due[due_date]
        if due_at > mono:
            wake_in = min(
                due_at - mono,
                max(0.0, next_direct_scan - mono),
                remaining,
                0.5,
            )
            time.sleep(max(0.05, wake_in))
            continue

        gap = MIN_REQUEST_GAP - (time.monotonic() - last_request)
        if gap > 0:
            time.sleep(min(gap, remaining))
        last_request = time.monotonic()

        events, error = check_one_date(session, due_date)
        total_requests += 1
        window_requests += 1
        if error or events is None:
            window_errors += 1
            print("❌ CGV API 오류 |", error)
            if error and "HTTP 429" in error:
                next_due = build_schedule(show_state, time.monotonic() + RATE_LIMIT_COOLDOWN)
            else:
                next_due[due_date] = time.monotonic() + effective_interval(due_date, show_state)
            continue

        window_success += 1
        cache[due_date] = events
        alerts = process_new_events(events, seen, show_state)
        alerts += process_state_transitions(events, seen, show_state)
        window_alerts += alerts
        save_seen(seen)
        save_booking_state(show_state)
        next_due[due_date] = time.monotonic() + effective_interval(due_date, show_state)

    save_seen(seen)
    save_booking_state(show_state)
    print(f"✅ CGV 용산 GV 감시 종료 | 누적 날짜조회 {total_requests}회")


def main():
    current = now_kst()
    if not (6 <= current.hour <= 23):
        print(f"⏹️ CGV 운영시간 밖이라 종료 | KST {current:%Y-%m-%d %H:%M:%S} | 운영 06:00~24:00")
        return

    started_at = time.monotonic()
    print("=" * 72)
    print("CGV YONGSAN GV-ONLY MONITOR")
    print("=" * 72)
    print("BRANCH:", SITE_NAME)
    print("SITE NO:", SITE_NO)
    print("TARGET: GV ONLY / 0023 직접필터 + videoAddexpCd=0023 + GV text fallback")
    print("DATE RANGE: +4 ~ +21 DAYS (18 DAYS TOTAL)")
    print("SOLD OUT / REOPEN: 사용자 알림 없음 / 내부 상태만 저장")
    print("ALERT: 날짜 + 영화 + GV 묶음 / 영화 제목에만 예매 링크")
    print("RUN SECONDS:", RUN_SECONDS)
    print("KST NOW:", now_kst().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 72)

    session = BrowserSession(label="GV MAIN")
    try:
        print("BOOKING PAGE STATUS:", session.booking_page_status)

        seen = load_seen()
        show_state, state_ready = load_booking_state()
        seen, show_state, ready = initialize_state(session, seen, show_state, state_ready)
        if not ready:
            return
        run_monitor(session, seen, show_state, started_at)
    finally:
        try:
            session.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
