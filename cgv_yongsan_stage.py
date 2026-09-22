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

import requests as std_requests
from curl_cffi import requests as cffi_requests

try:
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)
except Exception:
    pass

KST = ZoneInfo("Asia/Seoul")

CO_CD = "A420"
SITE_NO = "0013"
SITE_NAME = "CGV 용산아이파크몰"
RTCTL_SCOP_CD = "08"
CGV_STAGE_CODE = "0025"
DAYS = 43

# 무대인사는 수/토/일 + 대한민국 법정공휴일만 조회한다.
TARGET_WEEKDAYS = {2, 5, 6}  # 월=0 ... 일=6
TARGET_DAY_LABEL = "수/토/일/공휴일"

# 기존 최종 CGV 무대인사 구조를 유지: 대상 날짜 전체를 120초마다 재조회.
FULL_SCAN_INTERVAL = 120.0
MIN_REQUEST_GAP = 0.85
HTTP_RETRY_DELAY = 1.20
HTTP_RETRY_STATUSES = {403, 429}
RATE_LIMIT_COOLDOWN = 60.0
SUMMARY_SECONDS = 600.0

# 00/30 추가점검: +7~+21일 중 수/토/일/공휴일만.
FAST_SCAN_MINUTES = {0, 30}
FAST_SCAN_START_OFFSET = 7
FAST_SCAN_END_OFFSET = 21
FAST_SCAN_WORKERS = 2

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "86400"))

API_URL = "https://cgv.co.kr/api/v1/booking/searchMovScnInfo"

# CGV 영상부가체험 직접 필터. 일반 극장/날짜 API보다 무대인사 영화/날짜가 먼저 뜨는 경우를 잡는다.
DIRECT_MOVIE_LIST_URL = "https://cgv.co.kr/api/v1/booking/searchAtktTopPostrList"
DIRECT_DATE_LIST_URL = "https://cgv.co.kr/api/v1/booking/searchSiteScnscYmdListByMov"
DIRECT_FILTER_CODE = CGV_STAGE_CODE
DIRECT_SCAN_INTERVAL = 30.0
DIRECT_SCAN_TIMEOUT = 12
DIRECT_SIGNAL_DATES = set()

STATE_FILE = "seen_cgv_yongsan_stage_v4.json"
BASELINE_FILE = "baseline_cgv_yongsan_stage_v4.done"
BOOKING_STATE_FILE = "cgv_yongsan_stage_booking_state_v4.json"
BOOKING_STATE_SCHEMA = "CGV_YONGSAN_STAGE_0025_V4_20260912"

DISCORD_WEBHOOK = os.environ.get("CY_WEBHOOK", "").strip()
DISCORD_MENTION_ID = os.environ.get("DISCORD_MENTION_ID", "").strip()

# GitHub Secret: CGV_CUST_NO
# 값 자체는 소스/로그에 출력하지 않는다.
CGV_CUST_NO = os.environ.get("CGV_CUST_NO", "").strip()

# 2026~2027 대한민국 법정공휴일/대체공휴일.
# 현재 43일 창에서 수/토/일 외 공휴일도 무대인사 검사 대상에 포함한다.
KOREA_PUBLIC_HOLIDAYS = {
    # 2026
    "2026-01-01",
    "2026-02-16", "2026-02-17", "2026-02-18",
    "2026-03-01", "2026-03-02",
    "2026-05-05", "2026-05-24", "2026-05-25",
    "2026-06-03", "2026-06-06", "2026-07-17",
    "2026-08-15", "2026-08-17",
    "2026-09-24", "2026-09-25", "2026-09-26",
    "2026-10-03", "2026-10-05", "2026-10-09",
    "2026-12-25",
    # 2027
    "2027-01-01",
    "2027-02-06", "2027-02-07", "2027-02-08", "2027-02-09",
    "2027-03-01",
    "2027-05-05", "2027-05-13",
    "2027-06-06", "2027-07-17", "2027-07-19",
    "2027-08-15", "2027-08-16",
    "2027-09-14", "2027-09-15", "2027-09-16",
    "2027-10-03", "2027-10-04", "2027-10-09", "2027-10-11",
    "2027-12-25", "2027-12-27",
}

BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Origin": "https://cgv.co.kr",
}

BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}




def _fresh_session():
    return cffi_requests.Session(impersonate="chrome")


def get_with_fresh_retry(session, url, **kwargs):
    """일시적인 403/429는 같은 요청을 새 세션으로 딱 1회만 재시도한다."""
    response = session.get(url, **kwargs)
    if response.status_code not in HTTP_RETRY_STATUSES:
        return response, False

    time.sleep(HTTP_RETRY_DELAY)
    retry_session = _fresh_session()
    try:
        retry_response = retry_session.get(url, **kwargs)
        return retry_response, True
    finally:
        retry_session.close()


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


def is_stage_target_date(date_text):
    try:
        dt = datetime.strptime(date_text, "%Y%m%d").date()
    except Exception:
        return False
    hyphen = dt.strftime("%Y-%m-%d")
    return dt.weekday() in TARGET_WEEKDAYS or hyphen in KOREA_PUBLIC_HOLIDAYS


def make_dates(start_offset=7, end_offset=42):
    today = now_kst().date()
    result = []
    for offset in range(start_offset, end_offset + 1):
        date = (today + timedelta(days=offset)).strftime("%Y%m%d")
        # +7일 이후 수/토/일/공휴일만. 0025 직접필터 신호도 동일한 날짜 조건을 통과한 경우만 들어온다.
        if is_stage_target_date(date) or date in DIRECT_SIGNAL_DATES:
            result.append(date)
    return result


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


def make_headers(date):
    params = urlencode({"siteNo": SITE_NO, "siteNm": SITE_NAME, "scnYmd": date})
    headers = dict(BASE_HEADERS)
    headers["Referer"] = f"https://cgv.co.kr/cnm/movieBook/cinema?{params}"
    return headers


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
        response = std_requests.post(DISCORD_WEBHOOK, json=payload, timeout=15)
        response.raise_for_status()
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
        "STAGE",
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
        "type": "무대인사",
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
        "type": "무대인사",
        "movie": event.get("movie", ""),
        "mov_no": event.get("mov_no", ""),
        "prod_no": event.get("prod_no", ""),
        "screen": event.get("screen", ""),
        "time": event.get("time", ""),
        "end_time": event.get("end_time", ""),
        "status_source": event.get("status_source", ""),
        "updated_at_kst": now_kst().isoformat(),
    }


def is_stage_row(row):
    # 실제 코드 0025 최우선.
    if normalize_code(row.get("videoAddexpCd")) == CGV_STAGE_CODE:
        return True

    # 코드가 비거나 CGV 응답 형태가 달라진 경우 이름/설명 텍스트도 보조 판정한다.
    event_fields = [
        "videoAddexpCdNm", "videoAddexpNm", "videoAddexpCont",
        "eventNm", "eventName", "specialEventNm", "specialEventName",
        "addexpNm", "addexpName", "expoProdNm", "movNm", "movName",
    ]
    event_text = " | ".join(clean(row.get(k)) for k in event_fields if clean(row.get(k)))
    compact = re.sub(r"\s+", "", event_text)
    return "무대인사" in compact or "舞台挨拶" in event_text


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
    return "|".join([SITE_NO, clean(date), clean(mov_no), "STAGE_DIRECT_0025"])


def make_direct_event(date, mov_no, movie):
    return {
        "date": clean(date),
        "type": "무대인사",
        "movie": clean(movie) or f"영화번호 {clean(mov_no)}",
        "mov_no": clean(mov_no),
        "prod_no": "",
        "screen": "",
        "time": "",
        "end_time": "",
        "status": "UNKNOWN",
        "status_source": "VIDEO_ADDEXP_CD=0025 direct filter",
        "link": direct_filter_link(date, mov_no),
        "row": {
            "movNo": clean(mov_no),
            "movNm": clean(movie),
            "videoAddexpCd": DIRECT_FILTER_CODE,
            "videoAddexpCdNm": "무대인사",
            "_directSynthetic": True,
        },
    }


def scan_direct_filter(session):
    """0025 전용 영화목록 -> 용산 날짜목록. 상세 회차가 숨겨져 있어도 영화/날짜 신호를 먼저 잡는다."""
    today = now_kst().date()
    # 당일~+6일은 제외. +7~+42일 중 수/토/일/공휴일만 직접필터 대상으로 인정한다.
    valid_dates = {
        (today + timedelta(days=i)).strftime("%Y%m%d")
        for i in range(7, DAYS)
        if is_stage_target_date((today + timedelta(days=i)).strftime("%Y%m%d"))
    }
    response, _ = get_with_fresh_retry(
        session,
        DIRECT_MOVIE_LIST_URL,
        params={
            "coCd": CO_CD,
            "movNm": "",
            "div": "VIDEO_ADDEXP_CD",
            "attrCd": DIRECT_FILTER_CODE,
        },
        headers={**BASE_HEADERS, "Referer": "https://cgv.co.kr/cnm/movieBook/movie"},
        timeout=DIRECT_SCAN_TIMEOUT,
    )
    response.raise_for_status()
    movies = extract_direct_movies(response.json())

    signals = {}
    errors = 0
    found_dates = set()
    for mov_no, movie in movies.items():
        try:
            dr, _ = get_with_fresh_retry(
                session,
                DIRECT_DATE_LIST_URL,
                params={
                    "coCd": CO_CD,
                    "siteNo": SITE_NO,
                    "movNo": mov_no,
                    "div": "VIDEO_ADDEXP_CD",
                    "attrCd": DIRECT_FILTER_CODE,
                },
                headers={**BASE_HEADERS, "Referer": "https://cgv.co.kr/cnm/movieBook/movie"},
                timeout=DIRECT_SCAN_TIMEOUT,
            )
            dr.raise_for_status()
            dates = extract_direct_dates(dr.json()) & valid_dates
        except Exception as error:
            errors += 1
            print(f"⚠️ 무대인사 직접필터 날짜조회 오류 | MOV={mov_no} | {repr(error)}")
            continue

        found_dates.update(dates)
        for date in dates:
            key = direct_event_key(date, mov_no)
            signals[key] = make_direct_event(date, mov_no, movie)

    DIRECT_SIGNAL_DATES.clear()
    DIRECT_SIGNAL_DATES.update(found_dates)
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


def check_one_date(session, date):
    try:
        started = time.monotonic()
        response, retried = get_with_fresh_retry(
            session,
            API_URL,
            params={
                "coCd": CO_CD,
                "siteNo": SITE_NO,
                "scnYmd": date,
                "scnsNo": "",
                "scnSseq": "",
                "rtctlScopCd": RTCTL_SCOP_CD,
                "custNo": CGV_CUST_NO,
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
            if not is_stage_row(row):
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
    link = event.get("link") or "https://cgv.co.kr/cnm/movieBook"
    return f"**🎟️ {alert_time_range(event)} · [{movie}]({link}) · {screen}**"


def alert_title(status):
    if status == "DETECTED":
        return "🔎 무대인사가 감지됐습니다"
    if status == "PREPARING":
        return "⏳ 무대인사 상영준비중이 감지됐습니다"
    if status == "OPEN":
        return "🚨 무대인사 예매가 오픈됐습니다"
    return "🔎 무대인사 상태가 변경됐습니다"


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
        f"**🎬 {SITE_NAME} · 무대인사**",
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

        # 이미 실제 회차로 알고 있는 무대인사라면 직접필터 키만 조용히 동기화한다.
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
    # 30초마다 실제 조회는 계속하되, 반복 로그는 10분 요약에서만 보여준다.
    try:
        signals, inner_errors = scan_direct_filter(session)
        alerts, new_count = process_direct_signals(signals, seen, show_state)
        return {"signals": len(signals), "alerts": alerts, "errors": inner_errors, "new": new_count}
    except Exception:
        return {"signals": 0, "alerts": 0, "errors": 1, "new": 0}

def process_new_events(events, seen, show_state):
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
                # 매진 -> OPEN은 취소표/재오픈이므로 알리지 않는다.
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


def count_stage(events):
    return sum(1 for e in events.values() if e.get("type") == "무대인사")


def merged_cache(cache):
    result = {}
    for events in cache.values():
        if isinstance(events, dict):
            result.update(events)
    return result


def baseline_scan(session):
    dates = make_dates()
    events_all = {}
    errors = 0
    last_request = 0.0

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
        events_all.update(events)
        if index % 10 == 0 or index == len(dates):
            print(f"⏳ 무대인사 baseline {index}/{len(dates)} 대상날짜 완료")

    return events_all, errors


def initialize_state(session, seen, show_state, state_ready):
    need_seen = not baseline_done()
    need_state = not state_ready
    if not need_seen and not need_state:
        return seen, show_state, True

    print("=" * 72)
    print("INITIAL CGV YONGSAN STAGE 0025 BASELINE")
    print("=" * 72)
    events, errors = baseline_scan(session)
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

    print("BASELINE STAGE COUNT:", count_stage(events))
    print("BASELINE COMPLETE | 기존 회차 Discord 알림 없음")
    return seen, show_state, True


def run_cycle(session, seen, show_state, cache, dates, label):
    started = time.monotonic()
    requests_count = success = errors = alerts = 0
    last_request = 0.0
    rate_limited = False

    for date in dates:
        wait = MIN_REQUEST_GAP - (time.monotonic() - last_request)
        if wait > 0:
            time.sleep(wait)
        last_request = time.monotonic()
        events, error = check_one_date(session, date)
        requests_count += 1
        if error or events is None:
            errors += 1
            if error and "HTTP 429" in error:
                rate_limited = True
                break
            continue

        success += 1
        cache[date] = events
        alerts += process_new_events(events, seen, show_state)
        alerts += process_state_transitions(events, seen, show_state)

    save_seen(seen)
    save_booking_state(show_state)
    return {
        "started": started,
        "requests": requests_count,
        "success": success,
        "errors": errors,
        "alerts": alerts,
        "rate_limited": rate_limited,
    }


def run_fast_scan(seen, show_state, cache):
    dates = make_dates(FAST_SCAN_START_OFFSET, FAST_SCAN_END_OFFSET)
    if not dates:
        return {"requests": 0, "success": 0, "errors": 0, "alerts": 0}

    lock = threading.Lock()
    next_start = [time.monotonic()]

    def worker(date):
        with lock:
            wait = next_start[0] - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            next_start[0] = time.monotonic() + MIN_REQUEST_GAP
        session = cffi_requests.Session(impersonate="chrome")
        try:
            return date, *check_one_date(session, date)
        finally:
            session.close()

    started = time.monotonic()
    results = []
    with ThreadPoolExecutor(max_workers=FAST_SCAN_WORKERS) as executor:
        futures = [executor.submit(worker, date) for date in dates]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as error:
                results.append(("", None, f"WORKER ERROR | {repr(error)}"))

    success = errors = alerts = 0
    for date, events, error in sorted(results, key=lambda item: item[0]):
        if error or events is None:
            errors += 1
            print("❌ CGV STAGE 00/30 ERROR |", error)
            continue
        success += 1
        cache[date] = events
        alerts += process_new_events(events, seen, show_state)
        alerts += process_state_transitions(events, seen, show_state)

    save_seen(seen)
    save_booking_state(show_state)
    return {"requests": len(dates), "success": success, "errors": errors, "alerts": alerts}


def run_monitor(session, seen, show_state, started_at):
    cache = {}
    report_started = time.monotonic()
    window_requests = window_success = window_errors = window_alerts = 0
    total_requests = total_cycles = 0
    last_fast_slot = None

    # 시작하자마자 0025 직접필터부터 확인한다.
    # 일반 회차 API에 아직 상세 row가 없어도 영화/날짜 신호를 먼저 잡고,
    # 단, +7~+42일의 수/토/일/공휴일 조건을 만족한 날짜만 이후 상세감시에 편입한다.
    direct = run_direct_filter_scan(session, seen, show_state)
    latest_direct_signals = direct["signals"]
    window_alerts += direct["alerts"]
    window_errors += direct["errors"]
    next_direct_scan = time.monotonic() + DIRECT_SCAN_INTERVAL

    target_dates = make_dates()
    result = run_cycle(session, seen, show_state, cache, target_dates, "일반 전체스캔")
    total_cycles += 1
    total_requests += result["requests"]
    window_requests += result["requests"]
    window_success += result["success"]
    window_errors += result["errors"]
    window_alerts += result["alerts"]
    next_regular = (
        time.monotonic() + RATE_LIMIT_COOLDOWN
        if result["rate_limited"]
        else max(time.monotonic(), result["started"] + FULL_SCAN_INTERVAL)
    )

    print(
        f"📡 무대인사 일반감시 | +7~+42일 {TARGET_DAY_LABEL} | "
        "전체 120초 주기"
    )
    print("🎯 무대인사 0025 직접필터 | 영화목록+용산 날짜목록 | 30초 주기")
    print(f"⚡ 00/30 추가점검 | +7~+21일 중 {TARGET_DAY_LABEL} | 2 workers")
    print("🎯 무대인사 판정: videoAddexpCd=0025 + 무대인사 텍스트 fallback")

    while time.monotonic() - started_at < RUN_SECONDS and 6 <= now_kst().hour <= 23:
        mono = time.monotonic()
        remaining = RUN_SECONDS - (mono - started_at)
        if remaining <= 0:
            break

        if mono >= next_direct_scan:
            direct = run_direct_filter_scan(session, seen, show_state)
            latest_direct_signals = direct["signals"]
            window_alerts += direct["alerts"]
            window_errors += direct["errors"]
            next_direct_scan = time.monotonic() + DIRECT_SCAN_INTERVAL
            continue

        wall = now_kst()
        if wall.minute in FAST_SCAN_MINUTES:
            slot = wall.strftime("%Y%m%d%H%M")
            if slot != last_fast_slot:
                last_fast_slot = slot
                result = run_fast_scan(seen, show_state, cache)
                total_requests += result["requests"]
                window_requests += result["requests"]
                window_success += result["success"]
                window_errors += result["errors"]
                window_alerts += result["alerts"]
                next_regular = time.monotonic() + FULL_SCAN_INTERVAL
                continue

        if mono - report_started >= SUMMARY_SECONDS:
            icon = "💚" if window_errors == 0 else "⚠️"
            label = "정상 감시중" if window_errors == 0 else "감시중(API 오류 있음)"
            print(
                f"{icon} {label} | 최근 10분 날짜조회 {window_requests}회 / 성공 {window_success}회 | "
                f"누적 전체스캔 {total_cycles}회 / 누적 날짜조회 {total_requests}회 | "
                f"무대인사 {count_stage(merged_cache(cache))} | 0025 신호 {latest_direct_signals} | "
                f"직접필터 날짜 {len(DIRECT_SIGNAL_DATES)} | Discord 알림 {window_alerts} | 오류 {window_errors}"
            )
            report_started = mono
            window_requests = window_success = window_errors = window_alerts = 0
            continue

        if mono >= next_regular:
            target_dates = make_dates()
            result = run_cycle(session, seen, show_state, cache, target_dates, "일반 전체스캔")
            total_cycles += 1
            total_requests += result["requests"]
            window_requests += result["requests"]
            window_success += result["success"]
            window_errors += result["errors"]
            window_alerts += result["alerts"]
            next_regular = (
                time.monotonic() + RATE_LIMIT_COOLDOWN
                if result["rate_limited"]
                else max(time.monotonic(), result["started"] + FULL_SCAN_INTERVAL)
            )
            continue

        sleep_for = min(
            max(0.05, next_regular - mono),
            max(0.05, next_direct_scan - mono),
            remaining,
            0.5,
        )
        time.sleep(sleep_for)

    save_seen(seen)
    save_booking_state(show_state)
    print(f"✅ CGV 용산 무대인사 감시 종료 | 누적 전체스캔 {total_cycles}회 | 누적 날짜조회 {total_requests}회")


def main():
    current = now_kst()
    if not (6 <= current.hour <= 23):
        print(f"⏹️ CGV 운영시간 밖이라 종료 | KST {current:%Y-%m-%d %H:%M:%S} | 운영 06:00~24:00")
        return

    started_at = time.monotonic()
    print("=" * 72)
    print("CGV YONGSAN STAGE-ONLY MONITOR")
    print("=" * 72)
    print("BRANCH:", SITE_NAME)
    print("SITE NO:", SITE_NO)
    print("TARGET: 무대인사 ONLY / 0025 직접필터 + videoAddexpCd=0025 + 무대인사 text fallback")
    print(f"TARGET DAYS: +7~+42일 / {TARGET_DAY_LABEL}")
    print("DATE RANGE: +7 ~ +42 DAYS")
    print("SCAN: +7~+42 대상 날짜 120초 + 0025 직접필터 30초 + 00/30 +7~+21일 2 workers")
    print("SOLD OUT / REOPEN: 사용자 알림 없음 / 내부 상태만 저장")
    print("ALERT: 날짜 + 영화 + 무대인사 묶음 / 영화 제목에만 예매 링크")
    print("RUN SECONDS:", RUN_SECONDS)
    print("KST NOW:", now_kst().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 72)

    if not CGV_CUST_NO:
        print("❌ CGV 회원 일련번호(custNo)를 찾지 못했습니다.")
        print("GitHub Secret CGV_CUST_NO가 필요합니다.")
        return

    print("CGV CUST NO: LOADED (VALUE NOT PRINTED)")
    print("CGV HTTP CLIENT: curl_cffi / impersonate=chrome / fresh-session retry on 403·429")
    session = cffi_requests.Session(impersonate="chrome")
    try:
        seen = load_seen()
        show_state, state_ready = load_booking_state()
        seen, show_state, ready = initialize_state(session, seen, show_state, state_ready)
        if not ready:
            return
        run_monitor(session, seen, show_state, started_at)
    finally:
        session.close()


if __name__ == "__main__":
    main()
