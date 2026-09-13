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

# 기존 최종 CGV 무대인사 구조를 유지: 대상 날짜 전체를 1분마다 재조회.
FULL_SCAN_INTERVAL = 120.0
MIN_REQUEST_GAP = 0.35
RATE_LIMIT_COOLDOWN = 60.0
SUMMARY_SECONDS = 600.0

# 롯데/메가박스와 같은 00/30 추가점검: +4~+21일 중 수/토/일/공휴일만.
FAST_SCAN_MINUTES = {0, 30}
FAST_SCAN_START_OFFSET = 4
FAST_SCAN_END_OFFSET = 21
FAST_SCAN_WORKERS = 2

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "3900"))

API_URL = "https://cgv.co.kr/api/v1/booking/searchMovScnInfo"

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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/152.0.0.0 Safari/537.36"
    ),
}

BLOCK_STATUSES = {403, 429, 500, 502, 503, 504}


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


def make_dates(start_offset=0, end_offset=42):
    today = now_kst().date()
    result = []
    for offset in range(start_offset, end_offset + 1):
        date = (today + timedelta(days=offset)).strftime("%Y%m%d")
        if is_stage_target_date(date):
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
        response = requests.post(DISCORD_WEBHOOK, json=payload, timeout=15)
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
        response = session.get(
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
            # 텍스트 추측이 아니라 CGV 실제 일련코드 0025만 무대인사로 인정한다.
            if normalize_code(row.get("videoAddexpCd")) != CGV_STAGE_CODE:
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
            print("❌ CGV STAGE API 오류 |", error)
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
    elapsed = time.monotonic() - started
    print(
        f"{'⚠️' if errors else '🔎'} {now_kst():%H:%M:%S} {label} 완료 | "
        f"{TARGET_DAY_LABEL} {len(dates)}일 | 요청 {requests_count} | 성공 {success} | "
        f"오류 {errors} | 무대인사 {count_stage(merged_cache(cache))} | "
        f"Discord 알림 {alerts} | {elapsed:.2f}초"
    )
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
        session = requests.Session()
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
    elapsed = time.monotonic() - started
    icon = "⚡" if errors == 0 else "⚠️"
    print(
        f"{icon} {now_kst():%H:%M} 00/30 추가점검 완료 | +4~+21일 중 {TARGET_DAY_LABEL} | "
        f"성공 {success}/{len(dates)} | {elapsed:.2f}초 | Discord 알림 {alerts} | 오류 {errors}"
    )
    return {"requests": len(dates), "success": success, "errors": errors, "alerts": alerts}


def run_monitor(session, seen, show_state, started_at):
    cache = {}
    target_dates = make_dates()
    report_started = time.monotonic()
    window_requests = window_success = window_errors = window_alerts = 0
    total_requests = total_cycles = 0
    last_fast_slot = None

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

    print(f"📡 무대인사 일반감시 | TODAY~+42 중 {TARGET_DAY_LABEL}만 | 전체 1분 주기")
    print(f"⚡ 00/30 추가점검 | +4~+21일 중 {TARGET_DAY_LABEL}만 | 2 workers")
    print("🎯 무대인사 판정: videoAddexpCd=0025 ONLY")

    while time.monotonic() - started_at < RUN_SECONDS:
        mono = time.monotonic()
        remaining = RUN_SECONDS - (mono - started_at)
        if remaining <= 0:
            break

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
                f"무대인사 {count_stage(merged_cache(cache))} | Discord 알림 {window_alerts} | 오류 {window_errors}"
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

        time.sleep(min(max(0.05, next_regular - mono), remaining, 0.5))

    save_seen(seen)
    save_booking_state(show_state)
    print(f"✅ CGV 용산 무대인사 감시 종료 | 누적 전체스캔 {total_cycles}회 | 누적 날짜조회 {total_requests}회")


def main():
    started_at = time.monotonic()
    print("=" * 72)
    print("CGV YONGSAN STAGE-ONLY MONITOR")
    print("=" * 72)
    print("BRANCH:", SITE_NAME)
    print("SITE NO:", SITE_NO)
    print("TARGET: 무대인사 ONLY / videoAddexpCd=0025")
    print(f"TARGET DAYS: {TARGET_DAY_LABEL} ONLY")
    print("DATE RANGE: TODAY ~ +42 DAYS")
    print("SCAN: 대상 날짜 전체 60초 + 00/30 +4~+21일 2 workers")
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
    session = requests.Session()
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
