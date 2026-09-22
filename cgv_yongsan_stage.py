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
MIN_REQUEST_GAP = 1.00
HTTP_RETRY_DELAY = 1.20
HTTP_RETRY_STATUSES = {403, 429}
RATE_LIMIT_COOLDOWN = 25.0
CYCLE_RETRY_ROUNDS = 3
RETRY_ROUND_DELAY = 4.0
SUMMARY_SECONDS = 600.0

# 기존 workflow 소스검증 호환용 상수. 현재 감시 실행에는 병렬/00·30 burst를 사용하지 않는다.
FAST_SCAN_START_OFFSET = 7
FAST_SCAN_END_OFFSET = 21

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "86400"))

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
        # 사용자 설정 그대로: +7~+42일 중 수/토/일/대한민국 공휴일만 전부 확인한다.
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
    """초기 기준선도 대상 날짜 전부가 200으로 확인된 경우에만 완료한다."""
    dates = make_dates()
    events_all = {}
    pending = list(dates)
    failed_details = {}
    requests_count = 0
    last_request = 0.0

    for round_no in range(1, CYCLE_RETRY_ROUNDS + 1):
        if not pending:
            break
        if round_no > 1:
            delay = RATE_LIMIT_COOLDOWN if any("HTTP 429" in v for v in failed_details.values()) else RETRY_ROUND_DELAY
            time.sleep(delay)

        current = list(pending)
        pending = []
        failed_details = {}

        for idx, date in enumerate(current):
            wait = MIN_REQUEST_GAP - (time.monotonic() - last_request)
            if wait > 0:
                time.sleep(wait)
            last_request = time.monotonic()

            events, error = check_one_date(session, date)
            requests_count += 1
            if error or events is None:
                pending.append(date)
                failed_details[date] = error or "UNKNOWN ERROR"
                if error and "HTTP 429" in error:
                    # 429 뒤에는 나머지 날짜를 즉시 더 두드리지 않는다.
                    for remain in current[idx + 1:]:
                        if remain not in pending:
                            pending.append(remain)
                            failed_details.setdefault(remain, "NOT TRIED AFTER HTTP 429")
                    break
                continue

            events_all.update(events)
            failed_details.pop(date, None)

    return events_all, len(pending), pending, failed_details, requests_count


def initialize_state(session, seen, show_state, state_ready):
    need_seen = not baseline_done()
    need_state = not state_ready
    if not need_seen and not need_state:
        return seen, show_state, True

    print("=" * 72)
    print("INITIAL CGV YONGSAN STAGE 0025 BASELINE")
    print("=" * 72)
    events, errors, failed_dates, failed_details, baseline_requests = baseline_scan(session)
    if errors:
        print(
            f"❌ BASELINE FAILED | 커버리지 {len(make_dates()) - errors}/{len(make_dates())} | "
            f"미확인 {','.join(failed_dates)} | 불완전 baseline은 저장하지 않습니다."
        )
        for date in failed_dates:
            print(f"❌ BASELINE 실패 상세 | {date}:{failed_details.get(date, 'UNKNOWN ERROR')}")
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


def run_cycle(session, seen, show_state, cache, dates, label, notify=True):
    """대상 날짜를 전부 확인한다. 실패 날짜는 버리지 않고 같은 사이클에서 재시도한다."""
    started = time.monotonic()
    requests_count = 0
    attempt_errors = 0
    alerts = 0
    succeeded = set()
    pending = list(dates)
    failed_details = {}
    last_request = 0.0
    saw_429 = False

    for round_no in range(1, CYCLE_RETRY_ROUNDS + 1):
        if not pending:
            break

        if round_no > 1:
            delay = RATE_LIMIT_COOLDOWN if saw_429 else RETRY_ROUND_DELAY
            time.sleep(delay)
            saw_429 = False

        current = list(pending)
        pending = []
        failed_details = {}

        for idx, date in enumerate(current):
            wait = MIN_REQUEST_GAP - (time.monotonic() - last_request)
            if wait > 0:
                time.sleep(wait)
            last_request = time.monotonic()

            events, error = check_one_date(session, date)
            requests_count += 1

            if error or events is None:
                attempt_errors += 1
                pending.append(date)
                failed_details[date] = error or "UNKNOWN ERROR"

                if error and "HTTP 429" in error:
                    saw_429 = True
                    # 429 뒤에는 남은 날짜를 즉시 더 호출하지 않고 다음 라운드로 넘긴다.
                    for remain in current[idx + 1:]:
                        if remain not in pending:
                            pending.append(remain)
                            failed_details.setdefault(remain, "NOT TRIED AFTER HTTP 429")
                    break
                continue

            succeeded.add(date)
            failed_details.pop(date, None)
            cache[date] = events

            if notify:
                alerts += process_new_events(events, seen, show_state)
                alerts += process_state_transitions(events, seen, show_state)
            else:
                # 시작 시점에 이미 존재하는 무대인사는 기준선으로만 등록한다.
                # 과거 무대인사 재알림을 막고, 다음 사이클부터 새로 생긴 회차만 알린다.
                for key, event in events.items():
                    seen.add(key)
                    show_state[key] = state_record(event, event.get("status", "UNKNOWN"))

    save_seen(seen)
    save_booking_state(show_state)

    # 한 번이라도 성공한 날짜는 pending에 남지 않도록 정리한다.
    pending = [d for d in pending if d not in succeeded]
    complete = len(succeeded) == len(dates)

    return {
        "started": started,
        "requests": requests_count,
        "success": len(succeeded),
        "errors": attempt_errors,
        "alerts": alerts,
        "complete": complete,
        "failed_dates": pending,
        "failed_details": {d: failed_details.get(d, "UNKNOWN ERROR") for d in pending},
        "rate_limited": saw_429 or any("HTTP 429" in v for v in failed_details.values()),
    }


def run_monitor(session, seen, show_state, started_at):
    cache = {}
    report_started = time.monotonic()
    window_requests = window_errors = window_alerts = 0
    total_requests = total_cycles = 0
    target_dates = make_dates()
    target_count = len(target_dates)

    print(f"📡 무대인사 전체감시 | +7~+42일 {TARGET_DAY_LABEL} | 대상 {target_count}일 전부")
    print("🔐 조회 경로 | searchMovScnInfo + CGV_CUST_NO")
    print("🚫 0025 영화별 날짜조회 | 403 반복 경로라 실행하지 않음")
    print(f"🛡️ 요청 분산 | 날짜 요청 시작간격 >= {MIN_REQUEST_GAP:.2f}초 | 병렬 burst 없음")
    print(f"🔁 실패 날짜 | 버리지 않음 / 최대 {CYCLE_RETRY_ROUNDS}라운드 재시도 / 429는 {RATE_LIMIT_COOLDOWN:.0f}초 휴식")
    print("🧾 로그 | 시작 즉시 전체점검 결과 출력 → 이후 10분 요약")
    print("🔕 시작 기준선 | 현재 존재하는 무대인사는 재알림하지 않고, 다음 점검부터 새 회차만 즉시 알림")

    print(f"🔎 초기 전체점검 시작 | +7~+42일 {TARGET_DAY_LABEL} {target_count}개 날짜")
    result = run_cycle(session, seen, show_state, cache, target_dates, "초기 전체점검", notify=False)
    total_cycles += 1
    total_requests += result["requests"]
    window_requests += result["requests"]
    window_errors += result["errors"]
    window_alerts += result["alerts"]

    stage_count = count_stage(merged_cache(cache))
    if result["complete"]:
        print(
            f"✅ 초기 전체점검 통과 | 커버리지 {result['success']}/{target_count} | "
            f"무대인사 {stage_count} | {time.monotonic() - result['started']:.2f}초"
        )
    else:
        failed = ",".join(result["failed_dates"]) or "UNKNOWN"
        print(
            f"❌ 초기 전체점검 미통과 | 커버리지 {result['success']}/{target_count} | "
            f"미확인 {failed} | 무대인사 {stage_count}"
        )
        for date in result["failed_dates"]:
            print(f"❌ 초기 실패 상세 | {date}:{result['failed_details'].get(date, 'UNKNOWN ERROR')}")

    last_coverage = result["success"]
    last_failed_dates = list(result["failed_dates"])
    next_regular = time.monotonic() + (FULL_SCAN_INTERVAL if result["complete"] else RATE_LIMIT_COOLDOWN)

    while time.monotonic() - started_at < RUN_SECONDS and 6 <= now_kst().hour <= 23:
        mono = time.monotonic()
        remaining = RUN_SECONDS - (mono - started_at)
        if remaining <= 0:
            break

        if mono - report_started >= SUMMARY_SECONDS:
            icon = "💚" if last_coverage == target_count else "⚠️"
            label = "전체 날짜 확인됨" if last_coverage == target_count else "전체 날짜 미확인"
            failed_text = "없음" if not last_failed_dates else ",".join(last_failed_dates)
            print(
                f"{icon} {label} | 최근10분 커버리지 {last_coverage}/{target_count} | "
                f"미확인 {failed_text} | 날짜요청 {window_requests} | "
                f"무대인사 {count_stage(merged_cache(cache))} | Discord 알림 {window_alerts} | "
                f"요청오류 {window_errors}"
            )
            report_started = mono
            window_requests = window_errors = window_alerts = 0
            continue

        if mono >= next_regular:
            target_dates = make_dates()
            target_count = len(target_dates)
            result = run_cycle(session, seen, show_state, cache, target_dates, "정규 전체점검", notify=True)
            total_cycles += 1
            total_requests += result["requests"]
            window_requests += result["requests"]
            window_errors += result["errors"]
            window_alerts += result["alerts"]
            last_coverage = result["success"]
            last_failed_dates = list(result["failed_dates"])
            next_regular = time.monotonic() + (FULL_SCAN_INTERVAL if result["complete"] else RATE_LIMIT_COOLDOWN)
            continue

        time.sleep(min(max(0.05, next_regular - mono), remaining, 0.5))

    save_seen(seen)
    save_booking_state(show_state)
    print(
        f"✅ CGV 용산 무대인사 감시 종료 | 누적 전체점검 {total_cycles}회 | "
        f"누적 날짜요청 {total_requests}회"
    )


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
    print("TARGET: 무대인사 ONLY / videoAddexpCd=0025 + 무대인사 text fallback")
    print(f"TARGET DAYS: +7~+42일 / {TARGET_DAY_LABEL}")
    print("DATE RANGE: +7 ~ +42 DAYS")
    print("SCAN: +7~+42 대상 날짜 전체 120초 주기 / 실패 날짜 재시도")
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
    print("STAGE COVERAGE POLICY: 대상 날짜 전부 실제 200 응답 확인 시에만 전체 확인으로 표시")
    print("STAGE API MODE: searchMovScnInfo + CGV_CUST_NO only")
    print("MONITOR BUILD: STAGE_FULL_COVERAGE_20260922_V1")
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
