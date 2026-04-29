from flask import Flask, request, jsonify
import threading
import asyncio
import aiohttp
import time
import json
import socket
from collections import deque

app = Flask(__name__)

SERVER_URL = "http://192.168.31.63:8000/event"
STATE_URL = "http://192.168.31.63:8000/debug/state"
ACCESS_TOKEN = "bobr9475"

HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8889

YOTIK_HOST = "0.0.0.0"
YOTIK_PORTS = [8883, 8884, 8885, 8886, 8887, 8888]

PULT_CALLBACK_URL = None
# пример:
# PULT_CALLBACK_URL = "http://192.168.31.100:8890/pult/update"

CITY_FETCH_INTERVAL = 2.0
DEFAULT_SEGMENT_TIME = 30.0
DELAY_FACTOR_THRESHOLD = 1.15
DELAY_MIN_SECONDS = 0.0
MAX_SAMPLES_PER_SEGMENT = 30

TYPE2_DURATION_MS = 1000
TYPE2_FREQUENCY_HZ = 1100

# НОВОЕ:
# Если True, то при отправке type 2 на городской сервер
# мы дополнительно пытаемся сразу активировать локальную точку через Йотик.
ENABLE_DIRECT_LOCAL_TYPE2_BRIDGE = True

MAX_PROCESSED_FACE_EVENTS = 2000

# НОВОЕ: настройки логики входящих данных с Йотика 8888
YOTIK_8888_TRIGGER_THRESHOLD = 100
YOTIK_8888_RESET_THRESHOLD = 100
YOTIK_8888_RELEASE_DELAY_SECONDS = 3.0
YOTIK_8888_CITY_TEXT = "Осторожно, проезжает автобус"

# НОВОЕ: настройки обработки devices_type2 -> Йотик 8883
TYPE2_COLOR_DEVICE_ID = 35
TYPE2_COLOR_YOTIK_PORT = 8884
TYPE2_COLOR_DEFAULT_DURATION_MS = 10000

city_state = {}
state_lock = threading.Lock()

pult_command = {}
pult_command_lock = threading.Lock()

last_route_result = {}
route_result_lock = threading.Lock()

yotik_conns = {}
yotik_lock = threading.Lock()

DEVICE_TYPE1_TO_YOTIK_PORT = {
    5: 8885,
    7: 8886,
    11: 8887,
    15: 8888
}

processed_type1_updates = {}
processed_type1_lock = threading.Lock()

# НОВОЕ: антиспам для devices_type2
processed_type2_updates = {}
processed_type2_lock = threading.Lock()

active_routes = {}
active_routes_lock = threading.Lock()

processed_face_event_keys = set()
processed_face_event_order = deque()
processed_face_lock = threading.Lock()

BUS_ROUTES = {
    "1": [
        "Кирова",
        "Усова",
        "ТПУ",
        "Дворец спорта",
        "Транспортное кольцо",
        "Южная"
    ],
    "2": [
        "Герцена",
        "Красноармейская",
        "Никитина",
        "Сибирская",
        "Яковлева",
        "Башня"
    ]
}

BUS_STOP_NUMBERS = [1, 2, 3, 4, 5, 6]

# Словари остановок для mixed-маршрута.
# Ключ — id точки/камеры на маршруте, значение — номер остановки автобуса.
# Заполните своими соответствиями.
stops1 = {}
stops2 = {}

bus_tracker = {}
bus_tracker_lock = threading.Lock()

segment_stats = {}
segment_stats_lock = threading.Lock()

last_bus_schedule = {}
last_bus_schedule_lock = threading.Lock()

bus_current_lap = {}
bus_current_lap_lock = threading.Lock()

active_delay_warnings = {}
active_delay_warnings_lock = threading.Lock()

last_delay_warning = {}
last_delay_warning_lock = threading.Lock()

# хранение последнего события камеры
last_camera_event = {}
last_camera_event_lock = threading.Lock()

# НОВОЕ: хранение последней строки одежды с Йотика 8883
last_clothes_data = {}
last_clothes_data_lock = threading.Lock()

# НОВОЕ: антиспам/антидребезг для Йотика 8888
yotik_8888_state = {
    "active": False,
    "rearmed": True,
    "last_value": None,
    "last_trigger_at": 0
}
yotik_8888_state_lock = threading.Lock()

grapf2 = {
    "1": ["2", "3", "4", "10"],
    "2": ["1", "3", "4"],
    "3": ["2", "1", "4"],
    "4": ["2", "3", "1", "12"],
    "5": ["6", "7", "11"],
    "6": ["5", "8", "9"],
    "7": ["8", "5", "11"],
    "8": ["9", "6", "7"],
    "9": ["10", "6", "8", "14"],
    "10": ["13", "1"],
    "11": ["7", "5", "12", "15"],
    "12": ["11", "4", "16"],
    "13": ["10"],
    "14": ["9"],
    "15": ["11"],
    "16": ["12"],
    "19": ["32"],
    "20": ["31"],
    "21": ["22", "23", "24"],
    "22": ["23", "21", "24"],
    "23": ["31", "21", "24"],
    "24": ["23", "22", "30"],
    "25": ["32", "26", "27"],
    "26": ["29", "28", "25"],
    "27": ["32", "25", "28"],
    "28": ["27", "26", "29"],
    "29": ["28", "26", "42", "30"],
    "30": ["29", "24", "41"],
    "31": ["32", "23", "20"],
    "32": ["19", "25", "27", "31"],
    "41": ["30"],
    "42": ["29"]
}

grapf1 = {
    "1": ["2", "3", "4", "10"],
    "2": ["1", "3", "4"],
    "3": ["2", "1", "4"],
    "4": ["2", "3", "1", "12"],
    "5": ["6", "7", "11"],
    "6": ["5", "8", "9"],
    "7": ["8", "5", "11"],
    "8": ["9", "6", "7"],
    "9": ["10", "6", "8", "14"],
    "10": ["13", "1"],
    "11": ["7", "5", "12", "15"],
    "12": ["11", "4", "16"],
    "13": ["10", "14", "15", "16", "17"],
    "14": ["9", "15", "16", "17", "13"],
    "15": ["11", "16", "17", "13", "14"],
    "16": ["12", "17", "13", "14", "15"],
    "17": ["13", "18", "14", "15", "16"],
    "18": ["17", "20", "19", "42", "41"],
    "19": ["32", "42", "20", "18", "41"],
    "20": ["31", "19", "18", "41", "42"],
    "21": ["22", "23", "24"],
    "22": ["23", "21", "24"],
    "23": ["31", "21", "24"],
    "24": ["23", "22", "30"],
    "25": ["32", "26", "27"],
    "26": ["29", "28", "25"],
    "27": ["32", "25", "28"],
    "28": ["27", "26", "29"],
    "29": ["28", "26", "42", "30"],
    "30": ["29", "24", "41"],
    "31": ["32", "23", "20"],
    "32": ["19", "25", "27", "31"],
    "41": ["30", "18", "42", "20", "19"],
    "42": ["29", "41", "20", "19", "18"]
}


def shortest_path(graph, start, end):
    if start not in graph or end not in graph:
        return []

    queue = deque([start])
    visited = {start}
    parent = {start: None}

    while queue:
        current = queue.popleft()

        if current == end:
            break

        for neighbor in graph[current]:
            if neighbor not in visited:
                visited.add(neighbor)
                parent[neighbor] = current
                queue.append(neighbor)

    if end not in parent:
        return []

    path = []
    current = end
    while current is not None:
        path.append(current)
        current = parent[current]

    return path[::-1]


def build_route(route_type, start, end):
    if route_type == "walking":
        graph = grapf2
    elif route_type == "mixed":
        graph = grapf1
    else:
        return {
            "status": "Ошибка",
            "message": f"Неизвестный route_type: {route_type}",
            "path": []
        }

    path = shortest_path(graph, start, end)

    if not path:
        return {
            "status": "Ошибка",
            "message": f"Маршрут не найден: {start} -> {end}",
            "route_type": route_type,
            "start": start,
            "end": end,
            "path": []
        }

    return {
        "status": "OK",
        "route_type": route_type,
        "start": start,
        "end": end,
        "path": path,
        "path_length": len(path),
        "calculated_at": int(time.time())
    }


def build_route_with_intermediate_points(route_type, start, end, intermediate_points=None):
    intermediate_points = intermediate_points or []
    all_points = [str(start), *[str(p) for p in intermediate_points], str(end)]

    full_path = []

    for i in range(len(all_points) - 1):
        part_start = all_points[i]
        part_end = all_points[i + 1]

        part_result = build_route(route_type, part_start, part_end)
        if part_result["status"] != "OK":
            return {
                "status": "Ошибка",
                "message": f"Не удалось построить сегмент {part_start} -> {part_end}",
                "failed_segment": {
                    "start": part_start,
                    "end": part_end,
                    "step": i + 1
                },
                "path": full_path
            }

        part_path = part_result.get("path", [])
        if not part_path:
            return {
                "status": "Ошибка",
                "message": f"Пустой путь для сегмента {part_start} -> {part_end}",
                "failed_segment": {
                    "start": part_start,
                    "end": part_end,
                    "step": i + 1
                },
                "path": full_path
            }

        if full_path and full_path[-1] == part_path[0]:
            full_path.extend(part_path[1:])
        else:
            full_path.extend(part_path)

    return {
        "status": "OK",
        "route_type": route_type,
        "start": str(start),
        "end": str(end),
        "intermediate_points": [str(p) for p in intermediate_points],
        "path": full_path,
        "path_length": len(full_path),
        "calculated_at": int(time.time())
    }


def get_mixed_stop_info(point_id):
    point_id = str(point_id)

    if point_id in stops1:
        try:
            return {
                "bus_id": "1",
                "stop_num": int(stops1[point_id]),
                "device_id": point_id
            }
        except (TypeError, ValueError):
            return None

    if point_id in stops2:
        try:
            return {
                "bus_id": "2",
                "stop_num": int(stops2[point_id]),
                "device_id": point_id
            }
        except (TypeError, ValueError):
            return None

    return None


def build_mixed_stop_markers(path):
    markers = []

    for idx, point_id in enumerate(path):
        info = get_mixed_stop_info(point_id)
        if info is None:
            continue

        markers.append({
            "path_index": idx,
            "device_id": str(point_id),
            "bus_id": info["bus_id"],
            "stop_num": info["stop_num"],
            "stop_name": get_stop_name(info["bus_id"], info["stop_num"])
        })

    return markers


def is_boarding_marker(marker_index):
    return marker_index % 2 == 0


def build_navigation_chat_payload(user_id, message, extra=None):
    payload = {
        "type": "navigation_chat",
        "created_at": int(time.time()),
        "user_id": str(user_id),
        "message": str(message)
    }

    if isinstance(extra, dict):
        payload.update(extra)

    return payload


def send_navigation_chat_message(user_id, message, extra=None):
    payload = build_navigation_chat_payload(user_id, message, extra)

    print(f"[NAV CHAT] {json.dumps(payload, ensure_ascii=False)}")

    if PULT_CALLBACK_URL:
        asyncio.run(send_to_pult(payload))


async def send_event(payload):
    headers = {
        "Content-Type": "application/json",
        "X-Access-Token": ACCESS_TOKEN
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(SERVER_URL, json=payload, headers=headers) as resp:
                text = await resp.text()
                print(f"[CITY EVENT SENT] {json.dumps(payload, ensure_ascii=False)}")
                print(f"[CITY RESPONSE] {resp.status} | {text}")
                return resp.status, text
    except Exception as e:
        print(f"[ERROR] Send event: {e}")
        return None, str(e)


async def fetch_city_state():
    global city_state

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(STATE_URL) as resp:
                data = await resp.json()

                with state_lock:
                    city_state = data

                return data
    except Exception as e:
        print(f"[ERROR] Fetch city state: {e}")
        return None


async def send_to_pult(payload):
    if not PULT_CALLBACK_URL:
        return

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(PULT_CALLBACK_URL, json=payload) as resp:
                text = await resp.text()
                print(f"[PULT SENT] {resp.status} | {text}")
    except Exception as e:
        print(f"[ERROR] Send to pult: {e}")


def build_type2_payload(device_id):
    return {
        "type": 2,
        "device_id": int(device_id),
        "duration_ms": TYPE2_DURATION_MS,
        "frequency_hz": TYPE2_FREQUENCY_HZ
    }


def build_city_type1_warning_payload():
    return {
        "type": 1,
        "text": YOTIK_8888_CITY_TEXT,
        "timestamp": int(time.time())
    }


def try_send_type2_to_local_device(device_id, duration_ms=TYPE2_DURATION_MS, frequency_hz=TYPE2_FREQUENCY_HZ):
    """
    Если точка относится к нашим локальным устройствам type1,
    сразу отправляем команду в Йотик, не дожидаясь отражения в city_state.
    """
    try:
        device_id_int = int(device_id)
    except (TypeError, ValueError):
        return False, "invalid_device_id"

    port = DEVICE_TYPE1_TO_YOTIK_PORT.get(device_id_int)
    if port is None:
        return False, "device_is_not_local_type1"

    command = f"on {int(frequency_hz)} {int(duration_ms)}"
    send_to_yotik(port, command)

    print(
        f"[DIRECT TYPE2->LOCAL] device_id={device_id_int} "
        f"port={port} command={command}"
    )

    return True, {
        "device_id": device_id_int,
        "port": port,
        "command": command
    }


def send_type2_to_city(device_id):
    payload = build_type2_payload(device_id)

    status_code, response_text = asyncio.run(send_event(payload))
    city_ok = status_code is not None and 200 <= status_code < 300

    local_ok = False
    local_result = None

    if ENABLE_DIRECT_LOCAL_TYPE2_BRIDGE:
        local_ok, local_result = try_send_type2_to_local_device(
            device_id=device_id,
            duration_ms=0,
            frequency_hz=TYPE2_FREQUENCY_HZ
        )

    # Успех считается, если либо город принял event,
    # либо локальная точка успешно была включена напрямую.
    overall_ok = city_ok or local_ok

    return overall_ok, {
        "city_payload": payload,
        "city_response_status": status_code,
        "city_response_text": response_text,
        "city_ok": city_ok,
        "local_ok": local_ok,
        "local_result": local_result
    }


def get_next_stop_num(stop_num):
    return 1 if stop_num == 6 else stop_num + 1


def get_segment_key(from_stop, to_stop):
    return f"{from_stop}->{to_stop}"


def get_stop_name(bus_id, stop_num):
    route = BUS_ROUTES.get(str(bus_id), [])
    if 1 <= stop_num <= len(route):
        return route[stop_num - 1]
    return f"Остановка {stop_num}"


def get_segment_average(bus_id, from_stop, to_stop):
    bus_id = str(bus_id)
    key = get_segment_key(from_stop, to_stop)

    with segment_stats_lock:
        bus_segments = segment_stats.get(bus_id, {})
        item = bus_segments.get(key)

        if not item or item["samples"] <= 0:
            return None

        return item["total_time"] / item["samples"]


def get_bus_fallback_average(bus_id):
    bus_id = str(bus_id)

    with segment_stats_lock:
        bus_segments = segment_stats.get(bus_id, {})

        total_sum = 0.0
        total_samples = 0
        for item in bus_segments.values():
            total_sum += item["total_time"]
            total_samples += item["samples"]

    if total_samples == 0:
        return DEFAULT_SEGMENT_TIME

    return total_sum / total_samples


def add_segment_measurement(bus_id, from_stop, to_stop, travel_time):
    if travel_time <= 0:
        return

    bus_id = str(bus_id)
    key = get_segment_key(from_stop, to_stop)

    with segment_stats_lock:
        if bus_id not in segment_stats:
            segment_stats[bus_id] = {}

        if key not in segment_stats[bus_id]:
            segment_stats[bus_id][key] = {
                "total_time": 0.0,
                "samples": 0
            }

        item = segment_stats[bus_id][key]
        item["total_time"] += float(travel_time)
        item["samples"] += 1

        if item["samples"] > MAX_SAMPLES_PER_SEGMENT:
            avg = item["total_time"] / item["samples"]
            item["samples"] = MAX_SAMPLES_PER_SEGMENT
            item["total_time"] = avg * MAX_SAMPLES_PER_SEGMENT


def build_bus_forecast(bus_id, current_stop):
    bus_id = str(bus_id)
    current_stop = int(current_stop)

    now_ts = time.time()
    fallback_avg = get_bus_fallback_average(bus_id)
    forecast = []

    cumulative = 0.0
    from_stop = current_stop

    for _ in range(6):
        to_stop = get_next_stop_num(from_stop)

        avg = get_segment_average(bus_id, from_stop, to_stop)
        segment_time = avg if avg is not None else fallback_avg

        cumulative += segment_time
        eta_timestamp = int(now_ts + cumulative)

        forecast.append({
            "stop_num": to_stop,
            "stop_name": get_stop_name(bus_id, to_stop),
            "eta_seconds_from_now": int(round(cumulative)),
            "eta_timestamp": eta_timestamp,
            "segment_average": round(segment_time, 2)
        })

        from_stop = to_stop

    return {
        "bus_id": bus_id,
        "current_stop_num": current_stop,
        "current_stop_name": get_stop_name(bus_id, current_stop),
        "generated_at": int(now_ts),
        "forecast": forecast
    }


def schedule_changed_significantly(old_schedule, new_schedule):
    if not old_schedule:
        return True

    old_forecast = old_schedule.get("forecast", [])
    new_forecast = new_schedule.get("forecast", [])

    if len(old_forecast) != len(new_forecast):
        return True

    for old_item, new_item in zip(old_forecast, new_forecast):
        if old_item["stop_num"] != new_item["stop_num"]:
            return True

        if abs(old_item["eta_timestamp"] - new_item["eta_timestamp"]) >= 5:
            return True

    return False


def format_bus_lap_summary(bus_id):
    bus_id = str(bus_id)
    parts = []

    with bus_current_lap_lock:
        current_lap = bus_current_lap.get(bus_id, {}).copy()

    for from_stop in range(1, 7):
        to_stop = get_next_stop_num(from_stop)
        segment_key = get_segment_key(from_stop, to_stop)

        avg = get_segment_average(bus_id, from_stop, to_stop)
        cur = current_lap.get(segment_key)

        parts.append({
            "segment": segment_key,
            "from_stop_name": get_stop_name(bus_id, from_stop),
            "to_stop_name": get_stop_name(bus_id, to_stop),
            "current_lap_time": round(cur, 2) if cur is not None else None,
            "average_time": round(avg, 2) if avg is not None else None
        })

    return parts


def print_bus_lap_summary(bus_id):
    bus_id = str(bus_id)
    summary = format_bus_lap_summary(bus_id)

    print(f"\n[BUS {bus_id} LAP COMPLETED]")
    print(f"Маршрут автобуса {bus_id}:")
    for item in summary:
        print(
            f"  {item['segment']} "
            f"({item['from_stop_name']} -> {item['to_stop_name']}) | "
            f"текущий круг: {item['current_lap_time']} сек | "
            f"среднее: {item['average_time']} сек"
        )


def build_delay_warning_payload(bus_id, from_stop, to_stop, elapsed_time, average_time):
    bus_id = str(bus_id)

    return {
        "type": "bus_delay_warning",
        "created_at": int(time.time()),
        "bus_id": bus_id,
        "stations": {
            "from_stop_num": from_stop,
            "from_stop_name": get_stop_name(bus_id, from_stop),
            "to_stop_num": to_stop,
            "to_stop_name": get_stop_name(bus_id, to_stop)
        },
        "elapsed_time_seconds": round(elapsed_time, 2),
        "average_time_seconds": round(average_time, 2),
        "delay_percent": round(((elapsed_time - average_time) / average_time) * 100, 2) if average_time > 0 else None,
        "notification": (
            f"Задержка автобуса между станциями "
            f"{get_stop_name(bus_id, from_stop)} и {get_stop_name(bus_id, to_stop)}. "
            f"Возможна неисправность автобуса."
        )
    }


def maybe_send_active_delay_warning(bus_id, current_stop, seen_at):
    bus_id = str(bus_id)
    from_stop = int(current_stop)
    to_stop = get_next_stop_num(from_stop)

    avg = get_segment_average(bus_id, from_stop, to_stop)
    if avg is None:
        avg = get_bus_fallback_average(bus_id)

    if avg <= 0:
        return

    elapsed = time.time() - seen_at
    threshold = avg * DELAY_FACTOR_THRESHOLD

    if elapsed < threshold:
        return

    segment_key = get_segment_key(from_stop, to_stop)

    with active_delay_warnings_lock:
        active_segment = active_delay_warnings.get(bus_id)
        if active_segment == segment_key:
            return
        active_delay_warnings[bus_id] = segment_key

    payload = build_delay_warning_payload(
        bus_id=bus_id,
        from_stop=from_stop,
        to_stop=to_stop,
        elapsed_time=elapsed,
        average_time=avg
    )

    with last_delay_warning_lock:
        last_delay_warning[bus_id] = payload

    print(f"\n[BUS DELAY WARNING] {json.dumps(payload, ensure_ascii=False, indent=2)}")

    if PULT_CALLBACK_URL:
        asyncio.run(send_to_pult(payload))


def clear_active_delay_warning_if_needed(bus_id, prev_stop, current_stop):
    bus_id = str(bus_id)
    completed_segment = get_segment_key(prev_stop, current_stop)

    with active_delay_warnings_lock:
        active_segment = active_delay_warnings.get(bus_id)
        if active_segment == completed_segment:
            del active_delay_warnings[bus_id]


def process_type1_devices(city_data):
    if not city_data:
        return

    devices = city_data.get("devices_type1", {})
    if not isinstance(devices, dict):
        return

    for device_id_raw, device_info in devices.items():
        try:
            device_id = int(device_id_raw)
        except (TypeError, ValueError):
            continue

        if device_id not in DEVICE_TYPE1_TO_YOTIK_PORT:
            continue

        if not isinstance(device_info, dict):
            continue

        frequency_hz = device_info.get("frequency_hz")
        duration_ms = device_info.get("duration_ms")
        last_update = device_info.get("last_update")

        try:
            frequency_hz = int(frequency_hz)
            duration_ms = int(duration_ms)
            last_update = int(last_update)
        except (TypeError, ValueError):
            print(f"[TYPE1 SKIP] Некорректные данные для device_id={device_id}: {device_info}")
            continue

        with processed_type1_lock:
            prev_last_update = processed_type1_updates.get(device_id)
            if prev_last_update == last_update:
                continue
            processed_type1_updates[device_id] = last_update

        port = DEVICE_TYPE1_TO_YOTIK_PORT[device_id]
        command = f"on {frequency_hz} {duration_ms}"

        print(
            f"[TYPE1 MATCH] device_id={device_id} -> port={port} | "
            f"freq={frequency_hz} | duration={duration_ms} | last_update={last_update}"
        )

        send_to_yotik(port, command)


def process_type2_devices(city_data):
    """
    Ищем в city_state -> devices_type2 устройство 35.
    Если нашли и last_update новый, отправляем на Йотик 8883:
    on R G B 10000
    """
    if not city_data:
        return

    devices = city_data.get("devices_type2", {})
    if not isinstance(devices, dict):
        return

    target_id = str(TYPE2_COLOR_DEVICE_ID)
    device_info = devices.get(target_id)

    if not isinstance(device_info, dict):
        return

    color = device_info.get("color", {})
    last_update = device_info.get("last_update")

    if not isinstance(color, dict):
        print(f"[TYPE2 COLOR SKIP] Некорректный color для device_id={target_id}: {device_info}")
        return

    try:
        r = int(color.get("r"))
        g = int(color.get("g"))
        b = int(color.get("b"))
        last_update = int(last_update)
    except (TypeError, ValueError):
        print(f"[TYPE2 COLOR SKIP] Некорректные данные для device_id={target_id}: {device_info}")
        return

    with processed_type2_lock:
        prev_last_update = processed_type2_updates.get(target_id)
        if prev_last_update == last_update:
            return
        processed_type2_updates[target_id] = last_update

    duration = TYPE2_COLOR_DEFAULT_DURATION_MS
    command = f"on {r} {g} {b} {duration}"

    print(
        f"[TYPE2 COLOR MATCH] device_id={target_id} -> port={TYPE2_COLOR_YOTIK_PORT} | "
        f"r={r} g={g} b={b} duration={duration} | last_update={last_update}"
    )

    send_to_yotik(TYPE2_COLOR_YOTIK_PORT, command)


def make_face_event_key(face_event):
    return (
        int(face_event.get("timestamp", 0)),
        int(face_event.get("device_id", -1)),
        str(face_event.get("user_id", "")).strip()
    )


def is_face_event_processed(event_key):
    with processed_face_lock:
        return event_key in processed_face_event_keys


def mark_face_event_processed(event_key):
    with processed_face_lock:
        if event_key in processed_face_event_keys:
            return

        processed_face_event_keys.add(event_key)
        processed_face_event_order.append(event_key)

        while len(processed_face_event_order) > MAX_PROCESSED_FACE_EVENTS:
            old_key = processed_face_event_order.popleft()
            processed_face_event_keys.discard(old_key)


def start_navigation_session(user_id, route_type, start, end, path):
    user_id = str(user_id).strip()
    if not user_id:
        return False, {
            "status": "Ошибка",
            "message": "Пустой user_id"
        }

    if not path:
        return False, {
            "status": "Ошибка",
            "message": "Маршрут пустой"
        }

    first_point = str(path[0])

    ok, send_result = send_type2_to_city(first_point)
    if not ok:
        return False, {
            "status": "Ошибка",
            "message": "Не удалось активировать стартовую точку",
            "send_result": send_result
        }

    normalized_path = [str(x) for x in path]
    mixed_stop_markers = build_mixed_stop_markers(normalized_path) if route_type == "mixed" else []

    route_info = {
        "user_id": user_id,
        "route_type": route_type,
        "start": str(start),
        "end": str(end),
        "path": normalized_path,
        "current_index": 0,
        "current_target": str(first_point),
        "started_at": int(time.time()),
        "last_step_at": int(time.time()),
        "status": "active",
        "last_type2_result": send_result,
        "mixed_state": {
            "enabled": route_type == "mixed",
            "stop_markers": mixed_stop_markers,
            "marker_index": 0,
            "phase": "walking",
            "waiting_for_bus": None,
            "riding_bus": None,
            "last_bus_action_at": None
        }
    }

    with active_routes_lock:
        active_routes[user_id] = route_info

    print("\n[NAVIGATION STARTED]")
    print(json.dumps(route_info, ensure_ascii=False, indent=2))

    return True, {
        "status": "OK",
        "message": "Навигация запущена",
        "navigation": route_info,
        "send_result": send_result
    }



def try_handle_mixed_stop_arrival(route_info, user_id, device_id):
    mixed_state = route_info.get("mixed_state") or {}
    if not mixed_state.get("enabled"):
        return False

    stop_markers = mixed_state.get("stop_markers", [])
    marker_index = mixed_state.get("marker_index", 0)

    if marker_index >= len(stop_markers):
        return False

    marker = stop_markers[marker_index]
    if str(marker.get("device_id")) != str(device_id):
        return False

    if not is_boarding_marker(marker_index):
        return False

    bus_id = str(marker["bus_id"])
    stop_num = int(marker["stop_num"])

    mixed_state["phase"] = "waiting_bus"
    mixed_state["waiting_for_bus"] = {
        "bus_id": bus_id,
        "stop_num": stop_num,
        "stop_name": marker.get("stop_name")
    }
    mixed_state["riding_bus"] = None
    mixed_state["last_bus_action_at"] = int(time.time())

    send_navigation_chat_message(
        user_id=user_id,
        message=(
            f"Вы на остановке {marker.get('stop_name')}. "
            f"Ожидайте автобус {bus_id}."
        ),
        extra={
            "bus_id": bus_id,
            "stop_num": stop_num,
            "device_id": str(device_id),
            "action": "wait_bus"
        }
    )

    return True


def process_mixed_bus_navigation():
    with active_routes_lock:
        routes_snapshot = {user_id: dict(route_info) for user_id, route_info in active_routes.items()}

    for user_id, route_info in routes_snapshot.items():
        mixed_state = route_info.get("mixed_state") or {}
        if not mixed_state.get("enabled"):
            continue

        stop_markers = mixed_state.get("stop_markers", [])
        marker_index = mixed_state.get("marker_index", 0)
        phase = mixed_state.get("phase")

        if marker_index >= len(stop_markers):
            continue

        marker = stop_markers[marker_index]
        bus_id = str(marker.get("bus_id"))
        stop_num = int(marker.get("stop_num"))

        with bus_tracker_lock:
            bus_info = bus_tracker.get(bus_id)

        if not bus_info:
            continue

        current_stop = int(bus_info.get("current_stop"))

        if phase == "waiting_bus":
            if current_stop != stop_num:
                continue

            next_marker_index = marker_index + 1
            if next_marker_index >= len(stop_markers):
                continue

            next_marker = stop_markers[next_marker_index]
            next_device = str(next_marker["device_id"])

            ok, send_result = send_type2_to_city(next_device)
            if not ok:
                print(
                    f"[MIXED BUS ERROR] Не удалось переключить пользователя {user_id} "
                    f"на остановку выхода {next_device}"
                )
                continue

            with active_routes_lock:
                active_route = active_routes.get(user_id)
                if active_route is None:
                    continue

                active_route["current_index"] = int(next_marker["path_index"])
                active_route["current_target"] = next_device
                active_route["last_step_at"] = int(time.time())
                active_route["last_type2_result"] = send_result

                active_mixed_state = active_route.get("mixed_state") or {}
                active_mixed_state["marker_index"] = next_marker_index
                active_mixed_state["phase"] = "riding_bus"
                active_mixed_state["waiting_for_bus"] = None
                active_mixed_state["riding_bus"] = {
                    "bus_id": bus_id,
                    "from_stop_num": stop_num,
                    "from_stop_name": marker.get("stop_name"),
                    "to_stop_num": int(next_marker["stop_num"]),
                    "to_stop_name": next_marker.get("stop_name")
                }
                active_mixed_state["last_bus_action_at"] = int(time.time())
                active_route["mixed_state"] = active_mixed_state

            send_navigation_chat_message(
                user_id=user_id,
                message=(
                    f"Автобус {bus_id} прибыл на остановку {marker.get('stop_name')}. "
                    f"Садится."
                ),
                extra={
                    "bus_id": bus_id,
                    "stop_num": stop_num,
                    "device_id": marker.get("device_id"),
                    "action": "board_bus"
                }
            )

        elif phase == "riding_bus":
            if current_stop != stop_num:
                continue

            with active_routes_lock:
                active_route = active_routes.get(user_id)
                if active_route is None:
                    continue

                active_mixed_state = active_route.get("mixed_state") or {}
                active_mixed_state["phase"] = "walking"
                active_mixed_state["waiting_for_bus"] = None
                active_mixed_state["riding_bus"] = None
                active_mixed_state["marker_index"] = marker_index + 1
                active_mixed_state["last_bus_action_at"] = int(time.time())
                active_route["mixed_state"] = active_mixed_state

            send_navigation_chat_message(
                user_id=user_id,
                message=(
                    f"Автобус {bus_id} прибыл на остановку {marker.get('stop_name')}. "
                    f"Высадится."
                ),
                extra={
                    "bus_id": bus_id,
                    "stop_num": stop_num,
                    "device_id": marker.get("device_id"),
                    "action": "leave_bus"
                }
            )


def process_navigation_events(city_data):
    if not city_data:
        return

    events = city_data.get("events", {})
    if not isinstance(events, dict):
        return

    face_events = events.get("face", [])
    if not isinstance(face_events, list):
        return

    for face_event in face_events:
        if not isinstance(face_event, dict):
            continue

        try:
            event_key = make_face_event_key(face_event)
        except Exception:
            continue

        if is_face_event_processed(event_key):
            continue

        user_id = str(face_event.get("user_id", "")).strip()
        if not user_id:
            mark_face_event_processed(event_key)
            continue

        try:
            device_id = str(int(face_event.get("device_id")))
        except (TypeError, ValueError):
            mark_face_event_processed(event_key)
            continue

        with active_routes_lock:
            route_info = active_routes.get(user_id)
            if route_info is None:
                mark_face_event_processed(event_key)
                continue

            current_index = route_info.get("current_index", 0)
            path = route_info.get("path", [])
            if not path or current_index >= len(path):
                del active_routes[user_id]
                mark_face_event_processed(event_key)
                continue

            current_target = str(path[current_index])

        if device_id != current_target:
            mark_face_event_processed(event_key)
            continue

        print(
            f"\n[NAVIGATION STEP MATCH] user_id={user_id} "
            f"device_id={device_id} current_target={current_target}"
        )

        with active_routes_lock:
            route_info = active_routes.get(user_id)
            if route_info is None:
                mark_face_event_processed(event_key)
                continue

            current_index = route_info["current_index"]
            path = route_info["path"]

        route_info_snapshot = None
        with active_routes_lock:
            route_info_snapshot = active_routes.get(user_id)

        if route_info_snapshot is not None and try_handle_mixed_stop_arrival(route_info_snapshot, user_id, device_id):
            mark_face_event_processed(event_key)
            continue

        is_last_point = current_index >= len(path) - 1

        if is_last_point:
            with active_routes_lock:
                route_info = active_routes.get(user_id)
                if route_info is not None:
                    route_info["status"] = "completed"
                    route_info["completed_at"] = int(time.time())
                    print("\n[УРОООООО]")
                    print(f"Пользователь {user_id} дошёл до конечной точки {device_id}")
                    del active_routes[user_id]

            mark_face_event_processed(event_key)
            continue

        next_index = current_index + 1
        next_point = str(path[next_index])

        ok, send_result = send_type2_to_city(next_point)
        if not ok:
            print(
                f"[ERROR] Не удалось переключить пользователя {user_id} "
                f"на следующую точку {next_point} | "
                f"send_result={json.dumps(send_result, ensure_ascii=False)}"
            )
            continue

        with active_routes_lock:
            route_info = active_routes.get(user_id)
            if route_info is None:
                mark_face_event_processed(event_key)
                continue

            route_info["current_index"] = next_index
            route_info["current_target"] = next_point
            route_info["last_step_at"] = int(time.time())
            route_info["last_type2_result"] = send_result

        print(
            f"[NAVIGATION NEXT POINT] user_id={user_id} "
            f"next_point={next_point} "
            f"send_result={json.dumps(send_result, ensure_ascii=False)}"
        )

        mark_face_event_processed(event_key)


def update_bus_state_from_city(city_data):
    if not city_data:
        return

    buses = city_data.get("buses", {})
    updates_to_push = []
    now_seen = time.time()

    for bus_id_raw, bus_info in buses.items():
        bus_id = str(bus_id_raw)

        if bus_id not in BUS_ROUTES:
            continue

        try:
            current_stop = int(bus_info.get("current_stop"))
        except (TypeError, ValueError):
            continue

        city_timestamp = bus_info.get("timestamp")

        if current_stop not in BUS_STOP_NUMBERS:
            continue

        lap_completed = False
        warning_check_stop = None
        warning_check_seen_at = None

        with bus_tracker_lock:
            previous = bus_tracker.get(bus_id)

            if previous is None:
                bus_tracker[bus_id] = {
                    "current_stop": current_stop,
                    "seen_at": now_seen,
                    "city_timestamp": city_timestamp
                }
                warning_check_stop = current_stop
                warning_check_seen_at = now_seen

            else:
                prev_stop = previous["current_stop"]
                prev_seen_at = previous["seen_at"]

                if current_stop != prev_stop:
                    expected_next = get_next_stop_num(prev_stop)
                    actual_travel_time = now_seen - prev_seen_at

                    if current_stop == expected_next and actual_travel_time > 0:
                        add_segment_measurement(bus_id, prev_stop, current_stop, actual_travel_time)

                        segment_key = get_segment_key(prev_stop, current_stop)

                        with bus_current_lap_lock:
                            if bus_id not in bus_current_lap:
                                bus_current_lap[bus_id] = {}
                            bus_current_lap[bus_id][segment_key] = actual_travel_time

                        clear_active_delay_warning_if_needed(bus_id, prev_stop, current_stop)

                        if prev_stop == 6 and current_stop == 1:
                            lap_completed = True

                    bus_tracker[bus_id] = {
                        "current_stop": current_stop,
                        "seen_at": now_seen,
                        "city_timestamp": city_timestamp
                    }

                    warning_check_stop = current_stop
                    warning_check_seen_at = now_seen

                else:
                    bus_tracker[bus_id]["city_timestamp"] = city_timestamp
                    warning_check_stop = prev_stop
                    warning_check_seen_at = prev_seen_at

        if warning_check_stop is not None and warning_check_seen_at is not None:
            maybe_send_active_delay_warning(
                bus_id=bus_id,
                current_stop=warning_check_stop,
                seen_at=warning_check_seen_at
            )

        if lap_completed:
            print_bus_lap_summary(bus_id)
            with bus_current_lap_lock:
                bus_current_lap[bus_id] = {}

        new_schedule = build_bus_forecast(
            bus_id=bus_id,
            current_stop=current_stop
        )

        with last_bus_schedule_lock:
            old_schedule = last_bus_schedule.get(bus_id)

            if schedule_changed_significantly(old_schedule, new_schedule):
                last_bus_schedule[bus_id] = new_schedule
                updates_to_push.append(new_schedule)

    process_mixed_bus_navigation()

    if updates_to_push and PULT_CALLBACK_URL:
        payload = {
            "type": "bus_schedule_update",
            "updated_at": int(time.time()),
            "buses": updates_to_push
        }
        asyncio.run(send_to_pult(payload))


def bus_monitor_loop():
    while True:
        city_data = asyncio.run(fetch_city_state())
        if city_data is not None:
            process_type1_devices(city_data)
            process_type2_devices(city_data)
            process_navigation_events(city_data)
            update_bus_state_from_city(city_data)
        time.sleep(CITY_FETCH_INTERVAL)


@app.route("/api/bus-schedule", methods=["GET"])
def bus_schedule():
    result = []

    now = time.time()

    with bus_tracker_lock:
        tracker_copy = bus_tracker.copy()

    for bus_id, info in tracker_copy.items():
        current_stop = info.get("current_stop")
        seen_at = info.get("seen_at")

        if current_stop is None or seen_at is None:
            continue

        next_stop = get_next_stop_num(current_stop)

        avg = get_segment_average(bus_id, current_stop, next_stop)
        if avg is None:
            avg = get_bus_fallback_average(bus_id)

        elapsed = now - seen_at
        remaining = max(0, avg - elapsed)

        delay = False
        if avg > 0 and elapsed >= avg * DELAY_FACTOR_THRESHOLD:
            delay = True

        result.append({
            "bus_id": bus_id,
            "current_station": get_stop_name(bus_id, current_stop),
            "next_station": get_stop_name(bus_id, next_stop),
            "eta_to_next_station_seconds": int(round(remaining)),
            "delay": delay
        })

    return jsonify({
        "status": "ok",
        "buses": result
    }), 200


@app.route("/route", methods=["POST"])
def receive_route():
    global pult_command, last_route_result

    data = request.get_json(silent=True)

    if not data:
        return jsonify({
            "status": "Ошибка",
            "message": "JSON не получен"
        }), 400

    route_type = data.get("route_type")
    start = data.get("start")
    end = data.get("end")
    user_id = data.get("user_id")
    intermediate_points = data.get("intermediate_points", [])
    received_at = data.get("received_at", int(time.time()))

    if route_type is None or start is None or end is None or user_id is None:
        return jsonify({
            "status": "Ошибка",
            "message": "Не хватает полей route_type, start, end, user_id"
        }), 400

    if not isinstance(intermediate_points, list):
        return jsonify({
            "status": "Ошибка",
            "message": "Поле intermediate_points должно быть списком"
        }), 400

    start = str(start)
    end = str(end)
    user_id = str(user_id).strip()
    intermediate_points = [str(point) for point in intermediate_points]

    if not user_id:
        return jsonify({
            "status": "Ошибка",
            "message": "Поле user_id пустое"
        }), 400

    with pult_command_lock:
        pult_command = {
            "route_type": route_type,
            "start": start,
            "end": end,
            "intermediate_points": intermediate_points,
            "user_id": user_id,
            "received_at": received_at
        }

    print("\n[PULT COMMAND RECEIVED]")
    print(json.dumps(pult_command, ensure_ascii=False, indent=2))

    route_result = build_route_with_intermediate_points(
        route_type=route_type,
        start=start,
        end=end,
        intermediate_points=intermediate_points
    )
    route_result["user_id"] = user_id

    with route_result_lock:
        last_route_result = route_result

    print("\n[ROUTE RESULT]")
    print(json.dumps(route_result, ensure_ascii=False, indent=2))

    if route_result["status"] != "OK":
        return jsonify(route_result), 400

    ok, nav_result = start_navigation_session(
        user_id=user_id,
        route_type=route_type,
        start=start,
        end=end,
        path=route_result["path"]
    )

    if not ok:
        response = dict(route_result)
        response["navigation"] = nav_result
        return jsonify(response), 500

    response = dict(route_result)
    response["navigation"] = nav_result

    return jsonify(response), 200


@app.route("/route/active", methods=["GET"])
def get_active_routes():
    with active_routes_lock:
        return jsonify({
            "status": "OK",
            "routes": active_routes
        }), 200


@app.route("/camera/event", methods=["POST"])
def receive_camera_event():
    global last_camera_event

    data = request.get_json(silent=True)

    if not data:
        return jsonify({
            "status": "Ошибка",
            "message": "JSON не получен"
        }), 400

    number = data.get("number")
    user_id = data.get("user_id")
    conf_in_char = data.get("confidence")

    if number is None or user_id is None:
        return jsonify({
            "status": "Ошибка",
            "message": "Не хватает полей number, user_id"
        }), 400

    try:
        number = int(number)
    except (TypeError, ValueError):
        return jsonify({
            "status": "Ошибка",
            "message": "Поле number должно быть числом"
        }), 400

    user_id = str(user_id).strip()
    if not user_id:
        return jsonify({
            "status": "Ошибка",
            "message": "Поле user_id пустое"
        }), 400

    city_payload = {
        "type": 6,
        "device_id": number,
        "user_id": user_id,
        "confidence": conf_in_char
    }

    with last_camera_event_lock:
        last_camera_event = {
            "received_payload": data,
            "forwarded_payload": city_payload,
            "saved_at": int(time.time())
        }

    print("\n[CAMERA EVENT RECEIVED]")
    print(json.dumps(data, ensure_ascii=False, indent=2))

    print("\n[FORWARD TYPE 6 TO CITY]")
    print(json.dumps(city_payload, ensure_ascii=False, indent=2))

    status_code, response_text = asyncio.run(send_event(city_payload))

    if status_code is None:
        return jsonify({
            "status": "Ошибка",
            "message": "Не удалось отправить событие в городской сервер",
            "forwarded_payload": city_payload,
            "city_response_status": status_code,
            "city_response_text": response_text
        }), 500

    return jsonify({
        "status": "OK",
        "message": "Событие type 6 отправлено на городской сервер",
        "forwarded_payload": city_payload,
        "city_response_status": status_code,
        "city_response_text": response_text
    }), 200


@app.route("/camera/last", methods=["GET"])
def get_last_camera_event():
    with last_camera_event_lock:
        return jsonify(last_camera_event), 200


@app.route("/api/clothes", methods=["GET"])
def get_clothes_data():
    with last_clothes_data_lock:
        print(last_clothes_data)
        return jsonify({
            "status": "ok",
            "data": last_clothes_data
        }), 200


@app.route("/pult/last", methods=["GET"])
def get_last_pult_command():
    with pult_command_lock:
        return jsonify(pult_command), 200


@app.route("/route/last_path", methods=["GET"])
def get_last_route_path():
    with route_result_lock:
        return jsonify(last_route_result), 200


@app.route("/city/state", methods=["GET"])
def get_saved_city_state():
    with state_lock:
        return jsonify(city_state), 200


@app.route("/buses/schedule", methods=["GET"])
def get_buses_schedule():
    with last_bus_schedule_lock:
        return jsonify({
            "updated_at": int(time.time()),
            "buses": last_bus_schedule
        }), 200


@app.route("/buses/stats", methods=["GET"])
def get_buses_stats():
    result = {}

    with segment_stats_lock:
        for bus_id, segments in segment_stats.items():
            result[bus_id] = {}
            for segment_key, item in segments.items():
                avg = item["total_time"] / item["samples"] if item["samples"] > 0 else 0
                result[bus_id][segment_key] = {
                    "samples": item["samples"],
                    "average_time": round(avg, 2),
                    "total_time": round(item["total_time"], 2)
                }

    return jsonify(result), 200


@app.route("/buses/last_warning", methods=["GET"])
def get_last_warning():
    with last_delay_warning_lock:
        return jsonify(last_delay_warning), 200


def delayed_send_to_yotik(port, command, delay_seconds):
    def worker():
        time.sleep(delay_seconds)
        send_to_yotik(port, command)

        if port == 8888:
            with yotik_8888_state_lock:
                yotik_8888_state["active"] = False

    threading.Thread(target=worker, daemon=True).start()


def handle_yotik_8888_line(line):
    """
    На 8888 приходит строка с одним числом.
    Если число < 200:
      - один раз на событие отправляем vib
      - шлём type 1 в город
      - через 5 секунд шлём dv
    Пока датчик не вернётся обратно в >= 200, повторно не срабатываем.
    """
    try:
        value = float(str(line).strip())
    except (TypeError, ValueError):
        print(f"[YOTIK 8888] Некорректная строка: {line!r}")
        return

    should_trigger = False

    with yotik_8888_state_lock:
        yotik_8888_state["last_value"] = value

        if value >= YOTIK_8888_RESET_THRESHOLD:
            yotik_8888_state["rearmed"] = True
            return

        if (
            value < YOTIK_8888_TRIGGER_THRESHOLD
            and yotik_8888_state["rearmed"]
            and not yotik_8888_state["active"]
        ):
            yotik_8888_state["active"] = True
            yotik_8888_state["rearmed"] = False
            yotik_8888_state["last_trigger_at"] = time.time()
            should_trigger = True

    if not should_trigger:
        return

    print(f"[YOTIK 8888 TRIGGER] value={value}")

    send_to_yotik(8888, "vib")

    payload = build_city_type1_warning_payload()
    status_code, response_text = asyncio.run(send_event(payload))

    print(
        f"[YOTIK 8888 -> CITY TYPE1] "
        f"status={status_code} response={response_text}"
    )

    delayed_send_to_yotik(
        port=8888,
        command="dv",
        delay_seconds=YOTIK_8888_RELEASE_DELAY_SECONDS
    )


def handle_yotik_8883_line(line):
    """
    На 8883 приходит строка из трёх чисел через пробел.
    Сохраняем последнюю валидную строку.
    """
    global last_clothes_data

    raw = str(line).strip()
    parts = raw.split()

    if len(parts) != 3:
        print(f"[YOTIK 8883] Ожидалось 3 числа, пришло: {raw!r}")
        return

    try:
        values = [float(x) for x in parts]
    except ValueError:
        print(f"[YOTIK 8883] Не удалось распарсить числа: {raw!r}")
        return

    payload = {
        "raw": raw,
        "values": values,
        "saved_at": int(time.time())
    }

    with last_clothes_data_lock:
        last_clothes_data = payload

    print(f"[YOTIK 8883 SAVED] {json.dumps(payload, ensure_ascii=False)}")


def process_yotik_incoming_line(listen_port, line):
    line = str(line).strip()
    if not line:
        return

    print(f"[YOTIK RECV] port={listen_port} line={line!r}")

    if listen_port == 8888:
        handle_yotik_8888_line(line)
    elif listen_port == 8883:
        handle_yotik_8883_line(line)


def handle_yotik_client(conn, addr, listen_port):
    print(f"[YOTIK CONNECTED] {addr[0]}:{addr[1]} -> local port {listen_port}")

    with yotik_lock:
        yotik_conns[listen_port] = conn

    buffer = ""

    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break

            try:
                chunk = data.decode("utf-8", errors="ignore")
            except Exception:
                continue

            buffer += chunk

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                process_yotik_incoming_line(listen_port, line)

        tail = buffer.strip()
        if tail:
            process_yotik_incoming_line(listen_port, tail)

    except Exception as e:
        print(f"[ERROR] YOTIK {listen_port}: {e}")
    finally:
        with yotik_lock:
            if yotik_conns.get(listen_port) == conn:
                del yotik_conns[listen_port]

        conn.close()
        print(f"[YOTIK DISCONNECTED] {addr[0]}:{addr[1]} -> local port {listen_port}")


def start_yotik_server(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((YOTIK_HOST, port))
        server.listen(1)

        print(f"[YOTIK SERVER] TCP {YOTIK_HOST}:{port}")
        print(f"[YOTIK SERVER] Ожидание подключения Йотика на порту {port}...")

        while True:
            conn, addr = server.accept()
            threading.Thread(
                target=handle_yotik_client,
                args=(conn, addr, port),
                daemon=True
            ).start()


def send_to_yotik(port, command):
    with yotik_lock:
        conn = yotik_conns.get(port)

        if conn is None:
            print(f"[INFO] Йотик на порту {port} не подключён")
            return

        try:
            conn.sendall((command + "\n").encode("utf-8"))
            print(f"[YOTIK SENT] port={port} command={command}")
        except Exception as e:
            print(f"[ERROR] Send to YOTIK {port}: {e}")


def console_loop():
    print("Команды:")
    print("  on1                  -> отправить 'on' Йотику на 8885")
    print("  on2                  -> отправить 'on' Йотику на 8886")
    print("  on3                  -> отправить 'on' Йотику на 8887")
    print("  on4                  -> отправить 'on' Йотику на 8888")
    print("  send <port> <cmd>    -> отправить произвольную команду в Йотик")
    print("                         пример: send 8883 vib")
    print("                         пример: send 8888 dv")
    print("  state                -> получить state с городского сервера")
    print("  show_state           -> показать сохранённый state")
    print("  show_pult            -> показать последнюю команду пульта")
    print("  show_route           -> показать последний рассчитанный маршрут")
    print("  show_routes          -> показать активные маршруты пользователей")
    print("  show_buses           -> показать прогноз автобусов")
    print("  show_stats           -> показать статистику по перегонам автобусов")
    print("  show_warning         -> показать последнее предупреждение")
    print("  show_camera          -> показать последнее событие камеры")
    print("  show_clothes         -> показать последние данные одежды с 8883")
    print("  svet R,G,B           -> отправить на 8884 команду on R G B 1488")
    print("  exit                 -> выход\n")

    while True:
        raw_cmd = input("Команда: ").strip()
        cmd = raw_cmd.lower()

        if cmd == "exit":
            break

        elif cmd == "on1":
            send_to_yotik(8885, "on")
            continue

        elif cmd == "on2":
            send_to_yotik(8886, "on")
            continue

        elif cmd == "on3":
            send_to_yotik(8887, "on")
            continue

        elif cmd == "on4":
            send_to_yotik(8888, "on")
            continue

        elif cmd.startswith("send "):
            parts = raw_cmd.split(maxsplit=2)
            if len(parts) < 3:
                print("Формат: send <port> <command>")
                continue

            try:
                port = int(parts[1])
            except ValueError:
                print("Порт должен быть числом")
                continue

            command = parts[2].strip()
            if not command:
                print("Команда пустая")
                continue

            send_to_yotik(port, command)
            continue

        elif cmd == "state":
            data = asyncio.run(fetch_city_state())
            if data is not None:
                print("\n[CITY STATE UPDATED]")
                print(json.dumps(data, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_state":
            with state_lock:
                print("\n[CURRENT CITY STATE]")
                print(json.dumps(city_state, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_pult":
            with pult_command_lock:
                print("\n[LAST PULT COMMAND]")
                print(json.dumps(pult_command, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_route":
            with route_result_lock:
                print("\n[LAST ROUTE RESULT]")
                print(json.dumps(last_route_result, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_routes":
            with active_routes_lock:
                print("\n[ACTIVE ROUTES]")
                print(json.dumps(active_routes, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_buses":
            with last_bus_schedule_lock:
                print("\n[BUS SCHEDULE]")
                print(json.dumps(last_bus_schedule, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_stats":
            with segment_stats_lock:
                printable = {}
                for bus_id, segments in segment_stats.items():
                    printable[bus_id] = {}
                    for segment_key, item in segments.items():
                        avg = item["total_time"] / item["samples"] if item["samples"] > 0 else 0
                        printable[bus_id][segment_key] = {
                            "samples": item["samples"],
                            "average_time": round(avg, 2),
                            "total_time": round(item["total_time"], 2)
                        }

                print("\n[BUS SEGMENT STATS]")
                print(json.dumps(printable, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_warning":
            with last_delay_warning_lock:
                print("\n[LAST DELAY WARNING]")
                print(json.dumps(last_delay_warning, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_camera":
            with last_camera_event_lock:
                print("\n[LAST CAMERA EVENT]")
                print(json.dumps(last_camera_event, ensure_ascii=False, indent=2))
            continue

        elif cmd == "show_clothes":
            with last_clothes_data_lock:
                print("\n[LAST CLOTHES DATA]")
                print(json.dumps(last_clothes_data, ensure_ascii=False, indent=2))
            continue

        elif cmd.startswith("svet "):
            parts = raw_cmd.split()

            if len(parts) != 2:
                print("Формат: svet R,G,B")
                continue

            rgb_part = parts[1]
            rgb_values = rgb_part.split(",")

            if len(rgb_values) != 3:
                print("Формат: svet R,G,B")
                continue

            try:
                r = int(rgb_values[0])
                g = int(rgb_values[1])
                b = int(rgb_values[2])
            except ValueError:
                print("R, G, B должны быть числами")
                continue

            duration = 1488
            command = f"on {r} {g} {b} {duration}"

            send_to_yotik(8884, command)
            print(f"[SVET SENT] port=8884 command={command}")
            continue

        else:
            print("Доступные команды: on1, on2, on3, on4, send <port> <command>, state, show_state, show_pult, show_route, show_routes, show_buses, show_stats, show_warning, show_camera, show_clothes, svet R,G,B, exit")
            continue


if __name__ == "__main__":
    for port in YOTIK_PORTS:
        threading.Thread(
            target=start_yotik_server,
            args=(port,),
            daemon=True
        ).start()

    threading.Thread(target=console_loop, daemon=True).start()
    threading.Thread(target=bus_monitor_loop, daemon=True).start()

    print(f"[SYSTEM] HTTP сервер запущен на {HTTP_HOST}:{HTTP_PORT}")
    app.run(host=HTTP_HOST, port=HTTP_PORT, debug=False)