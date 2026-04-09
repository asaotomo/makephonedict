import csv
import builtins
import json
import math
import os
import random
import re
import struct
import tempfile
import time
import urllib.request
import urllib.error
from pathlib import Path


ISP_NAME_ALIASES = {
    "1": "中国移动",
    "中国移动": "中国移动",
    "移动": "中国移动",
    "cmcc": "中国移动",
    "4001": "中国移动",
    "2": "中国联通",
    "中国联通": "中国联通",
    "联通": "中国联通",
    "cucc": "中国联通",
    "4006": "中国联通",
    "3": "中国电信",
    "中国电信": "中国电信",
    "电信": "中国电信",
    "ctcc": "中国电信",
    "4008": "中国电信",
    "4": "中国电信虚拟运营商",
    "中国电信虚拟运营商": "中国电信虚拟运营商",
    "电信虚拟运营商": "中国电信虚拟运营商",
    "5": "中国联通虚拟运营商",
    "中国联通虚拟运营商": "中国联通虚拟运营商",
    "联通虚拟运营商": "中国联通虚拟运营商",
    "6": "中国移动虚拟运营商",
    "中国移动虚拟运营商": "中国移动虚拟运营商",
    "移动虚拟运营商": "中国移动虚拟运营商",
}

ISP_TYPE_TO_NAME = {
    1: "中国移动",
    2: "中国联通",
    3: "中国电信",
    4: "中国电信虚拟运营商",
    5: "中国联通虚拟运营商",
    6: "中国移动虚拟运营商",
}
ISP_SORT_ORDER = {name: idx for idx, name in enumerate([ISP_TYPE_TO_NAME[i] for i in sorted(ISP_TYPE_TO_NAME)])}

PANGONGZI_PHONE_DAT_URL = "https://raw.githubusercontent.com/pangongzi/phone/master/src/data/phone.dat"
LAST_CONFIG_FILE = "last_run_config.json"
APP_SETTINGS_FILE = "app_settings.json"
DEFAULT_APP_SETTINGS = {
    "auto_update": True,
    "check_interval_hours": 24,
}

_SEGMENT_ROWS_CACHE = None
_SEGMENT_INFO_CACHE = None
_SEGMENT_CSV_MTIME = None
_UPDATE_META_CACHE = {
    "etag": "",
    "last_modified": "",
    "last_check_ts": 0,
}


class UserAbort(Exception):
    pass


def _safe_input(prompt=""):
    try:
        return builtins.input(prompt)
    except (EOFError, KeyboardInterrupt):
        raise UserAbort("用户中断输入")


# 统一安全输入包装
input = _safe_input


def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def _print_wizard_banner():
    print(r"""
 __  __       _        ____  _                      ____  _      _   
|  \/  | __ _| | _____|  _ \| |__   ___  _ __   ___|  _ \(_) ___| |_ 
| |\/| |/ _` | |/ / _ \ |_) | '_ \ / _ \| '_ \ / _ \ | | | |/ __| __|
| |  | | (_| |   <  __/  __/| | | | (_) | | | |  __/ |_| | | (__| |_ 
|_|  |_|\__,_|_|\_\___|_|   |_| |_|\___/|_| |_|\___|____/|_|\___|\__|
Hx0战队-手机号字典生成器V1.3               Update:2026.04.09
""")


def _pause():
    input("\n[+] 按回车继续...")


def _last_config_path():
    return Path(__file__).with_name(LAST_CONFIG_FILE)


def _load_last_config():
    path = _last_config_path()
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="UTF-8") as f:
            data = json.load(f)
        total = int(data.get("total", 0))
        isps = [str(x).strip() for x in data.get("isp_names", []) if str(x).strip()]
        cities = [str(x).strip() for x in data.get("city_names", []) if str(x).strip()]
        mode = str(data.get("generation_mode", "random")).strip().lower()
        if mode not in {"random", "sequential"}:
            mode = "random"
        if total < 1 or not isps or not cities:
            return None
        return {"total": total, "isp_names": isps, "city_names": cities, "generation_mode": mode}
    except Exception:
        return None


def _save_last_config(total, isp_names, city_names, generation_mode):
    mode = generation_mode if generation_mode in {"random", "sequential"} else "random"
    payload = {
        "total": int(total),
        "isp_names": list(isp_names),
        "city_names": list(city_names),
        "generation_mode": mode,
    }
    with _last_config_path().open("w", encoding="UTF-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _settings_path():
    return Path(__file__).with_name(APP_SETTINGS_FILE)


def _load_app_settings():
    path = _settings_path()
    data = {}
    if path.exists():
        try:
            with path.open("r", encoding="UTF-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                data = loaded
        except Exception:
            data = {}

    try:
        interval_hours = int(data.get("check_interval_hours", DEFAULT_APP_SETTINGS["check_interval_hours"]))
    except Exception:
        interval_hours = DEFAULT_APP_SETTINGS["check_interval_hours"]

    settings = {
        "auto_update": bool(data.get("auto_update", DEFAULT_APP_SETTINGS["auto_update"])),
        "check_interval_hours": interval_hours,
    }
    if settings["check_interval_hours"] < 0:
        settings["check_interval_hours"] = DEFAULT_APP_SETTINGS["check_interval_hours"]

    # 文件不存在或字段缺失时自动落盘，形成可编辑配置
    if (not path.exists()) or any(k not in data for k in DEFAULT_APP_SETTINGS):
        with path.open("w", encoding="UTF-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
    return settings


def _save_app_settings(settings):
    payload = {
        "auto_update": bool(settings.get("auto_update", DEFAULT_APP_SETTINGS["auto_update"])),
        "check_interval_hours": int(settings.get("check_interval_hours", DEFAULT_APP_SETTINGS["check_interval_hours"])),
    }
    if payload["check_interval_hours"] < 0:
        payload["check_interval_hours"] = DEFAULT_APP_SETTINGS["check_interval_hours"]
    with _settings_path().open("w", encoding="UTF-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _touch_last_check_ts():
    _UPDATE_META_CACHE["last_check_ts"] = int(time.time())


def _print_last_config_summary():
    last = _load_last_config()
    if not last:
        print("[*] 暂无上次配置。先完成一次生成后可快速复用。")
        return

    city_preview = ",".join(last["city_names"][:5])
    if len(last["city_names"]) > 5:
        city_preview += f"...(共{len(last['city_names'])}个)"
    print(
        f"[*] 上次配置：数量={last['total']} | 模式={last['generation_mode']} | 运营商={','.join(last['isp_names'])} | 城市={city_preview}"
    )


def _prompt_menu_choice():
    print("[!] 欢迎使用手机号字典生成器")
    print("[?] 快速开始：按回车直接进入“生成手机号字典”")
    print("  1. 📱 生成手机号字典（推荐）")
    print("  2. 📊 查看号段库统计信息")
    print("  3. 🔄 重新构建本地号段库（phone.dat -> CSV）")
    print("  4. ⚡ 使用上次配置重新生成")
    print("  5. 🔎 查询手机号归属地（支持批量）")
    print("  6. ⚙️ 更新设置（自动更新/检查间隔）")
    print("  0. 🚪 退出程序")
    choice = input("\n[>] 请输入菜单编号（默认1）: ").strip()
    return choice or "1"


def _prompt_total_count():
    while True:
        raw = input("[1/3] 请输入要生成的手机号数量（默认1000，输入 b 返回主菜单）: ").strip()
        if raw.lower() in {"b", "back"}:
            return None
        if not raw:
            return 1000
        if not raw.isdigit():
            print("[!] 请输入数字。")
            continue
        total = int(raw)
        if total < 1:
            print("[!] 数量必须 >= 1。")
            continue
        return total


def _parse_phone_inputs(raw_text):
    normalized = raw_text.replace("，", ",").replace("\n", ",").replace("\t", ",").replace(" ", ",")
    tokens = [x.strip() for x in normalized.split(",") if x.strip()]
    return tokens


def _parse_city_inputs(raw_text):
    # 支持逗号/空格/换行/顿号/分号等多种分隔
    parts = re.split(r"[,\s，、;；]+", raw_text.strip())
    return [p.strip().replace(" ", "") for p in parts if p.strip()]


def _dedupe_keep_order(items):
    seen = set()
    output = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _resolve_input_file_path(raw_path):
    p = Path(raw_path).expanduser()
    if p.is_absolute():
        return p
    cwd_path = Path.cwd() / p
    if cwd_path.exists():
        return cwd_path
    return Path(__file__).parent / p


def _extract_phones_from_file(raw_path):
    file_path = _resolve_input_file_path(raw_path)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    if not file_path.is_file():
        raise ValueError(f"不是有效文件: {file_path}")

    suffix = file_path.suffix.lower()
    phones = []

    if suffix == ".txt":
        text = file_path.read_text(encoding="UTF-8", errors="ignore")
        phones = _parse_phone_inputs(text)
    elif suffix == ".csv":
        with file_path.open("r", encoding="UTF-8", errors="ignore", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            has_header = csv.Sniffer().has_header(sample) if sample.strip() else False
            if has_header:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row:
                        continue
                    # 优先读取常见列名
                    value = (
                        row.get("手机号")
                        or row.get("phone")
                        or row.get("mobile")
                        or row.get("号码")
                        or next(iter(row.values()), "")
                    )
                    phones.extend(_parse_phone_inputs(str(value)))
            else:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    phones.extend(_parse_phone_inputs(str(row[0])))
    else:
        raise ValueError("仅支持 .txt 或 .csv 文件")

    return _dedupe_keep_order(phones)


def _invalidate_segment_cache():
    global _SEGMENT_ROWS_CACHE, _SEGMENT_INFO_CACHE, _SEGMENT_CSV_MTIME
    _SEGMENT_ROWS_CACHE = None
    _SEGMENT_INFO_CACHE = None
    _SEGMENT_CSV_MTIME = None


def _parse_index_selection(raw_text, total_count):
    text = raw_text.strip().lower()
    if text in {"all", "a", "*"}:
        return list(range(total_count))

    selected = set()
    chunks = [x.strip() for x in raw_text.replace("，", ",").split(",") if x.strip()]
    if not chunks:
        raise ValueError("未输入有效序号。")

    for chunk in chunks:
        if "-" in chunk:
            parts = [p.strip() for p in chunk.split("-", 1)]
            if len(parts) != 2 or (not parts[0].isdigit()) or (not parts[1].isdigit()):
                raise ValueError(f"区间格式无效: {chunk}")
            start = int(parts[0])
            end = int(parts[1])
            if start > end:
                raise ValueError(f"区间起始不能大于结束: {chunk}")
            for i in range(start, end + 1):
                if i < 1 or i > total_count:
                    raise ValueError(f"序号越界: {i}")
                selected.add(i - 1)
        else:
            if not chunk.isdigit():
                raise ValueError(f"序号格式无效: {chunk}")
            idx = int(chunk)
            if idx < 1 or idx > total_count:
                raise ValueError(f"序号越界: {idx}")
            selected.add(idx - 1)

    return sorted(selected)


def _prompt_multi_select(title, display_options, default_all=False, allow_back=False):
    if not display_options:
        raise ValueError(f"{title} 可选项为空。")

    print(f"\n[+] {title}")
    for i, line in enumerate(display_options, start=1):
        print(f"{i:>3}. {line}")

    while True:
        hint = "[+] 请输入序号（示例: 1,3 或 2-4，全部输入 all）"
        if default_all:
            hint += "，回车=all"
        if allow_back:
            hint += "，输入 b 返回上一步"
        hint += ": "
        raw = input(hint)
        if allow_back and raw.strip().lower() in {"b", "back"}:
            return None
        if default_all and not raw.strip():
            return list(range(len(display_options)))
        try:
            idx_list = _parse_index_selection(raw, len(display_options))
            return idx_list
        except Exception as e:
            print(f"[!] 输入无效: {e}")


def _normalize_isp_name(value):
    key = value.strip().lower()
    if key in ISP_NAME_ALIASES:
        return ISP_NAME_ALIASES[key]
    return ISP_NAME_ALIASES.get(value.strip(), value.strip())


def _select_isp_names(segment_rows):
    isp_order = [ISP_TYPE_TO_NAME[i] for i in sorted(ISP_TYPE_TO_NAME)]
    isp_segment_count = {name: 0 for name in isp_order}

    for row in segment_rows:
        if row["isp"] in isp_segment_count:
            isp_segment_count[row["isp"]] += 1

    display = [
        f'{name}（可用7位号段: {isp_segment_count[name]}）'
        for name in isp_order
    ]

    while True:
        selected_idx = _prompt_multi_select(
            "[2/3] 请选择要生成的运营商",
            display,
            default_all=True,
            allow_back=True,
        )
        if selected_idx is None:
            return None
        selected_isps = [isp_order[i] for i in selected_idx]
        if not selected_isps:
            print("[!] 至少选择一个运营商。")
            continue

        empty_isps = [x for x in selected_isps if isp_segment_count.get(x, 0) == 0]
        if empty_isps:
            print(f"[!] 以下运营商当前号段库中无数据：{','.join(empty_isps)}，请重新选择。")
            continue
        return selected_isps


def _confirm_yes(prompt):
    raw = input(prompt).strip().lower()
    return raw in {"y", "yes", "是", "确认", "1"}


def _prompt_generation_mode():
    while True:
        raw = input("[0/3] 生成模式：1随机(默认) / 2顺序，输入 b 返回主菜单: ").strip().lower()
        if raw in {"", "1", "random", "r"}:
            return "random"
        if raw in {"2", "sequential", "s"}:
            return "sequential"
        if raw in {"b", "back"}:
            return None
        print("[!] 输入无效，请输入 1 或 2。")


def _iter_indices(total, max_total, generation_mode):
    if generation_mode == "sequential":
        for i in range(total):
            yield i
        return

    # 随机模式：用线性同余的遍历方式，避免重复且无需超大内存
    start = random.randrange(max_total)
    step = random.randrange(1, max_total)
    while math.gcd(step, max_total) != 1:
        step = random.randrange(1, max_total)
    for i in range(total):
        yield (start + i * step) % max_total


def _select_city_names(segment_rows):
    city_segment_count = {}
    for row in segment_rows:
        city = row["city"].strip()
        if not city:
            continue
        city_segment_count[city] = city_segment_count.get(city, 0) + 1

    all_city_names = sorted(city_segment_count.keys())
    if not all_city_names:
        raise ValueError("号段库中无可选城市。")

    while True:
        print("\n[3/3] 请选择城市")
        print("    方式A：直接输入城市名（支持多选），例如：成都,北京")
        print("    方式B：输入 ? 进入关键词搜索后按序号选择")
        print("    返回上一步：输入 b")
        raw = input("[+] 请输入城市: ").strip()
        if not raw:
            print("[!] 城市不能为空。")
            continue
        if raw.lower() in {"b", "back"}:
            return None

        if raw in {"?", "？", "h", "help"}:
            keyword_input = input("[+] 输入关键词（如 成都/北京，多个逗号分隔，输入 b 返回上一步）: ").strip()
            if keyword_input.lower() in {"b", "back"}:
                return None
            keywords = [x.strip() for x in keyword_input.replace("，", ",").split(",") if x.strip()]
            if not keywords:
                print("[!] 关键词不能为空。")
                continue
            city_names = [
                city for city in all_city_names
                if any(keyword in city for keyword in keywords)
            ]
            if not city_names:
                print("[!] 没有匹配到城市，请换个关键词。")
                continue
            if len(city_names) > 80:
                print(f"[!] 匹配到 {len(city_names)} 个城市，请增加关键词缩小范围。")
                continue

            city_display = [f'{name}（可用7位号段: {city_segment_count.get(name, 0)}）' for name in city_names]
            selected_city_idx = _prompt_multi_select("请选择城市（可多选）", city_display, allow_back=True)
            if selected_city_idx is None:
                return None
            selected_cities = [city_names[i] for i in selected_city_idx]
            if selected_cities:
                print(f"[*] 已选择城市：{','.join(selected_cities)}")
                confirm = input("[+] 确认使用以上城市？(y确认，其他重新选择): ").strip().lower()
                if confirm in {"y", "yes", "是", "确认", "1"}:
                    return selected_cities
                print("[*] 已取消本次城市选择，请重新输入。")
                continue
            print("[!] 至少选择一个城市。")
            continue

        wanted_raw = _parse_city_inputs(raw)
        if not wanted_raw:
            print("[!] 未识别到有效城市。")
            continue

        wanted = []
        missing = []
        ambiguous = []
        for token in wanted_raw:
            if token in city_segment_count:
                wanted.append(token)
                continue

            candidates = [c for c in all_city_names if c.startswith(token) or token in c]
            if len(candidates) == 1:
                wanted.append(candidates[0])
            elif len(candidates) > 1:
                ambiguous.append((token, candidates[:5]))
            else:
                missing.append(token)

        wanted = _dedupe_keep_order(wanted)
        if ambiguous:
            for token, cands in ambiguous:
                print(f"[!] 城市“{token}”匹配到多个候选：{','.join(cands)}，请输入更完整城市名或用 ? 搜索。")
            continue
        if missing:
            print(f"[!] 以下城市未找到：{','.join(missing)}。可输入 ? 进入搜索。")
            continue
        print(f"[*] 识别到城市：{','.join(wanted)}")
        confirm = input("[+] 确认使用以上城市？(y确认，其他重新输入): ").strip().lower()
        if confirm in {"y", "yes", "是", "确认", "1"}:
            return wanted
        print("[*] 已取消本次城市选择，请重新输入。")


def _load_segments_db():
    global _SEGMENT_ROWS_CACHE, _SEGMENT_INFO_CACHE, _SEGMENT_CSV_MTIME
    db_path = Path(__file__).with_name("phone_segments.csv")
    _ensure_segments_csv(db_path)
    current_mtime = db_path.stat().st_mtime
    if _SEGMENT_ROWS_CACHE is not None and _SEGMENT_CSV_MTIME == current_mtime:
        return _SEGMENT_ROWS_CACHE

    with db_path.open("r", encoding="UTF-8", newline="") as f:
        reader = csv.DictReader(f)
        required_headers = {"segment", "province", "city", "isp"}
        if not reader.fieldnames:
            raise ValueError("phone_segments.csv 缺少表头，必须包含: segment,province,city,isp")
        if not required_headers.issubset(set(reader.fieldnames)):
            raise ValueError("phone_segments.csv 表头不完整，必须包含: segment,province,city,isp")

        rows = []
        segment_info = {}
        for raw in reader:
            segment = str(raw.get("segment", "")).strip()
            province = str(raw.get("province", "")).strip()
            city = str(raw.get("city", "")).strip()
            isp = _normalize_isp_name(str(raw.get("isp", "")).strip())

            if not (segment.isdigit() and len(segment) == 7):
                continue
            if not city or not isp:
                continue

            rows.append({
                "segment": segment,
                "province": province,
                "city": city,
                "isp": isp,
            })
            if province == city:
                location = city
            else:
                location = f"{province}{city}"
            segment_info[segment] = f"{location}{isp}"

    if not rows:
        raise ValueError("phone_segments.csv 没有可用数据，请导入真实 7 位号段数据。")
    _SEGMENT_ROWS_CACHE = rows
    _SEGMENT_INFO_CACHE = segment_info
    _SEGMENT_CSV_MTIME = current_mtime
    return rows


def _get_segment_info_map():
    _load_segments_db()
    return _SEGMENT_INFO_CACHE or {}


def _segments_csv_looks_valid(csv_path):
    if not csv_path.exists():
        return False
    try:
        with csv_path.open("r", encoding="UTF-8", newline="") as f:
            reader = csv.DictReader(f)
            required_headers = {"segment", "province", "city", "isp"}
            if not reader.fieldnames or not required_headers.issubset(set(reader.fieldnames)):
                return False

            checked = 0
            passed = 0
            for row in reader:
                checked += 1
                segment = str(row.get("segment", "")).strip()
                city = str(row.get("city", "")).strip()
                isp = _normalize_isp_name(str(row.get("isp", "")).strip())

                if segment.isdigit() and len(segment) == 7 and city and (not city.isdigit()) and isp in ISP_TYPE_TO_NAME.values():
                    passed += 1
                if checked >= 100:
                    break

            if checked == 0:
                return False
            return passed / checked >= 0.8
    except Exception:
        return False


def _download_phone_dat(dat_path):
    print("[*] 正在从 pangongzi/phone 下载 phone.dat（免费开源数据）...")
    urllib.request.urlretrieve(PANGONGZI_PHONE_DAT_URL, str(dat_path))
    try:
        req = urllib.request.Request(PANGONGZI_PHONE_DAT_URL, method="HEAD")
        with urllib.request.urlopen(req, timeout=8) as resp:
            _UPDATE_META_CACHE["etag"] = resp.headers.get("ETag", "")
            _UPDATE_META_CACHE["last_modified"] = resp.headers.get("Last-Modified", "")
            _UPDATE_META_CACHE["last_check_ts"] = int(time.time())
    except Exception:
        # 元数据获取失败不影响主流程
        pass
    print(f"[*] phone.dat 下载完成：{dat_path.name}")


def _auto_update_phone_dat_if_needed(dat_path):
    headers = {}
    if _UPDATE_META_CACHE.get("etag"):
        headers["If-None-Match"] = _UPDATE_META_CACHE["etag"]
    if _UPDATE_META_CACHE.get("last_modified"):
        headers["If-Modified-Since"] = _UPDATE_META_CACHE["last_modified"]

    req = urllib.request.Request(PANGONGZI_PHONE_DAT_URL, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            # 返回200说明有新内容（或无条件命中）
            data = resp.read()
            if not data:
                print("[*] 已检测更新：远端数据为空，跳过更新。")
                return False

            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(data)
                tmp_path = Path(tmp.name)
            tmp_path.replace(dat_path)

            _UPDATE_META_CACHE["etag"] = resp.headers.get("ETag", "")
            _UPDATE_META_CACHE["last_modified"] = resp.headers.get("Last-Modified", "")
            _UPDATE_META_CACHE["last_check_ts"] = int(time.time())
            print("[*] 检测到 phone.dat 有更新，已自动下载。")
            return True
    except urllib.error.HTTPError as e:
        if e.code == 304:
            print("[*] phone.dat 无更新，继续使用本地数据。")
            _touch_last_check_ts()
            return False
        print(f"[*] 更新检测失败（HTTP {e.code}），继续使用本地数据。")
        _touch_last_check_ts()
        return False
    except Exception:
        print("[*] 网络不可用或更新检测失败，继续使用本地数据。")
        _touch_last_check_ts()
        return False


def _decode_record_text(record_bytes):
    try:
        return record_bytes.decode("UTF-8")
    except UnicodeDecodeError:
        return record_bytes.decode("GBK", errors="ignore")


def _convert_phone_dat_to_csv(dat_path, csv_path):
    with dat_path.open("rb") as f:
        raw = f.read()
    if len(raw) < 8:
        raise ValueError("phone.dat 文件异常，长度不足。")

    first_index_offset = struct.unpack("<I", raw[4:8])[0]
    if first_index_offset <= 8 or first_index_offset >= len(raw):
        raise ValueError("phone.dat 文件异常，索引偏移不合法。")

    records_area = raw[8:first_index_offset]
    index_area = raw[first_index_offset:]
    if len(index_area) % 9 != 0:
        raise ValueError("phone.dat 文件异常，索引区长度不正确。")

    total_index = len(index_area) // 9
    written = 0
    seen_segments = set()

    with csv_path.open("w", encoding="UTF-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["segment", "province", "city", "isp"])

        for i in range(total_index):
            pos = i * 9
            segment = struct.unpack("<I", index_area[pos:pos + 4])[0]
            rec_offset_raw = struct.unpack("<I", index_area[pos + 4:pos + 8])[0]
            isp_type = index_area[pos + 8]

            isp_name = ISP_TYPE_TO_NAME.get(isp_type)
            if isp_name is None:
                # 保留 1-6 运营商类型，其它类型暂不纳入
                continue

            segment_text = str(segment)
            if len(segment_text) != 7 or not segment_text.isdigit():
                continue
            if segment_text in seen_segments:
                continue

            # phone.dat 偏移是 1-based（从1开始）
            rec_offset = rec_offset_raw - 1
            if rec_offset < 0 or rec_offset >= len(records_area):
                continue
            rec_end = records_area.find(b"\x00", rec_offset)
            if rec_end == -1:
                continue
            record_text = _decode_record_text(records_area[rec_offset:rec_end])
            parts = [p.strip() for p in record_text.split("|")]
            if len(parts) < 3:
                continue

            if len(parts) >= 4:
                province = parts[0]
                city = parts[1]
            else:
                # 兼容仅有单地名字段的数据：<地名>|<邮编>|<区号>
                province = parts[0]
                city = parts[0]
            if not province or not city:
                continue

            writer.writerow([segment_text, province, city, isp_name])
            seen_segments.add(segment_text)
            written += 1

    if written == 0:
        raise ValueError("phone.dat 转换后无可用 1-6 运营商类型号段数据。")
    print(f"[*] 已生成 phone_segments.csv，共 {written} 条 1-6 运营商类型号段。")
    _invalidate_segment_cache()


def _ensure_segments_csv(csv_path):
    if _segments_csv_looks_valid(csv_path):
        return

    dat_path = Path(__file__).with_name("phone.dat")
    if not dat_path.exists():
        _download_phone_dat(dat_path)
    else:
        print("[*] 检测到现有 phone_segments.csv 不可用，正在根据 phone.dat 重新生成...")
    _convert_phone_dat_to_csv(dat_path, csv_path)


def _rebuild_segments_csv_from_dat():
    csv_path = Path(__file__).with_name("phone_segments.csv")
    dat_path = Path(__file__).with_name("phone.dat")
    if not dat_path.exists():
        _download_phone_dat(dat_path)
    print("[*] 检测到所需运营商类型缺失，正在重建 phone_segments.csv...")
    _convert_phone_dat_to_csv(dat_path, csv_path)


def _try_auto_update_and_rebuild():
    settings = _load_app_settings()
    if not settings.get("auto_update", True):
        print("[*] 已关闭自动更新检测（app_settings.json）。")
        return

    dat_path = Path(__file__).with_name("phone.dat")
    csv_path = Path(__file__).with_name("phone_segments.csv")
    if not dat_path.exists():
        # 首次场景由既有逻辑下载，不在此强制处理
        return

    check_interval_seconds = int(settings.get("check_interval_hours", 24)) * 3600
    last_check_ts = int(_UPDATE_META_CACHE.get("last_check_ts", 0) or 0)
    now_ts = int(time.time())
    if check_interval_seconds > 0 and last_check_ts > 0 and (now_ts - last_check_ts) < check_interval_seconds:
        return

    updated = _auto_update_phone_dat_if_needed(dat_path)
    if updated:
        print("[*] 正在根据新 phone.dat 更新 phone_segments.csv ...")
        _convert_phone_dat_to_csv(dat_path, csv_path)
        print("[*] 本地号段库已同步到最新版本。")


def make_dict(total, city_codes, isp_codes, generation_mode="random"):
    if total < 1:
        raise ValueError("手机号数量必须大于0")

    segment_rows = _load_segments_db()

    if not city_codes:
        raise ValueError("城市不能为空，请输入城市名。")
    city_names = city_codes

    if not isp_codes:
        raise ValueError("运营商不能为空，请输入运营商代码或名称。")
    isp_names = [_normalize_isp_name(code) for code in isp_codes]

    available_isps = {row["isp"] for row in segment_rows}
    if not set(isp_names).issubset(available_isps):
        _rebuild_segments_csv_from_dat()
        segment_rows = _load_segments_db()

    matched_rows = [
        row for row in segment_rows
        if row["city"] in city_names and row["isp"] in isp_names
    ]
    if not matched_rows:
        raise ValueError(
            "号段库中未找到匹配数据，请确认城市/运营商输入，或补充 phone_segments.csv。"
        )

    unique_segments = sorted({row["segment"] for row in matched_rows})
    segment_info = {}
    segment_isp = {}
    for row in matched_rows:
        if row["province"] == row["city"]:
            location = row["city"]
        else:
            location = f'{row["province"]}{row["city"]}'
        segment_info[row["segment"]] = f'{location}{row["isp"]}'
        segment_isp[row["segment"]] = row["isp"]

    max_total = len(unique_segments) * 10_000
    if total > max_total:
        raise ValueError(
            f"当前筛选条件下最多可生成 {max_total} 条唯一号码（{len(unique_segments)} 个7位号段），请降低数量。"
        )

    with open("telephone_number_dict.csv", "w+", encoding="UTF-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["手机号", "手机号归属地", "命中号段"])
        generated_rows = []
        for logical_idx in _iter_indices(total, max_total, generation_mode):
            segment_idx = logical_idx % len(unique_segments)
            suffix_idx = logical_idx // len(unique_segments)
            segment = unique_segments[segment_idx]
            phone_num = f"{segment}{suffix_idx:04d}"
            phone_info = segment_info[segment]
            isp_name = segment_isp[segment]
            generated_rows.append((phone_num, phone_info, segment, isp_name))

        # 随机生成后，按运营商分组，再按手机号排序
        generated_rows.sort(key=lambda x: (ISP_SORT_ORDER.get(x[3], 999), int(x[0]), x[1]))

        no = len(generated_rows)
        print_limit = 30
        for i, (phone_num, phone_info, segment, _isp_name) in enumerate(generated_rows):
            if i < print_limit:
                print(phone_num, phone_info)
            elif i == print_limit:
                print(f"...（仅展示前 {print_limit} 条，完整结果见 telephone_number_dict.csv）")
            writer.writerow([phone_num, phone_info, segment])

    print("[*]手机号字典（telephone_number_dict.csv）生成成功，共计生成：{}条，请打开字典查看详细内容！".format(no))


def _show_db_stats():
    rows = _load_segments_db()
    isp_count = {}
    city_set = set()
    for row in rows:
        city_set.add(row["city"])
        isp = row["isp"]
        isp_count[isp] = isp_count.get(isp, 0) + 1

    print("\n[📊 号段库统计]")
    print(f"- 总 7 位号段数: {len(rows)}")
    print(f"- 城市数量: {len(city_set)}")
    for isp_name in [ISP_TYPE_TO_NAME[i] for i in sorted(ISP_TYPE_TO_NAME)]:
        print(f"- {isp_name}: {isp_count.get(isp_name, 0)}")


def _run_query_flow():
    print("\n[🔎 手机号归属地查询]")
    print("    支持单个或多个手机号查询。")
    print("    方式1：手工输入（逗号/空格/换行分隔）")
    print("    方式2：读取 txt/csv 文件进行批量查询")
    print("    输入 b 返回主菜单。")
    mode = input("[+] 请选择方式（1手工输入，2读取文件，默认1）: ").strip().lower()
    if mode in {"b", "back"}:
        print("[*] 已返回主菜单。")
        return
    if mode == "":
        mode = "1"

    if mode == "1":
        raw = input("[+] 请输入手机号: ").strip()
        if raw.lower() in {"b", "back"}:
            print("[*] 已返回主菜单。")
            return
        phones = _parse_phone_inputs(raw)
    elif mode == "2":
        file_input = input("[+] 请输入文件路径（支持 txt/csv）: ").strip()
        if file_input.lower() in {"b", "back"}:
            print("[*] 已返回主菜单。")
            return
        try:
            phones = _extract_phones_from_file(file_input)
            print(f"[*] 已从文件读取 {len(phones)} 个待查询号码。")
        except Exception as e:
            print(f"[!] 文件读取失败: {e}")
            return
    else:
        print("[!] 无效方式，请输入 1 或 2。")
        return

    if not phones:
        print("[!] 未输入有效手机号。")
        return

    segment_info = _get_segment_info_map()

    print("\n[查询结果]")
    result_rows = []
    for phone in phones:
        if not (phone.isdigit() and len(phone) == 11 and phone.startswith("1")):
            print(f"- {phone}: 格式无效（应为11位手机号）")
            result_rows.append([phone, "", "格式无效"])
            continue
        segment = phone[:7]
        info = segment_info.get(segment)
        if info:
            print(f"- {phone}: {info}")
            result_rows.append([phone, info, "命中"])
        else:
            print(f"- {phone}: 未命中本地号段库")
            result_rows.append([phone, "", "未命中"])

    export = input("\n[+] 是否导出查询结果到CSV？(y确认，其他不导出): ").strip().lower()
    if export in {"y", "yes", "是", "确认", "1"}:
        default_name = f"query_result_{time.strftime('%Y%m%d_%H%M%S')}.csv"
        out_input = input(f"[+] 请输入导出文件路径（默认 {default_name}）: ").strip()
        out_path = _resolve_input_file_path(out_input or default_name)
        with out_path.open("w", encoding="UTF-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["手机号", "手机号归属地", "查询状态"])
            writer.writerows(result_rows)
        print(f"[*] 已导出查询结果：{out_path}")


def _run_settings_flow():
    settings = _load_app_settings()
    print("\n[⚙️ 更新设置]")
    print(f"- 当前自动更新: {'开启' if settings['auto_update'] else '关闭'}")
    print(f"- 当前检查间隔(小时): {settings['check_interval_hours']}")
    print("  输入 b 可取消并返回主菜单。")

    raw_auto = input("[+] 自动更新？(y开启/n关闭，回车保持不变): ").strip().lower()
    if raw_auto in {"b", "back"}:
        print("[*] 已取消设置更新。")
        return
    if raw_auto in {"y", "yes", "是", "1"}:
        settings["auto_update"] = True
    elif raw_auto in {"n", "no", "否", "0"}:
        settings["auto_update"] = False

    while True:
        raw_interval = input("[+] 检查间隔小时（>=0，回车保持不变）: ").strip()
        if raw_interval.lower() in {"b", "back"}:
            print("[*] 已取消设置更新。")
            return
        if raw_interval == "":
            break
        if not raw_interval.isdigit():
            print("[!] 请输入非负整数。")
            continue
        settings["check_interval_hours"] = int(raw_interval)
        break

    _save_app_settings(settings)
    print("[*] 设置已保存到 app_settings.json")


def _run_generate_flow():
    print("\n[📱 手机号字典生成向导]")
    segment_rows = _load_segments_db()
    total = None
    selected_isps = None
    selected_cities = None
    generation_mode = "random"
    step = 0

    while True:
        if step == 0:
            generation_mode = _prompt_generation_mode()
            if generation_mode is None:
                print("[*] 已返回主菜单。")
                return
            step = 1
            continue

        if step == 1:
            total = _prompt_total_count()
            if total is None:
                print("[*] 已返回主菜单。")
                return
            step = 2
            continue

        if step == 2:
            selected_isps = _select_isp_names(segment_rows)
            if selected_isps is None:
                step = 1
                continue
            step = 3
            continue

        if step == 3:
            selected_cities = _select_city_names(segment_rows)
            if selected_cities is None:
                step = 2
                continue
            step = 4
            continue

        preview_cities = ",".join(selected_cities[:8])
        if len(selected_cities) > 8:
            preview_cities += f"...(共{len(selected_cities)}个城市)"

        print("\n[确认信息]")
        print(f"- 模式: {generation_mode}")
        print(f"- 数量: {total}")
        print(f"- 运营商: {','.join(selected_isps)}")
        print(f"- 城市: {preview_cities}")
        confirm = input("[+] 确认开始生成？(y确认，b返回上一步，其他取消): ").strip().lower()
        if confirm in {"b", "back"}:
            step = 3
            continue
        if confirm not in {"y", "yes", "是", "确认", "1"}:
            print("[*] 已取消本次生成。")
            return

        make_dict(total, selected_cities, selected_isps, generation_mode)
        _save_last_config(total, selected_isps, selected_cities, generation_mode)
        print("[*] 已保存本次配置，可在首页使用“上次配置重新生成”。")
        return


def _run_last_config_flow():
    last = _load_last_config()
    if not last:
        print("[!] 没有可用的上次配置，请先执行一次正常生成。")
        return

    print("\n[⚡ 上次配置快速生成]")
    preview_cities = ",".join(last["city_names"][:8])
    if len(last["city_names"]) > 8:
        preview_cities += f"...(共{len(last['city_names'])}个城市)"
    print(f"- 上次数量: {last['total']}")
    print(f"- 上次模式: {last['generation_mode']}")
    print(f"- 上次运营商: {','.join(last['isp_names'])}")
    print(f"- 上次城市: {preview_cities}")

    raw_total = input("[+] 输入新的数量（回车沿用上次）: ").strip()
    if raw_total:
        if not raw_total.isdigit() or int(raw_total) < 1:
            print("[!] 数量无效，已取消。")
            return
        total = int(raw_total)
    else:
        total = last["total"]

    if not _confirm_yes("[+] 确认按以上配置生成？(y确认，其他取消): "):
        print("[*] 已取消本次生成。")
        return

    make_dict(total, last["city_names"], last["isp_names"], last["generation_mode"])
    _save_last_config(total, last["isp_names"], last["city_names"], last["generation_mode"])
    print("[*] 已更新并保存本次配置。")


def run_interactive_wizard():
    _try_auto_update_and_rebuild()
    while True:
        _clear_screen()
        _print_wizard_banner()
        _print_last_config_summary()
        choice = _prompt_menu_choice()

        try:
            if choice == "1":
                _run_generate_flow()
                _pause()
            elif choice == "2":
                _show_db_stats()
                _pause()
            elif choice == "3":
                _rebuild_segments_csv_from_dat()
                print("[*] 已完成重建。")
                _pause()
            elif choice == "4":
                _run_last_config_flow()
                _pause()
            elif choice == "5":
                _run_query_flow()
                _pause()
            elif choice == "6":
                _run_settings_flow()
                _pause()
            elif choice == "0":
                print("[*] 已退出。")
                break
            else:
                print("[!] 无效菜单编号。")
                _pause()
        except Exception as e:
            print(f"[!] 执行失败: {e}")
            _pause()


if __name__ == '__main__':
    try:
        run_interactive_wizard()
    except UserAbort:
        print("\n[*] 输入已中断，程序退出。")
