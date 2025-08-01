import requests
import json
import time
import os

# --- 配置区 ---

# 保存结果的目录，每个卡组的JSON文件都会存放在这里
OUTPUT_DIR = os.path.join(
    os.path.dirname(__file__), "deck_data"
)

# 每次请求之间的延时（秒），用于控制爬虫速率
RATE_LIMIT_DELAY = 1

# --- 请求头信息 ---
HEADERS_API = {
    "accept": "*/*",
    "accept-language": "zh,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7",
    "clientsource": "Web",
    "dnt": "1",
    "origin": "https://neos.moecube.com",
    "referer": "https://neos.moecube.com/",
    "reqsource": "MDPro3",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Brave";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "sec-gpc": "1",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
}


def fetch_all_deck_ids(session):
    """
    获取所有分页中的所有卡组ID，不再使用关键词。
    """
    deck_ids = []
    page = 1
    total_pages = 1

    print("开始获取所有公开卡组的ID列表...")

    while page <= total_pages:
        # URL中去掉了 keyWord 参数，以获取所有卡组
        list_url = f"https://zgai.tech:38443/api/mdpro3/deck/list?page={page}&size=30&sortLike=true"

        try:
            print(f"正在请求第 {page}/{total_pages if total_pages > 1 else '?'} 页...")
            response = session.get(list_url, headers=HEADERS_API, timeout=15)
            time.sleep(RATE_LIMIT_DELAY)  # 控制请求速率

            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0 and "data" in data:
                    # 首次或每次都更新总页数
                    total_pages = data["data"].get("pages", 1)
                    records = data["data"].get("records", [])

                    if not records and page == 1:
                        print("未找到任何卡组记录。")
                        break

                    for record in records:
                        deck_ids.append(record["deckId"])

                    print(f"成功获取 {len(records)} 条记录。")
                    page += 1
                else:
                    print(f"API返回错误: {data.get('message', '未知错误')}")
                    break
            else:
                print(f"请求失败，状态码: {response.status_code}")
                break

        except requests.exceptions.RequestException as e:
            print(f"网络请求时发生错误: {e}")
            break

    return list(set(deck_ids))  # 去重，确保ID列表唯一


def fetch_deck_details(session, deck_id):
    """
    获取单个卡组的详细信息并处理数据。
    """
    detail_url = f"https://zgai.tech:38443/api/mdpro3/deck/{deck_id}"

    try:
        response = session.get(detail_url, headers=HEADERS_API, timeout=15)
        time.sleep(RATE_LIMIT_DELAY)  # 控制请求速率

        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 0 and "data" in data:
                deck_data = data["data"]

                # 数据清洗：处理 deckYdk 字段中的转义字符
                if "deckYdk" in deck_data and isinstance(deck_data["deckYdk"], str):
                    deck_data["deckYdk"] = deck_data["deckYdk"].replace("\\r\\n", "\n").replace("\\n","\n")

                return deck_data
            else:
                print(
                    f"获取卡组 {deck_id} 详情时API返回错误: {data.get('message', '未知错误')}"
                )
                return None
        else:
            print(f"获取卡组 {deck_id} 详情失败，状态码: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"获取卡组 {deck_id} 详情时网络请求发生错误: {e}")
        return None


def main():
    """
    主执行函数
    """
    # 确保输出目录存在
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"已创建目录: {OUTPUT_DIR}")

    with requests.Session() as session:
        # 步骤 1: 获取所有需要处理的卡组ID
        deck_ids = fetch_all_deck_ids(session)

        if not deck_ids:
            print("未能获取任何卡组ID，程序退出。")
            return

        total_ids = len(deck_ids)
        print(f"\n总共获取到 {total_ids} 个不重复的卡组ID。")
        print("开始处理卡组数据，将自动跳过已下载的文件...\n")

        # 步骤 2: 遍历ID，获取、清洗并保存，同时实现断点续传
        success_count = 0
        skipped_count = 0

        for i, deck_id in enumerate(deck_ids):
            # 构造预期的输出文件名
            output_filename = os.path.join(OUTPUT_DIR, f"{deck_id}.json")

            # --- 断点续传核心逻辑 ---
            # 检查文件是否已经存在
            if os.path.exists(output_filename):
                # 如果文件存在，打印信息并跳过
                print(f"({i + 1}/{total_ids}) 文件已存在，跳过: {deck_id}.json")
                skipped_count += 1
                continue

            # 如果文件不存在，则执行爬取
            print(f"--- ({i + 1}/{total_ids}) 正在处理新卡组: {deck_id} ---")
            details = fetch_deck_details(session, deck_id)

            if details:
                # 将单个卡组数据保存到以其ID命名的JSON文件
                try:
                    with open(output_filename, "w", encoding="utf-8") as f:
                        json.dump(details, f, ensure_ascii=False, indent=4)
                    print(
                        f"成功保存卡组 '{details.get('deckName', '未知名称')}' -> {output_filename}"
                    )
                    success_count += 1
                except IOError as e:
                    print(f"保存文件 {output_filename} 时发生错误: {e}")
            else:
                # fetch_deck_details 函数内部已打印失败原因
                print(f"未能获取卡组 {deck_id} 的详细数据，本次跳过。")

        print("\n" + "=" * 20 + " 任务完成 " + "=" * 20)
        print(f"总目标卡组数: {total_ids}")
        print(f"本次新保存文件数: {success_count}")
        print(f"已存在而跳过的文件数: {skipped_count}")
        print(f"文件保存在目录: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()
