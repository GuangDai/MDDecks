# -*- coding: utf-8 -*-
import sqlite3
import os
import argparse
from datetime import datetime

# --- 请修改为您的数据库文件路径 ---
DB_FILE = '/home/hp/Projects/OpenSource/MDDecks/yugioh_decks.db'


def build_dynamic_query(args):
    """
    根据用户输入的参数动态构建SQL查询语句。
    """
    joins, conditions, params = set(), [], []
    base_query = "SELECT DISTINCT D.* FROM Decks AS D"

    # --- 卡组名模糊搜索 ---
    if args.deck_name:
        conditions.append("D.deck_name LIKE ?")
        params.append(f"%{args.deck_name}%")

    # --- 卡片名模糊搜索 ---
    if args.cn_name:
        for i, name_part in enumerate(args.cn_name):
            alias_dc, alias_c = f"DC_cn{i}", f"C_cn{i}"
            joins.add(f"JOIN DeckCards AS {alias_dc} ON D.deck_id = {alias_dc}.deck_id")
            joins.add(f"JOIN Cards AS {alias_c} ON {alias_dc}.card_id = {alias_c}.id")
            conditions.append(f"{alias_c}.cn_name LIKE ?")
            params.append(f"%{name_part}%")

    if args.en_name:
        for i, name_part in enumerate(args.en_name):
            alias_dc, alias_c = f"DC_en{i}", f"C_en{i}"
            joins.add(f"JOIN DeckCards AS {alias_dc} ON D.deck_id = {alias_dc}.deck_id")
            joins.add(f"JOIN Cards AS {alias_c} ON {alias_dc}.card_id = {alias_c}.id")
            conditions.append(f"{alias_c}.en_name LIKE ?")
            params.append(f"%{name_part}%")

    if args.jp_name:
        for i, name_part in enumerate(args.jp_name):
            alias_dc, alias_c = f"DC_jp{i}", f"C_jp{i}"
            joins.add(f"JOIN DeckCards AS {alias_dc} ON D.deck_id = {alias_dc}.deck_id")
            joins.add(f"JOIN Cards AS {alias_c} ON {alias_dc}.card_id = {alias_c}.id")
            conditions.append(f"{alias_c}.jp_name LIKE ?")
            params.append(f"%{name_part}%")

    # --- 分类和范围过滤器 ---
    if args.type:
        for i, type_name in enumerate(args.type):
            alias_dc, alias_ctt, alias_ct = f"DC_Type{i}", f"CTT{i}", f"CT{i}"
            joins.add(f"JOIN DeckCards AS {alias_dc} ON D.deck_id = {alias_dc}.deck_id")
            joins.add(f"JOIN CardToType AS {alias_ctt} ON {alias_dc}.card_id = {alias_ctt}.card_id")
            joins.add(f"JOIN CardTypes AS {alias_ct} ON {alias_ctt}.type_code = {alias_ct}.type_code")
            conditions.append(f"{alias_ct}.type_name = ?")
            params.append(type_name)
    if args.race:
        for i, race_name in enumerate(args.race):
            alias_dc, alias_ctr, alias_r = f"DC_Race{i}", f"CTR{i}", f"R{i}"
            joins.add(f"JOIN DeckCards AS {alias_dc} ON D.deck_id = {alias_dc}.deck_id")
            joins.add(f"JOIN CardToRace AS {alias_ctr} ON {alias_dc}.card_id = {alias_ctr}.card_id")
            joins.add(f"JOIN Races AS {alias_r} ON {alias_ctr}.race_code = {alias_r}.race_code")
            conditions.append(f"{alias_r}.race_name = ?")
            params.append(race_name)
    if args.attribute:
        for i, attr_name in enumerate(args.attribute):
            alias_dc, alias_cta, alias_a = f"DC_Attr{i}", f"CTA{i}", f"A{i}"
            joins.add(f"JOIN DeckCards AS {alias_dc} ON D.deck_id = {alias_dc}.deck_id")
            joins.add(f"JOIN CardToAttribute AS {alias_cta} ON {alias_dc}.card_id = {alias_cta}.card_id")
            joins.add(f"JOIN Attributes AS {alias_a} ON {alias_cta}.attribute_code = {alias_a}.attribute_code")
            conditions.append(f"{alias_a}.attribute_name = ?")
            params.append(attr_name)

    # ===================================================================
    #
    #   【新增功能】按 Setcode (系列/字段) 查询
    #
    # ===================================================================
    if args.setcode:
        for i, setcode_name in enumerate(args.setcode):
            alias_dc, alias_cts, alias_s = f"DC_Setcode{i}", f"CTS{i}", f"S{i}"
            joins.add(f"JOIN DeckCards AS {alias_dc} ON D.deck_id = {alias_dc}.deck_id")
            joins.add(f"JOIN CardToSetcode AS {alias_cts} ON {alias_dc}.card_id = {alias_cts}.card_id")
            joins.add(f"JOIN Setcodes AS {alias_s} ON {alias_cts}.set_code = {alias_s}.set_code")
            # 使用 LIKE 进行模糊匹配，以处理 "文具电子人|非「电子」" 这类情况
            conditions.append(f"{alias_s}.set_name_cn LIKE ?")
            params.append(f"%{setcode_name}%")


    if args.likes_ge is not None:
        conditions.append("D.deck_like >= ?")
        params.append(args.likes_ge)
    if args.likes_le is not None:
        conditions.append("D.deck_like <= ?")
        params.append(args.likes_le)
    if args.after_date:
        ts = int(datetime.strptime(args.after_date, "%Y-%m-%d").timestamp() * 1000)
        conditions.append("D.upload_date >= ?")
        params.append(ts)
    if args.before_date:
        ts = int(datetime.strptime(args.before_date, "%Y-%m-%d").timestamp() * 1000)
        conditions.append("D.upload_date <= ?")
        params.append(ts)

    # --- 组合最终的查询语句 ---
    full_query = base_query
    if joins:
        full_query += " " + " ".join(sorted(list(joins)))
    if conditions:
        full_query += " WHERE " + " AND ".join(conditions)

    # 排序和数量限制
    if args.sort_by == 'likes':
        full_query += " ORDER BY D.deck_like DESC"
    elif args.sort_by == 'date':
        full_query += " ORDER BY D.update_date DESC"
    full_query += " LIMIT ?"
    params.append(args.limit)

    return full_query, params


def execute_query(sql, params):
    if not os.path.exists(DB_FILE):
        print(f"错误: 数据库文件 '{DB_FILE}' 不存在。")
        return None
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"数据库查询时发生错误: {e}")
        return None
    finally:
        if conn:
            conn.close()


def print_results(decks, args):
    if decks is None:
        return
    if not decks:
        print("未找到满足所有条件的卡组。")
        return

    print(f"--- 找到 {len(decks)} 个匹配的卡组 (按 {args.sort_by} 排序) ---")

    for i, deck in enumerate(decks):
        print("\n" + "=" * 60)
        update_date_str = "未知日期"
        if deck['update_date']:
            try:
                update_date_str = datetime.fromtimestamp(deck['update_date'] / 1000).strftime('%Y-%m-%d %H:%M')
            except (TypeError, ValueError):
                pass

        print(f"#{i + 1:02d} | 卡组名: {deck['deck_name']}")
        print(f"    | 点赞: {deck['deck_like']:<5} | 最后更新: {update_date_str}")
        print(f"    | Deck ID: {deck['deck_id']}")

        if deck['deck_ydk']:
            print("--- 卡组代码 (YDK) ---")
            print(deck['deck_ydk'].strip())
            print("----------------------")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="游戏王卡组高级查询工具 (模糊匹配版)。\n可以查询同时包含多种卡片或多个系列的卡组。",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--deck-name', type=str, help='模糊搜索卡组名 (例如: "珠泪")。')
    parser.add_argument('--cn-name', nargs='+', help='按中文名模糊查找一张或多张卡片 (并列查询, 例如: --cn-name 青眼 混沌帝龙)。')
    parser.add_argument('--en-name', nargs='+', help='按英文名模糊查找一张或多张卡片 (例如: --en-name "Blue-Eyes" "Stardust Dragon")。')
    parser.add_argument('--jp-name', nargs='+', help='按日文名模糊查找一张或多张卡片 (例如: --jp-name ブルーアイズ)。')

    # --- 新增参数 ---
    parser.add_argument('--setcode', nargs='+', help='筛选包含指定系列(archetype)卡片的卡组 (例如: --setcode 青眼 幻影骑士团)。')

    parser.add_argument('--type', nargs='+', help='筛选包含指定类型卡片的卡组 (例如: --type 融合 效果)。')
    parser.add_argument('--race', nargs='+', help='筛选包含指定种族卡片的卡组 (例如: --race 龙族 魔法师族)。')
    parser.add_argument('--attribute', nargs='+', help='筛选包含指定属性卡片的卡组 (例如: --attribute 光 暗)。')
    parser.add_argument('--likes-ge', type=int, help='筛选点赞数大于或等于 N 的卡组。')
    parser.add_argument('--likes-le', type=int, help='筛选点赞数小于或等于 N 的卡组。')
    parser.add_argument('--after-date', type=str, help='筛选指定日期后上传的卡组 (格式: YYYY-MM-DD)。')
    parser.add_argument('--before-date', type=str, help='筛选指定日期前上传的卡组 (格式: YYYY-MM-DD)。')
    parser.add_argument('--sort-by', choices=['likes', 'date'], default='likes', help='排序方式 (默认: likes)。')
    parser.add_argument('-n', '--limit', type=int, default=10, help='返回结果的数量 (默认: 10)。')
    args = parser.parse_args()

    # 检查是否提供了任何查询参数
    arg_dict = {k: v for k, v in vars(args).items() if k not in ['limit', 'sort_by']}
    if not any(v for v in arg_dict.values() if v is not None):
        parser.print_help()
    else:
        sql_query, params = build_dynamic_query(args)
        print("\n[SQL] 正在执行查询...")
        # print(f"    > Query: {sql_query}")  # 取消此行注释可打印完整SQL语句
        # print(f"    > Params: {params}")   # 取消此行注释可打印查询参数
        results = execute_query(sql_query, params)
        print_results(results, args)