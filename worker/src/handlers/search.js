import { JsonResponse } from '../utils/jsonResponse.js';

/**
 * @description 根据 URL 参数动态构建 SQL 查询语句。
 * @param {URLSearchParams} params - 输入：请求的 URL 搜索参数。
 * @param {boolean} isCountQuery - 输入：如果为 true，则构建 COUNT 查询用于分页。
 * @param {object} options - 输入：包含 limit, offset 和 reverse 标志的选项对象。
 * @returns {{sql: string, params: any[]}} - 输出：包含 SQL 字符串及其绑定参数的对象。
 */
function buildSearchQuery(params, isCountQuery = false, options = {}) {
  const { limit = 10, offset = 0, reverse = false } = options;
  const joins = new Set();
  const conditions = [];
  const bindings = [];

  const baseSelect = isCountQuery ?
    "SELECT COUNT(DISTINCT D.deck_id) FROM Decks AS D" :
    "SELECT DISTINCT D.* FROM Decks AS D";

  /**
   * @description 通用函数，用于处理接收多个值的过滤参数 (例如 ?race=战士&race=魔法师)
   * @param {string} paramName - URL参数名 (例如 'race')
   * @param {(value: string, alias: string) => void} joinLogic - 一个函数，定义了如何为该参数添加 JOIN 和 WHERE 条件
   */
  const processMultiParamFilter = (paramName, joinLogic) => {
    const values = params.getAll(paramName);
    values.forEach((value, i) => {
        // 为每个参数创建唯一的别名，防止SQL冲突
        const alias = `${paramName.replace(/[^a-zA-Z]/g, '')}${i}`;
        joinLogic(value, alias);
    });
  };
  
  // 卡组名模糊搜索
  if (params.has('deck_name')) {
    conditions.push("D.deck_name LIKE ?");
    bindings.push(`%${params.get('deck_name')}%`);
  }

  // 卡片名和描述的模糊搜索 (支持多语言)
  const cardQueries = params.getAll('card');
  if (cardQueries.length > 0) {
    const langs = (params.get('lang') || 'cn').split(',').map(l => l.trim()).filter(l => ['cn', 'en', 'jp'].includes(l));
    if (langs.length > 0) {
        cardQueries.forEach((query, i) => {
            const alias = `card${i}`;
            joins.add(`JOIN DeckCards AS DC_${alias} ON D.deck_id = DC_${alias}.deck_id`);
            joins.add(`JOIN Cards AS C_${alias} ON DC_${alias}.card_id = C_${alias}.id`);
            
            const langConditions = langs.map(lang => `C_${alias}.${lang}_name LIKE ?`);
            // FIX: 将 'desc' 修正为数据库 schema 中定义的 'card_text_desc'
            const allConditions = [...langConditions, `C_${alias}.card_text_desc LIKE ?`].join(' OR ');

            conditions.push(`(${allConditions})`);
            langs.forEach(() => bindings.push(`%${query}%`));
            bindings.push(`%${query}%`);
        });
    }
  }

  // --- 新增的过滤器逻辑 ---

  // 种族 (race) 过滤器
  processMultiParamFilter('race', (raceName, alias) => {
    joins.add(`JOIN DeckCards AS DC_${alias} ON D.deck_id = DC_${alias}.deck_id`);
    joins.add(`JOIN CardToRace AS CTR_${alias} ON DC_${alias}.card_id = CTR_${alias}.card_id`);
    joins.add(`JOIN Races AS R_${alias} ON CTR_${alias}.race_code = R_${alias}.race_code`);
    conditions.push(`R_${alias}.race_name LIKE ?`);
    bindings.push(`%${raceName}%`);
  });

  // 属性 (attribute) 过滤器
  processMultiParamFilter('attribute', (attributeName, alias) => {
    joins.add(`JOIN DeckCards AS DC_${alias} ON D.deck_id = DC_${alias}.deck_id`);
    joins.add(`JOIN CardToAttribute AS CTA_${alias} ON DC_${alias}.card_id = CTA_${alias}.card_id`);
    joins.add(`JOIN Attributes AS A_${alias} ON CTA_${alias}.attribute_code = A_${alias}.attribute_code`);
    conditions.push(`A_${alias}.attribute_name LIKE ?`);
    bindings.push(`%${attributeName}%`);
  });

  // 卡片种类 (type) 过滤器
  processMultiParamFilter('type', (typeName, alias) => {
    joins.add(`JOIN DeckCards AS DC_${alias} ON D.deck_id = DC_${alias}.deck_id`);
    joins.add(`JOIN CardToType AS CTT_${alias} ON DC_${alias}.card_id = CTT_${alias}.card_id`);
    joins.add(`JOIN CardTypes AS CT_${alias} ON CTT_${alias}.type_code = CT_${alias}.type_code`);
    conditions.push(`CT_${alias}.type_name LIKE ?`);
    bindings.push(`%${typeName}%`);
  });
  
  // 系列/字段 (setcode) 过滤器 (此部分逻辑正确，无需修改)
  processMultiParamFilter('setcode', (setcode, alias) => {
    joins.add(`JOIN DeckCards AS DC_${alias} ON D.deck_id = DC_${alias}.deck_id`);
    joins.add(`JOIN CardToSetcode AS CTS_${alias} ON DC_${alias}.card_id = CTS_${alias}.card_id`);
    joins.add(`JOIN Setcodes AS S_${alias} ON CTS_${alias}.set_code = S_${alias}.set_code`);
    conditions.push(`S_${alias}.set_name_cn LIKE ?`);
    bindings.push(`%${setcode}%`);
  });

  // 日期和点赞数范围过滤器 (此部分逻辑正确，无需修改)
  if (params.has('likes_ge')) { conditions.push("D.deck_like >= ?"); bindings.push(parseInt(params.get('likes_ge'), 10)); }
  if (params.has('likes_le')) { conditions.push("D.deck_like <= ?"); bindings.push(parseInt(params.get('likes_le'), 10)); }
  // 注意：你的Python脚本存的是unix timestamp（整数），这里用Date.parse()是正确的
  if (params.has('after_date')) { conditions.push("D.upload_date >= ?"); bindings.push(Math.floor(Date.parse(params.get('after_date')) / 1000)); }
  if (params.has('before_date')) { conditions.push("D.upload_date <= ?"); bindings.push(Math.floor(Date.parse(params.get('before_date')) / 1000)); }

  // 组合查询语句
  let sql = baseSelect;
  if (joins.size > 0) sql += " " + [...joins].join(" ");
  if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");

  // 排序和分页 (仅对数据查询生效)
  if (!isCountQuery) {
    const orderDirection = reverse ? 'ASC' : 'DESC';
    const sortBy = params.get('order') || 'rate'; // 默认为 'rate'
    
    let orderByClause;
    if (sortBy === 'date') {
        // 主排序：日期；次排序：点赞
        orderByClause = ` ORDER BY D.update_date ${orderDirection}, D.deck_like ${orderDirection}`;
    } else { // 默认为 'rate'
        // 主排序：点赞；次排序：日期
        orderByClause = ` ORDER BY D.deck_like ${orderDirection}, D.update_date ${orderDirection}`;
    }
    sql += orderByClause;

    sql += " LIMIT ? OFFSET ?";
    bindings.push(limit, offset);
  }

  return { sql, params: bindings };
}

/**
 * @description 处理卡组搜索请求，负责解析分页参数并执行查询。
 * @param {Request} request - 输入：传入的请求对象。
 * @param {object} env - 输入：环境变量对象，包含数据库绑定。
 * @returns {Promise<Response>} - 输出：包含搜索结果或错误的 Response 对象。
 */
export default async function handleSearchRequest(request, env) {
  const { searchParams } = new URL(request.url);

  // --- 分页逻辑处理 ---
  let limit = 10;
  let offset = 0;
  const startParam = searchParams.get('start');
  const endParam = searchParams.get('end');
  const sizeParam = searchParams.get('size');

  if (startParam !== null) {
      const start = parseInt(startParam, 10);
      if (isNaN(start) || start < 0) {
          return JsonResponse({ success: false, error: "无效的 'start' 参数，必须为非负整数。" }, 400);
      }
      offset = start;

      if (endParam !== null) {
          const end = parseInt(endParam, 10);
          if (isNaN(end) || end <= start) {
              return JsonResponse({ success: false, error: "无效的 'end' 参数，必须是大于 'start' 的整数。" }, 400);
          }
          limit = end - start;
      } else if (sizeParam !== null) {
          const size = parseInt(sizeParam, 10);
          if (isNaN(size) || size <= 0) {
              return JsonResponse({ success: false, error: "无效的 'size' 参数，必须为正整数。" }, 400);
          }
          limit = size;
      }
  }

  // --- 构建并执行查询 ---
  const reverse = searchParams.get('reverse') === 'true';
  const queryOptions = { limit, offset, reverse };

  const query = buildSearchQuery(searchParams, false, queryOptions);
  const countQuery = buildSearchQuery(searchParams, true);

  try {
    const [dataResult, countResult] = await env.DECK_DB.batch([
      env.DECK_DB.prepare(query.sql).bind(...query.params),
      env.DECK_DB.prepare(countQuery.sql).bind(...countQuery.params),
    ]);

    if (!dataResult.success || !countResult.success) {
      console.error("数据库批量查询失败。", { 
          dataError: dataResult.error,
          countError: countResult.error 
      });
      return JsonResponse({ success: false, error: "数据库查询失败。" }, 500);
    }

    const total = countResult.results[0]['COUNT(DISTINCT D.deck_id)'] || 0;

    return JsonResponse({
      success: true,
      data: {
        total,
        start: offset,
        size: dataResult.results?.length || 0, // 实际返回的数量
        list: dataResult.results || [],
      }
    });

  } catch(e) {
    console.error("执行查询时发生异常: ", e, { query: query.sql, params: query.params });
    return JsonResponse({ success: false, error: `服务器内部错误: ${e.message}`}, 500);
  }
}
