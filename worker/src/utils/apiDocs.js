import { JsonResponse as BaseJsonResponse } from './jsonResponse.js';
export const JsonResponse = BaseJsonResponse; // 重新导出以方便使用

/**
 * @description 提供一个包含 API 文档的结构化对象。
 * @returns {object} - 输出：一个包含帮助文本和可用路由信息的对象。
 */
export function getApiDocs() {
  return {
    message: "请求的端点不存在。请检查以下可用的端点和参数。",
    available_endpoints: {
      "GET /api/decks/search": {
        description: "根据多种条件搜索卡组。可以按点赞数或日期排序。",
        query_parameters: {
          "deck_name": "按卡组标题模糊搜索。示例: `?deck_name=珠泪`",
          "card": "搜索包含此名称或描述的卡片的卡组（模糊匹配）。需要 `lang` 参数。可多次使用以实现 AND 逻辑。示例: `?card=灰流丽&card=增殖的G`",
          "lang": "为 `card` 参数指定搜索语言。可以是逗号分隔的列表 (`cn`, `en`, `jp`)。默认为 `cn`。示例: `?lang=en,jp`",
          "order": "排序依据。可选值为 `rate` (按点赞数) 或 `date` (按更新日期)。默认为 `rate`。示例: `?order=date`",
          "setcode": "按系列/字段名模糊搜索。示例: `?setcode=HERO`",
          "type": "按卡片类型精确过滤。示例: `?type=魔法卡`",
          "race": "按卡片种族精确过滤。示例: `?race=龙族`",
          "attribute": "按卡片属性精确过滤。示例: `?attribute=光`",
          "likes_ge": "筛选点赞数大于或等于 N 的卡组。示例: `?likes_ge=10`",
          "likes_le": "筛选点赞数小于或等于 N 的卡组。示例: `?likes_le=50`",
          "after_date": "筛选在此日期之后（含）上传的卡组 (格式: YYYY-MM-DD)。示例: `?after_date=2023-01-01`",
          "before_date": "筛选在此日期之前（含）上传的卡组 (格式: YYYY-MM-DD)。示例: `?before_date=2023-12-31`",
          "reverse": "反转默认排序（从点赞最多变为最少）。示例: `?reverse=true`",
        },
        pagination_parameters: {
            "方案一": "使用 `start` 和 `size`。`start` 是从 0 开始的索引。`size` 是返回的结果数量 (必须 > 0)。示例: `?start=0&size=20`",
            "方案二": "使用 `start` 和 `end`。返回从 `start` 到 `end`（不含）的结果。`end` 必须大于 `start`。示例: `?start=10&end=30`",
            "默认值": "如果没有提供分页参数，默认为 `start=0` 和 `size=10`。"
        }
      },
      "POST /api/decks/create": "创建新卡组 (尚未实现)",
      "PUT /api/decks/update": "更新现有卡组 (尚未实现)",
      "DELETE /api/decks/delete": "删除卡组 (尚未实现)",
      "POST /api/decks/rate": "为卡组点赞 (尚未实现)",
    },
  };
}
