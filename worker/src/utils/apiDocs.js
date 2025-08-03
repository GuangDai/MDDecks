import { JsonResponse as BaseJsonResponse } from './jsonResponse.js';
export const JsonResponse = BaseJsonResponse; // 重新导出以方便使用

/**
 * @description 提供一个包含 API 文档的结构化对象。
 * @returns {object} - 输出：一个包含帮助文本和可用路由信息的对象。
 */
export function getApiDocs() {
  return{
      "success": true,
      "message": "API 端点文档 (v2 - 支持独立筛选)",
      "available_endpoints": {
        "GET /api/decks/search": {
          "description": "根据多种条件搜索、筛选和排序卡组。支持多种参数组合，包括单独按点赞数或更新日期筛选。",
          "query_parameters": {
            "deck_name": {
              "description": "按卡组标题搜索。优先使用索引精确匹配，若无则回退为模糊搜索。",
              "example": "`?deck_name=珠泪哀歌`"
            },
            "card": {
              "description": "搜索包含指定卡片（基于卡名、系列等关键字）的卡组。可多次使用以实现 AND 逻辑（即查询同时包含所有指定卡片的卡组）。",
              "example_single": "`?card=灰流丽` (查找包含“灰流丽”的卡组)",
              "example_multiple": "`?card=灰流丽&card=增殖的G` (查找同时包含“灰流丽”和“增殖的G”的卡组)"
            },
            "setcode": {
              "description": "按系列/字段名（关键字）搜索。效果与 `card` 类似，但更侧重于字段。",
              "example": "`?setcode=HERO` (查找包含“HERO”字段相关卡片的卡组)"
            },
            "card_name": {
              "description": "根据卡片全名精确搜索。用于查找包含某一张特定卡片的卡组。",
              "example": "`?card_name=阿不思的落胤`"
            },
            "type": {
              "description": "按卡片类型精确过滤。此参数必须与一个【内容搜索】参数（如 `card`, `deck_name`）一同使用。",
              "example": "`?card=救援&type=连接怪兽` (查找包含“救援”字段且含有连接怪兽的卡组)"
            },
            "race": {
              "description": "按卡片种族精确过滤。此参数必须与一个【内容搜索】参数一同使用。",
              "example": "`?deck_name=龙&race=龙族` (查找标题含“龙”且包含龙族怪兽的卡组)"
            },
            "attribute": {
              "description": "按卡片属性精确过滤。此参数必须与一个【内容搜索】参数一同使用。",
              "example": "`?card=电子&attribute=光` (查找包含“电子”字段且含有光属性怪兽的卡组)"
            },
            "likes_ge": {
              "description": "筛选点赞数大于或等于(>=) N 的卡组。可以独立使用。",
              "example": "`?likes_ge=100` (查找所有点赞数超过100的卡组)"
            },
            "likes_le": {
              "description": "筛选点赞数小于或等于(<=) N 的卡组。可以独立使用。",
              "example": "`?likes_le=5` (查找所有点赞数不超过5的卡组)"
            },
            "after_date": {
              "description": "筛选在此日期之后（含）更新的卡组 (格式: YYYY-MM-DD)。可以独立使用。",
              "example": "`?after_date=2024-01-01` (查找所有2024年及以后更新的卡组)"
            },
            "before_date": {
              "description": "筛选在此日期之前（含）更新的卡组 (格式: YYYY-MM-DD)。可以独立使用。",
              "example": "`?before_date=2023-12-31` (查找所有2023年底及以前更新的卡组)"
            },
            "order": {
              "description": "排序依据。可选值为 `rate` (按点赞数，默认) 或 `date` (按更新日期)。",
              "example": "`?order=date` (结果将按更新日期从新到旧排序)"
            },
            "reverse": {
              "description": "反转排序。默认为从高到低（点赞最多/日期最新），设为`true`后变为从低到高。",
              "example": "`?order=rate&reverse=true` (结果将按点赞数从少到多排序)"
            },
            "start": {
              "description": "分页起始位置，从 0 开始的索引。默认为 `0`。",
              "example": "`?start=50` (跳过前50个结果)"
            },
            "size": {
              "description": "每页返回的结果数量。默认为 `50`。",
              "example": "`?size=20` (每页返回20个结果)"
            }
          },
          "usage_examples": [
            {
              "title": "查看最新发布的卡组",
              "request": "GET /api/decks/search?order=date"
            },
            {
              "title": "查看点赞数最高的卡组（默认行为）",
              "request": "GET /api/decks/search?order=rate"
            },
            {
              "title": "查找同时包含“白银城”和“无限泡影”且点赞超过50的卡组",
              "request": "GET /api/decks/search?card=白银城&card=无限泡影&likes_ge=50"
            },
            {
              "title": "查找2023年之后更新的“焰圣骑士”卡组，并按点赞数排序",
              "request": "GET /api/decks/search?setcode=焰圣骑士&after_date=2023-12-31&order=rate"
            },
            {
              "title": "查找点赞数在10到100之间的所有卡组",
              "request": "GET /api/decks/search?likes_ge=10&likes_le=100"
            },
            {
              "title": "搜索包含“闪刀姬”字段的卡组，结果第二页（每页25个）",
              "request": "GET /api/decks/search?card=闪刀姬&start=25&size=25"
            },
            {
              "title": "查找包含“俱舍怒威族”且含有“念动力族”怪兽的卡组",
              "request": "GET /api/decks/search?card=俱舍怒威族&race=念动力族"
            }
          ],
          "notes": [
            "【内容搜索】参数包括: `deck_name`, `card`, `setcode`, `card_name`。",
            "【卡片属性】参数包括: `type`, `race`, `attribute`。这些参数必须与至少一个【内容搜索】参数一起使用。",
            "【精确筛选】参数包括: `likes_ge`, `likes_le`, `after_date`, `before_date`。这些参数现在可以独立使用，也可以与任何其他参数组合使用。",
            "所有文本搜索（如 `card`, `deck_name`）均不区分大小写。"
          ]
        },
        "POST /api/decks/create": {
          "description": "创建新卡组 (尚未实现)"
        },
        "PUT /api/decks/update": {
          "description": "更新现有卡组 (尚未实现)"
        },
        "DELETE /api/decks/delete": {
          "description": "删除卡组 (尚未实现)"
        },
        "POST /api/decks/rate": {
          "description": "为卡组点赞 (尚未实现)"
        }
      
    },
  };
}
