import { JsonResponse } from '../utils/jsonResponse.js';
import { updateGitHubFile, jsonArrayMerger } from '../utils/githubUpdater.js';

// --- 应用配置 ---
const ALLOW_UNINDEXED_DECK_NAME_SEARCH = false;
const SQLITE_PARAM_LIMIT = 90;

// --- 过滤器类型定义 ---
const PRIMARY_FILTER_TYPES = new Set([
    'deck_name_indexed', 'deck_name_fallback', 'card', 'setcode', 'card_name', 'attribute', 'race', 'type'
]);
const REFINING_FILTER_TYPES = new Set([
    'likes_ge', 'likes_le', 'after_date', 'before_date'
]);

// =============================================================================
// #region 数据库查询辅助模块
// =============================================================================

/**
 * @description 详细的工作流程：此函数递归地解析一个关键词的指针，直到找到没有指针指向其他关键词的“根”关键词。
 * 工作步骤：
 * 1. 接收一个关键词作为初始输入。
 * 2. 使用一个 Set 来防止在解析过程中出现无限循环。
 * 3. 在一个有安全上限 (10次) 的循环中，查询数据库中当前关键词的 `pointer_to` 字段。
 * 4. 如果找到一个指针，将当前关键词更新为指针指向的新关键词，并继续循环。
 * 5. 如果没有找到指针，说明当前关键词是根关键词，循环终止。
 * 6. 返回最终找到的根关键词。
 * @param {string} keyword - 预计输入：需要解析的初始关键词。预计输入的类型：`string`。
 * @param {D1Database} db - 预计输入：Cloudflare D1 数据库的实例。预计输入的类型：`object`。
 * @returns {Promise<string>} - 预计输出：一个解析为最终根关键词字符串的 Promise。预计输出的类型：`Promise<string>`。
 */
async function resolveKeywordPointer(keyword, db) {
    let currentKeyword = keyword;
    const visited = new Set();
    for (let i = 0; i < 10; i++) {
        if (visited.has(currentKeyword)) throw new Error(`关键词检测到循环指针: ${keyword}`);
        visited.add(currentKeyword);
        const result = await db.prepare("SELECT pointer_to FROM SearchKeywords WHERE keyword = ?").bind(currentKeyword).first("pointer_to");
        if (result) {
            currentKeyword = String(result);
        } else {
            return currentKeyword;
        }
    }
    throw new Error(`关键词指针解析超出深度限制: ${keyword}`);
}

/**
 * @description 详细的工作流程：此函数根据一个卡牌 ID 列表，查询并返回包含这些卡牌的所有卡组的 ID。
 * 工作步骤：
 * 1. 接收一个卡牌 ID 数组。如果数组为空，则直接返回一个空的 Set。
 * 2. 为了避免超出 SQL 参数数量限制，将卡牌 ID 数组分块处理。
 * 3. 对每个分块，执行 `SELECT deck_id FROM DeckCards WHERE card_id IN (...)` 查询。
 * 4. 将所有查询结果中的 `deck_id` 收集到一个 Set 中并去重。
 * 5. 返回包含所有卡组 ID 的 Set。
 * @param {number[]} cardIds - 预计输入：一个包含卡牌 ID 的数组。预计输入的类型：`Array<number>`。
 * @param {D1Database} db - 预计输入：Cloudflare D1 数据库的实例。预计输入的类型：`object`。
 * @returns {Promise<Set<string>>} - 预计输出：一个解析为包含卡组 ID 字符串的 Set 的 Promise。预计输出的类型：`Promise<Set<string>>`。
 */
async function getDeckIdsForCardIds(cardIds, db) {
    if (!cardIds || cardIds.length === 0) return new Set();
    const deckIds = new Set();
    for (let i = 0; i < cardIds.length; i += SQLITE_PARAM_LIMIT) {
        const chunk = cardIds.slice(i, i + SQLITE_PARAM_LIMIT);
        const placeholders = chunk.map(() => '?').join(',');
        const query = `SELECT deck_id FROM DeckCards WHERE card_id IN (${placeholders})`;
        const results = await db.prepare(query).bind(...chunk).all();
        if (results.results) {
            results.results.forEach(r => deckIds.add(String(r.deck_id)));
        }
    }
    return deckIds;
}

/**
 * @description 详细的工作流程：此函数根据卡牌的特定属性（如种族、属性、类型）获取包含这些卡牌的卡组 ID。
 * 工作步骤：
 * 1. 根据传入的 `filterType`（'race', 'attribute', 'type'）选择对应的配置。
 * 2. 查询对应的属性表（如 `Races`），将属性名称（`value`）转换为属性代码。
 * 3. 如果找不到任何属性代码，返回一个空的 Set。
 * 4. 使用找到的属性代码，查询关联表（如 `CardToRace`），获取所有匹配的卡牌 ID。
 * 5. 调用 `getDeckIdsForCardIds` 函数，将这些卡牌 ID 转换为卡组 ID。
 * 6. 返回最终的卡组 ID Set。
 * @param {string} filterType - 预计输入：过滤器类型，必须是 'race', 'attribute', 或 'type'。预计输入的类型：`string`。
 * @param {string} value - 预计输入：要搜索的属性值，例如 "战士"。预计输入的类型：`string`。
 * @param {D1Database} db - 预计输入：Cloudflare D1 数据库的实例。预计输入的类型：`object`。
 * @returns {Promise<Set<string>>} - 预计输出：一个解析为包含卡组 ID 字符串的 Set 的 Promise。预计输出的类型：`Promise<Set<string>>`。
 */
async function getDeckIdsByAttribute(filterType, value, db) {
    const config = {
        race: { table: "Races", linkTable: "CardToRace", nameCol: "race_name", codeCol: "race_code", value: (() => value.endsWith('族') ? [value] : [value, `${value}族`])() },
        attribute: { table: "Attributes", linkTable: "CardToAttribute", nameCol: "attribute_name", codeCol: "attribute_code", value: [value] },
        type: { table: "CardTypes", linkTable: "CardToType", nameCol: "type_name", codeCol: "type_code", value: [value] }
    };
    const c = config[filterType];
    if (!c) return new Set();

    const placeholders = c.value.map(() => '?').join(',');
    const codeResults = await db.prepare(`SELECT ${c.codeCol} FROM ${c.table} WHERE ${c.nameCol} IN (${placeholders})`).bind(...c.value).all();
    const codes = codeResults.results.map(r => Number(r[c.codeCol]));
    if (codes.length === 0) return new Set();

    const codePlaceholders = codes.map(() => '?').join(',');
    const cardIdResults = await db.prepare(`SELECT card_id FROM ${c.linkTable} WHERE ${c.codeCol} IN (${codePlaceholders})`).bind(...codes).all();
    const cardIds = cardIdResults.results.map(r => Number(r.card_id));

    return await getDeckIdsForCardIds(cardIds, db);
}

// =============================================================================
// #endregion
// #region 搜索流水线模块
// =============================================================================

/**
 * @description 详细的工作流程：这是搜索流水线的第一步。它负责解析 URL 查询参数，并将其分类为主过滤器和精确过滤器。
 * 工作步骤：
 * 1. 从 URL 中获取所有 `deck_name` 参数，并查询数据库以确定哪些是已索引的。
 * 2. 遍历所有 URL 查询参数，根据预定义的类型（`PRIMARY_FILTER_TYPES`, `REFINING_FILTER_TYPES`）进行分类。
 * 3. 对于 `deck_name` 参数，根据其是否被索引以及全局配置，决定是作为 `deck_name_indexed` 还是 `deck_name_fallback` 过滤器，或者忽略它。
 * 4. 收集所有未被索引的 `deck_name` 词汇，用于后续的后台更新。
 * 5. 执行安全检查：如果用户提供了 `deck_name` 参数但最终没有一个有效，则返回一个错误信号以中止搜索。
 * 6. 对主过滤器根据预设的优先级进行排序。
 * 7. 返回一个包含分类好的过滤器、错误信息和待更新词汇的对象。
 * @param {URLSearchParams} searchParams - 预计输入：请求的 URL 查询参数。预计输入的类型：`URLSearchParams`。
 * @param {object} env - 预计输入：Worker 的环境变量。预计输入的类型：`object`。
 * @returns {Promise<object>} - 预计输出：一个包含 `{ primaryFilters, refiningFilters, unindexedTerms, error }` 的对象。预计输出的类型：`Promise<object>`。
 */
async function parseAndCategorizeFilters(searchParams, env) {
    const deckNameValues = searchParams.getAll('deck_name');
    const indexedDeckNameTerms = new Set();
    if (deckNameValues.length > 0) {
        const termPlaceholders = deckNameValues.map(() => '?').join(',');
        const lowerCaseTerms = deckNameValues.map(v => v.toLowerCase());
        const query = `SELECT DISTINCT term FROM SearchIndexToDecks WHERE term IN (${termPlaceholders})`;
        const indexedResults = await env.DECK_DB.prepare(query).bind(...lowerCaseTerms).all();
        if (indexedResults.results) {
            indexedResults.results.forEach(row => indexedDeckNameTerms.add(row.term));
        }
    }
    
    const allFilters = [];
    let hasValidDeckNameFilter = false;
    const unindexedTerms = new Set();
    const uniqueKeys = [...new Set(searchParams.keys())];

    const isDeckNameIndexed = indexedDeckNameTerms.size > 0;
    const priority = isDeckNameIndexed
        ? { deck_name_indexed: 0, card: 1, setcode: 2, card_name: 3, attribute: 4, type: 5, race: 6, deck_name_fallback: 8 }
        : { card: 0, setcode: 1, card_name: 2, deck_name_fallback: 3, attribute: 4, type: 5, race: 6 };

    for (const key of uniqueKeys) {
        if (!PRIMARY_FILTER_TYPES.has(key) && !REFINING_FILTER_TYPES.has(key) && key !== 'deck_name') continue;
        const values = searchParams.getAll(key);
        values.forEach(val => {
            if (!val) return;
            if (key === 'deck_name') {
                if (indexedDeckNameTerms.has(val.toLowerCase())) {
                    allFilters.push({ type: 'deck_name_indexed', value: val, priority: priority['deck_name_indexed'] });
                    hasValidDeckNameFilter = true;
                } else {
                    unindexedTerms.add(val);
                    if (ALLOW_UNINDEXED_DECK_NAME_SEARCH) {
                        allFilters.push({ type: 'deck_name_fallback', value: val, priority: priority['deck_name_fallback'] });
                        hasValidDeckNameFilter = true;
                    }
                }
            } else if (PRIMARY_FILTER_TYPES.has(key)) {
                allFilters.push({ type: key, value: val, priority: priority[key] });
            } else if (REFINING_FILTER_TYPES.has(key)) {
                allFilters.push({ type: key, value: val });
            }
        });
    }

    if (deckNameValues.length > 0 && !hasValidDeckNameFilter) {
        return { 
            error: { payload: { success: true, data: { total: 0, start: 0, size: 0, list: [] } }, status: 200 },
            unindexedTerms 
        };
    }
    
    const primaryFilters = allFilters.filter(f => PRIMARY_FILTER_TYPES.has(f.type)).sort((a, b) => a.priority - b.priority);
    const refiningFilters = allFilters.filter(f => REFINING_FILTER_TYPES.has(f.type));

    if (primaryFilters.length === 0 && refiningFilters.length === 0) {
        return { error: { payload: { success: true, data: { total: 0, start: 0, size: 0, list: [], message: "请输入搜索条件。" } }, status: 200 } };
    }
    
    const hasContentFilter = primaryFilters.some(f => ['card', 'setcode', 'card_name', 'deck_name_indexed', 'deck_name_fallback'].includes(f.type));
    const hasAttributeFilter = primaryFilters.some(f => ['attribute', 'type', 'race'].includes(f.type));
    if (!hasContentFilter && hasAttributeFilter) {
        return { error: { payload: { success: true, data: { total: 0, start: 0, size: 0, list: [], message: "种族、属性、类型等参数必须与卡名、系列等核心参数一同使用。" } }, status: 200 } };
    }

    return { primaryFilters, refiningFilters, unindexedTerms, error: null };
}

/**
 * @description 详细的工作流程：这是搜索流水线的第二步。它按优先级顺序执行所有主过滤器，并返回它们结果的交集。
 * 工作步骤：
 * 1. 接收一个按优先级排序的主过滤器数组。
 * 2. 初始化一个 `candidateDeckIds` 变量为 `null`。
 * 3. 遍历过滤器数组。对于每个过滤器：
 * a. 根据过滤器类型（如 'card', 'card_name', 'deck_name_indexed' 等）调用相应的数据库查询辅助函数，获取一个卡组 ID 的 Set。
 * b. 如果 `candidateDeckIds` 是 `null`（即这是第一个过滤器），则将其设置为本次查询的结果。
 * c. 如果 `candidateDeckIds` 已有值，则计算它与本次结果的交集，并将交集作为新的 `candidateDeckIds`。
 * d. 如果任何时候交集变为空集，则提前中止循环。
 * 4. 如果没有主过滤器，则返回 `null`。
 * 5. 返回最终计算出的候选卡组 ID Set。
 * @param {object[]} primaryFilters - 预计输入：经过排序的主过滤器对象数组。预计输入的类型：`Array<object>`。
 * @param {D1Database} db - 预计输入：Cloudflare D1 数据库的实例。预计输入的类型：`object`。
 * @returns {Promise<Set<string>|null>} - 预计输出：一个解析为包含候选卡组 ID 的 Set 的 Promise，如果没有主过滤器则为 `null`。预计输出的类型：`Promise<Set<string>|null>`。
 */
async function executePrimaryFilters(primaryFilters, db) {
    let candidateDeckIds = null;

    for (const filter of primaryFilters) {
        if (candidateDeckIds !== null && candidateDeckIds.size === 0) break;

        let idsFromThisFilter = new Set();
        switch (filter.type) {
            case 'deck_name_indexed': {
                const res = await db.prepare("SELECT deck_id FROM SearchIndexToDecks WHERE term = ?").bind(filter.value.toLowerCase()).all();
                idsFromThisFilter = new Set(res.results.map(r => String(r.deck_id)));
                break;
            }
            case 'deck_name_fallback': {
                const res = await db.prepare("SELECT deck_id FROM Decks WHERE deck_name LIKE ?").bind(`%${filter.value}%`).all();
                idsFromThisFilter = new Set(res.results.map(r => String(r.deck_id)));
                break;
            }
            case 'card_name': {
                const cardIdQuery = `SELECT id FROM Cards WHERE cn_name = ?1 OR sc_name = ?1 OR md_name = ?1 OR nwbbs_n = ?1 OR cnocg_n = ?1`;
                const cardIdRes = await db.prepare(cardIdQuery).bind(filter.value).all();
                idsFromThisFilter = await getDeckIdsForCardIds(cardIdRes.results.map(r => Number(r.id)), db);
                break;
            }
            case 'card':
            case 'setcode': {
                const baseKeyword = await resolveKeywordPointer(filter.value, db);
                const cardIdRes = await db.prepare("SELECT card_id FROM KeywordToCard WHERE keyword = ?").bind(baseKeyword).all();
                const cardIds = cardIdRes.results.map(r => Number(r.card_id)); // Corrected, was r.id
                idsFromThisFilter = await getDeckIdsForCardIds(cardIds, db);
                break;
            }
            case 'race':
            case 'attribute':
            case 'type': {
                idsFromThisFilter = await getDeckIdsByAttribute(filter.type, filter.value, db);
                break;
            }
        }
        
        if (candidateDeckIds === null) {
            candidateDeckIds = idsFromThisFilter;
        } else {
            candidateDeckIds = new Set([...candidateDeckIds].filter(id => idsFromThisFilter.has(id)));
        }
    }
    return candidateDeckIds;
}

/**
 * @description 详细的工作流程：这是搜索流水线的第三步。它对一个已有的卡组 ID 集合应用一系列精确过滤器。
 * 工作步骤：
 * 1. 接收一个候选卡组 ID Set 和一个精确过滤器数组。
 * 2. 如果候选 ID Set 为 `null`（意味着没有主过滤器被执行），但存在精确过滤器，则首先查询数据库获取所有卡组的 ID 作为初始候选集。
 * 3. 遍历精确过滤器数组。对于每个过滤器：
 * a. 根据过滤器类型（如 'likes_ge', 'after_date'）构建相应的 SQL `WHERE` 子句。
 * b. 在当前的候选卡组 ID 集合上执行这个 `WHERE` 子句，以进一步缩小范围。
 * c. 将查询结果更新为新的候选卡组 ID Set。
 * 4. 返回经过所有精确过滤器筛选后的最终卡组 ID Set。
 * @param {Set<string>|null} candidateDeckIds - 预计输入：主过滤器产生的结果集，或在没有主过滤器时为 `null`。预计输入的类型：`Set<string>|null`。
 * @param {object[]} refiningFilters - 预计输入：精确过滤器对象数组。预计输入的类型：`Array<object>`。
 * @param {D1Database} db - 预计输入：Cloudflare D1 数据库的实例。预计输入的类型：`object`。
 * @returns {Promise<Set<string>>} - 预计输出：一个解析为最终卡组 ID Set 的 Promise。预计输出的类型：`Promise<Set<string>>`。
 */
async function executeRefiningFilters(candidateDeckIds, refiningFilters, db) {
    let currentIdSet = candidateDeckIds;

    if (refiningFilters.length === 0) {
        return currentIdSet ?? new Set();
    }

    if (currentIdSet === null) {
        console.log("没有主过滤器，为精确过滤器初始化所有卡组ID。");
        const allIdsResult = await db.prepare("SELECT deck_id FROM Decks").all();
        currentIdSet = allIdsResult.results ? new Set(allIdsResult.results.map(r => String(r.deck_id))) : new Set();
    }

    for (const filter of refiningFilters) {
        if (currentIdSet.size === 0) break;
        
        let clause = '';
        let value;
        
        switch (filter.type) {
            case 'likes_ge':
                clause = 'deck_like >= ?';
                value = parseInt(filter.value, 10);
                break;
            case 'likes_le':
                clause = 'deck_like <= ?';
                value = parseInt(filter.value, 10);
                break;
            case 'after_date': {
                clause = 'update_date >= ?';
                const [year, month, day] = filter.value.split('-').map(Number);
                // 使用 Date.UTC() 创建明确的UTC时间戳, 并确保是毫秒单位
                value = Date.UTC(year, month - 1, day);
                break;
            }
            case 'before_date': {
                clause = 'update_date <= ?';
                const [year, month, day] = filter.value.split('-').map(Number);
                // 使用 Date.UTC() 创建明确的UTC时间戳, 代表一天的结束, 毫秒单位
                value = Date.UTC(year, month - 1, day, 23, 59, 59, 999);
                break;
            }
            default:
                continue;
        }

        if (isNaN(value)) continue;

        const currentIdArray = [...currentIdSet];
        const nextRefinedIds = new Set();

        for (let i = 0; i < currentIdArray.length; i += (SQLITE_PARAM_LIMIT - 1)) {
            const chunk = currentIdArray.slice(i, i + (SQLITE_PARAM_LIMIT - 1));
            const placeholders = chunk.map(() => '?').join(',');
            const query = `SELECT deck_id FROM Decks WHERE ${clause} AND deck_id IN (${placeholders})`;
            const params = [value, ...chunk];
            const res = await db.prepare(query).bind(...params).all();
            if (res.results) {
                res.results.forEach(r => nextRefinedIds.add(String(r.deck_id)));
            }
        }
        currentIdSet = nextRefinedIds;
    }
    return currentIdSet;
}

/**
 * @description 详细的工作流程：这是搜索流水线的第四步。它对最终的卡组 ID 列表进行排序和分页。
 * 工作步骤：
 * 1. 接收一个最终的卡组 ID Set 和分页/排序参数。
 * 2. 为了排序，需要从数据库中获取这些 ID 对应的 `deck_like` 和 `update_date`。此查询分块进行以提高性能。
 * 3. 根据 `order` 参数（'rate' 或 'date'）和 `reverse` 参数，在内存中对获取到的数据进行排序。
 * 4. 排序后，根据 `start` 和 `size` 参数对 ID 列表进行切片，实现分页。
 * 5. 返回一个包含总数和当前页 ID 列表的对象。
 * @param {Set<string>} deckIds - 预计输入：最终的卡组 ID Set。预计输入的类型：`Set<string>`。
 * @param {URLSearchParams} searchParams - 预计输入：请求的 URL 查询参数，用于获取分页和排序信息。预计输入的类型：`URLSearchParams`。
 * @param {D1Database} db - 预计输入：Cloudflare D1 数据库的实例。预计输入的类型：`object`。
 * @returns {Promise<object>} - 预计输出：一个解析为包含 `{ paginatedIds, total }` 的对象的 Promise。预计输出的类型：`Promise<object>`。
 */
async function sortAndPaginateResults(deckIds, searchParams, db) {
    const total = deckIds.size;
    const offset = Math.max(0, parseInt(searchParams.get('start'), 10) || 0);
    const limit = Math.max(1, parseInt(searchParams.get('size'), 10) || 30);
    const finalDeckIds = [...deckIds];

    let allDecksWithSortData = [];
    for (let i = 0; i < total; i += SQLITE_PARAM_LIMIT) {
        const chunk = finalDeckIds.slice(i, i + SQLITE_PARAM_LIMIT);
        if (chunk.length === 0) continue;
        const placeholders = chunk.map(() => '?').join(',');
        const query = `SELECT deck_id, deck_like, update_date FROM Decks WHERE deck_id IN (${placeholders})`;
        const res = await db.prepare(query).bind(...chunk).all();
        if (res.results) allDecksWithSortData.push(...res.results);
    }

    const reverse = searchParams.get('reverse') === 'true';
    const sortBy = searchParams.get('order') || 'rate';
    
    allDecksWithSortData.sort((a, b) => {
        const order = reverse ? -1 : 1;
        const a_update = Number(a.update_date || 0);
        const b_update = Number(b.update_date || 0);
        const a_like = Number(a.deck_like || 0);
        const b_like = Number(b.deck_like || 0);

        if (sortBy === 'date') {
            const dateDiff = b_update - a_update;
            return (dateDiff !== 0 ? dateDiff : b_like - a_like) * order;
        } else {
            const likeDiff = b_like - a_like;
            return (likeDiff !== 0 ? likeDiff : b_update - a_update) * order;
        }
    });

    const paginatedIds = allDecksWithSortData.slice(offset, offset + limit).map(d => String(d.deck_id)); // Corrected d.deck_id
    return { paginatedIds, total };
}

/**
 * @description 详细的工作流程：这是搜索流水线的最后一步。它根据已排序和分页的 ID 列表，获取完整的卡组数据。
 * 工作步骤：
 * 1. 接收一个经过排序和分页的卡组 ID 数组。
 * 2. 执行一个 `SELECT * FROM Decks WHERE deck_id IN (...)` 查询来获取这些卡组的全部字段数据。
 * 3. 为了保持之前步骤计算出的排序顺序，将查询结果存入一个 Map。
 * 4. 遍历输入的 ID 数组，从 Map 中按顺序取出完整的卡组数据，构建最终的有序结果列表。
 * 5. 返回包含完整卡组数据的有序数组。
 * @param {string[]} paginatedIds - 预计输入：已排序和分页的卡组 ID 数组。预计输入的类型：`Array<string>`。
 * @param {D1Database} db - 预计输入：Cloudflare D1 数据库的实例。预计输入的类型：`object`。
 * @returns {Promise<object[]>} - 预计输出：一个解析为包含完整卡组对象的有序数组的 Promise。预计输出的类型：`Promise<Array<object>>`。
 */
async function fetchFinalDeckData(paginatedIds, db) {
    if (paginatedIds.length === 0) return [];
    const finalPlaceholders = paginatedIds.map(() => '?').join(',');
    const dataResult = await db.prepare(`SELECT * FROM Decks WHERE deck_id IN (${finalPlaceholders})`).bind(...paginatedIds).all();
    const resultsMap = new Map(dataResult.results.map(r => [String(r.deck_id), r]));
    return paginatedIds.map(id => resultsMap.get(id)).filter(Boolean);
}

// =============================================================================
// #endregion
// #region 主处理函数
// =============================================================================

/**
 * @description 详细的工作流程：此函数是处理 `/api/decks/search` 请求的主入口和总控制器。
 * 工作步骤：
 * 1.  调用 `parseAndCategorizeFilters` 解析并验证 URL 参数，获取过滤器和待更新的关键词。如果验证失败，则直接返回错误或空结果。
 * 2.  调用 `executePrimaryFilters` 执行主过滤器，获取初步的候选卡组 ID 集合。
 * 3.  调用 `executeRefiningFilters` 在上述结果上执行精确过滤器，进一步筛选卡组 ID。
 * 4.  如果此时卡组 ID 集合为空，则直接返回空结果响应。
 * 5.  调用 `sortAndPaginateResults` 对最终的卡组 ID 集合进行排序和分页。
 * 6.  调用 `fetchFinalDeckData` 获取当前页卡组的完整数据。
 * 7.  如果存在未被索引的关键词，则调用 `ctx.waitUntil` 在后台触发 `updateGitHubFile` 函数，并为其提供关键词文件的特定路径和专用的数组合并器。
 * 8.  构建最终的成功响应 JSON 并返回。
 * 9.  捕获整个过程中的任何异常，并返回 500 内部服务器错误。
 * @param {Request} request - 预计输入：传入的 HTTP 请求对象。预计输入的类型：`Request`。
 * @param {object} env - 预计输入：Worker 的环境变量，包含数据库绑定和密钥。预计输入的类型：`object`。
 * @param {object} ctx - 预计输入：Worker 的执行上下文，用于 `waitUntil`。预计输入的类型：`object`。
 * @returns {Promise<Response>} - 预计输出：一个解析为 Response 对象的 Promise。预计输出的类型：`Promise<Response>`。
 */
export default async function handleSearchRequest(request, env, ctx) {
    try {
        const { searchParams } = new URL(request.url);
        const offset = Math.max(0, parseInt(searchParams.get('start'), 10) || 0);

        const { primaryFilters, refiningFilters, unindexedTerms, error } = await parseAndCategorizeFilters(searchParams, env);
        
        if (error) {
            if (unindexedTerms && unindexedTerms.size > 0 && env.GITHUB_KEYWORDS_FILE_PATH) {
                ctx.waitUntil(updateGitHubFile({
                    filePath: env.GITHUB_KEYWORDS_FILE_PATH,
                    dataToAdd: [...unindexedTerms],
                    commitMessage: `feat: 自动添加 ${unindexedTerms.size} 个新关键词`,
                    env: env,
                    merger: jsonArrayMerger
                }));
            }
            return JsonResponse(error.payload, error.status);
        }

        let finalDeckIds = await executePrimaryFilters(primaryFilters, env.DECK_DB);
        finalDeckIds = await executeRefiningFilters(finalDeckIds, refiningFilters, env.DECK_DB);

        if (finalDeckIds.size === 0) {
            if (unindexedTerms.size > 0 && env.GITHUB_KEYWORDS_FILE_PATH) {
                ctx.waitUntil(updateGitHubFile({
                    filePath: env.GITHUB_KEYWORDS_FILE_PATH,
                    dataToAdd: [...unindexedTerms],
                    commitMessage: `feat: 自动添加 ${unindexedTerms.size} 个新关键词`,
                    env: env,
                    merger: jsonArrayMerger
                }));
            }
            return JsonResponse({ success: true, data: { total: 0, start: offset, size: 0, list: [] } });
        }

        const { paginatedIds, total } = await sortAndPaginateResults(finalDeckIds, searchParams, env.DECK_DB);
        const finalDecks = await fetchFinalDeckData(paginatedIds, env.DECK_DB);
        
        if (unindexedTerms.size > 0 && env.GITHUB_KEYWORDS_FILE_PATH) {
            ctx.waitUntil(updateGitHubFile({
                filePath: env.GITHUB_KEYWORDS_FILE_PATH,
                dataToAdd: [...unindexedTerms],
                commitMessage: `feat: 自动添加 ${unindexedTerms.size} 个新关键词`,
                env: env,
                merger: jsonArrayMerger
            }));
        }

        return JsonResponse({
            success: true,
            data: {
                total,
                start: offset,
                size: finalDecks.length,
                list: finalDecks
            }
        });

    } catch (e) {
        console.error("搜索处理时发生异常: ", e);
        return JsonResponse({ success: false, error: 'Internal Server Error', message: e.message }, 500);
    }
}
// =============================================================================
// #endregion
