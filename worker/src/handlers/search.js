// Note: This file is for a Cloudflare Worker and is written in Javascript.
import { JsonResponse } from '../utils/jsonResponse.js';

// --- Configuration ---
/**
 * @description Controls the search behavior for deck names not found in the pre-computed index.
 * - `false` (Recommended for Security): If a deck name is not indexed, it is ignored. If ALL provided deck names
 * are unindexed, the entire search returns empty, preventing resource-intensive fallback scans.
 * - `true`: Allows the system to fall back to a `... LIKE %term%` query on the main `Decks` table for unindexed
 * deck names. This is more flexible but can be slow and vulnerable to performance attacks.
 */
const ALLOW_UNINDEXED_DECK_NAME_SEARCH = false;

// Set a conservative limit for SQL parameters to avoid "too many SQL variables" errors in SQLite/D1.
const SQLITE_PARAM_LIMIT = 90;

// --- Filter Type Definitions ---
const PRIMARY_FILTER_TYPES = new Set([
    'deck_name_indexed', 'deck_name_fallback', 'card', 'setcode', 'card_name', 'attribute', 'race', 'type'
]);
const REFINING_FILTER_TYPES = new Set([
    'likes_ge', 'likes_le', 'after_date', 'before_date'
]);


/**
 * Helper function: Recursively resolves a keyword pointer to find the ultimate base keyword.
 * @param {string} keyword - The initial keyword.
 * @param {object} db - The D1 database instance.
 * @returns {Promise<string>} - The resolved base keyword.
 */
async function resolveKeywordPointer(keyword, db) {
    let currentKeyword = keyword;
    const visited = new Set(); // To prevent infinite loops

    for (let i = 0; i < 10; i++) { // Loop limit for safety
        if (visited.has(currentKeyword)) {
            throw new Error(`Circular pointer reference detected for keyword: ${keyword}`);
        }
        visited.add(currentKeyword);

        const result = await db.prepare("SELECT pointer_to FROM SearchKeywords WHERE keyword = ?")
            .bind(currentKeyword)
            .first("pointer_to");
        if (result) {
            currentKeyword = String(result);
        } else {
            return currentKeyword;
        }
    }
    throw new Error(`Keyword pointer resolution exceeded depth limit for: ${keyword}`);
}

/**
 * Helper function: Gets all deck IDs that contain any of the specified card IDs.
 * @param {number[]} cardIds - An array of card IDs.
 * @param {object} db - The D1 database instance.
 * @returns {Promise<Set<string>>} - A set of deck IDs.
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
 * Helper function: Gets matching deck IDs based on a card attribute (Primary Filter).
 * @param {string} filterType - 'race', 'attribute', or 'type'.
 * @param {string} value - The attribute value to search for.
 * @param {object} db - The D1 database instance.
 * @returns {Promise<Set<string>>} - A set of matching deck IDs.
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

/**
 * Helper function: Applies a refining filter to an existing set of candidate deck IDs.
 * @param {object} db - The D1 database instance.
 * @param {Set<string>} currentDeckIds - The set of deck IDs to refine.
 * @param {object} filter - The refining filter to apply (e.g., {type: 'likes_ge', value: '10'}).
 * @returns {Promise<Set<string>>} - A new, smaller set of refined deck IDs.
 */
async function applyRefiningFilter(db, currentDeckIds, filter) {
    if (currentDeckIds.size === 0) return new Set();

    let clause = '';
    let value;
    switch (filter.type) {
        case 'likes_ge': clause = 'deck_like >= ?'; value = parseInt(filter.value, 10); break;
        case 'likes_le': clause = 'deck_like <= ?'; value = parseInt(filter.value, 10); break;
        case 'after_date': clause = 'update_date >= ?'; value = new Date(filter.value).getTime() / 1000; break;
        case 'before_date': clause = 'update_date <= ?'; value = new Date(`${filter.value}T23:59:59.999Z`).getTime() / 1000; break;
        default: return currentDeckIds;
    }
    if (isNaN(value)) return currentDeckIds;

    const refinedIds = new Set();
    const idArray = [...currentDeckIds];

    for (let i = 0; i < idArray.length; i += (SQLITE_PARAM_LIMIT - 1)) {
        const chunk = idArray.slice(i, i + (SQLITE_PARAM_LIMIT - 1));
        const placeholders = chunk.map(() => '?').join(',');
        const query = `SELECT deck_id FROM Decks WHERE ${clause} AND deck_id IN (${placeholders})`;
        const params = [value, ...chunk];
        const res = await db.prepare(query).bind(...params).all();
        if (res.results) {
            res.results.forEach(r => refinedIds.add(String(r.deck_id)));
        }
    }
    return refinedIds;
}


/**
 * Placeholder function for handling unindexed deck name searches.
 * @param {string} unindexedTerm - The search term that was not found in the SearchIndexToDecks table.
 */
function handleUnindexedDeckNameSearch(unindexedTerm) {
    console.log(`Deck name search term not found in index: "${unindexedTerm}". Fallback policy is: ${ALLOW_UNINDEXED_DECK_NAME_SEARCH ? 'ENABLED' : 'DISABLED'}.`);
}

/**
 * @description Main entry point for handling the API request.
 * @param {object} request
 * @param {object} env
 * @returns {Promise<Response>}
 */
export default async function handleSearchRequest(request, env) {
    const { searchParams } = new URL(request.url);
    const db = env.DECK_DB;

    try {
        const offset = Math.max(0, parseInt(searchParams.get('start'), 10) || 0);
        const limit = Math.max(1, parseInt(searchParams.get('size'), 10) || 30);

        // --- Step 1: Pre-process `deck_name` to determine which are indexed ---
        const deckNameValues = searchParams.getAll('deck_name');
        const indexedDeckNameTerms = new Set();
        if (deckNameValues.length > 0) {
            const termPlaceholders = deckNameValues.map(() => '?').join(',');
            const lowerCaseTerms = deckNameValues.map(v => v.toLowerCase());
            const query = `SELECT DISTINCT term FROM SearchIndexToDecks WHERE term IN (${termPlaceholders})`;
            const indexedResults = await db.prepare(query).bind(...lowerCaseTerms).all();
            if (indexedResults.results) {
                indexedResults.results.forEach(row => indexedDeckNameTerms.add(row.term));
            }
        }

        // --- Step 2: Collect and categorize all filters ---
        const allFilters = [];
        let hasValidDeckNameFilter = false;
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
                        handleUnindexedDeckNameSearch(val);
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
        
        const primaryFilters = allFilters.filter(f => PRIMARY_FILTER_TYPES.has(f.type)).sort((a, b) => a.priority - b.priority);
        const refiningFilters = allFilters.filter(f => REFINING_FILTER_TYPES.has(f.type));

        // --- Step 3: Validate Filter Combinations ---
        
        // [MODIFIED] Security Check: If the user provided `deck_name` parameters but none were valid
        // (i.e., they were all unindexed and fallback is disabled), abort the search.
        // This prevents attacks like `deck_name=garbage&likes_ge=1` from querying the entire database.
        if (deckNameValues.length > 0 && !hasValidDeckNameFilter) {
            console.log("Search aborted: User provided deck_name parameter(s), but none were valid/indexed.");
            return JsonResponse({ success: true, data: { total: 0, start: offset, size: 0, list: [] } });
        }

        if (primaryFilters.length === 0 && refiningFilters.length === 0) {
            return JsonResponse({ success: true, data: { total: 0, start: offset, size: 0, list: [], message: "请输入搜索条件。" } });
        }

        const hasContentFilter = primaryFilters.some(f => ['card', 'setcode', 'card_name', 'deck_name_indexed', 'deck_name_fallback'].includes(f.type));
        const hasAttributeFilter = primaryFilters.some(f => ['attribute', 'type', 'race'].includes(f.type));
        if (!hasContentFilter && hasAttributeFilter) {
            return JsonResponse({ success: true, data: { total: 0, start: offset, size: 0, list: [], message: "种族、属性、类型等参数必须与卡名、系列等核心参数一同使用。" } });
        }

        // --- Step 4: Execute Two-Stage Layered Filtering ---
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
                    idsFromThisFilter = await getDeckIdsForCardIds(cardIdRes.results.map(r => Number(r.id)), db);
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

        if (candidateDeckIds === null && refiningFilters.length > 0) {
            const allIdsResult = await db.prepare("SELECT deck_id FROM Decks").all();
            candidateDeckIds = allIdsResult.results ? new Set(allIdsResult.results.map(r => String(r.deck_id))) : new Set();
        }

        for (const filter of refiningFilters) {
            if (candidateDeckIds.size === 0) break;
            candidateDeckIds = await applyRefiningFilter(db, candidateDeckIds, filter);
        }

        if (candidateDeckIds === null || candidateDeckIds.size === 0) {
            return JsonResponse({ success: true, data: { total: 0, start: offset, size: 0, list: [] } });
        }

        const finalDeckIds = [...candidateDeckIds];
        const total = finalDeckIds.length;

        // --- Step 5: Global Sorting and Pagination of the FINAL ID list ---
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
            if (sortBy === 'date') {
                const dateDiff = (b.update_date || 0) - (a.update_date || 0);
                return (dateDiff !== 0 ? dateDiff : (b.deck_like || 0) - (a.deck_like || 0)) * order;
            } else { // 'rate'
                const likeDiff = (b.deck_like || 0) - (a.deck_like || 0);
                return (likeDiff !== 0 ? likeDiff : (b.update_date || 0) - (a.update_date || 0)) * order;
            }
        });

        const sortedAndPaginatedIds = allDecksWithSortData.slice(offset, offset + limit).map(d => d.deck_id);

        if (sortedAndPaginatedIds.length === 0) {
            return JsonResponse({ success: true, data: { total, start: offset, size: 0, list: [] } });
        }

        // --- Step 6: Fetch full data for the paginated page ---
        const finalPlaceholders = sortedAndPaginatedIds.map(() => '?').join(',');
        const dataResult = await db.prepare(`SELECT * FROM Decks WHERE deck_id IN (${finalPlaceholders})`).bind(...sortedAndPaginatedIds).all();

        const resultsMap = new Map(dataResult.results.map(r => [String(r.deck_id), r]));
        const orderedResults = sortedAndPaginatedIds.map(id => resultsMap.get(id)).filter(Boolean);

        return JsonResponse({
            success: true,
            data: { total, start: offset, size: orderedResults.length, list: orderedResults }
        });

    } catch (e) {
        console.error("An exception occurred during the query: ", e);
        return JsonResponse({ success: false, error: `Internal Server Error: ${e.message}` }, 500);
    }
}
