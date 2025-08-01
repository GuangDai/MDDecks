/**
 * ====================================================================================
 * YGO Deck API - Cloudflare Worker
 *
 * 主入口文件，负责路由传入的请求到相应的处理模块。
 * ====================================================================================
 */
import { JsonResponse, getApiDocs } from './src/utils/apiDocs.js';
import handleSearchRequest from './src/handlers/search.js';
import handleCreateRequest from './src/handlers/create.js';
import handleUpdateRequest from './src/handlers/update.js';
import handleDeleteRequest from './src/handlers/delete.js';
import handleRateRequest from './src/handlers/rate.js';

export default {
  /**
   * @description Cloudflare Worker 的主 fetch 处理函数，作为 API 路由器。
   * @param {Request} request - 输入：传入的请求对象。
   * @param {object} env - 输入：环境变量对象，包含数据库等绑定。
   * @returns {Promise<Response>} - 输出：一个解析为 Response 对象的 Promise。
   */
  async fetch(request, env) {
    // 处理 CORS 预检请求
    if (request.method === 'OPTIONS') {
      const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      };
      return new Response(null, { status: 204, headers });
    }

    const url = new URL(request.url);

    try {
      // 根据路径进行路由分发
      switch (url.pathname) {
        case '/api/decks/search':
          if (request.method !== 'GET') return JsonResponse({ success: false, error: 'Method Not Allowed' }, 405);
          return await handleSearchRequest(request, env);

        case '/api/decks/create':
          if (request.method !== 'POST') return JsonResponse({ success: false, error: 'Method Not Allowed' }, 405);
          return await handleCreateRequest(request, env);

        case '/api/decks/update':
          if (request.method !== 'PUT' && request.method !== 'POST') return JsonResponse({ success: false, error: 'Method Not Allowed. Use PUT or POST.' }, 405);
          return await handleUpdateRequest(request, env);

        case '/api/decks/delete':
          if (request.method !== 'DELETE' && request.method !== 'POST') return JsonResponse({ success: false, error: 'Method Not Allowed. Use DELETE or POST.' }, 405);
          return await handleDeleteRequest(request, env);

        case '/api/decks/rate':
          if (request.method !== 'POST') return JsonResponse({ success: false, error: 'Method Not Allowed' }, 405);
          return await handleRateRequest(request, env);

        default:
          // 对于未匹配的路径，返回 404 和 API 文档
          return JsonResponse({ success: false, error: 'Not Found', ...getApiDocs() }, 404);
      }
    } catch (e) {
      console.error("Top-level error:", e.name, e.message);
      return JsonResponse({ success: false, error: 'Internal Server Error', message: e.message }, 500);
    }
  },
};
