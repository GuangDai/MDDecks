/**
 * @description 创建一个标准化的 JSON 响应对象，并附带 CORS 头部。
 * @param {object | string | null} data - 输入：响应的数据负载，可以是对象、字符串或 null。
 * @param {number} [status=200] - 输入：HTTP 状态码，默认为 200。
 * @returns {Response} - 输出：一个标准的 Response 对象。
 */
export function JsonResponse(data, status = 200) {
    const payload = (data === null || typeof data === 'object') ? JSON.stringify(data, null, 2) : data;
    const headers = {
      'Content-Type': 'application/json;charset=UTF-8',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    };
    return new Response(payload, { status, headers });
  }
