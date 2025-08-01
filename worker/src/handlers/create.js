import { JsonResponse } from '../utils/jsonResponse.js';

/**
 * @description 处理创建新卡组的请求。[存根]
 * @param {Request} request - 输入：传入的请求对象。
 * @param {object} env - 输入：环境变量对象。
 * @returns {Promise<Response>} - 输出：一个 501 Not Implemented 响应。
 */
export default async function handleCreateRequest(request, env) {
  return JsonResponse({
    success: false,
    error: 'Not Implemented',
    message: '卡组创建功能尚未实现。'
  }, 501);
}
