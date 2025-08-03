// src/utils/githubUpdater.js

/**
 * @description 这是一个通用的 JSON 数组合并器。它接收一个 JSON 字符串和一个新项目数组，
 * 返回一个合并、去重、排序后的新数组。
 * @param {string|null} jsonString - 预计输入：一个代表数组的 JSON 字符串，或者在文件不存在时为 null。预计输入的类型：`string|null`。
 * @param {Array<any>} itemsToAdd - 预计输入：需要添加到数组中的新项目。预计输入的类型：`Array<any>`。
 * @returns {Array<any>} - 预计输出：合并、去重、排序后的最终数组。预计输出的类型：`Array<any>`。
 */
export function jsonArrayMerger(jsonString, itemsToAdd) {
    let existingItems = [];
    if (jsonString) {
        try {
            existingItems = JSON.parse(jsonString);
            if (!Array.isArray(existingItems)) {
                console.error("GitHub 上的现有文件内容不是一个有效的 JSON 数组，将创建一个新数组。");
                existingItems = [];
            }
        } catch (e) {
            console.error("解析 GitHub 上的 JSON 文件失败，将创建一个新数组。", e);
            existingItems = [];
        }
    }
    const combined = [...existingItems, ...itemsToAdd];
    return [...new Set(combined)].sort((a, b) => String(a).localeCompare(String(b)));
}

/**
 * @description 详细的工作流程：这是一个通用的 GitHub 文件更新函数，它处理所有与 API 的交互。
 * 工作步骤：
 * 1.  从环境变量中读取 GitHub 的凭证和基础配置。
 * 2.  根据传入的 `filePath` 构建完整的 API URL。
 * 3.  使用 `GET` 请求获取文件的当前内容和 `sha`。如果文件不存在（404），则视为空文件。
 * 4.  调用作为参数传入的 `merger` 函数，将旧内容（解码后）和新数据合并成最终的数据。
 * 5.  检查最终数据与旧数据是否有实际变化，如果没有则中止，以避免创建空的 commit。
 * 6.  将最终数据序列化并进行 Base64 编码。
 * 7.  使用 `PUT` 请求将新内容、`sha` 和自定义的 commit 消息提交到 GitHub。
 * @param {object} options - 预计输入：一个包含更新所需所有选项的对象。预计输入的类型：`object`。
 * @param {string} options.filePath - 预计输入：文件在仓库中的完整路径。
 * @param {any} options.dataToAdd - 预计输入：需要添加的新数据。
 * @param {string} options.commitMessage - 预计输入：本次更新的 commit 消息。
 * @param {object} options.env - 预计输入：Worker 的环境变量。
 * @param {Function} options.merger - 预计输入：一个合并函数 `(oldContent: string|null, newData: any) => any`。
 * @returns {Promise<void>} - 预计输出：无返回值，函数在后台执行。预计输出的类型：`Promise<void>`。
 */
export async function updateGitHubFile({ filePath, dataToAdd, commitMessage, env, merger }) {
    if (!dataToAdd) return;

    const { GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO } = env;
    if (!GITHUB_TOKEN || !GITHUB_OWNER || !GITHUB_REPO) {
        console.error("GitHub API 环境变量未完全设置 (需要 GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO)。");
        return;
    }

    const apiUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${filePath}`;
    const headers = {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Cloudflare-Worker-GitHub-Updater',
        'Content-Type': 'application/json'
    };

    try {
        const getFileResponse = await fetch(apiUrl, { headers });
        let oldContent = null;
        let currentSha = null;

        if (getFileResponse.status === 404) {
            console.log(`文件不存在于 ${filePath}，将创建新文件。`);
        } else if (getFileResponse.ok) {
            const fileData = await getFileResponse.json();
            currentSha = fileData.sha;
            oldContent = decodeURIComponent(escape(atob(fileData.content)));
        } else {
            throw new Error(`获取文件时 GitHub API 错误: ${getFileResponse.status} ${await getFileResponse.text()}`);
        }

        const finalData = merger(oldContent, dataToAdd);
        const newContent = JSON.stringify(finalData, null, 2);

        if (newContent === oldContent) {
            console.log("内容无变化，跳过 GitHub 更新。");
            return;
        }

        const newContentBase64 = btoa(unescape(encodeURIComponent(newContent)));
        const payload = { message: commitMessage, content: newContentBase64, sha: currentSha };
        const updateResponse = await fetch(apiUrl, { method: 'PUT', headers, body: JSON.stringify(payload) });

        if (!updateResponse.ok) throw new Error(`更新文件时 GitHub API 错误: ${updateResponse.status} ${await updateResponse.text()}`);
        console.log(`成功更新 GitHub 文件: ${filePath}`);
    } catch (error) {
        console.error(`在更新 GitHub 文件 (${filePath}) 过程中发生错误:`, error);
    }
}
