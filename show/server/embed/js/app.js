// 全局状态
let currentKey = null;
let eventSource = null;
let allData = {};
let hasInitialData = false;
let sseConnected = false;
let reconnectAttempts = 0;
let maxReconnectAttempts = 10;

// DOM 元素
const keysList = document.getElementById('keysList');
const currentKeyEl = document.getElementById('currentKey');
const statusIndicator = document.getElementById('statusIndicator');
const totalFilesEl = document.getElementById('totalFiles');
const completedFilesEl = document.getElementById('completedFiles');
const totalLinesEl = document.getElementById('totalLines');
const processedLinesEl = document.getElementById('processedLines');
const overallProgressEl = document.getElementById('overallProgress');
const overallProgressTextEl = document.getElementById('overallProgressText');
const filesTableBody = document.getElementById('filesTableBody');

// 初始化
async function init() {
    try {
        // 获取 key 列表
        const response = await fetch('/api/keys');
        const data = await response.json();
        renderKeysList(data.keys);

        // 立即获取数据并显示
        await loadInitialData();

        // 建立 SSE 连接进行实时更新
        connectSSE();
    } catch (error) {
        console.error('Initialization error:', error);
        updateStatus('error', '连接失败');
    }
}

// 加载初始数据
async function loadInitialData() {
    try {
        // 获取所有 key 的数据
        const keys = Array.from(document.querySelectorAll('.keys-list button')).map(btn => btn.textContent);

        for (const key of keys) {
            try {
                const response = await fetch(`/api/progress?key=${key}`);
                if (response.ok) {
                    const data = await response.json();
                    allData[key] = data;
                }
            } catch (error) {
                console.warn(`Failed to load data for key ${key}:`, error);
            }
        }

        // 标记已有初始数据并显示页面
        if (!hasInitialData) {
            hasInitialData = true;
            document.querySelector('.main-content').style.display = 'block';
        }

        // 更新显示
        updateDisplay();
        updateStatus('loading', '已加载数据');
    } catch (error) {
        console.error('Failed to load initial data:', error);
        updateStatus('error', '数据加载失败');
    }
}

// 渲染 key 列表
function renderKeysList(keys) {
    keysList.innerHTML = '';
    keys.forEach(key => {
        const li = document.createElement('li');
        const button = document.createElement('button');
        button.textContent = key;
        button.onclick = () => selectKey(key);
        li.appendChild(button);
        keysList.appendChild(li);

        // 选中第一个 key
        if (!currentKey) {
            selectKey(key);
        }
    });
}

// 选择 key
function selectKey(key) {
    currentKey = key;
    currentKeyEl.textContent = key;

    // 更新按钮状态
    document.querySelectorAll('.keys-list button').forEach(btn => {
        btn.classList.toggle('active', btn.textContent === key);
    });

    // 更新显示
    updateDisplay();
}

// 建立 SSE 连接
function connectSSE() {
    if (eventSource) {
        eventSource.close();
    }

    eventSource = new EventSource('/api/stream');

    eventSource.onopen = () => {
        sseConnected = true;
        reconnectAttempts = 0;
        updateStatus('connected', '实时更新');
    };

    eventSource.onmessage = (event) => {
        try {
            allData = JSON.parse(event.data);
            updateDisplay();
        } catch (error) {
            console.error('Parse error:', error);
        }
    };

    eventSource.onerror = (error) => {
        if (!sseConnected) {
            // 首次连接失败
            console.error('SSE connection failed:', error);
            updateStatus('error', '实时更新断开');
        } else {
            // 已连接后断开，自动重连
            if (reconnectAttempts < maxReconnectAttempts) {
                reconnectAttempts++;
                const delay = Math.min(3000 * reconnectAttempts, 10000); // 指数退避，最大 10 秒
                console.warn(`SSE connection lost, retrying in ${delay}ms (attempt ${reconnectAttempts}/${maxReconnectAttempts})...`);
                updateStatus('loading', `重连中 (${reconnectAttempts}/${maxReconnectAttempts})...`);

                setTimeout(() => {
                    connectSSE();
                }, delay);
            } else {
                console.error('Max reconnection attempts reached');
                updateStatus('error', '连接断开，请刷新页面');
            }
        }
    };
}

// 更新状态指示器
function updateStatus(status, text) {
    const dot = statusIndicator.querySelector('.status-dot');
    const statusText = statusIndicator.querySelector('.status-text');

    statusText.textContent = text;

    if (status === 'connected') {
        dot.style.background = '#10b981';
    } else if (status === 'error') {
        dot.style.background = '#ef4444';
    } else if (status === 'loading') {
        dot.style.background = '#f59e0b';
    } else {
        dot.style.background = '#6b7280';
    }
}

// 更新显示
function updateDisplay() {
    if (!currentKey || !allData[currentKey]) {
        return;
    }

    const data = allData[currentKey];

    // 更新统计
    totalFilesEl.textContent = data.total_files || 0;
    completedFilesEl.textContent = data.completed_files || 0;
    totalLinesEl.textContent = formatNumber(data.total_lines || 0);
    processedLinesEl.textContent = formatNumber(data.processed_lines || 0);

    // 更新整体进度
    const progress = data.overall_pct || 0;
    overallProgressEl.style.width = `${progress}%`;
    overallProgressTextEl.textContent = `${progress.toFixed(2)}%`;

    // 更新文件列表
    renderFilesTable(data.files || []);
}

// 渲染文件表格
function renderFilesTable(files) {
    filesTableBody.innerHTML = '';

    // 按文件名排序
    const sortedFiles = [...files].sort((a, b) => {
        return a.file_name.localeCompare(b.file_name);
    });

    sortedFiles.forEach(file => {
        const tr = document.createElement('tr');

        // 文件名
        tr.innerHTML = `
            <td>${file.file_name}</td>
            <td>${formatNumber(file.source_lines)}</td>
            <td>${formatNumber(file.result_lines)}</td>
            <td>${formatNumber(file.ignore_lines)}</td>
            <td>
                <div class="file-progress-bar">
                    <div class="file-progress-fill" style="width: ${file.processed_pct}%"></div>
                </div>
                <div style="margin-top: 4px; font-size: 12px; color: #6b7280;">
                    ${file.processed_pct.toFixed(1)}%
                </div>
            </td>
            <td>
                <span class="status-badge ${getStatusClass(file.processed_pct)}">
                    ${getStatusText(file.processed_pct)}
                </span>
            </td>
        `;

        filesTableBody.appendChild(tr);
    });
}

// 获取状态样式类
function getStatusClass(pct) {
    if (pct >= 100) return 'completed';
    if (pct > 0) return 'processing';
    return 'pending';
}

// 获取状态文本
function getStatusText(pct) {
    if (pct >= 100) return '已完成';
    if (pct > 0) return '处理中';
    return '等待中';
}

// 格式化数字 - 直接返回数字，不格式化
function formatNumber(num) {
    return num.toLocaleString();
}

// 启动应用
document.addEventListener('DOMContentLoaded', () => {
    // 页面加载时隐藏主内容区，等待数据
    document.querySelector('.main-content').style.display = 'none';
    init();
});
