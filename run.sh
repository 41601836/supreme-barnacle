#!/bin/bash

echo "========================================"
echo "股票情绪分析与趋势洞察工具"
echo "========================================"
echo ""

echo "[1] 检查虚拟环境..."
if [ ! -f ".venv/bin/activate" ]; then
    echo "[错误] 虚拟环境不存在，请先运行: python3 -m venv .venv"
    exit 1
fi

echo "[2] 激活虚拟环境..."
source .venv/bin/activate

echo "[3] 检查依赖..."
if ! pip show streamlit > /dev/null 2>&1; then
    echo "[信息] 正在安装依赖..."
    pip install -r requirements.txt
fi

echo "[4] 启动应用..."
echo ""
echo "应用将在浏览器中打开: http://localhost:8501"
echo "按 Ctrl+C 停止应用"
echo ""
streamlit run app.py
