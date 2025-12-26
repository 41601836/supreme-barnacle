@echo off
echo ========================================
echo 股票情绪分析与趋势洞察工具
echo ========================================
echo.

echo [1] 检查虚拟环境...
if not exist ".venv\Scripts\activate.bat" (
    echo [错误] 虚拟环境不存在，请先运行: python -m venv .venv
    pause
    exit /b 1
)

echo [2] 激活虚拟环境...
call .venv\Scripts\activate.bat

echo [3] 检查依赖...
pip show streamlit >nul 2>&1
if errorlevel 1 (
    echo [信息] 正在安装依赖...
    pip install -r requirements.txt
)

echo [4] 启动应用...
echo.
echo 应用将在浏览器中打开: http://localhost:8501
echo 按 Ctrl+C 停止应用
echo.
streamlit run app.py

pause
