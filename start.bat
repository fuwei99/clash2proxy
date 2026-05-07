@echo off
setlocal
cd /d "%~dp0\.."
set PYTHONUTF8=1
"C:\Users\zhishang\AppData\Local\Programs\Python\Python312\python.exe" proxy\main.py
