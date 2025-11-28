@echo off
cd /d C:\flask_apps\flyer_web
call ..\main_server\venv\Scripts\activate
python flyer_app.py
pause
