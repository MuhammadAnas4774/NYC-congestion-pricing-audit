@echo off
echo Starting NYC Congestion Audit Pipeline...
cd /d "C:\Users\Ayyan LapTop\nyc_congestion_audit"
call venv\Scripts\activate.bat
python pipeline.py
pause
