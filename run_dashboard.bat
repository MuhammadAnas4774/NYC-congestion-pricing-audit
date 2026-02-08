@echo off
echo Starting Dashboard...
cd /d "C:\Users\Ayyan LapTop\nyc_congestion_audit"
call venv\Scripts\activate.bat
streamlit run dashboard.py
pause
