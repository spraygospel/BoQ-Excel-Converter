@echo off
echo ================================================
echo     CEK VERSI DEPENDENCIES
echo ================================================
echo.

:: Aktivasi virtual environment
call venv\Scripts\activate

:: Menampilkan daftar paket dalam format tabel
echo ===== DAFTAR SEMUA PACKAGES =====
pip list

echo.
echo ===== DETAIL PACKAGES UTAMA =====
echo.
echo === streamlit ===
pip show streamlit

echo.
echo === pandas ===
pip show pandas

echo.
echo === numpy ===
pip show numpy

echo.
echo === openpyxl ===
pip show openpyxl

:: Deaktivasi virtual environment
call venv\Scripts\deactivate

echo.
echo ================================================
echo     SELESAI
echo ================================================
echo.
pause
