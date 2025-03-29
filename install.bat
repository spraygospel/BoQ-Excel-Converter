@echo off
echo ================================================
echo     INSTALASI EXCEL CONVERTER
echo ================================================
echo.

:: Cek apakah Python tersedia
python --version > nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python tidak ditemukan. Silakan install Python 3.8 atau lebih baru.
    echo         Download Python dari: https://www.python.org/downloads/
    pause
    exit /b 1
)

:: Cek versi Python
for /f "tokens=2" %%a in ('python --version 2^>^&1') do set PYTHON_VERSION=%%a
echo Python versi %PYTHON_VERSION% terdeteksi.

:: Buat virtual environment
echo.
echo [INFO] Membuat virtual environment...
if exist venv (
    echo [INFO] Virtual environment sudah ada, menggunakan yang sudah ada...
) else (
    python -m venv venv
    if %errorlevel% neq 0 (
        echo [ERROR] Gagal membuat virtual environment.
        pause
        exit /b 1
    )
)

:: Aktifkan virtual environment dan update pip
echo.
echo [INFO] Mengupdate pip dan setuptools...
call venv\Scripts\activate
python -m pip install --upgrade pip setuptools wheel
if %errorlevel% neq 0 (
    echo [WARNING] Gagal mengupdate pip, mencoba melanjutkan instalasi...
)

echo.
echo [INFO] Instalasi paket utama...
pip install streamlit pandas numpy
if %errorlevel% neq 0 (
    echo [ERROR] Gagal menginstall paket utama.
    pause
    exit /b 1
)

echo.
echo [INFO] Instalasi paket pendukung...
pip install openpyxl xlrd XlsxWriter pytz python-dateutil tqdm
if %errorlevel% neq 0 (
    echo [WARNING] Beberapa paket pendukung gagal diinstall.
)

:: Simpan versi package yang terinstall ke requirements.txt
echo.
echo [INFO] Menyimpan versi paket terinstall ke requirements.txt...
pip freeze > requirements.txt

:: Deaktifkan virtual environment
call venv\Scripts\deactivate

echo.
echo ================================================
echo      INSTALASI SELESAI
echo ================================================
echo.
echo Excel Converter berhasil diinstall.
echo Versi package yang terinstall telah disimpan ke requirements.txt
echo Jalankan aplikasi dengan mengklik 'start_app.bat'
echo.
pause
